"""Job submission and status routes, including the UI progress stream."""

import asyncio
import json
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse

from api.logging_config import get_logger
from api.schemas import JobLookupRequest, JobResponse, JobSubmitRequest
from api.services.job_lock import DatasetJobLock, JobAlreadyRunning
from api.services.jobs import (
    create_job,
    granular_progress,
    load_current_job_status,
    load_job_status,
    progress_from_receipts,
    run_job_background,
    store_for,
)
from main.process.runner import load_manifest

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])
logger = get_logger(__name__)


def _sse(event: str, data: Any) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


@router.post("", response_model=JobResponse, status_code=202)
def submit_job(
    request: JobSubmitRequest,
    background_tasks: BackgroundTasks,
) -> JobResponse:
    logger.info(
        "job submit requested dataset=%s sources=%d",
        request.dataset,
        len(request.sources),
    )
    try:
        job_lock = DatasetJobLock.acquire()
    except JobAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        job_lock.update({
            "dataset": request.dataset,
            "target_uri": request.target.uri,
            "status": "preparing",
        })
        job_id, config, _, status = create_job(request)
    except Exception as exc:
        job_lock.release()
        logger.warning(
            "job submit rejected dataset=%s error=%s",
            request.dataset,
            type(exc).__name__,
        )
        logger.debug("job submit failure detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job_lock.update({
        "id": job_id,
        "dataset": config.dataset,
        "target_uri": config.target.uri if config.target is not None else None,
        "status": "pending",
    })
    background_tasks.add_task(run_job_background, job_id, config, job_lock)
    logger.info("job accepted job_id=%s dataset=%s", job_id, request.dataset)
    return JobResponse.model_validate(status)


def job_stream_response(
    job_id: str,
    config,
    store,
    initial_status: dict[str, Any],
    job_lock: DatasetJobLock,
) -> StreamingResponse:
    """Return an SSE response that runs and reports one prepared job."""

    job_lock.update({
        "id": job_id,
        "dataset": config.dataset,
        "target_uri": config.target.uri if config.target is not None else None,
        "status": "pending",
    })

    async def events():
        logger.info("job stream opened job_id=%s dataset=%s", job_id, config.dataset)
        worker = asyncio.create_task(
            asyncio.to_thread(run_job_background, job_id, config, job_lock)
        )
        yield _sse("job", JobResponse.model_validate(initial_status).model_dump(mode="json"))
        # Batches fill the manifest sequentially, so one manifest read backs
        # every granular per-source update; receipts supply the cursors.
        manifest = load_manifest(store, job_id)
        previous: tuple[int, int] | None = None
        last_heartbeat = asyncio.get_running_loop().time()
        committed_event_sent = False

        while True:
            await asyncio.sleep(0.5)
            status = load_job_status(store, job_id) or {}
            total = int(initial_status["progress"]["total_items"])
            # The receipt cursor is a cheap poll (one listing plus one small
            # read); the granular breakdown reads recent ticks, so it only
            # runs when the cursor actually moved.
            cursor = progress_from_receipts(store, job_id, total)
            marker = (cursor.processed_items, cursor.completed_batches)
            if marker != previous:
                previous = marker
                progress = cursor if manifest is None else granular_progress(
                    store, job_id, manifest, total_items=total
                )
                yield _sse("progress", {
                    "job_id": job_id,
                    "status": status.get("status", "running"),
                    "progress": progress.model_dump(mode="json"),
                })
                last_heartbeat = asyncio.get_running_loop().time()

            if status.get("phase") == "cleanup" and not committed_event_sent:
                committed_event_sent = True
                yield _sse("committed", JobResponse.model_validate(status).model_dump(mode="json"))

            if status.get("status") in {"completed", "failed"}:
                await worker
                logger.info(
                    "job stream closed job_id=%s status=%s",
                    job_id,
                    status["status"],
                )
                yield _sse(
                    status["status"],
                    JobResponse.model_validate(status).model_dump(mode="json"),
                )
                break

            now = asyncio.get_running_loop().time()
            if now - last_heartbeat >= 15:
                yield ": keep-alive\n\n"
                last_heartbeat = now

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/stream")
async def submit_job_stream(request: JobSubmitRequest) -> StreamingResponse:
    """Submit a job and stream receipt-backed progress as SSE."""
    logger.info(
        "job stream requested dataset=%s sources=%d",
        request.dataset,
        len(request.sources),
    )
    try:
        job_lock = DatasetJobLock.acquire()
    except JobAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        job_lock.update({
            "dataset": request.dataset,
            "target_uri": request.target.uri,
            "status": "preparing",
        })
        job_id, config, store, initial_status = create_job(request)
    except Exception as exc:
        job_lock.release()
        logger.warning(
            "job stream rejected dataset=%s error=%s",
            request.dataset,
            type(exc).__name__,
        )
        logger.debug("job stream failure detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return job_stream_response(job_id, config, store, initial_status, job_lock)


@router.post("/{job_id}/status", response_model=JobResponse)
def get_job_status(job_id: str, request: JobLookupRequest) -> JobResponse:
    logger.debug("job status requested job_id=%s dataset=%s", job_id, request.dataset)
    store = store_for(request.target, request.dataset)
    try:
        status = load_current_job_status(store, job_id)
        if status is None:
            logger.warning("job status not found job_id=%s dataset=%s", job_id, request.dataset)
            raise HTTPException(status_code=404, detail="Job not found")
        return JobResponse.model_validate(status)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "job status read failed job_id=%s dataset=%s error=%s",
            job_id,
            request.dataset,
            type(exc).__name__,
        )
        logger.debug("job status failure detail", exc_info=True)
        raise HTTPException(status_code=500, detail="Stored job status is invalid") from exc
