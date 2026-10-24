"""Job submission and status routes, including the UI progress stream."""

import asyncio
import json
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse

from api.schemas import JobLookupRequest, JobResponse, JobSubmitRequest
from api.services.jobs import (
    create_job,
    load_job_status,
    progress_from_receipts,
    run_job_background,
    status_with_current_progress,
    store_for,
)

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


def _sse(event: str, data: Any) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


@router.post("", response_model=JobResponse, status_code=202)
def submit_job(
    request: JobSubmitRequest,
    background_tasks: BackgroundTasks,
) -> JobResponse:
    try:
        job_id, config, _, status = create_job(request)
        background_tasks.add_task(run_job_background, job_id, config)
        return JobResponse.model_validate(status)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stream")
async def submit_job_stream(request: JobSubmitRequest) -> StreamingResponse:
    """Submit a job and stream receipt-backed progress as SSE."""
    try:
        job_id, config, store, initial_status = create_job(request)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def events():
        worker = asyncio.create_task(asyncio.to_thread(run_job_background, job_id, config))
        yield _sse("job", JobResponse.model_validate(initial_status).model_dump(mode="json"))
        previous: tuple[int, int] | None = None
        last_heartbeat = asyncio.get_running_loop().time()

        while True:
            await asyncio.sleep(0.5)
            status = load_job_status(store, job_id) or {}
            total = int(initial_status["progress"]["total_items"])
            progress = progress_from_receipts(store, job_id, total)
            marker = (progress.processed_items, progress.completed_batches)
            if marker != previous:
                previous = marker
                yield _sse("progress", {
                    "job_id": job_id,
                    "status": status.get("status", "running"),
                    "progress": progress.model_dump(mode="json"),
                })
                last_heartbeat = asyncio.get_running_loop().time()

            if status.get("status") in {"completed", "failed"}:
                await worker
                yield _sse(status["status"], JobResponse.model_validate(status).model_dump(mode="json"))
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


@router.post("/{job_id}/status", response_model=JobResponse)
def get_job_status(job_id: str, request: JobLookupRequest) -> JobResponse:
    store = store_for(request.target, request.dataset)
    try:
        status = load_job_status(store, job_id)
        if status is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobResponse.model_validate(status_with_current_progress(store, job_id, status))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Stored job status is invalid") from exc
