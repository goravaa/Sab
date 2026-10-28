"""Routes backed by the local workspace YAML instead of browser credentials."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from api.logging_config import get_logger
from api.routes.jobs import job_stream_response
from api.schemas import JobResponse, JobSubmitRequest
from api.services.jobs import (
    create_job,
    load_current_job_status,
    store_for,
)
from api.services.job_lock import DatasetJobLock, JobAlreadyRunning
from api.services.workspace import reconcile_workspace, workspace_status
from main.process.workspace import (
    WorkspaceConfigError,
    load_workspace,
    workspace_view,
)
from main.schema.job import DiffSummary

router = APIRouter(prefix="/v1/workspace", tags=["workspace"])
logger = get_logger(__name__)


def _load():
    try:
        return load_workspace()
    except WorkspaceConfigError as exc:
        logger.warning("workspace config rejected error=%s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _job_request(config) -> JobSubmitRequest:
    if config.target is None:
        raise HTTPException(status_code=400, detail="workspace has no target")
    return JobSubmitRequest(
        dataset=config.dataset,
        sources=config.sources,
        target=config.target,
        item_batch_size=config.item_batch_size,
    )


@router.get("")
def get_workspace() -> dict:
    """Return the safe workspace definition without credentials."""

    return workspace_view(_load())


@router.get("/status")
def get_workspace_status(check_sources: bool = True) -> dict:
    """Return source access, target reachability, and indexed counts."""

    config = _load()
    try:
        return workspace_status(config, check_sources=check_sources)
    except Exception as exc:
        logger.exception("workspace status failed dataset=%s", config.dataset)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reconcile", response_model=DiffSummary)
def reconcile_workspace_route() -> DiffSummary:
    """Preview changes for every source in the workspace."""

    config = _load()
    try:
        return reconcile_workspace(config)
    except Exception as exc:
        logger.warning(
            "workspace reconcile failed dataset=%s error=%s",
            config.dataset,
            type(exc).__name__,
        )
        logger.debug("workspace reconcile detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/stream")
async def stream_workspace_job():
    """Index the workspace and stream progress without accepting credentials."""

    config = _load()
    request = _job_request(config)
    try:
        job_lock = DatasetJobLock.acquire()
    except JobAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        job_lock.update({
            "dataset": config.dataset,
            "target_uri": config.target.uri,
            "status": "preparing",
        })
        job_id, job_config, store, initial_status = create_job(
            request,
            data_model=config.data_model,
        )
    except Exception as exc:
        job_lock.release()
        logger.warning(
            "workspace job rejected dataset=%s error=%s",
            config.dataset,
            type(exc).__name__,
        )
        logger.debug("workspace job detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return job_stream_response(job_id, job_config, store, initial_status, job_lock)


@router.get("/jobs/active")
def get_active_workspace_job() -> dict:
    """Return the host's active job, including progress persisted in S3."""

    active = DatasetJobLock.read_active()
    if active is None:
        return {"job": None}

    job_id = active.get("id")
    config = _load()
    if not job_id:
        return {"job": active}
    if (
        active.get("dataset") != config.dataset
        or active.get("target_uri") != config.target.uri
    ):
        return {"job": active}

    store = store_for(config.target, config.dataset)
    status = load_current_job_status(store, job_id)
    if status is None:
        return {"job": active}
    return {
        "job": status,
    }


@router.get("/jobs/{job_id}", response_model=JobResponse)
def get_workspace_job_status(job_id: str) -> JobResponse:
    """Read a workspace job's status without requiring browser credentials."""

    config = _load()
    store = store_for(config.target, config.dataset)
    status = load_current_job_status(store, job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse.model_validate(status)
