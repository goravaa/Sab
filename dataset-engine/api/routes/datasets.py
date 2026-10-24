from fastapi import APIRouter, HTTPException

from api.logging_config import get_logger
from api.schemas import ReconcileRequest
from api.services.sources import dataset_config, reconcile
from main.schema.job import DiffSummary

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])
logger = get_logger(__name__)


@router.post("/reconcile", response_model=DiffSummary)
def reconcile_route(request: ReconcileRequest) -> DiffSummary:
    logger.info(
        "reconcile requested dataset=%s sources=%d",
        request.dataset,
        len(request.sources),
    )
    config = dataset_config(request.dataset, request.sources, request.target)
    try:
        diff = reconcile(request.dataset, config, request.target)
    except Exception as exc:
        logger.warning(
            "reconcile failed dataset=%s error=%s",
            request.dataset,
            type(exc).__name__,
        )
        logger.debug("reconcile failure detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.info(
        "reconcile completed dataset=%s new=%d modified=%d deleted=%d",
        request.dataset,
        diff.new,
        diff.modified,
        diff.deleted,
    )
    return diff
