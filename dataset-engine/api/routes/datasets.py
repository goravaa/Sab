from fastapi import APIRouter, HTTPException

from api.schemas import ReconcileRequest
from api.services.sources import dataset_config, reconcile
from main.schema.job import DiffSummary

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])


@router.post("/reconcile", response_model=DiffSummary)
def reconcile_route(request: ReconcileRequest) -> DiffSummary:
    config = dataset_config(request.dataset, request.sources, request.target)
    try:
        return reconcile(request.dataset, config, request.target)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
