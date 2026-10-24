from fastapi import APIRouter, HTTPException

from api.schemas import SourceCheckRequest, SourceCheckResponse
from api.services.sources import check_source

router = APIRouter(prefix="/v1/sources", tags=["sources"])


@router.post("/check", response_model=SourceCheckResponse)
def check_source_route(request: SourceCheckRequest) -> SourceCheckResponse:
    try:
        return SourceCheckResponse(connected=True, info=check_source(request.config))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
