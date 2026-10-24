from fastapi import APIRouter, HTTPException

from api.logging_config import get_logger
from api.schemas import SourceCheckRequest, SourceCheckResponse
from api.services.sources import check_source

router = APIRouter(prefix="/v1/sources", tags=["sources"])
logger = get_logger(__name__)


@router.post("/check", response_model=SourceCheckResponse)
def check_source_route(request: SourceCheckRequest) -> SourceCheckResponse:
    logger.info(
        "source check requested name=%s type=%s",
        request.name,
        request.config.type,
    )
    try:
        info = check_source(request.config)
    except Exception as exc:
        logger.warning(
            "source check failed name=%s type=%s error=%s",
            request.name,
            request.config.type,
            type(exc).__name__,
        )
        logger.debug("source check failure detail", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.info(
        "source check completed name=%s type=%s",
        request.name,
        request.config.type,
    )
    return SourceCheckResponse(connected=True, info=info)
