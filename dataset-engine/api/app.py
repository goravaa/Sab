"""FastAPI application assembly; business operations live in services."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from api.logging_config import get_logger, install_logging
from api.routes.datasets import router as datasets_router
from api.routes.health import router as health_router
from api.routes.jobs import router as jobs_router
from api.routes.sources import router as sources_router
from api.routes.ui import router as ui_router

app = FastAPI(title="Sab Dataset Engine", version="1.0.0")
install_logging(app)
logger = get_logger(__name__)
logger.info("dataset engine api initialized")
_STATIC = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC), name="static")
app.include_router(ui_router)
app.include_router(health_router)
app.include_router(sources_router)
app.include_router(datasets_router)
app.include_router(jobs_router)
