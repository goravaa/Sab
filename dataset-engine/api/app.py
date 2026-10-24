"""FastAPI application assembly; business operations live in services."""

from fastapi import FastAPI

from api.routes.datasets import router as datasets_router
from api.routes.health import router as health_router
from api.routes.jobs import router as jobs_router
from api.routes.sources import router as sources_router

app = FastAPI(title="Sab Dataset Engine", version="1.0.0")
app.include_router(health_router)
app.include_router(sources_router)
app.include_router(datasets_router)
app.include_router(jobs_router)
