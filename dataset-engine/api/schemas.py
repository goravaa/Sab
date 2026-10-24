"""HTTP request and response models for the dataset engine API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from main.process.config import SourceConfig
from main.schema.job import DiffSummary, JobProgress, JobStatus
from main.schema.service import ConnectionConfig, DatasetTarget


class SourceCheckRequest(BaseModel):
    name: str = Field(min_length=1)
    config: ConnectionConfig


class SourceCheckResponse(BaseModel):
    connected: bool
    info: dict[str, Any] = Field(default_factory=dict)


class ReconcileRequest(BaseModel):
    dataset: str = Field(min_length=1)
    sources: list[SourceConfig]
    target: DatasetTarget


class JobSubmitRequest(ReconcileRequest):
    item_batch_size: int = Field(default=1000, gt=0)


class JobResponse(BaseModel):
    id: str
    dataset: str
    status: JobStatus
    progress: JobProgress
    diff: DiffSummary | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime


class JobLookupRequest(BaseModel):
    dataset: str = Field(min_length=1)
    target: DatasetTarget
