from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, computed_field

from .document import SourceType
from .service import DatasetTarget


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    # The runner stopped (crash, host restart) without a terminal write.
    # Terminal from the UI's perspective, but distinguishable from FAILED.
    INTERRUPTED = "interrupted"


class DiffAction(str, Enum):
    NEW = "new"
    MODIFIED = "modified"
    DELETED = "deleted"


class DiffSummary(BaseModel):
    new: int = Field(default=0, ge=0)
    modified: int = Field(default=0, ge=0)
    deleted: int = Field(default=0, ge=0)

    @computed_field
    @property
    def total(self) -> int:
        return self.new + self.modified + self.deleted


class ManifestEntry(BaseModel):
    doc_id: str
    signature: str
    source_type: SourceType
    action: DiffAction
    source: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobManifest(BaseModel):
    job_id: str
    dataset: str
    created_at: datetime
    entries: list[ManifestEntry]


class BatchReceipt(BaseModel):
    batch_number: int = Field(ge=1)
    start_index: int = Field(ge=0)
    end_index: int = Field(ge=0)
    item_count: int = Field(ge=0)
    processed_at: datetime
    records: list[dict[str, Any]] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)


class SourceProgress(BaseModel):
    """Granular progress for one configured source within a running job."""

    name: str
    total_items: int = Field(default=0, ge=0)
    processed_items: int = Field(default=0, ge=0)
    new: int = Field(default=0, ge=0)
    modified: int = Field(default=0, ge=0)
    deleted: int = Field(default=0, ge=0)

    @computed_field
    @property
    def ratio(self) -> float:
        if self.total_items == 0:
            return 0.0
        return min(self.processed_items / self.total_items, 1.0)


class JobProgress(BaseModel):
    total_items: int = Field(default=0, ge=0)
    processed_items: int = Field(default=0, ge=0)
    completed_batches: int = Field(default=0, ge=0)
    # Server-side throughput estimate from per-batch ticks; None until at
    # least two batches have timestamps to compare.
    items_per_second: Optional[float] = Field(default=None, ge=0)
    eta_seconds: Optional[float] = Field(default=None, ge=0)
    # Optional per-source breakdown; receipts have no source info and old
    # persisted statuses stay valid because the field defaults to empty.
    sources: list[SourceProgress] = Field(default_factory=list)

    @computed_field
    @property
    def ratio(self) -> float:
        if self.total_items == 0:
            return 0.0
        return min(self.processed_items / self.total_items, 1.0)


class JobBase(BaseModel):
    dataset: str


class JobCreate(JobBase):
    """A BYOB job request: the caller chooses where the dataset lives."""

    target: DatasetTarget


class JobUpdate(BaseModel):
    status: Optional[JobStatus] = None
    progress: Optional[JobProgress] = None
    error: Optional[str] = None


class Job(JobBase):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: JobStatus = JobStatus.PENDING
    progress: JobProgress = Field(default_factory=JobProgress)
    error: Optional[str] = None
    # Safe read-model projection; raw credentials stay out of job responses.
    target_uri: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
