from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class SourceType(str, Enum):
    DB_ROW = "db_row"
    S3_OBJECT = "s3_object"


class Document(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    doc_id: str
    signature: str
    source_type: SourceType
    raw_content: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class Chunk(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    chunk_id: str
    doc_id: str
    source_id: Optional[str] = None
    job_id: Optional[str] = None
    source_type: SourceType
    table_name: str = ""
    table_kind: str = "document"
    chunk_index: int = Field(ge=0)
    text: str
    header_path: Optional[list[str]] = None
    token_count: Optional[int] = Field(default=None, ge=0)
    embedding: Optional[list[float]] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
