from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class ServiceType(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    S3 = "s3"


class ServiceStatus(str, Enum):
    PENDING = "pending"
    CONNECTED = "connected"
    ERROR = "error"


class DatabaseConfig(BaseModel):
    type: Literal["postgres", "mysql"]
    host: str
    port: int = Field(ge=1, le=65535)
    database: str
    username: str
    password: SecretStr
    tables: Optional[list[str]] = None


class S3Config(BaseModel):
    type: Literal["s3"]
    bucket: str
    access_key_id: Optional[str] = None
    secret_access_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    profile: Optional[str] = None
    region: Optional[str] = None
    endpoint_url: Optional[str] = None
    prefix: str = ""

    @model_validator(mode="after")
    def _validate_auth(self):
        has_key = bool(self.access_key_id)
        has_secret = self.secret_access_key is not None
        if has_key != has_secret:
            raise ValueError(
                "access_key_id and secret_access_key must be provided together"
            )
        if self.session_token is not None and not has_key:
            raise ValueError("session_token requires access_key_id")
        if self.profile and has_key:
            raise ValueError("use either profile or access keys, not both")
        return self


class DatasetTarget(S3Config):
    """User-supplied S3 destination for a dataset's state and Lance data."""

    type: Literal["s3"] = "s3"

    @property
    def uri(self) -> str:
        prefix = self.prefix.strip("/")
        return f"s3://{self.bucket}/{prefix}" if prefix else f"s3://{self.bucket}"


ConnectionConfig = Annotated[
    Union[DatabaseConfig, S3Config], Field(discriminator="type")
]


class ChunkConfig(BaseModel):
    splitter: Literal["recursive_markdown"] = "recursive_markdown"
    chunk_size: int = Field(default=512, gt=0)
    chunk_overlap: int = Field(default=50, ge=0)


class EmbedderConfig(BaseModel):
    model: str = "BAAI/bge-m3"
    dimensions: Optional[int] = None
    batch_size: int = Field(default=1000, gt=0)


class DataModelConfig(BaseModel):
    chunk: ChunkConfig = Field(default_factory=ChunkConfig)
    embedder: EmbedderConfig = Field(default_factory=EmbedderConfig)


class ServiceInfo(BaseModel):
    row_count: Optional[int] = None
    object_count: Optional[int] = None
    size_bytes: Optional[int] = None
    tables: Optional[list[str]] = None
    files: Optional[list[str]] = None


class ServiceBase(BaseModel):
    name: str


class ServiceCreate(ServiceBase):
    config: ConnectionConfig


class ServiceUpdate(BaseModel):
    name: Optional[str] = None
    config: Optional[ConnectionConfig] = None


class Service(ServiceBase):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: ServiceType
    status: ServiceStatus = ServiceStatus.PENDING
    error: Optional[str] = None
    info: Optional[ServiceInfo] = None
    created_at: datetime
    updated_at: datetime
