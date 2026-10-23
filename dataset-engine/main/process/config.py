"""Configuration objects for a dataset processing run.

A dataset config describes where source data lives and how it should be
documented. Credentials for the central dataset bucket are kept separate from
the dataset config and can be loaded from the standard S3 environment
variables with ``target_from_env``.
"""

from __future__ import annotations

import os

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from main.schema.service import (
    ConnectionConfig,
    DataModelConfig,
    DatasetTarget,
)


class SourceConfig(BaseModel):
    """One source system that contributes documents to a dataset."""

    name: str = Field(min_length=1)
    connection: ConnectionConfig
    # Source-specific knobs such as ``namespace`` and ``id_column`` for
    # tabular files. Keeping them here avoids adding format-specific fields to
    # the shared connection schemas.
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetConfig(BaseModel):
    """The complete definition of a dataset and its sources."""

    dataset: str = Field(min_length=1)
    sources: list[SourceConfig] = Field(default_factory=list)
    data_model: DataModelConfig = Field(default_factory=DataModelConfig)
    item_batch_size: int = Field(default=1000, gt=0)
    # Optional so local/dev callers can still supply a target directly to the
    # runner. In the API path this is the user's BYOB destination.
    target: DatasetTarget | None = None


def read_env_file(path: str | Path) -> dict[str, str]:
    """Read a dotenv-style or flat YAML file without logging its values.

    Both ``KEY = value`` and ``KEY: value`` lines are accepted so the existing
    ``s3.yaml`` file (which is actually key/value text) can be used directly.
    """

    values: dict[str, str] = {}

    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line.removeprefix("export ").strip()

        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue

        key = key.strip().strip("\"'")
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]

        if key:
            values[key] = value

    return values


def load_env_file(
    path: str | Path,
    *,
    override: bool = False,
) -> set[str]:
    """Load an env file into ``os.environ`` and return the keys it contained.

    Values are never printed or returned, so callers can safely log the key
    names for diagnostics.
    """

    values = read_env_file(path)

    for key, value in values.items():
        if override or key not in os.environ:
            os.environ[key] = value

    return set(values)


def target_from_file(
    path: str | Path,
    *,
    prefix: str | None = None,
) -> DatasetTarget:
    """Build a dataset target config from an env file."""

    return target_from_env(read_env_file(path), prefix=prefix)


def target_from_env(
    env: Mapping[str, str] | None = None,
    *,
    prefix: str | None = None,
) -> DatasetTarget:
    """Build the central dataset bucket config from standard S3 variables.

    The function intentionally does not read ``s3.yaml`` or log values, so
    credentials stay in the process environment (or in the mapping supplied by
    the caller).
    """

    values = os.environ if env is None else env

    bucket = values.get("S3_BUCKET", "").strip()
    if not bucket:
        raise ValueError("S3_BUCKET is required for the dataset bucket")

    access_key_id = values.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_access_key = values.get("AWS_SECRET_ACCESS_KEY", "").strip()
    if not access_key_id or not secret_access_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for the "
            "dataset bucket"
        )

    return DatasetTarget(
        type="s3",
        bucket=bucket,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=values.get("S3_REGION") or None,
        endpoint_url=values.get("S3_ENDPOINT_URL") or None,
        prefix=(prefix if prefix is not None else values.get("S3_PREFIX", "")),
    )
