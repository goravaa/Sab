"""Read-only dataset health and summary operations."""

from __future__ import annotations

import os

from ..dataset import DatasetConnection, open_dataset
from ..schema import DatasetSummary, IntegrityReport


def configured_connection() -> DatasetConnection:
    """Resolve one dataset from server-side environment/AWS credentials."""
    bucket = os.getenv("SAB_DATASET_BUCKET", "sab-data")
    dataset = os.getenv("SAB_DATASET_NAME", "ui-smoke-173645")
    return DatasetConnection(
        bucket=bucket,
        dataset=dataset,
        prefix=os.getenv("SAB_DATASET_PREFIX", "sab-datasets"),
        region=os.getenv("SAB_DATASET_REGION", "us-west-2"),
        endpoint_url=os.getenv("SAB_S3_ENDPOINT_URL") or None,
        access_key_id=os.getenv("SAB_S3_ACCESS_KEY_ID") or None,
        secret_access_key=os.getenv("SAB_S3_SECRET_ACCESS_KEY") or None,
    )


def inspect_dataset() -> dict:
    """Return dataset name, source counts, committed document counts, and index size."""
    dataset = open_dataset(configured_connection())
    return DatasetSummary.model_validate(dataset.summary).model_dump(mode="json")


def audit_integrity() -> dict:
    """Compare committed source/doc/chunk counts with the visible Lance index."""
    dataset = open_dataset(configured_connection())
    return IntegrityReport.model_validate(dataset.integrity_report()).model_dump(mode="json")
