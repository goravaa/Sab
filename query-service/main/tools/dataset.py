"""Read-only dataset health and summary operations."""

from __future__ import annotations

import os

from ..dataset import DatasetConnection, open_dataset
from ..schema import DatasetSummary, IntegrityReport
from ..workspace import workspace_connection


def configured_connection() -> DatasetConnection:
    """Resolve one dataset from server-side configuration.

    Precedence: workspace.yaml (SAB_WORKSPACE or the checkout default) wins;
    discovery from SAB_DATASET_BUCKET is the fallback, and the legacy sample
    variables still apply last when no bucket is configured.
    """
    workspace = workspace_connection()
    if workspace is not None:
        return workspace
    bucket = os.getenv("SAB_DATASET_BUCKET")
    endpoint_url = os.getenv("SAB_S3_ENDPOINT_URL") or None
    access_key_id = os.getenv("SAB_S3_ACCESS_KEY_ID") or None
    secret_access_key = os.getenv("SAB_S3_SECRET_ACCESS_KEY") or None
    region = os.getenv("SAB_DATASET_REGION", os.getenv("AWS_REGION", "us-west-2"))
    if bucket:
        from ..discovery import resolve_connection

        connection, _ = resolve_connection(
            region=region,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )
        return connection
    return DatasetConnection(
        bucket=os.getenv("SAB_DATASET_BUCKET", "sab-data"),
        dataset=os.getenv("SAB_DATASET_NAME", "ui-smoke-173645"),
        prefix=os.getenv("SAB_DATASET_PREFIX", "sab-datasets"),
        region=region,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def inspect_dataset() -> dict:
    """Return dataset name, source counts, committed document counts, and index size."""
    dataset = open_dataset(configured_connection())
    return DatasetSummary.model_validate(dataset.summary).model_dump(mode="json")


def audit_integrity() -> dict:
    """Compare committed source/doc/chunk counts with the visible Lance index."""
    dataset = open_dataset(configured_connection())
    return IntegrityReport.model_validate(dataset.integrity_report()).model_dump(mode="json")
