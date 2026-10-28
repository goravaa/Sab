"""Bucket-based dataset discovery.

Dataset-engine commits a non-secret ``config.json`` at every dataset root
(``<prefix>/<dataset>/config.json``). This module scans a dataset bucket for
those commit markers, so query-service derives the target dataset from the
bucket instead of hand-copied environment values.
"""

from __future__ import annotations

import json
import os
from typing import Any

from .dataset import DatasetConnection, DatasetError
from .workspace import workspace_connection

CONFIG_KEY = "config.json"
MAX_SCAN_LISTINGS = 200
MAX_SCAN_DEPTH = 6


def _region() -> str:
    return os.environ.get("SAB_DATASET_REGION") or os.environ.get("AWS_REGION") or "us-west-2"


def _split_credentials(connection: DatasetConnection) -> tuple[str | None, str | None, str | None]:
    return (
        connection.endpoint_url,
        connection.access_key_id,
        connection.secret_access_key.get_secret_value() if connection.secret_access_key else None,
    )


def _client(connection: DatasetConnection) -> Any:
    import boto3

    endpoint, key, secret = _split_credentials(connection)
    kwargs: dict[str, Any] = {"region_name": connection.region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if key and secret:
        kwargs["aws_access_key_id"] = key
        kwargs["aws_secret_access_key"] = secret
    return boto3.client("s3", **kwargs)


def _scan_level(client: Any, bucket: str, prefix: str, *, depth: int, budget: list[int]) -> tuple[list[str], list[str]]:
    """Return (dataset roots ending in config.json, child prefixes) below one prefix."""
    found: list[str] = []
    children: list[str] = []
    listing_prefix = f"{prefix}/" if prefix else ""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=listing_prefix, Delimiter="/"):
        budget[0] -= 1
        if budget[0] <= 0:
            raise DatasetError(
                f"Scanning s3://{bucket} for {CONFIG_KEY} markers exceeded {MAX_SCAN_LISTINGS} listings; "
                "set SAB_DATASET_PREFIX or SAB_DATASET_NAME to narrow the scan"
            )
        for entry in page.get("Contents", []):
            key = entry.get("Key", "")
            if key == f"{listing_prefix}{CONFIG_KEY}" and prefix:
                found.append(prefix)
        for entry in page.get("CommonPrefixes", []):
            child = entry.get("Prefix", "").rstrip("/")
            if child and child != prefix:
                children.append(child)
    if depth < MAX_SCAN_DEPTH:
        return found, children
    return found, []


def discover_datasets(connection: DatasetConnection) -> list[dict[str, Any]]:
    """List committed datasets visible under one bucket prefix."""
    bucket = connection.bucket
    try:
        client = _client(connection)
        client.head_bucket(Bucket=bucket)
    except DatasetError:
        raise
    except Exception as exc:
        raise DatasetError(f"Dataset bucket {bucket!r} is not accessible: {exc}") from exc

    scan_prefix = connection.prefix
    budget = [MAX_SCAN_LISTINGS]
    datasets: dict[str, dict[str, Any]] = {}
    queue: list[tuple[str, int]] = [(scan_prefix, 0)]
    while queue:
        prefix, depth = queue.pop(0)
        roots, children = _scan_level(client, bucket, prefix, depth=depth, budget=budget)
        for root in roots:
            name = root.rsplit("/", 1)[-1]
            entry = _dataset_entry(client, connection, bucket=bucket, prefix=root.rsplit("/", 1)[0] if "/" in root else "", dataset=name)
            if entry:
                datasets[root] = entry
        queue.extend((child, depth + 1) for child in children)
    return sorted(datasets.values(), key=lambda item: (item["dataset"], item["uri"]))


def _dataset_entry(client: Any, connection: DatasetConnection, *, bucket: str, prefix: str, dataset: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket, Key=f"{prefix}/{dataset}/{CONFIG_KEY}".lstrip("/"))
    except Exception:
        return None
    try:
        document = json.loads(response["Body"].read())
    except Exception:
        return None
    sources = document.get("sources", [])
    return {
        "dataset": document.get("dataset", dataset),
        "uri": f"s3://{bucket}/{prefix}/{dataset}".rstrip("/"),
        "embedding_model": document.get("data_model", {}).get("embedder", {}).get("model"),
        "source_count": len(sources),
        "sources": [
            {"name": source.get("name", "Source"), "type": source.get("connection", {}).get("type", "unknown")}
            for source in sources
        ],
    }


def resolve_connection(
    *,
    region: str,
    endpoint_url: str | None,
    access_key_id: str | None,
    secret_access_key: str | None,
) -> tuple[DatasetConnection, list[dict[str, Any]]]:
    """Build a DatasetConnection from the bucket itself.

    ``SAB_DATASET_BUCKET`` scopes discovery. When ``SAB_DATASET_NAME`` is set
    the matching dataset is required; otherwise a unique dataset is selected
    and ambiguity raises with the choices.
    """
    bucket = os.environ.get("SAB_DATASET_BUCKET")
    if not bucket:
        raise DatasetError("SAB_DATASET_BUCKET is required")
    wanted = os.environ.get("SAB_DATASET_NAME") or None
    placeholder = DatasetConnection(
        bucket=bucket,
        dataset="discovery",
        prefix=os.environ.get("SAB_DATASET_PREFIX", ""),
        region=region,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    candidates = discover_datasets(placeholder)
    if wanted:
        matches = [entry for entry in candidates if entry["dataset"] == wanted]
        if not matches:
            names = [entry["dataset"] for entry in candidates]
            raise DatasetError(
                f"Dataset {wanted!r} is not committed in s3://{bucket}; committed datasets: {names or 'none'}"
            )
        entry = matches[0]
    elif len(candidates) == 1:
        entry = candidates[0]
    elif not candidates:
        raise DatasetError(
            f"No committed {CONFIG_KEY} datasets found in s3://{bucket}/"
            f"{placeholder.prefix}; complete indexing with dataset-engine first"
        )
    else:
        names = sorted(entry["dataset"] for entry in candidates)
        raise DatasetError(
            f"Multiple datasets committed in s3://{bucket}; set SAB_DATASET_NAME to one of: {names}"
        )
    uri = entry["uri"].removeprefix("s3://")
    path = uri.split("/", 1)[1]
    parts = path.rsplit("/", 1)
    resolved_prefix = parts[0] if len(parts) > 1 else ""
    resolved_name = parts[-1]
    resolved = DatasetConnection(
        bucket=bucket,
        dataset=resolved_name,
        prefix=resolved_prefix,
        region=region,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    return resolved, candidates


def _scoping_connection() -> DatasetConnection:
    """Bucket/prefix scope: the workspace yaml first, env variables second."""
    workspace = workspace_connection()
    if workspace is not None:
        return DatasetConnection(
            bucket=workspace.bucket,
            dataset="discovery",
            prefix=workspace.prefix,
            region=workspace.region,
            endpoint_url=workspace.endpoint_url,
            access_key_id=workspace.access_key_id,
            secret_access_key=workspace.secret_access_key,
        )
    return _connection_from_env()


def _connection_from_env() -> DatasetConnection:
    bucket = os.environ.get("SAB_DATASET_BUCKET")
    if not bucket:
        raise DatasetError(
            "No dataset scope configured: provide SAB_WORKSPACE (or place "
            "config/workspace.yaml next to the checkout) or set SAB_DATASET_BUCKET"
        )
    return DatasetConnection(
        bucket=bucket,
        dataset="discovery",
        prefix=os.environ.get("SAB_DATASET_PREFIX", ""),
        region=_region(),
        endpoint_url=os.getenv("SAB_S3_ENDPOINT_URL") or None,
        access_key_id=os.getenv("SAB_S3_ACCESS_KEY_ID") or None,
        secret_access_key=os.getenv("SAB_S3_SECRET_ACCESS_KEY") or None,
    )


def list_datasets() -> dict[str, Any]:
    """MCP-visible enumeration of committed datasets under the configured scope."""
    placeholder = _scoping_connection()
    return {
        "bucket": placeholder.bucket,
        "prefix": placeholder.prefix,
        "datasets": discover_datasets(placeholder),
    }
