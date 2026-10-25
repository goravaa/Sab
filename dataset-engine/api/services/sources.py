"""Source connection checks and reconciliation operations."""

import os

from typing import Any
from uuid import uuid4

from api.logging_config import get_logger
from main.connect.database.postgres import PostgresConnector
from main.connect.object_storage.s3 import S3Connector
from main.process.config import DatasetConfig
from main.process.identifiers import load_identifiers
from main.process.runner import build_manifest
from main.process.sources import iter_source_items
from main.process.store import S3DatasetStore
from main.schema.job import DiffAction, DiffSummary
from main.schema.service import (
    ConnectionConfig,
    DataModelConfig,
    DatasetTarget,
    EmbedderConfig,
)

logger = get_logger(__name__)


def check_source(config: ConnectionConfig) -> dict[str, Any]:
    logger.info("connecting source type=%s", config.type)

    if config.type == "postgres":
        connector = PostgresConnector(config)
        connector.check_connection()
        tables = connector.list_tables()
        logger.info("postgres source connected tables=%d", len(tables))
        return {
            "tables": tables,
            "row_count": sum(connector.count_rows(table) for table in tables),
        }

    connector = S3Connector(config)
    connector.check_connection()
    count, size = connector.object_count_and_size()
    logger.info("s3 source connected objects=%d size_bytes=%d", count, size)
    return {"object_count": count, "size_bytes": size}


def dataset_config(
    dataset: str,
    sources: list[Any],
    target: DatasetTarget,
    *,
    item_batch_size: int = 1000,
    data_model: DataModelConfig | None = None,
) -> DatasetConfig:
    if data_model is None:
        model = os.getenv("SAB_EMBED_MODEL", "").strip()
        data_model = (
            DataModelConfig(embedder=EmbedderConfig(model=model))
            if model
            else DataModelConfig()
        )
    return DatasetConfig(
        dataset=dataset,
        sources=sources,
        target=target,
        data_model=data_model,
        item_batch_size=item_batch_size,
    )


def summarize_diff(entries: list[Any]) -> DiffSummary:
    counts = {action: 0 for action in DiffAction}
    for entry in entries:
        counts[entry.action] += 1
    return DiffSummary(
        new=counts[DiffAction.NEW],
        modified=counts[DiffAction.MODIFIED],
        deleted=counts[DiffAction.DELETED],
    )


def reconcile(dataset: str, config: DatasetConfig, target: DatasetTarget) -> DiffSummary:
    logger.info(
        "scanning sources dataset=%s sources=%d",
        dataset,
        len(config.sources),
    )
    store = S3DatasetStore(target, dataset)
    identifiers = load_identifiers(store)
    # Exhaust the scan before build_manifest can infer any deletions.
    items = list(iter_source_items(config))
    logger.info("source scan completed dataset=%s items=%d", dataset, len(items))
    manifest = build_manifest(
        job_id=f"preview-{uuid4()}",
        dataset=dataset,
        items=items,
        identifiers=identifiers,
    )
    return summarize_diff(manifest.entries)
