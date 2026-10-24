"""Dataset job preparation, execution, and persisted progress reads."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from main.process.config import DatasetConfig
from main.process.identifiers import load_identifiers
from main.process.runner import build_manifest, load_manifest, run_job, save_manifest
from main.process.sources import iter_source_items
from main.process.store import S3DatasetStore
from main.schema.job import JobProgress
from main.schema.service import DatasetTarget

from api.schemas import JobSubmitRequest
from api.services.sources import dataset_config, summarize_diff

logger = logging.getLogger(__name__)


def store_for(target: DatasetTarget, dataset: str) -> S3DatasetStore:
    return S3DatasetStore(target, dataset)


def status_key(job_id: str) -> str:
    return f"jobs/{job_id}/status.json"


def write_status(store: S3DatasetStore, data: dict[str, Any]) -> None:
    store.write_bytes(status_key(data["id"]), json.dumps(data).encode("utf-8"))


def create_job(request: JobSubmitRequest):
    job_id = str(uuid4())
    now = datetime.now(timezone.utc)
    config = dataset_config(
        request.dataset,
        request.sources,
        request.target,
        item_batch_size=request.item_batch_size,
    )
    store = store_for(request.target, request.dataset)
    identifiers = load_identifiers(store)
    # Exhaust source listing first so incomplete scans cannot produce deletions.
    items = list(iter_source_items(config))
    manifest = build_manifest(
        job_id=job_id,
        dataset=request.dataset,
        items=items,
        identifiers=identifiers,
        created_at=now,
    )
    save_manifest(store, manifest)
    status = {
        "id": job_id,
        "dataset": request.dataset,
        "status": "pending",
        "progress": JobProgress(total_items=len(manifest.entries)).model_dump(mode="json"),
        "diff": summarize_diff(manifest.entries).model_dump(mode="json"),
        "error": None,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    write_status(store, status)
    return job_id, config, store, status


def run_job_background(job_id: str, config: DatasetConfig) -> None:
    store = store_for(config.target, config.dataset)  # type: ignore[arg-type]
    status = json.loads(store.read_bytes(status_key(job_id)) or b"{}")
    status.update(status="running", updated_at=datetime.now(timezone.utc).isoformat())
    write_status(store, status)
    try:
        progress = run_job(job_id=job_id, config=config, target=config.target)
        status.update(
            status="completed",
            progress=progress.model_dump(mode="json"),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        logger.exception("Dataset job %s failed", job_id)
        status.update(
            status="failed",
            error=str(exc),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
    write_status(store, status)


def load_job_status(store: S3DatasetStore, job_id: str) -> dict[str, Any] | None:
    raw = store.read_bytes(status_key(job_id))
    return json.loads(raw) if raw else None


def progress_from_receipts(
    store: S3DatasetStore,
    job_id: str,
    total_items: int,
) -> JobProgress:
    keys = sorted(store.list_keys(f"jobs/{job_id}/progress/"))
    processed = 0
    for key in keys:
        receipt = json.loads(store.read_bytes(key) or b"{}")
        processed = max(processed, int(receipt.get("end_index", 0)))
    return JobProgress(
        total_items=total_items,
        processed_items=processed,
        completed_batches=len(keys),
    )


def status_with_current_progress(
    store: S3DatasetStore,
    job_id: str,
    status: dict[str, Any],
) -> dict[str, Any]:
    manifest = load_manifest(store, job_id)
    if manifest is None or status.get("status") not in {"pending", "running"}:
        return status
    status["progress"] = progress_from_receipts(
        store, job_id, len(manifest.entries)
    ).model_dump(mode="json")
    return status
