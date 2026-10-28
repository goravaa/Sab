"""Dataset job preparation, execution, and persisted progress reads."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from api.logging_config import get_logger
from api.services.job_lock import DatasetJobLock
from api.schemas import JobSubmitRequest
from api.services.sources import dataset_config, summarize_diff
from main.process.config import DatasetConfig
from main.process.identifiers import load_identifiers
from main.process.runner import (
    build_manifest,
    load_manifest,
    receipt_key,
    run_job,
    save_manifest,
    tick_key,
)
from main.process.sources import iter_source_items
from main.process.store import S3DatasetStore
from main.schema.job import JobManifest, JobProgress, SourceProgress
from main.schema.service import DataModelConfig, DatasetTarget

logger = get_logger(__name__)


def store_for(target: DatasetTarget, dataset: str) -> S3DatasetStore:
    return S3DatasetStore(target, dataset)


def status_key(job_id: str) -> str:
    return f"jobs/{job_id}/status.json"


def write_status(store: S3DatasetStore, data: dict[str, Any]) -> None:
    store.write_bytes(status_key(data["id"]), json.dumps(data).encode("utf-8"))


def create_job(
    request: JobSubmitRequest,
    *,
    data_model: DataModelConfig | None = None,
):
    job_id = str(uuid4())
    now = datetime.now(timezone.utc)
    logger.info(
        "preparing job job_id=%s dataset=%s sources=%d",
        job_id,
        request.dataset,
        len(request.sources),
    )
    config = dataset_config(
        request.dataset,
        request.sources,
        request.target,
        item_batch_size=request.item_batch_size,
        data_model=data_model,
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
    diff = summarize_diff(manifest.entries)
    status = {
        "id": job_id,
        "dataset": request.dataset,
        "status": "pending",
        "progress": JobProgress(total_items=len(manifest.entries)).model_dump(mode="json"),
        "diff": diff.model_dump(mode="json"),
        "error": None,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    write_status(store, status)
    logger.info(
        "job prepared job_id=%s dataset=%s entries=%d new=%d modified=%d deleted=%d",
        job_id,
        request.dataset,
        len(manifest.entries),
        diff.new,
        diff.modified,
        diff.deleted,
    )
    return job_id, config, store, status


def run_job_background(
    job_id: str,
    config: DatasetConfig,
    job_lock: DatasetJobLock | None = None,
) -> None:
    try:
        store = store_for(config.target, config.dataset)  # type: ignore[arg-type]
        status = json.loads(store.read_bytes(status_key(job_id)) or b"{}")
        status.update(status="running", updated_at=datetime.now(timezone.utc).isoformat())
        write_status(store, status)
        if job_lock is not None:
            job_lock.update({
                "id": job_id,
                "dataset": config.dataset,
                "target_uri": config.target.uri if config.target is not None else None,
                "status": "running",
            })
        logger.info("job started job_id=%s dataset=%s", job_id, config.dataset)
        try:
            progress = run_job(job_id=job_id, config=config, target=config.target)
            # Persist a final granular breakdown, from the manifest the
            # runner actually processed, so completed states keep the
            # per-source view after a refresh.
            manifest = load_manifest(store, job_id)
            if manifest is not None:
                progress = granular_progress(store, job_id, manifest)
            status.update(
                status="completed",
                progress=progress.model_dump(mode="json"),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
            logger.info(
                "job completed job_id=%s dataset=%s processed=%d total=%d",
                job_id,
                config.dataset,
                progress.processed_items,
                progress.total_items,
            )
        except Exception as exc:
            logger.exception("job failed job_id=%s dataset=%s", job_id, config.dataset)
            status.update(
                status="failed",
                error=str(exc),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
        write_status(store, status)
    finally:
        if job_lock is not None:
            job_lock.release()


def load_job_status(store: S3DatasetStore, job_id: str) -> dict[str, Any] | None:
    raw = store.read_bytes(status_key(job_id))
    return json.loads(raw) if raw else None


def receipt_numbers(store: S3DatasetStore, job_id: str) -> list[int]:
    """Batch numbers with a persisted receipt, ascending and numeric."""

    keys = store.list_keys(f"jobs/{job_id}/progress/")
    return sorted(
        int(key.rsplit("_", 1)[-1].removesuffix(".json"))
        for key in keys
        if key.rsplit("_", 1)[-1].removesuffix(".json").isdigit()
    )


def progress_from_receipts(
    store: S3DatasetStore,
    job_id: str,
    total_items: int,
) -> JobProgress:
    """Overall receipt-backed progress without requiring a manifest."""

    numbers = receipt_numbers(store, job_id)
    processed = 0
    if numbers:
        raw = store.read_bytes(receipt_key(job_id, numbers[-1]))
        if raw is not None:
            processed = int(json.loads(raw).get("end_index", 0))
    return JobProgress(
        total_items=total_items,
        processed_items=processed,
        completed_batches=len(numbers),
    )


def _source_label(entry) -> str:
    """Stable label for grouping manifest entries by configured source."""

    return str(entry.metadata.get("source_name") or entry.source or entry.doc_id)


# Batches beyond this window say little about the current speed, so rate
# estimation stays responsive and bounded in S3 reads.
MAX_RATE_TICKS = 32


def tick_numbers(store: S3DatasetStore, job_id: str) -> list[int]:
    """Batch numbers with a persisted tick sidecar, ascending."""

    numbers: list[int] = []
    for key in store.list_keys(f"jobs/{job_id}/progress/"):
        if not key.endswith(".tick.json"):
            continue
        stem = key.removesuffix(".tick.json").rsplit("_", 1)[-1]
        if stem.isdigit():
            numbers.append(int(stem))
    return sorted(numbers)


def _load_recent_ticks(
    store: S3DatasetStore,
    job_id: str,
    limit: int = MAX_RATE_TICKS,
) -> list[tuple[int, datetime]]:
    """End index and timestamp of the newest ticks, in processing order."""

    samples: list[tuple[int, datetime]] = []
    for number in tick_numbers(store, job_id)[-limit:]:
        raw = store.read_bytes(tick_key(job_id, number))
        if raw is None:
            continue
        try:
            data = json.loads(raw)
            processed_at = datetime.fromisoformat(data["processed_at"])
            samples.append((int(data["end_index"]), processed_at))
        except (KeyError, TypeError, ValueError):
            continue
    return samples


def median_rate(samples: list[tuple[int, datetime]]) -> float | None:
    """Items per second as the median of consecutive batch rates."""

    rates: list[float] = []
    for (end_a, at_a), (end_b, at_b) in zip(samples, samples[1:]):
        elapsed = (at_b - at_a).total_seconds()
        completed = end_b - end_a
        if elapsed > 0 and completed > 0:
            rates.append(completed / elapsed)
    if not rates:
        return None
    rates.sort()
    return rates[len(rates) // 2]


def granular_progress(
    store: S3DatasetStore,
    job_id: str,
    manifest: JobManifest,
    total_items: int | None = None,
) -> JobProgress:
    """Compute receipt-backed progress with per-source and rate breakdowns.

    Batches fill the manifest sequentially, so a completed receipt's
    ``end_index`` is the prefix of manifest entries already done. Aggregating
    that prefix against the manifest gives per-source numbers without reading
    receipt contents, and the batch ticks give a median items-per-second
    rate. Everything is derived from persisted state, so repeated polls and
    UI refreshes return the exact same numbers.
    """

    numbers = receipt_numbers(store, job_id)
    processed = 0
    if numbers:
        raw = store.read_bytes(receipt_key(job_id, numbers[-1]))
        if raw is not None:
            processed = min(
                int(json.loads(raw).get("end_index", 0)),
                len(manifest.entries),
            )
    total = total_items if total_items is not None else len(manifest.entries)

    by_source: dict[str, dict[str, int]] = {}
    for entry in manifest.entries:
        bucket = by_source.setdefault(_source_label(entry), {
            "total_items": 0,
            "processed_items": 0,
            "new": 0,
            "modified": 0,
            "deleted": 0,
        })
        bucket["total_items"] += 1
        bucket[str(entry.action.value)] += 1
    for entry in manifest.entries[:processed]:
        by_source[_source_label(entry)]["processed_items"] += 1

    rate = median_rate(_load_recent_ticks(store, job_id))
    remaining = max(0, total - processed)
    eta_seconds = remaining / rate if rate else None

    sources = [SourceProgress(name=name, **counts) for name, counts in by_source.items()]
    return JobProgress(
        total_items=total,
        processed_items=processed,
        completed_batches=len(numbers),
        items_per_second=round(rate, 2) if rate else None,
        eta_seconds=round(eta_seconds, 1) if eta_seconds is not None else None,
        sources=sources,
    )


def _job_is_orphaned(job_id: str, status: dict[str, Any]) -> bool:
    """True when a non-terminal job has no runner holding the host lock.

    The flock only lives while the submitting or runner process is alive, so
    a pending/running status with no lock holder means the runner is gone.
    """

    if status.get("status") not in {"pending", "running"}:
        return False
    active = DatasetJobLock.read_active() or {}
    return active.get("id") != job_id


def mark_interrupted(store: S3DatasetStore, status: dict[str, Any]) -> dict[str, Any]:
    """Record a non-terminal job whose runner is gone; safe to call again."""

    logger.warning("job runner is gone job_id=%s", status.get("id"))
    status["status"] = "interrupted"
    status["error"] = (
        "Indexing was interrupted before it finished. "
        "Start indexing again to pick up the remaining changes."
    )
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_status(store, status)
    return status


def load_current_job_status(
    store: S3DatasetStore,
    job_id: str,
) -> dict[str, Any] | None:
    """Load stored status, refreshed with granular progress and staleness.

    Reading a non-terminal job whose runner no longer holds the host lock
    marks it interrupted once; after that the persisted terminal status keeps
    every subsequent read (and every UI refresh) returning the same result.
    """

    status = load_job_status(store, job_id)
    if status is None:
        return None
    if _job_is_orphaned(job_id, status):
        status = mark_interrupted(store, status)
    manifest = load_manifest(store, job_id)
    if manifest is not None and status.get("status") in {"pending", "running"}:
        status["progress"] = granular_progress(store, job_id, manifest).model_dump(mode="json")
    return status
