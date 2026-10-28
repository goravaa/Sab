import json
from datetime import datetime, timedelta, timezone

from api.services.jobs import (
    granular_progress,
    load_current_job_status,
    median_rate,
    progress_from_receipts,
)
from main.process.runner import save_tick
from main.process.store import MemoryDatasetStore
from main.schema.document import SourceType
from main.schema.job import DiffAction, JobManifest, ManifestEntry


class ReceiptStore:
    def __init__(self):
        self.read = []

    def list_keys(self, prefix):
        assert prefix == "jobs/job-1/progress/"
        return [
            "jobs/job-1/progress/batch_10000.json",
            "jobs/job-1/progress/batch_9999.json",
        ]

    def read_bytes(self, key):
        self.read.append(key)
        end_index = 10_000 if key.endswith("10000.json") else 9_999
        return json.dumps({"end_index": end_index}).encode()


def test_progress_reads_only_the_numerically_latest_receipt():
    store = ReceiptStore()

    progress = progress_from_receipts(store, "job-1", total_items=10_100)

    assert progress.processed_items == 10_000
    assert progress.completed_batches == 2
    assert store.read == ["jobs/job-1/progress/batch_10000.json"]


def _entry(number, label, action):
    return ManifestEntry(
        doc_id=f"doc-{number}",
        signature=f"sig-{number}",
        source_type=SourceType.S3_OBJECT,
        action=action,
        source=f"s3://{label}/obj-{number}",
        metadata={"source_name": label, "key": f"obj-{number}"},
    )


def _manifest(entries):
    return JobManifest(
        job_id="job-granular",
        dataset="demo",
        created_at="2026-10-27T00:00:00+00:00",
        entries=entries,
    )


def _store_with_receipts(store, job_id, ends):
    for batch_number, end_index in enumerate(ends, start=1):
        store.write_bytes(
            f"jobs/{job_id}/progress/batch_{batch_number:04d}.json",
            json.dumps({"batch_number": batch_number, "end_index": end_index}).encode(),
        )


def _build_store(job_id, extra=0):
    # A: 3 new + 1 deleted, B: 2 modified. Manifest order groups A first.
    entries = []
    entries.extend(_entry(i, "bucket-a", DiffAction.NEW) for i in range(3))
    entries.extend(_entry(i, "bucket-b", DiffAction.MODIFIED) for i in range(3, 5))
    entries.append(_entry(5, "bucket-a", DiffAction.DELETED))
    entries.extend(_entry(100 + i, "bucket-a", DiffAction.NEW) for i in range(extra))
    manifest = _manifest(entries)
    store = MemoryDatasetStore()
    store.write_bytes(
        f"jobs/{job_id}/manifest.json",
        manifest.model_dump_json().encode(),
    )
    return store, manifest


def test_granular_progress_aggregates_per_source_from_manifest_prefix():
    store, manifest = _build_store("job-granular")
    # One batch finished: the 3 new bucket-a entries.
    _store_with_receipts(store, "job-granular", [3])

    progress = granular_progress(store, "job-granular", manifest)

    assert (progress.processed_items, progress.completed_batches) == (3, 1)
    by_name = {source.name: source for source in progress.sources}
    assert set(by_name) == {"bucket-a", "bucket-b"}
    a = by_name["bucket-a"]
    assert (a.total_items, a.processed_items) == (4, 3)
    assert (a.new, a.modified, a.deleted) == (3, 0, 1)
    b = by_name["bucket-b"]
    assert (b.total_items, b.processed_items) == (2, 0)
    assert (b.new, b.modified, b.deleted) == (0, 2, 0)
    assert round(a.ratio, 2) == 0.75


def test_granular_progress_is_the_same_for_repeated_refreshes():
    store, manifest = _build_store("job-granular")
    _store_with_receipts(store, "job-granular", [3, 6])

    progress = granular_progress(store, "job-granular", manifest)
    again = granular_progress(store, "job-granular", manifest)

    assert progress.model_dump() == again.model_dump()
    assert progress.processed_items == 6
    by_name = {source.name: source for source in progress.sources}
    assert by_name["bucket-a"].processed_items == 4
    assert by_name["bucket-b"].processed_items == 2
    assert all(source.ratio == 1.0 for source in progress.sources)


def test_granular_progress_clamps_receipts_past_the_manifest(tmp_path):
    store, manifest = _build_store("job-granular")
    _store_with_receipts(store, "job-granular", [99])

    progress = granular_progress(store, "job-granular", manifest)

    assert progress.processed_items == len(manifest.entries)


def test_load_current_job_status_marks_orphans_interrupted(monkeypatch, tmp_path):
    monkeypatch.setenv("SAB_JOB_LOCK_PATH", str(tmp_path / "lock"))
    store, _ = _build_store("job-orphan")
    store.write_bytes(
        "jobs/job-orphan/status.json",
        json.dumps({
            "id": "job-orphan",
            "dataset": "demo",
            "status": "running",
            "progress": {"total_items": 6, "processed_items": 0, "completed_batches": 0},
        }).encode(),
    )

    status = load_current_job_status(store, "job-orphan")

    assert status["status"] == "interrupted"
    assert "Start indexing again" in status["error"]
    # Reading twice yields the same terminal state; refresh is idempotent.
    persisted = json.loads(store.read_bytes("jobs/job-orphan/status.json"))
    assert persisted["status"] == "interrupted"
    again = load_current_job_status(store, "job-orphan")
    assert again["status"] == "interrupted"
    assert again == status


def test_load_current_job_status_keeps_running_jobs_running(monkeypatch, tmp_path):
    from api.services.job_lock import DatasetJobLock

    monkeypatch.setenv("SAB_JOB_LOCK_PATH", str(tmp_path / "lock"))
    store, manifest = _build_store("job-live")
    _store_with_receipts(store, "job-live", [3])
    store.write_bytes(
        "jobs/job-live/status.json",
        json.dumps({
            "id": "job-live",
            "dataset": "demo",
            "status": "running",
            "progress": {"total_items": 6, "processed_items": 0, "completed_batches": 0},
        }).encode(),
    )
    lock = DatasetJobLock.acquire()
    try:
        lock.update({"id": "job-live", "status": "running"})

        status = load_current_job_status(store, "job-live")
    finally:
        lock.release()

    assert status["status"] == "running"
    assert status["progress"]["processed_items"] == 3
    assert {row["name"] for row in status["progress"]["sources"]} == {"bucket-a", "bucket-b"}


def test_granular_progress_reports_median_rate_and_eta_from_ticks():
    store, manifest = _build_store("job-eta", extra=6)
    _store_with_receipts(store, "job-eta", [3, 6, 8])
    start = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    save_tick(store, "job-eta", 1, 3, start)
    save_tick(store, "job-eta", 2, 6, start + timedelta(seconds=12))
    save_tick(store, "job-eta", 3, 8, start + timedelta(seconds=60))

    progress = granular_progress(store, "job-eta", manifest, total_items=12)

    # Rates 0.25/s and 2/54 item/s; the median is 0.25/s.
    assert progress.items_per_second == 0.25
    assert progress.processed_items == 8
    assert progress.eta_seconds == 16.0


def test_granular_progress_without_tick_history_has_no_eta():
    store, manifest = _build_store("job-noticks")
    _store_with_receipts(store, "job-noticks", [3])

    progress = granular_progress(store, "job-noticks", manifest)

    assert progress.items_per_second is None
    assert progress.eta_seconds is None
    assert progress.processed_items == 3


def test_median_rate_uses_consecutive_batch_intervals():
    start = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    samples = [
        (0, start),
        (10, start + timedelta(seconds=5)),
        (10, start + timedelta(seconds=105)),
        (30, start + timedelta(seconds=125)),
    ]

    # Intervals yield rates [1.0, 2.0]; the implementation takes the upper
    # middle when the count is even, i.e. the most recent interval wins ties.
    assert median_rate(samples) == 2.0
