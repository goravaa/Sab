from api.services.job_lock import DatasetJobLock


def test_read_active_returns_metadata_only_while_lock_is_held(tmp_path, monkeypatch):
    monkeypatch.setenv("SAB_JOB_LOCK_PATH", str(tmp_path / "active-job.lock"))
    assert DatasetJobLock.read_active() is None

    lock = DatasetJobLock.acquire()
    try:
        assert DatasetJobLock.read_active() == {}
        metadata = {
            "id": "job-123",
            "dataset": "supplier-demo",
            "target_uri": "s3://supplier-ai/dataset-engine",
            "status": "running",
        }
        lock.update(metadata)
        assert DatasetJobLock.read_active() == metadata
    finally:
        lock.release()

    assert DatasetJobLock.read_active() is None
