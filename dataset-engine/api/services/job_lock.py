"""Host-wide lock that prevents overlapping indexing jobs."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import tempfile
from typing import Any


class JobAlreadyRunning(RuntimeError):
    """Raised when another indexing job owns the host lock."""


class DatasetJobLock:
    """Advisory file lock shared by API workers on one Linux host."""

    def __init__(self, descriptor: int) -> None:
        self._descriptor: int | None = descriptor

    @classmethod
    def acquire(cls) -> "DatasetJobLock":
        path = Path(os.getenv(
            "SAB_JOB_LOCK_PATH",
            str(Path(tempfile.gettempdir()) / "sab-dataset-engine-index.lock"),
        ))
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise JobAlreadyRunning("An indexing job is already running") from exc
        return cls(descriptor)

    def update(self, data: dict[str, Any]) -> None:
        if self._descriptor is None:
            raise RuntimeError("Cannot update a released indexing lock")
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        os.ftruncate(self._descriptor, 0)
        os.lseek(self._descriptor, 0, os.SEEK_SET)
        os.write(self._descriptor, payload)
        os.fsync(self._descriptor)

    def release(self) -> None:
        if self._descriptor is None:
            return
        descriptor, self._descriptor = self._descriptor, None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
