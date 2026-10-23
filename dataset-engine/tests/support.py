from __future__ import annotations

import io
from contextlib import contextmanager

from main.connect.object_storage.s3 import SourceObject


class TrackedBytesIO(io.BytesIO):
    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self.closed_by_connector = False

    def close(self) -> None:
        self.closed_by_connector = True
        super().close()


class MemoryObjectConnector:
    """S3Connector-shaped test double that tracks streaming body cleanup."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.bodies: list[TrackedBytesIO] = []

    @contextmanager
    def open_object(self, key: str):
        body = TrackedBytesIO(self.objects[key])
        self.bodies.append(body)
        try:
            yield body
        finally:
            body.close()


def source(key: str) -> SourceObject:
    return SourceObject(key=key, size=0)
