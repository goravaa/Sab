"""Dataset-bucket storage used for state, manifests, and Lance data.

The central bucket is the single source of truth for a dataset. This module
keeps object-store access in one place so the processor can be tested against
an in-memory store without touching S3.
"""

from __future__ import annotations

import mimetypes

from collections.abc import Iterator
from typing import Protocol

from main.connect.object_storage.s3 import build_s3_client
from main.schema.service import S3Config


class DatasetStore(Protocol):
    """Minimal object-store surface needed by the processor."""

    def read_bytes(self, relative_key: str) -> bytes | None: ...

    def write_bytes(self, relative_key: str, data: bytes) -> None: ...

    def delete(self, relative_key: str) -> None: ...

    def exists(self, relative_key: str) -> bool: ...

    def uri(self, relative_key: str) -> str: ...


class S3DatasetStore:
    """Read/write access to one dataset beneath the central S3 prefix."""

    def __init__(self, config: S3Config, dataset: str) -> None:
        self.config = config
        self.dataset = dataset
        self.root = "/".join(
            part.strip("/")
            for part in (config.prefix, dataset)
            if part.strip("/")
        )
        self._client_instance = None

    def _client(self):
        if self._client_instance is not None:
            return self._client_instance

        self._client_instance = build_s3_client(self.config)
        return self._client_instance

    def _key(self, relative_key: str) -> str:
        relative = relative_key.lstrip("/")
        return f"{self.root}/{relative}" if self.root else relative

    def uri(self, relative_key: str) -> str:
        return f"s3://{self.config.bucket}/{self._key(relative_key)}"

    def read_bytes(self, relative_key: str) -> bytes | None:
        try:
            response = self._client().get_object(
                Bucket=self.config.bucket,
                Key=self._key(relative_key),
            )
        except Exception as exc:
            if _is_missing(exc):
                return None
            raise
        return response["Body"].read()

    def write_bytes(self, relative_key: str, data: bytes) -> None:
        key = self._key(relative_key)
        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        self._client().put_object(
            Bucket=self.config.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    def delete(self, relative_key: str) -> None:
        try:
            self._client().delete_object(
                Bucket=self.config.bucket,
                Key=self._key(relative_key),
            )
        except Exception as exc:
            if not _is_missing(exc):
                raise

    def exists(self, relative_key: str) -> bool:
        try:
            self._client().head_object(
                Bucket=self.config.bucket,
                Key=self._key(relative_key),
            )
        except Exception as exc:
            if _is_missing(exc):
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        paginator = self._client().get_paginator("list_objects_v2")
        full_prefix = self._key(prefix)
        root_prefix = f"{self.root}/" if self.root else ""
        for page in paginator.paginate(
            Bucket=self.config.bucket,
            Prefix=full_prefix,
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if root_prefix and key.startswith(root_prefix):
                    key = key[len(root_prefix):]
                yield key


class MemoryDatasetStore:
    """In-memory DatasetStore for contract tests and local dry runs."""

    def __init__(self, dataset: str = "test-dataset") -> None:
        self.dataset = dataset
        self.objects: dict[str, bytes] = {}

    def uri(self, relative_key: str) -> str:
        return f"memory://{self.dataset}/{relative_key.lstrip('/')}"

    def read_bytes(self, relative_key: str) -> bytes | None:
        return self.objects.get(relative_key.lstrip("/"))

    def write_bytes(self, relative_key: str, data: bytes) -> None:
        self.objects[relative_key.lstrip("/")] = data

    def delete(self, relative_key: str) -> None:
        self.objects.pop(relative_key.lstrip("/"), None)

    def exists(self, relative_key: str) -> bool:
        return relative_key.lstrip("/") in self.objects

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        prefix = prefix.lstrip("/")
        for key in sorted(self.objects):
            if key.startswith(prefix):
                yield key


def _is_missing(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}:
            return True
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return True

    text = str(exc).lower()
    return "not found" in text or "no such key" in text or "does not exist" in text
