"""S3-compatible source connector (including MinIO with a custom endpoint)."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO
from main.schema.service import S3Config


@dataclass(frozen=True)
class SourceObject:
    """Metadata returned while listing objects in a source bucket."""

    key: str
    size: int
    etag: str | None = None
    last_modified: datetime | None = None


class S3Connector:
    """Read-only access to an S3 or S3-compatible source bucket."""

    def __init__(self, config: S3Config) -> None:
        self.config = config

    def _client(self):
        """Create the boto3 client lazily so other connectors need no AWS SDK."""
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError(
                "S3 support requires boto3; install the 'boto3' package"
            ) from exc

        return boto3.client(
            "s3",
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key.get_secret_value(),
            region_name=self.config.region,
            endpoint_url=self.config.endpoint_url,
        )

    def check_connection(self) -> None:
        """Raise the provider's error if the configured bucket is not accessible."""
        self._client().head_bucket(Bucket=self.config.bucket)

    def iter_objects(self, *, prefix: str | None = None) -> Iterator[SourceObject]:
        """Yield every object under the configured prefix, paging through results.

        Provider/listing errors propagate to the caller. A scan is complete only
        after this iterator is exhausted without an exception, which prevents a
        partial listing from being mistaken for deletions.
        """
        effective_prefix = self.config.prefix if prefix is None else prefix
        paginator = self._client().get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.config.bucket, Prefix=effective_prefix)

        for page in pages:
            for item in page.get("Contents", []):
                yield SourceObject(
                    key=item["Key"],
                    size=item["Size"],
                    # ETags are useful metadata but are not universal content
                    # hashes (for example, multipart uploads use composite ETags).
                    etag=item.get("ETag", "").strip('"') or None,
                    last_modified=item.get("LastModified"),
                )

    def read_object(self, key: str) -> bytes:
        """Read the full object body; callers can hash its bytes for a signature."""
        with self.open_object(key) as body:
            return body.read()

    @contextmanager
    def open_object(self, key: str) -> Iterator[BinaryIO]:
        """Open an object as a streaming body, closing it when the context exits.

        This avoids buffering the whole object in memory. Provider errors
        propagate to the caller; callers should consume the body inside the
        ``with`` block.
        """
        response = self._client().get_object(Bucket=self.config.bucket, Key=key)
        body = response["Body"]
        try:
            yield body
        finally:
            body.close()

    def object_count_and_size(self) -> tuple[int, int]:
        """Return object count and total bytes for the configured prefix."""
        count = total_size = 0
        for item in self.iter_objects():
            count += 1
            total_size += item.size
        return count, total_size
