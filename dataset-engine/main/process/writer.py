"""Write embedded chunks to a Lance dataset.

Lance is imported lazily so the rest of the engine (and its unit tests) can
run without the optional dependency installed.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from main.connect.object_storage.s3 import resolve_s3_credentials
from main.document.canonical import canonical_json
from main.schema.document import Chunk
from main.schema.service import S3Config


def lance_storage_options(config: S3Config) -> dict[str, str]:
    """Translate an S3Config into Lance/object-store options."""

    access_key_id, secret_access_key, session_token = resolve_s3_credentials(config)
    options = {
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }

    if session_token:
        options["session_token"] = session_token

    if config.region:
        options["region"] = config.region

    if config.endpoint_url:
        options["endpoint"] = config.endpoint_url
        if config.endpoint_url.startswith("http://"):
            options["allow_http"] = "true"

    return options


def chunks_to_table(chunks: list[Chunk]):
    """Build a typed Arrow table, including a fixed-size embedding vector."""

    import pyarrow as pa

    if not chunks:
        raise ValueError("chunks must not be empty")

    dimension = len(chunks[0].embedding or [])
    if dimension == 0:
        raise ValueError("chunks must have embeddings before they are written")

    for chunk in chunks:
        if len(chunk.embedding or []) != dimension:
            raise ValueError("all chunk embeddings must have the same dimension")

    schema = pa.schema([
        pa.field("chunk_id", pa.string(), nullable=False),
        pa.field("doc_id", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("job_id", pa.string(), nullable=False),
        pa.field("source_type", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("table_kind", pa.string(), nullable=False),
        pa.field("chunk_index", pa.int32(), nullable=False),
        pa.field("text", pa.string(), nullable=False),
        pa.field("header_path", pa.list_(pa.string()), nullable=False),
        pa.field("token_count", pa.int32(), nullable=False),
        pa.field("embedding", pa.list_(pa.float32(), dimension), nullable=False),
        pa.field("metadata", pa.string(), nullable=False),
    ])

    rows = [
        {
            "chunk_id": chunk.chunk_id,
            "doc_id": chunk.doc_id,
            "source_id": chunk.source_id or chunk.doc_id,
            "job_id": chunk.job_id or "",
            "source_type": chunk.source_type.value,
            "table_name": chunk.table_name or chunk.source_id or chunk.doc_id,
            "table_kind": chunk.table_kind or "document",
            "chunk_index": chunk.chunk_index,
            "text": chunk.text,
            "header_path": list(chunk.header_path or []),
            "token_count": chunk.token_count or 0,
            "embedding": [float(value) for value in (chunk.embedding or [])],
            "metadata": canonical_json(chunk.metadata),
        }
        for chunk in chunks
    ]

    return pa.Table.from_pylist(rows, schema=schema)


class LanceChunkWriter:
    """Append and delete chunks in a local or S3 Lance dataset."""

    def __init__(
        self,
        uri: str,
        *,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.uri = uri
        self.storage_options = storage_options

    def _dataset(self):
        import lance

        try:
            return lance.dataset(
                self.uri,
                storage_options=self.storage_options,
            )
        except Exception as exc:
            if _is_missing_dataset(exc):
                return None
            raise

    def append_chunks(self, chunks: Iterable[Chunk]) -> int:
        items = list(chunks)
        if not items:
            return 0

        import lance

        table = chunks_to_table(items)
        lance.write_dataset(
            table,
            self.uri,
            mode="append",
            storage_options=self.storage_options,
        )
        return len(items)

    def delete_source(self, source_id: str) -> None:
        dataset = self._dataset()
        if dataset is None:
            return
        dataset.delete(f"source_id = {_sql_literal(source_id)}")

    def delete_source_job(self, source_id: str, job_id: str) -> None:
        dataset = self._dataset()
        if dataset is None:
            return
        dataset.delete(
            f"source_id = {_sql_literal(source_id)} "
            f"AND job_id = {_sql_literal(job_id)}"
        )

    def delete_source_except_job(self, source_id: str, job_id: str) -> None:
        dataset = self._dataset()
        if dataset is None:
            return
        dataset.delete(
            f"source_id = {_sql_literal(source_id)} "
            f"AND job_id != {_sql_literal(job_id)}"
        )

    def delete_doc(self, doc_id: str) -> None:
        dataset = self._dataset()
        if dataset is None:
            return
        dataset.delete(f"doc_id = {_sql_literal(doc_id)}")


class MemoryChunkWriter:
    """In-memory writer with the same replacement semantics as Lance."""

    def __init__(self) -> None:
        self.by_source: dict[str, list[Chunk]] = {}

    def append_chunks(self, chunks: Iterable[Chunk]) -> int:
        items = list(chunks)
        for chunk in items:
            self.by_source.setdefault(
                chunk.source_id or chunk.doc_id,
                [],
            ).append(chunk)
        return len(items)

    def delete_source(self, source_id: str) -> None:
        self.by_source.pop(source_id, None)

    def delete_source_job(self, source_id: str, job_id: str) -> None:
        chunks = self.by_source.get(source_id)
        if chunks is None:
            return
        remaining = [
            chunk for chunk in chunks
            if chunk.job_id != job_id
        ]
        if remaining:
            self.by_source[source_id] = remaining
        else:
            self.by_source.pop(source_id, None)

    def delete_source_except_job(self, source_id: str, job_id: str) -> None:
        chunks = self.by_source.get(source_id)
        if chunks is None:
            return
        remaining = [
            chunk for chunk in chunks
            if chunk.job_id == job_id
        ]
        if remaining:
            self.by_source[source_id] = remaining
        else:
            self.by_source.pop(source_id, None)

    def delete_doc(self, doc_id: str) -> None:
        for source_id, chunks in list(self.by_source.items()):
            remaining = [chunk for chunk in chunks if chunk.doc_id != doc_id]
            if remaining:
                self.by_source[source_id] = remaining
            else:
                self.by_source.pop(source_id, None)

    def all_chunks(self) -> list[Chunk]:
        return [
            chunk
            for chunks in self.by_source.values()
            for chunk in chunks
        ]


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_missing_dataset(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "not found",
            "does not exist",
            "no such file",
            "nosuchkey",
        )
    )
