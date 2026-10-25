"""Open and validate an indexed Sab dataset stored in S3."""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field, SecretStr, field_validator


class DatasetConnection(BaseModel):
    bucket: str = Field(min_length=1, max_length=255)
    dataset: str = Field(min_length=1, max_length=128)
    prefix: str = ""
    region: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None

    @field_validator("dataset")
    @classmethod
    def safe_dataset_name(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
            raise ValueError("dataset must be a simple name without path separators")
        if value in {".", ".."}:
            raise ValueError("dataset name is invalid")
        return value

    @field_validator("prefix")
    @classmethod
    def safe_prefix(cls, value: str) -> str:
        parts = [part for part in value.strip("/").split("/") if part]
        if any(part in {".", ".."} for part in parts):
            raise ValueError("prefix cannot contain relative path segments")
        return "/".join(parts)

    @property
    def root(self) -> str:
        return "/".join(part for part in (self.prefix, self.dataset) if part)

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.root}"

    def boto_options(self) -> dict[str, Any]:
        if bool(self.access_key_id) != bool(self.secret_access_key):
            raise DatasetError("Provide both S3 credentials, or leave both blank")
        kwargs: dict[str, Any] = {"region_name": self.region, "endpoint_url": self.endpoint_url}
        if self.access_key_id and self.secret_access_key:
            kwargs["aws_access_key_id"] = self.access_key_id
            kwargs["aws_secret_access_key"] = self.secret_access_key.get_secret_value()
        return kwargs

    def lance_options(self) -> dict[str, str]:
        options: dict[str, str] = {}
        if self.access_key_id and self.secret_access_key:
            options["access_key_id"] = self.access_key_id
            options["secret_access_key"] = self.secret_access_key.get_secret_value()
        if self.region:
            options["region"] = self.region
        if self.endpoint_url:
            options["endpoint"] = self.endpoint_url
            if self.endpoint_url.startswith("http://"):
                options["allow_http"] = "true"
        return options


class DatasetError(RuntimeError):
    """Dataset is missing or cannot safely be queried."""


class IdentifierStore:
    """Disk-backed source commit lookup used by streaming scans."""

    def __init__(self, path: str) -> None:
        # DuckDB may pull Arrow batches from its worker thread after the reader
        # is registered; a single SQLite connection serves those serial lookups.
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute(
            "CREATE TABLE identifiers (source_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, config_hash TEXT NOT NULL)"
        )

    def insert_many(self, rows: list[tuple[str, str, str]]) -> None:
        self.connection.executemany(
            "INSERT INTO identifiers VALUES (?, ?, ?)", rows,
        )

    def get(self, source_id: str) -> dict[str, str] | None:
        row = self.connection.execute(
            "SELECT job_id, config_hash FROM identifiers WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if row is None:
            return None
        return {"job_id": row[0], "config_hash": row[1]}

    def get_many(self, source_ids: list[str]) -> dict[str, dict[str, str]]:
        if not source_ids:
            return {}
        placeholders = ",".join("?" for _ in source_ids)
        rows = self.connection.execute(
            f"SELECT source_id, job_id, config_hash FROM identifiers WHERE source_id IN ({placeholders})",
            source_ids,
        ).fetchall()
        return {
            source_id: {"job_id": job_id, "config_hash": config_hash}
            for source_id, job_id, config_hash in rows
        }

    def close(self) -> None:
        self.connection.close()


def _table_name_from_metadata(metadata: dict[str, Any]) -> str:
    schema = metadata.get("schema")
    table = metadata.get("table")
    if table:
        return f"{schema}.{table}" if schema else str(table)
    source = str(metadata.get("source_label") or metadata.get("source_id") or "")
    if source.lower().endswith((".xlsx", ".xlsm")) and metadata.get("sheet"):
        return f"{source}::{metadata['sheet']}"
    return source


def _table_kind_from_metadata(metadata: dict[str, Any], table_name: str) -> str:
    if metadata.get("table"):
        return "postgres_table"
    lowered = table_name.lower()
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith((".xlsx", ".xlsm")):
        return "xlsx"
    return "document"


@dataclass
class OpenDataset:
    connection: DatasetConnection
    config: dict[str, Any]
    identifiers: dict[str, dict[str, Any]]
    chunk_count: int
    lance_dataset: Any = field(repr=False)
    identifier_store: IdentifierStore | None = field(default=None, repr=False)
    temp_directory: tempfile.TemporaryDirectory[str] | None = field(default=None, repr=False)

    def __enter__(self) -> "OpenDataset":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        if self.identifier_store is not None:
            self.identifier_store.close()
            self.identifier_store = None
        if self.temp_directory is not None:
            self.temp_directory.cleanup()
            self.temp_directory = None

    def _identifier(self, source_id: str | None) -> dict[str, Any] | None:
        if not source_id:
            return None
        if self.identifier_store is not None:
            return self.identifier_store.get(source_id)
        return self.identifiers.get(source_id)

    @property
    def lance_columns(self) -> set[str]:
        return set(self.lance_dataset.schema.names)

    @property
    def committed_chunk_count(self) -> int:
        return sum(
            int(record.get("metadata", {}).get("chunk_count", 0) or 0)
            for record in self.identifiers.values()
        )

    @property
    def summary(self) -> dict[str, Any]:
        sources = self.config.get("sources", [])
        return {
            "name": self.config.get("dataset", self.connection.dataset),
            "uri": self.connection.uri,
            "source_count": len(sources),
            "sources": [
                {"name": source.get("name", "Source"), "type": source.get("connection", {}).get("type", "unknown")}
                for source in sources
            ],
            "source_item_count": len(self.identifiers),
            "document_count": len({
                doc_id
                for record in self.identifiers.values()
                for doc_id in record.get("doc_ids", [])
            }),
            "chunk_count": self.chunk_count,
            "embedding_model": self.config.get("data_model", {}).get("embedder", {}).get("model"),
        }

    def search(self, vector: list[float], *, limit: int = 6) -> list[dict[str, Any]]:
        # Expand the candidate set when uncommitted/orphaned chunks are filtered.
        total = self.chunk_count
        candidate_count = min(max(limit * 4, limit), total)
        accepted: list[dict[str, Any]] = []
        seen: set[str] = set()
        committed_docs = {
            source_id: set(record.get("doc_ids") or [])
            for source_id, record in self.identifiers.items()
        }
        while candidate_count:
            try:
                candidates = self.lance_dataset.to_table(
                    nearest={
                        "column": "embedding",
                        "q": vector,
                        "k": candidate_count,
                        "metric": "cosine",
                    },
                ).to_pylist()
            except Exception as exc:
                raise DatasetError("Vector search failed for this dataset") from exc
            accepted = []
            seen.clear()
            for hit in candidates:
                source_id = hit.get("source_id")
                committed = self.identifiers.get(source_id)
                if not committed:
                    continue
                if hit.get("job_id") != committed.get("job_id"):
                    continue
                if hit.get("doc_id") not in committed_docs.get(source_id, set()):
                    continue
                if committed.get("config_hash") != self.config.get("config_hash"):
                    continue
                chunk_id = hit.get("chunk_id")
                if chunk_id in seen:
                    continue
                seen.add(chunk_id)
                accepted.append(hit)
                if len(accepted) >= limit:
                    break
            if len(accepted) >= limit or candidate_count >= total:
                break
            candidate_count = min(total, candidate_count * 2)
        return accepted[:limit]

    def committed_chunks(self) -> list[dict[str, Any]]:
        """Read chunk metadata and retain only the committed job/doc versions."""
        table = self.lance_dataset.to_table(
            columns=["chunk_id", "doc_id", "source_id", "job_id", "source_type", "chunk_index", "metadata"],
        )
        committed: list[dict[str, Any]] = []
        committed_docs = {
            source_id: set(record.get("doc_ids") or [])
            for source_id, record in self.identifiers.items()
        }
        for chunk in table.to_pylist():
            record = self.identifiers.get(chunk.get("source_id"))
            if not record or chunk.get("job_id") != record.get("job_id"):
                continue
            if chunk.get("doc_id") not in committed_docs.get(chunk.get("source_id"), set()):
                continue
            if record.get("config_hash") != self.config.get("config_hash"):
                continue
            metadata = chunk.get("metadata") or "{}"
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except ValueError:
                    metadata = {}
            chunk["metadata"] = metadata if isinstance(metadata, dict) else {}
            committed.append(chunk)
        return committed

    def committed_text_chunks(self) -> list[dict[str, Any]]:
        """Read committed chunk text for lexical search, without loading vectors."""
        table = self.lance_dataset.to_table(
            columns=["chunk_id", "doc_id", "source_id", "job_id", "source_type", "chunk_index", "text", "metadata"],
        )
        committed: list[dict[str, Any]] = []
        committed_docs = {
            source_id: set(record.get("doc_ids") or [])
            for source_id, record in self.identifiers.items()
        }
        for chunk in table.to_pylist():
            record = self.identifiers.get(chunk.get("source_id"))
            if not record or chunk.get("job_id") != record.get("job_id"):
                continue
            if chunk.get("doc_id") not in committed_docs.get(chunk.get("source_id"), set()):
                continue
            if record.get("config_hash") != self.config.get("config_hash"):
                continue
            metadata = chunk.get("metadata") or "{}"
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except ValueError:
                    metadata = {}
            chunk["metadata"] = metadata if isinstance(metadata, dict) else {}
            committed.append(chunk)
        return committed

    def structured_rows(self) -> list[dict[str, Any]]:
        """Return de-duplicated structured rows from committed tabular chunks."""
        rows: list[dict[str, Any]] = []
        seen_docs: set[str] = set()
        for chunk in self.committed_chunks():
            doc_id = chunk.get("doc_id")
            metadata = chunk.get("metadata") or {}
            row = metadata.get("row")
            if not isinstance(row, dict) or not doc_id or doc_id in seen_docs:
                continue
            seen_docs.add(doc_id)
            rows.append({
                "doc_id": doc_id,
                "source_id": chunk.get("source_id"),
                "source_type": chunk.get("source_type"),
                "source_label": metadata.get("source_label"),
                "row_number": metadata.get("row_number"),
                "sheet": metadata.get("sheet"),
                "row": row,
            })
        return rows

    def iter_structured_rows(
        self,
        *,
        table_name: str | None = None,
        table_kind: str | None = None,
        namespace: str | None = None,
        batch_size: int = 512,
    ):
        """Yield committed structured rows from Lance in bounded batches.

        `chunk_index = 0` yields one representative chunk per source row, even
        when a long row was split into several text chunks.
        """
        available = self.lance_columns
        columns = [
            name for name in (
                "chunk_id", "doc_id", "source_id", "job_id", "source_type",
                "chunk_index", "table_name", "table_kind", "metadata",
            ) if name in available
        ]
        predicates = ["chunk_index = 0"]
        if table_name is not None and "table_name" in available:
            safe_name = table_name.replace("'", "''")
            predicates.append(f"table_name = '{safe_name}'")
        if table_kind is not None and "table_kind" in available:
            safe_kind = table_kind.replace("'", "''")
            predicates.append(f"table_kind = '{safe_kind}'")
        filter_expr = " AND ".join(predicates)
        try:
            batches = self.lance_dataset.to_batches(
                columns=columns,
                filter=filter_expr,
                batch_size=batch_size,
                batch_readahead=1,
                fragment_readahead=1,
                io_buffer_size=64 * 1024 * 1024,
            )
            committed_docs: dict[str, set[str]] = {}
            for batch in batches:
                batch_chunks = batch.to_pylist()
                if self.identifier_store is not None:
                    batch_source_ids = list({
                        chunk["source_id"]
                        for chunk in batch_chunks
                        if chunk.get("source_id")
                    })
                    batch_commits = self.identifier_store.get_many(batch_source_ids)
                else:
                    batch_commits = {}
                for chunk in batch_chunks:
                    source_id = chunk.get("source_id")
                    record = (
                        batch_commits.get(source_id)
                        if self.identifier_store is not None
                        else self._identifier(source_id)
                    )
                    if not record or chunk.get("job_id") != record.get("job_id"):
                        continue
                    # Streaming identifier state stores one commit marker per
                    # source on disk. The committed job is the authority for
                    # all rows/chunks emitted by that source in that job.
                    if self.identifier_store is None:
                        if source_id not in committed_docs:
                            committed_docs[source_id] = set(record.get("doc_ids") or [])
                        if chunk.get("doc_id") not in committed_docs[source_id]:
                            continue
                    if record.get("config_hash") != self.config.get("config_hash"):
                        continue
                    metadata = chunk.get("metadata") or "{}"
                    if isinstance(metadata, str):
                        try:
                            metadata = json.loads(metadata)
                        except ValueError:
                            metadata = {}
                    if not isinstance(metadata, dict):
                        metadata = {}
                    row = metadata.get("row")
                    if not isinstance(row, dict):
                        continue
                    resolved_name = chunk.get("table_name") or _table_name_from_metadata(metadata)
                    resolved_kind = chunk.get("table_kind") or _table_kind_from_metadata(metadata, resolved_name)
                    resolved_namespace = metadata.get("namespace") or ""
                    if table_name is not None and resolved_name != table_name:
                        continue
                    if table_kind is not None and resolved_kind != table_kind:
                        continue
                    if namespace is not None and resolved_namespace != namespace:
                        continue
                    yield {
                        "doc_id": chunk.get("doc_id"),
                        "source_id": source_id,
                        "source_type": chunk.get("source_type"),
                        "table_name": resolved_name,
                        "table_kind": resolved_kind,
                        "namespace": resolved_namespace,
                        "source_label": metadata.get("source_label"),
                        "row_number": metadata.get("row_number"),
                        "sheet": metadata.get("sheet"),
                        "row": row,
                    }
        except Exception as exc:
            raise DatasetError("Could not scan committed structured rows") from exc

    def integrity_report(self) -> dict[str, Any]:
        """Compare committed identifiers with visible Lance chunks."""
        expected_doc_list = [
            doc_id
            for record in self.identifiers.values()
            for doc_id in record.get("doc_ids", [])
        ]
        expected_doc_ids = set(expected_doc_list)
        duplicate_document_ids = len(expected_doc_list) - len(expected_doc_ids)
        expected_chunks = sum(
            int(record.get("metadata", {}).get("chunk_count", 0) or 0)
            for record in self.identifiers.values()
        )
        observed = self.committed_chunks()
        observed_docs = {chunk.get("doc_id") for chunk in observed}
        chunk_ids = [chunk.get("chunk_id") for chunk in observed]
        observed_by_source: dict[str, int] = {}
        for chunk in observed:
            source_id = chunk.get("source_id")
            observed_by_source[source_id] = observed_by_source.get(source_id, 0) + 1
        missing_chunks = sum(
            max(
                int(record.get("metadata", {}).get("chunk_count", 0) or 0)
                - observed_by_source.get(source_id, 0),
                0,
            )
            for source_id, record in self.identifiers.items()
        )
        duplicate_chunk_ids = len(chunk_ids) - len(set(chunk_ids))
        missing_documents = len(expected_doc_ids - observed_docs)
        ignored_stale = self.chunk_count - len(observed)
        return {
            "status": "warning" if missing_documents or missing_chunks or duplicate_document_ids or duplicate_chunk_ids else "ok",
            "source_items": len(self.identifiers),
            "expected_documents": len(expected_doc_ids),
            "observed_documents": len(observed_docs),
            "expected_chunks": expected_chunks,
            "observed_committed_chunks": len(observed),
            "missing_documents": missing_documents,
            "missing_chunks": missing_chunks,
            "ignored_stale_or_orphan_chunks": max(ignored_stale, 0),
            "duplicate_document_ids": duplicate_document_ids,
            "duplicate_chunk_ids": duplicate_chunk_ids,
        }


def open_dataset(
    connection: DatasetConnection,
    *,
    streaming_identifiers: bool = False,
) -> OpenDataset:
    temp_directory: tempfile.TemporaryDirectory[str] | None = None
    identifier_store: IdentifierStore | None = None
    try:
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq

        client = boto3.client("s3", **connection.boto_options())
        def read(key: str) -> bytes:
            try:
                response = client.get_object(
                    Bucket=connection.bucket,
                    Key=f"{connection.root}/{key}",
                )
            except Exception as exc:
                message = str(exc).lower()
                if key == "identifiers.parquet" and (
                    "nosuchkey" in message or "404" in message or "not found" in message
                ):
                    raise DatasetError(
                        "Indexing has not committed yet; identifiers.parquet is not available"
                    ) from exc
                raise
            body = response["Body"]
            try:
                return body.read()
            finally:
                body.close()
        config = json.loads(read("config.json"))
        if config.get("version") != 1:
            raise DatasetError("Unsupported dataset config version")
        if config.get("dataset") != connection.dataset:
            raise DatasetError("Dataset name does not match config.json")
        expected_uri = connection.uri.rstrip("/")
        stored_uri = (config.get("target_uri") or "").rstrip("/")
        if stored_uri and stored_uri != expected_uri:
            raise DatasetError("Dataset location does not match config.json")
        config_hash = config.get("config_hash")
        if not config_hash:
            raise DatasetError("config.json is missing its config hash")
        identifiers: dict[str, dict[str, Any]] = {}
        if streaming_identifiers:
            temp_directory = tempfile.TemporaryDirectory(prefix="sab-query-")
            parquet_path = f"{temp_directory.name}/identifiers.parquet"
            db_path = f"{temp_directory.name}/identifiers.sqlite3"
            try:
                response = client.get_object(
                    Bucket=connection.bucket,
                    Key=f"{connection.root}/identifiers.parquet",
                )
            except Exception as exc:
                message = str(exc).lower()
                if "nosuchkey" in message or "404" in message or "not found" in message:
                    raise DatasetError(
                        "Indexing has not committed yet; identifiers.parquet is not available"
                    ) from exc
                raise
            body = response["Body"]
            try:
                with open(parquet_path, "wb") as local_file:
                    shutil.copyfileobj(body, local_file, length=8 * 1024 * 1024)
            finally:
                body.close()
            parquet = pq.ParquetFile(parquet_path)
            identifier_store = IdentifierStore(db_path)
            identifier_store.connection.execute("PRAGMA journal_mode = OFF")
            identifier_store.connection.execute("PRAGMA synchronous = OFF")
            identifier_store.connection.execute("PRAGMA cache_size = -8192")
            for batch in parquet.iter_batches(
                columns=["source_id", "job_id", "config_hash"],
                batch_size=8192,
            ):
                rows = list(zip(
                    batch.column("source_id").to_pylist(),
                    batch.column("job_id").to_pylist(),
                    batch.column("config_hash").to_pylist(),
                ))
                if any(not source_id or not job_id or row_hash != config_hash for source_id, job_id, row_hash in rows):
                    raise DatasetError("Committed identifier state is incomplete or inconsistent")
                try:
                    identifier_store.insert_many(rows)
                except sqlite3.IntegrityError as exc:
                    raise DatasetError("identifiers.parquet contains a duplicate source_id") from exc
                identifier_store.connection.commit()
            import os
            os.unlink(parquet_path)
        else:
            table = pq.read_table(pa.BufferReader(read("identifiers.parquet")))
            identifier_rows = table.to_pylist()
            for row in identifier_rows:
                metadata = row.get("metadata") or "{}"
                try:
                    row["metadata"] = json.loads(metadata) if isinstance(metadata, str) else metadata
                except (TypeError, json.JSONDecodeError) as exc:
                    raise DatasetError(
                        f"Identifier {row.get('source_id')!r} has invalid metadata JSON"
                    ) from exc
                if not isinstance(row["metadata"], dict):
                    raise DatasetError(
                        f"Identifier {row.get('source_id')!r} metadata must be an object"
                    )
                source_id = row.get("source_id")
                if not source_id or source_id in identifiers:
                    raise DatasetError("identifiers.parquet contains a missing or duplicate source_id")
                identifiers[source_id] = row
            for row in identifiers.values():
                if not row.get("job_id") or row.get("config_hash") != config_hash:
                    raise DatasetError("Committed identifier state is incomplete or inconsistent")
        import lance
        lance_ds = lance.dataset(
            f"{connection.uri}/data.lance",
            storage_options=connection.lance_options() or None,
        )
        return OpenDataset(
            connection,
            config,
            identifiers,
            lance_ds.count_rows(),
            lance_ds,
            identifier_store=identifier_store,
            temp_directory=temp_directory,
        )
    except DatasetError:
        if identifier_store is not None:
            identifier_store.close()
        if temp_directory is not None:
            temp_directory.cleanup()
        raise
    except Exception as exc:
        if identifier_store is not None:
            identifier_store.close()
        if temp_directory is not None:
            temp_directory.cleanup()
        message = str(exc).lower()
        if "nosuchkey" in message or "404" in message or "not found" in message:
            raise DatasetError("Dataset files are missing; check the bucket, prefix, and dataset name") from exc
        raise DatasetError("Could not read or validate this dataset") from exc
