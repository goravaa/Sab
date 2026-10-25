"""Open and validate an indexed Sab dataset stored in S3."""

from __future__ import annotations

import json
import re
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


@dataclass
class OpenDataset:
    connection: DatasetConnection
    config: dict[str, Any]
    identifiers: dict[str, dict[str, Any]]
    chunk_count: int
    lance_dataset: Any = field(repr=False)

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


def open_dataset(connection: DatasetConnection) -> OpenDataset:
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
            return response["Body"].read()
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
        table = pq.read_table(pa.BufferReader(read("identifiers.parquet")))
        identifier_rows = table.to_pylist()
        identifiers: dict[str, dict[str, Any]] = {}
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
        return OpenDataset(connection, config, identifiers, lance_ds.count_rows(), lance_ds)
    except DatasetError:
        raise
    except Exception as exc:
        message = str(exc).lower()
        if "nosuchkey" in message or "404" in message or "not found" in message:
            raise DatasetError("Dataset files are missing; check the bucket, prefix, and dataset name") from exc
        raise DatasetError("Could not read or validate this dataset") from exc
