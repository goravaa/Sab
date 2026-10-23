"""Persistent record of which source items have already been indexed.

``identifiers.parquet`` is written only after a job finishes successfully. It
maps each source item to its source signature, the child document IDs it
produced, and the metadata needed to resolve the item again on a retry. It is
also the commit point for readers: each record names the ``job_id`` whose
chunks are authoritative and the ``config_hash`` those chunks were built with.
"""

from __future__ import annotations

import json

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from main.document.canonical import canonical_json
from main.process.store import DatasetStore


IDENTIFIERS_KEY = "identifiers.parquet"


@dataclass
class IdentifierRecord:
    source_id: str
    signature: str
    source_type: str
    source: str = ""
    doc_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    config_hash: str = ""
    job_id: str = ""


def _schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
        pa.field("source_type", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("doc_ids", pa.list_(pa.string()), nullable=False),
        pa.field("metadata", pa.string(), nullable=False),
        pa.field("config_hash", pa.string(), nullable=False),
        pa.field("job_id", pa.string(), nullable=False),
    ])


def parse_identifiers(data: bytes) -> dict[str, IdentifierRecord]:
    """Parse a serialized ``identifiers.parquet`` payload."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Identifier state requires pyarrow; install the 'pyarrow' package"
        ) from exc

    table = pq.read_table(pa.BufferReader(data))
    records: dict[str, IdentifierRecord] = {}

    for row in table.to_pylist():
        records[row["source_id"]] = IdentifierRecord(
            source_id=row["source_id"],
            signature=row["signature"],
            source_type=row["source_type"],
            source=row.get("source") or "",
            doc_ids=list(row.get("doc_ids") or []),
            metadata=json.loads(row.get("metadata") or "{}"),
            config_hash=row.get("config_hash") or "",
            job_id=row.get("job_id") or "",
        )

    return records


def load_identifiers(store: DatasetStore) -> dict[str, IdentifierRecord]:
    """Load identifiers, returning an empty mapping for a new dataset."""

    data = store.read_bytes(IDENTIFIERS_KEY)
    if data is None:
        return {}
    return parse_identifiers(data)


def write_identifiers(
    store: DatasetStore,
    records: Iterable[IdentifierRecord],
) -> None:
    """Write the complete identifier state as a deterministic Parquet file."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Identifier state requires pyarrow; install the 'pyarrow' package"
        ) from exc

    rows = [
        {
            "source_id": record.source_id,
            "signature": record.signature,
            "source_type": record.source_type,
            "source": record.source,
            "doc_ids": list(record.doc_ids),
            "metadata": canonical_json(record.metadata),
            "config_hash": record.config_hash,
            "job_id": record.job_id,
        }
        for record in sorted(records, key=lambda item: item.source_id)
    ]

    table = pa.Table.from_pylist(rows, schema=_schema())
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer, compression="snappy")
    store.write_bytes(IDENTIFIERS_KEY, buffer.getvalue().to_pybytes())
