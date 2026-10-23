import hashlib
import json
from collections.abc import Mapping
from typing import Any

from main.schema.document import Document, SourceType


def row_to_document(
    row: Mapping[str, Any],
    *,
    namespace: str,
    schema: str,
    table: str,
    primary_keys: tuple[str, ...],
) -> Document:
    """Convert a database row into a deterministic Document."""

    key_values = {key: row[key] for key in primary_keys}

    def canonicalize(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(k): canonicalize(v)
                for k, v in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if isinstance(value, (list, tuple)):
            return [canonicalize(v) for v in value]
        if hasattr(value, "isoformat"):
            return value.isoformat()
        if isinstance(value, bytes):
            return {"$bytes": value.hex()}
        return value

    def serialize(value: Any) -> str:
        return json.dumps(
            canonicalize(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )

    # Stable identity: row edits must not change the document ID.
    doc_id = serialize([
        namespace,
        schema,
        table,
        [[key, canonicalize(row[key])] for key in primary_keys],
    ])

    # Human-readable content for embedding.
    raw_content = "\n".join(
        f"{column}: {serialize(value)}"
        for column, value in row.items()
        if column not in primary_keys
    )

    # Hash the complete row so any relevant edit is detectable.
    canonical_row = {
        "version": 1,
        "schema": schema,
        "table": table,
        "columns": [
            [column, canonicalize(value)]
            for column, value in row.items()
        ],
    }

    signature = hashlib.sha256(
        serialize(canonical_row).encode("utf-8")
    ).hexdigest()

    return Document(
        doc_id=doc_id,
        source_type=SourceType.DB_ROW,
        raw_content=raw_content,
        metadata={
            "namespace": namespace,
            "schema": schema,
            "table": table,
            "primary_keys": list(primary_keys),
            "key_values": canonicalize(key_values),
            "row": canonicalize(dict(row)),
        },
        signature=signature,
    )
