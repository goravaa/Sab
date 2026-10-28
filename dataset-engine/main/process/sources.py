"""Scan source systems and resolve manifest entries back into Documents."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from main.connect.database.postgres import PostgresConnector
from main.connect.object_storage.s3 import S3Connector, SourceObject
from main.document.canonical import canonicalize, sha256_json
from main.document.structured_data import database as database_documents
from main.document.structured_data import tabular
from main.document.unstructured_data import documents as unstructured
from main.process.config import DatasetConfig, SourceConfig
from main.schema.document import Document, SourceType
from main.schema.job import DiffAction, ManifestEntry


ALL_S3_EXTENSIONS = (
    unstructured.SUPPORTED_EXTENSIONS | tabular.SUPPORTED_EXTENSIONS
)


@dataclass(frozen=True)
class SourceItem:
    """One independently reconcilable source item (row or object)."""

    source_id: str
    signature: str
    source_type: SourceType
    source: str
    metadata: dict[str, Any]


def object_doc_id(namespace: str, source_id: str) -> str:
    """Return the stable parent ID for an S3 object."""

    return sha256_json({
        "version": 1,
        "namespace": namespace,
        "source_id": source_id,
        "kind": "object",
    })


def build_connectors(config: DatasetConfig) -> dict[str, Any]:
    """Create one connector per configured source."""

    connectors: dict[str, Any] = {}

    for source in config.sources:
        connection = source.connection

        if connection.type == "postgres":
            connectors[source.name] = PostgresConnector(connection)
        elif connection.type == "s3":
            connectors[source.name] = S3Connector(connection)
        else:
            raise NotImplementedError(
                f"Source {source.name!r} uses unsupported type {connection.type!r}"
            )

    return connectors


def _s3_extension(key: str) -> str | None:
    lowered = key.lower()
    return next(
        (extension for extension in ALL_S3_EXTENSIONS if lowered.endswith(extension)),
        None,
    )


def _source_namespace(source: SourceConfig) -> str:
    return str(source.options.get("namespace") or source.name)


def _iter_database_items(
    source: SourceConfig,
    connector: PostgresConnector,
) -> Iterator[SourceItem]:
    namespace = _source_namespace(source)

    for table in connector.list_tables():
        schema, _, table_name = table.rpartition(".")
        schema = schema or "public"
        primary_keys = tuple(connector.primary_key_columns(table))

        if not primary_keys:
            raise ValueError(
                f"table {table!r} has no primary key; stable row IDs are required"
            )

        for row in connector.iter_rows(table):
            document = database_documents.row_to_document(
                row,
                namespace=namespace,
                schema=schema,
                table=table_name,
                primary_keys=primary_keys,
            )
            yield SourceItem(
                source_id=document.doc_id,
                signature=document.signature,
                source_type=SourceType.DB_ROW,
                source=f"postgres://{source.name}/{table}",
                metadata={
                    "source_name": source.name,
                    "namespace": namespace,
                    "schema": schema,
                    "table": table,
                    "primary_keys": list(primary_keys),
                    "key_values": document.metadata["key_values"],
                },
            )


def _iter_s3_items(
    source: SourceConfig,
    connector: S3Connector,
) -> Iterator[SourceItem]:
    namespace = _source_namespace(source)
    id_column = source.options.get("id_column")

    for obj in connector.iter_objects():
        extension = _s3_extension(obj.key)
        if extension is None:
            continue

        signature = obj.etag or sha256_json({
            "size": obj.size,
            "last_modified": (
                obj.last_modified.isoformat() if obj.last_modified else None
            ),
        })

        yield SourceItem(
            source_id=object_doc_id(namespace, obj.key),
            signature=signature,
            source_type=SourceType.S3_OBJECT,
            source=f"s3://{connector.config.bucket}/{obj.key}",
            metadata={
                "source_name": source.name,
                "namespace": namespace,
                "key": obj.key,
                "extension": extension,
                "tabular": extension in tabular.SUPPORTED_EXTENSIONS,
                "id_column": id_column,
                "size": obj.size,
                "etag": obj.etag,
                "last_modified": (
                    obj.last_modified.isoformat() if obj.last_modified else None
                ),
            },
        )


def iter_source_items(
    config: DatasetConfig,
    connectors: Mapping[str, Any] | None = None,
) -> Iterator[SourceItem]:
    """Scan every configured source completely.

    The iterator is intentionally lazy, but callers must exhaust it without an
    exception before treating missing items as deletions.
    """

    connectors = connectors or build_connectors(config)

    for source in config.sources:
        connector = connectors[source.name]

        if source.connection.type == "postgres":
            yield from _iter_database_items(source, connector)
        elif source.connection.type == "s3":
            yield from _iter_s3_items(source, connector)


class SourceLoader:
    """Resolve a manifest entry into the Documents it represents."""

    def __init__(
        self,
        config: DatasetConfig,
        connectors: Mapping[str, Any] | None = None,
    ) -> None:
        self.config = config
        self.connectors = connectors or build_connectors(config)

    def load(self, entry: ManifestEntry) -> Iterator[Document]:
        metadata = entry.metadata
        connector = self.connectors[metadata["source_name"]]

        if entry.source_type == SourceType.DB_ROW:
            key_values = {
                key: _restore_key_value(value)
                for key, value in metadata["key_values"].items()
            }
            row = connector.fetch_row(metadata["table"], key_values)
            yield database_documents.row_to_document(
                row,
                namespace=metadata["namespace"],
                schema=metadata["schema"],
                table=metadata["table"],
                primary_keys=tuple(metadata["primary_keys"]),
            )
            return

        source = SourceObject(
            key=metadata["key"],
            size=int(metadata.get("size") or 0),
            etag=metadata.get("etag"),
            last_modified=_parse_datetime(metadata.get("last_modified")),
        )

        if metadata.get("tabular"):
            yield from tabular.iter_tabular_documents(
                connector,
                source,
                namespace=metadata["namespace"],
                id_column=metadata.get("id_column"),
            )
        else:
            yield from unstructured.iter_unstructured_documents(
                connector,
                source,
                namespace=metadata["namespace"],
            )

    def load_batch(self, entries: list[ManifestEntry]) -> dict[str, list[Document]]:
        """Resolve database entries in one query per source table."""
        from collections import defaultdict

        groups: dict[tuple[str, str], list[ManifestEntry]] = defaultdict(list)
        result: dict[str, list[Document]] = {}
        for entry in entries:
            if entry.action == DiffAction.DELETED:
                continue
            if entry.source_type == SourceType.DB_ROW:
                groups[(entry.metadata["source_name"], entry.metadata["table"])].append(entry)
            else:
                result[entry.doc_id] = list(self.load(entry))

        for (source_name, table), group in groups.items():
            connector = self.connectors[source_name]
            primary_keys = tuple(group[0].metadata["primary_keys"])
            key_values = [
                {key: _restore_key_value(value) for key, value in entry.metadata["key_values"].items()}
                for entry in group
            ]
            rows = connector.fetch_rows(table, key_values, primary_keys=primary_keys)
            for entry, keys in zip(group, key_values):
                row = rows.get(tuple(keys[key] for key in primary_keys))
                if row is None:
                    raise KeyError(f"row in {table!r} with key {keys!r} no longer exists")
                result[entry.doc_id] = [database_documents.row_to_document(
                    row, namespace=entry.metadata["namespace"],
                    schema=entry.metadata["schema"], table=table.rsplit(".", 1)[-1],
                    primary_keys=primary_keys,
                )]
        return result


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _restore_key_value(value: Any) -> Any:
    """Undo tagged values produced by ``canonicalize`` for SQL parameters."""

    if not isinstance(value, dict) or len(value) != 1:
        return value

    tag, raw = next(iter(value.items()))

    if tag == "$uuid":
        return UUID(raw)
    if tag == "$date":
        return date.fromisoformat(raw)
    if tag == "$datetime":
        return datetime.fromisoformat(raw)
    if tag == "$time":
        return time.fromisoformat(raw)
    if tag == "$decimal":
        return Decimal(raw)
    if tag == "$bytes":
        return bytes.fromhex(raw)

    return value


def source_item_from_document(
    document: Document,
    *,
    source_name: str,
    source: str,
    signature: str,
) -> SourceItem:
    """Build a SourceItem for tests or custom source adapters."""

    metadata = canonicalize(document.metadata)
    return SourceItem(
        source_id=document.doc_id,
        signature=signature,
        source_type=document.source_type,
        source=source,
        metadata={"source_name": source_name, **metadata},
    )
