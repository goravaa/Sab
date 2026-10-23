from __future__ import annotations

import io
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from openpyxl import Workbook

from main.connect.object_storage.s3 import SourceObject
from main.document.structured_data.database import row_to_document as database_row_to_document
from main.document.structured_data.tabular import (
    canonical_json,
    canonicalize,
    iter_tabular_documents,
    make_row,
    normalize_headers,
    row_to_document as tabular_row_to_document,
    sha256_json,
)
from main.schema.document import SourceType


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


def test_canonicalize_handles_common_scalar_and_nested_types():
    sample_uuid = UUID("12345678-1234-5678-1234-567812345678")
    result = canonicalize({
        "number": Decimal("12.30"),
        "date": date(2026, 10, 22),
        "timestamp": datetime(2026, 10, 22, 10, 30, tzinfo=timezone.utc),
        "time": time(10, 30),
        "uuid": sample_uuid,
        "bytes": b"\x00\xff",
        "items": (1, float("nan"), float("inf")),
    })

    assert result == {
        "bytes": {"$bytes": "00ff"},
        "date": {"$date": "2026-10-22"},
        "items": [1, None, {"$float": "inf"}],
        "number": {"$decimal": "12.30"},
        "time": {"$time": "10:30:00"},
        "timestamp": {"$datetime": "2026-10-22T10:30:00+00:00"},
        "uuid": {"$uuid": str(sample_uuid)},
    }


def test_canonical_json_and_hash_ignore_mapping_insertion_order():
    left = {"z": 1, "nested": {"b": 2, "a": 3}}
    right = {"nested": {"a": 3, "b": 2}, "z": 1}

    assert canonical_json(left) == canonical_json(right)
    assert sha256_json(left) == sha256_json(right)
    assert canonical_json(left) == '{"nested":{"a":3,"b":2},"z":1}'


def test_canonicalize_fails_for_unknown_objects_instead_of_stringifying_them():
    with pytest.raises(TypeError, match="Unsupported value type"):
        canonicalize(object())


def test_normalize_headers_makes_blank_duplicate_and_colliding_names_unique():
    assert normalize_headers([" Name ", "", "Name", "column_2", "", None]) == [
        "Name", "column_2", "Name_2", "column_2_2", "column_5", "column_6",
    ]


def test_make_row_fills_missing_cells_and_ignores_extras():
    assert make_row(["id", "name", "region"], ["C-1", "Acme", "west", "ignored"]) == {
        "id": "C-1", "name": "Acme", "region": "west",
    }
    assert make_row(["id", "name", "region"], ["C-2"]) == {
        "id": "C-2", "name": None, "region": None,
    }


def test_tabular_document_keeps_identity_stable_but_changes_signature_with_content():
    base = {"shipment_id": "S-1", "status": "in transit", "delivered_on": None}
    original = tabular_row_to_document(
        base, namespace="supplier", source_id="tracking.csv", row_number=2,
        id_column="shipment_id",
    )
    updated = tabular_row_to_document(
        {**base, "status": "delivered"}, namespace="supplier", source_id="tracking.csv",
        row_number=8, id_column="shipment_id",
    )

    assert original.source_type == SourceType.S3_OBJECT
    assert original.doc_id == updated.doc_id
    assert original.signature != updated.signature
    assert original.metadata["row_identity"] == {
        "type": "column", "column": "shipment_id", "value": "S-1",
    }
    assert "shipment_id:" in original.raw_content


def test_tabular_document_uses_row_number_when_id_is_empty_and_requires_configured_column():
    first = tabular_row_to_document(
        {"id": "", "name": "Acme"}, namespace="supplier", source_id="a.csv",
        row_number=2, id_column="id",
    )
    second = tabular_row_to_document(
        {"id": "", "name": "Acme"}, namespace="supplier", source_id="a.csv",
        row_number=3, id_column="id",
    )
    assert first.doc_id != second.doc_id
    assert first.metadata["row_identity"] == {"type": "row_number", "value": 2}

    with pytest.raises(ValueError, match="Configured ID column 'missing' was not found"):
        tabular_row_to_document(
            {"id": "C-1"}, namespace="supplier", source_id="a.csv", row_number=2,
            id_column="missing",
        )


def test_database_document_identity_uses_primary_key_and_signature_tracks_row():
    row = {"customer_id": "C-1", "company_name": "Acme", "created_at": date(2026, 1, 2), "payload": b"x"}
    original = database_row_to_document(
        row, namespace="supplier", schema="supplier_demo", table="customers",
        primary_keys=("customer_id",),
    )
    updated = database_row_to_document(
        {**row, "company_name": "Acme Industrial"}, namespace="supplier",
        schema="supplier_demo", table="customers", primary_keys=("customer_id",),
    )

    assert original.source_type == SourceType.DB_ROW
    assert original.doc_id == updated.doc_id
    assert original.signature != updated.signature
    assert original.metadata["row"]["created_at"] == "2026-01-02"
    assert original.metadata["row"]["payload"] == {"$bytes": "78"}
    assert "customer_id" not in original.raw_content


def test_csv_reader_normalizes_headers_skips_blank_rows_and_closes_stream():
    content = b'\xef\xbb\xbfID,Name,Name,\nS-1,"Acme, Inc.",West,\nS-2,Beta\n,,,\n'
    connector = MemoryObjectConnector({"tracking.csv": content})

    documents = list(iter_tabular_documents(
        connector, source("tracking.csv"), namespace="supplier", id_column="ID",
    ))

    assert len(documents) == 2
    assert documents[0].metadata["row"] == {
        "ID": "S-1", "Name": "Acme, Inc.", "Name_2": "West", "column_4": "",
    }
    assert documents[1].metadata["row"] == {
        "ID": "S-2", "Name": "Beta", "Name_2": None, "column_4": None,
    }
    assert all(body.closed_by_connector for body in connector.bodies)


@pytest.mark.parametrize("content", [b"", b"id,name\n"])
def test_csv_reader_handles_empty_and_header_only_files(content: bytes):
    connector = MemoryObjectConnector({"empty.csv": content})
    assert list(iter_tabular_documents(connector, source("empty.csv"))) == []
    assert connector.bodies[0].closed_by_connector


def test_csv_reader_closes_stream_when_conversion_fails():
    connector = MemoryObjectConnector({"bad.csv": b"name\nAcme\n"})
    with pytest.raises(ValueError, match="Configured ID column 'id' was not found"):
        list(iter_tabular_documents(
            connector, source("bad.csv"), id_column="id",
        ))
    assert connector.bodies[0].closed_by_connector


def test_excel_reader_processes_each_sheet_and_builds_documents():
    workbook = Workbook()
    products = workbook.active
    products.title = "Products"
    products.append(["sku", "price", "active"])
    products.append(["TOOL-1", 12.5, True])
    products.append(["TOOL-2", 21.0, False])
    orders = workbook.create_sheet("Orders")
    orders.append(["order_id", "customer_id"])
    orders.append(["ORD-1", "CUST-1"])
    buffer = io.BytesIO()
    workbook.save(buffer)

    connector = MemoryObjectConnector({"snapshot.xlsx": buffer.getvalue()})
    documents = list(iter_tabular_documents(
        connector, source("snapshot.xlsx"), namespace="supplier",
    ))

    assert len(documents) == 3
    assert [doc.metadata["sheet"] for doc in documents] == ["Products", "Products", "Orders"]
    assert documents[0].metadata["row"]["sku"] == "TOOL-1"
    assert documents[-1].metadata["row"]["customer_id"] == "CUST-1"
    assert all(doc.source_type == SourceType.S3_OBJECT for doc in documents)
    assert connector.bodies[0].closed_by_connector


def test_dispatch_rejects_unsupported_file_extensions():
    connector = MemoryObjectConnector({})
    with pytest.raises(ValueError, match="Unsupported tabular file"):
        list(iter_tabular_documents(connector, source("contract.pdf")))
