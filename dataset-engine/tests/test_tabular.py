from __future__ import annotations

import io

import pytest
from openpyxl import Workbook

from main.document.structured_data.tabular import (
    iter_tabular_documents,
    make_row,
    normalize_headers,
    row_to_document as tabular_row_to_document,
)
from main.schema.document import SourceType

from support import MemoryObjectConnector, source


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
