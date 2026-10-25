from __future__ import annotations

import pytest

from main.process.chunking import chunk_document
from main.schema.document import Document, SourceType
from main.schema.service import ChunkConfig


def _document(text: str) -> Document:
    return Document(
        doc_id="doc-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT,
        raw_content=text,
        metadata={"source_id": "contract.pdf"},
    )


def test_small_document_becomes_one_contextual_chunk():
    chunks = chunk_document(
        _document("# Terms\n\nPayment is due in thirty days."),
        ChunkConfig(chunk_size=50, chunk_overlap=5),
        source_id="source-1",
        job_id="job-1",
    )

    assert len(chunks) == 1
    assert chunks[0].source_id == "source-1"
    assert chunks[0].job_id == "job-1"
    assert chunks[0].header_path == ["Terms"]
    assert chunks[0].text.startswith("Source: contract.pdf > Terms")
    assert "Payment is due" in chunks[0].text


def test_markdown_sections_and_long_blocks_split_with_header_paths():
    text = (
        "# Agreement\n\nIntro paragraph.\n\n"
        "## Payment\n\n" + " ".join(f"word{i}" for i in range(80))
    )

    chunks = chunk_document(
        _document(text),
        ChunkConfig(chunk_size=20, chunk_overlap=5),
    )

    assert len(chunks) > 2
    assert chunks[0].header_path == ["Agreement"]
    payment_chunks = [chunk for chunk in chunks if chunk.header_path == ["Agreement", "Payment"]]
    assert len(payment_chunks) > 1
    assert all("Agreement > Payment" in chunk.text for chunk in payment_chunks)
    assert [chunk.chunk_index for chunk in chunks] == list(range(len(chunks)))


def test_chunk_ids_are_deterministic_for_the_same_content():
    document = _document("# Terms\n\nPayment is due in thirty days.")
    config = ChunkConfig(chunk_size=50, chunk_overlap=5)

    first = chunk_document(document, config)
    second = chunk_document(document, config)

    assert [chunk.chunk_id for chunk in first] == [chunk.chunk_id for chunk in second]


def test_empty_document_produces_no_chunks():
    assert chunk_document(_document("   \n\n")) == []


def test_postgres_chunks_use_schema_and_table_as_identity():
    document = Document(
        doc_id="row-1",
        signature="sig-1",
        source_type=SourceType.DB_ROW,
        raw_content="name: Alice",
        metadata={
            "schema": "supplier_demo",
            "table": "customers",
            "row": {"customer_id": "CUST-1", "name": "Alice"},
        },
    )

    chunk = chunk_document(
        document,
        ChunkConfig(chunk_size=50),
        source_id="row-1",
        source_label="postgres://primary/supplier_demo.customers",
    )[0]

    assert chunk.table_name == "supplier_demo.customers"
    assert chunk.table_kind == "postgres_table"


def test_csv_chunks_use_the_object_key_as_identity():
    document = Document(
        doc_id="row-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT,
        raw_content="order_id: A-1",
        metadata={
            "source_id": "exports/orders.csv",
            "row": {"order_id": "A-1"},
            "row_number": 2,
        },
    )

    chunk = chunk_document(
        document,
        ChunkConfig(chunk_size=50),
        source_id="file-1",
        source_label="exports/orders.csv",
    )[0]

    assert chunk.table_name == "exports/orders.csv"
    assert chunk.table_kind == "csv"


def test_excel_chunks_include_the_sheet_name():
    document = Document(
        doc_id="row-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT,
        raw_content="sku: A-1",
        metadata={
            "source_id": "exports/catalog.xlsx",
            "sheet": "Products",
            "row": {"sku": "A-1"},
            "row_number": 2,
        },
    )

    chunk = chunk_document(
        document,
        ChunkConfig(chunk_size=50),
        source_id="file-1",
        source_label="exports/catalog.xlsx",
    )[0]

    assert chunk.table_name == "exports/catalog.xlsx::Products"
    assert chunk.table_kind == "xlsx"


@pytest.mark.parametrize("suffix", [".pdf", ".docx", ".md", ".txt"])
def test_document_chunks_use_the_object_key_as_identity(suffix):
    key = f"documents/contract{suffix}"
    document = Document(
        doc_id="doc-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT,
        raw_content="Contract text.",
        metadata={"source_id": key, "extension": suffix},
    )

    chunk = chunk_document(
        document,
        ChunkConfig(chunk_size=50),
        source_id="file-1",
        source_label=key,
    )[0]

    assert chunk.table_name == key
    assert chunk.table_kind == "document"
