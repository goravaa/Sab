from __future__ import annotations

from collections import Counter

import psycopg
import pytest

from main.connect.database.postgres import PostgresConnector
from main.connect.object_storage.s3 import S3Config, S3Connector
from main.document.structured_data.database import row_to_document as db_row_to_document
from main.document.structured_data.tabular import iter_tabular_documents
from main.document.unstructured_data.documents import iter_unstructured_documents
from main.schema.document import SourceType
from main.schema.service import DatabaseConfig, S3Config

PREFIX = "supplier-demo/seed-0042/"


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def connectors(integration_data) -> tuple[PostgresConnector, S3Connector]:
    db = integration_data.postgres_config
    postgres = PostgresConnector(DatabaseConfig(
        type="postgres",
        host=db["host"],
        port=db["port"],
        database=db["dbname"],
        username=db["user"],
        password=db["password"],
        tables=[
            "supplier_demo.customers", "supplier_demo.products", "supplier_demo.orders",
            "supplier_demo.order_items", "supplier_demo.invoices", "supplier_demo.shipments",
            "supplier_demo.support_tickets",
        ],
    ))
    s3 = S3Connector(S3Config(
        type="s3",
        bucket=integration_data.s3_config["bucket"],
        access_key_id=integration_data.s3_config["access_key"],
        secret_access_key=integration_data.s3_config["secret_key"],
        endpoint_url=integration_data.s3_config["endpoint"],
        region="us-east-1",
        prefix=PREFIX,
    ))
    return postgres, s3


def test_postgres_connector_reads_expected_dataset(integration_data, connectors):
    postgres, _ = connectors
    postgres.check_connection()
    assert set(postgres.list_tables()) == {
        "supplier_demo.customers", "supplier_demo.invoices", "supplier_demo.order_items",
        "supplier_demo.orders", "supplier_demo.products", "supplier_demo.shipments",
        "supplier_demo.support_tickets",
    }

    expected = integration_data.manifest["counts"]
    for table, count in expected.items():
        db_table = "support_tickets" if table == "tickets" else table
        assert postgres.count_rows(f"supplier_demo.{db_table}") == count

    sample = next(postgres.iter_rows("supplier_demo.customers", batch_size=1))
    assert sample["customer_id"].startswith("CUST-0042-")
    assert postgres.primary_key_columns("supplier_demo.customers") == ["customer_id"]

    document = db_row_to_document(
        sample,
        namespace="supplier-demo",
        schema="supplier_demo",
        table="customers",
        primary_keys=("customer_id",),
    )
    repeated = db_row_to_document(
        sample,
        namespace="supplier-demo",
        schema="supplier_demo",
        table="customers",
        primary_keys=("customer_id",),
    )
    assert document.source_type == SourceType.DB_ROW
    assert document.doc_id == repeated.doc_id
    assert document.signature == repeated.signature
    assert document.metadata["row"]["customer_id"] == sample["customer_id"]


def test_postgres_foreign_keys_resolve_to_s3_objects(integration_data, object_by_key):
    config = integration_data.postgres_config
    with psycopg.connect(**config) as connection:
        linked_invoice_count = connection.execute(
            """
            SELECT count(*) FROM supplier_demo.invoices i
            JOIN supplier_demo.orders o USING (order_id)
            JOIN supplier_demo.customers c USING (customer_id)
            WHERE i.object_key LIKE %s
            """,
            (f"{PREFIX}%",),
        ).fetchone()[0]
        invoice_keys = connection.execute(
            "SELECT object_key FROM supplier_demo.invoices"
        ).fetchall()
        agreement_keys = connection.execute(
            "SELECT agreement_key FROM supplier_demo.customers"
        ).fetchall()
        ticket_keys = connection.execute(
            "SELECT document_key FROM supplier_demo.support_tickets"
        ).fetchall()
        tracking_keys = connection.execute(
            "SELECT DISTINCT tracking_export_key FROM supplier_demo.shipments"
        ).fetchall()

    assert linked_invoice_count == integration_data.manifest["counts"]["invoices"]
    for rows in (invoice_keys, agreement_keys, ticket_keys, tracking_keys):
        assert all(row[0] in object_by_key for row in rows)


def test_s3_connector_lists_and_streams_all_file_formats(
    integration_data,
    connectors,
    object_by_key,
    object_suffix_counts: Counter[str],
):
    _, s3 = connectors
    s3.check_connection()
    assert len(integration_data.objects) == 4474
    assert object_suffix_counts == Counter({".pdf": 2753, ".docx": 1718, ".csv": 1, ".xlsx": 1, ".json": 1})

    formats = {
        "customers/CUST-0042-000001/agreement.docx": b"PK",
        "invoices/INV-0042-0000001.pdf": b"%PDF-",
        "exports/carrier_tracking.csv": b"shipment_id,order_id,customer_id",
        "exports/catalog_inventory.xlsx": b"PK",
    }
    for relative_key, prefix_bytes in formats.items():
        full_key = f"{PREFIX}{relative_key}"
        with s3.open_object(full_key) as body:
            assert body.read(len(prefix_bytes)).startswith(prefix_bytes)
        assert full_key in object_by_key


def test_tracking_csv_builds_one_document_per_shipment(connectors, object_by_key, integration_data):
    _, s3 = connectors
    source = object_by_key[f"{PREFIX}exports/carrier_tracking.csv"]
    documents = list(iter_tabular_documents(s3, source, namespace="supplier-demo", id_column="shipment_id"))

    assert len(documents) == integration_data.manifest["counts"]["shipments"]
    assert len({document.doc_id for document in documents}) == len(documents)
    assert documents[0].source_type == SourceType.S3_OBJECT
    assert documents[0].metadata["row_identity"]["column"] == "shipment_id"
    assert documents[0].metadata["row"]["order_id"].startswith("ORD-0042-")


def test_excel_catalog_and_order_summary_are_readable(connectors, object_by_key, integration_data):
    _, s3 = connectors
    source = object_by_key[f"{PREFIX}exports/catalog_inventory.xlsx"]
    documents = iter_tabular_documents(s3, source, namespace="supplier-demo")
    rows = list(documents)

    assert len(rows) == integration_data.manifest["counts"]["products"] + integration_data.manifest["counts"]["orders"]
    assert rows[0].metadata["sheet"] == "Products"
    assert rows[0].metadata["row"]["sku"].startswith("TOOL-0042-")
    first_order = next(document for document in rows if document.metadata["sheet"] == "Order Summary")
    assert first_order.metadata["row"]["customer_id"].startswith("CUST-0042-")


def test_docx_agreement_extracts_headings_and_terms(connectors, object_by_key):
    _, s3 = connectors
    source = object_by_key[f"{PREFIX}customers/CUST-0042-000001/agreement.docx"]
    documents = list(iter_unstructured_documents(s3, source, namespace="supplier-demo"))

    assert len(documents) == 1
    document = documents[0]
    assert document.source_type == SourceType.S3_OBJECT
    assert document.raw_content.startswith("# Customer Supply Agreement")
    assert "# Commercial terms" in document.raw_content
    assert "CUST-0042-000001" in document.raw_content
    assert document.metadata["extension"] == ".docx"
    assert document.metadata["source_id"] == source.key


def test_pdf_invoice_extracts_text_and_page_count(connectors, object_by_key):
    _, s3 = connectors
    key = f"{PREFIX}invoices/INV-0042-0000006.pdf"
    documents = list(iter_unstructured_documents(s3, object_by_key[key], namespace="supplier-demo"))

    assert len(documents) == 1
    document = documents[0]
    assert "INVOICE" in document.raw_content
    assert "INV-0042-0000006" in document.raw_content
    assert document.metadata["extension"] == ".pdf"
    assert document.metadata["page_count"] >= 1
