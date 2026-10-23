from __future__ import annotations

from datetime import date

from main.document.structured_data.database import row_to_document as database_row_to_document
from main.schema.document import SourceType


def test_database_document_identity_uses_primary_key_and_signature_tracks_row():
    row = {
        "customer_id": "C-1",
        "company_name": "Acme",
        "created_at": date(2026, 1, 2),
        "payload": b"x",
    }
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
