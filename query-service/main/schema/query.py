"""Typed domain results shared by the MCP tools."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class DatasetSummary(BaseModel):
    name: str
    uri: str
    source_count: int
    sources: list[dict[str, str]]
    source_item_count: int
    document_count: int
    chunk_count: int
    embedding_model: str | None = None


class SearchResult(BaseModel):
    rank: int = Field(ge=1)
    chunk_id: str
    doc_id: str
    source_id: str
    source_type: str
    source_label: str | None = None
    row_number: int | None = None
    sheet: str | None = None
    chunk_index: int
    text: str
    row: dict[str, Any] | None = None
    distance: float | None = None


class IntegrityReport(BaseModel):
    status: Literal["ok", "warning"]
    source_items: int
    expected_documents: int
    observed_documents: int
    expected_chunks: int
    observed_committed_chunks: int
    missing_documents: int
    missing_chunks: int
    ignored_stale_or_orphan_chunks: int
    duplicate_document_ids: int
    duplicate_chunk_ids: int


class AggregateResult(BaseModel):
    operation: str
    field: str | None = None
    group_by: str | None = None
    matched_rows: int
    result: Any
    truncated_groups: bool = False


class RowSearchResult(BaseModel):
    matched_rows: int
    returned_rows: int
    rows: list[dict[str, Any]]
    truncated: bool
