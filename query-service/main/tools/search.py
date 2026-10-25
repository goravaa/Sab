"""Token-bounded semantic retrieval operations."""

from __future__ import annotations

import json

from ..schema import SearchResult
from ..search import search_chunks
from .dataset import configured_connection
from ..dataset import open_dataset


def semantic_search(question: str, top_k: int = 10) -> dict:
    """Return the requested nearest committed passages, up to a safe cap of 50.

    Choose a small top_k for exact lookups and increase it when the question
    needs broader evidence. Tabular rows are returned as compact row objects.
    """
    if not question.strip():
        raise ValueError("question must not be empty")
    if not 1 <= top_k <= 50:
        raise ValueError("top_k must be between 1 and 50")

    dataset = open_dataset(configured_connection())
    hits = search_chunks(dataset, question, limit=top_k)
    results: list[dict] = []
    for rank, hit in enumerate(hits, start=1):
        metadata = hit.get("metadata") or "{}"
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except ValueError:
                metadata = {}
        row = metadata.get("row") if isinstance(metadata, dict) else None
        results.append(SearchResult(
            rank=rank,
            chunk_id=hit.get("chunk_id") or "",
            doc_id=hit.get("doc_id") or "",
            source_id=hit.get("source_id") or "",
            source_type=hit.get("source_type") or "unknown",
            source_label=(metadata.get("source_label") if isinstance(metadata, dict) else None),
            row_number=(metadata.get("row_number") if isinstance(metadata, dict) else None),
            sheet=(metadata.get("sheet") if isinstance(metadata, dict) else None),
            chunk_index=int(hit.get("chunk_index") or 0),
            text="" if isinstance(row, dict) else (hit.get("text") or "")[:1200],
            row=row if isinstance(row, dict) else None,
            distance=hit.get("_distance"),
        ).model_dump(mode="json"))

    return {
        "query": question,
        "search_type": "semantic",
        "requested_depth": top_k,
        "available_committed_chunks": dataset.committed_chunk_count,
        "returned": len(results),
        "results": results,
    }


def keyword_search(query: str, limit: int = 10) -> dict:
    """Search committed chunk text and tabular values by literal keywords, without embeddings."""
    if not query.strip():
        raise ValueError("query must not be empty")
    if not 1 <= limit <= 20:
        raise ValueError("limit must be between 1 and 20")

    terms = query.casefold().split()
    dataset = open_dataset(configured_connection())
    matches = []
    for chunk in dataset.committed_text_chunks():
        metadata = chunk.get("metadata") or {}
        row = metadata.get("row") if isinstance(metadata, dict) else None
        haystack = " ".join((chunk.get("text") or "", json.dumps(row, ensure_ascii=False) if isinstance(row, dict) else "")).casefold()
        if not all(term in haystack for term in terms):
            continue
        matches.append({
            "chunk_id": chunk.get("chunk_id") or "",
            "doc_id": chunk.get("doc_id") or "",
            "source_id": chunk.get("source_id") or "",
            "source_type": chunk.get("source_type") or "unknown",
            "source": metadata.get("source_label"),
            "row_number": metadata.get("row_number"),
            "sheet": metadata.get("sheet"),
            "text": "" if isinstance(row, dict) else (chunk.get("text") or "")[:1200],
            "row": row if isinstance(row, dict) else None,
        })
    return {
        "query": query,
        "search_type": "keyword",
        "matching_chunks": len(matches),
        "returned": min(len(matches), limit),
        "truncated": len(matches) > limit,
        "results": matches[:limit],
    }
