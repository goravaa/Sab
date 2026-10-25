"""Deterministic Markdown-aware chunking for Documents.

The configuration expresses sizes in tokens, while this implementation uses
whitespace-delimited words as a dependency-free approximation. That keeps the
splitter usable before a model-specific tokenizer is selected and is accurate
enough for the default 512-token chunks.
"""

from __future__ import annotations

import re

from dataclasses import dataclass

from main.document.canonical import sha256_json
from main.schema.document import Chunk, Document
from main.schema.service import ChunkConfig


_HEADING = re.compile(r"^(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$")
_PARAGRAPH = re.compile(r"\n\s*\n")
_SENTENCE = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class _Section:
    header_path: list[str]
    body: str


def _word_count(text: str) -> int:
    return len(text.split())


def _last_words(text: str, count: int) -> str:
    if count <= 0:
        return ""
    return " ".join(text.split()[-count:])


def _sections(text: str) -> list[_Section]:
    """Split text at Markdown headings while retaining the heading path."""

    sections: list[_Section] = []
    path: list[str] = []
    lines: list[str] = []

    def flush() -> None:
        body = "\n".join(lines).strip()
        if body:
            sections.append(_Section(header_path=list(path), body=body))

    for line in text.splitlines():
        match = _HEADING.match(line)
        if match is None:
            lines.append(line)
            continue

        flush()
        level = len(match.group(1))
        title = match.group(2).strip()
        path = path[: level - 1] + [title]
        lines = [line]

    flush()

    if not sections and text.strip():
        sections.append(_Section(header_path=[], body=text.strip()))

    return sections


def _split_units(text: str, max_words: int) -> list[str]:
    """Recursively split a block until every unit fits the word budget."""

    text = text.strip()
    if not text:
        return []
    if _word_count(text) <= max_words:
        return [text]

    paragraphs = [part.strip() for part in _PARAGRAPH.split(text) if part.strip()]
    if len(paragraphs) > 1:
        units: list[str] = []
        for paragraph in paragraphs:
            units.extend(_split_units(paragraph, max_words))
        return units

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 1:
        units = []
        for line in lines:
            units.extend(_split_units(line, max_words))
        return units

    sentences = [part.strip() for part in _SENTENCE.split(text) if part.strip()]
    if len(sentences) > 1:
        units = []
        for sentence in sentences:
            units.extend(_split_units(sentence, max_words))
        return units

    words = text.split()
    return [
        " ".join(words[start:start + max_words])
        for start in range(0, len(words), max_words)
    ]


def _pack_units(units: list[str], max_words: int, overlap: int) -> list[str]:
    """Pack units into chunks, carrying a word overlap between chunks."""

    chunks: list[str] = []
    current: list[str] = []
    current_words = 0

    for unit in units:
        unit_words = _word_count(unit)

        if current and current_words + unit_words > max_words:
            chunk_text = "\n\n".join(current).strip()
            if chunk_text:
                chunks.append(chunk_text)

            tail = _last_words(chunk_text, overlap)
            current = [tail] if tail else []
            current_words = _word_count(tail)

        current.append(unit)
        current_words += unit_words

    if current:
        chunk_text = "\n\n".join(current).strip()
        if chunk_text:
            chunks.append(chunk_text)

    return chunks


def _table_identity(
    document: Document,
    source_id: str | None,
    source_label: str | None,
) -> tuple[str, str]:
    schema = document.metadata.get("schema")
    table = document.metadata.get("table")

    if table:
        name = f"{schema}.{table}" if schema else str(table)
        return name, "postgres_table"

    key = str(
        source_label
        or document.metadata.get("source_id")
        or source_id
        or document.doc_id
    )
    lowered = key.lower()

    if lowered.endswith(".csv"):
        return key, "csv"

    if lowered.endswith((".xlsx", ".xlsm")):
        sheet = document.metadata.get("sheet")
        return (f"{key}::{sheet}" if sheet else key), "xlsx"

    return key, "document"


def _source_label(
    document: Document,
    source_id: str | None,
    source_label: str | None,
) -> str:
    if source_label:
        return source_label

    source = document.metadata.get("source_id")
    if source:
        return str(source)

    schema = document.metadata.get("schema")
    table = document.metadata.get("table")
    if schema and table:
        return f"{schema}.{table}"

    return source_id or document.doc_id


def chunk_document(
    document: Document,
    config: ChunkConfig | None = None,
    *,
    source_id: str | None = None,
    source_label: str | None = None,
    job_id: str | None = None,
) -> list[Chunk]:
    """Turn one Document into deterministic, context-rich chunks."""

    config = config or ChunkConfig()
    max_words = max(config.chunk_size, 1)
    overlap = min(max(config.chunk_overlap, 0), max(max_words - 1, 0))
    label = _source_label(document, source_id, source_label)
    table_name, table_kind = _table_identity(document, source_id, source_label)

    chunks: list[Chunk] = []

    for section in _sections(document.raw_content):
        units: list[str] = []
        for paragraph in _PARAGRAPH.split(section.body):
            units.extend(_split_units(paragraph, max_words))

        for body in _pack_units(units, max_words, overlap):
            context = f"Source: {label}"
            if section.header_path:
                context = f"{context} > {' > '.join(section.header_path)}"

            text = f"{context}\n\n{body}".strip()
            chunk_index = len(chunks)

            chunks.append(Chunk(
                chunk_id=sha256_json({
                    "version": 1,
                    "doc_id": document.doc_id,
                    "chunk_index": chunk_index,
                    "text": text,
                }),
                doc_id=document.doc_id,
                source_id=source_id or document.metadata.get("source_id"),
                job_id=job_id,
                source_type=document.source_type,
                table_name=table_name,
                table_kind=table_kind,
                chunk_index=chunk_index,
                text=text,
                header_path=section.header_path or None,
                token_count=_word_count(text),
                metadata={
                    **document.metadata,
                    "source_label": label,
                    "chunk_index": chunk_index,
                },
            ))

    return chunks
