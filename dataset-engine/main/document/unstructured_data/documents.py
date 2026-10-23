"""Read unstructured files from S3-compatible storage and yield Documents.

Supported formats:
    - PDF
    - DOCX
    - Markdown
    - Plain text

Responsibilities:
    1. Read source objects through S3Connector.
    2. Extract normalized text while preserving headings and table structure.
    3. Convert each file into the existing Document model.
    4. Generate deterministic document IDs and content signatures.

Chunking, embedding, and storage belong to downstream pipeline stages.
"""

from __future__ import annotations

import re
import tempfile

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any


from main.connect.object_storage.s3 import S3Connector, SourceObject
from main.document.canonical import sha256_json
from main.schema.document import Document, SourceType


SUPPORTED_EXTENSIONS = frozenset({".pdf", ".docx", ".md", ".markdown", ".txt"})
TEXT_EXTENSIONS = frozenset({".md", ".markdown", ".txt"})
SIGNATURE_VERSION = 1

MEDIA_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".txt": "text/plain",
}


# ---------------------------------------------------------------------------
# Text normalization and document construction
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """Normalize extracted text without discarding its structure."""

    text = text.replace("\ufeff", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join(lines).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def text_to_document(
    text: str,
    *,
    namespace: str,
    source_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> Document:
    """Convert extracted text into a deterministic Document.

    Identity depends only on the source location, so edits keep the same
    document ID. The signature tracks the normalized text content.
    """

    normalized = normalize_text(text)

    identity = {
        "namespace": namespace,
        "source_id": source_id,
        "kind": "object",
    }

    doc_id = sha256_json({
        "version": 1,
        **identity,
    })

    signature = sha256_json({
        "version": SIGNATURE_VERSION,
        "text": normalized,
    })

    return Document(
        doc_id=doc_id,
        source_type=SourceType.S3_OBJECT,
        raw_content=normalized,
        signature=signature,
        metadata={
            **identity,
            "word_count": len(normalized.split()),
            **(metadata or {}),
        },
    )


# ---------------------------------------------------------------------------
# DOCX reader
# ---------------------------------------------------------------------------

def _heading_prefix(paragraph: Any) -> str:
    style = getattr(paragraph, "style", None)
    name = (getattr(style, "name", None) or "").strip()

    if name == "Title":
        return "# "

    if name.startswith("Heading"):
        level_text = name.removeprefix("Heading").strip()
        level = int(level_text) if level_text.isdigit() else 1
        return f"{'#' * min(max(level, 1), 6)} "

    return ""


def _list_prefix(paragraph: Any) -> str:
    style = getattr(paragraph, "style", None)
    name = (getattr(style, "name", None) or "").strip()

    if name.startswith("List Bullet"):
        return "- "
    if name.startswith("List Number"):
        return "1. "
    return ""


def _table_to_markdown(table: Any) -> str:
    rows: list[list[str]] = []

    for row in table.rows:
        cells = [
            " ".join(cell.text.split()).replace("|", "\\|")
            for cell in row.cells
        ]
        rows.append(cells)

    if not rows:
        return ""

    width = max(len(row) for row in rows)
    rows = [row + [""] * (width - len(row)) for row in rows]

    rendered = ["| " + " | ".join(row) + " |" for row in rows]
    separator = "| " + " | ".join("---" for _ in range(width)) + " |"
    return "\n".join([rendered[0], separator, *rendered[1:]])


def extract_docx_text(path: str | Path) -> tuple[str, dict[str, Any]]:
    """Extract Markdown-like text from a DOCX file in document order."""

    try:
        from docx import Document as WordDocument
        from docx.oxml.ns import qn
        from docx.table import Table
        from docx.text.paragraph import Paragraph
    except ImportError as exc:
        raise RuntimeError(
            "DOCX support requires python-docx; install 'python-docx'."
        ) from exc

    document = WordDocument(str(path))

    blocks: list[str] = []
    paragraph_count = 0
    table_count = 0

    for child in document.element.body.iterchildren():
        if child.tag == qn("w:p"):
            paragraph = Paragraph(child, document)
            text = paragraph.text.strip()
            if not text:
                continue

            paragraph_count += 1
            prefix = _heading_prefix(paragraph) or _list_prefix(paragraph)
            blocks.append(f"{prefix}{text}")
        elif child.tag == qn("w:tbl"):
            table = Table(child, document)
            rendered = _table_to_markdown(table)
            if rendered:
                table_count += 1
                blocks.append(rendered)

    properties = document.core_properties
    metadata: dict[str, Any] = {
        "paragraph_count": paragraph_count,
        "table_count": table_count,
    }
    if properties.title:
        metadata["title"] = properties.title
    if properties.author:
        metadata["author"] = properties.author

    return normalize_text("\n\n".join(blocks)), metadata


# ---------------------------------------------------------------------------
# PDF reader
# ---------------------------------------------------------------------------

def extract_pdf_text(path: str | Path) -> tuple[str, dict[str, Any]]:
    """Extract normalized text page by page from a PDF file."""

    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError(
            "PDF support requires pypdf; install 'pypdf'."
        ) from exc

    reader = PdfReader(str(path))

    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception as exc:
            raise ValueError("Encrypted PDFs cannot be read") from exc

    pages: list[str] = []

    for page in reader.pages:
        page_text = normalize_text(page.extract_text() or "")
        if page_text:
            pages.append(page_text)

    return normalize_text("\n\n".join(pages)), {
        "page_count": len(reader.pages),
    }


# ---------------------------------------------------------------------------
# Source helpers
# ---------------------------------------------------------------------------

def _extension(key: str) -> str | None:
    return next(
        (
            extension
            for extension in SUPPORTED_EXTENSIONS
            if key.lower().endswith(extension)
        ),
        None,
    )


@contextmanager
def _seekable_copy(
    connector: S3Connector,
    source: SourceObject,
    extension: str,
) -> Iterator[Path]:
    """Stream an object to a temporary file for parsers that need seeking."""

    with connector.open_object(source.key) as body:
        with tempfile.NamedTemporaryFile(suffix=extension) as temporary_file:
            while True:
                chunk = body.read(8 * 1024 * 1024)
                if not chunk:
                    break
                temporary_file.write(chunk)

            temporary_file.flush()
            yield Path(temporary_file.name)


# ---------------------------------------------------------------------------
# Source dispatch
# ---------------------------------------------------------------------------

def iter_unstructured_documents(
    connector: S3Connector,
    source: SourceObject,
    *,
    namespace: str = "files",
    encoding: str = "utf-8-sig",
) -> Iterator[Document]:
    """Dispatch a source object to the appropriate unstructured reader.

    Empty extractions are skipped so the pipeline never embeds blank files.
    Unsupported formats raise ValueError instead of being silently ignored.
    """

    extension = _extension(source.key)

    if extension is None:
        raise ValueError(
            f"Unsupported unstructured file: {source.key!r}. "
            f"Supported extensions: {sorted(SUPPORTED_EXTENSIONS)}"
        )

    metadata = {
        "extension": extension,
        "media_type": MEDIA_TYPES[extension],
    }

    if extension in TEXT_EXTENSIONS:
        with connector.open_object(source.key) as body:
            text = body.read().decode(encoding, errors="replace")
        text, extracted = normalize_text(text), {}
    elif extension == ".docx":
        with _seekable_copy(connector, source, extension) as path:
            text, extracted = extract_docx_text(path)
    else:
        with _seekable_copy(connector, source, extension) as path:
            text, extracted = extract_pdf_text(path)

    if not text:
        return

    yield text_to_document(
        text,
        namespace=namespace,
        source_id=source.key,
        metadata={
            **metadata,
            **extracted,
        },
    )
