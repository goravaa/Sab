from __future__ import annotations

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
