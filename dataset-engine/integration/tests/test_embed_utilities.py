from __future__ import annotations

from types import SimpleNamespace

import pytest

from main.embed.embeddings import EmbeddingError, TextEmbedder
from main.schema.service import EmbedderConfig


class FakeEmbeddings:
    def __init__(self, *, size: int = 1, drop: int = 0) -> None:
        self.size = size
        self.drop = drop
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        data = [
            SimpleNamespace(
                index=index,
                embedding=[float(ord(text[0]))] + [0.0] * (self.size - 1),
            )
            for index, text in enumerate(kwargs["input"])
        ]
        if self.drop:
            data = data[self.drop:]
        return SimpleNamespace(data=list(reversed(data)))


class FakeClient:
    def __init__(self, *, size: int = 1, drop: int = 0) -> None:
        self.embeddings = FakeEmbeddings(size=size, drop=drop)


def test_embed_texts_batches_requests_and_returns_input_order():
    client = FakeClient()
    embedder = TextEmbedder(
        EmbedderConfig(model="bge-m3", batch_size=2),
        client=client,
    )

    vectors = embedder.embed_texts(["alpha", "beta", "charlie"])

    assert [call["input"] for call in client.embeddings.calls] == [
        ["alpha", "beta"],
        ["charlie"],
    ]
    assert all(call["model"] == "bge-m3" for call in client.embeddings.calls)
    assert all("dimensions" not in call for call in client.embeddings.calls)
    assert [vector[0] for vector in vectors] == [97.0, 98.0, 99.0]


def test_embed_texts_passes_dimensions_when_configured():
    client = FakeClient(size=3)
    embedder = TextEmbedder(
        EmbedderConfig(model="bge-m3", dimensions=3, batch_size=10),
        client=client,
    )

    vectors = embedder.embed_texts(["alpha"])

    assert client.embeddings.calls[0]["dimensions"] == 3
    assert len(vectors[0]) == 3


def test_embed_texts_returns_early_for_empty_input_and_embeds_single_text():
    client = FakeClient()
    embedder = TextEmbedder(client=client)

    assert embedder.embed_texts([]) == []
    assert client.embeddings.calls == []

    assert embedder.embed_text("query") == [float(ord("q"))]
    assert client.embeddings.calls[0]["input"] == ["query"]


def test_embed_texts_rejects_single_string_and_non_string_items():
    embedder = TextEmbedder(client=FakeClient())

    with pytest.raises(TypeError, match="iterable of strings"):
        embedder.embed_texts("not-a-list")

    with pytest.raises(TypeError, match=r"texts\[1\] must be a string"):
        embedder.embed_texts(["valid", 2])


def test_embedding_response_count_and_dimension_are_validated():
    with pytest.raises(EmbeddingError, match="Expected 2 embeddings, received 1"):
        TextEmbedder(client=FakeClient(drop=1)).embed_texts(["a", "b"])

    with pytest.raises(EmbeddingError, match="Expected 3 dimensions, received 1"):
        TextEmbedder(
            EmbedderConfig(dimensions=3),
            client=FakeClient(),
        ).embed_texts(["a"])
