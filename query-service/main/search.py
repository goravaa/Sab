"""Question embedding and committed-chunk retrieval."""

from __future__ import annotations
from typing import Any

from .dataset import OpenDataset
from .workspace import embed_settings


class SearchError(RuntimeError):
    """Embedding or retrieval provider failed."""


def embed_query(text: str, model: str) -> list[float]:
    try:
        from openai import OpenAI

        settings = embed_settings()
        client = OpenAI(
            api_key=settings["api_key"] or "ollama",
            base_url=settings["base_url"],
            timeout=45,
        )
        result = client.embeddings.create(model=model, input=[text])
        vector = list(result.data[0].embedding)
        if not vector:
            raise SearchError("Embedding endpoint returned an empty vector")
        return vector
    except SearchError:
        raise
    except Exception as exc:
        raise SearchError("Could not embed the question; check the embedding endpoint and model") from exc


def search_chunks(dataset: OpenDataset, question: str, *, limit: int = 6) -> list[dict[str, Any]]:
    model = (
        embed_settings()["model"]
        or dataset.config.get("data_model", {}).get("embedder", {}).get("model")
    )
    if not model:
        raise SearchError("Dataset config does not specify an embedding model")
    vector = embed_query(question, model)
    return dataset.search(vector, limit=max(1, min(limit, 50)))
