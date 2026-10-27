"""Embed text through an OpenAI-compatible embeddings endpoint.

Ollama exposes an OpenAI-compatible API at ``/v1``, so the same client works
for local models and hosted providers:

    embedder = TextEmbedder(
        EmbedderConfig(model="bge-m3"),
        base_url="http://localhost:11434/v1",
    )
    vectors = embedder.embed_texts(["first chunk", "second chunk"])

The client is created lazily so importing this module does not require the
OpenAI SDK or a running endpoint.
"""

from __future__ import annotations

import os

from collections.abc import Iterable
from typing import Any


from main.schema.service import EmbedderConfig


class EmbeddingError(RuntimeError):
    """Raised when an embeddings endpoint returns an unusable response."""


class TextEmbedder:
    """Batch text embeddings with an OpenAI-compatible client."""

    def __init__(
        self,
        config: EmbedderConfig | None = None,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 60.0,
        max_retries: int = 2,
        client: Any | None = None,
    ) -> None:
        self.config = config or EmbedderConfig()

        # Ollama ignores the key but requires a non-empty value. Hosted
        # providers can supply their own through the argument or environment.
        self.base_url = (
            base_url
            if base_url is not None
            else os.getenv("SAB_EMBED_BASE_URL") or None
        )
        self.api_key = (
            api_key
            or os.getenv("SAB_EMBED_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or "ollama"
        )
        self.timeout = timeout
        self.max_retries = max_retries

        self._client = client

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError(
                "Embedding support requires the OpenAI SDK; install 'openai'."
            ) from exc

        self._client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )
        return self._client

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        request: dict[str, Any] = {
            "model": self.config.model,
            "input": texts,
        }

        # Some OpenAI-compatible servers reject unknown parameters, so only
        # send dimensions when the caller explicitly configured them.
        if self.config.dimensions is not None:
            request["dimensions"] = self.config.dimensions

        response = self._get_client().embeddings.create(**request)
        data = list(getattr(response, "data", None) or [])

        if len(data) != len(texts):
            raise EmbeddingError(
                f"Expected {len(texts)} embeddings, received {len(data)}"
            )

        # Response order is usually stable, but index is authoritative.
        ordered = sorted(data, key=lambda item: getattr(item, "index", 0))

        embeddings: list[list[float]] = []

        for item in ordered:
            embedding = getattr(item, "embedding", None)

            if not embedding:
                raise EmbeddingError("Embeddings endpoint returned an empty vector")

            vector = [float(value) for value in embedding]

            if (
                self.config.dimensions is not None
                and len(vector) != self.config.dimensions
            ):
                raise EmbeddingError(
                    f"Expected {self.config.dimensions} dimensions, "
                    f"received {len(vector)}"
                )

            embeddings.append(vector)

        return embeddings

    def embed_texts(self, texts: Iterable[str]) -> list[list[float]]:
        """Embed texts in input order, batching requests by configured size."""

        if isinstance(texts, str):
            raise TypeError("texts must be an iterable of strings, not a single string")

        items = list(texts)

        for position, text in enumerate(items):
            if not isinstance(text, str):
                raise TypeError(
                    f"texts[{position}] must be a string, "
                    f"not {type(text).__qualname__}"
                )

        if not items:
            return []

        embeddings: list[list[float]] = []
        batch_size = self.config.batch_size

        for start in range(0, len(items), batch_size):
            embeddings.extend(
                self._embed_batch(items[start:start + batch_size])
            )

        return embeddings

    def embed_text(self, text: str) -> list[float]:
        """Embed one piece of text, such as a search query."""

        if not isinstance(text, str):
            raise TypeError(
                f"text must be a string, not {type(text).__qualname__}"
            )

        return self.embed_texts([text])[0]


def embed_texts(
    texts: Iterable[str],
    *,
    config: EmbedderConfig | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
) -> list[list[float]]:
    """Convenience wrapper for one-off embedding calls."""

    return TextEmbedder(
        config,
        base_url=base_url,
        api_key=api_key,
    ).embed_texts(texts)


def embed_text(
    text: str,
    *,
    config: EmbedderConfig | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
) -> list[float]:
    """Convenience wrapper for embedding one piece of text."""

    return TextEmbedder(
        config,
        base_url=base_url,
        api_key=api_key,
    ).embed_text(text)
