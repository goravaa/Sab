"""Central workspace.yaml settings for the query service.

``workspace.yaml`` is the single deployment config shared with
dataset-engine (mounted as ``SAB_WORKSPACE`` by the Compose stack). The
connection fields (``dataset`` and ``target``) win over the legacy
environment variables; explicit environment values still override runtime
infra such as the embedding endpoint because those change per deployment
profile (host Ollama vs GPU vLLM).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from .dataset import DatasetConnection

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_CANDIDATES = (
    "./config/workspace.yaml",
    "../dataset-engine/config/workspace.yaml",
)


class WorkspaceError(RuntimeError):
    """The workspace file is present but unusable."""


def workspace_file() -> Path | None:
    """Return the workspace yaml path, or None when none is configured."""
    candidates: list[Path] = []
    configured = os.getenv("SAB_WORKSPACE")
    if configured:
        candidates.append(Path(configured).expanduser())
    else:
        candidates.extend(Path(__file__).resolve().parents[1] / name for name in _CANDIDATES)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise WorkspaceError(
                    f"workspace config references unset environment variable {name!r}"
                )
            return os.environ[name]

        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def load_workspace(path: str | Path | None = None) -> dict[str, Any] | None:
    """Parse the workspace yaml into a plain document, or None when absent."""
    file_path = Path(path).expanduser() if path else workspace_file()
    if file_path is None or not file_path.is_file():
        return None
    try:
        import yaml
    except ImportError as exc:
        raise WorkspaceError(
            f"workspace config requires PyYAML; install the 'pyyaml' package: {file_path}"
        ) from exc
    try:
        document = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise WorkspaceError(f"cannot read workspace config: {exc}") from exc
    except yaml.YAMLError as exc:
        raise WorkspaceError(f"invalid workspace YAML in {file_path}: {exc}") from exc
    if not isinstance(document, dict):
        raise WorkspaceError(f"workspace YAML must contain a mapping: {file_path}")
    try:
        return _expand_env(document)
    except WorkspaceError as exc:
        raise WorkspaceError(f"{file_path}: {exc}") from exc


def workspace_connection(path: str | Path | None = None) -> DatasetConnection | None:
    """Build the query target from workspace.yaml, or None when not set."""
    document = load_workspace(path)
    if document is None:
        return None
    target = document.get("target")
    if not isinstance(target, dict) or not target.get("bucket"):
        raise WorkspaceError(
            "workspace config target must set at least 'bucket' for query-service"
        )
    dataset = document.get("dataset")
    if not isinstance(dataset, str) or not dataset.strip():
        raise WorkspaceError("workspace config must set dataset")
    return DatasetConnection(
        bucket=str(target["bucket"]),
        dataset=dataset.strip(),
        prefix=str(target.get("prefix") or ""),
        region=str(target.get("region") or os.environ.get("AWS_REGION") or "us-west-2"),
        endpoint_url=target.get("endpoint_url"),
        access_key_id=target.get("access_key_id"),
        secret_access_key=target.get("secret_access_key"),
    )


def embed_settings() -> dict[str, str]:
    """Embedding endpoint resolution: explicit env, then workspace.yaml."""
    document = load_workspace() or {}
    embeddings = document.get("embeddings") if isinstance(document.get("embeddings"), dict) else {}
    return {
        "base_url": os.environ.get("SAB_EMBED_BASE_URL")
        or embeddings.get("base_url")
        or "http://localhost:11434/v1",
        "api_key": os.environ.get("SAB_EMBED_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or embeddings.get("api_key")
        or "ollama",
        "model": embeddings.get("model") or "",
    }


def sql_settings() -> dict[str, str]:
    """DuckDB limits from environment, or workspace.yaml query section."""
    document = load_workspace() or {}
    query = document.get("query") if isinstance(document.get("query"), dict) else {}
    return {
        "memory_limit": os.environ.get("SAB_SQL_MEMORY_LIMIT")
        or str(query.get("sql_memory_limit") or "512MB"),
        "threads": os.environ.get("SAB_SQL_THREADS")
        or str(query.get("sql_threads") or "2"),
    }
