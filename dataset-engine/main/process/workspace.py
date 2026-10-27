"""Load the local workspace definition from YAML.

The workspace file is the only place credentials are configured. It is read
on every request so editing the file and refreshing the UI is enough to pick
up changes. Secrets never leave this process in API responses.
"""

from __future__ import annotations

import os
import re

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from main.process.config import DatasetConfig, SourceConfig
from main.schema.service import DataModelConfig, DatasetTarget


DEFAULT_WORKSPACE = (
    Path(__file__).resolve().parents[2] / "config" / "workspace.yaml"
)
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class WorkspaceConfigError(RuntimeError):
    """Raised when the local workspace file cannot be used."""


def workspace_path(path: str | Path | None = None) -> Path:
    """Return the configured workspace path, honoring ``SAB_WORKSPACE``."""

    configured = path or os.getenv("SAB_WORKSPACE") or DEFAULT_WORKSPACE
    return Path(configured).expanduser()


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise WorkspaceConfigError(
                    f"environment variable {name!r} is not set"
                )
            return os.environ[name]

        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def _read_yaml(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        raise WorkspaceConfigError(
            f"workspace config not found at {path}; copy "
            "config/workspace.example.yaml to config/workspace.yaml"
        )

    try:
        import yaml
    except ImportError as exc:
        raise WorkspaceConfigError(
            "workspace config requires PyYAML; install the 'pyyaml' package"
        ) from exc

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise WorkspaceConfigError(f"cannot read workspace config: {exc}") from exc
    except yaml.YAMLError as exc:
        raise WorkspaceConfigError(f"invalid workspace YAML: {exc}") from exc

    if not isinstance(document, dict):
        raise WorkspaceConfigError("workspace YAML must contain a mapping")
    return _expand_env(document)


def load_workspace(path: str | Path | None = None) -> DatasetConfig:
    """Parse and validate the workspace YAML into a dataset config."""

    resolved = workspace_path(path)
    document = _read_yaml(resolved)

    dataset = document.get("dataset")
    if not isinstance(dataset, str) or not dataset.strip():
        raise WorkspaceConfigError("workspace config must set dataset")

    target_data = document.get("target")
    if not isinstance(target_data, dict):
        raise WorkspaceConfigError("workspace config must set target")

    raw_sources = document.get("sources", [])
    if not isinstance(raw_sources, list) or not raw_sources:
        raise WorkspaceConfigError("workspace config must define at least one source")

    try:
        target = DatasetTarget.model_validate(target_data)
        sources = [
            SourceConfig.model_validate(item)
            for item in raw_sources
        ]
        raw_data_model = document.get("data_model") or {}
        embed_model = os.getenv("SAB_EMBED_MODEL")
        if embed_model:
            raw_data_model = dict(raw_data_model)
            raw_embedder = dict(raw_data_model.get("embedder") or {})
            raw_embedder["model"] = embed_model
            raw_data_model["embedder"] = raw_embedder
        data_model = DataModelConfig.model_validate(raw_data_model)
        item_batch_size = int(document.get("item_batch_size", 1000))
    except Exception as exc:
        raise WorkspaceConfigError(f"invalid workspace config: {exc}") from exc

    if item_batch_size <= 0:
        raise WorkspaceConfigError("item_batch_size must be greater than zero")

    names = [source.name for source in sources]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise WorkspaceConfigError(
            f"source names must be unique; duplicates: {', '.join(duplicates)}"
        )

    return DatasetConfig(
        dataset=dataset.strip(),
        sources=sources,
        data_model=data_model,
        item_batch_size=item_batch_size,
        target=target,
    )


def target_uri(config: DatasetConfig) -> str:
    target = config.target
    if target is None:
        raise WorkspaceConfigError("workspace config has no target")
    root = "/".join(
        part.strip("/")
        for part in (target.prefix, config.dataset)
        if part.strip("/")
    )
    return f"s3://{target.bucket}/{root}" if root else f"s3://{target.bucket}"


def _auth_mode(connection: Any) -> str:
    if connection.type == "postgres":
        return connection.auth_mode
    if connection.type == "mysql":
        return "password"
    if getattr(connection, "access_key_id", None):
        return "static"
    return "default"


def _source_location(source: SourceConfig) -> str:
    connection = source.connection
    if connection.type == "postgres":
        return f"postgres://{connection.host}:{connection.port}/{connection.database}"
    return f"s3://{connection.bucket}/{connection.prefix.strip('/')}".rstrip("/")


def workspace_view(config: DatasetConfig) -> dict[str, Any]:
    """Return a credential-free description of the workspace for the UI."""

    target = config.target
    if target is None:
        raise WorkspaceConfigError("workspace config has no target")

    return {
        "dataset": config.dataset,
        "target": {
            "bucket": target.bucket,
            "prefix": target.prefix,
            "region": target.region,
            "endpoint_url": target.endpoint_url,
            "uri": target_uri(config),
            "auth": _auth_mode(target),
            "profile": target.profile,
        },
        "sources": [
            {
                "name": source.name,
                "label": source.label or source.name,
                "description": source.description,
                "type": source.connection.type,
                "location": _source_location(source),
                "auth": _auth_mode(source.connection),
                "profile": getattr(source.connection, "profile", None),
            }
            for source in config.sources
        ],
    }
