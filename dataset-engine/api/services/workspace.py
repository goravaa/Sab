"""Inspect the configured workspace and its indexed state."""

from __future__ import annotations

import json
import re

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from api.logging_config import get_logger
from api.services.sources import reconcile
from main.connect.database.postgres import PostgresConnector
from main.connect.object_storage.s3 import S3Connector
from main.process.config import DatasetConfig
from main.process.identifiers import load_identifiers
from main.process.state import CONFIG_KEY, STATE_VERSION, config_fingerprint
from main.process.store import S3DatasetStore
from main.process.workspace import workspace_view
from main.schema.job import DiffSummary
from main.schema.service import DatasetTarget

logger = get_logger(__name__)


def _source_access(source) -> dict[str, Any]:
    connection = source.connection
    try:
        if connection.type == "postgres":
            PostgresConnector(connection).check_connection()
        else:
            S3Connector(connection).check_connection()
    except Exception as exc:
        return {"reachable": False, "error": _short_error(exc)}

    return {"reachable": True, "error": None}


_AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{12,}\b")


def _short_error(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    return _AWS_KEY.sub("[redacted]", text)[:240]


def _source_accesses(sources, enabled: bool) -> dict[str, dict[str, Any]]:
    if not enabled:
        return {
            source.name: {"reachable": None, "error": None}
            for source in sources
        }

    if not sources:
        return {}

    with ThreadPoolExecutor(max_workers=min(4, len(sources))) as pool:
        results = list(pool.map(_source_access, sources))
    return {
        source.name: result
        for source, result in zip(sources, results, strict=True)
    }


def workspace_status(
    config: DatasetConfig,
    *,
    check_sources: bool = True,
) -> dict[str, Any]:
    """Return the safe workspace view plus target and source status."""

    target = config.target
    if target is None:
        raise ValueError("workspace config has no target")

    view = workspace_view(config)
    store = S3DatasetStore(target, config.dataset)
    target_state: dict[str, Any] = {
        "reachable": False,
        "error": None,
        "config_present": False,
        "definition_changed": False,
        "indexed_items": 0,
        "indexed_documents": 0,
        "orphaned_documents": 0,
    }
    items: Counter[str] = Counter()
    documents: Counter[str] = Counter()

    try:
        raw_config = store.read_bytes(CONFIG_KEY)
        identifiers = load_identifiers(store)
        target_state["reachable"] = True
        target_state["config_present"] = raw_config is not None
        target_state["indexed_items"] = len(identifiers)
        target_state["indexed_documents"] = sum(
            len(record.doc_ids) for record in identifiers.values()
        )

        for record in identifiers.values():
            name = str(record.metadata.get("source_name") or "")
            items[name] += 1
            documents[name] += len(record.doc_ids)

        if raw_config is not None:
            document = json.loads(raw_config)
            target_state["definition_changed"] = (
                document.get("version") != STATE_VERSION
                or document.get("config_hash") != config_fingerprint(config)
            )
    except Exception as exc:
        target_state["error"] = _short_error(exc)
        logger.warning(
            "workspace target check failed dataset=%s error=%s",
            config.dataset,
            type(exc).__name__,
        )

    configured_names = {source.name for source in config.sources}
    target_state["orphaned_documents"] = sum(
        count for name, count in documents.items() if name not in configured_names
    )

    accesses = _source_accesses(config.sources, check_sources)
    for source in view["sources"]:
        source["access"] = accesses.get(
            source["name"],
            {"reachable": None, "error": None},
        )
        source["indexed_items"] = items.get(source["name"], 0)
        source["indexed_documents"] = documents.get(source["name"], 0)

    view["target"]["status"] = target_state
    return view


def reconcile_workspace(config: DatasetConfig) -> DiffSummary:
    target = _target(config)
    logger.info(
        "workspace reconcile dataset=%s sources=%d",
        config.dataset,
        len(config.sources),
    )
    return reconcile(config.dataset, config, target)


def _target(config: DatasetConfig) -> DatasetTarget:
    if config.target is None:
        raise ValueError("workspace config has no target")
    return config.target
