"""Dataset-level state stored in the target bucket.

``config.json`` is the dataset definition used by the UI and the processor.
It deliberately contains no credentials: source auth is supplied at runtime
and belongs in a secret store, not in the dataset bucket.
"""

from __future__ import annotations

import json

from dataclasses import dataclass

from main.document.canonical import sha256_json
from main.process.config import DatasetConfig, SourceConfig
from main.process.identifiers import (
    IDENTIFIERS_KEY,
    IdentifierRecord,
    parse_identifiers,
)
from main.process.store import DatasetStore


CONFIG_KEY = "config.json"
STATE_VERSION = 1
# Bump when extraction/chunking behavior changes and old chunks must be rebuilt.
PIPELINE_VERSION = 1


class DatasetStateError(RuntimeError):
    """Raised when target state cannot be safely reconciled."""


@dataclass(frozen=True)
class ConfigState:
    config_hash: str
    force_reprocess: bool
    created: bool
    document: dict


def _source_descriptor(source: SourceConfig) -> dict:
    """Return a UI-friendly source description without secrets."""

    connection = source.connection

    if connection.type in {"postgres", "mysql"}:
        connection_data = connection.model_dump(
            mode="json",
            exclude={"password"},
        )
        if connection_data.get("tables"):
            connection_data["tables"] = sorted(connection_data["tables"])
    else:
        connection_data = connection.model_dump(
            mode="json",
            exclude={
                "access_key_id",
                "secret_access_key",
                "session_token",
                "profile",
            },
        )

    return {
        "name": source.name,
        "connection": connection_data,
        "options": source.options,
        "credentials": "runtime",
    }


def _data_model_document(config: DatasetConfig) -> dict:
    return config.data_model.model_dump(mode="json")


def _fingerprint_payload(config: DatasetConfig) -> dict:
    """Hash only settings that change indexed output, not credentials/batching."""

    data_model = _data_model_document(config)
    data_model["embedder"].pop("batch_size", None)

    return {
        "version": PIPELINE_VERSION,
        "dataset": config.dataset,
        "sources": [
            _source_descriptor(source)
            for source in sorted(config.sources, key=lambda item: item.name)
        ],
        "data_model": data_model,
    }


def config_fingerprint(config: DatasetConfig) -> str:
    """Stable fingerprint used to detect processing-config changes."""

    return sha256_json(_fingerprint_payload(config))


def build_config_document(
    config: DatasetConfig,
    *,
    target_uri: str,
) -> dict:
    return {
        "version": STATE_VERSION,
        "dataset": config.dataset,
        "target_uri": target_uri,
        "config_hash": config_fingerprint(config),
        "sources": [
            _source_descriptor(source)
            for source in sorted(config.sources, key=lambda item: item.name)
        ],
        "data_model": _data_model_document(config),
        "item_batch_size": config.item_batch_size,
    }


def write_dataset_config(
    store: DatasetStore,
    config: DatasetConfig,
    *,
    target_uri: str | None = None,
) -> dict:
    """Persist the non-secret dataset definition in the target bucket."""

    document = build_config_document(
        config,
        target_uri=target_uri if target_uri is not None else store.uri(""),
    )
    store.write_bytes(
        CONFIG_KEY,
        (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return document


def load_or_create_config(
    store: DatasetStore,
    config: DatasetConfig,
    *,
    target_uri: str | None = None,
) -> ConfigState:
    """Bootstrap ``config.json`` or compare an existing one.

    On a new dataset both ``config.json`` and ``identifiers.parquet`` are
    absent. We write the config immediately so the UI can discover sources.
    If identifiers exist without a config, we refuse to guess which config
    produced them.
    """

    data = store.read_bytes(CONFIG_KEY)

    if data is None:
        identifier_data = store.read_bytes(IDENTIFIERS_KEY)
        if identifier_data is not None:
            try:
                identifiers = parse_identifiers(identifier_data)
            except Exception as exc:
                raise DatasetStateError(
                    "target contains unreadable identifiers.parquet "
                    "without config.json"
                ) from exc
            if identifiers:
                raise DatasetStateError(
                    "target contains identifiers.parquet but no config.json; "
                    "refusing to guess the dataset configuration"
                )

        document = write_dataset_config(
            store,
            config,
            target_uri=target_uri,
        )
        return ConfigState(
            config_hash=document["config_hash"],
            force_reprocess=False,
            created=True,
            document=document,
        )

    try:
        document = json.loads(data)
    except json.JSONDecodeError as exc:
        raise DatasetStateError("config.json is not valid JSON") from exc

    if document.get("dataset") != config.dataset:
        raise DatasetStateError(
            "target bucket belongs to dataset "
            f"{document.get('dataset')!r}, not {config.dataset!r}"
        )

    stored_target = document.get("target_uri")
    if (
        target_uri is not None
        and stored_target
        and stored_target.rstrip("/") != target_uri.rstrip("/")
    ):
        raise DatasetStateError(
            "config.json belongs to target "
            f"{stored_target!r}, not {target_uri!r}"
        )

    expected_hash = config_fingerprint(config)
    stored_hash = document.get("config_hash")
    version_changed = document.get("version") != STATE_VERSION

    return ConfigState(
        config_hash=expected_hash,
        force_reprocess=version_changed or stored_hash != expected_hash,
        created=False,
        document=document,
    )


def load_committed_identifiers(
    store: DatasetStore,
    *,
    dataset: str | None = None,
) -> dict[str, IdentifierRecord]:
    """Load the state a query engine is allowed to serve.

    ``identifiers.parquet`` is the commit point. Every record must carry the
    same ``config_hash`` as ``config.json`` and the ``job_id`` that produced
    its chunks. Anything else is an in-flight or orphaned write.
    """

    data = store.read_bytes(CONFIG_KEY)
    if data is None:
        raise DatasetStateError("dataset has no config.json")

    try:
        document = json.loads(data)
    except json.JSONDecodeError as exc:
        raise DatasetStateError("config.json is not valid JSON") from exc

    if document.get("version") != STATE_VERSION:
        raise DatasetStateError(
            f"config.json version {document.get('version')!r} is not supported"
        )

    if dataset is not None and document.get("dataset") != dataset:
        raise DatasetStateError(
            "config.json belongs to dataset "
            f"{document.get('dataset')!r}, not {dataset!r}"
        )

    expected_hash = document.get("config_hash")
    if not expected_hash:
        raise DatasetStateError("config.json has no config_hash")

    identifier_data = store.read_bytes(IDENTIFIERS_KEY)
    if identifier_data is None:
        raise DatasetStateError("dataset has no committed identifiers.parquet")

    records = parse_identifiers(identifier_data)
    for record in records.values():
        if not record.job_id:
            raise DatasetStateError(
                f"identifier {record.source_id!r} has no committed job_id"
            )
        if record.config_hash != expected_hash:
            raise DatasetStateError(
                f"identifier {record.source_id!r} belongs to a different config"
            )

    return records
