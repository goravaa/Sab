"""Job orchestration: reconcile sources, build chunks, embed, and write."""

from __future__ import annotations

import json
import warnings

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from main.process.chunking import chunk_document
from main.process.config import (
    DatasetConfig,
    load_env_file,
    target_from_env,
)
from main.process.identifiers import (
    IDENTIFIERS_KEY,
    IdentifierRecord,
    load_identifiers,
    write_identifiers,
)
from main.process.sources import (
    SourceItem,
    SourceLoader,
    build_connectors,
    iter_source_items,
)
from main.process.state import (
    config_fingerprint,
    load_or_create_config,
    write_dataset_config,
)
from main.process.store import DatasetStore, S3DatasetStore
from main.process.writer import (
    LanceChunkWriter,
    lance_storage_options,
)
from main.schema.document import Document, SourceType
from main.schema.job import (
    BatchReceipt,
    DiffAction,
    JobManifest,
    JobProgress,
    ManifestEntry,
)
from main.schema.service import S3Config


DocumentLoader = Callable[[ManifestEntry], Iterable[Document]]


def build_manifest(
    *,
    job_id: str,
    dataset: str,
    items: Iterable[SourceItem],
    identifiers: Mapping[str, IdentifierRecord],
    force_reprocess: bool = False,
    created_at: datetime | None = None,
) -> JobManifest:
    """Compare a complete source scan with the committed identifiers."""

    entries: list[ManifestEntry] = []
    seen: set[str] = set()

    for item in items:
        seen.add(item.source_id)
        previous = identifiers.get(item.source_id)

        if previous is None:
            action = DiffAction.NEW
        elif force_reprocess or previous.signature != item.signature:
            action = DiffAction.MODIFIED
        else:
            continue

        entries.append(ManifestEntry(
            doc_id=item.source_id,
            signature=item.signature,
            source_type=item.source_type,
            action=action,
            source=item.source,
            metadata=item.metadata,
        ))

    for source_id, previous in identifiers.items():
        if source_id in seen:
            continue

        entries.append(ManifestEntry(
            doc_id=source_id,
            signature=previous.signature,
            source_type=SourceType(previous.source_type),
            action=DiffAction.DELETED,
            source=previous.source,
            metadata=previous.metadata,
        ))

    return JobManifest(
        job_id=job_id,
        dataset=dataset,
        created_at=created_at or datetime.now(timezone.utc),
        entries=entries,
    )


def manifest_key(job_id: str) -> str:
    return f"jobs/{job_id}/manifest.json"


def receipt_key(job_id: str, batch_number: int) -> str:
    return f"jobs/{job_id}/progress/batch_{batch_number:04d}.json"


def tick_key(job_id: str, batch_number: int) -> str:
    """Small timestamp sidecar used for server-side ETA estimation."""

    return f"jobs/{job_id}/progress/batch_{batch_number:04d}.tick.json"


def save_tick(
    store: DatasetStore,
    job_id: str,
    batch_number: int,
    end_index: int,
    processed_at: datetime,
) -> None:
    """Persist a compact progress tick; identical rewrites are harmless."""

    store.write_bytes(
        tick_key(job_id, batch_number),
        json.dumps({
            "batch_number": batch_number,
            "end_index": end_index,
            "processed_at": processed_at.isoformat(),
        }).encode("utf-8"),
    )


def save_manifest(store: DatasetStore, manifest: JobManifest) -> None:
    store.write_bytes(
        manifest_key(manifest.job_id),
        manifest.model_dump_json(indent=2).encode("utf-8"),
    )


def load_manifest(store: DatasetStore, job_id: str) -> JobManifest | None:
    data = store.read_bytes(manifest_key(job_id))
    if data is None:
        return None
    return JobManifest.model_validate_json(data)


def prepare_job(
    *,
    job_id: str,
    config: DatasetConfig,
    store: DatasetStore,
    connectors: Mapping[str, Any] | None = None,
) -> JobManifest:
    """Scan sources completely and persist the resulting manifest."""

    state = load_or_create_config(
        store,
        config,
        target_uri=store.uri(""),
    )
    identifiers = load_identifiers(store)
    stored_hash = state.document.get("config_hash")
    legacy = any(
        not record.config_hash or not record.job_id
        for record in identifiers.values()
    )
    inconsistent = any(
        record.config_hash and record.config_hash != stored_hash
        for record in identifiers.values()
    )
    items = list(iter_source_items(config, connectors))
    manifest = build_manifest(
        job_id=job_id,
        dataset=config.dataset,
        items=items,
        identifiers=identifiers,
        force_reprocess=state.force_reprocess or legacy or inconsistent,
    )
    save_manifest(store, manifest)
    return manifest


class JobProcessor:
    """Execute a prepared manifest in batches and commit identifiers last."""

    def __init__(
        self,
        *,
        config: DatasetConfig,
        store: DatasetStore,
        writer: Any,
        embedder: Any,
        document_loader: DocumentLoader | None = None,
        item_batch_size: int | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.writer = writer
        self.embedder = embedder
        self.document_loader = (
            document_loader
            or SourceLoader(config).load
        )
        self.item_batch_size = item_batch_size or config.item_batch_size

    def run(self, manifest: JobManifest) -> JobProgress:
        """Process every manifest entry, writing receipts as batches finish."""

        identifiers = load_identifiers(self.store)
        updates: dict[str, IdentifierRecord] = {}
        deleted: set[str] = set()
        config_hash = config_fingerprint(self.config)
        progress = JobProgress(total_items=len(manifest.entries))

        for batch_number, start in enumerate(
            range(0, len(manifest.entries), self.item_batch_size),
            start=1,
        ):
            batch = manifest.entries[start:start + self.item_batch_size]
            end = start + len(batch)
            key = receipt_key(manifest.job_id, batch_number)
            saved = self.store.read_bytes(key)
            receipt_data = None
            if saved is not None:
                try:
                    candidate = BatchReceipt.model_validate_json(saved)
                    if (candidate.batch_number == batch_number and
                        candidate.start_index == start and candidate.end_index == end and
                        candidate.item_count == len(batch) and len(candidate.records) + len(candidate.removed) == len(batch)):
                        receipt_data = candidate
                except Exception:
                    receipt_data = None
            if receipt_data is not None:
                records = {row["source_id"]: IdentifierRecord(**row) for row in receipt_data.records}
                removed = set(receipt_data.removed)
            else:
                records, removed = self._process_batch(manifest.job_id, batch, config_hash)
            updates.update(records)
            deleted.update(removed)

            receipt = BatchReceipt(
                batch_number=batch_number,
                start_index=start,
                end_index=end,
                item_count=len(batch),
                processed_at=datetime.now(timezone.utc),
                records=[record.__dict__ for record in records.values()],
                removed=sorted(removed),
            )
            processed_at = receipt.processed_at
            if receipt_data is None:
                self.store.write_bytes(key, receipt.model_dump_json(indent=2).encode("utf-8"))
            # Rewritten on resume too, so freshly restarted batches report a
            # current timestamp for rate estimation.
            save_tick(self.store, manifest.job_id, batch_number, end, processed_at)

            progress = JobProgress(
                total_items=len(manifest.entries),
                processed_items=end,
                completed_batches=batch_number,
            )

        # Identifiers are the commit point. Old chunks stay in place until the
        # new records are visible, so a crash here leaves the previous version
        # queryable and any new chunks are ignored as orphans.
        identifiers_changed = (
            bool(updates)
            or bool(deleted)
            or not self.store.exists(IDENTIFIERS_KEY)
        )
        if identifiers_changed:
            identifiers.update(updates)
            for source_id in deleted:
                identifiers.pop(source_id, None)
            write_identifiers(self.store, identifiers.values())

        # Commit the current config hash only after identifiers succeed. If a
        # previous run changed chunk/embed settings, this is where the new
        # fingerprint becomes authoritative.
        write_dataset_config(
            self.store,
            self.config,
            target_uri=self.store.uri(""),
        )

        # Cleanup is best-effort: the commit already succeeded, and orphan
        # chunks are ignored by the query engine.
        try:
            for source_id in updates:
                self.writer.delete_source_except_job(
                    source_id,
                    manifest.job_id,
                )
            for source_id in deleted:
                self.writer.delete_source(source_id)
        except Exception as exc:
            warnings.warn(
                f"post-commit chunk cleanup failed: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )

        return progress

    def _process_batch(
        self,
        job_id: str,
        entries: list[ManifestEntry],
        config_hash: str,
    ) -> tuple[dict[str, IdentifierRecord], set[str]]:
        chunks = []
        doc_ids_by_source: dict[str, list[str]] = {}
        removed: set[str] = set()

        batch_loader = getattr(getattr(self.document_loader, "__self__", None), "load_batch", None)
        loaded = batch_loader(entries) if batch_loader else None

        for entry in entries:
            if entry.action == DiffAction.DELETED:
                removed.add(entry.doc_id)
                continue

            documents = loaded[entry.doc_id] if loaded is not None else list(self.document_loader(entry))
            doc_ids_by_source[entry.doc_id] = [
                document.doc_id for document in documents
            ]

            source_label = (
                entry.metadata.get("key")
                or entry.source
                or entry.doc_id
            )
            for document in documents:
                chunks.extend(chunk_document(
                    document,
                    self.config.data_model.chunk,
                    source_id=entry.doc_id,
                    source_label=str(source_label),
                    job_id=job_id,
                ))

        if chunks:
            vectors = self.embedder.embed_texts([chunk.text for chunk in chunks])
            if len(vectors) != len(chunks):
                raise RuntimeError(
                    "embedding endpoint returned a different number of vectors"
                )
            for chunk, vector in zip(chunks, vectors):
                chunk.embedding = vector

        # Remove chunks from an earlier attempt of this same job. Committed
        # chunks from older jobs stay in place until identifiers are updated.
        for entry in entries:
            if entry.action != DiffAction.DELETED:
                self.writer.delete_source_job(entry.doc_id, job_id)

        self.writer.append_chunks(chunks)

        records: dict[str, IdentifierRecord] = {}
        for entry in entries:
            if entry.action == DiffAction.DELETED:
                continue

            doc_ids = doc_ids_by_source.get(entry.doc_id, [])
            records[entry.doc_id] = IdentifierRecord(
                source_id=entry.doc_id,
                signature=entry.signature,
                source_type=entry.source_type.value,
                source=entry.source or "",
                doc_ids=doc_ids,
                metadata={
                    **entry.metadata,
                    "chunk_count": sum(
                        1 for chunk in chunks
                        if chunk.source_id == entry.doc_id
                    ),
                },
                config_hash=config_hash,
                job_id=job_id,
            )

        return records, removed


def run_job(
    *,
    job_id: str,
    config: DatasetConfig,
    target: S3Config | None = None,
    target_env_file: str | Path | None = "s3.yaml",
    store: DatasetStore | None = None,
    writer: Any | None = None,
    embedder: Any | None = None,
    connectors: Mapping[str, Any] | None = None,
    embed_base_url: str | None = None,
    embed_api_key: str | None = None,
) -> JobProgress:
    """Prepare and run a complete job with production defaults."""

    # Precedence: explicit argument > provided store > dataset config (BYOB)
    # > local env file. A store is authoritative because its config already
    # points at the bucket holding identifiers and data.lance.
    if target is None and isinstance(store, S3DatasetStore):
        target = store.config

    if target is None:
        target = config.target

    if store is None:
        if target is None:
            if target_env_file is not None:
                env_path = Path(target_env_file)
                if env_path.exists():
                    load_env_file(env_path, override=True)
            target = target_from_env()
        store = S3DatasetStore(target, config.dataset)

    if connectors is None:
        connectors = build_connectors(config)

    if writer is None:
        writer = LanceChunkWriter(
            store.uri("data.lance"),
            storage_options=(
                lance_storage_options(target)
                if target is not None
                else None
            ),
        )

    if embedder is None:
        from main.embed.embeddings import TextEmbedder

        embedder = TextEmbedder(
            config.data_model.embedder,
            base_url=embed_base_url,
            api_key=embed_api_key,
        )

    manifest = prepare_job(
        job_id=job_id,
        config=config,
        store=store,
        connectors=connectors,
    )
    return JobProcessor(
        config=config,
        store=store,
        writer=writer,
        embedder=embedder,
        document_loader=SourceLoader(config, connectors).load,
    ).run(manifest)
