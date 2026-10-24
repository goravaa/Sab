from __future__ import annotations

import json
import os

import pytest

from main.process.config import (
    DatasetConfig,
    DatasetTarget,
    SourceConfig,
    load_env_file,
    read_env_file,
    target_from_env,
    target_from_file,
)
from main.process.state import (
    CONFIG_KEY,
    DatasetStateError,
    config_fingerprint,
    load_committed_identifiers,
    load_or_create_config,
    write_dataset_config,
)
from main.schema.job import JobCreate
from main.process.identifiers import (
    IdentifierRecord,
    load_identifiers,
    write_identifiers,
)
from main.connect.object_storage.s3 import SourceObject
from main.process.runner import (
    JobProcessor,
    build_manifest,
    prepare_job,
    receipt_key,
)
from main.process.sources import SourceItem
from main.process.store import MemoryDatasetStore
from main.process.writer import LanceChunkWriter, MemoryChunkWriter
from main.schema.document import Chunk, Document, SourceType
from main.schema.job import DiffAction, JobManifest, ManifestEntry
from main.schema.service import ChunkConfig, DataModelConfig, S3Config


class FakeEmbedder:
    def __init__(self, dimensions: int = 3, fail: bool = False) -> None:
        self.dimensions = dimensions
        self.fail = fail
        self.calls: list[list[str]] = []

    def embed_texts(self, texts):
        items = list(texts)
        self.calls.append(items)
        if self.fail:
            raise RuntimeError("embedding service unavailable")
        return [
            [float(index + 1)] * self.dimensions
            for index, _ in enumerate(items)
        ]


class FakeS3Connector:
    def __init__(self, bucket: str, objects: list[SourceObject]) -> None:
        self.config = type("Config", (), {"bucket": bucket})()
        self.objects = objects

    def iter_objects(self):
        return iter(self.objects)


def _config() -> DatasetConfig:
    return DatasetConfig(
        dataset="demo",
        sources=[],
        data_model=DataModelConfig(),
    )


def _document(doc_id: str, text: str = "hello world") -> Document:
    return Document(
        doc_id=doc_id,
        signature=f"sig-{doc_id}",
        source_type=SourceType.S3_OBJECT,
        raw_content=text,
        metadata={"source_id": doc_id},
    )


def _entry(source_id: str, signature: str, action: DiffAction) -> ManifestEntry:
    return ManifestEntry(
        doc_id=source_id,
        signature=signature,
        source_type=SourceType.S3_OBJECT,
        action=action,
        source=f"s3://bucket/{source_id}",
        metadata={"source_name": "files", "key": source_id},
    )


def _old_chunk(source_id: str, doc_id: str = "old-doc") -> Chunk:
    return Chunk(
        chunk_id=f"old-{source_id}",
        doc_id=doc_id,
        source_id=source_id,
        source_type=SourceType.S3_OBJECT,
        chunk_index=0,
        text="old text",
        embedding=[0.0, 0.0, 0.0],
    )


def test_build_manifest_classifies_new_modified_unchanged_and_deleted():
    identifiers = {
        "same": IdentifierRecord(
            source_id="same", signature="same-sig",
            source_type=SourceType.S3_OBJECT.value,
        ),
        "changed": IdentifierRecord(
            source_id="changed", signature="old-sig",
            source_type=SourceType.S3_OBJECT.value,
        ),
        "gone": IdentifierRecord(
            source_id="gone", signature="gone-sig",
            source_type=SourceType.S3_OBJECT.value,
            source="s3://bucket/gone",
        ),
    }
    items = [
        SourceItem("new", "new-sig", SourceType.S3_OBJECT, "s3://bucket/new", {}),
        SourceItem("same", "same-sig", SourceType.S3_OBJECT, "s3://bucket/same", {}),
        SourceItem("changed", "new-sig", SourceType.S3_OBJECT, "s3://bucket/changed", {}),
    ]

    manifest = build_manifest(
        job_id="job-1", dataset="demo", items=items, identifiers=identifiers,
    )

    actions = {entry.doc_id: entry.action for entry in manifest.entries}
    assert actions == {
        "new": DiffAction.NEW,
        "changed": DiffAction.MODIFIED,
        "gone": DiffAction.DELETED,
    }


def test_build_manifest_force_reprocess_marks_unchanged_items_modified():
    identifiers = {
        "same": IdentifierRecord(
            source_id="same", signature="same-sig",
            source_type=SourceType.S3_OBJECT.value,
        ),
    }
    items = [
        SourceItem("same", "same-sig", SourceType.S3_OBJECT, "s3://bucket/same", {}),
    ]

    manifest = build_manifest(
        job_id="job-1",
        dataset="demo",
        items=items,
        identifiers=identifiers,
        force_reprocess=True,
    )

    assert [(entry.doc_id, entry.action) for entry in manifest.entries] == [
        ("same", DiffAction.MODIFIED),
    ]


def test_processor_replaces_changed_sources_and_commits_identifiers():
    store = MemoryDatasetStore()
    writer = MemoryChunkWriter()
    writer.append_chunks([_old_chunk("src-modified"), _old_chunk("src-deleted")])

    write_identifiers(store, [
        IdentifierRecord(
            source_id="src-modified", signature="sig-v1",
            source_type=SourceType.S3_OBJECT.value,
            doc_ids=["old-doc"],
        ),
        IdentifierRecord(
            source_id="src-deleted", signature="sig-del",
            source_type=SourceType.S3_OBJECT.value,
            doc_ids=["old-doc"],
        ),
    ])

    documents = {
        "src-new": [_document("new-doc")],
        "src-modified": [_document("modified-doc", "updated text")],
        "src-deleted": [],
    }
    embedder = FakeEmbedder()
    manifest = JobManifest(
        job_id="job-1",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[
            _entry("src-new", "sig-new", DiffAction.NEW),
            _entry("src-modified", "sig-v2", DiffAction.MODIFIED),
            _entry("src-deleted", "sig-del", DiffAction.DELETED),
        ],
    )

    progress = JobProcessor(
        config=_config(),
        store=store,
        writer=writer,
        embedder=embedder,
        document_loader=lambda entry: documents[entry.doc_id],
    ).run(manifest)

    assert progress.processed_items == 3
    assert progress.completed_batches == 1
    assert set(writer.by_source) == {"src-new", "src-modified"}

    new_chunks = writer.by_source["src-new"]
    modified_chunks = writer.by_source["src-modified"]
    assert new_chunks[0].doc_id == "new-doc"
    assert modified_chunks[0].doc_id == "modified-doc"
    assert all(chunk.job_id == "job-1" for chunk in new_chunks + modified_chunks)
    assert all(len(chunk.embedding) == 3 for chunk in new_chunks + modified_chunks)

    assert store.exists(receipt_key("job-1", 1))
    receipt = json.loads(store.read_bytes(receipt_key("job-1", 1)))
    assert receipt["item_count"] == 3
    assert receipt["end_index"] == 3

    identifiers = load_identifiers(store)
    assert set(identifiers) == {"src-new", "src-modified"}
    assert identifiers["src-new"].doc_ids == ["new-doc"]
    assert identifiers["src-modified"].signature == "sig-v2"
    assert identifiers["src-modified"].doc_ids == ["modified-doc"]
    assert all(record.job_id == "job-1" for record in identifiers.values())
    assert all(
        record.config_hash == config_fingerprint(_config())
        for record in identifiers.values()
    )


def test_processor_leaves_state_untouched_when_embedding_fails():
    store = MemoryDatasetStore()
    writer = MemoryChunkWriter()
    writer.append_chunks([_old_chunk("src-modified")])

    previous = IdentifierRecord(
        source_id="src-modified", signature="sig-v1",
        source_type=SourceType.S3_OBJECT.value,
        doc_ids=["old-doc"],
    )
    write_identifiers(store, [previous])

    manifest = JobManifest(
        job_id="job-fail",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[_entry("src-modified", "sig-v2", DiffAction.MODIFIED)],
    )

    with pytest.raises(RuntimeError, match="embedding service unavailable"):
        JobProcessor(
            config=_config(),
            store=store,
            writer=writer,
            embedder=FakeEmbedder(fail=True),
            document_loader=lambda entry: [_document("modified-doc")],
        ).run(manifest)

    assert not store.exists(receipt_key("job-fail", 1))
    assert {chunk.chunk_id for chunk in writer.all_chunks()} == {"old-src-modified"}
    identifiers = load_identifiers(store)
    assert identifiers["src-modified"].signature == "sig-v1"
    assert identifiers["src-modified"].doc_ids == ["old-doc"]


def test_first_run_prepare_and_process_produces_queryable_state():
    store = MemoryDatasetStore()
    source = SourceConfig(
        name="files",
        connection=S3Config(
            type="s3",
            bucket="raw-bucket",
            access_key_id="key",
            secret_access_key="secret",
            prefix="raw/",
        ),
        options={"namespace": "supplier-files"},
    )
    config = DatasetConfig(dataset="supplier", sources=[source])
    connectors = {
        "files": FakeS3Connector(
            "raw-bucket",
            [SourceObject(key="raw/contract.txt", size=12, etag="etag-1")],
        ),
    }

    manifest = prepare_job(
        job_id="job-1",
        config=config,
        store=store,
        connectors=connectors,
    )
    writer = MemoryChunkWriter()
    JobProcessor(
        config=config,
        store=store,
        writer=writer,
        embedder=FakeEmbedder(),
        document_loader=lambda entry: [_document("doc-1", "hello world")],
    ).run(manifest)

    records = load_committed_identifiers(store, dataset="supplier")
    assert len(records) == 1
    record = next(iter(records.values()))
    assert record.job_id == "job-1"
    assert record.config_hash == config_fingerprint(config)

    chunks = writer.all_chunks()
    assert chunks
    assert all(
        chunk.source_id == record.source_id
        and chunk.job_id == record.job_id
        for chunk in chunks
    )


def test_processor_keeps_unchanged_source_chunks_and_job_id():
    store = MemoryDatasetStore()
    writer = MemoryChunkWriter()
    old_chunk = _old_chunk("src-unchanged")
    old_chunk.job_id = "job-old"
    writer.append_chunks([old_chunk])

    old_hash = config_fingerprint(_config())
    write_identifiers(store, [IdentifierRecord(
        source_id="src-unchanged", signature="sig-unchanged",
        source_type=SourceType.S3_OBJECT.value,
        doc_ids=["old-doc"],
        config_hash=old_hash,
        job_id="job-old",
    )])

    manifest = JobManifest(
        job_id="job-new",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[],
    )
    progress = JobProcessor(
        config=_config(),
        store=store,
        writer=writer,
        embedder=FakeEmbedder(),
    ).run(manifest)

    assert progress.total_items == 0
    assert {chunk.chunk_id for chunk in writer.all_chunks()} == {
        "old-src-unchanged",
    }
    identifiers = load_identifiers(store)
    assert identifiers["src-unchanged"].job_id == "job-old"
    assert identifiers["src-unchanged"].config_hash == old_hash


def test_processor_commits_empty_identifiers_for_empty_dataset():
    store = MemoryDatasetStore()
    manifest = JobManifest(
        job_id="job-empty",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[],
    )

    JobProcessor(
        config=_config(),
        store=store,
        writer=MemoryChunkWriter(),
        embedder=FakeEmbedder(),
    ).run(manifest)

    assert store.exists("identifiers.parquet")
    assert load_identifiers(store) == {}
    assert load_committed_identifiers(store) == {}


def test_processor_keeps_old_chunks_and_identifiers_when_commit_fails(
    monkeypatch,
):
    store = MemoryDatasetStore()
    writer = MemoryChunkWriter()
    old_chunk = _old_chunk("src-modified")
    old_chunk.job_id = "job-old"
    writer.append_chunks([old_chunk])

    old_hash = config_fingerprint(_config())
    write_identifiers(store, [IdentifierRecord(
        source_id="src-modified", signature="sig-v1",
        source_type=SourceType.S3_OBJECT.value,
        doc_ids=["old-doc"],
        config_hash=old_hash,
        job_id="job-old",
    )])

    manifest = JobManifest(
        job_id="job-fail",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[_entry("src-modified", "sig-v2", DiffAction.MODIFIED)],
    )

    def fail_commit(*args, **kwargs):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(
        "main.process.runner.write_identifiers",
        fail_commit,
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        JobProcessor(
            config=_config(),
            store=store,
            writer=writer,
            embedder=FakeEmbedder(),
            document_loader=lambda entry: [_document("modified-doc")],
        ).run(manifest)

    chunks = writer.all_chunks()
    assert any(chunk.chunk_id == "old-src-modified" for chunk in chunks)
    assert any(chunk.job_id == "job-fail" for chunk in chunks)

    identifiers = load_identifiers(store)
    assert identifiers["src-modified"].job_id == "job-old"
    assert identifiers["src-modified"].doc_ids == ["old-doc"]


def test_processor_retry_same_job_replaces_partial_chunks():
    store = MemoryDatasetStore()
    writer = MemoryChunkWriter()
    partial_chunk = _old_chunk("src-modified")
    partial_chunk.job_id = "job-1"
    writer.append_chunks([partial_chunk])

    write_identifiers(store, [IdentifierRecord(
        source_id="src-modified", signature="sig-v1",
        source_type=SourceType.S3_OBJECT.value,
        doc_ids=["old-doc"],
        config_hash=config_fingerprint(_config()),
        job_id="job-old",
    )])

    manifest = JobManifest(
        job_id="job-1",
        dataset="demo",
        created_at="2026-10-22T00:00:00+00:00",
        entries=[_entry("src-modified", "sig-v2", DiffAction.MODIFIED)],
    )
    JobProcessor(
        config=_config(),
        store=store,
        writer=writer,
        embedder=FakeEmbedder(),
        document_loader=lambda entry: [_document("modified-doc")],
    ).run(manifest)

    chunks = writer.by_source["src-modified"]
    assert len(chunks) == 1
    assert chunks[0].job_id == "job-1"
    assert chunks[0].chunk_id != "old-src-modified"


def test_lance_writer_deletes_only_older_jobs_for_a_source(tmp_path):
    pytest.importorskip("lance")
    pytest.importorskip("pyarrow")

    uri = str(tmp_path / "data.lance")
    writer = LanceChunkWriter(uri)
    old_chunk = Chunk(
        chunk_id="chunk-old", doc_id="doc-a", source_id="src-a",
        job_id="job-old", source_type=SourceType.S3_OBJECT,
        chunk_index=0, text="old", embedding=[1.0, 2.0, 3.0],
    )
    new_chunk = Chunk(
        chunk_id="chunk-new", doc_id="doc-a", source_id="src-a",
        job_id="job-new", source_type=SourceType.S3_OBJECT,
        chunk_index=0, text="new", embedding=[4.0, 5.0, 6.0],
    )

    writer.append_chunks([old_chunk, new_chunk])
    writer.delete_source_except_job("src-a", "job-new")

    import lance
    rows = lance.dataset(uri).to_table().to_pylist()
    assert [row["chunk_id"] for row in rows] == ["chunk-new"]


def test_lance_writer_appends_and_deletes_sources_locally(tmp_path):
    pytest.importorskip("lance")
    pytest.importorskip("pyarrow")

    uri = str(tmp_path / "data.lance")
    writer = LanceChunkWriter(uri)
    first = _document("doc-a")
    second = _document("doc-b")
    chunk_a = Chunk(
        chunk_id="chunk-a", doc_id=first.doc_id, source_id="src-a",
        source_type=SourceType.S3_OBJECT, chunk_index=0, text="a",
        embedding=[1.0, 2.0, 3.0],
    )
    chunk_b = Chunk(
        chunk_id="chunk-b", doc_id=second.doc_id, source_id="src-b",
        source_type=SourceType.S3_OBJECT, chunk_index=0, text="b",
        embedding=[4.0, 5.0, 6.0],
    )

    assert writer.append_chunks([chunk_a, chunk_b]) == 2
    writer.delete_source("src-a")

    import lance
    rows = lance.dataset(uri).to_table().to_pylist()
    assert [row["chunk_id"] for row in rows] == ["chunk-b"]


def test_target_from_env_reads_only_documented_s3_variables():
    config = target_from_env({
        "S3_BUCKET": "sab-data",
        "S3_REGION": "us-west-2",
        "S3_PREFIX": "datasets",
        "S3_ENDPOINT_URL": "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
    })

    assert config == DatasetTarget(
        type="s3",
        bucket="sab-data",
        access_key_id="key",
        secret_access_key="secret",
        region="us-west-2",
        endpoint_url="http://127.0.0.1:9000",
        prefix="datasets",
    )
    assert config.uri == "s3://sab-data/datasets"


def test_dataset_and_job_models_accept_a_byob_target():
    target = DatasetTarget(
        type="s3",
        bucket="customer-data",
        access_key_id="key",
        secret_access_key="secret",
        prefix="sab/datasets/supplier",
        region="us-west-2",
    )

    config = DatasetConfig(dataset="supplier", target=target)
    job = JobCreate(dataset="supplier", target=target)

    assert config.target == target
    assert job.target.uri == "s3://customer-data/sab/datasets/supplier"
    dumped = job.model_dump_json()
    assert '"secret_access_key":"**********"' in dumped
    assert '"secret_access_key":"secret"' not in dumped


def test_first_run_bootstraps_config_json_without_identifiers():
    store = MemoryDatasetStore()
    config = DatasetConfig(
        dataset="supplier",
        target=DatasetTarget(
            bucket="target-bucket",
            access_key_id="target-key",
            secret_access_key="target-secret",
            prefix="datasets/supplier",
        ),
        sources=[SourceConfig(
            name="files",
            connection=S3Config(
                type="s3",
                bucket="raw-bucket",
                access_key_id="source-access-key",
                secret_access_key="source-secret",
                prefix="raw/",
            ),
            options={"namespace": "supplier-files"},
        )],
    )

    state = load_or_create_config(
        store,
        config,
        target_uri="s3://target-bucket/datasets/supplier",
    )

    assert state.created is True
    assert state.force_reprocess is False
    assert store.exists(CONFIG_KEY)
    assert not store.exists("identifiers.parquet")

    document = json.loads(store.read_bytes(CONFIG_KEY))
    assert document["target_uri"] == "s3://target-bucket/datasets/supplier"
    assert document["config_hash"] == config_fingerprint(config)
    assert document["sources"][0]["connection"]["bucket"] == "raw-bucket"
    assert document["sources"][0]["credentials"] == "runtime"

    text = store.read_bytes(CONFIG_KEY).decode("utf-8")
    assert "source-access-key" not in text
    assert "source-secret" not in text
    assert "target-secret" not in text


def test_config_change_forces_reprocess_and_is_not_committed_until_run_finishes():
    store = MemoryDatasetStore()
    original = DatasetConfig(
        dataset="supplier",
        data_model=DataModelConfig(chunk=ChunkConfig(chunk_size=512)),
    )
    write_dataset_config(store, original, target_uri="s3://bucket/dataset")
    before = store.read_bytes(CONFIG_KEY)

    changed = DatasetConfig(
        dataset="supplier",
        data_model=DataModelConfig(chunk=ChunkConfig(chunk_size=128)),
    )
    state = load_or_create_config(store, changed)

    assert state.force_reprocess is True
    assert store.read_bytes(CONFIG_KEY) == before


def test_prepare_job_bootstraps_first_run_and_reprocesses_on_config_change():
    store = MemoryDatasetStore()
    source = SourceConfig(
        name="files",
        connection=S3Config(
            type="s3",
            bucket="raw-bucket",
            access_key_id="key",
            secret_access_key="secret",
            prefix="raw/",
        ),
        options={"namespace": "supplier-files"},
    )
    config = DatasetConfig(dataset="supplier", sources=[source])
    connectors = {
        "files": FakeS3Connector(
            "raw-bucket",
            [SourceObject(key="raw/contract.txt", size=12, etag="etag-1")],
        ),
    }

    first = prepare_job(
        job_id="job-1",
        config=config,
        store=store,
        connectors=connectors,
    )

    assert store.exists(CONFIG_KEY)
    assert len(first.entries) == 1
    assert first.entries[0].action == DiffAction.NEW

    write_identifiers(store, [IdentifierRecord(
        source_id=first.entries[0].doc_id,
        signature="etag-1",
        source_type=SourceType.S3_OBJECT.value,
        doc_ids=["doc-1"],
        config_hash=config_fingerprint(config),
        job_id="job-1",
    )])

    second = prepare_job(
        job_id="job-2",
        config=config,
        store=store,
        connectors=connectors,
    )
    assert second.entries == []

    changed = DatasetConfig(
        dataset="supplier",
        sources=[source],
        data_model=DataModelConfig(chunk=ChunkConfig(chunk_size=128)),
    )
    third = prepare_job(
        job_id="job-3",
        config=changed,
        store=store,
        connectors=connectors,
    )

    assert [entry.action for entry in third.entries] == [DiffAction.MODIFIED]


def test_identifiers_without_config_are_rejected():
    store = MemoryDatasetStore()
    write_identifiers(store, [IdentifierRecord(
        source_id="source-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT.value,
    )])

    with pytest.raises(DatasetStateError, match="no config.json"):
        load_or_create_config(store, DatasetConfig(dataset="supplier"))


def test_empty_identifiers_without_config_can_bootstrap():
    store = MemoryDatasetStore()
    write_identifiers(store, [])

    state = load_or_create_config(store, DatasetConfig(dataset="supplier"))

    assert state.created is True
    assert store.exists(CONFIG_KEY)


def test_load_committed_identifiers_requires_config_and_identifiers():
    store = MemoryDatasetStore()

    with pytest.raises(DatasetStateError, match="no config.json"):
        load_committed_identifiers(store)

    config = DatasetConfig(dataset="supplier")
    write_dataset_config(store, config)

    with pytest.raises(DatasetStateError, match="no committed identifiers"):
        load_committed_identifiers(store)


def test_load_committed_identifiers_accepts_consistent_state():
    store = MemoryDatasetStore()
    config = DatasetConfig(dataset="supplier")
    write_dataset_config(store, config)
    write_identifiers(store, [IdentifierRecord(
        source_id="source-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT.value,
        config_hash=config_fingerprint(config),
        job_id="job-1",
    )])

    records = load_committed_identifiers(store, dataset="supplier")

    assert records["source-1"].job_id == "job-1"
    assert records["source-1"].config_hash == config_fingerprint(config)


def test_load_committed_identifiers_rejects_mismatched_or_legacy_state():
    store = MemoryDatasetStore()
    config = DatasetConfig(dataset="supplier")
    write_dataset_config(store, config)

    write_identifiers(store, [IdentifierRecord(
        source_id="source-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT.value,
        config_hash="different-config",
        job_id="job-1",
    )])
    with pytest.raises(DatasetStateError, match="different config"):
        load_committed_identifiers(store)

    write_identifiers(store, [IdentifierRecord(
        source_id="source-1",
        signature="sig-1",
        source_type=SourceType.S3_OBJECT.value,
        config_hash=config_fingerprint(config),
    )])
    with pytest.raises(DatasetStateError, match="no committed job_id"):
        load_committed_identifiers(store)


def test_env_file_loader_accepts_the_existing_s3_yaml_format(tmp_path):
    path = tmp_path / "s3.yaml"
    path.write_text(
        'AWS_ACCESS_KEY_ID = "file-key"\n'
        "AWS_SECRET_ACCESS_KEY = file-secret\n"
        "S3_BUCKET = sab-data\n"
        'S3_REGION = "us-west-2"\n'
        "S3_PREFIX = datasets\n"
        'S3_ENDPOINT_URL = "https://example.invalid"\n',
        encoding="utf-8",
    )

    values = read_env_file(path)
    assert values == {
        "AWS_ACCESS_KEY_ID": "file-key",
        "AWS_SECRET_ACCESS_KEY": "file-secret",
        "S3_BUCKET": "sab-data",
        "S3_REGION": "us-west-2",
        "S3_PREFIX": "datasets",
        "S3_ENDPOINT_URL": "https://example.invalid",
    }

    config = target_from_file(path)
    assert config.bucket == "sab-data"
    assert config.region == "us-west-2"
    assert config.prefix == "datasets"
    assert config.access_key_id == "file-key"
    assert config.secret_access_key.get_secret_value() == "file-secret"


def test_load_env_file_sets_variables_without_overriding_by_default(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "s3.yaml"
    path.write_text(
        "S3_BUCKET = from-file\n"
        "S3_REGION = from-file\n"
        "AWS_ACCESS_KEY_ID = key\n"
        "AWS_SECRET_ACCESS_KEY = secret\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("S3_BUCKET", "already-set")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "existing-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "existing-secret")
    monkeypatch.delenv("S3_REGION", raising=False)

    loaded = load_env_file(path)

    assert loaded == {
        "S3_BUCKET", "S3_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    }
    assert os.environ["S3_BUCKET"] == "already-set"
    assert os.environ["S3_REGION"] == "from-file"
    assert os.environ["AWS_ACCESS_KEY_ID"] == "existing-key"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "existing-secret"
