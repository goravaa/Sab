from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from main.embed.embeddings import TextEmbedder
from main.process.config import DatasetConfig, SourceConfig, target_from_file
from main.process.identifiers import load_identifiers
from main.process.runner import load_manifest, run_job
from main.process.state import CONFIG_KEY, load_committed_identifiers
from main.process.store import S3DatasetStore
from main.process.writer import lance_storage_options
from main.schema.job import DiffAction
from main.schema.service import (
    ChunkConfig,
    DataModelConfig,
    EmbedderConfig,
    S3Config,
)


pytestmark = pytest.mark.integration

TARGET_ENV_FILE = Path(__file__).resolve().parents[3] / "s3.yaml"


ALPHA = """# Alpha agreement

This is the first paragraph of the alpha agreement.

## Terms

Payment is due within thirty days. The customer may cancel with notice.

## Support

Support is available during normal business hours.
"""

BETA = """# Beta notes

Beta is a short support note used by the pipeline smoke test.
"""

ORDERS = """order_id,customer_id,total,status
ORD-E2E-001,CUST-E2E-001,120.50,paid
ORD-E2E-002,CUST-E2E-002,88.00,pending
ORD-E2E-003,CUST-E2E-003,42.25,paid
"""


def _embedder(env: dict[str, str]):
    base_url = (env.get("SAB_EMBED_BASE_URL") or "").strip()
    if not base_url:
        pytest.fail(
            "SAB_EMBED_BASE_URL must be set in integration/docker/.env; "
            "the E2E pipeline test requires a live embedding endpoint."
        )

    return TextEmbedder(
        EmbedderConfig(
            model=env.get("SAB_EMBED_MODEL", "all-minilm"),
            batch_size=16,
        ),
        base_url=base_url,
        api_key=env.get("SAB_EMBED_API_KEY", "ollama"),
    )


def _list_keys(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def _delete_prefix(client, bucket: str, prefix: str) -> None:
    keys = _list_keys(client, bucket, prefix)
    for start in range(0, len(keys), 1000):
        batch = keys[start:start + 1000]
        if batch:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in batch]},
            )


def _lance_rows(store: S3DatasetStore, target: DatasetTarget) -> list[dict]:
    import lance

    dataset = lance.dataset(
        store.uri("data.lance"),
        storage_options=lance_storage_options(target),
    )
    return dataset.to_table().to_pylist()


def test_pipeline_writes_queryable_state_and_reprocesses_changes(pipeline_s3):
    env = pipeline_s3["env"]
    client = pipeline_s3["client"]
    bucket = pipeline_s3["bucket"]

    run_id = uuid4().hex[:10]
    source_prefix = f"pipeline-e2e/{run_id}/source/"
    target_prefix = f"pipeline-e2e/{run_id}/target/"
    dataset_name = f"smoke-{run_id}"
    cleanup_prefix = f"pipeline-e2e/{run_id}/"

    objects = {
        f"{source_prefix}notes/alpha.md": ALPHA,
        f"{source_prefix}notes/beta.md": BETA,
        f"{source_prefix}exports/orders.csv": ORDERS,
    }

    source = SourceConfig(
        name="files",
        connection=S3Config(
            type="s3",
            bucket=bucket,
            access_key_id=env["MINIO_ROOT_USER"],
            secret_access_key=env["MINIO_ROOT_PASSWORD"],
            region="us-east-1",
            endpoint_url=f"http://127.0.0.1:{env.get('MINIO_API_PORT', '9000')}",
            prefix=source_prefix,
        ),
        options={"namespace": dataset_name, "id_column": "order_id"},
    )
    if not TARGET_ENV_FILE.exists():
        pytest.fail(
            f"Missing {TARGET_ENV_FILE}; the pipeline target bucket is "
            "configured there."
        )
    target = target_from_file(TARGET_ENV_FILE, prefix=target_prefix)
    target_client = boto3.client(
        "s3",
        aws_access_key_id=target.access_key_id,
        aws_secret_access_key=target.secret_access_key.get_secret_value(),
        region_name=target.region,
        endpoint_url=target.endpoint_url,
    )
    config = DatasetConfig(
        dataset=dataset_name,
        sources=[source],
        data_model=DataModelConfig(
            chunk=ChunkConfig(chunk_size=128, chunk_overlap=16),
            embedder=EmbedderConfig(
                model=env.get("SAB_EMBED_MODEL", "all-minilm"),
                batch_size=16,
            ),
        ),
        item_batch_size=10,
    )
    store = S3DatasetStore(target, dataset_name)
    embedder = _embedder(env)

    try:
        for key, body in objects.items():
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body.encode("utf-8"),
            )

        job_one = f"{run_id}-1"
        progress_one = run_job(
            job_id=job_one,
            config=config,
            target=target,
            store=store,
            embedder=embedder,
        )
        manifest_one = load_manifest(store, job_one)

        assert progress_one.total_items == 3
        assert progress_one.processed_items == 3
        assert manifest_one is not None
        assert {entry.action for entry in manifest_one.entries} == {DiffAction.NEW}

        document = json.loads(store.read_bytes(CONFIG_KEY))
        records = load_committed_identifiers(store, dataset=dataset_name)
        assert document["target_uri"] == store.uri("")
        assert len(records) == 3
        assert all(record.job_id == job_one for record in records.values())

        rows = _lance_rows(store, target)
        valid = {(source_id, record.job_id) for source_id, record in records.items()}
        assert rows
        assert all(row["embedding"] for row in rows)
        assert all((row["source_id"], row["job_id"]) in valid for row in rows)

        keys = _list_keys(target_client, target.bucket, store.root)
        assert f"{store.root}/config.json" in keys
        assert f"{store.root}/identifiers.parquet" in keys
        assert any("/data.lance/" in key for key in keys)
        assert f"{store.root}/jobs/{job_one}/manifest.json" in keys
        assert f"{store.root}/jobs/{job_one}/progress/batch_0001.json" in keys

        job_two = f"{run_id}-2"
        progress_two = run_job(
            job_id=job_two,
            config=config,
            target=target,
            store=store,
            embedder=embedder,
        )
        manifest_two = load_manifest(store, job_two)

        assert progress_two.total_items == 0
        assert progress_two.processed_items == 0
        assert manifest_two is not None
        assert manifest_two.entries == []

        updated_alpha = ALPHA + "\n## Renewal\n\nThis agreement renews annually.\n"
        client.put_object(
            Bucket=bucket,
            Key=f"{source_prefix}notes/alpha.md",
            Body=updated_alpha.encode("utf-8"),
        )

        job_three = f"{run_id}-3"
        progress_three = run_job(
            job_id=job_three,
            config=config,
            target=target,
            store=store,
            embedder=embedder,
        )
        manifest_three = load_manifest(store, job_three)

        assert progress_three.total_items == 1
        assert manifest_three is not None
        assert [entry.action for entry in manifest_three.entries] == [
            DiffAction.MODIFIED,
        ]

        records_after = load_committed_identifiers(store, dataset=dataset_name)
        alpha = next(
            record
            for record in records_after.values()
            if record.source.endswith("alpha.md")
        )
        assert alpha.job_id == job_three
        assert all(
            record.job_id == job_one
            for record in records_after.values()
            if not record.source.endswith("alpha.md")
        )

        rows_after = _lance_rows(store, target)
        valid_after = {
            (source_id, record.job_id)
            for source_id, record in records_after.items()
        }
        assert rows_after
        assert all(
            (row["source_id"], row["job_id"]) in valid_after
            for row in rows_after
        )

        alpha_rows = [
            row for row in rows_after
            if row["source_id"] == alpha.source_id
        ]
        assert alpha_rows
        assert all(row["job_id"] == job_three for row in alpha_rows)
        assert len(rows_after) == len([
            row for row in rows_after
            if (row["source_id"], row["job_id"]) in valid_after
        ])
        assert set(load_identifiers(store)) == set(records_after)
    finally:
        _delete_prefix(client, bucket, cleanup_prefix)
        _delete_prefix(target_client, target.bucket, target_prefix)
