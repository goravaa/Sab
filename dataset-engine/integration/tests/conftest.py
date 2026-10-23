from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
import psycopg
import pytest

REPO = Path(__file__).resolve().parents[2]
INTEGRATION = REPO / "integration"
DOCKER_DIR = INTEGRATION / "docker"
ENV_FILE = DOCKER_DIR / ".env"
COMPOSE_FILE = DOCKER_DIR / "docker-compose.yaml"
GENERATOR = INTEGRATION / "data" / "generate_supplier_dataset.py"
SEED = 42
BUCKET = "sab-integration"
PREFIX = f"supplier-demo/seed-{SEED:04d}/"


@dataclass
class IntegrationData:
    postgres_config: dict[str, Any] = field(repr=False)
    s3_config: dict[str, Any] = field(repr=False)
    embed_config: dict[str, Any] = field(repr=False)
    manifest: dict[str, Any]
    objects: tuple[Any, ...] = field(repr=False)


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("\"'")
    return values


def compose(*args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    command = [
        "docker", "compose", "--env-file", str(ENV_FILE),
        "-f", str(COMPOSE_FILE), *args,
    ]
    return subprocess.run(
        command,
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
        timeout=300,
    )


def wait_for_services(env: dict[str, str], timeout: int = 120) -> None:
    pg_port = int(env.get("POSTGRES_PORT", "5432"))
    s3_port = int(env.get("MINIO_API_PORT", "9000"))
    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["MINIO_ROOT_USER"],
        aws_secret_access_key=env["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        endpoint_url=f"http://127.0.0.1:{s3_port}",
    )
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host="127.0.0.1",
                port=pg_port,
                dbname=env["POSTGRES_DB"],
                user=env["POSTGRES_USER"],
                password=env["POSTGRES_PASSWORD"],
                connect_timeout=3,
            ) as connection:
                connection.execute("SELECT 1").fetchone()
            s3.list_buckets()
            return
        except Exception as exc:  # services can need a few seconds after Compose starts
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Local services did not become ready in {timeout}s: {last_error}")


@pytest.fixture(scope="session")
def integration_data() -> IntegrationData:
    if not ENV_FILE.exists():
        pytest.fail(
            f"Missing {ENV_FILE}. Create it from {DOCKER_DIR / '.env.example'} first."
        )
    env = os.environ.copy()
    env.update(read_env_file(ENV_FILE))

    result = compose("up", "-d", "--wait", "--wait-timeout", "120", "postgres", "minio", env=env)
    print(result.stdout)
    wait_for_services(env)

    generated = subprocess.run(
        [sys.executable, str(GENERATOR), "--seed", str(SEED)],
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=1800,
    )
    if generated.returncode:
        pytest.fail(f"Dataset generation failed:\n{generated.stdout}")
    print(generated.stdout)

    db_config = {
        "host": "127.0.0.1",
        "port": int(env.get("POSTGRES_PORT", "5432")),
        "dbname": env["POSTGRES_DB"],
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
    }
    s3_config = {
        "bucket": BUCKET,
        "access_key": env["MINIO_ROOT_USER"],
        "secret_key": env["MINIO_ROOT_PASSWORD"],
        "endpoint": f"http://127.0.0.1:{env.get('MINIO_API_PORT', '9000')}",
    }
    embed_config = {
        "base_url": env.get("SAB_EMBED_BASE_URL", "").strip(),
        "api_key": env.get("SAB_EMBED_API_KEY", "ollama"),
        "model": env.get("SAB_EMBED_MODEL", "bge-m3"),
    }
    manifest_path = INTEGRATION / "generated" / f"seed-{SEED:04d}" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    from main.connect.object_storage.s3 import S3Config, S3Connector

    connector = S3Connector(S3Config(
        type="s3",
        bucket=BUCKET,
        access_key_id=s3_config["access_key"],
        secret_access_key=s3_config["secret_key"],
        endpoint_url=s3_config["endpoint"],
        region="us-east-1",
        prefix=PREFIX,
    ))
    connector.check_connection()
    objects = tuple(connector.iter_objects())

    return IntegrationData(
        postgres_config=db_config,
        s3_config=s3_config,
        embed_config=embed_config,
        manifest=manifest,
        objects=objects,
    )


@pytest.fixture(scope="session")
def object_by_key(integration_data: IntegrationData) -> dict[str, Any]:
    return {obj.key: obj for obj in integration_data.objects}


@pytest.fixture(scope="session")
def object_suffix_counts(integration_data: IntegrationData) -> Counter[str]:
    return Counter(Path(obj.key).suffix.lower() for obj in integration_data.objects)
