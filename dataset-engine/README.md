# Dataset Engine API

The HTTP layer lives in `api/`; `main/` stays the framework independent engine
library. The API is split into `routes/`, `services/`, and `schemas.py`.
Install its dependencies and launch it from `dataset-engine/`:

```sh
python -m pip install -r api/requirements.txt
cp config/workspace.example.yaml config/workspace.yaml
uvicorn api.app:app --host 127.0.0.1 --port 8000
```

### Docker

The v0.1.0 image is `garvw/sab-dataset-engine:0.1.0`. From this directory,
create `config/workspace.yaml` and start the dataset engine and vLLM GPU
embedding service with Docker Compose:

```sh
cp config/workspace.example.yaml config/workspace.yaml
docker compose pull
docker compose up -d
```

To build from source instead, run `docker build -t garvw/sab-dataset-engine:local .`
and set `DATASET_ENGINE_IMAGE` to that tag.

Compose ships one profile-free path and one optional profile:

- **CPU host with Ollama (default, e.g. the EC2 instance):** `docker compose up -d`
  runs only the dataset engine and points `SAB_EMBED_BASE_URL` at the host
  Ollama through `host.docker.internal:11434`. Ollama must listen on the
  Docker bridge interface, so set `OLLAMA_HOST=0.0.0.0` in its systemd unit
  (or one-off export) and restart it; keep port 11434 firewalled away from
  the internet. The embedder model stays whatever `data_model.embedder.model`
  in `workspace.yaml` names (leave `SAB_EMBED_MODEL` unset).
- **GPU host with vLLM:** `docker compose --profile gpu up -d` additionally
  starts vLLM with `BAAI/bge-m3`. Then set both values in the `.env` file:
  `SAB_EMBED_BASE_URL=http://vllm:8000/v1` and `SAB_EMBED_MODEL=BAAI/bge-m3`.
  The same model must be used for indexing and querying; changing models
  requires reindexing.

Compose mounts the workspace read-only and binds the API to `127.0.0.1:8000`
by default. Set `DATASET_ENGINE_BIND` only when a trusted reverse proxy or
other access control protects the service. On AWS, leave AWS credentials out
of Compose so the container can use its instance role through IMDS. For local
AWS credentials, put `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and (when
using temporary credentials) `AWS_SESSION_TOKEN` in an untracked
`.aws-credentials.env` file in this directory; Compose loads it only when the
file exists. Do not create an empty credential file, since empty credential
variables can override the standard AWS credential chain.
The image includes the engine and API dependencies but does not include
workspace secrets or local configuration. Remember the container runs as the
non-root `app` user: a workspace file with mode `0600` owned by another host
user is unreadable inside the container; grant the container group read
access and keep the read-only mount (`docker compose exec -T dataset-engine id`
prints the group to own the file with).

The normal UI path is workspace-driven. Edit `config/workspace.yaml` with
your dataset, target bucket, and any number of source systems, then reload the
page. The API reads the file on every request, so no restart is needed. If the
page is refreshed during indexing, it restores the active job and resumes
showing progress from the saved job receipts.

```yaml
dataset: personal
target:
  bucket: sab-data
  prefix: sab-datasets
  region: us-west-2
sources:
  - name: supplier-db
    label: Supplier database
    description: Orders and products.
    connection:
      type: postgres
      host: 127.0.0.1
      port: 5432
      database: dev_db
      username: devuser
      password: ${POSTGRES_PASSWORD}
      # For IAM auth, omit password and set auth_mode: iam plus region: us-west-2.
  - name: supplier-exports
    label: Supplier exports
    description: PDFs and spreadsheets.
    connection:
      type: s3
      bucket: sab-integration
      prefix: supplier-demo/
      region: us-east-1
      endpoint_url: http://127.0.0.1:9000
      access_key_id: devminio
      secret_access_key: devminio123
```

S3 sources and the target may omit credentials to use the standard AWS chain
(environment, profile, SSO, or instance role). Set `profile` to choose a named
profile. `${ENV_VAR}` values are expanded from the server environment.

Postgres sources support both password authentication (the default) and Aurora
PostgreSQL IAM authentication. For IAM, set `auth_mode: iam`, set the AWS
`region`, omit `password`, and run the engine with AWS credentials that have
`rds-db:connect` permission for the database user. The connector generates a
fresh IAM token for every database connection and requires TLS. AWS credentials
come from the standard boto3 chain, including `AWS_PROFILE`, SSO, environment
credentials, or an instance role.

### Embeddings

Indexing requires the OpenAI Python client and an OpenAI-compatible embeddings
endpoint. The API requirements install the client. The model name is selected
by `data_model.embedder.model` in the workspace file. For the local Ollama
service on the EC2 instance, export these variables in the shell that starts
the API:

```sh
export SAB_EMBED_BASE_URL=http://127.0.0.1:11434/v1
export SAB_EMBED_API_KEY=ollama
```

`ollama` is a placeholder key required by the OpenAI client; local Ollama
ignores it. The supplier workspace uses the `all-minilm` model. Keep Ollama
bound to localhost; the API should call it over `127.0.0.1` from the same EC2
instance.

The engine also accepts any OpenAI-compatible embeddings server, including
vLLM's embeddings endpoint. Run a supported pooling/embedding model on a GPU
machine, then point both the dataset engine and query service at that endpoint:

```sh
export SAB_EMBED_BASE_URL=http://gpu-embedder.internal:8000/v1
export SAB_EMBED_API_KEY=your-vllm-api-key
```

Set `data_model.embedder.model` to the model name served by vLLM. The same
model and endpoint must be used for indexing and querying. Changing embedding
models requires reindexing the dataset so all stored vectors use the same
model. Keep the GPU server on a private network and configure its API key.
vLLM supports the OpenAI-compatible `/v1/embeddings` API for embedding models;
see the [vLLM embedding documentation](https://docs.vllm.ai/en/stable/models/pooling_models/embed/).

The UI exposes workspace status, per-source indexed counts, target state,
change preview, and indexing. Credentials are read only by the server and are
never returned to the browser. `config/workspace.yaml` is gitignored; keep it
at mode `600`.

The underlying HTTP API also exposes `GET /health`,
`GET /v1/workspace`, `GET /v1/workspace/status`,
`POST /v1/workspace/reconcile`, `POST /v1/workspace/jobs/stream`, plus the
legacy BYOB routes `POST /v1/sources/check`,
`POST /v1/datasets/reconcile`, `POST /v1/jobs`,
`POST /v1/jobs/stream`, and `POST /v1/jobs/{job_id}/status`. Job status is
written to the target alongside the manifest and batch receipts.

The streaming endpoint responds with `text/event-stream`. It sends a `job`
event when the job is accepted, a `progress` event after each batch receipt is
written, then a `completed` or `failed` event. Workspace submissions carry no
credentials; the legacy BYOB endpoint still accepts them in the POST body.
Browser clients should read either stream with `fetch()` rather than native
`EventSource` (which only supports GET).
Each progress event contains `progress.processed_items`,
`progress.total_items`, `progress.completed_batches`, and `progress.ratio`.

Open `http://127.0.0.1:8000/` for the plain HTML interface. The API binds to
loopback by default; do not expose it on `0.0.0.0` without adding TLS and
authentication. The page is served directly by FastAPI and has no frontend
build step.

Logging is configured centrally in `api/logging_config.py` and installed by
`api/app.py`. Set `SAB_LOG_LEVEL` (default `INFO`) and optionally
`SAB_LOG_FORMAT` to control output. Every request reuses or receives an
`X-Request-ID` response header, and request logs include that ID. Request
bodies and credentials are never logged.
