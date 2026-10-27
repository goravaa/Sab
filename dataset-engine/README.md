# Dataset Engine API

The HTTP layer lives in `api/`; `main/` stays the framework independent engine
library. The API is split into `routes/`, `services/`, and `schemas.py`.
Install its dependencies and launch it from `dataset-engine/`:

```sh
python -m pip install -r api/requirements.txt
cp config/workspace.example.yaml config/workspace.yaml
uvicorn api.app:app --host 127.0.0.1 --port 8000
```

The normal UI path is workspace-driven. Edit `config/workspace.yaml` with
your dataset, target bucket, and any number of source systems, then reload the
page. The API reads the file on every request, so no restart is needed.

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
