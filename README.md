# Sab

Sab indexes data sources into S3 datasets (dataset-engine) and queries the
committed, verified index read-only (query-service).

## v0.1.0 capabilities and limits

This release ships two Docker images, both tagged `0.1.0`:

- `garvw/sab-dataset-engine:0.1.0` indexes configured PostgreSQL tables and
  S3 objects into a committed S3 dataset. S3 inputs support CSV, XLSX/XLSM,
  PDF, DOCX, Markdown, and plain text. PostgreSQL password and Aurora
  PostgreSQL IAM authentication are supported.
- `garvw/sab-query-service:0.1.0` serves read-only MCP tools over stdio or
  Streamable HTTP. It can inspect and audit committed datasets, search text
  literally or semantically, find and aggregate structured rows, and run
  bounded read-only SQL.

The services are separate and can run independently. The dataset engine
provides a simple HTML workspace UI and HTTP API for status, reconciliation,
and indexing jobs. Query-service does not index, change, or repair data and
does not host a chat model; the MCP client supplies the model that chooses
tools. Both services use S3 for committed dataset storage, and semantic search
requires an OpenAI-compatible embedding endpoint serving the same model used
at indexing time. Changing embedding models requires reindexing.

This is an initial release: source connectors are PostgreSQL and S3; query SQL
runs over committed chunks and is capped at 100 result rows. Sab is not a
general database proxy, arbitrary S3 query interface, or write-capable SQL
service. See the service READMEs for configuration and operational details.
The root Compose stack defaults to the matching 0.1.0 images; set
`DATASET_ENGINE_IMAGE` or `QUERY_SERVICE_IMAGE` to override them.

See [CHANGELOG.md](CHANGELOG.md) for the v0.1.0 release notes.
Maintainers can find Docker Hub setup and release steps in
[docs/releasing.md](docs/releasing.md).

## Quickstart: both services on one machine

```sh
cp .env.example .env                       # future glue only; works empty
# edit dataset-engine/config/workspace.yaml — the single central config
docker compose up -d                       # dataset-engine :8000, query MCP :8080
docker compose --profile gpu up -d         # replaces host Ollama with vLLM bge-m3
```

- One workspace.yaml drives both sides: mounted read-only into both
  containers. dataset-engine indexes the `dataset`/`target`/`sources`
  sections; query-service serves the committed dataset from the same file.
- One embedding endpoint drives both indexing and semantic_search: the
  `embeddings:` section in workspace.yaml (Compose overrides for containers,
  `--profile gpu` overrides for vLLM; env only when you want it).
- `.env` is deployment glue only (URL overrides, user/value config does not
  belong there) and can stay empty in the common case.

MCP access to query-service is Streamable HTTP on port 8080 by
default (`SAB_MCP_TRANSPORT`); each image also runs standalone — see
`dataset-engine/README.md` and `query-service/README.md`.
