# sab

Index data you own into S3, then ask questions about the committed dataset
through MCP. The indexer and query service run as Docker containers, and you
choose the AWS account, embedding endpoint, and MCP host they use.

## Working demo

[Watch the Sab demo](https://youtu.be/g98wnnadK9I)

## Why this exists

I wanted a way to ask useful questions about documents and database exports
using infrastructure I control. Sab writes the index to an S3 bucket I
choose, then exposes a small set of read-only tools to an MCP client. The
client brings the chat model; Sab doesn’t host one.

## What it does

Sab has two services:

- **dataset-engine** reads PostgreSQL tables and objects in S3, extracts and
  chunks their contents, creates embeddings, and writes a committed dataset
  back to S3. It includes a small web UI and HTTP API for workspace status,
  change previews, indexing jobs, and progress.
- **query-service** reads the committed dataset and exposes MCP tools to
  inspect and audit it, search text, find structured rows, calculate
  aggregates, and run bounded read-only SQL.

S3 inputs support CSV, XLSX/XLSM, PDF, DOCX, Markdown, and plain text.
PostgreSQL password authentication and Aurora PostgreSQL IAM authentication
are supported. Both services use the same `workspace.yaml`; the query service
only reads committed data, while the dataset engine writes new commits.

## How to use

You need Docker Compose, an AWS identity that can access your source and
target buckets, and an OpenAI-compatible embeddings endpoint serving the model
configured in the workspace. For a local Ollama endpoint, it must be
reachable from the containers; for a GPU setup, Compose can start vLLM.

Start with the example workspace and edit the dataset name, target bucket,
sources, credentials, and embedding model:

```sh
cp .env.example .env
cp dataset-engine/config/workspace.example.yaml dataset-engine/config/workspace.yaml
# Edit dataset-engine/config/workspace.yaml
docker compose pull
docker compose up -d
```

The services bind to localhost by default. The dataset engine UI and API are
at `http://127.0.0.1:8000`; query-service exposes MCP over Streamable HTTP at
`http://127.0.0.1:8080/mcp`. Connect your MCP host to that endpoint. To use
the bundled vLLM embedding service instead of host Ollama, set
`SAB_EMBED_BASE_URL=http://vllm:8000/v1` in `.env` and start the GPU profile:

```sh
docker compose --profile gpu up -d
```

The model served for embeddings must match the model used at indexing time.
Changing it requires reindexing. See [dataset-engine](dataset-engine/README.md)
and [query-service](query-service/README.md) for configuration, AWS access,
embedding setup, and standalone Docker instructions.

During indexing and semantic search, text is sent to the embedding endpoint
you configure. Search results are returned to your MCP host, which may send
them to its chat model. Choose endpoints and MCP hosts according to your data
handling requirements.

## What it won’t do

Sab is an index and a set of data tools, not a chat application. It doesn’t
host an LLM, ingest arbitrary source types, answer from uncommitted indexing
work, or modify data through query-service. SQL can only read committed
chunks and returns at most 100 rows; it isn’t a general database proxy or an
arbitrary S3 query interface. Search quality depends on the source data and,
for semantic search, the embedding model and endpoint you configure.

## How it works

The dataset engine reads the configured PostgreSQL tables and S3 objects,
extracts text and structured rows, and writes chunks, identifiers, manifests,
and job receipts to the target S3 dataset. Query-service checks those commit
records and only searches data that belongs to the committed index. Its MCP
tools return bounded results with source references so the MCP host can show
where an answer came from.

The two containers are published as `garvw/sab-dataset-engine:0.1.0` and
`garvw/sab-query-service:0.1.0`. The root Compose stack uses these matching
images by default. See [CHANGELOG.md](CHANGELOG.md) for release notes and
[docs/releasing.md](docs/releasing.md) for maintainer release steps.
