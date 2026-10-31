# Sab v0.1.0

Sab v0.1.0 is the first versioned release of the dataset engine and query
service as Docker images.

## What is included

- `garvw/sab-dataset-engine:0.1.0` indexes PostgreSQL tables and supported
  objects from S3 into committed S3 datasets, with a workspace UI and HTTP API
  for indexing jobs and status.
- `garvw/sab-query-service:0.1.0` exposes read-only MCP tools for committed
  dataset inspection, integrity checks, keyword and semantic search, exact row
  lookup, numeric aggregation, and bounded SQL.
- The root Compose stack runs both services from one read-only mounted
  `workspace.yaml`; the optional `gpu` profile starts vLLM for embeddings.

Supported S3 file types are CSV, XLSX/XLSM, PDF, DOCX, Markdown, and plain
text. PostgreSQL password and Aurora PostgreSQL IAM authentication are
supported.

## Boundaries

Query-service does not index or modify datasets, repair integrity problems,
host a chat model, or act as a general database proxy. SQL is read-only over
committed chunks and returns at most 100 rows. Semantic search needs an
OpenAI-compatible embeddings endpoint with the same model used for indexing;
changing that model requires reindexing. See the service READMEs for setup,
security, and operational details.
