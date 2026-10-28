# Sab

Sab indexes data sources into S3 datasets (dataset-engine) and queries the
committed, verified index read-only (query-service).

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
