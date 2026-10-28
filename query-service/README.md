# Sab data MCP

Sab exposes an indexed dataset to an MCP host such as OpenCode. The host model
chooses a read-only tool; Sab validates the committed index and returns the
smallest useful result. Sab does not host or call a chat model.

## Tools

- `inspect_dataset` — dataset identity, source counts, committed documents,
  chunks, and embedding model.
- `audit_integrity` — compare committed identifiers with visible Lance chunks;
  report missing documents/chunks, duplicates, and ignored stale/orphan chunks.
- `list_tables` — list the committed logical structured tables identified by
  Lance `table_name` and `table_kind` columns, with namespace, row count, and
  field names.
- `describe_table` — inspect one table's columns and observed value types.
- `keyword_search` — literal, case-insensitive keyword search over committed
  chunk text and tabular values. It scans the index without computing query
  embeddings and returns a bounded set of source-cited matches.
- `semantic_search` — nearest-neighbor search whose `top_k` is chosen by the
  host model on each call (1–50, default 10). The result reports the requested
  depth and available committed chunks. The result includes source references;
  tabular results return row values instead of duplicating the same row as text.
- `find_rows` — exact structured row lookup with a bounded result size.
- `analyze_rows` — exact `count`, `sum`, `avg`, `min`, or `max`, optionally
  grouped by a field and filtered by one condition. Aggregation runs in Sab and
  returns compact numbers instead of sending all matching rows to the model.
- `query_sql` — run one read-only SQL `SELECT` over all committed chunks
  exposed as `data`. Filter by `table_name` and `table_kind` to select one
  logical table, or use `table_kind = 'document'` plus `text` and
  `chunk_index` to retrieve document content. Structured row fields are
  exposed as typed columns; `doc_id`, `source_id`, `source`, `sheet`, and
  `row_number` provide source references. These provenance fields and
  `metadata` (large JSON) should be selected only when needed; prefer explicit
  columns over `SELECT *` to keep results token-efficient. Lance yields
  committed chunks to DuckDB in batches. The commit manifest is copied to temporary disk and
  indexed in SQLite rather than retained as a Python dictionary. DuckDB uses a
  512 MB memory limit by default and spills larger intermediate results to its
  temporary directory. Set `SAB_SQL_MEMORY_LIMIT` and `SAB_SQL_THREADS` to tune
  those defaults. Results are capped at 100 rows.

Use keyword search when a specific name, ID, phrase, or value is known; use
semantic search when the wording may differ from the indexed text. For numeric
questions, use `analyze_rows` so totals and counts are calculated over the data.

All search and row operations use only chunks whose `source_id`, `job_id`,
`doc_id`, and `config_hash` match committed `identifiers.parquet` state.
Structured operations read `metadata.row` from committed tabular chunks; they
do not infer numeric values from natural-language text. SQL exposes all
committed chunks as one `data` table; filter with `table_name` to isolate a
logical table. Structured row fields with the same name across tables share one
typed column, with compatible types merged. Lance input is read in bounded batches; SQL operators such as
large sorts and high-cardinality grouping can require substantial temporary
disk space and take longer. The manifest and DuckDB spill files use the system
temporary directory, so SQL queries need available local disk space. SQL runs
in an isolated DuckDB connection with external file access disabled;
it cannot query arbitrary S3 paths or alter the dataset.

## Configure and run

Install dependencies in Python 3.10+. Configuration is central: the service
reads the same `workspace.yaml` dataset-engine uses, resolvable through
`SAB_WORKSPACE` or checked-out relative locations (`config/workspace.yaml`,
next to it in `../dataset-engine`). From it, query-service derives the
dataset name, bucket, prefix, region, and the optional `embeddings:` /
`query:` sections. No dataset-related environment variables are required.

```sh
python -m pip install -r requirements.txt
python mcp_server.py   # resolves workspace.yaml; set SAB_WORKSPACE otherwise
```

Connection precedence: workspace.yaml first; bucket discovery from
`SAB_DATASET_BUCKET` as a fallback for deployments without a workspace file
(a unique committed dataset is selected automatically); the legacy
`sab-data/sab-datasets/ui-smoke-173645` sample variables still apply last.

Embedding endpoint precedence: `SAB_EMBED_BASE_URL`/`SAB_EMBED_API_KEY`
environment overrides (deployment glue, e.g. pointing both services at vLLM
under `--profile gpu`), then the `embeddings:` section of workspace.yaml,
then `http://localhost:11434/v1` with key `ollama`. The model name requested
still comes from the dataset's committed `config.json` unless workspace.yaml
`embeddings.model` overrides it (e.g. serving the model under an alias).

The dataset-engine Compose stack starts vLLM with `BAAI/bge-m3` and maps it to
`127.0.0.1:8001` on the host. For a query service running directly on that
host, set `SAB_EMBED_BASE_URL=http://127.0.0.1:8001/v1` and use the same
`VLLM_API_KEY` value as `SAB_EMBED_API_KEY`. A query service in the same Compose
network can use `http://vllm:8000/v1` instead.

The default transport is MCP stdio, where the host starts `mcp_server.py` as a
subprocess. Containerized deployments set `SAB_MCP_TRANSPORT=streamable-http`
(and optionally `SAB_MCP_HOST`/`SAB_MCP_PORT`) to serve MCP over HTTP.

## Docker

```sh
docker build -t garvw/sab-query-service:<tag> .
docker run --rm -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / instance role \
  -e SAB_WORKSPACE=/app/config/workspace.yaml \
  -v /path/to/workspace.yaml:/app/config/workspace.yaml:ro \
  -e SAB_EMBED_BASE_URL=http://host.docker.internal:8001/v1 \
  -e SAB_MCP_TRANSPORT=streamable-http \
  garvw/sab-query-service:<tag>
```

Without `SAB_WORKSPACE`, a bare container uses `SAB_DATASET_BUCKET`
(discovery) as fallback.


## OpenCode

Add Sab as a local MCP server to OpenCode's config on the machine where
OpenCode runs. The command and working directory must point to this checkout.
On Chotu, prefer its instance role for AWS access and set an embedding URL
reachable from the Chotu instance:

```jsonc
{
  "mcp": {
    "sab-data": {
      "type": "local",
      "command": ["/path/to/query-service/env/bin/python", "/path/to/query-service/mcp_server.py"],
      "cwd": "/path/to/query-service",
      "environment": {
        "SAB_WORKSPACE": "/path/to/dataset-engine/config/workspace.yaml",
        "SAB_EMBED_BASE_URL": "http://127.0.0.1:11434/v1",
        "SAB_EMBED_API_KEY": "ollama"
      },
      "timeout": 120000
    }
  }
}
```

`SAB_WORKSPACE` is optional when the two checkouts sit side by side (the
loader falls back to `../dataset-engine/config/workspace.yaml`); the embedding
endpoint is the only environment value normally worth pinning for a host-run
server.

The Ollama endpoint above is an example for a host running Ollama locally;
use an endpoint serving the model recorded in the dataset's committed
`config.json` (`ollama cp bge-m3 BAAI/bge-m3` keeps the served name matching).
Chotu needs a matching embedding endpoint; its Qwen chat endpoint does not
replace the embedding model. OpenCode starts local MCP servers over stdio and
accepts a per-server environment and timeout in its [MCP
configuration](https://opencode.ai/docs/en/mcp-servers/). Every `/v1`
environment variable, including `SAB_EMBED_BASE_URL`, must be reachable from
where the server runs. For a local AWS profile, add `AWS_PROFILE` to the
server's `environment`; some MCP hosts do not pass the parent shell
environment through automatically.
