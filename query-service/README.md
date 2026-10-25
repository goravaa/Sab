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

Install dependencies in Python 3.10+ and set the dataset and embedding
endpoint. AWS access uses the AWS SDK credential chain unless explicit S3
credentials are supplied through environment variables.

```sh
python -m pip install -r requirements.txt
cp .env.example .env
set -a && source .env && set +a
python mcp_server.py
```

The default sample target is `sab-data/sab-datasets/ui-smoke-173645` in
`us-west-2`. Change `SAB_DATASET_BUCKET`, `SAB_DATASET_NAME`,
`SAB_DATASET_PREFIX`, or `SAB_DATASET_REGION` for another dataset.

`SAB_EMBED_BASE_URL` must serve the same embedding model recorded in the
dataset config. The example uses local Ollama `all-minilm`; when the MCP server
runs on Chotu, point it at an embedding endpoint reachable from Chotu that
serves the matching model. The vLLM chat model does not need to be called by
Sab; the MCP host model receives tool results and writes the answer.

The default transport is MCP stdio, where the host starts `mcp_server.py` as a
subprocess. Streamable HTTP can be added for remote clients when deployment and
authentication are ready.

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
        "SAB_DATASET_BUCKET": "sab-data",
        "SAB_DATASET_NAME": "ui-smoke-173645",
        "SAB_DATASET_PREFIX": "sab-datasets",
        "SAB_DATASET_REGION": "us-west-2",
        "SAB_EMBED_BASE_URL": "http://127.0.0.1:11434/v1",
        "SAB_EMBED_API_KEY": "ollama"
      },
      "timeout": 120000
    }
  }
}
```

The Ollama endpoint above is an example for a host running Ollama locally.
Chotu needs a matching `all-minilm` embedding endpoint; its Qwen chat endpoint
does not replace the embedding model. OpenCode starts local MCP servers over
stdio and accepts a per-server environment and timeout in its [MCP
configuration](https://opencode.ai/docs/en/mcp-servers/).
For a local AWS profile, add `AWS_PROFILE` to the server's `environment`; some
MCP hosts do not pass the parent shell environment through automatically.
