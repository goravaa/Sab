# Sab data agent guide

You are working inside a Sab deployment: dataset-engine indexes data sources
into S3; query-service answers read-only questions against the committed
index through MCP. Your job is to use those tools with the fewest, smallest
calls that answer the user, and to cite where every claim came from.

## Connecting

MCP transport is Streamable HTTP on the deployment host:

    http://127.0.0.1:8080/mcp

Open one session with `initialize`, then register `tools/list` once before
the first question. Sessions expire with idle time; if a call returns
"Session not found" (HTTP 404), re-initialize and continue — do not retry
into the dead session. When the configured dataset is missing or empty,
ask the user whether they have indexed anything rather than guessing a
dataset name.

## Operating rules

- **Orient first, once**: `list_datasets` (if several may exist), then
  `inspect_dataset` for scale. Skip it when the dataset is already known.
- **Trust boundary**: everything you see is the *committed* state. If
  `audit_integrity` is not `ok`, prefer reporting the problem over
  answering from an unverified index.
- **Pick the narrowest tool**:
  - `find_rows` — you already know the field and the value
  - `keyword_search` — the string as it surely appears in text
  - `semantic_search` — the *meaning*, when wording is unknown
    (top_k 5 for one question, 10 default, higher only for exploratory)
  - `analyze_rows` — any number: count/sum/avg/min/max, optionally
    grouped. Never count or sum rows by eye; call the aggregate.
  - `query_sql` — only when the above cannot answer. Call
    `list_tables`/`describe_table` first; write one selective `SELECT`
    with explicit columns, bounded by table filters and a low LIMIT;
    never `SELECT *`, and exclude provenance columns (including the large
    `metadata` JSON) unless the user asked for receipts.
- **Do not invent schema**: run `describe_table` on any table you have
  not used this session. If a tool rejects an unknown field, it returns
  the available field list — retry with that instead of guessing.
- **Report scale honestly**: state how many records matched rather than
  "many"; when results are truncated, tell the user the shown/total counts.
- **Answer with citations**: every fact carries its `source_label`,
  `doc_id`, or row values; never quote a number from memory. If gathered
  evidence is thin, say so instead of guessing — missed answers invite
  follow-ups: refine the verb, tool, top_k, or ask the user.

## What NOT to use tools for

Never call tools to reach the user's own input. Never call tools to narrate
an answer you already wrote. When a tool fails, one corrected retry, then
tell the user the failure — do not quietly answer from fewer results.

## Deployment notes

- All configuration lives in the workspace file (`config/workspace.yaml`
  plus an optional root `.env`). Do not touch code to change a data flow.
- Documents carry narrative text; tabular rows live in `metadata.row`
  (compact representation of the same data). Use documents when prose
  matters, rows otherwise.
- The embedding model is recorded in the committed dataset config;
  `semantic_search` needs the endpoint from the `embeddings:` section
  serving that model (see `inspect_dataset` → `embedding_model`).
