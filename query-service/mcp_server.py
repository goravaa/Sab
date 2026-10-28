"""MCP entrypoint for Sab's read-only indexed-data operations."""

import os

from mcp.server.fastmcp import FastMCP

from main.tools import (
    analyze_rows,
    audit_integrity,
    describe_table,
    find_rows,
    inspect_dataset,
    keyword_search,
    list_datasets,
    list_tables,
    query_sql,
    semantic_search,
)

mcp = FastMCP(
    "sab-data",
    instructions=(
        "Sab serves one committed, integrity-verified dataset stored in S3. "
        "Work in this order:\n"
        "1. Orient: run list_datasets when several datasets may exist, then "
        "inspect_dataset once. If audit_integrity is not 'ok', report the state "
        "instead of answering unsafely.\n"
        "2. Choose the narrowest tool, never a broader one: find_rows for exact "
        "values you can already name, keyword_search for names/IDs/phrases as "
        "they appear in text, semantic_search for meaning you cannot spell "
        "(top_k 1-50, raise it only for broad reviews of evidence), "
        "analyze_rows for any count/sum/avg/min/max — compute nothing yourself "
        "from returned rows, call the aggregate instead. query_sql is last "
        "resort: list_tables and describe_table first, then one SELECT, "
        "explicit columns only, never SELECT *, and use table_name/table_kind "
        "filters to isolate one logical table.\n"
        "3. Understand tables before inventing field names: describe_table "
        "before find_rows or analyze_rows on any field you have not seen; the "
        "error message lists available fields when you are wrong.\n"
        "4. Compose, do not dump: prefer one aggregate that answers the "
        "question over several row dumps. Keep provenance columns (doc_id, "
        "source_id, source, row_number, metadata) out of reads unless the user "
        "asks for receipts or the result is ambiguous.\n"
        "5. Cite: every claim in your final answer references the source_label, "
        "doc_id, or row values returned; if evidence is thin say so rather "
        "than soften. Numbers quoted must come from tool results verbatim.\n"
        "Evidence is committed-only: stale or in-flight state is filtered by "
        "the server, you will never see it; keyword_search reports truncation — "
        "say the match count when the list was cut short."
    ),
)

mcp.tool()(list_datasets)
mcp.tool()(inspect_dataset)
mcp.tool()(audit_integrity)
mcp.tool()(list_tables)
mcp.tool()(describe_table)
mcp.tool()(keyword_search)
mcp.tool()(semantic_search)
mcp.tool()(find_rows)
mcp.tool()(analyze_rows)
mcp.tool()(query_sql)


if __name__ == "__main__":
    transport = os.getenv("SAB_MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.settings.host = os.getenv("SAB_MCP_HOST", mcp.settings.host)
        mcp.settings.port = int(os.getenv("SAB_MCP_PORT", mcp.settings.port))
        mcp.run(transport=transport)
