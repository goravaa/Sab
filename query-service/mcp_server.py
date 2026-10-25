"""MCP entrypoint for Sab's read-only indexed-data operations."""

from mcp.server.fastmcp import FastMCP

from main.tools import (
    analyze_rows,
    audit_integrity,
    describe_table,
    find_rows,
    inspect_dataset,
    keyword_search,
    list_tables,
    query_sql,
    semantic_search,
)

mcp = FastMCP(
    "sab-data",
    instructions=(
        "Use inspect_dataset and audit_integrity to understand index state. "
        "Use keyword_search for literal terms without embeddings, semantic_search "
        "for meaning-based retrieval, find_rows for exact "
        "row lookups, and analyze_rows for simple exact aggregates. For SQL, call "
        "list_tables and describe_table to inspect structured tables, then query_sql "
        "for read-only SQL over all committed chunks exposed as `data`. Filter by "
        "`table_name` and `table_kind`; use `text` and `chunk_index` to retrieve "
        "document content. Write SELECTs with `FROM data`. The chunk/source columns "
        "are provenance metadata, and `metadata` is large JSON: select explicit "
        "columns and omit provenance unless needed; avoid `SELECT *` except when "
        "the user explicitly needs full rows. "
        "Choose semantic_search top_k based on how much evidence the question needs "
        "(1-50); increase it when a broader review is useful. "
        "Prefer aggregate tools when they answer the question, since they return "
        "compact results without exposing every row. Cite source references from results."
    ),
)

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
    mcp.run(transport="stdio")
