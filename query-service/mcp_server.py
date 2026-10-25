"""MCP entrypoint for Sab's read-only indexed-data operations."""

from mcp.server.fastmcp import FastMCP

from main.tools import (
    analyze_rows,
    audit_integrity,
    find_rows,
    inspect_dataset,
    keyword_search,
    query_sql,
    semantic_search,
)

mcp = FastMCP(
    "sab-data",
    instructions=(
        "Use inspect_dataset and audit_integrity to understand index state. "
        "Use keyword_search for literal terms without embeddings, semantic_search "
        "for meaning-based retrieval, find_rows for exact "
        "row lookups, analyze_rows for simple exact aggregates, and query_sql "
        "for flexible read-only SQL against committed structured rows in table data. "
        "Choose semantic_search top_k based on how much evidence the question needs "
        "(1-50); increase it when a broader review is useful. "
        "Prefer aggregate tools when they answer the question, since they return "
        "compact results without exposing every row. Cite source references from results."
    ),
)

mcp.tool()(inspect_dataset)
mcp.tool()(audit_integrity)
mcp.tool()(keyword_search)
mcp.tool()(semantic_search)
mcp.tool()(find_rows)
mcp.tool()(analyze_rows)
mcp.tool()(query_sql)


if __name__ == "__main__":
    mcp.run(transport="stdio")
