"""Read-only SQL over the dataset's committed structured rows."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from .dataset import configured_connection
from ..dataset import open_dataset


MAX_RESULT_ROWS = 100


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def query_sql(sql: str, limit: int = 50) -> dict:
    """Run one read-only SELECT against committed structured rows in table `data`."""
    if not sql.strip():
        raise ValueError("sql must not be empty")
    if len(sql) > 16_384:
        raise ValueError("sql must be at most 16384 characters")
    if not 1 <= limit <= MAX_RESULT_ROWS:
        raise ValueError(f"limit must be between 1 and {MAX_RESULT_ROWS}")

    try:
        import duckdb
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("SQL querying requires DuckDB; install query-service requirements") from exc

    dataset = open_dataset(configured_connection())
    indexed_rows = dataset.structured_rows()
    if not indexed_rows:
        raise ValueError("The dataset has no committed structured rows to query")

    arrow_rows = []
    for entry in indexed_rows:
        row = dict(entry["row"])
        row.update({
            "_sab_doc_id": entry["doc_id"],
            "_sab_source_id": entry["source_id"],
            "_sab_source": entry.get("source_label"),
            "_sab_sheet": entry.get("sheet"),
            "_sab_row_number": entry.get("row_number"),
        })
        arrow_rows.append(row)
    table = pa.Table.from_pylist(arrow_rows)

    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("SET enable_external_access = false")
        connection.register("data", table)
        statements = duckdb.extract_statements(sql)
        if len(statements) != 1 or statements[0].type.name != "SELECT":
            raise ValueError("Only a single SELECT query is allowed")
        normalized_sql = sql.strip().removesuffix(";").rstrip()
        result = connection.execute(
            f"SELECT * FROM ({normalized_sql}) AS _sab_result LIMIT {limit + 1}"
        )
        columns = [item[0] for item in result.description]
        fetched = result.fetchmany(limit + 1)
        truncated = len(fetched) > limit
        rows = [
            {column: _json_value(value) for column, value in zip(columns, values)}
            for values in fetched[:limit]
        ]
        return {
            "table": "data",
            "available_rows": len(arrow_rows),
            "returned_rows": len(rows),
            "truncated": truncated,
            "columns": columns,
            "rows": rows,
        }
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"SQL query failed: {exc}") from exc
    finally:
        connection.close()
