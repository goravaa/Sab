"""Read-only SQL over one logical table, streamed from committed Lance rows."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..dataset import open_dataset
from .catalog import describe_dataset_table
from .dataset import configured_connection


MAX_RESULT_ROWS = 100
_PROVENANCE_TYPES = {
    "_sab_doc_id": "VARCHAR",
    "_sab_source_id": "VARCHAR",
    "_sab_source": "VARCHAR",
    "_sab_sheet": "VARCHAR",
    "_sab_row_number": "BIGINT",
    "_sab_table_name": "VARCHAR",
    "_sab_table_kind": "VARCHAR",
    "_sab_namespace": "VARCHAR",
}


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


def _arrow_type(pa: Any, logical_type: str) -> Any:
    decimal_match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", logical_type)
    if decimal_match:
        return pa.decimal128(int(decimal_match.group(1)), int(decimal_match.group(2)))
    return {
        "BOOLEAN": pa.bool_(),
        "BIGINT": pa.int64(),
        "DOUBLE": pa.float64(),
        "JSON": pa.string(),
        "VARCHAR": pa.string(),
        "UNKNOWN": pa.string(),
    }.get(logical_type, pa.string())


def _arrow_value(value: Any, logical_type: str) -> Any:
    if value is None:
        return None
    if logical_type == "JSON":
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    decimal_match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", logical_type)
    if decimal_match:
        raw = value.get("$decimal") if isinstance(value, dict) and set(value) == {"$decimal"} else value
        return Decimal(str(raw))
    if logical_type == "VARCHAR":
        return value if isinstance(value, str) else str(value)
    if logical_type == "DOUBLE":
        return float(value)
    if logical_type == "BIGINT":
        return int(value)
    if logical_type == "BOOLEAN":
        return bool(value)
    return str(value)


def query_sql(
    table_name: str,
    sql: str,
    table_kind: str | None = None,
    namespace: str | None = None,
    limit: int = 50,
) -> dict:
    """Run one SELECT against a single catalog table, exposed to SQL as `data`.

    Lance reads the selected table in bounded batches. The SQL statement must
    use `FROM data`; the table selector prevents unrelated source schemas from
    being mixed into the Arrow stream.
    """
    if not table_name.strip():
        raise ValueError("table_name must not be empty")
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

    with open_dataset(configured_connection(), streaming_identifiers=True) as dataset:
        return _query_open_dataset(
            dataset,
            table_name,
            sql,
            table_kind,
            namespace,
            limit,
            duckdb,
            pa,
        )


def _query_open_dataset(
    dataset: Any,
    table_name: str,
    sql: str,
    table_kind: str | None,
    namespace: str | None,
    limit: int,
    duckdb: Any,
    pa: Any,
) -> dict:
    description = describe_dataset_table(dataset, table_name, table_kind, namespace)
    identity = (description["table_name"], description["table_kind"], description["namespace"])
    logical_types = {column["name"]: column["type"] for column in description["columns"]}
    for name in _PROVENANCE_TYPES:
        if name in logical_types:
            raise ValueError(f"Source column {name!r} conflicts with a reserved provenance column")
    schema = pa.schema(
        [pa.field(name, _arrow_type(pa, kind)) for name, kind in logical_types.items()]
        + [pa.field(name, _arrow_type(pa, kind)) for name, kind in _PROVENANCE_TYPES.items()]
    )

    def batches():
        pending: list[dict[str, Any]] = []
        for entry in dataset.iter_structured_rows(
            table_name=identity[0], table_kind=identity[1], namespace=identity[2],
        ):
            row = {
                name: _arrow_value(entry["row"].get(name), kind)
                for name, kind in logical_types.items()
            }
            row.update({
                "_sab_doc_id": entry["doc_id"],
                "_sab_source_id": entry["source_id"],
                "_sab_source": entry.get("source_label"),
                "_sab_sheet": entry.get("sheet"),
                "_sab_row_number": entry.get("row_number"),
                "_sab_table_name": entry["table_name"],
                "_sab_table_kind": entry["table_kind"],
                "_sab_namespace": entry["namespace"],
            })
            pending.append(row)
            if len(pending) >= 512:
                yield pa.RecordBatch.from_pylist(pending, schema=schema)
                pending.clear()
        if pending:
            yield pa.RecordBatch.from_pylist(pending, schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    memory_limit = os.getenv("SAB_SQL_MEMORY_LIMIT", "512MB").upper()
    if not re.fullmatch(r"\d+(KB|MB|GB|TB)", memory_limit):
        raise ValueError("SAB_SQL_MEMORY_LIMIT must be a size such as 512MB or 2GB")
    try:
        threads = int(os.getenv("SAB_SQL_THREADS", "2"))
    except ValueError as exc:
        raise ValueError("SAB_SQL_THREADS must be an integer from 1 to 16") from exc
    if not 1 <= threads <= 16:
        raise ValueError("SAB_SQL_THREADS must be an integer from 1 to 16")

    connection = duckdb.connect(database=":memory:")
    try:
        spill_directory = Path(dataset.temp_directory.name) / "duckdb-spill"
        spill_directory.mkdir()
        escaped_spill_directory = str(spill_directory).replace("'", "''")
        connection.execute(f"SET memory_limit = '{memory_limit}'")
        connection.execute(f"SET threads = {threads}")
        connection.execute("SET preserve_insertion_order = false")
        connection.execute(f"SET temp_directory = '{escaped_spill_directory}'")
        connection.execute("SET enable_external_access = false")
        connection.register("data", reader)
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
            "table_name": identity[0],
            "table_kind": identity[1],
            "namespace": identity[2],
            "available_rows": description["row_count"],
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
