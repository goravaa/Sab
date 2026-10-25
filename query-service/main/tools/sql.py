"""Read-only SQL over all committed chunks, streamed from Lance rows."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..dataset import open_dataset
from .catalog import _logical_type, _merge_types
from .dataset import configured_connection


MAX_RESULT_ROWS = 100
_COMMON_TYPES = {
    "chunk_id": "VARCHAR",
    "doc_id": "VARCHAR",
    "source_id": "VARCHAR",
    "job_id": "VARCHAR",
    "source_type": "VARCHAR",
    "table_name": "VARCHAR",
    "table_kind": "VARCHAR",
    "namespace": "VARCHAR",
    "chunk_index": "BIGINT",
    "text": "VARCHAR",
    "header_path": "LIST",
    "token_count": "BIGINT",
    "source": "VARCHAR",
    "sheet": "VARCHAR",
    "row_number": "BIGINT",
    "metadata": "VARCHAR",
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


def _field_type(pa: Any, name: str, logical_type: str) -> Any:
    if name == "header_path":
        return pa.list_(pa.string())
    return _arrow_type(pa, logical_type)


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


def _row_types(dataset: Any) -> dict[str, str]:
    columns: dict[str, set[str]] = {}
    decimal_shapes: dict[str, tuple[int, int]] = {}
    for entry in dataset.iter_structured_rows():
        for name, value in entry["row"].items():
            name = str(name)
            if name in _COMMON_TYPES:
                raise ValueError(
                    f"Source column {name!r} conflicts with a reserved SQL column"
                )
            kind = _logical_type(value)
            if kind:
                columns.setdefault(name, set()).add(kind)
                if kind == "BIGINT":
                    digits = len(str(abs(value)))
                    previous = decimal_shapes.get(name, (1, 0))
                    decimal_shapes[name] = (max(previous[0], digits), previous[1])
                if kind == "DECIMAL":
                    raw = value.get("$decimal") if isinstance(value, dict) else value
                    try:
                        decimal_value = Decimal(str(raw))
                    except Exception:
                        decimal_value = None
                    if decimal_value is not None and decimal_value.is_finite():
                        observed_scale = max(-decimal_value.as_tuple().exponent, 0)
                        observed_precision = max(decimal_value.adjusted() + 1, 1) + observed_scale
                        previous = decimal_shapes.get(name, (1, 0))
                        decimal_shapes[name] = (
                            max(previous[0], observed_precision),
                            max(previous[1], observed_scale),
                        )
            else:
                columns.setdefault(name, set())
    return {
        name: _merge_types(
            types,
            precision=decimal_shapes.get(name, (1, 0))[0],
            scale=decimal_shapes.get(name, (1, 0))[1],
        )
        for name, types in sorted(columns.items())
    }


def _chunk_values(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "chunk_id": entry["chunk_id"],
        "doc_id": entry["doc_id"],
        "source_id": entry["source_id"],
        "job_id": entry["job_id"],
        "source_type": entry["source_type"],
        "table_name": entry["table_name"],
        "table_kind": entry["table_kind"],
        "namespace": entry["namespace"],
        "chunk_index": entry["chunk_index"],
        "text": entry["text"],
        "header_path": list(entry.get("header_path") or []),
        "token_count": entry["token_count"],
        "source": entry.get("source_label"),
        "sheet": entry.get("sheet"),
        "row_number": entry.get("row_number"),
        "metadata": json.dumps(
            entry.get("metadata") or {},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ),
    }


def query_sql(sql: str, limit: int = 50) -> dict:
    """Run one read-only SELECT against all committed chunks exposed as `data`.

    `data` contains `table_name`, `table_kind`, `chunk_index`, `text`, and
    source provenance for every committed chunk. Structured row fields are
    also exposed as typed columns. Filter by `table_name` to pull one logical
    table or all chunks of one document.

    System columns (`chunk_id`, `doc_id`, `source_id`, `job_id`, `source_type`,
    `namespace`, `source`, `sheet`, `row_number`, `metadata`) are provenance
    metadata. `metadata` is a large JSON string. Select explicit columns rather
    than `SELECT *`, and omit metadata columns unless they are needed.
    """
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
        return _query_open_dataset(dataset, sql, limit, duckdb, pa)


def _query_open_dataset(
    dataset: Any,
    sql: str,
    limit: int,
    duckdb: Any,
    pa: Any,
) -> dict:
    row_types = _row_types(dataset)
    schema = pa.schema(
        [pa.field(name, _field_type(pa, name, kind)) for name, kind in _COMMON_TYPES.items()]
        + [pa.field(name, _arrow_type(pa, kind)) for name, kind in row_types.items()]
    )

    def batches():
        pending: list[dict[str, Any]] = []
        for entry in dataset.iter_committed_chunks():
            values = _chunk_values(entry)
            row = entry.get("row") or {}
            values.update({
                name: _arrow_value(row.get(name), kind)
                for name, kind in row_types.items()
            })
            pending.append(values)
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
        spill_directory.mkdir(exist_ok=True)
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
            "available_chunks": dataset.chunk_count,
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
