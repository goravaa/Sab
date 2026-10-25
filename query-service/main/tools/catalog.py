"""Catalog discovery for logical tables represented inside a Lance dataset."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any

from ..dataset import OpenDataset
from .dataset import configured_connection
from ..dataset import open_dataset


def _logical_type(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, (dict, list, tuple)):
        if isinstance(value, dict) and set(value) == {"$decimal"}:
            return "DECIMAL"
        return "JSON"
    if isinstance(value, (float, Decimal)):
        return "DECIMAL"
    return "VARCHAR"


def _merge_types(types: set[str], *, precision: int = 1, scale: int = 0) -> str:
    if not types:
        return "UNKNOWN"
    if types <= {"BIGINT", "DECIMAL"} and "DECIMAL" in types:
        integral_digits = max(precision - scale, 1)
        decimal_precision = integral_digits + scale
        if decimal_precision <= 38:
            return f"DECIMAL({decimal_precision},{scale})"
        return "DOUBLE"
    if types <= {"BIGINT", "DOUBLE"}:
        return "DOUBLE" if "DOUBLE" in types else "BIGINT"
    if len(types) == 1:
        return next(iter(types))
    return "VARCHAR"


def describe_dataset_table(
    dataset: OpenDataset,
    table_name: str,
    table_kind: str | None = None,
    namespace: str | None = None,
) -> dict:
    matches: set[tuple[str, str, str]] = set()
    columns: dict[str, set[str]] = defaultdict(set)
    decimal_shapes: dict[str, tuple[int, int]] = {}
    row_count = 0
    for entry in dataset.iter_structured_rows(
        table_name=table_name, table_kind=table_kind, namespace=namespace,
    ):
        matches.add((entry["table_name"], entry["table_kind"], entry["namespace"]))
        row_count += 1
        for name, value in entry["row"].items():
            name = str(name)
            kind = _logical_type(value)
            if kind:
                columns[name].add(kind)
                if kind == "BIGINT":
                    digits = len(str(abs(value)))
                    previous = decimal_shapes.get(name, (1, 0))
                    decimal_shapes[name] = (max(previous[0], digits), previous[1])
                decimal_value = None
                if kind == "DECIMAL":
                    raw = value.get("$decimal") if isinstance(value, dict) else value
                    try:
                        decimal_value = Decimal(str(raw))
                    except Exception:
                        pass
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
    if not matches:
        raise ValueError(f"No committed structured table named {table_name!r} matches those selectors")
    if len(matches) > 1:
        raise ValueError(f"Table name is ambiguous; call list_tables and pass table_kind and namespace: {sorted(matches)}")
    identity = next(iter(matches))
    return {
        "table_name": identity[0],
        "table_kind": identity[1],
        "namespace": identity[2],
        "row_count": row_count,
        "columns": [
            {
                "name": name,
                "type": _merge_types(
                    types,
                    precision=decimal_shapes.get(name, (1, 0))[0],
                    scale=decimal_shapes.get(name, (1, 0))[1],
                ),
                "nullable": True,
            }
            for name, types in sorted(columns.items())
        ],
    }


def list_tables() -> dict:
    """List structured logical tables and their field names in the committed Lance dataset."""
    with open_dataset(configured_connection(), streaming_identifiers=True) as dataset:
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for entry in dataset.iter_structured_rows():
            identity = (entry["table_name"], entry["table_kind"], entry["namespace"])
            table = grouped.setdefault(identity, {"rows": 0, "columns": set()})
            table["rows"] += 1
            table["columns"].update(entry["row"].keys())
        tables = [
            {
                "table_name": name,
                "table_kind": kind,
                "namespace": namespace,
                "row_count": value["rows"],
                "columns": sorted(value["columns"]),
            }
            for (name, kind, namespace), value in sorted(grouped.items())
        ]
        return {"table_count": len(tables), "tables": tables}


def describe_table(
    table_name: str,
    table_kind: str | None = None,
    namespace: str | None = None,
) -> dict:
    """Describe the columns and observed value types of one committed logical table."""
    with open_dataset(configured_connection(), streaming_identifiers=True) as dataset:
        return describe_dataset_table(dataset, table_name, table_kind, namespace)
