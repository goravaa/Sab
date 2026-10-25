"""Exact, dataset-wide operations over committed tabular rows."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from ..dataset import open_dataset
from ..schema import AggregateResult, RowSearchResult
from .dataset import configured_connection


FilterOperator = Literal["eq", "ne", "gt", "gte", "lt", "lte", "contains"]
AggregateOperation = Literal["count", "sum", "avg", "min", "max"]
FilterValue = str | int | float | bool | None


def _coerce_like(value: Any, sample: Any) -> Any:
    if value is None or sample is None:
        return value
    if isinstance(sample, bool):
        if isinstance(value, bool):
            return value
        lowered = str(value).strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        return value
    if isinstance(sample, int) and not isinstance(sample, bool):
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if isinstance(sample, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if isinstance(sample, str):
        return str(value)
    return value


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _number(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _matches(
    row: dict[str, Any],
    *,
    field: str | None,
    operator: FilterOperator,
    value: Any,
) -> bool:
    if not field:
        return True
    cell = row.get(field)
    target = _coerce_like(value, cell)
    if isinstance(cell, str) and isinstance(value, (int, float)):
        cell_decimal = _decimal(cell)
        value_decimal = _decimal(value)
        if cell_decimal is not None and value_decimal is not None:
            cell, target = cell_decimal, value_decimal
    elif operator in {"gt", "gte", "lt", "lte"} and isinstance(cell, str) and isinstance(value, str):
        cell_decimal = _decimal(cell)
        value_decimal = _decimal(value)
        if cell_decimal is not None and value_decimal is not None:
            cell, target = cell_decimal, value_decimal
    if operator == "eq":
        return cell == target
    if operator == "ne":
        return cell != target
    if operator == "contains":
        if isinstance(cell, (list, tuple, set)):
            return target in cell
        return str(target).casefold() in str(cell or "").casefold()
    if cell is None or target is None:
        return False
    try:
        return {
            "gt": lambda: cell > target,
            "gte": lambda: cell >= target,
            "lt": lambda: cell < target,
            "lte": lambda: cell <= target,
        }[operator]()
    except (TypeError, KeyError):
        return False


def _operation(values: list[Any], operation: AggregateOperation) -> Any:
    if operation == "count":
        return len(values)
    present = [value for value in values if value is not None]
    if not present:
        return None
    if operation in {"sum", "avg"}:
        numbers = [_decimal(value) for value in present]
        if any(value is None for value in numbers):
            raise ValueError(f"{operation} requires a numeric field; nonnumeric values were found")
        total = sum((value for value in numbers if value is not None), Decimal(0))
        result = total if operation == "sum" else total / Decimal(len(present))
        return _number(result)
    numeric = [_decimal(value) for value in present]
    if numeric and all(value is not None for value in numeric):
        extreme = min(value for value in numeric if value is not None) if operation == "min" else max(value for value in numeric if value is not None)
        return _number(extreme)
    return min(present) if operation == "min" else max(present)


def analyze_rows(
    operation: AggregateOperation,
    field: str | None = None,
    group_by: str | None = None,
    filter_field: str | None = None,
    filter_operator: FilterOperator = "eq",
    filter_value: FilterValue = None,
) -> dict:
    """Calculate an exact aggregate across all committed tabular rows.

    Only the aggregate is returned; source rows stay inside the MCP server.
    One optional filter and one optional grouping key keep calls small and safe.
    """
    dataset = open_dataset(configured_connection())
    indexed_rows = dataset.structured_rows()
    if not indexed_rows:
        return AggregateResult(
            operation=operation,
            field=field,
            group_by=group_by,
            matched_rows=0,
            result=None,
        ).model_dump(mode="json")

    available_fields = set().union(*(entry["row"].keys() for entry in indexed_rows))
    for name in (field, group_by, filter_field):
        if name and name not in available_fields:
            raise ValueError(f"Unknown field {name!r}; available fields: {sorted(available_fields)}")
    if operation != "count" and not field:
        raise ValueError(f"field is required for {operation}")

    matched = [
        entry
        for entry in indexed_rows
        if _matches(
            entry["row"],
            field=filter_field,
            operator=filter_operator,
            value=filter_value,
        )
    ]

    if group_by:
        groups: dict[str, list[dict[str, Any]]] = {}
        for entry in matched:
            key = str(entry["row"].get(group_by))
            groups.setdefault(key, []).append(entry)
        results = [
            {
                "group": key,
                "value": _operation(
                    [entry["row"].get(field) for entry in group] if field else [None] * len(group),
                    operation,
                ),
                "rows": len(group),
            }
            for key, group in groups.items()
        ]
        results.sort(
            key=lambda item: (
                item["value"] is not None,
                item["value"] if isinstance(item["value"], (int, float)) else str(item["value"]),
            ),
            reverse=True,
        )
        return AggregateResult(
            operation=operation,
            field=field,
            group_by=group_by,
            matched_rows=len(matched),
            result=results[:20],
            truncated_groups=len(results) > 20,
        ).model_dump(mode="json")

    result = _operation(
        [entry["row"].get(field) for entry in matched] if field else [None] * len(matched),
        operation,
    )
    return AggregateResult(
        operation=operation,
        field=field,
        group_by=None,
        matched_rows=len(matched),
        result=result,
    ).model_dump(mode="json")


def find_rows(
    field: str,
    value: FilterValue,
    operator: FilterOperator = "eq",
    columns: list[str] | None = None,
    limit: int = 10,
) -> dict:
    """Find exact matching rows; return at most 20 compact row records."""
    if not 1 <= limit <= 20:
        raise ValueError("limit must be between 1 and 20")
    dataset = open_dataset(configured_connection())
    indexed_rows = dataset.structured_rows()
    if not indexed_rows:
        return RowSearchResult(matched_rows=0, returned_rows=0, rows=[], truncated=False).model_dump(mode="json")

    available_fields = set().union(*(entry["row"].keys() for entry in indexed_rows))
    if field not in available_fields:
        raise ValueError(f"Unknown field {field!r}; available fields: {sorted(available_fields)}")
    if columns:
        unknown = sorted(set(columns) - available_fields)
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")

    matched = [
        entry
        for entry in indexed_rows
        if _matches(entry["row"], field=field, operator=operator, value=value)
    ]
    output = []
    for entry in matched[:limit]:
        values = entry["row"] if columns is None else {
            key: entry["row"].get(key) for key in columns
        }
        output.append({
            "doc_id": entry["doc_id"],
            "source_id": entry["source_id"],
            "source": entry.get("source_label"),
            "sheet": entry.get("sheet"),
            "row_number": entry.get("row_number"),
            "values": values,
        })
    return RowSearchResult(
        matched_rows=len(matched),
        returned_rows=len(output),
        rows=output,
        truncated=len(matched) > len(output),
    ).model_dump(mode="json")
