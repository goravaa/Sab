from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from main.document.canonical import canonical_json, canonicalize, sha256_json


def test_canonicalize_handles_common_scalar_and_nested_types():
    sample_uuid = UUID("12345678-1234-5678-1234-567812345678")
    result = canonicalize({
        "number": Decimal("12.30"),
        "date": date(2026, 10, 22),
        "timestamp": datetime(2026, 10, 22, 10, 30, tzinfo=timezone.utc),
        "time": time(10, 30),
        "uuid": sample_uuid,
        "bytes": b"\x00\xff",
        "items": (1, float("nan"), float("inf")),
    })

    assert result == {
        "bytes": {"$bytes": "00ff"},
        "date": {"$date": "2026-10-22"},
        "items": [1, None, {"$float": "inf"}],
        "number": {"$decimal": "12.30"},
        "time": {"$time": "10:30:00"},
        "timestamp": {"$datetime": "2026-10-22T10:30:00+00:00"},
        "uuid": {"$uuid": str(sample_uuid)},
    }


def test_canonical_json_and_hash_ignore_mapping_insertion_order():
    left = {"z": 1, "nested": {"b": 2, "a": 3}}
    right = {"nested": {"a": 3, "b": 2}, "z": 1}

    assert canonical_json(left) == canonical_json(right)
    assert sha256_json(left) == sha256_json(right)
    assert canonical_json(left) == '{"nested":{"a":3,"b":2},"z":1}'


def test_canonicalize_fails_for_unknown_objects_instead_of_stringifying_them():
    with pytest.raises(TypeError, match="Unsupported value type"):
        canonicalize(object())
