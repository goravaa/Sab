"""Deterministic serialization shared by document builders.

Documents use canonical JSON for identities, metadata, and signatures so the
same logical content produces the same hashes regardless of mapping order.
"""

from __future__ import annotations

import hashlib
import json
import math

from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID


def canonicalize(value: Any) -> Any:
    """Convert common values into deterministic JSON-compatible data."""

    if value is None:
        return None

    if isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if not math.isfinite(value):
            return {"$float": str(value)}
        return value

    if isinstance(value, Decimal):
        return {"$decimal": str(value)}

    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}

    if isinstance(value, date):
        return {"$date": value.isoformat()}

    if isinstance(value, time):
        return {"$time": value.isoformat()}

    if isinstance(value, UUID):
        return {"$uuid": str(value)}

    if isinstance(value, bytes):
        return {"$bytes": value.hex()}

    if isinstance(value, Mapping):
        return {
            str(key): canonicalize(item)
            for key, item in sorted(
                value.items(),
                key=lambda pair: str(pair[0]),
            )
        }

    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]

    # Handle scalar missing-value types such as pandas.NA without
    # making pandas a required dependency.
    if type(value).__name__ in {"NAType", "NaTType"}:
        return None

    # Fail rather than silently hash an unknown object using an
    # unstable or lossy string representation.
    raise TypeError(
        f"Unsupported value type: {type(value).__qualname__}"
    )


def canonical_json(value: Any) -> str:
    """Serialize a value deterministically."""

    return json.dumps(
        canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    """Return a SHA-256 hex digest of a canonical JSON representation."""

    return hashlib.sha256(
        canonical_json(value).encode("utf-8")
    ).hexdigest()
