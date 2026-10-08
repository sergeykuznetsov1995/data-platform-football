"""Pending-row identities, normalized to the persisted Trino column types."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


# These fields describe the observation/execution, not the parsed payload.
# Batch ID already binds content and parser. Snapshot dates derive from the
# observation time, so an unchanged payload can reuse an older physical batch.
OBSERVATION_COLUMNS = frozenset({
    "_target_batch_id", "_payload_sha256", "_parser_version", "_raw_uri",
    "_observed_at", "_ingested_at", "_source", "_entity_type",
    "observed_at", "discovery_run_id", "snapshot_date",
})


def identity_value(value: Any, column_type: str = "") -> Any:
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    kind = column_type.upper()
    if kind.startswith(("VARCHAR", "CHAR")):
        return str(value)
    if kind in {"BIGINT", "INTEGER", "SMALLINT", "TINYINT"}:
        try:
            if isinstance(value, bool):
                raise TypeError("boolean is not an integer source value")
            integer = int(value)
            if not isinstance(value, str) and value != integer:
                raise ValueError("non-integral numeric value")
            return integer
        except (ValueError, TypeError, OverflowError):
            raise ValueError(f"non-null value {value!r} is incompatible with {kind}") from None
    if kind in {"DOUBLE", "REAL"}:
        if isinstance(value, bool):
            raise ValueError(f"non-null value {value!r} is incompatible with {kind}")
        result = float(value) if kind == "DOUBLE" else float(np.float32(value))
        if not np.isfinite(result):
            raise ValueError(f"non-null value {value!r} is incompatible with {kind}")
        return 0.0 if result == 0 else result
    if kind.startswith("DECIMAL"):
        decimal = Decimal(str(value))
        shape = re.fullmatch(r"DECIMAL\(\s*\d+\s*,\s*(\d+)\s*\)", kind)
        if shape:
            decimal = decimal.quantize(Decimal(1).scaleb(-int(shape[1])), rounding=ROUND_HALF_UP)
        return "0" if decimal == 0 else format(decimal.normalize(), "f")
    if kind == "BOOLEAN" and isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1"}:
            return True
        if lowered in {"false", "f", "0", ""}:
            return False
        raise ValueError("invalid persisted BOOLEAN row identity")
    if kind == "DATE":
        return value.date().isoformat() if isinstance(value, datetime) else str(value)
    if "TIMESTAMP" in kind:
        value = pd.Timestamp(value).to_pydatetime(warn=False)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return "0" if value == 0 else format(value.normalize(), "f")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return value


def row_digest(
    row: Mapping[str, Any], columns: Sequence[str], types: Mapping[str, str]
) -> str:
    values = [identity_value(row.get(column), types.get(column, "")) for column in columns]
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def row_multiset(
    rows: Iterable[Mapping[str, Any]], columns: Sequence[str], types: Mapping[str, str]
) -> Counter[str]:
    """Row order is irrelevant; duplicate multiplicity is significant."""

    return Counter(row_digest(row, columns, types) for row in rows)
