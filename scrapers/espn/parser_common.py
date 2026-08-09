"""Strict, side-effect-free helpers for ESPN raw JSON parsing."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from typing import Any, Mapping, Sequence


class EspnParseError(ValueError):
    """A committed ESPN response cannot be interpreted without guessing."""


def decode_object(raw: bytes, field: str = "raw payload") -> Mapping[str, Any]:
    if not isinstance(raw, bytes):
        raise TypeError("raw payload must be bytes")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EspnParseError(f"{field} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise EspnParseError(f"{field} root must be an object")
    return value


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise EspnParseError("source contains a non-JSON optional value") from exc


def required_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EspnParseError(f"{field} must be an object")
    return value


def required_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise EspnParseError(f"{field} must be an array")
    return value


def required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EspnParseError(f"{field} must be a non-empty string")
    return value.strip()


def optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return required_string(value, field)


def native_id(value: Any, field: str) -> int:
    if type(value) is int:
        result = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.isdigit()
        and not value.startswith("0")
    ):
        result = int(value)
    else:
        raise EspnParseError(f"{field} must be a canonical positive native ID")
    if result <= 0:
        raise EspnParseError(f"{field} must be a canonical positive native ID")
    return result


def source_year(value: Any, field: str) -> int:
    if type(value) is not int or value < 1800:
        raise EspnParseError(f"{field} must be an integer ESPN season year")
    return value


def optional_nonnegative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if type(value) is int:
        result = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        result = int(value)
    else:
        raise EspnParseError(f"{field} must be a non-negative integer")
    if result < 0:
        raise EspnParseError(f"{field} must be a non-negative integer")
    return result


def optional_bool(value: Any, field: str) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        raise EspnParseError(f"{field} must be boolean when present")
    return value


def utc_datetime(value: Any, field: str) -> datetime:
    text = required_string(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise EspnParseError(f"{field} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EspnParseError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def unknown_fields(value: Mapping[str, Any], known: Sequence[str]) -> dict[str, Any]:
    known_set = set(known)
    return {str(key): item for key, item in value.items() if key not in known_set}


def source_day_bounds(start: date, end: date) -> tuple[date, date]:
    """Translate ESPN source-calendar bounds to their admissible UTC dates."""

    minimum = max(date.min.toordinal(), start.toordinal() - 1)
    maximum = min(date.max.toordinal(), end.toordinal() + 1)
    return date.fromordinal(minimum), date.fromordinal(maximum)


def source_day_contains(value: date, start: date, end: date) -> bool:
    buffered_start, buffered_end = source_day_bounds(start, end)
    return buffered_start <= value <= buffered_end
