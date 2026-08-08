"""Canonical codec for exact FotMob ``competition_id=source_season`` scopes."""

from __future__ import annotations

from collections.abc import Iterable
import unicodedata


def _is_control(value: str) -> bool:
    return any(unicodedata.category(character) == "Cc" for character in value)


def _validate_source_season_key(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "," in value
        or _is_control(value)
    ):
        raise ValueError("source season key must be a non-empty exact value")
    return value


def parse_scope_token(value: str) -> tuple[int, str]:
    """Parse one exact scope, splitting only on its first equals sign."""

    if not isinstance(value, str) or value != value.strip() or "=" not in value:
        raise ValueError("scope must be a positive numeric ID=exact-season token")
    competition_text, source_season_key = value.split("=", 1)
    if (
        not competition_text
        or not competition_text.isascii()
        or not competition_text.isdecimal()
        or competition_text.startswith("0")
    ):
        raise ValueError("scope competition ID must be a positive ASCII decimal")
    return int(competition_text), _validate_source_season_key(source_season_key)


def format_scope_token(competition_id: int, source_season_key: str) -> str:
    """Format one exact scope after applying the same closed validation."""

    if type(competition_id) is not int or competition_id <= 0:
        raise ValueError("scope competition ID must be a positive integer")
    season = _validate_source_season_key(source_season_key)
    return f"{competition_id}={season}"


def parse_scope_groups(values: Iterable[str]) -> tuple[tuple[int, str], ...]:
    """Parse repeatable comma-separated CLI groups, retaining first occurrence."""

    scopes: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    for group in values:
        if not isinstance(group, str):
            raise ValueError("scope group must be a string")
        if not group:
            continue
        for token in group.split(","):
            if not token:
                continue
            scope = parse_scope_token(token)
            if scope not in seen:
                scopes.append(scope)
                seen.add(scope)
    return tuple(scopes)


def validate_scope_tokens(values: Iterable[str]) -> tuple[str, ...]:
    """Validate already-tokenized evidence and retain first occurrence order."""

    tokens: list[str] = []
    seen: set[tuple[int, str]] = set()
    for value in values:
        scope = parse_scope_token(value)
        if scope not in seen:
            tokens.append(format_scope_token(*scope))
            seen.add(scope)
    return tuple(tokens)
