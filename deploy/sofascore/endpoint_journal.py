"""Read-only adapter for the SofaScore endpoint journal SQL template.

``render_journal_sql`` renders the adjacent
``sofascore_endpoint_journal.sql`` template with a validated match-id list
(execution stays with the caller), and ``summarize_journal`` folds result
rows into per-match capture state without coupling to Trino or the manifest
store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping


CORE_ENDPOINTS = ("event", "lineups", "statistics", "shotmap", "incidents")

JOURNAL_COLUMNS = (
    "match_id",
    "league",
    "season",
    "source_tournament_id",
    "endpoint",
    "status",
    "manifest_status",
    "http_status",
    "status_updated_at",
    "observed_at",
    "identity_conflict",
)

SENTINEL_MATCH_ID = "__sentinel__"

JOURNAL_STATUSES = frozenset(
    {"collected", "source_404", "quarantine", "not_collected"}
)
TERMINAL_STATUSES = frozenset({"collected", "source_404"})

_SQL_TEMPLATE = Path(__file__).with_name("sofascore_endpoint_journal.sql")


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _truthy_conflict(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true"}
    return bool(value)


def _epoch(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        stamp = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            stamp = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{name} is not an ISO timestamp: {value!r}") from exc
    else:
        raise ValueError(f"{name} is not an ISO timestamp: {value!r}")
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware: {value!r}")
    return stamp.timestamp()


def render_journal_sql(match_ids: Iterable[str]) -> str:
    """Render the adjacent SQL template with a validated, deduplicated id list."""

    normalized: set[str] = set()
    for value in match_ids:
        normalized.add(_required_text(value, "match_id"))
    if not normalized:
        raise ValueError("match_ids must contain at least one id")
    literal = ", ".join(
        "'" + text.replace("'", "''") + "'" for text in sorted(normalized)
    )
    template = _SQL_TEMPLATE.read_text(encoding="utf-8")
    if "{match_ids}" not in template:
        raise ValueError("SQL template is missing the {match_ids} placeholder")
    return template.replace("{match_ids}", literal)


@dataclass(frozen=True)
class JournalMatch:
    statuses: dict[str, str]
    capture_complete: bool
    closed_at: float | None
    observed_at: float | None


def summarize_journal(rows: Iterable[Mapping[str, object]]) -> dict[str, JournalMatch]:
    """Fold journal rows into per-match state; sentinel rows are skipped."""

    statuses: dict[str, dict[str, str]] = {}
    observed_stamps: dict[str, list[float]] = {}
    terminal_stamps: dict[str, list[float]] = {}
    terminal_missing: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()

    for row in rows:
        if row.get("match_id") == SENTINEL_MATCH_ID:
            continue
        missing = [column for column in JOURNAL_COLUMNS if column not in row]
        if missing:
            raise ValueError(f"journal row missing columns: {', '.join(missing)}")
        match_id = _required_text(row["match_id"], "match_id")
        endpoint = _required_text(row["endpoint"], "endpoint")
        if endpoint not in CORE_ENDPOINTS:
            raise ValueError(f"unsupported endpoint for match {match_id}: {endpoint!r}")
        if (match_id, endpoint) in seen:
            raise ValueError(f"duplicate journal row: {match_id}/{endpoint}")
        seen.add((match_id, endpoint))
        status = row["status"]
        if status not in JOURNAL_STATUSES:
            raise ValueError(f"invalid status for {match_id}/{endpoint}: {status!r}")
        if _truthy_conflict(row["identity_conflict"]):
            raise ValueError(f"identity conflict for match {match_id}")
        status_updated = _epoch(row["status_updated_at"], "status_updated_at")
        observed_at = _epoch(row["observed_at"], "observed_at")
        statuses.setdefault(match_id, {})[endpoint] = status
        if observed_at is not None:
            observed_stamps.setdefault(match_id, []).append(observed_at)
        if status in TERMINAL_STATUSES:
            if status_updated is None:
                terminal_missing[match_id] = terminal_missing.get(match_id, 0) + 1
            else:
                terminal_stamps.setdefault(match_id, []).append(status_updated)

    summary: dict[str, JournalMatch] = {}
    for match_id, endpoint_statuses in statuses.items():
        complete = (
            len(endpoint_statuses) == len(CORE_ENDPOINTS)
            and all(status in TERMINAL_STATUSES for status in endpoint_statuses.values())
        )
        closed_at: float | None = None
        if complete and not terminal_missing.get(match_id):
            closed_at = max(terminal_stamps[match_id])
        summary[match_id] = JournalMatch(
            statuses=endpoint_statuses,
            capture_complete=complete,
            closed_at=closed_at,
            observed_at=(
                max(observed_stamps[match_id])
                if observed_stamps.get(match_id)
                else None
            ),
        )
    return summary
