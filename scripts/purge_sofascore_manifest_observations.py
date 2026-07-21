#!/usr/bin/env python3
"""Guarded one-shot purge for the 11 stale SofaScore manifest observations.

Issue #999 identified eleven ``retryable_failure`` observations in the old
World Cup partition (source tournament 16, season 58210).  Newer terminal
observations already supersede every affected target, so retaining these rows
only makes operational checks noisy.

The script is deliberately fail-closed.  Its default mode is a read-only
preflight; ``--apply`` is the only path that emits a DELETE.  The write is one
row-level DELETE over the exact six-column natural-key allowlist.  It never
updates statuses and never deletes an entire partition.

Run from a container that can reach Trino::

    python /opt/airflow/scripts/purge_sofascore_manifest_observations.py
    python /opt/airflow/scripts/purge_sofascore_manifest_observations.py --apply

Authenticated HTTPS verifies certificates by default and honors
``TRINO_TLS_VERIFY``, ``REQUESTS_CA_BUNDLE``, and ``SSL_CERT_FILE``.  An
explicit insecure opt-out is accepted only for the read-only dry-run.

Before the DELETE, the current Iceberg ``main`` snapshot is recorded.  A
rollback procedure is printed only as a conditional instruction: writers must
first be quiesced and ``main`` must still point at the purge snapshot.  A
concurrent descendant makes an unconditional rollback unsafe.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("purge_sofascore_manifest_observations")

TABLE = "iceberg.ops.sofascore_capture_manifest"
SNAPSHOTS_TABLE = 'iceberg.ops."sofascore_capture_manifest$snapshots"'
REFS_TABLE = 'iceberg.ops."sofascore_capture_manifest$refs"'
SOURCE_TOURNAMENT_ID = "16"
SOURCE_SEASON_ID = "58210"
DOOMED_STATUS = "retryable_failure"
REPLACEMENT_STATUSES = ("success", "legitimate_empty")
REPLACEMENT_STATUSES_BY_ENDPOINT: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "schedule_last": frozenset({"success"}),
        "standings_total": frozenset({"success"}),
        "schedule_next": frozenset({"success", "legitimate_empty"}),
    }
)
FINAL_EVENT_ENDPOINTS = ("event", "lineups", "statistics", "shotmap", "incidents")
EXPECTED_FINAL_EVENT_COUNT = 104

NATURAL_KEY_COLUMNS = (
    "source_tournament_id",
    "source_season_id",
    "target_type",
    "target_id",
    "endpoint",
    "freshness_key",
)


@dataclass(frozen=True, order=True)
class ManifestKey:
    source_tournament_id: str
    source_season_id: str
    target_type: str
    target_id: str
    endpoint: str
    freshness_key: str

    def as_tuple(self) -> tuple[str, str, str, str, str, str]:
        return tuple(getattr(self, column) for column in NATURAL_KEY_COLUMNS)  # type: ignore[return-value]

    def stable_id(self) -> str:
        return "/".join(self.as_tuple())


def _key(
    target_type: str,
    target_id: str,
    endpoint: str,
    freshness_key: str,
) -> ManifestKey:
    return ManifestKey(
        SOURCE_TOURNAMENT_ID,
        SOURCE_SEASON_ID,
        target_type,
        target_id,
        endpoint,
        freshness_key,
    )


# Immutable destructive allowlist.  Do not broaden this to a partition/status
# predicate: six other historical failures intentionally remain out of scope.
PURGE_ALLOWLIST = frozenset(
    {
        _key("season_page", "last:0", "schedule_last", "day-2026-07-16"),
        *(
            _key("season_page", f"last:{page}", "schedule_last", "day-2026-07-18")
            for page in range(4)
        ),
        _key("season", SOURCE_SEASON_ID, "standings_total", "day-2026-07-19"),
        *(
            _key("season_page", f"last:{page}", "schedule_last", "day-2026-07-19")
            for page in range(4)
        ),
        _key("season_page", "next:0", "schedule_next", "day-2026-07-19"),
    }
)
TRANSPORT_ERROR_KEY = _key(
    "season_page", "last:0", "schedule_last", "day-2026-07-16"
)


# Captured read-only from production on 2026-07-21.  These immutable cutoffs
# let an already-clean rerun still prove that every replacement is strictly
# newer than the deleted observation.  A first apply uses the live candidate
# timestamp (and therefore remains stricter if a key was ever rewritten).
_OBSERVED_CANDIDATE_UPDATED_AT: Mapping[ManifestKey, str] = MappingProxyType(
    {
        _key(
            "season_page", "last:0", "schedule_last", "day-2026-07-16"
        ): "2026-07-16T21:38:31.107367+00:00",
        _key(
            "season_page", "last:0", "schedule_last", "day-2026-07-18"
        ): "2026-07-18T23:14:59.467099+00:00",
        _key(
            "season_page", "last:1", "schedule_last", "day-2026-07-18"
        ): "2026-07-18T23:14:59.599168+00:00",
        _key(
            "season_page", "last:2", "schedule_last", "day-2026-07-18"
        ): "2026-07-18T23:14:59.553108+00:00",
        _key(
            "season_page", "last:3", "schedule_last", "day-2026-07-18"
        ): "2026-07-18T23:14:59.691470+00:00",
        _key(
            "season_page", "last:0", "schedule_last", "day-2026-07-19"
        ): "2026-07-20T00:02:41.613257+00:00",
        _key(
            "season_page", "last:1", "schedule_last", "day-2026-07-19"
        ): "2026-07-20T00:02:42.312410+00:00",
        _key(
            "season_page", "last:2", "schedule_last", "day-2026-07-19"
        ): "2026-07-20T00:02:43.067192+00:00",
        _key(
            "season_page", "last:3", "schedule_last", "day-2026-07-19"
        ): "2026-07-20T00:02:43.744029+00:00",
        _key(
            "season_page", "next:0", "schedule_next", "day-2026-07-19"
        ): "2026-07-20T00:02:57.599280+00:00",
        _key(
            "season", SOURCE_SEASON_ID, "standings_total", "day-2026-07-19"
        ): "2026-07-20T00:03:02.607688+00:00",
    }
)


@dataclass(frozen=True)
class Candidate:
    key: ManifestKey
    status: str
    error_type: str | None
    raw_content_hash: str | None
    raw_blob_key: str | None
    updated_at: str


@dataclass(frozen=True)
class TerminalObservation:
    target_type: str
    target_id: str
    endpoint: str
    freshness_key: str
    status: str
    updated_at: str
    raw_content_hash: str | None
    raw_blob_key: str | None
    http_status: int | None
    row_count: int

    @property
    def target(self) -> tuple[str, str, str]:
        return (self.target_type, self.target_id, self.endpoint)


@dataclass(frozen=True)
class FinalEvidence:
    event_count: int
    observation_count: int
    endpoint_count: int
    min_endpoints_per_event: int
    max_endpoints_per_event: int


@dataclass(frozen=True)
class SnapshotMetadata:
    snapshot_id: int
    parent_id: int | None
    committed_at: str
    operation: str
    summary: Mapping[str, str]


@dataclass(frozen=True)
class DeleteExecution:
    query_id_after_execute: str | None
    query_id_after_fetch: str | None
    rowcount: int


@dataclass(frozen=True)
class PurgeResult:
    applied: bool
    deleted_rows: int
    already_clean: bool
    pre_snapshot_id: int | None = None
    purge_snapshot_id: int | None = None
    delete_query_id: str | None = None
    delete_rowcount: int | None = None
    snapshot_summary_count: int | None = None
    conditional_rollback_instruction: str | None = None


class PurgeRefused(RuntimeError):
    """A precondition failed before any destructive SQL was issued."""


class PostDeleteVerificationError(RuntimeError):
    """The DELETE was sent or a mandatory post-send check failed."""

    def __init__(
        self,
        message: str,
        *,
        conditional_rollback_instruction: str | None = None,
    ) -> None:
        super().__init__(message)
        self.conditional_rollback_instruction = conditional_rollback_instruction


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _in_literals(values: Iterable[str]) -> str:
    return ", ".join(_sql_literal(value) for value in values)


def _execute(cursor, sql: str) -> list[Sequence[object]]:
    compact = " ".join(sql.split())
    logger.info("SQL: %s%s", compact[:180], "..." if len(compact) > 180 else "")
    cursor.execute(sql)
    return list(cursor.fetchall())


def _db_integer(value: object, *, field: str, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise PurgeRefused(
            f"terminal replacement {field} is not an integer: {value!r}"
        )
    return value


def _key_predicate(keys: Iterable[ManifestKey], *, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    clauses = []
    for key in sorted(keys):
        clauses.append(
            "(" + " AND ".join(
                f"{prefix}{column} = {_sql_literal(value)}"
                for column, value in zip(NATURAL_KEY_COLUMNS, key.as_tuple())
            ) + ")"
        )
    if not clauses:
        raise ValueError("exact-key predicate cannot be empty")
    return "(\n        " + "\n        OR ".join(clauses) + "\n    )"


def _target_predicate(*, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    targets = sorted(
        {(key.target_type, key.target_id, key.endpoint) for key in PURGE_ALLOWLIST}
    )
    return "(\n        " + "\n        OR ".join(
        "(" + " AND ".join(
            (
                f"{prefix}target_type = {_sql_literal(target_type)}",
                f"{prefix}target_id = {_sql_literal(target_id)}",
                f"{prefix}endpoint = {_sql_literal(endpoint)}",
            )
        ) + ")"
        for target_type, target_id, endpoint in targets
    ) + "\n    )"


def load_candidates(cursor) -> list[Candidate]:
    """Read every retryable row in the partition, not just the allowlist."""

    rows = _execute(
        cursor,
        f"""
SELECT {', '.join(NATURAL_KEY_COLUMNS)}, status, error_type,
       raw_content_hash, raw_blob_key, updated_at
FROM {TABLE}
WHERE source_tournament_id = {_sql_literal(SOURCE_TOURNAMENT_ID)}
  AND source_season_id = {_sql_literal(SOURCE_SEASON_ID)}
  AND status = {_sql_literal(DOOMED_STATUS)}
ORDER BY freshness_key, endpoint, target_type, target_id
""".strip(),
    )
    candidates: list[Candidate] = []
    for row in rows:
        if len(row) != 11:
            raise PurgeRefused(
                f"candidate query returned {len(row)} columns; expected 11"
            )
        candidates.append(
            Candidate(
                key=ManifestKey(*(str(value) for value in row[:6])),
                status=str(row[6]),
                error_type=None if row[7] is None else str(row[7]),
                raw_content_hash=None if row[8] is None else str(row[8]),
                raw_blob_key=None if row[9] is None else str(row[9]),
                updated_at=str(row[10]),
            )
        )
    return candidates


def load_terminal_observations(cursor) -> list[TerminalObservation]:
    rows = _execute(
        cursor,
        f"""
SELECT target_type, target_id, endpoint, freshness_key, status, updated_at,
       raw_content_hash, raw_blob_key, http_status, row_count
FROM {TABLE}
WHERE source_tournament_id = {_sql_literal(SOURCE_TOURNAMENT_ID)}
  AND source_season_id = {_sql_literal(SOURCE_SEASON_ID)}
  AND status IN ({_in_literals(REPLACEMENT_STATUSES)})
  AND {_target_predicate()}
ORDER BY target_type, target_id, endpoint, updated_at
""".strip(),
    )
    observations: list[TerminalObservation] = []
    for row in rows:
        if len(row) != 10:
            raise PurgeRefused(
                f"terminal query returned {len(row)} columns; expected 10"
            )
        http_status = _db_integer(
            row[8],
            field="http_status",
            optional=True,
        )
        row_count = _db_integer(row[9], field="row_count")
        if row_count is None:  # pragma: no cover - guarded by optional=False
            raise PurgeRefused("terminal replacement row_count is null")
        observations.append(
            TerminalObservation(
                target_type=str(row[0]),
                target_id=str(row[1]),
                endpoint=str(row[2]),
                freshness_key=str(row[3]),
                status=str(row[4]),
                updated_at=str(row[5]),
                raw_content_hash=None if row[6] is None else str(row[6]),
                raw_blob_key=None if row[7] is None else str(row[7]),
                http_status=http_status,
                row_count=row_count,
            )
        )
    return observations


def load_final_evidence(cursor) -> FinalEvidence:
    rows = _execute(
        cursor,
        f"""
WITH final_success AS (
    SELECT target_id, endpoint
    FROM {TABLE}
    WHERE source_tournament_id = {_sql_literal(SOURCE_TOURNAMENT_ID)}
      AND source_season_id = {_sql_literal(SOURCE_SEASON_ID)}
      AND target_type = 'event'
      AND freshness_key = 'final'
      AND status = 'success'
      AND endpoint IN ({_in_literals(FINAL_EVENT_ENDPOINTS)})
), per_event AS (
    SELECT target_id, COUNT(DISTINCT endpoint) AS endpoint_count
    FROM final_success
    GROUP BY target_id
)
SELECT (SELECT COUNT(*) FROM per_event) AS event_count,
       (SELECT COUNT(*) FROM final_success) AS observation_count,
       (SELECT COUNT(DISTINCT endpoint) FROM final_success) AS endpoint_count,
       COALESCE((SELECT MIN(endpoint_count) FROM per_event), 0) AS min_per_event,
       COALESCE((SELECT MAX(endpoint_count) FROM per_event), 0) AS max_per_event
""".strip(),
    )
    if len(rows) != 1 or len(rows[0]) != 5:
        raise PurgeRefused("final-event evidence query returned an invalid shape")
    return FinalEvidence(*(int(value) for value in rows[0]))


def count_table_rows(cursor) -> int:
    rows = _execute(cursor, f"SELECT COUNT(*) AS total_rows FROM {TABLE}")
    if len(rows) != 1 or len(rows[0]) != 1:
        raise PurgeRefused("table row-count query returned an invalid shape")
    return int(rows[0][0])


def count_protected_terminal_rows(cursor) -> int:
    rows = _execute(
        cursor,
        f"""
SELECT COUNT(*) AS protected_terminal_rows
FROM {TABLE}
WHERE source_tournament_id = {_sql_literal(SOURCE_TOURNAMENT_ID)}
  AND source_season_id = {_sql_literal(SOURCE_SEASON_ID)}
  AND status IN ({_in_literals(REPLACEMENT_STATUSES)})
""".strip(),
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise PurgeRefused("terminal row-count query returned an invalid shape")
    return int(rows[0][0])


def load_current_snapshot(cursor) -> SnapshotMetadata:
    """Resolve the actual current ``main`` ref and its direct parent.

    Ordering ``$snapshots`` by commit time is insufficient after a rollback:
    historical descendants remain in the metadata table.  Trino 482 exposes
    the authoritative branch head through ``$refs``.
    """

    rows = _execute(
        cursor,
        f"""
SELECT r.snapshot_id, s.parent_id, CAST(s.committed_at AS varchar), s.operation,
       s.summary
FROM {REFS_TABLE} r
JOIN {SNAPSHOTS_TABLE} s ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main' AND r.type = 'BRANCH'
""".strip(),
    )
    if len(rows) != 1 or len(rows[0]) != 5 or rows[0][0] is None:
        raise PurgeRefused("cannot establish the current Iceberg main snapshot")
    raw_summary = rows[0][4]
    if not isinstance(raw_summary, Mapping):
        raise PurgeRefused("current Iceberg snapshot summary is not a map")
    return SnapshotMetadata(
        snapshot_id=int(rows[0][0]),
        parent_id=None if rows[0][1] is None else int(rows[0][1]),
        committed_at=str(rows[0][2]),
        operation=str(rows[0][3]),
        summary=MappingProxyType(
            {str(key): str(value) for key, value in raw_summary.items()}
        ),
    )


def rollback_call(snapshot_id: int) -> str:
    return (
        "CALL iceberg.system.rollback_to_snapshot("
        f"'ops', 'sofascore_capture_manifest', {int(snapshot_id)})"
    )


def conditional_rollback_instruction(
    *,
    pre_snapshot_id: int,
    purge_snapshot_id: int,
) -> str:
    return (
        "QUIESCE ALL WRITERS; only if iceberg.ops."
        '"sofascore_capture_manifest$refs" main snapshot_id still equals '
        f"{int(purge_snapshot_id)}, run: {rollback_call(pre_snapshot_id)}"
    )


def snapshot_summary_count(snapshot: SnapshotMetadata) -> int | None:
    """Return a diagnostic count hint; DB-API rowcount stays authoritative."""

    for key in (
        "deleted-records",
        "added-position-deletes",
        "removed-records",
    ):
        value = snapshot.summary.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def validate_delete_ownership(
    *,
    pre_snapshot: SnapshotMetadata,
    purge_snapshot: SnapshotMetadata,
    execution: DeleteExecution,
) -> str:
    """Prove that the direct child snapshot belongs to this exact DELETE."""

    query_id = execution.query_id_after_fetch
    ownership_error: str | None = None
    if (
        not execution.query_id_after_execute
        or not query_id
        or execution.query_id_after_execute != query_id
    ):
        ownership_error = (
            "cursor query_id is missing or changed between execute and fetch "
            f"({execution.query_id_after_execute!r} -> {query_id!r})"
        )
    elif execution.rowcount != len(PURGE_ALLOWLIST):
        ownership_error = (
            "Trino DELETE update count is not exactly 11 "
            f"(rowcount={execution.rowcount})"
        )
    elif (
        purge_snapshot.snapshot_id == pre_snapshot.snapshot_id
        or purge_snapshot.parent_id != pre_snapshot.snapshot_id
    ):
        ownership_error = (
            "current snapshot is not exactly one direct child of pre-delete "
            f"(pre={pre_snapshot.snapshot_id}, current={purge_snapshot.snapshot_id}, "
            f"parent={purge_snapshot.parent_id})"
        )
    elif purge_snapshot.operation != "delete":
        ownership_error = (
            "direct child snapshot operation is not delete "
            f"(operation={purge_snapshot.operation!r})"
        )
    elif purge_snapshot.summary.get("trino_query_id") != query_id:
        ownership_error = (
            "direct delete snapshot trino_query_id does not own this cursor "
            f"(cursor={query_id!r}, "
            f"snapshot={purge_snapshot.summary.get('trino_query_id')!r})"
        )

    if ownership_error is not None:
        raise PostDeleteVerificationError(
            "DELETE outcome is unknown and ownership is unproven; do not run "
            "a rollback automatically: " + ownership_error
        )
    return query_id


def render_delete_sql() -> str:
    return f"""
DELETE FROM {TABLE}
WHERE status = {_sql_literal(DOOMED_STATUS)}
  AND {_key_predicate(PURGE_ALLOWLIST)}
""".strip()


def delete_allowlisted_rows(cursor) -> DeleteExecution:
    """Send the single DELETE and capture DB-API ownership metadata.

    ``trino-python-client`` 0.338 exposes the query id after ``execute`` and
    populates ``rowcount`` from Trino's update count once ``fetchall`` drains
    the response.  Callers must treat every exception here as an unknown
    commit outcome: the server may have accepted the statement already.
    """

    sql = render_delete_sql()
    compact = " ".join(sql.split())
    logger.info("SQL: %s%s", compact[:180], "..." if len(compact) > 180 else "")
    cursor.execute(sql)
    query_id_after_execute = getattr(cursor, "query_id", None)
    cursor.fetchall()
    query_id_after_fetch = getattr(cursor, "query_id", None)
    rowcount = int(getattr(cursor, "rowcount", -1))
    return DeleteExecution(
        query_id_after_execute=(
            None if query_id_after_execute is None else str(query_id_after_execute)
        ),
        query_id_after_fetch=(
            None if query_id_after_fetch is None else str(query_id_after_fetch)
        ),
        rowcount=rowcount,
    )


def _parse_timestamp(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PurgeRefused(f"invalid {label} timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PurgeRefused(f"{label} timestamp is not timezone-aware: {value!r}")
    return parsed


def _parse_day_freshness(value: str, *, label: str) -> date:
    normalized = str(value)
    if re.fullmatch(r"day-\d{4}-\d{2}-\d{2}", normalized) is None:
        raise PurgeRefused(
            f"{label} freshness_key is not canonical day-YYYY-MM-DD: {value!r}"
        )
    try:
        return date.fromisoformat(normalized.removeprefix("day-"))
    except ValueError as exc:
        raise PurgeRefused(
            f"invalid {label} freshness_key calendar day: {value!r}"
        ) from exc


def validate_candidate_set(candidates: Sequence[Candidate]) -> bool:
    """Return True for an idempotently clean partition; otherwise validate 11."""

    if not candidates:
        return True
    keys = [candidate.key for candidate in candidates]
    key_set = set(keys)
    missing = PURGE_ALLOWLIST - key_set
    unexpected = key_set - PURGE_ALLOWLIST
    duplicates = len(keys) - len(key_set)
    if missing or unexpected or duplicates:
        details = []
        if missing:
            details.append(
                "missing=" + ",".join(key.stable_id() for key in sorted(missing))
            )
        if unexpected:
            details.append(
                "unexpected="
                + ",".join(key.stable_id() for key in sorted(unexpected))
            )
        if duplicates:
            details.append(f"duplicate_rows={duplicates}")
        raise PurgeRefused(
            "retryable candidate key set is not the exact 11-key allowlist: "
            + "; ".join(details)
        )
    if len(candidates) != 11:
        raise PurgeRefused(f"expected exactly 11 candidates, got {len(candidates)}")
    return False


def validate_candidate_evidence(candidates: Sequence[Candidate]) -> None:
    def has_raw_value(value: str | None) -> bool:
        return bool(value and value.strip())

    by_key = {candidate.key: candidate for candidate in candidates}
    transport = by_key.get(TRANSPORT_ERROR_KEY)
    if transport is None or transport.error_type != "TransportError":
        raise PurgeRefused(
            "candidate error fingerprint changed: the only TransportError must be "
            + TRANSPORT_ERROR_KEY.stable_id()
        )
    deferred = [
        candidate
        for candidate in candidates
        if candidate.key != TRANSPORT_ERROR_KEY
        and candidate.error_type == "DeferredMaterialization"
    ]
    unexpected_errors = [
        candidate.key.stable_id()
        for candidate in candidates
        if candidate.key != TRANSPORT_ERROR_KEY
        and candidate.error_type != "DeferredMaterialization"
    ]
    if len(deferred) != 10 or unexpected_errors:
        raise PurgeRefused(
            "candidate error fingerprint changed: all other 10 keys must be "
            "DeferredMaterialization; invalid=" + ", ".join(unexpected_errors)
        )
    invalid_deferred = [
        candidate.key.stable_id()
        for candidate in deferred
        if not has_raw_value(candidate.raw_content_hash)
        or not has_raw_value(candidate.raw_blob_key)
    ]
    if invalid_deferred:
        raise PurgeRefused(
            "DeferredMaterialization candidate lacks committed raw lineage: "
            + ", ".join(invalid_deferred)
        )
    invalid_transport = [
        candidate.key.stable_id()
        for candidate in (transport,)
        if has_raw_value(candidate.raw_content_hash)
        or has_raw_value(candidate.raw_blob_key)
    ]
    if invalid_transport:
        raise PurgeRefused(
            "TransportError candidate unexpectedly has raw lineage: "
            + ", ".join(invalid_transport)
        )


def validate_terminal_manifest_invariants(observation: TerminalObservation) -> None:
    """Mirror the persisted ``EndpointManifest`` terminal-state invariants."""

    def has_lineage(value: str | None) -> bool:
        return bool(value)

    if not has_lineage(observation.raw_content_hash) or not has_lineage(
        observation.raw_blob_key
    ):
        raise PurgeRefused(
            f"{observation.status} replacement lacks committed raw lineage: "
            f"{observation.target_type}/{observation.target_id}/{observation.endpoint}"
        )
    if isinstance(observation.row_count, bool) or not isinstance(
        observation.row_count, int
    ):
        raise PurgeRefused(
            f"replacement row_count is not an integer: {observation.row_count!r}"
        )
    if observation.row_count < 0:
        raise PurgeRefused(
            f"replacement row_count must be non-negative: {observation.row_count}"
        )

    http_status = observation.http_status
    if http_status is not None and (
        isinstance(http_status, bool) or not isinstance(http_status, int)
    ):
        raise PurgeRefused(
            f"replacement http_status is not an integer: {http_status!r}"
        )
    if http_status in {403, 429} or (
        http_status is not None and http_status >= 500
    ):
        raise PurgeRefused(
            f"HTTP {http_status} cannot be terminal replacement status "
            f"{observation.status!r}"
        )

    if observation.status == "success":
        if (
            http_status is None
            or not 200 <= http_status < 300
            or http_status == 204
        ):
            raise PurgeRefused(
                "success replacement requires a non-empty 2xx HTTP response "
                f"(got {http_status!r})"
            )
        if observation.row_count <= 0:
            raise PurgeRefused(
                "success replacement requires row_count > 0 "
                f"(got {observation.row_count})"
            )


def validate_replacements(
    observations: Sequence[TerminalObservation],
    candidates: Sequence[Candidate],
) -> None:
    by_target: dict[
        tuple[str, str, str],
        list[tuple[TerminalObservation, date, datetime]],
    ] = {}
    for observation in observations:
        allowed_statuses = REPLACEMENT_STATUSES_BY_ENDPOINT.get(observation.endpoint)
        if allowed_statuses is None:
            raise PurgeRefused(
                f"replacement endpoint is outside the guarded set: {observation.endpoint!r}"
            )
        if observation.status not in allowed_statuses:
            raise PurgeRefused(
                f"status {observation.status!r} is not valid replacement evidence "
                f"for endpoint {observation.endpoint!r}"
            )
        validate_terminal_manifest_invariants(observation)
        freshness_day = _parse_day_freshness(
            observation.freshness_key,
            label="terminal",
        )
        updated_at = _parse_timestamp(observation.updated_at, label="terminal")
        by_target.setdefault(observation.target, []).append(
            (observation, freshness_day, updated_at)
        )

    live_cutoffs = {candidate.key: candidate.updated_at for candidate in candidates}
    missing: list[str] = []
    for key in sorted(PURGE_ALLOWLIST):
        cutoff_value = live_cutoffs.get(key, _OBSERVED_CANDIDATE_UPDATED_AT[key])
        cutoff = _parse_timestamp(cutoff_value, label="candidate")
        candidate_day = _parse_day_freshness(key.freshness_key, label="candidate")
        target = (key.target_type, key.target_id, key.endpoint)
        replacements = by_target.get(target, ())
        if not any(
            replacement_day > candidate_day and replacement_updated_at > cutoff
            for _observation, replacement_day, replacement_updated_at in replacements
        ):
            missing.append(key.stable_id())
    if missing:
        raise PurgeRefused(
            "no terminal observation with both strictly newer day freshness and "
            "updated_at for: " + ", ".join(missing)
        )


def validate_final_evidence(evidence: FinalEvidence) -> None:
    expected_observations = EXPECTED_FINAL_EVENT_COUNT * len(FINAL_EVENT_ENDPOINTS)
    expected = FinalEvidence(
        event_count=EXPECTED_FINAL_EVENT_COUNT,
        observation_count=expected_observations,
        endpoint_count=len(FINAL_EVENT_ENDPOINTS),
        min_endpoints_per_event=len(FINAL_EVENT_ENDPOINTS),
        max_endpoints_per_event=len(FINAL_EVENT_ENDPOINTS),
    )
    if evidence != expected:
        raise PurgeRefused(
            "World Cup final event evidence changed: "
            f"expected {expected}, got {evidence}"
        )


def purge(cursor, *, apply: bool = False) -> PurgeResult:
    snapshot_guard = load_current_snapshot(cursor) if apply else None
    candidates = load_candidates(cursor)
    already_clean = validate_candidate_set(candidates)
    if not already_clean:
        validate_candidate_evidence(candidates)

    replacements_before = load_terminal_observations(cursor)
    validate_replacements(replacements_before, candidates)
    final_before = load_final_evidence(cursor)
    validate_final_evidence(final_before)

    if already_clean:
        if snapshot_guard is not None:
            clean_end_snapshot = load_current_snapshot(cursor)
            if clean_end_snapshot.snapshot_id != snapshot_guard.snapshot_id:
                raise PurgeRefused(
                    "Iceberg main snapshot changed during idempotent validation; "
                    "retry in a quiet window"
                )
        logger.info(
            "Manifest is already clean; replacement and 104x5 final evidence remain valid"
        )
        return PurgeResult(
            applied=apply,
            deleted_rows=0,
            already_clean=True,
        )

    logger.info("Preflight passed for the exact 11-key candidate set")
    if not apply:
        logger.info("DRY-RUN: would delete exactly 11 rows; pass --apply to write")
        return PurgeResult(applied=False, deleted_rows=0, already_clean=False)

    total_before = count_table_rows(cursor)
    protected_before = count_protected_terminal_rows(cursor)
    pre_snapshot = load_current_snapshot(cursor)
    if snapshot_guard is None:  # pragma: no cover - apply branch invariant
        raise AssertionError("apply requires an initial snapshot guard")
    if pre_snapshot.snapshot_id != snapshot_guard.snapshot_id:
        raise PurgeRefused(
            "Iceberg main snapshot changed during preflight; no DELETE was issued "
            f"(start={snapshot_guard.snapshot_id}, current={pre_snapshot.snapshot_id})"
        )
    logger.warning(
        "PRE-DELETE CURRENT SNAPSHOT: id=%s parent=%s operation=%s",
        pre_snapshot.snapshot_id,
        pre_snapshot.parent_id,
        pre_snapshot.operation,
    )
    logger.warning(
        "No rollback instruction exists until DELETE query ownership and "
        "rowcount=11 are proven"
    )

    try:
        delete_execution = delete_allowlisted_rows(cursor)
        purge_snapshot = load_current_snapshot(cursor)
    except Exception as exc:
        raise PostDeleteVerificationError(
            "DELETE was sent or may have been sent, but its first snapshot could "
            "not be owned; outcome is unknown and no rollback instruction is safe"
        ) from exc
    delete_query_id = validate_delete_ownership(
        pre_snapshot=pre_snapshot,
        purge_snapshot=purge_snapshot,
        execution=delete_execution,
    )
    summary_count = snapshot_summary_count(purge_snapshot)
    rollback_instruction = conditional_rollback_instruction(
        pre_snapshot_id=pre_snapshot.snapshot_id,
        purge_snapshot_id=purge_snapshot.snapshot_id,
    )
    logger.warning(
        "PURGE SNAPSHOT OWNED: id=%s parent=%s operation=%s query_id=%s "
        "rowcount=%s summary_count_hint=%s",
        purge_snapshot.snapshot_id,
        purge_snapshot.parent_id,
        purge_snapshot.operation,
        delete_query_id,
        delete_execution.rowcount,
        summary_count,
    )
    logger.warning("CONDITIONAL ROLLBACK: %s", rollback_instruction)

    try:
        remaining = load_candidates(cursor)
        if remaining:
            raise PostDeleteVerificationError(
                f"{len(remaining)} retryable candidate rows remain after DELETE",
                conditional_rollback_instruction=rollback_instruction,
            )

        total_after = count_table_rows(cursor)
        if total_before - total_after != len(PURGE_ALLOWLIST):
            raise PostDeleteVerificationError(
                "table row count did not decrease by exactly 11: "
                f"before={total_before}, after={total_after}",
                conditional_rollback_instruction=rollback_instruction,
            )

        protected_after = count_protected_terminal_rows(cursor)
        if protected_after != protected_before:
            raise PostDeleteVerificationError(
                "protected terminal evidence count changed: "
                f"before={protected_before}, after={protected_after}",
                conditional_rollback_instruction=rollback_instruction,
            )

        replacements_after = load_terminal_observations(cursor)
        validate_replacements(replacements_after, ())
        final_after = load_final_evidence(cursor)
        validate_final_evidence(final_after)
        if final_after != final_before:
            raise PostDeleteVerificationError(
                f"final evidence changed: before={final_before}, after={final_after}",
                conditional_rollback_instruction=rollback_instruction,
            )

        final_snapshot = load_current_snapshot(cursor)
        if final_snapshot.snapshot_id != purge_snapshot.snapshot_id:
            raise PostDeleteVerificationError(
                "Iceberg main advanced during final verification; a concurrent "
                "snapshot exists, so do not run an unconditional rollback "
                f"(purge={purge_snapshot.snapshot_id}, "
                f"current={final_snapshot.snapshot_id})"
            )
        if final_snapshot.parent_id != pre_snapshot.snapshot_id:
            raise PostDeleteVerificationError(
                "purge snapshot parent changed during verification; inspect the "
                "snapshot lineage manually and do not run an unconditional rollback"
            )
    except PostDeleteVerificationError:
        raise
    except Exception as exc:
        raise PostDeleteVerificationError(
            str(exc),
            conditional_rollback_instruction=rollback_instruction,
        ) from exc

    logger.info(
        "Deleted exactly 11 allowlisted rows; terminal and 104x5 final evidence unchanged"
    )
    return PurgeResult(
        applied=True,
        deleted_rows=len(PURGE_ALLOWLIST),
        already_clean=False,
        pre_snapshot_id=pre_snapshot.snapshot_id,
        purge_snapshot_id=purge_snapshot.snapshot_id,
        delete_query_id=delete_query_id,
        delete_rowcount=delete_execution.rowcount,
        snapshot_summary_count=summary_count,
        conditional_rollback_instruction=rollback_instruction,
    )


def _trino_tls_verify(*, allow_insecure: bool = False) -> bool | str:
    """Resolve the repository-standard, fail-closed Trino TLS contract."""

    raw = os.environ.get("TRINO_TLS_VERIFY", "true").strip().casefold()
    if raw in {"0", "false", "no"}:
        if not allow_insecure:
            raise PurgeRefused(
                "TRINO_TLS_VERIFY=false is forbidden for --apply; configure a "
                "trusted CA bundle instead"
            )
        logger.warning(
            "TLS verification explicitly disabled for read-only dry-run only"
        )
        return False
    if raw not in {"1", "true", "yes"}:
        raise PurgeRefused(
            "TRINO_TLS_VERIFY must be one of true/false, yes/no, or 1/0"
        )
    for variable in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        bundle = os.environ.get(variable, "").strip()
        if bundle:
            return bundle
    return True


def get_conn(*, allow_insecure: bool = False):
    verify = _trino_tls_verify(allow_insecure=allow_insecure)

    import trino

    password = os.environ.get("TRINO_PASSWORD", "")
    user = os.environ.get("TRINO_USER", "airflow")
    default_port = 8443 if password else 8080
    kwargs: dict[str, Any] = {
        "host": os.environ.get("TRINO_HOST", "trino"),
        "port": int(os.environ.get("TRINO_PORT", default_port)),
        "user": user,
        "catalog": "iceberg",
    }
    if password:
        kwargs.update(
            http_scheme="https",
            auth=trino.auth.BasicAuthentication(user, password),
            verify=verify,
        )
    return trino.dbapi.connect(**kwargs)


def _close_quietly(resource, label: str) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:  # pragma: no cover - defensive cleanup path
        logger.exception("failed to close %s", label)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="execute the exact-key DELETE (default: read-only dry-run)",
    )
    args = parser.parse_args(argv)

    if not args.apply:
        logger.info("DRY-RUN: no writes are permitted without --apply")

    conn = None
    cursor = None
    try:
        conn = get_conn(allow_insecure=not args.apply)
        cursor = conn.cursor()
        purge(cursor, apply=args.apply)
        return 0
    except PurgeRefused as exc:
        logger.error("PURGE REFUSED: %s", exc)
        return 2
    except PostDeleteVerificationError as exc:
        logger.error("POST-DELETE CHECK FAILED: %s", exc)
        if exc.conditional_rollback_instruction:
            logger.error(
                "ROLLBACK IS CONDITIONAL: %s",
                exc.conditional_rollback_instruction,
            )
        else:
            logger.error(
                "No rollback instruction is safe: inspect current snapshot lineage "
                "and quiesce writers first"
            )
        return 3
    finally:
        _close_quietly(cursor, "cursor")
        _close_quietly(conn, "connection")


if __name__ == "__main__":
    sys.exit(main())
