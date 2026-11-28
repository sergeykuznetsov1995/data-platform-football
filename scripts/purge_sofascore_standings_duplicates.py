#!/usr/bin/env python3
"""Guarded one-shot purge for seven duplicate SofaScore standings rows.

Issue #2469: ``iceberg.bronze.sofascore_league_table`` holds seven natural-key
duplicates ``(league, season, "group", team_id)``. Every pair is one team under
two names; the row kept is the one captured from the newer source answer
(``raw_fetched_at``). The stale rows were written on 2026-11-22 by the raw
replay of older payloads (#1367) or by the earlier name-keyed writer. The
committed-state DQ gate fails the refresh scope for any affected partition,
so ``SS-52/2627`` turns the scheduled run red until they are gone.

Fail-closed by construction. The default mode is a read-only preflight;
``--apply`` is the only path that emits a DELETE: one row-level DELETE over
the exact seven row identities pinned below (partition key, name, batch,
ingest timestamp and raw hash). It never rewrites a partition.

Run from a container that can reach Trino::

    python /opt/airflow/scripts/purge_sofascore_standings_duplicates.py
    python /opt/airflow/scripts/purge_sofascore_standings_duplicates.py --apply

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
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping, Sequence


warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("purge_sofascore_standings_duplicates")

TABLE = "iceberg.bronze.sofascore_league_table"
SNAPSHOTS_TABLE = 'iceberg.bronze."sofascore_league_table$snapshots"'
REFS_TABLE = 'iceberg.bronze."sofascore_league_table$refs"'
# Real ids look like 20261124_173750_86204_dyben or 20261121_120820_33877_3qvg9.
QID = re.compile(r"[0-9]{8}_[0-9]{6}_[0-9]{5,}_[a-z0-9]{4,8}")
# Iceberg operations that remove rows: Trino writes "delete"; other engines
# may write a single "overwrite" for the same copy-on-write result.
REMOVAL_OPERATIONS = frozenset({"delete", "overwrite"})

# The preflight compares every pinned column. The DELETE arm carries all of
# them but raw_fetched_at, which the preflight has already pinned at the
# guarded snapshot, so a row that changed since the review can neither be
# deleted nor hide.
IDENTITY_COLUMNS = (
    "league",
    "season",
    "group",
    "team_id",
    "team",
    "batch_id",
    "ingested_at",
    "raw_content_hash",
    "raw_fetched_at",
)
ROW_QUERY_COLUMNS = (
    "league",
    "season",
    '"group"',
    "team_id",
    "team",
    "_batch_id",
    "CAST(_ingested_at AS varchar) AS ingested_at",
    "raw_content_hash",
    "raw_fetched_at",
)


@dataclass(frozen=True)
class StandingsRow:
    league: str
    season: str
    group: str
    team_id: str
    team: str
    batch_id: str
    ingested_at: str
    raw_content_hash: str
    raw_fetched_at: str

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return (self.league, self.season, self.group, self.team_id)

    def as_tuple(self) -> tuple[str, ...]:
        return tuple(getattr(self, column) for column in IDENTITY_COLUMNS)  # type: ignore[return-value]

    def stable_id(self) -> str:
        return "/".join(self.as_tuple())


# The stale row of every pair: the answer of the source is older than the
# keeper's one. Archive of all 28 columns for both rows of each pair:
# .local/tasks/2469-standings-duplicates/archive-14-rows.csv (2026-11-28).
STALE_ROWS = (
    StandingsRow(
        league="SS-203",
        season="2526",
        group="__total__",
        team_id="201654",
        team="Pari Nizhny Novgorod",
        batch_id="68932c87-fb60-4b61-bed0-3394b1ee55c5",
        ingested_at="2026-07-31 14:19:33.905385",
        raw_content_hash="cdd9f494ec3f602660fa6819c05c819e4c2997edbe70e1c5e8edaa6f3df21f48",
        raw_fetched_at="2026-07-31T14:17:20.824043+00:00",
    ),
    StandingsRow(
        league="SS-203",
        season="2627",
        group="__total__",
        team_id="2321",
        team="FC Zenit",
        batch_id="901bd69c-49af-4946-a36d-7b59b4cb1d48",
        ingested_at="2026-11-22 16:12:05.845150",
        raw_content_hash="0e7172f67e4d630009eb77acce000f77e41341d4b71b775023f13c01d2051a1d",
        raw_fetched_at="2026-09-13T14:17:52.361463+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1643",
        team="Lille",
        batch_id="61ddeba9-e902-4e9c-a63d-c6a9e5bb0f89",
        ingested_at="2026-11-22 20:47:02.070874",
        raw_content_hash="48d54d55408f1035e4d0a5b30a5b72d78e9418de6b59496bf80c759aec9ce082",
        raw_fetched_at="2026-10-26T00:23:17.176710+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1646",
        team="Auxerre",
        batch_id="61ddeba9-e902-4e9c-a63d-c6a9e5bb0f89",
        ingested_at="2026-11-22 20:47:02.070874",
        raw_content_hash="48d54d55408f1035e4d0a5b30a5b72d78e9418de6b59496bf80c759aec9ce082",
        raw_fetched_at="2026-10-26T00:23:17.176710+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1661",
        team="Nice",
        batch_id="61ddeba9-e902-4e9c-a63d-c6a9e5bb0f89",
        ingested_at="2026-11-22 20:47:02.070874",
        raw_content_hash="48d54d55408f1035e4d0a5b30a5b72d78e9418de6b59496bf80c759aec9ce082",
        raw_fetched_at="2026-10-26T00:23:17.176710+00:00",
    ),
    StandingsRow(
        league="SS-52",
        season="2627",
        group="__total__",
        team_id="207011",
        team="Amed Sportif Faaliyetler",
        batch_id="e0307b85-97c9-4869-b9d5-7224cbcd10c8",
        ingested_at="2026-11-22 15:30:05.718652",
        raw_content_hash="2e15e5f3e7ae3cbfe0aa266c9878996d193aaa3aab69d8c5505293a977da7e98",
        raw_fetched_at="2026-10-27T14:15:39.369458+00:00",
    ),
    StandingsRow(
        league="SS-8",
        season="2324",
        group="__total__",
        team_id="6577",
        team="Las Palmas",
        batch_id="48e2361c-9010-4016-a458-89aae4e87891",
        ingested_at="2026-07-30 08:13:57.405187",
        raw_content_hash="5e66636bb475ca3faf95465510e718f601186dacac9c303cc809498df7f85603",
        raw_fetched_at="2026-07-30T08:11:31.049746+00:00",
    ),
)

# The keeper of every pair, in the same order as STALE_ROWS.
KEEPER_ROWS = (
    StandingsRow(
        league="SS-203",
        season="2526",
        group="__total__",
        team_id="201654",
        team="FC Nizhny Novgorod",
        batch_id="b6433114-be80-46a5-a4fb-53ccdc5cb98c",
        ingested_at="2026-08-21 16:51:12.808994",
        raw_content_hash="254d08ab382f1105c8a8ade2c3e19d0a5023acb502af511bb46c840286196ed4",
        raw_fetched_at="2026-08-21T16:48:16.267059+00:00",
    ),
    StandingsRow(
        league="SS-203",
        season="2627",
        group="__total__",
        team_id="2321",
        team="Zenit St. Petersburg",
        batch_id="cd28a895-094f-45f7-be58-5140bcbd9210",
        ingested_at="2026-11-22 16:13:56.794094",
        raw_content_hash="398bb3625c50517142e40be1753bfde087553090d384a7c65658687ae3100313",
        raw_fetched_at="2026-10-26T00:32:14.398756+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1643",
        team="LOSC Lille",
        batch_id="06541b77-3b00-4632-86d5-57e1febf3707",
        ingested_at="2026-11-10 00:27:50.866925",
        raw_content_hash="90928f22db12c788faffbb73ef0c9d2fbc1e2e3f4953a3ff938e358b38a5c9d8",
        raw_fetched_at="2026-11-10T00:26:22.076165+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1646",
        team="AJ Auxerre",
        batch_id="06541b77-3b00-4632-86d5-57e1febf3707",
        ingested_at="2026-11-10 00:27:50.866925",
        raw_content_hash="90928f22db12c788faffbb73ef0c9d2fbc1e2e3f4953a3ff938e358b38a5c9d8",
        raw_fetched_at="2026-11-10T00:26:22.076165+00:00",
    ),
    StandingsRow(
        league="SS-34",
        season="2627",
        group="__total__",
        team_id="1661",
        team="OGC Nice",
        batch_id="06541b77-3b00-4632-86d5-57e1febf3707",
        ingested_at="2026-11-10 00:27:50.866925",
        raw_content_hash="90928f22db12c788faffbb73ef0c9d2fbc1e2e3f4953a3ff938e358b38a5c9d8",
        raw_fetched_at="2026-11-10T00:26:22.076165+00:00",
    ),
    StandingsRow(
        league="SS-52",
        season="2627",
        group="__total__",
        team_id="207011",
        team="Amedspor",
        batch_id="b24c6ca6-714a-4d14-83ef-85d4a7cf10d0",
        ingested_at="2026-11-09 14:53:29.484083",
        raw_content_hash="7cab892f39a19f43d20aa0cfae65d5d9b0e97281b50d07406476586782523809",
        raw_fetched_at="2026-11-09T14:51:46.073583+00:00",
    ),
    StandingsRow(
        league="SS-8",
        season="2324",
        group="__total__",
        team_id="6577",
        team="UD Las Palmas",
        batch_id="aab971fb-5333-454a-8799-28b0e2178ab3",
        ingested_at="2026-07-31 08:07:03.570454",
        raw_content_hash="47d0bfbd3e972eee4b0890fc015c3af5420cb3875b3e7c6ae5e61e214d135a1b",
        raw_fetched_at="2026-07-31T08:04:42.242602+00:00",
    ),
)


class PurgeRefused(RuntimeError):
    """The table is not in the reviewed state; nothing was sent."""


class PostDeleteVerificationError(RuntimeError):
    """The DELETE was sent or may have been sent; inspect before any rollback."""

    def __init__(
        self,
        message: str,
        *,
        conditional_rollback_instruction: str | None = None,
    ) -> None:
        super().__init__(message)
        self.conditional_rollback_instruction = conditional_rollback_instruction


@dataclass(frozen=True)
class SnapshotMetadata:
    snapshot_id: int
    parent_id: int | None
    committed_at: str
    operation: str
    summary: Mapping[str, str]


@dataclass(frozen=True)
class SnapshotNode:
    snapshot_id: int
    parent_id: int | None
    operation: str


@dataclass(frozen=True)
class DeleteExecution:
    query_id_after_execute: str | None
    query_id_after_fetch: str | None
    rowcount: int


@dataclass(frozen=True)
class TableState:
    """Rows found under every pinned name and key, plus the duplicate census."""

    stale: Mapping[StandingsRow, Sequence[Sequence[object]]]
    keepers: Mapping[StandingsRow, Sequence[Sequence[object]]]
    key_counts: Mapping[StandingsRow, int]
    duplicate_keys: frozenset[tuple[str, str, str, str]]


@dataclass(frozen=True)
class PurgeResult:
    applied: bool
    deleted_rows: int
    already_clean: bool
    pre_snapshot_id: int | None = None
    purge_snapshot_id: int | None = None
    delete_query_id: str | None = None
    delete_rowcount: int | None = None
    conditional_rollback_instruction: str | None = None


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _execute(cursor, sql: str) -> list[Sequence[object]]:
    compact = " ".join(sql.split())
    logger.info("SQL: %s%s", compact[:180], "..." if len(compact) > 180 else "")
    cursor.execute(sql)
    return list(cursor.fetchall())


def _partition_predicate(row: StandingsRow, *, with_name: bool) -> str:
    name = f" AND team = {_sql_literal(row.team)}" if with_name else ""
    return (
        f"league = {_sql_literal(row.league)} AND season = {_sql_literal(row.season)} "
        f'AND "group" = {_sql_literal(row.group)} AND team_id = {_sql_literal(row.team_id)}'
        f"{name}"
    )


def _identity_predicate(row: StandingsRow) -> str:
    return (
        f"({_partition_predicate(row, with_name=True)} "
        f"AND _batch_id = {_sql_literal(row.batch_id)} "
        f"AND _ingested_at = TIMESTAMP {_sql_literal(row.ingested_at)} "
        f"AND raw_content_hash = {_sql_literal(row.raw_content_hash)})"
    )


def _parse_instant(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PurgeRefused(f"{label} raw_fetched_at is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def load_named_rows(cursor, row: StandingsRow) -> list[Sequence[object]]:
    sql = (
        f"SELECT {', '.join(ROW_QUERY_COLUMNS)} FROM {TABLE} "
        f"WHERE {_partition_predicate(row, with_name=True)}"
    )
    return _execute(cursor, sql)


def load_key_count(cursor, row: StandingsRow) -> int:
    """Count every row under the pinned key, whatever name the team carries."""

    rows = _execute(
        cursor,
        f"SELECT COUNT(*) FROM {TABLE} "
        f"WHERE {_partition_predicate(row, with_name=False)}",
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise PurgeRefused("key row-count query returned an invalid shape")
    return int(rows[0][0])


def load_duplicate_keys(cursor) -> frozenset[tuple[str, str, str, str]]:
    """Census of the contract key with its name fallback (endpoint_coverage.yaml)."""

    rows = _execute(
        cursor,
        f"""
SELECT league, season, "group", COALESCE(team_id, team) AS team_key, COUNT(*)
FROM {TABLE}
GROUP BY league, season, "group", COALESCE(team_id, team)
HAVING COUNT(*) > 1
""".strip(),
    )
    return frozenset((str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows)


def load_partition_counts(cursor) -> dict[tuple[str, str], int]:
    rows = _execute(
        cursor,
        f"SELECT league, season, COUNT(*) FROM {TABLE} GROUP BY league, season",
    )
    return {(str(r[0]), str(r[1])): int(r[2]) for r in rows}


def count_table_rows(cursor) -> int:
    rows = _execute(cursor, f"SELECT COUNT(*) FROM {TABLE}")
    if len(rows) != 1 or len(rows[0]) != 1:
        raise PurgeRefused("table row-count query returned an invalid shape")
    return int(rows[0][0])


def load_current_snapshot(cursor) -> SnapshotMetadata:
    """Resolve the actual current ``main`` ref and its direct parent."""

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
        f"'bronze', 'sofascore_league_table', {int(snapshot_id)})"
    )


def load_owned_snapshots(cursor, query_id: str) -> dict[int, SnapshotNode]:
    """Every snapshot that one Trino query committed to this table."""

    if not QID.fullmatch(query_id):
        raise PurgeRefused(f"not a Trino query id: {query_id!r}")
    rows = _execute(
        cursor,
        f"""
SELECT snapshot_id, parent_id, operation
FROM {SNAPSHOTS_TABLE}
WHERE element_at(summary, 'trino_query_id') = {_sql_literal(query_id)}
""".strip(),
    )
    owned: dict[int, SnapshotNode] = {}
    for snapshot_id, parent_id, operation in rows:
        if snapshot_id is None or operation is None:
            raise PurgeRefused("owned snapshot row is incomplete")
        owned[int(snapshot_id)] = SnapshotNode(
            snapshot_id=int(snapshot_id),
            parent_id=None if parent_id is None else int(parent_id),
            operation=str(operation),
        )
    return owned


def conditional_rollback_instruction(
    *, pre_snapshot_id: int, purge_snapshot_id: int
) -> str:
    return (
        "QUIESCE ALL WRITERS; only if iceberg.bronze."
        '"sofascore_league_table$refs" main snapshot_id still equals '
        f"{int(purge_snapshot_id)}, run: {rollback_call(pre_snapshot_id)}"
    )


def load_table_state(cursor) -> TableState:
    stale = {row: load_named_rows(cursor, row) for row in STALE_ROWS}
    keepers = {keeper: load_named_rows(cursor, keeper) for keeper in KEEPER_ROWS}
    key_counts = {row: load_key_count(cursor, row) for row in STALE_ROWS}
    return TableState(
        stale=stale,
        keepers=keepers,
        key_counts=key_counts,
        duplicate_keys=load_duplicate_keys(cursor),
    )


def _require_pinned_row(rows: Sequence[Sequence[object]], expected: StandingsRow, label: str) -> None:
    if len(rows) != 1:
        raise PurgeRefused(
            f"{label} {expected.stable_id()}: expected exactly one row, "
            f"found {len(rows)}"
        )
    if len(rows[0]) < len(IDENTITY_COLUMNS):
        raise PurgeRefused(
            f"{label} {expected.stable_id()}: row has {len(rows[0])} columns, "
            f"expected at least {len(IDENTITY_COLUMNS)}"
        )
    actual = {
        column: None if value is None else str(value)
        for column, value in zip(IDENTITY_COLUMNS, rows[0])
    }
    for column in IDENTITY_COLUMNS:
        if actual[column] != getattr(expected, column):
            raise PurgeRefused(
                f"{label} {expected.stable_id()}: column {column} is "
                f"{actual[column]!r}, pinned {getattr(expected, column)!r}"
            )


def validate_table_state(state: TableState) -> str:
    """Return ``"dirty"`` when the exact reviewed set can be deleted, else ``"clean"``."""

    if len(KEEPER_ROWS) != len(STALE_ROWS):
        raise PurgeRefused("pinned keeper set is not aligned with the stale set")
    expected_keys = frozenset(row.natural_key for row in STALE_ROWS)
    present = [row for row, rows in state.stale.items() if rows]
    if not present:
        if state.duplicate_keys:
            raise PurgeRefused(
                "pinned stale rows are gone but duplicate keys remain: "
                f"{sorted(state.duplicate_keys)}"
            )
        for row, count in state.key_counts.items():
            if count != 1:
                raise PurgeRefused(
                    f"pinned stale rows are gone but key {row.natural_key} holds "
                    f"{count} rows instead of the single keeper"
                )
        return "clean"
    if len(present) != len(STALE_ROWS):
        missing = sorted(set(STALE_ROWS) - set(present), key=lambda row: row.stable_id())
        raise PurgeRefused(
            "partially applied state: pinned stale rows are missing: "
            f"{[row.stable_id() for row in missing]}"
        )
    for stale, count in state.key_counts.items():
        if count != 2:
            raise PurgeRefused(
                f"key {stale.natural_key} holds {count} rows instead of exactly "
                "the stale row and its keeper"
            )
    for stale, keeper, stale_rows, keeper_rows in zip(
        STALE_ROWS, KEEPER_ROWS, state.stale.values(), state.keepers.values()
    ):
        _require_pinned_row(stale_rows, stale, "stale")
        _require_pinned_row(keeper_rows, keeper, "keeper")
        if _parse_instant(keeper.raw_fetched_at, label="keeper") <= _parse_instant(
            stale.raw_fetched_at, label="stale"
        ):
            raise PurgeRefused(
                f"keeper {keeper.stable_id()} is not newer than the stale row"
            )
    if state.duplicate_keys != expected_keys:
        raise PurgeRefused(
            "duplicate keys differ from the pinned set: "
            f"unexpected={sorted(state.duplicate_keys - expected_keys)}, "
            f"missing={sorted(expected_keys - state.duplicate_keys)}"
        )
    return "dirty"


def validate_delete_lineage(
    *,
    pre_snapshot_id: int,
    current: SnapshotMetadata,
    owned: Mapping[int, SnapshotNode],
) -> int:
    """Prove that the snapshot chain down to ``pre`` is only this DELETE.

    Trino commits a row-level DELETE as the delete snapshot itself, or as the
    rewritten files of the same query appended before or after it. Every
    snapshot between ``pre`` and the current one must carry the query id of
    this cursor, exactly one of them must be the removal, and no other
    operation may appear. Returns the number of owned snapshots in the chain.
    """

    ownership_error: str | None = None
    operations: list[str] = []
    node_id = current.snapshot_id
    while node_id != pre_snapshot_id and ownership_error is None:
        node = owned.get(node_id)
        if node is None:
            ownership_error = (
                f"snapshot {node_id} between pre-delete {pre_snapshot_id} and current "
                f"{current.snapshot_id} was not committed by this DELETE"
            )
        elif node.parent_id is None or len(operations) >= 8:
            ownership_error = (
                f"owned snapshot chain does not reach pre-delete {pre_snapshot_id}: "
                f"stopped at {node_id}"
            )
        else:
            operations.append(node.operation)
            node_id = node.parent_id
    if ownership_error is None and not operations:
        ownership_error = (
            "current snapshot is still the pre-delete one: no snapshot was committed "
            f"({pre_snapshot_id})"
        )
    elif ownership_error is None and sum(op in REMOVAL_OPERATIONS for op in operations) != 1:
        ownership_error = (
            "owned snapshot chain must contain exactly one removal, got "
            f"{operations}"
        )
    elif ownership_error is None and any(
        op not in REMOVAL_OPERATIONS and op != "append" for op in operations
    ):
        ownership_error = f"owned snapshot chain has an unexpected operation: {operations}"

    if ownership_error is not None:
        raise PostDeleteVerificationError(
            "DELETE outcome is unknown and ownership is unproven; do not run "
            "a rollback automatically: " + ownership_error
        )
    return len(operations)


def validate_delete_ownership(
    *,
    pre_snapshot: SnapshotMetadata,
    purge_snapshot: SnapshotMetadata,
    execution: DeleteExecution,
    owned: Mapping[int, SnapshotNode],
) -> int:
    """Prove that the snapshots committed since ``pre`` belong to this exact DELETE."""

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
    elif execution.rowcount != len(STALE_ROWS):
        ownership_error = (
            "Trino DELETE update count is not exactly "
            f"{len(STALE_ROWS)} (rowcount={execution.rowcount})"
        )
    elif purge_snapshot.summary.get("trino_query_id") != query_id:
        ownership_error = (
            "current snapshot was not committed by this cursor "
            f"(cursor={query_id!r}, "
            f"snapshot={purge_snapshot.summary.get('trino_query_id')!r})"
        )
    elif not owned:
        ownership_error = "no snapshot carries the query id of this cursor"

    if ownership_error is not None:
        raise PostDeleteVerificationError(
            "DELETE outcome is unknown and ownership is unproven; do not run "
            "a rollback automatically: " + ownership_error
        )
    return validate_delete_lineage(
        pre_snapshot_id=pre_snapshot.snapshot_id,
        current=purge_snapshot,
        owned=owned,
    )


def render_delete_sql() -> str:
    arms = "\n   OR ".join(_identity_predicate(row) for row in STALE_ROWS)
    return f"DELETE FROM {TABLE}\nWHERE {arms}"


def delete_stale_rows(cursor) -> DeleteExecution:
    """Send the single DELETE and capture DB-API ownership metadata.

    ``trino-python-client`` exposes the query id after ``execute`` and
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


def expected_partition_counts_after(
    before: Mapping[tuple[str, str], int]
) -> dict[tuple[str, str], int]:
    after = dict(before)
    for stale in STALE_ROWS:
        partition = (stale.league, stale.season)
        after[partition] = after.get(partition, 0) - 1
    return after


def purge(cursor, *, apply: bool = False) -> PurgeResult:
    snapshot_guard = load_current_snapshot(cursor) if apply else None
    status = validate_table_state(load_table_state(cursor))

    if status == "clean":
        if snapshot_guard is not None:
            clean_end_snapshot = load_current_snapshot(cursor)
            if clean_end_snapshot.snapshot_id != snapshot_guard.snapshot_id:
                raise PurgeRefused(
                    "Iceberg main snapshot changed during idempotent validation; "
                    "retry in a quiet window"
                )
        logger.info(
            "Standings table is already clean: no pinned stale row and no duplicate key"
        )
        return PurgeResult(applied=apply, deleted_rows=0, already_clean=True)

    logger.info(
        "Preflight passed for the exact %d-row stale set; every keeper is the "
        "newer source answer",
        len(STALE_ROWS),
    )
    if not apply:
        logger.info(
            "DRY-RUN: would delete exactly %d rows; pass --apply to write",
            len(STALE_ROWS),
        )
        return PurgeResult(applied=False, deleted_rows=0, already_clean=False)

    total_before = count_table_rows(cursor)
    partitions_before = load_partition_counts(cursor)
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
        f"rowcount={len(STALE_ROWS)} are proven"
    )

    try:
        delete_execution = delete_stale_rows(cursor)
        purge_snapshot = load_current_snapshot(cursor)
    except Exception as exc:
        raise PostDeleteVerificationError(
            "DELETE was sent or may have been sent, but its first snapshot could "
            "not be owned; outcome is unknown and no rollback instruction is safe"
        ) from exc
    delete_query_id = execution_query_id = delete_execution.query_id_after_fetch
    try:
        owned_snapshots = (
            load_owned_snapshots(cursor, execution_query_id)
            if execution_query_id
            else {}
        )
    except Exception as exc:
        # The DELETE was already sent, so nothing below may report a refusal.
        raise PostDeleteVerificationError(
            "DELETE was sent or may have been sent, but its snapshots could not "
            "be read; outcome is unknown and no rollback instruction is safe"
        ) from exc
    owned_chain = validate_delete_ownership(
        pre_snapshot=pre_snapshot,
        purge_snapshot=purge_snapshot,
        execution=delete_execution,
        owned=owned_snapshots,
    )
    rollback_instruction = conditional_rollback_instruction(
        pre_snapshot_id=pre_snapshot.snapshot_id,
        purge_snapshot_id=purge_snapshot.snapshot_id,
    )
    logger.warning(
        "PURGE SNAPSHOT OWNED: id=%s parent=%s operation=%s query_id=%s "
        "rowcount=%s owned_chain=%s",
        purge_snapshot.snapshot_id,
        purge_snapshot.parent_id,
        purge_snapshot.operation,
        delete_query_id,
        delete_execution.rowcount,
        owned_chain,
    )
    logger.warning("CONDITIONAL ROLLBACK: %s", rollback_instruction)

    try:
        remaining_duplicates = load_duplicate_keys(cursor)
        if remaining_duplicates:
            raise PostDeleteVerificationError(
                f"{len(remaining_duplicates)} duplicate natural keys remain after DELETE",
                conditional_rollback_instruction=rollback_instruction,
            )

        total_after = count_table_rows(cursor)
        if total_before - total_after != len(STALE_ROWS):
            raise PostDeleteVerificationError(
                "table row count did not decrease by exactly "
                f"{len(STALE_ROWS)}: before={total_before}, after={total_after}",
                conditional_rollback_instruction=rollback_instruction,
            )

        expected_partitions = expected_partition_counts_after(partitions_before)
        partitions_after = load_partition_counts(cursor)
        if partitions_after != expected_partitions:
            changed = sorted(
                partition
                for partition in set(partitions_after) | set(partitions_before)
                if partitions_after.get(partition) != expected_partitions.get(partition)
            )
            raise PostDeleteVerificationError(
                "per-partition row counts changed outside the pinned stale rows: "
                f"{changed}",
                conditional_rollback_instruction=rollback_instruction,
            )

        state_after = load_table_state(cursor)
        if any(state_after.stale.values()):
            raise PostDeleteVerificationError(
                "pinned stale rows remain after DELETE",
                conditional_rollback_instruction=rollback_instruction,
            )
        for keeper, rows in state_after.keepers.items():
            _require_pinned_row(rows, keeper, "keeper")

        final_snapshot = load_current_snapshot(cursor)
        if final_snapshot.snapshot_id != purge_snapshot.snapshot_id:
            raise PostDeleteVerificationError(
                "Iceberg main advanced during final verification; a concurrent "
                "snapshot exists, so do not run an unconditional rollback "
                f"(purge={purge_snapshot.snapshot_id}, "
                f"current={final_snapshot.snapshot_id})"
            )
        validate_delete_lineage(
            pre_snapshot_id=pre_snapshot.snapshot_id,
            current=final_snapshot,
            owned=owned_snapshots,
        )
    except PostDeleteVerificationError:
        raise
    except Exception as exc:
        raise PostDeleteVerificationError(
            str(exc), conditional_rollback_instruction=rollback_instruction
        ) from exc

    logger.info(
        "Deleted exactly %d stale rows; keepers and every other partition are "
        "unchanged",
        len(STALE_ROWS),
    )
    return PurgeResult(
        applied=True,
        deleted_rows=len(STALE_ROWS),
        already_clean=False,
        pre_snapshot_id=pre_snapshot.snapshot_id,
        purge_snapshot_id=purge_snapshot.snapshot_id,
        delete_query_id=delete_query_id,
        delete_rowcount=delete_execution.rowcount,
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
        help="execute the exact-identity DELETE (default: read-only dry-run)",
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
