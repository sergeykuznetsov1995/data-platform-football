#!/usr/bin/env python3
"""Quiescent, reversible WhoScored Bronze V2 migration.

The script is intentionally dry-run by default.  ``--apply`` builds and
validates partitioned shadow tables, swaps them into place, creates the V2
manifest/current views, and seeds legacy commits.  Source tables are retained
under a timestamped suffix for rollback; no source page is refetched.

Operational prerequisite: ``dag_ingest_whoscored`` must be paused and no
manual WhoScored process may be writing to Iceberg.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402
from scrapers.whoscored.repository import (  # noqa: E402
    MATCH_MANIFEST_TABLE,
    PREVIEW_MANIFEST_TABLE,
    PROFILE_MANIFEST_TABLE,
    PROFILE_VERSIONS_TABLE,
    WhoScoredRepository,
)


CATALOG = "iceberg"
SCHEMA = "bronze"
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUFFIX_RE = re.compile(r"^[a-zA-Z0-9_]+$")

EVENT_KEY = (
    "league", "season", "game_id", "source_event_id", "period", "minute", "second",
    "expanded_minute", "type", "outcome_type", "team_id", "player_id",
    "x", "y", "end_x", "end_y", "qualifiers", "related_event_id",
    "related_player_id", "team",
)

TABLE_KEYS: Mapping[str, Sequence[str]] = {
    "whoscored_events": EVENT_KEY,
    "whoscored_lineups": ("league", "season", "game_id", "player_id"),
    "whoscored_schedule": ("league", "season", "game_id"),
    "whoscored_missing_players": (
        "league", "season", "game_id", "team", "player_id", "reason", "status"
    ),
    "whoscored_season_stages": ("league", "season", "stage_id"),
    "whoscored_player_profile": ("league", "season", "player_id"),
}

TABLE_REQUIRED_KEYS: Mapping[str, Sequence[str]] = {
    "whoscored_events": ("league", "season", "game_id"),
    "whoscored_lineups": ("league", "season", "game_id", "player_id"),
    "whoscored_schedule": ("league", "season", "game_id"),
    "whoscored_missing_players": ("league", "season", "game_id", "player_id"),
    "whoscored_season_stages": ("league", "season", "stage_id"),
    "whoscored_player_profile": ("league", "season", "player_id"),
}

ID_CASTS: Mapping[str, Mapping[str, str]] = {
    "whoscored_events": {
        "game_id": "BIGINT",
        "team_id": "BIGINT",
        "player_id": "BIGINT",
        "related_event_id": "BIGINT",
        "related_player_id": "BIGINT",
        "source_event_id": "BIGINT",
    },
    "whoscored_lineups": {
        "game_id": "BIGINT",
        "team_id": "BIGINT",
        "player_id": "BIGINT",
    },
    "whoscored_schedule": {
        "game_id": "BIGINT",
        "home_team_id": "BIGINT",
        "away_team_id": "BIGINT",
        "stage_id": "BIGINT",
    },
    "whoscored_missing_players": {
        "game_id": "BIGINT",
        "player_id": "BIGINT",
    },
    "whoscored_season_stages": {
        "region_id": "BIGINT",
        "league_id": "BIGINT",
        "season_id": "BIGINT",
        "stage_id": "BIGINT",
    },
    "whoscored_player_profile": {
        "player_id": "BIGINT",
        "current_team_id": "BIGINT",
    },
}


def _name(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


def _suffix(value: str) -> str:
    """Validate a suffix appended after an already-safe identifier.

    Timestamp suffixes intentionally begin with a digit, which is safe in
    ``table_20260710`` but is not itself a standalone SQL identifier.
    """

    if not _SUFFIX_RE.fullmatch(value):
        raise ValueError(f"unsafe migration suffix: {value!r}")
    return value


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qualified(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{_name(table)}"


def _columns(trino: TrinoTableManager, table: str) -> list[str]:
    return list(trino.get_table_columns(SCHEMA, table))


def _scalar(trino: TrinoTableManager, sql: str) -> int:
    rows = trino.execute_query(sql)
    return int(rows[0][0]) if rows else 0


def _snapshot_id(trino: TrinoTableManager, table: str) -> int | None:
    rows = trino.execute_query(
        f'SELECT snapshot_id FROM {CATALOG}.{SCHEMA}."{_name(table)}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )
    return int(rows[0][0]) if rows else None


def capture_state(trino: TrinoTableManager) -> dict:
    state = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }
    for table in TABLE_KEYS:
        if not trino.table_exists(SCHEMA, table):
            continue
        state["tables"][table] = {
            "snapshot_id": _snapshot_id(trino, table),
            "rows": _scalar(trino, f"SELECT COUNT(*) FROM {_qualified(table)}"),
            "max_ingested_at": trino.execute_query(
                f"SELECT MAX(_ingested_at) FROM {_qualified(table)}"
            )[0][0],
        }
    return state


def _projection(table: str, columns: Sequence[str]) -> list[str]:
    casts = ID_CASTS.get(table, {})
    result = [
        (
            f'CAST("{column}" AS {casts[column]}) AS "{column}"'
            if column in casts
            else f'"{column}" AS "{column}"'
        )
        for column in columns
    ]
    present = set(columns)
    if table == "whoscored_events":
        additions = {
            "source_event_id": "CAST(NULL AS BIGINT) AS source_event_id",
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_game_batch_id": (
                "concat('legacy-', lower(to_hex(sha256(to_utf8(concat("
                '"league", \'|\', "season", \'|\', '
                'CAST(CAST("game_id" AS BIGINT) AS VARCHAR))))))) '
                "AS _game_batch_id"
            ),
        }
        result.extend(expression for name, expression in additions.items() if name not in present)
    elif table == "whoscored_lineups":
        additions = {
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_game_batch_id": (
                "concat('legacy-', lower(to_hex(sha256(to_utf8(concat("
                '"league", \'|\', "season", \'|\', '
                'CAST(CAST("game_id" AS BIGINT) AS VARCHAR))))))) '
                "AS _game_batch_id"
            ),
        }
        result.extend(expression for name, expression in additions.items() if name not in present)
    elif table == "whoscored_missing_players":
        additions = {
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_preview_batch_id": (
                "concat('legacy-preview-', lower(to_hex(sha256(to_utf8(concat("
                '"league", \'|\', "season", \'|\', '
                'CAST(CAST("game_id" AS BIGINT) AS VARCHAR))))))) '
                "AS _preview_batch_id"
            ),
        }
        result.extend(
            expression for name, expression in additions.items() if name not in present
        )
    return result


def _available_keys(
    table: str, columns: Sequence[str], requested: Sequence[str]
) -> list[str]:
    present = set(columns)
    required = set(TABLE_REQUIRED_KEYS[table]) | {"_ingested_at", "_batch_id"}
    missing = sorted(required - present)
    if missing:
        raise RuntimeError(
            f"{table} lacks required migration columns: {', '.join(missing)}"
        )
    keys = [column for column in requested if column in present]
    return keys


def build_shadow(
    trino: TrinoTableManager,
    table: str,
    *,
    suffix: str,
) -> tuple[str, int, int]:
    columns = _columns(trino, table)
    keys = _available_keys(table, columns, TABLE_KEYS[table])
    projection = _projection(table, columns)
    shadow = f"{table}_v2_{suffix}"
    partition_by = ", ".join(f'"{column}"' for column in keys)
    source_count = _scalar(trino, f"SELECT COUNT(*) FROM {_qualified(table)}")
    null_scopes = _scalar(
        trino,
        f"SELECT COUNT(*) FROM {_qualified(table)} "
        "WHERE league IS NULL OR season IS NULL",
    )
    if null_scopes:
        raise RuntimeError(
            f"{table} contains {null_scopes} rows without league/season; "
            "refusing to discard them"
        )
    scopes = trino.execute_query(
        f"SELECT DISTINCT league, season FROM {_qualified(table)} "
        "WHERE league IS NOT NULL AND season IS NOT NULL ORDER BY 1, 2"
    )
    expected_by_scope: list[tuple[str, str, int]] = []
    for league, season in scopes:
        where = (
            f"league = {_literal(str(league))} "
            f"AND season = {_literal(str(season))}"
        )
        expected = _scalar(
            trino,
            f"SELECT COUNT(*) FROM (SELECT {partition_by} "
            f"FROM {_qualified(table)} WHERE {where} "
            f"GROUP BY {partition_by}) AS source_groups",
        )
        expected_by_scope.append((str(league), str(season), expected))
    expected_count = sum(item[2] for item in expected_by_scope)

    if trino.table_exists(SCHEMA, shadow):
        shadow_columns = set(_columns(trino, shadow))
        expected_columns = set(columns)
        if table == "whoscored_events":
            expected_columns.update(
                {"source_event_id", "_payload_sha256", "_parser_version", "_game_batch_id"}
            )
        elif table == "whoscored_lineups":
            expected_columns.update(
                {"_payload_sha256", "_parser_version", "_game_batch_id"}
            )
        elif table == "whoscored_missing_players":
            expected_columns.update(
                {"_payload_sha256", "_parser_version", "_preview_batch_id"}
            )
        shadow_count = _scalar(trino, f"SELECT COUNT(*) FROM {_qualified(shadow)}")
        shadow_scopes = {
            (str(row[0]), str(row[1]))
            for row in trino.execute_query(
                f"SELECT DISTINCT league, season FROM {_qualified(shadow)}"
            )
        }
        duplicate_groups = 0
        scope_counts_match = shadow_scopes == {
            (league, season) for league, season, _ in expected_by_scope
        }
        if scope_counts_match:
            for league, season, expected in expected_by_scope:
                where = (
                    f"league = {_literal(league)} AND season = {_literal(season)}"
                )
                actual = _scalar(
                    trino,
                    f"SELECT COUNT(*) FROM {_qualified(shadow)} WHERE {where}",
                )
                duplicates = _scalar(
                    trino,
                    f"SELECT COUNT(*) FROM (SELECT {partition_by}, COUNT(*) n "
                    f"FROM {_qualified(shadow)} WHERE {where} "
                    f"GROUP BY {partition_by} HAVING COUNT(*) > 1) "
                    "AS duplicate_groups",
                )
                duplicate_groups += duplicates
                if actual != expected:
                    scope_counts_match = False
                    break
        if (
            shadow_columns == expected_columns
            and shadow_count == expected_count
            and scope_counts_match
            and duplicate_groups == 0
        ):
            return shadow, source_count, shadow_count
        # A crash can leave a partially populated table.  Only the exact
        # suffix-scoped shadow is disposable; source data remains untouched.
        trino._execute(f"DROP TABLE {_qualified(shadow)}")

    trino._execute(
        f"CREATE TABLE {_qualified(shadow)} "
        "WITH (partitioning = ARRAY['league', 'season']) AS "
        f"SELECT {', '.join(projection)} FROM {_qualified(table)} WHERE FALSE"
    )
    for league, season, expected in expected_by_scope:
        trino._execute(
            f"""
            INSERT INTO {_qualified(shadow)}
            SELECT {', '.join(projection)}
            FROM (
                SELECT source_rows.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY {partition_by}
                           ORDER BY _ingested_at DESC, _batch_id DESC
                       ) AS _migration_rank
                FROM {_qualified(table)} source_rows
                WHERE league = {_literal(str(league))}
                  AND season = {_literal(str(season))}
            )
            WHERE _migration_rank = 1
            """
        )
        where = f"league = {_literal(league)} AND season = {_literal(season)}"
        actual = _scalar(
            trino, f"SELECT COUNT(*) FROM {_qualified(shadow)} WHERE {where}"
        )
        duplicates = _scalar(
            trino,
            f"SELECT COUNT(*) FROM (SELECT {partition_by}, COUNT(*) n "
            f"FROM {_qualified(shadow)} WHERE {where} GROUP BY {partition_by} "
            "HAVING COUNT(*) > 1) AS duplicate_groups",
        )
        if actual != expected or duplicates:
            raise RuntimeError(
                f"{shadow} scope {league}/{season}: rows={actual}, "
                f"expected={expected}, duplicate_groups={duplicates}"
            )

    shadow_count = _scalar(trino, f"SELECT COUNT(*) FROM {_qualified(shadow)}")
    if shadow_count > source_count:
        raise RuntimeError(f"{shadow} grew from {source_count} to {shadow_count}")
    if shadow_count != expected_count:
        raise RuntimeError(
            f"{shadow} has {shadow_count} rows; expected {expected_count} natural-key groups"
        )
    return shadow, source_count, shadow_count


def swap_shadow(
    trino: TrinoTableManager, table: str, shadow: str, *, suffix: str
) -> str:
    backup = f"{table}_legacy_{suffix}"
    source_exists = trino.table_exists(SCHEMA, table)
    shadow_exists = trino.table_exists(SCHEMA, shadow)
    backup_exists = trino.table_exists(SCHEMA, backup)

    if backup_exists:
        if source_exists and not shadow_exists:
            # The previous attempt completed this table's swap.
            return backup
        if not source_exists and shadow_exists:
            # Process interruption between the two RENAME statements.
            trino._execute(f"ALTER TABLE {_qualified(shadow)} RENAME TO {_name(table)}")
            return backup
        raise RuntimeError(
            f"ambiguous swap state for {table}: source={source_exists}, "
            f"shadow={shadow_exists}, backup={backup_exists}"
        )
    if not source_exists or not shadow_exists:
        raise RuntimeError(
            f"cannot swap {table}: source={source_exists}, shadow={shadow_exists}"
        )
    trino._execute(f"ALTER TABLE {_qualified(table)} RENAME TO {_name(backup)}")
    try:
        trino._execute(f"ALTER TABLE {_qualified(shadow)} RENAME TO {_name(table)}")
    except BaseException:
        # Restore the original name even for KeyboardInterrupt/SystemExit.
        if not trino.table_exists(SCHEMA, table):
            trino._execute(
                f"ALTER TABLE {_qualified(backup)} RENAME TO {_name(table)}"
            )
        raise
    return backup


def seed_match_manifest(trino: TrinoTableManager) -> int:
    if not trino.table_exists(SCHEMA, "whoscored_events"):
        return 0
    required = ("whoscored_lineups", "whoscored_schedule", MATCH_MANIFEST_TABLE)
    missing = [name for name in required if not trino.table_exists(SCHEMA, name)]
    if missing:
        raise RuntimeError(
            "cannot seed match manifests; missing tables: " + ", ".join(missing)
        )
    manifest = _qualified(MATCH_MANIFEST_TABLE)
    trino._execute(
        f"""
        INSERT INTO {manifest} (
            league, season, game_id, game, kickoff, batch_id,
            payload_sha256, raw_uri, parser_version, state, is_final,
            is_opta, events_count, lineups_count, lineups_available,
            transport_mode, proxy_mode, http_status, failure_code, error,
            attempt_no, retry_after, fetched_at, completed_at,
            direct_bytes, paid_bytes, _source, _entity_type,
            _ingested_at, _batch_id
        )
        SELECT
            e.league, e.season, e.game_id, MAX(e.game), MAX(s.date),
            MAX(e._game_batch_id), NULL, NULL, 'legacy-v1', 'success',
            TRUE, COALESCE(BOOL_OR(s.match_is_opta), TRUE), COUNT(*),
            COALESCE(MAX(l.lineups_count), 0),
            COALESCE(MAX(l.lineups_count), 0) > 0,
            'legacy', 'unknown', NULL, NULL, NULL, 1, NULL,
            MAX(e._ingested_at), MAX(e._ingested_at), 0, 0,
            'whoscored', 'match_manifest',
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
            concat('legacy-seed-', lower(to_hex(sha256(to_utf8(concat(
                e.league, '|', e.season, '|', CAST(e.game_id AS VARCHAR)))))))
        FROM {_qualified('whoscored_events')} e
        LEFT JOIN (
            SELECT league, season, game_id, COUNT(*) AS lineups_count
            FROM {_qualified('whoscored_lineups')}
            GROUP BY 1, 2, 3
        ) l
          ON l.league = e.league
         AND l.season = e.season
         AND l.game_id = e.game_id
        LEFT JOIN (
            SELECT league, season, game_id,
                   MAX_BY(date, _ingested_at) AS date,
                   MAX_BY(match_is_opta, _ingested_at) AS match_is_opta
            FROM {_qualified('whoscored_schedule')}
            GROUP BY 1, 2, 3
        ) s
          ON s.league = e.league
         AND s.season = e.season
         AND s.game_id = e.game_id
        LEFT JOIN (
            SELECT league, season, game_id
            FROM {manifest}
            WHERE state = 'success'
            GROUP BY 1, 2, 3
        ) committed
          ON committed.league = e.league
         AND committed.season = e.season
         AND committed.game_id = e.game_id
        WHERE committed.game_id IS NULL
        GROUP BY e.league, e.season, e.game_id
        """
    )
    return _scalar(trino, f"SELECT COUNT(*) FROM {manifest} WHERE state = 'success'")


def backfill_preview_metadata(trino: TrinoTableManager) -> int:
    """Assign deterministic legacy preview metadata before strict-view seeding."""
    table = "whoscored_missing_players"
    if not trino.table_exists(SCHEMA, table):
        return 0
    required = {"league", "season", "game_id", "_preview_batch_id", "_parser_version"}
    missing = sorted(required - set(_columns(trino, table)))
    if missing:
        raise RuntimeError(
            f"cannot backfill preview metadata; {table} lacks: {', '.join(missing)}"
        )
    pending = _scalar(
        trino,
        f"SELECT COUNT(*) FROM {_qualified(table)} "
        "WHERE _preview_batch_id IS NULL OR _parser_version IS NULL",
    )
    if not pending:
        return 0
    trino._execute(
        f"""
        UPDATE {_qualified(table)}
        SET _preview_batch_id = COALESCE(
                _preview_batch_id,
                concat('legacy-preview-', lower(to_hex(sha256(to_utf8(concat(
                    league, '|', season, '|',
                    CAST(CAST(game_id AS BIGINT) AS VARCHAR)))))))
            ),
            _parser_version = COALESCE(_parser_version, 'legacy-v1')
        WHERE _preview_batch_id IS NULL OR _parser_version IS NULL
        """
    )
    remaining = _scalar(
        trino,
        f"SELECT COUNT(*) FROM {_qualified(table)} "
        "WHERE _preview_batch_id IS NULL OR _parser_version IS NULL",
    )
    if remaining:
        raise RuntimeError(
            f"preview metadata backfill left {remaining} incomplete rows"
        )
    return pending


def seed_preview_manifest(trino: TrinoTableManager) -> int:
    """Publish legacy non-empty preview batches created by the shadow rebuild.

    Legacy storage cannot prove that a game once had a successful zero-row
    preview, so only existing physical rows are seeded. Future zero snapshots
    are represented exactly by the append-only V2 manifest.
    """
    source = "whoscored_missing_players"
    if not trino.table_exists(SCHEMA, source):
        return 0
    required = ("whoscored_schedule", PREVIEW_MANIFEST_TABLE)
    missing = [name for name in required if not trino.table_exists(SCHEMA, name)]
    if missing:
        raise RuntimeError(
            "cannot seed preview manifests; missing tables: " + ", ".join(missing)
        )
    manifest = _qualified(PREVIEW_MANIFEST_TABLE)
    trino._execute(
        f"""
        INSERT INTO {manifest} (
            league, season, game_id, game, kickoff, batch_id,
            payload_sha256, raw_uri, parser_version, state,
            missing_players_count, transport_mode, proxy_mode, http_status,
            failure_code, error, attempt_no, retry_after, fetched_at,
            completed_at, direct_bytes, paid_bytes, _source, _entity_type,
            _ingested_at, _batch_id
        )
        SELECT
            p.league, p.season, CAST(p.game_id AS BIGINT), MAX(p.game),
            MAX(s.date), MAX(p._preview_batch_id), NULL, NULL, 'legacy-v1',
            'success', COUNT(*), 'legacy', 'unknown', NULL, NULL, NULL, 1,
            NULL, MAX(p._ingested_at), MAX(p._ingested_at), 0, 0,
            'whoscored', 'preview_manifest',
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
            concat('legacy-preview-seed-', lower(to_hex(sha256(to_utf8(concat(
                p.league, '|', p.season, '|',
                CAST(CAST(p.game_id AS BIGINT) AS VARCHAR)))))))
        FROM {_qualified(source)} p
        LEFT JOIN (
            SELECT league, season, game_id, MAX_BY(date, _ingested_at) AS date
            FROM {_qualified('whoscored_schedule')}
            GROUP BY 1, 2, 3
        ) s
          ON s.league = p.league
         AND s.season = p.season
         AND s.game_id = CAST(p.game_id AS BIGINT)
        LEFT JOIN (
            SELECT league, season, game_id
            FROM {manifest}
            WHERE state = 'success'
            GROUP BY 1, 2, 3
        ) committed
          ON committed.league = p.league
         AND committed.season = p.season
         AND committed.game_id = CAST(p.game_id AS BIGINT)
        WHERE p._preview_batch_id IS NOT NULL
          AND committed.game_id IS NULL
        GROUP BY p.league, p.season, CAST(p.game_id AS BIGINT)
        """
    )
    return _scalar(trino, f"SELECT COUNT(*) FROM {manifest} WHERE state = 'success'")


def seed_profiles(trino: TrinoTableManager) -> int:
    if not trino.table_exists(SCHEMA, "whoscored_player_profile"):
        return 0
    required = (PROFILE_VERSIONS_TABLE, PROFILE_MANIFEST_TABLE)
    missing_tables = [
        name for name in required if not trino.table_exists(SCHEMA, name)
    ]
    if missing_tables:
        raise RuntimeError(
            "cannot seed profiles; missing tables: " + ", ".join(missing_tables)
        )
    source_columns = set(_columns(trino, "whoscored_player_profile"))
    target_types = {
        "player_id": "BIGINT",
        "name": "VARCHAR",
        "current_team_id": "BIGINT",
        "current_team_name": "VARCHAR",
        "shirt_number": "INTEGER",
        "age": "INTEGER",
        "date_of_birth": "DATE",
        "height_cm": "INTEGER",
        "nationality": "VARCHAR",
        "country_code": "VARCHAR",
        "positions": "VARCHAR",
    }

    def value(column: str, expression: str | None = None) -> str:
        if column not in source_columns:
            return f"CAST(NULL AS {target_types[column]}) AS \"{column}\""
        source = expression if expression is not None else f'ranked."{column}"'
        return f'{source} AS "{column}"'

    profile_projection = [
        value("player_id", 'CAST(ranked."player_id" AS BIGINT)'),
        value("name"),
        value("current_team_id", 'CAST(ranked."current_team_id" AS BIGINT)'),
        value("current_team_name"),
        value("shirt_number", 'CAST(ranked."shirt_number" AS INTEGER)'),
        value("age", 'CAST(ranked."age" AS INTEGER)'),
        value("date_of_birth", 'TRY_CAST(ranked."date_of_birth" AS DATE)'),
        value("height_cm", 'CAST(ranked."height_cm" AS INTEGER)'),
        value("nationality"),
        value("country_code"),
        value("positions"),
    ]
    trino._execute(
        f"""
        INSERT INTO {_qualified(PROFILE_VERSIONS_TABLE)} (
            player_id, name, current_team_id, current_team_name,
            shirt_number, age, date_of_birth, height_cm, nationality,
            country_code, positions, payload_sha256, raw_uri,
            parser_version, fetched_at, _source, _entity_type,
            _ingested_at, _batch_id
        )
        SELECT {', '.join(profile_projection)}, NULL, NULL, 'legacy-v1',
               ranked._ingested_at, 'whoscored', 'player_profile',
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
               concat('legacy-profile-', CAST(ranked.player_id AS VARCHAR))
        FROM (
            SELECT p.*, ROW_NUMBER() OVER (
                PARTITION BY CAST(player_id AS BIGINT)
                ORDER BY _ingested_at DESC, _batch_id DESC
            ) rn
            FROM {_qualified('whoscored_player_profile')} p
            WHERE player_id IS NOT NULL
        ) ranked
        LEFT JOIN (
            SELECT player_id
            FROM {_qualified(PROFILE_VERSIONS_TABLE)}
            WHERE parser_version = 'legacy-v1'
            GROUP BY player_id
        ) existing
          ON existing.player_id = CAST(ranked.player_id AS BIGINT)
        WHERE ranked.rn = 1
          AND existing.player_id IS NULL
        """
    )
    trino._execute(
        f"""
        INSERT INTO {_qualified(PROFILE_MANIFEST_TABLE)} (
            player_id, payload_sha256, raw_uri, parser_version, state,
            http_status, failure_code, error, attempt_no, retry_after,
            transport_mode, proxy_mode, direct_bytes, paid_bytes,
            fetched_at, completed_at, _source, _entity_type,
            _ingested_at, _batch_id
        )
        SELECT v.player_id, v.payload_sha256, v.raw_uri, v.parser_version, 'success',
               200, NULL, NULL, 1, NULL, 'legacy', 'unknown', 0, 0,
               v.fetched_at, v.fetched_at, 'whoscored', 'profile_manifest',
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
               concat('legacy-profile-', CAST(v.player_id AS VARCHAR))
        FROM {_qualified(PROFILE_VERSIONS_TABLE)} v
        LEFT JOIN (
            SELECT player_id
            FROM {_qualified(PROFILE_MANIFEST_TABLE)}
            WHERE state = 'success'
            GROUP BY player_id
        ) committed ON committed.player_id = v.player_id
        WHERE v.parser_version = 'legacy-v1'
          AND committed.player_id IS NULL
        """
    )
    return _scalar(trino, f"SELECT COUNT(*) FROM {_qualified(PROFILE_VERSIONS_TABLE)}")


def rollback(trino: TrinoTableManager, suffix: str) -> list[str]:
    """Restore legacy physical tables and isolate V2 state for a clean retry."""

    suffix = _suffix(suffix)
    physical_actions: list[tuple[str, str, str, bool]] = []
    for table in reversed(list(TABLE_KEYS)):
        backup = f"{table}_legacy_{suffix}"
        if not trino.table_exists(SCHEMA, backup):
            continue
        failed = f"{table}_v2_failed_{suffix}"
        current_exists = trino.table_exists(SCHEMA, table)
        if current_exists and trino.table_exists(SCHEMA, failed):
            raise RuntimeError(
                f"cannot rollback {table}: forensic target already exists: {failed}"
            )
        physical_actions.append((table, backup, failed, current_exists))

    forensic_physical_exists = any(
        trino.table_exists(SCHEMA, f"{table}_v2_failed_{suffix}")
        for table in TABLE_KEYS
    )
    # A wrong or already-rolled-back suffix must be a complete no-op.  Never
    # isolate active V2 state without a matching backup or forensic table
    # proving that this migration run swapped a physical source table.
    if not physical_actions and not forensic_physical_exists:
        return []

    v2_actions: list[tuple[str, str]] = []
    for table in (
        MATCH_MANIFEST_TABLE,
        PREVIEW_MANIFEST_TABLE,
        PROFILE_VERSIONS_TABLE,
        PROFILE_MANIFEST_TABLE,
    ):
        failed = f"{table}_v2_failed_{suffix}"
        active_exists = trino.table_exists(SCHEMA, table)
        failed_exists = trino.table_exists(SCHEMA, failed)
        if active_exists and failed_exists:
            raise RuntimeError(
                f"cannot isolate {table}: forensic target already exists: {failed}"
            )
        if active_exists:
            v2_actions.append((table, failed))

    if not physical_actions and not v2_actions:
        return []

    for schema, view in (
        ("silver", "whoscored_player_profile_current"),
        (SCHEMA, "whoscored_player_roster"),
        (SCHEMA, "whoscored_missing_players_current"),
        (SCHEMA, "whoscored_preview_ingest_latest_success"),
        (SCHEMA, "whoscored_preview_ingest_latest"),
        (SCHEMA, "whoscored_events_current"),
        (SCHEMA, "whoscored_lineups_current"),
        (SCHEMA, "whoscored_match_ingest_latest_success"),
        (SCHEMA, "whoscored_match_ingest_latest"),
    ):
        trino._execute(f"DROP VIEW IF EXISTS {CATALOG}.{schema}.{_name(view)}")

    restored: list[str] = []
    for table, backup, failed, current_exists in physical_actions:
        if current_exists:
            trino._execute(
                f"ALTER TABLE {_qualified(table)} RENAME TO {_name(failed)}"
            )
        trino._execute(f"ALTER TABLE {_qualified(backup)} RENAME TO {_name(table)}")
        restored.append(table)

    for table, failed in v2_actions:
        trino._execute(f"ALTER TABLE {_qualified(table)} RENAME TO {_name(failed)}")

    return restored


def _failed_artifacts(trino: TrinoTableManager, suffix: str) -> list[str]:
    """Return forensic tables proving that this suffix was rolled back.

    Reusing such a suffix is unsafe: a second failed attempt could collide with
    the first attempt's ``*_v2_failed_*`` tables and make automatic rollback
    impossible.  Rollback itself remains idempotent; only a new apply is
    rejected.
    """

    suffix = _suffix(suffix)
    candidates = [
        *(f"{table}_v2_failed_{suffix}" for table in TABLE_KEYS),
        *(
            f"{table}_v2_failed_{suffix}"
            for table in (
                MATCH_MANIFEST_TABLE,
                PREVIEW_MANIFEST_TABLE,
                PROFILE_VERSIONS_TABLE,
                PROFILE_MANIFEST_TABLE,
            )
        ),
    ]
    return [table for table in candidates if trino.table_exists(SCHEMA, table)]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the migration")
    parser.add_argument("--rollback-suffix", help="restore legacy tables for this suffix")
    parser.add_argument(
        "--confirm-quiescent",
        action="store_true",
        help="confirm the DAG and all manual WhoScored writers are stopped",
    )
    parser.add_argument(
        "--suffix",
        default=datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
        help="identifier suffix used for shadow/backup tables",
    )
    parser.add_argument("--report", type=Path, help="optional JSON report path")
    return parser.parse_args(argv)


def _emit_report(report: Mapping, path: Path | None) -> None:
    rendered = json.dumps(report, default=str, indent=2, sort_keys=True)
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


def main(
    argv: Sequence[str] | None = None,
    *,
    trino: TrinoTableManager | None = None,
) -> int:
    args = parse_args(argv)
    suffix = _suffix(args.suffix)
    if args.apply and not args.confirm_quiescent:
        raise SystemExit("--apply requires --confirm-quiescent")
    trino = trino or TrinoTableManager(catalog=CATALOG)
    if args.rollback_suffix:
        if not args.apply:
            raise SystemExit("--rollback-suffix requires --apply")
        rollback_suffix = _suffix(args.rollback_suffix)
        restored = rollback(trino, rollback_suffix)
        _emit_report(
            {"mode": "rollback", "suffix": rollback_suffix, "restored": restored},
            args.report,
        )
        return 0

    if args.apply:
        failed_artifacts = _failed_artifacts(trino, suffix)
        if failed_artifacts:
            raise RuntimeError(
                f"migration suffix {suffix!r} has rollback artifacts: "
                f"{', '.join(failed_artifacts)}; choose a new --suffix"
            )

    before = capture_state(trino)
    report = {
        "mode": "apply" if args.apply else "dry-run",
        "suffix": suffix,
        "before": before,
        "tables": {},
    }
    if not args.apply:
        _emit_report(report, args.report)
        return 0
    shadows: dict[str, str] = {}
    swap_performed = False
    try:
        for table in TABLE_KEYS:
            shadow = f"{table}_v2_{suffix}"
            backup = f"{table}_legacy_{suffix}"
            source_exists = trino.table_exists(SCHEMA, table)
            shadow_exists = trino.table_exists(SCHEMA, shadow)
            backup_exists = trino.table_exists(SCHEMA, backup)
            if backup_exists:
                if source_exists and not shadow_exists:
                    # A prior process completed this swap.  Any later schema or
                    # seed failure must still roll all swapped tables back.
                    swap_performed = True
                    source_count = _scalar(
                        trino, f"SELECT COUNT(*) FROM {_qualified(backup)}"
                    )
                    shadow_count = _scalar(
                        trino, f"SELECT COUNT(*) FROM {_qualified(table)}"
                    )
                    report["tables"][table] = {
                        "source_rows": source_count,
                        "deduplicated_rows": shadow_count,
                        "removed_rows": source_count - shadow_count,
                        "backup": backup,
                        "status": "already_swapped",
                    }
                    continue
                if not source_exists and shadow_exists:
                    # A prior process stopped between the two RENAMEs.  The
                    # matching backup makes rollback possible even if resume
                    # fails before swap_shadow returns.
                    swap_performed = True
                    source_count = _scalar(
                        trino, f"SELECT COUNT(*) FROM {_qualified(backup)}"
                    )
                    shadow_count = _scalar(
                        trino, f"SELECT COUNT(*) FROM {_qualified(shadow)}"
                    )
                    shadows[table] = shadow
                    report["tables"][table] = {
                        "source_rows": source_count,
                        "deduplicated_rows": shadow_count,
                        "removed_rows": source_count - shadow_count,
                        "status": "resume_swap",
                    }
                    continue
                raise RuntimeError(
                    f"ambiguous migration state for {table}: source={source_exists}, "
                    f"shadow={shadow_exists}, backup={backup_exists}"
                )
            if not source_exists:
                if shadow_exists:
                    raise RuntimeError(f"orphan shadow without source: {shadow}")
                continue
            shadow, source_count, shadow_count = build_shadow(
                trino, table, suffix=suffix
            )
            shadows[table] = shadow
            report["tables"][table] = {
                "source_rows": source_count,
                "deduplicated_rows": shadow_count,
                "removed_rows": source_count - shadow_count,
                "status": "prepared",
            }

        for table, shadow in shadows.items():
            backup = swap_shadow(trino, table, shadow, suffix=suffix)
            swap_performed = True
            report["tables"][table].update(
                {"backup": backup, "status": "swapped"}
            )

        repository = WhoScoredRepository(trino=trino)
        # Strict current views must not become visible until every legacy row
        # has batch metadata and the corresponding manifests are committed.
        repository.ensure_schema(create_views=False)
        report["match_manifests"] = seed_match_manifest(trino)
        report["preview_metadata_backfilled"] = backfill_preview_metadata(trino)
        report["preview_manifests"] = seed_preview_manifest(trino)
        report["profile_versions"] = seed_profiles(trino)
        repository.ensure_schema()
        report["after"] = capture_state(trino)
        report["status"] = "success"
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        if swap_performed:
            try:
                report["auto_rollback"] = rollback(trino, suffix)
            except BaseException as rollback_exc:
                report["rollback_error"] = (
                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                )
        _emit_report(report, args.report)
        raise

    _emit_report(report, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
