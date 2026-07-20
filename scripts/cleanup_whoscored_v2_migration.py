#!/usr/bin/env python3
"""Safely remove validated WhoScored V2 migration artifacts.

The command is a dry-run unless ``--apply`` is supplied. Cleanup is limited to
exact table names derived from a strict migration suffix. Before any DROP, the
complete 25-table object contract, active V2 manifests, and current views must
agree.

Example dry-run::

    python scripts/cleanup_whoscored_v2_migration.py --suffix 20260711run1

Applying additionally requires the suffix-bound confirmation token printed by
the dry-run report. Active V2 tables and views are never dropped. The retired
pre-V2 ``whoscored_player_profile`` and ``whoscored_season_stages`` names are
removed last, only after all guards and suffix-bound backup cleanup pass.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if getattr(
        _whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None
    ) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = (
    _whoscored_bootstrap_sys._load_whoscored_runtime_contract(_whoscored_root)
)
_WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
    operation="WhoScored V2 migration cleanup"
)

import argparse
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402
from scripts import whoscored_v2_object_contract as _contract  # noqa: E402

BATCH_COLUMN_BY_TABLE = _contract.BATCH_COLUMN_BY_TABLE
BUSINESS_REQUIRED_COLUMNS = _contract.BUSINESS_REQUIRED_COLUMNS
BUSINESS_TABLES = _contract.BUSINESS_TABLES
DEPRECATED_ACTIVE_TABLES = _contract.DEPRECATED_ACTIVE_TABLES
LEGACY_MIGRATION_KEYS = _contract.LEGACY_MIGRATION_KEYS
MANIFEST_REQUIRED_COLUMNS = _contract.MANIFEST_REQUIRED_COLUMNS
MATCH_TABLES = _contract.MATCH_TABLES
PREVIEW_TABLES = _contract.PREVIEW_TABLES
REQUIRED_BRONZE_OBJECTS = _contract.REQUIRED_BRONZE_OBJECTS
REQUIRED_SILVER_OBJECTS = _contract.REQUIRED_SILVER_OBJECTS
ROLLBACK_STATE_TABLES = _contract.ROLLBACK_STATE_TABLES
SCOPE_TABLES = _contract.SCOPE_TABLES


CATALOG = "iceberg"
BRONZE = "bronze"
SILVER = "silver"

CONFIRMATION_PREFIX = "drop-whoscored-migration-artifacts"
_SUFFIX_RE = re.compile(r"^[a-zA-Z0-9_]+$")

MIGRATED_TABLES = tuple(LEGACY_MIGRATION_KEYS)
V2_STATE_TABLES = ROLLBACK_STATE_TABLES

# Every frozen legacy backup is authoritative until its keys are represented
# by the manifest-gated current dataset.  V2 tables may contain more keys.
LEGACY_COVERAGE_TARGETS = {
    "whoscored_events": (
        BRONZE,
        "whoscored_events_current",
        ("league", "season", "game_id"),
        True,
    ),
    "whoscored_lineups": (
        BRONZE,
        "whoscored_lineups_current",
        ("league", "season", "game_id", "player_id"),
        False,
    ),
    "whoscored_schedule": (
        BRONZE,
        "whoscored_schedule_current",
        ("league", "season", "game_id"),
        False,
    ),
    "whoscored_missing_players": (
        BRONZE,
        "whoscored_missing_players_current",
        (
            "league",
            "season",
            "game_id",
            "team",
            "player_id",
            "reason",
            "status",
        ),
        False,
    ),
    "whoscored_season_stages": (
        BRONZE,
        "whoscored_stages_current",
        ("stage_id",),
        False,
    ),
    "whoscored_player_profile": (
        SILVER,
        "whoscored_player_profile_current",
        ("player_id",),
        False,
    ),
}


def _allowed_suffix(value: str) -> str:
    """Accept only the identifier suffix emitted by the migration script."""

    if not _SUFFIX_RE.fullmatch(value):
        raise ValueError(f"unsafe cleanup suffix: {value!r}")
    return value


def confirmation_token(suffix: str) -> str:
    return f"{CONFIRMATION_PREFIX}:{_allowed_suffix(suffix)}"


def artifact_names(suffix: str) -> tuple[str, ...]:
    """Return the complete exact-name DROP allowlist for one migration run."""

    suffix = _allowed_suffix(suffix)
    names: list[str] = []
    for table in MIGRATED_TABLES:
        names.extend(
            (
                f"{table}_legacy_{suffix}",
                f"{table}_v2_{suffix}",
                f"{table}_v2_failed_{suffix}",
            )
        )
    names.extend(f"{table}_v2_failed_{suffix}" for table in V2_STATE_TABLES)
    return tuple(names)


def _qualified(schema: str, table: str, *, artifact_suffix: str | None = None) -> str:
    if schema == BRONZE:
        allowed = REQUIRED_BRONZE_OBJECTS | set(DEPRECATED_ACTIVE_TABLES)
        if artifact_suffix is not None:
            allowed = allowed | set(artifact_names(artifact_suffix))
    elif schema == SILVER:
        allowed = REQUIRED_SILVER_OBJECTS
    else:
        raise ValueError(f"schema is outside the cleanup allowlist: {schema!r}")
    if table not in allowed:
        raise ValueError(
            f"object is outside the WhoScored cleanup allowlist: {schema}.{table}"
        )
    return f"{CATALOG}.{schema}.{table}"


def discover_artifacts(trino: TrinoTableManager, suffix: str) -> list[str]:
    """Find only allowlisted, exact-name Bronze artifacts for ``suffix``."""

    return [
        table for table in artifact_names(suffix) if trino.table_exists(BRONZE, table)
    ]


_LEGACY_BACKUP_RE = re.compile(
    "^(?:"
    + "|".join(re.escape(table) for table in MIGRATED_TABLES)
    + r")_legacy_[a-zA-Z0-9_]+$"
)


def discover_all_legacy_backups(trino: TrinoTableManager) -> list[str]:
    """Find exact WhoScored backups across suffixes before active-name DROP."""

    alternatives = "|".join(re.escape(table) for table in MIGRATED_TABLES)
    rows = trino.execute_query(
        "SELECT table_name FROM iceberg.information_schema.tables "
        "WHERE table_schema = 'bronze' "
        f"AND regexp_like(table_name, '^({alternatives})_legacy_[A-Za-z0-9_]+$')"
    )
    names = sorted({str(row[0]) for row in rows if row})
    invalid = [name for name in names if not _LEGACY_BACKUP_RE.fullmatch(name)]
    if invalid:
        raise RuntimeError(
            "information_schema returned invalid WhoScored backup names: "
            + ", ".join(invalid)
        )
    return names


def _scalar(trino: TrinoTableManager, sql: str) -> int:
    rows = trino.execute_query(sql)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError("validation query returned an unexpected shape")
    return int(rows[0][0] or 0)


def inspect_current_state(trino: TrinoTableManager) -> dict[str, Any]:
    """Validate that active current views are fully backed by V2 manifests."""

    missing = [
        f"{BRONZE}.{table}"
        for table in sorted(REQUIRED_BRONZE_OBJECTS)
        if not trino.table_exists(BRONZE, table)
    ]
    missing.extend(
        f"{SILVER}.{table}"
        for table in sorted(REQUIRED_SILVER_OBJECTS)
        if not trino.table_exists(SILVER, table)
    )
    if missing:
        return {
            "passed": False,
            "missing_objects": missing,
            "errors": ["required active objects are missing"],
        }

    missing_columns: dict[str, list[str]] = {}
    required_columns = {
        **BUSINESS_REQUIRED_COLUMNS,
        **MANIFEST_REQUIRED_COLUMNS,
    }
    for table, required in required_columns.items():
        columns = {
            str(column).lower() for column in trino.get_table_columns(BRONZE, table)
        }
        absent = sorted(set(required) - columns)
        if absent:
            missing_columns[table] = absent
    if missing_columns:
        return {
            "passed": False,
            "missing_objects": [],
            "missing_commit_columns": missing_columns,
            "errors": ["business/manifest commit columns are missing"],
        }

    latest_success = _qualified(BRONZE, "whoscored_match_ingest_latest_success")
    events_current = _qualified(BRONZE, "whoscored_events_current")
    lineups_current = _qualified(BRONZE, "whoscored_lineups_current")
    preview_latest_success = _qualified(
        BRONZE, "whoscored_preview_ingest_latest_success"
    )
    previews_current = _qualified(BRONZE, "whoscored_missing_players_current")
    profile_manifest = _qualified(BRONZE, "whoscored_profile_ingest_manifest")
    profile_current = _qualified(SILVER, "whoscored_player_profile_current")

    rows = trino.execute_query(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {latest_success}),
            (SELECT COUNT(*) FROM (
                SELECT league, season, game_id FROM {events_current}
                GROUP BY 1, 2, 3
            )),
            (SELECT COALESCE(SUM(events_count), 0) FROM {latest_success}),
            (SELECT COUNT(*) FROM {events_current}),
            (SELECT COALESCE(SUM(lineups_count), 0) FROM {latest_success}),
            (SELECT COUNT(*) FROM {lineups_current}),
            (SELECT COUNT(*) FROM {events_current}
             WHERE _game_batch_id IS NULL),
            (SELECT COUNT(*) FROM {lineups_current}
             WHERE _game_batch_id IS NULL),
            (SELECT COUNT(*) FROM {previews_current}
             WHERE _preview_batch_id IS NULL),
            (SELECT COUNT(*) FROM {preview_latest_success}),
            (SELECT COALESCE(SUM(missing_players_count), 0)
             FROM {preview_latest_success}),
            (SELECT COUNT(*) FROM {previews_current}),
            (SELECT COUNT(DISTINCT player_id) FROM {profile_manifest}
             WHERE state = 'success'),
            (SELECT COUNT(*) FROM {profile_current})
        """
    )
    if len(rows) != 1 or len(rows[0]) != 14:
        return {
            "passed": False,
            "missing_objects": [],
            "errors": ["current-state count query returned an unexpected shape"],
        }

    metric_names = (
        "manifest_games",
        "current_event_games",
        "manifest_event_rows",
        "current_event_rows",
        "manifest_lineup_rows",
        "current_lineup_rows",
        "visible_legacy_event_rows",
        "visible_legacy_lineup_rows",
        "visible_legacy_preview_rows",
        "preview_manifest_games",
        "preview_manifest_rows",
        "current_preview_rows",
        "manifest_profile_players",
        "current_profile_rows",
    )
    metrics = {name: int(value or 0) for name, value in zip(metric_names, rows[0])}

    event_mismatch = int(
        trino.execute_query(
            f"""
            SELECT COUNT(*) FROM (
                SELECT m.league, m.season, m.game_id
                FROM {latest_success} m
                LEFT JOIN (
                    SELECT league, season, game_id, COUNT(*) AS row_count
                    FROM {events_current}
                    GROUP BY 1, 2, 3
                ) e
                  ON e.league = m.league
                 AND e.season = m.season
                 AND e.game_id = m.game_id
                WHERE COALESCE(e.row_count, 0) <> m.events_count
            ) mismatches
            """
        )[0][0]
    )
    lineup_mismatch = int(
        trino.execute_query(
            f"""
            SELECT COUNT(*) FROM (
                SELECT m.league, m.season, m.game_id
                FROM {latest_success} m
                LEFT JOIN (
                    SELECT league, season, game_id, COUNT(*) AS row_count
                    FROM {lineups_current}
                    GROUP BY 1, 2, 3
                ) l
                  ON l.league = m.league
                 AND l.season = m.season
                 AND l.game_id = m.game_id
                WHERE COALESCE(l.row_count, 0) <> m.lineups_count
            ) mismatches
            """
        )[0][0]
    )
    preview_mismatch = int(
        trino.execute_query(
            f"""
            SELECT COUNT(*) FROM (
                SELECT m.league, m.season, m.game_id
                FROM {preview_latest_success} m
                LEFT JOIN (
                    SELECT league, season, game_id, COUNT(*) AS row_count
                    FROM {previews_current}
                    GROUP BY 1, 2, 3
                ) p
                  ON p.league = m.league
                 AND p.season = m.season
                 AND p.game_id = m.game_id
                WHERE COALESCE(p.row_count, 0) <> m.missing_players_count
            ) mismatches
            """
        )[0][0]
    )
    metrics["event_manifest_mismatches"] = event_mismatch
    metrics["lineup_manifest_mismatches"] = lineup_mismatch
    metrics["preview_manifest_mismatches"] = preview_mismatch

    errors: list[str] = []
    if metrics["manifest_games"] <= 0:
        errors.append("latest-success manifest has no games")
    if metrics["manifest_event_rows"] != metrics["current_event_rows"]:
        errors.append("manifest/current event row counts differ")
    if metrics["manifest_lineup_rows"] != metrics["current_lineup_rows"]:
        errors.append("manifest/current lineup row counts differ")
    if metrics["visible_legacy_event_rows"]:
        errors.append("events_current still exposes legacy fallback rows")
    if metrics["visible_legacy_lineup_rows"]:
        errors.append("lineups_current still exposes legacy fallback rows")
    if metrics["visible_legacy_preview_rows"]:
        errors.append("missing_players_current still exposes legacy fallback rows")
    if metrics["event_manifest_mismatches"]:
        errors.append("per-game event counts differ from the manifest")
    if metrics["lineup_manifest_mismatches"]:
        errors.append("per-game lineup counts differ from the manifest")
    if metrics["preview_manifest_games"] <= 0:
        errors.append("latest-success preview manifest has no games")
    if metrics["preview_manifest_rows"] != metrics["current_preview_rows"]:
        errors.append("manifest/current preview row counts differ")
    if metrics["preview_manifest_mismatches"]:
        errors.append("per-game preview counts differ from the manifest")
    if metrics["manifest_profile_players"] <= 0:
        errors.append("successful profile manifest has no players")
    if metrics["manifest_profile_players"] != metrics["current_profile_rows"]:
        errors.append("manifest/current profile counts differ")

    # Every manifest-owned business view must be free of the transitional
    # NULL-batch bridge before migration backups can be removed.
    dataset_metrics: dict[str, dict[str, int]] = {}
    for table in (*SCOPE_TABLES, *MATCH_TABLES, *PREVIEW_TABLES):
        current = _qualified(BRONZE, f"{table}_current")
        batch_column = BATCH_COLUMN_BY_TABLE[table]
        visible_legacy = _scalar(
            trino,
            f"SELECT COUNT(*) FROM {current} WHERE {batch_column} IS NULL",
        )
        dataset_metrics[table] = {"visible_legacy_rows": visible_legacy}
        if visible_legacy:
            errors.append(f"{table}_current still exposes legacy fallback rows")

    # Validate every match/preview/scope dataset against the count committed in
    # its latest-success manifest, including legitimate zero-row datasets.
    latest_by_group = {
        "scope": _qualified(BRONZE, "whoscored_scope_ingest_latest_success"),
        "match": latest_success,
        "preview": preview_latest_success,
    }
    groups = (
        ("scope", SCOPE_TABLES, lambda table: table),
        ("match", MATCH_TABLES, lambda table: table.removeprefix("whoscored_")),
        ("preview", PREVIEW_TABLES, lambda table: table.removeprefix("whoscored_")),
    )
    for group, tables, json_key in groups:
        manifest_view = latest_by_group[group]
        identity = (
            ("league", "season")
            if group == "scope"
            else ("league", "season", "game_id")
        )
        joins = " AND ".join(f"d.{key} = m.{key}" for key in identity)
        for table in tables:
            current = _qualified(BRONZE, f"{table}_current")
            key = json_key(table)
            expected = (
                "COALESCE(TRY_CAST(json_extract_scalar("
                f"m.entity_counts_json, '$.{key}') AS BIGINT), 0)"
            )
            group_by = ", ".join(identity)
            mismatch = _scalar(
                trino,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {", ".join(f"m.{key}" for key in identity)}
                    FROM {manifest_view} m
                    LEFT JOIN (
                        SELECT {group_by}, COUNT(*) AS row_count
                        FROM {current}
                        GROUP BY {", ".join(str(index) for index in range(1, len(identity) + 1))}
                    ) d ON {joins}
                    WHERE COALESCE(d.row_count, 0) <> {expected}
                ) mismatches
                """,
            )
            dataset_metrics[table]["manifest_mismatches"] = mismatch
            if mismatch:
                errors.append(f"{table}_current counts differ from its manifest")

    catalog_latest = _qualified(BRONZE, "whoscored_catalog_latest_success")
    catalog_counts = trino.execute_query(
        f"""
        SELECT
            m.competitions_count,
            (SELECT COUNT(*) FROM {_qualified(BRONZE, "whoscored_competitions_current")}),
            m.seasons_count,
            (SELECT COUNT(*) FROM {_qualified(BRONZE, "whoscored_seasons_current")}),
            m.stages_count,
            (SELECT COUNT(*) FROM {_qualified(BRONZE, "whoscored_stages_current")}),
            m.quarantined_count
        FROM {catalog_latest} m
        """
    )
    if len(catalog_counts) != 1 or len(catalog_counts[0]) != 7:
        errors.append("catalog latest-success validation returned an unexpected shape")
    else:
        catalog_metrics = tuple(int(value or 0) for value in catalog_counts[0])
        metrics["catalog_quarantined"] = catalog_metrics[6]
        for table, expected_count, actual_count in (
            ("whoscored_competitions", catalog_metrics[0], catalog_metrics[1]),
            ("whoscored_seasons", catalog_metrics[2], catalog_metrics[3]),
            ("whoscored_stages", catalog_metrics[4], catalog_metrics[5]),
        ):
            dataset_metrics[table] = {
                "manifest_rows": expected_count,
                "current_rows": actual_count,
                "manifest_mismatches": int(expected_count != actual_count),
            }
        if catalog_metrics[0] != catalog_metrics[1]:
            errors.append("catalog competition count differs from current")
        if catalog_metrics[2] != catalog_metrics[3]:
            errors.append("catalog season count differs from current")
        if catalog_metrics[4] != catalog_metrics[5]:
            errors.append("catalog stage count differs from current")
        if catalog_metrics[6]:
            errors.append("catalog contains quarantined discoveries")

    invalid_profiles = _scalar(
        trino,
        f"SELECT COUNT(*) FROM {profile_current} "
        "WHERE raw_uri IS NULL OR parser_version = 'legacy-v1'",
    )
    metrics["visible_legacy_profiles"] = invalid_profiles
    dataset_metrics["whoscored_player_profile_versions"] = {
        "manifest_rows": metrics["manifest_profile_players"],
        "current_rows": metrics["current_profile_rows"],
        "visible_legacy_rows": invalid_profiles,
        "manifest_mismatches": int(
            metrics["manifest_profile_players"] != metrics["current_profile_rows"]
        ),
    }
    if invalid_profiles:
        errors.append("profile_current still exposes legacy/raw-less profiles")

    participations_current = _qualified(
        BRONZE, "whoscored_player_stage_participations_current"
    )
    participations = trino.execute_query(
        f"""
        WITH latest AS (
            SELECT * FROM (
                SELECT m.*, ROW_NUMBER() OVER (
                    PARTITION BY player_id
                    ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                             _profile_batch_id DESC, _batch_id DESC
                ) AS rn
                FROM {profile_manifest} m
                WHERE m.state = 'success'
                  AND m.raw_uri IS NOT NULL
                  AND m._profile_batch_id LIKE 'wspr2-%'
            ) WHERE rn = 1
        ), actual AS (
            SELECT player_id, COUNT(*) AS row_count
            FROM {participations_current}
            GROUP BY player_id
        )
        SELECT
            COALESCE(SUM(latest.participations_count), 0),
            (SELECT COUNT(*) FROM {participations_current}),
            COUNT_IF(
                COALESCE(actual.row_count, 0) <>
                COALESCE(latest.participations_count, 0)
            )
        FROM latest
        LEFT JOIN actual ON actual.player_id = latest.player_id
        """
    )
    if len(participations) != 1 or len(participations[0]) != 3:
        errors.append("profile participation validation returned an unexpected shape")
    else:
        expected_rows, current_rows, mismatches = (
            int(value or 0) for value in participations[0]
        )
        dataset_metrics["whoscored_player_stage_participations"] = {
            "manifest_rows": expected_rows,
            "current_rows": current_rows,
            "manifest_mismatches": mismatches,
        }
        if expected_rows != current_rows or mismatches:
            errors.append("profile participation counts differ from the manifest")

    return {
        "passed": not errors,
        "business_table_count": len(BUSINESS_TABLES),
        "missing_objects": [],
        "metrics": metrics,
        "datasets": dataset_metrics,
        "errors": errors,
    }


def inspect_authoritative_backup_keys(
    trino: TrinoTableManager,
    suffix: str,
    artifacts: Sequence[str],
) -> dict[str, Any]:
    """Require every frozen backup key to remain present in active V2 data.

    Active tables are allowed to grow after migration.  Requiring bidirectional
    equality would make a safe cleanup impossible as soon as the first normal
    ingest discovers a new match or stage.  ``active_only_keys`` remains in the
    report for audit; only ``backup_only_keys`` represents possible migration
    loss and blocks deletion.
    """

    suffix = _allowed_suffix(suffix)
    expected = {
        f"{source}_legacy_{suffix}": (source, *target)
        for source, target in LEGACY_COVERAGE_TARGETS.items()
    }
    discovered = set(artifacts)
    requested = discovered & set(expected)
    present = {backup for backup in expected if trino.table_exists(BRONZE, backup)}
    checked: dict[str, dict[str, int]] = {}
    errors: list[str] = []

    # The direct API must validate every backup that is still present.  This
    # prevents callers from deleting one unverified table, while still making
    # a process interruption between exact DROP statements resumable.
    if requested != present:
        errors.append(
            "all present authoritative backups must be requested together: "
            f"requested={sorted(requested)}, present={sorted(present)}"
        )

    if errors or not present:
        return {"passed": not errors, "checked": checked, "errors": errors}

    for backup in sorted(present):
        _source, target_schema, active, keys, compare_row_counts = expected[backup]
        projected = [
            (f"TRY_CAST({key} AS BIGINT) AS {key}" if key.endswith("_id") else key)
            for key in keys
        ]
        projected_keys = ", ".join(projected)
        null_checks = [
            (
                f"TRY_CAST({key} AS BIGINT) IS NULL"
                if key.endswith("_id")
                else f"{key} IS NULL"
            )
            for key in keys
        ]
        null_predicate = " OR ".join(null_checks)
        active_table = _qualified(target_schema, active)
        backup_table = _qualified(BRONZE, backup, artifact_suffix=suffix)
        rows = trino.execute_query(
            f"""
            SELECT
                (SELECT COUNT(*) FROM {active_table}
                 WHERE {null_predicate}),
                (SELECT COUNT(*) FROM {backup_table}
                 WHERE {null_predicate}),
                (SELECT COUNT(*) FROM (
                    SELECT DISTINCT {projected_keys} FROM {active_table}
                    EXCEPT
                    SELECT DISTINCT {projected_keys} FROM {backup_table}
                ) active_only),
                (SELECT COUNT(*) FROM (
                    SELECT DISTINCT {projected_keys} FROM {backup_table}
                    EXCEPT
                    SELECT DISTINCT {projected_keys} FROM {active_table}
                ) backup_only)
            """
        )
        if len(rows) != 1 or len(rows[0]) != 4:
            errors.append(f"{backup}: key-set query returned an unexpected shape")
            continue
        names = (
            "active_null_keys",
            "backup_null_keys",
            "active_only_keys",
            "backup_only_keys",
        )
        metrics = {name: int(value or 0) for name, value in zip(names, rows[0])}
        if compare_row_counts:
            group_by = ", ".join(str(index) for index in range(1, len(keys) + 1))
            joins = " AND ".join(f"a.{key} = b.{key}" for key in keys)
            metrics["row_count_shortfalls"] = _scalar(
                trino,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {projected_keys}, COUNT(*) AS row_count
                    FROM {backup_table}
                    GROUP BY {group_by}
                ) b
                LEFT JOIN (
                    SELECT {projected_keys}, COUNT(*) AS row_count
                    FROM {active_table}
                    GROUP BY {group_by}
                ) a ON {joins}
                WHERE COALESCE(a.row_count, 0) < b.row_count
                """,
            )
        checked[backup] = metrics
        if metrics["active_null_keys"] or metrics["backup_null_keys"]:
            errors.append(f"{backup}: active or backup contains null/invalid keys")
        if metrics["backup_only_keys"]:
            errors.append(f"{backup}: backup contains keys missing from active")
        if metrics.get("row_count_shortfalls"):
            errors.append(f"{backup}: current rows are fewer than legacy rows")

    return {"passed": not errors, "checked": checked, "errors": errors}


def drop_artifacts(
    trino: TrinoTableManager, suffix: str, artifacts: Sequence[str]
) -> list[str]:
    """Drop a preflighted subset of the exact suffix-bound allowlist."""

    allowed = set(artifact_names(suffix))
    requested = list(artifacts)
    invalid = sorted(set(requested) - allowed)
    if invalid:
        raise ValueError("refusing to drop non-artifact tables: " + ", ".join(invalid))
    if len(requested) != len(set(requested)):
        raise ValueError("refusing duplicate artifact names")

    backup_guards = inspect_authoritative_backup_keys(trino, suffix, requested)
    if not backup_guards["passed"]:
        raise RuntimeError(
            "authoritative backup guard failed: " + "; ".join(backup_guards["errors"])
        )

    dropped: list[str] = []
    for table in requested:
        # Re-check immediately before each DDL so a stale dry-run report cannot
        # turn a missing/renamed object into a broad or ambiguous operation.
        if not trino.table_exists(BRONZE, table):
            continue
        trino._execute(
            f"DROP TABLE {_qualified(BRONZE, table, artifact_suffix=suffix)}"
        )
        dropped.append(table)
    return dropped


def deprecated_active_names(trino: TrinoTableManager, suffix: str) -> list[str]:
    """Return deprecated pre-V2 active names eligible after this cutover."""

    _allowed_suffix(suffix)
    return [
        table for table in DEPRECATED_ACTIVE_TABLES if trino.table_exists(BRONZE, table)
    ]


def drop_deprecated_active(
    trino: TrinoTableManager, suffix: str, tables: Sequence[str]
) -> list[str]:
    """Drop retired active legacy tables only after their backups are gone."""

    suffix = _allowed_suffix(suffix)
    requested = list(tables)
    allowed = set(DEPRECATED_ACTIVE_TABLES)
    invalid = sorted(set(requested) - allowed)
    if invalid:
        raise ValueError(
            "refusing to drop non-deprecated active tables: " + ", ".join(invalid)
        )
    if len(requested) != len(set(requested)):
        raise ValueError("refusing duplicate deprecated active table names")

    remaining_backups = discover_all_legacy_backups(trino)
    if remaining_backups:
        raise RuntimeError(
            "authoritative backups must be removed before deprecated active tables: "
            + ", ".join(remaining_backups)
        )

    dropped: list[str] = []
    for table in requested:
        if not trino.table_exists(BRONZE, table):
            continue
        trino._execute(f"DROP TABLE {_qualified(BRONZE, table)}")
        dropped.append(table)
    return dropped


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suffix",
        required=True,
        help="exact identifier suffix emitted by migrate_whoscored_v2.py",
    )
    parser.add_argument(
        "--apply", action="store_true", help="execute exact DROP TABLE statements"
    )
    parser.add_argument(
        "--confirm",
        help=(
            "suffix-bound confirmation token; required with --apply "
            f"({CONFIRMATION_PREFIX}:<suffix>)"
        ),
    )
    parser.add_argument("--report", type=Path, help="optional JSON report path")
    return parser.parse_args(argv)


def _emit_report(report: Mapping[str, Any], path: Path | None) -> None:
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


def main(
    argv: Sequence[str] | None = None,
    *,
    trino: TrinoTableManager | None = None,
) -> int:
    args = parse_args(argv)
    suffix = _allowed_suffix(args.suffix)
    expected_confirmation = confirmation_token(suffix)
    if args.apply and args.confirm != expected_confirmation:
        raise SystemExit("--apply requires --confirm " + expected_confirmation)

    trino = trino or TrinoTableManager(catalog=CATALOG)
    artifacts = discover_artifacts(trino, suffix)
    all_legacy_backups = discover_all_legacy_backups(trino)
    foreign_backups = sorted(set(all_legacy_backups) - set(artifacts))
    guards = inspect_current_state(trino)
    if guards["passed"]:
        backup_guards = inspect_authoritative_backup_keys(trino, suffix, artifacts)
        if not backup_guards["passed"]:
            guards["passed"] = False
            guards["errors"].extend(backup_guards["errors"])
    else:
        backup_guards = {
            "passed": False,
            "checked": {},
            "errors": ["skipped because current-state guards failed"],
        }
    guards["authoritative_backups"] = backup_guards
    guards["all_legacy_backups"] = all_legacy_backups
    if foreign_backups:
        guards["passed"] = False
        guards["errors"].append(
            "legacy backups from another migration suffix are present: "
            + ", ".join(foreign_backups)
        )
    deprecated_active = deprecated_active_names(trino, suffix)
    report: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry-run",
        "suffix": suffix,
        "confirmation_token": expected_confirmation,
        "guards": guards,
        "artifacts": artifacts,
        "dropped": [],
        "deprecated_active": deprecated_active,
        "dropped_deprecated_active": [],
    }
    if not guards["passed"]:
        report["status"] = "blocked"
        _emit_report(report, args.report)
        return 2

    if args.apply:
        report["dropped"] = drop_artifacts(trino, suffix, artifacts)
        report["dropped_deprecated_active"] = drop_deprecated_active(
            trino, suffix, deprecated_active
        )
        report["status"] = "success"
    else:
        report["status"] = "ready"
    _emit_report(report, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
