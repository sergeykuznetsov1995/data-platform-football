#!/usr/bin/env python3
"""Safely remove validated WhoScored V2 migration artifacts.

The command is a dry-run unless ``--apply`` is supplied.  Cleanup is limited
to exact table names derived from a small, hard-coded migration suffix
allowlist.  Before any DROP, the active V2 manifests and current views must
agree on match, event, lineup, preview, and profile counts.

Example dry-run::

    python scripts/cleanup_whoscored_v2_migration.py --suffix 20260710v2

Applying additionally requires the suffix-bound confirmation token printed by
the dry-run report. Active V2 tables and views are never dropped; the sole
active-name exception is the retired pre-V2 ``whoscored_player_profile`` table,
and only the final successful migration suffix may remove it after all guards.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402


CATALOG = "iceberg"
BRONZE = "bronze"
SILVER = "silver"

# These are the only production migration runs whose artifacts this command
# is authorized to remove.  Expanding the list requires a reviewed code
# change; arbitrary CLI-provided suffixes are rejected.
ALLOWED_SUFFIXES = frozenset({"20260710v2", "20260710v2r2"})
CONFIRMATION_PREFIX = "drop-whoscored-migration-artifacts"

MIGRATED_TABLES = (
    "whoscored_events",
    "whoscored_lineups",
    "whoscored_schedule",
    "whoscored_missing_players",
    "whoscored_season_stages",
    "whoscored_player_profile",
)
V2_STATE_TABLES = (
    "whoscored_match_ingest_manifest",
    "whoscored_preview_ingest_manifest",
    "whoscored_player_profile_versions",
    "whoscored_profile_ingest_manifest",
)
DEPRECATED_ACTIVE_TABLE = "whoscored_player_profile"
REQUIRED_PHYSICAL_TABLES = frozenset(
    table for table in MIGRATED_TABLES if table != DEPRECATED_ACTIVE_TABLE
)
REQUIRED_BRONZE_OBJECTS = frozenset(
    {
        *REQUIRED_PHYSICAL_TABLES,
        *V2_STATE_TABLES,
        "whoscored_match_ingest_latest",
        "whoscored_match_ingest_latest_success",
        "whoscored_events_current",
        "whoscored_lineups_current",
        "whoscored_preview_ingest_latest",
        "whoscored_preview_ingest_latest_success",
        "whoscored_missing_players_current",
        "whoscored_player_roster",
    }
)
REQUIRED_SILVER_OBJECTS = frozenset({"whoscored_player_profile_current"})
AUTHORITATIVE_R2_BACKUPS = {
    "whoscored_schedule_legacy_20260710v2r2": (
        "whoscored_schedule",
        ("league", "season", "game_id"),
    ),
    "whoscored_season_stages_legacy_20260710v2r2": (
        "whoscored_season_stages",
        ("league", "season", "stage_id"),
    ),
}


def _allowed_suffix(value: str) -> str:
    if value not in ALLOWED_SUFFIXES:
        allowed = ", ".join(sorted(ALLOWED_SUFFIXES))
        raise ValueError(f"cleanup suffix {value!r} is not allowlisted ({allowed})")
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


def _qualified(schema: str, table: str) -> str:
    all_artifacts = {
        name for suffix in ALLOWED_SUFFIXES for name in artifact_names(suffix)
    }
    if schema == BRONZE:
        allowed = REQUIRED_BRONZE_OBJECTS | all_artifacts | {DEPRECATED_ACTIVE_TABLE}
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

    latest_success = _qualified(BRONZE, "whoscored_match_ingest_latest_success")
    events_physical = _qualified(BRONZE, "whoscored_events")
    lineups_physical = _qualified(BRONZE, "whoscored_lineups")
    events_current = _qualified(BRONZE, "whoscored_events_current")
    lineups_current = _qualified(BRONZE, "whoscored_lineups_current")
    previews_physical = _qualified(BRONZE, "whoscored_missing_players")
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
            (SELECT COUNT(*) FROM {events_physical}
             WHERE _game_batch_id IS NULL),
            (SELECT COUNT(*) FROM {lineups_physical}
             WHERE _game_batch_id IS NULL),
            (SELECT COUNT(*) FROM {previews_physical}
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
        "physical_events_without_batch",
        "physical_lineups_without_batch",
        "physical_previews_without_batch",
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
    if metrics["manifest_games"] != metrics["current_event_games"]:
        errors.append("manifest/current game counts differ")
    if metrics["manifest_event_rows"] != metrics["current_event_rows"]:
        errors.append("manifest/current event row counts differ")
    if metrics["manifest_lineup_rows"] != metrics["current_lineup_rows"]:
        errors.append("manifest/current lineup row counts differ")
    if metrics["physical_events_without_batch"]:
        errors.append("physical events still contain rows without a V2 batch id")
    if metrics["physical_lineups_without_batch"]:
        errors.append("physical lineups still contain rows without a V2 batch id")
    if metrics["physical_previews_without_batch"]:
        errors.append("physical previews still contain rows without a preview batch id")
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

    return {
        "passed": not errors,
        "missing_objects": [],
        "metrics": metrics,
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
    if suffix != "20260710v2r2":
        return {"passed": True, "checked": {}, "errors": []}

    discovered = set(artifacts)
    expected = set(AUTHORITATIVE_R2_BACKUPS)
    requested = discovered & expected
    present = {backup for backup in expected if trino.table_exists(BRONZE, backup)}
    checked: dict[str, dict[str, int]] = {}
    errors: list[str] = []

    # The two frozen r2 backups form one safety boundary.  A partial physical
    # set is ambiguous: the absent table may have been dropped without ever
    # proving its key parity.  Fail closed rather than using the remaining
    # backup as permission to delete it and the deprecated active table.  Once
    # both have been removed by a successful cleanup, the empty set is the
    # intentional idempotent state and no key query is possible or necessary.
    if present and present != expected:
        errors.append(
            "authoritative r2 backups are only partially present: "
            f"present={sorted(present)}, missing={sorted(expected - present)}"
        )

    # The destructive API must never validate/drop just one member of the
    # authoritative pair.  When both physical backups exist, callers must pass
    # both; an explicit one-name request is rejected even after both tables are
    # already absent so the direct API cannot encode an unsafe partial cleanup.
    if requested and requested != expected:
        errors.append(
            "authoritative r2 backups must be requested together: "
            f"requested={sorted(requested)}, missing={sorted(expected - requested)}"
        )
    if present == expected and requested != expected:
        errors.append(
            "both present authoritative r2 backups must be included in cleanup"
        )

    if errors or not present:
        return {"passed": not errors, "checked": checked, "errors": errors}

    for backup, (active, keys) in AUTHORITATIVE_R2_BACKUPS.items():
        string_keys = keys[:-1]
        id_key = keys[-1]
        projected_keys = ", ".join(
            (*string_keys, f"TRY_CAST({id_key} AS BIGINT) AS {id_key}")
        )
        null_checks = [f"{key} IS NULL" for key in string_keys]
        null_checks.append(f"TRY_CAST({id_key} AS BIGINT) IS NULL")
        null_predicate = " OR ".join(null_checks)
        active_table = _qualified(BRONZE, active)
        backup_table = _qualified(BRONZE, backup)
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
        checked[backup] = metrics
        if metrics["active_null_keys"] or metrics["backup_null_keys"]:
            errors.append(f"{backup}: active or backup contains null/invalid keys")
        if metrics["backup_only_keys"]:
            errors.append(f"{backup}: backup contains keys missing from active")

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
        trino._execute(f"DROP TABLE {_qualified(BRONZE, table)}")
        dropped.append(table)
    return dropped


def deprecated_active_names(trino: TrinoTableManager, suffix: str) -> list[str]:
    """Return the exact active legacy table eligible after the final run."""

    suffix = _allowed_suffix(suffix)
    if suffix != "20260710v2r2":
        return []
    if not trino.table_exists(BRONZE, DEPRECATED_ACTIVE_TABLE):
        return []
    return [DEPRECATED_ACTIVE_TABLE]


def drop_deprecated_active(
    trino: TrinoTableManager, suffix: str, tables: Sequence[str]
) -> list[str]:
    """Drop the retired pre-V2 profile table only for the successful r2 run."""

    suffix = _allowed_suffix(suffix)
    requested = list(tables)
    allowed = {DEPRECATED_ACTIVE_TABLE} if suffix == "20260710v2r2" else set()
    invalid = sorted(set(requested) - allowed)
    if invalid:
        raise ValueError(
            "refusing to drop non-deprecated active tables: " + ", ".join(invalid)
        )
    if len(requested) != len(set(requested)):
        raise ValueError("refusing duplicate deprecated active table names")

    # The direct API must preserve the same ordering as ``main``: verified r2
    # backups are removed as one pair before the deprecated active table.  This
    # also blocks a caller from bypassing a partial-backup failure by invoking
    # this helper directly.
    backup_guards = inspect_authoritative_backup_keys(trino, suffix, [])
    if not backup_guards["passed"]:
        raise RuntimeError(
            "authoritative backup guard failed: "
            + "; ".join(backup_guards["errors"])
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
        choices=sorted(ALLOWED_SUFFIXES),
        help="allowlisted migration run whose artifacts should be inspected",
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
