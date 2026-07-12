#!/usr/bin/env python3
"""Quiescent, reversible WhoScored Bronze V2 migration.

The script is intentionally dry-run by default.  ``--apply`` builds and
validates partitioned shadow tables, swaps them into place, creates the V2
manifest/current views, and imports only legacy player profiles. Match,
preview, and scope rows stay on the explicit null-batch compatibility bridge
until raw-backed V2 recapture. Source tables are retained under a timestamped
suffix for rollback; no source page is refetched.

Operational prerequisite: ``dag_ingest_whoscored`` must be paused and no
manual WhoScored process may be writing to Iceberg.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
while str(PROJECT_ROOT) in sys.path:
    sys.path.remove(str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

_CONTRACT_NAME = "_dpf_whoscored_v2_object_contract"
_CONTRACT_PATH = PROJECT_ROOT / "scripts" / "whoscored_v2_object_contract.py"
_contract = sys.modules.get(_CONTRACT_NAME)
if _contract is None:
    _contract_spec = importlib.util.spec_from_file_location(
        _CONTRACT_NAME, _CONTRACT_PATH
    )
    if _contract_spec is None or _contract_spec.loader is None:
        raise ImportError(f"cannot load WhoScored object contract: {_CONTRACT_PATH}")
    _contract = importlib.util.module_from_spec(_contract_spec)
    sys.modules[_CONTRACT_NAME] = _contract
    _contract_spec.loader.exec_module(_contract)

from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402
from scrapers.whoscored.repository import (  # noqa: E402
    PROFILE_MANIFEST_TABLE,
    PROFILE_VERSIONS_TABLE,
    WhoScoredRepository,
)

BATCH_COLUMN_BY_TABLE = _contract.BATCH_COLUMN_BY_TABLE
BRONZE_VIEWS = _contract.BRONZE_VIEWS
BUSINESS_REQUIRED_COLUMNS = _contract.BUSINESS_REQUIRED_COLUMNS
BUSINESS_TABLES = _contract.BUSINESS_TABLES
CATALOG_TABLES = _contract.CATALOG_TABLES
LEGACY_MIGRATION_KEYS = _contract.LEGACY_MIGRATION_KEYS
MANIFEST_REQUIRED_COLUMNS = _contract.MANIFEST_REQUIRED_COLUMNS
MANIFEST_TABLES = _contract.MANIFEST_TABLES
MATCH_TABLES = _contract.MATCH_TABLES
PREVIEW_TABLES = _contract.PREVIEW_TABLES
PROFILE_TABLES = _contract.PROFILE_TABLES
REQUIRED_BRONZE_OBJECTS = _contract.REQUIRED_BRONZE_OBJECTS
REQUIRED_SILVER_OBJECTS = _contract.REQUIRED_SILVER_OBJECTS
ROLLBACK_STATE_TABLES = _contract.ROLLBACK_STATE_TABLES
SILVER_VIEWS = _contract.SILVER_VIEWS


CATALOG = "iceberg"
SCHEMA = "bronze"
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUFFIX_RE = re.compile(r"^[a-zA-Z0-9_]+$")

EVENT_KEY = (
    "league",
    "season",
    "game_id",
    "source_event_id",
    "period",
    "minute",
    "second",
    "expanded_minute",
    "type",
    "outcome_type",
    "team_id",
    "player_id",
    "x",
    "y",
    "end_x",
    "end_y",
    "qualifiers",
    "related_event_id",
    "related_player_id",
    "team",
)
if EVENT_KEY != tuple(LEGACY_MIGRATION_KEYS["whoscored_events"]):
    raise RuntimeError("WhoScored event migration key drifted from object contract")
TABLE_KEYS: Mapping[str, Sequence[str]] = dict(LEGACY_MIGRATION_KEYS)

TABLE_REQUIRED_KEYS: Mapping[str, Sequence[str]] = {
    "whoscored_events": ("league", "season", "game_id"),
    "whoscored_lineups": ("league", "season", "game_id", "player_id"),
    "whoscored_schedule": ("league", "season", "game_id"),
    "whoscored_missing_players": ("league", "season", "game_id", "player_id"),
    "whoscored_season_stages": ("league", "season", "stage_id"),
    "whoscored_player_profile": ("league", "season", "player_id"),
}

_BATCH_PREFIX_BY_COLUMN = {
    "_catalog_batch_id": "wsc2-",
    "_scope_batch_id": "wss2-",
    "_game_batch_id": "ws2-",
    "_preview_batch_id": "wsp2-",
    "_profile_batch_id": "wspr2-",
}

ID_CASTS: Mapping[str, Mapping[str, str]] = {
    "whoscored_events": {
        "game_id": "BIGINT",
        "team_id": "BIGINT",
        "player_id": "BIGINT",
        "team_event_id": "BIGINT",
        "related_team_event_id": "BIGINT",
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
    """Capture every V2 physical/state table plus pre-V2 source tables.

    The old implementation reported only six legacy tables, which made a
    migration report look healthy even when catalog/scope manifests or one of
    the newly supported datasets was absent.
    """

    state = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }
    inventory = dict.fromkeys((*BUSINESS_TABLES, *MANIFEST_TABLES, *TABLE_KEYS))
    for table in inventory:
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


def inspect_object_contract(trino: TrinoTableManager) -> dict:
    """Validate the complete 25-table/manifest/current-view object contract."""

    missing_bronze = [
        name
        for name in sorted(REQUIRED_BRONZE_OBJECTS)
        if not trino.table_exists(SCHEMA, name)
    ]
    missing_silver = [
        name
        for name in sorted(REQUIRED_SILVER_OBJECTS)
        if not trino.table_exists("silver", name)
    ]
    missing_columns: dict[str, list[str]] = {}
    required_columns = {
        **BUSINESS_REQUIRED_COLUMNS,
        **MANIFEST_REQUIRED_COLUMNS,
    }
    for table, required in required_columns.items():
        if table in missing_bronze:
            continue
        columns = {
            str(column).lower() for column in trino.get_table_columns(SCHEMA, table)
        }
        absent = sorted(set(required) - columns)
        if absent:
            missing_columns[table] = absent

    errors: list[str] = []
    if missing_bronze or missing_silver:
        errors.append("required WhoScored V2 objects are missing")
    if missing_columns:
        errors.append("business/manifest commit columns are missing")
    return {
        "passed": not errors,
        "business_table_count": len(BUSINESS_TABLES),
        "bronze_view_count": len(BRONZE_VIEWS),
        "silver_view_count": len(SILVER_VIEWS),
        "missing_bronze_objects": missing_bronze,
        "missing_silver_objects": missing_silver,
        "missing_commit_columns": missing_columns,
        "errors": errors,
    }


def _batch_map_fingerprint(values: Mapping[str, int]) -> str:
    payload = json.dumps(
        sorted((str(batch_id), int(count)) for batch_id, count in values.items()),
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _batch_counts(
    trino: TrinoTableManager,
    table: str,
    *,
    batch_column: str,
    prefix: str,
    schema: str = SCHEMA,
) -> dict[str, int]:
    """Return exact logical-batch row counts for a physical table or view."""

    if not trino.table_exists(schema, table):
        return {}
    columns = {str(column).lower() for column in trino.get_table_columns(schema, table)}
    if batch_column.lower() not in columns:
        return {}
    rows = trino.execute_query(
        f"SELECT {batch_column}, COUNT(*) FROM {CATALOG}.{schema}.{_name(table)} "
        f"WHERE {batch_column} LIKE '{prefix}%' "
        f"GROUP BY {batch_column} ORDER BY {batch_column}"
    )
    result: dict[str, int] = {}
    for row in rows:
        batch_id = str(row[0])
        if batch_id in result:
            raise RuntimeError(f"duplicate grouped batch result for {table}/{batch_id}")
        result[batch_id] = int(row[1])
    return result


def _count_map_metrics(expected: Mapping[str, int], actual: Mapping[str, int]) -> dict:
    batches = sorted(set(expected) | set(actual))
    mismatches = [
        (batch_id, int(expected.get(batch_id, 0)), int(actual.get(batch_id, 0)))
        for batch_id in batches
        if int(expected.get(batch_id, 0)) != int(actual.get(batch_id, 0))
    ]
    return {
        "expected_rows": sum(int(value) for value in expected.values()),
        "actual_rows": sum(int(value) for value in actual.values()),
        "expected_batches": len(expected),
        "actual_batches": len(actual),
        "expected_fingerprint": _batch_map_fingerprint(expected),
        "actual_fingerprint": _batch_map_fingerprint(actual),
        "mismatch_count": len(mismatches),
        "mismatch_samples": [
            {"batch_id": batch_id, "expected": expected_count, "actual": actual_count}
            for batch_id, expected_count, actual_count in mismatches[:10]
        ],
    }


def _parse_entity_counts(
    *,
    group: str,
    batch_id: str,
    payload: object,
    expected_keys: Sequence[str],
) -> dict[str, int]:
    try:
        decoded = json.loads(str(payload))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{group} manifest {batch_id} has invalid entity_counts_json"
        ) from exc
    if not isinstance(decoded, dict):
        raise RuntimeError(
            f"{group} manifest {batch_id} entity_counts_json is not an object"
        )
    missing = sorted(set(expected_keys) - set(decoded))
    if missing:
        raise RuntimeError(
            f"{group} manifest {batch_id} lacks dataset counts: {', '.join(missing)}"
        )
    result: dict[str, int] = {}
    for key in expected_keys:
        value = decoded[key]
        if isinstance(value, bool):
            raise RuntimeError(
                f"{group} manifest {batch_id}/{key} has boolean row count"
            )
        try:
            count = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"{group} manifest {batch_id}/{key} has invalid row count"
            ) from exc
        if count < 0 or str(value).strip() not in {str(count), f"{count}.0"}:
            raise RuntimeError(
                f"{group} manifest {batch_id}/{key} has invalid row count {value!r}"
            )
        result[key] = count
    return result


def _inspect_json_commit_group(
    trino: TrinoTableManager,
    *,
    group: str,
    manifest_table: str,
    latest_view: str,
    tables: Sequence[str],
    prefix: str,
    manifest_batch_column: str,
    physical_batch_column: str,
    fallback_count_column: str | None = None,
    fallback_dataset_key: str | None = None,
) -> dict:
    """Validate one JSON-count manifest against append-only and current data."""

    errors: list[str] = []
    dataset_keys = tuple(table.removeprefix("whoscored_") for table in tables)
    manifest_rows_count = 0
    manifest_rows: list[Sequence[object]] = []
    manifest_columns: set[str] = set()
    if trino.table_exists(SCHEMA, manifest_table):
        manifest_columns = {
            str(column).lower()
            for column in trino.get_table_columns(SCHEMA, manifest_table)
        }
        marker_columns = {manifest_batch_column, "state"}
        if marker_columns <= manifest_columns:
            manifest_rows_count = _scalar(
                trino,
                f"SELECT COUNT(*) FROM {_qualified(manifest_table)} "
                f"WHERE state = 'success' AND {manifest_batch_column} LIKE '{prefix}%'",
            )
        required = {
            manifest_batch_column,
            "state",
            "raw_uri",
        }
        count_column_available = "entity_counts_json" in manifest_columns or (
            fallback_count_column is not None
            and fallback_count_column in manifest_columns
        )
        if manifest_rows_count and (
            not required <= manifest_columns or not count_column_available
        ):
            missing = set(required) - manifest_columns
            if not count_column_available:
                missing.add(
                    "entity_counts_json"
                    + (
                        f" or {fallback_count_column}"
                        if fallback_count_column is not None
                        else ""
                    )
                )
            errors.append(
                f"{manifest_table} lacks V2 integrity columns: "
                + ", ".join(sorted(missing))
            )
        elif required <= manifest_columns and count_column_available:
            entity_expression = (
                "entity_counts_json"
                if "entity_counts_json" in manifest_columns
                else "CAST(NULL AS VARCHAR)"
            )
            fallback_expression = (
                fallback_count_column
                if fallback_count_column is not None
                and fallback_count_column in manifest_columns
                else "CAST(NULL AS BIGINT)"
            )
            manifest_rows = trino.execute_query(
                f"SELECT {manifest_batch_column}, raw_uri, {entity_expression}, "
                f"{fallback_expression} "
                f"FROM {_qualified(manifest_table)} "
                f"WHERE state = 'success' AND {manifest_batch_column} LIKE '{prefix}%' "
                f"GROUP BY {manifest_batch_column}, raw_uri, {entity_expression}, "
                f"{fallback_expression} "
                f"ORDER BY {manifest_batch_column}"
            )

    expected: dict[str, dict[str, int]] = {key: {} for key in dataset_keys}
    semantic_manifests: dict[str, tuple[str, dict[str, int]]] = {}
    for row in manifest_rows:
        batch_id = str(row[0])
        raw_uri = None if row[1] is None else str(row[1])
        if not raw_uri:
            errors.append(f"{group} manifest {batch_id} is not raw-backed")
            continue
        if row[2] is not None:
            try:
                counts = _parse_entity_counts(
                    group=group,
                    batch_id=batch_id,
                    payload=row[2],
                    expected_keys=dataset_keys,
                )
            except RuntimeError as exc:
                errors.append(str(exc))
                continue
        elif fallback_count_column is not None and fallback_dataset_key is not None:
            try:
                fallback_count = int(row[3])
            except (TypeError, ValueError):
                errors.append(
                    f"{group} manifest {batch_id} has invalid {fallback_count_column}"
                )
                continue
            if fallback_count < 0:
                errors.append(
                    f"{group} manifest {batch_id} has negative {fallback_count_column}"
                )
                continue
            counts = dict.fromkeys(dataset_keys, 0)
            counts[fallback_dataset_key] = fallback_count
        else:
            errors.append(f"{group} manifest {batch_id} has null entity_counts_json")
            continue
        semantic = (raw_uri, counts)
        previous = semantic_manifests.get(batch_id)
        if previous is not None and previous != semantic:
            errors.append(f"{group} batch {batch_id} has conflicting manifests")
            continue
        semantic_manifests[batch_id] = semantic
        for key, count in counts.items():
            expected[key][batch_id] = count

    datasets: dict[str, dict] = {}
    physical_detected = False
    for table, key in zip(tables, dataset_keys):
        actual = _batch_counts(
            trino,
            table,
            batch_column=physical_batch_column,
            prefix=prefix,
        )
        physical_detected = physical_detected or bool(actual)
        metrics = _count_map_metrics(expected[key], actual)
        datasets[table] = {"physical": metrics}
        if metrics["mismatch_count"]:
            errors.append(f"{table} physical V2 counts differ from {manifest_table}")

    detected = bool(manifest_rows_count or semantic_manifests or physical_detected)
    if detected and not trino.table_exists(SCHEMA, manifest_table):
        errors.append(f"{manifest_table} is missing for V2 physical rows")
    if manifest_rows_count != 0 and not semantic_manifests and not errors:
        errors.append(f"{manifest_table} V2 rows could not be inspected")

    latest_expected: dict[str, dict[str, int]] = {key: {} for key in dataset_keys}
    if detected:
        if not trino.table_exists(SCHEMA, latest_view):
            errors.append(f"{latest_view} is missing for existing V2 commits")
        else:
            latest_columns = {
                str(column).lower()
                for column in trino.get_table_columns(SCHEMA, latest_view)
            }
            required = {
                manifest_batch_column,
                "raw_uri",
            }
            count_column_available = "entity_counts_json" in latest_columns or (
                fallback_count_column is not None
                and fallback_count_column in latest_columns
            )
            if not required <= latest_columns or not count_column_available:
                missing = set(required) - latest_columns
                if not count_column_available:
                    missing.add(
                        "entity_counts_json"
                        + (
                            f" or {fallback_count_column}"
                            if fallback_count_column is not None
                            else ""
                        )
                    )
                errors.append(
                    f"{latest_view} lacks V2 integrity columns: "
                    + ", ".join(sorted(missing))
                )
            else:
                entity_expression = (
                    "entity_counts_json"
                    if "entity_counts_json" in latest_columns
                    else "CAST(NULL AS VARCHAR)"
                )
                fallback_expression = (
                    fallback_count_column
                    if fallback_count_column is not None
                    and fallback_count_column in latest_columns
                    else "CAST(NULL AS BIGINT)"
                )
                rows = trino.execute_query(
                    f"SELECT {manifest_batch_column}, raw_uri, {entity_expression}, "
                    f"{fallback_expression} "
                    f"FROM {_qualified(latest_view)} "
                    f"WHERE {manifest_batch_column} LIKE '{prefix}%' "
                    f"AND raw_uri IS NOT NULL ORDER BY {manifest_batch_column}"
                )
                for row in rows:
                    batch_id = str(row[0])
                    if row[2] is not None:
                        try:
                            counts = _parse_entity_counts(
                                group=f"{group} latest-success",
                                batch_id=batch_id,
                                payload=row[2],
                                expected_keys=dataset_keys,
                            )
                        except RuntimeError as exc:
                            errors.append(str(exc))
                            continue
                    elif (
                        fallback_count_column is not None
                        and fallback_dataset_key is not None
                    ):
                        try:
                            fallback_count = int(row[3])
                        except (TypeError, ValueError):
                            errors.append(
                                f"{group} latest-success manifest {batch_id} has "
                                f"invalid {fallback_count_column}"
                            )
                            continue
                        if fallback_count < 0:
                            errors.append(
                                f"{group} latest-success manifest {batch_id} has "
                                f"negative {fallback_count_column}"
                            )
                            continue
                        counts = dict.fromkeys(dataset_keys, 0)
                        counts[fallback_dataset_key] = fallback_count
                    else:
                        errors.append(
                            f"{group} latest-success manifest {batch_id} has null "
                            "entity_counts_json"
                        )
                        continue
                    if batch_id not in semantic_manifests:
                        errors.append(
                            f"{latest_view} exposes unvalidated batch {batch_id}"
                        )
                    for key, count in counts.items():
                        latest_expected[key][batch_id] = count

        for table, key in zip(tables, dataset_keys):
            current = f"{table}_current"
            if not trino.table_exists(SCHEMA, current):
                metrics = _count_map_metrics(latest_expected[key], {})
                datasets[table]["current"] = metrics
                if metrics["mismatch_count"]:
                    errors.append(f"{current} is missing with non-zero committed rows")
                continue
            actual = _batch_counts(
                trino,
                current,
                batch_column=physical_batch_column,
                prefix=prefix,
            )
            metrics = _count_map_metrics(latest_expected[key], actual)
            datasets[table]["current"] = metrics
            if metrics["mismatch_count"]:
                errors.append(f"{current} V2 counts differ from {latest_view}")

    manifest_fingerprint = hashlib.sha256(
        json.dumps(
            sorted(
                (
                    batch_id,
                    raw_uri,
                    sorted(counts.items()),
                )
                for batch_id, (raw_uri, counts) in semantic_manifests.items()
            ),
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "detected": detected,
        "manifest_rows": manifest_rows_count,
        "manifest_batches": len(semantic_manifests),
        "manifest_fingerprint": manifest_fingerprint,
        "datasets": datasets,
        "errors": errors,
    }


def _inspect_match_event_identity(trino: TrinoTableManager, result: dict) -> None:
    """Require raw-backed, globally unique Opta identities for V2 events."""

    table = "whoscored_events"
    metrics = {
        "v2_rows": 0,
        "invalid_or_mismatched_rows": 0,
        "duplicate_source_ids": 0,
        "duplicate_team_event_ids": 0,
        "max_source_event_id": None,
    }
    result["event_identity"] = metrics
    if not trino.table_exists(SCHEMA, table):
        return
    columns = {str(column).lower() for column in trino.get_table_columns(SCHEMA, table)}
    if "_game_batch_id" not in columns:
        return
    metrics["v2_rows"] = _scalar(
        trino,
        f"SELECT COUNT(*) FROM {_qualified(table)} WHERE _game_batch_id LIKE 'ws2-%'",
    )
    if not metrics["v2_rows"]:
        return

    required = {
        "_game_batch_id",
        "game_id",
        "team_id",
        "source_event_id",
        "team_event_id",
        "related_team_event_id",
        "source_raw_json",
    }
    missing = sorted(required - columns)
    if missing:
        result["errors"].append(
            "whoscored_events lacks V2 event identity columns: " + ", ".join(missing)
        )
        return

    row = trino.execute_query(
        f"""
        SELECT
            COUNT_IF(
                source_event_id IS NULL
                OR source_event_id <= 0
                OR team_event_id IS NULL
                OR team_event_id <= 0
                OR TRY_CAST(
                    TRY(json_extract_scalar(source_raw_json, '$.id')) AS BIGINT
                ) IS DISTINCT FROM source_event_id
                OR TRY_CAST(
                    TRY(json_extract_scalar(source_raw_json, '$.eventId')) AS BIGINT
                ) IS DISTINCT FROM team_event_id
                OR TRY_CAST(
                    TRY(json_extract_scalar(source_raw_json, '$.relatedEventId'))
                    AS BIGINT
                ) IS DISTINCT FROM related_team_event_id
            ),
            MAX(source_event_id)
        FROM {_qualified(table)}
        WHERE _game_batch_id LIKE 'ws2-%'
        """
    )[0]
    metrics["invalid_or_mismatched_rows"] = int(row[0] or 0)
    metrics["max_source_event_id"] = None if row[1] is None else int(row[1])
    metrics["duplicate_source_ids"] = _scalar(
        trino,
        f"""
        SELECT COUNT(*) FROM (
            SELECT _game_batch_id, game_id, source_event_id
            FROM {_qualified(table)}
            WHERE _game_batch_id LIKE 'ws2-%'
            GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
        ) duplicate_ids
        """,
    )
    metrics["duplicate_team_event_ids"] = _scalar(
        trino,
        f"""
        SELECT COUNT(*) FROM (
            SELECT _game_batch_id, game_id, team_id, team_event_id
            FROM {_qualified(table)}
            WHERE _game_batch_id LIKE 'ws2-%'
            GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
        ) duplicate_ids
        """,
    )
    if metrics["invalid_or_mismatched_rows"]:
        result["errors"].append(
            "whoscored_events V2 rows do not preserve raw id/eventId identity: "
            f"{metrics['invalid_or_mismatched_rows']}"
        )
    if metrics["duplicate_source_ids"]:
        result["errors"].append(
            "whoscored_events V2 has duplicate global source_event_id groups: "
            f"{metrics['duplicate_source_ids']}"
        )
    if metrics["duplicate_team_event_ids"]:
        result["errors"].append(
            "whoscored_events V2 has duplicate team-local event groups: "
            f"{metrics['duplicate_team_event_ids']}"
        )


def _inspect_profile_commits(trino: TrinoTableManager) -> dict:
    manifest_table = "whoscored_profile_ingest_manifest"
    batch_column = "_profile_batch_id"
    prefix = "wspr2-"
    errors: list[str] = []
    manifest_rows_count = 0
    semantic: dict[str, tuple[int, str, str, str, int]] = {}
    if trino.table_exists(SCHEMA, manifest_table):
        columns = {
            str(column).lower()
            for column in trino.get_table_columns(SCHEMA, manifest_table)
        }
        marker_columns = {batch_column, "state"}
        if marker_columns <= columns:
            manifest_rows_count = _scalar(
                trino,
                f"SELECT COUNT(*) FROM {_qualified(manifest_table)} "
                f"WHERE state = 'success' AND {batch_column} LIKE '{prefix}%'",
            )
        required = {
            batch_column,
            "player_id",
            "payload_sha256",
            "parser_version",
            "raw_uri",
            "participations_count",
            "state",
        }
        if manifest_rows_count and not required <= columns:
            errors.append(
                f"{manifest_table} lacks V2 integrity columns: "
                + ", ".join(sorted(required - columns))
            )
        elif required <= columns:
            rows = trino.execute_query(
                f"SELECT {batch_column}, player_id, payload_sha256, parser_version, "
                f"raw_uri, participations_count FROM {_qualified(manifest_table)} "
                f"WHERE state = 'success' AND {batch_column} LIKE '{prefix}%' "
                f"GROUP BY {batch_column}, player_id, payload_sha256, parser_version, "
                f"raw_uri, participations_count ORDER BY {batch_column}"
            )
            for row in rows:
                batch_id = str(row[0])
                raw_uri = None if row[4] is None else str(row[4])
                if not raw_uri:
                    errors.append(f"profile manifest {batch_id} is not raw-backed")
                    continue
                try:
                    participations = int(row[5])
                except (TypeError, ValueError):
                    errors.append(
                        f"profile manifest {batch_id} has invalid participation count"
                    )
                    continue
                if participations < 0:
                    errors.append(
                        f"profile manifest {batch_id} has negative participation count"
                    )
                    continue
                value = (
                    int(row[1]),
                    str(row[2]),
                    str(row[3]),
                    raw_uri,
                    participations,
                )
                if batch_id in semantic and semantic[batch_id] != value:
                    errors.append(f"profile batch {batch_id} has conflicting manifests")
                    continue
                semantic[batch_id] = value

    expected = {
        PROFILE_TABLES[0]: {batch_id: 1 for batch_id in semantic},
        PROFILE_TABLES[1]: {batch_id: value[4] for batch_id, value in semantic.items()},
    }
    datasets: dict[str, dict] = {}
    physical_detected = False
    for table in PROFILE_TABLES:
        actual = _batch_counts(
            trino,
            table,
            batch_column=batch_column,
            prefix=prefix,
        )
        physical_detected = physical_detected or bool(actual)
        metrics = _count_map_metrics(expected[table], actual)
        datasets[table] = {"physical": metrics}
        if metrics["mismatch_count"]:
            errors.append(f"{table} physical V2 counts differ from {manifest_table}")

    detected = bool(manifest_rows_count or semantic or physical_detected)
    if detected and not trino.table_exists(SCHEMA, manifest_table):
        errors.append(f"{manifest_table} is missing for V2 physical rows")

    current_metrics = {
        "expected_players": len({value[0] for value in semantic.values()}),
        "missing_or_mismatched_players": 0,
    }
    if detected:
        current = "whoscored_player_profile_current"
        if not trino.table_exists("silver", current):
            errors.append(f"silver.{current} is missing for existing V2 commits")
        else:
            mismatches = _scalar(
                trino,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY player_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     _profile_batch_id DESC, _batch_id DESC
                        ) AS rn
                        FROM {_qualified(manifest_table)} m
                        WHERE state = 'success'
                          AND _profile_batch_id LIKE 'wspr2-%'
                          AND raw_uri IS NOT NULL
                    ) WHERE rn = 1
                ) m
                LEFT JOIN {CATALOG}.silver.{current} p
                  ON p.player_id = m.player_id
                 AND p.payload_sha256 = m.payload_sha256
                 AND p.parser_version = m.parser_version
                 AND p.raw_uri IS NOT NULL
                WHERE p.player_id IS NULL
                """,
            )
            current_metrics["missing_or_mismatched_players"] = mismatches
            if mismatches:
                errors.append(
                    "silver.whoscored_player_profile_current differs from latest V2 profiles"
                )

    manifest_fingerprint = hashlib.sha256(
        json.dumps(sorted(semantic.items()), separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "detected": detected,
        "manifest_rows": manifest_rows_count,
        "manifest_batches": len(semantic),
        "manifest_fingerprint": manifest_fingerprint,
        "datasets": datasets,
        "current": current_metrics,
        "errors": errors,
    }


def _v2_integrity_signature(report: Mapping) -> dict:
    """Return the stable subset that must be identical before and after cutover."""

    signature: dict[str, dict] = {}
    for group, result in report.get("groups", {}).items():
        group_signature = {
            "detected": bool(result.get("detected")),
            "manifest_rows": int(result.get("manifest_rows", 0)),
            "manifest_batches": int(result.get("manifest_batches", 0)),
            "manifest_fingerprint": result.get("manifest_fingerprint"),
            "datasets": {},
        }
        for table, metrics in result.get("datasets", {}).items():
            group_signature["datasets"][table] = {
                kind: {
                    "actual_rows": int(values.get("actual_rows", 0)),
                    "actual_batches": int(values.get("actual_batches", 0)),
                    "actual_fingerprint": values.get("actual_fingerprint"),
                }
                for kind, values in metrics.items()
            }
        if "current" in result:
            group_signature["current"] = dict(result["current"])
        if "event_identity" in result:
            group_signature["event_identity"] = dict(result["event_identity"])
        signature[group] = group_signature
    return signature


def compare_v2_integrity(before: Mapping, after: Mapping) -> dict:
    """Fail closed if any pre-existing V2 commit changes during the migration."""

    before_signature = _v2_integrity_signature(before)
    after_signature = _v2_integrity_signature(after)
    return {
        "passed": before_signature == after_signature,
        "before_fingerprint": hashlib.sha256(
            json.dumps(before_signature, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "after_fingerprint": hashlib.sha256(
            json.dumps(after_signature, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    }


def inspect_existing_v2_commits(trino: TrinoTableManager) -> dict:
    """Validate pre-existing raw-backed match, preview and profile commits.

    Unlike the old marker-only guard, a valid V2 commit is allowed. Every
    declared dataset count must match the append-only physical batch and the
    manifest-gated current view. Orphan/partial batches, conflicting manifests,
    raw-less successes, and missing views fail before the first migration DDL.
    """

    groups = {
        "match": _inspect_json_commit_group(
            trino,
            group="match",
            manifest_table="whoscored_match_ingest_manifest",
            latest_view="whoscored_match_ingest_latest_success",
            tables=MATCH_TABLES,
            prefix="ws2-",
            manifest_batch_column="batch_id",
            physical_batch_column="_game_batch_id",
        ),
        "preview": _inspect_json_commit_group(
            trino,
            group="preview",
            manifest_table="whoscored_preview_ingest_manifest",
            latest_view="whoscored_preview_ingest_latest_success",
            tables=PREVIEW_TABLES,
            prefix="wsp2-",
            manifest_batch_column="batch_id",
            physical_batch_column="_preview_batch_id",
            fallback_count_column="missing_players_count",
            fallback_dataset_key="missing_players",
        ),
        "profile": _inspect_profile_commits(trino),
    }
    _inspect_match_event_identity(trino, groups["match"])
    errors = [error for result in groups.values() for error in result.get("errors", ())]
    detected = any(result.get("detected") for result in groups.values())
    total_markers = sum(
        int(result.get("manifest_rows", 0))
        + sum(
            int(metrics.get("physical", {}).get("actual_rows", 0))
            for metrics in result.get("datasets", {}).values()
        )
        for result in groups.values()
    )
    return {
        "passed": not errors,
        "detected": detected,
        "groups": groups,
        "total_markers": total_markers,
        "errors": errors,
    }


def _projection(table: str, columns: Sequence[str]) -> list[str]:
    casts = ID_CASTS.get(table, {})
    present = set(columns)
    if (
        table == "whoscored_events"
        and "related_event_id" in present
        and "related_team_event_id" in present
    ):
        raise RuntimeError(
            "whoscored_events contains both legacy related_event_id and "
            "related_team_event_id"
        )
    logical_batch_column = BATCH_COLUMN_BY_TABLE.get(table)
    logical_batch_prefix = (
        _BATCH_PREFIX_BY_COLUMN.get(logical_batch_column)
        if logical_batch_column is not None
        else None
    )
    result: list[str] = []
    for column in columns:
        if table == "whoscored_events" and column == "related_event_id":
            # The source field is a team-local Opta sequence, not a foreign
            # key to the globally unique event ``id``.  Rename it during the
            # reversible shadow copy so the active schema cannot imply a
            # false global relationship.
            result.append(
                'CAST("related_event_id" AS BIGINT) AS "related_team_event_id"'
            )
        elif column == logical_batch_column:
            # Current views use a NULL logical batch as the explicit legacy
            # bridge. Only the source-specific, raw-backed V2 prefix is a
            # logical commit; old ``legacy-*`` and arbitrary identifiers are
            # normalised to NULL. This also keeps pre-existing V2 scope rows.
            result.append(
                f"CASE WHEN \"{column}\" LIKE '{logical_batch_prefix}%' "
                f'THEN "{column}" ELSE CAST(NULL AS VARCHAR) END AS "{column}"'
            )
        elif column in casts:
            result.append(f'CAST("{column}" AS {casts[column]}) AS "{column}"')
        else:
            result.append(f'"{column}" AS "{column}"')

    if table == "whoscored_events":
        additions = {
            "source_event_id": "CAST(NULL AS BIGINT) AS source_event_id",
            "team_event_id": "CAST(NULL AS BIGINT) AS team_event_id",
            "related_team_event_id": ("CAST(NULL AS BIGINT) AS related_team_event_id"),
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_game_batch_id": "CAST(NULL AS VARCHAR) AS _game_batch_id",
        }
        projected = present | (
            {"related_team_event_id"} if "related_event_id" in present else set()
        )
        result.extend(
            expression
            for name, expression in additions.items()
            if name not in projected
        )
    elif table == "whoscored_lineups":
        additions = {
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_game_batch_id": "CAST(NULL AS VARCHAR) AS _game_batch_id",
        }
        result.extend(
            expression for name, expression in additions.items() if name not in present
        )
    elif table == "whoscored_missing_players":
        additions = {
            "_payload_sha256": "CAST(NULL AS VARCHAR) AS _payload_sha256",
            "_parser_version": "'legacy-v1' AS _parser_version",
            "_preview_batch_id": "CAST(NULL AS VARCHAR) AS _preview_batch_id",
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
    if (
        table == "whoscored_events"
        and "related_event_id" in requested
        and "related_event_id" not in present
        and "related_team_event_id" in present
    ):
        # Idempotent re-runs operate on the honest V2 column name while the
        # legacy source contract above deliberately retains its old name.
        relation_index = list(requested).index("related_event_id")
        insertion = sum(
            candidate in present for candidate in requested[:relation_index]
        )
        keys.insert(insertion, "related_team_event_id")
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
    partition_expressions = [f'"{column}"' for column in keys]
    logical_batch_column = BATCH_COLUMN_BY_TABLE.get(table)
    logical_batch_prefix = (
        _BATCH_PREFIX_BY_COLUMN.get(logical_batch_column)
        if logical_batch_column is not None
        else None
    )
    if logical_batch_column in columns and logical_batch_prefix is not None:
        # Legacy duplicates collapse by natural key, while every V2 commit is
        # an independent append-only version. Deduplicating only by natural key
        # would retain the newest V2 row but orphan older valid manifests.
        partition_expressions.append(
            f'CASE WHEN "{logical_batch_column}" LIKE '
            f"'{logical_batch_prefix}%' THEN \"{logical_batch_column}\" END"
        )
    partition_by = ", ".join(partition_expressions)
    source_v2_batches = (
        _batch_counts(
            trino,
            table,
            batch_column=logical_batch_column,
            prefix=logical_batch_prefix,
        )
        if logical_batch_column in columns and logical_batch_prefix is not None
        else {}
    )
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
        where = f"league = {_literal(str(league))} AND season = {_literal(str(season))}"
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
            if "related_event_id" in expected_columns:
                expected_columns.remove("related_event_id")
            expected_columns.update(
                {
                    "source_event_id",
                    "team_event_id",
                    "related_team_event_id",
                    "_payload_sha256",
                    "_parser_version",
                    "_game_batch_id",
                }
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
                where = f"league = {_literal(league)} AND season = {_literal(season)}"
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
            shadow_v2_batches = (
                _batch_counts(
                    trino,
                    shadow,
                    batch_column=logical_batch_column,
                    prefix=logical_batch_prefix,
                )
                if logical_batch_column is not None and logical_batch_prefix is not None
                else {}
            )
            if shadow_v2_batches != source_v2_batches:
                raise RuntimeError(
                    f"{shadow} does not preserve V2 logical batches: "
                    f"source={source_v2_batches}, shadow={shadow_v2_batches}"
                )
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
            SELECT {", ".join(projection)}
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
    shadow_v2_batches = (
        _batch_counts(
            trino,
            shadow,
            batch_column=logical_batch_column,
            prefix=logical_batch_prefix,
        )
        if logical_batch_column is not None and logical_batch_prefix is not None
        else {}
    )
    if shadow_v2_batches != source_v2_batches:
        raise RuntimeError(
            f"{shadow} does not preserve V2 logical batches: "
            f"source={source_v2_batches}, shadow={shadow_v2_batches}"
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
            trino._execute(f"ALTER TABLE {_qualified(backup)} RENAME TO {_name(table)}")
        raise
    return backup


def seed_profiles(trino: TrinoTableManager) -> int:
    if not trino.table_exists(SCHEMA, "whoscored_player_profile"):
        return 0
    required = (PROFILE_VERSIONS_TABLE, PROFILE_MANIFEST_TABLE)
    missing_tables = [name for name in required if not trino.table_exists(SCHEMA, name)]
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
            return f'CAST(NULL AS {target_types[column]}) AS "{column}"'
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
        SELECT {", ".join(profile_projection)}, NULL, NULL, 'legacy-v1',
               ranked._ingested_at, 'whoscored', 'player_profile',
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
               concat('legacy-profile-', CAST(ranked.player_id AS VARCHAR))
        FROM (
            SELECT p.*, ROW_NUMBER() OVER (
                PARTITION BY CAST(player_id AS BIGINT)
                ORDER BY _ingested_at DESC, _batch_id DESC
            ) rn
            FROM {_qualified("whoscored_player_profile")} p
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


def rollback(
    trino: TrinoTableManager,
    suffix: str,
    *,
    preserve_v2_state: bool = False,
) -> list[str]:
    """Restore legacy physical tables, optionally retaining prior V2 commits."""

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

    if preserve_v2_state:
        # A manual rollback may happen long after cutover. If ingestion added a
        # new V2 batch to an active migrated table, the frozen backup cannot
        # replace it safely. Compare every logical batch before the first
        # RENAME and leave both sides untouched on drift.
        for table, backup, _failed, current_exists in physical_actions:
            batch_column = BATCH_COLUMN_BY_TABLE.get(table)
            prefix = (
                _BATCH_PREFIX_BY_COLUMN.get(batch_column)
                if batch_column is not None
                else None
            )
            if not current_exists or batch_column is None or prefix is None:
                continue
            active_batches = _batch_counts(
                trino,
                table,
                batch_column=batch_column,
                prefix=prefix,
            )
            backup_batches = _batch_counts(
                trino,
                backup,
                batch_column=batch_column,
                prefix=prefix,
            )
            unsafe_batches = {
                batch_id: {
                    "active": active_count,
                    "backup": int(backup_batches.get(batch_id, 0)),
                }
                for batch_id, active_count in active_batches.items()
                if int(active_count) > int(backup_batches.get(batch_id, 0))
            }
            if unsafe_batches:
                raise RuntimeError(
                    f"cannot rollback {table}: active V2 batches are newer than "
                    f"{backup}; unsafe={unsafe_batches}"
                )

    v2_actions: list[tuple[str, str]] = []
    if not preserve_v2_state:
        for table in ROLLBACK_STATE_TABLES:
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

    if not preserve_v2_state:
        for schema, view in (
            *(("silver", view) for view in reversed(SILVER_VIEWS)),
            *((SCHEMA, view) for view in reversed(BRONZE_VIEWS)),
        ):
            trino._execute(f"DROP VIEW IF EXISTS {CATALOG}.{schema}.{_name(view)}")

    restored: list[str] = []
    for table, backup, failed, current_exists in physical_actions:
        if current_exists:
            trino._execute(f"ALTER TABLE {_qualified(table)} RENAME TO {_name(failed)}")
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
        *(f"{table}_v2_failed_{suffix}" for table in ROLLBACK_STATE_TABLES),
    ]
    return [table for table in candidates if trino.table_exists(SCHEMA, table)]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the migration")
    parser.add_argument(
        "--rollback-suffix", help="restore legacy tables for this suffix"
    )
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
        rollback_integrity = inspect_existing_v2_commits(trino)
        if rollback_integrity["detected"] and not rollback_integrity["passed"]:
            _emit_report(
                {
                    "mode": "rollback",
                    "suffix": rollback_suffix,
                    "status": "blocked",
                    "existing_v2_integrity": rollback_integrity,
                    "error": "pre-existing V2 commits are inconsistent",
                },
                args.report,
            )
            return 2
        restored = rollback(
            trino,
            rollback_suffix,
            preserve_v2_state=rollback_integrity["detected"],
        )
        after_rollback = inspect_existing_v2_commits(trino)
        rollback_parity = compare_v2_integrity(rollback_integrity, after_rollback)
        if rollback_integrity["detected"] and (
            not after_rollback["passed"] or not rollback_parity["passed"]
        ):
            raise RuntimeError("rollback changed pre-existing V2 commit state")
        _emit_report(
            {
                "mode": "rollback",
                "suffix": rollback_suffix,
                "restored": restored,
                "existing_v2_integrity": after_rollback,
                "v2_parity": rollback_parity,
            },
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
    report["existing_v2_integrity"] = inspect_existing_v2_commits(trino)
    if args.apply and not report["existing_v2_integrity"]["passed"]:
        report["status"] = "blocked"
        report["error"] = "pre-existing V2 commits are inconsistent"
        _emit_report(report, args.report)
        return 2
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
            report["tables"][table].update({"backup": backup, "status": "swapped"})

        repository = WhoScoredRepository(trino=trino)
        # Create all physical/state objects before exposing views.  Match,
        # preview and scope legacy rows intentionally retain NULL logical batch
        # ids; the repository's transitional views keep them visible until a
        # raw-backed V2 commit replaces the same game/scope.
        repository.ensure_schema(create_views=False)
        report["profile_versions"] = seed_profiles(trino)
        repository.ensure_schema()
        report["object_contract"] = inspect_object_contract(trino)
        if not report["object_contract"]["passed"]:
            raise RuntimeError(
                "WhoScored V2 object contract is incomplete: "
                + json.dumps(report["object_contract"], sort_keys=True)
            )
        report["existing_v2_integrity_after"] = inspect_existing_v2_commits(trino)
        if not report["existing_v2_integrity_after"]["passed"]:
            raise RuntimeError(
                "WhoScored V2 commit integrity failed after shadow swap: "
                + json.dumps(
                    report["existing_v2_integrity_after"]["errors"],
                    sort_keys=True,
                )
            )
        report["existing_v2_parity"] = compare_v2_integrity(
            report["existing_v2_integrity"],
            report["existing_v2_integrity_after"],
        )
        if not report["existing_v2_parity"]["passed"]:
            raise RuntimeError(
                "pre-existing V2 commits changed during WhoScored shadow migration"
            )
        report["after"] = capture_state(trino)
        report["status"] = "success"
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        if swap_performed:
            try:
                report["auto_rollback"] = rollback(
                    trino,
                    suffix,
                    preserve_v2_state=report["existing_v2_integrity"]["detected"],
                )
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
