#!/usr/bin/env python3
"""Idempotent production bootstrap for SofaScore raw-first tables.

The migration creates only empty Iceberg contracts. It never invents manifest
success, raw lineage or source rows, so committed-state DQ remains fail-closed
until real capture/replay has populated every required endpoint.

Modes:

* ``--dry-run`` (default) renders SQL without opening a Trino connection;
* ``--preflight`` performs read-only existence/schema/partition/comment checks;
* ``--apply`` executes idempotent DDL and then requires a green preflight.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence


BOOTSTRAP_VERSION = "sofascore-production-bootstrap-v2"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SofaScoreBootstrapError(RuntimeError):
    """The empty-table production contract could not be established."""


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str

    def __post_init__(self) -> None:
        _identifier(self.name, "column")
        if not isinstance(self.sql_type, str) or not self.sql_type.strip():
            raise ValueError("column sql_type must not be empty")
        if any(token in self.sql_type for token in (";", "--", "/*", "*/")):
            raise ValueError(f"unsafe SQL type: {self.sql_type!r}")


@dataclass(frozen=True)
class BootstrapTable:
    schema: str
    name: str
    columns: tuple[Column, ...]
    partition_columns: tuple[str, ...]
    grain: str
    natural_key: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.schema, "schema")
        _identifier(self.name, "table")
        if not self.columns:
            raise ValueError("bootstrap table must declare columns")
        names = tuple(column.name for column in self.columns)
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate columns in {self.schema}.{self.name}")
        if not self.grain.strip():
            raise ValueError("table grain must not be empty")
        if not self.natural_key or not set(self.natural_key) <= set(names):
            raise ValueError("natural key must be non-empty and declared in columns")
        if not set(self.partition_columns) <= set(names):
            raise ValueError("partition columns must be declared in columns")

    @property
    def comment(self) -> str:
        key = ", ".join(self.natural_key)
        return f"Grain: {self.grain}. Natural key: ({key})."


def _identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"invalid {label}: {value!r}")
    return value


def _columns(*items: tuple[str, str]) -> tuple[Column, ...]:
    return tuple(Column(name, sql_type) for name, sql_type in items)


_LINEAGE_COLUMNS = (
    ("source_tournament_id", "varchar"),
    ("source_season_id", "varchar"),
    ("raw_content_hash", "varchar"),
    ("raw_blob_key", "varchar"),
    ("league", "varchar"),
    ("season", "varchar"),
    ("_source", "varchar"),
    ("_entity_type", "varchar"),
    ("_ingested_at", "timestamp(6)"),
    ("_batch_id", "varchar"),
)

_PLAYER_UNIVERSE_METADATA = (
    ("source_tournament_id", "varchar"),
    ("source_season_id", "varchar"),
    ("league", "varchar"),
    ("season", "varchar"),
    ("_source", "varchar"),
    ("_entity_type", "varchar"),
    ("_ingested_at", "timestamp(6)"),
    ("_batch_id", "varchar"),
)


OPS_MANIFEST = BootstrapTable(
    schema="ops",
    name="sofascore_capture_manifest",
    columns=_columns(
        ("source_tournament_id", "varchar"),
        ("source_season_id", "varchar"),
        ("target_type", "varchar"),
        ("target_id", "varchar"),
        ("endpoint", "varchar"),
        ("freshness_key", "varchar"),
        ("status", "varchar"),
        ("run_id", "varchar"),
        ("task_id", "varchar"),
        ("attempts", "integer"),
        ("row_count", "bigint"),
        ("http_status", "integer"),
        ("raw_content_hash", "varchar"),
        ("raw_blob_key", "varchar"),
        ("request_url", "varchar"),
        ("error_type", "varchar"),
        ("error_message", "varchar"),
        ("duration_ms", "bigint"),
        ("provider_bytes", "bigint"),
        ("fetched_at", "varchar"),
        ("parsed_at", "varchar"),
        ("updated_at", "varchar"),
        ("manifest_version", "varchar"),
    ),
    partition_columns=("source_tournament_id", "source_season_id"),
    grain="one latest capture state per exact source target endpoint freshness",
    natural_key=(
        "source_tournament_id",
        "source_season_id",
        "target_type",
        "target_id",
        "endpoint",
        "freshness_key",
    ),
)


SOFASCORE_EVENTS = BootstrapTable(
    schema="bronze",
    name="sofascore_events",
    columns=_columns(
        ("match_id", "varchar"),
        ("id", "bigint"),
        ("start_timestamp", "bigint"),
        ("status_code", "bigint"),
        ("status_type", "varchar"),
        ("status_description", "varchar"),
        ("unique_tournament_id", "bigint"),
        ("unique_tournament_name", "varchar"),
        ("season_id", "bigint"),
        ("season_name", "varchar"),
        ("season_year", "varchar"),
        ("home_team_id", "bigint"),
        ("home_team_name", "varchar"),
        ("home_team_gender", "varchar"),
        ("home_team_team_type", "bigint"),
        ("away_team_id", "bigint"),
        ("away_team_name", "varchar"),
        ("away_team_gender", "varchar"),
        ("away_team_team_type", "bigint"),
        ("home_score_current", "bigint"),
        ("home_score_display", "bigint"),
        ("away_score_current", "bigint"),
        ("away_score_display", "bigint"),
        ("round_info_round", "bigint"),
        ("venue_stadium_id", "bigint"),
        ("venue_stadium_name", "varchar"),
        ("venue_stadium_capacity", "bigint"),
        ("venue_city_name", "varchar"),
        ("venue_country_name", "varchar"),
        ("venue_country_alpha2", "varchar"),
        ("venue_venue_coordinates_latitude", "double"),
        ("venue_venue_coordinates_longitude", "double"),
        ("referee_id", "bigint"),
        ("referee_name", "varchar"),
        ("referee_country_name", "varchar"),
        *_LINEAGE_COLUMNS,
    ),
    partition_columns=("league", "season"),
    grain="one full-event scalar projection",
    natural_key=("league", "season", "match_id"),
)


SOFASCORE_EVENT_PARTICIPANTS = BootstrapTable(
    schema="bronze",
    name="sofascore_event_participants",
    columns=_columns(
        ("match_id", "varchar"),
        ("team_id", "varchar"),
        ("team_side", "varchar"),
        ("name", "varchar"),
        ("gender", "varchar"),
        ("team_type", "varchar"),
        ("id", "bigint"),
        *_LINEAGE_COLUMNS,
    ),
    partition_columns=("league", "season"),
    grain="one home or away team in one event",
    natural_key=("league", "season", "match_id", "team_id"),
)


SOFASCORE_LINEUPS = BootstrapTable(
    schema="bronze",
    name="sofascore_lineups",
    columns=_columns(
        ("match_id", "varchar"),
        ("player_id", "varchar"),
        ("team_side", "varchar"),
        ("position", "varchar"),
        ("is_starter", "boolean"),
        ("is_bench", "boolean"),
        ("is_unused_substitute", "boolean"),
        ("participation_status", "varchar"),
        ("shirt_number", "bigint"),
        ("substitute", "boolean"),
        ("captain", "boolean"),
        *_LINEAGE_COLUMNS,
    ),
    partition_columns=("league", "season"),
    grain="one listed player in one event",
    natural_key=("league", "season", "match_id", "player_id"),
)


SOFASCORE_INCIDENTS = BootstrapTable(
    schema="bronze",
    name="sofascore_incidents",
    columns=_columns(
        ("match_id", "varchar"),
        ("incident_id", "varchar"),
        ("incident_order", "bigint"),
        ("incident_type", "varchar"),
        ("id", "bigint"),
        ("time", "bigint"),
        ("added_time", "bigint"),
        ("is_home", "boolean"),
        ("incident_class", "varchar"),
        ("reason", "varchar"),
        ("reversed", "boolean"),
        ("player_id", "bigint"),
        ("player_name", "varchar"),
        ("assist1_id", "bigint"),
        ("assist1_name", "varchar"),
        ("player_in_id", "bigint"),
        ("player_in_name", "varchar"),
        ("player_out_id", "bigint"),
        ("player_out_name", "varchar"),
        ("home_score", "bigint"),
        ("away_score", "bigint"),
        ("var_decision", "varchar"),
        *_LINEAGE_COLUMNS,
    ),
    partition_columns=("league", "season"),
    grain="one ordered incident in one event",
    natural_key=("league", "season", "match_id", "incident_id"),
)


SOFASCORE_PLAYER_UNIVERSE = BootstrapTable(
    schema="bronze",
    name="sofascore_player_universe",
    columns=_columns(
        ("player_id", "varchar"),
        ("in_registered_squad", "boolean"),
        ("observed_in_match", "boolean"),
        *_PLAYER_UNIVERSE_METADATA,
    ),
    partition_columns=("league", "season"),
    grain="one registered or match-observed player in one competition season",
    natural_key=("league", "season", "player_id"),
)


NEW_BRONZE_BOOTSTRAP_TABLES = (
    SOFASCORE_EVENTS,
    SOFASCORE_EVENT_PARTICIPANTS,
    SOFASCORE_LINEUPS,
    SOFASCORE_INCIDENTS,
    SOFASCORE_PLAYER_UNIVERSE,
)
BOOTSTRAP_TABLES = (OPS_MANIFEST, *NEW_BRONZE_BOOTSTRAP_TABLES)


@dataclass(frozen=True)
class LegacyTableMigration:
    """A live Bronze table whose legacy rows must satisfy the new MERGE key."""

    name: str
    natural_key: tuple[str, ...]
    normalized_column: str
    normalized_type: str
    fallback_columns: tuple[str, ...]
    null_replacement: Optional[str]
    grain: str

    def __post_init__(self) -> None:
        _identifier(self.name, "legacy table")
        _identifier(self.normalized_column, "normalized column")
        for column in (*self.natural_key, *self.fallback_columns):
            _identifier(column, "legacy column")
        if self.normalized_column not in self.natural_key:
            raise ValueError("normalized column must belong to the natural key")

    @property
    def comment(self) -> str:
        return f"Grain: {self.grain}. Natural key: ({', '.join(self.natural_key)})."


LEGACY_MATCH_STATS = LegacyTableMigration(
    name="sofascore_match_stats",
    natural_key=(
        "league",
        "season",
        "match_id",
        "period",
        "stat_group",
        "statistic_key",
    ),
    normalized_column="statistic_key",
    normalized_type="varchar",
    fallback_columns=(
        "stat_key",
        "key",
        "statistics_type",
        "stat_name",
        "name",
    ),
    null_replacement=None,
    grain="one statistic key in one event period",
)

LEGACY_STANDINGS = LegacyTableMigration(
    name="sofascore_league_table",
    natural_key=("league", "season", "group", "team"),
    normalized_column="group",
    normalized_type="varchar",
    fallback_columns=(),
    null_replacement="__total__",
    grain="one team in one standings scope",
)

LEGACY_TABLE_MIGRATIONS = (LEGACY_MATCH_STATS, LEGACY_STANDINGS)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qualified(table: BootstrapTable, catalog: str) -> str:
    _identifier(catalog, "catalog")
    return f"{catalog}.{table.schema}.{table.name}"


def render_create_table(table: BootstrapTable, *, catalog: str = "iceberg") -> str:
    qualified = _qualified(table, catalog)
    columns = ",\n".join(
        f'    "{column.name}" {column.sql_type}' for column in table.columns
    )
    properties = ["format = 'PARQUET'"]
    if table.partition_columns:
        partitioning = ", ".join(
            _sql_string(column) for column in table.partition_columns
        )
        properties.append(f"partitioning = ARRAY[{partitioning}]")
    rendered_properties = ",\n    ".join(properties)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n{columns}\n)\n"
        f"COMMENT {_sql_string(table.comment)}\n"
        f"WITH (\n    {rendered_properties}\n)"
    )


def render_comment_table(table: BootstrapTable, *, catalog: str = "iceberg") -> str:
    return (
        f"COMMENT ON TABLE {_qualified(table, catalog)} IS {_sql_string(table.comment)}"
    )


def bootstrap_sql(*, catalog: str = "iceberg") -> tuple[str, ...]:
    _identifier(catalog, "catalog")
    schemas = tuple(dict.fromkeys(table.schema for table in BOOTSTRAP_TABLES))
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}" for schema in schemas
    ]
    for table in BOOTSTRAP_TABLES:
        statements.extend(
            (
                render_create_table(table, catalog=catalog),
                render_comment_table(table, catalog=catalog),
            )
        )
    return tuple(statements)


def legacy_dry_run_steps(*, catalog: str = "iceberg") -> tuple[dict, ...]:
    """Describe dynamic, column-preserving migrations without connecting."""

    _identifier(catalog, "catalog")
    return (
        {
            "table": "bronze.sofascore_match_stats",
            "alter": (
                f"ALTER TABLE {catalog}.bronze.sofascore_match_stats "
                "ADD COLUMN statistic_key varchar (only when absent)"
            ),
            "backfill": (
                "statistic_key = COALESCE(stat_key, key, statistics_type, "
                "stat_name, name), ignoring blank strings"
            ),
            "dedup_natural_key": list(LEGACY_MATCH_STATS.natural_key),
            "rewrite": (
                "atomic CREATE OR REPLACE only when duplicates exist; preserve "
                "every DESCRIBE column and keep latest _ingested_at/_batch_id"
            ),
        },
        {
            "table": "bronze.sofascore_league_table",
            "backfill": "group = '__total__' where group is null or blank",
            "dedup_natural_key": list(LEGACY_STANDINGS.natural_key),
            "rewrite": (
                "atomic CREATE OR REPLACE only when duplicates exist; preserve "
                "every DESCRIBE column and keep latest _ingested_at/_batch_id"
            ),
        },
    )


def _quoted_identifier(value: str) -> str:
    return f'"{_identifier(value, "column")}"'


def _qualified_legacy(migration: LegacyTableMigration, catalog: str) -> str:
    _identifier(catalog, "catalog")
    return f"{catalog}.bronze.{migration.name}"


def _scalar_count(manager, sql: str) -> int:
    rows = manager._execute(sql, fetch=True)
    if (
        not rows
        or not rows[0]
        or isinstance(rows[0][0], bool)
        or not isinstance(rows[0][0], int)
        or rows[0][0] < 0
    ):
        raise SofaScoreBootstrapError("migration count query returned no integer")
    return int(rows[0][0])


def _blank(column: str) -> str:
    quoted = _quoted_identifier(column)
    return f"{quoted} IS NULL OR TRIM(CAST({quoted} AS varchar)) = ''"


def _null_natural_key_sql(
    migration: LegacyTableMigration,
    *,
    catalog: str,
) -> str:
    predicate = " OR ".join(_blank(column) for column in migration.natural_key)
    return (
        f"SELECT COUNT(*) FROM {_qualified_legacy(migration, catalog)} "
        f"WHERE {predicate}"
    )


def _duplicate_natural_key_sql(
    migration: LegacyTableMigration,
    *,
    catalog: str,
) -> str:
    keys = ", ".join(_quoted_identifier(column) for column in migration.natural_key)
    return (
        "SELECT COUNT(*) FROM ("
        f"SELECT 1 FROM {_qualified_legacy(migration, catalog)} "
        f"GROUP BY {keys} HAVING COUNT(*) > 1"
        ") AS duplicate_keys"
    )


def _normalization_sql(
    migration: LegacyTableMigration,
    columns: Mapping[str, object],
    *,
    catalog: str,
) -> Optional[str]:
    qualified = _qualified_legacy(migration, catalog)
    target = _quoted_identifier(migration.normalized_column)
    if migration.null_replacement is not None:
        value_sql = _sql_string(migration.null_replacement)
    else:
        fallbacks = [
            column for column in migration.fallback_columns if column in columns
        ]
        if not fallbacks:
            return None
        value_sql = (
            "COALESCE("
            + ", ".join(
                f"NULLIF(TRIM(CAST({_quoted_identifier(column)} AS varchar)), '')"
                for column in fallbacks
            )
            + ")"
        )
    return (
        f"UPDATE {qualified} SET {target} = {value_sql} "
        f"WHERE {_blank(migration.normalized_column)}"
    )


def _dedup_rewrite_sql(
    migration: LegacyTableMigration,
    columns: Mapping[str, object],
    *,
    catalog: str,
) -> str:
    """Render an atomic, schema-position-preserving latest-row rewrite."""

    qualified = _qualified_legacy(migration, catalog)
    names = tuple(columns)
    if not names or "league" not in names or "season" not in names:
        raise SofaScoreBootstrapError(
            f"{qualified} cannot preserve its league/season partition contract"
        )
    selected = ", ".join(_quoted_identifier(column) for column in names)
    keys = ", ".join(_quoted_identifier(column) for column in migration.natural_key)
    preferred = [
        column for column in ("_ingested_at", "_batch_id") if column in columns
    ]
    deterministic = preferred + [
        column
        for column in names
        if column not in preferred and column not in migration.natural_key
    ]
    order = ", ".join(
        f"{_quoted_identifier(column)} DESC NULLS LAST" for column in deterministic
    )
    if not order:
        order = keys
    return (
        f"CREATE OR REPLACE TABLE {qualified}\n"
        "WITH (format = 'PARQUET', partitioning = ARRAY['league', 'season'])\n"
        "AS\n"
        f"SELECT {selected}\n"
        "FROM (\n"
        f"    SELECT {selected},\n"
        "           ROW_NUMBER() OVER (\n"
        f"               PARTITION BY {keys}\n"
        f"               ORDER BY {order}\n"
        '           ) AS "__sofascore_migration_rn"\n'
        f"    FROM {qualified}\n"
        ") migrated\n"
        'WHERE "__sofascore_migration_rn" = 1'
    )


def apply_legacy_migrations(manager, *, catalog: str = "iceberg") -> list[dict]:
    """Evolve and deduplicate legacy tables before the new MERGE writers run."""

    actions = []
    for migration in LEGACY_TABLE_MIGRATIONS:
        qualified = _qualified_legacy(migration, catalog)
        if not manager.table_exists("bronze", migration.name):
            raise SofaScoreBootstrapError(
                f"legacy source table is missing: {qualified}"
            )
        columns: Mapping[str, object] = manager.get_table_columns(
            "bronze", migration.name
        )
        altered = False
        if migration.normalized_column not in columns:
            manager._execute(
                f"ALTER TABLE {qualified} ADD COLUMN "
                f"{_quoted_identifier(migration.normalized_column)} "
                f"{migration.normalized_type}"
            )
            altered = True
            columns = manager.get_table_columns("bronze", migration.name)
        observed_type = columns.get(migration.normalized_column)
        if _normalize_type(observed_type) != _normalize_type(migration.normalized_type):
            raise SofaScoreBootstrapError(
                f"{qualified}.{migration.normalized_column} has type "
                f"{observed_type!r}, expected {migration.normalized_type}"
            )
        missing_key_columns = sorted(set(migration.natural_key) - set(columns))
        if missing_key_columns:
            raise SofaScoreBootstrapError(
                f"{qualified} lacks natural-key columns: "
                + ", ".join(missing_key_columns)
            )
        normalization = _normalization_sql(
            migration,
            columns,
            catalog=catalog,
        )
        if normalization is not None:
            manager._execute(normalization)
        null_keys = _scalar_count(
            manager,
            _null_natural_key_sql(migration, catalog=catalog),
        )
        if null_keys:
            raise SofaScoreBootstrapError(
                f"{qualified} still has {null_keys} null/blank natural keys"
            )
        duplicate_keys = _scalar_count(
            manager,
            _duplicate_natural_key_sql(migration, catalog=catalog),
        )
        rewritten = duplicate_keys > 0
        if rewritten:
            manager._execute(_dedup_rewrite_sql(migration, columns, catalog=catalog))
        manager._execute(
            f"COMMENT ON TABLE {qualified} IS {_sql_string(migration.comment)}"
        )
        actions.append(
            {
                "table": f"bronze.{migration.name}",
                "altered": altered,
                "normalized": normalization is not None,
                "duplicate_keys_before": duplicate_keys,
                "rewritten": rewritten,
            }
        )
    return actions


def _normalize_type(value: object) -> str:
    return re.sub(r"\s+", "", str(value).strip().casefold())


def _show_create(manager, qualified: str) -> str:
    rows = manager._execute(f"SHOW CREATE TABLE {qualified}", fetch=True)
    if not rows or not rows[0] or not isinstance(rows[0][0], str):
        raise SofaScoreBootstrapError(
            f"SHOW CREATE TABLE returned no DDL for {qualified}"
        )
    return rows[0][0]


def _declared_partitioning(ddl: str) -> tuple[str, ...]:
    match = re.search(
        r"partitioning\s*=\s*ARRAY\s*\[(.*?)\]",
        ddl,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ()
    return tuple(re.findall(r"'([^']+)'", match.group(1)))


def _preflight_legacy(manager, *, catalog: str) -> list[dict]:
    results = []
    for migration in LEGACY_TABLE_MIGRATIONS:
        qualified = _qualified_legacy(migration, catalog)
        result = {
            "table": f"bronze.{migration.name}",
            "exists": False,
            "missing_natural_key_columns": [],
            "normalized_column_type_ok": False,
            "null_natural_keys": None,
            "duplicate_natural_keys": None,
            "natural_key_comment_ok": False,
            "ready": False,
        }
        if not manager.table_exists("bronze", migration.name):
            results.append(result)
            continue
        result["exists"] = True
        columns: Mapping[str, object] = manager.get_table_columns(
            "bronze", migration.name
        )
        result["missing_natural_key_columns"] = sorted(
            set(migration.natural_key) - set(columns)
        )
        result["normalized_column_type_ok"] = _normalize_type(
            columns.get(migration.normalized_column)
        ) == _normalize_type(migration.normalized_type)
        if not result["missing_natural_key_columns"]:
            result["null_natural_keys"] = _scalar_count(
                manager,
                _null_natural_key_sql(migration, catalog=catalog),
            )
            result["duplicate_natural_keys"] = _scalar_count(
                manager,
                _duplicate_natural_key_sql(migration, catalog=catalog),
            )
        ddl = _show_create(manager, qualified)
        result["natural_key_comment_ok"] = migration.comment in ddl
        result["ready"] = bool(
            not result["missing_natural_key_columns"]
            and result["normalized_column_type_ok"]
            and result["null_natural_keys"] == 0
            and result["duplicate_natural_keys"] == 0
            and result["natural_key_comment_ok"]
        )
        results.append(result)
    return results


def preflight(manager, *, catalog: str = "iceberg") -> dict:
    """Read-only verification of every table contract."""

    _identifier(catalog, "catalog")
    table_results = []
    for table in BOOTSTRAP_TABLES:
        qualified = _qualified(table, catalog)
        result = {
            "table": f"{table.schema}.{table.name}",
            "exists": False,
            "missing_columns": [],
            "type_mismatches": {},
            "partitioning_ok": False,
            "natural_key_comment_ok": False,
            "ready": False,
        }
        if not manager.table_exists(table.schema, table.name):
            table_results.append(result)
            continue
        result["exists"] = True
        observed: Mapping[str, object] = manager.get_table_columns(
            table.schema, table.name
        )
        expected = {column.name: column.sql_type for column in table.columns}
        result["missing_columns"] = sorted(set(expected) - set(observed))
        result["type_mismatches"] = {
            name: {"expected": expected[name], "observed": str(observed[name])}
            for name in sorted(set(expected) & set(observed))
            if _normalize_type(expected[name]) != _normalize_type(observed[name])
        }
        ddl = _show_create(manager, qualified)
        result["partitioning_ok"] = (
            _declared_partitioning(ddl) == table.partition_columns
        )
        result["natural_key_comment_ok"] = table.comment in ddl
        result["ready"] = not (
            result["missing_columns"]
            or result["type_mismatches"]
            or not result["partitioning_ok"]
            or not result["natural_key_comment_ok"]
        )
        table_results.append(result)
    legacy_results = _preflight_legacy(manager, catalog=catalog)
    ready = all(result["ready"] for result in table_results) and all(
        result["ready"] for result in legacy_results
    )
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "mode": "preflight",
        "ready": ready,
        "status": "ready" if ready else "not_ready",
        "tables": table_results,
        "legacy_tables": legacy_results,
    }


def apply_bootstrap(manager, *, catalog: str = "iceberg") -> dict:
    """Apply idempotent empty-table DDL, then fail if preflight is not green."""

    for statement in bootstrap_sql(catalog=catalog):
        manager._execute(statement)
    legacy_actions = apply_legacy_migrations(manager, catalog=catalog)
    report = preflight(manager, catalog=catalog)
    report["mode"] = "apply"
    report["legacy_actions"] = legacy_actions
    if not report["ready"]:
        failed = [
            item["table"]
            for item in (*report["tables"], *report["legacy_tables"])
            if not item["ready"]
        ]
        raise SofaScoreBootstrapError(
            "bootstrap preflight failed after apply: " + ", ".join(failed)
        )
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap empty SofaScore production Iceberg contracts",
    )
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--dry-run", action="store_true")
    actions.add_argument("--preflight", action="store_true")
    actions.add_argument("--apply", action="store_true")
    parser.add_argument("--catalog", default="iceberg")
    return parser


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    manager_factory: Optional[Callable[..., object]] = None,
) -> int:
    args = _parser().parse_args(argv)
    try:
        _identifier(args.catalog, "catalog")
        if not args.preflight and not args.apply:
            print(
                json.dumps(
                    {
                        "bootstrap_version": BOOTSTRAP_VERSION,
                        "mode": "dry_run",
                        "mutates": False,
                        "tables": [
                            f"{table.schema}.{table.name}" for table in BOOTSTRAP_TABLES
                        ],
                        "statements": bootstrap_sql(catalog=args.catalog),
                        "legacy_migrations": legacy_dry_run_steps(catalog=args.catalog),
                    },
                    sort_keys=True,
                )
            )
            return 0

        if manager_factory is None:
            from scrapers.base.trino_manager import TrinoTableManager

            manager_factory = TrinoTableManager
        manager = manager_factory(catalog=args.catalog)
        try:
            report = (
                apply_bootstrap(manager, catalog=args.catalog)
                if args.apply
                else preflight(manager, catalog=args.catalog)
            )
        finally:
            close = getattr(manager, "close", None)
            if callable(close):
                close()
        print(json.dumps(report, sort_keys=True))
        return 0 if report["ready"] else 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "bootstrap_version": BOOTSTRAP_VERSION,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
