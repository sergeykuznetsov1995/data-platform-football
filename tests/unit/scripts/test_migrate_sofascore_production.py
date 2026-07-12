from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scrapers.sofascore.adapters import MANIFEST_COLUMNS
from scrapers.sofascore.pipeline import build_event_spec
from scripts.migrate_sofascore_production import (
    BOOTSTRAP_TABLES,
    LEGACY_MATCH_STATS,
    LEGACY_STANDINGS,
    NEW_BRONZE_BOOTSTRAP_TABLES,
    OPS_MANIFEST,
    SOFASCORE_EVENTS,
    SOFASCORE_EVENT_PARTICIPANTS,
    SOFASCORE_INCIDENTS,
    SOFASCORE_LINEUPS,
    apply_bootstrap,
    apply_legacy_migrations,
    bootstrap_sql,
    main,
    preflight,
    render_create_table,
)


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


class FakeManager:
    def __init__(self, *, catalog="iceberg", ready=False, legacy=False):
        self.catalog = catalog
        self.executed = []
        self.closed = False
        self.tables = {}
        if ready:
            for table in BOOTSTRAP_TABLES:
                self._install(table)
            self._install_legacy(clean=True)
        elif legacy:
            self._install_legacy(clean=False)

    def _install(self, table):
        self.tables[(table.schema, table.name)] = {
            "columns": {column.name: column.sql_type for column in table.columns},
            "ddl": render_create_table(table, catalog=self.catalog),
        }

    def _install_legacy(self, *, clean):
        match_columns = {
            "league": "varchar",
            "season": "varchar",
            "match_id": "varchar",
            "period": "varchar",
            "stat_group": "varchar",
            "stat_key": "varchar",
            "key": "varchar",
            "statistics_type": "varchar",
            "stat_name": "varchar",
            "name": "varchar",
            "_ingested_at": "timestamp(6)",
            "_batch_id": "varchar",
        }
        if clean:
            match_columns["statistic_key"] = "varchar"
        for migration, columns in (
            (LEGACY_MATCH_STATS, match_columns),
            (
                LEGACY_STANDINGS,
                {
                    "league": "varchar",
                    "season": "varchar",
                    "group": "varchar",
                    "team": "varchar",
                    "_ingested_at": "timestamp(6)",
                    "_batch_id": "varchar",
                },
            ),
        ):
            self.tables[("bronze", migration.name)] = {
                "columns": columns,
                "ddl": (
                    f"CREATE TABLE {self.catalog}.bronze.{migration.name} "
                    "WITH (partitioning = ARRAY['league', 'season'])"
                    + (f" COMMENT '{migration.comment}'" if clean else "")
                ),
                "null_keys": 0 if clean else 4,
                "duplicate_keys": 0 if clean else 2,
            }

    def table_exists(self, schema, table):
        return (schema, table) in self.tables

    def get_table_columns(self, schema, table):
        return dict(self.tables[(schema, table)]["columns"])

    def _execute(self, sql, fetch=False, params=None):
        assert params is None
        self.executed.append(sql)
        if sql.startswith("CREATE TABLE IF NOT EXISTS"):
            for table in BOOTSTRAP_TABLES:
                qualified = f"{self.catalog}.{table.schema}.{table.name}"
                if sql.startswith(f"CREATE TABLE IF NOT EXISTS {qualified} "):
                    self._install(table)
                    break
        if sql.startswith("COMMENT ON TABLE"):
            for table in BOOTSTRAP_TABLES:
                qualified = f"{self.catalog}.{table.schema}.{table.name}"
                if sql.startswith(f"COMMENT ON TABLE {qualified} "):
                    self.tables[(table.schema, table.name)]["ddl"] = (
                        render_create_table(table, catalog=self.catalog)
                    )
                    break
            for migration in (LEGACY_MATCH_STATS, LEGACY_STANDINGS):
                qualified = f"{self.catalog}.bronze.{migration.name}"
                if sql.startswith(f"COMMENT ON TABLE {qualified} "):
                    self.tables[("bronze", migration.name)]["ddl"] += (
                        f" COMMENT '{migration.comment}'"
                    )
                    break
        if sql.startswith("ALTER TABLE") and "ADD COLUMN" in sql:
            self.tables[("bronze", LEGACY_MATCH_STATS.name)]["columns"][
                "statistic_key"
            ] = "varchar"
        if sql.startswith("UPDATE"):
            table = (
                LEGACY_MATCH_STATS.name
                if LEGACY_MATCH_STATS.name in sql
                else LEGACY_STANDINGS.name
            )
            self.tables[("bronze", table)]["null_keys"] = 0
        if sql.startswith("CREATE OR REPLACE TABLE"):
            table = (
                LEGACY_MATCH_STATS.name
                if LEGACY_MATCH_STATS.name in sql
                else LEGACY_STANDINGS.name
            )
            self.tables[("bronze", table)]["duplicate_keys"] = 0
        if sql.startswith("SELECT COUNT(*) FROM"):
            table = (
                LEGACY_MATCH_STATS.name
                if LEGACY_MATCH_STATS.name in sql
                else LEGACY_STANDINGS.name
            )
            kind = "duplicate_keys" if "duplicate_keys" in sql else "null_keys"
            return [[self.tables[("bronze", table)][kind]]]
        if sql.startswith("SHOW CREATE TABLE"):
            _, _, _, qualified = sql.split(maxsplit=3)
            _, schema, table = qualified.split(".")
            return [[self.tables[(schema, table)]["ddl"]]]
        return [] if fetch else None

    def close(self):
        self.closed = True


def _payload(endpoint: str) -> object:
    suffix = "" if endpoint == "event" else f"_{endpoint}"
    return json.loads((FIXTURES / f"sofascore_event_14023925{suffix}.json").read_text())


def test_bootstrap_inventory_matches_new_normalized_bronze_contracts():
    contract = yaml.safe_load(
        (
            Path(__file__).resolve().parents[3]
            / "configs/sofascore/endpoint_coverage.yaml"
        ).read_text(encoding="utf-8")
    )
    by_name = {
        f"{table.schema}.{table.name}": table for table in NEW_BRONZE_BOOTSTRAP_TABLES
    }
    assert set(by_name) == {
        "bronze.sofascore_events",
        "bronze.sofascore_event_participants",
        "bronze.sofascore_lineups",
        "bronze.sofascore_incidents",
        "bronze.sofascore_player_universe",
    }
    for name, table in by_name.items():
        declared = contract["tables"][name]
        assert table.natural_key == tuple(declared["natural_key"])
        assert table.partition_columns == tuple(declared["partition_columns"])
        assert declared["grain"] in table.grain or table.grain in declared["grain"]
        assert set(declared["required_columns"]) <= {
            column.name for column in table.columns
        }


def test_ops_manifest_schema_and_natural_key_match_runtime_adapter():
    assert tuple(column.name for column in OPS_MANIFEST.columns) == MANIFEST_COLUMNS
    assert OPS_MANIFEST.natural_key == (
        "source_tournament_id",
        "source_season_id",
        "target_type",
        "target_id",
        "endpoint",
        "freshness_key",
    )


@pytest.mark.parametrize(
    ("endpoint", "dataset", "table"),
    [
        ("event", "events", SOFASCORE_EVENTS),
        ("event", "event_participants", SOFASCORE_EVENT_PARTICIPANTS),
        ("lineups", "lineups", SOFASCORE_LINEUPS),
        ("incidents", "incidents", SOFASCORE_INCIDENTS),
    ],
)
def test_explicit_bootstrap_schema_contains_every_saved_fixture_field(
    endpoint,
    dataset,
    table,
):
    spec = build_event_spec(
        source_tournament_id=17,
        source_season_id=76986,
        target_id=14023925,
        endpoint=endpoint,
        freshness_key="fixture",
        paid_proxy=False,
    )
    rows = spec.parsers[dataset](_payload(endpoint))
    fixture_columns = {key for row in rows for key in row}
    declared_columns = {column.name for column in table.columns}
    assert fixture_columns <= declared_columns


def test_rendered_ddl_is_idempotent_partitioned_and_documents_natural_key():
    ddl = render_create_table(SOFASCORE_EVENTS)
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS iceberg.bronze.sofascore_events")
    assert "partitioning = ARRAY['league', 'season']" in ddl
    assert SOFASCORE_EVENTS.comment in ddl
    assert "DROP TABLE" not in ddl
    assert "INSERT INTO" not in ddl


def test_preflight_is_read_only_and_reports_missing_tables():
    manager = FakeManager()
    report = preflight(manager)
    assert report["ready"] is False
    assert {item["table"] for item in report["tables"] if not item["exists"]} == {
        f"{table.schema}.{table.name}" for table in BOOTSTRAP_TABLES
    }
    assert manager.executed == []


def test_preflight_rejects_type_partition_and_comment_drift():
    manager = FakeManager(ready=True)
    key = ("bronze", "sofascore_events")
    manager.tables[key]["columns"]["season_id"] = "varchar"
    manager.tables[key]["ddl"] = (
        manager.tables[key]["ddl"]
        .replace(
            "partitioning = ARRAY['league', 'season']",
            "partitioning = ARRAY['league']",
        )
        .replace(SOFASCORE_EVENTS.comment, "wrong comment")
    )

    report = preflight(manager)
    event = next(
        item for item in report["tables"] if item["table"] == "bronze.sofascore_events"
    )
    assert event["ready"] is False
    assert event["type_mismatches"]["season_id"] == {
        "expected": "bigint",
        "observed": "varchar",
    }
    assert event["partitioning_ok"] is False
    assert event["natural_key_comment_ok"] is False


def test_legacy_migration_backfills_exact_keys_and_atomically_deduplicates():
    manager = FakeManager(legacy=True)
    actions = apply_legacy_migrations(manager)
    joined = "\n".join(manager.executed)

    assert actions == [
        {
            "table": "bronze.sofascore_match_stats",
            "altered": True,
            "normalized": True,
            "duplicate_keys_before": 2,
            "rewritten": True,
        },
        {
            "table": "bronze.sofascore_league_table",
            "altered": False,
            "normalized": True,
            "duplicate_keys_before": 2,
            "rewritten": True,
        },
    ]
    assert 'ADD COLUMN "statistic_key" varchar' in joined
    assert (
        "COALESCE(NULLIF(TRIM(CAST(\"stat_key\" AS varchar)), ''), "
        "NULLIF(TRIM(CAST(\"key\" AS varchar)), ''), "
        "NULLIF(TRIM(CAST(\"statistics_type\" AS varchar)), ''), "
        "NULLIF(TRIM(CAST(\"stat_name\" AS varchar)), ''), "
        "NULLIF(TRIM(CAST(\"name\" AS varchar)), ''))"
    ) in joined
    assert "SET \"group\" = '__total__'" in joined
    assert joined.count("CREATE OR REPLACE TABLE") == 2
    assert "ROW_NUMBER() OVER" in joined


def test_legacy_preflight_reports_null_and_duplicate_natural_keys():
    manager = FakeManager(ready=True)
    key = ("bronze", LEGACY_STANDINGS.name)
    manager.tables[key]["null_keys"] = 3
    manager.tables[key]["duplicate_keys"] = 1
    report = preflight(manager)
    standings = next(
        item
        for item in report["legacy_tables"]
        if item["table"] == "bronze.sofascore_league_table"
    )
    assert standings["ready"] is False
    assert standings["null_natural_keys"] == 3
    assert standings["duplicate_natural_keys"] == 1
    assert report["ready"] is False


def test_apply_is_idempotent_and_uses_only_create_or_comment_ddl():
    manager = FakeManager(legacy=True)
    first = apply_bootstrap(manager)
    first_statements = tuple(manager.executed)
    second = apply_bootstrap(manager)
    second_statements = tuple(manager.executed[len(first_statements) :])

    assert first["ready"] is True
    assert second["ready"] is True
    assert first_statements != second_statements
    assert (
        tuple(statement for statement in first_statements[: len(bootstrap_sql())])
        == bootstrap_sql()
    )
    assert any(statement.startswith("ALTER TABLE") for statement in first_statements)
    assert any(
        statement.startswith("CREATE OR REPLACE TABLE")
        for statement in first_statements
    )
    assert not any(
        statement.startswith("ALTER TABLE") for statement in second_statements
    )
    assert not any(
        statement.startswith("CREATE OR REPLACE TABLE")
        for statement in second_statements
    )


def test_cli_defaults_to_connection_free_dry_run(capsys):
    def forbidden_factory(**kwargs):
        raise AssertionError(f"dry-run opened Trino: {kwargs}")

    assert main([], manager_factory=forbidden_factory) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "dry_run"
    assert report["mutates"] is False
    assert len(report["tables"]) == len(BOOTSTRAP_TABLES)


def test_cli_preflight_returns_two_when_migration_is_required(capsys):
    manager = FakeManager()
    assert main(["--preflight"], manager_factory=lambda **_: manager) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "not_ready"
    assert manager.closed is True
