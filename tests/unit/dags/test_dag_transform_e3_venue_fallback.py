"""Unit tests for dag_transform_e3 graceful-degrade of optional Bronze (#812).

bronze.sofascore_venue is optional (#753) — when absent, the silver_e3
`sofascore_venue` build must fall back to an empty-schema SQL instead of
failing on TABLE_NOT_FOUND, which previously cascaded to the whole Gold layer
(gold_e3 here AND dag_transform_fbref_gold's dim_venue).

Airflow is stubbed by tests/unit/dags/conftest.py. `_run_silver_e3` does a
lazy `from utils.silver_tasks import ...`, so we inject a fake module to spy on
which SQL file actually reaches run_silver_transform.
"""
from __future__ import annotations

import importlib
import sys
import types

import pytest


def _reload_e3():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_e3", None)
    sys.modules.pop("dags.dag_transform_e3", None)
    return importlib.import_module("dag_transform_e3")


@pytest.fixture
def e3():
    return _reload_e3()


def _fake_silver_tasks(bronze_exists: bool, calls: list):
    """Fake utils.silver_tasks capturing the sql_file run_silver_transform gets."""
    mod = types.ModuleType("utils.silver_tasks")

    def check_bronze_table_exists(table_name, schema="bronze", **kw):
        return bronze_exists

    def run_silver_transform(sql_file, table_name, schema="silver", **kw):
        calls.append(sql_file)
        return {"rows": 0, "table": f"iceberg.{schema}.{table_name}"}

    mod.check_bronze_table_exists = check_bronze_table_exists
    mod.run_silver_transform = run_silver_transform
    return mod


class TestVenueFallbackConfig:
    def test_fallback_map_points_to_empty_sql(self, e3):
        fb = e3.SILVER_E3_FALLBACKS["sofascore_venue"]
        assert fb["fallback_sql_file"] == "dags/sql/silver/sofascore_venue_empty.sql"
        assert fb["require_bronze"] == ["sofascore_venue"]

    def test_spadl_uses_partition_staged_runner(self, e3):
        version_sql = e3.WHOSCORED_EVENTS_SOURCE_VERSION_SQL
        config = e3.SILVER_E3_PARTITION_STAGED["whoscored_events_spadl"]
        assert config == {
            "partition_source_table":
                "iceberg.bronze.whoscored_events_current",
            "source_version_sql": version_sql,
        }
        assert version_sql == (
            "SELECT league, season, game_id, batch_id\n"
            "FROM iceberg.bronze.whoscored_match_ingest_latest_success\n"
            "ORDER BY league, season, game_id, batch_id"
        )

        from airflow.operators.python import PythonOperator

        task = next(
            op for op in PythonOperator._instances
            if op.task_id == "silver_e3.whoscored_events_spadl"
        )
        assert task.op_kwargs["partition_source_table"] == (
            "iceberg.bronze.whoscored_events_current"
        )
        assert task.op_kwargs["source_version_sql"] == version_sql
        timeout = e3.SILVER_E3_TASK_OVERRIDES["whoscored_events_spadl"][
            "execution_timeout"
        ]
        assert timeout.total_seconds() == 8 * 60 * 60


class TestRunSilverE3GracefulDegrade:
    def test_missing_bronze_uses_fallback_sql(self, e3, monkeypatch):
        calls = []
        monkeypatch.setitem(
            sys.modules, "utils.silver_tasks",
            _fake_silver_tasks(bronze_exists=False, calls=calls),
        )
        e3._run_silver_e3(
            sql_file="dags/sql/silver/sofascore_venue.sql",
            table_name="sofascore_venue",
            require_bronze=["sofascore_venue"],
            fallback_sql_file="dags/sql/silver/sofascore_venue_empty.sql",
        )
        assert calls == ["dags/sql/silver/sofascore_venue_empty.sql"]

    def test_present_bronze_uses_real_sql(self, e3, monkeypatch):
        calls = []
        monkeypatch.setitem(
            sys.modules, "utils.silver_tasks",
            _fake_silver_tasks(bronze_exists=True, calls=calls),
        )
        e3._run_silver_e3(
            sql_file="dags/sql/silver/sofascore_venue.sql",
            table_name="sofascore_venue",
            require_bronze=["sofascore_venue"],
            fallback_sql_file="dags/sql/silver/sofascore_venue_empty.sql",
        )
        assert calls == ["dags/sql/silver/sofascore_venue.sql"]

    def test_table_without_fallback_runs_its_own_sql(self, e3, monkeypatch):
        # A normal silver table (no require_bronze) must run unchanged even if
        # the bronze-exists probe would say False.
        calls = []
        monkeypatch.setitem(
            sys.modules, "utils.silver_tasks",
            _fake_silver_tasks(bronze_exists=False, calls=calls),
        )
        e3._run_silver_e3(
            sql_file="dags/sql/silver/understat_team_match.sql",
            table_name="understat_team_match",
        )
        assert calls == ["dags/sql/silver/understat_team_match.sql"]

    def test_spadl_routes_only_to_partition_staged_runner(self, e3, monkeypatch):
        regular_calls = []
        staged_calls = []
        fake = _fake_silver_tasks(bronze_exists=True, calls=regular_calls)

        def run_staged(
            sql_file,
            table_name,
            source_table,
            source_version_sql,
            schema="silver",
            **kw,
        ):
            staged_calls.append((
                sql_file,
                table_name,
                source_table,
                source_version_sql,
                schema,
            ))
            return {"rows": 28, "table": f"iceberg.{schema}.{table_name}"}

        fake.run_silver_transform_partition_staged = run_staged
        monkeypatch.setitem(sys.modules, "utils.silver_tasks", fake)

        result = e3._run_silver_e3(
            sql_file="dags/sql/silver/whoscored_events_spadl.sql",
            table_name="whoscored_events_spadl",
            partition_source_table=(
                "iceberg.bronze.whoscored_events_current"
            ),
            source_version_sql=e3.WHOSCORED_EVENTS_SOURCE_VERSION_SQL,
        )

        assert regular_calls == []
        assert staged_calls == [(
            "dags/sql/silver/whoscored_events_spadl.sql",
            "whoscored_events_spadl",
            "iceberg.bronze.whoscored_events_current",
            e3.WHOSCORED_EVENTS_SOURCE_VERSION_SQL,
            "silver",
        )]
        assert result["rows"] == 28

    def test_partition_staged_route_requires_source_version(self, e3, monkeypatch):
        fake = _fake_silver_tasks(bronze_exists=True, calls=[])
        fake.run_silver_transform_partition_staged = lambda **kw: None
        monkeypatch.setitem(sys.modules, "utils.silver_tasks", fake)

        with pytest.raises(ValueError, match="requires source_version_sql"):
            e3._run_silver_e3(
                sql_file="dags/sql/silver/whoscored_events_spadl.sql",
                table_name="whoscored_events_spadl",
                partition_source_table=(
                    "iceberg.bronze.whoscored_events_current"
                ),
            )
