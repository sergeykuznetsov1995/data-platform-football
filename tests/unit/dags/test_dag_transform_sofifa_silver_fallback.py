"""Unit tests for the resilient SoFIFA Silver build (issue #180).

bronze.sofifa_player_ratings periodically vanishes (Cloudflare Turnstile). The
DAG must then build an empty-but-typed fallback instead of failing the
DROP-then-CREATE CTAS, and relax the row-count floor for that run. When Bronze
returns, the normal path resumes automatically.

These tests pin that contract on the DAG callables ``_run_transform`` and
``_validate_silver`` (Airflow APIs are stubbed by tests/unit/dags/conftest.py;
the heavy Trino helpers are monkeypatched).
"""

from __future__ import annotations

import importlib
import sys

import pytest

pytestmark = pytest.mark.unit


def _load_dag_module():
    sys.modules.pop("dag_transform_sofifa_silver", None)
    return importlib.import_module("dag_transform_sofifa_silver")


@pytest.fixture
def mod():
    return _load_dag_module()


class _Ti:
    """Minimal TaskInstance stub exposing xcom_pull."""

    def __init__(self, pulled):
        self._pulled = pulled

    def xcom_pull(self, task_ids=None):
        return self._pulled


class TestRunTransformFallback:

    def test_missing_bronze_builds_empty_fallback(self, mod, monkeypatch):
        """bronze absent -> empty fallback SQL, result tagged fallback=True."""
        calls = {}

        def fake_check(table_name, schema="bronze", **kw):
            calls["check"] = (table_name, schema)
            return False  # bronze missing

        def fake_run(sql_file, table_name, schema, **kw):
            calls["run"] = {"sql_file": sql_file, "table_name": table_name, "schema": schema}
            return {"table": f"iceberg.{schema}.{table_name}", "rows": 0, "status": "success"}

        monkeypatch.setattr("utils.silver_tasks.check_bronze_table_exists", fake_check)
        monkeypatch.setattr("utils.silver_tasks.run_silver_transform", fake_run)

        result = mod._run_transform(
            sql_file="dags/sql/silver/sofifa_player_profile.sql",
            table_name="sofifa_player_profile",
        )

        assert calls["check"] == (mod.SOFIFA_REQUIRE_BRONZE, "bronze")
        # Built from the EMPTY fallback, not the real SQL.
        assert calls["run"]["sql_file"] == mod.SOFIFA_FALLBACK_SQL
        assert calls["run"]["table_name"] == "sofifa_player_profile"
        assert result["fallback"] is True
        assert "fallback_reason" in result

    def test_present_bronze_builds_real_sql(self, mod, monkeypatch):
        """bronze present -> real SQL, no fallback flag."""
        calls = {}

        def fake_check(table_name, schema="bronze", **kw):
            return True  # bronze present

        def fake_run(sql_file, table_name, schema, **kw):
            calls["run"] = {"sql_file": sql_file}
            return {"table": f"iceberg.{schema}.{table_name}", "rows": 546, "status": "success"}

        monkeypatch.setattr("utils.silver_tasks.check_bronze_table_exists", fake_check)
        monkeypatch.setattr("utils.silver_tasks.run_silver_transform", fake_run)

        result = mod._run_transform(
            sql_file="dags/sql/silver/sofifa_player_profile.sql",
            table_name="sofifa_player_profile",
        )

        assert calls["run"]["sql_file"] == "dags/sql/silver/sofifa_player_profile.sql"
        assert "fallback" not in result


class TestValidateSilverThreshold:

    def test_fallback_relaxes_row_floor(self, mod, monkeypatch):
        """When the transform fell back, the empty (0-row) table must pass:
        threshold for sofifa_player_profile -> 0 AND min_rows -> 0."""
        captured = {}

        def fake_validate(tables, min_rows):
            captured["tables"] = dict(tables)
            captured["min_rows"] = min_rows
            return {"status": "success", "warnings": [], "details": {"sofifa_player_profile": 0}}

        monkeypatch.setattr("utils.silver_tasks.validate_silver_tables", fake_validate)

        mod._validate_silver(ti=_Ti({"fallback": True, "rows": 0, "status": "success"}))

        assert captured["tables"]["sofifa_player_profile"] == 0
        assert captured["min_rows"] == 0

    def test_normal_path_keeps_strict_floor(self, mod, monkeypatch):
        """No fallback -> 100-row floor and min_rows=1 unchanged."""
        captured = {}

        def fake_validate(tables, min_rows):
            captured["tables"] = dict(tables)
            captured["min_rows"] = min_rows
            return {"status": "success", "warnings": [], "details": {"sofifa_player_profile": 546}}

        monkeypatch.setattr("utils.silver_tasks.validate_silver_tables", fake_validate)

        mod._validate_silver(ti=_Ti({"rows": 546, "status": "success"}))

        assert captured["tables"]["sofifa_player_profile"] == 100
        assert captured["min_rows"] == 1

    def test_warnings_raise(self, mod, monkeypatch):
        """A genuine row-count shortfall still raises (normal path)."""
        from airflow.exceptions import AirflowException

        def fake_validate(tables, min_rows):
            return {"status": "partial_success",
                    "warnings": ["sofifa_player_profile: 5 rows (expected >= 100)"],
                    "details": {"sofifa_player_profile": 5}}

        monkeypatch.setattr("utils.silver_tasks.validate_silver_tables", fake_validate)

        with pytest.raises(AirflowException, match="Silver validation FAILED"):
            mod._validate_silver(ti=_Ti({"rows": 5, "status": "success"}))
