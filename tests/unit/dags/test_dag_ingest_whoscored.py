"""
Unit tests for ``dags/dag_ingest_whoscored.py`` validation wrappers.

The fail-closed validator itself was extracted to
``dags/utils/bronze_validation.py`` (issue #466) and is covered by
``test_bronze_validation.py``. What's left to pin here is the wrapper
contract: each validate task must call ``validate_table`` with the right
``(table_name, threshold_key)`` pair — a fat-fingered key would otherwise
surface only at DAG runtime as a missing-key AirflowException.

These tests run on the host using the lightweight Airflow stubs from
``tests/unit/dags/conftest.py``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def _use_real_competition_catalog(real_medallion_config_dir):
    """The DAG resolves active canonical scopes at import time."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    yield


def _load_dag_module():
    sys.modules.pop("dag_ingest_whoscored", None)
    sys.modules.pop("dags.dag_ingest_whoscored", None)
    return importlib.import_module("dag_ingest_whoscored")


class TestValidateWrappers:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "wrapper, expected_table",
        [
            ("validate_schedule", "whoscored_schedule"),
            ("validate_events", "whoscored_events_current"),
        ],
    )
    def test_wrapper_passes_table_and_threshold_key(
        self, monkeypatch, wrapper, expected_table
    ):
        mod = _load_dag_module()
        calls = []

        monkeypatch.setattr(
            mod,
            "validate_table",
            lambda table, key: calls.append((table, key)) or {"table": table},
        )

        getattr(mod, wrapper)()

        expected_key = (
            "whoscored_events" if expected_table == "whoscored_events_current"
            else expected_table
        )
        assert calls == [(expected_table, expected_key)]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "wrapper, expected_table",
        [
            ("validate_lineups", "whoscored_lineups_current"),
        ],
    )
    def test_v2_presence_guards_are_mandatory(
        self, monkeypatch, wrapper, expected_table
    ):
        mod = _load_dag_module()
        calls = []
        monkeypatch.setattr(
            mod,
            "bronze_count",
            lambda table: calls.append(table) or 1,
        )

        result = getattr(mod, wrapper)()

        assert calls == [expected_table]
        assert result == {"table": expected_table, "rows": 1, "threshold": 1}

    @pytest.mark.unit
    def test_manifest_validator_checks_commit_counts(self, monkeypatch):
        mod = _load_dag_module()
        monkeypatch.setattr(mod, "bronze_count", lambda _table: 10)
        integrity = {
            "successful_games": 5,
            "invalid_states": 0,
            "invalid_success_rows": 0,
            "count_mismatches": 0,
        }
        monkeypatch.setattr(
            mod, "_manifest_integrity_summary", lambda: integrity
        )

        result = mod.validate_match_ingest_manifest()

        assert result["table"] == "whoscored_match_ingest_manifest"
        assert result["integrity"] == integrity


class TestCanonicalDailyScope:
    @pytest.mark.unit
    def test_split_year_active_start_is_canonicalised(self):
        mod = _load_dag_module()
        assert mod._canonical_season_id("ENG-Premier League", 2025) == "2526"

    @pytest.mark.unit
    def test_single_year_scope_is_not_rewritten(self):
        mod = _load_dag_module()
        assert mod._canonical_season_id("INT-World Cup", 2026) == "2026"

    @pytest.mark.unit
    def test_new_dag_commands_use_explicit_scopes_only(self):
        mod = _load_dag_module()
        from airflow.operators.bash import BashOperator

        tasks = [
            task
            for task in BashOperator._instances
            if task.task_id.startswith("scrape_whoscored_")
        ]
        assert len(tasks) == len(mod.ACTIVE_WHOSCORED_SCOPES)
        assert tasks
        for task in tasks:
            assert "run_whoscored_scraper.py all" in task.bash_command
            assert "--scope " in task.bash_command
            assert "--leagues" not in task.bash_command
            assert "--seasons" not in task.bash_command
            assert "--proxy-file" not in task.bash_command
            assert task._init_kwargs["trigger_rule"] == "all_success"
            assert task.env["PROXY_FILTER_URL"] == ""
            assert task.env["WHOSCORED_PAID_PROXY_URL"] == (
                "http://proxy_filter:8900"
            )
            assert task.env["WHOSCORED_PROXY_CONTROL_URL"] == (
                "http://proxy_filter:8899"
            )
            assert task.env["WHOSCORED_RAW_STORE_URI"].endswith(
                "/raw/whoscored"
            )

    @pytest.mark.unit
    def test_browser_pool_and_bounded_airflow_retry(self):
        mod = _load_dag_module()
        assert mod.WHOSCORED_ARGS["pool"] == "ingest_scraper_pool"
        assert mod.WHOSCORED_ARGS["retries"] == 1
