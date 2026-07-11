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
        # leagues: #920 Phase 2 — schedule is validated per league over
        # WHOSCORED_LEAGUES; events stays a whole-table wipe-floor (None).
        "wrapper, expected_table, expected_key, per_league",
        [
            ("validate_schedule", "whoscored_schedule", "whoscored_schedule", True),
            ("validate_events", "whoscored_events_current", "whoscored_events", False),
        ],
    )
    def test_wrapper_passes_table_and_threshold_key(
        self, monkeypatch, wrapper, expected_table, expected_key, per_league
    ):
        mod = _load_dag_module()
        calls = []

        monkeypatch.setattr(
            mod,
            "validate_table",
            lambda table, key, leagues=None: calls.append(
                (table, key, leagues)
            ) or {"table": table},
        )

        getattr(mod, wrapper)()

        from utils.config import WHOSCORED_LEAGUES
        expected_leagues = WHOSCORED_LEAGUES if per_league else None
        assert calls == [(expected_table, expected_key, expected_leagues)]

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
            "unbatched_payload_rows": 0,
        }
        monkeypatch.setattr(
            mod, "_manifest_integrity_summary", lambda: integrity
        )

        result = mod.validate_match_ingest_manifest()

        assert result["table"] == "whoscored_match_ingest_manifest"
        assert result["integrity"] == integrity

    @pytest.mark.unit
    def test_preview_manifest_accepts_valid_zero_row_snapshots(self, monkeypatch):
        mod = _load_dag_module()
        calls = []
        monkeypatch.setattr(
            mod, "bronze_count", lambda table: calls.append(table) or 0
        )
        integrity = {
            "successful_games": 1,
            "invalid_states": 0,
            "invalid_success_rows": 0,
            "count_mismatches": 0,
            "null_batch_rows": 0,
        }
        monkeypatch.setattr(
            mod, "_preview_manifest_integrity_summary", lambda: integrity
        )

        result = mod.validate_preview_ingest_manifest()

        assert calls == [
            "whoscored_preview_ingest_manifest",
            "whoscored_missing_players_current",
        ]
        assert result["rows"] == 0
        assert result["integrity"] == integrity

    @pytest.mark.unit
    def test_preview_manifest_rejects_null_physical_batch_ids(self, monkeypatch):
        mod = _load_dag_module()
        monkeypatch.setattr(mod, "bronze_count", lambda _table: 1)
        monkeypatch.setattr(
            mod,
            "_preview_manifest_integrity_summary",
            lambda: {
                "successful_games": 1,
                "invalid_states": 0,
                "invalid_success_rows": 0,
                "count_mismatches": 0,
                "null_batch_rows": 2,
            },
        )

        with pytest.raises(mod.AirflowException, match="integrity violations"):
            mod.validate_preview_ingest_manifest()


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
            assert "--flaresolverr-url" not in task.bash_command
            assert task._init_kwargs["trigger_rule"] == "all_success"
            # Raw-store credentials and optional paid-proxy endpoints come
            # only from the deployment environment (compose/secrets). The DAG
            # must not invent rolling-deploy fallbacks that silently enable a
            # proxy or point at a hard-coded warehouse.
            assert "WHOSCORED_RAW_STORE_URI" not in task.env
            assert "WHOSCORED_PAID_PROXY_URL" not in task.env
            assert "WHOSCORED_PROXY_CONTROL_URL" not in task.env
            assert task._init_kwargs["append_env"] is True

    @pytest.mark.unit
    def test_browser_pool_and_manifest_backoff_is_not_masked_by_airflow_retry(self):
        mod = _load_dag_module()
        assert mod.WHOSCORED_ARGS["pool"] == "ingest_scraper_pool"
        assert mod.WHOSCORED_ARGS["retries"] == 0
        assert "retry_delay" not in mod.WHOSCORED_ARGS

    @pytest.mark.unit
    def test_profile_task_uses_all_active_scopes_with_one_global_limit(self):
        mod = _load_dag_module()
        from airflow.operators.bash import BashOperator

        profile_task = next(
            task
            for task in BashOperator._instances
            if task.task_id == "refresh_whoscored_profiles"
        )
        command = profile_task.bash_command
        assert "run_whoscored_scraper.py profiles" in command
        assert command.count("--scope ") == len(mod.ACTIVE_WHOSCORED_SCOPES)
        for scope in mod.ACTIVE_WHOSCORED_SCOPES:
            assert f'--scope "{scope}"' in command
        assert command.count("--limit 200") == 1
        assert "--flaresolverr-url" not in command
        assert profile_task._init_kwargs["retries"] == 0
