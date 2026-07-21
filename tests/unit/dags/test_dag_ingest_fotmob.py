"""Unit tests for the source-native FotMob ingestion DAG.

The host uses lightweight Airflow stubs. Tests cover environment inheritance,
exact-scope params, one schedule owner, run-specific reports, the dedicated
HTTP pool and fail-closed native validation.
"""

from __future__ import annotations

import importlib
import sys

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _reload_dag_module():
    """Force a fresh import of the FotMob DAG module so each test sees
    a clean ``BashOperator._instances`` list (the stubbed BashOperator
    registers every constructed instance globally for inspection)."""
    from pathlib import Path

    from airflow.operators.bash import BashOperator  # stub

    BashOperator._instances.clear()

    # #920 Phase 1: the DAG module now calls is_single_year_competition() at
    # import time to build its task graph — point CONFIG_DIR at the real
    # shipped configs/medallion (on the host, it otherwise defaults to
    # /opt/airflow/configs/medallion, which only exists in the container).
    # CONFIG_DIR is resolved once at medallion_config import time, so patch
    # the module attribute directly (mirrors
    # tests/unit/sql/test_dim_competition_render.py).
    from utils import medallion_config

    medallion_config.CONFIG_DIR = (
        Path(__file__).resolve().parents[3] / "configs" / "medallion"
    )
    medallion_config.reset_cache()

    # Drop cached module so DAG body re-executes (and re-creates operators)
    sys.modules.pop("dag_ingest_fotmob", None)
    sys.modules.pop("dags.dag_ingest_fotmob", None)

    # The DAG file lives in /root/data_platform/dags/ which is on sys.path
    # via the conftest.
    return importlib.import_module("dag_ingest_fotmob")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestFotmobBashOperatorEnv:
    """Regression coverage for the append_env=True fix."""

    @pytest.mark.unit
    def test_dag_loads_without_errors(self):
        """The DAG module should import cleanly under the stubbed Airflow."""
        mod = _reload_dag_module()

        # validate_data is a plain function — it should still be exported
        assert hasattr(mod, "validate_data")
        assert callable(mod.validate_data)

    @pytest.mark.unit
    def test_scrape_task_has_append_env_true(self):
        """``scrape_fotmob_data`` BashOperator MUST set append_env=True.

        Without append_env, the explicit ``env`` dict completely replaces
        the parent process environment, dropping TRINO_PASSWORD,
        FLARESOLVERR_URL, etc. and breaking the scraper.
        """
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        scrape_tasks = [
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        ]
        assert len(scrape_tasks) == 1, (
            f"Expected exactly one scrape_fotmob_data BashOperator, "
            f"found {len(scrape_tasks)}: "
            f"{[op.task_id for op in BashOperator._instances]}"
        )
        task = scrape_tasks[0]

        assert task.append_env is True, (
            "scrape_fotmob_data BashOperator must set append_env=True so "
            "the parent Airflow env (TRINO_PASSWORD, PYTHONPATH, ...) "
            "leaks through to the subprocess."
        )

    @pytest.mark.unit
    def test_scrape_task_env_still_overrides_pythonpath(self):
        """append_env=True doesn't mean we lose explicit overrides — the
        ``env`` dict should still contain the expected PYTHONPATH/PATH/HOME
        keys, otherwise the subprocess won't find scrapers/."""
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )

        assert task.env is not None, "explicit env dict must be set"
        assert "PYTHONPATH" in task.env
        assert "/opt/airflow" in task.env["PYTHONPATH"]


class TestFotmobNativeParams:
    """Native runs use exact FotMob scopes instead of calculated years."""

    @pytest.mark.unit
    def test_mode_and_scope_are_ui_configurable_params(self):
        mod = _reload_dag_module()

        from airflow.models.param import Param  # stub

        params = mod.dag._dag_kwargs["params"]
        assert isinstance(params["mode"], Param)
        assert params["mode"].default == "daily"
        assert isinstance(params["scope"], Param)
        assert params["scope"].default == ""
        assert "season" not in params

    @pytest.mark.unit
    def test_scrape_command_renders_native_mode_and_exact_scope(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )

        assert '--mode "{{ params.mode }}"' in task.bash_command
        assert '--scope "{{ params.scope }}"' in task.bash_command
        assert "--season " not in task.bash_command
        assert "--leagues " not in task.bash_command


class TestDynamicDiscoveryDag:
    """One native task dynamically discovers club and tournament identities."""

    @pytest.mark.unit
    def test_one_native_task_has_run_specific_output_and_http_pool(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )
        assert "{{ ts_nodash }}_{{ ti.try_number }}" in task.bash_command
        assert "/tmp/fotmob_result_" in task.bash_command
        assert task._init_kwargs["pool"] == "fotmob_http_pool"

    @pytest.mark.unit
    def test_no_hardcoded_tournament_fanout(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        assert [op.task_id for op in BashOperator._instances] == ["scrape_fotmob_data"]

    @pytest.mark.unit
    def test_master_is_single_schedule_owner(self):
        mod = _reload_dag_module()

        assert mod.dag._dag_kwargs["schedule"] is None


class TestNativeValidation:
    @pytest.mark.unit
    def test_legacy_or_missing_mode_is_rejected(self, tmp_path):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps({"status": "success", "rows": {"schedule": 1}}),
            encoding="utf-8",
        )

        with pytest.raises(AirflowException, match="native mode is required"):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_incomplete_native_report_fails(self, tmp_path):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps(
                {
                    "mode": "daily",
                    "status": "incomplete",
                    "complete": False,
                    "operations": [],
                    "transport": {"proxy_bytes": 0},
                    "budget": {"requests": 1, "max_requests": 2000},
                    "errors": ["schema drift"],
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(AirflowException, match="Incomplete FotMob"):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_complete_direct_native_report_passes(self, tmp_path):
        import json

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps(
                {
                    "run_id": "run-1",
                    "mode": "daily",
                    "status": "success",
                    "complete": True,
                    "operations": [
                        {
                            "entity": "competition_catalog",
                            "status": "review_required",
                            "errors": [],
                            "retryable": [],
                            "terminal": [],
                            "counts": {"competitions": 10},
                        }
                    ],
                    "transport": {
                        "attempts": 1,
                        "direct_bytes": 100,
                        "proxy_bytes": 0,
                    },
                    "budget": {
                        "requests": 1,
                        "max_requests": 2000,
                        "direct_bytes": 100,
                        "max_direct_bytes": 1024,
                        "proxy_bytes": 0,
                        "max_proxy_bytes": 0,
                    },
                    "errors": [],
                    "rows": {"competition_catalog": 10},
                    "tables": ["iceberg.bronze.fotmob_competitions"],
                }
            ),
            encoding="utf-8",
        )

        validation = mod.validate_data(str(report))

        assert validation["status"] == "success"
        assert validation["transport"]["proxy_bytes"] == 0


class TestSilverDependency:
    @pytest.mark.unit
    def test_ingest_waits_for_silver_before_master_can_start_xref(self):
        mod = _reload_dag_module()

        assert mod.trigger_silver._init_kwargs["wait_for_completion"] is True
        assert mod.trigger_silver._init_kwargs["poke_interval"] == 30
