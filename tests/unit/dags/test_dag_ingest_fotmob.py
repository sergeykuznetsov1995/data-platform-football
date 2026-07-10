"""
Unit tests for ``dags/dag_ingest_fotmob.py``.

Verifies the regression fix that adds ``append_env=True`` to the
``scrape_fotmob_data`` BashOperator. Without it, the operator passes ONLY
the explicit ``env`` dict to the subprocess, so anything the Airflow
container relies on (PYTHONPATH extensions, TRINO_PASSWORD, etc.) is
stripped — and the scraper aborts during import or Trino auth.

These tests run on the host where Airflow is NOT installed, so they
rely on the lightweight ``airflow.*`` stubs installed by
``tests/unit/dags/conftest.py`` (DAG, BashOperator, PythonOperator,
@dag/@task decorators).
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
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
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
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
        )

        assert task.env is not None, "explicit env dict must be set"
        assert "PYTHONPATH" in task.env
        assert "/opt/airflow" in task.env["PYTHONPATH"]


class TestFotmobSeasonBackfillParam:
    """#714: season must be a UI-configurable Param wired into the scrape
    command via Jinja, so past seasons can be backfilled with "Trigger DAG
    w/ config" (mirrors ESPN #713 / MatchHistory #710)."""

    @pytest.mark.unit
    def test_season_is_ui_configurable_param(self):
        """``params['season']`` must be an Airflow ``Param`` defaulting to the
        current season (not a bare int) so the UI exposes a season override."""
        mod = _reload_dag_module()

        from airflow.models.param import Param  # stub
        from utils.config import CURRENT_SEASON

        season = mod.dag._dag_kwargs["params"]["season"]
        assert isinstance(season, Param), (
            "season must be wrapped in a Param so it is UI-configurable for "
            "backfill, not a hardcoded int"
        )
        assert season.default == CURRENT_SEASON, (
            "the daily scheduled run must still default to CURRENT_SEASON"
        )

    @pytest.mark.unit
    def test_scrape_command_renders_season_from_params(self):
        """The bash command must pass ``--season {{ params.season }}`` (Jinja),
        NOT a hardcoded season literal — otherwise the UI override is ignored
        and every run scrapes CURRENT_SEASON."""
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
        )

        assert "--season {{ params.season }}" in task.bash_command, (
            "season must be rendered from params.season at runtime so the "
            "backfill override takes effect"
        )


class TestTournamentFanOut:
    """#920 Phase 1: club leagues stay in the original scrape_fotmob_data
    task (task_id/output/Jinja-season unchanged); each single-year tournament
    (e.g. INT-World Cup) gets its own dedicated task, always present in the
    graph (the runner's own #920 bridge resolves/no-ops it)."""

    @pytest.mark.unit
    def test_club_task_excludes_tournament_leagues(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
        )
        assert '--leagues "ENG-Premier League"' in task.bash_command
        assert "INT-World Cup" not in task.bash_command
        assert "/tmp/fotmob_result.json" in task.bash_command

    @pytest.mark.unit
    def test_tournament_task_exists_dedicated(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            (op for op in BashOperator._instances
             if op.task_id == "scrape_fotmob_data_int_world_cup"),
            None,
        )
        assert task is not None
        assert '--leagues "INT-World Cup"' in task.bash_command
        assert "--season {{ params.season }}" in task.bash_command
        assert "/tmp/fotmob_result_int_world_cup.json" in task.bash_command

    @pytest.mark.unit
    def test_club_only_leagues_produce_no_tournament_task(self, monkeypatch):
        import utils.config as config

        monkeypatch.setattr(config, "LEAGUES", ["ENG-Premier League"])
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task_ids = {op.task_id for op in BashOperator._instances}
        assert "scrape_fotmob_data_int_world_cup" not in task_ids
        assert "scrape_fotmob_data" in task_ids
