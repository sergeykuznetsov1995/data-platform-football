"""Unit tests for ``dags/dag_ingest_understat.py`` — #712 season backfill.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules (including ``airflow.models.param.Param``) into
``sys.modules`` so the DAG module body executes and can be asserted on.

These tests pin the UI-configurable season wiring added for the 10-season
Understat backfill (issue #712, epic #708): the daily run must be unchanged
(default = CURRENT_SEASON), and the scrape task must render
``--season {{ params.season }}`` so a "Trigger DAG w/ config" override flows
through to the scraper instead of a hardcoded current season.
"""
from __future__ import annotations

import importlib
import sys

import pytest


SCRAPE_TASK_ID = 'scrape_understat_data'


def _reload_dag_module():
    """Force a fresh import of the Understat ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_understat", None)
    sys.modules.pop("dags.dag_ingest_understat", None)

    return importlib.import_module("dag_ingest_understat")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


class TestSeasonParam:
    """The season must be a UI Param defaulting to CURRENT_SEASON so the
    scheduled daily run keeps ingesting the current season unchanged."""

    def test_dag_module_imports(self, dag_module):
        assert hasattr(dag_module, 'validate_data')

    def test_season_param_default_is_current_season(self, dag_module):
        from utils.config import CURRENT_SEASON

        season_param = dag_module.dag._dag_kwargs['params']['season']
        # conftest's _Param stub stores the default (real Param also exposes it).
        assert season_param.default == CURRENT_SEASON


class TestValidateData:
    """Row floors must scale with the number of leagues the runner actually
    scraped (results JSON ``leagues``), and the summary must read the runner's
    real keys (``team_match_stats_rows``/``player_match_stats_rows`` — the old
    ``team_stats_rows`` key never existed in the results file)."""

    RESULTS_PATH = '/tmp/understat_result.json'

    @pytest.fixture
    def write_results(self):
        import json
        import os

        def _write(payload):
            with open(self.RESULTS_PATH, 'w') as f:
                json.dump(payload, f)

        yield _write
        if os.path.exists(self.RESULTS_PATH):
            os.unlink(self.RESULTS_PATH)

    @staticmethod
    def _payload(n_leagues=1, scale=1.0, errors=None):
        leagues = ['ENG-Premier League', 'ESP-La Liga'][:n_leagues]
        return {
            'tables': ['iceberg.bronze.understat_x'],
            'leagues': leagues,
            'schedule_rows': int(380 * n_leagues * scale),
            'shots_rows': int(9800 * n_leagues * scale),
            'player_stats_rows': int(550 * n_leagues * scale),
            'team_match_stats_rows': int(380 * n_leagues * scale),
            'player_match_stats_rows': int(11000 * n_leagues * scale),
            'errors': errors or [],
        }

    def test_full_two_league_run_passes_clean(self, dag_module, write_results):
        write_results(self._payload(n_leagues=2))
        validation = dag_module.validate_data()
        assert validation['status'] == 'success'
        assert validation['warnings'] == []
        assert validation['summary']['team_match_stats_rows'] == 760

    def test_floors_scale_with_league_count(self, dag_module, write_results):
        """A row count above the single-league floor but below the two-league
        floor must warn on a two-league run — a flat floor would wave it
        through."""
        payload = self._payload(n_leagues=2)
        # floor is 1000/league: 1500 passes 1 league, fails 2 leagues (2000)
        payload['player_match_stats_rows'] = 1500
        write_results(payload)
        validation = dag_module.validate_data()
        assert any('player_match_stats_rows' in w for w in validation['warnings'])

    def test_zero_rows_with_errors_fails(self, dag_module, write_results):
        from airflow.exceptions import AirflowException

        payload = self._payload(scale=0.0, errors=['understat_schedule: empty'])
        write_results(payload)
        with pytest.raises(AirflowException, match='Validation failed'):
            dag_module.validate_data()


class TestSeasonRenderedFromParams:
    """The scrape task must inject the season via Jinja so an overridden
    season (backfill) reaches the scraper — not a baked-in current season."""

    def test_scrape_task_renders_season_from_params(self, dag_module):
        task = _bash_task(SCRAPE_TASK_ID)
        assert task is not None, f"missing task {SCRAPE_TASK_ID}"
        # f-string collapses {{{{ }}}} -> {{ }}, so the literal Jinja tag
        # survives into the rendered bash_command.
        assert '--season {{ params.season }}' in task.bash_command, (
            f"{SCRAPE_TASK_ID} does not render season from params"
        )
