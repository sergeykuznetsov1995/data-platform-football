"""Unit tests for ``dags/dag_ingest_matchhistory.py`` — multi-league scope +
skip-aware validation.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules so the DAG module body executes and can be asserted on.

Pins:
* the scrape task fans out over the dedicated MATCHHISTORY_LEAGUES scope
  (independent of the global LEAGUES — flipping LEAGUES switches every source);
* ``validate_data`` treats an all-304 run as a clean no-op, fails a silently
  empty scrape, and applies row thresholds only to leagues that actually
  fetched (304-skipped partitions are complete by definition).
"""
from __future__ import annotations

import importlib
import json
import os
import sys

import pytest


RESULT_PATH = '/tmp/matchhistory_result.json'
SCRAPE_TASK_ID = 'scrape_match_results'


def _reload_dag_module():
    """Force a fresh import of the MatchHistory ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_matchhistory", None)
    sys.modules.pop("dags.dag_ingest_matchhistory", None)

    return importlib.import_module("dag_ingest_matchhistory")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


@pytest.fixture
def result_file():
    """Write the runner-result JSON to the path validate_data reads."""
    def _write(payload):
        with open(RESULT_PATH, 'w') as f:
            json.dump(payload, f)
    yield _write
    if os.path.exists(RESULT_PATH):
        os.unlink(RESULT_PATH)


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


class TestMultiLeagueScope:
    """The DAG must fan out over MATCHHISTORY_LEAGUES, not the global LEAGUES."""

    @pytest.mark.unit
    def test_bash_command_fans_out_matchhistory_leagues(self, dag_module):
        from utils.config import MATCHHISTORY_LEAGUES

        task = _bash_task(SCRAPE_TASK_ID)
        assert task is not None
        leagues_arg = ','.join(MATCHHISTORY_LEAGUES)
        assert f'--leagues "{leagues_arg}"' in task.bash_command
        assert (
            "/opt/legacy-scraper-venv/bin/python "
            "dags/scripts/run_matchhistory_scraper.py"
        ) in task.bash_command
        assert "\npython dags/scripts/run_matchhistory_scraper.py" not in task.bash_command

    @pytest.mark.unit
    def test_scope_covers_top5(self):
        from utils.config import MATCHHISTORY_LEAGUES

        for league in ['ENG-Premier League', 'ESP-La Liga', 'GER-Bundesliga',
                       'ITA-Serie A', 'FRA-Ligue 1']:
            assert league in MATCHHISTORY_LEAGUES


class TestValidateDataSkipAware:
    """validate_data: 304-skips are a no-op, silent-empty scrapes fail."""

    @pytest.mark.unit
    def test_all_leagues_skipped_is_noop(self, dag_module, result_file):
        result_file({
            'rows': 0, 'errors': [], 'league_details': {}, 'tables': [],
            'skipped_not_modified': ['ENG-Premier League', 'ESP-La Liga'],
        })

        validation = dag_module.validate_data()

        assert validation['status'] == 'no_op'
        assert validation['warnings'] == []

    @pytest.mark.unit
    def test_zero_rows_without_skips_fails(self, dag_module, result_file):
        """Previously masked as a warning-only success — must now fail."""
        from airflow.exceptions import AirflowException

        result_file({
            'rows': 0, 'errors': [], 'league_details': {}, 'tables': [],
            'skipped_not_modified': [],
        })

        with pytest.raises(AirflowException):
            dag_module.validate_data()

    @pytest.mark.unit
    def test_skipped_leagues_are_not_warned(self, dag_module, result_file):
        """Thresholds apply only to fetched leagues; skipped ones are complete."""
        result_file({
            'rows': 380, 'errors': [],
            'league_details': {'ENG-Premier League': 380}, 'tables': ['t'],
            'skipped_not_modified': ['ESP-La Liga', 'GER-Bundesliga',
                                     'ITA-Serie A', 'FRA-Ligue 1'],
        })

        validation = dag_module.validate_data()

        assert validation['status'] == 'success'
        assert validation['warnings'] == []

    @pytest.mark.unit
    def test_total_threshold_scales_with_fetched_leagues(self, dag_module, result_file):
        """2 fetched leagues × 90 rows < 100/league → warning, not failure."""
        result_file({
            'rows': 180, 'errors': [],
            'league_details': {'ENG-Premier League': 90, 'ESP-La Liga': 90},
            'tables': ['t'], 'skipped_not_modified': [],
        })

        validation = dag_module.validate_data()

        assert validation['status'] == 'success'
        assert any('Low total row count' in w for w in validation['warnings'])
