"""Unit tests for ``dags/dag_ingest_fbref.py``.

#920 Phase 1: the club season_stats_all + match_data pipeline stays
byte-identical (task_id/output_file/traffic_output unchanged — regression
guard for "клубные лиги не должны измениться"); each single-year tournament
(e.g. INT-World Cup) gets its own parallel mini-pipeline with distinct
task_ids/output files/traffic-output paths, so it never collides with the
club pipeline's traffic-guard files.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules into ``sys.modules`` so the DAG module body (operators
+ ``>>``/TaskGroup wiring) executes and can be asserted on.
"""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

import pytest


def _reload_dag_module():
    """Force a fresh import of the FBref ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    # #920 Phase 1: the DAG module calls is_single_year_competition() at
    # import time to build its task graph — point CONFIG_DIR at the real
    # shipped configs/medallion (on the host, it otherwise defaults to
    # /opt/airflow/configs/medallion, which only exists in the container).
    from utils import medallion_config

    medallion_config.CONFIG_DIR = (
        Path(__file__).resolve().parents[3] / "configs" / "medallion"
    )
    medallion_config.reset_cache()

    sys.modules.pop("dag_ingest_fbref", None)
    sys.modules.pop("dags.dag_ingest_fbref", None)

    return importlib.import_module("dag_ingest_fbref")


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


def _flag_value(command, flag):
    match = re.search(rf'{re.escape(flag)}\s+"([^"]+)"', command)
    return match.group(1) if match else None


@pytest.fixture
def dag_module():
    return _reload_dag_module()


@pytest.mark.unit
class TestClubPipelineUnchanged:
    """Regression guard: the club pipeline's task_ids/output files/
    traffic-output paths must stay byte-identical to before #920 Phase 1."""

    def test_season_stats_all_unchanged(self, dag_module):
        task = _bash_task('season_stats_all')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == "{{ params.leagues | join(',') }}"
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_season_stats.json'
        )
        assert _flag_value(task.bash_command, '--traffic-output').endswith(
            '/fbref_traffic_season_stats.json'
        )

    def test_match_schedule_unchanged(self, dag_module):
        task = _bash_task('match_schedule')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == "{{ params.leagues | join(',') }}"
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_match_schedule.json'
        )

    def test_match_all_data_unchanged(self, dag_module):
        task = _bash_task('match_all_data')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == "{{ params.leagues | join(',') }}"
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_match_all_data.json'
        )
        assert _flag_value(task.bash_command, '--traffic-output').endswith(
            '/fbref_traffic_match_all_data.json'
        )


@pytest.mark.unit
class TestTournamentMiniPipeline:
    """Each single-year tournament in LEAGUES gets its own parallel
    season_stats_all_{slug} + match_data_{slug} pipeline."""

    def test_tournament_season_stats_task_exists_dedicated(self, dag_module):
        task = _bash_task('season_stats_all_int_world_cup')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == 'INT-World Cup'
        assert '--season {{ params.season }}' in task.bash_command
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_season_stats_int_world_cup.json'
        )

    def test_tournament_match_schedule_task_exists_dedicated(self, dag_module):
        task = _bash_task('match_schedule_int_world_cup')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == 'INT-World Cup'
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_match_schedule_int_world_cup.json'
        )

    def test_tournament_match_all_data_task_exists_dedicated(self, dag_module):
        task = _bash_task('match_all_data_int_world_cup')
        assert task is not None
        assert task.env['FBREF_LEAGUES'] == 'INT-World Cup'
        assert _flag_value(task.bash_command, '--output').endswith(
            '/fbref_match_all_data_int_world_cup.json'
        )

    def test_traffic_output_paths_never_collide_with_club(self, dag_module):
        """Regression guard: without --traffic-output plumbing, the club and
        tournament runs would both write /tmp/fbref_traffic_<label>.json
        (the runner labels by mode, not by league) — a real race/corruption
        risk this test protects against."""
        pairs = [
            ('season_stats_all', 'season_stats_all_int_world_cup'),
            ('match_schedule', 'match_schedule_int_world_cup'),
            ('match_all_data', 'match_all_data_int_world_cup'),
        ]
        for club_id, tournament_id in pairs:
            club_task = _bash_task(club_id)
            tournament_task = _bash_task(tournament_id)
            assert club_task is not None and tournament_task is not None

            club_traffic = _flag_value(
                club_task.bash_command, '--traffic-output'
            )
            tournament_traffic = _flag_value(
                tournament_task.bash_command, '--traffic-output'
            )
            assert club_traffic is not None
            assert tournament_traffic is not None
            assert club_traffic != tournament_traffic

    def test_club_only_leagues_produce_no_tournament_pipeline(self, monkeypatch):
        import utils.config as config

        monkeypatch.setattr(config, 'LEAGUES', ['ENG-Premier League'])
        _reload_dag_module()

        assert _bash_task('season_stats_all_int_world_cup') is None
        assert _bash_task('match_schedule_int_world_cup') is None
        assert _bash_task('match_all_data_int_world_cup') is None
        # club pipeline must still exist untouched
        assert _bash_task('season_stats_all') is not None
        assert _bash_task('match_schedule') is not None
        assert _bash_task('match_all_data') is not None
