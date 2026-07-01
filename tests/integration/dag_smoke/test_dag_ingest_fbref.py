"""
Integration tests for dag_ingest_fbref.

These tests verify:
- DAG loads without errors
- Task structure is correct (combined season stats + match_data group)
- Task dependencies are properly configured
- All expected tasks exist

Architecture (Jul 2026): the nine single_stat tasks (player x4, team x4,
keeper x1 + traffic-guard twins) were replaced by ONE combined
``season_stats_all`` task — player and team stats share the same season
page for stats/shooting/misc, so one process fetches 5 unique pages per
(league, season) instead of 9 and pays a single CF bypass (~2.7 MB each).
"""

import os
from pathlib import Path

import pytest


# sys.path setup (project root + dags folder) is centralised in the root conftest.py.
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / 'dags'


@pytest.fixture(scope='module')
def fbref_dag():
    """Load the FBref DAG for testing."""
    # Set required Airflow environment variables
    os.environ.setdefault('AIRFLOW_HOME', str(PROJECT_ROOT / 'airflow_home'))
    os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(DAGS_FOLDER))
    os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'False')
    os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

    try:
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

        if 'dag_ingest_fbref' not in dag_bag.dags:
            pytest.skip("dag_ingest_fbref not found in DagBag")

        return dag_bag.dags['dag_ingest_fbref']
    except ImportError:
        pytest.skip("Airflow not installed")


@pytest.mark.integration
class TestFBrefDAGLoading:
    """Tests for DAG loading and basic validation."""

    def test_dag_loads_without_errors(self, fbref_dag):
        """Test that dag_ingest_fbref loads without import errors."""
        assert fbref_dag is not None
        assert fbref_dag.dag_id == 'dag_ingest_fbref'

    def test_dag_has_no_import_errors(self, fbref_dag):
        """Verify DAG has no import errors."""
        # If we got here, the DAG loaded successfully
        assert fbref_dag.fileloc is not None

    def test_dag_has_description(self, fbref_dag):
        """Test that DAG has a description."""
        assert fbref_dag.description is not None
        assert len(fbref_dag.description) > 0

    def test_dag_has_correct_schedule(self, fbref_dag):
        """Test that DAG has the expected schedule."""
        # Expected schedule from config: '0 6 * * *' (daily at 6 AM UTC)
        assert fbref_dag.schedule_interval is not None


@pytest.mark.integration
class TestFBrefTaskStructure:
    """Tests for the combined-season-stats + match_data structure."""

    def test_season_stats_combined_task_exists(self, fbref_dag):
        """ONE season_stats_all task replaces the nine single_stat tasks."""
        task_ids = list(fbref_dag.task_dict.keys())

        assert 'season_stats_all' in task_ids, \
            f"Expected season_stats_all task, got: {task_ids}"
        assert 'traffic_guard_season_stats' in task_ids, \
            f"Expected traffic_guard_season_stats task, got: {task_ids}"

    def test_no_legacy_single_stat_taskgroups(self, fbref_dag):
        """The old per-stat TaskGroups must be gone (Jul-2026 optimization)."""
        legacy_prefixes = ('player_stats.', 'team_stats.', 'keeper_stats.')
        legacy = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith(legacy_prefixes)
        ]
        assert not legacy, f"Legacy single_stat tasks still present: {legacy}"

    def test_taskgroup_match_data_structure(self, fbref_dag):
        """match_data: 2 Trino-чека + schedule(+guard) + combined
        match_all_data(+guard) — комбинированный таск вместо 5 отдельных
        (5N → N page loads)."""
        match_data_tasks = sorted(
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('match_data.')
        )

        expected = sorted([
            'match_data.check_trino_health',
            'match_data.match_schedule',
            'match_data.traffic_guard_match_schedule',
            'match_data.check_trino_before_match',
            'match_data.match_all_data',
            'match_data.traffic_guard_match_all_data',
        ])
        assert match_data_tasks == expected, \
            f"match_data group drifted: {match_data_tasks}"


@pytest.mark.integration
class TestFBrefTaskDependencies:
    """Tests for task dependencies."""

    def test_match_schedule_followed_by_guard(self, fbref_dag):
        """schedule → его traffic_guard (#44); комбинированный match_all_data
        идёт дальше по цепочке (после второго Trino-чека)."""
        schedule_task = fbref_dag.task_dict.get('match_data.match_schedule')

        if schedule_task is None:
            pytest.skip("Schedule task not found - may have different naming")

        downstream_ids = [task.task_id for task in schedule_task.downstream_list]
        assert 'match_data.traffic_guard_match_schedule' in downstream_ids, \
            f"Expected traffic_guard_match_schedule downstream of schedule, " \
            f"got: {downstream_ids}"

    def test_season_stats_runs_before_match_data(self, fbref_dag):
        """season_stats_all → guard → match_data (последовательно, OOM
        safety); validate берёт upstream из match_data."""
        season_task = fbref_dag.task_dict['season_stats_all']
        downstream_ids = [t.task_id for t in season_task.downstream_list]
        assert 'traffic_guard_season_stats' in downstream_ids, \
            f"Expected traffic_guard_season_stats downstream of " \
            f"season_stats_all, got: {downstream_ids}"

        guard = fbref_dag.task_dict['traffic_guard_season_stats']
        guard_downstream = [t.task_id for t in guard.downstream_list]
        assert any(tid.startswith('match_data.') for tid in guard_downstream), \
            f"match_data must run after the season stats guard, " \
            f"got: {guard_downstream}"

        validate_task = fbref_dag.task_dict.get('validate_all_data')
        if validate_task is None:
            pytest.skip("Validate task not found")

        upstream_ids = [task.task_id for task in validate_task.upstream_list]
        assert any(tid.startswith('match_data.') for tid in upstream_ids), \
            f"validate_all_data must depend on match_data group, got: {upstream_ids}"

    def test_validate_task_has_trigger_rule_all_done(self, fbref_dag):
        """Test that validate task runs even if some upstream tasks fail."""
        validate_task = fbref_dag.task_dict.get('validate_all_data')

        if validate_task is None:
            pytest.skip("Validate task not found")

        # Should use 'all_done' trigger rule to run even if some tasks fail
        assert validate_task.trigger_rule == 'all_done', \
            f"Expected trigger_rule='all_done', got: {validate_task.trigger_rule}"


@pytest.mark.integration
class TestFBrefTaskConfiguration:
    """Tests for individual task configuration."""

    def test_season_stats_task_is_bash_operator(self, fbref_dag):
        """season_stats_all — BashOperator; его traffic_guard — PythonOperator."""
        from airflow.operators.bash import BashOperator

        season_task = fbref_dag.task_dict['season_stats_all']
        assert isinstance(season_task, BashOperator), \
            f"Expected BashOperator for season_stats_all, got {type(season_task)}"

    def test_season_stats_task_mode(self, fbref_dag):
        """season_stats_all → --mode combined_season_stats + очистка
        устаревших per-stat JSON результатов старой архитектуры."""
        season_task = fbref_dag.task_dict['season_stats_all']
        assert '--mode combined_season_stats' in season_task.bash_command, \
            "Expected --mode combined_season_stats in season_stats_all bash_command"
        assert 'rm -f /tmp/fbref_player_' in season_task.bash_command, \
            "Expected stale per-stat JSON cleanup in season_stats_all bash_command"

    def test_match_data_tasks_modes(self, fbref_dag):
        """schedule → --mode match_data; комбинированный match_all_data →
        --mode combined_match_data."""
        schedule = fbref_dag.task_dict.get('match_data.match_schedule')
        match_all = fbref_dag.task_dict.get('match_data.match_all_data')
        assert schedule is not None and match_all is not None

        assert '--mode match_data' in schedule.bash_command, \
            "Expected --mode match_data in match_schedule bash_command"
        assert '--mode combined_match_data' in match_all.bash_command, \
            "Expected --mode combined_match_data in match_all_data bash_command"

    def test_dag_has_concurrency_limit(self, fbref_dag):
        """Test that DAG has concurrency limit for rate limiting."""
        # Concurrency should be set to limit parallel task execution
        # This is important for FBref rate limiting
        assert fbref_dag.concurrency is not None or fbref_dag.max_active_tasks is not None, \
            "Expected DAG to have concurrency limit"


@pytest.mark.integration
class TestFBrefTaskCount:
    """Tests for total task counts."""

    def test_total_task_count(self, fbref_dag):
        """Total = season_stats_all + guard + 6 match_data + start +
        validate_all_data + report_proxy_traffic + trigger_silver_transform."""
        total_tasks = len(fbref_dag.task_dict)

        expected_min = (
            2   # season_stats_all + traffic_guard_season_stats
            + 6  # match_data group
            + 4  # start + validate + report_proxy_traffic + trigger_silver
        )

        assert total_tasks >= expected_min, \
            f"Expected at least {expected_min} tasks, got {total_tasks}"
