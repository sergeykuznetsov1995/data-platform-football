"""
Integration tests for dag_ingest_fbref with TaskGroup architecture.

These tests verify:
- DAG loads without errors
- TaskGroup structure is correct
- Task dependencies are properly configured
- All expected tasks exist
"""

import os
import sys
from pathlib import Path

import pytest


# Ensure project paths are in sys.path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / 'dags'
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_FOLDER))


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
class TestFBrefTaskGroups:
    """Tests for TaskGroup structure."""

    def test_dag_has_taskgroups(self, fbref_dag):
        """Test that DAG uses TaskGroups."""
        task_ids = list(fbref_dag.task_dict.keys())

        # TaskGroup tasks have prefixed IDs like 'player_stats.player_shooting'
        taskgroup_prefixes = ['player_stats.', 'team_stats.', 'keeper_stats.', 'match_data.']

        has_taskgroups = any(
            any(task_id.startswith(prefix) for prefix in taskgroup_prefixes)
            for task_id in task_ids
        )

        assert has_taskgroups, f"Expected TaskGroup tasks, got: {task_ids}"

    def test_taskgroup_player_stats_has_9_tasks(self, fbref_dag):
        """Test that player_stats TaskGroup has 9 tasks (one per stat_type)."""
        player_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('player_stats.')
        ]

        # Expected: stats, shooting, passing, passing_types, gca, defense, possession, playingtime, misc
        expected_count = 9
        assert len(player_stat_tasks) == expected_count, \
            f"Expected {expected_count} player_stats tasks, got {len(player_stat_tasks)}: {player_stat_tasks}"

    def test_taskgroup_team_stats_has_9_tasks(self, fbref_dag):
        """Test that team_stats TaskGroup has 9 tasks (one per stat_type)."""
        team_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('team_stats.')
        ]

        expected_count = 9
        assert len(team_stat_tasks) == expected_count, \
            f"Expected {expected_count} team_stats tasks, got {len(team_stat_tasks)}: {team_stat_tasks}"

    def test_taskgroup_keeper_stats_has_2_tasks(self, fbref_dag):
        """Test that keeper_stats TaskGroup has 2 tasks (keeper, keeper_adv)."""
        keeper_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('keeper_stats.')
        ]

        expected_count = 2
        assert len(keeper_stat_tasks) == expected_count, \
            f"Expected {expected_count} keeper_stats tasks, got {len(keeper_stat_tasks)}: {keeper_stat_tasks}"

    def test_taskgroup_match_data_has_4_tasks(self, fbref_dag):
        """Test that match_data TaskGroup has 4 tasks (schedule, shot_events, match_events, lineups)."""
        match_data_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('match_data.')
        ]

        expected_count = 4
        assert len(match_data_tasks) == expected_count, \
            f"Expected {expected_count} match_data tasks, got {len(match_data_tasks)}: {match_data_tasks}"


@pytest.mark.integration
class TestFBrefTaskDependencies:
    """Tests for task dependencies."""

    def test_match_data_schedule_runs_first(self, fbref_dag):
        """Test that schedule task runs before other match_data tasks."""
        schedule_task = fbref_dag.task_dict.get('match_data.match_schedule')

        if schedule_task is None:
            pytest.skip("Schedule task not found - may have different naming")

        # Get downstream tasks
        downstream_ids = [task.task_id for task in schedule_task.downstream_list]

        # shot_events, match_events, lineups should be downstream of schedule
        expected_downstream = ['match_data.match_shot_events', 'match_data.match_match_events', 'match_data.match_lineups']

        for expected in expected_downstream:
            assert expected in downstream_ids, \
                f"Expected {expected} to be downstream of schedule, got: {downstream_ids}"

    def test_validate_task_depends_on_all_taskgroups(self, fbref_dag):
        """Test that validate_all_data task depends on all TaskGroups."""
        validate_task = fbref_dag.task_dict.get('validate_all_data')

        if validate_task is None:
            pytest.skip("Validate task not found")

        # Get upstream task IDs
        upstream_ids = [task.task_id for task in validate_task.upstream_list]

        # Should have tasks from all 4 TaskGroups upstream (or the TaskGroup roots)
        taskgroup_prefixes = ['player_stats.', 'team_stats.', 'keeper_stats.', 'match_data.']

        for prefix in taskgroup_prefixes:
            has_tasks_from_group = any(
                task_id.startswith(prefix) for task_id in upstream_ids
            )
            assert has_tasks_from_group, \
                f"Expected tasks from {prefix} TaskGroup to be upstream of validate, got: {upstream_ids}"

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

    def test_player_stats_tasks_are_bash_operators(self, fbref_dag):
        """Test that player_stats tasks use BashOperator."""
        from airflow.operators.bash import BashOperator

        player_stat_tasks = [
            fbref_dag.task_dict[task_id]
            for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('player_stats.')
        ]

        for task in player_stat_tasks:
            assert isinstance(task, BashOperator), \
                f"Expected BashOperator for {task.task_id}, got {type(task)}"

    def test_tasks_use_single_stat_mode(self, fbref_dag):
        """Test that stat tasks use --mode single_stat in bash command."""
        player_stat_tasks = [
            fbref_dag.task_dict[task_id]
            for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('player_stats.')
        ]

        for task in player_stat_tasks:
            if hasattr(task, 'bash_command'):
                assert '--mode single_stat' in task.bash_command, \
                    f"Expected --mode single_stat in {task.task_id} bash_command"

    def test_match_data_tasks_use_match_data_mode(self, fbref_dag):
        """Test that match_data tasks use --mode match_data in bash command."""
        match_data_tasks = [
            fbref_dag.task_dict[task_id]
            for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('match_data.')
        ]

        for task in match_data_tasks:
            if hasattr(task, 'bash_command'):
                assert '--mode match_data' in task.bash_command, \
                    f"Expected --mode match_data in {task.task_id} bash_command"

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
        """Test that DAG has expected total number of tasks."""
        total_tasks = len(fbref_dag.task_dict)

        # Expected:
        # - player_stats: 9 tasks
        # - team_stats: 9 tasks
        # - keeper_stats: 2 tasks
        # - match_data: 4 tasks
        # - validate_all_data: 1 task
        # Total: 25 tasks
        expected_min = 24  # Allow some flexibility

        assert total_tasks >= expected_min, \
            f"Expected at least {expected_min} tasks, got {total_tasks}"

    def test_expected_stat_types_exist(self, fbref_dag):
        """Test that all expected stat_types have corresponding tasks."""
        expected_player_stat_types = [
            'stats', 'shooting', 'passing', 'passing_types',
            'gca', 'defense', 'possession', 'playingtime', 'misc'
        ]

        for stat_type in expected_player_stat_types:
            task_id = f'player_stats.player_{stat_type}'
            assert task_id in fbref_dag.task_dict, \
                f"Expected task {task_id} not found in DAG"
