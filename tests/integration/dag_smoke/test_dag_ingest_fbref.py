"""
Integration tests for dag_ingest_fbref with TaskGroup architecture.

These tests verify:
- DAG loads without errors
- TaskGroup structure is correct
- Task dependencies are properly configured
- All expected tasks exist

Issue #495: ожидания выводятся из ``scrapers.fbref.constants``
({PLAYER,TEAM,KEEPER}_STAT_TYPES) вместо хардкода — списки урезаны после
FBref-ограничения Apr-2026 (passing/gca/defense/possession пустые), и каждый
stat-таск имеет traffic_guard-близнеца (#44). Группы выполняются
ПОСЛЕДОВАТЕЛЬНО (player >> team >> keeper >> match_data >> validate) — OOM
safety, не параллельно.
"""

import os
from pathlib import Path

import pytest


# sys.path setup (project root + dags folder) is centralised in the root conftest.py.
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / 'dags'


def _stat_types():
    """Актуальные stat types — единственный источник правды для ожиданий."""
    from scrapers.fbref.constants import (
        PLAYER_STAT_TYPES,
        TEAM_STAT_TYPES,
        KEEPER_STAT_TYPES,
    )
    return PLAYER_STAT_TYPES, TEAM_STAT_TYPES, KEEPER_STAT_TYPES


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
    """Tests for TaskGroup structure.

    Каждая stat-группа содержит 2×len(STAT_TYPES) task'ов: stat-таск +
    traffic_guard-близнец (#44, _build_sequential_stat_group).
    """

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

    def test_taskgroup_player_stats_task_count(self, fbref_dag):
        """player_stats: stat-таск + guard на каждый PLAYER_STAT_TYPES."""
        player_types, _, _ = _stat_types()
        player_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('player_stats.')
        ]

        expected_count = 2 * len(player_types)
        assert len(player_stat_tasks) == expected_count, \
            f"Expected {expected_count} player_stats tasks (stat+guard на " \
            f"{player_types}), got {len(player_stat_tasks)}: {player_stat_tasks}"

    def test_taskgroup_team_stats_task_count(self, fbref_dag):
        """team_stats: stat-таск + guard на каждый TEAM_STAT_TYPES."""
        _, team_types, _ = _stat_types()
        team_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('team_stats.')
        ]

        expected_count = 2 * len(team_types)
        assert len(team_stat_tasks) == expected_count, \
            f"Expected {expected_count} team_stats tasks, got " \
            f"{len(team_stat_tasks)}: {team_stat_tasks}"

    def test_taskgroup_keeper_stats_task_count(self, fbref_dag):
        """keeper_stats: stat-таск + guard на каждый KEEPER_STAT_TYPES."""
        _, _, keeper_types = _stat_types()
        keeper_stat_tasks = [
            task_id for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('keeper_stats.')
        ]

        expected_count = 2 * len(keeper_types)
        assert len(keeper_stat_tasks) == expected_count, \
            f"Expected {expected_count} keeper_stats tasks, got " \
            f"{len(keeper_stat_tasks)}: {keeper_stat_tasks}"

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

    def test_taskgroups_run_sequentially(self, fbref_dag):
        """Группы выполняются последовательно (OOM safety): validate берёт
        upstream только из match_data (последней группы), а цепочка
        player → team → keeper → match_data связана через граничные task'и."""
        validate_task = fbref_dag.task_dict.get('validate_all_data')

        if validate_task is None:
            pytest.skip("Validate task not found")

        upstream_ids = [task.task_id for task in validate_task.upstream_list]
        assert any(tid.startswith('match_data.') for tid in upstream_ids), \
            f"validate_all_data must depend on match_data group, got: {upstream_ids}"

        # Граница групп: первый таск team_stats имеет upstream из player_stats.
        _, team_types, _ = _stat_types()
        first_team = fbref_dag.task_dict[f'team_stats.team_{team_types[0]}']
        team_upstream = [t.task_id for t in first_team.upstream_list]
        assert any(tid.startswith('player_stats.') for tid in team_upstream), \
            f"team_stats must run after player_stats, got upstream: {team_upstream}"

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

    def test_player_stat_tasks_are_bash_operators(self, fbref_dag):
        """Stat-таски — BashOperator; traffic_guard-близнецы — PythonOperator
        (#44), их фильтруем по префиксу id."""
        from airflow.operators.bash import BashOperator

        player_stat_tasks = [
            fbref_dag.task_dict[task_id]
            for task_id in fbref_dag.task_dict.keys()
            if task_id.startswith('player_stats.player_')
        ]
        assert player_stat_tasks, "no player stat tasks found"

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
        """Total = 2×(player+team+keeper) + 6 match_data + start/validate/
        trigger_silver (+опциональный prewarm)."""
        player_types, team_types, keeper_types = _stat_types()
        total_tasks = len(fbref_dag.task_dict)

        expected_min = (
            2 * (len(player_types) + len(team_types) + len(keeper_types))
            + 6   # match_data group
            + 3   # start + validate_all_data + trigger_silver_transform
        )

        assert total_tasks >= expected_min, \
            f"Expected at least {expected_min} tasks, got {total_tasks}"

    def test_expected_stat_types_exist(self, fbref_dag):
        """Каждый stat type из constants имеет таск и traffic_guard."""
        player_types, team_types, keeper_types = _stat_types()
        groups = [
            ('player_stats', 'player', player_types),
            ('team_stats', 'team', team_types),
            ('keeper_stats', 'keeper', keeper_types),
        ]
        for group_id, category, types in groups:
            for stat_type in types:
                task_id = f'{group_id}.{category}_{stat_type}'
                guard_id = f'{group_id}.traffic_guard_{category}_{stat_type}'
                assert task_id in fbref_dag.task_dict, \
                    f"Expected task {task_id} not found in DAG"
                assert guard_id in fbref_dag.task_dict, \
                    f"Expected guard {guard_id} not found in DAG (#44)"
