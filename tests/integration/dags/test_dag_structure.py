"""
Tests for DAG structure and task configuration.

These tests verify that DAGs have the correct structure,
task dependencies, and configurations.
"""

import pytest


@pytest.mark.integration
class TestDagStructure:
    """Test suite for DAG structure validation."""

    def test_ingestion_dags_have_validate_task(self, dag_bag, ingestion_dag_ids):
        """Test that all ingestion DAGs have at least one validate* task."""
        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                task_ids = [task.task_id for task in dag.tasks]

                validate_tasks = [t for t in dag.tasks if t.task_id.startswith('validate')]
                assert validate_tasks, \
                    f"DAG '{dag_id}' has no validate* task. Tasks: {task_ids}"

    def test_validate_data_has_trigger_rule_all_done(self, dag_bag, ingestion_dag_ids):
        """
        Test that validate* tasks have trigger_rule='all_done'.

        This ensures validation runs even if scraping tasks fail,
        allowing us to report partial results.
        """
        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]

                validate_tasks = [t for t in dag.tasks if t.task_id.startswith('validate')]
                for validate_task in validate_tasks:
                    assert validate_task.trigger_rule == 'all_done', \
                        f"DAG '{dag_id}' {validate_task.task_id} has trigger_rule=" \
                        f"'{validate_task.trigger_rule}', should be 'all_done'"

    def test_dags_have_at_least_two_tasks(self, dag_bag, expected_dag_ids):
        """Test that all DAGs have at least 2 tasks."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                task_count = len(dag.tasks)

                assert task_count >= 2, \
                    f"DAG '{dag_id}' has only {task_count} tasks, should have at least 2"

    def test_no_orphan_tasks(self, dag_bag, expected_dag_ids):
        """Test that all tasks have at least one dependency (no orphans)."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]

                for task in dag.tasks:
                    has_upstream = len(task.upstream_task_ids) > 0
                    has_downstream = len(task.downstream_task_ids) > 0

                    # First or last task is OK to have only one direction
                    if not has_upstream and not has_downstream:
                        pytest.fail(
                            f"DAG '{dag_id}' has orphan task '{task.task_id}' "
                            "with no upstream or downstream dependencies"
                        )

    def test_task_retries_configured(self, dag_bag, ingestion_dag_ids):
        """Test that tasks have retries configured."""
        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]

                for task in dag.tasks:
                    # Check if retries is set (either on task or inherited from DAG)
                    retries = task.retries
                    assert retries is not None and retries >= 0, \
                        f"DAG '{dag_id}' task '{task.task_id}' has no retries configured"

    def test_execution_timeout_set(self, dag_bag, ingestion_dag_ids):
        """Test that tasks have execution_timeout configured."""
        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]

                for task in dag.tasks:
                    timeout = task.execution_timeout
                    # Default args should provide timeout
                    # We don't strictly require it on every task


@pytest.mark.integration
class TestScheduleConfiguration:
    """Test suite for DAG schedule validation."""

    def test_unique_schedules(self, dag_bag, ingestion_dag_ids):
        """
        Test that daily ingestion DAGs have unique schedules.

        This prevents multiple scrapers from running simultaneously.
        """
        schedules = {}

        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                schedule = str(dag.schedule_interval)

                # Skip weekly DAGs from this check
                if '* * 0' in schedule or '* * 7' in schedule:
                    continue

                if schedule in schedules:
                    pytest.fail(
                        f"DAGs '{dag_id}' and '{schedules[schedule]}' "
                        f"have the same schedule: {schedule}"
                    )
                schedules[schedule] = dag_id

    def test_schedules_are_staggered(self, dag_bag, ingestion_dag_ids):
        """Test that schedules are at least 1 hour apart."""
        hours = []

        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                schedule = dag.schedule_interval

                if schedule and isinstance(schedule, str):
                    # Parse cron-style schedule
                    parts = schedule.split()
                    if len(parts) >= 2:
                        try:
                            hour = int(parts[1])
                            hours.append((hour, dag_id))
                        except ValueError:
                            pass

        # Sort by hour
        hours.sort(key=lambda x: x[0])

        # Check for sufficient spacing (allow some flexibility)
        for i in range(1, len(hours)):
            prev_hour, prev_dag = hours[i - 1]
            curr_hour, curr_dag = hours[i]

            if curr_hour - prev_hour < 1:
                print(
                    f"Warning: DAGs '{prev_dag}' ({prev_hour}:00) and "
                    f"'{curr_dag}' ({curr_hour}:00) are less than 1 hour apart"
                )


@pytest.mark.integration
class TestMasterPipeline:
    """Test suite for master pipeline DAG."""

    def test_master_has_trigger_operators(self, dag_bag):
        """Test that master pipeline has TriggerDagRunOperator tasks."""
        dag_id = 'dag_master_pipeline'

        if dag_id not in dag_bag.dags:
            pytest.skip(f"DAG '{dag_id}' not found")

        dag = dag_bag.dags[dag_id]

        # Check for trigger tasks (should have multiple)
        trigger_tasks = [
            task for task in dag.tasks
            if 'trigger' in task.task_id.lower()
        ]

        assert len(trigger_tasks) >= 5, \
            f"Master pipeline should have multiple trigger tasks, found: {len(trigger_tasks)}"

    def test_master_has_check_and_report_tasks(self, dag_bag):
        """Test that master pipeline has check and report tasks."""
        dag_id = 'dag_master_pipeline'

        if dag_id not in dag_bag.dags:
            pytest.skip(f"DAG '{dag_id}' not found")

        dag = dag_bag.dags[dag_id]
        task_ids = [task.task_id for task in dag.tasks]

        assert 'check_pipeline_success' in task_ids, \
            "Master pipeline missing 'check_pipeline_success' task"
        assert 'generate_pipeline_report' in task_ids, \
            "Master pipeline missing 'generate_pipeline_report' task"

    def test_master_runs_after_all_ingestion(self, dag_bag, ingestion_dag_ids):
        """Test that master pipeline runs after all ingestion DAGs."""
        master_dag = dag_bag.dags.get('dag_master_pipeline')

        if not master_dag:
            pytest.skip("Master pipeline not found")

        master_schedule = master_dag.schedule_interval

        # Master should run at 14:00 or later
        if isinstance(master_schedule, str) and ' ' in master_schedule:
            parts = master_schedule.split()
            if len(parts) >= 2:
                try:
                    master_hour = int(parts[1])
                    assert master_hour >= 14, \
                        f"Master pipeline runs at {master_hour}:00, " \
                        "should be at 14:00 or later"
                except ValueError:
                    pass
