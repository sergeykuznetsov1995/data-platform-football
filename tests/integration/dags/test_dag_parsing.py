"""
Tests for DAG parsing and import validation.

These tests verify that all DAGs can be parsed without errors.
"""

import pytest


@pytest.mark.integration
class TestDagParsing:
    """Test suite for DAG parsing validation."""

    def test_no_import_errors(self, dag_bag):
        """
        Test that all DAGs can be imported without errors.

        This is the most critical test - if DAGs have import errors,
        they won't appear in Airflow at all.
        """
        import_errors = dag_bag.import_errors

        if import_errors:
            error_messages = [
                f"{dag_id}: {error}"
                for dag_id, error in import_errors.items()
            ]
            pytest.fail(
                f"DAG import errors found:\n" + "\n".join(error_messages)
            )

    def test_dag_bag_not_empty(self, dag_bag):
        """Test that DagBag contains DAGs."""
        assert len(dag_bag.dags) > 0, "No DAGs found in DagBag"

    def test_all_expected_dags_present(self, dag_bag, expected_dag_ids):
        """Test that all expected DAGs are present."""
        loaded_dag_ids = set(dag_bag.dags.keys())

        for expected_id in expected_dag_ids:
            assert expected_id in loaded_dag_ids, \
                f"Expected DAG '{expected_id}' not found. Loaded: {loaded_dag_ids}"

    def test_no_extra_unknown_dags(self, dag_bag, expected_dag_ids):
        """Test that there are no unexpected DAGs (informational)."""
        loaded_dag_ids = set(dag_bag.dags.keys())
        expected_set = set(expected_dag_ids)

        extra_dags = loaded_dag_ids - expected_set

        # This is informational, not a failure
        if extra_dags:
            print(f"Note: Found extra DAGs not in expected list: {extra_dags}")

    def test_dags_have_tags(self, dag_bag, ingestion_dag_ids):
        """Test that all ingestion DAGs have tags defined."""
        for dag_id in ingestion_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                assert dag.tags, f"DAG '{dag_id}' has no tags"
                assert len(dag.tags) >= 2, \
                    f"DAG '{dag_id}' should have at least 2 tags, has: {dag.tags}"

    def test_dags_have_description(self, dag_bag, expected_dag_ids):
        """Test that all DAGs have a description."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                assert dag.description, f"DAG '{dag_id}' has no description"

    def test_dags_have_start_date(self, dag_bag, expected_dag_ids):
        """Test that all DAGs have a start_date."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                assert dag.start_date is not None, \
                    f"DAG '{dag_id}' has no start_date"

    def test_catchup_disabled(self, dag_bag, expected_dag_ids):
        """Test that catchup is disabled for all DAGs."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                assert dag.catchup is False, \
                    f"DAG '{dag_id}' has catchup enabled (should be False)"

    def test_max_active_runs_limited(self, dag_bag, expected_dag_ids):
        """Test that max_active_runs is set to 1 for all DAGs."""
        for dag_id in expected_dag_ids:
            if dag_id in dag_bag.dags:
                dag = dag_bag.dags[dag_id]
                assert dag.max_active_runs == 1, \
                    f"DAG '{dag_id}' has max_active_runs={dag.max_active_runs}, should be 1"
