"""Real-Airflow regression for ESPN's legitimate empty network map."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


@pytest.mark.integration
def test_real_airflow_zero_map_reaches_dq_verdict_and_cleanup():
    try:
        import airflow
        from airflow.decorators import dag, task
    except ImportError:
        pytest.skip("real Airflow is not installed in the host unit environment")
    if not isinstance(getattr(airflow, "__version__", None), str):
        pytest.skip("unit-test Airflow stubs cannot execute a mapped DAG")

    @dag(
        dag_id="test_espn_zero_map_runtime",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catchup=False,
    )
    def zero_map_dag():
        @task
        def signed_finished_scope():
            return []

        @task
        def network_fetch(binding):
            del binding
            raise AssertionError("a zero network map must create no fetch worker")

        @task(trigger_rule="none_failed")
        def reduce_to_noop(mapped_results):
            assert list(mapped_results) == []
            return "noop"

        @task
        def published_dq(state):
            assert state == "noop"
            return "dq-green"

        @task(trigger_rule="all_done")
        def terminal_verdict(dq_state):
            assert dq_state == "dq-green"
            return "complete"

        @task(trigger_rule="all_done")
        def release_scope_lease(verdict):
            assert verdict == "complete"
            return "released"

        mapped = network_fetch.expand(binding=signed_finished_scope())
        reduced = reduce_to_noop(mapped)
        dq = published_dq(reduced)
        verdict = terminal_verdict(dq)
        release_scope_lease(verdict)

    dag_instance = zero_map_dag()
    dag_run = dag_instance.test(
        execution_date=datetime(2026, 8, 1, tzinfo=timezone.utc)
    )
    states = {
        (instance.task_id, instance.map_index): str(
            getattr(instance.state, "value", instance.state)
        ).lower()
        for instance in dag_run.get_task_instances()
    }

    assert any(
        task_id == "network_fetch" and state == "skipped"
        for (task_id, _), state in states.items()
    )
    for task_id in (
        "reduce_to_noop",
        "published_dq",
        "terminal_verdict",
        "release_scope_lease",
    ):
        assert states[(task_id, -1)] == "success"
