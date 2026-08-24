from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone

import pytest


@pytest.mark.unit
def test_all_mens_backfill_uses_one_bounded_dynamic_operator():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dags.dag_backfill_sofascore_all_mens", None)
    module = importlib.import_module("dags.dag_backfill_sofascore_all_mens")
    dag = module.dag

    assert dag.schedule == "@continuous"
    assert dag._dag_kwargs["max_active_runs"] == 1
    assert dag._dag_kwargs["max_active_tasks"] == 1
    assert len(BashOperator._instances) + len(PythonOperator._instances) < 10
    mapped = next(
        item for item in BashOperator._instances
        if item.task_id == "run_historical_scope"
    )
    assert mapped._expand_kwargs["env"].operator.task_id == "plan_historical_batch"
    assert mapped._init_kwargs["pool"] == "ingest_scraper_pool"
    assert mapped._init_kwargs["priority_weight"] == 1
    assert "run_sofascore_scope_cycle.py" in mapped.bash_command
    assert '--expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}"' in (
        mapped.bash_command
    )
    assert '--dag-id "${AIRFLOW_CTX_DAG_ID}"' in mapped.bash_command
    assert '--run-id "${AIRFLOW_CTX_DAG_RUN_ID}"' in mapped.bash_command
    assert '--task-id "${AIRFLOW_CTX_TASK_ID}"' in mapped.bash_command
    assert mapped._init_kwargs["do_xcom_push"] is False
    assert mapped._init_kwargs["max_active_tis_per_dag"] == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    ("hour", "production_active", "expected"),
    [(7, False, True), (8, False, False), (14, False, False),
     (17, False, True), (2, True, False), (18, True, False)],
)
def test_history_admission_protects_the_daily_window(
    hour, production_active, expected
):
    module = importlib.import_module("dags.dag_backfill_sofascore_all_mens")
    assert module._history_start_allowed(
        now=datetime(2026, 8, 22, hour, tzinfo=timezone.utc),
        production_active=production_active,
    ) is expected
