import importlib
import sys

from airflow.operators.python import PythonOperator


def test_high_churn_cleanup_runs_even_when_fbref_janitor_fails():
    PythonOperator._instances.clear()
    sys.modules.pop("dags.dag_iceberg_maintenance_daily", None)

    module = importlib.import_module("dags.dag_iceberg_maintenance_daily")
    tasks = {task.task_id: task for task in PythonOperator._instances}

    assert module.dag.dag_id == "dag_iceberg_maintenance_daily"
    assert set(tasks) == {
        "janitor_fbref_generic_stages",
        "maintain_high_churn_bronze",
    }
    assert tasks["janitor_fbref_generic_stages"].downstream_task_ids == {
        "maintain_high_churn_bronze"
    }
    assert tasks["maintain_high_churn_bronze"].upstream_task_ids == {
        "janitor_fbref_generic_stages"
    }
    assert (
        tasks["maintain_high_churn_bronze"]._init_kwargs["trigger_rule"]
        == "all_done"
    )
