from __future__ import annotations

import importlib
import sys


def _reload_daily(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_trigger_fotmob_daily", None)
    sys.modules.pop("dags.dag_trigger_fotmob_daily", None)
    return importlib.import_module("dag_trigger_fotmob_daily")


def test_shared_default_does_not_materialize_daily_dag(monkeypatch):
    module = _reload_daily(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_exact_isolated_opt_in_materializes_daily_dag(monkeypatch):
    module = _reload_daily(monkeypatch, isolated=True)
    from airflow.operators.python import PythonOperator

    assert module.dag is not None
    assert module.dag.dag_id == "dag_trigger_fotmob_daily"
    assert module.dag.schedule == "0 14 * * *"
    assert {task.task_id for task in PythonOperator._instances} == {
        "attest_isolated_runtime",
        "initialize_fotmob_publication",
        "trigger_fotmob_ingest",
        "finalize_fotmob_publication",
    }
    trigger = next(
        task for task in PythonOperator._instances if task.task_id == "trigger_fotmob_ingest"
    )
    expected_conf = module.fotmob_daily_trigger_conf()
    assert {
        key: trigger._init_kwargs["conf"][key] for key in expected_conf
    } == expected_conf
    assert trigger._init_kwargs["execution_timeout"].total_seconds() == 14 * 3600
    attestation = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "attest_isolated_runtime"
    )
    initializer = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "initialize_fotmob_publication"
    )
    assert attestation.python_callable is module.attest_fotmob_isolated_runtime
    assert initializer.upstream_task_ids == {"attest_isolated_runtime"}
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}


def test_non_exact_isolated_role_does_not_materialize_daily_dag(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_trigger_fotmob_daily", None)
    sys.modules.pop("dags.dag_trigger_fotmob_daily", None)
    module = importlib.import_module("dag_trigger_fotmob_daily")
    assert module.dag is None
    assert PythonOperator._instances == []
