import importlib
import sys

import pytest


@pytest.mark.parametrize("flag", [None, "true", "1"])
def test_c1_refresh_only_in_isolated_stack(monkeypatch, flag):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if flag is None:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    else:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", flag)
    sys.modules.pop("dag_refresh_fotmob_c1", None)
    module = importlib.import_module("dag_refresh_fotmob_c1")
    if flag != "1":
        assert module.dag is None
        assert PythonOperator._instances == []
        return
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag.schedule == "15 * * * *"
    assert module.dag._dag_kwargs["default_args"]["on_failure_callback"] is not None
    task, = PythonOperator._instances
    assert task.python_callable is module.run_refresh
    assert task._init_kwargs["execution_timeout"].total_seconds() == 1800
    assert task._init_kwargs["retries"] == 0
