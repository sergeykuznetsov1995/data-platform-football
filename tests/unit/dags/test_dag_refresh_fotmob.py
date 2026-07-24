from __future__ import annotations

import importlib
import sys


def _reload_refresh(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_refresh_fotmob", None)
    sys.modules.pop("dags.dag_refresh_fotmob", None)
    return importlib.import_module("dag_refresh_fotmob")


def test_shared_default_does_not_materialize_refresh_dag(monkeypatch):
    module = _reload_refresh(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_exact_isolated_opt_in_materializes_continuous_refresh(monkeypatch):
    module = _reload_refresh(monkeypatch, isolated=True)
    from airflow.operators.python import PythonOperator

    assert module.dag is not None
    assert module.dag.dag_id == "dag_refresh_fotmob"
    assert module.dag.schedule == "@continuous"
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["catchup"] is False

    assert {task.task_id for task in PythonOperator._instances} == {
        "attest_isolated_runtime",
        "initialize_fotmob_publication",
        "trigger_fotmob_refresh",
        "finalize_fotmob_publication",
        "schedule_next_poll",
        "wait_before_next_continuous_run",
    }

    trigger = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "trigger_fotmob_refresh"
    )
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_fotmob"
    conf = trigger._init_kwargs["conf"]
    # Bounded, direct-only, whole-catalog current-season refresh (no explicit
    # scope, no pinned cohort — the runner plans every included men's comp and
    # season_limit rotates the stalest).
    assert conf["mode"] == "refresh"
    assert conf["scope"] == ""
    assert conf["max_proxy_mib"] == 0
    assert conf["season_limit"] == module.REFRESH_SEASON_LIMIT
    assert conf["requests_per_minute"] == module.REFRESH_REQUESTS_PER_MINUTE
    assert conf["requests_per_minute"] <= 60
    assert conf["fotmob_publication"]["generation_id"] == module.GENERATION_TEMPLATE
    assert conf["fotmob_publication"]["binding"] == module.BINDING_TEMPLATE
    assert trigger._init_kwargs["trigger_run_id"].startswith("fotmob_refresh__")

    initializer = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "initialize_fotmob_publication"
    )
    finalize = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "finalize_fotmob_publication"
    )
    cooldown = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "schedule_next_poll"
    )
    assert initializer._init_kwargs["op_kwargs"] == {"publication_owner": "isolated"}
    assert initializer.upstream_task_ids == {"attest_isolated_runtime"}
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}
    assert finalize.upstream_task_ids == {"trigger_fotmob_refresh"}
    assert cooldown.upstream_task_ids == {"finalize_fotmob_publication"}
    assert finalize._init_kwargs["op_kwargs"] == {
        "publication_owner": "isolated",
        "success_task_id": "trigger_fotmob_refresh",
        "writer_task_ids": ["trigger_fotmob_refresh"],
    }
    assert finalize._init_kwargs["trigger_rule"] == "all_done"


def test_non_exact_isolated_role_does_not_materialize(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_refresh_fotmob", None)
    sys.modules.pop("dags.dag_refresh_fotmob", None)
    module = importlib.import_module("dag_refresh_fotmob")
    assert module.dag is None
    assert PythonOperator._instances == []


def test_rows_total_reads_committed_bronze_rows(monkeypatch):
    module = _reload_refresh(monkeypatch, isolated=False)
    assert module._rows_total({"rows": {"fotmob_matches": 4, "fotmob_schedule": 6}}) == 10
    assert module._rows_total({"rows": {}}) == 0
    assert module._rows_total({"rows": "7"}) == 7
    assert module._rows_total({}) == 0
    assert module._rows_total(None) == 0
