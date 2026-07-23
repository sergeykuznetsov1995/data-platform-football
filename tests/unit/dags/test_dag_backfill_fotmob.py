from __future__ import annotations

import importlib
import sys


def _reload_backfill(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_backfill_fotmob", None)
    sys.modules.pop("dags.dag_backfill_fotmob", None)
    return importlib.import_module("dag_backfill_fotmob")


def test_shared_default_does_not_materialize_backfill_dag(monkeypatch):
    module = _reload_backfill(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_exact_isolated_opt_in_materializes_continuous_backfill(monkeypatch):
    module = _reload_backfill(monkeypatch, isolated=True)
    from airflow.operators.python import PythonOperator

    assert module.dag is not None
    assert module.dag.dag_id == "dag_backfill_fotmob"
    # Self-draining backfill: continuous, single-flight, paused until an
    # operator opts in — never catches up historical logical dates.
    assert module.dag.schedule == "@continuous"
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["catchup"] is False

    assert {task.task_id for task in PythonOperator._instances} == {
        "attest_isolated_runtime",
        "initialize_fotmob_publication",
        "trigger_fotmob_backfill",
        "finalize_fotmob_publication",
        "schedule_next_poll",
        "wait_before_next_continuous_run",
    }

    trigger = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "trigger_fotmob_backfill"
    )
    # The child is the fenced parent ingest DAG, run in backfill mode with the
    # direct-only, bounded profile and the exact minted generation.
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_fotmob"
    conf = trigger._init_kwargs["conf"]
    assert conf["mode"] == "backfill"
    assert conf["max_proxy_mib"] == 0
    assert conf["season_limit"] == module.BACKFILL_SEASON_LIMIT
    assert conf["requests_per_minute"] == module.BACKFILL_REQUESTS_PER_MINUTE
    assert conf["requests_per_minute"] <= 60  # ingest param ceiling
    assert conf["scope"] == "{{ params.scopes }}"
    assert conf["fotmob_publication"]["generation_id"] == module.GENERATION_TEMPLATE
    assert conf["fotmob_publication"]["binding"] == module.BINDING_TEMPLATE

    # Linear, fail-closed wiring identical in spirit to the daily owner.
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
    assert attestation.python_callable is module.attest_fotmob_isolated_runtime
    assert initializer._init_kwargs["op_kwargs"] == {"publication_owner": "isolated"}
    assert initializer.upstream_task_ids == {"attest_isolated_runtime"}
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}
    assert finalize.upstream_task_ids == {"trigger_fotmob_backfill"}
    assert cooldown.upstream_task_ids == {"finalize_fotmob_publication"}
    # Finalize retains the lock for a failed child; it must own the same
    # isolated generation and watch the exact writer child.
    assert finalize._init_kwargs["op_kwargs"] == {
        "publication_owner": "isolated",
        "success_task_id": "trigger_fotmob_backfill",
        "writer_task_ids": ["trigger_fotmob_backfill"],
    }
    assert finalize._init_kwargs["trigger_rule"] == "all_done"


def test_non_exact_isolated_role_does_not_materialize(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_backfill_fotmob", None)
    sys.modules.pop("dags.dag_backfill_fotmob", None)
    module = importlib.import_module("dag_backfill_fotmob")
    assert module.dag is None
    assert PythonOperator._instances == []


def test_rows_total_reads_committed_bronze_rows(monkeypatch):
    # Pure helpers live at module scope, available even when the DAG is gated
    # off, so the cooldown logic is unit-testable without any Airflow runtime.
    module = _reload_backfill(monkeypatch, isolated=False)
    assert module._rows_total({"rows": {"fotmob_matches": 3, "fotmob_schedule": 5}}) == 8
    assert module._rows_total({"rows": {}}) == 0  # drained plan → idle backoff
    assert module._rows_total({"rows": "7"}) == 7
    assert module._rows_total({}) == 0
    assert module._rows_total(None) == 0
