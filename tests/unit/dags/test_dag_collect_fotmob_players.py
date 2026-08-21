from __future__ import annotations

import importlib
import sys


def _reload_collector(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_collect_fotmob_players", None)
    sys.modules.pop("dags.dag_collect_fotmob_players", None)
    return importlib.import_module("dag_collect_fotmob_players")


def test_shared_default_does_not_materialize_player_collector(monkeypatch):
    module = _reload_collector(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_exact_isolated_opt_in_materializes_manual_player_collector(monkeypatch):
    module = _reload_collector(monkeypatch, isolated=True)
    from airflow.operators.python import PythonOperator

    assert module.dag is not None
    assert module.dag.dag_id == "dag_collect_fotmob_players"
    assert module.dag.schedule is None
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["catchup"] is False
    assert {task.task_id for task in PythonOperator._instances} == {
        "attest_isolated_runtime",
        "initialize_fotmob_publication",
        "trigger_fotmob_player_collector",
        "finalize_fotmob_publication",
    }

    trigger = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "trigger_fotmob_player_collector"
    )
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_fotmob"
    assert trigger._init_kwargs["trigger_run_id"].startswith("fotmob_players__")
    assert trigger._init_kwargs["execution_timeout"].total_seconds() == 3600
    conf = trigger._init_kwargs["conf"]
    assert conf == {
        "mode": module.PLAYER_COLLECTOR_MODE,
        "scope": "",
        "catalog_contract": "",
        "daily_contract": "",
        "competition_scope_file": "",
        "competition_scope_sha256": "",
        "competition_ids_sha256": "",
        "source_refresh_profile": "",
        "source_refresh_targets_sha256": "",
        "entities": "players",
        "max_requests": module.PLAYER_COLLECTOR_MAX_REQUESTS,
        "max_direct_mib": module.PLAYER_COLLECTOR_MAX_DIRECT_MIB,
        "max_proxy_mib": 0,
        "competition_limit": 0,
        "season_limit": 0,
        "match_limit": 0,
        "team_limit": 0,
        "player_limit": module.PLAYER_COLLECTOR_PLAYER_LIMIT,
        "max_attempts": 4,
        "requests_per_minute": module.PLAYER_COLLECTOR_REQUESTS_PER_MINUTE,
        "deadline": "",
        "fotmob_publication": {
            "generation_id": module.GENERATION_TEMPLATE,
            "binding": module.BINDING_TEMPLATE,
        },
    }

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
    assert attestation.python_callable is module.attest_fotmob_isolated_runtime
    assert initializer._init_kwargs["op_kwargs"] == {"publication_owner": "isolated"}
    assert initializer.upstream_task_ids == {"attest_isolated_runtime"}
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}
    assert finalize.upstream_task_ids == {"trigger_fotmob_player_collector"}
    assert finalize._init_kwargs["op_kwargs"] == {
        "publication_owner": "isolated",
        "success_task_id": "trigger_fotmob_player_collector",
        "writer_task_ids": ["trigger_fotmob_player_collector"],
    }
    assert finalize._init_kwargs["trigger_rule"] == "all_done"


def test_non_exact_isolated_role_does_not_materialize(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_collect_fotmob_players", None)
    sys.modules.pop("dags.dag_collect_fotmob_players", None)
    module = importlib.import_module("dag_collect_fotmob_players")
    assert module.dag is None
    assert PythonOperator._instances == []
