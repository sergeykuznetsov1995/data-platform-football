"""Contracts for the manual resumable WhoScored backfill DAG."""

from __future__ import annotations

import importlib
import json
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _clean_operator_registries():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    yield


def _load_module():
    sys.modules.pop("dag_backfill_whoscored", None)
    sys.modules.pop("dags.dag_backfill_whoscored", None)
    return importlib.import_module("dag_backfill_whoscored")


@pytest.mark.unit
def test_backfill_is_manual_single_run_and_fixed_25_match_chunks():
    mod = _load_module()
    from airflow.operators.bash import BashOperator

    assert mod.dag.schedule is None
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.BACKFILL_CHUNK_SIZE == 25
    task = next(
        item
        for item in BashOperator._instances
        if item.task_id == "run_whoscored_backfill"
    )
    command = task.bash_command
    assert "run_whoscored_scraper.py backfill" in command
    assert "--scopes-json" in command
    assert "--game-ids-json" in command
    assert "--queue-id" in command
    assert "--chunk-size 25" in command
    assert "--full-history" in command
    assert "/opt/airflow/logs/whoscored_state" in command
    assert task._init_kwargs["retries"] == 1
    assert task._init_kwargs["retry_delay"].total_seconds() == 6 * 3600
    assert task._init_kwargs["pool"] == mod.BACKFILL_POOL
    assert "WHOSCORED_REQUEST_LEDGER_PATH" in task.env
    assert "WHOSCORED_PAID_PROXY_URL" not in task.env


@pytest.mark.unit
def test_backfill_requires_explicit_selector():
    mod = _load_module()

    with pytest.raises(mod.AirflowException, match="requires explicit scopes"):
        mod.validate_backfill_params(params={"scopes": [], "game_ids": []})


@pytest.mark.unit
def test_backfill_params_validate_scope_ids_and_dates():
    mod = _load_module()

    result = mod.validate_backfill_params(
        params={
            "scopes": ["WS-252-2=2526"],
            "game_ids": [30, 10, 30],
            "queue_id": "migration-2026",
            "date_from": "2025-08-01",
            "date_to": "2026-05-31",
        }
    )

    assert result == {
        "scopes": ["WS-252-2=2526"],
        "game_ids": [10, 30],
        "all_catalog": False,
        "queue_id": "migration-2026",
        "chunk_size": 25,
    }


@pytest.mark.unit
def test_backfill_can_select_every_persisted_catalog_scope():
    mod = _load_module()

    result = mod.validate_backfill_params(
        params={
            "scopes": [],
            "game_ids": [],
            "all_catalog": True,
            "queue_id": "all-adult-men",
        }
    )

    assert result["all_catalog"] is True
    assert result["scopes"] == []


@pytest.mark.unit
def test_backfill_rejects_ambiguous_catalog_selectors():
    mod = _load_module()

    with pytest.raises(mod.AirflowException, match="mutually exclusive"):
        mod.validate_backfill_params(
            params={
                "scopes": ["WS-252-2=2526"],
                "game_ids": [],
                "all_catalog": True,
                "queue_id": "ambiguous",
            }
        )


@pytest.mark.unit
def test_backfill_dq_requires_complete_queue(monkeypatch, tmp_path):
    mod = _load_module()
    path = tmp_path / "backfill.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "command": "backfill",
                "status": "success",
                "paid_proxy_bytes": 0,
                "queue": {
                    "status": "running",
                    "pending_matches": 1,
                    "completed_matches": 25,
                },
            }
        )
    )
    monkeypatch.setattr(mod, "_result_path", lambda _context: path)

    with pytest.raises(mod.AirflowException, match="queue is incomplete"):
        mod.validate_backfill_result(params={})


@pytest.mark.unit
def test_backfill_gate_cannot_hide_failed_producer():
    mod = _load_module()
    instances = [
        SimpleNamespace(task_id="run_whoscored_backfill", state="failed"),
        SimpleNamespace(task_id="validate_whoscored_backfill", state="success"),
        SimpleNamespace(task_id="final_success_gate", state="running"),
    ]
    dag_run = SimpleNamespace(get_task_instances=lambda: instances)

    with pytest.raises(mod.AirflowException, match="run_whoscored_backfill=failed"):
        mod.enforce_backfill_gate(
            dag_run=dag_run,
            ti=SimpleNamespace(task_id="final_success_gate"),
        )
