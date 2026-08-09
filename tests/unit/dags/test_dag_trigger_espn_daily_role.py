from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import importlib
import sys
from types import SimpleNamespace

import pytest


UTC = timezone.utc


def _reload_daily(monkeypatch, *, role: str | None):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if role is None:
        monkeypatch.delenv("ESPN_ISOLATED_STACK", raising=False)
    else:
        monkeypatch.setenv("ESPN_ISOLATED_STACK", role)
    sys.modules.pop("dag_trigger_espn_daily", None)
    sys.modules.pop("dags.dag_trigger_espn_daily", None)
    return importlib.import_module("dag_trigger_espn_daily")


def _owner_context() -> dict[str, object]:
    logical_date = datetime(2026, 7, 31, 14, tzinfo=UTC)
    interval_end = logical_date + timedelta(days=1)
    run_id = f"scheduled__{logical_date.isoformat()}"
    dag_run = SimpleNamespace(
        dag_id="dag_trigger_espn_daily",
        run_type="scheduled",
        run_id=run_id,
        state="running",
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=interval_end,
    )
    return {
        "dag": SimpleNamespace(dag_id="dag_trigger_espn_daily"),
        "dag_run": dag_run,
        "run_id": run_id,
        "logical_date": logical_date,
        "data_interval_start": logical_date,
        "data_interval_end": interval_end,
    }


def test_profile_is_frozen_and_only_exact_allowlisted_name_resolves():
    from scrapers.espn import daily_owner

    profile = daily_owner.resolve_daily_owner_profile("espn-isolated-v1")

    assert profile.parent_dag_id == "dag_trigger_espn_daily"
    assert profile.trigger_task_id == "trigger_espn_ingest"
    assert profile.child_dag_id == "dag_ingest_espn"
    assert profile.child_run_prefix == "espn_daily"
    assert profile.envelope_schema == "espn-daily-parent-v2"
    with pytest.raises(FrozenInstanceError):
        profile.parent_dag_id = "forged"
    with pytest.raises(daily_owner.DailyOwnerError, match="unknown"):
        daily_owner.resolve_daily_owner_profile("from-child-conf")


@pytest.mark.parametrize("role", [None, "", "true", "01"])
def test_daily_owner_dag_materializes_only_for_exact_isolated_role(monkeypatch, role):
    module = _reload_daily(monkeypatch, role=role)

    assert module.dag is None


def test_exact_isolated_role_builds_one_scheduled_fail_propagating_owner(monkeypatch):
    module = _reload_daily(monkeypatch, role="1")
    from airflow.operators.python import PythonOperator

    tasks = {task.task_id: task for task in PythonOperator._instances}
    assert module.dag.dag_id == "dag_trigger_espn_daily"
    assert module.dag.schedule == "0 14 * * *"
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert set(tasks) == {"validate_scheduled_owner", "trigger_espn_ingest"}

    guard = tasks["validate_scheduled_owner"]
    trigger = tasks["trigger_espn_ingest"]
    assert trigger.upstream_task_ids == {guard.task_id}
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_espn"
    assert trigger._init_kwargs["trigger_run_id"] == (
        "espn_daily__dag_trigger_espn_daily__{{ run_id }}"
    )
    assert trigger._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert trigger._init_kwargs["wait_for_completion"] is True
    assert trigger._init_kwargs["allowed_states"] == ["success"]
    assert trigger._init_kwargs["failed_states"] == ["failed"]
    assert trigger._init_kwargs["reset_dag_run"] is False
    assert trigger._init_kwargs["conf"] == {
        "espn_parent": {
            "schema": "espn-daily-parent-v2",
            "owner_profile": "espn-isolated-v1",
            "parent_dag_id": "dag_trigger_espn_daily",
            "parent_task_id": "trigger_espn_ingest",
            "parent_run_id": "{{ run_id }}",
            "parent_run_type": "scheduled",
            "logical_date": "{{ logical_date.isoformat() }}",
            "data_interval_start": "{{ data_interval_start.isoformat() }}",
            "data_interval_end": "{{ data_interval_end.isoformat() }}",
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": ("espn_daily__dag_trigger_espn_daily__{{ run_id }}"),
        }
    }


def test_owner_context_accepts_only_standard_scheduled_daily_run():
    from scrapers.espn import daily_owner

    context = _owner_context()
    result = daily_owner.validate_scheduled_owner(
        environ={"ESPN_ISOLATED_STACK": "1"}, **context
    )

    assert result == {
        "owner_profile": "espn-isolated-v1",
        "parent_dag_id": "dag_trigger_espn_daily",
        "parent_run_id": context["run_id"],
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda context: setattr(context["dag_run"], "run_type", "manual"),
            "scheduled",
        ),
        (lambda context: setattr(context["dag_run"], "run_id", "manual__x"), "run ID"),
        (
            lambda context: setattr(
                context["dag_run"], "dag_id", "dag_master_pipeline"
            ),
            "DAG",
        ),
        (
            lambda context: setattr(
                context["dag_run"],
                "data_interval_end",
                context["data_interval_end"] + timedelta(hours=1),
            ),
            "interval",
        ),
    ],
)
def test_owner_context_rejects_identity_drift(mutation, message):
    from scrapers.espn import daily_owner

    context = _owner_context()
    mutation(context)

    with pytest.raises(daily_owner.DailyOwnerError, match=message):
        daily_owner.validate_scheduled_owner(
            environ={"ESPN_ISOLATED_STACK": "1"}, **context
        )


def test_owner_context_rechecks_isolated_role_at_task_runtime():
    from scrapers.espn import daily_owner

    with pytest.raises(daily_owner.DailyOwnerError, match="ESPN_ISOLATED_STACK=1"):
        daily_owner.validate_scheduled_owner(environ={}, **_owner_context())
