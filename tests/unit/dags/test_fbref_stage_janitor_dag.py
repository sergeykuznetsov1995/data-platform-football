from __future__ import annotations

import importlib
import sys
from datetime import timedelta


def _load_module():
    sys.modules.pop("dag_fbref_stage_janitor", None)
    return importlib.import_module("dag_fbref_stage_janitor")


def test_fbref_stage_janitor_runs_bounded_after_compaction_window() -> None:
    from airflow.sensors.python import PythonSensor

    PythonSensor._instances.clear()
    mod = _load_module()
    tasks = {task.task_id: task for task in PythonSensor._instances}

    assert mod.dag._dag_kwargs["dag_id"] == "dag_fbref_stage_janitor"
    assert mod.dag._dag_kwargs["schedule"] == "30 5 * * *"
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.dag._dag_kwargs["default_args"]["retries"] == 0
    assert set(tasks) == {"janitor_fbref_generic_stages"}
    task = tasks["janitor_fbref_generic_stages"]
    assert task._init_kwargs["retries"] == 0
    assert task._init_kwargs["execution_timeout"] == timedelta(minutes=50)
    assert task._init_kwargs["timeout"] == 45 * 60
    assert task._init_kwargs["mode"] == "reschedule"
    # The pooled FBref writer must be able to release its slot while the
    # janitor waits for publication ownership (same pattern as ingest).
    assert task._init_kwargs["pool"] == "fbref_scraper_pool"


def test_fbref_stage_janitor_default_args_do_not_mutate_shared_constant() -> None:
    from utils.default_args import SILVER_ARGS

    _load_module()

    assert SILVER_ARGS["retries"] == 2


def test_fbref_stage_janitor_callable_wraps_lock_wait(monkeypatch) -> None:
    import sys
    from types import SimpleNamespace

    import utils.fbref_maintenance as fbref_maintenance

    monkeypatch.setitem(
        sys.modules,
        "airflow.sensors.base",
        SimpleNamespace(PokeReturnValue=lambda **kw: kw),
    )
    mod = _load_module()
    calls: list[dict] = []

    def fake_lock_wait(**kwargs):
        calls.append(kwargs)
        return {"mode": "audit", "control_run_id": "maintenance"}

    monkeypatch.setattr(
        fbref_maintenance, "maintain_fbref_stages_with_lock_wait", fake_lock_wait
    )
    poke = mod._maintain_fbref_stages()
    assert calls == [{"reschedule_on_busy": True}]
    assert poke == {
        "is_done": True,
        "xcom_value": {"mode": "audit", "control_run_id": "maintenance"},
    }


def test_fbref_stage_janitor_callable_reschedules_when_busy(monkeypatch) -> None:
    import utils.fbref_maintenance as fbref_maintenance

    mod = _load_module()
    monkeypatch.setattr(
        fbref_maintenance,
        "maintain_fbref_stages_with_lock_wait",
        lambda **kwargs: False,
    )
    assert mod._maintain_fbref_stages() is False
