from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


def _reload_owner(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_orchestrate_fotmob", None)
    sys.modules.pop("dags.dag_orchestrate_fotmob", None)
    return importlib.import_module("dag_orchestrate_fotmob")


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_shared_default_does_not_materialize_owner(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_isolated_owner_is_the_one_fair_active_schedule(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=True)

    assert module.dag.dag_id == "dag_orchestrate_fotmob"
    assert module.dag.schedule == "*/5 * * * *"
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["render_template_as_native_obj"] is True

    assert _task("choose_fotmob_lane").upstream_task_ids == {
        "attest_isolated_runtime"
    }
    assert _task("initialize_fotmob_publication").upstream_task_ids == {
        "choose_fotmob_lane"
    }
    assert _task("initialize_fotmob_publication").python_callable is (
        module.initialize_admitted_publication
    )
    assert _task("initialize_fotmob_publication")._init_kwargs[
        "ignore_downstream_trigger_rules"
    ] is True
    trigger = _task("trigger_fotmob_ingest")
    assert isinstance(trigger, module.BoundaryCheckedTriggerDagRunOperator)
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_fotmob"
    assert trigger._init_kwargs["wait_for_completion"] is True
    conf = trigger._init_kwargs["conf"]
    assert set(conf) == {*module._CONF_KEYS, "fotmob_publication"}
    assert all("choose_fotmob_lane" in conf[key] for key in module._CONF_KEYS)
    assert _task("finalize_fotmob_publication").upstream_task_ids == {
        "trigger_fotmob_ingest"
    }
    assert _task("finalize_fotmob_publication").python_callable is (
        module.finalize_or_skip_rejected_launch
    )
    assert _task("advance_fotmob_scheduler_state").upstream_task_ids == {
        "finalize_fotmob_publication"
    }


def test_only_automatic_owner_keeps_an_isolated_schedule(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    modules = {}
    for name in (
        "dag_orchestrate_fotmob",
        "dag_refresh_fotmob",
        "dag_backfill_fotmob",
        "dag_trigger_fotmob_daily",
    ):
        sys.modules.pop(name, None)
        sys.modules.pop(f"dags.{name}", None)
        modules[name] = importlib.import_module(name)

    assert {
        name: module.dag.schedule for name, module in modules.items()
    } == {
        "dag_orchestrate_fotmob": "*/5 * * * *",
        "dag_refresh_fotmob": None,
        "dag_backfill_fotmob": None,
        "dag_trigger_fotmob_daily": None,
    }


def test_state_advances_only_after_successful_publication(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=True)
    writes = []
    monkeypatch.setattr(
        module.Variable,
        "set",
        lambda key, value, **kwargs: writes.append((key, value, kwargs)),
        raising=False,
    )

    state = module.FotMobSchedulerState.initial()
    module._store_state(state)
    assert writes == [
        (
            module.SCHEDULER_STATE_VARIABLE,
            state.to_dict(),
            {"serialize_json": True},
        )
    ]

    # Failure has no writer path: only the success-only downstream task owns
    # the durable state mutation.
    assert _task("advance_fotmob_scheduler_state")._init_kwargs.get(
        "trigger_rule", "all_success"
    ) == "all_success"
    assert _task("finalize_fotmob_publication")._init_kwargs["trigger_rule"] == (
        "all_done"
    )


def test_first_run_state_default_is_canonical_json(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)

    def _realistic_get(key, *, default_var, deserialize_json):
        assert key == module.SCHEDULER_STATE_VARIABLE
        assert deserialize_json is True
        assert isinstance(default_var, str)
        return json.loads(default_var)

    monkeypatch.setattr(module.Variable, "get", _realistic_get)
    assert module._load_state() == module.FotMobSchedulerState.initial()


@pytest.mark.parametrize(
    ("queued", "running", "expected"),
    [
        ([SimpleNamespace(state="queued")], [], True),
        ([], [SimpleNamespace(state="running")], True),
        ([], [], False),
    ],
)
def test_ingest_child_activity_queries_queued_and_running_dagruns(
    monkeypatch, queued, running, expected
):
    import airflow.models

    module = _reload_owner(monkeypatch, isolated=False)
    calls = []

    class _DagRun:
        @classmethod
        def find(cls, **kwargs):
            calls.append(kwargs)
            return {"queued": queued, "running": running}[kwargs["state"]]

    monkeypatch.setattr(airflow.models, "DagRun", _DagRun, raising=False)

    assert module._ingest_child_active() is expected
    assert calls[0] == {"dag_id": module.INGEST_DAG_ID, "state": "queued"}
    if not queued:
        assert calls[1] == {"dag_id": module.INGEST_DAG_ID, "state": "running"}


def test_successful_child_advances_durable_lane_once_across_task_retry(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    storage = {}
    writes = []

    def _get(key, *, default_var, deserialize_json):
        raw = storage.get(key, default_var)
        return json.loads(raw) if isinstance(raw, str) else raw

    def _set(key, value, *, serialize_json):
        assert serialize_json is True
        storage[key] = value
        writes.append(value)

    monkeypatch.setattr(module.Variable, "get", _get)
    monkeypatch.setattr(module.Variable, "set", _set, raising=False)
    decision = {
        "lane": "refresh",
        "selected_date": "2026-08-08",
        "state": state.to_dict(),
        "state_generation": 0,
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    now = datetime(2026, 8, 8, 8, 30, tzinfo=timezone.utc)

    first = module.advance_fotmob_scheduler_state(ti=ti, now_utc=now)
    second = module.advance_fotmob_scheduler_state(ti=ti, now_utc=now)

    assert first == second
    assert first["generation"] == 1
    assert first["next_background_lane"] == "backfill"
    assert writes == [first]


def test_background_launch_rechecks_1330_before_initializing(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    initialized = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "initialize_fotmob_publication",
        lambda **context: initialized.append(context) or {"generation_id": "one"},
    )

    assert (
        module.initialize_admitted_publication(
            ti=ti,
            now_utc=datetime(2026, 8, 8, 13, 30, tzinfo=timezone.utc),
            child_running=False,
        )
        is False
    )
    assert initialized == []

    assert module.initialize_admitted_publication(
        ti=ti,
        now_utc=datetime(2026, 8, 8, 13, 29, 30, tzinfo=timezone.utc),
        child_running=False,
    ) == {"generation_id": "one"}
    assert len(initialized) == 1


def test_pretrigger_cutoff_safely_releases_without_child_or_state_advance(
    monkeypatch,
):
    from airflow.exceptions import AirflowSkipException
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }

    class _TI:
        def __init__(self):
            self.values = {}

        def xcom_pull(self, *, task_ids, key=None):
            if task_ids == module.DECISION_TASK_ID:
                return decision
            if task_ids == module.INITIALIZER_TASK_ID:
                return {"generation_id": "generation-one"}
            return self.values.get((task_ids, key))

        def xcom_push(self, *, key, value):
            self.values[(module.TRIGGER_TASK_ID, key)] = value

    ti = _TI()
    releases = []
    child_triggers = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "_release_unstarted_publication",
        lambda context: releases.append(context) or {"released": True},
    )
    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(
        TriggerDagRunOperator,
        "execute",
        lambda self, context: child_triggers.append(context),
        raising=False,
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        lambda *args, **kwargs: pytest.fail("scheduler state must not advance"),
        raising=False,
    )

    context = {
        "ti": ti,
        "now_utc": datetime(2026, 8, 8, 13, 30, tzinfo=timezone.utc),
        "child_running": False,
    }
    with pytest.raises(AirflowSkipException, match="window closed"):
        module.trigger_ingest.execute(context)

    assert releases == [context]
    assert child_triggers == []
    assert ti.values[(module.TRIGGER_TASK_ID, module.LAUNCH_REJECTED_XCOM_KEY)] == {
        "safe_release": True,
        "state_advanced": False,
        "verdict": "skipped",
    }
    monkeypatch.setattr(
        module,
        "fail_unsealed_fotmob_publication",
        lambda **context: pytest.fail("rejected launch must not finalize"),
    )
    with pytest.raises(AirflowSkipException, match="safely rejected"):
        module.finalize_or_skip_rejected_launch(ti=ti)


def test_pretrigger_boundary_allows_background_before_cutoff(monkeypatch):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    child_triggers = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "_release_unstarted_publication",
        lambda context: pytest.fail("allowed launch must not release"),
    )
    monkeypatch.setattr(
        TriggerDagRunOperator,
        "execute",
        lambda self, context: child_triggers.append(context) or "triggered",
        raising=False,
    )
    context = {
        "ti": ti,
        "now_utc": datetime(2026, 8, 8, 13, 29, 30, tzinfo=timezone.utc),
        "child_running": False,
    }

    assert module.trigger_ingest.execute(context) == "triggered"
    assert child_triggers == [context]


def test_pretrigger_exception_releases_generation_and_preserves_red_verdict(
    monkeypatch,
):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)

    class _TI:
        def __init__(self):
            self.values = {}

        def xcom_pull(self, *, task_ids, key=None):
            if task_ids == module.INITIALIZER_TASK_ID:
                return {"generation_id": "generation-one"}
            return self.values.get((task_ids, key))

        def xcom_push(self, *, key, value):
            self.values[(module.TRIGGER_TASK_ID, key)] = value

    ti = _TI()
    releases = []
    child_triggers = []
    monkeypatch.setattr(
        module,
        "_launch_still_admitted",
        lambda context: (_ for _ in ()).throw(
            module.AirflowException("scheduler state read failed")
        ),
    )
    monkeypatch.setattr(
        module,
        "_release_unstarted_publication",
        lambda context: releases.append(context) or {"released": True},
    )
    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(
        TriggerDagRunOperator,
        "execute",
        lambda self, context: child_triggers.append(context),
        raising=False,
    )
    monkeypatch.setattr(
        module.Variable,
        "set",
        lambda *args, **kwargs: pytest.fail("scheduler state must not advance"),
        raising=False,
    )
    context = {"ti": ti}

    with pytest.raises(module.AirflowException, match="state read failed"):
        module.trigger_ingest.execute(context)

    assert releases == [context]
    assert child_triggers == []
    assert ti.values[(module.TRIGGER_TASK_ID, module.LAUNCH_REJECTED_XCOM_KEY)] == {
        "safe_release": True,
        "state_advanced": False,
        "verdict": "failed",
    }
    with pytest.raises(module.AirflowSkipException, match="safely rejected"):
        module.finalize_or_skip_rejected_launch(ti=ti)


def test_pretrigger_final_check_orders_active_query_then_clock_then_trigger(
    monkeypatch,
):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    events = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "_ingest_child_active",
        lambda: events.append("active_child_query") or False,
    )
    monkeypatch.setattr(
        TriggerDagRunOperator,
        "execute",
        lambda self, context: events.append("trigger_child") or "triggered",
        raising=False,
    )
    context = {
        "ti": ti,
        "utcnow": lambda: events.append("actual_clock")
        or datetime(2026, 8, 8, 13, 29, 59, tzinfo=timezone.utc),
    }

    assert module.trigger_ingest.execute(context) == "triggered"
    assert events == ["active_child_query", "actual_clock", "trigger_child"]


def test_queued_child_appearing_between_init_and_trigger_is_rejected(monkeypatch):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }

    class _TI:
        def __init__(self):
            self.values = {}

        def xcom_pull(self, *, task_ids, key=None):
            if task_ids == module.DECISION_TASK_ID:
                return decision
            if task_ids == module.INITIALIZER_TASK_ID:
                return {"generation_id": "generation-one"}
            return self.values.get((task_ids, key))

        def xcom_push(self, *, key, value):
            self.values[(module.TRIGGER_TASK_ID, key)] = value

    ti = _TI()
    active = iter([False, True])
    clock = iter(
        [
            datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc),
            datetime(2026, 8, 8, 13, 29, 30, tzinfo=timezone.utc),
        ]
    )
    initialized = []
    released = []
    child_triggers = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(module, "_ingest_child_active", lambda: next(active))
    monkeypatch.setattr(
        module,
        "initialize_fotmob_publication",
        lambda **context: initialized.append(context)
        or {"generation_id": "generation-one"},
    )
    monkeypatch.setattr(
        module,
        "_release_unstarted_publication",
        lambda context: released.append(context) or {"released": True},
    )
    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(
        TriggerDagRunOperator,
        "execute",
        lambda self, context: child_triggers.append(context),
        raising=False,
    )
    context = {"ti": ti, "utcnow": lambda: next(clock)}

    assert module.initialize_admitted_publication(**context)["generation_id"] == (
        "generation-one"
    )
    with pytest.raises(module.AirflowSkipException):
        module.trigger_ingest.execute(context)

    assert len(initialized) == 1
    assert released == [context]
    assert child_triggers == []


def test_unstarted_publication_release_is_explicitly_safe(monkeypatch):
    from scrapers.fbref.control import ControlStore

    module = _reload_owner(monkeypatch, isolated=False)
    calls = []
    store = SimpleNamespace(
        fail_publication_generation=lambda generation_id, **kwargs: calls.append(
            (generation_id, kwargs)
        )
        or {"released": True}
    )
    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(ControlStore, "from_env", staticmethod(lambda: store))
    ti = SimpleNamespace(
        xcom_pull=lambda **kwargs: {"generation_id": "generation-one"}
    )

    assert module._release_unstarted_publication({"ti": ti}) == {"released": True}
    assert calls == [
        (
            "generation-one",
            {"safe_to_release": True, "source": module.FOTMOB_PUBLICATION_SOURCE},
        )
    ]


def test_deployed_runtime_rejects_legacy_owner_admission(monkeypatch, tmp_path):
    module = _reload_owner(monkeypatch, isolated=False)
    report = tmp_path / "deployment.json"
    report.write_text(
        json.dumps(
            {
                "activation_state": "active",
                "unpaused": [
                    "dag_ingest_fotmob",
                    "dag_transform_fotmob_silver",
                    "dag_trigger_fotmob_daily",
                ],
                "paused": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(module.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, str(report))
    monkeypatch.setattr(
        module,
        "attest_fotmob_isolated_runtime",
        lambda **context: {"status": "attested"},
    )
    dag_run = SimpleNamespace(dag_id=module.OWNER_DAG_ID, run_type="scheduled")

    with pytest.raises(
        module.AirflowException,
        match="does not name the automatic owner",
    ):
        module._attest_owner_runtime(dag_run=dag_run)

    report.write_text(
        json.dumps(
            {
                "activation_state": "active",
                "unpaused": sorted(module.AUTOMATIC_ADMITTED_DAGS),
                "paused": [],
            }
        ),
        encoding="utf-8",
    )
    assert module._attest_owner_runtime(dag_run=dag_run) == {
        "status": "attested"
    }


def test_non_exact_isolated_value_does_not_materialize_owner(monkeypatch):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    sys.modules.pop("dag_orchestrate_fotmob", None)
    sys.modules.pop("dags.dag_orchestrate_fotmob", None)
    module = importlib.import_module("dag_orchestrate_fotmob")
    assert module.dag is None
    assert PythonOperator._instances == []
