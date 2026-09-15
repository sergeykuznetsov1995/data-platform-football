from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from types import ModuleType, SimpleNamespace

import pytest

from utils.fotmob_orchestration import BACKGROUND_HOLD_START


def _hold(shift: timedelta = timedelta(0)) -> datetime:
    """Границу фонового старта берём из константы, а не из литерала."""

    return (
        datetime(
            2026,
            8,
            8,
            BACKGROUND_HOLD_START.hour,
            BACKGROUND_HOLD_START.minute,
            tzinfo=timezone.utc,
        )
        + shift
    )


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
        "dag_collect_fotmob_players",
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
        "dag_collect_fotmob_players": None,
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
    selected_at = _hold(-timedelta(minutes=1))
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
            now_utc=_hold(),
            child_running=False,
            refresh_done_today=False,
            last_failure_ended_at=None,
        )
        is False
    )
    assert initialized == []

    assert module.initialize_admitted_publication(
        ti=ti,
        now_utc=_hold(-timedelta(seconds=30)),
        child_running=False,
        refresh_done_today=False,
        last_failure_ended_at=None,
    ) == {"generation_id": "one"}
    assert len(initialized) == 1


def test_daily_initializer_mints_exact_shared_1400_interval(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    decision = {
        "lane": "daily",
        "selected_date": "2026-08-08",
        "state": state.to_dict(),
    }
    ti = SimpleNamespace(xcom_pull=lambda **_kwargs: decision)
    observed = []
    monkeypatch.setattr(module, "_launch_still_admitted", lambda _context: True)
    monkeypatch.setattr(
        module,
        "initialize_fotmob_publication",
        lambda **context: observed.append(context) or {"generation_id": "daily"},
    )

    result = module.initialize_admitted_publication(
        ti=ti,
        data_interval_start=datetime(2026, 8, 8, 14, 0, tzinfo=timezone.utc),
        data_interval_end=datetime(2026, 8, 8, 14, 5, tzinfo=timezone.utc),
    )

    assert result == {"generation_id": "daily"}
    assert observed[0]["data_interval_start"] == datetime(
        2026, 8, 7, 14, 0, tzinfo=timezone.utc
    )
    assert observed[0]["data_interval_end"] == datetime(
        2026, 8, 8, 14, 0, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("lane", ("refresh", "backfill"))
def test_background_success_abandons_and_releases_generation(
    monkeypatch, lane
):
    import scrapers.fbref.control as control_module

    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    decision = {
        "lane": lane,
        "selected_date": "2026-08-08",
        "state": state.to_dict(),
    }

    class TI:
        def xcom_pull(self, *, task_ids, **_kwargs):
            if task_ids == module.INITIALIZER_TASK_ID:
                return {"generation_id": "background-generation"}
            if task_ids == module.DECISION_TASK_ID:
                return decision
            return None

    complete = []

    class Store:
        def complete_publication_generation(self, generation_id, **kwargs):
            complete.append((generation_id, kwargs))
            return {
                "phase": "abandoned",
                "active": False,
                "released": True,
                "published": False,
            }

    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(
        control_module.ControlStore, "from_env", lambda: Store(), raising=False
    )
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=module.TRIGGER_TASK_ID, state="success")
        ]
    )

    result = module.finalize_or_skip_rejected_launch(ti=TI(), dag_run=dag_run)

    assert result["status"] == "abandoned"
    assert result["publication_state"]["released"] is True
    assert complete == [
        (
            "background-generation",
            {"published": False, "source": module.FOTMOB_PUBLICATION_SOURCE},
        )
    ]


def test_daily_success_stays_ready_and_bad_background_terminal_is_rejected(
    monkeypatch,
):
    import scrapers.fbref.control as control_module

    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    lane = {"value": "daily"}

    class TI:
        def xcom_pull(self, *, task_ids, **_kwargs):
            if task_ids == module.INITIALIZER_TASK_ID:
                return {"generation_id": "generation-one"}
            if task_ids == module.DECISION_TASK_ID:
                return {
                    "lane": lane["value"],
                    "selected_date": "2026-08-08",
                    "state": state.to_dict(),
                }
            return None

    complete = []

    class Store:
        def complete_publication_generation(self, *_args, **_kwargs):
            complete.append(True)
            return {
                "phase": "ready",
                "active": True,
                "released": False,
                "published": False,
            }

    monkeypatch.setattr(module, "fotmob_ceremony_configured", lambda *args: True)
    monkeypatch.setattr(
        control_module.ControlStore, "from_env", lambda: Store(), raising=False
    )
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=module.TRIGGER_TASK_ID, state="success")
        ]
    )

    assert module.finalize_or_skip_rejected_launch(
        ti=TI(), dag_run=dag_run
    ) == {
        "status": "ready",
        "generation_id": "generation-one",
        "lane": "daily",
    }
    assert complete == []

    lane["value"] = "refresh"
    with pytest.raises(Exception, match="not abandoned safely"):
        module.finalize_or_skip_rejected_launch(ti=TI(), dag_run=dag_run)


def test_pretrigger_cutoff_safely_releases_without_child_or_state_advance(
    monkeypatch,
):
    from airflow.exceptions import AirflowSkipException
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = _hold(-timedelta(minutes=1))
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
        "now_utc": _hold(),
        "child_running": False,
        "refresh_done_today": False,
        "last_failure_ended_at": None,
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
    with pytest.raises(AirflowSkipException, match="safely rejected"):
        module.finalize_or_skip_rejected_launch(ti=ti)


def test_pretrigger_boundary_allows_background_before_cutoff(monkeypatch):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = _hold(-timedelta(minutes=1))
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
        "now_utc": _hold(-timedelta(seconds=30)),
        "child_running": False,
        "refresh_done_today": False,
        "last_failure_ended_at": None,
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
    with pytest.raises(
        module.AirflowException, match="failed before child trigger"
    ):
        module.finalize_or_skip_rejected_launch(ti=ti)
    advance = _task("advance_fotmob_scheduler_state")
    assert advance.upstream_task_ids == {"finalize_fotmob_publication"}
    assert advance._init_kwargs.get("trigger_rule", "all_success") == "all_success"


def test_pretrigger_final_check_orders_active_query_then_clock_then_trigger(
    monkeypatch,
):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = _hold(-timedelta(minutes=1))
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
        "_ingest_child_observations",
        lambda *args, **kwargs: events.append("observations_query") or (False, None),
    )
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
        or _hold(-timedelta(seconds=1)),
    }

    assert module.trigger_ingest.execute(context) == "triggered"
    # Наблюдения метабазы обязаны лечь ДО последнего чтения часов: после него
    # остаются только чистые сравнения перед вставкой ребёнка.
    assert events == [
        "observations_query",
        "active_child_query",
        "actual_clock",
        "trigger_child",
    ]


def test_queued_child_appearing_between_init_and_trigger_is_rejected(monkeypatch):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    module = _reload_owner(monkeypatch, isolated=True)
    state = module.FotMobSchedulerState.initial()
    selected_at = _hold(-timedelta(minutes=1))
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
    clock = iter([_hold(-timedelta(minutes=1)), _hold(-timedelta(seconds=30))])
    initialized = []
    released = []
    child_triggers = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(module, "_ingest_child_active", lambda: next(active))
    monkeypatch.setattr(
        module, "_ingest_child_observations", lambda *args, **kwargs: (False, None)
    )
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
                    "paused": sorted(module.LEGACY_PAUSED_DAGS),
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


def _state_with(module, lane, **kwargs):
    return module.FotMobSchedulerState(
        next_background_lane=lane,
        daily_date=kwargs.get("daily_date"),
        generation=3,
        updated_at=datetime(2026, 8, 8, tzinfo=timezone.utc),
    )


def test_lane_selection_feeds_both_metadb_observations_into_the_policy(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = _state_with(module, module.FotMobLane.BACKFILL)
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "_ingest_child_observations",
        lambda *args, **kwargs: pytest.fail("test hook must replace the query"),
    )
    midnight = datetime(2026, 8, 8, 0, 5, tzinfo=timezone.utc)

    owed = module.select_fotmob_lane(
        now_utc=midnight,
        child_running=False,
        refresh_done_today=False,
        last_failure_ended_at=None,
    )
    assert owed["lane"] == "refresh"
    assert owed["reason"] == "refresh_daily_guarantee"

    settled = module.select_fotmob_lane(
        now_utc=midnight,
        child_running=False,
        refresh_done_today=True,
        last_failure_ended_at=None,
    )
    assert settled["lane"] == "backfill"

    paused = module.select_fotmob_lane(
        now_utc=midnight,
        child_running=False,
        refresh_done_today=True,
        last_failure_ended_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc),
    )
    assert paused is False


def _fake_metadb(monkeypatch, rows, *, session_factory=None):
    """Фейковая метабаза, которая РЕАЛЬНО применяет собранный фильтр к строкам.

    Мок, который фильтры только записывает, пропустил бы ровно тот дефект, ради
    которого этот запрос существует (пауза, потерянная у длинной волны).
    """

    class _Clause:
        def __init__(self, op, name, value):
            self.op, self.name, self.value = op, name, value

        def matches(self, row):
            actual = getattr(row, self.name, None)
            if self.op == "eq":
                return actual == self.value
            if self.op == "like":
                return isinstance(actual, str) and actual.startswith(
                    self.value.rstrip("%")
                )
            if self.op == "ge":
                # SQL-семантика: сравнение с NULL — не истина.
                return actual is not None and actual >= self.value
            raise AssertionError(f"неизвестный оператор {self.op}")

        def __repr__(self):
            return f"({self.op}, {self.name}, {self.value})"

        def __eq__(self, other):
            return (self.op, self.name, self.value) == other

    class _Or:
        def __init__(self, clauses):
            self.clauses = clauses

        def matches(self, row):
            return any(clause.matches(row) for clause in self.clauses)

    class _Column:
        def __init__(self, name):
            self.name = name

        def __eq__(self, other):
            return _Clause("eq", self.name, other)

        def like(self, pattern):
            return _Clause("like", self.name, pattern)

        def __ge__(self, other):
            return _Clause("ge", self.name, other)

        def asc(self):
            return ("asc", self.name)

    class _DagRun:
        dag_id = _Column("dag_id")
        run_id = _Column("run_id")
        start_date = _Column("start_date")
        end_date = _Column("end_date")

    log = []

    class _Query:
        def __init__(self):
            self.clauses = []

        def filter(self, *clauses):
            log.append(("filter", clauses))
            self.clauses.extend(clauses)
            return self

        def order_by(self, *clauses):
            log.append(("order_by", clauses))
            return self

        def all(self):
            return [
                row
                for row in rows
                if all(clause.matches(row) for clause in self.clauses)
            ]

    class _Session:
        def query(self, model):
            log.append(("query", model))
            return _Query()

        def close(self):
            log.append(("close",))

    airflow_models = ModuleType("airflow.models")
    airflow_models.DagRun = _DagRun
    airflow_settings = ModuleType("airflow.settings")
    airflow_settings.Session = session_factory or _Session
    sqlalchemy_module = ModuleType("sqlalchemy")
    sqlalchemy_module.or_ = lambda *clauses: _Or(clauses)
    monkeypatch.setitem(sys.modules, "airflow.models", airflow_models)
    monkeypatch.setitem(sys.modules, "airflow.settings", airflow_settings)
    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_module)
    return log


def _run(state, mode, *, end_date=None, start_date=None):
    return SimpleNamespace(
        dag_id="dag_ingest_fotmob",
        run_id="fotmob_orchestrated__generation",
        state=state,
        conf={"mode": mode},
        end_date=end_date,
        start_date=start_date or datetime(2026, 8, 8, 6, tzinfo=timezone.utc),
    )


def test_observations_read_todays_orchestrated_children_in_one_query(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    failed_at = datetime(2026, 8, 8, 9, 30, tzinfo=timezone.utc)
    rows = [
        _run("success", "refresh"),
        _run("running", "daily"),
        _run("failed", "backfill", end_date=failed_at),
    ]
    log = _fake_metadb(monkeypatch, rows)

    refresh_done_today, last_failure_ended_at = module._ingest_child_observations(
        datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    )

    assert refresh_done_today is True
    assert last_failure_ended_at == failed_at
    filters = [entry for entry in log if entry[0] == "filter"]
    assert len(filters) == 1, "наблюдения обязаны стоить один запрос на тик"
    scalar, like, disjunction = filters[0][1][0], filters[0][1][1], filters[0][1][2]
    assert scalar == ("eq", "dag_id", module.INGEST_DAG_ID)
    assert like == ("like", "run_id", f"{module.ORCHESTRATED_RUN_ID_PREFIX}%")
    # Суточный долг — по началу волны, пауза — по её концу: две разные оси времени.
    assert [
        (clause.op, clause.name, clause.value) for clause in disjunction.clauses
    ] == [
        ("ge", "start_date", datetime(2026, 8, 8, tzinfo=timezone.utc)),
        (
            "ge",
            "end_date",
            datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc) - module.FAILURE_BACKOFF,
        ),
    ]
    assert ("close",) in log


def test_observations_forget_a_failure_once_a_later_wave_is_green(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    rows = [
        _run("failed", "refresh", end_date=datetime(2026, 8, 8, 1, tzinfo=timezone.utc)),
        _run("success", "refresh"),
    ]
    _fake_metadb(monkeypatch, rows)

    assert module._ingest_child_observations(
        datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    ) == (True, None)


def test_empty_metadb_day_owes_a_refresh_and_holds_no_pause(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    _fake_metadb(monkeypatch, [])

    assert module._ingest_child_observations(
        datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    ) == (False, None)


def test_unavailable_metadb_raises_instead_of_reporting_a_quiet_day(monkeypatch):
    """Молчаливый None перезапускал бы красную полосу вслепую каждые пять минут."""

    module = _reload_owner(monkeypatch, isolated=False)

    def _broken():
        raise RuntimeError("metadb is unavailable")

    _fake_metadb(monkeypatch, [], session_factory=_broken)

    with pytest.raises(RuntimeError, match="metadb is unavailable"):
        module._ingest_child_observations(
            datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
        )


def test_child_run_id_prefix_matches_the_observation_filter(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=True)

    assert module.ORCHESTRATED_RUN_ID_PREFIX == "fotmob_orchestrated__"
    assert module.trigger_ingest._init_kwargs["trigger_run_id"].startswith(
        module.ORCHESTRATED_RUN_ID_PREFIX
    )


def test_failure_backoff_survives_midnight(monkeypatch):
    """Волна, упавшая в 23:55, обязана держать паузу и после полуночи.

    Выборка «за текущие сутки» теряла такой отказ: в 00:00 запрос его уже не видел,
    и красная полоса стартовала повторно через пять минут вместо паузы.
    """

    module = _reload_owner(monkeypatch, isolated=False)
    yesterday_failed_at = datetime(2026, 8, 7, 23, 55, tzinfo=timezone.utc)
    rows = [
        _run(
            "failed",
            "daily",
            start_date=datetime(2026, 8, 7, 23, 50, tzinfo=timezone.utc),
            end_date=yesterday_failed_at,
        )
    ]
    _fake_metadb(monkeypatch, rows)
    now = datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc)

    refresh_done_today, last_failure_ended_at = module._ingest_child_observations(now)

    assert last_failure_ended_at == yesterday_failed_at
    assert refresh_done_today is False


def test_yesterdays_green_refresh_does_not_settle_todays_guarantee(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    rows = [
        _run(
            "success",
            "refresh",
            start_date=datetime(2026, 8, 7, 22, tzinfo=timezone.utc),
        )
    ]
    _fake_metadb(monkeypatch, rows)

    assert module._ingest_child_observations(
        datetime(2026, 8, 8, 0, 30, tzinfo=timezone.utc)
    ) == (False, None)


def test_long_running_wave_keeps_its_pause_whatever_its_age(monkeypatch):
    """Жёсткий таймаут накрывает только scrape_fotmob_data — дальше ран ждёт Silver.

    Возрастом старта паузу ограничивать нечем: волна, начатая вчера в 14:00 и
    упавшая сегодня в 06:00, обязана держать паузу до 06:30.
    """

    module = _reload_owner(monkeypatch, isolated=False)
    failed_at = datetime(2026, 8, 8, 6, 0, tzinfo=timezone.utc)
    rows = [
        _run(
            "failed",
            "backfill",
            start_date=datetime(2026, 8, 7, 14, tzinfo=timezone.utc),
            end_date=failed_at,
        )
    ]
    _fake_metadb(monkeypatch, rows)

    refresh_done_today, last_failure_ended_at = module._ingest_child_observations(
        datetime(2026, 8, 8, 6, 5, tzinfo=timezone.utc)
    )

    assert last_failure_ended_at == failed_at
    assert refresh_done_today is False


def test_an_old_failure_outside_the_backoff_does_not_reach_the_policy(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    rows = [
        _run(
            "failed",
            "backfill",
            start_date=datetime(2026, 8, 7, 14, tzinfo=timezone.utc),
            end_date=datetime(2026, 8, 7, 20, tzinfo=timezone.utc),
        )
    ]
    _fake_metadb(monkeypatch, rows)

    assert module._ingest_child_observations(
        datetime(2026, 8, 8, 6, 5, tzinfo=timezone.utc)
    ) == (False, None)
