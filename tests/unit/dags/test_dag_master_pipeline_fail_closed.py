"""Fail-closed contracts for required sources through the master DAG."""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
import sys
from unittest.mock import MagicMock

import pytest


def _reload_master():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_master_pipeline", None)
    sys.modules.pop("dags.dag_master_pipeline", None)
    return importlib.import_module("dag_master_pipeline")


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def _dag_run(**states: str):
    return SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ]
    )


def test_whoscored_scheduled_failure_is_not_an_allowed_success_state():
    _reload_master()
    whoscored = _task("wait_for_scheduled_whoscored")

    assert whoscored._init_kwargs["allowed_states"] == ["success"]
    assert whoscored._init_kwargs["failed_states"] == ["failed"]
    assert whoscored._init_kwargs["execution_delta"].total_seconds() == 4 * 3600
    assert whoscored._init_kwargs["mode"] == "reschedule"
    assert whoscored._init_kwargs["check_existence"] is True


def test_fotmob_child_failure_is_not_an_allowed_success_state():
    module = _reload_master()
    fotmob = _task("ingestion_triggers.trigger_fotmob")

    assert fotmob._init_kwargs["allowed_states"] == ["success"]
    assert fotmob._init_kwargs["failed_states"] == ["failed"]
    assert fotmob._init_kwargs["execution_timeout"].total_seconds() == 14 * 3600
    expected_daily = module.fotmob_daily_trigger_conf()
    assert {
        key: fotmob._init_kwargs["conf"][key] for key in expected_daily
    } == expected_daily


def test_fotmob_trigger_has_exclusive_runtime_schedule_owner_gate():
    _reload_master()
    owner = _task("ingestion_triggers.fotmob_shared_schedule_owner")
    initializer = _task("ingestion_triggers.initialize_fotmob_publication")
    fotmob = _task("ingestion_triggers.trigger_fotmob")
    matchhistory = _task("ingestion_triggers.trigger_matchhistory")

    assert owner._init_kwargs["ignore_downstream_trigger_rules"] is False
    assert owner.upstream_task_ids == set()
    assert initializer.upstream_task_ids == {owner.task_id}
    assert fotmob.upstream_task_ids == {initializer.task_id}
    assert fotmob._init_kwargs["reset_dag_run"] is False
    assert fotmob._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert "execution_date" not in fotmob._init_kwargs
    assert matchhistory.upstream_task_ids == {fotmob.task_id}
    assert matchhistory._init_kwargs["trigger_rule"] == "all_done"


def test_fotmob_owner_defaults_shared_and_captures_isolated_decision(
    monkeypatch,
):
    module = _reload_master()
    from airflow.models import Variable

    monkeypatch.delenv(module.FOTMOB_SCHEDULE_OWNER_ENV, raising=False)
    monkeypatch.setattr(
        Variable,
        "get",
        lambda _key, default_var=None, **_kwargs: default_var,
    )
    assert module.resolve_fotmob_schedule_owner() == "shared"

    monkeypatch.setattr(
        Variable,
        "get",
        lambda _key, **_kwargs: "isolated",
    )
    task_instance = MagicMock()
    assert module.allow_shared_fotmob_schedule(ti=task_instance) is False
    task_instance.xcom_push.assert_called_once_with(
        key=module.FOTMOB_OWNER_XCOM_KEY,
        value="isolated",
    )


def test_fotmob_owner_rejects_unknown_value():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="Invalid FotMob schedule owner"):
        module.resolve_fotmob_schedule_owner("both")


def test_isolated_owner_requires_skipped_shared_trigger_and_gate_evidence():
    module = _reload_master()
    run = _dag_run(
        **{
            module.FOTMOB_OWNER_GATE_TASK_ID: "success",
            "ingestion_triggers.trigger_fotmob": "skipped",
            "wait_for_scheduled_whoscored": "success",
        }
    )

    assert module.enforce_required_source_success(
        dag_run=run,
        fotmob_schedule_owner="isolated",
    ) == {
        "dag_ingest_fotmob": "isolated_owner",
        "dag_ingest_whoscored": "success",
    }


@pytest.mark.parametrize(
    ("trigger_state", "gate_state"),
    [("success", "success"), ("skipped", "missing")],
)
def test_isolated_owner_rejects_overlap_or_missing_gate(
    trigger_state,
    gate_state,
):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            module.FOTMOB_OWNER_GATE_TASK_ID: gate_state,
            "ingestion_triggers.trigger_fotmob": trigger_state,
            "wait_for_scheduled_whoscored": "success",
        }
    )
    with pytest.raises(AirflowException, match="downstream transforms are blocked"):
        module.enforce_required_source_success(
            dag_run=run,
            fotmob_schedule_owner="isolated",
        )


def test_shared_owner_rejects_skipped_fotmob_trigger():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="dag_ingest_fotmob=skipped"):
        module.enforce_required_source_success(
            dag_run=_dag_run(
                **{
                    "ingestion_triggers.trigger_fotmob": "skipped",
                    "wait_for_scheduled_whoscored": "success",
                }
            ),
            fotmob_schedule_owner="shared",
        )


@pytest.mark.parametrize(
    "task_id",
    [
        "wait_for_scheduled_fbref",
        "trigger_xref_transforms",
        "trigger_e3_transforms",
        "trigger_e4_transforms",
        "trigger_fbref_gold",
    ],
)
def test_required_publication_child_failure_is_not_an_allowed_state(task_id):
    _reload_master()
    trigger = _task(task_id)

    assert trigger._init_kwargs["allowed_states"] == ["success"]
    assert trigger._init_kwargs["failed_states"] == ["failed"]


def test_required_source_gate_waits_for_all_ingestion_and_blocks_transforms():
    _reload_master()
    gate = _task("validate_required_sources")
    fbref_sensor = _task("wait_for_scheduled_fbref")
    fotmob_sensor = _task("wait_for_fotmob_publication")
    scope = _task("resolve_fbref_publication_scope")
    xref = _task("trigger_xref_transforms")
    e3 = _task("trigger_e3_transforms")

    # #933: FBref is externally scheduled at 06:00 and only sensed here — it
    # must not appear among the master-triggered ingestion tasks.
    # #951: SofaScore moved to dag_sofascore_pipeline — re-triggering it here
    # would double the ingest run and the publication slot.
    expected_ingestion = {
        "ingestion_triggers.trigger_fotmob",
        "ingestion_triggers.trigger_matchhistory",
        "ingestion_triggers.trigger_understat",
        "ingestion_triggers.trigger_espn",
        "ingestion_triggers.trigger_clubelo",
    }
    assert expected_ingestion <= gate.upstream_task_ids
    assert "wait_for_scheduled_whoscored" in gate.upstream_task_ids
    assert "ingestion_triggers.trigger_whoscored" not in gate.upstream_task_ids
    assert "ingestion_triggers.trigger_fbref" not in gate.upstream_task_ids
    assert (
        "ingestion_triggers.trigger_sofascore" not in gate.upstream_task_ids
    )
    assert gate._init_kwargs["trigger_rule"] == "all_done"
    assert fbref_sensor.upstream_task_ids == set()
    assert gate.task_id in fotmob_sensor.upstream_task_ids
    assert fotmob_sensor._init_kwargs["mode"] == "reschedule"
    assert fotmob_sensor._init_kwargs["timeout"] == 16 * 60 * 60
    assert fotmob_sensor.task_id in scope.upstream_task_ids
    assert fbref_sensor.task_id in scope.upstream_task_ids
    assert scope.task_id in xref.upstream_task_ids
    assert xref.task_id in e3.upstream_task_ids
    assert fbref_sensor._init_kwargs["external_dag_id"] == "dag_ingest_fbref"
    assert fbref_sensor._init_kwargs["execution_delta"].total_seconds() == (
        8 * 60 * 60
    )
    assert fbref_sensor._init_kwargs["timeout"] == 12 * 60 * 60
    assert e3._init_kwargs["trigger_rule"] == "all_success"


def test_master_pins_every_fotmob_child_to_exact_claimed_generation():
    module = _reload_master()
    scope = _task("resolve_fbref_publication_scope")
    xref = _task("trigger_xref_transforms")
    e3 = _task("trigger_e3_transforms")
    e4 = _task("trigger_e4_transforms")
    gold = _task("trigger_fbref_gold")
    release = _task("release_fbref_publication_lock")

    expected_template = (
        "{{ ti.xcom_pull(task_ids='resolve_fbref_publication_scope') }}"
    )
    assert scope._init_kwargs["retries"] == 0
    assert xref._init_kwargs["conf"]["fbref_control_run_id"] == (
        expected_template
    )
    for child in (xref, e3, e4, gold):
        conf = child._init_kwargs["conf"]
        assert conf["publication_owner"] == "dag_master_pipeline"
        assert conf["master_run_id"] == "{{ run_id }}"
        fotmob_conf = conf["fotmob_publication"]
        assert "wait_for_fotmob_publication" in fotmob_conf["generation_id"]
        assert fotmob_conf["generation_id"].startswith("{{")
        assert fotmob_conf["generation_id"].endswith("}}")
        assert set(fotmob_conf["binding"]) == {
            "schema",
            "source",
            "owner",
            "data_interval_start",
            "data_interval_end",
            "runtime_fingerprint",
        }
        assert all(
            "wait_for_fotmob_publication" in value
            for value in fotmob_conf["binding"].values()
        )
        assert "fotmob_generation_id" not in conf
    assert gold._init_kwargs["conf"]["fbref_control_run_id"] == (
        expected_template
    )
    assert module.MASTER_ARGS["retries"] == 0
    assert module.MASTER_DAGRUN_TIMEOUT_HOURS == (
        module.MASTER_CRITICAL_PATH_HOURS
        + module.MASTER_TIMEOUT_SLACK_HOURS
    )
    assert module.dag._dag_kwargs["dagrun_timeout"].total_seconds() == (
        module.MASTER_DAGRUN_TIMEOUT_HOURS * 60 * 60
    )
    assert module.MASTER_SOURCE_CHAIN_HOURS == (
        module.MASTER_FOTMOB_TRIGGER_TIMEOUT_HOURS
        + (len(module.TRIGGERED_INGESTION_DAGS) - 1) * 12
    )
    assert module.MASTER_DAGRUN_TIMEOUT_HOURS - (
        module.MASTER_SOURCE_CHAIN_HOURS
        + module.MASTER_PUBLICATION_CHAIN_HOURS
        + module.MASTER_CONTROL_TASK_HOURS
    ) == module.MASTER_TIMEOUT_SLACK_HOURS
    assert release._init_kwargs["trigger_rule"] == "all_done"
    assert {
        "wait_for_scheduled_fbref",
        "generate_pipeline_report",
    } == release.upstream_task_ids


def test_master_resolves_exact_successful_scheduled_fbref_control_run(
    monkeypatch,
):
    module = _reload_master()
    import scrapers.fbref.control as control

    expected_id = "11111111-1111-4111-8111-111111111111"
    store = SimpleNamespace(
        get_run=lambda run_id: {
            "run_type": "current",
            "status": "succeeded",
        }
        if run_id == expected_id
        else None,
        get_publication_lock=lambda source: {
            "owner_run_id": expected_id,
            "active": True,
        },
        renew_publication_lock=MagicMock(),
    )
    monkeypatch.setattr(control.ControlStore, "from_env", lambda: store)
    def make_id(airflow_run_id, dag_id):
        assert airflow_run_id == "scheduled__2026-07-14T06:00:00+00:00"
        assert dag_id == "dag_ingest_fbref"
        return expected_id

    monkeypatch.setattr(control, "make_control_run_id", make_id)

    assert module.resolve_scheduled_fbref_control_run(
        logical_date=datetime(2026, 7, 14, 14, tzinfo=timezone.utc)
    ) == expected_id
    store.renew_publication_lock.assert_called_once_with(
        expected_id,
        source="fbref",
        ttl_seconds=8 * 24 * 60 * 60,
    )


def test_master_releases_only_after_scheduled_source_sensor_succeeded(
    monkeypatch,
):
    module = _reload_master()
    import scrapers.fbref.control as control
    from utils import fbref_pipeline_tasks

    expected_id = "11111111-1111-4111-8111-111111111111"

    def make_id(airflow_run_id, dag_id):
        assert airflow_run_id == "scheduled__2026-07-14T06:00:00+00:00"
        assert dag_id == "dag_ingest_fbref"
        return expected_id

    release = MagicMock(return_value={"released": True})
    monkeypatch.setattr(control, "make_control_run_id", make_id)
    monkeypatch.setattr(
        fbref_pipeline_tasks, "release_fbref_publication_lock", release
    )
    dag_run = SimpleNamespace(
        logical_date=datetime(2026, 7, 14, 14, tzinfo=timezone.utc),
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="wait_for_scheduled_fbref", state="success"
            ),
            SimpleNamespace(
                task_id="generate_pipeline_report", state="success"
            ),
        ],
    )

    result = module.release_scheduled_fbref_publication_lock(
        dag_run=dag_run
    )
    assert result == {"released": True}
    release.assert_called_once_with(control_run_id=expected_id)


def test_master_cleanup_releases_unstarted_path_but_preserves_failed_verdict(
    monkeypatch,
):
    module = _reload_master()
    import scrapers.fbref.control as control
    from airflow.exceptions import AirflowException
    from utils import fbref_pipeline_tasks

    expected_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setattr(
        control, "make_control_run_id", lambda *_args, **_kwargs: expected_id
    )
    release = MagicMock(return_value={"released": True})
    monkeypatch.setattr(
        fbref_pipeline_tasks, "release_fbref_publication_lock", release
    )
    publication_ids = {
        "trigger_xref_transforms",
        "trigger_e3_transforms",
        "trigger_e4_transforms",
        "trigger_silver_transfermarkt",
        "trigger_silver_capology",
        "trigger_silver_sofifa",
        "trigger_fbref_gold",
    }
    instances = [
        SimpleNamespace(
            task_id="wait_for_scheduled_fbref", state="success"
        ),
        SimpleNamespace(task_id="generate_pipeline_report", state="skipped"),
        *[
            SimpleNamespace(task_id=task_id, state="upstream_failed")
            for task_id in publication_ids
        ],
    ]
    dag_run = SimpleNamespace(
        logical_date=datetime(2026, 7, 14, 14, tzinfo=timezone.utc),
        get_task_instances=lambda: instances,
    )

    with pytest.raises(AirflowException, match="lock released"):
        module.release_scheduled_fbref_publication_lock(dag_run=dag_run)
    release.assert_called_once_with(control_run_id=expected_id)

    release.reset_mock()
    instances[-1].state = "failed"
    with pytest.raises(AirflowException, match="lock retained"):
        module.release_scheduled_fbref_publication_lock(dag_run=dag_run)
    release.assert_not_called()


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_required_source_gate_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "wait_for_scheduled_whoscored": state,
        }
    )
    with pytest.raises(AirflowException, match="downstream transforms are blocked"):
        module.enforce_required_source_success(dag_run=run)


def test_required_source_gate_rejects_missing_trigger_evidence():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="dag_ingest_whoscored=missing"):
        module.enforce_required_source_success(dag_run=_dag_run())


def test_required_source_gate_accepts_exact_current_master_success():
    module = _reload_master()
    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "wait_for_scheduled_whoscored": "success",
        }
    )

    assert module.enforce_required_source_success(dag_run=run) == {
        "dag_ingest_fotmob": "success",
        "dag_ingest_whoscored": "success",
    }


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_required_publication_gate_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        wait_for_fotmob_publication="success",
        wait_for_scheduled_fbref="success",
        trigger_xref_transforms="success",
        trigger_e3_transforms=state,
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match="Required publication transform did not complete successfully",
    ):
        module.enforce_required_publication_success(dag_run=run)


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_master_xref_failure_blocks_e3_and_gold_without_changing_source_verdict(
    state,
):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        wait_for_scheduled_fbref="success",
        trigger_xref_transforms=state,
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )
    with pytest.raises(AirflowException, match=f"dag_transform_xref={state}"):
        module.enforce_required_publication_success(dag_run=run)

    # The independently sensed FBref source evidence remains successful.
    states = module.REQUIRED_PUBLICATION_TASKS
    assert states["dag_ingest_fbref"] == "wait_for_scheduled_fbref"


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_fbref_silver_publication_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    # The sensed source run is FBref-only; xref is independent master evidence.
    run = _dag_run(
        wait_for_scheduled_fbref=state,
        trigger_xref_transforms="success",
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match=f"dag_ingest_fbref={state}",
    ):
        module.enforce_required_publication_success(dag_run=run)


def test_fbref_silver_publication_rejects_missing_current_run_evidence():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        trigger_xref_transforms="success",
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match="dag_ingest_fbref=missing",
    ):
        module.enforce_required_publication_success(dag_run=run)


def test_required_publication_gate_accepts_exact_current_master_success():
    module = _reload_master()
    run = _dag_run(
        wait_for_fotmob_publication="success",
        wait_for_scheduled_fbref="success",
        trigger_xref_transforms="success",
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )

    assert module.enforce_required_publication_success(dag_run=run) == {
        "fotmob_publication": "success",
        "dag_ingest_fbref": "success",
        "dag_transform_xref": "success",
        "dag_transform_e3": "success",
        "dag_transform_fbref_gold": "success",
    }


def test_no_downstream_publication_task_uses_all_done():
    _reload_master()
    for task_id in (
        "trigger_xref_transforms",
        "trigger_e3_transforms",
        "trigger_e4_transforms",
        "trigger_silver_transfermarkt",
        "trigger_silver_capology",
        "trigger_silver_sofifa",
    ):
        assert _task(task_id)._init_kwargs["trigger_rule"] == "all_success"


def test_terminal_check_reuses_required_source_gate():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "wait_for_scheduled_whoscored": "failed",
        }
    )
    with pytest.raises(AirflowException, match="downstream transforms are blocked"):
        module.check_pipeline_success(dag_run=run)


def test_terminal_check_rejects_failed_downstream_publication():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "wait_for_scheduled_whoscored": "success",
            "wait_for_scheduled_fbref": "success",
            "trigger_xref_transforms": "success",
            "trigger_e3_transforms": "failed",
            "trigger_fbref_gold": "upstream_failed",
        }
    )
    with pytest.raises(AirflowException, match="dag_transform_e3=failed"):
        module.check_pipeline_success(dag_run=run)
