import copy
import json
import uuid
from datetime import datetime, timezone

import pytest

from scripts import fotmob_recover as mod
from utils.fotmob_orchestration import FotMobSchedulerState, choose_lane


GENERATION_ID = "28c9c8a2-16d1-50a4-bf8d-c8a5ca4ed50b"
OWNER_RUN_ID = "scheduled__2026-08-08T14:05:00+00:00"
SOFA_RUN_ID = "scheduled__2026-08-07T14:00:00+00:00"


def _binding():
    return {
        "schema": "fotmob-publication-v1",
        "source": "fotmob",
        "owner": "isolated",
        "data_interval_start": "2026-08-07T14:00:00.000000+00:00",
        "data_interval_end": "2026-08-08T14:00:00.000000+00:00",
        "runtime_fingerprint": "a" * 40,
    }


def _publication():
    binding = _binding()
    generated = mod.make_generation_id(binding)
    assert generated == GENERATION_ID
    return {"generation_id": generated, "binding": binding}


def _selected_state():
    return {
        "next_background_lane": "refresh",
        "daily_date": "2026-08-07",
        "generation": 7,
        "updated_at": "2026-08-08T12:00:00+00:00",
    }


def _owner(*, state="failed", lane="daily"):
    selected = _selected_state()
    return {
        "run_id": OWNER_RUN_ID,
        "run_type": "scheduled",
        "state": state,
        "logical_date": "2026-08-08T14:05:00+00:00",
        "data_interval_start": "2026-08-08T14:05:00+00:00",
        "data_interval_end": "2026-08-08T14:10:00+00:00",
        "initializer": {**_publication(), "state": {"phase": "writing"}},
        "decision": {
            "lane": lane,
            "selected_date": "2026-08-08",
            "state": selected,
            "state_generation": selected["generation"],
            "conf": {"mode": lane},
        },
        "task_states": {
            "initialize_fotmob_publication": "success",
            "trigger_fotmob_ingest": "failed" if state == "failed" else "success",
            "advance_fotmob_scheduler_state": (
                "upstream_failed" if state == "failed" else "success"
            ),
            "finalize_fotmob_publication": (
                "upstream_failed" if state == "failed" else "success"
            ),
        },
    }


def _isolated_snapshot(
    *, owner_state="failed", ingest_state="failed", silver_state="failed"
):
    publication = _publication()
    return {
        "schema_version": mod.ISOLATED_SNAPSHOT_SCHEMA,
        "observed_at": "2026-08-08T18:00:00+00:00",
        "pause_states_before": {
            dag_id: dag_id in mod.LEGACY_DAGS for dag_id in mod.ISOLATED_DAGS
        },
        "pause_states_after": {dag_id: True for dag_id in mod.ISOLATED_DAGS},
        "active_runs": [],
        "active_task_instances": [],
        "owner_matches": [_owner(state=owner_state)],
        "ingest": {
            "dag_id": mod.INGEST_DAG_ID,
            "run_id": f"fotmob_orchestrated__{GENERATION_ID}",
            "run_type": "manual",
            "state": ingest_state,
            "conf": {mod.PUBLICATION_CONF_KEY: publication},
            "task_states": {"trigger_silver_transform": "failed"},
        },
        "silver": {
            "dag_id": mod.SILVER_DAG_ID,
            "run_id": f"fotmob_silver__{GENERATION_ID}",
            "run_type": "manual",
            "state": silver_state,
            "conf": {mod.PUBLICATION_CONF_KEY: publication},
            "task_states": {"seal_fotmob_publication": "upstream_failed"},
        },
        "scheduler_state": _selected_state(),
        "atomic_metadata_transaction": True,
    }


def _control(*, phase="writing", status="running", active=True):
    return {
        "generation_id": GENERATION_ID,
        "source": "fotmob",
        "status": status,
        "phase": phase,
        "binding": _binding(),
        "candidate": None,
        "consumer": None,
        "lock_active": active,
        "active": active,
        "owner_dag_id": mod.OWNER_DAG_ID,
        "released_at": None,
    }


def _shared_snapshot():
    return {
        "schema_version": mod.SHARED_SNAPSHOT_SCHEMA,
        "observed_at": "2026-08-08T18:00:00+00:00",
        "consumer_run": {
            "dag_id": mod.SHARED_CONSUMER_DAG_ID,
            "run_id": SOFA_RUN_ID,
            "run_type": "scheduled",
            "state": "failed",
            "logical_date": _binding()["data_interval_start"],
            "data_interval_start": _binding()["data_interval_start"],
            "data_interval_end": _binding()["data_interval_end"],
            "task_states": dict(mod.EXPECTED_TERMINAL_CONSUMER_TASKS),
        },
        "active_bound_downstream_runs": [],
        "active_bound_task_instances": [],
    }


def _arguments(tmp_path, *, generation_id=GENERATION_ID):
    evidence = tmp_path / "evidence"
    evidence.mkdir(exist_ok=True)
    return type(
        "Args",
        (),
        {
            "project": "fotmob-airflow",
            "compose_file": tmp_path / "compose.yaml",
            "env_file": tmp_path / "fotmob.env",
            "deployment_report": evidence / "deployment.json",
            "generation_id": generation_id,
            "output": evidence / "automatic-recovery.json",
            "execute": True,
            "confirm": mod.CONFIRM_RECOVERY,
        },
    )()


def _context(tmp_path):
    evidence = tmp_path / "evidence"
    return {
        "activation_state": "active",
        "automatic_rollout_summary": {"phase": "active", "passed": True},
        "deployment_id": "f" * 32,
        "git_sha": "a" * 40,
        "scheduler_container_id": "1" * 64,
        "evidence_dir": str(evidence.resolve()),
        "shared_handoff_final": {"shared_scheduler_container": "2" * 64},
    }


def _background_case(lane, *, phase, finalize_state, advance_state):
    binding = {
        **_binding(),
        "data_interval_start": "2026-08-08T12:00:00.000000+00:00",
        "data_interval_end": "2026-08-08T12:05:00.000000+00:00",
    }
    generation_id = mod.make_generation_id(binding)
    publication = {"generation_id": generation_id, "binding": binding}
    owner = _owner(state="failed", lane=lane)
    owner.update(
        {
            "data_interval_start": binding["data_interval_start"],
            "data_interval_end": binding["data_interval_end"],
            "initializer": {**publication, "state": {"phase": "ready"}},
        }
    )
    selected = owner["decision"]["state"]
    selected["next_background_lane"] = lane
    owner["task_states"].update(
        {
            "trigger_fotmob_ingest": "success",
            "finalize_fotmob_publication": finalize_state,
            "advance_fotmob_scheduler_state": advance_state,
        }
    )
    snapshot = _isolated_snapshot(
        owner_state="failed", ingest_state="success", silver_state="success"
    )
    snapshot["owner_matches"] = [owner]
    snapshot["ingest"].update(
        {
            "run_id": f"fotmob_orchestrated__{generation_id}",
            "conf": {mod.PUBLICATION_CONF_KEY: publication},
        }
    )
    snapshot["silver"].update(
        {
            "run_id": f"fotmob_silver__{generation_id}",
            "conf": {mod.PUBLICATION_CONF_KEY: publication},
        }
    )
    snapshot["scheduler_state"] = copy.deepcopy(selected)
    active = phase == "ready"
    control = {
        **_control(phase=phase, status="succeeded", active=active),
        "generation_id": generation_id,
        "binding": binding,
        "released_at": None if active else "2026-08-08T18:01:00+00:00",
    }
    return generation_id, snapshot, control


def _install_common(monkeypatch, tmp_path, *, controls, snapshots=None):
    snapshots = snapshots or [
        _isolated_snapshot(),
        _isolated_snapshot(),
        _isolated_snapshot(),
    ]
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    monkeypatch.setattr(
        mod,
        "validate_live_runtimes",
        lambda *_args, **_kwargs: {
            "isolated": {"passed": True},
            "shared": {"passed": True},
        },
    )
    control_values = iter(copy.deepcopy(controls))
    monkeypatch.setattr(
        mod, "_get_control_state", lambda *_args, **_kwargs: next(control_values)
    )
    snapshot_values = iter(copy.deepcopy(snapshots))
    monkeypatch.setattr(
        mod,
        "_observe_isolated",
        lambda *_args, **_kwargs: next(snapshot_values),
    )
    monkeypatch.setattr(
        mod,
        "_pause_shared_for_rollout",
        lambda *_args, **_kwargs: {
            "schema_version": mod.SHARED_PAUSE_SCHEMA,
            "observed_at": "2026-08-08T18:02:00+00:00",
            "schedule_owner": "isolated",
            "consumer_pause_before": False,
            "consumer_pause_after": True,
            "active_runs": [],
            "active_task_instances": [],
            "rollout_ready": True,
            "atomic_metadata_transaction": True,
        },
    )


def test_generation_identity_matches_runtime_uuid5_contract():
    payload = json.dumps(_binding(), sort_keys=True, separators=(",", ":"))
    expected = str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{payload}"))
    assert mod.make_generation_id(_binding()) == expected == GENERATION_ID


def test_daily_roll_forward_makes_next_owner_tick_skip_same_generation():
    decision = _owner()["decision"]
    advanced = mod.expected_advanced_state(
        decision, recovered_at="2026-08-08T18:00:00+00:00"
    )

    state = FotMobSchedulerState.from_dict(advanced)
    next_tick = choose_lane(
        datetime(2026, 8, 8, 14, 10, tzinfo=timezone.utc), state, False
    )

    assert advanced["daily_date"] == "2026-08-08"
    assert advanced["generation"] == 8
    assert next_tick.lane is None
    assert next_tick.reason == "daily_already_completed"


@pytest.mark.parametrize(
    ("lane", "current", "expected"),
    (("refresh", "refresh", "backfill"), ("backfill", "backfill", "refresh")),
)
def test_background_roll_forward_moves_to_other_lane(lane, current, expected):
    owner = _owner(lane=lane)
    owner["decision"]["state"]["next_background_lane"] = current
    advanced = mod.expected_advanced_state(
        owner["decision"], recovered_at="2026-08-08T12:05:00+00:00"
    )
    assert advanced["next_background_lane"] == expected
    assert advanced["generation"] == 8


def test_generated_pause_code_is_serializable_and_covers_exact_six():
    code, marker = mod._isolated_snapshot_code(GENERATION_ID, pause=True)
    compile(code, "fotmob-recovery-isolated", "exec")
    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in code
    assert "with_for_update" in code
    assert repr(list(mod.ISOLATED_DAGS)) in code
    assert marker == "FOTMOB_AUTOMATIC_RECOVERY_ISOLATED_JSON="


def test_generated_shared_terminal_query_compiles(tmp_path, monkeypatch):
    captured = {}

    def execute(_context, *, code, marker, run):
        del run
        compile(code, "fotmob-recovery-shared", "exec")
        captured["code"] = code
        captured["marker"] = marker
        return _shared_snapshot()

    monkeypatch.setattr(mod, "_shared_python_json", execute)
    result = mod._observe_shared(_context(tmp_path), GENERATION_ID, _binding())

    assert result == _shared_snapshot()
    assert repr(list(mod.SHARED_DOWNSTREAM_DAGS)) in captured["code"]
    assert SOFA_RUN_ID in captured["code"]
    assert "isoformat(timespec='microseconds')" in captured["code"]
    assert "DagRun.execution_date >=" in captured["code"]
    assert "DagRun.logical_date >=" not in captured["code"]
    assert captured["marker"] == "FOTMOB_AUTOMATIC_RECOVERY_SHARED_JSON="


def test_generated_cursor_cas_compiles(tmp_path, monkeypatch):
    captured = {}

    def execute(_args, _context_value, *, code, marker, run):
        del run
        compile(code, "fotmob-recovery-cursor", "exec")
        captured["code"] = code
        return {
            "schema_version": mod.CURSOR_TRANSITION_SCHEMA,
            "before": _selected_state(),
            "after": mod.expected_advanced_state(
                _owner()["decision"], recovered_at="2026-08-08T18:00:00+00:00"
            ),
            "idempotent": False,
        }

    monkeypatch.setattr(mod, "_isolated_python_json", execute)
    result = mod._advance_scheduler_cursor(
        _arguments(tmp_path),
        _context(tmp_path),
        _owner()["decision"],
        recovered_at="2026-08-08T18:00:00+00:00",
    )

    assert result["idempotent"] is False
    assert "with_for_update" in captured["code"]
    assert mod.SCHEDULER_STATE_VARIABLE in captured["code"]


def test_generated_shared_rollout_pause_is_atomic(tmp_path, monkeypatch):
    captured = {}

    def execute(_context_value, *, code, marker, run):
        del run
        compile(code, "fotmob-recovery-shared-pause", "exec")
        captured["code"] = code
        return {
            "schema_version": mod.SHARED_PAUSE_SCHEMA,
            "observed_at": "2026-08-08T18:02:00+00:00",
            "schedule_owner": "isolated",
            "consumer_pause_before": False,
            "consumer_pause_after": True,
            "active_runs": [],
            "active_task_instances": [],
            "rollout_ready": True,
            "atomic_metadata_transaction": True,
        }

    monkeypatch.setattr(mod, "_shared_python_json", execute)
    proof = mod._pause_shared_for_rollout(_context(tmp_path))

    assert proof["rollout_ready"] is True
    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in captured["code"]
    assert "with_for_update" in captured["code"]


def _writing_snapshot_for_lane(lane):
    snapshot = _isolated_snapshot()
    decision = snapshot["owner_matches"][0]["decision"]
    decision["lane"] = lane
    decision["conf"]["mode"] = lane
    if lane in {"refresh", "backfill"}:
        decision["state"]["next_background_lane"] = lane
        snapshot["scheduler_state"]["next_background_lane"] = lane
    return snapshot


@pytest.mark.parametrize(
    ("lane", "next_tick_at"),
    (
        ("daily", datetime(2026, 8, 8, 14, 10, tzinfo=timezone.utc)),
        ("refresh", datetime(2026, 8, 8, 12, 10, tzinfo=timezone.utc)),
        ("backfill", datetime(2026, 8, 8, 12, 10, tzinfo=timezone.utc)),
    ),
)
def test_writing_failure_releases_without_advancing_and_same_lane_retries(
    tmp_path, monkeypatch, lane, next_tick_at
):
    released = {
        **_control(phase="failed", status="failed", active=False),
        "released": True,
        "released_at": "2026-08-08T18:01:00+00:00",
        "safe_to_release": True,
    }
    snapshots = [
        _writing_snapshot_for_lane(lane),
        _writing_snapshot_for_lane(lane),
        _writing_snapshot_for_lane(lane),
    ]
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[_control(), _control(), released],
        snapshots=snapshots,
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: pytest.fail("failed work must not move the cursor"),
    )
    monkeypatch.setattr(
        mod,
        "_fail_and_release",
        lambda *_args, **_kwargs: events.append("release") or released,
    )

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    selected_state = snapshots[0]["owner_matches"][0]["decision"]["state"]
    next_tick = choose_lane(
        next_tick_at,
        FotMobSchedulerState.from_dict(selected_state),
        False,
    )
    assert events == ["release"]
    assert next_tick.lane is not None
    assert next_tick.lane.value == lane
    assert report["passed"] is True
    assert report["phase"] == "failed_generation_released"
    assert report["roll_forward"]["cursor_transition"] is None
    assert report["roll_forward"]["scheduler_state_unchanged"] == selected_state
    assert report["roll_forward"]["retry_lane"] == lane
    assert report["roll_forward"]["same_generation_reopen_allowed"] is False
    assert report["isolated_writers_paused"] is True
    assert json.loads(_arguments(tmp_path).output.read_text())["passed"] is True


def test_failed_release_retry_after_lost_response_is_idempotent_and_keeps_cursor(
    tmp_path, monkeypatch
):
    released = {
        **_control(phase="failed", status="failed", active=False),
        "released": True,
        "released_at": "2026-08-08T18:01:00+00:00",
        "safe_to_release": True,
        "idempotent": True,
    }
    snapshots = [
        _isolated_snapshot(),
        _isolated_snapshot(),
        _isolated_snapshot(),
    ]
    for snapshot in snapshots:
        snapshot["pause_states_before"] = dict(mod.ALL_PAUSED_STATES)
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[released, released, released],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: pytest.fail("recovery retry must not move the cursor"),
    )
    monkeypatch.setattr(mod, "_fail_and_release", lambda *_args, **_kwargs: released)

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert report["passed"] is True
    assert report["publication_transition"]["idempotent"] is True
    assert report["roll_forward"]["cursor_transition"] is None
    assert report["roll_forward"]["scheduler_state_unchanged"] == _selected_state()


def test_writing_release_requires_unchanged_cursor_readback_after_mutation(
    tmp_path, monkeypatch
):
    released = {
        **_control(phase="failed", status="failed", active=False),
        "released": True,
        "released_at": "2026-08-08T18:01:00+00:00",
        "safe_to_release": True,
    }
    after_release = _isolated_snapshot()
    after_release["pause_states_before"] = dict(mod.ALL_PAUSED_STATES)
    after_release["scheduler_state"] = mod.expected_advanced_state(
        _owner()["decision"], recovered_at="2026-08-08T18:00:00+00:00"
    )
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[_control(), _control(), released],
        snapshots=[_isolated_snapshot(), _isolated_snapshot(), after_release],
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_fail_and_release",
        lambda *_args, **_kwargs: events.append("release") or released,
    )

    with pytest.raises(mod.RecoveryError, match="cursor did not stay unchanged"):
        mod.recover_automatic_failure(_arguments(tmp_path))

    assert events == ["release"]


def test_recovery_rejects_active_isolated_writer_before_cursor_or_release(
    tmp_path, monkeypatch
):
    snapshot = _isolated_snapshot()
    snapshot["active_runs"] = [
        {
            "dag_id": mod.INGEST_DAG_ID,
            "run_id": f"fotmob_orchestrated__{GENERATION_ID}",
            "state": "running",
        }
    ]
    _install_common(monkeypatch, tmp_path, controls=[_control()], snapshots=[snapshot])
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: pytest.fail("cursor must remain unchanged"),
    )
    monkeypatch.setattr(
        mod,
        "_fail_and_release",
        lambda *_args, **_kwargs: pytest.fail("lock must remain retained"),
    )

    with pytest.raises(mod.RecoveryError, match="active isolated runs"):
        mod.recover_automatic_failure(_arguments(tmp_path))


def test_recovery_rejects_running_writer_task_under_failed_dagrun(
    tmp_path, monkeypatch
):
    snapshot = _isolated_snapshot()
    snapshot["active_task_instances"] = [
        {
            "dag_id": mod.SILVER_DAG_ID,
            "run_id": f"fotmob_silver__{GENERATION_ID}",
            "task_id": "transform_fotmob_player_profile",
            "state": "running",
        }
    ]
    _install_common(monkeypatch, tmp_path, controls=[_control()], snapshots=[snapshot])
    monkeypatch.setattr(
        mod,
        "_fail_and_release",
        lambda *_args, **_kwargs: pytest.fail("live task keeps lock retained"),
    )

    with pytest.raises(mod.RecoveryError, match="active isolated task"):
        mod.recover_automatic_failure(_arguments(tmp_path))


def test_recovery_rejects_forged_owner_lineage(tmp_path, monkeypatch):
    snapshot = _isolated_snapshot()
    snapshot["owner_matches"][0]["initializer"]["binding"]["runtime_fingerprint"] = (
        "b" * 40
    )
    _install_common(monkeypatch, tmp_path, controls=[_control()], snapshots=[snapshot])

    with pytest.raises(mod.RecoveryError, match="owner initializer"):
        mod.recover_automatic_failure(_arguments(tmp_path))


def test_ready_unclaimed_terminal_sofa_is_abandoned_not_retried(tmp_path, monkeypatch):
    ready = _control(phase="ready", status="succeeded")
    abandoned = {
        **_control(phase="abandoned", status="succeeded", active=False),
        "released": True,
        "published": False,
        "released_at": "2026-08-08T18:01:00+00:00",
    }
    snapshots = [
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        ),
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        ),
    ]
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[ready, ready, abandoned],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod, "_observe_shared", lambda *_args, **_kwargs: _shared_snapshot()
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: events.append("abandon") or abandoned,
    )
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: pytest.fail("successful owner is already advanced"),
    )

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert events == ["abandon"]
    assert report["passed"] is True
    assert report["phase"] == "unclaimed_ready_abandoned"
    assert report["shared_terminal_proof"]["consumer_run"]["run_id"] == SOFA_RUN_ID


def test_ready_after_owner_cursor_failure_advances_then_abandons(tmp_path, monkeypatch):
    ready = _control(phase="ready", status="succeeded")
    abandoned = {
        **_control(phase="abandoned", status="succeeded", active=False),
        "released": True,
        "published": False,
        "released_at": "2026-08-08T18:01:00+00:00",
    }
    snapshots = [
        _isolated_snapshot(
            owner_state="failed", ingest_state="success", silver_state="success"
        ),
        _isolated_snapshot(
            owner_state="failed", ingest_state="success", silver_state="success"
        ),
    ]
    for snapshot in snapshots:
        tasks = snapshot["owner_matches"][0]["task_states"]
        tasks["trigger_fotmob_ingest"] = "success"
        tasks["finalize_fotmob_publication"] = "success"
        tasks["advance_fotmob_scheduler_state"] = "failed"
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[ready, ready, abandoned],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod, "_observe_shared", lambda *_args, **_kwargs: _shared_snapshot()
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: (
            events.append("advance")
            or {
                "schema_version": mod.CURSOR_TRANSITION_SCHEMA,
                "before": _selected_state(),
                "after": mod.expected_advanced_state(
                    _owner()["decision"], recovered_at="2026-08-08T18:00:00+00:00"
                ),
                "idempotent": False,
            }
        ),
    )
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: events.append("abandon") or abandoned,
    )

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert events == ["advance", "abandon"]
    assert report["passed"] is True
    assert report["roll_forward"]["cursor_transition"]["idempotent"] is False


def test_daily_ready_after_owner_finalizer_failure_advances_then_abandons(
    tmp_path, monkeypatch
):
    ready = _control(phase="ready", status="succeeded")
    abandoned = {
        **_control(phase="abandoned", status="succeeded", active=False),
        "released": True,
        "published": False,
        "released_at": "2026-08-08T18:01:00+00:00",
    }
    snapshots = [
        _isolated_snapshot(
            owner_state="failed", ingest_state="success", silver_state="success"
        ),
        _isolated_snapshot(
            owner_state="failed", ingest_state="success", silver_state="success"
        ),
    ]
    for snapshot in snapshots:
        tasks = snapshot["owner_matches"][0]["task_states"]
        tasks["trigger_fotmob_ingest"] = "success"
        tasks["finalize_fotmob_publication"] = "failed"
        tasks["advance_fotmob_scheduler_state"] = "upstream_failed"
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[ready, ready, abandoned],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod, "_observe_shared", lambda *_args, **_kwargs: _shared_snapshot()
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: (
            events.append("advance")
            or {
                "schema_version": mod.CURSOR_TRANSITION_SCHEMA,
                "before": _selected_state(),
                "after": mod.expected_advanced_state(
                    _owner()["decision"], recovered_at="2026-08-08T18:00:00+00:00"
                ),
                "idempotent": False,
            }
        ),
    )
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: events.append("abandon") or abandoned,
    )

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert events == ["advance", "abandon"]
    assert report["passed"] is True


@pytest.mark.parametrize(
    ("lane", "phase", "finalize_state", "advance_state"),
    (
        ("refresh", "ready", "failed", "upstream_failed"),
        ("backfill", "abandoned", "success", "failed"),
        ("refresh", "abandoned", "failed", "upstream_failed"),
    ),
)
def test_background_terminal_child_recovers_cursor_without_sofa(
    tmp_path, monkeypatch, lane, phase, finalize_state, advance_state
):
    generation_id, first, control = _background_case(
        lane,
        phase=phase,
        finalize_state=finalize_state,
        advance_state=advance_state,
    )
    second = copy.deepcopy(first)
    abandoned = {
        **control,
        "phase": "abandoned",
        "active": False,
        "lock_active": False,
        "released": True,
        "published": False,
        "released_at": "2026-08-08T18:01:00+00:00",
        "idempotent": phase == "abandoned",
    }
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[control, control, abandoned],
        snapshots=[first, second],
    )
    monkeypatch.setattr(
        mod,
        "_observe_shared",
        lambda *_args, **_kwargs: pytest.fail("background has no Sofa consumer"),
    )
    events = []
    decision = first["owner_matches"][0]["decision"]
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: (
            events.append("advance")
            or {
                "schema_version": mod.CURSOR_TRANSITION_SCHEMA,
                "before": decision["state"],
                "after": mod.expected_advanced_state(
                    decision, recovered_at="2026-08-08T18:00:00+00:00"
                ),
                "idempotent": False,
            }
        ),
    )
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: events.append("abandon") or abandoned,
    )

    report = mod.recover_automatic_failure(
        _arguments(tmp_path, generation_id=generation_id)
    )

    assert events == ["advance", "abandon"]
    assert report["phase"] == "background_abandoned_cursor_recovered"
    assert report["lane"] == lane


def test_background_cursor_recovery_retry_is_idempotent(tmp_path, monkeypatch):
    generation_id, first, control = _background_case(
        "refresh",
        phase="abandoned",
        finalize_state="failed",
        advance_state="upstream_failed",
    )
    decision = first["owner_matches"][0]["decision"]
    advanced = mod.expected_advanced_state(
        decision, recovered_at="2026-08-08T18:00:00+00:00"
    )
    snapshots = [first, copy.deepcopy(first)]
    for snapshot in snapshots:
        snapshot["scheduler_state"] = copy.deepcopy(advanced)
        snapshot["pause_states_before"] = dict(mod.ALL_PAUSED_STATES)
    abandoned = {
        **control,
        "released": True,
        "published": False,
        "idempotent": True,
    }
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[control, control, abandoned],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod,
        "_advance_scheduler_cursor",
        lambda *_args, **_kwargs: {
            "schema_version": mod.CURSOR_TRANSITION_SCHEMA,
            "before": advanced,
            "after": advanced,
            "idempotent": True,
        },
    )
    monkeypatch.setattr(mod, "_abandon_unclaimed", lambda *_args, **_kwargs: abandoned)

    report = mod.recover_automatic_failure(
        _arguments(tmp_path, generation_id=generation_id)
    )

    assert report["roll_forward"]["cursor_transition"]["idempotent"] is True


def test_normal_successful_background_abandon_needs_no_recovery(tmp_path, monkeypatch):
    generation_id, snapshot, control = _background_case(
        "backfill",
        phase="abandoned",
        finalize_state="success",
        advance_state="success",
    )
    owner = snapshot["owner_matches"][0]
    owner["state"] = "success"
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[control],
        snapshots=[snapshot],
    )
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: pytest.fail("completed background is immutable"),
    )

    with pytest.raises(mod.RecoveryError, match="needs no recovery"):
        mod.recover_automatic_failure(_arguments(tmp_path, generation_id=generation_id))


def test_ready_abandon_lost_response_is_idempotent(tmp_path, monkeypatch):
    abandoned = {
        **_control(phase="abandoned", status="succeeded", active=False),
        "released": True,
        "published": False,
        "released_at": "2026-08-08T18:01:00+00:00",
        "idempotent": True,
    }
    snapshots = [
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        ),
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        ),
    ]
    for snapshot in snapshots:
        snapshot["pause_states_before"] = dict(mod.ALL_PAUSED_STATES)
    _install_common(
        monkeypatch,
        tmp_path,
        controls=[abandoned, abandoned, abandoned],
        snapshots=snapshots,
    )
    monkeypatch.setattr(
        mod, "_observe_shared", lambda *_args, **_kwargs: _shared_snapshot()
    )
    monkeypatch.setattr(mod, "_abandon_unclaimed", lambda *_args, **_kwargs: abandoned)

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert report["passed"] is True
    assert report["publication_transition"]["idempotent"] is True


def test_ready_recovery_rejects_nonterminal_or_started_downstream(
    tmp_path, monkeypatch
):
    ready = _control(phase="ready", status="succeeded")
    snapshots = [
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        )
    ]
    _install_common(monkeypatch, tmp_path, controls=[ready], snapshots=snapshots)
    shared = _shared_snapshot()
    shared["consumer_run"]["task_states"]["trigger_xref_transforms"] = "running"
    monkeypatch.setattr(mod, "_observe_shared", lambda *_args, **_kwargs: shared)
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: pytest.fail("ready lock must remain retained"),
    )

    with pytest.raises(mod.RecoveryError, match="terminal task proof"):
        mod.recover_automatic_failure(_arguments(tmp_path))


def test_ready_recovery_rejects_running_task_under_failed_downstream(
    tmp_path, monkeypatch
):
    ready = _control(phase="ready", status="succeeded")
    snapshots = [
        _isolated_snapshot(
            owner_state="success", ingest_state="success", silver_state="success"
        )
    ]
    _install_common(monkeypatch, tmp_path, controls=[ready], snapshots=snapshots)
    shared = _shared_snapshot()
    shared["active_bound_task_instances"] = [
        {
            "dag_id": "dag_transform_xref",
            "run_id": "failed-but-task-live",
            "task_id": "xref_player",
            "state": "running",
        }
    ]
    monkeypatch.setattr(mod, "_observe_shared", lambda *_args, **_kwargs: shared)
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: pytest.fail("live shared task keeps lock retained"),
    )

    with pytest.raises(mod.RecoveryError, match="active consumer/downstream task"):
        mod.recover_automatic_failure(_arguments(tmp_path))


def test_consuming_generation_is_never_released_and_writes_actionable_report(
    tmp_path, monkeypatch
):
    consuming = _control(phase="consuming", status="succeeded")
    consuming["consumer"] = {
        "dag_id": mod.SHARED_CONSUMER_DAG_ID,
        "run_id": SOFA_RUN_ID,
    }
    _install_common(monkeypatch, tmp_path, controls=[consuming], snapshots=[])
    monkeypatch.setattr(
        mod,
        "_observe_isolated",
        lambda *_args, **_kwargs: pytest.fail("consuming must not be blind-paused"),
    )
    monkeypatch.setattr(
        mod,
        "_fail_and_release",
        lambda *_args, **_kwargs: pytest.fail("consuming must never release"),
    )
    monkeypatch.setattr(
        mod,
        "_abandon_unclaimed",
        lambda *_args, **_kwargs: pytest.fail("consuming must never abandon"),
    )

    report = mod.recover_automatic_failure(_arguments(tmp_path))

    assert report["passed"] is False
    assert report["phase"] == "blocked_consuming"
    assert report["recovery_required"] is True
    assert report["safe_action"] == "retain_lock_and_inspect_exact_consumer"
    assert json.loads(_arguments(tmp_path).output.read_text()) == report


def test_confirmation_is_checked_before_any_runtime_call(tmp_path, monkeypatch):
    arguments = _arguments(tmp_path)
    arguments.confirm = "wrong"
    monkeypatch.setattr(
        mod,
        "_deployment_context",
        lambda _args: pytest.fail("deployment must not be read"),
    )

    with pytest.raises(mod.RecoveryError, match="nothing changed"):
        mod.recover_automatic_failure(arguments)


def test_output_must_stay_inside_protected_evidence_directory(tmp_path, monkeypatch):
    arguments = _arguments(tmp_path)
    arguments.output = tmp_path / "outside.json"
    _install_common(monkeypatch, tmp_path, controls=[_control()])

    with pytest.raises(mod.RecoveryError, match="evidence directory"):
        mod.recover_automatic_failure(arguments)


@pytest.mark.parametrize("target", ("deployment", "outside"))
def test_main_never_overwrites_unvalidated_error_path(tmp_path, monkeypatch, target):
    arguments = _arguments(tmp_path)
    deployment = arguments.deployment_report
    deployment.write_bytes(b"protected-deployment-bytes\n")
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"protected-outside-bytes\n")
    arguments.output = deployment if target == "deployment" else outside
    expected = arguments.output.read_bytes()
    context = _context(tmp_path)
    context["deployment_report"] = str(deployment.resolve())
    monkeypatch.setattr(mod, "_parser", lambda _argv: arguments)
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: context)
    monkeypatch.setattr(
        mod,
        "recover_automatic_failure",
        lambda _args: (_ for _ in ()).throw(mod.RecoveryError("blocked")),
    )

    assert mod.main([]) == 2
    assert arguments.output.read_bytes() == expected


def test_isolated_commands_use_exact_admitted_container_not_compose_service(tmp_path):
    arguments = _arguments(tmp_path)
    context = _context(tmp_path)
    calls = []
    marker = "RECOVERY_TEST_JSON="

    def run(command, **_kwargs):
        calls.append(command)
        return __import__("subprocess").CompletedProcess(
            command, 0, stdout=marker + "{}\n", stderr=""
        )

    assert (
        mod._isolated_python_json(
            arguments,
            context,
            code="print('ok')",
            marker=marker,
            run=run,
        )
        == {}
    )
    assert calls == [
        (
            "docker",
            "exec",
            context["scheduler_container_id"],
            "python",
            "-c",
            "print('ok')",
        )
    ]
