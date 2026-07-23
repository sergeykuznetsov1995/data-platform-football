"""Finite accelerated-bootstrap scheduler and pause contracts."""

from __future__ import annotations

import hashlib
import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dags.scripts import whoscored_bootstrap as bootstrap
from scripts.whoscored_proxy_campaign import _expected_daily_logical_date


def _slots() -> list[dict[str, str]]:
    first = datetime(2026, 7, 15, 10, tzinfo=timezone.utc)
    return [
        {
            "logical_date": (first + timedelta(days=index))
            .isoformat()
            .replace("+00:00", "Z"),
            "run_id": bootstrap.scheduled_run_id(first + timedelta(days=index)),
            "wave_id": bootstrap.BOOTSTRAP_WAVES[index],
        }
        for index in range(6)
    ]


def _authority(*, wave_id: str = "wave-all") -> dict[str, object]:
    return {
        "acceptance_mode": bootstrap.ACCEPTANCE_MODE,
        "bootstrap_slots": _slots(),
        "capacity_receipt_sha256": "a" * 64,
        "provider_order_cap_bytes": 300_000_000,
        "wave_id": wave_id,
    }


def _pointer() -> dict[str, object]:
    unsigned = {
        "schema_version": 1,
        "acceptance_mode": bootstrap.ACCEPTANCE_MODE,
        "bootstrap_slots": _slots(),
        "capacity_receipt_sha256": "a" * 64,
        "provider_order_cap_bytes": 300_000_000,
        "rollout_id": "rollout-954",
        "runtime_sha256": "b" * 64,
        "provider_policy_sha256": "c" * 64,
    }
    return {
        **unsigned,
        "authority_sha256": hashlib.sha256(
            bootstrap.canonical_json_bytes(unsigned)
        ).hexdigest(),
        "signature": "d" * 64,
    }


def _run_pointer(run_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "dag_id": bootstrap.WHOSCORED_INGEST_DAG_ID,
        "run_id": run_id,
        "approval_id": "wsdaily-approval-" + "1" * 32,
        "approval_sha256": "2" * 64,
    }


def _write_run_pointer(root, run_id: str) -> None:
    name = hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json"
    path = root / name
    path.write_bytes(bootstrap.canonical_json_bytes(_run_pointer(run_id)) + b"\n")
    path.chmod(0o600)


def _metadata_record(
    slot: dict[str, str],
    *,
    state: str = "success",
    terminal_task_state: str = "success",
) -> dict[str, object]:
    logical_date = datetime.fromisoformat(
        slot["logical_date"].replace("Z", "+00:00")
    )
    return {
        "run_id": slot["run_id"],
        "run_type": "scheduled",
        "external_trigger": False,
        "conf": {},
        "state": state,
        "execution_date": logical_date,
        "data_interval_start": logical_date,
        "terminal_task_state": terminal_task_state,
    }


@pytest.mark.unit
def test_timetable_emits_exact_six_then_cron_data_interval() -> None:
    now = datetime(2026, 7, 22, 9, 15, tzinfo=timezone.utc)
    timetable = bootstrap.AcceleratedBootstrapTimetable(
        _slots(), now_factory=lambda: now, pointer_ready=lambda _run_id: True
    )
    restriction = bootstrap.TimeRestriction(
        earliest=datetime(2024, 1, 1, tzinfo=timezone.utc),
        latest=None,
        catchup=False,
    )
    last = None
    for expected in _slots():
        info = timetable.next_dagrun_info(
            last_automated_data_interval=last,
            restriction=restriction,
        )
        assert info is not None
        logical = datetime.fromisoformat(
            expected["logical_date"].replace("Z", "+00:00")
        )
        assert info.data_interval.start == logical
        assert info.data_interval.end == logical
        assert info.run_after == logical
        last = info.data_interval

    daily = timetable.next_dagrun_info(
        last_automated_data_interval=last,
        restriction=restriction,
    )
    assert daily is not None
    assert daily.data_interval.start == _expected_daily_logical_date(now)
    assert daily.data_interval.end == datetime(
        2026, 7, 22, 10, tzinfo=timezone.utc
    )
    assert daily.run_after == daily.data_interval.end

    following = timetable.next_dagrun_info(
        last_automated_data_interval=daily.data_interval,
        restriction=restriction,
    )
    assert following is not None
    assert following.data_interval.start == datetime(
        2026, 7, 22, 10, tzinfo=timezone.utc
    )
    assert following.run_after == datetime(
        2026, 7, 23, 10, tzinfo=timezone.utc
    )


@pytest.mark.unit
def test_timetable_waits_for_each_exact_run_pointer(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    now = datetime(2026, 7, 22, 9, 15, tzinfo=timezone.utc)
    monkeypatch.setenv(bootstrap.BOOTSTRAP_POINTER_ROOT_ENV, str(tmp_path))
    timetable = bootstrap.AcceleratedBootstrapTimetable(
        _slots(), now_factory=lambda: now
    )
    restriction = bootstrap.TimeRestriction(
        earliest=datetime(2024, 1, 1, tzinfo=timezone.utc),
        latest=None,
        catchup=False,
    )

    assert timetable.next_dagrun_info(
        last_automated_data_interval=None, restriction=restriction
    ) is None
    _write_run_pointer(tmp_path, _slots()[0]["run_id"])
    first = timetable.next_dagrun_info(
        last_automated_data_interval=None, restriction=restriction
    )
    assert first is not None

    # A failed first DagRun still advances Airflow's automated frontier. The
    # absent next issuance pointer must keep slot 1 from being created.
    assert timetable.next_dagrun_info(
        last_automated_data_interval=first.data_interval,
        restriction=restriction,
    ) is None
    _write_run_pointer(tmp_path, _slots()[1]["run_id"])
    second = timetable.next_dagrun_info(
        last_automated_data_interval=first.data_interval,
        restriction=restriction,
    )
    assert second is not None
    assert second.data_interval.start == datetime(
        2026, 7, 16, 10, tzinfo=timezone.utc
    )

    last = bootstrap.DataInterval.exact(
        datetime(2026, 7, 20, 10, tzinfo=timezone.utc)
    )
    assert timetable.next_dagrun_info(
        last_automated_data_interval=last, restriction=restriction
    ) is None
    daily_run_id = "scheduled__2026-07-21T10:00:00+00:00"
    _write_run_pointer(tmp_path, daily_run_id)
    daily = timetable.next_dagrun_info(
        last_automated_data_interval=last, restriction=restriction
    )
    assert daily is not None
    assert bootstrap.scheduled_run_id(daily.data_interval.start) == daily_run_id


@pytest.mark.unit
def test_metadata_preflight_rejects_frontier_and_slot_collisions() -> None:
    old = datetime(2026, 7, 14, 10, tzinfo=timezone.utc)
    historical = {
        "run_id": bootstrap.scheduled_run_id(old),
        "run_type": "scheduled",
        "external_trigger": False,
        "conf": {},
        "state": "success",
        "execution_date": old,
        "data_interval_start": old,
        "terminal_task_state": "",
    }
    result = bootstrap.validate_bootstrap_metadata_preflight(
        _slots(), [historical], phase="publish"
    )
    assert result["sealed_predecessor_count"] == 0

    at_slot_zero = {**historical, "execution_date": old + timedelta(days=1)}
    at_slot_zero["data_interval_start"] = at_slot_zero["execution_date"]
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="collides"):
        bootstrap.validate_bootstrap_metadata_preflight(
            _slots(), [at_slot_zero], phase="publish"
        )

    frontier_collision = {
        **historical,
        "run_id": "backfill__foreign",
        "run_type": "backfill_job",
        "execution_date": old + timedelta(days=1, hours=1),
        "data_interval_start": old + timedelta(days=1, hours=1),
    }
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="frontier"):
        bootstrap.validate_bootstrap_metadata_preflight(
            _slots(), [frontier_collision], phase="publish"
        )


@pytest.mark.unit
def test_metadata_preflight_binds_each_issue_to_sealed_predecessor() -> None:
    first = _metadata_record(_slots()[0])
    result = bootstrap.validate_bootstrap_metadata_preflight(
        _slots(), [first], phase="issue", run_id=_slots()[1]["run_id"]
    )
    assert result["sealed_predecessor_count"] == 1

    failed_receipt = {**first, "terminal_task_state": "failed"}
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="sealed"):
        bootstrap.validate_bootstrap_metadata_preflight(
            _slots(),
            [failed_receipt],
            phase="issue",
            run_id=_slots()[1]["run_id"],
        )

    precreated_next = _metadata_record(_slots()[1], state="queued")
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="already exists"):
        bootstrap.validate_bootstrap_metadata_preflight(
            _slots(),
            [first, precreated_next],
            phase="issue",
            run_id=_slots()[1]["run_id"],
        )

    all_green = [_metadata_record(slot) for slot in _slots()]
    completed = bootstrap.validate_bootstrap_metadata_preflight(
        _slots(), all_green, phase="complete"
    )
    assert completed["sealed_predecessor_count"] == 6

    post_bootstrap_daily = {
        **_metadata_record(_slots()[-1]),
        "run_id": "scheduled__2026-07-22T10:00:00+00:00",
        "execution_date": datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
        "data_interval_start": datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
    }
    repeated_daily_preflight = bootstrap.validate_bootstrap_metadata_preflight(
        _slots(), [*all_green, post_bootstrap_daily], phase="complete"
    )
    assert repeated_daily_preflight["sealed_predecessor_count"] == 6
    assert repeated_daily_preflight["latest_automated_data_interval_start"] == (
        "2026-07-22T10:00:00Z"
    )


@pytest.mark.unit
def test_timetable_serialization_is_exact_and_detached() -> None:
    timetable = bootstrap.AcceleratedBootstrapTimetable(_slots())
    serialized = timetable.serialize()
    restored = bootstrap.AcceleratedBootstrapTimetable.deserialize(serialized)

    assert restored.serialize() == {"bootstrap_slots": _slots()}
    serialized["bootstrap_slots"][0]["wave_id"] = "mutated"
    assert timetable.serialize() == {"bootstrap_slots": _slots()}
    with pytest.raises(bootstrap.AirflowTimetableInvalid):
        bootstrap.AcceleratedBootstrapTimetable.deserialize(
            {"bootstrap_slots": _slots(), "extra": True}
        )


@pytest.mark.unit
def test_production_schedule_rejects_mutated_or_symlink_pointer(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    pointer_path = tmp_path / bootstrap.BOOTSTRAP_POINTER_NAME
    pointer_path.write_bytes(bootstrap.canonical_json_bytes(_pointer()) + b"\n")
    pointer_path.chmod(0o600)
    monkeypatch.setenv(bootstrap.BOOTSTRAP_POINTER_ROOT_ENV, str(tmp_path))

    schedule = bootstrap.production_schedule()
    assert isinstance(schedule, bootstrap.AcceleratedBootstrapTimetable)
    assert schedule.bootstrap_slots == _slots()

    mutated = _pointer()
    mutated["capacity_receipt_sha256"] = "e" * 64
    pointer_path.write_bytes(bootstrap.canonical_json_bytes(mutated) + b"\n")
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="content digest"):
        bootstrap.production_schedule()

    pointer_path.unlink()
    target = tmp_path / "target.json"
    target.write_bytes(bootstrap.canonical_json_bytes(_pointer()) + b"\n")
    pointer_path.symlink_to(target)
    with pytest.raises(bootstrap.WhoScoredBootstrapError, match="not protected"):
        bootstrap.production_schedule()


@pytest.mark.unit
def test_pause_barrier_handles_every_bootstrap_slot_and_normal_daily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    paused: list[bool] = []
    monkeypatch.setattr(dag, "_set_whoscored_dag_paused", lambda: paused.append(True))

    assert dag.pause_after_bootstrap_slot(
        scope_plan={"schema_version": 2},
        run_id="manual__smoke",
        logical_date=datetime(2026, 7, 22, tzinfo=timezone.utc),
    ) == {"status": "not_required", "reason": "non_bootstrap_run"}

    for slot_index in range(6):
        slot = _slots()[slot_index]
        result = dag.pause_after_bootstrap_slot(
            scope_plan=_authority(wave_id=slot["wave_id"]),
            run_id=slot["run_id"],
            logical_date=datetime.fromisoformat(
                slot["logical_date"].replace("Z", "+00:00")
            ),
        )
        assert result["status"] == "paused"
        assert result["reason"] == "accepted_slot_boundary"
        assert result["slot_index"] == slot_index
    assert paused == [True] * 6

    normal = dag.pause_after_bootstrap_slot(
        scope_plan=_authority(wave_id="wave-all"),
        run_id="scheduled__2026-07-22T10:00:00+00:00",
        logical_date=datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
    )
    assert normal == {"status": "not_required", "reason": "normal_daily"}


@pytest.mark.unit
def test_terminal_seal_failure_propagates_after_bootstrap_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    slot = _slots()[0]
    paused: list[bool] = []
    monkeypatch.setattr(dag, "_set_whoscored_dag_paused", lambda: paused.append(True))

    def fail_seal(**_kwargs) -> None:
        raise dag.AirflowException("acceptance receipt write failed")

    monkeypatch.setattr(
        dag,
        "_record_rollout_acceptance_at_terminal_task",
        fail_seal,
    )

    with pytest.raises(dag.AirflowException, match="acceptance receipt write failed"):
        dag.seal_rollout_acceptance_and_pause(
            scope_plan=_authority(wave_id=slot["wave_id"]),
            run_id=slot["run_id"],
            logical_date=datetime.fromisoformat(
                slot["logical_date"].replace("Z", "+00:00")
            ),
        )

    assert paused == [True]


@pytest.mark.unit
def test_terminal_seal_preserves_receipt_error_when_pause_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    slot = _slots()[0]

    def fail_seal(**_kwargs) -> None:
        raise dag.AirflowException("acceptance receipt write failed")

    def fail_pause() -> None:
        raise dag.AirflowException("DagModel pause failed")

    monkeypatch.setattr(
        dag,
        "_record_rollout_acceptance_at_terminal_task",
        fail_seal,
    )
    monkeypatch.setattr(dag, "_set_whoscored_dag_paused", fail_pause)

    with pytest.raises(
        dag.AirflowException, match="acceptance receipt write failed"
    ) as captured:
        dag.seal_rollout_acceptance_and_pause(
            scope_plan=_authority(wave_id=slot["wave_id"]),
            run_id=slot["run_id"],
            logical_date=datetime.fromisoformat(
                slot["logical_date"].replace("Z", "+00:00")
            ),
        )

    assert len(captured.value.__notes__) == 1
    assert captured.value.__notes__[0].startswith(
        "WhoScored bootstrap pause also failed after receipt sealing ("
    )


@pytest.mark.unit
def test_combined_terminal_task_does_not_pause_normal_daily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    monkeypatch.setattr(
        dag,
        "_record_rollout_acceptance_at_terminal_task",
        lambda **_kwargs: {
            "status": "not_counted",
            "reason": "bootstrap_slots_complete",
        },
    )
    monkeypatch.setattr(
        dag,
        "_set_whoscored_dag_paused",
        lambda: pytest.fail("normal daily must remain unpaused"),
    )

    result = dag.seal_rollout_acceptance_and_pause(
        scope_plan=_authority(wave_id="wave-all"),
        run_id="scheduled__2026-07-22T10:00:00+00:00",
        logical_date=datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
    )

    assert result == {
        "acceptance": {
            "status": "not_counted",
            "reason": "bootstrap_slots_complete",
        },
        "pause": {"status": "not_required", "reason": "normal_daily"},
    }


@pytest.mark.unit
def test_early_authority_barrier_fails_closed_without_paid_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    pointer = _pointer()
    monkeypatch.setattr(dag, "load_bootstrap_pointer", lambda **_kwargs: pointer)
    monkeypatch.setattr(
        dag,
        "_transport_runtime",
        lambda _context: SimpleNamespace(is_paid=False),
    )
    slot = _slots()[0]
    dag_run = SimpleNamespace(
        run_id=slot["run_id"],
        run_type="scheduled",
        external_trigger=False,
        conf={},
        execution_date=datetime.fromisoformat(
            slot["logical_date"].replace("Z", "+00:00")
        ),
    )

    with pytest.raises(dag.AirflowException, match="lacks paid authority"):
        dag.validate_whoscored_bootstrap_authority(dag_run=dag_run)


@pytest.mark.unit
def test_early_authority_barrier_allows_charter_rotation_between_waves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = importlib.import_module("dag_ingest_whoscored")
    pointer = _pointer()
    monkeypatch.setattr(dag, "load_bootstrap_pointer", lambda **_kwargs: pointer)

    def authority(wave_id: str, charter_sha256: str) -> SimpleNamespace:
        return SimpleNamespace(
            **_authority(wave_id=wave_id),
            rollout_id="rollout-954",
            runtime_sha256="b" * 64,
            provider_policy_sha256="c" * 64,
            charter_sha256=charter_sha256,
        )

    for slot_index, charter_sha256 in ((0, "1" * 64), (2, "2" * 64)):
        slot = _slots()[slot_index]
        signed = authority(slot["wave_id"], charter_sha256)
        monkeypatch.setattr(
            dag,
            "_transport_runtime",
            lambda _context, signed=signed: SimpleNamespace(
                is_paid=True,
                approval=SimpleNamespace(scheduled_authority=signed),
            ),
        )
        dag_run = SimpleNamespace(
            run_id=slot["run_id"],
            run_type="scheduled",
            external_trigger=False,
            conf={},
            execution_date=datetime.fromisoformat(
                slot["logical_date"].replace("Z", "+00:00")
            ),
        )
        result = dag.validate_whoscored_bootstrap_authority(dag_run=dag_run)
        assert result["status"] == "success"
        assert result["slot_index"] == slot_index
