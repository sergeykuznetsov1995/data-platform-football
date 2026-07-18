"""Focused tests for the persistent WhoScored source circuit."""

from __future__ import annotations

import fcntl
import json
import multiprocessing
import os
import threading
from pathlib import Path
from queue import Empty
from typing import Any

import pytest

from scrapers.whoscored.source_circuit import (
    DEFAULT_COOLDOWN_FACTOR,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_JITTER_SECONDS,
    DEFAULT_PROBE_LEASE_SECONDS,
    MAX_COOLDOWN_SECONDS,
    CircuitPermit,
    SharedSourceCircuit,
    SourceCircuitConfigurationError,
    SourceCircuitOpen,
    SourceCircuitStateError,
)


class FakeClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = float(value)
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(float(seconds))
        self.value += float(seconds)


def _new_circuit(
    tmp_path: Path,
    *,
    clock: FakeClock | None = None,
    path: Path | None = None,
    random_uniform: Any = None,
    nonce_factory: Any = None,
    cooldown_seconds: float = 2.0,
    cooldown_factor: float = 2.0,
    max_cooldown_seconds: float = 8.0,
    jitter_seconds: float = 0.0,
    probe_lease_seconds: float = 3.0,
) -> SharedSourceCircuit:
    current_clock = clock or FakeClock()
    kwargs: dict[str, Any] = {
        "cooldown_seconds": cooldown_seconds,
        "cooldown_factor": cooldown_factor,
        "max_cooldown_seconds": max_cooldown_seconds,
        "jitter_seconds": jitter_seconds,
        "probe_lease_seconds": probe_lease_seconds,
        "wait_poll_seconds": 0.5,
        "clock": current_clock.time,
        "sleep": current_clock.sleep,
    }
    if random_uniform is not None:
        kwargs["random_uniform"] = random_uniform
    if nonce_factory is not None:
        kwargs["nonce_factory"] = nonce_factory
    return SharedSourceCircuit(
        path or tmp_path / "source-circuit" / "state.json",
        **kwargs,
    )


def _raw_state(circuit: SharedSourceCircuit) -> dict[str, Any]:
    return json.loads(circuit.path.read_text(encoding="utf-8"))


def _overwrite_state(circuit: SharedSourceCircuit, value: object) -> None:
    circuit.path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    circuit.path.chmod(0o600)


def _multiprocess_claim(
    path: str,
    start: multiprocessing.synchronize.Event,
    output: multiprocessing.queues.Queue,
) -> None:
    circuit = SharedSourceCircuit(
        path,
        cooldown_seconds=1.0,
        cooldown_factor=2.0,
        max_cooldown_seconds=4.0,
        jitter_seconds=0.0,
        probe_lease_seconds=10.0,
        clock=lambda: 2.0,
    )
    start.wait(5.0)
    try:
        permit = circuit.admit()
    except SourceCircuitOpen as exc:
        output.put(("blocked", exc.state))
    else:
        output.put(("probe" if permit.is_probe else "closed", permit.generation))


def test_production_policy_defaults_are_fixed() -> None:
    assert DEFAULT_COOLDOWN_SECONDS == 15 * 60
    assert DEFAULT_COOLDOWN_FACTOR == 2.0
    assert MAX_COOLDOWN_SECONDS == 60 * 60
    assert DEFAULT_JITTER_SECONDS == 60.0
    assert DEFAULT_PROBE_LEASE_SECONDS == 180.0


def test_first_admission_creates_private_secret_free_closed_state(tmp_path: Path) -> None:
    circuit = _new_circuit(tmp_path)

    permit = circuit.admit()

    assert permit == CircuitPermit(generation=0)
    assert circuit.snapshot().state == "closed"
    assert circuit.path.stat().st_mode & 0o777 == 0o600
    assert circuit.lock_path.stat().st_mode & 0o777 == 0o600
    document = _raw_state(circuit)
    assert set(document) == {
        "schema_version",
        "state",
        "generation",
        "level",
        "updated_at",
        "not_before",
        "probe_lease_until",
        "probe_nonce",
    }
    assert document == {
        "schema_version": 1,
        "state": "closed",
        "generation": 0,
        "level": 0,
        "updated_at": 0.0,
        "not_before": None,
        "probe_lease_until": None,
        "probe_nonce": None,
    }
    rendered = circuit.path.read_text(encoding="utf-8").lower()
    for forbidden in ("url", "cookie", "session", "airflow", "context", "token"):
        assert forbidden not in rendered
    assert not list(circuit.path.parent.glob(".*.tmp"))


def test_trip_persists_one_jitter_and_fail_fast_never_sleeps(tmp_path: Path) -> None:
    clock = FakeClock(100.0)
    random_calls: list[tuple[float, float]] = []

    def jitter(low: float, high: float) -> float:
        random_calls.append((low, high))
        return 7.0

    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        cooldown_seconds=900,
        max_cooldown_seconds=3600,
        jitter_seconds=60,
        random_uniform=jitter,
    )
    opened = circuit.trip(circuit.admit())

    assert opened.state == "open"
    assert opened.generation == 1
    assert opened.level == 0
    assert opened.not_before == 1007.0
    assert random_calls == [(0.0, 60.0)]

    second = _new_circuit(
        tmp_path,
        clock=clock,
        cooldown_seconds=900,
        max_cooldown_seconds=3600,
        jitter_seconds=60,
        random_uniform=jitter,
    )
    with pytest.raises(SourceCircuitOpen) as raised:
        second.admit(wait=False)

    assert raised.value.retryable is True
    assert raised.value.kind == "cooldown"
    assert raised.value.retry_at == 1007.0
    assert random_calls == [(0.0, 60.0)]
    assert clock.sleeps == []


def test_wait_mode_polls_then_claims_the_only_probe(tmp_path: Path) -> None:
    clock = FakeClock()
    circuit = _new_circuit(tmp_path, clock=clock)
    circuit.trip(circuit.admit())

    probe = circuit.admit(wait=True)

    assert probe.is_probe
    assert clock.value == 2.0
    assert clock.sleeps == [0.5, 0.5, 0.5, 0.5]
    snapshot = circuit.snapshot()
    assert snapshot.state == "half_open"
    assert snapshot.probe_lease_until == 5.0
    # A transport carries this permit from direct HTTP to its browser without
    # calling admit again. Even the same circuit object cannot issue it twice.
    with pytest.raises(SourceCircuitOpen) as raised:
        circuit.admit()
    assert raised.value.state == "half_open"
    assert raised.value.retry_at == 5.0


def test_only_one_process_can_claim_an_expired_open_circuit(tmp_path: Path) -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("process-level flock test requires fork")
    path = tmp_path / "multiprocess" / "state.json"
    setup = SharedSourceCircuit(
        path,
        cooldown_seconds=1.0,
        cooldown_factor=2.0,
        max_cooldown_seconds=4.0,
        jitter_seconds=0.0,
        probe_lease_seconds=10.0,
        clock=lambda: 0.0,
    )
    setup.trip(setup.admit())

    context = multiprocessing.get_context("fork")
    start = context.Event()
    output = context.Queue()
    processes = [
        context.Process(target=_multiprocess_claim, args=(str(path), start, output))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    results = []
    try:
        for _ in processes:
            results.append(output.get(timeout=5.0))
    except Empty as exc:  # pragma: no cover - diagnostic guard
        raise AssertionError("circuit claim process did not report") from exc
    finally:
        for process in processes:
            process.join(timeout=5.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5.0)

    assert sorted(item[0] for item in results) == ["blocked", "probe"]
    assert all(process.exitcode == 0 for process in processes)


def test_generation_and_nonce_ignore_stale_outcomes(tmp_path: Path) -> None:
    clock = FakeClock()
    nonces = iter(("1" * 32, "2" * 32))
    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        nonce_factory=lambda: next(nonces),
    )
    original = circuit.admit()
    circuit.trip(original)
    clock.value = 2.0
    probe = circuit.admit()
    closed = circuit.succeed(probe)

    assert closed.state == "closed"
    assert closed.generation == 3
    assert circuit.trip(original).state == "closed"
    assert circuit.trip(probe).state == "closed"
    assert circuit.snapshot().generation == 3


def test_probe_failures_back_off_15_30_60_minutes_with_persisted_jitter(
    tmp_path: Path,
) -> None:
    clock = FakeClock(0.0)
    jitters = iter((10.0, 20.0, 30.0))
    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        cooldown_seconds=900,
        cooldown_factor=2,
        max_cooldown_seconds=3600,
        jitter_seconds=60,
        random_uniform=lambda _low, _high: next(jitters),
        probe_lease_seconds=180,
    )

    first = circuit.trip(circuit.admit())
    assert first.level == 0
    assert first.not_before == 910.0

    clock.value = 910.0
    second = circuit.trip(circuit.admit())
    assert second.level == 1
    assert second.not_before == 910.0 + 1800.0 + 20.0

    clock.value = second.not_before
    assert clock.value is not None
    third = circuit.trip(circuit.admit())
    assert third.level == 2
    assert third.not_before == clock.value + 3600.0 + 30.0


def test_inconclusive_and_abandon_reopen_without_escalating(tmp_path: Path) -> None:
    clock = FakeClock()
    circuit = _new_circuit(tmp_path, clock=clock)
    circuit.trip(circuit.admit())
    clock.value = 2.0
    probe = circuit.admit()

    clock.value = 2.5
    reopened = circuit.inconclusive(probe)
    assert reopened.state == "open"
    assert reopened.level == 0
    assert reopened.not_before == 4.5

    clock.value = 4.5
    replacement = circuit.admit()
    clock.value = 5.0
    abandoned = circuit.abandon(replacement)
    assert abandoned.state == "open"
    assert abandoned.level == 0
    assert abandoned.not_before == 7.0


def test_expired_probe_lease_recovers_after_sigkill_and_rejects_old_nonce(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    nonces = iter(("a" * 32, "b" * 32))
    first = _new_circuit(
        tmp_path,
        clock=clock,
        nonce_factory=lambda: next(nonces),
        probe_lease_seconds=3,
    )
    first.trip(first.admit())
    clock.value = 2.0
    abandoned_process_probe = first.admit()

    replacement = _new_circuit(
        tmp_path,
        clock=clock,
        nonce_factory=lambda: next(nonces),
        probe_lease_seconds=3,
    )
    with pytest.raises(SourceCircuitOpen) as raised:
        replacement.admit()
    assert raised.value.state == "half_open"
    assert raised.value.retry_at == 5.0

    clock.value = 5.0
    replacement_probe = replacement.admit()
    assert replacement_probe.is_probe
    assert replacement_probe != abandoned_process_probe
    assert first.succeed(abandoned_process_probe).state == "half_open"
    assert replacement.succeed(replacement_probe).state == "closed"


def test_probe_owner_cannot_reuse_or_close_its_own_expired_lease(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    nonces = iter(("c" * 32, "d" * 32))
    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        nonce_factory=lambda: next(nonces),
        probe_lease_seconds=3,
    )
    circuit.trip(circuit.admit())
    clock.value = 2.0
    expired = circuit.admit()

    clock.value = 5.1
    replacement = circuit.admit()

    assert replacement.is_probe
    assert replacement != expired
    assert circuit.succeed(expired).state == "half_open"
    assert circuit.succeed(replacement).state == "closed"


def test_late_probe_outcome_is_accepted_until_a_replacement_is_claimed(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        probe_lease_seconds=3,
    )
    circuit.trip(circuit.admit())
    clock.value = 2.0
    slow_probe = circuit.admit()

    # The request started under the only valid lease.  Its exact identity is
    # still authoritative after the lease deadline because nobody has claimed
    # a replacement generation.
    clock.value = 5.1
    reopened = circuit.trip(slow_probe)

    assert reopened.state == "open"
    assert reopened.level == 1
    assert reopened.not_before == 9.1


@pytest.mark.parametrize(
    "document",
    [
        {
            "schema_version": 1,
            "state": "open",
            "generation": 1,
            "level": 2,
            "updated_at": 100.0,
            "not_before": 102.0,
            "probe_lease_until": None,
            "probe_nonce": None,
        },
        {
            "schema_version": 1,
            "state": "open",
            "generation": 1,
            "level": 1,
            "updated_at": 100.0,
            "not_before": 108.0,
            "probe_lease_until": None,
            "probe_nonce": None,
        },
        {
            "schema_version": 1,
            "state": "half_open",
            "generation": 2,
            "level": 0,
            "updated_at": 100.0,
            "not_before": 100.0,
            "probe_lease_until": 100.0,
            "probe_nonce": "a" * 32,
        },
    ],
)
def test_state_rejects_unconstructible_cooldown_or_probe_deadline(
    tmp_path: Path,
    document: dict[str, Any],
) -> None:
    circuit = _new_circuit(
        tmp_path,
        cooldown_seconds=2,
        cooldown_factor=2,
        max_cooldown_seconds=8,
        jitter_seconds=1,
        probe_lease_seconds=3,
    )
    circuit.admit()
    _overwrite_state(circuit, document)

    with pytest.raises(SourceCircuitStateError):
        circuit.snapshot()


@pytest.mark.parametrize(
    "document",
    [
        {},
        {
            "schema_version": 1,
            "state": "closed",
            "generation": 0,
            "level": 0,
            "updated_at": 0.0,
            "not_before": None,
            "probe_lease_until": None,
            "probe_nonce": None,
            "url": "https://forbidden.invalid/",
        },
        {
            "schema_version": 99,
            "state": "closed",
            "generation": 0,
            "level": 0,
            "updated_at": 0.0,
            "not_before": None,
            "probe_lease_until": None,
            "probe_nonce": None,
        },
        {
            "schema_version": 1,
            "state": "open",
            "generation": 1,
            "level": 0,
            "updated_at": 0.0,
            "not_before": None,
            "probe_lease_until": None,
            "probe_nonce": None,
        },
        {
            "schema_version": 1,
            "state": "half_open",
            "generation": 2,
            "level": 0,
            "updated_at": 2.0,
            "not_before": 2.0,
            "probe_lease_until": 5.0,
            "probe_nonce": "not-a-valid-nonce",
        },
    ],
)
def test_corrupt_or_sensitive_state_fails_closed_before_admission(
    tmp_path: Path, document: object
) -> None:
    circuit = _new_circuit(tmp_path)
    circuit.admit()
    _overwrite_state(circuit, document)

    with pytest.raises(SourceCircuitStateError) as raised:
        circuit.admit()

    assert raised.value.kind == "config"
    assert raised.value.retryable is False


def test_duplicate_json_and_oversized_state_fail_closed(tmp_path: Path) -> None:
    circuit = _new_circuit(tmp_path)
    circuit.admit()
    circuit.path.write_text(
        '{"schema_version":1,"schema_version":1}\n', encoding="utf-8"
    )
    circuit.path.chmod(0o600)
    with pytest.raises(SourceCircuitStateError):
        circuit.snapshot()

    circuit.path.write_bytes(b"{" + b"x" * 5000 + b"}")
    circuit.path.chmod(0o600)
    with pytest.raises(SourceCircuitStateError):
        circuit.admit()


def test_wrong_mode_hardlink_and_state_symlink_are_rejected(tmp_path: Path) -> None:
    wrong_mode = _new_circuit(tmp_path, path=tmp_path / "mode" / "state.json")
    wrong_mode.admit()
    wrong_mode.path.chmod(0o644)
    with pytest.raises(SourceCircuitStateError):
        wrong_mode.snapshot()

    linked = _new_circuit(tmp_path, path=tmp_path / "link" / "state.json")
    linked.admit()
    os.link(linked.path, linked.path.with_name("second-link.json"))
    with pytest.raises(SourceCircuitStateError):
        linked.admit()

    symlinked = _new_circuit(tmp_path, path=tmp_path / "symlink" / "state.json")
    symlinked.admit()
    target = symlinked.path.with_name("target.json")
    target.write_text("{}\n", encoding="utf-8")
    symlinked.path.unlink()
    symlinked.path.symlink_to(target)
    with pytest.raises(SourceCircuitStateError):
        symlinked.snapshot()


def test_lock_symlink_and_parent_symlink_are_rejected(tmp_path: Path) -> None:
    lock_circuit = _new_circuit(tmp_path, path=tmp_path / "lock" / "state.json")
    lock_circuit.path.parent.mkdir(parents=True)
    lock_target = lock_circuit.lock_path.with_name("lock-target")
    lock_target.write_text("", encoding="utf-8")
    lock_circuit.lock_path.symlink_to(lock_target)
    with pytest.raises(SourceCircuitStateError):
        lock_circuit.admit()

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    parent_circuit = _new_circuit(
        tmp_path, path=linked_parent / "state.json"
    )
    with pytest.raises(SourceCircuitConfigurationError):
        parent_circuit.admit()


def test_symlink_ancestor_does_not_create_descendants_in_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "symlink-target"
    target.mkdir()
    linked_ancestor = tmp_path / "linked-ancestor"
    linked_ancestor.symlink_to(target, target_is_directory=True)
    circuit = _new_circuit(
        tmp_path,
        path=linked_ancestor / "must-not-exist" / "state.json",
    )

    with pytest.raises(SourceCircuitConfigurationError):
        circuit.admit()

    assert not (target / "must-not-exist").exists()
    assert list(target.iterdir()) == []


def test_parent_swap_after_flock_fails_without_creating_a_split_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "swap-after-flock"
    parent.mkdir()
    displaced = tmp_path / "displaced-after-flock"
    circuit = _new_circuit(tmp_path, path=parent / "state.json")
    swap_requested = threading.Event()
    swap_finished = threading.Event()
    swap_errors: list[BaseException] = []

    def swap_parent() -> None:
        try:
            assert swap_requested.wait(5.0)
            parent.rename(displaced)
            parent.mkdir(mode=0o700)
        except BaseException as exc:  # pragma: no cover - diagnostic guard
            swap_errors.append(exc)
        finally:
            swap_finished.set()

    original_flock = fcntl.flock

    def flock_then_swap(descriptor: int, operation: int) -> None:
        original_flock(descriptor, operation)
        if operation == fcntl.LOCK_EX:
            swap_requested.set()
            assert swap_finished.wait(5.0)

    swap_thread = threading.Thread(target=swap_parent)
    swap_thread.start()
    monkeypatch.setattr(fcntl, "flock", flock_then_swap)
    try:
        with pytest.raises(SourceCircuitStateError, match="parent changed"):
            circuit.admit()
    finally:
        swap_thread.join(timeout=5.0)

    assert not swap_thread.is_alive()
    assert swap_errors == []
    assert (displaced / "state.json.lock").is_file()
    assert not (displaced / "state.json").exists()
    assert list(parent.iterdir()) == []


def test_parent_swap_during_replace_fails_without_writing_replacement_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "swap-during-replace"
    circuit = _new_circuit(tmp_path, path=parent / "state.json")
    permit = circuit.admit()
    displaced = tmp_path / "displaced-during-replace"
    swap_requested = threading.Event()
    swap_finished = threading.Event()
    swap_errors: list[BaseException] = []

    def swap_parent() -> None:
        try:
            assert swap_requested.wait(5.0)
            parent.rename(displaced)
            parent.mkdir(mode=0o700)
        except BaseException as exc:  # pragma: no cover - diagnostic guard
            swap_errors.append(exc)
        finally:
            swap_finished.set()

    original_replace = os.replace

    def replace_after_swap(
        source: Any,
        destination: Any,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        swap_requested.set()
        assert swap_finished.wait(5.0)
        original_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    swap_thread = threading.Thread(target=swap_parent)
    swap_thread.start()
    monkeypatch.setattr(os, "replace", replace_after_swap)
    try:
        with pytest.raises(SourceCircuitStateError, match="commit failed"):
            circuit.trip(permit)
    finally:
        swap_thread.join(timeout=5.0)

    assert not swap_thread.is_alive()
    assert swap_errors == []
    assert json.loads((displaced / "state.json").read_text())["state"] == "open"
    assert (displaced / "state.json.lock").is_file()
    assert not list(displaced.glob(".*.tmp"))
    assert list(parent.iterdir()) == []


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"cooldown_seconds": 0}, "cooldown"),
        ({"cooldown_factor": 0.5}, "factor"),
        ({"cooldown_seconds": 9, "max_cooldown_seconds": 8}, "maximum"),
        ({"jitter_seconds": -1}, "jitter"),
        ({"probe_lease_seconds": 0}, "probe"),
    ],
)
def test_invalid_policy_is_rejected(
    tmp_path: Path, overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(SourceCircuitConfigurationError, match=message):
        _new_circuit(tmp_path, **overrides)


def test_relative_path_and_non_boolean_wait_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(SourceCircuitConfigurationError, match="absolute"):
        SharedSourceCircuit("relative/state.json")
    circuit = _new_circuit(tmp_path)
    with pytest.raises(SourceCircuitConfigurationError, match="boolean"):
        circuit.admit(wait=1)  # type: ignore[arg-type]


def test_invalid_clock_jitter_nonce_and_permit_fail_closed(tmp_path: Path) -> None:
    bad_clock = SharedSourceCircuit(
        tmp_path / "clock" / "state.json", clock=lambda: float("nan")
    )
    with pytest.raises(SourceCircuitStateError, match="clock"):
        bad_clock.admit()

    bad_jitter = _new_circuit(
        tmp_path,
        path=tmp_path / "jitter" / "state.json",
        jitter_seconds=1,
        random_uniform=lambda _low, _high: 2,
    )
    with pytest.raises(SourceCircuitStateError, match="jitter"):
        bad_jitter.trip(bad_jitter.admit())

    clock = FakeClock()
    bad_nonce = _new_circuit(
        tmp_path,
        clock=clock,
        path=tmp_path / "nonce" / "state.json",
        nonce_factory=lambda: "unsafe",
    )
    bad_nonce.trip(bad_nonce.admit())
    clock.value = 2
    with pytest.raises(SourceCircuitStateError, match="nonce"):
        bad_nonce.admit()

    valid = _new_circuit(tmp_path, path=tmp_path / "permit" / "state.json")
    with pytest.raises(SourceCircuitConfigurationError, match="permit"):
        valid.trip(CircuitPermit(generation=-1))


def test_snapshot_never_exposes_probe_nonce(tmp_path: Path) -> None:
    clock = FakeClock()
    circuit = _new_circuit(
        tmp_path,
        clock=clock,
        nonce_factory=lambda: "f" * 32,
    )
    circuit.trip(circuit.admit())
    clock.value = 2
    circuit.admit()

    snapshot = circuit.snapshot()

    assert snapshot.state == "half_open"
    assert not hasattr(snapshot, "probe_nonce")
    assert "f" * 32 not in repr(snapshot)
