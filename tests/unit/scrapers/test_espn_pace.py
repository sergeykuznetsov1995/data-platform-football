from dataclasses import replace
from datetime import datetime, timedelta, timezone
import multiprocessing

import pytest

from scrapers.espn.gate import TransportGate, load_transport_policy
from scrapers.espn.pace import PaceWindow, evaluate

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)
POLICY = load_transport_policy()


def window(**changes):
    return replace(PaceWindow(1, NOW-timedelta(days=1), NOW, 1000, 0, 0, 500, 0, True, True), **changes)


@pytest.mark.parametrize('changes,reason', [
    ({'attempts': 0}, 'empty_window'), ({'count_429': 1}, 'http429'),
    ({'error_attempts': 5}, 'error_share'), ({'http_p95_ms': 1500}, 'http_p95_limit'),
    ({'reset_count': 1}, 'reset_in_window'), ({'freshness_ok': None}, 'freshness_unknown_or_low'),
    ({'coverage_ok': None}, 'incomplete_coverage'), ({'attempts': None}, 'unknown_metrics'),
    ({'http_p95_ms': float('nan')}, 'unknown_latency'),
    ({'http_p95_ms': 751}, 'baseline_regression'),
    ({'eligible_intervals': 10, 'loaded_intervals': 8}, 'insufficient_load'),
    ({'eligible_intervals': 0, 'loaded_intervals': 0}, 'insufficient_load'),
    ({'hold_active': True}, 'protection_active'),
])
def test_fail_closed(changes, reason):
    assert evaluate(window(**changes), 500, POLICY).reason == reason


@pytest.mark.parametrize('step,hours', [(0, 2), (1, 24), (2, 24)])
def test_exact_window_and_thresholds(step, hours):
    w = window(step=step, started_at=NOW-timedelta(hours=hours), http_p95_ms=750,
               eligible_intervals=10, loaded_intervals=9)
    assert evaluate(w, 500, POLICY).eligible
    assert evaluate(w, 500, POLICY).next_step == step + 1
    assert not evaluate(replace(w, started_at=w.started_at+timedelta(microseconds=1)), 500, POLICY).eligible
    assert evaluate(replace(w, error_attempts=4), 500, POLICY).eligible


def test_aware_time_required():
    with pytest.raises(ValueError):
        evaluate(window(ended_at=NOW.replace(tzinfo=None)), 500, POLICY)


class Clock:
    def __init__(self): self.now = NOW
    def __call__(self): return self.now
    def sleep(self, seconds): self.now += timedelta(seconds=seconds)


def gate_at(path, clock, ceiling=3):
    return TransportGate(POLICY, path, step_ceiling=ceiling, utcnow_fn=clock, sleep_fn=clock.sleep)


def promote(gate, clock):
    snap = gate.snapshot()
    start = clock.now
    clock.sleep(POLICY.pace.minimum_window_seconds[snap['step']])
    w = window(step=snap['step'], started_at=start, ended_at=clock.now)
    result = gate.confirm(w, 500, expected_revision=snap['revision'])
    assert result.eligible, result
    return w


def test_default_shared_confirmation_restart_and_env_cap(tmp_path):
    clock = Clock(); path = tmp_path/'gate.json'
    gate = gate_at(path, clock)
    assert gate.acquire('core').step == 0
    promote(gate, clock)
    other = gate_at(path, clock)
    assert other.acquire('core').step == 1
    assert gate_at(path, clock, 0).acquire('core').step == 0
    assert other.snapshot()['confirmed_ceiling'] == 1


def test_reset_invalidates_decision_and_preserves_hold(tmp_path):
    clock = Clock(); gate = gate_at(tmp_path/'gate.json', clock)
    promote(gate, clock)
    snap = gate.snapshot(); start = clock.now
    clock.sleep(86400)
    w = window(started_at=start, ended_at=clock.now)
    gate.report(gate.acquire('core'), status=429)
    assert gate.confirm(w, 500, expected_revision=snap['revision']).reason == 'stale_revision'
    gate.report(gate.acquire('core'), status=429)
    hold = gate.snapshot()['hold_until']
    assert not gate.confirm(w, 500, expected_revision=gate.snapshot()['revision']).eligible
    assert gate.snapshot()['hold_until'] == hold
    clock.sleep(86400)
    assert len([e for e in gate.snapshot()['events'] if e['kind']=='reset']) == 2


def test_s3_expires_on_acquire_without_controller(tmp_path):
    clock = Clock(); path = tmp_path/'gate.json'; gate = gate_at(path, clock)
    for _ in range(3): promote(gate, clock)
    assert gate.acquire('core').step == 3
    clock.sleep(21600)
    restarted = gate_at(path, clock)
    assert restarted.acquire('core').step == 2
    assert restarted.snapshot()['confirmed_ceiling'] == 2
    assert any(e['reason']=='s3_expired' for e in restarted.snapshot()['events'])


def _confirm_child(path, now, w, revision, queue):
    gate = TransportGate(POLICY, path, utcnow_fn=lambda: now)
    queue.put(gate.confirm(w, 500, expected_revision=revision).eligible)


def test_two_processes_cannot_double_promote(tmp_path):
    clock = Clock(); path = tmp_path/'gate.json'; gate = gate_at(path, clock)
    revision = gate.snapshot()['revision']; start = clock.now; clock.sleep(7200)
    w = window(step=0, started_at=start, ended_at=clock.now)
    ctx = multiprocessing.get_context('fork'); queue = ctx.Queue()
    children = [ctx.Process(target=_confirm_child, args=(path, clock.now, w, revision, queue)) for _ in range(2)]
    for child in children: child.start()
    for child in children: child.join(10); assert child.exitcode == 0
    assert sorted(queue.get(timeout=2) for _ in children) == [False, True]
    assert gate.snapshot()['confirmed_ceiling'] == 1
