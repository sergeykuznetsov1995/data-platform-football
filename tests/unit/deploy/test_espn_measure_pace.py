"""Controller, workload, isolation and runtime CLI tests; no origin or live Trino."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import threading
from types import SimpleNamespace

import pytest

from scrapers.espn.attempts import AttemptJournal
from scrapers.espn.gate import TransportGate, load_transport_policy
from scrapers.espn.measure_pace import (Controller, MeasurementStopped, accepted_ids, digest,
                                      main, read_status, validate_state)
from scrapers.espn.pace_report import format_measurement, load_evidence
from scrapers.espn.pace_store import ControllerBusy, PaceStore
from scrapers.espn.transport_contracts import LaneClosed

pytestmark = pytest.mark.unit
START = datetime(2026, 10, 1, 23, 45, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.now = START
    def __call__(self):
        return self.now


class Trino:
    def __init__(self):
        self.debt = 0
        self.owner = threading.get_ident()
        self.connection = object()
        self.unknown = False
    def execute_query(self, sql, *args):
        assert threading.get_ident() == self.owner
        if self.unknown:
            raise RuntimeError('offline')
        if 'AS debt' in sql:
            return [[self.debt]]
        return [['eng.1', 100, 100, 0, 0, 0]]


class Journal:
    def __init__(self):
        self.data = []
        self.complete = True
    def rows(self, start, end):
        return [r for r in self.data if start <= datetime.fromisoformat(r['requested_at']) < end]
    def coverage(self, start, end):
        return self.complete and bool(self.rows(start, end))
    def flush(self, conn):
        return 0


@pytest.fixture
def controller(tmp_path):
    clock = Clock()
    policy = load_transport_policy()
    policy = replace(policy, pace=replace(policy.pace, minimum_window_seconds=(300, 300, 86400)))
    gate = TransportGate(policy, tmp_path / 'gate.json', lane='history', step_ceiling=3,
                         utcnow_fn=clock, sleep_fn=lambda _: None)
    result = Controller(gate=gate, journal=Journal(), store=PaceStore(tmp_path / 'pace.sqlite3'),
                        ids=(1, 2, 3), trino=Trino(), targets=('eng.1',),
                        client_factory=lambda *_: None, stop_file=tmp_path / 'measurement.off', now=clock)
    result.initialize()
    result.clock = clock
    return result


def fill_window(c, seconds=300, *, load=True, observation_gap=False, latency=100):
    start = c.state['start']
    step = c.state['step']
    count_per_bin = int(c.gate.policy.steps[step] * 0.5 * 5 * 0.8)
    count = int(count_per_bin * seconds / 300) if load else 1
    for index in range(count):
        requested = datetime.fromtimestamp(start + index * seconds / count, timezone.utc)
        c.journal.data.append(dict(attempt_id=f'{step}-{start}-{index}',
            requested_at=requested.isoformat(), origin='https://site.web.api.espn.com',
            endpoint='summary', lane='history', step=step, status=200, timeout=False,
            http_ms=latency, complete=True, measurement_id=c.state['measurement_id']))
    observations = []
    for offset in range(10, seconds + 1, 10):
        if observation_gap and offset == 20:
            continue
        if observation_gap and offset == 30:
            continue
        if observation_gap and offset == 40:
            continue
        observations.append(dict(at=start + offset, known=True, debt=0, protected=False,
                                 step=step, revision=c.state['revision'], error=None))
    with c.store.db() as db:
        db.executemany('INSERT INTO evidence(kind, at, payload) VALUES (?, ?, ?)',
            [('observation', o['at'], json.dumps({k: v for k, v in o.items() if k != 'at'}))
             for o in observations])
    c.clock.now = datetime.fromtimestamp(start + seconds, timezone.utc)
    c.observation = observations[-1]
    c.state['heartbeat'] = start + seconds
    c.freshness = dict(day=(c.clock().date() - timedelta(days=1)).isoformat(),
                       due=100, ok=100, eligible=True, checked_at=start + seconds)


def test_steps_require_loaded_complete_windows_and_stop_at_s2(controller):
    c = controller
    fill_window(c)
    assert c.decide()['reason'] == 'eligible'
    assert c.gate.snapshot()['confirmed_ceiling'] == 1
    assert c.state['baseline_p95_ms'] == 100
    fill_window(c)
    assert c.decide()['reason'] == 'eligible'
    assert c.gate.snapshot()['confirmed_ceiling'] == 2
    fill_window(c, 86400)
    report = c.decide()
    assert report['reason'] == 'accepted_s2'
    assert c.state['status'] == 'complete'
    assert c.gate.snapshot()['confirmed_ceiling'] == 2
    assert c.state['accepted_step'] == 2
    assert len(c.state['completed']) == 3


@pytest.mark.parametrize('mode,reason', [('underload', 'insufficient_load'),
                                        ('gap', 'incomplete_coverage'),
                                        ('spool', 'incomplete_coverage'),
                                        ('freshness', 'freshness_unknown_or_low')])
def test_missing_or_bad_evidence_never_promotes(controller, mode, reason):
    c = controller
    fill_window(c, load=mode != 'underload', observation_gap=mode == 'gap')
    if mode == 'spool':
        c.journal.complete = False
    if mode == 'freshness':
        c.freshness['eligible'] = False
    assert c.decide()['reason'] == reason
    assert c.gate.snapshot()['confirmed_ceiling'] == 0


def test_known_pause_is_not_unknown_evidence(controller):
    c = controller
    fill_window(c)
    observations = c.store.rows('observation', c.state['start'], c.observation['at'])
    for o in observations:
        o['debt'] = 1
    result = load_evidence(c.journal.data, observations, START, c.clock(), step=0, policy=c.gate.policy)
    assert result['continuous']
    assert result['paused_intervals'] == 1
    assert result['eligible_intervals'] == 0
    observations[2]['known'] = False
    result = load_evidence(c.journal.data, observations, START, c.clock(), step=0, policy=c.gate.policy)
    assert not result['continuous']
    assert result['unknown_intervals'] == 1


def test_restart_retains_identity_baseline_but_records_gap_and_restarts_window(controller):
    c = controller
    fill_window(c)
    c.decide()
    previous = c.store.get()
    c.clock.now += timedelta(seconds=61)
    c.observation = None
    c.initialize()
    assert c.state['measurement_id'] == previous['measurement_id']
    assert c.state['baseline_p95_ms'] == 100
    assert c.state['step'] == 1
    assert c.state['start'] == c.clock().timestamp()
    assert c.state['start'] > previous['start']
    assert len(c.store.rows('lifecycle', 0, c.clock().timestamp())) >= 2


def test_second_process_lock_cannot_touch_first_controller(tmp_path):
    import subprocess
    import sys
    store = PaceStore(tmp_path / 'pace.sqlite3')
    store.save({'sentinel': True})
    with store.lock():
        code = "from scrapers.espn.pace_store import PaceStore; s=PaceStore(%r); s.lock().__enter__()" % str(store.path)
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
        assert result.returncode != 0
        assert 'another ESPN pace controller' in result.stderr
    assert store.get() == {'sentinel': True}


def test_stop_and_unknown_live_debt_pause_before_http(controller):
    c = controller
    c.stop_file.touch()
    with pytest.raises(MeasurementStopped):
        c.check()
    c.stop_file.unlink()
    c.trino.unknown = True
    c.clock.now += timedelta(seconds=10)
    c.observe(force=True)
    with pytest.raises(LaneClosed, match='unknown'):
        c.check()
    assert not c.observation['known']


def test_reset_restarts_window_and_lowering_keeps_hold(controller):
    c = controller
    fill_window(c)
    c.decide()
    permit = c.gate.acquire('core')
    c.gate.report(permit, status=429)
    c.clock.now += timedelta(seconds=10)
    c.observe(force=True)
    assert c.state['start'] == c.clock().timestamp()
    before = c.gate.snapshot()
    after = c.gate.lower_ceiling(0)
    assert after['confirmed_ceiling'] == 0
    assert after['cooldown_until'] == before['cooldown_until']
    assert after['history_frozen_until'] == before['history_frozen_until']
    assert not c.gate.lower_ceiling(3)['confirmed_ceiling']


def test_real_attempt_spool_missing_completion_blocks_coverage(tmp_path):
    clock = Clock()
    journal = AttemptJournal(tmp_path / 'attempts.sqlite3', utcnow_fn=clock)
    attempt = journal.begin(run_id='r', task_id='m', requested_at=clock(),
                            origin='https://site.web.api.espn.com', endpoint='summary',
                            lane='history', step=0, measurement_id='m')
    assert not journal.coverage(clock(), clock() + timedelta(seconds=1))
    journal.finish(attempt, status=200, timeout=False, http_ms=10, direct_bytes=10)
    assert journal.coverage(clock(), clock() + timedelta(seconds=1))
    assert len(journal.rows(clock(), clock() + timedelta(seconds=1))) == 1
    assert journal.rows(clock() + timedelta(seconds=1), clock() + timedelta(seconds=2)) == []


def test_status_missing_and_dead_heartbeat_warns_without_network(controller, capsys):
    c = controller
    assert read_status(c.store.path, now=c.clock())['stale'] is False
    c.clock.now += timedelta(seconds=31)
    assert read_status(c.store.path, now=c.clock())['stale']
    assert '⚠️' in format_measurement(read_status(c.store.path, now=c.clock()))[0]
    main(['status', '--state-dir', str(c.store.path.parent / 'missing'), '--format', 'json'])
    assert json.loads(capsys.readouterr().out)['stale']


def test_workload_hash_and_persisted_state_validation(controller):
    c = controller
    with pytest.raises(ValueError, match='incompatible'):
        validate_state(c.state, c.state['policy_hash'], digest([99]))
    state = dict(c.state, start=float('nan'))
    with pytest.raises(ValueError, match='timestamp'):
        validate_state(state, state['policy_hash'], state['workload_hash'])


def test_p95_uses_raw_profile_rows_and_keeps_global_errors(controller):
    c = controller
    fill_window(c)
    c.journal.data[0]['status'] = 403
    c.journal.data[0]['origin'] = 'https://site.api.espn.com'
    c.journal.data[0]['measurement_id'] = None
    report = c.decide()
    assert report['reason'] == 'error_share'
    assert report['metrics']['attempts_b'] == 1
    assert report['metrics']['count_403'] == 1
    assert report['profile_p95_ms'] == 100


def test_run_stop_drains_and_returns_to_accepted_step(controller, monkeypatch):
    c = controller
    fill_window(c)
    c.decide()
    assert c.gate.snapshot()['confirmed_ceiling'] == 1
    c.observation = None
    c.stop_file.touch()
    c.run_locked()
    assert c.state['status'] == 'stopped'
    assert c.gate.snapshot()['confirmed_ceiling'] == 0


def test_accepted_ids_exact_season_and_cardinality():
    queries = []
    trino = SimpleNamespace(execute_query=lambda sql: queries.append(sql) or [[i] for i in range(380)])
    assert accepted_ids(trino) == list(range(380))
    assert "competition_slug='eng.1' AND season_year=2015" in queries[0]
    trino.execute_query = lambda _: [[1]]
    with pytest.raises(ValueError, match='380'):
        accepted_ids(trino)


def test_supervisor_run_does_not_clear_explicit_stop(tmp_path, capsys):
    stop = tmp_path / 'measurement.off'
    stop.touch()
    assert main(['run', '--state-dir', str(tmp_path)]) == 0
    assert stop.exists()
    assert not (tmp_path / 'gate.json').exists()
    assert 'explicitly stopped' in capsys.readouterr().out


def test_supervisor_run_completed_is_idle_without_network(tmp_path):
    store = PaceStore(tmp_path / 'pace.sqlite3')
    store.save({'status': 'complete'})
    assert main(['run', '--state-dir', str(tmp_path)]) == 0
    assert not (tmp_path / 'gate.json').exists()


def test_boundary_benchmark_runs_once_and_new_window_starts_afterwards(controller):
    c = controller
    calls = []
    def benchmark(step):
        calls.append(step)
        assert c.store.get()['step'] == 1
        c.clock.now += timedelta(seconds=40)
        return {'p95_seconds': 1.2, 'isolated': True}
    c.benchmark_fn = benchmark
    fill_window(c)
    report = c.decide()
    assert calls == [0]
    assert report['isolated_benchmark']['isolated']
    assert c.state['start'] == c.clock().timestamp()
    assert c.state['step'] == 1
    assert c.state['accepted_step'] == 0


def test_s3_expiry_finishes_on_s2_under_virtual_clock(controller):
    c = controller
    c.max_step = 3
    with c.gate._state() as state:
        state['confirmed_ceiling'] = state['step'] = 3
        state['s3_expires_at'] = c.clock().timestamp() + 21600
        c.gate._event(state, c.clock().timestamp(), 'step', 'confirmed', 3)
    snap = c.gate.snapshot()
    c.state.update(step=3, revision=snap['revision'], epoch=snap['measurement_started_at'], accepted_step=2)
    c.clock.now += timedelta(seconds=21600)
    c.observe(force=True)
    assert c.gate.snapshot()['confirmed_ceiling'] == 2
    assert c.state['status'] == 'complete'
    assert c.state['accepted_step'] == 2


def test_benchmarks_record_all_completed_steps_and_failures_are_visible(controller):
    c = controller
    called = []
    c.benchmark_fn = lambda step: called.append(step) or {'isolated': True, 'p95_seconds': step + 1}
    for seconds in (300, 300, 86400):
        fill_window(c, seconds)
        c.decide()
    assert called == [0, 1, 2]
    assert [r['step'] for r in c.state['completed']] == [0, 1, 2]
    assert all(r['isolated_benchmark']['isolated'] for r in c.state['completed'])


def test_failed_boundary_benchmark_does_not_claim_write_measurement(controller):
    c = controller
    def fail(step):
        raise RuntimeError('isolated write failed')
    c.benchmark_fn = fail
    fill_window(c)
    with pytest.raises(RuntimeError, match='isolated write failed'):
        c.decide()
    assert 'isolated_benchmark' not in c.state['completed'][-1]
    assert c.state['status'] != 'complete'


def test_tick_requests_drain_instead_of_running_benchmark_with_workers(controller):
    c = controller
    called = []
    c.benchmark_fn = lambda step: called.append(step)
    fill_window(c)
    c.tick()
    assert c.decision_due
    assert not called
    with pytest.raises(LaneClosed, match='drain'):
        c.check()
    c.finish_boundary()
    assert called == [0]


def test_final_benchmark_cannot_accept_new_protection_without_revision_change(controller):
    c = controller
    for _ in range(2):
        fill_window(c)
        c.decide()
    fill_window(c, 86400)
    def protect(step):
        assert step == 2
        with c.gate._state() as state:
            state['history_frozen_until'] = c.clock().timestamp() + 1800
        return {'isolated': True, 'p95_seconds': 1}
    c.benchmark_fn = protect
    report = c.decide()
    assert report['reason'] == 'gate_changed_during_benchmark'
    assert c.state['status'] != 'complete'
    assert c.state['accepted_step'] == 1
