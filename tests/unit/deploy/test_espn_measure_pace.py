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
    def flush(self, conn, **kwargs):
        return 0
    def pending(self, **kwargs):
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
    c.benchmark_fn = lambda step: called.append(step) or {'isolated': True, 'p95_seconds': 1}
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


def test_first_known_observation_after_unknown_starts_fresh_qualifying_window(controller):
    c = controller
    c.trino.unknown = True
    c.clock.now += timedelta(seconds=10)
    c.observe(force=True)
    unknown_start = c.state['start']
    assert not c.observation['known']
    c.trino.unknown = False
    c.clock.now += timedelta(seconds=10)
    c.observe(force=True)
    assert c.state['start'] > unknown_start
    assert c.state['start'] == c.clock().timestamp()
    fill_window(c, 600)
    assert c.decide()['reason'] == 'eligible'
    assert c.gate.snapshot()['confirmed_ceiling'] == 1


def test_checkout_wrapper_help_from_arbitrary_directory(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    wrapper = Path(__file__).resolve().parents[3] / 'deploy' / 'espn' / 'measure_pace.py'
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    result = subprocess.run([sys.executable, str(wrapper), '--help'], cwd=tmp_path,
                            env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert 'benchmark-at-boundaries' in result.stdout
    assert 'run,start,resume,status,stop,benchmark' in result.stdout


@pytest.mark.parametrize('protection', ['reset', 'history_freeze'])
def test_final_acceptance_rechecks_gate_atomically_after_freshness_sql(controller, protection):
    c = controller
    for _ in range(2):
        fill_window(c)
        c.decide()
    fill_window(c, 86400)
    original_query = c.trino.execute_query
    def freshness_with_concurrent_protection(sql, *args):
        result = original_query(sql, *args)
        if 'AS deadline' in sql:
            if protection == 'reset':
                c.gate.report(c.gate.acquire('core'), status=429)
            else:
                with c.gate._state() as state:
                    state['history_frozen_until'] = c.clock().timestamp() + 1800
        return result
    c.trino.execute_query = freshness_with_concurrent_protection
    report = c.decide()
    assert report['reason'] in ('stale_revision', 'protection_active')
    assert c.state['status'] != 'complete'
    assert c.state['accepted_step'] == 1
    assert not any(e['kind'] == 'accepted' for e in c.gate.snapshot()['events'])


def test_supervisor_failure_restart_runs_without_creating_operator_stop(tmp_path, monkeypatch):
    from scrapers.espn import measure_pace, trino_manager
    store = PaceStore(tmp_path / 'pace.sqlite3')
    store.save({'status': 'failed', 'accepted_step': 0})
    calls = []
    monkeypatch.setattr(trino_manager, 'EspnTrinoTableManager', lambda: Trino())
    monkeypatch.setattr(measure_pace, 'accepted_ids', lambda _: list(range(380)))
    monkeypatch.setattr(Controller, 'run_locked', lambda self: calls.append(self))
    assert main(['run', '--state-dir', str(tmp_path), '--benchmark-at-boundaries']) == 0
    assert len(calls) == 1
    assert not (tmp_path / 'measurement.off').exists()


def test_documented_supervisor_never_installs_persistent_stop_as_execstop():
    from pathlib import Path
    text = (Path(__file__).resolve().parents[3] / 'deploy' / 'espn' / 'README.md').read_text()
    command = next(line for line in text.splitlines() if line.startswith('systemd-run --unit=espn-1510-measure'))
    assert 'Restart=on-failure' in command
    assert '--benchmark-at-boundaries' in command
    assert 'ExecStop=' not in command
    assert '--allow-s3' not in command


def test_full_s3_opt_in_progression_expires_to_previously_accepted_s2(controller):
    c = controller
    c.max_step = 3
    for seconds in (300, 300, 86400):
        fill_window(c, seconds)
        assert c.decide()['reason'] == 'eligible'
    assert c.gate.snapshot()['confirmed_ceiling'] == 3
    assert c.state['accepted_step'] == 2
    assert c.state['status'] == 'running'
    c.clock.now += timedelta(seconds=21600)
    c.observe(force=True)
    assert c.gate.snapshot()['confirmed_ceiling'] == 2
    assert c.state['status'] == 'complete'
    assert c.state['report']['reason'] == 's3_expired_return_s2'


def test_slow_final_benchmark_is_reused_without_relaxing_evidence_age(controller):
    c = controller
    for _ in range(2):
        fill_window(c)
        c.decide()
    fill_window(c, 86400)
    calls = []
    def slow(step):
        calls.append(step)
        c.clock.now += timedelta(seconds=301)
        return {'isolated': True, 'p95_seconds': 100}
    c.benchmark_fn = slow
    report = c.decide()
    assert report['reason'] == 'stale_window'
    assert c.state['status'] == 'running'
    measured_at = report['isolated_benchmark']['measured_at']
    assert c.store.get()['benchmarks']['2']['measured_at'] == measured_at
    # Fresh evidence needs a new uninterrupted window after the observation gap.
    c.observe(force=True)
    fill_window(c, 86400)
    report = c.decide()
    assert report['reason'] == 'accepted_s2'
    assert report['isolated_benchmark']['measured_at'] == measured_at
    assert calls == [2]


def test_slow_debt_result_never_renews_http_admission(controller):
    c = controller
    original = c.trino.execute_query
    def slow(sql, *args):
        result = original(sql, *args)
        if 'AS debt' in sql:
            c.clock.now += timedelta(seconds=31)
        return result
    c.trino.execute_query = slow
    c.observe(force=True)
    with pytest.raises(LaneClosed, match='unknown or stale'):
        c.check()
    assert not c.observation['known']


class AsyncSQL:
    """Production lane fixture: one manager/connection per constructing thread."""
    def __init__(self):
        self.owner = threading.get_ident()
        self.name = threading.current_thread().name.rsplit('-', 1)[-1]
        self.connection = self
        self.closed = False
    def execute_query(self, sql):
        assert threading.get_ident() == self.owner
        return [[0]] if 'AS debt' in sql else [['eng.1', 100, 100, 0, 0, 0]]
    def close(self):
        assert threading.get_ident() == self.owner
        self.closed = True


def eventually(predicate, timeout=3):
    import time
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail('condition did not become true')
        time.sleep(0.005)


def async_io(c, *, factory=AsyncSQL, read=None, publish=None, publish_seconds=60):
    from scrapers.espn.measure_pace import read_freshness
    from scrapers.espn.pace_io import PaceIO
    if publish is not None:
        c.journal.flush = publish
    c.io = PaceIO(factory=factory, gate=c.gate, journal=c.journal, store=c.store,
                  targets=c.targets, now=c.now, read_freshness=read or read_freshness,
                  observe_seconds=0.01, freshness_seconds=60, publish_seconds=publish_seconds)
    c.io.start()
    c.io_started = True
    return c.io


def test_async_observer_survives_blocked_publication_freshness_and_aggregation(controller):
    c = controller
    release = threading.Event()
    publishing, freshness = threading.Event(), threading.Event()
    managers = []
    def factory():
        manager = AsyncSQL()
        managers.append(manager)
        return manager
    def publish(connection, **kwargs):
        assert connection.owner == threading.get_ident()
        assert kwargs == dict(max_batches=1, blocking=False)
        publishing.set()
        assert release.wait(5)
        return 0
    def fresh(manager, targets, now):
        assert manager.owner == threading.get_ident()
        freshness.set()
        assert release.wait(5)
        return dict(eligible=True, checked_at=now.timestamp(), day='2026-09-30')
    io = async_io(c, factory=factory, read=fresh, publish=publish)
    try:
        assert publishing.wait(2) and freshness.wait(2)
        eventually(lambda: io.snapshot('debt'))
        c.observe()
        initial = len(c.store.rows('observation', 0, c.now().timestamp()))
        # Deliberately block owner aggregation until independent observations land.
        original = c.evidence
        def slow_evidence():
            eventually(lambda: len(c.store.rows('observation', 0, c.now().timestamp())) >= initial + 3)
            return original()
        c.evidence = slow_evidence
        assert c.decide()['reason'] != 'eligible'
        c.check()  # Fresh debt allows HTTP despite blocked unrelated SQL.
        assert len({manager.owner for manager in managers}) == 3
        assert all(manager.owner != c.owner for manager in managers)
        assert not release.is_set()
    finally:
        release.set()
        io.close()
    assert all(manager.closed for manager in managers)
    assert not any(thread.is_alive() for thread in io.threads)


def test_async_slow_debt_stays_stale_until_real_recovery(controller):
    c = controller
    entered, release = threading.Event(), threading.Event()
    class Slow(AsyncSQL):
        def execute_query(self, sql):
            if self.name == 'debt':
                entered.set()
                assert release.wait(5)
            return super().execute_query(sql)
    io = async_io(c, factory=Slow, publish=lambda *_args, **_kwargs: 0)
    try:
        assert entered.wait(2)
        with pytest.raises(LaneClosed, match='unknown or stale'):
            c.check()
        c.clock.now += timedelta(seconds=31)
        release.set()
        eventually(lambda: io.snapshot('debt'))
        # The first slow result is recorded as unknown, not a completion heartbeat.
        eventually(lambda: any(row.get('error') == 'debt_query_stale'
            for row in c.store.rows('observation', 0, c.now().timestamp())))
        eventually(lambda: io.snapshot('debt')['known'])
        c.observe()
        assert c.state['start'] == c.now().timestamp()
        c.check()
        c.clock.now += timedelta(seconds=31)
        with pytest.raises(LaneClosed, match='unknown or stale'):
            c.check()
    finally:
        release.set()
        io.close()


def test_async_publication_failure_is_durable_and_retry_is_coalesced(controller):
    c = controller
    calls = []
    def publish(conn, **kwargs):
        calls.append(conn)
        if len(calls) == 1:
            raise RuntimeError('write failed')
        return 200
    io = async_io(c, publish=publish, publish_seconds=0.05)
    try:
        eventually(lambda: io.snapshot('publication'))
        assert io.snapshot('publication')['error'] == 'RuntimeError'
        c.observe()
        assert c.store.rows('publication', 0, c.now().timestamp())[-1]['error'] == 'RuntimeError'
        assert len(calls) == 1
        io.request('publication')
        eventually(lambda: len(calls) == 2 and io.snapshot('publication')['error'] is None)
        assert calls[0] is calls[1]
        assert io.snapshot('publication')['published'] == 200
    finally:
        io.close()


def test_async_stop_waits_for_inflight_sql_and_closes_owned_connections(controller):
    c = controller
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    def publish(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return 0
    io = async_io(c, publish=publish)
    assert entered.wait(2)
    closer = threading.Thread(target=lambda: (io.close(), closed.set()))
    closer.start()
    try:
        assert not closed.wait(0.05)
        assert any(thread.is_alive() for thread in io.threads)
    finally:
        release.set()
        closer.join(3)
    assert closed.is_set()
    assert not any(thread.is_alive() for thread in io.threads)


def test_async_owner_polling_does_not_fabricate_observations_or_reset_covered_window(controller):
    c = controller
    io = async_io(c, publish=lambda *_args, **_kwargs: 0)
    try:
        eventually(lambda: io.snapshot('debt'))
        c.observe()
        initial = c.state['start']
        for _ in range(4):
            c.clock.now += timedelta(seconds=10)
            io.request('debt')
            eventually(lambda: io.snapshot('debt')['at'] == c.now().timestamp())
        # Owner was busy for >30s, but actual observation had no gap.
        c.observe()
        assert c.state['start'] == initial
        io.close()
        before = c.store.rows('observation', 0, c.now().timestamp())
        c.clock.now += timedelta(seconds=31)
        c.observe(force=True)
        assert c.store.rows('observation', 0, c.now().timestamp()) == before
        with pytest.raises(LaneClosed, match='unknown or stale'):
            c.check()
    finally:
        io.close()


@pytest.mark.parametrize('protection', [None, 'history_freeze', 'reset'])
def test_async_final_acceptance_uses_new_freshness_and_atomic_gate(controller, protection):
    from scrapers.espn.measure_pace import read_freshness
    c = controller
    c.gate.policy = replace(c.gate.policy, pace=replace(c.gate.policy.pace,
                           minimum_window_seconds=(300, 300, 300)))
    armed = threading.Event()
    calls = []
    def fresh(manager, targets, now):
        calls.append(now)
        result = read_freshness(manager, targets, now)
        if armed.is_set():
            if protection == 'reset':
                c.gate.report(c.gate.acquire('core'), status=429)
            elif protection == 'history_freeze':
                with c.gate._state() as state:
                    state['history_frozen_until'] = now.timestamp() + 1800
        return result
    io = async_io(c, read=fresh, publish=lambda *_args, **_kwargs: 0)
    try:
        eventually(lambda: io.snapshot('debt') and io.snapshot('freshness'))
        c.observe()
        for step in range(3):
            eventually(lambda: io.snapshot('debt')['known']
                       and io.snapshot('debt')['revision'] == c.gate.snapshot()['revision'])
            c.observe()
            for _ in range(30):
                c.clock.now += timedelta(seconds=10)
                io.request('debt')
                eventually(lambda: io.snapshot('debt')['at'] == c.now().timestamp())
            fill_window(c, 300)
            if step == 2:
                armed.set()
                before = len(calls)
            report = c.decide()
            if step < 2:
                assert report['reason'] == 'eligible'
        assert len(calls) > before
        if protection:
            assert report['reason'] in ('stale_revision', 'protection_active',
                                        'freshness_changed_during_benchmark',
                                        'observer_changed_during_benchmark')
            assert c.state['status'] != 'complete'
            assert c.state['accepted_step'] == 1
        else:
            assert report['reason'] == 'accepted_s2'
            assert c.state['status'] == 'complete'
    finally:
        io.close()


def test_async_run_stop_drains_and_restart_preserves_completed_baseline(controller):
    c = controller
    fill_window(c)
    c.decide()
    saved = c.store.get()
    c.journal.flush = lambda *_args, **_kwargs: 0
    c.journal.pending = lambda **_kwargs: 0
    def make():
        return Controller(gate=c.gate, journal=c.journal, store=c.store, ids=c.ids,
                          trino=Trino(), targets=c.targets, client_factory=lambda *_: None,
                          stop_file=c.stop_file, now=c.clock, io_factory=AsyncSQL)
    c.clock.now += timedelta(seconds=40)
    stopped = make()
    c.stop_file.touch()
    stopped.run_locked()
    assert stopped.state['status'] == 'stopped'
    assert stopped.state['publication']['final']
    assert stopped.state['publication']['pending'] == 0
    assert all(not thread.is_alive() for thread in stopped.io.threads)
    c.stop_file.unlink()
    resumed = make()
    try:
        resumed.initialize()
        eventually(lambda: resumed.io.snapshot('debt'))
        resumed.observe()
        assert resumed.state['measurement_id'] == saved['measurement_id']
        assert resumed.state['baseline_p95_ms'] == saved['baseline_p95_ms']
        assert resumed.state['completed'] == saved['completed']
        assert resumed.state['start'] > saved['start']
    finally:
        resumed.io.close()


def test_query_start_age_gap_is_not_continuous_evidence(controller):
    c = controller
    start = c.state['start']
    rows = [dict(at=start + 14, observed_at=start, known=True, debt=0, protected=False, step=0),
            dict(at=start + 38, observed_at=start + 24, known=True, debt=0, protected=False, step=0)]
    result = load_evidence([], rows, datetime.fromtimestamp(start + 14, timezone.utc),
                           datetime.fromtimestamp(start + 38, timezone.utc), step=0, policy=c.gate.policy)
    assert not result['continuous']


def test_async_evidence_write_failure_is_visible_and_fail_closed(controller):
    c = controller
    original = c.store.record
    def fail(kind, *args, **kwargs):
        if kind == 'observation':
            raise OSError('local disk failure')
        return original(kind, *args, **kwargs)
    c.store.record = fail
    io = async_io(c, publish=lambda *_args, **_kwargs: 0)
    try:
        eventually(lambda: io.snapshot('debt'))
        assert io.snapshot('debt')['error'] == 'evidence_write_OSError'
        with pytest.raises(LaneClosed, match='unknown or stale'):
            c.check()
    finally:
        io.close()


def test_bounded_outbox_retries_exact_versions_and_final_cutoff(tmp_path, monkeypatch):
    from scrapers.espn import attempts
    from scrapers.espn.pace_io import drain_publication
    journal = AttemptJournal(tmp_path / 'outbox.sqlite3', utcnow_fn=lambda: START)
    for index in range(202):
        at = START + timedelta(seconds=1 if index < 201 else 100)
        identity = journal.begin(run_id='r', task_id='t', requested_at=at,
                                 origin='https://site.web.api.espn.com', endpoint='summary',
                                 lane='history', step=0)
        journal.finish(identity, status=200, timeout=False, http_ms=1, direct_bytes=1)
    queries = []
    fail = [True]
    def execute(conn, sql):
        if sql.startswith('MERGE'):
            queries.append(sql)
            if fail[0]:
                fail[0] = False
                raise RuntimeError('commit acknowledgement lost')
    monkeypatch.setattr(attempts, '_execute', execute)
    with pytest.raises(RuntimeError):
        journal.flush(object(), max_batches=1)
    assert journal.pending() == 202
    assert journal.flush(object(), max_batches=1) == 200
    assert queries[0] == queries[1]  # same attempt IDs and values on replay
    assert journal.pending() == 2
    result = drain_publication(journal, SimpleNamespace(connection=object()),
                               PaceStore(tmp_path / 'pace.sqlite3'), lambda: START + timedelta(seconds=10),
                               START + timedelta(seconds=10))
    assert result['pending'] == 0 and result['published'] == 1 and result['error'] is None
    assert journal.pending() == 1  # concurrent newer traffic cannot extend final drain


def test_publication_nonblocking_lock_preserves_dirty_outbox(tmp_path):
    import fcntl
    import os
    journal = AttemptJournal(tmp_path / 'outbox.sqlite3', utcnow_fn=lambda: START)
    descriptor = os.open(str(journal.path) + '.publish.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        assert journal.flush(object(), max_batches=1, blocking=False) == 0
    finally:
        os.close(descriptor)


def test_publish_recovery_reconciles_status_without_resuming_http(controller, monkeypatch, capsys):
    from scrapers.espn import attempts, trino_manager
    c = controller
    c.state.update(status='complete', publication_pending=30)
    c.store.save(c.state)
    c.stop_file.touch()
    before = c.gate.snapshot()
    journal = AttemptJournal(c.store.path.parent / 'http-attempts.sqlite3', utcnow_fn=lambda: START)
    journal.begin(run_id='r', task_id='t', requested_at=START,
                  origin='https://site.web.api.espn.com', endpoint='summary', lane='history', step=0)
    monkeypatch.setattr(trino_manager, 'EspnTrinoTableManager', AsyncSQL)
    monkeypatch.setattr(attempts, '_execute', lambda *_: None)
    assert main(['publish', '--state-dir', str(c.store.path.parent)]) == 0
    assert json.loads(capsys.readouterr().out)['pending'] == 0
    report = read_status(c.store.path, now=c.clock())
    assert report['publication_pending'] == 0
    assert report['publication']['pending'] == 0
    assert c.stop_file.exists()
    assert c.store.get()['status'] == 'complete'
    assert c.gate.snapshot() == before


def test_restart_after_crash_during_final_drain_never_restarts_accepted_s2(controller, monkeypatch):
    from scrapers.espn import trino_manager
    c = controller
    c.state.update(status='complete', accepted_step=2, baseline_p95_ms=100,
                   completed=[dict(step=2, reason='accepted_s2')])
    c.store.save(c.state)
    journal = AttemptJournal(c.store.path.parent / 'http-attempts.sqlite3', utcnow_fn=lambda: START)
    journal.begin(run_id='r', task_id='t', requested_at=START,
                  origin='https://site.web.api.espn.com', endpoint='summary', lane='history', step=2)
    def crash(**kwargs):
        raise SystemExit('process interrupted while publisher drains')
    c.io = SimpleNamespace(close=crash)
    c.initialize = lambda: None
    with pytest.raises(SystemExit):
        c.run_locked()
    assert c.store.get()['status'] == 'draining'
    def forbidden():
        pytest.fail('completed recovery must not create a SQL manager or restart HTTP')
    monkeypatch.setattr(trino_manager, 'EspnTrinoTableManager', forbidden)
    assert main(['run', '--state-dir', str(c.store.path.parent)]) == 0
    recovered = c.store.get()
    assert recovered['status'] == 'complete'
    assert recovered['accepted_step'] == 2
    assert recovered['baseline_p95_ms'] == 100
    assert recovered['completed'] == c.state['completed']
    assert recovered['publication']['error'] == 'final_publication_interrupted'
    assert recovered['publication']['total_pending'] == 1
    assert journal.pending() == 1
    with pytest.raises(ValueError, match='already complete'):
        main(['resume', '--state-dir', str(c.store.path.parent)])
    assert c.store.get()['accepted_step'] == 2


def durable_window(c):
    """Use the production begin/finish outbox contract, not fake coverage."""
    fill_window(c)
    journal = AttemptJournal(c.store.path.parent / 'real-attempts.sqlite3', utcnow_fn=lambda: START)
    for row in c.journal.data:
        identity = journal.begin(run_id='r', task_id='measure_pace',
            requested_at=datetime.fromisoformat(row['requested_at']), origin=row['origin'],
            endpoint='summary', lane='history', step=row['step'], measurement_id=row['measurement_id'])
        journal.finish(identity, status=200, timeout=False, http_ms=100, direct_bytes=1)
    journal.flush = lambda *_args, **_kwargs: 0
    c.journal = journal
    return journal.rows(datetime.fromtimestamp(c.state['start'], timezone.utc))[0]['attempt_id']


def test_drained_incomplete_window_restarts_then_healthy_traffic_promotes(controller):
    c = controller
    bad = durable_window(c)
    c.journal.finish(bad, status=None, timeout=False, http_ms=5, direct_bytes=0, complete=False)
    original = dict(c.state)
    # Merely inspecting/evaluating a live snapshot must not restart anything.
    assert c.decide()['reason'] == 'incomplete_coverage'
    assert c.state['start'] == original['start']
    c.tick()
    c.finish_boundary()  # same owner path called after bounded_fetch drains
    rejected = c.store.latest('rejected_window')
    assert rejected['attempt_ids'] == [bad]
    assert rejected['report']['reason'] == 'incomplete_coverage'
    assert c.state['restart_pending'] == c.clock().timestamp()
    with pytest.raises(LaneClosed, match='boundary'):
        c.check()
    c.clock.now += timedelta(seconds=10)
    c.observe(force=True)
    assert c.state['start'] == c.clock().timestamp()
    assert 'restart_pending' not in c.state
    assert c.state['measurement_id'] == original['measurement_id']
    assert c.state['baseline_p95_ms'] == original['baseline_p95_ms']
    assert c.journal.rows()[0]['complete'] is False
    # New complete traffic qualifies independently; old audit/attempt persists.
    real = c.journal
    c.journal = Journal()
    fill_window(c, seconds=600)
    new_rows = c.journal.data
    c.journal = real
    for row in new_rows:
        identity = real.begin(run_id='r', task_id='measure_pace',
            requested_at=datetime.fromisoformat(row['requested_at']), origin=row['origin'],
            endpoint='summary', lane='history', step=0, measurement_id=c.state['measurement_id'])
        real.finish(identity, status=200, timeout=False, http_ms=100, direct_bytes=1)
    c.tick()
    c.finish_boundary()
    assert c.gate.snapshot()['confirmed_ceiling'] == 1
    assert len(c.store.rows('rejected_window', 0, c.clock().timestamp())) == 1
    assert c.journal.rows()[0]['attempt_id'] == bad


@pytest.mark.parametrize('mode', ['live_inflight', '500', 'timeout', 'stop', 'complete'])
def test_boundary_does_not_retire_inflight_errors_or_terminal_state(controller, mode):
    c = controller
    bad = durable_window(c)
    c.journal.finish(bad, status=None, timeout=False, http_ms=5, direct_bytes=0, complete=False)
    original = dict(c.state)
    if mode == 'live_inflight':
        c.journal.begin(run_id='current', task_id='current', requested_at=START + timedelta(seconds=1),
            origin='https://sports.core.api.espn.com', endpoint='scoreboard', lane='live', step=0)
    elif mode in ('500', 'timeout'):
        c.journal.finish(bad, status=500 if mode == '500' else None,
                         timeout=mode == 'timeout', http_ms=5, direct_bytes=0)
    elif mode == 'stop':
        c.stop_file.touch()
    else:
        c.state.update(status='complete', accepted_step=2)
    c.tick()
    c.finish_boundary()
    assert c.state['start'] == original['start']
    assert 'restart_pending' not in c.state
    assert c.store.latest('rejected_window') is None


def test_rejected_window_and_pending_restart_survive_crash_without_rebaseline(controller):
    c = controller
    fill_window(c)
    c.decide()  # accepted S0 baseline must survive rejection of S1
    bad = durable_window(c)
    c.journal.finish(bad, status=None, timeout=False, http_ms=5, direct_bytes=0, complete=False)
    c.tick()
    c.finish_boundary()
    saved = c.store.get()
    assert saved['restart_pending'] == c.clock().timestamp()
    assert saved['step'] == 1
    c.clock.now += timedelta(seconds=5)
    c.observation = None
    c.initialize()
    assert c.state['step'] == 1
    assert c.state['baseline_p95_ms'] == saved['baseline_p95_ms'] == 100
    assert c.state['completed'] == saved['completed']
    assert c.state['measurement_id'] == saved['measurement_id']
    assert 'restart_pending' not in c.state
    c.tick()
    c.finish_boundary()
    assert len(c.store.rows('rejected_window', 0, c.clock().timestamp())) == 1


def test_async_restart_waits_for_post_drain_query_and_cannot_repeat(controller):
    c = controller
    bad = durable_window(c)
    c.journal.finish(bad, status=None, timeout=False, http_ms=5, direct_bytes=0, complete=False)
    io = async_io(c, publish=lambda *_args, **_kwargs: 0)
    try:
        eventually(lambda: io.snapshot('debt') is not None)
        # Seed the already-continuous observer lineage; real IO owns subsequent polls.
        with io.lock:
            io.values['debt']['restart_at'] = c.state['start']
        c.observe()
        old = c.state['start']
        c.tick()
        c.finish_boundary()
        pending = c.state['restart_pending']
        # Polling an earlier query cannot open the new window.
        previous = dict(c.observation)
        c._apply_observation(dict(previous, at=pending + 1, observed_at=pending - 1), c.gate.snapshot())
        assert c.state['restart_pending'] == pending
        assert c.state['start'] == old
        c.clock.now += timedelta(seconds=10)
        eventually(lambda: io.snapshot('debt')['observed_at'] >= c.clock().timestamp())
        c.observe()
        assert c.state['start'] == c.clock().timestamp()
        assert 'restart_pending' not in c.state
        c.finish_boundary()
        assert len(c.store.rows('rejected_window', 0, c.clock().timestamp())) == 1
    finally:
        io.close()


def test_real_fetch_boundary_drains_before_retiring_incomplete_attempt(controller):
    from scrapers.espn.parallel import bounded_fetch
    c = controller
    durable_window(c)
    begun, release, finished = threading.Event(), threading.Event(), threading.Event()
    c.last_decision = c.clock().timestamp()
    identity = []
    def fetch(client, item):
        identity.append(c.journal.begin(run_id='r', task_id='measure_pace',
            requested_at=c.clock(), origin='https://site.web.api.espn.com',
            endpoint='summary', lane='history', step=0, measurement_id=c.state['measurement_id']))
        begun.set()
        assert release.wait(3)
        c.journal.finish(identity[0], status=None, timeout=False, http_ms=5,
                         direct_bytes=0, complete=False)
        finished.set()
    def tick():
        if begun.is_set() and not release.is_set():
            c.clock.now += timedelta(seconds=10)
            c.observe(force=True)
            assert c.decide()['reason'] == 'incomplete_coverage'
            assert 'restart_pending' not in c.state
            assert c.store.latest('rejected_window') is None
            c.last_decision = 0
            release.set()
        c.tick()
    try:
        with pytest.raises(LaneClosed, match='drain'):
            list(bounded_fetch([1, 2], fetch, limit=lambda: 1, check=c.check,
                 client_factory=lambda _: SimpleNamespace(flush=lambda: None), tick=tick))
        assert finished.is_set()
        c.finish_boundary()
        assert c.store.latest('rejected_window')['attempt_ids'] == identity
        assert c.state['restart_pending'] == c.clock().timestamp()
    finally:
        release.set()


def test_rejected_window_audit_and_state_are_atomic(controller):
    import sqlite3
    c = controller
    bad = durable_window(c)
    c.journal.finish(bad, status=None, timeout=False, http_ms=5, direct_bytes=0, complete=False)
    with c.store.db() as db:
        db.execute("""CREATE TRIGGER reject_restart BEFORE INSERT ON state
            WHEN NEW.value LIKE '%restart_pending%'
            BEGIN SELECT RAISE(ABORT, 'disk rejected restart'); END""")
    c.tick()
    with pytest.raises(sqlite3.IntegrityError, match='disk rejected restart'):
        c.finish_boundary()
    assert c.store.latest('rejected_window') is None
    assert 'restart_pending' not in c.store.get()
    assert 'restart_pending' not in c.state
