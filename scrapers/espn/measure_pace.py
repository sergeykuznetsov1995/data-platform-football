"""Single persistent ESPN pace controller. Run via python -m scrapers.espn.measure_pace.

Workers perform measurement HTTP only. The owning thread samples live debt,
queries freshness and publishes evidence; no measurement writes target Bronze.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import signal
import threading
import time
import uuid

from . import urls
from .criterion import meets, render_daily_criterion_sql
from .gate import TransportGate, default_state_path
from .history import live_debt
from .pace import PaceWindow, evaluate
from .pace_report import attempt_metrics, format_measurement, load_evidence, write_metrics
from .pace_store import ControllerBusy, PaceStore
from .parallel import WORKERS, bounded_fetch, interruptible_sleep, protected
from .transport_contracts import AllOriginsBlocked, LaneClosed
from .wave import _STATUS_ERRORS

OBSERVE_SECONDS = 10
MAXIMUM_GAP = 30
FRESHNESS_SECONDS = 60
SCHEMA_VERSION = 1


def now_utc():
    return datetime.now(timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def accepted_ids(trino):
    rows = trino.execute_query("""SELECT DISTINCT event_id FROM iceberg.bronze.espn_match
WHERE competition_slug='eng.1' AND season_year=2015 AND duplicate_of IS NULL
AND played_final AND lineup_state IN ('captured', 'valid_empty')
AND team_stats_state IN ('captured', 'valid_empty') ORDER BY event_id""")
    ids = [int(r[0]) for r in rows]
    if len(ids) != 380 or len(set(ids)) != 380:
        raise ValueError('measurement requires the accepted 380 eng.1 2015 IDs')
    return ids


def read_freshness(trino, targets, now):
    day = (now.date() - timedelta(days=1)).isoformat()
    rows = trino.execute_query(render_daily_criterion_sql(day, targets))
    due, ok = sum(int(r[1]) for r in rows), sum(int(r[2]) for r in rows)
    return dict(day=day, due=due, ok=ok, eligible=meets(ok, due), checked_at=now.timestamp())


def validate_state(state, policy_hash, workload_hash):
    if (not isinstance(state, dict) or state.get('version') != SCHEMA_VERSION
            or state.get('policy_hash') != policy_hash or state.get('workload_hash') != workload_hash):
        raise ValueError('incompatible controller state/policy/workload; preserve evidence and use a new run')
    for key in ('step', 'accepted_step'):
        if type(state.get(key)) is not int or state[key] not in range(4):
            raise ValueError('invalid persisted controller step')
    for key in ('start', 'heartbeat', 'epoch'):
        if type(state.get(key)) not in (int, float) or not math.isfinite(state[key]) or state[key] < 0:
            raise ValueError('invalid persisted controller timestamp')
    if type(state.get('cursor')) is not int or state['cursor'] < 0:
        raise ValueError('invalid persisted workload cursor')
    baseline = state.get('baseline_p95_ms')
    if baseline is not None and (type(baseline) not in (int, float) or not math.isfinite(baseline) or baseline <= 0):
        raise ValueError('invalid persisted baseline')


class MeasurementStopped(RuntimeError):
    pass


class Controller:
    def __init__(self, *, gate, journal, store, ids, trino, targets, client_factory,
                 stop_file, history_stop=None, now=now_utc, monotonic=time.monotonic,
                 max_step=2, benchmark_fn=None):
        if max_step not in (2, 3):
            raise ValueError('acceptance maximum must be S2; S3 only with explicit opt-in')
        self.gate, self.journal, self.store = gate, journal, store
        self.ids, self.trino, self.targets = tuple(ids), trino, tuple(targets)
        self.client_factory = client_factory
        self.stop_file = Path(stop_file)
        self.history_stop = Path(history_stop) if history_stop is not None else None
        self.now, self.monotonic, self.max_step = now, monotonic, max_step
        self.cancel = threading.Event()
        self.clients = []
        self.client_slots = {}
        self.state = None
        self.freshness = {}
        self.observation = None
        self.last_publish = 0
        self.last_decision = 0
        self.decision_due = False
        self.owner = threading.get_ident()
        self.benchmark_fn = benchmark_fn

    def _main_thread(self):
        if threading.get_ident() != self.owner:
            raise RuntimeError('controller Trino access must remain in the owning thread')

    def initialize(self):
        self._main_thread()
        if not self.ids or tuple(sorted(set(self.ids))) != self.ids:
            raise ValueError('measurement IDs must be a nonempty sorted unique set')
        now = self.now().timestamp()
        snapshot = self.gate.snapshot()
        policy_hash = digest(asdict(self.gate.policy))
        workload_hash = digest(dict(slug='eng.1', year=2015, ids=self.ids, endpoints=['summary']))
        state = self.store.get()
        if state is None:
            if snapshot['confirmed_ceiling'] != 0:
                raise ValueError('a new measurement must start at confirmed S0')
            state = dict(version=SCHEMA_VERSION, policy_hash=policy_hash,
                         workload_hash=workload_hash, measurement_id=uuid.uuid4().hex,
                         ids=list(self.ids), cursor=0, accepted_step=0,
                         baseline_p95_ms=None, completed=[], status='starting',
                         step=snapshot['step'], revision=snapshot['revision'],
                         epoch=snapshot['measurement_started_at'], start=now, heartbeat=now)
        else:
            validate_state(state, policy_hash, workload_hash)
            if state.get('status') == 'complete':
                raise ValueError('measurement already complete; use status, not resume')
            # A process interruption is an explicit observation gap, even if brief.
            state['start'] = now
            state['revision'] = snapshot['revision']
            state['epoch'] = snapshot['measurement_started_at']
            state['step'] = snapshot['step']
        state.update(status='running', heartbeat=now)
        self.state = state
        self.store.record('lifecycle', now, event='start', measurement_id=state['measurement_id'])
        self.store.save(state)
        self.observe(force=True)

    def check(self):
        if self.cancel.is_set() or self.stop_file.exists():
            raise MeasurementStopped('measurement stop requested')
        if self.decision_due:
            raise LaneClosed('controller evidence boundary; drain workers')
        if self.history_stop is not None and self.history_stop.exists():
            raise LaneClosed('history stop file present')
        now = self.now().timestamp()
        observation = self.observation
        if not observation or not observation['known'] or now - observation['at'] > MAXIMUM_GAP:
            raise LaneClosed('live-debt observation unknown or stale')
        if observation['debt']:
            raise LaneClosed('live debt pauses measurement')
        if protected(self.gate.snapshot(), now):
            raise LaneClosed('gate protection pauses measurement')

    def observe(self, *, force=False):
        self._main_thread()
        now = self.now()
        if not force and self.observation and now.timestamp() - self.observation['at'] < OBSERVE_SECONDS:
            return
        snapshot = self.gate.snapshot()
        known, debt, error = True, 0, None
        try:
            debt = live_debt(self.trino, self.targets, now)
            if force or now.timestamp() - self.freshness.get('checked_at', 0) >= FRESHNESS_SECONDS:
                self.freshness = read_freshness(self.trino, self.targets, now)
        except Exception as exc:
            known, error = False, type(exc).__name__
            self.freshness = {}
        at = self.now().timestamp()
        observation = dict(at=at, known=known, debt=debt, protected=protected(snapshot, at),
                           step=snapshot['step'], revision=snapshot['revision'], error=error)
        previous = self.observation
        self.observation = observation
        self.store.record('observation', **observation)
        state = self.state
        if (self.max_step == 3 and state['step'] == 3 and snapshot['confirmed_ceiling'] == 2
                and any(e['reason'] == 's3_expired' and e['at'] >= state['epoch']
                        for e in snapshot['events'])):
            state.update(status='complete', accepted_step=2)
            state.setdefault('report', {})['reason'] = 's3_expired_return_s2'
            self.cancel.set()
        gap = previous is not None and at - previous['at'] > MAXIMUM_GAP
        if (snapshot['revision'] != state['revision'] or snapshot['measurement_started_at'] != state['epoch']
                or not known or gap):
            state.update(start=at, revision=snapshot['revision'],
                         epoch=snapshot['measurement_started_at'], step=snapshot['step'])
            self.store.record('lifecycle', at, event='window_restarted', reason='epoch_or_observation_gap')
        # The first observation defines a covered boundary after its query completed.
        if previous is None:
            state['start'] = at
        state['heartbeat'] = at
        self.store.save(state)

    def evidence(self):
        self._main_thread()
        snapshot = self.gate.snapshot()  # revision captured before evidence collection
        end = self.observation['at']
        start = self.state['start']
        begin, finish = (datetime.fromtimestamp(t, timezone.utc) for t in (start, end))
        rows = self.journal.rows(begin, finish)
        metrics = attempt_metrics(rows)
        profile = [r for r in rows if r.get('measurement_id') == self.state['measurement_id']]
        profile_metrics = attempt_metrics(profile)
        observations = self.store.rows('observation', start, end)
        load = load_evidence(rows, observations, begin, finish, step=self.state['step'],
                             policy=self.gate.policy, maximum_gap=MAXIMUM_GAP)
        resets = sum(e['kind'] == 'reset' and start <= e['at'] <= end for e in snapshot['events'])
        baseline = self.state['baseline_p95_ms']
        if self.state['step'] == 0:
            baseline = profile_metrics['http_p95_ms']
        fresh = dict(self.freshness)
        current = self.now().timestamp()
        fresh_ok = (fresh.get('eligible') is True and
                    current - fresh.get('checked_at', 0) <= FRESHNESS_SECONDS + MAXIMUM_GAP)
        complete = (start < end and self.journal.coverage(begin, finish) and load['continuous']
                    and bool(profile) and snapshot['revision'] == self.state['revision']
                    and snapshot['measurement_started_at'] == self.state['epoch'])
        # All traffic must meet the absolute latency bound; relative comparison
        # uses the fixed measurement profile across S0/S1/S2.
        complete = complete and metrics['http_p95_ms'] is not None
        p95 = profile_metrics['http_p95_ms']
        if metrics['http_p95_ms'] is not None and metrics['http_p95_ms'] >= self.gate.policy.pace.http_p95_limit_ms:
            p95 = metrics['http_p95_ms']
        window = PaceWindow(self.state['step'], begin, finish, metrics['attempts'],
                            metrics['count_429'], metrics['error_attempts'], p95, resets,
                            bool(fresh_ok), bool(complete), load['eligible_intervals'],
                            load['loaded_intervals'], current < snapshot['hold_until'],
                            current < max(snapshot['cooldown_until'], snapshot['history_frozen_until']))
        decision = evaluate(window, baseline, self.gate.policy)
        writes = self.store.rows('write', start, end)
        report = dict(step=window.step, start=begin.isoformat(), end=finish.isoformat(),
                      metrics=metrics, profile_p95_ms=profile_metrics['http_p95_ms'],
                      baseline_p95_ms=baseline, resets=resets,
                      load={k: v for k, v in load.items() if k != 'bins'},
                      freshness=fresh, write=write_metrics(writes), reason=decision.reason,
                      history_quota_per_minute=self.gate.policy.steps[window.step] * (1 - self.gate.policy.live_share),
                      measurement_id=self.state['measurement_id'], heartbeat=self.state['heartbeat'])
        return snapshot, window, baseline, decision, report

    def _benchmark(self, report):
        if self.benchmark_fn is not None:
            if self.cancel.is_set() or self.stop_file.exists():
                raise MeasurementStopped('stop before isolated benchmark')
            try:
                result = self.benchmark_fn(report['step'])
            except Exception as exc:
                report['reason'] = 'isolated_benchmark_failed'
                self.state['report'] = report
                self.store.record('lifecycle', self.now().timestamp(), event='benchmark_failed',
                                  step=report['step'], error=type(exc).__name__)
                self.store.save(self.state)
                raise
            if self.cancel.is_set() or self.stop_file.exists():
                raise MeasurementStopped('stop after isolated benchmark')
            report['isolated_benchmark'] = result
            self.store.record('lifecycle', self.now().timestamp(), event='benchmark_complete',
                              step=report['step'], result=result)

    def decide(self):
        snapshot, window, baseline, decision, report = self.evidence()
        state = self.state
        if decision.eligible:
            final = window.step >= self.max_step
            if final:
                # Require a fully closed UTC freshness day overlapping S2 load.
                day = datetime.fromisoformat(self.freshness['day']).replace(tzinfo=timezone.utc)
                if not (day.timestamp() < window.ended_at.timestamp() and
                        (day + timedelta(days=1)).timestamp() > window.started_at.timestamp()):
                    report['reason'] = 'awaiting_overlapping_closed_utc_day'
                else:
                    self._benchmark(report)
                    final_snapshot = self.gate.snapshot()
                    if (final_snapshot['revision'] != snapshot['revision']
                            or protected(final_snapshot, self.now().timestamp())):
                        report['reason'] = 'gate_changed_during_benchmark'
                        state['report'] = report
                        self.store.save(state)
                        return report
                    self.freshness = read_freshness(self.trino, self.targets, self.now())
                    report['freshness'] = dict(self.freshness)
                    if not self.freshness['eligible']:
                        report['reason'] = 'freshness_changed_during_benchmark'
                        state['report'] = report
                        self.store.save(state)
                        return report
                    state['accepted_step'] = window.step
                    state['status'] = 'complete'
                    report['reason'] = 'accepted_s2' if window.step == 2 else 'accepted_s3'
                    state['completed'].append(report)
                    self.cancel.set()
            else:
                confirmed = self.gate.confirm(window, baseline, expected_revision=snapshot['revision'])
                report['reason'] = confirmed.reason
                if confirmed.eligible:
                    state['accepted_step'] = window.step
                    if window.step == 0:
                        state['baseline_p95_ms'] = baseline
                    state['completed'].append(report)
                    next_snapshot = self.gate.snapshot()
                    state.update(start=self.now().timestamp(), step=next_snapshot['step'],
                                 revision=next_snapshot['revision'], epoch=next_snapshot['measurement_started_at'])
                    # Persist promotion before optional isolated writes. No HTTP
                    # runs during this boundary; fresh observations start afterwards.
                    self.store.save(state)
                    self._benchmark(report)
                    # Bracket the next window with a fresh observation.
                    self.observe(force=True)
                    state['start'] = self.observation['at']
        state['report'] = report
        self.store.save(state)
        return report

    def tick(self):
        self.observe()
        if self.state['status'] != 'complete' and self.now().timestamp() - self.last_decision >= 60:
            # Stop submission and retry admission; bounded_fetch drains existing
            # HTTP before the owner evaluates, publishes or benchmarks.
            self.decision_due = True

    def finish_boundary(self):
        self.observe(force=True)
        self.decision_due = False
        if self.state['status'] != 'complete':
            self.decide()
        self.last_decision = self.now().timestamp()
        self.journal.flush(self.trino.connection)
        self.last_publish = self.now().timestamp()

    def _items(self):
        while not self.cancel.is_set():
            cursor = self.state['cursor']
            self.state['cursor'] += 1
            yield self.ids[cursor % len(self.ids)]

    def _worker(self, slot):
        if slot in self.client_slots:
            return self.client_slots[slot]
        client = self.client_factory(self.state['measurement_id'], self.check)
        self.clients.append(client)
        self.client_slots[slot] = client
        return client

    def run_locked(self):
        try:
            self.initialize()
        except BaseException:
            self.gate.lower_ceiling(0, reason='controller_initialization_failed')
            raise
        error = None
        try:
            while self.state['status'] != 'complete' and not self.cancel.is_set():
                try:
                    for _, _, failure in bounded_fetch(
                        self._items(), lambda client, event_id: client.fetch_json(
                            urls.summary('eng.1', event_id).url, 'summary',
                            urls.summary('eng.1', event_id).params, force_refresh=True),
                        limit=lambda: WORKERS[self.gate.snapshot()['step']], check=self.check,
                        client_factory=self._worker, tick=self.tick,
                    ):
                        if isinstance(failure, MeasurementStopped):
                            raise failure
                        # Actual transport failures remain recorded in the attempt spool.
                        if failure is not None and not isinstance(failure, (LaneClosed, AllOriginsBlocked)):
                            if not isinstance(failure, _STATUS_ERRORS):
                                raise failure
                            self.store.record('request_error', self.now().timestamp(), error=type(failure).__name__)
                except (LaneClosed, AllOriginsBlocked):
                    if self.stop_file.exists() or self.cancel.is_set():
                        break
                    # Existing futures have drained: no HTTP overlaps an
                    # evidence query, publisher or isolated write benchmark.
                    if self.decision_due:
                        self.finish_boundary()
                    else:
                        self.observe()
                    time.sleep(0.25)
        except MeasurementStopped:
            pass
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.cancel.set()
            try:
                if self.state['status'] != 'complete':
                    self.state['status'] = 'failed' if error else 'stopped'
                    self.gate.lower_ceiling(self.state['accepted_step'])
                self.state['heartbeat'] = self.now().timestamp()
                self.store.record('lifecycle', self.state['heartbeat'], event=self.state['status'])
                self.store.save(self.state)
            finally:
                for client in self.clients:
                    client.close()
                    client.session.close()
                self.journal.flush(self.trino.connection)
        return self.state


def state_path(gate):
    return gate.state_path.with_name('pace.sqlite3')


def read_status(path, *, now=None):
    if not Path(path).exists():
        return {'stale': True, 'reason': 'no_controller_state'}
    state = PaceStore(path).get()
    if not state:
        return {'stale': True, 'reason': 'no_controller_state'}
    report = dict(state.get('report') or {})
    now = now or now_utc()
    report.update(status=state.get('status'), accepted_step=state.get('accepted_step'),
                  completed=state.get('completed', []), heartbeat=state.get('heartbeat'))
    report['stale'] = state.get('status') != 'complete' and (
        state.get('status') != 'running' or now.timestamp() - state.get('heartbeat', 0) > MAXIMUM_GAP)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('run', 'start', 'resume', 'status', 'stop', 'benchmark'))
    parser.add_argument('--state-dir', type=Path, default=default_state_path().parent)
    parser.add_argument('--format', choices=('json', 'text'), default='text')
    parser.add_argument('--expected-ids-sha256')
    parser.add_argument('--allow-s3', action='store_true', help='not used in #1510 acceptance')
    parser.add_argument('--benchmark-count', type=int, default=20)
    parser.add_argument('--benchmark-at-boundaries', action='store_true',
                        help='run the isolated write benchmark after each qualified step')
    args = parser.parse_args(argv)
    args.state_dir = args.state_dir.resolve()
    store_path = args.state_dir / 'pace.sqlite3'
    stop_file = args.state_dir / 'measurement.off'
    if args.command == 'status':
        result = read_status(store_path)
        print(json.dumps(result, sort_keys=True) if args.format == 'json' else '\n'.join(format_measurement(result)))
        return 0
    if args.command == 'stop':
        args.state_dir.mkdir(parents=True, exist_ok=True)
        stop_file.touch()
        print('stop requested; check status for drained workers and safe ceiling')
        return 0
    store = PaceStore(store_path)
    try:
        with store.lock():
            return _run_owned(args, store, stop_file, parser)
    except ControllerBusy as exc:
        parser.exit(2, str(exc) + '\n')
    return 0


def _run_owned(args, store, stop_file, parser):
    from .denominator import load_denominator
    from .transport import EspnHttpClient
    from .trino_manager import EspnTrinoTableManager
    from .attempts import AttemptJournal
    gate = None
    try:
        previous = store.get()
        if args.command == 'run' and (stop_file.exists() or (previous and previous.get('status') == 'complete')):
            print('controller complete or explicitly stopped; no measurement started')
            return 0
        gate = TransportGate(lane='history', state_path=args.state_dir / 'gate.json')
        trino = EspnTrinoTableManager()
        ids = accepted_ids(trino)
        if args.expected_ids_sha256 and digest(ids) != args.expected_ids_sha256:
            raise ValueError('accepted ID fingerprint differs from pinned baseline')
        if args.command == 'benchmark':
            from .pace_benchmark import benchmark
            result = benchmark(trino, store, args.state_dir, ids, count=args.benchmark_count)
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.command == 'start' and store.get() is not None:
            raise ValueError('state exists; use resume')
        if args.command == 'resume' and store.get() is None:
            raise ValueError('no state to resume')
        # Explicit start/resume is the authorized removal of this runner's stop flag.
        if args.command in ('start', 'resume'):
            stop_file.unlink(missing_ok=True)
        journal = AttemptJournal(args.state_dir / 'http-attempts.sqlite3')
        def factory(measurement_id, check):
            sleep = lambda seconds: interruptible_sleep(seconds, check)
            return EspnHttpClient(
                gate=TransportGate(lane='history', state_path=gate.state_path, sleep_fn=sleep),
                raw_store_uri=(args.state_dir / 'measurement-raw' / measurement_id).as_uri(),
                run_id=measurement_id, task_id='measure_pace', measurement_id=measurement_id,
                before_attempt=check, sleep_fn=sleep,
            )
        def boundary_benchmark(step):
            from .pace_benchmark import benchmark
            return benchmark(trino, store, args.state_dir, ids,
                             count=args.benchmark_count, step=step)
        controller = Controller(gate=gate, journal=journal, store=store, ids=ids,
                                trino=trino, targets=sorted(load_denominator().live_targets()),
                                client_factory=factory, stop_file=stop_file,
                                history_stop=args.state_dir / 'history.off', max_step=3 if args.allow_s3 else 2,
                                benchmark_fn=boundary_benchmark if args.benchmark_at_boundaries else None)
        old = {sig: signal.signal(sig, lambda *_: controller.cancel.set())
               for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            controller.run_locked()
        finally:
            for sig, handler in old.items():
                signal.signal(sig, handler)

    except BaseException:
        if gate is not None:
            try:
                state = store.get()
            except Exception:
                state = None
            safe = state.get('accepted_step', 0) if isinstance(state, dict) else 0
            if type(safe) is not int or safe not in range(3):
                safe = 0
            gate.lower_ceiling(safe, reason='controller_exit')
        raise
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
