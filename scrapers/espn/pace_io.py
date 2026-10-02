"""Bounded, independently owned SQL lanes for the pace measurement.

No lane mutates Controller state. Snapshots are copied under a short lock; SQL
and durable evidence writes never hold that lock. Non-daemon threads are joined
before the runtime lock is released, including during delivery/SIGTERM.
"""
from copy import deepcopy
import threading
import time

from .history import live_debt
from .parallel import protected


class PaceIO:
    def __init__(self, *, factory, gate, journal, store, targets, now,
                 read_freshness, observe_seconds=10, freshness_seconds=60,
                 publish_seconds=60, maximum_gap=30):
        self.factory, self.gate, self.journal, self.store = factory, gate, journal, store
        self.targets, self.now, self.read_freshness = targets, now, read_freshness
        self.intervals = dict(debt=observe_seconds, freshness=freshness_seconds, publication=publish_seconds)
        self.maximum_gap = maximum_gap
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.values = {}
        self.threads = []
        self.publish_before = None
        self.wakes = {name: threading.Event() for name in self.intervals}

    def snapshot(self, name):
        with self.lock:
            return deepcopy(self.values.get(name))

    def start(self):
        for name in self.intervals:
            thread = threading.Thread(target=self._run, args=(name,), name='espn-pace-' + name)
            try:
                thread.start()
            except BaseException:
                self.close()
                raise
            self.threads.append(thread)

    def request(self, name):
        self.wakes[name].set()  # Coalesced signal, never an unbounded work queue.

    def close(self, *, publish_before=None):
        self.publish_before = publish_before
        self.stop.set()
        for wake in self.wakes.values():
            wake.set()
        for thread in self.threads:
            thread.join()

    def _run(self, name):
        manager = None
        try:
            while not self.stop.is_set():
                self.wakes[name].clear()
                previous = self.snapshot(name)
                sequence = previous.get('sequence', 0) + 1 if previous else 1
                started = self.now()
                timer = time.monotonic()
                error, result = None, {}
                try:
                    if manager is None:
                        manager = self.factory()
                    if name == 'debt':
                        before = self.gate.snapshot()
                        debt = live_debt(manager, self.targets, started)
                        after = self.gate.snapshot()
                        result = dict(debt=debt, protected=protected(after, self.now().timestamp()),
                                      step=after['step'], revision=after['revision'],
                                      epoch=after['measurement_started_at'])
                        if before['revision'] != after['revision']:
                            error = 'gate_changed_during_debt'
                    elif name == 'freshness':
                        result = self.read_freshness(manager, self.targets, started)
                    else:
                        result = dict(published=self.journal.flush(manager.connection, max_batches=1,
                                                                  blocking=False),
                                      total_pending=self.journal.pending())
                except Exception as exc:
                    error = type(exc).__name__
                at = self.now().timestamp()
                result.update(at=at, sequence=sequence, observed_at=started.timestamp(), seconds=time.monotonic() - timer,
                              error=error)
                if name == 'debt':
                    if at - started.timestamp() > self.maximum_gap:
                        result['error'] = 'debt_query_stale'
                    result['known'] = result['error'] is None
                    previous = self.snapshot(name)
                    restart = (previous is None or not result['known'] or not previous['known']
                               or at - previous.get('observed_at', previous['at']) > self.maximum_gap
                               or result.get('revision') != previous.get('revision'))
                    result['restart_at'] = at if restart else previous['restart_at']
                else:
                    if error and name == 'freshness':
                        result['eligible'] = False
                try:
                    self.store.record('observation' if name == 'debt' else name, **result)
                except Exception as exc:
                    result['error'] = 'evidence_write_' + type(exc).__name__
                    if name == 'debt':
                        result.update(known=False, restart_at=at)
                    elif name == 'freshness':
                        result['eligible'] = False
                with self.lock:
                    self.values[name] = result
                # Observation cadence is measured from query start. Failed lanes
                # retain a minimum retry delay even if their operation was slow.
                delay = max(1 if result['error'] else 0, self.intervals[name] - result['seconds'])
                if result['error']:
                    self.stop.wait(delay)
                else:
                    self.wakes[name].wait(delay)
        finally:
            if name == 'publication' and self.publish_before is not None:
                try:
                    if manager is None:
                        manager = self.factory()
                    result = drain_publication(self.journal, manager, self.store,
                                               self.now, self.publish_before)
                    with self.lock:
                        self.values[name] = result
                except Exception as exc:
                    with self.lock:
                        self.values[name] = dict(at=self.now().timestamp(), error=type(exc).__name__, final=True)
            if manager is not None:
                manager.close()


def drain_publication(journal, manager, store, now, before):
    """Drain a finite cutoff of the outbox; concurrent new traffic cannot extend it.

    A failed/busy publisher leaves exact versions dirty and reports the remainder.
    The publish CLI retries this same durable protocol after a stopped/completed run.
    """
    timer = time.monotonic()
    published, error = 0, None
    pending = journal.pending(before=before)
    try:
        published = journal.flush(manager.connection, max_batches=(pending + 199) // 200,
                                  blocking=False, before=before) if pending else 0
    except Exception as exc:
        error = type(exc).__name__
    pending = journal.pending(before=before)
    result = dict(at=now().timestamp(), cutoff=before.isoformat(), seconds=time.monotonic() - timer,
                  published=published, pending=pending, total_pending=journal.pending(),
                  error=error or ('publication_pending' if pending else None),
                  final=True)
    store.record('publication', **result)
    return result
