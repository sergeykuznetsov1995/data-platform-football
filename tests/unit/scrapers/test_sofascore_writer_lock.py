"""D5: cross-process bronze writer lock (advisory lock in the metadata DB)."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from scrapers.sofascore import writer_lock


DSN = "postgresql+psycopg2://airflow:secret@airflow-metadb:5432/airflow"
NORMALIZED_DSN = "postgresql://airflow:secret@airflow-metadb:5432/airflow"


class _FakeCursor:
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self._connection.executed.append((sql, params))

    def fetchone(self):
        return (self._connection.answers.pop(0),)


class _FakeConnection:
    def __init__(self, answers):
        self.autocommit = False
        self.closed = False
        self.answers = list(answers)
        self.executed = []

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        self.closed = True


class _FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _fake_psycopg2(monkeypatch, *, answers):
    connections = []

    def connect(dsn):
        connection = _FakeConnection(answers)
        connections.append((dsn, connection))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=connect))
    return connections


def _refusing_psycopg2(monkeypatch):
    def connect(_dsn):
        raise AssertionError("the lock must not open a connection here")

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=connect))


@pytest.mark.unit
def test_lock_is_taken_and_released_by_closing_the_connection(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[True])
    clock = _FakeClock()
    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}

    with writer_lock.bronze_writer_lock(
        environ, sleep=clock.sleep, clock=clock
    ) as acquired:
        (dsn, connection), = connections
        assert acquired is True
        assert connection.autocommit is True
        assert connection.closed is False

    # SQLAlchemy dialect prefix is stripped: psycopg2 does not understand it.
    assert dsn == NORMALIZED_DSN
    assert connection.executed == [
        ("SELECT pg_try_advisory_lock(%s)", (writer_lock.WRITER_LOCK_KEY,))
    ]
    # Closing the session releases the advisory lock; no explicit unlock.
    assert connection.closed is True
    assert clock.sleeps == []


@pytest.mark.unit
def test_season_capture_lock_has_stable_scope_specific_key_and_releases_on_failure(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[True])
    env = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}
    for season, freshness in [(92699, "final"), (92699, "final"), (92700, "final"), (92699, "next-day")]:
        with pytest.raises(RuntimeError, match="injected crash"):
            with writer_lock.season_capture_lock(26947, season, freshness, env):
                raise RuntimeError("injected crash")
    keys = [connection.executed[0][1][0] for _, connection in connections]
    assert keys[0] == keys[1]
    assert len(set(keys)) == 3
    assert writer_lock.WRITER_LOCK_KEY not in keys
    assert all(connection.closed for _, connection in connections)


@pytest.mark.unit
def test_busy_lock_waits_and_then_acquires(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[False, False, True])
    clock = _FakeClock()
    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}

    with writer_lock.bronze_writer_lock(
        environ, sleep=clock.sleep, clock=clock
    ) as acquired:
        assert acquired is True

    (_, connection), = connections
    assert len(connection.executed) == 3
    assert clock.sleeps == [writer_lock.WRITER_LOCK_POLL_SECONDS] * 2
    assert connection.closed is True


@pytest.mark.unit
def test_busy_lock_times_out_and_closes_the_connection(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[False] * 10)
    clock = _FakeClock()
    environ = {
        writer_lock.WRITER_LOCK_DSN_ENV: DSN,
        writer_lock.WRITER_LOCK_TIMEOUT_ENV: "12",
    }

    with pytest.raises(writer_lock.WriterLockTimeout) as excinfo:
        with writer_lock.bronze_writer_lock(
            environ, sleep=clock.sleep, clock=clock
        ):
            raise AssertionError("the body must not run without the lock")

    assert "12" in str(excinfo.value)
    (_, connection), = connections
    # Polled at t=0, 5, 10, 15: the deadline is only checked after a refusal.
    assert len(connection.executed) == 4
    assert clock.sleeps == [writer_lock.WRITER_LOCK_POLL_SECONDS] * 3
    assert connection.closed is True


@pytest.mark.unit
@pytest.mark.parametrize("value", ["0", "false", "No"])
def test_lock_is_disabled_only_by_an_explicit_switch(monkeypatch, value):
    _refusing_psycopg2(monkeypatch)
    environ = {writer_lock.WRITER_LOCK_ENV: value}

    with writer_lock.bronze_writer_lock(environ) as acquired:
        assert acquired is False


@pytest.mark.unit
@pytest.mark.parametrize("value", ["1", "", "off"])
def test_unrecognised_switch_values_keep_the_lock_enabled(monkeypatch, value):
    _fake_psycopg2(monkeypatch, answers=[True])
    environ = {
        writer_lock.WRITER_LOCK_ENV: value,
        writer_lock.WRITER_LOCK_DSN_ENV: DSN,
    }

    with writer_lock.bronze_writer_lock(environ) as acquired:
        assert acquired is True


@pytest.mark.unit
def test_missing_dsn_fails_closed_when_the_lock_is_enabled(monkeypatch):
    _refusing_psycopg2(monkeypatch)

    with pytest.raises(RuntimeError, match=writer_lock.WRITER_LOCK_DSN_ENV):
        with writer_lock.bronze_writer_lock({}):
            raise AssertionError("the body must not run without the lock")


class _FakeTimer:
    def __init__(self, interval, function, args=()):
        self.interval = interval
        self.function = function
        self.args = args
        self.daemon = False
        self.started = False
        self.cancelled = False

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True

    def fire(self):
        self.function(*self.args)


def _timer_factory(timers):
    def factory(interval, function, args=()):
        timer = _FakeTimer(interval, function, args)
        timers.append(timer)
        return timer
    return factory


@pytest.mark.unit
def test_global_key_hold_is_bounded_and_aborts_a_stuck_holder(monkeypatch):
    # #2062: one stuck holder kept the global key for 3h15m and stalled every
    # SofaScore writer. Past the hold limit the holder must be dumped and
    # killed (which closes its PG session) instead of holding on silently.
    _fake_psycopg2(monkeypatch, answers=[True])
    clock = _FakeClock()
    timers, aborts = [], []
    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}

    with writer_lock.bronze_writer_lock(
        environ, sleep=clock.sleep, clock=clock,
        _abort=aborts.append, _timer=_timer_factory(timers),
    ):
        (timer,) = timers
        assert timer.started and timer.daemon
        assert timer.interval == writer_lock.DEFAULT_WRITER_LOCK_MAX_HOLD_SECONDS
        timer.fire()

    (message,) = aborts
    assert str(writer_lock.WRITER_LOCK_KEY) in message
    assert "hold limit exceeded" in message
    assert timer.cancelled is True


@pytest.mark.unit
def test_hold_limit_is_configurable_and_cancelled_on_normal_release(monkeypatch):
    _fake_psycopg2(monkeypatch, answers=[True])
    timers = []
    environ = {
        writer_lock.WRITER_LOCK_DSN_ENV: DSN,
        writer_lock.WRITER_LOCK_MAX_HOLD_ENV: "600",
    }

    with writer_lock.bronze_writer_lock(environ, _timer=_timer_factory(timers)):
        pass

    (timer,) = timers
    assert timer.interval == 600.0
    assert timer.cancelled is True


@pytest.mark.unit
def test_season_capture_lock_is_not_hold_bounded(monkeypatch):
    # A season key legitimately spans the whole network capture.
    _fake_psycopg2(monkeypatch, answers=[True])
    timers = []
    monkeypatch.setattr(writer_lock.threading, "Timer", _timer_factory(timers))

    with writer_lock.season_capture_lock(
        26947, 92699, "final", {writer_lock.WRITER_LOCK_DSN_ENV: DSN}
    ):
        pass

    assert timers == []


@pytest.mark.unit
def test_busy_lock_warns_on_the_first_refusal_with_its_key(monkeypatch, caplog):
    # Before #2062 the "busy" warning never fired (``waited`` is already > 0
    # after the first poll), so 3h of waiting left no trace in the logs.
    _fake_psycopg2(monkeypatch, answers=[False, False, True])
    clock = _FakeClock()
    clock.now = 100.0
    real_clock = clock.__call__

    def ticking():
        clock.now += 0.01
        return real_clock()

    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}
    with caplog.at_level("INFO", logger=writer_lock.__name__):
        with writer_lock.bronze_writer_lock(environ, sleep=clock.sleep, clock=ticking):
            pass

    busy = [r for r in caplog.records if "is busy" in r.getMessage()]
    assert len(busy) == 1
    assert str(writer_lock.WRITER_LOCK_KEY) in busy[0].getMessage()
    acquired = [r for r in caplog.records if "acquired after" in r.getMessage()]
    assert str(writer_lock.WRITER_LOCK_KEY) in acquired[0].getMessage()


@pytest.mark.unit
def test_abort_dumps_all_stacks_and_exits_with_the_hold_code(monkeypatch):
    dumps, exits = [], []
    monkeypatch.setattr(
        writer_lock.faulthandler, "dump_traceback",
        lambda **kwargs: dumps.append(kwargs),
    )
    monkeypatch.setattr(writer_lock.os, "_exit", exits.append)

    writer_lock._abort_stuck_holder("hold limit exceeded")

    assert dumps and dumps[0]["all_threads"] is True
    assert exits == [writer_lock.WRITER_LOCK_HOLD_EXIT_CODE]


@pytest.mark.unit
def test_abort_exits_even_when_the_stack_dump_fails(monkeypatch):
    exits = []

    def broken(**kwargs):
        raise ValueError("stderr has no fileno")

    monkeypatch.setattr(writer_lock.faulthandler, "dump_traceback", broken)
    monkeypatch.setattr(writer_lock.os, "_exit", exits.append)

    with pytest.raises(ValueError):
        writer_lock._abort_stuck_holder("hold limit exceeded")

    assert exits == [writer_lock.WRITER_LOCK_HOLD_EXIT_CODE]


@pytest.mark.unit
def test_default_hold_limit_is_below_the_waiters_timeout():
    assert (writer_lock.DEFAULT_WRITER_LOCK_MAX_HOLD_SECONDS
            < writer_lock.DEFAULT_WRITER_LOCK_TIMEOUT_SECONDS)
