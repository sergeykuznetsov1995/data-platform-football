from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import sofascore_capture_manifest_guard_v1 as guard


class FakeConnection:
    def __init__(
        self,
        *,
        snapshots=(8, 1),
        fail_on: str | None = None,
        fail_on_occurrence: int = 1,
    ):
        self.snapshots = iter(snapshots)
        self.files = iter(((10, 5000), (2, 1000)))
        self.fail_on = fail_on
        self.fail_on_occurrence = fail_on_occurrence
        self.fail_matches = 0
        self.executed: list[str] = []
        self.cursors = []
        self.closed = False

    def cursor(self):
        connection = self

        class Cursor:
            def __init__(self):
                self.sql = ""
                self.closed = False

            def execute(self, sql):
                self.sql = sql
                connection.executed.append(sql)
                if connection.fail_on and connection.fail_on in sql:
                    connection.fail_matches += 1
                    if connection.fail_matches == connection.fail_on_occurrence:
                        raise RuntimeError("injected Trino failure")

            def fetchall(self):
                if "$snapshots" in self.sql:
                    return [(next(connection.snapshots),)]
                if "$files" in self.sql:
                    return [next(connection.files)]
                if self.sql.startswith("SELECT count(*)"):
                    return [(104,)]
                return []

            def close(self):
                self.closed = True

        cursor = Cursor()
        self.cursors.append(cursor)
        return cursor

    def close(self):
        self.closed = True


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds):
        self.now += seconds


def test_single_cycle_sets_zero_floor_and_never_removes_orphans(monkeypatch):
    conn = FakeConnection()
    events = []
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **fields: events.append((event, fields)),
    )

    result = guard.run_cycle(connection_factory=lambda: conn)

    assert result["before"]["snapshot_count"] == 8
    assert result["after"]["snapshot_count"] == 1
    assert conn.executed[0] == (
        "SET SESSION iceberg.expire_snapshots_min_retention = '0s'"
    )
    optimize_index = conn.executed.index(
        'ALTER TABLE iceberg."ops"."sofascore_capture_manifest" EXECUTE optimize'
    )
    expire_index = conn.executed.index(
        'ALTER TABLE iceberg."ops"."sofascore_capture_manifest" '
        "EXECUTE expire_snapshots(retention_threshold => '0s')"
    )
    assert optimize_index < expire_index
    assert all("remove_orphan_files" not in sql for sql in conn.executed)
    assert [event for event, _fields in events] == ["cycle_before", "cycle_after"]
    assert conn.closed is True
    assert all(cursor.closed for cursor in conn.cursors)


def test_success_resets_consecutive_failure_counter(monkeypatch):
    outcomes = iter(
        (RuntimeError("first"), None, RuntimeError("second"), RuntimeError("third"))
    )
    calls = []
    clock = FakeClock()
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    def cycle():
        calls.append(True)
        outcome = next(outcomes)
        if outcome is not None:
            raise outcome
        return {}

    result = guard.supervise(
        interval=300,
        max_consecutive_failures=2,
        cycle=cycle,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert result == 1
    assert len(calls) == 4
    assert clock.sleeps == [300, 300, 300]


def test_once_success_exits_without_sleep(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    assert guard.supervise(
        once=True,
        cycle=lambda: {},
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    ) == 0
    assert clock.sleeps == []


def test_once_failure_has_distinct_terminal_event(monkeypatch):
    events = []
    clock = FakeClock()
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **fields: events.append(event),
    )

    def fail():
        raise RuntimeError("boom")

    assert guard.supervise(
        once=True,
        cycle=fail,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    ) == 1
    assert events == ["cycle_failed", "once_cycle_failed"]
    assert "failure_threshold_reached" not in events


def test_stop_file_exits_before_a_cycle(tmp_path, monkeypatch):
    stop_file = tmp_path / "stop"
    stop_file.touch()
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    assert guard.supervise(
        stop_file=stop_file,
        cycle=lambda: pytest.fail("stop file must prevent a cycle"),
    ) == 0


def test_single_instance_lock_is_non_blocking(tmp_path):
    lock_file = tmp_path / "guard.lock"

    with guard.single_instance_lock(lock_file):
        with pytest.raises(guard.GuardAlreadyRunning):
            with guard.single_instance_lock(lock_file):
                pytest.fail("second guard must not acquire the lock")

    # The descriptor is closed and the same path can be acquired again.
    with guard.single_instance_lock(lock_file):
        pass


def test_snapshot_limit_is_immediately_fatal(monkeypatch):
    conn = FakeConnection(snapshots=(2001,))
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    with pytest.raises(guard.SnapshotLimitExceeded):
        guard.run_cycle(connection_factory=lambda: conn)

    assert conn.closed is True
    assert not any("EXECUTE optimize" in sql for sql in conn.executed)


def test_post_maintenance_snapshot_limit_is_fatal_and_closes(monkeypatch):
    conn = FakeConnection(snapshots=(2000, 2001))
    events = []
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **kwargs: events.append(event),
    )

    with pytest.raises(guard.SnapshotLimitExceeded, match="after maintenance"):
        guard.run_cycle(connection_factory=lambda: conn)

    assert conn.closed is True
    assert "cycle_after" in events
    assert any("EXECUTE optimize" in sql for sql in conn.executed)
    assert any("EXECUTE expire_snapshots" in sql for sql in conn.executed)


def test_snapshot_limit_cannot_exceed_hard_cap(monkeypatch):
    monkeypatch.setattr(
        guard,
        "_get_trino_connection",
        lambda: pytest.fail("invalid cap must fail before connecting"),
    )

    with pytest.raises(ValueError, match="1 to 2000"):
        guard.run_cycle(max_snapshots=2001)
    with pytest.raises(SystemExit):
        guard.build_parser().parse_args(["--max-snapshots", "2001"])


def test_main_rejects_weakened_cap_before_lock(monkeypatch):
    monkeypatch.setattr(
        guard,
        "single_instance_lock",
        lambda *_args, **_kwargs: pytest.fail("invalid cap must fail before lock"),
    )

    with pytest.raises(SystemExit):
        guard.main(["--max-snapshots", "2001"])


@pytest.mark.parametrize(
    ("stage", "fail_on", "occurrence", "expected_sql_count"),
    [
        (
            "set_session",
            "SET SESSION iceberg.expire_snapshots_min_retention",
            1,
            1,
        ),
        ("optimize", "EXECUTE optimize", 1, 5),
        ("expire", "EXECUTE expire_snapshots", 1, 6),
        (
            "after_stats",
            'SELECT count(*) FROM iceberg."ops"."sofascore_capture_manifest"',
            2,
            7,
        ),
    ],
)
def test_stage_failure_closes_resources_and_runs_no_later_sql(
    monkeypatch,
    stage,
    fail_on,
    occurrence,
    expected_sql_count,
):
    conn = FakeConnection(
        fail_on=fail_on,
        fail_on_occurrence=occurrence,
    )
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="injected Trino failure") as error:
        guard.run_cycle(connection_factory=lambda: conn)

    assert stage in {"set_session", "optimize", "expire", "after_stats"}
    assert str(error.value) == "injected Trino failure"
    assert conn.closed is True
    assert all(cursor.closed for cursor in conn.cursors)
    assert len(conn.executed) == expected_sql_count
    assert fail_on in conn.executed[-1]


def test_cycles_use_fixed_monotonic_start_deadlines(monkeypatch):
    clock = FakeClock()
    starts = []
    monkeypatch.setattr(guard, "_emit", lambda *args, **kwargs: None)

    def cycle():
        starts.append(clock.monotonic())
        if len(starts) == 3:
            raise guard.SnapshotLimitExceeded("test complete")
        clock.advance(50)
        return {}

    assert guard.supervise(
        interval=300,
        cycle=cycle,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    ) == 1
    assert starts == [0, 300, 600]
    assert clock.sleeps == [250, 250]


def test_overrun_skips_missed_deadlines_without_busy_loop(monkeypatch):
    clock = FakeClock()
    starts = []
    events = []
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **kwargs: events.append((event, kwargs)),
    )

    def cycle():
        starts.append(clock.monotonic())
        if len(starts) == 1:
            clock.advance(650)
            return {}
        raise guard.SnapshotLimitExceeded("test complete")

    assert guard.supervise(
        interval=300,
        cycle=cycle,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    ) == 1
    assert starts == [0, 900]
    assert clock.sleeps == [250]
    assert ("cadence_deadline_missed", {"level": 30, "skipped_intervals": 2}) in events


def test_exact_multiple_overrun_starts_immediately_on_deadline(monkeypatch):
    clock = FakeClock()
    starts = []
    events = []
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **kwargs: events.append((event, kwargs)),
    )

    def cycle():
        starts.append(clock.monotonic())
        if len(starts) == 1:
            clock.advance(600)
            return {}
        raise guard.SnapshotLimitExceeded("test complete")

    assert guard.supervise(
        interval=300,
        cycle=cycle,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    ) == 1
    assert starts == [0, 600]
    assert clock.sleeps == []
    assert ("cadence_deadline_missed", {"level": 30, "skipped_intervals": 1}) in events


def test_parser_operational_defaults():
    args = guard.build_parser().parse_args(["--once", "--stop-file", "/tmp/stop"])

    assert args.once is True
    assert args.interval == 300
    assert args.max_consecutive_failures == 2
    assert args.max_snapshots == guard.HARD_MAX_SNAPSHOTS
    assert args.stop_file == Path("/tmp/stop")


def _install_fake_trino(monkeypatch):
    captured = {}

    class BasicAuthentication:
        def __init__(self, user, password):
            self.user = user
            self.password = password

    def connect(**kwargs):
        captured.update(kwargs)
        return object()

    fake = SimpleNamespace(
        auth=SimpleNamespace(BasicAuthentication=BasicAuthentication),
        dbapi=SimpleNamespace(connect=connect),
    )
    monkeypatch.setitem(sys.modules, "trino", fake)
    return captured


def _set_tls_environment(monkeypatch, **values):
    for variable in (
        "TRINO_PASSWORD",
        "TRINO_TLS_VERIFY",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("TRINO_PASSWORD", "secret")
    for variable, value in values.items():
        monkeypatch.setenv(variable, value)


def test_tls_verification_defaults_to_true(monkeypatch):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch)

    guard._get_trino_connection()

    assert captured["http_scheme"] == "https"
    assert captured["verify"] is True


@pytest.mark.parametrize("variable", ["REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"])
def test_tls_ca_bundle_environment_is_forwarded(monkeypatch, variable):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, **{variable: "/run/certs/trino-ca.pem"})

    guard._get_trino_connection()

    assert captured["verify"] == "/run/certs/trino-ca.pem"


def test_tls_verification_requires_explicit_opt_out(monkeypatch):
    captured = _install_fake_trino(monkeypatch)
    events = []
    _set_tls_environment(monkeypatch, TRINO_TLS_VERIFY="false")
    monkeypatch.setattr(
        guard,
        "_emit",
        lambda event, **fields: events.append((event, fields)),
    )

    guard._get_trino_connection()

    assert captured["verify"] is False
    assert events[0][0] == "tls_verification_disabled"


def test_invalid_tls_verification_setting_fails_closed(monkeypatch):
    captured = _install_fake_trino(monkeypatch)
    _set_tls_environment(monkeypatch, TRINO_TLS_VERIFY="sometimes")

    with pytest.raises(RuntimeError, match="TRINO_TLS_VERIFY"):
        guard._get_trino_connection()

    assert captured == {}
