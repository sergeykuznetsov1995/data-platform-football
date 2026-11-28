"""Offline C1 entrypoint contract through the real Trino connection builder."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scrapers.base import trino_manager as base
from scrapers.fotmob import c1_inputs as c1
from scrapers.fotmob import trino_instrumentation


@pytest.mark.unit
@pytest.mark.parametrize("password", [None, "synthetic-c1-password"])
@pytest.mark.parametrize("refresh_fails", [False, True])
def test_run_refresh_connection_contract(monkeypatch, password, refresh_fails):
    # Isolate both authentication branches from any real environment credentials.
    if password is None:
        monkeypatch.delenv("TRINO_SRC_FOTMOB_PASSWORD", raising=False)
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        monkeypatch.delenv("TRINO_USER", raising=False)
    else:
        monkeypatch.setenv("TRINO_SRC_FOTMOB_PASSWORD", password)
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        monkeypatch.delenv("TRINO_USER", raising=False)
    monkeypatch.setenv("TRINO_PORT", "8443" if password else "8080")
    monkeypatch.setattr(base, "TRINO_AVAILABLE", True)

    events = []
    calls = []
    held = []
    connection = object()
    result = {"generation": "synthetic-generation", "seasons": 0, "rows": 1}
    failure = RuntimeError("synthetic refresh failure")

    def check_authority():
        assert authority.held
        events.append("check")

    authority = SimpleNamespace(held=False, check=check_authority)

    @contextmanager
    def writer_lock():
        assert not authority.held
        authority.held = True
        events.append("acquire")
        try:
            yield authority
        finally:
            authority.held = False
            events.append("release")

    def connect(**kwargs):
        events.append("connect")
        held.append(authority.held)
        calls.append(kwargs)
        return connection

    monkeypatch.setattr(
        base,
        "trino",
        SimpleNamespace(
            dbapi=SimpleNamespace(connect=connect),
            auth=SimpleNamespace(
                BasicAuthentication=lambda user, password: ("basic", user, password)
            ),
        ),
        raising=False,
    )
    manager_init = trino_instrumentation.FotMobTrinoTableManager.__init__

    def construct(self, **kwargs):
        events.append("construct")
        manager_init(self, **kwargs)

    def build_writer_lock():
        events.append("build_lock")
        return writer_lock

    monkeypatch.setattr(c1, "wait_for_wave", lambda: events.append("wave"))
    monkeypatch.setattr(trino_instrumentation.FotMobTrinoTableManager, "__init__", construct)
    monkeypatch.setattr(c1, "build_writer_lock", build_writer_lock)

    def refresh(manager, *, writer_lock, stage_manager=None):
        assert type(manager) is trino_instrumentation.FotMobTrinoTableManager
        assert type(stage_manager) is trino_instrumentation.FotMobTrinoTableManager
        assert stage_manager is not manager
        assert not authority.held
        assert manager._write_authority is None
        events.append("refresh")
        # Candidate staging connects and mutates without ever taking a lease.
        assert stage_manager._create_connection() is connection
        with writer_lock() as lease:
            manager.set_write_authority(lease)
            assert manager._authority_required is True
            # Exercise real construction, authority binding and connection forwarding.
            assert manager._create_connection() is connection
            manager.set_write_authority(None)
        assert stage_manager._authority_required is False
        if refresh_fails:
            raise failure
        return result

    monkeypatch.setattr(c1, "refresh", refresh)

    if refresh_fails:
        with pytest.raises(RuntimeError) as caught:
            c1.run_refresh(dag_run=SimpleNamespace(conf={}))
        assert caught.value is failure
    else:
        assert c1.run_refresh(dag_run=SimpleNamespace(conf={})) is result

    assert authority.held is False
    assert events == [
        "wave", "construct", "construct", "build_lock", "refresh", "connect",
        "acquire", "check", "connect", "release",
    ]
    assert held == [False, True]
    assert len(calls) == 2 and calls[0] == calls[1]
    kwargs = calls[0]
    assert kwargs["source"] == "fotmob:c1-inputs"
    assert kwargs["session_properties"] == {
        "enable_dynamic_filtering": "false",
        "query_max_run_time": "300s",
        "query_max_execution_time": "300s",
    }
    assert kwargs["max_attempts"] == 1
    assert kwargs["request_timeout"] == (3, 30)
    assert kwargs["host"] == "trino"
    assert kwargs["user"] == "src_fotmob"
    assert kwargs["catalog"] == "iceberg"
    if password:
        assert kwargs["port"] == 8443
        assert kwargs["http_scheme"] == "https"
        assert kwargs["auth"] == ("basic", "src_fotmob", password)
        assert kwargs["verify"] is False
    else:
        assert kwargs["port"] == 8080
        assert "http_scheme" not in kwargs
        assert "auth" not in kwargs
        assert "verify" not in kwargs


@pytest.mark.unit
@pytest.mark.parametrize("conf", [
    {"first_day": "2026-11-01", "last_day": "2026-11-02"},
    {"unexpected": False},
])
def test_run_refresh_rejects_conf_before_construction_or_lock(monkeypatch, conf):
    def unexpected(*args, **kwargs):
        pytest.fail("C1 conf must be rejected before wave wait, construction, lock or refresh")

    monkeypatch.setattr(c1, "wait_for_wave", unexpected)
    monkeypatch.setattr(trino_instrumentation, "FotMobTrinoTableManager", unexpected)
    monkeypatch.setattr(c1, "build_writer_lock", unexpected)
    monkeypatch.setattr(c1, "refresh", unexpected)

    with pytest.raises(ValueError, match="C1 bootstrap is manual and requires an approved window"):
        c1.run_refresh(dag_run=SimpleNamespace(conf=conf))


@pytest.mark.unit
def test_run_refresh_retries_snapshot_invalidation(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.delenv("TRINO_SRC_FOTMOB_PASSWORD", raising=False)
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    monkeypatch.delenv("TRINO_USER", raising=False)
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setattr(base, "TRINO_AVAILABLE", True)
    monkeypatch.setattr(base, "trino", SimpleNamespace(
        dbapi=SimpleNamespace(connect=lambda **k: object()),
        auth=SimpleNamespace(BasicAuthentication=lambda u, p: ("basic", u, p)),
    ), raising=False)
    monkeypatch.setattr(c1, "wait_for_wave", lambda: None)
    calls = []
    staging = []

    def refresh(manager, *, writer_lock, stage_manager=None):
        calls.append(None)
        staging.append(id(stage_manager))
        if len(calls) < c1.C1_REFRESH_ATTEMPTS:
            raise RuntimeError("C1 source snapshots changed during refresh")
        return {"generation": "g", "seasons": 0, "rows": 1}

    monkeypatch.setattr(c1, "refresh", refresh)
    result = c1.run_refresh(dag_run=SimpleNamespace(conf={}))
    assert result == {"generation": "g", "seasons": 0, "rows": 1}
    assert len(calls) == c1.C1_REFRESH_ATTEMPTS
    # Every attempt stages candidates on the same never-leased manager.
    assert len(set(staging)) == 1 and staging[0] is not None

    fatal = RuntimeError("C1 requires an approved bootstrap")
    calls.clear()

    def refresh_fatal(manager, *, writer_lock, stage_manager=None):
        calls.append(None)
        raise fatal

    monkeypatch.setattr(c1, "refresh", refresh_fatal)
    with pytest.raises(RuntimeError) as caught:
        c1.run_refresh(dag_run=SimpleNamespace(conf={}))
    assert caught.value is fatal
    assert len(calls) == 1
