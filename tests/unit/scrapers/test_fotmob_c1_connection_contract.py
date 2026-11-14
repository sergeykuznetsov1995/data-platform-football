"""Offline C1 entrypoint contract through the real Trino connection builder."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scrapers.base import trino_manager as base
from scrapers.fotmob import c1_inputs as c1
from scrapers.fotmob import maintenance, trino_instrumentation


@pytest.mark.unit
@pytest.mark.parametrize("password", [None, "synthetic-c1-password"])
@pytest.mark.parametrize("refresh_fails", [False, True])
def test_run_refresh_connection_contract(monkeypatch, password, refresh_fails):
    # Isolate both authentication branches from any real environment credentials.
    if password is None:
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    else:
        monkeypatch.setenv("TRINO_PASSWORD", password)
    monkeypatch.setenv("TRINO_PORT", "8443" if password else "8080")
    monkeypatch.setattr(base, "TRINO_AVAILABLE", True)

    events = []
    calls = []
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
        assert authority.held
        events.append("connect")
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
    monkeypatch.setattr(maintenance, "writer_lock", writer_lock)

    def refresh(manager):
        assert type(manager) is trino_instrumentation.FotMobTrinoTableManager
        assert authority.held
        assert manager._write_authority is authority
        assert manager._authority_required is True
        events.append("refresh")
        # Exercise real construction, authority binding and connection forwarding.
        assert manager._create_connection() is connection
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
    assert events == ["acquire", "refresh", "check", "connect", "release"]
    assert len(calls) == 1
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
    assert kwargs["user"] == "airflow"
    assert kwargs["catalog"] == "iceberg"
    if password:
        assert kwargs["port"] == 8443
        assert kwargs["http_scheme"] == "https"
        assert kwargs["auth"] == ("basic", "airflow", password)
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
        pytest.fail("C1 conf must be rejected before construction, lock or refresh")

    monkeypatch.setattr(trino_instrumentation, "FotMobTrinoTableManager", unexpected)
    monkeypatch.setattr(maintenance, "writer_lock", unexpected)
    monkeypatch.setattr(c1, "refresh", unexpected)

    with pytest.raises(ValueError, match="C1 bootstrap is manual and requires an approved window"):
        c1.run_refresh(dag_run=SimpleNamespace(conf=conf))
