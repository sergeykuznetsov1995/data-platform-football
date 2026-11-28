"""ClubElo source authentication at the Bronze entry points."""

import importlib.util
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, call, sentinel

import pytest
import requests
import trino.auth
import trino.dbapi

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.clubelo import daily, history
from scrapers.clubelo.trino_client import ClubEloIcebergWriter


ARCHIVE = Path(__file__).resolve().parents[3] / "scripts" / "clubelo_freeze_archive.py"
PASSWORD_VAR = "TRINO_SRC_CLUBELO_PASSWORD"
STORES = (daily.IcebergDailyStore, history.IcebergHistoryStore)


def load_archive(monkeypatch):
    spec = importlib.util.spec_from_file_location("archive_under_another_name", ARCHIVE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def dbapi(monkeypatch):
    monkeypatch.setenv("TRINO_HOST", "source.test")
    monkeypatch.delenv("TRINO_PORT", raising=False)
    monkeypatch.setenv("TRINO_USER", "airflow")
    monkeypatch.setenv("TRINO_PASSWORD", "generic-test-password")
    monkeypatch.setenv(PASSWORD_VAR, "source-test-password")
    monkeypatch.setattr(TrinoTableManager, "_trino_unreachable", False)
    auth = Mock(return_value=sentinel.auth)
    connect = Mock(side_effect=lambda **kwargs: Mock())
    monkeypatch.setattr(trino.auth, "BasicAuthentication", auth)
    monkeypatch.setattr(trino.dbapi, "connect", connect)
    return connect, auth


def session_call(host="source.test", port=8443, catalog="iceberg"):
    return call(host=host, port=port, catalog=catalog, user="src_clubelo",
                http_scheme="https", verify=False, auth=sentinel.auth)


@pytest.mark.parametrize("store", STORES)
def test_default_store_opens_lazily_and_reauthenticates(store, monkeypatch, dbapi):
    connect, auth = dbapi
    manager = store().writer._get_trino_manager()
    connect.assert_not_called()
    first = manager.connection
    first.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    assert manager.connection is first
    manager._reset_connection()
    monkeypatch.setenv(PASSWORD_VAR, "rotated-test-password")
    assert manager.connection is not first
    assert connect.call_args_list == [session_call(), session_call()]
    assert auth.call_args_list == [call("src_clubelo", "source-test-password"),
                                   call("src_clubelo", "rotated-test-password")]
    manager._reset_connection()
    monkeypatch.delenv(PASSWORD_VAR)
    with pytest.raises(RuntimeError, match=PASSWORD_VAR):
        _ = manager.connection
    assert connect.call_count == 2
    assert not TrinoTableManager._trino_unreachable


def test_explicit_writer_settings_win_over_environment(monkeypatch, dbapi):
    connect, _ = dbapi
    monkeypatch.setenv("TRINO_PORT", "18443")
    writer = ClubEloIcebergWriter(trino_host="explicit.test", trino_port=9443, catalog="archive")
    assert writer._get_trino_manager().connection is not None
    assert connect.call_args == session_call("explicit.test", 9443, "archive")


@pytest.mark.parametrize("port", [None, "18443"])
def test_archive_opens_fresh_source_sessions(port, monkeypatch, dbapi):
    connect, auth = dbapi
    if port is None:
        monkeypatch.delenv("TRINO_PASSWORD")
    else:
        monkeypatch.setenv("TRINO_PORT", port)
    archive = load_archive(monkeypatch)
    assert archive._connect() is not archive._connect()
    assert connect.call_args_list == [session_call(port=int(port or 8443))] * 2
    assert auth.call_args_list == [call("src_clubelo", "source-test-password")] * 2


@pytest.mark.parametrize("password", [None, ""])
def test_missing_source_password_stops_real_operations(password, monkeypatch, dbapi):
    connect, auth = dbapi
    if password is None:
        monkeypatch.delenv(PASSWORD_VAR)
    else:
        monkeypatch.setenv(PASSWORD_VAR, password)
    monkeypatch.setattr(requests.Session, "request", lambda *a, **kw: pytest.fail("network call"))
    monkeypatch.setattr(daily, "run_daily", Mock(side_effect=AssertionError("daily started")))
    monkeypatch.setattr(history, "run_history", Mock(side_effect=AssertionError("history started")))
    archive = load_archive(monkeypatch)
    for operation in (daily.run_default, history.run_default, archive._connect):
        with pytest.raises(RuntimeError, match=PASSWORD_VAR) as exc:
            operation()
        assert "generic-test-password" not in str(exc.value)
    connect.assert_not_called()
    auth.assert_not_called()


@pytest.mark.parametrize("store", STORES)
def test_injected_writer_needs_no_credentials(store, monkeypatch, dbapi):
    monkeypatch.delenv(PASSWORD_VAR)
    assert store(writer=sentinel.writer).writer is sentinel.writer
    dbapi[0].assert_not_called()


def test_bulk_append_names_the_source_in_its_snapshot(monkeypatch, dbapi):
    """A catalog append never passes Trino: the snapshot summary is the only
    place that names its writer, and the Trino meter reads it (#2132)."""
    from scrapers.clubelo import trino_client

    writer = trino_client.ClubEloIcebergWriter()
    target, aligned = Mock(), ["aligned row"]
    monkeypatch.setattr(writer, "_load_pyiceberg_table", Mock(return_value=target))
    monkeypatch.setattr(writer, "_align_pyiceberg_arrow_table", Mock(return_value=aligned))
    frame = ["row"]
    rows = writer._append_dataframe_pyiceberg(frame, database="bronze", table="clubelo_raw_page")
    assert rows == 1
    writer._load_pyiceberg_table.assert_called_once_with(database="bronze", table="clubelo_raw_page")
    writer._align_pyiceberg_arrow_table.assert_called_once_with(frame, target=target)
    target.append.assert_called_once_with(aligned, snapshot_properties={
        "dpf.source": "clubelo", "dpf.operation": "bulk-arrow-append"})
    assert trino_client.SNAPSHOT_STAMP == {"dpf.source": "clubelo", "dpf.operation": "bulk-arrow-append"}


@pytest.mark.parametrize("password,command", [(None, "verify"), ("source-test-password", "copy")])
def test_standalone_archive_reaches_auth_check(tmp_path, password, command):
    # An isolated interpreter exercises the direct script import from outside the repo.
    env = dict(TRINO_USER="airflow", TRINO_PASSWORD="generic-test-password",
               TRINO_HOST="[", TRINO_PORT="8443")
    if password is not None:
        env[PASSWORD_VAR] = password
    child = """
import runpy
import sys

sys.modules["pyarrow"] = None
import trino.dbapi

def synthetic_connect(**kwargs):
    print("synthetic DBAPI called")
    sys.exit(0)

trino.dbapi.connect = synthetic_connect
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
"""
    result = subprocess.run([sys.executable, "-I", "-c", child, str(ARCHIVE), command], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=30)
    output = result.stdout + result.stderr
    assert "generic-test-password" not in output
    if password is None:
        assert result.returncode != 0
        assert PASSWORD_VAR in output
        assert "synthetic DBAPI called" not in output
    else:
        assert result.returncode == 0, output
        assert "synthetic DBAPI called" in output
