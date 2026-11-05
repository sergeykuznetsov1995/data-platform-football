"""No Docker, credentials, or network: exercise the gate and transport contracts."""
import contextlib
import io
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts.ops import trino_monitor_ro as client


@pytest.mark.parametrize("sql", [
    "SHOW TABLES FROM iceberg.bronze LIKE 'clubelo%'",
    "SHOW TABLES FROM iceberg.ops LIKE 'understat_ingest_%'",
    "SHOW TABLES FROM iceberg.bronze",
    "SELECT to_iso8601(current_timestamp AT TIME ZONE 'UTC')",
    "SELECT 1", "\n-- monitor\nSELECT '; DELETE FROM x'",
    "WITH x AS (SELECT 1 AS n) SELECT count_if(n > 0) FROM x",
    "SELECT cast(NULL AS varchar) UNION ALL SELECT '__sentinel__'",
    'SELECT count(*) FROM iceberg.bronze."table$snapshots"',
])
def test_accepts_monitor_queries(sql):
    client.validate_sql(sql)


@pytest.mark.parametrize("sql", [
    "SHOW TABLES FROM iceberg.bronze; DELETE FROM t",
    "SHOW TABLES FROM iceberg.bronze LIKE 'x'; SELECT 1",
    "SHOW CREATE TABLE x", "DESCRIBE x",
    "SHOW TABLES FROM iceberg.bronze LIKE 'unterminated",
    "SELECT system.to_iso8601(current_timestamp)",
    "", "-- empty", "SELECT 1; DELETE FROM x", "DELETE FROM x",
    "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
    "SELECT * INTO x FROM y", "CALL system.flush_metadata_cache()",
    "EXPLAIN ANALYZE DELETE FROM x", "CREATE TABLE x AS SELECT 1",
    "SELECT unknown_side_effect()", "SELECT system.query('DELETE FROM x')",
    "SELECT * FROM TABLE(system.query(query => 'DELETE FROM x'))",
    "SELECT 1; SELECT 2", "SELECT * FROM",
])
def test_rejects_before_transport(sql, monkeypatch, capsys):
    def unexpected(*args, **kwargs):
        pytest.fail("transport must not be invoked")
    monkeypatch.setattr(client.subprocess, "run", unexpected)
    assert client.main([sql]) == 2
    assert capsys.readouterr().out == ""


def test_csv_contract_and_container_configuration(monkeypatch):
    class Cursor:
        def execute(self, sql):
            assert sql == "SELECT 1"
        def fetchall(self):
            return [(None, 'a,"b', 42)]
        def close(self):
            pass
    class Connection:
        def cursor(self):
            return Cursor()
        def close(self):
            pass
    def connect(**kwargs):
        assert kwargs["max_attempts"] == 1
        assert kwargs["request_timeout"] == 30
        assert kwargs["session_properties"]["query_max_run_time"] == "840s"
        return Connection()
    monkeypatch.setitem(sys.modules, "trino", SimpleNamespace(dbapi=SimpleNamespace(connect=connect)))
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    monkeypatch.setattr(sys, "argv", ["-c", "SELECT 1"])
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        exec(client.REMOTE_SCRIPT, {})
    assert output.getvalue() == '"","a,""b","42"\n'


@pytest.mark.parametrize("result", [
    SimpleNamespace(returncode=1, stdout='"partial"', stderr="SECRET"),
    SimpleNamespace(returncode=0, stdout="", stderr="SECRET"),
    subprocess.TimeoutExpired("docker", 210), OSError("SECRET"),
])
def test_transport_failure_never_emits_partial_data_or_details(result, monkeypatch, capsys):
    def run(*args, **kwargs):
        if isinstance(result, Exception):
            raise result
        return result
    monkeypatch.setattr(client.subprocess, "run", run)
    assert client.main(["SELECT 1"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "SECRET" not in captured.err


def test_success_uses_bounded_no_bytecode_transport(monkeypatch, capsys):
    def run(command, **kwargs):
        assert command[:5] == ["docker", "exec", "--workdir", "/tmp", "airflow-scheduler"]
        assert command[5:8] == ["python", "-B", "-W"]
        assert kwargs["timeout"] == 870
        return SimpleNamespace(returncode=0, stdout='"1"\n', stderr="")
    monkeypatch.setattr(client.subprocess, "run", run)
    assert client.main(["SELECT 1"]) == 0
    assert capsys.readouterr().out == '"1"\n'


@pytest.mark.parametrize("rows", [[], RuntimeError("SECRET connection details")])
def test_remote_failure_is_empty_and_redacted(rows, monkeypatch, capsys):
    class Cursor:
        def execute(self, sql):
            pass
        def fetchall(self):
            if isinstance(rows, Exception):
                raise rows
            return rows
        def close(self):
            pass
    connection = SimpleNamespace(cursor=Cursor, close=lambda: None)
    monkeypatch.setitem(sys.modules, "trino", SimpleNamespace(
        dbapi=SimpleNamespace(connect=lambda **kwargs: connection)))
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    monkeypatch.setattr(sys, "argv", ["-c", "SELECT 1"])
    with pytest.raises(SystemExit) as error:
        exec(client.REMOTE_SCRIPT, {})
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "SECRET" not in captured.err
