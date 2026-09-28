"""Orchestration contract of ``dag_espn_history`` (#1509).

The DAG file lives in ``deploy/espn/dags/`` (the espn-live DAG folder, #1507),
so it is imported by path under the Airflow stubs of this package.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.espn import history

DAG_FILE = Path(__file__).resolve().parents[3] / "deploy" / "espn" / "dags" / "dag_espn_history.py"

pytestmark = pytest.mark.unit


@pytest.fixture
def dag_module():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    spec = importlib.util.spec_from_file_location("dag_espn_history_under_test", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_every_30_minutes_one_run_paused_on_creation(dag_module) -> None:
    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["dag_id"] == "dag_espn_history"
    assert kwargs["schedule"] == "*/30 * * * *"
    assert kwargs["max_active_runs"] == 1
    assert kwargs["catchup"] is False
    assert kwargs["is_paused_upon_creation"] is True


def test_run_is_below_the_live_lane_and_bounded_in_time(dag_module) -> None:
    prepare, run = _task("prepare"), _task("run_history")

    assert prepare.downstream_task_ids == {"run_history"}
    assert run._init_kwargs["pool"] == "espn_history"
    assert run._init_kwargs["priority_weight"] == 1
    assert run._init_kwargs["weight_rule"] == "absolute"
    assert run._init_kwargs["execution_timeout"] == timedelta(minutes=25)
    assert dag_module.BUDGET == timedelta(minutes=20)
    assert dag_module.dag._dag_kwargs["dagrun_timeout"] == timedelta(minutes=28)
    assert run._init_kwargs["retries"] == 0


def test_runs_leave_a_delivery_window_between_them(dag_module) -> None:
    # busy_reason of auto_deliver (cron */5) waits for a history run: budget plus
    # two delivery ticks must fit the 30-min interval, and a stuck run is killed
    # before the next one is due.
    interval = timedelta(minutes=30)
    tick = timedelta(minutes=5)
    assert dag_module.SCHEDULE == "*/30 * * * *"
    assert dag_module.BUDGET + 2 * tick <= interval
    assert dag_module.BUDGET < dag_module.TASK_TIMEOUT < dag_module.DAGRUN_TIMEOUT < interval


def test_prepare_creates_the_queue_table(dag_module, monkeypatch) -> None:
    import scrapers.base.iceberg_writer as iceberg_writer
    import scrapers.espn.bronze_schema as bronze_schema

    monkeypatch.setattr(iceberg_writer, "IcebergWriter", lambda: None)
    monkeypatch.setattr(bronze_schema, "ensure_bronze_tables", lambda writer: None)
    sql: list[str] = []

    class Cursor:
        def execute(self, statement):
            sql.append(statement)

        def fetchall(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(
        dag_module, "_trino", lambda: SimpleNamespace(connection=SimpleNamespace(cursor=Cursor))
    )

    dag_module.prepare()

    assert any(history.QUEUE_TABLE in statement and "CREATE TABLE" in statement for statement in sql)
    assert any("espn_request_journal_v1" in statement for statement in sql)


def test_stop_file_skips_the_run(dag_module, monkeypatch, tmp_path) -> None:
    stop = tmp_path / "history.off"
    stop.touch()
    monkeypatch.setattr(history, "default_stop_file", lambda: stop)
    monkeypatch.setattr(history, "run_history", lambda **_: pytest.fail("run despite the stop file"))
    monkeypatch.setattr(dag_module, "_trino", lambda: pytest.fail("Trino despite the stop file"))

    assert dag_module.run_history(run_id="r") == {"reason": "stopped"}


def test_run_uses_the_history_lane_scope_budget_and_stop_file(dag_module, monkeypatch, tmp_path) -> None:
    stop = tmp_path / "history.off"
    monkeypatch.setattr(history, "default_stop_file", lambda: stop)
    closed = []
    client = SimpleNamespace(close=lambda: closed.append(True))
    trino = SimpleNamespace(connection="conn")
    monkeypatch.setattr(dag_module, "_client", lambda: client)
    monkeypatch.setattr(dag_module, "_trino", lambda: trino)
    seen = {}

    def fake_run(**kwargs):
        seen.update(kwargs)
        return history.HistoryRun(history.IDLE, 380, 0, 1)

    monkeypatch.setattr(history, "run_history", fake_run)
    before = datetime.now(timezone.utc)

    result = dag_module.run_history(run_id="scheduled__1")

    assert result["reason"] == "idle" and result["matches"] == 380
    assert seen["client"] is client and seen["conn"] == "conn" and seen["stop_file"] == stop
    assert seen["scope"] == (("eng.1", 2015),) and seen["run_id"] == "scheduled__1"
    assert seen["task_id"] == "run_history"
    assert timedelta(minutes=19) < seen["deadline"] - before <= timedelta(minutes=20, seconds=5)
    assert closed == [True]


def test_client_is_on_the_history_lane(dag_module, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ESPN_GATE_STATE_PATH", str(tmp_path / "gate.json"))
    monkeypatch.setenv("ESPN_RAW_STORE_URI", f"file://{tmp_path}/raw")
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    client = dag_module._client()
    assert client.lane == "history" and client.gate.lane == "history"
