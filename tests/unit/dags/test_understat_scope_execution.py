"""Scope isolation, retry classification and discovery outage regressions."""

from datetime import datetime, timedelta, timezone
import importlib
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dags.scripts import run_understat_scraper as runner
from scrapers.understat.client import UnderstatPayloadError
from utils import understat_tasks as tasks


def environment():
    return tasks.scope_environment(
        dict(league="ENG-Premier League", season="2627",
             source_season_id=2026, discovered=True),
        mode="current", run_id="test-run",
    )


def test_scope_does_not_allocate_result_file():
    assert "UNDERSTAT_RESULT_PATH" not in environment()


@pytest.mark.parametrize("error", [UnderstatPayloadError("drift"), ValueError("DQ")])
def test_deterministic_errors_have_distinct_exit_code(error):
    assert runner._classify_exception(error)[1] == 2
    assert runner._classify_exception(TimeoutError("transport"))[1] == 1


def operator(monkeypatch, code, report):
    op = tasks.UnderstatScopeOperator(
        task_id="scope", bash_command="exec python runner", env=environment(),
        append_env=True, cwd="/opt/airflow",
    )
    monkeypatch.setattr(op, "cwd", "/opt/airflow", raising=False)
    hook = SimpleNamespace(run_command=Mock(return_value=SimpleNamespace(
        exit_code=code, output=json.dumps(report))), send_sigterm=Mock())
    monkeypatch.setattr(op, "subprocess_hook", hook, raising=False)
    monkeypatch.setattr(op, "get_env", lambda context: op.env, raising=False)
    return op, hook


def test_failed_scope_cannot_block_other_scope_validation(monkeypatch):
    from airflow.exceptions import AirflowFailException
    failed, _ = operator(monkeypatch, 2, {"status": "schema_drift"})
    good, hook = operator(monkeypatch, 0, {"status": "complete"})
    validated = Mock(return_value={"status": "complete", "validated": True})
    monkeypatch.setattr(tasks, "validate_scope_result", validated)
    with pytest.raises(AirflowFailException):
        failed.execute({})
    assert good.execute({}) == {"status": "complete", "validated": True}
    validated.assert_called_once()
    assert hook.run_command.call_args.kwargs["cwd"] == "/opt/airflow"


def test_transport_is_retryable(monkeypatch):
    from airflow.exceptions import AirflowException, AirflowFailException
    op, _ = operator(monkeypatch, 1, dict(status="retryable_failure",
        league="ENG-Premier League", season="2627", source_season_id=2026))
    with pytest.raises(AirflowException) as exc:
        op.execute({})
    assert not isinstance(exc.value, AirflowFailException)


@pytest.mark.parametrize("output", [
    "ModuleNotFoundError: No module named pandas",
    json.dumps(dict(status="schema_drift", league="ENG-Premier League",
                    season="2627", source_season_id=2026)),
    json.dumps(dict(status="retryable_failure", league="ESP-La Liga",
                    season="2627", source_season_id=2026)),
])
def test_exit_one_requires_confirmed_failure_for_this_scope(monkeypatch, output):
    from airflow.exceptions import AirflowFailException
    op, hook = operator(monkeypatch, 1, {})
    hook.run_command.return_value.output = output
    with pytest.raises(AirflowFailException):
        op.execute({})


@pytest.mark.parametrize("code", [2, 3, 7, -9])
def test_non_transport_exit_never_retries(monkeypatch, code):
    from airflow.exceptions import AirflowFailException
    op, _ = operator(monkeypatch, code, {})
    with pytest.raises(AirflowFailException):
        op.execute({})


@pytest.mark.parametrize("output", ["broken JSON", "[]"])
def test_invalid_success_result_fails_without_retry(monkeypatch, output):
    from airflow.exceptions import AirflowFailException
    op, hook = operator(monkeypatch, 0, {})
    hook.run_command.return_value.output = output
    with pytest.raises(AirflowFailException):
        op.execute({})


def test_discovery_schema_drift_does_not_retry_or_fallback(monkeypatch, tmp_path):
    from airflow.exceptions import AirflowFailException
    import scrapers.understat as understat
    module = importlib.import_module("dag_ingest_understat")
    client = SimpleNamespace(get_stat_data=Mock(return_value={"stat": []}),
                             close=Mock(), cache_dir=tmp_path)
    monkeypatch.setattr(understat, "UnderstatClient", lambda **kwargs: client)
    with pytest.raises(AirflowFailException):
        module.plan_current_scopes(ti=SimpleNamespace(try_number=4))
    assert client.get_stat_data.call_count == 1
    client.close.assert_called_once()


@pytest.mark.parametrize("name", ["dag_ingest_understat", "dag_backfill_understat"])
def test_graph_has_no_cross_scope_validator_barrier(name):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop(name, None)
    importlib.import_module(name)
    assert not any(op.task_id.startswith("validate_")
                   for op in PythonOperator._instances)
    [op] = BashOperator._instances
    assert isinstance(op, tasks.UnderstatScopeOperator)
    assert op._init_kwargs["execution_timeout"] == timedelta(minutes=45)


def test_three_discovery_failures_then_calendar_fallback(monkeypatch, tmp_path):
    from airflow.exceptions import AirflowException
    import scrapers.understat as understat
    module = importlib.import_module("dag_ingest_understat")
    monkeypatch.setenv("UNDERSTAT_CACHE_DIR", str(tmp_path))
    client = SimpleNamespace(get_stat_data=Mock(side_effect=TimeoutError("outage")),
                             close=Mock(), cache_dir=tmp_path)
    monkeypatch.setattr(understat, "UnderstatClient", lambda **kwargs: client)
    boundary = datetime(2026, 10, 5, tzinfo=timezone.utc)
    for attempt in (1, 2, 3):
        with pytest.raises(AirflowException):
            module.plan_current_scopes(logical_date=boundary,
                                      ti=SimpleNamespace(try_number=attempt))
    plan = module.plan_current_scopes(logical_date=boundary,
                                     ti=SimpleNamespace(try_number=4))
    assert len(plan) == 12
    assert {row["UNDERSTAT_SEASON_SLUG"] for row in plan} == {"2526", "2627"}
    assert all(row["UNDERSTAT_SOURCE_DISCOVERED"] == "false" for row in plan)
    assert client.get_stat_data.call_count == 4
    assert client.close.call_count == 4
