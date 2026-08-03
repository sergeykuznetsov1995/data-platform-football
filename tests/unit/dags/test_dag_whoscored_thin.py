"""Production-shape tests for the thin WhoScored ingest and backfill DAGs."""

from __future__ import annotations

import importlib
import sys

import pytest


def _reload(module_name: str):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop(module_name, None)
    sys.modules.pop(f"dags.{module_name}", None)
    return importlib.import_module(module_name)


def _bash(task_id: str):
    from airflow.operators.bash import BashOperator

    return next(item for item in BashOperator._instances if item.task_id == task_id)


def _python(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(item for item in PythonOperator._instances if item.task_id == task_id)


@pytest.fixture
def ingest():
    return _reload("dag_ingest_whoscored")


@pytest.fixture
def backfill():
    return _reload("dag_backfill_whoscored")


# --------------------------- daily ingest --------------------------------

def test_ingest_dag_shape(ingest):
    from utils.default_args import SCRAPER_ARGS

    assert ingest.dag.dag_id == "dag_ingest_whoscored"
    assert ingest.dag.schedule == "0 10 * * *"
    assert ingest.dag._dag_kwargs["max_active_runs"] == 1
    assert ingest.dag._dag_kwargs["catchup"] is False
    assert ingest.dag._dag_kwargs["default_args"] is SCRAPER_ARGS


def test_ingest_tasks_are_direct_pool_native(ingest):
    discover = _bash("discover_catalog")
    daily = _bash("ingest_daily")
    assert "run_whoscored_scraper.py discover" in discover._init_kwargs["bash_command"]
    cmd = daily._init_kwargs["bash_command"]
    assert "run_whoscored_scraper.py daily" in cmd
    assert "--skip-profiles" in cmd
    assert "--transport-policy direct_only" in cmd
    # No ceremony flags survive on the daily path.
    for banned in ("--proxy-approval", "direct_then_paid", "gateway", "--catalog-batch-id"):
        assert banned not in cmd
    # The residential pool reaches the scraper through the environment.
    assert "WHOSCORED_PROXY_FILE" in daily._init_kwargs["env"]
    assert daily._init_kwargs["append_env"] is True


def test_ingest_has_validation_and_freshness(ingest):
    _python("validate_data")
    freshness = _python("validate_bronze_freshness")
    assert freshness._init_kwargs["trigger_rule"] == "all_done"


# ----------------------------- backfill ----------------------------------

def test_backfill_dag_is_continuous_and_paused(backfill):
    from utils.default_args import SCRAPER_ARGS

    assert backfill.dag.dag_id == "dag_backfill_whoscored"
    assert backfill.dag.schedule == "@continuous"
    assert backfill.dag._dag_kwargs["max_active_runs"] == 1
    assert backfill.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert backfill.dag._dag_kwargs["default_args"] is SCRAPER_ARGS
    params = backfill.dag._dag_kwargs["params"]
    assert set(params) == {"max_work_items"}
    assert params["max_work_items"].default == 100


def test_backfill_drains_full_catalog_over_the_pool(backfill):
    chunk = _bash("run_backfill_chunk")
    cmd = chunk._init_kwargs["bash_command"]
    assert "run_whoscored_scraper.py backfill" in cmd
    assert "--all-catalog" in cmd
    assert "--queue-id whoscored-history" in cmd
    assert "--transport-policy direct_only" in cmd
    for banned in ("--proxy-approval", "direct_then_paid", "gateway"):
        assert banned not in cmd
    assert "WHOSCORED_PROXY_FILE" in chunk._init_kwargs["env"]


def test_backfill_has_finalize_and_cooldown(backfill):
    _python("finalize_chunk")
    cooldown = _python("wait_before_next_continuous_run")
    assert cooldown._init_kwargs["mode"] == "reschedule"


# ------------------------ error budget (#1053) ----------------------------

def _budget_context(tmp_path, report):
    import json

    path = tmp_path / "result.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return {"templates_dict": {"result_path": str(path)}}


def _scopes(total, failed_names=()):
    scopes = [
        {"scope": name, "status": "failed"} for name in failed_names
    ]
    scopes += [
        {"scope": f"OK-{i}", "status": "success"}
        for i in range(total - len(scopes))
    ]
    return scopes


def test_ingest_daily_tolerates_runner_rc_when_report_exists(ingest):
    # Красный rc раннера при живом отчёте — норма (#1053): судит бюджет.
    cmd = _bash("ingest_daily")._init_kwargs["bash_command"]
    assert "|| [ -s" in cmd


def test_freshness_is_not_the_sole_leaf(ingest):
    # #1053: единственный all_done-лист красил ран зелёным при упавшем сборе.
    # Теперь гейт качества — самостоятельный лист, а freshness висит на
    # ingest_daily параллельной веткой.
    validate = _python("validate_data")
    freshness = _python("validate_bronze_freshness")
    assert freshness._init_kwargs["trigger_rule"] == "all_done"
    assert validate.downstream_task_ids == set()
    assert freshness.downstream_task_ids == set()
    assert freshness.upstream_task_ids == {"ingest_daily"}
    assert validate.upstream_task_ids == {"ingest_daily"}


def test_validate_data_passes_within_error_budget(ingest, tmp_path):
    report = {
        "schema_version": 3,
        "status": "failed",
        "rows": 2_956_592,
        "scopes": _scopes(141, ["RUS-Premier League=2526", "TUR-Super Lig=2526"]),
        "errors": ["scope x: ProxyError"],
    }
    ingest.validate_data(**_budget_context(tmp_path, report))


def test_validate_data_fails_on_protected_scope(ingest, tmp_path):
    from airflow.exceptions import AirflowException

    report = {
        "schema_version": 3,
        "status": "failed",
        "rows": 1_000,
        "scopes": _scopes(141, ["ENG-Premier League=2526"]),
        "errors": [],
    }
    with pytest.raises(AirflowException, match="protected"):
        ingest.validate_data(**_budget_context(tmp_path, report))


def test_validate_data_fails_beyond_share_budget(ingest, tmp_path):
    from airflow.exceptions import AirflowException

    failed = [f"X-{i}=2526" for i in range(10)]
    report = {
        "schema_version": 3,
        "status": "failed",
        "rows": 1_000,
        "scopes": _scopes(100, failed),
        "errors": [],
    }
    with pytest.raises(AirflowException, match="budget"):
        ingest.validate_data(**_budget_context(tmp_path, report))


def test_validate_data_fails_on_zero_rows(ingest, tmp_path):
    from airflow.exceptions import AirflowException

    report = {
        "schema_version": 3,
        "status": "failed",
        "rows": 0,
        "scopes": _scopes(141, ["X-1=2526"]),
        "errors": [],
    }
    with pytest.raises(AirflowException, match="zero rows"):
        ingest.validate_data(**_budget_context(tmp_path, report))
