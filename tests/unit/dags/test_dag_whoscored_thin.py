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
