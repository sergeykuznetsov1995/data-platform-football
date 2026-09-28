"""Orchestration contracts for the on-demand Understat history DAG."""

from __future__ import annotations

import importlib
from pathlib import Path
import sys

import pytest


def _reload_dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    for name in (
        "dag_ingest_understat",
        "dags.dag_ingest_understat",
        "dag_backfill_understat",
        "dags.dag_backfill_understat",
    ):
        sys.modules.pop(name, None)
    return importlib.import_module("dag_backfill_understat")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _bash(task_id: str):
    from airflow.operators.bash import BashOperator

    return next(task for task in BashOperator._instances if task.task_id == task_id)


def _python(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_history_dag_is_paused_daily_cron_and_strictly_serial(dag_module):
    from datetime import timedelta

    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["schedule"] == "0 12 * * *"
    assert kwargs["dagrun_timeout"] == timedelta(hours=5)
    assert kwargs["is_paused_upon_creation"] is True
    assert kwargs["max_active_runs"] == 1
    assert kwargs["max_active_tasks"] == 1
    assert kwargs["catchup"] is False


def test_history_module_does_not_import_daily_dag_at_top_level(dag_module):
    """Prevent Airflow from registering the daily DAG under this file too."""

    assert "dag_ingest_understat" not in sys.modules
    assert not any(
        getattr(value, "dag_id", None) == "dag_ingest_understat"
        for value in vars(dag_module).values()
    )


def test_real_airflow_dagbag_accepts_understat_pair_if_available():
    """Exercise DagContext autoregistration when a real Airflow is installed."""

    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow not installed")
    if not hasattr(DagBag, "process_file"):
        pytest.skip("Stubbed Airflow detected")

    dags_dir = Path(__file__).resolve().parents[3] / "dags"
    bag = DagBag(dag_folder=str(dags_dir), include_examples=False)
    understat_files = {
        "dag_backfill_understat.py",
        "dag_ingest_understat.py",
    }
    relevant_errors = {
        path: error
        for path, error in bag.import_errors.items()
        if Path(path).name in understat_files
    }
    assert relevant_errors == {}
    assert Path(bag.dags["dag_backfill_understat"].fileloc).name == (
        "dag_backfill_understat.py"
    )
    assert Path(bag.dags["dag_ingest_understat"].fileloc).name == (
        "dag_ingest_understat.py"
    )


def test_history_runner_is_one_mapped_exact_scope(dag_module):
    plan = _python("plan_history_scope")
    runner = _bash("run_history_scope")
    validator = _python("validate_history_scope")

    assert runner.is_mapped is True
    assert runner._expand_kwargs["env"].operator is plan
    assert validator.is_mapped is True
    assert validator._expand_kwargs["op_kwargs"].operator is plan
    assert "pool" not in plan._init_kwargs
    assert plan._init_kwargs["priority_weight"] == dag_module.BACKFILL_PRIORITY
    assert runner._init_kwargs["pool"] == "ingest_scraper_pool"
    assert runner._init_kwargs["priority_weight"] == dag_module.BACKFILL_PRIORITY
    assert "--mode backfill" in runner.bash_command
    assert "--league \"${UNDERSTAT_LEAGUE}\"" in runner.bash_command
    assert "--season-slug \"${UNDERSTAT_SEASON_SLUG}\"" in runner.bash_command
    assert "--source-season-id \"${UNDERSTAT_SOURCE_SEASON_ID}\"" in runner.bash_command
    assert "--source-discovered \"${UNDERSTAT_SOURCE_DISCOVERED}\"" in runner.bash_command
    assert "--output \"${UNDERSTAT_RESULT_PATH}\"" in runner.bash_command


def test_daily_scope_has_higher_shared_pool_priority(dag_module):
    current = importlib.import_module("dag_ingest_understat")
    assert current.CURRENT_PRIORITY > dag_module.BACKFILL_PRIORITY


def test_history_graph_is_plan_run_validate_only(dag_module):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    task_ids = {
        task.task_id
        for task in [*BashOperator._instances, *PythonOperator._instances]
    }
    assert task_ids == {
        "plan_history_scope",
        "run_history_scope",
        "validate_history_scope",
    }
    plan = _python("plan_history_scope")
    runner = _bash("run_history_scope")
    validator = _python("validate_history_scope")
    assert plan.downstream_task_ids == {runner.task_id}
    assert runner.downstream_task_ids == {validator.task_id}
    assert validator.downstream_task_ids == set()
    assert not hasattr(dag_module, "PythonSensor")


class _Query:
    def __init__(self, rows=None, error=None):
        self.calls = []
        self.rows = rows or []
        self.error = error

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        if self.error is not None:
            raise self.error
        return self.rows


def _patch_repository(monkeypatch, query):
    import scrapers.understat as understat
    from scrapers.understat import manifest

    def _no_site(*_args, **_kwargs):
        raise AssertionError("history plan must not contact Understat")

    monkeypatch.setattr(understat, "UnderstatClient", _no_site)
    monkeypatch.setattr(understat, "UnderstatCatalog", _no_site)
    monkeypatch.setattr(
        manifest.UnderstatManifestRepository,
        "from_env",
        classmethod(lambda cls: cls(query=query)),
    )


def test_planner_without_work_runs_one_sql_and_no_site_request(
    dag_module, monkeypatch
):
    query = _Query(rows=[])
    _patch_repository(monkeypatch, query)

    assert dag_module.plan_history_scope(run_id="scheduled__drained") == []
    assert len(query.calls) == 1
    assert query.calls[0][0].lstrip().startswith("WITH keys AS")


def test_planner_selects_incomplete_closed_scopes_in_manifest_order(
    dag_module, monkeypatch
):
    query = _Query(
        rows=[
            ("ENG-Premier League", "1415", "EPL", 2014),
            ("ESP-La Liga", "1516", "La_liga", 2015),
        ]
    )
    _patch_repository(monkeypatch, query)

    plan = dag_module.plan_history_scope(run_id="scheduled__history")

    assert [(item["UNDERSTAT_LEAGUE"], item["UNDERSTAT_SEASON_SLUG"]) for item in plan] == [
        ("ENG-Premier League", "1415"),
        ("ESP-La Liga", "1516"),
    ]
    assert plan[1]["UNDERSTAT_SOURCE_SEASON_ID"] == "2015"
    assert {item["UNDERSTAT_MODE"] for item in plan} == {"backfill"}
    assert {item["UNDERSTAT_SOURCE_DISCOVERED"] for item in plan} == {"true"}
    assert len(query.calls) == 1
    sql, params = query.calls[0]
    assert 'ORDER BY k."source_season_id", k."league" LIMIT ?' in sql
    assert params[-1] == dag_module.HISTORY_SCOPES_PER_RUN == 12


def test_planner_bounds_history_by_current_source_season(dag_module, monkeypatch):
    from scrapers.understat import catalog

    query = _Query(rows=[])
    _patch_repository(monkeypatch, query)
    monkeypatch.setattr(catalog, "current_source_season_id", lambda: 2031)

    dag_module.plan_history_scope(run_id="scheduled__bound")

    assert query.calls[0][1][0] == 2031


def test_planner_fails_loudly_when_manifest_query_fails(dag_module, monkeypatch):
    query = _Query(error=RuntimeError("Table 'iceberg.ops.x' does not exist"))
    _patch_repository(monkeypatch, query)

    with pytest.raises(RuntimeError, match="does not exist"):
        dag_module.plan_history_scope(run_id="scheduled__broken")
    assert len(query.calls) == 1
