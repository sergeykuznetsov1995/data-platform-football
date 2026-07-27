"""Orchestration contracts for the self-draining Understat history DAG."""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys
from unittest.mock import MagicMock

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


def _scope(league: str, season: str, source_id: int, *, closed: bool = True):
    source_league = {
        "ENG-Premier League": "EPL",
        "ESP-La Liga": "La_liga",
    }[league]
    return SimpleNamespace(
        league=league,
        source_league=source_league,
        season=season,
        source_season_id=source_id,
        is_closed=closed,
        discovered=True,
    )


def test_history_dag_is_paused_continuous_and_strictly_serial(dag_module):
    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["schedule"] == "@continuous"
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
    assert plan._init_kwargs["pool"] == "ingest_scraper_pool"
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


class _Client:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def test_planner_selects_oldest_incomplete_closed_scope(dag_module, monkeypatch):
    import scrapers.understat as understat
    from scrapers.understat import manifest

    scopes = [
        _scope("ENG-Premier League", "2526", 2025, closed=False),
        _scope("ESP-La Liga", "1516", 2015),
        _scope("ENG-Premier League", "1415", 2014),
        _scope("ESP-La Liga", "1415", 2014),
    ]

    class Catalog:
        def __init__(self, _client):
            pass

        def discover_scopes(self, **_kwargs):
            return scopes

    repository = MagicMock()
    repository.is_scope_complete.side_effect = lambda key, **_kwargs: (
        key.league == "ENG-Premier League" and key.season == "1415"
    )
    monkeypatch.setattr(understat, "UnderstatClient", _Client)
    monkeypatch.setattr(understat, "UnderstatCatalog", Catalog)
    monkeypatch.setattr(
        manifest.UnderstatManifestRepository,
        "from_env",
        classmethod(lambda cls: repository),
    )

    plan = dag_module.plan_history_scope(run_id="scheduled__history")

    assert len(plan) == 1
    assert plan[0]["UNDERSTAT_LEAGUE"] == "ESP-La Liga"
    assert plan[0]["UNDERSTAT_SEASON_SLUG"] == "1415"
    assert plan[0]["UNDERSTAT_MODE"] == "backfill"
    assert repository.is_scope_complete.call_count == 2


def test_planner_returns_empty_mapping_when_history_is_drained(dag_module, monkeypatch):
    import scrapers.understat as understat
    from scrapers.understat import manifest

    class Catalog:
        def __init__(self, _client):
            pass

        def discover_scopes(self, **_kwargs):
            return [_scope("ENG-Premier League", "1415", 2014)]

    repository = MagicMock()
    repository.is_scope_complete.return_value = True
    monkeypatch.setattr(understat, "UnderstatClient", _Client)
    monkeypatch.setattr(understat, "UnderstatCatalog", Catalog)
    monkeypatch.setattr(
        manifest.UnderstatManifestRepository,
        "from_env",
        classmethod(lambda cls: repository),
    )

    assert dag_module.plan_history_scope(run_id="scheduled__drained") == []


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
        return value if tz is not None else value.replace(tzinfo=None)


def test_finalize_uses_active_then_idle_cooldowns(dag_module, monkeypatch, tmp_path):
    monkeypatch.setattr(dag_module, "datetime", _FrozenDateTime)
    result = tmp_path / "history.json"
    result.write_text(json.dumps({"status": "complete"}), encoding="utf-8")

    active_ti = MagicMock()
    active_ti.xcom_pull.return_value = [
        {"UNDERSTAT_RESULT_PATH": str(result)}
    ]
    active = dag_module.finalize_history_run(ti=active_ti)
    assert active["did_work"] is True
    assert datetime.fromisoformat(active["next_poll_at"]) == datetime(
        2026, 7, 27, 12, 5, tzinfo=timezone.utc
    )

    idle_ti = MagicMock()
    idle_ti.xcom_pull.return_value = []
    idle = dag_module.finalize_history_run(ti=idle_ti)
    assert idle["did_work"] is False
    assert datetime.fromisoformat(idle["next_poll_at"]) == datetime(
        2026, 7, 27, 12, 30, tzinfo=timezone.utc
    )


def test_invalid_poll_timestamp_fails_closed(dag_module):
    from airflow.exceptions import AirflowException

    ti = MagicMock()
    ti.xcom_pull.return_value = "not-a-timestamp"
    with pytest.raises(AirflowException, match="next-poll timestamp"):
        dag_module.history_poll_ready(ti=ti)


def test_terminal_watcher_runs_after_cooldown_and_is_the_only_safe_leaf(dag_module):
    cooldown = _python("wait_before_next_continuous_run")
    watcher = _python("propagate_history_status")

    assert watcher._init_kwargs["trigger_rule"] == "all_done"
    assert watcher._init_kwargs["python_callable"] is (
        dag_module.propagate_history_status
    )
    assert watcher.upstream_task_ids == {cooldown.task_id}
    assert cooldown.downstream_task_ids == {watcher.task_id}


def test_terminal_watcher_propagates_mapped_runner_failure(dag_module):
    from airflow.exceptions import AirflowException

    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="run_history_scope",
                map_index=3,
                state="failed",
            ),
            SimpleNamespace(
                task_id="wait_before_next_continuous_run",
                map_index=-1,
                state="success",
            ),
        ]
    )

    with pytest.raises(AirflowException, match=r"run_history_scope\[3\]=failed"):
        dag_module.propagate_history_status(dag_run=dag_run)


def test_terminal_watcher_accepts_success_and_zero_scope_skips(dag_module):
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="run_history_scope", map_index=-1, state="skipped"
            ),
            SimpleNamespace(
                task_id="validate_history_scope", map_index=-1, state="skipped"
            ),
            SimpleNamespace(
                task_id="wait_before_next_continuous_run",
                map_index=-1,
                state="success",
            ),
            SimpleNamespace(task_id="unrelated", map_index=-1, state="failed"),
        ]
    )

    assert dag_module.propagate_history_status(dag_run=dag_run) == {
        "status": "success",
        "observed_task_instances": 3,
    }


def test_terminal_watcher_fails_closed_without_current_run_tasks(dag_module):
    from airflow.exceptions import AirflowException

    dag_run = SimpleNamespace(get_task_instances=lambda: [])
    with pytest.raises(AirflowException, match="found no task instances"):
        dag_module.propagate_history_status(dag_run=dag_run)
