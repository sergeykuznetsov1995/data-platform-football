"""#1359: the 00:20 UTC matches-only tail of the SofaScore daily."""

from __future__ import annotations

import csv
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
BASIS = "registry:tournaments.json (дейли до #1370)"


def _load():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_trigger_sofascore_daily_tail", None)
    return importlib.import_module("dag_trigger_sofascore_daily_tail")


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next(t for t in PythonOperator._instances if t.task_id == task_id)


def _denominator_ids():
    with (ROOT / "configs" / "sofascore" / "denominator.tsv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = csv.DictReader(handle, delimiter="\t", quoting=csv.QUOTE_NONE)
        return sorted(row["capture_key"] for row in rows if row["basis"] == BASIS)


@pytest.mark.unit
def test_the_tail_runs_the_denominators_daily_leagues_matches_only():
    module = _load()

    trigger = _task("trigger_sofascore_daily_tail")
    kwargs = trigger._init_kwargs
    assert kwargs["conf"] == {
        "competition_ids": _denominator_ids(),
        "run_players": False,
    }
    assert len(_denominator_ids()) == 7
    assert kwargs["trigger_dag_id"] == "dag_ingest_sofascore"
    # Its own run_id; the daily's run is never reset or reused.
    assert kwargs["trigger_run_id"] == "tail__{{ ts_nodash }}"
    assert "reset_dag_run" not in kwargs
    assert "execution_date" not in kwargs
    assert kwargs["wait_for_completion"] is True
    assert module.dag._dag_kwargs["schedule"] == "20 0 * * *"
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    # The contour creates DAGs paused and delivery unpauses nothing.
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is False
    assert trigger.upstream_task_ids == {"skip_if_daily_running"}


@pytest.mark.unit
def test_the_tail_leagues_are_a_subset_the_ingest_dag_accepts(
    real_medallion_config_dir,
):
    module = _load()
    sys.modules.pop("dag_ingest_sofascore", None)
    ingest = importlib.import_module("dag_ingest_sofascore")

    requested = module.daily_tail_competition_ids()
    assert set(
        ingest._requested_leagues(SimpleNamespace(conf={"competition_ids": requested}))
    ) == set(requested)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("busy", "skipped"),
    [({"running": ["scheduled__1"]}, True), ({"queued": ["manual__2"]}, True), ({}, False)],
)
def test_the_tail_skips_while_the_daily_is_running(monkeypatch, caplog, busy, skipped):
    import airflow.models as models
    from airflow.exceptions import AirflowSkipException

    module = _load()
    asked = []

    class _DagRun:
        @staticmethod
        def find(dag_id, state):
            asked.append((dag_id, state))
            return [SimpleNamespace(run_id=run_id) for run_id in busy.get(state, [])]

    monkeypatch.setattr(models, "DagRun", _DagRun, raising=False)
    if skipped:
        with pytest.raises(AirflowSkipException, match="still running"):
            module._skip_if_daily_running()
        assert "daily tail is skipped" in caplog.text
    else:
        assert module._skip_if_daily_running() is None
    assert {dag_id for dag_id, _ in asked} == {"dag_ingest_sofascore"}
    assert {state for _, state in asked} == {"running", "queued"}
