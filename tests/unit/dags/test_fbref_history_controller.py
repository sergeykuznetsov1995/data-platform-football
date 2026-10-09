"""Admission, queue and hard boundary tests for the history controller."""

from datetime import datetime, timedelta, timezone
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from utils import fbref_pipeline_tasks as tasks


def test_new_identity_paused_one_controller_with_all_writer_pool_and_absolute_priority():
    module = importlib.import_module("dag_fbref_history_controller")
    settings = module.dag._dag_kwargs
    assert module.dag.dag_id == "dag_fbref_history_controller"
    assert settings["is_paused_upon_creation"] is True
    assert module.dag.schedule == "30 22 * * *"
    assert settings["start_date"].utcoffset() == timedelta(0)
    assert settings["max_active_runs"] == settings["max_active_tasks"] == 1
    assert settings["dagrun_timeout"] == timedelta(minutes=20)
    assert settings["default_args"]["pool"] == tasks.FBREF_SCRAPER_POOL
    assert settings["default_args"]["priority_weight"] == 10
    assert settings["default_args"]["weight_rule"] == "absolute"
    assert module.BACKFILL_MAX_BATCHES == module.MAX_SHARD_SIZE == 1
    assert settings["params"]["publish"]._kw["enum"] == [False]
    assert module.prepare_campaign.upstream_task_ids == {"acquire_publication_lock"}
    assert "run_live_waves" in module.release_publication_lock.upstream_task_ids
    assert module.acquire_publication_lock._init_kwargs["mode"] == "reschedule"


@pytest.mark.parametrize(
    "dag_id,enabled",
    [("dag_backfill_fbref", "1"), ("dag_fbref_history_controller", "0")],
)
def test_disabled_or_retired_history_never_initializes_a_control_run(
    monkeypatch, dag_id, enabled
):
    monkeypatch.setenv("FBREF_HISTORY_CONTROLLER_ENABLED", enabled)
    pipeline = MagicMock()
    monkeypatch.setattr(tasks, "_pipeline", lambda: pipeline)
    with pytest.raises(Exception, match="retired|disabled"):
        tasks.initialize_fbref_run(
            airflow_run_id="manual__escape",
            dag_id=dag_id,
            run_type="backfill",
            publishing=False,
            shard_size=1,
        )
    pipeline.initialize_run.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [{"publishing": True, "shard_size": 1}, {"publishing": False, "shard_size": 25}],
)
def test_conf_cannot_bypass_nonpublishing_and_one_page_boundary(monkeypatch, kwargs):
    monkeypatch.setenv("FBREF_HISTORY_CONTROLLER_ENABLED", "1")
    pipeline = MagicMock()
    monkeypatch.setattr(tasks, "_pipeline", lambda: pipeline)
    with pytest.raises(ValueError):
        tasks.initialize_fbref_run(
            airflow_run_id="manual__escape",
            dag_id="dag_fbref_history_controller",
            run_type="backfill",
            **kwargs,
        )
    pipeline.initialize_run.assert_not_called()


def test_lock_sensor_returns_false_on_busy_and_current_queue_uses_exact_generation(
    monkeypatch,
):
    control = MagicMock()
    control.acquire_publication_lock.return_value = {"acquired": False, "queued": True}
    monkeypatch.setattr(tasks, "_control_store", lambda: control)
    assert (
        tasks.wait_fbref_publication_lock(
            airflow_run_id="scheduled__x", dag_id="dag_ingest_fbref", ttl_seconds=3600
        )
        is False
    )
    assert control.acquire_publication_lock.call_args.kwargs == {
        "dag_id": "dag_ingest_fbref",
        "ttl_seconds": 3600,
        "queue": True,
    }
    control.acquire_publication_lock.return_value = {
        "acquired": False,
        "idempotent": True,
    }
    assert (
        tasks.wait_fbref_publication_lock(
            airflow_run_id="scheduled__x", dag_id="dag_ingest_fbref", ttl_seconds=3600
        )
        is True
    )


def test_lock_ttl_never_outlives_the_remaining_dagrun(monkeypatch):
    acquire = MagicMock(return_value={"acquired": True})
    monkeypatch.setattr(tasks, "acquire_fbref_publication_lock", acquire)
    now = datetime.now(timezone.utc)
    tasks.wait_fbref_publication_lock(
        airflow_run_id="run",
        dag_id="dag_ingest_fbref",
        ttl_seconds=18 * 3600,
        dag_run=SimpleNamespace(start_date=now - timedelta(hours=17)),
        dag=SimpleNamespace(dagrun_timeout=timedelta(hours=18)),
    )
    assert 3590 <= acquire.call_args.kwargs["ttl_seconds"] <= 3600


def test_history_rechecks_window_after_queue_wait_before_trying_to_acquire(monkeypatch):
    acquire = MagicMock()
    monkeypatch.setattr(tasks, "acquire_fbref_publication_lock", acquire)
    monkeypatch.setattr(
        tasks,
        "guard_fbref_history_slice",
        MagicMock(side_effect=RuntimeError("reserved")),
    )
    with pytest.raises(RuntimeError, match="reserved"):
        tasks.wait_fbref_publication_lock(
            airflow_run_id="history",
            dag_id="dag_fbref_history_controller",
            ttl_seconds=1200,
        )
    acquire.assert_not_called()


@pytest.mark.parametrize("measured", [None, 0, float("nan"), float("inf")])
def test_unknown_invalid_timing_never_admits_history(monkeypatch, measured):
    if measured is None:
        from scrapers.fbref.history import HistoryCampaign

        class Night(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc)

        monkeypatch.setattr(tasks, "datetime", Night)
        monkeypatch.setenv("FBREF_HISTORY_CONTROLLER_ENABLED", "1")
        monkeypatch.setattr(
            HistoryCampaign,
            "page_seconds",
            MagicMock(side_effect=RuntimeError("No successful measured")),
        )
        monkeypatch.setattr(tasks, "_control_store", MagicMock())
        with pytest.raises(RuntimeError, match="No successful measured"):
            tasks.guard_fbref_history_slice()
    else:
        with pytest.raises(ValueError):
            tasks.guard_fbref_history_window(
                max_batches=1,
                shard_size=1,
                measured_page_seconds=measured,
                now=datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc),
            )


def test_old_fetch_only_projection_cannot_admit_a_long_parse_batch():
    with pytest.raises(Exception, match="next ingest window"):
        tasks.guard_fbref_history_window(
            max_batches=1,
            shard_size=25,
            overhead_minutes=10,
            measured_page_seconds=180,
            now=datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc),
        )
