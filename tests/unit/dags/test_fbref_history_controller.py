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


@pytest.fixture
def measured_history():
    """Execute admission SQL offline against a batch with queued completions."""
    import sqlite3
    from contextlib import contextmanager
    from scrapers.fbref import history

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("ATTACH DATABASE ':memory:' AS fbref_control")
    db.execute("""CREATE TABLE fbref_control.observation_processing (
        logical_refresh_id text, target_id text, content_hash text,
        status text, generic_status text, typed_status text, stateful_status text,
        validation_status text, parser_version text, typed_parser_version text,
        stateful_parser_version text, started_at real, completed_at real,
        processing_work_ms integer)""")
    db.execute("""CREATE TABLE fbref_control.fetch_attempt (
        logical_refresh_id text, target_id text, content_hash text,
        status text, started_at real, latency_ms integer)""")
    for i in range(3):
        db.execute(
            "INSERT INTO fbref_control.observation_processing VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(i), str(i), 'hash', 'succeeded', 'succeeded', 'skipped',
             'succeeded', 'succeeded', history.PAGE_DOCUMENT_VERSION,
             history.TYPED_BRONZE_PARSER_VERSION, history.DISCOVERY_PARSER_VERSION,
             1000, 2000 + i * 30, 30000),
        )
        db.execute("INSERT INTO fbref_control.fetch_attempt VALUES (?,?,?,?,?,?)",
                   (str(i), str(i), 'hash', 'succeeded', 500, 200000))
    db.execute("INSERT INTO fbref_control.fetch_attempt VALUES (?,?,?,?,?,?)",
               ('0', '0', 'hash', 'failed', 0, 1000))

    class Cursor:
        def execute(self, sql, params=()):
            if 'history_timing' in sql:
                self.row = {'seconds': None}
            else:
                # Only PostgreSQL's interval extraction needs translation for
                # the pre-fix regression run; timestamps here are epoch seconds.
                sql = sql.replace(
                    'extract(epoch FROM (observed.completed_at-attempt.started_at))',
                    '(observed.completed_at-attempt.started_at)',
                )
                self.row = dict(db.execute(sql.replace('%s', '?'), params).fetchone())

        def fetchone(self):
            return self.row

    @contextmanager
    def transaction():
        yield Cursor()

    yield history.HistoryCampaign(SimpleNamespace(_transaction=transaction)), db
    db.close()


def test_initial_timing_measures_page_work_without_batch_queue_or_retry(measured_history):
    campaign, db = measured_history
    assert campaign.page_seconds() == 230
    db.execute('UPDATE fbref_control.observation_processing SET completed_at=completed_at+9000')
    assert campaign.page_seconds() == 230


@pytest.mark.parametrize('change', [
    'processing_work_ms=NULL', 'processing_work_ms=0', "status='failed'",
    "generic_status='failed'", "typed_status='failed'", "stateful_status='failed'",
    "validation_status='failed'", "parser_version='old'",
    "typed_parser_version='old'", "stateful_parser_version='old'",
])
def test_initial_timing_refuses_unmeasured_or_incompatible_observations(measured_history, change):
    campaign, db = measured_history
    db.execute('UPDATE fbref_control.observation_processing SET ' + change)
    with pytest.raises(RuntimeError, match='history stays closed'):
        campaign.page_seconds()


@pytest.mark.parametrize('change', [
    'latency_ms=NULL', 'latency_ms=0', "status='failed'",
    "target_id='other'", "content_hash='other'", "logical_refresh_id='other'",
])
def test_initial_timing_requires_matching_measured_fetch(measured_history, change):
    campaign, db = measured_history
    db.execute('UPDATE fbref_control.fetch_attempt SET ' + change)
    with pytest.raises(RuntimeError, match='history stays closed'):
        campaign.page_seconds()


def test_initial_timing_excludes_ambiguous_successes(measured_history):
    campaign, db = measured_history
    db.execute("""INSERT INTO fbref_control.fetch_attempt
        SELECT logical_refresh_id,target_id,content_hash,status,started_at,latency_ms
        FROM fbref_control.fetch_attempt WHERE status='succeeded'""")
    with pytest.raises(RuntimeError, match='history stays closed'):
        campaign.page_seconds()


def test_initial_timing_uses_max_of_latest_100_usable_observations(measured_history):
    campaign, db = measured_history
    db.execute('UPDATE fbref_control.observation_processing SET processing_work_ms=999000')
    for i in range(3, 103):
        db.execute("""INSERT INTO fbref_control.observation_processing
            SELECT ?,?,?,status,generic_status,typed_status,stateful_status,
            validation_status,parser_version,typed_parser_version,stateful_parser_version,
            started_at,?,30001 FROM fbref_control.observation_processing LIMIT 1""",
            (str(i), str(i), 'hash', 3000 + i))
        db.execute("INSERT INTO fbref_control.fetch_attempt VALUES (?,?,?,?,?,?)",
                   (str(i), str(i), 'hash', 'succeeded', 500, 200000))
    assert campaign.page_seconds() == 231
    db.execute("UPDATE fbref_control.fetch_attempt SET latency_ms=1")
    db.execute("UPDATE fbref_control.observation_processing SET processing_work_ms=1")
    assert campaign.page_seconds() == 180


def test_history_run_timing_remains_authoritative():
    from scrapers.fbref.history import HistoryCampaign

    cursor = MagicMock()
    cursor.fetchone.return_value = {'seconds': 230001}
    control = MagicMock()
    control._transaction.return_value.__enter__.return_value = cursor
    assert HistoryCampaign(control).page_seconds() == 231
    assert cursor.execute.call_count == 1
