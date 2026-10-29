"""Admission, queue and hard boundary tests for the history controller."""

from datetime import datetime, timedelta, timezone
import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from utils import fbref_pipeline_tasks as tasks


def test_new_identity_paused_one_controller_with_all_writer_pool_and_absolute_priority():
    module = importlib.import_module("dag_fbref_history_controller")
    settings = module.dag._dag_kwargs
    assert module.dag.dag_id == "dag_fbref_history_controller"
    assert settings["is_paused_upon_creation"] is True
    assert module.dag.schedule == "30 4,16,22 * * *"
    assert settings["start_date"].utcoffset() == timedelta(0)
    assert settings["max_active_runs"] == settings["max_active_tasks"] == 1
    assert settings["dagrun_timeout"] == timedelta(minutes=45)
    assert settings["default_args"]["pool"] == tasks.FBREF_SCRAPER_POOL
    assert settings["default_args"]["priority_weight"] == 10
    assert settings["default_args"]["weight_rule"] == "absolute"
    assert module.BACKFILL_MAX_BATCHES == 12
    assert module.MAX_SHARD_SIZE == 1
    assert settings["params"]["max_batches"]._kw["maximum"] == 12
    assert settings["params"]["shard_size"]._kw["maximum"] == 1
    lock = module.acquire_publication_lock._init_kwargs
    assert lock["op_kwargs"]["ttl_seconds"] == lock["timeout"] == 45 * 60
    live = module.live_waves._init_kwargs
    assert live["op_kwargs"]["deadline_seconds"] == 35 * 60
    assert live["execution_timeout"] == timedelta(minutes=40)
    assert module.assert_history_window._init_kwargs["op_kwargs"] == {
        "max_batches": module.MAX_BATCHES
    }
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


@pytest.mark.parametrize('kind', ['generic', 'match'])
@pytest.mark.parametrize('failed_write', [False, True])
def test_batch_processing_supplies_individual_history_timing(
    tmp_path, measured_history, kind, failed_write
):
    import uuid
    from tests.unit.scrapers.test_fbref_pipeline import (
        _pipeline_with_saved_generic_pages,
        _pipeline_with_small_durable_matches,
        _settings,
    )

    campaign, db = measured_history
    if kind == 'match':
        pipeline, control, _ = _pipeline_with_small_durable_matches(
            tmp_path, match_ids=('d000000a', 'd000000b', 'd000000c')
        )
        page_kinds = ['match']
    else:
        pipeline, control, _ = _pipeline_with_saved_generic_pages(
            tmp_path, ['9', '12', '13']
        )
        page_kinds = ['competition']
    # Restore production orchestration, including sampling, around the batch
    # writer doubles used by the isolated cohort tests.
    del pipeline._process_prepared_observations
    now = [0.0]
    pipeline.stage_clock = lambda: now[0]

    def timed(target, name, seconds):
        original = getattr(target, name)

        def call(*args, **kwargs):
            now[0] += seconds
            return original(*args, **kwargs)

        setattr(target, name, call)

    timed(pipeline.raw_store, 'load_fetch_html', 2)
    timed(pipeline, '_parse_generic', 3)
    timed(pipeline, '_apply_stateful_effects', 13)
    timed(pipeline, '_record_page_completion', 17)
    timed(pipeline, '_record_generic_table_results', 1)
    timed(pipeline.generic_writer, 'persist_page', 7)
    if kind == 'match':
        timed(pipeline, '_parse_typed_match', 5)
        timed(pipeline.typed_adapter.writer, 'persist_match', 11)
    if failed_write:
        original_write = pipeline.generic_writer.persist_pages

        def fail_shared_write(items):
            original_write(items)
            now[0] += 9000
            raise RuntimeError('shared write interrupted')

        pipeline.generic_writer.persist_pages = fail_shared_write
    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=page_kinds, settings=_settings()
    )
    assert result.parsed == result.claimed == 3
    assert not result.failures
    expected_ms = (43 + (16 if kind == 'match' else 0)) * 1000
    assert [row['processing_work_ms'] for row in control.observations.values()] == [
        expected_ms, None, None,
    ]
    assert pipeline.generic_writer.batch_sizes == [2]

    # Feed the actual completion arguments into the admission SQL, rather than
    # inventing processing measurements that the batch path cannot produce.
    db.execute('DELETE FROM fbref_control.observation_processing')
    db.execute('DELETE FROM fbref_control.fetch_attempt')
    for index, (key, row) in enumerate(control.observations.items()):
        db.execute(
            'INSERT INTO fbref_control.observation_processing VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (key[0], row['target_id'], row['content_hash'], row['status'],
             'succeeded', row['typed_status'], row['stateful_status'], 'succeeded',
             *key[1:], 1000, 10000 + index * 30, row['processing_work_ms']),
        )
        db.execute(
            'INSERT INTO fbref_control.fetch_attempt VALUES (?,?,?,?,?,?)',
            (key[0], row['target_id'], row['content_hash'], 'succeeded', 500, 200000),
        )
    assert campaign.page_seconds() == 200 + expected_ms // 1000


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


def _slice_clock(monkeypatch, at, page_seconds=180):
    """Pin the admission clock and the measured page duration."""
    from scrapers.fbref.history import HistoryCampaign

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at

    monkeypatch.setattr(tasks, "datetime", Clock)
    monkeypatch.setenv("FBREF_HISTORY_CONTROLLER_ENABLED", "1")
    monkeypatch.setattr(HistoryCampaign, "page_seconds", lambda self: page_seconds)
    monkeypatch.setattr(HistoryCampaign, "has_timing", lambda self: True)
    monkeypatch.setattr(tasks, "_control_store", MagicMock())


@pytest.mark.parametrize(
    "at",
    ["04:30", "04:59", "16:30", "16:59", "22:30", "22:59"],
)
def test_history_admission_opens_in_each_of_three_slices(monkeypatch, at):
    now = datetime.fromisoformat(f"2026-10-09T{at}+00:00")
    _slice_clock(monkeypatch, now)
    verdict = tasks.guard_fbref_history_slice()
    assert verdict["admitted_batches"] >= 1
    assert datetime.fromisoformat(verdict["projected_end"]) <= datetime.fromisoformat(
        verdict["deadline"]
    )


@pytest.mark.parametrize("at", ["04:29:59", "05:00", "12:00", "16:29", "17:00", "22:29:59", "23:00"])
def test_history_admission_refuses_outside_the_three_slices(monkeypatch, at):
    from airflow.exceptions import AirflowFailException

    _slice_clock(monkeypatch, datetime.fromisoformat(f"2026-10-09T{at}+00:00"))
    with pytest.raises(AirflowFailException, match="04:30-05:00, 16:30-17:00, 22:30-23:00"):
        tasks.guard_fbref_history_slice()


@pytest.mark.parametrize(
    "at,page_seconds,ceiling,admitted,deadline",
    [
        # 04:31 + 10 min setup leaves 34 min to 05:15: 11 pages of 3 min,
        # one of them reserved for the slice's recovery batch.
        ("04:31", 180, 12, 10, "05:15"),
        ("16:30", 180, 12, 10, "17:15"),
        ("22:30", 180, 12, 10, "23:15"),
        ("04:31", 600, 12, 2, "05:15"),
        ("04:31", 180, 3, 3, "05:15"),
        ("04:59", 180, 12, 1, "05:15"),
    ],
)
def test_history_admission_picks_largest_batch_count_before_the_deadline(
    monkeypatch, at, page_seconds, ceiling, admitted, deadline
):
    now = datetime.fromisoformat(f"2026-10-09T{at}+00:00")
    _slice_clock(monkeypatch, now, page_seconds)
    verdict = tasks.guard_fbref_history_slice(max_batches=str(ceiling))
    assert verdict["admitted_batches"] == admitted
    assert verdict["pages"] == admitted + 1
    assert verdict["deadline"] == f"2026-10-09T{deadline}:00+00:00"
    assert datetime.fromisoformat(verdict["projected_end"]) <= datetime.fromisoformat(
        verdict["deadline"]
    )
    if admitted < ceiling:
        # One more batch would cross "next ingest window - 45 min".
        from airflow.exceptions import AirflowFailException

        with pytest.raises(AirflowFailException, match="next ingest window"):
            tasks.guard_fbref_history_window(
                max_batches=admitted + 2,
                shard_size=1,
                overhead_minutes=10,
                measured_page_seconds=page_seconds,
                now=now,
            )


def test_history_admission_with_no_room_admits_zero_and_refuses(monkeypatch):
    from airflow.exceptions import AirflowFailException

    _slice_clock(monkeypatch, datetime(2026, 10, 9, 4, 59, tzinfo=timezone.utc), 600)
    with pytest.raises(AirflowFailException, match="next ingest window"):
        tasks.guard_fbref_history_slice()


def test_history_live_waves_run_only_the_admitted_batches_before_the_deadline(
    monkeypatch,
):
    _slice_clock(monkeypatch, datetime(2026, 10, 9, 22, 45, tzinfo=timezone.utc))
    monkeypatch.setenv(tasks.LEGACY_SCRAPER_PYTHON_ENV, sys.executable)
    captured = {}

    class Process:
        pid = 1234
        returncode = 0

        def communicate(self, *, timeout=None):
            return ('FBREF_LIVE_WAVES_RESULT:{"batches": 1}\n', "")

    monkeypatch.setattr(
        tasks.subprocess,
        "Popen",
        lambda command, **_kwargs: captured.update(command=command) or Process(),
    )
    monkeypatch.setattr(tasks, "_process_group_exists", lambda _pgid: False)
    tasks.run_fbref_live_waves(
        airflow_run_id="scheduled__history",
        dag_id="dag_fbref_history_controller",
        worker_id="backfill-live",
        page_kinds=["match"],
        run_type="backfill",
        request_limit=100,
        byte_limit_mb=50,
        shard_size=1,
        max_batches="12",
        deadline_seconds=35 * 60,
    )
    command = captured["command"]
    # 22:45 + 10 min setup leaves 20 min to 23:15: 6 pages, one for recovery.
    assert command[command.index("--max-batches") + 1] == "5"
    assert command[command.index("--deadline-seconds") + 1] == "1800.0"


@pytest.mark.parametrize(
    "kwargs",
    [{"max_batches": 13}, {"shard_size": 2}, {"deadline_seconds": 35 * 60 + 1}],
)
def test_history_live_waves_refuse_beyond_slice_ceilings(monkeypatch, kwargs):
    _slice_clock(monkeypatch, datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc))
    popen = MagicMock()
    monkeypatch.setattr(tasks.subprocess, "Popen", popen)
    arguments = {
        "max_batches": 12, "shard_size": 1, "deadline_seconds": 35 * 60, **kwargs
    }
    with pytest.raises(ValueError, match="History slice"):
        tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__history",
            dag_id="dag_fbref_history_controller",
            worker_id="backfill-live",
            page_kinds=["match"],
            run_type="backfill",
            request_limit=100,
            byte_limit_mb=50,
            **arguments,
        )
    popen.assert_not_called()


def _history_with_timing(monkeypatch, present):
    from scrapers.fbref.history import HistoryCampaign

    monkeypatch.setattr(HistoryCampaign, "has_timing", lambda self: present)


@pytest.mark.parametrize("present,limits", [(False, (100, 50)), (True, (4096, 2048))])
def test_first_history_run_is_initialized_as_canary_by_rule(monkeypatch, present, limits):
    monkeypatch.setenv("FBREF_HISTORY_CONTROLLER_ENABLED", "1")
    monkeypatch.setattr(tasks, "_control_store", MagicMock())
    _history_with_timing(monkeypatch, present)
    pipeline = MagicMock()
    monkeypatch.setattr(tasks, "_pipeline", lambda: pipeline)
    tasks.initialize_fbref_run(
        airflow_run_id="scheduled__history",
        dag_id="dag_fbref_history_controller",
        run_type="backfill",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=1,
        publishing=False,
    )
    settings = pipeline.initialize_run.call_args.kwargs["settings"]
    assert (settings.request_limit, settings.byte_limit // (1024 * 1024)) == limits


@pytest.mark.parametrize("present,limits", [(False, ("100", "50")), (True, ("4096", "2048"))])
def test_first_history_live_waves_use_canary_limits_by_rule(monkeypatch, present, limits):
    _slice_clock(monkeypatch, datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc))
    _history_with_timing(monkeypatch, present)
    monkeypatch.setenv(tasks.LEGACY_SCRAPER_PYTHON_ENV, sys.executable)
    captured = {}

    class Process:
        pid = 1234
        returncode = 0

        def communicate(self, *, timeout=None):
            return ('FBREF_LIVE_WAVES_RESULT:{"batches": 1}\n', "")

    monkeypatch.setattr(
        tasks.subprocess,
        "Popen",
        lambda command, **_kwargs: captured.update(command=command) or Process(),
    )
    monkeypatch.setattr(tasks, "_process_group_exists", lambda _pgid: False)
    result = tasks.run_fbref_live_waves(
        airflow_run_id="scheduled__history",
        dag_id="dag_fbref_history_controller",
        worker_id="backfill-live",
        page_kinds=["match"],
        run_type="backfill",
        request_limit="4096",
        byte_limit_mb="2048",
        shard_size=1,
        max_batches=12,
        deadline_seconds=35 * 60,
    )
    command = captured["command"]
    assert (
        command[command.index("--request-limit") + 1],
        command[command.index("--byte-limit-mb") + 1],
    ) == limits
    assert result["canary_first_run"] is (not present)
    assert result["admitted_batches"] == 10


def test_history_timing_presence_counts_only_measured_pages():
    import sqlite3
    from contextlib import contextmanager
    from scrapers.fbref.history import HistoryCampaign

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("ATTACH DATABASE ':memory:' AS fbref_control")
    db.execute("CREATE TABLE fbref_control.history_timing (run_id text, pages integer)")

    class Cursor:
        def execute(self, sql, params=()):
            self.row = dict(db.execute(sql, params).fetchone())

        def fetchone(self):
            return self.row

    @contextmanager
    def transaction():
        yield Cursor()

    campaign = HistoryCampaign(SimpleNamespace(_transaction=transaction))
    assert campaign.has_timing() is False
    db.execute("INSERT INTO fbref_control.history_timing VALUES ('a', 0)")
    assert campaign.has_timing() is False
    db.execute("INSERT INTO fbref_control.history_timing VALUES ('b', 3)")
    assert campaign.has_timing() is True
