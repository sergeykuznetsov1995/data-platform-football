from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import SimpleNamespace

import pytest


MODULE = "dags.dag_refresh_sofascore_all_mens"
REFRESH_KNOBS = (
    "SOFASCORE_REFRESH_EVIDENCE_DIR",
    "SOFASCORE_EVIDENCE_CODE_REVISION",
    "SOFASCORE_REFRESH_BATCH_SIZE",
    "SOFASCORE_REFRESH_POOL",
    "SOFASCORE_REFRESH_MAX_ACTIVE_TASKS",
    "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES",
    "SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES",
    "SOFASCORE_REFRESH_MAX_DUE",
    "SOFASCORE_REFRESH_MAX_STALE",
    "SOFASCORE_REFRESH_CHASE_PAGES",
    "SOFASCORE_REFRESH_MAX_SEED",
    "SOFASCORE_REFRESH_SEED_PAGES",
    "SOFASCORE_REFRESH_WINDOW_HOURS",
    "SOFASCORE_REFRESH_RESULT_DIR",
    "SOFASCORE_REFRESH_PROXY_CONTROL_URL",
    "SOFASCORE_METADATA_SEASONS_PER_RUN",
    "SOFASCORE_REFRESH_SECONDS_PER_MATCH",
)


def test_optional_evidence_captures_same_full_pending_read(clean_env, monkeypatch, tmp_path):
    from scrapers.sofascore.denominator import Denominator, DenominatorRow

    module = _load_dag_module(monkeypatch)
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}")
    monkeypatch.setattr(module, "WORKLOAD_ARTIFACT", str(artifact))
    monkeypatch.setenv("SOFASCORE_REFRESH_EVIDENCE_DIR", str(tmp_path / "evidence"))
    monkeypatch.setenv("SOFASCORE_EVIDENCE_CODE_REVISION", "a" * 40)
    import scrapers.sofascore.denominator as denominator
    registry = Denominator({n: DenominatorRow(n, f"SS-{n}", str(n), "core", 1, "fixture")
                            for n in range(1, 22)})
    monkeypatch.setattr(denominator, "load_denominator", lambda: registry)
    monkeypatch.setattr(denominator, "denominator_path", lambda: artifact)
    rows = [(f"SS-{n}", "2627", 2, 1792024800, 1, n + 100) for n in range(1, 22)]
    selected = [{"SOFASCORE_TOURNAMENT_ID": str(n), "SOFASCORE_SOURCE_SEASON_ID": str(n + 100),
                 "SOFASCORE_SCOPE_KEY": f"c:{n}:{n + 100}", "SOFASCORE_SCOPE_TIMEOUT_S": "300"}
                for n in range(1, 9)]
    context = dict(run_id="scheduled__fixture", dag_run=SimpleNamespace(
        logical_date=datetime(2026, 10, 15, tzinfo=timezone.utc),
        start_date=datetime(2026, 10, 15, tzinfo=timezone.utc),
        run_type=DagRunType.SCHEDULED))
    snapshot = {"snapshot_id": "d" * 64, "campaign_id": "e" * 64}
    calls = []
    monkeypatch.setattr(module, "_pending_refresh_partitions", lambda: calls.append("existing_read") or rows)
    monkeypatch.setattr(module.state, "read_snapshot", lambda *a, **k: snapshot)
    monkeypatch.setattr(module.state, "plan_refresh_batch", lambda snapshot, pending, **kwargs:
                        selected if pending is rows else pytest.fail("planner input changed"))
    assert len(module._plan_refresh_batch(**context)) == 8
    assert calls == ["existing_read"]
    [path] = (tmp_path / "evidence").glob("*/*.json")
    saved = json.loads(path.read_text())
    assert saved["due_core_scopes"] == 21 and len(saved["selected"]) == 8
    assert saved["due_core_matches"] == 42 and saved["overdue_core_matches"] == 21
    assert saved["identity"]["code_revision"] == "a" * 40
    window = saved["capture_window"]
    assert datetime.fromisoformat(window["started_at"]) > context["dag_run"].start_date
    assert datetime.fromisoformat(window["deadline_at"]).timestamp() == int(
        selected[0]["SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH"])
    assert 14399 <= (datetime.fromisoformat(window["deadline_at"])
                     - datetime.fromisoformat(window["started_at"])).total_seconds() <= 14400


def test_disabled_evidence_has_no_input_or_filesystem_effect(clean_env, monkeypatch):
    module = _load_dag_module(monkeypatch)
    # Invalid inputs are deliberately untouched when the optional installation
    # has not happened; no additional read, query, directory or task is created.
    module._capture_refresh_evidence(None, None, None, None, None, None)


class DagRunType(str, Enum):
    """Airflow's concrete ``str, Enum`` DagRunType representation."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    BACKFILL = "backfill"


def _load_dag_module(monkeypatch=None):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop(MODULE, None)
    return importlib.import_module(MODULE)


def _operators():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    return {
        item.task_id: item
        for item in (*BashOperator._instances, *PythonOperator._instances)
    }


@pytest.fixture
def clean_env(monkeypatch):
    for name in REFRESH_KNOBS:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.unit
def test_refresh_dag_runs_three_times_a_day_with_one_bounded_batch(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)
    dag = module.dag
    operators = _operators()

    assert dag.dag_id == "dag_refresh_sofascore_all_mens"
    assert dag.schedule == "30 0,8,15 * * *"
    assert dag._dag_kwargs["max_active_runs"] == 1
    assert dag._dag_kwargs["catchup"] is False
    assert dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=7)
    assert dag._dag_kwargs["is_paused_upon_creation"] is True
    assert dag._dag_kwargs["default_args"] is module.DEFAULT_ARGS
    assert set(operators) == {
        "refresh_season_schedules",
        "plan_refresh_batch",
        "run_refresh_scope",
        "validate_refresh_scope",
        "refill_refresh_window",
        "enrich_season_metadata",
        "propagate_refresh_status",
    }

    fetch = operators["refresh_season_schedules"]
    assert "run_sofascore_schedule_refresh.py" in fetch.bash_command
    assert '--budget-cap-bytes "${SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES}"' in (
        fetch.bash_command
    )
    assert '--dag-id "${AIRFLOW_CTX_DAG_ID}"' in fetch.bash_command
    assert '--run-id "${AIRFLOW_CTX_DAG_RUN_ID}"' in fetch.bash_command
    for flag in (
        '--max-due "${SOFASCORE_REFRESH_MAX_DUE}"',
        '--max-stale "${SOFASCORE_REFRESH_MAX_STALE}"',
        '--chase-pages "${SOFASCORE_REFRESH_CHASE_PAGES}"',
        '--max-seed "${SOFASCORE_REFRESH_MAX_SEED}"',
        '--seed-pages "${SOFASCORE_REFRESH_SEED_PAGES}"',
        '--window-hours "${SOFASCORE_REFRESH_WINDOW_HOURS}"',
        '--per-lease-max-bytes "${SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES}"',
        "schedule-sweep-cursor.json",
        "schedule-sweep-incomplete.json",
    ):
        assert flag in fetch.bash_command
    # #1358: the sweep is sized in pages — 150 min x 20/min x 0.8 — and the
    # byte cap is derived from them by the run (0 = derive, no fixed 64 MiB).
    assert '--page-budget "${SOFASCORE_REFRESH_PAGE_BUDGET}"' in fetch.bash_command
    assert fetch.env["SOFASCORE_REFRESH_PAGE_BUDGET"] == "2400"
    assert module.REFRESH_PAGE_BUDGET == 2400
    assert fetch.env["SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES"] == "0"
    # The preflight counts one warm-up per lease, so the ceiling has to reach it.
    assert fetch.env["SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES"] == str(8 * 1024 * 1024)
    # Every due season of the run; the env value is an emergency bound only.
    assert fetch.env["SOFASCORE_REFRESH_MAX_DUE"] == "4096"
    assert fetch.env["SOFASCORE_REFRESH_MAX_STALE"] == "200"
    assert fetch.env["SOFASCORE_REFRESH_CHASE_PAGES"] == "3"
    assert fetch.env["SOFASCORE_REFRESH_MAX_SEED"] == "40"
    assert fetch.env["SOFASCORE_REFRESH_SEED_PAGES"] == "12"
    assert fetch.env["SOFASCORE_REFRESH_WINDOW_HOURS"] == "36"
    assert fetch.env["SOFASCORE_CAMPAIGN_SNAPSHOT"] == module.SNAPSHOT_PATH
    assert fetch.env["SOFASCORE_REFRESH_RESULT_DIR"] == (
        "/opt/airflow/runtime/sofascore/all-men/refresh-results"
    )
    assert fetch.append_env is True
    assert fetch._init_kwargs["pool"] == "ingest_scraper_pool"
    # Sol r12 #2: a second attempt shares the DagRun's paid budget with the
    # first while the preflight sizes the full plan again — lesson #7.  The
    # sweep saves its state class by class, so the next scheduled run resumes
    # it instead.
    assert fetch._init_kwargs["retries"] == 0
    assert fetch._init_kwargs["execution_timeout"] == timedelta(minutes=150)
    # #1358: 7 h hold the sweep, the scopes' shared window, the reserve for
    # their retries and the metadata enrichment: 150 min + 2 h + 2 h + 25 min.
    assert module.REFRESH_SCOPE_BUDGET == timedelta(hours=2)
    assert module.REFRESH_RETRY_RESERVE == timedelta(hours=2)
    assert (
        module.REFRESH_FETCH_TIMEOUT
        + module.REFRESH_SCOPE_BUDGET
        + module.REFRESH_RETRY_RESERVE
        + module.METADATA_TIMEOUT
        <= module.REFRESH_DAGRUN_TIMEOUT
    )
    assert not hasattr(module, "REFRESH_BATCH_FITS")
    assert module.REFRESH_BATCH_SIZE == 128
    assert module.REFRESH_SECONDS_PER_MATCH == 15

    run = operators["run_refresh_scope"]
    assert run.is_mapped
    # Every scope brings its own timeout in the mapped kwargs.
    assert run._expand_kwargs_arg.operator.task_id == "plan_refresh_batch"
    assert run._expand_kwargs_strict is False
    assert "execution_timeout" not in run._init_kwargs
    assert run._init_kwargs["pool"] == "ingest_scraper_pool"
    assert run._init_kwargs["priority_weight"] == 5
    assert run._init_kwargs["retries"] == 1
    assert run._init_kwargs["retry_delay"] >= timedelta(seconds=60)
    assert run._init_kwargs["max_active_tis_per_dag"] == 1
    assert run._init_kwargs["do_xcom_push"] is False
    for flag in (
        "run_sofascore_scope_cycle.py",
        "--phase matches",
        "--season-evidence bronze",
        "--allow-pending-season",
        '--run-id "${SOFASCORE_SCOPE_RUN_ID}"',
        '--expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}"',
    ):
        assert flag in run.bash_command

    validate = operators["validate_refresh_scope"]
    assert validate.is_mapped
    assert validate._expand_kwargs["op_kwargs"].operator.task_id == (
        "plan_refresh_batch"
    )


@pytest.mark.unit
def test_refresh_dag_has_one_all_done_leaf(clean_env, monkeypatch):
    _load_dag_module(monkeypatch)
    operators = _operators()

    leaves = [
        task_id for task_id, item in operators.items()
        if not item.downstream_task_ids
    ]
    assert leaves == ["propagate_refresh_status"]
    propagate = operators["propagate_refresh_status"]
    assert propagate._init_kwargs["trigger_rule"] == "all_done"
    assert operators["plan_refresh_batch"].upstream_task_ids == {
        "refresh_season_schedules"
    }
    assert operators["run_refresh_scope"].upstream_task_ids == {
        "plan_refresh_batch"
    }
    assert operators["validate_refresh_scope"].upstream_task_ids == {
        "run_refresh_scope"
    }
    assert operators["enrich_season_metadata"].upstream_task_ids == {
        "validate_refresh_scope", "run_refresh_scope", "refill_refresh_window"
    }
    assert propagate.upstream_task_ids == {"enrich_season_metadata"}


@pytest.mark.unit
def test_refresh_lane_knobs_come_from_env(clean_env, monkeypatch):
    monkeypatch.setenv("SOFASCORE_REFRESH_BATCH_SIZE", "3")
    monkeypatch.setenv("SOFASCORE_REFRESH_POOL", "sofascore_refresh_pool")
    monkeypatch.setenv("SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", "2")
    monkeypatch.setenv("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "4194304")
    monkeypatch.setenv("SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES", "2097152")
    monkeypatch.setenv("SOFASCORE_REFRESH_MAX_DUE", "120")
    monkeypatch.setenv("SOFASCORE_REFRESH_MAX_SEED", "20")
    monkeypatch.setenv("SOFASCORE_REFRESH_RESULT_DIR", "/tmp/refresh")
    monkeypatch.setenv(
        "SOFASCORE_REFRESH_PROXY_CONTROL_URL", "http://sofascore-gw-refresh:8080"
    )
    module = _load_dag_module(monkeypatch)
    operators = _operators()

    assert module.dag._dag_kwargs["max_active_tasks"] == 2
    fetch = operators["refresh_season_schedules"]
    assert fetch._init_kwargs["pool"] == "sofascore_refresh_pool"
    assert fetch.env["SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES"] == "4194304"
    # Sol r10 #5: the lease ceiling is what the preflight counts warm-ups by,
    # and an override of it used to stop at the DAG.
    assert fetch.env["SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES"] == "2097152"
    assert fetch.env["SOFASCORE_REFRESH_MAX_DUE"] == "120"
    assert fetch.env["SOFASCORE_REFRESH_MAX_SEED"] == "20"
    assert fetch.env["SOFASCORE_REFRESH_RESULT_DIR"] == "/tmp/refresh"
    assert fetch.env["SOFASCORE_PROXY_CONTROL_URL"] == (
        "http://sofascore-gw-refresh:8080"
    )
    run = operators["run_refresh_scope"]
    assert run._init_kwargs["pool"] == "sofascore_refresh_pool"
    assert run._init_kwargs["max_active_tis_per_dag"] == 2
    kwargs = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=DagRunType.MANUAL, conf={}),
    )
    # #1358: the env value is only the upper bound on the number of scopes;
    # the shared window decides how many actually go.
    assert kwargs["batch_size"] == 3
    assert kwargs["scope_budget_s"] == 2 * 3600
    assert kwargs["seconds_per_match"] == 15
    assert kwargs["result_dir"] == "/tmp/refresh"
    assert kwargs["task_env"] == {
        "SOFASCORE_PROXY_CONTROL_URL": "http://sofascore-gw-refresh:8080"
    }


@pytest.mark.unit
def test_task_env_survives_airflow_template_rendering(clean_env, monkeypatch):
    """``env`` is a template field: a native-rendering DAG turns a numeric string
    back into an int and BashOperator's Popen dies with ``expected str ... not int``.
    """

    from jinja2 import Environment
    from jinja2.nativetypes import NativeEnvironment

    module = _load_dag_module(monkeypatch)
    native = bool(module.dag._dag_kwargs.get("render_template_as_native_obj"))
    jinja = NativeEnvironment() if native else Environment()

    for name, value in _operators()["refresh_season_schedules"].env.items():
        rendered = jinja.from_string(value).render()
        assert isinstance(rendered, str), (
            f"{name} renders as {type(rendered).__name__}, not str"
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SOFASCORE_REFRESH_BATCH_SIZE", "0"),
        ("SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", "two"),
        ("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "-1"),
        ("SOFASCORE_REFRESH_SECONDS_PER_MATCH", "0"),
        ("SOFASCORE_REFRESH_MAX_DUE", "0"),
        ("SOFASCORE_REFRESH_MAX_SEED", "0"),
        ("SOFASCORE_REFRESH_SEED_PAGES", "нет"),
    ],
)
def test_invalid_refresh_lane_knob_fails_dag_parse(
    clean_env, monkeypatch, name, value
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        _load_dag_module(monkeypatch)


def _planner_kwargs(module, monkeypatch, **context):
    """Drive ``_plan_refresh_batch`` to the planner and capture its kwargs."""

    captured = {}

    def _plan(snapshot, pending_partitions, **kwargs):
        captured["snapshot"] = snapshot
        captured["pending_partitions"] = pending_partitions
        captured.update(kwargs)
        return []

    monkeypatch.setattr(
        module.state, "read_snapshot", lambda *a, **k: {"campaign_id": "c"}
    )
    monkeypatch.setattr(
        module, "_pending_refresh_partitions",
        lambda: [("SS-17", "2627", 4, 1_787_788_800)],
    )
    monkeypatch.setattr(module.state, "plan_refresh_batch", _plan)
    assert module._plan_refresh_batch(run_id="manual__1", **context) == []
    return captured


@pytest.mark.unit
def test_plan_task_feeds_bronze_partitions_without_daily_exclusions(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)

    kwargs = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=DagRunType.MANUAL, conf={}),
    )

    assert kwargs["snapshot"] == {"campaign_id": "c"}
    assert kwargs["pending_partitions"] == [
        ("SS-17", "2627", 4, 1_787_788_800)
    ]
    # #1370: the daily registry leagues are planned here as well.
    assert "exclude_tournament_ids" not in kwargs
    # #1358: the batch size (128) is only the upper bound on the number of
    # scopes; the planner fills the 2 h scope window by their estimates.
    assert kwargs["batch_size"] == 128
    assert kwargs["scope_budget_s"] == 7200
    assert kwargs["dag_run_id"] == "manual__1"
    assert kwargs["snapshot_path"] == module.SNAPSHOT_PATH
    assert kwargs["workload_artifact"] == module.WORKLOAD_ARTIFACT
    assert kwargs["task_env"] == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("run_type", "interval_end", "conf"),
    [
        ("scheduled", datetime(2026, 8, 27, 0, 30, tzinfo=timezone.utc), {}),
        ("scheduled", datetime(2026, 8, 27, 8, 30, tzinfo=timezone.utc), {}),
        ("scheduled", datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc), {}),
        (DagRunType.SCHEDULED, datetime(2026, 8, 27, 0, 30, tzinfo=timezone.utc), {}),
        (DagRunType.BACKFILL, datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc), {}),
        ("scheduled", datetime(2026, 8, 27, 11, 30, tzinfo=timezone.utc), {}),
        ("scheduled", None, {"queue_mode": "backlog"}),
    ],
)
def test_plan_task_uses_one_deadline_queue_for_every_slot(
    clean_env, monkeypatch, run_type, interval_end, conf
):
    # #1359: no fresh/backlog split by slot; conf on a scheduled run is ignored.
    module = _load_dag_module(monkeypatch)

    kwargs = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=run_type, conf=conf),
        data_interval_end=interval_end,
    )

    assert kwargs["queue_mode"] == "deadline"


@pytest.mark.unit
def test_plan_task_manual_defaults_to_deadline_and_allows_backlog_override(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)

    default = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=DagRunType.MANUAL, conf={}),
    )
    overridden = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(
            run_type=DagRunType.MANUAL, conf={"queue_mode": "backlog"}
        ),
    )

    assert default["queue_mode"] == "deadline"
    assert overridden["queue_mode"] == "backlog"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("dag_run", "interval_end", "message"),
    [
        (
            SimpleNamespace(run_type="manual", conf={"queue_mode": "oldest"}),
            None,
            "queue_mode",
        ),
        (
            SimpleNamespace(run_type="manual", conf={"queue_mode": "fresh"}),
            None,
            "queue_mode",
        ),
        (SimpleNamespace(run_type="manual", conf=["backlog"]), None, "mapping"),
        (SimpleNamespace(run_type="unexpected", conf={}), None, "run_type"),
        (SimpleNamespace(conf={}), None, "run_type"),
    ],
)
def test_plan_task_fails_closed_for_invalid_mode_or_run_type(
    clean_env, monkeypatch, dag_run, interval_end, message
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)
    with pytest.raises(AirflowException, match=message):
        _planner_kwargs(
            module,
            monkeypatch,
            dag_run=dag_run,
            data_interval_end=interval_end,
        )


@pytest.mark.unit
def test_plan_task_fails_closed_without_a_dag_run(clean_env, monkeypatch):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)

    with pytest.raises(AirflowException, match="run_type"):
        module._plan_refresh_batch(run_id="manual__1")


@pytest.mark.unit
def test_pending_partitions_skip_only_the_season_a_moved_match_was_refused_in(
    clean_env, monkeypatch
):
    """#1778: a match journaled as season_mismatch leaves only that season."""
    import duckdb
    import sqlglot

    module = _load_dag_module(monkeypatch)
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS iceberg")
    conn.execute("CREATE SCHEMA iceberg.bronze")
    conn.execute(
        "CREATE TABLE iceberg.bronze.sofascore_schedule (league VARCHAR, season VARCHAR,"
        " game_id BIGINT, season_id DOUBLE, status_type VARCHAR,"
        " start_timestamp BIGINT, changes_change_timestamp BIGINT)"
    )
    conn.execute(
        "CREATE TABLE iceberg.bronze.sofascore_match_capture_status"
        " (league VARCHAR, season VARCHAR, match_id VARCHAR, capture_complete BOOLEAN)"
    )
    conn.execute(
        "CREATE TABLE iceberg.bronze.sofascore_rejected_rows"
        " (league VARCHAR, season VARCHAR, reason_code VARCHAR, natural_key VARCHAR)"
    )
    conn.execute(
        "INSERT INTO iceberg.bronze.sofascore_schedule VALUES"
        " ('SS-16437', '2026', 17003244, 90005.0, 'finished', 1000, NULL),"
        " ('SS-16437', '2026', 17003245, 90005.0, 'finished', 1000, NULL),"
        " ('SS-16437', '2027', 17003244, 91000.0, 'finished', 1000, NULL)"
    )
    conn.execute(
        "INSERT INTO iceberg.bronze.sofascore_rejected_rows VALUES"
        " ('SS-16437', '2026', 'season_mismatch', '90005|17003244'),"
        " ('SS-16437', '2026', 'invalid_enum_value', '90005|17003245')"
    )
    sql = sqlglot.transpile(module.PENDING_PARTITIONS_SQL, read="trino", write="duckdb")[0]
    rows = sorted(
        (league, season, pending, source)
        for league, season, pending, _, _, source, _ in conn.execute(sql).fetchall()
    )
    assert rows == [("SS-16437", "2026", 1, 90005), ("SS-16437", "2027", 1, 91000)]


@pytest.mark.unit
def test_pending_partitions_query_joins_finished_games_without_complete_capture(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)
    executed = []

    class _Cursor:
        def execute(self, sql, *params):
            executed.append(sql)

        def fetchall(self):
            return [
                ("SS-17", "2627", 12, 1_787_788_800, 5, 76986, 1_787_700_000),
                ("SS-8", 2026, 3, None, 0, 98037, None),
            ]

    class _Connection:
        closed = False

        def cursor(self):
            return _Cursor()

        def close(self):
            self.closed = True

    connection = _Connection()
    import utils.silver_tasks as silver_tasks

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", lambda: connection)

    partitions = module._pending_refresh_partitions()

    assert partitions == [
        ("SS-17", "2627", 12, 1_787_788_800, 5, 76986, 1_787_700_000),
        ("SS-8", "2026", 3, None, 0, 98037, None),
    ]
    assert connection.closed is True
    sql = executed[0]
    assert "iceberg.bronze.sofascore_schedule" in sql
    assert "iceberg.bronze.sofascore_match_capture_status" in sql
    assert "LIKE 'SS-%'" in sql
    assert "status_type = 'finished'" in sql
    assert "capture_complete" in sql
    # #1359: the nearest deadline that has not passed, by the meter's rule.
    from scrapers.sofascore.match_deadline import match_deadline_sql

    assert match_deadline_sql(
        "s.start_timestamp", "s.changes_change_timestamp"
    ) in sql
    normalized_sql = " ".join(sql.split())
    assert "SELECT DISTINCT s.league" in normalized_sql
    assert (
        "min(CASE WHEN p.deadline >= to_unixtime(current_timestamp) "
        "THEN CAST(p.deadline AS bigint) END) AS min_open_deadline_ts"
    ) in normalized_sql
    assert "AS open_deadline_matches" in normalized_sql
    assert "count(*) AS pending_matches" in normalized_sql
    # #1751: the most recently missed deadline orders the debt.
    assert (
        "max(CASE WHEN p.deadline < to_unixtime(current_timestamp) "
        "THEN CAST(p.deadline AS bigint) END) AS max_overdue_deadline_ts"
    ) in normalized_sql


def _refresh_env(result_path, scope_key="c:8:825"):
    return {
        "SOFASCORE_CAMPAIGN_ACTION": "refresh",
        "SOFASCORE_SCOPE_RESULT_PATH": str(result_path),
        "SOFASCORE_EXPECTED_SNAPSHOT_ID": "s",
        "SOFASCORE_SCOPE_KEY": scope_key,
    }


@pytest.mark.unit
def test_validate_refresh_scope_checks_provenance_without_marking_completed(
    clean_env, monkeypatch, tmp_path
):
    module = _load_dag_module(monkeypatch)
    result = tmp_path / "result.json"
    result.write_text(
        '{"status": "success", "snapshot_id": "s", "campaign_id": "c",'
        ' "tournament_id": 8, "source_season_id": 825}'
    )

    def _forbidden(*a, **k):
        raise AssertionError("refresh must not touch the campaign state")

    monkeypatch.setattr(module.state, "mark_completed", _forbidden)
    monkeypatch.setattr(module.state, "clear_failed", _forbidden)

    assert module._validate_refresh_scope(env=_refresh_env(result)) == {
        "status": "refreshed", "scope_key": "c:8:825", "elapsed_s": None
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("document", "message"),
    [
        ('{"status": "failed"}', "did not finish successfully"),
        (
            '{"status": "success", "snapshot_id": "s", "campaign_id": "other",'
            ' "tournament_id": 8, "source_season_id": 825}',
            "provenance mismatch",
        ),
        (
            '{"status": "success", "snapshot_id": "s", "campaign_id": "c",'
            ' "tournament_id": 8, "source_season_id": 824}',
            "provenance mismatch",
        ),
        (
            '{"status": "success", "snapshot_id": "s", "campaign_id": "c",'
            ' "tournament_id": 17, "source_season_id": 825}',
            "provenance mismatch",
        ),
    ],
)
def test_validate_refresh_scope_fails_closed(
    clean_env, monkeypatch, tmp_path, document, message
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)
    result = tmp_path / "result.json"
    result.write_text(document)

    with pytest.raises(AirflowException, match=message):
        module._validate_refresh_scope(env=_refresh_env(result))


@pytest.mark.unit
def test_validate_refresh_scope_accepts_a_snapshot_revised_after_planning(
    clean_env, monkeypatch, tmp_path
):
    # #1354: enrichment advances the snapshot between plan and run; the same
    # campaign, tournament and season still validate.
    module = _load_dag_module(monkeypatch)
    result = tmp_path / "result.json"
    result.write_text(
        '{"status": "success", "snapshot_id": "revised", "campaign_id": "c",'
        ' "tournament_id": 8, "source_season_id": 825}'
    )

    assert module._validate_refresh_scope(env=_refresh_env(result)) == {
        "status": "refreshed", "scope_key": "c:8:825", "elapsed_s": None
    }


@pytest.mark.unit
def test_validate_refresh_scope_rejects_other_campaign_actions(
    clean_env, monkeypatch, tmp_path
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)
    result = tmp_path / "result.json"
    result.write_text('{"status": "success"}')
    environment = _refresh_env(result)
    environment["SOFASCORE_CAMPAIGN_ACTION"] = "capture"

    with pytest.raises(AirflowException, match="unknown SofaScore campaign action"):
        module._validate_refresh_scope(env=environment)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("states", "failed"),
    [
        ({"refresh_season_schedules": "success", "run_refresh_scope": "success"}, []),
        (
            {"refresh_season_schedules": "failed", "plan_refresh_batch": "success",
             "run_refresh_scope": "success", "validate_refresh_scope": "success"},
            ["refresh_season_schedules"],
        ),
        (
            {"refresh_season_schedules": "failed", "plan_refresh_batch": "upstream_failed"},
            ["plan_refresh_batch", "refresh_season_schedules"],
        ),
        ({"run_refresh_scope": "failed", "unrelated_task": "failed"}, ["run_refresh_scope"]),
    ],
)
def test_propagate_refresh_status_is_the_single_honest_leaf(
    clean_env, monkeypatch, states, failed
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ]
    )

    if failed:
        with pytest.raises(AirflowException, match=", ".join(failed)):
            module._propagate_status(dag_run=dag_run)
    else:
        verdict = module._propagate_status(dag_run=dag_run)
        assert verdict["status"] == "success"
        assert verdict["partial"] == 0


@pytest.mark.unit
def test_failed_sweep_allows_planning_existing_bronze_after_fetch(clean_env, monkeypatch):
    _load_dag_module(monkeypatch)
    plan = _operators()["plan_refresh_batch"]
    assert plan._init_kwargs["trigger_rule"] == "all_done"
    assert plan.upstream_task_ids == {"refresh_season_schedules"}


@pytest.mark.unit
def test_enrich_season_metadata_task_shape(clean_env, monkeypatch):
    module = _load_dag_module(monkeypatch)
    enrich = _operators()["enrich_season_metadata"]

    assert enrich._init_kwargs["trigger_rule"] == "all_done"
    assert enrich._init_kwargs["retries"] == 0
    assert enrich._init_kwargs["execution_timeout"] == module.METADATA_TIMEOUT
    # Astra r1 #2: the enrichment runs inside the same DagRun window as the
    # sweep, the scopes' window and the reserve for their retries (#1358).
    assert (
        module.REFRESH_FETCH_TIMEOUT
        + module.REFRESH_SCOPE_BUDGET
        + module.REFRESH_RETRY_RESERVE
        + module.METADATA_TIMEOUT
        <= module.REFRESH_DAGRUN_TIMEOUT
    )
    assert enrich._init_kwargs["pool"] == "ingest_scraper_pool"
    assert enrich._init_kwargs["priority_weight"] < 5
    # A failed enrichment is a red task, never a red DagRun.
    assert "enrich_season_metadata" not in module.REFRESH_TASK_IDS
    assert module.METADATA_SEASONS_PER_RUN == 170


def _fake_enrichment(module, monkeypatch, tmp_path, *, returncode=0):
    calls = []
    monkeypatch.setattr(module, "METADATA_RESULT_DIR", str(tmp_path / "metadata-results"))
    monkeypatch.setattr(
        module.state, "read_snapshot",
        lambda *_a, **_k: {"snapshot_id": "snap-1", "campaign_id": "camp-1"},
    )

    def run(command, *, env, cwd, check):
        calls.append({"command": command, "env": env, "cwd": cwd})
        report = command[command.index("--report") + 1]
        with open(report, "w", encoding="utf-8") as handle:
            handle.write(
                '{"status": "success", "ready_wave_scopes": 7,'
                ' "source_requests": 9, "traffic": {"paid_proxy_bytes": 123}}'
            )
        return SimpleNamespace(returncode=returncode)

    monkeypatch.setattr(module.subprocess, "run", run)
    return calls


@pytest.mark.unit
def test_enrich_season_metadata_builds_the_priority_command(
    clean_env, monkeypatch, tmp_path
):
    monkeypatch.setenv("SOFASCORE_REFRESH_PROXY_CONTROL_URL", "http://gw:8899")
    module = _load_dag_module(monkeypatch)
    calls = _fake_enrichment(module, monkeypatch, tmp_path)

    summary = module._enrich_season_metadata(run_id="scheduled__2026-09-24T00:30:00+00:00")

    assert summary == {
        "campaign_id": "camp-1",
        "ready_wave_scopes": 7,
        "source_requests": 9,
        "paid_proxy_bytes": 123,
    }
    command = calls[0]["command"]
    assert command[1] == module.ENRICH_SCRIPT

    def flag(name):
        return command[command.index(name) + 1]

    assert flag("--snapshot") == module.SNAPSHOT_PATH
    assert flag("--output") == module.SNAPSHOT_PATH
    assert flag("--policy") == module.POLICY_PATH
    assert flag("--select") == "priority"
    assert flag("--max-seasons") == "170"
    assert flag("--expected-snapshot-id") == "snap-1"
    assert flag("--dag-id") == "dag_refresh_sofascore_all_mens"
    # Astra r1 #1: its own gateway run key, so the sweep cannot starve it.
    assert flag("--run-id") == "scheduled__2026-09-24T00:30:00+00:00:metadata"
    assert flag("--task-id") == "enrich_season_metadata"
    assert flag("--budget-cap-bytes") == str(16 * 1024 * 1024)
    report = flag("--report")
    assert report.startswith(str(tmp_path / "metadata-results") + "/")
    assert report.endswith(".json") and ":" not in report.rsplit("/", 1)[1]
    assert calls[0]["env"]["SOFASCORE_PROXY_CONTROL_URL"] == "http://gw:8899"
    assert calls[0]["env"]["PYTHONPATH"] == "/opt/airflow:/opt/airflow/dags"


@pytest.mark.unit
def test_enrich_season_metadata_fails_on_nonzero_exit(
    clean_env, monkeypatch, tmp_path
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module(monkeypatch)
    _fake_enrichment(module, monkeypatch, tmp_path, returncode=1)

    with pytest.raises(AirflowException, match="enrichment failed"):
        module._enrich_season_metadata(run_id="manual__x")


@pytest.mark.unit
def test_enrich_season_metadata_zero_seasons_skips(clean_env, monkeypatch, tmp_path):
    from airflow.exceptions import AirflowSkipException

    monkeypatch.setenv("SOFASCORE_METADATA_SEASONS_PER_RUN", "0")
    module = _load_dag_module(monkeypatch)
    calls = _fake_enrichment(module, monkeypatch, tmp_path)

    with pytest.raises(AirflowSkipException):
        module._enrich_season_metadata(run_id="manual__x")
    assert calls == []


@pytest.mark.unit
def test_validate_refresh_scope_accepts_a_partial_scope(
    clean_env, monkeypatch, tmp_path
):
    # #1358: a scope that stopped at its own ceiling is a finished task, and
    # says what it left for the next run.
    module = _load_dag_module(monkeypatch)
    result = tmp_path / "result.json"
    result.write_text(json.dumps({
        "status": "partial", "snapshot_id": "s", "campaign_id": "c",
        "tournament_id": 8, "source_season_id": 825, "elapsed_s": 1_700.0,
        "remaining_matches": 60, "stop_reason": "time_budget", "bytes": 9_000,
    }))

    assert module._validate_refresh_scope(
        env=_refresh_env(result), execution_timeout=timedelta(minutes=30)
    ) == {
        "status": "partial", "scope_key": "c:8:825", "elapsed_s": 1_700.0,
        "remaining_matches": 60, "stop_reason": "time_budget", "bytes": 9_000,
    }


@pytest.mark.unit
def test_plan_task_maps_each_scope_with_its_own_timeout(clean_env, monkeypatch):
    module = _load_dag_module(monkeypatch)
    monkeypatch.setattr(
        module.state, "read_snapshot", lambda *a, **k: {"campaign_id": "c"}
    )
    monkeypatch.setattr(module, "_pending_refresh_partitions", lambda: [])
    monkeypatch.setattr(
        module.state, "plan_refresh_batch",
        lambda *a, **k: [{"SOFASCORE_SCOPE_TIMEOUT_S": "1030", "X": "1"}],
    )

    monkeypatch.setattr(module.time, "time", lambda: 1_000_000.0)

    items = module._plan_refresh_batch(
        run_id="manual__1",
        dag_run=SimpleNamespace(run_type=DagRunType.MANUAL, conf={}),
    )

    # Every scope, and every retry of it, stops by the batch's own deadline:
    # plan time + the 2 h window + the 2 h retry reserve.
    assert items == [{
        "env": {
            "SOFASCORE_SCOPE_TIMEOUT_S": "1030",
            "X": "1",
            "SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH": str(1_000_000 + 4 * 3600),
        },
        "execution_timeout": timedelta(seconds=1030),
    }]


@pytest.mark.unit
def test_propagate_reports_partial_scopes_and_window_use(clean_env, monkeypatch):
    module = _load_dag_module(monkeypatch)
    dag_run = SimpleNamespace(get_task_instances=lambda: [
        SimpleNamespace(task_id="run_refresh_scope", state="success"),
    ])
    ti = SimpleNamespace(xcom_pull=lambda task_ids: [
        {"status": "refreshed", "scope_key": "c:1:1", "elapsed_s": 1_800.0},
        {"status": "partial", "scope_key": "c:2:2", "elapsed_s": 3_600.0,
         "remaining_matches": 12, "stop_reason": "time_budget"},
    ])

    verdict = module._propagate_status(dag_run=dag_run, ti=ti)

    assert verdict["status"] == "success"
    assert verdict["scopes"] == 2
    assert verdict["partial"] == 1
    assert verdict["partial_scopes"] == [
        {"scope_key": "c:2:2", "stop_reason": "time_budget", "remaining_matches": 12}
    ]
    assert verdict["window_used_s"] == 5_400.0
    assert verdict["window_use"] == 0.75


_BLIP = ('SofaScore endpoint did not reach a publishable state: ["25","97078","event","16395967","event","final"] '
         'status=retryable_failure error=TransportError: warmed SofaScore request failed for '
         '/api/v1/event/16395967: Page.goto: NS_ERROR_PROXY_BAD_GATEWAY')


def _scope_result(tmp_path, *, run_id="r:refill:11--25-97078", errors=(_BLIP,), uncertain=0, schema=0):
    path = tmp_path / "scope.json"
    path.write_text(json.dumps({
        "status": "failed", "run_id": run_id, "errors": [f"matches: {e}" for e in errors],
        "phases": [{"phase": "matches", "status": "failed", "errors": list(errors),
                    "accounting_uncertain": uncertain, "control_channel_failures": 0,
                    "status_counts": {"retryable_failure": 1, "schema_error": schema}}]}))
    return {"SOFASCORE_SCOPE_RESULT_PATH": str(path), "SOFASCORE_SCOPE_RUN_ID": "r:refill:11--25-97078"}


def test_refill_treats_only_a_clean_transport_loss_as_transient(clean_env, monkeypatch, tmp_path):
    module = _load_dag_module(monkeypatch)
    assert module._transient_scope_failure(_scope_result(tmp_path)) is True
    for kwargs in ({"uncertain": 1}, {"schema": 1}, {"run_id": "stale"},
                   {"errors": (_BLIP, "reported traffic exceeds endpoint reservation")},
                   {"errors": ("season_mismatch: 2 rows do not match source season",)}, {"errors": ()}):
        assert module._transient_scope_failure(_scope_result(tmp_path, **kwargs)) is False, kwargs
    assert module._transient_scope_failure(
        {"SOFASCORE_SCOPE_RESULT_PATH": str(tmp_path / "missing.json"), "SOFASCORE_SCOPE_RUN_ID": "x"}) is False


def test_validate_passes_a_transport_loss_on_to_the_window(clean_env, monkeypatch, tmp_path):
    module = _load_dag_module(monkeypatch)
    env = {**_scope_result(tmp_path), "SOFASCORE_CAMPAIGN_ACTION": "refresh", "SOFASCORE_SCOPE_KEY": "c:25:97078"}
    outcome = module._validate_refresh_scope(env=env)
    assert outcome["status"] == "transient_failure"
    assert outcome["scope_key"] == "c:25:97078"
    with pytest.raises(module.AirflowException):
        module._validate_refresh_scope(env={**_scope_result(tmp_path, uncertain=1),
                                            "SOFASCORE_CAMPAIGN_ACTION": "refresh",
                                            "SOFASCORE_SCOPE_KEY": "c:25:97078"})


@pytest.mark.parametrize("losses, red", [(2, False), (3, True)])
def test_propagate_is_red_only_on_a_streak_of_transport_losses(clean_env, monkeypatch, losses, red):
    module = _load_dag_module(monkeypatch)
    dag_run = SimpleNamespace(get_task_instances=lambda: [
        SimpleNamespace(task_id="run_refresh_scope", state="success")])
    outcomes = [{"status": "refreshed", "scope_key": "c:1:1", "elapsed_s": 60.0}] + [
        {"status": "transient_failure", "scope_key": f"c:{n}:{n}", "elapsed_s": 20.0} for n in range(2, 2 + losses)]
    ti = SimpleNamespace(xcom_pull=lambda task_ids: outcomes)
    if red:
        with pytest.raises(module.AirflowException, match="3 transport failures"):
            module._propagate_status(dag_run=dag_run, ti=ti)
    else:
        assert module._propagate_status(dag_run=dag_run, ti=ti)["initial_transient_failures"] == losses


def _run_scope_command(module, tmp_path, *, try_number, result, exit_code=1):
    """Execute the real rendered scope command with a stub scope cycle."""
    import subprocess
    from pathlib import Path

    import jinja2

    root = tmp_path / "airflow"
    if not root.exists():
        (root / "dags").mkdir(parents=True)
        (root / "dags" / "utils").symlink_to(Path(module.__file__).parent / "utils")
    stub = tmp_path / "python"
    result_path = tmp_path / "out" / "result.json"
    result_path.parent.mkdir(exist_ok=True)
    stub.write_text(
        f"#!/bin/sh\nif [ \"$1\" = dags/scripts/run_sofascore_scope_cycle.py ]; then\n"
        f"  cat > {result_path} <<'JSON'\n{json.dumps(result)}\nJSON\n  exit {exit_code}\nfi\n"
        f"exec {sys.executable} \"$@\"\n")
    stub.chmod(0o755)
    command = jinja2.Template(module.RUN_SCOPE_COMMAND).render(
        ti=SimpleNamespace(try_number=try_number, max_tries=1))
    command = command.replace("/opt/legacy-scraper-venv/bin/python", str(stub)).replace(
        "cd /opt/airflow", f"cd {root}")
    env = {"PATH": "/usr/bin:/bin", "SOFASCORE_SCOPE_RESULT_PATH": str(result_path),
           "SOFASCORE_SCOPE_RUN_ID": "r:1"}
    for name in ("SOFASCORE_CAMPAIGN_SNAPSHOT", "SOFASCORE_TOURNAMENT_ID", "SOFASCORE_SOURCE_SEASON_ID",
                 "SOFASCORE_EXPECTED_SNAPSHOT_ID", "SOFASCORE_EXPECTED_CAMPAIGN_ID",
                 "SOFASCORE_SCOPE_OUTPUT_DIR", "SOFASCORE_WORKLOAD_ARTIFACT"):
        env[name] = "x"
    return subprocess.run(["bash", "-c", command], env=env, capture_output=True, text=True).returncode


def test_scope_command_lets_a_transport_loss_through_only_on_the_last_try(clean_env, monkeypatch, tmp_path):
    from datetime import datetime, timezone

    module = _load_dag_module(monkeypatch)
    now = datetime.now(timezone.utc).isoformat()
    blip = {"status": "failed", "run_id": "r:1", "started_at": now, "errors": [f"matches: {_BLIP}"],
            "phases": [{"status": "failed", "errors": [_BLIP], "accounting_uncertain": 0,
                        "control_channel_failures": 0, "status_counts": {"retryable_failure": 1}}]}
    assert _run_scope_command(module, tmp_path, try_number=1, result=blip) == 1
    assert _run_scope_command(module, tmp_path, try_number=2, result=blip) == 0
    doubt = {**blip, "phases": [{**blip["phases"][0], "accounting_uncertain": 1}]}
    assert _run_scope_command(module, tmp_path, try_number=2, result=doubt) == 1
    stale = {**blip, "started_at": "2026-01-01T00:00:00+00:00"}
    assert _run_scope_command(module, tmp_path, try_number=2, result=stale) == 1
    assert _run_scope_command(module, tmp_path, try_number=1, result={"status": "success"}, exit_code=0) == 0
