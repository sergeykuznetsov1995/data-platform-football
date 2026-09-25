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
    assert module.REFRESH_BATCH_SIZE == 64
    assert module.REFRESH_SECONDS_PER_MATCH == 25

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
        "validate_refresh_scope"
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
    assert kwargs["seconds_per_match"] == 25
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
    monkeypatch.setattr(
        module, "_configured_tournament_ids", lambda: frozenset({17, 8})
    )
    monkeypatch.setattr(module.state, "plan_refresh_batch", _plan)
    assert module._plan_refresh_batch(run_id="manual__1", **context) == []
    return captured


@pytest.mark.unit
def test_plan_task_feeds_bronze_partitions_and_configured_exclusions(
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
    assert kwargs["exclude_tournament_ids"] == frozenset({17, 8})
    # #1358: the batch size (64) is only the upper bound on the number of
    # scopes; the planner fills the 2 h scope window by their estimates.
    assert kwargs["batch_size"] == 64
    assert kwargs["scope_budget_s"] == 7200
    assert kwargs["dag_run_id"] == "manual__1"
    assert kwargs["snapshot_path"] == module.SNAPSHOT_PATH
    assert kwargs["workload_artifact"] == module.WORKLOAD_ARTIFACT
    assert kwargs["task_env"] == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("run_type", "interval_end", "conf", "expected_mode"),
    [
        ("scheduled", datetime(2026, 8, 27, 0, 30, tzinfo=timezone.utc), {}, "fresh"),
        ("scheduled", datetime(2026, 8, 27, 8, 30, tzinfo=timezone.utc), {}, "fresh"),
        ("scheduled", datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc), {}, "backlog"),
        (DagRunType.SCHEDULED, datetime(2026, 8, 27, 0, 30, tzinfo=timezone.utc), {}, "fresh"),
        (DagRunType.BACKFILL, datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc), {}, "backlog"),
        ("backfill", datetime(2026, 8, 27, 2, 30, tzinfo=timezone(timedelta(hours=2))), {}, "fresh"),
        ("scheduled", datetime(2026, 8, 27, 17, 30, tzinfo=timezone(timedelta(hours=2))), {}, "backlog"),
        ("scheduled", datetime(2026, 8, 27, 0, 30, tzinfo=timezone.utc), {"queue_mode": "backlog"}, "fresh"),
        ("backfill", datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc), {"queue_mode": "fresh"}, "backlog"),
    ],
)
def test_plan_task_derives_ffb_mode_from_interval_end_not_delayed_task_start(
    clean_env, monkeypatch, run_type, interval_end, conf, expected_mode
):
    module = _load_dag_module(monkeypatch)
    delayed_start = datetime(2026, 8, 27, 20, 4, tzinfo=timezone.utc)

    kwargs = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=run_type, conf=conf),
        data_interval_end=interval_end,
        ti=SimpleNamespace(start_date=delayed_start),
    )

    assert kwargs["queue_mode"] == expected_mode


@pytest.mark.unit
def test_plan_task_manual_defaults_to_fresh_and_allows_backlog_override(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)

    default = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(run_type=DagRunType.MANUAL, conf={}),
        data_interval_end=datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc),
    )
    overridden = _planner_kwargs(
        module,
        monkeypatch,
        dag_run=SimpleNamespace(
            run_type=DagRunType.MANUAL, conf={"queue_mode": "backlog"}
        ),
        data_interval_end=datetime(2026, 8, 27, 15, 30, tzinfo=timezone.utc),
    )

    assert default["queue_mode"] == "fresh"
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
            SimpleNamespace(run_type="scheduled", conf={}),
            datetime(2026, 8, 27, 11, 30, tzinfo=timezone.utc),
            "data_interval_end",
        ),
        (SimpleNamespace(run_type="scheduled", conf={}), None, "data_interval_end"),
        (SimpleNamespace(run_type="scheduled", conf={}), datetime(2026, 8, 27, 0, 30), "data_interval_end"),
        (SimpleNamespace(run_type="unexpected", conf={}), None, "run_type"),
        (SimpleNamespace(conf={}), None, "run_type"),
    ],
)
def test_plan_task_fails_closed_for_invalid_mode_or_non_ffb_interval(
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
                ("SS-17", "2627", 12, 1_787_788_800),
                ("SS-8", 2026, 3, None),
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
        ("SS-17", "2627", 12, 1_787_788_800),
        ("SS-8", "2026", 3, None),
    ]
    assert connection.closed is True
    sql = executed[0]
    assert "iceberg.bronze.sofascore_schedule" in sql
    assert "iceberg.bronze.sofascore_match_capture_status" in sql
    assert "LIKE 'SS-%'" in sql
    assert "status_type = 'finished'" in sql
    assert "capture_complete" in sql
    normalized_sql = " ".join(sql.upper().split())
    assert (
        "MAX(CASE WHEN TRY_CAST(S.START_TIMESTAMP AS BIGINT) BETWEEN 1 AND "
        "CAST(TO_UNIXTIME(CURRENT_TIMESTAMP + INTERVAL '6' HOUR) AS BIGINT) "
        "THEN TRY_CAST(S.START_TIMESTAMP AS BIGINT) END) "
        "AS NEWEST_PENDING_START_TIMESTAMP"
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
    monkeypatch.setattr(module, "_configured_tournament_ids", lambda: frozenset())
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
