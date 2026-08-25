from __future__ import annotations

import importlib
import sys
from datetime import timedelta
from types import SimpleNamespace

import pytest


MODULE = "dags.dag_refresh_sofascore_all_mens"
REFRESH_KNOBS = (
    "SOFASCORE_REFRESH_BATCH_SIZE",
    "SOFASCORE_REFRESH_POOL",
    "SOFASCORE_REFRESH_MAX_ACTIVE_TASKS",
    "SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES",
    "SOFASCORE_REFRESH_RESULT_DIR",
    "SOFASCORE_REFRESH_PROXY_CONTROL_URL",
)


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
    assert dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=5)
    assert dag._dag_kwargs["is_paused_upon_creation"] is True
    assert dag._dag_kwargs["default_args"] is module.DEFAULT_ARGS
    assert set(operators) == {
        "fetch_daily_events",
        "plan_refresh_batch",
        "run_refresh_scope",
        "validate_refresh_scope",
        "propagate_refresh_status",
    }

    fetch = operators["fetch_daily_events"]
    assert "run_sofascore_daily_events.py" in fetch.bash_command
    assert '--budget-cap-bytes "${SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES}"' in (
        fetch.bash_command
    )
    assert '--dag-id "${AIRFLOW_CTX_DAG_ID}"' in fetch.bash_command
    assert '--run-id "${AIRFLOW_CTX_DAG_RUN_ID}"' in fetch.bash_command
    assert fetch.env["SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES"] == str(16 * 1024 * 1024)
    assert fetch.env["SOFASCORE_CAMPAIGN_SNAPSHOT"] == module.SNAPSHOT_PATH
    assert fetch.env["SOFASCORE_REFRESH_RESULT_DIR"] == (
        "/opt/airflow/runtime/sofascore/all-men/refresh-results"
    )
    assert fetch.append_env is True
    assert fetch._init_kwargs["pool"] == "ingest_scraper_pool"

    run = operators["run_refresh_scope"]
    assert run.is_mapped
    assert run._expand_kwargs["env"].operator.task_id == "plan_refresh_batch"
    assert run._init_kwargs["pool"] == "ingest_scraper_pool"
    assert run._init_kwargs["priority_weight"] == 5
    assert run._init_kwargs["retries"] == 1
    assert run._init_kwargs["retry_delay"] >= timedelta(seconds=60)
    assert run._init_kwargs["execution_timeout"] == timedelta(hours=2)
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
        "fetch_daily_events"
    }
    assert operators["run_refresh_scope"].upstream_task_ids == {
        "plan_refresh_batch"
    }
    assert operators["validate_refresh_scope"].upstream_task_ids == {
        "run_refresh_scope"
    }
    assert propagate.upstream_task_ids == {"validate_refresh_scope"}


@pytest.mark.unit
def test_refresh_lane_knobs_come_from_env(clean_env, monkeypatch):
    monkeypatch.setenv("SOFASCORE_REFRESH_BATCH_SIZE", "3")
    monkeypatch.setenv("SOFASCORE_REFRESH_POOL", "sofascore_refresh_pool")
    monkeypatch.setenv("SOFASCORE_REFRESH_MAX_ACTIVE_TASKS", "2")
    monkeypatch.setenv("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "4194304")
    monkeypatch.setenv("SOFASCORE_REFRESH_RESULT_DIR", "/tmp/refresh")
    monkeypatch.setenv(
        "SOFASCORE_REFRESH_PROXY_CONTROL_URL", "http://sofascore-gw-refresh:8080"
    )
    module = _load_dag_module(monkeypatch)
    operators = _operators()

    assert module.dag._dag_kwargs["max_active_tasks"] == 2
    fetch = operators["fetch_daily_events"]
    assert fetch._init_kwargs["pool"] == "sofascore_refresh_pool"
    assert fetch.env["SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES"] == "4194304"
    assert fetch.env["SOFASCORE_REFRESH_RESULT_DIR"] == "/tmp/refresh"
    assert fetch.env["SOFASCORE_PROXY_CONTROL_URL"] == (
        "http://sofascore-gw-refresh:8080"
    )
    run = operators["run_refresh_scope"]
    assert run._init_kwargs["pool"] == "sofascore_refresh_pool"
    assert run._init_kwargs["max_active_tis_per_dag"] == 2
    kwargs = _planner_kwargs(module, monkeypatch)
    assert kwargs["batch_size"] == 3
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

    for name, value in _operators()["fetch_daily_events"].env.items():
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
        ("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "0"),
    ],
)
def test_invalid_refresh_lane_knob_fails_dag_parse(
    clean_env, monkeypatch, name, value
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        _load_dag_module(monkeypatch)


def _planner_kwargs(module, monkeypatch):
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
        lambda: [("SS-17", "2627", 4)],
    )
    monkeypatch.setattr(
        module, "_configured_tournament_ids", lambda: frozenset({17, 8})
    )
    monkeypatch.setattr(module.state, "plan_refresh_batch", _plan)
    assert module._plan_refresh_batch(run_id="manual__1") == []
    return captured


@pytest.mark.unit
def test_plan_task_feeds_bronze_partitions_and_configured_exclusions(
    clean_env, monkeypatch
):
    module = _load_dag_module(monkeypatch)

    kwargs = _planner_kwargs(module, monkeypatch)

    assert kwargs["snapshot"] == {"campaign_id": "c"}
    assert kwargs["pending_partitions"] == [("SS-17", "2627", 4)]
    assert kwargs["exclude_tournament_ids"] == frozenset({17, 8})
    assert kwargs["batch_size"] == 8
    assert kwargs["dag_run_id"] == "manual__1"
    assert kwargs["snapshot_path"] == module.SNAPSHOT_PATH
    assert kwargs["workload_artifact"] == module.WORKLOAD_ARTIFACT
    assert kwargs["task_env"] == {}


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
            return [("SS-17", "2627", 12), ("SS-8", 2026, 3)]

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

    assert partitions == [("SS-17", "2627", 12), ("SS-8", "2026", 3)]
    assert connection.closed is True
    sql = executed[0]
    assert "iceberg.bronze.sofascore_schedule" in sql
    assert "iceberg.bronze.sofascore_match_capture_status" in sql
    assert "LIKE 'SS-%'" in sql
    assert "status_type = 'finished'" in sql
    assert "capture_complete" in sql


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

    assert module._validate_refresh_scope(**_refresh_env(result)) == {
        "status": "refreshed", "scope_key": "c:8:825"
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("document", "message"),
    [
        ('{"status": "failed"}', "did not finish successfully"),
        (
            '{"status": "success", "snapshot_id": "other", "campaign_id": "c",'
            ' "tournament_id": 8, "source_season_id": 825}',
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
        module._validate_refresh_scope(**_refresh_env(result))


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
        module._validate_refresh_scope(**environment)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("states", "failed"),
    [
        ({"fetch_daily_events": "success", "run_refresh_scope": "success"}, []),
        (
            {"fetch_daily_events": "failed", "plan_refresh_batch": "upstream_failed"},
            ["fetch_daily_events", "plan_refresh_batch"],
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
        assert module._propagate_status(dag_run=dag_run) == {"status": "success"}
