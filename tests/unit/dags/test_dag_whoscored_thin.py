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
    # #1474: two runs a day, set in the DAG itself (utils/config.py is locked).
    assert ingest.dag.schedule == "0 10,22 * * *"
    assert ingest.dag._dag_kwargs["max_active_runs"] == 1
    assert ingest.dag._dag_kwargs["catchup"] is False
    assert ingest.dag._dag_kwargs["default_args"] is SCRAPER_ARGS


def test_ingest_tasks_are_direct_pool_native(ingest):
    discover = _bash("discover_catalog")
    daily = _bash("ingest_matches")
    assert "run_whoscored_scraper.py discover" in discover._init_kwargs["bash_command"]
    # #1474: the catalog is refreshed once a week; the runner short-circuits.
    assert "--weekly-gate" in discover._init_kwargs["bash_command"]
    cmd = daily._init_kwargs["bash_command"]
    assert "run_whoscored_scraper.py daily" in cmd
    assert "--skip-profiles" in cmd
    assert "--transport-policy direct_only" in cmd
    assert "--daily-part" not in cmd
    assert "--weekly-gate" not in cmd
    # No ceremony flags survive on the daily path.
    for banned in ("--proxy-approval", "direct_then_paid", "gateway", "--catalog-batch-id"):
        assert banned not in cmd
    # The residential pool reaches the scraper through the environment.
    assert "WHOSCORED_PROXY_FILE" in daily._init_kwargs["env"]
    assert daily._init_kwargs["append_env"] is True


def test_ingest_matches_has_eight_hour_timeout(ingest):
    from datetime import timedelta

    daily = _bash("ingest_matches")
    assert daily._init_kwargs["execution_timeout"] == timedelta(hours=8)


def test_ingest_stages_is_a_separate_weekly_task(ingest):
    # #1474: stage feeds live in their own task; its failure never fails
    # ingest_matches, and it runs whatever ingest_matches ended with.
    from datetime import timedelta

    stages = _bash("ingest_stages")
    matches = _bash("ingest_matches")
    cmd = stages._init_kwargs["bash_command"]
    assert "run_whoscored_scraper.py daily" in cmd
    assert "--daily-part stages" in cmd
    assert "--weekly-gate" in cmd
    assert "--skip-profiles" in cmd
    assert "--transport-policy direct_only" in cmd
    # No report gate downstream: a failed stage scope must turn the task red.
    assert "|| [ -s" not in cmd
    assert ingest.STAGES_RESULT_PATH in cmd
    assert ingest.RESULT_PATH not in cmd
    assert stages._init_kwargs["trigger_rule"] == "all_done"
    assert stages._init_kwargs["execution_timeout"] == timedelta(hours=8)
    assert "WHOSCORED_PROXY_FILE" in stages._init_kwargs["env"]
    assert stages.upstream_task_ids == {"ingest_matches"}
    assert stages.downstream_task_ids == set()
    assert matches.downstream_task_ids == {
        "ingest_stages",
        "validate_data",
        "validate_bronze_freshness",
    }


@pytest.mark.parametrize("module_name", ["dag_ingest_whoscored", "dag_backfill_whoscored"])
def test_proxy_file_has_no_default(monkeypatch, module_name):
    # #1471: no silent default pool — an unset pool must reach the runner empty.
    monkeypatch.delenv("WHOSCORED_PROXY_FILE", raising=False)
    module = _reload(module_name)
    assert module._TASK_ENV["WHOSCORED_PROXY_FILE"] == ""

    monkeypatch.setenv("WHOSCORED_PROXY_FILE", "/opt/airflow/proxys.txt")
    module = _reload(module_name)
    assert module._TASK_ENV["WHOSCORED_PROXY_FILE"] == "/opt/airflow/proxys.txt"


@pytest.mark.parametrize(
    ("module_name", "task_ids"),
    [
        ("dag_ingest_whoscored", ("discover_catalog", "ingest_matches", "ingest_stages")),
        ("dag_backfill_whoscored", ("run_backfill_chunk",)),
    ],
)
def test_runtime_root_drives_cd_and_pythonpath(monkeypatch, module_name, task_ids):
    monkeypatch.delenv("WHOSCORED_RUNTIME_ROOT", raising=False)
    module = _reload(module_name)
    assert module._TASK_ENV["PYTHONPATH"] == "/opt/airflow:/opt/airflow/dags"
    for task_id in task_ids:
        assert _bash(task_id)._init_kwargs["bash_command"].startswith(
            "cd /opt/airflow && "
        )

    monkeypatch.setenv("WHOSCORED_RUNTIME_ROOT", "/opt/whoscored-src")
    module = _reload(module_name)
    # Order is load-bearing: runtime_contract wants (root, root/dags).
    assert (
        module._TASK_ENV["PYTHONPATH"]
        == "/opt/whoscored-src:/opt/whoscored-src/dags"
    )
    for task_id in task_ids:
        assert _bash(task_id)._init_kwargs["bash_command"].startswith(
            "cd /opt/whoscored-src && "
        )


def test_ingest_has_validation_and_freshness(ingest):
    _python("validate_data")
    freshness = _python("validate_bronze_freshness")
    assert freshness._init_kwargs["trigger_rule"] == "all_done"


# ----------------------------- backfill ----------------------------------

def test_backfill_dag_is_manual_and_paused(backfill):
    from utils.default_args import SCRAPER_ARGS

    assert backfill.dag.dag_id == "dag_backfill_whoscored"
    # History stays manual until #1480.
    assert backfill.dag.schedule is None
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


# ------------------------ error budget (#1053) ----------------------------

def _budget_context(tmp_path, report):
    import json

    path = tmp_path / "result.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return {"templates_dict": {"result_path": str(path)}}


def test_ingest_matches_tolerates_runner_rc_when_report_exists(ingest):
    # Красный rc раннера при живом отчёте — норма (#1053): судит бюджет.
    cmd = _bash("ingest_matches")._init_kwargs["bash_command"]
    assert "|| [ -s" in cmd


def test_freshness_is_not_the_sole_leaf(ingest):
    # #1053: единственный all_done-лист красил ран зелёным при упавшем сборе.
    # Теперь гейт качества — самостоятельный лист, а freshness висит на
    # ingest_matches параллельной веткой.
    validate = _python("validate_data")
    freshness = _python("validate_bronze_freshness")
    assert freshness._init_kwargs["trigger_rule"] == "all_done"
    assert validate.downstream_task_ids == set()
    assert freshness.downstream_task_ids == set()
    assert freshness.upstream_task_ids == {"ingest_matches"}
    assert validate.upstream_task_ids == {"ingest_matches"}


# ----------------------- honest colour (#1476) ------------------------------

DENOMINATOR = ("ENG-Premier League=2627", "WS-182-77=2627")
PROBE = "WS-206-63=2526"


def _scope(spec, status="success", is_active=None):
    league, season = spec.split("=")
    scope = {"scope": spec, "competition_id": league, "season_id": season,
             "status": status}
    if is_active is not None:
        scope["is_active"] = is_active
    return scope


def _report(statuses=None, *, probe_status="success", successes=5, status="success",
            **extra):
    statuses = statuses or {}
    report = {
        "schema_version": 3,
        "status": status,
        "rows": 10,
        "scopes": [_scope(spec, statuses.get(spec, "success")) for spec in DENOMINATOR]
        + [_scope(PROBE, probe_status)],
        "traffic": {"route_successes": {"direct_http": successes} if successes else {}},
        "errors": [],
    }
    report.update(extra)
    return report


@pytest.fixture
def trino(ingest, monkeypatch):
    """Fake Trino: no overdue games, every schedule refreshed an hour ago."""
    from datetime import timedelta

    state = {"overdue": [], "schedule_age_h": {}, "fail": None, "sql": []}

    def query(sql):
        state["sql"].append(sql)
        if state["fail"]:
            raise state["fail"]
        now = ingest._utc_now()
        if "whoscored_schedule\nWHERE (league, season) IN" in sql:
            rows = []
            for spec in DENOMINATOR:
                league, season = spec.split("=")
                age = state["schedule_age_h"].get(spec, 1)
                if age is not None:
                    rows.append((league, season, now - timedelta(hours=age)))
            return rows
        if "AS table_name" in sql:
            return [("t", now - timedelta(hours=state.get("content_age_h", 1)))]
        if "WHERE collected_at IS NULL AND NOT ceiling" in sql:
            return state["overdue"]
        raise AssertionError(sql)

    monkeypatch.setattr(ingest, "_trino_query", query)
    return state


def test_validate_data_green_when_no_rule_fires(ingest, tmp_path, trino):
    ingest.validate_data(**_budget_context(tmp_path, _report()))
    # The reference SQL and the schedule gate both ran.
    assert any("NOT ceiling" in sql for sql in trino["sql"])
    assert any("MAX(_ingested_at)" in sql for sql in trino["sql"])


@pytest.mark.parametrize("status", ["failed", "retryable", "pending", "running"])
def test_validate_data_rule_a_any_denominator_scope_not_success(
    ingest, tmp_path, trino, status
):
    from airflow.exceptions import AirflowException

    report = _report({"WS-182-77=2627": status})
    with pytest.raises(AirflowException, match=rf"\(a\) 1/2 .*WS-182-77=2627:{status}"):
        ingest.validate_data(**_budget_context(tmp_path, report))


def test_validate_data_rule_b_probe_failures_beyond_budget(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    report = _report(probe_status="failed")
    with pytest.raises(AirflowException, match=r"\(b\) 1/1 probe") as caught:
        ingest.validate_data(**_budget_context(tmp_path, report))
    assert "(a)" not in str(caught.value)


def test_validate_data_rule_c_zero_network_successes(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match=r"\(c\) zero successful"):
        ingest.validate_data(**_budget_context(tmp_path, _report(successes=0)))


def test_validate_data_rule_c_source_unavailable(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    report = _report(
        {"ENG-Premier League=2627": "source_unavailable", "WS-182-77=2627": "pending"},
        status="source_unavailable",
        source_unavailable={"scope": "ENG-Premier League=2627", "message": "407"},
    )
    with pytest.raises(AirflowException) as caught:
        ingest.validate_data(**_budget_context(tmp_path, report))
    message = str(caught.value)
    assert "(c) source unavailable" in message
    # Every rule that fired is named, not just the first.
    assert "(a) 2/2" in message


def test_validate_data_rule_d_overdue_game(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    trino["overdue"] = [("WS-182-77", "2627", 1990001, "A-B", "2026-09-28 16:00")]
    with pytest.raises(AirflowException, match=r"\(d\) 1 denominator game.*WS-182-77=2627#1990001"):
        ingest.validate_data(**_budget_context(tmp_path, _report()))


def test_validate_data_rule_d_query_failure_is_red(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    trino["fail"] = RuntimeError("trino down")
    with pytest.raises(AirflowException, match=r"\(d\) overdue query failed"):
        ingest.validate_data(**_budget_context(tmp_path, _report()))


@pytest.mark.parametrize(
    ("age", "text"),
    [
        (49, r"\(e\) 1 active denominator schedule\(s\) older than 48 h: WS-182-77=2627 \(49h\)"),
        (None, r"\(e\) 1 denominator schedule\(s\) absent: WS-182-77=2627"),
    ],
)
def test_validate_data_rule_e_stale_schedule(ingest, tmp_path, trino, age, text):
    from airflow.exceptions import AirflowException

    trino["schedule_age_h"] = {"WS-182-77=2627": age}
    with pytest.raises(AirflowException, match=text):
        ingest.validate_data(**_budget_context(tmp_path, _report()))


def _activity_report(is_active):
    report = _report()
    report["scopes"] = [
        _scope(spec, is_active=is_active) for spec in DENOMINATOR
    ] + [_scope(PROBE)]
    return report


def test_validate_data_rule_e_finished_season_old_schedule_is_green(ingest, tmp_path, trino):
    # #1601: a finished season is not re-read daily; its age is not a signal.
    trino["schedule_age_h"] = {"WS-182-77=2627": 56}
    ingest.validate_data(**_budget_context(tmp_path, _activity_report(False)))


def test_validate_data_rule_e_finished_season_without_schedule_is_red(ingest, tmp_path, trino):
    from airflow.exceptions import AirflowException

    trino["schedule_age_h"] = {"WS-182-77=2627": None}
    with pytest.raises(AirflowException, match=r"\(e\) 1 denominator schedule\(s\) absent"):
        ingest.validate_data(**_budget_context(tmp_path, _activity_report(False)))


@pytest.mark.parametrize("is_active", [True, None])
def test_validate_data_rule_e_active_or_unknown_keeps_48h(ingest, tmp_path, trino, is_active):
    # ``None`` = report without the field (older format): strict, not relaxed.
    from airflow.exceptions import AirflowException

    trino["schedule_age_h"] = {"WS-182-77=2627": 56}
    with pytest.raises(AirflowException, match=r"older than 48 h: WS-182-77=2627 \(56h\)"):
        ingest.validate_data(**_budget_context(tmp_path, _activity_report(is_active)))


def test_validate_data_ignores_old_protected_prefix_and_row_rules(ingest, tmp_path, trino):
    # Probe scopes and rows are not colour signals any more; rows=0 with all
    # denominator scopes successful and a fresh schedule is green.
    report = _report(rows=0)
    ingest.validate_data(**_budget_context(tmp_path, report))
    assert not hasattr(ingest, "WHOSCORED_PROTECTED_SCOPE_PREFIXES")


def test_freshness_is_error_per_denominator_partition(ingest, tmp_path, trino, monkeypatch):
    from airflow.exceptions import AirflowException
    import utils.alerts as alerts

    sent = []
    monkeypatch.setattr(alerts, "telegram_dq_summary", lambda report, header: sent.append(report))
    context = _budget_context(tmp_path, _report())

    ingest.validate_bronze_freshness(**context)
    assert sent[-1].errors == [] and len(sent[-1].results) == 3

    trino["schedule_age_h"] = {"ENG-Premier League=2627": 50}
    with pytest.raises(AirflowException, match="ENG-Premier League=2627"):
        ingest.validate_bronze_freshness(**context)
    assert all(result.severity == "ERROR" for result in sent[-1].results)

    trino["schedule_age_h"] = {}
    trino["content_age_h"] = 49
    with pytest.raises(AirflowException, match="whoscored_events over denominator"):
        ingest.validate_bronze_freshness(**context)


def test_freshness_schedule_finished_season_needs_rows_not_age(
    ingest, tmp_path, trino, monkeypatch
):
    # #1601: same rule as (e) - finished season old rows pass, absent fails.
    from airflow.exceptions import AirflowException
    import utils.alerts as alerts

    sent = []
    monkeypatch.setattr(alerts, "telegram_dq_summary", lambda report, header: sent.append(report))
    context = _budget_context(tmp_path, _activity_report(False))

    trino["schedule_age_h"] = {"ENG-Premier League=2627": 56}
    ingest.validate_bronze_freshness(**context)
    assert sent[-1].errors == []

    trino["schedule_age_h"] = {"ENG-Premier League=2627": None}
    with pytest.raises(AirflowException, match=r"1/2 stale: 1 denominator schedule\(s\) absent"):
        ingest.validate_bronze_freshness(**context)

    context = _budget_context(tmp_path, _report())
    trino["schedule_age_h"] = {"ENG-Premier League=2627": 56}
    with pytest.raises(AirflowException, match=r"older than 48 h: ENG-Premier League=2627 \(56h\)"):
        ingest.validate_bronze_freshness(**context)


def test_freshness_without_report_is_error(ingest, tmp_path, trino, monkeypatch):
    from airflow.exceptions import AirflowException
    import utils.alerts as alerts

    monkeypatch.setattr(alerts, "telegram_dq_summary", lambda report, header: None)
    context = {"templates_dict": {"result_path": str(tmp_path / "missing.json")}}
    with pytest.raises(AirflowException, match="freshness\\[denominator\\]"):
        ingest.validate_bronze_freshness(**context)
    assert trino["sql"] == []
