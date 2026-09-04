"""Topology and safety of the SofaScore players lane (#1244)."""

from __future__ import annotations

import importlib
import json
import re
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


MODULE = "dags.dag_players_sofascore_all_mens"
ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "deploy" / "sofascore" / "airflow.compose.yaml"
PLAYERS_KNOBS = (
    "SOFASCORE_PLAYERS_BATCH_SIZE",
    "SOFASCORE_PLAYERS_POOL",
    "SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS",
    "SOFASCORE_PLAYERS_MAX_SCOPE_ATTEMPTS",
    "SOFASCORE_PLAYERS_STATE",
    "SOFASCORE_PLAYERS_RESULT_DIR",
    "SOFASCORE_PLAYERS_PROXY_CONTROL_URL",
)
# deploy/sofascore/auto_deliver.sh: WINDOW_FROM=0330, WINDOW_TO=0600.
DELIVERY_WINDOW_UTC = (timedelta(hours=3, minutes=30), timedelta(hours=6))


def _load_dag_module():
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
    for name in PLAYERS_KNOBS:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.unit
def test_players_dag_runs_four_times_a_day_with_one_bounded_serial_batch(clean_env):
    module = _load_dag_module()
    dag = module.dag
    operators = _operators()

    assert dag.dag_id == "dag_players_sofascore_all_mens"
    assert dag.schedule == "0 7,12,17,22 * * *"
    assert dag._dag_kwargs["max_active_runs"] == 1
    assert dag._dag_kwargs["catchup"] is False
    assert dag._dag_kwargs["is_paused_upon_creation"] is True
    assert dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=4, minutes=30)
    # No native rendering: it would turn the numeric env strings into ints.
    assert "render_template_as_native_obj" not in dag._dag_kwargs
    assert set(operators) == {
        "plan_players_batch",
        "run_players_scope",
        "validate_players_scope",
        "finalize_players_run",
        "propagate_players_status",
    }
    # Both attempts of a scope have to fit the window, not just the first.
    assert module.PLAYERS_BATCH_FITS == 3
    assert module.PLAYERS_BATCH_SIZE == 3
    assert operators["run_players_scope"]._init_kwargs["retries"] == 1
    assert operators["run_players_scope"]._init_kwargs["execution_timeout"] == timedelta(
        minutes=45
    )


@pytest.mark.unit
def test_the_players_window_never_overlaps_the_delivery_window(clean_env):
    """The nightly delivery recreates the scheduler and kills running tasks.

    Computed from the module's own constants, so moving the schedule or the
    timeout without moving the other one fails here instead of in production.
    """

    module = _load_dag_module()
    hours = [int(part) for part in module.PLAYERS_SCHEDULE.split()[1].split(",")]
    assert hours == list(module.PLAYERS_RUN_HOURS_UTC)
    window_from, window_to = DELIVERY_WINDOW_UTC
    day = timedelta(hours=24)

    for hour in hours:
        start = timedelta(hours=hour)
        end = start + module.PLAYERS_DAGRUN_TIMEOUT
        # A run may cross midnight, so compare the occupied span on both days.
        for offset in (timedelta(0), -day):
            assert not (
                start + offset < window_to and window_from < end + offset
            ), f"run at {hour}:00 UTC can still be alive inside the delivery window"


@pytest.mark.unit
def test_the_lane_never_falls_back_into_the_shared_pool(clean_env, monkeypatch):
    module = _load_dag_module()

    assert module.PLAYERS_POOL == "sofascore_players_pool"
    assert _operators()["run_players_scope"]._init_kwargs["pool"] == "sofascore_players_pool"

    # An empty override is not an invitation to share the daily ingest's pool.
    monkeypatch.setenv("SOFASCORE_PLAYERS_POOL", "   ")
    assert _load_dag_module().PLAYERS_POOL == "sofascore_players_pool"


@pytest.mark.unit
def test_the_weekly_freshness_key_lives_in_the_task_and_not_in_the_scheduler(
    clean_env,
):
    """In the scheduler environment it would freeze the daily profile rotation."""

    module = _load_dag_module()

    assert module.PLAYERS_TASK_ENV["SOFASCORE_PLAYER_FRESHNESS_KEY"] == "final"
    compose = COMPOSE.read_text(encoding="utf-8")
    assert not re.search(
        r"^\s*SOFASCORE_PLAYER_FRESHNESS_KEY\s*:", compose, re.MULTILINE
    )


@pytest.mark.unit
def test_the_unstarted_season_reason_matches_the_runner(clean_env):
    from dags.scripts.run_sofascore_scraper import UNSTARTED_SEASON_REASON

    assert _load_dag_module().UNSTARTED_SEASON_REASON == UNSTARTED_SEASON_REASON


@pytest.mark.unit
def test_plan_task_feeds_both_state_files_and_excludes_registry_leagues(
    clean_env, monkeypatch
):
    module = _load_dag_module()
    seen = {}

    monkeypatch.setattr(
        module.state, "read_snapshot", lambda *a, **k: {"campaign_id": "c-1"}
    )
    monkeypatch.setattr(
        module.state,
        "read_completed",
        lambda path, *, campaign_id: {f"completed::{path}"},
    )
    monkeypatch.setattr(module.state, "read_failures", lambda *a, **k: {})
    monkeypatch.setattr(module, "_configured_tournament_ids", lambda: frozenset({17}))
    monkeypatch.setattr(
        module.state,
        "plan_players_batch",
        lambda snapshot, **kwargs: seen.update(kwargs) or [],
    )

    assert module._plan_players_batch(run_id="scheduled-1") == []
    assert seen["completed"] == {f"completed::{module.HISTORY_STATE_PATH}"}
    assert seen["players_completed"] == {f"completed::{module.STATE_PATH}"}
    assert seen["exclude_tournament_ids"] == frozenset({17})
    assert seen["batch_size"] == module.PLAYERS_BATCH_SIZE
    assert seen["task_env"] is module.PLAYERS_TASK_ENV


def _scope_env(tmp_path, **overrides):
    environment = {
        "SOFASCORE_CAMPAIGN_ACTION": "players",
        "SOFASCORE_SCOPE_KEY": "c-1:17:1725",
        "SOFASCORE_EXPECTED_SNAPSHOT_ID": "a" * 64,
        "SOFASCORE_SCOPE_RESULT_PATH": str(tmp_path / "result.json"),
        "SOFASCORE_SCOPE_OUTPUT_DIR": str(tmp_path / "scope"),
    }
    environment.update(overrides)
    return environment


def _write_scope_result(tmp_path, result, players=None):
    (tmp_path / "result.json").write_text(json.dumps(result), encoding="utf-8")
    if players is not None:
        (tmp_path / "scope").mkdir(parents=True, exist_ok=True)
        (tmp_path / "scope" / "players.json").write_text(
            json.dumps(players), encoding="utf-8"
        )


def _success_result():
    return {
        "status": "success",
        "snapshot_id": "a" * 64,
        "campaign_id": "c-1",
        "tournament_id": 17,
        "source_season_id": 1725,
    }


@pytest.mark.unit
def test_validate_marks_a_captured_scope_complete_and_clears_its_park(
    clean_env, tmp_path, monkeypatch
):
    module = _load_dag_module()
    calls = []
    monkeypatch.setattr(
        module.state, "mark_completed", lambda *a, **k: calls.append(("done", k))
    )
    monkeypatch.setattr(
        module.state, "clear_failed", lambda *a, **k: calls.append(("clear", k))
    )
    _write_scope_result(tmp_path, _success_result(), {"universe_players": 412})

    assert module._validate_players_scope(**_scope_env(tmp_path)) == {
        "status": "complete",
        "scope_key": "c-1:17:1725",
    }
    assert [item[0] for item in calls] == ["done", "clear"]


@pytest.mark.unit
def test_validate_accepts_a_season_that_simply_has_not_started(
    clean_env, tmp_path, monkeypatch
):
    """A top-up scope legitimately writes zero rows, so the threshold is the
    universe; a season with nothing played says so explicitly."""

    module = _load_dag_module()
    monkeypatch.setattr(module.state, "mark_completed", lambda *a, **k: None)
    monkeypatch.setattr(module.state, "clear_failed", lambda *a, **k: None)
    _write_scope_result(
        tmp_path,
        _success_result(),
        {"universe_players": 0, "fallback_reason": module.UNSTARTED_SEASON_REASON},
    )

    assert module._validate_players_scope(**_scope_env(tmp_path))["status"] == (
        "complete"
    )


@pytest.mark.unit
def test_validate_refuses_a_green_but_empty_capture(clean_env, tmp_path, monkeypatch):
    module = _load_dag_module()
    monkeypatch.setattr(
        module.state,
        "mark_completed",
        lambda *a, **k: pytest.fail("an empty universe must not be completed"),
    )
    _write_scope_result(tmp_path, _success_result(), {"universe_players": 0})

    with pytest.raises(Exception, match="committed no player universe"):
        module._validate_players_scope(**_scope_env(tmp_path))


@pytest.mark.unit
def test_validate_parks_a_deferred_scope_without_reddening_the_task(
    clean_env, tmp_path, monkeypatch
):
    module = _load_dag_module()
    parked = {}
    monkeypatch.setattr(
        module.state, "mark_failed", lambda *a, **k: parked.update(k)
    )
    monkeypatch.setattr(
        module.state,
        "mark_completed",
        lambda *a, **k: pytest.fail("a deferred scope is not complete"),
    )
    _write_scope_result(
        tmp_path,
        {
            **_success_result(),
            "status": "deferred",
            "deferral_reason": "PlayerEvidenceNotReady: matches pending",
        },
    )

    outcome = module._validate_players_scope(**_scope_env(tmp_path))

    assert outcome["status"] == "deferred"
    assert parked["scope_key"] == "c-1:17:1725"


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"SOFASCORE_CAMPAIGN_ACTION": "capture"},
        {"SOFASCORE_SCOPE_KEY": "c-1:8:825"},
        {"SOFASCORE_EXPECTED_SNAPSHOT_ID": "b" * 64},
    ],
)
def test_validate_fails_closed_on_foreign_provenance(
    clean_env, tmp_path, monkeypatch, overrides
):
    module = _load_dag_module()
    monkeypatch.setattr(
        module.state,
        "mark_completed",
        lambda *a, **k: pytest.fail("provenance must be checked first"),
    )
    _write_scope_result(tmp_path, _success_result(), {"universe_players": 5})

    with pytest.raises(Exception):
        module._validate_players_scope(**_scope_env(tmp_path, **overrides))


@pytest.mark.unit
def test_finalize_parks_a_scope_whose_validation_failed_after_a_paid_capture(
    clean_env, monkeypatch
):
    """Lesson #1216: looking only at the bash task left such a scope neither
    completed nor failed — it came back first next run and was paid for again."""

    module = _load_dag_module()
    parked = []
    monkeypatch.setattr(
        module.state, "mark_failed", lambda *a, **k: parked.append(k["scope_key"])
    )
    planned = [
        {"SOFASCORE_SCOPE_KEY": "c-1:17:1725", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1"},
        {"SOFASCORE_SCOPE_KEY": "c-1:8:825", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1"},
    ]
    states = {
        ("run_players_scope", 0): "success",
        ("validate_players_scope", 0): "failed",
        ("run_players_scope", 1): "success",
        ("validate_players_scope", 1): "success",
    }
    dag_run = SimpleNamespace(
        get_task_instance=lambda task_id, map_index: SimpleNamespace(
            state=states[(task_id, map_index)]
        )
    )
    ti = SimpleNamespace(xcom_pull=lambda task_ids: planned)

    outcome = module._finalize_players_run(ti=ti, dag_run=dag_run, run_id="run-1")

    assert parked == ["c-1:17:1725"]
    assert outcome == {"planned": 2, "failed": 1}


@pytest.mark.unit
def test_the_scope_command_runs_the_players_phase_only(clean_env):
    module = _load_dag_module()

    assert "--phase players" in module.RUN_SCOPE_COMMAND
    assert "--allow-pending-season" not in module.RUN_SCOPE_COMMAND
    assert "--season-evidence" not in module.RUN_SCOPE_COMMAND


@pytest.mark.unit
def test_both_airflowignores_agree_on_who_may_parse_the_lane(clean_env):
    shared = (ROOT / "dags" / ".airflowignore").read_text(encoding="utf-8")
    contour = (
        ROOT / "deploy" / "sofascore" / ".airflowignore"
    ).read_text(encoding="utf-8")
    name = "dag_players_sofascore_all_mens.py"

    # Hidden from the shared platform scheduler...
    assert any(
        not line.startswith("#") and re.search(line.strip(), name)
        for line in shared.splitlines()
        if line.strip()
    )
    # ...and parsed by the isolated contour, whose block-list must not match it.
    assert not any(
        not line.startswith("#") and re.search(line.strip(), name)
        for line in contour.splitlines()
        if line.strip()
    )
