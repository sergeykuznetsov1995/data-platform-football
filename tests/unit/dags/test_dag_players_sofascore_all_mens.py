"""Topology and safety of the SofaScore players lane (#1374)."""

from __future__ import annotations

import importlib
import json
import re
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml


MODULE = "dags.dag_players_sofascore_all_mens"
ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "deploy" / "sofascore" / "airflow.compose.yaml"
SCOPE_CYCLE = ROOT / "dags" / "scripts" / "run_sofascore_scope_cycle.py"
PLAYERS_KNOBS = (
    "SOFASCORE_PLAYERS_BATCH_SIZE",
    "SOFASCORE_PLAYERS_POOL",
    "SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS",
    "SOFASCORE_PLAYERS_MAX_SCOPE_ATTEMPTS",
    "SOFASCORE_PLAYERS_STATE",
    "SOFASCORE_PLAYERS_RESULT_DIR",
    "SOFASCORE_PLAYERS_PROXY_CONTROL_URL",
)


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
    assert dag._dag_kwargs["max_active_tasks"] == 1
    assert dag._dag_kwargs["catchup"] is False
    assert dag._dag_kwargs["is_paused_upon_creation"] is True
    assert dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=4, minutes=30)
    assert dag._dag_kwargs["default_args"] is module.DEFAULT_ARGS
    # No native rendering: it would turn the numeric env strings into ints.
    assert "render_template_as_native_obj" not in dag._dag_kwargs
    assert set(operators) == {
        "plan_players_batch",
        "run_players_scope",
        "validate_players_scope",
        "finalize_players_run",
        "propagate_players_status",
    }

    run = operators["run_players_scope"]
    assert run.is_mapped
    assert run._expand_kwargs["env"].operator.task_id == "plan_players_batch"
    # Both attempts of a scope have to fit the window, not just the first.
    assert module.PLAYERS_BATCH_FITS == 3
    assert module.PLAYERS_BATCH_SIZE == 3
    assert run._init_kwargs["retries"] == 1
    assert run._init_kwargs["retry_delay"] == timedelta(minutes=2)
    assert run._init_kwargs["execution_timeout"] == timedelta(minutes=40)
    assert run._init_kwargs["priority_weight"] == 1
    assert run._init_kwargs["do_xcom_push"] is False
    assert run._init_kwargs["max_active_tis_per_dag"] == 1
    assert run.append_env is True

    validate = operators["validate_players_scope"]
    assert validate.is_mapped
    assert validate._expand_kwargs["op_kwargs"].operator.task_id == (
        "plan_players_batch"
    )
    assert operators["plan_players_batch"]._init_kwargs["retries"] == 0


@pytest.mark.unit
def test_players_dag_keeps_one_serial_lane_shape(clean_env):
    _load_dag_module()
    operators = _operators()

    leaves = [
        task_id for task_id, item in operators.items()
        if not item.downstream_task_ids
    ]
    assert leaves == ["propagate_players_status"]
    assert operators["run_players_scope"].upstream_task_ids == {
        "plan_players_batch"
    }
    assert operators["validate_players_scope"].upstream_task_ids == {
        "run_players_scope"
    }
    assert operators["finalize_players_run"].upstream_task_ids == {
        "plan_players_batch", "validate_players_scope",
    }
    assert operators["propagate_players_status"].upstream_task_ids == {
        "finalize_players_run"
    }


@pytest.mark.unit
def test_the_worst_case_batch_leaves_the_window_room_to_book_the_work(clean_env):
    """Capture that succeeded must still be booked before the window closes.

    The batch runs serially on one pool slot and every scope may be attempted
    twice, so the worst case is ``batch x timeout x attempts``.  If that eats
    the whole DagRun, ``validate_players_scope`` and ``finalize_players_run``
    are killed by ``dagrun_timeout`` and a scope whose capture already went
    through is never written to ``players-state.json`` — the lane pays the
    source for it a second time next run.
    """

    module = _load_dag_module()
    worst_case = (
        module.PLAYERS_BATCH_SIZE
        * module.PLAYERS_SCOPE_TIMEOUT
        * module.PLAYERS_SCOPE_ATTEMPTS
    )
    slack = module.PLAYERS_DAGRUN_TIMEOUT - worst_case
    assert slack >= module.PLAYERS_BOOKING_RESERVE, (
        f"worst case {worst_case} leaves only {slack} of the "
        f"{module.PLAYERS_DAGRUN_TIMEOUT} window for validate and finalize"
    )
    # The slack must not have been bought by shrinking the batch.
    assert module.PLAYERS_BATCH_FITS == 3
    assert module.PLAYERS_BATCH_SIZE == 3


@pytest.mark.unit
def test_the_lane_never_falls_back_into_the_shared_pool(clean_env, monkeypatch):
    module = _load_dag_module()

    assert module.PLAYERS_POOL == "sofascore_players_pool"
    assert (
        _operators()["run_players_scope"]._init_kwargs["pool"]
        == "sofascore_players_pool"
    )

    # An empty override is not an invitation to share the daily ingest's pool.
    monkeypatch.setenv("SOFASCORE_PLAYERS_POOL", "   ")
    assert _load_dag_module().PLAYERS_POOL == "sofascore_players_pool"


@pytest.mark.unit
def test_the_final_freshness_key_lives_in_the_task_and_not_in_the_scheduler(
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
def test_the_task_env_forwards_the_lanes_own_gateway(monkeypatch):
    monkeypatch.setenv(
        "SOFASCORE_PLAYERS_PROXY_CONTROL_URL", "http://sofascore_gw_players:8899"
    )
    module = _load_dag_module()

    assert module.PLAYERS_TASK_ENV == {
        "SOFASCORE_PLAYER_FRESHNESS_KEY": "final",
        "SOFASCORE_PROXY_CONTROL_URL": "http://sofascore_gw_players:8899",
    }
    # The lane's gateway is wired by the recipe, not by a scheduler default.
    environment = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))[
        "x-ss-airflow-common"
    ]["environment"]
    assert environment["SOFASCORE_PLAYERS_PROXY_CONTROL_URL"] == (
        "http://sofascore_gw_players:8899"
    )


@pytest.mark.unit
def test_the_unstarted_season_reason_matches_the_scope_cycle(clean_env):
    # The waiting reason is a literal here: importing the runner at DAG parse
    # would drag the whole scraper stack in.  The scope cycle stamps the same
    # text (``waiting_for_finished_matches``).
    module = _load_dag_module()

    assert module.UNSTARTED_SEASON_REASON == "season_has_no_finished_matches"
    assert module.UNSTARTED_SEASON_REASON in (
        SCOPE_CYCLE.read_text(encoding="utf-8")
    )


def _planner_kwargs(module, monkeypatch):
    """Drive ``_plan_players_batch`` to the planner and capture its kwargs."""

    captured = {}

    def _plan(snapshot, **kwargs):
        captured["snapshot"] = snapshot
        captured.update(kwargs)
        return []

    monkeypatch.setattr(
        module.state, "read_snapshot", lambda *a, **k: {"campaign_id": "c-1"}
    )
    monkeypatch.setattr(
        module.state,
        "read_completed",
        lambda path, *, campaign_id: {f"completed::{path}"},
    )
    monkeypatch.setattr(module.state, "read_failures", lambda *a, **k: {})
    monkeypatch.setattr(module.state, "plan_players_batch", _plan)
    assert module._plan_players_batch(run_id="scheduled-1") == []
    return captured


@pytest.mark.unit
def test_plan_task_feeds_both_state_files_and_lets_the_denominator_decide(
    clean_env, monkeypatch
):
    module = _load_dag_module()

    kwargs = _planner_kwargs(module, monkeypatch)

    assert kwargs["snapshot"] == {"campaign_id": "c-1"}
    # The QUEUE is the history lane's closed scopes, read-only here; the lane's
    # own memory (players-state.json / players-failures.json) is separate.
    assert kwargs["completed"] == {f"completed::{module.HISTORY_STATE_PATH}"}
    assert kwargs["players_completed"] == {f"completed::{module.STATE_PATH}"}
    assert kwargs["failures"] == {}
    # No ``exclude_tournament_ids``: the denominator (#1353) decides, not a knob.
    assert "exclude_tournament_ids" not in kwargs
    assert kwargs["batch_size"] == module.PLAYERS_BATCH_SIZE == 3
    assert kwargs["max_scope_attempts"] == module.PLAYERS_MAX_SCOPE_ATTEMPTS
    assert kwargs["snapshot_path"] == module.SNAPSHOT_PATH
    assert kwargs["policy_path"] == module.POLICY_PATH
    assert kwargs["result_dir"] == module.RESULT_DIR
    assert kwargs["workload_artifact"] == module.WORKLOAD_ARTIFACT
    assert kwargs["dag_run_id"] == "scheduled-1"
    assert kwargs["task_env"] is module.PLAYERS_TASK_ENV


@pytest.mark.unit
def test_players_lane_knobs_come_from_env(monkeypatch):
    monkeypatch.setenv("SOFASCORE_PLAYERS_BATCH_SIZE", "2")
    monkeypatch.setenv("SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS", "2")
    monkeypatch.setenv("SOFASCORE_PLAYERS_MAX_SCOPE_ATTEMPTS", "5")
    monkeypatch.setenv("SOFASCORE_PLAYERS_STATE", "/tmp/lane/players-state.json")
    monkeypatch.setenv("SOFASCORE_PLAYERS_RESULT_DIR", "/tmp/lane/results")
    module = _load_dag_module()
    operators = _operators()

    assert module.dag._dag_kwargs["max_active_tasks"] == 2
    run = operators["run_players_scope"]
    assert run._init_kwargs["max_active_tis_per_dag"] == 2
    assert module.PLAYERS_BATCH_SIZE == 2
    assert module.PLAYERS_MAX_SCOPE_ATTEMPTS == 5
    assert module.STATE_PATH == "/tmp/lane/players-state.json"
    assert module.FAILURES_PATH == "/tmp/lane/players-failures.json"
    assert module.RESULT_DIR == "/tmp/lane/results"
    kwargs = _planner_kwargs(module, monkeypatch)
    assert kwargs["batch_size"] == 2
    assert kwargs["max_scope_attempts"] == 5
    assert kwargs["result_dir"] == "/tmp/lane/results"


@pytest.mark.unit
def test_players_state_lives_beside_the_history_queue_not_inside_it(clean_env):
    module = _load_dag_module()

    assert module.HISTORY_STATE_PATH.endswith("/state.json")
    assert module.STATE_PATH.endswith("/players-state.json")
    assert module.STATE_PATH != module.HISTORY_STATE_PATH
    assert module.FAILURES_PATH == str(
        module.Path(module.STATE_PATH).with_name("players-failures.json")
    )
    assert module.RESULT_DIR == module.state.DEFAULT_PLAYERS_RESULT_DIR


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SOFASCORE_PLAYERS_BATCH_SIZE", "0"),
        ("SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS", "two"),
        ("SOFASCORE_PLAYERS_MAX_SCOPE_ATTEMPTS", "0"),
    ],
)
def test_invalid_players_lane_knob_fails_dag_parse(
    clean_env, monkeypatch, name, value
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        _load_dag_module()


def _scope_env(tmp_path, **overrides):
    environment = {
        "SOFASCORE_CAMPAIGN_ACTION": "players",
        "SOFASCORE_SCOPE_KEY": "c-1:17:1725",
        "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1",
        "SOFASCORE_SCOPE_RUN_ID": "run-1--17-1725",
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
        "run_id": "run-1--17-1725",
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
    assert all(item[1]["campaign_id"] == "c-1" for item in calls)


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
    """#1374: the match evidence has not closed yet — zero paid traffic left
    the cycle, so the task stays green; the failure memory counts the attempt
    and parks the scope on the attempts/cooldown cycle, never on the
    no-traffic streak that quarantines until a new release."""

    module = _load_dag_module()
    parked = {}
    monkeypatch.setattr(
        module.state, "mark_failed", lambda path, **k: parked.update(k)
    )
    monkeypatch.setattr(
        module.state,
        "mark_completed",
        lambda *a, **k: pytest.fail("a deferred scope is not complete"),
    )
    monkeypatch.setattr(module.state, "current_release", lambda: "rel-1374a")
    _write_scope_result(
        tmp_path,
        {
            **_success_result(),
            "status": "deferred",
            "deferral_reason": "PlayerEvidenceNotReady: matches pending",
        },
    )

    outcome = module._validate_players_scope(**_scope_env(tmp_path))

    assert outcome == {
        "status": "deferred",
        "scope_key": "c-1:17:1725",
        "reason": "PlayerEvidenceNotReady: matches pending",
    }
    # The runner defers before planning: no paid traffic happened (#1351).
    assert parked["source_requests"] == 0
    # An everyday evidence wait parks, it is not an incident: the deferred
    # branch opts out of the no-traffic streak/quarantine while the accurate
    # zero-source report is kept.
    assert parked["quarantine_on_no_traffic"] is False
    assert parked["reason"] == "PlayerEvidenceNotReady: matches pending"
    assert parked["release"] == "rel-1374a"
    assert parked["run_id"] == "run-1--17-1725"
    assert parked["campaign_id"] == "c-1"
    assert parked["scope_key"] == "c-1:17:1725"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("document", "message"),
    [
        ('{"status": "failed", "campaign_id": "c-1", "tournament_id": 17,'
         ' "source_season_id": 1725, "run_id": "run-1--17-1725"}',
         "did not finish successfully"),
        (
            '{"status": "success", "campaign_id": "other", "tournament_id": 17,'
            ' "source_season_id": 1725, "run_id": "run-1--17-1725"}',
            "provenance mismatch",
        ),
        (
            '{"status": "success", "campaign_id": "c-1", "tournament_id": 8,'
            ' "source_season_id": 1725, "run_id": "run-1--17-1725"}',
            "provenance mismatch",
        ),
        (
            '{"status": "success", "campaign_id": "c-1", "tournament_id": 17,'
            ' "source_season_id": 824, "run_id": "run-1--17-1725"}',
            "provenance mismatch",
        ),
        # The RUN identity is strict: the gateway holds one immutable signed
        # plan per run_id, so a result of another attempt must never book here.
        (
            '{"status": "success", "campaign_id": "c-1", "tournament_id": 17,'
            ' "source_season_id": 1725, "run_id": "another-run"}',
            "provenance mismatch",
        ),
    ],
)
def test_validate_fails_closed_on_foreign_provenance(
    clean_env, tmp_path, monkeypatch, document, message
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module()
    monkeypatch.setattr(
        module.state,
        "mark_completed",
        lambda *a, **k: pytest.fail("provenance must be checked first"),
    )
    _write_scope_result(tmp_path, json.loads(document), {"universe_players": 5})

    with pytest.raises(AirflowException, match=message):
        module._validate_players_scope(**_scope_env(tmp_path))


@pytest.mark.unit
def test_validate_rejects_a_foreign_campaign_action(clean_env, tmp_path):
    from airflow.exceptions import AirflowException

    module = _load_dag_module()
    _write_scope_result(tmp_path, _success_result(), {"universe_players": 5})

    with pytest.raises(AirflowException, match="unknown SofaScore campaign action"):
        module._validate_players_scope(
            **_scope_env(tmp_path, SOFASCORE_CAMPAIGN_ACTION="capture")
        )


@pytest.mark.unit
def test_validate_accepts_a_snapshot_revised_after_planning(
    clean_env, tmp_path, monkeypatch
):
    """#1354: the refresh lane's enrichment advances the snapshot between plan
    and run; the paid scope of the same campaign/tournament/season counts."""

    module = _load_dag_module()
    monkeypatch.setattr(module.state, "mark_completed", lambda *a, **k: None)
    monkeypatch.setattr(module.state, "clear_failed", lambda *a, **k: None)
    result = _success_result()
    result["snapshot_id"] = "revised"
    _write_scope_result(tmp_path, result, {"universe_players": 7})

    assert module._validate_players_scope(**_scope_env(tmp_path)) == {
        "status": "complete",
        "scope_key": "c-1:17:1725",
    }


@pytest.mark.unit
def test_validate_fails_closed_on_an_unreadable_result(clean_env, tmp_path):
    from airflow.exceptions import AirflowException

    module = _load_dag_module()
    # No result file was written at all (a killed attempt, for example).

    with pytest.raises(AirflowException, match="scope result is unreadable"):
        module._validate_players_scope(**_scope_env(tmp_path))


@pytest.mark.unit
def test_validate_fails_closed_on_an_unreadable_players_report(
    clean_env, tmp_path
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module()
    _write_scope_result(tmp_path, _success_result())

    with pytest.raises(AirflowException, match="players result is unreadable"):
        module._validate_players_scope(**_scope_env(tmp_path))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("run_state", "validate_state"),
    [("failed", "upstream_failed"), ("success", "failed")],
)
def test_finalize_parks_only_the_scope_that_actually_failed(
    clean_env, monkeypatch, run_state, validate_state
):
    """Lesson #1216: looking only at the bash task left a scope whose capture
    succeeded but whose validation failed neither completed nor failed — it
    came back first next run and was paid for in full again, every day."""

    module = _load_dag_module()
    parked = []
    monkeypatch.setattr(
        module.state, "mark_failed", lambda path, **k: parked.append(k["scope_key"])
    )
    planned = [
        {"SOFASCORE_SCOPE_KEY": "c-1:17:1725", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1"},
        {"SOFASCORE_SCOPE_KEY": "c-1:8:825", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1"},
    ]
    states = {
        ("run_players_scope", 0): run_state,
        ("validate_players_scope", 0): validate_state,
        ("run_players_scope", 1): "success",
        ("validate_players_scope", 1): "success",
    }
    dag_run = SimpleNamespace(
        get_task_instance=lambda task_id, map_index: SimpleNamespace(
            state=states[(task_id, map_index)]
        )
    )
    ti = SimpleNamespace(xcom_pull=lambda task_ids: planned)
    monkeypatch.delenv("SOFASCORE_RELEASE_ROOT", raising=False)

    outcome = module._finalize_players_run(ti=ti, dag_run=dag_run, run_id="run-1")

    assert parked == ["c-1:17:1725"]
    assert outcome == {"planned": 2, "failed": 1}


@pytest.mark.unit
def test_finalize_passes_the_scope_result_reason_traffic_and_release(
    clean_env, monkeypatch, tmp_path
):
    """#1351: quarantine needs the class of the failure, whether the attempt
    reached the source at all, and which release ran it."""

    module = _load_dag_module()
    result = tmp_path / "result.json"
    long_tail = "x" * 400
    result.write_text(json.dumps({
        "status": "failed",
        "errors": [
            "players: capture_engine: endpoint player_profile is nonterminal: "
            "schema_error; attempts: [attempt 1: " + long_tail + "]\nsecond line",
            "other",
        ],
        "phases": [{"phase": "players", "source_request_count": 4}],
    }))
    planned = [{
        "SOFASCORE_SCOPE_KEY": "c-1:17:1725",
        "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1",
        "SOFASCORE_SCOPE_RESULT_PATH": str(result),
        "SOFASCORE_SEASON_ALIGNMENT_IDENTITY": "season-input-identity",
    }]
    marked = []
    monkeypatch.setattr(
        module.state, "mark_failed", lambda path, **k: marked.append(k)
    )
    monkeypatch.setenv(
        "SOFASCORE_RELEASE_ROOT", "/opt/sofascore/releases/release-1a3d9890"
    )
    context = {
        "ti": SimpleNamespace(xcom_pull=lambda **kw: planned),
        "dag_run": SimpleNamespace(
            get_task_instance=lambda task_id, map_index=-1: SimpleNamespace(
                state="failed"
            )
        ),
        "run_id": "manual__4",
    }

    module._finalize_players_run(**context)

    assert marked == [{
        "campaign_id": "c-1", "scope_key": "c-1:17:1725", "run_id": "manual__4",
        "reason": (
            "players: capture_engine: endpoint player_profile is nonterminal: "
            "schema_error"
        ),
        "source_requests": 4,
        "release": "1a3d9890",
        "season_identity": "season-input-identity",
    }]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("states", "failed"),
    [
        (
            {
                "plan_players_batch": "success",
                "run_players_scope": "success",
                "validate_players_scope": "success",
                "finalize_players_run": "success",
            },
            [],
        ),
        (
            {
                "run_players_scope": "failed",
                "validate_players_scope": "upstream_failed",
            },
            ["run_players_scope", "validate_players_scope"],
        ),
        (
            {"validate_players_scope": "failed", "unrelated_task": "failed"},
            ["validate_players_scope"],
        ),
    ],
)
def test_propagate_players_status_is_the_single_honest_leaf(
    clean_env, states, failed
):
    from airflow.exceptions import AirflowException

    module = _load_dag_module()
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


@pytest.mark.unit
def test_the_scope_command_runs_the_players_phase_only(clean_env):
    module = _load_dag_module()

    assert "--phase players" in module.RUN_SCOPE_COMMAND
    assert "--phase all" not in module.RUN_SCOPE_COMMAND
    # The runner refuses a pending season for profiles anyway (#1374); the
    # refresh-lane flags have no business on this lane's command.
    assert "--allow-pending-season" not in module.RUN_SCOPE_COMMAND
    assert "--season-evidence" not in module.RUN_SCOPE_COMMAND
    assert "--registry-overlay" not in module.RUN_SCOPE_COMMAND
    assert '--run-id "${SOFASCORE_SCOPE_RUN_ID}"' in module.RUN_SCOPE_COMMAND


@pytest.mark.unit
def test_validate_is_isolated_from_sibling_scopes(clean_env):
    """A batch is a batch of independent scopes: with the default
    ``all_success`` one failed ``run[i]`` marks EVERY validation
    upstream_failed, and finalize then books two paid, healthy scopes as
    failures.  ``all_done`` lets each validation decide on its own result."""

    _load_dag_module()
    validate = _operators()["validate_players_scope"]

    assert validate._init_kwargs.get("trigger_rule") == "all_done"
    assert _operators()["finalize_players_run"]._init_kwargs["trigger_rule"] == (
        "all_done"
    )
    assert _operators()["propagate_players_status"]._init_kwargs["trigger_rule"] == (
        "all_done"
    )


@pytest.mark.unit
def test_compose_wires_the_players_lane_back_to_its_own_gateway_and_pool(clean_env):
    """#1374: refresh returns to the shared daily lane; players gets the third
    gateway and pool back (names kept since #1244)."""

    environment = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))[
        "x-ss-airflow-common"
    ]["environment"]

    assert environment["SOFASCORE_REFRESH_POOL"] == "ingest_scraper_pool"
    assert environment["SOFASCORE_REFRESH_PROXY_CONTROL_URL"] == (
        "http://sofascore_proxy_filter:8899"
    )
    assert environment["SOFASCORE_PLAYERS_POOL"] == "sofascore_players_pool"
    assert environment["SOFASCORE_PLAYERS_PROXY_CONTROL_URL"] == (
        "http://sofascore_gw_players:8899"
    )
    assert environment["SOFASCORE_PLAYERS_STATE"] == (
        "/opt/airflow/runtime/sofascore/all-men/players-state.json"
    )
    assert environment["SOFASCORE_PLAYERS_RESULT_DIR"] == (
        "/opt/airflow/runtime/sofascore/all-men/players-results"
    )
    assert environment["SOFASCORE_PLAYERS_BATCH_SIZE"] == (
        "${SOFASCORE_PLAYERS_BATCH_SIZE:-3}"
    )


@pytest.mark.unit
def test_both_airflowignores_agree_on_who_may_parse_the_lane(clean_env):
    shared = (ROOT / "dags" / ".airflowignore").read_text(encoding="utf-8")
    contour = (
        ROOT / "deploy" / "sofascore" / ".airflowignore"
    ).read_text(encoding="utf-8")
    fotmob = (
        ROOT / "deploy" / "fotmob" / "isolated.airflowignore"
    ).read_text(encoding="utf-8")
    name = "dag_players_sofascore_all_mens.py"

    def hidden_by(text):
        return any(
            not line.startswith("#") and re.search(line.strip(), name)
            for line in text.splitlines()
            if line.strip()
        )

    # Hidden from the shared platform scheduler and from the FotMob isolate...
    assert hidden_by(shared), "the shared scheduler must not parse the lane"
    assert hidden_by(fotmob), "the FotMob isolate must not parse the lane"
    # ...and parsed by the SofaScore contour, whose block-list must not match it.
    assert not hidden_by(contour), "the SofaScore contour must load the lane"
