"""Fail-closed evidence for the bounded issue-930 player source refresh."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


GENERATION_ID = "11111111-1111-4111-8111-111111111111"


def _reload_dag():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dag_ingest_fotmob", None)
    sys.modules.pop("dags.dag_ingest_fotmob", None)
    return importlib.import_module("dag_ingest_fotmob")


def _context(
    *,
    attempt: int = 1,
    preflight_state: str = "success",
    scrape_state: str = "failed",
    preflight_try: int = 1,
    scrape_try: int = 1,
    proof_try: int = 1,
):
    states = {
        "validate_publication_writer_fence": (preflight_state, preflight_try),
        "scrape_fotmob_data": (scrape_state, scrape_try),
    }
    dag_run = SimpleNamespace(
        run_id=f"issue930_replay_a{attempt}__{GENERATION_ID.replace('-', '')}",
        conf={"fotmob_publication": {"generation_id": GENERATION_ID}},
    )
    dag_run.get_task_instance = lambda *, task_id: SimpleNamespace(
        state=states[task_id][0], try_number=states[task_id][1]
    )
    return {
        "dag_run": dag_run,
        "ti": SimpleNamespace(
            task_id="capture_replay_missing_inputs", try_number=proof_try
        ),
    }


def _gap_report(mod):
    scope_path = (
        Path(__file__).resolve().parents[3] / "configs/fotmob/issue-930-scopes.txt"
    )
    scopes = scope_path.read_text(encoding="utf-8").splitlines()
    contract = mod._source_refresh_contract()
    affected_scopes = sorted(
        {
            f"{target['competition_id']}={target['source_season_key']}"
            for target in contract["targets"]
        }
    )
    return {
        "run_id": GENERATION_ID,
        "mode": "replay",
        "status": "incomplete",
        "complete": False,
        "errors": ["typed missing raw input"],
        "transport": {"attempts": 0, "direct_bytes": 0, "proxy_bytes": 0},
        "budget": {
            "requests": 0,
            "max_requests": 2_000,
            "direct_bytes": 0,
            "max_direct_bytes": 256 * 1024 * 1024,
            "proxy_bytes": 0,
            "max_proxy_bytes": 0,
        },
        "selection": {
            "entities": list(mod.ISSUE_930_REPLAY_ENTITIES),
            "explicit_scopes": scopes,
            "competition_limit": 0,
            "season_limit": 0,
            "scope_plan_signature": "fmplan1-98c9a8f98ba8eaa14bfc8232b9667682e11e4fce27e120eee5ea9572b66e0385",
            "planned_scopes": list(scopes),
            "completed_scopes": [
                scope for scope in scopes if scope not in affected_scopes
            ],
            "replay_missing_player_inputs": {
                "schema": mod.REPLAY_MISSING_INPUT_SCHEMA,
                "failure_class": "missing_player_raw_inputs_only",
                "missing_player_ids": contract["player_ids"],
                "affected_scopes": affected_scopes,
            },
        },
    }


@pytest.mark.unit
def test_offline_service_records_typed_missing_manifest_identity():
    from scrapers.fotmob.planner import RunMode
    from tests.unit.scrapers.test_fotmob_service import _service

    service, transport, _repository = _service({}, mode=RunMode.REPLAY)

    result = service.sync_player_snapshots([302783], capture_terminal_outcomes=True)

    assert not result.ok
    assert result.metadata["missing_raw_player_ids"] == [302783]
    assert result.metadata["terminal_outcomes"] == [
        {"player_id": 302783, "status": "missing_raw_input"}
    ]
    assert transport.calls == []


@pytest.mark.unit
def test_runner_emits_evidence_only_when_typed_gap_explains_every_failure():
    from dags.scripts import run_fotmob_scraper as runner
    from scrapers.fotmob.service import OperationResult

    scope = "47=2026/2027"
    ids = [302783, 798654]
    outstanding = {"player_snapshots": len(ids)}
    retryable = f"scope {scope} incomplete; outstanding={outstanding}"
    player = OperationResult(
        "player_snapshots",
        attempted=len(ids),
        errors=[
            f"player {player_id}: {runner._MISSING_PLAYER_RAW_ERROR}"
            for player_id in ids
        ],
        metadata={
            "missing_raw_player_ids": ids,
            "terminal_outcomes": [
                {"player_id": player_id, "status": "missing_raw_input"}
                for player_id in ids
            ],
        },
    )
    work_plan = OperationResult(
        "season_work_plan",
        attempted=1,
        retryable=[retryable],
        metadata={"incomplete_scopes": [{"scope": scope, "outstanding": outstanding}]},
    )
    flush = OperationResult("commit_flush", attempted=1, succeeded=1)
    gap_entries = [
        {
            "scope": scope,
            "outstanding": outstanding,
            "work_plan_retryable": retryable,
            "player_operation": player,
            "missing_player_ids": ids,
        }
    ]

    proof = runner._replay_missing_raw_evidence(
        operations=[work_plan, player, flush],
        work_plan=work_plan,
        planned_scopes=[scope],
        completed_scopes=[],
        gap_entries=gap_entries,
    )

    assert proof == {
        "schema": runner.REPLAY_MISSING_INPUT_SCHEMA,
        "failure_class": "missing_player_raw_inputs_only",
        "missing_player_ids": ids,
        "affected_scopes": [scope],
    }
    flush.errors.append("unrelated commit failure")
    assert (
        runner._replay_missing_raw_evidence(
            operations=[work_plan, player, flush],
            work_plan=work_plan,
            planned_scopes=[scope],
            completed_scopes=[],
            gap_entries=gap_entries,
        )
        is None
    )


@pytest.mark.unit
def test_all_done_task_returns_canonical_attempt_one_seven_target_proof(tmp_path):
    mod = _reload_dag()
    payload = _gap_report(mod)
    report_path = tmp_path / "replay.json"
    raw = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    report_path.write_bytes(raw)

    proof = mod.prove_replay_missing_player_inputs(str(report_path), **_context())

    assert proof["schema_version"] == mod.REPLAY_MISSING_INPUT_SCHEMA
    assert proof["status"] == "source_refresh_required"
    assert proof["runner_result_sha256"] == hashlib.sha256(raw).hexdigest()
    assert proof["run_id"] == GENERATION_ID
    assert proof["scope_count"] == mod.FOTMOB_DAILY_SCOPE_COUNT
    assert proof["scope_sha256"] == mod.FOTMOB_DAILY_SCOPE_SHA256
    assert proof["artifact_sha256"] == mod.PLAYER_SOURCE_REFRESH_SHA256
    assert proof["target_count"] == 7
    assert proof["targets"] == mod._source_refresh_contract()["targets"]

    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    task = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "capture_replay_missing_inputs"
    )
    assert task._init_kwargs["trigger_rule"] == "all_done"
    assert task.upstream_task_ids == {"scrape_fotmob_data"}
    assert task.downstream_task_ids == {"finalize_fotmob_publication"}
    scraper = next(
        task for task in BashOperator._instances if task.task_id == "scrape_fotmob_data"
    )
    assert "ti.try_number" not in mod.RESULT_PATH
    assert f'/usr/bin/rm -f -- "{mod.RESULT_PATH}"' in scraper.bash_command


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["selection"]["replay_missing_player_inputs"][
            "missing_player_ids"
        ].pop(),
        lambda payload: payload["selection"][
            "replay_missing_player_inputs"
        ].__setitem__("failure_class", "mixed_failure"),
        lambda payload: payload["transport"].__setitem__("attempts", 1),
        lambda payload: payload.__setitem__("transport", {}),
        lambda payload: payload.__setitem__("budget", {}),
        lambda payload: payload["selection"]["explicit_scopes"].pop(),
        lambda payload: payload["selection"]["completed_scopes"].append("47=2026/2027"),
    ],
)
def test_gap_proof_rejects_widening_drift_and_mixed_failures(tmp_path, mutate):
    from airflow.exceptions import AirflowException

    mod = _reload_dag()
    payload = copy.deepcopy(_gap_report(mod))
    mutate(payload)
    path = tmp_path / "bad-replay.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AirflowException):
        mod.prove_replay_missing_player_inputs(str(path), **_context())


@pytest.mark.unit
def test_successful_replay_does_not_authorize_a_source_refresh(tmp_path):
    mod = _reload_dag()
    payload = _gap_report(mod)
    payload["status"] = "success"
    payload["complete"] = True
    payload["errors"] = []
    payload["selection"].pop("replay_missing_player_inputs")
    path = tmp_path / "green-replay.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert (
        mod.prove_replay_missing_player_inputs(
            str(path), **_context(scrape_state="success")
        )
        is None
    )


@pytest.mark.unit
def test_successful_second_replay_keeps_proof_task_green_without_authorizing(tmp_path):
    mod = _reload_dag()
    payload = _gap_report(mod)
    payload["status"] = "success"
    payload["complete"] = True
    payload["errors"] = []
    payload["selection"].pop("replay_missing_player_inputs")
    path = tmp_path / "green-replay-a2.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert (
        mod.prove_replay_missing_player_inputs(
            str(path), **_context(attempt=2, scrape_state="success")
        )
        is None
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("preflight_state", "scrape_state"),
    [("failed", "upstream_failed"), ("success", "upstream_failed")],
)
def test_stale_result_cannot_authorize_without_exact_task_states(
    tmp_path, preflight_state, scrape_state
):
    mod = _reload_dag()
    path = tmp_path / "stale-replay.json"
    path.write_text(json.dumps(_gap_report(mod)), encoding="utf-8")

    assert (
        mod.prove_replay_missing_player_inputs(
            str(path),
            **_context(
                preflight_state=preflight_state,
                scrape_state=scrape_state,
            ),
        )
        is None
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "try_overrides",
    [
        {"preflight_try": 2},
        {"scrape_try": 2},
        {"proof_try": 2},
    ],
)
def test_retried_task_cannot_authorize_source_refresh(tmp_path, try_overrides):
    mod = _reload_dag()
    path = tmp_path / "retried-replay.json"
    path.write_text(json.dumps(_gap_report(mod)), encoding="utf-8")

    assert (
        mod.prove_replay_missing_player_inputs(str(path), **_context(**try_overrides))
        is None
    )
