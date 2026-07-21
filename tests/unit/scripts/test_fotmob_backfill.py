from __future__ import annotations

import hashlib
import json
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts import fotmob_backfill as mod


SHA = "a" * 40
GENERATION_ID = "12345678-1234-5678-9234-123456789abc"


def _args(tmp_path: Path, **overrides) -> Namespace:
    values = {
        "command": "run",
        "compose_file": tmp_path / "compose.yaml",
        "env_file": tmp_path / "fotmob.env",
        "deployment_report": tmp_path / "deployment.json",
        "recovery_report": None,
        "project": "fotmob-airflow",
        "output": tmp_path / "backfill.json",
        "mode": "backfill",
        "publication_attempt": 1,
        "scopes": tmp_path / "scopes.txt",
        "scope_sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
        "source_refresh_profile": "",
        "source_refresh_targets_sha256": "",
        "expected_git_sha": SHA,
        "max_requests": 2_000,
        "max_direct_mib": 256,
        "timeout_seconds": 5,
        "execute": True,
        "confirm": mod.CONFIRM_RUN,
    }
    values.update(overrides)
    return Namespace(**values)


def _source_args(tmp_path: Path, **overrides) -> Namespace:
    values = {
        "source_refresh_profile": mod.PLAYER_SOURCE_REFRESH_PROFILE,
        "source_refresh_targets_sha256": mod.PLAYER_SOURCE_REFRESH_SHA256,
        "max_requests": mod.PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        "max_direct_mib": mod.PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
    }
    values.update(overrides)
    return _args(tmp_path, **values)


def _context(tmp_path: Path) -> dict:
    return {
        "kept_paused": True,
        "git_sha": SHA,
        "deployment_id": "f" * 32,
        "generated_at": "2026-07-21T10:00:00Z",
    }


def _publication(*, start: str = "2026-07-22T10:00:00.000000+00:00") -> dict:
    end = (datetime.fromisoformat(start) + timedelta(seconds=1)).isoformat(
        timespec="microseconds"
    )
    return {
        "generation_id": GENERATION_ID,
        "binding": {
            "schema": "fotmob-publication-v1",
            "source": "fotmob",
            "owner": "isolated",
            "data_interval_start": start,
            "data_interval_end": end,
            "runtime_fingerprint": SHA,
        },
    }


def _scope_contract(tmp_path: Path) -> dict:
    return {
        "name": "issue-930-verify",
        "artifact": str((tmp_path / "scopes.txt").resolve()),
        "sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
        "count": mod.APPROVED_SCOPE_COUNT,
        "identities": [f"{index}=2025" for index in range(1, 159)],
    }


def _quiet() -> dict:
    return {
        "pause_states": {dag_id: True for dag_id in mod.DAGS},
        "active_runs": {},
    }


def _candidate() -> dict:
    return {
        "generation_id": GENERATION_ID,
        "digest": "d" * 64,
        "transform_task_ids": ["transform_a", "transform_b"],
    }


def _state(phase: str = "ready") -> dict:
    return {
        "generation_id": GENERATION_ID,
        "binding": _publication()["binding"],
        "owner_dag_id": mod.PUBLICATION_OWNER_DAG_ID,
        "status": "running" if phase in {"writing", "failed"} else "succeeded",
        "phase": phase,
        "active": phase not in {"failed", "abandoned"},
        "candidate": _candidate() if phase in {"ready", "abandoned"} else None,
        "released_at": "2026-07-21T12:00:00Z" if phase == "abandoned" else None,
    }


def _validation() -> dict:
    return {
        "status": "success",
        "run_id": GENERATION_ID,
        "mode": "backfill",
        "transport": {"proxy_bytes": 0},
        "budget": {"requests": 2000, "max_requests": 2000},
        "selection": {
            "entities": sorted(mod.ISSUE_930_SCOPE_ENTITIES),
            "explicit_scope_count": 158,
            "explicit_scope_sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
            "scope_plan_signature": "fmplan1-" + "e" * 64,
            "competition_limit": 0,
            "season_limit": 0,
        },
    }


def _source_validation() -> dict:
    contract = mod.load_player_source_refresh_contract(
        mod.REPOSITORY_ROOT / mod.PLAYER_SOURCE_REFRESH_ARTIFACT
    )
    source = {
        key: contract[key]
        for key in (
            "profile",
            "artifact",
            "sha256",
            "target_count",
            "targets",
            "plan_signature",
        )
    }
    return {
        "status": "success",
        "run_id": GENERATION_ID,
        "mode": "backfill",
        "transport": {"attempts": 8, "proxy_bytes": 0},
        "budget": {
            "requests": 8,
            "max_requests": mod.PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
            "direct_bytes": 1024,
            "max_direct_bytes": (
                mod.PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB * 1024 * 1024
            ),
            "proxy_bytes": 0,
            "max_proxy_bytes": 0,
        },
        "selection": {
            "profile": contract["profile"],
            "entities": ["players"],
            "explicit_scope_count": 0,
            "explicit_scope_sha256": hashlib.sha256(b"").hexdigest(),
            "scope_plan_signature": contract["plan_signature"],
            "competition_limit": 0,
            "season_limit": 0,
            "source_refresh": source,
            "target_outcomes": [
                {**target, "status": "success"} for target in contract["targets"]
            ],
        },
    }


def _wire_preflight(monkeypatch, tmp_path: Path, calls: list[tuple]) -> None:
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    monkeypatch.setattr(
        mod, "_load_scope_contract", lambda *_args: _scope_contract(tmp_path)
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_args, **_kw: {})
    monkeypatch.setattr(mod, "inspect_writer_state", lambda *_args, **_kw: _quiet())
    monkeypatch.setattr(
        mod,
        "require_no_active_publication",
        lambda *_args, **_kw: {"source": "fotmob", "active": False, "safe": True},
    )
    monkeypatch.setattr(
        mod, "_publication_envelope", lambda *_args, **_kw: _publication()
    )

    def airflow(_args, *command, **_kwargs):
        calls.append(command)
        return ""

    monkeypatch.setattr(mod, "_airflow", airflow)


def test_success_triggers_only_parent_and_abandons_exact_candidate(
    tmp_path, monkeypatch
):
    calls: list[tuple] = []
    transitions: list[str] = []
    _wire_preflight(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(
        mod, "_initialize_publication", lambda *_args, **_kw: _state("writing")
    )

    def exact(_args, dag_id, run_id, **_kwargs):
        expected = (
            f"issue930_backfill_a1__{GENERATION_ID.replace('-', '')}"
            if dag_id == mod.INGEST_DAG_ID
            else f"fotmob_silver__{GENERATION_ID}"
        )
        assert run_id == expected
        return {"run_id": run_id, "state": "success"}

    monkeypatch.setattr(mod, "_exact_run", exact)
    monkeypatch.setattr(mod, "_get_publication", lambda *_args, **_kw: _state())
    monkeypatch.setattr(mod, "_validation_xcom", lambda *_args, **_kw: _validation())

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        return {
            **_state("abandoned"),
            "active": False,
            "released": True,
            "published": False,
        }

    monkeypatch.setattr(mod, "_transition_publication", transition)

    report = mod.run_backfill(
        _args(tmp_path), sleeper=lambda _value: None, monotonic=lambda: 0
    )

    assert report["passed"] is True
    assert report["phase"] == "abandoned"
    assert report["scope"]["count"] == 158
    assert report["entities"] == [
        "season",
        "leaderboards",
        "matches",
        "teams",
        "players",
    ]
    assert report["plan_signature"] == "fmplan1-" + "e" * 64
    assert report["publication_attempt"] == 1
    assert transitions == ["abandon"]
    trigger_calls = [call for call in calls if call[:2] == ("dags", "trigger")]
    assert len(trigger_calls) == 1
    assert trigger_calls[0][2] == mod.INGEST_DAG_ID
    assert all(
        not (call[:2] == ("dags", "trigger") and call[2] == mod.SILVER_DAG_ID)
        for call in calls
    )
    assert ("dags", "unpause", mod.DAILY_DAG_ID) not in calls
    conf = json.loads(trigger_calls[0][trigger_calls[0].index("--conf") + 1])
    assert conf["mode"] == "backfill"
    assert len(conf["scope"].split(",")) == 158
    assert conf["entities"] == "season,leaderboards,matches,teams,players"
    assert "transfers" not in conf["entities"]
    assert conf["fotmob_publication"] == _publication()
    assert json.loads(_args(tmp_path).output.read_text())["phase"] == "abandoned"


def test_source_refresh_triggers_exact_seven_player_parent_profile(
    tmp_path, monkeypatch
):
    calls: list[tuple] = []
    transitions: list[str] = []
    _wire_preflight(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(
        mod, "_initialize_publication", lambda *_args, **_kw: _state("writing")
    )
    monkeypatch.setattr(
        mod,
        "_exact_run",
        lambda _args, _dag_id, run_id, **_kw: {
            "run_id": run_id,
            "state": "success",
        },
    )
    monkeypatch.setattr(mod, "_get_publication", lambda *_args, **_kw: _state())
    monkeypatch.setattr(
        mod, "_validation_xcom", lambda *_args, **_kw: _source_validation()
    )

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        return {
            **_state("abandoned"),
            "active": False,
            "released": True,
            "published": False,
        }

    monkeypatch.setattr(mod, "_transition_publication", transition)
    arguments = _source_args(tmp_path)

    report = mod.run_backfill(
        arguments, sleeper=lambda _value: None, monotonic=lambda: 0
    )

    assert report["passed"] is True and report["phase"] == "abandoned"
    assert report["profile"] == mod.PLAYER_SOURCE_REFRESH_PROFILE
    assert report["entities"] == ["players"]
    assert report["limits"]["executed_scope_count"] == 0
    assert report["source_refresh"]["target_count"] == 7
    assert len(report["validation"]["target_outcomes"]) == 7
    trigger = next(call for call in calls if call[:2] == ("dags", "trigger"))
    conf = json.loads(trigger[trigger.index("--conf") + 1])
    assert conf["mode"] == "backfill"
    assert conf["scope"] == ""
    assert conf["entities"] == "players"
    assert conf["source_refresh_profile"] == mod.PLAYER_SOURCE_REFRESH_PROFILE
    assert conf["source_refresh_targets_sha256"] == (mod.PLAYER_SOURCE_REFRESH_SHA256)
    assert conf["source_refresh_target_count"] == 7
    assert conf["max_requests"] == 64
    assert conf["max_direct_mib"] == 8
    assert conf["match_limit"] == conf["team_limit"] == conf["player_limit"] == 0
    assert transitions == ["abandon"]


def test_proven_failed_parent_releases_and_requires_next_attempt(tmp_path, monkeypatch):
    calls: list[tuple] = []
    _wire_preflight(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(
        mod, "_initialize_publication", lambda *_args, **_kw: _state("writing")
    )
    monkeypatch.setattr(
        mod,
        "_exact_run",
        lambda _args, dag_id, run_id, **_kw: (
            {"run_id": run_id, "state": "failed"}
            if dag_id == mod.INGEST_DAG_ID
            else None
        ),
    )
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("writing")
    )
    actions = []

    def transition(_args, _generation_id, *, action, **_kwargs):
        actions.append(action)
        return {
            **_state("failed"),
            "status": "failed",
            "active": False,
            "released": True,
        }

    monkeypatch.setattr(mod, "_transition_publication", transition)

    report = mod.run_backfill(
        _args(tmp_path), sleeper=lambda _value: None, monotonic=lambda: 0
    )

    assert report["passed"] is False
    assert report["phase"] == "failed_generation_released"
    assert report["recovery_required"] is False
    assert report["next_publication_attempt"] == 2
    assert actions == ["fail_release"]


def test_timeout_with_active_writer_retains_generation(tmp_path, monkeypatch):
    calls: list[tuple] = []
    _wire_preflight(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(
        mod, "_initialize_publication", lambda *_args, **_kw: _state("writing")
    )
    states = iter(
        [
            _quiet(),
            {
                "pause_states": {dag_id: True for dag_id in mod.DAGS},
                "active_runs": {mod.INGEST_DAG_ID: {"running": ["exact-ingest"]}},
            },
        ]
    )
    monkeypatch.setattr(mod, "inspect_writer_state", lambda *_args, **_kw: next(states))
    monkeypatch.setattr(
        mod,
        "_exact_run",
        lambda _args, _dag_id, run_id, **_kw: {"run_id": run_id, "state": "running"},
    )
    monkeypatch.setattr(
        mod,
        "_transition_publication",
        lambda *_args, **_kw: pytest.fail("ambiguous timeout must not release"),
    )
    clock = iter([0, 2])

    report = mod.run_backfill(
        _args(tmp_path, timeout_seconds=1),
        sleeper=lambda _value: None,
        monotonic=lambda: next(clock),
    )

    assert report["passed"] is False
    assert report["recovery_required"] is True
    assert report["phase"] == "lock_retained_writer_state_ambiguous"
    assert "timeout" in report["error"]


def test_acquire_response_loss_persists_exact_recovery_identity(tmp_path, monkeypatch):
    calls: list[tuple] = []
    _wire_preflight(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(
        mod,
        "_initialize_publication",
        lambda *_args, **_kw: (_ for _ in ()).throw(ConnectionError("lost reply")),
    )

    report = mod.run_backfill(_args(tmp_path))
    durable = json.loads(_args(tmp_path).output.read_text())

    assert report["phase"] == "acquire_ambiguous"
    assert report["recovery_required"] is True
    assert durable["publication"]["generation_id"] == GENERATION_ID
    assert durable["runs"]["silver_run_id"] == f"fotmob_silver__{GENERATION_ID}"
    assert not calls


def _recovery_report(tmp_path: Path, phase: str) -> Path:
    arguments = _args(tmp_path)
    report = mod._base_report(
        arguments,
        mode="backfill",
        publication=_publication(),
        scope_contract=_scope_contract(tmp_path),
    )
    report["phase"] = phase
    path = tmp_path / "recovery-input.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def _wire_recovery(monkeypatch, tmp_path: Path, calls: list[tuple], phase: str):
    _wire_preflight(monkeypatch, tmp_path, calls)
    recovery_path = _recovery_report(tmp_path, phase)
    monkeypatch.setattr(
        mod,
        "_load_recovery_report",
        lambda *_args, **_kw: (
            json.loads(recovery_path.read_text()),
            _publication(),
        ),
    )
    return _args(
        tmp_path,
        command="recover",
        mode=None,
        recovery_report=recovery_path,
        confirm=mod.CONFIRM_RECOVER,
    )


def test_recovery_releases_pretrigger_acquire_and_keeps_daily_paused(
    tmp_path, monkeypatch
):
    calls: list[tuple] = []
    arguments = _wire_recovery(monkeypatch, tmp_path, calls, "acquired_writers_paused")
    monkeypatch.setattr(mod, "_exact_run", lambda *_args, **_kw: None)
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("writing")
    )
    actions = []

    def transition(_args, _generation_id, *, action, **_kwargs):
        actions.append(action)
        return {
            **_state("failed"),
            "status": "failed",
            "active": False,
            "released": True,
        }

    monkeypatch.setattr(mod, "_transition_publication", transition)

    report = mod.recover_backfill(arguments)

    assert report["phase"] == "failed_generation_released"
    assert report["recovery_required"] is False
    assert actions == ["fail_release"]
    assert all(call[:2] != ("dags", "unpause") for call in calls)


def test_recovery_after_committed_abandon_is_idempotently_green(tmp_path, monkeypatch):
    calls: list[tuple] = []
    arguments = _wire_recovery(monkeypatch, tmp_path, calls, "ready_pending_abandon")
    monkeypatch.setattr(
        mod,
        "_exact_run",
        lambda _args, _dag_id, run_id, **_kw: {
            "run_id": run_id,
            "state": "success",
        },
    )
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("abandoned")
    )
    monkeypatch.setattr(mod, "_validation_xcom", lambda *_args, **_kw: _validation())
    monkeypatch.setattr(
        mod,
        "_transition_publication",
        lambda *_args, **_kw: pytest.fail(
            "already-abandoned recovery must be read-only"
        ),
    )

    report = mod.recover_backfill(arguments)

    assert report["passed"] is True
    assert report["phase"] == "abandoned"
    assert report["publication_state"]["active"] is False


def test_recovery_releases_when_write_ahead_trigger_intent_has_no_exact_run(
    tmp_path, monkeypatch
):
    calls: list[tuple] = []
    arguments = _wire_recovery(monkeypatch, tmp_path, calls, "trigger_intent")
    monkeypatch.setattr(mod, "_exact_run", lambda *_args, **_kw: None)
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("writing")
    )
    actions = []

    def transition(_args, _generation_id, *, action, **_kwargs):
        actions.append(action)
        return {
            **_state("failed"),
            "status": "failed",
            "active": False,
            "released": True,
        }

    monkeypatch.setattr(mod, "_transition_publication", transition)

    report = mod.recover_backfill(arguments)

    assert report["passed"] is False
    assert report["recovery_required"] is False
    assert report["phase"] == "failed_generation_released"
    assert actions == ["fail_release"]


def test_recovery_retains_when_confirmed_ingest_run_disappears(tmp_path, monkeypatch):
    calls: list[tuple] = []
    arguments = _wire_recovery(monkeypatch, tmp_path, calls, "ingest_running")
    monkeypatch.setattr(mod, "_exact_run", lambda *_args, **_kw: None)
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("writing")
    )
    monkeypatch.setattr(
        mod,
        "_transition_publication",
        lambda *_args, **_kw: pytest.fail("confirmed missing run is anomalous"),
    )

    report = mod.recover_backfill(arguments)

    assert report["passed"] is False
    assert report["recovery_required"] is True
    assert report["phase"] == "lock_retained_pending_recovery"
    assert "absent after trigger intent" in report["error"]


def test_publication_attempts_have_distinct_namespace_from_rollback(
    tmp_path, monkeypatch
):
    responses = [
        _publication(start="2026-07-22T10:00:00.000000+00:00"),
        {
            **_publication(start="2026-07-22T10:00:02.000000+00:00"),
            "generation_id": "22345678-1234-5678-9234-123456789abc",
        },
        {
            **_publication(start="2026-07-22T10:00:01.000000+00:00"),
            "generation_id": "32345678-1234-5678-9234-123456789abc",
        },
    ]
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    monkeypatch.setattr(
        mod,
        "_container_python_json",
        lambda *_args, **_kwargs: responses.pop(0),
    )

    first = mod._publication_envelope(
        _args(tmp_path, publication_attempt=1), "backfill", run=None
    )
    second = mod._publication_envelope(
        _args(tmp_path, publication_attempt=2), "backfill", run=None
    )
    replay = mod._publication_envelope(
        _args(tmp_path, publication_attempt=1), "replay", run=None
    )

    assert (
        len({first["generation_id"], second["generation_id"], replay["generation_id"]})
        == 3
    )
    assert first["binding"]["data_interval_start"] == (
        "2026-07-22T10:00:00.000000+00:00"
    )
    assert second["binding"]["data_interval_start"] == (
        "2026-07-22T10:00:02.000000+00:00"
    )
    # Rollback attempt 1 owns 2026-07-21T10:00:01Z, not this +1-day namespace.
    assert replay["binding"]["data_interval_start"] == (
        "2026-07-22T10:00:01.000000+00:00"
    )


def test_scope_contract_is_exactly_the_reviewed_158_byte_artifact(tmp_path):
    contract = mod._load_scope_contract(
        mod.APPROVED_SCOPE_ARTIFACT, mod.APPROVED_SCOPE_ARTIFACT_SHA256
    )
    assert contract["count"] == 158
    assert contract["sha256"] == mod.APPROVED_SCOPE_ARTIFACT_SHA256

    changed = tmp_path / "changed-scopes.txt"
    changed.write_bytes(mod.APPROVED_SCOPE_ARTIFACT.read_bytes() + b"1=2025\n")
    with pytest.raises(mod.BackfillError, match="reviewed SHA-256"):
        mod._load_scope_contract(changed, mod.APPROVED_SCOPE_ARTIFACT_SHA256)


def test_issue_930_entity_contract_exactly_matches_acceptance_without_transfers():
    assert frozenset(mod.ISSUE_930_SCOPE_ENTITIES) == mod.REQUIRED_SCOPE_ENTITIES
    assert mod.ISSUE_930_SCOPE_ENTITIES == (
        "season",
        "leaderboards",
        "matches",
        "teams",
        "players",
    )
    assert "transfers" not in mod.ISSUE_930_SCOPE_ENTITIES


def test_validation_rejects_transfer_entity_outside_issue_contract(tmp_path):
    payload = _validation()
    payload["selection"]["entities"].append("transfers")

    with pytest.raises(mod.BackfillError, match="exact issue-930 plan"):
        mod._validation_summary(
            payload,
            mode="backfill",
            generation_id=GENERATION_ID,
            scope_contract=_scope_contract(tmp_path),
        )


def test_recovery_report_rejects_transfer_entity_contract(tmp_path, monkeypatch):
    arguments = _args(tmp_path, command="recover")
    scope_contract = _scope_contract(tmp_path)
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    report = mod._base_report(
        arguments,
        mode="backfill",
        publication=_publication(),
        scope_contract=scope_contract,
    )
    report["entities"].append("transfers")
    recovery = tmp_path / "transfer-report.json"
    recovery.write_text(json.dumps(report), encoding="utf-8")
    arguments.recovery_report = recovery

    with pytest.raises(mod.BackfillError, match="stack or scope identity differs"):
        mod._load_recovery_report(arguments, scope_contract, run=None)


@pytest.mark.parametrize(
    "overrides",
    [
        {"mode": "replay"},
        {"max_requests": 63},
        {"max_direct_mib": 9},
        {"source_refresh_profile": "unreviewed"},
        {"source_refresh_targets_sha256": "0" * 64},
    ],
)
def test_source_refresh_drift_fails_before_external_action(
    tmp_path, monkeypatch, overrides
):
    called = False

    def context(_args):
        nonlocal called
        called = True
        return _context(tmp_path)

    monkeypatch.setattr(mod, "_deployment_context", context)

    with pytest.raises(mod.BackfillError):
        mod.run_backfill(_source_args(tmp_path, **overrides))

    assert called is False


def test_source_refresh_recovery_requires_same_artifact_identity(tmp_path, monkeypatch):
    source_arguments = _source_args(tmp_path, command="recover")
    mod._load_source_refresh_contract(source_arguments)
    scope_contract = _scope_contract(tmp_path)
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    report = mod._base_report(
        source_arguments,
        mode="backfill",
        publication=_publication(),
        scope_contract=scope_contract,
    )
    recovery = tmp_path / "source-recovery.json"
    recovery.write_text(json.dumps(report), encoding="utf-8")
    normal_arguments = _args(
        tmp_path,
        command="recover",
        recovery_report=recovery,
        mode=None,
        confirm=mod.CONFIRM_RECOVER,
    )
    mod._load_source_refresh_contract(normal_arguments)

    with pytest.raises(mod.BackfillError, match="stack or scope identity differs"):
        mod._load_recovery_report(normal_arguments, scope_contract, run=None)


def test_source_refresh_recovery_accepts_exact_profile_and_is_idempotent(
    tmp_path, monkeypatch
):
    arguments = _source_args(
        tmp_path,
        command="recover",
        mode="backfill",
        confirm=mod.CONFIRM_RECOVER,
    )
    monkeypatch.setattr(mod, "_deployment_context", lambda _args: _context(tmp_path))
    scope_contract = _scope_contract(tmp_path)
    mod._load_source_refresh_contract(arguments)
    report = mod._base_report(
        arguments,
        mode="backfill",
        publication=_publication(),
        scope_contract=scope_contract,
    )
    report["phase"] = "ready_pending_abandon"
    recovery = tmp_path / "source-recovery-exact.json"
    recovery.write_text(json.dumps(report), encoding="utf-8")
    arguments.recovery_report = recovery
    monkeypatch.setattr(mod, "_load_scope_contract", lambda *_args: scope_contract)
    monkeypatch.setattr(
        mod, "_publication_envelope", lambda *_args, **_kw: _publication()
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_args, **_kw: {})
    monkeypatch.setattr(mod, "_pause_all", lambda *_args, **_kw: _quiet())
    monkeypatch.setattr(mod, "inspect_writer_state", lambda *_args, **_kw: _quiet())
    monkeypatch.setattr(
        mod,
        "require_no_active_publication",
        lambda *_args, **_kw: {"source": "fotmob", "active": False, "safe": True},
    )
    monkeypatch.setattr(
        mod,
        "_exact_run",
        lambda _args, _dag_id, run_id, **_kw: {
            "run_id": run_id,
            "state": "success",
        },
    )
    monkeypatch.setattr(
        mod, "_get_publication", lambda *_args, **_kw: _state("abandoned")
    )
    monkeypatch.setattr(
        mod, "_validation_xcom", lambda *_args, **_kw: _source_validation()
    )
    monkeypatch.setattr(
        mod,
        "_transition_publication",
        lambda *_args, **_kw: pytest.fail(
            "already-abandoned source recovery must be read-only"
        ),
    )

    resolved = mod.recover_backfill(arguments)

    assert resolved["passed"] is True
    assert resolved["phase"] == "abandoned"
    assert resolved["validation"]["profile"] == mod.PLAYER_SOURCE_REFRESH_PROFILE
    assert len(resolved["validation"]["target_outcomes"]) == 7


def test_nonpositive_attempt_fails_before_any_external_action(tmp_path, monkeypatch):
    called = False

    def context(_args):
        nonlocal called
        called = True
        return _context(tmp_path)

    monkeypatch.setattr(mod, "_deployment_context", context)
    with pytest.raises(mod.BackfillError, match="positive integer"):
        mod.run_backfill(_args(tmp_path, publication_attempt=0))
    assert called is False
