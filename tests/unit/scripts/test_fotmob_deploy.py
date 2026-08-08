import copy
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts import fotmob_runtime
from scrapers.fotmob.catalog_contract import build_catalog_contract


SCRIPT = Path(__file__).resolve().parents[3] / "deploy/fotmob/deploy.py"
SPEC = importlib.util.spec_from_file_location("fotmob_deploy", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)

NEXT_SCHEDULE_BOUNDARY = {
    "logical_date": "2026-07-20T14:00:00+00:00",
    "data_interval_start": "2026-07-20T14:00:00+00:00",
    "data_interval_end": "2026-07-21T14:00:00+00:00",
    "run_after": "2026-07-21T14:00:00+00:00",
}
ADVANCED_SCHEDULE_BOUNDARY = {
    "logical_date": "2026-07-21T14:00:00+00:00",
    "data_interval_start": "2026-07-21T14:00:00+00:00",
    "data_interval_end": "2026-07-22T14:00:00+00:00",
    "run_after": "2026-07-22T14:00:00+00:00",
}


def _exact_scheduled_row(dag_id, *, state="queued", run_id=None):
    boundary = mod.validate_schedule_boundary(NEXT_SCHEDULE_BOUNDARY, label="test")
    return {
        "dag_id": dag_id,
        "run_id": run_id or mod._scheduled_run_id(boundary["logical_date"]),
        "run_type": "scheduled",
        "logical_date": boundary["logical_date"],
        "data_interval_start": boundary["data_interval_start"],
        "data_interval_end": boundary["data_interval_end"],
        "state": state,
    }


def _proved_scheduled_activation(*, state="queued", run_id=None):
    return {
        "status": "proved",
        "producer": _exact_scheduled_row(
            mod.ISOLATED_DAILY_DAG_ID, state=state, run_id=run_id
        ),
        "consumer": _exact_scheduled_row(
            mod.SHARED_CONSUMER_DAG_ID, state=state, run_id=run_id
        ),
        "exact_identity_match": True,
    }


def test_image_reference_must_be_versioned():
    for value in (
        "",
        "registry/image",
        "registry/image:latest",
        "registry/image:release-679f9c2",
        "registry/image@sha256:short",
    ):
        with pytest.raises(mod.DeploymentError):
            mod.validate_image_reference(value)
    mod.validate_image_reference("registry/image@sha256:" + "a" * 64)


def _automatic_catalog_admission(
    *,
    deployment_id: str = "f" * 32,
    git_sha: str = "a" * 40,
    scheduler_container_id: str = "1" * 64,
    now: datetime | None = None,
) -> dict:
    entities = (
        "leaderboards",
        "matches",
        "players",
        "season",
        "teams",
        "transfers",
    )
    entity_policy = {
        "match_policy": "finished_only",
        "leaderboard_policy": "all_advertised",
        "team_policy": "global_observed_snapshot",
        "player_policy": "global_observed_snapshot",
        "transfer_policy": {
            "window": "1year",
            "pagination": "unique_hits",
            "completion_scope": "included_ids",
            "completion_signature": "catalog_contract",
        },
    }
    contract = build_catalog_contract(
        catalog_batch_id="batch-1",
        catalog_content_hash="a" * 64,
        classifier_version="fotmob-men-v1",
        included_ids=[47],
        scopes=[(47, "2025 Apertura")],
        entities=entities,
        entity_policy=entity_policy,
    ).as_dict()
    now = now or datetime.now(timezone.utc)
    generation_id = "11111111-1111-1111-1111-111111111111"
    runner_report = {
        "run_id": generation_id,
        "mode": "daily",
        "status": "success",
        "complete": True,
        "completed_at": now.isoformat(),
        "transport": {"proxy_bytes": 0},
        "budget": {
            "requests": 1,
            "direct_bytes": 1024,
            "proxy_bytes": 0,
            "max_requests": 10_000,
            "max_direct_bytes": 512 * 1024 * 1024,
            "max_proxy_bytes": 0,
        },
        "selection": {
            "scope_lane": "current",
            "catalog_contract": contract,
            "entities": list(entities),
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "requests_per_minute": 60,
            "catalog_ids": [47],
            "catalog_decisions": [
                {
                    "competition_id": 47,
                    "catalog_name": "Premier League",
                    "profile_name": "Premier League",
                    "source_gender": "male",
                    "source_age_group": "adult",
                    "source_type": "league",
                    "probe_status": "success",
                    "decision": "included",
                    "reason": "structurally confirmed adult men's competition",
                    "policy_rule": "include_structural_male_adult",
                    "classifier_version": "fotmob-men-v1",
                    "profile_target_key": "leagues?id=47",
                    "profile_content_hash": "b" * 64,
                }
            ],
            "scope_plan_signature": contract["plan_signature"],
            "planned_scopes": ["47=2025 Apertura"],
            "scope_attempts": [
                {
                    "competition_id": 47,
                    "source_season_key": "2025 Apertura",
                    "plan_signature": contract["plan_signature"],
                    "attempt_count": 1,
                    "last_attempt_at": now.isoformat(),
                    "next_retry_at": None,
                    "outcome": "success",
                    "reason": "scope completed",
                    "attempt_identities": [],
                }
            ],
            "completed_transfer_competition_ids": [47],
            "transfer_plan_signature": contract["plan_signature"],
            "deferrals": [],
        },
    }
    catalog_sha = fotmob_runtime._automatic_id_digest([47], label="catalog")[1]
    decision_sha = fotmob_runtime.automatic_decision_digest(
        runner_report["selection"]["catalog_decisions"]
    )[1]
    candidate = {
        "generation_id": generation_id,
        "digest": "d" * 64,
        "transform_task_ids": ["silver_transforms.example"],
    }
    publication = {
        "generation_id": generation_id,
        "binding": {
            "schema": "fotmob-publication-v1",
            "source": "fotmob",
            "owner": "isolated",
            "data_interval_start": now.isoformat(),
            "data_interval_end": now.isoformat(),
            "runtime_fingerprint": git_sha,
        },
    }
    return {
        "schema_version": "fotmob-automatic-admission-v1",
        "validated_at": now.isoformat(),
        "classifier_version": "fotmob-men-v1",
        "contract_schema": "fotmob-catalog-v1",
        "scope_observations": {
            "table": "fotmob_competition_scope_observations",
            "table_exists": True,
            "current_view": "fotmob_competition_scope_observations_current",
            "current_view_exists": True,
            "snapshot_run_id": generation_id,
            "catalog_batch_id": contract["catalog_batch_id"],
            "catalog_content_hash": contract["catalog_content_hash"],
            "catalog_id_count": 1,
            "catalog_ids_sha256": catalog_sha,
            "decision_count": 1,
            "decision_ids_sha256": catalog_sha,
            "decision_evidence_sha256": decision_sha,
            "duplicate_decision_count": 0,
            "classifier_version": "fotmob-men-v1",
            "included_id_count": 1,
            "included_ids_sha256": contract["included_ids_sha256"],
        },
        "writer_snapshot": {
            "schema_version": "fotmob-writer-snapshot-v1",
            "transaction_id": "e" * 32,
            "observed_at": now.isoformat(),
            "pause_states": {dag_id: True for dag_id in fotmob_runtime.EXPECTED_DAGS},
            "active_runs": {},
        },
        "legacy_owners": {
            dag_id: {"schedule": None, "is_paused": True}
            for dag_id in (
                "dag_trigger_fotmob_daily",
                "dag_refresh_fotmob",
                "dag_backfill_fotmob",
            )
        },
        "lane_budgets": {
            lane: {"max_proxy_mib": 0}
            for lane in ("daily", "refresh", "backfill")
        },
        "active_writers": [],
        "current_run_reports": [runner_report],
        "canary": {
            "schema_version": "fotmob-automatic-canary-v1",
            "deployment_id": deployment_id,
            "git_sha": git_sha,
            "scheduler_container_id": scheduler_container_id,
            "generation_id": generation_id,
            "ingest_run_state": "success",
            "silver_run_state": "success",
            "candidate_digest": candidate["digest"],
            "runner_report_sha256": "c" * 64,
            "publication": publication,
            "final_publication": {
                "generation_id": generation_id,
                "status": "succeeded",
                "phase": "abandoned",
                "active": False,
                "released": True,
                "published": False,
                "candidate": candidate,
            },
        },
    }


def _automatic_activation_fixture(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    report_path = evidence_dir / "deployment.json"
    canary_path = evidence_dir / "automatic-canary.json"
    isolated = "1" * 64
    shared = "9" * 64
    immutable_handoff = {
        "shared_scheduler_container": shared,
        "shared_admission_mount": {"read_only": True},
        "runtime_code_sha256": {"dags/example.py": "e" * 64},
        "runtime_git_sha": "a" * 40,
        "control_database": {"same_shared_database": True},
        "schedule_owner": "isolated",
        "next_scheduled_interval": NEXT_SCHEDULE_BOUNDARY,
        "passed": True,
    }
    deployment = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "activation_state": "kept_paused",
        "kept_paused": True,
        "paused": sorted(mod.EXPECTED_DAGS),
        "unpaused": [],
        "deployment_id": "f" * 32,
        "git_sha": "a" * 40,
        "scheduler_container_id": isolated,
        "evidence_dir": str(evidence_dir.resolve()),
        "shared_handoff_initial": copy.deepcopy(immutable_handoff),
        "shared_handoff_final": copy.deepcopy(immutable_handoff),
        "automatic_rollout": {
            "schema_version": mod.AUTOMATIC_ROLLOUT_SCHEMA,
            "phase": "awaiting_canary",
            "scope_observation_bootstrap": {
                "table": fotmob_runtime.SCOPE_OBSERVATIONS_TABLE,
                "table_exists": True,
                "current_view": fotmob_runtime.SCOPE_OBSERVATIONS_CURRENT_VIEW,
                "current_view_exists": True,
            },
        },
    }
    report_path.write_text(json.dumps(deployment), encoding="utf-8")
    canary_path.write_text("{}", encoding="utf-8")
    args = SimpleNamespace(
        keep_paused=False,
        resume_pending=False,
        automatic_catalog=True,
        automatic_canary_report=canary_path,
        report=report_path,
        evidence_dir=evidence_dir,
        release_root=SCRIPT.parents[2],
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "fotmob.env",
        project="fotmob-airflow",
        image="registry/image@sha256:" + "b" * 64,
        postgres_image="postgres@sha256:" + "c" * 64,
    )
    return args, deployment, isolated, shared, immutable_handoff


def _automatic_rollout_certificate(admission, *, evidence_dir, handoff):
    raw_boundary = handoff.get("next_scheduled_interval") or {}
    start = datetime.fromisoformat(
        str(raw_boundary.get("data_interval_start", "2026-08-07T14:00:00+00:00"))
    )
    end = datetime.fromisoformat(
        str(raw_boundary.get("data_interval_end", "2026-08-08T14:00:00+00:00"))
    )
    boundary = {
        "schema_version": "fotmob-automatic-boundary-v1",
        "checked_at": (end - timedelta(minutes=30)).isoformat(),
        "selected_date": end.date().isoformat(),
        "state": "future",
        "data_interval_start": start.isoformat(),
        "data_interval_end": end.isoformat(),
        "safe_start": (end - timedelta(minutes=30)).isoformat(),
        "safe_cutoff": (end + timedelta(minutes=45)).isoformat(),
        "passed": True,
    }
    scheduler_state = {
        "next_background_lane": "refresh",
        "daily_date": None,
        "generation": 0,
        "updated_at": "1970-01-01T00:00:00+00:00",
    }
    all_paused = {dag_id: True for dag_id in fotmob_runtime.AUTOMATIC_DAGBAG_DAGS}
    children_paused = {
        dag_id: dag_id
        not in {"dag_ingest_fotmob", "dag_transform_fotmob_silver"}
        for dag_id in fotmob_runtime.AUTOMATIC_DAGBAG_DAGS
    }
    active_paused = {
        dag_id: dag_id in fotmob_runtime.LEGACY_OWNER_DAGS
        for dag_id in fotmob_runtime.AUTOMATIC_DAGBAG_DAGS
    }

    def isolated_tx(phase, before, after, minute):
        return {
            "schema_version": "fotmob-writer-snapshot-v1",
            "transaction_id": f"{minute:x}"[-1] * 32,
            "observed_at": (end - timedelta(minutes=minute)).isoformat(),
            "pause_states": before,
            "active_runs": {},
            "pause_states_after": after,
            "phase": phase,
            "scheduler_state": scheduler_state,
        }

    shared_before = dict(fotmob_runtime.EXPECTED_SHARED_PAUSE_STATES)
    shared_after = dict(shared_before)
    shared_after[fotmob_runtime.SHARED_CONSUMER_DAG_ID] = False

    def shared_tx(phase, before, minute):
        return {
            "schema_version": "fotmob-shared-consumer-snapshot-v1",
            "transaction_id": f"{minute:x}"[-1] * 32,
            "observed_at": (end - timedelta(minutes=minute)).isoformat(),
            "dag_id": fotmob_runtime.SHARED_CONSUMER_DAG_ID,
            "phase": phase,
            "pause_states_before": before,
            "pause_states_after": shared_after,
            "schedule_owner": "isolated",
            "active_runs": [],
        }

    return {
        "automatic_rollout": {
            "schema_version": fotmob_runtime.AUTOMATIC_ROLLOUT_SCHEMA,
            "phase": "active",
            "scope_observation_bootstrap": {
                "table": fotmob_runtime.SCOPE_OBSERVATIONS_TABLE,
                "table_exists": True,
                "current_view": fotmob_runtime.SCOPE_OBSERVATIONS_CURRENT_VIEW,
                "current_view_exists": True,
            },
            "canary_report": str(Path(evidence_dir) / "automatic-canary.json"),
        },
        "automatic_activation": {
            "fresh_shared_handoff": {**copy.deepcopy(handoff), "passed": True},
            "daily_boundary_initial": boundary,
            "daily_boundary_commit": dict(boundary),
            "quiescence_before": {"source": "fotmob", "safe": True, "active": False},
            "live_canary": {
                "runner_sha256": admission["canary"]["runner_report_sha256"],
                "runner_bytes": 123,
            },
            "children_transaction": isolated_tx(
                "children", all_paused, children_paused, 28
            ),
            "shared_consumer_unpaused": True,
            "shared_consumer_transaction": shared_tx(
                "unpause", shared_before, 27
            ),
            "shared_consumer_readback": shared_tx(
                "inspect_unpaused", shared_after, 26
            ),
            "control_quiescence_at_commit": {
                "source": "fotmob",
                "safe": True,
                "active": False,
            },
            "owner_unpaused_last": True,
            "owner_transaction": isolated_tx(
                "owner", children_paused, active_paused, 25
            ),
        },
    }


def test_automatic_catalog_admission_accepts_one_recomputed_contract():
    payload = _automatic_catalog_admission()

    assert mod.validate_automatic_catalog_admission(payload) == (
        fotmob_runtime.validate_automatic_catalog_admission(payload)
    )


def test_active_automatic_rollout_requires_complete_ordered_ceremony(tmp_path):
    _args, deployment, _isolated, _shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    admission = _automatic_catalog_admission(
        now=datetime(2026, 7, 21, 13, 35, tzinfo=timezone.utc)
    )
    certificate = _automatic_rollout_certificate(
        admission,
        evidence_dir=tmp_path / "evidence",
        handoff=handoff,
    )
    payload = {
        **deployment,
        "evidence_dir": str((tmp_path / "evidence").resolve()),
        "shared_handoff_final": handoff,
        **certificate,
    }

    result = fotmob_runtime.validate_automatic_rollout_activation(
        payload, admission
    )
    assert result["passed"] is True
    assert result["recovered"] is False

    mutations = []
    missing_activation = copy.deepcopy(payload)
    missing_activation.pop("automatic_activation")
    mutations.append(missing_activation)
    missing_owner = copy.deepcopy(payload)
    missing_owner["automatic_activation"].pop("owner_transaction")
    mutations.append(missing_owner)
    swapped_order = copy.deepcopy(payload)
    swapped_order["automatic_activation"]["owner_transaction"]["observed_at"] = (
        "2026-07-21T13:31:00+00:00"
    )
    mutations.append(swapped_order)
    stale_scheduler = copy.deepcopy(payload)
    stale_scheduler["automatic_activation"]["children_transaction"][
        "scheduler_state"
    ]["daily_date"] = payload["automatic_activation"]["daily_boundary_commit"][
        "selected_date"
    ]
    mutations.append(stale_scheduler)
    malformed_scheduler = copy.deepcopy(payload)
    malformed_scheduler["automatic_activation"]["children_transaction"][
        "scheduler_state"
    ]["updated_at"] = "not-a-date"
    mutations.append(malformed_scheduler)
    wrong_shared_interval = copy.deepcopy(payload)
    wrong_shared_interval["automatic_activation"]["fresh_shared_handoff"][
        "next_scheduled_interval"
    ]["data_interval_end"] = "2026-08-09T14:00:00+00:00"
    mutations.append(wrong_shared_interval)

    for mutated in mutations:
        with pytest.raises(fotmob_runtime.RuntimeBindingError):
            fotmob_runtime.validate_automatic_rollout_activation(
                mutated, admission
            )


def test_live_shared_runtime_rechecks_exact_container_mount_env_and_bytes(
    tmp_path, monkeypatch
):
    release = tmp_path / "release"
    release.mkdir()
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    shared_id = "9" * 64
    isolated_id = "1" * 64
    manifest = {"dags/example.py": "e" * 64}
    report_path = str(
        fotmob_runtime.SHARED_CONTAINER_EVIDENCE_ROOT / "deployment.json"
    )
    context = {
        "release_root": str(release),
        "evidence_dir": str(evidence),
        "git_sha": "a" * 40,
        "scheduler_container_id": isolated_id,
        "shared_container_report_path": report_path,
        "shared_handoff_final": {
            "shared_scheduler_container": shared_id,
            "runtime_code_sha256": manifest,
        },
    }
    isolated = {
        "Id": isolated_id,
        "State": {"Running": True},
        "Config": {"Env": ["FBREF_CONTROL_DB_URI=postgresql://control"]},
        "Mounts": [],
    }
    shared = {
        "Id": shared_id,
        "State": {"Running": True},
        "Config": {
            "Env": [
                "FBREF_CONTROL_DB_URI=postgresql://control",
                "FOTMOB_DEPLOY_GIT_SHA=" + "a" * 40,
                "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH=" + report_path,
            ]
        },
        "Mounts": [
            {
                "Source": str(evidence.resolve()),
                "Destination": str(
                    fotmob_runtime.SHARED_CONTAINER_EVIDENCE_ROOT
                ),
                "RW": False,
            }
        ],
    }
    containers = {shared_id: shared, isolated_id: isolated}
    monkeypatch.setattr(
        fotmob_runtime,
        "_inspect_container",
        lambda container_id, **_kwargs: copy.deepcopy(containers[container_id]),
    )
    monkeypatch.setattr(
        fotmob_runtime, "shared_runtime_manifest", lambda _release: manifest
    )

    def run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_ACTIVE_SHARED_MANIFEST_JSON=" + json.dumps(manifest),
            stderr="",
        )

    assert fotmob_runtime.validate_live_shared_runtime(context, run=run)[
        "passed"
    ] is True

    mutations = []
    wrong_id = copy.deepcopy(containers)
    wrong_id[shared_id]["Id"] = "8" * 64
    mutations.append(wrong_id)
    stopped = copy.deepcopy(containers)
    stopped[shared_id]["State"]["Running"] = False
    mutations.append(stopped)
    wrong_control = copy.deepcopy(containers)
    wrong_control[shared_id]["Config"]["Env"][0] = (
        "FBREF_CONTROL_DB_URI=postgresql://other"
    )
    mutations.append(wrong_control)
    wrong_git = copy.deepcopy(containers)
    wrong_git[shared_id]["Config"]["Env"][1] = "FOTMOB_DEPLOY_GIT_SHA=" + "b" * 40
    mutations.append(wrong_git)
    writable_mount = copy.deepcopy(containers)
    writable_mount[shared_id]["Mounts"][0]["RW"] = True
    mutations.append(writable_mount)
    wrong_mount = copy.deepcopy(containers)
    wrong_mount[shared_id]["Mounts"][0]["Source"] = str(tmp_path / "other")
    mutations.append(wrong_mount)

    for mutated in mutations:
        monkeypatch.setattr(
            fotmob_runtime,
            "_inspect_container",
            lambda container_id, _items=mutated, **_kwargs: copy.deepcopy(
                _items[container_id]
            ),
        )
        with pytest.raises(fotmob_runtime.RuntimeBindingError):
            fotmob_runtime.validate_live_shared_runtime(context, run=run)

    monkeypatch.setattr(
        fotmob_runtime,
        "_inspect_container",
        lambda container_id, **_kwargs: copy.deepcopy(containers[container_id]),
    )
    context_drift = copy.deepcopy(context)
    context_drift["shared_handoff_final"]["runtime_code_sha256"] = {
        "dags/example.py": "d" * 64
    }
    with pytest.raises(fotmob_runtime.RuntimeBindingError, match="manifest"):
        fotmob_runtime.validate_live_shared_runtime(context_drift, run=run)

    def drifted_run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_ACTIVE_SHARED_MANIFEST_JSON="
            + json.dumps({"dags/example.py": "d" * 64}),
            stderr="",
        )

    with pytest.raises(fotmob_runtime.RuntimeBindingError, match="bytes"):
        fotmob_runtime.validate_live_shared_runtime(context, run=drifted_run)


def test_automatic_catalog_admission_rejects_self_consistent_but_stale_evidence():
    payload = _automatic_catalog_admission()
    stale = "2020-01-01T00:00:00+00:00"
    payload["validated_at"] = stale
    report = payload["current_run_reports"][0]
    report["completed_at"] = stale
    report["selection"]["scope_attempts"][0]["last_attempt_at"] = stale

    with pytest.raises(mod.DeploymentError, match="stale|old|72"):
        mod.validate_automatic_catalog_admission(payload)


def test_automatic_catalog_admission_rejects_stale_canary_with_fresh_stamp():
    payload = _automatic_catalog_admission()
    payload["current_run_reports"][0]["completed_at"] = (
        "2020-01-01T00:00:00+00:00"
    )

    with pytest.raises(mod.DeploymentError, match="canary.*stale"):
        mod.validate_automatic_catalog_admission(payload)


def test_automatic_catalog_admission_rejects_alternate_entity_policy():
    payload = _automatic_catalog_admission()
    report = payload["current_run_reports"][0]
    selection = report["selection"]
    policy = copy.deepcopy(selection["catalog_contract"]["entity_policy"])
    policy["team_policy"] = "incremental"
    contract = build_catalog_contract(
        catalog_batch_id="batch-1",
        catalog_content_hash="a" * 64,
        classifier_version="fotmob-men-v1",
        included_ids=[47],
        scopes=[(47, "2025 Apertura")],
        entities=selection["entities"],
        entity_policy=policy,
    ).as_dict()
    selection["catalog_contract"] = contract
    selection["scope_plan_signature"] = contract["plan_signature"]
    selection["scope_attempts"][0]["plan_signature"] = contract["plan_signature"]
    selection["transfer_plan_signature"] = contract["plan_signature"]

    with pytest.raises(mod.DeploymentError, match="unsafe entity profile"):
        mod.validate_automatic_catalog_admission(payload)


def test_automatic_admission_imports_work_for_direct_scripts_outside_repo(tmp_path):
    admission = tmp_path / "admission.json"
    admission.write_text(json.dumps(_automatic_catalog_admission()), encoding="utf-8")
    scripts = SCRIPT.parents[2] / "scripts"
    code = (
        "import json,sys; "
        f"sys.path.insert(0,{str(scripts)!r}); "
        "import fotmob_runtime; "
        f"p=json.load(open({str(admission)!r},encoding='utf-8')); "
        "assert fotmob_runtime.validate_automatic_catalog_admission(p)['passed']"
    )

    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda value: value.update({"classifier_version": "fotmob-men-v2"}),
            "classifier",
        ),
        (
            lambda value: value["scope_observations"].update(
                {"current_view_exists": False}
            ),
            "scope-observation",
        ),
        (
            lambda value: value["legacy_owners"][
                "dag_refresh_fotmob"
            ].update({"schedule": "@continuous", "is_paused": False}),
            "legacy owner",
        ),
        (
            lambda value: value["current_run_reports"][0].update(
                {"mode": "refresh"}
            ),
            "daily/current",
        ),
        (
            lambda value: value["lane_budgets"]["daily"].update(
                {"max_proxy_mib": 1}
            ),
            "proxy",
        ),
        (
            lambda value: value["current_run_reports"][0]["budget"].update(
                {"max_requests": 9_999}
            ),
            "daily.*budget",
        ),
        (
            lambda value: value["current_run_reports"][0]["selection"].update(
                {"requests_per_minute": 59}
            ),
            "daily.*profile",
        ),
        (
            lambda value: value["active_writers"].append(
                {
                    "dag_id": "dag_orchestrate_fotmob",
                    "run_id": "scheduled__2026-08-08T12:00:00+00:00",
                    "state": "running",
                }
            ),
            "active writer",
        ),
        (
            lambda value: value["current_run_reports"][0]["selection"].update(
                {"catalog_decisions": []}
            ),
            "dynamic acceptance",
        ),
    ),
)
def test_automatic_catalog_admission_fails_closed(mutation, message):
    payload = _automatic_catalog_admission()
    mutation(payload)

    with pytest.raises(mod.DeploymentError, match=message):
        mod.validate_automatic_catalog_admission(payload)


def test_automatic_catalog_admission_rejects_any_second_or_legacy_report():
    payload = _automatic_catalog_admission()
    payload["current_run_reports"].append(
        {
            "selection": {
                "daily_contract": "fotmob-daily-v1",
                "competition_scope": {"sha256": "b" * 64},
            },
            "transport": {"proxy_bytes": 0},
            "budget": {"proxy_bytes": 0, "max_proxy_bytes": 0},
        }
    )
    with pytest.raises(mod.DeploymentError, match="exactly one canary"):
        mod.validate_automatic_catalog_admission(payload)

    payload = _automatic_catalog_admission()
    second = copy.deepcopy(payload["current_run_reports"][0])
    second_contract = build_catalog_contract(
        catalog_batch_id="batch-2",
        catalog_content_hash="b" * 64,
        classifier_version="fotmob-men-v1",
        included_ids=[47],
        scopes=[(47, "2025 Apertura")],
        entities=payload["current_run_reports"][0]["selection"]["entities"],
        entity_policy=payload["current_run_reports"][0]["selection"][
            "catalog_contract"
        ]["entity_policy"],
    ).as_dict()
    second["selection"]["catalog_contract"] = second_contract
    second["selection"]["scope_plan_signature"] = second_contract[
        "plan_signature"
    ]
    second["selection"]["scope_attempts"][0]["plan_signature"] = second_contract[
        "plan_signature"
    ]
    second["selection"]["transfer_plan_signature"] = second_contract[
        "plan_signature"
    ]
    payload["current_run_reports"].append(second)
    with pytest.raises(mod.DeploymentError, match="exactly one canary"):
        mod.validate_automatic_catalog_admission(payload)


def test_scope_observation_bootstrap_creates_table_and_current_view_before_cutover():
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "FOTMOB_AUTOMATIC_SCOPE_JSON="
                + json.dumps(_automatic_catalog_admission()["scope_observations"])
            ),
            stderr="",
        )

    evidence = mod.bootstrap_automatic_scope_observations("1" * 64, run=run)

    assert evidence["table_exists"] is True
    assert evidence["current_view_exists"] is True
    assert calls[0][:3] == ("docker", "exec", "1" * 64)
    code = calls[0][-1]
    assert "ensure_schema" in code
    assert "ensure_current_views" in code
    assert "r.schema" in code
    assert "created" in code
    assert "fotmob_competition_scope_observations_current" in code
    compile(code, "<automatic-scope-bootstrap>", "exec")


def test_scope_observation_bootstrap_rejects_missing_view():
    evidence = _automatic_catalog_admission()["scope_observations"]
    evidence["current_view_exists"] = False

    def run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_AUTOMATIC_SCOPE_JSON=" + json.dumps(evidence),
            stderr="",
        )

    with pytest.raises(mod.DeploymentError, match="scope-observation"):
        mod.bootstrap_automatic_scope_observations("1" * 64, run=run)


def test_database_password_must_be_safe_for_sqlalchemy_uri(tmp_path):
    env_file = tmp_path / "fotmob.env"
    env_file.write_text("FOTMOB_AIRFLOW_DB_PASSWORD=hex_abc-123\n")
    mod.validate_database_password(env_file, {})
    env_file.write_text("FOTMOB_AIRFLOW_DB_PASSWORD=bad@host:5432/value\n")
    with pytest.raises(mod.DeploymentError, match="URL-safe"):
        mod.validate_database_password(env_file, {})


def test_delivery_credentials_are_required_without_exposing_values(tmp_path):
    env_file = tmp_path / "fotmob.env"
    env_file.write_text("TELEGRAM_BOT_TOKEN=\nTELEGRAM_CHAT_ID=chat\n")
    with pytest.raises(mod.DeploymentError, match="TELEGRAM_BOT_TOKEN") as exc_info:
        mod.validate_delivery_credentials(env_file, {})
    assert "chat" not in str(exc_info.value)

    env_file.write_text("TELEGRAM_BOT_TOKEN=secret\nTELEGRAM_CHAT_ID=chat\n")
    mod.validate_delivery_credentials(env_file, {})


def test_parse_airflow_json_tolerates_log_prefix():
    rows = mod.parse_airflow_json(
        '[2026-07-21T10:06:03] INFO loading\n[{"dag_id":"dag_ingest_fotmob"}]\n'
    )
    assert rows == [{"dag_id": "dag_ingest_fotmob"}]


def test_schedule_boundary_requires_exact_matching_automated_interval():
    proof = mod.validate_matching_schedule_boundaries(
        shared_initial=NEXT_SCHEDULE_BOUNDARY,
        shared_final=NEXT_SCHEDULE_BOUNDARY,
        isolated_initial=NEXT_SCHEDULE_BOUNDARY,
        isolated_final=NEXT_SCHEDULE_BOUNDARY,
    )
    assert proof["exact_match"] is True
    assert proof["shared_initial"] == proof["isolated_final"]

    different = dict(NEXT_SCHEDULE_BOUNDARY)
    different["logical_date"] = "2026-07-21T14:00:00+00:00"
    different["data_interval_start"] = "2026-07-21T14:00:00+00:00"
    different["data_interval_end"] = "2026-07-22T14:00:00+00:00"
    different["run_after"] = "2026-07-22T14:00:00+00:00"
    with pytest.raises(mod.DeploymentError, match="different next scheduled intervals"):
        mod.validate_matching_schedule_boundaries(
            shared_initial=NEXT_SCHEDULE_BOUNDARY,
            shared_final=NEXT_SCHEDULE_BOUNDARY,
            isolated_initial=different,
            isolated_final=different,
        )


@pytest.mark.parametrize(
    ("raw", "accepted"),
    (
        (NEXT_SCHEDULE_BOUNDARY, True),
        (
            {
                "logical_date": "2026-07-20T16:00:00+02:00",
                "data_interval_start": "2026-07-20T16:00:00+02:00",
                "data_interval_end": "2026-07-21T16:00:00+02:00",
                "run_after": "2026-07-21T16:00:00+02:00",
            },
            True,
        ),
        (
            {
                key: value
                for key, value in NEXT_SCHEDULE_BOUNDARY.items()
                if key != "run_after"
            },
            False,
        ),
        ({**NEXT_SCHEDULE_BOUNDARY, "extra": "forged"}, False),
        ({**NEXT_SCHEDULE_BOUNDARY, "logical_date": "2026-07-20T14:00:00"}, False),
        ({**NEXT_SCHEDULE_BOUNDARY, "logical_date": "not-a-timestamp"}, False),
        (
            {
                **NEXT_SCHEDULE_BOUNDARY,
                "logical_date": "2026-07-20T13:59:59+00:00",
            },
            False,
        ),
        (
            {
                **NEXT_SCHEDULE_BOUNDARY,
                "data_interval_end": "2026-07-20T13:00:00+00:00",
                "run_after": "2026-07-20T13:00:00+00:00",
            },
            False,
        ),
        (
            {
                **NEXT_SCHEDULE_BOUNDARY,
                "run_after": "2026-07-21T14:00:01+00:00",
            },
            False,
        ),
        (None, False),
    ),
)
def test_deploy_and_runtime_schedule_validators_have_adversarial_parity(raw, accepted):
    outcomes = []
    for validator, error in (
        (
            lambda value: mod.validate_schedule_boundary(value, label="parity"),
            mod.DeploymentError,
        ),
        (
            lambda value: fotmob_runtime._normalize_schedule_boundary(
                value, label="parity"
            ),
            fotmob_runtime.RuntimeBindingError,
        ),
    ):
        try:
            outcomes.append((True, validator(raw)))
        except error:
            outcomes.append((False, None))

    assert outcomes[0][0] is outcomes[1][0] is accepted
    if accepted:
        assert outcomes[0][1] == outcomes[1][1]


def test_schedule_boundary_reader_uses_exact_paused_dagmodel_fields():
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "log prefix\nFOTMOB_SCHEDULE_BOUNDARY_JSON="
                + json.dumps({"is_paused": True, "boundary": NEXT_SCHEDULE_BOUNDARY})
            ),
            stderr="",
        )

    result = mod.read_schedule_boundary(
        "1" * 64,
        "dag_trigger_fotmob_daily",
        run=run,
    )
    assert result["logical_date"] == "2026-07-20T14:00:00.000000+00:00"
    assert calls[0][:3] == ("docker", "exec", "1" * 64)
    code = calls[0][-1]
    assert "next_dagrun_data_interval_start" in code
    assert "next_dagrun_data_interval_end" in code
    compile(code, "<schedule-boundary-proof>", "exec")


def test_schedule_boundary_reader_rejects_unpaused_commit_snapshot():
    def run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_SCHEDULE_BOUNDARY_JSON="
            + json.dumps({"is_paused": False, "boundary": NEXT_SCHEDULE_BOUNDARY}),
            stderr="",
        )

    with pytest.raises(mod.DeploymentError, match="not paused"):
        mod.read_schedule_boundary(
            "1" * 64,
            mod.ISOLATED_DAILY_DAG_ID,
            run=run,
        )


@pytest.mark.parametrize(
    ("timeout_seconds", "safe_now", "unsafe_now", "required_seconds"),
    (
        (
            100,
            datetime(2026, 7, 21, 13, 44, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 21, 13, 45, 1, tzinfo=timezone.utc),
            15 * 60,
        ),
        (
            1_200,
            datetime(2026, 7, 21, 13, 35, tzinfo=timezone.utc),
            datetime(2026, 7, 21, 13, 35, 1, tzinfo=timezone.utc),
            1_200 + 5 * 60,
        ),
    ),
)
def test_activation_safety_window_uses_larger_floor_or_timeout_margin(
    timeout_seconds, safe_now, unsafe_now, required_seconds
):
    proof = mod.validate_activation_safety_window(
        NEXT_SCHEDULE_BOUNDARY,
        timeout_seconds=timeout_seconds,
        now=safe_now,
    )
    assert proof["required_seconds"] == required_seconds
    assert proof["remaining_seconds"] >= required_seconds

    with pytest.raises(mod.DeploymentError, match="too close"):
        mod.validate_activation_safety_window(
            NEXT_SCHEDULE_BOUNDARY,
            timeout_seconds=timeout_seconds,
            now=unsafe_now,
        )


def test_exact_scheduled_run_rejects_forged_run_id():
    forged = {
        "run_id": "scheduled__forged",
        "expected_run_id": "scheduled__2026-07-20T14:00:00+00:00",
        "run_type": "scheduled",
        "logical_date": NEXT_SCHEDULE_BOUNDARY["logical_date"],
        "data_interval_start": NEXT_SCHEDULE_BOUNDARY["data_interval_start"],
        "data_interval_end": NEXT_SCHEDULE_BOUNDARY["data_interval_end"],
        "state": "queued",
    }
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_SCHEDULED_RUNS_JSON=" + json.dumps([forged]),
            stderr="",
        )

    with pytest.raises(mod.DeploymentError, match="scheduled DagRun identity"):
        mod.read_exact_scheduled_run(
            "1" * 64,
            mod.ISOLATED_DAILY_DAG_ID,
            NEXT_SCHEDULE_BOUNDARY,
            run=run,
        )
    assert "DagRun.generate_run_id(DagRunType.SCHEDULED,r.logical_date)" in calls[0][-1]
    assert "DagRun.execution_date.desc()" in calls[0][-1]
    assert "DagRun.logical_date.desc()" not in calls[0][-1]
    compile(calls[0][-1], "<scheduled-run-proof>", "exec")


def test_exact_scheduled_run_rejects_unknown_or_empty_state():
    for state in ("", "up_for_retry", "removed"):
        row = {
            **_exact_scheduled_row(mod.ISOLATED_DAILY_DAG_ID, state=state),
            "expected_run_id": mod._scheduled_run_id(
                NEXT_SCHEDULE_BOUNDARY["logical_date"]
            ),
        }

        def run(command, **_kwargs):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="FOTMOB_SCHEDULED_RUNS_JSON=" + json.dumps([row]),
                stderr="",
            )

        with pytest.raises(mod.DeploymentError, match="invalid state"):
            mod.read_exact_scheduled_run(
                "1" * 64,
                mod.ISOLATED_DAILY_DAG_ID,
                NEXT_SCHEDULE_BOUNDARY,
                run=run,
            )


def test_delivery_runtime_proof_records_only_presence_booleans():
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                'FOTMOB_DELIVERY_ENV_JSON={"telegram_bot_token_configured":true,'
                '"telegram_chat_id_configured":true}\n'
            ),
            stderr="",
        )

    assert mod.validate_delivery_runtime("1" * 64, run=run) == {
        "telegram_bot_token_configured": True,
        "telegram_chat_id_configured": True,
    }
    assert "secret" not in calls[0][-1].casefold()
    compile(calls[0][-1], "<delivery-presence-proof>", "exec")


def test_dagbag_requires_exact_three_and_zero_import_errors():
    rows = [{"dag_id": dag_id} for dag_id in mod.EXPECTED_DAGS]
    mod.validate_dagbag(rows, ())
    with pytest.raises(mod.DeploymentError, match="unexpected DagBag"):
        mod.validate_dagbag(rows[:-1], ())
    with pytest.raises(mod.DeploymentError, match="import error"):
        mod.validate_dagbag(rows, ({"filepath": "broken.py"},))


def test_compose_masks_image_dags_with_exact_projection():
    compose = SCRIPT.with_name("airflow.compose.yaml").read_text()
    assert "source: ${FOTMOB_DAGBAG_ROOT" in compose
    assert "target: /opt/airflow/dags" in compose
    assert "${FOTMOB_RELEASE_ROOT" in compose
    assert "/configs/fotmob" in compose
    assert "${FOTMOB_POSTGRES_IMAGE" in compose
    assert "postgres:16-alpine" not in compose
    assert compose.count("target: /opt/airflow/configs/fotmob") == 1
    assert "name: fotmob_airflow_metadata" not in compose
    assert "FBREF_CONTROL_DB_URI: ${FBREF_CONTROL_DB_URI:?" in compose
    assert 'FOTMOB_ISOLATED_STACK: "1"' in compose
    assert "FOTMOB_DEPLOYMENT_REPORT_PATH: ${FOTMOB_DEPLOYMENT_REPORT_PATH:?" in compose
    assert "TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:?" in compose
    assert "TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID:?" in compose
    assert "name: dp-backend" in compose
    for dag_id in mod.EXPECTED_DAGS:
        assert f"airflow dags pause {dag_id}" in compose


def test_shared_compose_requires_control_db_and_release_sha_contract():
    root = SCRIPT.parents[2]
    compose = (root / "compose.yaml").read_text(encoding="utf-8")
    example = (root / ".env.example").read_text(encoding="utf-8")

    assert "FBREF_CONTROL_DB_URI: ${FBREF_CONTROL_DB_URI:?" in compose
    assert "FOTMOB_DEPLOY_GIT_SHA: ${FOTMOB_DEPLOY_GIT_SHA:?" in compose
    assert "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH:" in compose
    assert "source: ${FOTMOB_SHARED_ADMISSION_HOST_DIR:" in compose
    assert "target: /opt/airflow/fotmob-admission" in compose
    assert "read_only: true" in compose
    assert "./configs/fotmob:/opt/airflow/configs/fotmob:ro" in compose
    assert "FOTMOB_ISOLATED_STACK" not in compose
    assert (
        "FBREF_CONTROL_DB_URI=postgresql://airflow:"
        "%3Cyour-airflow-db-password%3E@postgres:5432/airflow"
    ) in example
    assert "FOTMOB_DEPLOY_GIT_SHA=" + "0" * 40 in example
    assert "FOTMOB_SHARED_ADMISSION_HOST_DIR=" in example
    assert (
        "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH="
        "/opt/airflow/fotmob-admission/deployment.json"
    ) in example

    document = yaml.safe_load(compose)
    assert document["services"]["airflow-scheduler"]["environment"][
        "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH"
    ] == (
        "${FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH:?set exact report path "
        "under /opt/airflow/fotmob-admission}"
    )
    scheduler_mounts = {
        mount["target"]: mount
        for mount in document["services"]["airflow-scheduler"]["volumes"]
        if isinstance(mount, dict)
    }
    assert scheduler_mounts["/opt/airflow/configs/fotmob"] == {
        "type": "bind",
        "source": "./configs/fotmob",
        "target": "/opt/airflow/configs/fotmob",
        "read_only": True,
        "bind": {"create_host_path": False},
    }
    assert scheduler_mounts["/opt/airflow/fotmob-admission"] == {
        "type": "bind",
        "source": (
            "${FOTMOB_SHARED_ADMISSION_HOST_DIR:?set the absolute FotMob "
            "evidence directory}"
        ),
        "target": "/opt/airflow/fotmob-admission",
        "read_only": True,
        "bind": {"create_host_path": False},
    }


def test_prepare_dagbag_contains_exact_root_files_and_detects_tampering(tmp_path):
    release = tmp_path / "release"
    evidence = tmp_path / "evidence"
    for relative in (
        "dags/dag_ingest_fotmob.py",
        "dags/dag_orchestrate_fotmob.py",
        "dags/dag_refresh_fotmob.py",
        "dags/dag_backfill_fotmob.py",
        "dags/dag_transform_fotmob_silver.py",
        "dags/dag_trigger_fotmob_daily.py",
        "deploy/fotmob/.airflowignore",
    ):
        path = release / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative)
    projection = mod.prepare_dagbag(release, evidence, "a" * 40)
    assert {path.name for path in projection.iterdir()} == {
        "dag_orchestrate_fotmob.py",
        "dag_ingest_fotmob.py",
        "dag_transform_fotmob_silver.py",
        "dag_trigger_fotmob_daily.py",
        "dag_refresh_fotmob.py",
        "dag_backfill_fotmob.py",
        ".airflowignore",
        "utils",
        "sql",
        "scripts",
    }
    projection.chmod(0o755)
    projection.joinpath("dag_ingest_fotmob.py").chmod(0o644)
    projection.joinpath("dag_ingest_fotmob.py").write_text("tampered")
    with pytest.raises(mod.DeploymentError, match="drifted"):
        mod.prepare_dagbag(release, evidence, "a" * 40)


def test_fresh_dagbag_requires_exact_file_locations_and_schedules():
    payload = {
        "dags": {
            dag_id: {
                "fileloc": mod.EXPECTED_DAG_FILES[dag_id],
                "schedule": mod.EXPECTED_SCHEDULES[dag_id],
            }
            for dag_id in mod.EXPECTED_DAGS
        },
        "import_errors": {},
    }
    mod.validate_fresh_dagbag(payload)
    payload["dags"]["dag_trigger_fotmob_daily"]["schedule"] = "@daily"
    with pytest.raises(mod.DeploymentError, match="unexpected schedule"):
        mod.validate_fresh_dagbag(payload)


def _shared_runtime_digests(root):
    relative_paths = {
        *mod.SHARED_REQUIRED_RUNTIME_PATHS,
        "configs/medallion/competitions.yaml",
        "scripts/runtime_marker.py",
    }
    for relative_path in relative_paths:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative_path in {
            mod.APPROVED_SCOPE_PATH,
            mod.PLAYER_SOURCE_REFRESH_PATH,
        }:
            path.write_bytes((SCRIPT.parents[2] / relative_path).read_bytes())
        else:
            path.write_text(str(path.relative_to(root)))
    return mod.shared_runtime_manifest(root)


def test_expected_isolated_manifest_is_exact_effective_projection(tmp_path):
    shared = _shared_runtime_digests(tmp_path)
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    dagbag.joinpath(".airflowignore").write_text("^utils/\n")

    manifest = mod.expected_isolated_runtime_manifest(tmp_path, dagbag)

    assert (
        manifest["dags/dag_trigger_fotmob_daily.py"]
        == shared["dags/dag_trigger_fotmob_daily.py"]
    )
    assert (
        manifest["dags/utils/fotmob_publication.py"]
        == shared["dags/utils/fotmob_publication.py"]
    )
    assert "dags/dag_master_pipeline.py" not in manifest
    assert manifest["dags/.airflowignore"] == mod._sha256(dagbag / ".airflowignore")


def test_isolated_container_manifest_must_match_exact_paths_and_bytes():
    expected = {
        "dags/.airflowignore": "a" * 64,
        "dags/dag_trigger_fotmob_daily.py": "b" * 64,
    }
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON=" + json.dumps(expected) + "\n"
            ),
            stderr="",
        )

    assert mod.validate_isolated_runtime_manifest("1" * 64, expected, run=run) == (
        expected
    )
    assert calls[0][:3] == ("docker", "exec", "1" * 64)
    compile(calls[0][-1], "<isolated-manifest-proof>", "exec")

    stale = dict(expected)
    stale["dags/dag_trigger_fotmob_daily.py"] = "c" * 64

    def stale_run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON=" + json.dumps(stale),
            stderr="",
        )

    with pytest.raises(mod.DeploymentError, match="bind-mounted runtime differs"):
        mod.validate_isolated_runtime_manifest("1" * 64, expected, run=stale_run)


def _orchestration_payload(
    *,
    active_runs=None,
    pause_states=None,
    safe_edges=True,
    safe_xref=True,
    safe_downstream=True,
    daily_present=False,
    daily_paused=None,
    schedule_boundary=None,
):
    xref_writers = [
        "xref_transforms.xref_team",
        "xref_transforms.xref_referee",
        "xref_transforms.xref_match",
        "xref_transforms.xref_manager",
        "xref_player",
    ]
    xref_tail = ["validate_xref", "end_marker"]

    def downstream_proof(dag_id, first_task, *, has_start):
        tail_task = f"{dag_id}.terminal"
        task_ids = [
            "validate_fotmob_publication_consumer",
            first_task,
            tail_task,
        ]
        if has_start:
            task_ids.insert(0, "start_marker")
        return {
            "present": True,
            "fileloc": f"/opt/airflow/dags/{dag_id}.py",
            "task_ids": task_ids,
            "start_present": has_start,
            "start_downstream": (
                ["validate_fotmob_publication_consumer"] if has_start else []
            ),
            "preflight_present": True,
            "preflight_upstream": ["start_marker"] if has_start else [],
            "preflight_downstream": [first_task],
            "preflight_descendants": (
                [first_task, tail_task] if safe_downstream else [tail_task]
            ),
            "preflight_trigger_rule": "all_success",
            "direct_downstream_trigger_rules": {first_task: "all_success"},
        }

    return {
        "master": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_master_pipeline.py",
            "gate_present": True,
            "trigger_upstream": ["ingestion_triggers.fotmob_shared_schedule_owner"],
        },
        "sofascore": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_sofascore_pipeline.py",
            "sensor_present": True,
            "xref_present": True,
            "e4_present": True,
            "finalizer_present": True,
            "sensor_downstream": (["trigger_xref_transforms"] if safe_edges else []),
            "xref_upstream": (["wait_for_fotmob_publication"] if safe_edges else []),
            "e4_downstream": ["finalize_fotmob_publication"],
            "finalizer_upstream": [
                "trigger_e4_transforms",
                "wait_for_fotmob_publication",
            ],
            "finalizer_trigger_rule": "all_done",
        },
        "xref": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_transform_xref.py",
            "task_ids": [
                "start_marker",
                "validate_fotmob_publication_consumer",
                *xref_writers,
                *xref_tail,
            ],
            "start_present": True,
            "preflight_present": True,
            "start_downstream": ["validate_fotmob_publication_consumer"],
            "preflight_upstream": ["start_marker"],
            "preflight_descendants": (
                [*xref_writers, *xref_tail] if safe_xref else xref_tail
            ),
            "preflight_trigger_rule": "all_success",
            "task_trigger_rules": {task_id: "all_success" for task_id in xref_writers},
        },
        "fenced_downstream": {
            "dag_transform_e3": downstream_proof(
                "dag_transform_e3",
                "silver_e3.whoscored_events_spadl",
                has_start=True,
            ),
            "dag_transform_e4": downstream_proof(
                "dag_transform_e4",
                "silver_e4.matchhistory_match_odds",
                has_start=True,
            ),
            "dag_transform_fbref_gold": downstream_proof(
                "dag_transform_fbref_gold",
                "transfermarkt_reader_precondition",
                has_start=False,
            ),
        },
        "pause_states": pause_states
        or {
            "dag_master_pipeline": True,
            "dag_sofascore_pipeline": True,
            "dag_ingest_fotmob": True,
            "dag_transform_fotmob_silver": True,
        },
        "sofascore_schedule_boundary": (
            dict(NEXT_SCHEDULE_BOUNDARY)
            if schedule_boundary is None
            else schedule_boundary
        ),
        "schedule_owner": "isolated",
        "shared_daily_trigger": {
            "isolated_stack_env": None,
            "serialized_present": daily_present,
            "serialized_fileloc": (
                "/opt/airflow/dags/dag_trigger_fotmob_daily.py"
                if daily_present
                else None
            ),
            "dag_model_present": daily_present,
            "dag_model_paused": daily_paused if daily_present else None,
        },
        "active_runs": active_runs or [],
    }


def _shared_handoff_runner(
    root,
    orchestration,
    *,
    omitted_runtime_path=None,
    stale_runtime_path=None,
    mount_source=None,
    mount_type="bind",
    mount_rw=False,
    extra_mounts=(),
    report_path="/opt/airflow/fotmob-admission/deployment.json",
):
    remote_digests = _shared_runtime_digests(root)
    if omitted_runtime_path is not None:
        remote_digests.pop(omitted_runtime_path)
    if stale_runtime_path is not None:
        remote_digests[stale_runtime_path] = "f" * 64

    def run(command, **_kwargs):
        run.calls.append(command)
        if command[:4] == ("docker", "inspect", "--format", "{{.Id}}"):
            stdout = "9" * 64 + "\n"
        elif command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{json .Mounts}}",
        ):
            stdout = json.dumps(
                [
                    {
                        "Type": mount_type,
                        "Source": str(mount_source or root.resolve()),
                        "Destination": "/opt/airflow/fotmob-admission",
                        "RW": mount_rw,
                    },
                    *extra_mounts,
                ]
            )
        elif command[-2:] == ("printenv", "FBREF_CONTROL_DB_URI"):
            stdout = "postgresql://control@postgres/control\n"
        elif command[-2:] == (
            "printenv",
            "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH",
        ):
            stdout = report_path + "\n"
        elif command[-2:] == ("printenv", "FOTMOB_DEPLOY_GIT_SHA"):
            stdout = "a" * 40 + "\n"
        elif command[:4] == ("git", "-C", str(root), "rev-parse"):
            stdout = "a" * 40 + "\n"
        elif "FOTMOB_CONTROL_DB_JSON=" in command[-1]:
            stdout = (
                'FOTMOB_CONTROL_DB_JSON={"status":"passed","versions":[1],'
                '"checksum_verified":true}\n'
            )
        elif "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" in command[-1]:
            stdout = "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" + json.dumps(remote_digests)
        elif "FOTMOB_SHARED_ORCHESTRATION_JSON=" in command[-1]:
            stdout = "FOTMOB_SHARED_ORCHESTRATION_JSON=" + json.dumps(orchestration)
        else:
            raise AssertionError(f"unexpected command: {command}")
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    run.calls = []
    return run


def _validate_shared_handoff(
    release_root, shared_container, expected_control_uri, *, run
):
    return mod.validate_shared_handoff(
        release_root,
        shared_container,
        expected_control_uri,
        evidence_dir=release_root,
        report_relative_path=Path("deployment.json"),
        run=run,
    )


def test_shared_handoff_proves_production_orchestrator_and_no_running_run(tmp_path):
    runner = _shared_handoff_runner(tmp_path, _orchestration_payload())
    evidence = _validate_shared_handoff(
        tmp_path,
        "shared-scheduler",
        "postgresql://control@postgres/control",
        run=runner,
    )
    assert evidence["passed"] is True
    assert evidence["shared_scheduler_container"] == "9" * 64
    assert evidence["shared_admission_mount"] == {
        "type": "bind",
        "source": str(tmp_path.resolve()),
        "destination": "/opt/airflow/fotmob-admission",
        "read_only": True,
        "report_path": "/opt/airflow/fotmob-admission/deployment.json",
    }
    assert all(
        command[2] == "9" * 64
        for command in runner.calls
        if command[:2] == ("docker", "exec")
    )
    assert evidence["schedule_owner"] == "isolated"
    assert evidence["orchestration_state"]["pause_states"] == {
        "dag_master_pipeline": True,
        "dag_sofascore_pipeline": True,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": True,
    }
    assert evidence["next_scheduled_interval"] == {
        key: value.replace("+00:00", ".000000+00:00")
        for key, value in NEXT_SCHEDULE_BOUNDARY.items()
    }
    assert "configs/fotmob/competitions.json" in evidence["runtime_code_sha256"]
    assert "scrapers/fotmob/service.py" in evidence["runtime_code_sha256"]
    assert (
        "dags/sql/silver/fotmob_player_profile.sql" in evidence["runtime_code_sha256"]
    )
    assert evidence["runtime_code_sha256"] == mod.shared_runtime_manifest(tmp_path)
    assert mod.SHARED_RUNTIME_ROOTS == fotmob_runtime.SHARED_RUNTIME_ROOTS
    assert mod.SHARED_RUNTIME_SUFFIXES == fotmob_runtime.SHARED_RUNTIME_SUFFIXES
    assert mod.SHARED_REQUIRED_RUNTIME_PATHS == (
        fotmob_runtime.SHARED_REQUIRED_RUNTIME_PATHS
    )
    assert mod.APPROVED_SCOPE_SHA256 == fotmob_runtime.APPROVED_SCOPE_SHA256
    assert set(evidence["active_run_checks"]) == fotmob_runtime.SHARED_STATE_DAGS
    manifest_commands = [
        command
        for command in runner.calls
        if "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" in command[-1]
    ]
    assert len(manifest_commands) == 1
    compile(manifest_commands[0][-1], "<shared-runtime-manifest>", "exec")
    orchestration_commands = [
        command
        for command in runner.calls
        if "FOTMOB_SHARED_ORCHESTRATION_JSON=" in command[-1]
    ]
    assert len(orchestration_commands) == 1
    compile(orchestration_commands[0][-1], "<shared-orchestration-proof>", "exec")
    proof_code = orchestration_commands[0][-1]
    assert "REPEATABLE READ, READ ONLY" in proof_code
    assert "Variable.key == 'fotmob_schedule_owner'" in proof_code


@pytest.mark.parametrize(
    ("runner_kwargs", "message"),
    [
        ({"mount_type": "volume"}, "exact read-only evidence directory"),
        ({"mount_rw": True}, "exact read-only evidence directory"),
        (
            {"report_path": "/opt/airflow/fotmob-admission/other.json"},
            "report path differs",
        ),
    ],
)
def test_shared_handoff_rejects_wrong_admission_mount_or_report(
    tmp_path, runner_kwargs, message
):
    _shared_runtime_digests(tmp_path)
    runner = _shared_handoff_runner(
        tmp_path,
        _orchestration_payload(),
        **runner_kwargs,
    )

    with pytest.raises(mod.DeploymentError, match=message):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=runner,
        )


def test_shared_handoff_rejects_different_resolved_evidence_source(tmp_path):
    _shared_runtime_digests(tmp_path)
    other = tmp_path / "other-evidence"
    other.mkdir()
    runner = _shared_handoff_runner(
        tmp_path,
        _orchestration_payload(),
        mount_source=other,
    )

    with pytest.raises(mod.DeploymentError, match="exact read-only evidence directory"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=runner,
        )


@pytest.mark.parametrize("relation", ["parent", "child"])
def test_shared_handoff_rejects_writable_alias_or_nested_mount(tmp_path, relation):
    _shared_runtime_digests(tmp_path)
    if relation == "parent":
        writable_source = tmp_path.parent
    else:
        writable_source = tmp_path / "writable-child"
        writable_source.mkdir()
    runner = _shared_handoff_runner(
        tmp_path,
        _orchestration_payload(),
        extra_mounts=(
            {
                "Type": "bind",
                "Source": str(writable_source.resolve()),
                "Destination": "/opt/airflow/logs",
                "RW": True,
            },
        ),
    )

    with pytest.raises(mod.DeploymentError, match="aliases or nests"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=runner,
        )


def test_shared_handoff_identity_must_remain_stable_until_activation():
    initial = {
        "shared_scheduler_container": "1" * 64,
        "shared_admission_mount": {
            "source": "/evidence",
            "destination": "/opt/airflow/fotmob-admission",
            "read_only": True,
        },
        "runtime_code_sha256": {"dags/example.py": "a" * 64},
    }
    mod.validate_stable_shared_handoff(initial, dict(initial))
    replaced = {**initial, "shared_scheduler_container": "2" * 64}
    with pytest.raises(mod.DeploymentError, match="identity changed"):
        mod.validate_stable_shared_handoff(initial, replaced)
    drifted = {
        **initial,
        "runtime_code_sha256": {"dags/example.py": "b" * 64},
    }
    with pytest.raises(mod.DeploymentError, match="identity changed"):
        mod.validate_stable_shared_handoff(initial, drifted)
    remounted = {
        **initial,
        "shared_admission_mount": {
            **initial["shared_admission_mount"],
            "source": "/other-evidence",
        },
    }
    with pytest.raises(mod.DeploymentError, match="identity changed"):
        mod.validate_stable_shared_handoff(initial, remounted)


def test_runtime_manifest_ignores_only_generated_bytecode(tmp_path):
    expected = _shared_runtime_digests(tmp_path)
    bytecode = tmp_path / "dags/__pycache__/example.cpython-312.pyc"
    bytecode.parent.mkdir(parents=True, exist_ok=True)
    bytecode.write_bytes(b"generated")
    assert mod.shared_runtime_manifest(tmp_path) == expected
    assert fotmob_runtime._is_generated_bytecode_path(
        "dags/__pycache__/example.cpython-312.pyc"
    )
    assert not fotmob_runtime._is_generated_bytecode_path("dags/example.pyc")


def test_runtime_manifest_rejects_wrong_approved_scope_bytes(tmp_path):
    _shared_runtime_digests(tmp_path)
    (tmp_path / mod.APPROVED_SCOPE_PATH).write_text("1=wrong\n")
    with pytest.raises(mod.DeploymentError, match="approved SHA-256"):
        mod.shared_runtime_manifest(tmp_path)


@pytest.mark.parametrize(
    "runtime_path",
    [
        "dags/.airflowignore",
        "configs/fotmob/competitions.json",
        "configs/fotmob/issue-930-scopes.txt",
        "scrapers/fotmob/service.py",
    ],
)
@pytest.mark.parametrize("failure_mode", ["omitted", "stale"])
def test_shared_handoff_rejects_incomplete_or_stale_runtime_manifest(
    tmp_path, runtime_path, failure_mode
):
    runner = _shared_handoff_runner(
        tmp_path,
        _orchestration_payload(),
        omitted_runtime_path=runtime_path if failure_mode == "omitted" else None,
        stale_runtime_path=runtime_path if failure_mode == "stale" else None,
    )
    with pytest.raises(mod.DeploymentError, match="bind-mounted runtime differs"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=runner,
        )


def test_shared_handoff_rejects_running_sofa_master_or_fotmob_run(tmp_path):
    orchestration = _orchestration_payload(
        active_runs=[
            {
                "dag_id": "dag_sofascore_pipeline",
                "run_id": "still-running",
                "state": "running",
            }
        ]
    )
    with pytest.raises(mod.DeploymentError, match="still has active"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, orchestration),
        )


def test_shared_handoff_rejects_running_downstream_consumer(tmp_path):
    orchestration = _orchestration_payload(
        active_runs=[
            {
                "dag_id": "dag_transform_e3",
                "run_id": "orphaned-e3",
                "state": "running",
            }
        ]
    )
    with pytest.raises(mod.DeploymentError, match="E3/E4/Gold"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, orchestration),
        )


def test_shared_handoff_rejects_wrong_production_pause_state(tmp_path):
    orchestration = _orchestration_payload(
        pause_states={
            "dag_master_pipeline": False,
            "dag_sofascore_pipeline": True,
            "dag_ingest_fotmob": True,
            "dag_transform_fotmob_silver": True,
        }
    )
    with pytest.raises(mod.DeploymentError, match="master/SofaScore/ingest/Silver"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, orchestration),
        )


def test_shared_handoff_rejects_unpaused_sofascore_consumer(tmp_path):
    pause_states = {
        "dag_master_pipeline": True,
        "dag_sofascore_pipeline": False,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": True,
    }
    with pytest.raises(mod.DeploymentError, match="master/SofaScore/ingest/Silver"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(
                tmp_path, _orchestration_payload(pause_states=pause_states)
            ),
        )


def test_shared_handoff_rejects_missing_next_sofascore_interval(tmp_path):
    orchestration = _orchestration_payload(schedule_boundary={})
    with pytest.raises(mod.DeploymentError, match="next scheduled interval"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, orchestration),
        )


def test_shared_handoff_rejects_wrong_owner_from_atomic_snapshot(tmp_path):
    orchestration = _orchestration_payload()
    orchestration["schedule_owner"] = "shared"
    with pytest.raises(mod.DeploymentError, match="must equal 'isolated'"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, orchestration),
        )


def test_shared_handoff_allows_only_paused_stale_shared_daily_row(tmp_path):
    safe = _orchestration_payload(daily_present=True, daily_paused=True)
    evidence = _validate_shared_handoff(
        tmp_path,
        "shared-scheduler",
        "postgresql://control@postgres/control",
        run=_shared_handoff_runner(tmp_path, safe),
    )
    assert (
        evidence["orchestration_state"]["shared_daily_trigger"]
        == (safe["shared_daily_trigger"])
    )

    unsafe = _orchestration_payload(daily_present=True, daily_paused=False)
    with pytest.raises(mod.DeploymentError, match="paused stale row"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, unsafe),
        )

    opted_in = _orchestration_payload()
    opted_in["shared_daily_trigger"]["isolated_stack_env"] = "1"
    with pytest.raises(mod.DeploymentError, match="paused stale row"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(tmp_path, opted_in),
        )


def test_shared_handoff_rejects_unsafe_sofa_publication_edges(tmp_path):
    with pytest.raises(mod.DeploymentError, match="unsafe FotMob publication edges"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(
                tmp_path, _orchestration_payload(safe_edges=False)
            ),
        )


def test_shared_handoff_rejects_xref_writer_outside_publication_preflight(tmp_path):
    with pytest.raises(mod.DeploymentError, match="gate every writer"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(
                tmp_path, _orchestration_payload(safe_xref=False)
            ),
        )


def test_shared_handoff_rejects_unpaused_shared_silver(tmp_path):
    pause_states = {
        "dag_master_pipeline": True,
        "dag_sofascore_pipeline": True,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": False,
    }
    with pytest.raises(mod.DeploymentError, match="master/SofaScore/ingest/Silver"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(
                tmp_path, _orchestration_payload(pause_states=pause_states)
            ),
        )


def test_shared_handoff_rejects_downstream_writer_outside_preflight(tmp_path):
    with pytest.raises(mod.DeploymentError, match="before every downstream task"):
        _validate_shared_handoff(
            tmp_path,
            "shared-scheduler",
            "postgresql://control@postgres/control",
            run=_shared_handoff_runner(
                tmp_path, _orchestration_payload(safe_downstream=False)
            ),
        )


def test_redeploy_aborts_before_pause_or_stop_when_isolated_run_is_active(
    tmp_path, monkeypatch
):
    release = tmp_path / "release"
    release.mkdir()
    env_file = tmp_path / "fotmob.env"
    compose_file = tmp_path / "compose.yaml"
    env_file.write_text(
        "FOTMOB_AIRFLOW_DB_PASSWORD=safe_password\n"
        "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control\n"
        "TELEGRAM_BOT_TOKEN=test-token\n"
        "TELEGRAM_CHAT_ID=test-chat\n"
    )
    compose_file.write_text("services: {}\n")
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    monkeypatch.setattr(mod, "release_sha", lambda *_: "a" * 40)
    monkeypatch.setattr(mod, "prepare_dagbag", lambda *_: dagbag)
    monkeypatch.setattr(
        mod,
        "validate_shared_handoff",
        lambda *_args, **_kwargs: {"passed": True},
    )
    arguments = type(
        "Args",
        (),
        {
            "image": "registry/image@sha256:" + "b" * 64,
            "postgres_image": "postgres@sha256:" + "c" * 64,
            "release_root": release,
            "env_file": env_file,
            "compose_file": compose_file,
            "evidence_dir": tmp_path / "evidence",
            "project": "fotmob-airflow",
            "shared_scheduler_container": "shared",
            "timeout_seconds": 1,
            "keep_paused": False,
        },
    )()
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        if "ps" in command and "-q" in command:
            stdout = "isolated-container\n"
        elif "list-runs" in command:
            stdout = (
                '[{"run_id":"active","state":"running"}]'
                if "dag_ingest_fotmob" in command
                else "[]"
            )
        else:
            stdout = ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    with pytest.raises(
        mod.DeploymentError, match="legacy scheduled activation is retired"
    ):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    assert calls == []


def test_admission_failure_stops_scheduler_when_repause_cannot_be_proven(
    tmp_path, monkeypatch
):
    release = tmp_path / "release"
    release.mkdir()
    env_file = tmp_path / "fotmob.env"
    compose_file = tmp_path / "compose.yaml"
    env_file.write_text(
        "FOTMOB_AIRFLOW_DB_PASSWORD=safe_password\n"
        "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control\n"
        "TELEGRAM_BOT_TOKEN=test-token\n"
        "TELEGRAM_CHAT_ID=test-chat\n"
    )
    compose_file.write_text("services: {}\n")
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    monkeypatch.setattr(mod, "release_sha", lambda *_: "a" * 40)
    monkeypatch.setattr(mod, "prepare_dagbag", lambda *_: dagbag)
    monkeypatch.setattr(
        mod,
        "validate_shared_handoff",
        lambda *_args, **_kwargs: {"passed": True},
    )
    arguments = type(
        "Args",
        (),
        {
            "image": "registry/image@sha256:" + "b" * 64,
            "postgres_image": "postgres@sha256:" + "c" * 64,
            "release_root": release,
            "env_file": env_file,
            "compose_file": compose_file,
            "evidence_dir": tmp_path / "evidence",
            "project": "fotmob-airflow",
            "shared_scheduler_container": "shared",
            "timeout_seconds": 1,
            "keep_paused": False,
        },
    )()
    calls = []
    scheduler_ps_calls = 0
    paused = set(mod.EXPECTED_DAGS)
    failure_cleanup = False

    def run(command, **_kwargs):
        nonlocal scheduler_ps_calls, failure_cleanup
        calls.append(command)
        if command[:2] == ("docker", "inspect"):
            failure_cleanup = True
            stdout = "mutable-image-id\n"
        elif "ps" in command and "-q" in command:
            if "airflow-scheduler" in command:
                scheduler_ps_calls += 1
                stdout = "" if scheduler_ps_calls == 1 else "1" * 64 + "\n"
            else:
                stdout = "2" * 64 + "\n"
        elif "list-import-errors" in command:
            stdout = "[]"
        elif "list-runs" in command:
            stdout = "[]"
        elif "dags" in command and "list" in command and "--output" in command:
            # During the exception safeguard, pretend the metadata DB ignored
            # successful pause commands; the deployer must stop the scheduler.
            observed = set() if failure_cleanup else paused
            stdout = __import__("json").dumps(
                [
                    {"dag_id": dag_id, "is_paused": dag_id in observed}
                    for dag_id in mod.EXPECTED_DAGS
                ]
            )
        elif "unpause" in command:
            paused.discard(command[-1])
            stdout = ""
        elif "pause" in command:
            if not failure_cleanup:
                paused.add(command[-1])
            stdout = ""
        elif "FOTMOB_DAGBAG_JSON=" in command[-1]:
            payload = {
                "dags": {
                    dag_id: {
                        "fileloc": mod.EXPECTED_DAG_FILES[dag_id],
                        "schedule": mod.EXPECTED_SCHEDULES[dag_id],
                    }
                    for dag_id in mod.EXPECTED_DAGS
                },
                "import_errors": {},
            }
            stdout = "FOTMOB_DAGBAG_JSON=" + __import__("json").dumps(payload)
        else:
            stdout = ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    with pytest.raises(
        mod.DeploymentError, match="legacy scheduled activation is retired"
    ):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    assert calls == []


def test_partial_compose_up_failure_still_stops_scheduler(tmp_path, monkeypatch):
    release = tmp_path / "release"
    release.mkdir()
    env_file = tmp_path / "fotmob.env"
    compose_file = tmp_path / "compose.yaml"
    env_file.write_text(
        "FOTMOB_AIRFLOW_DB_PASSWORD=safe_password\n"
        "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control\n"
        "TELEGRAM_BOT_TOKEN=test-token\n"
        "TELEGRAM_CHAT_ID=test-chat\n"
    )
    compose_file.write_text("services: {}\n")
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    monkeypatch.setattr(mod, "release_sha", lambda *_: "a" * 40)
    monkeypatch.setattr(mod, "prepare_dagbag", lambda *_: dagbag)
    monkeypatch.setattr(
        mod, "validate_shared_handoff", lambda *_args, **_kwargs: {"passed": True}
    )
    arguments = type(
        "Args",
        (),
        {
            "image": "registry/image@sha256:" + "b" * 64,
            "postgres_image": "postgres@sha256:" + "c" * 64,
            "release_root": release,
            "env_file": env_file,
            "compose_file": compose_file,
            "evidence_dir": tmp_path / "evidence",
            "project": "fotmob-airflow",
            "shared_scheduler_container": "shared",
            "timeout_seconds": 1,
            "keep_paused": False,
        },
    )()
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        if "up" in command:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    with pytest.raises(
        mod.DeploymentError, match="legacy scheduled activation is retired"
    ):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    assert calls == []


def test_trigger_activation_commits_pending_then_proves_both_runs(
    tmp_path, monkeypatch
):
    report_path = tmp_path / "deployment.json"
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "deployment_id": "f" * 32,
        "schedule_boundary": mod.validate_matching_schedule_boundaries(
            shared_initial=NEXT_SCHEDULE_BOUNDARY,
            shared_final=NEXT_SCHEDULE_BOUNDARY,
            isolated_initial=NEXT_SCHEDULE_BOUNDARY,
            isolated_final=NEXT_SCHEDULE_BOUNDARY,
        ),
    }
    calls = []

    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: mod.validate_schedule_boundary(
            NEXT_SCHEDULE_BOUNDARY, label="test"
        ),
    )

    def unpause(container, dag_id, **_kwargs):
        persisted = json.loads(report_path.read_text(encoding="utf-8"))
        assert persisted["activation_state"] == "pending_consumer"
        assert persisted["paused"] == [mod.ISOLATED_DAILY_DAG_ID]
        assert set(persisted["unpaused"]) == (
            mod.EXPECTED_DAGS - {mod.ISOLATED_DAILY_DAG_ID}
        )
        calls.append((container, dag_id))

    monkeypatch.setattr(mod, "_docker_unpause", unpause)
    scheduled = _proved_scheduled_activation()
    monkeypatch.setattr(
        mod, "poll_exact_scheduled_handoff", lambda **_kwargs: scheduled
    )

    result = mod._commit_trigger_activation(
        report_path,
        report,
        isolated_container="1" * 64,
        shared_container="2" * 64,
        timeout_seconds=300,
        run=lambda *_args, **_kwargs: None,
        sleeper=lambda _seconds: None,
        now=datetime(2026, 7, 21, 16, tzinfo=timezone.utc),
    )

    assert calls[-2:] == [
        ("1" * 64, mod.ISOLATED_DAILY_DAG_ID),
        ("2" * 64, mod.SHARED_CONSUMER_DAG_ID),
    ]
    assert result["activation_state"] == "active"
    assert result["paused"] == []
    assert set(result["unpaused"]) == mod.EXPECTED_DAGS
    assert result["scheduled_activation"] == scheduled
    assert json.loads(report_path.read_text(encoding="utf-8")) == result


@pytest.mark.parametrize(
    ("shared_commit", "isolated_commit"),
    (
        (NEXT_SCHEDULE_BOUNDARY, ADVANCED_SCHEDULE_BOUNDARY),
        (ADVANCED_SCHEDULE_BOUNDARY, ADVANCED_SCHEDULE_BOUNDARY),
    ),
    ids=("schedulers-disagree", "both-advanced-from-admitted-proof"),
)
def test_trigger_activation_rejects_commit_edge_boundary_drift_before_unpause(
    tmp_path, monkeypatch, shared_commit, isolated_commit
):
    report_path = tmp_path / "deployment.json"
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "schedule_boundary": mod.validate_matching_schedule_boundaries(
            shared_initial=NEXT_SCHEDULE_BOUNDARY,
            shared_final=NEXT_SCHEDULE_BOUNDARY,
            isolated_initial=NEXT_SCHEDULE_BOUNDARY,
            isolated_final=NEXT_SCHEDULE_BOUNDARY,
        ),
    }
    observed = []
    commit_boundaries = iter((shared_commit, isolated_commit))

    def boundary(container, _dag_id, **_kwargs):
        observed.append(container)
        return next(commit_boundaries)

    monkeypatch.setattr(mod, "read_schedule_boundary", boundary)
    monkeypatch.setattr(
        mod,
        "_docker_unpause",
        lambda *_args, **_kwargs: pytest.fail("must not unpause after boundary drift"),
    )

    with pytest.raises(mod.DeploymentError, match="different next scheduled intervals"):
        mod._commit_trigger_activation(
            report_path,
            report,
            isolated_container="1" * 64,
            shared_container="2" * 64,
            timeout_seconds=300,
            run=lambda *_args, **_kwargs: None,
            sleeper=lambda _seconds: None,
        )

    assert observed == ["2" * 64, "1" * 64]
    assert not report_path.exists()


def test_atomic_deployment_report_is_scheduler_readable_after_root_style_replace(
    tmp_path,
):
    report_path = tmp_path / "deployment.json"
    report_path.write_text("stale", encoding="utf-8")
    report_path.chmod(0o600)
    payload = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "delivery_credentials": {
            "telegram_bot_token_configured": True,
            "telegram_chat_id_configured": True,
        },
    }

    mod._atomic_json(report_path, payload)

    report_stat = report_path.stat()
    mode = report_stat.st_mode & 0o777
    assert mode == mod.DEPLOYMENT_REPORT_MODE == 0o444
    # A scheduler running as uid 50000 can read a report written by a
    # different host uid (normally root); no credential value is exposed.
    assert mode & 0o004
    rendered = report_path.read_text(encoding="utf-8")
    assert json.loads(rendered) == payload
    assert "must-not-appear" not in rendered
    if os.geteuid() == 0:
        assert report_stat.st_uid == 0


def test_evidence_report_directories_ignore_restrictive_root_umask(tmp_path):
    evidence_dir = tmp_path / "evidence"
    report_path = evidence_dir / "nested" / "deployment.json"
    previous_umask = os.umask(0o077)
    try:
        mod._prepare_evidence_report_path(evidence_dir, report_path)
        mod._atomic_json(report_path, {"passed": True})
    finally:
        os.umask(previous_umask)

    assert evidence_dir.stat().st_mode & 0o777 == mod.EVIDENCE_DIRECTORY_MODE
    assert report_path.parent.stat().st_mode & 0o777 == (mod.EVIDENCE_DIRECTORY_MODE)
    assert report_path.stat().st_mode & 0o777 == mod.DEPLOYMENT_REPORT_MODE


def test_trigger_activation_never_unpauses_when_durable_commit_crashes(
    tmp_path, monkeypatch
):
    class SimulatedProcessKill(BaseException):
        pass

    unpause_called = False

    def unpause(*_args, **_kwargs):
        nonlocal unpause_called
        unpause_called = True

    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: mod.validate_schedule_boundary(
            NEXT_SCHEDULE_BOUNDARY, label="test"
        ),
    )
    monkeypatch.setattr(mod, "_docker_unpause", unpause)
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedProcessKill),
    )
    with pytest.raises(SimulatedProcessKill):
        mod._commit_trigger_activation(
            tmp_path / "deployment.json",
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "schedule_boundary": mod.validate_matching_schedule_boundaries(
                    shared_initial=NEXT_SCHEDULE_BOUNDARY,
                    shared_final=NEXT_SCHEDULE_BOUNDARY,
                    isolated_initial=NEXT_SCHEDULE_BOUNDARY,
                    isolated_final=NEXT_SCHEDULE_BOUNDARY,
                ),
            },
            isolated_container="1" * 64,
            shared_container="2" * 64,
            timeout_seconds=300,
            run=lambda *_args, **_kwargs: None,
            sleeper=lambda _seconds: None,
            now=datetime(2026, 7, 21, 16, tzinfo=timezone.utc),
        )
    assert unpause_called is False


def test_pending_consumer_error_preserves_producer_and_resume_state(
    tmp_path, monkeypatch
):
    report_path = tmp_path / "deployment.json"
    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: mod.validate_schedule_boundary(
            NEXT_SCHEDULE_BOUNDARY, label="test"
        ),
    )
    calls = []

    def unpause(container, dag_id, **_kwargs):
        calls.append((container, dag_id))
        if container == "2" * 64:
            raise RuntimeError("shared unavailable")

    monkeypatch.setattr(mod, "_docker_unpause", unpause)
    with pytest.raises(mod.PendingConsumerError):
        mod._commit_trigger_activation(
            report_path,
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "schedule_boundary": mod.validate_matching_schedule_boundaries(
                    shared_initial=NEXT_SCHEDULE_BOUNDARY,
                    shared_final=NEXT_SCHEDULE_BOUNDARY,
                    isolated_initial=NEXT_SCHEDULE_BOUNDARY,
                    isolated_final=NEXT_SCHEDULE_BOUNDARY,
                ),
            },
            isolated_container="1" * 64,
            shared_container="2" * 64,
            timeout_seconds=300,
            run=lambda *_args, **_kwargs: None,
            sleeper=lambda _seconds: None,
            now=datetime(2026, 7, 21, 16, tzinfo=timezone.utc),
        )

    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["activation_state"] == "pending_consumer"
    assert persisted["scheduled_activation"]["resume_required"] is True
    assert "shared unavailable" in persisted["scheduled_activation"]["last_error"]
    assert ("1" * 64, mod.ISOLATED_DAILY_DAG_ID) in calls


def _pending_activation_report():
    return {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "activation_state": "pending_consumer",
        "kept_paused": False,
        "paused": [mod.ISOLATED_DAILY_DAG_ID],
        "unpaused": sorted(mod.EXPECTED_DAGS - {mod.ISOLATED_DAILY_DAG_ID}),
        "schedule_boundary": mod.validate_matching_schedule_boundaries(
            shared_initial=NEXT_SCHEDULE_BOUNDARY,
            shared_final=NEXT_SCHEDULE_BOUNDARY,
            isolated_initial=NEXT_SCHEDULE_BOUNDARY,
            isolated_final=NEXT_SCHEDULE_BOUNDARY,
            shared_commit=NEXT_SCHEDULE_BOUNDARY,
            isolated_commit=NEXT_SCHEDULE_BOUNDARY,
        ),
        "scheduled_activation": {
            "status": "pending",
            "producer_dag_id": mod.ISOLATED_DAILY_DAG_ID,
            "consumer_dag_id": mod.SHARED_CONSUMER_DAG_ID,
            "resume_required": True,
        },
        "activation_safety_window": {
            "checked_at": "2026-07-21T12:00:00.000000+00:00",
            "next_boundary": "2026-07-21T14:00:00.000000+00:00",
            "remaining_seconds": 7200,
            "required_seconds": 900,
            "timeout_seconds": 300,
            "passed": True,
        },
    }


def _resume_arguments(report_path, evidence_dir, *, timeout_seconds=300):
    return type(
        "Args",
        (),
        {
            "keep_paused": False,
            "report": report_path,
            "evidence_dir": evidence_dir,
            "timeout_seconds": timeout_seconds,
        },
    )()


def test_resume_identity_rejects_a_copied_report_path(tmp_path, monkeypatch):
    evidence_dir = (tmp_path / "evidence").resolve()
    original_report = evidence_dir / "admitted" / "deployment.json"
    copied_report = evidence_dir / "copied" / "deployment.json"
    release_root = (tmp_path / "release").resolve()
    compose_file = (tmp_path / "compose.yaml").resolve()
    original_report.parent.mkdir(parents=True)
    copied_report.parent.mkdir(parents=True)
    release_root.mkdir()
    compose_file.write_text("services: {}\n", encoding="utf-8")
    relative = original_report.relative_to(evidence_dir)
    container_report = str(mod.CONTAINER_EVIDENCE_ROOT / relative)
    shared_report = str(mod.SHARED_CONTAINER_EVIDENCE_ROOT / relative)
    shared_mount = {
        "type": "bind",
        "source": str(evidence_dir),
        "destination": str(mod.SHARED_CONTAINER_EVIDENCE_ROOT),
        "read_only": True,
        "report_path": shared_report,
    }
    payload = {
        "project": "fotmob-airflow",
        "compose_file": str(compose_file),
        "release_root": str(release_root),
        "evidence_dir": str(evidence_dir),
        "image": "registry/image@sha256:" + "b" * 64,
        "postgres_image": "postgres@sha256:" + "c" * 64,
        "git_sha": "a" * 40,
        "container_report_path": container_report,
        "shared_container_report_path": shared_report,
        "scheduler_container_id": "1" * 64,
        "shared_handoff_initial": {
            "shared_scheduler_container": "2" * 64,
            "shared_admission_mount": dict(shared_mount),
        },
        "shared_handoff_final": {
            "shared_scheduler_container": "2" * 64,
            "shared_admission_mount": dict(shared_mount),
        },
    }
    args = type(
        "Args",
        (),
        {
            "report": original_report,
            "evidence_dir": evidence_dir,
            "release_root": release_root,
            "compose_file": compose_file,
            "project": "fotmob-airflow",
            "image": payload["image"],
            "postgres_image": payload["postgres_image"],
        },
    )()
    monkeypatch.setattr(mod, "release_sha", lambda *_args, **_kwargs: "a" * 40)

    def run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command, 0, stdout=command[-1] + "\n", stderr=""
        )

    resolved, isolated, shared = mod._validate_resume_identity(args, payload, run=run)
    assert resolved == original_report
    assert isolated == "1" * 64
    assert shared == "2" * 64

    args.report = copied_report
    with pytest.raises(mod.DeploymentError, match="host report path differs"):
        mod._validate_resume_identity(args, payload, run=run)


def test_plain_deploy_preserves_existing_green_pending_report(
    tmp_path, monkeypatch, capsys
):
    evidence_dir = tmp_path / "evidence"
    report_path = evidence_dir / "deployment.json"
    evidence_dir.mkdir()
    original = (
        json.dumps(
            _pending_activation_report(),
            ensure_ascii=False,
            indent=2,
            sort_keys=False,
        ).encode("utf-8")
        + b"\n"
    )
    report_path.write_bytes(original)
    deploy_called = False

    def deploy(*_args, **_kwargs):
        nonlocal deploy_called
        deploy_called = True
        raise AssertionError("plain deploy must not start from pending_consumer")

    monkeypatch.setattr(mod, "deploy", deploy)
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: pytest.fail("pending report must not be overwritten"),
    )

    exit_code = mod.main(
        [
            "--release-root",
            str(tmp_path / "release"),
            "--env-file",
            str(tmp_path / "fotmob.env"),
            "--image",
            "registry/image@sha256:" + "b" * 64,
            "--postgres-image",
            "postgres@sha256:" + "c" * 64,
            "--evidence-dir",
            str(evidence_dir),
            "--report",
            str(report_path),
        ]
    )

    assert exit_code == 1
    assert deploy_called is False
    assert report_path.read_bytes() == original
    assert "--resume-pending" in capsys.readouterr().out


def test_plain_deploy_preserves_corrupted_green_pending_as_incident(
    tmp_path, monkeypatch, capsys
):
    evidence_dir = tmp_path / "evidence"
    report_path = evidence_dir / "deployment.json"
    evidence_dir.mkdir()
    payload = _pending_activation_report()
    payload["scheduled_activation"] = {"status": "forged"}
    original = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    report_path.write_bytes(original)
    monkeypatch.setattr(
        mod,
        "deploy",
        lambda *_args, **_kwargs: pytest.fail(
            "ordinary deploy must never overwrite a green pending incident"
        ),
    )
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: pytest.fail(
            "corrupted pending report must remain byte-for-byte"
        ),
    )

    exit_code = mod.main(_main_deploy_arguments(tmp_path, evidence_dir, report_path))

    assert exit_code == 1
    assert report_path.read_bytes() == original
    output = json.loads(capsys.readouterr().out)
    assert "incident" in output["operator_action"]
    assert "#997" in output["operator_action"]


@pytest.mark.parametrize("failure", ("invalid_json", "unreadable"))
def test_plain_deploy_preserves_unknown_report_before_any_runtime_mutation(
    tmp_path, monkeypatch, capsys, failure
):
    evidence_dir = tmp_path / "evidence"
    report_path = evidence_dir / "deployment.json"
    evidence_dir.mkdir()
    original = b"not-json-and-do-not-overwrite\n"
    report_path.write_bytes(original)
    if failure == "unreadable":
        original_read_text = Path.read_text

        def deny_report_read(path, *args, **kwargs):
            if path == report_path:
                raise PermissionError("simulated unreadable report")
            return original_read_text(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", deny_report_read)
    monkeypatch.setattr(
        mod,
        "deploy",
        lambda *_args, **_kwargs: pytest.fail(
            "unknown existing report must block before deploy"
        ),
    )
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: pytest.fail(
            "pre-mutation report must remain byte-for-byte"
        ),
    )

    exit_code = mod.main(_main_deploy_arguments(tmp_path, evidence_dir, report_path))

    assert exit_code == 1
    assert report_path.read_bytes() == original
    output = json.loads(capsys.readouterr().out)
    assert output["existing_report_preserved"] is True
    assert output["previous_activation_state"] is None


def _main_deploy_arguments(tmp_path, evidence_dir, report_path, *, image=None):
    return [
        "--release-root",
        str(tmp_path / "release"),
        "--env-file",
        str(tmp_path / "fotmob.env"),
        "--image",
        image or ("registry/image@sha256:" + "b" * 64),
        "--postgres-image",
        "postgres@sha256:" + "c" * 64,
        "--evidence-dir",
        str(evidence_dir),
        "--report",
        str(report_path),
    ]


@pytest.mark.parametrize("activation_state", ("active", "kept_paused"))
def test_pre_mutation_upgrade_failure_preserves_completed_certificate(
    tmp_path, monkeypatch, capsys, activation_state
):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    report_path = evidence_dir / "deployment.json"
    original = (
        json.dumps(
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "activation_state": activation_state,
            },
            indent=2,
        ).encode("utf-8")
        + b"\n"
    )
    report_path.write_bytes(original)
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: pytest.fail(
            "pre-mutation failure must not replace the completed certificate"
        ),
    )

    exit_code = mod.main(
        _main_deploy_arguments(
            tmp_path,
            evidence_dir,
            report_path,
            image="not-a-digest-pinned-image",
        )
    )

    assert exit_code == 1
    assert report_path.read_bytes() == original
    output = json.loads(capsys.readouterr().out)
    assert output["existing_report_preserved"] is True
    assert output["previous_activation_state"] == activation_state


def test_post_mutation_upgrade_failure_may_replace_old_certificate(
    tmp_path, monkeypatch
):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    report_path = evidence_dir / "deployment.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "activation_state": "active",
            }
        ),
        encoding="utf-8",
    )

    def fail_after_mutation(args):
        mod._mark_runtime_mutation_started(args)
        raise mod.DeploymentError("compose up changed runtime")

    monkeypatch.setattr(mod, "deploy", fail_after_mutation)

    assert mod.main(_main_deploy_arguments(tmp_path, evidence_dir, report_path)) == 1
    replaced = json.loads(report_path.read_text(encoding="utf-8"))
    assert replaced["passed"] is False
    assert "compose up changed runtime" in replaced["error"]


def test_evidence_lock_blocks_a_second_invocation_without_report_or_docker_changes(
    tmp_path, monkeypatch, capsys
):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    report_path = evidence_dir / "deployment.json"
    original = b'{"sentinel":"unchanged"}\n'
    report_path.write_bytes(original)
    monkeypatch.setattr(
        mod,
        "_main_locked",
        lambda *_args, **_kwargs: pytest.fail(
            "second invocation must stop before report preflight or Docker"
        ),
    )

    with mod._deployment_invocation_lock(evidence_dir):
        exit_code = mod.main(
            _main_deploy_arguments(tmp_path, evidence_dir, report_path)
        )

    assert exit_code == 1
    assert report_path.read_bytes() == original
    assert "holds the evidence lock" in capsys.readouterr().out

    # Kernel/file-descriptor release makes the same lock immediately reusable.
    with mod._deployment_invocation_lock(evidence_dir):
        pass


def test_evidence_lock_rejects_symlinked_evidence_directory(tmp_path):
    real = tmp_path / "real-evidence"
    real.mkdir()
    linked = tmp_path / "linked-evidence"
    linked.symlink_to(real, target_is_directory=True)

    with pytest.raises(mod.DeploymentError, match="must not contain symlinks"):
        with mod._deployment_invocation_lock(linked):
            pass


def test_evidence_lock_rejects_group_or_world_writable_directory(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    evidence_dir.chmod(0o777)
    try:
        with pytest.raises(mod.DeploymentError, match="not group/world writable"):
            with mod._deployment_invocation_lock(evidence_dir):
                pass
    finally:
        evidence_dir.chmod(0o700)


def test_evidence_lock_rejects_symlink_without_touching_target(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    target = tmp_path / "target"
    original = b"do-not-touch\n"
    target.write_bytes(original)
    (evidence_dir / ".fotmob-deploy.lock").symlink_to(target)

    with pytest.raises(mod.DeploymentError, match="safe regular file"):
        with mod._deployment_invocation_lock(evidence_dir):
            pass

    assert target.read_bytes() == original


def test_evidence_lock_rejects_existing_file_with_wrong_mode(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    lock_path = evidence_dir / ".fotmob-deploy.lock"
    lock_path.write_text("", encoding="utf-8")
    lock_path.chmod(0o644)

    with pytest.raises(mod.DeploymentError, match="owner-controlled 0600"):
        with mod._deployment_invocation_lock(evidence_dir):
            pass


def test_resume_pending_is_idempotent_after_active_commit(tmp_path, monkeypatch):
    report_path = tmp_path / "deployment.json"
    report_path.write_text(json.dumps(_pending_activation_report()), encoding="utf-8")
    args = _resume_arguments(report_path, tmp_path)
    isolated_container = "1" * 64
    shared_container = "2" * 64
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: (
            report_path,
            isolated_container,
            shared_container,
        ),
    )
    monkeypatch.setattr(
        mod,
        "read_exact_scheduled_run",
        lambda _container, dag_id, *_args, **_kwargs: _exact_scheduled_row(
            dag_id, state="success"
        ),
    )
    unpauses = []
    monkeypatch.setattr(
        mod,
        "_docker_unpause",
        lambda container, dag_id, **_kwargs: unpauses.append((container, dag_id)),
    )
    activation = _proved_scheduled_activation(state="queued")
    monkeypatch.setattr(
        mod,
        "poll_exact_scheduled_handoff",
        lambda **_kwargs: activation,
    )

    first = mod.resume_pending_activation(
        args,
        run=lambda *_args, **_kwargs: None,
        sleeper=lambda _seconds: None,
    )
    second = mod.resume_pending_activation(
        args,
        run=lambda *_args, **_kwargs: None,
        sleeper=lambda _seconds: None,
    )

    assert first == second
    assert second["activation_state"] == "active"
    assert len(unpauses) == 4


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        (
            lambda payload: payload["scheduled_activation"].__setitem__(
                "producer", {"run_id": "scheduled__partial"}
            ),
            "incomplete",
        ),
        (
            lambda payload: payload.__setitem__(
                "scheduled_activation",
                _proved_scheduled_activation(run_id="scheduled__forged"),
            ),
            "differs from admitted schedule",
        ),
        (
            lambda payload: payload["scheduled_activation"]["consumer"].__setitem__(
                "state", "up_for_retry"
            ),
            "differs from admitted schedule",
        ),
    ),
)
def test_active_resume_rejects_incomplete_forged_or_invalid_report_proof(
    tmp_path, monkeypatch, mutation, error
):
    report_path = tmp_path / "deployment.json"
    payload = _pending_activation_report()
    payload.update(
        {
            "activation_state": "active",
            "paused": [],
            "unpaused": sorted(mod.EXPECTED_DAGS),
            "scheduled_activation": _proved_scheduled_activation(),
        }
    )
    mutation(payload)
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    args = _resume_arguments(report_path, tmp_path)
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: pytest.fail(
            "forged report must fail before live container checks"
        ),
    )

    with pytest.raises(mod.DeploymentError, match=error):
        mod.resume_pending_activation(args, run=lambda *_args, **_kwargs: None)


@pytest.mark.parametrize("live_failure", ("producer_absent", "consumer_mismatch"))
def test_active_resume_rejects_absent_or_mismatched_live_exact_run(
    tmp_path, monkeypatch, live_failure
):
    report_path = tmp_path / "deployment.json"
    payload = _pending_activation_report()
    payload.update(
        {
            "activation_state": "active",
            "paused": [],
            "unpaused": sorted(mod.EXPECTED_DAGS),
            "scheduled_activation": _proved_scheduled_activation(),
        }
    )
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    args = _resume_arguments(report_path, tmp_path)
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: (report_path, "1" * 64, "2" * 64),
    )

    def live_run(_container, dag_id, *_args, **_kwargs):
        if live_failure == "producer_absent" and dag_id == mod.ISOLATED_DAILY_DAG_ID:
            return None
        row = _exact_scheduled_row(dag_id, state="running")
        if live_failure == "consumer_mismatch" and dag_id == mod.SHARED_CONSUMER_DAG_ID:
            row["data_interval_end"] = "2026-07-21T14:00:01.000000+00:00"
        return row

    monkeypatch.setattr(mod, "read_exact_scheduled_run", live_run)

    with pytest.raises(mod.DeploymentError, match="differs from live exact"):
        mod.resume_pending_activation(args, run=lambda *_args, **_kwargs: None)


@pytest.mark.parametrize("state", sorted(mod.EXACT_SCHEDULED_RUN_STATES))
def test_active_schedule_proof_accepts_every_known_identity_state(state):
    payload = {"scheduled_activation": _proved_scheduled_activation(state=state)}
    boundary = mod.validate_schedule_boundary(NEXT_SCHEDULE_BOUNDARY, label="test")

    result = mod._validate_active_scheduled_proof(payload, boundary)
    fotmob_runtime._validate_active_schedule_proof(
        {
            "activation_safety_window": {
                "checked_at": "2026-07-21T12:00:00+00:00",
                "next_boundary": "2026-07-21T14:00:00+00:00",
                "remaining_seconds": 7200,
                "required_seconds": 900,
                "timeout_seconds": 300,
                "passed": True,
            },
            "scheduled_activation": _proved_scheduled_activation(state=state),
        },
        boundary,
    )

    assert result["producer"]["state"] == state


def test_resume_rechecks_safety_when_only_consumer_run_is_missing(
    tmp_path, monkeypatch
):
    report_path = tmp_path / "deployment.json"
    report_path.write_text(json.dumps(_pending_activation_report()), encoding="utf-8")
    args = _resume_arguments(report_path, tmp_path, timeout_seconds=300)
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: (report_path, "1" * 64, "2" * 64),
    )

    def exact_run(_container, dag_id, *_args, **_kwargs):
        if dag_id == mod.ISOLATED_DAILY_DAG_ID:
            return _exact_scheduled_row(dag_id)
        return None

    monkeypatch.setattr(mod, "read_exact_scheduled_run", exact_run)
    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: mod.validate_schedule_boundary(
            NEXT_SCHEDULE_BOUNDARY, label="test"
        ),
    )
    monkeypatch.setattr(
        mod,
        "_continue_pending_consumer_activation",
        lambda *_args, **_kwargs: pytest.fail("unsafe resume must not unpause"),
    )

    with pytest.raises(mod.PendingConsumerError, match="pending its exact shared"):
        mod.resume_pending_activation(
            args,
            run=lambda *_args, **_kwargs: None,
            sleeper=lambda _seconds: None,
            now=datetime(2026, 7, 21, 13, 45, 1, tzinfo=timezone.utc),
        )

    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["activation_state"] == "pending_consumer"
    assert persisted["scheduled_activation"]["resume_required"] is True
    assert "too close" in persisted["scheduled_activation"]["last_error"]


@pytest.mark.parametrize(
    "missing_dag_id",
    (mod.ISOLATED_DAILY_DAG_ID, mod.SHARED_CONSUMER_DAG_ID),
)
def test_resume_missing_one_run_reads_live_boundary_without_false_pause_requirement(
    tmp_path, monkeypatch, missing_dag_id
):
    report_path = tmp_path / "deployment.json"
    report_path.write_text(json.dumps(_pending_activation_report()), encoding="utf-8")
    args = _resume_arguments(report_path, tmp_path)
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: (report_path, "1" * 64, "2" * 64),
    )
    monkeypatch.setattr(
        mod,
        "read_exact_scheduled_run",
        lambda _container, dag_id, *_args, **_kwargs: (
            None if dag_id == missing_dag_id else _exact_scheduled_row(dag_id)
        ),
    )
    boundary_calls = []

    def read_boundary(_container, dag_id, **kwargs):
        boundary_calls.append((dag_id, kwargs.get("require_paused")))
        return mod.validate_schedule_boundary(NEXT_SCHEDULE_BOUNDARY, label="test")

    monkeypatch.setattr(mod, "read_schedule_boundary", read_boundary)
    expected_result = {"resumed": True}
    monkeypatch.setattr(
        mod,
        "_continue_pending_consumer_activation",
        lambda *_args, **_kwargs: expected_result,
    )

    result = mod.resume_pending_activation(
        args,
        run=lambda *_args, **_kwargs: None,
        sleeper=lambda _seconds: None,
        now=datetime(2026, 7, 21, 12, tzinfo=timezone.utc),
    )

    assert result == expected_result
    assert boundary_calls == [(missing_dag_id, False)]


def test_keep_paused_deploy_takes_a_real_second_shared_handoff_snapshot(
    tmp_path, monkeypatch
):
    release = tmp_path / "release"
    release.mkdir()
    env_file = tmp_path / "fotmob.env"
    env_file.write_text(
        "FOTMOB_AIRFLOW_DB_PASSWORD=safe_password\n"
        "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control\n"
        "TELEGRAM_BOT_TOKEN=test-token\n"
        "TELEGRAM_CHAT_ID=test-chat\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    evidence_dir = tmp_path / "evidence"
    args = type(
        "Args",
        (),
        {
            "image": "registry/image@sha256:" + "b" * 64,
            "postgres_image": "postgres@sha256:" + "c" * 64,
            "release_root": release,
            "env_file": env_file,
            "compose_file": compose_file,
            "evidence_dir": evidence_dir,
            "project": "fotmob-airflow",
            "shared_scheduler_container": "shared",
            "timeout_seconds": 1,
            "keep_paused": True,
        },
    )()
    events = []
    monkeypatch.setattr(mod, "release_sha", lambda *_args, **_kwargs: "a" * 40)
    monkeypatch.setattr(mod, "prepare_dagbag", lambda *_args, **_kwargs: dagbag)
    monkeypatch.setattr(
        mod,
        "validate_control_database",
        lambda *_args, **_kwargs: {"same_shared_database": True},
    )
    monkeypatch.setattr(
        mod,
        "validate_delivery_runtime",
        lambda *_args, **_kwargs: {
            "telegram_bot_token_configured": True,
            "telegram_chat_id_configured": True,
        },
    )
    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: NEXT_SCHEDULE_BOUNDARY,
    )
    monkeypatch.setattr(
        mod, "expected_isolated_runtime_manifest", lambda *_args, **_kwargs: {}
    )

    def isolated_runtime(*_args, **_kwargs):
        events.append("isolated_runtime_manifest")
        return {}

    monkeypatch.setattr(mod, "validate_isolated_runtime_manifest", isolated_runtime)
    handoff_calls = []

    def shared_handoff(*_args, **_kwargs):
        handoff_calls.append(len(handoff_calls) + 1)
        events.append(f"shared_snapshot_{handoff_calls[-1]}")
        return {
            "shared_scheduler_container": "9" * 64,
            "shared_admission_mount": {"read_only": True},
            "runtime_code_sha256": {"dags/example.py": "e" * 64},
            "next_scheduled_interval": NEXT_SCHEDULE_BOUNDARY,
            "control_database": {"same_shared_database": True},
            "snapshot_number": handoff_calls[-1],
            "passed": True,
        }

    monkeypatch.setattr(mod, "validate_shared_handoff", shared_handoff)
    fresh_dagbag = {
        "dags": {
            dag_id: {
                "fileloc": mod.EXPECTED_DAG_FILES[dag_id],
                "schedule": mod.EXPECTED_SCHEDULES[dag_id],
            }
            for dag_id in mod.EXPECTED_DAGS
        },
        "import_errors": {},
    }

    def run(command, **_kwargs):
        if command[:3] == ("docker", "inspect", "--format"):
            stdout = "sha256:" + "d" * 64 + "\n"
        elif command[:2] == ("docker", "compose"):
            if "ps" in command:
                if "--all" not in command:
                    stdout = ""
                elif command[-1] == "airflow-scheduler":
                    stdout = "1" * 64 + "\n"
                elif command[-1] == "airflow-metadb":
                    stdout = "2" * 64 + "\n"
                else:
                    raise AssertionError(f"unexpected compose ps command: {command}")
            elif "list-import-errors" in command:
                stdout = "[]"
            elif "list-runs" in command:
                stdout = "[]"
            elif "FOTMOB_DAGBAG_JSON=" in command[-1]:
                stdout = "FOTMOB_DAGBAG_JSON=" + json.dumps(fresh_dagbag)
            elif "FOTMOB_RUNTIME_MARKER_JSON=" in command[-1]:
                events.append("data_plane_marker")
                stdout = 'FOTMOB_RUNTIME_MARKER_JSON={"count":1}'
            elif command[-4:] == ("dags", "list", "--output", "json"):
                stdout = json.dumps(
                    [
                        {"dag_id": dag_id, "is_paused": True}
                        for dag_id in mod.EXPECTED_DAGS
                    ]
                )
            else:
                if "up" in command:
                    events.append("compose_up")
                stdout = ""
        else:
            raise AssertionError(f"unexpected command: {command}")
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    result = mod.deploy(args, run=run, sleeper=lambda _seconds: None)

    assert handoff_calls == [1, 2]
    assert events.index("shared_snapshot_1") < events.index("compose_up")
    assert events.index("compose_up") < events.index("data_plane_marker")
    assert events.index("data_plane_marker") < events.index("isolated_runtime_manifest")
    assert events.index("isolated_runtime_manifest") < events.index("shared_snapshot_2")
    assert result["activation_state"] == "kept_paused"
    assert result["shared_handoff_initial"]["snapshot_number"] == 1
    assert result["shared_handoff_final"]["snapshot_number"] == 2


def test_automatic_activation_boundary_matches_owner_daily_binding():
    before = mod.validate_automatic_activation_boundary(
        NEXT_SCHEDULE_BOUNDARY,
        now=datetime(2026, 7, 21, 13, 30, tzinfo=timezone.utc),
    )
    during = mod.validate_automatic_activation_boundary(
        NEXT_SCHEDULE_BOUNDARY,
        now=datetime(2026, 7, 21, 14, 30, tzinfo=timezone.utc),
    )

    assert before["state"] == "future"
    assert during["state"] == "daily_window_open"
    assert before["data_interval_start"] == "2026-07-20T14:00:00.000000+00:00"
    assert before["data_interval_end"] == "2026-07-21T14:00:00.000000+00:00"


def test_owner_committed_shared_recovery_accepts_only_exact_wait_sensor_run():
    expected_run_id = mod._scheduled_run_id(NEXT_SCHEDULE_BOUNDARY["logical_date"])
    downstream = {
        "trigger_xref_transforms": None,
        "trigger_e3_transforms": None,
        "trigger_e4_transforms": None,
        "finalize_fotmob_publication": None,
    }
    consumer = {
        "dag_id": mod.SHARED_CONSUMER_DAG_ID,
        "run_id": expected_run_id,
        "run_type": "scheduled",
        "state": "running",
        "logical_date": "2026-07-20T14:00:00.000000+00:00",
        "data_interval_start": "2026-07-20T14:00:00.000000+00:00",
        "data_interval_end": "2026-07-21T14:00:00.000000+00:00",
        "task_states": {
            "wait_for_fotmob_publication": "up_for_reschedule",
            **downstream,
        },
    }
    snapshot = {
        "active_runs": [
            {
                "dag_id": mod.SHARED_CONSUMER_DAG_ID,
                "run_id": expected_run_id,
                "state": "running",
            }
        ],
        "consumer_runs": [consumer],
    }

    proof = mod.validate_owner_committed_shared_recovery(
        snapshot,
        stored_boundary=NEXT_SCHEDULE_BOUNDARY,
        live_boundary=ADVANCED_SCHEDULE_BOUNDARY,
    )
    assert proof["mode"] == "scheduled_wait_sensor"

    terminal = copy.deepcopy(snapshot)
    terminal["active_runs"] = []
    terminal["consumer_runs"][0]["state"] = "failed"
    terminal["consumer_runs"][0]["task_states"][
        "wait_for_fotmob_publication"
    ] = "failed"
    terminal["consumer_runs"][0]["task_states"].update(
        {
            "trigger_xref_transforms": "upstream_failed",
            "trigger_e3_transforms": "upstream_failed",
            "trigger_e4_transforms": "upstream_failed",
            "finalize_fotmob_publication": "failed",
        }
    )
    terminal_proof = mod.validate_owner_committed_shared_recovery(
        terminal,
        stored_boundary=NEXT_SCHEDULE_BOUNDARY,
        live_boundary=ADVANCED_SCHEDULE_BOUNDARY,
    )
    assert terminal_proof["mode"] == "terminal_wait_sensor_failed"
    impossible_terminal = copy.deepcopy(terminal)
    impossible_terminal["consumer_runs"][0]["task_states"][
        "finalize_fotmob_publication"
    ] = None
    with pytest.raises(mod.DeploymentError, match="wait-only failed"):
        mod.validate_owner_committed_shared_recovery(
            impossible_terminal,
            stored_boundary=NEXT_SCHEDULE_BOUNDARY,
            live_boundary=ADVANCED_SCHEDULE_BOUNDARY,
        )

    mutations = []
    wrong_run = copy.deepcopy(snapshot)
    wrong_run["active_runs"][0]["run_id"] = "scheduled__wrong"
    mutations.append(wrong_run)
    wrong_type = copy.deepcopy(snapshot)
    wrong_type["consumer_runs"][0]["run_type"] = "manual"
    mutations.append(wrong_type)
    wrong_interval = copy.deepcopy(snapshot)
    wrong_interval["consumer_runs"][0]["data_interval_end"] = (
        "2026-07-21T14:00:01.000000+00:00"
    )
    mutations.append(wrong_interval)
    downstream_active = copy.deepcopy(snapshot)
    downstream_active["consumer_runs"][0]["task_states"][
        "trigger_xref_transforms"
    ] = "running"
    mutations.append(downstream_active)

    for mutated in mutations:
        with pytest.raises(mod.DeploymentError):
            mod.validate_owner_committed_shared_recovery(
                mutated,
                stored_boundary=NEXT_SCHEDULE_BOUNDARY,
                live_boundary=ADVANCED_SCHEDULE_BOUNDARY,
            )

    with pytest.raises(mod.DeploymentError, match="exactly one"):
        mod.validate_owner_committed_shared_recovery(
            snapshot,
            stored_boundary=NEXT_SCHEDULE_BOUNDARY,
            live_boundary={
                key: (
                    datetime.fromisoformat(value) + timedelta(days=2)
                ).isoformat()
                for key, value in NEXT_SCHEDULE_BOUNDARY.items()
            },
        )
@pytest.mark.parametrize(
    ("boundary", "now"),
    (
        (NEXT_SCHEDULE_BOUNDARY, datetime(2026, 7, 21, 13, 29, tzinfo=timezone.utc)),
        (NEXT_SCHEDULE_BOUNDARY, datetime(2026, 7, 21, 14, 45, tzinfo=timezone.utc)),
        (NEXT_SCHEDULE_BOUNDARY, datetime(2026, 7, 21, 15, 0, tzinfo=timezone.utc)),
        (NEXT_SCHEDULE_BOUNDARY, datetime(2026, 7, 22, 13, 0, tzinfo=timezone.utc)),
        (
            {
                **NEXT_SCHEDULE_BOUNDARY,
                "data_interval_start": "2026-07-21T13:00:00+00:00",
                "logical_date": "2026-07-21T13:00:00+00:00",
            },
            datetime(2026, 7, 21, 13, 0, tzinfo=timezone.utc),
        ),
        (
            {
                **NEXT_SCHEDULE_BOUNDARY,
                "data_interval_end": "2026-07-21T15:00:00+00:00",
                "run_after": "2026-07-21T15:00:00+00:00",
            },
            datetime(2026, 7, 21, 13, 0, tzinfo=timezone.utc),
        ),
    ),
)
def test_automatic_activation_boundary_rejects_unsafe_daily_cut(boundary, now):
    with pytest.raises(mod.DeploymentError, match="safe 14:00"):
        mod.validate_automatic_activation_boundary(boundary, now=now)


def test_atomic_writer_transaction_names_exact_six_and_rejects_partial_result():
    commands = []

    def run(command, **_kwargs):
        commands.append(command)
        payload = {
            "schema_version": "fotmob-writer-snapshot-v1",
            "transaction_id": "e" * 32,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "pause_states": {dag_id: True for dag_id in mod.EXPECTED_DAGS},
            "active_runs": {},
            "pause_states_after": {
                dag_id: dag_id
                not in {"dag_ingest_fotmob", "dag_transform_fotmob_silver"}
                for dag_id in mod.EXPECTED_DAGS
            },
            "phase": "children",
            "scheduler_state": {
                "next_background_lane": "refresh",
                "daily_date": None,
                "generation": 0,
                "updated_at": "1970-01-01T00:00:00+00:00",
            },
        }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_AUTOMATIC_WRITER_TX_JSON=" + json.dumps(payload),
            stderr="",
        )

    result = mod.atomic_automatic_writer_transition(
        "1" * 64, phase="children", selected_date="2026-07-21", run=run
    )
    code = commands[0][-1]
    compile(code, "<automatic-writer-transaction>", "exec")
    assert f"ids = {sorted(mod.EXPECTED_DAGS)!r}" in code
    assert "SERIALIZABLE" in code
    assert "with_for_update" in code
    assert result["active_runs"] == {}

    def partial(command, **_kwargs):
        payload = dict(result)
        payload["pause_states"] = {"dag_orchestrate_fotmob": True}
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_AUTOMATIC_WRITER_TX_JSON=" + json.dumps(payload),
            stderr="",
        )

    with pytest.raises(mod.DeploymentError, match="incomplete"):
        mod.atomic_automatic_writer_transition(
            "1" * 64,
            phase="children",
            selected_date="2026-07-21",
            run=partial,
        )


def test_atomic_shared_cutover_locks_full_shared_inventory():
    commands = []
    after = dict(fotmob_runtime.EXPECTED_SHARED_PAUSE_STATES)
    after[mod.SHARED_CONSUMER_DAG_ID] = False

    def run(command, **_kwargs):
        commands.append(command)
        payload = {
            "schema_version": "fotmob-shared-consumer-snapshot-v1",
            "transaction_id": "e" * 32,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "dag_id": mod.SHARED_CONSUMER_DAG_ID,
            "phase": "unpause",
            "pause_states_before": dict(
                fotmob_runtime.EXPECTED_SHARED_PAUSE_STATES
            ),
            "pause_states_after": after,
            "schedule_owner": "isolated",
            "active_runs": [],
        }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="FOTMOB_SHARED_CONSUMER_TX_JSON=" + json.dumps(payload),
            stderr="",
        )

    mod.atomic_shared_consumer_transition("9" * 64, phase="unpause", run=run)
    code = commands[0][-1]
    compile(code, "<shared-consumer-transaction>", "exec")
    assert f"active_ids = {sorted(fotmob_runtime.SHARED_STATE_DAGS)!r}" in code
    assert "fotmob_schedule_owner" in code
    assert "from airflow.models import DagModel, DagRun, TaskInstance, Variable" in code
    assert "consumer_runs" in code
    assert "with_for_update" in code
    assert "SERIALIZABLE" in code


def _install_automatic_activation_mocks(
    monkeypatch, args, deployment, isolated, shared, handoff
):
    events = []
    admission = _automatic_catalog_admission(
        deployment_id=deployment["deployment_id"],
        git_sha=deployment["git_sha"],
        scheduler_container_id=deployment["scheduler_container_id"],
    )
    activation = _automatic_rollout_certificate(
        admission,
        evidence_dir=args.evidence_dir,
        handoff=handoff,
    )["automatic_activation"]
    canary = {
        "current_run_reports": admission["current_run_reports"],
        "runner_report_sha256": admission["canary"]["runner_report_sha256"],
        "runner_report_bytes": 123,
    }
    monkeypatch.setattr(
        mod,
        "_validate_resume_identity",
        lambda *_args, **_kwargs: (args.report, isolated, shared),
    )
    monkeypatch.setattr(
        mod, "load_automatic_canary_report", lambda *_args, **_kwargs: canary
    )
    monkeypatch.setattr(
        mod.runtime_binding,
        "load_deployment_context",
        lambda *_args, **_kwargs: deployment,
    )
    monkeypatch.setattr(
        mod.runtime_binding,
        "validate_live_deployment",
        lambda *_args, **_kwargs: events.append("live_deployment"),
    )
    monkeypatch.setattr(
        mod.runtime_binding,
        "validate_live_shared_runtime",
        lambda *_args, **_kwargs: events.append("live_shared_runtime"),
    )
    monkeypatch.setattr(
        mod.runtime_binding,
        "assert_no_active_fotmob_publication",
        lambda *_args, **_kwargs: events.append("full_quiescence")
        or copy.deepcopy(activation["quiescence_before"]),
    )
    monkeypatch.setattr(
        mod,
        "validate_shared_handoff",
        lambda *_args, **_kwargs: events.append("fresh_shared_snapshot")
        or copy.deepcopy(handoff),
    )
    monkeypatch.setattr(
        mod,
        "validate_automatic_activation_boundary",
        lambda *_args, **_kwargs: copy.deepcopy(
            activation["daily_boundary_initial"]
        ),
    )
    monkeypatch.setattr(
        mod,
        "validate_live_automatic_canary",
        lambda *_args, **_kwargs: events.append("live_canary")
        or copy.deepcopy(activation["live_canary"]),
    )
    monkeypatch.setattr(
        mod,
        "collect_automatic_scope_observations",
        lambda *_args, **_kwargs: events.append("scope_snapshot") or {},
    )
    monkeypatch.setattr(
        mod,
        "build_automatic_catalog_admission",
        lambda *_args, **_kwargs: copy.deepcopy(admission),
    )
    monkeypatch.setattr(
        mod,
        "_configured_env_value",
        lambda *_args, **_kwargs: "postgresql://control",
    )

    def isolated_transition(_container, *, phase, selected_date=None, run):
        events.append("isolated_" + phase)
        if phase in {"children", "owner"}:
            return copy.deepcopy(activation[f"{phase}_transaction"])
        return {
            "schema_version": "fotmob-writer-snapshot-v1",
            "transaction_id": "e" * 32,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "pause_states": {dag_id: True for dag_id in mod.EXPECTED_DAGS},
            "active_runs": {},
            "pause_states_after": {},
            "phase": phase,
            "scheduler_state": {
                "next_background_lane": "refresh",
                "daily_date": None,
                "generation": 0,
                "updated_at": "1970-01-01T00:00:00+00:00",
            },
        }

    def shared_transition(_container, *, phase, recovery_boundary=None, run):
        events.append("shared_" + phase)
        if phase == "unpause":
            return copy.deepcopy(activation["shared_consumer_transaction"])
        if phase == "inspect_unpaused":
            return copy.deepcopy(activation["shared_consumer_readback"])
        return {"phase": phase, "pause_states_after": {}, "active_runs": []}

    monkeypatch.setattr(mod, "atomic_automatic_writer_transition", isolated_transition)
    monkeypatch.setattr(mod, "atomic_shared_consumer_transition", shared_transition)
    monkeypatch.setattr(
        mod,
        "assert_no_active_control_publication",
        lambda *_args, **_kwargs: events.append("control_quiescence")
        or copy.deepcopy(activation["control_quiescence_at_commit"]),
    )
    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda _path, payload: events.append("write_" + payload["activation_state"]),
    )
    return events


def test_automatic_activation_enables_children_shared_then_owner_last(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )

    def run(command, **_kwargs):
        assert command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        )
        return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "active"
    assert result["paused"] == sorted(mod.LEGACY_OWNER_DAGS)
    assert result["unpaused"] == sorted(mod.AUTOMATIC_ACTIVE_DAGS)
    assert events.index("fresh_shared_snapshot") < events.index("full_quiescence")
    assert events.index("full_quiescence") < events.index("live_canary")
    assert events.index("scope_snapshot") < events.index("isolated_children")
    assert events.index("write_pending_automatic") < events.index("shared_unpause")
    assert events.index("shared_inspect_unpaused") < events.index("isolated_owner")
    assert events.index("isolated_owner") < events.index("write_active")


def test_automatic_activation_owner_failure_preserves_shared_pending_state(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    original = mod.atomic_automatic_writer_transition

    def fail_owner(container, *, phase, selected_date=None, run):
        if phase == "owner":
            events.append("isolated_owner_failed")
            raise mod.DeploymentError("owner failed")
        return original(
            container, phase=phase, selected_date=selected_date, run=run
        )

    monkeypatch.setattr(mod, "atomic_automatic_writer_transition", fail_owner)

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        if command[:2] == ("docker", "stop"):
            events.append("scheduler_stop")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["durable_pending_report_preserved"] is True
    assert "isolated_pause_all" not in events
    assert "shared_pause" not in events
    assert "scheduler_stop" not in events


def test_automatic_activation_preserves_pending_when_shared_response_is_lost(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    original = mod.atomic_shared_consumer_transition

    def lose_unpause_response(container, *, phase, run):
        evidence = original(container, phase=phase, run=run)
        if phase == "unpause":
            raise OSError("docker response lost after shared commit")
        return evidence

    monkeypatch.setattr(
        mod, "atomic_shared_consumer_transition", lose_unpause_response
    )

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["durable_pending_report_preserved"] is True
    assert result["automatic_rollout"]["phase"] == (
        "shared_committed_pending_owner"
    )
    assert "isolated_pause_all" not in events
    assert "shared_pause" not in events


def test_automatic_activation_preserves_pending_when_owner_response_is_lost(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    original = mod.atomic_automatic_writer_transition
    active_pause_shape = {
        dag_id: dag_id in mod.LEGACY_OWNER_DAGS for dag_id in mod.EXPECTED_DAGS
    }

    def lose_owner_response(container, *, phase, selected_date=None, run):
        evidence = original(
            container, phase=phase, selected_date=selected_date, run=run
        )
        if phase == "owner":
            raise OSError("docker response lost after owner commit")
        return evidence

    monkeypatch.setattr(
        mod, "atomic_automatic_writer_transition", lose_owner_response
    )
    monkeypatch.setattr(
        mod,
        "inspect_automatic_writer_pause_shape",
        lambda *_args, **_kwargs: {
            "pause_states": active_pause_shape,
            "active_runs": [],
            "atomic_metadata_snapshot": True,
        },
    )

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["durable_pending_report_preserved"] is True
    assert result["automatic_rollout"]["phase"] == (
        "owner_committed_pending_report"
    )
    assert "isolated_pause_all" not in events
    assert "shared_pause" not in events


def test_automatic_activation_never_stops_after_owner_commit(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )

    def write(_path, payload):
        events.append("write_" + payload["activation_state"])
        if payload["activation_state"] == "active":
            raise OSError("disk full")

    monkeypatch.setattr(mod, "_atomic_json", write)

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        if command[:2] == ("docker", "stop"):
            events.append("scheduler_stop")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["recovery_required"] is True
    assert result["durable_pending_report_preserved"] is True
    assert events.count("write_pending_automatic") == 1
    assert "isolated_pause_all" not in events
    assert "shared_pause" not in events
    assert "scheduler_stop" not in events


def test_main_preserves_wait_only_pending_file_after_final_active_write_failure(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    args.activate_automatic = True
    real_atomic_json = mod._atomic_json
    _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )

    def write(path, payload):
        if payload["activation_state"] == "active":
            raise OSError("disk full")
        real_atomic_json(path, payload)

    monkeypatch.setattr(mod, "_atomic_json", write)

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    activate = mod.activate_automatic_catalog
    monkeypatch.setattr(
        mod,
        "activate_automatic_catalog",
        lambda selected_args: activate(selected_args, run=run),
    )

    assert mod._main_locked(args) == 1
    durable = json.loads(args.report.read_text(encoding="utf-8"))
    assert durable["passed"] is True
    assert durable["activation_state"] == "pending_automatic"
    assert durable["automatic_rollout"]["phase"] == "pending_owner"
    assert "durable_pending_report_preserved" not in durable


def test_automatic_activation_refuses_invalid_final_certificate_after_owner(
    tmp_path, monkeypatch
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    original = mod.atomic_automatic_writer_transition

    def corrupt_owner(container, *, phase, selected_date=None, run):
        evidence = original(
            container,
            phase=phase,
            selected_date=selected_date,
            run=run,
        )
        if phase == "owner":
            evidence["transaction_id"] = "not-a-transaction"
        return evidence

    monkeypatch.setattr(mod, "atomic_automatic_writer_transition", corrupt_owner)

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["recovery_required"] is True
    assert "write_active" not in events
    assert "scheduler_stop" not in events


@pytest.mark.parametrize("failure", ("changed_boundary", "active_isolated_run"))
def test_owner_committed_recovery_rejects_unsafe_live_state(
    tmp_path, monkeypatch, failure
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    admission = _automatic_catalog_admission()
    certificate = _automatic_rollout_certificate(
        admission,
        evidence_dir=args.evidence_dir,
        handoff=handoff,
    )
    deployment.update(
        {
            "activation_state": "pending_automatic",
            "generated_at": "2026-07-21T13:36:00+00:00",
            "automatic_catalog_admission": admission,
            "automatic_rollout": {
                **certificate["automatic_rollout"],
                "phase": "owner_committed_pending_report",
            },
            "automatic_activation": certificate["automatic_activation"],
        }
    )
    args.report.write_text(json.dumps(deployment), encoding="utf-8")
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    active_pause_shape = {
        dag_id: dag_id in mod.LEGACY_OWNER_DAGS for dag_id in mod.EXPECTED_DAGS
    }
    monkeypatch.setattr(
        mod,
        "inspect_automatic_writer_pause_shape",
        lambda *_args, **_kwargs: {
            "pause_states": active_pause_shape,
            "active_runs": (
                [
                    {
                        "dag_id": "dag_ingest_fotmob",
                        "run_id": "manual__unsafe",
                        "state": "running",
                    }
                ]
                if failure == "active_isolated_run"
                else []
            ),
            "atomic_metadata_snapshot": True,
        },
    )
    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: copy.deepcopy(
            ADVANCED_SCHEDULE_BOUNDARY
            if failure == "changed_boundary"
            else NEXT_SCHEDULE_BOUNDARY
        ),
    )

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "pending_automatic"
    assert result["recovery_required"] is True
    expected_error = "changed" if failure == "changed_boundary" else "active isolated"
    assert expected_error in result["error"]
    assert "write_active" not in events
    assert "scheduler_stop" not in events


@pytest.mark.parametrize(
    ("lost_owner_evidence", "shared_state"),
    (
        (False, "idle"),
        (True, "idle"),
        (True, "active_wait"),
        (True, "terminal_failed"),
    ),
)
def test_owner_committed_recovery_promotes_only_exact_shared_state(
    tmp_path, monkeypatch, lost_owner_evidence, shared_state
):
    args, deployment, isolated, shared, handoff = _automatic_activation_fixture(
        tmp_path
    )
    admission = _automatic_catalog_admission(
        now=datetime(2026, 7, 21, 13, 35, tzinfo=timezone.utc)
    )
    certificate = _automatic_rollout_certificate(
        admission,
        evidence_dir=args.evidence_dir,
        handoff=handoff,
    )
    activation = copy.deepcopy(certificate["automatic_activation"])
    if lost_owner_evidence:
        for key in (
            "shared_consumer_transaction",
            "shared_consumer_readback",
            "control_quiescence_at_commit",
            "owner_transaction",
        ):
            activation.pop(key)
        activation["shared_consumer_unpaused"] = False
        activation["owner_unpaused_last"] = False
    deployment.update(
        {
            "activation_state": "pending_automatic",
            "generated_at": "2026-07-21T13:36:00+00:00",
            "automatic_catalog_admission": admission,
            "automatic_rollout": {
                **certificate["automatic_rollout"],
                "phase": "pending_owner",
            },
            "automatic_activation": activation,
        }
    )
    args.report.write_text(json.dumps(deployment), encoding="utf-8")
    events = _install_automatic_activation_mocks(
        monkeypatch, args, deployment, isolated, shared, handoff
    )
    if shared_state != "idle":
        monkeypatch.setattr(
            mod, "_now", lambda: "2026-07-21T16:00:00+00:00"
        )
    active_pause_shape = {
        dag_id: dag_id in mod.LEGACY_OWNER_DAGS for dag_id in mod.EXPECTED_DAGS
    }
    monkeypatch.setattr(
        mod,
        "inspect_automatic_writer_pause_shape",
        lambda *_args, **_kwargs: {
            "pause_states": active_pause_shape,
            "active_runs": [],
            "atomic_metadata_snapshot": True,
        },
    )
    if shared_state != "idle":
        shared_readback = copy.deepcopy(
            certificate["automatic_activation"]["shared_consumer_readback"]
        )
        expected_run_id = mod._scheduled_run_id(
            NEXT_SCHEDULE_BOUNDARY["logical_date"]
        )
        consumer = {
            "dag_id": mod.SHARED_CONSUMER_DAG_ID,
            "run_id": expected_run_id,
            "run_type": "scheduled",
            "state": "failed" if shared_state == "terminal_failed" else "running",
            "logical_date": "2026-07-20T14:00:00.000000+00:00",
            "data_interval_start": "2026-07-20T14:00:00.000000+00:00",
            "data_interval_end": "2026-07-21T14:00:00.000000+00:00",
            "task_states": {
                "wait_for_fotmob_publication": (
                    "failed"
                    if shared_state == "terminal_failed"
                    else "up_for_reschedule"
                ),
                "trigger_xref_transforms": None,
                "trigger_e3_transforms": None,
                "trigger_e4_transforms": None,
                "finalize_fotmob_publication": None,
            },
        }
        if shared_state == "terminal_failed":
            consumer["task_states"].update(
                {
                    "trigger_xref_transforms": "upstream_failed",
                    "trigger_e3_transforms": "upstream_failed",
                    "trigger_e4_transforms": "upstream_failed",
                    "finalize_fotmob_publication": "failed",
                }
            )
        shared_readback["active_runs"] = (
            []
            if shared_state == "terminal_failed"
            else [
                {
                    "dag_id": mod.SHARED_CONSUMER_DAG_ID,
                    "run_id": expected_run_id,
                    "state": "running",
                }
            ]
        )
        shared_readback["consumer_runs"] = [consumer]
        original_shared = mod.atomic_shared_consumer_transition

        def shared_transition(container, *, phase, recovery_boundary=None, run):
            if phase == "inspect_unpaused":
                events.append("shared_" + phase)
                return copy.deepcopy(shared_readback)
            return original_shared(
                container,
                phase=phase,
                recovery_boundary=recovery_boundary,
                run=run,
            )

        monkeypatch.setattr(
            mod, "atomic_shared_consumer_transition", shared_transition
        )
    monkeypatch.setattr(
        mod,
        "read_schedule_boundary",
        lambda *_args, **_kwargs: copy.deepcopy(
            ADVANCED_SCHEDULE_BOUNDARY
            if shared_state != "idle"
            else NEXT_SCHEDULE_BOUNDARY
        ),
    )

    def run(command, **_kwargs):
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
        ):
            return subprocess.CompletedProcess(command, 0, stdout="true\n", stderr="")
        raise AssertionError(command)

    result = mod.activate_automatic_catalog(args, run=run)

    assert result["activation_state"] == "active"
    summary = fotmob_runtime.validate_automatic_rollout_activation(
        result, admission
    )
    assert summary["recovered"] is True
    assert "write_active" in events
    assert "isolated_pause_all" not in events
    assert "shared_pause" not in events
    assert "scheduler_stop" not in events


def test_ordinary_deploy_cannot_overwrite_pending_automatic_bytes(tmp_path):
    report = tmp_path / "deployment.json"
    payload = {
        "schema_version": "fotmob-deploy-v2",
        "passed": False,
        "activation_state": "pending_automatic",
        "automatic_rollout": {
            "schema_version": mod.AUTOMATIC_ROLLOUT_SCHEMA,
            "phase": "owner_committed_pending_report",
        },
    }
    report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    original = report.read_bytes()

    with pytest.raises(mod.DeploymentError, match="activate-automatic"):
        mod._guard_existing_pending_activation(report)

    assert report.read_bytes() == original
