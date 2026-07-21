"""Generation binding and cross-stack FotMob publication fence tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import fotmob_runtime
from utils import fotmob_publication as publication


pytestmark = pytest.mark.unit
DEPLOY_SCRIPT = Path(__file__).resolve().parents[3] / "deploy/fotmob/deploy.py"
DEPLOY_SPEC = importlib.util.spec_from_file_location(
    "fotmob_deploy_publication_parity", DEPLOY_SCRIPT
)
assert DEPLOY_SPEC is not None and DEPLOY_SPEC.loader is not None
deploy_validation = importlib.util.module_from_spec(DEPLOY_SPEC)
DEPLOY_SPEC.loader.exec_module(deploy_validation)
GIT_SHA = "a" * 40
OTHER_SHA = "b" * 40
START = datetime(2026, 7, 20, 14, tzinfo=timezone.utc)
END = START + timedelta(days=1)


def _issue_930_scope_file() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "configs"
        / "fotmob"
        / "issue-930-scopes.txt"
    )


def _issue_930_source_refresh_file() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "configs"
        / "fotmob"
        / "issue-930-player-source-refresh.json"
    )


def _isolated_runtime_evidence(tmp_path: Path):
    roots = {
        prefix: tmp_path / prefix.replace("/", "-")
        for prefix in publication.FOTMOB_ISOLATED_RUNTIME_ROOTS
    }
    for root in roots.values():
        root.mkdir(parents=True)
    for relative_path in publication.FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS:
        prefix = next(
            candidate
            for candidate in sorted(roots, key=len, reverse=True)
            if relative_path.startswith(candidate + "/")
        )
        path = roots[prefix] / relative_path.removeprefix(prefix + "/")
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative_path == "configs/fotmob/issue-930-scopes.txt":
            path.write_bytes(_issue_930_scope_file().read_bytes())
        elif relative_path == publication.PLAYER_SOURCE_REFRESH_ARTIFACT:
            path.write_bytes(_issue_930_source_refresh_file().read_bytes())
        else:
            path.write_text(relative_path, encoding="utf-8")
    manifest = publication.isolated_runtime_manifest(roots=roots)
    report_path = (tmp_path / "deployment.json").resolve()
    deployment_id = "f" * 32
    container_id = "1" * 64
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "activation_state": "active",
        "kept_paused": False,
        "paused": [],
        "unpaused": sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        "container_report_path": str(report_path),
        "generated_at": "2026-07-21T10:00:00Z",
        "deployment_id": deployment_id,
        "git_sha": GIT_SHA,
        "scheduler_container_id": container_id,
        "resolved_image_id": "sha256:" + "2" * 64,
        "isolated_runtime_sha256": manifest,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    environment = {
        publication.FOTMOB_ISOLATED_STACK_ENV: "1",
        publication.FOTMOB_DEPLOYMENT_ID_ENV: deployment_id,
        publication.FOTMOB_RUNTIME_FINGERPRINT_ENV: GIT_SHA,
        publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV: str(report_path),
        "TELEGRAM_BOT_TOKEN": "must-not-appear",
    }
    dag_run = SimpleNamespace(
        dag_id=publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
        run_type="scheduled",
    )
    return roots, report_path, report, environment, dag_run


def _shared_runtime_evidence(tmp_path: Path):
    roots = {
        prefix: tmp_path / ("shared-" + prefix.replace("/", "-"))
        for prefix in publication.FOTMOB_SHARED_RUNTIME_ROOTS
    }
    for root in roots.values():
        root.mkdir(parents=True)
    for relative_path in publication.FOTMOB_SHARED_REQUIRED_RUNTIME_PATHS:
        prefix = next(
            candidate
            for candidate in sorted(roots, key=len, reverse=True)
            if relative_path.startswith(candidate + "/")
        )
        path = roots[prefix] / relative_path.removeprefix(prefix + "/")
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative_path == "configs/fotmob/issue-930-scopes.txt":
            path.write_bytes(_issue_930_scope_file().read_bytes())
        elif relative_path == publication.PLAYER_SOURCE_REFRESH_ARTIFACT:
            path.write_bytes(_issue_930_source_refresh_file().read_bytes())
        else:
            path.write_text(relative_path, encoding="utf-8")
    manifest = publication.shared_runtime_manifest(roots=roots)
    report_path = (tmp_path / "shared-deployment.json").resolve()
    deployment_id = "e" * 32
    container_id = "3" * 64
    control = {
        "same_runtime_configuration": True,
        "shared": {
            "same_shared_database": True,
            "migrations": {
                "status": "passed",
                "checksum_verified": True,
                "versions": [1],
            },
        },
        "isolated": {
            "same_shared_database": True,
            "migrations": {
                "status": "passed",
                "checksum_verified": True,
                "versions": [1],
            },
        },
    }
    handoff = {
        "passed": True,
        "schedule_owner": "isolated",
        "runtime_git_sha": GIT_SHA,
        "shared_scheduler_container": container_id,
        "shared_admission_mount": {
            "type": "bind",
            "source": str(tmp_path.resolve()),
            "destination": publication.FOTMOB_SHARED_EVIDENCE_ROOT,
            "read_only": True,
            "report_path": str(report_path),
        },
        "runtime_code_sha256": manifest,
        "control_database": control["shared"],
    }
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "activation_state": "kept_paused",
        "kept_paused": True,
        "paused": sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        "unpaused": [],
        "deployment_id": deployment_id,
        "git_sha": GIT_SHA,
        "shared_container_report_path": str(report_path),
        "control_database": control,
        "shared_handoff_initial": handoff,
        "shared_handoff_final": dict(handoff),
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    environment = {
        publication.FOTMOB_RUNTIME_FINGERPRINT_ENV: GIT_SHA,
        publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV: str(report_path),
        "FBREF_CONTROL_DB_URI": "postgresql://control.example/airflow",
        "TRINO_PASSWORD": "must-not-appear",
    }
    dag_run = SimpleNamespace(
        dag_id=publication.FOTMOB_SHARED_MASTER_DAG_ID,
        run_type="scheduled",
    )
    return roots, report_path, report, environment, dag_run


def _pending_consumer_report(report: dict) -> dict[str, str]:
    boundary = {
        "logical_date": START.isoformat(timespec="microseconds"),
        "data_interval_start": START.isoformat(timespec="microseconds"),
        "data_interval_end": END.isoformat(timespec="microseconds"),
        "run_after": END.isoformat(timespec="microseconds"),
    }
    report.update(
        activation_state="pending_consumer",
        kept_paused=False,
        paused=[publication.FOTMOB_ISOLATED_DAILY_DAG_ID],
        unpaused=sorted(
            publication.FOTMOB_EXPECTED_ISOLATED_DAGS
            - {publication.FOTMOB_ISOLATED_DAILY_DAG_ID}
        ),
        schedule_boundary={
            "shared_dag_id": "dag_sofascore_pipeline",
            "isolated_dag_id": publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
            **{
                name: dict(boundary)
                for name in (
                    "shared_initial",
                    "shared_final",
                    "isolated_initial",
                    "isolated_final",
                    "shared_commit",
                    "isolated_commit",
                )
            },
            "exact_match": True,
        },
        activation_safety_window={
            "checked_at": (END - timedelta(hours=1)).isoformat(timespec="microseconds"),
            "next_boundary": END.isoformat(timespec="microseconds"),
            "remaining_seconds": 3600,
            "required_seconds": 900,
            "timeout_seconds": 300,
            "passed": True,
        },
        scheduled_activation={
            "status": "pending",
            "producer_dag_id": publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
            "consumer_dag_id": "dag_sofascore_pipeline",
            "resume_required": True,
        },
    )
    return boundary


def _mutate_pending_consumer_report(report: dict, case: str) -> None:
    proof = report["schedule_boundary"]
    activation = report["scheduled_activation"]
    safety = report["activation_safety_window"]
    if case == "valid":
        return
    if case == "valid_last_error":
        activation["last_error"] = "TimeoutError: consumer not created yet"
    elif case == "missing_proof_boundary":
        del proof["shared_commit"]
    elif case == "extra_proof_key":
        proof["forged"] = True
    elif case == "missing_boundary_field":
        del proof["shared_initial"]["run_after"]
    elif case == "extra_boundary_field":
        proof["shared_initial"]["forged"] = "value"
    elif case == "wrong_proof_dag":
        proof["shared_dag_id"] = "dag_master_pipeline"
    elif case == "wrong_isolated_proof_dag":
        proof["isolated_dag_id"] = "dag_ingest_fotmob"
    elif case == "false_exact_match":
        proof["exact_match"] = False
    elif case == "wrong_activation_dag":
        activation["consumer_dag_id"] = "dag_master_pipeline"
    elif case == "extra_activation_key":
        activation["forged"] = True
    elif case == "empty_last_error":
        activation["last_error"] = " "
    elif case == "missing_safety_field":
        del safety["checked_at"]
    elif case == "extra_safety_key":
        safety["forged"] = True
    elif case == "boolean_safety_integer":
        safety["timeout_seconds"] = True
    elif case == "bad_safety_formula":
        safety["timeout_seconds"] = 1200
        safety["required_seconds"] = 900
    elif case == "insufficient_remaining":
        safety["remaining_seconds"] = 899
    elif case == "inverted_safety_window":
        safety["next_boundary"] = safety["checked_at"]
    else:  # pragma: no cover - the parametrized corpus is exhaustive
        raise AssertionError(f"unknown pending mutation: {case}")


@pytest.mark.parametrize(
    ("case", "accepted"),
    (
        ("valid", True),
        ("valid_last_error", True),
        ("missing_proof_boundary", False),
        ("extra_proof_key", False),
        ("missing_boundary_field", False),
        ("extra_boundary_field", False),
        ("wrong_proof_dag", False),
        ("wrong_isolated_proof_dag", False),
        ("false_exact_match", False),
        ("wrong_activation_dag", False),
        ("extra_activation_key", False),
        ("empty_last_error", False),
        ("missing_safety_field", False),
        ("extra_safety_key", False),
        ("boolean_safety_integer", False),
        ("bad_safety_formula", False),
        ("insufficient_remaining", False),
        ("inverted_safety_window", False),
    ),
)
def test_pending_consumer_deploy_and_task_runtime_have_adversarial_parity(
    case, accepted
):
    report = {}
    _pending_consumer_report(report)
    _mutate_pending_consumer_report(report, case)
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    writer_identity = {
        "component": "bronze_runner",
        "mode": "daily",
        "publication": {"generation_id": generation_id, "binding": binding},
    }
    publication_error = type(publication._airflow_exception("test"))
    outcomes = []
    for validator, error in (
        (
            lambda: deploy_validation._validate_pending_report(report),
            deploy_validation.DeploymentError,
        ),
        (
            lambda: publication._validate_pending_consumer_runtime(
                report,
                require_scheduled_owner=False,
                writer_identity=writer_identity,
                context={},
                runtime_fingerprint_value=GIT_SHA,
            ),
            publication_error,
        ),
    ):
        try:
            validator()
            outcomes.append(True)
        except error:
            outcomes.append(False)

    assert outcomes == [accepted, accepted]


def test_scheduled_runtime_attestation_binds_report_container_and_manifest(tmp_path):
    roots, _path, _report, environment, dag_run = _isolated_runtime_evidence(tmp_path)

    result = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        dag_run=dag_run,
    )

    assert result["deployment_id"] == "f" * 32
    assert result["scheduler_container_id"] == "1" * 64
    assert result["runtime_file_count"] == len(
        publication.FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS
    )
    assert "must-not-appear" not in json.dumps(result)


def test_pending_runtime_allows_only_the_exact_scheduled_producer(
    tmp_path, monkeypatch
):
    roots, report_path, report, environment, _dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    _pending_consumer_report(report)
    report_path.write_text(json.dumps(report), encoding="utf-8")

    class DagRun:
        @staticmethod
        def generate_run_id(_run_type, logical_date):
            return "scheduled__" + logical_date.isoformat()

    models = sys.modules["airflow.models"]
    monkeypatch.setattr(models, "DagRun", DagRun, raising=False)
    types_module = ModuleType("airflow.utils.types")
    types_module.DagRunType = SimpleNamespace(SCHEDULED="scheduled")
    monkeypatch.setitem(sys.modules, "airflow.utils.types", types_module)
    expected_run_id = "scheduled__" + START.isoformat()
    dag_run = SimpleNamespace(
        dag_id=publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
        run_type="scheduled",
        run_id=expected_run_id,
    )

    admitted = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        dag_run=dag_run,
        data_interval_start=START,
        data_interval_end=END,
    )
    assert admitted["pending_consumer"]["generation_id"] == (
        publication.make_generation_id(_binding())
    )

    dag_run.run_id = "scheduled__forged"
    with pytest.raises(Exception, match="pending producer interval differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
            data_interval_start=START,
            data_interval_end=END,
        )


def test_pending_runtime_allows_only_exact_daily_child_writers(tmp_path):
    roots, report_path, report, environment, _dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    _pending_consumer_report(report)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    payload = {"generation_id": generation_id, "binding": binding}

    bronze = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        writer_identity={
            "component": "bronze_runner",
            "mode": "daily",
            "publication": payload,
        },
    )
    assert bronze["pending_consumer"]["generation_id"] == generation_id

    silver_run = SimpleNamespace(
        dag_id="dag_transform_fotmob_silver",
        run_id=f"fotmob_silver__{generation_id}",
        conf={publication.FOTMOB_PUBLICATION_CONF_KEY: payload},
    )
    silver = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        dag_run=silver_run,
    )
    assert silver["pending_consumer"]["generation_id"] == generation_id

    with pytest.raises(Exception, match="pending Bronze writer is not daily"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            require_scheduled_owner=False,
            writer_identity={
                "component": "bronze_runner",
                "mode": "replay",
                "publication": payload,
            },
        )

    silver_run.run_id = f"manual__{generation_id}"
    with pytest.raises(Exception, match="pending writer identity differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            require_scheduled_owner=False,
            dag_run=silver_run,
        )


def test_scheduled_runtime_attestation_rejects_drift_manual_and_pending(tmp_path):
    roots, report_path, report, environment, dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    service = roots["scrapers"] / "fotmob/service.py"
    service.write_text("drift", encoding="utf-8")
    with pytest.raises(Exception, match="runtime bytes differ"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )

    service.write_text("scrapers/fotmob/service.py", encoding="utf-8")
    with pytest.raises(Exception, match="exact scheduled DagRun"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=SimpleNamespace(
                dag_id=publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
                run_type="manual",
            ),
        )

    report["activation_state"] = "committed_pending_trigger"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(Exception, match="no completed active"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_shared_runtime_attestation_binds_report_container_control_and_bytes(
    tmp_path,
):
    roots, _path, _report, environment, dag_run = _shared_runtime_evidence(tmp_path)

    result = publication.attest_fotmob_shared_runtime(
        environ=environment,
        hostname="3" * 12,
        roots=roots,
        dag_run=dag_run,
    )

    assert result["deployment_id"] == "e" * 32
    assert result["shared_scheduler_container_id"] == "3" * 64
    assert result["runtime_file_count"] == len(
        publication.FOTMOB_SHARED_REQUIRED_RUNTIME_PATHS
    )
    assert result["control_database_bound"] is True
    assert "must-not-appear" not in json.dumps(result)


def test_shared_task_and_host_admission_cover_the_same_required_runtime():
    assert publication.FOTMOB_SHARED_REQUIRED_RUNTIME_PATHS == frozenset(
        fotmob_runtime.SHARED_REQUIRED_RUNTIME_PATHS
    )


def test_shared_runtime_attestation_rejects_drift_manual_and_stale_handoff(
    tmp_path,
):
    roots, report_path, report, environment, dag_run = _shared_runtime_evidence(
        tmp_path
    )
    service = roots["scrapers"] / "fotmob/service.py"
    service.write_text("drift", encoding="utf-8")
    with pytest.raises(Exception, match="runtime bytes differ"):
        publication.attest_fotmob_shared_runtime(
            environ=environment,
            hostname="3" * 12,
            roots=roots,
            dag_run=dag_run,
        )

    service.write_text("scrapers/fotmob/service.py", encoding="utf-8")
    with pytest.raises(Exception, match="exact scheduled master DagRun"):
        publication.attest_fotmob_shared_runtime(
            environ=environment,
            hostname="3" * 12,
            roots=roots,
            dag_run=SimpleNamespace(
                dag_id=publication.FOTMOB_SHARED_MASTER_DAG_ID,
                run_type="manual",
            ),
        )

    report["shared_handoff_final"]["shared_scheduler_container"] = "4" * 64
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(Exception, match="exact handoff identity"):
        publication.attest_fotmob_shared_runtime(
            environ=environment,
            hostname="3" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_shared_runtime_attestation_rejects_active_isolated_admission(tmp_path):
    roots, report_path, report, environment, dag_run = _shared_runtime_evidence(
        tmp_path
    )
    report.update(
        activation_state="active",
        kept_paused=False,
        paused=[],
        unpaused=sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
    )
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(Exception, match="no completed deployment admission"):
        publication.attest_fotmob_shared_runtime(
            environ=environment,
            hostname="3" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_kept_paused_attestation_allows_only_exact_issue930_bronze_and_silver(
    tmp_path,
):
    roots, report_path, report, environment, _dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    report.update(
        activation_state="kept_paused",
        kept_paused=True,
        paused=sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        unpaused=[],
    )
    start = datetime(2026, 7, 22, 10, 0, 1, tzinfo=timezone.utc)
    binding = publication.make_publication_binding(
        owner="isolated",
        data_interval_start=start,
        data_interval_end=start + timedelta(seconds=1),
        fingerprint=GIT_SHA,
    )
    lifecycle_publication = {
        "generation_id": publication.make_generation_id(binding),
        "binding": binding,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    scopes = _issue_930_scope_file().read_text(encoding="utf-8").splitlines()
    bronze = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        writer_identity={
            "component": "bronze_runner",
            "mode": "replay",
            "scopes": scopes,
            "entities": sorted(publication.FOTMOB_ISSUE930_WRITER_ENTITIES),
            "competition_limit": 0,
            "season_limit": 0,
            "publication": lifecycle_publication,
        },
    )
    assert bronze["issue930_lifecycle"] == {
        "mode": "replay",
        "attempt": 1,
        "generation_id": lifecycle_publication["generation_id"],
    }

    silver_run = SimpleNamespace(
        dag_id="dag_transform_fotmob_silver",
        run_id="fotmob_silver__" + lifecycle_publication["generation_id"],
        conf={"fotmob_publication": lifecycle_publication},
    )
    silver = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        dag_run=silver_run,
    )
    assert silver["issue930_lifecycle"]["mode"] == "replay"

    with pytest.raises(Exception, match="mode/attempt identity differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            require_scheduled_owner=False,
            allow_kept_paused_writer=True,
            writer_identity={
                "component": "bronze_runner",
                "mode": "daily",
                "publication": lifecycle_publication,
            },
        )


def test_kept_paused_attestation_binds_exact_source_refresh_profile(tmp_path):
    roots, report_path, report, environment, _dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    report.update(
        activation_state="kept_paused",
        kept_paused=True,
        paused=sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        unpaused=[],
    )
    start = datetime(2026, 7, 22, 10, 0, 0, tzinfo=timezone.utc)
    binding = publication.make_publication_binding(
        owner="isolated",
        data_interval_start=start,
        data_interval_end=start + timedelta(seconds=1),
        fingerprint=GIT_SHA,
    )
    lifecycle_publication = {
        "generation_id": publication.make_generation_id(binding),
        "binding": binding,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    identity = {
        "component": "bronze_runner",
        "mode": "backfill",
        "scopes": [],
        "entities": ["players"],
        "competition_limit": 0,
        "season_limit": 0,
        "match_limit": 0,
        "team_limit": 0,
        "player_limit": 0,
        "max_requests": publication.PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        "max_direct_mib": publication.PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
        "max_proxy_mib": 0,
        "requests_per_minute": 30,
        "max_attempts": 4,
        "next_build_id": "",
        "source_refresh_profile": publication.PLAYER_SOURCE_REFRESH_PROFILE,
        "source_refresh_targets_sha256": (publication.PLAYER_SOURCE_REFRESH_SHA256),
        "source_refresh_target_count": (publication.PLAYER_SOURCE_REFRESH_TARGET_COUNT),
        "publication": lifecycle_publication,
    }

    admitted = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        writer_identity=identity,
    )

    assert admitted["issue930_lifecycle"] == {
        "mode": "backfill",
        "attempt": 1,
        "generation_id": lifecycle_publication["generation_id"],
        "source_refresh_profile": publication.PLAYER_SOURCE_REFRESH_PROFILE,
        "source_refresh_targets_sha256": publication.PLAYER_SOURCE_REFRESH_SHA256,
        "source_refresh_target_count": publication.PLAYER_SOURCE_REFRESH_TARGET_COUNT,
    }

    mutations = {
        "source_refresh_profile": "unreviewed",
        "source_refresh_targets_sha256": "0" * 64,
        "source_refresh_target_count": 8,
        "max_requests": publication.PLAYER_SOURCE_REFRESH_MAX_REQUESTS - 1,
        "player_limit": 7,
    }
    for field, value in mutations.items():
        with pytest.raises(Exception, match="source-refresh contract differs"):
            publication.attest_fotmob_isolated_runtime(
                environ=environment,
                hostname="1" * 12,
                roots=roots,
                require_scheduled_owner=False,
                allow_kept_paused_writer=True,
                writer_identity={**identity, field: value},
            )


def test_isolated_owner_missing_role_env_never_skips_attestation(tmp_path):
    roots, _path, _report, environment, dag_run = _isolated_runtime_evidence(tmp_path)
    environment.pop(publication.FOTMOB_ISOLATED_STACK_ENV)
    with pytest.raises(Exception, match="runtime identity differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_daily_contract_derives_exact_competitions_from_immutable_scope_bytes():
    contract = publication.load_fotmob_daily_competition_contract(
        _issue_930_scope_file(),
        scope_sha256=publication.FOTMOB_DAILY_SCOPE_SHA256,
        competition_ids_sha256=(publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256),
    )

    assert contract["scope_count"] == 158
    assert contract["competition_count"] == 21
    assert contract["competition_ids"] == list(publication.FOTMOB_DAILY_COMPETITION_IDS)
    assert contract["competition_ids_sha256"] == (
        "664f972d5d86002131293bcc8da8382f6b7378cd43a8bd37a247c321decf689a"
    )


def test_daily_contract_rejects_same_count_identity_substitution(tmp_path):
    raw = _issue_930_scope_file().read_text(encoding="utf-8")
    mutated = tmp_path / "scopes.txt"
    mutated.write_text(raw.replace("42=2010/2011", "41=2010/2011"), encoding="utf-8")

    with pytest.raises(ValueError, match="artifact bytes differ"):
        publication.load_fotmob_daily_competition_contract(
            mutated,
            scope_sha256=publication.FOTMOB_DAILY_SCOPE_SHA256,
            competition_ids_sha256=(publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256),
        )


def test_daily_trigger_conf_is_exact_all_entity_profile():
    assert publication.fotmob_daily_trigger_conf() == {
        "mode": "daily",
        "scope": "",
        "daily_contract": "fotmob-daily-v1",
        "competition_scope_file": publication.FOTMOB_DAILY_SCOPE_FILE,
        "competition_scope_sha256": publication.FOTMOB_DAILY_SCOPE_SHA256,
        "competition_ids_sha256": (publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256),
        "entities": "season,leaderboards,matches,teams,players,transfers",
        "max_requests": 10_000,
        "max_direct_mib": 512,
        "competition_limit": 0,
        "season_limit": 0,
        "requests_per_minute": 60,
    }


def _binding(owner: str = "isolated", fingerprint: str = GIT_SHA):
    return publication.make_publication_binding(
        owner=owner,
        data_interval_start=START,
        data_interval_end=END,
        fingerprint=fingerprint,
    )


def _context(binding=None, generation_id=None, **states):
    binding = binding or _binding()
    generation_id = generation_id or publication.make_generation_id(binding)
    dag_run = SimpleNamespace(
        dag_id="dag_master_pipeline",
        run_id="scheduled__2026-07-20T14:00:00+00:00",
        conf={
            "fotmob_publication": {
                "generation_id": generation_id,
                "binding": binding,
            }
        },
        data_interval_start=START,
        data_interval_end=END,
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ],
    )
    return {
        "dag_run": dag_run,
        "data_interval_start": START,
        "data_interval_end": END,
        "ti": MagicMock(),
    }


def test_generation_id_binds_exact_interval_runtime_and_owner(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()

    assert publication.make_generation_id(binding) == publication.make_generation_id(
        dict(binding)
    )
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        _binding(owner="shared")
    )
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        _binding(fingerprint=OTHER_SHA)
    )
    shifted = dict(binding)
    shifted["data_interval_start"] = (START + timedelta(days=1)).isoformat(
        timespec="microseconds"
    )
    shifted["data_interval_end"] = (END + timedelta(days=1)).isoformat(
        timespec="microseconds"
    )
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        shifted
    )


def test_child_rejects_stale_runtime_and_wrong_generation(monkeypatch):
    binding = _binding()
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, OTHER_SHA)
    with pytest.raises(Exception, match="runtime fingerprint mismatch"):
        publication.publication_from_context(_context(binding=binding))

    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    with pytest.raises(Exception, match="does not match its binding"):
        publication.publication_from_context(
            _context(binding=binding, generation_id=str("1" * 32))
        )


def test_child_rejects_noncanonical_binding_even_with_matching_id(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    binding["source"] = "not-fotmob"

    with pytest.raises(Exception, match="binding is not canonical"):
        publication.publication_from_context(
            _context(binding=binding, generation_id=generation_id)
        )


def test_master_waits_for_writing_and_claims_only_exact_ready(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    store = SimpleNamespace(
        get_publication_generation=MagicMock(
            return_value={
                "generation_id": generation_id,
                "binding": binding,
                "phase": "writing",
                "status": "running",
                "active": True,
            }
        ),
        claim_publication_generation=MagicMock(),
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    assert (
        publication.wait_and_claim_fotmob_publication(
            publication_owner="isolated", **context
        )
        is False
    )
    store.claim_publication_generation.assert_not_called()

    store.get_publication_generation.return_value.update(
        phase="ready", status="succeeded"
    )
    store.claim_publication_generation.return_value = {"phase": "consuming"}
    assert (
        publication.wait_and_claim_fotmob_publication(
            publication_owner="isolated", **context
        )
        is True
    )
    kwargs = store.claim_publication_generation.call_args.kwargs
    assert kwargs["binding"] == binding
    assert kwargs["consumer"] == {
        "dag_id": "dag_master_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }
    assert kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_initializer_acquires_exact_generation_before_return(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.setenv(publication.FOTMOB_ISOLATED_STACK_ENV, "1")
    attestation = MagicMock(return_value={"runtime_manifest_sha256": "c" * 64})
    monkeypatch.setattr(publication, "attest_fotmob_isolated_runtime", attestation)
    initialize = MagicMock(return_value={"phase": "writing", "active": True})
    store = SimpleNamespace(initialize_publication_generation=initialize)
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context()
    context["dag_run"].dag_id = "dag_trigger_fotmob_daily"

    result = publication.initialize_fotmob_publication(
        publication_owner="isolated", **context
    )

    attestation.assert_called_once()
    assert result["generation_id"] == publication.make_generation_id(_binding())
    args, kwargs = initialize.call_args
    assert args == (result["generation_id"],)
    assert kwargs["dag_id"] == "dag_trigger_fotmob_daily"
    assert kwargs["binding"] == _binding()
    assert kwargs["source"] == "fotmob"
    assert kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_shared_initializer_attests_admitted_runtime_before_acquire(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    attestation = MagicMock(return_value={"runtime_manifest_sha256": "c" * 64})
    monkeypatch.setattr(publication, "attest_fotmob_shared_runtime", attestation)
    initialize = MagicMock(return_value={"phase": "writing", "active": True})
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(initialize_publication_generation=initialize),
    )
    context = _context(binding=_binding(owner="shared"))

    result = publication.initialize_fotmob_publication(
        publication_owner="shared", **context
    )

    attestation.assert_called_once()
    assert result["generation_id"] == publication.make_generation_id(
        _binding(owner="shared")
    )
    assert initialize.call_args.kwargs["binding"] == _binding(owner="shared")


def test_writer_preflight_holds_exact_phase_guard(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.setenv(publication.FOTMOB_ISOLATED_STACK_ENV, "1")
    events = []
    attestation = MagicMock(
        side_effect=lambda **_kwargs: (
            events.append("attest") or {"runtime_manifest_sha256": "c" * 64}
        )
    )
    monkeypatch.setattr(publication, "attest_fotmob_isolated_runtime", attestation)

    @contextmanager
    def guard(run_id, *, source):
        events.append(("enter", run_id, source))
        try:
            yield {"phase": "writing"}
        finally:
            events.append(("exit", run_id, source))

    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(guard_publication_writer=guard),
    )
    expected_id = publication.make_generation_id(_binding())

    assert (
        publication.validate_fotmob_writer_fence(**_context())["generation_id"]
        == expected_id
    )
    assert events == [
        "attest",
        ("enter", expected_id, "fotmob"),
        "attest",
        ("exit", expected_id, "fotmob"),
    ]
    assert attestation.call_count == 2
    assert attestation.call_args.kwargs["require_scheduled_owner"] is False
    assert attestation.call_args.kwargs["allow_kept_paused_writer"] is True


def test_shared_writer_attests_bind_bytes_before_and_after_guarded_work(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    events = []
    attestation = MagicMock(
        side_effect=lambda **_kwargs: (
            events.append("attest") or {"runtime_manifest_sha256": "d" * 64}
        )
    )
    monkeypatch.setattr(publication, "attest_fotmob_shared_runtime", attestation)

    @contextmanager
    def guard(run_id, *, source):
        events.append(("enter", run_id, source))
        try:
            yield {"phase": "writing"}
        finally:
            events.append(("exit", run_id, source))

    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(guard_publication_writer=guard),
    )
    binding = _binding(owner="shared")

    result = publication.validate_fotmob_writer_fence(**_context(binding=binding))

    assert result["binding"] == binding
    assert events == [
        "attest",
        ("enter", publication.make_generation_id(binding), "fotmob"),
        "attest",
        ("exit", publication.make_generation_id(binding), "fotmob"),
    ]
    assert attestation.call_count == 2
    assert attestation.call_args.kwargs["require_scheduled_owner"] is False


def test_writer_post_attestation_drift_fails_before_guard_release(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.setenv(publication.FOTMOB_ISOLATED_STACK_ENV, "1")
    events = []

    def attest(**_kwargs):
        events.append("attest")
        if events.count("attest") == 2:
            raise RuntimeError("post-operation runtime drift")
        return {"runtime_manifest_sha256": "c" * 64}

    @contextmanager
    def guard(_run_id, *, source):
        assert source == "fotmob"
        events.append("guard_enter")
        try:
            yield {"phase": "writing"}
        finally:
            events.append("guard_exit")

    monkeypatch.setattr(publication, "attest_fotmob_isolated_runtime", attest)
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(guard_publication_writer=guard),
    )

    with pytest.raises(RuntimeError, match="post-operation runtime drift"):
        with publication.fotmob_publication_writer(_context()):
            events.append("silver_write")

    assert events == [
        "attest",
        "guard_enter",
        "silver_write",
        "attest",
        "guard_exit",
    ]


def test_isolated_writer_missing_role_cannot_skip_runtime_attestation(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.delenv(publication.FOTMOB_ISOLATED_STACK_ENV, raising=False)
    store = MagicMock()
    monkeypatch.setattr(publication, "_control_store", store)

    with pytest.raises(Exception, match="report path is not exactly configured"):
        publication.validate_fotmob_writer_fence(**_context())

    store.assert_not_called()


def test_candidate_is_exact_digested_and_seal_renews_full_lease(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    record = MagicMock(return_value={"phase": "writing"})
    seal = MagicMock(return_value={"phase": "ready"})
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(
            record_publication_candidate=record,
            seal_publication_generation=seal,
        ),
    )
    values = {
        "silver_transforms.a": {
            "status": "success",
            "table": "iceberg.silver.a",
            "rows": 10,
        },
        "silver_transforms.b": {
            "status": "success",
            "table": "iceberg.silver.b",
            "rows": 20,
        },
        "validate_silver": {"status": "success", "warnings": []},
        "validate_silver_quality": {"passed": 2, "errors": [], "warnings": []},
    }
    context = _context()
    context["ti"].xcom_pull.side_effect = lambda task_ids: values[task_ids]

    candidate = publication.record_fotmob_silver_candidate(
        transform_task_ids=["silver_transforms.b", "silver_transforms.a"],
        **context,
    )
    assert candidate["transform_task_ids"] == [
        "silver_transforms.a",
        "silver_transforms.b",
    ]
    assert len(candidate["digest"]) == 64
    assert record.call_args.args[1] == candidate

    assert publication.seal_fotmob_publication(**context)["phase"] == "ready"
    assert seal.call_args.kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_xref_consumer_preflight_requires_full_active_claim(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    context["dag_run"].conf.update(
        publication_owner="dag_sofascore_pipeline",
        master_run_id="scheduled__2026-07-20T14:00:00+00:00",
    )
    consumer = {
        "dag_id": "dag_sofascore_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }
    store = SimpleNamespace(
        get_publication_generation=MagicMock(
            return_value={
                "binding": _binding(),
                "phase": "consuming",
                "status": "succeeded",
                "active": True,
                "consumer": consumer,
            }
        )
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    result = publication.validate_fotmob_consumer_fence(**context)
    assert result["consumer"] == consumer
    assert result["phase"] == "consuming"

    store.get_publication_generation.return_value["consumer"] = {
        "dag_id": "dag_master_pipeline",
        "run_id": consumer["run_id"],
    }
    with pytest.raises(Exception, match="consumer identity mismatch"):
        publication.validate_fotmob_consumer_fence(**context)


def test_raw_manual_xref_has_no_publication_authority(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    context["dag_run"].conf = {}

    with pytest.raises(Exception, match="DagRun conf requires fotmob_publication"):
        publication.validate_fotmob_consumer_fence(**context)


@pytest.mark.parametrize(
    "phase,status", [("failed", "failed"), ("published", "succeeded")]
)
def test_master_rejects_failed_or_already_published_generation(
    monkeypatch, phase, status
):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": binding,
            "phase": phase,
            "status": status,
            "active": phase != "published",
        }
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    with pytest.raises(Exception, match="terminal or invalid"):
        publication.wait_and_claim_fotmob_publication(
            publication_owner="isolated", **_context(binding=binding)
        )


def test_lost_child_response_never_releases_failed_writer(monkeypatch):
    """A failed parent trigger may leave the exact Silver child still writing."""

    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    fail = MagicMock(return_value={"phase": "failed", "released": False})
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(fail_publication_generation=fail),
    )
    context = _context(
        trigger_silver_transform="failed",
        seal_fotmob_publication_ready="upstream_failed",
    )

    with pytest.raises(Exception, match="lock retained"):
        publication.fail_unsealed_fotmob_publication(
            success_task_id="seal_fotmob_publication_ready",
            writer_task_ids=["trigger_silver_transform"],
            **context,
        )
    assert fail.call_args.kwargs["safe_to_release"] is False


def test_master_publishes_and_releases_only_after_sensor_and_report(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    complete = MagicMock(return_value={"phase": "published", "released": True})
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": _binding(),
            "phase": "consuming",
            "status": "succeeded",
        },
        complete_publication_generation=complete,
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context(
        wait_for_fotmob_publication="success",
        generate_pipeline_report="success",
    )

    result = publication.finalize_fotmob_publication_consumer(
        publication_owner="isolated",
        report_task_id="generate_pipeline_report",
        sensor_task_id="wait_for_fotmob_publication",
        **context,
    )
    assert result == {"phase": "published", "released": True}
    assert complete.call_args.kwargs["published"] is True
    assert complete.call_args.kwargs["consumer"] == {
        "dag_id": "dag_master_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }


@pytest.mark.parametrize("phase", ["ready", "consuming"])
def test_sofascore_failure_retains_ready_or_consuming_lock(monkeypatch, phase):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    complete = MagicMock()
    fail = MagicMock()
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": _binding(),
            "phase": phase,
            "status": "succeeded",
            "active": True,
        },
        complete_publication_generation=complete,
        fail_publication_generation=fail,
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context(
        wait_for_fotmob_publication=("failed" if phase == "ready" else "success"),
        trigger_e4_transforms="failed",
    )

    with pytest.raises(Exception, match="lock retained"):
        publication.finalize_fotmob_publication_consumer(
            publication_owner="isolated",
            report_task_id="trigger_e4_transforms",
            sensor_task_id="wait_for_fotmob_publication",
            release_unclaimed_ready_on_failure=False,
            **context,
        )
    complete.assert_not_called()
    fail.assert_not_called()


def test_consumer_trigger_conf_is_complete_and_uses_exact_sensor_xcom():
    conf = publication.fotmob_consumer_trigger_conf("dag_master_pipeline")

    assert conf["publication_owner"] == "dag_master_pipeline"
    assert conf["master_run_id"] == "{{ run_id }}"
    payload = conf["fotmob_publication"]
    assert "wait_for_fotmob_publication" in payload["generation_id"]
    assert set(payload["binding"]) == set(publication.FOTMOB_PUBLICATION_BINDING_FIELDS)
    assert all(
        "wait_for_fotmob_publication" in value for value in payload["binding"].values()
    )


def test_isolated_owner_initializes_before_deterministic_exact_trigger(
    monkeypatch,
):
    from airflow.operators.python import PythonOperator

    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    PythonOperator._instances.clear()
    sys.modules.pop("dag_trigger_fotmob_daily", None)
    sys.modules.pop("dags.dag_trigger_fotmob_daily", None)
    module = importlib.import_module("dag_trigger_fotmob_daily")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    initializer = tasks["initialize_fotmob_publication"]
    trigger = tasks["trigger_fotmob_ingest"]
    finalizer = tasks["finalize_fotmob_publication"]

    assert trigger.upstream_task_ids == {initializer.task_id}
    assert trigger._init_kwargs["trigger_run_id"].startswith("fotmob_ingest__")
    assert trigger._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert trigger._init_kwargs["reset_dag_run"] is False
    assert trigger._init_kwargs["execution_timeout"].total_seconds() == 14 * 3600
    expected_daily = publication.fotmob_daily_trigger_conf()
    assert {
        key: trigger._init_kwargs["conf"][key] for key in expected_daily
    } == expected_daily
    assert "execution_date" not in trigger._init_kwargs
    assert finalizer.upstream_task_ids == {trigger.task_id}
    assert finalizer._init_kwargs["trigger_rule"] == "all_done"
    assert module.dag._dag_kwargs["schedule"] == "0 14 * * *"
