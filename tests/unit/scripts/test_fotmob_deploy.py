import importlib.util
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from scripts import fotmob_runtime


SCRIPT = Path(__file__).resolve().parents[3] / "deploy/fotmob/deploy.py"
SPEC = importlib.util.spec_from_file_location("fotmob_deploy", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


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
        "dags/dag_transform_fotmob_silver.py",
        "dags/dag_trigger_fotmob_daily.py",
        "deploy/fotmob/.airflowignore",
    ):
        path = release / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative)
    projection = mod.prepare_dagbag(release, evidence, "a" * 40)
    assert {path.name for path in projection.iterdir()} == {
        "dag_ingest_fotmob.py",
        "dag_transform_fotmob_silver.py",
        "dag_trigger_fotmob_daily.py",
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
        if relative_path == mod.APPROVED_SCOPE_PATH:
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
            "dag_sofascore_pipeline": False,
            "dag_ingest_fotmob": True,
            "dag_transform_fotmob_silver": True,
        },
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
                    }
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
        "dag_sofascore_pipeline": False,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": True,
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
            "dag_sofascore_pipeline": False,
            "dag_ingest_fotmob": True,
            "dag_transform_fotmob_silver": True,
        }
    )
    with pytest.raises(mod.DeploymentError, match="master/ingest/Silver paused"):
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
        "dag_sofascore_pipeline": False,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": False,
    }
    with pytest.raises(mod.DeploymentError, match="master/ingest/Silver paused"):
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

    with pytest.raises(mod.DeploymentError, match="active runs"):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    flattened = [part for command in calls for part in command]
    assert "up" not in flattened
    assert "stop" not in flattened
    assert "pause" not in flattened


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

    with pytest.raises(mod.DeploymentError, match="image ID"):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    assert any("stop" in command for command in calls)


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

    with pytest.raises(subprocess.CalledProcessError):
        mod.deploy(arguments, run=run, sleeper=lambda _: None)
    assert any(
        "stop" in command and "airflow-scheduler" in command for command in calls
    )


def test_trigger_activation_is_active_before_unpause_kill_point(tmp_path):
    report_path = tmp_path / "deployment.json"
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "deployment_id": "f" * 32,
    }

    class SimulatedProcessKill(BaseException):
        pass

    def airflow(*command):
        assert command == ("dags", "unpause", "dag_trigger_fotmob_daily")
        persisted = json.loads(report_path.read_text(encoding="utf-8"))
        assert persisted["activation_state"] == "active"
        assert persisted["paused"] == []
        assert set(persisted["unpaused"]) == mod.EXPECTED_DAGS
        raise SimulatedProcessKill

    with pytest.raises(SimulatedProcessKill):
        mod._commit_trigger_activation(
            report_path,
            report,
            airflow=airflow,
            assert_paused=lambda _expected: [],
        )

    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["passed"] is True
    assert persisted["activation_state"] == "active"


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

    def airflow(*_command):
        nonlocal unpause_called
        unpause_called = True

    monkeypatch.setattr(
        mod,
        "_atomic_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedProcessKill),
    )
    with pytest.raises(SimulatedProcessKill):
        mod._commit_trigger_activation(
            tmp_path / "deployment.json",
            {"schema_version": "fotmob-deploy-v2", "passed": True},
            airflow=airflow,
            assert_paused=lambda _expected: [],
        )
    assert unpause_called is False


def test_trigger_activation_commits_final_active_report(tmp_path):
    report_path = tmp_path / "deployment.json"
    observed_pause_assertions = []
    result = mod._commit_trigger_activation(
        report_path,
        {"schema_version": "fotmob-deploy-v2", "passed": True},
        airflow=lambda *_command: subprocess.CompletedProcess((), 0),
        assert_paused=lambda expected: observed_pause_assertions.append(expected) or [],
    )

    assert observed_pause_assertions == [set()]
    assert result["activation_state"] == "active"
    assert result["paused"] == []
    assert set(result["unpaused"]) == mod.EXPECTED_DAGS
    assert json.loads(report_path.read_text(encoding="utf-8")) == result
