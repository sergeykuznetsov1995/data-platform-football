import importlib.util
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from scripts import fotmob_runtime


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
