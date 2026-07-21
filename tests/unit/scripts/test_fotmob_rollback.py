import subprocess
import json

import pytest

from scripts import fotmob_rollback as mod
from tests.unit.scripts.fotmob_runtime_fixture import (
    isolated_runtime_proof,
    materialize_shared_runtime,
    schedule_boundary_proof,
    shared_handoff_proof,
)


def args(tmp_path, **overrides):
    compose_file = tmp_path / "compose.yaml"
    env_file = tmp_path / "fotmob.env"
    deployment_report = tmp_path / "deployment.json"
    compose_file.write_text("services: {}\n")
    env_file.write_text("TRINO_HOST=trino\n")
    deployment_id = "f" * 32
    scheduler_id = "1" * 64
    scheduler_image = "sha256:" + "d" * 64
    shared_control = {
        "same_shared_database": True,
        "migrations": {
            "status": "passed",
            "versions": [1],
            "checksum_verified": True,
        },
    }
    deployment_report.write_text(
        json.dumps(
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "activation_state": "kept_paused",
                "generated_at": "2026-07-21T10:00:00Z",
                "project": "fotmob-airflow",
                "compose_file": str(compose_file.resolve()),
                "release_root": str(tmp_path.resolve()),
                "evidence_dir": str((tmp_path / "evidence").resolve()),
                "container_report_path": ("/opt/airflow/logs/fotmob/deployment.json"),
                "shared_container_report_path": (
                    "/opt/airflow/fotmob-admission/deployment.json"
                ),
                "dagbag_root": str((tmp_path / "dagbag").resolve()),
                "git_sha": "a" * 40,
                "image": "registry/image@sha256:" + "b" * 64,
                "postgres_image": "postgres@sha256:" + "c" * 64,
                "resolved_image_id": scheduler_image,
                "resolved_postgres_image_id": "sha256:" + "e" * 64,
                "deployment_id": deployment_id,
                "scheduler_container_id": scheduler_id,
                "metadb_container_id": "2" * 64,
                "data_plane_marker": {
                    "table": "iceberg.bronze.fotmob_runtime_deployments",
                    "deployment_id": deployment_id,
                    "git_sha": "a" * 40,
                    "scheduler_container_id": scheduler_id,
                    "scheduler_image_id": scheduler_image,
                },
                "delivery_credentials": {
                    "telegram_bot_token_configured": True,
                    "telegram_chat_id_configured": True,
                },
                "isolated_runtime_sha256": isolated_runtime_proof(tmp_path),
                "control_database": {
                    "same_runtime_configuration": True,
                    "shared": shared_control,
                    "isolated": {
                        "migrations": {
                            "status": "passed",
                            "versions": [1],
                            "checksum_verified": True,
                        }
                    },
                },
                "shared_handoff_initial": shared_handoff_proof(
                    shared_control, release_root=tmp_path
                ),
                "shared_handoff_final": shared_handoff_proof(
                    shared_control, release_root=tmp_path
                ),
                "schedule_boundary": schedule_boundary_proof(),
                "kept_paused": True,
                "paused": [
                    "dag_ingest_fotmob",
                    "dag_transform_fotmob_silver",
                    "dag_trigger_fotmob_daily",
                ],
                "unpaused": [],
            }
        )
    )
    values = {
        "project": "fotmob-airflow",
        "compose_file": compose_file,
        "env_file": env_file,
        "deployment_report": deployment_report,
        "publication_report": None,
        "trino_env_file": env_file,
        "output": tmp_path / "rollback-output.json",
        "execute": False,
        "confirm": None,
        "expected_consumer_sha": "",
        "silver_run_id": "",
        "catalog": "iceberg",
        "bronze_schema": "bronze",
        "timeout_seconds": 1,
        "publication_attempt": 1,
    }
    values.update(overrides)
    return type("Args", (), values)()


def test_rollback_plan_explicitly_retains_native_objects(tmp_path):
    report = mod.rollback_plan(args(tmp_path))
    assert report["passed"] is True
    assert report["mutated"] is False
    assert report["deployed_git_sha"] == "a" * 40
    assert report["native_objects_action"] == "retain"
    assert [step["action"] for step in report["steps"]] == [
        "pause_writers",
        "deploy_consumer_revert",
        "run_fenced_legacy_silver_and_dq",
        "validate",
    ]


def test_runtime_rejects_v1_or_incomplete_shared_handoff_report(tmp_path):
    arguments = args(tmp_path)
    original = json.loads(arguments.deployment_report.read_text(encoding="utf-8"))

    invalid_reports = []
    v1 = json.loads(json.dumps(original))
    v1["schema_version"] = "fotmob-deploy-v1"
    invalid_reports.append((v1, "unsupported deployment report schema"))

    missing_delivery = json.loads(json.dumps(original))
    missing_delivery.pop("delivery_credentials")
    invalid_reports.append((missing_delivery, "runtime context"))

    missing_hash = json.loads(json.dumps(original))
    missing_hash["shared_handoff_final"]["runtime_code_sha256"].pop(
        "scrapers/fotmob/service.py"
    )
    invalid_reports.append((missing_hash, "exact shared code hashes"))

    stale_config = json.loads(json.dumps(original))
    stale_config["shared_handoff_final"]["runtime_code_sha256"][
        "configs/fotmob/competitions.json"
    ] = "0" * 64
    invalid_reports.append((stale_config, "exact shared code hashes"))

    missing_topology = json.loads(json.dumps(original))
    missing_topology["shared_handoff_final"].pop("serialized_xref")
    invalid_reports.append((missing_topology, "serialized xref"))

    wrong_report_path = json.loads(json.dumps(original))
    wrong_report_path["shared_container_report_path"] = (
        "/opt/airflow/fotmob-admission/other.json"
    )
    invalid_reports.append((wrong_report_path, "identify different files"))

    wrong_mount = json.loads(json.dumps(original))
    wrong_mount["shared_handoff_final"]["shared_admission_mount"]["source"] = (
        "/other/evidence"
    )
    invalid_reports.append((wrong_mount, "exact shared admission mount"))

    active_shared_run = json.loads(json.dumps(original))
    active_shared_run["shared_handoff_final"]["orchestration_state"]["active_runs"] = [
        {"dag_id": "dag_transform_xref", "run_id": "active"}
    ]
    invalid_reports.append((active_shared_run, "atomic shared quiescence"))

    incomplete_downstream_run_checks = json.loads(json.dumps(original))
    incomplete_downstream_run_checks["shared_handoff_final"]["active_run_checks"].pop(
        "dag_transform_e3"
    )
    invalid_reports.append(
        (incomplete_downstream_run_checks, "incomplete shared active-run proof")
    )

    unpaused_shared_daily = json.loads(json.dumps(original))
    unpaused_shared_daily["shared_handoff_final"]["orchestration_state"][
        "shared_daily_trigger"
    ] = {
        "isolated_stack_env": None,
        "serialized_present": True,
        "serialized_fileloc": "/opt/airflow/dags/dag_trigger_fotmob_daily.py",
        "dag_model_present": True,
        "dag_model_paused": False,
    }
    invalid_reports.append(
        (unpaused_shared_daily, "unsafe shared isolated daily trigger")
    )

    for payload, message in invalid_reports:
        arguments.deployment_report.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(mod.runtime_binding.RuntimeBindingError, match=message):
            mod.runtime_binding.load_deployment_context(
                arguments.deployment_report,
                project=arguments.project,
                compose_file=arguments.compose_file,
            )


def test_airflow_json_parser_ignores_bracketed_log_timestamp():
    assert mod._parse_json_array(
        '[2026-07-21T10:06:03] INFO\n[{"run_id":"daily-1"}]\n'
    ) == [{"run_id": "daily-1"}]


def test_pause_requires_exact_confirmation_before_subprocess(tmp_path):
    called = False

    def run(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("must not run")

    with pytest.raises(mod.RollbackError, match="nothing changed"):
        mod.pause_writers(args(tmp_path), run=run)
    assert called is False


def test_pause_pauses_all_dags_and_proves_no_running_runs(tmp_path, monkeypatch):
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        if "FOTMOB_WRITER_STATE_JSON=" in command[-1]:
            stdout = "FOTMOB_WRITER_STATE_JSON=" + json.dumps(
                [
                    {
                        "dag_id": dag_id,
                        "is_paused": True,
                        "run_id": None,
                        "state": None,
                    }
                    for dag_id in mod.DAGS
                ]
            )
        elif "printenv FOTMOB_DEPLOY_GIT_SHA" in command:
            stdout = "a" * 40
        else:
            stdout = ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(
        mod,
        "validate_live_deployment",
        lambda *_args, **_kwargs: {"mounts_verified": True},
    )
    report = mod.pause_writers(
        args(
            tmp_path,
            execute=True,
            confirm=mod.CONFIRM_PAUSE,
        ),
        run=run,
    )
    assert report["passed"] is True
    assert report["native_objects_action"] == "retain"
    assert sum("pause" in command for command in calls) == len(mod.DAGS)
    assert sum("FOTMOB_WRITER_STATE_JSON=" in command[-1] for command in calls) == 2
    assert report["pause_states"] == {dag_id: True for dag_id in mod.DAGS}
    assert report["project"] == "fotmob-airflow"


def test_pause_all_writers_attempts_every_dag_and_aggregates_safely(monkeypatch):
    attempted = []
    inspected = []
    verified = []
    writer_state = {
        "pause_states": {dag_id: dag_id != mod.DAGS[-1] for dag_id in mod.DAGS},
        "active_runs": {},
    }

    def airflow(_args, *command, **_kwargs):
        dag_id = command[-1]
        attempted.append(dag_id)
        if dag_id in mod.DAGS[:2]:
            raise subprocess.CalledProcessError(
                17 + mod.DAGS.index(dag_id),
                ("airflow", "dags", "pause", "secret-command-value"),
                stderr="secret-stderr-value",
            )
        return ""

    def inspect(*_args, **_kwargs):
        inspected.append(True)
        return writer_state

    def require(state):
        verified.append(state)
        raise mod.RollbackError("secret-verification-detail")

    monkeypatch.setattr(mod, "_airflow", airflow)
    monkeypatch.setattr(mod, "inspect_writer_state", inspect)
    monkeypatch.setattr(mod, "require_writers_stopped", require)

    with pytest.raises(mod.RollbackError) as raised:
        mod._pause_all_writers(object(), run=lambda *_a, **_k: None)

    assert attempted == list(mod.DAGS)
    assert mod.DAGS[-1] == "dag_trigger_fotmob_daily"
    assert inspected == [True]
    assert verified == [writer_state]
    message = str(raised.value)
    assert f"pause[{mod.DAGS[0]}]=CalledProcessError(returncode=17)" in message
    assert f"pause[{mod.DAGS[1]}]=CalledProcessError(returncode=18)" in message
    assert "require_writers_stopped=RollbackError" in message
    assert "secret-command-value" not in message
    assert "secret-stderr-value" not in message
    assert "secret-verification-detail" not in message


def test_pause_all_writers_runs_require_even_when_atomic_inspection_fails(
    monkeypatch,
):
    verified = []

    monkeypatch.setattr(mod, "_airflow", lambda *_a, **_k: "")
    monkeypatch.setattr(
        mod,
        "inspect_writer_state",
        lambda *_a, **_k: (_ for _ in ()).throw(
            mod.RollbackError("secret-inspection-detail")
        ),
    )

    def require(state):
        verified.append(state)
        raise mod.RollbackError("missing atomic evidence")

    monkeypatch.setattr(mod, "require_writers_stopped", require)

    with pytest.raises(mod.RollbackError) as raised:
        mod._pause_all_writers(object(), run=lambda *_a, **_k: None)

    assert verified == [{}]
    message = str(raised.value)
    assert "inspect_writer_state=RollbackError" in message
    assert "require_writers_stopped=RollbackError" in message
    assert "secret-inspection-detail" not in message


class LegacyClient:
    def __init__(self, count):
        self.count = count

    def query(self, _sql):
        return [(self.count,)]

    def close(self):
        pass


def test_legacy_validation_requires_every_table_nonempty():
    counts = mod._legacy_counts(LegacyClient(1), catalog="iceberg", schema="bronze")
    assert set(counts) == set(mod.LEGACY_TABLES)
    with pytest.raises(mod.RollbackError, match="legacy table is empty"):
        mod._legacy_counts(LegacyClient(0), catalog="iceberg", schema="bronze")


def test_legacy_validation_rejects_identifier_injection():
    with pytest.raises(mod.RollbackError, match="unsafe SQL identifier"):
        mod._legacy_counts(LegacyClient(1), catalog='iceberg"."bronze', schema="bronze")


@pytest.mark.parametrize("active_downstream", [False, True])
def test_publication_quiescence_is_read_only_and_runtime_bound(
    tmp_path, active_downstream
):
    release = tmp_path / "release"
    runtime_manifest = materialize_shared_runtime(release)
    context = {
        "scheduler_container_id": "1" * 64,
        "git_sha": "a" * 40,
        "release_root": str(release),
        "shared_handoff_final": {
            "shared_scheduler_container": "2" * 64,
            "runtime_code_sha256": runtime_manifest,
        },
    }
    isolated = {
        "Id": "1" * 64,
        "State": {"Running": False},
        "Config": {
            "Env": ["FBREF_CONTROL_DB_URI=postgresql://control@postgres/control"]
        },
    }
    shared = {
        "Id": "2" * 64,
        "State": {"Running": True},
        "Config": {
            "Env": [
                "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control",
                "FOTMOB_DEPLOY_GIT_SHA=" + "a" * 40,
            ]
        },
    }
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        if command[:2] == ("docker", "inspect"):
            payload = isolated if command[-1] == "1" * 64 else shared
            stdout = json.dumps(payload)
        elif "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" in command[-1]:
            stdout = "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" + json.dumps(
                runtime_manifest
            )
        elif "FOTMOB_PUBLICATION_QUIESCENCE_JSON=" in command[-1]:
            checks = {
                dag_id: {"running": [], "queued": []}
                for dag_id in mod.runtime_binding.SHARED_STATE_DAGS
            }
            if active_downstream:
                checks["dag_transform_e3"]["running"] = ["orphaned-e3"]
            stdout = "FOTMOB_PUBLICATION_QUIESCENCE_JSON=" + json.dumps(
                {
                    "source": "fotmob",
                    "safe": True,
                    "active": False,
                    "phase": None,
                    "active_run_checks": checks,
                    "shared_daily_trigger": {
                        "dag_model_present": False,
                        "dag_model_paused": None,
                    },
                }
            )
        else:
            raise AssertionError(f"unexpected command: {command}")
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    if active_downstream:
        with pytest.raises(
            mod.runtime_binding.RuntimeBindingError, match="prove quiescence"
        ):
            mod.runtime_binding.assert_no_active_fotmob_publication(context, run=run)
        return
    result = mod.runtime_binding.assert_no_active_fotmob_publication(context, run=run)
    assert result["safe"] is True
    assert result["control_database_bound"] is True
    executed_code = "\n".join(str(command) for command in calls)
    assert "assert_no_active_publication_generation" in executed_code
    assert "dag_transform_e3" in executed_code
    assert "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON=" in executed_code
    assert "release_publication" not in executed_code
    for command in calls:
        if command[-2:-1] == ("-c",):
            compile(command[-1], "<live-shared-proof>", "exec")


def _rollback_publication():
    return {
        "generation_id": "12345678-1234-5678-9234-123456789abc",
        "binding": {
            "schema": "fotmob-publication-v1",
            "source": "fotmob",
            "owner": "isolated",
            "data_interval_start": "2026-07-21T10:00:00.000000+00:00",
            "data_interval_end": "2026-07-21T10:00:01.000000+00:00",
            "runtime_fingerprint": "a" * 40,
        },
    }


def _publication_state(publication, *, status="running", phase="writing", active=True):
    return {
        "generation_id": publication["generation_id"],
        "binding": publication["binding"],
        "status": status,
        "phase": phase,
        "active": active,
        "owner_dag_id": "fotmob_rollback_validation",
        "candidate": {
            "generation_id": publication["generation_id"],
            "digest": "d" * 64,
            "transform_task_ids": ["silver_transforms.example"],
        },
    }


def test_container_python_json_uses_one_exact_python_code_argument(tmp_path):
    arguments = args(tmp_path)
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='ROLLBACK_JSON={"passed":true}\n',
            stderr="",
        )

    assert mod._container_python_json(
        arguments,
        code="print('exact')",
        marker="ROLLBACK_JSON=",
        run=run,
    ) == {"passed": True}
    assert calls[0][-3:] == ("python", "-c", "print('exact')")
    assert calls[0].count("-c") == 1


def test_publication_attempt_selects_an_exact_distinct_synthetic_interval(tmp_path):
    first = _rollback_publication()
    first["binding"]["data_interval_start"] = "2026-07-21T10:00:01.000000+00:00"
    first["binding"]["data_interval_end"] = "2026-07-21T10:00:02.000000+00:00"
    second = json.loads(json.dumps(first))
    second["generation_id"] = "22345678-1234-5678-9234-123456789abc"
    second["binding"]["data_interval_start"] = "2026-07-21T10:00:02.000000+00:00"
    second["binding"]["data_interval_end"] = "2026-07-21T10:00:03.000000+00:00"
    responses = [first, second]

    def run(command, **_kwargs):
        publication = responses.pop(0)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "FOTMOB_ROLLBACK_PUBLICATION_BINDING_JSON="
                + json.dumps(publication)
                + "\n"
            ),
            stderr="",
        )

    first_result = mod._rollback_publication_envelope(
        args(tmp_path, publication_attempt=1), run=run
    )
    second_result = mod._rollback_publication_envelope(
        args(tmp_path, publication_attempt=2), run=run
    )

    assert first_result["generation_id"] != second_result["generation_id"]
    assert first_result["binding"]["data_interval_start"].endswith(
        "10:00:01.000000+00:00"
    )
    assert second_result["binding"]["data_interval_start"].endswith(
        "10:00:02.000000+00:00"
    )


def test_run_silver_persists_acquire_before_exact_fenced_trigger(tmp_path, monkeypatch):
    publication = _rollback_publication()
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RUN_SILVER,
        expected_consumer_sha="a" * 40,
    )
    writer_state = {
        "pause_states": {dag_id: True for dag_id in mod.DAGS},
        "active_runs": {},
    }
    phases = []
    airflow_calls = []
    transitions = []
    original_atomic = mod._atomic_json
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "inspect_writer_state", lambda *_a, **_k: writer_state)
    monkeypatch.setattr(
        mod, "require_no_active_fotmob_publication", lambda *_a, **_k: {"safe": True}
    )
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(
        mod,
        "_initialize_rollback_publication",
        lambda *_a, **_k: _publication_state(publication),
    )
    monkeypatch.setattr(mod, "_pause_all_writers", lambda *_a, **_k: writer_state)
    monkeypatch.setattr(
        mod,
        "_get_rollback_publication",
        lambda *_a, **_k: _publication_state(publication),
    )

    def atomic(path, payload):
        phases.append(payload["phase"])
        original_atomic(path, payload)

    def airflow(_args, *command, **_kwargs):
        airflow_calls.append(command)
        if "trigger" in command:
            assert phases[-1] == "acquired_pending_trigger"
            conf = json.loads(command[command.index("--conf") + 1])
            assert conf == {"fotmob_publication": publication}
        if "list-runs" in command:
            return json.dumps(
                [
                    {
                        "run_id": "rollback_silver__"
                        + publication["generation_id"].replace("-", ""),
                        "state": "success",
                        "start_date": "2026-07-21T10:01:00Z",
                    }
                ]
            )
        return ""

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        if action == "seal":
            return _publication_state(
                publication, status="succeeded", phase="ready", active=True
            )
        assert action == "abandon"
        return {
            **_publication_state(
                publication, status="succeeded", phase="abandoned", active=False
            ),
            "released": True,
            "published": False,
        }

    monkeypatch.setattr(mod, "_atomic_json", atomic)
    monkeypatch.setattr(mod, "_airflow", airflow)
    monkeypatch.setattr(mod, "_transition_rollback_publication", transition)
    report = mod.run_rollback_validation_silver(
        arguments, sleeper=lambda _seconds: None
    )

    assert report["passed"] is True
    assert report["phase"] == "abandoned"
    assert transitions == ["seal", "abandon"]
    assert phases[:2] == ["prepared_pending_acquire", "acquired_pending_trigger"]
    assert phases[-1] == "abandoned"
    assert any("trigger" in command for command in airflow_calls)


def test_run_silver_retains_lock_after_ambiguous_trigger(tmp_path, monkeypatch):
    publication = _rollback_publication()
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RUN_SILVER,
        expected_consumer_sha="a" * 40,
    )
    writer_state = {
        "pause_states": {dag_id: True for dag_id in mod.DAGS},
        "active_runs": {},
    }
    transitions = []
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "inspect_writer_state", lambda *_a, **_k: writer_state)
    monkeypatch.setattr(
        mod, "require_no_active_fotmob_publication", lambda *_a, **_k: {"safe": True}
    )
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(
        mod,
        "_initialize_rollback_publication",
        lambda *_a, **_k: _publication_state(publication),
    )
    monkeypatch.setattr(mod, "_pause_all_writers", lambda *_a, **_k: writer_state)

    def airflow(_args, *command, **_kwargs):
        if "trigger" in command:
            raise subprocess.CalledProcessError(1, command)
        return ""

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        return {
            **_publication_state(publication, status="failed", phase="failed"),
            "released": False,
        }

    monkeypatch.setattr(mod, "_airflow", airflow)
    monkeypatch.setattr(mod, "_transition_rollback_publication", transition)
    report = mod.run_rollback_validation_silver(arguments)

    assert report["passed"] is False
    assert report["phase"] == "lock_retained_pending_terminal_proof"
    assert report["recovery_required"] is True
    assert transitions == ["fail_retain"]


def test_recovery_keeps_lock_until_exact_silver_run_is_terminal(tmp_path, monkeypatch):
    publication = _rollback_publication()
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RECOVER_PUBLICATION,
        publication_report=tmp_path / "publication.json",
    )
    report = mod._rollback_publication_report_base(
        arguments,
        publication,
        silver_run_id="rollback_silver__"
        + publication["generation_id"].replace("-", ""),
    )
    report.update(
        {"phase": "lock_retained_pending_terminal_proof", "recovery_required": True}
    )
    arguments.publication_report.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "_pause_all_writers", lambda *_a, **_k: {})
    monkeypatch.setattr(
        mod,
        "_exact_silver_run",
        lambda *_a, **_k: {"run_id": report["silver_run_id"], "state": "running"},
    )
    monkeypatch.setattr(
        mod,
        "_transition_rollback_publication",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("non-terminal recovery must not release")
        ),
    )

    recovered = mod.recover_rollback_publication(arguments)
    assert recovered["passed"] is False
    assert recovered["phase"] == "lock_retained_pending_terminal_proof"


def test_recovery_releases_exact_pretrigger_generation_when_run_is_absent(
    tmp_path, monkeypatch
):
    publication = _rollback_publication()
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RECOVER_PUBLICATION,
        publication_report=tmp_path / "publication.json",
    )
    report = mod._rollback_publication_report_base(
        arguments,
        publication,
        silver_run_id="rollback_silver__"
        + publication["generation_id"].replace("-", ""),
    )
    report.update({"phase": "acquire_ambiguous", "recovery_required": True})
    arguments.publication_report.write_text(json.dumps(report), encoding="utf-8")
    writer_state = {
        "pause_states": {dag_id: True for dag_id in mod.DAGS},
        "active_runs": {},
    }
    transitions = []
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "_pause_all_writers", lambda *_a, **_k: writer_state)
    monkeypatch.setattr(mod, "_exact_silver_run", lambda *_a, **_k: None)
    monkeypatch.setattr(
        mod,
        "_get_rollback_publication",
        lambda *_a, **_k: _publication_state(publication),
    )

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        assert action == "fail_release"
        return {
            **_publication_state(
                publication, status="failed", phase="failed", active=False
            ),
            "released": True,
        }

    monkeypatch.setattr(mod, "_transition_rollback_publication", transition)
    recovered = mod.recover_rollback_publication(arguments)

    assert recovered["passed"] is False
    assert recovered["phase"] == "pretrigger_generation_released"
    assert recovered["recovery_required"] is False
    assert "--publication-attempt 2" in recovered["error"]
    assert transitions == ["fail_release"]


def test_recovery_proves_pre_acquire_kill_created_no_generation(tmp_path, monkeypatch):
    publication = _rollback_publication()
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RECOVER_PUBLICATION,
        publication_report=tmp_path / "publication.json",
    )
    report = mod._rollback_publication_report_base(
        arguments,
        publication,
        silver_run_id="rollback_silver__"
        + publication["generation_id"].replace("-", ""),
    )
    report.update({"phase": "prepared_pending_acquire", "recovery_required": True})
    arguments.publication_report.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(
        mod,
        "_pause_all_writers",
        lambda *_a, **_k: {
            "pause_states": {dag_id: True for dag_id in mod.DAGS},
            "active_runs": {},
        },
    )
    monkeypatch.setattr(mod, "_exact_silver_run", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "_get_rollback_publication", lambda *_a, **_k: None)
    monkeypatch.setattr(
        mod,
        "_transition_rollback_publication",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("absent generation must not be transitioned")
        ),
    )

    recovered = mod.recover_rollback_publication(arguments)

    assert recovered["passed"] is False
    assert recovered["phase"] == "no_generation_acquired"
    assert recovered["publication_state"] is None
    assert recovered["recovery_required"] is False
    assert "same publication attempt" in recovered["error"]


def test_recovery_seals_and_abandons_exact_successful_silver_candidate(
    tmp_path, monkeypatch
):
    publication = _rollback_publication()
    publication_report = tmp_path / "publication.json"
    arguments = args(
        tmp_path,
        execute=True,
        confirm=mod.CONFIRM_RECOVER_PUBLICATION,
        publication_report=publication_report,
        output=tmp_path / "recovery.json",
    )
    silver_run_id = "rollback_silver__" + publication["generation_id"].replace("-", "")
    report = mod._rollback_publication_report_base(
        arguments, publication, silver_run_id=silver_run_id
    )
    report.update(
        {"phase": "lock_retained_pending_terminal_proof", "recovery_required": True}
    )
    publication_report.write_text(json.dumps(report), encoding="utf-8")
    transitions = []
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    monkeypatch.setattr(mod, "validate_live_deployment", lambda *_a, **_k: {})
    monkeypatch.setattr(
        mod,
        "_pause_all_writers",
        lambda *_a, **_k: {
            "pause_states": {dag_id: True for dag_id in mod.DAGS},
            "active_runs": {},
        },
    )
    monkeypatch.setattr(
        mod,
        "_exact_silver_run",
        lambda *_a, **_k: {
            "run_id": silver_run_id,
            "state": "success",
            "start_date": "2026-07-21T10:01:00Z",
        },
    )
    monkeypatch.setattr(
        mod,
        "_get_rollback_publication",
        lambda *_a, **_k: _publication_state(publication),
    )

    def transition(_args, _generation_id, *, action, **_kwargs):
        transitions.append(action)
        if action == "seal":
            return _publication_state(
                publication, status="succeeded", phase="ready", active=True
            )
        assert action == "abandon"
        return {
            **_publication_state(
                publication, status="succeeded", phase="abandoned", active=False
            ),
            "released": True,
            "published": False,
        }

    monkeypatch.setattr(mod, "_transition_rollback_publication", transition)
    recovered = mod.recover_rollback_publication(arguments)

    assert recovered["passed"] is True
    assert recovered["phase"] == "abandoned"
    assert recovered["candidate"]["digest"] == "d" * 64
    assert transitions == ["seal", "abandon"]


def test_validate_publication_evidence_requires_exact_abandoned_db_candidate(
    tmp_path, monkeypatch
):
    publication = _rollback_publication()
    publication_report = tmp_path / "publication.json"
    silver_run_id = "rollback_silver__" + publication["generation_id"].replace("-", "")
    arguments = args(
        tmp_path,
        publication_report=publication_report,
        silver_run_id=silver_run_id,
    )
    report = mod._rollback_publication_report_base(
        arguments, publication, silver_run_id=silver_run_id
    )
    report.update(
        {
            "passed": True,
            "phase": "abandoned",
            "recovery_required": False,
            "candidate": {
                "generation_id": publication["generation_id"],
                "digest": "d" * 64,
                "transform_task_ids": ["silver_transforms.example"],
            },
        }
    )
    publication_report.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(
        mod, "_rollback_publication_envelope", lambda *_a, **_k: publication
    )
    state = _publication_state(
        publication, status="succeeded", phase="abandoned", active=False
    )
    monkeypatch.setattr(mod, "_get_rollback_publication", lambda *_a, **_k: state)

    evidence = mod.validate_rollback_publication_evidence(
        arguments, expected_sha="a" * 40, run=lambda *_a, **_k: None
    )
    assert evidence["phase"] == "abandoned"
    assert evidence["candidate_digest"] == "d" * 64

    state["candidate"]["digest"] = "e" * 64
    with pytest.raises(mod.RollbackError, match="abandoned exact candidate"):
        mod.validate_rollback_publication_evidence(
            arguments, expected_sha="a" * 40, run=lambda *_a, **_k: None
        )


def test_main_preserves_write_ahead_identity_when_run_silver_crashes(
    tmp_path, monkeypatch
):
    arguments = args(tmp_path)
    publication = _rollback_publication()

    def crash(parsed):
        report = mod._rollback_publication_report_base(
            parsed,
            publication,
            silver_run_id="rollback_silver__"
            + publication["generation_id"].replace("-", ""),
        )
        mod._atomic_json(parsed.output, report)
        raise RuntimeError("simulated process failure")

    monkeypatch.setattr(mod, "run_rollback_validation_silver", crash)
    result = mod.main(
        [
            "run-silver",
            "--env-file",
            str(arguments.env_file),
            "--compose-file",
            str(arguments.compose_file),
            "--deployment-report",
            str(arguments.deployment_report),
            "--output",
            str(arguments.output),
        ]
    )

    persisted = json.loads(arguments.output.read_text(encoding="utf-8"))
    assert result == 1
    assert persisted["publication"] == publication
    assert persisted["phase"] == "prepared_pending_acquire"
    assert persisted["recovery_required"] is True
    assert "simulated process failure" in persisted["error"]


def test_validate_rejects_successful_silver_run_from_before_rollback_deploy(
    tmp_path, monkeypatch
):
    arguments = args(
        tmp_path,
        expected_consumer_sha="a" * 40,
        silver_run_id="manual-old",
    )

    def run(command, **_kwargs):
        if command[:2] == ("docker", "inspect"):
            raise AssertionError("unexpected image inspection")
        if "printenv FOTMOB_DEPLOY_GIT_SHA" in command:
            stdout = "a" * 40
        elif "FOTMOB_WRITER_STATE_JSON=" in command[-1]:
            stdout = "FOTMOB_WRITER_STATE_JSON=" + json.dumps(
                [
                    {
                        "dag_id": dag_id,
                        "is_paused": True,
                        "run_id": None,
                        "state": None,
                    }
                    for dag_id in mod.DAGS
                ]
            )
        elif "config" in command and "get-value" in command:
            stdout = "/opt/airflow/dags\n"
        elif "dags" in command and "list" in command and "--output" in command:
            stdout = json.dumps(
                [{"dag_id": dag_id, "is_paused": True} for dag_id in mod.DAGS]
            )
        elif "list-runs" in command and "--state" in command:
            stdout = "[]"
        elif "list-runs" in command:
            stdout = json.dumps(
                [
                    {
                        "run_id": "manual-old",
                        "state": "success",
                        "start_date": "2026-07-21T09:59:59Z",
                    }
                ]
            )
        else:
            raise AssertionError(f"unexpected command: {command}")
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(
        mod,
        "validate_live_deployment",
        lambda *_args, **_kwargs: {"mounts_verified": True},
    )
    with pytest.raises(mod.RollbackError, match="predates"):
        mod.validate_rollback(
            arguments,
            run=run,
            client_factory=lambda **_: (_ for _ in ()).throw(
                AssertionError("Trino must not be queried")
            ),
        )


def test_live_deployment_identity_binds_container_images_nonce_and_mounts(tmp_path):
    arguments = args(tmp_path)
    context = json.loads(arguments.deployment_report.read_text())
    release = tmp_path.resolve()
    dagbag = tmp_path / "dagbag"
    dagbag.mkdir()
    for name, relative in mod.runtime_binding.PROJECTION_SOURCES.items():
        source = release / relative
        source.parent.mkdir(parents=True, exist_ok=True)
        if not source.exists():
            source.write_text(relative)
        (dagbag / name).write_bytes(source.read_bytes())
    for name in mod.runtime_binding.PROJECTION_DIRECTORIES:
        (dagbag / name).mkdir()
    mount_specs = {
        "/opt/airflow/dags": (tmp_path / "dagbag", False),
        "/opt/airflow/dags/utils": (release / "dags/utils", False),
        "/opt/airflow/dags/sql": (release / "dags/sql", False),
        "/opt/airflow/dags/scripts": (release / "dags/scripts", False),
        "/opt/airflow/scrapers": (release / "scrapers", False),
        "/opt/airflow/scripts": (release / "scripts", False),
        "/opt/airflow/configs/medallion": (release / "configs/medallion", False),
        "/opt/airflow/configs/fotmob": (release / "configs/fotmob", False),
        "/opt/airflow/logs/fotmob": (tmp_path / "evidence", True),
    }
    scheduler = {
        "Id": context["scheduler_container_id"],
        "Image": context["resolved_image_id"],
        "State": {"Running": True},
        "Config": {
            "Env": [
                f"FOTMOB_DEPLOYMENT_ID={context['deployment_id']}",
                ("FOTMOB_DEPLOYMENT_REPORT_PATH=" + context["container_report_path"]),
                f"FOTMOB_DEPLOY_GIT_SHA={context['git_sha']}",
                "FOTMOB_ISOLATED_STACK=1",
                "TELEGRAM_BOT_TOKEN=test-token",
                "TELEGRAM_CHAT_ID=test-chat",
                "TRINO_HOST=trino",
                "TRINO_PORT=8443",
                "TRINO_USER=airflow",
                "TRINO_PASSWORD=secret",
                "TRINO_HTTP_SCHEME=https",
                "TRINO_TLS_VERIFY=false",
                "FBREF_CONTROL_DB_URI=postgresql://control@postgres/control",
            ]
        },
        "Mounts": [
            {
                "Type": "bind",
                "Source": str(source.resolve()),
                "Destination": destination,
                "RW": writable,
            }
            for destination, (source, writable) in mount_specs.items()
        ],
    }
    metadb = {
        "Id": context["metadb_container_id"],
        "Image": context["resolved_postgres_image_id"],
        "State": {"Running": True},
        "Mounts": [
            {
                "Type": "volume",
                "Source": "volume-name",
                "Destination": "/var/lib/postgresql/data",
                "RW": True,
            }
        ],
    }

    def run(command, **_kwargs):
        if command[:2] == ("git", "-C"):
            if "rev-parse" in command:
                stdout = context["git_sha"] + "\n"
            else:
                stdout = ""
            return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")
        if "ps" in command and "-q" in command:
            service = command[-1]
            stdout = (
                context["scheduler_container_id"]
                if service == "airflow-scheduler"
                else context["metadb_container_id"]
            ) + "\n"
            return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")
        payload = (
            scheduler if command[-1] == context["scheduler_container_id"] else metadb
        )
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(payload), stderr=""
        )

    evidence = mod.validate_live_deployment(arguments, require_running=True, run=run)
    assert evidence["mounts_verified"] is True
    scheduler["Mounts"][0]["Source"] = str((tmp_path / "stale-dagbag").resolve())
    with pytest.raises(mod.RollbackError, match="mount source differs"):
        mod.validate_live_deployment(arguments, require_running=True, run=run)
