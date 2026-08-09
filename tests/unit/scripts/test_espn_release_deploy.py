"""Fail-closed contracts for the versioned ESPN release deploy operator."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "deploy/espn/deploy.py"
SPEC = importlib.util.spec_from_file_location("espn_release_deploy", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
deploy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = deploy
SPEC.loader.exec_module(deploy)


def _release_tree(root: Path) -> None:
    for relative in (
        "dags/dag_ingest_espn.py",
        "scrapers/espn/runner.py",
        "scripts/verify_espn_database_topology.py",
        "configs/espn/registry.yaml",
        "configs/medallion/espn_season_mapping.yaml",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative + "\n", encoding="utf-8")


def _spec(tmp_path: Path) -> dict[str, object]:
    release_root = tmp_path / "release"
    dagbag_root = tmp_path / "dagbag"
    _release_tree(release_root)
    dagbag_root.mkdir()
    (dagbag_root / ".airflowignore").write_text("ignored\n", encoding="utf-8")
    guard = release_root / "scripts/espn_release_guard_v1.py"
    guard.write_text(
        """import json
import os

report = {
    "kind": "espn-release-guard-v1",
    "schema_version": 1,
    "status": "ok",
    "phase": os.environ["ESPN_DEPLOY_GUARD_PHASE"],
    "attempt": int(os.environ["ESPN_DEPLOY_GUARD_ATTEMPT"]),
    "transition_id": os.environ["ESPN_DEPLOY_TRANSITION_ID"],
    "plan_sha256": os.environ["ESPN_DEPLOY_PLAN_SHA256"],
    "checks": {
        "exact_dag_inventory": True,
        "all_dags_paused": True,
        "zero_active_dagruns": True,
        "transaction_read_only": True,
    },
}
print(json.dumps(report, sort_keys=True, separators=(",", ":")))
""",
        encoding="utf-8",
    )
    compose = release_root / "deploy/espn/airflow.compose.yaml"
    compose.parent.mkdir(parents=True)
    compose.write_text("services: {}\n", encoding="utf-8")
    env_file = tmp_path / "espn.env"
    env_file.write_text("SECRET=not-read-by-plan\n", encoding="utf-8")
    os.chmod(env_file, 0o600)
    return {
        "transition_id": "issue-1148-release-001",
        "release_commit": "a" * 40,
        "release_tree_sha256": deploy.release_tree_sha256(release_root),
        "release_root": str(release_root),
        "dagbag_root": str(dagbag_root),
        "compose_file": str(compose),
        "env_file": str(env_file),
        "stack_lock_root": str(tmp_path / "stack-lock"),
        "state_root": str(tmp_path / "durable-state"),
        "backup_root": str(tmp_path / "durable-backups"),
        "airflow_image": "registry.example/espn/airflow@sha256:" + "b" * 64,
        "postgres_image": "registry.example/espn/postgres@sha256:" + "c" * 64,
        "layout_mode": "legacy14",
        "guard_argv": [
            sys.executable,
            "-B",
            str(guard),
            "guard",
            "--docker-path",
            "/usr/bin/docker",
            "--poll-seconds",
            "15",
            "--max-wait-seconds",
            "1740",
        ],
        "guard_artifacts": [str(guard), "/usr/bin/docker"],
    }


def _guard_report(
    plan: dict[str, object], *, phase: str, attempt: int
) -> dict[str, object]:
    return {
        "kind": "espn-release-guard-v1",
        "schema_version": 1,
        "status": "ok",
        "phase": phase,
        "attempt": attempt,
        "transition_id": plan["transition_id"],
        "plan_sha256": plan["plan_sha256"],
        "checks": {
            "exact_dag_inventory": True,
            "all_dags_paused": True,
            "zero_active_dagruns": True,
            "transaction_read_only": True,
        },
    }


def _fake_restore_proof(
    context, plan: dict[str, object], *, label: str
) -> dict[str, object]:
    logs: dict[str, dict[str, str]] = {}
    for name in ("start_log", "ready_log", "restore_log", "verify_log", "cleanup_log"):
        path = context.log_root / f"fake-{label}-{name}.log"
        payload = f"{label}:{name}\n".encode()
        path.write_bytes(payload)
        path.chmod(0o400)
        logs[name] = {
            "path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    return {
        "passed": True,
        "container": plan["restore_drill"]["container"],
        "image": plan["postgres_image"],
        "network": "none",
        **logs,
    }


def test_plan_is_canonical_repeatable_and_strictly_non_mutating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    before = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))

    def forbidden(*_args, **_kwargs):
        raise AssertionError("plan reached a mutating/runtime boundary")

    monkeypatch.setattr(deploy, "_acquire_lock", forbidden)
    monkeypatch.setattr(deploy, "_run_process", forbidden)
    monkeypatch.setattr(deploy, "_write_checksummed_json", forbidden)
    first = deploy.build_plan(spec)
    second = deploy.build_plan(spec)

    assert first == second
    assert first["kind"] == "espn-release-deploy-plan-v1"
    assert first["mutates"] is False
    assert first["limits"] == {
        "guard_timeout_seconds": 1800,
        "heartbeat_interval_seconds": 60,
        "total_timeout_seconds": 10800,
    }
    unsigned = {key: value for key, value in first.items() if key != "plan_sha256"}
    assert (
        first["plan_sha256"]
        == hashlib.sha256(deploy.canonical_bytes(unsigned)).hexdigest()
    )
    assert (
        sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))
        == before
    )


def test_plan_cli_seals_the_current_operator_and_guard_executable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spec = _spec(tmp_path)
    argv = [
        "plan",
        "--transition-id",
        str(spec["transition_id"]),
        "--release-commit",
        str(spec["release_commit"]),
        "--release-tree-sha256",
        str(spec["release_tree_sha256"]),
        "--release-root",
        str(spec["release_root"]),
        "--dagbag-root",
        str(spec["dagbag_root"]),
        "--compose-file",
        str(spec["compose_file"]),
        "--env-file",
        str(spec["env_file"]),
        "--stack-lock-root",
        str(spec["stack_lock_root"]),
        "--state-root",
        str(spec["state_root"]),
        "--backup-root",
        str(spec["backup_root"]),
        "--airflow-image",
        str(spec["airflow_image"]),
        "--postgres-image",
        str(spec["postgres_image"]),
        "--layout-mode",
        str(spec["layout_mode"]),
        "--guard-argv-json",
        json.dumps(spec["guard_argv"]),
    ]
    for artifact in spec["guard_artifacts"]:
        argv.extend(("--guard-artifact", str(artifact)))

    assert deploy.main(argv) == 0
    plan = json.loads(capsys.readouterr().out)

    assert plan["operator_path"] == str(SCRIPT.resolve())
    assert plan["operator_sha256"] == hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    assert deploy.GUARD_PYTHON_PATH == sys.executable
    assert plan["guard"]["argv"][0] == sys.executable
    assert plan["guard"]["executable"]["path"] == sys.executable
    assert plan["guard"]["artifacts"][0]["path"] == spec["guard_artifacts"][0]


def test_guard_interpreter_binding_follows_the_operator_interpreter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    interpreter = tmp_path / "ci-python"
    interpreter.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    interpreter.chmod(0o755)
    module_name = "espn_release_deploy_ci_interpreter"
    alternate_spec = importlib.util.spec_from_file_location(module_name, SCRIPT)
    assert alternate_spec is not None and alternate_spec.loader is not None

    with monkeypatch.context() as scoped_patch:
        scoped_patch.setattr(sys, "executable", str(interpreter))
        alternate_deploy = importlib.util.module_from_spec(alternate_spec)
        sys.modules[module_name] = alternate_deploy
        try:
            alternate_spec.loader.exec_module(alternate_deploy)
        finally:
            sys.modules.pop(module_name, None)

    spec = _spec(tmp_path)
    spec["guard_argv"] = [str(interpreter), *spec["guard_argv"][1:]]

    assert alternate_deploy.GUARD_PYTHON_PATH == str(interpreter)
    plan = alternate_deploy.build_plan(spec)
    assert plan["guard"]["argv"][0] == str(interpreter)
    assert plan["guard"]["executable"]["path"] == str(interpreter)


def test_release_plan_seals_the_exact_versioned_guard_and_docker_bytes(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    guard_path = (ROOT / "scripts/espn_release_guard_v1.py").resolve()
    docker_path = Path("/usr/bin/docker")
    spec["release_root"] = str(ROOT)
    spec["release_tree_sha256"] = deploy.release_tree_sha256(ROOT)
    guard_argv = [
        sys.executable,
        "-B",
        str(guard_path),
        "guard",
        "--docker-path",
        str(docker_path),
        "--poll-seconds",
        "15",
        "--max-wait-seconds",
        "1740",
    ]
    spec["guard_argv"] = guard_argv
    spec["guard_artifacts"] = [str(guard_path), str(docker_path)]

    plan = deploy.build_plan(spec)

    assert plan["guard"]["argv"] == guard_argv
    assert plan["guard"]["artifacts"] == [
        {
            "path": str(guard_path),
            "sha256": hashlib.sha256(guard_path.read_bytes()).hexdigest(),
        },
        {
            "path": str(docker_path),
            "sha256": hashlib.sha256(docker_path.read_bytes()).hexdigest(),
        },
    ]
    assert plan["guard"]["executable"]["path"] == sys.executable


@pytest.mark.parametrize(
    ("index", "replacement"),
    [
        (0, "/alternate/python"),
        (1, "-I"),
        (2, "/absolute/other/guard.py"),
        (3, "observe"),
        (5, "/usr/local/bin/docker"),
        (7, "30"),
        (9, "1710"),
    ],
)
def test_plan_rejects_any_nonversioned_guard_argv(
    tmp_path: Path, index: int, replacement: str
) -> None:
    spec = _spec(tmp_path)
    argv = list(spec["guard_argv"])
    argv[index] = replacement
    spec["guard_argv"] = argv

    with pytest.raises(deploy.DeployError, match="versioned release guard argv"):
        deploy.build_plan(spec)


def test_plan_requires_guard_script_and_docker_in_hashed_artifacts(
    tmp_path: Path,
) -> None:
    spec = _spec(tmp_path)
    spec["guard_artifacts"] = [str(spec["guard_artifacts"][0])]

    with pytest.raises(deploy.DeployError, match="required artifact set"):
        deploy.build_plan(spec)


def test_guard_success_report_is_canonical_exact_and_plan_bound(tmp_path: Path) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    log_path = tmp_path / "guard.log"
    payload = deploy.canonical_bytes(
        _guard_report(plan, phase="initial_state", attempt=1)
    )
    deploy._exclusive_regular_write(log_path, payload)
    digest = hashlib.sha256(payload).hexdigest()

    deploy._validate_guard_success_report(
        plan,
        phase="initial_state",
        attempt=1,
        log_path=log_path,
        log_sha256=digest,
    )

    invalid_reports = [
        b"",
        json.dumps(
            _guard_report(plan, phase="initial_state", attempt=1), indent=2
        ).encode(),
    ]
    for mutation in (
        {"status": "failed"},
        {"phase": "pre_backup"},
        {"attempt": 2},
        {"transition_id": "other-transition"},
        {"plan_sha256": "f" * 64},
        {
            "checks": {
                **_guard_report(plan, phase="initial_state", attempt=1)["checks"],
                "all_dags_paused": False,
            }
        },
        {"extra": True},
    ):
        report = _guard_report(plan, phase="initial_state", attempt=1)
        report.update(mutation)
        invalid_reports.append(deploy.canonical_bytes(report))

    for index, invalid in enumerate(invalid_reports):
        invalid_path = tmp_path / f"invalid-guard-{index}.log"
        deploy._exclusive_regular_write(invalid_path, invalid)
        with pytest.raises(deploy.DeployError, match="guard success report"):
            deploy._validate_guard_success_report(
                plan,
                phase="initial_state",
                attempt=1,
                log_path=invalid_path,
                log_sha256=hashlib.sha256(invalid).hexdigest(),
            )


def test_exit_zero_without_canonical_guard_report_is_journaled_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    monkeypatch.setattr(
        deploy,
        "_capture_physical_fingerprint",
        lambda *_args, **_kwargs: {
            "fingerprint_sha256": "d" * 64,
            "evidence_path": "/durable/mock-fingerprint.json",
        },
    )

    def fake_noop(_context, _argv, *, log_path, **_kwargs):
        deploy._exclusive_regular_write(log_path, b"")
        return deploy.ProcessResult(0, 0.1, hashlib.sha256(b"").hexdigest())

    monkeypatch.setattr(deploy, "_run_process", fake_noop)

    with pytest.raises(deploy.DeployError, match="guard success report"):
        deploy._run_guard_phase(context, "initial_state")

    assert [event["status"] for event in context.guards["events"]] == [
        "started",
        "failed",
    ]


def test_apply_and_resume_load_only_the_exact_canonical_plan(tmp_path: Path) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    path = tmp_path / "reviewed-plan.json"
    path.write_bytes(deploy.canonical_bytes(plan))

    assert deploy.load_reviewed_plan(path, plan["plan_sha256"]) == plan
    with pytest.raises(deploy.DeployError, match="requested plan SHA-256"):
        deploy.load_reviewed_plan(path, "f" * 64)

    noncanonical = tmp_path / "noncanonical.json"
    noncanonical.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    with pytest.raises(deploy.DeployError, match="canonical"):
        deploy.load_reviewed_plan(noncanonical, plan["plan_sha256"])

    drifted = dict(plan)
    drifted["layout_mode"] = "compact6"
    path.write_bytes(deploy.canonical_bytes(drifted))
    with pytest.raises(deploy.DeployError, match="plan SHA-256"):
        deploy.load_reviewed_plan(path, plan["plan_sha256"])


def test_stack_lock_is_separate_from_transition_state_and_exclusive(
    tmp_path: Path,
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    lock_root = Path(str(plan["stack_lock_root"]))
    state_root = Path(str(plan["state_root"]))

    assert lock_root != state_root
    assert lock_root not in state_root.parents
    assert state_root not in lock_root.parents
    deploy._ensure_private_directory(lock_root)
    with deploy._acquire_lock(lock_root):
        with pytest.raises(deploy.DeployError, match="another ESPN deploy"):
            with deploy._acquire_lock(lock_root):
                pass


def test_apply_preflight_rejects_rehashed_runtime_command_drift(tmp_path: Path) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    drifted = json.loads(json.dumps(plan))
    drifted["commands"]["backup"] = [sys.executable, "-c", "raise SystemExit(0)"]
    unsigned = {key: value for key, value in drifted.items() if key != "plan_sha256"}
    drifted["plan_sha256"] = deploy.canonical_sha256(unsigned)

    deploy._validate_plan_document(drifted)
    with pytest.raises(deploy.DeployError, match="runtime contract drifted"):
        deploy._validate_static_plan(drifted)
    with pytest.raises(deploy.DeployError, match="runtime contract drifted"):
        deploy.execute_plan(drifted, mode="apply")
    assert not Path(str(drifted["state_root"])).exists()


def test_static_preflight_consumes_total_monotonic_budget_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    monotonic = {"now": 100.0}
    monkeypatch.setattr(deploy.time, "monotonic", lambda: monotonic["now"])

    def slow_static_validation(_plan) -> None:
        monotonic["now"] += deploy.TOTAL_TIMEOUT_SECONDS + 0.001

    monkeypatch.setattr(deploy, "_validate_static_plan", slow_static_validation)

    with pytest.raises(deploy.DeployError, match="expired in preflight"):
        deploy.execute_plan(plan, mode="apply", clock=lambda: 1_786_182_400.0)

    assert not Path(str(plan["stack_lock_root"])).exists()
    assert not Path(str(plan["state_root"])).exists()
    assert not Path(str(plan["backup_root"])).exists()


@pytest.mark.parametrize(
    ("target", "message"),
    (
        ("release", "release filesystem tree drifted"),
        ("dagbag", "DagBag projection drifted"),
        ("compose", "Compose bytes drifted"),
    ),
)
def test_compose_action_rehashes_runtime_inputs_immediately_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    message: str,
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    deploy._validate_static_plan(plan)
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    targets = {
        "release": Path(str(plan["release_root"])) / "dags/dag_ingest_espn.py",
        "dagbag": Path(str(plan["dagbag_root"])) / ".airflowignore",
        "compose": Path(str(plan["compose_file"])),
    }
    targets[target].write_text(
        "runtime drift after initial preflight\n", encoding="utf-8"
    )

    def forbidden_spawn(*_args, **_kwargs):
        raise AssertionError("drifted runtime bytes reached a Compose action")

    monkeypatch.setattr(deploy, "_run_named_action", forbidden_spawn)

    with pytest.raises(deploy.DeployError, match=message):
        deploy._run_verified_compose_action(
            context,
            "compose_config",
            plan["commands"]["compose_config"],
            env={},
        )


def test_guard_journal_is_separate_cross_bound_and_started_only_reruns() -> None:
    plan = {
        "transition_id": "release-001",
        "plan_sha256": "a" * 64,
    }
    transition = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    guards = deploy.new_guard_journal(plan)

    guards, started_one = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="started",
        attempt=1,
        duration_seconds=0.0,
        physical_fingerprint="b" * 64,
        physical_fingerprint_path="/durable/fingerprint-b.json",
        recorded_at="2026-08-08T10:00:01Z",
        log_path="/durable/guards/initial-state-attempt-001.log",
        log_sha256=None,
    )
    transition = deploy.append_transition_event(
        transition,
        event="guard_started",
        phase="initial_state",
        recorded_at="2026-08-08T10:00:01Z",
        guard_event_sha256=started_one["event_sha256"],
        detail={"attempt": 1},
    )

    assert deploy.next_guard_attempt(guards, "initial_state") == 2
    assert deploy.guard_phase_succeeded(guards, "initial_state") is False

    guards, started_two = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="started",
        attempt=2,
        duration_seconds=0.0,
        physical_fingerprint="c" * 64,
        physical_fingerprint_path="/durable/fingerprint-c.json",
        recorded_at="2026-08-08T10:01:00Z",
        log_path="/durable/guards/initial-state-attempt-002.log",
        log_sha256=None,
    )
    transition = deploy.append_transition_event(
        transition,
        event="guard_started",
        phase="initial_state",
        recorded_at="2026-08-08T10:01:00Z",
        guard_event_sha256=started_two["event_sha256"],
        detail={"attempt": 2},
    )
    guards, succeeded = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="succeeded",
        attempt=2,
        duration_seconds=12.5,
        physical_fingerprint="d" * 64,
        physical_fingerprint_path="/durable/fingerprint-d.json",
        recorded_at="2026-08-08T10:01:13Z",
        log_path="/durable/guards/initial-state-attempt-002.log",
        log_sha256="e" * 64,
    )
    transition = deploy.append_transition_event(
        transition,
        event="guard_succeeded",
        phase="initial_state",
        recorded_at="2026-08-08T10:01:13Z",
        guard_event_sha256=succeeded["event_sha256"],
        detail={"attempt": 2},
    )
    transition["guard_attempt_journal_sha256"] = guards["journal_sha256"]
    transition = deploy.reseal_transition_journal(transition)

    deploy.validate_cross_bound_journals(transition, guards)
    assert deploy.guard_phase_succeeded(guards, "initial_state") is True
    assert deploy.next_guard_attempt(guards, "initial_state") is None
    assert guards["events"][0] == started_one


def test_completed_guard_identity_is_immutable() -> None:
    plan = {"transition_id": "release-001", "plan_sha256": "a" * 64}
    transition = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    guards = deploy.new_guard_journal(plan)
    guards, started = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="started",
        attempt=1,
        duration_seconds=0,
        physical_fingerprint="b" * 64,
        physical_fingerprint_path="/durable/fingerprint-b.json",
        recorded_at="2026-08-08T10:00:01Z",
        log_path="/durable/pre-backup.log",
        log_sha256=None,
    )
    transition = deploy.append_transition_event(
        transition,
        event="guard_started",
        phase="initial_state",
        recorded_at="2026-08-08T10:00:01Z",
        guard_event_sha256=started["event_sha256"],
    )
    guards, _completed = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="failed",
        attempt=1,
        duration_seconds=3,
        physical_fingerprint="c" * 64,
        physical_fingerprint_path="/durable/fingerprint-c.json",
        recorded_at="2026-08-08T10:00:04Z",
        log_path="/durable/pre-backup.log",
        log_sha256="d" * 64,
    )
    with pytest.raises(deploy.DeployError, match="already completed"):
        deploy.append_guard_event(
            guards,
            transition_event_sha256=deploy.transition_tail_sha256(transition),
            phase="initial_state",
            status="succeeded",
            attempt=1,
            duration_seconds=4,
            physical_fingerprint="e" * 64,
            physical_fingerprint_path="/durable/fingerprint-e.json",
            recorded_at="2026-08-08T10:00:05Z",
            log_path="/durable/pre-backup.log",
            log_sha256="f" * 64,
        )


def test_resume_repairs_only_the_guard_first_cross_file_crash_window() -> None:
    plan = {"transition_id": "release-001", "plan_sha256": "a" * 64}
    transition = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    guards = deploy.new_guard_journal(plan)
    transition["guard_attempt_journal_sha256"] = guards["journal_sha256"]
    transition = deploy.reseal_transition_journal(transition)
    guards, started = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="started",
        attempt=1,
        duration_seconds=0,
        physical_fingerprint="b" * 64,
        physical_fingerprint_path="/durable/fingerprint-b.json",
        recorded_at="2026-08-08T10:00:01Z",
        log_path="/durable/initial-state.log",
        log_sha256=None,
    )

    repaired, same_guards, changed = deploy._reconcile_torn_cross_binding(
        transition, guards
    )

    assert changed is True
    assert same_guards == guards
    assert repaired["events"][-1]["guard_event_sha256"] == started["event_sha256"]
    deploy.validate_cross_bound_journals(repaired, guards)
    assert deploy.next_guard_attempt(guards, "initial_state") == 2


def test_cross_binding_rejects_a_nonadjacent_transition_reference() -> None:
    plan = {"transition_id": "release-001", "plan_sha256": "a" * 64}
    transition = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    guards = deploy.new_guard_journal(plan)
    transition["guard_attempt_journal_sha256"] = guards["journal_sha256"]
    transition = deploy.reseal_transition_journal(transition)
    guards, started = deploy.append_guard_event(
        guards,
        transition_event_sha256=deploy.transition_tail_sha256(transition),
        phase="initial_state",
        status="started",
        attempt=1,
        duration_seconds=0,
        physical_fingerprint="b" * 64,
        physical_fingerprint_path="/durable/fingerprint-b.json",
        recorded_at="2026-08-08T10:00:01Z",
        log_path="/durable/initial-state.log",
        log_sha256=None,
    )
    transition = deploy.append_transition_event(
        transition,
        event="action_started",
        phase=None,
        recorded_at="2026-08-08T10:00:01Z",
        detail={"action": "unrelated", "attempt": 1},
    )
    transition = deploy.append_transition_event(
        transition,
        event="guard_started",
        phase="initial_state",
        recorded_at="2026-08-08T10:00:02Z",
        guard_event_sha256=started["event_sha256"],
        detail={"attempt": 1},
    )
    transition["guard_attempt_journal_sha256"] = guards["journal_sha256"]
    transition = deploy.reseal_transition_journal(transition)

    with pytest.raises(deploy.DeployError, match="not adjacent"):
        deploy.validate_cross_bound_journals(transition, guards)


def test_deployed_transition_is_terminal_unique_and_bound_to_exact_result() -> None:
    plan = {"transition_id": "release-001", "plan_sha256": "a" * 64}
    result_sha = "b" * 64
    transition = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    transition = deploy.append_transition_event(
        transition,
        event="deployed",
        phase=None,
        recorded_at="2026-08-08T10:00:01Z",
        detail={"result_sha256": result_sha},
    )

    deploy._validate_deployed_transition(transition, result_sha)
    with pytest.raises(deploy.DeployError, match="result binding drifted"):
        deploy._validate_deployed_transition(transition, "c" * 64)

    advanced = deploy.append_transition_event(
        transition,
        event="action_started",
        phase=None,
        recorded_at="2026-08-08T10:00:02Z",
        detail={"action": "forged-tail", "attempt": 1},
    )
    with pytest.raises(deploy.DeployError, match="unique and terminal"):
        deploy._validate_deployed_transition(advanced, result_sha)


def test_resume_reuses_completed_backup_restore_evidence_before_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )

    def fake_process(_context, _argv, *, log_path, stdout_path=None, **_kwargs):
        log_payload = b"fake backup process log\n"
        log_path.write_bytes(log_payload)
        log_path.chmod(0o400)
        if stdout_path is not None:
            payload = (
                b"PGDMP-complete-archive"
                if ".dump.toc." not in stdout_path.name
                else b"; complete archive TOC\n"
            )
            stdout_path.write_bytes(payload)
        return deploy.ProcessResult(
            0, 0.1, hashlib.sha256(log_payload).hexdigest(), "b" * 64
        )

    monkeypatch.setattr(deploy, "_run_process", fake_process)
    monkeypatch.setattr(
        deploy,
        "_restore_drill",
        lambda restore_context, *_args, **_kwargs: _fake_restore_proof(
            restore_context, plan, label="completed-action"
        ),
    )

    first = deploy._backup_and_restore(context)
    resumed_context = deploy._load_or_initialize_context(
        plan, "resume", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    resumed = deploy._backup_and_restore(resumed_context)

    assert resumed == first
    assert Path(str(resumed["dump_path"])).read_bytes().startswith(b"PGDMP")
    assert resumed["restore_proof"]["passed"] is True


def test_resume_revalidates_started_only_published_backup_before_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    compose_log = context.log_root / "action-compose_config-attempt-001.log"
    compose_payload = b"compose config valid\n"
    compose_log.write_bytes(compose_payload)
    compose_log.chmod(0o400)
    context.transition_event(
        "action_started",
        detail={
            "action": "compose_config",
            "attempt": 1,
            "log_path": str(compose_log),
        },
    )
    context.transition_event(
        "action_succeeded",
        detail={
            "action": "compose_config",
            "attempt": 1,
            "duration_seconds": 0.1,
            "log_path": str(compose_log),
            "log_sha256": hashlib.sha256(compose_payload).hexdigest(),
        },
    )
    context.transition_event(
        "action_started",
        detail={"action": "backup_restore_proof", "attempt": 1},
    )
    context.transition_event(
        "action_started",
        detail={
            "action": "backup_restore_proof",
            "attempt": 2,
            "recovery": True,
            "source_attempt": 1,
        },
    )
    backup_path = Path(str(plan["backup_path"]))
    toc_path = Path(str(plan["backup_toc_path"]))
    for path, payload in (
        (backup_path, b"PGDMP-complete-archive"),
        (toc_path, b"; complete archive TOC\n"),
        (context.log_root / "backup-attempt-001.log", b"dump complete\n"),
        (context.log_root / "backup-toc-attempt-001.log", b"toc complete\n"),
    ):
        path.write_bytes(payload)
        path.chmod(0o400)

    def fake_process(_context, _argv, *, log_path, stdout_path=None, **_kwargs):
        log_payload = b"fake recovery validation log\n"
        log_path.write_bytes(log_payload)
        log_path.chmod(0o400)
        if stdout_path is not None:
            stdout_path.write_bytes(b"; complete archive TOC\n")
        return deploy.ProcessResult(
            0, 0.1, hashlib.sha256(log_payload).hexdigest(), "b" * 64
        )

    monkeypatch.setattr(deploy, "_run_process", fake_process)
    monkeypatch.setattr(
        deploy,
        "_restore_drill",
        lambda restore_context, *_args, **_kwargs: _fake_restore_proof(
            restore_context, plan, label="published-recovery"
        ),
    )

    evidence = deploy._backup_and_restore(context)

    assert evidence["dump_path"] == str(backup_path)
    assert evidence["toc_path"] == str(toc_path)
    assert evidence["restore_proof"]["passed"] is True
    assert deploy._action_succeeded(context.transition, "backup_restore_proof")
    deploy._validate_transition_action_evidence(plan, context.transition)

    forged = json.loads(json.dumps(context.transition))
    terminal = dict(forged["events"][-1])
    terminal.pop("event_sha256")
    terminal["detail"]["recovered_started_attempt"] = 2
    terminal["event_sha256"] = deploy._event_sha(terminal)
    forged["events"][-1] = terminal
    forged = deploy.reseal_transition_journal(forged)
    with pytest.raises(deploy.DeployError, match="backup recovery identity drifted"):
        deploy._validate_transition_action_evidence(plan, forged)


def test_backup_recovers_after_crash_between_dump_and_toc_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )

    def fake_process(_context, _argv, *, log_path, stdout_path=None, **_kwargs):
        log_payload = b"fake crash-window process log\n"
        log_path.write_bytes(log_payload)
        log_path.chmod(0o400)
        if stdout_path is not None:
            payload = (
                b"; complete archive TOC\n"
                if ".dump.toc." in stdout_path.name
                else b"PGDMP-complete-archive"
            )
            stdout_path.write_bytes(payload)
            stdout_path.chmod(0o400)
        return deploy.ProcessResult(
            0, 0.1, hashlib.sha256(log_payload).hexdigest(), "b" * 64
        )

    monkeypatch.setattr(deploy, "_run_process", fake_process)
    monkeypatch.setattr(
        deploy,
        "_restore_drill",
        lambda restore_context, *_args, attempt, **_kwargs: _fake_restore_proof(
            restore_context, plan, label=f"publish-crash-{attempt}"
        ),
    )
    real_replace = deploy.os.replace
    toc_path = Path(str(plan["backup_toc_path"]))
    crash = {"armed": True}

    def crash_once_between_publish(source, target):
        if Path(target) == toc_path and crash["armed"]:
            crash["armed"] = False
            raise OSError("simulated crash between artifact publishes")
        return real_replace(source, target)

    monkeypatch.setattr(deploy.os, "replace", crash_once_between_publish)

    with pytest.raises(OSError, match="between artifact publishes"):
        deploy._backup_and_restore(context)
    assert Path(str(plan["backup_path"])).is_file()
    assert not toc_path.exists()

    evidence = deploy._backup_and_restore(context)

    assert Path(str(evidence["dump_path"])).read_bytes().startswith(b"PGDMP")
    assert Path(str(evidence["toc_path"])).read_text(encoding="utf-8").startswith(";")
    events = deploy._action_events(context.transition, "backup_restore_proof")
    assert [event["event"] for event in events] == [
        "action_started",
        "action_failed",
        "action_started",
        "action_succeeded",
    ]
    assert events[-1]["detail"]["recovered_started_attempt"] == 1


def test_resume_removes_only_an_exact_owned_stale_restore_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    commands = deploy._restore_commands(plan, Path(str(plan["backup_path"])))
    identity = {"owned": False}
    removals: list[list[str]] = []

    def fake_process(_context, argv, *, log_path, **_kwargs):
        if list(argv) == commands["inspect_owner"]:
            labels = {
                "org.dpf.espn.deploy.ownership": (
                    plan["restore_drill"]["ownership_label"]
                    if identity["owned"]
                    else "another-transition"
                )
            }
            log_path.write_text(
                "\n".join(
                    json.dumps(value)
                    for value in (labels, "none", plan["postgres_image"])
                ),
                encoding="utf-8",
            )
            return deploy.ProcessResult(0, 0.1, "a" * 64)
        removals.append(list(argv))
        return deploy.ProcessResult(0, 0.1, "b" * 64)

    monkeypatch.setattr(deploy, "_run_process", fake_process)

    with pytest.raises(deploy.DeployError, match="not owned by this plan"):
        deploy._remove_stale_owned_restore_container(context, commands, attempt=1)
    assert removals == []

    identity["owned"] = True
    deploy._remove_stale_owned_restore_container(context, commands, attempt=2)
    assert removals == [commands["remove"]]


def test_journal_and_heartbeat_writes_are_durable_owned_regular_files(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "heartbeat.json"
    path.parent.mkdir(mode=0o700)
    payload = {"kind": "espn-release-deploy-heartbeat-v1", "sequence": 1}
    deploy._write_checksummed_json(path, payload)

    assert stat.S_ISREG(path.lstat().st_mode)
    assert path.stat().st_uid == os.geteuid()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    checksum = path.with_name(path.name + ".sha256")
    assert checksum.read_text(encoding="ascii") == (
        f"{hashlib.sha256(deploy.canonical_bytes(payload)).hexdigest()}  {path.name}\n"
    )


def test_resume_repairs_only_a_valid_main_first_journal_sidecar_crash(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "transition-journal.json"
    path.parent.mkdir(mode=0o700)
    plan = {"transition_id": "release-001", "plan_sha256": "a" * 64}
    original = deploy.new_transition_journal(plan, "2026-08-08T10:00:00Z")
    deploy._write_checksummed_json(path, original)
    advanced = deploy.append_transition_event(
        original,
        event="action_started",
        phase=None,
        recorded_at="2026-08-08T10:00:01Z",
        detail={"action": "compose_config", "attempt": 1},
    )
    deploy._atomic_regular_write(path, deploy.canonical_bytes(advanced), 0o600)

    with pytest.raises(deploy.DeployError, match="sidecar drifted"):
        deploy._read_checksummed_json(path)
    recovered = deploy._read_checksummed_json(path, repair_valid_main_first_crash=True)

    assert recovered == advanced
    expected = hashlib.sha256(deploy.canonical_bytes(advanced)).hexdigest()
    assert path.with_name(path.name + ".sha256").read_text(encoding="ascii") == (
        f"{expected}  {path.name}\n"
    )


def test_resume_recovers_guard_first_initialization_crash(tmp_path: Path) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    paths = deploy._context_paths(plan)
    deploy._ensure_private_directory(paths["state_root"])
    deploy._ensure_private_directory(paths["log_root"])
    deploy._ensure_private_directory(Path(str(plan["backup_root"])))
    started_at = "2026-08-08T10:00:00Z"
    guards = deploy.new_guard_journal(plan, budget_started_at=started_at)
    deploy._write_checksummed_json(paths["guards"], guards)

    context = deploy._load_or_initialize_context(
        plan,
        "resume",
        clock=lambda: 1_786_183_201.0,
        sleeper=lambda _seconds: None,
    )

    assert context.transition["budget_started_at"] == started_at
    assert context.guards == guards
    deploy.validate_cross_bound_journals(context.transition, context.guards)
    assert paths["transition"].is_file()
    assert paths["transition"].with_name("transition-journal.json.sha256").is_file()


def test_total_budget_uses_monotonic_time_when_wall_clock_is_frozen() -> None:
    monotonic = {"now": 100.0}
    budget = deploy.Budget(
        1_786_182_400.0,
        clock=lambda: 1_786_182_400.0,
        monotonic_clock=lambda: monotonic["now"],
    )

    assert budget.remaining() == deploy.TOTAL_TIMEOUT_SECONDS
    monotonic["now"] += deploy.TOTAL_TIMEOUT_SECONDS + 0.001
    with pytest.raises(deploy.DeployError, match="three-hour total"):
        budget.require("frozen-wall proof")


def test_heartbeat_elapsed_remaining_and_eta_follow_monotonic_budget(
    tmp_path: Path,
) -> None:
    frozen_epoch = 1_786_182_400.0
    monotonic = {"now": 100.0}
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan,
        "apply",
        clock=lambda: frozen_epoch,
        sleeper=lambda _seconds: None,
    )
    context.budget = deploy.Budget(
        frozen_epoch,
        clock=lambda: frozen_epoch,
        monotonic_clock=lambda: monotonic["now"],
        invocation_started_epoch=frozen_epoch,
        invocation_started_monotonic=100.0,
    )
    monotonic["now"] = 160.0

    context.heartbeat("frozen-wall heartbeat proof", 100.0)
    heartbeat = deploy._read_checksummed_json(context.heartbeat_path)

    assert heartbeat["operation_elapsed_seconds"] == 60.0
    assert heartbeat["total_elapsed_seconds"] == 60.0
    assert heartbeat["remaining_seconds"] == 10_740.0
    assert heartbeat["eta_deadline"] == "2026-08-08T12:45:40Z"

    impossible = dict(heartbeat)
    impossible.update(
        {
            "operation_elapsed_seconds": 9_999.0,
            "total_elapsed_seconds": 1.0,
            "remaining_seconds": 1.0,
        }
    )
    with pytest.raises(deploy.DeployError, match="budget accounting drifted"):
        deploy._validate_heartbeat_document(impossible, plan)


def test_guard_process_contract_has_real_group_timeout_and_no_pipe_boundary() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "start_new_session=True" in source
    assert "os.killpg" in source
    assert "signal.SIGTERM" in source
    assert "signal.SIGKILL" in source
    assert "stdout=subprocess.PIPE" not in source
    assert "stderr=subprocess.PIPE" not in source
    assert "shell=True" not in source
    assert deploy.GUARD_TIMEOUT_SECONDS == 1800
    assert deploy.GUARD_PROCESS_TIMEOUT_SECONDS == 1740
    assert deploy.TOTAL_TIMEOUT_SECONDS == 10800
    assert 0 < deploy.HEARTBEAT_INTERVAL_SECONDS <= 60
    assert deploy.GUARD_PHASES == (
        "initial_state",
        "pre_backup",
        "pre_checkpoint_mutation",
        "pre_airflow_init",
        "pre_recreate",
        "post_deploy",
    )


def test_process_timeout_uses_real_monotonic_time_when_wall_clock_is_frozen(
    tmp_path: Path,
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    frozen_epoch = 1_786_182_400.0
    context = deploy._load_or_initialize_context(
        plan,
        "apply",
        clock=lambda: frozen_epoch,
        sleeper=deploy.time.sleep,
    )

    started = deploy.time.monotonic()
    with pytest.raises(deploy.DeployError, match="process-group timeout"):
        deploy._run_process(
            context,
            [sys.executable, "-c", "import time; time.sleep(0.5)"],
            operation="monotonic timeout proof",
            log_path=context.log_root / "monotonic-timeout.log",
            timeout_seconds=0.1,
        )
    assert deploy.time.monotonic() - started < 0.45


def test_unjournaled_fingerprint_capture_can_retry_without_log_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    observed_logs: list[Path] = []

    def fake_process(_context, argv, *, log_path, **_kwargs):
        container = argv[-1]
        observed_logs.append(log_path)
        log_path.write_text(
            "\n".join(
                json.dumps(value)
                for value in (
                    "id-" + container,
                    "sha256:" + "1" * 64,
                    "registry.example/espn/runtime@sha256:" + "2" * 64,
                    "running",
                    "2026-08-08T10:00:00Z",
                    "healthy",
                    {},
                    [],
                )
            ),
            encoding="utf-8",
        )
        return deploy.ProcessResult(0, 0.1, "a" * 64)

    monkeypatch.setattr(deploy, "_run_process", fake_process)
    deadline = deploy.time.monotonic() + 30
    first = deploy._capture_physical_fingerprint(
        context,
        phase="initial_state",
        attempt=1,
        stage="before",
        phase_deadline_monotonic=deadline,
    )
    second = deploy._capture_physical_fingerprint(
        context,
        phase="initial_state",
        attempt=1,
        stage="before",
        phase_deadline_monotonic=deadline,
    )

    assert first["fingerprint_sha256"] != second["fingerprint_sha256"]
    assert Path(str(first["evidence_path"])).is_file()
    assert Path(str(second["evidence_path"])).is_file()
    assert len(set(observed_logs)) == 6
    assert set(observed_logs[:3]).isdisjoint(observed_logs[3:])


def test_postdeploy_fingerprint_requires_all_six_exact_read_only_binds(
    tmp_path: Path,
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    release_root = Path(str(plan["release_root"]))
    mounts = [
        {
            "Type": "bind",
            "RW": False,
            "Destination": destination,
            "Source": str(source),
        }
        for destination, source in (
            ("/opt/airflow/dags", Path(str(plan["dagbag_root"]))),
            ("/opt/espn-source/dags", release_root / "dags"),
            ("/opt/airflow/scrapers", release_root / "scrapers"),
            ("/opt/airflow/scripts", release_root / "scripts"),
            ("/opt/airflow/configs/espn", release_root / "configs/espn"),
            (
                "/opt/airflow/configs/medallion",
                release_root / "configs/medallion",
            ),
        )
    ]
    fingerprint = {
        "containers": {
            "espn-airflow-airflow-metadb-1": {
                "status": "running",
                "health": "healthy",
                "image_reference": plan["postgres_image"],
            },
            "espn-airflow-airflow-scheduler-1": {
                "status": "running",
                "health": "healthy",
                "image_reference": plan["airflow_image"],
                "mounts": mounts,
            },
            "espn-airflow-airflow-webserver-1": {
                "status": "running",
                "health": "none",
                "image_reference": plan["airflow_image"],
                "port_bindings": {
                    "8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8086"}]
                },
                "mounts": mounts,
            },
        }
    }

    deploy._assert_postdeploy_fingerprint(plan, fingerprint)
    drifted = json.loads(json.dumps(fingerprint))
    drifted["containers"]["espn-airflow-airflow-scheduler-1"]["mounts"][2]["RW"] = True
    with pytest.raises(deploy.DeployError, match="exact read-only binds"):
        deploy._assert_postdeploy_fingerprint(plan, drifted)


def test_console_epipe_is_never_a_correctness_boundary() -> None:
    class Broken:
        def write(self, _value):
            raise BrokenPipeError

        def flush(self):
            raise BrokenPipeError

    deploy.emit_console({"status": "still-durable"}, stream=Broken())


def test_postdeploy_fingerprint_failure_is_journaled_failed_before_resume_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = deploy.build_plan(_spec(tmp_path))
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    fingerprint_sha = "d" * 64
    for phase in deploy.GUARD_PHASES[:-1]:
        attempt = 1
        log_path = str(context.log_root / f"seed-{phase}.log")
        context.guards, started = deploy.append_guard_event(
            context.guards,
            transition_event_sha256=deploy.transition_tail_sha256(context.transition),
            phase=phase,
            status="started",
            attempt=attempt,
            duration_seconds=0,
            physical_fingerprint=fingerprint_sha,
            physical_fingerprint_path="/durable/fingerprint-seed.json",
            recorded_at="2026-08-08T10:00:00Z",
            log_path=log_path,
            log_sha256=None,
        )
        context.persist_guards()
        context.transition_event(
            "guard_started",
            phase=phase,
            guard_event_sha256=started["event_sha256"],
            detail={"attempt": attempt},
        )
        context.guards, succeeded = deploy.append_guard_event(
            context.guards,
            transition_event_sha256=deploy.transition_tail_sha256(context.transition),
            phase=phase,
            status="succeeded",
            attempt=attempt,
            duration_seconds=1,
            physical_fingerprint=fingerprint_sha,
            physical_fingerprint_path="/durable/fingerprint-seed.json",
            recorded_at="2026-08-08T10:00:01Z",
            log_path=log_path,
            log_sha256="e" * 64,
        )
        context.persist_guards()
        context.transition_event(
            "guard_succeeded",
            phase=phase,
            guard_event_sha256=succeeded["event_sha256"],
            detail={"attempt": attempt},
        )

    monkeypatch.setattr(
        deploy,
        "_capture_physical_fingerprint",
        lambda *_args, **_kwargs: {
            "fingerprint_sha256": fingerprint_sha,
            "evidence_path": "/durable/mock-fingerprint.json",
        },
    )

    observed_timeouts: list[float] = []

    def successful_guard(_context, _argv, *, log_path, env, timeout_seconds, **_kwargs):
        observed_timeouts.append(timeout_seconds)
        payload = deploy.canonical_bytes(
            _guard_report(
                plan,
                phase=env["ESPN_DEPLOY_GUARD_PHASE"],
                attempt=int(env["ESPN_DEPLOY_GUARD_ATTEMPT"]),
            )
        )
        deploy._exclusive_regular_write(log_path, payload)
        return deploy.ProcessResult(0, 1, hashlib.sha256(payload).hexdigest())

    monkeypatch.setattr(deploy, "_run_process", successful_guard)

    def reject_postdeploy(_plan, _fingerprint):
        raise deploy.DeployError("post-deploy bind drifted")

    monkeypatch.setattr(deploy, "_assert_postdeploy_fingerprint", reject_postdeploy)

    with pytest.raises(deploy.DeployError, match="bind drifted"):
        deploy._run_guard_phase(context, "post_deploy")

    statuses = [
        event["status"]
        for event in context.guards["events"]
        if event["phase"] == "post_deploy"
    ]
    assert statuses == ["started", "failed"]
    assert observed_timeouts == [deploy.GUARD_PROCESS_TIMEOUT_SECONDS]
    assert deploy.guard_phase_succeeded(context.guards, "post_deploy") is False
    assert deploy.next_guard_attempt(context.guards, "post_deploy") == 2


def test_each_guard_spawn_revalidates_hashed_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    plan = deploy.build_plan(spec)
    context = deploy._load_or_initialize_context(
        plan, "apply", clock=deploy.time.time, sleeper=lambda _seconds: None
    )
    monkeypatch.setattr(
        deploy,
        "_capture_physical_fingerprint",
        lambda *_args, **_kwargs: {
            "fingerprint_sha256": "d" * 64,
            "evidence_path": "/durable/mock-fingerprint.json",
        },
    )

    def forbidden_spawn(*_args, **_kwargs):
        raise AssertionError("drifted guard reached process spawn")

    monkeypatch.setattr(deploy, "_run_process", forbidden_spawn)
    Path(str(spec["guard_artifacts"][0])).write_text(
        "raise SystemExit(9)\n", encoding="utf-8"
    )

    with pytest.raises(deploy.DeployError, match="artifact bytes drifted"):
        deploy._run_guard_phase(context, "initial_state")

    events = [
        event for event in context.guards["events"] if event["phase"] == "initial_state"
    ]
    assert [event["status"] for event in events] == ["started", "failed"]
    failed_log = Path(str(events[-1]["log_path"]))
    assert failed_log.is_file()
    assert stat.S_IMODE(failed_log.stat().st_mode) == 0o400
    assert (
        hashlib.sha256(failed_log.read_bytes()).hexdigest() == events[-1]["log_sha256"]
    )


def test_full_fake_apply_and_resume_seal_all_six_guards_and_restore_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
if args[0] == "inspect":
    name = args[-1]
    if name.startswith("espn-deploy-restore-"):
        raise SystemExit(1)
    web = name.endswith("webserver-1")
    airflow = web or name.endswith("scheduler-1")
    image_reference = (
        os.environ["FAKE_AIRFLOW_IMAGE"]
        if airflow
        else os.environ["FAKE_POSTGRES_IMAGE"]
    )
    mounts = []
    if airflow:
        mounts = [
            {"Type": "bind", "RW": False, "Destination": "/opt/airflow/dags", "Source": os.environ["FAKE_DAGBAG_ROOT"]},
            {"Type": "bind", "RW": False, "Destination": "/opt/espn-source/dags", "Source": os.environ["FAKE_RELEASE_ROOT"] + "/dags"},
            {"Type": "bind", "RW": False, "Destination": "/opt/airflow/scrapers", "Source": os.environ["FAKE_RELEASE_ROOT"] + "/scrapers"},
            {"Type": "bind", "RW": False, "Destination": "/opt/airflow/scripts", "Source": os.environ["FAKE_RELEASE_ROOT"] + "/scripts"},
            {"Type": "bind", "RW": False, "Destination": "/opt/airflow/configs/espn", "Source": os.environ["FAKE_RELEASE_ROOT"] + "/configs/espn"},
            {"Type": "bind", "RW": False, "Destination": "/opt/airflow/configs/medallion", "Source": os.environ["FAKE_RELEASE_ROOT"] + "/configs/medallion"},
            {"Type": "volume", "RW": True, "Destination": "/opt/airflow/logs", "Source": "/var/lib/docker/volumes/logs"},
        ]
    else:
        mounts = [{"Destination": "/var/lib/postgresql/data", "Source": "/var/lib/docker/volumes/pg"}]
    binding = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8086"}]} if web else {}
    for value in (
        "id-" + name,
        "sha256:" + "1" * 64,
        image_reference,
        "running",
        "2026-08-08T10:00:00Z",
        "none" if web else "healthy",
        binding,
        mounts,
    ):
        print(json.dumps(value, separators=(",", ":")))
    raise SystemExit(0)
if args[0] == "compose":
    raise SystemExit(0)
if args[0] == "run":
    print("restore-container-id")
    raise SystemExit(0)
if args[0] == "rm":
    raise SystemExit(0)
if args[0] == "exec":
    if "pg_dump" in args:
        sys.stdout.buffer.write(b"PGDMP-fake-complete-backup")
    elif "pg_restore" in args:
        payload = sys.stdin.buffer.read()
        if not payload.startswith(b"PGDMP"):
            raise SystemExit(9)
        if "--list" in args:
            print("; fake complete archive TOC")
    elif "psql" in args:
        print("1")
    raise SystemExit(0)
raise SystemExit(8)
""",
        encoding="utf-8",
    )
    os.chmod(fake_docker, 0o700)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RELEASE_ROOT", str(spec["release_root"]))
    monkeypatch.setenv("FAKE_DAGBAG_ROOT", str(spec["dagbag_root"]))
    monkeypatch.setenv("FAKE_AIRFLOW_IMAGE", str(spec["airflow_image"]))
    monkeypatch.setenv("FAKE_POSTGRES_IMAGE", str(spec["postgres_image"]))
    plan = deploy.build_plan(spec)

    result = deploy.execute_plan(plan, mode="apply")

    assert result["status"] == "deployed"
    state_root = Path(str(spec["state_root"]))
    assert result["heartbeat_path"] == str(state_root / "heartbeat.json")
    assert (
        result["heartbeat_sha256"]
        == hashlib.sha256((state_root / "heartbeat.json").read_bytes()).hexdigest()
    )
    transition = deploy._read_checksummed_json(state_root / "transition-journal.json")
    guards = deploy._read_checksummed_json(state_root / "guard-attempt-journal.json")
    deploy.validate_cross_bound_journals(transition, guards)
    for phase in deploy.GUARD_PHASES:
        assert [
            event["status"] for event in guards["events"] if event["phase"] == phase
        ] == ["started", "succeeded"]
    checkpoint = deploy._read_checkpoint(state_root / "checkpoint.json", plan)
    assert checkpoint["backup"]["restore_proof"]["passed"] is True
    assert Path(checkpoint["backup"]["dump_path"]).read_bytes().startswith(b"PGDMP")
    verify_log_path = Path(
        str(checkpoint["backup"]["restore_proof"]["verify_log"]["path"])
    )
    verify_log_payload = verify_log_path.read_bytes()
    verify_log_path.unlink()
    with pytest.raises(deploy.DeployError, match="restore proof verify_log"):
        deploy.execute_plan(plan, mode="resume")
    deploy._exclusive_regular_write(verify_log_path, verify_log_payload)
    recreate_success = next(
        event
        for event in transition["events"]
        if event["event"] == "action_succeeded"
        and event["detail"].get("action") == "airflow_recreate"
    )
    recreate_log_path = Path(str(recreate_success["detail"]["log_path"]))
    recreate_log_payload = recreate_log_path.read_bytes()
    recreate_log_path.unlink()
    with pytest.raises(
        deploy.DeployError, match="successful airflow_recreate action log"
    ):
        deploy.execute_plan(plan, mode="resume")
    deploy._exclusive_regular_write(recreate_log_path, recreate_log_payload)
    fingerprint_path = Path(str(guards["events"][0]["physical_fingerprint_path"]))
    fingerprint_payload = fingerprint_path.read_bytes()
    fingerprint_path.unlink()
    with pytest.raises(deploy.DeployError, match="fingerprint evidence"):
        deploy.execute_plan(plan, mode="resume")
    deploy._exclusive_regular_write(fingerprint_path, fingerprint_payload)
    heartbeat_path = state_root / "heartbeat.json"
    heartbeat_sidecar = state_root / "heartbeat.json.sha256"
    heartbeat_payload = heartbeat_path.read_bytes()
    heartbeat_sidecar_payload = heartbeat_sidecar.read_bytes()
    heartbeat_path.unlink()
    heartbeat_sidecar.unlink()
    with pytest.raises(deploy.DeployError, match="requires durable heartbeat"):
        deploy.execute_plan(plan, mode="resume")
    deploy._exclusive_regular_write(heartbeat_path, heartbeat_payload, mode=0o600)
    deploy._exclusive_regular_write(
        heartbeat_sidecar, heartbeat_sidecar_payload, mode=0o600
    )
    (state_root / "checkpoint.json.sha256").unlink()
    (state_root / "result.json.sha256").unlink()
    assert deploy.execute_plan(plan, mode="resume")["status"] == "already_converged"
    assert (state_root / "checkpoint.json.sha256").is_file()
    assert (state_root / "result.json.sha256").is_file()

    checkpoint_path = state_root / "checkpoint.json"
    result_path = state_root / "result.json"
    transition_path = state_root / "transition-journal.json"
    originals = {
        path: path.read_bytes()
        for path in (
            checkpoint_path,
            checkpoint_path.with_name(checkpoint_path.name + ".sha256"),
            result_path,
            result_path.with_name(result_path.name + ".sha256"),
            transition_path,
            transition_path.with_name(transition_path.name + ".sha256"),
        )
    }
    forged_checkpoint = json.loads(checkpoint_path.read_bytes())
    forged_checkpoint["transition_event_sha256"] = transition["events"][0][
        "event_sha256"
    ]
    forged_checkpoint_bytes = deploy.canonical_bytes(forged_checkpoint)
    forged_checkpoint_sha = hashlib.sha256(forged_checkpoint_bytes).hexdigest()
    deploy._atomic_regular_write(checkpoint_path, forged_checkpoint_bytes, 0o400)
    deploy._atomic_regular_write(
        checkpoint_path.with_name(checkpoint_path.name + ".sha256"),
        f"{forged_checkpoint_sha}  {checkpoint_path.name}\n".encode("ascii"),
        0o400,
    )
    forged_result = json.loads(result_path.read_bytes())
    forged_result["checkpoint_sha256"] = forged_checkpoint_sha
    forged_result_bytes = deploy.canonical_bytes(forged_result)
    forged_result_sha = hashlib.sha256(forged_result_bytes).hexdigest()
    deploy._atomic_regular_write(result_path, forged_result_bytes, 0o400)
    deploy._atomic_regular_write(
        result_path.with_name(result_path.name + ".sha256"),
        f"{forged_result_sha}  {result_path.name}\n".encode("ascii"),
        0o400,
    )
    forged_transition = json.loads(transition_path.read_bytes())
    deployed_event = dict(forged_transition["events"][-1])
    deployed_event.pop("event_sha256")
    deployed_event["detail"] = {"result_sha256": forged_result_sha}
    deployed_event["event_sha256"] = deploy._event_sha(deployed_event)
    forged_transition["events"][-1] = deployed_event
    forged_transition = deploy.reseal_transition_journal(forged_transition)
    deploy._write_checksummed_json(transition_path, forged_transition)
    with pytest.raises(deploy.DeployError, match="not exact pre-checkpoint guard"):
        deploy.execute_plan(plan, mode="resume")
    for path, payload in originals.items():
        deploy._atomic_regular_write(
            path,
            payload,
            0o600 if "transition-journal" in path.name else 0o400,
        )

    (state_root / "checkpoint.json").unlink()
    (state_root / "checkpoint.json.sha256").unlink()
    with pytest.raises(deploy.DeployError, match="result requires an exact checkpoint"):
        deploy.execute_plan(plan, mode="resume")
