"""Contracts for the immutable ESPN deploy quiescence guard."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import espn_release_guard_v1 as guard


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/espn_release_guard_v1.py"
DOCKER = Path("/usr/bin/docker")


def _environment(**overrides: str) -> dict[str, str]:
    values = {
        "ESPN_DEPLOY_GUARD_PHASE": "initial_state",
        "ESPN_DEPLOY_GUARD_ATTEMPT": "1",
        "ESPN_DEPLOY_TRANSITION_ID": "issue-1148-release-ordinal001",
        "ESPN_DEPLOY_PLAN_SHA256": "a" * 64,
    }
    values.update(overrides)
    return values


def _snapshot(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "kind": "espn-release-guard-snapshot-v1",
        "schema_version": 1,
        "transaction_read_only": "on",
        "dag_ids": list(guard.EXPECTED_DAG_IDS),
        "inactive_dag_ids": [],
        "unpaused_dag_ids": [],
        "active_dagrun_count": 0,
        "active_dag_ids": [],
    }
    values.update(overrides)
    return values


def _completed(
    payload: object, *, returncode: int = 0, stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    stdout = (
        payload
        if isinstance(payload, str)
        else json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    )
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_exact_readonly_query_accepts_only_the_quiescent_seven_dag_snapshot():
    invocation = guard.load_invocation(_environment())
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run_command(argv, **kwargs):
        calls.append((list(argv), dict(kwargs)))
        return _completed(_snapshot())

    observed = guard.read_snapshot(DOCKER, run_command=run_command)
    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=15,
        max_wait_seconds=0,
        run_command=run_command,
    )

    assert observed == guard.GuardSnapshot.from_mapping(_snapshot())
    assert report == {
        "kind": "espn-release-guard-v1",
        "schema_version": 1,
        "status": "ok",
        "phase": "initial_state",
        "attempt": 1,
        "transition_id": "issue-1148-release-ordinal001",
        "plan_sha256": "a" * 64,
        "checks": {
            "exact_dag_inventory": True,
            "all_dags_paused": True,
            "zero_active_dagruns": True,
            "transaction_read_only": True,
        },
    }
    command, kwargs = calls[0]
    assert command == [
        str(DOCKER),
        "exec",
        "--env",
        "PGOPTIONS=--default_transaction_read_only=on",
        guard.EXPECTED_METADB_CONTAINER,
        "psql",
        "--username=airflow",
        "--dbname=airflow",
        "--no-password",
        "--no-psqlrc",
        "--quiet",
        "--tuples-only",
        "--no-align",
        "--set=ON_ERROR_STOP=1",
        "--command",
        guard.READONLY_SQL,
    ]
    assert kwargs == {
        "capture_output": True,
        "text": True,
        "timeout": guard.QUERY_TIMEOUT_SECONDS,
        "check": False,
    }
    assert "BEGIN TRANSACTION READ ONLY" in guard.READONLY_SQL
    assert "current_setting('transaction_read_only')" in guard.READONLY_SQL
    assert "is_paused IS DISTINCT FROM TRUE" in guard.READONLY_SQL
    assert "is_active IS DISTINCT FROM TRUE" in guard.READONLY_SQL


@pytest.mark.parametrize(
    ("mutation", "failed_check"),
    [
        ({"dag_ids": list(guard.EXPECTED_DAG_IDS[:-1])}, "exact_dag_inventory"),
        (
            {"dag_ids": [*guard.EXPECTED_DAG_IDS, "dag_unreviewed_espn"]},
            "exact_dag_inventory",
        ),
        (
            {"inactive_dag_ids": [guard.EXPECTED_DAG_IDS[0]]},
            "exact_dag_inventory",
        ),
        ({"unpaused_dag_ids": [guard.EXPECTED_DAG_IDS[0]]}, "all_dags_paused"),
        (
            {
                "active_dagrun_count": 1,
                "active_dag_ids": [guard.EXPECTED_DAG_IDS[0]],
            },
            "zero_active_dagruns",
        ),
        ({"transaction_read_only": "off"}, "transaction_read_only"),
    ],
)
def test_every_nonquiescent_or_nonreadonly_snapshot_fails_closed(
    mutation: dict[str, object], failed_check: str
):
    invocation = guard.load_invocation(_environment())
    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=15,
        max_wait_seconds=0,
        run_command=lambda *_args, **_kwargs: _completed(_snapshot(**mutation)),
    )

    assert report["status"] == "failed"
    assert report["error_code"] == "quiescence_timeout"
    assert report["checks"][failed_check] is False


@pytest.mark.parametrize(
    "payload",
    [
        "not-json\n",
        '{"kind":"espn-release-guard-snapshot-v1","kind":"duplicate"}\n',
        "{}\n",
        json.dumps(_snapshot()) + "\nextra\n",
        json.dumps(_snapshot(active_dagrun_count=True)) + "\n",
    ],
)
def test_malformed_database_output_fails_closed_without_echoing_it(payload: str):
    invocation = guard.load_invocation(_environment())
    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=15,
        max_wait_seconds=0,
        run_command=lambda *_args, **_kwargs: _completed(payload),
    )

    encoded = guard.canonical_bytes(report)
    assert report["status"] == "failed"
    assert report["error_code"] == "snapshot_invalid"
    assert payload.encode() not in encoded


def test_child_failure_is_secret_safe_and_fails_closed():
    invocation = guard.load_invocation(_environment())
    secret = "postgresql://airflow:do-not-print@example.invalid/airflow"
    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=15,
        max_wait_seconds=0,
        run_command=lambda *_args, **_kwargs: _completed(
            "", returncode=9, stderr=secret
        ),
    )

    encoded = guard.canonical_bytes(report)
    assert report["status"] == "failed"
    assert report["error_code"] == "snapshot_command_failed"
    assert secret.encode() not in encoded
    assert b"stderr" not in encoded


def test_polling_uses_monotonic_deadline_and_can_converge_on_the_last_poll():
    invocation = guard.load_invocation(_environment())
    clock_value = [0.0]
    sleeps: list[float] = []
    observations = iter(
        [
            _snapshot(unpaused_dag_ids=[guard.EXPECTED_DAG_IDS[0]]),
            _snapshot(
                active_dagrun_count=1, active_dag_ids=[guard.EXPECTED_DAG_IDS[0]]
            ),
            _snapshot(),
        ]
    )

    def clock() -> float:
        return clock_value[0]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock_value[0] += seconds

    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=15,
        max_wait_seconds=31,
        run_command=lambda *_args, **_kwargs: _completed(next(observations)),
        clock=clock,
        sleeper=sleep,
    )

    assert report["status"] == "ok"
    assert sleeps == [15, 15]
    assert guard.MAX_WAIT_SECONDS == 1740
    assert guard.MAX_WAIT_SECONDS < 1800


def test_each_query_timeout_is_capped_by_the_remaining_guard_budget():
    invocation = guard.load_invocation(_environment())
    clock_value = [0.0]
    observed_timeouts: list[float] = []

    def clock() -> float:
        return clock_value[0]

    def run_command(_argv, **kwargs):
        timeout = float(kwargs["timeout"])
        observed_timeouts.append(timeout)
        clock_value[0] += timeout
        raise subprocess.TimeoutExpired(cmd="docker", timeout=timeout)

    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=1,
        max_wait_seconds=5,
        run_command=run_command,
        clock=clock,
        sleeper=lambda _seconds: pytest.fail("deadline query must not sleep"),
    )

    assert report["status"] == "failed"
    assert report["error_code"] == "snapshot_command_failed"
    assert observed_timeouts == [5.0]
    assert clock_value[0] == 5.0


def test_valid_snapshot_returned_after_deadline_still_fails_closed():
    invocation = guard.load_invocation(_environment())
    clock_value = [0.0]
    observed_timeouts: list[float] = []

    def clock() -> float:
        return clock_value[0]

    def run_command(_argv, **kwargs):
        observed_timeouts.append(float(kwargs["timeout"]))
        clock_value[0] = 6.0
        return _completed(_snapshot())

    report = guard.run_guard(
        invocation,
        docker_path=DOCKER,
        poll_seconds=1,
        max_wait_seconds=5,
        run_command=run_command,
        clock=clock,
        sleeper=lambda _seconds: pytest.fail("expired guard must not sleep"),
    )

    assert report["status"] == "failed"
    assert report["error_code"] == "quiescence_timeout"
    assert observed_timeouts == [5.0]
    assert all(report["checks"].values())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"ESPN_DEPLOY_GUARD_PHASE": "unknown"}, "guard phase"),
        ({"ESPN_DEPLOY_GUARD_ATTEMPT": "0"}, "guard attempt"),
        ({"ESPN_DEPLOY_GUARD_ATTEMPT": "01"}, "guard attempt"),
        ({"ESPN_DEPLOY_TRANSITION_ID": "bad/id"}, "transition ID"),
        ({"ESPN_DEPLOY_PLAN_SHA256": "A" * 64}, "plan SHA-256"),
    ],
)
def test_injected_deploy_identity_is_strictly_validated(
    mutation: dict[str, str], message: str
):
    with pytest.raises(guard.GuardError, match=message):
        guard.load_invocation(_environment(**mutation))


def test_cli_output_is_one_canonical_json_document(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.setattr(
        guard,
        "_run_command",
        lambda *_args, **_kwargs: _completed(_snapshot()),
    )

    status = guard.main(
        [
            "guard",
            "--docker-path",
            str(DOCKER),
            "--poll-seconds",
            "15",
            "--max-wait-seconds",
            "1740",
        ],
        environ=_environment(),
    )
    output = capsys.readouterr()

    assert status == 0
    assert output.err == ""
    decoded = json.loads(output.out)
    assert output.out.encode() == guard.canonical_bytes(decoded)


def test_invalid_cli_environment_is_canonical_and_secret_safe(
    capsys: pytest.CaptureFixture[str],
):
    environment = _environment(ESPN_DEPLOY_PLAN_SHA256="secret-value")

    status = guard.main(
        [
            "guard",
            "--docker-path",
            str(DOCKER),
            "--max-wait-seconds",
            "0",
        ],
        environ=environment,
    )
    output = capsys.readouterr()

    assert status == 1
    assert output.err == ""
    decoded = json.loads(output.out)
    assert decoded == {
        "error_code": "invalid_invocation",
        "kind": "espn-release-guard-v1",
        "schema_version": 1,
        "status": "failed",
    }
    assert "secret-value" not in output.out
    assert output.out.encode() == guard.canonical_bytes(decoded)


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["guard"],
        ["guard", "--docker-path", str(DOCKER), "--poll-seconds", "secret"],
        ["unknown"],
    ],
)
def test_malformed_cli_arguments_are_canonical_and_secret_safe(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    status = guard.main(argv, environ=_environment())
    output = capsys.readouterr()

    assert status == 1
    assert output.err == ""
    decoded = json.loads(output.out)
    assert decoded == {
        "error_code": "invalid_invocation",
        "kind": "espn-release-guard-v1",
        "schema_version": 1,
        "status": "failed",
    }
    assert "secret" not in output.out
    assert output.out.encode() == guard.canonical_bytes(decoded)


def test_source_has_no_writer_or_shell_boundary_and_is_package_visible():
    source = SCRIPT.read_text(encoding="utf-8")

    assert SCRIPT.is_file()
    assert hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    assert "shell=True" not in source
    for forbidden in (
        "os.remove(",
        "os.unlink(",
        ".mkdir(",
        ".touch(",
        "os.replace(",
        "os.rename(",
        "tempfile",
        "shutil",
    ):
        assert forbidden not in source
    upper_sql = guard.READONLY_SQL.upper()
    for forbidden_sql in (
        " INSERT ",
        " UPDATE ",
        " DELETE ",
        " CREATE ",
        " ALTER ",
        " DROP ",
        " TRUNCATE ",
        " GRANT ",
        " REVOKE ",
        " COPY ",
        " CALL ",
        " DO ",
    ):
        assert forbidden_sql not in f" {upper_sql} "


def test_script_help_runs_from_release_root_without_import_side_effects():
    completed = subprocess.run(
        [sys.executable, "-B", str(SCRIPT), "--help"],
        cwd=ROOT,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0
    assert "strictly read-only" in completed.stdout
    assert completed.stderr == ""
