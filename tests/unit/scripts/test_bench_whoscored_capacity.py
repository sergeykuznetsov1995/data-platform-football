"""Focused tests for the sustained non-publishing WhoScored canary."""

from argparse import Namespace
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "research"
    / "bench_whoscored_capacity.py"
)
SPEC = importlib.util.spec_from_file_location("bench_whoscored_capacity", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
capacity = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = capacity
SPEC.loader.exec_module(capacity)


def _args(**overrides: Any) -> Namespace:
    values = {
        "duration_seconds": 900.0,
        "sample_interval_seconds": 30.0,
        "scopes": ["INT-World Cup=2026"],
        "match_limit": 3,
        "profile_limit": 3,
        "catalog": "/tmp/competitions.yaml",
        "flaresolverr_url": "http://127.0.0.1:8191",
        "containers": ["flaresolverr", "proxy_filter"],
        "output": None,
    }
    values.update(overrides)
    return Namespace(**values)


def _workflow_report(
    *,
    page_units: int = 375,
    source_request_attempts: int | None = None,
    paid_bytes: int = 0,
    paid_route_requests: int = 0,
    publishes: bool = False,
) -> dict[str, Any]:
    return {
        "benchmark_version": capacity.EXPECTED_WORKFLOW_VERSION,
        "status": "success",
        "publishes": publishes,
        "writes_bronze": False,
        "executes_ddl": False,
        "elapsed_seconds": 900.0,
        "stage_statistics_contract": {"expected_feed_states_per_stage": 68},
        "phases": [
            {
                "name": "cold",
                "selected_match_ids": [101, 102, 103],
                "selected_profile_ids": [901, 902, 903],
                "results": [
                    {
                        "entity": "schedule",
                        "metadata": {
                            "source_stage_count": 3,
                            "source_stage_ids": [1, 2, 3],
                        },
                    },
                    {"entity": "matches"},
                    {"entity": "previews"},
                    {"entity": "profiles"},
                ],
                "traffic": {
                    "source_request_attempts": (
                        page_units
                        if source_request_attempts is None
                        else source_request_attempts
                    ),
                    "successful_page_units": page_units,
                    "paid_proxy_bytes": paid_bytes,
                    "paid_route_requests": paid_route_requests,
                },
            }
        ],
    }


def _container_state(
    name: str,
    *,
    restart_count: int = 0,
    oom_killed: bool = False,
    running: bool = True,
    container_id: str | None = None,
    image_id: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "id": container_id or f"id-{name}",
        "image_id": image_id or (
            capacity.REQUIRED_FLARESOLVERR_IMAGE_ID
            if name == "flaresolverr"
            else f"sha256:{name}"
        ),
        "command_contract_ok": name == "flaresolverr",
        "helper_mount_contract_ok": name == "flaresolverr",
        "compose_identity_ok": name == "flaresolverr",
        "published_endpoint_contract_ok": name == "flaresolverr",
        "status": "running" if running else "exited",
        "running": running,
        "oom_killed": oom_killed,
        "restart_count": restart_count,
        "pid": 100 if name == "flaresolverr" else 200,
        "memory_usage_bytes": 64 * capacity.GIB // 1024,
        "memory_limit_bytes": 4 * capacity.GIB,
        "process_count": 3,
    }


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


def _pid_is_dead(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text().split()[2]
    except (FileNotFoundError, ProcessLookupError):
        return True
    return state == "Z"


class FakeCapacityRuntime:
    def __init__(
        self,
        *,
        report: dict[str, Any] | None = None,
        final_rss_bytes: int = capacity.GIB,
        restart_after_baseline: bool = False,
        oom_after_baseline: bool = False,
        mutate_runtime_identity: bool = False,
        runtime_identity_mutation_call: int | None = None,
        curl_cffi_version: str = capacity.REQUIRED_CURL_CFFI_VERSION,
    ) -> None:
        self.clock = FakeClock()
        self.report = report or _workflow_report()
        self.final_rss_bytes = final_rss_bytes
        self.restart_after_baseline = restart_after_baseline
        self.oom_after_baseline = oom_after_baseline
        self.mutate_runtime_identity = mutate_runtime_identity
        self.runtime_identity_mutation_call = (
            runtime_identity_mutation_call
            if runtime_identity_mutation_call is not None
            else (2 if mutate_runtime_identity else None)
        )
        self.curl_cffi_version = curl_cffi_version
        self.inspect_calls = 0
        self.identity_calls = 0
        self.commands: list[Any] = []
        self.launch_attempts: list[Any] = []

    def inspect_containers(self, names):
        self.inspect_calls += 1
        changed = self.inspect_calls > 1
        return {
            name: _container_state(
                name,
                restart_count=int(changed and self.restart_after_baseline),
                oom_killed=bool(changed and self.oom_after_baseline),
            )
            for name in names
        }

    def sample_rss(self, root_pids):
        assert os.getpid() in root_pids
        rss_bytes = (
            self.final_rss_bytes if self.clock.value > 0 else capacity.GIB
        )
        return {
            "root_pids": list(root_pids),
            "process_count": 7,
            "rss_bytes": rss_bytes,
        }

    def runtime_identity(self, args):
        del args
        self.identity_calls += 1
        manifest_token = (
            "d"
            if self.runtime_identity_mutation_call is not None
            and self.identity_calls >= self.runtime_identity_mutation_call
            else "b"
        )
        return {
            "git_revision": "a" * 40,
            "git_clean": False,
            "manifest_sha256": manifest_token * 64,
            "file_sha256": {
                "runtime.py": "c" * 64,
                "external:unshare": capacity.REQUIRED_UNSHARE_SHA256,
            },
            "python_executable": sys.executable,
            "python_prefix": sys.prefix,
            "python_version": sys.version.split()[0],
            "dependency_versions": {"curl_cffi": self.curl_cffi_version},
        }

    def run_round(
        self,
        commands,
        *,
        deadline,
        on_sample,
        on_outcome,
        should_stop,
        before_launch,
        monotonic,
        sleep,
    ):
        del sleep
        assert monotonic is self.clock.monotonic or callable(monotonic)
        assert len(commands) == capacity.WORKER_COUNT
        self.commands.extend(commands)
        self.clock.value = deadline
        for command in commands:
            before_launch()
            self.launch_attempts.append(command)
            on_outcome(
                capacity.WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=0,
                    report=dict(self.report),
                    elapsed_seconds=900.0,
                    stderr_bytes=0,
                    stderr_sha256=hashlib.sha256(b"").hexdigest(),
                )
            )
        on_sample(True)
        assert not should_stop() or self.report.get("publishes") is True

    def dependencies(self):
        return capacity.CapacityDependencies(
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
            inspect_containers=self.inspect_containers,
            sample_rss=self.sample_rss,
            runtime_identity=self.runtime_identity,
            run_round=self.run_round,
        )


def _gate(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(gate for gate in report["gates"] if gate["name"] == name)


def test_four_process_capacity_run_passes_exact_gates_without_publish_sinks():
    runtime = FakeCapacityRuntime()

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 0
    assert report["status"] == "success"
    assert report["worker_count"] == 4
    assert report["completed_by_worker"] == {"0": 1, "1": 1, "2": 1, "3": 1}
    assert report["projected_page_units_per_day"] == 144_000
    assert report["publishes"] is False
    assert report["writes_bronze"] is False
    assert report["executes_ddl"] is False
    assert report["raw_store_policy"] == "per-process temporary local storage"
    assert report["repository_policy"] == "per-process in-memory repository"
    assert report["max_source_stage_count"] == 3
    assert _gate(report, "representative_workload")["passed"] is True
    assert all(gate["passed"] for gate in report["gates"])
    assert len(runtime.commands) == 4
    assert {command.worker_id for command in runtime.commands} == {0, 1, 2, 3}
    assert all(str(capacity.WORKFLOW_SCRIPT) in command.argv for command in runtime.commands)
    assert all("--output" not in command.argv for command in runtime.commands)
    assert all("phases" not in summary for summary in report["runs"])
    assert runtime.identity_calls >= 6
    assert _gate(report, "runtime_identity")["passed"] is True
    assert _gate(report, "runtime_identity")["git_clean"] is False
    assert json.loads(json.dumps(report)) == report


def test_throughput_below_144k_fails_closed():
    runtime = FakeCapacityRuntime(report=_workflow_report(page_units=374))

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "throughput")["passed"] is False
    assert report["projected_page_units_per_day"] == 143_616


def test_physical_retries_do_not_inflate_completed_page_unit_projection():
    runtime = FakeCapacityRuntime(
        report=_workflow_report(page_units=375, source_request_attempts=900)
    )

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 0
    assert report["source_request_attempts"] == 3_600
    assert report["page_units"] == 1_500
    assert report["projected_page_units_per_day"] == 144_000


def test_runtime_hash_change_stops_before_workers_launch():
    runtime = FakeCapacityRuntime(mutate_runtime_identity=True)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert runtime.commands == []
    assert _gate(report, "runtime_identity")["passed"] is False
    assert "runtime_identity" in report["stop_reasons"]


def test_runtime_hash_change_immediately_before_launch_stops_actual_launch():
    runtime = FakeCapacityRuntime(runtime_identity_mutation_call=3)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert len(runtime.commands) == capacity.WORKER_COUNT
    assert runtime.launch_attempts == []
    assert _gate(report, "runtime_identity")["passed"] is False
    assert "runtime_identity" in report["stop_reasons"]


def test_initial_runtime_identity_must_retain_production_dependency_pin():
    runtime = FakeCapacityRuntime(curl_cffi_version="0.14.0")

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert runtime.inspect_calls == 0
    assert runtime.commands == []
    gate = _gate(report, "runtime_identity")
    assert gate["passed"] is False
    assert gate["violations"] == [
        "runtime identity does not match production curl_cffi==0.15.0"
    ]


def test_malformed_traffic_cannot_falsely_prove_paid_or_sink_safety():
    child = _workflow_report()
    child["publishes"] = True
    child["phases"][0]["traffic"]["source_request_attempts"] = "malformed"
    runtime = FakeCapacityRuntime(report=child)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "paid_traffic")["passed"] is False
    assert _gate(report, "paid_traffic")["evidence_violations"]
    assert _gate(report, "non_publishing")["passed"] is False
    assert report["runs"][0]["publishes"] is True
    assert report["runs"][0]["traffic_evidence_valid"] is False


def test_fractional_paid_request_count_is_rejected_not_truncated():
    child = _workflow_report()
    child["phases"][0]["traffic"]["paid_route_requests"] = 0.9
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=0,
        report=child,
        elapsed_seconds=1.0,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )

    summary = capacity._summarize_outcome(outcome)

    assert summary["status"] == "failed"
    assert summary["traffic_evidence_valid"] is False
    assert summary["paid_route_requests"] == 0
    assert summary["error"] == (
        "paid_route_requests must be a non-negative integer"
    )


def test_non_json_child_report_leaves_paid_and_sink_evidence_unproven():
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=1,
        report=None,
        elapsed_seconds=1.0,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )
    accumulator = capacity.CapacityAccumulator()

    capacity._accept_outcome(accumulator, outcome)

    assert accumulator.traffic_evidence_violations == [
        "worker 0 iteration 0 did not prove paid traffic counters"
    ]
    assert accumulator.safety_violations == [
        "worker 0 iteration 0 did not prove non-publishing execution"
    ]


def test_huge_elapsed_integer_is_safely_rejected_after_traffic_is_retained():
    child = _workflow_report()
    child["elapsed_seconds"] = 10**400
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=0,
        report=child,
        elapsed_seconds=1.0,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )

    summary = capacity._summarize_outcome(outcome)

    assert summary["status"] == "failed"
    assert summary["traffic_evidence_valid"] is True
    assert summary["paid_bytes"] == 0
    assert summary["publishes"] is False
    assert summary["error"] == (
        "workflow elapsed_seconds must be finite and non-negative"
    )


def test_any_paid_traffic_fails_closed():
    runtime = FakeCapacityRuntime(
        report=_workflow_report(paid_bytes=1, paid_route_requests=1)
    )

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "paid_traffic")["passed"] is False
    assert report["paid_bytes"] == 4
    assert report["paid_route_requests"] == 4


def test_aggregate_rss_above_12_gib_fails_closed():
    runtime = FakeCapacityRuntime(final_rss_bytes=capacity.MAX_RSS_BYTES + 1)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "memory")["passed"] is False
    assert report["max_harness_rss_bytes"] == capacity.MAX_RSS_BYTES + 1
    assert report["max_aggregate_memory_bytes"] > capacity.MAX_RSS_BYTES


def test_container_restart_and_oom_each_fail_closed():
    for runtime in (
        FakeCapacityRuntime(restart_after_baseline=True),
        FakeCapacityRuntime(oom_after_baseline=True),
    ):
        code, report = capacity.run(_args(), dependencies=runtime.dependencies())

        assert code == 1
        assert _gate(report, "container_restart_oom")["passed"] is False
        assert _gate(report, "container_restart_oom")["violations"]


def test_child_that_does_not_prove_disabled_sinks_fails_closed():
    runtime = FakeCapacityRuntime(report=_workflow_report(publishes=True))

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "non_publishing")["passed"] is False
    assert report["stop_reasons"] == ["non_publishing", "worker_health"]


def test_single_stage_scope_fails_representative_workload_gate():
    child = _workflow_report()
    child["phases"][0]["results"][0]["metadata"]["source_stage_count"] = 1
    runtime = FakeCapacityRuntime(report=child)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    gate = _gate(report, "representative_workload")
    assert gate["passed"] is False
    assert gate["observed_max_source_stage_count"] == 1


def test_missing_match_preview_profile_shape_fails_worker_contract():
    child = _workflow_report()
    child["phases"][0]["results"] = child["phases"][0]["results"][:1]
    runtime = FakeCapacityRuntime(report=child)

    code, report = capacity.run(_args(), dependencies=runtime.dependencies())

    assert code == 1
    assert _gate(report, "worker_health")["passed"] is False
    assert "omitted entity results" in " ".join(
        _gate(report, "worker_health")["errors"]
    )


def test_child_failure_payload_is_hashed_not_copied_into_evidence():
    child = _workflow_report()
    child["status"] = "failed"
    child["error"] = "parser saw secret-payload at https://source.invalid/?token=x"
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=1,
        report=child,
        elapsed_seconds=1,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )

    summary = capacity._summarize_outcome(outcome)

    encoded = json.dumps(summary)
    assert summary["error"] == "workflow reported failure"
    assert len(summary["workflow_error_sha256"]) == 64
    assert "secret-payload" not in encoded
    assert "token=x" not in encoded


@pytest.mark.parametrize(
    "path,value",
    [
        (("elapsed_seconds",), "SENSITIVE_SENTINEL"),
        (
            ("phases", 0, "traffic", "source_request_attempts"),
            "SENSITIVE_SENTINEL",
        ),
        (
            ("phases", 0, "traffic", "successful_page_units"),
            "SENSITIVE_SENTINEL",
        ),
        (
            ("phases", 0, "traffic", "paid_proxy_bytes"),
            "SENSITIVE_SENTINEL",
        ),
        (
            ("phases", 0, "traffic", "paid_route_requests"),
            "SENSITIVE_SENTINEL",
        ),
        (("publishes",), {"credential": "SENSITIVE_SENTINEL"}),
        (("writes_bronze",), "SENSITIVE_SENTINEL"),
        (("executes_ddl",), ["SENSITIVE_SENTINEL"]),
        (
            ("stage_statistics_contract", "expected_feed_states_per_stage"),
            "SENSITIVE_SENTINEL",
        ),
        (
            ("phases", 0, "results", 0, "metadata", "source_stage_count"),
            "SENSITIVE_SENTINEL",
        ),
    ],
)
def test_untrusted_child_fields_never_leak_into_evidence(path, value):
    child = deepcopy(_workflow_report())
    target: Any = child
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=0,
        report=child,
        elapsed_seconds=1.0,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )

    encoded = json.dumps(capacity._summarize_outcome(outcome), sort_keys=True)

    assert "SENSITIVE_SENTINEL" not in encoded


def test_pre_phase_dependency_failure_surfaces_actual_safe_error():
    child = {
        "benchmark_version": capacity.EXPECTED_WORKFLOW_VERSION,
        "status": "failed",
        "error": "RuntimeError: curl_cffi is required for WhoScoredTransport",
        "publishes": False,
        "writes_bronze": False,
        "executes_ddl": False,
        "elapsed_seconds": 0.0,
        "phases": [],
    }
    outcome = capacity.WorkerOutcome(
        worker_id=0,
        iteration=0,
        scope="INT-World Cup=2026",
        returncode=1,
        report=child,
        elapsed_seconds=0.003,
        stderr_bytes=0,
        stderr_sha256=hashlib.sha256(b"").hexdigest(),
    )

    summary = capacity._summarize_outcome(outcome)

    assert summary["error"] == child["error"]
    assert "cold phase" not in summary["error"]
    assert len(summary["workflow_error_sha256"]) == 64


def test_real_runtime_preflight_requires_pinned_curl_cffi(monkeypatch):
    def missing_module(name: str):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(capacity.importlib, "import_module", missing_module)

    assert capacity._workflow_runtime_preflight() == (
        "host Python dependency unavailable: curl_cffi==0.15.0 is required"
    )


def test_real_runtime_preflight_fails_when_requests_submodule_cannot_import(
    monkeypatch,
):
    calls = []

    def import_module(name: str):
        calls.append(name)
        if name == "curl_cffi":
            return SimpleNamespace(__name__=name)
        raise ImportError("SENSITIVE_SENTINEL")

    monkeypatch.setattr(capacity.importlib, "import_module", import_module)

    error = capacity._workflow_runtime_preflight()

    assert calls == ["curl_cffi", "curl_cffi.requests"]
    assert error == (
        "host Python dependency unavailable: curl_cffi==0.15.0 is required"
    )
    assert "SENSITIVE_SENTINEL" not in error


def test_real_runtime_preflight_rejects_nonproduction_distribution(monkeypatch):
    monkeypatch.setattr(
        capacity.importlib,
        "import_module",
        lambda name: SimpleNamespace(__name__=name),
    )
    monkeypatch.setattr(
        capacity, "_installed_curl_cffi_version", lambda: "0.14.0"
    )

    assert capacity._workflow_runtime_preflight() == (
        "host Python dependency version mismatch: expected "
        "curl_cffi==0.15.0, found 0.14.0"
    )


def test_preflight_pin_matches_production_scraping_requirements():
    requirements = (
        capacity.REPO_ROOT
        / "docker"
        / "images"
        / "airflow"
        / "requirements-scraping.txt"
    ).read_text().splitlines()

    assert f"curl_cffi=={capacity.REQUIRED_CURL_CFFI_VERSION}" in requirements


def test_real_run_stops_before_host_monitoring_when_preflight_fails(monkeypatch):
    monkeypatch.setattr(
        capacity,
        "_workflow_runtime_preflight",
        lambda: "host Python dependency unavailable: curl_cffi==0.15.0 is required",
    )
    monkeypatch.setattr(
        capacity,
        "_default_dependencies",
        lambda: (_ for _ in ()).throw(AssertionError("monitoring initialized")),
    )

    code, report = capacity.run(_args())

    assert code == 2
    assert report["status"] == "configuration_error"
    assert report["error"] == (
        "host Python dependency unavailable: curl_cffi==0.15.0 is required"
    )
    assert report["publishes"] is False


def test_container_gate_detects_recreate_restart_stop_and_oom():
    baseline = {"flaresolverr": _container_state("flaresolverr")}

    assert capacity._container_gate_violations(baseline, baseline) == []
    assert "recreated" in " ".join(
        capacity._container_gate_violations(
            baseline,
            {
                "flaresolverr": _container_state(
                    "flaresolverr", container_id="replacement"
                )
            },
        )
    )
    assert "restart count changed" in " ".join(
        capacity._container_gate_violations(
            baseline,
            {"flaresolverr": _container_state("flaresolverr", restart_count=1)},
        )
    )
    assert "not running" in " ".join(
        capacity._container_gate_violations(
            baseline,
            {"flaresolverr": _container_state("flaresolverr", running=False)},
        )
    )
    assert "OOMKilled" in " ".join(
        capacity._container_gate_violations(
            baseline,
            {"flaresolverr": _container_state("flaresolverr", oom_killed=True)},
        )
    )


def test_wrong_flaresolverr_image_digest_stops_before_worker_launch():
    runtime = FakeCapacityRuntime()

    def inspect(names):
        return {
            name: _container_state(
                name,
                image_id=("sha256:" + "0" * 64)
                if name == "flaresolverr"
                else None,
            )
            for name in names
        }

    dependencies = replace(
        runtime.dependencies(), inspect_containers=inspect
    )
    code, report = capacity.run(_args(), dependencies=dependencies)

    assert code == 1
    assert runtime.commands == []
    container_gate = _gate(report, "container_restart_oom")
    assert container_gate["passed"] is False
    assert any(
        "image digest" in violation
        for violation in container_gate["violations"]
    )


def test_host_docker_inspector_binds_restart_oom_and_pid_evidence(
    monkeypatch, tmp_path
):
    environment_file = tmp_path / ".env"
    environment_file.write_text("SAFE_TEST_VALUE=1\n")
    environment_file.chmod(0o600)
    monkeypatch.setattr(
        capacity, "PRODUCTION_COMPOSE_ENV_FILE", environment_file
    )
    docker_payload = [
        {
            "Name": f"/{name}",
            "Id": ("a" if name == "airflow-scheduler" else "b") * 64,
            "Image": (
                capacity.REQUIRED_FLARESOLVERR_IMAGE_ID
                if name == "flaresolverr"
                else "sha256:" + "c" * 64
            ),
            "Config": (
                {
                    "Cmd": list(capacity.REQUIRED_FLARESOLVERR_COMMAND),
                    "Labels": {
                        "com.docker.compose.project": "data-platform",
                        "com.docker.compose.service": "flaresolverr",
                        "com.docker.compose.config-hash": "d" * 64,
                        "com.docker.compose.image": (
                            capacity.REQUIRED_FLARESOLVERR_IMAGE_ID
                        ),
                        "com.docker.compose.oneoff": "False",
                        "com.docker.compose.project.config_files": str(
                            (capacity.REPO_ROOT / "compose.yaml").resolve()
                        ),
                        "com.docker.compose.project.environment_file": str(
                            capacity.PRODUCTION_COMPOSE_ENV_FILE
                        ),
                        "com.docker.compose.project.working_dir": str(
                            capacity.REPO_ROOT.resolve()
                        ),
                    },
                }
                if name == "flaresolverr"
                else {}
            ),
            "Mounts": (
                [
                    {
                        "Type": "bind",
                        "Source": str(
                            (
                                capacity.REPO_ROOT
                                / "scripts"
                                / "flaresolverr_extended.py"
                            ).resolve()
                        ),
                        "Destination": "/app/flaresolverr_extended.py",
                        "Mode": "ro",
                        "RW": False,
                    }
                ]
                if name == "flaresolverr"
                else []
            ),
            "NetworkSettings": (
                {
                    "Ports": {
                        "8191/tcp": [
                            {"HostIp": "127.0.0.1", "HostPort": "8191"}
                        ]
                    }
                }
                if name == "flaresolverr"
                else {}
            ),
            "RestartCount": index,
            "State": {
                "Status": "running",
                "Running": True,
                "OOMKilled": False,
                "Pid": 1000 + index,
            },
        }
        for index, name in enumerate(("airflow-scheduler", "flaresolverr"))
    ]
    observed = {"calls": []}

    def fake_run(argv, **kwargs):
        observed["calls"].append((argv, kwargs))
        if argv[1] == "inspect":
            return SimpleNamespace(returncode=0, stdout=json.dumps(docker_payload))
        if argv[1] == "compose":
            return SimpleNamespace(
                returncode=0, stdout=f"flaresolverr {'d' * 64}\n"
            )
        stats = "\n".join(
            json.dumps(
                {
                    "Name": name,
                    "MemUsage": f"{64 + index}.5MiB / 4GiB",
                    "PIDs": str(7 + index),
                }
            )
            for index, name in enumerate(
                ("airflow-scheduler", "flaresolverr")
            )
        )
        return SimpleNamespace(returncode=0, stdout=stats)

    monkeypatch.setattr(capacity.subprocess, "run", fake_run)

    result = capacity._inspect_containers(
        ("airflow-scheduler", "flaresolverr")
    )

    assert observed["calls"][0][0] == [
        "docker",
        "inspect",
        "airflow-scheduler",
        "flaresolverr",
    ]
    assert observed["calls"][1][0][:2] == ["docker", "compose"]
    assert observed["calls"][2][0] == [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{json .}}",
        "airflow-scheduler",
        "flaresolverr",
    ]
    assert all(call[1]["timeout"] == 15 for call in observed["calls"])
    assert result["airflow-scheduler"]["restart_count"] == 0
    assert result["flaresolverr"]["restart_count"] == 1
    assert result["flaresolverr"]["pid"] == 1001
    assert (
        result["flaresolverr"]["image_id"]
        == capacity.REQUIRED_FLARESOLVERR_IMAGE_ID
    )
    assert result["flaresolverr"]["command_contract_ok"] is True
    assert result["flaresolverr"]["helper_mount_contract_ok"] is True
    assert result["flaresolverr"]["compose_identity_ok"] is True
    assert result["flaresolverr"]["published_endpoint_contract_ok"] is True
    assert result["flaresolverr"]["memory_usage_bytes"] == int(
        65.5 * 1024**2
    )


def test_flaresolverr_inspect_contract_fails_each_runtime_binding():
    expected_hash = "d" * 64
    raw = {
        "Name": "/flaresolverr",
        "Id": "a" * 64,
        "Image": capacity.REQUIRED_FLARESOLVERR_IMAGE_ID,
        "RestartCount": 0,
        "State": {
            "Status": "running",
            "Running": True,
            "OOMKilled": False,
            "Pid": 1000,
        },
        "Config": {
            "Cmd": list(capacity.REQUIRED_FLARESOLVERR_COMMAND),
            "Labels": {
                "com.docker.compose.project": capacity.REQUIRED_COMPOSE_PROJECT,
                "com.docker.compose.service": "flaresolverr",
                "com.docker.compose.config-hash": expected_hash,
                "com.docker.compose.image": capacity.REQUIRED_FLARESOLVERR_IMAGE_ID,
                "com.docker.compose.oneoff": "False",
                "com.docker.compose.project.config_files": str(
                    (capacity.REPO_ROOT / "compose.yaml").resolve()
                ),
                "com.docker.compose.project.environment_file": str(
                    capacity.PRODUCTION_COMPOSE_ENV_FILE
                ),
                "com.docker.compose.project.working_dir": str(
                    capacity.REPO_ROOT.resolve()
                ),
            },
        },
        "Mounts": [
            {
                "Type": "bind",
                "Source": str(
                    (
                        capacity.REPO_ROOT
                        / "scripts"
                        / "flaresolverr_extended.py"
                    ).resolve()
                ),
                "Destination": capacity.REQUIRED_FLARESOLVERR_MOUNT_TARGET,
                "Mode": "ro",
                "RW": False,
            }
        ],
        "NetworkSettings": {
            "Ports": {
                "8191/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8191"}]
            }
        },
    }
    valid = capacity._normalise_container(
        raw, expected_flaresolverr_config_hash=expected_hash
    )
    assert all(
        valid[field]
        for field in (
            "command_contract_ok",
            "helper_mount_contract_ok",
            "compose_identity_ok",
            "published_endpoint_contract_ok",
        )
    )

    wrong_command = deepcopy(raw)
    wrong_command["Config"]["Cmd"] = ["python", "stock.py"]
    assert capacity._normalise_container(
        wrong_command, expected_flaresolverr_config_hash=expected_hash
    )["command_contract_ok"] is False
    writable_mount = deepcopy(raw)
    writable_mount["Mounts"][0]["RW"] = True
    assert capacity._normalise_container(
        writable_mount, expected_flaresolverr_config_hash=expected_hash
    )["helper_mount_contract_ok"] is False
    stale_compose = deepcopy(raw)
    stale_compose["Config"]["Labels"]["com.docker.compose.config-hash"] = "e" * 64
    assert capacity._normalise_container(
        stale_compose, expected_flaresolverr_config_hash=expected_hash
    )["compose_identity_ok"] is False
    public_port = deepcopy(raw)
    public_port["NetworkSettings"]["Ports"]["8191/tcp"][0]["HostIp"] = "0.0.0.0"
    assert capacity._normalise_container(
        public_port, expected_flaresolverr_config_hash=expected_hash
    )["published_endpoint_contract_ok"] is False


def test_subprocess_round_launches_four_real_isolated_processes():
    payload = json.dumps(_workflow_report(page_units=1))
    child_code = (
        "import os; "
        "assert os.environ['WHOSCORED_SOURCE_CIRCUIT_WAIT']=='1'; "
        "assert os.environ['WHOSCORED_SOURCE_CIRCUIT_PATH'].endswith("
        "'/logs/whoscored/source-circuit-v1.json'); "
        f"print({payload!r})"
    )
    commands = [
        capacity.WorkerCommand(
            worker_id=worker_id,
            iteration=0,
            scope="INT-World Cup=2026",
            argv=(sys.executable, "-c", child_code),
        )
        for worker_id in range(capacity.WORKER_COUNT)
    ]
    outcomes = []

    capacity._run_subprocess_round(
        commands,
        deadline=time.monotonic() + 10,
        on_sample=lambda force: None,
        on_outcome=outcomes.append,
        should_stop=lambda: len(outcomes) >= 4,
        before_launch=lambda: None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )

    assert len(outcomes) == 4
    assert {outcome.worker_id for outcome in outcomes} == {0, 1, 2, 3}
    assert all(capacity._summarize_outcome(outcome)["status"] == "success" for outcome in outcomes)


def test_worker_control_is_absent_from_proc_cmdline_and_parent_fds_close(
    monkeypatch, tmp_path
):
    owner = "privateownerscope123456789"
    endpoint = capacity.REQUIRED_FLARESOLVERR_ENDPOINT
    ready_paths = [tmp_path / f"control-{index}.ready" for index in range(4)]
    commands = []
    for worker_id, ready_path in enumerate(ready_paths):
        child_code = (
            "import json,os,sys,time; from pathlib import Path; "
            "fd=int(sys.argv[sys.argv.index('--capacity-control-fd')+1]); "
            "document=json.loads(os.read(fd,512)); os.close(fd); "
            "assert document['schema_version']==1; "
            "host_pid=next(line for line in "
            "Path('/proc/self/status').read_text().splitlines() "
            "if line.startswith('NSpid:')).split()[1]; "
            f"Path({str(ready_path)!r}).write_text(host_pid); time.sleep(30)"
        )
        commands.append(
            capacity.WorkerCommand(
                worker_id=worker_id,
                iteration=0,
                scope="INT-World Cup=2026",
                argv=(sys.executable, "-c", child_code),
                browser_session_owner=owner,
                flaresolverr_endpoint=endpoint,
            )
        )

    real_popen = capacity.subprocess.Popen
    launched_pids = []

    def track_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        launched_pids.append(process.pid)
        return process

    real_control_pipe = capacity._capacity_control_pipe
    issued_control_fds = []

    def track_control_pipe(**kwargs):
        control_fd = real_control_pipe(**kwargs)
        issued_control_fds.append(control_fd)
        return control_fd

    monkeypatch.setattr(capacity, "_worker_exec_preflight", lambda: None)
    monkeypatch.setattr(capacity.subprocess, "Popen", track_popen)
    monkeypatch.setattr(capacity, "_capacity_control_pipe", track_control_pipe)
    cmdlines = []

    def sample(force):
        del force
        if not all(path.exists() for path in ready_paths) or cmdlines:
            return
        inner_pids = [int(path.read_text()) for path in ready_paths]
        for pid in [*launched_pids, *inner_pids]:
            cmdlines.append(Path(f"/proc/{pid}/cmdline").read_bytes())

    outcomes = []
    capacity._run_subprocess_round(
        commands,
        deadline=time.monotonic() + 10,
        on_sample=sample,
        on_outcome=outcomes.append,
        should_stop=lambda: all(path.exists() for path in ready_paths),
        before_launch=lambda: None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )

    assert len(cmdlines) == 8
    assert all(owner.encode() not in cmdline for cmdline in cmdlines)
    assert all(endpoint.encode() not in cmdline for cmdline in cmdlines)
    assert any(b"--capacity-control-fd" in cmdline for cmdline in cmdlines)
    assert owner not in json.dumps([outcome.__dict__ for outcome in outcomes])
    assert endpoint not in json.dumps([outcome.__dict__ for outcome in outcomes])
    for control_fd in issued_control_fds:
        with pytest.raises(OSError):
            os.fstat(control_fd)


def test_subprocess_round_checks_runtime_identity_before_any_popen(monkeypatch):
    commands = [
        capacity.WorkerCommand(
            worker_id=worker_id,
            iteration=0,
            scope="INT-World Cup=2026",
            argv=(sys.executable, "-c", "raise SystemExit(0)"),
        )
        for worker_id in range(capacity.WORKER_COUNT)
    ]
    popen_calls = []
    monkeypatch.setattr(capacity, "_worker_exec_preflight", lambda: None)
    monkeypatch.setattr(
        capacity.subprocess,
        "Popen",
        lambda *args, **kwargs: popen_calls.append((args, kwargs)),
    )

    with pytest.raises(RuntimeError, match="runtime identity changed"):
        capacity._run_subprocess_round(
            commands,
            deadline=time.monotonic() + 10,
            on_sample=lambda force: None,
            on_outcome=lambda outcome: None,
            should_stop=lambda: False,
            before_launch=lambda: (_ for _ in ()).throw(
                RuntimeError("runtime identity changed before worker launch")
            ),
            monotonic=time.monotonic,
            sleep=time.sleep,
        )

    assert popen_calls == []


def test_subprocess_supervisor_refills_fast_slot_without_waiting_for_slowest():
    payload = json.dumps(_workflow_report(page_units=1))
    commands = []
    for worker_id in range(capacity.WORKER_COUNT):
        delay = 0 if worker_id == 0 else 5
        commands.append(
            capacity.WorkerCommand(
                worker_id=worker_id,
                iteration=0,
                scope="INT-World Cup=2026",
                argv=(
                    sys.executable,
                    "-c",
                    f"import time; time.sleep({delay}); print({payload!r})",
                ),
            )
        )
    outcomes = []

    capacity._run_subprocess_round(
        commands,
        deadline=time.monotonic() + 10,
        on_sample=lambda force: None,
        on_outcome=outcomes.append,
        should_stop=lambda: sum(
            outcome.worker_id == 0 and outcome.termination_reason is None
            for outcome in outcomes
        )
        >= 2,
        before_launch=lambda: None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )

    fast_iterations = {
        outcome.iteration
        for outcome in outcomes
        if outcome.worker_id == 0 and outcome.termination_reason is None
    }
    assert {0, 1}.issubset(fast_iterations)
    assert any(outcome.termination_reason == "aborted_by_gate" for outcome in outcomes)


def test_sigterm_handler_sets_stop_callback_once_without_async_raise():
    observed = []
    previous = capacity._install_termination_handlers(True, observed.append)
    try:
        handler = capacity.signal.getsignal(capacity.signal.SIGTERM)
        handler(capacity.signal.SIGTERM, None)
        handler(capacity.signal.SIGHUP, None)
        assert observed == [capacity.signal.SIGTERM]
        assert capacity.signal.SIGINT in previous
    finally:
        capacity._restore_termination_handlers(previous)


@pytest.mark.parametrize("handled_signal", [signal.SIGTERM, signal.SIGINT])
def test_signal_flag_set_at_popen_return_registers_then_terminates_child(
    monkeypatch, handled_signal
):
    stop_requested = False
    launched_pids = []
    outcomes = []

    def record_signal(signum):
        nonlocal stop_requested
        assert signum == handled_signal
        stop_requested = True

    previous_handlers = capacity._install_termination_handlers(
        True, record_signal
    )
    monkeypatch.setattr(capacity, "_worker_exec_preflight", lambda: None)
    real_popen = capacity.subprocess.Popen

    def popen_then_signal(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        launched_pids.append(process.pid)
        handler = signal.getsignal(handled_signal)
        handler(handled_signal, None)
        return process

    monkeypatch.setattr(capacity.subprocess, "Popen", popen_then_signal)
    commands = [
        capacity.WorkerCommand(
            worker_id=worker_id,
            iteration=0,
            scope="INT-World Cup=2026",
            argv=(sys.executable, "-c", "import time; time.sleep(30)"),
        )
        for worker_id in range(capacity.WORKER_COUNT)
    ]
    try:
        capacity._run_subprocess_round(
            commands,
            deadline=time.monotonic() + 10,
            on_sample=lambda force: None,
            on_outcome=outcomes.append,
            should_stop=lambda: stop_requested,
            before_launch=lambda: None,
            monotonic=time.monotonic,
            sleep=time.sleep,
        )
    finally:
        capacity._restore_termination_handlers(previous_handlers)
        for pid in launched_pids:
            if not _pid_is_dead(pid):
                os.kill(pid, signal.SIGKILL)

    assert len(launched_pids) == 1
    assert len(outcomes) == 1
    assert outcomes[0].termination_reason == "aborted_by_gate"
    assert _pid_is_dead(launched_pids[0])


@pytest.mark.skipif(sys.platform != "linux", reason="Linux prctl contract")
def test_real_parent_sigkill_kills_pid_namespace_worker_subtree(tmp_path):
    worker_pid_path = tmp_path / "worker.pid"
    descendant_pid_path = tmp_path / "descendant.pid"
    descendant_code = (
        "import os,signal,time; from pathlib import Path; os.setsid(); "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "host_pid=next(line for line in "
        "Path('/proc/self/status').read_text().splitlines() "
        "if line.startswith('NSpid:')).split()[1]; "
        f"Path({str(descendant_pid_path)!r}).write_text(host_pid); "
        "time.sleep(60)"
    )
    worker_code = (
        "import subprocess,sys,time; from pathlib import Path; "
        f"subprocess.Popen([sys.executable,'-c',{descendant_code!r}]); "
        "host_pid=next(line for line in "
        "Path('/proc/self/status').read_text().splitlines() "
        "if line.startswith('NSpid:')).split()[1]; "
        f"Path({str(worker_pid_path)!r}).write_text(host_pid); "
        "time.sleep(60)"
    )
    parent_code = "\n".join(
        [
            "import os, subprocess, sys, time",
            "subprocess.Popen([",
            f"    {sys.executable!r},",
            f"    {str(capacity.WORKER_EXEC_SCRIPT)!r},",
            "    '--expected-parent-pid', str(os.getpid()), '--',",
            f"    {str(capacity.WORKER_NAMESPACE_EXECUTABLE)!r},",
            "    '--pid', '--fork', '--kill-child=SIGKILL', '--',",
            f"    {sys.executable!r}, '-c', {worker_code!r},",
            "])",
            "time.sleep(60)",
        ]
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_code],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    tracked_pids = []
    try:
        ready_deadline = time.monotonic() + 5
        while time.monotonic() < ready_deadline and not (
            worker_pid_path.exists() and descendant_pid_path.exists()
        ):
            time.sleep(0.05)
        assert worker_pid_path.exists() and descendant_pid_path.exists()
        tracked_pids = [
            int(worker_pid_path.read_text()),
            int(descendant_pid_path.read_text()),
        ]

        os.kill(parent.pid, signal.SIGKILL)
        parent.wait(timeout=5)
        dead_deadline = time.monotonic() + 5
        while time.monotonic() < dead_deadline and not all(
            _pid_is_dead(pid) for pid in tracked_pids
        ):
            time.sleep(0.05)

        assert all(_pid_is_dead(pid) for pid in tracked_pids)
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        for pid in tracked_pids:
            if not _pid_is_dead(pid):
                os.kill(pid, signal.SIGKILL)


def test_base_signal_unwind_terminates_every_detached_worker_group(tmp_path):
    pid_files = [tmp_path / f"worker-{worker_id}.pid" for worker_id in range(4)]
    commands = [
        capacity.WorkerCommand(
            worker_id=worker_id,
            iteration=0,
            scope="INT-World Cup=2026",
            argv=(
                sys.executable,
                "-c",
                    (
                        "import time; from pathlib import Path; "
                        "host_pid=next(line for line in "
                        "Path('/proc/self/status').read_text().splitlines() "
                        "if line.startswith('NSpid:')).split()[1]; "
                        f"Path({str(pid_file)!r}).write_text(host_pid); "
                    "time.sleep(30)"
                ),
            ),
        )
        for worker_id, pid_file in enumerate(pid_files)
    ]

    def interrupt_when_started(force):
        del force
        if all(path.exists() for path in pid_files):
            raise capacity._SupervisorTermination(capacity.signal.SIGTERM)

    with pytest.raises(capacity._SupervisorTermination):
        capacity._run_subprocess_round(
            commands,
            deadline=time.monotonic() + 10,
            on_sample=interrupt_when_started,
            on_outcome=lambda outcome: None,
            should_stop=lambda: False,
            before_launch=lambda: None,
            monotonic=time.monotonic,
            sleep=time.sleep,
        )

    pids = [int(path.read_text()) for path in pid_files]
    assert all(_pid_is_dead(pid) for pid in pids)


def test_stop_processes_kills_term_ignoring_descendant_after_leader_exit(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(capacity, "_TERMINATE_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(capacity, "_KILL_CONFIRM_SECONDS", 2.0)
    descendant_path = tmp_path / "descendant.pid"
    descendant_code = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"Path({str(descendant_path)!r}).write_text(str(os.getpid())); "
        "time.sleep(60)"
    )
    leader_code = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{descendant_code!r}]); "
        "time.sleep(60)"
    )
    stdout_handle = (tmp_path / "leader.stdout").open("w+")
    stderr_handle = (tmp_path / "leader.stderr").open("w+")
    process = subprocess.Popen(
        [sys.executable, "-c", leader_code],
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        start_new_session=True,
    )
    running = capacity._RunningProcess(
        command=capacity.WorkerCommand(0, 0, "scope", (sys.executable,)),
        process=process,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
        started_at=time.monotonic(),
    )
    descendant_pid = None
    try:
        deadline = time.monotonic() + 5
        descendant_text = ""
        while time.monotonic() < deadline:
            if descendant_path.exists():
                descendant_text = descendant_path.read_text().strip()
                if descendant_text.isdigit():
                    break
            time.sleep(0.05)
        assert descendant_text.isdigit()
        descendant_pid = int(descendant_text)

        capacity._stop_processes(
            [running], monotonic=time.monotonic, sleep=time.sleep
        )

        assert process.poll() is not None
        assert _pid_is_dead(descendant_pid)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        if descendant_pid is not None and not _pid_is_dead(descendant_pid):
            os.kill(descendant_pid, signal.SIGKILL)
        stdout_handle.close()
        stderr_handle.close()


def test_normal_leader_exit_cleans_old_group_before_slot_replacement(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(capacity, "_worker_exec_preflight", lambda: None)
    monkeypatch.setattr(
        capacity, "_worker_exec_argv", lambda argv: tuple(argv)
    )
    monkeypatch.setattr(capacity, "_TERMINATE_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(capacity, "_KILL_CONFIRM_SECONDS", 2.0)
    descendant_paths = [tmp_path / f"orphan-{index}.pid" for index in range(4)]
    report = json.dumps(_workflow_report(page_units=1))
    commands = []
    for worker_id, descendant_path in enumerate(descendant_paths):
        descendant_code = (
            "import os,signal,time; from pathlib import Path; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"Path({str(descendant_path)!r}).write_text(str(os.getpid())); "
            "time.sleep(60)"
        )
        leader_code = "\n".join(
            [
                "import subprocess,sys,time",
                "from pathlib import Path",
                f"subprocess.Popen([sys.executable,'-c',{descendant_code!r}])",
                "deadline=time.monotonic()+5",
                f"path=Path({str(descendant_path)!r})",
                "while not path.exists() and time.monotonic() < deadline:",
                "    time.sleep(0.01)",
                f"print({report!r})",
            ]
        )
        commands.append(
            capacity.WorkerCommand(
                worker_id,
                0,
                "INT-World Cup=2026",
                (sys.executable, "-c", leader_code),
            )
        )
    outcomes = []

    capacity._run_subprocess_round(
        commands,
        deadline=time.monotonic() + 10,
        on_sample=lambda force: None,
        on_outcome=outcomes.append,
        should_stop=lambda: len(outcomes) == 4,
        before_launch=lambda: None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )

    assert len(outcomes) == 4
    assert all(
        outcome.termination_reason == "orphan_process_group"
        for outcome in outcomes
    )
    assert all(
        _pid_is_dead(int(path.read_text())) for path in descendant_paths
    )


def test_gate_abort_allows_four_real_children_to_run_cleanup(tmp_path):
    ready_files = [tmp_path / f"worker-{worker_id}.ready" for worker_id in range(4)]
    cleanup_files = [
        tmp_path / f"worker-{worker_id}.cleanup" for worker_id in range(4)
    ]
    commands = []
    for worker_id, (ready_file, cleanup_file) in enumerate(
        zip(ready_files, cleanup_files)
    ):
        child_code = "\n".join(
            [
                "import importlib.util, signal, sys, time",
                "from pathlib import Path",
                (
                    "spec = importlib.util.spec_from_file_location("
                    f"'bench_whoscored_abort_{worker_id}', "
                    f"{str(capacity.WORKFLOW_SCRIPT)!r})"
                ),
                "module = importlib.util.module_from_spec(spec)",
                "sys.modules[spec.name] = module",
                "spec.loader.exec_module(module)",
                "previous = module._install_cli_termination_handlers()",
                "try:",
                f"    Path({str(ready_file)!r}).touch()",
                "    while True:",
                "        time.sleep(1)",
                "finally:",
                f"    Path({str(cleanup_file)!r}).touch()",
                "    module._restore_cli_termination_handlers(previous)",
            ]
        )
        commands.append(
            capacity.WorkerCommand(
                worker_id=worker_id,
                iteration=0,
                scope="INT-World Cup=2026",
                argv=(sys.executable, "-c", child_code),
            )
        )
    outcomes = []

    capacity._run_subprocess_round(
        commands,
        deadline=time.monotonic() + 30,
        on_sample=lambda force: None,
        on_outcome=outcomes.append,
        should_stop=lambda: all(path.exists() for path in ready_files),
        before_launch=lambda: None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )

    assert all(path.exists() for path in cleanup_files)
    assert len(outcomes) == capacity.WORKER_COUNT
    assert all(outcome.termination_reason == "aborted_by_gate" for outcome in outcomes)
    assert all(outcome.returncode == 128 + signal.SIGTERM for outcome in outcomes)


def test_default_cli_is_six_hours_four_workers_and_requires_container_evidence():
    args = capacity._parser().parse_args([])

    assert args.duration_seconds == 21_600
    assert capacity.WORKER_COUNT == 4
    assert capacity._container_values(args) == (
        "airflow-scheduler",
        "flaresolverr",
        "proxy_filter",
    )
    assert capacity._scope_values(args) == (
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    )
    assert args.flaresolverr_url == "http://127.0.0.1:8191"
    assert capacity._validate_args(args) is None


def test_host_cli_accepts_representative_scopes_and_all_runtime_containers():
    args = capacity._parser().parse_args(
        [
            "--scope",
            "INT-World Cup=2026",
            "--scope",
            "ENG-Premier League=2526",
            "--container",
            "airflow-scheduler",
            "--container",
            "flaresolverr",
            "--container",
            "proxy_filter",
        ]
    )

    assert capacity._scope_values(args) == (
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    )
    assert capacity._container_values(args) == (
        "airflow-scheduler",
        "flaresolverr",
        "proxy_filter",
    )
    commands = capacity._build_commands(
        args,
        0,
        browser_session_owner="a" * 24,
    )
    assert [command.scope for command in commands] == [
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    ]
    assert all("--browser-session-owner" not in command.argv for command in commands)
    assert all("--flaresolverr-url" not in command.argv for command in commands)
    assert all(command.browser_session_owner == "a" * 24 for command in commands)
    assert all(
        command.flaresolverr_endpoint == capacity.REQUIRED_FLARESOLVERR_ENDPOINT
        for command in commands
    )


def test_container_arguments_can_only_add_to_mandatory_runtime_set():
    args = capacity._parser().parse_args(["--container", "trino"])

    assert capacity._container_values(args) == (
        "airflow-scheduler",
        "flaresolverr",
        "proxy_filter",
        "trino",
    )


def test_nonfinite_duration_and_sampling_interval_are_rejected():
    assert "duration-seconds" in capacity._validate_args(
        _args(duration_seconds=float("inf"))
    )
    assert "sample-interval-seconds" in capacity._validate_args(
        _args(sample_interval_seconds=float("nan"))
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1:8191",
        "http://localhost:8191",
        "http://10.0.0.1:8191",
        "http://127.0.0.1:8192",
        "http://user:pass@127.0.0.1:8191",
        "http://127.0.0.1:8191/",
        "http://127.0.0.1:8191/v1",
        "http://127.0.0.1:8191?token=secret",
        "http://127.0.0.1:8191#fragment",
    ],
)
def test_flaresolverr_endpoint_must_be_exact_safe_loopback_origin(endpoint):
    assert capacity._validate_args(_args(flaresolverr_url=endpoint)) is not None


def test_evidence_file_is_atomic_create_once_and_mode_0600(tmp_path):
    target = tmp_path / "capacity.json"
    report = {"status": "success", "publishes": False}

    capacity._write_report(target, report)

    assert json.loads(target.read_text()) == report
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [target]
    with pytest.raises(FileExistsError):
        capacity._write_report(target, report)
    assert list(tmp_path.iterdir()) == [target]


def test_procfs_sampler_includes_current_process_rss():
    sample = capacity._sample_process_rss([os.getpid()])

    assert sample["process_count"] >= 1
    assert sample["rss_bytes"] > 0
    assert os.getpid() in sample["root_pids"]


def _fast_cleanup(monkeypatch, *, quiet=2.0, deadline=8.0, interval=1.0):
    monkeypatch.setattr(capacity, "_SESSION_QUIET_SECONDS", quiet)
    monkeypatch.setattr(
        capacity, "_SESSION_CLEANUP_DEADLINE_SECONDS", deadline
    )
    monkeypatch.setattr(capacity, "_SESSION_SCAN_INTERVAL_SECONDS", interval)
    monkeypatch.setattr(capacity, "_SESSION_CLEANUP_MAX_SCANS", 20)


def _successful_cleanup_result(*, required=True):
    result = capacity._empty_cleanup_result(required=required, verified=True)
    result["quiet_window_observed"] = True
    result["final_zero_scans"] = 2
    return result


def _successful_fresh_probe_result():
    result = capacity._empty_cleanup_result(required=True, verified=True)
    result["poll_attempts"] = 1
    result["successful_polls"] = 1
    result["zero_scans"] = 1
    return result


def _cleanup_snapshot(**overrides: Any) -> dict[str, Any]:
    snapshot = {
        "status": "ok",
        "version": capacity.REQUIRED_FLARESOLVERR_VERSION,
        "extension_sha256": capacity._current_flaresolverr_extension_sha256(),
        "active": 0,
        "pending_create": 0,
        "pending_destroy": 0,
        "failed_create": 0,
        "failed_destroy": 0,
        "failure_generation": 0,
        "cleanup_scheduled": True,
    }
    snapshot.update(overrides)
    return snapshot


def test_cleanup_client_is_direct_and_post_retries_are_disabled():
    client = capacity._fresh_session_api_client()
    try:
        assert client.trust_env is False
        assert client.adapters["http://"].max_retries.total == 0
        assert client.adapters["https://"].max_retries.total == 0
    finally:
        client.close()


def test_session_api_rejects_unknown_status_protocol_drift():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: _cleanup_snapshot(status="new-status"),
    )
    calls = []

    def post(*args, **kwargs):
        calls.append((args, kwargs))
        return response

    session = SimpleNamespace(post=post)

    with pytest.raises(capacity._SessionApiProtocolError):
        capacity._session_api_post(
            session,
            "http://127.0.0.1:8191",
            "a" * 24,
        )

    assert calls[0][1]["allow_redirects"] is False
    assert calls[0][0] == (
        "http://127.0.0.1:8191/v1/whoscored/capacity-sessions/cleanup",
    )
    assert calls[0][1]["json"] == {"owner": "a" * 24}
    assert calls[0][1]["timeout"] == 3.0


def test_session_api_rejects_redirect_without_following_it():
    response = SimpleNamespace(status_code=307)
    calls = []

    def post(*args, **kwargs):
        calls.append((args, kwargs))
        return response

    with pytest.raises(capacity._SessionApiProtocolError):
        capacity._session_api_post(
            SimpleNamespace(post=post),
            "http://127.0.0.1:8191",
            "a" * 24,
        )

    assert len(calls) == 1
    assert calls[0][1]["allow_redirects"] is False


@pytest.mark.parametrize(
    "body",
    [
        _cleanup_snapshot(version="3.4.5"),
        _cleanup_snapshot(extension_sha256="0" * 64),
        _cleanup_snapshot(extension_sha256=True),
        _cleanup_snapshot(active=True),
        _cleanup_snapshot(active=-1),
        _cleanup_snapshot(cleanup_scheduled=1),
        _cleanup_snapshot(cleanup_scheduled=False),
        {**_cleanup_snapshot(), "extra": 0},
        {key: value for key, value in _cleanup_snapshot().items() if key != "active"},
        {
            key: value
            for key, value in _cleanup_snapshot().items()
            if key != "extension_sha256"
        },
    ],
)
def test_session_api_rejects_non_exact_cleanup_schema(body):
    response = SimpleNamespace(status_code=200, json=lambda: body)
    with pytest.raises(capacity._SessionApiProtocolError):
        capacity._session_api_post(
            SimpleNamespace(post=lambda *args, **kwargs: response),
            "http://127.0.0.1:8191",
            "a" * 24,
        )


def test_owned_sweep_waits_for_pending_lifecycle_then_two_final_scans(
    monkeypatch,
):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=5.0)
    clock = FakeClock()
    responses = [
        _cleanup_snapshot(pending_create=1),
        _cleanup_snapshot(),
        _cleanup_snapshot(),
        _cleanup_snapshot(),
    ]
    monkeypatch.setattr(
        capacity,
        "_session_api_post",
        lambda client, url, owner: responses.pop(0),
    )
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["verified_zero"] is True
    assert result["quiet_window_observed"] is True
    assert result["pending_create_max"] == 1
    assert result["final_zero_scans"] == 2


def test_fresh_owner_probe_requires_one_exact_zero_ack(monkeypatch):
    snapshots = [_cleanup_snapshot(), _cleanup_snapshot(active=1)]
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )
    monkeypatch.setattr(
        capacity,
        "_session_api_post",
        lambda client, url, owner: snapshots.pop(0),
    )

    ready = capacity._probe_fresh_session_owner(
        flaresolverr_url=capacity.REQUIRED_FLARESOLVERR_ENDPOINT,
        owner="a" * 24,
    )
    busy = capacity._probe_fresh_session_owner(
        flaresolverr_url=capacity.REQUIRED_FLARESOLVERR_ENDPOINT,
        owner="b" * 24,
    )

    assert ready["verified_zero"] is True
    assert ready["poll_attempts"] == 1
    assert ready["successful_polls"] == 1
    assert busy["verified_zero"] is False
    assert busy["active_max"] == 1


def test_owned_sweep_fails_closed_on_persistent_api_failure(monkeypatch):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=3.0)
    clock = FakeClock()
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(capacity, "_fresh_session_api_client", lambda: client)

    def fail_poll(active_client, url, owner):
        raise RuntimeError("SENSITIVE_SENTINEL https://secret.invalid")

    monkeypatch.setattr(capacity, "_session_api_post", fail_poll)

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    encoded = json.dumps(result, sort_keys=True)
    assert result["verified_zero"] is False
    assert result["deadline_exhausted"] is True
    assert result["error_count"] > 0
    assert result["error_sha256"]
    assert "SENSITIVE_SENTINEL" not in encoded
    assert "secret.invalid" not in encoded


def test_transient_api_error_then_zero_snapshots_still_fails(monkeypatch):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=6.0)
    clock = FakeClock()
    calls = 0

    def poll(active_client, url, owner):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return _cleanup_snapshot()

    monkeypatch.setattr(capacity, "_session_api_post", poll)
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["verified_zero"] is False
    assert result["final_zero_scans"] == 2
    assert result["error_count"] == 1


def test_fresh_owner_failure_generation_or_failed_count_is_sticky(monkeypatch):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=6.0)
    clock = FakeClock()
    responses = [
        _cleanup_snapshot(failed_destroy=1, failure_generation=1),
        _cleanup_snapshot(failure_generation=1),
        _cleanup_snapshot(failure_generation=1),
        _cleanup_snapshot(failure_generation=1),
    ]
    monkeypatch.setattr(
        capacity,
        "_session_api_post",
        lambda client, url, owner: responses.pop(0),
    )
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["verified_zero"] is False
    assert result["failed_destroy_max"] == 1
    assert result["failure_generation_changed"] is True


def test_stale_owner_can_retry_old_failed_destroy_with_stable_generation(
    monkeypatch,
):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=6.0)
    clock = FakeClock()
    responses = [
        _cleanup_snapshot(failed_destroy=1, failure_generation=4),
        _cleanup_snapshot(failure_generation=4),
        _cleanup_snapshot(failure_generation=4),
        _cleanup_snapshot(failure_generation=4),
    ]
    monkeypatch.setattr(
        capacity,
        "_session_api_post",
        lambda client, url, owner: responses.pop(0),
    )
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        stale=True,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["verified_zero"] is True
    assert result["failed_destroy_max"] == 1
    assert result["failure_generation_initial"] == 4
    assert result["failure_generation_changed"] is False


def test_stale_owner_fails_on_new_failure_generation(monkeypatch):
    _fast_cleanup(monkeypatch, quiet=1.0, deadline=6.0)
    clock = FakeClock()
    responses = [
        _cleanup_snapshot(failed_destroy=1, failure_generation=4),
        _cleanup_snapshot(failure_generation=5),
        _cleanup_snapshot(failure_generation=5),
        _cleanup_snapshot(failure_generation=5),
    ]
    monkeypatch.setattr(
        capacity,
        "_session_api_post",
        lambda client, url, owner: responses.pop(0),
    )
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(close=lambda: None),
    )

    result = capacity._sweep_owned_browser_sessions(
        flaresolverr_url="http://127.0.0.1:8191",
        owner="a" * 24,
        stale=True,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["verified_zero"] is False
    assert result["failure_generation_changed"] is True


def test_owner_state_is_atomic_0600_and_removed_only_after_verified_cleanup(
    monkeypatch, tmp_path
):
    lock_path = tmp_path / "capacity.lock"
    state_path = tmp_path / "owner.json"
    monkeypatch.setattr(capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", lock_path)
    monkeypatch.setattr(capacity, "_DEFAULT_SESSION_OWNER_PATH", state_path)
    monkeypatch.setattr(capacity.secrets, "token_hex", lambda size: "a" * 24)
    monkeypatch.setattr(
        capacity,
        "_sweep_owned_browser_sessions",
        lambda **kwargs: _successful_cleanup_result(),
    )
    monkeypatch.setattr(
        capacity,
        "_probe_fresh_session_owner",
        lambda **kwargs: _successful_fresh_probe_result(),
    )

    lease = capacity._prepare_session_ownership(
        _args(), monotonic=time.monotonic, sleep=lambda seconds: None
    )
    try:
        state = json.loads(state_path.read_text())
        assert state == {
            "schema_version": capacity._OWNER_STATE_SCHEMA_VERSION,
            "owner": "a" * 24,
            "flaresolverr_endpoint": "http://127.0.0.1:8191",
        }
        assert state_path.stat().st_mode & 0o777 == 0o600
        assert not list(tmp_path.glob(".owner.json.*.tmp"))

        evidence = lease.finalize()

        assert evidence["final_verified_zero"] is True
        assert evidence["state_file_removed"] is True
        assert not state_path.exists()
    finally:
        lease.close()


def test_failed_final_cleanup_keeps_owner_state_for_next_preflight(
    monkeypatch, tmp_path
):
    state_path = tmp_path / "owner.json"
    monkeypatch.setattr(
        capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", tmp_path / "capacity.lock"
    )
    monkeypatch.setattr(capacity, "_DEFAULT_SESSION_OWNER_PATH", state_path)
    monkeypatch.setattr(capacity.secrets, "token_hex", lambda size: "a" * 24)
    monkeypatch.setattr(
        capacity,
        "_sweep_owned_browser_sessions",
        lambda **kwargs: capacity._empty_cleanup_result(
            required=True, verified=False
        ),
    )
    monkeypatch.setattr(
        capacity,
        "_probe_fresh_session_owner",
        lambda **kwargs: _successful_fresh_probe_result(),
    )

    lease = capacity._prepare_session_ownership(
        _args(), monotonic=time.monotonic, sleep=lambda seconds: None
    )
    try:
        evidence = lease.finalize()

        assert evidence["final_verified_zero"] is False
        assert evidence["state_file_removed"] is False
        assert state_path.exists()
    finally:
        lease.close()


def test_stale_owner_is_cleaned_before_new_owner_is_persisted(
    monkeypatch, tmp_path
):
    lock_path = tmp_path / "capacity.lock"
    state_path = tmp_path / "owner.json"
    monkeypatch.setattr(capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", lock_path)
    monkeypatch.setattr(capacity, "_DEFAULT_SESSION_OWNER_PATH", state_path)
    stale_owner = "b" * 24
    stale_endpoint = "http://127.0.0.1:8192"
    current_endpoint = "http://127.0.0.1:8191"
    capacity._write_owner_state(state_path, stale_owner, stale_endpoint)
    observed_sweeps = []

    def successful_sweep(**kwargs):
        observed_sweeps.append(
            (kwargs["owner"], kwargs["flaresolverr_url"], kwargs["stale"])
        )
        return _successful_cleanup_result()

    monkeypatch.setattr(capacity, "_sweep_owned_browser_sessions", successful_sweep)
    monkeypatch.setattr(
        capacity,
        "_probe_fresh_session_owner",
        lambda **kwargs: _successful_fresh_probe_result(),
    )
    monkeypatch.setattr(capacity.secrets, "token_hex", lambda size: "a" * 24)

    lease = capacity._prepare_session_ownership(
        _args(), monotonic=time.monotonic, sleep=lambda seconds: None
    )
    try:
        assert observed_sweeps == [(stale_owner, stale_endpoint, True)]
        state = json.loads(state_path.read_text())
        assert state["owner"] == "a" * 24
        assert state["flaresolverr_endpoint"] == current_endpoint
        lease.finalize()
        assert observed_sweeps == [
            (stale_owner, stale_endpoint, True),
            ("a" * 24, current_endpoint, False),
        ]
    finally:
        lease.close()


def test_stale_cleanup_failure_prevents_identity_and_worker_launch(
    monkeypatch, tmp_path
):
    state_path = tmp_path / "owner.json"
    monkeypatch.setattr(
        capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", tmp_path / "capacity.lock"
    )
    monkeypatch.setattr(capacity, "_DEFAULT_SESSION_OWNER_PATH", state_path)
    capacity._write_owner_state(
        state_path, "b" * 24, "http://127.0.0.1:8191"
    )
    monkeypatch.setattr(
        capacity,
        "_sweep_owned_browser_sessions",
        lambda **kwargs: capacity._empty_cleanup_result(
            required=True, verified=False
        ),
    )
    runtime = FakeCapacityRuntime()
    deps = replace(
        runtime.dependencies(),
        prepare_session_ownership=capacity._prepare_session_ownership,
    )

    code, report = capacity.run(_args(), dependencies=deps)

    assert code == 2
    assert report["status"] == "configuration_error"
    assert report["workers_launched"] == 0
    assert runtime.identity_calls == 0
    assert runtime.commands == []
    assert state_path.exists()
    assert report["session_cleanup"]["preflight_verified_zero"] is False


def test_stock_lifecycle_endpoint_never_launches_and_retains_fresh_state(
    monkeypatch, tmp_path
):
    state_path = tmp_path / "owner.json"
    monkeypatch.setattr(
        capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", tmp_path / "capacity.lock"
    )
    monkeypatch.setattr(capacity, "_DEFAULT_SESSION_OWNER_PATH", state_path)
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"status": "ok", "sessions": []},
    )
    monkeypatch.setattr(
        capacity,
        "_fresh_session_api_client",
        lambda: SimpleNamespace(
            post=lambda *args, **kwargs: response,
            close=lambda: None,
        ),
    )
    runtime = FakeCapacityRuntime()
    dependencies = replace(
        runtime.dependencies(),
        prepare_session_ownership=capacity._prepare_session_ownership,
    )

    code, report = capacity.run(_args(), dependencies=dependencies)

    assert code == 2
    assert report["status"] == "configuration_error"
    assert runtime.identity_calls == 0
    assert runtime.commands == []
    assert state_path.exists()
    assert report["session_cleanup"]["preflight_verified_zero"] is False
    encoded = json.dumps(report, sort_keys=True)
    assert "sessions" not in encoded
    assert json.loads(state_path.read_text())["owner"] not in encoded


def test_nonblocking_host_lock_refuses_second_supervisor_before_workers(
    monkeypatch, tmp_path
):
    lock_path = tmp_path / "capacity.lock"
    monkeypatch.setattr(capacity, "_DEFAULT_SUPERVISOR_LOCK_PATH", lock_path)
    monkeypatch.setattr(
        capacity, "_DEFAULT_SESSION_OWNER_PATH", tmp_path / "owner.json"
    )
    held_descriptor = capacity._acquire_supervisor_lock(lock_path)
    runtime = FakeCapacityRuntime()
    deps = replace(
        runtime.dependencies(),
        prepare_session_ownership=capacity._prepare_session_ownership,
    )
    try:
        code, report = capacity.run(_args(), dependencies=deps)
    finally:
        os.close(held_descriptor)

    assert code == 2
    assert report["status"] == "configuration_error"
    assert report["workers_launched"] == 0
    assert report["session_cleanup"]["lock_acquired"] is False
    assert runtime.identity_calls == 0
    assert runtime.commands == []


def test_owner_and_flaresolverr_url_never_appear_in_report():
    runtime = FakeCapacityRuntime()
    owner = "secretownerscope1234567890"
    preflight = capacity._empty_cleanup_result(required=False, verified=True)
    evidence = capacity._ownership_evidence(
        lock_acquired=True,
        preflight=preflight,
        final=capacity._empty_cleanup_result(required=True, verified=True),
        state_file_removed=True,
    )

    def prepare(args, *, monotonic, sleep):
        return capacity._SessionOwnershipLease(
            owner=owner,
            preflight=preflight,
            finalize_callback=lambda: evidence,
            close_callback=lambda: None,
        )

    deps = replace(runtime.dependencies(), prepare_session_ownership=prepare)
    private_url = capacity.REQUIRED_FLARESOLVERR_ENDPOINT

    code, report = capacity.run(
        _args(flaresolverr_url=private_url), dependencies=deps
    )

    encoded = json.dumps(report, sort_keys=True)
    assert code == 0
    assert owner not in encoded
    assert private_url not in encoded
    assert "ws-cap-" not in encoded
    assert _gate(report, "browser_session_cleanup")["passed"] is True


def test_mixed_sigterm_and_sigkill_outcomes_still_require_and_pass_cleanup_gate():
    runtime = FakeCapacityRuntime()
    cleanup_called = []
    preflight = capacity._empty_cleanup_result(required=False, verified=True)
    final = capacity._empty_cleanup_result(required=True, verified=True)
    final["poll_attempts"] = 3
    final["final_zero_scans"] = 2
    evidence = capacity._ownership_evidence(
        lock_acquired=True,
        preflight=preflight,
        final=final,
        state_file_removed=True,
    )

    def prepare(args, *, monotonic, sleep):
        def finalize():
            cleanup_called.append(True)
            return evidence

        return capacity._SessionOwnershipLease(
            owner="a" * 24,
            preflight=preflight,
            finalize_callback=finalize,
            close_callback=lambda: None,
        )

    def killed_round(
        commands,
        *,
        deadline,
        on_sample,
        on_outcome,
        should_stop,
        before_launch,
        monotonic,
        sleep,
    ):
        del on_sample, should_stop, monotonic, sleep
        for command, returncode in zip(commands, (143, -9, -9, -9)):
            before_launch()
            on_outcome(
                capacity.WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=returncode,
                    report=None,
                    elapsed_seconds=10.0,
                    stderr_bytes=0,
                    stderr_sha256=hashlib.sha256(b"").hexdigest(),
                    termination_reason="deadline_terminated",
                )
            )
        runtime.clock.value = deadline

    deps = replace(
        runtime.dependencies(),
        run_round=killed_round,
        prepare_session_ownership=prepare,
    )

    code, report = capacity.run(_args(), dependencies=deps)

    assert code == 1
    assert cleanup_called == [True]
    cleanup_gate = _gate(report, "browser_session_cleanup")
    assert cleanup_gate["passed"] is True
    assert cleanup_gate["final_verified_zero"] is True
    assert [run["returncode"] for run in report["runs"]] == [143, -9, -9, -9]


def test_term_ignoring_workers_are_killed_before_session_cleanup(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(capacity, "_TERMINATE_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(capacity, "_KILL_CONFIRM_SECONDS", 2.0)
    runtime = FakeCapacityRuntime()
    pid_paths = [tmp_path / f"ignore-{worker_id}.pid" for worker_id in range(4)]
    cleanup_observations = []
    preflight = capacity._empty_cleanup_result(required=False, verified=True)
    evidence = capacity._ownership_evidence(
        lock_acquired=True,
        preflight=preflight,
        final=_successful_cleanup_result(),
        state_file_removed=True,
    )

    def prepare(args, *, monotonic, sleep):
        del args, monotonic, sleep

        def finalize():
            pids = [int(path.read_text()) for path in pid_paths]
            cleanup_observations.append(all(_pid_is_dead(pid) for pid in pids))
            return evidence

        return capacity._SessionOwnershipLease(
            owner="a" * 24,
            preflight=preflight,
            finalize_callback=finalize,
            close_callback=lambda: None,
        )

    def real_ignoring_round(
        commands,
        *,
        deadline,
        on_sample,
        on_outcome,
        should_stop,
        before_launch,
        monotonic,
        sleep,
    ):
        ignoring_commands = []
        for command, pid_path in zip(commands, pid_paths):
            child_code = (
                "import signal,time; from pathlib import Path; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "host_pid=next(line for line in "
                "Path('/proc/self/status').read_text().splitlines() "
                "if line.startswith('NSpid:')).split()[1]; "
                f"Path({str(pid_path)!r}).write_text(host_pid); "
                "time.sleep(60)"
            )
            ignoring_commands.append(
                replace(command, argv=(sys.executable, "-c", child_code))
            )
        capacity._run_subprocess_round(
            ignoring_commands,
            deadline=deadline,
            on_sample=on_sample,
            on_outcome=on_outcome,
            should_stop=lambda: should_stop()
            or all(path.exists() for path in pid_paths),
            before_launch=before_launch,
            monotonic=monotonic,
            sleep=sleep,
        )

    dependencies = capacity.CapacityDependencies(
        monotonic=time.monotonic,
        sleep=time.sleep,
        inspect_containers=runtime.inspect_containers,
        sample_rss=runtime.sample_rss,
        runtime_identity=runtime.runtime_identity,
        run_round=real_ignoring_round,
        prepare_session_ownership=prepare,
    )

    code, report = capacity.run(
        _args(duration_seconds=5.0, sample_interval_seconds=0.1),
        dependencies=dependencies,
    )

    assert code == 1
    assert cleanup_observations == [True]
    assert all(_pid_is_dead(int(path.read_text())) for path in pid_paths)
    assert [run["returncode"] for run in report["runs"]] == [-9, -9, -9, -9]
    assert _gate(report, "browser_session_cleanup")["passed"] is True


def test_unverified_worker_death_skips_api_cleanup_and_retains_state():
    runtime = FakeCapacityRuntime()
    cleanup_called = []
    preflight = capacity._empty_cleanup_result(required=False, verified=True)

    def prepare(args, *, monotonic, sleep):
        del args, monotonic, sleep
        return capacity._SessionOwnershipLease(
            owner="a" * 24,
            preflight=preflight,
            finalize_callback=lambda: cleanup_called.append(True),
            close_callback=lambda: None,
        )

    def unverified_round(*args, **kwargs):
        del args, kwargs
        raise capacity._WorkerTerminationUnverified("still alive")

    dependencies = replace(
        runtime.dependencies(),
        run_round=unverified_round,
        prepare_session_ownership=prepare,
    )

    code, report = capacity.run(_args(), dependencies=dependencies)

    cleanup_gate = _gate(report, "browser_session_cleanup")
    assert code == 1
    assert cleanup_called == []
    assert cleanup_gate["passed"] is False
    assert cleanup_gate["final_verified_zero"] is False
    assert cleanup_gate["state_file_removed"] is False


def test_post_cleanup_sample_detects_container_change_before_lock_release():
    runtime = FakeCapacityRuntime()
    cleanup_finished = False
    lock_released_after_post_sample = False
    preflight = capacity._empty_cleanup_result(required=False, verified=True)
    evidence = capacity._ownership_evidence(
        lock_acquired=True,
        preflight=preflight,
        final=capacity._empty_cleanup_result(required=True, verified=True),
        state_file_removed=True,
    )

    def inspect(names):
        return {
            name: _container_state(
                name, restart_count=int(cleanup_finished)
            )
            for name in names
        }

    def prepare(args, *, monotonic, sleep):
        def finalize():
            nonlocal cleanup_finished
            cleanup_finished = True
            return evidence

        def close():
            nonlocal lock_released_after_post_sample
            lock_released_after_post_sample = inspect_calls >= 4

        return capacity._SessionOwnershipLease(
            owner="a" * 24,
            preflight=preflight,
            finalize_callback=finalize,
            close_callback=close,
        )

    inspect_calls = 0

    def counted_inspect(names):
        nonlocal inspect_calls
        inspect_calls += 1
        return inspect(names)

    deps = replace(
        runtime.dependencies(),
        inspect_containers=counted_inspect,
        prepare_session_ownership=prepare,
    )

    code, report = capacity.run(_args(), dependencies=deps)

    assert code == 1
    assert lock_released_after_post_sample is True
    assert inspect_calls >= 4
    assert _gate(report, "container_restart_oom")["passed"] is False


def test_deferred_cleanup_handler_records_only_first_signal(monkeypatch):
    callbacks = []
    installed = {}
    monkeypatch.setattr(
        capacity.signal,
        "signal",
        lambda signum, handler: installed.__setitem__(signum, handler),
    )

    capacity._install_deferred_termination_handlers(True, callbacks.append)
    installed[signal.SIGTERM](signal.SIGTERM, None)
    installed[signal.SIGHUP](signal.SIGHUP, None)

    assert callbacks == [signal.SIGTERM]
    assert signal.SIGINT in installed


def test_real_run_restores_host_signal_handlers_before_report_build(monkeypatch):
    runtime = FakeCapacityRuntime()
    previous_term = signal.getsignal(signal.SIGTERM)
    previous_hup = signal.getsignal(signal.SIGHUP)
    previous_int = signal.getsignal(signal.SIGINT)
    report_build_observations = []

    def json_safe_document(report):
        report_build_observations.append(
            (
                signal.getsignal(signal.SIGTERM) is previous_term,
                signal.getsignal(signal.SIGHUP) is previous_hup,
                signal.getsignal(signal.SIGINT) is previous_int,
            )
        )
        return json.loads(json.dumps(report, sort_keys=True, default=str))

    monkeypatch.setattr(capacity, "_workflow_runtime_preflight", lambda: None)
    monkeypatch.setattr(capacity, "_default_dependencies", runtime.dependencies)
    monkeypatch.setattr(capacity, "_json_safe_document", json_safe_document)

    code, report = capacity.run(_args())

    assert code == 0
    assert report["status"] == "success"
    assert report_build_observations == [(True, True, True)]
    assert signal.getsignal(signal.SIGTERM) is previous_term
    assert signal.getsignal(signal.SIGHUP) is previous_hup
    assert signal.getsignal(signal.SIGINT) is previous_int


def test_procfs_sampler_fails_when_a_required_root_is_not_visible():
    with pytest.raises(RuntimeError, match="required RSS roots"):
        capacity._sample_process_rss([os.getpid(), 2_147_483_647])


def test_runtime_identity_covers_canary_parser_transport_and_container_helpers():
    identity = capacity._runtime_identity(capacity._parser().parse_args([]))

    assert len(identity["git_revision"]) == 40
    assert len(identity["manifest_sha256"]) == 64
    assert identity["python_executable"] == sys.executable
    assert identity["python_prefix"] == sys.prefix
    assert identity["dependency_versions"] == {
        "curl_cffi": capacity._installed_curl_cffi_version()
    }
    assert {
        "compose.yaml",
        "scripts/research/bench_whoscored_capacity.py",
        "scripts/research/bench_whoscored_workflow.py",
        "scripts/research/whoscored_capacity_worker_exec.py",
        "scripts/flaresolverr_extended.py",
        "scripts/proxy_filter/filter_proxy.py",
        "docker/images/airflow/requirements-scraping.txt",
        "scrapers/utils/rate_limiter.py",
        "scrapers/whoscored/parsers.py",
        "scrapers/whoscored/repository.py",
        "scrapers/whoscored/service.py",
        "scrapers/whoscored/transport.py",
        "scrapers/whoscored/raw_store.py",
        "scrapers/whoscored/runtime_contract.py",
        "configs/medallion/competitions.yaml",
    }.issubset(identity["file_sha256"])
    assert (
        identity["file_sha256"]["external:unshare"]
        == capacity.REQUIRED_UNSHARE_SHA256
    )


def test_runtime_manifest_binds_unresolved_venv_and_curl_cffi_version(
    monkeypatch, tmp_path
):
    runtime_file = tmp_path / "runtime.py"
    runtime_file.write_text("VALUE = 1\n")
    monkeypatch.setattr(capacity, "_runtime_files", lambda args: (runtime_file,))

    def fake_git(argv, **kwargs):
        del kwargs
        stdout = "a" * 40 + "\n" if argv[1:3] == ["rev-parse", "HEAD"] else ""
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(capacity.subprocess, "run", fake_git)
    real_python = tmp_path / "python-real"
    real_python.write_text("python\n")
    venv = tmp_path / "venv"
    executable = venv / "bin" / "python"
    executable.parent.mkdir(parents=True)
    executable.symlink_to(real_python)
    monkeypatch.setattr(capacity.sys, "executable", str(executable))
    monkeypatch.setattr(capacity.sys, "prefix", str(venv))
    installed_version = "0.15.0"
    monkeypatch.setattr(
        capacity, "_installed_curl_cffi_version", lambda: installed_version
    )
    args = capacity._parser().parse_args([])

    before = capacity._runtime_identity(args)
    installed_version = "0.15.1"
    after = capacity._runtime_identity(args)

    assert before["python_executable"] == str(executable)
    assert before["python_executable"] != str(executable.resolve())
    assert before["python_prefix"] == str(venv)
    assert before["dependency_versions"] == {"curl_cffi": "0.15.0"}
    assert after["dependency_versions"] == {"curl_cffi": "0.15.1"}
    assert before["manifest_sha256"] != after["manifest_sha256"]


def test_runtime_identity_changes_when_rate_limiter_input_changes(
    monkeypatch, tmp_path
):
    runtime_file = tmp_path / "rate_limiter.py"
    runtime_file.write_text("RATE = 30\n")
    monkeypatch.setattr(
        capacity, "_runtime_files", lambda args: (runtime_file,)
    )

    def fake_git(argv, **kwargs):
        del kwargs
        stdout = "a" * 40 + "\n" if argv[1:3] == ["rev-parse", "HEAD"] else ""
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(capacity.subprocess, "run", fake_git)
    args = capacity._parser().parse_args([])

    before = capacity._runtime_identity(args)
    runtime_file.write_text("RATE = 60\n")
    after = capacity._runtime_identity(args)

    assert before["manifest_sha256"] != after["manifest_sha256"]


def test_main_prints_one_json_document(monkeypatch, capsys):
    expected = {"status": "failed", "publishes": False}
    monkeypatch.setattr(
        capacity,
        "_parser",
        lambda: SimpleNamespace(parse_args=lambda: _args()),
    )
    monkeypatch.setattr(capacity, "run", lambda args: (1, expected))

    assert capacity.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected
    assert captured.out.count("\n") == 1
