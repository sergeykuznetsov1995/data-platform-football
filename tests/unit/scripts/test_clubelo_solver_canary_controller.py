"""Offline controller contracts. Every process, clock and health probe is fake."""

import copy
import json
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

from scripts.research import clubelo_solver_canary as h
from scripts.research import clubelo_canary_solver_instrumentation as obs
from scripts.research import clubelo_solver_canary_controller as c


SERVICE_IDS = {
    "CLUBELO_CANARY_GATEWAY_UID": "50000", "CLUBELO_CANARY_GATEWAY_GID": "0",
    "CLUBELO_CANARY_SOLVER_UID": "1000", "CLUBELO_CANARY_SOLVER_GID": "1000",
    "CLUBELO_CANARY_RUNNER_UID": "50000", "CLUBELO_CANARY_RUNNER_GID": "0",
}


def write(path, value):
    path = Path(path)
    path.write_text(json.dumps(value) if not isinstance(value, str) else value)
    path.chmod(0o600)
    return path


def private(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    return str(path)


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 60
        self.sleeps.append(seconds)
        self.now += seconds


class FakeDocker:
    """An exact argv allowlist; unsupported commands fail the test immediately."""

    def __init__(self, plan, clock):
        self.p, self.clock = plan, clock
        self.calls = []
        self.runs = []
        self.gateway_health = dict(healthy=True, daily_budget_bytes=2147483648,
                                   daily_total_bytes=0, max_lease_bytes=33554432,
                                   max_active_leases=1, source_mode="shared-no-whoscored")
        self.gateway_readbacks = []
        self.mutate_report = None
        self.after_run = None
        self.fail_candidates = False
        self.crash = False
        self.networks = {}
        self.resolved = {name: "sha256:" + digit * 64
                         for name, digit in zip(("control", "candidate", "gateway", "runner"), "abcd")}
        self.repo_digests = {name: [reference] for name, reference in plan["images"].items()}
        self.repo_digests["runner"] = ["apache/airflow@" + plan["images"]["runner"].split("@", 1)[1]]
        self.rows = {}
        for name, identity in plan["shared"].items():
            self.rows[identity] = self.row(identity, name, project="production", image="sha256:" + "1" * 64)
            self.rows[identity]["networks"] = {"production_internal": {"NetworkID": "9" * 64}}
        self.serial = 100

    def row(self, identity, service, *, project=None, image=None):
        return dict(id=identity, image=image, status="running", oom=False, restarts=0,
                    project=project or self.p["project"], service=service,
                    config_hash=c.sha(service.encode()), networks={}, mounts=[])

    def own(self):
        return [row for row in self.rows.values() if row["project"] == self.p["project"]]

    def __call__(self, argv, *, env, output=None, timeout=60):
        self.calls.append(list(argv))
        assert env["PATH"] == "/usr/local/bin:/usr/bin:/bin"
        assert {key: value for key, value in env.items()
                if key.startswith("CLUBELO_CANARY_") and key.endswith(("_UID", "_GID"))} == SERVICE_IDS
        assert not {"CLUBELO_CANARY_UID", "CLUBELO_CANARY_GID"} & env.keys()
        assert "PROXY_POOL_JSON" not in env and "CLUBELO_PROXY_CONTROL_TOKEN" not in env
        assert not any("BUDGET" in key or "CDP" in key or "ARMED_REVISION" in key for key in env)
        if argv[:3] == ["docker", "image", "inspect"]:
            assert argv[3:5] == ["--format", c.IMAGE_FORMAT] and len(argv) == 6
            name = next(name for name, ref in self.p["images"].items() if ref == argv[5])
            return json.dumps(dict(id=self.resolved[name], digests=self.repo_digests[name]))
        if argv[:2] == ["docker", "inspect"]:
            assert argv[2:6] == ["--type", "container", "--format", c.CONTAINER_FORMAT] and len(argv) == 7
            return json.dumps(self.rows[argv[6]])
        if argv[:2] == ["docker", "ps"]:
            assert argv[:5] == ["docker", "ps", "--all", "--quiet", "--no-trunc"]
            assert len(argv) == 5 or argv[5:] == ["--filter", "label=com.docker.compose.project=" + self.p["project"]]
            rows = self.rows.values() if len(argv) == 5 else self.own()
            return "\n".join(r["id"] for r in rows)
        if argv[:3] == ["docker", "network", "ls"]:
            assert argv[3:] == ["--quiet", "--no-trunc", "--filter", "label=com.docker.compose.project=" + self.p["project"]]
            return "\n".join(self.networks)
        if argv[:3] == ["docker", "network", "inspect"]:
            assert argv[3:5] == ["--format", c.NETWORK_FORMAT] and len(argv) == 6
            identity = argv[5]
            return json.dumps(dict(id=identity, internal=self.networks[identity], project=self.p["project"],
                                   members={r["id"]: {} for r in self.own()
                                            if identity in [n["NetworkID"] for n in r["networks"].values()]}))
        if argv[:3] == ["docker", "network", "rm"]:
            assert len(argv) == 4 and not self.own()
            del self.networks[argv[3]]
            return ""
        if argv[:2] == ["docker", "exec"]:
            row = self.rows[argv[2]]
            assert row["project"] == self.p["project"] and argv[3:5] == ["python", "-c"]
            if argv[5:] == [c.SOLVER_HEALTH]:
                assert row["service"].startswith("solver-")
                return '{"solver":"3.4.6","chrome":"142.0.7444.175"}'
            assert argv[5:] == [c.GATEWAY_HEALTH, self.p["gateway_health_path"]]
            assert row["service"] == "clubelo-canary-gateway"
            raw = copy.deepcopy(self.gateway_health)
            if "healthy" in raw:
                health = raw.pop("healthy")
                raw["status"] = "ok" if health is True else health
            normalized = dict(healthy=raw.get("status") == "ok",
                              **{key: raw.get(key) for key in (
                                  "daily_budget_bytes", "daily_total_bytes", "max_lease_bytes",
                                  "max_active_leases", "source_mode")})
            self.gateway_readbacks.append(copy.deepcopy(normalized))
            return json.dumps(normalized)
        if argv[:2] == ["docker", "stop"]:
            assert argv[2:4] == ["--time", "30"] and len(argv) == 5
            assert self.rows[argv[4]]["project"] == self.p["project"]
            self.rows[argv[4]]["status"] = "exited"
            return ""
        if argv[:2] == ["docker", "rm"]:
            assert len(argv) == 3 and self.rows[argv[2]]["project"] == self.p["project"]
            assert self.rows[argv[2]]["status"] == "exited"
            del self.rows[argv[2]]
            return ""
        assert argv[:8] == ["docker", "compose", "--project-name", self.p["project"],
                             "--file", self.p["compose"], "--env-file", self.env_file]
        args = argv[8:]
        if args[0] == "up":
            assert args[:3] == ["up", "-d", "--no-deps"]
            assert args[3:] in (["clubelo-canary-gateway", "solver-control"],
                                ["clubelo-canary-gateway", "solver-candidate"])
            self.networks = {"e" * 64: True, "f" * 64: False}
            for service in args[3:]:
                if any(r["service"] == service for r in self.own()):
                    continue
                self.serial += 1
                identity = format(self.serial, "064x")
                key = service.removeprefix("solver-") if service.startswith("solver-") else "gateway"
                row = self.row(identity, service, image=self.resolved[key])
                row["networks"] = {self.p["project"] + "_internal": {"NetworkID": "e" * 64}}
                if key == "gateway":
                    row["networks"][self.p["project"] + "_egress"] = {"NetworkID": "f" * 64}
                self.rows[identity] = row
            return ""
        if args[0] == "stop":
            assert args == ["stop", "--timeout", "30", "solver-control", "solver-candidate"]
            for row in self.own():
                if row["service"].startswith("solver-"):
                    row["status"] = "exited"
            return ""
        if args[0] == "rm":
            assert args == ["rm", "-f", "solver-control", "solver-candidate"]
            for row in list(self.own()):
                if row["service"].startswith("solver-"):
                    assert row["status"] == "exited"
                    del self.rows[row["id"]]
            return ""
        assert args == ["run", "--rm", "--no-deps", "-e", "CLUBELO_PROXY_MAX_BYTES=33554432", "runner"]
        assert timeout == c.RUN_SECONDS and output == str(Path(env["CLUBELO_CANARY_OUTPUT_DIR"]) / "process.raw")
        self.runs.append((env["CLUBELO_CANARY_ARM"], self.clock()))
        write(output, "private synthetic process output\n")
        if self.crash:
            raise KeyboardInterrupt()
        report = self.emit_report(env)
        if self.after_run:
            self.after_run(self, report)
        return 0 if report["page_validated"] else 1

    def emit_report(self, env):
        arm, run_id = env["CLUBELO_CANARY_ARM"], env["CLUBELO_CANARY_RUN_ID"]
        other = next(side for side in c.ARMS if side != arm)
        slot = next(slot for slot in self.p["slots"] if slot["output"] == env["CLUBELO_CANARY_OUTPUT_DIR"])
        assert c.read_json(Path(slot["observers"][other]) / obs.ACTIVE_FILE)["active"] is False
        active = c.read_json(Path(slot["observers"][arm]) / obs.ACTIVE_FILE)
        obs._active(active, arm, self.clock())
        start = self.clock()
        accepted = not (arm == "candidate" and self.fail_candidates)
        attempts, events, leases = [], [], []
        for ordinal in (1, 2):
            solved = ordinal == 2 and accepted
            identity = run_id + "-lease-" + str(ordinal)
            begin = start + ordinal * 2 - 1
            attempts.append(dict(schema=1, event=h.PREFIX, attempt=ordinal,
                                 outcome="clearance_accepted" if solved else "solver_timeout",
                                 retry=not solved, lease_id=identity, upstream_fingerprint="a" * 16,
                                 latency_ms=1000, failed_validation_predicate=None, run_id=run_id,
                                 started_epoch=begin, finished_epoch=begin + 1,
                                 detection="detected", solved=solved, validated=solved))
            events.append(dict(schema=obs.EVENT_SCHEMA, sequence=len(events) + 1, run_id=run_id,
                               arm=arm, kind="detected", observed_epoch=begin + 0.1))
            if solved:
                events.append(dict(schema=obs.EVENT_SCHEMA, sequence=len(events) + 1, run_id=run_id,
                                   arm=arm, kind="solved", observed_epoch=begin + 0.2))
            leases.append(dict(lease_id=identity, acquisition="acquired", constructed=True,
                               close_attempted=True, close_failed=False, accounting_complete=True,
                               close_proof=dict(close_complete=True, closed=True, accounting_uncertain=False,
                                                active_tunnels=0, reserved_bytes=0, total_bytes=100)))
        write(Path(slot["observers"][arm]) / obs.EVENT_FILE,
              "".join(json.dumps(event) + "\n" for event in events))
        report = dict(schema=h.SCHEMA, run_id=run_id, arm=arm, started_epoch=start, finished_epoch=start + 5,
                      actual_start_utc=h.utc(start), actual_end_utc=h.utc(start + 5), attempts=attempts,
                      detection=dict(detected=2, solved=int(accepted), unknown=0),
                      page_outcome="acceptable_200" if accepted else "blocked", page_validated=accepted,
                      validated_clearance=accepted, no_challenge=False, wire_bytes=200,
                      accounting=dict(complete=True, total_bytes=200, leases=leases),
                      invalid_solutions_forwarded=0, complete=True, errors=[])
        if self.mutate_report:
            self.mutate_report(report)
        target = Path(slot["output"]) / "report.json"
        h.claim_output(target)
        h.atomic_report(target, report)
        self.clock.now += 10
        return report


@pytest.fixture
def rig(tmp_path):
    # pytest's own ancestors are private; all provisioned leaves are private too.
    tmp_path.chmod(0o700)
    directories = {name: private(tmp_path / name) for name in ("state", "code", "release", "gateway", "deploy", "proofs", "secrets")}
    code = Path(directories["code"])
    private(code / "scripts")
    private(code / "scripts" / "research")
    for name in ("clubelo_solver_canary.py", "clubelo_canary_solver_instrumentation.py"):
        write(code / "scripts" / "research" / name, "# synthetic reviewed artifact\n")
    write(Path(directories["release"]) / "gateway.py", "# synthetic gateway\n")
    compose = write(Path(directories["deploy"]) / "solver_canary.compose.yaml", "# frozen fixture\n")
    dag = write(Path(directories["deploy"]) / "solver_canary_dag.py", "# frozen fixture DAG\n")
    slots = [dict(output=private(tmp_path / ("out-" + str(n))),
                  observers={arm: private(tmp_path / ("obs-" + str(n) + "-" + arm)) for arm in c.ARMS})
             for n in range(120)]
    plan = dict(schema=c.PLAN_SCHEMA, experiment="test2251", project="clubelo-canary-test2251",
                state_dir=directories["state"], compose=str(compose), dag=str(dag),
                code_dir=directories["code"], release_dir=directories["release"], gateway_state_dir=directories["gateway"],
                preflight_dirs={arm: private(tmp_path / ("preflight-" + arm)) for arm in c.ARMS},
                slots=slots, local_evidence=str(Path(directories["proofs"]) / "local.json"),
                traffic_evidence=str(Path(directories["proofs"]) / "traffic.json"),
                images={**{name: "test/" + name + "@sha256:" + digit * 64
                           for name, digit in zip(("control", "candidate", "gateway"), "abc")}, "runner": c.RUNNER_IMAGE},
                hashes={"compose": c.sha(compose.read_bytes()), "dag": c.sha(dag.read_bytes())},
                commit="a" * 40, diff_sha256="b" * 64,
                shared={name: str(n) * 64 for n, name in enumerate(sorted(c.SHARED), 1)},
                gateway_health_path="/health", daily_lease_cap=c.LEASE_BYTES, deadline_epoch=1000 + c.MAX_SECONDS)
    for prefix in ("code", "release"):
        root = Path(directories[prefix])
        for file in root.rglob("*"):
            if file.is_file():
                plan["hashes"][prefix + "/" + file.relative_to(root).as_posix()] = c.sha(file.read_bytes())
    clock = Clock()
    docker = FakeDocker(plan, clock)
    local = dict(schema=c.LOCAL_SCHEMA, status="PASS", local_only=True, cleanup_complete=True, reviewed=True,
                 images={arm: docker.resolved[arm] for arm in c.ARMS}, code_hashes=plan["hashes"],
                 airflow_smoke=dict(status="PASS", real_airflow=True, runner_image=docker.resolved["runner"],
                                    evidence_sha256=c.sha(b"synthetic real Airflow evidence fixture")),
                 focused_unit=dict(status="PASS", passed=12, failed=0, skipped=0,
                                   evidence_sha256=c.sha(b"synthetic focused unit evidence fixture")))
    write(plan["local_evidence"], local)
    env_file = write(Path(directories["secrets"]) / "gateway.env", "DO_NOT_READ_SYNTHETIC_ENV\n")
    token = write(Path(directories["secrets"]) / "control_token", "DO_NOT_READ_SYNTHETIC_TOKEN\n")
    docker.env_file = str(env_file)

    def factory():
        return c.Controller(plan, env_file=env_file, token_file=token, execute=docker, clock=clock, sleep=clock.sleep)

    return plan, docker, clock, factory


def test_service_ids_are_fixed_and_preserved_in_slot_environment(rig, monkeypatch):
    plan, _, _, factory = rig
    controller = factory()

    def unexpected_identity_lookup():
        pytest.fail("service IDs must not depend on the controller's effective IDs")

    monkeypatch.setattr(c.os, "geteuid", unexpected_identity_lookup)
    monkeypatch.setattr(c.os, "getegid", unexpected_identity_lookup)
    for env in (controller._environment(),
                controller._slot_env(plan["slots"][0], "control", "synthetic-run")):
        assert {key: value for key, value in env.items()
                if key.startswith("CLUBELO_CANARY_") and key.endswith(("_UID", "_GID"))} == SERVICE_IDS
        assert not {"CLUBELO_CANARY_UID", "CLUBELO_CANARY_GID"} & env.keys()


def test_no_asset_budget_logic_or_custom_cdp(rig):
    plan, _, _, factory = rig
    assert c.LOCAL_SCHEMA == "clubelo.canary.offline_smoke.v1"
    assert "budget_dirs" not in c.PLAN_KEYS
    assert not {"admitted", "revision"} & c.STATE_KEYS
    assert "request_cap" not in c.REASONS
    assert not hasattr(c, "MAX_REQUESTS") and not hasattr(c, "allocation")
    controller = factory()
    assert not hasattr(controller, "counters") and not hasattr(controller, "budgets")
    env = controller._slot_env(plan["slots"][0], "control", "synthetic-run")
    assert not any("BUDGET" in key or "CDP" in key or "ARMED_REVISION" in key for key in env)
    source = Path(c.__file__).read_text()
    for obsolete in ("BrowserBudgetGate", "read_counter", "read_budget", "counter.json", "budget.json",
                     "browser_requests_", "local_browser_budget_check", "CDP"):
        assert obsolete not in source


def test_all_three_lease_reservations():
    assert c.reservation(0, c.LEASE_BYTES) == c.LEASE_BYTES
    assert c.reservation(c.MAX_BYTES - 3 * c.LEASE_BYTES, c.LEASE_BYTES) == c.LEASE_BYTES
    assert c.reservation(c.MAX_BYTES - 3 * c.LEASE_BYTES + 1, c.LEASE_BYTES) == c.LEASE_BYTES - 1
    assert c.reservation(c.MAX_BYTES - 5, c.LEASE_BYTES) == 1
    assert c.reservation(0, 123) == 123
    for remaining in (0, 1, 2):
        with pytest.raises(c.Refusal, match="byte_cap"):
            c.reservation(c.MAX_BYTES - remaining, c.LEASE_BYTES)


def test_block_balance_seed_and_boundaries():
    import random
    for index in range(3):
        expected = ["control"] * 20 + ["candidate"] * 20
        random.Random(2251 + index).shuffle(expected)
        assert c.block(index) == expected
        assert c.block(index).count("control") == c.block(index).count("candidate") == 20
    assert c.block(0) != c.block(1)
    for invalid in (-1, 3, True):
        with pytest.raises(c.Refusal):
            c.block(invalid)


def test_rolling_two_starts_not_calendar_buckets():
    assert c.rate_delay([0, 59], 59) == 1
    assert c.rate_delay([0, 59], 60) == 0
    assert c.rate_delay([59, 60], 60) == 59
    assert c.rate_delay([1, 1], 1) == 60
    with pytest.raises(c.Refusal):
        c.rate_delay([10, 9], 10)
    with pytest.raises(c.Refusal):
        c.rate_delay([11], 10)


@pytest.mark.parametrize("repository,reported_repository", [
    ("apache/airflow:2.11.2-python3.11", "apache/airflow:2.11.2-python3.11"),
    ("apache/airflow:2.11.2-python3.11", "apache/airflow"),
    ("registry.example:5000/team/runner:tag", "registry.example:5000/team/runner"),
    ("registry.example:5000/team/runner", "registry.example:5000/team/runner"),
    ("runner:tag", "runner"),
])
def test_image_repo_digests_allow_exact_or_tagless_reference(repository, reported_repository):
    digest = "sha256:" + "a" * 64
    reference = repository + "@" + digest
    resolved = "sha256:" + "b" * 64

    def execute(argv, *, env):
        assert argv == ["docker", "image", "inspect", "--format", c.IMAGE_FORMAT, reference]
        return json.dumps(dict(id=resolved, digests=[reported_repository + "@" + digest]))

    controller = SimpleNamespace(plan={"images": {"runner": reference}}, execute=execute, env={})
    assert c.Controller._images(controller) == {"runner": {"requested": reference, "resolved": resolved}}


@pytest.mark.parametrize("digests", [
    [],
    ["another/airflow@" + c.RUNNER_IMAGE.split("@", 1)[1]],
    ["apache/airflow@sha256:" + "0" * 64],
    [c.RUNNER_IMAGE.split("@", 1)[0]],
    ["apache/airflow"],
    ["sha256:" + "d" * 64],
    ["apache/airflow@"],
])
def test_wrong_or_missing_image_repo_digests_prevent_solve(rig, digests):
    _, docker, _, factory = rig
    docker.repo_digests["runner"] = digests
    controller = factory()
    assert controller.run()["status"] == "INCONCLUSIVE"
    assert controller.state["stop_reason"] == "invalid_evidence"
    assert controller.state["preflight"] is None
    assert not docker.runs


@pytest.mark.parametrize("reported_repository", ["registry.example/team/runner", "registry.example"])
def test_image_repo_digests_must_preserve_registry_port(reported_repository):
    digest = "sha256:" + "a" * 64
    reference = "registry.example:5000/team/runner@" + digest
    controller = SimpleNamespace(
        plan={"images": {"runner": reference}}, env={},
        execute=lambda argv, **kwargs: json.dumps(dict(
            id="sha256:" + "b" * 64, digests=[reported_repository + "@" + digest])))
    with pytest.raises(c.Refusal):
        c.Controller._images(controller)


def test_full_block_fake_execution_and_exact_command_allowlist(rig):
    plan, docker, clock, factory = rig
    controller = factory()
    result = controller.run()
    assert docker.gateway_readbacks == [dict(
        healthy=True, daily_budget_bytes=2147483648, daily_total_bytes=0,
        max_lease_bytes=33554432, max_active_leases=1,
        source_mode="shared-no-whoscored")] * 2
    assert set(c.read_json(Path(plan["state_dir"]) / "preflight.json")) == {
        "images", "versions", "gateway", "networks", "shared", "consumers", "local_proof_sha256",
        "code_hashes", "commit", "diff_sha256"}
    assert [arm for arm, _ in docker.runs] == c.block(0)
    assert len(docker.runs) == 40 and clock.sleeps
    assert all(docker.runs[n][1] - docker.runs[n - 2][1] >= 60 for n in range(2, 40))
    assert controller.state["attempts"] == 80
    assert controller.state["provider_bytes"] == 8000
    assert set(controller.state) == c.STATE_KEYS and "admitted" not in controller.status()
    assert controller.state["stop_reason"] == "sample_complete"
    assert all(controller.state["cleanup"].values())
    assert result["status"] == "INCONCLUSIVE"  # Natural-traffic proof is absent.
    assert not docker.own() and not docker.networks
    assert set(docker.rows) == set(plan["shared"].values())
    for slot in plan["slots"][:40]:
        for root in slot["observers"].values():
            assert c.read_json(Path(root) / obs.ACTIVE_FILE)["active"] is False
    commands = len(docker.calls)
    assert factory().status()["runs"] == 40
    assert factory().report()["status"] == "INCONCLUSIVE"
    assert len(docker.calls) == commands  # status/report are strictly read-only.


@pytest.mark.parametrize("status", ["ok", "missing", None, "unknown", False, 1])
def test_gateway_health_probe_projects_only_existing_safe_fields(monkeypatch, capsys, status):
    import io
    import sys
    import urllib.request

    expected = dict(healthy=status == "ok", daily_budget_bytes=2147483648, daily_total_bytes=123,
                    max_lease_bytes=33554432, max_active_leases=1,
                    source_mode="shared-no-whoscored")
    raw = {key: value for key, value in expected.items() if key != "healthy"}
    raw.update(unrelated="MUST_NOT_APPEAR", healthy=False if status == "ok" else True)
    if status != "missing":
        raw["status"] = status

    def health_response(url, timeout):
        assert url == "http://127.0.0.1:8899/health" and timeout == 5
        response = io.StringIO(json.dumps(raw))
        response.status = 200
        return response

    monkeypatch.setattr(urllib.request, "urlopen", health_response)
    monkeypatch.setattr(sys, "argv", ["-c", "/health"])
    exec(c.GATEWAY_HEALTH, {})
    assert json.loads(capsys.readouterr().out) == expected


@pytest.mark.parametrize("field", ["healthy", "daily_budget_bytes", "daily_total_bytes",
                                   "max_lease_bytes", "max_active_leases", "source_mode"])
@pytest.mark.parametrize("invalid", ["missing", None, "unknown"])
def test_missing_or_unknown_gateway_readback_prevents_solve(rig, field, invalid):
    _, docker, _, factory = rig
    if invalid == "missing":
        del docker.gateway_health[field]
    else:
        docker.gateway_health[field] = invalid
    controller = factory()
    assert controller.run()["status"] == "INCONCLUSIVE"
    assert controller.state["stop_reason"] == "invalid_evidence"
    assert controller.state["preflight"] is None
    assert not docker.runs


@pytest.mark.parametrize("field,value", [
    ("healthy", False), ("healthy", 1),
    ("daily_budget_bytes", 2147483647), ("daily_budget_bytes", 2147483649),
    ("daily_budget_bytes", 2147483648.0), ("daily_budget_bytes", True),
    ("max_lease_bytes", 33554431), ("max_lease_bytes", 33554433),
    ("max_lease_bytes", 33554432.0), ("max_lease_bytes", True),
    ("max_active_leases", 0), ("max_active_leases", 2),
    ("max_active_leases", 1.0), ("max_active_leases", True),
    ("daily_total_bytes", -1), ("daily_total_bytes", True),
    ("daily_total_bytes", float("inf")), ("daily_total_bytes", float("-inf")),
    ("daily_total_bytes", float("nan")),
    ("source_mode", "shared"), ("source_mode", "whoscored-only"),
])
def test_wrong_gateway_caps_or_health_prevent_solve(rig, field, value):
    _, docker, _, factory = rig
    docker.gateway_health[field] = value
    controller = factory()
    assert controller.run()["status"] == "INCONCLUSIVE"
    assert controller.state["stop_reason"] == "invalid_evidence"
    assert controller.state["preflight"] is None
    assert not docker.runs


@pytest.mark.parametrize("total", [0, 123, 123.5])
def test_finite_nonnegative_gateway_total_allows_preflight(rig, total):
    plan, docker, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    docker.gateway_health["daily_total_bytes"] = total
    controller = factory()
    controller.run()
    assert controller.state["preflight"] is not None
    assert docker.gateway_readbacks == [docker.gateway_health] * 2
    assert len(docker.runs) == 1


def test_only_completed_reports_count_and_five_candidate_failures_stop(rig):
    _, docker, _, factory = rig
    docker.fail_candidates = True
    controller = factory()
    result = controller.run()
    assert sum(arm == "candidate" for arm, _ in docker.runs) == 5
    assert controller.state["candidate_failures"] == 5
    assert controller.state["stop_reason"] == "five_candidate_failures"
    assert controller.state["attempts"] == 2 * len(docker.runs)
    assert controller.state["provider_bytes"] == 200 * len(docker.runs)
    assert result["status"] == "FAIL"


@pytest.mark.parametrize("active", [None, {"kind": "run", "index": 1, "arm": "control", "run_id": "unknown"}])
def test_crash_before_or_after_continue_never_restarts_traffic(rig, active):
    plan, docker, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    original = factory()
    original.run()
    original.state.update(phase="executing", active=active, cleanup=None, finished_epoch=None)
    original.save()
    count = len(docker.calls)
    with pytest.raises(c.Refusal, match="unknown_execution"):
        factory().run()
    assert len(docker.calls) == count
    assert factory().status()["review_required"] is True
    assert factory().report()["status"] == "INCONCLUSIVE"


def test_interrupt_keeps_unknown_spend_and_deactivates_observers(rig):
    plan, docker, _, factory = rig
    docker.crash = True
    controller = factory()
    with pytest.raises(KeyboardInterrupt):
        controller.run()
    assert len(docker.runs) == 1
    assert controller.state["unknown"] is True
    assert controller.state["cleanup"]["accounting_final"] is False
    assert not docker.own()
    assert controller.state["cleanup"]["observers_inactive"] is True
    for root in plan["slots"][0]["observers"].values():
        assert c.read_json(Path(root) / obs.ACTIVE_FILE)["active"] is False
    with pytest.raises(c.Refusal):
        factory().run()


@pytest.mark.parametrize("mutate", [
    lambda r: r.update(run_id="wrong"),
    lambda r: r.update(complete=False),
    lambda r: r.update(secret="MUST_NOT_APPEAR"),
    lambda r: r.update(wire_bytes=-1),
    lambda r: r["accounting"].update(total_bytes=201),
    lambda r: r["accounting"]["leases"][0]["close_proof"].update(accounting_uncertain=True),
    lambda r: r["accounting"]["leases"][0]["close_proof"].update(active_tunnels=1),
    lambda r: r["accounting"]["leases"][0].update(close_failed=True),
    lambda r: r["attempts"][0].update(run_id="wrong"),
    lambda r: r["attempts"][0].update(started_epoch=r["attempts"][0]["started_epoch"] + 10),
    lambda r: r["attempts"][0].update(detection="unknown"),
])
def test_malformed_ambiguous_or_secret_report_stops(rig, mutate):
    _, docker, _, factory = rig
    docker.mutate_report = mutate
    controller = factory()
    result = controller.run()
    assert len(docker.runs) == 1 and controller.state["unknown"]
    assert result["status"] == "INCONCLUSIVE"
    assert "MUST_NOT_APPEAR" not in json.dumps(result)


def test_shared_restart_stops_without_any_shared_mutation(rig):
    plan, docker, _, factory = rig

    def drift(fake, report):
        fake.rows[plan["shared"]["flaresolverr"]]["restarts"] += 1

    docker.after_run = drift
    controller = factory()
    result = controller.run()
    assert len(docker.runs) == 1 and result["status"] == "FAIL"
    assert controller.state["stop_reason"] == "shared_drift"
    assert controller.state["cleanup"]["shared_unchanged"] is False
    assert all(argv[-1] not in plan["shared"].values()
               for argv in docker.calls if argv[:2] in (["docker", "stop"], ["docker", "rm"]))


@pytest.mark.parametrize("field,value", [("status", "FAIL"), ("local_only", False), ("cleanup_complete", False),
                                        ("reviewed", False), ("images", {}), ("code_hashes", {}),
                                        ("schema", "clubelo.canary.local_browser_budget_check.v1"),
                                        ("airflow_smoke", {}), ("focused_unit", {})])
def test_offline_smoke_gate_precedes_any_start(rig, field, value):
    plan, docker, _, factory = rig
    proof = c.read_json(plan["local_evidence"])
    proof[field] = value
    write(plan["local_evidence"], proof)
    result = factory().run()
    assert result["status"] == "INCONCLUSIVE"
    assert not any(argv[:2] in (["docker", "compose"], ["docker", "exec"]) for argv in docker.calls)


def test_missing_local_evidence_is_read_only_refusal(rig):
    plan, docker, _, factory = rig
    Path(plan["local_evidence"]).unlink()
    with pytest.raises((c.Refusal, FileNotFoundError)):
        factory()
    assert docker.calls == []


def test_preexisting_project_never_cleaned_up_as_ours(rig):
    _, docker, _, factory = rig
    identity = "7" * 64
    docker.rows[identity] = docker.row(identity, "solver-control", image=docker.resolved["control"])
    result = factory().run()
    assert result["status"] == "INCONCLUSIVE"
    assert identity in docker.rows
    assert not any(argv[:2] in (["docker", "stop"], ["docker", "rm"], ["docker", "compose"])
                   for argv in docker.calls)


@pytest.fixture
def runtime_owner_metadata(monkeypatch):
    """Synthetic lstat records; no contents, chmod or chown are needed."""
    parent = Path("/canary-fixture")
    observer_dir = parent / "observer"
    token = observer_dir / "control_token"
    metadata = {
        parent: SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=0, st_gid=0, st_nlink=1),
        observer_dir: SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=1000, st_gid=0, st_nlink=1),
        token: SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=50000, st_gid=0, st_nlink=1),
    }

    def lstat(path):
        return metadata[path]

    def forbidden_read(*args, **kwargs):
        pytest.fail("owner validation must inspect metadata only")

    monkeypatch.setattr(c.os, "geteuid", lambda: 24680)
    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(Path, "read_bytes", forbidden_read)
    monkeypatch.setattr(Path, "read_text", forbidden_read)
    return token, metadata


@pytest.mark.parametrize("ancestor,uid,gid,mode,accepted", [
    ("/canary-fixture", 0, 0, 0o775, True),
    ("/canary-fixture", 0, 0, 0o777, False),
    ("/canary-fixture", 0, 1000, 0o775, False),
    ("/canary-fixture", 1000, 0, 0o775, False),
    ("/canary-fixture", 0, 0, 0o755, True),
    ("/canary-fixture", 0, 0, 0o1777, False),
    ("/tmp", 0, 0, 0o1777, True),
    ("/tmp", 0, 0, 0o777, False),
    ("/tmp", 1000, 0, 0o1777, False),
])
def test_safe_path_ancestor_write_modes(monkeypatch, ancestor, uid, gid, mode, accepted):
    parent = Path(ancestor)
    token = parent / "control_token"
    metadata = {
        parent: SimpleNamespace(st_mode=stat.S_IFDIR | mode, st_uid=uid, st_gid=gid, st_nlink=1),
        token: SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0, st_gid=0, st_nlink=1),
    }
    monkeypatch.setattr(Path, "lstat", lambda path: metadata[path])
    if accepted:
        assert c.safe_path(str(token), secret=True) == token
    else:
        with pytest.raises(c.Refusal):
            c.safe_path(str(token), secret=True)


@pytest.mark.parametrize("uid", [0, 1000, 50000, 24680])
def test_safe_path_accepts_runtime_and_current_owners(runtime_owner_metadata, uid):
    token, metadata = runtime_owner_metadata
    metadata[token].st_uid = uid
    assert c.safe_path(str(token), secret=True) == token
    assert c.safe_path(str(token.parent), directory=True) == token.parent


@pytest.mark.parametrize("ancestor", [False, True])
def test_safe_path_rejects_unexpected_owner(runtime_owner_metadata, ancestor):
    token, metadata = runtime_owner_metadata
    metadata[token.parent if ancestor else token].st_uid = 12345
    with pytest.raises(c.Refusal):
        c.safe_path(str(token), secret=True)


@pytest.mark.parametrize("mode", [0o620, 0o602])
@pytest.mark.parametrize("secret", [False, True])
@pytest.mark.parametrize("uid", [0, 50000])
def test_runtime_owned_writable_leaf_is_refused(runtime_owner_metadata, mode, secret, uid):
    token, metadata = runtime_owner_metadata
    metadata[token].st_uid = uid
    metadata[token].st_mode = stat.S_IFREG | mode
    with pytest.raises(c.Refusal):
        c.safe_path(str(token), secret=secret)


@pytest.mark.parametrize("mode", [0o775, 0o777])
def test_root_owned_writable_directory_leaf_is_refused(runtime_owner_metadata, mode):
    token, metadata = runtime_owner_metadata
    metadata[token.parent].st_uid = 0
    metadata[token.parent].st_mode = stat.S_IFDIR | mode
    with pytest.raises(c.Refusal):
        c.safe_path(str(token.parent), directory=True)


@pytest.mark.parametrize("ancestor", [False, True])
def test_runtime_owned_symlink_component_is_refused(runtime_owner_metadata, ancestor):
    token, metadata = runtime_owner_metadata
    metadata[token.parent if ancestor else token].st_mode = stat.S_IFLNK | 0o700
    with pytest.raises(c.Refusal):
        c.safe_path(str(token), secret=True)


def test_secrets_are_metadata_only_and_unsafe_modes_refused(rig, monkeypatch):
    _, docker, _, factory = rig
    original_read = Path.read_bytes
    original_text = Path.read_text
    secrets = {docker.env_file, str(Path(docker.env_file).with_name("control_token"))}

    def guarded_bytes(path, *args, **kwargs):
        assert str(path) not in secrets
        return original_read(path, *args, **kwargs)

    def guarded_text(path, *args, **kwargs):
        assert str(path) not in secrets
        return original_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", guarded_bytes)
    monkeypatch.setattr(Path, "read_text", guarded_text)
    factory()
    Path(docker.env_file).chmod(0o644)
    with pytest.raises(c.Refusal):
        factory()
    assert docker.calls == []


def test_symlinks_unexpected_files_and_production_paths_refused(rig, tmp_path):
    plan, _, _, factory = rig
    write(Path(plan["state_dir"]) / "unexpected.json", "{}")
    with pytest.raises(c.Refusal):
        factory()
    alias = tmp_path / "alias"
    alias.symlink_to(plan["state_dir"], target_is_directory=True)
    with pytest.raises(c.Refusal):
        c.safe_path(str(alias), directory=True)
    runtime = private(tmp_path / "clubelo-runtime")
    with pytest.raises(c.Refusal):
        c.safe_path(runtime, directory=True)


def test_corrupt_state_is_never_reinitialized(rig):
    plan, docker, _, factory = rig
    controller = factory()
    write(Path(plan["state_dir"]) / "state.json", '{"schema":1,"schema":2}')
    with pytest.raises(ValueError):
        controller.run()
    assert docker.calls == []


@pytest.mark.parametrize("section,field,value", [
    ("airflow_smoke", "status", "FAIL"),
    ("airflow_smoke", "real_airflow", False),
    ("airflow_smoke", "runner_image", "sha256:" + "0" * 64),
    ("airflow_smoke", "evidence_sha256", "unknown"),
    ("airflow_smoke", "raw_output", "MUST_NOT_APPEAR"),
    ("focused_unit", "status", "FAIL"),
    ("focused_unit", "passed", 0),
    ("focused_unit", "passed", True),
    ("focused_unit", "failed", 1),
    ("focused_unit", "skipped", 1),
    ("focused_unit", "evidence_sha256", None),
    ("focused_unit", "raw_output", "MUST_NOT_APPEAR"),
])
def test_missing_real_smoke_or_focused_unit_evidence_blocks_start(rig, section, field, value):
    plan, docker, _, factory = rig
    proof = c.read_json(plan["local_evidence"])
    proof[section][field] = value
    write(plan["local_evidence"], proof)
    result = factory().run()
    assert result["status"] == "INCONCLUSIVE"
    assert "MUST_NOT_APPEAR" not in json.dumps(result)
    assert not docker.runs
    assert not any(argv[:2] == ["docker", "compose"] for argv in docker.calls)


@pytest.mark.parametrize("field,value,reason", [("attempts", 238, "attempt_cap"),
                                               ("candidate_failures", 5, "five_candidate_failures"),
                                               ("provider_bytes", c.MAX_BYTES - 3 * c.LEASE_BYTES + 1, "harness_byte_guard"),
                                               ("provider_bytes", c.MAX_BYTES - 2, "byte_cap")])
def test_before_run_reservations_stop_before_commands(rig, field, value, reason):
    _, docker, _, factory = rig
    controller = factory()
    controller.owned = True
    controller.state = dict(phase="executing", active=None, unknown=False, run_starts=[], completed_runs=[],
                            attempts=0, candidate_failures=0, provider_bytes=0)
    controller.state[field] = value
    with pytest.raises(c.Refusal, match=reason):
        controller._before_run()
    assert docker.calls == []


def test_maximum_three_blocks_never_has_a_fourth_run(rig):
    _, docker, _, factory = rig
    controller = factory()
    controller.owned = True
    controller.state = dict(phase="executing", active=None, unknown=False,
                            run_starts=[0] * 120, completed_runs=[{}] * 120)
    with pytest.raises(c.Refusal, match="run_cap"):
        controller._before_run()
    assert docker.calls == []


def natural(requests=100, errors=2, p95=110):
    before = dict(requests=requests, errors=0, p95_ms=100, start=900, end=1000, identity_sha256="a" * 64)
    after = dict(requests=requests, errors=errors, p95_ms=p95, start=1000, end=1100, identity_sha256="a" * 64)
    return dict(schema=c.TRAFFIC_SCHEMA,
                sources={source: dict(baseline=copy.deepcopy(before), current=copy.deepcopy(after))
                         for source in ("whoscored", "sofifa")})


def test_natural_traffic_exact_gates_and_additional_consumers():
    assert c.traffic_gate(natural(), [], 1000, 1100)["status"] == "PASS"
    assert c.traffic_gate(natural(errors=3), [], 1000, 1100)["status"] == "FAIL"
    assert c.traffic_gate(natural(p95=110.01), [], 1000, 1100)["status"] == "FAIL"
    assert c.traffic_gate(natural(requests=29), [], 1000, 1100)["status"] == "INCONCLUSIVE"
    assert c.traffic_gate(natural(), ["new-consumer"], 1000, 1100)["status"] == "INCONCLUSIVE"
    assert c.traffic_gate({}, [], 1000, 1100)["status"] == "INCONCLUSIVE"
    changed = natural()
    changed["sources"]["sofifa"]["current"]["identity_sha256"] = "b" * 64
    assert c.traffic_gate(changed, [], 1000, 1100)["status"] == "INCONCLUSIVE"


def test_late_orphan_solver_event_invalidates_saved_evidence(rig):
    plan, _, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    controller = factory()
    controller.run()
    arm = c.block(0)[0]
    root = Path(plan["slots"][0]["observers"][arm])
    write(root / obs.ERROR_FILE, dict(schema=obs.ERROR_SCHEMA, version=1, reason="invalid_state"))
    with pytest.raises(c.Refusal):
        factory().report()


def test_mutated_report_after_checkpoint_is_not_reused(rig):
    plan, _, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    factory().run()
    target = Path(plan["slots"][0]["output"]) / "report.json"
    report = c.read_json(target)
    report["wire_bytes"] += 1
    write(target, report)
    with pytest.raises(c.Refusal):
        factory().status()


def test_no_arbitrary_compose_operation_or_shared_service(rig):
    _, docker, _, factory = rig
    controller = factory()
    for operation, services in (("down", ["solver-control"]), ("up", ["airflow-scheduler"]),
                                ("run", ["runner"]), ("up", ["runner"])):
        with pytest.raises(c.Refusal):
            controller.compose(operation, services, controller.env)
    assert docker.calls == []


def test_new_natural_traffic_regression_stops_before_next_run(rig):
    plan, docker, _, factory = rig

    def regression(fake, report):
        data = natural(errors=3)
        for row in data["sources"].values():
            row["baseline"].update(start=990, end=1000)
            row["current"].update(start=1000, end=1010)
        write(plan["traffic_evidence"], data)

    docker.after_run = regression
    controller = factory()
    result = controller.run()
    assert len(docker.runs) == 1
    assert controller.state["stop_reason"] == "other_source_regression"
    assert result["status"] == "FAIL"


def test_pass_requires_statistics_other_sources_and_all_rollback_checks(rig, monkeypatch):
    plan, docker, clock, factory = rig
    plan["slots"] = plan["slots"][:1]
    controller = factory()
    controller.run()
    # The statistical evaluator has its own tests; isolate the outer AND gate.
    monkeypatch.setattr(h, "evaluate", lambda reports: {"status": "PASS"})
    assert factory().report()["status"] == "INCONCLUSIVE"
    data = natural(errors=0)
    for row in data["sources"].values():
        row["baseline"].update(start=990, end=1000)
        row["current"].update(start=1000, end=clock())
    write(plan["traffic_evidence"], data)
    # Exhausting slots cannot promote an incomplete experiment to PASS.
    assert factory().report()["status"] == "INCONCLUSIVE"
    controller.state["stop_reason"] = "sample_complete"
    controller.save()
    assert factory().report()["status"] == "PASS"
    controller.state["cleanup"]["accounting_final"] = False
    controller.save()
    assert factory().report()["status"] == "INCONCLUSIVE"


def test_late_inactive_arm_event_invalidates_run(rig):
    plan, docker, _, factory = rig

    def inactive_event(fake, report):
        other = next(arm for arm in c.ARMS if arm != report["arm"])
        path = Path(plan["slots"][0]["observers"][other]) / obs.EVENT_FILE
        write(path, "unexpected event")

    docker.after_run = inactive_event
    controller = factory()
    assert controller.run()["status"] == "INCONCLUSIVE"
    assert len(docker.runs) == 1 and controller.state["stop_reason"] == "invalid_evidence"


def test_deadline_does_not_extend_or_start_a_partial_window(rig):
    plan, docker, clock, factory = rig
    plan["deadline_epoch"] = clock() + 100
    controller = factory()
    assert controller.run()["status"] == "INCONCLUSIVE"
    assert not docker.runs
    assert controller.state["deadline_epoch"] == 1100
    assert controller.state["stop_reason"] == "deadline"


def test_simultaneous_controller_lock_is_refused_without_commands(rig):
    _, docker, _, factory = rig
    first, second = factory(), factory()
    with first.lock():
        with pytest.raises(BlockingIOError):
            with second.lock():
                pytest.fail("second controller obtained the same experiment lock")
    assert docker.calls == []


@pytest.mark.parametrize("attempts,spent,starts,wait", [
    (237, c.MAX_BYTES - 3 * c.LEASE_BYTES, [], []),
    (0, 0, [940.0, 999.0], []),
    (0, 0, [941.0, 999.0], [1]),
    (0, 0, [0.0] * 119, []),
])
def test_last_available_run_reservation_and_rate_wait(rig, monkeypatch, attempts, spent, starts, wait):
    plan, docker, clock, factory = rig
    controller = factory()
    controller.owned = True
    controller.state = dict(phase="executing", active=None, unknown=False,
                            run_starts=starts, completed_runs=[{}] * len(starts),
                            attempts=attempts, candidate_failures=0, provider_bytes=spent,
                            deadline_epoch=plan["deadline_epoch"])
    monkeypatch.setattr(controller, "unchanged", lambda: None)
    monkeypatch.setattr(controller, "verify_hashes", lambda: None)
    monkeypatch.setattr(controller, "_preflight_evidence", lambda: {"images": {}})
    monkeypatch.setattr(controller, "_images", lambda: {})
    monkeypatch.setattr(controller, "_local_proof", lambda images: None)
    n, arm, slot, cap = controller._before_run()
    assert n == len(starts) and arm == c.block(n // 40)[n % 40]
    assert slot == plan["slots"][n] and cap == c.LEASE_BYTES
    assert docker.calls == []
    assert clock.sleeps == wait


def test_rate_wait_honors_deadline_before_commands(rig):
    _, docker, clock, factory = rig
    controller = factory()
    controller.owned = True
    controller.state = dict(phase="executing", active=None, unknown=False,
                            run_starts=[clock(), clock()], completed_runs=[{}, {}],
                            attempts=4, candidate_failures=0, provider_bytes=400,
                            deadline_epoch=clock() + c.RUN_SECONDS + 59)
    with pytest.raises(c.Refusal, match="deadline"):
        controller._before_run()
    assert clock.sleeps == [60] and docker.calls == []


@pytest.mark.parametrize("remaining", [c.MAX_SECONDS + 1, 0, -1])
def test_deadline_outside_thirty_days_refused_without_commands(rig, remaining):
    plan, docker, clock, factory = rig
    plan["deadline_epoch"] = clock() + remaining
    with pytest.raises(c.Refusal, match="deadline"):
        factory().run()
    assert docker.calls == []


@pytest.mark.parametrize("field,value", [("attempts", 241), ("provider_bytes", c.MAX_BYTES + 1),
                                        ("candidate_failures", 6), ("unknown", "false")])
def test_corrupt_saved_limits_refuse_read_only_status(rig, field, value):
    plan, docker, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    controller = factory()
    controller.run()
    controller.state[field] = value
    controller.save()
    calls = len(docker.calls)
    with pytest.raises(c.Refusal):
        factory().status()
    assert len(docker.calls) == calls


@pytest.mark.parametrize("lease_bytes", [c.LEASE_BYTES, c.LEASE_BYTES + 1])
def test_exact_lease_close_byte_boundary(rig, lease_bytes):
    plan, docker, _, factory = rig
    plan["slots"] = plan["slots"][:1]

    def close_bytes(report):
        for lease in report["accounting"]["leases"]:
            lease["close_proof"]["total_bytes"] = lease_bytes
        report["accounting"]["total_bytes"] = report["wire_bytes"] = 2 * lease_bytes

    docker.mutate_report = close_bytes
    controller = factory()
    controller.run()
    assert len(docker.runs) == 1
    if lease_bytes == c.LEASE_BYTES:
        assert controller.state["provider_bytes"] == 2 * lease_bytes
        assert controller.state["cleanup"]["accounting_final"] is True
    else:
        assert controller.state["unknown"] is True
        assert controller.state["cleanup"]["accounting_final"] is False


@pytest.mark.parametrize("evidence", [{}, {"schema": c.TRAFFIC_SCHEMA, "sources": {}},
                                     '{"schema":1,"schema":2}', "private invalid evidence"])
def test_unknown_or_corrupt_present_traffic_stops_next_run(rig, evidence):
    plan, docker, _, factory = rig

    def unknown_traffic(fake, report):
        write(plan["traffic_evidence"], evidence)

    docker.after_run = unknown_traffic
    controller = factory()
    result = controller.run()
    assert len(docker.runs) == 1 and controller.state["unknown"] is True
    assert controller.state["stop_reason"] == "invalid_evidence"
    assert result["status"] == "INCONCLUSIVE"
    assert "private invalid evidence" not in json.dumps(result)


def test_cleanup_failure_withholds_rollback_pass(rig, monkeypatch):
    plan, docker, _, factory = rig
    plan["slots"] = plan["slots"][:1]
    controller = factory()
    controller.run()

    def cannot_deactivate(root):
        raise ValueError("MUST_NOT_APPEAR")

    monkeypatch.setattr(controller, "_deactivate", cannot_deactivate)
    cleanup = controller.cleanup()
    assert cleanup["observers_inactive"] is False
    assert cleanup["resources_removed"] is True and not docker.own()
    result = controller.report()
    assert result["rollback_pass"] is False and result["status"] == "INCONCLUSIVE"
    assert "MUST_NOT_APPEAR" not in json.dumps(result)


def test_three_blocks_bound_each_arm_to_sixty_runs():
    arms = [arm for index in range(3) for arm in c.block(index)]
    assert len(arms) == c.MAX_RUNS == 120
    assert arms.count("control") == arms.count("candidate") == 60
    assert c.MAX_ATTEMPTS == 240 and c.MAX_BYTES == 2 * 1024**3


@pytest.mark.parametrize("runs,attempts,detected,expected", [
    (20, 40, 30, True), (19, 40, 30, False), (20, 39, 30, False), (20, 40, 29, False),
])
def test_each_arm_requires_all_three_minima(monkeypatch, runs, attempts, detected, expected):
    def metrics(reports):
        arm = next(iter(reports))["arm"]
        return {"counts": dict(runs=runs, attempts=attempts, detected=detected) if arm == "candidate"
                else dict(runs=20, attempts=40, detected=30)}

    monkeypatch.setattr(h, "arm_metrics", metrics)
    assert c.minima([{"arm": arm} for arm in c.ARMS]) is expected


def test_validated_candidate_clearance_resets_five_failure_streak(rig):
    _, docker, _, factory = rig
    docker.fail_candidates = True

    def failures_around_clearance(fake, report):
        count = sum(arm == "candidate" for arm, _ in fake.runs)
        fake.fail_candidates = count != 4

    docker.after_run = failures_around_clearance
    controller = factory()
    result = controller.run()
    candidates = [r for r in controller.reports if r["arm"] == "candidate"]
    assert len(candidates) == 10 and candidates[4]["validated_clearance"] is True
    assert controller.state["candidate_failures"] == 5
    assert controller.state["stop_reason"] == "five_candidate_failures"
    assert result["status"] == "FAIL"


def test_removed_plan_and_state_fields_are_rejected(rig):
    plan, docker, _, factory = rig
    plan["budget_dirs"] = {}
    with pytest.raises(c.Refusal):
        factory()
    assert docker.calls == []
    del plan["budget_dirs"]
    plan["slots"] = plan["slots"][:1]
    controller = factory()
    controller.run()
    controller.state["admitted"] = dict.fromkeys(c.ARMS, 0)
    controller.save()
    calls = len(docker.calls)
    with pytest.raises(c.Refusal):
        factory().status()
    assert len(docker.calls) == calls
