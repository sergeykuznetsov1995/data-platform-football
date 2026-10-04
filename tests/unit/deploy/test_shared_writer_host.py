from copy import deepcopy
import json
import subprocess

import pytest

from deploy.shared_writer import host as module


@pytest.fixture
def environment(tmp_path, monkeypatch):
    root = tmp_path / "runtime"
    root.mkdir()
    config = tmp_path / "compose.yaml"
    config.write_text("services: {}\n")
    rows = []
    for index, name in enumerate((*module.CONSUMERS, module.METADB), 1):
        rows.append({
            "name": "/" + name, "id": f"{index:064x}", "image": "sha256:" + f"{index:064x}",
            "status": "running", "health": "healthy", "project": "data-platform",
            "working_dir": str(root), "config_files": str(config),
            "mounts": [{"Type": "bind", "Source": str(root / part), "Destination": "/opt/airflow/" + part,
                        "RW": False} for part in ("scrapers", "dags", "scripts")] if name != module.METADB else [],
        })
    env = {
        "rows": rows, "db": {"dags": {"dag_ingest_clubelo": {"paused": True, "errors": False, "parsed": 1000, "fileloc": "/opt/airflow/dags/dag_ingest_clubelo.py"}},
                             "work": {"now": 1000000, "active_runs": 0, "unstarted_tasks": 0, "tasks": [], "local_jobs": []}, "import_errors": []},
        "pids": "", "calls": [], "config": config,
    }

    def run(args, **kwargs):
        assert kwargs.get("shell", False) is False
        assert 0 < kwargs["timeout"] <= 20
        assert "Env" not in " ".join(args)
        env["calls"].append(args)
        if args[:3] == ["docker", "ps", "-aq"]:
            output = "\n".join(row["id"] for row in env["rows"])
        elif args[:2] == ["docker", "inspect"]:
            assert args[3] == module.INSPECT_FORMAT
            output = "\n".join(json.dumps(row) for row in env["rows"])
        elif args[:3] == ["docker", "exec", "postgres"]:
            assert args[3:5] == ["psql", "-X"]
            assert "READ ONLY" in args[-1] and "ROLLBACK" in args[-1]
            output = json.dumps(env["db"])
        elif args[:2] == ["docker", "exec"] and args[3] == "sha256sum":
            output = env.get("writer_hash", "a" * 64) + "  /opt/airflow/scrapers/base/iceberg_writer.py\n"
        elif args[:2] == ["pgrep", "-f"]:
            output = env["pids"]
            return subprocess.CompletedProcess(args, 0 if output else 1, output, "")
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(module.subprocess, "run", run)
    monkeypatch.setattr(module, "classify_processes", lambda pids, containers, root:
                        [{"pid": pid, "blocks": True, "reason": "unverified_process"} for pid in pids])
    return module.Host(root), env


def test_snapshot_and_preflight_are_read_only(environment):
    host, env = environment
    baseline = host.snapshot()
    assert baseline["dags"]["dag_ingest_clubelo"]["parsed"] == 1000.0
    assert baseline["drivers"] == []
    assert set(baseline["identity"]["consumers"]) == set(module.CONSUMERS)
    assert baseline["identity"]["metadb"]["id"]
    assert host.preflight(baseline) == baseline
    assert not any(command[:2] in (["docker", "run"], ["docker", "compose"]) for command in env["calls"])


def test_dormant_consumer_is_pinned_and_cannot_start(environment):
    host, env = environment
    extra = deepcopy(env["rows"][0])
    extra.update(name="/unreviewed-consumer", id="f" * 64, status="exited")
    env["rows"].append(extra)
    baseline = host.snapshot()
    assert baseline["identity"]["dormant_consumers"]["unreviewed-consumer"]["status"] == "exited"
    extra["status"] = "running"
    with pytest.raises(RuntimeError, match="unexpected consumer"):
        host.preflight(baseline)


@pytest.mark.parametrize("relative", ["scrapers/base", "scrapers/base/iceberg_writer.py", "dags", "scripts"])
def test_extra_mount_exposing_writer_or_code_tree_rejected(environment, relative):
    host, env = environment
    extra = deepcopy(env["rows"][0])
    extra.update(name="/unreviewed-consumer", id="f" * 64)
    extra["mounts"] = [{"Type": "bind", "Source": str(host.root / relative), "Destination": "/arbitrary", "RW": False}]
    env["rows"].append(extra)
    with pytest.raises(RuntimeError, match="unexpected consumer"):
        host.snapshot()


@pytest.mark.parametrize("relative", ["proxys.txt", "scripts/seaweedfs_legacy_entrypoint.sh"])
def test_unrelated_single_file_mount_only_recorded_as_path(environment, relative):
    host, env = environment
    extra = deepcopy(env["rows"][0])
    extra.update(name="/unrelated-service", id="f" * 64)
    extra["mounts"] = [{"Type": "bind", "Source": str(host.root / relative), "Destination": "/unrelated", "RW": False}]
    env["rows"].append(extra)
    snapshot = host.snapshot()
    assert snapshot["identity"]["other_shared_mounts"] == {
        "unrelated-service": [{"source": str(host.root / relative), "destination": "/unrelated"}]
    }


def test_writer_hashes_read_each_consumer(environment):
    host, env = environment
    assert host.writer_hashes("a" * 64) == {name: "a" * 64 for name in module.CONSUMERS}
    assert len(env["calls"]) == 4


def test_writer_hash_mismatch_fails_closed(environment):
    host, env = environment
    env["writer_hash"] = "b" * 64
    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        host.writer_hashes("a" * 64)


def test_invalid_writer_hash_does_not_run_commands(environment):
    host, env = environment
    with pytest.raises(RuntimeError, match="invalid expected"):
        host.writer_hashes("not a hash")
    assert env["calls"] == []


@pytest.mark.parametrize("change", ["mount", "image", "config", "id"])
def test_identity_drift_is_rejected(environment, change):
    host, env = environment
    baseline = host.snapshot()
    if change == "mount":
        env["rows"][0]["mounts"].append({"Type": "bind", "Source": "/different", "Destination": "/config", "RW": False})
    elif change == "config":
        env["config"].write_text("services: {changed: true}\n")
    elif change == "image":
        env["rows"][0][change] = "sha256:" + "b" * 64
    else:
        env["rows"][0][change] = "b" * 64
    with pytest.raises(RuntimeError, match="identity drift"):
        host.preflight(baseline)


@pytest.mark.parametrize("field,value", [("RW", True), ("Source", "/wrong/runtime/scrapers"), ("Type", "volume")])
def test_unsafe_code_mount_rejected(environment, field, value):
    host, env = environment
    env["rows"][0]["mounts"][0][field] = value
    with pytest.raises(RuntimeError, match="unexpected shared code mount"):
        host.snapshot()


@pytest.mark.parametrize("field,value", [("health", "unhealthy"), ("health", None), ("status", "restarting")])
def test_bad_health_rejected(environment, field, value):
    host, env = environment
    env["rows"][0][field] = value
    with pytest.raises(RuntimeError, match="not running and healthy"):
        host.snapshot()


@pytest.mark.parametrize("condition", ["unpaused", "pause_changed", "dag_changed", "dag_error", "active", "import_error", "driver"])
def test_busy_or_changed_window_refuses_preflight(environment, condition):
    host, env = environment
    if condition == "unpaused":
        env["db"]["dags"]["dag_ingest_clubelo"]["paused"] = False
    baseline = host.snapshot()
    if condition == "pause_changed":
        env["db"]["dags"]["dag_ingest_clubelo"]["paused"] = False
    elif condition == "dag_changed":
        env["db"]["dags"]["another"] = {"paused": True, "errors": False, "parsed": 1000, "fileloc": "/opt/airflow/dags/dag_ingest_clubelo.py"}
    elif condition == "dag_error":
        env["db"]["dags"]["dag_ingest_clubelo"]["errors"] = True
    elif condition == "active":
        env["db"]["work"]["active_runs"] = 1
    elif condition == "import_error":
        env["db"]["import_errors"] = [{"filename": "/new.py", "stacktrace": "new error"}]
    elif condition == "driver":
        env["pids"] = "345\n"
    with pytest.raises(RuntimeError):
        host.preflight(baseline)


def test_rollback_allows_import_errors_and_running_unhealthy(environment):
    host, env = environment
    baseline = host.snapshot()
    env["db"]["import_errors"] = [{"filename": "/new.py", "stacktrace": "new error"},
                                    {"filename": "/other.py", "stacktrace": "other error"}]
    env["db"]["dags"]["dag_ingest_clubelo"]["errors"] = True
    env["rows"][0]["health"] = "unhealthy"
    assert host.rollback_preflight(baseline)["import_errors"] == 2
    with pytest.raises(RuntimeError):
        host.preflight(baseline)


@pytest.mark.parametrize("condition", ["unpaused", "active", "identity", "metadb_unhealthy", "stopped_consumer", "driver"])
def test_rollback_still_rejects_unsafe_window(environment, condition):
    host, env = environment
    baseline = host.snapshot()
    env["db"]["import_errors"] = [{"filename": "/new.py", "stacktrace": "new error"},
                                    {"filename": "/other.py", "stacktrace": "other error"}]
    if condition == "unpaused":
        env["db"]["dags"]["dag_ingest_clubelo"]["paused"] = False
    elif condition == "active":
        env["db"]["work"]["active_runs"] = 1
    elif condition == "identity":
        env["rows"][0]["image"] = "sha256:" + "e" * 64
    elif condition == "metadb_unhealthy":
        env["rows"][-1]["health"] = "unhealthy"
    elif condition == "stopped_consumer":
        env["rows"][0]["status"] = "exited"
    else:
        env["pids"] = "456\n"
    with pytest.raises(RuntimeError):
        host.rollback_preflight(baseline)


def test_postflight_waits_for_strictly_new_parse(environment, monkeypatch):
    host, env = environment
    baseline = host.snapshot()
    env["db"]["dags"]["dag_ingest_clubelo"]["parsed"] = 1060
    sleeps = []
    def sleep(seconds):
        sleeps.append(seconds)
        env["db"]["dags"]["dag_ingest_clubelo"]["parsed"] = 1061
    monkeypatch.setattr(module.time, "sleep", sleep)
    result = host.postflight(baseline, since=1000)
    assert result["dags"]["dag_ingest_clubelo"]["parsed"] == 1061.0
    assert sleeps == [20.0]


def test_postflight_deadline_bounds_commands_and_sleep(environment, monkeypatch):
    host, env = environment
    baseline = host.snapshot()
    now = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    with pytest.raises(RuntimeError, match="deadline"):
        host.postflight(baseline, since=2000, timeout=7)
    assert now[0] == 7


def test_postflight_aborts_on_busy_window_instead_of_waiting(environment, monkeypatch):
    host, env = environment
    baseline = host.snapshot()
    env["db"]["work"]["active_runs"] = 1
    monkeypatch.setattr(module.time, "sleep", lambda _: pytest.fail("must not sleep through safety failure"))
    with pytest.raises(RuntimeError, match="unfinished work"):
        host.postflight(baseline, since=0)


@pytest.mark.parametrize("malformation", ["empty", "negative", "nan", "boolean_counter"])
def test_invalid_metabase_refused(environment, malformation):
    host, env = environment
    if malformation == "empty":
        env["db"]["dags"] = {}
    elif malformation == "negative":
        env["db"]["work"]["active_runs"] = -1
    elif malformation == "nan":
        env["db"]["dags"]["dag_ingest_clubelo"]["parsed"] = float("nan")
    else:
        env["db"]["work"]["active_runs"] = False
    with pytest.raises(RuntimeError, match="invalid read-only"):
        host.snapshot()


def test_timeout_fails_closed_and_does_not_echo_stderr(environment, monkeypatch):
    host, env = environment
    def timeout(args, **kwargs):
        raise subprocess.TimeoutExpired(args, kwargs["timeout"])
    monkeypatch.setattr(module.subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="timed out"):
        host.snapshot()


def test_snapshot_sql_covers_waiting_work():
    for state in ("queued", "running", "restarting", "deferred", "scheduled", "up_for_retry", "up_for_reschedule"):
        assert f"'{state}'" in module.SNAPSHOT_SQL
    assert "ISOLATION LEVEL REPEATABLE READ READ ONLY" in module.SNAPSHOT_SQL
    assert "WHERE is_active IS TRUE" in module.SNAPSHOT_SQL


def test_symlink_compose_config_rejected(environment, tmp_path):
    host, env = environment
    link = tmp_path / "alias.yaml"
    link.symlink_to(env["config"])
    env["rows"][0]["config_files"] = str(link)
    with pytest.raises(RuntimeError, match="regular absolute"):
        host.snapshot()


def test_dormant_mount_order_does_not_block_apply_or_recovery(environment):
    host, env = environment
    extra = deepcopy(env["rows"][0])
    extra.update(name="/airflow-init", id="f" * 64, status="exited")
    env["rows"].append(extra)
    baseline = host.snapshot()
    extra["mounts"].reverse()
    assert host.preflight(baseline)["identity"] == baseline["identity"]
    assert host.rollback_preflight(baseline)["identity"] == baseline["identity"]
    extra["mounts"][0]["Source"] += "-foreign"
    with pytest.raises(RuntimeError, match="identity drift"):
        host.rollback_preflight(baseline)


def test_error_evidence_contains_hashes_not_tracebacks(environment):
    host, env = environment
    env["db"]["import_errors"] = [{"filename": "/new.py", "stacktrace": "do not journal raw trace"}]
    snapshot = host._snapshot()
    assert snapshot["import_errors"] == 1
    assert len(snapshot["import_error_details"][0]["sha256"]) == 64
    assert "do not journal" not in json.dumps(snapshot)
    with pytest.raises(RuntimeError, match="unapproved"):
        host.snapshot()


def test_active_dag_file_change_rejected_even_without_error_flag(environment):
    host, env = environment
    baseline = host.snapshot()
    env["db"]["dags"]["dag_ingest_clubelo"]["fileloc"] = "/opt/airflow/dags/other.py"
    with pytest.raises(RuntimeError, match="file location"):
        host.preflight(baseline)


def test_process_exceptions_are_evidence_not_blanket_pgrep_ignore(environment, monkeypatch):
    host, env = environment
    env["pids"] = "100\n200\n300\n"
    def classify(pids, containers, root):
        assert pids == [100, 200, 300] and containers == env["rows"] and root == host.root
        return [{"pid": 100, "blocks": False, "reason": "verified_tail"},
                {"pid": 200, "blocks": False, "reason": "separate_container_writer"},
                {"pid": 300, "blocks": True, "reason": "unverified_process"}]
    monkeypatch.setattr(module, "classify_processes", classify)
    baseline = host.snapshot()
    assert baseline["drivers"] == [300]
    assert len(baseline["processes"]) == 3
    with pytest.raises(RuntimeError, match="external drivers"):
        host.preflight(baseline)


@pytest.fixture
def legacy_environment(environment, monkeypatch):
    import hashlib
    host, env = environment
    host.import_error_policy = module.legacy_errors.POLICY
    raw_errors = [{"filename": name, "stacktrace": "reviewed legacy traceback"}
                  for name in module.legacy_errors.ERRORS]
    errors = {e["filename"]: hashlib.sha256(e["stacktrace"].encode()).hexdigest() for e in raw_errors}
    monkeypatch.setattr(module.legacy_errors, "ERRORS", errors)
    monkeypatch.setattr(module.legacy_errors, "protected_hashes", lambda root, policy: dict(module.legacy_errors.PROTECTED))
    env["db"]["import_errors"] = raw_errors
    return host, env


def test_explicit_legacy_policy_survives_new_host_and_requires_same_errors(legacy_environment):
    host, env = legacy_environment
    baseline = host.snapshot()
    assert baseline["import_errors"] == 6
    assert baseline["import_error_policy"] == module.legacy_errors.POLICY
    # Check/apply load the policy from the approved manifest, not a repeated flag.
    assert module.Host(host.root).preflight(baseline) == baseline
    env["db"]["import_errors"][0]["stacktrace"] += "new failure"
    with pytest.raises(RuntimeError, match="baseline changed"):
        host.preflight(baseline)
    # A writer failure may be rolled back; healthy postflight still checks baseline.
    assert host.rollback_preflight(baseline)["import_errors"] == 6
    with pytest.raises(RuntimeError, match="baseline changed"):
        host.postflight(baseline, since=0)
    env["db"]["import_errors"][0]["stacktrace"] = "reviewed legacy traceback"
    assert host.postflight(baseline, since=0)["import_errors"] == 6


def test_legacy_policy_cannot_allow_active_dag_errors(legacy_environment):
    host, env = legacy_environment
    baseline = host.snapshot()
    env["db"]["dags"]["dag_ingest_clubelo"]["errors"] = True
    with pytest.raises(RuntimeError, match="DAG reports import errors"):
        host.preflight(baseline)


def test_legacy_file_becoming_active_is_rejected_at_prepare(legacy_environment):
    host, env = legacy_environment
    env["db"]["dags"]["legacy"] = {"paused": True, "errors": False, "parsed": 1000,
                                             "fileloc": next(iter(module.legacy_errors.ERRORS))}
    with pytest.raises(RuntimeError, match="active DAG"):
        host.snapshot()


@pytest.mark.parametrize("invalid", [None, {}, [{"filename": "/x", "stacktrace": None}],
                                      [{"filename": "/x", "stacktrace": "a"}] * 2])
def test_invalid_import_error_evidence_rejected(environment, invalid):
    host, env = environment
    env["db"]["import_errors"] = invalid
    with pytest.raises(RuntimeError, match="invalid read-only"):
        host.snapshot()
