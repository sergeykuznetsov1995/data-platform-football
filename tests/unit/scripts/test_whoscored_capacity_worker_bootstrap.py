from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any

import pytest

from scrapers.whoscored.runtime_contract import (
    EXPECTED_RUNTIME_FILES,
    RUNTIME_CONTRACT_PATH,
)


ROOT = Path(__file__).resolve().parents[3]
HELPER_PATH = (
    ROOT / "docker/images/airflow/whoscored_capacity_worker_bootstrap.py"
)
SPEC = importlib.util.spec_from_file_location(
    "test_whoscored_capacity_worker_bootstrap_module", HELPER_PATH
)
assert SPEC is not None and SPEC.loader is not None
bootstrap = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bootstrap
SPEC.loader.exec_module(bootstrap)


def _document(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": 1,
        "worker_id": 2,
        "owner": "0123456789abcdef",
        "expected_python": "3.11",
        "expected_curl_cffi": "0.15.0",
        "argv": [
            bootstrap.WORKFLOW_SCRIPT,
            "--scope",
            "INT-World Cup=2026",
            "--match-limit",
            "3",
        ],
    }
    value.update(overrides)
    return value


def _control(**overrides: Any):
    return bootstrap._parse_control(bootstrap._canonical_json(_document(**overrides)))


def _copy_image_shaped_runtime(target_root: Path) -> None:
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    contract["files"] = {}
    for relative in EXPECTED_RUNTIME_FILES:
        payload = (ROOT / relative).read_bytes()
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        contract["files"][relative] = hashlib.sha256(payload).hexdigest()
    (target_root / "scrapers/whoscored/runtime_contract.lock").write_text(
        json.dumps(contract, sort_keys=True),
        encoding="utf-8",
    )


def _fatal_pth_line(call: str, *, wrapper_path: Path) -> str:
    program = (
        "try:\n"
        f" _path={str(wrapper_path)!r}\n"
        " _namespace={'__builtins__':__builtins__}\n"
        " exec(compile(open(_path,'rb').read(),_path,'exec'),_namespace)\n"
        f" _namespace['run']{call}\n"
        "except BaseException:\n"
        " sys.modules['posix']._exit(78)\n"
    )
    return f"import sys;exec({program!r},{{'sys':sys}})\n"


@pytest.mark.unit
def test_helper_imports_under_isolated_no_site_python():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            f"import runpy; runpy.run_path({str(HELPER_PATH)!r}, run_name='smoke')",
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")


@pytest.mark.unit
def test_main_rejects_every_control_path_override(monkeypatch):
    monkeypatch.setattr(sys, "argv", [str(HELPER_PATH), "/tmp/attacker.json"])
    monkeypatch.setattr(
        bootstrap,
        "_install_signal_handlers",
        lambda: None,
    )

    assert bootstrap.main() == bootstrap.EXIT_CONFIG


@pytest.mark.unit
def test_control_is_exact_canonical_and_binds_fixed_workflow():
    control = _control()

    assert control.worker_id == 2
    assert control.owner == "0123456789abcdef"
    assert control.argv[0] == bootstrap.WORKFLOW_SCRIPT

    pretty = json.dumps(_document(), sort_keys=True, indent=2).encode("utf-8")
    with pytest.raises(bootstrap.BootstrapError, match="not canonical"):
        bootstrap._parse_control(pretty)
    duplicate = bootstrap._canonical_json(_document()).replace(
        b'{"argv":', b'{"schema_version":1,"argv":', 1
    )
    with pytest.raises(bootstrap.BootstrapError, match="canonical JSON"):
        bootstrap._parse_control(duplicate)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"schema_version": True}, "schema version"),
        ({"worker_id": 4}, "worker id"),
        ({"owner": "UPPERCASEOWNER000"}, "owner"),
        ({"expected_python": "3.12"}, "Python version"),
        ({"expected_curl_cffi": "0.14.0"}, "curl_cffi version"),
        ({"argv": ["/tmp/worker.py"]}, "workflow command"),
        (
            {
                "argv": [
                    bootstrap.WORKFLOW_SCRIPT,
                    "--browser-session-owner=attacker",
                ]
            },
            "protected argument",
        ),
        (
            {"argv": [bootstrap.WORKFLOW_SCRIPT, "--flaresolverr-url", "x"]},
            "protected argument",
        ),
        (
            {"argv": [bootstrap.WORKFLOW_SCRIPT, "--capacity-control-fd=9"]},
            "protected argument",
        ),
        (
            {"argv": [bootstrap.WORKFLOW_SCRIPT, "--f", "http://attacker"]},
            "protected argument",
        ),
        (
            {"argv": [bootstrap.WORKFLOW_SCRIPT, "--"]},
            "protected argument",
        ),
    ],
)
def test_control_rejects_drift_and_protected_cli(override, message):
    with pytest.raises(bootstrap.BootstrapError, match=message):
        _control(**override)


@pytest.mark.unit
def test_control_and_fifo_metadata_are_root_owned_exact_types():
    control_metadata = SimpleNamespace(
        st_mode=stat.S_IFREG | 0o444,
        st_uid=0,
        st_gid=0,
        st_nlink=1,
        st_size=100,
    )
    fifo_metadata = SimpleNamespace(
        st_mode=stat.S_IFIFO | 0o640,
        st_uid=0,
        st_gid=0,
        st_nlink=1,
    )

    bootstrap._validate_control_metadata(control_metadata)
    bootstrap._validate_fifo_metadata(
        fifo_metadata,
        name="release.fifo",
        expected_mode=0o640,
    )
    control_metadata.st_uid = 50000
    with pytest.raises(bootstrap.BootstrapError, match="control file metadata"):
        bootstrap._validate_control_metadata(control_metadata)
    fifo_metadata.st_mode = stat.S_IFREG | 0o640
    with pytest.raises(bootstrap.BootstrapError, match="FIFO.*metadata"):
        bootstrap._validate_fifo_metadata(
            fifo_metadata,
            name="release.fifo",
            expected_mode=0o640,
        )


@pytest.mark.unit
def test_control_file_symlink_is_rejected(tmp_path):
    target = tmp_path / "target.json"
    target.write_bytes(bootstrap._canonical_json(_document()))
    (tmp_path / bootstrap.CONTROL_NAME).symlink_to(target)
    root_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(bootstrap.BootstrapError, match="unavailable"):
            bootstrap._read_control(root_fd)
    finally:
        os.close(root_fd)


@pytest.mark.unit
def test_preflight_uses_production_python_and_exact_image_identity(monkeypatch):
    captured: dict[str, Any] = {}
    expected = bootstrap._canonical_json(
        {
            "curl_cffi": "0.15.0",
            "python": "3.11",
            "runtime_class": "production-v1",
        }
    )

    class Probe:
        returncode = 0

        def communicate(self, *, timeout):
            captured["timeout"] = timeout
            return expected, b""

    def popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return Probe()

    monkeypatch.setattr(bootstrap.subprocess, "Popen", popen)

    bootstrap._run_preflight(_control())

    assert captured["command"][:5] == [
        "/usr/local/bin/python",
        "-E",
        "-P",
        "-B",
        "-u",
    ]
    assert captured["command"][5] == "-c"
    assert "_require_whoscored_runtime_class" in captured["command"][6]
    assert "import curl_cffi.requests" in captured["command"][6]
    assert "runtime_class = verifier(" in captured["command"][6]
    assert captured["kwargs"]["start_new_session"] is True
    assert captured["timeout"] == bootstrap.PREFLIGHT_TIMEOUT_SECONDS


@pytest.mark.unit
def test_preflight_calls_real_lazy_image_verifier_from_site_hook(tmp_path):
    runtime_root = tmp_path / "runtime"
    _copy_image_shaped_runtime(runtime_root)
    runtime_source = runtime_root / "scrapers/whoscored/runtime_contract.py"
    runtime_lock = runtime_root / "scrapers/whoscored/runtime_contract.lock"
    trust_root = tmp_path / "runtime-trust-root"
    trust_root.write_text(
        "\n".join(
            (
                "schema_version=1",
                "runtime_class=production-v1",
                "runtime_contract_source_sha256="
                + hashlib.sha256(runtime_source.read_bytes()).hexdigest(),
                "runtime_contract_lock_sha256="
                + hashlib.sha256(runtime_lock.read_bytes()).hexdigest(),
                "",
            )
        ),
        encoding="ascii",
    )
    gate_marker = tmp_path / "production-gate-called"
    gate = tmp_path / "whoscored-production-gate"
    gate.write_text(
        f"#!/bin/sh\nprintf x >> {gate_marker}\nexit 0\n",
        encoding="ascii",
    )
    gate.chmod(0o755)
    site_directory = tmp_path / "site-packages"
    site_directory.mkdir()
    wrapper = runtime_root / "docker/images/airflow/whoscored_runtime_pth.py"
    installed_wrapper = site_directory / "whoscored_runtime_pth.py"
    installed_wrapper.write_bytes(wrapper.read_bytes())
    startup = runtime_root / "docker/images/airflow/whoscored_runtime_startup.py"
    call = (
        "('direct',"
        f"startup_path={str(startup)!r},runtime_root={str(runtime_root)!r},"
        f"trust_root_path={str(trust_root)!r},"
        f"production_gate_path={str(gate)!r},"
        "require_full=True,enforce_trust_ownership=False)"
    )
    (site_directory / "00000000-whoscored-runtime-bootstrap.pth").write_text(
        _fatal_pth_line(call, wrapper_path=installed_wrapper),
        encoding="utf-8",
    )
    dependency_site = (
        Path(sys.prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    assert dependency_site.is_dir()
    # The production guard compares nanosecond file ctime with a process start
    # timestamp derived from kernel clock ticks.  Let the copied fixture age by
    # two ticks so quantization cannot make a pre-spawn file look post-spawn.
    time.sleep(max(0.02, 2 / os.sysconf("SC_CLK_TCK")))
    script = f"""
import site
import sys
sys.prefix = {str(sys.prefix)!r}
sys.exec_prefix = {str(sys.exec_prefix)!r}
sys.path.append({str(dependency_site)!r})
assert getattr(sys, '_whoscored_runtime_class', None) is None
site.addsitedir({str(site_directory)!r})
assert callable(getattr(sys, '_require_whoscored_runtime_class', None))
assert getattr(sys, '_whoscored_runtime_class', None) is None
exec(compile({bootstrap._PREFLIGHT_CODE!r}, '<capacity-preflight>', 'exec'))
"""

    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["runtime_class"] == "production-v1"
    assert result["curl_cffi"] == "0.15.0"
    assert gate_marker.read_text(encoding="ascii")


@pytest.mark.unit
def test_ready_then_one_release_byte_and_liveness_eof():
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    live_read, live_write = os.pipe()
    try:
        bootstrap._signal_ready(ready_write)
        ready_write = -1
        assert os.read(ready_read, 64) == b"READY\n"

        os.write(release_write, b"G")
        bootstrap._await_release(release_read, live_read)

        os.close(live_write)
        live_write = -1
        with pytest.raises(bootstrap.BootstrapError, match="liveness ended"):
            bootstrap._await_release(release_read, live_read)
    finally:
        for descriptor in (
            ready_read,
            ready_write,
            release_read,
            release_write,
            live_read,
            live_write,
        ):
            if descriptor >= 0:
                os.close(descriptor)


@pytest.mark.unit
def test_workflow_gets_protected_capacity_control_pipe(monkeypatch):
    captured: dict[str, Any] = {}
    monkeypatch.setenv("LD_PRELOAD", "/tmp/attacker.so")
    monkeypatch.setenv("LD_AUDIT", "/tmp/audit.so")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/libs")
    monkeypatch.setenv("WHOSCORED_SOURCE_CIRCUIT_PATH", "/tmp/attacker.json")
    monkeypatch.setenv("WHOSCORED_SOURCE_CIRCUIT_WAIT", "0")

    class Workflow:
        pass

    def popen(command, **kwargs):
        control_fd = kwargs["pass_fds"][0]
        captured["payload"] = os.read(control_fd, 512)
        captured["command"] = command
        captured["kwargs"] = kwargs
        return Workflow()

    monkeypatch.setattr(bootstrap.subprocess, "Popen", popen)

    bootstrap._start_workflow(_control())

    assert json.loads(captured["payload"]) == {
        "schema_version": 1,
        "owner": "0123456789abcdef",
        "flaresolverr_endpoint": "http://127.0.0.1:8191",
    }
    control_fd = captured["kwargs"]["pass_fds"][0]
    assert captured["command"][-2:] == ["--capacity-control-fd", str(control_fd)]
    assert captured["command"][:5] == [
        "/usr/local/bin/python",
        "-E",
        "-P",
        "-B",
        "-u",
    ]
    assert captured["kwargs"]["start_new_session"] is True
    assert "LD_PRELOAD" not in captured["kwargs"]["env"]
    assert "LD_AUDIT" not in captured["kwargs"]["env"]
    assert captured["kwargs"]["env"]["LD_LIBRARY_PATH"] == ""
    assert captured["kwargs"]["env"]["WHOSCORED_SOURCE_CIRCUIT_PATH"] == (
        "/run/whoscored-source/source-circuit-v1.json"
    )
    assert captured["kwargs"]["env"]["WHOSCORED_SOURCE_CIRCUIT_WAIT"] == "1"


@pytest.mark.unit
def test_cleanup_is_bounded_term_then_kill(monkeypatch):
    sent: list[int] = []

    class StuckProcess:
        waits = 0

        def poll(self):
            return None

        def wait(self, *, timeout):
            self.waits += 1
            if self.waits == 1:
                raise subprocess.TimeoutExpired("worker", timeout)
            return -signal.SIGKILL

    monkeypatch.setattr(
        bootstrap,
        "_send_process_group",
        lambda _process, signum: sent.append(signum),
    )

    assert bootstrap._bounded_cleanup(StuckProcess()) is True
    assert sent == [signal.SIGTERM, signal.SIGKILL]


@pytest.mark.unit
def test_orchestration_signals_ready_only_after_preflight(monkeypatch):
    events: list[str] = []
    descriptors = [os.open(os.devnull, os.O_RDONLY) for _ in range(5)]

    class FinishedWorkflow:
        def poll(self):
            return 0

    control = _control()
    monkeypatch.setattr(bootstrap, "_open_control_root", lambda: descriptors[0])
    monkeypatch.setattr(
        bootstrap,
        "_read_control",
        lambda _fd: events.append("control") or control,
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_preflight",
        lambda _control: events.append("preflight"),
    )
    monkeypatch.setattr(
        bootstrap,
        "_open_ready_directory",
        lambda _fd: descriptors[1],
    )
    fifo_descriptors = iter(descriptors[2:])
    monkeypatch.setattr(
        bootstrap,
        "_open_fifo_at",
        lambda *_args, **_kwargs: next(fifo_descriptors),
    )
    monkeypatch.setattr(
        bootstrap,
        "_assert_protocol_quiet",
        lambda *_args: events.append("quiet"),
    )

    def signal_ready(fd):
        events.append("ready")
        os.close(fd)

    monkeypatch.setattr(bootstrap, "_signal_ready", signal_ready)
    monkeypatch.setattr(
        bootstrap,
        "_await_release",
        lambda *_args: events.append("release"),
    )
    monkeypatch.setattr(bootstrap, "_liveness_events", lambda *_args: 0)
    monkeypatch.setattr(
        bootstrap,
        "_start_workflow",
        lambda _control: events.append("start") or FinishedWorkflow(),
    )
    monkeypatch.setattr(bootstrap, "_monitor_workflow", lambda *_args: 0)

    assert bootstrap.run() == 0
    assert events == ["control", "preflight", "quiet", "ready", "release", "start"]


@pytest.mark.unit
def test_docker_bakes_helper_only_into_scheduler_payload():
    dockerfile = (ROOT / "docker/images/airflow/Dockerfile").read_text(
        encoding="utf-8"
    )
    scheduler_payload = dockerfile.split(
        "FROM airflow-base AS airflow-scheduler-payload", 1
    )[1].split("FROM airflow-base AS airflow-whoscored-proxy-payload", 1)[0]

    assert (
        "COPY --chown=root:root whoscored_capacity_worker_bootstrap.py "
        "/usr/local/libexec/whoscored_capacity_worker_bootstrap.py"
        in scheduler_payload
    )
    assert (
        'stat -c \'%u:%g:%a:%h\' '
        "/usr/local/libexec/whoscored_capacity_worker_bootstrap.py"
        in scheduler_payload
    )
    assert "= \"0:0:444:1\"" in scheduler_payload
