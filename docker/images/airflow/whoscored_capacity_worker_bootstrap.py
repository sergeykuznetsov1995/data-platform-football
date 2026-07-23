#!/usr/bin/env python3
"""Fail-closed bootstrap for one image-backed WhoScored capacity worker.

The supervisor supplies one root-owned control file and four root-owned FIFO
endpoints through the fixed ``/run/whoscored-capacity`` mount.  This helper is
image-owned and intentionally imports only the Python standard library so it
can itself run under ``python -I -S``.
"""

from __future__ import annotations

import json
import os
import re
import select
import signal
import stat
import subprocess
import sys
from dataclasses import dataclass
from types import FrameType
from typing import Any


CONTROL_ROOT = "/run/whoscored-capacity"
CONTROL_NAME = "control.json"
CONTROL_PATH = f"{CONTROL_ROOT}/{CONTROL_NAME}"
READY_DIRECTORY_NAME = "ready"
RELEASE_FIFO_NAME = "release.fifo"
LIVENESS_FIFO_NAME = "liveness.fifo"
PRODUCTION_PYTHON = "/usr/local/bin/python"
WORKFLOW_SCRIPT = "/opt/airflow/scripts/research/bench_whoscored_workflow.py"
EXPECTED_PYTHON = "3.11"
EXPECTED_CURL_CFFI = "0.15.0"
EXPECTED_RUNTIME_CLASS = "production-v1"

EXIT_SOFTWARE = 70
EXIT_CONFIG = 78
READY_PAYLOAD = b"READY\n"
RELEASE_BYTE = b"G"
CONTROL_MODE = 0o444
READY_FIFO_MODE = 0o620
INPUT_FIFO_MODE = 0o640
POLL_INTERVAL_MS = 100
PREFLIGHT_TIMEOUT_SECONDS = 30.0
TERM_GRACE_SECONDS = 5.0
KILL_GRACE_SECONDS = 2.0
MAX_CONTROL_BYTES = 64 * 1024
MAX_ARGV_ITEMS = 64
MAX_ARG_BYTES = 4096
MAX_ARGV_BYTES = 32 * 1024
CAPACITY_FLARESOLVERR_ENDPOINT = "http://127.0.0.1:8191"
CACHE_CAPACITY_MODE = "cache-capacity-v1"
DIRECT_DIAGNOSTIC_MODE = "direct-diagnostic-v1"
SOURCE_CIRCUIT_ROOT = "/run/whoscored-source"
SOURCE_CIRCUIT_PATH = f"{SOURCE_CIRCUIT_ROOT}/source-circuit-v1.json"

_OWNER = re.compile(r"\A[a-z0-9]{16,32}\Z")
_CONTROL_FIELDS = {
    "schema_version",
    "worker_id",
    "owner",
    "expected_python",
    "expected_curl_cffi",
    "argv",
}
_FORBIDDEN_WORKFLOW_FLAGS = (
    "--browser-session-owner",
    "--capacity-control-fd",
    "--flaresolverr-url",
)
_POLL_FAILURE = select.POLLERR | select.POLLHUP | select.POLLNVAL
_STOP_SIGNAL: int | None = None

_PREFLIGHT_CODE = """\
import importlib.metadata as metadata
import json
import sys
verifier = getattr(sys, "_require_whoscored_runtime_class", None)
if not callable(verifier):
    raise RuntimeError("WhoScored image runtime verifier is unavailable")
runtime_class = verifier(
    "production-v1", "WhoScored capacity worker image preflight"
)
if runtime_class != "production-v1":
    raise RuntimeError("WhoScored capacity worker image is not production-v1")
import curl_cffi
import curl_cffi.requests
result = {
    "curl_cffi": metadata.version("curl_cffi"),
    "python": f"{sys.version_info.major}.{sys.version_info.minor}",
    "runtime_class": runtime_class,
}
sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\\n")
"""


class BootstrapError(RuntimeError):
    """Raised when the fixed worker bootstrap contract is not satisfied."""


class _DuplicateKey(ValueError):
    pass


@dataclass(frozen=True)
class Control:
    worker_id: int
    owner: str
    mode: str
    argv: tuple[str, ...]


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey(key)
        value[key] = item
    return value


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _parse_control(raw: bytes) -> Control:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BootstrapError("capacity control is not valid canonical JSON") from exc
    if not isinstance(value, dict) or set(value) != _CONTROL_FIELDS:
        raise BootstrapError("capacity control schema is invalid")
    if raw != _canonical_json(value):
        raise BootstrapError("capacity control bytes are not canonical")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise BootstrapError("capacity control schema version is invalid")
    worker_id = value["worker_id"]
    if type(worker_id) is not int or not 0 <= worker_id <= 3:
        raise BootstrapError("capacity worker id is invalid")
    owner = value["owner"]
    if not isinstance(owner, str) or _OWNER.fullmatch(owner) is None:
        raise BootstrapError("capacity owner is invalid")
    if value["expected_python"] != EXPECTED_PYTHON:
        raise BootstrapError("capacity Python version contract is invalid")
    if value["expected_curl_cffi"] != EXPECTED_CURL_CFFI:
        raise BootstrapError("capacity curl_cffi version contract is invalid")

    argv = value["argv"]
    if (
        not isinstance(argv, list)
        or not 1 <= len(argv) <= MAX_ARGV_ITEMS
        or argv[0] != WORKFLOW_SCRIPT
    ):
        raise BootstrapError("capacity workflow command is invalid")
    total = 0
    for argument in argv:
        if not isinstance(argument, str) or not argument:
            raise BootstrapError("capacity workflow argument is invalid")
        encoded = argument.encode("utf-8")
        if (
            len(encoded) > MAX_ARG_BYTES
            or "\x00" in argument
            or any(ord(character) < 0x20 for character in argument)
        ):
            raise BootstrapError("capacity workflow argument is unsafe")
        total += len(encoded) + 1
        option = argument.split("=", 1)[0]
        if argument == "--" or (
            option.startswith("--")
            and any(
                flag.startswith(option) or option == flag
                for flag in _FORBIDDEN_WORKFLOW_FLAGS
            )
        ):
            raise BootstrapError("capacity workflow contains a protected argument")
    if total > MAX_ARGV_BYTES:
        raise BootstrapError("capacity workflow command is too large")
    if argv.count("--mode") != 1:
        raise BootstrapError("capacity workflow mode is invalid")
    mode_index = argv.index("--mode") + 1
    if mode_index >= len(argv) or argv[mode_index] not in {
        CACHE_CAPACITY_MODE,
        DIRECT_DIAGNOSTIC_MODE,
    }:
        raise BootstrapError("capacity workflow mode is invalid")
    return Control(
        worker_id=worker_id,
        owner=owner,
        mode=argv[mode_index],
        argv=tuple(argv),
    )


def _stable_identity(before: os.stat_result, after: os.stat_result) -> bool:
    fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    return all(getattr(before, field) == getattr(after, field) for field in fields)


def _validate_directory(metadata: os.stat_result, *, label: str) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise BootstrapError(f"{label} directory is not root-owned and immutable")


def _validate_control_metadata(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != CONTROL_MODE
        or metadata.st_nlink != 1
        or not 0 < metadata.st_size <= MAX_CONTROL_BYTES
    ):
        raise BootstrapError("capacity control file metadata is invalid")


def _validate_fifo_metadata(
    metadata: os.stat_result, *, name: str, expected_mode: int
) -> None:
    if (
        not stat.S_ISFIFO(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != expected_mode
        or metadata.st_nlink != 1
    ):
        raise BootstrapError(f"capacity FIFO {name} metadata is invalid")


def _open_control_root() -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(CONTROL_ROOT, flags)
        _validate_directory(os.fstat(descriptor), label="capacity control")
        return descriptor
    except BaseException:
        if "descriptor" in locals():
            os.close(descriptor)
        raise


def _read_control(root_fd: int) -> Control:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(CONTROL_NAME, flags, dir_fd=root_fd)
    except OSError as exc:
        raise BootstrapError("capacity control file is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        _validate_control_metadata(before)
        chunks: list[bytes] = []
        remaining = MAX_CONTROL_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(8192, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
    except OSError as exc:
        raise BootstrapError("capacity control file cannot be read") from exc
    finally:
        os.close(descriptor)
    if not _stable_identity(before, after):
        raise BootstrapError("capacity control changed while it was read")
    raw = b"".join(chunks)
    if len(raw) != before.st_size:
        raise BootstrapError("capacity control size changed while it was read")
    return _parse_control(raw)


def _open_ready_directory(root_fd: int) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(READY_DIRECTORY_NAME, flags, dir_fd=root_fd)
        _validate_directory(os.fstat(descriptor), label="capacity ready")
        return descriptor
    except BaseException:
        if "descriptor" in locals():
            os.close(descriptor)
        raise


def _open_fifo_at(
    directory_fd: int,
    name: str,
    *,
    write: bool,
    expected_mode: int,
) -> int:
    flags = (os.O_WRONLY if write else os.O_RDONLY) | os.O_NONBLOCK
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise BootstrapError(f"capacity FIFO {name} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        _validate_fifo_metadata(metadata, name=name, expected_mode=expected_mode)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _child_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for name in ("LD_AUDIT", "LD_PRELOAD"):
        environment.pop(name, None)
    environment["LD_LIBRARY_PATH"] = ""
    environment["WHOSCORED_SOURCE_CIRCUIT_PATH"] = SOURCE_CIRCUIT_PATH
    environment["WHOSCORED_SOURCE_CIRCUIT_WAIT"] = "1"
    # Scope rows can exceed the deliberately small /tmp tmpfs.  Keep their
    # SQLite spools on the already admitted, owner-private host-disk bind;
    # the supervisor removes the whole owner tree after every cohort.
    environment["WHOSCORED_SCOPE_SPOOL_DIR"] = SOURCE_CIRCUIT_ROOT
    return environment


def _production_command(argv: tuple[str, ...]) -> list[str]:
    return [PRODUCTION_PYTHON, "-E", "-P", "-B", "-u", *argv]


def _workflow_directory() -> str:
    """Return the immutable runtime root containing the baked workflow."""

    return os.path.dirname(os.path.dirname(os.path.dirname(WORKFLOW_SCRIPT)))


def _send_process_group(process: subprocess.Popen[bytes], sig: int) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        return


def _bounded_cleanup(process: subprocess.Popen[bytes]) -> bool:
    if process.poll() is not None:
        return True
    _send_process_group(process, signal.SIGTERM)
    try:
        process.wait(timeout=TERM_GRACE_SECONDS)
        return True
    except subprocess.TimeoutExpired:
        pass
    _send_process_group(process, signal.SIGKILL)
    try:
        process.wait(timeout=KILL_GRACE_SECONDS)
        return True
    except subprocess.TimeoutExpired:
        return False


def _run_preflight(control: Control) -> None:
    del control
    expected = _canonical_json(
        {
            "curl_cffi": EXPECTED_CURL_CFFI,
            "python": EXPECTED_PYTHON,
            "runtime_class": EXPECTED_RUNTIME_CLASS,
        }
    )
    command = _production_command(("-c", _PREFLIGHT_CODE))
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/opt/airflow",
            env=_child_environment(),
            close_fds=True,
            start_new_session=True,
        )
    except OSError as exc:
        raise BootstrapError("capacity production Python preflight cannot start") from exc
    try:
        stdout, _stderr = process.communicate(timeout=PREFLIGHT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as exc:
        if not _bounded_cleanup(process):
            raise BootstrapError("capacity production Python preflight cleanup failed") from exc
        raise BootstrapError("capacity production Python preflight timed out") from exc
    if process.returncode != 0 or stdout != expected:
        raise BootstrapError("capacity production Python preflight failed")


def _protocol_events(release_fd: int, liveness_fd: int, timeout_ms: int) -> dict[int, int]:
    poller = select.poll()
    poller.register(release_fd, select.POLLIN | _POLL_FAILURE)
    poller.register(liveness_fd, select.POLLIN | _POLL_FAILURE)
    return dict(poller.poll(timeout_ms))


def _liveness_events(liveness_fd: int, timeout_ms: int) -> int:
    poller = select.poll()
    poller.register(liveness_fd, select.POLLIN | _POLL_FAILURE)
    return dict(poller.poll(timeout_ms)).get(liveness_fd, 0)


def _check_liveness_event(liveness_fd: int, event: int) -> None:
    if event & select.POLLNVAL:
        raise BootstrapError("capacity liveness FIFO became invalid")
    if event & (select.POLLIN | select.POLLHUP | select.POLLERR):
        try:
            payload = os.read(liveness_fd, 1)
        except BlockingIOError:
            payload = b""
        if payload:
            raise BootstrapError("capacity liveness FIFO carried unexpected data")
        raise BootstrapError("capacity supervisor liveness ended")


def _assert_protocol_quiet(release_fd: int, liveness_fd: int) -> None:
    events = _protocol_events(release_fd, liveness_fd, 0)
    if liveness_fd in events:
        _check_liveness_event(liveness_fd, events[liveness_fd])
    if release_fd in events:
        raise BootstrapError("capacity release arrived before readiness")


def _signal_ready(ready_fd: int) -> None:
    try:
        if os.write(ready_fd, READY_PAYLOAD) != len(READY_PAYLOAD):
            raise BootstrapError("capacity readiness write was incomplete")
    except OSError as exc:
        raise BootstrapError("capacity readiness cannot be signalled") from exc
    finally:
        os.close(ready_fd)


def _await_release(release_fd: int, liveness_fd: int) -> None:
    while True:
        if _STOP_SIGNAL is not None:
            raise BootstrapError("capacity worker was stopped before release")
        events = _protocol_events(release_fd, liveness_fd, POLL_INTERVAL_MS)
        if liveness_fd in events:
            _check_liveness_event(liveness_fd, events[liveness_fd])
        release_event = events.get(release_fd, 0)
        if not release_event:
            continue
        if release_event & (select.POLLERR | select.POLLNVAL):
            raise BootstrapError("capacity release FIFO failed")
        if release_event & select.POLLIN:
            try:
                payload = os.read(release_fd, 1)
            except BlockingIOError:
                continue
            if payload != RELEASE_BYTE:
                raise BootstrapError("capacity release byte does not match")
            return
        if release_event & select.POLLHUP:
            raise BootstrapError("capacity release FIFO closed before release")


def _normal_exit_code(returncode: int) -> int:
    if returncode >= 0:
        return min(returncode, 255)
    return min(128 + abs(returncode), 255)


def _monitor_workflow(process: subprocess.Popen[bytes], liveness_fd: int) -> int:
    poller = select.poll()
    poller.register(liveness_fd, select.POLLIN | _POLL_FAILURE)
    while True:
        if _STOP_SIGNAL is not None:
            cleaned = _bounded_cleanup(process)
            return (128 + _STOP_SIGNAL) if cleaned else EXIT_SOFTWARE
        returncode = process.poll()
        if returncode is not None:
            return _normal_exit_code(returncode)
        events = dict(poller.poll(POLL_INTERVAL_MS))
        if liveness_fd in events:
            try:
                _check_liveness_event(liveness_fd, events[liveness_fd])
            except BootstrapError:
                if not _bounded_cleanup(process):
                    return EXIT_SOFTWARE
                return EXIT_SOFTWARE


def _start_workflow(control: Control) -> subprocess.Popen[bytes]:
    if control.mode == CACHE_CAPACITY_MODE:
        try:
            return subprocess.Popen(
                _production_command(control.argv),
                stdin=subprocess.DEVNULL,
                cwd=_workflow_directory(),
                env=_child_environment(),
                close_fds=True,
                start_new_session=True,
            )
        except OSError as exc:
            raise BootstrapError("capacity workflow cannot start") from exc
    if control.mode != DIRECT_DIAGNOSTIC_MODE:
        raise BootstrapError("capacity workflow mode is invalid")
    payload = _canonical_json(
        {
            "flaresolverr_endpoint": CAPACITY_FLARESOLVERR_ENDPOINT,
            "owner": control.owner,
            "schema_version": 1,
        }
    )
    pipe_flags = getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "pipe2"):
        read_fd, write_fd = os.pipe2(pipe_flags)
    else:  # pragma: no cover - the production image is Linux
        read_fd, write_fd = os.pipe()
    try:
        if read_fd < 3 or write_fd < 3:
            raise BootstrapError("capacity workflow control descriptors are unsafe")
        if os.write(write_fd, payload) != len(payload):
            raise BootstrapError("capacity workflow control write was incomplete")
        os.close(write_fd)
        write_fd = -1
        command = (
            *control.argv,
            "--capacity-control-fd",
            str(read_fd),
        )
        return subprocess.Popen(
            _production_command(command),
            stdin=subprocess.DEVNULL,
            cwd=_workflow_directory(),
            env=_child_environment(),
            close_fds=True,
            pass_fds=(read_fd,),
            start_new_session=True,
        )
    except BootstrapError:
        raise
    except OSError as exc:
        raise BootstrapError("capacity workflow cannot start") from exc
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


def _remember_signal(signum: int, _frame: FrameType | None) -> None:
    global _STOP_SIGNAL
    if _STOP_SIGNAL is None:
        _STOP_SIGNAL = signum


def _install_signal_handlers() -> None:
    for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, _remember_signal)


def run() -> int:
    root_fd = _open_control_root()
    ready_directory_fd: int | None = None
    release_fd: int | None = None
    liveness_fd: int | None = None
    ready_fd: int | None = None
    process: subprocess.Popen[bytes] | None = None
    try:
        control = _read_control(root_fd)
        _run_preflight(control)
        ready_directory_fd = _open_ready_directory(root_fd)
        release_fd = _open_fifo_at(
            root_fd,
            RELEASE_FIFO_NAME,
            write=False,
            expected_mode=INPUT_FIFO_MODE,
        )
        liveness_fd = _open_fifo_at(
            root_fd,
            LIVENESS_FIFO_NAME,
            write=False,
            expected_mode=INPUT_FIFO_MODE,
        )
        ready_fd = _open_fifo_at(
            ready_directory_fd,
            f"{control.worker_id}.fifo",
            write=True,
            expected_mode=READY_FIFO_MODE,
        )
        _assert_protocol_quiet(release_fd, liveness_fd)
        _signal_ready(ready_fd)
        ready_fd = None
        _await_release(release_fd, liveness_fd)
        os.close(release_fd)
        release_fd = None
        liveness_event = _liveness_events(liveness_fd, 0)
        if liveness_event:
            _check_liveness_event(liveness_fd, liveness_event)
        process = _start_workflow(control)
        result = _monitor_workflow(process, liveness_fd)
        # The monitor either reaped the workflow or exhausted the one bounded
        # TERM/KILL sequence.  Do not silently double that cleanup budget.
        process = None
        return result
    finally:
        if process is not None and process.poll() is None:
            _bounded_cleanup(process)
        for descriptor in (
            ready_fd,
            liveness_fd,
            release_fd,
            ready_directory_fd,
            root_fd,
        ):
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def main() -> int:
    _install_signal_handlers()
    if sys.argv[1:] != [CONTROL_PATH]:
        print(
            "WhoScored capacity worker bootstrap failed: fixed control path is required",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    try:
        return run()
    except (BootstrapError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"WhoScored capacity worker bootstrap failed: {exc}", file=sys.stderr)
        return EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
