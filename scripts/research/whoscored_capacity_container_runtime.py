#!/usr/bin/env python3
"""Fail-closed host lifecycle for four disposable capacity containers.

This module deliberately does not know how to build an image.  The caller must
provide the admitted scheduler image ID (``sha256:<64 lowercase hex>``) and the
admitted FlareSolverr container ID (``64 lowercase hex``).  Workers therefore
use the scheduler image's Python and native libraries, and share only the
network namespace of that exact FlareSolverr container.

The scheduler image must contain the baked production gate and bootstrap named
below. Docker starts ``dumb-init -> production-entrypoint -> isolated Python ->
bootstrap`` so the image validates its production runtime before the workload.
The bootstrap contract is intentionally small:

1. open the common liveness FIFO for reading and fail on host-side EOF/data;
2. open the common release FIFO for reading;
3. write exactly ``READY\\n`` to its private ready FIFO;
4. read exactly one byte and require it to be ``G``;
5. run only the workload argv admitted by its canonical control JSON.

The host creates all four containers, verifies their complete hardening, starts
and verifies all four, waits for all READY records, and releases the cohort with
one atomic ``GGGG`` write.  A successful slot is then removed by its exact ID,
reported immediately, replaced with iteration + 1, re-admitted, and released
without waiting for slower slots. Cleanup never searches by label, prefix,
glob, or partial ID.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import stat
import subprocess
import threading
import time
from typing import BinaryIO, Callable, Mapping, Protocol, Sequence


DOCKER_CLI = "/usr/bin/docker"
DOCKER_ENVIRONMENT = {
    "HOME": "/nonexistent",
    "PATH": "/usr/bin:/bin",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "DOCKER_HOST": "unix:///run/docker.sock",
}
CONTAINER_ENTRYPOINT = "/usr/bin/dumb-init"
PRODUCTION_ENTRYPOINT = "/usr/local/bin/whoscored-production-entrypoint"
PRODUCTION_PYTHON = "/usr/local/bin/python"
BOOTSTRAP_PATH = "/usr/local/libexec/whoscored_capacity_worker_bootstrap.py"
CONTAINER_RUNTIME_ROOT = "/opt/airflow"
CONTAINER_SOURCE_CIRCUIT_ROOT = "/run/whoscored-source"
CONTAINER_SOURCE_CIRCUIT = (
    f"{CONTAINER_SOURCE_CIRCUIT_ROOT}/source-circuit-v1.json"
)
CONTAINER_CONTROL_ROOT = "/run/whoscored-capacity"
CONTAINER_READY_ROOT = f"{CONTAINER_CONTROL_ROOT}/ready"
CONTAINER_CONTROL_JSON = f"{CONTAINER_CONTROL_ROOT}/control.json"
HOST_ARTIFACT_ROOT = Path("/tmp")
HOST_RUNTIME_OWNER_PREFIX = "whoscored-capacity-runtime-"
HOST_CONTROL_ROOT_PREFIX = "whoscored-capacity-control-"
CONTROL_SCHEMA_VERSION = 1
EXPECTED_PYTHON = "3.11"
EXPECTED_CURL_CFFI = "0.15.0"
WORKLOAD_PATH = "/opt/airflow/scripts/research/bench_whoscored_workflow.py"
WORKER_COUNT = 4
MEMORY_BYTES = 2 * 1024**3
PIDS_LIMIT = 128
WORKER_TMPFS_OPTIONS = (
    "rw,noexec,nosuid,nodev,size=256m,uid=50000,gid=0,mode=0700"
)
READY_PAYLOAD = b"READY\n"
RELEASE_PAYLOAD = b"G" * WORKER_COUNT
MAX_WORKER_STDOUT_BYTES = 2 * 1024 * 1024
MAX_WORKER_STDERR_BYTES = 512 * 1024

_IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{64}\Z")
_OWNER_RE = re.compile(r"[a-z0-9]{16,32}\Z")
_WORKLOAD_TOKEN_RE = re.compile(r"[^\x00\r\n]+\Z")
_MEMORY_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)(B|kB|KB|KiB|MB|MiB|GB|GiB)\Z")
_LABEL_OWNER = "io.data-platform.whoscored.capacity.owner"
_LABEL_INDEX = "io.data-platform.whoscored.capacity.worker-index"
_LABEL_RUNTIME = "io.data-platform.whoscored.capacity.runtime"
_RUNTIME_LABEL = "container-runtime-v1"


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


class ContainerRuntimeError(RuntimeError):
    """The cohort could not be proved safe."""


class StaleContainerError(ContainerRuntimeError):
    """An exact deterministic worker name already exists."""

    def __init__(self, container_ids: Sequence[str]) -> None:
        self.container_ids = tuple(container_ids)
        super().__init__(
            "stale capacity containers require explicit exact-ID cleanup: "
            + ",".join(self.container_ids)
        )


@dataclass(frozen=True)
class CommandResult:
    """Minimal subprocess result used by the injectable command runner."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def __call__(
        self, argv: tuple[str, ...], timeout_seconds: float | None
    ) -> CommandResult: ...


class AttachProcess(Protocol):
    stdout: BinaryIO | None
    stderr: BinaryIO | None

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


AttachFactory = Callable[[tuple[str, ...]], AttachProcess]


@dataclass(frozen=True)
class WorkerSpec:
    """One workload command; worker indexes in a cohort must be exactly 0..3."""

    worker_index: int
    workload_argv: tuple[str, ...]
    iteration: int = 0


@dataclass(frozen=True)
class ContainerSnapshot:
    """A sampled, identity-bound state for one worker container."""

    worker_index: int
    iteration: int
    container_id: str
    status: str
    running: bool
    exit_code: int
    oom_killed: bool
    memory_usage_bytes: int
    pids_current: int


@dataclass(frozen=True)
class CohortSample:
    """One observation containing only the four stored full IDs."""

    monotonic_seconds: float
    containers: tuple[ContainerSnapshot, ...]


@dataclass(frozen=True)
class WorkerResult:
    """Bounded output evidence from one attached worker."""

    worker_index: int
    iteration: int
    container_id: str
    attach_returncode: int | None
    stdout_bytes: bytes
    stdout_json: Mapping[str, object] | None
    stderr_bytes: int
    stderr_sha256: str
    output_complete: bool


@dataclass(frozen=True)
class Outcome:
    """Final lifecycle result delivered once to ``on_outcome``.

    ``status`` is one of ``completed``, ``stopped``, ``deadline``, or ``failed``.
    ``cleanup_complete`` is false if any exact-ID cleanup could not be proved.
    In refill mode, successful results are streamed to ``on_worker_result``;
    ``worker_results`` contains only the workers present at final cleanup.
    """

    status: str
    reason: str
    released: bool
    container_ids: tuple[str, ...]
    exit_codes: tuple[int | None, ...]
    worker_results: tuple[WorkerResult, ...]
    cleanup_complete: bool


@dataclass(frozen=True)
class _ContainerRecord:
    container_id: str
    scheduler_image_id: str
    flaresolverr_container_id: str
    owner: str
    spec: WorkerSpec
    runtime_root: Path
    source_circuit_root: Path
    control_root: Path
    control_json: Path


BeforeRelease = Callable[[], None]
OnSample = Callable[[CohortSample], None]
BooleanCallback = Callable[[], bool]
OnOutcome = Callable[[Outcome], None]
OnWorkerResult = Callable[[WorkerResult], None]
ReplacementWorker = Callable[[WorkerSpec], WorkerSpec | None]


def _validate_docker_cli() -> None:
    try:
        metadata = os.lstat(DOCKER_CLI)
    except OSError as exc:
        raise ContainerRuntimeError("/usr/bin/docker is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or not metadata.st_mode & stat.S_IXUSR
    ):
        raise ContainerRuntimeError("/usr/bin/docker metadata is unsafe")


def _default_runner(
    argv: tuple[str, ...], timeout_seconds: float | None
) -> CommandResult:
    if not argv or argv[0] != DOCKER_CLI:
        raise ContainerRuntimeError("only /usr/bin/docker may be executed")
    _validate_docker_cli()
    try:
        completed = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=DOCKER_ENVIRONMENT,
            cwd="/",
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ContainerRuntimeError(f"docker command failed safely: {exc}") from exc
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _default_attach_factory(argv: tuple[str, ...]) -> AttachProcess:
    if (
        len(argv) != 5
        or argv[:4] != (DOCKER_CLI, "container", "start", "--attach")
        or not _CONTAINER_ID_RE.fullmatch(argv[4])
    ):
        raise ContainerRuntimeError("invalid attached docker command")
    _validate_docker_cli()
    try:
        return subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=DOCKER_ENVIRONMENT,
            cwd="/",
            start_new_session=True,
        )
    except OSError as exc:
        raise ContainerRuntimeError("attached docker start failed safely") from exc


class _CapturedAttach:
    def __init__(
        self,
        process: AttachProcess,
        *,
        worker_index: int,
        iteration: int,
        container_id: str,
    ) -> None:
        if process.stdout is None or process.stderr is None:
            raise ContainerRuntimeError("attach process did not expose both pipes")
        self.process = process
        self.worker_index = worker_index
        self.iteration = iteration
        self.container_id = container_id
        self._stdout = bytearray()
        self._stderr = bytearray()
        self._stderr_total = 0
        self._stderr_hash = hashlib.sha256()
        self._overflow = False
        self._threads = (
            threading.Thread(
                target=self._drain,
                args=(process.stdout, self._stdout, MAX_WORKER_STDOUT_BYTES, None),
                name=f"whoscored-stdout-{worker_index}",
                daemon=True,
            ),
            threading.Thread(
                target=self._drain,
                args=(
                    process.stderr,
                    self._stderr,
                    MAX_WORKER_STDERR_BYTES,
                    self._stderr_hash,
                ),
                name=f"whoscored-stderr-{worker_index}",
                daemon=True,
            ),
        )
        for thread in self._threads:
            thread.start()

    def _drain(
        self,
        stream: BinaryIO,
        destination: bytearray,
        limit: int,
        digest: object | None,
    ) -> None:
        try:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    self._overflow = True
                    break
                if digest is not None:
                    digest.update(chunk)  # type: ignore[attr-defined]
                    self._stderr_total += len(chunk)
                remaining = max(0, limit - len(destination))
                destination.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    self._overflow = True
        except OSError:
            self._overflow = True
        finally:
            try:
                stream.close()
            except OSError:
                pass

    def result(self) -> WorkerResult:
        returncode = self.process.poll()
        if returncode is None:
            try:
                returncode = self.process.wait(timeout=5.0)
            except (OSError, subprocess.TimeoutExpired, TimeoutError):
                self._overflow = True
                try:
                    self.process.terminate()
                    returncode = self.process.wait(timeout=2.0)
                except (OSError, subprocess.TimeoutExpired, TimeoutError):
                    try:
                        self.process.kill()
                        returncode = self.process.wait(timeout=2.0)
                    except (OSError, subprocess.TimeoutExpired, TimeoutError):
                        returncode = None
        for thread in self._threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                self._overflow = True
        stdout = bytes(self._stdout)
        parsed: Mapping[str, object] | None = None
        try:
            candidate = json.loads(
                stdout.decode("utf-8"), object_pairs_hook=_unique_json_object
            )
            if isinstance(candidate, dict):
                parsed = candidate
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            pass
        return WorkerResult(
            worker_index=self.worker_index,
            iteration=self.iteration,
            container_id=self.container_id,
            attach_returncode=returncode,
            stdout_bytes=stdout,
            stdout_json=parsed,
            stderr_bytes=self._stderr_total,
            stderr_sha256=self._stderr_hash.hexdigest(),
            output_complete=not self._overflow,
        )


def _docker(
    runner: CommandRunner,
    *arguments: str,
    timeout_seconds: float | None = 15.0,
    check: bool = True,
) -> CommandResult:
    result = runner((DOCKER_CLI, *arguments), timeout_seconds)
    if check and result.returncode != 0:
        detail = result.stderr.strip()[:300]
        raise ContainerRuntimeError(
            f"docker {' '.join(arguments[:2])} failed ({result.returncode}): {detail}"
        )
    return result


def _validate_input_path(
    path: Path,
    *,
    directory: bool,
    owner: tuple[int, int] | None = None,
    exact_mode: int | None = None,
    reject_group_write: bool = True,
) -> Path:
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("bind paths must be absolute")
    try:
        before = path.lstat()
        resolved = path.resolve(strict=True)
        after = path.lstat()
    except OSError as exc:
        raise ValueError(f"bind path is unavailable: {path}") from exc
    if stat.S_ISLNK(before.st_mode) or (before.st_dev, before.st_ino) != (
        after.st_dev,
        after.st_ino,
    ):
        raise ValueError(f"bind path changed or is a symlink: {path}")
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected(before.st_mode):
        raise ValueError(f"bind path has the wrong type: {path}")
    if owner is not None and (before.st_uid, before.st_gid) != owner:
        raise ValueError(f"bind path has the wrong owner: {path}")
    mode = stat.S_IMODE(before.st_mode)
    if exact_mode is not None and mode != exact_mode:
        raise ValueError(f"bind path has the wrong mode: {path}")
    forbidden_write = stat.S_IWOTH | (stat.S_IWGRP if reject_group_write else 0)
    if before.st_mode & forbidden_write:
        raise ValueError(f"bind path is group/world writable: {path}")
    if resolved != path:
        raise ValueError(f"bind path must already be canonical: {path}")
    return resolved


def _validate_inputs(
    scheduler_image_id: str,
    flaresolverr_container_id: str,
    owner: str,
    workers: Sequence[WorkerSpec],
    runtime_root: Path,
    source_circuit_root: Path,
) -> tuple[tuple[WorkerSpec, ...], Path, Path]:
    if not _IMAGE_ID_RE.fullmatch(scheduler_image_id):
        raise ValueError("scheduler image must be an exact sha256 image ID")
    if not _CONTAINER_ID_RE.fullmatch(flaresolverr_container_id):
        raise ValueError("FlareSolverr must be an exact full container ID")
    if not _OWNER_RE.fullmatch(owner):
        raise ValueError("owner must contain 16-32 lowercase letters/digits")
    normalized = tuple(workers)
    if len(normalized) != WORKER_COUNT:
        raise ValueError("exactly four workers are required")
    if tuple(item.worker_index for item in normalized) != tuple(range(WORKER_COUNT)):
        raise ValueError("worker indexes must be ordered exactly 0,1,2,3")
    for item in normalized:
        _validate_worker_spec(item)
    return (
        normalized,
        _validate_input_path(
            runtime_root,
            directory=True,
            owner=(0, 0),
        ),
        _validate_input_path(
            source_circuit_root,
            directory=True,
            owner=(0, 0),
            exact_mode=0o770,
            reject_group_write=False,
        ),
    )


def _validate_worker_spec(spec: WorkerSpec) -> None:
    if type(spec.worker_index) is not int or not 0 <= spec.worker_index < WORKER_COUNT:
        raise ValueError("worker index is invalid")
    if type(spec.iteration) is not int or spec.iteration < 0:
        raise ValueError("worker iteration is invalid")
    if not spec.workload_argv:
        raise ValueError("worker workload argv may not be empty")
    if any(
        not isinstance(token, str)
        or not token
        or not _WORKLOAD_TOKEN_RE.fullmatch(token)
        for token in spec.workload_argv
    ):
        raise ValueError("worker argv contains an invalid token")
    if spec.workload_argv[0] != WORKLOAD_PATH:
        raise ValueError("worker argv must start with the baked workflow path")
    for token in spec.workload_argv[1:]:
        if token in {
            "--browser-session-owner",
            "--flaresolverr-url",
            "--capacity-control-fd",
        } or token.startswith(
            (
                "--browser-session-owner=",
                "--flaresolverr-url=",
                "--capacity-control-fd=",
            )
        ):
            raise ValueError("worker argv attempts to override sealed control")


def _container_name(owner: str, worker_index: int) -> str:
    return f"whoscored-capacity-{owner}-{worker_index}"


def _host_artifact_paths(owner: str) -> tuple[Path, Path]:
    if not _OWNER_RE.fullmatch(owner):
        raise ValueError("invalid owner")
    return (
        HOST_ARTIFACT_ROOT / f"{HOST_RUNTIME_OWNER_PREFIX}{owner}",
        HOST_ARTIFACT_ROOT / f"{HOST_CONTROL_ROOT_PREFIX}{owner}",
    )


def _bootstrap_argv() -> tuple[str, ...]:
    return (
        "--",
        PRODUCTION_ENTRYPOINT,
        PRODUCTION_PYTHON,
        "-I",
        "-S",
        "-B",
        "-u",
        BOOTSTRAP_PATH,
        CONTAINER_CONTROL_JSON,
    )


def _control_document(owner: str, spec: WorkerSpec) -> bytes:
    payload = {
        "argv": list(spec.workload_argv),
        "expected_curl_cffi": EXPECTED_CURL_CFFI,
        "expected_python": EXPECTED_PYTHON,
        "owner": owner,
        "schema_version": CONTROL_SCHEMA_VERSION,
        "worker_id": spec.worker_index,
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _mount_argument(source: Path, target: str, *, readonly: bool) -> str:
    parts = [
        "type=bind",
        f"src={source}",
        f"dst={target}",
        "bind-propagation=rprivate",
    ]
    if readonly:
        parts.append("readonly")
    return ",".join(parts)


def _create_argv(
    *,
    scheduler_image_id: str,
    flaresolverr_container_id: str,
    owner: str,
    spec: WorkerSpec,
    runtime_root: Path,
    source_circuit_root: Path,
    control_root: Path,
    control_json: Path,
) -> tuple[str, ...]:
    return (
        "container",
        "create",
        "--pull=never",
        "--restart=no",
        "--name",
        _container_name(owner, spec.worker_index),
        "--label",
        f"{_LABEL_OWNER}={owner}",
        "--label",
        f"{_LABEL_INDEX}={spec.worker_index}",
        "--label",
        f"{_LABEL_RUNTIME}={_RUNTIME_LABEL}",
        "--user",
        "50000:0",
        "--read-only",
        "--no-healthcheck",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--security-opt",
        "apparmor=docker-default",
        "--security-opt",
        "seccomp=builtin",
        "--network",
        f"container:{flaresolverr_container_id}",
        "--memory",
        "2g",
        "--memory-swap",
        "2g",
        "--pids-limit",
        str(PIDS_LIMIT),
        "--tmpfs",
        f"/tmp:{WORKER_TMPFS_OPTIONS}",
        "--log-driver",
        "none",
        "--mount",
        _mount_argument(runtime_root, CONTAINER_RUNTIME_ROOT, readonly=True),
        "--mount",
        _mount_argument(
            source_circuit_root,
            CONTAINER_SOURCE_CIRCUIT_ROOT,
            readonly=False,
        ),
        "--mount",
        _mount_argument(control_root, CONTAINER_CONTROL_ROOT, readonly=False),
        "--mount",
        _mount_argument(control_json, CONTAINER_CONTROL_JSON, readonly=True),
        "--entrypoint",
        CONTAINER_ENTRYPOINT,
        scheduler_image_id,
        *_bootstrap_argv(),
    )


def _parse_single_inspect(result: CommandResult) -> Mapping[str, object]:
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ContainerRuntimeError("docker inspect returned invalid JSON") from exc
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise ContainerRuntimeError("docker inspect must return exactly one object")
    return payload[0]


def _inspect(
    runner: CommandRunner, reference: str, *, check: bool = True
) -> Mapping[str, object] | None:
    result = _docker(
        runner,
        "container",
        "inspect",
        reference,
        check=check,
    )
    if result.returncode != 0:
        return None
    return _parse_single_inspect(result)


def _dict(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ContainerRuntimeError(f"inspect field {field} is not an object")
    return value


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ContainerRuntimeError(f"inspect field {field} is not a list")
    return value


def _validate_mounts(
    inspect: Mapping[str, object],
    runtime_root: Path,
    source_circuit_root: Path,
    control_root: Path,
    control_json: Path,
) -> None:
    expected = {
        CONTAINER_RUNTIME_ROOT: (str(runtime_root), False),
        CONTAINER_SOURCE_CIRCUIT_ROOT: (str(source_circuit_root), True),
        CONTAINER_CONTROL_ROOT: (str(control_root), True),
        CONTAINER_CONTROL_JSON: (str(control_json), False),
    }
    actual: dict[str, tuple[str, bool]] = {}
    for item in _list(inspect.get("Mounts"), "Mounts"):
        mount = _dict(item, "Mounts[]")
        if mount.get("Type") != "bind" or mount.get("Propagation") != "rprivate":
            raise ContainerRuntimeError("worker has a non-exact bind mount")
        destination = mount.get("Destination")
        source = mount.get("Source")
        writable = mount.get("RW")
        if not isinstance(destination, str) or not isinstance(source, str):
            raise ContainerRuntimeError("worker mount identity is invalid")
        if not isinstance(writable, bool):
            raise ContainerRuntimeError("worker mount mode is invalid")
        if destination in actual:
            raise ContainerRuntimeError("worker has a duplicate mount destination")
        actual[destination] = (source, writable)
    if actual != expected:
        raise ContainerRuntimeError("worker bind mounts differ from the admitted set")


def _validate_static_inspect(
    inspect: Mapping[str, object],
    *,
    container_id: str,
    scheduler_image_id: str,
    flaresolverr_container_id: str,
    owner: str,
    spec: WorkerSpec,
    runtime_root: Path,
    source_circuit_root: Path,
    control_root: Path,
    control_json: Path,
) -> None:
    if inspect.get("Id") != container_id or inspect.get("Image") != scheduler_image_id:
        raise ContainerRuntimeError("worker container/image identity changed")
    if inspect.get("Name") != f"/{_container_name(owner, spec.worker_index)}":
        raise ContainerRuntimeError("worker container name changed")
    config = _dict(inspect.get("Config"), "Config")
    host = _dict(inspect.get("HostConfig"), "HostConfig")
    labels = _dict(config.get("Labels"), "Config.Labels")
    expected_labels = {
        _LABEL_OWNER: owner,
        _LABEL_INDEX: str(spec.worker_index),
        _LABEL_RUNTIME: _RUNTIME_LABEL,
    }
    if any(labels.get(key) != value for key, value in expected_labels.items()):
        raise ContainerRuntimeError("worker labels differ from the exact admitted set")
    if config.get("Image") != scheduler_image_id or config.get("User") != "50000:0":
        raise ContainerRuntimeError("worker image/user changed")
    healthcheck = _dict(config.get("Healthcheck"), "Config.Healthcheck")
    if healthcheck.get("Test") != ["NONE"] or set(healthcheck) != {"Test"}:
        raise ContainerRuntimeError("worker image healthcheck was not disabled")
    if config.get("Entrypoint") != [CONTAINER_ENTRYPOINT]:
        raise ContainerRuntimeError("worker bootstrap entrypoint changed")
    if config.get("Cmd") != list(_bootstrap_argv()):
        raise ContainerRuntimeError("worker bootstrap arguments changed")
    restart = _dict(host.get("RestartPolicy"), "HostConfig.RestartPolicy")
    log_config = _dict(host.get("LogConfig"), "HostConfig.LogConfig")
    security = host.get("SecurityOpt")
    cap_drop = host.get("CapDrop")
    exact_security = {
        "no-new-privileges:true",
        "apparmor=docker-default",
        "seccomp=builtin",
    }
    if restart.get("Name") not in ("", "no") or restart.get("MaximumRetryCount") != 0:
        raise ContainerRuntimeError("worker restart policy is unsafe")
    if host.get("ReadonlyRootfs") is not True or host.get("Privileged") is not False:
        raise ContainerRuntimeError("worker rootfs/privilege hardening changed")
    security_list = _list(security, "HostConfig.SecurityOpt")
    if (
        cap_drop != ["ALL"]
        or len(security_list) != len(exact_security)
        or set(security_list) != exact_security
    ):
        raise ContainerRuntimeError("worker capability/security options changed")
    if host.get("NetworkMode") != f"container:{flaresolverr_container_id}":
        raise ContainerRuntimeError("worker network namespace changed")
    if host.get("Memory") != MEMORY_BYTES or host.get("MemorySwap") != MEMORY_BYTES:
        raise ContainerRuntimeError("worker memory limit changed")
    if host.get("PidsLimit") != PIDS_LIMIT:
        raise ContainerRuntimeError("worker PID limit changed")
    if host.get("Tmpfs") != {"/tmp": WORKER_TMPFS_OPTIONS}:
        raise ContainerRuntimeError("worker tmpfs changed")
    if log_config.get("Type") != "none" or log_config.get("Config") not in ({}, None):
        raise ContainerRuntimeError("worker logging changed")
    if host.get("AutoRemove") is not False:
        raise ContainerRuntimeError("worker auto-remove must be disabled")
    if host.get("PidMode") != "" or host.get("IpcMode") != "private":
        raise ContainerRuntimeError("worker inherited an unsafe host namespace")
    _validate_mounts(
        inspect,
        runtime_root,
        source_circuit_root,
        control_root,
        control_json,
    )


def _snapshot(
    inspect: Mapping[str, object],
    worker_index: int,
    iteration: int,
    container_id: str,
    *,
    memory_usage_bytes: int,
    pids_current: int,
) -> ContainerSnapshot:
    state = _dict(inspect.get("State"), "State")
    status = state.get("Status")
    running = state.get("Running")
    exit_code = state.get("ExitCode")
    oom_killed = state.get("OOMKilled")
    if (
        not isinstance(status, str)
        or not isinstance(running, bool)
        or not isinstance(exit_code, int)
        or not isinstance(oom_killed, bool)
    ):
        raise ContainerRuntimeError("worker state has invalid types")
    if state.get("Dead") is not False or state.get("Restarting") is not False:
        raise ContainerRuntimeError("worker entered dead/restarting state")
    return ContainerSnapshot(
        worker_index=worker_index,
        iteration=iteration,
        container_id=container_id,
        status=status,
        running=running,
        exit_code=exit_code,
        oom_killed=oom_killed,
        memory_usage_bytes=memory_usage_bytes,
        pids_current=pids_current,
    )


def _memory_bytes(value: object) -> int:
    if not isinstance(value, str):
        raise ContainerRuntimeError("docker stats memory is not text")
    used = value.split("/", 1)[0].strip()
    match = _MEMORY_RE.fullmatch(used)
    if match is None:
        raise ContainerRuntimeError("docker stats memory format is invalid")
    multipliers = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "KiB": 1024,
        "MB": 1000**2,
        "MiB": 1024**2,
        "GB": 1000**3,
        "GiB": 1024**3,
    }
    result = int(float(match.group(1)) * multipliers[match.group(2)])
    if result < 0 or result > MEMORY_BYTES:
        raise ContainerRuntimeError("worker memory exceeds its exact limit")
    return result


def _resource_stats(
    runner: CommandRunner, records: Sequence[_ContainerRecord]
) -> Mapping[str, tuple[int, int]]:
    if len(records) != WORKER_COUNT:
        raise ContainerRuntimeError("docker stats cohort identity is incomplete")
    expected = {
        record.container_id: _container_name(
            record.owner, record.spec.worker_index
        )
        for record in records
    }
    if len(expected) != WORKER_COUNT:
        raise ContainerRuntimeError("docker stats cohort IDs are not unique")
    result = _docker(
        runner,
        "container",
        "stats",
        "--no-stream",
        "--no-trunc",
        "--format",
        "{{json .}}",
        *(record.container_id for record in records),
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) > WORKER_COUNT:
        raise ContainerRuntimeError("docker stats returned too many workers")
    observed: dict[str, tuple[int, int]] = {}
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContainerRuntimeError("docker stats returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ContainerRuntimeError("docker stats worker is not an object")
        container_id = payload.get("ID")
        if (
            not isinstance(container_id, str)
            or not _CONTAINER_ID_RE.fullmatch(container_id)
            or container_id not in expected
            or container_id in observed
            or payload.get("Container") not in (container_id, container_id[:12])
            or payload.get("Name") != expected[container_id]
        ):
            raise ContainerRuntimeError("docker stats worker identity changed")
        raw_pids = payload.get("PIDs")
        if not isinstance(raw_pids, str) or not raw_pids.isdigit():
            raise ContainerRuntimeError("docker stats PID count is invalid")
        pids = int(raw_pids)
        if pids < 0 or pids > PIDS_LIMIT:
            raise ContainerRuntimeError("worker PID count exceeds its exact limit")
        observed[container_id] = (
            _memory_bytes(payload.get("MemUsage")),
            pids,
        )
    return observed


def _cohort_sample(
    runner: CommandRunner,
    records: Sequence[_ContainerRecord],
    *,
    monotonic: Callable[[], float],
) -> CohortSample:
    if len(records) != WORKER_COUNT or tuple(
        record.spec.worker_index for record in records
    ) != tuple(range(WORKER_COUNT)):
        raise ContainerRuntimeError("worker cohort identity is incomplete")
    inspected: list[Mapping[str, object]] = []
    for record in records:
        inspect = _inspect(runner, record.container_id)
        assert inspect is not None
        _validate_static_inspect(
            inspect,
            container_id=record.container_id,
            scheduler_image_id=record.scheduler_image_id,
            flaresolverr_container_id=record.flaresolverr_container_id,
            owner=record.owner,
            spec=record.spec,
            runtime_root=record.runtime_root,
            source_circuit_root=record.source_circuit_root,
            control_root=record.control_root,
            control_json=record.control_json,
        )
        inspected.append(inspect)
    stats = _resource_stats(runner, records)
    snapshots: list[ContainerSnapshot] = []
    for record, inspect in zip(records, inspected, strict=True):
        state = _dict(inspect.get("State"), "State")
        if record.container_id not in stats:
            fresh = _inspect(runner, record.container_id)
            assert fresh is not None
            _validate_static_inspect(
                fresh,
                container_id=record.container_id,
                scheduler_image_id=record.scheduler_image_id,
                flaresolverr_container_id=record.flaresolverr_container_id,
                owner=record.owner,
                spec=record.spec,
                runtime_root=record.runtime_root,
                source_circuit_root=record.source_circuit_root,
                control_root=record.control_root,
                control_json=record.control_json,
            )
            if _dict(fresh.get("State"), "State").get("Running") is True:
                raise ContainerRuntimeError(
                    "docker stats omitted a running exact worker"
                )
            inspect = fresh
            state = _dict(inspect.get("State"), "State")
        if state.get("Running") is True:
            memory_usage_bytes, pids_current = stats[record.container_id]
        else:
            memory_usage_bytes, pids_current = 0, 0
        snapshots.append(
            _snapshot(
                inspect,
                record.spec.worker_index,
                record.spec.iteration,
                record.container_id,
                memory_usage_bytes=memory_usage_bytes,
                pids_current=pids_current,
            )
        )
    return CohortSample(monotonic(), tuple(snapshots))


def _validate_created(inspect: Mapping[str, object]) -> None:
    state = _dict(inspect.get("State"), "State")
    if state.get("Status") != "created" or state.get("Running") is not False:
        raise ContainerRuntimeError("new worker is not exactly in created state")


def _validate_running(inspect: Mapping[str, object]) -> None:
    state = _dict(inspect.get("State"), "State")
    if (
        state.get("Status") != "running"
        or state.get("Running") is not True
        or state.get("OOMKilled") is not False
        or state.get("Dead") is not False
        or state.get("Restarting") is not False
        or not isinstance(state.get("Pid"), int)
        or int(state["Pid"]) <= 0
    ):
        raise ContainerRuntimeError("worker is not exactly in safe running state")
    if inspect.get("AppArmorProfile") != "docker-default":
        raise ContainerRuntimeError("worker AppArmor profile is not docker-default")


def find_stale_owner_containers(
    owner: str, *, runner: CommandRunner = _default_runner
) -> tuple[str, ...]:
    """Inspect only four exact names and return their full IDs; never cleanup."""

    if not _OWNER_RE.fullmatch(owner):
        raise ValueError("invalid owner")
    found: list[str] = []
    for worker_index in range(WORKER_COUNT):
        inspect = _inspect(runner, _container_name(owner, worker_index), check=False)
        if inspect is None:
            continue
        container_id = inspect.get("Id")
        if not isinstance(container_id, str) or not _CONTAINER_ID_RE.fullmatch(container_id):
            raise ContainerRuntimeError("stale worker did not expose a full container ID")
        config = _dict(inspect.get("Config"), "Config")
        labels = _dict(config.get("Labels"), "Config.Labels")
        if (
            inspect.get("Name") != f"/{_container_name(owner, worker_index)}"
            or labels.get(_LABEL_OWNER) != owner
            or labels.get(_LABEL_INDEX) != str(worker_index)
            or labels.get(_LABEL_RUNTIME) != _RUNTIME_LABEL
        ):
            raise ContainerRuntimeError("deterministic worker name is owned by another object")
        found.append(container_id)
    return tuple(found)


def _validate_control_file(path: Path) -> os.stat_result:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o444
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
    ):
        raise ContainerRuntimeError("control JSON metadata is unsafe")
    return metadata


def _write_control_file(path: Path, *, owner: str, spec: WorkerSpec) -> None:
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o444,
    )
    try:
        payload = _control_document(owner, spec)
        if os.write(fd, payload) != len(payload):
            raise ContainerRuntimeError("control JSON write was incomplete")
        os.fsync(fd)
        os.fchmod(fd, 0o444)
        os.fchown(fd, 0, 0)
    finally:
        os.close(fd)
    _validate_control_file(path)


def _replace_control_file(
    path: Path,
    *,
    owner: str,
    previous: WorkerSpec,
    replacement: WorkerSpec,
) -> None:
    before = _validate_control_file(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        observed = os.read(descriptor, 64 * 1024 + 1)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable_fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink")
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise ContainerRuntimeError("control JSON identity changed")
    if observed != _control_document(owner, previous):
        raise ContainerRuntimeError("control JSON contents changed")
    path.unlink()
    _write_control_file(path, owner=owner, spec=replacement)


def _make_control_root(
    owner: str, workers: Sequence[WorkerSpec]
) -> tuple[Path, tuple[Path, ...]]:
    _runtime_owner_root, root = _host_artifact_paths(owner)
    created = False
    try:
        root.mkdir(mode=0o750)
        created = True
        os.chown(root, 0, 0)
        os.chmod(root, 0o750)
        ready_root = root / "ready"
        ready_root.mkdir(mode=0o750)
        os.chown(ready_root, 0, 0)
        for index in range(WORKER_COUNT):
            fifo = ready_root / f"{index}.fifo"
            os.mkfifo(fifo, 0o620)
            os.chown(fifo, 0, 0)
            os.chmod(fifo, 0o620)
        for name in ("release.fifo", "liveness.fifo"):
            fifo = root / name
            os.mkfifo(fifo, 0o640)
            os.chown(fifo, 0, 0)
            os.chmod(fifo, 0o640)
        control_files: list[Path] = []
        for spec in workers:
            path = root / f"control-{spec.worker_index}.json"
            _write_control_file(path, owner=owner, spec=spec)
            control_files.append(path)
    except BaseException:
        if created:
            shutil.rmtree(root, ignore_errors=True)
        raise
    return root, tuple(control_files)


def _open_control_fifos(
    control_root: Path,
) -> tuple[list[int], int, int]:
    ready_fds = [
        os.open(
            control_root / "ready" / f"{index}.fifo",
            os.O_RDONLY | os.O_NONBLOCK,
        )
        for index in range(WORKER_COUNT)
    ]
    try:
        release_fd = os.open(control_root / "release.fifo", os.O_RDWR)
        try:
            liveness_fd = os.open(control_root / "liveness.fifo", os.O_RDWR)
        except BaseException:
            os.close(release_fd)
            raise
    except BaseException:
        for fd in ready_fds:
            os.close(fd)
        raise
    return ready_fds, release_fd, liveness_fd


def _atomic_release(release_fd: int, count: int = WORKER_COUNT) -> None:
    if type(count) is not int or not 1 <= count <= WORKER_COUNT:
        raise ContainerRuntimeError("release worker count is invalid")
    payload = b"G" * count
    if os.get_blocking(release_fd) is not True:
        raise ContainerRuntimeError("release FIFO must be blocking")
    pipe_buf = os.fpathconf(release_fd, "PC_PIPE_BUF")
    if not isinstance(pipe_buf, int) or pipe_buf < len(payload):
        raise ContainerRuntimeError("release payload is not provably atomic")
    written = os.write(release_fd, payload)
    if written != len(payload):
        raise ContainerRuntimeError("atomic cohort release was incomplete")


def _close_fds(*groups: object) -> None:
    for group in groups:
        values = group if isinstance(group, list) else [group]
        for value in values:
            if isinstance(value, int) and value >= 0:
                try:
                    os.close(value)
                except OSError:
                    pass


def _exit_code_from_wait(result: CommandResult) -> int | None:
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if not value.isdigit():
        return None
    return int(value)


def _cleanup_one(
    runner: CommandRunner, record: _ContainerRecord
) -> tuple[bool, int | None]:
    inspect = _inspect(runner, record.container_id, check=False)
    if inspect is None:
        return True, None
    try:
        _validate_static_inspect(
            inspect,
            container_id=record.container_id,
            scheduler_image_id=record.scheduler_image_id,
            flaresolverr_container_id=record.flaresolverr_container_id,
            owner=record.owner,
            spec=record.spec,
            runtime_root=record.runtime_root,
            source_circuit_root=record.source_circuit_root,
            control_root=record.control_root,
            control_json=record.control_json,
        )
    except ContainerRuntimeError:
        return False, None
    state = _dict(inspect.get("State"), "State")
    exit_code: int | None = None
    if state.get("Status") != "created":
        _docker(
            runner,
            "container",
            "stop",
            "--time",
            "10",
            record.container_id,
            check=False,
        )
        try:
            waited = _docker(
                runner,
                "container",
                "wait",
                record.container_id,
                timeout_seconds=12.0,
                check=False,
            )
            exit_code = _exit_code_from_wait(waited)
        except ContainerRuntimeError:
            pass
        _docker(
            runner,
            "container",
            "kill",
            "--signal",
            "KILL",
            record.container_id,
            check=False,
        )
        try:
            waited = _docker(
                runner,
                "container",
                "wait",
                record.container_id,
                timeout_seconds=5.0,
                check=False,
            )
            waited_exit = _exit_code_from_wait(waited)
            if waited_exit is not None:
                exit_code = waited_exit
        except ContainerRuntimeError:
            return False, exit_code
    final_inspect = _inspect(runner, record.container_id, check=False)
    if final_inspect is None:
        return False, exit_code
    try:
        _validate_static_inspect(
            final_inspect,
            container_id=record.container_id,
            scheduler_image_id=record.scheduler_image_id,
            flaresolverr_container_id=record.flaresolverr_container_id,
            owner=record.owner,
            spec=record.spec,
            runtime_root=record.runtime_root,
            source_circuit_root=record.source_circuit_root,
            control_root=record.control_root,
            control_json=record.control_json,
        )
    except ContainerRuntimeError:
        return False, exit_code
    final_state = _dict(final_inspect.get("State"), "State")
    if final_state.get("Running") is not False:
        return False, exit_code
    removed = _docker(
        runner,
        "container",
        "rm",
        record.container_id,
        timeout_seconds=10.0,
        check=False,
    )
    return removed.returncode == 0, exit_code


def _validate_stale_identity(
    inspect: Mapping[str, object],
    *,
    owner: str,
    worker_index: int,
    scheduler_image_id: str,
    container_id: str,
) -> None:
    config = _dict(inspect.get("Config"), "Config")
    labels = _dict(config.get("Labels"), "Config.Labels")
    host = _dict(inspect.get("HostConfig"), "HostConfig")
    restart = _dict(host.get("RestartPolicy"), "HostConfig.RestartPolicy")
    if (
        inspect.get("Id") != container_id
        or inspect.get("Name") != f"/{_container_name(owner, worker_index)}"
        or inspect.get("Image") != scheduler_image_id
        or config.get("Image") != scheduler_image_id
        or labels.get(_LABEL_OWNER) != owner
        or labels.get(_LABEL_INDEX) != str(worker_index)
        or labels.get(_LABEL_RUNTIME) != _RUNTIME_LABEL
        or restart.get("Name") not in ("", "no")
        or restart.get("MaximumRetryCount") != 0
        or host.get("AutoRemove") is not False
    ):
        raise ContainerRuntimeError("stale worker identity is not exact")
    runtime_owner_root, control_root = _host_artifact_paths(owner)
    _validate_mounts(
        inspect,
        runtime_owner_root / "root",
        runtime_owner_root / "source-circuit",
        control_root,
        control_root / f"control-{worker_index}.json",
    )


def _remove_exact_host_artifact(path: Path, *, expected_mode: int) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ContainerRuntimeError("host artifact cannot be inspected") from exc
    if (
        path.parent != HOST_ARTIFACT_ROOT
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise ContainerRuntimeError("host artifact identity is unsafe")
    try:
        shutil.rmtree(path)
    except OSError as exc:
        raise ContainerRuntimeError("host artifact cleanup failed") from exc
    if path.exists():
        raise ContainerRuntimeError("host artifact remained after cleanup")


def cleanup_owner_host_artifacts(
    *, owner: str, runner: CommandRunner = _default_runner
) -> None:
    """Remove only owner-derived host dirs after proving no worker remains."""

    owner = owner if _OWNER_RE.fullmatch(owner) else ""
    if not owner:
        raise ValueError("invalid owner")
    if find_stale_owner_containers(owner, runner=runner):
        raise ContainerRuntimeError("worker containers remain before host cleanup")
    runtime_owner_root, control_root = _host_artifact_paths(owner)
    _remove_exact_host_artifact(control_root, expected_mode=0o750)
    _remove_exact_host_artifact(runtime_owner_root, expected_mode=0o700)


def cleanup_stale_owner_containers(
    *,
    owner: str,
    scheduler_image_id: str,
    runner: CommandRunner = _default_runner,
) -> tuple[str, ...]:
    """Cleanup exact deterministic stale names after strict full-ID admission."""

    if not _OWNER_RE.fullmatch(owner) or not _IMAGE_ID_RE.fullmatch(scheduler_image_id):
        raise ValueError("stale cleanup owner/image identity is invalid")
    admitted: list[tuple[int, str]] = []
    for worker_index in range(WORKER_COUNT):
        inspect = _inspect(runner, _container_name(owner, worker_index), check=False)
        if inspect is None:
            continue
        container_id = inspect.get("Id")
        if not isinstance(container_id, str) or not _CONTAINER_ID_RE.fullmatch(container_id):
            raise ContainerRuntimeError("stale cleanup requires a full container ID")
        _validate_stale_identity(
            inspect,
            owner=owner,
            worker_index=worker_index,
            scheduler_image_id=scheduler_image_id,
            container_id=container_id,
        )
        admitted.append((worker_index, container_id))

    removed: list[str] = []
    for worker_index, container_id in reversed(admitted):
        inspect = _inspect(runner, container_id)
        assert inspect is not None
        _validate_stale_identity(
            inspect,
            owner=owner,
            worker_index=worker_index,
            scheduler_image_id=scheduler_image_id,
            container_id=container_id,
        )
        state = _dict(inspect.get("State"), "State")
        if state.get("Status") != "created":
            _docker(
                runner,
                "container",
                "stop",
                "--time",
                "10",
                container_id,
                check=False,
            )
            try:
                _docker(
                    runner,
                    "container",
                    "wait",
                    container_id,
                    timeout_seconds=12.0,
                    check=False,
                )
            except ContainerRuntimeError:
                pass
            _docker(
                runner,
                "container",
                "kill",
                "--signal",
                "KILL",
                container_id,
                check=False,
            )
            _docker(
                runner,
                "container",
                "wait",
                container_id,
                timeout_seconds=5.0,
                check=False,
            )
        final = _inspect(runner, container_id)
        assert final is not None
        _validate_stale_identity(
            final,
            owner=owner,
            worker_index=worker_index,
            scheduler_image_id=scheduler_image_id,
            container_id=container_id,
        )
        if _dict(final.get("State"), "State").get("Running") is not False:
            raise ContainerRuntimeError("stale worker remained running")
        _docker(runner, "container", "rm", container_id)
        removed.append(container_id)
    cleanup_owner_host_artifacts(owner=owner, runner=runner)
    return tuple(reversed(removed))


def run_capacity_containers(
    *,
    scheduler_image_id: str,
    flaresolverr_container_id: str,
    owner: str,
    workers: Sequence[WorkerSpec],
    runtime_root: Path,
    source_circuit_root: Path,
    before_release: BeforeRelease,
    on_sample: OnSample,
    stop_requested: BooleanCallback,
    deadline_reached: BooleanCallback,
    on_outcome: OnOutcome,
    replacement_worker: ReplacementWorker | None = None,
    on_worker_result: OnWorkerResult | None = None,
    runner: CommandRunner = _default_runner,
    attach_factory: AttachFactory = _default_attach_factory,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    ready_timeout_seconds: float = 30.0,
    sample_interval_seconds: float = 1.0,
) -> Outcome:
    """Run four exact slots and optionally refill each completed slot."""

    normalized, runtime_path, circuit_path = _validate_inputs(
        scheduler_image_id,
        flaresolverr_container_id,
        owner,
        workers,
        runtime_root,
        source_circuit_root,
    )
    if ready_timeout_seconds <= 0 or sample_interval_seconds <= 0:
        raise ValueError("timeouts must be positive")
    if (replacement_worker is None) != (on_worker_result is None):
        raise ValueError("replacement and per-worker callbacks must be paired")
    stale = find_stale_owner_containers(owner, runner=runner)
    if stale:
        raise StaleContainerError(stale)

    control_root, control_files = _make_control_root(owner, normalized)
    ready_fds: list[int] = []
    release_fd = -1
    liveness_fd = -1
    records: dict[int, _ContainerRecord] = {}
    captures: dict[int, _CapturedAttach] = {}
    created_container_ids: list[str] = []
    released = False
    status = "failed"
    reason = "cohort did not start"
    observed_exit_codes: list[int | None] = [None] * WORKER_COUNT
    cleanup_complete = True

    def ordered_records() -> tuple[_ContainerRecord, ...]:
        if set(records) != set(range(WORKER_COUNT)):
            raise ContainerRuntimeError("worker slots are not all occupied")
        return tuple(records[index] for index in range(WORKER_COUNT))

    def validate_record(
        record: _ContainerRecord, inspect: Mapping[str, object]
    ) -> None:
        _validate_static_inspect(
            inspect,
            container_id=record.container_id,
            scheduler_image_id=record.scheduler_image_id,
            flaresolverr_container_id=record.flaresolverr_container_id,
            owner=record.owner,
            spec=record.spec,
            runtime_root=record.runtime_root,
            source_circuit_root=record.source_circuit_root,
            control_root=record.control_root,
            control_json=record.control_json,
        )

    def requested_status(context: str) -> bool:
        nonlocal status, reason
        suffix = f" {context}" if context else ""
        if stop_requested():
            status, reason = "stopped", f"stop requested{suffix}"
            return True
        if deadline_reached():
            status, reason = "deadline", f"deadline reached{suffix}"
            return True
        return False

    def create_record(spec: WorkerSpec) -> _ContainerRecord:
        _validate_worker_spec(spec)
        if spec.worker_index in records:
            raise ContainerRuntimeError("worker slot is already occupied")
        result = _docker(
            runner,
            *_create_argv(
                scheduler_image_id=scheduler_image_id,
                flaresolverr_container_id=flaresolverr_container_id,
                owner=owner,
                spec=spec,
                runtime_root=runtime_path,
                source_circuit_root=circuit_path,
                control_root=control_root,
                control_json=control_files[spec.worker_index],
            ),
        )
        container_id = result.stdout.strip()
        if not _CONTAINER_ID_RE.fullmatch(container_id):
            raise ContainerRuntimeError("docker create did not return one full ID")
        record = _ContainerRecord(
            container_id=container_id,
            scheduler_image_id=scheduler_image_id,
            flaresolverr_container_id=flaresolverr_container_id,
            owner=owner,
            spec=spec,
            runtime_root=runtime_path,
            source_circuit_root=circuit_path,
            control_root=control_root,
            control_json=control_files[spec.worker_index],
        )
        # Track the full ID before the first inspect so cleanup never falls
        # back to a name, label, prefix, or partial ID.
        records[spec.worker_index] = record
        created_container_ids.append(container_id)
        observed_exit_codes[spec.worker_index] = None
        inspect = _inspect(runner, container_id)
        assert inspect is not None
        validate_record(record, inspect)
        _validate_created(inspect)
        return record

    def start_record(index: int) -> None:
        record = records[index]
        process = attach_factory(
            (
                DOCKER_CLI,
                "container",
                "start",
                "--attach",
                record.container_id,
            )
        )
        captures[index] = _CapturedAttach(
            process,
            worker_index=index,
            iteration=record.spec.iteration,
            container_id=record.container_id,
        )

    def wait_until_running(indices: set[int], admission_deadline: float) -> bool:
        pending = set(indices)
        while pending:
            if requested_status("while workers started"):
                return False
            if monotonic() >= admission_deadline:
                raise ContainerRuntimeError(
                    "workers did not enter running state in time"
                )
            for index in tuple(pending):
                record = records[index]
                capture = captures.get(index)
                if capture is None or capture.process.poll() is not None:
                    raise ContainerRuntimeError(
                        "attached Docker client exited at startup"
                    )
                inspect = _inspect(runner, record.container_id)
                assert inspect is not None
                validate_record(record, inspect)
                state = _dict(inspect.get("State"), "State")
                if state.get("Status") == "running":
                    _validate_running(inspect)
                    pending.remove(index)
                elif state.get("Status") != "created":
                    raise ContainerRuntimeError(
                        "worker exited before running admission"
                    )
            if pending:
                sleep(0.01)
        return True

    def sample_and_reconcile() -> CohortSample:
        sample = _cohort_sample(
            runner, ordered_records(), monotonic=monotonic
        )
        reconciled = list(sample.containers)
        for index, snapshot in enumerate(reconciled):
            capture = captures.get(index)
            if (
                capture is None
                or capture.process.poll() is None
                or not snapshot.running
            ):
                continue
            record = records[index]
            inspect = _inspect(runner, record.container_id)
            assert inspect is not None
            validate_record(record, inspect)
            fresh = _snapshot(
                inspect,
                index,
                record.spec.iteration,
                record.container_id,
                memory_usage_bytes=0,
                pids_current=0,
            )
            if fresh.running:
                raise ContainerRuntimeError(
                    "attached Docker client exited while worker remained running"
                )
            reconciled[index] = fresh
        return CohortSample(sample.monotonic_seconds, tuple(reconciled))

    def completed_indices(sample: CohortSample) -> set[int]:
        if any(snapshot.oom_killed for snapshot in sample.containers):
            raise ContainerRuntimeError("a worker was OOM-killed")
        if any(
            not snapshot.running and snapshot.exit_code != 0
            for snapshot in sample.containers
        ):
            raise ContainerRuntimeError("a worker exited unsuccessfully")
        return {
            snapshot.worker_index
            for snapshot in sample.containers
            if not snapshot.running
        }

    def wait_until_ready(
        indices: set[int], admission_deadline: float
    ) -> set[int] | None:
        pending = set(indices)
        buffers = {index: bytearray() for index in indices}
        last_sample_at = monotonic()
        while pending:
            if requested_status("before release"):
                return None
            if monotonic() >= admission_deadline:
                raise ContainerRuntimeError("workers did not become READY in time")
            fd_indexes = {ready_fds[index]: index for index in pending}
            readable, _, _ = select.select(tuple(fd_indexes), [], [], 0.05)
            for fd in readable:
                index = fd_indexes[fd]
                try:
                    chunk = os.read(fd, len(READY_PAYLOAD) + 1)
                except BlockingIOError:
                    continue
                if not chunk:
                    continue
                buffers[index].extend(chunk)
                if bytes(buffers[index]) == READY_PAYLOAD:
                    pending.remove(index)
                elif not READY_PAYLOAD.startswith(bytes(buffers[index])):
                    raise ContainerRuntimeError("worker READY payload is invalid")
            if monotonic() - last_sample_at >= sample_interval_seconds:
                sample = sample_and_reconcile()
                on_sample(sample)
                finished = completed_indices(sample)
                if finished & indices:
                    raise ContainerRuntimeError(
                        "unreleased worker exited during admission"
                    )
                if finished:
                    return finished
                last_sample_at = monotonic()
                if requested_status("by startup sample"):
                    return None
            if pending:
                sleep(0.01)
        return set()

    def prepare_and_release(waiting: set[int]) -> set[int] | None:
        nonlocal released
        sample = sample_and_reconcile()
        on_sample(sample)
        finished = completed_indices(sample)
        if finished & waiting:
            raise ContainerRuntimeError("unreleased worker exited at barrier")
        if finished:
            return finished
        if requested_status("by barrier sample"):
            return None
        before_release()
        if requested_status("at release barrier"):
            return None
        finished_at_final_check: set[int] = set()
        for index in range(WORKER_COUNT):
            record = records[index]
            inspect = _inspect(runner, record.container_id)
            assert inspect is not None
            validate_record(record, inspect)
            state = _dict(inspect.get("State"), "State")
            if state.get("Running") is True:
                _validate_running(inspect)
                continue
            snapshot = _snapshot(
                inspect,
                index,
                record.spec.iteration,
                record.container_id,
                memory_usage_bytes=0,
                pids_current=0,
            )
            if snapshot.oom_killed or snapshot.exit_code != 0:
                raise ContainerRuntimeError("a worker exited unsuccessfully")
            if index in waiting:
                raise ContainerRuntimeError("unreleased worker exited at barrier")
            finished_at_final_check.add(index)
        if finished_at_final_check:
            return finished_at_final_check
        _atomic_release(release_fd, len(waiting))
        released = True
        return set()

    def finish_completed_slot(index: int) -> WorkerSpec:
        nonlocal cleanup_complete
        record = records[index]
        inspect = _inspect(runner, record.container_id)
        assert inspect is not None
        validate_record(record, inspect)
        snapshot = _snapshot(
            inspect,
            index,
            record.spec.iteration,
            record.container_id,
            memory_usage_bytes=0,
            pids_current=0,
        )
        if snapshot.running or snapshot.oom_killed or snapshot.exit_code != 0:
            raise ContainerRuntimeError("completed worker state is not safe")
        capture = captures[index]
        result = capture.result()
        if (
            result.worker_index != index
            or result.iteration != record.spec.iteration
            or result.container_id != record.container_id
            or result.attach_returncode != 0
            or result.stdout_json is None
            or not result.output_complete
        ):
            raise ContainerRuntimeError("worker output evidence is incomplete or invalid")
        ok, cleanup_exit = _cleanup_one(runner, record)
        cleanup_complete = cleanup_complete and ok
        if not ok:
            raise ContainerRuntimeError("completed worker cleanup could not be proved")
        observed_exit_codes[index] = (
            snapshot.exit_code if cleanup_exit is None else cleanup_exit
        )
        del records[index]
        del captures[index]
        assert on_worker_result is not None
        on_worker_result(result)
        return record.spec

    def replacement_for(previous: WorkerSpec) -> WorkerSpec:
        assert replacement_worker is not None
        replacement = replacement_worker(previous)
        if replacement is None:
            raise ContainerRuntimeError(
                "replacement worker is unavailable before the deadline"
            )
        _validate_worker_spec(replacement)
        if (
            replacement.worker_index != previous.worker_index
            or replacement.iteration != previous.iteration + 1
        ):
            raise ContainerRuntimeError(
                "replacement worker identity did not advance exactly once"
            )
        return replacement

    def reset_ready_fifo(index: int) -> None:
        os.close(ready_fds[index])
        ready_fds[index] = os.open(
            control_root / "ready" / f"{index}.fifo",
            os.O_RDONLY | os.O_NONBLOCK,
        )

    try:
        ready_fds, release_fd, liveness_fd = _open_control_fifos(control_root)
        for spec in normalized:
            create_record(spec)
        initial = set(range(WORKER_COUNT))
        for index in sorted(initial):
            start_record(index)
        admission_deadline = monotonic() + ready_timeout_seconds
        cohort_running = wait_until_running(initial, admission_deadline)
        if cohort_running:
            startup_sample = sample_and_reconcile()
            on_sample(startup_sample)
            if completed_indices(startup_sample):
                raise ContainerRuntimeError("worker exited during startup")
            cohort_running = not requested_status("by startup sample")
        if cohort_running:
            unexpected = wait_until_ready(initial, admission_deadline)
            cohort_running = unexpected is not None
            if unexpected:
                raise ContainerRuntimeError("worker exited during startup")
        if cohort_running:
            unexpected = prepare_and_release(initial)
            cohort_running = unexpected is not None
            if unexpected:
                raise ContainerRuntimeError("worker exited during startup")

        while released and cohort_running:
            sample = sample_and_reconcile()
            on_sample(sample)
            finished = completed_indices(sample)
            for snapshot in sample.containers:
                if not snapshot.running:
                    observed_exit_codes[snapshot.worker_index] = snapshot.exit_code
            if replacement_worker is None:
                if len(finished) == WORKER_COUNT:
                    status, reason = (
                        "completed",
                        "all four workers exited successfully",
                    )
                    break
                if requested_status(""):
                    break
                sleep(sample_interval_seconds)
                continue

            if not finished:
                if requested_status(""):
                    break
                sleep(sample_interval_seconds)
                continue

            waiting: set[int] = set()
            pending_finished = set(finished)
            while pending_finished:
                previous_specs = {
                    index: finish_completed_slot(index)
                    for index in sorted(pending_finished)
                }
                if requested_status("after a worker result"):
                    cohort_running = False
                    break
                replacements = {
                    index: replacement_for(previous)
                    for index, previous in previous_specs.items()
                }
                new_indices = set(replacements)
                for index in sorted(new_indices):
                    replacement = replacements[index]
                    _replace_control_file(
                        control_files[index],
                        owner=owner,
                        previous=previous_specs[index],
                        replacement=replacement,
                    )
                    reset_ready_fifo(index)
                    create_record(replacement)
                    start_record(index)
                    waiting.add(index)
                admission_deadline = monotonic() + ready_timeout_seconds
                if not wait_until_running(new_indices, admission_deadline):
                    cohort_running = False
                    break
                unexpected = wait_until_ready(new_indices, admission_deadline)
                if unexpected is None:
                    cohort_running = False
                    break
                if unexpected:
                    pending_finished = unexpected
                    continue
                unexpected = prepare_and_release(waiting)
                if unexpected is None:
                    cohort_running = False
                    break
                pending_finished = unexpected
    except Exception as exc:
        status, reason = "failed", str(exc)
    finally:
        _close_fds(ready_fds, release_fd, liveness_fd)
        for index in sorted(records, reverse=True):
            record = records[index]
            try:
                ok, exit_code = _cleanup_one(runner, record)
            except Exception:
                ok, exit_code = False, None
            cleanup_complete = cleanup_complete and ok
            if observed_exit_codes[index] is None:
                observed_exit_codes[index] = exit_code
        try:
            shutil.rmtree(control_root)
        except OSError:
            cleanup_complete = False

    worker_results_list: list[WorkerResult] = []
    for index in sorted(captures):
        capture = captures[index]
        try:
            worker_results_list.append(capture.result())
        except Exception:
            cleanup_complete = False
    worker_results = tuple(worker_results_list)
    if len(worker_results) != len(captures) or any(
        result.attach_returncode is None or not result.output_complete
        for result in worker_results
    ):
        status, reason = "failed", "attached worker output capture did not finish"
    elif status == "completed" and replacement_worker is None and (
        len(worker_results) != WORKER_COUNT
        or any(
            result.attach_returncode != 0 or result.stdout_json is None
            for result in worker_results
        )
    ):
        status, reason = "failed", "worker output evidence is incomplete or invalid"
    if not cleanup_complete:
        status, reason = "failed", "exact container cleanup could not be proved"
    outcome = Outcome(
        status=status,
        reason=reason,
        released=released,
        container_ids=tuple(created_container_ids),
        exit_codes=tuple(observed_exit_codes),
        worker_results=worker_results,
        cleanup_complete=cleanup_complete,
    )
    on_outcome(outcome)
    return outcome


__all__ = [
    "BOOTSTRAP_PATH",
    "CohortSample",
    "CommandResult",
    "ContainerRuntimeError",
    "ContainerSnapshot",
    "Outcome",
    "StaleContainerError",
    "WorkerResult",
    "WorkerSpec",
    "cleanup_owner_host_artifacts",
    "cleanup_stale_owner_containers",
    "find_stale_owner_containers",
    "run_capacity_containers",
]
