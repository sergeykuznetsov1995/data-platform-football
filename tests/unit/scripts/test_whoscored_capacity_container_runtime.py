from __future__ import annotations

import copy
import io
import importlib.util
import os
from pathlib import Path
import sys
from typing import Callable

import pytest

from scripts.research import whoscored_capacity_container_runtime as runtime


IMAGE_ID = "sha256:" + "a" * 64
FLARE_ID = "b" * 64
OWNER = "owner000000000001"
BOOTSTRAP_SOURCE = (
    Path(__file__).resolve().parents[3]
    / "docker/images/airflow/whoscored_capacity_worker_bootstrap.py"
)


def _workers() -> tuple[runtime.WorkerSpec, ...]:
    return tuple(
        runtime.WorkerSpec(index, (runtime.WORKLOAD_PATH, "--worker-id", str(index)))
        for index in range(4)
    )


def _load_bootstrap():
    name = "test_capacity_container_runtime_bootstrap_contract"
    spec = importlib.util.spec_from_file_location(name, BOOTSTRAP_SOURCE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeDocker:
    def __init__(
        self,
        *,
        inspect_mutation: Callable[[dict[str, object]], None] | None = None,
        stats_mutation: Callable[[dict[str, object]], None] | None = None,
        stale: bool = False,
        complete_after_stats_call: int | None = None,
        omit_completed_stats_rows: bool = False,
        stats_container_full_id: bool = True,
    ) -> None:
        self.calls: list[tuple[tuple[str, ...], float | None]] = []
        self.attach_calls: list[tuple[str, ...]] = []
        self.control_documents: list[bytes] = []
        self.containers: dict[str, dict[str, object]] = {}
        self.names: dict[str, str] = {}
        self.inspect_mutation = inspect_mutation
        self.stats_mutation = stats_mutation
        self.stale = stale
        self.complete_after_stats_call = complete_after_stats_call
        self.omit_completed_stats_rows = omit_completed_stats_rows
        self.stats_container_full_id = stats_container_full_id
        self.stats_calls = 0
        self.next_container_number = 1
        self.liveness_writers: list[int] = []
        self.release_readers: list[int] = []

    def __call__(
        self, argv: tuple[str, ...], timeout_seconds: float | None
    ) -> runtime.CommandResult:
        self.calls.append((argv, timeout_seconds))
        assert argv[0] == "/usr/bin/docker"
        args = argv[1:]
        if args[:2] == ("container", "inspect"):
            return self._inspect(args[2])
        if args[:2] == ("container", "create"):
            return self._create(args)
        if args[:2] == ("container", "start"):
            return self._start(args[2])
        if args[:2] == ("container", "stats"):
            self.stats_calls += 1
            assert args[2:6] == (
                "--no-stream",
                "--no-trunc",
                "--format",
                "{{json .}}",
            )
            container_ids = args[6:]
            assert len(container_ids) == runtime.WORKER_COUNT
            if (
                self.omit_completed_stats_rows
                and self.stats_calls == self.complete_after_stats_call
            ):
                self.complete()
            lines = []
            for container_id in container_ids:
                item = self.containers[container_id]
                state = item["State"]
                assert isinstance(state, dict)
                if self.omit_completed_stats_rows and not state["Running"]:
                    continue
                name = item["Name"]
                assert isinstance(name, str)
                payload = {
                    "Container": (
                        container_id
                        if self.stats_container_full_id
                        else container_id[:12]
                    ),
                    "ID": container_id,
                    "Name": name.removeprefix("/"),
                    "MemUsage": "16MiB / 2GiB",
                    "PIDs": "3",
                }
                if self.stats_mutation is not None:
                    self.stats_mutation(payload)
                lines.append(runtime.json.dumps(payload))
            result = runtime.CommandResult(0, "\n".join(lines) + "\n")
            if (
                not self.omit_completed_stats_rows
                and self.stats_calls == self.complete_after_stats_call
            ):
                self.complete()
            return result
        if args[:2] == ("container", "stop"):
            return self._stop(args[-1])
        if args[:2] == ("container", "wait"):
            item = self.containers[args[2]]
            state = item["State"]
            assert isinstance(state, dict)
            return runtime.CommandResult(0, f"{state['ExitCode']}\n")
        if args[:2] == ("container", "kill"):
            return self._stop(args[-1])
        if args[:2] == ("container", "rm"):
            container_id = args[2]
            item = self.containers.pop(container_id)
            name = item["Name"]
            assert isinstance(name, str)
            self.names.pop(name.removeprefix("/"), None)
            if self.stale and not self.containers:
                self.stale = False
            return runtime.CommandResult(0)
        raise AssertionError(f"unexpected docker call: {argv!r}")

    def _inspect(self, reference: str) -> runtime.CommandResult:
        container_id = self.names.get(reference, reference)
        if self.stale and reference.startswith("whoscored-capacity-"):
            index = int(reference.rsplit("-", 1)[1])
            stale_id = f"{index + 8:x}" * 64
            runtime_owner_root, control_root = runtime._host_artifact_paths(OWNER)
            payload = self._inspect_payload_from_values(
                container_id=stale_id,
                name=reference,
                index=index,
                control_root=str(control_root),
                control_json=str(control_root / f"control-{index}.json"),
                runtime_root=str(runtime_owner_root / "root"),
                source_circuit_root=str(runtime_owner_root / "source-circuit"),
                status="running",
            )
            self.containers.setdefault(stale_id, payload)
            self.names.setdefault(reference, stale_id)
            return runtime.CommandResult(0, runtime.json.dumps([payload]))
        item = self.containers.get(container_id)
        if item is None:
            return runtime.CommandResult(1, stderr="No such container")
        payload = copy.deepcopy(item)
        if self.inspect_mutation is not None:
            self.inspect_mutation(payload)
        return runtime.CommandResult(0, runtime.json.dumps([payload]))

    @staticmethod
    def _value_after(args: tuple[str, ...], option: str) -> str:
        return args[args.index(option) + 1]

    def _create(self, args: tuple[str, ...]) -> runtime.CommandResult:
        name = self._value_after(args, "--name")
        index = int(name.rsplit("-", 1)[1])
        digit = format(self.next_container_number, "x")[-1]
        self.next_container_number += 1
        container_id = digit * 64
        assert container_id not in self.containers
        mounts = [args[pos + 1] for pos, value in enumerate(args) if value == "--mount"]
        parsed_mounts: list[dict[str, object]] = []
        sources: dict[str, str] = {}
        for value in mounts:
            pieces: dict[str, str] = {}
            readonly = False
            for part in value.split(","):
                if part == "readonly":
                    readonly = True
                else:
                    key, item = part.split("=", 1)
                    pieces[key] = item
            parsed_mounts.append(
                {
                    "Type": "bind",
                    "Source": pieces["src"],
                    "Destination": pieces["dst"],
                    "RW": not readonly,
                    "Propagation": "rprivate",
                }
            )
            sources[pieces["dst"]] = pieces["src"]
        image_position = args.index(IMAGE_ID)
        command = list(args[image_position + 1 :])
        security_options = [
            args[pos + 1]
            for pos, value in enumerate(args)
            if value == "--security-opt"
        ]
        control_path = Path(sources[runtime.CONTAINER_CONTROL_JSON])
        metadata = control_path.stat()
        assert metadata.st_mode & 0o777 == 0o444
        assert metadata.st_uid == metadata.st_gid == 0
        assert metadata.st_nlink == 1
        self.control_documents.append(control_path.read_bytes())
        payload = self._inspect_payload_from_values(
            container_id=container_id,
            name=name,
            index=index,
            control_root=sources[runtime.CONTAINER_CONTROL_ROOT],
            control_json=sources[runtime.CONTAINER_CONTROL_JSON],
            runtime_root=sources[runtime.CONTAINER_RUNTIME_ROOT],
            source_circuit_root=sources[runtime.CONTAINER_SOURCE_CIRCUIT_ROOT],
            status="created",
            command=command,
            mounts=parsed_mounts,
            security_options=security_options,
        )
        self.containers[container_id] = payload
        self.names[name] = container_id
        return runtime.CommandResult(0, container_id + "\n")

    def _inspect_payload_from_values(
        self,
        *,
        container_id: str,
        name: str,
        index: int,
        control_root: str,
        control_json: str,
        runtime_root: str,
        source_circuit_root: str,
        status: str,
        command: list[str] | None = None,
        mounts: list[dict[str, object]] | None = None,
        security_options: list[str] | None = None,
    ) -> dict[str, object]:
        return {
            "Id": container_id,
            "Name": "/" + name,
            "Image": IMAGE_ID,
            "AppArmorProfile": "docker-default" if status == "running" else "",
            "Config": {
                "Image": IMAGE_ID,
                "User": "50000:0",
                "Healthcheck": {"Test": ["NONE"]},
                "Entrypoint": [runtime.CONTAINER_ENTRYPOINT],
                "Cmd": command or list(runtime._bootstrap_argv()),
                "Labels": {
                    runtime._LABEL_OWNER: OWNER,
                    runtime._LABEL_INDEX: str(index),
                    runtime._LABEL_RUNTIME: runtime._RUNTIME_LABEL,
                    "image.supplied.label": "allowed",
                },
            },
            "HostConfig": {
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
                "ReadonlyRootfs": True,
                "Privileged": False,
                "CapDrop": ["ALL"],
                "SecurityOpt": (
                    security_options
                    if security_options is not None
                    else [
                        "no-new-privileges:true",
                        "apparmor=docker-default",
                        "seccomp=builtin",
                    ]
                ),
                "NetworkMode": "container:" + FLARE_ID,
                "Memory": runtime.MEMORY_BYTES,
                "MemorySwap": runtime.MEMORY_BYTES,
                "PidsLimit": runtime.PIDS_LIMIT,
                "Tmpfs": {"/tmp": runtime.WORKER_TMPFS_OPTIONS},
                "LogConfig": {"Type": "none", "Config": {}},
                "AutoRemove": False,
                "PidMode": "",
                "IpcMode": "private",
            },
            "Mounts": mounts
            or [
                {
                    "Type": "bind",
                    "Source": runtime_root,
                    "Destination": runtime.CONTAINER_RUNTIME_ROOT,
                    "RW": False,
                    "Propagation": "rprivate",
                },
                {
                    "Type": "bind",
                    "Source": source_circuit_root,
                    "Destination": runtime.CONTAINER_SOURCE_CIRCUIT_ROOT,
                    "RW": True,
                    "Propagation": "rprivate",
                },
                {
                    "Type": "bind",
                    "Source": control_json,
                    "Destination": runtime.CONTAINER_CONTROL_JSON,
                    "RW": False,
                    "Propagation": "rprivate",
                },
                {
                    "Type": "bind",
                    "Source": control_root,
                    "Destination": runtime.CONTAINER_CONTROL_ROOT,
                    "RW": True,
                    "Propagation": "rprivate",
                },
            ],
            "State": {
                "Status": status,
                "Running": status == "running",
                "OOMKilled": False,
                "Dead": False,
                "Restarting": False,
                "Pid": 1000 + index if status == "running" else 0,
                "ExitCode": 0,
            },
        }

    def _start(self, container_id: str) -> runtime.CommandResult:
        item = self.containers[container_id]
        state = item["State"]
        assert isinstance(state, dict)
        state.update(Status="running", Running=True, Pid=1000 + len(self.liveness_writers))
        item["AppArmorProfile"] = "docker-default"
        index = int(
            str(item["Name"]).rsplit("-", 1)[1]
        )
        mounts = item["Mounts"]
        assert isinstance(mounts, list)
        control = next(
            str(mount["Source"])
            for mount in mounts
            if isinstance(mount, dict)
            and mount.get("Destination") == runtime.CONTAINER_CONTROL_ROOT
        )
        liveness = os.open(Path(control) / "liveness.fifo", os.O_RDONLY | os.O_NONBLOCK)
        release = os.open(Path(control) / "release.fifo", os.O_RDONLY | os.O_NONBLOCK)
        ready = os.open(Path(control) / "ready" / f"{index}.fifo", os.O_WRONLY)
        os.write(ready, runtime.READY_PAYLOAD)
        os.close(ready)
        self.liveness_writers.append(liveness)
        self.release_readers.append(release)
        return runtime.CommandResult(0, container_id + "\n")

    def _stop(self, container_id: str) -> runtime.CommandResult:
        item = self.containers[container_id]
        state = item["State"]
        assert isinstance(state, dict)
        state.update(Status="exited", Running=False, Pid=0)
        return runtime.CommandResult(0, container_id + "\n")

    def complete(self) -> None:
        for item in self.containers.values():
            state = item["State"]
            assert isinstance(state, dict)
            state.update(Status="exited", Running=False, Pid=0, ExitCode=0)

    def complete_slot(self, index: int) -> None:
        container_id = self.names[f"whoscored-capacity-{OWNER}-{index}"]
        item = self.containers[container_id]
        state = item["State"]
        assert isinstance(state, dict)
        state.update(Status="exited", Running=False, Pid=0, ExitCode=0)

    def close(self) -> None:
        for fd in self.liveness_writers + self.release_readers:
            try:
                os.close(fd)
            except OSError:
                pass

    def attach(self, argv: tuple[str, ...]) -> "FakeAttachProcess":
        self.attach_calls.append(argv)
        assert argv[:4] == (
            "/usr/bin/docker",
            "container",
            "start",
            "--attach",
        )
        self._start(argv[4])
        return FakeAttachProcess(self, argv[4])


class FakeAttachProcess:
    def __init__(self, docker: FakeDocker, container_id: str) -> None:
        self.docker = docker
        self.container_id = container_id
        self.stdout = io.BytesIO(b'{"page_units":1,"paid_bytes":0}\n')
        self.stderr = io.BytesIO(b"safe diagnostic\n")

    def poll(self) -> int | None:
        item = self.docker.containers.get(self.container_id)
        if item is None:
            return 0
        state = item["State"]
        assert isinstance(state, dict)
        return None if state["Running"] else 0

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0

    def terminate(self) -> None:
        self.docker._stop(self.container_id)

    def kill(self) -> None:
        self.docker._stop(self.container_id)


@pytest.fixture
def binds(tmp_path: Path) -> tuple[Path, Path]:
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700)
    source_circuit_root = tmp_path / "source-circuit"
    source_circuit_root.mkdir(mode=0o770)
    source_circuit_root.chmod(0o770)
    return runtime_root, source_circuit_root


def test_control_bytes_and_fixed_paths_match_baked_bootstrap() -> None:
    bootstrap = _load_bootstrap()

    assert bootstrap.CONTROL_PATH == runtime.CONTAINER_CONTROL_JSON
    assert bootstrap.WORKFLOW_SCRIPT == runtime.WORKLOAD_PATH
    assert bootstrap.SOURCE_CIRCUIT_PATH == runtime.CONTAINER_SOURCE_CIRCUIT
    assert bootstrap.READY_PAYLOAD == runtime.READY_PAYLOAD
    assert bootstrap.RELEASE_BYTE * 4 == runtime.RELEASE_PAYLOAD
    for spec in _workers():
        control = bootstrap._parse_control(runtime._control_document(OWNER, spec))
        assert control.worker_id == spec.worker_index
        assert control.owner == OWNER
        assert control.argv == spec.workload_argv


def test_success_uses_exact_hardening_and_one_atomic_release(
    binds: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runtime_root, source_circuit = binds
    outcomes: list[runtime.Outcome] = []
    samples: list[runtime.CohortSample] = []
    release_writes: list[bytes] = []
    events: list[str] = []
    released_gate = False
    completed = False
    real_write = os.write

    def tracked_write(fd: int, payload: bytes) -> int:
        if payload == runtime.RELEASE_PAYLOAD:
            release_writes.append(payload)
        return real_write(fd, payload)

    monkeypatch.setattr(runtime.os, "write", tracked_write)

    def on_sample(sample: runtime.CohortSample) -> None:
        nonlocal completed
        samples.append(sample)
        events.append("sample")
        if released_gate and not completed:
            fake.complete()
            completed = True

    def before_release() -> None:
        nonlocal released_gate
        events.append("before_release")
        released_gate = True

    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=before_release,
            on_sample=on_sample,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=outcomes.append,
            runner=fake,
            attach_factory=fake.attach,
            sample_interval_seconds=0.001,
        )
    finally:
        fake.close()

    assert outcome.status == "completed"
    assert outcome.released is True
    assert outcome.cleanup_complete is True
    assert outcomes == [outcome]
    assert len(samples) == 4
    assert events[:3] == ["sample", "sample", "before_release"]
    assert [
        (item.memory_usage_bytes, item.pids_current)
        for item in samples[0].containers
    ] == [(16 * 1024**2, 3)] * 4
    assert release_writes == [b"GGGG"]
    assert fake.containers == {}
    assert len(fake.attach_calls) == 4
    assert all(
        call[:4]
        == ("/usr/bin/docker", "container", "start", "--attach")
        and len(call[4]) == 64
        for call in fake.attach_calls
    )
    assert len(outcome.worker_results) == 4
    assert all(result.stdout_json is not None for result in outcome.worker_results)
    assert all(len(result.stderr_sha256) == 64 for result in outcome.worker_results)
    stats_calls = [
        call[0]
        for call in fake.calls
        if call[0][1:3] == ("container", "stats")
    ]
    assert len(stats_calls) == len(samples)
    assert all(
        call[3:7]
        == ("--no-stream", "--no-trunc", "--format", "{{json .}}")
        and len(call[7:]) == runtime.WORKER_COUNT
        and all(len(container_id) == 64 for container_id in call[7:])
        for call in stats_calls
    )
    assert [runtime.json.loads(item) for item in fake.control_documents] == [
        {
            "argv": list(spec.workload_argv),
            "expected_curl_cffi": "0.15.0",
            "expected_python": "3.11",
            "owner": OWNER,
            "schema_version": 1,
            "worker_id": spec.worker_index,
        }
        for spec in _workers()
    ]
    create_calls = [call[0] for call in fake.calls if call[0][1:3] == ("container", "create")]
    assert len(create_calls) == 4
    for argv in create_calls:
        joined = tuple(argv)
        for required in (
            "--pull=never",
            "--restart=no",
            "--read-only",
            "--no-healthcheck",
            "ALL",
            "no-new-privileges:true",
            "apparmor=docker-default",
            "seccomp=builtin",
            "2g",
            "128",
            f"/tmp:{runtime.WORKER_TMPFS_OPTIONS}",
            "none",
            IMAGE_ID,
        ):
            assert required in joined
        assert f"container:{FLARE_ID}" in joined
        assert joined[joined.index("--entrypoint") + 1] == "/usr/bin/dumb-init"
        assert joined[joined.index(IMAGE_ID) + 1 :] == runtime._bootstrap_argv()


def test_barrier_resource_sample_can_stop_before_atomic_release(
    binds: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runtime_root, source_circuit = binds
    release_writes: list[bytes] = []
    real_write = os.write
    sampled = False

    def tracked_write(fd: int, payload: bytes) -> int:
        if payload == runtime.RELEASE_PAYLOAD:
            release_writes.append(payload)
        return real_write(fd, payload)

    def on_sample(sample: runtime.CohortSample) -> None:
        nonlocal sampled
        assert len(sample.containers) == runtime.WORKER_COUNT
        sampled = True

    monkeypatch.setattr(runtime.os, "write", tracked_write)
    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: pytest.fail("release gate must not run"),
            on_sample=on_sample,
            stop_requested=lambda: sampled,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
        )
    finally:
        fake.close()

    assert outcome.status == "stopped"
    assert outcome.released is False
    assert release_writes == []


def test_normal_exit_during_stats_is_reconciled_with_fresh_exact_inspect(
    binds: tuple[Path, Path],
) -> None:
    # Two four-worker samples happen before release; finish during the third.
    fake = FakeDocker(
        complete_after_stats_call=3,
        omit_completed_stats_rows=True,
    )
    runtime_root, source_circuit = binds
    samples = []
    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=samples.append,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
            sample_interval_seconds=0.001,
        )
    finally:
        fake.close()

    assert outcome.status == "completed"
    assert fake.stats_calls == 3
    assert all(not item.running for item in samples[-1].containers)


def test_stats_accepts_legacy_short_container_display_id(
    binds: tuple[Path, Path],
) -> None:
    fake = FakeDocker(
        complete_after_stats_call=3,
        omit_completed_stats_rows=True,
        stats_container_full_id=False,
    )
    runtime_root, source_circuit = binds
    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=lambda _: None,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
            sample_interval_seconds=0.001,
        )
    finally:
        fake.close()

    assert outcome.status == "completed"
    assert outcome.cleanup_complete is True


@pytest.mark.parametrize("field", ["ID", "Container", "Name"])
def test_stats_rejects_non_exact_full_container_identity(
    binds: tuple[Path, Path], field: str,
) -> None:
    def change_identity(payload: dict[str, object]) -> None:
        if field == "ID":
            payload[field] = str(payload[field])[:12]
        else:
            payload[field] = str(payload[field]) + "-wrong"

    fake = FakeDocker(stats_mutation=change_identity)
    runtime_root, source_circuit = binds
    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=lambda _: None,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
        )
    finally:
        fake.close()

    assert outcome.status == "failed"
    assert "stats worker identity" in outcome.reason
    assert outcome.cleanup_complete is True


def test_completed_slot_is_removed_streamed_and_replaced_before_stop(
    binds: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runtime_root, source_circuit = binds
    streamed: list[runtime.WorkerResult] = []
    release_writes: list[bytes] = []
    completed_initial_slot = False
    real_write = os.write

    def tracked_write(fd: int, payload: bytes) -> int:
        if payload in (runtime.RELEASE_PAYLOAD, b"G"):
            release_writes.append(payload)
        return real_write(fd, payload)

    def on_sample(_sample: runtime.CohortSample) -> None:
        nonlocal completed_initial_slot
        if release_writes == [runtime.RELEASE_PAYLOAD] and not completed_initial_slot:
            fake.complete_slot(0)
            completed_initial_slot = True

    def replacement(previous: runtime.WorkerSpec) -> runtime.WorkerSpec:
        return runtime.WorkerSpec(
            worker_index=previous.worker_index,
            workload_argv=previous.workload_argv,
            iteration=previous.iteration + 1,
        )

    monkeypatch.setattr(runtime.os, "write", tracked_write)
    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=on_sample,
            stop_requested=lambda: release_writes[-1:] == [b"G"],
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            replacement_worker=replacement,
            on_worker_result=streamed.append,
            runner=fake,
            attach_factory=fake.attach,
            sample_interval_seconds=0.001,
        )
    finally:
        fake.close()

    assert outcome.status == "stopped"
    assert outcome.cleanup_complete is True
    assert release_writes == [b"GGGG", b"G"]
    assert [(item.worker_index, item.iteration) for item in streamed] == [(0, 0)]
    assert len(outcome.container_ids) == 5
    assert len(set(outcome.container_ids)) == 5
    assert sorted(
        (item.worker_index, item.iteration) for item in outcome.worker_results
    ) == [(0, 1), (1, 0), (2, 0), (3, 0)]
    create_calls = [
        call[0]
        for call in fake.calls
        if call[0][1:3] == ("container", "create")
    ]
    assert len(create_calls) == 5


def test_stop_at_barrier_releases_nobody_and_cleans_exact_ids(
    binds: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runtime_root, source_circuit = binds
    release_writes: list[bytes] = []
    real_write = os.write

    def tracked_write(fd: int, payload: bytes) -> int:
        if payload == runtime.RELEASE_PAYLOAD:
            release_writes.append(payload)
        return real_write(fd, payload)

    monkeypatch.setattr(runtime.os, "write", tracked_write)
    stop_calls = 0

    def stop_requested() -> bool:
        nonlocal stop_calls
        stop_calls += 1
        return stop_calls >= 2

    try:
        outcome = runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=lambda _: None,
            stop_requested=stop_requested,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
        )
    finally:
        fake.close()

    assert outcome.status == "stopped"
    assert outcome.released is False
    assert release_writes == []
    destructive = [
        call[0] for call in fake.calls if call[0][1:3] in {
            ("container", "stop"),
            ("container", "kill"),
            ("container", "rm"),
        }
    ]
    assert destructive
    assert all(call[-1] in outcome.container_ids for call in destructive)


def test_existing_exact_name_fails_closed_without_create(
    binds: tuple[Path, Path],
) -> None:
    fake = FakeDocker(stale=True)
    runtime_root, source_circuit = binds

    with pytest.raises(runtime.StaleContainerError) as raised:
        runtime.run_capacity_containers(
            scheduler_image_id=IMAGE_ID,
            flaresolverr_container_id=FLARE_ID,
            owner=OWNER,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=lambda _: None,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=fake,
            attach_factory=fake.attach,
        )

    assert len(raised.value.container_ids) == 4
    assert not any(call[0][1:3] == ("container", "create") for call in fake.calls)
    assert all(call[0][1:3] == ("container", "inspect") for call in fake.calls)


def test_stale_cleanup_discovers_exact_names_then_uses_only_full_ids() -> None:
    fake = FakeDocker(stale=True)
    runtime_owner_root, control_root = runtime._host_artifact_paths(OWNER)
    assert not runtime_owner_root.exists()
    assert not control_root.exists()
    runtime_owner_root.mkdir(mode=0o700)
    control_root.mkdir(mode=0o750)

    removed = runtime.cleanup_stale_owner_containers(
        owner=OWNER,
        scheduler_image_id=IMAGE_ID,
        runner=fake,
    )

    assert len(removed) == 4
    assert all(len(container_id) == 64 for container_id in removed)
    destructive = [
        call[0]
        for call in fake.calls
        if call[0][1:3]
        in {
            ("container", "stop"),
            ("container", "wait"),
            ("container", "kill"),
            ("container", "rm"),
        }
    ]
    assert destructive
    assert all(call[-1] in removed for call in destructive)
    assert not any("ls" in call or "ps" in call for call, _ in fake.calls)
    assert not runtime_owner_root.exists()
    assert not control_root.exists()


def test_owner_host_artifacts_cleanup_works_before_any_container_create() -> None:
    fake = FakeDocker()
    runtime_owner_root, control_root = runtime._host_artifact_paths(OWNER)
    assert not runtime_owner_root.exists()
    assert not control_root.exists()
    runtime_owner_root.mkdir(mode=0o700)
    control_root.mkdir(mode=0o750)

    runtime.cleanup_owner_host_artifacts(owner=OWNER, runner=fake)

    assert not runtime_owner_root.exists()
    assert not control_root.exists()
    assert all(
        call[0][1:3] == ("container", "inspect") for call in fake.calls
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [("ReadonlyRootfs", False), ("Tmpfs", {})],
)
def test_inspect_hardening_mismatch_keeps_exact_orphan_id(
    binds: tuple[Path, Path], field: str, value: object
) -> None:
    def mutate(payload: dict[str, object]) -> None:
        host = payload["HostConfig"]
        assert isinstance(host, dict)
        host[field] = value

    fake = FakeDocker(inspect_mutation=mutate)
    runtime_root, source_circuit = binds
    outcome = runtime.run_capacity_containers(
        scheduler_image_id=IMAGE_ID,
        flaresolverr_container_id=FLARE_ID,
        owner=OWNER,
        workers=_workers(),
        runtime_root=runtime_root,
        source_circuit_root=source_circuit,
        before_release=lambda: None,
        on_sample=lambda _: None,
        stop_requested=lambda: False,
        deadline_reached=lambda: False,
        on_outcome=lambda _: None,
        runner=fake,
        attach_factory=fake.attach,
    )

    assert outcome.status == "failed"
    assert outcome.cleanup_complete is False
    assert outcome.container_ids == ("1" * 64,)
    assert not any(call[0][1:3] == ("container", "rm") for call in fake.calls)


@pytest.mark.parametrize(
    ("image_id", "flare_id", "owner"),
    [
        ("scheduler:latest", FLARE_ID, OWNER),
        (IMAGE_ID, "short", OWNER),
        (IMAGE_ID, FLARE_ID, "BAD OWNER"),
    ],
)
def test_exact_id_and_owner_validation(
    binds: tuple[Path, Path], image_id: str, flare_id: str, owner: str
) -> None:
    runtime_root, source_circuit = binds
    with pytest.raises(ValueError):
        runtime.run_capacity_containers(
            scheduler_image_id=image_id,
            flaresolverr_container_id=flare_id,
            owner=owner,
            workers=_workers(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit,
            before_release=lambda: None,
            on_sample=lambda _: None,
            stop_requested=lambda: False,
            deadline_reached=lambda: False,
            on_outcome=lambda _: None,
            runner=FakeDocker(),
        )
