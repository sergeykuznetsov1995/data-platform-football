from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import sofascore_runtime_preflight as preflight
from scrapers.sofascore.workload_plan import WorkloadPolicyUnavailable


ARTIFACT_ID = "a" * 64


@pytest.fixture(autouse=True)
def _expected_artifact_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", ARTIFACT_ID)


def _artifact(tmp_path: Path, *, mode: int = 0o640) -> Path:
    path = tmp_path / "budget.json"
    path.write_text("{}\n", encoding="utf-8")
    path.chmod(mode)
    return path


def _state(tmp_path: Path, *, mode: int = 0o770) -> Path:
    path = tmp_path / "gateway-state"
    path.mkdir()
    path.chmod(mode)
    return path


def _allow_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        preflight,
        "load_verified_workload_policy",
        lambda _path: SimpleNamespace(artifact_id=ARTIFACT_ID),
    )


def _health(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": "ok",
        "sofascore_paid_enabled": True,
        "sofascore_dagrun_budget_bytes": 326_485,
        "sofascore_budget_artifact_id": ARTIFACT_ID,
    }
    payload.update(overrides)
    return payload


def test_filesystem_preflight_is_uid_aware_and_loads_verified_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_policy(monkeypatch)
    artifact = _artifact(tmp_path)
    state = _state(tmp_path)

    assert (
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )
        == ARTIFACT_ID
    )
    preflight.validate_state_directory(
        state,
        probe_write=True,
        runtime_uid=state.stat().st_uid,
        runtime_gid=state.stat().st_gid,
    )
    assert list(state.iterdir()) == []


def test_exact_uid_50000_gid_0_permission_model_is_synthetic_and_portable() -> None:
    root_group_readable = SimpleNamespace(st_mode=0o100640, st_uid=0, st_gid=0)
    wrong_group_private = SimpleNamespace(st_mode=0o100600, st_uid=0, st_gid=123)
    runtime_owned = SimpleNamespace(
        st_mode=0o100400,
        st_uid=preflight.RUNTIME_UID,
        st_gid=123,
    )
    root_group_state = SimpleNamespace(st_mode=0o040770, st_uid=0, st_gid=0)

    assert preflight._identity_has_permissions(
        root_group_readable,
        uid=preflight.RUNTIME_UID,
        gid=preflight.RUNTIME_GID,
        required=0o4,
    )
    assert not preflight._identity_has_permissions(
        wrong_group_private,
        uid=preflight.RUNTIME_UID,
        gid=preflight.RUNTIME_GID,
        required=0o4,
    )
    assert preflight._identity_has_permissions(
        runtime_owned,
        uid=preflight.RUNTIME_UID,
        gid=preflight.RUNTIME_GID,
        required=0o4,
    )
    assert preflight._identity_has_permissions(
        root_group_state,
        uid=preflight.RUNTIME_UID,
        gid=preflight.RUNTIME_GID,
        required=0o3,
    )


def test_artifact_must_be_regular_immutable_and_scheduler_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_policy(monkeypatch)
    artifact = _artifact(tmp_path, mode=0o000)
    with pytest.raises(preflight.ReadinessError, match="unreadable|not readable"):
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )

    artifact.chmod(0o662)
    with pytest.raises(preflight.ReadinessError, match="group/world writable"):
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )

    artifact.unlink()
    artifact.mkdir()
    with pytest.raises(preflight.ReadinessError, match="stable regular non-symlink"):
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )


def test_filesystem_preflight_rejects_symlinked_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_policy(monkeypatch)
    artifact = _artifact(tmp_path)
    state = _state(tmp_path)
    artifact_link = tmp_path / "artifact-link.json"
    artifact_link.symlink_to(artifact)
    state_link = tmp_path / "state-link"
    state_link.symlink_to(state, target_is_directory=True)

    with pytest.raises(preflight.ReadinessError, match="must not contain symlinks"):
        preflight.validate_artifact(artifact_link)
    with pytest.raises(preflight.ReadinessError, match="must not contain symlinks"):
        preflight.validate_state_directory(state_link)


@pytest.mark.parametrize("inside", ("artifact", "state"))
def test_host_preflight_fence_requires_both_paths_outside_release_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, inside: str
) -> None:
    release = tmp_path / "release"
    release.mkdir()
    outside_artifact = _artifact(tmp_path)
    outside_state = _state(tmp_path)
    artifact = outside_artifact
    state = outside_state
    if inside == "artifact":
        artifact = release / "budget.json"
        artifact.write_text("{}\n", encoding="utf-8")
    else:
        state = release / "gateway-state"
        state.mkdir()
    monkeypatch.setattr(preflight, "validate_release_root", lambda path: path)

    with pytest.raises(preflight.ReadinessError, match="outside the release root"):
        preflight.require_outside_release_root(
            release,
            {
                "SofaScore artifact": artifact,
                "SofaScore gateway state": state,
            },
        )


def test_host_preflight_requires_release_root_argument(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    state = _state(tmp_path)
    with pytest.raises(SystemExit):
        preflight.run(
            [
                "preflight",
                "--artifact",
                str(artifact),
                "--state-dir",
                str(state),
            ]
        )


def test_host_preflight_rejects_unprotected_parent_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_policy(monkeypatch)
    unsafe_parent = tmp_path / "unsafe-parent"
    unsafe_parent.mkdir()
    unsafe_parent.chmod(0o777)
    artifact = _artifact(unsafe_parent)
    state = _state(unsafe_parent)
    with pytest.raises(preflight.ReadinessError, match="parent chain"):
        preflight.validate_artifact(
            artifact,
            require_protected_parents=True,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )
    with pytest.raises(preflight.ReadinessError, match="parent chain"):
        preflight.validate_state_directory(
            state,
            require_protected_parents=True,
            runtime_uid=state.stat().st_uid,
            runtime_gid=state.stat().st_gid,
        )


def test_artifact_rejects_unverified_runtime_and_wrong_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(tmp_path)

    def unavailable(_path: Path) -> None:
        raise WorkloadPolicyUnavailable("test detail must remain hidden")

    monkeypatch.setattr(preflight, "load_verified_workload_policy", unavailable)
    with pytest.raises(preflight.ReadinessError, match="current runtime") as raised:
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )
    assert "test detail" not in str(raised.value)

    _allow_policy(monkeypatch)
    with pytest.raises(preflight.ReadinessError, match="differs from the pin"):
        preflight.validate_artifact(
            artifact,
            expected_artifact_id="b" * 64,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )


def test_artifact_id_is_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_policy(monkeypatch)
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID")
    artifact = _artifact(tmp_path)

    with pytest.raises(preflight.ReadinessError, match="artifact ID is required"):
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )

    with pytest.raises(preflight.ReadinessError, match="artifact ID is required"):
        preflight.run(
            [
                "gateway-health",
                "--artifact",
                str(artifact),
                "--state-dir",
                str(tmp_path),
                "--health-url",
                "http://localhost:8899/health",
            ]
        )


def test_runtime_preflight_rejects_ci_zero_artifact_placeholder_before_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "0" * 64)
    monkeypatch.setattr(
        preflight,
        "require_runtime_identity",
        lambda: pytest.fail("runtime I/O must not start for the zero placeholder"),
    )

    with pytest.raises(preflight.ReadinessError, match="zero placeholder"):
        preflight.run(
            [
                "gateway-health",
                "--artifact",
                str(tmp_path / "artifact.json"),
                "--state-dir",
                str(tmp_path / "state"),
                "--health-url",
                "http://localhost:8899/health",
            ]
        )


def test_artifact_inode_replacement_during_fd_verification_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(tmp_path)
    replacement = tmp_path / "replacement.json"
    replacement.write_text('{"replacement":true}\n', encoding="utf-8")
    replacement.chmod(0o640)

    def replace_while_loading(fd_path: Path) -> SimpleNamespace:
        assert str(fd_path).startswith("/proc/self/fd/")
        replacement.replace(artifact)
        return SimpleNamespace(artifact_id=ARTIFACT_ID)

    monkeypatch.setattr(
        preflight, "load_verified_workload_policy", replace_while_loading
    )
    with pytest.raises(preflight.ReadinessError, match="changed during verification"):
        preflight.validate_artifact(
            artifact,
            runtime_uid=artifact.stat().st_uid,
            runtime_gid=artifact.stat().st_gid,
        )


@pytest.mark.parametrize("mode", (0o500, 0o707))
def test_state_directory_must_be_protected_writable_and_traversable(
    tmp_path: Path, mode: int
) -> None:
    state = _state(tmp_path, mode=mode)
    with pytest.raises(preflight.ReadinessError, match="writable|world writable"):
        preflight.validate_state_directory(
            state,
            runtime_uid=state.stat().st_uid,
            runtime_gid=state.stat().st_gid,
        )


def test_gateway_health_requires_paid_mode_positive_budget_and_same_artifact() -> None:
    preflight.validate_gateway_health(_health(), artifact_id=ARTIFACT_ID)

    with pytest.raises(preflight.ReadinessError, match="paid traffic disabled"):
        preflight.validate_gateway_health(
            _health(sofascore_paid_enabled=False), artifact_id=ARTIFACT_ID
        )
    with pytest.raises(preflight.ReadinessError, match="positive"):
        preflight.validate_gateway_health(
            _health(sofascore_dagrun_budget_bytes=0), artifact_id=ARTIFACT_ID
        )
    with pytest.raises(preflight.ReadinessError, match="different"):
        preflight.validate_gateway_health(_health(), artifact_id="b" * 64)
    with pytest.raises(preflight.ReadinessError, match="differs from the pin"):
        preflight.validate_gateway_health(
            _health(),
            artifact_id=ARTIFACT_ID,
            expected_artifact_id="b" * 64,
        )


def test_health_url_rejects_credentials_without_disclosing_them() -> None:
    url = "http://operator:do-not-log@localhost:8899/health"
    with pytest.raises(preflight.ReadinessError) as raised:
        preflight.read_gateway_health(url)
    assert "operator" not in str(raised.value)
    assert "do-not-log" not in str(raised.value)


def test_internal_health_uses_an_explicit_no_proxy_opener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, str]] = []

    class Response:
        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return b'{"status":"ok"}'

    class Opener:
        def open(self, _request: object, *, timeout: float) -> Response:
            assert timeout == 2.0
            return Response()

    def build_opener(handler: object) -> Opener:
        assert isinstance(handler, preflight.urllib.request.ProxyHandler)
        observed.append(handler.proxies)
        return Opener()

    monkeypatch.setenv("HTTPS_PROXY", "http://must-not-be-used.invalid:8080")
    monkeypatch.setattr(preflight.urllib.request, "build_opener", build_opener)

    assert preflight.read_gateway_health("http://localhost:8899/health") == {
        "status": "ok"
    }
    assert observed == [{}]


def test_runtime_identity_is_exact_uid_50000_gid_0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(preflight.os, "geteuid", lambda: preflight.RUNTIME_UID)
    monkeypatch.setattr(preflight.os, "getegid", lambda: preflight.RUNTIME_GID)
    preflight.require_runtime_identity()

    monkeypatch.setattr(preflight.os, "getegid", lambda: 50000)
    with pytest.raises(preflight.ReadinessError, match="UID 50000/GID 0"):
        preflight.require_runtime_identity()


def test_gateway_health_mode_checks_state_artifact_and_live_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(tmp_path)
    state = _state(tmp_path)
    calls: list[tuple[str, bool]] = []
    monkeypatch.setattr(preflight, "require_runtime_identity", lambda: None)
    monkeypatch.setattr(
        preflight,
        "validate_artifact",
        lambda *_args, **_kwargs: ARTIFACT_ID,
    )
    monkeypatch.setattr(
        preflight,
        "validate_state_directory",
        lambda path, *, probe_write=False: calls.append((str(path), probe_write)),
    )
    monkeypatch.setattr(preflight, "read_gateway_health", lambda _url: _health())

    report = preflight.run(
        [
            "gateway-health",
            "--artifact",
            str(artifact),
            "--state-dir",
            str(state),
            "--health-url",
            "http://localhost:8899/health",
        ]
    )

    assert report == {
        "artifact_id": ARTIFACT_ID,
        "mode": "gateway-health",
        "status": "ok",
    }
    assert calls == [(str(state), True)]


def test_scheduler_health_mode_keeps_airflow_job_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(tmp_path)
    called: list[str] = []
    monkeypatch.setattr(preflight, "require_runtime_identity", lambda: None)
    monkeypatch.setattr(
        preflight,
        "validate_artifact",
        lambda *_args, **_kwargs: ARTIFACT_ID,
    )
    monkeypatch.setattr(preflight, "read_gateway_health", lambda _url: _health())
    monkeypatch.setattr(
        preflight, "require_scheduler_job", lambda: called.append("scheduler")
    )

    report = preflight.run(
        [
            "scheduler-health",
            "--artifact",
            str(artifact),
            "--health-url",
            "http://sofascore_proxy_filter:8899/health",
        ]
    )

    assert report["status"] == "ok"
    assert called == ["scheduler"]


def test_scheduler_job_check_discards_output_and_uses_container_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def runner(command: list[str], **kwargs: object) -> SimpleNamespace:
        observed["command"] = command
        observed.update(kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setenv("HOSTNAME", "scheduler-container-id")
    monkeypatch.setattr(preflight.subprocess, "run", runner)
    preflight.require_scheduler_job()

    assert observed["command"] == [
        "airflow",
        "jobs",
        "check",
        "--job-type",
        "SchedulerJob",
        "--hostname",
        "scheduler-container-id",
    ]
    assert observed["stdout"] is subprocess.DEVNULL
    assert observed["stderr"] is subprocess.DEVNULL
