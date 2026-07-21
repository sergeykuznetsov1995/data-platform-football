from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "transition_seaweedfs_recovery_state.py"


def _module():
    spec = importlib.util.spec_from_file_location("seaweedfs_transition", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _state(path: Path, *, mode: str = "supervised-v1") -> bytes:
    payload = {
        "schema_version": 2,
        "mode": mode,
        "volume_name": "old-volume",
        "image_id": "sha256:" + "a" * 64,
        "inventory_sha256": "b" * 64,
        "volume_size_limit_mb": 1024,
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.write_bytes(encoded)
    path.chmod(0o644)
    return encoded


def _arguments(tmp_path: Path) -> list[str]:
    inventory = tmp_path / "inventory.json"
    if not inventory.exists():
        objects = [{"path": "raw/object.json", "bytes": 1, "sha256": "c" * 64}]
        objects_sha256 = hashlib.sha256(
            json.dumps(objects, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        payload = {
            "inventory_version": "whoscored-raw-inventory-v2",
            "created_at": "2026-07-15T12:00:02+00:00",
            "snapshot_started_at": "2026-07-15T12:00:00+00:00",
            "snapshot_completed_at": "2026-07-15T12:00:01+00:00",
            "snapshot_consistency": "localexecutor-cut-flock-v1",
            "source_uri": "s3://football",
            "object_count": 1,
            "total_bytes": 1,
            "objects_sha256": objects_sha256,
            "objects": objects,
        }
        payload["inventory_sha256"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        inventory.write_text(json.dumps(payload))
    inventory_sha256 = json.loads(inventory.read_text())["inventory_sha256"]
    return [
        "--state-file",
        str(tmp_path / "topology.mode"),
        "--lock-file",
        str(tmp_path / "topology.lock"),
        "--volume-name",
        "recovery-volume",
        "--image-id",
        "sha256:" + "a" * 64,
        "--inventory-file",
        str(inventory),
        "--expected-source-uri",
        "s3://football",
        "--inventory-sha256",
        inventory_sha256,
    ]


def _prepare(tmp_path: Path):
    mod = _module()
    mod.SEAWEEDFS_RECOVERY_TRANSITION_AVAILABLE = True
    original = _state(tmp_path / "topology.mode")
    lock = tmp_path / "topology.lock"
    lock.write_text("")
    lock.chmod(0o600)
    return mod, original


def test_recovery_transition_rejects_group_readable_lifecycle_lock(tmp_path):
    mod, original = _prepare(tmp_path)
    lock = tmp_path / "topology.lock"
    lock.chmod(0o640)

    with pytest.raises(SystemExit, match="lifecycle lock is not host-protected"):
        mod.main(_arguments(tmp_path))

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_recovery_transition_rejects_unprotected_state_directory(
    tmp_path, monkeypatch
):
    state_dir = tmp_path / "unprotected-state"
    state_dir.mkdir()
    mod = _module()
    mod.SEAWEEDFS_RECOVERY_TRANSITION_AVAILABLE = True
    original = _state(state_dir / "topology.mode")
    lock = tmp_path / "topology.lock"
    lock.write_text("")
    lock.chmod(0o600)
    arguments = _arguments(tmp_path)
    arguments[arguments.index(str(tmp_path / "topology.mode"))] = str(
        state_dir / "topology.mode"
    )
    state_dir.chmod(0o777)
    monkeypatch.setattr(
        mod,
        "_docker",
        lambda *_args, **_kwargs: pytest.fail("unprotected state called Docker"),
    )

    with pytest.raises(SystemExit, match="state directory is not host-protected"):
        mod.main(arguments)

    assert (state_dir / "topology.mode").read_bytes() == original


def test_recovery_transition_is_code_owned_disabled_without_side_effects(monkeypatch):
    mod = _module()
    monkeypatch.setattr(
        mod,
        "_docker",
        lambda *_args, **_kwargs: pytest.fail("disabled transition called Docker"),
    )

    with pytest.raises(
        SystemExit,
        match="code-owned disabled.*runtime-adoption.*recovery audits",
    ):
        mod.main([])


def test_transition_atomically_pins_exact_verified_volume(tmp_path, monkeypatch):
    mod, _original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)

    def docker(*args, check=True):
        if args[0] == "ps":
            return ""
        if args[:2] == ("image", "inspect"):
            return "sha256:" + "a" * 64
        if args[0] == "inspect":
            return ""
        return "ok"

    monkeypatch.setattr(mod, "_docker", docker)

    assert mod.main(_arguments(tmp_path)) == 0
    state = json.loads((tmp_path / "topology.mode").read_text())
    assert state["mode"] == "supervised-v1"
    assert state["volume_name"] == "recovery-volume"
    assert state["inventory_sha256"] == json.loads(
        (tmp_path / "inventory.json").read_text()
    )["inventory_sha256"]
    assert state["volume_size_limit_mb"] == 1024


@pytest.mark.parametrize("running_name", ["airflow-webserver", "seaweedfs-master"])
def test_running_writer_or_storage_rejects_without_changing_state(
    tmp_path, monkeypatch, running_name
):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda name: name == running_name)

    with pytest.raises(SystemExit, match="must (?:remain |be )stopped"):
        mod.main(_arguments(tmp_path))

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_running_oneoff_writer_rejects_recovery_transition(tmp_path, monkeypatch):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)

    def docker(*args, check=True):
        if args[0] == "ps":
            return "aaaaaaaaaaaa\tairflow-scheduler"
        return ""

    monkeypatch.setattr(mod, "_docker", docker)

    with pytest.raises(SystemExit, match="one-off writer.*blocks recovery"):
        mod.main(_arguments(tmp_path))

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_raw_recovery_volume_holder_rejects_transition(tmp_path, monkeypatch):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)

    def docker(*args, check=True):
        if args[0] == "ps" and "--all" in args:
            return "bbbbbbbbbbbb\traw-recovery-reader"
        if args[0] == "ps":
            return ""
        return ""

    monkeypatch.setattr(mod, "_docker", docker)

    with pytest.raises(SystemExit, match="unreviewed container"):
        mod.main(_arguments(tmp_path))

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_stopped_canonical_recovery_planes_are_allowed_before_removal(
    tmp_path, monkeypatch
):
    mod, _original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)

    def docker(*args, check=True):
        if args[0] == "ps" and "--all" in args:
            return "cccccccccccc\tseaweedfs-master"
        if args[0] == "ps":
            return ""
        if args[:3] == ("inspect", "--format", "{{.Image}}"):
            return "sha256:" + "a" * 64
        if args[:2] == ("inspect", "--format") and "/data" in args[2]:
            return "recovery-volume"
        if args[:2] == ("image", "inspect"):
            return "sha256:" + "a" * 64
        if args[0] == "inspect":
            return ""
        return "ok"

    monkeypatch.setattr(mod, "_docker", docker)

    assert mod.main(_arguments(tmp_path)) == 0


def test_wrong_inventory_evidence_leaves_state_byte_identical(tmp_path, monkeypatch):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)
    arguments = _arguments(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    inventory = json.loads(inventory_path.read_text())
    inventory["inventory_sha256"] = "d" * 64
    inventory_path.write_text(json.dumps(inventory))

    with pytest.raises(SystemExit, match="document checksum"):
        mod.main(arguments)

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_transition_rejects_reusing_failed_volume(tmp_path, monkeypatch):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)
    arguments = _arguments(tmp_path)
    arguments[arguments.index("recovery-volume")] = "old-volume"

    with pytest.raises(SystemExit, match="preserve and replace"):
        mod.main(arguments)

    assert (tmp_path / "topology.mode").read_bytes() == original


def test_wrong_marker_or_volume_failure_leaves_state_unchanged(tmp_path, monkeypatch):
    mod, original = _prepare(tmp_path)
    monkeypatch.setattr(mod, "_container_running", lambda _name: False)

    def docker(*args, check=True):
        if args[0] == "ps":
            return ""
        if args[:2] == ("image", "inspect"):
            return "sha256:" + "a" * 64
        if args[0] == "run":
            raise subprocess.CalledProcessError(1, args)
        if args[0] == "inspect":
            return ""
        return "ok"

    monkeypatch.setattr(mod, "_docker", docker)
    with pytest.raises(subprocess.CalledProcessError):
        mod.main(_arguments(tmp_path))
    assert (tmp_path / "topology.mode").read_bytes() == original


def test_lifecycle_lock_rejects_a_concurrent_transition(tmp_path):
    mod, _original = _prepare(tmp_path)
    lock = tmp_path / "topology.lock"
    first = mod._acquire_lock(lock)
    try:
        with pytest.raises(SystemExit, match="another SeaweedFS lifecycle"):
            mod._acquire_lock(lock)
    finally:
        first.close()
