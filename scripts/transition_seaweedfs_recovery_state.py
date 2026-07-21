#!/usr/bin/env python3
"""Atomically pin a verified recovery volume in protected topology state."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Sequence


VOLUME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
IMAGE_RE = re.compile(r"sha256:[0-9a-f]{64}")
INVENTORY_VERSION = "whoscored-raw-inventory-v2"
# Code-owned false until recovery can retain a server-side Read/List fence and
# protected pending state through catalog/schema, frozen 25-dataset DQ, and a
# direct-only application canary. Exact runtime adoption and the
# repository lifecycle/rollback audit are independent prerequisites.
# Environment variables cannot enable this.
SEAWEEDFS_RECOVERY_TRANSITION_AVAILABLE = False
WRITER_CONTAINERS = (
    "airflow-init",
    "airflow-scheduler",
    "airflow-webserver",
    "lakekeeper-migrate",
    "lakekeeper",
    "trino",
    "superset",
    "superset-worker",
    "superset-beat",
    "openmetadata-migrate",
    "openmetadata-server",
    "openmetadata-ingestion",
    "jupyterhub",
)
STORAGE_CONTAINERS = (
    "seaweedfs",
    "seaweedfs-s3",
    "seaweedfs-master",
    "seaweedfs-volume",
    "seaweedfs-filer",
)


def _validate_protected_directory(path: Path, *, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SystemExit(f"{label} directory must be pre-provisioned") from exc
    if not stat.S_ISDIR(info.st_mode) or path.is_symlink():
        raise SystemExit(f"{label} directory must be a non-symlink directory")
    if info.st_mode & 0o022 or info.st_uid not in {0, os.geteuid()}:
        raise SystemExit(f"{label} directory is not host-protected")


def _docker(*args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ("docker", *args),
        check=check,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _container_running(name: str) -> bool:
    value = _docker(
        "inspect", "--format", "{{.State.Running}}", name, check=False
    )
    return value == "true"


def _assert_no_storage_writer_oneoffs() -> None:
    output = _docker(
        "ps",
        "--filter",
        "label=com.docker.compose.oneoff=True",
        "--format",
        '{{.ID}}\t{{.Label "com.docker.compose.service"}}',
    )
    blocked_services = set(WRITER_CONTAINERS) | set(STORAGE_CONTAINERS)
    for line in output.splitlines():
        fields = line.split("\t")
        if (
            len(fields) != 2
            or re.fullmatch(r"[0-9a-f]{12,64}", fields[0]) is None
            or VOLUME_RE.fullmatch(fields[1]) is None
        ):
            raise SystemExit("cannot validate a running Compose one-off container")
        if fields[1] in blocked_services or fields[1].startswith("seaweedfs-"):
            raise SystemExit(
                f"running one-off writer {fields[0]} ({fields[1]}) blocks recovery"
            )


def _assert_recovery_volume_consumers(volume: str, image_id: str) -> None:
    output = _docker(
        "ps",
        "--all",
        "--filter",
        f"volume={volume}",
        "--format",
        "{{.ID}}\t{{.Names}}",
    )
    allowed = {"seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer"}
    seen: set[str] = set()
    for line in output.splitlines():
        fields = line.split("\t")
        if (
            len(fields) != 2
            or re.fullmatch(r"[0-9a-f]{12,64}", fields[0]) is None
            or VOLUME_RE.fullmatch(fields[1]) is None
            or fields[1] in seen
        ):
            raise SystemExit("cannot validate a recovery volume consumer")
        name = fields[1]
        seen.add(name)
        if name not in allowed:
            raise SystemExit("recovery volume is attached to an unreviewed container")
        if _container_running(name):
            raise SystemExit("recovery volume consumer must remain stopped")
        if _docker("inspect", "--format", "{{.Image}}", name) != image_id:
            raise SystemExit("recovery volume consumer uses another image")
        mounted_volume = _docker(
            "inspect",
            "--format",
            '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}',
            name,
        )
        if mounted_volume != volume:
            raise SystemExit("recovery volume consumer has an unexpected data mount")


def _load_state(path: Path) -> dict[str, Any]:
    _validate_protected_directory(path.parent, label="topology state")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        raise SystemExit("topology state must be a regular non-symlink file")
    if info.st_mode & 0o022:
        raise SystemExit("topology state must not be group/world writable")
    if info.st_uid not in {0, os.getuid()}:
        raise SystemExit("topology state has an unexpected owner")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if set(payload) != {
        "schema_version",
        "mode",
        "volume_name",
        "image_id",
        "inventory_sha256",
        "volume_size_limit_mb",
    }:
        raise SystemExit("topology state has unexpected fields")
    if payload.get("schema_version") != 2 or payload.get("mode") not in {
        "supervised-v1",
        "supervised-verification-pending-v1",
    }:
        raise SystemExit("topology state has an unexpected schema or mode")
    volume_size_limit_mb = payload.get("volume_size_limit_mb")
    if (
        not isinstance(volume_size_limit_mb, int)
        or isinstance(volume_size_limit_mb, bool)
        or not 1 <= volume_size_limit_mb <= 1_048_576
    ):
        raise SystemExit("topology state has an invalid volume size limit")
    return payload


def _acquire_lock(path: Path) -> Any:
    _validate_protected_directory(path.parent, label="lifecycle lock")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        raise SystemExit("lifecycle lock must be a regular non-symlink file")
    if info.st_mode & 0o077 or info.st_uid not in {0, os.getuid()}:
        raise SystemExit("lifecycle lock is not host-protected")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    handle = os.fdopen(descriptor, "rb", closefd=True)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise SystemExit("another SeaweedFS lifecycle operation is running") from exc
    return handle


def _use_inherited_lock(path: Path, descriptor: int) -> Any:
    _validate_protected_directory(path.parent, label="lifecycle lock")
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or path.is_symlink()
        or info.st_mode & 0o077
        or info.st_uid not in {0, os.getuid()}
    ):
        raise SystemExit("inherited lifecycle lock is not host-protected")
    try:
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}")).resolve()
    except OSError as exc:
        raise SystemExit("inherited lifecycle lock descriptor is invalid") from exc
    if target != path.resolve():
        raise SystemExit("inherited lifecycle lock points to another file")
    handle = os.fdopen(os.dup(descriptor), "rb", closefd=True)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise SystemExit("inherited lifecycle lock is not held by this session") from exc
    return handle


def _durable_replace(path: Path, payload: dict[str, Any]) -> None:
    parent = path.parent
    _validate_protected_directory(parent, label="topology state")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    try:
        os.fchmod(descriptor, 0o644)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _canonical_sha256(value: Any) -> str:
    rendered = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _load_validated_inventory(path: Path, *, expected_source_uri: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("inventory_version") != INVENTORY_VERSION:
        raise SystemExit("recovery inventory has an unsupported schema")
    if payload.get("source_uri") != expected_source_uri:
        raise SystemExit("recovery inventory belongs to another source URI")
    objects = payload.get("objects")
    if not isinstance(objects, list):
        raise SystemExit("recovery inventory objects must be a list")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in objects:
        if not isinstance(item, dict):
            raise SystemExit("recovery inventory object must be a mapping")
        object_path = item.get("path")
        size = item.get("bytes")
        checksum = item.get("sha256")
        if (
            not isinstance(object_path, str)
            or not object_path
            or PurePosixPath(object_path).is_absolute()
            or ".." in PurePosixPath(object_path).parts
            or object_path in seen
            or type(size) is not int
            or size < 0
            or not isinstance(checksum, str)
            or SHA256_RE.fullmatch(checksum) is None
        ):
            raise SystemExit("recovery inventory contains an invalid object")
        seen.add(object_path)
        normalized.append(
            {"path": object_path, "bytes": size, "sha256": checksum}
        )
    if normalized != sorted(normalized, key=lambda item: item["path"]):
        raise SystemExit("recovery inventory objects are not sorted")
    if (
        payload.get("object_count") != len(normalized)
        or payload.get("total_bytes") != sum(item["bytes"] for item in normalized)
        or payload.get("objects_sha256") != _canonical_sha256(normalized)
    ):
        raise SystemExit("recovery inventory object summary is invalid")
    digest_fields = {
        key: payload.get(key)
        for key in (
            "inventory_version",
            "created_at",
            "snapshot_started_at",
            "snapshot_completed_at",
            "snapshot_consistency",
            "source_uri",
            "object_count",
            "total_bytes",
            "objects_sha256",
            "objects",
        )
    }
    if payload.get("inventory_sha256") != _canonical_sha256(digest_fields):
        raise SystemExit("recovery inventory document checksum is invalid")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    if not SEAWEEDFS_RECOVERY_TRANSITION_AVAILABLE:
        raise SystemExit(
            "SeaweedFS recovery transition is code-owned disabled pending "
            "application/catalog attestation, runtime-adoption and recovery audits"
        )
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--volume-name", required=True)
    parser.add_argument("--inventory-sha256", required=True)
    parser.add_argument("--inventory-file", required=True)
    parser.add_argument("--expected-source-uri", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument(
        "--lock-file",
        default=os.environ.get(
            "SEAWEEDFS_CUTOVER_LOCK_FILE",
            "/var/lib/data-platform-football/seaweedfs-topology.lock",
        ),
    )
    parser.add_argument("--lock-fd", type=int)
    args = parser.parse_args(argv)
    state_path = Path(args.state_file)
    if not state_path.is_absolute():
        raise SystemExit("--state-file must be absolute")
    if VOLUME_RE.fullmatch(args.volume_name) is None:
        raise SystemExit("invalid recovery volume name")
    if SHA256_RE.fullmatch(args.inventory_sha256) is None:
        raise SystemExit("invalid recovery inventory SHA-256")
    if IMAGE_RE.fullmatch(args.image_id) is None:
        raise SystemExit("invalid recovery image ID")

    lock_path = Path(args.lock_file)
    if not lock_path.is_absolute():
        raise SystemExit("--lock-file must be absolute")
    lock_handle = (
        _use_inherited_lock(lock_path, args.lock_fd)
        if args.lock_fd is not None
        else _acquire_lock(lock_path)
    )
    current = _load_state(state_path)
    if current.get("image_id") != args.image_id:
        raise SystemExit("recovery cannot change the protected SeaweedFS image")
    if current.get("volume_name") == args.volume_name:
        raise SystemExit("recovery must preserve and replace the failed volume")
    if any(_container_running(name) for name in WRITER_CONTAINERS):
        raise SystemExit("every storage writer must remain stopped")
    if any(_container_running(name) for name in STORAGE_CONTAINERS):
        raise SystemExit("every SeaweedFS storage container must be stopped")
    _assert_no_storage_writer_oneoffs()
    _assert_recovery_volume_consumers(args.volume_name, args.image_id)
    inventory_path = Path(args.inventory_file)
    inventory_info = inventory_path.lstat()
    if not stat.S_ISREG(inventory_info.st_mode) or inventory_path.is_symlink():
        raise SystemExit("recovery inventory must be a regular non-symlink file")
    inventory = _load_validated_inventory(
        inventory_path,
        expected_source_uri=args.expected_source_uri,
    )
    if inventory.get("inventory_sha256") != args.inventory_sha256:
        raise SystemExit("recovery inventory content differs from its approved SHA-256")
    _docker("volume", "inspect", args.volume_name)
    if _docker("image", "inspect", "--format", "{{.Id}}", args.image_id) != args.image_id:
        raise SystemExit("protected SeaweedFS image is unavailable")
    for name in STORAGE_CONTAINERS:
        image_id = _docker("inspect", "--format", "{{.Image}}", name, check=False)
        if image_id and image_id != args.image_id:
            raise SystemExit(f"{name} runs another SeaweedFS image")
    expected_marker = f"full-bucket-inventory-v2:{args.inventory_sha256}"
    _docker(
        "run",
        "--rm",
        "--read-only",
        "--network",
        "none",
        "--entrypoint",
        "/bin/sh",
        "-v",
        f"{args.volume_name}:/data:ro",
        args.image_id,
        "-euc",
        'test ! -L /data/.supervised-topology-cutover-approved; '
        'test -f /data/.supervised-topology-cutover-approved; '
        'test "$(cat /data/.supervised-topology-cutover-approved)" = "$1"; '
        'test ! -L /data/mini.options; '
        'test -f /data/mini.options; '
        'test "$(grep -c "master[.]volumeSizeLimitMB" '
        '/data/mini.options || true)" = 1; '
        'test "$(grep "^master[.]volumeSizeLimitMB=[1-9][0-9]*$" '
        '/data/mini.options)" = "master.volumeSizeLimitMB=$2"',
        "_",
        expected_marker,
        str(current["volume_size_limit_mb"]),
    )
    for name in STORAGE_CONTAINERS:
        if _docker("inspect", name, check=False):
            _docker("rm", name)
    _durable_replace(
        state_path,
        {
            "schema_version": 2,
            "mode": "supervised-v1",
            "volume_name": args.volume_name,
            "image_id": args.image_id,
            "inventory_sha256": args.inventory_sha256,
            "volume_size_limit_mb": current["volume_size_limit_mb"],
        },
    )
    lock_handle.close()
    print(json.dumps({"status": "transitioned", "volume_name": args.volume_name}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
