#!/usr/bin/env python3
"""Image-baked fail-closed production gate for the WhoScored workload.

This program deliberately lives outside every bind-mounted application path.
There is no environment, CLI, Airflow Variable or DagRun override.  Promotion
replaces the image-baked attestation only after its content-addressed build
provenance manifest has been reviewed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any


ATTESTATION_PATH = Path("/usr/local/share/whoscored/build-provenance-attestation.json")
PROVENANCE_MANIFEST_PATH = Path(
    "/usr/local/share/whoscored/build-provenance-manifest.json"
)
EXIT_CONFIG = 78
_DIGEST = re.compile(r"\A[0-9a-f]{64}\Z")
_IMAGE = re.compile(r"\A[^\s@]+@sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"\A[0-9a-f]{40}\Z")
_UTC_TIMESTAMP = re.compile(
    r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)
_SAFE_RELATIVE = re.compile(r"\A(?!/)(?!.*(?:^|/)\.\.(?:/|$))[^\x00]+\Z")
_STAGE = re.compile(r"\A(?:<default>|[A-Za-z0-9_.-]+)\Z")
_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024


class ProductionGateError(RuntimeError):
    """Raised when immutable production provenance cannot be proven."""


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey(key)
        value[key] = item
    return value


def _open_parent_without_symlinks(
    path: Path,
    *,
    expected_uid: int,
    enforce_immutable_parents: bool,
) -> tuple[int, str]:
    if not path.is_absolute() or not path.name:
        raise ProductionGateError("production evidence path must be absolute")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open("/", flags)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode) or (
                enforce_immutable_parents
                and (
                    metadata.st_uid != expected_uid
                    or stat.S_IMODE(metadata.st_mode) & 0o022
                )
            ):
                raise ProductionGateError(
                    "production evidence directory is not immutable"
                )
        return descriptor, path.name
    except BaseException:
        os.close(descriptor)
        raise


def _read_immutable_file(
    path: Path,
    *,
    expected_uid: int = 0,
    enforce_immutable_parents: bool = True,
) -> bytes:
    try:
        parent, name = _open_parent_without_symlinks(
            path,
            expected_uid=expected_uid,
            enforce_immutable_parents=enforce_immutable_parents,
        )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=parent)
        finally:
            os.close(parent)
    except (OSError, ProductionGateError) as exc:
        if isinstance(exc, ProductionGateError):
            raise
        raise ProductionGateError("production evidence is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != expected_uid
            or stat.S_IMODE(before.st_mode) != 0o444
            or before.st_size <= 0
            or before.st_size > _MAX_EVIDENCE_BYTES
        ):
            raise ProductionGateError(
                "production evidence owner, mode, type or size is invalid"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    except OSError as exc:
        raise ProductionGateError("cannot read production evidence") from exc
    finally:
        os.close(descriptor)
    stable = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable):
        raise ProductionGateError("production evidence changed while read")
    return b"".join(chunks)


def _canonical_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionGateError(f"{label} is not canonical JSON") from exc
    if not isinstance(value, dict):
        raise ProductionGateError(f"{label} must be a JSON object")
    canonical = (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")
    if raw != canonical:
        raise ProductionGateError(f"{label} bytes are not canonical")
    return value


def _digest_value(value: object) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _canonical_records(
    value: object,
    *,
    label: str,
    fields: set[str],
    identity: tuple[str, ...],
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ProductionGateError(f"{label} must be a non-empty list")
    records: list[dict[str, Any]] = []
    identities: list[tuple[str, ...]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != fields:
            raise ProductionGateError(f"{label} record schema is invalid")
        record_identity = tuple(str(item.get(field) or "") for field in identity)
        if any(not token for token in record_identity):
            raise ProductionGateError(f"{label} record identity is empty")
        records.append(item)
        identities.append(record_identity)
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise ProductionGateError(f"{label} records are not canonical and unique")
    return records


def _validate_provenance_manifest(manifest: dict[str, Any]) -> None:
    expected_fields = {
        "schema_version",
        "generated_at",
        "source_revision",
        "source_tree_sha256",
        "closure_report_sha256",
        "base_images",
        "apt_snapshots",
        "apt_packages",
        "downloaded_artifacts",
        "python_locks",
        "github_actions",
        "compose_images",
        "local_images",
    }
    if (
        set(manifest) != expected_fields
        or manifest.get("schema_version") != 1
        or not isinstance(manifest.get("generated_at"), str)
        or _UTC_TIMESTAMP.fullmatch(str(manifest.get("generated_at"))) is None
        or not isinstance(manifest.get("source_revision"), str)
        or _COMMIT.fullmatch(str(manifest.get("source_revision"))) is None
        or not _digest_value(manifest.get("source_tree_sha256"))
        or not _digest_value(manifest.get("closure_report_sha256"))
    ):
        raise ProductionGateError(
            "WhoScored build provenance manifest identity is invalid"
        )

    base_images = _canonical_records(
        manifest.get("base_images"),
        label="base_images",
        fields={"dockerfile", "stage", "image"},
        identity=("dockerfile", "stage"),
    )
    apt_snapshots = _canonical_records(
        manifest.get("apt_snapshots"),
        label="apt_snapshots",
        fields={"url", "release_sha256"},
        identity=("url",),
    )
    apt_packages = _canonical_records(
        manifest.get("apt_packages"),
        label="apt_packages",
        fields={"name", "version"},
        identity=("name",),
    )
    artifacts = _canonical_records(
        manifest.get("downloaded_artifacts"),
        label="downloaded_artifacts",
        fields={"name", "url", "sha256", "size"},
        identity=("name",),
    )
    locks = _canonical_records(
        manifest.get("python_locks"),
        label="python_locks",
        fields={"interpreter", "python_abi", "path", "sha256", "require_hashes"},
        identity=("interpreter", "path"),
    )
    actions = _canonical_records(
        manifest.get("github_actions"),
        label="github_actions",
        fields={"workflow", "uses", "commit"},
        identity=("workflow", "uses"),
    )
    compose_images = _canonical_records(
        manifest.get("compose_images"),
        label="compose_images",
        fields={"service", "image"},
        identity=("service",),
    )
    local_images = _canonical_records(
        manifest.get("local_images"),
        label="local_images",
        fields={
            "service",
            "context",
            "dockerfile",
            "target",
            "payload_target",
            "context_sha256",
            "base_image_sha256",
            "payload_image_id",
        },
        identity=("service",),
    )

    if any(
        _SAFE_RELATIVE.fullmatch(str(item["dockerfile"])) is None
        or _IMAGE.fullmatch(str(item["image"])) is None
        for item in base_images
    ):
        raise ProductionGateError("base image provenance is invalid")
    if any(
        not str(item["url"]).startswith("https://snapshot.debian.org/archive/")
        or not _digest_value(item["release_sha256"])
        for item in apt_snapshots
    ):
        raise ProductionGateError("APT snapshot provenance is invalid")
    if any(
        not isinstance(item["name"], str)
        or not item["name"]
        or not isinstance(item["version"], str)
        or not item["version"]
        or any(token in item["version"] for token in ("*", ">", "<", "~"))
        for item in apt_packages
    ):
        raise ProductionGateError("APT package provenance is invalid")
    if any(
        not str(item["url"]).startswith("https://")
        or not _digest_value(item["sha256"])
        or type(item["size"]) is not int
        or item["size"] <= 0
        for item in artifacts
    ):
        raise ProductionGateError("downloaded artifact provenance is invalid")
    if {str(item["interpreter"]) for item in locks} != {
        "airflow",
        "legacy-scraper",
    } or any(
        item["require_hashes"] is not True
        or not isinstance(item["python_abi"], str)
        or not item["python_abi"].startswith("cp")
        or _SAFE_RELATIVE.fullmatch(str(item["path"])) is None
        or not _digest_value(item["sha256"])
        for item in locks
    ):
        raise ProductionGateError("Python lock provenance is invalid")
    if any(
        _SAFE_RELATIVE.fullmatch(str(item["workflow"])) is None
        or not isinstance(item["uses"], str)
        or "/" not in item["uses"]
        or _COMMIT.fullmatch(str(item["commit"])) is None
        for item in actions
    ):
        raise ProductionGateError("GitHub Action provenance is invalid")
    if any(
        not isinstance(item["service"], str)
        or not item["service"]
        or _IMAGE.fullmatch(str(item["image"])) is None
        for item in compose_images
    ):
        raise ProductionGateError("Compose image provenance is invalid")
    if any(
        _SAFE_RELATIVE.fullmatch(str(item["context"])) is None
        or _SAFE_RELATIVE.fullmatch(str(item["dockerfile"])) is None
        or _STAGE.fullmatch(str(item["target"])) is None
        or _STAGE.fullmatch(str(item["payload_target"])) is None
        or not _digest_value(item["context_sha256"])
        or not _digest_value(item["base_image_sha256"])
        or not isinstance(item["payload_image_id"], str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", item["payload_image_id"]) is None
        for item in local_images
    ):
        raise ProductionGateError("local image provenance is invalid")


def validate_production_attestation(
    attestation_path: Path = ATTESTATION_PATH,
    manifest_path: Path = PROVENANCE_MANIFEST_PATH,
    *,
    expected_uid: int = 0,
    enforce_immutable_parents: bool = True,
) -> str:
    """Return the reviewed manifest digest or fail closed.

    The path parameters exist for the non-production unit harness only.
    ``main`` always calls this function with the two fixed image paths and UID
    zero; no production-facing input can select another attestation.
    """

    attestation_raw = _read_immutable_file(
        attestation_path,
        expected_uid=expected_uid,
        enforce_immutable_parents=enforce_immutable_parents,
    )
    attestation = _canonical_object(
        attestation_raw,
        label="WhoScored build provenance attestation",
    )
    expected_fields = {
        "schema_version",
        "status",
        "provenance_manifest_sha256",
    }
    digest = attestation.get("provenance_manifest_sha256")
    if (
        set(attestation) != expected_fields
        or attestation.get("schema_version") != 1
        or attestation.get("status") != "ready-v1"
        or not isinstance(digest, str)
        or _DIGEST.fullmatch(digest) is None
    ):
        raise ProductionGateError(
            "WhoScored immutable build provenance is not promoted"
        )
    manifest_raw = _read_immutable_file(
        manifest_path,
        expected_uid=expected_uid,
        enforce_immutable_parents=enforce_immutable_parents,
    )
    manifest = _canonical_object(
        manifest_raw,
        label="WhoScored build provenance manifest",
    )
    _validate_provenance_manifest(manifest)
    observed = hashlib.sha256(manifest_raw).hexdigest()
    if not hmac.compare_digest(observed, digest):
        raise ProductionGateError("WhoScored build provenance manifest digest differs")
    return observed


def main() -> int:
    try:
        validate_production_attestation()
    except ProductionGateError as exc:
        print(f"WhoScored production blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
