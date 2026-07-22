#!/usr/bin/env python3
"""Validate the complete, immutable WhoScored production build closure.

The validator has three deliberately separate evidence checks:

* ``--expect-blocked`` accepts only the checked-in canonical ``blocked-v1``
  attestation and proves that unresolved mutable inputs still exist.
* ``--expect-ready-build`` accepts only repository-bound ``ready-v1`` build
  evidence, but does not authorize deployment.
* the default accepts only ``ready-v1`` after the manifest is an exact semantic
  representation of the repository inputs and a separate deployment
  attestation binds each payload-stage image ID to a final immutable image
  digest.

``--generate-ready`` is a deliberately separate release-preparation mode.  It
accepts explicit payload-stage image IDs, refuses every unresolved discovery
issue and every dirty path outside the two generated evidence files, then
publishes the manifest before the attestation.  A crash can therefore leave
only a fail-closed blocked/mismatched pair, never a ready attestation pointing
at older manifest bytes.  The default mode still requires the external
deployment attestation.

Local image records use ``payload_image_id``.  It is the image ID measured
before the image-baked gate and provenance files are copied into the final
image, avoiding a manifest/image self-reference.  The final digest is never
stored in the build manifest; it belongs to the external deployment
attestation.

Build-context hashes include every directory, regular file, and symlink.  The
only exclusions are the two exact generated provenance output paths listed in
``GENERATED_PROVENANCE_OUTPUTS``.  In particular, similarly named nested
files, ignore files, cache directories, and untracked files remain measured.

The implementation is standard-library only and performs no network access.
It writes only an explicit ``--report`` or, in ``--generate-ready`` mode, the
two exact canonical evidence paths.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


EXIT_CONFIG = 78
SCHEMA_VERSION = 1
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
GIT_CLI = Path("/usr/bin/git")
# Canonical SHA-1 empty tree.  Pinned Git resolves it without materializing an
# object; a Git build that cannot do so makes every provenance command fail closed.
_EMPTY_GIT_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
_SUBPROCESS_RUN = subprocess.run
_SUBPROCESS_DEVNULL = subprocess.DEVNULL
_SUBPROCESS_PIPE = subprocess.PIPE
_GIT_SAFE_CONFIG = (
    ("core.attributesFile", "/dev/null"),
    ("core.autocrlf", "false"),
    ("core.bare", "false"),
    ("core.checkStat", "default"),
    ("core.excludesFile", "/dev/null"),
    ("core.fileMode", "true"),
    ("core.fsmonitor", "false"),
    ("core.hooksPath", "/dev/null"),
    ("core.ignoreCase", "false"),
    ("core.ignoreStat", "false"),
    ("core.preloadIndex", "false"),
    ("core.sparseCheckout", "false"),
    ("core.sparseCheckoutCone", "false"),
    ("core.trustctime", "true"),
    ("credential.helper", ""),
    ("diff.external", "/bin/false"),
    ("diff.ignoreSubmodules", "none"),
    ("filter.lfs.clean", ""),
    ("filter.lfs.process", ""),
    ("filter.lfs.required", "false"),
    ("filter.lfs.smudge", ""),
    ("submodule.recurse", "false"),
)
REGULAR_FILE_IDENTITY_FIELDS = (
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
ATTESTATION_RELATIVE = Path(
    "docker/images/airflow/whoscored-build-provenance-attestation.json"
)
MANIFEST_RELATIVE = Path(
    "docker/images/airflow/whoscored-build-provenance-manifest.json"
)
GENERATED_PROVENANCE_OUTPUTS = frozenset(
    {
        ATTESTATION_RELATIVE.as_posix(),
        MANIFEST_RELATIVE.as_posix(),
    }
)
BLOCKED_ATTESTATION_BYTES = (
    b'{"provenance_manifest_sha256":"","schema_version":1,"status":"blocked-v1"}\n'
)
BLOCKED_MANIFEST_BYTES = b'{"schema_version":1,"status":"blocked-v1"}\n'
FLARESOLVERR_DOCKERFILE = Path("docker/images/flaresolverr-whoscored/Dockerfile")
FLARESOLVERR_CONTEXT_RULES = """*
!.dockerignore
!docker/
docker/*
!docker/images/
docker/images/*
!docker/images/flaresolverr-whoscored/
docker/images/flaresolverr-whoscored/*
!docker/images/flaresolverr-whoscored/Dockerfile
!docker/images/flaresolverr-whoscored/Dockerfile.dockerignore
!docker/images/flaresolverr-whoscored/entrypoint.sh
!scripts/
scripts/*
!scripts/flaresolverr_extended.py
"""
FLARESOLVERR_CONTEXT_FILES = frozenset(
    {
        ".dockerignore",
        "docker/images/flaresolverr-whoscored/Dockerfile",
        "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
        "docker/images/flaresolverr-whoscored/entrypoint.sh",
        "scripts/flaresolverr_extended.py",
    }
)
PRODUCTION_OVERLAY_RELATIVE = Path("compose.seaweedfs-supervised.yaml")
PROTECTED_PRODUCTION_SERVICES = frozenset(
    {
        "airflow-scheduler",
        "flaresolverr",
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    }
)
PROTECTED_SERVICE_FINAL_TARGETS = {
    "airflow-scheduler": "airflow-scheduler",
    "whoscored_paid_gateway": "airflow-whoscored-proxy",
    "whoscored_proxy_filter": "airflow-whoscored-proxy",
}
PROTECTED_SERVICE_BUILDS = {
    "airflow-scheduler": (
        "docker/images/airflow",
        "docker/images/airflow/Dockerfile",
        "airflow-scheduler",
        "data-platform-airflow-scheduler:2.11.2-whoscored",
    ),
    "flaresolverr": (
        ".",
        "docker/images/flaresolverr-whoscored/Dockerfile",
        "<default>",
        "data-platform-flaresolverr-whoscored:3.4.6",
    ),
    "flaresolverr_whoscored_paid": (
        ".",
        "docker/images/flaresolverr-whoscored/Dockerfile",
        "<default>",
        "data-platform-flaresolverr-whoscored:3.4.6",
    ),
    "whoscored_paid_gateway": (
        "docker/images/airflow",
        "docker/images/airflow/Dockerfile",
        "airflow-whoscored-proxy",
        "data-platform-airflow-whoscored-proxy:2.11.2-whoscored",
    ),
    "whoscored_proxy_filter": (
        "docker/images/airflow",
        "docker/images/airflow/Dockerfile",
        "airflow-whoscored-proxy",
        "data-platform-airflow-whoscored-proxy:2.11.2-whoscored",
    ),
}
PROTECTED_STAGE_RECIPE_SHA256 = {
    "airflow-scheduler": "f784ae95f5ac83d33cd52866e81a406a23cdb65fbdad3912168cd3ed85cabe6d",
    "flaresolverr": "e4e28b69572d38f4f877154e6bc7a6f8fae0906edb624b62860fff53d0bcab20",
    "flaresolverr_whoscored_paid": "e4e28b69572d38f4f877154e6bc7a6f8fae0906edb624b62860fff53d0bcab20",
    "whoscored_paid_gateway": "0cb2bdeed26cb9fe7cb33da3f50c1f29124ffcc07d31f9d09f69dc67f1b290b8",
    "whoscored_proxy_filter": "0cb2bdeed26cb9fe7cb33da3f50c1f29124ffcc07d31f9d09f69dc67f1b290b8",
}
WHOSCORED_PROXY_COMMAND = (
    "python",
    "/opt/airflow/scripts/proxy_filter/filter_proxy.py",
    "--source-mode",
    "whoscored-only",
    "--listen",
    "0.0.0.0:8899",
    "--lease-listen",
    "0.0.0.0:8900",
    "--lease-proxy-url",
    "http://whoscored_proxy_filter:8900",
    "--blocklist",
    "/opt/airflow/configs/proxy_filter/blocklist.txt",
    "--out",
    "/opt/airflow/state/whoscored-proxy-filter/bytes.json",
    "--daily-budget-bytes",
    "${WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES:?set exact provider-policy daily cap in decimal bytes}",
    "--max-lease-bytes",
    "${WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES:-2000000}",
    "--max-lease-ttl-seconds",
    "${WHOSCORED_PROXY_FILTER_MAX_LEASE_TTL_SECONDS:-3600}",
    "--dagrun-budget-bytes",
    "${WHOSCORED_PROXY_FILTER_DAGRUN_BUDGET_BYTES:-1000000000}",
    "--transfermarkt-dagrun-budget-bytes",
    "0",
    "--url-budget-bytes",
    "${WHOSCORED_PROXY_FILTER_URL_BUDGET_BYTES:-2000000}",
    "--max-active-leases",
    "${WHOSCORED_PROXY_FILTER_MAX_ACTIVE_LEASES:-2}",
    "--sofascore-canary-hard-cap-bytes",
    "0",
    "--sofascore-discovery-dagrun-budget-bytes",
    "0",
    "--ledger",
    "/opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl",
    "--whoscored-campaign-ledger",
    "/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json",
    "--whoscored-state-marker",
    "/opt/airflow/state/whoscored-proxy-filter/.whoscored_state_initialized.json",
)
WHOSCORED_PAID_GATEWAY_COMMAND = (
    "python",
    "/opt/airflow/scripts/whoscored_paid_gateway.py",
    "--host",
    "0.0.0.0",
    "--port",
    "8898",
    "--proxy-url",
    "http://whoscored_proxy_filter:8900",
    "--proxy-control-url",
    "http://whoscored_proxy_filter:8899",
    "--flaresolverr-url",
    "http://flaresolverr_whoscored_paid:8191",
)

_DIGEST = re.compile(r"\A[0-9a-f]{64}\Z")
_SHA256_ID = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"\A[0-9a-f]{40}\Z")
_PINNED_IMAGE = re.compile(r"\A[^\s@]+@sha256:[0-9a-f]{64}\Z")
_SNAPSHOT_URL = re.compile(
    r"\Ahttps://snapshot\.debian\.org/archive/[^/]+/"
    r"[0-9]{8}T[0-9]{6}Z(?:/|\Z)"
)
_URL = re.compile(r"https://[^\s\"'|;\\]+")
_REQUIREMENT_PIN = re.compile(r"\A[A-Za-z0-9_.-]+(?:\[[^\]]+\])?==([^\s;]+)")
_REQUIREMENT_HASH = re.compile(r"(?:\A|\s)--hash=sha256:([0-9a-f]{64})(?=\s|\Z)")
_UTC_TIMESTAMP = re.compile(
    r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)


class ProvenanceError(RuntimeError):
    """Raised when provenance cannot be established exactly."""


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey(key)
        value[key] = item
    return value


def canonical_bytes(value: object) -> bytes:
    """Return the one accepted JSON byte representation."""

    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _open_parent_directory(
    path: Path, *, label: str, require_protected_parents: bool
) -> tuple[int, str]:
    absolute = path.absolute()
    components = absolute.parts[1:]
    if not components:
        raise ProvenanceError(f"{label} is not a file path: {path}")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory_fd = -1
    try:
        directory_fd = os.open("/", directory_flags)
        for component in components[:-1]:
            child_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
            if require_protected_parents:
                metadata = os.fstat(directory_fd)
                writable = metadata.st_mode & 0o022
                sticky_root_directory = (
                    metadata.st_uid == 0
                    and metadata.st_mode & stat.S_ISVTX
                    and metadata.st_mode & 0o002
                )
                if metadata.st_uid != 0 or (writable and not sticky_root_directory):
                    raise ProvenanceError(
                        f"{label} has an unsafe parent directory: {path}"
                    )
        return directory_fd, components[-1]
    except Exception:
        if directory_fd >= 0:
            os.close(directory_fd)
        raise


def open_protected_parent(path: Path, *, label: str) -> tuple[int, str]:
    """Open a protected parent directory; the caller owns the returned fd."""

    return _open_parent_directory(path, label=label, require_protected_parents=True)


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Return the immutable metadata identity captured around one fd read."""

    return tuple(getattr(value, field) for field in REGULAR_FILE_IDENTITY_FIELDS)


def _read_fd_regular_file_snapshot(
    path: Path, *, label: str, require_protected_parents: bool
) -> tuple[bytes, tuple[int, ...]]:
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    directory_fd = -1
    file_fd = -1
    try:
        directory_fd, name = _open_parent_directory(
            path,
            label=label,
            require_protected_parents=require_protected_parents,
        )
        file_fd = os.open(name, file_flags, dir_fd=directory_fd)
        before = os.fstat(file_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or require_protected_parents
            and before.st_uid != 0
        ):
            raise ProvenanceError(f"{label} is not a protected regular file: {path}")
        chunks: list[bytes] = []
        while chunk := os.read(file_fd, 1024 * 1024):
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(file_fd)
        entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise ProvenanceError(f"{label} is missing or unreadable: {path}") from exc
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        if directory_fd >= 0:
            os.close(directory_fd)
    identity = _stat_identity(after)
    if _stat_identity(before) != identity or _stat_identity(entry) != identity:
        raise ProvenanceError(f"{label} changed while it was read: {path}")
    return raw, identity


def _read_fd_regular_file(
    path: Path, *, label: str, require_protected_parents: bool
) -> bytes:
    """Compatibility wrapper returning only bytes from one stable fd read."""

    raw, _ = _read_fd_regular_file_snapshot(
        path,
        label=label,
        require_protected_parents=require_protected_parents,
    )
    return raw


def read_stable_regular_file_snapshot(
    path: Path, *, label: str
) -> tuple[bytes, tuple[int, ...]]:
    """Read one stable regular file and return bytes plus its fd identity."""

    return _read_fd_regular_file_snapshot(
        path, label=label, require_protected_parents=False
    )


def _read_stable_regular_file(path: Path, *, label: str) -> bytes:
    raw, _ = read_stable_regular_file_snapshot(path, label=label)
    return raw


def read_protected_regular_file_snapshot(
    path: Path, *, label: str
) -> tuple[bytes, tuple[int, ...]]:
    """Read one protected file and return bytes plus its fd identity."""

    return _read_fd_regular_file_snapshot(
        path, label=label, require_protected_parents=True
    )


def read_protected_regular_file(path: Path, *, label: str) -> bytes:
    """Read one root-owned file through a no-symlink, fd-pinned path walk."""

    raw, _ = read_protected_regular_file_snapshot(path, label=label)
    return raw


def _replace_regular_file(path: Path, raw: bytes, *, label: str) -> None:
    """Atomically replace one existing regular file without following links."""

    directory_fd = -1
    temporary_fd = -1
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    try:
        directory_fd, name = _open_parent_directory(
            path,
            label=label,
            require_protected_parents=False,
        )
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or current.st_mode & 0o022
        ):
            raise ProvenanceError(f"{label} is not a protected regular file: {path}")
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory_fd,
        )
        offset = 0
        while offset < len(raw):
            written = os.write(temporary_fd, raw[offset:])
            if written <= 0:
                raise ProvenanceError(f"cannot write {label}: {path}")
            offset += written
        os.fchmod(temporary_fd, 0o644)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = -1
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    except OSError as exc:
        raise ProvenanceError(f"cannot publish {label}: {path}") from exc
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if directory_fd >= 0:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except OSError:
                pass
            os.close(directory_fd)


def _load_canonical_object_snapshot(
    path: Path, *, label: str, protected: bool = False
) -> tuple[dict[str, Any], bytes, tuple[int, ...]]:
    raw, identity = (
        read_protected_regular_file_snapshot(path, label=label)
        if protected
        else read_stable_regular_file_snapshot(path, label=label)
    )
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvenanceError(f"{label} is not canonical JSON: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value):
        raise ProvenanceError(f"{label} is not canonical JSON: {path}")
    return value, raw, identity


def _load_canonical_object(
    path: Path, *, label: str, protected: bool = False
) -> tuple[dict[str, Any], bytes]:
    value, raw, _ = _load_canonical_object_snapshot(
        path, label=label, protected=protected
    )
    return value, raw


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError) as exc:
        raise ProvenanceError(f"path escapes repository root: {path}") from exc


def _lexical_relative(root: Path, path: Path) -> str:
    try:
        return path.absolute().relative_to(root.absolute()).as_posix()
    except ValueError as exc:
        raise ProvenanceError(f"path escapes repository root: {path}") from exc


def _gitignore_match(pattern: str, relative: str) -> bool:
    anchored = pattern.startswith("/")
    if anchored:
        pattern = pattern[1:]
    if pattern.endswith("/"):
        pattern = f"{pattern}**"
    candidate = relative.strip("/")
    if "/" in pattern or anchored:
        base_candidate = relative.strip("/") if anchored else candidate
        return fnmatch.fnmatchcase(base_candidate, pattern)
    return any(fnmatch.fnmatchcase(part, pattern) for part in candidate.split("/"))


def _is_ignored(root: Path, path: Path) -> bool:
    """Evaluate the subset of gitignore syntax relevant to evidence files."""

    relative = Path(_relative(root, path))
    ignored = False
    parents = [Path(".")]
    current = relative.parent
    if current != Path("."):
        pieces = current.parts
        parents.extend(Path(*pieces[:index]) for index in range(1, len(pieces) + 1))
    for directory in parents:
        ignore_path = root / directory / ".gitignore"
        try:
            lines = ignore_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        try:
            local = relative.relative_to(directory).as_posix()
        except ValueError:
            continue
        for raw_pattern in lines:
            if not raw_pattern or raw_pattern.startswith("#"):
                continue
            negated = raw_pattern.startswith("!")
            pattern = raw_pattern[1:] if negated else raw_pattern
            if _gitignore_match(pattern, local):
                ignored = not negated
    return ignored


def _open_trusted_git() -> int:
    """Open the exact protected Git executable used for provenance reads."""

    directory_fd = -1
    executable_fd = -1
    admitted = False
    try:
        directory_fd, name = open_protected_parent(GIT_CLI, label="Git CLI")
        executable_fd = os.open(
            name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=directory_fd,
        )
        metadata = os.fstat(executable_fd)
        entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
            or metadata.st_mode & 0o111 == 0
            or _stat_identity(metadata) != _stat_identity(entry)
        ):
            raise ProvenanceError("Git CLI is not a protected executable")
        admitted = True
        return executable_fd
    except OSError as exc:
        raise ProvenanceError("Git provenance is unavailable") from exc
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)
        if executable_fd >= 0 and not admitted:
            os.close(executable_fd)


def _require_no_git_info_attributes(root: Path) -> None:
    """Reject the one repository attribute source that --attr-source cannot mask."""

    try:
        git_dir = _resolve_git_dir(root)
        candidates = [git_dir / "info/attributes"]
        common_marker = git_dir / "commondir"
        if common_marker.exists() or common_marker.is_symlink():
            if not common_marker.is_file() or common_marker.is_symlink():
                raise ProvenanceError("Git common metadata is unsafe")
            common_value = common_marker.read_text(encoding="utf-8").strip()
            if not common_value or "\0" in common_value:
                raise ProvenanceError("Git common metadata is unsafe")
            common = Path(common_value)
            if not common.is_absolute():
                common = git_dir / common
            candidates.append(common.resolve() / "info/attributes")
        if any(candidate.exists() or candidate.is_symlink() for candidate in candidates):
            raise ProvenanceError(
                "Git info attributes are unsupported for protected source validation"
            )
    except ProvenanceError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ProvenanceError("Git attributes cannot be validated safely") from exc


def _run_git(
    root: Path,
    *arguments: str,
    stdout: int = _SUBPROCESS_DEVNULL,
) -> subprocess.CompletedProcess[bytes]:
    """Run fd-pinned Git with no inherited process or repository executors."""

    if (
        not arguments
        or any(not isinstance(argument, str) or "\0" in argument for argument in arguments)
        or stdout not in {_SUBPROCESS_DEVNULL, _SUBPROCESS_PIPE}
    ):
        raise ProvenanceError("Git provenance command is invalid")
    canonical_root = root.resolve()
    _require_no_git_info_attributes(canonical_root)
    git_arguments = list(arguments)
    if git_arguments[0] == "diff":
        git_arguments[1:1] = ["--no-ext-diff", "--no-textconv"]
    safe_config = (*_GIT_SAFE_CONFIG, ("core.worktree", str(canonical_root)))
    config_arguments = tuple(
        argument
        for key, value in safe_config
        for argument in ("-c", f"{key}={value}")
    )
    environment = {
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_EDITOR": "/bin/false",
        "GIT_EXTERNAL_DIFF": "/bin/false",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_PAGER": "/bin/false",
        "GIT_SEQUENCE_EDITOR": "/bin/false",
        "GIT_SSH_COMMAND": "/bin/false",
        "GIT_TERMINAL_PROMPT": "0",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    git_fd = _open_trusted_git()
    try:
        return _SUBPROCESS_RUN(
            (
                f"/proc/self/fd/{git_fd}",
                "--no-pager",
                "--no-optional-locks",
                f"--attr-source={_EMPTY_GIT_TREE}",
                *config_arguments,
                "-C",
                str(canonical_root),
                *git_arguments,
            ),
            stdin=_SUBPROCESS_DEVNULL,
            stdout=stdout,
            stderr=_SUBPROCESS_DEVNULL,
            env=environment,
            pass_fds=(git_fd,),
            check=False,
        )
    except OSError as exc:
        raise ProvenanceError("Git provenance is unavailable") from exc
    finally:
        os.close(git_fd)


def _git_evidence_is_checked(root: Path, path: Path) -> bool:
    relative = _relative(root, path)
    commands = (
        ("ls-files", "--error-unmatch", "--", relative),
        ("diff", "--quiet", "HEAD", "--", relative),
    )
    for arguments in commands:
        try:
            result = _run_git(root, *arguments)
        except ProvenanceError:
            return False
        if result.returncode != 0:
            return False
    return True


def _git_output(root: Path, *arguments: str) -> str:
    result = _run_git(root, *arguments, stdout=_SUBPROCESS_PIPE)
    if result.returncode != 0:
        raise ProvenanceError(f"Git provenance command failed: {' '.join(arguments)}")
    try:
        return result.stdout.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ProvenanceError("Git provenance output is not UTF-8") from exc


def _git_nul_paths(root: Path, *arguments: str) -> set[str]:
    result = _run_git(root, *arguments, stdout=_SUBPROCESS_PIPE)
    if result.returncode != 0:
        raise ProvenanceError(f"Git provenance command failed: {' '.join(arguments)}")
    try:
        values = result.stdout.decode("utf-8").split("\0")
    except UnicodeDecodeError as exc:
        raise ProvenanceError("Git provenance output is not UTF-8") from exc
    return {value for value in values if value}


def _git_changed_paths(root: Path) -> set[str]:
    tracked = _git_nul_paths(root, "diff", "--name-only", "-z", "HEAD", "--")
    untracked = _git_nul_paths(
        root,
        "ls-files",
        "-z",
        "--others",
        "--exclude-standard",
        "--",
    )
    return tracked | untracked


def _validate_default_index_flags(root: Path) -> None:
    records = _git_nul_paths(root, "ls-files", "-v", "-z", "--")
    for record in records:
        if len(record) < 3 or record[1] != " ":
            raise ProvenanceError("Git index provenance output is malformed")
        marker = record[0]
        if marker == "S" or marker.islower():
            raise ProvenanceError(
                "Git index contains skip-worktree or assume-unchanged entries"
            )
    debug = _run_git(
        root, "ls-files", "--debug", "-z", "--", stdout=_SUBPROCESS_PIPE
    )
    if debug.returncode != 0:
        raise ProvenanceError("Git index flags are unavailable")
    flags = re.findall(rb"\tflags: ([0-9a-fA-F]+)\n", debug.stdout)
    if not flags or any(value != b"0" for value in flags):
        raise ProvenanceError("Git index contains non-default entry flags")


def _git_path_is_tracked(root: Path, path: Path) -> bool:
    relative = _relative(root, path)
    try:
        result = _run_git(root, "ls-files", "--error-unmatch", "--", relative)
    except ProvenanceError:
        return False
    return result.returncode == 0


def _git_blob(root: Path, revision: str, path: Path) -> bytes:
    if _COMMIT.fullmatch(revision) is None:
        raise ProvenanceError("Git blob revision is not a full commit")
    relative = _relative(root, path)
    tree = _run_git(
        root, "ls-tree", "-z", revision, "--", relative, stdout=_SUBPROCESS_PIPE
    )
    records = tree.stdout.split(b"\0") if tree.returncode == 0 else []
    if len(records) != 2 or records[1] or b"\t" not in records[0]:
        raise ProvenanceError(
            f"payload commit does not contain required evidence: {relative}"
        )
    metadata, encoded_path = records[0].split(b"\t", 1)
    fields = metadata.split()
    if (
        fields[:2] != [b"100644", b"blob"]
        or len(fields) != 3
        or encoded_path != relative.encode("utf-8")
    ):
        raise ProvenanceError(
            f"payload evidence is not a regular non-executable file: {relative}"
        )
    try:
        result = _run_git(
            root,
            "cat-file",
            "blob",
            fields[2].decode("ascii"),
            stdout=_SUBPROCESS_PIPE,
        )
    except UnicodeDecodeError as exc:
        raise ProvenanceError("Git provenance is unavailable") from exc
    if result.returncode != 0:
        raise ProvenanceError(f"cannot read payload evidence blob: {relative}")
    return result.stdout


def _validate_payload_blocked_evidence(root: Path, revision: str) -> None:
    expected = (
        (ATTESTATION_RELATIVE, BLOCKED_ATTESTATION_BYTES),
        (MANIFEST_RELATIVE, BLOCKED_MANIFEST_BYTES),
    )
    for relative, raw in expected:
        observed = _git_blob(root, revision, root / relative)
        if not hmac.compare_digest(observed, raw):
            raise ProvenanceError(
                "payload commit must contain the exact canonical blocked-v1 pair"
            )


def _validate_promotion_revision(
    root: Path,
    payload_revision: str,
    *,
    release_revision: str | None = None,
) -> None:
    if _COMMIT.fullmatch(payload_revision) is None:
        raise ProvenanceError("manifest source_revision is not a full payload commit")
    checkout_revision = source_revision(root)
    release = release_revision or checkout_revision
    if _COMMIT.fullmatch(release) is None:
        raise ProvenanceError("ready release revision is not a full commit")
    if release != checkout_revision:
        checkout_ancestry = _git_output(
            root, "rev-list", "--parents", "-n", "1", checkout_revision
        ).split()
        if (
            len(checkout_ancestry) != 3
            or checkout_ancestry[0] != checkout_revision
            or release not in checkout_ancestry[1:]
        ):
            raise ProvenanceError(
                "ready release revision is not a direct parent of the CI merge checkout"
            )
    ancestry = _git_output(root, "rev-list", "--parents", "-n", "1", release).split()
    if (
        len(ancestry) != 2
        or ancestry[0] != release
        or ancestry[1] != payload_revision
        or release == payload_revision
    ):
        raise ProvenanceError(
            "ready evidence must be a single-parent, immediate provenance-only "
            "child of the payload commit"
        )
    _validate_payload_blocked_evidence(root, payload_revision)
    changed = _git_nul_paths(
        root,
        "diff",
        "--name-only",
        "-z",
        payload_revision,
        release,
        "--",
    )
    if changed != set(GENERATED_PROVENANCE_OUTPUTS):
        raise ProvenanceError(
            "provenance promotion commit changes files outside the two generated outputs"
        )
    if _git_output(root, "status", "--porcelain", "--untracked-files=all"):
        raise ProvenanceError("ready provenance requires a clean working tree")


def _validate_material_revision(
    root: Path, payload_revision: str, paths: Iterable[Path]
) -> None:
    for path in sorted(
        {item.absolute() for item in paths},
        key=lambda item: _lexical_relative(root, item),
    ):
        relative = _lexical_relative(root, path)
        if relative in GENERATED_PROVENANCE_OUTPUTS:
            continue
        if not _git_evidence_is_checked_against(root, relative, payload_revision):
            raise ProvenanceError(
                f"build material is untracked or differs from payload revision: {relative}"
            )


def _git_evidence_is_checked_against(root: Path, relative: str, revision: str) -> bool:
    commands = (
        ("ls-files", "--error-unmatch", "--", relative),
        ("diff", "--quiet", revision, "--", relative),
    )
    for arguments in commands:
        try:
            result = _run_git(root, *arguments)
        except ProvenanceError:
            return False
        if result.returncode != 0:
            return False
    return True


def _require_tracked_evidence(
    root: Path, path: Path, *, label: str, require_checked: bool
) -> None:
    if not path.is_file():
        raise ProvenanceError(f"{label} is missing: {path}")
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return
    if _is_ignored(root, path):
        raise ProvenanceError(f"{label} is ignored by repository rules: {path}")
    if require_checked and not _git_evidence_is_checked(root, path):
        raise ProvenanceError(
            f"{label} is not tracked and identical to the checked revision: {path}"
        )


def _resolve_git_dir(root: Path) -> Path:
    marker = root / ".git"
    if marker.is_dir():
        return marker
    try:
        line = marker.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ProvenanceError("source revision is unavailable") from exc
    if not line.startswith("gitdir: "):
        raise ProvenanceError("source revision is unavailable")
    candidate = Path(line.removeprefix("gitdir: "))
    if not candidate.is_absolute():
        candidate = (root / candidate).resolve()
    return candidate


def source_revision(root: Path) -> str:
    git_dir = _resolve_git_dir(root)
    try:
        head = (git_dir / "HEAD").read_text(encoding="ascii").strip()
    except OSError as exc:
        raise ProvenanceError("source revision is unavailable") from exc
    if _COMMIT.fullmatch(head):
        return head
    if not head.startswith("ref: "):
        raise ProvenanceError("source revision is not a full commit")
    ref = head.removeprefix("ref: ")
    candidates = [git_dir / ref]
    common_marker = git_dir / "commondir"
    if common_marker.is_file():
        common = (git_dir / common_marker.read_text(encoding="utf-8").strip()).resolve()
        candidates.append(common / ref)
    else:
        common = git_dir
    for candidate in candidates:
        try:
            revision = candidate.read_text(encoding="ascii").strip()
        except OSError:
            continue
        if _COMMIT.fullmatch(revision):
            return revision
    packed = common / "packed-refs"
    try:
        packed_lines = packed.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise ProvenanceError("source revision ref is unavailable") from exc
    for line in packed_lines:
        if line.startswith(("#", "^")):
            continue
        parts = line.split(" ", 1)
        if len(parts) == 2 and parts[1] == ref and _COMMIT.fullmatch(parts[0]):
            return parts[0]
    raise ProvenanceError("source revision ref is unavailable")


@dataclass(frozen=True)
class LogicalLine:
    number: int
    text: str


@dataclass
class BuildConfig:
    service: str
    context: str
    dockerfile: str
    target: str
    image: str | None


@dataclass
class DockerfileClosure:
    bases_by_target: dict[str, list[str]]
    dependencies_by_stage: dict[str, set[str]]
    direct_base_by_stage: dict[str, str | None]
    generated_outputs_by_stage: dict[str, set[str]]
    instructions_by_stage: dict[str, list[str]]
    parent_by_stage: dict[str, str | None]
    stage_by_line: dict[int, str]
    final_stage: str


@dataclass(frozen=True)
class PipInvocation:
    interpreter: str
    requirement_paths: tuple[str, ...]
    require_hashes: bool
    lock_only: bool


@dataclass
class Discovery:
    root: Path
    revision: str
    records: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    issues: list[dict[str, str]] = field(default_factory=list)
    material_paths: set[Path] = field(default_factory=set)
    report: dict[str, Any] = field(default_factory=dict)
    build_attestation_raw: bytes | None = None
    build_attestation_identity: tuple[int, ...] | None = None
    build_manifest_raw: bytes | None = None
    build_manifest_identity: tuple[int, ...] | None = None
    deployment_attestation: Mapping[str, Any] | None = None
    deployment_attestation_raw: bytes | None = None
    deployment_attestation_identity: tuple[int, ...] | None = None
    deployment_final_images: Mapping[str, str] | None = None
    validated_release_revision: str | None = None
    validated_payload_revision: str | None = None
    validated_manifest_sha256: str | None = None
    validated_source_tree_sha256: str | None = None
    validated_payload_image_ids: Mapping[str, str] | None = None

    def issue(self, category: str, input_name: str, detail: str) -> None:
        self.issues.append(
            {"category": category, "detail": detail, "input": input_name}
        )


RECORD_KEYS = (
    "base_images",
    "apt_snapshots",
    "apt_packages",
    "downloaded_artifacts",
    "python_locks",
    "github_actions",
    "compose_images",
    "local_images",
)

_RECORD_IDENTITIES: dict[str, tuple[str, ...]] = {
    "base_images": ("dockerfile", "stage"),
    "apt_snapshots": ("url",),
    "apt_packages": ("name", "version"),
    "downloaded_artifacts": ("name",),
    "python_locks": ("interpreter", "path"),
    "github_actions": ("workflow", "uses", "commit"),
    "compose_images": ("service",),
    "local_images": ("service",),
}

_FIXED_PAYLOAD_TARGETS = {
    "airflow-scheduler": "airflow-scheduler-payload",
    "airflow-whoscored-proxy": "airflow-whoscored-proxy-payload",
}


def _logical_lines(text: str) -> list[LogicalLine]:
    result: list[LogicalLine] = []
    buffer: list[str] = []
    start = 0
    for number, raw in enumerate(text.splitlines(), 1):
        stripped = raw.rstrip()
        if not buffer:
            start = number
        if stripped.endswith("\\"):
            buffer.append(stripped[:-1])
            continue
        buffer.append(stripped)
        result.append(LogicalLine(start, " ".join(buffer)))
        buffer = []
    if buffer:
        result.append(LogicalLine(start, " ".join(buffer)))
    return result


def _expand(value: str, variables: Mapping[str, str]) -> str:
    variable = re.compile(
        r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))"
    )
    current = value
    for _ in range(8):
        expanded = variable.sub(
            lambda match: variables.get(
                match.group(1) or match.group(2), match.group(0)
            ),
            current,
        )
        if expanded == current:
            return expanded
        current = expanded
    return current


def _dockerfile_lines(path: Path, discovery: Discovery) -> list[LogicalLine]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ProvenanceError(f"Dockerfile is unavailable: {path}") from exc
    if re.search(r"(?im)^\s*#\s*(?:syntax|escape|check)\s*=", text):
        raise ProvenanceError(f"unsupported Dockerfile parser directive: {path}")
    discovery.material_paths.add(path)
    return _logical_lines(text)


def _canonical_mapping_keys(
    lines: Sequence[str], indent: int, *, description: str
) -> set[str]:
    prefix = " " * indent
    keys: set[str] = set()
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if "\t" in line:
            raise ProvenanceError(
                f"noncanonical YAML in {description}: tab indentation"
            )
        if len(line) - len(line.lstrip(" ")) != indent:
            continue
        match = re.match(rf"^{re.escape(prefix)}(<<|[A-Za-z0-9_.-]+):(?=\s|$)", line)
        if match is None:
            raise ProvenanceError(
                f"noncanonical YAML mapping key in {description}: {line.strip()}"
            )
        key = match.group(1)
        if key in keys:
            raise ProvenanceError(f"duplicate YAML mapping key in {description}: {key}")
        keys.add(key)
    return keys


def _validate_compose_top_level(lines: Sequence[str], *, description: str) -> None:
    keys = _canonical_mapping_keys(lines, 0, description=description)
    allowed = {
        "configs",
        "name",
        "networks",
        "secrets",
        "services",
        "version",
        "volumes",
    }
    unsupported = sorted(
        key for key in keys if key not in allowed and not key.startswith("x-")
    )
    if unsupported:
        raise ProvenanceError(
            f"unsupported top-level Compose input in {description}: {', '.join(unsupported)}"
        )
    if "services" not in keys:
        raise ProvenanceError(f"{description} has no services mapping")


def _parse_compose(
    root: Path, discovery: Discovery
) -> tuple[list[BuildConfig], list[tuple[Path, Path]]]:
    compose_path = root / "compose.yaml"
    try:
        lines = compose_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ProvenanceError("compose.yaml is unavailable") from exc
    discovery.material_paths.add(compose_path)
    _validate_compose_top_level(lines, description="compose.yaml")

    anchors: dict[str, dict[str, str | None]] = {}
    anchor_execution_overrides: set[str] = set()
    anchor_unmodelled_inputs: set[str] = set()
    index = 0
    while index < len(lines):
        match = re.match(
            r"^(x-[A-Za-z0-9_.-]+):\s*&([A-Za-z0-9_.-]+)\s*$", lines[index]
        )
        if not match:
            index += 1
            continue
        anchor = match.group(2)
        end = index + 1
        while end < len(lines) and (
            not lines[end].strip() or lines[end].startswith(" ")
        ):
            end += 1
        anchor_block = lines[index + 1 : end]
        anchor_keys = _canonical_mapping_keys(
            anchor_block, 2, description=f"Compose anchor {anchor}"
        )
        anchors[anchor] = _yaml_build_fields(anchor_block, base_indent=2)
        if anchor_keys & {"entrypoint", "command"}:
            anchor_execution_overrides.add(anchor)
        if anchor_keys & {"<<", "extends"}:
            anchor_unmodelled_inputs.add(anchor)
        index = end

    try:
        services_start = next(
            i for i, line in enumerate(lines) if line.strip() == "services:"
        )
    except StopIteration as exc:
        raise ProvenanceError("compose.yaml has no services mapping") from exc
    services: dict[str, dict[str, str | None]] = {}
    index = services_start + 1
    while index < len(lines):
        if lines[index].strip() and not lines[index].startswith(" "):
            break
        if not lines[index].strip() or lines[index].lstrip().startswith("#"):
            index += 1
            continue
        match = re.match(r"^  ([A-Za-z0-9_.-]+):\s*$", lines[index])
        if not match:
            if (
                lines[index].strip()
                and len(lines[index]) - len(lines[index].lstrip(" ")) == 2
            ):
                raise ProvenanceError(
                    f"unsupported compose service declaration: {lines[index].strip()}"
                )
            index += 1
            continue
        name = match.group(1)
        if name in services:
            raise ProvenanceError(f"duplicate compose service: {name}")
        end = index + 1
        while end < len(lines) and (
            not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip(" ")) > 2
        ):
            end += 1
        block = lines[index + 1 : end]
        service_keys = _canonical_mapping_keys(
            block, 4, description=f"Compose service {name}"
        )
        fields: dict[str, str | None] = {}
        merged_anchors: set[str] = set()
        for line in block:
            if re.match(r"^    <<:", line) and not re.match(
                r"^    <<:\s*\*([A-Za-z0-9_.-]+)\s*$", line
            ):
                raise ProvenanceError(f"unsupported Compose service merge: {name}")
            merged = re.match(r"^    <<:\s*\*([A-Za-z0-9_.-]+)\s*$", line)
            if merged:
                if merged.group(1) not in anchors:
                    raise ProvenanceError(
                        f"unsupported Compose service merge: {name}:{merged.group(1)}"
                    )
                merged_anchors.add(merged.group(1))
                fields.update(anchors[merged.group(1)])
        if merged_anchors & anchor_unmodelled_inputs:
            raise ProvenanceError(f"Compose service merges an unsafe anchor: {name}")
        if "extends" in service_keys:
            raise ProvenanceError(f"unsupported Compose extends input: {name}")
        if name in PROTECTED_PRODUCTION_SERVICES and any(
            "/usr/local/libexec/whoscored-python-real" in line for line in block
        ):
            raise ProvenanceError(
                f"protected service exposes the raw Python runtime: {name}"
            )
        if name in PROTECTED_PRODUCTION_SERVICES and (
            "entrypoint" in service_keys or merged_anchors & anchor_execution_overrides
        ):
            raise ProvenanceError(
                f"protected service overrides its image entrypoint: {name}"
            )
        command = _service_command(block)
        if (
            name in {"flaresolverr", "flaresolverr_whoscored_paid"}
            and command is not None
        ):
            raise ProvenanceError(
                "FlareSolverr Compose command bypasses baked preflight"
            )
        if name == "airflow-scheduler" and command != ("scheduler",):
            raise ProvenanceError(
                "scheduler Compose command differs from production policy"
            )
        if name == "whoscored_proxy_filter" and command != WHOSCORED_PROXY_COMMAND:
            raise ProvenanceError(
                "WhoScored proxy Compose command differs from production policy"
            )
        if (
            name == "whoscored_paid_gateway"
            and command != WHOSCORED_PAID_GATEWAY_COMMAND
        ):
            raise ProvenanceError(
                "WhoScored paid gateway Compose command differs from production policy"
            )
        fields.update(
            {
                key: value
                for key, value in _yaml_build_fields(block, 4).items()
                if value is not None
            }
        )
        services[name] = fields
        index = end

    _apply_production_overlay(root, discovery, services)

    direct_builds: dict[str, BuildConfig] = {}
    image_to_build: dict[str, BuildConfig] = {}
    for service, fields in services.items():
        context = fields.get("context")
        if context is None:
            if service in PROTECTED_SERVICE_BUILDS:
                raise ProvenanceError(
                    f"protected service has no direct canonical local build: {service}"
                )
            continue
        context_text = _clean_yaml_scalar(str(context))
        dockerfile_name = _clean_yaml_scalar(
            str(fields.get("dockerfile") or "Dockerfile")
        )
        target = _clean_yaml_scalar(str(fields.get("target") or "<default>"))
        if any("$" in value for value in (context_text, dockerfile_name, target)):
            raise ProvenanceError(
                f"local build identity uses unresolved Compose interpolation: {service}"
            )
        expected_target = PROTECTED_SERVICE_FINAL_TARGETS.get(service)
        if expected_target is not None and target != expected_target:
            raise ProvenanceError(
                f"protected service does not select its final gate target: {service}"
            )
        if target.endswith("-payload"):
            raise ProvenanceError(
                f"Compose service exposes a payload stage without its final gate: {service}"
            )
        image_value = fields.get("image")
        image_text = _clean_yaml_scalar(str(image_value)) if image_value else None
        if image_text and "$" in image_text:
            raise ProvenanceError(
                f"local build image tag uses unresolved Compose interpolation: {service}"
            )
        context_path = _safe_repository_path(root, context_text)
        dockerfile_path = _safe_repository_path(
            context_path, dockerfile_name, root=root
        )
        config = BuildConfig(
            service=service,
            context=_relative(root, context_path),
            dockerfile=_relative(root, dockerfile_path),
            target=target,
            image=image_text,
        )
        _validate_protected_build(config)
        direct_builds[service] = config
        if image_text:
            normalized_image = _normalized_image_tag(image_text)
            existing = image_to_build.get(normalized_image)
            if existing is not None and (
                existing.context,
                existing.dockerfile,
                existing.target,
            ) != (config.context, config.dockerfile, config.target):
                raise ProvenanceError(
                    f"local image tag has conflicting build producers: {image_text}"
                )
            image_to_build[normalized_image] = config

    local_builds: list[BuildConfig] = []
    for service, fields in services.items():
        config = direct_builds.get(service)
        image_value = fields.get("image")
        image_text = _clean_yaml_scalar(str(image_value)) if image_value else None
        normalized_image = (
            _normalized_image_tag(image_text)
            if image_text is not None and "@" not in image_text
            else None
        )
        if config is None and normalized_image in image_to_build:
            producer = image_to_build[str(normalized_image)]
            config = BuildConfig(
                service=service,
                context=producer.context,
                dockerfile=producer.dockerfile,
                target=producer.target,
                image=image_text,
            )
        if config is not None:
            _validate_resolved_service_target(service, config.target)
            local_builds.append(config)
            continue
        if not image_text:
            discovery.issue(
                "compose_image_without_digest",
                f"compose.yaml:{service}",
                "service has no statically resolved image or build closure",
            )
            continue
        if _PINNED_IMAGE.fullmatch(image_text):
            discovery.records["compose_images"].append(
                {"image": image_text, "service": service}
            )
        else:
            discovery.issue(
                "compose_image_without_digest",
                f"compose.yaml:{service}",
                f"third-party image is not pinned by @sha256: {image_text}",
            )
    build_graphs = sorted(
        {(root / item.dockerfile, root / item.context) for item in local_builds}
    )
    return local_builds, build_graphs


def _validate_resolved_service_target(service: str, target: str) -> None:
    expected_target = PROTECTED_SERVICE_FINAL_TARGETS.get(service)
    if expected_target is not None and target != expected_target:
        raise ProvenanceError(
            f"protected service does not resolve to its final gate target: {service}"
        )
    if target.endswith("-payload"):
        raise ProvenanceError(
            f"Compose service exposes a payload stage without its final gate: {service}"
        )


def _validate_protected_build(config: BuildConfig) -> None:
    expected = PROTECTED_SERVICE_BUILDS.get(config.service)
    if expected is None:
        return
    actual = (config.context, config.dockerfile, config.target, config.image)
    if actual != expected:
        raise ProvenanceError(
            f"protected service build identity differs from production policy: {config.service}"
        )


def _validate_protected_stage_recipe(
    config: BuildConfig, closure: DockerfileClosure
) -> None:
    expected = PROTECTED_STAGE_RECIPE_SHA256.get(config.service)
    if expected is None:
        return
    reachable = _reachable_stages(closure, config.target)
    recipe = [
        {"instructions": closure.instructions_by_stage.get(stage, []), "stage": stage}
        for stage in sorted(reachable)
    ]
    actual = hashlib.sha256(canonical_bytes(recipe)).hexdigest()
    if actual != expected:
        raise ProvenanceError(
            f"protected service final stage recipe differs from production policy: {config.service}"
        )


def _normalized_image_tag(image: str) -> str:
    if (
        "$" in image
        or "@" in image
        or image.lower() != image
        or re.fullmatch(
            r"[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9_][A-Za-z0-9_.-]*)?", image
        )
        is None
    ):
        raise ProvenanceError(f"noncanonical local image tag: {image}")
    last = image.rsplit("/", 1)[-1]
    if ":" not in last:
        raise ProvenanceError(f"local image tag must have an explicit tag: {image}")
    name, tag = image.rsplit(":", 1)
    parts = name.split("/")
    if len(parts) == 1:
        registry = "docker.io"
        repository = f"library/{parts[0]}"
    elif "." in parts[0] or ":" in parts[0] or parts[0] == "localhost":
        registry = "docker.io" if parts[0] == "index.docker.io" else parts[0]
        repository = "/".join(parts[1:])
    else:
        registry = "docker.io"
        repository = name
    if registry == "docker.io" and "/" not in repository:
        repository = f"library/{repository}"
    return f"{registry}/{repository}:{tag}"


def _service_command(block: Sequence[str]) -> tuple[str, ...] | None:
    for index, line in enumerate(block):
        match = re.match(r"^    command:\s*(.*?)\s*$", line)
        if not match:
            continue
        scalar = match.group(1)
        if scalar:
            return (_clean_yaml_scalar(scalar),)
        values: list[str] = []
        for nested in block[index + 1 :]:
            if nested.strip() and len(nested) - len(nested.lstrip(" ")) <= 4:
                break
            item = re.match(r"^      -\s+(.*?)\s*$", nested)
            if item:
                values.append(_clean_yaml_scalar(item.group(1)))
        return tuple(values)
    return None


def _apply_production_overlay(
    root: Path,
    discovery: Discovery,
    services: dict[str, dict[str, str | None]],
) -> None:
    path = root / PRODUCTION_OVERLAY_RELATIVE
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ProvenanceError(
            f"required production Compose overlay is unavailable: {path}"
        ) from exc
    discovery.material_paths.add(path)
    _validate_compose_top_level(lines, description="production Compose overlay")
    try:
        start = next(
            index for index, line in enumerate(lines) if line.strip() == "services:"
        )
    except StopIteration as exc:
        raise ProvenanceError(
            "production Compose overlay has no services mapping"
        ) from exc
    index = start + 1
    while index < len(lines):
        if lines[index].strip() and not lines[index].startswith(" "):
            break
        if not lines[index].strip() or lines[index].lstrip().startswith("#"):
            index += 1
            continue
        match = re.match(r"^  ([A-Za-z0-9_.-]+):\s*(.*?)\s*$", lines[index])
        if not match:
            if lines[index].strip():
                raise ProvenanceError(
                    f"unsupported production overlay service syntax: {lines[index].strip()}"
                )
            index += 1
            continue
        name, declaration = match.groups()
        end = index + 1
        while end < len(lines) and (
            not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip(" ")) > 2
        ):
            end += 1
        block = lines[index + 1 : end]
        overlay_keys = _canonical_mapping_keys(
            block, 4, description=f"production overlay service {name}"
        )
        if declaration:
            if declaration == "!reset null":
                if name in PROTECTED_PRODUCTION_SERVICES:
                    raise ProvenanceError(
                        f"production overlay removes protected service: {name}"
                    )
                services.pop(name, None)
                index = end
                continue
            raise ProvenanceError(
                f"unsupported production overlay service declaration: {name}: {declaration}"
            )
        meaningful_block = [
            line for line in block if line.strip() and not line.lstrip().startswith("#")
        ]
        if name in PROTECTED_PRODUCTION_SERVICES and meaningful_block:
            raise ProvenanceError(
                f"production overlay changes protected service: {name}"
            )
        if overlay_keys & {"<<", "extends"}:
            raise ProvenanceError(
                f"production overlay uses an unmodelled merge for service: {name}"
            )
        attested_override = bool(
            overlay_keys & {"image", "build", "platform", "pull_policy"}
        )
        if name in services:
            if attested_override:
                raise ProvenanceError(
                    f"production overlay overrides attested image/build surface: {name}"
                )
        else:
            fields = _yaml_build_fields(block, 4)
            if fields.get("image") is None and fields.get("context") is None:
                raise ProvenanceError(
                    f"new production overlay service has no attested image/build: {name}"
                )
            services[name] = fields
        index = end


def _yaml_build_fields(lines: Sequence[str], base_indent: int) -> dict[str, str | None]:
    prefix = " " * base_indent
    fields: dict[str, str | None] = {}
    in_build = False
    unsupported_service_fields = {"platform", "pull_policy"}
    supported_build_fields = {"context", "dockerfile", "target"}
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == base_indent:
            key_match = re.match(rf"^{re.escape(prefix)}([A-Za-z0-9_.-]+):", line)
            if key_match and key_match.group(1) in unsupported_service_fields:
                raise ProvenanceError(
                    f"unsupported input-bearing Compose field: {key_match.group(1)}"
                )
            match = re.match(rf"^{re.escape(prefix)}(image|build):\s*(.*?)\s*$", line)
            if match:
                key, value = match.groups()
                if key == "image":
                    if "image" in fields:
                        raise ProvenanceError("duplicate Compose image field")
                    fields["image"] = value
                    in_build = False
                elif value:
                    if "context" in fields:
                        raise ProvenanceError("duplicate Compose build field")
                    fields["context"] = value
                    in_build = False
                else:
                    in_build = True
                continue
            in_build = False
        elif in_build and indent == base_indent + 2:
            match = re.match(r"^\s+([A-Za-z0-9_.-]+):\s*(.*?)\s*$", line)
            if not match or match.group(1) not in supported_build_fields:
                field = match.group(1) if match else line.strip()
                raise ProvenanceError(
                    f"unsupported input-bearing Compose build field: {field}"
                )
            if match.group(1) in fields:
                raise ProvenanceError(
                    f"duplicate Compose build field: {match.group(1)}"
                )
            fields[match.group(1)] = match.group(2)
    return fields


def _clean_yaml_scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _safe_repository_path(base: Path, value: str, *, root: Path | None = None) -> Path:
    repository_root = (root or base).resolve()
    candidate = (base / value).resolve()
    try:
        candidate.relative_to(repository_root)
    except ValueError as exc:
        raise ProvenanceError(f"build path escapes repository: {value}") from exc
    return candidate


def _empty_discovery(root: Path, revision: str) -> Discovery:
    return Discovery(
        root=root,
        revision=revision,
        records={key: [] for key in RECORD_KEYS},
    )


def _merge_discovery(target: Discovery, source: Discovery) -> None:
    for key in RECORD_KEYS:
        target.records[key].extend(source.records[key])
    target.issues.extend(source.issues)
    target.material_paths.update(source.material_paths)


def _reachable_stages(closure: DockerfileClosure, target: str) -> set[str]:
    stage = closure.final_stage if target == "<default>" else target
    if stage not in closure.parent_by_stage:
        return set()
    reachable: set[str] = set()
    pending = [stage]
    while pending:
        current = pending.pop()
        if current in reachable:
            continue
        reachable.add(current)
        parent = closure.parent_by_stage.get(current)
        if parent is not None:
            pending.append(parent)
        pending.extend(closure.dependencies_by_stage.get(current, set()))
    return reachable


def _from_ancestry(closure: DockerfileClosure, target: str) -> set[str]:
    stage = closure.final_stage if target == "<default>" else target
    ancestry: set[str] = set()
    current: str | None = stage
    while current is not None and current not in ancestry:
        ancestry.add(current)
        current = closure.parent_by_stage.get(current)
    return ancestry if current is None else set()


def _scan_dockerfile(
    path: Path,
    context: Path,
    context_inputs: set[str],
    discovery: Discovery,
    *,
    selected_targets: set[str],
) -> DockerfileClosure:
    relative = _relative(discovery.root, path)
    variables: dict[str, str] = {}
    stage_bases: dict[str, list[str]] = {}
    stage_order: list[str] = []
    parent_by_stage: dict[str, str | None] = {}
    dependencies_by_stage: dict[str, set[str]] = {}
    direct_base_by_stage: dict[str, str | None] = {}
    generated_outputs_by_stage: dict[str, set[str]] = {}
    instructions_by_stage: dict[str, list[str]] = {}
    stage_by_line: dict[int, str] = {}
    stage_evidence: dict[str, Discovery] = {}
    current_stage = ""
    apt_commands: dict[str, list[tuple[int, str]]] = {}
    apt_environment_tainted: set[str] = set()
    snapshot_records: dict[str, list[dict[str, Any]]] = {}
    lines = _dockerfile_lines(path, discovery)
    for logical in lines:
        stripped = logical.text.strip()
        if not stripped or stripped.startswith("#"):
            continue
        instruction, _, body = stripped.partition(" ")
        instruction = instruction.upper()
        if instruction != "FROM" and current_stage:
            stage_by_line[logical.number] = current_stage
            instructions_by_stage[current_stage].append(stripped)
        if instruction == "ARG":
            evidence = stage_evidence.get(current_stage, discovery)
            evidence.issue(
                "local_image_provenance_absent",
                f"{relative}:{logical.number}",
                "Docker ARG is externally overrideable and is not represented in the manifest",
            )
            continue
        if instruction == "ENV":
            if not current_stage:
                raise ProvenanceError(
                    f"Docker ENV precedes the first FROM: {relative}:{logical.number}"
                )
            tokens = _shell_tokens(body)
            if tokens and all("=" in token for token in tokens):
                for token in tokens:
                    key, value = token.split("=", 1)
                    variables[key] = _expand(value, variables)
                    if key == "APT_CONFIG":
                        apt_environment_tainted.add(current_stage)
                        stage_evidence[current_stage].issue(
                            "apt_snapshot_mutable",
                            f"{relative}:{logical.number}:APT_CONFIG",
                            "Docker ENV may not redirect APT configuration",
                        )
            elif len(tokens) >= 2:
                variables[tokens[0]] = _expand(" ".join(tokens[1:]), variables)
                if tokens[0] == "APT_CONFIG":
                    apt_environment_tainted.add(current_stage)
                    stage_evidence[current_stage].issue(
                        "apt_snapshot_mutable",
                        f"{relative}:{logical.number}:APT_CONFIG",
                        "Docker ENV may not redirect APT configuration",
                    )
            continue
        expanded = _expand(body, variables)
        if instruction == "FROM":
            tokens = _shell_tokens(expanded)
            platform_tokens = [
                token for token in tokens if token.startswith("--platform=")
            ]
            tokens = [token for token in tokens if not token.startswith("--platform=")]
            if not tokens:
                discovery.issue(
                    "from_without_digest",
                    f"{relative}:{logical.number}",
                    "FROM is empty",
                )
                continue
            image = tokens[0]
            stage = f"stage-{logical.number}"
            if len(tokens) >= 3 and tokens[-2].upper() == "AS":
                stage = tokens[-1]
            if stage in stage_bases:
                raise ProvenanceError(
                    f"duplicate Docker stage name: {relative}:{stage}"
                )
            evidence = _empty_discovery(discovery.root, discovery.revision)
            stage_evidence[stage] = evidence
            if "$" in body:
                evidence.issue(
                    "from_without_digest",
                    f"{relative}:{logical.number}",
                    "FROM contains an externally mutable variable",
                )
            if platform_tokens:
                evidence.issue(
                    "local_image_provenance_absent",
                    f"{relative}:{logical.number}",
                    "FROM platform is not represented in the provenance schema",
                )
            if image in stage_bases:
                bases = list(stage_bases[image])
                parent = image
                direct_base = None
            elif image.isdigit() and int(image) < len(stage_order):
                parent = stage_order[int(image)]
                bases = list(stage_bases[parent])
                direct_base = None
            else:
                parent = None
                bases = []
                direct_base = image if _PINNED_IMAGE.fullmatch(image) else None
                if _PINNED_IMAGE.fullmatch(image):
                    evidence.records["base_images"].append(
                        {"dockerfile": relative, "image": image, "stage": stage}
                    )
                    bases.append(image)
                else:
                    evidence.issue(
                        "from_without_digest",
                        f"{relative}:{logical.number}",
                        f"FROM is not pinned by @sha256: {image}",
                    )
            stage_bases[stage] = bases
            parent_by_stage[stage] = parent
            dependencies_by_stage[stage] = set()
            direct_base_by_stage[stage] = direct_base
            generated_outputs_by_stage[stage] = set()
            instructions_by_stage[stage] = [stripped]
            stage_order.append(stage)
            current_stage = stage
            stage_by_line[logical.number] = stage
            continue
        if not current_stage:
            raise ProvenanceError(
                f"Docker instruction precedes the first FROM: {relative}:{logical.number}"
            )
        evidence = stage_evidence[current_stage]
        if instruction == "ADD":
            evidence.issue(
                "download_artifact_unverified",
                f"{relative}:{logical.number}",
                "ADD has implicit local/remote extraction semantics; use an explicit COPY or verified fetch",
            )
            continue
        if instruction == "COPY":
            _scan_copy_instruction(
                relative,
                logical.number,
                expanded,
                context=context,
                context_inputs=context_inputs,
                current_stage=current_stage,
                stage_bases=stage_bases,
                dependencies_by_stage=dependencies_by_stage,
                generated_outputs_by_stage=generated_outputs_by_stage,
                discovery=evidence,
            )
            continue
        if instruction != "RUN":
            if instruction in {"ONBUILD", "SHELL"}:
                evidence.issue(
                    "local_image_provenance_absent",
                    f"{relative}:{logical.number}",
                    f"unsupported input-bearing Docker instruction: {instruction}",
                )
            continue
        command, network_none, unsupported_run_option = _run_command(expanded)
        if unsupported_run_option:
            evidence.issue(
                "local_image_provenance_absent",
                f"{relative}:{logical.number}",
                "BuildKit RUN mounts/heredocs are not represented in the manifest",
            )
        if any(
            _segment_executable(segment) in {"apt", "apt-get"}
            and any(token in {"update", "install"} for token in segment)
            for segment in _shell_segments(command)
        ):
            apt_commands.setdefault(current_stage, []).append((logical.number, command))
        _scan_apt_packages(relative, logical.number, command, evidence)
        snapshot_records.setdefault(current_stage, []).extend(
            _scan_downloads(relative, logical.number, command, evidence)
        )
        if re.search(r"(?:^|\s)(?:\S*/)?camoufox\s+fetch(?:\s|$)", command):
            evidence.issue(
                "camoufox_unverified",
                f"{relative}:{logical.number}",
                "camoufox fetch resolves mutable browser artifacts internally",
            )
        _validate_run_network_boundary(
            relative,
            logical.number,
            command,
            network_none=network_none,
            discovery=evidence,
        )
        _scan_unmodelled_installers(relative, logical.number, command, evidence)
    closure = DockerfileClosure(
        bases_by_target=stage_bases,
        dependencies_by_stage=dependencies_by_stage,
        direct_base_by_stage=direct_base_by_stage,
        generated_outputs_by_stage=generated_outputs_by_stage,
        instructions_by_stage=instructions_by_stage,
        parent_by_stage=parent_by_stage,
        stage_by_line=stage_by_line,
        final_stage=stage_order[-1] if stage_order else "",
    )
    reachable = set().union(
        *(_reachable_stages(closure, target) for target in selected_targets)
    )
    for stage in sorted(reachable):
        commands = apt_commands.get(stage, [])
        if not commands:
            continue
        evidence = stage_evidence[stage]
        verified_snapshots = [
            item
            for item in snapshot_records.get(stage, [])
            if _SNAPSHOT_URL.match(str(item["url"]))
        ]
        used_snapshots: dict[str, dict[str, Any]] = {}
        for line, command in commands:
            snapshot = (
                None
                if stage in apt_environment_tainted
                else _apt_snapshot_consumed(command, verified_snapshots)
            )
            if snapshot is None:
                evidence.issue(
                    "apt_snapshot_mutable",
                    f"{relative}:{line}",
                    "APT command does not exclusively rewrite and consume the verified snapshot",
                )
            else:
                used_snapshots[str(snapshot["url"])] = snapshot
        for artifact in used_snapshots.values():
            evidence.records["apt_snapshots"].append(
                {
                    "release_sha256": artifact["sha256"],
                    "url": artifact["url"],
                }
            )
        if not used_snapshots:
            evidence.issue(
                "apt_snapshot_mutable",
                f"{relative}:{stage}",
                "APT is not bound to a timestamped snapshot Release URL and SHA-256",
            )
    for stage in sorted(reachable):
        _merge_discovery(discovery, stage_evidence[stage])
    if stage_order:
        stage_bases["<default>"] = list(stage_bases[stage_order[-1]])
    return closure


def _apt_snapshot_consumed(
    command: str, snapshots: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any] | None:
    if (
        "||" in command
        or ";" in command
        or "&" in command.replace("&&", "")
        or any(character in command for character in ("$", "\r", "\n"))
        or "\\" in command.replace("%s\\n", "")
    ):
        return None
    segments = _shell_segments(command)
    if len(segments) != 5 or command.count("&&") != 4:
        return None
    clear_sources, write_source, update, install, clear_lists = segments
    if clear_sources != [
        "rm",
        "-rf",
        "/etc/apt/sources.list.d",
        "/var/lib/apt/lists/*",
    ]:
        return None
    if (
        len(write_source) != 4
        or write_source[:2] != ["printf", "%s\\n"]
        or write_source[3] != ">/etc/apt/sources.list"
    ):
        return None
    source_match = re.fullmatch(
        r"deb (https://snapshot\.debian\.org/archive/[A-Za-z0-9_.+-]+/"
        r"[0-9]{8}T[0-9]{6}Z) ([A-Za-z0-9_.+-]+) "
        r"([A-Za-z0-9_.+-]+(?: [A-Za-z0-9_.+-]+)*)",
        write_source[2],
    )
    if source_match is None:
        return None
    source_url = source_match.group(1)
    suite = source_match.group(2)
    apt_source_options = [
        "-o",
        "Dir::Etc::sourcelist=/etc/apt/sources.list",
        "-o",
        "Dir::Etc::sourceparts=-",
        "-o",
        "Dir::Etc::main=-",
        "-o",
        "Dir::Etc::parts=-",
        "-o",
        "Dir::State::lists=/var/lib/apt/lists",
    ]
    apt_command = ["APT_CONFIG=/dev/null", "apt-get", *apt_source_options]
    if update != [*apt_command, "update"]:
        return None
    install_prefix = [
        *apt_command,
        "install",
        "-y",
        "--no-install-recommends",
    ]
    packages = install[len(install_prefix) :]
    if install[: len(install_prefix)] != install_prefix or not packages:
        return None
    if any(
        re.fullmatch(r"[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?=[^\s]+", package) is None
        for package in packages
    ):
        return None
    if clear_lists != ["rm", "-rf", "/var/lib/apt/lists/*"]:
        return None
    # The rewritten sources.list must contain the command's sole HTTPS input.
    # This prevents an unrelated snapshot URL (for example, a note written to
    # /tmp) from blessing an apt update that still consumes a mutable mirror.
    if re.findall(r"https?://[^\s\"'|;\\]+", command) != [source_url]:
        return None
    matches: list[Mapping[str, Any]] = []
    for snapshot in snapshots:
        url = str(snapshot["url"])
        if url == f"{source_url}/dists/{suite}/Release":
            matches.append(snapshot)
    return matches[0] if len(matches) == 1 else None


def _scan_unmodelled_installers(
    dockerfile: str, line: int, command: str, discovery: Discovery
) -> None:
    if command.lstrip().startswith("["):
        discovery.issue(
            "pip_install_without_hash_lock",
            f"{dockerfile}:{line}:exec-form",
            "JSON-form RUN package installation is not supported",
        )
    pattern = re.compile(
        r"\b(?:uv\s+pip\s+install|poetry\s+install|pipx\s+install|"
        r"(?:conda|mamba)\s+install|python\S*\s+[^;&|]*setup\.py\s+install|"
        r"dpkg\s+-i|rpm\s+-i|apk\s+add|(?:dnf|yum)\s+install)\b"
    )
    if pattern.search(command):
        discovery.issue(
            "pip_install_without_hash_lock",
            f"{dockerfile}:{line}:unmodelled-installer",
            "unmodelled package installer bypasses the exact lock closure",
        )


def _run_command(body: str) -> tuple[str, bool, bool]:
    value = body.lstrip()
    options: list[str] = []
    while value.startswith("--"):
        option, separator, remainder = value.partition(" ")
        options.append(option)
        if not separator:
            return "", False, True
        value = remainder.lstrip()
    network_options = [option for option in options if option.startswith("--network=")]
    network_none = network_options == ["--network=none"]
    unsupported = (
        value.startswith("<<")
        or len(network_options) > 1
        or any(
            option not in {"--network=default", "--network=host", "--network=none"}
            for option in network_options
        )
        or any(not option.startswith("--network=") for option in options)
    )
    return value, network_none, unsupported


def _validate_run_network_boundary(
    dockerfile: str,
    line: int,
    command: str,
    *,
    network_none: bool,
    discovery: Discovery,
) -> None:
    if network_none:
        return
    fetch_executables = {"curl", "wget", "apt", "apt-get"}
    pip_invocations = _pip_invocations(command)
    segments = _shell_segments(command)
    has_modelled_network = bool(pip_invocations) or any(
        _segment_executable(segment) in fetch_executables for segment in segments
    )
    if not has_modelled_network:
        discovery.issue(
            "local_image_provenance_absent",
            f"{dockerfile}:{line}",
            "non-fetch RUN must declare --network=none",
        )
        return
    allowed = {
        "apt",
        "apt-get",
        "chmod",
        "chown",
        "curl",
        "echo",
        "install",
        "mv",
        "printf",
        "rm",
        "sha256sum",
        "stat",
        "tar",
        "tee",
        "test",
        "wget",
    }
    for segment in segments:
        executable = _segment_executable(segment)
        if executable in allowed:
            continue
        if _pip_invocation_from_segment(segment) is not None:
            continue
        discovery.issue(
            "local_image_provenance_absent",
            f"{dockerfile}:{line}:{executable or '<unparsed>'}",
            "network-enabled RUN contains an unmodelled executable",
        )
    substitutions_removed = re.sub(r"\$\(\s*stat\b[^)]*\)", "", command)
    if "$(" in substitutions_removed or "`" in substitutions_removed:
        discovery.issue(
            "local_image_provenance_absent",
            f"{dockerfile}:{line}:substitution",
            "network-enabled RUN contains an unmodelled command substitution",
        )


def _scan_copy_instruction(
    dockerfile: str,
    line: int,
    body: str,
    *,
    context: Path,
    context_inputs: set[str],
    current_stage: str,
    stage_bases: dict[str, list[str]],
    dependencies_by_stage: dict[str, set[str]],
    generated_outputs_by_stage: dict[str, set[str]],
    discovery: Discovery,
) -> None:
    if body.lstrip().startswith("["):
        discovery.issue(
            "local_image_provenance_absent",
            f"{dockerfile}:{line}",
            "JSON-form COPY is not supported by the closure parser",
        )
        return
    tokens = _shell_tokens(body)
    from_value: str | None = None
    positional: list[str] = []
    for token in tokens:
        if token.startswith("--from="):
            from_value = token.split("=", 1)[1]
        elif not token.startswith("--"):
            positional.append(token)
    if len(positional) < 2:
        discovery.issue(
            "local_image_provenance_absent",
            f"{dockerfile}:{line}",
            "COPY sources and destination are not statically resolved",
        )
        return
    if from_value is not None:
        if from_value in stage_bases:
            dependencies_by_stage[current_stage].add(from_value)
            generated_outputs_by_stage[current_stage].update(
                generated_outputs_by_stage[from_value]
            )
            stage_bases[current_stage] = sorted(
                set(stage_bases[current_stage] + stage_bases[from_value])
            )
        elif from_value.isdigit():
            discovery.issue(
                "local_image_provenance_absent",
                f"{dockerfile}:{line}",
                "numeric COPY --from is ambiguous to the closure parser",
            )
        elif _PINNED_IMAGE.fullmatch(from_value):
            image_stage = f"copy-{line}"
            discovery.records["base_images"].append(
                {"dockerfile": dockerfile, "image": from_value, "stage": image_stage}
            )
            stage_bases[current_stage] = sorted(
                set(stage_bases[current_stage] + [from_value])
            )
        else:
            discovery.issue(
                "from_without_digest",
                f"{dockerfile}:{line}",
                f"external COPY --from is not pinned by @sha256: {from_value}",
            )
        return

    generated = {
        (discovery.root / relative).absolute()
        for relative in GENERATED_PROVENANCE_OUTPUTS
    }
    for source in positional[:-1]:
        if any(character in source for character in "*?["):
            discovery.issue(
                "local_image_provenance_absent",
                f"{dockerfile}:{line}:{source}",
                "globbed COPY can include unmeasured generated provenance outputs",
            )
            continue
        candidate = (context / source).absolute()
        try:
            candidate.relative_to(context.absolute())
        except ValueError:
            discovery.issue(
                "local_image_provenance_absent",
                f"{dockerfile}:{line}:{source}",
                "COPY source escapes the measured build context",
            )
            continue
        exact_generated = {
            _lexical_relative(discovery.root, output)
            for output in generated
            if output == candidate
        }
        if exact_generated:
            generated_outputs_by_stage[current_stage].update(exact_generated)
            continue
        candidate_relative = candidate.relative_to(context.absolute()).as_posix()
        if candidate.is_file() and candidate_relative not in context_inputs:
            discovery.issue(
                "local_image_provenance_absent",
                f"{dockerfile}:{line}:{source}",
                "COPY source is excluded from the effective Docker context",
            )
            continue
        if any(output.is_relative_to(candidate) for output in generated):
            discovery.issue(
                "local_image_provenance_absent",
                f"{dockerfile}:{line}:{source}",
                "COPY can place excluded generated evidence in the payload stage",
            )


def _scan_apt_packages(
    dockerfile: str, line: int, command: str, discovery: Discovery
) -> None:
    for segment in _shell_segments(command):
        if _segment_executable(segment) not in {"apt", "apt-get"}:
            continue
        try:
            install_index = segment.index("install")
        except ValueError:
            continue
        for token in segment[install_index + 1 :]:
            if token.startswith("-") or token in {"\\"}:
                continue
            if "=" in token:
                name, version = token.split("=", 1)
                if name and version and not any(char in version for char in "*?[]<>"):
                    discovery.records["apt_packages"].append(
                        {"name": name, "version": version}
                    )
                    continue
            discovery.issue(
                "apt_package_unversioned",
                f"{dockerfile}:{line}:{token}",
                "APT package is not pinned with name=version",
            )


def _scan_downloads(
    dockerfile: str, line: int, command: str, discovery: Discovery
) -> list[dict[str, Any]]:
    segments = _shell_segments(command)
    fetches: list[tuple[str, list[str]]] = []
    for segment in segments:
        executable = _segment_executable(segment)
        if executable in {"curl", "wget"}:
            fetches.append((executable, segment))
    all_urls = [url.rstrip(")],") for url in _URL.findall(command)]
    fetch_urls = {
        url.rstrip(")],")
        for _, tokens in fetches
        for token in tokens
        for url in _URL.findall(token)
    }
    unexplained_urls = [
        url
        for url in all_urls
        if url not in fetch_urls
        and not (
            _SNAPSHOT_URL.match(url)
            and re.search(r"\bapt(?:-get)?\b", command) is not None
        )
    ]
    risky_network = re.search(
        r"\b(?:git\s+(?:clone|fetch|pull|submodule)|urlopen|urllib\.|"
        r"requests\.(?:get|post|put|delete)|httpx\.|npm\s+(?:install|ci)|"
        r"yarn\s+install|mvn\s+|aws\s+s3|scp\s+|sftp\s+|ftp\s+|"
        r"nc\s+|ncat\s+|socat\s+|svn\s+|hg\s+|cargo\s+|go\s+(?:get|install))\b"
        r"|/dev/(?:tcp|udp)/",
        command,
    )
    if unexplained_urls or risky_network:
        discovery.issue(
            "download_artifact_unverified",
            f"{dockerfile}:{line}",
            "network-capable RUN input is not a canonical verified file fetch",
        )
    records: list[dict[str, Any]] = []
    for executable, tokens in fetches:
        urls = [url.rstrip(")],") for token in tokens for url in _URL.findall(token)]
        output = _fetch_output_path(executable, tokens)
        url = urls[0] if len(urls) == 1 else "<unresolved-url>"
        name = f"{dockerfile}:{line}:{url.rsplit('/', 1)[-1] or 'artifact'}"
        hashes = _hashes_bound_to_path(command, output, url) if output else []
        size = _size_bound_to_path(command, output, url) if output else None
        canonical_chain = (
            _canonical_fetch_receipt(command, executable) if output else False
        )
        if (
            len(urls) != 1
            or output is None
            or len(hashes) != 1
            or size is None
            or not canonical_chain
        ):
            missing: list[str] = []
            if len(urls) != 1:
                missing.append("exactly one HTTPS URL")
            if output is None:
                missing.append("explicit output path")
            if len(hashes) != 1:
                missing.append("SHA-256 check bound to the output path")
            if size is None:
                missing.append("byte-size check bound to the output path")
            if not canonical_chain:
                missing.append("exclusive fail-fast fetch→hash→size chain")
            discovery.issue(
                "download_artifact_unverified",
                name,
                f"download lacks {', '.join(missing)}",
            )
            continue
        record = {"name": name, "sha256": hashes[0], "size": size, "url": url}
        discovery.records["downloaded_artifacts"].append(record)
        records.append(record)
    return records


def _canonical_fetch_receipt(command: str, executable: str) -> bool:
    if executable != "curl":
        return False
    segments, operators = _shell_chain(command)
    if operators != ["&&", "|", "&&"] or len(segments) != 4:
        return False
    fetch, checksum, verifier, size_check = segments
    if len(fetch) < 5:
        return False
    curl_prefix = fetch[:-3]
    if curl_prefix not in (
        ["curl", "-fsSL"],
        [
            "curl",
            "--proto",
            "=https",
            "--tlsv1.2",
            "--proto-redir",
            "=https",
            "-fsSL",
        ],
        [
            "curl",
            "--proto",
            "=https",
            "--tlsv1.2",
            "--proto-redir",
            "=https",
            "--connect-timeout",
            "20",
            "--speed-limit",
            "1024",
            "--speed-time",
            "60",
            "--max-time",
            "3600",
            "-fsSL",
        ],
    ):
        return False
    url, output_flag, output = fetch[-3:]
    if (
        _URL.fullmatch(url) is None
        or output_flag != "-o"
        or re.fullmatch(r"/[A-Za-z0-9_./-]+", output) is None
    ):
        return False
    if verifier != ["sha256sum", "-c", "-"]:
        return False
    if len(checksum) != 2 or checksum[0] != "echo":
        return False
    checksum_match = re.fullmatch(
        rf"([0-9a-f]{{64}})  {re.escape(output)}", checksum[1]
    )
    if checksum_match is None:
        return False
    return (
        len(size_check) == 4
        and size_check[:3] == ["test", f"$(stat -c %s {output})", "-eq"]
        and re.fullmatch(r"[1-9][0-9]*", size_check[3]) is not None
    )


def _shell_chain(command: str) -> tuple[list[list[str]], list[str]]:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        return [[command]], []
    segments: list[list[str]] = []
    operators: list[str] = []
    current: list[str] = []
    for token in tokens:
        if token and all(character in ";&|" for character in token):
            if not current:
                return [[command]], []
            segments.append(current)
            current = []
            operators.append(token)
        else:
            current.append(token)
    if not current:
        return [[command]], []
    segments.append(current)
    return segments, operators


def _shell_segments(command: str) -> list[list[str]]:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        return [[command]]
    segments: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        if token and all(character in ";&|" for character in token):
            if current:
                segments.append(current)
                current = []
        else:
            current.append(token)
    if current:
        segments.append(current)
    return segments


def _segment_executable(tokens: Sequence[str]) -> str:
    index = _segment_executable_index(tokens)
    return "" if index is None else Path(tokens[index]).name


def _segment_executable_index(tokens: Sequence[str]) -> int | None:
    index = 0
    while index < len(tokens) and re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*=.*", tokens[index]
    ):
        index += 1
    if index < len(tokens) and tokens[index] in {"command", "env", "sudo"}:
        index += 1
        while index < len(tokens) and (
            tokens[index].startswith("-")
            or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", tokens[index])
        ):
            index += 1
    if index >= len(tokens):
        return None
    return index


def _fetch_output_path(executable: str, tokens: Sequence[str]) -> str | None:
    options = {"curl": {"-o", "--output"}, "wget": {"-O", "--output-document"}}
    candidates: list[str] = []
    for index, token in enumerate(tokens):
        if token in options[executable] and index + 1 < len(tokens):
            candidates.append(tokens[index + 1])
        elif executable == "curl" and token.startswith("--output="):
            candidates.append(token.split("=", 1)[1])
        elif executable == "wget" and token.startswith("--output-document="):
            candidates.append(token.split("=", 1)[1])
    if len(candidates) != 1 or candidates[0] == "-":
        return None
    return candidates[0]


def _hashes_bound_to_path(command: str, output: str, url: str) -> list[str]:
    verifier = re.search(r"\bsha256sum\b[^;&]*?(?:-c|--check)\b", command)
    if verifier is None or command.find(url) >= verifier.start():
        return []
    pattern = re.compile(
        rf"(?<![0-9a-f])([0-9a-f]{{64}})(?![0-9a-f])\s+\*?{re.escape(output)}"
    )
    return sorted(set(pattern.findall(command)))


def _size_bound_to_path(command: str, output: str, url: str) -> int | None:
    pattern = re.compile(
        rf"\bstat\s+-c\s+[\"']?%s[\"']?\s+[\"']?{re.escape(output)}[\"']?"
        rf"(?:(?!&&|;|\|).){{0,48}}?-eq\s+[\"']?([1-9][0-9]+)",
    )
    values = {
        int(match.group(1))
        for match in pattern.finditer(command)
        if command.find(url) < match.start()
    }
    return next(iter(values)) if len(values) == 1 else None


def _shell_tokens(value: str) -> list[str]:
    try:
        return shlex.split(value, comments=False, posix=True)
    except ValueError:
        return value.split()


def _copy_map_by_stage(
    lines: Sequence[LogicalLine], context: Path, stage_by_line: Mapping[int, str]
) -> dict[str, dict[str, Path]]:
    mapping: dict[str, dict[str, Path]] = {}
    for logical in lines:
        stage = stage_by_line.get(logical.number)
        if stage is None:
            continue
        stripped = logical.text.strip()
        if not stripped.upper().startswith("COPY "):
            continue
        tokens = _shell_tokens(stripped[5:])
        if any(token.startswith("--from=") for token in tokens):
            continue
        tokens = [token for token in tokens if not token.startswith("--")]
        if len(tokens) != 2:
            continue
        source, destination = tokens
        source_path = (context / source).resolve()
        try:
            source_path.relative_to(context.resolve())
        except ValueError:
            continue
        mapping.setdefault(stage, {})[destination] = source_path
    return mapping


def _copied_repository_path(
    container_path: str,
    stage: str,
    copies: Mapping[str, Mapping[str, Path]],
    closure: DockerfileClosure,
) -> Path | None:
    current: str | None = stage
    seen: set[str] = set()
    while current is not None and current not in seen:
        seen.add(current)
        candidate = copies.get(current, {}).get(container_path)
        if candidate is not None:
            return candidate
        current = closure.parent_by_stage.get(current)
    return None


def _python_abi_for_stage(closure: DockerfileClosure, stage: str) -> str:
    proofs: list[tuple[int, str]] = []
    instructions = closure.instructions_by_stage.get(stage, [])
    for index, instruction in enumerate(instructions):
        if not instruction.upper().startswith("RUN "):
            continue
        command, network_none, unsupported_run_option = _run_command(instruction[4:])
        if not network_none or unsupported_run_option:
            continue
        segments, operators = _shell_chain(command)
        if operators not in ([], ["&&"]):
            continue
        expected_prefix = [["python", "-m", "pip", "check"]]
        if len(segments) == 1:
            assertion = segments[0]
        elif len(segments) == 2 and segments[:1] == expected_prefix:
            assertion = segments[1]
        else:
            continue
        if len(assertion) != 3 or assertion[:2] != ["python", "-c"]:
            continue
        match = re.fullmatch(
            r"import sys; raise SystemExit\(sys\.version_info\[:2\] != "
            r"\(([0-9]+), ([0-9]+)\)\)",
            assertion[2],
        )
        if match is None:
            continue
        proofs.append((index, f"cp{match.group(1)}{match.group(2)}"))

    asserted = ""
    if len(proofs) == 1:
        proof_index, proof_abi = proofs[0]
        trailing = instructions[proof_index + 1 :]
        if all(
            item.partition(" ")[0].upper()
            in {
                "CMD",
                "ENTRYPOINT",
                "EXPOSE",
                "HEALTHCHECK",
                "LABEL",
                "STOPSIGNAL",
                "USER",
                "WORKDIR",
            }
            for item in trailing
        ):
            asserted = proof_abi
    values: set[str] = set()
    current: str | None = stage
    seen: set[str] = set()
    while current is not None and current not in seen:
        seen.add(current)
        image = closure.direct_base_by_stage.get(current)
        if image is not None:
            match = re.search(
                r"python(?:-|:)?([0-9]+)\.([0-9]+)",
                image,
                re.IGNORECASE,
            )
            if match:
                values.add(f"cp{match.group(1)}{match.group(2)}")
        alias_match = re.search(
            r"python(?:-|:)?([0-9]+)\.([0-9]+)",
            current,
            re.IGNORECASE,
        )
        if alias_match:
            alias_abi = f"cp{alias_match.group(1)}{alias_match.group(2)}"
            if alias_abi == asserted:
                values.add(alias_abi)
        current = closure.parent_by_stage.get(current)
    return next(iter(values)) if len(values) == 1 else ""


def _scan_python_installs(
    path: Path,
    context: Path,
    discovery: Discovery,
    *,
    closure: DockerfileClosure,
    selected_targets: set[str],
    required_interpreters_by_target: Mapping[str, set[str]],
) -> None:
    lines = _dockerfile_lines(path, discovery)
    copies = _copy_map_by_stage(lines, context, closure.stage_by_line)
    relative = _relative(discovery.root, path)
    ancestry_by_target = {
        target: _from_ancestry(closure, target) for target in selected_targets
    }
    reachable_by_target = {
        target: _reachable_stages(closure, target) for target in selected_targets
    }
    reachable = set().union(*reachable_by_target.values())
    valid_stages_by_interpreter: dict[str, set[str]] = {}
    for logical in lines:
        stage = closure.stage_by_line.get(logical.number)
        if stage is None or stage not in reachable:
            continue
        stripped = logical.text.strip()
        if not stripped.upper().startswith("RUN "):
            continue
        command, _, _ = _run_command(stripped[4:])
        invocations = _pip_invocations(command)
        textual_installs = len(
            re.findall(r"\bpip(?:3(?:\.[0-9]+)?)?\s+install\b", command)
        )
        if not invocations and not textual_installs:
            continue
        if textual_installs != len(invocations):
            discovery.issue(
                "pip_install_without_hash_lock",
                f"{relative}:{logical.number}:unparsed",
                "nested or wrapped pip install is not a statically isolated invocation",
            )
        for invocation_index, invocation in enumerate(invocations, 1):
            interpreter = invocation.interpreter
            requirement_paths = list(invocation.requirement_paths)
            if (
                not invocation.require_hashes
                or len(requirement_paths) != 1
                or not invocation.lock_only
            ):
                discovery.issue(
                    "pip_install_without_hash_lock",
                    f"{relative}:{logical.number}:{interpreter}:{invocation_index}",
                    "each production pip install must contain only --require-hashes, --only-binary=:all:, and exactly one copied lock",
                )
            closure_valid = False
            closure_hash = ""
            repository_lock = ""
            if len(requirement_paths) == 1:
                container_path = requirement_paths[0]
                lock_path = _copied_repository_path(
                    container_path, stage, copies, closure
                )
                if lock_path is None and not container_path.startswith("/"):
                    lock_path = (context / container_path).resolve()
                if lock_path is None or not lock_path.is_file():
                    discovery.issue(
                        "python_interpreter_lock_missing",
                        f"{relative}:{logical.number}:{container_path}",
                        "pip requirement lock is not a copied repository file",
                    )
                else:
                    closure_valid, closure_hash = _scan_requirement_closure(
                        lock_path, discovery, seen=set()
                    )
                    repository_lock = _relative(discovery.root, lock_path)
            abi = _python_abi_for_stage(closure, stage)
            if not abi:
                discovery.issue(
                    "python_interpreter_lock_missing",
                    f"{relative}:{logical.number}:{interpreter}",
                    "Python ABI cannot be derived from an immutable base or a matching offline assertion",
                )
            if (
                invocation.require_hashes
                and len(requirement_paths) == 1
                and invocation.lock_only
                and closure_valid
                and abi
            ):
                record = {
                    "interpreter": interpreter,
                    "path": repository_lock,
                    "python_abi": abi,
                    "require_hashes": True,
                    "sha256": closure_hash,
                }
                discovery.records["python_locks"].append(record)
                valid_stages_by_interpreter.setdefault(interpreter, set()).add(stage)
    for target, required_interpreters in sorted(
        required_interpreters_by_target.items()
    ):
        target_reachable = ancestry_by_target.get(target, set())
        valid_interpreters = {
            interpreter
            for interpreter, stages in valid_stages_by_interpreter.items()
            if stages & target_reachable
        }
        for interpreter in sorted(required_interpreters - valid_interpreters):
            discovery.issue(
                "python_interpreter_lock_missing",
                f"{relative}:{target}:{interpreter}",
                "selected target lineage has no complete --require-hashes lock installation",
            )


def _pip_invocations(command: str) -> list[PipInvocation]:
    return [
        invocation
        for segment in _shell_segments(command)
        if (invocation := _pip_invocation_from_segment(segment)) is not None
    ]


def _pip_invocation_from_segment(segment: Sequence[str]) -> PipInvocation | None:
    executable_index = _segment_executable_index(segment)
    if executable_index is None:
        return None
    executable = Path(segment[executable_index]).name
    install_index: int | None = None
    if re.fullmatch(r"pip(?:3(?:\.[0-9]+)?)?", executable):
        if (
            executable_index + 1 < len(segment)
            and segment[executable_index + 1] == "install"
        ):
            install_index = executable_index + 1
    elif re.fullmatch(r"python(?:3(?:\.[0-9]+)?)?", executable):
        remainder = list(segment[executable_index + 1 :])
        try:
            module_index = remainder.index("-m")
        except ValueError:
            module_index = -1
        if (
            module_index >= 0
            and all(
                flag in {"-E", "-I", "-S", "-s"} for flag in remainder[:module_index]
            )
            and remainder[module_index : module_index + 3] == ["-m", "pip", "install"]
        ):
            install_index = executable_index + module_index + 3
    if install_index is None:
        return None
    interpreter = (
        "legacy-scraper"
        if "/opt/legacy-scraper-venv/" in segment[executable_index]
        else "airflow"
    )
    paths: list[str] = []
    require_hashes = False
    only_binary = False
    user_install = False
    lock_only = True
    arguments = segment[install_index + 1 :]
    index = 0
    allowed_flags = {
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--no-compile",
        "--no-deps",
        "--require-hashes",
    }
    while index < len(arguments):
        token = arguments[index]
        if token in {"-r", "--requirement"} and index + 1 < len(arguments):
            paths.append(arguments[index + 1])
            index += 2
            continue
        if token.startswith("--requirement="):
            paths.append(token.split("=", 1)[1])
            index += 1
            continue
        if token.startswith("-r") and token != "-r":
            paths.append(token[2:])
            index += 1
            continue
        if token == "--only-binary=:all:":
            only_binary = True
            index += 1
            continue
        if token == "--user":
            user_install = True
            index += 1
            continue
        if token in allowed_flags:
            require_hashes = require_hashes or token == "--require-hashes"
            index += 1
            continue
        lock_only = False
        index += 1
    if user_install and (
        interpreter != "airflow"
        or "PYTHONUSERBASE=/home/airflow/.local" not in segment[:executable_index]
    ):
        lock_only = False
    return PipInvocation(
        interpreter=interpreter,
        requirement_paths=tuple(paths),
        require_hashes=require_hashes,
        lock_only=lock_only and only_binary,
    )


def _requirement_arguments(command: str) -> list[str]:
    return [
        path
        for invocation in _pip_invocations(command)
        for path in invocation.requirement_paths
    ]


def _scan_requirement_closure(
    path: Path,
    discovery: Discovery,
    *,
    seen: set[Path],
    _active: set[Path] | None = None,
    _materials: dict[Path, bytes] | None = None,
) -> tuple[bool, str]:
    path = path.resolve()
    active = _active if _active is not None else set()
    materials = _materials if _materials is not None else {}
    if path in active:
        discovery.issue(
            "python_requirement_unpinned",
            _relative(discovery.root, path),
            "recursive requirement include cycle",
        )
        return False, ""
    if path in seen:
        return True, _requirement_material_digest(discovery.root, materials)
    seen.add(path)
    active.add(path)
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        discovery.issue(
            "python_interpreter_lock_missing",
            str(path),
            "requirement lock is missing or not UTF-8",
        )
        active.discard(path)
        return False, ""
    discovery.material_paths.add(path)
    materials[path] = raw
    valid = True
    for logical in _logical_lines(text):
        value = logical.text.strip()
        if not value or value.startswith("#"):
            continue
        value = value.split(" #", 1)[0].strip()
        include = re.match(r"(?:-r|--requirement(?:=|\s+))\s*(\S+)", value)
        if include:
            child = (path.parent / include.group(1)).resolve()
            child_valid, _ = _scan_requirement_closure(
                child,
                discovery,
                seen=seen,
                _active=active,
                _materials=materials,
            )
            valid = valid and child_valid
            continue
        if value.startswith("-"):
            discovery.issue(
                "python_requirement_unpinned",
                f"{_relative(discovery.root, path)}:{logical.number}",
                "mutable requirement option is not an exact distribution pin",
            )
            valid = False
            continue
        pinned = _REQUIREMENT_PIN.match(value)
        if pinned is None or "*" in pinned.group(1):
            discovery.issue(
                "python_requirement_unpinned",
                f"{_relative(discovery.root, path)}:{logical.number}",
                "requirement must use an exact == version",
            )
            valid = False
        if _REQUIREMENT_HASH.search(value) is None:
            discovery.issue(
                "python_requirement_unhashed",
                f"{_relative(discovery.root, path)}:{logical.number}",
                "requirement has no --hash=sha256 value",
            )
            valid = False
    active.discard(path)
    return valid, _requirement_material_digest(discovery.root, materials)


def _requirement_material_digest(root: Path, materials: Mapping[Path, bytes]) -> str:
    digest = hashlib.sha256()
    ordered = sorted(materials.items(), key=lambda item: _relative(root, item[0]))
    for path, content in ordered:
        relative = _relative(root, path)
        digest.update(relative.encode("utf-8") + b"\0")
        digest.update(len(content).to_bytes(8, "big") + content)
    return digest.hexdigest()


def _scan_workflow(root: Path, discovery: Discovery) -> None:
    path = root / ".github/workflows/whoscored-ci.yml"
    if not path.is_file():
        discovery.issue(
            "github_action_unpinned",
            ".github/workflows/whoscored-ci.yml",
            "production workflow is missing",
        )
        return
    text = path.read_text(encoding="utf-8")
    discovery.material_paths.add(path)
    relative = _relative(root, path)
    workflow_lines = text.splitlines()
    for number, line in enumerate(workflow_lines, 1):
        if re.match(r"^\s*steps:\s*\S", line) or re.search(
            r"[\[{][^#\n]*(?:uses|run)\s*:", line
        ):
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                "inline workflow steps are outside the canonical block parser subset",
            )
        if (
            re.match(r"^\s*-?\s*<<\s*:", line)
            or re.match(r"^\s*-\s*\*[A-Za-z0-9_.-]+\s*$", line)
            or re.match(r"^\s*[A-Za-z0-9_.-]+:\s*[&*][A-Za-z0-9_.-]+", line)
        ):
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                "workflow anchors, aliases, and merge keys are outside the parser subset",
            )
        if re.match(r"^\s*-\s*(?:\{|!|&|\*)", line):
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                "flow, tagged, or aliased workflow step is outside the parser subset",
            )
        if re.match(r'^\s*-?\s*(?:["\']uses["\']|uses\s+):', line):
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                "noncanonical YAML uses key is outside the workflow parser subset",
            )
        if re.match(r'^\s*-?\s*(?:["\']run["\']|run\s+):', line):
            discovery.issue(
                "ci_floating_install",
                f"{relative}:{number}",
                "noncanonical YAML run key is outside the workflow parser subset",
            )
        match = re.match(r"\s*-?\s*uses:\s*([^\s#]+)", line)
        if not match:
            continue
        reference = _clean_yaml_scalar(match.group(1))
        if reference.startswith("./"):
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                "local action/workflow references are outside the recursive closure model",
            )
            continue
        action, separator, commit = reference.rpartition("@")
        if not separator or _COMMIT.fullmatch(commit) is None:
            discovery.issue(
                "github_action_unpinned",
                f"{relative}:{number}",
                f"GitHub Action is not pinned to a 40-hex commit: {reference}",
            )
        else:
            discovery.records["github_actions"].append(
                {"commit": commit, "uses": action, "workflow": relative}
            )
    for line_number, command in _workflow_run_commands(
        workflow_lines, relative=relative, discovery=discovery
    ):
        invocations = _pip_invocations(command)
        textual_installs = len(
            re.findall(r"\bpip(?:3(?:\.[0-9]+)?)?\s+install\b", command)
        )
        if not invocations and not textual_installs:
            continue
        requirement_paths = [
            path for invocation in invocations for path in invocation.requirement_paths
        ]
        valid_lock = False
        if len(requirement_paths) == 1:
            lock_path = (root / requirement_paths[0]).resolve()
            try:
                lock_path.relative_to(root.resolve())
            except ValueError:
                lock_path = root / "__invalid_ci_lock__"
            if lock_path.is_file():
                valid_lock, _ = _scan_requirement_closure(
                    lock_path, discovery, seen=set()
                )
        if (
            len(invocations) != 1
            or textual_installs != 1
            or not invocations[0].require_hashes
            or not invocations[0].lock_only
            or len(requirement_paths) != 1
            or not valid_lock
        ):
            discovery.issue(
                "ci_floating_install",
                f"{relative}:{line_number}",
                "CI pip installation is not a single --require-hashes, wheel-only lock invocation",
            )


def _workflow_run_commands(
    lines: Sequence[str], *, relative: str, discovery: Discovery
) -> list[tuple[int, str]]:
    commands: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = re.match(r"^(?P<indent>\s*)-?\s*run:\s*(?P<value>.*?)\s*$", line)
        if match is None:
            index += 1
            continue
        value = match.group("value")
        line_number = index + 1
        if (
            not value
            or value.startswith(">")
            or (value != "|" and value.startswith("|"))
        ):
            discovery.issue(
                "ci_floating_install",
                f"{relative}:{line_number}",
                "folded or modified workflow block scalar is outside the parser subset",
            )
            index += 1
            continue
        if value == "|":
            base_indent = len(match.group("indent"))
            end = index + 1
            block: list[str] = []
            while end < len(lines):
                nested = lines[end]
                if (
                    nested.strip()
                    and len(nested) - len(nested.lstrip(" ")) <= base_indent
                ):
                    break
                block.append(nested[base_indent + 2 :] if nested else "")
                end += 1
            for logical in _logical_lines("\n".join(block)):
                if logical.text.strip():
                    commands.append(
                        (line_number + logical.number, logical.text.strip())
                    )
            index = end
            continue
        if value.startswith(("*", "&", "!", "[", "{", '"', "'")):
            discovery.issue(
                "ci_floating_install",
                f"{relative}:{line_number}",
                "aliased or structured workflow run input is outside the parser subset",
            )
        elif value:
            commands.append((line_number, value))
        index += 1
    return commands


def _context_entries(
    context: Path, dockerfile: Path, discovery: Discovery
) -> list[Path]:
    if not context.is_dir():
        raise ProvenanceError(f"build context is missing: {context}")
    relative_dockerfile = Path(_lexical_relative(discovery.root, dockerfile))
    root_ignore = context / ".dockerignore"
    specific_ignore = dockerfile.with_name(f"{dockerfile.name}.dockerignore")
    if (
        context.resolve() == discovery.root
        and relative_dockerfile == FLARESOLVERR_DOCKERFILE
    ):
        expected = FLARESOLVERR_CONTEXT_RULES.encode("utf-8")
        for ignore in (root_ignore, specific_ignore):
            if (
                _read_stable_regular_file(
                    ignore, label="FlareSolverr context allowlist"
                )
                != expected
            ):
                raise ProvenanceError(
                    f"FlareSolverr Docker context allowlist differs from the canonical policy: {ignore}"
                )
        files = [context / relative for relative in sorted(FLARESOLVERR_CONTEXT_FILES)]
        for path in files:
            metadata = path.lstat() if path.exists() else None
            if metadata is None or not stat.S_ISREG(metadata.st_mode):
                raise ProvenanceError(
                    f"FlareSolverr effective context input is missing or non-regular: {path}"
                )
        directories = {
            parent
            for path in files
            for parent in path.parents
            if parent != context and parent.is_relative_to(context)
        }
        return sorted(
            {*files, *directories},
            key=lambda item: item.relative_to(context).as_posix(),
        )
    if root_ignore.is_file() or specific_ignore.is_file():
        raise ProvenanceError(
            f"unsupported Docker ignore policy for build context: {context}"
        )
    entries: list[Path] = []
    for path in context.rglob("*"):
        repository_relative = _lexical_relative(discovery.root, path)
        if repository_relative in GENERATED_PROVENANCE_OUTPUTS:
            continue
        metadata = path.lstat()
        if not (
            stat.S_ISREG(metadata.st_mode)
            or stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
        ):
            raise ProvenanceError(f"unsupported special file in Docker context: {path}")
        entries.append(path)
    return sorted(entries, key=lambda item: item.relative_to(context).as_posix())


def _context_digest(
    context: Path, dockerfile: Path, discovery: Discovery
) -> tuple[str, set[str]]:
    entries = _context_entries(context, dockerfile, discovery)
    digest = hashlib.sha256()
    context_inputs: set[str] = set()
    for path in entries:
        relative = path.relative_to(context).as_posix()
        if path.is_symlink():
            target = os.readlink(path)
            mode = path.lstat().st_mode & 0o777
            context_inputs.add(relative)
            discovery.material_paths.add(path)
            digest.update(b"L\0" + relative.encode("utf-8") + b"\0")
            digest.update(mode.to_bytes(2, "big"))
            digest.update(target.encode("utf-8", "surrogateescape") + b"\0")
        elif path.is_file():
            raw = _read_stable_regular_file(path, label="Docker context input")
            context_inputs.add(relative)
            discovery.material_paths.add(path)
            mode = path.stat().st_mode & 0o777
            digest.update(b"F\0" + relative.encode("utf-8") + b"\0")
            digest.update(mode.to_bytes(2, "big") + len(raw).to_bytes(8, "big") + raw)
        elif path.is_dir():
            mode = path.stat().st_mode & 0o777
            digest.update(b"D\0" + relative.encode("utf-8") + b"\0")
            digest.update(mode.to_bytes(2, "big"))
    return digest.hexdigest(), context_inputs


def _source_tree_digest(root: Path, paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    unique = sorted(
        {path.absolute() for path in paths},
        key=lambda item: _lexical_relative(root, item),
    )
    for path in unique:
        if _lexical_relative(root, path) in GENERATED_PROVENANCE_OUTPUTS:
            continue
        relative = _lexical_relative(root, path)
        if path.is_symlink():
            raw = os.readlink(path).encode("utf-8", "surrogateescape")
            kind = b"L"
        else:
            raw = path.read_bytes()
            kind = b"F"
        digest.update(kind + b"\0" + relative.encode("utf-8") + b"\0")
        digest.update(len(raw).to_bytes(8, "big") + raw)
    return digest.hexdigest()


def _base_closure_digest(pinned_bases: Sequence[str]) -> str:
    digests = sorted(image.rsplit("@sha256:", 1)[1] for image in pinned_bases)
    if len(digests) == 1:
        return digests[0]
    if not digests:
        return ""
    return hashlib.sha256(canonical_bytes(digests)).hexdigest()


def _payload_target(closure: DockerfileClosure, target: str) -> str:
    final_stage = closure.final_stage if target == "<default>" else target
    if final_stage not in closure.parent_by_stage:
        return ""
    lineage = _reachable_stages(closure, target)
    generated_stages = {
        stage for stage in lineage if closure.generated_outputs_by_stage.get(stage)
    }
    expected_fixed = _FIXED_PAYLOAD_TARGETS.get(final_stage)
    if generated_stages:
        if generated_stages != {final_stage}:
            return ""
        if closure.generated_outputs_by_stage[final_stage] != set(
            GENERATED_PROVENANCE_OUTPUTS
        ):
            return ""
        payload = closure.parent_by_stage.get(final_stage) or ""
    else:
        payload = final_stage if target != "<default>" else "<default>"
    if expected_fixed is not None and payload != expected_fixed:
        return ""
    return payload


def _record_sort_key(record: Mapping[str, Any]) -> bytes:
    return canonical_bytes(record)


def _canonicalize_discovery(discovery: Discovery) -> None:
    for key in RECORD_KEYS:
        identity = _RECORD_IDENTITIES[key]
        records = sorted(
            discovery.records[key],
            key=lambda record: tuple(str(record.get(field, "")) for field in identity),
        )
        unique: list[dict[str, Any]] = []
        seen: set[bytes] = set()
        for record in records:
            encoded = canonical_bytes(record)
            if encoded not in seen:
                unique.append(record)
                seen.add(encoded)
        discovery.records[key] = unique
    issues = sorted(discovery.issues, key=_record_sort_key)
    unique_issues: list[dict[str, str]] = []
    seen_issues: set[bytes] = set()
    for issue in issues:
        encoded = canonical_bytes(issue)
        if encoded not in seen_issues:
            unique_issues.append(issue)
            seen_issues.add(encoded)
    discovery.issues = unique_issues


def discover_repository(
    root: Path,
    *,
    payload_image_ids: Mapping[str, str] | None = None,
    revision: str | None = None,
) -> Discovery:
    """Discover inputs and produce a deterministic closure report."""

    root = root.resolve()
    discovery = Discovery(
        root=root,
        revision=revision or source_revision(root),
        records={key: [] for key in RECORD_KEYS},
    )
    local_builds, build_graphs = _parse_compose(root, discovery)
    builds_by_graph: dict[tuple[str, str], list[BuildConfig]] = {}
    for config in local_builds:
        builds_by_graph.setdefault((config.dockerfile, config.context), []).append(
            config
        )
    closure_by_graph: dict[tuple[str, str], DockerfileClosure] = {}
    context_sha_by_graph: dict[tuple[str, str], str] = {}
    for dockerfile, context in build_graphs:
        key = (_relative(root, dockerfile), _relative(root, context))
        graph_builds = builds_by_graph.get(key, [])
        selected_targets = {config.target for config in graph_builds}
        context_sha, context_inputs = _context_digest(context, dockerfile, discovery)
        context_sha_by_graph[key] = context_sha
        closure_by_graph[key] = _scan_dockerfile(
            dockerfile,
            context,
            context_inputs,
            discovery,
            selected_targets=selected_targets,
        )
        required_interpreters_by_target: dict[str, set[str]] = {}
        if key[0] == "docker/images/airflow/Dockerfile":
            for config in graph_builds:
                if config.service == "airflow-scheduler":
                    required_interpreters_by_target.setdefault(
                        config.target, set()
                    ).update({"airflow", "legacy-scraper"})
                elif config.service == "whoscored_proxy_filter":
                    required_interpreters_by_target.setdefault(
                        config.target, set()
                    ).add("airflow")
            if not required_interpreters_by_target:
                required_interpreters_by_target = {
                    target: {"airflow", "legacy-scraper"} for target in selected_targets
                }
        _scan_python_installs(
            dockerfile,
            context,
            discovery,
            closure=closure_by_graph[key],
            selected_targets=selected_targets,
            required_interpreters_by_target=required_interpreters_by_target,
        )
    _scan_workflow(root, discovery)

    supplied = dict(payload_image_ids or {})
    discovered_services = {item.service for item in local_builds}
    extras = sorted(set(supplied) - discovered_services)
    if extras:
        raise ProvenanceError(
            f"payload image evidence contains extra services: {', '.join(extras)}"
        )
    for config in sorted(local_builds, key=lambda item: item.service):
        graph_key = (config.dockerfile, config.context)
        context_sha = context_sha_by_graph.get(graph_key, "")
        dockerfile_closure = closure_by_graph.get(graph_key)
        if dockerfile_closure is not None:
            _validate_protected_stage_recipe(config, dockerfile_closure)
        target_bases = (
            dockerfile_closure.bases_by_target.get(config.target, [])
            if dockerfile_closure is not None
            else []
        )
        base_sha = _base_closure_digest(target_bases)
        payload_target = (
            _payload_target(dockerfile_closure, config.target)
            if dockerfile_closure is not None
            else ""
        )
        payload_id = supplied.get(config.service, "")
        if (
            not base_sha
            or not payload_target
            or _SHA256_ID.fullmatch(payload_id) is None
        ):
            missing = []
            if not base_sha:
                missing.append("immutable base closure")
            if not payload_target:
                missing.append("self-reference-free payload target")
            if _SHA256_ID.fullmatch(payload_id) is None:
                missing.append("payload-stage image ID")
            discovery.issue(
                "local_image_provenance_absent",
                f"compose.yaml:{config.service}",
                f"local build lacks {', '.join(missing)}",
            )
        discovery.records["local_images"].append(
            {
                "base_image_sha256": base_sha,
                "context": config.context,
                "context_sha256": context_sha,
                "dockerfile": config.dockerfile,
                "payload_image_id": payload_id,
                "payload_target": payload_target,
                "service": config.service,
                "target": config.target,
            }
        )
    _canonicalize_discovery(discovery)
    tree_sha = _source_tree_digest(root, discovery.material_paths)
    status = "blocked-v1" if discovery.issues else "ready-v1"
    discovery.report = {
        "inputs": {key: discovery.records[key] for key in RECORD_KEYS},
        "issues": discovery.issues,
        "schema_version": SCHEMA_VERSION,
        "source_revision": discovery.revision,
        "source_tree_sha256": tree_sha,
        "status": status,
    }
    return discovery


def _validate_attestation(
    attestation: Mapping[str, Any], *, expected_status: str
) -> str:
    expected = {"provenance_manifest_sha256", "schema_version", "status"}
    digest = attestation.get("provenance_manifest_sha256")
    if (
        set(attestation) != expected
        or attestation.get("schema_version") != SCHEMA_VERSION
        or attestation.get("status") != expected_status
        or not isinstance(digest, str)
    ):
        raise ProvenanceError(
            f"attestation is not a canonical {expected_status} decision"
        )
    if expected_status == "blocked-v1" and digest:
        raise ProvenanceError("blocked-v1 attestation must not name a manifest")
    if expected_status == "ready-v1" and _DIGEST.fullmatch(digest) is None:
        raise ProvenanceError("ready-v1 attestation has no manifest SHA-256")
    return digest


def _manifest_payload_ids(manifest: Mapping[str, Any]) -> dict[str, str]:
    records = manifest.get("local_images")
    if not isinstance(records, list) or not records:
        raise ProvenanceError("manifest local_images must be a non-empty list")
    result: dict[str, str] = {}
    expected_fields = {
        "base_image_sha256",
        "context",
        "context_sha256",
        "dockerfile",
        "payload_image_id",
        "payload_target",
        "service",
        "target",
    }
    for record in records:
        if not isinstance(record, dict) or set(record) != expected_fields:
            raise ProvenanceError("manifest local_images record schema is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        if not isinstance(service, str) or not service or service in result:
            raise ProvenanceError("manifest local_images contains duplicate services")
        if not isinstance(payload, str) or _SHA256_ID.fullmatch(payload) is None:
            raise ProvenanceError("manifest payload_image_id is invalid")
        result[service] = payload
    if list(result) != sorted(result):
        raise ProvenanceError("manifest local_images records are not sorted")
    return result


def _validate_ready_manifest(
    manifest: Mapping[str, Any], raw: bytes, discovery: Discovery, expected_digest: str
) -> None:
    expected_fields = {
        "apt_packages",
        "apt_snapshots",
        "base_images",
        "closure_report_sha256",
        "compose_images",
        "downloaded_artifacts",
        "generated_at",
        "github_actions",
        "local_images",
        "python_locks",
        "schema_version",
        "source_revision",
        "source_tree_sha256",
    }
    if (
        len(raw) > MAX_EVIDENCE_BYTES
        or len(raw) <= 0
        or set(manifest) != expected_fields
        or manifest.get("schema_version") != SCHEMA_VERSION
    ):
        raise ProvenanceError("manifest schema is invalid")
    if _UTC_TIMESTAMP.fullmatch(str(manifest.get("generated_at"))) is None:
        raise ProvenanceError("manifest generated_at is not a canonical UTC timestamp")
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_digest):
        raise ProvenanceError("attested manifest SHA-256 differs from manifest bytes")
    if discovery.issues:
        categories = ", ".join(sorted({item["category"] for item in discovery.issues}))
        raise ProvenanceError(f"mutable build inputs remain: {categories}")
    if manifest.get("source_revision") != discovery.revision:
        raise ProvenanceError("manifest source_revision differs from repository")
    if manifest.get("source_tree_sha256") != discovery.report["source_tree_sha256"]:
        raise ProvenanceError(
            "manifest source_tree_sha256 differs from discovered closure"
        )
    report_sha = hashlib.sha256(canonical_bytes(discovery.report)).hexdigest()
    if manifest.get("closure_report_sha256") != report_sha:
        raise ProvenanceError(
            "manifest closure_report_sha256 differs from canonical report"
        )
    for key in RECORD_KEYS:
        if not discovery.records[key]:
            raise ProvenanceError(f"ready build closure has no {key} records")
        if manifest.get(key) != discovery.records[key]:
            raise ProvenanceError(
                f"manifest {key} has duplicate, extra, missing, or changed records"
            )


def generate_ready_evidence(
    root: Path,
    *,
    payload_image_ids: Mapping[str, str],
    generated_at: str,
) -> dict[str, Any]:
    """Publish the canonical ready pair for one clean payload revision."""

    root = root.resolve()
    if _UTC_TIMESTAMP.fullmatch(generated_at) is None:
        raise ProvenanceError("generated_at is not a canonical UTC timestamp")
    _validate_default_index_flags(root)
    normalized_payloads: dict[str, str] = {}
    for service, image_id in payload_image_ids.items():
        if not isinstance(service, str) or not service or service.strip() != service:
            raise ProvenanceError("payload image service is empty or noncanonical")
        if not isinstance(image_id, str) or _SHA256_ID.fullmatch(image_id) is None:
            raise ProvenanceError(f"payload image ID is invalid: {service}")
        normalized_payloads[service] = image_id
    if not normalized_payloads:
        raise ProvenanceError("at least one payload image ID is required")

    attestation_path = root / ATTESTATION_RELATIVE
    manifest_path = root / MANIFEST_RELATIVE
    for path, label in (
        (attestation_path, "build attestation"),
        (manifest_path, "build manifest"),
    ):
        if not _git_path_is_tracked(root, path):
            raise ProvenanceError(f"{label} is not tracked: {path}")

    changed = _git_changed_paths(root)
    unexpected = sorted(changed - GENERATED_PROVENANCE_OUTPUTS)
    if unexpected:
        raise ProvenanceError(
            "ready evidence generation requires a clean payload tree; changed: "
            + ", ".join(unexpected)
        )

    current_attestation, _ = _load_canonical_object(
        attestation_path, label="build attestation"
    )
    current_manifest, _ = _load_canonical_object(manifest_path, label="build manifest")
    blocked_attestation = {
        "provenance_manifest_sha256": "",
        "schema_version": SCHEMA_VERSION,
        "status": "blocked-v1",
    }
    blocked_manifest = {"schema_version": SCHEMA_VERSION, "status": "blocked-v1"}
    ready_manifest_fields = {
        "closure_report_sha256",
        "generated_at",
        *RECORD_KEYS,
        "schema_version",
        "source_revision",
        "source_tree_sha256",
    }
    # A crash may publish the manifest while the attestation remains blocked.
    # Reuse that create-once timestamp so a normal rerun with a fresh shell
    # clock can finish the exact interrupted decision instead of deadlocking.
    current_generated_at = current_manifest.get("generated_at")
    if (
        set(current_manifest) == ready_manifest_fields
        and current_manifest.get("schema_version") == SCHEMA_VERSION
        and isinstance(current_generated_at, str)
        and _UTC_TIMESTAMP.fullmatch(current_generated_at) is not None
    ):
        generated_at = current_generated_at

    revision = source_revision(root)
    _validate_payload_blocked_evidence(root, revision)
    discovery = discover_repository(
        root,
        payload_image_ids=normalized_payloads,
        revision=revision,
    )
    _validate_material_revision(root, revision, discovery.material_paths)
    if discovery.issues:
        categories = ", ".join(sorted({item["category"] for item in discovery.issues}))
        raise ProvenanceError(
            f"cannot generate ready evidence; mutable build inputs remain: {categories}"
        )
    for key in RECORD_KEYS:
        if not discovery.records[key]:
            raise ProvenanceError(f"ready build closure has no {key} records")

    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source_revision": revision,
        "source_tree_sha256": discovery.report["source_tree_sha256"],
        "closure_report_sha256": hashlib.sha256(
            canonical_bytes(discovery.report)
        ).hexdigest(),
        **{key: discovery.records[key] for key in RECORD_KEYS},
    }
    manifest_raw = canonical_bytes(manifest)
    if len(manifest_raw) > MAX_EVIDENCE_BYTES:
        raise ProvenanceError("generated ready manifest exceeds the runtime gate limit")
    manifest_digest = hashlib.sha256(manifest_raw).hexdigest()
    attestation = {
        "provenance_manifest_sha256": manifest_digest,
        "schema_version": SCHEMA_VERSION,
        "status": "ready-v1",
    }
    attestation_raw = canonical_bytes(attestation)

    allowed_states = (
        current_attestation == blocked_attestation
        and current_manifest in (blocked_manifest, manifest)
    ) or (current_attestation == attestation and current_manifest == manifest)
    if not allowed_states:
        raise ProvenanceError(
            "existing provenance evidence is neither canonical blocked state nor "
            "this exact ready release"
        )

    if source_revision(root) != revision:
        raise ProvenanceError("payload revision changed during evidence generation")
    changed_after_discovery = _git_changed_paths(root)
    unexpected_after_discovery = sorted(
        changed_after_discovery - GENERATED_PROVENANCE_OUTPUTS
    )
    if unexpected_after_discovery:
        raise ProvenanceError(
            "payload tree changed during evidence generation: "
            + ", ".join(unexpected_after_discovery)
        )

    if current_manifest != manifest:
        _replace_regular_file(manifest_path, manifest_raw, label="build manifest")
    if current_attestation != attestation:
        _replace_regular_file(
            attestation_path, attestation_raw, label="build attestation"
        )

    published_manifest, published_raw = _load_canonical_object(
        manifest_path, label="published build manifest"
    )
    published_attestation, _ = _load_canonical_object(
        attestation_path, label="published build attestation"
    )
    if published_manifest != manifest or published_attestation != attestation:
        raise ProvenanceError("published ready evidence differs from generated bytes")
    if not hmac.compare_digest(
        hashlib.sha256(published_raw).hexdigest(), manifest_digest
    ):
        raise ProvenanceError("published ready manifest digest differs")

    return {
        "payload_image_count": len(normalized_payloads),
        "provenance_manifest_sha256": manifest_digest,
        "schema_version": SCHEMA_VERSION,
        "source_revision": revision,
        "status": "ready-generated-v1",
    }


def _validate_deployment_attestation(
    path: Path,
    *,
    manifest_digest: str,
    local_images: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], bytes, tuple[int, ...], dict[str, str]]:
    deployment, raw, identity = _load_canonical_object_snapshot(
        path, label="deployment attestation", protected=True
    )
    expected_fields = {
        "images",
        "provenance_manifest_sha256",
        "schema_version",
        "status",
    }
    if (
        set(deployment) != expected_fields
        or deployment.get("schema_version") != SCHEMA_VERSION
        or deployment.get("status") != "ready-v1"
        or deployment.get("provenance_manifest_sha256") != manifest_digest
    ):
        raise ProvenanceError("deployment attestation identity is invalid")
    images = deployment.get("images")
    if not isinstance(images, list):
        raise ProvenanceError("deployment attestation images must be a list")
    expected = {
        str(item["service"]): str(item["payload_image_id"]) for item in local_images
    }
    observed: dict[str, str] = {}
    final_images: dict[str, str] = {}
    prior = ""
    for record in images:
        if not isinstance(record, dict) or set(record) != {
            "final_image",
            "payload_image_id",
            "service",
        }:
            raise ProvenanceError("deployment image record schema is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        final_image = record.get("final_image")
        if (
            not isinstance(service, str)
            or not service
            or service <= prior
            or service in observed
            or not isinstance(payload, str)
            or _SHA256_ID.fullmatch(payload) is None
            or not isinstance(final_image, str)
            or _PINNED_IMAGE.fullmatch(final_image) is None
        ):
            raise ProvenanceError(
                "deployment image records are not canonical and immutable"
            )
        prior = service
        observed[service] = payload
        final_images[service] = final_image
    if observed != expected:
        raise ProvenanceError(
            "deployment images have duplicate, extra, missing, or changed payload bindings"
        )
    return deployment, raw, identity, final_images


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def validate_ready_build_evidence(
    root: Path,
    *,
    attestation_path: Path,
    manifest_path: Path,
    release_revision: str | None = None,
) -> Discovery:
    """Validate repository-bound ready evidence without authorizing deployment."""

    root = root.resolve()
    canonical_attestation = root / ATTESTATION_RELATIVE
    canonical_manifest = root / MANIFEST_RELATIVE
    if (
        Path(os.path.abspath(attestation_path)) != canonical_attestation
        or Path(os.path.abspath(manifest_path)) != canonical_manifest
    ):
        raise ProvenanceError(
            "ready build evidence must use the canonical repository paths"
        )
    _validate_default_index_flags(root)
    _require_tracked_evidence(
        root, attestation_path, label="build attestation", require_checked=True
    )
    attestation, attestation_raw, attestation_identity = (
        _load_canonical_object_snapshot(attestation_path, label="build attestation")
    )
    manifest_digest = _validate_attestation(attestation, expected_status="ready-v1")
    _require_tracked_evidence(
        root, manifest_path, label="build manifest", require_checked=True
    )
    manifest, manifest_raw, manifest_identity = _load_canonical_object_snapshot(
        manifest_path, label="build manifest"
    )
    checkout_revision = source_revision(root)
    evidence_revision = release_revision or checkout_revision
    if not hmac.compare_digest(
        attestation_raw,
        _git_blob(root, evidence_revision, canonical_attestation),
    ) or not hmac.compare_digest(
        manifest_raw,
        _git_blob(root, evidence_revision, canonical_manifest),
    ):
        raise ProvenanceError(
            "ready evidence bytes differ from the selected release commit"
        )
    payload_revision = manifest.get("source_revision")
    if not isinstance(payload_revision, str):
        raise ProvenanceError("manifest source_revision is missing")
    _validate_promotion_revision(
        root, payload_revision, release_revision=evidence_revision
    )
    payload_ids = _manifest_payload_ids(manifest)
    discovery = discover_repository(
        root, payload_image_ids=payload_ids, revision=payload_revision
    )
    _validate_material_revision(root, payload_revision, discovery.material_paths)
    _validate_ready_manifest(manifest, manifest_raw, discovery, manifest_digest)
    source_tree_digest = manifest.get("source_tree_sha256")
    if not isinstance(source_tree_digest, str):
        raise ProvenanceError("manifest source_tree_sha256 is missing")
    discovery.build_attestation_raw = attestation_raw
    discovery.build_attestation_identity = attestation_identity
    discovery.build_manifest_raw = manifest_raw
    discovery.build_manifest_identity = manifest_identity
    discovery.validated_release_revision = evidence_revision
    discovery.validated_payload_revision = payload_revision
    discovery.validated_manifest_sha256 = manifest_digest
    discovery.validated_source_tree_sha256 = source_tree_digest
    discovery.validated_payload_image_ids = MappingProxyType(dict(payload_ids))
    if source_revision(root) != checkout_revision:
        raise ProvenanceError("checkout revision changed during ready validation")
    return discovery


def validate(
    root: Path,
    *,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path | None,
    expect_blocked: bool,
) -> Discovery:
    root = root.resolve()
    if expect_blocked:
        _require_tracked_evidence(
            root,
            attestation_path,
            label="build attestation",
            require_checked=False,
        )
        attestation, _ = _load_canonical_object(
            attestation_path, label="build attestation"
        )
        _validate_attestation(attestation, expected_status="blocked-v1")
        discovery = discover_repository(root)
        if not discovery.issues:
            raise ProvenanceError(
                "blocked-v1 attestation has no unresolved build inputs"
            )
        return discovery

    discovery = validate_ready_build_evidence(
        root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
    )
    if deployment_attestation_path is None:
        raise ProvenanceError(
            "external deployment attestation is required for final image promotion"
        )
    manifest_digest = discovery.validated_manifest_sha256
    if (
        not isinstance(manifest_digest, str)
        or _DIGEST.fullmatch(manifest_digest) is None
    ):
        raise ProvenanceError("ready validation did not preserve the manifest digest")
    deployment, deployment_raw, deployment_identity, final_images = (
        _validate_deployment_attestation(
            deployment_attestation_path,
            manifest_digest=manifest_digest,
            local_images=discovery.records["local_images"],
        )
    )
    discovery.deployment_attestation = _freeze_json(deployment)
    discovery.deployment_attestation_raw = deployment_raw
    discovery.deployment_attestation_identity = deployment_identity
    discovery.deployment_final_images = MappingProxyType(dict(final_images))
    return discovery


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--attestation", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--deployment-attestation", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--expect-blocked",
        action="store_true",
        help="accept only an explicit blocked-v1 NO-GO decision",
    )
    mode.add_argument(
        "--expect-ready-build",
        action="store_true",
        help="verify ready repository evidence without authorizing deployment",
    )
    mode.add_argument(
        "--generate-ready",
        action="store_true",
        help="atomically prepare canonical ready evidence from explicit payload IDs",
    )
    parser.add_argument(
        "--payload-image-id",
        action="append",
        default=[],
        metavar="SERVICE=sha256:HEX",
        help="one exact payload-stage image binding (generate mode only)",
    )
    parser.add_argument(
        "--generated-at",
        help="canonical UTC manifest timestamp (generate mode only)",
    )
    parser.add_argument(
        "--release-revision",
        help="exact ready PR-head commit for CI merge checkout validation",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="also write the canonical report to this explicitly selected path",
    )
    return parser


def _payload_image_mapping(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        service, separator, image_id = value.partition("=")
        if (
            not separator
            or not service
            or service.strip() != service
            or service in result
            or _SHA256_ID.fullmatch(image_id) is None
        ):
            raise ProvenanceError(
                f"payload image binding is duplicated or invalid: {value}"
            )
        result[service] = image_id
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.root.resolve()
    attestation = (args.attestation or root / ATTESTATION_RELATIVE).resolve()
    manifest = (args.manifest or root / MANIFEST_RELATIVE).resolve()
    try:
        if args.generate_ready:
            if (
                args.attestation is not None
                or args.manifest is not None
                or args.deployment_attestation is not None
                or args.report is not None
                or args.generated_at is None
                or args.release_revision is not None
            ):
                raise ProvenanceError(
                    "generate-ready requires canonical output paths, generated-at, "
                    "and no validation/deployment output overrides"
                )
            payload_image_ids = _payload_image_mapping(args.payload_image_id)
            if (
                not sys.flags.isolated
                or not sys.flags.no_site
                or not sys.flags.ignore_environment
            ):
                raise ProvenanceError(
                    "generate-ready requires an isolated Python -I -S interpreter"
                )
            output = generate_ready_evidence(
                root,
                payload_image_ids=payload_image_ids,
                generated_at=args.generated_at,
            )
        else:
            if args.payload_image_id or args.generated_at is not None:
                raise ProvenanceError(
                    "payload image IDs and generated-at are accepted only by generate-ready"
                )
            if args.expect_ready_build:
                if (
                    args.attestation is not None
                    or args.manifest is not None
                    or args.deployment_attestation is not None
                ):
                    raise ProvenanceError(
                        "expect-ready-build accepts only canonical repository evidence"
                    )
                discovery = validate_ready_build_evidence(
                    root,
                    attestation_path=attestation,
                    manifest_path=manifest,
                    release_revision=args.release_revision,
                )
            else:
                if args.release_revision is not None:
                    raise ProvenanceError(
                        "release-revision is accepted only by expect-ready-build"
                    )
                discovery = validate(
                    root,
                    attestation_path=attestation,
                    manifest_path=manifest,
                    deployment_attestation_path=(
                        args.deployment_attestation.resolve()
                        if args.deployment_attestation is not None
                        else None
                    ),
                    expect_blocked=args.expect_blocked,
                )
            output = discovery.report
    except ProvenanceError as exc:
        print(f"WhoScored build provenance blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    raw_report = canonical_bytes(output)
    try:
        sys.stdout.buffer.write(raw_report)
        if args.report is not None:
            args.report.write_bytes(raw_report)
    except OSError as exc:
        print(
            f"WhoScored build provenance blocked: cannot write report: {exc}",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
