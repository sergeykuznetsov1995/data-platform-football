#!/usr/bin/env python3
"""Generate the external WhoScored final-image deployment attestation.

The generator accepts exactly six immutable registry references and six
protected Buildx maximum-provenance files.  It reuses the repository validator,
binds the exact clean source revision, target, Dockerfile, gate inputs and image
config without creating or starting containers, and expands those six images
to every one of the fourteen locally built Compose services.

The output is canonical JSON, is created once with mode 0600, and is never
overwritten.  Run this script only through an isolated system Python
interpreter (``/usr/bin/python3 -I -S``) from a protected release checkout.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence


EXIT_CONFIG = 78
SCHEMA_VERSION = 1
MAX_INSPECT_BYTES = 1024 * 1024
MAX_BUILD_METADATA_BYTES = 64 * 1024 * 1024
MAX_GATE_FILE_BYTES = 4 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 1_000_000
DOCKER_CLI = Path("/usr/bin/docker")
DOCKER_SOCKET = Path("/run/docker.sock")
GIT_CLI = Path("/usr/bin/git")
DOCKER_INSPECT_FORMAT = (
    "{{json .Id}}\t{{json .RepoDigests}}\t{{json .RootFS}}\t{{json .Config}}"
)
BUILD_ATTESTATION_RELATIVE = Path(
    "docker/images/airflow/whoscored-build-provenance-attestation.json"
)
BUILD_MANIFEST_RELATIVE = Path(
    "docker/images/airflow/whoscored-build-provenance-manifest.json"
)

IMAGE_GROUP_SERVICES = MappingProxyType(
    {
        "airflow-base": (
            "airflow-init",
            "airflow-log-init",
            "airflow-webserver",
            "proxy_filter",
        ),
        "airflow-scheduler": (
            "airflow-scheduler",
            "fbref_proxy_filter",
        ),
        "airflow-whoscored-proxy": (
            "whoscored_paid_gateway",
            "whoscored_proxy_filter",
        ),
        "flaresolverr": (
            "flaresolverr",
            "flaresolverr_whoscored_paid",
        ),
        "jupyterhub": ("jupyterhub",),
        "superset": (
            "superset",
            "superset-beat",
            "superset-worker",
        ),
    }
)
DERIVED_FINAL_GROUPS = frozenset(
    {"airflow-scheduler", "airflow-whoscored-proxy"}
)
IMAGE_GROUP_BUILD_SPECS = MappingProxyType(
    {
        "airflow-base": (
            "docker/images/airflow/Dockerfile",
            "Dockerfile",
            "airflow-base",
            "payload",
        ),
        "airflow-scheduler": (
            "docker/images/airflow/Dockerfile",
            "Dockerfile",
            "airflow-scheduler",
            "release",
        ),
        "airflow-whoscored-proxy": (
            "docker/images/airflow/Dockerfile",
            "Dockerfile",
            "airflow-whoscored-proxy",
            "release",
        ),
        "flaresolverr": (
            "docker/images/flaresolverr-whoscored/Dockerfile",
            "Dockerfile",
            "",
            "payload",
        ),
        "jupyterhub": (
            "docker/images/jupyterhub/Dockerfile",
            "Dockerfile",
            "",
            "payload",
        ),
        "superset": (
            "docker/images/superset/Dockerfile",
            "Dockerfile",
            "",
            "payload",
        ),
    }
)
IMAGE_GROUP_CONTEXT_SPECS = MappingProxyType(
    {
        "airflow-base": ("docker/images/airflow", "docker/images/airflow"),
        "airflow-scheduler": ("docker/images/airflow", "docker/images/airflow"),
        "airflow-whoscored-proxy": (
            "docker/images/airflow",
            "docker/images/airflow",
        ),
        "flaresolverr": (".", "docker/images/flaresolverr-whoscored"),
        "jupyterhub": ("docker/images/jupyterhub", "docker/images/jupyterhub"),
        "superset": ("docker/images/superset", "docker/images/superset"),
    }
)
GATE_CONTEXT_INPUTS = frozenset(
    {
        "whoscored-build-provenance-attestation.json",
        "whoscored-build-provenance-manifest.json",
        "whoscored-production-entrypoint",
        "whoscored-production-gate",
        "whoscored-production-python",
        "whoscored-runtime-trust-root-production",
        "whoscored_production_gate.py",
    }
)
GATE_IMAGE_FILES = MappingProxyType(
    {
        "usr/local/bin/whoscored-production-entrypoint": (
            "whoscored-production-entrypoint",
            0o555,
        ),
        "usr/local/bin/whoscored-production-gate": (
            "whoscored-production-gate",
            0o555,
        ),
        "usr/local/bin/whoscored-production-python": (
            "whoscored-production-python",
            0o555,
        ),
        "usr/local/libexec/whoscored_production_gate.py": (
            "whoscored_production_gate.py",
            0o444,
        ),
        "usr/local/share/whoscored/build-provenance-attestation.json": (
            "whoscored-build-provenance-attestation.json",
            0o444,
        ),
        "usr/local/share/whoscored/build-provenance-manifest.json": (
            "whoscored-build-provenance-manifest.json",
            0o444,
        ),
        "usr/local/share/whoscored/runtime-trust-root": (
            "whoscored-runtime-trust-root-production",
            0o444,
        ),
    }
)
INHERITED_GATE_IMAGE_FILE = "usr/local/share/whoscored/runtime-trust-root"
COMMON_FINAL_SUFFIX_FILES = frozenset(
    (set(GATE_IMAGE_FILES) - {INHERITED_GATE_IMAGE_FILE})
    | {
        "usr/local/bin/python",
        "usr/local/bin/python3",
        "usr/local/bin/python3.11",
        "usr/local/libexec/whoscored-python-real",
    }
)
SCHEDULER_FINAL_SUFFIX_FILES = frozenset(
    {
        "opt/legacy-scraper-venv/bin/python",
        "opt/legacy-scraper-venv/bin/python3",
        "opt/legacy-scraper-venv/bin/python3.11",
    }
)
COMMON_FINAL_SUFFIX_REMOVALS = frozenset(
    {
        "usr/local/bin/python",
        "usr/local/bin/python3",
        "usr/local/bin/python3.11",
    }
)
SCHEDULER_FINAL_SUFFIX_REMOVALS = frozenset(
    {
        "opt/legacy-scraper-venv/bin/python",
        "opt/legacy-scraper-venv/bin/python3",
        "opt/legacy-scraper-venv/bin/python3.11",
    }
)
EXPECTED_GATE_CONFIG_CHANGES = MappingProxyType(
    {
        "User": "50000:0",
        "WorkingDir": "/opt/airflow",
        "Entrypoint": [
            "/usr/bin/dumb-init",
            "--",
            "/usr/local/bin/whoscored-production-entrypoint",
            "/entrypoint",
        ],
    }
)
EXPECTED_SERVICES = frozenset(
    service
    for services in IMAGE_GROUP_SERVICES.values()
    for service in services
)

_SHA256 = re.compile(r"\A[0-9a-f]{64}\Z")
_SHA256_ID = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_PINNED_IMAGE = re.compile(r"\A[^\s@]+@sha256:[0-9a-f]{64}\Z")
_CONTROL_ENV_PREFIXES = ("COMPOSE_", "DOCKER_", "GIT_", "LD_", "DYLD_")
_CONTROL_ENV_NAMES = frozenset(
    {
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "LOCPATH",
        "MALLOC_TRACE",
        "PYTHONHOME",
        "PYTHONPATH",
    }
)


class DeploymentAttestationError(RuntimeError):
    """Raised when final-image provenance cannot be established exactly."""


class _DuplicateKey(ValueError):
    pass


@dataclass(frozen=True)
class ImageInspection:
    image_id: str
    repo_digests: tuple[str, ...]
    layers: tuple[str, ...]
    config_raw: bytes


@dataclass(frozen=True)
class BuildProvenanceEvidence:
    group: str
    final_digest: str
    dockerfile_sha256: str
    dockerfile_identity: tuple[int, ...]
    metadata_sha256: str
    metadata_identity: tuple[int, ...]
    gate_inputs: tuple[tuple[str, str, tuple[int, ...]], ...]
    source_revision: str
    target: str


@dataclass(frozen=True)
class ReadyEvidence:
    manifest_sha256: str
    payload_image_ids: tuple[tuple[str, str], ...]
    payload_revision: str
    release_revision: str
    build_attestation_raw: bytes
    build_attestation_identity: tuple[int, ...]
    build_manifest_raw: bytes
    build_manifest_identity: tuple[int, ...]

    @property
    def payloads(self) -> dict[str, str]:
        return dict(self.payload_image_ids)


@dataclass(frozen=True)
class _ArchivedNode:
    kind: str
    raw: bytes
    mode: int
    uid: int
    gid: int
    link_target: str
    has_extended_metadata: bool


@dataclass(frozen=True)
class _LayerChanges:
    removals: tuple[str, ...]
    opaque_directories: tuple[str, ...]
    additions: tuple[tuple[str, _ArchivedNode], ...]
    changed_paths: tuple[str, ...]
    has_extended_metadata: bool


class _DigestingReader:
    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self._digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        raw = self._stream.read(size)
        if raw:
            self._digest.update(raw)
        return raw

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


InspectImage = Callable[[str], ImageInspection]


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey(key)
        value[key] = item
    return value


def _validator_module() -> Any:
    module_name = "_whoscored_deployment_build_validator"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    path = Path(__file__).resolve().with_name(
        "validate_whoscored_build_provenance.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise DeploymentAttestationError("build-provenance validator is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise DeploymentAttestationError(
            "build-provenance validator cannot be loaded"
        ) from exc
    return module


class _PinnedGitSubprocess:
    DEVNULL = subprocess.DEVNULL
    PIPE = subprocess.PIPE

    def __init__(self, git_fd: int) -> None:
        self._git_fd = git_fd

    def run(self, command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess:
        if (
            not isinstance(command, (tuple, list))
            or not command
            or command[0] != "git"
            or any(not isinstance(argument, str) or "\0" in argument for argument in command)
            or set(kwargs) - {"check", "stderr", "stdin", "stdout"}
            or kwargs.get("stdin") != subprocess.DEVNULL
            or kwargs.get("stderr") != subprocess.DEVNULL
            or kwargs.get("stdout") not in {subprocess.DEVNULL, subprocess.PIPE}
            or kwargs.get("check") is not False
        ):
            raise DeploymentAttestationError(
                "build validator attempted an untrusted subprocess"
            )
        if len(command) < 4 or command[1] != "-C":
            raise DeploymentAttestationError(
                "build validator attempted Git outside the release checkout"
            )
        root = command[2]
        if not os.path.isabs(root) or root != os.path.abspath(root):
            raise DeploymentAttestationError(
                "build validator attempted Git outside the release checkout"
            )
        arguments = list(command[3:])
        if arguments[0] == "diff":
            arguments[1:1] = ["--no-ext-diff", "--no-textconv"]
        safe_config = (
            ("core.attributesFile", "/dev/null"),
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
            ("core.worktree", root),
            ("credential.helper", ""),
            ("diff.external", "/bin/false"),
            ("diff.ignoreSubmodules", "none"),
            ("filter.lfs.clean", ""),
            ("filter.lfs.process", ""),
            ("filter.lfs.required", "false"),
            ("filter.lfs.smudge", ""),
            ("submodule.recurse", "false"),
        )
        config_arguments = tuple(
            argument
            for key, value in safe_config
            for argument in ("-c", f"{key}={value}")
        )
        environment = {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_EXTERNAL_DIFF": "/bin/false",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PAGER": "/bin/false",
            "GIT_TERMINAL_PROMPT": "0",
            "HOME": "/nonexistent",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
        }
        return subprocess.run(
            (
                f"/proc/self/fd/{self._git_fd}",
                "--no-pager",
                "--no-optional-locks",
                *config_arguments,
                "-C",
                root,
                *arguments,
            ),
            env=environment,
            pass_fds=(self._git_fd,),
            **kwargs,
        )


def _require_no_git_attributes(root: Path) -> None:
    def walk_error(error: OSError) -> None:
        raise DeploymentAttestationError(
            "release checkout cannot be scanned for Git attributes"
        ) from error

    for current, directories, files in os.walk(
        root, topdown=True, onerror=walk_error, followlinks=False
    ):
        directories[:] = [
            name
            for name in directories
            if name != ".git" and not Path(current, name).is_symlink()
        ]
        if ".gitattributes" in files:
            raise DeploymentAttestationError(
                "Git attributes are unsupported for protected source validation"
            )
    validator = _validator_module()
    try:
        git_dir = validator._resolve_git_dir(root)
        candidates = [git_dir / "info/attributes"]
        common_marker = git_dir / "commondir"
        if common_marker.is_file():
            common = (git_dir / common_marker.read_text(encoding="utf-8").strip()).resolve()
            candidates.append(common / "info/attributes")
        for candidate in candidates:
            if candidate.exists() or candidate.is_symlink():
                raise DeploymentAttestationError(
                    "Git info attributes are unsupported for protected source validation"
                )
    except DeploymentAttestationError:
        raise
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError("Git metadata cannot be validated safely") from exc


def _validated_ready_evidence(root: Path) -> ReadyEvidence:
    validator = _validator_module()
    _require_no_git_attributes(root)
    git_fd = _open_trusted_executable(GIT_CLI, label="Git CLI")
    original_subprocess = validator.subprocess
    validator.subprocess = _PinnedGitSubprocess(git_fd)
    try:
        discovery = validator.validate_ready_build_evidence(
            root,
            attestation_path=root / BUILD_ATTESTATION_RELATIVE,
            manifest_path=root / BUILD_MANIFEST_RELATIVE,
        )
    except validator.ProvenanceError as exc:
        raise DeploymentAttestationError(
            f"ready build evidence is invalid: {exc}"
        ) from exc
    except Exception as exc:
        raise DeploymentAttestationError("ready build validation failed") from exc
    finally:
        validator.subprocess = original_subprocess
        os.close(git_fd)
    payloads = discovery.validated_payload_image_ids
    digest = discovery.validated_manifest_sha256
    payload_revision = discovery.validated_payload_revision
    release_revision = discovery.validated_release_revision
    if (
        not isinstance(digest, str)
        or _SHA256.fullmatch(digest) is None
        or payloads is None
        or not isinstance(payload_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", payload_revision) is None
        or not isinstance(release_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", release_revision) is None
        or discovery.build_attestation_raw is None
        or discovery.build_attestation_identity is None
        or discovery.build_manifest_raw is None
        or discovery.build_manifest_identity is None
    ):
        raise DeploymentAttestationError(
            "ready build validation did not preserve exact evidence bytes"
        )
    _validate_payload_groups(payloads)
    return ReadyEvidence(
        manifest_sha256=digest,
        payload_image_ids=tuple(sorted(dict(payloads).items())),
        payload_revision=payload_revision,
        release_revision=release_revision,
        build_attestation_raw=discovery.build_attestation_raw,
        build_attestation_identity=tuple(discovery.build_attestation_identity),
        build_manifest_raw=discovery.build_manifest_raw,
        build_manifest_identity=tuple(discovery.build_manifest_identity),
    )


def _validate_payload_groups(payloads: Mapping[str, str]) -> dict[str, str]:
    if set(payloads) != EXPECTED_SERVICES:
        missing = sorted(EXPECTED_SERVICES - set(payloads))
        extra = sorted(set(payloads) - EXPECTED_SERVICES)
        detail = []
        if missing:
            detail.append("missing=" + ",".join(missing))
        if extra:
            detail.append("extra=" + ",".join(extra))
        raise DeploymentAttestationError(
            "ready manifest must bind exactly fourteen local services"
            + (": " + " ".join(detail) if detail else "")
        )
    grouped: dict[str, str] = {}
    for group, services in IMAGE_GROUP_SERVICES.items():
        image_ids = {payloads[service] for service in services}
        if len(image_ids) != 1:
            raise DeploymentAttestationError(
                f"payload image IDs differ inside shared group {group}"
            )
        image_id = image_ids.pop()
        if not isinstance(image_id, str) or _SHA256_ID.fullmatch(image_id) is None:
            raise DeploymentAttestationError(
                f"payload image ID is invalid for group {group}"
            )
        grouped[group] = image_id
    if len(set(grouped.values())) != len(IMAGE_GROUP_SERVICES):
        raise DeploymentAttestationError(
            "the six payload image groups must have distinct image IDs"
        )
    return grouped


def _validate_final_images(final_images: Mapping[str, str]) -> dict[str, str]:
    expected = set(IMAGE_GROUP_SERVICES)
    if set(final_images) != expected:
        missing = sorted(expected - set(final_images))
        extra = sorted(set(final_images) - expected)
        detail = []
        if missing:
            detail.append("missing=" + ",".join(missing))
        if extra:
            detail.append("extra=" + ",".join(extra))
        raise DeploymentAttestationError(
            "final image bindings must name exactly six groups"
            + (": " + " ".join(detail) if detail else "")
        )
    normalized: dict[str, str] = {}
    for group, reference in final_images.items():
        if not isinstance(reference, str) or _PINNED_IMAGE.fullmatch(reference) is None:
            raise DeploymentAttestationError(
                f"final image for {group} is not repository@sha256:digest"
            )
        normalized[group] = reference
    if len(set(normalized.values())) != len(normalized):
        raise DeploymentAttestationError("the six final image references must be distinct")
    return normalized


def _build_metadata_mapping(values: Sequence[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for value in values:
        group, separator, raw_path = value.partition("=")
        path = Path(raw_path)
        if (
            not separator
            or not group
            or group.strip() != group
            or group in result
            or not path.is_absolute()
            or path != Path(os.path.abspath(path))
        ):
            raise DeploymentAttestationError(
                f"BuildKit metadata binding is duplicated or invalid: {value}"
            )
        result[group] = path
    if set(result) != set(IMAGE_GROUP_SERVICES):
        raise DeploymentAttestationError(
            "BuildKit metadata bindings must name exactly six groups"
        )
    if len(set(result.values())) != len(result):
        raise DeploymentAttestationError(
            "the six BuildKit metadata paths must be distinct"
        )
    return result


def _load_protected_build_metadata(path: Path) -> tuple[bytes, tuple[int, ...]]:
    validator = _validator_module()
    try:
        raw, identity = validator.read_protected_regular_file_snapshot(
            path, label="BuildKit metadata"
        )
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError(
            f"BuildKit metadata is not protected: {path}"
        ) from exc
    fields = tuple(validator.REGULAR_FILE_IDENTITY_FIELDS)
    if len(fields) != len(identity):
        raise DeploymentAttestationError("BuildKit metadata identity is incomplete")
    metadata = dict(zip(fields, identity, strict=True))
    if (
        not raw
        or len(raw) > MAX_BUILD_METADATA_BYTES
        or not stat.S_ISREG(metadata["st_mode"])
        or stat.S_IMODE(metadata["st_mode"]) != 0o600
        or metadata["st_uid"] != 0
        or metadata["st_gid"] != 0
        or metadata["st_nlink"] != 1
        or metadata["st_size"] != len(raw)
    ):
        raise DeploymentAttestationError(
            f"BuildKit metadata must be root:root mode 0600: {path}"
        )
    return raw, tuple(identity)


def _require_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise DeploymentAttestationError(f"{label} is malformed")
    return value


def _require_nonempty_llb(value: Any, *, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise DeploymentAttestationError(f"{label} is incomplete")
    definitions: list[dict[str, Any]] = []
    ids: set[str] = set()
    for item in value:
        definition = _require_mapping(item, label=label)
        identifier = definition.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in ids:
            raise DeploymentAttestationError(f"{label} has invalid step identities")
        ids.add(identifier)
        definitions.append(definition)
    return definitions


def _require_digest_mapping(
    value: Any, *, definitions: Sequence[Mapping[str, Any]], label: str
) -> None:
    mapping = _require_mapping(value, label=label)
    ids = {item["id"] for item in definitions}
    if (
        not mapping
        or any(_SHA256_ID.fullmatch(key) is None for key in mapping)
        or any(not isinstance(item, str) or item not in ids for item in mapping.values())
    ):
        raise DeploymentAttestationError(f"{label} is incomplete")


def _require_layer_provenance(
    value: Any, *, definitions: Sequence[Mapping[str, Any]], label: str
) -> None:
    layers = _require_mapping(value, label=label)
    ids = {item["id"] for item in definitions}
    descriptor_count = 0
    if not layers:
        raise DeploymentAttestationError(f"{label} is incomplete")
    for key, alternatives in layers.items():
        step, separator, output = key.rpartition(":")
        if (
            not separator
            or step not in ids
            or not output.isdigit()
            or not isinstance(alternatives, list)
            or not alternatives
        ):
            raise DeploymentAttestationError(f"{label} is malformed")
        for chain in alternatives:
            if not isinstance(chain, list) or not chain:
                raise DeploymentAttestationError(f"{label} is malformed")
            for item in chain:
                descriptor = _require_mapping(item, label=label)
                size = descriptor.get("size")
                if (
                    _SHA256_ID.fullmatch(descriptor.get("digest", "")) is None
                    or descriptor.get("mediaType")
                    not in {
                        "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "application/vnd.oci.image.layer.v1.tar+gzip",
                        "application/vnd.oci.image.layer.v1.tar+zstd",
                    }
                    or not isinstance(size, int)
                    or isinstance(size, bool)
                    or size <= 0
                ):
                    raise DeploymentAttestationError(f"{label} is malformed")
                descriptor_count += 1
    if not descriptor_count:
        raise DeploymentAttestationError(f"{label} is incomplete")


def _followpaths(value: Any) -> set[str]:
    observed: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "local.followpaths":
                if not isinstance(item, str):
                    raise DeploymentAttestationError(
                        "BuildKit context followpaths are malformed"
                    )
                try:
                    paths = json.loads(item)
                except json.JSONDecodeError as exc:
                    raise DeploymentAttestationError(
                        "BuildKit context followpaths are malformed"
                    ) from exc
                if not isinstance(paths, list) or any(
                    not isinstance(path, str) for path in paths
                ):
                    raise DeploymentAttestationError(
                        "BuildKit context followpaths are malformed"
                    )
                for path in paths:
                    normalized = path.removeprefix("./")
                    if (
                        not normalized
                        or normalized.startswith("/")
                        or ".." in Path(normalized).parts
                    ):
                        raise DeploymentAttestationError(
                            "BuildKit context followpaths are unsafe"
                        )
                    observed.add(normalized)
            else:
                observed.update(_followpaths(item))
    elif isinstance(value, list):
        for item in value:
            observed.update(_followpaths(item))
    return observed


def _validate_build_provenance_value(
    *,
    group: str,
    final_image: str,
    expected_revision: str,
    raw: bytes,
    identity: tuple[int, ...],
    dockerfile_raw: bytes,
    dockerfile_identity: tuple[int, ...],
    gate_inputs: tuple[tuple[str, str, tuple[int, ...]], ...],
) -> BuildProvenanceEvidence:
    try:
        document = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object
        )
    except (
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
        _DuplicateKey,
    ) as exc:
        raise DeploymentAttestationError(
            f"BuildKit metadata is malformed for group {group}"
        ) from exc
    top = _require_mapping(document, label=f"BuildKit metadata for group {group}")
    _, entrypoint, target, _ = IMAGE_GROUP_BUILD_SPECS[group]
    expected_digest = final_image.rsplit("@", 1)[1]
    descriptor = _require_mapping(
        top.get("containerimage.descriptor"),
        label=f"BuildKit image descriptor for group {group}",
    )
    if (
        top.get("containerimage.digest") != expected_digest
        or descriptor.get("digest") != expected_digest
        or descriptor.get("mediaType")
        not in {
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.oci.image.manifest.v1+json",
        }
        or not isinstance(descriptor.get("size"), int)
        or isinstance(descriptor.get("size"), bool)
        or descriptor["size"] <= 0
    ):
        raise DeploymentAttestationError(
            f"BuildKit metadata does not bind the final digest for group {group}"
        )

    provenance = _require_mapping(
        top.get("buildx.build.provenance"),
        label=f"BuildKit provenance for group {group}",
    )
    if provenance.get("buildType") != "https://mobyproject.org/buildkit@v1":
        raise DeploymentAttestationError(
            f"BuildKit provenance type is invalid for group {group}"
        )
    invocation = _require_mapping(
        provenance.get("invocation"), label=f"BuildKit invocation for group {group}"
    )
    config_source = _require_mapping(
        invocation.get("configSource"),
        label=f"BuildKit config source for group {group}",
    )
    parameters = _require_mapping(
        invocation.get("parameters"),
        label=f"BuildKit parameters for group {group}",
    )
    environment = _require_mapping(
        invocation.get("environment"),
        label=f"BuildKit environment for group {group}",
    )
    expected_args: dict[str, str] = {}
    if target:
        expected_args["target"] = target
    if entrypoint != "Dockerfile":
        expected_args["filename"] = entrypoint
    expected_parameters: dict[str, Any] = {
        "frontend": "dockerfile.v0",
        "locals": [{"name": "context"}, {"name": "dockerfile"}],
    }
    if expected_args:
        expected_parameters["args"] = expected_args
    if (
        config_source != {"entryPoint": entrypoint}
        or parameters != expected_parameters
        or environment != {"platform": "linux/amd64"}
    ):
        raise DeploymentAttestationError(
            f"BuildKit invocation is not the exact production build for group {group}"
        )

    build_config = _require_mapping(
        provenance.get("buildConfig"),
        label=f"BuildKit build config for group {group}",
    )
    definitions = _require_nonempty_llb(
        build_config.get("llbDefinition"),
        label=f"BuildKit LLB for group {group}",
    )
    _require_digest_mapping(
        build_config.get("digestMapping"),
        definitions=definitions,
        label=f"BuildKit LLB digest mapping for group {group}",
    )

    metadata = _require_mapping(
        provenance.get("metadata"), label=f"BuildKit metadata for group {group}"
    )
    completeness = _require_mapping(
        metadata.get("completeness"),
        label=f"BuildKit completeness for group {group}",
    )
    buildkit_metadata = _require_mapping(
        metadata.get("https://mobyproject.org/buildkit@v1#metadata"),
        label=f"BuildKit maximum provenance for group {group}",
    )
    vcs = _require_mapping(
        buildkit_metadata.get("vcs"), label=f"BuildKit VCS for group {group}"
    )
    context_localdir, dockerfile_localdir = IMAGE_GROUP_CONTEXT_SPECS[group]
    if (
        completeness
        != {"environment": True, "materials": False, "parameters": True}
        or metadata.get("reproducible") is not False
        or vcs
        != {
            "localdir:context": context_localdir,
            "localdir:dockerfile": dockerfile_localdir,
            "revision": expected_revision,
        }
    ):
        raise DeploymentAttestationError(
            f"BuildKit provenance is incomplete or dirty for group {group}"
        )
    source = _require_mapping(
        buildkit_metadata.get("source"), label=f"BuildKit source for group {group}"
    )
    infos = source.get("infos")
    if not isinstance(infos, list) or len(infos) != 1:
        raise DeploymentAttestationError(
            f"BuildKit Dockerfile source is incomplete for group {group}"
        )
    info = _require_mapping(infos[0], label=f"BuildKit Dockerfile for group {group}")
    try:
        encoded_dockerfile = info.get("data")
        if not isinstance(encoded_dockerfile, str):
            raise ValueError("missing Dockerfile data")
        attested_dockerfile = base64.b64decode(
            encoded_dockerfile.encode("ascii"), validate=True
        )
    except (UnicodeEncodeError, ValueError) as exc:
        raise DeploymentAttestationError(
            f"BuildKit Dockerfile source is malformed for group {group}"
        ) from exc
    info_definitions = _require_nonempty_llb(
        info.get("llbDefinition"),
        label=f"BuildKit Dockerfile LLB for group {group}",
    )
    _require_digest_mapping(
        info.get("digestMapping"),
        definitions=info_definitions,
        label=f"BuildKit Dockerfile digest mapping for group {group}",
    )
    if (
        info.get("filename") != entrypoint
        or info.get("language") != "Dockerfile"
        or attested_dockerfile != dockerfile_raw
        or not isinstance(source.get("locations"), dict)
        or not source["locations"]
    ):
        raise DeploymentAttestationError(
            f"BuildKit provenance does not bind the exact Dockerfile for group {group}"
        )
    _require_layer_provenance(
        buildkit_metadata.get("layers"),
        definitions=definitions,
        label=f"BuildKit maximum layer provenance for group {group}",
    )
    if group in DERIVED_FINAL_GROUPS:
        observed_inputs = _followpaths([definitions, info_definitions])
        if not GATE_CONTEXT_INPUTS.issubset(observed_inputs):
            raise DeploymentAttestationError(
                f"BuildKit provenance omits production gate inputs for group {group}"
            )
        if (
            {name for name, _, _ in gate_inputs} != GATE_CONTEXT_INPUTS
            or len(gate_inputs) != len(GATE_CONTEXT_INPUTS)
            or any(_SHA256.fullmatch(digest) is None for _, digest, _ in gate_inputs)
            or any(not identity for _, _, identity in gate_inputs)
        ):
            raise DeploymentAttestationError(
                f"protected production gate inputs are incomplete for group {group}"
            )
    elif gate_inputs:
        raise DeploymentAttestationError(
            f"unexpected production gate inputs for group {group}"
        )
    return BuildProvenanceEvidence(
        group=group,
        final_digest=final_image,
        dockerfile_sha256=hashlib.sha256(dockerfile_raw).hexdigest(),
        dockerfile_identity=tuple(dockerfile_identity),
        metadata_sha256=hashlib.sha256(raw).hexdigest(),
        metadata_identity=identity,
        gate_inputs=gate_inputs,
        source_revision=expected_revision,
        target=target,
    )


def _load_all_build_provenance(
    root: Path,
    *,
    metadata_paths: Mapping[str, Path],
    final_images: Mapping[str, str],
    ready_evidence: ReadyEvidence,
) -> dict[str, BuildProvenanceEvidence]:
    if set(metadata_paths) != set(IMAGE_GROUP_SERVICES):
        raise DeploymentAttestationError(
            "BuildKit metadata paths must bind exactly six image groups"
        )
    result: dict[str, BuildProvenanceEvidence] = {}
    for group in sorted(IMAGE_GROUP_SERVICES):
        revision_kind = IMAGE_GROUP_BUILD_SPECS[group][3]
        expected_revision = (
            ready_evidence.payload_revision
            if revision_kind == "payload"
            else ready_evidence.release_revision
        )
        raw, identity = _load_protected_build_metadata(metadata_paths[group])
        dockerfile_relative = IMAGE_GROUP_BUILD_SPECS[group][0]
        validator = _validator_module()
        try:
            dockerfile_raw, dockerfile_identity = (
                validator.read_protected_regular_file_snapshot(
                    root / dockerfile_relative,
                    label=f"Dockerfile for group {group}",
                )
            )
        except (OSError, validator.ProvenanceError) as exc:
            raise DeploymentAttestationError(
                f"Dockerfile is not protected for group {group}"
            ) from exc
        gate_inputs: list[tuple[str, str, tuple[int, ...]]] = []
        if group in DERIVED_FINAL_GROUPS:
            context_path = root / IMAGE_GROUP_CONTEXT_SPECS[group][0]
            for name in sorted(GATE_CONTEXT_INPUTS):
                try:
                    gate_raw, gate_identity = (
                        validator.read_protected_regular_file_snapshot(
                            context_path / name,
                            label=f"production gate input {name}",
                        )
                    )
                except (OSError, validator.ProvenanceError) as exc:
                    raise DeploymentAttestationError(
                        f"production gate input is not protected: {name}"
                    ) from exc
                gate_inputs.append(
                    (
                        name,
                        hashlib.sha256(gate_raw).hexdigest(),
                        tuple(gate_identity),
                    )
                )
        result[group] = _validate_build_provenance_value(
            group=group,
            final_image=final_images[group],
            expected_revision=expected_revision,
            raw=raw,
            identity=identity,
            dockerfile_raw=dockerfile_raw,
            dockerfile_identity=tuple(dockerfile_identity),
            gate_inputs=tuple(gate_inputs),
        )
    return result


def _verify_image_binding(
    *,
    group: str,
    payload_image_id: str,
    final_image: str,
    inspect_image: InspectImage,
) -> ImageInspection:
    payload = inspect_image(payload_image_id)
    final = inspect_image(final_image)
    if payload.image_id != payload_image_id:
        raise DeploymentAttestationError(
            f"Docker returned a changed payload image ID for group {group}"
        )
    if final_image not in final.repo_digests:
        raise DeploymentAttestationError(
            f"final digest is absent from Docker RepoDigests for group {group}"
        )
    if group in DERIVED_FINAL_GROUPS:
        if (
            final.image_id == payload.image_id
            or len(final.layers) <= len(payload.layers)
            or final.layers[: len(payload.layers)] != payload.layers
        ):
            raise DeploymentAttestationError(
                f"final image is not a strict payload-layer descendant for group {group}"
            )
        try:
            payload_config = json.loads(
                payload.config_raw, object_pairs_hook=_unique_object
            )
            final_config = json.loads(final.config_raw, object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError, _DuplicateKey) as exc:
            raise DeploymentAttestationError(
                f"Docker config is malformed for group {group}"
            ) from exc
        if not isinstance(payload_config, dict) or not isinstance(final_config, dict):
            raise DeploymentAttestationError(
                f"Docker config is malformed for group {group}"
            )
        expected_config = dict(payload_config)
        expected_config.update(EXPECTED_GATE_CONFIG_CHANGES)
        if final_config != expected_config:
            raise DeploymentAttestationError(
                f"final image config has unexpected gate-stage changes for group {group}"
            )
    elif final.image_id != payload.image_id or final.layers != payload.layers:
        raise DeploymentAttestationError(
            f"final digest does not resolve to the payload image for group {group}"
        )
    return final


def render_deployment_attestation(
    *,
    manifest_sha256: str,
    payloads: Mapping[str, str],
    final_images: Mapping[str, str],
    build_provenance: Mapping[str, BuildProvenanceEvidence],
    inspect_image: InspectImage,
) -> bytes:
    """Render canonical evidence after checking every local image binding."""

    if not isinstance(manifest_sha256, str) or _SHA256.fullmatch(manifest_sha256) is None:
        raise DeploymentAttestationError("manifest SHA-256 is invalid")
    grouped_payloads = _validate_payload_groups(payloads)
    normalized_finals = _validate_final_images(final_images)
    if set(build_provenance) != set(IMAGE_GROUP_SERVICES):
        raise DeploymentAttestationError(
            "BuildKit provenance must bind exactly six image groups"
        )
    records: list[dict[str, str]] = []
    observed_final_ids: set[str] = set()
    for group in sorted(IMAGE_GROUP_SERVICES):
        payload_id = grouped_payloads[group]
        final_image = normalized_finals[group]
        evidence = build_provenance[group]
        if evidence.group != group or evidence.final_digest != final_image:
            raise DeploymentAttestationError(
                f"BuildKit provenance does not bind the final digest for group {group}"
            )
        final = _verify_image_binding(
            group=group,
            payload_image_id=payload_id,
            final_image=final_image,
            inspect_image=inspect_image,
        )
        if final.image_id in observed_final_ids:
            raise DeploymentAttestationError(
                "the six final image references resolve to duplicate image IDs"
            )
        observed_final_ids.add(final.image_id)
        records.extend(
            {
                "final_image": final_image,
                "payload_image_id": payload_id,
                "service": service,
            }
            for service in IMAGE_GROUP_SERVICES[group]
        )
    records.sort(key=lambda record: record["service"])
    if len(records) != 14 or {record["service"] for record in records} != EXPECTED_SERVICES:
        raise DeploymentAttestationError(
            "internal service expansion did not produce exactly fourteen services"
        )
    return canonical_bytes(
        {
            "images": records,
            "provenance_manifest_sha256": manifest_sha256,
            "schema_version": SCHEMA_VERSION,
            "status": "ready-v1",
        }
    )


def _parse_inspection(raw: bytes, *, reference: str) -> ImageInspection:
    if not raw or len(raw) > MAX_INSPECT_BYTES:
        raise DeploymentAttestationError(
            f"Docker inspect output size is invalid for {reference}"
        )
    try:
        text = raw.decode("utf-8")
        if not text.endswith("\n") or "\n" in text[:-1]:
            raise ValueError("not one line")
        parts = text[:-1].split("\t")
        if len(parts) != 4:
            raise ValueError("not four fields")
        image_id = json.loads(parts[0], object_pairs_hook=_unique_object)
        repo_digests = json.loads(parts[1], object_pairs_hook=_unique_object)
        rootfs = json.loads(parts[2], object_pairs_hook=_unique_object)
        config = json.loads(parts[3], object_pairs_hook=_unique_object)
    except (
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
        _DuplicateKey,
    ) as exc:
        raise DeploymentAttestationError(
            f"Docker inspect output is malformed for {reference}"
        ) from exc
    if (
        not isinstance(image_id, str)
        or _SHA256_ID.fullmatch(image_id) is None
        or not isinstance(repo_digests, list)
        or any(
            not isinstance(item, str) or _PINNED_IMAGE.fullmatch(item) is None
            for item in repo_digests
        )
        or len(set(repo_digests)) != len(repo_digests)
        or not isinstance(rootfs, dict)
        or rootfs.get("Type") != "layers"
        or not isinstance(rootfs.get("Layers"), list)
        or not rootfs["Layers"]
        or any(
            not isinstance(layer, str) or _SHA256_ID.fullmatch(layer) is None
            for layer in rootfs["Layers"]
        )
        or not isinstance(config, dict)
        or any(not isinstance(key, str) for key in config)
    ):
        raise DeploymentAttestationError(
            f"Docker inspect identity is invalid for {reference}"
        )
    return ImageInspection(
        image_id=image_id,
        repo_digests=tuple(sorted(repo_digests)),
        layers=tuple(rootfs["Layers"]),
        config_raw=canonical_bytes(config),
    )


def _normalized_tar_name(name: str) -> str:
    while name.startswith("./"):
        name = name[2:]
    path = PurePosixPath(name)
    if (
        not name
        or name.startswith("/")
        or "\0" in name
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise DeploymentAttestationError("Docker image archive has an unsafe path")
    return path.as_posix()


def _relevant_archive_paths(expected_files: Mapping[str, tuple[bytes, int]]) -> set[str]:
    paths = set(expected_files)
    for target in expected_files:
        parts = PurePosixPath(target).parts
        paths.update("/".join(parts[:index]) for index in range(1, len(parts)))
    return paths


def _final_suffix_policy(group: str) -> tuple[set[str], set[str], set[str]]:
    if group not in DERIVED_FINAL_GROUPS:
        raise DeploymentAttestationError(
            f"production gate suffix policy is unavailable for group {group}"
        )
    files = set(COMMON_FINAL_SUFFIX_FILES)
    removals = set(COMMON_FINAL_SUFFIX_REMOVALS)
    if group == "airflow-scheduler":
        files.update(SCHEDULER_FINAL_SUFFIX_FILES)
        removals.update(SCHEDULER_FINAL_SUFFIX_REMOVALS)
    directories: set[str] = set()
    for target in files:
        parts = PurePosixPath(target).parts
        directories.update(
            "/".join(parts[:index]) for index in range(1, len(parts))
        )
    return files, directories, removals


def _parse_layer_changes(
    stream: _DigestingReader,
    *,
    tracked_paths: set[str],
) -> _LayerChanges:
    removals: set[str] = set()
    opaque_directories: set[str] = set()
    additions: list[tuple[str, _ArchivedNode]] = []
    changed_paths: list[str] = []
    observed_names: set[str] = set()
    has_extended_metadata = False
    member_count = 0
    with tarfile.open(fileobj=stream, mode="r|*") as archive:
        if archive.pax_headers:
            has_extended_metadata = True
        for member in archive:
            member_count += 1
            if member_count > MAX_ARCHIVE_MEMBERS:
                raise DeploymentAttestationError(
                    "Docker image layer contains too many archive members"
                )
            name = _normalized_tar_name(member.name)
            if name in observed_names:
                raise DeploymentAttestationError(
                    "Docker image layer has duplicate normalized member names"
                )
            observed_names.add(name)
            if member.pax_headers:
                has_extended_metadata = True
            parent, _, basename = name.rpartition("/")
            is_whiteout = basename == ".wh..wh..opq" or basename.startswith(
                ".wh."
            )
            if is_whiteout and (
                not member.isreg()
                or member.size != 0
                or bool(member.linkname)
            ):
                raise DeploymentAttestationError(
                    "Docker image layer has a malformed whiteout"
                )
            if basename == ".wh..wh..opq":
                opaque_directories.add(parent)
                continue
            if basename.startswith(".wh."):
                if basename == ".wh.":
                    raise DeploymentAttestationError(
                        "Docker image layer has a malformed whiteout"
                    )
                removed = f"{parent}/{basename[4:]}" if parent else basename[4:]
                removals.add(removed)
                continue
            changed_paths.append(name)
            if name not in tracked_paths:
                continue
            if member.isdir():
                kind = "directory"
                raw = b""
            elif member.isreg():
                kind = "regular"
                if member.size < 0 or member.size > MAX_GATE_FILE_BYTES:
                    raise DeploymentAttestationError(
                        "tracked production image file is too large"
                    )
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise DeploymentAttestationError(
                        "tracked production image file cannot be read"
                    )
                raw = extracted.read(MAX_GATE_FILE_BYTES + 1)
                if len(raw) != member.size:
                    raise DeploymentAttestationError(
                        "tracked production image file is truncated"
                    )
            elif member.issym():
                kind = "symlink"
                raw = b""
            elif member.islnk():
                kind = "hardlink"
                raw = b""
            else:
                kind = "special"
                raw = b""
            additions.append(
                (
                    name,
                    _ArchivedNode(
                        kind=kind,
                        raw=raw,
                        mode=stat.S_IMODE(member.mode),
                        uid=member.uid,
                        gid=member.gid,
                        link_target=member.linkname,
                        has_extended_metadata=bool(member.pax_headers),
                    ),
                )
            )
    return _LayerChanges(
        removals=tuple(sorted(removals)),
        opaque_directories=tuple(sorted(opaque_directories)),
        additions=tuple(additions),
        changed_paths=tuple(changed_paths),
        has_extended_metadata=has_extended_metadata,
    )


def _remove_archive_state(state: dict[str, _ArchivedNode], path: str) -> None:
    for existing in tuple(state):
        if existing == path or existing.startswith(path + "/"):
            state.pop(existing)


def _apply_layer_changes(
    state: dict[str, _ArchivedNode], changes: _LayerChanges
) -> None:
    for directory in changes.opaque_directories:
        if not directory:
            state.clear()
        else:
            for existing in tuple(state):
                if existing.startswith(directory + "/"):
                    state.pop(existing)
    for removed in changes.removals:
        _remove_archive_state(state, removed)
    for path, node in changes.additions:
        if node.kind != "directory":
            for existing in tuple(state):
                if existing.startswith(path + "/"):
                    state.pop(existing)
        state[path] = node


def _verify_suffix_layer_changes(
    changes: _LayerChanges,
    *,
    allowed_files: set[str],
    allowed_directories: set[str],
    allowed_removals: set[str],
    expected_files: Mapping[str, tuple[bytes, int]],
    payload_python: _ArchivedNode,
) -> None:
    if changes.has_extended_metadata:
        raise DeploymentAttestationError(
            "production gate suffix layer has extended archive metadata"
        )
    if changes.opaque_directories:
        raise DeploymentAttestationError(
            "production gate suffix layer has an opaque-directory whiteout"
        )
    unexpected_removals = set(changes.removals) - allowed_removals
    if unexpected_removals:
        raise DeploymentAttestationError(
            "production gate suffix layer removes an unreviewed path: /"
            + sorted(unexpected_removals)[0]
        )
    unexpected_paths = set(changes.changed_paths) - (
        allowed_files | allowed_directories
    )
    if unexpected_paths:
        raise DeploymentAttestationError(
            "production gate suffix layer changes an unreviewed path: /"
            + sorted(unexpected_paths)[0]
        )
    symlinks = {
        "usr/local/bin/python": "python3",
        "usr/local/bin/python3": "python3.11",
        "opt/legacy-scraper-venv/bin/python": "python3",
        "opt/legacy-scraper-venv/bin/python3": "python3.11",
    }
    payload_python_copies = {
        "usr/local/libexec/whoscored-python-real",
        "opt/legacy-scraper-venv/bin/python3.11",
    }
    wrapper_path = "usr/local/bin/python3.11"
    wrapper_raw = expected_files["usr/local/bin/whoscored-production-python"][0]
    for path, node in changes.additions:
        if node.has_extended_metadata:
            raise DeploymentAttestationError(
                f"production gate suffix path has extended metadata: /{path}"
            )
        if path in allowed_directories:
            if (
                node.kind != "directory"
                or node.mode != 0o755
                or node.uid != 0
                or node.gid != 0
                or node.link_target
            ):
                raise DeploymentAttestationError(
                    f"production gate suffix directory is unexpected: /{path}"
                )
            continue
        if path in symlinks:
            if (
                node.kind != "symlink"
                or node.link_target != symlinks[path]
                or node.mode != 0o777
                or node.uid != 0
                or node.gid != 0
            ):
                raise DeploymentAttestationError(
                    f"production gate suffix symlink is unexpected: /{path}"
                )
            continue
        if node.kind != "regular" or node.link_target or node.uid != 0 or node.gid != 0:
            raise DeploymentAttestationError(
                f"production gate suffix file type or owner is unexpected: /{path}"
            )
        if path in expected_files:
            expected_raw, expected_mode = expected_files[path]
            if node.raw != expected_raw or node.mode not in {
                expected_mode,
                expected_mode | 0o200,
            }:
                raise DeploymentAttestationError(
                    f"production gate suffix file content is unexpected: /{path}"
                )
        elif path == wrapper_path:
            if node.raw != wrapper_raw or node.mode != 0o555:
                raise DeploymentAttestationError(
                    f"production Python wrapper is unexpected: /{path}"
                )
        elif path in payload_python_copies:
            if node.raw != payload_python.raw or node.mode != 0o555:
                raise DeploymentAttestationError(
                    f"production Python payload copy is unexpected: /{path}"
                )
        else:
            raise DeploymentAttestationError(
                f"production gate suffix regular file is unreviewed: /{path}"
            )


def _verify_gate_archive(
    stream: Any,
    *,
    group: str,
    payload_layer_count: int,
    final_layer_count: int,
    expected_files: Mapping[str, tuple[bytes, int]],
) -> None:
    if set(expected_files) != set(GATE_IMAGE_FILES):
        raise DeploymentAttestationError(
            "internal production gate image-file policy is incomplete"
        )
    if (
        isinstance(payload_layer_count, bool)
        or not isinstance(payload_layer_count, int)
        or isinstance(final_layer_count, bool)
        or not isinstance(final_layer_count, int)
        or payload_layer_count <= 0
        or final_layer_count <= payload_layer_count
    ):
        raise DeploymentAttestationError(
            "production gate payload/final layer boundary is invalid"
        )
    allowed_files, allowed_directories, allowed_removals = _final_suffix_policy(
        group
    )
    tracked_paths = _relevant_archive_paths(expected_files)
    tracked_paths.update(allowed_files)
    tracked_paths.update(allowed_directories)
    layer_changes: dict[str, _LayerChanges] = {}
    invalid_tar_blobs: set[str] = set()
    observed_blobs: set[str] = set()
    manifest_raw: bytes | None = None
    member_count = 0
    try:
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                member_count += 1
                if member_count > MAX_ARCHIVE_MEMBERS:
                    raise DeploymentAttestationError(
                        "Docker image archive contains too many members"
                    )
                name = _normalized_tar_name(member.name)
                if not member.isreg():
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise DeploymentAttestationError(
                        "Docker image archive member cannot be read"
                    )
                if name == "manifest.json":
                    if manifest_raw is not None or member.size > MAX_INSPECT_BYTES:
                        raise DeploymentAttestationError(
                            "Docker image archive manifest is invalid"
                        )
                    manifest_raw = extracted.read(MAX_INSPECT_BYTES + 1)
                    if len(manifest_raw) != member.size:
                        raise DeploymentAttestationError(
                            "Docker image archive manifest is truncated"
                        )
                    continue
                prefix = "blobs/sha256/"
                if not name.startswith(prefix):
                    continue
                blob_digest = name.removeprefix(prefix)
                if (
                    _SHA256.fullmatch(blob_digest) is None
                    or name in observed_blobs
                    or member.size <= 0
                ):
                    raise DeploymentAttestationError(
                        "Docker image archive blob identity is invalid"
                    )
                observed_blobs.add(name)
                reader = _DigestingReader(extracted)
                try:
                    layer_changes[name] = _parse_layer_changes(
                        reader,
                        tracked_paths=tracked_paths,
                    )
                except tarfile.TarError:
                    invalid_tar_blobs.add(name)
                while reader.read(1024 * 1024):
                    pass
                if reader.hexdigest() != blob_digest:
                    raise DeploymentAttestationError(
                        "Docker image archive blob digest is invalid"
                    )
    except (OSError, tarfile.TarError) as exc:
        raise DeploymentAttestationError("Docker image archive is malformed") from exc
    if manifest_raw is None:
        raise DeploymentAttestationError("Docker image archive manifest is absent")
    try:
        manifest = json.loads(
            manifest_raw.decode("utf-8"), object_pairs_hook=_unique_object
        )
    except (
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
        _DuplicateKey,
    ) as exc:
        raise DeploymentAttestationError(
            "Docker image archive manifest is malformed"
        ) from exc
    if not isinstance(manifest, list) or len(manifest) != 1:
        raise DeploymentAttestationError(
            "Docker image archive must contain exactly one image"
        )
    record = _require_mapping(manifest[0], label="Docker image archive manifest")
    layers = record.get("Layers")
    if (
        not isinstance(layers, list)
        or not layers
        or len(layers) != final_layer_count
        or any(
            not isinstance(name, str)
            or name not in observed_blobs
            or name in invalid_tar_blobs
            or name not in layer_changes
            for name in layers
        )
    ):
        raise DeploymentAttestationError(
            "Docker image archive layer order is incomplete"
        )
    state: dict[str, _ArchivedNode] = {}
    payload_state: dict[str, _ArchivedNode] | None = None
    suffix_changed_paths: set[str] = set()
    for index, name in enumerate(layers):
        if index == payload_layer_count:
            payload_state = dict(state)
        changes = layer_changes[name]
        if index >= payload_layer_count:
            if payload_state is None:
                raise DeploymentAttestationError(
                    "production gate payload layer boundary is absent"
                )
            payload_python = payload_state.get("usr/local/bin/python3.11")
            if (
                payload_python is None
                or payload_python.kind != "regular"
                or not payload_python.raw
                or payload_python.uid != 0
                or payload_python.gid != 0
                or payload_python.mode & 0o111 == 0
                or payload_python.mode & 0o022
                or payload_python.link_target
                or payload_python.has_extended_metadata
            ):
                raise DeploymentAttestationError(
                    "payload system Python is unsafe or absent from image"
                )
            _verify_suffix_layer_changes(
                changes,
                allowed_files=allowed_files,
                allowed_directories=allowed_directories,
                allowed_removals=allowed_removals,
                expected_files=expected_files,
                payload_python=payload_python,
            )
            suffix_changed_paths.update(changes.changed_paths)
            suffix_changed_paths.update(changes.removals)
        _apply_layer_changes(state, changes)
    if payload_state is None or not allowed_files.issubset(suffix_changed_paths):
        raise DeploymentAttestationError(
            "production gate suffix does not contain the exact reviewed file delta"
        )
    for path in sorted(tracked_paths - set(expected_files) - allowed_files):
        node = state.get(path)
        if (
            node is None
            or node.kind != "directory"
            or node.uid != 0
            or node.gid != 0
            or node.mode != 0o755
            or node.link_target
            or node.has_extended_metadata
        ):
            raise DeploymentAttestationError(
                f"production gate parent directory is unsafe in image: /{path}"
            )
    for path, (expected_raw, expected_mode) in expected_files.items():
        node = state.get(path)
        if (
            node is None
            or node.kind != "regular"
            or node.raw != expected_raw
            or node.mode != expected_mode
            or node.uid != 0
            or node.gid != 0
            or node.link_target
            or node.has_extended_metadata
        ):
            raise DeploymentAttestationError(
                f"production gate file differs in final image: /{path}"
            )
    payload_python = payload_state["usr/local/bin/python3.11"]
    final_regular_copies = {
        "usr/local/libexec/whoscored-python-real": payload_python.raw,
        "usr/local/bin/python3.11": expected_files[
            "usr/local/bin/whoscored-production-python"
        ][0],
    }
    if group == "airflow-scheduler":
        final_regular_copies[
            "opt/legacy-scraper-venv/bin/python3.11"
        ] = payload_python.raw
    for path, expected_raw in final_regular_copies.items():
        node = state.get(path)
        if (
            node is None
            or node.kind != "regular"
            or node.raw != expected_raw
            or node.mode != 0o555
            or node.uid != 0
            or node.gid != 0
            or node.link_target
            or node.has_extended_metadata
        ):
            raise DeploymentAttestationError(
                f"production Python executable differs in final image: /{path}"
            )
    expected_symlinks = {
        path: target
        for path, target in {
            "usr/local/bin/python": "python3",
            "usr/local/bin/python3": "python3.11",
            "opt/legacy-scraper-venv/bin/python": "python3",
            "opt/legacy-scraper-venv/bin/python3": "python3.11",
        }.items()
        if path in allowed_files
    }
    for path, target in expected_symlinks.items():
        node = state.get(path)
        if (
            node is None
            or node.kind != "symlink"
            or node.link_target != target
            or node.mode != 0o777
            or node.uid != 0
            or node.gid != 0
            or node.has_extended_metadata
        ):
            raise DeploymentAttestationError(
                f"production Python symlink differs in final image: /{path}"
            )


def _protected_gate_file_expectations(
    root: Path, build_provenance: Mapping[str, BuildProvenanceEvidence]
) -> dict[str, tuple[bytes, int]]:
    validator = _validator_module()
    context = root / IMAGE_GROUP_CONTEXT_SPECS["airflow-scheduler"][0]
    snapshots: dict[str, tuple[bytes, tuple[int, ...]]] = {}
    for name in sorted(GATE_CONTEXT_INPUTS):
        try:
            raw, identity = validator.read_protected_regular_file_snapshot(
                context / name, label=f"production gate input {name}"
            )
        except (OSError, validator.ProvenanceError) as exc:
            raise DeploymentAttestationError(
                f"production gate input is not protected: {name}"
            ) from exc
        snapshots[name] = (raw, tuple(identity))
    observed = tuple(
        (name, hashlib.sha256(raw).hexdigest(), identity)
        for name, (raw, identity) in sorted(snapshots.items())
    )
    for group in DERIVED_FINAL_GROUPS:
        if build_provenance[group].gate_inputs != observed:
            raise DeploymentAttestationError(
                f"production gate inputs changed before image verification: {group}"
            )
    return {
        destination: (snapshots[source][0], mode)
        for destination, (source, mode) in GATE_IMAGE_FILES.items()
    }


def _verify_final_gate_files(
    reference: str,
    *,
    group: str,
    payload_layer_count: int,
    final_layer_count: int,
    docker_fd: int,
    expected_files: Mapping[str, tuple[bytes, int]],
) -> None:
    if _SHA256_ID.fullmatch(reference) is None and _PINNED_IMAGE.fullmatch(reference) is None:
        raise DeploymentAttestationError("Docker image-save reference is invalid")
    environment = {
        "DOCKER_CONFIG": "/nonexistent",
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            (
                f"/proc/self/fd/{docker_fd}",
                "image",
                "save",
                "--",
                reference,
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            pass_fds=(docker_fd,),
        )
        if process.stdout is None:
            raise DeploymentAttestationError("Docker image archive is unavailable")
        _verify_gate_archive(
            process.stdout,
            group=group,
            payload_layer_count=payload_layer_count,
            final_layer_count=final_layer_count,
            expected_files=expected_files,
        )
        process.stdout.close()
        if process.wait(timeout=30) != 0:
            raise DeploymentAttestationError("Docker image save failed")
    except DeploymentAttestationError:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        raise
    except (OSError, subprocess.TimeoutExpired) as exc:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        raise DeploymentAttestationError("Docker image save failed") from exc


def _inspect_with_docker(reference: str, *, docker_fd: int) -> ImageInspection:
    if _SHA256_ID.fullmatch(reference) is None and _PINNED_IMAGE.fullmatch(reference) is None:
        raise DeploymentAttestationError("Docker inspect reference is invalid")
    command = (
        f"/proc/self/fd/{docker_fd}",
        "image",
        "inspect",
        "--format",
        DOCKER_INSPECT_FORMAT,
        "--",
        reference,
    )
    environment = {
        "DOCKER_CONFIG": "/nonexistent",
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            pass_fds=(docker_fd,),
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DeploymentAttestationError(
            f"Docker image inspection failed for {reference}"
        ) from exc
    if result.returncode != 0 or result.stderr:
        raise DeploymentAttestationError(
            f"Docker image inspection failed for {reference}"
        )
    return _parse_inspection(result.stdout, reference=reference)


def _open_trusted_executable(path: Path, *, label: str) -> int:
    validator = _validator_module()
    directory_fd = -1
    executable_fd = -1
    admitted = False
    try:
        directory_fd, name = validator.open_protected_parent(path, label=label)
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
            or validator._stat_identity(metadata) != validator._stat_identity(entry)
        ):
            raise DeploymentAttestationError(
                f"{label} is not a protected executable"
            )
        admitted = True
        return executable_fd
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError(f"{label} is unavailable") from exc
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)
        if executable_fd >= 0 and not admitted:
            os.close(executable_fd)


def _open_trusted_docker() -> int:
    validator = _validator_module()
    socket_parent_fd = -1
    try:
        socket_parent_fd, socket_name = validator.open_protected_parent(
            DOCKER_SOCKET, label="Docker socket"
        )
        socket_metadata = os.stat(
            socket_name, dir_fd=socket_parent_fd, follow_symlinks=False
        )
        if (
            not stat.S_ISSOCK(socket_metadata.st_mode)
            or socket_metadata.st_uid != 0
            or socket_metadata.st_nlink != 1
            or socket_metadata.st_mode & 0o007
        ):
            raise DeploymentAttestationError("Docker socket is not protected")
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError("Docker socket is unavailable") from exc
    finally:
        if socket_parent_fd >= 0:
            os.close(socket_parent_fd)
    return _open_trusted_executable(DOCKER_CLI, label="Docker CLI")


def _require_protected_root(root: Path) -> Path:
    if not root.is_absolute():
        raise DeploymentAttestationError("release root must be an absolute path")
    try:
        resolved = root.resolve(strict=True)
    except OSError as exc:
        raise DeploymentAttestationError("release root is missing") from exc
    if resolved != root or not root.is_dir():
        raise DeploymentAttestationError(
            "release root must be an existing canonical directory"
        )
    invoked_script = Path(__file__).absolute()
    canonical_script = root / "scripts/generate_whoscored_deployment_attestation.py"
    try:
        canonical_script_resolved = canonical_script.resolve(strict=True)
    except OSError as exc:
        raise DeploymentAttestationError(
            "canonical deployment-attestation generator is missing"
        ) from exc
    if invoked_script != canonical_script or canonical_script_resolved != canonical_script:
        raise DeploymentAttestationError(
            "generator must be invoked from the exact protected release root"
        )
    validator = _validator_module()
    directory_fd = -1
    try:
        directory_fd, _ = validator.open_protected_parent(
            root / ".deployment-attestation-root-check",
            label="release root",
        )
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError(f"release root is unsafe: {exc}") from exc
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)
    return root


def _open_output_parent(output: Path) -> tuple[int, str]:
    if not output.is_absolute():
        raise DeploymentAttestationError("output must be an absolute path")
    if output != Path(os.path.abspath(output)):
        raise DeploymentAttestationError("output must be a canonical lexical path")
    validator = _validator_module()
    try:
        return validator.open_protected_parent(output, label="deployment attestation")
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError(
            f"deployment attestation parent is unsafe: {exc}"
        ) from exc


def _require_output_absent(output: Path) -> None:
    directory_fd = -1
    try:
        directory_fd, name = _open_output_parent(output)
        try:
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise DeploymentAttestationError(
            "deployment attestation output already exists"
        )
    except OSError as exc:
        raise DeploymentAttestationError(
            "deployment attestation output cannot be inspected"
        ) from exc
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)


def _publish_new_file(output: Path, raw: bytes) -> None:
    directory_fd = -1
    temporary_fd = -1
    temporary_name = f".{output.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    try:
        directory_fd, name = _open_output_parent(output)
        try:
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise DeploymentAttestationError(
                "deployment attestation output already exists"
            )
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
                raise DeploymentAttestationError(
                    "cannot write deployment attestation"
                )
            offset += written
        os.fchmod(temporary_fd, 0o600)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = -1
        os.link(
            temporary_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
            follow_symlinks=False,
        )
        os.unlink(temporary_name, dir_fd=directory_fd)
        os.fsync(directory_fd)
    except DeploymentAttestationError:
        raise
    except OSError as exc:
        raise DeploymentAttestationError(
            "cannot publish deployment attestation"
        ) from exc
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if directory_fd >= 0:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except OSError:
                pass
            os.close(directory_fd)


def _verify_published(output: Path, expected: bytes) -> None:
    validator = _validator_module()
    try:
        raw, _ = validator.read_protected_regular_file_snapshot(
            output, label="published deployment attestation"
        )
        metadata = output.stat(follow_symlinks=False)
    except (OSError, validator.ProvenanceError) as exc:
        raise DeploymentAttestationError(
            "published deployment attestation is not protected"
        ) from exc
    if raw != expected or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise DeploymentAttestationError(
            "published deployment attestation differs or has the wrong mode"
        )


def generate_deployment_attestation(
    root: Path,
    *,
    output: Path,
    final_images: Mapping[str, str],
    build_metadata: Mapping[str, Path],
) -> dict[str, Any]:
    """Validate twice and create one external deployment attestation."""

    root = _require_protected_root(root)
    normalized_finals = _validate_final_images(final_images)
    if set(build_metadata) != set(IMAGE_GROUP_SERVICES):
        raise DeploymentAttestationError(
            "BuildKit metadata paths must bind exactly six image groups"
        )
    if output.is_absolute() and output.is_relative_to(root):
        raise DeploymentAttestationError(
            "deployment attestation output must be outside the release checkout"
        )
    for group, metadata_path in build_metadata.items():
        if not metadata_path.is_absolute() or metadata_path != Path(
            os.path.abspath(metadata_path)
        ):
            raise DeploymentAttestationError(
                f"BuildKit metadata path is not absolute and canonical for group {group}"
            )
        if metadata_path.is_relative_to(root):
            raise DeploymentAttestationError(
                f"BuildKit metadata must be outside the release checkout for group {group}"
            )
        if metadata_path == output:
            raise DeploymentAttestationError(
                "deployment output and BuildKit metadata must be distinct"
            )
    if len(set(build_metadata.values())) != len(build_metadata):
        raise DeploymentAttestationError(
            "the six BuildKit metadata paths must be distinct"
        )
    _require_output_absent(output)
    first_evidence = _validated_ready_evidence(root)
    first_provenance = _load_all_build_provenance(
        root,
        metadata_paths=build_metadata,
        final_images=normalized_finals,
        ready_evidence=first_evidence,
    )
    docker_fd = _open_trusted_docker()
    try:
        observed_inspections: dict[str, ImageInspection] = {}

        def inspector(reference: str) -> ImageInspection:
            inspection = _inspect_with_docker(reference, docker_fd=docker_fd)
            observed_inspections[reference] = inspection
            return inspection

        first = render_deployment_attestation(
            manifest_sha256=first_evidence.manifest_sha256,
            payloads=first_evidence.payloads,
            final_images=normalized_finals,
            build_provenance=first_provenance,
            inspect_image=inspector,
        )
        second_evidence = _validated_ready_evidence(root)
        second_provenance = _load_all_build_provenance(
            root,
            metadata_paths=build_metadata,
            final_images=normalized_finals,
            ready_evidence=second_evidence,
        )
        second = render_deployment_attestation(
            manifest_sha256=second_evidence.manifest_sha256,
            payloads=second_evidence.payloads,
            final_images=normalized_finals,
            build_provenance=second_provenance,
            inspect_image=inspector,
        )
        expected_gate_files = _protected_gate_file_expectations(
            root, second_provenance
        )
        grouped_payloads = _validate_payload_groups(second_evidence.payloads)
        for group in sorted(DERIVED_FINAL_GROUPS):
            payload_inspection = observed_inspections[grouped_payloads[group]]
            final_inspection = observed_inspections[normalized_finals[group]]
            _verify_final_gate_files(
                normalized_finals[group],
                group=group,
                payload_layer_count=len(payload_inspection.layers),
                final_layer_count=len(final_inspection.layers),
                docker_fd=docker_fd,
                expected_files=expected_gate_files,
            )
    finally:
        os.close(docker_fd)
    if (
        first_evidence != second_evidence
        or first_provenance != second_provenance
        or first != second
    ):
        raise DeploymentAttestationError(
            "build or Docker image evidence changed during generation"
        )
    _require_output_absent(output)
    _publish_new_file(output, first)
    _verify_published(output, first)
    return {
        "deployment_attestation_sha256": hashlib.sha256(first).hexdigest(),
        "image_group_count": len(IMAGE_GROUP_SERVICES),
        "provenance_manifest_sha256": first_evidence.manifest_sha256,
        "schema_version": SCHEMA_VERSION,
        "service_count": len(EXPECTED_SERVICES),
        "status": "ready-generated-v1",
    }


def _final_image_mapping(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        group, separator, reference = value.partition("=")
        if (
            not separator
            or not group
            or group.strip() != group
            or group in result
            or _PINNED_IMAGE.fullmatch(reference) is None
        ):
            raise DeploymentAttestationError(
                f"final image binding is duplicated or invalid: {value}"
            )
        result[group] = reference
    return _validate_final_images(result)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--final-image",
        action="append",
        default=[],
        metavar="GROUP=REPOSITORY@sha256:HEX",
        help="one of the six exact final registry image bindings",
    )
    parser.add_argument(
        "--build-metadata",
        action="append",
        default=[],
        metavar="GROUP=/ABSOLUTE/PROTECTED/METADATA.JSON",
        help="one of the six root-owned Buildx maximum-provenance metadata files",
    )
    return parser


def _require_isolated_process() -> None:
    if os.geteuid() != 0:
        raise DeploymentAttestationError("generator must run as root")
    if (
        sys.executable != "/usr/bin/python3"
        or not sys.flags.isolated
        or not sys.flags.no_site
        or not sys.flags.ignore_environment
    ):
        raise DeploymentAttestationError(
            "generator requires an isolated system Python -I -S interpreter"
        )
    inherited = sorted(
        name
        for name in os.environ
        if name in _CONTROL_ENV_NAMES
        or any(name.startswith(prefix) for prefix in _CONTROL_ENV_PREFIXES)
    )
    if inherited:
        raise DeploymentAttestationError(
            "unsafe Docker, Compose, loader, or Python environment is set: "
            + ", ".join(inherited)
        )
    if os.environ.get("PATH") != "/usr/bin:/bin":
        raise DeploymentAttestationError(
            "generator requires the exact PATH /usr/bin:/bin"
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _require_isolated_process()
        final_images = _final_image_mapping(args.final_image)
        build_metadata = _build_metadata_mapping(args.build_metadata)
        receipt = generate_deployment_attestation(
            args.root,
            output=args.output,
            final_images=final_images,
            build_metadata=build_metadata,
        )
    except DeploymentAttestationError as exc:
        print(f"WhoScored deployment attestation blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    sys.stdout.buffer.write(canonical_bytes(receipt))
    return 0


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    raise SystemExit(main())
