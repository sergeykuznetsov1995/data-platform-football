"""Fail-closed compatibility contract for a deployed WhoScored worker tree.

The Airflow services use bind-mounted source code, so files from two releases
can otherwise be combined in one Python process.  The contract pins the full
production closure: orchestration, runners, parsing, persistence, paid-proxy
authorization, and alert delivery.  Both the manifest and the checked-out core
package are validated against an explicit allowlist so a partial or stale
deployment cannot silently become part of a release.
"""

from __future__ import annotations

import hashlib
import importlib.machinery
import inspect
import json
import os
import re
import stat
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Optional


RUNTIME_CONTRACT_PATH = Path(__file__).with_name("runtime_contract.lock")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DISABLED_PYCACHE_PREFIX = Path("/__whoscored_runtime_bytecode_disabled__")
_IMAGE_TRUSTED_RUNTIME_CONTRACT_SHA256 = globals().get(
    "_IMAGE_TRUSTED_RUNTIME_CONTRACT_SHA256"
)

# Keep this tuple explicit and sorted.  It is the production code closure, not
# a best-effort directory snapshot: changing any member or adding a new core
# WhoScored module requires a reviewed lock-file update.
EXPECTED_RUNTIME_FILES = (
    ".dockerignore",
    "configs/medallion/competitions.yaml",
    "dags/__init__.py",
    "dags/dag_backfill_whoscored.py",
    "dags/dag_backup_whoscored_storage.py",
    "dags/dag_canary_whoscored_proxy.py",
    "dags/dag_ingest_whoscored.py",
    "dags/scripts/__init__.py",
    "dags/scripts/run_whoscored_backfill_item.py",
    "dags/scripts/run_whoscored_scraper.py",
    "dags/scripts/whoscored_frozen_dq.py",
    "dags/scripts/whoscored_identity.py",
    "dags/scripts/whoscored_ops_store.py",
    "dags/scripts/whoscored_proxy_runtime.py",
    "dags/utils/__init__.py",
    "dags/utils/alerts.py",
    "dags/utils/config.py",
    "dags/utils/default_args.py",
    "dags/utils/maintenance_tasks.py",
    "dags/utils/silver_tasks.py",
    "docker/images/airflow/Dockerfile",
    "docker/images/airflow/requirements-airflow.txt",
    "docker/images/airflow/requirements-build-tools.txt",
    "docker/images/airflow/requirements-scheduler.txt",
    "docker/images/airflow/requirements-scraper-runner.txt",
    "docker/images/airflow/requirements-scraping.txt",
    "docker/images/airflow/requirements.txt",
    "docker/images/airflow/whoscored-production-entrypoint",
    "docker/images/airflow/whoscored-production-gate",
    "docker/images/airflow/whoscored-production-python",
    "docker/images/airflow/whoscored_capacity_worker_bootstrap.py",
    "docker/images/airflow/whoscored_production_gate.py",
    "docker/images/airflow/whoscored_runtime_pth.py",
    "docker/images/airflow/whoscored_runtime_startup.py",
    "docker/images/flaresolverr-whoscored/Dockerfile",
    "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
    "docker/images/flaresolverr-whoscored/entrypoint.sh",
    "scrapers/__init__.py",
    "scrapers/base/__init__.py",
    "scrapers/base/flaresolverr_client.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/sql_validator.py",
    "scrapers/base/trino_manager.py",
    "scrapers/fbref/__init__.py",
    "scrapers/fbref/control/__init__.py",
    "scrapers/fbref/control/migrations.py",
    "scrapers/fbref/control/models.py",
    "scrapers/fbref/control/store.py",
    "scrapers/fbref/policy.py",
    "scrapers/fbref/settings.py",
    "scrapers/sofascore/__init__.py",
    "scrapers/sofascore/runtime_fingerprint.py",
    "scrapers/sofascore/workload_plan.py",
    "scrapers/utils/__init__.py",
    "scrapers/utils/proxy_manager.py",
    "scrapers/utils/rate_limiter.py",
    "scrapers/whoscored/__init__.py",
    "scrapers/whoscored/catalog.py",
    "scrapers/whoscored/detailed_feeds.py",
    "scrapers/whoscored/domain.py",
    "scrapers/whoscored/parsers.py",
    "scrapers/whoscored/profile_policy.py",
    "scrapers/whoscored/proxy_campaign.py",
    "scrapers/whoscored/raw_store.py",
    "scrapers/whoscored/repository.py",
    "scrapers/whoscored/runtime_contract.py",
    "scrapers/whoscored/runtime_limits.py",
    "scrapers/whoscored/service.py",
    "scrapers/whoscored/source_circuit.py",
    "scrapers/whoscored/stage_feeds.py",
    "scrapers/whoscored/transport.py",
    "scripts/__init__.py",
    "scripts/cleanup_whoscored_v2_migration.py",
    "scripts/flaresolverr_extended.py",
    "scripts/migrate_whoscored_v2.py",
    "scripts/proxy_filter/__init__.py",
    "scripts/proxy_filter/budget.py",
    "scripts/proxy_filter/filter_proxy.py",
    "scripts/research/bench_whoscored_capacity.py",
    "scripts/research/bench_whoscored_workflow.py",
    "scripts/research/whoscored_capacity_container_runtime.py",
    "scripts/research/whoscored_capacity_worker_exec.py",
    "scripts/validate_whoscored_build_provenance.py",
    "scripts/whoscored_paid_gateway.py",
    "scripts/whoscored_production_admission.py",
    "scripts/whoscored_proxy_campaign.py",
    "scripts/whoscored_raw_backup.py",
    "scripts/whoscored_v2_object_contract.py",
)
_EXPECTED_RUNTIME_FILE_SET = frozenset(EXPECTED_RUNTIME_FILES)
_ATTESTED_STATIC_RUNTIME_FILES = frozenset(
    {"configs/medallion/competitions.yaml"}
)
_EXPECTED_CORE_RUNTIME_FILES = frozenset(
    relative
    for relative in EXPECTED_RUNTIME_FILES
    if Path(relative).parent.as_posix() == "scrapers/whoscored"
    and relative.endswith(".py")
)
_SOURCE_LOADER = importlib.machinery.SourceFileLoader
_EXTENSION_SUFFIXES = tuple(
    sorted(
        {
            *importlib.machinery.EXTENSION_SUFFIXES,
            ".so",
            ".pyd",
            ".dll",
            ".dylib",
        },
        key=len,
        reverse=True,
    )
)


class RuntimeContractError(RuntimeError):
    """Raised when mounted WhoScored code is not one coherent release."""


def require_production_runtime_class(*, operation: str) -> str:
    """Require the image-authenticated production class for source or storage.

    A checkout outside ``/opt/airflow`` remains usable for local development
    and unit tests when no image runtime marker exists.  Once an image marker
    exists, the image-owned private verifier is mandatory; assigning a forged
    value to ``sys._whoscored_runtime_class`` is therefore insufficient.
    """

    if not isinstance(operation, str) or not operation.strip():
        raise ValueError("WhoScored runtime operation must be a non-empty string")
    runtime_root = Path(os.path.abspath(__file__)).parents[2]
    runtime_class = getattr(sys, "_whoscored_runtime_class", None)
    if runtime_root != Path("/opt/airflow") and runtime_class is None:
        return "local-development"
    loader = getattr(sys, "_load_whoscored_runtime_contract", None)
    if not callable(loader):
        raise RuntimeContractError(
            f"{operation} requires the image-owned WhoScored runtime loader"
        )
    try:
        canonical = loader(str(runtime_root))
    except Exception as exc:
        raise RuntimeContractError(str(exc)) from exc
    if canonical is not getattr(sys, "_whoscored_runtime_contract", None):
        raise RuntimeContractError("WhoScored canonical runtime cache was replaced")
    verifier = getattr(sys, "_require_whoscored_runtime_class", None)
    if not callable(verifier):
        raise RuntimeContractError(
            f"{operation} requires the image-owned WhoScored runtime-class verifier"
        )
    try:
        verified_class = verifier("production-v1", operation)
    except Exception as exc:
        raise RuntimeContractError(str(exc)) from exc
    if verified_class != "production-v1":
        raise RuntimeContractError(
            f"{operation} requires WhoScored runtime class production-v1"
        )
    return verified_class


class _DuplicateContractKey(ValueError):
    """Internal JSON decoding error for a duplicate object key."""


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateContractKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _process_start_time_ns() -> int:
    """Return the kernel-owned start timestamp for this Linux process.

    Bind-mounted Python cannot safely attest a replacement tree from an
    already-running interpreter: its functions may still be the old code even
    though ``__file__`` now names the new bytes.  Linux records the process
    start in clock ticks since boot; combining that value with ``/proc/uptime``
    avoids the lazily-instantiated ctime of procfs inodes.
    """

    try:
        stat_payload = Path("/proc/self/stat").read_text(encoding="ascii")
        stat_fields = stat_payload.rsplit(")", 1)[1].strip().split()
        start_ticks = int(stat_fields[19])
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        wall_after_ns = time.time_ns()
        uptime_ns = int(
            float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
            * 1_000_000_000
        )
    except (OSError, ValueError, IndexError) as exc:
        raise RuntimeContractError(
            "cannot prove WhoScored process start time; Linux /proc is required"
        ) from exc
    if clock_ticks <= 0 or start_ticks < 0 or uptime_ns <= 0:
        raise RuntimeContractError("invalid WhoScored process start timestamp")
    boot_ns = wall_after_ns - uptime_ns
    started_ns = boot_ns + (start_ticks * 1_000_000_000 // clock_ticks)
    if started_ns <= 0:
        raise RuntimeContractError("invalid WhoScored process start timestamp")
    return started_ns


def _read_stable_regular_file(
    path: Path,
    *,
    process_started_ns: Optional[int] = None,
) -> bytes:
    """Read one non-symlink regular file and reject concurrent replacement."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeContractError(f"cannot open required runtime file {path}: {exc}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeContractError(
                f"required runtime path is not a regular file: {path}"
            )
        if process_started_ns is not None and before.st_ctime_ns > process_started_ns:
            raise RuntimeContractError(
                "WhoScored runtime file changed after process start; restart all "
                f"workers before executing this release: {path}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    except OSError as exc:
        raise RuntimeContractError(f"cannot read required runtime file {path}: {exc}") from exc
    finally:
        os.close(descriptor)

    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise RuntimeContractError(
            f"required runtime file changed while it was being read: {path}"
        )
    if process_started_ns is not None and after.st_ctime_ns > process_started_ns:
        raise RuntimeContractError(
            "WhoScored runtime file changed after process start; restart all "
            f"workers before executing this release: {path}"
        )
    return b"".join(chunks)


def _open_runtime_root(path: Path) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise RuntimeContractError(
            f"cannot open WhoScored runtime root without symlinks {path}: {exc}"
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise RuntimeContractError(f"WhoScored runtime root is not a directory: {path}")
    return descriptor


def _assert_immutable_runtime_directory(
    metadata: os.stat_result,
    *,
    display_path: Path,
    process_started_ns: Optional[int],
    enforce: bool,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeContractError(
            f"WhoScored runtime path component is not a directory: {display_path}"
        )
    if (
        enforce
        and process_started_ns is not None
        and metadata.st_ctime_ns > process_started_ns
    ):
        raise RuntimeContractError(
            "WhoScored runtime directory changed after process start; restart all "
            f"workers before executing this release: {display_path}"
        )


def _open_relative_directory(
    root_descriptor: int,
    root: Path,
    parts: tuple[str, ...],
    *,
    process_started_ns: Optional[int],
    enforce_directory_immutability: bool,
) -> int:
    descriptor = os.dup(root_descriptor)
    display_path = root
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        for part in parts:
            display_path /= part
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except OSError as exc:
                raise RuntimeContractError(
                    "cannot open WhoScored runtime directory component without "
                    f"symlinks {display_path}: {exc}"
                ) from exc
            os.close(descriptor)
            descriptor = child
            _assert_immutable_runtime_directory(
                os.fstat(descriptor),
                display_path=display_path,
                process_started_ns=process_started_ns,
                enforce=enforce_directory_immutability,
            )
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _read_stable_relative_file(
    root_descriptor: int,
    root: Path,
    relative: str,
    *,
    process_started_ns: Optional[int],
    enforce_directory_immutability: bool,
) -> bytes:
    parts = Path(relative).parts
    if (
        not parts
        or Path(relative).is_absolute()
        or "." in parts
        or ".." in parts
    ):
        raise RuntimeContractError(f"invalid relative runtime path: {relative!r}")
    parent_descriptor = _open_relative_directory(
        root_descriptor,
        root,
        tuple(parts[:-1]),
        process_started_ns=process_started_ns,
        enforce_directory_immutability=enforce_directory_immutability,
    )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    display_path = root.joinpath(*parts)
    try:
        try:
            descriptor = os.open(parts[-1], flags, dir_fd=parent_descriptor)
        except OSError as exc:
            raise RuntimeContractError(
                f"cannot open required runtime file {display_path}: {exc}"
            ) from exc
    finally:
        os.close(parent_descriptor)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeContractError(
                f"required runtime path is not a regular file: {display_path}"
            )
        if process_started_ns is not None and before.st_ctime_ns > process_started_ns:
            raise RuntimeContractError(
                "WhoScored runtime file changed after process start; restart all "
                f"workers before executing this release: {display_path}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    except OSError as exc:
        raise RuntimeContractError(
            f"cannot read required runtime file {display_path}: {exc}"
        ) from exc
    finally:
        os.close(descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise RuntimeContractError(
            f"required runtime file changed while it was being read: {display_path}"
        )
    if process_started_ns is not None and after.st_ctime_ns > process_started_ns:
        raise RuntimeContractError(
            "WhoScored runtime file changed after process start; restart all "
            f"workers before executing this release: {display_path}"
        )
    return b"".join(chunks)


def _load_contract(
    path: Path,
    *,
    process_started_ns: Optional[int] = None,
    root_descriptor: Optional[int] = None,
    root: Optional[Path] = None,
    relative: Optional[str] = None,
    enforce_directory_immutability: bool = False,
    expected_sha256: Optional[str] = None,
) -> Mapping[str, Any]:
    try:
        if root_descriptor is not None:
            if root is None or relative is None:
                raise RuntimeContractError("incomplete fd-relative contract path")
            raw = _read_stable_relative_file(
                root_descriptor,
                root,
                relative,
                process_started_ns=process_started_ns,
                enforce_directory_immutability=enforce_directory_immutability,
            )
        else:
            raw = _read_stable_regular_file(
                path,
                process_started_ns=process_started_ns,
            )
        if expected_sha256 is not None:
            actual_sha256 = hashlib.sha256(raw).hexdigest()
            if actual_sha256 != expected_sha256:
                raise RuntimeContractError(
                    "WhoScored runtime contract differs from the image trust root: "
                    f"expected={expected_sha256}, actual={actual_sha256}"
                )
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except _DuplicateContractKey as exc:
        raise RuntimeContractError(
            f"duplicate key in WhoScored runtime contract {path}: {exc}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeContractError(
            f"cannot load WhoScored runtime contract {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeContractError("WhoScored runtime contract must be a JSON object")
    return payload


def _sha256(path: Path, *, process_started_ns: Optional[int] = None) -> str:
    return hashlib.sha256(
        _read_stable_regular_file(
            path,
            process_started_ns=process_started_ns,
        )
    ).hexdigest()


def _sha256_relative(
    root_descriptor: int,
    root: Path,
    relative: str,
    *,
    process_started_ns: Optional[int],
    enforce_directory_immutability: bool,
) -> str:
    return hashlib.sha256(
        _read_stable_relative_file(
            root_descriptor,
            root,
            relative,
            process_started_ns=process_started_ns,
            enforce_directory_immutability=enforce_directory_immutability,
        )
    ).hexdigest()


def _runtime_candidate(root: Path, relative: str) -> Path:
    """Resolve source files or their immutable image-baked build inputs."""

    candidate = root / relative
    image_baked_input = relative == ".dockerignore" or relative.startswith(
        "docker/images/"
    )
    if candidate.exists() or not image_baked_input:
        return candidate
    return root / "runtime-contract" / relative


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _validate_declared_file_set(files: Mapping[str, Any]) -> None:
    if tuple(sorted(_EXPECTED_RUNTIME_FILE_SET)) != EXPECTED_RUNTIME_FILES:
        raise RuntimeContractError(
            "WhoScored expected runtime file allowlist must be sorted and unique"
        )
    for relative, expected_hash in files.items():
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or "." in Path(relative).parts
            or ".." in Path(relative).parts
            or not isinstance(expected_hash, str)
            or _SHA256_RE.fullmatch(expected_hash) is None
        ):
            raise RuntimeContractError(
                f"invalid WhoScored runtime file contract entry: {relative!r}"
            )

    declared = frozenset(files)
    missing = sorted(_EXPECTED_RUNTIME_FILE_SET - declared)
    unexpected = sorted(declared - _EXPECTED_RUNTIME_FILE_SET)
    if missing or unexpected:
        raise RuntimeContractError(
            "WhoScored runtime contract file set mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )


def _validate_core_package_file_set(
    root: Path,
    *,
    root_descriptor: int,
    process_started_ns: Optional[int],
    enforce_directory_immutability: bool,
) -> None:
    core_dir = root / "scrapers" / "whoscored"
    descriptor: Optional[int] = None
    try:
        descriptor = _open_relative_directory(
            root_descriptor,
            root,
            ("scrapers", "whoscored"),
            process_started_ns=process_started_ns,
            enforce_directory_immutability=enforce_directory_immutability,
        )
        actual = frozenset(
            f"scrapers/whoscored/{name}"
            for name in os.listdir(descriptor)
            if name.endswith(".py")
        )
        for relative in actual:
            metadata = os.stat(
                Path(relative).name,
                dir_fd=descriptor,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(metadata.st_mode):
                raise RuntimeContractError(
                    "WhoScored core runtime member is not a regular file: "
                    f"{root / relative}"
                )
    except OSError as exc:
        raise RuntimeContractError(
            f"cannot inspect WhoScored core runtime directory {core_dir}: {exc}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    missing = sorted(_EXPECTED_CORE_RUNTIME_FILES - actual)
    unexpected = sorted(actual - _EXPECTED_CORE_RUNTIME_FILES)
    if missing or unexpected:
        raise RuntimeContractError(
            "WhoScored core runtime file set mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )


def _runtime_module_name(relative: str) -> tuple[str, bool]:
    path = Path(relative)
    parts = list(path.with_suffix("").parts)
    is_package = parts[-1] == "__init__"
    if is_package:
        parts.pop()
    return ".".join(parts), is_package


def _expected_runtime_module_origins(
    root: Path,
) -> dict[str, tuple[Path, bool]]:
    """Map every supported import spelling to one attested source file."""

    result: dict[str, tuple[Path, bool]] = {}

    def add(name: str, relative: str, *, is_package: bool) -> None:
        expected = (root / relative, is_package)
        previous = result.setdefault(name, expected)
        if previous != expected:
            raise RuntimeContractError(
                "WhoScored runtime import name is ambiguous: "
                f"module={name!r}, origins={[str(previous[0]), str(expected[0])]}"
            )

    for relative in EXPECTED_RUNTIME_FILES:
        if (
            not relative.endswith(".py")
            or relative.startswith("docker/images/airflow/")
        ):
            continue
        name, is_package = _runtime_module_name(relative)
        add(name, relative, is_package=is_package)
        path = Path(relative)
        if path.parts[:2] == ("dags", "utils"):
            alias_parts = path.parts[1:]
            if alias_parts[-1] == "__init__.py":
                alias_parts = alias_parts[:-1]
            else:
                alias_parts = (*alias_parts[:-1], Path(alias_parts[-1]).stem)
            add(".".join(alias_parts), relative, is_package=is_package)
        if path.parent.as_posix() == "dags" and path.stem.startswith("dag_"):
            add(path.stem, relative, is_package=False)
    return result


def _is_module_file_artifact(name: str, stem: str) -> bool:
    if name in {f"{stem}.py", f"{stem}.pyc", f"{stem}.pyo"}:
        return True
    return any(
        name == f"{stem}{suffix}"
        or (name.startswith(f"{stem}.") and name.endswith(suffix))
        for suffix in _EXTENSION_SUFFIXES
    )


def _is_package_init_artifact(name: str) -> bool:
    if name in {"__init__.py", "__init__.pyc", "__init__.pyo"}:
        return True
    return any(
        name == f"__init__{suffix}"
        or (name.startswith("__init__.") and name.endswith(suffix))
        for suffix in _EXTENSION_SUFFIXES
    )


def _import_candidates(search_root: Path, stem: str) -> tuple[Path, ...]:
    """Return executable file/package candidates without importing them."""

    candidates: set[Path] = set()
    try:
        entries = tuple(search_root.iterdir())
    except FileNotFoundError:
        return ()
    except OSError as exc:
        raise RuntimeContractError(
            f"cannot inspect Python import path {search_root}: {exc}"
        ) from exc
    for entry in entries:
        if _is_module_file_artifact(entry.name, stem):
            candidates.add(Path(os.path.abspath(entry)))
    package = search_root / stem
    if os.path.lexists(package):
        try:
            package_metadata = os.lstat(package)
        except OSError as exc:
            raise RuntimeContractError(
                f"cannot inspect Python package candidate {package}: {exc}"
            ) from exc
        if stat.S_ISLNK(package_metadata.st_mode):
            candidates.add(Path(os.path.abspath(package)))
        elif stat.S_ISDIR(package_metadata.st_mode):
            try:
                package_entries = tuple(package.iterdir())
            except OSError as exc:
                raise RuntimeContractError(
                    f"cannot inspect Python package candidate {package}: {exc}"
                ) from exc
            for entry in package_entries:
                if _is_package_init_artifact(entry.name):
                    candidates.add(Path(os.path.abspath(entry)))
            if not any(
                _is_package_init_artifact(entry.name)
                for entry in package_entries
            ):
                candidates.add(Path(os.path.abspath(package)))
    return tuple(sorted(candidates, key=str))


def _normalize_import_path(value: object) -> Path:
    if not isinstance(value, str):
        raise RuntimeContractError(
            f"WhoScored Python import path is not text: {value!r}"
        )
    return Path(os.path.abspath(value or os.getcwd()))


def _effective_import_paths() -> tuple[Path, ...]:
    paths = tuple(_normalize_import_path(value) for value in sys.path)
    pythonpath = os.environ.get("PYTHONPATH")
    if not pythonpath or sys.flags.isolated:
        return paths
    declared = tuple(
        _normalize_import_path(value)
        for value in pythonpath.split(os.pathsep)
    )
    cursor = 0
    for value in declared:
        try:
            cursor = paths.index(value, cursor) + 1
        except ValueError as exc:
            raise RuntimeContractError(
                "WhoScored PYTHONPATH differs from the interpreter import path: "
                f"missing_or_reordered={value}"
            ) from exc
    return paths


def _validate_top_level_import_resolution(
    root: Path,
    paths: tuple[Path, ...],
) -> None:
    canonical = {
        "dags": (root, root / "dags" / "__init__.py"),
        "scrapers": (root, root / "scrapers" / "__init__.py"),
        "scripts": (root, root / "scripts" / "__init__.py"),
        "utils": (root / "dags", root / "dags" / "utils" / "__init__.py"),
    }
    for module_name, (expected_search_root, expected_origin) in canonical.items():
        winner: Optional[tuple[Path, tuple[Path, ...]]] = None
        for search_root in paths:
            candidates = _import_candidates(search_root, module_name)
            if candidates:
                winner = (search_root, candidates)
                break
        if winner is None:
            raise RuntimeContractError(
                f"WhoScored canonical Python package is not importable: {module_name}"
            )
        search_root, candidates = winner
        if search_root != expected_search_root or candidates != (expected_origin,):
            raise RuntimeContractError(
                "WhoScored Python import shadow would win resolution: "
                f"module={module_name!r}, search_root={search_root}, "
                f"candidates={[str(item) for item in candidates]}, "
                f"expected={expected_origin}"
            )


def _validate_canonical_import_artifacts(
    root: Path,
    origins: Mapping[str, tuple[Path, bool]],
) -> None:
    checked: set[Path] = set()
    for expected_origin, is_package in origins.values():
        if expected_origin in checked:
            continue
        checked.add(expected_origin)
        if is_package:
            search_root = expected_origin.parent.parent
            stem = expected_origin.parent.name
        else:
            search_root = expected_origin.parent
            stem = expected_origin.stem
        candidates = _import_candidates(search_root, stem)
        if candidates != (expected_origin,):
            raise RuntimeContractError(
                "WhoScored canonical module has executable sibling shadows: "
                f"source={expected_origin}, "
                f"candidates={[str(item) for item in candidates]}"
            )
        try:
            metadata = os.lstat(expected_origin)
        except OSError as exc:
            raise RuntimeContractError(
                f"cannot inspect canonical Python source {expected_origin}: {exc}"
            ) from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeContractError(
                f"canonical Python source is not a regular file: {expected_origin}"
            )
        try:
            expected_origin.relative_to(root)
        except ValueError as exc:  # pragma: no cover - construction invariant
            raise RuntimeContractError(
                f"canonical Python source escaped runtime root: {expected_origin}"
            ) from exc


def _validate_loaded_module_origin(
    module_name: str,
    module: Any,
    *,
    expected_origin: Path,
    is_package: bool,
) -> None:
    spec = getattr(module, "__spec__", None)
    loader = getattr(spec, "loader", None)
    origin = getattr(spec, "origin", None)
    module_file = getattr(module, "__file__", None)
    loader_path = getattr(loader, "path", None)
    expected_text = str(expected_origin)
    if (
        not isinstance(origin, str)
        or Path(os.path.abspath(origin)) != expected_origin
        or not isinstance(module_file, str)
        or Path(os.path.abspath(module_file)) != expected_origin
        or not (
            type(loader) is _SOURCE_LOADER
            or (
                isinstance(loader, _SOURCE_LOADER)
                and getattr(loader, "_whoscored_fd_attested", False) is True
            )
        )
        or not isinstance(loader_path, str)
        or Path(os.path.abspath(loader_path)) != expected_origin
        or getattr(module, "__loader__", None) is not loader
    ):
        raise RuntimeContractError(
            "loaded WhoScored runtime module is not the fd-attested source: "
            f"module={module_name!r}, expected={expected_text}, "
            f"origin={origin!r}, file={module_file!r}, "
            f"loader={type(loader).__name__}, loader_path={loader_path!r}"
        )
    locations = getattr(spec, "submodule_search_locations", None)
    if is_package:
        expected_locations = (str(expected_origin.parent),)
        actual_locations = (
            tuple(str(Path(os.path.abspath(item))) for item in locations)
            if locations is not None
            else ()
        )
        if actual_locations != expected_locations:
            raise RuntimeContractError(
                "loaded WhoScored package has an unexpected search path: "
                f"module={module_name!r}, expected={expected_locations}, "
                f"actual={actual_locations}"
            )
    elif locations is not None:
        raise RuntimeContractError(
            f"loaded WhoScored source module became a package: {module_name!r}"
        )


def _validate_loaded_runtime_modules(
    root: Path,
    origins: Mapping[str, tuple[Path, bool]],
) -> None:
    for module_name, (expected_origin, is_package) in origins.items():
        module = sys.modules.get(module_name)
        if module is not None:
            _validate_loaded_module_origin(
                module_name,
                module,
                expected_origin=expected_origin,
                is_package=is_package,
            )

    expected_by_origin = {
        expected_origin: is_package
        for expected_origin, is_package in origins.values()
    }
    for module_name, module in tuple(sys.modules.items()):
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None)
        if not isinstance(origin, str) or origin in {"built-in", "frozen"}:
            continue
        absolute_origin = Path(os.path.abspath(origin))
        try:
            absolute_origin.relative_to(root)
        except ValueError:
            continue
        is_package = expected_by_origin.get(absolute_origin)
        if is_package is None:
            continue
        _validate_loaded_module_origin(
            module_name,
            module,
            expected_origin=absolute_origin,
            is_package=is_package,
        )


def _same_directory_identity(
    left: os.stat_result,
    right: os.stat_result,
) -> bool:
    return (
        stat.S_ISDIR(left.st_mode)
        and stat.S_ISDIR(right.st_mode)
        and (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)
    )


class _PinnedRuntimeTree:
    """Pin every protected parent directory across the hash/import barrier."""

    _whoscored_pinned_runtime_tree = True

    def __init__(
        self,
        *,
        root_descriptor: int,
        root: Path,
        relatives: tuple[str, ...],
        process_started_ns: int,
    ) -> None:
        self._root_descriptor = os.dup(root_descriptor)
        self._root = root
        self._process_started_ns = process_started_ns
        self._directories: dict[tuple[str, ...], int] = {}
        try:
            self._assert_live_root()
            parents = {
                tuple(Path(relative).parts[:depth])
                for relative in relatives
                for depth in range(1, len(Path(relative).parts))
            }
            for parts in sorted(parents, key=lambda value: (len(value), value)):
                self._directories[parts] = _open_relative_directory(
                    self._root_descriptor,
                    self._root,
                    parts,
                    process_started_ns=self._process_started_ns,
                    enforce_directory_immutability=True,
                )
        except Exception:
            for descriptor in self._directories.values():
                os.close(descriptor)
            os.close(self._root_descriptor)
            raise

    def _assert_live_root(self) -> None:
        pinned = os.fstat(self._root_descriptor)
        _assert_immutable_runtime_directory(
            pinned,
            display_path=self._root,
            process_started_ns=self._process_started_ns,
            enforce=True,
        )
        try:
            live = os.stat(self._root, follow_symlinks=False)
        except OSError as exc:
            raise RuntimeContractError(
                f"cannot re-open live WhoScored runtime root {self._root}: {exc}"
            ) from exc
        if not _same_directory_identity(pinned, live):
            raise RuntimeContractError(
                "WhoScored runtime root path changed after attestation: "
                f"{self._root}"
            )

    def assert_live(self, relative: str) -> None:
        parts = Path(relative).parts
        if (
            not parts
            or Path(relative).is_absolute()
            or "." in parts
            or ".." in parts
        ):
            raise RuntimeContractError(f"invalid relative runtime path: {relative!r}")
        self._assert_live_root()
        for depth in range(1, len(parts)):
            prefix = tuple(parts[:depth])
            pinned_descriptor = self._directories.get(prefix)
            if pinned_descriptor is None:
                raise RuntimeContractError(
                    "WhoScored runtime guard did not pin protected directory: "
                    f"{self._root.joinpath(*prefix)}"
                )
            current_descriptor = _open_relative_directory(
                self._root_descriptor,
                self._root,
                prefix,
                process_started_ns=self._process_started_ns,
                enforce_directory_immutability=True,
            )
            try:
                if not _same_directory_identity(
                    os.fstat(pinned_descriptor),
                    os.fstat(current_descriptor),
                ):
                    raise RuntimeContractError(
                        "WhoScored runtime subtree changed after attestation: "
                        f"{self._root.joinpath(*prefix)}"
                    )
            finally:
                os.close(current_descriptor)

    def read(self, relative: str) -> bytes:
        self.assert_live(relative)
        return _read_stable_relative_file(
            self._root_descriptor,
            self._root,
            relative,
            process_started_ns=self._process_started_ns,
            enforce_directory_immutability=True,
        )


class _FdAttestedSourceLoader(_SOURCE_LOADER):
    """Compile only stable fd-read bytes matching the release manifest."""

    _whoscored_fd_attested = True

    def __init__(
        self,
        fullname: str,
        path: str,
        *,
        tree: _PinnedRuntimeTree,
        relative: str,
        expected_sha256: str,
    ) -> None:
        super().__init__(fullname, path)
        self._tree = tree
        self._relative = relative
        self._expected_sha256 = expected_sha256

    def get_code(self, fullname: str) -> Any:
        if fullname != self.name:
            raise ImportError(f"unexpected WhoScored module name {fullname!r}")
        source = self._tree.read(self._relative)
        actual_sha256 = hashlib.sha256(source).hexdigest()
        if actual_sha256 != self._expected_sha256:
            raise ImportError(
                "WhoScored source changed before import: "
                f"module={fullname!r}, expected={self._expected_sha256}, "
                f"actual={actual_sha256}"
            )
        return compile(source, self.path, "exec", dont_inherit=True)


class _FdAttestedRuntimeFinder:
    """Resolve every attested application import independently of sys.path."""

    def __init__(
        self,
        *,
        tree: _PinnedRuntimeTree,
        root: Path,
        modules: Mapping[str, tuple[str, bool, str]],
    ) -> None:
        self._tree = tree
        self._root = root
        self._modules = dict(modules)

    def find_spec(
        self,
        fullname: str,
        path: Any = None,
        target: Any = None,
    ) -> Any:
        del path, target
        item = self._modules.get(fullname)
        if item is None:
            return None
        relative, is_package, expected_sha256 = item
        source_path = self._root / relative
        loader = _FdAttestedSourceLoader(
            fullname,
            str(source_path),
            tree=self._tree,
            relative=relative,
            expected_sha256=expected_sha256,
        )
        spec = importlib.machinery.ModuleSpec(
            fullname,
            loader,
            origin=str(source_path),
            is_package=is_package,
        )
        spec.has_location = True
        if is_package:
            spec.submodule_search_locations = [str(source_path.parent)]
        return spec


def _protected_import_alternative(
    filename: Path,
    expected_sources: frozenset[Path],
) -> bool:
    for expected in expected_sources:
        if filename == expected:
            return False
        without_suffix = expected.with_suffix("")
        try:
            filename.relative_to(without_suffix)
        except ValueError:
            pass
        else:
            return True
        if filename.parent == expected.parent and filename.name.startswith(
            expected.stem + "."
        ):
            return True
    return False


def _install_runtime_import_guard(
    root: Path,
    *,
    root_descriptor: int,
    files: Mapping[str, str],
    process_started_ns: Optional[int],
    enforce: bool,
    static_files: Optional[Mapping[str, bytes]] = None,
) -> None:
    """Install fd/hash enforcement before the first application import."""

    if not enforce or process_started_ns is None:
        return
    origins = _expected_runtime_module_origins(root)
    modules: dict[str, tuple[str, bool, str]] = {}
    for module_name, (origin, is_package) in origins.items():
        relative = origin.relative_to(root).as_posix()
        modules[module_name] = (relative, is_package, files[relative])
    identity = _canonical_sha256(
        {
            "files": dict(sorted(files.items())),
            "modules": {
                name: [relative, is_package, expected_sha256]
                for name, (relative, is_package, expected_sha256) in sorted(
                    modules.items()
                )
            },
        }
    )
    existing = getattr(sys, "_whoscored_runtime_import_guard", None)
    if existing is not None:
        if existing.get("root") != str(root) or existing.get("identity") != identity:
            raise RuntimeContractError(
                "another WhoScored runtime import guard is already active"
            )
        tree = existing.get("tree")
        if getattr(tree, "_whoscored_pinned_runtime_tree", False) is not True:
            raise RuntimeContractError("invalid WhoScored runtime import guard state")
        for relative in _ATTESTED_STATIC_RUNTIME_FILES:
            tree.assert_live(relative)
        return
    protected_relatives = tuple(
        sorted(
            {
                *(relative for relative, _package, _sha256 in modules.values()),
                *_ATTESTED_STATIC_RUNTIME_FILES,
            }
        )
    )
    tree = _PinnedRuntimeTree(
        root_descriptor=root_descriptor,
        root=root,
        relatives=protected_relatives,
        process_started_ns=process_started_ns,
    )
    cached_static_files: dict[str, bytes] = {}
    supplied_static_files = dict(static_files or {})
    for relative in sorted(_ATTESTED_STATIC_RUNTIME_FILES):
        payload = supplied_static_files.get(relative)
        if payload is None:
            payload = tree.read(relative)
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != files[relative]:
            raise RuntimeContractError(
                "WhoScored static runtime input changed before pinning: "
                f"file={relative}, expected={files[relative]}, "
                f"actual={actual_sha256}"
            )
        cached_static_files[relative] = payload
    finder = _FdAttestedRuntimeFinder(
        tree=tree,
        root=root,
        modules=modules,
    )
    sys.meta_path.insert(0, finder)
    expected_hashes = {
        root / relative: expected_sha256
        for relative, _is_package, expected_sha256 in modules.values()
    }
    expected_sources = frozenset(expected_hashes)

    def audit_imported_source(event: str, arguments: tuple[Any, ...]) -> None:
        if event != "compile" or len(arguments) < 2:
            return
        source, raw_filename = arguments[:2]
        if not isinstance(raw_filename, str):
            return
        filename = Path(os.path.abspath(raw_filename))
        expected_sha256 = expected_hashes.get(filename)
        if expected_sha256 is None:
            if _protected_import_alternative(filename, expected_sources):
                raise RuntimeContractError(
                    f"unattested WhoScored import source is blocked: {filename}"
                )
            return
        if isinstance(source, str):
            payload = source.encode("utf-8")
        elif isinstance(source, (bytes, bytearray)):
            payload = bytes(source)
        else:
            raise RuntimeContractError(
                f"WhoScored source must compile from attested bytes: {filename}"
            )
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != expected_sha256:
            raise RuntimeContractError(
                "WhoScored source bytes changed before compilation: "
                f"file={filename}, expected={expected_sha256}, "
                f"actual={actual_sha256}"
            )

    sys.addaudithook(audit_imported_source)
    sys._whoscored_runtime_import_guard = {
        "root": str(root),
        "identity": identity,
        "finder": finder,
        "tree": tree,
        "static_files": cached_static_files,
        "file_hashes": dict(files),
        "guarded_relatives": frozenset(protected_relatives),
    }


def attested_runtime_file_sha256(
    relative: str,
    *,
    runtime_root: Optional[Path] = None,
) -> str:
    """Return one manifest hash only after the production barrier succeeded."""

    if relative not in _EXPECTED_RUNTIME_FILE_SET:
        raise RuntimeContractError(
            f"WhoScored runtime file is not attested: {relative!r}"
        )
    root = Path(
        os.path.abspath(runtime_root or Path(__file__).absolute().parents[2])
    )
    guard = getattr(sys, "_whoscored_runtime_import_guard", None)
    if guard is None or guard.get("root") != str(root):
        raise RuntimeContractError(
            "WhoScored runtime hash requested before production attestation"
        )
    file_hashes = guard.get("file_hashes")
    tree = guard.get("tree")
    guarded_relatives = guard.get("guarded_relatives")
    if (
        not isinstance(file_hashes, dict)
        or getattr(tree, "_whoscored_pinned_runtime_tree", False) is not True
        or not isinstance(guarded_relatives, frozenset)
    ):
        raise RuntimeContractError("invalid WhoScored runtime import guard state")
    expected_sha256 = file_hashes.get(relative)
    if not isinstance(expected_sha256, str) or _SHA256_RE.fullmatch(expected_sha256) is None:
        raise RuntimeContractError(
            f"WhoScored runtime hash was not cached: {relative!r}"
        )
    if relative in guarded_relatives:
        tree.assert_live(relative)
    return expected_sha256


def read_attested_static_runtime_file(
    relative: str,
    *,
    runtime_root: Optional[Path] = None,
) -> bytes:
    """Return the exact static bytes captured by the production hash barrier."""

    if relative not in _ATTESTED_STATIC_RUNTIME_FILES:
        raise RuntimeContractError(
            f"WhoScored static runtime input is not attested: {relative!r}"
        )
    root = Path(
        os.path.abspath(runtime_root or Path(__file__).absolute().parents[2])
    )
    guard = getattr(sys, "_whoscored_runtime_import_guard", None)
    if guard is None:
        if root == Path("/opt/airflow"):
            raise RuntimeContractError(
                "WhoScored production static input requested before attestation"
            )
        return _read_stable_regular_file(_runtime_candidate(root, relative))
    if guard.get("root") != str(root):
        raise RuntimeContractError(
            "WhoScored static input requested from a different runtime root"
        )
    tree = guard.get("tree")
    static_files = guard.get("static_files")
    if (
        getattr(tree, "_whoscored_pinned_runtime_tree", False) is not True
        or not isinstance(static_files, dict)
    ):
        raise RuntimeContractError("invalid WhoScored runtime import guard state")
    tree.assert_live(relative)
    payload = static_files.get(relative)
    if not isinstance(payload, bytes):
        raise RuntimeContractError(
            f"WhoScored static runtime input was not cached: {relative!r}"
        )
    return payload


def validate_runtime_import_boundary(*, runtime_root: Path) -> None:
    """Reject Python import shadows before any mutable application import."""

    root = Path(os.path.abspath(runtime_root))
    descriptor = _open_runtime_root(root)
    try:
        # Opening every canonical package component fd-relative establishes the
        # same no-symlink directory boundary used by the full hash attestation.
        for parts in (
            ("dags",),
            ("dags", "utils"),
            ("scrapers",),
            ("scrapers", "whoscored"),
            ("scripts",),
        ):
            child = _open_relative_directory(
                descriptor,
                root,
                parts,
                process_started_ns=None,
                enforce_directory_immutability=False,
            )
            os.close(child)
        origins = _expected_runtime_module_origins(root)
        _validate_top_level_import_resolution(root, _effective_import_paths())
        _validate_canonical_import_artifacts(root, origins)
        _validate_loaded_runtime_modules(root, origins)
    finally:
        os.close(descriptor)


def _validate_python_bytecode_boundary(
    root: Path,
    *,
    root_descriptor: int,
    process_started_ns: Optional[int],
    enforce: bool,
) -> None:
    """Ensure production imports can only compile the attested source bytes."""

    if not enforce:
        return
    expected_prefix = str(_DISABLED_PYCACHE_PREFIX)
    if sys.pycache_prefix != expected_prefix or not sys.dont_write_bytecode:
        raise RuntimeContractError(
            "WhoScored production Python must start with "
            f"PYTHONPYCACHEPREFIX={expected_prefix!r} and "
            "PYTHONDONTWRITEBYTECODE=1"
        )
    if os.path.lexists(_DISABLED_PYCACHE_PREFIX):
        raise RuntimeContractError(
            "WhoScored disabled bytecode-cache prefix unexpectedly exists: "
            f"{_DISABLED_PYCACHE_PREFIX}"
        )

    parents = sorted(
        {
            Path(relative).parent.parts
            for relative in EXPECTED_RUNTIME_FILES
            if relative.endswith(".py")
            and not relative.startswith("docker/images/airflow/")
        }
    )
    for parts in parents:
        descriptor = _open_relative_directory(
            root_descriptor,
            root,
            tuple(parts),
            process_started_ns=process_started_ns,
            enforce_directory_immutability=True,
        )
        try:
            forbidden = sorted(
                name
                for name in os.listdir(descriptor)
                if name == "__pycache__" or name.endswith((".pyc", ".pyo"))
            )
        except OSError as exc:
            raise RuntimeContractError(
                f"cannot inspect runtime bytecode boundary {root.joinpath(*parts)}: {exc}"
            ) from exc
        finally:
            os.close(descriptor)
        if forbidden:
            raise RuntimeContractError(
                "WhoScored production source tree contains executable bytecode: "
                f"directory={root.joinpath(*parts)}, entries={forbidden}"
            )


def validate_runtime_contract(
    *,
    contract_path: Optional[Path] = None,
    runtime_root: Optional[Path] = None,
    report_schema_version: Optional[int] = None,
) -> dict[str, Any]:
    """Validate file identity, parser/report schemas, and writer compatibility."""

    default_runtime = contract_path is None and runtime_root is None
    process_started_ns = _process_start_time_ns() if default_runtime else None
    root = Path(
        os.path.abspath(runtime_root or Path(__file__).absolute().parents[2])
    )
    path = Path(os.path.abspath(contract_path or RUNTIME_CONTRACT_PATH))
    enforce_directory_immutability = default_runtime and root == Path("/opt/airflow")
    root_descriptor = _open_runtime_root(root)
    try:
        try:
            contract_relative = path.relative_to(root).as_posix()
        except ValueError:
            contract_relative = None
        contract = _load_contract(
            path,
            process_started_ns=process_started_ns,
            root_descriptor=root_descriptor if contract_relative is not None else None,
            root=root if contract_relative is not None else None,
            relative=contract_relative,
            enforce_directory_immutability=enforce_directory_immutability,
            expected_sha256=(
                _IMAGE_TRUSTED_RUNTIME_CONTRACT_SHA256
                if default_runtime
                else None
            ),
        )
    except Exception:
        os.close(root_descriptor)
        raise
    expected_keys = {
        "schema_version",
        "parser_version",
        "report_schema_version",
        "business_dataset_count",
        "files",
    }
    if set(contract) != expected_keys or contract.get("schema_version") != 1:
        os.close(root_descriptor)
        raise RuntimeContractError("invalid WhoScored runtime contract schema")
    files = contract.get("files")
    if not isinstance(files, dict) or not files:
        os.close(root_descriptor)
        raise RuntimeContractError("WhoScored runtime contract files must be non-empty")
    try:
        _validate_declared_file_set(files)
        if default_runtime:
            validate_runtime_import_boundary(runtime_root=root)
        _validate_python_bytecode_boundary(
            root,
            root_descriptor=root_descriptor,
            process_started_ns=process_started_ns,
            enforce=enforce_directory_immutability,
        )
        _validate_core_package_file_set(
            root,
            root_descriptor=root_descriptor,
            process_started_ns=process_started_ns,
            enforce_directory_immutability=enforce_directory_immutability,
        )
        actual_hashes: dict[str, str] = {}
        attested_static_files: dict[str, bytes] = {}
        for relative, expected_hash in sorted(files.items()):
            candidate = _runtime_candidate(root, relative)
            candidate_relative = candidate.relative_to(root).as_posix()
            if relative in _ATTESTED_STATIC_RUNTIME_FILES:
                payload = _read_stable_relative_file(
                    root_descriptor,
                    root,
                    candidate_relative,
                    process_started_ns=process_started_ns,
                    enforce_directory_immutability=enforce_directory_immutability,
                )
                actual_hash = hashlib.sha256(payload).hexdigest()
                attested_static_files[relative] = payload
            else:
                actual_hash = _sha256_relative(
                    root_descriptor,
                    root,
                    candidate_relative,
                    process_started_ns=process_started_ns,
                    enforce_directory_immutability=enforce_directory_immutability,
                )
            if actual_hash != expected_hash:
                raise RuntimeContractError(
                    "WhoScored runtime file hash mismatch: "
                    f"file={relative}, expected={expected_hash}, actual={actual_hash}"
                )
            actual_hashes[relative] = actual_hash
        _install_runtime_import_guard(
            root,
            root_descriptor=root_descriptor,
            files=files,
            process_started_ns=process_started_ns,
            enforce=enforce_directory_immutability,
            static_files=attested_static_files,
        )
    finally:
        os.close(root_descriptor)

    # Do not import any other mutable application module until the complete
    # declared closure has matched the lock. This keeps a stale/mixed bind
    # mount from executing parser, writer, repository or runner import hooks
    # before identity validation fails closed.
    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.whoscored.parsers import PARSER_VERSION
    from scrapers.whoscored.repository import WHOSCORED_BUSINESS_TABLES

    if report_schema_version is None:
        from dags.scripts.run_whoscored_scraper import REPORT_SCHEMA_VERSION

        actual_report_schema_version = REPORT_SCHEMA_VERSION
    else:
        actual_report_schema_version = int(report_schema_version)

    if contract.get("parser_version") != PARSER_VERSION:
        raise RuntimeContractError(
            "WhoScored parser version mismatch: "
            f"expected={contract.get('parser_version')!r}, actual={PARSER_VERSION!r}"
        )
    if contract.get("report_schema_version") != actual_report_schema_version:
        raise RuntimeContractError(
            "WhoScored report schema version mismatch: "
            f"expected={contract.get('report_schema_version')!r}, "
            f"actual={actual_report_schema_version!r}"
        )
    if contract.get("business_dataset_count") != len(WHOSCORED_BUSINESS_TABLES):
        raise RuntimeContractError(
            "WhoScored business schema mismatch: "
            f"expected={contract.get('business_dataset_count')!r}, "
            f"actual={len(WHOSCORED_BUSINESS_TABLES)!r}"
        )

    signature = inspect.signature(IcebergWriter.write_dataframe)
    bulk_arrow = signature.parameters.get("bulk_arrow")
    if bulk_arrow is None or bulk_arrow.default is not False:
        raise RuntimeContractError(
            "IcebergWriter.write_dataframe must expose bulk_arrow=False"
        )

    # Imports performed after the hash barrier must still be canonical source
    # modules. This catches a loaded-module cache swap or a custom import path
    # mutation between the pre-import boundary and application imports.
    if default_runtime:
        validate_runtime_import_boundary(runtime_root=root)

    identity = _canonical_sha256(actual_hashes)
    manifest_sha256 = _canonical_sha256(contract)
    return {
        "status": "success",
        "parser_version": PARSER_VERSION,
        "report_schema_version": actual_report_schema_version,
        "business_dataset_count": len(WHOSCORED_BUSINESS_TABLES),
        "file_count": len(actual_hashes),
        "code_tree_sha256": identity,
        "manifest_sha256": manifest_sha256,
    }


def _airflow_pool_slots(pool_name: str) -> int:
    """Read one persisted Airflow pool size without relying on CLI output."""

    try:
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        with create_session() as session:
            pool = session.query(Pool).filter(Pool.pool == pool_name).one_or_none()
    except Exception as exc:
        raise RuntimeContractError(
            f"cannot read Airflow source pool {pool_name!r}: {exc}"
        ) from exc
    if pool is None:
        raise RuntimeContractError(f"Airflow source pool {pool_name!r} does not exist")
    return int(pool.slots)


def validate_airflow_source_pool(
    *,
    direct_pool: str,
    backfill_pool: str,
) -> dict[str, Any]:
    """Prove daily and backfill share the modeled physical source pool."""

    from scrapers.whoscored.runtime_limits import source_pool_slots

    if not direct_pool or direct_pool != backfill_pool:
        raise RuntimeContractError(
            "WhoScored daily and backfill must share one Airflow source pool"
        )
    try:
        expected_slots = source_pool_slots()
    except ValueError as exc:
        raise RuntimeContractError(str(exc)) from exc
    actual_slots = _airflow_pool_slots(direct_pool)
    if actual_slots != expected_slots:
        raise RuntimeContractError(
            "WhoScored Airflow source pool size mismatch: "
            f"pool={direct_pool!r}, expected={expected_slots}, actual={actual_slots}"
        )
    return {
        "pool": direct_pool,
        "expected_slots": expected_slots,
        "actual_slots": actual_slots,
    }
