"""Deterministic provenance for the paid SofaScore runtime.

The paid canary proves a byte cap only for the exact capture/filter runtime it
measured.  This module hashes logical relative paths plus file contents; it
never includes mtimes, absolute checkout paths, generated evidence or the
artifact itself.  The same tree therefore has the same digest in a worktree,
CI and the Airflow container.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import os
import re
from pathlib import Path
from typing import Mapping, Sequence


RUNTIME_FINGERPRINT_VERSION = 1
RUNTIME_FINGERPRINT_ALGORITHM = "sha256-relative-path-content-v1"

# Keep this allowlist explicit.  A broad glob can accidentally hash caches,
# artifacts or fixtures and make evidence non-reproducible.  Directories are
# expanded deterministically to tracked runtime source suffixes below.
RUNTIME_FILE_ENTRIES = (
    "scrapers/sofascore",
    "scripts/proxy_filter",
    "dags/dag_ingest_sofascore.py",
    "dags/dag_canary_sofascore_proxy.py",
    "dags/scripts/prepare_sofascore_workload.py",
    "dags/scripts/run_sofascore_scraper.py",
    "dags/utils/medallion_config.py",
    "dags/utils/sofascore_dq.py",
    "scripts/backfill_sofascore.py",
    "scripts/research/bench_sofascore_paid_canary.py",
    "scrapers/utils/rate_limiter.py",
    "configs/proxy_filter/blocklist.txt",
    "configs/sofascore/endpoint_coverage.yaml",
    "configs/medallion/competitions.yaml",
    "docker/images/airflow/Dockerfile",
    "docker/images/airflow/requirements-airflow.txt",
    "docker/images/airflow/requirements-scraping.txt",
    "docker/images/airflow/requirements.txt",
)
_RUNTIME_SOURCE_SUFFIXES = frozenset({".py"})
_BROWSER_PACKAGES = ("camoufox", "playwright")
_PIN_RE = re.compile(
    r"^\s*([A-Za-z0-9_.-]+)(?:\[[^\]]+\])?==([^;\s]+)(?:\s*;.*)?$"
)


class RuntimeFingerprintError(RuntimeError):
    """The local runtime cannot reproduce the measured canary runtime."""


def default_runtime_root() -> Path:
    """Return the checkout/container root without consulting cwd."""

    return Path(__file__).resolve().parents[2]


def _contract_path(root: Path, relative: str) -> Path:
    """Resolve build inputs copied into the immutable Airflow image.

    Source code/configs are bind-mounted at ``/opt/airflow`` in production.
    Dockerfile and requirement inputs are baked under ``runtime-contract`` so
    a stale image cannot silently borrow newer host-side dependency files.
    """

    direct = root / relative
    if direct.is_file() or direct.is_dir():
        return direct
    baked = root / "runtime-contract" / relative
    if baked.is_file() or baked.is_dir():
        return baked
    raise RuntimeFingerprintError(f"runtime input is missing: {relative}")


def runtime_files(
    root: os.PathLike[str] | str | None = None,
    *,
    entries: Sequence[str] = RUNTIME_FILE_ENTRIES,
) -> tuple[str, ...]:
    """Expand the explicit runtime allowlist into sorted logical paths."""

    base = Path(root) if root is not None else default_runtime_root()
    logical: set[str] = set()
    for raw_entry in entries:
        entry = str(raw_entry).strip().replace("\\", "/")
        if not entry or entry.startswith("/") or ".." in Path(entry).parts:
            raise RuntimeFingerprintError(f"unsafe runtime input: {raw_entry!r}")
        resolved = _contract_path(base, entry)
        if resolved.is_file():
            logical.add(entry)
            continue
        for candidate in resolved.rglob("*"):
            if candidate.is_file() and candidate.suffix in _RUNTIME_SOURCE_SUFFIXES:
                logical.add(
                    str(Path(entry) / candidate.relative_to(resolved)).replace(
                        "\\", "/"
                    )
                )
    if not logical:
        raise RuntimeFingerprintError("runtime fingerprint has no inputs")
    return tuple(sorted(logical))


def _read_logical_file(root: Path, relative: str) -> bytes:
    candidate = root / relative
    if not candidate.is_file():
        candidate = root / "runtime-contract" / relative
    try:
        return candidate.read_bytes()
    except OSError as exc:
        raise RuntimeFingerprintError(
            f"runtime input is unreadable: {relative}"
        ) from exc


def _browser_runtime_pins(root: Path) -> dict[str, str]:
    relative = "docker/images/airflow/requirements-scraping.txt"
    raw = _read_logical_file(root, relative).decode("utf-8")
    pins: dict[str, str] = {}
    for line in raw.splitlines():
        match = _PIN_RE.match(line)
        if match:
            pins[match.group(1).lower().replace("_", "-")] = match.group(2)
    missing = [name for name in _BROWSER_PACKAGES if name not in pins]
    if missing:
        raise RuntimeFingerprintError(
            "browser runtime must be exactly pinned: " + ", ".join(missing)
        )
    return {name: pins[name] for name in _BROWSER_PACKAGES}


def runtime_fingerprint(
    root: os.PathLike[str] | str | None = None,
    *,
    entries: Sequence[str] = RUNTIME_FILE_ENTRIES,
) -> dict[str, object]:
    """Hash the exact paid runtime using path+length+content framing."""

    base = Path(root) if root is not None else default_runtime_root()
    files = runtime_files(base, entries=entries)
    digest = hashlib.sha256()
    digest.update(f"sofascore-runtime-v{RUNTIME_FINGERPRINT_VERSION}\0".encode())
    for relative in files:
        data = _read_logical_file(base, relative)
        encoded_path = relative.encode("utf-8")
        digest.update(len(encoded_path).to_bytes(8, "big"))
        digest.update(encoded_path)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return {
        "version": RUNTIME_FINGERPRINT_VERSION,
        "algorithm": RUNTIME_FINGERPRINT_ALGORITHM,
        "digest": digest.hexdigest(),
        "files": list(files),
        "browser_runtime_pins": _browser_runtime_pins(base),
    }


def validate_runtime_fingerprint(
    value: object,
    *,
    root: os.PathLike[str] | str | None = None,
    entries: Sequence[str] = RUNTIME_FILE_ENTRIES,
    enforce_installed_browser: bool | None = None,
) -> Mapping[str, object]:
    """Require artifact provenance to match local code and browser packages."""

    if not isinstance(value, Mapping):
        raise RuntimeFingerprintError("runtime_fingerprint must be an object")
    expected = runtime_fingerprint(root, entries=entries)
    if dict(value) != expected:
        raise RuntimeFingerprintError(
            "canary runtime fingerprint does not match current runtime"
        )

    base = Path(root) if root is not None else default_runtime_root()
    if enforce_installed_browser is None:
        enforce_installed_browser = (
            str(base.resolve()) == "/opt/airflow"
            or os.environ.get("SOFASCORE_ENFORCE_RUNTIME_DEPENDENCIES") == "1"
        )
    expected_pins = expected["browser_runtime_pins"]
    assert isinstance(expected_pins, Mapping)
    installed: dict[str, str | None] = {}
    for package in _BROWSER_PACKAGES:
        try:
            installed[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            installed[package] = None
    present = {name: version for name, version in installed.items() if version}
    if enforce_installed_browser and len(present) != len(_BROWSER_PACKAGES):
        missing = sorted(set(_BROWSER_PACKAGES) - set(present))
        raise RuntimeFingerprintError(
            "installed browser runtime is incomplete: " + ", ".join(missing)
        )
    for package, installed_version in present.items():
        if installed_version != expected_pins[package]:
            raise RuntimeFingerprintError(
                f"installed {package}={installed_version} does not match "
                f"pinned {expected_pins[package]}"
            )
    return expected
