"""
Unit tests for ``configs/openmetadata/apply_descriptions.py`` + the
``configs/openmetadata/descriptions/*.yaml`` corpus (E7 / T5).

These tests run on the host without OpenMetadata (no HTTP). They verify:
* every YAML file parses,
* every YAML carries the required surface (table.fullyQualifiedName,
  table.description, table.tags as list),
* ``apply_descriptions.py`` is importable as a module,
* ``main(["--dry-run", ...])`` renders JSON-Patch ops to stdout without
  contacting the API.
"""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DESCRIPTIONS_DIR = PROJECT_ROOT / "configs" / "openmetadata" / "descriptions"
APPLY_DIR = PROJECT_ROOT / "configs" / "openmetadata"


pytestmark = pytest.mark.unit


def test_all_yaml_descriptions_parse() -> None:
    """Every YAML in descriptions/ must yaml.safe_load without errors."""
    files = sorted(DESCRIPTIONS_DIR.glob("*.yaml"))
    assert len(files) >= 20, f"expected >=20 YAML files, found {len(files)}"
    failures: list[tuple[str, str]] = []
    for f in files:
        try:
            with f.open("r", encoding="utf-8") as fh:
                yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            failures.append((f.name, str(exc)))
    assert not failures, f"YAML parse failures: {failures}"


def test_apply_descriptions_imports() -> None:
    """`apply_descriptions` must be importable on the host (deps: yaml + requests)."""
    if str(APPLY_DIR) not in sys.path:
        sys.path.insert(0, str(APPLY_DIR))
    # Force-reimport to a fresh module so prior runs can't mask issues.
    sys.modules.pop("apply_descriptions", None)
    try:
        import apply_descriptions  # noqa: F401
    except ImportError as exc:  # pragma: no cover — defensive
        pytest.skip(f"apply_descriptions imports unavailable dep: {exc}")
    assert hasattr(apply_descriptions, "main"), "apply_descriptions.main missing"
    assert hasattr(apply_descriptions, "build_patch"), "build_patch helper missing"


def test_dry_run_renders_patches(monkeypatch) -> None:
    """`apply_descriptions.main(["--dry-run", ...])` returns 0 and prints JSON ops."""
    if str(APPLY_DIR) not in sys.path:
        sys.path.insert(0, str(APPLY_DIR))
    sys.modules.pop("apply_descriptions", None)
    try:
        import apply_descriptions
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"apply_descriptions imports unavailable dep: {exc}")

    # main() reads sys.argv via argparse — patch it instead of subprocessing.
    monkeypatch.setattr(
        sys, "argv",
        ["apply_descriptions.py", "--dry-run", "--host", "http://nonexistent"],
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = apply_descriptions.main()
    out = buf.getvalue()

    assert rc == 0, f"--dry-run should exit 0, got {rc}. stdout:\n{out}"
    assert '"op":' in out, (
        "Expected at least one rendered JSON-Patch op in dry-run stdout. "
        f"Got first 500 chars:\n{out[:500]}"
    )


def test_required_fields() -> None:
    """Each YAML must define table.fullyQualifiedName + description + tags(list)."""
    files = sorted(DESCRIPTIONS_DIR.glob("*.yaml"))
    failures: list[str] = []
    for f in files:
        with f.open("r", encoding="utf-8") as fh:
            spec = yaml.safe_load(fh) or {}
        table = spec.get("table") or {}
        if not table.get("fullyQualifiedName"):
            failures.append(f"{f.name}: missing table.fullyQualifiedName")
            continue
        if not table.get("description"):
            failures.append(f"{f.name}: missing table.description")
        tags = table.get("tags")
        if not isinstance(tags, list) or not tags:
            failures.append(f"{f.name}: table.tags must be a non-empty list")
    assert not failures, "\n".join(failures)
