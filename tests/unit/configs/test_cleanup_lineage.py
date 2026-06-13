"""
Unit tests for ``configs/openmetadata/cleanup_lineage.py`` (issue #529).

These run on the host without OpenMetadata (no real HTTP). They verify:
* the module imports and exposes ``main`` + the curated FQN list,
* the curated list targets only ``gold`` tables and excludes the live
  ``entity_xref`` (its drop is the separate followup #146),
* dry-run (default) prints DELETE intent and makes NO HTTP calls,
* ``--apply`` hard-deletes with ``hardDelete=true&recursive=true`` (the flags
  that cascade lineage-edge removal),
* a table absent from the catalog (HTTP 404) is skipped, never DELETE'd,
* ``--apply`` without a JWT fails fast with exit code 2.
"""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEANUP_DIR = PROJECT_ROOT / "configs" / "openmetadata"


pytestmark = pytest.mark.unit


def _import_cleanup():
    if str(CLEANUP_DIR) not in sys.path:
        sys.path.insert(0, str(CLEANUP_DIR))
    sys.modules.pop("cleanup_lineage", None)
    try:
        import cleanup_lineage
    except ImportError as exc:  # pragma: no cover — defensive
        pytest.skip(f"cleanup_lineage imports unavailable dep: {exc}")
    return cleanup_lineage


def test_cleanup_lineage_imports() -> None:
    mod = _import_cleanup()
    assert hasattr(mod, "main"), "cleanup_lineage.main missing"
    assert hasattr(mod, "hard_delete"), "hard_delete helper missing"
    assert hasattr(mod, "CURATED_FQNS"), "CURATED_FQNS list missing"


def test_curated_list_shape() -> None:
    """19 #478 drops, all gold FQNs, entity_xref excluded (#146)."""
    mod = _import_cleanup()
    assert len(mod.CURATED_FQNS) == 19, mod.CURATED_FQNS
    assert all(f.startswith("trino_iceberg.iceberg.gold.") for f in mod.CURATED_FQNS), mod.CURATED_FQNS
    assert not any("entity_xref" in f for f in mod.CURATED_FQNS), "entity_xref is live (#146), must not be targeted"
    # spot-check a few known #478 drops
    for tbl in ("fct_match", "feat_team_form", "fotmob_team_season"):
        assert f"trino_iceberg.iceberg.gold.{tbl}" in mod.CURATED_FQNS


def test_dry_run_prints_delete_intent_no_http(monkeypatch) -> None:
    """Default run is dry: prints DELETE intent, exits 0, contacts no HTTP."""
    mod = _import_cleanup()

    def boom(*a, **k):  # noqa: ARG001
        raise AssertionError("dry-run must not hit HTTP")

    monkeypatch.setattr(mod.requests, "get", boom)
    monkeypatch.setattr(mod.requests, "delete", boom)
    monkeypatch.setattr(sys, "argv", ["cleanup_lineage.py", "--host", "http://nonexistent"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = mod.main()
    out = buf.getvalue()

    assert rc == 0, f"dry-run should exit 0, got {rc}. stdout:\n{out}"
    assert "[DRY] DELETE table" in out, f"expected dry DELETE intent, got:\n{out[:500]}"
    assert "hardDelete=true, recursive=true" in out


def test_hard_delete_apply_sends_recursive_hard_delete(monkeypatch) -> None:
    """--apply path resolves FQN→id then DELETEs with hardDelete+recursive."""
    mod = _import_cleanup()
    captured: dict = {}

    class _Get:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"id": "uuid-fct-match"}

    def fake_get(url, headers=None, params=None, timeout=None):  # noqa: ARG001
        return _Get()

    class _Del:
        status_code = 200
        text = ""

    def fake_delete(url, headers=None, params=None, timeout=None):  # noqa: ARG001
        captured["url"] = url
        captured["params"] = params
        return _Del()

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.requests, "delete", fake_delete)

    counter = {"deleted": 0, "absent": 0, "warn": 0, "dry": 0}
    mod.hard_delete("http://om", {}, "trino_iceberg.iceberg.gold.fct_match", dry_run=False, counter=counter)

    assert counter["deleted"] == 1, counter
    assert "uuid-fct-match" in captured["url"], captured
    assert captured["params"] == {"hardDelete": "true", "recursive": "true"}


def test_hard_delete_absent_table_is_skipped(monkeypatch) -> None:
    """404 on resolve → 'absent', and DELETE is never called (idempotent re-run)."""
    mod = _import_cleanup()

    class _Get404:
        status_code = 404

        @staticmethod
        def json() -> dict:
            return {}

    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: _Get404())

    def boom_delete(*a, **k):  # noqa: ARG001
        raise AssertionError("must not DELETE a table that is absent")

    monkeypatch.setattr(mod.requests, "delete", boom_delete)

    counter = {"deleted": 0, "absent": 0, "warn": 0, "dry": 0}
    mod.hard_delete("http://om", {}, "trino_iceberg.iceberg.gold.fct_match", dry_run=False, counter=counter)

    assert counter["absent"] == 1, counter
    assert counter["deleted"] == 0, counter


def test_apply_without_token_fails_fast(monkeypatch) -> None:
    """--apply with no JWT returns exit 2 before any HTTP."""
    mod = _import_cleanup()
    monkeypatch.delenv("OPENMETADATA_JWT_TOKEN", raising=False)
    monkeypatch.delenv("OM_JWT_TOKEN", raising=False)

    def boom(*a, **k):  # noqa: ARG001
        raise AssertionError("must not hit HTTP without a token")

    monkeypatch.setattr(mod.requests, "get", boom)
    monkeypatch.setattr(mod.requests, "delete", boom)
    monkeypatch.setattr(sys, "argv", ["cleanup_lineage.py", "--apply"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = mod.main()
    assert rc == 2, f"expected exit 2 without token, got {rc}"
