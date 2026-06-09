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


def _import_apply():
    if str(APPLY_DIR) not in sys.path:
        sys.path.insert(0, str(APPLY_DIR))
    sys.modules.pop("apply_descriptions", None)
    try:
        import apply_descriptions
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"apply_descriptions imports unavailable dep: {exc}")
    return apply_descriptions


def test_build_patch_emits_column_tags() -> None:
    """build_patch must render a /columns/{idx}/tags op for YAML column tags (#351)."""
    mod = _import_apply()
    spec = {
        "table": {"fullyQualifiedName": "x", "description": "d", "tags": ["Tier.Bronze"]},
        "columns": [{"name": "player", "description": "Имя игрока.", "tags": ["PII.Low"]}],
    }
    current = {"description": "", "tags": [], "columns": [{"name": "player", "description": "", "tags": []}]}

    ops = mod.build_patch(spec, current)
    tag_ops = [o for o in ops if o["path"] == "/columns/0/tags"]
    assert len(tag_ops) == 1, f"expected one column-tags op, got ops={ops}"
    assert tag_ops[0]["value"] == [
        {"tagFQN": "PII.Low", "labelType": "Manual", "state": "Confirmed", "source": "Classification"}
    ]


def test_build_patch_column_tags_idempotent() -> None:
    """No column-tags op when the desired tag is already present (#351)."""
    mod = _import_apply()
    spec = {
        "table": {"fullyQualifiedName": "x", "description": "d", "tags": ["Tier.Bronze"]},
        "columns": [{"name": "player", "tags": ["PII.Low"]}],
    }
    current = {
        "description": "d", "tags": [{"tagFQN": "Tier.Bronze"}],
        "columns": [{"name": "player", "description": "", "tags": [{"tagFQN": "PII.Low"}]}],
    }

    ops = mod.build_patch(spec, current)
    assert not [o for o in ops if o["path"] == "/columns/0/tags"], f"should be idempotent, got {ops}"


def test_table_fqn_strips_column() -> None:
    """_table_fqn drops a trailing column component, keeps a bare table FQN (#406)."""
    mod = _import_apply()
    col_fqn = "trino_iceberg.iceberg.gold.dim_team.team_id"
    tbl_fqn = "trino_iceberg.iceberg.gold.dim_team"
    assert mod._table_fqn(col_fqn) == tbl_fqn
    assert mod._table_fqn(tbl_fqn) == tbl_fqn


def test_resolve_table_id_caches(monkeypatch) -> None:
    """resolve_table_id hits the API once per table; column+table FQN share a key (#406)."""
    mod = _import_apply()
    calls: list[str] = []

    class _Resp:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"id": "uuid-dim-team"}

    def fake_get(url, headers=None, timeout=None, params=None):  # noqa: ARG001
        calls.append(url)
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", fake_get)
    cache: dict = {}
    a = mod.resolve_table_id("http://om", {}, "trino_iceberg.iceberg.gold.dim_team.team_id", cache)
    b = mod.resolve_table_id("http://om", {}, "trino_iceberg.iceberg.gold.dim_team", cache)
    assert a == b == "uuid-dim-team"
    assert len(calls) == 1, f"expected one GET (cached), got {calls}"


def test_resolve_table_id_negative_cache(monkeypatch) -> None:
    """404 → None, cached: a second lookup does not re-hit the API (#406)."""
    mod = _import_apply()
    calls: list[str] = []

    class _Resp404:
        status_code = 404

        @staticmethod
        def json() -> dict:
            return {}

    def fake_get(url, headers=None, timeout=None, params=None):  # noqa: ARG001
        calls.append(url)
        return _Resp404()

    monkeypatch.setattr(mod.requests, "get", fake_get)
    cache: dict = {}
    fqn = "trino_iceberg.iceberg.gold.not_ingested"
    assert mod.resolve_table_id("http://om", {}, fqn, cache) is None
    assert mod.resolve_table_id("http://om", {}, fqn, cache) is None
    assert len(calls) == 1, f"404 must be cached, got {calls}"


def test_apply_lineage_puts_resolved_uuid_edge(monkeypatch) -> None:
    """apply_lineage resolves FQN→UUID and PUTs parent→self edge (#406)."""
    mod = _import_apply()
    ids = {
        "trino_iceberg.iceberg.silver.fbref_match_enriched": "uuid-parent",
        "trino_iceberg.iceberg.silver.fbref_match_events": "uuid-self",
    }
    put_bodies: list[dict] = []

    class _Get:
        def __init__(self, tid):
            self.status_code = 200
            self._tid = tid

        def json(self):
            return {"id": self._tid}

    def fake_get(url, headers=None, timeout=None, params=None):  # noqa: ARG001
        fqn = url.rsplit("/name/", 1)[-1]
        return _Get(ids.get(fqn))

    class _Put:
        status_code = 200
        text = ""

    def fake_put(url, headers=None, json=None, timeout=None):  # noqa: ARG001, A002
        put_bodies.append(json)
        return _Put()

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.requests, "put", fake_put)

    rels = [{
        "from": "match_id",
        "to": "trino_iceberg.iceberg.silver.fbref_match_enriched.match_id",
        "type": "FOREIGN_KEY",
        "description": "N:1",
    }]
    counter: dict = {}
    mod.apply_lineage(
        "http://om", {}, rels,
        "trino_iceberg.iceberg.silver.fbref_match_events",
        dry_run=False, counter=counter,
        from_id="uuid-self", fqn_cache={},
    )

    assert counter.get("lineage_ok") == 1, counter
    assert len(put_bodies) == 1
    edge = put_bodies[0]["edge"]
    # Direction: referenced parent is upstream (fromEntity), self is downstream (toEntity).
    assert edge["fromEntity"]["id"] == "uuid-parent"
    assert edge["toEntity"]["id"] == "uuid-self"
    # No raw FQN leaked into the id fields.
    assert "iceberg" not in edge["fromEntity"]["id"]
    assert "iceberg" not in edge["toEntity"]["id"]


def test_apply_lineage_dry_run_no_http(monkeypatch) -> None:
    """dry-run renders intent without calling the API (#406)."""
    mod = _import_apply()

    def boom(*a, **k):  # noqa: ARG001
        raise AssertionError("dry-run must not hit HTTP")

    monkeypatch.setattr(mod.requests, "get", boom)
    monkeypatch.setattr(mod.requests, "put", boom)

    rels = [{"to": "trino_iceberg.iceberg.gold.dim_team.team_id", "type": "FOREIGN_KEY"}]
    counter: dict = {}
    mod.apply_lineage(
        "http://om", {}, rels, "trino_iceberg.iceberg.gold.fct_player_match",
        dry_run=True, counter=counter, fqn_cache={},
    )
    assert counter.get("lineage_dry") == 1, counter


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
