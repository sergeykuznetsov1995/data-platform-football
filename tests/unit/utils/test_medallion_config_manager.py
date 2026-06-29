"""
Unit tests for the manager-alias loader in ``dags/utils/medallion_config.py``
(issue #619). Mirrors ``test_medallion_config_referee.py``: mock YAML via
``MEDALLION_CONFIG_DIR``/tmp_path for loader mechanics, plus one pass over the
real shipped ``configs/medallion/manager_aliases.yaml`` so a production
regression (dup slug, bad canonical_id) trips immediately.

Manager-specific: canonical_id is the BARE FBref-spine slug (no ``mgr_``
prefix), and an empty/absent file degrades to a non-matching sentinel row
rather than raising (a missing optional alias file must not break the
pre-existing TM-coaches transform).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


pytestmark = pytest.mark.unit


_MOCK_MANAGER_ALIASES = """\
managers:
  - canonical_name: "Régis Le Bris"
    canonical_id: "regis_le_bris"
    aliases:
      transfermarkt: ["Le Bris, Régis"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Nuno Espírito Santo"
    canonical_id: "nuno_espirito_santo"
    aliases:
      _generic: ["Nuno"]
      transfermarkt: ["Espírito Santo, Nuno"]
    competition_scope: ["ENG-Premier League"]
"""


@pytest.fixture
def mock_manager_dir(tmp_path, monkeypatch):
    (tmp_path / "manager_aliases.yaml").write_text(
        _MOCK_MANAGER_ALIASES, encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    return mc


# ---------------------------------------------------------------------------
# Loader + schema validation
# ---------------------------------------------------------------------------

def test_load_manager_aliases_returns_dict(mock_manager_dir):
    doc = mock_manager_dir.load_manager_aliases()
    assert isinstance(doc, dict)
    assert len(doc["managers"]) == 2


def test_load_missing_file_returns_empty(tmp_path, monkeypatch):
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)  # no manager_aliases.yaml here
    mc.reset_cache()
    assert mc.load_manager_aliases() == {"managers": []}


def test_rejects_missing_top_level_key(tmp_path, monkeypatch):
    (tmp_path / "manager_aliases.yaml").write_text("foo: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="missing top-level 'managers'"):
        mc.load_manager_aliases()


def test_rejects_bad_canonical_id(tmp_path, monkeypatch):
    bad = (
        "managers:\n"
        '  - canonical_name: "X"\n'
        '    canonical_id: "Mgr-Bad!"\n'  # uppercase + illegal chars
        "    aliases:\n"
        '      transfermarkt: ["x"]\n'
    )
    (tmp_path / "manager_aliases.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="canonical_id"):
        mc.load_manager_aliases()


def test_rejects_duplicate_canonical_id(tmp_path, monkeypatch):
    dup = (
        "managers:\n"
        '  - canonical_name: "A"\n'
        '    canonical_id: "same_id"\n'
        "    aliases: {transfermarkt: [\"a\"]}\n"
        '  - canonical_name: "B"\n'
        '    canonical_id: "same_id"\n'
        "    aliases: {transfermarkt: [\"b\"]}\n"
    )
    (tmp_path / "manager_aliases.yaml").write_text(dup, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="duplicate canonical_id"):
        mc.load_manager_aliases()


# ---------------------------------------------------------------------------
# SQL VALUES emitter
# ---------------------------------------------------------------------------

def test_sql_values_three_tuple_shape(mock_manager_dir):
    sql = mock_manager_dir.get_manager_alias_sql_values(source="transfermarkt")
    # (raw_name, canonical_id, league) — 3 single-quoted fields per row.
    rows = [r for r in sql.split("\n") if r.strip().startswith("(")]
    assert rows, "expected at least one VALUES row"
    for r in rows:
        assert r.count("'") == 6, f"row must have 3 quoted fields: {r!r}"
    assert "regis_le_bris" in sql
    assert "ENG-Premier League" in sql


def test_sql_values_keeps_diacritics(mock_manager_dir):
    sql = mock_manager_dir.get_manager_alias_sql_values(source="transfermarkt")
    # Diacritics preserved verbatim (norm happens in SQL, not in the emitter).
    assert "Le Bris, Régis" in sql
    assert "Espírito Santo, Nuno" in sql


def test_empty_config_emits_sentinel_not_raise(tmp_path, monkeypatch):
    """A missing/empty manager_aliases.yaml must NOT raise (unlike referee) —
    it emits a guaranteed-non-matching sentinel so the coaches transform no-ops."""
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)  # no file
    mc.reset_cache()
    sql = mc.get_manager_alias_sql_values(source="transfermarkt")
    assert mc._NO_MANAGER_ALIAS_SENTINEL in sql
    assert "__none__" in sql  # league sentinel


def test_generic_bucket_merged_when_source_given(mock_manager_dir):
    sql = mock_manager_dir.get_manager_alias_sql_values(source="transfermarkt")
    # _generic forms are included alongside the transfermarkt bucket.
    assert "Nuno" in sql


# ---------------------------------------------------------------------------
# Real shipped config — schema regression guard
# ---------------------------------------------------------------------------

def test_real_manager_aliases_yaml_is_valid():
    import unittest.mock as _mock

    from utils import medallion_config as mc

    real_dir = PROJECT_ROOT / "configs" / "medallion"
    if not (real_dir / "manager_aliases.yaml").exists():
        pytest.skip("manager_aliases.yaml not shipped in this checkout")
    with _mock.patch.object(mc, "CONFIG_DIR", real_dir):
        mc.reset_cache()
        doc = mc.load_manager_aliases()  # raises on schema violation
        assert isinstance(doc.get("managers"), list)
        for m in doc["managers"]:
            # canonical_id must be the BARE spine slug (lower-snake, no prefix).
            assert m["canonical_id"] == m["canonical_id"].lower()
    mc.reset_cache()
