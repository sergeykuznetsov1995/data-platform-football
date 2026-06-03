"""
Unit tests for the referee-alias loader in ``dags/utils/medallion_config.py``
(issue #143). Mirrors ``test_medallion_config.py`` (team aliases): mock YAML via
``MEDALLION_CONFIG_DIR``/tmp_path for loader mechanics, plus one pass over the
real shipped ``configs/medallion/referee_aliases.yaml`` so a production
regression (dup slug, bad canonical_id) trips immediately.
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


_MOCK_REFEREE_ALIASES = """\
referees:
  - canonical_name: "Michael Oliver"
    canonical_id: "ref_michael_oliver"
    aliases:
      fbref: ["Michael Oliver"]
      matchhistory: ["M Oliver", "O Oliver"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Anthony Taylor"
    canonical_id: "ref_anthony_taylor"
    aliases:
      fbref: ["Anthony Taylor"]
      matchhistory: ["A Taylor"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Lee Mason"
    canonical_id: "ref_lee_mason"
    aliases:
      fbref: ["Lee Mason"]
    competition_scope: ["ENG-Premier League"]
"""


@pytest.fixture
def mock_referee_dir(tmp_path, monkeypatch):
    (tmp_path / "referee_aliases.yaml").write_text(
        _MOCK_REFEREE_ALIASES, encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    return mc


# ---------------------------------------------------------------------------
# Loader + schema validation
# ---------------------------------------------------------------------------

def test_load_referee_aliases_returns_dict(mock_referee_dir):
    doc = mock_referee_dir.load_referee_aliases()
    assert isinstance(doc, dict)
    assert len(doc["referees"]) == 3


def test_load_missing_file_returns_empty(tmp_path, monkeypatch):
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)  # no referee_aliases.yaml here
    mc.reset_cache()
    assert mc.load_referee_aliases() == {"referees": []}


def test_rejects_missing_top_level_key(tmp_path, monkeypatch):
    (tmp_path / "referee_aliases.yaml").write_text("foo: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_referee_aliases()


def test_rejects_invalid_canonical_id_slug(tmp_path, monkeypatch):
    bad = (
        'referees:\n'
        '  - canonical_name: "X"\n'
        '    canonical_id: "Ref Oliver"\n'   # space + uppercase → invalid
        '    aliases: {fbref: ["X"]}\n'
    )
    (tmp_path / "referee_aliases.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_referee_aliases()


def test_rejects_duplicate_canonical_id(tmp_path, monkeypatch):
    dup = (
        'referees:\n'
        '  - canonical_name: "A"\n    canonical_id: "ref_x"\n    aliases: {fbref: ["A"]}\n'
        '  - canonical_name: "B"\n    canonical_id: "ref_x"\n    aliases: {fbref: ["B"]}\n'
    )
    (tmp_path / "referee_aliases.yaml").write_text(dup, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="duplicate canonical_id"):
        mc.load_referee_aliases()


# ---------------------------------------------------------------------------
# get_referee_alias_sql_values — VALUES shape
# ---------------------------------------------------------------------------

def test_sql_values_default_four_columns(mock_referee_dir):
    """Default (with_league=True) emits 4-tuples (raw, canonical, id, league)."""
    sql = mock_referee_dir.get_referee_alias_sql_values()
    first = sql.splitlines()[0]
    assert first.count("'") == 8  # four single-quoted fields
    assert "ENG-Premier League" in first


def test_sql_values_merges_both_sources_to_one_id(mock_referee_dir):
    """FBref full name + MatchHistory initial form both carry ref_michael_oliver."""
    sql = mock_referee_dir.get_referee_alias_sql_values()
    assert "'Michael Oliver', 'Michael Oliver', 'ref_michael_oliver'" in sql
    assert "'M Oliver', 'Michael Oliver', 'ref_michael_oliver'" in sql


def test_sql_values_includes_fbref_only_referee(mock_referee_dir):
    sql = mock_referee_dir.get_referee_alias_sql_values()
    assert "ref_lee_mason" in sql


def test_sql_values_raises_on_empty(tmp_path, monkeypatch):
    (tmp_path / "referee_aliases.yaml").write_text("referees: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.get_referee_alias_sql_values()


# ---------------------------------------------------------------------------
# Real shipped YAML — production regression guard
# ---------------------------------------------------------------------------

def test_shipped_referee_yaml_loads_and_renders():
    import re
    from utils import medallion_config as mc
    mc.CONFIG_DIR = PROJECT_ROOT / "configs" / "medallion"
    mc.reset_cache()
    doc = mc.load_referee_aliases()
    refs = doc["referees"]
    assert len(refs) >= 40, "expected the full curated APL referee set"
    # canonical_id slugs valid + unique
    ids = [r["canonical_id"] for r in refs]
    assert len(set(ids)) == len(ids)
    for cid in ids:
        assert re.fullmatch(r"ref_[a-z0-9_]+", cid), cid
    # the known-pair anchors must be present
    for anchor in ("ref_michael_oliver", "ref_anthony_taylor",
                   "ref_paul_tierney", "ref_craig_pawson"):
        assert anchor in ids
    sql = mc.get_referee_alias_sql_values(with_canonical_id=True, with_league=True)
    assert "ref_michael_oliver" in sql and "'M Oliver'" in sql
