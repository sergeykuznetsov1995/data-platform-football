"""
Unit tests for the venue-alias loader in ``dags/utils/medallion_config.py``
(issue #145). Mirrors ``test_medallion_config_referee.py``: mock YAML via
``CONFIG_DIR``/tmp_path for loader mechanics, plus one pass over the real
shipped ``configs/medallion/venue_aliases.yaml`` so a production regression
(dup slug, bad canonical_id, missing city/country) trips immediately.
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


_MOCK_VENUE_ALIASES = """\
venues:
  - canonical_name: "Old Trafford"
    canonical_id: "venue_old_trafford"
    city: "Manchester"
    country: "England"
    aliases:
      _generic: ["Old Trafford"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Gtech Community Stadium"
    canonical_id: "venue_brentford"
    city: "London"
    country: "England"
    aliases:
      _generic: ["Gtech Community Stadium", "Brentford Community Stadium"]
    competition_scope: ["ENG-Premier League"]
"""


@pytest.fixture
def mock_venue_dir(tmp_path, monkeypatch):
    (tmp_path / "venue_aliases.yaml").write_text(
        _MOCK_VENUE_ALIASES, encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    return mc


# ---------------------------------------------------------------------------
# Loader + schema validation
# ---------------------------------------------------------------------------

def test_load_venue_aliases_returns_dict(mock_venue_dir):
    doc = mock_venue_dir.load_venue_aliases()
    assert isinstance(doc, dict)
    assert len(doc["venues"]) == 2


def test_load_missing_file_returns_empty(tmp_path, monkeypatch):
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)  # no venue_aliases.yaml
    mc.reset_cache()
    assert mc.load_venue_aliases() == {"venues": []}


def test_rejects_missing_top_level_key(tmp_path, monkeypatch):
    (tmp_path / "venue_aliases.yaml").write_text("foo: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_venue_aliases()


def test_rejects_invalid_canonical_id_slug(tmp_path, monkeypatch):
    bad = (
        'venues:\n'
        '  - canonical_name: "X"\n'
        '    canonical_id: "Venue X"\n'   # space + uppercase → invalid
        '    city: "C"\n    country: "K"\n'
        '    aliases: {_generic: ["X"]}\n'
    )
    (tmp_path / "venue_aliases.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_venue_aliases()


def test_rejects_duplicate_canonical_id(tmp_path, monkeypatch):
    dup = (
        'venues:\n'
        '  - canonical_name: "A"\n    canonical_id: "venue_x"\n'
        '    city: "C"\n    country: "K"\n    aliases: {_generic: ["A"]}\n'
        '  - canonical_name: "B"\n    canonical_id: "venue_x"\n'
        '    city: "C"\n    country: "K"\n    aliases: {_generic: ["B"]}\n'
    )
    (tmp_path / "venue_aliases.yaml").write_text(dup, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="duplicate canonical_id"):
        mc.load_venue_aliases()


def test_rejects_missing_city(tmp_path, monkeypatch):
    """city/country are mandatory for venues (#145 — geo is half the point)."""
    bad = (
        'venues:\n'
        '  - canonical_name: "A"\n    canonical_id: "venue_a"\n'
        '    country: "England"\n'   # city missing
        '    aliases: {_generic: ["A"]}\n'
    )
    (tmp_path / "venue_aliases.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="city"):
        mc.load_venue_aliases()


# ---------------------------------------------------------------------------
# get_venue_alias_sql_values — VALUES shape
# ---------------------------------------------------------------------------

def test_sql_values_six_columns(mock_venue_dir):
    """Each row is a 6-tuple (raw, canonical_id, canonical, city, country, league)."""
    sql = mock_venue_dir.get_venue_alias_sql_values()
    first = sql.splitlines()[0]
    assert first.count("'") == 12  # six single-quoted fields
    assert "ENG-Premier League" in first


def test_sql_values_merges_both_spellings_to_one_id(mock_venue_dir):
    """Both Brentford spellings carry the same canonical_id (#145 merge)."""
    sql = mock_venue_dir.get_venue_alias_sql_values()
    assert "'Gtech Community Stadium', 'venue_brentford'" in sql
    assert "'Brentford Community Stadium', 'venue_brentford'" in sql


def test_sql_values_raises_on_empty(tmp_path, monkeypatch):
    (tmp_path / "venue_aliases.yaml").write_text("venues: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.get_venue_alias_sql_values()


# ---------------------------------------------------------------------------
# Real shipped YAML — production regression guard
# ---------------------------------------------------------------------------

def test_shipped_venue_yaml_loads_and_renders():
    import re
    from utils import medallion_config as mc
    mc.CONFIG_DIR = PROJECT_ROOT / "configs" / "medallion"
    mc.reset_cache()
    doc = mc.load_venue_aliases()
    venues = doc["venues"]
    assert len(venues) >= 20, "expected the curated APL venue set"
    ids = [v["canonical_id"] for v in venues]
    assert len(set(ids)) == len(ids)
    for cid in ids:
        assert re.fullmatch(r"venue_[a-z0-9_]+", cid), cid
    # every venue carries non-empty city/country
    for v in venues:
        assert v["city"].strip()
        assert v["country"].strip()
    # known anchors present
    for anchor in ("venue_old_trafford", "venue_anfield",
                   "venue_brentford_community"):
        assert anchor in ids
    sql = mc.get_venue_alias_sql_values()
    # sponsor-rename merge survives into the rendered VALUES
    assert "'Gtech Community Stadium', 'venue_brentford_community'" in sql
    assert "'Brentford Community Stadium', 'venue_brentford_community'" in sql
