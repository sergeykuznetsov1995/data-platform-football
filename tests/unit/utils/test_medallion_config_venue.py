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
    capacity: 74310
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


def test_rejects_non_int_capacity(tmp_path, monkeypatch):
    """capacity is optional, but when present must be a positive int (#434)."""
    bad = (
        'venues:\n'
        '  - canonical_name: "A"\n    canonical_id: "venue_a"\n'
        '    city: "C"\n    country: "K"\n    capacity: "big"\n'
        '    aliases: {_generic: ["A"]}\n'
    )
    (tmp_path / "venue_aliases.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="capacity"):
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


def test_sql_values_capacity_appended_unquoted(mock_venue_dir):
    """include_capacity=True appends a 7th column; capacity is a bare int (#434)."""
    sql = mock_venue_dir.get_venue_alias_sql_values(include_capacity=True)
    ot_line = [ln for ln in sql.splitlines() if "venue_old_trafford" in ln][0]
    assert "74310" in ot_line, ot_line
    assert "'74310'" not in ot_line, "capacity must be UNQUOTED (numeric, not varchar)"


def test_sql_values_capacity_null_when_absent(mock_venue_dir):
    """A venue without a curated capacity renders bare NULL (not 'NULL')."""
    sql = mock_venue_dir.get_venue_alias_sql_values(include_capacity=True)
    brent_line = [ln for ln in sql.splitlines() if "venue_brentford" in ln][0]
    assert ", NULL)" in brent_line, brent_line  # bare NULL, may have trailing comma
    assert "'NULL'" not in brent_line


def test_default_render_excludes_capacity(mock_venue_dir):
    """Default (dim_match) stays 6 columns — no capacity, no arity drift."""
    sql = mock_venue_dir.get_venue_alias_sql_values()
    assert "74310" not in sql
    first = sql.splitlines()[0]
    assert first.count("'") == 12  # still six single-quoted fields


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
    # capacity (issue #750): FotMob is the PRIMARY source; the shipped YAML keeps only
    # a small CURATED FALLBACK for venues FotMob's current-ground team-profile can't
    # supply (moved grounds). So most venues have NO curated capacity, while a handful
    # (e.g. Goodison Park) still do. Guards that the bulk register was removed but the
    # fallback survives.
    with_cap = [v for v in venues if v.get("capacity") is not None]
    assert 0 < len(with_cap) <= 12, f"expected a small curated fallback set, got {len(with_cap)}"
    for v in with_cap:
        cap = v["capacity"]
        assert isinstance(cap, int) and not isinstance(cap, bool) and cap > 0, v["canonical_id"]
    # Goodison Park (Everton's pre-2025 ground) is a fallback — FotMob reports the new
    # Hill Dickinson Stadium for every season, so the curated value must remain.
    goodison = [v for v in venues if v["canonical_id"] == "venue_goodison_park"]
    assert goodison and goodison[0].get("capacity") == 39414, goodison
    # Old Trafford is FotMob-covered → no curated fallback (renders NULL, UNQUOTED).
    sql_cap = mc.get_venue_alias_sql_values(include_capacity=True)
    ot_line = [ln for ln in sql_cap.splitlines() if "venue_old_trafford" in ln][0]
    assert ot_line.rstrip().rstrip(",").endswith("NULL)"), ot_line
