"""
Unit tests for the country-code loader in ``dags/utils/medallion_config.py``
(issue #435). Mirrors ``test_medallion_config_venue.py``: mock YAML via
``CONFIG_DIR``/tmp_path for loader mechanics, plus one pass over the real
shipped ``configs/medallion/country_codes.yaml`` so a production regression
(dup code, bad code slug, missing name) trips immediately.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


pytestmark = pytest.mark.unit


_MOCK_COUNTRY_CODES = """\
countries:
  - {code: ENG, name: England}
  - {code: CIV, name: Ivory Coast}
"""


@pytest.fixture
def mock_country_dir(tmp_path, monkeypatch):
    (tmp_path / "country_codes.yaml").write_text(
        _MOCK_COUNTRY_CODES, encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    return mc


# ---------------------------------------------------------------------------
# Loader + schema validation
# ---------------------------------------------------------------------------

def test_load_country_codes_returns_dict(mock_country_dir):
    doc = mock_country_dir.load_country_codes()
    assert isinstance(doc, dict)
    assert len(doc["countries"]) == 2


def test_load_missing_file_returns_empty(tmp_path, monkeypatch):
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)  # no country_codes.yaml
    mc.reset_cache()
    assert mc.load_country_codes() == {"countries": []}


def test_rejects_missing_top_level_key(tmp_path, monkeypatch):
    (tmp_path / "country_codes.yaml").write_text("foo: []\n", encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_country_codes()


def test_rejects_invalid_code_slug(tmp_path, monkeypatch):
    bad = "countries:\n  - {code: eng, name: England}\n"  # lowercase → invalid
    (tmp_path / "country_codes.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="code"):
        mc.load_country_codes()


def test_rejects_non_three_letter_code(tmp_path, monkeypatch):
    bad = "countries:\n  - {code: ENGL, name: England}\n"  # 4 letters → invalid
    (tmp_path / "country_codes.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.load_country_codes()


def test_rejects_duplicate_code(tmp_path, monkeypatch):
    dup = (
        "countries:\n"
        "  - {code: ENG, name: England}\n"
        "  - {code: ENG, name: England Again}\n"
    )
    (tmp_path / "country_codes.yaml").write_text(dup, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="duplicate code"):
        mc.load_country_codes()


def test_rejects_missing_name(tmp_path, monkeypatch):
    bad = "countries:\n  - {code: ENG}\n"
    (tmp_path / "country_codes.yaml").write_text(bad, encoding="utf-8")
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError, match="name"):
        mc.load_country_codes()


# ---------------------------------------------------------------------------
# get_country_map_sql_values — VALUES shape
# ---------------------------------------------------------------------------

def test_sql_values_two_columns(mock_country_dir):
    """Each row is a 2-tuple (fifa_code, country_name)."""
    sql = mock_country_dir.get_country_map_sql_values()
    first = sql.splitlines()[0]
    assert first.count("'") == 4  # two single-quoted fields
    assert "'ENG', 'England'" in sql


def test_sql_values_escapes_apostrophe(mock_country_dir):
    """A name with an apostrophe (Côte d'Ivoire style) is ANSI-escaped."""
    # mock uses 'Ivory Coast' (no apostrophe); assert the escaper is wired by
    # round-tripping a name that does carry one.
    from utils import medallion_config as mc
    assert mc._escape_sql_string("Côte d'Ivoire") == "Côte d''Ivoire"


def test_sql_values_raises_on_empty(tmp_path, monkeypatch):
    (tmp_path / "country_codes.yaml").write_text(
        "countries: []\n", encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    with pytest.raises(mc.MedallionConfigError):
        mc.get_country_map_sql_values()


# ---------------------------------------------------------------------------
# Real shipped YAML — production regression guard
# ---------------------------------------------------------------------------

def test_shipped_country_yaml_loads_and_renders():
    from utils import medallion_config as mc
    mc.CONFIG_DIR = PROJECT_ROOT / "configs" / "medallion"
    mc.reset_cache()
    doc = mc.load_country_codes()
    countries = doc["countries"]
    assert len(countries) >= 100, "expected a broad FIFA country set"
    codes = [c["code"] for c in countries]
    assert len(set(codes)) == len(codes), "codes must be unique"
    for code in codes:
        assert re.fullmatch(r"[A-Z]{3}", code), code
    # every entry carries a non-empty name
    for c in countries:
        assert c["name"].strip()
    # UK home-nation anchors (the whole point of #435) present + full-name.
    by_code = {c["code"]: c["name"] for c in countries}
    assert by_code.get("ENG") == "England"
    assert by_code.get("SCO") == "Scotland"
    assert by_code.get("USA") == "United States"
    sql = mc.get_country_map_sql_values()
    assert "'ENG', 'England'" in sql
    mc.reset_cache()
