"""
Unit tests for ``dags/sql/gold/entity_xref.sql`` — structural / logical.

Strategy
--------
Pure regex/keyword sanity over the raw SQL — same approach as
``test_xref_referee_sql.py`` / ``test_xref_team_sql.py``. This legacy Gold
cross-reference is still materialized by ``dag_transform_fbref_gold`` (dual-run
parity vs ``silver.xref_*``), so its slug must stay consistent with the other
normalizers.

Documented invariants we exercise:
  * team canonical_id strips diacritics via NORMALIZE(NFD) + `\\p{Mn}` (#215).
  * player / match canonical_ids stay verbatim FBref IDs (not name slugs).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "entity_xref.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestEntityXrefStructure:
    """Regex/keyword sanity over ``entity_xref.sql``."""

    def test_team_canonical_id_transliterates_diacritics(self):
        """Team slug strips diacritics via NORMALIZE(NFD) + `\\p{Mn}` (issue #215)."""
        sql = _read_sql()
        assert re.search(r"NORMALIZE\s*\(\s*team_name\s*,\s*NFD\s*\)", sql, re.IGNORECASE), (
            "expected NORMALIZE(team_name, NFD) to decompose accents before slugging"
        )
        assert r"\p{Mn}" in sql, (
            "expected `\\p{Mn}` (Unicode combining marks) regex to strip diacritics"
        )

    def test_team_canonical_id_still_collapses_non_alnum(self):
        """The diacritic strip wraps — but does not replace — the slug collapse."""
        sql = _read_sql()
        assert "[^a-zA-Z0-9]+" in sql, (
            "expected regex character class `[^a-zA-Z0-9]+` for the slug collapse"
        )

    def test_player_and_match_canonical_ids_are_verbatim_ids(self):
        """player/match canonical_id = raw FBref id — must NOT be NFD-slugged."""
        sql = _read_sql()
        assert re.search(r"player_id\s+AS\s+canonical_id", sql, re.IGNORECASE), (
            "player canonical_id must stay the verbatim FBref player_id"
        )
        assert re.search(r"match_id\s+AS\s+canonical_id", sql, re.IGNORECASE), (
            "match canonical_id must stay the verbatim FBref match_id"
        )
