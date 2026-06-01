"""
Unit tests for ``dags/sql/silver/xref_referee.sql`` — structural / logical (T5/E1).

Strategy
--------
Pure regex/keyword sanity over the raw SQL — same approach as
``test_xref_team_sql.py`` and ``test_xref_match_sql.py``.

Documented invariants we exercise:
  * source ∈ {'fbref', 'matchhistory'}.
  * canonical_id derived as LOWER(REGEXP_REPLACE(referee, '[^a-zA-Z0-9]+', '_')).
  * confidence == 'name_normalize' for every row.
  * Bronze-only reads (no Gold-era references).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_referee.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefRefereeStructure:
    """Regex/keyword sanity over ``xref_referee.sql``."""

    def test_two_sources_fbref_and_matchhistory(self):
        """Documented sources: fbref + matchhistory only."""
        sql = _read_sql().lower()
        assert "'fbref'" in sql, "missing 'fbref' source literal"
        assert "'matchhistory'" in sql, "missing 'matchhistory' source literal"

    def test_no_other_sources_in_referee_xref(self):
        """E1 referee xref intentionally limited to FBref + MatchHistory."""
        sql = _read_sql().lower()
        for forbidden in [
            "'understat'", "'whoscored'", "'sofascore'",
            "'fotmob'", "'clubelo'", "'espn'",
        ]:
            pattern = re.compile(
                re.escape(forbidden) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert not pattern.search(sql), (
                f"source label {forbidden} must not be emitted in xref_referee — "
                "only fbref + matchhistory carry referee data at E1"
            )

    def test_referee_column_referenced(self):
        """Both bronze tables expose a `referee` column (lower-case in MH)."""
        sql_lower = _read_sql().lower()
        assert "referee" in sql_lower, "expected referee column reference"

    def test_canonical_id_normalize_pattern(self):
        """canonical_id = LOWER(REGEXP_REPLACE(<name>, '[^a-zA-Z0-9]+', '_'))."""
        sql = _read_sql()
        pattern = re.compile(
            r"LOWER\s*\(\s*REGEXP_REPLACE",
            re.IGNORECASE,
        )
        assert pattern.search(sql), (
            "expected canonical_id derivation via "
            "LOWER(REGEXP_REPLACE(name, '[^a-zA-Z0-9]+', '_'))"
        )

    def test_canonical_id_regex_uses_alphanumeric_class(self):
        """Normalize regex collapses non-alphanumerics to underscore."""
        sql = _read_sql()
        # Accept either escaped or raw character-class form.
        assert (
            "[^a-zA-Z0-9]+" in sql
            or "[^a-zA-Z0-9]+" in sql.lower()
        ), "expected regex character class `[^a-zA-Z0-9]+` for normalize"

    def test_canonical_id_transliterates_diacritics(self):
        """canonical_id strips diacritics via NORMALIZE(NFD) + `\\p{Mn}` (issue #215).

        A referee spelled with and without accents must map to ONE canonical_id;
        otherwise dim_referee risks the same SCD-2 split that broke dim_manager.
        """
        sql = _read_sql()
        assert re.search(r"NORMALIZE\s*\(\s*referee_name\s*,\s*NFD\s*\)", sql, re.IGNORECASE), (
            "expected NORMALIZE(referee_name, NFD) to decompose accents before slugging"
        )
        assert r"\p{Mn}" in sql, (
            "expected `\\p{Mn}` (Unicode combining marks) regex to strip diacritics"
        )

    def test_confidence_name_normalize(self):
        """confidence must be the literal 'name_normalize' (no alias map)."""
        sql = _read_sql()
        assert "'name_normalize'" in sql, (
            "expected confidence='name_normalize' — referee xref has no alias "
            "map yet, so the only knowable provenance is the slug normalize"
        )

    def test_match_score_null(self):
        """match_score must be NULL — no fuzzy here."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "match_score must be CAST(NULL AS double) for xref_referee"

    def test_season_cast_to_varchar(self):
        """Bronze stores season as BIGINT for both sources — cast to varchar."""
        sql = _read_sql()
        assert (
            "CAST(season AS varchar)" in sql
            or "CAST(season as varchar)" in sql
        ), "expected CAST(season AS varchar) for unified Silver schema"

    def test_pure_select_no_create_table(self):
        """File stays a pure SELECT — silver_tasks wraps in CTAS.

        Strip ``-- ...`` comments first; the header references
        ``CREATE TABLE iceberg.silver.xref_referee`` in a documentation note.
        """
        non_comment = "\n".join(
            line for line in _read_sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "xref_referee.sql must stay pure SELECT in executable SQL"
        )

    def test_filters_null_and_empty_referee(self):
        """`WHERE referee IS NOT NULL AND referee <> ''` to skip blank rows."""
        sql = _read_sql()
        assert "IS NOT NULL" in sql.upper(), (
            "expected NULL-filter on referee column"
        )
        assert "<> ''" in sql or "!= ''" in sql, (
            "expected empty-string filter on referee column"
        )

    def test_bronze_tables_only(self):
        """Reads from iceberg.bronze.* only — no Silver/Gold dependencies."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze." in sql_lower, (
            "expected at least one iceberg.bronze.* table reference"
        )
        assert "iceberg.silver.fbref_match_enriched" not in sql_lower, (
            "xref_referee must read Bronze, not the Gold-era Silver mart"
        )
