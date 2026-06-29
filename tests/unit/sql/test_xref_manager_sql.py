"""
Unit tests for ``dags/sql/silver/xref_manager.sql`` — two sources (issue #144).

Strategy
--------
Pure regex/keyword sanity over the raw SQL — same approach as
``test_xref_referee_sql.py`` and ``test_xref_team_sql.py``.

Documented invariants we exercise:
  * source ∈ {'fbref', 'fotmob'} (FBref spine + FotMob coachId mirror).
  * canonical_id = LOWER(REGEXP_REPLACE(<name>, '[^a-zA-Z0-9]+', '_')).
  * confidence ∈ {'name_normalize', 'orphan'} (orphan = FotMob coach with no
    FBref counterpart in the same league).
  * Reads bronze.fbref_match_managers (spine) and bronze.fotmob_player_details
    filtered to is_coach (coachId mirror).
  * FotMob source_id is the stable coachId (CAST(player_id AS varchar)).
  * NULL/empty manager/coach name is filtered out.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_manager.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefManagerStructure:
    """Regex/keyword sanity over ``xref_manager.sql``."""

    def test_emits_fbref_and_fotmob_sources(self):
        """Two sources: FBref spine + FotMob coachId mirror (issue #144)."""
        sql = _read_sql().lower()
        for src in ("'fbref'", "'fotmob'"):
            pattern = re.compile(
                re.escape(src) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert pattern.search(sql), f"missing `{src} AS source` literal"

    def test_no_other_sources_emitted(self):
        """Only 'fbref' and 'fotmob' may be emitted — others have no manager
        metadata in Bronze."""
        sql = _read_sql().lower()
        for forbidden in [
            "'understat'", "'whoscored'", "'sofascore'",
            "'matchhistory'", "'clubelo'", "'espn'",
        ]:
            pattern = re.compile(
                re.escape(forbidden) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert not pattern.search(sql), (
                f"source label {forbidden} must not be emitted in xref_manager — "
                "only FBref + FotMob carry coach identity in Bronze"
            )

    def test_reads_bronze_fbref_match_managers(self):
        """SELECT reads from iceberg.bronze.fbref_match_managers (spine)."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze.fbref_match_managers" in sql_lower, (
            "xref_manager must read from bronze.fbref_match_managers — the "
            "table populated by parsers/finders.py::parse_match_managers"
        )

    def test_reads_bronze_fotmob_player_details(self):
        """FotMob mirror reads from iceberg.bronze.fotmob_player_details."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze.fotmob_player_details" in sql_lower, (
            "xref_manager must read FotMob coaches from "
            "bronze.fotmob_player_details (is_coach rows)"
        )

    def test_fotmob_filters_is_coach(self):
        """FotMob block keeps only coaches (is_coach = true)."""
        sql_lower = _read_sql().lower()
        assert "is_coach = true" in sql_lower, (
            "FotMob mirror must filter `is_coach = true` — the table also holds "
            "players (filtered out elsewhere via NOT is_coach)"
        )

    def test_fotmob_source_id_is_stable_coach_id(self):
        """FotMob source_id is the stable coachId = CAST(player_id AS varchar)."""
        sql = _read_sql()
        assert re.search(
            r"CAST\s*\(\s*d?\.?player_id\s+AS\s+varchar\s*\)",
            sql, re.IGNORECASE,
        ), "expected CAST(player_id AS varchar) AS source_id for FotMob coachId"

    def test_canonical_id_normalize_pattern(self):
        """canonical_id = LOWER(REGEXP_REPLACE(<name>, '[^a-zA-Z0-9]+', '_'))."""
        sql = _read_sql()
        pattern = re.compile(
            r"LOWER\s*\(\s*REGEXP_REPLACE",
            re.IGNORECASE,
        )
        assert pattern.search(sql), (
            "expected canonical_id derivation via "
            "LOWER(REGEXP_REPLACE(manager_name, '[^a-zA-Z0-9]+', '_'))"
        )

    def test_canonical_id_regex_uses_alphanumeric_class(self):
        """Normalize regex collapses non-alphanumerics to underscore."""
        sql = _read_sql()
        assert "[^a-zA-Z0-9]+" in sql, (
            "expected regex character class `[^a-zA-Z0-9]+` for normalize"
        )

    def test_canonical_id_transliterates_diacritics(self):
        """canonical_id strips diacritics via NORMALIZE(NFD) + `\\p{Mn}` (issue #201).

        FBref emits the same manager both with and without accents
        ("Régis Le Bris" / "Regis Le Bris"); a bare `[^a-zA-Z0-9]+ -> _`
        produces two different canonical_ids and breaks dim_manager's SCD-2 PK.
        """
        sql = _read_sql()
        assert re.search(r"NORMALIZE\s*\(\s*manager_name\s*,\s*NFD\s*\)", sql, re.IGNORECASE), (
            "expected NORMALIZE(manager_name, NFD) to decompose accents before slugging"
        )
        assert r"\p{Mn}" in sql, (
            "expected `\\p{Mn}` (Unicode combining marks) regex to strip diacritics"
        )

    def test_confidence_name_normalize(self):
        """confidence carries the literal 'name_normalize'."""
        sql = _read_sql()
        assert "'name_normalize'" in sql, (
            "expected confidence='name_normalize' for glued rows"
        )

    def test_confidence_allows_orphan(self):
        """FotMob coach with no FBref counterpart is flagged 'orphan' so the
        Phase 2 orphan-rate report (evaluate_orphan_rate_per_source) can see it."""
        sql = _read_sql()
        assert "'orphan'" in sql, (
            "expected confidence='orphan' branch for un-glued FotMob coaches"
        )

    def test_match_score_null(self):
        """match_score must be NULL — no fuzzy matching at Phase 1.5."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "match_score must be CAST(NULL AS double) for xref_manager"

    def test_season_cast_to_varchar(self):
        """#404: bronze season is year-start bigint → converted to a slug varchar
        ('2425') via LPAD(MOD(...)), matching every other xref table."""
        sql = _read_sql()
        assert "LPAD(CAST(MOD(season" in sql or "LPAD(CAST(MOD(d.season" in sql, (
            "xref_manager.sql must build a slug season via LPAD(MOD(...)) (#404)"
        )

    def test_pure_select_no_create_table(self):
        """File stays a pure SELECT — silver_tasks wraps in CTAS."""
        non_comment = "\n".join(
            line for line in _read_sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "xref_manager.sql must stay pure SELECT in executable SQL"
        )

    def test_filters_null_and_empty_manager(self):
        """`WHERE manager_name IS NOT NULL AND manager_name <> ''`."""
        sql = _read_sql()
        assert "IS NOT NULL" in sql.upper(), (
            "expected NULL-filter on manager_name column"
        )
        assert "<> ''" in sql or "!= ''" in sql, (
            "expected empty-string filter on manager_name column"
        )

    def test_schema_columns_present(self):
        """All 8 documented schema columns appear in SELECT — either as
        ``AS <col>`` alias or as a bare reference (``league``, ``season``).
        """
        sql = _read_sql()
        expected_aliased = [
            "canonical_id", "source", "source_id", "display_name",
            "confidence", "match_score",
        ]
        for col in expected_aliased:
            pattern = re.compile(
                r"AS\s+" + re.escape(col) + r"\b",
                re.IGNORECASE,
            )
            assert pattern.search(sql), (
                f"schema column {col!r} missing as `AS {col}` alias in "
                "xref_manager.sql — Gold dim_manager will JOIN against this column"
            )
        # ``league`` and ``season`` come from Bronze with the right name,
        # so the SQL forwards them bare (matches xref_referee.sql convention).
        for col in ("league", "season"):
            assert re.search(rf"\b{col}\b", sql, re.IGNORECASE), (
                f"schema column {col!r} missing from xref_manager.sql SELECT"
            )

    def test_pk_grouping_present(self):
        """GROUP BY enforces the documented PK = (source, source_id, league, season)."""
        sql_lower = _read_sql().lower()
        assert "group by" in sql_lower, (
            "expected GROUP BY clause to act as DISTINCT for the (source, "
            "source_id, league, season) PK contract"
        )
