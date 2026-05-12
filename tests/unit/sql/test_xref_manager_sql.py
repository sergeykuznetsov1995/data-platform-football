"""
Unit tests for ``dags/sql/silver/xref_manager.sql`` — Phase 1.5 (E2).

Strategy
--------
Pure regex/keyword sanity over the raw SQL — same approach as
``test_xref_referee_sql.py`` and ``test_xref_team_sql.py``.

Documented invariants we exercise:
  * source ∈ {'fbref'} only (single-source spine at Phase 1.5).
  * canonical_id = LOWER(REGEXP_REPLACE(manager_name, '[^a-zA-Z0-9]+', '_')).
  * confidence == 'name_normalize' for every row.
  * Reads bronze.fbref_match_managers (the new Bronze landing table).
  * NULL/empty manager_name is filtered out.
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

    def test_single_source_fbref(self):
        """Phase 1.5 spine: FBref only."""
        sql = _read_sql().lower()
        assert "'fbref'" in sql, "missing 'fbref' source literal"

    def test_no_other_sources_emitted(self):
        """xref_manager must NOT emit any source label other than 'fbref'."""
        sql = _read_sql().lower()
        for forbidden in [
            "'understat'", "'whoscored'", "'sofascore'",
            "'fotmob'", "'matchhistory'", "'clubelo'", "'espn'",
        ]:
            pattern = re.compile(
                re.escape(forbidden) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert not pattern.search(sql), (
                f"source label {forbidden} must not be emitted in xref_manager — "
                "Phase 1.5 is FBref-only (FotMob coachId hardened, others have "
                "no manager metadata in Bronze)"
            )

    def test_reads_bronze_fbref_match_managers(self):
        """SELECT reads from iceberg.bronze.fbref_match_managers."""
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze.fbref_match_managers" in sql_lower, (
            "xref_manager must read from bronze.fbref_match_managers — the "
            "table populated by parsers/finders.py::parse_match_managers"
        )

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

    def test_confidence_name_normalize(self):
        """confidence must be the literal 'name_normalize' (no alias map)."""
        sql = _read_sql()
        assert "'name_normalize'" in sql, (
            "expected confidence='name_normalize' — manager xref has no alias "
            "map yet at Phase 1.5"
        )

    def test_match_score_null(self):
        """match_score must be NULL — no fuzzy matching at Phase 1.5."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "match_score must be CAST(NULL AS double) for xref_manager"

    def test_season_cast_to_varchar(self):
        """Bronze stores season as BIGINT — cast to varchar to match union."""
        sql = _read_sql()
        assert (
            "CAST(season AS varchar)" in sql
            or "CAST(season as varchar)" in sql
        ), "expected CAST(season AS varchar) for unified Silver schema"

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
