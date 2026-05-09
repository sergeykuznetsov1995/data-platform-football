"""
Unit tests for ``dags/sql/silver/xref_match.sql`` — structural / logical (T5/E1).

Strategy
--------
Pure regex/keyword sanity over the raw SQL. No Trino/DuckDB engine — see
``test_xref_team_sql.py`` rationale.

Documented invariants we exercise:
  * E1 MVP scope: source = 'fbref' only (cross-source bridging is Phase B).
  * match_id is derived from ``fbref_schedule.match_url`` via REGEXP_EXTRACT
    (kept in sync with the existing fbref_match_enriched derivation).
  * confidence is always ``'exact'`` (FBref hex match-id is authoritative).
  * canonical_id == source_id == match_id (the FBref id is the spine).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefMatchStructure:
    """Regex/keyword sanity over ``xref_match.sql``."""

    def test_only_fbref_source_emitted(self):
        """E1 MVP — only the literal ``'fbref'`` is emitted as a source label."""
        sql = _read_sql()
        # The fbref string literal must be present, exactly once as the
        # value side of `'fbref' AS source`.
        assert re.search(r"'fbref'\s+AS\s+source", sql, re.IGNORECASE), (
            "expected `'fbref' AS source` — E1 xref_match emits FBref only"
        )

    def test_no_other_sources_present(self):
        """E1 MVP must NOT have other source labels (cross-source = Phase B)."""
        sql_lower = _read_sql().lower()
        for forbidden in [
            "'understat'",
            "'whoscored'",
            "'sofascore'",
            "'fotmob'",
            "'matchhistory'",
            "'clubelo'",
            "'espn'",
        ]:
            # We want to fail if `<forbidden> AS source` appears. The bare
            # literal can legitimately appear in comments, so we look for
            # it followed by `AS source` to avoid false positives.
            pattern = re.compile(
                re.escape(forbidden) + r"\s+as\s+source",
                re.IGNORECASE,
            )
            assert not pattern.search(sql_lower), (
                f"source label {forbidden} must not be emitted in E1 — "
                "cross-source bridging is deferred to Phase B"
            )

    def test_match_id_extraction_pattern(self):
        """match_id derived via REGEXP_EXTRACT on fbref_schedule.match_url."""
        sql = _read_sql()
        assert "match_url" in sql.lower(), (
            "xref_match must derive match_id from fbref_schedule.match_url"
        )
        assert "fbref_schedule" in sql.lower(), (
            "xref_match must read from iceberg.bronze.fbref_schedule"
        )
        assert "REGEXP_EXTRACT" in sql.upper(), (
            "expected REGEXP_EXTRACT(match_url, ...) for match_id derivation"
        )

    def test_match_id_regex_captures_hex_segment(self):
        """The regex must capture the [a-f0-9] hex segment after /matches/."""
        sql = _read_sql()
        # We accept either single-quote-escaped or double-quote-escaped
        # variants — REGEXP_EXTRACT in Trino takes a string literal.
        assert re.search(r"/matches/\(\[a-f0-9\]\+\)", sql), (
            "expected `/matches/([a-f0-9]+)/` regex for match_id capture"
        )

    def test_canonical_id_equals_source_id(self):
        """canonical_id == source_id == match_id (spine equality)."""
        sql = _read_sql()
        # match_id appears at least twice (once as canonical_id, once as source_id)
        assert re.search(r"match_id\s+AS\s+canonical_id", sql, re.IGNORECASE), (
            "expected `match_id AS canonical_id`"
        )
        assert re.search(r"match_id\s+AS\s+source_id", sql, re.IGNORECASE), (
            "expected `match_id AS source_id`"
        )

    def test_confidence_always_exact(self):
        """confidence literal must be 'exact' (FBref id is authoritative)."""
        sql = _read_sql()
        assert "'exact'" in sql, (
            "confidence must be 'exact' — FBref match_id is authoritative; "
            "no fuzzy step lives in xref_match"
        )

    def test_match_score_null(self):
        """match_score is always NULL — no fuzzy step here."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        ), "match_score must be CAST(NULL AS double) for xref_match"

    def test_season_cast_to_varchar(self):
        """fbref_schedule.season is BIGINT — must be cast to varchar."""
        sql = _read_sql()
        assert (
            "CAST(season AS varchar)" in sql
            or "CAST(season as varchar)" in sql
        ), "expected CAST(season AS varchar) for unified Silver schema"

    def test_display_name_concatenates_home_away(self):
        """display_name uses CONCAT(home, ' vs ', away) for debug readability."""
        sql = _read_sql()
        # We allow either `||` or `CONCAT(...)` form — accept both.
        has_concat = "CONCAT(home" in sql.upper() or "CONCAT(HOME" in sql.upper()
        has_pipes = " || " in sql and "home" in sql.lower() and "away" in sql.lower()
        assert has_concat or has_pipes, (
            "expected display_name to be built from home/away (CONCAT or ||)"
        )

    def test_pure_select_no_create_table(self):
        """File must stay a pure SELECT (silver_tasks wraps in CTAS).

        We strip ``-- ...`` comment lines first because the documented
        DAG-integration note in the header explicitly references
        ``CREATE TABLE iceberg.silver.xref_match AS ...`` for the reader.
        """
        non_comment = "\n".join(
            line for line in _read_sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "xref_match.sql must remain a pure SELECT (no CREATE TABLE in "
            "executable SQL) — silver_tasks.run_silver_transform() wraps it"
        )

    def test_fut_pseudo_id_fallback(self):
        """Future fixtures (no match_url yet) get a `fut_<xxhash64>` pseudo-id."""
        sql = _read_sql()
        # The fallback must use a deterministic hash so re-runs don't churn ids.
        assert "fut_" in sql, (
            "expected `fut_` pseudo-id fallback for fixtures without match_url"
        )
        assert "XXHASH64" in sql.upper(), (
            "fut_ fallback must hash via XXHASH64 for determinism"
        )
