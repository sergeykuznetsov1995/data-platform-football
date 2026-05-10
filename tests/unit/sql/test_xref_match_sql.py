"""
Unit tests for ``dags/sql/silver/xref_match.sql`` — structural / logical (T5/E1).

Strategy
--------
Pure regex/keyword sanity over the raw SQL. No Trino/DuckDB engine — see
``test_xref_team_sql.py`` rationale.

Documented invariants we exercise (Phase B / Task 2.1):
  * 7 sources cascade through the CTAS: fbref + whoscored/understat/
    sofascore/fotmob/matchhistory/espn.
  * FBref spine block stays intact (REGEXP_EXTRACT on match_url +
    'fut_<xxhash64>' fallback for future fixtures).
  * Confidence enum is a 3-tier cascade: 'exact' (FBref) /
    'date_team_match' (bridged) / 'orphan' (no FBref counterpart).
  * Each cascade block JOINs silver.xref_team with explicit
    (league, season) predicates (CLAUDE.md hard rule against fan-out).
  * Orphan rows carry source-prefixed canonical_id ('ws_'/'us_'/...).
  * MatchHistory has NO native match_id and emits a deterministic
    'mh_<xxhash64>' synthetic id.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_match.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    """Drop ``-- ...`` line comments to avoid false positives in keyword search."""
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# FBref spine — preserved invariants from the E1 MVP file
# ---------------------------------------------------------------------------

class TestFBrefSpine:

    def test_fbref_source_emitted(self):
        """FBref spine block emits ``'fbref' AS source``."""
        sql = _read_sql()
        assert re.search(r"'fbref'\s+AS\s+source", sql, re.IGNORECASE), (
            "expected `'fbref' AS source` in the FBref spine block"
        )

    def test_match_id_extraction_pattern(self):
        """match_id derived via REGEXP_EXTRACT on fbref_schedule.match_url."""
        sql = _read_sql()
        assert "match_url" in sql.lower()
        assert "fbref_schedule" in sql.lower()
        assert "REGEXP_EXTRACT" in sql.upper()

    def test_match_id_regex_captures_hex_segment(self):
        """The regex must capture the [a-f0-9] hex segment after /matches/."""
        sql = _read_sql()
        assert re.search(r"/matches/\(\[a-f0-9\]\+\)", sql), (
            "expected `/matches/([a-f0-9]+)/` regex for match_id capture"
        )

    def test_fut_pseudo_id_fallback(self):
        """Future fixtures (no match_url yet) get a `fut_<xxhash64>` pseudo-id."""
        sql = _read_sql()
        assert "fut_" in sql, (
            "expected `fut_` pseudo-id fallback for fixtures without match_url"
        )
        assert "XXHASH64" in sql.upper()


# ---------------------------------------------------------------------------
# Phase B cascade — 7 sources, confidence enum, JOIN predicates
# ---------------------------------------------------------------------------

class TestCascadeSources:

    @pytest.mark.parametrize("src", [
        "fbref", "whoscored", "understat", "sofascore",
        "fotmob", "matchhistory", "espn",
    ])
    def test_each_source_emits_label(self, src):
        """Every cascaded source must appear as `'<src>' AS source`."""
        non_comment = _strip_comments(_read_sql())
        pattern = re.compile(rf"'{src}'\s+AS\s+source", re.IGNORECASE)
        assert pattern.search(non_comment), (
            f"source label {src!r} must be emitted by the cascade"
        )

    def test_clubelo_excluded(self):
        """ClubElo has no match-grain bronze — must NOT be in xref_match."""
        non_comment = _strip_comments(_read_sql()).lower()
        assert "'clubelo'" not in non_comment, (
            "clubelo has no match-grain bronze table; do not emit a clubelo source row"
        )


class TestConfidenceEnum:

    @pytest.mark.parametrize("tier", ["exact", "date_team_match", "orphan"])
    def test_three_tier_present(self, tier):
        sql = _read_sql()
        assert f"'{tier}'" in sql, (
            f"confidence tier {tier!r} must appear in cascade SQL"
        )


class TestXrefTeamJoinPredicates:
    """CLAUDE.md hard rule: every JOIN on silver.xref_team must include
    `(league, season)` predicates — otherwise multi-season fan-out 1.5-4×."""

    def test_xref_team_joined(self):
        sql = _read_sql()
        # Allow either `iceberg.silver.xref_team` or just the table name.
        assert "iceberg.silver.xref_team" in sql, (
            "xref_match cascade must JOIN silver.xref_team to resolve home/away canonicals"
        )

    def test_league_and_season_present_in_joins(self):
        """xref_team JOINs must include league + season predicates.

        Heuristic: count the number of `silver.xref_team xt_` aliases (one per
        home/away per source). Then count occurrences of `xt_X.league` and
        `xt_X.season` — each alias must contribute one of each. Easier: look
        for the literal phrase `xt_h.league` and `xt_h.season` (and similarly
        for xt_a) — both must appear at least once per cascade block.
        """
        sql = _read_sql()
        # We expect at least 7 home-side JOINs (one per source).
        home_join_aliases = re.findall(r"silver\.xref_team\s+xt_h", sql)
        assert len(home_join_aliases) >= 7, (
            f"expected ≥7 home-side xref_team JOINs (one per source), "
            f"got {len(home_join_aliases)}"
        )
        # And at least 7 away-side JOINs.
        away_join_aliases = re.findall(r"silver\.xref_team\s+xt_a", sql)
        assert len(away_join_aliases) >= 7

        # Every JOIN must include league + season predicates. We assert the
        # token counts are >= the JOIN counts as a structural sanity check.
        h_league = sql.count("xt_h.league")
        h_season = sql.count("xt_h.season")
        a_league = sql.count("xt_a.league")
        a_season = sql.count("xt_a.season")
        assert h_league >= 7 and h_season >= 7, (
            f"home-side xref_team JOINs must include league + season predicates "
            f"(got league={h_league}, season={h_season})"
        )
        assert a_league >= 7 and a_season >= 7, (
            f"away-side xref_team JOINs must include league + season predicates "
            f"(got league={a_league}, season={a_season})"
        )


class TestOrphanPrefixes:
    """Orphan rows carry a source-prefixed canonical_id, mirrors the
    xref_team.sql.j2:145-156 convention."""

    @pytest.mark.parametrize("prefix", ["ws_", "us_", "ss_", "fm_", "es_"])
    def test_short_prefix_present(self, prefix):
        sql = _read_sql()
        # Look for the prefix as a SQL string-literal concatenation:
        #   COALESCE(fb.canonical_id, 'ws_' || ...)
        assert f"'{prefix}'" in sql, (
            f"orphan-prefix {prefix!r} must appear in cascade canonical_id COALESCE"
        )

    def test_matchhistory_synthetic_id(self):
        """MatchHistory has no native match_id — uses 'mh_<xxhash64>' synthetic."""
        sql = _read_sql()
        # The synthetic id is built before the COALESCE, so look for the
        # 'mh_' prefix concatenated with TO_HEX(XXHASH64(...)).
        assert "'mh_'" in sql, "expected 'mh_' synthetic prefix for matchhistory rows"
        # The synthesis must use a deterministic hash so re-runs don't churn ids.
        # Multiple XXHASH64 occurrences are expected (fbref future-fixture fallback +
        # matchhistory synthesis).
        assert sql.upper().count("XXHASH64") >= 2, (
            "expected ≥2 XXHASH64 calls (fbref 'fut_' fallback + matchhistory 'mh_')"
        )


class TestCascadeBridge:
    """The bridge JOIN tuple is (date, home_canonical_id, away_canonical_id, league, season)."""

    def test_fbref_base_cte_present(self):
        sql = _read_sql()
        assert re.search(r"\bfbref_base\s+AS\b", sql), (
            "expected fbref_base CTE name (the bridge target)"
        )

    @pytest.mark.parametrize("cte", [
        "ws_resolved", "us_resolved", "ss_resolved",
        "fm_resolved", "mh_resolved", "es_resolved",
    ])
    def test_per_source_cte_present(self, cte):
        sql = _read_sql()
        assert re.search(rf"\b{cte}\s+AS\b", sql), (
            f"expected per-source CTE {cte}"
        )

    def test_bridge_join_keys(self):
        """Each cascade block must JOIN fbref_base on
        (match_date, home_canonical_id, away_canonical_id, league, season)."""
        sql = _read_sql()
        # The match_date predicate is the load-bearing one — assert >= 6 occurrences
        # (one per non-FBref cascade block).
        assert sql.count("fb.match_date") >= 6
        assert sql.count("fb.home_canonical_id") >= 6
        assert sql.count("fb.away_canonical_id") >= 6


# ---------------------------------------------------------------------------
# Schema-shape sanity (preserved from E1)
# ---------------------------------------------------------------------------

class TestSchemaShape:

    def test_match_score_null(self):
        """match_score is always NULL — no fuzzy step here."""
        sql = _read_sql()
        assert (
            "CAST(NULL AS double)" in sql
            or "CAST(NULL AS DOUBLE)" in sql
        )

    def test_pure_select_no_create_table(self):
        """File must stay a pure SELECT (silver_tasks wraps in CTAS)."""
        non_comment = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in non_comment.upper()

    def test_display_name_concatenates_home_away(self):
        """display_name uses CONCAT(...home..., ' vs ', ...away...) for debug."""
        sql = _read_sql()
        # Allow either `home`/`away` (FBref) or `home_team`/`away_team` (others).
        assert "CONCAT(" in sql.upper(), "display_name should be built via CONCAT()"
        assert "' vs '" in sql, "display_name separator ' vs ' must appear"
