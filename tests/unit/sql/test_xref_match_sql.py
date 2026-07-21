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
  * FotMob reads NATIVE bronze (fotmob_matches_current, #930 cutover), not
    legacy fotmob_schedule: match_id BIGINT → varchar, utc_time date slice,
    league via fotmob_league_map (14-league scope guard), season slug from
    source_season_key year-start.
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
# FotMob native cutover (#930) — fm_resolved reads fotmob_matches_current
# ---------------------------------------------------------------------------

class TestFotmobNativeSource:
    """#930 cutover: the fotmob branch reads native `fotmob_matches_current`,
    not legacy `bronze.fotmob_schedule`. source_id / date / team-name strings
    must stay byte-identical to legacy so canonical ids and the xref_team
    JOIN keep resolving."""

    def test_reads_native_current_view(self):
        non_comment = _strip_comments(_read_sql())
        assert "iceberg.bronze.fotmob_matches_current" in non_comment, (
            "fotmob branch must read the native fotmob_matches_current view"
        )

    def test_legacy_fotmob_schedule_gone(self):
        non_comment = _strip_comments(_read_sql()).lower()
        assert "bronze.fotmob_schedule" not in non_comment, (
            "legacy bronze.fotmob_schedule must no longer be referenced (#930)"
        )

    def test_fm_native_cte_present(self):
        sql = _read_sql()
        assert re.search(r"\bfm_native\s+AS\b", sql), (
            "expected fm_native CTE (native head-CTE feeding fm_resolved)"
        )

    def test_match_id_cast_to_varchar(self):
        """Native match_id is BIGINT (legacy was varchar) — source_id must be
        CAST back to varchar so it stays the exact same string."""
        non_comment = _strip_comments(_read_sql())
        assert re.search(
            r"CAST\(\s*match_id\s+AS\s+varchar\s*\)", non_comment, re.IGNORECASE,
        ), "expected CAST(match_id AS varchar) for the fotmob source_id"

    def test_match_date_from_utc_time(self):
        """Legacy `date` → native `utc_time` (same ISO-8601 string); the
        10-char slice before TRY_CAST must be preserved."""
        non_comment = _strip_comments(_read_sql())
        assert re.search(
            r"SUBSTR\(\s*utc_time\s*,\s*1\s*,\s*10\s*\)", non_comment, re.IGNORECASE,
        ), "expected SUBSTR(utc_time, 1, 10) date derivation"

    def test_team_names_from_native_columns(self):
        """Legacy home_team/away_team → native home_team_name/away_team_name
        (same source `home.name` strings, so xref_team source_id JOIN holds)."""
        non_comment = _strip_comments(_read_sql())
        assert "home_team_name" in non_comment
        assert "away_team_name" in non_comment

    def test_league_map_cte_joined_on_competition_id(self):
        """`league` is reconstructed from native competition_id via an INNER
        JOIN to fotmob_league_map — the join doubles as the legacy 14-league
        scope guard (native bronze covers the full FotMob catalogue)."""
        sql = _read_sql()
        assert re.search(
            r"fotmob_league_map\s*\(competition_id,\s*league\)\s+AS\s*\(", sql,
        ), "expected fotmob_league_map(competition_id, league) CTE"
        non_comment = _strip_comments(sql)
        assert re.search(
            r"JOIN\s+fotmob_league_map\s+lm\s+ON\s+lm\.competition_id\s*=\s*m\.competition_id",
            non_comment, re.IGNORECASE,
        ), "fm_native must INNER JOIN the league map on competition_id"

    @pytest.mark.parametrize("league", [
        "ENG-Premier League", "ENG-Championship", "ESP-La Liga",
        "GER-Bundesliga", "ITA-Serie A", "FRA-Ligue 1", "NED-Eredivisie",
        "POR-Primeira Liga", "UEFA-Champions League", "UEFA-Europa League",
        "INT-World Cup", "INT-European Championship",
        "INT-Africa Cup of Nations", "INT-Copa America",
    ])
    def test_league_map_covers_legacy_scope(self, league):
        """The map must reproduce all 14 legacy FotMobScraper.LEAGUE_IDS
        strings — a missing entry silently drops that league from silver."""
        block = re.search(
            r"fotmob_league_map\s*\(competition_id,\s*league\)\s+AS\s*\(.*?\n\),",
            _read_sql(), re.DOTALL,
        )
        assert block, "expected fotmob_league_map CTE block"
        assert f"'{league}'" in block.group(0), (
            f"league map must carry {league!r} (legacy 14-league scope)"
        )

    def test_season_slug_from_source_season_key(self):
        """Season year-start = substr(source_season_key, 1, 4) — never derived
        from the key shape (AFCON single-year keys must still slug '2526'
        via the shared legacy CASE, bit-compatible with xref_team)."""
        non_comment = _strip_comments(_read_sql())
        assert re.search(
            r"TRY_CAST\(\s*SUBSTR\(\s*m\.source_season_key\s*,\s*1\s*,\s*4\s*\)\s+AS\s+integer\s*\)",
            non_comment, re.IGNORECASE,
        ), (
            "expected TRY_CAST(SUBSTR(m.source_season_key, 1, 4) AS integer) "
            "year-start derivation"
        )


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


# ---------------------------------------------------------------------------
# PK dedup (#809) — one row per (canonical_id, source)
# ---------------------------------------------------------------------------

class TestPkDedup:
    """The cascade bridges WITHOUT season (#404) and emits each source row's own
    season. A source whose bronze schedule carries one physical match under two
    season labels (ESPN '2021' is a byte-for-byte copy of '1920') would emit two
    rows sharing one canonical_id — violating PK (canonical_id, source). The
    final SELECT must dedup to exactly one row per (canonical_id, source),
    preferring the season consistent with the FBref spine."""

    def test_cascade_wrapped_in_unioned_cte(self):
        """The 7-source UNION ALL is wrapped in a `unioned` CTE for dedup."""
        sql = _read_sql()
        assert re.search(r"\bunioned\s+AS\b", sql), (
            "expected the 7-source cascade to be wrapped in a `unioned` CTE"
        )

    def test_fbref_season_ground_truth_cte(self):
        """A `fbref_season` CTE supplies the canonical's true season (one row
        per canonical via MIN()+GROUP BY so the dedup JOIN cannot fan out)."""
        sql = _read_sql()
        assert re.search(r"\bfbref_season\s+AS\b", sql), (
            "expected a `fbref_season` CTE as the season ground-truth"
        )
        non_comment = _strip_comments(sql)
        assert "MIN(season)" in non_comment, (
            "fbref_season must collapse to one row per canonical via MIN(season)"
        )

    def test_row_number_partitions_by_canonical_and_source(self):
        """Dedup uses ROW_NUMBER() OVER (PARTITION BY canonical_id, source)."""
        sql = _read_sql()
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER", sql, re.IGNORECASE), (
            "expected ROW_NUMBER() OVER (...) for the PK dedup"
        )
        assert re.search(
            r"PARTITION\s+BY\s+u\.canonical_id\s*,\s*u\.source",
            sql, re.IGNORECASE,
        ), "ROW_NUMBER must PARTITION BY (canonical_id, source) to enforce the PK"

    def test_keeps_only_first_ranked_row(self):
        """Final SELECT keeps `_rn = 1` — one row per (canonical_id, source)."""
        non_comment = _strip_comments(_read_sql())
        assert re.search(r"WHERE\s+_rn\s*=\s*1", non_comment, re.IGNORECASE), (
            "expected `WHERE _rn = 1` to keep a single row per PK group"
        )

    def test_tiebreaker_prefers_fbref_season(self):
        """The ORDER BY prefers the row whose season matches the FBref spine."""
        non_comment = _strip_comments(_read_sql())
        assert "fs.fb_season" in non_comment, (
            "dedup ORDER BY must reference the FBref-spine season (fs.fb_season) "
            "so the season-consistent row wins"
        )
