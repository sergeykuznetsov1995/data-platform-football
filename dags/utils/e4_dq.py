"""
DQ check builders for E4 — Narrow Facts (Odds / Ratings)
========================================================

Universal builder helpers for Iteration **E4** Silver / Gold tables:

Silver
------
* ``iceberg.silver.matchhistory_match_odds``   — football-data.co.uk
  pre-match + closing odds (~47K rows; PK on bookmaker+market+
  closing_flag deduplicates the wide-schema upstream).
* ``iceberg.silver.sofascore_player_ratings``  — Sofascore per-match
  player ratings (smoke-test 200 rows on 5 APL 2526 fixtures;
  thresholds intentionally low until full backfill in E4 Phase 1.5).

Gold
----
* ``iceberg.gold.fct_match_odds``     — passthrough from silver.matchhistory_match_odds.
* ``iceberg.gold.fct_match_rating``   — passthrough from silver.sofascore_player_ratings.

(``fct_goal`` / ``fct_card`` / ``fct_substitution`` builders were removed in
#448 — the tables are superseded by ``gold.fct_match_timeline``, whose DQ
lives in ``utils.gold_tasks``.)

Pattern mirrors :mod:`utils.e3_dq` — pure :class:`Check` builder
functions using the universal :mod:`utils.data_quality` primitives. The
DAG ``dag_transform_e4`` (E4.6) imports :func:`build_all_e4_checks` for
the ``validate_e4`` callable.

Severity policy
---------------
ERROR (raise → AirflowException) is reserved for:

* row_count below threshold (volume regression),
* PK no_duplicates (when canonical IDs are NOT NULL),
* no_nulls on critical canonical / source / version columns,
* value_range violations (minute, odds, rating bounds),
* canonical_completeness (R0.4 schema-versioning contract),
* DoD: closing-odds coverage <50% (severity-when-ok escalation).

WARNING (Telegram, no raise) for:

* freshness,
* ref_integrity to dim_match / dim_player / dim_team (Phase B bridging
  not yet rolled out — see TODOs below),
* source-ENUM presence (smoke catches new source values),
* secondary metrics (bridge un-bridged ratio, low-cardinality buckets).

Open TODOs (E4 Phase 1.5 cutover and beyond)
--------------------------------------------
* ref_integrity to ``iceberg.gold.dim_match`` for
  fct_match_odds/fct_match_rating is **WARNING** until
  Phase B bridging via xref_match adds non-FBref source coverage. See
  ``MEDALLION_REDESIGN_ROADMAP.md`` E4.6.
* ref_integrity to ``iceberg.gold.dim_player`` for fct_match_rating is
  **WARNING** because the Sofascore→canonical resolver is orphan-tolerant
  (E4 Phase 1.5 backfill will tighten to ERROR).
* DoD coverage closing odds — once we hit ≥80% closing flag rate on
  full-backfill (currently smoke ~47K rows), promote to ERROR.
* sofascore row_count threshold currently set to 50 (smoke). After full
  backfill (~9K rows/season × N seasons), bump to season-floor.

Naming conventions
------------------
Every Check carries an explicit ``severity`` (we never trust the
factory default, mirroring e3_dq.py). Source-ENUM presence checks use
``name='source_enum_<value>'`` so the Telegram digest can group them.
"""

from __future__ import annotations

import logging
from typing import List

from utils.data_quality import (
    CHECK,
    Check,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source ENUM literals (frozen for E4 wave-1)
# ---------------------------------------------------------------------------
# Keep these in sync with the SQL CASE/literal expressions in
#   dags/sql/silver/matchhistory_match_odds.sql
#   dags/sql/silver/sofascore_player_ratings.sql
#   dags/sql/gold/fct_match_odds.sql (odds_source — passthrough)
#   dags/sql/gold/fct_match_rating.sql (rating_source — passthrough)
SILVER_ODDS_SOURCES: List[str] = ['matchhistory']
SILVER_RATING_SOURCES: List[str] = ['sofascore']

GOLD_ODDS_SOURCES: List[str] = ['matchhistory']
GOLD_RATING_SOURCES: List[str] = ['sofascore']


# ---------------------------------------------------------------------------
# Silver — matchhistory_match_odds
# ---------------------------------------------------------------------------

def _build_silver_matchhistory_match_odds_checks() -> List[Check]:
    """DQ for ``iceberg.silver.matchhistory_match_odds`` (E4.3).

    PK = (match_id_canonical, bookmaker_code, market, closing_flag).
    The wide football-data.co.uk schema unpivots into one row per
    (match × bookmaker × market × pre/closing), so closing_flag is
    part of the PK (the SAME bookmaker quotes pre-match AND closing
    for the same market).

    DoD (roadmap)
    -------------
    Closing-odds coverage ≥80%: among all rows, the share with
    ``closing_flag = TRUE`` should be ≥80% across the matched corpus.
    Implemented as row_count guard with absolute floor — actual coverage
    ratio promoted to ERROR/WARNING via two thresholds. After full
    backfill, switch to a coverage primitive if data_quality adds one.
    """
    table = 'iceberg.silver.matchhistory_match_odds'
    return [
        # Volume floor — 47K observed on multi-season corpus; min 30K
        # covers partial backfill.
        CHECK.row_count(table, min_rows=30_000, severity='ERROR'),

        # PK uniqueness (ERROR — match_id_canonical IS NOT NULL is a
        # hard contract here, no orphan mode).
        CHECK.no_duplicates(
            table,
            pk=['match_id_canonical', 'bookmaker_code', 'market', 'closing_flag'],
            severity='ERROR',
        ),

        # NULL guards — critical contract columns (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'bookmaker_code', 'market',
                  'closing_flag', 'source', 'source_version',
                  'league', 'season'],
            severity='ERROR',
        ),

        # source ENUM presence (matchhistory only at this layer).
        CHECK.row_count(
            table=table, min_rows=1,
            where="source = 'matchhistory'",
            severity='WARNING',
            name='odds_source_enum_matchhistory',
        ),

        # DoD: closing-odds coverage. We bound the ABSOLUTE count of
        # closing_flag=TRUE rows; with 30K floor and 80% target, that
        # gives a ≥24K floor on closing rows. Two-tier severity by
        # splitting into ERROR (50% floor = 15K) and WARNING (80% = 24K).
        CHECK.row_count(
            table=table, min_rows=15_000,
            where='closing_flag = TRUE',
            severity='ERROR',
            name='dod_closing_odds_coverage_50pct',
        ),
        CHECK.row_count(
            table=table, min_rows=24_000,
            where='closing_flag = TRUE',
            severity='WARNING',
            name='dod_closing_odds_coverage_80pct',
        ),

        # Freshness.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Silver — sofascore_player_ratings
# ---------------------------------------------------------------------------

def _build_silver_sofascore_player_ratings_checks() -> List[Check]:
    """DQ for ``iceberg.silver.sofascore_player_ratings`` (E4.4).

    Smoke-test status (May 2026): 200 rows on 5 APL 2526 fixtures.
    Thresholds intentionally low until E4 Phase 1.5 backfill.

    PK = (match_id_canonical, player_id_canonical, team_side) —
    team_side ∈ {'home', 'away'} disambiguates the rare case where
    the same player played for both teams across two fixtures of the
    same match window (does not happen in APL; defensive PK column).

    Rating ∈ [0, 10] is the Sofascore native scale. NULL ratings are
    legitimate for players who played 0 minutes (subbed off after a
    red card to a teammate and recorded in the lineup). We apply the
    value_range only to NOT NULL rows via WHERE clause — the runner
    already filters NULL values internally for value_range.
    """
    table = 'iceberg.silver.sofascore_player_ratings'
    return [
        # Volume floor — smoke 200; min 50 to allow Phase 1.5 partial
        # bronze sync (could drop to 0 if bronze empty).
        CHECK.row_count(table, min_rows=50, severity='ERROR'),

        # PK — strict (canonical-trio is required by E4 spec for
        # ratings; orphan-mode is at the resolver level via NULL on
        # player_id_canonical → those rows skipped from the table).
        CHECK.no_duplicates(
            table,
            pk=['match_id_canonical', 'player_id_canonical', 'team_side'],
            where='player_id_canonical IS NOT NULL',
            severity='WARNING',
        ),

        # NULL guards on canonical-trio + contract columns (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'team_side',
                  'source', 'source_version', 'league', 'season'],
            severity='ERROR',
        ),

        # Rating bounds — Sofascore native [0, 10] scale.
        CHECK.value_range(
            table=table, column='rating',
            min_val=0, max_val=10,
            severity='ERROR',
        ),

        # team_side ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="team_side = 'home'",
            severity='WARNING',
            name='ratings_team_side_home',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="team_side = 'away'",
            severity='WARNING',
            name='ratings_team_side_away',
        ),

        # Freshness.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Gold — fct_match_odds
# ---------------------------------------------------------------------------

def _build_gold_fct_match_odds_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_match_odds`` (E4.5).

    Passthrough from silver.matchhistory_match_odds.
    PK = odds_id (xxhash64). Source ENUM = {'matchhistory'}.

    Odds bounds
    -----------
    Decimal odds quoted by football-data.co.uk live in [1.01, 1000.0].
    The lower bound is the minimum bookmaker margin (no quote of 1.00
    or below, that would imply free money). The upper bound is the
    practical cap on long-shot odds (1000+ is reserved for clearly
    impossible outcomes like 50+1 minute corner). Out-of-range rows
    indicate either a parser bug or a malformed CSV cell upstream.
    """
    table = 'iceberg.gold.fct_match_odds'
    return [
        # Volume floor — silver baseline 47K.
        CHECK.row_count(table, min_rows=30_000, severity='ERROR'),

        # PK uniqueness (ERROR).
        CHECK.no_duplicates(
            table, pk=['odds_id'], severity='ERROR',
        ),

        # NULL guards (ERROR). #426: gold columns renamed (match_id,
        # bookmaker, is_closing); silver keeps the old names.
        CHECK.no_nulls(
            table,
            cols=['match_id', 'bookmaker', 'market',
                  'is_closing', 'odds_id', 'odds_source',
                  'odds_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='odds_id',
            severity='ERROR',
        ),

        # Decimal-odds bounds (ERROR). Filter NULL via WHERE because
        # some bookmakers don't quote certain markets.
        CHECK.value_range(
            table=table, column='odds_home',
            min_val=1.01, max_val=1000.0,
            where='odds_home IS NOT NULL',
            severity='ERROR',
        ),
        CHECK.value_range(
            table=table, column='odds_draw',
            min_val=1.01, max_val=1000.0,
            where='odds_draw IS NOT NULL',
            severity='ERROR',
        ),
        CHECK.value_range(
            table=table, column='odds_away',
            min_val=1.01, max_val=1000.0,
            where='odds_away IS NOT NULL',
            severity='ERROR',
        ),

        # odds_source ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="odds_source = 'matchhistory'",
            severity='WARNING',
            name='gold_odds_source_matchhistory',
        ),

        # DoD: closing-odds coverage gate (ERROR <50%, WARNING <80%).
        # 30K floor × 50% = 15K; × 80% = 24K. Same idiom as Silver.
        CHECK.row_count(
            table=table, min_rows=15_000,
            where='is_closing = TRUE',
            severity='ERROR',
            name='gold_dod_closing_odds_50pct',
        ),
        CHECK.row_count(
            table=table, min_rows=24_000,
            where='is_closing = TRUE',
            severity='WARNING',
            name='gold_dod_closing_odds_80pct',
        ),

        # ref_integrity to dim_match — WARNING (Phase B bridging
        # for non-FBref sources). Matchhistory uses bookmaker fixture
        # IDs that don't always map 1:1 to FBref hex match IDs.
        CHECK.ref_integrity(
            child='gold.fct_match_odds',
            parent='gold.dim_match',
            key='match_id',
            parent_key='match_id',
            severity='WARNING',
        ),

        # Freshness.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Gold — fct_match_rating
# ---------------------------------------------------------------------------

def _build_gold_fct_match_rating_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_match_rating`` (E4.5).

    Passthrough from silver.sofascore_player_ratings. PK =
    rating_id (xxhash64). Source ENUM = {'sofascore'}.

    Smoke-test status (May 2026): ~200 rows on 5 APL 2526 fixtures.
    Thresholds match the silver layer (50-row floor) — will be
    bumped to season-floor after E4 Phase 1.5 full backfill.
    """
    table = 'iceberg.gold.fct_match_rating'
    return [
        # Volume floor — smoke 200; min 50 (smoke-tolerant).
        CHECK.row_count(table, min_rows=50, severity='ERROR'),

        # PK uniqueness (ERROR).
        CHECK.no_duplicates(
            table, pk=['rating_id'], severity='ERROR',
        ),

        # NULL guards (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team_side',
                  'rating_id', 'rating_source', 'rating_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='rating_id',
            severity='ERROR',
        ),

        # Rating bounds — Sofascore [0, 10].
        CHECK.value_range(
            table=table, column='rating',
            min_val=0, max_val=10,
            severity='ERROR',
        ),

        # rating_source ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="rating_source = 'sofascore'",
            severity='WARNING',
            name='gold_rating_source_sofascore',
        ),

        # ref_integrity to dim_player — WARNING (orphan-tolerant
        # because Sofascore→canonical resolver isn't 100% on smoke).
        CHECK.ref_integrity(
            child='gold.fct_match_rating',
            parent='gold.dim_player',
            key='player_id',
            parent_key='player_id',
            severity='WARNING',
        ),

        # ref_integrity to dim_match — WARNING (Phase B).
        CHECK.ref_integrity(
            child='gold.fct_match_rating',
            parent='gold.dim_match',
            key='match_id',
            parent_key='match_id',
            severity='WARNING',
        ),

        # Freshness.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Public builder API
# ---------------------------------------------------------------------------

def build_silver_e4_checks() -> List[Check]:
    """Return DQ checks for Silver E4 tables.

    Composition: ``matchhistory_match_odds`` + ``sofascore_player_ratings``.
    (``match_cards`` / ``match_substitutions`` were folded into the Gold
    facts — their DQ now lives in the gold builders; #382.)
    """
    return (
        _build_silver_matchhistory_match_odds_checks()
        + _build_silver_sofascore_player_ratings_checks()
    )


def build_gold_e4_checks() -> List[Check]:
    """Return DQ checks for Gold E4 tables.

    Composition: ``fct_match_odds`` + ``fct_match_rating``.
    (``fct_goal`` / ``fct_card`` / ``fct_substitution`` checks removed in
    #448 together with the tables.)
    """
    return (
        _build_gold_fct_match_odds_checks()
        + _build_gold_fct_match_rating_checks()
    )


def build_all_e4_checks() -> List[Check]:
    """Convenience: silver + gold E4 checks.

    The DAG ``dag_transform_e4`` (E4.6) calls this in its
    ``validate_e4`` task.
    """
    return build_silver_e4_checks() + build_gold_e4_checks()


__all__ = [
    'SILVER_ODDS_SOURCES',
    'SILVER_RATING_SOURCES',
    'GOLD_ODDS_SOURCES',
    'GOLD_RATING_SOURCES',
    'build_silver_e4_checks',
    'build_gold_e4_checks',
    'build_all_e4_checks',
]
