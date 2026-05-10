"""
DQ check builders for E4 — Narrow Facts (Goals / Cards / Subs / Odds / Ratings)
==============================================================================

Universal builder helpers for Iteration **E4** Silver / Gold tables:

Silver
------
* ``iceberg.silver.match_cards``               — yellow/red cards stream
  (FBref + WhoScored union, ~13.6K rows; orphan-mode tolerable for
  player_id_canonical NULLs because Card events from WhoScored often
  ship with no player attribution).
* ``iceberg.silver.match_substitutions``       — sub on/off pairs
  (~25.6K rows; PK protects against the FBref+WhoScored union
  doubling rows when both sources have the same fixture).
* ``iceberg.silver.matchhistory_match_odds``   — football-data.co.uk
  pre-match + closing odds (~47K rows; PK on bookmaker+market+
  closing_flag deduplicates the wide-schema upstream).
* ``iceberg.silver.sofascore_player_ratings``  — Sofascore per-match
  player ratings (smoke-test 200 rows on 5 APL 2526 fixtures;
  thresholds intentionally low until full backfill in E4 Phase 1.5).

Gold
----
* ``iceberg.gold.fct_goal``           — goals (xxhash64 PK) sourced from
  fct_shot (is_goal=true) and FBref own-goal annotations.
* ``iceberg.gold.fct_card``           — passthrough from silver.match_cards.
* ``iceberg.gold.fct_substitution``   — passthrough from silver.match_substitutions.
* ``iceberg.gold.fct_match_odds``     — passthrough from silver.matchhistory_match_odds.
* ``iceberg.gold.fct_match_rating``   — passthrough from silver.sofascore_player_ratings.

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
* ref_integrity to ``iceberg.gold.dim_match`` for fct_goal/fct_card/
  fct_substitution/fct_match_odds/fct_match_rating is **WARNING** until
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
#   dags/sql/silver/match_cards.sql
#   dags/sql/silver/match_substitutions.sql
#   dags/sql/silver/matchhistory_match_odds.sql
#   dags/sql/silver/sofascore_player_ratings.sql
#   dags/sql/gold/fct_goal.sql       (goal_source)
#   dags/sql/gold/fct_card.sql       (card_source — passthrough)
#   dags/sql/gold/fct_substitution.sql (substitution_source — passthrough)
#   dags/sql/gold/fct_match_odds.sql (odds_source — passthrough)
#   dags/sql/gold/fct_match_rating.sql (rating_source — passthrough)
SILVER_CARD_SOURCES: List[str] = ['fbref', 'whoscored']
SILVER_SUB_SOURCES: List[str] = ['fbref', 'whoscored']
SILVER_ODDS_SOURCES: List[str] = ['matchhistory']
SILVER_RATING_SOURCES: List[str] = ['sofascore']

GOLD_GOAL_SOURCES: List[str] = ['fct_shot', 'fbref_own_goal']
GOLD_CARD_SOURCES: List[str] = ['fbref', 'whoscored']
GOLD_SUB_SOURCES: List[str] = ['fbref', 'whoscored']
GOLD_ODDS_SOURCES: List[str] = ['matchhistory']
GOLD_RATING_SOURCES: List[str] = ['sofascore']


# ---------------------------------------------------------------------------
# Silver — match_cards
# ---------------------------------------------------------------------------

def _build_silver_match_cards_checks() -> List[Check]:
    """DQ for ``iceberg.silver.match_cards`` (E4.1).

    Key invariants
    --------------
    * Volume floor 8K — 13.6K observed on APL 5-season FBref+WhoScored union;
      8K covers a partial-backfill scenario.
    * PK = (match_id_canonical, team_id_canonical, player_id_canonical,
      minute, card_type) — but only when player_id_canonical IS NOT NULL,
      since WhoScored Card events frequently lack player attribution
      (orphan-mode tolerable per E4 spec).
    * card_type ∈ {'yellow', 'red'} — ENUM presence checked via
      row_count(min_rows=1) per value.
    * Bridge un-bridged ratio: rows with match_id_canonical
      LIKE 'whoscored_raw_%' are E1-bridge fallbacks; ~7.4% baseline,
      WARNING-severity (tighten after E1.5 cutover).
    """
    table = 'iceberg.silver.match_cards'
    return [
        # Volume floor (ERROR).
        CHECK.row_count(table, min_rows=8_000, severity='ERROR'),

        # PK uniqueness — scoped to resolved player_id_canonical because
        # NULL-player Card events legitimately collapse into one bucket.
        CHECK.no_duplicates(
            table,
            pk=['match_id_canonical', 'team_id_canonical',
                'player_id_canonical', 'minute', 'card_type'],
            where='player_id_canonical IS NOT NULL',
            severity='WARNING',
        ),

        # NULL guards on critical contract columns (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'minute', 'card_type',
                  'source', 'source_version', 'league', 'season'],
            severity='ERROR',
        ),

        # Minute bounds — APL has 90' regulation + ET, allow up to 130
        # for stoppage-time cards in extra time (ERROR — out-of-range
        # signals upstream parser regression).
        CHECK.value_range(
            table=table, column='minute',
            min_val=0, max_val=130,
            severity='ERROR',
        ),

        # card_type ENUM presence — at least one yellow + one red.
        # data_quality has no `enum_in` primitive, so we use the e3_dq
        # pattern: row_count(min_rows=1) per allowed value.
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_type = 'yellow'",
            severity='WARNING',
            name='source_enum_card_type_yellow',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_type = 'red'",
            severity='WARNING',
            name='source_enum_card_type_red',
        ),

        # source ENUM presence (FBref + WhoScored).
        CHECK.row_count(
            table=table, min_rows=1,
            where="source = 'fbref'",
            severity='WARNING',
            name='source_enum_fbref',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="source = 'whoscored'",
            severity='WARNING',
            name='source_enum_whoscored',
        ),

        # Freshness — Silver is rebuilt weekly via E4 DAG.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),

        # Bridge un-bridged ratio — E1 placeholder bridge id pattern
        # 'whoscored_raw_<bronze_id>'. ~7.4% baseline; we surrogate
        # the e3_dq orphan_rate idiom: bound the absolute count at
        # 15% × 13.6K ≈ 2,040 (room for a partial-backfill spike).
        CHECK.row_count(
            table=table, min_rows=0, max_rows=2_040,
            where="match_id_canonical LIKE 'whoscored_raw_%'",
            severity='WARNING',
            name='bridge_unbridged_rate',
        ),
    ]


# ---------------------------------------------------------------------------
# Silver — match_substitutions
# ---------------------------------------------------------------------------

def _build_silver_match_substitutions_checks() -> List[Check]:
    """DQ for ``iceberg.silver.match_substitutions`` (E4.2).

    PK = (match_id_canonical, team_id_canonical, player_in_canonical,
    player_out_canonical, minute). Both player canonicals are required
    for de-dup (orphan-tolerant on either side individually but the
    pair must resolve at least one side to a canonical id).
    """
    table = 'iceberg.silver.match_substitutions'
    return [
        # Volume floor — APL multi-season ≈ 25.6K subs. Min 15K covers
        # partial backfill.
        CHECK.row_count(table, min_rows=15_000, severity='ERROR'),

        # PK — scoped to rows where BOTH player canonicals resolved.
        CHECK.no_duplicates(
            table,
            pk=['match_id_canonical', 'team_id_canonical',
                'player_in_canonical', 'player_out_canonical', 'minute'],
            where=(
                'player_in_canonical IS NOT NULL '
                'AND player_out_canonical IS NOT NULL'
            ),
            severity='WARNING',
        ),

        # NULL guards — critical contract columns (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'minute',
                  'source', 'source_version', 'league', 'season'],
            severity='ERROR',
        ),

        # Minute bounds (ERROR — same logic as match_cards).
        CHECK.value_range(
            table=table, column='minute',
            min_val=0, max_val=130,
            severity='ERROR',
        ),

        # source ENUM presence (FBref + WhoScored).
        CHECK.row_count(
            table=table, min_rows=1,
            where="source = 'fbref'",
            severity='WARNING',
            name='subs_source_enum_fbref',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="source = 'whoscored'",
            severity='WARNING',
            name='subs_source_enum_whoscored',
        ),

        # Freshness.
        CHECK.freshness(
            table=table, ts_col='_ingested_at',
            max_age_hours=14 * 24,
            severity='WARNING',
        ),
    ]


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
# Gold — fct_goal
# ---------------------------------------------------------------------------

def _build_gold_fct_goal_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_goal`` (E4.5).

    PK = goal_canonical (xxhash64 of match+minute+scorer+is_own_goal+
    pk_tiebreaker). Source ENUM = {'fct_shot', 'fbref_own_goal'} —
    the dominant tier MUST be fct_shot (regular play goals); own
    goals are a small fraction (<5% baseline).
    """
    table = 'iceberg.gold.fct_goal'
    return [
        # Volume floor — ~5.5K observed; min 4K with ERROR.
        CHECK.row_count(table, min_rows=4_000, severity='ERROR'),

        # PK uniqueness on the xxhash64 canonical (ERROR).
        CHECK.no_duplicates(
            table, pk=['goal_canonical'], severity='ERROR',
        ),

        # NULL guards — canonical-trio + critical contract (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'team_id_canonical', 'minute',
                  'goal_canonical', 'goal_source', 'goal_version'],
            severity='ERROR',
        ),

        # R0.4 schema-versioning completeness for goal_canonical.
        CHECK.canonical_completeness(
            table, canonical_col='goal_canonical',
            severity='ERROR',
        ),

        # Minute bounds — same as silver.
        CHECK.value_range(
            table=table, column='minute',
            min_val=0, max_val=130,
            severity='ERROR',
        ),

        # goal_source ENUM presence.
        # fct_shot is the DOMINANT source — must have data (ERROR).
        CHECK.row_count(
            table=table, min_rows=1,
            where="goal_source = 'fct_shot'",
            severity='ERROR',
            name='goal_source_enum_fct_shot',
        ),
        # fbref_own_goal is a minority source — WARNING if absent
        # (could legitimately be empty in a small partial-backfill).
        CHECK.row_count(
            table=table, min_rows=1,
            where="goal_source = 'fbref_own_goal'",
            severity='WARNING',
            name='goal_source_enum_fbref_own_goal',
        ),

        # ref_integrity to dim_match — WARNING (Phase B bridging).
        # parent_key='match_id' because dim_match.match_id is the canonical
        # column (no '_canonical' suffix); see E4 postmortem 2026-05-09.
        CHECK.ref_integrity(
            child='gold.fct_goal',
            parent='gold.dim_match',
            key='match_id_canonical',
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
# Gold — fct_card
# ---------------------------------------------------------------------------

def _build_gold_fct_card_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_card`` (E4.5).

    Passthrough from silver.match_cards. PK = card_canonical (xxhash64).
    Source ENUM = {'fbref', 'whoscored'}.
    """
    table = 'iceberg.gold.fct_card'
    return [
        # Volume floor — silver baseline 13.6K; gold ~equal (passthrough).
        CHECK.row_count(table, min_rows=8_000, severity='ERROR'),

        # PK uniqueness (ERROR).
        CHECK.no_duplicates(
            table, pk=['card_canonical'], severity='ERROR',
        ),

        # NULL guards (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'minute', 'card_type',
                  'card_canonical', 'card_source', 'card_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='card_canonical',
            severity='ERROR',
        ),

        # Minute bounds.
        CHECK.value_range(
            table=table, column='minute',
            min_val=0, max_val=130,
            severity='ERROR',
        ),

        # card_type ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_type = 'yellow'",
            severity='WARNING',
            name='gold_card_type_yellow',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_type = 'red'",
            severity='WARNING',
            name='gold_card_type_red',
        ),

        # card_source ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_source = 'fbref'",
            severity='WARNING',
            name='gold_card_source_fbref',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="card_source = 'whoscored'",
            severity='WARNING',
            name='gold_card_source_whoscored',
        ),

        # ref_integrity to dim_match — WARNING (Phase B).
        CHECK.ref_integrity(
            child='gold.fct_card',
            parent='gold.dim_match',
            key='match_id_canonical',
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
# Gold — fct_substitution
# ---------------------------------------------------------------------------

def _build_gold_fct_substitution_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_substitution`` (E4.5).

    Passthrough from silver.match_substitutions. PK =
    substitution_canonical (xxhash64). Source ENUM =
    {'fbref', 'whoscored'}.
    """
    table = 'iceberg.gold.fct_substitution'
    return [
        # Volume floor — silver baseline 25.6K.
        CHECK.row_count(table, min_rows=15_000, severity='ERROR'),

        # PK uniqueness (ERROR).
        CHECK.no_duplicates(
            table, pk=['substitution_canonical'], severity='ERROR',
        ),

        # NULL guards (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'minute',
                  'substitution_canonical', 'substitution_source',
                  'substitution_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='substitution_canonical',
            severity='ERROR',
        ),

        # Minute bounds.
        CHECK.value_range(
            table=table, column='minute',
            min_val=0, max_val=130,
            severity='ERROR',
        ),

        # substitution_source ENUM presence.
        CHECK.row_count(
            table=table, min_rows=1,
            where="substitution_source = 'fbref'",
            severity='WARNING',
            name='gold_sub_source_fbref',
        ),
        CHECK.row_count(
            table=table, min_rows=1,
            where="substitution_source = 'whoscored'",
            severity='WARNING',
            name='gold_sub_source_whoscored',
        ),

        # ref_integrity to dim_match — WARNING (Phase B).
        CHECK.ref_integrity(
            child='gold.fct_substitution',
            parent='gold.dim_match',
            key='match_id_canonical',
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
# Gold — fct_match_odds
# ---------------------------------------------------------------------------

def _build_gold_fct_match_odds_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_match_odds`` (E4.5).

    Passthrough from silver.matchhistory_match_odds.
    PK = odds_canonical (xxhash64). Source ENUM = {'matchhistory'}.

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
            table, pk=['odds_canonical'], severity='ERROR',
        ),

        # NULL guards (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'bookmaker_code', 'market',
                  'closing_flag', 'odds_canonical', 'odds_source',
                  'odds_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='odds_canonical',
            severity='ERROR',
        ),

        # Decimal-odds bounds (ERROR). Filter NULL via WHERE because
        # some bookmakers don't quote certain markets.
        CHECK.value_range(
            table=table, column='odds_h',
            min_val=1.01, max_val=1000.0,
            where='odds_h IS NOT NULL',
            severity='ERROR',
        ),
        CHECK.value_range(
            table=table, column='odds_d',
            min_val=1.01, max_val=1000.0,
            where='odds_d IS NOT NULL',
            severity='ERROR',
        ),
        CHECK.value_range(
            table=table, column='odds_a',
            min_val=1.01, max_val=1000.0,
            where='odds_a IS NOT NULL',
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
            where='closing_flag = TRUE',
            severity='ERROR',
            name='gold_dod_closing_odds_50pct',
        ),
        CHECK.row_count(
            table=table, min_rows=24_000,
            where='closing_flag = TRUE',
            severity='WARNING',
            name='gold_dod_closing_odds_80pct',
        ),

        # ref_integrity to dim_match — WARNING (Phase B bridging
        # for non-FBref sources). Matchhistory uses bookmaker fixture
        # IDs that don't always map 1:1 to FBref hex match IDs.
        CHECK.ref_integrity(
            child='gold.fct_match_odds',
            parent='gold.dim_match',
            key='match_id_canonical',
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
    rating_canonical (xxhash64). Source ENUM = {'sofascore'}.

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
            table, pk=['rating_canonical'], severity='ERROR',
        ),

        # NULL guards (ERROR).
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'team_side',
                  'rating_canonical', 'rating_source', 'rating_version'],
            severity='ERROR',
        ),

        # R0.4 canonical completeness.
        CHECK.canonical_completeness(
            table, canonical_col='rating_canonical',
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
            key='player_id_canonical',
            parent_key='player_id',
            severity='WARNING',
        ),

        # ref_integrity to dim_match — WARNING (Phase B).
        CHECK.ref_integrity(
            child='gold.fct_match_rating',
            parent='gold.dim_match',
            key='match_id_canonical',
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

    Composition: ``match_cards`` + ``match_substitutions`` +
    ``matchhistory_match_odds`` + ``sofascore_player_ratings``.
    """
    return (
        _build_silver_match_cards_checks()
        + _build_silver_match_substitutions_checks()
        + _build_silver_matchhistory_match_odds_checks()
        + _build_silver_sofascore_player_ratings_checks()
    )


def build_gold_e4_checks() -> List[Check]:
    """Return DQ checks for Gold E4 tables.

    Composition: ``fct_goal`` + ``fct_card`` + ``fct_substitution`` +
    ``fct_match_odds`` + ``fct_match_rating``.
    """
    return (
        _build_gold_fct_goal_checks()
        + _build_gold_fct_card_checks()
        + _build_gold_fct_substitution_checks()
        + _build_gold_fct_match_odds_checks()
        + _build_gold_fct_match_rating_checks()
    )


def build_all_e4_checks() -> List[Check]:
    """Convenience: silver + gold E4 checks.

    The DAG ``dag_transform_e4`` (E4.6) calls this in its
    ``validate_e4`` task.
    """
    return build_silver_e4_checks() + build_gold_e4_checks()


__all__ = [
    'SILVER_CARD_SOURCES',
    'SILVER_SUB_SOURCES',
    'SILVER_ODDS_SOURCES',
    'SILVER_RATING_SOURCES',
    'GOLD_GOAL_SOURCES',
    'GOLD_CARD_SOURCES',
    'GOLD_SUB_SOURCES',
    'GOLD_ODDS_SOURCES',
    'GOLD_RATING_SOURCES',
    'build_silver_e4_checks',
    'build_gold_e4_checks',
    'build_all_e4_checks',
]
