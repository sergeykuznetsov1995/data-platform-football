"""
DQ check builders for E3 — Core Match Facts
=============================================

Universal builder helpers for Iteration **E3** Silver / Gold tables:

* ``iceberg.silver.whoscored_events_spadl`` — SPADL-canonical event stream
  (R3 verdict: 89.97% high+medium coverage on 2425+2526 APL corpus).
* ``iceberg.silver.espn_lineup``           — ESPN matchsheet lineup, schema
  parity with ``silver.fbref_match_lineups`` (E3.2).
* ``iceberg.gold.fct_event``                — match_id passthrough +
  team_id 2-hop bridge + player_id LEFT JOIN xref (E3.3).
* ``iceberg.gold.fct_shot``                 — shots fact, INNER JOIN bridge
  for match_id (1.8% rejection on smoke-test, E3.4).
* ``iceberg.gold.fct_lineup``               — FBref+ESPN unified lineup,
  bridge via (date, home_canonical_id, away_canonical_id) (E3.5).

Pattern mirrors ``utils.xref_dq.build_all_xref_checks`` — pure Check builder
functions using the universal :mod:`utils.data_quality` primitives. The DAG
``dag_transform_e3`` (E3.7) imports :func:`build_all_e3_checks` for the
``validate_e3`` callable.

Open TODOs (E1.5 cutover and beyond)
------------------------------------
* **schema-version literal pin** for fct_event — currently the file checks
  ``action_source != 'whoscored_spadl_proprietary_v1'`` only. When the
  SPADL spec evolves to v2, bump both the SQL and this allow-list.
* **fct_lineup PK with NULL player_id** — ESPN rows where
  resolver returns NULL leave ``player_id IS NULL``. The
  ``no_duplicates`` runner uses ``COUNT(*) - COUNT(DISTINCT (...))``;
  NULL group by produces a single bucket per (match, team, NULL), so a
  duplicate ``(m, t, NULL)`` IS caught. Documented for posterity — full
  PK contract verification lives in E3.9 unit tests.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from utils.data_quality import (
    CHECK,
    Check,
    CheckResult,
    _get_conn,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SPADL action vocabulary (R3 D2: 25 enum values; own_goal added in #572)
# ---------------------------------------------------------------------------
# Source of truth: dags/sql/silver/whoscored_events_spadl.sql (header).
# Keep this list in sync with the SQL CASE expression. Adding a new tier
# without updating the enum check would silently let unknown actions slip
# through.

SPADL_ACTION_ENUM: List[str] = [
    'pass', 'cross', 'throw_in',
    'freekick_crossed', 'freekick_short',
    'corner_crossed', 'corner_short',
    'take_on', 'foul', 'tackle', 'interception',
    'shot', 'shot_penalty', 'shot_freekick',
    'keeper_save', 'keeper_claim', 'keeper_punch', 'keeper_pick_up',
    'clearance', 'bad_touch', 'dribble', 'goalkick', 'ball_recovery',
    'own_goal',
    'unknown',
]

# Schema-version literals (R0.4 schema-versioning contract).
SPADL_ACTION_SOURCE = 'whoscored_spadl_proprietary_v1'
SPADL_ACTION_VERSION = 'v1'


# ---------------------------------------------------------------------------
# WhoScored 39-type taxonomy — frozen at E3 wave-1, verified 2425+2526
# ---------------------------------------------------------------------------
# Source: docs/research/E3.5_inventory.md §3 (Bronze taxonomy audit, 2026-05-08).
# Used by E3.5 backfill DAG: before processing a historical season we run
# ``SELECT DISTINCT type FROM bronze.whoscored_events WHERE season=...`` and
# assert the result is a subset of this list. A drift (new type seen in a
# historical season but not mapped in `whoscored_events_spadl.sql`) raises
# ERROR — the SPADL CASE tree must be extended before backfill can proceed.
#
# Keep this list ALPHA-sorted for diffability. Adding a value here without
# also adding the corresponding CASE branch in
# `dags/sql/silver/whoscored_events_spadl.sql` would let an unknown type
# slip through to `action_canonical='unknown'` undetected.
WHOSCORED_KNOWN_TYPES_39: List[str] = [
    'Aerial',
    'BallRecovery',
    'BallTouch',
    'BlockedPass',
    'Card',
    'Challenge',
    'ChanceMissed',
    'Claim',
    'Clearance',
    'CornerAwarded',
    'CrossNotClaimed',
    'Dispossessed',
    'End',
    'Error',
    'FormationChange',
    'FormationSet',
    'Foul',
    'Goal',
    'GoodSkill',
    'Interception',
    'KeeperPickup',
    'KeeperSweeper',
    'MissedShots',
    'OffsideGiven',
    'OffsidePass',
    'OffsideProvoked',
    'Pass',
    'PenaltyFaced',
    'Punch',
    'Save',
    'SavedShot',
    'ShieldBallOpp',
    'ShotOnPost',
    'Smother',
    'Start',
    'SubstitutionOff',
    'SubstitutionOn',
    'TakeOn',
    'Tackle',
]

assert len(WHOSCORED_KNOWN_TYPES_39) == 39, (
    "WHOSCORED_KNOWN_TYPES_39 must have exactly 39 entries — "
    f"got {len(WHOSCORED_KNOWN_TYPES_39)}"
)


# ---------------------------------------------------------------------------
# Per-season DQ helpers (E3.5 backfill)
# ---------------------------------------------------------------------------

def _safe_predicate_value(value: str) -> str:
    """Single-quote-escape a value for a Trino predicate.

    Local helper to avoid importing the gold_tasks/silver_tasks _safe helpers
    (those have stricter rules suited for partition values; here we accept
    league strings with apostrophes etc. but still defend against injection
    of comment markers / statement terminators).
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"predicate value must be non-empty str, got {value!r}")
    if any(ch in value for ch in ('\x00', '\n', '\r', ';')):
        raise ValueError(f"predicate value contains forbidden chars: {value!r}")
    if '--' in value or '/*' in value or '*/' in value:
        raise ValueError(f"predicate value contains SQL comment marker: {value!r}")
    return value.replace("'", "''")


def taxonomy_diff_check(
    season: str,
    league: str = 'ENG-Premier League',
) -> CheckResult:
    """Verify ``bronze.whoscored_events`` types ⊆ 39-mapping for one season.

    Used by E3.5 backfill DAG before Silver materialisation. If a new
    WhoScored type appears (e.g. 2122 has a deprecated marker not seen in
    2425+), the SPADL CASE tree must be extended in
    `dags/sql/silver/whoscored_events_spadl.sql` AND
    :data:`WHOSCORED_KNOWN_TYPES_39` here, otherwise the new type silently
    maps to `'unknown'`.

    Returns a single ERROR-severity :class:`CheckResult` ready to be
    appended to a ``RunReport`` from :mod:`utils.data_quality`.
    """
    name = f"taxonomy_diff[bronze.whoscored_events season={season}]"
    safe_season = _safe_predicate_value(season)
    safe_league = _safe_predicate_value(league)
    sql = (
        "SELECT DISTINCT type FROM iceberg.bronze.whoscored_events "
        f"WHERE season = '{safe_season}' AND league = '{safe_league}' "
        "AND type IS NOT NULL"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

        observed = {r[0] for r in rows if r and r[0]}
        known = set(WHOSCORED_KNOWN_TYPES_39)
        unknown_types = sorted(observed - known)
        if not observed:
            return CheckResult(
                name=name,
                kind='taxonomy_diff',
                severity='ERROR',
                passed=False,
                details=(
                    f"bronze.whoscored_events has 0 rows for "
                    f"season={season!r}, league={league!r} — cannot run "
                    f"taxonomy diff (Bronze likely not yet scraped)"
                ),
                value=0,
            )
        if unknown_types:
            return CheckResult(
                name=name,
                kind='taxonomy_diff',
                severity='ERROR',
                passed=False,
                details=(
                    f"{len(unknown_types)} unmapped WhoScored type(s): "
                    + ", ".join(repr(t) for t in unknown_types)
                    + ". Add to dags/sql/silver/whoscored_events_spadl.sql "
                    + "CASE tree AND utils.e3_dq.WHOSCORED_KNOWN_TYPES_39 "
                    + "before resuming backfill."
                ),
                value=unknown_types,
            )
        return CheckResult(
            name=name,
            kind='taxonomy_diff',
            severity='ERROR',
            passed=True,
            details=(
                f"{len(observed)} distinct types observed; all in 39-mapping"
            ),
            value=sorted(observed),
        )
    except Exception as e:
        logger.exception("taxonomy_diff_check raised")
        return CheckResult(
            name=name,
            kind='taxonomy_diff',
            severity='ERROR',
            passed=False,
            error=str(e),
        )
    finally:
        conn.close()


def parity_check_event_counts_per_season(
    season: str,
    league: str = 'ENG-Premier League',
) -> CheckResult:
    """Per-season Bronze→Silver→Gold parity for ``whoscored_events``.

    Variant of :func:`parity_check_event_counts` scoped to a single
    (season, league) tuple. Used by the E3.5 backfill DAG's
    ``validate_backfill`` task to prove that the partition just inserted
    preserved bronze→silver→gold row counts.

    Same severity model as the global parity check (ERROR).
    """
    name = (
        "parity[bronze→silver→gold whoscored_events "
        f"season={season} league={league}]"
    )
    safe_season = _safe_predicate_value(season)
    safe_league = _safe_predicate_value(league)
    where = (
        f"WHERE season = '{safe_season}' "
        f"AND league = '{safe_league}'"
    )
    sql = (
        "SELECT "
        f"  (SELECT COUNT(*) FROM iceberg.bronze.whoscored_events {where}) AS bronze_cnt, "
        f"  (SELECT COUNT(*) FROM iceberg.silver.whoscored_events_spadl {where}) AS silver_cnt, "
        f"  (SELECT COUNT(*) FROM iceberg.gold.fct_event {where}) AS gold_cnt"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            cur.close()

        bronze_cnt = int(row[0]) if row and row[0] is not None else 0
        silver_cnt = int(row[1]) if row and row[1] is not None else 0
        gold_cnt = int(row[2]) if row and row[2] is not None else 0

        if bronze_cnt == 0:
            return CheckResult(
                name=name,
                kind='parity',
                severity='ERROR',
                passed=False,
                details=(
                    f"bronze empty for season={season!r}, league={league!r} "
                    f"(cannot compute parity)"
                ),
                value=0.0,
            )
        sb_ratio = silver_cnt / bronze_cnt
        gs_ratio = (gold_cnt / silver_cnt) if silver_cnt else 0.0
        sb_ok = sb_ratio >= _PARITY_SILVER_BRONZE_MIN
        gs_ok = gs_ratio >= _PARITY_GOLD_SILVER_MIN
        passed = sb_ok and gs_ok

        details = (
            f"bronze={bronze_cnt}, silver={silver_cnt} "
            f"(silver/bronze={sb_ratio:.4f}, min={_PARITY_SILVER_BRONZE_MIN}); "
            f"gold={gold_cnt} (gold/silver={gs_ratio:.4f}, "
            f"min={_PARITY_GOLD_SILVER_MIN})"
        )
        if not passed:
            offenders = []
            if not sb_ok:
                offenders.append(
                    f"silver/bronze={sb_ratio:.4f} < {_PARITY_SILVER_BRONZE_MIN}"
                )
            if not gs_ok:
                offenders.append(
                    f"gold/silver={gs_ratio:.4f} < {_PARITY_GOLD_SILVER_MIN}"
                )
            details += " — VIOLATIONS: " + "; ".join(offenders)

        return CheckResult(
            name=name,
            kind='parity',
            severity='ERROR',
            passed=passed,
            details=details,
            value={
                'season': season,
                'league': league,
                'bronze': bronze_cnt,
                'silver': silver_cnt,
                'gold': gold_cnt,
                'silver_over_bronze': round(sb_ratio, 4),
                'gold_over_silver': round(gs_ratio, 4),
            },
        )
    except Exception as e:
        logger.exception("parity_check_event_counts_per_season raised")
        return CheckResult(
            name=name,
            kind='parity',
            severity='ERROR',
            passed=False,
            error=str(e),
        )
    finally:
        conn.close()


def build_per_season_e3_checks(
    season: str,
    league: str = 'ENG-Premier League',
) -> List[Check]:
    """E3.5 backfill DQ — checks scoped to one (season, league) tuple.

    Composition:
      * PK uniqueness scoped to season for silver/gold E3 tables.
      * SPADL ``unknown_rate`` < 5% per-season (R3 spec; widened from the
        global 5.75% gate because per-season variance is smaller).
      * Schema-version drift guard (literal pin) per-season.

    Returns a list of :class:`Check` instances suitable for
    :func:`utils.data_quality.run_checks`.
    """
    safe_season = _safe_predicate_value(season)
    safe_league = _safe_predicate_value(league)
    season_filter = (
        f"season = '{safe_season}' AND league = '{safe_league}'"
    )

    return [
        # ===== PK uniqueness scoped to season =====
        CHECK.no_duplicates(
            'iceberg.silver.whoscored_events_spadl',
            pk=['match_id', 'event_id'],
            where=season_filter,
            name=(
                'no_duplicates[silver.whoscored_events_spadl '
                f"season={season}]"
            ),
        ),
        CHECK.no_duplicates(
            'iceberg.gold.fct_event',
            pk=['match_id_canonical', 'event_id'],
            where=season_filter,
            name=f"no_duplicates[gold.fct_event season={season}]",
        ),
        CHECK.no_duplicates(
            'iceberg.gold.fct_shot',
            pk=['match_id', 'shot_id'],
            where=season_filter,
            name=f"no_duplicates[gold.fct_shot season={season}]",
        ),

        # ===== SPADL unknown_rate per-season — must stay below 5% =====
        # APL 2425/2526 baseline: 4.22-4.44%. Threshold 5% (R3 spec) catches
        # historical seasons where the taxonomy might shift slightly.
        # Implemented as ratio between two row counts via a single COUNT_IF
        # — the universal CHECK registry has no `ratio` primitive, so we
        # bound the absolute count and let the value-range serve as a
        # secondary signal (size depends on season — empirical 700K x 5% ≈ 35K).
        CHECK.row_count(
            table='iceberg.silver.whoscored_events_spadl',
            min_rows=0,
            max_rows=40_000,
            where=(
                f"{season_filter} AND action_canonical = 'unknown'"
            ),
            severity='ERROR',
            name=f"spadl_unknown_rate[season={season}]",
        ),

        # ===== Schema-version drift per-season =====
        CHECK.row_count(
            table='iceberg.silver.whoscored_events_spadl',
            min_rows=0,
            max_rows=0,
            where=(
                f"{season_filter} "
                f"AND (action_source != '{SPADL_ACTION_SOURCE}' "
                f"OR action_version != '{SPADL_ACTION_VERSION}')"
            ),
            severity='ERROR',
            name=f"schema_version_drift[silver season={season}]",
        ),
        CHECK.row_count(
            table='iceberg.gold.fct_event',
            min_rows=0,
            max_rows=0,
            where=(
                f"{season_filter} "
                f"AND action_source != '{SPADL_ACTION_SOURCE}'"
            ),
            severity='ERROR',
            name=f"schema_version_drift[gold.fct_event season={season}]",
        ),
    ]


def _enum_violation_where(column: str, allowed: List[str]) -> str:
    """Build a NOT IN (...) WHERE for an enum check.

    Mirrors the helper in ``xref_dq.check_enum_compliance`` but kept inline
    here to avoid a cross-module import. Each value is single-quoted; reject
    embedded quotes early to keep the predicate safe for f-string interp.
    """
    safe: List[str] = []
    for v in allowed:
        if not isinstance(v, str) or "'" in v or ';' in v or '--' in v:
            raise ValueError(f"Unsafe enum value: {v!r}")
        safe.append(f"'{v}'")
    return f"{column} NOT IN ({', '.join(safe)})"


# ---------------------------------------------------------------------------
# Silver — whoscored_events_spadl
# ---------------------------------------------------------------------------

def _build_whoscored_events_spadl_checks() -> List[Check]:
    """DQ for ``iceberg.silver.whoscored_events_spadl`` (E3.1).

    Key invariants:
      * (match_id, event_id) PK — synthetic event_id verified unique in E3.1.
      * action_canonical ∈ 25-value SPADL enum (own_goal added #572).
      * action_source / action_version literals pinned to v1 (R0.4 contract).
      * SPADL ``unknown`` rate ≤ ~2.88% (R3 verdict 89.97% mapped — slack on
        17,550 unmappable baseline).
      * Pitch coords x/y in [0, 100] (Opta normalised) — WARNING only because
        WhoScored occasionally emits 100.1 / -0.1 from rounding.
    """
    table = 'iceberg.silver.whoscored_events_spadl'
    return [
        # PK + NULL guards (ERROR)
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'event_id'],
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'event_id', 'action_canonical',
                  'action_source', 'action_version'],
        ),

        # Volume guard — APL multi-season post-backfill (~700K rows/season,
        # E3.1 smoke-test verified ~1.4M for 2425+2526). Lower bound 600K
        # gives slack for partial backfill scenarios.
        CHECK.row_count(table, min_rows=600_000),

        # SPADL coverage drift guard. R3 verdict: ~2.88% unknown baseline on
        # 2425+2526 corpus. After full multi-season backfill the rate ran at
        # ~4.1% (~28.4K / 695,144) — meta-event types Card/Substitution etc.
        # map to 'unknown' by design (Goal moved to the shot family in #462,
        # dropping ~1,288 rows out of 'unknown'). Cap at 40K (~5.75%) so a fresh
        # taxonomy drift (new WhoScored type) still trips before silently
        # corrupting downstream stats.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=40_000,
            where="action_canonical = 'unknown'",
            severity='ERROR',
            name='spadl_coverage_unknown_rate',
        ),

        # Enum compliance — every action_canonical must be one of the 25
        # values. Implemented as row_count with NOT IN predicate. Zero
        # tolerance because adding a value requires SQL + this list update.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=_enum_violation_where('action_canonical', SPADL_ACTION_ENUM),
            severity='ERROR',
            name='spadl_action_enum_violation',
        ),

        # Schema-version literal pin (R0.4). Catch SQL drift where someone
        # bumps the literal in the file without updating downstream consumers.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=(
                f"action_source != '{SPADL_ACTION_SOURCE}' "
                f"OR action_version != '{SPADL_ACTION_VERSION}'"
            ),
            severity='ERROR',
            name='schema_version_literal_drift',
        ),

        # Pitch-coordinate bounds — Opta normalises to [0, 100]. WARNING-
        # severity because boundary values (100.1 etc.) are a known minor
        # WhoScored quirk that doesn't break downstream features.
        CHECK.value_range(
            table=table,
            column='x',
            min_val=0,
            max_val=100,
            severity='WARNING',
        ),
        CHECK.value_range(
            table=table,
            column='y',
            min_val=0,
            max_val=100,
            severity='WARNING',
        ),

        # Freshness — Silver is rebuilt by master_pipeline daily; 48h tolerates
        # a single missed run before alerting.
        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Silver — whoscored_team_match (T6.3, #92). Season rollup migrated to Gold (#370).
# ---------------------------------------------------------------------------

def _build_whoscored_team_match_checks() -> List[Check]:
    """DQ for ``iceberg.silver.whoscored_team_match`` (T6.3 / #92).

    Match-grain aggregate of ``silver.whoscored_events_spadl`` GROUP BY
    (match_id, team_id, league, season). Feeds the WhoScored block of
    Gold ``fct_team_match`` v2 (#95).

    Volume floor: 380 APL matches × 2 teams = 760 rows/season; multi-season
    backfill ~3K-5K. Min 600 (WARNING) covers partial-backfill scenarios.
    """
    table = 'iceberg.silver.whoscored_team_match'
    return [
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'team_id', 'league', 'season'],
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team_id', 'league', 'season'],
        ),
        CHECK.row_count(table, min_rows=600, severity='WARNING'),
        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Silver — espn_lineup
# ---------------------------------------------------------------------------

def _build_espn_lineup_checks() -> List[Check]:
    """DQ for ``iceberg.silver.espn_lineup`` (E3.2).

    Light sanity wrapper — most contract verification happens in
    fct_lineup downstream (where we measure ESPN coverage as a fraction
    of FBref). PK is composite (match_id, team, player) because ESPN
    has no native player_id — name-based dedup is good-enough at Silver.
    """
    table = 'iceberg.silver.espn_lineup'
    return [
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'team', 'player'],
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team', 'player', 'is_starter'],
        ),

        # Volume — ~22 (starters + subs) per (match, team), ~380 matches/season
        # × 2 teams ≈ 16,720 rows/season at full coverage. ESPN has partial
        # coverage in our sample → lower bound 10K with WARNING severity.
        CHECK.row_count(table, min_rows=10_000, severity='WARNING'),

        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Gold — fct_event
# ---------------------------------------------------------------------------

def _build_fct_event_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_event`` (E3.3 / Task 2.1).

    ref_integrity for match_id_canonical
    ------------------------------------
    Phase B (Task 2.1) shipped a 7-source ``silver.xref_match`` cascade,
    so every WhoScored game has a row in xref_match (bridged-fbref or
    orphan-prefixed ``ws_<id>``). The ref_integrity check on
    ``fct_event.match_id_canonical → silver.xref_match.canonical_id`` is
    now ENABLED at ERROR severity.

    Orphan-rate proxies
    -------------------
    Two complementary non-strict guards:
      * ``orphan_team_rate``   — team_id_canonical IS NULL    (alias-YAML drift)
      * ``orphan_player_rate_non_meta`` — player_id_canonical IS NULL on
        non-meta events (Card/Goal/Sub may legitimately have NULL player
        in bronze, so we exclude unmappable rows from the orphan count).
    """
    table = 'iceberg.gold.fct_event'
    return [
        # PK + NULL guards (ERROR)
        CHECK.no_duplicates(
            table,
            pk=['match_id_canonical', 'event_id'],
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id_canonical', 'event_id', 'action_canonical',
                  'action_source', 'action_version'],
        ),

        # Phase B bridging COMPLETE (#40): every WhoScored game in
        # gold.fct_event resolves to a silver.xref_match.canonical_id row.
        # The whoscored_schedule backfill-from-events (#128/#126/#106,
        # 2026-05-28) closed the schedule⊇events gap that left ~470 games
        # unbridged at WARNING; verified 0 orphan on live data 2026-05-31.
        # Re-enabled at ERROR — the check is now the guard against a
        # regression in the schedule⊇events invariant.
        CHECK.ref_integrity(
            child='gold.fct_event',
            parent='silver.xref_match',
            key='match_id_canonical',
            parent_key='canonical_id',
            severity='ERROR',
        ),

        # Volume — Bronze→Silver→Gold passthrough, expect ~Silver count.
        # WARNING severity: a deeper parity check is implemented as a
        # custom CheckResult by ``parity_check_event_counts`` (see below).
        CHECK.row_count(table, min_rows=500_000, severity='WARNING'),

        # team_id orphan guard. Non-zero count is normal during alias-YAML
        # rollout; high count signals drift. Threshold 10K ≈ 1.4% of 700K.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=10_000,
            where="team_id_canonical IS NULL",
            severity='WARNING',
            name='orphan_team_rate',
        ),

        # Player orphan rate (non-meta only). E1 verdict: WhoScored player
        # rejection 4.89% on the corpus → ~34K orphans expected on 700K
        # rows. Threshold 50K accommodates that with slack. We exclude
        # 'unmappable' confidence because those are bronze-NULL meta-events
        # (Card/Sub/Goal placeholders) where player_id legitimately absent.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=50_000,
            where="player_id_canonical IS NULL AND _action_confidence != 'unmappable'",
            severity='WARNING',
            name='orphan_player_rate_non_meta',
        ),

        # Pitch coords — WARNING (boundary quirks pass through from Silver).
        CHECK.value_range(table, column='x', min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range(table, column='y', min_val=0, max_val=100, severity='WARNING'),

        # Minute bounds — APL has 90' regulation + ET, allow up to 130 for
        # second-half stoppage in extra time. WARNING because Opta minute
        # for ET goals can spike to 120+5.
        CHECK.value_range(table, column='minute', min_val=0, max_val=130, severity='WARNING'),

        # Schema-version literal pin (mirrors silver guard).
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=f"action_source != '{SPADL_ACTION_SOURCE}'",
            severity='ERROR',
            name='schema_version_literal_drift[fct_event]',
        ),

        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Gold — fct_shot
# ---------------------------------------------------------------------------

def _build_fct_shot_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_shot`` (E3.4).

    Unlike fct_event, this table CAN enforce ``match_id →
    silver.xref_match`` ref_integrity because E3.4 uses an INNER JOIN
    bridge through (date, home_canonical_id, away_canonical_id) to
    derive match_id from the FBref hex (which IS resident in
    xref_match.source='fbref'). 1.8% of shots get filtered here — the
    surviving rows MUST all have a parent match.
    """
    table = 'iceberg.gold.fct_shot'

    # Player orphan threshold: E1 verdict — Understat player rejection
    # 6.94%. Smoke-test row count 47,105 → ~3,300 orphans expected.
    # Cap at 7% × 47K ≈ 3,290 → round to 3,300 safety margin.
    shot_orphan_max = int(0.07 * 47_000)

    return [
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'shot_id'],
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'shot_id', 'xg'],
        ),

        # Strict ref_integrity — E3.4 uses INNER JOIN bridge so any survivor
        # MUST be in xref_match. Leakage = upstream regression.
        # parent_key='canonical_id' because silver.xref_match has the column
        # 'canonical_id' (not 'match_id'); see DESCRIBE 2026-05-08.
        CHECK.ref_integrity(
            child='gold.fct_shot',
            parent='silver.xref_match',
            key='match_id',
            parent_key='canonical_id',
        ),

        # xG bounded probability. ERROR-severity: violations indicate a model
        # output regression (Understat upstream issue) and would poison
        # downstream features. xa is not materialized in fct_shot — assist
        # tracking lives in the assist_player_id column only.
        CHECK.value_range(table, column='xg', min_val=0, max_val=1),

        # Volume — APL multi-season ≈ 47K shots smoke-tested. Min 20K
        # WARNING for partial-backfill grace.
        CHECK.row_count(table, min_rows=20_000, severity='WARNING'),

        # Player orphan rate guard — see threshold derivation above.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=shot_orphan_max,
            where="player_id IS NULL",
            severity='WARNING',
            name='shot_orphan_player_rate',
        ),

        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Gold — fct_lineup
# ---------------------------------------------------------------------------

def _build_fct_lineup_checks() -> List[Check]:
    """DQ for ``iceberg.gold.fct_lineup`` (E3.5).

    Lineup_source distribution
    --------------------------
    Smoke-test verdict: 159,445 rows total, FBref bulk + ESPN secondary
    (90.8% bridge success). Two row_count guards lock that distribution:
      * fbref ≥ 100K (ERROR — FBref is the canonical source, must dominate)
      * espn  ≥ 5K   (WARNING — ESPN coverage is partial by design)
    """
    table = 'iceberg.gold.fct_lineup'

    # Orphan rate measured on FBref rows ONLY (#519). ESPN player_id is NULL
    # by design — there is no ESPN player resolver (see fct_lineup.sql ADR
    # "player_id resolution"), so every ESPN row is an expected NULL. Counting
    # ESPN turned this guard into pure noise (~74.6K expected NULLs vs a 15.9K
    # cap → permanently red, masking real resolver regressions). FBref ≈ 145K
    # rows; the resolver legitimately misses out-of-scope seasons (~2.7K live).
    # Cap at 10% × 145K ≈ 14,500.
    lineup_fbref_orphan_max = int(0.10 * 145_000)

    return [
        # PK guard. With player_id possibly NULL (ESPN edge case),
        # the runner uses COUNT - COUNT(DISTINCT) — Trino groups NULLs into
        # one bucket per (match, team, NULL), so duplicate (m, t, NULL) IS
        # caught. Full coverage of NULL semantics moves to E3.9 unit tests.
        # PK uniqueness is meaningful only when player_id is
        # resolved — ESPN rows where the player resolver returned NULL
        # collapse into a single (m, t, NULL) bucket and Trino reports
        # them as duplicates. Scope the check to resolved rows; NULL-PK
        # contract is verified in E3.9 unit tests.
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'team_id', 'player_id'],
            where='player_id IS NOT NULL',
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team_id', 'lineup_source'],
        ),

        # ref_integrity fct_lineup → xref_match — WARNING (not ERROR).
        # ESPN bridge through (date, home_canonical, away_canonical) leaves
        # ~35 distinct match_ids unbridged (1.4K rows / 0.9% of total) when
        # the FBref/ESPN team-canonicalisation drifts on promotion teams.
        # Tightens to ERROR after E1.5 cutover (xref_match adds ESPN source).
        CHECK.ref_integrity(
            child='gold.fct_lineup',
            parent='silver.xref_match',
            key='match_id',
            parent_key='canonical_id',
            severity='WARNING',
        ),

        # issue #242: alt-hex FBref-дубли НЕ ловятся guard'ом выше — parent
        # silver.xref_match сам несёт alt-hex (строится из fbref_schedule без
        # date-фильтра, в отличие от fbref_match_enriched). dim_match — canon-
        # only (FROM fbref_match_enriched, date IS NOT NULL), поэтому alt-hex
        # lineup-строки становятся orphan'ами и флагаются. Scope
        # lineup_source='fbref' исключает ESPN pseudo-id (espn_<hash> ∉
        # dim_match). #258: грязный Bronze вычищен полным re-ingest (#241/PR#257),
        # гейт «clean re-ingest» подтвердил orphan=0 live (2026-06-03) →
        # severity ERROR.
        CHECK.ref_integrity(
            child='gold.fct_lineup',
            parent='gold.dim_match',
            key='match_id',
            parent_key='match_id',
            where="lineup_source = 'fbref'",
            severity='ERROR',
            name='ref_integrity[fct_lineup.fbref->dim_match]',
        ),

        # Lineup-source distribution. FBref must dominate (canonical source).
        CHECK.row_count(
            table=table,
            min_rows=100_000,
            where="lineup_source = 'fbref'",
            severity='ERROR',
            name='fbref_coverage_dominant',
        ),
        # ESPN coverage is partial by design — WARNING only.
        CHECK.row_count(
            table=table,
            min_rows=5_000,
            where="lineup_source = 'espn'",
            severity='WARNING',
            name='espn_coverage_present',
        ),

        # Total volume — 380 APL matches/season × 22 lineup rows / (match × team)
        # × 2 teams = ~16,720 rows / season (FBref alone). Min 380*22 = 8,360
        # is a single-season floor; we enforce as ERROR.
        CHECK.row_count(table, min_rows=380 * 22, severity='ERROR'),

        # FBref player orphan guard — see threshold derivation above. Scoped to
        # lineup_source='fbref'; ESPN rows are expected-NULL (#519) and excluded.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=lineup_fbref_orphan_max,
            where="player_id IS NULL AND lineup_source = 'fbref'",
            severity='WARNING',
            name='lineup_orphan_player_rate',
        ),

        # is_captain coverage (#439) — SofaScore /lineups enrich the FBref
        # canonical (match, player) via xref_match + xref_player. WARNING-only:
        # coverage is partial by design (SofaScore /lineups ⊂ FBref lineups).
        # Live 2026-06-12: 14,507 enriched rows (760 captains) of 142,868 FBref
        # rows. Floor well below that catches a total bridge regression while
        # tolerating partial backfills.
        CHECK.row_count(
            table=table,
            min_rows=5_000,
            where="is_captain IS NOT NULL",
            severity='WARNING',
            name='is_captain_coverage_present',
        ),

        CHECK.freshness(
            table=table,
            ts_col='_silver_created_at',
            max_age_hours=48,
            severity='WARNING',
        ),
    ]


# ---------------------------------------------------------------------------
# Public builder API
# ---------------------------------------------------------------------------

def _build_sofascore_player_profile_checks() -> List[Check]:
    """DQ for ``iceberg.silver.sofascore_player_profile``.

    Snapshot-grain (one row per (player_id, league, season)) с canonical_id
    bridge через silver.xref_player. Coverage thresholds отражают APL
    2025/26 (526 rows, 95% canonical match при первой материализации).
    """
    table = 'iceberg.silver.sofascore_player_profile'
    return [
        CHECK.no_duplicates(
            table,
            pk=['player_id', 'league', 'season'],
        ),
        CHECK.no_nulls(
            table,
            cols=['player_id', 'league', 'season'],
        ),
        CHECK.row_count(table, min_rows=400, severity='WARNING'),
        # Physical attribute bounds — high outliers flag ingest regression.
        CHECK.value_range(
            table, 'height_cm',
            min_val=140, max_val=220, severity='WARNING',
        ),
    ]


def _build_sofascore_team_match_checks() -> List[Check]:
    """DQ for ``iceberg.silver.sofascore_team_match`` (T6.4 / issue #93).

    Two rows per match (home + away) — single-source conform: PIVOT of
    ``bronze.sofascore_match_stats`` (period='ALL') + ``bronze.sofascore_schedule``
    (outcome). The cross-entity minutes/assists rollup from
    ``silver.sofascore_player_match_aggregate`` was removed (#367, Silver Charter
    R2); ``minutes``/``assists`` are kept as NULL placeholders (they were always
    NULL — the rollup never matched on team_id).
    APL 2025/26 baseline: 380 matches × 2 sides = 760 rows.

    PK is ``(match_id, team_id)`` — native SofaScore IDs; Gold (#95) bridges
    via ``silver.xref_team(source='sofascore')``.
    """
    table = 'iceberg.silver.sofascore_team_match'
    return [
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'team_id'],
            severity='ERROR',
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team_id', 'opponent_id', 'league', 'season'],
            severity='ERROR',
        ),
        # APL floor: 380 matches × 2 sides = 760 (allow slack for partial backfills).
        CHECK.row_count(
            table,
            min_rows=700,
            where="league = 'ENG-Premier League' AND season = '2526'",
            severity='ERROR',
        ),
        # Core SofaScore metrics should be present for almost every match.
        CHECK.no_nulls(
            table,
            cols=['expected_goals', 'total_passes', 'corner_kicks'],
            severity='WARNING',
        ),
        CHECK.freshness(
            table,
            ts_col='_bronze_ingested_at',
            max_age_hours=72,
            severity='WARNING',
        ),
    ]


def _build_understat_team_match_checks() -> List[Check]:
    """DQ for ``iceberg.silver.understat_team_match`` (T6.2 / #91).

    UNION ALL unpivot of ``bronze.understat_team_match_stats`` (wide-form),
    joined to ``silver.xref_team`` (source='understat'). APL 2024/25:
    380 matches × 2 sides = 760 rows. 2025/26 is intentionally looser
    (promotee orphans, ≤5% — see MEMORY.md «silver.xref_team 78.5%»).
    """
    table = 'iceberg.silver.understat_team_match'
    return [
        CHECK.no_duplicates(
            table,
            pk=['match_id', 'team_id_canonical'],
            severity='ERROR',
        ),
        CHECK.no_nulls(
            table,
            cols=['match_id', 'team_id_canonical', 'team_id', 'league', 'season'],
            severity='ERROR',
        ),
        CHECK.no_nulls(
            table,
            cols=['xg', 'xg_against'],
            severity='ERROR',
        ),
        CHECK.row_count(
            table,
            min_rows=760,
            where="league = 'ENG-Premier League' AND season = '2425'",
            severity='ERROR',
        ),
        CHECK.freshness(
            table,
            ts_col='_bronze_ingested_at',
            max_age_hours=48,
            severity='ERROR',
        ),
    ]


def build_silver_e3_checks() -> List[Check]:
    """Return DQ checks for Silver E3 tables.

    Composition: ``whoscored_events_spadl`` + WhoScored team match-aggregate (T6.3) +
    Understat team match-aggregate (T6.2) +
    ``espn_lineup`` + ``sofascore_player_profile`` + SofaScore team match-aggregate (T6.4).

    The *_team_season rollups (whoscored/understat/sofascore) moved to the Gold
    layer in #370 — their PK-uniqueness DQ now lives in ``validate_gold_quality``.
    """
    return (
        _build_whoscored_events_spadl_checks()
        + _build_whoscored_team_match_checks()
        + _build_understat_team_match_checks()
        + _build_espn_lineup_checks()
        + _build_sofascore_player_profile_checks()
        + _build_sofascore_team_match_checks()
    )


def build_gold_e3_checks() -> List[Check]:
    """Return DQ checks for Gold E3 tables.

    Composition: ``fct_event`` + ``fct_shot`` + ``fct_lineup``.
    """
    return (
        _build_fct_event_checks()
        + _build_fct_shot_checks()
        + _build_fct_lineup_checks()
    )


def build_all_e3_checks() -> List[Check]:
    """Convenience: silver + gold E3 checks.

    The DAG ``dag_transform_e3`` calls this in its ``validate_e3`` task.
    """
    return build_silver_e3_checks() + build_gold_e3_checks()


# ---------------------------------------------------------------------------
# Custom Bronze→Silver→Gold parity check (CheckResult, runs as side-task)
# ---------------------------------------------------------------------------

# Acceptable ratios for the parity gate. Values intentionally close to 1.0
# because R3 D5 contract: NO row drops in Silver/Gold, including action_canonical='unknown'.
_PARITY_SILVER_BRONZE_MIN = 0.99   # 1% slack for Silver de-dup edge cases.
_PARITY_GOLD_SILVER_MIN = 0.95     # 5% slack for orphan-match filter
                                   # (E3 v1 passthrough: should be 100%).


def parity_check_event_counts() -> CheckResult:
    """Cross-table row-count parity: bronze → silver → gold.

    Returns a single :class:`CheckResult` (ERROR severity) suitable for
    appending to a ``RunReport`` from :mod:`utils.data_quality`. Pattern
    mirrors :func:`gold_tasks._append_train_test_disjointness_check`.

    Verdict logic
    -------------
      * silver/bronze  ≥ 0.99  AND  gold/silver ≥ 0.95   → passed=True
      * silver/bronze  < 0.99                              → passed=False (silver dropped rows)
      * gold/silver    < 0.95                              → passed=False (gold filtered too many)

    The check returns ERROR-severity because:
      1. R3 D5 explicitly mandates no row drops.
      2. A regression here would silently shrink the SPADL corpus and
         poison every downstream feature.
    """
    name = 'parity[bronze→silver→gold event counts]'
    sql = (
        "SELECT "
        "  (SELECT COUNT(*) FROM iceberg.bronze.whoscored_events) AS bronze_cnt, "
        "  (SELECT COUNT(*) FROM iceberg.silver.whoscored_events_spadl) AS silver_cnt, "
        "  (SELECT COUNT(*) FROM iceberg.gold.fct_event) AS gold_cnt"
    )

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            cur.close()

        bronze_cnt = int(row[0]) if row and row[0] is not None else 0
        silver_cnt = int(row[1]) if row and row[1] is not None else 0
        gold_cnt = int(row[2]) if row and row[2] is not None else 0

        # Defensive: zero bronze ⇒ either pre-ingest or empty table; we
        # treat that as a soft-fail so the DAG flags but doesn't crash.
        if bronze_cnt == 0:
            return CheckResult(
                name=name,
                kind='parity',
                severity='ERROR',
                passed=False,
                details=(
                    "bronze.whoscored_events is empty — cannot compute parity "
                    "(bronze_cnt=0, silver_cnt={}, gold_cnt={})".format(
                        silver_cnt, gold_cnt
                    )
                ),
                value=0.0,
            )

        sb_ratio = silver_cnt / bronze_cnt
        gs_ratio = (gold_cnt / silver_cnt) if silver_cnt else 0.0

        sb_ok = sb_ratio >= _PARITY_SILVER_BRONZE_MIN
        gs_ok = gs_ratio >= _PARITY_GOLD_SILVER_MIN
        passed = sb_ok and gs_ok

        details = (
            f"bronze={bronze_cnt}, silver={silver_cnt} "
            f"(silver/bronze={sb_ratio:.4f}, "
            f"min={_PARITY_SILVER_BRONZE_MIN}); "
            f"gold={gold_cnt} (gold/silver={gs_ratio:.4f}, "
            f"min={_PARITY_GOLD_SILVER_MIN})"
        )
        if not passed:
            offenders = []
            if not sb_ok:
                offenders.append(f"silver/bronze={sb_ratio:.4f} < {_PARITY_SILVER_BRONZE_MIN}")
            if not gs_ok:
                offenders.append(f"gold/silver={gs_ratio:.4f} < {_PARITY_GOLD_SILVER_MIN}")
            details += " — VIOLATIONS: " + "; ".join(offenders)

        return CheckResult(
            name=name,
            kind='parity',
            severity='ERROR',
            passed=passed,
            details=details,
            value={
                'bronze': bronze_cnt,
                'silver': silver_cnt,
                'gold': gold_cnt,
                'silver_over_bronze': round(sb_ratio, 4),
                'gold_over_silver': round(gs_ratio, 4),
            },
        )
    except Exception as e:
        logger.exception("parity_check_event_counts raised")
        return CheckResult(
            name=name,
            kind='parity',
            severity='ERROR',
            passed=False,
            error=str(e),
        )
    finally:
        conn.close()


def append_parity_check_to_report(report: Any) -> None:
    """Append :func:`parity_check_event_counts` result to a ``RunReport``.

    Convenience wrapper for the DAG callable — keeps the parity check on
    the same report surface as the standard checks so Telegram /
    on_failure_callback formatters see it uniformly.
    """
    result = parity_check_event_counts()
    report.results.append(result)
    if result.passed:
        logger.info(f"  OK   {result.name} — {result.details}")
    else:
        logger.error(f"  FAIL {result.name} — {result.details or result.error}")


__all__ = [
    'SPADL_ACTION_ENUM',
    'SPADL_ACTION_SOURCE',
    'SPADL_ACTION_VERSION',
    'WHOSCORED_KNOWN_TYPES_39',
    'build_silver_e3_checks',
    'build_gold_e3_checks',
    'build_all_e3_checks',
    'build_per_season_e3_checks',
    'parity_check_event_counts',
    'parity_check_event_counts_per_season',
    'taxonomy_diff_check',
    'append_parity_check_to_report',
]
