"""
Gold Transformation Tasks
==========================

Thin wrapper around ``silver_tasks.run_silver_transform`` — same CTAS engine,
just targets ``iceberg.gold.*``. Defined separately to keep Gold-specific
quality checks (point-in-time leakage, uniqueness by composite PK) isolated.

Use ``import trino`` directly like silver_tasks.py — avoids loading the
heavyweight ``scrapers/__init__.py`` in Airflow workers (~1.5 GB RAM).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from utils.silver_tasks import check_bronze_table_exists, run_silver_transform

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Point-in-time leakage protection — rolling-feature column registries
# ---------------------------------------------------------------------------
# Each feat_* table masks rolling columns to NULL for the first N rows per
# partition (see CASE WHEN match_rn > N in dags/sql/gold/feat_*.sql). DQ
# verifies that mask is intact by counting non-NULL values where rn <= N.
#
# Keep these lists in sync with the SELECT lists of the corresponding SQL
# files. New rolling column added in SQL? Add it here too — otherwise a
# regression that drops the CASE-WHEN mask ships silently.
#
# Columns are grouped by SQL file. (table, partition_by, order_by, skip_n)
# is shared across all columns in a group.

# feat_team_form — partition (team_id, season) ORDER BY date, mask match_rn > 5
FEAT_TEAM_FORM_ROLLING_COLS = [
    # T1.x baseline averages
    'l5_goals_for_avg',
    'l5_goals_against_avg',
    'l5_shots_avg',
    'l5_sot_avg',
    'l5_possession_avg',
    'l5_form_points',
    'l5_wins',
    'l5_losses',
    'l5_draws',
    # T3.3 volatility + trend
    'l5_goals_for_std',
    'l5_goals_against_std',
    'l5_points_std',
    'l5_form_trend',
]

# feat_team_h2h — partition (team_id, opponent_id) ORDER BY date, mask h2h_rn > 1
# NB: NO season in partition — head-to-head is a cross-season relationship.
# skip_first_n=1 (not 5) because h2h has at most ~2 matches/season per pair.
FEAT_TEAM_H2H_ROLLING_COLS = [
    'h2h_goals_diff_avg',
    'h2h_goals_for_avg',
    'h2h_goals_against_avg',
    'h2h_wins',
    'h2h_losses',
    'h2h_draws',
]

# feat_team_xg_form — partition (team_id, season) ORDER BY match_date, mask match_rn > 5
# Both L5 and L10 columns share skip_first_n=5 — see SQL header for trade-off
# rationale (APL 38-match seasons make >10 mask too restrictive).
FEAT_TEAM_XG_FORM_ROLLING_COLS = [
    # L5
    'xg_for_l5_avg',
    'xg_against_l5_avg',
    'xg_diff_l5_avg',
    'psxg_for_l5_avg',
    'psxg_against_l5_avg',
    'psxg_diff_l5_avg',
    # L10
    'xg_for_l10_avg',
    'xg_against_l10_avg',
    'xg_diff_l10_avg',
    'psxg_for_l10_avg',
    'psxg_against_l10_avg',
    'psxg_diff_l10_avg',
]

# feat_player_form — partition (player_id, season) ORDER BY match_id, mask appearance_rn > 5
FEAT_PLAYER_FORM_ROLLING_COLS = [
    'l5_minutes_avg',
    'l5_goals_avg',
    'l5_assists_avg',
    'l5_shots_avg',
    'l5_sot_avg',
    'l5_goals_sum',
    'l5_assists_sum',
    'l5_yellows_sum',
    'l5_reds_sum',
]


def run_gold_transform(
    sql_file: str,
    table_name: str,
    partition_columns: Optional[List[str]] = None,
    fallback_sql_file: Optional[str] = None,
    require_silver: Optional[List[str]] = None,
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """Run a Gold-layer CTAS.

    Delegates to ``run_silver_transform`` with ``schema='gold'``. Same
    DROP+CTAS flow, same connection settings, same partitioning API.

    Optional graceful-degrade mode for transforms that depend on optional
    Silver tables (e.g. ``feat_team_xg_form`` requires ``silver.fbref_shot_events``,
    which may be absent in MVP environments where the Bronze ``fbref_shot_events``
    isn't ingested yet).

    Args:
        fallback_sql_file: Alternative SQL to run when any of ``require_silver``
            is missing. Must produce an identical schema to ``sql_file`` so
            downstream JOINs keep resolving (typically NULL placeholders).
        require_silver: List of Silver table names (without schema prefix) that
            ``sql_file`` reads from. If any is absent in ``iceberg.silver``,
            ``fallback_sql_file`` is used instead. ``None`` (default) skips
            the existence check entirely.

    Returns:
        Same dict as ``run_silver_transform``. When fallback fires, the dict
        has ``status='success'`` and an extra ``fallback=True`` key so the
        caller / Airflow log makes the degraded state obvious.

    Note on ``partition_columns``:
        Unlike ``run_silver_transform`` (which silently defaults to
        ``['league', 'season']`` when ``None`` is passed), Gold honours
        ``None`` as **no partitioning** — required for global dims
        (``dim_venue``, ``dim_referee``, ``dim_competition``, ``dim_season``)
        whose row count is too small to justify partitioning, and whose
        schema may not even contain ``league``/``season`` columns.
    """
    if partition_columns is None:
        partition_columns = []

    if fallback_sql_file and require_silver:
        missing = [
            t for t in require_silver
            if not check_bronze_table_exists(table_name=t, schema='silver')
        ]
        if missing:
            logger.warning(
                "Gold transform '%s': required Silver table(s) %s not found — "
                "falling back to '%s' (NULL placeholders for downstream contract).",
                table_name, missing, fallback_sql_file,
            )
            result = run_silver_transform(
                sql_file=fallback_sql_file,
                table_name=table_name,
                schema='gold',
                partition_columns=partition_columns,
                add_timestamp=add_timestamp,
            )
            result['fallback'] = True
            result['fallback_reason'] = f"missing silver tables: {missing}"
            return result

    return run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='gold',
        partition_columns=partition_columns,
        add_timestamp=add_timestamp,
    )


def _append_train_test_disjointness_check(report) -> None:
    """Append a disjointness CheckResult for fct_match_train vs fct_match_test.

    WHY a custom Trino query: the universal CHECK registry does not (yet)
    expose a cross-table INNER-JOIN-COUNT primitive. The check is small,
    deterministic and important enough to inline here.

    Mutates ``report.results`` in place — same dataclass shape as run_checks().
    """
    from utils.data_quality import CheckResult, _get_conn

    name = 'disjointness[fct_match_train ∩ fct_match_test]'
    sql = (
        "SELECT COUNT(*) FROM iceberg.gold.fct_match_train tr "
        "INNER JOIN iceberg.gold.fct_match_test te "
        "ON tr.match_id = te.match_id"
    )

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            cur.close()
        overlap = row[0] if row else 0
        report.results.append(CheckResult(
            name=name,
            kind='disjointness',
            severity='ERROR',
            passed=(overlap == 0),
            details=f"{overlap} match_id(s) appear in BOTH train and test",
            value=overlap,
        ))
    except Exception as e:
        report.results.append(CheckResult(
            name=name,
            kind='disjointness',
            severity='ERROR',
            passed=False,
            error=str(e),
        ))
        logger.exception("disjointness check raised")
    finally:
        conn.close()


def _append_dim_standings_coverage_check(report) -> None:
    """E2: append a two-tier coverage CheckResult for dim_standings.

    Measures the fraction of standings rows whose team_id was resolved via
    the canonical resolver (``team_id_source = 'fbref_canonical'``) vs the
    fallback (``'sofascore_orphan'``). Uses two-tier severity:

      * ``coverage >= 95%`` -> OK
      * ``50% <= coverage < 95%`` -> WARNING (drop in resolver match-rate)
      * ``coverage < 50%`` -> ERROR-grade signal, but the check is wired as
        WARNING per the E2 spec (orphans are tracked, not blocking).

    Implemented inline (mirroring ``_append_train_test_disjointness_check``)
    because the universal CHECK registry has no two-tier ``coverage``
    primitive yet — see CLAUDE.md Gold/DQ section. When ``coverage()``
    lands in ``data_quality.py`` this helper should be folded into the
    main check list.
    """
    from utils.data_quality import CheckResult, _get_conn

    name = "coverage[dim_standings.team_id_source='fbref_canonical']"
    sql = (
        "SELECT "
        "  COUNT(*) AS total, "
        "  COUNT_IF(team_id_source = 'fbref_canonical') AS resolved "
        "FROM iceberg.gold.dim_standings"
    )

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            cur.close()
        total, resolved = (row[0], row[1]) if row else (0, 0)
        ratio = (resolved / total) if total else 0.0
        ratio_pct = round(ratio * 100, 2)

        if total == 0:
            # No standings yet — surfaced separately by row_count check.
            passed = True
            details = "dim_standings is empty — coverage skipped"
        elif ratio >= 0.95:
            passed = True
            details = (
                f"resolved={resolved}/{total} ({ratio_pct}%) >= 95% — OK"
            )
        elif ratio >= 0.50:
            passed = False
            details = (
                f"resolved={resolved}/{total} ({ratio_pct}%) in [50%, 95%) — "
                "resolver match-rate degraded"
            )
        else:
            passed = False
            details = (
                f"resolved={resolved}/{total} ({ratio_pct}%) < 50% — "
                "resolver largely failing; check _team_aliases coverage"
            )

        report.results.append(CheckResult(
            name=name,
            kind='coverage',
            severity='WARNING',  # spec: WARNING-only — orphans are tracked
            passed=passed,
            details=details,
            value=ratio,
        ))
    except Exception as e:
        report.results.append(CheckResult(
            name=name,
            kind='coverage',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))
        logger.exception("dim_standings coverage check raised")
    finally:
        conn.close()


def validate_gold_quality() -> Dict[str, Any]:
    """Run Gold-layer DQ checks — PK uniqueness, ref integrity, point-in-time.

    Raises AirflowException if any ERROR-severity check fails. WARNING-level
    checks are logged but do not fail the DAG.
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    checks = [
        # ========== PK uniqueness — ERROR ==========
        CHECK.no_duplicates('gold.dim_match',        pk=['match_id']),
        CHECK.no_duplicates('gold.dim_team',         pk=['team_id', 'season']),
        CHECK.no_duplicates('gold.dim_player',       pk=['player_id', 'season']),
        CHECK.no_duplicates('gold.fct_team_match',   pk=['match_id', 'team_id']),
        CHECK.no_duplicates('gold.fct_player_match', pk=['match_id', 'player_id']),
        CHECK.no_duplicates('gold.fct_match',        pk=['match_id']),
        CHECK.no_duplicates('gold.feat_team_form',    pk=['match_id', 'team_id']),
        CHECK.no_duplicates('gold.feat_team_xg_form', pk=['match_id', 'team_id']),
        CHECK.no_duplicates('gold.feat_player_form',  pk=['match_id', 'player_id']),
        CHECK.no_duplicates('gold.match_outcomes',    pk=['match_id']),
        # T4.1: ML splits — match_id is the PK in both tables.
        CHECK.no_duplicates('gold.fct_match_train',   pk=['match_id']),
        CHECK.no_duplicates('gold.fct_match_test',    pk=['match_id']),

        # ========== No NULLs in PKs — ERROR ==========
        CHECK.no_nulls('gold.dim_match',       cols=['match_id', 'date']),
        CHECK.no_nulls('gold.fct_team_match',  cols=['match_id', 'team_id', 'opponent_id']),
        CHECK.no_nulls('gold.fct_match',       cols=['match_id', 'home_team_id', 'away_team_id']),
        # match_outcomes is the source-of-truth for ML labels — PK + temporal
        # keys MUST be present, otherwise downstream backtests silently misalign.
        CHECK.no_nulls('gold.match_outcomes',  cols=['match_id', 'season', 'match_date']),
        # feat_team_xg_form keys/temporal columns — required for honest joins / windowing.
        CHECK.no_nulls('gold.feat_team_xg_form',
                       cols=['match_id', 'team_id', 'season', 'match_date']),
        # T4.1: ML splits — PK + season partition key + temporal column + the
        # primary classification target MUST all be present (the split only
        # contains completed matches, so result_1x2 cannot be NULL).
        # NB: in fct_match the temporal column is `date` (inherited from dim_match),
        # not `match_date` (which is the name in match_outcomes).
        CHECK.no_nulls('gold.fct_match_train',
                       cols=['match_id', 'season', 'date', 'result_1x2']),
        CHECK.no_nulls('gold.fct_match_test',
                       cols=['match_id', 'season', 'date', 'result_1x2']),

        # ========== Referential integrity — ERROR ==========
        CHECK.ref_integrity('gold.fct_team_match',   'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.fct_player_match', 'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.fct_match',        'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.feat_team_form',    'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.feat_team_xg_form', 'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.match_outcomes',    'gold.dim_match', 'match_id'),
        # T4.1: ML splits must trace back to dim_match (and through it, to Silver).
        CHECK.ref_integrity('gold.fct_match_train',   'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.fct_match_test',    'gold.dim_match', 'match_id'),

        # ========== Point-in-time correctness — ERROR (guard against leakage) ==========
        # For first N matches of the partition every rolling feature MUST be NULL.
        # Anything else means future data leaked into the feature window — silently
        # inflated training metrics, broken ML reliability. Severity stays ERROR
        # so the DAG fails before Gold ships features to the model.
        #
        # Column lists live in module-level constants (FEAT_*_ROLLING_COLS) so the
        # registry stays explicit (no SQL parsing magic) but adding a column is a
        # one-line change. T3.4 closed coverage gaps: previously only a sample of
        # rolling cols was checked, now every masked column is enforced.
        *(
            CHECK.point_in_time(
                'gold.feat_team_form',
                feature_col=col,
                partition_by=['team_id', 'season'],
                order_by='date',
                skip_first_n=5,
            )
            for col in FEAT_TEAM_FORM_ROLLING_COLS
        ),
        # h2h: partition is (team_id, opponent_id) — h2h is cross-season; mask
        # is h2h_rn > 1 (first encounter has no prior). skip_first_n=1.
        *(
            CHECK.point_in_time(
                'gold.feat_team_h2h',
                feature_col=col,
                partition_by=['team_id', 'opponent_id'],
                order_by='date',
                skip_first_n=1,
            )
            for col in FEAT_TEAM_H2H_ROLLING_COLS
        ),
        # xG / PSxG rolling features (L5 + L10).
        # SQL masks both L5 and L10 features at match_rn > 5 (deliberate trade-off:
        # an APL season has 38 matches; demanding 10 prior would null ~26% of rows).
        # So skip_first_n=5 applies uniformly to BOTH window sizes.
        *(
            CHECK.point_in_time(
                'gold.feat_team_xg_form',
                feature_col=col,
                partition_by=['team_id', 'season'],
                order_by='match_date',
                skip_first_n=5,
            )
            for col in FEAT_TEAM_XG_FORM_ROLLING_COLS
        ),
        *(
            CHECK.point_in_time(
                'gold.feat_player_form',
                feature_col=col,
                partition_by=['player_id', 'season'],
                order_by='match_id',
                skip_first_n=5,
            )
            for col in FEAT_PLAYER_FORM_ROLLING_COLS
        ),

        # ========== Value ranges — WARNING ==========
        CHECK.value_range('gold.fct_team_match', 'goals_for',  min_val=0, max_val=20,
                          severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'possession', min_val=0, max_val=100,
                          severity='WARNING'),
        CHECK.value_range('gold.fct_match',      'total_goals', min_val=0, max_val=20,
                          severity='WARNING'),
        # Targets sanity — only meaningful for completed matches; outliers
        # outside [0, 20] indicate parser regression in Silver score extraction.
        CHECK.value_range('gold.match_outcomes', 'total_goals', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),
        CHECK.value_range('gold.match_outcomes', 'home_score', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),
        CHECK.value_range('gold.match_outcomes', 'away_score', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),
        # T3.3: volatility / trend sanity bounds (WARNING — domain heuristics)
        # Std-dev of goals over 5 matches very rarely exceeds 5 in real data.
        CHECK.value_range('gold.feat_team_form', 'l5_goals_for_std',
                          min_val=0, max_val=5, severity='WARNING'),
        CHECK.value_range('gold.feat_team_form', 'l5_goals_against_std',
                          min_val=0, max_val=5, severity='WARNING'),
        # Points std bounded by max swing over 5 games (~1.6 in extreme cases).
        CHECK.value_range('gold.feat_team_form', 'l5_points_std',
                          min_val=0, max_val=5, severity='WARNING'),
        # Slope = points/match. Empirically stays within +/- 1.5 even for
        # dramatic form swings (3 -> 0 across 5 matches gives ~ -0.6 slope).
        CHECK.value_range('gold.feat_team_form', 'l5_form_trend',
                          min_val=-1.5, max_val=1.5, severity='WARNING'),
        # T3.2: xG sanity bounds. Single-match xG above ~6 is exceptional but
        # plausible (e.g. 8-0 routs); rolling AVG above 8 across 5 matches is
        # essentially impossible — if it appears something has gone wrong
        # in the shot_events parser. WARNING (not ERROR) since the bound is
        # a domain heuristic, not a hard invariant.
        CHECK.value_range('gold.feat_team_xg_form', 'xg_for_l5_avg',
                          min_val=0, max_val=8, severity='WARNING'),
        CHECK.value_range('gold.feat_team_xg_form', 'xg_against_l5_avg',
                          min_val=0, max_val=8, severity='WARNING'),
        CHECK.value_range('gold.feat_team_xg_form', 'xg_for_l10_avg',
                          min_val=0, max_val=8, severity='WARNING'),
        CHECK.value_range('gold.feat_team_xg_form', 'psxg_for_l5_avg',
                          min_val=0, max_val=8, severity='WARNING'),
        # xG diff is bounded by xG itself; +/- 8 over a rolling window is the
        # outer envelope (best APL team vs worst over 5 matches).
        CHECK.value_range('gold.feat_team_xg_form', 'xg_diff_l5_avg',
                          min_val=-8, max_val=8, severity='WARNING'),

        # ============================================================
        # E2: master-data dims (dim_venue / dim_referee / dim_standings /
        # dim_competition / dim_season). Mirrors the existing dim_match /
        # dim_team / dim_player block but adapted to the E2 PK shapes and
        # the R0.4 (_canonical, _source, _version) schema-versioning trio.
        # ============================================================

        # ----- E2: PK uniqueness — ERROR -----
        CHECK.no_duplicates('gold.dim_venue',       pk=['venue_id']),
        CHECK.no_duplicates('gold.dim_referee',     pk=['referee_id']),
        # Composite PK — one standings row per (league, season, team).
        CHECK.no_duplicates('gold.dim_standings',   pk=['league', 'season', 'team_id']),
        CHECK.no_duplicates('gold.dim_competition', pk=['competition_id']),
        CHECK.no_duplicates('gold.dim_season',      pk=['season_id']),

        # ----- E2: NOT NULL on PKs + critical attrs — ERROR -----
        CHECK.no_nulls('gold.dim_venue',       cols=['venue_id', 'venue_canonical']),
        CHECK.no_nulls('gold.dim_referee',     cols=['referee_id', 'referee_canonical']),
        # dim_standings has no canonical column — its source-tracking is via
        # team_id_source (covered by the coverage check below). Here we just
        # guarantee the PK trio + the load-bearing numeric attrs are present.
        CHECK.no_nulls('gold.dim_standings',
                       cols=['league', 'season', 'team_id', 'points', 'mp']),
        CHECK.no_nulls('gold.dim_competition',
                       cols=['competition_id', 'competition_name']),
        CHECK.no_nulls('gold.dim_season',
                       cols=['season_id', 'season_start_year',
                             'valid_from', 'valid_to']),

        # ----- E2: ref_integrity dim_standings.team_id → dim_team — ERROR -----
        # Soft FK: rows whose team_id_source='sofascore_orphan' are intentionally
        # NOT in dim_team (resolver couldn't match — they are tracked but not
        # joined). Only the canonical-resolved rows must point at a real
        # dim_team key. Implemented as row_count(max_rows=0) over the
        # offending predicate because the universal CHECK.ref_integrity has
        # no WHERE-filter mode (yet).
        # Severity = WARNING (not ERROR) because the upstream entity_xref
        # alias coverage (`_team_aliases.sql`) is incomplete by design —
        # SofaScore variants like 'Liverpool FC' map to a distinct
        # `liverpool_fc` canonical_id whereas dim_team uses `liverpool`.
        # Closing those gaps is E1's job (xref refactor → Silver), not E2's.
        # The orphan share is also surfaced via the coverage WARNING below.
        CHECK.row_count(
            'gold.dim_standings', min_rows=0, max_rows=0,
            where=("team_id_source = 'fbref_canonical' "
                   "AND team_id NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"),
            severity='WARNING',
            name='ref_integrity[dim_standings.team_id->dim_team]',
        ),

        # ----- E2: schema-versioning completeness (R0.4) — ERROR -----
        # Every row with a non-NULL <base>_canonical MUST also carry
        # <base>_source and <base>_version. Catches schema regressions
        # where a CTAS forgets to populate the trio.
        # NB: dim_competition / dim_season are intentionally included even
        # though their canonical = literal column — serves as a regression
        # guard for future v2 schema bumps.
        CHECK.canonical_completeness('gold.dim_venue',       'venue_canonical'),
        CHECK.canonical_completeness('gold.dim_referee',     'referee_canonical'),
        CHECK.canonical_completeness('gold.dim_competition', 'competition_canonical'),
        CHECK.canonical_completeness('gold.dim_season',      'season_canonical'),

        # ----- E2: value-range sanity (WARNING) -----
        # APL has 38 matches/season (max 46 across other supported leagues).
        # Points hard ceiling: 38 * 3 = 114 -> round to 120 for safety margin.
        CHECK.value_range('gold.dim_standings', 'points',
                          min_val=0, max_val=120, severity='WARNING'),
        CHECK.value_range('gold.dim_standings', 'mp',
                          min_val=0, max_val=46,  severity='WARNING'),
        CHECK.value_range('gold.dim_standings', 'position',
                          min_val=1, max_val=24,  severity='WARNING'),
    ]

    report = run_checks(checks, raise_on_error=False)

    # T4.1: ad-hoc disjointness — train and test splits must not share any
    # match_id. Implemented out-of-band because the CHECK registry has no
    # cross-table set-difference primitive yet. Failure is ERROR-grade: if
    # train and test overlap, every reported metric becomes invalid.
    _append_train_test_disjointness_check(report)

    # E2: two-tier coverage check on dim_standings.team_id resolver hit-rate.
    # Inline because the universal CHECK registry has no two-tier coverage
    # primitive yet (see helper docstring). WARNING-only — orphans are
    # intentionally retained with team_id_source='sofascore_orphan'.
    _append_dim_standings_coverage_check(report)

    logger.info(f"Gold DQ: {report.summary()}")

    telegram_dq_summary(report, header="Gold DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Gold DQ failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors[:5])
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


def validate_predictions_input() -> Dict[str, Any]:
    """T4.2: validate the inference snapshot ``gold.predictions_input``.

    Contract — the table must:
      * have a unique PK on match_id (one row per upcoming fixture);
      * carry non-null PK / temporal / team-id keys (joins on serve side);
      * keep ``date`` strictly inside [CURRENT_DATE, CURRENT_DATE + 7 days];
      * keep features fresh — the feat_team_form lineage stamp must not lag
        more than 6 hours (DAG runs every 2 h; >6 h means 3 missed cycles).

    WARNING-only: row count below 1 (legitimate during international break /
    off-season — must not page on-call).
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    checks = [
        # ===== ERROR: PK + critical keys =====
        CHECK.no_duplicates('gold.predictions_input', pk=['match_id']),
        CHECK.no_nulls(
            'gold.predictions_input',
            cols=['match_id', 'date', 'home_team_id', 'away_team_id'],
        ),

        # ===== ERROR: temporal window sanity =====
        # Re-uses row_count with max_rows=0 + a WHERE that selects rows
        # OUTSIDE the allowed window. Anything > 0 means the SELECT filter
        # regressed and we are about to ship stale or far-future fixtures.
        CHECK.row_count(
            'gold.predictions_input',
            min_rows=0, max_rows=0,
            where="date < CURRENT_DATE OR date > CURRENT_DATE + INTERVAL '7' DAY",
            severity='ERROR',
            name='date_window[predictions_input.date in [today, today+7d]]',
        ),

        # ===== WARNING: row count =====
        # Empty week is plausible (international break, off-season); only
        # surface as WARNING so a fixture-less week does not page on-call.
        CHECK.row_count(
            'gold.predictions_input', min_rows=1,
            severity='WARNING',
        ),

        # ===== WARNING: feature freshness =====
        # Inference DAG runs every 2 h; feat_team_form should be rebuilt at
        # least once per Gold cycle. >6 h stale means upstream Gold missed
        # several cycles — flag, but do not fail (model can still serve on
        # slightly older features for one tick).
        CHECK.freshness(
            'gold.feat_team_form', ts_col='_silver_created_at',
            max_age_hours=6, severity='WARNING',
        ),
    ]

    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Predictions input DQ: {report.summary()}")
    telegram_dq_summary(report, header="Predictions DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Predictions input DQ failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors[:5])
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


def count_predictions_input() -> Dict[str, Any]:
    """T4.2: log the inference snapshot row count for observability.

    Lightweight task — surfaces "how many fixtures the model will score in
    the next 7 days" in the Airflow log + XCom. No assertions; pure metric.
    """
    from utils.data_quality import _get_conn

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*), MIN(date), MAX(date) "
                "FROM iceberg.gold.predictions_input"
            )
            row = cur.fetchone() or (0, None, None)
        finally:
            cur.close()
    finally:
        conn.close()

    n, dmin, dmax = row
    logger.info(
        f"predictions_input: {n} upcoming fixture(s) "
        f"(date range: {dmin} .. {dmax})"
    )
    return {'count': n, 'date_min': str(dmin), 'date_max': str(dmax)}


def validate_gold_row_counts() -> Dict[str, Any]:
    """Sanity check: Gold tables have expected row counts."""
    from utils.data_quality import CHECK, run_checks

    # Rough expectations for APL-only history (9 complete seasons + current):
    # - 3420-3800 matches in dim_match
    # - 6840-7600 rows in fct_team_match (long form: 2 per match)
    # - ~1900-2200 player-seasons in dim_player
    checks = [
        CHECK.row_count('gold.dim_match',        min_rows=3000),
        CHECK.row_count('gold.fct_team_match',   min_rows=6000),
        CHECK.row_count('gold.fct_match',        min_rows=3000),
        CHECK.row_count('gold.feat_team_form',    min_rows=6000),
        # feat_team_xg_form built from optional shot_events Silver — may be
        # empty if shot_events isn't materialized. Use 0 floor to avoid hard
        # failure during MVP rollout; raise once shot_events ingestion is GA.
        CHECK.row_count('gold.feat_team_xg_form', min_rows=0),
        CHECK.row_count('gold.feat_team_h2h',     min_rows=6000),
        CHECK.row_count('gold.dim_team',         min_rows=50),
        CHECK.row_count('gold.dim_player',       min_rows=1000),
        CHECK.row_count('gold.fct_player_match', min_rows=50000),
        CHECK.row_count('gold.feat_player_form', min_rows=50000),
        CHECK.row_count('gold.entity_xref',      min_rows=2000),
        CHECK.row_count('gold.match_outcomes',   min_rows=3000),
        # T4.1: ML splits — soft floors. Tighten after first production run.
        # FBref-only ENG-PL: ~380 completed matches/season; with 9+ seasons
        # historical the train side easily clears 1500. Test side is per-season
        # (~76 rows from latest season alone — but historical seasons add up
        # to ~684). 75 is the absolute minimum (1 season's tail) and stays
        # safe even if only the current season is materialized.
        CHECK.row_count('gold.fct_match_train',  min_rows=1500),
        CHECK.row_count('gold.fct_match_test',   min_rows=75),

        # ===== E2: master-data dim row-count floors =====
        # dim_venue: APL has ~20 active stadiums per season; 9+ seasons of
        # history with promotion/relegation churn comfortably exceeds 20 unique.
        CHECK.row_count('gold.dim_venue',     min_rows=20),
        # dim_referee: typically ~30+ active EPL match officials across history.
        CHECK.row_count('gold.dim_referee',   min_rows=30),
        # dim_standings: at least one snapshot of the current 18-team table
        # (relaxed to 18 to cover early-season / partial loads — historical
        # snapshots will multiply this by season).
        CHECK.row_count('gold.dim_standings', min_rows=18),
        # dim_competition: derived from leagues.yaml — currently 5 supported
        # leagues. Hard equality (min=max=5) detects drift the moment the
        # leagues list changes without a corresponding CTAS update.
        CHECK.row_count('gold.dim_competition', min_rows=5, max_rows=5),
        # dim_season: derived from SEASONS list — currently 5 seasons in
        # rotation. Same drift-detection contract as dim_competition.
        CHECK.row_count('gold.dim_season',      min_rows=5, max_rows=5),
    ]
    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Gold row counts: {report.summary()}")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Gold row counts below threshold: "
            + "; ".join(f"{r.name}: {r.details}" for r in report.errors[:5])
        )
    return {'results': [(r.name, r.value, r.passed) for r in report.results]}
