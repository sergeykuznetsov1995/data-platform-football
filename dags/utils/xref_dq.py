"""
DQ checks + dual-run parity validators for Silver xref-tables (E1).
==================================================================

This module is the T6 deliverable of the Medallion E1 redesign. It is
consumed by ``dag_transform_xref.validate_xref`` and is intentionally
**read-only** with respect to Iceberg — every query is a SELECT.

Two responsibilities
--------------------
1. **Per-table DQ checks** (PK uniqueness, NULL guards, enum compliance,
   row-count, coverage). Built on the existing universal
   :mod:`utils.data_quality` primitives — we do NOT modify
   ``data_quality.py``. Enum / coverage are implemented as targeted
   ``CHECK.row_count`` invocations with a ``WHERE`` predicate that
   selects offending rows; offender count > 0 fails the check.

2. **Dual-run parity** (E1 policy: ≥3 days side-by-side with the legacy
   ``gold.entity_xref`` table). The parity functions return diff metrics
   without raising — diffs are surfaced via Telegram and XCom and
   investigated manually before the E1.5 cutover.

Confidence allow-lists (SOURCE OF TRUTH for enum compliance)
------------------------------------------------------------
Verified against the SQL files on 2026-05-08:

* ``xref_team``     — {``name_alias``, ``orphan``}
* ``xref_match``    — {``exact``}
* ``xref_referee``  — {``name_normalize``}
* ``xref_player``   — {``exact``, ``name_team``, ``name_team_jersey``,
                       ``name_team_dob``, ``orphan``}  (jersey/dob are
                       reserved STUBS but allowed in the enum so adding
                       a single tier later does not require touching DQ.)

Sources (xref_team / xref_referee / xref_match / xref_player) values
are also enforced via enum checks against the documented schema.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from utils.data_quality import CHECK, Check, _get_conn, _qualify  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enum / coverage helpers (built on row_count + WHERE — NO data_quality.py edits)
# ---------------------------------------------------------------------------

def check_enum_compliance(
    table: str,
    column: str,
    allowed: List[str],
    severity: str = 'ERROR',
    name: Optional[str] = None,
) -> Check:
    """Return a Check that fails if ``column`` has any value outside ``allowed``.

    Implemented as ``row_count(min=0, max=0, WHERE col NOT IN (...))``.
    A NULL in ``column`` is **not** an enum violation by definition (we
    have dedicated ``no_nulls`` checks for required columns); the WHERE
    predicate uses ``column NOT IN (...)`` which already filters NULL.
    """
    if not allowed:
        raise ValueError("check_enum_compliance requires a non-empty allowed list")
    # Quote each value as a SQL string literal; reject embedded quote
    # characters early to keep the predicate safe for f-string interpolation.
    safe_vals: List[str] = []
    for v in allowed:
        if not isinstance(v, str) or "'" in v or ';' in v or '--' in v:
            raise ValueError(f"Unsafe enum value: {v!r}")
        safe_vals.append(f"'{v}'")
    allowed_csv = ", ".join(safe_vals)
    short = table.split('.')[-1]
    return CHECK.row_count(
        table=table,
        min_rows=0,
        max_rows=0,
        where=f"{column} NOT IN ({allowed_csv})",
        severity=severity,
        name=name or f"enum_compliance[{short}.{column}]",
    )


# ---------------------------------------------------------------------------
# Per-table DQ definitions
# ---------------------------------------------------------------------------

def build_xref_team_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_team``.

    Orphan-rate (coverage-style) is **not** in this list — ratios cannot
    be expressed via ``row_count``. Instead the DAG callable runs
    :func:`evaluate_orphan_rate_per_source` after the standard checks
    and appends a synthetic CheckResult to the report.

    Row count is bounded above as well — a runaway UNION of duplicate
    rows would silently inflate the table; an upper bound flags it.
    """
    table = 'iceberg.silver.xref_team'
    return [
        # Row count: 8 sources × ~50 distinct teams across seasons — min 400.
        # Upper bound 5000 covers 5 seasons of growth before triggering.
        CHECK.row_count(table, min_rows=400, max_rows=5000),

        # PK uniqueness — guaranteed by SQL GROUP BY but DQ-enforced.
        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        # Required columns must be non-NULL.
        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        # Enum compliance — confidence ∈ {'name_alias', 'orphan'}
        check_enum_compliance(
            table, 'confidence',
            allowed=['name_alias', 'orphan'],
            severity='ERROR',
        ),

        # Source enum (8 sources documented in xref_team.sql.j2)
        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'understat', 'whoscored', 'sofascore',
                     'fotmob', 'matchhistory', 'clubelo', 'espn'],
            severity='ERROR',
        ),
    ]


def build_xref_match_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_match`` (Phase B — 7-source cascade).

    Source enum covers FBref spine + 6 cascaded sources. PK is composite
    ``(canonical_id, source)`` because the same FBref canonical legitimately
    appears under multiple ``source`` values (the bridged rows). Per-source
    bridge coverage is enforced via ``CHECK.coverage`` with the universal
    two-tier semantics: ``ratio = COUNT_IF(confidence != 'orphan') / COUNT(*)``.
    """
    table = 'iceberg.silver.xref_match'
    sources = [
        'fbref', 'whoscored', 'understat', 'sofascore',
        'fotmob', 'matchhistory', 'espn',
    ]
    checks: List[Check] = [
        # 5 seasons × ~380 APL fixtures × 7 sources ≈ 13K; cap 60K with headroom.
        CHECK.row_count(table, min_rows=1900, max_rows=60_000),

        # PK is composite — bridged sources share canonical_id with FBref.
        CHECK.no_duplicates(table, pk=['canonical_id', 'source']),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=sources,
            severity='ERROR',
        ),

        # 'exact' = FBref spine, 'date_team_match' = bridged cascade row,
        # 'orphan' = source row with no FBref counterpart.
        check_enum_compliance(
            table, 'confidence',
            allowed=['exact', 'date_team_match', 'orphan'],
            severity='ERROR',
        ),
    ]

    # Per-source bridge coverage (skip fbref — it's the spine, always 'exact').
    # Two-tier semantics via the data_quality coverage runner:
    #   ratio >= 0.95 -> OK; 0.80-0.95 -> WARNING; <0.80 -> ERROR.
    for src in sources:
        if src == 'fbref':
            continue
        checks.append(CHECK.coverage(
            table=table,
            condition="confidence != 'orphan'",
            where=f"source = '{src}'",
            warn_threshold=0.95,
            error_threshold=0.80,
            severity='WARNING',  # runner promotes to ERROR when ratio < 0.80
            name=f'bridge_coverage[xref_match.{src}]',
        ))

    return checks


def build_xref_referee_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_referee``."""
    table = 'iceberg.silver.xref_referee'
    return [
        CHECK.row_count(table, min_rows=200, max_rows=5000),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'matchhistory'],
            severity='ERROR',
        ),

        check_enum_compliance(
            table, 'confidence',
            allowed=['name_normalize'],
            severity='ERROR',
        ),
    ]


def build_xref_manager_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_manager`` — Phase 1.5 single-source spine.

    Source = FBref scorebox parser (bronze.fbref_match_managers). Bounds
    sized for APL across 8 seasons (2017-18 … 2024-25): ~30-50 distinct
    managers × per-season presence ≈ 60-200 rows. Upper bound is generous
    for future multi-league or multi-source expansion.
    """
    table = 'iceberg.silver.xref_manager'
    return [
        CHECK.row_count(table, min_rows=20, max_rows=2000),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=['fbref'],
            severity='ERROR',
        ),

        check_enum_compliance(
            table, 'confidence',
            allowed=['name_normalize'],
            severity='ERROR',
        ),
    ]


def build_xref_player_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_player``.

    Confidence allow-list aligned with ``xref_player_resolver.py`` cascade.
    Orphan-rate per source is evaluated separately by
    :func:`evaluate_orphan_rate_per_source`; the results are appended to
    the run report by the DAG callable (see ``dag_transform_xref``).
    """
    table = 'iceberg.silver.xref_player'
    return [
        # T3 hotfix produced ~1500 rows for ENG-Premier League; lower bound
        # 400 stays conservative. Upper bound 50k allows multi-season growth.
        CHECK.row_count(table, min_rows=400, max_rows=50000),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        # confidence — mirror the resolver cascade tier names verbatim.
        # 'name_team_surname' / 'name_team_subset' / 'name_team_nickname' /
        # 'name_team_alias' added by R2-followup v2 resolver. 'name_team_jersey'
        # / 'name_team_dob' remain reserved STUBs (Bronze does not yet expose
        # cross-source jersey/DOB consistently). 'ambiguous' is INTENTIONALLY
        # NOT in this list — Fellegi-Sunter clerical-review rows must land in
        # silver.xref_player_review, not xref_player. An 'ambiguous' value
        # here is therefore a DQ ERROR by design.
        check_enum_compliance(
            table, 'confidence',
            allowed=['exact', 'name_team', 'name_team_surname',
                     'name_team_subset', 'name_team_nickname',
                     'name_team_alias', 'name_team_jersey',
                     'name_team_dob', 'orphan'],
            severity='ERROR',
        ),

        # source enum — 5 sources (T6 added SofaScore + restored FotMob).
        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'understat', 'whoscored', 'fotmob', 'sofascore'],
            severity='ERROR',
        ),

        # canonical_id format guard — must start with one of the 5 known
        # prefixes (fb_/us_/ws_/fm_/ss_). Regex via Trino regexp_like.
        # We express this as a row_count of offending rows.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where="NOT regexp_like(canonical_id, '^(fb|us|ws|fm|ss)_.+$')",
            severity='ERROR',
            name='canonical_id_format[xref_player]',
        ),

        # Issue #70: prevent the fan-out pattern that prompted the Gold
        # ROW_NUMBER hack. A single canonical_id legitimately has one
        # source_id per (source, league, season); >1 means a Gold JOIN on
        # (source, source_id) without (league, season) will fan-out 2×.
        # Dedup is enforced in xref_player_resolver._dedup_canonical_per_season;
        # this gate makes regressions visible.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=(
                "(canonical_id, source, league, season) IN ("
                "SELECT canonical_id, source, league, season "
                "FROM iceberg.silver.xref_player "
                "WHERE confidence <> 'orphan' "
                "GROUP BY canonical_id, source, league, season "
                "HAVING COUNT(DISTINCT source_id) > 1"
                ") AND confidence <> 'orphan'"
            ),
            severity='ERROR',
            name='no_duplicates_per_canonical_season[xref_player]',
        ),
    ]


def build_xref_player_review_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_player_review`` (R2-followup v2 sibling).

    The review table holds Fellegi-Sunter clerical-review band rows — source
    candidates that the resolver flagged ambiguous (multiple candidates
    surfaced by surname-anchor / token_set / nickname tiers). It is intended
    to stay SMALL: a healthy resolver run produces ≤30 rows per (league,
    season). Significantly more rows is a signal that thresholds are
    miscalibrated or that the spine is missing entries.

    Checks:
      * row_count(min=0, max=200) — soft ceiling at 200 across the whole
        table. APL has 4 in-scope seasons × 3 sources, so 200 leaves
        meaningful headroom while still alarming on runaway growth.
      * no_duplicates on ``(source, source_id, league, season)`` — the
        resolver may legitimately surface the same source row across
        multiple seasons but never twice in the same season.
      * no_nulls on identifying columns.
      * enum compliance on ``rule`` — must match the rule labels emitted
        by the cascade (``surname_collision``, ``token_set_band``,
        ``nickname_collision``).
      * enum compliance on ``source`` — three v2-supported sources.
    """
    table = 'iceberg.silver.xref_player_review'
    return [
        CHECK.row_count(table, min_rows=0, max_rows=200),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(
            table,
            cols=['source', 'source_id', 'rule', 'league', 'season',
                  'detected_at'],
        ),

        check_enum_compliance(
            table, 'rule',
            allowed=['surname_collision', 'token_set_band',
                     'nickname_collision'],
            severity='ERROR',
        ),

        check_enum_compliance(
            table, 'source',
            allowed=['understat', 'whoscored', 'fotmob', 'sofascore'],
            severity='ERROR',
        ),
    ]


def build_all_xref_checks() -> List[Check]:
    """Aggregate DQ checks for all 5 xref tables + review sibling."""
    return (
        build_xref_team_checks()
        + build_xref_match_checks()
        + build_xref_referee_checks()
        + build_xref_manager_checks()
        + build_xref_player_checks()
        + build_xref_player_review_checks()
    )


# ---------------------------------------------------------------------------
# E1.5 post-cutover ref_integrity / canonical-format checks
# ---------------------------------------------------------------------------

def build_e1_5_post_cutover_checks() -> List[Check]:
    """Forward-looking DQ checks for the E1.5 cutover.

    These checks validate that Gold consumers correctly resolve to the new
    Silver xref source-of-truth. They are wired into Gold validate_gold_quality
    in the **prep PR** at severity=WARNING so we observe the diff during the
    ≥3-day green-parity gate-watch window without breaking the DAG. After
    cutover-merge a follow-up PR may tighten the team-level check to ERROR.

    Six checks (all severity=WARNING in this prep PR):

    1. ``ref_integrity[dim_team.team_id->silver.xref_team(fbref).canonical_id]``
       — Gold dim_team.team_id must trace back to silver.xref_team for the
       FBref source slice. Implemented as ``row_count(max=0)`` over the
       offending predicate because the universal ``CHECK.ref_integrity``
       has no WHERE-filter mode.

    2. ``ref_integrity[fct_player_match.team_id->dim_team]`` — narrow
       ref_integrity check that catches team_id slug drift introduced when
       SQL files cut over from gold.entity_xref → silver.xref_team.

    3-4. ``ref_integrity[match_outcomes.{home,away}_team_id->dim_team]`` —
        same intent, narrower scope. match_outcomes is the ML-label
        source-of-truth so a regression here invalidates target labels.

    5-6. Canonical-format guards on ``dim_player.player_id`` and
        ``fct_player_match.player_id`` — both MUST start with ``fb_``
        post-cutover (FBref is the player spine). Implemented via
        ``regexp_like`` over a row_count(max=0) predicate.

    Severity rationale
    ------------------
    All six checks ship at WARNING during the gate-watch window (2026-05-09
    → 2026-05-12+). A non-zero offender count surfaces in Telegram via
    ``telegram_dq_summary`` without raising. After cutover the team-level
    check (#1) is the first candidate for ERROR-severity tightening — see
    ``docs/decisions/E1.5-cutover-prep.md``.

    NOTE: This builder does NOT modify ``dags/utils/data_quality.py`` —
    every check leverages an existing primitive (``row_count`` with WHERE).
    """
    checks: List[Check] = [
        # 1) dim_team.team_id ⊆ silver.xref_team.canonical_id (source='fbref')
        CHECK.row_count(
            table='iceberg.gold.dim_team',
            min_rows=0,
            max_rows=0,
            where=(
                "team_id NOT IN ("
                "SELECT canonical_id FROM iceberg.silver.xref_team "
                "WHERE source = 'fbref'"
                ")"
            ),
            severity='WARNING',
            name='ref_integrity[dim_team.team_id->silver.xref_team(fbref)]',
        ),

        # 2) fct_player_match.team_id_canonical ⊆ dim_team.team_id
        # issue #46: fct_player_match теперь multi-source, колонка переименована
        # team_id → team_id_canonical.
        CHECK.row_count(
            table='iceberg.gold.fct_player_match',
            min_rows=0,
            max_rows=0,
            where=(
                "team_id_canonical IS NOT NULL "
                "AND team_id_canonical NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"
            ),
            severity='WARNING',
            name='ref_integrity[fct_player_match.team_id->dim_team]',
        ),

        # 3) match_outcomes.home_team_id ⊆ dim_team.team_id
        CHECK.row_count(
            table='iceberg.gold.match_outcomes',
            min_rows=0,
            max_rows=0,
            where=(
                "home_team_id IS NOT NULL "
                "AND home_team_id NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"
            ),
            severity='WARNING',
            name='ref_integrity[match_outcomes.home_team_id->dim_team]',
        ),

        # 4) match_outcomes.away_team_id ⊆ dim_team.team_id
        CHECK.row_count(
            table='iceberg.gold.match_outcomes',
            min_rows=0,
            max_rows=0,
            where=(
                "away_team_id IS NOT NULL "
                "AND away_team_id NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"
            ),
            severity='WARNING',
            name='ref_integrity[match_outcomes.away_team_id->dim_team]',
        ),

        # 5) dim_player.player_id matches '^fb_' regex
        CHECK.row_count(
            table='iceberg.gold.dim_player',
            min_rows=0,
            max_rows=0,
            where="NOT regexp_like(player_id, '^fb_.+')",
            severity='WARNING',
            name='canonical_format[dim_player.player_id]',
        ),

        # 6) fct_player_match.player_id_canonical matches '^fb_' regex
        # issue #46: переименование player_id → player_id_canonical.
        CHECK.row_count(
            table='iceberg.gold.fct_player_match',
            min_rows=0,
            max_rows=0,
            where="player_id_canonical IS NOT NULL AND NOT regexp_like(player_id_canonical, '^fb_.+')",
            severity='WARNING',
            name='canonical_format[fct_player_match.player_id]',
        ),
    ]
    return checks


# ---------------------------------------------------------------------------
# Orphan-rate evaluation (runs after the standard DQ pass)
# ---------------------------------------------------------------------------

def evaluate_orphan_rate_per_source(
    table: str = 'iceberg.silver.xref_player',
    warning_threshold: float = 10.0,
    error_threshold: float = 25.0,
) -> Dict[str, Any]:
    """Compute orphan-rate per ``source`` for an xref table and classify.

    Returns a dict::

        {
            'per_source': {
                'fbref': {'total': N, 'orphans': K, 'pct': X.X, 'verdict': 'OK'},
                ...
            },
            'overall_pct': float,
            'verdict': 'OK' | 'WARNING' | 'ERROR',
            'breaches': [{'source': str, 'pct': float, 'verdict': str}, ...],
        }

    Verdict semantics:
      * pct ≤ warning_threshold        — OK
      * warning < pct ≤ error          — WARNING
      * pct > error_threshold          — ERROR

    NOTE: This function does NOT raise. Callers decide whether to escalate.
    """
    qualified = _qualify(table)
    sql = (
        "SELECT source, "
        "       COUNT(*) AS total, "
        "       COUNT_IF(confidence = 'orphan') AS orphans "
        f"FROM {qualified} "
        "GROUP BY source"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    per_source: Dict[str, Dict[str, Any]] = {}
    breaches: List[Dict[str, Any]] = []
    overall_total = 0
    overall_orphans = 0
    overall_verdict = 'OK'

    for src, total, orphans in rows:
        pct = (100.0 * orphans / total) if total else 0.0
        if pct > error_threshold:
            verdict = 'ERROR'
        elif pct > warning_threshold:
            verdict = 'WARNING'
        else:
            verdict = 'OK'

        per_source[src] = {
            'total': int(total),
            'orphans': int(orphans),
            'pct': round(pct, 2),
            'verdict': verdict,
        }
        if verdict != 'OK':
            breaches.append({'source': src, 'pct': round(pct, 2), 'verdict': verdict})
        overall_total += int(total)
        overall_orphans += int(orphans)

        # Promote overall verdict to the strictest seen
        if verdict == 'ERROR':
            overall_verdict = 'ERROR'
        elif verdict == 'WARNING' and overall_verdict == 'OK':
            overall_verdict = 'WARNING'

    overall_pct = (100.0 * overall_orphans / overall_total) if overall_total else 0.0
    return {
        'per_source': per_source,
        'overall_pct': round(overall_pct, 2),
        'verdict': overall_verdict,
        'breaches': breaches,
    }


# ---------------------------------------------------------------------------
# Bronze-vs-xref freshness gap (Issue #15 regression guard)
# ---------------------------------------------------------------------------

#: Default Bronze tables consulted by :func:`evaluate_bronze_xref_freshness_gap`.
#: Each entry: (source_label, fully_qualified_bronze_table).
DEFAULT_FRESHNESS_BRONZE_TABLES = (
    ('understat', 'iceberg.bronze.understat_players'),
    ('fotmob', 'iceberg.bronze.fotmob_player_stats'),
)


def evaluate_bronze_xref_freshness_gap(
    bronze_tables=DEFAULT_FRESHNESS_BRONZE_TABLES,
    xref_table: str = 'iceberg.silver.xref_player',
    warning_lag_hours: float = 24.0,
    error_lag_hours: float = 72.0,
) -> Dict[str, Any]:
    """Compare Bronze player-table freshness against xref_player snapshot age.

    Issue #15 regression guard. ``silver.xref_player`` is materialised via a
    full DROP+CREATE+INSERT by :mod:`utils.xref_player_resolver`. If the
    resolver DAG (``dag_transform_xref``) is paused or stalls, recently-ingested
    Bronze players are silently absent from xref → downstream Gold facts get
    NULL canonical_id and orphan-rate metrics look healthy because the row
    never made it into the table at all (not even as an orphan).

    Symptom from Issue #15 (2026-05-17):
        * bronze.understat_players: 532 rows for season='2526' (incl. Bukayo Saka)
        * silver.xref_player: 267 rows for (understat, '2526') — Saka missing
        * Last resolver snapshot: 2026-05-15 14:54
        * Last understat 2526 Bronze ingest: 2026-05-17 09:00
        * Resulting Gold Understat coverage stuck at 50.30%

    Methodology
    -----------
    For each (source, bronze_table) pair we compute MAX(_ingested_at) per
    season and compare against MAX(committed_at) of the xref table's snapshot
    history. A positive lag means Bronze has data the resolver has not yet
    processed.

    Args:
        bronze_tables: Iterable of (source_label, qualified_bronze_table).
            Defaults to Understat + FotMob; WhoScored excluded because the
            resolver reads players from ``bronze.whoscored_events`` which is
            too large to scan freshness-per-season cheaply.
        xref_table: Iceberg table whose snapshot timestamp represents the
            last successful resolver run.
        warning_lag_hours: Lag above this — WARNING.
        error_lag_hours: Lag above this — ERROR.

    Returns:
        dict::

            {
                'xref_max_committed_at': datetime | None,
                'per_partition': [
                    {'source': str, 'season': str, 'bronze_max_ts': datetime,
                     'lag_hours': float, 'verdict': 'OK'|'WARNING'|'ERROR'},
                    ...
                ],
                'verdict': 'OK' | 'WARNING' | 'ERROR',
                'breaches': [...],  # entries with non-OK verdict
            }

    Does NOT raise — caller decides whether to escalate.
    """
    # Snapshot view name must NOT be sanitised by _safe_ident — it contains '$'.
    # We hardcode the schema/table parts; only the literal table name is
    # parameterised via xref_table.
    qualified = _qualify(xref_table)
    cat, schema, tbl = qualified.split('.')
    snapshots_view = f'{cat}.{schema}."{tbl}$snapshots"'

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT MAX(committed_at) FROM {snapshots_view}"
            )
            row = cur.fetchone()
            xref_max = row[0] if row else None

            per_partition: List[Dict[str, Any]] = []
            for source_label, bronze_table in bronze_tables:
                bronze_qualified = _qualify(bronze_table)
                cur.execute(
                    "SELECT CAST(season AS varchar) AS season_str, "
                    "       MAX(_ingested_at) AS bronze_max "
                    f"FROM {bronze_qualified} "
                    "GROUP BY season"
                )
                for season_str, bronze_max in cur.fetchall():
                    if bronze_max is None or xref_max is None:
                        lag_hours = None
                    else:
                        delta = bronze_max - xref_max.replace(tzinfo=bronze_max.tzinfo)
                        lag_hours = delta.total_seconds() / 3600.0

                    if lag_hours is None or lag_hours <= 0:
                        verdict = 'OK'
                    elif lag_hours > error_lag_hours:
                        verdict = 'ERROR'
                    elif lag_hours > warning_lag_hours:
                        verdict = 'WARNING'
                    else:
                        verdict = 'OK'

                    per_partition.append({
                        'source': source_label,
                        'season': season_str,
                        'bronze_max_ts': bronze_max,
                        'lag_hours': (
                            round(lag_hours, 2) if lag_hours is not None else None
                        ),
                        'verdict': verdict,
                    })
        finally:
            cur.close()
    finally:
        conn.close()

    breaches = [p for p in per_partition if p['verdict'] != 'OK']
    if any(p['verdict'] == 'ERROR' for p in per_partition):
        overall = 'ERROR'
    elif any(p['verdict'] == 'WARNING' for p in per_partition):
        overall = 'WARNING'
    else:
        overall = 'OK'

    return {
        'xref_max_committed_at': xref_max,
        'per_partition': per_partition,
        'verdict': overall,
        'breaches': breaches,
    }


# ---------------------------------------------------------------------------
# Dual-run parity validators (Silver xref vs legacy gold.entity_xref)
# ---------------------------------------------------------------------------

# Map silver canonical_id → legacy gold canonical_id formula.
# In legacy gold.entity_xref, team canonical_id was
# ``LOWER(REGEXP_REPLACE(team_name, '[^a-zA-Z0-9]+', '_'))`` derived
# directly from the FBref team name; player canonical_id was the FBref
# player_id (no prefix); match canonical_id was the FBref match_id.
#
# Silver xref applies the YAML alias map first, so for FBref-only rows
# whose raw name is NOT in the alias YAML the canonical_id will differ:
# legacy uses the raw FBref name, Silver uses the canonical_name. Hence
# parity for "FBref-only" team rows requires the raw name to either be
# in the alias map (canonical_name set) or absent (orphan, prefix
# 'fb_<slug>'). We do NOT try to bridge formulas here — the Silver value
# is the new source-of-truth; we simply REPORT the diff.

def _parity_diff_query(silver_table: str, legacy_entity_type: str) -> str:
    """Build the FULL OUTER JOIN diff SQL for one entity type."""
    silver_q = _qualify(silver_table)
    return f"""
WITH gold_legacy AS (
    SELECT
        source,
        source_id,
        canonical_id AS legacy_cid,
        league,
        CAST(season AS varchar) AS season
    FROM iceberg.gold.entity_xref
    WHERE entity_type = '{legacy_entity_type}'
),
silver_new AS (
    SELECT
        source,
        source_id,
        canonical_id AS new_cid,
        league,
        CAST(season AS varchar) AS season
    FROM {silver_q}
)
SELECT
    COALESCE(gl.source,    sn.source)    AS source,
    COALESCE(gl.source_id, sn.source_id) AS source_id,
    COALESCE(gl.league,    sn.league)    AS league,
    COALESCE(gl.season,    sn.season)    AS season,
    gl.legacy_cid,
    sn.new_cid,
    CASE
        WHEN gl.legacy_cid IS NULL                THEN 'silver_only'
        WHEN sn.new_cid    IS NULL                THEN 'gold_only'
        WHEN gl.legacy_cid = sn.new_cid           THEN 'match'
        ELSE                                            'cid_diff'
    END AS diff_kind
FROM gold_legacy gl
FULL OUTER JOIN silver_new sn
    ON  gl.source    = sn.source
    AND gl.source_id = sn.source_id
    AND gl.league    = sn.league
    AND COALESCE(gl.season, '') = COALESCE(sn.season, '')
"""


def _legacy_table_exists() -> bool:
    """Return True iff iceberg.gold.entity_xref exists.

    During the dual-run period the table MUST exist; this guard exists for
    pre-cutover edge cases (e.g. running parity in a fresh environment).
    """
    sql = (
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = 'iceberg' "
        "AND table_schema = 'gold' "
        "AND table_name = 'entity_xref'"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
        finally:
            cur.close()
        return bool(row and row[0])
    finally:
        conn.close()


def _run_parity(silver_table: str, legacy_entity_type: str) -> Dict[str, Any]:
    """Generic parity runner used by ``parity_check_xref_*`` wrappers.

    Returns the same shape as documented in
    :func:`parity_check_xref_team_vs_gold`.
    """
    base = {
        'silver_rows': 0,
        'gold_legacy_rows': 0,
        'matched_pairs': 0,
        'silver_only': 0,
        'gold_only': 0,
        'cid_diff': 0,
        'canonical_id_match_pct': 1.0,
        'sample_diffs': [],
        'verdict': 'PARITY_OK',
    }

    if not _legacy_table_exists():
        base['verdict'] = 'LEGACY_ABSENT'
        base['sample_diffs'] = [{'note': 'iceberg.gold.entity_xref does not exist'}]
        return base

    sql = _parity_diff_query(silver_table, legacy_entity_type)
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    silver_only = 0
    gold_only = 0
    cid_diff = 0
    matched = 0
    sample_diffs: List[Dict[str, Any]] = []
    silver_rows = 0
    gold_rows = 0

    for src, sid, league, season, legacy_cid, new_cid, diff_kind in rows:
        if legacy_cid is not None:
            gold_rows += 1
        if new_cid is not None:
            silver_rows += 1

        if diff_kind == 'match':
            matched += 1
        else:
            if diff_kind == 'silver_only':
                silver_only += 1
            elif diff_kind == 'gold_only':
                gold_only += 1
            elif diff_kind == 'cid_diff':
                cid_diff += 1
            if len(sample_diffs) < 10:
                sample_diffs.append({
                    'source': src,
                    'source_id': sid,
                    'league': league,
                    'season': season,
                    'legacy_cid': legacy_cid,
                    'new_cid': new_cid,
                    'diff_kind': diff_kind,
                })

    matched_pairs = matched + cid_diff
    cid_match_pct = (matched / matched_pairs) if matched_pairs else 1.0

    diff_total = silver_only + gold_only + cid_diff
    verdict = 'PARITY_OK' if diff_total == 0 else 'DIFF_DETECTED'

    return {
        'silver_rows': silver_rows,
        'gold_legacy_rows': gold_rows,
        'matched_pairs': matched_pairs,
        'silver_only': silver_only,
        'gold_only': gold_only,
        'cid_diff': cid_diff,
        'canonical_id_match_pct': round(cid_match_pct, 4),
        'sample_diffs': sample_diffs,
        'verdict': verdict,
    }


def parity_check_xref_team_vs_gold() -> Dict[str, Any]:
    """Diff ``silver.xref_team`` vs ``gold.entity_xref`` (entity_type='team').

    Output keys::

        silver_rows              — rows present in silver_new
        gold_legacy_rows         — rows present in gold legacy
        matched_pairs            — (source, source_id, league, season) keys in both
        silver_only              — keys present only in Silver  (expected during
                                   dual-run: Silver expands beyond FBref)
        gold_only                — keys present only in legacy Gold (REGRESSION risk)
        cid_diff                 — same key, different canonical_id
        canonical_id_match_pct   — matched / matched_pairs ∈ [0, 1]
        sample_diffs             — first 10 diffs, for XCom/Telegram debug
        verdict                  — 'PARITY_OK' if no diffs, else 'DIFF_DETECTED'
                                   (or 'LEGACY_ABSENT' if entity_xref dropped)

    Does NOT raise. Dual-run policy is informational at E1.
    """
    return _run_parity('iceberg.silver.xref_team', 'team')


def parity_check_xref_match_vs_gold() -> Dict[str, Any]:
    """Diff ``silver.xref_match`` vs ``gold.entity_xref`` (entity_type='match')."""
    return _run_parity('iceberg.silver.xref_match', 'match')


def parity_check_xref_player_vs_gold() -> Dict[str, Any]:
    """Diff ``silver.xref_player`` vs ``gold.entity_xref`` (entity_type='player').

    Note: legacy entity_xref player rows use ``canonical_id = player_id``
    (no ``fb_`` prefix), while Silver uses ``fb_<player_id>``. Therefore
    every FBref-only row will have ``cid_diff`` until E1.5 cutover. The
    verdict is informational — ``cid_diff > 0`` is EXPECTED here.
    """
    return _run_parity('iceberg.silver.xref_player', 'player')


# ---------------------------------------------------------------------------
# Telegram alert helper (private send via alerts._send_telegram)
# ---------------------------------------------------------------------------

# Baseline parity numbers captured 2026-05-09 (data/audit/e1_parity_2026-05-09.json).
# cid_diff for team is 43 (alias-canonicalisation drift, intended — see
# docs/decisions/E1.5-cutover-prep.md). Growth beyond
# ``43 + cid_diff_growth_threshold`` during the gate-watch window triggers an
# INFO Telegram message.
_PARITY_BASELINE_TEAM_CID_DIFF = 43


def _send_parity_alert(parity_summary: Dict[str, Dict[str, Any]],
                       header: str = "E1 dual-run parity",
                       extra_tag: str = "") -> None:
    """Send a one-line Telegram message summarising parity verdicts.

    Uses ``alerts._send_telegram`` directly (no public ``send_telegram_message``
    surface in ``alerts.py``). Always swallows exceptions.

    ``extra_tag`` is appended to the header (e.g. "REGRESSION", "INFO") so
    the same renderer powers normal threshold alerts AND the E1.5 gate-watch
    regression / cid_diff_growth alerts.
    """
    try:
        # private-OK: T6 dual-run parity uses lo-fi telegram; alerts.py only
        # exposes context-aware callbacks (telegram_on_failure/_success/
        # _dq_summary). A public wrapper for arbitrary messages is TBD.
        from utils.alerts import _send_telegram  # type: ignore[attr-defined]  # noqa: PLC2701
        env = os.environ.get('ALERT_ENV', 'dev')
        tag = f" [{extra_tag}]" if extra_tag else ""
        lines = [f"<b>[{env}] {header}{tag}</b>"]
        for entity, p in parity_summary.items():
            verdict = p.get('verdict', '?')
            pct = p.get('canonical_id_match_pct', 1.0) * 100
            lines.append(
                f"  <code>{entity}</code>: {verdict} "
                f"(cid match {pct:.1f}%, "
                f"silver_only={p.get('silver_only', 0)}, "
                f"gold_only={p.get('gold_only', 0)}, "
                f"cid_diff={p.get('cid_diff', 0)})"
            )
        _send_telegram("\n".join(lines))
    except Exception as e:
        logger.warning(f"_send_parity_alert swallowed: {e}")


def maybe_alert_parity(parity_summary: Dict[str, Dict[str, Any]],
                       diff_threshold: int = 100,
                       cid_diff_growth_threshold: int = 5) -> bool:
    """Send Telegram parity alert(s) for the E1 dual-run / E1.5 gate-watch window.

    Three independent alert branches (most-severe-first ordering on send):

    1. **REGRESSION (RED)** — any entity has ``gold_only > 0``: a row exists
       in the legacy ``gold.entity_xref`` but is MISSING from the new Silver
       xref. This is the highest-severity gate-watch signal because it means
       the new SQL refactor lost coverage. Sent as a separate message tagged
       ``[REGRESSION]``.

    2. **THRESHOLD (default)** — ``silver_only + gold_only + cid_diff >=
       diff_threshold`` (default 100; currently sized to the ``43`` cid_diff
       baseline + headroom).

       TODO E1.5: после cutover снизить до 10 — see
       docs/decisions/E1.5-cutover-prep.md. Default stays at 100 in this
       prep PR to avoid spamming during the gate-watch window with the
       expected 43-row cid_diff baseline.

    3. **CID_DIFF_GROWTH (INFO)** — team.cid_diff > baseline (43) +
       ``cid_diff_growth_threshold`` (default 5) — i.e. > 48. Surfaces
       additional alias-canonicalisation drift introduced after baseline
       so we notice early without firing the louder THRESHOLD alert.

    Returns True if at least one alert was sent (False on missing creds /
    no-op). Never raises.
    """
    sent_any = False

    # ---- 1) REGRESSION: gold_only > 0 anywhere ----
    has_regression = any(
        int(p.get('gold_only', 0)) > 0
        for p in parity_summary.values()
    )
    if has_regression:
        _send_parity_alert(
            parity_summary,
            header="E1 dual-run parity",
            extra_tag="REGRESSION",
        )
        sent_any = True

    # ---- 2) THRESHOLD: total diff per-entity ----
    significant = False
    for entity, p in parity_summary.items():
        diff_total = (
            int(p.get('silver_only', 0))
            + int(p.get('gold_only', 0))
            + int(p.get('cid_diff', 0))
        )
        if diff_total >= diff_threshold:
            significant = True
            break
    if significant:
        _send_parity_alert(parity_summary)
        sent_any = True

    # ---- 3) CID_DIFF_GROWTH (INFO): team.cid_diff > baseline + tolerance ----
    team = parity_summary.get('team') or {}
    team_cid_diff = int(team.get('cid_diff', 0))
    if team_cid_diff > _PARITY_BASELINE_TEAM_CID_DIFF + cid_diff_growth_threshold:
        _send_parity_alert(
            parity_summary,
            header="E1 dual-run parity",
            extra_tag=(
                f"INFO cid_diff_growth team.cid_diff={team_cid_diff} "
                f"baseline={_PARITY_BASELINE_TEAM_CID_DIFF}"
            ),
        )
        sent_any = True

    return sent_any
