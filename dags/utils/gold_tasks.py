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

from utils.silver_tasks import (
    _execute,
    _get_trino_connection,
    _resolve_sql_path,
    _validate_identifier,
    check_bronze_table_exists,
    run_silver_transform,
)

logger = logging.getLogger(__name__)


def _render_source_priority(template_file: str, table_name: str) -> str:
    """Render a source-priority fact template (``.sql.j2``) to a tempfile (#437).

    Resolves the template path, fills its ``{{ m_<metric> }}`` COALESCE
    placeholders from configs/medallion/source_priority.yaml via
    ``medallion_config.render_fact_sql``, writes the rendered SQL to a tempfile
    and returns its path. The CTAS runner then treats it like any plain .sql.
    Mirrors ``dim_loaders.run_inline_ctas`` (render -> tempfile -> CTAS).
    """
    import tempfile

    from utils.medallion_config import render_fact_sql

    template_path = _resolve_sql_path(template_file)
    rendered = render_fact_sql(template_path, table_name)
    with tempfile.NamedTemporaryFile(
        mode='w', suffix=f'_{table_name}.sql', delete=False, encoding='utf-8',
    ) as fh:
        fh.write(rendered)
        return fh.name


def run_gold_transform(
    sql_file: str,
    table_name: str,
    partition_columns: Optional[List[str]] = None,
    fallback_sql_file: Optional[str] = None,
    require_silver: Optional[List[str]] = None,
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """Run a Gold-layer CTAS.

    Delegates to ``run_silver_transform`` with ``schema='gold'``. Same atomic
    ``CREATE OR REPLACE`` CTAS engine (which auto-heals positional schema drift
    via DROP + rebuild, #741), same connection settings, same partitioning API.

    Optional graceful-degrade mode for transforms that depend on optional
    Silver tables (e.g. ``fct_player_unavailable`` requires
    ``silver.whoscored_player_unavailable``, which may be absent in MVP
    environments where the Bronze source isn't ingested yet).

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

    # #437: fct_*.sql.j2 templates render their cross-source COALESCE columns
    # from configs/medallion/source_priority.yaml. Resolve + render to a tempfile
    # before the CTAS engine (run_silver_transform), which expects a .sql path.
    if str(sql_file).endswith('.sql.j2'):
        sql_file = _render_source_priority(sql_file, table_name)

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


# ---------------------------------------------------------------------------
# E3.5: wrapper-style per-partition INSERT (no sentinel required)
# ---------------------------------------------------------------------------
#
# DESIGN
# ------
# ``run_gold_partition_insert_wrapped`` wraps the *original* SELECT verbatim as
# ``SELECT * FROM (<orig>) AS __src WHERE league=... AND season=...`` and
# inserts that — ZERO modification of the SQL files needed. Trino's optimiser
# usually pushes the partition predicate down past the outer wrapper anyway
# (verified for fct_event/fct_shot/fct_lineup). Idempotency is DELETE-then-
# INSERT scoped to the partition keys. Used by E3.5 (3 historical APL seasons).


def run_gold_partition_insert_wrapped(
    sql_file: str,
    table_name: str,
    partition_values: Dict[str, str],
    partition_columns: Optional[List[str]] = None,
    catalog: str = 'iceberg',
    schema: str = 'gold',
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """Idempotent per-partition INSERT for a Gold table (wrapper-style).

    Does NOT require the SQL file to carry any sentinel marker. Wraps the
    original SELECT verbatim as
    ``SELECT * FROM (<orig>) WHERE league=... AND season=...``.

    Used by ``dag_e3_backfill`` to materialise a single (league, season)
    slice of ``gold.fct_event`` / ``gold.fct_shot`` / ``gold.fct_lineup``
    without touching other partitions.

    Flow:
      1. CREATE SCHEMA IF NOT EXISTS — idempotent.
      2. If target table doesn't exist, bootstrap via a partition-scoped
         CTAS so the runner always has a target to INSERT into.
      3. DELETE FROM <table> WHERE <partition_filter>  — idempotency.
      4. INSERT INTO <table>
           SELECT *, CURRENT_TIMESTAMP AS _silver_created_at
             FROM (<orig SELECT>) AS __src
            WHERE <partition_filter>
      5. Return row count for the partition.

    Idempotency
    -----------
    Re-running this function for the same (league, season) tuple produces
    the same final state (DELETE-then-INSERT). Failure mid-flight leaves
    the partition in an unknown state — caller is expected to retry the
    same task.

    Parameters
    ----------
    sql_file : str
        Path to the SELECT-only SQL file.
    table_name : str
        Target Gold table (e.g. 'fct_event').
    partition_values : Dict[str, str]
        Concrete values for partition columns,
        e.g. ``{'league': 'ENG-Premier League', 'season': '2324'}``.
    partition_columns : Optional[List[str]]
        Partition column names (default ``['league', 'season']``).
    catalog, schema : str
        Iceberg target (default 'iceberg' / 'gold').
    add_timestamp : bool
        Append ``CURRENT_TIMESTAMP AS _silver_created_at`` to the wrapped
        SELECT (default True — matches the production E3 behaviour).
    """
    from utils.silver_tasks import (
        _build_silver_partition_filter,
        _execute,
        _get_trino_connection,
        _resolve_sql_path,
        _validate_identifier,
    )

    if partition_columns is None:
        partition_columns = ['league', 'season']

    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")
    for pc in partition_columns:
        _validate_identifier(pc, "partition column")

    if set(partition_values.keys()) != set(partition_columns):
        raise ValueError(
            f"partition_values keys must equal partition_columns. "
            f"Got keys={sorted(partition_values.keys())}, "
            f"expected={sorted(partition_columns)}"
        )

    full_table = f"{catalog}.{schema}.{table_name}"
    keys_ordered = list(partition_columns)
    values_ordered = [partition_values[k] for k in keys_ordered]
    partition_filter = _build_silver_partition_filter(keys_ordered, values_ordered)
    partition_label = ", ".join(
        f"{k}={v!r}" for k, v in zip(keys_ordered, values_ordered)
    )

    # Read SELECT body
    sql_path = _resolve_sql_path(sql_file)
    select_sql = sql_path.read_text(encoding='utf-8').strip()
    if not select_sql:
        raise ValueError(f"SQL file is empty: {sql_path}")
    if select_sql.endswith(';'):
        select_sql = select_sql[:-1].rstrip()

    result: Dict[str, Any] = {
        'table': full_table,
        'partition': dict(partition_values),
        'rows_inserted': 0,
        'status': 'pending',
        'bootstrap': False,
    }

    conn = _get_trino_connection(catalog=catalog)
    try:
        _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        exists_rows = _execute(
            conn,
            f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table_name}'",
            fetch=True,
        )
        target_exists = bool(exists_rows and len(exists_rows) > 0)

        if not target_exists:
            logger.warning(
                "%s does not exist — bootstrapping via partition-scoped CTAS (%s).",
                full_table, partition_label,
            )
            wrapped = (
                f"SELECT * FROM (\n{select_sql}\n) AS __src\n"
                f"WHERE {partition_filter}"
            )
            partition_clause = ''
            if partition_columns:
                cols = ", ".join(f"'{c}'" for c in partition_columns)
                partition_clause = f"WITH (partitioning = ARRAY[{cols}])\n"
            if add_timestamp:
                ctas_sql = (
                    f"CREATE TABLE {full_table}\n"
                    f"{partition_clause}"
                    f"AS\n"
                    f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at\n"
                    f"FROM (\n{wrapped}\n)"
                )
            else:
                ctas_sql = (
                    f"CREATE TABLE {full_table}\n"
                    f"{partition_clause}"
                    f"AS\n"
                    f"{wrapped}"
                )
            logger.info("Bootstrap CTAS for %s [%s]", full_table, partition_label)
            _execute(conn, ctas_sql)
            result['bootstrap'] = True
        else:
            delete_sql = f"DELETE FROM {full_table} WHERE {partition_filter}"
            logger.info("DELETE (idempotency): %s", delete_sql)
            _execute(conn, delete_sql)

            wrapped = (
                f"SELECT * FROM (\n{select_sql}\n) AS __src\n"
                f"WHERE {partition_filter}"
            )
            if add_timestamp:
                insert_select = (
                    f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at "
                    f"FROM (\n{wrapped}\n) AS __src_ts"
                )
            else:
                insert_select = wrapped
            insert_sql = f"INSERT INTO {full_table}\n{insert_select}"
            logger.info("INSERT into %s for %s", full_table, partition_label)
            _execute(conn, insert_sql)

        cnt_rows = _execute(
            conn,
            f"SELECT COUNT(*) FROM {full_table} WHERE {partition_filter}",
            fetch=True,
        )
        rows_inserted = cnt_rows[0][0] if cnt_rows else 0
        result['rows_inserted'] = int(rows_inserted)
        result['status'] = 'success'
        logger.info(
            "Gold partition INSERT done: %s [%s] => %d rows",
            full_table, partition_label, rows_inserted,
        )

    except Exception as e:
        result['status'] = 'failed'
        result['error'] = str(e)
        logger.error(
            "Gold partition INSERT FAILED for %s [%s]: %s",
            full_table, partition_label, e,
        )
        raise RuntimeError(
            f"Gold partition INSERT failed for {full_table} [{partition_label}]: {e}"
        ) from e
    finally:
        conn.close()

    return result


def _append_fct_standings_coverage_check(report) -> None:
    """E2: append a two-tier coverage CheckResult for fct_standings (#428).

    Measures the fraction of standings rows whose team_id was resolved via
    the canonical resolver (``team_id_source = 'fbref_canonical'``) vs the
    per-source fallback (``'sofascore_orphan'`` / ``'fotmob_orphan'``, #702).
    Uses two-tier severity:

      * ``coverage >= 95%`` -> OK
      * ``50% <= coverage < 95%`` -> WARNING (drop in resolver match-rate)
      * ``coverage < 50%`` -> ERROR-grade signal, but the check is wired as
        WARNING per the E2 spec (orphans are tracked, not blocking).

    Implemented inline because the universal CHECK registry has no two-tier
    ``coverage`` primitive yet — see CLAUDE.md Gold/DQ section. When
    ``coverage()`` lands in ``data_quality.py`` this helper should be folded
    into the main check list.
    """
    from utils.data_quality import CheckResult, _get_conn

    name = "coverage[fct_standings.team_id_source='fbref_canonical']"
    sql = (
        "SELECT "
        "  COUNT(*) AS total, "
        "  COUNT_IF(team_id_source = 'fbref_canonical') AS resolved "
        "FROM iceberg.gold.fct_standings"
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
            details = "fct_standings is empty — coverage skipped"
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
        logger.exception("fct_standings coverage check raised")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# #432: final star-schema DQ gate (design docs/design/gold-star-schema.md §6
# rule 8 + §7 step 6). Covers the live star tables (8 dims + 14 facts).
# fct_team_elo / fct_player_market_value / fct_player_fifa_rating are pointwise
# off-field facts (no league/season cols) → their PK/FK are asserted pointwise
# below, NOT via _STAR_FACT_TABLES. #430 added fct_player_salary to the list and
# moved market_value out of it (market_value dropped league/season). fct_event /
# fct_match_rating are outside the 17-fact star design (own DQ in e3_dq/e4_dq).
# All thresholds carry their live baseline + measure date.
# ---------------------------------------------------------------------------

# Star facts (design §2) that exist today. Every one carries (league, season).
_STAR_FACT_TABLES = [
    'gold.fct_team_match',
    'gold.fct_match_timeline',
    'gold.fct_shot',
    'gold.fct_player_match',
    'gold.fct_lineup',
    'gold.fct_player_unavailable',
    'gold.fct_match_odds',
    'gold.fct_player_season_stats',
    'gold.fct_keeper_season_stats',
    'gold.fct_team_season_stats',
    'gold.fct_standings',
    'gold.fct_manager_stint',
    'gold.fct_transfer',
    'gold.fct_player_salary',
]


def _star_gate_pk_checks() -> List:
    """Design-grain PK uniqueness for the 3 facts missing from the central
    registry (their local DQ lives in e3_dq/e4_dq and runs in earlier DAGs;
    the final gate re-asserts the DESIGN grain independently)."""
    from utils.data_quality import CHECK

    return [
        # Design §4.3 — one row per shot.
        CHECK.no_duplicates(
            'gold.fct_shot', pk=['match_id', 'shot_id'],
            name='star_pk[fct_shot(match_id,shot_id)]',
        ),
        # Design §4.7 — e4_dq checks only the odds_canonical surrogate
        # (trivially unique); the business grain is asserted nowhere else.
        CHECK.no_duplicates(
            'gold.fct_match_odds',
            pk=['match_id', 'bookmaker', 'market', 'is_closing'],
            name='star_pk[fct_match_odds(match_id,bookmaker,market,is_closing)]',
        ),
        # Design §4.5 PK (match_id, player_id), scoped to resolved rows:
        # ESPN rows with an unresolved player carry NULL player_id and would
        # collapse into false dup buckets (runner uses COUNT-COUNT DISTINCT).
        # Baseline 2026-06-12: 0 dups on the design PK among resolved rows.
        CHECK.no_duplicates(
            'gold.fct_lineup', pk=['match_id', 'player_id'],
            where='player_id IS NOT NULL',
            name='star_pk[fct_lineup(match_id,player_id) resolved]',
        ),
        # issue #431 — fct_team_elo design grain: one row per team per date.
        # Not in _STAR_FACT_TABLES (no league/season cols → excluded from the
        # league/season FK comprehension), so its PK is asserted pointwise here.
        CHECK.no_duplicates(
            'gold.fct_team_elo', pk=['team_id', 'elo_date'],
            name='star_pk[fct_team_elo(team_id,elo_date)]',
        ),
        # issue #430 — three player-money facts. salary carries (league,
        # season) and lives in _STAR_FACT_TABLES; market_value / fifa_rating are
        # pointwise (no league/season), so their design PK is asserted only here.
        CHECK.no_duplicates(
            'gold.fct_player_salary', pk=['player_id', 'league', 'season'],
            name='star_pk[fct_player_salary(player_id,league,season)]',
        ),
        CHECK.no_duplicates(
            'gold.fct_player_market_value',
            pk=['player_id', 'valuation_date', 'source'],
            name='star_pk[fct_player_market_value(player_id,valuation_date,source)]',
        ),
        CHECK.no_duplicates(
            'gold.fct_player_fifa_rating', pk=['player_id', 'fifa_edition'],
            name='star_pk[fct_player_fifa_rating(player_id,fifa_edition)]',
        ),
    ]


def _star_gate_league_season_fk_checks() -> List:
    """league -> dim_competition, season -> dim_season for every star fact.

    ERROR severity: both dims render straight from configs/medallion YAMLs
    and facts carry the same partition slugs — an orphan means a config hole,
    not a data wart. Baseline 2026-06-12: 0 orphans across all 14 facts.
    """
    from utils.data_quality import CHECK

    return [
        *[CHECK.ref_integrity(t, 'gold.dim_competition', 'league')
          for t in _STAR_FACT_TABLES],
        *[CHECK.ref_integrity(t, 'gold.dim_season', 'season')
          for t in _STAR_FACT_TABLES],
    ]


def _star_gate_dim_fk_checks() -> List:
    """Missing fct -> dim soft-FK pairs (design §3-5 FK lists).

    ERROR only where the id comes from the same xref the dim is built from
    (orphans impossible by construction). Everything else is WARNING with
    two-tier orphan-ROW-rate thresholds (#432 rate mode) — orphan-prefixed
    ids (tm_/ws_/fb_<slug>) are kept by design (rule 2) and only their share
    is policed. NULL-key shares are measured by the coverage checks at the
    end (ref_integrity ignores NULL keys by contract).
    All baselines measured live 2026-06-12.
    """
    from utils.data_quality import CHECK

    return [
        # ----- ERROR: FBref-spine team ids, same xref as dim_team -----
        CHECK.ref_integrity('gold.fct_team_match', 'gold.dim_team', 'team_id'),
        CHECK.ref_integrity('gold.fct_team_match', 'gold.dim_team',
                            'opponent_id', parent_key='team_id'),

        # ----- fct_team_elo team_id -> dim_team (issue #431) -----
        # APL ClubElo names resolve to dim_team canonicals via xref_team, but a
        # name absent from team_aliases.yaml falls back to a 'ce_<slug>' orphan
        # (kept by design, rule 2). WARNING rate-mode polices the orphan share.
        # Baseline 2026-06-15: 6.1% (168/2760) — 'Forest' + relegated teams
        # present only in clubelo_ratings_historical (#589). After the #716
        # 10-season backfill + an xref_team rebuild it is 0% live (13180 rows,
        # 34 historical APL teams all resolve) — comfortably under the 2% warn
        # floor. Re-baseline here if a future clubelo name lands unmapped.
        CHECK.ref_integrity('gold.fct_team_elo', 'gold.dim_team', 'team_id',
                            warn_rate=0.02, error_rate=0.10, severity='WARNING'),

        # ----- fct_shot (baseline: team 0%, player 0.52%, match 0%) -----
        CHECK.ref_integrity('gold.fct_shot', 'gold.dim_team', 'team_id',
                            warn_rate=0.01, error_rate=0.05, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_shot', 'gold.dim_player', 'player_id',
                            warn_rate=0.02, error_rate=0.05, severity='WARNING'),
        # assist_player_id is all-NULL today (FBref shot feed dead since
        # Feb 2026; Understat carries no assist id) — vacuous pass until a
        # source returns. Same thresholds as player_id when it does.
        CHECK.ref_integrity('gold.fct_shot', 'gold.dim_player',
                            'assist_player_id', parent_key='player_id',
                            warn_rate=0.02, error_rate=0.05, severity='WARNING'),
        # e3_dq only asserts fct_shot -> silver.xref_match; the star FK is
        # to dim_match (alt-hex divergence risk, see fct_lineup #242).
        CHECK.ref_integrity('gold.fct_shot', 'gold.dim_match', 'match_id',
                            warn_rate=0.005, error_rate=0.02, severity='WARNING'),

        # ----- fct_lineup (baseline: all 0% among non-NULL keys) -----
        CHECK.ref_integrity('gold.fct_lineup', 'gold.dim_team', 'team_id',
                            warn_rate=0.02, error_rate=0.10, severity='WARNING'),
        # #819: the #814 0.30 loose ceiling was a workaround for a wrong
        # diagnosis. The xref_player bridge is NOT broken — before cross-source
        # dedup ~93-99% of sofascore/whoscored/espn (28% fotmob) rows resolve to
        # 'fb_' and then win under lineup_source='fbref' via source_priority.
        # The 13.4% orphans were non-FBref rows in FBref-COVERED matches whose
        # player failed to resolve (xref_player per-season coverage gradient) —
        # pure duplicates of the authoritative FBref lineup. fct_lineup.sql now
        # drops them (the fbref_covered_matches filter). Live 2026-06-27: orphan
        # 1.7% (2508/147123), all ESPN players in ESPN-only matches (no FBref
        # twin to dedup against). Restore a tight ceiling (~3× floor); warn ~2×.
        CHECK.ref_integrity('gold.fct_lineup', 'gold.dim_player', 'player_id',
                            warn_rate=0.03, error_rate=0.05, severity='WARNING'),
        # Whole-table complement to the fbref-scoped ERROR check in e3_dq.
        # Orphans = ESPN-only / non-FBref-only matches (espn_/fm_/ss_/ws_ pseudo
        # match-ids) with no FBref-spine dim_match row — a fixed, stable set.
        # #819: dropping the FBref-covered non-FBref duplicates shrank the
        # denominator (258k→150k rows) without touching that orphan set, so the
        # rate rose 1.6%→2.7% live (2026-06-27: 4030/149699). Re-baseline
        # warn 0.02→0.04, error 0.05→0.06 so the stable floor passes clean.
        CHECK.ref_integrity('gold.fct_lineup', 'gold.dim_match', 'match_id',
                            warn_rate=0.04, error_rate=0.06, severity='WARNING',
                            name='star_fk[fct_lineup.match_id->dim_match all-sources]'),

        # ----- fct_match_odds (baseline 0%; matchhistory fixture-bridge) -----
        CHECK.ref_integrity('gold.fct_match_odds', 'gold.dim_match', 'match_id',
                            warn_rate=0.05, error_rate=0.20, severity='WARNING'),

        # ----- player facts -> dim_player (design FK; the existing checks
        # ----- against dim_player_attributes stay untouched) -----
        CHECK.ref_integrity('gold.fct_player_match', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.01, error_rate=0.05, severity='WARNING'),
        # ws_-prefixed orphans ≈12.6% (1174/9346) — by design (rule 2).
        CHECK.ref_integrity('gold.fct_player_unavailable', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.15, error_rate=0.30, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_player_unavailable', 'gold.dim_team',
                            'team_id',
                            warn_rate=0.02, error_rate=0.10, severity='WARNING'),
        # player_id/team_id FKs already exist for fct_match_timeline —
        # related_player_id (assist / sub-ON player, #427) was missing.
        CHECK.ref_integrity('gold.fct_match_timeline', 'gold.dim_player',
                            'related_player_id', parent_key='player_id',
                            warn_rate=0.05, error_rate=0.15, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_player_season_stats', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.05, error_rate=0.15, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_keeper_season_stats', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.05, error_rate=0.15, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_player_market_value', 'gold.dim_player',
                            'player_id', parent_key='player_id',
                            warn_rate=0.05, error_rate=0.15, severity='WARNING'),
        # issue #430 — salary keeps 'cap_' orphans, fifa_rating keeps 'sf_'
        # orphans; both kept by design (rule 2), policed by rate-mode. #814 saw
        # 100% orphan in the CURRENT season (526/526 cap_, 546/546 sf_) after the
        # #712 rebuild first populated these facts, and dropped error_rate to
        # WARNING-only. #815 root-caused it as STALE silver, NOT a resolver
        # failure: the manual #712 gold rebuild (run_gold_transform) never re-ran
        # the capology/sofifa silver DAGs, so silver.{capology_player_salaries,
        # sofifa_player_profile}.canonical_id stayed NULL while a freshly-rebuilt
        # xref_player already resolved ~90.5%/85% of them. Re-running silver
        # restores the structural floor (cap_ ≈ 9.5%, sf_ ≈ 15%: roster/loan-out
        # players with no FBref appearance). error_rate restored (≈2× floor).
        CHECK.ref_integrity('gold.fct_player_salary', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.12, error_rate=0.25, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_player_fifa_rating', 'gold.dim_player',
                            'player_id',
                            warn_rate=0.18, error_rate=0.35, severity='WARNING'),

        # ----- NULL-key shares (ref_integrity ignores NULLs by contract) -----
        # Baseline 99.7% non-NULL (172/58580 NULL).
        CHECK.coverage('gold.fct_shot', column='player_id',
                       warn_threshold=0.95, error_threshold=0.85),
        # #819: dropping the FBref-covered non-FBref duplicates (their unresolved
        # player twins) also removes ~82k NULL player_id rows, so the non-NULL
        # share jumps from the pre-#819 ~67% to 98.2% live (2026-06-27:
        # 147123/149888 non-NULL). Only ESPN-only matches with unresolved players
        # keep NULL. Tighten the lower bounds (floor 98.2%, warn ~3pp below).
        CHECK.coverage('gold.fct_lineup', column='player_id',
                       warn_threshold=0.95, error_threshold=0.90),
    ]


def _star_gate_grain_checks() -> List:
    """Grain sanity (issue #432): violations counted via HAVING subqueries.

    ``value`` = number of rows living in violating groups (0 = healthy).
    """
    from utils.data_quality import CHECK

    return [
        # Long format: exactly 2 rows (home + away) per match — by
        # construction, so ERROR. Baseline 2026-06-12: 0 violations.
        CHECK.row_count(
            'gold.fct_team_match', min_rows=0, max_rows=0,
            where=("match_id IN (SELECT match_id FROM iceberg.gold.fct_team_match "
                   "GROUP BY match_id HAVING COUNT(*) <> 2)"),
            severity='ERROR',
            name='star_grain[fct_team_match=2rows/match]',
        ),
        # ~20 teams per (league, season): 18 (Bundesliga/Ligue 1) to 24
        # (Championship). PK uniqueness is asserted separately, so COUNT(*)
        # equals the team count. WARNING — approximate invariant.
        # Baseline 2026-06-12: standings 1 group / season-stats 10 groups,
        # all exactly 20.
        CHECK.row_count(
            'gold.fct_standings', min_rows=0, max_rows=0,
            where=("(league, season) IN (SELECT league, season "
                   "FROM iceberg.gold.fct_standings "
                   "GROUP BY league, season HAVING COUNT(*) NOT BETWEEN 18 AND 24)"),
            severity='WARNING',
            name='star_grain[fct_standings~20teams/league-season]',
        ),
        CHECK.row_count(
            'gold.fct_team_season_stats', min_rows=0, max_rows=0,
            where=("(league, season) IN (SELECT league, season "
                   "FROM iceberg.gold.fct_team_season_stats "
                   "GROUP BY league, season HAVING COUNT(*) NOT BETWEEN 18 AND 24)"),
            severity='WARNING',
            name='star_grain[fct_team_season_stats~20teams/league-season]',
        ),
    ]


def build_star_gate_checks() -> List:
    """#432: final star-schema gate — design-PK / league+season FK /
    missing dim-FK with orphan-rate thresholds / grain sanity.

    Appended to the ``validate_gold_quality`` registry (the last task of
    ``dag_transform_fbref_gold``, which the master pipeline triggers after
    e3/e4 — so every star table is already materialised when this runs).
    """
    return (
        _star_gate_pk_checks()
        + _star_gate_league_season_fk_checks()
        + _star_gate_dim_fk_checks()
        + _star_gate_grain_checks()
    )


def validate_gold_quality() -> Dict[str, Any]:
    """Run Gold-layer DQ checks — PK uniqueness, ref integrity, point-in-time.

    Raises AirflowException if any ERROR-severity check fails. WARNING-level
    checks are logged but do not fail the DAG.
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks
    from utils.xref_dq import build_e1_5_post_cutover_checks

    checks = [
        # ========== PK uniqueness — ERROR ==========
        CHECK.no_duplicates('gold.dim_match',        pk=['match_id']),
        # #425 star grains: dim_team = one row per club, dim_player = one row
        # per player — season is no longer part of either PK.
        CHECK.no_duplicates('gold.dim_team',         pk=['team_id']),
        CHECK.no_duplicates('gold.dim_player',       pk=['player_id']),
        CHECK.no_duplicates('gold.fct_team_match',   pk=['match_id', 'team_id']),
        # #426 star design §4.4: fct_player_match PK = (match_id, player_id).
        CHECK.no_duplicates('gold.fct_player_match', pk=['match_id', 'player_id']),
        # E5 → #426 star design §4.6: PK = (match_id, player_id) — a player
        # cannot be unavailable twice for one match (verified dup-free on live
        # data pre-cutover). Empty fallback (0 rows) passes trivially.
        CHECK.no_duplicates(
            'gold.fct_player_unavailable',
            pk=['match_id', 'player_id'],
        ),

        # ========== No NULLs in PKs — ERROR ==========
        CHECK.no_nulls('gold.dim_match',       cols=['match_id', 'match_date']),
        CHECK.no_nulls('gold.fct_team_match',  cols=['match_id', 'team_id', 'opponent_id']),
        # E5: required keys. NB: `team_id` is intentionally NOT here — cross-
        # source slug mismatches (Wolves/Wolverhampton) leave it NULL by design;
        # coverage is observed via the WARNING-severity row_count check below.
        # #426: match_date dropped from the fact (context lives in dim_match).
        CHECK.no_nulls('gold.fct_player_unavailable',
                       cols=['match_id', 'player_id']),

        # ========== Referential integrity — ERROR ==========
        CHECK.ref_integrity('gold.fct_team_match',   'gold.dim_match', 'match_id'),
        CHECK.ref_integrity('gold.fct_player_match', 'gold.dim_match', 'match_id'),
        # fct_player_match → dim_player_attributes (snapshot grain per
        # canonical_id, T4). #426 renamed the child column to player_id; #696
        # aligned the dim too (player_id_canonical → player_id).
        CHECK.ref_integrity(
            'gold.fct_player_match',
            'gold.dim_player_attributes',
            'player_id',
            parent_key='player_id',
        ),
        # E5: every unavailability row must point at a real Gold match.
        # SQL already filters bridge failures, so 0 orphans by construction.
        CHECK.ref_integrity('gold.fct_player_unavailable', 'gold.dim_match', 'match_id'),

        # ========== Value ranges — WARNING ==========
        CHECK.value_range('gold.fct_team_match', 'goals_for',  min_val=0, max_val=20,
                          severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'possession_pct', min_val=0, max_val=100,
                          severity='WARNING'),
        # issue #95: v2 multi-source columns (#426 names: xg/xga/xa). xG/xGA/NPxG
        # per team-match rarely exceed 6 (extreme top-tier blowouts ≈4); cap at
        # 10 to catch parser regressions (xG=999) without false-positives. ppda
        # upper bound 50 (high-press teams ≈8, ultra-passive 30+). NULL for
        # non-FBref games.
        CHECK.value_range('gold.fct_team_match', 'xg',
                          min_val=0, max_val=10, severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'xga',
                          min_val=0, max_val=10, severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'npxg',
                          min_val=0, max_val=10, severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'ppda',
                          min_val=0, max_val=50, severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'pass_accuracy_pct',
                          min_val=0, max_val=100, severity='WARNING'),
        # issue #97: FotMob 5th source. xa (team-grain xA, FotMob-only)
        # and xgot per team-match rarely exceed 4; cap 10 to catch parser regressions.
        CHECK.value_range('gold.fct_team_match', 'xa',
                          min_val=0, max_val=10, severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'xgot',
                          min_val=0, max_val=10, severity='WARNING'),

        # ----- issue #46: fct_player_match multi-source xG/xA/rating sanity -----
        # xG/xA per single match: top observed values редко превышают 3.0 даже
        # для хет-триков; верхний bound 5.0 ловит явные парсер-выбросы (xG=999)
        # без false-positive'ов. Не value_range на goals/assists (могут быть
        # 4+ в редких матчах) и minutes (FBref гарантирует [0, 120]).
        CHECK.value_range('gold.fct_player_match', 'xg',
                          min_val=0, max_val=5, severity='WARNING'),
        CHECK.value_range('gold.fct_player_match', 'xa',
                          min_val=0, max_val=5, severity='WARNING'),
        # rating: SofaScore-источник, шкала [0, 10]; NULL для матчей без оценки.
        CHECK.value_range('gold.fct_player_match', 'rating',
                          min_val=0, max_val=10, severity='WARNING'),

        # ========== E5: fct_player_unavailable observability — WARNING ==========
        # season теперь varchar slug ('2021'..'2526'), per charter S2 (#388);
        # value_range сравнивал бы varchar с числовым литералом → type-error в
        # Trino. Range валидируем через TRY_CAST в row_count-предикате (тот же
        # приём, что у coverage-проверок ниже). Bronze собирается с сезона
        # 2020/21 ('2021'); верх '2930' даёт запас на несколько сезонов.
        CHECK.row_count(
            'gold.fct_player_unavailable',
            min_rows=0, max_rows=0,
            where="TRY_CAST(season AS integer) NOT BETWEEN 2021 AND 2930",
            severity='WARNING',
            name='value_range[gold.fct_player_unavailable.season slug]',
        ),

        # Cross-source team_id coverage. WhoScored team_name -> team_slug is
        # best-effort (e.g. "Wolverhampton" vs "Wolves" leaves team_id NULL).
        # #432: absolute row ceiling replaced with two-tier coverage —
        # baseline 100% non-NULL (9346/9346, measured 2026-06-12).
        CHECK.coverage(
            'gold.fct_player_unavailable',
            condition='team_id IS NOT NULL',
            warn_threshold=0.95, error_threshold=0.85,
            name='coverage[fct_player_unavailable.team_id non-NULL]',
        ),
        # player_id resolution share ('ws_<id>' fallbacks) is policed by the
        # rate-mode FK to dim_player in build_star_gate_checks (#432) — the
        # old 1500-row ceiling proxy was retired with it.

        # ============================================================
        # issue #427: fct_match_timeline — unified per-event chronicle.
        # ============================================================

        # ----- PK + critical attrs — ERROR -----
        CHECK.no_duplicates('gold.fct_match_timeline', pk=['match_id', 'event_seq']),
        # player_id/team_id intentionally NOT here — xref orphans are
        # legitimately NULL (orphan-tolerant design, same as sibling facts).
        CHECK.no_nulls('gold.fct_match_timeline',
                       cols=['match_id', 'event_seq', 'event_type', 'period',
                             'minute', 'score_home_after', 'score_away_after']),
        # event_type dictionary — the CHECK registry has no accepted_values
        # primitive, so a zero-tolerance row_count guards the 8-value enum.
        CHECK.row_count(
            'gold.fct_match_timeline',
            min_rows=0, max_rows=0,
            where=("event_type NOT IN ('goal', 'own_goal', 'penalty_goal', "
                   "'penalty_missed', 'yellow_card', 'second_yellow', "
                   "'red_card', 'substitution')"),
            severity='ERROR',
            name='accepted_values[gold.fct_match_timeline.event_type]',
        ),
        # Unbridged WhoScored fallback matches legitimately carry synthetic
        # 'whoscored_raw_<game_id>' ids that are absent from dim_match —
        # exclude them, hard-fail on any other orphan.
        CHECK.ref_integrity(
            'gold.fct_match_timeline', 'gold.dim_match', 'match_id',
            where="match_id NOT LIKE 'whoscored_raw_%'",
        ),

        # ----- Soft FKs + ranges — WARNING -----
        # #432: rate thresholds added (baseline 0% orphans, 2026-06-12) —
        # tolerate a small unbridged share instead of firing on the first id.
        CHECK.ref_integrity('gold.fct_match_timeline', 'gold.dim_player',
                            'player_id', warn_rate=0.05, error_rate=0.15,
                            severity='WARNING'),
        CHECK.ref_integrity('gold.fct_match_timeline', 'gold.dim_team',
                            'team_id', warn_rate=0.05, error_rate=0.15,
                            severity='WARNING'),
        CHECK.value_range('gold.fct_match_timeline', 'minute',
                          min_val=0, max_val=120, severity='WARNING'),
        # max 25: live corpus has legitimate 90+16..90+20 stoppage events
        # (modern APL added time) — 15 fired permanent false-positives.
        CHECK.value_range('gold.fct_match_timeline', 'minute_added',
                          min_val=1, max_val=25, severity='WARNING'),
        CHECK.value_range('gold.fct_match_timeline', 'score_home_after',
                          min_val=0, max_val=20, severity='WARNING'),
        CHECK.value_range('gold.fct_match_timeline', 'score_away_after',
                          min_val=0, max_val=20, severity='WARNING'),

        # ============================================================
        # issue #613: fct_match_officials — per-match officiating crew.
        # ============================================================
        # ----- PK + critical attrs — ERROR -----
        CHECK.no_duplicates('gold.fct_match_officials', pk=['match_id', 'role']),
        # referee_id intentionally NOT here — pure assistants / VAR-only
        # officials are single-source (FBref) and legitimately carry a NULL
        # canonical id (#613 design).
        CHECK.no_nulls('gold.fct_match_officials',
                       cols=['match_id', 'role', 'official_name']),
        # role dictionary — zero-tolerance row_count guards the 5-value enum
        # (same idiom as fct_match_timeline.event_type; no accepted_values
        # primitive in the CHECK registry).
        CHECK.row_count(
            'gold.fct_match_officials',
            min_rows=0, max_rows=0,
            where=("role NOT IN ('referee', 'ar1', 'ar2', "
                   "'fourth_official', 'var')"),
            severity='ERROR',
            name='accepted_values[gold.fct_match_officials.role]',
        ),
        # match_id must point at a real Gold match — ERROR.
        CHECK.ref_integrity('gold.fct_match_officials', 'gold.dim_match', 'match_id'),
        # ----- Soft FK — WARNING -----
        # referee_id → dim_referee is best-effort. ref_integrity ignores NULL
        # child keys, so the denominator is only the populated ids (main referee
        # + assistants who also referee elsewhere) — those are xref canonicals
        # and thus always in dim_referee, so the rate is ~0. Rate-mode WARNING
        # catches a regression without firing on the by-design NULLs.
        CHECK.ref_integrity('gold.fct_match_officials', 'gold.dim_referee',
                            'referee_id', warn_rate=0.05, error_rate=0.15,
                            severity='WARNING'),

        # ============================================================
        # E2: master-data dims (dim_venue / dim_referee / dim_competition /
        # dim_season) + fct_standings (ex-dim_standings, renamed #428).
        # Mirrors the existing dim_match / dim_team / dim_player block but
        # adapted to the E2 PK shapes and the R0.4 (_canonical, _source,
        # _version) schema-versioning trio.
        # ============================================================

        # ----- E2: PK uniqueness — ERROR -----
        CHECK.no_duplicates('gold.dim_venue',       pk=['venue_id']),
        CHECK.no_duplicates('gold.dim_referee',     pk=['referee_id']),
        # Composite PK — one standings row per (league, season, team).
        CHECK.no_duplicates('gold.fct_standings',   pk=['league', 'season', 'team_id']),
        # #425: PK renamed to the design keys — league slug / season slug.
        CHECK.no_duplicates('gold.dim_competition', pk=['league']),
        CHECK.no_duplicates('gold.dim_season',      pk=['season']),

        # ----- E2: NOT NULL on PKs + critical attrs — ERROR -----
        CHECK.no_nulls('gold.dim_venue',       cols=['venue_id', 'venue_name']),
        CHECK.no_nulls('gold.dim_referee',     cols=['referee_id', 'referee_name']),
        # fct_standings has no canonical column — its source-tracking is via
        # team_id_source (covered by the coverage check below) и standings_source
        # (#702 provenance: 'sofascore'/'fotmob'). Here we just guarantee the PK
        # trio + the load-bearing numeric attrs + provenance are present.
        # #428: mp → played (design §5.5).
        CHECK.no_nulls('gold.fct_standings',
                       cols=['league', 'season', 'team_id', 'points', 'played',
                             'standings_source']),
        CHECK.no_nulls('gold.dim_competition',
                       cols=['league', 'competition_name', 'country']),
        CHECK.no_nulls('gold.dim_season',
                       cols=['season', 'season_name',
                             'start_date', 'end_date']),

        # ----- E2: ref_integrity fct_standings.team_id → dim_team — WARNING -----
        # Soft FK: rows whose team_id_source='sofascore_orphan' are intentionally
        # NOT in dim_team (resolver couldn't match — they are tracked but not
        # joined). Only the canonical-resolved rows must point at a real
        # dim_team key. Implemented as row_count(max_rows=0) over the
        # offending predicate because the universal CHECK.ref_integrity has
        # no WHERE-filter mode (yet).
        # Severity = WARNING (not ERROR) because the upstream alias coverage
        # is incomplete by design — SofaScore variants like 'Liverpool FC'
        # map to a distinct `liverpool_fc` canonical_id whereas dim_team uses
        # `liverpool`. The orphan share is surfaced via the coverage WARNING.
        CHECK.row_count(
            'gold.fct_standings', min_rows=0, max_rows=0,
            where=("team_id_source = 'fbref_canonical' "
                   "AND team_id NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"),
            severity='WARNING',
            name='ref_integrity[fct_standings.team_id->dim_team]',
        ),

        # NB (#425): the R0.4 canonical/source/version trio left the slim
        # star dims — the canonical_completeness checks went with it. Only
        # dim_venue keeps venue_source (feeds the orphan-rate check below).

        # ----- E2: dim_venue alias coverage / dup report (issue #145) — WARNING -----
        # Every venue whose raw name failed to match venue_aliases.yaml falls
        # back to a normalised-name hash id and is marked venue_source='orphan'.
        # Orphans are the dedup/merge candidates a curator must triage (new
        # stadium, unseen spelling, sponsor rename). WARNING (not ERROR): the
        # table still loads and orphans carry a stable id — but a non-zero count
        # means the curated dictionary needs extending. city/country are NULL
        # only for orphans, so this doubles as the city/country-completeness
        # signal for in-scope venues.
        CHECK.row_count(
            'gold.dim_venue', min_rows=0, max_rows=0,
            where="venue_source = 'orphan'",
            severity='WARNING',
            name='unmapped_venues[dim_venue]',
        ),

        # ----- E2: value-range sanity (WARNING) -----
        # APL has 38 matches/season (max 46 across other supported leagues).
        # Points hard ceiling: 38 * 3 = 114 -> round to 120 for safety margin.
        CHECK.value_range('gold.fct_standings', 'points',
                          min_val=0, max_val=120, severity='WARNING'),
        CHECK.value_range('gold.fct_standings', 'played',
                          min_val=0, max_val=46,  severity='WARNING'),
        CHECK.value_range('gold.fct_standings', 'position',
                          min_val=1, max_val=24,  severity='WARNING'),
        # #702: standings_source provenance — only the two wired sources are
        # valid. Any other value means a producer bug in fct_standings.sql.
        CHECK.row_count(
            'gold.fct_standings', min_rows=0, max_rows=0,
            where="standings_source NOT IN ('sofascore', 'fotmob')",
            severity='ERROR',
            name='enum[fct_standings.standings_source]',
        ),

        # ============================================================
        # dim_manager — plain per-manager dictionary since #425 (the SCD-2
        # stint table moved out; it returns as fct_manager_stint in #429,
        # together with its scd2_no_overlap check — the primitive stays in
        # data_quality.py).
        # ============================================================
        CHECK.no_duplicates('gold.dim_manager', pk=['manager_id']),
        CHECK.no_nulls('gold.dim_manager', cols=['manager_id', 'manager_name']),
        CHECK.ref_integrity(
            'gold.dim_manager',
            'silver.xref_manager',
            'manager_id',
            parent_key='canonical_id',
        ),

        # ============================================================
        # #429: fct_manager_stint — SCD-2 employment history (the stint
        # table that moved out of dim_manager in #433). One row per
        # (manager × team × stint), closed-open [valid_from, valid_to).
        # ============================================================
        # PK uniqueness — valid_from distinguishes returning managers
        # (Mourinho-Chelsea-2004 vs Mourinho-Chelsea-2013).
        CHECK.no_duplicates(
            'gold.fct_manager_stint',
            pk=['manager_id', 'team_id', 'valid_from'],
        ),
        CHECK.no_nulls(
            'gold.fct_manager_stint',
            cols=['manager_id', 'team_id', 'valid_from', 'matches_in_charge'],
        ),
        # For a single team at any date there is at most ONE active manager.
        # Adjacent stints sharing an endpoint are OK (closed-open intervals).
        CHECK.scd2_no_overlap(
            'gold.fct_manager_stint',
            pk_cols=['team_id'],
        ),
        # Interval sanity: a stint can never end before it starts (DoD).
        CHECK.row_count(
            'gold.fct_manager_stint', min_rows=0, max_rows=0,
            where='valid_to < valid_from',
            name='valid_to_before_valid_from[fct_manager_stint]',
        ),
        # Soft FK → star dims. Both ids come from the same xref tables the
        # dims are built from, so ERROR severity holds by construction.
        CHECK.ref_integrity('gold.fct_manager_stint', 'gold.dim_manager',
                            'manager_id'),
        CHECK.ref_integrity('gold.fct_manager_stint', 'gold.dim_team',
                            'team_id'),

        # ============================================================
        # #429: fct_transfer — player transfers, pure projection of
        # silver.transfermarkt_transfers. Orphan rows are KEPT with
        # 'tm_'-prefixed ids (≈18% players, most clubs) — FK checks to
        # dims are therefore WARNING-severity (dim_match referee/venue
        # precedent), and the orphan share itself is monitored below.
        # ============================================================
        CHECK.no_duplicates(
            'gold.fct_transfer',
            pk=['player_id', 'transfer_date', 'from_team_id', 'to_team_id'],
        ),
        CHECK.no_nulls(
            'gold.fct_transfer',
            cols=['player_id', 'transfer_date', 'from_team_id', 'to_team_id',
                  'is_loan', 'is_upcoming'],
        ),
        # #432: rate thresholds replace the absolute orphan_players row_count
        # proxy. player_id: a historized fct_transfer (#788) references players
        # outside the current dim_player FBref-spine (left the league + non-spine
        # clubs). #814: the #712 rebuild first surfaced this at 97.6% orphan; the
        # #788 TM alias-fix + two-pass resolver cut it to 43.7% live (gradient
        # 2526=10.3% → 1920=57.1%, aggregate pulled up by history). WARNING-only
        # (never escalate). warn 0.55 re-baselined for historized transfers (was
        # 0.27, current-season era): passes the honest historical orphan, still
        # alerts on a regression back toward the resolver-gap state. Full
        # player-spine historization = #825; orphan share tracked in #815.
        CHECK.ref_integrity('gold.fct_transfer', 'gold.dim_player',
                            'player_id', warn_rate=0.55,
                            severity='WARNING'),
        # from/to team: foreign clubs are legitimately outside dim_team —
        # baseline 82%/69% orphans. warn 0.90 only alerts on a total break
        # (no error_rate: never escalate).
        CHECK.ref_integrity('gold.fct_transfer', 'gold.dim_team',
                            'from_team_id', parent_key='team_id',
                            warn_rate=0.90, severity='WARNING'),
        CHECK.ref_integrity('gold.fct_transfer', 'gold.dim_team',
                            'to_team_id', parent_key='team_id',
                            warn_rate=0.90, severity='WARNING'),

        # ============================================================
        # #425: dim_match — soft FK to every star dim. Team / referee / venue
        # FKs are ERROR — the id comes from the same xref / alias table the dim
        # is built from, so a non-NULL id is orphan-free by construction (LEFT
        # JOIN coverage gaps surface as NULLs, which ref_integrity ignores).
        # #436 tightened referee / venue from WARNING after stable green runs.
        # manager FKs stay WARNING until the FBref scorebox parser
        # (bronze.fbref_match_managers, ~80% coverage) improves — then ERROR.
        # ============================================================
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_team',
                            'home_team_id', parent_key='team_id'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_team',
                            'away_team_id', parent_key='team_id'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_referee',
                            'referee_id'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_venue',
                            'venue_id'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_manager',
                            'home_manager_id', parent_key='manager_id',
                            severity='WARNING'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_manager',
                            'away_manager_id', parent_key='manager_id',
                            severity='WARNING'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_competition',
                            'league'),
        CHECK.ref_integrity('gold.dim_match', 'gold.dim_season',
                            'season'),

        # ============================================================
        # T4: dim_player_attributes — cross-source snapshot per canonical
        # player. Additive относительно dim_player (per-season FBref-only).
        # FotMob coverage низкая (~40%) потому что FotMob Bronze покрывает
        # только APL 2025, а FBref-spine — все сезоны (history). Coverage
        # thresholds выставлены под реальный baseline; tighten после
        # подключения R3 источников (Sofascore/Transfermarkt).
        # ============================================================
        CHECK.no_duplicates('gold.dim_player_attributes',
                            pk=['player_id']),
        CHECK.no_nulls('gold.dim_player_attributes',
                       cols=['player_id']),
        CHECK.ref_integrity(
            'gold.dim_player_attributes',
            'silver.xref_player',
            'player_id',
            parent_key='canonical_id',
        ),
        CHECK.value_range('gold.dim_player_attributes', 'height_cm_fotmob',
                          min_val=140, max_val=220, severity='WARNING'),
        CHECK.coverage('gold.dim_player_attributes', column='height_cm_fotmob',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='dob_fotmob',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='foot_fotmob',
                       warn_threshold=0.30, error_threshold=0.15),
        # SofaScore block — coverage ниже FotMob потому что Bronze покрывает
        # только current APL season (~500 игроков из ~1200 в FBref-spine).
        CHECK.value_range('gold.dim_player_attributes', 'height_cm_sofascore',
                          min_val=140, max_val=220, severity='WARNING'),
        CHECK.coverage('gold.dim_player_attributes', column='height_cm_sofascore',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='dob_sofascore',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='foot_sofascore',
                       warn_threshold=0.30, error_threshold=0.15),
        # Transfermarkt block — Bronze covers APL 2025/26 only, поэтому coverage
        # в full FBref-spine (~5+ сезонов истории) низкая как у FotMob/SS.
        # Бизнес-DoD >80% применим к APL 2025/26 cohort (verify-SQL отдельно).
        CHECK.value_range('gold.dim_player_attributes', 'height_cm_tm',
                          min_val=140, max_val=220, severity='WARNING'),
        CHECK.coverage('gold.dim_player_attributes', column='height_cm_tm',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='dob_tm',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.coverage('gold.dim_player_attributes', column='foot_tm',
                       warn_threshold=0.30, error_threshold=0.15),
        CHECK.value_range('gold.dim_player_attributes',
                          'current_market_value_eur_tm',
                          min_val=0, max_val=300_000_000, severity='WARNING'),
        CHECK.coverage('gold.dim_player_attributes',
                       column='current_market_value_eur_tm',
                       warn_threshold=0.30, error_threshold=0.15),

        # issue #609: SoFIFA EA-rating range checks — relocated here from
        # dim_player_attributes (ratings now live in gold.fct_player_fifa_rating
        # per-(player, fifa_edition)). FIFA card stats bounded 0-99 → ERROR;
        # value/wage game-side estimates loosely bounded → WARNING. The
        # identity/contract *_sofifa cols remain in the dim and were never
        # range-checked (kept as-is, surgical).
        *[
            CHECK.value_range('gold.fct_player_fifa_rating', col,
                              min_val=0, max_val=99, severity='ERROR')
            for col in (
                'overall', 'potential',
                'pace', 'shooting', 'passing',
                'dribbling', 'defending', 'physical',
                'gk_diving', 'gk_handling', 'gk_kicking',
                'gk_positioning', 'gk_reflexes',
            )
        ],
        CHECK.value_range('gold.fct_player_fifa_rating', 'value_eur',
                          min_val=0, max_val=500_000_000, severity='WARNING'),
        CHECK.value_range('gold.fct_player_fifa_rating', 'wage_eur',
                          min_val=0, max_val=5_000_000, severity='WARNING'),

        # ============================================================
        # issue #430: fct_player_market_value — two-source MV timeline, one row
        # per (player_id, valuation_date, source). Pointwise off-field
        # fact (no league/season). The design PK is asserted in
        # build_star_gate_checks (pointwise, like fct_team_elo); here we keep
        # only the NOT-NULL / FK / range checks.
        # ============================================================
        CHECK.no_nulls('gold.fct_player_market_value',
                       cols=['player_id', 'valuation_date',
                             'market_value_eur', 'source']),
        CHECK.ref_integrity(
            'gold.fct_player_market_value',
            'gold.dim_player_attributes',
            'player_id',
            parent_key='player_id',
        ),
        CHECK.value_range('gold.fct_player_market_value', 'market_value_eur',
                          min_val=0, max_val=500_000_000, severity='ERROR'),
        # Coverage `current_market_value_eur_fotmob` в dim_player_attributes:
        # бизнес-DoD issue #11 ≥50% применим к APL 2025/26 cohort, но dim
        # содержит full FBref-spine (~28K canonical_id × все сезоны истории),
        # а FotMob Bronze покрывает только current APL — measured baseline
        # ~1.6%. Threshold выставлен под реальную форму spine; WARNING-only,
        # detects полный регресс FotMob ingest.
        CHECK.coverage('gold.dim_player_attributes',
                       column='current_market_value_eur_fotmob',
                       warn_threshold=0.05, error_threshold=0.01),

        # ============================================================
        # T5: fct_player_season_stats — cross-source per-season stats.
        # FBref-spine + FotMob bridge через silver.xref_player. Outfield
        # only (вратари в fct_keeper_season_stats). Business-витрина:
        # PK + ref_integrity ERROR; audit-diff чеки переехали в _audit.
        # ============================================================
        # #428: PK renamed to plain player_id (design §5.2). #696: the dim
        # (dim_player_attributes) now matches — both plain player_id.
        CHECK.no_duplicates('gold.fct_player_season_stats',
                            pk=['player_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_player_season_stats',
                       cols=['player_id', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_player_season_stats',
            'gold.dim_player_attributes',
            'player_id',
            parent_key='player_id',
        ),
        # #428: new team_id FK (squad with most minutes). WARNING-only —
        # orphan-fallback ids ('fb_<slug>') are intentionally NOT in dim_team.
        CHECK.ref_integrity(
            'gold.fct_player_season_stats',
            'gold.dim_team',
            'team_id',
            severity='WARNING',
        ),
        # Value-range plausibility — bounded domain метрики (ERROR на нарушение
        # домена). T6: HARD_FACT pct metrics single-column (COALESCE WS→SS),
        # MODELED xG/rating with per-source suffix.
        CHECK.value_range('gold.fct_player_season_stats', 'expected_goals',
                          min_val=0, max_val=60, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'non_penalty_xg_understat',
                          min_val=0, max_val=60, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'pass_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'tackle_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'take_on_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        # T6 — SofaScore rating (Opta-style 0-10 scale). ERROR — рейтинг
        # вне диапазона указывает на ingest regression или schema drift.
        CHECK.value_range('gold.fct_player_season_stats', 'rating_sofascore',
                          min_val=0, max_val=10, severity='ERROR'),
        # SofaScore pct metrics — единые HARD_FACT в [0, 100] (ERROR).
        CHECK.value_range('gold.fct_player_season_stats', 'ground_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'aerial_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'total_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_player_season_stats', 'goal_conversion_pct',
                          min_val=0, max_val=100, severity='ERROR'),

        # ============================================================
        # T5: fct_keeper_season_stats — keeper-variant. Зеркальный набор.
        # ============================================================
        # #428: PK renamed to plain player_id (design §5.3) — mirror outfield.
        CHECK.no_duplicates('gold.fct_keeper_season_stats',
                            pk=['player_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_keeper_season_stats',
                       cols=['player_id', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_keeper_season_stats',
            'gold.dim_player_attributes',
            'player_id',
            parent_key='player_id',
        ),
        # #428: new team_id FK — WARNING-only (orphan-fallback tolerated).
        CHECK.ref_integrity(
            'gold.fct_keeper_season_stats',
            'gold.dim_team',
            'team_id',
            severity='WARNING',
        ),

        # ============================================================
        # T5 audit: fct_player_season_stats_audit — DQ-таблица для
        # cross-source согласованности FBref vs FotMob по HARD_FACT.
        # INNER JOIN на оба источника → rows только где обе стороны не-NULL.
        # ERROR: PK uniqueness, ref к main fct (audit ⊆ main fct).
        # WARNING: audit-diff coverage ≥95% rows укладываются в threshold
        #          (план «<5% beyond» в acceptance). Threshold per metric:
        #          1 для счётных событий, 90 для minutes.
        # ============================================================
        CHECK.no_duplicates('gold.fct_player_season_stats_audit',
                            pk=['player_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_player_season_stats_audit',
                       cols=['player_id', 'league', 'season']),
        # #696: audit aligned to plain player_id (matches main fct, #428).
        CHECK.ref_integrity(
            'gold.fct_player_season_stats_audit',
            'gold.fct_player_season_stats',
            'player_id',
            parent_key='player_id',
        ),
        # 6 audit-diff coverage WARNING-only (error_threshold=0). Audit —
        # observability, не gate; ERROR ломал бы DAG при нормальных
        # cross-source расхождениях (mid-season transfer, разные методики
        # подсчёта). NULL diff засчитывается как "not measured" (passed).
        # #564: goals/assists/cards FotMob теперь COALESCE→0 в SQL (NULL=«не
        # было события»); penalties_won/conceded diff-колонки удалены (FotMob
        # не отдаёт сезонные пенальти — были полностью NULL).
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(matches_diff_fotmob) <= 1 OR matches_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.matches]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(minutes_diff_fotmob) <= 90 OR minutes_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.minutes]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(goals_diff_fotmob) <= 1 OR goals_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.goals]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(assists_diff_fotmob) <= 1 OR assists_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.assists]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(yellow_cards_diff_fotmob) <= 1 OR yellow_cards_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.yellow_cards]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(red_cards_diff_fotmob) <= 1 OR red_cards_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.red_cards]'),
        # ----- WhoScored audit (1: только matches есть в event-aggregate) -----
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(matches_diff_whoscored) <= 1 OR matches_diff_whoscored IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.matches_whoscored]'),
        # ----- Understat audit (6) -----
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(matches_diff_understat) <= 1 OR matches_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.matches_understat]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(minutes_diff_understat) <= 90 OR minutes_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.minutes_understat]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(goals_diff_understat) <= 1 OR goals_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.goals_understat]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(assists_diff_understat) <= 1 OR assists_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.assists_understat]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(yellow_cards_diff_understat) <= 1 OR yellow_cards_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.yellow_cards_understat]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(red_cards_diff_understat) <= 1 OR red_cards_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.red_cards_understat]'),

        # ============================================================
        # T5 audit: fct_keeper_season_stats_audit — keeper variant.
        # ============================================================
        CHECK.no_duplicates('gold.fct_keeper_season_stats_audit',
                            pk=['player_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_keeper_season_stats_audit',
                       cols=['player_id', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_keeper_season_stats_audit',
            'gold.fct_keeper_season_stats',
            'player_id',
            parent_key='player_id',  # #696: audit + main fct both plain player_id
        ),
        CHECK.coverage('gold.fct_keeper_season_stats_audit',
                       condition='ABS(matches_diff_fotmob) <= 1 OR matches_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_keeper_season_stats_audit.matches]'),
        CHECK.coverage('gold.fct_keeper_season_stats_audit',
                       condition='ABS(minutes_diff_fotmob) <= 90 OR minutes_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_keeper_season_stats_audit.minutes]'),
        CHECK.coverage('gold.fct_keeper_season_stats_audit',
                       condition='ABS(clean_sheets_diff_fotmob) <= 1 OR clean_sheets_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_keeper_season_stats_audit.clean_sheets]'),
        # WhoScored saves diff (SPADL keeper_save vs FBref `saves` — разная
        # дефиниция; threshold выше: ±5 reasonable cross-source noise).
        CHECK.coverage('gold.fct_keeper_season_stats_audit',
                       condition='ABS(saves_diff_whoscored) <= 5 OR saves_diff_whoscored IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_keeper_season_stats_audit.saves_whoscored]'),

        # ============================================================
        # T6.4 (#94): fct_team_season_stats — cross-source per-season team
        # stats. FBref-spine + Understat/WhoScored/SofaScore через
        # silver.xref_team. PK + ref_integrity (→ dim_team) ERROR; pct и
        # MODELED value-range ERROR на нарушение домена.
        # ============================================================
        # #428: PK renamed to plain team_id (design §5.4).
        CHECK.no_duplicates('gold.fct_team_season_stats',
                            pk=['team_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_team_season_stats',
                       cols=['team_id', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_team_season_stats',
            'gold.dim_team',
            'team_id',
        ),
        # MODELED xG/xA — bounded domain on season-level (≤ ~150 for top APL teams).
        CHECK.value_range('gold.fct_team_season_stats', 'expected_goals',
                          min_val=0, max_val=150, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'expected_goals_against',
                          min_val=0, max_val=150, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'npxg',
                          min_val=0, max_val=150, severity='ERROR'),
        # issue #97: FotMob-only team-grain xA (season SUM). NULL для сезонов вне
        # FotMob-покрытия (value_range игнорирует NULL). Cap 150 как у xG-семейства.
        CHECK.value_range('gold.fct_team_season_stats', 'expected_assists',
                          min_val=0, max_val=150, severity='ERROR'),
        # Pct metrics — все в [0, 100] (ERROR).
        CHECK.value_range('gold.fct_team_season_stats', 'possession_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'save_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'pass_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'takeon_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'accurate_passes_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'possession_pct_avg',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'ground_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'aerial_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'total_duels_won_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'accurate_long_balls_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'set_piece_share_pct',
                          min_val=0, max_val=100, severity='ERROR'),
        # issue #192: team-finance — неотрицательные суммы. NULL вне APL 2025/26
        # покрытия (value_range игнорирует NULL). Верхняя граница — sanity-потолок.
        CHECK.value_range('gold.fct_team_season_stats', 'squad_market_value_eur',
                          min_val=0, max_val=5_000_000_000, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'total_wage_bill_gbp',
                          min_val=0, max_val=2_000_000_000, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'total_wage_bill_eur',
                          min_val=0, max_val=2_000_000_000, severity='ERROR'),

        # ============================================================
        # T6.4 (#94) audit: fct_team_season_stats_audit — cross-source DQ
        # для HARD_FACT diff'ов. INNER FBref ∩ Understat (primary secondary),
        # LEFT WS/SS. WARNING-only — audit observability, не gate. Threshold
        # ±1 для целочисленных событий, ±0.5 для xG-derived (RX2 r ≥ 0.99).
        # NULL diff = "источник отсутствует" → засчитывается как passed.
        # ============================================================
        CHECK.no_duplicates('gold.fct_team_season_stats_audit',
                            pk=['team_id', 'league', 'season']),
        CHECK.no_nulls('gold.fct_team_season_stats_audit',
                       cols=['team_id', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_team_season_stats_audit',
            'gold.fct_team_season_stats',
            'team_id',
            parent_key='team_id',  # #696: audit + main fct both plain team_id
        ),
        # ----- Understat diff (INNER spine: всегда non-NULL) -----
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(matches_diff_understat) <= 1 OR matches_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.matches_understat]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(goals_diff_understat) <= 1 OR goals_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.goals_understat]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(goals_against_diff_understat) <= 1 OR goals_against_diff_understat IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.goals_against_understat]'),
        # Understat не отдаёт shots count в season — no shots_diff_understat check.
        # ----- WhoScored diff (LEFT, NULL when absent) -----
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(matches_diff_whoscored) <= 1 OR matches_diff_whoscored IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.matches_whoscored]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(shots_diff_whoscored) <= 2 OR shots_diff_whoscored IS NULL',
                       warn_threshold=0.85, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.shots_whoscored]'),
        # ----- SofaScore diff (LEFT) -----
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(matches_diff_sofascore) <= 1 OR matches_diff_sofascore IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.matches_sofascore]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(goals_diff_sofascore) <= 1 OR goals_diff_sofascore IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.goals_sofascore]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(shots_diff_sofascore) <= 1 OR shots_diff_sofascore IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.shots_sofascore]'),
        # ----- MODELED xG diff (cross-source us vs ss) -----
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(xg_diff_us_vs_ss) <= 0.5 OR xg_diff_us_vs_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.xg_us_vs_ss]'),
        # ----- FotMob diff (LEFT — NULL when absent; #97). WARNING-only -----
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(matches_diff_fotmob) <= 1 OR matches_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.matches_fotmob]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(goals_diff_fotmob) <= 1 OR goals_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.goals_fotmob]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(goals_against_diff_fotmob) <= 1 OR goals_against_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.goals_against_fotmob]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(shots_diff_fotmob) <= 2 OR shots_diff_fotmob IS NULL',
                       warn_threshold=0.85, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.shots_fotmob]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(shots_on_target_diff_fotmob) <= 2 OR shots_on_target_diff_fotmob IS NULL',
                       warn_threshold=0.85, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.shots_on_target_fotmob]'),
        CHECK.coverage('gold.fct_team_season_stats_audit',
                       condition='ABS(xg_diff_us_vs_fm) <= 0.5 OR xg_diff_us_vs_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_season_stats_audit.xg_us_vs_fm]'),

        # ============================================================
        # issue #46 audit: fct_player_match_audit — cross-source diff на
        # match-grain между FBref (primary spine), SofaScore (INNER secondary
        # spine), Understat (LEFT) и WhoScored (LEFT). 50 diff-колонок:
        # 18 SS + 8 US + 22 WS + 2 modeled xG/xA.
        # WARNING-only по convention (feedback_audit_in_separate_table): audit
        # никогда не должен ERROR-фейлить pipeline — `error_threshold=0.0`.
        # Thresholds: ±1 для целочисленных, ±90 для minutes, ±0.5 для xG/xA.
        # NULL diff = "источник отсутствует" (не ошибка) → засчитывается как
        # passed через `OR <col> IS NULL`.
        # ============================================================
        CHECK.no_duplicates('gold.fct_player_match_audit',
                            pk=['match_id', 'player_id']),
        CHECK.no_nulls('gold.fct_player_match_audit',
                       cols=['match_id', 'player_id']),
        # audit ⊆ main fct (INNER FBref ∩ SofaScore) → каждая audit-строка
        # должна находить парную строку в gold.fct_player_match.
        # #442: audit PK renamed off *_canonical to match parent (player_id).
        CHECK.ref_integrity(
            'gold.fct_player_match_audit',
            'gold.fct_player_match',
            'player_id',
            parent_key='player_id',
        ),

        # ----- SofaScore diff (18 checks, INNER spine: всегда non-NULL) -----
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(minutes_diff_ss) <= 90 OR minutes_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.minutes_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(goals_diff_ss) <= 1 OR goals_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.goals_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(assists_diff_ss) <= 1 OR assists_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.assists_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(own_goals_diff_ss) <= 1 OR own_goals_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.own_goals_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(shots_diff_ss) <= 1 OR shots_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.shots_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(shots_on_target_diff_ss) <= 1 OR shots_on_target_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.shots_on_target_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(yellow_cards_diff_ss) <= 1 OR yellow_cards_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.yellow_cards_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(red_cards_diff_ss) <= 1 OR red_cards_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.red_cards_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(crosses_diff_ss) <= 1 OR crosses_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.crosses_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(fouls_committed_diff_ss) <= 1 OR fouls_committed_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.fouls_committed_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(fouls_drawn_diff_ss) <= 1 OR fouls_drawn_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.fouls_drawn_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(offsides_diff_ss) <= 1 OR offsides_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.offsides_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(tackles_won_diff_ss) <= 1 OR tackles_won_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.tackles_won_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(interceptions_diff_ss) <= 1 OR interceptions_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.interceptions_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(penalty_goals_diff_ss) <= 1 OR penalty_goals_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.penalty_goals_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(penalty_attempts_diff_ss) <= 1 OR penalty_attempts_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.penalty_attempts_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(penalties_won_diff_ss) <= 1 OR penalties_won_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.penalties_won_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(penalties_conceded_diff_ss) <= 1 OR penalties_conceded_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.penalties_conceded_ss]'),

        # ----- Understat diff (8 checks, LEFT JOIN → NULL допустим) -----
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(minutes_diff_us) <= 90 OR minutes_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.minutes_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(goals_diff_us) <= 1 OR goals_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.goals_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(assists_diff_us) <= 1 OR assists_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.assists_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(own_goals_diff_us) <= 1 OR own_goals_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.own_goals_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(shots_diff_us) <= 1 OR shots_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.shots_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(yellow_cards_diff_us) <= 1 OR yellow_cards_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.yellow_cards_us]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(red_cards_diff_us) <= 1 OR red_cards_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.red_cards_us]'),
        # key_passes_diff_ss_us: SS - US (FBref на match-grain не отдаёт)
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(key_passes_diff_ss_us) <= 1 OR key_passes_diff_ss_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.key_passes_ss_us]'),

        # ----- WhoScored diff (22 checks, LEFT JOIN → NULL допустим) -----
        # FBref vs WS (HARD_FACT pairs):
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(goals_diff_ws) <= 1 OR goals_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.goals_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(shots_diff_ws) <= 1 OR shots_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.shots_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(shots_on_target_diff_ws) <= 1 OR shots_on_target_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.shots_on_target_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(yellow_cards_diff_ws) <= 1 OR yellow_cards_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.yellow_cards_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(red_cards_diff_ws) <= 1 OR red_cards_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.red_cards_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(crosses_diff_ws) <= 1 OR crosses_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.crosses_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(fouls_committed_diff_ws) <= 1 OR fouls_committed_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.fouls_committed_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(fouls_drawn_diff_ws) <= 1 OR fouls_drawn_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.fouls_drawn_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(offsides_diff_ws) <= 1 OR offsides_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.offsides_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(tackles_won_diff_ws) <= 1 OR tackles_won_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.tackles_won_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(interceptions_diff_ws) <= 1 OR interceptions_diff_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.interceptions_ws]'),
        # SS vs WS (FBref не отдаёт key_passes/passes/tackles/clearances/...
        # на match-grain → diff = SS - WS). Threshold ±1 — могут шуметь сильнее
        # на passes/touches; калибровка thresholds = followup после первого run.
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(key_passes_diff_ss_ws) <= 1 OR key_passes_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.key_passes_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(passes_diff_ss_ws) <= 1 OR passes_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.passes_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(passes_completed_diff_ss_ws) <= 1 OR passes_completed_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.passes_completed_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(tackles_diff_ss_ws) <= 1 OR tackles_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.tackles_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(clearances_diff_ss_ws) <= 1 OR clearances_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.clearances_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(ball_recoveries_diff_ss_ws) <= 1 OR ball_recoveries_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.ball_recoveries_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(dribbles_attempted_diff_ss_ws) <= 1 OR dribbles_attempted_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.dribbles_attempted_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(dribbles_won_diff_ss_ws) <= 1 OR dribbles_won_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.dribbles_won_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(aerials_won_diff_ss_ws) <= 1 OR aerials_won_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.aerials_won_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(touches_diff_ss_ws) <= 1 OR touches_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.touches_ss_ws]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(dispossessed_diff_ss_ws) <= 1 OR dispossessed_diff_ss_ws IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.dispossessed_ss_ws]'),

        # ----- MODELED xG / xA diff (US ↔ SS, разные модели) -----
        # Threshold ±0.5 — разные xG модели обычно отличаются <0.3 на shot,
        # суммарно по матчу редко >0.5.
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(xg_diff_us_ss) <= 0.5 OR xg_diff_us_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.xg_us_ss]'),
        CHECK.coverage('gold.fct_player_match_audit',
                       condition='ABS(xa_diff_us_ss) <= 0.5 OR xa_diff_us_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_match_audit.xa_us_ss]'),

        # ============================================================
        # issue #95 audit: fct_team_match_audit — cross-source diff на
        # team-match-grain между FBref (primary spine), Understat (INNER
        # secondary spine, per design doc §8.3), SofaScore (LEFT) и
        # WhoScored (LEFT). WS-блок ожидаемо NULL для current seasons до
        # резолва canonical_id в Silver (followup #120) — IS NULL ветка в
        # coverage condition прячет это от WARN-носа.
        # Thresholds: ±1 для integer counters, ±5 для possession, ±0.5 для xG.
        # ============================================================
        CHECK.no_duplicates('gold.fct_team_match_audit',
                            pk=['match_id', 'team_id']),
        CHECK.no_nulls('gold.fct_team_match_audit',
                       cols=['match_id', 'team_id']),
        # audit ⊆ main fct (INNER FBref ∩ US) → каждая audit-строка должна
        # находить парную (match_id, team_id) в gold.fct_team_match. #442: audit
        # PK renamed off *_canonical to match parent names; значения идентичны
        # (для source='fbref' canonical == raw).
        CHECK.ref_integrity('gold.fct_team_match_audit', 'gold.fct_team_match',
                            'match_id', parent_key='match_id'),

        # ----- Understat diff (INNER spine — всегда non-NULL) -----
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_for_diff_us) <= 1 OR goals_for_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_for_us]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_against_diff_us) <= 1 OR goals_against_diff_us IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_against_us]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(xg_diff_us_ss) <= 0.5 OR xg_diff_us_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.xg_us_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(xga_diff_us_ss) <= 0.5 OR xga_diff_us_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.xga_us_ss]'),

        # ----- SofaScore diff (LEFT — NULL допустим) -----
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_for_diff_ss) <= 1 OR goals_for_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_for_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_against_diff_ss) <= 1 OR goals_against_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_against_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_diff_ss) <= 2 OR shots_diff_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_on_target_diff_ss) <= 2 OR shots_on_target_diff_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_on_target_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(yellow_cards_diff_ss) <= 1 OR yellow_cards_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.yellow_cards_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(red_cards_diff_ss) <= 1 OR red_cards_diff_ss IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.red_cards_ss]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(possession_diff_ss) <= 5 OR possession_diff_ss IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.possession_ss]'),

        # ----- WhoScored diff (LEFT — NULL expected для current seasons; #120) -----
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_diff_ws) <= 2 OR shots_diff_ws IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_ws]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_on_target_diff_ws) <= 2 OR shots_on_target_diff_ws IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_on_target_ws]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(fouls_diff_ss_ws) <= 2 OR fouls_diff_ss_ws IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.fouls_ss_ws]'),

        # ----- FotMob diff (LEFT — NULL when absent; #97). WARNING-only -----
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_for_diff_fm) <= 1 OR goals_for_diff_fm IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_for_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(goals_against_diff_fm) <= 1 OR goals_against_diff_fm IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.goals_against_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_diff_fm) <= 2 OR shots_diff_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(shots_on_target_diff_fm) <= 2 OR shots_on_target_diff_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.shots_on_target_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(possession_diff_fm) <= 5 OR possession_diff_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.possession_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(yellow_cards_diff_fm) <= 1 OR yellow_cards_diff_fm IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.yellow_cards_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(red_cards_diff_fm) <= 1 OR red_cards_diff_fm IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.red_cards_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(xg_diff_us_fm) <= 0.5 OR xg_diff_us_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.xg_us_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(passes_diff_ss_fm) <= 30 OR passes_diff_ss_fm IS NULL',
                       warn_threshold=0.85, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.passes_ss_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(corners_diff_ss_fm) <= 2 OR corners_diff_ss_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.corners_ss_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(fouls_diff_ss_fm) <= 2 OR fouls_diff_ss_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.fouls_ss_fm]'),
        CHECK.coverage('gold.fct_team_match_audit',
                       condition='ABS(offsides_diff_ss_fm) <= 2 OR offsides_diff_ss_fm IS NULL',
                       warn_threshold=0.90, error_threshold=0.0,
                       name='audit_diff[fct_team_match_audit.offsides_ss_fm]'),

        # ============================================================
        # E1.5: post-cutover ref_integrity / canonical-format checks
        # (silver.xref_team is the source-of-truth; player_id MUST start
        # with 'fb_'). All severity=WARNING in this prep PR — operate as
        # observability during the ≥3-day green-parity gate-watch window.
        # See dags/utils/xref_dq.py::build_e1_5_post_cutover_checks for
        # the full list (4 checks). After cutover-merge a follow-up PR
        # may tighten the team-level check to ERROR severity.
        # ============================================================
        *build_e1_5_post_cutover_checks(),

        # ============================================================
        # #432: final star-schema gate — design-PK / league+season FK /
        # missing dim-FK with orphan-rate thresholds / grain sanity.
        # See the builder + sub-builders above for the full list and
        # per-threshold baselines.
        # ============================================================
        *build_star_gate_checks(),
    ]

    report = run_checks(checks, raise_on_error=False)

    # E2: two-tier coverage check on fct_standings.team_id resolver hit-rate.
    # Inline because the universal CHECK registry has no two-tier coverage
    # primitive yet (see helper docstring). WARNING-only — orphans are
    # intentionally retained with team_id_source='sofascore_orphan'.
    _append_fct_standings_coverage_check(report)

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


def validate_gold_row_counts() -> Dict[str, Any]:
    """Sanity check: Gold tables have expected row counts."""
    from utils.data_quality import CHECK, run_checks

    # Rough expectations for APL-only history (9 complete seasons + current):
    # - 3420-3800 matches in dim_match
    # - 6840-7600 rows in fct_team_match (long form: 2 per match)
    # - ~1000+ canonical players in dim_player (one row per player, #425)
    checks = [
        CHECK.row_count('gold.dim_match',        min_rows=3000),
        CHECK.row_count('gold.fct_team_match',   min_rows=6000),
        # #425: dim_team grain = one row per CLUB (~34 incl. relegated).
        CHECK.row_count('gold.dim_team',         min_rows=25),
        # #425: dim_player grain = one row per player (FBref-spine canonical
        # union across seasons stays >1000).
        CHECK.row_count('gold.dim_player',       min_rows=1000),
        # T4: cross-source attribute snapshot — one row per FBref-spine player
        # canonical_id (all seasons union). Floor ~1000 = dim_player baseline.
        CHECK.row_count('gold.dim_player_attributes', min_rows=1000),
        # T5: cross-source per-season stats. Outfield baseline ≈2551 rows
        # (5 сезонов APL × ~500 outfield); floor 400 с запасом на partition
        # gaps. Keeper baseline ≈204; floor 50.
        CHECK.row_count('gold.fct_player_season_stats', min_rows=400),
        CHECK.row_count('gold.fct_keeper_season_stats', min_rows=50),
        # issue #11: FotMob market_value timeline — ~500 игроков × несколько
        # точек, APL 2025/26 floor ≥1000.
        CHECK.row_count('gold.fct_player_market_value', min_rows=1000),
        # T5 audit: subset main fct (INNER JOIN на оба источника). FotMob
        # покрывает только 2025/26 → audit-row только для пересечения.
        # Outfield baseline ≈270 rows (2025/26 only); floor 100.
        # Keeper baseline ≈25; floor 10.
        CHECK.row_count('gold.fct_player_season_stats_audit', min_rows=100),
        CHECK.row_count('gold.fct_keeper_season_stats_audit', min_rows=10),
        # issue #46: multi-source это column-wise обогащение spine, не
        # row-wise разрастание. Floor 10000 с запасом под orphan-drops в
        # xref-bridge JOIN'ах (Understat/WhoScored LEFT JOIN допускают
        # NULL, но фильтр fb.match_id/fb.player_id IS NOT NULL сохраняет
        # FBref-spine). Baseline ≈14-15K на APL 5 сезонов.
        CHECK.row_count('gold.fct_player_match', min_rows=10000),
        # issue #427: unified chronicle — first materialisation baseline
        # 50081 rows (goals 10969 + cards 14021 + subs 25091 on APL 10
        # seasons). Floor 40K leaves headroom for partition gaps while
        # still catching a broken/partial build.
        CHECK.row_count('gold.fct_match_timeline', min_rows=40000),
        # issue #46 audit: INNER FBref ∩ SofaScore — pewer rows than main.
        # SofaScore cherry-pick покрывает APL 2024/25 + 2025/26 (~526 игроков
        # на сезон × ~38 матчей × ~22 в составе ≈ 22000 audit-rows). Floor 1000
        # с запасом на тестовые/частичные backfill'ы.
        CHECK.row_count('gold.fct_player_match_audit', min_rows=1000),
        # T6.4 (#94): cross-source team-season stats. APL spine ≈20 teams × 10
        # seasons (2016–2025) = 200 rows, floor 80 с запасом на partition gaps.
        # Audit INNER FBref ∩ Understat — после backfill (#213) Understat team-match
        # покрывает 6 сезонов (2020/21–2025/26), поэтому пересечение = 6 × 20 = 120
        # rows, floor 100 с запасом на orphan-промоутов / partition gaps.
        CHECK.row_count('gold.fct_team_season_stats',       min_rows=80),
        CHECK.row_count('gold.fct_team_season_stats_audit', min_rows=100),

        # ===== E2: master-data dim row-count floors =====
        # dim_venue: APL has ~20 active stadiums per season; 9+ seasons of
        # history with promotion/relegation churn comfortably exceeds 20 unique.
        CHECK.row_count('gold.dim_venue',     min_rows=20),
        # dim_referee: typically ~30+ active EPL match officials across history.
        CHECK.row_count('gold.dim_referee',   min_rows=30),
        # dim_manager (#425): one row per manager. APL history counts
        # ~30-50+ distinct head coaches; floor 20 still catches an empty table.
        CHECK.row_count('gold.dim_manager',   min_rows=20),
        # fct_manager_stint (#429): one row per (manager × team × stint) —
        # the SCD-2 history that moved out of dim_manager. APL 9+ seasons of
        # managerial churn ≈ 50-200 stints; floor 20 catches an empty/broken
        # build while tolerating partial bronze coverage.
        CHECK.row_count('gold.fct_manager_stint', min_rows=20),
        # fct_transfer (#429): pure projection of silver.transfermarkt_transfers
        # — baseline 750 rows (APL '2526', measured 2026-06-11); floor 500.
        CHECK.row_count('gold.fct_transfer',  min_rows=500),
        # fct_standings (#428, ex-dim_standings): at least one snapshot of the
        # current 18-team table (relaxed to 18 to cover early-season / partial
        # loads — historical snapshots will multiply this by season).
        CHECK.row_count('gold.fct_standings', min_rows=18),
        # dim_competition (#425): one row per competitions.yaml entry —
        # 8 today (1 in-scope + 7 stubs). Hard equality detects drift the
        # moment the YAML changes without a corresponding re-run.
        CHECK.row_count('gold.dim_competition', min_rows=8, max_rows=8),
        # dim_season (#425): union of seasons across in-scope competitions
        # in competitions.yaml — 10 APL seasons (1617..2526, full ingested
        # history). Same drift contract.
        CHECK.row_count('gold.dim_season',      min_rows=10, max_rows=10),
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
