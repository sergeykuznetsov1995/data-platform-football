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
import time
from typing import Any, Dict, List, Optional, Tuple

from utils.silver_tasks import (
    _execute,
    _get_trino_connection,
    _resolve_sql_path,
    _validate_identifier,
    check_bronze_table_exists,
    run_silver_transform,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-partition INSERT runner — sentinel for WHERE-clause injection
# ---------------------------------------------------------------------------
# SQL files consumed by ``run_gold_partition_inserts`` MUST contain exactly
# one occurrence of this marker on a line by itself (or as the only content
# of a comment line). At runtime the marker is replaced with a concrete
# ``WHERE league = '...' AND season = '...'`` clause built from the partition
# tuple. We deliberately use a comment marker (``--``) so the file is still
# valid SQL when opened in an editor or run manually with no replacement
# applied (the unreplaced template would fail the WHERE-required contract,
# which is the intended fail-loud behaviour).
PARTITION_FILTER_SENTINEL = "-- WHERE_PARTITION_FILTER_HERE"


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
    # E5: rolling avg # of confirmed unavailable players over L5. Same
    # skip_first_n=5 mask — point-in-time leakage risk is identical to
    # other rolling features here.
    'unavailable_count_l5',
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

# E6 / W2: feat_referee_bias — partition (referee_id, season) ORDER BY date,
# mask ref_match_rn > 5. Each column is a rolling stat over the referee's last
# 10 completed matches (within season). Same point-in-time semantics as the
# other feat_* tables: first 5 matches of a referee's season carry NULL to
# avoid leakage during early-season fitting.
FEAT_REFEREE_BIAS_ROLLING_COLS = [
    'ref_yellow_per_match_l10',
    'ref_red_per_match_l10',
    'ref_cards_per_match_l10',
    'ref_goals_per_match_l10',
    'ref_home_win_rate_l10',
    'ref_pen_per_match_l10',
]

# E6 / W3: feat_team_event_style — partition (team_id, season) ORDER BY date,
# mask match_rn > 5. All columns are share/rate metrics derived from
# silver.whoscored_events_spadl rolling over the team's last 5 matches.
# Bounded in [0, 1] (share = fraction of team's actions of given type, or
# success_rate = share of successful actions). NB: skip_first_n=5 — share
# numerators are unstable for the first ~5 matches when sample sizes are
# small, so the SQL masks them to NULL and the DQ enforces the mask.
FEAT_TEAM_EVENT_STYLE_ROLLING_COLS = [
    'pass_share_l5_avg',
    'dribble_share_l5_avg',
    'tackle_share_l5_avg',
    'interception_share_l5_avg',
    'cross_share_l5_avg',
    'shot_share_l5_avg',
    'success_rate_l5_avg',
    'set_piece_share_l5_avg',
    'open_play_share_l5_avg',
    'header_share_l5_avg',
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


# ---------------------------------------------------------------------------
# E3.6: per-partition INSERT runner (opt-in for backfills / multi-competition)
# ---------------------------------------------------------------------------
#
# DESIGN NOTES
# ------------
# * Default Gold path stays ``run_gold_transform`` (DROP + CTAS). It is
#   atomic for the *whole* table and faster for full rebuilds; ideal for
#   the current scope (APL/2526, single season).
# * ``run_gold_partition_inserts`` is opt-in for situations where DROP is
#   destructive: backfilling a *historical* season into a Gold table that
#   already serves the current season, or expanding to a new competition
#   alongside existing data. Trino R4 verdict: per-partition INSERT,
#   528K rows / 2.7s wall-clock.
# * Idempotency is achieved with DELETE-then-INSERT scoped to the partition
#   keys. Iceberg in our Trino catalog supports row-level DELETE on
#   partition columns — so reruns of the same (league, season) are safe.
# * Resumability: ``start_from_partition`` lets the caller skip partitions
#   that were already completed in a previous run. Semantics — exclusive:
#   "the named partition was *successfully* finished, resume *after* it".
# * Per-partition DQ (``per_partition_checks``) runs AFTER the INSERT.
#   On ERROR-severity failure the partition is rolled back (DELETE again).
#   ``raise_on_partition_failure`` controls whether the runner aborts the
#   whole sweep or logs and continues with the next partition.
# * SQL injection — ``partition_keys`` are validated as identifiers; the
#   *values* are escaped via single-quote doubling (Trino-standard).
#   Length cap on values so no monster strings ever land in the WHERE.


# Hard cap on partition-value string length. APL season codes ('2526') and
# league names ('ENG-Premier League') are well under this; the cap exists
# only as a defence-in-depth shield against accidental injection of huge
# literals via a misuse of the API.
_MAX_PARTITION_VALUE_LEN = 128


def _safe_partition_value(value: str, key: str) -> str:
    """Escape a partition VALUE for inline use in a Trino WHERE clause.

    Identifiers (table / column names) are validated by ``_validate_identifier``;
    values cannot use that path because they may legitimately contain spaces,
    hyphens or apostrophes (e.g. ``'ENG-Premier League'``, ``"O'Higgins"``).

    We:
      1. require ``str`` type and bounded length;
      2. reject characters that have no place in a literal we trust to inline
         — control chars, NUL, semicolons, comment markers — even if Trino
         would tolerate them;
      3. double single quotes to produce a valid Trino string literal.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"partition value for {key!r} must be str, got {type(value).__name__}"
        )
    if not value:
        raise ValueError(f"partition value for {key!r} must be non-empty")
    if len(value) > _MAX_PARTITION_VALUE_LEN:
        raise ValueError(
            f"partition value for {key!r} too long: "
            f"{len(value)} chars (max {_MAX_PARTITION_VALUE_LEN})"
        )
    # Block characters that should never appear in a partition literal.
    # NB: backslash is allowed (Trino does not interpret it specially in
    # standard string literals when ``escape`` is unset), but control
    # characters and SQL-comment / statement-terminator tokens are not.
    if any(ch in value for ch in ('\x00', '\n', '\r', '\t', ';')):
        raise ValueError(
            f"partition value for {key!r} contains forbidden chars: {value!r}"
        )
    if '--' in value or '/*' in value or '*/' in value:
        raise ValueError(
            f"partition value for {key!r} contains SQL comment marker: {value!r}"
        )
    return value.replace("'", "''")


def _build_partition_filter(
    partition_keys: Tuple[str, ...],
    partition_values: Tuple[str, ...],
) -> str:
    """Build a ``league = 'X' AND season = 'Y'`` predicate (no leading WHERE).

    The leading WHERE is the caller's job: that way the same builder serves
    both ``DELETE FROM t WHERE <pred>`` and ``... WHERE <pred>`` injection
    into a SELECT body, and the sentinel replacement preserves the comment
    line's leading whitespace.
    """
    if len(partition_keys) != len(partition_values):
        raise ValueError(
            f"partition_keys / values length mismatch: "
            f"{len(partition_keys)} vs {len(partition_values)}"
        )
    parts: List[str] = []
    for k, v in zip(partition_keys, partition_values):
        _validate_identifier(k, "partition column")
        parts.append(f"{k} = '{_safe_partition_value(v, k)}'")
    return " AND ".join(parts)


def _read_select_template(sql_file: str) -> str:
    """Read the SELECT SQL template and verify the partition-filter sentinel.

    The file MUST contain exactly one occurrence of
    ``PARTITION_FILTER_SENTINEL`` — that is where the runner injects the
    ``WHERE league=... AND season=...`` predicate. Zero occurrences is a
    contract violation (caller is using the wrong runner or forgot the
    marker); more than one is ambiguous and rejected for the same reason.
    """
    sql_path = _resolve_sql_path(sql_file)
    text = sql_path.read_text(encoding='utf-8').strip()
    if not text:
        raise ValueError(f"SQL file is empty: {sql_path}")
    if text.endswith(';'):
        text = text[:-1].rstrip()
    occurrences = text.count(PARTITION_FILTER_SENTINEL)
    if occurrences == 0:
        raise ValueError(
            f"SQL file {sql_path} must contain the sentinel "
            f"{PARTITION_FILTER_SENTINEL!r} where the WHERE-clause should be "
            f"injected. Place it on a line by itself, e.g.:\n"
            f"  -- ... your SELECT body ...\n"
            f"  {PARTITION_FILTER_SENTINEL}\n"
        )
    if occurrences > 1:
        raise ValueError(
            f"SQL file {sql_path} contains {occurrences} occurrences of "
            f"{PARTITION_FILTER_SENTINEL!r}; expected exactly one."
        )
    return text


def _read_ddl(ddl_file: str) -> str:
    """Read the DDL file (CREATE TABLE IF NOT EXISTS ...) verbatim.

    The DDL is run once at the top of the sweep. It MUST be idempotent
    (``IF NOT EXISTS``) — if the table is already there the statement is
    a Trino no-op. Trailing semicolons stripped to keep ``_execute()``
    happy (the trino-python client expects a single statement per call).
    """
    sql_path = _resolve_sql_path(ddl_file)
    text = sql_path.read_text(encoding='utf-8').strip()
    if not text:
        raise ValueError(f"DDL file is empty: {sql_path}")
    if text.endswith(';'):
        text = text[:-1].rstrip()
    upper = text.upper()
    if 'CREATE TABLE' not in upper or 'IF NOT EXISTS' not in upper:
        raise ValueError(
            f"DDL file {sql_path} must contain 'CREATE TABLE ... IF NOT EXISTS' "
            f"to remain idempotent across reruns."
        )
    return text


def run_gold_partition_inserts(
    *,
    sql_file: str,
    table_name: str,
    ddl_file: str,
    partitions: List[Tuple[str, str]],
    partition_keys: Tuple[str, ...] = ('league', 'season'),
    skip_existing: bool = False,
    per_partition_checks: Optional[List[Any]] = None,
    raise_on_partition_failure: bool = True,
    start_from_partition: Optional[Tuple[str, str]] = None,
    catalog: str = 'iceberg',
    schema: str = 'gold',
) -> Dict[str, Any]:
    """Idempotent per-partition INSERT runner for backfills and multi-competition.

    This is the **opt-in** Gold runner used by E3.5 (3 historical APL seasons
    backfill) and E8 (multi-competition expansion). For the default E3 path
    (``APL/2526`` only), use ``run_gold_transform`` — DROP+CTAS is faster and
    atomic at the table level.

    Flow per partition (key1, key2[, ...]):

      1. **Idempotency** — ``DELETE FROM gold.{table} WHERE <partition_filter>``
         removes any prior data for these partition keys. Iceberg supports
         row-level deletes on partition columns; the operation is metadata-only
         when the predicate aligns with the partitioning spec.
      2. **INSERT** — ``INSERT INTO gold.{table} SELECT ... WHERE <partition_filter>``
         The SELECT body is taken from ``sql_file`` with the
         ``PARTITION_FILTER_SENTINEL`` comment replaced by the concrete
         WHERE-predicate.
      3. **Per-partition DQ** — if ``per_partition_checks`` is provided, the
         checks are run against the freshly inserted partition. Any
         ERROR-severity failure triggers a rollback (re-DELETE) of the
         partition; the sweep then either aborts (default) or skips to the
         next partition (``raise_on_partition_failure=False``).

    The DDL file is executed **once** at the top of the sweep with
    ``IF NOT EXISTS`` semantics — establishes the empty Iceberg table with
    correct schema + partitioning if not already present, otherwise no-op.

    SQL template contract
    ---------------------
    The SELECT-side ``sql_file`` must contain exactly one occurrence of the
    sentinel comment ``-- WHERE_PARTITION_FILTER_HERE`` (see
    ``PARTITION_FILTER_SENTINEL``) on a line by itself. At execute time the
    runner replaces it with the concrete predicate, e.g.::

        SELECT ... FROM iceberg.silver.fct_event
        -- WHERE_PARTITION_FILTER_HERE        <-- becomes:
        WHERE league = 'ENG-Premier League' AND season = '2324'

    Parameters
    ----------
    sql_file : str
        Path to the SELECT-only SQL file (containing the sentinel).
    table_name : str
        Target Gold table name (e.g. ``'fct_event'``).
    ddl_file : str
        Path to the ``CREATE TABLE IF NOT EXISTS`` statement.
    partitions : List[Tuple[str, str]]
        Ordered list of (key1, key2, ...) tuples to process. Tuple arity
        must match ``partition_keys``.
    partition_keys : Tuple[str, ...]
        Identifier names of the partitioning columns (default
        ``('league', 'season')``).
    skip_existing : bool, default False
        If True, skip partitions that already have rows in the target.
        Used for resume-without-overwrite semantics.
    per_partition_checks : Optional[List[Check]]
        Universal-DQ checks to run after each partition's INSERT. ERRORs
        trigger rollback of *that* partition.
    raise_on_partition_failure : bool, default True
        If True, the first failed partition aborts the sweep and re-raises.
        If False, the runner logs the failure, records it in the result and
        continues with the next partition (best-effort backfill mode).
    start_from_partition : Optional[Tuple[str, str]]
        If set, all partitions up to and INCLUDING this tuple are skipped.
        Resume semantics: "this partition was already completed, start
        AFTER it". Useful for re-running a backfill that died midway.
    catalog : str, default 'iceberg'
    schema : str, default 'gold'

    Returns
    -------
    Dict[str, Any]
        ``{
            'partitions_processed': int,
            'partitions_skipped': List[Tuple],
            'partitions_failed': List[Tuple],
            'total_rows_inserted': int,
            'duration_per_partition': List[float],
            'status': 'success' | 'partial' | 'failed',
        }``

    Raises
    ------
    AirflowException / RuntimeError
        On per-partition DQ ERROR if ``raise_on_partition_failure=True``,
        or on Trino-level execution failures (which are always fatal —
        they indicate connectivity / permission problems, not data issues).
    """
    # ---- Argument validation up front ------------------------------------
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")
    if not partition_keys:
        raise ValueError("partition_keys must be a non-empty tuple")
    for pk in partition_keys:
        _validate_identifier(pk, "partition column")
    if not partitions:
        raise ValueError("partitions list is empty — nothing to do")
    arity = len(partition_keys)
    for p in partitions:
        if not isinstance(p, tuple) or len(p) != arity:
            raise ValueError(
                f"each partition must be a tuple of length {arity} "
                f"(matching partition_keys), got {p!r}"
            )

    full_table = f"{catalog}.{schema}.{table_name}"

    # Read templates BEFORE opening the connection — fail fast on bad files.
    select_template = _read_select_template(sql_file)
    ddl_sql = _read_ddl(ddl_file)

    # ---- Resume logic ----------------------------------------------------
    # Start AFTER the named partition (exclusive). If it isn't in the list
    # we treat that as a hard error — the caller is asking to resume from
    # a partition that wasn't queued, almost certainly a bug.
    work: List[Tuple[str, ...]] = list(partitions)
    if start_from_partition is not None:
        try:
            idx = work.index(tuple(start_from_partition))
        except ValueError:
            raise ValueError(
                f"start_from_partition={start_from_partition!r} not present in "
                f"partitions list (cannot resume after an unknown partition)"
            )
        skipped_resume = work[: idx + 1]
        work = work[idx + 1:]
        logger.info(
            "Resume mode: skipping %d partition(s) up to and including %r",
            len(skipped_resume), start_from_partition,
        )

    # ---- Result accumulator ---------------------------------------------
    result: Dict[str, Any] = {
        'partitions_processed': 0,
        'partitions_skipped': [],
        'partitions_failed': [],
        'total_rows_inserted': 0,
        'duration_per_partition': [],
        'status': 'pending',
    }

    conn = _get_trino_connection(catalog=catalog)
    try:
        # ---- 1. Ensure schema + table exist (idempotent) ----------------
        _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        logger.info("Schema ensured: %s.%s", catalog, schema)
        logger.info("Running DDL (idempotent IF NOT EXISTS): %s", ddl_file)
        _execute(conn, ddl_sql)

        # ---- 2. Iterate partitions --------------------------------------
        for partition in work:
            t0 = time.monotonic()
            partition_filter = _build_partition_filter(partition_keys, partition)
            partition_label = ", ".join(
                f"{k}={v!r}" for k, v in zip(partition_keys, partition)
            )
            logger.info("=== Partition %s ===", partition_label)

            try:
                # 2a. skip_existing — bail out if data already there.
                if skip_existing:
                    cnt_sql = (
                        f"SELECT COUNT(*) FROM {full_table} WHERE {partition_filter}"
                    )
                    rows = _execute(conn, cnt_sql, fetch=True)
                    existing = rows[0][0] if rows else 0
                    if existing > 0:
                        logger.info(
                            "skip_existing=True and partition already has "
                            "%d rows — skipping", existing,
                        )
                        result['partitions_skipped'].append(partition)
                        result['duration_per_partition'].append(
                            time.monotonic() - t0
                        )
                        continue

                # 2b. Idempotency DELETE — wipe any prior rows for this key.
                delete_sql = f"DELETE FROM {full_table} WHERE {partition_filter}"
                logger.info("DELETE (idempotency): %s", delete_sql)
                _execute(conn, delete_sql)

                # 2c. INSERT INTO ... SELECT (with sentinel replaced).
                where_clause = f"WHERE {partition_filter}"
                select_sql = select_template.replace(
                    PARTITION_FILTER_SENTINEL, where_clause
                )
                insert_sql = f"INSERT INTO {full_table}\n{select_sql}"
                logger.info("INSERT into %s for %s", full_table, partition_label)
                _execute(conn, insert_sql)

                # 2d. Row-count after insert (observability + return value).
                rows = _execute(
                    conn,
                    f"SELECT COUNT(*) FROM {full_table} WHERE {partition_filter}",
                    fetch=True,
                )
                inserted = rows[0][0] if rows else 0
                logger.info(
                    "Partition %s inserted: %d row(s)", partition_label, inserted,
                )

                # 2e. Per-partition DQ (if requested).
                if per_partition_checks:
                    dq_failed = _run_partition_dq(
                        per_partition_checks, partition_label,
                    )
                    if dq_failed:
                        # Rollback this partition — re-DELETE so the table
                        # never carries data that failed validation.
                        logger.error(
                            "Per-partition DQ failed for %s — rolling back",
                            partition_label,
                        )
                        _execute(conn, delete_sql)
                        raise RuntimeError(
                            f"Per-partition DQ ERROR for {partition_label}: "
                            + "; ".join(dq_failed)
                        )

                result['partitions_processed'] += 1
                result['total_rows_inserted'] += int(inserted)
                result['duration_per_partition'].append(time.monotonic() - t0)

            except Exception as e:
                duration = time.monotonic() - t0
                result['duration_per_partition'].append(duration)
                result['partitions_failed'].append(partition)
                logger.error(
                    "Partition %s FAILED after %.2fs: %s",
                    partition_label, duration, e,
                )
                if raise_on_partition_failure:
                    result['status'] = 'failed'
                    raise
                # Best-effort: continue with the next partition.
                continue

        # All partitions handled.
        if result['partitions_failed']:
            result['status'] = 'partial'
        else:
            result['status'] = 'success'

    finally:
        conn.close()

    logger.info(
        "Per-partition INSERT done: %d processed, %d skipped, %d failed, "
        "%d total rows.",
        result['partitions_processed'],
        len(result['partitions_skipped']),
        len(result['partitions_failed']),
        result['total_rows_inserted'],
    )
    return result


# ---------------------------------------------------------------------------
# E3.5: wrapper-style per-partition INSERT (no sentinel required)
# ---------------------------------------------------------------------------
#
# DESIGN
# ------
# ``run_gold_partition_inserts`` (above) requires the SQL file to contain
# the ``-- WHERE_PARTITION_FILTER_HERE`` sentinel — letting Trino push the
# predicate INSIDE the SELECT body. That gives the best partition pruning
# but requires every Gold SQL to be aware of it.
#
# ``run_gold_partition_insert_wrapped`` (this function) is the unblocked
# alternative: it wraps the *original* SELECT verbatim as
# ``SELECT * FROM (<orig>) AS __src WHERE league=... AND season=...`` and
# inserts that. ZERO modification of the SQL files needed. Trino's
# optimiser usually pushes the partition predicate down past the outer
# wrapper anyway (verified for fct_event/fct_shot/fct_lineup); the
# performance delta vs the sentinel path is small for E3 backfill scale.
#
# Both runners produce identical idempotency semantics (DELETE-then-INSERT).
# Use the sentinel path for huge-scale repeated backfills (E8 multi-comp);
# use the wrapper path for E3.5 (3 historical APL seasons).


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

    Sister of ``run_gold_partition_inserts`` that does NOT require the
    SQL file to carry the ``-- WHERE_PARTITION_FILTER_HERE`` sentinel.
    Wraps the original SELECT verbatim as
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


def _run_partition_dq(
    checks: List[Any],
    partition_label: str,
) -> List[str]:
    """Run the supplied DQ checks; return a list of ERROR-severity failure
    descriptions (empty list = all good).

    Each check is expected to be a ``utils.data_quality.Check`` instance.
    The function intentionally swallows the ``run_checks`` raise (we pass
    ``raise_on_error=False``) so that the caller can perform a
    rollback DELETE before re-raising — losing the chance to clean up
    would defeat the per-partition atomicity guarantee.
    """
    from utils.data_quality import run_checks

    report = run_checks(checks, raise_on_error=False)
    logger.info(
        "Per-partition DQ for %s: %s", partition_label, report.summary(),
    )
    if not report.errors:
        return []
    return [f"{r.name}: {r.details or r.error}" for r in report.errors]


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
    from utils.xref_dq import build_e1_5_post_cutover_checks

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
        # E5: composite PK guards against double-listing the same player as
        # absent for the same match. Empty fallback (0 rows) passes trivially.
        CHECK.no_duplicates(
            'gold.fct_player_unavailable',
            pk=['match_id', 'team_id', 'player_id_canonical'],
        ),
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
        # E5: required keys. NB: `team_id` is intentionally NOT here — cross-
        # source slug mismatches (Wolves/Wolverhampton) leave it NULL by design;
        # coverage is observed via the WARNING-severity row_count check below.
        CHECK.no_nulls('gold.fct_player_unavailable',
                       cols=['match_id', 'match_date', 'player_id_canonical']),

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
        # E5: every unavailability row must point at a real Gold match.
        # SQL already filters bridge failures, so 0 orphans by construction.
        CHECK.ref_integrity('gold.fct_player_unavailable', 'gold.dim_match', 'match_id'),

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

        # ========== E5: fct_player_unavailable observability — WARNING ==========
        # Bronze whoscored_missing_players has been collected since 2021 (APL);
        # upper bound 2030 leaves several seasons of headroom.
        CHECK.value_range('gold.fct_player_unavailable', 'season',
                          min_val=2021, max_val=2030, severity='WARNING'),

        # Cross-source team_id coverage. WhoScored team_name -> team_slug is
        # best-effort (e.g. "Wolverhampton" vs "Wolves" leaves team_id NULL).
        # 200 ≈ ~10% of a typical season — surfaces the issue without paging
        # during a known-broken alias state. The CHECK registry has no
        # two-tier coverage primitive yet, so we use a hard row count.
        # Tighten once _team_aliases work absorbs the residual mismatches.
        CHECK.row_count(
            'gold.fct_player_unavailable',
            min_rows=0, max_rows=200,
            where='team_id IS NULL',
            severity='WARNING',
            name='coverage[fct_player_unavailable.team_id non-NULL]',
        ),

        # Cross-source player_id resolution coverage. Players that didn't
        # match dim_player get a synthetic 'ws_<id>' fallback (D4); counting
        # those tells us how much of the WhoScored player namespace is still
        # un-bridged. Generous threshold until E1 xref_player ships a proper
        # crosswalk.
        CHECK.row_count(
            'gold.fct_player_unavailable',
            min_rows=0, max_rows=1500,
            where="player_id_canonical LIKE 'ws_%'",
            severity='WARNING',
            name='coverage[fct_player_unavailable.player_id_canonical resolved]',
        ),

        # ============================================================
        # E6 / W2: feat_referee_bias — referee rolling stats over L10 matches
        # (within season). PK (referee_id, match_id), partition (season).
        # Mirrors the feat_team_form block: PK uniqueness + no_nulls on keys
        # + ref_integrity to dim_match + point-in-time leakage guard for every
        # registered rolling column + WARNING-severity domain bounds.
        # ============================================================

        # ----- E6: row-count soft floor — WARNING -----
        # 5 seasons × ~30 matches/season × ~25 ref-season rows ≈ 100 floor.
        # WARNING (not ERROR) — historical seasons may not all be backfilled
        # in MVP environments.
        CHECK.row_count('gold.feat_referee_bias', min_rows=100,
                        severity='WARNING'),

        # ----- E6: PK uniqueness — ERROR -----
        CHECK.no_duplicates('gold.feat_referee_bias',
                            pk=['referee_id', 'match_id']),

        # ----- E6: NOT NULL on PKs + temporal/partition keys — ERROR -----
        CHECK.no_nulls('gold.feat_referee_bias',
                       cols=['referee_id', 'match_id', 'date', 'season']),

        # ----- E6: ref_integrity feat_referee_bias.match_id → dim_match — ERROR -----
        CHECK.ref_integrity('gold.feat_referee_bias',
                            'gold.dim_match', 'match_id'),

        # ----- E6: point-in-time leakage guard — ERROR -----
        # SQL masks rolling cols at ref_match_rn > 5 within (referee_id, season).
        # Same skip_first_n=5 contract as the other feat_* tables: leakage
        # triggers DQ failure before Gold ships features to the model.
        *(
            CHECK.point_in_time(
                'gold.feat_referee_bias',
                feature_col=col,
                partition_by=['referee_id', 'season'],
                order_by='date',
                skip_first_n=5,
            )
            for col in FEAT_REFEREE_BIAS_ROLLING_COLS
        ),

        # ----- E6: value-range sanity (WARNING) -----
        # Domain heuristics for APL referees (single-match upper bounds across
        # rolling L10 averages). Tight enough to catch Bronze parser
        # regressions but loose enough not to flag legitimate outliers.
        CHECK.value_range('gold.feat_referee_bias', 'ref_yellow_per_match_l10',
                          min_val=0, max_val=6, severity='WARNING'),
        CHECK.value_range('gold.feat_referee_bias', 'ref_red_per_match_l10',
                          min_val=0, max_val=1, severity='WARNING'),
        CHECK.value_range('gold.feat_referee_bias', 'ref_cards_per_match_l10',
                          min_val=0, max_val=8, severity='WARNING'),
        CHECK.value_range('gold.feat_referee_bias', 'ref_home_win_rate_l10',
                          min_val=0, max_val=1, severity='WARNING'),

        # ============================================================
        # E6 / W3: feat_team_event_style — share/rate metrics from
        # whoscored_events_spadl, rolling L5 within (team_id, season).
        # PK (match_id, team_id), partition (league, season). Mirrors
        # feat_team_form block; all columns are bounded in [0, 1].
        # ============================================================

        # ----- E6: row-count soft floor — WARNING -----
        # ~380 APL matches × 2 teams/match × 5 seasons / sparse coverage ≈
        # 1000 floor for the rolling (post-mask) population.
        CHECK.row_count('gold.feat_team_event_style', min_rows=1000,
                        severity='WARNING'),

        # ----- E6: PK uniqueness — ERROR -----
        CHECK.no_duplicates('gold.feat_team_event_style',
                            pk=['match_id', 'team_id']),

        # ----- E6: NOT NULL on PKs + temporal/partition keys — ERROR -----
        CHECK.no_nulls('gold.feat_team_event_style',
                       cols=['match_id', 'team_id', 'date', 'season']),

        # ----- E6: ref_integrity feat_team_event_style.match_id → dim_match — ERROR -----
        CHECK.ref_integrity('gold.feat_team_event_style',
                            'gold.dim_match', 'match_id'),

        # ----- E6: point-in-time leakage guard — ERROR -----
        # SQL masks rolling cols at match_rn > 5 within (team_id, season).
        *(
            CHECK.point_in_time(
                'gold.feat_team_event_style',
                feature_col=col,
                partition_by=['team_id', 'season'],
                order_by='date',
                skip_first_n=5,
            )
            for col in FEAT_TEAM_EVENT_STYLE_ROLLING_COLS
        ),

        # ----- E6: value-range sanity (WARNING) -----
        # All columns are shares (fraction of team's actions of given type)
        # or success_rate (share of successful actions). Hard bound [0, 1]
        # — anything outside indicates Silver SPADL parser regression.
        *(
            CHECK.value_range(
                'gold.feat_team_event_style',
                column=col,
                min_val=0, max_val=1,
                severity='WARNING',
            )
            for col in FEAT_TEAM_EVENT_STYLE_ROLLING_COLS
        ),

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

        # ============================================================
        # E7 / T7: BI dashboard marts — DQ guards complementing the
        # row_count floors registered in validate_gold_row_counts().
        # PK uniqueness ERROR + value-range / freshness WARNINGs +
        # point-in-time leakage guard for mart_scouting_radar.xg_l5.
        # ============================================================

        # ----- E7: mart_scouting_radar — PK uniqueness — ERROR -----
        # SQL declares (player_id, season, league, match_id) as PK in the
        # header, but (player_id, match_id) is the unique tuple — season +
        # league are passthrough partition keys (one row per appearance).
        CHECK.no_duplicates('gold.mart_scouting_radar',
                            pk=['player_id', 'match_id']),

        # ----- E7: mart_scouting_radar — freshness — WARNING -----
        # `match_date` is a DATE (not a load timestamp): used here as the
        # most recent fixture played. Threshold = 2 days (48h) accommodates
        # international breaks while still surfacing parser regressions.
        CHECK.freshness('gold.mart_scouting_radar', ts_col='match_date',
                        max_age_hours=48, severity='WARNING'),

        # ----- E7: mart_scouting_radar — value-range — WARNING -----
        # Single-match xG headroom: penalties are 0.79; legitimate top
        # outliers (e.g. multiple big chances) rarely exceed 2.5; cap at 5
        # leaves headroom for unusual matches without flagging real data.
        CHECK.value_range('gold.mart_scouting_radar', 'xg',
                          min_val=0, max_val=5, severity='WARNING'),
        CHECK.value_range('gold.mart_scouting_radar', 'xa',
                          min_val=0, max_val=5, severity='WARNING'),

        # ----- E7: mart_scouting_radar — point-in-time — ERROR -----
        # SQL masks xg_l5 / xa_l5 / shots_l5 / defensive_l5 at
        # appearance_rn > 5 within (player_id, season). Ordered by
        # (match_date, match_id). Leakage triggers DAG failure before
        # Gold ships features to BI.
        CHECK.point_in_time(
            'gold.mart_scouting_radar',
            feature_col='xg_l5',
            partition_by=['player_id', 'season'],
            order_by='match_date',
            skip_first_n=5,
        ),

        # ----- E7: mart_referee_dashboard — PK uniqueness — ERROR -----
        CHECK.no_duplicates('gold.mart_referee_dashboard',
                            pk=['referee_id', 'season', 'league']),

        # ----- E7: mart_referee_dashboard — value-range -----
        # cards_per_match: typical EPL window is 2-5; cap at 15 leaves
        # headroom for derby outliers. WARNING — a soft heuristic.
        CHECK.value_range('gold.mart_referee_dashboard', 'cards_per_match',
                          min_val=0, max_val=15, severity='WARNING'),
        # home_win_pct is a fraction in [0, 1]. Boundary check is ERROR:
        # values outside the unit interval indicate an aggregation bug.
        CHECK.value_range('gold.mart_referee_dashboard', 'home_win_pct',
                          min_val=0, max_val=1, severity='ERROR'),
        # matches_officiated >= 1 — a referee-season row only exists if
        # the ref appeared at least once. Zero or negative would mean an
        # empty GROUP that survived the LEFT JOIN — definitely a bug.
        CHECK.value_range('gold.mart_referee_dashboard', 'matches_officiated',
                          min_val=1, severity='ERROR'),

        # ----- E7: mart_event_heatmap — PK uniqueness — ERROR -----
        CHECK.no_duplicates(
            'gold.mart_event_heatmap',
            pk=['team_id', 'season', 'league', 'zone_x', 'zone_y',
                'action_canonical'],
        ),

        # ----- E7: mart_event_heatmap — bin safety — ERROR -----
        # zone_x / zone_y are SQL-clamped to [0, 11] / [0, 7] via
        # LEAST/GREATEST. Anything outside means the binning formula
        # regressed (e.g. divisor change, missing GREATEST). Hard ERROR.
        CHECK.value_range('gold.mart_event_heatmap', 'zone_x',
                          min_val=0, max_val=11, severity='ERROR'),
        CHECK.value_range('gold.mart_event_heatmap', 'zone_y',
                          min_val=0, max_val=7,  severity='ERROR'),
        # success_rate is AVG of a boolean cast to double — strictly in
        # [0, 1]. Boundary breach = aggregation regression.
        CHECK.value_range('gold.mart_event_heatmap', 'success_rate',
                          min_val=0, max_val=1, severity='ERROR'),
        # event_count >= 1 — the GROUP BY guarantees a row only when at
        # least one event landed in the bin. Zero indicates a bug.
        CHECK.value_range('gold.mart_event_heatmap', 'event_count',
                          min_val=1, severity='ERROR'),

        # ============================================================
        # E1.5: post-cutover ref_integrity / canonical-format checks
        # (silver.xref_team is the source-of-truth; player_id MUST start
        # with 'fb_'). All severity=WARNING in this prep PR — operate as
        # observability during the ≥3-day green-parity gate-watch window.
        # See dags/utils/xref_dq.py::build_e1_5_post_cutover_checks for
        # the full list (6 checks). After cutover-merge a follow-up PR
        # may tighten the team-level check to ERROR severity.
        # ============================================================
        *build_e1_5_post_cutover_checks(),
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


def validate_predictions_input_v2() -> Dict[str, Any]:
    """E6 / T4.2-v2 — DQ check on the dual-run v2 predictions snapshot.

    Mirrors :func:`validate_predictions_input` but adds a schema-parity check
    against ``fct_match_train`` / ``fct_match_test`` so the inference snapshot
    cannot drift away from the feature contract used at training time. Drift
    in column set or types is a silent killer for the model — a NEW column
    introduced in train but missing from predictions_input_v2 means the
    serve-side pipeline imputes ``NULL`` and quietly degrades predictions.

    The set of columns that legitimately differ between the three tables is
    enumerated explicitly:

    * ``_silver_created_at`` — every Gold table stamps its own load time;
    * targets present only in train/test snapshots (``home_score``,
      ``away_score``, ``result_1x2``, ``total_goals``, ``btts``,
      ``is_completed``, plus the derived classification labels); these are
      not knowable for an upcoming fixture so predictions_input_v2 omits
      them by design.

    The ``CHECK.schema_parity`` primitive itself is added in W5b. If this
    function is invoked before W5b lands, ``run_checks`` will surface a
    clear ImportError / AttributeError — picked up by W8 unit tests + W9
    verification, fail-loud-and-early as designed.
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    checks = [
        # ===== ERROR: PK + critical keys =====
        CHECK.no_duplicates('gold.predictions_input_v2', pk=['match_id']),
        CHECK.no_nulls(
            'gold.predictions_input_v2',
            cols=['match_id', 'date', 'season',
                  'home_team_id', 'away_team_id'],
        ),

        # ===== WARNING: row count =====
        # Empty week is plausible (international break, off-season). min_rows=0
        # so a fixture-less window passes; cutoff stays in the WARNING freshness
        # check instead of paging on-call.
        CHECK.row_count('gold.predictions_input_v2', min_rows=0,
                        severity='WARNING'),

        # ===== ERROR: schema parity vs train/test =====
        # train/test/predictions_input_v2 must share the same feature surface
        # (modulo targets, which serve-time data cannot have). Drift here =
        # silent prediction degradation.
        CHECK.schema_parity(
            tables=['gold.fct_match_train',
                    'gold.fct_match_test',
                    'gold.predictions_input_v2'],
            ignore_cols=[
                # Per-table load timestamp — not part of the feature contract.
                '_silver_created_at',
                # Match-outcome columns — known only after the match is played,
                # so predictions_input_v2 (upcoming fixtures) has no values.
                'home_score', 'away_score', 'result_1x2', 'total_goals',
                'btts', 'is_completed',
                # Derived classification targets present only in train/test.
                'over_2_5', 'over_3_5', 'home_win', 'draw', 'away_win',
            ],
            severity='ERROR',
        ),
    ]

    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Predictions input v2 DQ: {report.summary()}")
    telegram_dq_summary(report, header="Predictions v2 DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Predictions input v2 DQ failed: {len(report.errors)} error(s). "
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

        # ===== E7: dashboard mart row-count floors =====
        # mart_scouting_radar: ≥800 player-season radars (FBref-only APL,
        # ~1000-2000/season after MIN_MINUTES filter; floor stays generous).
        CHECK.row_count('gold.mart_scouting_radar',    min_rows=800),
        # mart_referee_dashboard: ≥40 referee-season rows (~30 active EPL
        # refs across 9+ seasons of history easily clears 40).
        CHECK.row_count('gold.mart_referee_dashboard', min_rows=40),
        # mart_event_heatmap: ≥80 (zone, action_canonical, league, season)
        # buckets. WhoScored events cover only the recent seasons; floor
        # tuned to a single season's worth of populated cells.
        CHECK.row_count('gold.mart_event_heatmap',     min_rows=80),
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
