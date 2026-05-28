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
        # issue #46: fct_player_match теперь multi-source, PK переименована
        # match_id → match_id_canonical, player_id → player_id_canonical.
        CHECK.no_duplicates('gold.fct_player_match', pk=['match_id_canonical', 'player_id_canonical']),
        CHECK.no_duplicates('gold.match_outcomes',    pk=['match_id']),
        # E5: composite PK guards against double-listing the same player as
        # absent for the same match. Empty fallback (0 rows) passes trivially.
        CHECK.no_duplicates(
            'gold.fct_player_unavailable',
            pk=['match_id', 'team_id', 'player_id_canonical'],
        ),

        # ========== No NULLs in PKs — ERROR ==========
        CHECK.no_nulls('gold.dim_match',       cols=['match_id', 'date']),
        CHECK.no_nulls('gold.fct_team_match',  cols=['match_id', 'team_id', 'opponent_id']),
        # match_outcomes is the source-of-truth for ML labels — PK + temporal
        # keys MUST be present, otherwise downstream backtests silently misalign.
        CHECK.no_nulls('gold.match_outcomes',  cols=['match_id', 'season', 'match_date']),
        # E5: required keys. NB: `team_id` is intentionally NOT here — cross-
        # source slug mismatches (Wolves/Wolverhampton) leave it NULL by design;
        # coverage is observed via the WARNING-severity row_count check below.
        CHECK.no_nulls('gold.fct_player_unavailable',
                       cols=['match_id', 'match_date', 'player_id_canonical']),

        # ========== Referential integrity — ERROR ==========
        CHECK.ref_integrity('gold.fct_team_match',   'gold.dim_match', 'match_id'),
        # issue #46: ref_integrity на dim_match — child = match_id_canonical, parent = match_id.
        CHECK.ref_integrity(
            'gold.fct_player_match', 'gold.dim_match',
            'match_id_canonical', parent_key='match_id',
        ),
        # issue #46: fct_player_match теперь multi-source с player_id_canonical
        # — добавляем ref_integrity к dim_player_attributes (snapshot grain per
        # canonical_id, T4). Без `parent_key=` так как обе таблицы используют
        # одну и ту же колонку player_id_canonical.
        CHECK.ref_integrity(
            'gold.fct_player_match',
            'gold.dim_player_attributes',
            'player_id_canonical',
            parent_key='player_id_canonical',
        ),
        CHECK.ref_integrity('gold.match_outcomes',    'gold.dim_match', 'match_id'),
        # E5: every unavailability row must point at a real Gold match.
        # SQL already filters bridge failures, so 0 orphans by construction.
        CHECK.ref_integrity('gold.fct_player_unavailable', 'gold.dim_match', 'match_id'),

        # ========== Value ranges — WARNING ==========
        CHECK.value_range('gold.fct_team_match', 'goals_for',  min_val=0, max_val=20,
                          severity='WARNING'),
        CHECK.value_range('gold.fct_team_match', 'possession', min_val=0, max_val=100,
                          severity='WARNING'),

        # ----- issue #46: fct_player_match multi-source xG/xA/rating sanity -----
        # xG/xA per single match: top observed values редко превышают 3.0 даже
        # для хет-триков; верхний bound 5.0 ловит явные парсер-выбросы (xG=999)
        # без false-positive'ов. Не value_range на goals/assists (могут быть
        # 4+ в редких матчах) и minutes (FBref гарантирует [0, 120]).
        CHECK.value_range('gold.fct_player_match', 'expected_goals',
                          min_val=0, max_val=5, severity='WARNING'),
        CHECK.value_range('gold.fct_player_match', 'expected_assists',
                          min_val=0, max_val=5, severity='WARNING'),
        # rating: SofaScore-источник, шкала [0, 10]; NULL для матчей без оценки.
        CHECK.value_range('gold.fct_player_match', 'rating',
                          min_val=0, max_val=10, severity='WARNING'),
        # Targets sanity — only meaningful for completed matches; outliers
        # outside [0, 20] indicate parser regression in Silver score extraction.
        CHECK.value_range('gold.match_outcomes', 'total_goals', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),
        CHECK.value_range('gold.match_outcomes', 'home_score', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),
        CHECK.value_range('gold.match_outcomes', 'away_score', min_val=0, max_val=20,
                          where='is_completed = true', severity='WARNING'),

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

        # ----- E2: ref_integrity dim_standings.team_id → dim_team — WARNING -----
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
        # E2 Phase 1.5: dim_manager — SCD-2 head-coach dimension
        # (one row per manager × team × stint). Source =
        # silver.xref_manager × silver.xref_team × bronze.fbref_match_managers.
        # ============================================================

        # ----- dim_manager: PK uniqueness — ERROR -----
        # Composite PK: (manager_id_canonical, team_id_canonical, valid_from).
        # The triple distinguishes returning managers (Mourinho-Chelsea-2004
        # vs Mourinho-Chelsea-2013) which share the first two components.
        CHECK.no_duplicates(
            'gold.dim_manager',
            pk=['manager_id_canonical', 'team_id_canonical', 'valid_from'],
        ),

        # ----- dim_manager: NOT NULL on PKs + display_name — ERROR -----
        CHECK.no_nulls(
            'gold.dim_manager',
            cols=['manager_id_canonical', 'team_id_canonical',
                  'valid_from', 'display_name'],
        ),

        # ----- dim_manager: SCD-2 timeline integrity — ERROR -----
        # Closed-open intervals [valid_from, valid_to). For a single team
        # at any given date there can be at most ONE active manager.
        # Adjacent stints sharing an endpoint are OK.
        CHECK.scd2_no_overlap(
            'gold.dim_manager',
            pk_cols=['team_id_canonical'],
        ),

        # ----- dim_manager: ref_integrity → silver.xref_manager — ERROR -----
        CHECK.ref_integrity(
            'gold.dim_manager',
            'silver.xref_manager',
            'manager_id_canonical',
            parent_key='canonical_id',
        ),

        # ----- dim_manager: ref_integrity → silver.xref_team — ERROR -----
        CHECK.ref_integrity(
            'gold.dim_manager',
            'silver.xref_team',
            'team_id_canonical',
            parent_key='canonical_id',
        ),

        # ============================================================
        # T4: dim_player_attributes — cross-source snapshot per canonical
        # player. Additive относительно dim_player (per-season FBref-only).
        # FotMob coverage низкая (~40%) потому что FotMob Bronze покрывает
        # только APL 2025, а FBref-spine — все сезоны (history). Coverage
        # thresholds выставлены под реальный baseline; tighten после
        # подключения R3 источников (Sofascore/Transfermarkt).
        # ============================================================
        CHECK.no_duplicates('gold.dim_player_attributes',
                            pk=['player_id_canonical']),
        CHECK.no_nulls('gold.dim_player_attributes',
                       cols=['player_id_canonical']),
        CHECK.ref_integrity(
            'gold.dim_player_attributes',
            'silver.xref_player',
            'player_id_canonical',
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

        # ============================================================
        # issue #11: fct_player_market_value — FotMob MV timeline per
        # (player_id_canonical, value_date, league, season). Cross-season
        # дубликаты исторических точек ожидаемы (FotMob отдаёт history в
        # каждом ingest-snapshot) — PK включает (league, season).
        # ============================================================
        CHECK.no_duplicates('gold.fct_player_market_value',
                            pk=['player_id_canonical', 'value_date',
                                'league', 'season']),
        CHECK.no_nulls('gold.fct_player_market_value',
                       cols=['player_id_canonical', 'value_date',
                             'market_value_eur', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_player_market_value',
            'gold.dim_player_attributes',
            'player_id_canonical',
            parent_key='player_id_canonical',
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
        CHECK.no_duplicates('gold.fct_player_season_stats',
                            pk=['player_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_player_season_stats',
                       cols=['player_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_player_season_stats',
            'gold.dim_player_attributes',
            'player_id_canonical',
            parent_key='player_id_canonical',
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
        CHECK.no_duplicates('gold.fct_keeper_season_stats',
                            pk=['player_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_keeper_season_stats',
                       cols=['player_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_keeper_season_stats',
            'gold.dim_player_attributes',
            'player_id_canonical',
            parent_key='player_id_canonical',
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
                            pk=['player_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_player_season_stats_audit',
                       cols=['player_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_player_season_stats_audit',
            'gold.fct_player_season_stats',
            'player_id_canonical',
            parent_key='player_id_canonical',
        ),
        # 8 audit-diff coverage WARNING-only (error_threshold=0). Audit —
        # observability, не gate; ERROR ломал бы DAG при нормальных
        # cross-source расхождениях (например FotMob не отдаёт нулевой
        # penalty_won → diff=NULL для большинства rows). NULL diff
        # засчитывается как "not measured" (passed) — не ошибка.
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
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(penalties_won_diff_fotmob) <= 1 OR penalties_won_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.penalties_won]'),
        CHECK.coverage('gold.fct_player_season_stats_audit',
                       condition='ABS(penalties_conceded_diff_fotmob) <= 1 OR penalties_conceded_diff_fotmob IS NULL',
                       warn_threshold=0.95, error_threshold=0.0,
                       name='audit_diff[fct_player_season_stats_audit.penalties_conceded]'),
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
                            pk=['player_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_keeper_season_stats_audit',
                       cols=['player_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_keeper_season_stats_audit',
            'gold.fct_keeper_season_stats',
            'player_id_canonical',
            parent_key='player_id_canonical',
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
        CHECK.no_duplicates('gold.fct_team_season_stats',
                            pk=['team_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_team_season_stats',
                       cols=['team_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_team_season_stats',
            'gold.dim_team',
            'team_id_canonical',
            parent_key='team_id',
        ),
        # MODELED xG/xA — bounded domain on season-level (≤ ~150 for top APL teams).
        CHECK.value_range('gold.fct_team_season_stats', 'expected_goals',
                          min_val=0, max_val=150, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'expected_goals_against',
                          min_val=0, max_val=150, severity='ERROR'),
        CHECK.value_range('gold.fct_team_season_stats', 'npxg',
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

        # ============================================================
        # T6.4 (#94) audit: fct_team_season_stats_audit — cross-source DQ
        # для HARD_FACT diff'ов. INNER FBref ∩ Understat (primary secondary),
        # LEFT WS/SS. WARNING-only — audit observability, не gate. Threshold
        # ±1 для целочисленных событий, ±0.5 для xG-derived (RX2 r ≥ 0.99).
        # NULL diff = "источник отсутствует" → засчитывается как passed.
        # ============================================================
        CHECK.no_duplicates('gold.fct_team_season_stats_audit',
                            pk=['team_id_canonical', 'league', 'season']),
        CHECK.no_nulls('gold.fct_team_season_stats_audit',
                       cols=['team_id_canonical', 'league', 'season']),
        CHECK.ref_integrity(
            'gold.fct_team_season_stats_audit',
            'gold.fct_team_season_stats',
            'team_id_canonical',
            parent_key='team_id_canonical',
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
                            pk=['match_id_canonical', 'player_id_canonical']),
        CHECK.no_nulls('gold.fct_player_match_audit',
                       cols=['match_id_canonical', 'player_id_canonical']),
        # audit ⊆ main fct (INNER FBref ∩ SofaScore) → каждая audit-строка
        # должна находить парную строку в gold.fct_player_match.
        CHECK.ref_integrity(
            'gold.fct_player_match_audit',
            'gold.fct_player_match',
            'player_id_canonical',
            parent_key='player_id_canonical',
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
        CHECK.row_count('gold.dim_team',         min_rows=50),
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
        # issue #46 audit: INNER FBref ∩ SofaScore — pewer rows than main.
        # SofaScore cherry-pick покрывает APL 2024/25 + 2025/26 (~526 игроков
        # на сезон × ~38 матчей × ~22 в составе ≈ 22000 audit-rows). Floor 1000
        # с запасом на тестовые/частичные backfill'ы.
        CHECK.row_count('gold.fct_player_match_audit', min_rows=1000),
        # T6.4 (#94): cross-source team-season stats. APL spine ≈20 teams × 5+
        # seasons = ≥100 expected, floor 80 с запасом на partition gaps.
        # Audit INNER FBref ∩ Understat — Understat covers all APL seasons, so
        # audit floor ≈ main fct (≈80).
        CHECK.row_count('gold.fct_team_season_stats',       min_rows=80),
        CHECK.row_count('gold.fct_team_season_stats_audit', min_rows=80),
        CHECK.row_count('gold.match_outcomes',   min_rows=3000),

        # ===== E2: master-data dim row-count floors =====
        # dim_venue: APL has ~20 active stadiums per season; 9+ seasons of
        # history with promotion/relegation churn comfortably exceeds 20 unique.
        CHECK.row_count('gold.dim_venue',     min_rows=20),
        # dim_referee: typically ~30+ active EPL match officials across history.
        CHECK.row_count('gold.dim_referee',   min_rows=30),
        # dim_manager: SCD-2, one row per manager × team × stint. APL has
        # ~30-50 distinct head coaches across 8 seasons (2017-18 → 2024-25)
        # with frequent in-season changes; ~50 stint rows is a conservative
        # floor that still catches a wholly empty table.
        CHECK.row_count('gold.dim_manager',   min_rows=20),
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
