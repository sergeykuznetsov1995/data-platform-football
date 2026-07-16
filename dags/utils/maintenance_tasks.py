"""
Iceberg Maintenance Tasks
=========================

Periodic maintenance for Iceberg tables: expire stale snapshots and remove
orphan files. Without this, delete-then-insert DAGs (e.g. dag_ingest_whoscored)
accumulate thousands of metadata snapshots — the `whoscored_events` warehouse
footprint reached 12K+ files / 26 GB while current data was only 49 MB before
the first sweep.

Trino 482 exposes the Iceberg table properties `delete_after_commit_enabled`
and `max_previous_versions`, but those only bound tracked metadata versions;
they do not expire snapshots or orphaned data files. Periodic sweeps from this
module therefore remain necessary for the complete lifecycle.

IMPORTANT (#266): `expire_snapshots` / `remove_orphan_files` reject any
`retention_threshold` shorter than Trino's configured minimum
(`iceberg.expire_snapshots_min_retention` / `..._remove_orphan_files...`,
both default 7d) with `INVALID_PROCEDURE_ARGUMENT`. The daily high-churn DAG
asks for '3d', so without lowering that minimum every expire silently fails
and the sweep becomes a no-op. We lower it PER SESSION (`SET SESSION ...`) at
connection time — scoped to this connection only, no Trino restart, no global
config change. High-churn tables (e.g. `fotmob_match_details`) also commit
hundreds of snapshots per run, so the per-session floor must be well under the
requested threshold.

Uses `_get_trino_connection()` from `silver_tasks` (lightweight `import trino`,
avoids heavy `scrapers/__init__.py`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable, List, Optional, Sequence, Tuple

import trino as trino_lib

from utils.silver_tasks import _get_trino_connection

logger = logging.getLogger(__name__)

DEFAULT_SCHEMAS: Tuple[str, ...] = ("bronze", "silver", "gold")
DEFAULT_RETENTION = "30d"

# Per-session floor for the expire/orphan retention guards. Set well below any
# `retention_threshold` this module uses (down to '3d' for daily high-churn) so
# the requested threshold is always honored. See #266 — without this the daily
# sweep's '3d' is rejected by Trino's 7d default and every expire no-ops.
SESSION_MIN_RETENTION = "1h"

# High-churn tables — daily DAGs do delete-then-insert, so even a 7-day
# retention leaves >14 stale snapshots between weekly sweeps. The daily DAG
# keeps a longer rollback window for WhoScored than for the other high-churn
# feeds: raw WhoScored payloads are durable, but operators still need enough
# Iceberg history to undo a bad publication without performing a full replay.
WHOSCORED_DAILY_RETENTION = "14d"
OTHER_HIGH_CHURN_DAILY_RETENTION = "3d"

# Live-file compaction is deliberately much tighter than snapshot cleanup.
# A maintenance task may rewrite at most four exact path sets / 2 GiB of input,
# and each table contributes at most 64 files / 512 MiB from one partition.
# The selector ranks partitions from Iceberg's ``$files`` metadata, then reads
# at most 256 paths from one eligible partition. OPTIMIZE is never allowed to
# scan/rewrite a whole table.
COMPACTION_FILE_SIZE_THRESHOLD = "64MB"
COMPACTION_SMALL_FILE_MAX_BYTES = 64 * 1024 * 1024
COMPACTION_DISCOVERY_MAX_FILES = 256
COMPACTION_MAX_FILES_PER_TABLE = 64
COMPACTION_MAX_INPUT_BYTES_PER_TABLE = 512 * 1024 * 1024
COMPACTION_MAX_TABLES_PER_RUN = 4
COMPACTION_MAX_INPUT_BYTES_PER_RUN = 2 * 1024 * 1024 * 1024
COMPACTION_DELETE_FILE_POLICY = "skip_partitions_with_live_delete_files"

WHOSCORED_OPERATIONAL_HIGH_CHURN: Tuple[str, ...] = (
    # Terminal backfill DQ atomically replaces one population partition and
    # prunes expired partitions. Snapshot/orphan maintenance is still required
    # to reclaim the replaced/deleted Iceberg files.
    "whoscored_backfill_dq_population",
)

WHOSCORED_HIGH_CHURN: Tuple[str, ...] = (
    # All 25 business datasets, five commit manifests, and durable operational
    # relations. Mapped writers can create many snapshots even when payloads
    # are append-only.
    "whoscored_competitions",
    "whoscored_seasons",
    "whoscored_stages",
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
    "whoscored_matches",
    "whoscored_events",
    "whoscored_lineups",
    "whoscored_substitutions",
    "whoscored_formations",
    "whoscored_team_match_stats",
    "whoscored_player_match_stats",
    "whoscored_preview_lineups",
    "whoscored_missing_players",
    "whoscored_preview_sections",
    "whoscored_player_profile_versions",
    "whoscored_player_stage_participations",
    "whoscored_catalog_manifest",
    "whoscored_scope_ingest_manifest",
    "whoscored_match_ingest_manifest",
    "whoscored_preview_ingest_manifest",
    "whoscored_profile_ingest_manifest",
    *WHOSCORED_OPERATIONAL_HIGH_CHURN,
)

HIGH_CHURN_BRONZE: Tuple[str, ...] = (
    *WHOSCORED_HIGH_CHURN,
    "fbref_match_events",
    "fbref_match_player_stats",
    "fbref_match_team_stats",
    "fbref_lineups",
    "understat_shots",
    "understat_player_match_stats",
    "understat_players",
    "understat_schedule",
    "understat_team_match_stats",
    "matchhistory_results",  # #307: was matchhistory_games (legacy table dropped)
    # #266: daily fotmob/sofascore/espn writers were never on the list and
    # bloated to multi-GB metadata (fotmob_match_details hit 7.2G / 154M data).
    "fotmob_match_details",
    "fotmob_player_details",
    "fotmob_player_stats",
    "sofascore_player_ratings",
    "sofascore_player_universe",
    "sofascore_player_profile",
    "sofascore_player_season_stats",
    "sofascore_event_player_stats",
    "sofascore_match_stats",
    "espn_lineup",
    "espn_matchsheet",
)

NON_WHOSCORED_HIGH_CHURN: Tuple[str, ...] = tuple(
    table for table in HIGH_CHURN_BRONZE if table not in WHOSCORED_HIGH_CHURN
)


def _set_session_min_retention(conn) -> None:
    """Lower the expire/orphan retention floor for THIS session only (#266).

    Trino rejects `retention_threshold` shorter than the configured minimum
    (default 7d). Scoped `SET SESSION` lets the daily DAG's '3d' (and the
    aggressive cleanup of churn tables) actually run, without a Trino restart
    or a global config change.
    """
    cur = conn.cursor()
    try:
        for prop in (
            "iceberg.expire_snapshots_min_retention",
            "iceberg.remove_orphan_files_min_retention",
        ):
            cur.execute(f"SET SESSION {prop} = '{SESSION_MIN_RETENTION}'")
            cur.fetchall()
    finally:
        cur.close()


def _connect():
    """Open a Trino connection with the session retention floor lowered."""
    conn = _get_trino_connection()
    _set_session_min_retention(conn)
    return conn


def _row_to_stats(cursor) -> dict:
    """Convert one-row procedure output to {col_name: value}.

    Trino's `EXECUTE remove_orphan_files` returns either:
      - a single row with named columns (scanned_files_count, deleted_files_count, ...)
      - or, depending on procedure, a list of (name, value) pairs.
    Read via `cursor.description` to handle both shapes safely.
    """
    rows = cursor.fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in (cursor.description or [])]
    # Shape A: single row, multi-column
    if len(rows) == 1 and len(cols) == len(rows[0]) and len(cols) > 1:
        return {cols[i]: rows[0][i] for i in range(len(cols))}
    # Shape B: list of (name, value) pairs (legacy / different procedures)
    if all(len(r) == 2 for r in rows):
        return {r[0]: r[1] for r in rows}
    # Fallback — return as-is dict by column zero
    return {f"row_{i}": r for i, r in enumerate(rows)}


def _list_tables(conn, schema: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES FROM iceberg.{schema}")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [r[0] for r in rows]


def cleanup_whoscored_dq_stage_partitions() -> dict:
    """Apply wall-clock retention to frozen-DQ logical partitions.

    Snapshot/orphan maintenance only reclaims files after row deletes; it
    cannot remove an expired partition that remains current.  This scheduled
    entry point therefore complements the backfill's success-time cleanup and
    works even when no backfill has completed recently.
    """

    from scrapers.whoscored.runtime_contract import (
        require_production_runtime_class,
    )

    require_production_runtime_class(operation="WhoScored frozen DQ cleanup")
    from dags.scripts.whoscored_frozen_dq import (
        DQ_STAGE_RETENTION_DAYS,
        DQ_STAGE_TABLE,
        cleanup_staged_frozen_populations,
    )

    conn = _connect()
    try:
        if DQ_STAGE_TABLE not in _list_tables(conn, "bronze"):
            return {
                "status": "skipped",
                "reason": "stage_table_missing",
                "partitions_deleted": 0,
            }
        cur = conn.cursor()
        try:
            deleted = cleanup_staged_frozen_populations(
                cur,
                retention_days=DQ_STAGE_RETENTION_DAYS,
            )
        finally:
            cur.close()
    finally:
        conn.close()
    logger.info(
        "WhoScored frozen-DQ retention deleted %d expired partition(s)",
        deleted,
    )
    return {
        "status": "success",
        "retention_days": DQ_STAGE_RETENTION_DAYS,
        "partitions_deleted": deleted,
    }


def _exec_alter(conn, sql: str) -> dict:
    """Execute ALTER TABLE ... EXECUTE ... and return parsed stats."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return _row_to_stats(cur)
    finally:
        cur.close()


def _quote_identifier(value: str) -> str:
    """Quote a catalog identifier returned by Trino itself."""

    token = str(value)
    if not token or "\x00" in token:
        raise RuntimeError("Iceberg maintenance received an invalid identifier")
    return '"' + token.replace('"', '""') + '"'


def _path_sql_literal(value: object) -> str:
    """Render one metadata-owned file path as an injection-safe SQL literal."""

    if not isinstance(value, str) or not value or len(value) > 8192:
        raise RuntimeError("Iceberg compaction returned an invalid file path")
    if any(ord(char) < 32 for char in value):
        raise RuntimeError("Iceberg compaction file path contains a control character")
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class _CompactionProbeResult:
    """Bounded paths plus partitions excluded by the delete-file contract."""

    candidates: tuple[tuple[str, int], ...]
    skipped_delete_partitions: int


def _compaction_candidates(
    conn,
    *,
    schema: str,
    table: str,
    max_input_bytes: int,
) -> _CompactionProbeResult:
    """Return a bounded exact-path set from one small-file partition.

    The exact ``count_if`` predicate avoids permanently skipping partitions
    that contain both large and small files. The lowest lexical small-file
    path is a deterministic progress cursor: after those exact paths are
    rewritten, the next partition/path set advances without mutable state.

    Any partition containing a live position/equality delete file is excluded.
    Otherwise OPTIMIZE could read bytes that are absent from the data-file-only
    budget, and exact-path subset rewrites can leave active delete files behind.
    The skipped partition count is returned for operational visibility rather
    than silently undercounting that workload. Only one clean partition and a
    256-path window reach the bounded ranking CTE. Both the 64-file and byte
    limits are enforced in SQL and revalidated in Python before a path can
    reach ``OPTIMIZE``.
    """

    if type(max_input_bytes) is not int or max_input_bytes <= 0:
        raise ValueError("max_input_bytes must be a positive integer")
    budget = min(max_input_bytes, COMPACTION_MAX_INPUT_BYTES_PER_TABLE)
    if budget <= 0:
        return _CompactionProbeResult((), 0)
    schema_sql = _quote_identifier(schema)
    files_sql = _quote_identifier(f"{table}$files")
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH partition_inventory AS (
                SELECT
                    partition,
                    count_if(
                        content = 0
                        AND file_size_in_bytes > 0
                        AND file_size_in_bytes < {COMPACTION_SMALL_FILE_MAX_BYTES}
                    ) AS small_data_files_count,
                    count_if(content IS DISTINCT FROM 0) AS delete_files_count,
                    min(
                        IF(
                            content = 0
                            AND file_size_in_bytes > 0
                            AND file_size_in_bytes < {COMPACTION_SMALL_FILE_MAX_BYTES},
                            file_path,
                            NULL
                        )
                    ) AS first_small_file_path
                FROM iceberg.{schema_sql}.{files_sql}
                GROUP BY partition
            ),
            candidate_partition AS (
                SELECT partition
                FROM partition_inventory
                WHERE small_data_files_count >= 2
                  AND delete_files_count = 0
                ORDER BY first_small_file_path
                LIMIT 1
            ),
            candidate_files AS (
                SELECT f.file_path, f.file_size_in_bytes
                FROM iceberg.{schema_sql}.{files_sql} f
                JOIN candidate_partition p
                  ON f.partition IS NOT DISTINCT FROM p.partition
                WHERE f.content = 0
                  AND f.file_size_in_bytes > 0
                  AND f.file_size_in_bytes < {COMPACTION_SMALL_FILE_MAX_BYTES}
                ORDER BY f.file_path
                LIMIT {COMPACTION_DISCOVERY_MAX_FILES}
            ),
            bounded_files AS (
                SELECT
                    file_path,
                    file_size_in_bytes,
                    row_number() OVER (ORDER BY file_path) AS file_rank,
                    sum(file_size_in_bytes) OVER (
                        ORDER BY file_path
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_bytes
                FROM candidate_files
            ),
            selected_files AS (
                SELECT file_path, file_size_in_bytes, file_rank
                FROM bounded_files
                WHERE file_rank <= {COMPACTION_MAX_FILES_PER_TABLE}
                  AND running_bytes <= {budget}
            ),
            delete_file_summary AS (
                SELECT count_if(delete_files_count > 0)
                    AS skipped_delete_partitions
                FROM partition_inventory
            )
            SELECT
                selected.file_path,
                selected.file_size_in_bytes,
                summary.skipped_delete_partitions
            FROM selected_files selected
            CROSS JOIN delete_file_summary summary
            UNION ALL
            SELECT NULL, NULL, summary.skipped_delete_partitions
            FROM delete_file_summary summary
            WHERE NOT EXISTS (SELECT 1 FROM selected_files)
            ORDER BY file_path NULLS LAST
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    selected: list[tuple[str, int]] = []
    selected_paths: set[str] = set()
    selected_bytes = 0
    skipped_delete_partitions: Optional[int] = None
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            raise RuntimeError("Iceberg compaction metadata row is malformed")
        path = row[0]
        size = row[1]
        skipped = row[2]
        if type(skipped) is not int or skipped < 0:
            raise RuntimeError("Iceberg compaction delete-file summary is invalid")
        if (
            skipped_delete_partitions is not None
            and skipped != skipped_delete_partitions
        ):
            raise RuntimeError("Iceberg compaction delete-file summary is inconsistent")
        skipped_delete_partitions = skipped
        if path is None and size is None:
            if len(rows) != 1:
                raise RuntimeError("Iceberg compaction summary row is misplaced")
            continue
        # Validate the path now as well as at SQL rendering time. Metadata must
        # never be able to smuggle SQL through an operational procedure call.
        _path_sql_literal(path)
        if type(size) is not int or not 0 < size < COMPACTION_SMALL_FILE_MAX_BYTES:
            raise RuntimeError("Iceberg compaction metadata file size is invalid")
        if path in selected_paths:
            raise RuntimeError("Iceberg compaction metadata contains duplicate paths")
        selected.append((path, size))
        selected_paths.add(path)
        selected_bytes += size

    if len(selected) > COMPACTION_MAX_FILES_PER_TABLE or selected_bytes > budget:
        raise RuntimeError("Iceberg compaction metadata exceeded its SQL bounds")
    if skipped_delete_partitions is None:
        raise RuntimeError("Iceberg compaction delete-file summary is missing")

    # Trino rewrites small files only when at least two selected files belong
    # to the partition. A singleton procedure call would create no progress.
    return _CompactionProbeResult(
        tuple(selected) if len(selected) >= 2 else (),
        skipped_delete_partitions,
    )


@dataclass(frozen=True)
class _CompactionTarget:
    """One exact, already-bounded table rewrite candidate."""

    schema: str
    table: str
    display_name: str
    candidates: tuple[tuple[str, int], ...]

    @property
    def input_bytes(self) -> int:
        return sum(size for _path, size in self.candidates)


def _select_compaction_targets(
    candidates: Sequence[_CompactionTarget],
    *,
    rotation: int,
) -> list[_CompactionTarget]:
    """Choose a rotating, globally bounded subset of eligible tables.

    Airflow supplies a logical-run sequence (daily ordinal or weekly ordinal),
    keeping the rotation key stable across retries while moving the starting
    table by one full task allowance on the next scheduled run. Thus a stable
    set of eight perpetually eligible tables is covered in two runs instead of
    starving the latter four behind ``SHOW TABLES`` order.
    """

    if type(rotation) is not int or rotation < 0:
        raise ValueError("compaction_rotation must be a non-negative integer")
    if not candidates:
        return []

    ordered = sorted(candidates, key=lambda item: item.display_name)
    start = (rotation * COMPACTION_MAX_TABLES_PER_RUN) % len(ordered)
    rotated = ordered[start:] + ordered[:start]
    selected: list[_CompactionTarget] = []
    selected_bytes = 0
    for target in rotated:
        if len(selected) >= COMPACTION_MAX_TABLES_PER_RUN:
            break
        if target.input_bytes > COMPACTION_MAX_INPUT_BYTES_PER_TABLE:
            raise RuntimeError("Iceberg compaction target exceeds its table bound")
        if selected_bytes + target.input_bytes > COMPACTION_MAX_INPUT_BYTES_PER_RUN:
            continue
        selected.append(target)
        selected_bytes += target.input_bytes
    return selected


def _compact_exact_files(
    conn,
    *,
    schema: str,
    table: str,
    candidates: Sequence[tuple[str, int]],
) -> dict:
    """Compact only the validated file paths selected by the bounded probe."""

    if not 2 <= len(candidates) <= COMPACTION_MAX_FILES_PER_TABLE:
        raise RuntimeError("Iceberg compaction candidate count is outside its bound")
    total_bytes = 0
    path_literals: list[str] = []
    seen: set[str] = set()
    for path, size in candidates:
        literal = _path_sql_literal(path)
        if (
            path in seen
            or type(size) is not int
            or not 0 < size < COMPACTION_SMALL_FILE_MAX_BYTES
        ):
            raise RuntimeError("Iceberg compaction candidates are invalid")
        seen.add(path)
        total_bytes += size
        path_literals.append(literal)
    if total_bytes > COMPACTION_MAX_INPUT_BYTES_PER_TABLE:
        raise RuntimeError("Iceberg compaction input exceeds its per-table bound")

    fq = f"iceberg.{_quote_identifier(schema)}.{_quote_identifier(table)}"
    return _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE optimize("
        f"file_size_threshold => '{COMPACTION_FILE_SIZE_THRESHOLD}') "
        'WHERE "$path" IN (' + ",".join(path_literals) + ")",
    )


def _maintain_one(conn, fq: str, retention_threshold: str) -> dict:
    """Run expire_snapshots + remove_orphan_files on a single table.

    Returns parsed stats from remove_orphan_files (deleted_files_count etc.).
    """
    _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE expire_snapshots(retention_threshold => '{retention_threshold}')",
    )
    return _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE remove_orphan_files(retention_threshold => '{retention_threshold}')",
    )


def maintain_iceberg_tables(
    schemas: Tuple[str, ...] = DEFAULT_SCHEMAS,
    retention_threshold: str = DEFAULT_RETENTION,
    table_filter: Optional[Iterable[str]] = None,
    *,
    compact_live_files: bool = False,
    compaction_rotation: Optional[int] = None,
) -> dict:
    """Run bounded compaction and retention maintenance on Iceberg tables.

    Args:
        schemas: which Iceberg schemas to walk (default bronze/silver/gold).
        retention_threshold: '30d' for weekly, shorter values for the split
            daily high-churn groups.
            Requires `iceberg.{expire-snapshots,remove-orphan-files}.min-retention`
            in `configs/trino/catalog/iceberg.properties` to allow it.
        table_filter: if set, only tables whose short name is in this set
            are processed (used by the daily high-churn DAG).
        compact_live_files: opt in to an exact-path, globally bounded small-file
            rewrite before snapshot/orphan retention. A bare/full-table
            ``OPTIMIZE`` is never issued.
        compaction_rotation: logical scheduled-run sequence used to rotate the
            first eligible table fairly. Daily DAGs pass a date ordinal and
            weekly DAGs pass ``ordinal // 7``. Defaults to today's ordinal for
            direct/manual invocations.
    """
    filter_set = set(table_filter) if table_filter is not None else None
    if filter_set is None or filter_set.intersection(WHOSCORED_HIGH_CHURN):
        from scrapers.whoscored.runtime_contract import (
            require_production_runtime_class,
        )

        require_production_runtime_class(
            operation="WhoScored Iceberg lifecycle maintenance"
        )
    if type(compact_live_files) is not bool:
        raise ValueError("compact_live_files must be a boolean")
    if compaction_rotation is None:
        compaction_rotation = date.today().toordinal()
    if type(compaction_rotation) is not int or compaction_rotation < 0:
        raise ValueError("compaction_rotation must be a non-negative integer")

    conn = _connect()
    total_tables = 0
    total_deleted = 0
    total_scanned = 0
    retention_successes = 0
    retention_errors: List[str] = []
    failure_messages: dict[str, str] = {}
    tables_to_maintain: list[tuple[str, str, str, str]] = []

    compaction_tables_probed = 0
    compaction_tables_eligible = 0
    compaction_tables_selected = 0
    compaction_tables_succeeded = 0
    compaction_delete_partitions_skipped = 0
    compaction_files_selected = 0
    compaction_input_bytes_selected = 0
    compaction_rewritten_data_files = 0
    compaction_added_data_files = 0
    compaction_removed_delete_files = 0

    def add_failure(target: str, stage: str, error: object) -> None:
        detail = f"{stage}: {error}"
        previous = failure_messages.get(target)
        failure_messages[target] = (f"{previous}; {detail}" if previous else detail)[
            :600
        ]

    def reconnect() -> None:
        nonlocal conn
        try:
            conn.close()
        except Exception:
            pass
        conn = _connect()

    try:
        # Enumerate first so compaction selection is independent of SHOW TABLES
        # order and retention can still run for every discovered target.
        for schema in schemas:
            try:
                tables = _list_tables(conn, schema)
            except Exception as e:
                display_schema = f"iceberg.{schema}"
                logger.error("Failed to list tables in %s: %s", display_schema, e)
                add_failure(display_schema, "list_tables", e)
                reconnect()
                continue

            for table in tables:
                if filter_set is not None and table not in filter_set:
                    continue
                display_name = f"iceberg.{schema}.{table}"
                sql_name = (
                    f"iceberg.{_quote_identifier(schema)}.{_quote_identifier(table)}"
                )
                tables_to_maintain.append((schema, table, display_name, sql_name))

        total_tables = len(tables_to_maintain)

        if compact_live_files:
            eligible: list[_CompactionTarget] = []
            for schema, table, display_name, _sql_name in tables_to_maintain:
                compaction_tables_probed += 1
                try:
                    probe = _compaction_candidates(
                        conn,
                        schema=schema,
                        table=table,
                        max_input_bytes=COMPACTION_MAX_INPUT_BYTES_PER_TABLE,
                    )
                except trino_lib.exceptions.TrinoConnectionError as e:
                    logger.warning(
                        "Connection lost probing compaction for %s: %s",
                        display_name,
                        e,
                    )
                    add_failure(display_name, "compaction_probe_connection", e)
                    reconnect()
                    continue
                except Exception as e:
                    logger.error("Compaction probe failed on %s: %s", display_name, e)
                    add_failure(display_name, "compaction_probe", e)
                    continue

                compaction_delete_partitions_skipped += probe.skipped_delete_partitions
                if probe.candidates:
                    compaction_tables_eligible += 1
                    eligible.append(
                        _CompactionTarget(
                            schema=schema,
                            table=table,
                            display_name=display_name,
                            candidates=probe.candidates,
                        )
                    )

            selected_targets = _select_compaction_targets(
                eligible,
                rotation=compaction_rotation,
            )
            for target in selected_targets:
                # Charge budgets before executing. A failed procedure attempt
                # must not allow a later table to exceed this task's rewrite
                # envelope.
                compaction_tables_selected += 1
                compaction_files_selected += len(target.candidates)
                compaction_input_bytes_selected += target.input_bytes
                try:
                    stats = _compact_exact_files(
                        conn,
                        schema=target.schema,
                        table=target.table,
                        candidates=target.candidates,
                    )
                    compaction_tables_succeeded += 1
                    compaction_rewritten_data_files += int(
                        stats.get("rewritten_data_files_count", 0) or 0
                    )
                    compaction_added_data_files += int(
                        stats.get("added_data_files_count", 0) or 0
                    )
                    compaction_removed_delete_files += int(
                        stats.get("removed_delete_files_count", 0) or 0
                    )
                except trino_lib.exceptions.TrinoConnectionError as e:
                    logger.warning(
                        "Connection lost compacting %s: %s",
                        target.display_name,
                        e,
                    )
                    add_failure(
                        target.display_name,
                        "compaction_connection",
                        e,
                    )
                    reconnect()
                except Exception as e:
                    logger.error("Compaction failed on %s: %s", target.display_name, e)
                    add_failure(target.display_name, "compaction", e)

        # Retention deliberately follows compaction, but preserves the rollback
        # window: superseded files from this rewrite remain referenced until its
        # snapshot ages past the threshold. This pass only reclaims history that
        # was already old enough. Every table is attempted even if compaction
        # discovery or execution failed.
        for _schema, _table, display_name, sql_name in tables_to_maintain:
            try:
                stats = _maintain_one(conn, sql_name, retention_threshold)
                retention_successes += 1
                deleted = int(stats.get("deleted_files_count", 0) or 0)
                scanned = int(stats.get("scanned_files_count", 0) or 0)
                total_deleted += deleted
                total_scanned += scanned
                if deleted > 0:
                    logger.info(
                        "%s: scanned=%d deleted=%d",
                        display_name,
                        scanned,
                        deleted,
                    )
            except trino_lib.exceptions.TrinoConnectionError as e:
                logger.warning(
                    "Connection lost on %s, reconnecting: %s", display_name, e
                )
                add_failure(display_name, "retention_connection", e)
                retention_errors.append(str(e))
                reconnect()
            except Exception as e:
                logger.error("Maintenance failed on %s: %s", display_name, e)
                add_failure(display_name, "retention", e)
                retention_errors.append(str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    failures: List[Tuple[str, str]] = list(failure_messages.items())
    if compaction_delete_partitions_skipped > 0:
        logger.warning(
            "Iceberg compaction policy %s skipped %d live-delete partition(s)",
            COMPACTION_DELETE_FILE_POLICY,
            compaction_delete_partitions_skipped,
        )
    logger.info(
        "Iceberg maintenance done: tables=%d retained=%d scanned=%d "
        "deleted=%d compacted=%d/%d selected_files=%d selected_bytes=%d "
        "delete_partitions_skipped=%d failures=%d",
        total_tables,
        retention_successes,
        total_scanned,
        total_deleted,
        compaction_tables_succeeded,
        compaction_tables_selected,
        compaction_files_selected,
        compaction_input_bytes_selected,
        compaction_delete_partitions_skipped,
        len(failures),
    )
    for fq, err in failures:
        logger.warning("  FAIL %s: %s", fq, err)

    # #266: a systemic misconfiguration (e.g. min-retention floor above the
    # requested threshold) makes EVERY per-table expire fail while the task
    # still returns "success". Raise when nothing could be processed so the
    # sweep can no longer no-op silently.
    if total_tables > 0 and retention_successes == 0:
        first_error = retention_errors[0] if retention_errors else "unknown error"
        raise RuntimeError(
            f"Iceberg maintenance failed on all {total_tables} tables "
            f"(first retention error: {first_error})"
        )

    return {
        "tables_processed": total_tables,
        "retention_tables_succeeded": retention_successes,
        "files_scanned": total_scanned,
        "files_deleted": total_deleted,
        "compaction_enabled": compact_live_files,
        "compaction_tables_probed": compaction_tables_probed,
        "compaction_tables_eligible": compaction_tables_eligible,
        "compaction_tables_selected": compaction_tables_selected,
        "compaction_tables_succeeded": compaction_tables_succeeded,
        "compaction_delete_file_policy": COMPACTION_DELETE_FILE_POLICY,
        "compaction_delete_partitions_skipped": (compaction_delete_partitions_skipped),
        "compaction_files_selected": compaction_files_selected,
        "compaction_input_bytes_selected": compaction_input_bytes_selected,
        "compaction_rewritten_data_files": compaction_rewritten_data_files,
        "compaction_added_data_files": compaction_added_data_files,
        "compaction_removed_delete_files": compaction_removed_delete_files,
        "failures": failures,
    }
