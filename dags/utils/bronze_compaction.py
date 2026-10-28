"""Serial, bounded Bronze compaction; snapshot retention belongs to other tasks."""

from __future__ import annotations

from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
from threading import Event, Thread
import time

from utils import maintenance_tasks as maintenance

logger = logging.getLogger(__name__)
MAX_PORTIONS = 120
MAX_INPUT_BYTES = 8 * 1024**3

# Physical writer contracts, separate from the retention allowlist. Frozen
# legacy FotMob/ESPN relations and ClubElo historical are intentionally absent.
FBREF_TABLES = (
    "fbref_schedule",
    "fbref_target_scope",
    "fbref_page_manifest",
    "fbref_table_inventory",
    "fbref_table_cells",
    "fbref_dataset_availability",
    "fbref_shot_events",
    "fbref_match_events",
    "fbref_lineups",
    "fbref_match_team_stats",
    "fbref_match_managers",
    "fbref_match_officials",
    "fbref_match_keeper_stats",
    "fbref_match_player_stats",
    *(
        f"fbref_{entity}_{dataset}"
        for entity in ("player", "team")
        for dataset in (
            "stats",
            "shooting",
            "passing",
            "passing_types",
            "gca",
            "defense",
            "possession",
            "playingtime",
            "misc",
        )
    ),
    "fbref_keeper_keeper",
    "fbref_keeper_keeper_adv",
)
ESPN_TABLES = (
    "espn_match",
    "espn_match_lineup",
    "espn_team_stats",
    "espn_match_events",
)
SOFASCORE_TABLES = (
    "sofascore_schedule",
    "sofascore_events",
    "sofascore_event_participants",
    "sofascore_incidents",
    "sofascore_league_table",
    "sofascore_lineups",
    "sofascore_event_shotmap",
    "sofascore_event_player_stats",
    "sofascore_match_stats",
    "sofascore_match_capture_status",
    "sofascore_venue",
    "sofascore_rejected_rows",
    "sofascore_player_ratings",
    "sofascore_player_profile",
    "sofascore_player_universe",
    "sofascore_player_season_stats",
)
BRONZE_COMPACTION_TABLES = (
    *maintenance.WHOSCORED_HIGH_CHURN,
    *maintenance.FOTMOB_NATIVE_HIGH_CHURN,
    *FBREF_TABLES,
    *ESPN_TABLES,
    *SOFASCORE_TABLES,
    "understat_shots",
    "understat_player_match_stats",
    "understat_players",
    "understat_schedule",
    "understat_team_match_stats",
    "understat_player_team_season_stats",
    "understat_team_season_breakdowns",
    "matchhistory_results",
)


def nightly_deadline(now=None):
    now = now or datetime.now(timezone.utc)
    if not 2 <= now.hour < 5:
        raise TimeoutError("Bronze compaction is permitted only at 02:00–05:00 UTC")
    return now.replace(hour=5, minute=0, second=0, microsecond=0)


class _DeadlineConnection:
    """Enforce the same absolute deadline during probes, retries and queries."""

    def __init__(self, connection, deadline, stop, fence):
        self.connection = connection
        self.deadline = deadline
        self.stop = stop
        self.fence = fence
        self.errors = []

    def check(self):
        if self.errors:
            raise self.errors[0]
        if self.stop.is_set():
            raise InterruptedError("Bronze compaction stopped")
        if time.time() >= self.deadline.timestamp():
            raise TimeoutError("Bronze compaction deadline reached")

    def cursor(self):
        owner = self
        raw = self.connection.cursor()
        done = Event()
        errors = []
        worker = heartbeat = None

        def watch():
            while not done.wait(0.2):
                try:
                    owner.check()
                except Exception as exc:
                    if not errors:
                        errors.append(exc)
                if errors:
                    # Keep cancelling if execute had not yet acquired a query ID.
                    try:
                        raw.cancel()
                    except Exception:
                        logger.exception("Compaction query cancellation failed")

        def renew():
            # DB renewal must never block the cancellation watchdog.
            while not done.wait(10):
                try:
                    owner.fence()
                except Exception as exc:
                    owner.errors.append(exc)
                    errors.append(exc)
                    return

        class Cursor:
            @property
            def description(self):
                return raw.description

            def execute(self, sql):
                nonlocal worker, heartbeat
                owner.check()
                owner.fence()
                worker = Thread(target=watch, daemon=True)
                worker.start()
                heartbeat = Thread(target=renew, daemon=True)
                heartbeat.start()
                # Server-side timeout backs up client cancellation, also for
                # the TM committing retry wrapper (every attempt gets a cursor).
                seconds = max(1, math.floor(owner.deadline.timestamp() - time.time()))
                raw.execute(f"SET SESSION query_max_run_time = '{seconds}s'")
                raw.fetchall()
                if errors:
                    raise errors[0]
                owner.check()
                owner.fence()
                owner.check()
                if errors:
                    raise errors[0]
                try:
                    raw.execute(sql)
                except Exception:
                    owner.check()
                    if errors:
                        raise errors[0]
                    raise

            def fetchall(self):
                try:
                    rows = raw.fetchall()
                except Exception:
                    if errors:
                        raise errors[0]
                    raise
                if errors:
                    raise errors[0]
                owner.check()
                owner.fence()
                owner.check()
                return rows

            def close(self):
                done.set()
                if worker is not None:
                    worker.join(timeout=1)
                if heartbeat is not None:
                    heartbeat.join(timeout=2)
                raw.close()

        return Cursor()


@contextmanager
def _publication_fence(enabled, deadline=None):
    if not enabled:
        yield lambda: None
        return
    from scrapers.fbref.control import ControlStore

    def connect(dsn):
        import psycopg2

        return psycopg2.connect(
            dsn,
            connect_timeout=1,
            options="-c statement_timeout=1000 -c lock_timeout=500",
            keepalives=1,
            keepalives_idle=1,
            keepalives_interval=1,
            keepalives_count=1,
            tcp_user_timeout=1000,
        )

    def lease_seconds():
        if deadline is None:
            return 4 * 60 * 60
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            raise TimeoutError("FBref control deadline reached")
        # A crashed maintenance worker must not block the next 06:00 ingest.
        return max(60, math.ceil(remaining) + 60)

    control = ControlStore.from_env(connection_factory=connect)
    run_id = control.create_run(
        "maintenance",
        request_limit=0,
        byte_limit=0,
        metadata={"purpose": "bronze_compaction", "network": False},
    )
    control.start_run(run_id)
    acquired = succeeded = False
    try:
        control.acquire_publication_lock(
            run_id,
            dag_id="dag_iceberg_maintenance_daily",
            ttl_seconds=lease_seconds(),
        )
        acquired = True

        def fence():
            control.renew_publication_lock(run_id, ttl_seconds=lease_seconds())
            control.assert_publication_lock_owner(run_id)

        yield fence
        succeeded = True
    finally:
        try:
            if acquired:
                control.release_publication_lock(run_id)
        finally:
            control.finish_run(run_id, succeeded=succeeded)


def _shared_budget(deadline, *, reservation=None):
    """Atomically reserve the UTC window budget; keep partition cursors across nights.

    One metadata row serves daily and all catch-up runs. The transaction lock
    also serializes first creation. A crashed/failed portion keeps its charge.
    """
    from airflow.models import Variable
    from airflow.utils.session import create_session
    from sqlalchemy import text

    def check_window():
        end = nightly_deadline()
        if (
            deadline.tzinfo is None
            or deadline > end
            or deadline.astimezone(timezone.utc).date() != end.date()
            or deadline <= datetime.now(timezone.utc)
        ):
            raise TimeoutError("Shared compaction budget requires a live nightly deadline")

    check_window()
    key = "bronze_compaction_shared_budget"
    with create_session() as session:
        session.execute(text("SET LOCAL lock_timeout = '1s'"))
        session.execute(text("SET LOCAL statement_timeout = '1s'"))
        session.execute(text("SELECT pg_advisory_xact_lock(1892)"))
        check_window()
        row = session.query(Variable).filter(Variable.key == key).one_or_none()
        state = json.loads(row.val) if row else {}
        window = deadline.astimezone(timezone.utc).date().isoformat()
        if state.get("window") != window:
            state = {
                "window": window,
                "portions": 0,
                "input_bytes": 0,
                "partition_offsets": state.get("partition_offsets", {}),
            }
        if reservation is not None:
            table, size, next_offset = reservation
            if (
                state["portions"] >= MAX_PORTIONS
                or state["input_bytes"] + size > MAX_INPUT_BYTES
            ):
                return None
            state["portions"] += 1
            state["input_bytes"] += size
            state["partition_offsets"][table] = next_offset
        value = json.dumps(state)
        if row is None:
            session.add(Variable(key=key, val=value))
        else:
            row.val = value
        return state


def run_compaction(
    *,
    tables,
    deadline,
    max_portions=MAX_PORTIONS,
    max_input_bytes=MAX_INPUT_BYTES,
    dry_run=False,
    stop=None,
    rotation=0,
    record=None,
    checkpoint=None,
    portion_limit=None,
    reserve=None,
    window_budget=None,
    partition_offsets=None,
):
    """Re-read metadata for each portion; never replay paths or run retention.

    The caller must hold fbref_scraper_pool when selecting FBref tables.
    Each FBref portion owns a generation, renewed/fenced during Trino queries.
    A per-run Airflow Variable retains the queue across reschedules, never paths.
    The caller reserves the shared nightly budget before any rewrite.
    """
    if type(dry_run) is not bool:
        raise ValueError("dry_run must be boolean")
    tables = tuple(tables)
    if not tables or len(set(tables)) != len(tables):
        raise ValueError("An explicit, unique table allowlist is required")
    if set(tables) - set(BRONZE_COMPACTION_TABLES):
        raise ValueError("Unknown or frozen compaction table")
    if (
        not 1 <= max_portions <= MAX_PORTIONS
        or not 1 <= max_input_bytes <= MAX_INPUT_BYTES
    ):
        raise ValueError("Compaction run budget exceeds the approved profile")
    if deadline.tzinfo is None or deadline > nightly_deadline():
        raise ValueError("Deadline must be within this night's UTC window")
    stop = stop or Event()
    if set(tables).intersection(maintenance.WHOSCORED_HIGH_CHURN):
        from scrapers.whoscored.runtime_contract import require_production_runtime_class

        require_production_runtime_class(operation="WhoScored Bronze compaction")
    result = checkpoint or {
        "portions": 0,
        "input_bytes": 0,
        "selected_files": 0,
        "failures": [],
        "targets": {},
        "delete_partitions": {},
        "stop_reason": "exhausted",
    }
    result["stop_reason"] = "exhausted"
    portions_at_start = result["portions"]
    raw = maintenance._get_trino_connection()
    conn = _DeadlineConnection(raw, deadline, stop, lambda: None)
    try:
        conn.check()
        if checkpoint is None:
            physical = set(maintenance._list_tables(conn, "bronze"))
            queue = deque(sorted(set(tables) & physical))
            queue.rotate(-(rotation % max(1, len(queue))))
            for table in set(tables) - physical:
                result["targets"][table] = "not_present"
        else:
            queue = deque(result["queue"])
        partition_offsets = defaultdict(
            int,
            partition_offsets if partition_offsets is not None
            else result.get("partition_offsets", {}),
        )
        while queue:
            conn.check()
            if (
                portion_limit is not None
                and result["portions"] - portions_at_start >= portion_limit
            ):
                result["stop_reason"] = "yield"
                break
            if (
                result["portions"] >= max_portions
                or result["input_bytes"] >= max_input_bytes
                or (
                    window_budget is not None
                    and (
                        window_budget["portions"] >= MAX_PORTIONS
                        or window_budget["input_bytes"] >= MAX_INPUT_BYTES
                    )
                )
            ):
                result["stop_reason"] = "budget"
                break
            table = queue.popleft()
            try:
                with _publication_fence(table in FBREF_TABLES, deadline) as fence:
                    conn = _DeadlineConnection(raw, deadline, stop, fence)
                    input_budget = min(
                        max_input_bytes - result["input_bytes"],
                        MAX_INPUT_BYTES - window_budget["input_bytes"]
                        if window_budget is not None else MAX_INPUT_BYTES,
                    )
                    probe = maintenance._compaction_candidates(
                        conn,
                        schema="bronze",
                        table=table,
                        max_input_bytes=input_budget,
                        partition_offset=partition_offsets[table],
                        max_files=maintenance.COMPACTION_MAX_FILES_PER_PORTION,
                    )
                    if not probe.candidates and partition_offsets[table]:
                        partition_offsets[table] = 0
                        probe = maintenance._compaction_candidates(
                            conn,
                            schema="bronze",
                            table=table,
                            max_input_bytes=input_budget,
                            max_files=maintenance.COMPACTION_MAX_FILES_PER_PORTION,
                        )
                    result["delete_partitions"][table] = probe.skipped_delete_partitions
                    if not probe.candidates:
                        result["targets"][table] = (
                            "live_delete_files"
                            if probe.skipped_delete_partitions
                            else "no_candidates"
                        )
                        continue
                    paths = probe.candidates
                    if not dry_run and reserve is not None and not reserve(
                        table, sum(size for _, size in paths), partition_offsets[table] + 1
                    ):
                        queue.appendleft(table)
                        result["stop_reason"] = "budget"
                        break
                    if dry_run:
                        result["targets"][table] = "dry_run"
                        stats = {}
                    else:
                        # Charge before execution, including a failed attempt.
                        result["targets"][table] = "pending"
                        stats = None
                    result["portions"] += 1
                    result["input_bytes"] += sum(size for _, size in paths)
                    result["selected_files"] += len(paths)
                    if not dry_run:
                        before = maintenance._fetch_scalar(
                            conn,
                            f'SELECT count(*) FROM iceberg.bronze."{table}$files" WHERE content = 0',
                        )
                        stats = maintenance._compact_exact_files(
                            conn,
                            schema="bronze",
                            table=table,
                            candidates=paths,
                            max_files=maintenance.COMPACTION_MAX_FILES_PER_PORTION,
                        )
                        # Validate actual disappearance of selected paths rather
                        # than interpreting an empty procedure result as success.
                        literals = ",".join(
                            maintenance._path_sql_literal(p) for p, _ in paths
                        )
                        remaining = maintenance._fetch_scalar(
                            conn,
                            f'SELECT count(*) FROM iceberg.bronze."{table}$files" '
                            f"WHERE file_path IN ({literals})",
                        )
                        after = maintenance._fetch_scalar(
                            conn,
                            f'SELECT count(*) FROM iceberg.bronze."{table}$files" WHERE content = 0',
                        )
                        if (
                            type(remaining) is not int
                            or remaining
                            or type(before) is not int
                            or type(after) is not int
                            or after >= before
                        ):
                            raise RuntimeError("Compaction made no proven progress")
                        result["targets"][table] = "compacted"
                        partition_offsets[table] += 1
                        queue.append(table)
                    event = {
                        "table": table,
                        "files": len(paths),
                        "bytes": sum(size for _, size in paths),
                        "stats": stats,
                    }
                    logger.info("Bronze compaction portion: %s", event)
                    if record:
                        record(event)
            except (TimeoutError, InterruptedError):
                raise
            except Exception as exc:
                from scrapers.fbref.control import StateConflict

                if isinstance(exc, StateConflict):
                    if str(exc) == "FBref publication is locked by another control run":
                        queue.append(table)
                        result["stop_reason"] = "busy"
                        break
                    raise
                conn.check()
                result["failures"].append((table, str(exc)))
                result["targets"][table] = "failed"
        result["queue"] = list(queue)
        result["partition_offsets"] = dict(partition_offsets)
        for table in queue:
            result["targets"][table] = "pending"
    finally:
        raw.close()
    return result


def compaction_step(*, context, tables, deadline, **kwargs):
    """One portion per sensor poke releases the pool between rewrites."""
    checkpoint_key = None
    checkpoint = None
    if context.get("run_id") and context.get("task"):
        from airflow.models import Variable

        identity = "\0".join(
            (context["dag"].dag_id, context["run_id"], context["task"].task_id)
        )
        checkpoint_key = (
            "bronze_compaction_" + hashlib.sha256(identity.encode()).hexdigest()
        )
        checkpoint = Variable.get(
            checkpoint_key, default_var=None, deserialize_json=True
        )
    if checkpoint:
        deadline = datetime.fromisoformat(checkpoint["deadline"])
    shared = _shared_budget(deadline) if not kwargs.get("dry_run", False) else None

    def reserve(table, size, next_offset):
        return _shared_budget(
            deadline, reservation=(table, size, next_offset)
        ) is not None

    result = run_compaction(
        tables=tables,
        deadline=deadline,
        checkpoint=checkpoint,
        portion_limit=1,
        reserve=reserve if shared is not None else None,
        window_budget=shared,
        partition_offsets=shared["partition_offsets"] if shared is not None else None,
        **kwargs,
    )
    result["deadline"] = deadline.isoformat()
    if checkpoint_key:
        Variable.set(checkpoint_key, result, serialize_json=True)
    return result


def compact_nightly(*, reschedule_on_busy=False, **context):
    result = compaction_step(
        context=context,
        tables=BRONZE_COMPACTION_TABLES,
        deadline=nightly_deadline(),
        rotation=datetime.now(timezone.utc).date().toordinal(),
    )
    if reschedule_on_busy and result["stop_reason"] in {"busy", "yield"}:
        return False
    return result
