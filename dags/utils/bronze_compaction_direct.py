"""Host-only nightly driver; no Airflow imports or persisted file paths."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import tempfile
import time

from utils import bronze_compaction as compaction


def _now():
    return datetime.now(timezone.utc)


class StopFile:
    def __init__(self, path):
        self.path = path

    def is_set(self):
        return self.path.exists()


def _save(path, state):
    """Replace JSON without replacing the inode used for process admission."""
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
            name = out.name
            json.dump(state, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


@contextmanager
def _locked(path):
    with Path(str(path) + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def _load(path, tables, window):
    if path.exists():
        state = json.loads(path.read_text())
        # Never turn malformed/incompatible state into a fresh nightly budget.
        if (
            state.get("version") not in (1, 2)
            or state.get("tables") != tables
            or not isinstance(state.get("window"), str)
            or state["window"] > window
            or any(type(state.get(k)) is not int or state[k] < 0
                   for k in ("portions", "input_bytes"))
            or state["portions"] > compaction.MAX_PORTIONS
            or state["input_bytes"] > compaction.MAX_INPUT_BYTES
            or not isinstance(state.get("queue"), list)
            or len(set(state["queue"])) != len(state["queue"])
            or set(state["queue"]) - set(tables)
            or (state.get("version") == 1 and (
                not isinstance(state.get("partition_offsets"), dict)
                or any(k not in tables or type(v) is not int or v < 0
                       for k, v in state["partition_offsets"].items())))
            or (state.get("version") == 2 and (
                not isinstance(state.get("partition_cursors"), dict)
                or set(state["partition_cursors"]) - set(tables)))
        ):
            raise ValueError("Invalid direct compaction state")
        datetime.strptime(state["window"], "%Y-%m-%d")
        compaction._migrate_partition_cursors(state)
        state["version"] = 2
    else:
        state = dict(version=2, tables=tables, window=window, portions=0,
                     input_bytes=0, queue=tables[:], partition_cursors={})
    if state["window"] != window:
        state.update(window=window, portions=0, input_bytes=0)
        if not state["queue"]:
            state["queue"] = tables[:]
    return state


def _maintenance_ready(dsn, now):
    """The scheduled daily for this night must have finished; fail closed."""
    import psycopg2

    conn = psycopg2.connect(
        dsn, connect_timeout=2,
        options="-c statement_timeout=5000 -c lock_timeout=1000",
    )
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cursor:
            logical_date = (now - timedelta(days=1)).replace(
                hour=2, minute=0, second=0, microsecond=0
            )
            cursor.execute(
                "SELECT NOT EXISTS (SELECT 1 FROM public.dag_run "
                "WHERE dag_id IN ('dag_iceberg_maintenance_daily', "
                "'dag_iceberg_maintenance') AND state = 'running') "
                "AND EXISTS (SELECT 1 FROM public.dag_run "
                "WHERE dag_id = 'dag_iceberg_maintenance_daily' "
                "AND execution_date = %s AND state IN ('success', 'failed'))",
                (logical_date,),
            )
            return cursor.fetchone()[0] is True
    finally:
        conn.close()


def _wait_ready(dsn, deadline, stop):
    while True:
        if stop.is_set():
            raise InterruptedError("stop")
        if _now() >= deadline:
            raise TimeoutError("window")
        if _maintenance_ready(dsn, _now()):
            # A bounded DB call may itself have crossed the deadline.
            if stop.is_set():
                raise InterruptedError("stop")
            if _now() >= deadline:
                raise TimeoutError("window")
            return
        time.sleep(min(5, max(0, (deadline - _now()).total_seconds())))


def run_direct(args, deadline):
    """Run serial portions until exhausted, stopped, or the nightly budget ends."""
    summary = dict(event="summary", portions=0, input_bytes=0, tables={},
                   stop_reason="error", exit_code=1)
    old_logging = logging.root.manager.disable
    # Dependency exceptions can contain URLs, passwords or server diagnostics.
    # Only the explicit, numeric journal below is emitted in host mode.
    logging.disable(logging.CRITICAL)
    try:
        with args.journal.open("a") as journal:
            def record(event):
                journal.write(json.dumps(event) + "\n")
                journal.flush()

            try:
                tables = sorted(args.table)
                if (len(set(tables)) != len(tables) or not tables
                        or set(tables) - set(compaction.BRONZE_COMPACTION_TABLES)):
                    raise ValueError("Invalid physical table allowlist")
                for key in ("TRINO_USER", "TRINO_PASSWORD", "FBREF_CONTROL_DB_URI"):
                    if not os.environ.get(key):
                        raise ValueError("Missing required environment")
                dsn = os.environ["FBREF_CONTROL_DB_URI"]
                stop = StopFile(args.stop_file)
                with _locked(args.state_file):
                    state = _load(args.state_file, tables, _now().date().isoformat())
                    if not args.apply:
                        # _load creates an independent in-memory copy; never save it.
                        state["portions"] = state["input_bytes"] = 0
                        state["queue"] = tables[:]

                    def reserve(table, size, next_cursor):
                        if (state["portions"] >= args.portions
                                or state["input_bytes"] + size > args.bytes):
                            return False
                        # Charge AND rotate before ALTER, including process death.
                        state["portions"] += 1
                        state["input_bytes"] += size
                        state["partition_cursors"][table] = next_cursor
                        state["queue"].remove(table)
                        state["queue"].append(table)
                        _save(args.state_file, state)
                        summary["portions"] += 1
                        summary["input_bytes"] += size
                        return True

                    def portion(event):
                        event = {k: event[k] for k in
                                 ("table", "files", "bytes", "files_before", "files_after")}
                        event["event"] = "portion"
                        record(event)
                        counts = summary["tables"].setdefault(event["table"], {})
                        if "files_before" not in counts:
                            counts["files_before"] = event["files_before"]
                        counts["files_after"] = event["files_after"]

                    while state["queue"]:
                        if (state["portions"] >= args.portions
                                or state["input_bytes"] >= args.bytes):
                            summary["stop_reason"] = "budget"
                            break
                        _wait_ready(dsn, deadline, stop)
                        table = state["queue"][0]
                        result = compaction.run_compaction(
                            tables=[table], deadline=deadline, dry_run=not args.apply,
                            stop=stop, max_portions=args.portions,
                            max_input_bytes=args.bytes - state["input_bytes"], portion_limit=1,
                            partition_cursors=state["partition_cursors"],
                            window_budget=state if args.apply else None,
                            reserve=reserve if args.apply else None,
                            connection_factory=lambda: compaction.maintenance._get_trino_connection(
                                host="127.0.0.1", port=8082),
                            record=portion,
                        )
                        if not args.apply:
                            summary["portions"] += result["portions"]
                            summary["input_bytes"] += result["input_bytes"]
                        if result["failures"]:
                            summary.update(stop_reason="portion_failed", exit_code=1)
                            break
                        if result["stop_reason"] == "budget":
                            summary["stop_reason"] = "budget"
                            break
                        if result["stop_reason"] == "busy":
                            time.sleep(min(5, max(0, (deadline - _now()).total_seconds())))
                            continue
                        if not args.apply or not result["queue"]:
                            state["queue"].remove(table)
                        if not args.apply:
                            state["portions"] += result["portions"]
                            state["input_bytes"] += result["input_bytes"]
                        else:
                            _save(args.state_file, state)
                    else:
                        summary["stop_reason"] = "exhausted"
                    if summary["stop_reason"] != "portion_failed":
                        summary["exit_code"] = 0
            except (TimeoutError, InterruptedError) as exc:
                summary.update(stop_reason="deadline" if isinstance(exc, TimeoutError)
                               else "stop", exit_code=0)
            except Exception as exc:
                summary.update(stop_reason="error", error_type=type(exc).__name__, exit_code=1)
            finally:
                record(summary)
    except Exception:
        # Even an unavailable journal must not expose credentials in a traceback.
        return 1
    finally:
        logging.disable(old_logging)
    return summary["exit_code"]
