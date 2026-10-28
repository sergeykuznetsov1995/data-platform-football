"""Bounded, partition-only compaction of SofaScore schedule (#1879).

Trino Iceberg optimize also rewrites a single small data file with attached
Deletes (connector/iceberg optimize contract, confirmed in the task brief).
No source requests, schema changes or schedule row rewrites are performed here.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from scrapers.sofascore.writer_lock import (
    WRITER_LOCK_TIMEOUT_ENV,
    WriterLockTimeout,
    bronze_writer_lock,
)

logger = logging.getLogger(__name__)
TABLE = 'iceberg.bronze."sofascore_schedule"'
MAX_FILES = 1000
MAX_BYTES = 64 * 1024 * 1024
MAX_PARTITIONS = 32
QUERY_SECONDS = 180
HTTP_SECONDS = 10
RUN_SECONDS = 50 * 60
# Leave time for a query, failed cancellation and the server lifetime fallback.
CHUNK_RESERVE = 2 * QUERY_SECONDS + 4 * HTTP_SECONDS
MOSCOW = timezone(timedelta(hours=3))
FILES_SQL = '''SELECT partition.league, partition.season,
    count(*), coalesce(sum(file_size_in_bytes), 0),
    count_if(content <> 0),
    count_if(content = 0 AND file_size_in_bytes < 134217728)
    FROM iceberg.bronze."sofascore_schedule$files"
    GROUP BY partition.league, partition.season'''
METRICS_SQL = '''SELECT content, count(*), coalesce(sum(file_size_in_bytes), 0)
    FROM iceberg.bronze."sofascore_schedule$files" GROUP BY content'''
SNAPSHOT_SQL = '''SELECT snapshot_id FROM iceberg.bronze."sofascore_schedule$snapshots"
    ORDER BY committed_at DESC LIMIT 1'''


def utcnow():
    return datetime.now(timezone.utc)


def literal(value):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError("invalid schedule partition")
    if any(ord(char) < 32 for char in value):
        raise ValueError("invalid schedule partition")
    return "'" + value.replace("'", "''") + "'"


def optimize_sql(partitions):
    if not partitions or len(partitions) > MAX_PARTITIONS:
        raise ValueError("invalid compaction chunk")
    predicate = " OR ".join(
        f"(league = {literal(league)} AND season = {literal(season)})"
        for league, season in partitions
    )
    return (f"ALTER TABLE {TABLE} EXECUTE optimize(file_size_threshold => '128MB') "
            f"WHERE {predicate}")


def plan_chunks(rows):
    chunks, skipped = [], []
    chunk, files, size = [], 0, 0
    # Garbage-heavy partitions first; completed partitions disappear from the
    # next plan, so subsequent daily runs continue without a mutable cursor.
    for league, season, count, nbytes, deletes, small in sorted(
        rows, key=lambda row: (-row[4], str(row[0]), str(row[1]))
    ):
        literal(league)
        literal(season)
        if min(count, nbytes, deletes, small) < 0 or count == 0:
            raise ValueError("invalid file metrics")
        if not deletes and small < 2:
            continue
        partition = (league, season)
        if count > MAX_FILES or nbytes > MAX_BYTES:
            skipped.append({"partition": partition, "files": count, "bytes": nbytes,
                            "reason": "partition_over_limit"})
            continue
        if chunk and (len(chunk) == MAX_PARTITIONS or files + count > MAX_FILES
                      or size + nbytes > MAX_BYTES):
            chunks.append(chunk)
            chunk, files, size = [], 0, 0
        chunk.append(partition)
        files += count
        size += nbytes
    if chunk:
        chunks.append(chunk)
    return chunks, skipped


class Queries:
    """No client retries; absolute HTTP deadline plus server query lifetime.

    A lost POST response may hide the query ID. In that case keep the writer
    lock for the full server lifetime after the last possible submission.
    A confirmed HTTP DELETE from cursor.cancel() is sufficient cancellation.
    """
    def __init__(self):
        import requests
        import trino

        class DeadlineSession(requests.Session):
            deadline = 0.0

            def request(self, method, url, **kwargs):
                remaining = self.deadline - time.monotonic()
                if method.upper() != "DELETE" and remaining <= 0:
                    raise TimeoutError("compaction query deadline")
                kwargs["timeout"] = min(HTTP_SECONDS, max(0.001, remaining)) \
                    if method.upper() != "DELETE" else HTTP_SECONDS
                return super().request(method, url, **kwargs)

        self.session = DeadlineSession()
        password = os.environ.get("TRINO_PASSWORD")
        options = dict(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8443" if password else "8080")),
            user=os.environ.get("TRINO_USER", "airflow"), catalog="iceberg",
            source="sofascore:maintenance", max_attempts=1, encoding=None,
            http_session=self.session, request_timeout=HTTP_SECONDS,
            session_properties={"query_max_run_time": f"{QUERY_SECONDS}s",
                                "query_max_execution_time": f"{QUERY_SECONDS}s"},
        )
        if password:
            options.update(http_scheme="https", verify=False,
                           auth=trino.auth.BasicAuthentication(options["user"], password))
        self.connection = trino.dbapi.connect(**options)
        self.queries = []
        self.deadline = float("inf")

    def execute(self, sql):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("compaction run deadline")
        cursor = self.connection.cursor()
        self.session.deadline = min(self.deadline, time.monotonic() + QUERY_SECONDS)
        started = time.monotonic()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except BaseException:
            # Even a transport error can leave a live query. Never release the
            # writer lock merely because the client stopped receiving results.
            confirmed = False
            if cursor.query_id:
                try:
                    cursor.cancel()  # Trino client requires HTTP 204.
                    confirmed = cursor._query.cancelled or cursor._query.finished
                except Exception:
                    logger.exception("compaction cancellation unconfirmed")
            if not confirmed:
                time.sleep(QUERY_SECONDS + HTTP_SECONDS)
            raise
        finally:
            self.queries.append({"query_id": cursor.query_id,
                                 "seconds": time.monotonic() - started})
            # close() cancels again; cancellation above already handled the
            # error path, so do not mask its report with a second HTTP failure.
            if cursor._query is not None and cursor._query.finished:
                cursor.close()

    def close(self):
        self.connection.close()
        self.session.close()


def probe_refresh():
    import psycopg2
    from scrapers.sofascore.writer_lock import _writer_lock_dsn

    connection = psycopg2.connect(_writer_lock_dsn(os.environ), connect_timeout=10,
                                  options="-c statement_timeout=10000")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM dag_run WHERE dag_id = %s "
                "AND state IN ('running', 'queued')",
                ("dag_refresh_sofascore_all_mens",),
            )
            return bool(cursor.fetchone()[0])
    finally:
        connection.close()


def maintenance_lock():
    # Do not inherit a long writer timeout from the ingest environment.
    env = dict(os.environ, **{WRITER_LOCK_TIMEOUT_ENV: "30"})
    from psycopg2.extensions import make_dsn
    from scrapers.sofascore.writer_lock import WRITER_LOCK_DSN_ENV, _writer_lock_dsn

    env[WRITER_LOCK_DSN_ENV] = make_dsn(
        _writer_lock_dsn(env), connect_timeout=10, options="-c statement_timeout=10000"
    )
    return bronze_writer_lock(env)


def measure(queries):
    return {"snapshot": queries.execute(SNAPSHOT_SQL),
            "files": queries.execute(METRICS_SQL)}


def run(*, query_factory=Queries, writer_lock=maintenance_lock,
        refresh_probe=probe_refresh, now=utcnow, clock=time.monotonic):
    started = now()
    local = started.astimezone(MOSCOW)
    window_end = local.replace(hour=7, minute=0, second=0, microsecond=0)
    deadline = clock() + RUN_SECONDS
    result = {"started_at": started.isoformat(), "stopped": "complete",
              "processed": [], "skipped": [], "queries": []}
    queries = None

    def allowed():
        current = now().astimezone(MOSCOW)
        return (current.hour == 6
                and (window_end - current).total_seconds() >= CHUNK_RESERVE
                and deadline - clock() >= CHUNK_RESERVE)

    try:
        if not allowed():
            result["stopped"] = "out_of_window"
        elif refresh_probe():
            result["stopped"] = "refresh_running"
        else:
            queries = query_factory()
            queries.deadline = min(
                deadline, clock() + (window_end - now()).total_seconds()
                - QUERY_SECONDS - HTTP_SECONDS
            )
            result["before"] = measure(queries)
            chunks, result["skipped"] = plan_chunks(queries.execute(FILES_SQL))
            for chunk in chunks:
                if not allowed():
                    result["stopped"] = "deadline"
                    break
                with writer_lock() as acquired:
                    if not acquired:
                        raise RuntimeError("SofaScore writer lock is disabled")
                    if not allowed():
                        result["stopped"] = "deadline"
                        break
                    if refresh_probe():
                        result["stopped"] = "refresh_running"
                        break
                    # Re-read ALL file sizes, including delete files, under the
                    # lock. A growing partition must never bypass the ceiling.
                    rows = queries.execute(FILES_SQL)
                    selected = [row for row in rows if (row[0], row[1]) in chunk]
                    fresh, skipped = plan_chunks(selected)
                    result["skipped"].extend(skipped)
                    for portion in fresh:
                        if not allowed():
                            result["stopped"] = "deadline"
                            break
                        if refresh_probe():
                            result["stopped"] = "refresh_running"
                            break
                        before = clock()
                        queries.execute(optimize_sql(portion))
                        result["processed"].append({"partitions": portion,
                                                    "seconds": clock() - before})
                if result["stopped"] != "complete":
                    break
            result["after"] = measure(queries)
            remaining, skipped = plan_chunks(queries.execute(FILES_SQL))
            result["remaining_chunks"] = len(remaining)
            result["skipped"].extend(skipped)
            if result["stopped"] == "complete" and (remaining or result["skipped"]):
                result["stopped"] = "partial"
    except WriterLockTimeout:
        result["stopped"] = "lock_busy"
    except Exception as error:
        result["stopped"] = "error"
        result["error"] = f"{type(error).__name__}: {error}"
        logger.exception("SofaScore schedule compaction failed")
    finally:
        if queries is not None:
            result["queries"] = queries.queries
            try:
                queries.close()
            except Exception:
                result["stopped"] = "error"
                result["error"] = "compaction connection cleanup failed"
                logger.exception("compaction connection cleanup failed")
        result["finished_at"] = now().isoformat()
        result["seconds"] = (now() - started).total_seconds()
        logger.info("SofaScore schedule compaction: %s", json.dumps(result))
    return result


def main():
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(json.dumps(result))
    return int(result["stopped"] == "error")


if __name__ == "__main__":
    raise SystemExit(main())
