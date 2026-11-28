"""ClubElo daily Trino meter (#2132). A scan is one physical listener input
record, including repeats and failed queries. It is not an Iceberg ScanReport,
a row count or a byte count. Snapshot commits are measured independently.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import csv
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import http.client
import io
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import tempfile
from threading import Lock
from time import monotonic
from urllib.parse import urlsplit
import uuid


UTC = timezone.utc
DAY = timedelta(days=1)
MINUTE = timedelta(minutes=1)
RECEIPT_MINUTES = 3
RECEIVE_DB_TIMEOUT = 0.25
RETENTION = 8
LIMIT = 1440
TABLE = re.compile(r"clubelo_[a-z0-9_]+", re.ASCII)
QUERY_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,255}")
ACTORS = ("source", "monitor", "airflow", "foreign", "unknown")
LOSS_MARKER = re.compile(r"clubelo-loss-([0-9]{4}-[0-9]{2}-[0-9]{2})\."
                         r"(unknown_day|dropped_event|airflow_table_use|non_source_output|input_record_limit)")
PART = r'[a-z0-9_$]+'
REFERENCE = re.compile(rf'(?<![a-z0-9_$.])(?:(from|join|using)\s+)?((?:{PART}\s*\.\s*){{0,2}}clubelo_[a-z0-9_$]+)(?![a-z0-9_$])')
LITERALS = re.compile(r"'(?:''|[^'])*'|--[^\n]*|/\*.*?\*/", re.S)
PROBE = re.compile(r"select\s+(?:[01]|true|false|current_(?:user|catalog|schema|date|timestamp)(?:\s*\(\s*\))?|version\(\))(?:\s+as\s+\w+)?\s*;?")
META_TABLE = r"(?:(?:\w+\.)?information_schema\.\w+|(?:(?:iceberg\.)?bronze\.)?clubelo_\w+\$\w+)"
META_FROM = re.compile(rf"\bfrom\s+{META_TABLE}(?:\s+(?:as\s+)?\w+)?\s*(?=;?$|\b(?:where|group|order|limit)\b)")
WRITE = re.compile(r"\b(?:insert|merge|update|delete|create|replace|truncate|alter|drop)\b")
# TrinoTableManager stages a merge in ``<table>__stg_<12 hex>`` and drops it in
# the same write. A staging table is never a Bronze data table.
STAGING = re.compile(r"clubelo_[a-z0-9_]+__stg_[0-9a-f]{12}")
STAGED = r"(?:[a-z_][a-z0-9_]*\s*\.\s*){0,2}clubelo_[a-z0-9_]+__stg_[0-9a-f]{12}(?![a-z0-9_])"
STAGING_DDL = re.compile(rf"(?:drop\s+table\s+(?:if\s+exists\s+)?|insert\s+into\s+){STAGED}", re.S)
STAGING_CTAS = re.compile(rf"create\s+table\s+(?:if\s+not\s+exists\s+)?{STAGED}\s+as\s+select\b"
                          r".*\bwhere\s+false\s*;?\s*$", re.S)
# DESCRIBE reports table metadata and has no data-table I/O.
META_DESCRIBE = re.compile(r"(?:describe|show\s+columns\s+from|show\s+create\s+table)\s+"
                           r"(?:table\s+|from\s+)?[\w.$]+\s*;?")
# The platform's Iceberg maintenance is DDL by the shared account. It reports
# an output but expires snapshots and removes orphans without committing data.
MAINTENANCE = re.compile(r"alter\s+table\s+(?:[a-z_][a-z0-9_]*\s*\.\s*){0,2}clubelo_[a-z0-9_]+"
                         r"\s+execute\s+(?:optimize|expire_snapshots|remove_orphan_files)\b", re.S)


def now_utc():
    return datetime.now(UTC)


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("timestamp")
    value = value.strip()
    if value.endswith(("Z", "z")):
        value = value[:-1] + "+00:00"
    if not re.match(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}", value):
        raise ValueError("timestamp")
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def day_value(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("day")
    return date.fromisoformat(value)


def digest(data):
    return hashlib.sha256(data).hexdigest()


@contextmanager
def database(root, create=False, *, timeout=5):
    path = Path(root) / "clubelo.sqlite3"
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path.resolve().as_uri() + ("?mode=rwc" if create else "?mode=rw"),
                         uri=True, timeout=timeout)
    try:
        with db:
            yield db
    finally:
        db.close()


def identity(db):
    return db.execute("SELECT token FROM store WHERE id=1").fetchone()[0]


def mark(db, day, reason):
    db.execute("INSERT OR IGNORE INTO gaps VALUES (?,?)", (day.isoformat(), reason))


def remember(db, tables, day):
    db.executemany("""INSERT INTO inventory VALUES (?,?) ON CONFLICT(name)
                      DO UPDATE SET first_day=min(inventory.first_day,excluded.first_day)""",
                   ((name, day.isoformat()) for name in tables))


def unknown_day(db, received):
    # The completion day of a dropped body is unknowable.
    for offset in range(RETENTION + 1):
        mark(db, received.date() - offset * DAY, "dropped_event")


def record_loss(root, day, reasons):
    # The empty directory entry is the evidence. Exclusive creation coalesces
    # concurrent losses and never opens an existing target.
    for reason in reasons:
        path = Path(root) / f"clubelo-loss-{day.isoformat()}.{reason}"
        try:
            path.touch(mode=0o600, exist_ok=False)
        except FileExistsError:
            pass
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    # A prolonged writer lock can also prevent normal retention maintenance.
    # Cleanup is retried on the next loss or ordinary prune.
    try:
        prune_losses(root, now_utc().date() - RETENTION * DAY)
    except OSError:
        pass


def loss_markers(root):
    for name in os.listdir(root):
        match = LOSS_MARKER.fullmatch(name)
        if match:
            yield Path(root) / name, day_value(match[1]), match[2]


def prune_losses(root, cutoff):
    for path, marker_day, _ in loss_markers(root):
        if marker_day < cutoff:
            path.unlink(missing_ok=True)


def losses_for_day(root, day):
    reasons = set()
    for _, marker_day, reason in loss_markers(root):
        if reason == "unknown_day":
            if 0 <= (marker_day - day).days <= RETENTION:
                reasons.add("dropped_event")
        elif marker_day == day:
            reasons.add("dropped_event")
            reasons.add(reason)
    return reasons


def include_losses(report, losses):
    if losses:
        report["incomplete"] = sorted(set(report["incomplete"]) | {"dropped_event"})
        report["violations"] = sorted(set(report["violations"]) | (losses - {"dropped_event"}))
        report["verdict"] = "VIOLATION" if report["violations"] else "INCOMPLETE"


def event_violations(facts):
    violations = set()
    if facts["actor"] == "airflow" and facts["outputs"] and not facts.get("maintenance"):
        violations.add("airflow_table_use")
    if facts["outputs"] and facts["actor"] in ("monitor", "foreign"):
        violations.add("non_source_output")
    if any(n > LIMIT for n in facts["reads"].values()):
        violations.add("input_record_limit")
    return violations


def physical(entry):
    counts = Counter()
    data = bad = staged = False
    for item in entry:
        if not isinstance(item, dict):
            bad = True
            continue
        parts = (item.get("catalogName"), item.get("schema"), item.get("table", item.get("tableName")))
        if not all(isinstance(s, str) and s for s in parts):
            bad = True
            continue
        catalog, schema, table = (s.lower() for s in parts)
        if schema == "information_schema" or "$" in table:
            continue
        if STAGING.fullmatch(table):
            staged = True
            continue
        data = True
        if (catalog, schema) == ("iceberg", "bronze") and TABLE.fullmatch(table):
            counts[table] += 1
    return counts, data, bad, staged


def classify(event, monitor_user):
    """Return only physical names, occurrence counts and fixed categories."""
    meta = event.get("metadata")
    meta = meta if isinstance(meta, dict) else {}
    ctx = event.get("context")
    user = ctx.get("user") if isinstance(ctx, dict) else None
    own = user == "src_clubelo"
    sql = meta.get("query", event.get("query"))
    sql = sql.lower() if isinstance(sql, str) else ""
    io_meta = event.get("ioMetadata")
    io_meta = io_meta if isinstance(io_meta, dict) else {}
    inputs = io_meta.get("inputs")
    output = io_meta.get("output")
    reads, input_data, bad_in, staged_in = physical(inputs if isinstance(inputs, list) else [])
    writes, output_data, bad_out, staged_out = physical(output if isinstance(output, list) else [output] if output is not None else [])
    broken = bad_in or bad_out or (inputs is not None and not isinstance(inputs, list))
    if not (own or reads or writes or "clubelo_" in sql):
        if (broken or not meta.get("queryId") or not event.get("endTime")
                or (not sql and not inputs and output is None)):
            raise ValueError("unclassifiable")
        return None

    clean_sql = LITERALS.sub(" ", sql).replace('"', "").strip()
    if META_DESCRIBE.fullmatch(clean_sql) and not (reads or writes or broken):
        return None
    hints = set()
    read_hints = set()
    staged = staged_in or staged_out
    for match in REFERENCE.finditer(clean_sql):
        parts = re.split(r"\s*\.\s*", match[2])
        if parts[:-1] in ([], ["bronze"], ["iceberg", "bronze"]) and TABLE.fullmatch(parts[-1]):
            if STAGING.fullmatch(parts[-1]):
                staged = True
                continue
            hints.add(parts[-1])
            if match[1]:
                read_hints.add(parts[-1])
    if not (reads or writes or hints):
        if broken:
            raise ValueError("malformed IO")
        if not own or PROBE.fullmatch(clean_sql):
            return None
        # Known metadata-only operations have no data-table I/O. A staging
        # table is not one: a statement that also touches another data table
        # is classified like any other.
        if not (input_data or output_data or broken) and (
            staged or (inputs and not bad_in)
            or re.fullmatch(r"show\s+(catalogs|schemas|tables)(\s+(from|in)\s+[\w.]+)?\s*;?", clean_sql)
            or (re.match(r"select\b", clean_sql) and META_FROM.search(clean_sql)
                and len(re.findall(r"\b(?:from|join|select|union)\b", clean_sql)) == 2)
        ):
            return None
    # An empty staging copy reads no split of the table it copies.
    if staged and not reads and STAGING_CTAS.fullmatch(clean_sql):
        return None

    if own:
        actor = "source"
    elif user == "airflow":
        actor = "airflow"
    elif monitor_user and user == monitor_user:
        actor = "monitor"
    elif isinstance(user, str) and user.strip():
        actor = "foreign"
    else:
        actor = "unknown"
    stats = event.get("statistics")
    partial = meta.get("complete") is False or (isinstance(stats, dict) and stats.get("complete") is False)
    success = meta.get("queryState") == "FINISHED" and event.get("failureInfo") is None and not partial
    uncertain = (broken or not (reads or writes) or bool(hints.difference(reads, writes))
                 or bool(read_hints.difference(reads))
                 or (not writes and bool(WRITE.search(clean_sql)) and bool(hints))
                 or not success)
    return {"actor": actor, "reads": reads, "outputs": sorted(writes), "finished": success,
            "uncertain": uncertain, "maintenance": MAINTENANCE.match(clean_sql) is not None}


def initialize(root):
    with database(root, create=True) as db:
        db.executescript("""
                CREATE TABLE IF NOT EXISTS store(id INTEGER PRIMARY KEY, token TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, day TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, facts TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS events_day ON events(day);
                CREATE TABLE IF NOT EXISTS minutes(start INTEGER PRIMARY KEY, ok INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS gaps(day TEXT, reason TEXT, PRIMARY KEY(day,reason));
                CREATE TABLE IF NOT EXISTS reports(day TEXT PRIMARY KEY, revision TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS inventory(name TEXT PRIMARY KEY, first_day TEXT NOT NULL);
        """)
        # A restarting receiver can attach to an initialized store while
        # another connection holds its writer reservation.
        if db.execute("SELECT 1 FROM store WHERE id=1").fetchone() is None:
            db.execute("INSERT OR IGNORE INTO store VALUES (1,?)", (uuid.uuid4().hex,))
        return identity(db)


class Sink:
    def __init__(self, root, monitor_user=None):
        if monitor_user in ("airflow", "src_clubelo"):
            raise ValueError("shared or source monitor identity")
        self.root = Path(root)
        self.monitor_user = monitor_user
        self.token = initialize(root)
        self._lock = Lock()
        self.ready = True
        self.pruned = None
        self.activate()

    def activate(self):
        with self._lock:
            self.started = now_utc()
            self.tick = monotonic()
            self._minutes = set()

    def disable(self):
        with self._lock:
            self.ready = False
            self._minutes.clear()

    def health(self):
        with self._lock:
            now = now_utc()
            latest = int(now.replace(second=0, microsecond=0).timestamp())
            return {"enabled": True, "ready": self.ready, "store": self.token,
                    "started": self.started.isoformat(), "checked": now.isoformat(),
                    "uptime": monotonic() - self.tick,
                    "event_minutes": sorted(m for m in self._minutes if latest - 60 * RECEIPT_MINUTES < m <= latest)}

    def _observe(self, event, received, end, facts):
        # Delayed deliveries cannot fill an earlier minute. Keep only minute
        # starts, even for the independent discovery traffic.
        minute = received.replace(second=0, microsecond=0)
        if end > received or end.replace(second=0, microsecond=0) != minute:
            return
        stamp = int(minute.timestamp())
        with self._lock:
            if not self.ready or end < self.started or stamp in self._minutes:
                return
        meta = event.get("metadata")
        ctx = event.get("context")
        io_meta = event.get("ioMetadata")
        stats = event.get("statistics", {})
        if (not isinstance(meta, dict) or not isinstance(meta.get("queryId"), str)
                or not QUERY_ID.fullmatch(meta["queryId"])
                or not isinstance(meta.get("query"), str) or not meta["query"].strip()
                or meta.get("queryState") not in ("FINISHED", "FAILED")
                or (meta["queryState"] == "FINISHED" and event.get("failureInfo") is not None)
                or (meta["queryState"] == "FAILED" and not isinstance(event.get("failureInfo"), dict))
                or meta.get("complete", True) is not True or not isinstance(stats, dict)
                or stats.get("complete", True) is not True
                or not isinstance(ctx, dict) or not isinstance(ctx.get("user"), str) or not ctx["user"].strip()
                or not isinstance(io_meta, dict) or not isinstance(io_meta.get("inputs"), list)):
            return
        # Classification may retain partial physical counts. They cannot
        # certify delivery of a well-formed completion.
        if facts and facts["uncertain"]:
            output = io_meta.get("output")
            outputs = output if isinstance(output, list) else [output] if output is not None else []
            if physical(io_meta["inputs"])[2] or physical(outputs)[2]:
                return
        with self._lock:
            cutoff = max(stamp, max(self._minutes, default=stamp)) - 60 * RECEIPT_MINUTES
            if self.ready and end >= self.started and stamp > cutoff and stamp not in self._minutes:
                self._minutes = {m for m in self._minutes if m > cutoff}
                self._minutes.add(stamp)

    def gap(self, received_at):
        received = timestamp(received_at)
        try:
            with database(self.root, timeout=RECEIVE_DB_TIMEOUT) as db:
                unknown_day(db, received)
        except sqlite3.Error:
            record_loss(self.root, received.date(), ("unknown_day",))

    def capture(self, event, received_at):
        if event is None:
            self.gap(received_at)
            return
        facts = classify(event, self.monitor_user)
        received = timestamp(received_at)
        end = timestamp(event.get("endTime"))
        day = end.date()
        if day < received.date() - RETENTION * DAY:
            return
        if end >= received + timedelta(seconds=1):
            raise ValueError("future completion")
        if facts is None:
            self._observe(event, received, end, facts)
            return
        try:
            self._store_event(event, received, end, facts)
        except sqlite3.Error:
            record_loss(self.root, day, event_violations(facts) or {"dropped_event"})

    def _store_event(self, event, received, end, facts):
        day = end.date()
        meta = event.get("metadata")
        query_id = meta.get("queryId") if isinstance(meta, dict) else None
        with database(self.root, timeout=RECEIVE_DB_TIMEOUT) as db:
            db.execute("BEGIN IMMEDIATE")
            if self.token != identity(db):
                raise ValueError("store changed")
            if self.pruned != received.date():
                prune(self.root, db, received.date())
                self.pruned = received.date()
            remember(db, set(facts["reads"]).union(facts["outputs"]), day)
            if not isinstance(query_id, str) or not QUERY_ID.fullmatch(query_id):
                mark(db, day, "missing_query_id")
                for reason in event_violations(facts):
                    mark(db, day, reason)
                return
            key = digest(query_id.encode())
            fingerprint = digest(json.dumps(event, sort_keys=True, allow_nan=False, separators=(",", ":")).encode())
            existing = db.execute("SELECT day,fingerprint,facts FROM events WHERE id=?", (key,)).fetchone()
            record = (day.isoformat(), fingerprint, json.dumps(facts, sort_keys=True))
            if existing is None:
                db.execute("INSERT INTO events VALUES (?,?,?,?)", (key, *record))
            elif existing != record:
                mark(db, day, "conflicting_query_id")
                mark(db, day_value(existing[0]), "conflicting_query_id")
                for reason in event_violations(facts):
                    mark(db, day, reason)
                return
        self._observe(event, received, end, facts)


def receipt(url):
    address = urlsplit(url)
    if (address.scheme != "http" or address.path != "/clubelo-healthz" or "?" in url or "#" in url
            or not re.fullmatch(r"(?:\[[0-9a-fA-F:.]+\]|[a-zA-Z0-9.]+)(?::[0-9]+)?", address.netloc)
            or any(ord(c) <= 32 or ord(c) == 127 for c in url)):
        raise ValueError("local health URL")
    port = address.port if address.port is not None else 80
    host = ipaddress.ip_address("127.0.0.1" if address.hostname == "localhost" else address.hostname)
    effective = getattr(host, "ipv4_mapped", None) or host
    if (not port or effective.is_unspecified or effective.is_multicast or effective.is_link_local
            or str(effective) == "255.255.255.255"):
        raise ValueError("local health URL")
    if not effective.is_loopback:
        family = socket.AF_INET6 if host.version == 6 else socket.AF_INET
        try:
            with socket.socket(family, socket.SOCK_STREAM) as probe:
                probe.bind((str(host), 0))
        except OSError as exc:
            raise ValueError("local health URL") from exc
    try:
        return _read_receipt(str(host), port, address.path)
    except ConnectionRefusedError:
        if address.hostname != "localhost":
            raise
        return _read_receipt("::1", port, address.path)


def _read_receipt(host, port, path):
    conn = http.client.HTTPConnection(host, port, timeout=2)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read(4097)
        if response.status != 200 or len(body) > 4096:
            raise ValueError("health response")
        return json.loads(body)
    finally:
        conn.close()


def prune(root, db, today):
    cutoff = today - RETENTION * DAY
    for table in ("events", "gaps", "reports"):
        db.execute(f"DELETE FROM {table} WHERE day < ?", (cutoff.isoformat(),))
    db.execute("DELETE FROM minutes WHERE start < ?", (midnight(cutoff),))
    prune_losses(root, cutoff)
    for path in (Path(root) / "daily").glob("????-??-??.json"):
        if path.stem < cutoff.isoformat():
            path.unlink(missing_ok=True)


def midnight(day):
    return int(datetime.combine(day, time.min, UTC).timestamp())


def heartbeat(root, end, status, url):
    stop = end.astimezone(UTC).replace(second=0, microsecond=0)
    start = stop - MINUTE
    if stop > now_utc() or start.date() < now_utc().date() - RETENTION * DAY:
        raise ValueError("heartbeat time")
    with database(root) as db:
        ok = False
        if status == "ok":
            try:
                health = receipt(url)
                now = now_utc()
                checked = timestamp(health["checked"])
                minutes = health["event_minutes"]
                latest = int(checked.replace(second=0, microsecond=0).timestamp())
                uptime = health["uptime"]
                ok = (health["enabled"] is True and health["ready"] is True
                      and health["store"] == identity(db) and timestamp(health["started"]) <= start
                      and stop <= checked <= now < stop + MINUTE
                      and 0 <= (now - checked).total_seconds() <= 5
                      and type(uptime) in (int, float) and math.isfinite(uptime)
                      and uptime >= (checked - start).total_seconds()
                      and isinstance(minutes, list) and 0 < len(minutes) <= RECEIPT_MINUTES
                      and all(type(m) is int and m % 60 == 0 and latest - 60 * RECEIPT_MINUTES < m <= latest
                              for m in minutes)
                      and int(start.timestamp()) in minutes)
            except (OSError, http.client.HTTPException, KeyError, TypeError, ValueError, OverflowError):
                pass
        db.execute("""INSERT INTO minutes VALUES (?,?) ON CONFLICT(start)
                      DO UPDATE SET ok=min(minutes.ok,excluded.ok)""", (int(start.timestamp()), int(ok)))
        prune(root, db, now_utc().date())
    if not ok:
        raise ValueError("unproven minute")


DISCOVER = """SELECT table_name FROM iceberg.information_schema.tables
WHERE table_catalog = 'iceberg' AND table_schema = 'bronze' AND table_type = 'BASE TABLE'
AND regexp_like(table_name, '^clubelo_[a-z0-9_]+$') ORDER BY table_name LIMIT 257"""


def rows(client, sql):
    result = subprocess.run([str(client), sql], check=True, capture_output=True,
                            text=True, encoding="utf-8", timeout=30)
    return list(csv.reader(io.StringIO(result.stdout), strict=True))


def discovery(client):
    names = []
    for row in rows(client, DISCOVER):
        if len(row) != 1 or not TABLE.fullmatch(row[0]):
            raise ValueError("discovery row")
        names.append(row[0])
    if not 0 < len(names) <= 256 or len(names) != len(set(names)):
        raise ValueError("discovery set")
    return sorted(names)


def empty_evidence():
    return {"snapshots": 0, "trino": 0, "catalog": 0, "unmatched": 0, "unstamped": 0,
            "other_engine": 0, "users": {}, "query_ids": []}


def snapshots(client, day, names):
    """Snapshot rows of one UTC day, tagged by the engine that committed them.

    Trino stamps ``trino_user`` and ``trino_query_id``. The platform's bulk
    writer reaches the catalog directly and leaves no principal. Its
    identity-partition replace is an unstamped delete followed by an append
    stamped ``dpf.operation``; ClubElo stamps its plain bulk appends with
    ``dpf.source`` (ClubEloIcebergWriter.SNAPSHOT_STAMP). Any other commit
    without an engine belongs to a writer the source cannot own.
    """
    selects = []
    for name in names:
        if not TABLE.fullmatch(name):
            raise ValueError("table")
        selects.append(f"""SELECT '{name}', operation, element_at(summary, 'engine-name'),
element_at(summary, 'trino_user'), element_at(summary, 'trino_query_id'), element_at(summary, 'dpf.operation'),
element_at(summary, 'dpf.source')
FROM iceberg.bronze."{name}$snapshots"
WHERE committed_at >= TIMESTAMP '{day.isoformat()} 00:00:00 UTC'
AND committed_at < TIMESTAMP '{(day + DAY).isoformat()} 00:00:00 UTC'""")
    # The monitor reports an empty result as a failure; a quiet day still needs evidence.
    selects.append("SELECT '', '', '', '', '', '', ''")
    evidence = {name: empty_evidence() for name in names}
    for row in rows(client, " UNION ALL ".join(selects)):
        if len(row) != 7:
            raise ValueError("snapshot row")
        if row == [""] * 7:
            continue
        name, operation, engine, user, query_id, stamped, source = row
        info = evidence.get(name)
        if info is None or not re.fullmatch(r"[a-z_]+", operation) or info["snapshots"] >= LIMIT * 64:
            raise ValueError("snapshot row")
        info["snapshots"] += 1
        if engine.lower() == "trino":
            info["trino"] += 1
            if QUERY_ID.fullmatch(query_id or ""):
                info["users"][user or ""] = info["users"].get(user or "", 0) + 1
                info["query_ids"].append(query_id)
            else:
                info["unmatched"] += 1   # the query id is missing evidence
        elif not engine:
            # A replace batch (unstamped delete, stamped append) or a stamped
            # ClubElo bulk append. Everything else is not the source's output.
            if operation == "delete" or (operation == "append" and (
                    stamped == "replace-identity-partition-batch" or source == "clubelo")):
                info["catalog"] += 1
            else:
                info["unstamped"] += 1
        else:
            info["other_engine"] += 1
    return evidence


def valid_evidence(info):
    counters = ("snapshots", "trino", "catalog", "unmatched", "unstamped", "other_engine")
    return (isinstance(info, dict) and set(info) == set(empty_evidence())
            and all(type(info[key]) is int and info[key] >= 0 for key in counters)
            and isinstance(info["users"], dict)
            and all(isinstance(key, str) and type(value) is int and value >= 0
                    for key, value in info["users"].items())
            and isinstance(info["query_ids"], list)
            and all(isinstance(value, str) and QUERY_ID.fullmatch(value) for value in info["query_ids"]))


def empty_metrics():
    return {"input_records": dict.fromkeys(ACTORS, 0),
            "output_queries": dict.fromkeys(ACTORS, 0),
            "successful_outputs": dict.fromkeys(ACTORS, 0), "input_records_total": 0,
            "snapshots": None, "trino_snapshots": None, "catalog_snapshots": None}


def summarize(db, day, evidence):
    day_text = day.isoformat()
    reasons = {r[0] for r in db.execute("SELECT reason FROM gaps WHERE day=?", (day_text,))}
    violations = reasons.intersection({"airflow_table_use", "non_source_output", "input_record_limit"})
    reasons.difference_update(violations)
    if reasons - {"dropped_event", "missing_query_id", "conflicting_query_id"}:
        raise ValueError("unknown gap")
    minutes = db.execute("SELECT count(*) FROM minutes WHERE ok=1 AND start>=? AND start<? AND start%60=0",
                         (midnight(day), midnight(day + DAY))).fetchone()[0]
    if minutes != 1440:
        reasons.add("minute_coverage")
    tables = {name: empty_metrics() for name, in db.execute(
        "SELECT name FROM inventory WHERE first_day<=? ORDER BY name", (day_text,)) if not STAGING.fullmatch(name)}
    source_activity = 0
    written = {}
    for key, facts_text in db.execute("SELECT id,facts FROM events WHERE day=?", (day_text,)):
        facts = json.loads(facts_text)
        actor = facts["actor"]
        reads, outputs = facts["reads"], [name for name in facts["outputs"] if not STAGING.fullmatch(name)]
        if actor == "source" and (reads or (facts["finished"] and outputs)):
            source_activity += 1
        violations.update(event_violations({**facts, "outputs": outputs}))
        if actor == "airflow" and facts.get("maintenance") is True:
            pass  # the platform's own DDL maintenance of Bronze tables
        elif actor == "airflow" and reads and not outputs:
            reasons.add("unattributed_airflow_read")
        elif actor in ("airflow", "foreign", "unknown"):
            reasons.add(actor + "_use")
        if facts["uncertain"]:
            reasons.add("uncertain_event")
        for name, count in reads.items():
            if STAGING.fullmatch(name):
                continue
            tables[name]["input_records"][actor] += count
            tables[name]["input_records_total"] += count
        for name in outputs:
            tables[name]["output_queries"][actor] += 1
            if facts["finished"]:
                tables[name]["successful_outputs"][actor] += 1
                written.setdefault(name, []).append((key, facts.get("maintenance") is True))
    if not source_activity:
        reasons.add("no_source_activity")
    committed_evidence = evidence["tables"]
    if not evidence["stable"] or not committed_evidence:
        reasons.add("snapshot_evidence")
    for name, metrics in tables.items():
        info = committed_evidence.get(name)
        count = None if info is None else info["snapshots"]
        metrics["snapshots"] = count
        if info is None:
            reasons.add("missing_table_evidence")
            continue
        metrics["trino_snapshots"], metrics["catalog_snapshots"] = info["trino"], info["catalog"]
        # Every Trino commit needs the stored successful output of the same
        # query on the same table, and every successful data write its
        # committed snapshot. Procedures such as expire_snapshots report an
        # output without committing one.
        committed = {digest(value.encode()) for value in info["query_ids"]}
        outputs = written.get(name, [])
        if (not committed <= {key for key, _ in outputs}
                or any(not procedure and key not in committed for key, procedure in outputs)
                or info["unmatched"]):
            reasons.add("unattributed_snapshot_or_output")
        # A commit without a known writer's stamp, by another engine or by
        # another principal, is an output the source cannot own.
        if info["unstamped"] or info["other_engine"] or set(info["users"]) - {"src_clubelo", "airflow"}:
            violations.add("non_source_output")
        if metrics["input_records_total"] > LIMIT:
            violations.add("input_record_limit")
        if count > LIMIT:
            violations.add("snapshot_limit")
    return {"day": day_text, "verdict": "VIOLATION" if violations else "INCOMPLETE" if reasons else "PASS",
            "violations": sorted(violations), "incomplete": sorted(reasons),
            "covered_minutes": minutes, "source_queries": source_activity, "tables": tables,
            "scan_metric": "Trino QueryCompleted physical input records, including repeats and failed queries. "
                           "Iceberg ScanReports, rows and bytes require separate instrumentation.",
            "snapshot_metric": "Iceberg snapshots by committed_at. A Trino commit is matched to its successful "
                               "output query by trino_query_id. Catalog commits carry no principal: ClubElo's "
                               "bulk appends are recognized by their dpf.source stamp, a replace batch by "
                               "dpf.operation and its unstamped delete half."}


def write_report(root, day, report):
    directory = Path(root) / "daily"
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as tmp:
        path = Path(tmp.name)
        try:
            json.dump(report, tmp, sort_keys=True, allow_nan=False)
            tmp.write("\n")
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            os.replace(path, directory / (day.isoformat() + ".json"))
        finally:
            path.unlink(missing_ok=True)


def aggregate(root, day, client):
    today = now_utc().date()
    if not today - RETENTION * DAY <= day < today:
        raise ValueError("expected retained completed UTC day")
    initialize(root)
    revision = uuid.uuid4().hex
    with database(root) as db:
        # An interrupted refresh must invalidate the old JSON even when writing
        # the replacement fails. A concurrent refresh owns its own revision.
        db.execute("INSERT OR REPLACE INTO reports VALUES (?,?)", (day.isoformat(), revision))
    names = []
    evidence = {"stable": False, "tables": {}}
    try:
        names = discovery(client)
        evidence["tables"] = snapshots(client, day, names)
        evidence["stable"] = discovery(client) == names
    except (OSError, ValueError, csv.Error, subprocess.SubprocessError):
        pass
    with database(root) as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT revision FROM reports WHERE day=?", (day.isoformat(),)).fetchone() != (revision,):
            raise ValueError("superseded refresh")
        prune(root, db, today)
        remember(db, names, day)
        report = summarize(db, day, evidence)
        include_losses(report, losses_for_day(root, day))
        report.update(store=identity(db), revision=revision, generated_at=now_utc().isoformat(), snapshot_evidence=evidence)
        write_report(root, day, report)
    return report


def render(root, requested=None):
    now = now_utc()
    day = now.date() - DAY
    prefix = f"• Trino clubelo {day.isoformat()}"
    try:
        if requested is not None and day_value(requested) != day:
            raise ValueError("wrong morning day")
        with (Path(root) / "daily" / (day.isoformat() + ".json")).open("rb") as file:
            raw = file.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("large report")
        report = json.loads(raw)
        generated = timestamp(report["generated_at"])
        if generated.date() != now.date() or generated > now:
            raise ValueError("report time")
        evidence = report["snapshot_evidence"]
        if (type(evidence["stable"]) is not bool or not isinstance(evidence["tables"], dict)
                or any(not TABLE.fullmatch(k) or not valid_evidence(v)
                       for k, v in evidence["tables"].items())):
            raise ValueError("snapshot evidence")
        with database(root) as db:
            db.execute("BEGIN")
            if report["store"] != identity(db):
                raise ValueError("report store")
            if db.execute("SELECT revision FROM reports WHERE day=?", (day.isoformat(),)).fetchone() != (report["revision"],):
                raise ValueError("report revision")
            expected = summarize(db, day, evidence)
        # A new marker can postdate this revision. Apply the same durable
        # evidence to both views before checking the other published metrics.
        losses = losses_for_day(root, day)
        include_losses(expected, losses)
        include_losses(report, losses)
        if (any(report[k] != v for k, v in expected.items())
                or evidence["tables"].keys() - expected["tables"].keys()):
            # Even a stale snapshot report cannot invalidate a violation
            # independently established by the listener or its loss markers.
            proven = set(expected["violations"]) - {"snapshot_limit"}
            if losses and proven:
                return f"{prefix} VIOLATION {min(proven)} minutes={expected['covered_minutes']}"
            raise ValueError("stale report")
        if expected["verdict"] == "PASS":
            scans = sum(t["input_records_total"] for t in expected["tables"].values())
            commits = sum(t["snapshots"] for t in expected["tables"].values())
            return f"{prefix} PASS scans={scans} snapshots={commits} minutes=1440"
        reason = (expected["violations"] or expected["incomplete"])[0]
        return f"{prefix} {expected['verdict']} {reason} minutes={expected['covered_minutes']}"
    except (OSError, ValueError, TypeError, KeyError, IndexError, OverflowError, RecursionError, sqlite3.Error):
        return f"{prefix} INCOMPLETE report_unavailable"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    daily = commands.add_parser("aggregate")
    daily.add_argument("--monitor", required=True, type=Path)
    morning = commands.add_parser("render")
    for cmd in (daily, morning):
        cmd.add_argument("--root", required=True, type=Path)
        cmd.add_argument("--date")
    args = parser.parse_args(argv)
    if args.command == "render":
        print(render(args.root, args.date))
        return 0
    try:
        day = day_value(args.date) if args.date is not None else now_utc().date() - DAY
        report = aggregate(args.root, day, args.monitor)
        print(report["day"], report["verdict"])
        return 0
    except (ValueError, OSError, sqlite3.Error, OverflowError) as exc:
        print(f"clubelo_meter: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
