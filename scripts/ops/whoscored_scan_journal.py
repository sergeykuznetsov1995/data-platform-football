"""Validate a continuous segment of the WhoScored physical scan journal against Trino's console."""

import argparse
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
from uuid import UUID


SECOND = 1_000_000_000
MINUTE = 60 * SECOND
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ISO = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)")
TABLE = re.compile(r'(bronze|ops)\.(?:"(whoscored_[a-z0-9_]+)"|(whoscored_[a-z0-9_]+))')
COLOR = re.compile(r"\x1b\[[0-9;]*m")
START_LOG = "org.apache.iceberg.SnapshotScan"
HEALTH_LOG = "football.whoscored.scan.health"
START = re.compile(r"Scanning table ([^ ]+) snapshot (\d+) created at ")
CLOCK_GAP = 45 * SECOND


def nanos(value):
    match = ISO.fullmatch(value)
    if not match:
        raise ValueError(
            "expected an ISO timestamp with timezone and at most nine fractional digits"
        )
    stamp, fraction, offset = match.groups()
    elapsed = (
        datetime.fromisoformat(stamp + ("+00:00" if offset == "Z" else offset)) - EPOCH
    )
    return (elapsed.days * 86400 + elapsed.seconds) * SECOND + int(
        (fraction or "").ljust(9, "0")
    )


def integer(value):
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    if type(value) is int and value >= 0:
        return value
    raise ValueError("invalid nonnegative counter")


def source_table(raw):
    match = TABLE.fullmatch(raw)
    if match:
        schema, quoted, plain = match.groups()
        return f"{schema}.{quoted or plain}"
    if "whoscored_" in raw:
        raise ValueError("unrecognized WhoScored table spelling or namespace")
    return None


def lines(stream):
    for line in stream:
        if not line.endswith("\n"):
            raise ValueError("incomplete line in evidence")
        yield line[:-1]


@dataclass(frozen=True)
class Health:
    generation: str
    at: str
    beat: int
    produced: int
    written: int
    dropped: int
    invalid: int
    errors: int
    pending: int
    active: bool

    @classmethod
    def parse(cls, message):
        tag, generation, at, *fields, active = message.split("\t")
        if (
            tag != "WS_SCAN_HEALTH1"
            or str(UUID(generation)) != generation
            or active not in {"true", "false"}
        ):
            raise ValueError("invalid journal health record")
        nanos(at)
        return cls(generation, at, *(integer(f) for f in fields), active == "true")


def select_console(stream):
    """Read plain 'docker logs CONTAINER_ID' from one container. Never retain filters or other logs."""
    for line in lines(stream):
        if START_LOG not in line and HEALTH_LOG not in line:
            continue
        if line.startswith("{"):
            row = json.loads(line)
            at, level, logger, msg = (
                row[k] for k in ("timestamp", "level", "logger", "message")
            )
        else:
            fields = COLOR.sub("", line).split("\t", 4)
            if len(fields) != 5:
                continue
            at, level, _, logger, msg = fields
        if logger == HEALTH_LOG:
            if level != "INFO":
                raise ValueError("unexpected journal health log level")
            yield {"kind": "health", **asdict(Health.parse(msg))}
        elif logger == START_LOG and level == "INFO":
            match = START.match(msg)
            if match:
                name, snapshot = match.groups()
                table = source_table(name)
                if table is not None:
                    nanos(at)
                    yield {
                        "kind": "start",
                        "at": at,
                        "table": table,
                        "snapshot": int(snapshot),
                    }
            elif "whoscored_" in msg and not msg.startswith("Scanning empty table "):
                raise ValueError("unrecognized WhoScored planning record")


@dataclass(frozen=True)
class Scan:
    gen: str
    seq: int
    at: str
    table: str
    snapshot: int
    planning_ns: int

    @classmethod
    def parse(cls, row):
        tag, gen, seq, at, name, snapshot, duration = row.split("\t")
        if tag != "SCAN1" or str(UUID(gen)) != gen or source_table(name) != name:
            raise ValueError("invalid native scan record")
        nanos(at)
        return cls(gen, integer(seq), at, name, integer(snapshot), integer(duration))


def valid_marks(checkpoints, start, end):
    idle = [i for i, m in enumerate(checkpoints) if m.active and m.pending == 0]
    if len(idle) < 2:
        raise ValueError("missing drained checkpoints that enclose the interval")
    marks = checkpoints[idle[0] : idle[-1] + 1]
    first, last = marks[0], marks[-1]
    if nanos(first.at) > start - MINUTE or nanos(last.at) <= end:
        raise ValueError(
            "coverage must include the preceding minute and a checkpoint after the end"
        )
    for mark in marks:
        if not mark.active or mark.generation != first.generation:
            raise ValueError("reporter disabled or coordinator generation changed")
        if (mark.dropped, mark.invalid, mark.errors) != (
            first.dropped,
            first.invalid,
            first.errors,
        ):
            raise ValueError("producer reports loss, malformed data or a write error")
        if mark.written + mark.dropped + mark.pending > mark.produced:
            raise ValueError("inconsistent producer counters")
    for old, new in zip(marks, marks[1:]):
        gap = nanos(new.at) - nanos(old.at)
        if new.beat != old.beat + 1 or not 0 < gap <= CLOCK_GAP:
            raise ValueError("missing heartbeat or discontinuous clock")
        if new.produced < old.produced or new.written < old.written:
            raise ValueError("producer counters moved backwards")
    if last.written - first.written != last.produced - first.produced:
        raise ValueError("producer and persistent journal counts differ")
    return first, last


def qualify(start, end, inventory, console, journal):
    begin, stop = nanos(start), nanos(end)
    if stop <= begin:
        raise ValueError("interval end must follow its start")
    if (
        not isinstance(inventory, list)
        or not inventory
        or len(set(inventory)) != len(inventory)
    ):
        raise ValueError("expected a unique, nonempty inventory")
    if any(source_table(t) != t for t in inventory):
        raise ValueError("invalid inventory")
    marks, starts = [], []
    for entry in console:
        kind = entry["kind"]
        if kind == "health":
            if type(entry["active"]) is not bool:
                raise ValueError("invalid health state")
            mark = Health(**{k: v for k, v in entry.items() if k != "kind"})
            if str(UUID(mark.generation)) != mark.generation:
                raise ValueError("invalid coordinator generation")
            nanos(mark.at)
            for counter in (
                mark.beat,
                mark.produced,
                mark.written,
                mark.pending,
                mark.dropped,
                mark.invalid,
                mark.errors,
            ):
                if type(counter) is not int or counter < 0:
                    raise ValueError("invalid producer counters")
            marks.append(mark)
        elif kind == "start":
            starts.append(
                (nanos(entry["at"]), entry["table"], integer(entry["snapshot"]))
            )
        else:
            raise ValueError("unknown console evidence")
    first, last = valid_marks(marks, begin, stop)
    left, right = nanos(first.at), nanos(last.at)
    by_seq = {}
    for row in journal:
        if row.gen != first.generation:
            continue
        event_time = nanos(row.at)
        if first.produced < row.seq <= last.produced:
            if not left <= event_time < right or row.table not in inventory:
                raise ValueError("scan outside the bracket or table inventory")
            if row.seq in by_seq and by_seq[row.seq] != row:
                raise ValueError("conflicting replay of a physical scan")
            by_seq[row.seq] = row
        elif begin - MINUTE <= event_time < stop:
            raise ValueError("scan timestamp crosses its journal checkpoints")
    if len(by_seq) != last.produced - first.produced:
        raise ValueError("missing physical scan sequence")

    # One independent INFO record precedes the real ScanReport for each nonempty
    # SnapshotScan. Keep both sides of concurrent/repeated scans; never assign a SQL ID.
    planning = []
    for when, table, snapshot in starts:
        if left <= when < right:
            if table not in inventory:
                raise ValueError("planning event outside table inventory")
            planning.append((when, 0, table, snapshot))
    planning.extend((nanos(r.at), 1, r.table, r.snapshot) for r in by_seq.values())
    open_scans = Counter()
    for _, is_finish, table, snapshot in sorted(planning):
        key = table, snapshot
        open_scans[key] += -1 if is_finish else 1
        if open_scans[key] < 0:
            raise ValueError(
                "missing planning start or a scan crosses the evidence bracket"
            )
    if any(open_scans.values()):
        raise ValueError(
            "open planning events or missing completed scans; extend the bracket"
        )

    totals = {name: {"scans": 0, "peak_60s": 0, "peak_at": None} for name in inventory}
    recent = defaultdict(deque)
    events = []
    for r in sorted(by_seq.values(), key=lambda s: (nanos(s.at), s.seq)):
        when = nanos(r.at)
        if not begin - MINUTE <= when < stop:
            continue
        times = recent[r.table]
        while times and times[0] <= when - MINUTE:
            times.popleft()
        times.append(when)
        events.append(
            {
                "kind": "scan",
                "id": f"{r.gen}/{r.seq}",
                "at": r.at,
                "at_ns": when,
                "table": r.table,
            }
        )
        counts = totals[r.table]
        if begin - MINUTE < when < begin:
            # Even with no in-day events the leading window can attain the day's peak.
            counts.update(peak_60s=counts["peak_60s"] + 1, peak_at=start)
        elif when >= begin:
            counts["scans"] += 1
            if len(times) > counts["peak_60s"]:
                counts.update(peak_60s=len(times), peak_at=r.at)
    return {
        "start": start,
        "end": end,
        "generation": first.generation,
        "tables": totals,
        "events": events,
        "first_heartbeat": first.at,
        "last_heartbeat": last.at,
        "checked_scan_count": len(by_seq),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    modes.add_parser(
        "select-console",
        help="write only independent starts and producer health as JSONL",
    )
    verify = modes.add_parser(
        "verify", help="verify one continuous capture and calculate exact scan peaks"
    )
    for field in ("start", "end", "tables", "console"):
        verify.add_argument("--" + field, required=True)
    verify.add_argument("journal", type=Path, nargs="+")
    args = parser.parse_args()
    try:
        if args.mode == "select-console":
            for row in select_console(sys.stdin):
                print(json.dumps(row))
        else:

            def scans():
                for path in args.journal:
                    with path.open() as stream:
                        yield from (Scan.parse(line) for line in lines(stream))

            with Path(args.console).open() as stream:
                report = qualify(
                    args.start,
                    args.end,
                    json.loads(Path(args.tables).read_text()),
                    map(json.loads, lines(stream)),
                    scans(),
                )
            print(json.dumps(report, separators=(",", ":")))
    except (
        AttributeError,
        KeyError,
        OSError,
        OverflowError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(1, f"incomplete WhoScored scan evidence: {exc}\n")


if __name__ == "__main__":
    main()
