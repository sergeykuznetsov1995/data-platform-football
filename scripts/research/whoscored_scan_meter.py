"""Count and qualify paired physical Iceberg scans on a bounded WhoScored stand.

Supply TABLES.json, the complete Trino 484 console segment at TrinoMetricsReporter
DEBUG and the isolated REST receiver's JSONL rows {at, table, report}. The receiver
records the qualified table from its route. Use a stable --origin for the coordinator
instance. Events use the native timestamp and retain their identity on replay.

Use --paired-only to count observed scans of the inventory under arbitrary concurrency.
It does not attribute scans to SQL or certify that both reporters saw every scan.

Use --workload only for an isolated experiment with a known list of submitted queries.
Provide disjoint [start, end) intervals in any order, the expected counts per table,
and either one query_id or a concurrent group of query_ids. A scan in a gap or a
count mismatch invalidates the experiment. Only a single-query control supplies
a query ID. Iceberg's two reporting paths do not independently establish it.
"""

import argparse
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import astuple, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys


LOGGER = "io.trino.plugin.iceberg.TrinoMetricsReporter"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
TABLE = re.compile(r'(bronze|ops)\.(?:"(whoscored_[a-z0-9_]+)"|(whoscored_[a-z0-9_]+))')
NATIVE = re.compile(
    r"Received metrics report: ScanReport\{tableName=([^,]+), snapshotId=(\d+), "
    r"filter=.+, schemaId=(\d+), projectedFieldIds=\[([\d, ]*)\], projectedFieldNames=\[.*?\], "
    r"scanMetrics=ScanMetricsResult\{totalPlanningDuration=TimerResult\{timeUnit=NANOSECONDS, "
    r"totalDuration=(PT[^,]+), count=(\d+)\}, (.+)\}, metadata=\{([^{}]*)\}\}"
)
COUNTER = re.compile(r"(\w+)=CounterResult\{unit=(\w+), value=(\d+)\}")
DURATION = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)(?:\.(\d{1,9}))?S)?")


def timestamp(raw):
    at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if at.utcoffset() is None:
        raise ValueError("timestamp requires a UTC offset")
    return at.astimezone(timezone.utc)


def table_name(raw):
    match = TABLE.fullmatch(raw)
    if not match:
        raise ValueError(f"unsupported stand table {raw!r}")
    schema, quoted, plain = match.groups()
    return f"{schema}.{quoted or plain}"


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError("expected a nonnegative integer")
    return value


def duration_ns(raw):
    match = DURATION.fullmatch(raw)
    if not match or not any(match.groups()):
        raise ValueError("invalid native planning duration")
    hours, minutes, seconds, fraction = match.groups(default="")
    elapsed = int(hours or 0) * 3600 + int(minutes or 0) * 60 + int(seconds or 0)
    return elapsed * 10**9 + int(fraction.ljust(9, "0"))


@dataclass(frozen=True, order=True)
class Signature:
    table: str
    snapshot: int
    schema: int
    projected: tuple
    nanos: int
    timer_count: int
    counters: tuple


def native_scan(line):
    fields = ANSI.sub("", line.rstrip("\r\n")).split("\t", 4)
    if len(fields) < 4 or fields[3] != LOGGER:
        return None
    if len(fields) != 5 or fields[1] != "DEBUG":
        raise ValueError("unexpected native reporter record")
    report = NATIVE.fullmatch(fields[4])
    if not report:
        raise ValueError("invalid native ScanReport")
    name, snapshot, schema, projected, duration, count, metrics, metadata = (
        report.groups()
    )
    properties = dict(pair.split("=", 1) for pair in metadata.split(", "))
    if (
        properties.get("engine-name") != "trino"
        or properties.get("engine-version") != "484"
    ):
        raise ValueError("unexpected native engine")
    counters = {}
    matches = list(COUNTER.finditer(metrics))
    if ", ".join(match.group() for match in matches) != metrics:
        raise ValueError("invalid native counters")
    for match in matches:
        camel, unit, value = match.groups()
        key = re.sub(r"([A-Z])", r"-\1", camel).lower()
        if key in counters:
            raise ValueError("repeated native counter")
        counters[key] = (unit.lower(), int(value))
    if "result-data-files" not in counters:
        raise ValueError("missing native data-file counter")
    ids = tuple(int(part) for part in projected.split(",") if part.strip())
    return timestamp(fields[0]), Signature(
        table_name(name),
        int(snapshot),
        int(schema),
        ids,
        duration_ns(duration),
        int(count),
        tuple(sorted(counters.items())),
    )


def rest_scan(receipt):
    report = receipt["report"]
    table = table_name(report["table-name"])
    timestamp(receipt["at"])
    if table != receipt["table"] or report["report-type"] != "scan-report":
        raise ValueError("REST report type or table disagrees with its route")
    metadata = report["metadata"]
    if (
        metadata.get("engine-name") != "trino"
        or metadata.get("engine-version") != "484"
    ):
        raise ValueError("unexpected REST engine")
    metrics = report["metrics"]
    timer = metrics["total-planning-duration"]
    if timer["time-unit"] != "nanoseconds" or "result-data-files" not in metrics:
        raise ValueError("invalid REST scan metrics")
    counters = tuple(
        sorted(
            (key, (field["unit"], integer(field["value"])))
            for key, field in metrics.items()
            if key != "total-planning-duration"
        )
    )
    return Signature(
        table,
        integer(report["snapshot-id"]),
        integer(report["schema-id"]),
        tuple(integer(x) for x in report["projected-field-ids"]),
        integer(timer["total-duration"]),
        integer(timer["count"]),
        counters,
    )


def paired_scans(origin, tables, native_lines, rest_receipts):
    if not isinstance(origin, str) or not origin.strip():
        raise ValueError("a stable coordinator origin is required")
    if not isinstance(tables, list) or not tables or len(set(tables)) != len(tables):
        raise ValueError("a nonempty inventory of unique tables is required")
    if any(table != table_name(table) for table in tables):
        raise ValueError("inventory must use qualified, unquoted table names")
    native = [scan for line in native_lines if (scan := native_scan(line)) is not None]
    native_counts = Counter(key for _, key in native)
    rest_counts = Counter(rest_scan(row) for row in rest_receipts)
    if native_counts != rest_counts:
        missing = sum((native_counts - rest_counts).values())
        extra = sum((rest_counts - native_counts).values())
        raise ValueError(f"REST differs from native: {missing} missing, {extra} extra")
    if any(count > 1 for count in native_counts.values()):
        raise ValueError("ambiguous repeated scan signature across both channels")

    events = []
    for at, signature in sorted(native):
        if signature.table not in tables:
            raise ValueError("observed table outside stand inventory")
        identity = json.dumps(
            [origin, at.isoformat(), astuple(signature)], separators=(",", ":")
        )
        events.append(
            {
                "kind": "scan",
                "table": signature.table,
                "at": at.isoformat(),
                "id": hashlib.sha256(identity.encode()).hexdigest(),
            }
        )
    return events


def qualify(tables, workload, native_lines, rest_receipts, *, origin):
    events = paired_scans(origin, tables, native_lines, rest_receipts)
    windows = []
    all_ids, covered = set(), set()
    for row in workload:
        if ("query_id" in row) == ("query_ids" in row):
            raise ValueError("declare either one query ID or a group")
        ids = [row["query_id"]] if "query_id" in row else row["query_ids"]
        if (
            not isinstance(ids, list)
            or not ids
            or not all(isinstance(q, str) and q.strip() for q in ids)
            or len(set(ids)) != len(ids)
            or not all_ids.isdisjoint(ids)
        ):
            raise ValueError("invalid or repeated query ID")
        all_ids.update(ids)
        start, end = timestamp(row["start"]), timestamp(row["end"])
        if end <= start:
            raise ValueError("invalid query window")
        expected = row["expected_scans"]
        if not isinstance(expected, dict) or not expected.keys() <= set(tables):
            raise ValueError("unexpected workload table")
        for table, count in expected.items():
            if integer(count):
                covered.add(table)
        windows.append((start, end, ids, Counter(expected)))
    windows.sort(key=lambda window: window[0])
    if covered != set(tables):
        raise ValueError("every stand table needs a positive control")
    if any(current[0] < previous[1] for previous, current in zip(windows, windows[1:])):
        raise ValueError("control windows must be disjoint; group simultaneous queries")

    cursor = 0
    for start, end, ids, expected in windows:
        seen = Counter()
        while cursor < len(events):
            event = events[cursor]
            at = timestamp(event["at"])
            if at >= end:
                break
            if at < start:
                raise ValueError("scan outside a known query window")
            seen[event["table"]] += 1
            if len(ids) == 1:
                event["query_id"] = ids[0]
            cursor += 1
        if seen != expected:
            raise ValueError(
                f"physical scans differ from workload for {ids}: {seen} vs {expected}"
            )
    if cursor != len(events):
        raise ValueError("scan after the workload")
    return events


def complete_lines(stream: Iterable[str]) -> Iterator[str]:
    for line in stream:
        if not line.endswith("\n"):
            raise ValueError("truncated evidence line")
        yield line


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tables", "native", "rest"):
        parser.add_argument(name, type=Path)
    parser.add_argument(
        "--origin", required=True, help="stable native coordinator instance"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--workload", type=Path, help="independent stand control intervals as JSONL"
    )
    mode.add_argument(
        "--paired-only", action="store_true", help="emit paired observations only"
    )
    args = parser.parse_args()
    try:
        tables = json.loads(args.tables.read_text())
        with args.native.open() as log, args.rest.open() as receipts:
            lines = complete_lines(log)
            rows = map(json.loads, complete_lines(receipts))
            if args.workload:
                with args.workload.open() as control:
                    events = qualify(
                        tables,
                        map(json.loads, complete_lines(control)),
                        lines,
                        rows,
                        origin=args.origin,
                    )
            else:
                events = paired_scans(args.origin, tables, lines, rows)
                print(
                    "Paired observations only; interval completeness is unverified.",
                    file=sys.stderr,
                )
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
        parser.exit(1, f"incomplete stand evidence: {exc}\n")
    for event in events:
        print(json.dumps(event))


if __name__ == "__main__":
    main()
