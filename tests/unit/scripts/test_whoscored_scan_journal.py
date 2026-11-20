from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from io import StringIO
import json

import pytest

from scripts.ops import whoscored_scan_journal as meter


GEN = "612119f3-017f-450e-9a08-ab1c32c78b4b"
TABLE = "bronze.whoscored_matches"
DAY = datetime(2026, 1, 2, tzinfo=timezone.utc)


def stamp(seconds):
    return (DAY + timedelta(seconds=seconds)).isoformat()


def health(seconds, beat, produced=0, **updates):
    mark = meter.Health(GEN, stamp(seconds), beat, produced, produced, 0, 0, 0, 0, True)
    return {"kind": "health", **asdict(replace(mark, **updates))}


def scan(seq, at, snapshot=7):
    return meter.Scan(GEN, seq, at, TABLE, snapshot, 500000)


def opened(row):
    return {"kind": "start", "at": row.at, "table": row.table, "snapshot": row.snapshot}


def checkpoints(rows):
    out = []
    for i, seconds in enumerate(range(-75, 91, 15), 1):
        count = sum(meter.nanos(r.at) <= meter.nanos(stamp(seconds)) for r in rows)
        out.append(health(seconds, i, count))
    return out


def audit(rows, changes=None):
    console = checkpoints(rows) + [opened(r) for r in rows]
    physical = list(rows)
    if changes:
        changes(console, physical)
    return meter.qualify(stamp(0), stamp(80), [TABLE], console, physical)


def test_preserves_nanoseconds_at_exact_minute_and_midnight():
    rows = [
        scan(1, "2026-01-01T23:59:59.999999999Z"),
        scan(2, "2026-01-02T00:00:00.000000000Z"),
        scan(3, "2026-01-02T00:00:59.999999998Z"),
        scan(4, "2026-01-02T00:00:59.999999999Z"),
        scan(5, stamp(80)),
    ]
    result = audit(rows, lambda console, physical: physical.extend(rows))
    assert result["tables"][TABLE] == {"scans": 3, "peak_60s": 3, "peak_at": rows[2].at}
    assert [e["id"] for e in result["events"]] == [f"{GEN}/{i}" for i in range(1, 5)]
    assert result["events"][2]["at_ns"] - result["events"][0]["at_ns"] == 59999999999
    assert "query_id" not in result["events"][0]


def test_two_indistinguishable_signatures_with_distinct_native_ordinals_are_both_scans():
    a, b = scan(1, stamp(1)), scan(2, stamp(1))
    result = audit([a, b])
    assert result["tables"][TABLE]["scans"] == 2
    assert result["tables"][TABLE]["peak_60s"] == 2


def test_peak_at_day_open_includes_previous_minute_even_with_no_new_scans():
    rows = [
        scan(1, stamp(-60)),
        scan(2, "2026-01-01T23:59:00.000000001Z"),
        scan(3, stamp(-5)),
    ]
    assert audit(rows)["tables"][TABLE] == {
        "scans": 0,
        "peak_60s": 2,
        "peak_at": stamp(0),
    }


@pytest.mark.parametrize(
    "fault",
    [
        "native_loss",
        "start_loss",
        "both_lost",
        "bad_snapshot",
        "replay_conflict",
        "heartbeat_lost",
        "disabled",
        "disk_error",
        "unknown_report",
        "overflow",
        "generation_changed",
        "missing_tail",
        "late_native",
    ],
)
def test_incomplete_evidence_fails_closed(fault):
    rows = [scan(1, stamp(2)), scan(2, stamp(5))]

    def change(console, native):
        if fault in {"native_loss", "both_lost"}:
            native.pop()
        if fault in {"start_loss", "both_lost"}:
            console.pop()
        if fault == "bad_snapshot":
            console[-1]["snapshot"] = 9
        if fault == "replay_conflict":
            native.append(replace(rows[0], planning_ns=1))
        if fault == "heartbeat_lost":
            console.pop(5)
        if fault == "disabled":
            console[5]["active"] = False
        if fault in {"disk_error", "unknown_report", "overflow"}:
            key = {
                "disk_error": "errors",
                "unknown_report": "invalid",
                "overflow": "dropped",
            }[fault]
            console[6][key] = 1
        if fault == "generation_changed":
            console[6]["generation"] = "0758a63e-266b-4916-9fc0-636f1b42b98c"
        if fault == "missing_tail":
            del console[-3]
        if fault == "late_native":
            native.append(scan(3, stamp(3)))

    with pytest.raises(ValueError):
        audit(rows, change)


def test_earlier_loss_outside_bracket_does_not_invalidate_clean_window():
    rows = [scan(51, stamp(2))]
    counters = [
        health(
            seconds,
            beat,
            produced=(50 if seconds < 2 else 51),
            written=(49 if seconds < 2 else 50),
            dropped=1,
        )
        for beat, seconds in enumerate(range(-75, 91, 15), 85)
    ]
    result = meter.qualify(
        stamp(0), stamp(80), [TABLE], [*counters, opened(rows[0])], rows
    )
    assert result["tables"][TABLE]["scans"] == 1


def test_selects_only_telemetry_and_redacts_filter():
    time = stamp(1)
    msg = f'Scanning table bronze."whoscored_matches" snapshot 7 created at {time} with filter private-value'
    hp = f"WS_SCAN_HEALTH1\t{GEN}\t{time}\t1\t2\t2\t0\t0\t0\t0\ttrue"
    raw = (
        "\n".join(
            [
                f"{time}\tINFO\tworker\t{meter.START_LOG}\t{msg}",
                f"{time}\tINFO\tworker\t{meter.START_LOG}\tScanning empty table bronze.whoscored_empty",
                f"{time}\tINFO\tworker\t{meter.START_LOG}\t{msg.replace('whoscored_matches', 'other_source')}",
                f"{time}\tINFO\tworker\tunrelated.logger\t{hp}",
                json.dumps(
                    {
                        "timestamp": time,
                        "level": "INFO",
                        "logger": meter.HEALTH_LOG,
                        "message": hp,
                    }
                ),
            ]
        )
        + "\n"
    )
    selected = list(meter.select_console(StringIO(raw)))
    assert [item["kind"] for item in selected] == ["start", "health"]
    assert "private-value" not in json.dumps(selected)
    assert selected[0]["table"] == TABLE
    assert selected[1]["produced"] == 2


def test_rejects_unrecognized_source_or_incomplete_line():
    time = stamp(0)
    row = f'{time}\tINFO\tworker\t{meter.START_LOG}\tScanning table bronze."whoscored_mixed$files" snapshot 7 created at noon'
    with pytest.raises(ValueError):
        list(meter.select_console(StringIO(row + "\n")))
    with pytest.raises(ValueError):
        list(meter.select_console(StringIO(row)))
    with pytest.raises(ValueError):
        meter.Scan.parse(f"SCAN1\t{GEN}\t1\t{time}\tbronze.other\t1\t0")
