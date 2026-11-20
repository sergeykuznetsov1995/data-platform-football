"""Qualify two independent Iceberg streams against a closed, serial stand workload."""

from copy import deepcopy
from io import StringIO

import pytest

from functools import partial

from scripts.research import whoscored_scan_meter as meter

qualify = partial(meter.qualify, origin="stand-node-1")
complete_lines = meter.complete_lines


A = "bronze.whoscored_alpha"
B = "bronze.whoscored_beta"
START = "2026-11-19T23:59:58Z"
END = "2026-11-20T00:00:09Z"
QID = "20261119_235958_00001_abcde"
LOGGER = "io.trino.plugin.iceberg.TrinoMetricsReporter"


def pair(table=A, ns=3, at="2026-11-19T23:59:59.990Z"):
    ns_table, name = table.split(".")
    wire = f'{ns_table}."{name}"'
    native = (
        f"{at}\tDEBUG\ticeberg-split-source-0\t{LOGGER}\tReceived metrics report: "
        f"ScanReport{{tableName={wire}, snapshotId=34, filter=true, schemaId=0, "
        f"projectedFieldIds=[1, 2], projectedFieldNames=[item, value], "
        f"scanMetrics=ScanMetricsResult{{totalPlanningDuration=TimerResult{{timeUnit=NANOSECONDS, "
        f"totalDuration=PT0.{ns:09d}S, count=1}}, resultDataFiles=CounterResult{{unit=COUNT, value=1}}}}, "
        "metadata={engine-version=484, engine-name=trino}}"
    )
    receipt = {
        "at": "2026-11-20T00:01:07Z",
        "table": table,
        "report": {
            "report-type": "scan-report",
            "table-name": wire,
            "snapshot-id": 34,
            "schema-id": 0,
            "projected-field-ids": [1, 2],
            "metadata": {"engine-name": "trino", "engine-version": "484"},
            "metrics": {
                "total-planning-duration": {
                    "time-unit": "nanoseconds",
                    "count": 1,
                    "total-duration": ns,
                },
                "result-data-files": {"unit": "count", "value": 1},
            },
        },
    }
    return native, receipt


def workload(counts, **updates):
    return dict(
        {"query_id": QID, "start": START, "end": END, "expected_scans": counts},
        **updates,
    )


def test_multi_scan_replay_order_and_midnight():
    one, two, other = pair(ns=3), pair(ns=4, at="2026-11-20T00:00:00.010Z"), pair(B)
    inputs = [one[0], two[0], other[0]]
    receipts = [other[1], one[1], two[1]]
    expected = [workload({A: 2, B: 1})]
    events = qualify([A, B], expected, inputs, receipts)
    assert [e["table"] for e in events].count(A) == 2
    assert len({e["id"] for e in events}) == 3
    assert {e["query_id"] for e in events} == {QID}
    assert [e["at"][:10] for e in events] == ["2026-11-19", "2026-11-19", "2026-11-20"]
    assert {e["kind"] for e in events} == {"scan"}
    assert qualify([A, B], expected, reversed(inputs), reversed(receipts)) == events


@pytest.mark.parametrize(
    "side", ["native", "rest", "both", "extra-rest", "extra-native"]
)
def test_missing_or_duplicate_observation(side):
    native, rest = pair()
    lines = [] if side in ("native", "both") else [native]
    receipts = [] if side in ("rest", "both") else [rest]
    if side == "extra-rest":
        receipts.append(rest)
    if side == "extra-native":
        lines.append(native)
    with pytest.raises(ValueError):
        qualify([A], [workload({A: 1})], lines, receipts)


def test_equal_fingerprints_are_ambiguous_even_when_multiplicities_agree():
    one, two = pair(), pair(at="2026-11-20T00:00:01Z")
    with pytest.raises(ValueError, match="ambiguous"):
        qualify([A], [workload({A: 2})], [one[0], two[0]], [one[1], two[1]])


@pytest.mark.parametrize(
    "change", ["route", "metrics", "unit", "duration", "snapshot", "type", "engine"]
)
def test_changed_rest_evidence(change):
    native, rest = pair()
    metrics = rest["report"]["metrics"]
    if change == "route":
        rest["table"] = B
    elif change == "metrics":
        metrics["result-data-files"]["value"] = 2
    elif change == "unit":
        metrics["result-data-files"]["unit"] = "bytes"
    elif change == "duration":
        metrics["total-planning-duration"]["time-unit"] = "seconds"
    elif change == "snapshot":
        rest["report"]["snapshot-id"] += 1
    elif change == "type":
        rest["report"]["report-type"] = "commit-report"
    else:
        rest["report"]["metadata"]["engine-version"] = "485"
    with pytest.raises(ValueError):
        qualify([A], [workload({A: 1})], [native], [rest])


def test_query_boundaries_inventory_and_zero_sql():
    line, receipt = pair()
    zero = workload(
        {},
        query_id="20261119_235957_00000_abcde",
        start="2026-11-19T23:59:57Z",
        end=START,
    )
    single = workload({A: 1})
    assert len(qualify([A], [zero, single], [line], [receipt])) == 1
    for inventory, windows in [
        ([A, B], [single]),
        ([B], [single]),
        ([A], [zero, zero, single]),
        ([A], [single, deepcopy(single)]),
        ([A], [workload({A: 1}, start="2026-11-20T00:00:00Z")]),
        ([A], [workload({A: 1}, end="2026-11-19T23:59:59.990Z")]),
    ]:
        with pytest.raises(ValueError):
            qualify(inventory, windows, [line], [receipt])


def test_malformed_native_or_truncated_capture():
    line, receipt = pair()
    for broken in (
        line[:-1],
        line.replace("ScanReport{", "CommitReport{"),
        line.replace("DEBUG", "INFO"),
    ):
        with pytest.raises(ValueError):
            qualify([A], [workload({A: 1})], [broken], [receipt])
    with pytest.raises(ValueError):
        list(complete_lines(StringIO("complete\npartial")))


def test_concurrent_group_counts_physical_events_without_guessing_query():
    one, two = pair(), pair(B, ns=10)
    with_group = {
        "query_ids": [QID, "second"],
        "start": START,
        "end": END,
        "expected_scans": {A: 1, B: 1},
    }
    native, rest = [one[0], two[0]], [two[1], one[1]]
    observed = meter.paired_scans("stand-node-1", [A, B], native, rest)
    assert qualify([A, B], [with_group], native, rest) == observed
    assert not any("query_id" in row for row in observed)
    # The same individual event keeps its ID across overlapping capture segments.
    assert meter.paired_scans("stand-node-1", [A], [one[0]], [one[1]])[0] in observed
    assert (
        meter.paired_scans("other-node", [A, B], native, rest)[0]["id"]
        != observed[0]["id"]
    )


def test_common_loss_requires_the_independent_workload():
    one = pair()
    assert len(meter.paired_scans("stand", [A], [one[0]], [one[1]])) == 1
    with pytest.raises(ValueError, match="differ from workload"):
        qualify([A], [workload({A: 2})], [one[0]], [one[1]])


def test_bad_inventory_or_query_attribution():
    native, rest = pair()
    for windows in (
        [workload({A: True})],
        [workload({A: 1}, query_id="")],
        [workload({A: 1}, query_ids=["second"])],
        [workload({A: 1}), workload({A: 1}, query_id="other")],
    ):
        with pytest.raises(ValueError):
            qualify([A], windows, [native], [rest])
    with pytest.raises(ValueError):
        meter.paired_scans("stand", [B], [native], [rest])


def test_cli_emits_nothing_when_evidence_is_incomplete(tmp_path, capsys, monkeypatch):
    import json
    import sys

    native, rest = pair()
    tables, log, receipts, control = [
        tmp_path / name for name in ("tables", "native", "rest", "control")
    ]
    tables.write_text(json.dumps([A]))
    log.write_text(native + "\n")
    receipts.write_text(json.dumps(rest) + "\n")
    control.write_text(json.dumps(workload({A: 2})) + "\n")
    args = ["meter", str(tables), str(log), str(receipts), "--origin", "stand"]
    monkeypatch.setattr(sys, "argv", args + ["--workload", str(control)])
    with pytest.raises(SystemExit) as failure:
        meter.main()
    assert failure.value.code == 1
    assert capsys.readouterr().out == ""
    monkeypatch.setattr(sys, "argv", args + ["--paired-only"])
    meter.main()
    output = capsys.readouterr()
    assert json.loads(output.out)["table"] == A
    assert "unverified" in output.err
