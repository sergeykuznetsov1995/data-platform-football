"""Traffic report (#1404): ledger files, ops SQL on DuckDB, receipts, history step 1, CLI."""

from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path

import duckdb
import pytest
import sqlglot

from dags.scripts import transfermarkt_traffic_report as cli
from dags.utils import transfermarkt_backfill_state as backfill
from dags.utils import transfermarkt_traffic_report as traffic
from dags.utils.transfermarkt_daily_measure import day_bounds, utc

pytestmark = pytest.mark.unit

DAY = "2026-10-29"
START, END = day_bounds(DAY)  # 2026-10-28T21:00Z .. 2026-10-29T21:00Z
AS_OF = END + timedelta(days=2)  # every run of the day has settled
SAMPLE = [  # one real ledger line of each event type (.local/task/ledger-sample.jsonl), trimmed
    {"event_type": "bytes", "event_version": "paid-proxy-v2", "direction": "down", "bytes": 1183,
     "dag_id": "dag_ingest_transfermarkt", "entity": "market_value_points",
     "lease_id": "ddb9fcc200d93f49f0854d31606ef701", "run_id": "scheduled__2026-10-28T04:00:00+00:00",
     "stream_id": "current-0", "source": "transfermarkt", "request_id": "", "provider_meter": "",
     "occurred_at": "2026-10-29T04:34:59.860984+00:00"},
    {"event_type": "lease_closed", "event_version": "paid-proxy-v2", "dag_id": "dag_ingest_transfermarkt",
     "entity": "market_value_points", "lease_id": "ddb9fcc200d93f49f0854d31606ef701",
     "run_id": "scheduled__2026-10-28T04:00:00+00:00", "stream_id": "current-0", "total_bytes": 539320,
     "tunnels_total": 1, "occurred_at": "2026-10-29T04:45:27.590722+00:00"},
    {"event_type": "lease_created", "event_version": "paid-proxy-v2", "dag_id": "dag_ingest_transfermarkt",
     "entity": "market_value_points", "lease_id": "f482a4b55a024bed0d2fa84caa8cfed0", "max_bytes": 5752136,
     "run_id": "scheduled__2026-10-28T04:00:00+00:00", "stream_id": "current-0",
     "occurred_at": "2026-10-29T04:45:28.312692+00:00"},
    {"event_type": "connect_rejected", "event_version": "paid-proxy-v2", "classification": "provider_rejected",
     "reason": "502", "dag_id": "dag_ingest_transfermarkt", "entity": "market_value_points",
     "lease_id": "57e9b0226879907e8e195aa9e8429da9", "run_id": "scheduled__2026-10-28T04:00:00+00:00",
     "stream_id": "current-0", "occurred_at": "2026-10-29T04:46:35.669048+00:00"},
    {"event_type": "bytes", "event_version": "paid-proxy-v2", "direction": "up", "bytes": 180,
     "dag_id": "dag_discover_transfermarkt_registry", "entity": "squad",
     "lease_id": "baa1f4bfc102f09c5a089e4c686c3f70", "run_id": "tm1393-measurement-8a6eea260ebc44d38f19ea616603c59e",
     "stream_id": "current-0", "occurred_at": "2026-10-29T15:00:10.635305+00:00"},
]
CURRENT_RUN = "scheduled__2026-10-28T04:00:00+00:00"


def event(kind="bytes", *, at, size=0, dag="dag_ingest_transfermarkt", entity="market_value_points",
          run=CURRENT_RUN, stream="current-0", direction="down", **extra):
    base = next(dict(item) for item in SAMPLE if item["event_type"] == kind)
    base.update(occurred_at=at, dag_id=dag, entity=entity, run_id=run, stream_id=stream, **extra)
    if kind == "bytes":
        base.update(bytes=size, direction=direction)
    return base


def write_ledger(directory: Path, events, *, archives=None):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "paid_requests.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in events), encoding="utf-8")
    for stamp, items in (archives or {}).items():
        with gzip.open(directory / f"paid_requests.{stamp}.jsonl.gz", "wt", encoding="utf-8") as handle:
            handle.writelines(json.dumps(item) + "\n" for item in items)
    return directory


def ledger_summary(directory: Path):
    return traffic.read_ledger(directory, START, END, AS_OF)


def summarize(events, as_of=AS_OF):
    events = [{**item, "_at": utc(item["occurred_at"])} for item in events]
    return traffic.summarize_ledger(events, START, END, as_of)


def test_ledger_day_window_archives_and_malformed_lines(tmp_path):
    events = [
        event(at="2026-10-28T20:59:59+00:00", size=10_000),           # previous Moscow day
        event(at="2026-10-28T21:00:00+00:00", size=100, direction="up"),
        event(at="2026-10-29T20:59:59+00:00", size=200),
        event(at="2026-10-29T21:00:00+00:00", size=20_000),           # next Moscow day
        event(at="2026-10-29T10:00:00+00:00", size=300, dag="dag_backfill_transfermarkt",
              stream="history-1", entity="squad", run="hist-run"),
        event(at="2026-10-29T15:00:10+00:00", size=180, dag="dag_discover_transfermarkt_registry",
              entity="squad", run="reg-run", direction="up"),
        event(at="2026-10-29T16:00:00+00:00", size=5, dag="", entity="gateway_probe", run=""),
        event(at="2026-10-30T09:00:00+00:00", size=4, run="tomorrow-only"),
        event("lease_created", at="2026-10-29T04:45:28+00:00"),
        event("lease_closed", at="2026-10-29T04:45:27+00:00", tunnels_total=3),
        event("lease_closed", at="2026-10-29T06:00:00+00:00", tunnels_total=1, total_bytes=10**9),
        event("lease_closed", at="2026-10-30T06:00:00+00:00", tunnels_total=5),
        event("connect_rejected", at="2026-10-29T04:46:35+00:00"),
        event("connect_rejected", at="2026-10-29T04:46:36+00:00"),
    ]
    directory = write_ledger(tmp_path / "gw", events, archives={
        "20261029T120000Z": [event(at="2026-10-29T11:00:00+00:00", size=1000)],
        "20261028T000000Z": [event(at="2026-10-29T11:00:00+00:00", size=7_777_777)],  # stamped before the day
    })
    with (directory / "paid_requests.jsonl").open("a") as handle:
        handle.write("{not json\n" + json.dumps({"event_type": "bytes", "bytes": 9}) + "\n")
    summary = ledger_summary(directory)
    assert summary["provider_bytes"] == {"total": 1785, "up": 280, "down": 1505}
    assert summary["by_lane"] == {"current": 1300, "history": 300, "registry": 180, "other": 5}
    assert summary["by_stream"] == {"current-0": 1485, "history-1": 300}
    assert summary["by_lane_entity"]["registry"] == {"players": 180}
    run = summary["runs"][CURRENT_RUN]
    assert (run["bytes_in_day"], run["bytes_window"], run["groups"]) == (1300, 31_300, {"market_value_history": 31_300})
    assert run["started_before_day"] and not run["in_progress"] and run["tunnels_window"] == 9
    assert run["first_at"] == "2026-10-28T20:59:59+00:00" and run["last_at"] == "2026-10-29T21:00:00+00:00"
    assert "tomorrow-only" not in summary["runs"]
    assert summary["run_ids"] == {"current": [CURRENT_RUN], "history": ["hist-run"],
                                  "registry": ["reg-run"], "other": [""]}
    assert (summary["leases_opened"], summary["leases_closed"], summary["tunnels"]) == (1, 2, 4)
    assert summary["tunnels_by_lane"] == {"current": 4}
    assert summary["connect_rejected"] == {"502": 2}
    assert summary["bytes_per_tunnel"] == 1785 / 4
    assert summary["malformed_lines"] == 2 and summary["complete"] is True and summary["incomplete_reason"] is None
    assert summary["earliest_event_at"] == "2026-10-28T20:59:59+00:00"
    assert summary["files_read"] == ["paid_requests.20261029T120000Z.jsonl.gz", "paid_requests.jsonl"]


def test_unreadable_archive_makes_ledger_incomplete_and_missing_ledger_fails(tmp_path):
    directory = write_ledger(tmp_path / "gw", [event(at="2026-10-29T10:00:00+00:00", size=7)])
    (directory / "paid_requests.20261030T000000Z.jsonl.gz").write_bytes(b"not gzip")
    ledger = ledger_summary(directory)
    assert ledger["provider_bytes"]["total"] == 7 and ledger["complete"] is False
    assert ledger["incomplete_reason"] == "archives_unreadable"
    assert ledger["archives_unreadable"] == ["paid_requests.20261030T000000Z.jsonl.gz"]
    merged = traffic.merge_ops(ledger, [ops_row("events_v1", CURRENT_RUN, "market_value_points", 7)], {}, ALL_OK)
    assert (merged["reconciliation"]["status"], merged["reconciliation"]["reason"]) == (
        "not_measured", "ledger_incomplete")
    assert merged["lanes"]["current"]["reconciliation_status"] == "not_measured"
    with pytest.raises(traffic.TrafficReportError):
        ledger_summary(tmp_path / "absent")
    (tmp_path / "empty").mkdir()
    with pytest.raises(traffic.TrafficReportError):
        ledger_summary(tmp_path / "empty")


def _duck(sql, params, setup):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA ops")
    for statement in setup:
        con.execute(statement)
    cur = con.execute(sqlglot.transpile(sql.replace("iceberg.ops.", "ops."), read="trino", write="duckdb")[0],
                      list(params))
    columns = [item[0] for item in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


OPS_SETUP = (
    "CREATE TABLE ops.transfermarkt_proxy_ledger_events_v1 (parent_cycle_id varchar, child_cycle_id varchar, "
    "scope_id varchar, manifest_digest varchar, entity varchar, decoded_bytes bigint, wire_bytes bigint, "
    "provider_metered_bytes bigint, requests bigint, retries bigint, cache_hits bigint, duration_ms bigint, "
    "receipt_id varchar)",
    "INSERT INTO ops.transfermarkt_proxy_ledger_events_v1 VALUES "
    "('run-a', 'c1', 's1', 'm', 'squad', 1000, 500, 600, 3, 1, 2, 10, 'r1'),"
    "('run-a', 'c2', 's2', 'm', 'squad', 1000, 500, 400, 2, 0, 0, 10, 'r2'),"
    "('run-z', 'c3', 's3', 'm', 'squad', 1, 1, 1, 1, 0, 0, 1, 'r3')",
    "CREATE TABLE ops.proxy_traffic_runs (source varchar, dag_run_id varchar, entity varchar, "
    "request_count bigint, retry_count bigint, failure_count bigint, decoded_response_body_bytes bigint, "
    "provider_metered_bytes bigint)",
    "INSERT INTO ops.proxy_traffic_runs VALUES ('transfermarkt', 'run-a', 'squad', 9, 9, 0, 9, 9999),"
    "('transfermarkt', 'run-b', 'transfers', 4, 1, 1, 800, 700),"
    "('whoscored', 'run-b', 'transfers', 4, 1, 1, 800, 700)",
)


def test_ops_sql_aggregates_only_ledger_runs():
    events = _duck(*traffic.build_ops_events_sql(["run-a", "run-b"]), OPS_SETUP)
    assert events == [{"ops_source": "events_v1", "run_id": "run-a", "entity": "squad", "requests": 5,
                       "retries": 1, "cache_hits": 2, "decoded_bytes": 2000, "wire_bytes": 1000,
                       "provider_metered_bytes": 1000, "duration_ms": 20}]
    runs = sorted(_duck(*traffic.build_ops_runs_sql(["run-a", "run-b"]), OPS_SETUP), key=lambda r: r["run_id"])
    assert [(r["run_id"], r["requests"], r["provider_metered_bytes"]) for r in runs] == [
        ("run-a", 9, 9999), ("run-b", 4, 700)]
    sql, params = traffic.build_ops_events_sql(["run-a", "", "run-a"])
    assert params == ("run-a",) and "run-a" not in sql
    with pytest.raises(ValueError):
        traffic.build_ops_runs_sql([""])


ALL_OK = {"events_v1": "ok", "proxy_traffic_runs": "ok", "current_receipts": "not_available"}


def _ledger(by_run, tunnels=None):
    events = [event(at="2026-10-29T10:00:00+00:00", size=size, dag=dag, run=run, entity=entity)
              for dag, runs in by_run.items() for run, entities in runs.items()
              for entity, size in entities.items()]
    events += [event("lease_closed", at="2026-10-29T11:00:00+00:00", dag=dag, run=run, tunnels_total=count)
               for dag, (run, count) in (tunnels or {}).items()]
    return summarize(events)


def ops_row(source, run, entity, provider, requests=0, **extra):
    return {"ops_source": source, "run_id": run, "entity": entity, "provider_metered_bytes": provider,
            "requests": requests, "retries": 0, "cache_hits": 0, "decoded_bytes": 0, **extra}


def test_merge_prefers_events_v1_assigns_lane_and_lists_unexplained():
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"squad": 10_240, "gateway_probe": 50}},
                      "dag_backfill_transfermarkt": {"run-h": {"transfers": 2048}},
                      "dag_discover_transfermarkt_registry": {"reg": {"squad": 4096}}},
                     tunnels={"dag_ingest_transfermarkt": ("run-a", 2)})
    rows = [ops_row("proxy_traffic_runs", "run-a", "squad", 99_999, requests=99),
            ops_row("events_v1", "run-a", "squad", 10_000, requests=5, retries=1, cache_hits=2),
            ops_row("proxy_traffic_runs", "run-h", "transfers", 2000, requests=2)]
    merged = traffic.merge_ops(ledger, rows, {"status": "not_available"}, ALL_OK)
    current = merged["lanes"]["current"]
    assert (current["ops_provider_bytes"], current["requests"], current["retries"], current["cache_hits"]) == (
        10_000, 5, 1, 2)
    assert current["kib_per_request"] == (10_240 + 50) / 1024 / 5
    assert current["requests_per_tunnel"] == 2.5
    assert merged["lanes"]["history"]["ops_provider_bytes"] == 2000
    assert merged["entities"]["current"]["players"]["kib_per_request"] == 10_240 / 1024 / 5
    rec = merged["reconciliation"]
    assert rec["ledger_provider_bytes"] == 10_240 + 50 + 2048 and rec["ops_provider_bytes"] == 12_000
    assert rec["status"] == "gap" and rec["covered_lanes"] == ["current", "history"]
    assert merged["lanes"]["registry"]["reconciliation_status"] == "not_covered"
    assert merged["lanes"]["history"]["reconciliation_status"] == "gap"
    assert [(item["lane"], item["entity"]) for item in rec["unexplained"]] == [("current", "gateway_probe")]
    assert rec["unexplained_bytes"] == 50
    none = {"events_v1": "not_available", "proxy_traffic_runs": "not_available", "current_receipts": "not_available"}
    assert traffic.merge_ops(ledger, [], {}, none)["reconciliation"]["status"] == "not_measured"


@pytest.mark.parametrize("ops, status", [(98_100, "ok"), (97_900, "gap"), (101_900, "ok"), (102_100, "gap")])
def test_reconciliation_tolerance_is_two_percent(ops, status):
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"squad": 100_000}}})
    rec = traffic.merge_ops(ledger, [ops_row("events_v1", "run-a", "squad", ops)], {}, ALL_OK)["reconciliation"]
    assert rec["status"] == status and rec["delta_pct"] == pytest.approx((ops - 100_000) / 1000)


def test_run_across_midnight_is_reconciled_whole_unless_still_running():
    mb = 1_000_000
    night = [event(at="2026-10-29T20:00:00+00:00", size=5 * mb, run="night"),
             event(at="2026-10-29T21:30:00+00:00", size=10, run="night")]   # after the Moscow day
    rows = [ops_row("events_v1", "night", "market_value_points", 5 * mb + 10, requests=4)]
    settled = traffic.merge_ops(summarize(night), rows, {}, ALL_OK)["reconciliation"]
    assert (settled["status"], settled["ledger_provider_bytes"], settled["coverage_pct"]) == ("ok", 5 * mb + 10, 100.0)
    running = summarize(night, as_of=utc("2026-10-29T21:40:00+00:00"))   # last bytes 10 min ago
    rec = traffic.merge_ops(running, rows, {}, ALL_OK)["reconciliation"]
    assert (rec["status"], rec["reason"]) == ("not_measured", "no_settled_runs")
    assert rec["in_progress_runs"] == [{"run_id": "night", "lane": "current", "bytes_in_day": 5 * mb}]
    mixed = summarize([event(at="2026-10-29T10:00:00+00:00", size=mb, run="small"),
                       event(at="2026-10-29T20:50:00+00:00", size=100 * mb, run="big"),
                       event(at="2026-10-29T21:35:00+00:00", size=10, run="big")],
                      as_of=utc("2026-10-29T21:40:00+00:00"))
    merged = traffic.merge_ops(mixed, [ops_row("events_v1", "small", "squad", mb)], {}, ALL_OK)
    rec = merged["reconciliation"]
    assert (rec["status"], rec["reason"]) == ("partial", "coverage_below_threshold")
    assert rec["coverage_pct"] == pytest.approx(100 / 101) and rec["in_progress_runs"][0]["run_id"] == "big"
    summary = traffic.format_traffic_summary({"report_day": DAY, "ledger": mixed, **merged})
    assert summary.endswith("ops частично: сверено 1,0 % байт дня (нужно ≥ 98,0 %)")


def test_one_failed_source_makes_reconciliation_partial():
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"squad": 1000}}})
    sources = {"events_v1": "ok", "proxy_traffic_runs": "not_available", "current_receipts": "empty"}
    merged = traffic.merge_ops(ledger, [ops_row("events_v1", "run-a", "squad", 1000)], {}, sources)
    rec = merged["reconciliation"]
    assert rec["status"] == "partial" and rec["unavailable_sources"] == ["proxy_traffic_runs"]
    assert merged["lanes"]["current"]["ops_provider_bytes"] == 1000
    summary = traffic.format_traffic_summary({"report_day": DAY, "ledger": ledger, **merged})
    assert summary.endswith("ops частично (недоступно: proxy_traffic_runs); сверено 100,0 % байт дня")


def test_wire_and_duration_are_none_until_a_source_measures_them():
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"squad": 1000}, "run-b": {"transfers": 10}}})
    rows = [ops_row("proxy_traffic_runs", "run-a", "squad", 1000),
            ops_row("events_v1", "run-b", "transfer_events", 6, wire_bytes=300, duration_ms=40),
            ops_row("events_v1", "run-b", "transfers", 4, wire_bytes=200, duration_ms=10)]
    merged = traffic.merge_ops(ledger, rows, {}, ALL_OK)
    squads, transfers = merged["entities"]["current"]["players"], merged["entities"]["current"]["transfers"]
    assert (squads["wire_bytes"], squads["duration_ms"]) == (None, None)
    assert (transfers["wire_bytes"], transfers["duration_ms"]) == (500, 50)
    assert (merged["lanes"]["current"]["wire_bytes"], merged["lanes"]["current"]["duration_ms"]) == (500, 50)


def test_registry_lane_is_outside_reconciliation():
    mib = 1048576
    ledger = _ledger({"dag_discover_transfermarkt_registry": {"reg": {"squad": 10 * mib}},
                      "dag_ingest_transfermarkt": {"run-a": {"squad": mib}}})
    merged = traffic.merge_ops(ledger, [ops_row("events_v1", "run-a", "squad", mib, requests=4)], {}, ALL_OK)
    rec = merged["reconciliation"]
    assert rec["status"] == "ok" and rec["not_covered_lanes"] == ["registry"]
    assert rec["ledger_provider_bytes"] == mib and rec["unexplained"] == []
    assert merged["lanes"]["current"]["reconciliation_status"] == "ok"
    summary = traffic.format_traffic_summary({"report_day": DAY, "ledger": ledger, **merged})
    assert "запросов 4," in summary
    assert summary.endswith("ops покрывает 100,0 % актуалки и истории (ok), сверено 100,0 % байт дня; "
                            "реестр в ops не пишется")
    empty = traffic.merge_ops(_ledger({"dag_ingest_transfermarkt": {"run-a": {"squad": 1}}}), [], {}, ALL_OK)
    assert empty["reconciliation"]["not_covered_lanes"] == ["registry"]


def test_entity_names_merge_by_group():
    assert traffic.entity_group("coach_profiles") == "coaches" and traffic.entity_group("gateway_probe") == "gateway_probe"
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"coach_history": 9000, "gateway_probe": 7}}})
    rows = [ops_row("proxy_traffic_runs", "run-a", "coaches", 9000, requests=3),
            ops_row("events_v1", "run-a", "coach_profiles", 4000, requests=1),
            ops_row("events_v1", "run-a", "coach_history", 5000, requests=1)]
    merged = traffic.merge_ops(ledger, rows, {}, ALL_OK)
    assert set(merged["entities"]["current"]) == {"coaches", "gateway_probe"}
    coaches = merged["entities"]["current"]["coaches"]
    assert coaches["ledger_provider_bytes"] == coaches["ops_provider_bytes"] == 9000 and coaches["requests"] == 2
    assert [(item["entity"], item["ledger_bytes"]) for item in merged["reconciliation"]["unexplained"]] == [
        ("gateway_probe", 7)]
    only_ops = traffic.merge_ops(_ledger({"dag_ingest_transfermarkt": {"run-a": {"coach_history": 9000}}}),
                                 [ops_row("proxy_traffic_runs", "run-a", "coaches", 9000)], {}, ALL_OK)
    assert only_ops["reconciliation"]["unexplained"] == []


def receipt(child, by_entity, cycle="cycle-1"):
    return {"cycle_id": cycle, "child_cycle_id": child, "kind": "listing", "captured_at": "2026-10-29T10:00:00+00:00",
            "traffic": {"shared_traffic_ledger": {"provider_metered_bytes": 1, "by_entity": by_entity}}}


def test_current_receipts_by_run_and_duplicates(tmp_path):
    scopes = tmp_path / "current" / "scopes"
    scopes.mkdir(parents=True)
    squad = {"squad": {"requests": 2, "retries": 1, "decoded_bytes": 900, "provider_bytes": 300, "cache_hits": 1,
                       "wire_bytes": 250, "duration_seconds": 1.5}}
    first = receipt("child-1", squad)
    (scopes / "a.json").write_text(json.dumps({"traffic_receipts": [first, first, receipt("child-2", squad)]}))
    (scopes / "b.json").write_text(json.dumps({"traffic_receipts": [
        first, receipt("child-3", {"transfers": {"requests": 1, "provider_bytes": 50}})]}))
    (scopes / "bad.json").write_text("{")
    result = traffic.read_current_receipts(tmp_path / "current", ["child-1", "child-3"])
    assert result["status"] == "ok" and result["receipts"] == 2 and result["malformed"] == 1
    assert result["by_entity"]["players"] == {"requests": 2, "retries": 1, "cache_hits": 1, "decoded_bytes": 900,
                                             "provider_metered_bytes": 300, "wire_bytes": 250, "duration_ms": 1500}
    assert result["by_entity"]["transfers"]["provider_metered_bytes"] == 50
    # The ledger run_id of the signal lane is the receipt's child_cycle_id.
    ledger = _ledger({"dag_ingest_transfermarkt": {"child-1": {"squad": 300}, "child-3": {"transfers": 50}}})
    sources = {**ALL_OK, "current_receipts": "ok"}
    merged = traffic.merge_ops(ledger, [], result, sources)
    assert merged["reconciliation"]["status"] == "ok" and merged["reconciliation"]["unexplained"] == []
    assert merged["lanes"]["current"]["requests"] == 3
    assert traffic.read_current_receipts(tmp_path / "current", ["other"])["status"] == "empty"
    assert traffic.read_current_receipts(tmp_path / "absent", [])["status"] == "not_available"


def scope_row(scope, competition, edition, status, *, manifest=None, evidence=(), days=(), campaign="camp"):
    return (campaign, scope, status, manifest, json.dumps(list(evidence)),
            json.dumps({"competition_id": competition, "edition_id": edition}),
            json.dumps({"unavailable_confirmation_days": list(days)}))


def editions(rows, campaign=None):
    ddl = next(item for item in backfill.ddl_statements() if backfill.SCOPE_TABLE in item)
    setup = [sqlglot.transpile(ddl.split(" WITH (format")[0].replace("iceberg.ops.", "ops."),
                               read="trino", write="duckdb")[0]]
    setup += ["INSERT INTO ops.transfermarkt_backfill_scope_v1 (campaign_id, scope_id, status, scope_manifest_uri, "
              "raw_evidence_ids_json, target_json, record_json) VALUES " + ", ".join(
                  "(" + ", ".join("NULL" if v is None else "'" + v.replace("'", "''") + "'" for v in row) + ")"
                  for row in rows)]
    return _duck(*traffic.build_history_editions_sql(campaign), setup)


def campaigns(rows, scopes=(), campaign=None):
    ddl = {item.split("(")[0].split()[-1]: item for item in backfill.ddl_statements()}
    setup = [sqlglot.transpile(ddl[table].split(" WITH (format")[0].replace("iceberg.ops.", "ops."),
                               read="trino", write="duckdb")[0] for table in (backfill.CAMPAIGN_TABLE, backfill.SCOPE_TABLE)]
    if rows:
        setup.append("INSERT INTO ops.transfermarkt_backfill_campaign_v1 (campaign_id, status, target_count) VALUES "
                     + ", ".join(f"('{c}', '{status}', {count})" for c, status, count in rows))
    if scopes:
        setup.append("INSERT INTO ops.transfermarkt_backfill_scope_v1 (campaign_id, scope_id) VALUES "
                     + ", ".join(f"('{c}', '{scope}')" for c, scope in scopes))
    return _duck(*traffic.build_history_campaigns_sql(campaign), setup)


def test_history_step1_categories():
    rows = editions([
        scope_row("gb1-2025", "GB1", "2025", "complete", manifest="s3://m", evidence=["e1"]),
        scope_row("gb1-2015", "GB1", "2015", "pending"),
        scope_row("es1-2024", "ES1", "2024", "unavailable", evidence=["e"], days=["2026-10-01", "2026-10-02"]),
        scope_row("it1-2024", "IT1", "2024", "unavailable", evidence=["e"], days=["2026-10-01"]),
        scope_row("fr1-2024", "FR1", "2024", "terminal_error", evidence=["e"]),
        scope_row("l1-2024", "L1", "2024", "pending"),
        scope_row("nl1-2024", "NL1", "2024", "complete", manifest="s3://m", evidence=[]),
        scope_row("wm-x", "WM", "2022-qatar", "complete", manifest="s3://m", evidence=["e"]),
        scope_row("gb1-2025-other", "GB1", "2025", "complete", manifest="s3://m", evidence=["e"], campaign="old"),
    ])
    by_scope = {row["scope_id"]: row for row in rows}
    assert by_scope["gb1-2025"]["step"] == "step1" and by_scope["gb1-2015"]["step"] == "later"
    assert by_scope["wm-x"]["step"] == "unknown"
    assert {scope: row["category"] for scope, row in by_scope.items() if row["step"] == "step1"} == {
        "gb1-2025": "loaded", "es1-2024": "unavailable_with_evidence", "it1-2024": "undetermined",
        "fr1-2024": "failed_with_evidence", "l1-2024": "undetermined", "nl1-2024": "undetermined",
        "gb1-2025-other": "loaded"}
    assert by_scope["it1-2024"]["evidence_missing"] and not by_scope["l1-2024"]["evidence_missing"]
    known = [{"campaign_id": "camp", "status": "active", "target_count": 9, "scope_count": 9},
             {"campaign_id": "old", "status": "complete", "target_count": 1, "scope_count": 1}]
    summary = traffic.summarize_editions(rows, known)
    assert summary["status"] == "ok"
    assert summary["campaigns"]["camp"] == {
        "campaign_status": "active", "target_count": 9, "scope_count": 9, "complete_set": True,
        "step1_total": 6, "loaded": 1, "unavailable_with_evidence": 1, "failed_with_evidence": 1,
        "undetermined": 3, "evidence_missing": 2,
        "undetermined_scope_ids": ["it1-2024", "l1-2024", "nl1-2024"]}
    assert summary["step1"]["step1_total"] == 7 and summary["step1"]["loaded"] == 2
    only = editions([scope_row("a", "GB1", "2025", "pending"), scope_row("b", "GB1", "2025", "pending", campaign="x")],
                    campaign="x")
    assert [row["scope_id"] for row in only] == ["b"]
    assert traffic.format_editions_summary({"history_editions": summary}) == (
        "история шаг 1: изданий 7, загружено 2, недоступно с доказательством 1, "
        "ошибка с доказательством 1, не определено 3")


def test_campaign_denominator_blocks_acceptance():
    found = campaigns([("camp", "active", 3), ("other", "active", 1)],
                      scopes=[("camp", "a"), ("camp", "b"), ("other", "x")])
    assert [(r["campaign_id"], r["status"], r["target_count"], r["scope_count"]) for r in found] == [
        ("camp", "active", 3, 2), ("other", "active", 1, 1)]
    summary = traffic.summarize_editions([], found)
    assert summary["status"] == "incomplete" and summary["campaigns"]["camp"]["complete_set"] is False
    assert traffic.format_editions_summary({"history_editions": summary}) == (
        "история шаг 1: набор целей неполный: scope 2 из 3 (camp)")
    assert campaigns([("camp", "active", 3)], campaign="nope") == []
    assert traffic.summarize_editions([], [], "nope") == {"status": "not_available", "reason": "кампания не найдена"}
    assert traffic.summarize_editions([], []) == {"status": "not_available", "reason": "кампаний нет"}


def test_cli_no_trino_prints_summary(tmp_path, capsys):
    directory = write_ledger(tmp_path / "gw", [
        event(at="2026-10-29T10:00:00+00:00", size=3 * 1048576),
        event(at="2026-10-29T10:00:00+00:00", size=1048576, dag="dag_discover_transfermarkt_registry", run="reg"),
        event("lease_closed", at="2026-10-29T11:00:00+00:00", tunnels_total=2),
        event("connect_rejected", at="2026-10-29T11:00:00+00:00")])
    (directory / "paid_requests.20261030T000000Z.jsonl.gz").write_bytes(b"broken")
    out = tmp_path / "report.json"
    rc = cli.main(["--day", DAY, "--ledger-dir", str(directory), "--current-state-dir", str(tmp_path / "none"),
                   "--no-trino", "--json", str(out)])
    assert rc == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == json.loads(out.read_text())
    report = printed["report"]
    assert report["current_receipts"]["status"] == "not_available"
    assert report["history_editions"]["status"] == "not_available"
    assert report["reconciliation"]["status"] == "not_measured"
    assert printed["summary"].splitlines() == [
        "Transfermarkt трафик 2026-10-29 МСК: оплачено 4,00 МиБ (реестр 1,00 / актуалка 3,00 / история 0,00), "
        "запросов —, КиБ/запрос: реестр —, актуалка —, история —, туннелей 2 (CONNECT-отказы 502: 1), "
        "ops не измерено (леджер неполный)",
        "ТРЕВОГА: леджер: нечитаемые архивы: paid_requests.20261030T000000Z.jsonl.gz",
        "история шаг 1: не измерено — Trino не читали (--no-trino)"]


def test_cli_missing_ledger_is_rc1(tmp_path, capsys):
    assert cli.main(["--day", DAY, "--ledger-dir", str(tmp_path / "none"), "--no-trino"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "not_measured"


def test_cli_trino_errors_become_not_available(tmp_path, capsys, monkeypatch):
    from dags.utils import transfermarkt_native_v2 as native

    class Cursor:
        description = None

        def execute(self, sql, params):
            raise RuntimeError("secret host:8443 password")

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(native, "connect", Connection)
    directory = write_ledger(tmp_path / "gw", [event(at="2026-10-29T10:00:00+00:00", size=100)])
    assert cli.main(["--day", DAY, "--ledger-dir", str(directory), "--current-state-dir", str(tmp_path),
                     "--as-of", AS_OF.isoformat()]) == 0
    output = capsys.readouterr().out
    assert "secret" not in output and "8443" not in output
    report = json.loads(output)["report"]
    assert report["ops_sources"] == {"events_v1": "not_available", "proxy_traffic_runs": "not_available",
                                     "current_receipts": "not_available"}
    assert report["history_editions"]["status"] == "not_available"
    assert report["reconciliation"]["status"] == "not_measured"


def test_cli_reads_ops_sources_independently(tmp_path, capsys, monkeypatch):
    from dags.utils import transfermarkt_native_v2 as native

    class Cursor:
        description, rows = None, []

        def execute(self, sql, params):
            if "proxy_traffic_runs" in sql:
                raise RuntimeError("table missing")
            if "transfermarkt_proxy_ledger_events_v1" in sql:
                self.description = [(name,) for name in ("ops_source", "run_id", "entity", "requests",
                                                         "provider_metered_bytes", "wire_bytes")]
                self.rows = [("events_v1", CURRENT_RUN, "market_value_points", 3, 100, 70)]
            else:
                self.description, self.rows = [("campaign_id",)], []

        def fetchall(self):
            return self.rows

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(native, "connect", Connection)
    directory = write_ledger(tmp_path / "gw", [event(at="2026-10-29T10:00:00+00:00", size=100)])
    assert cli.main(["--day", DAY, "--ledger-dir", str(directory), "--current-state-dir", str(tmp_path),
                     "--as-of", AS_OF.isoformat()]) == 0
    report = json.loads(capsys.readouterr().out)["report"]
    assert report["ops_sources"]["events_v1"] == "ok" and report["ops_sources"]["proxy_traffic_runs"] == "not_available"
    assert report["lanes"]["current"]["ops_provider_bytes"] == 100 and report["lanes"]["current"]["wire_bytes"] == 70
    assert report["reconciliation"]["status"] == "partial"
    assert report["history_editions"] == {"status": "not_available", "reason": "кампаний нет"}


def test_cli_day_outside_ledger_raises_an_alarm(tmp_path, capsys):
    directory = write_ledger(tmp_path / "gw", [event(at="2026-10-29T10:00:00+00:00", size=100)])
    assert cli.main(["--day", "2026-01-01", "--ledger-dir", str(directory), "--no-trino"]) == 0
    printed = json.loads(capsys.readouterr().out)
    ledger = printed["report"]["ledger"]
    assert (ledger["complete"], ledger["incomplete_reason"]) == (False, "day_not_covered")
    assert printed["report"]["reconciliation"]["reason"] == "ledger_incomplete"
    assert "ТРЕВОГА: леджер не покрывает день" in printed["summary"].splitlines()
    # An earlier event than the day start means the day is covered, even if empty.
    assert cli.main(["--day", "2026-10-30", "--ledger-dir", str(directory), "--no-trino",
                     "--as-of", "2026-11-02T00:00:00+00:00"]) == 0
    assert json.loads(capsys.readouterr().out)["report"]["ledger"]["complete"] is True


def test_entity_groups_cover_every_label_and_output_name():
    from dags.scripts.run_transfermarkt_current import _ENTITY_LABELS
    from dags.scripts.run_transfermarkt_scope_cycle import ENTITY_OUTPUTS

    for group, labels in _ENTITY_LABELS.items():
        assert traffic.entity_group(group) == group
        for label in labels:
            assert traffic.entity_group(label) == group, label
    for group, outputs in ENTITY_OUTPUTS.items():
        assert traffic.entity_group(group) == group
        for _key, name in outputs:
            assert traffic.entity_group(name) == group, name


def test_listing_bytes_meet_squad_memberships_ops():
    ledger = _ledger({"dag_ingest_transfermarkt": {"run-a": {"listing": 700, "squad": 300}}})
    rows = [ops_row("events_v1", "run-a", "squad_memberships", 1000, requests=2),
            ops_row("events_v1", "run-a", "player_contract_observations", 0)]
    merged = traffic.merge_ops(ledger, rows, {}, ALL_OK)
    assert merged["reconciliation"]["unexplained"] == [] and merged["reconciliation"]["status"] == "ok"
    assert merged["entities"]["current"]["players"]["kib_per_request"] == 1000 / 1024 / 2
