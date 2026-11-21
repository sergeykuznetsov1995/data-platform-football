"""Synthetic, offline load-meter checks; no Trino installation is required."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import types
from datetime import timedelta
from pathlib import Path

import pytest

from scrapers.fotmob import load_meter as meter


FIXTURES = Path(__file__).parents[2] / "fixtures" / "fotmob" / "load_meter"
REPOSITORY = Path(__file__).parents[3]
DAY = "2026-11-01"
MATCHES = "iceberg.bronze.fotmob_matches"
MANIFEST = "iceberg.bronze.fotmob_ingest_manifest"
SQUADS = "iceberg.bronze.fotmob_squad_snapshots"
TABLES = [MATCHES, MANIFEST, SQUADS]
UPDATE = "UPDATE iceberg.bronze.fotmob_matches SET synthetic_value = 1"
INSERT = "INSERT INTO iceberg.bronze.fotmob_matches VALUES (1)"
READ = "SELECT * FROM iceberg.bronze.fotmob_matches"
POLL = "SELECT * FROM iceberg.bronze.fotmob_ingest_manifest"
EXPIRE_SNAPSHOTS = f"ALTER TABLE {MATCHES} EXECUTE expire_snapshots(retention_threshold => '3d')"


def snapshot(query_id, query, ended, *, user="src_fotmob", poll_id="synthetic"):
    return {
        "grain": "snapshot", "poll_id": poll_id, "query_id": query_id,
        "user": user, "source": "synthetic_fixture", "query": query,
        "state": "FINISHED", "created": ended, "started": ended, "ended": ended,
        "stmt_type": "read", "error_code": None,
    }


def build_day(spec=None):
    """Expand a small specification into raw heartbeat/snapshot objects.

    ``queries`` maps minute offsets to (SQL, user) pairs; ``each_minute``
    repeats pairs across the day. One-minute windows intentionally make
    coverage edges explicit; real producer windows overlap by one interval.
    """
    spec = spec or {}
    start, _ = meter.day_bounds(DAY)
    tables = list(spec.get("tables", TABLES))
    for index in range(1440):
        minute = start + timedelta(minutes=index)
        if index in spec.get("new_tables", {}):
            tables.extend(spec["new_tables"][index])
        poll_id = f"synthetic_{index:04d}"
        if index not in spec.get("gaps", set()):
            beat = {
                "grain": "heartbeat", "poll_id": poll_id,
                "polled_at": meter.utc_text(minute + timedelta(minutes=1)),
                "window_start": meter.utc_text(minute),
                "window_end": meter.utc_text(minute + timedelta(minutes=1)),
                "watermark_ok": index not in spec.get("history_lost", set()),
                "n5_measurable": spec.get("n5_measurable", True),
                "status": "ok", "producer_variant": meter.PRODUCER_VARIANT,
                "poll_interval_s": 60, "discovered_tables": list(tables),
            }
            if index in spec.get("partial", set()):
                beat["window_start"] = meter.utc_text(minute + timedelta(seconds=30))
            yield beat
        queries = list(spec.get("each_minute", [])) + list(spec.get("queries", {}).get(index, []))
        for ordinal, (query, user) in enumerate(queries):
            yield snapshot(f"q_{index:04d}_{ordinal}", query, meter.utc_text(minute + timedelta(seconds=10 + ordinal)), user=user, poll_id=poll_id)


def quiet_spec():
    return {"queries": {0: [(UPDATE, "src_fotmob"), (POLL, "src_fotmob")], 10: [(UPDATE, "src_fotmob")], 60: [(READ, "src_fotmob")]}}


def report_for(spec=None, counts=None):
    return meter.aggregate_day(build_day(spec), DAY, snapshot_counts={} if counts is None else counts)


def table_row(report, table=MATCHES, attr="src_fotmob"):
    return next(row for row in report["tables"] if row["table"] == table and row["user_attr"] == attr)


@pytest.mark.parametrize(("sql", "operations"), [
    ("SELECT * FROM fotmob_matches m LEFT JOIN bronze.fotmob_ingest_manifest i ON true FULL OUTER JOIN iceberg.bronze.fotmob_squad_snapshots s ON true", {(MATCHES, "scan_read"), (MANIFEST, "scan_read"), (SQUADS, "scan_read")}),
    ("SELECT * FROM fotmob_matches CROSS JOIN fotmob_ingest_manifest", {(MATCHES, "scan_read"), (MANIFEST, "scan_read")}),
    ("MERGE INTO fotmob_matches t USING bronze.fotmob_ingest_manifest s ON t.id=s.id WHEN MATCHED THEN UPDATE SET x=1", {(MATCHES, "commit"), (MANIFEST, "scan_write")}),
    ("INSERT INTO fotmob_matches SELECT * FROM fotmob_ingest_manifest", {(MATCHES, "commit"), (MANIFEST, "scan_write")}),
    ("INSERT INTO fotmob_matches (id) VALUES (1)", {(MATCHES, "commit")}),
    ("UPDATE fotmob_matches SET x=1", {(MATCHES, "commit"), (MATCHES, "scan_write")}),
    ("DELETE FROM fotmob_matches WHERE id=1", {(MATCHES, "commit"), (MATCHES, "scan_write")}),
    ("CREATE TABLE bronze.fotmob_matches AS SELECT * FROM fotmob_ingest_manifest", {(MATCHES, "commit"), (MANIFEST, "scan_write")}),
    ("ALTER TABLE iceberg.bronze.fotmob_matches EXECUTE optimize", {(MATCHES, "commit")}),
    ('SELECT * FROM "ICEBERG"."BRONZE"."FOTMOB_MATCHES"', {(MATCHES, "scan_read")}),
    ("EXPLAIN ANALYZE SELECT * FROM fotmob_matches", {(MATCHES, "scan_read")}),
    ("SELECT * FROM fotmob_matches a JOIN fotmob_matches b ON a.id=b.id", {(MATCHES, "scan_read")}),
])
def test_parser_operations(sql, operations):
    parsed = meter.parse_sql(sql, TABLES)
    assert not parsed.unparsed
    assert set(parsed.operations) == operations


def test_comments_literals_and_quoted_keywords():
    parsed = meter.parse_sql("/* leading /* nested */ comment */ -- ignored\n SELECT 'FROM fotmob_ingest_manifest; it''s text', \"from\" FROM fotmob_matches -- JOIN fotmob_squad_snapshots", TABLES)
    assert not parsed.unparsed
    assert parsed.operations == ((MATCHES, "scan_read"),)


def test_cte_shadow_and_real_table_inside_definition():
    parsed = meter.parse_sql("WITH fotmob_matches AS (SELECT * FROM fotmob_ingest_manifest) SELECT * FROM fotmob_matches", TABLES)
    assert parsed.operations == ((MANIFEST, "scan_read"),)
    own_definition = meter.parse_sql("WITH fotmob_matches AS (SELECT * FROM fotmob_matches) SELECT * FROM fotmob_matches", TABLES)
    assert own_definition.operations == ((MATCHES, "scan_read"),)
    chained = meter.parse_sql("WITH first_cte AS (SELECT * FROM fotmob_matches), second_cte AS (SELECT * FROM first_cte) SELECT * FROM second_cte", TABLES)
    assert chained.operations == ((MATCHES, "scan_read"),)


def test_nested_cte_does_not_shadow_outer_real_table():
    parsed = meter.parse_sql("SELECT * FROM (WITH fotmob_matches AS (SELECT * FROM fotmob_ingest_manifest) SELECT * FROM fotmob_matches) nested JOIN fotmob_matches real_table ON true", TABLES)
    assert not parsed.unparsed
    assert set(parsed.operations) == {(MATCHES, "scan_read"), (MANIFEST, "scan_read")}


@pytest.mark.parametrize("sql", [
    "CALL system.flush_metadata_cache('fotmob_matches')",
    "SELECT * FROM fotmob_matches; SELECT 1",
    "SELECT * FROM fotmob_matches WHERE x='unterminated",
    "SELECT * FROM fotmob_matches /* unterminated",
    "SELECT * FROM fotmob_matches, fotmob_ingest_manifest",
    "SELECT * FROM UNNEST(fotmob_matches)",
    "CREATE TABLE fotmob_matches (id bigint)",
    "CREATE TABLE IF NOT EXISTS fotmob_matches AS SELECT 1",
    "WITH RECURSIVE fotmob_matches AS (SELECT 1) SELECT * FROM fotmob_matches",
    "EXPLAIN ANALYZE INSERT INTO fotmob_matches VALUES (1)",
])
def test_unknown_grammar_is_never_guessed(sql):
    parsed = meter.parse_sql(sql, TABLES)
    assert parsed.unparsed and parsed.unparsed_fotmob
    assert parsed.operations == ()


def test_one_terminator_is_not_multiple_statements():
    parsed = meter.parse_sql("SELECT * FROM fotmob_matches; -- final comment", TABLES)
    assert not parsed.unparsed
    assert parsed.operations == ((MATCHES, "scan_read"),)


def test_name_resolution_and_non_source_sql():
    for sql in ("SELECT * FROM ordinary_table", "SELECT * FROM other.bronze.fotmob_matches", "SELECT * FROM iceberg.silver.fotmob_matches", "SELECT * FROM fotmobX", "SELECT 1"):
        parsed = meter.parse_sql(sql, TABLES)
        assert not parsed.unparsed
        assert not parsed.operations
    assert not meter.parse_sql("SELECT * FROM fotmob_new", TABLES).operations
    assert meter.parse_sql("SELECT * FROM bronze.fotmob_new", TABLES).operations == (("iceberg.bronze.fotmob_new", "scan_read"),)


def test_producer_self_exclusion_by_sql_content():
    queries = [meter.S1_SQL.format(t_from="2026-11-01 00:00:00", t_to="2026-11-01 00:02:00"), meter.S2_SQL,
               meter.S4_SQL.format(query_id="synthetic_query"), meter.S5_SQL, meter.S6_SQL, meter.snapshot_sql(MATCHES, DAY)]
    for query in queries:
        parsed = meter.parse_sql(query, TABLES)
        assert not parsed.unparsed
        assert parsed.operations == ()
    _, report = report_for({"queries": {0: [(query, "src_fotmob") for query in queries]}})
    assert report["pass"]
    assert all(row["commits"] == row["scans_total"] == 0 for row in report["tables"])


@pytest.mark.parametrize(("ended", "started", "created", "minute"), [
    ("2026-11-01T12:00:59.999Z", "2026-11-01T11:59:00Z", "2026-11-01T11:58:00Z", "2026-11-01T12:00:00Z"),
    ("2026-11-01T12:01:00.000Z", None, "2026-11-01T11:58:00Z", "2026-11-01T12:01:00Z"),
    (None, "2026-11-01T12:02:01Z", "2026-11-01T11:58:00Z", "2026-11-01T12:02:00Z"),
    (None, None, "2026-11-01T12:03:01Z", "2026-11-01T12:03:00Z"),
])
def test_event_minute_coalesce(ended, started, created, minute):
    event = snapshot("coalesce", READ, created)
    event.update(ended=ended, started=started, created=created)
    rows, _ = meter.aggregate_day([*build_day(), event], DAY, snapshot_counts={})
    assert [(row["minute_utc"], row["count"]) for row in rows if row["table"] == MATCHES] == [(minute, 1)]


@pytest.mark.parametrize("timestamp", ["2026-11-01T00:00:00", "2026-11-01T00:00:00+02:00"])
def test_raw_timestamps_must_be_explicit_utc(timestamp):
    with pytest.raises(ValueError):
        meter.minute_utc(timestamp)


def test_quiet_full_day_pass():
    _, report = report_for(quiet_spec(), {MATCHES: 2})
    assert report["pass"] and report["pass_reasons"] == []
    assert report["coverage_minutes_total"] == 1440
    assert report["gap_intervals_total"] == []
    assert report["completeness"] == "full"
    assert table_row(report)["commits"] == 2
    assert table_row(report)["scan_writes"] == 2
    assert table_row(report)["scan_reads"] == 1


@pytest.mark.parametrize("manifest_commits", [0, 8])
def test_n1_two_commits_in_one_minute(manifest_commits):
    queries = {
        0: [(f"INSERT INTO {MANIFEST} VALUES (1)", "src_fotmob")] * manifest_commits,
        15: [(INSERT, "src_fotmob")] * 2,
    }
    _, report = report_for({"queries": queries}, {MATCHES: 2, MANIFEST: manifest_commits})
    assert table_row(report, MANIFEST)["peak_commits_per_min"] == manifest_commits
    assert report["pass_reasons"] == ["N1"]
    assert report["verdicts"]["N1"]["value"] == 2
    assert report["verdicts"]["N1"]["threshold"] == "<=1/min"
    assert report["verdicts"]["N1"]["evidence"] == MATCHES + "@2026-11-01T00:15:00Z"
    assert all(report["verdicts"][key]["pass"] for key in ("N2", "N3", "N4", "N5"))


def test_n2_only_read_scans_of_poll_eligible_tables():
    queries = {
        0: [(f"SELECT * FROM {SQUADS}", "src_fotmob")] * 2,
        1: [(POLL, "src_fotmob")] * 3,
    }
    _, report = report_for({"queries": queries})
    assert table_row(report, MANIFEST)["peak_polls_per_min"] == 3
    assert report["pass_reasons"] == ["N2"]
    assert report["verdicts"]["N2"] == {
        "pass": False, "value": 2, "threshold": "<=1/min", "evidence": SQUADS + "@2026-11-01T00:00:00Z",
    }
    _, other = report_for({"queries": {0: [(READ, "src_fotmob")] * 2}})
    assert other["verdicts"]["N2"]["value"] == 0
    assert other["pass"]
    _, write_scan = report_for({"queries": {0: [("INSERT INTO fotmob_matches SELECT * FROM fotmob_ingest_manifest", "src_fotmob")] * 2}}, {MATCHES: 2})
    assert table_row(write_scan, MANIFEST)["scan_writes"] == 2
    assert write_scan["verdicts"]["N2"]["value"] == 0


@pytest.mark.parametrize(("commit_peak", "poll_peak", "scans", "commits", "failure"), [
    (8, 102, 4761, 1440, None),  # Observed minute peaks and scan total on 18 November.
    (8, 130, 5760, 1440, None),
    (9, 102, 4761, 1440, "N1"),
    (8, 131, 4761, 1440, "N2"),
    (8, 102, 4761, 1441, "N3"),
    (8, 102, 5761, 1440, "N4"),
])
def test_manifest_thresholds_on_saved_day_rows(commit_peak, poll_peak, scans, commits, failure):
    queries = {0: [(f"INSERT INTO {MANIFEST} VALUES (1)", "src_fotmob"), (POLL, "src_fotmob")]}
    _, report = report_for({"queries": queries}, {MANIFEST: 1})
    table_row(report, MANIFEST).update(
        commits=commits, peak_commits_per_min=commit_peak, peak_polls_per_min=poll_peak,
        scan_reads=scans, scans_total=scans,
    )
    report["commit_checks"][0].update(sql_commits=commits, snapshot_commits=commits)
    checked = meter.evaluate_day(report)
    assert checked["pass"] == (failure is None)
    assert checked["pass_reasons"] == ([] if failure is None else [failure])
    peak = MANIFEST + "@2026-11-01T00:00:00Z"
    for norm, value, threshold, evidence in (
        ("N1", commit_peak, "<=8/min (control)", peak),
        ("N2", poll_peak, "<=130/min (control)", peak),
        ("N3", commits, "<=1440/day", MANIFEST),
        ("N4", scans, "<=5760/day (control)", MANIFEST),
    ):
        assert checked["verdicts"][norm] == {
            "pass": norm != failure, "value": value, "threshold": threshold, "evidence": evidence,
        }


@pytest.mark.parametrize(("table", "peak", "failure"), [
    ("iceberg.bronze.fotmob_competition_seasons", 5, None),  # Observed 18 November.
    ("iceberg.bronze.fotmob_competition_scope_observations", 4, None),  # Observed 19 November.
    ("iceberg.bronze.fotmob_team_snapshots", 5, None),
    ("iceberg.bronze.fotmob_transfer_events", 2, None),
    ("iceberg.bronze.fotmob_competition_seasons", 9, "N1"),
    # Data tables keep the regular limit even while journals pass.
    ("iceberg.bronze.fotmob_matches", 2, "N1"),
    ("iceberg.bronze.fotmob_match_payloads", 2, "N1"),
])
def test_n1_wave_journal_tables_get_control_limit_data_tables_do_not(table, peak, failure):
    _, report = report_for({"queries": {0: [(f"INSERT INTO {table} VALUES (1)", "src_fotmob")]}}, {table: 1})
    table_row(report, table).update(commits=peak, peak_commits_per_min=peak, peak_polls_per_min=0,
                                    scan_reads=0, scans_total=0)
    for check in report["commit_checks"]:
        check.update(sql_commits=peak, snapshot_commits=peak)
    checked = meter.evaluate_day(report)
    verdict = checked["verdicts"]["N1"]
    assert verdict["pass"] == (failure is None)
    assert verdict["value"] == peak
    assert verdict["threshold"] == ("<=%d/min (control)" % meter.CONTROL_N1_MAX_COMMITS_PER_MIN
                                    if table in meter.WAVE_JOURNAL_TABLES else "<=1/min")
    assert checked["pass_reasons"] == ([] if failure is None else ["N1"])


def test_boundary_1440_commits_and_scans_with_one_per_minute_pass():
    _, report = report_for({"each_minute": [(UPDATE, "src_fotmob"), (POLL, "src_fotmob")]}, {MATCHES: 1440})
    assert report["pass"]
    assert [report["verdicts"][key]["value"] for key in ("N1", "N2", "N3", "N4")] == [1, 1, 1440, 1440]


def test_n3_1441_commits_necessarily_also_violates_n1():
    # 1441 events in 1440 minute buckets cannot all have at most one event.
    _, report = report_for({"each_minute": [(INSERT, "src_fotmob")], "queries": {1439: [(INSERT, "src_fotmob")]}}, {MATCHES: 1441})
    assert report["pass_reasons"] == ["N1", "N3"]
    assert report["verdicts"]["N3"]["value"] == 1441
    assert report["verdicts"]["N3"]["evidence"] == MATCHES


def test_n4_1441_combines_read_and_write_scans():
    _, report = report_for({"each_minute": [(UPDATE, "src_fotmob")], "queries": {0: [(READ, "src_fotmob")]}}, {MATCHES: 1440})
    assert report["pass_reasons"] == ["N4"]
    assert report["verdicts"]["N4"]["value"] == 1441


def test_producer_down_interval_is_not_zero():
    rows, report = report_for({"gaps": set(range(130, 160))})
    assert report["pass_reasons"] == ["gap"]
    assert report["coverage_minutes_total"] == 1410
    assert report["gap_intervals_total"] == [{"start": "2026-11-01T02:10:00Z", "end": "2026-11-01T02:40:00Z", "reason": "producer_down"}]
    gaps = [row for row in rows if row["coverage"] == "gap"]
    assert len(gaps) == 30
    assert all(row["table"] is None and row["count"] is None for row in gaps)
    assert all(row["count"] != 0 for row in rows)
    assert all(row["coverage_minutes"] == 1410 and row["completeness"] == "gap" for row in report["tables"])
    assert "gaps=1(producer_down:02:10-02:40)" in meter.summary_line(report)
    assert "verdict=NOT_PASS(gap)" in meter.summary_line(report)


def test_history_loss_and_partial_windows_are_gaps():
    _, report = report_for({"history_lost": {10}, "partial": {20}})
    assert report["coverage_minutes_total"] == 1438
    assert [gap["reason"] for gap in report["gap_intervals_total"]] == ["history_lost", "no_sample"]


def test_partial_windows_can_jointly_cover_a_minute_and_good_poll_overrides_loss():
    records = list(build_day({"partial": {20}, "history_lost": {21}}))
    second = copy.deepcopy(records[20])
    second.update(poll_id="second_half", window_start="2026-11-01T00:20:00Z", window_end="2026-11-01T00:20:30Z")
    recovered = copy.deepcopy(records[21])
    recovered.update(poll_id="recovered", watermark_ok=True)
    _, report = meter.aggregate_day([*records, second, recovered], DAY, snapshot_counts={})
    assert report["pass"]


def test_missing_visibility_blocks_a_full_quiet_day():
    _, report = report_for({"n5_measurable": False})
    assert report["coverage_minutes_total"] == 1440
    assert report["pass_reasons"] == ["n5_unmeasurable"]
    assert report["verdicts"]["N5"]["pass"]


def test_duplicate_fixture_first_record_wins_and_conflict_is_marked():
    records = meter.read_raw([FIXTURES / "snapshot_overlap_duplicates.jsonl"])
    rows, report = meter.aggregate_day([*build_day(), *records], DAY, snapshot_counts={MATCHES: 1})
    assert report["duplicate_queries"] == 3
    assert report["conflicting_query_ids"] == ["duplicate_write"]
    assert table_row(report)["commits"] == 1
    assert table_row(report)["scan_writes"] == 0  # conflicting UPDATE was second
    assert table_row(report, MANIFEST)["scan_reads"] == 1
    assert report["pass"]
    commit = next(row for row in rows if row["operation"] == "commit")
    assert commit["source_polls"] == ["overlap_1"]


def test_files_are_read_in_filename_order(tmp_path):
    first = snapshot("same", INSERT, "2026-11-01T00:00:10Z")
    second = snapshot("same", UPDATE, "2026-11-01T00:00:10Z")
    earlier, later = tmp_path / "20261101-0001.jsonl", tmp_path / "20261101-0002.jsonl"
    earlier.write_text(json.dumps(first) + "\n", encoding="utf-8")
    later.write_text(json.dumps(second) + "\n", encoding="utf-8")
    _, report = meter.aggregate_day([*build_day(), *meter.read_raw([later, earlier])], DAY, snapshot_counts={MATCHES: 1})
    assert table_row(report)["scan_writes"] == 0
    assert report["conflicting_query_ids"] == ["same"]


def test_unparsed_and_foreign_fixture_preserves_source_attribution():
    records = meter.read_raw([FIXTURES / "unparsed_and_foreign.jsonl"])
    _, report = meter.aggregate_day([*build_day(), *records], DAY, snapshot_counts={MATCHES: 2})
    assert report["unparsed_queries"] == 2
    assert report["unparsed_fotmob"]
    assert report["foreign_load"]
    assert report["platform_ops_total"] == 2
    assert report["unknown_ops_total"] == 1
    assert table_row(report)["commits"] == 1
    assert table_row(report)["scans_total"] == 0
    assert table_row(report, attr="platform")["commits"] == 1
    assert table_row(report, attr="unknown")["scan_reads"] == 1
    assert len(report["platform_activity"]) == len(report["unknown_activity"]) == 1
    assert report["pass_reasons"] == ["N5", "unparsed_fotmob"]
    assert report["commit_checks"][0]["sql_commits"] == 2


def test_unparsed_sql_without_source_name_does_not_block_pass():
    _, report = report_for({"queries": {0: [("CALL system.flush_metadata_cache()", "src_fotmob")]}})
    assert report["unparsed_queries"] == 1
    assert not report["unparsed_fotmob"]
    assert report["pass"]


@pytest.mark.parametrize("sql", [
    EXPIRE_SNAPSHOTS,
    f"ALTER TABLE {MATCHES} EXECUTE optimize()",
    f"ALTER TABLE {MATCHES} EXECUTE remove_orphan_files(retention_threshold => '3d')",
])
def test_n5_deducts_platform_procedure_operations(sql):
    parsed = meter.parse_sql(sql, TABLES)
    assert not parsed.unparsed and parsed.stmt_type == "alter_write"
    records = list(build_day({"queries": {0: [(sql, "airflow")] * 2}}))
    first = next(row for row in records if row["grain"] == "snapshot")
    first.update(io_output_tables=[MATCHES], io_input_tables=[MATCHES])
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 2})
    assert report["maintenance_procedure_ops"] == {"platform": [[MATCHES, "commit", 2], [MATCHES, "scan_write", 1]]}
    assert report["maintenance_procedure_ops_total"] == report["platform_ops_total"] == 3
    assert report["blocking_ops_total"] == report["verdicts"]["N5"]["value"] == 0
    assert report["pass"] and report["foreign_load"]


@pytest.mark.parametrize(("sql", "user", "attr", "ops"), [
    (EXPIRE_SNAPSHOTS, "other", "unknown", 1),
    (INSERT, "airflow", "platform", 1),
    (f"MERGE INTO {MATCHES} t USING {MANIFEST} s ON t.id=s.id WHEN MATCHED THEN UPDATE SET x=1", "airflow", "platform", 2),
    (f"CREATE TABLE {MATCHES} AS SELECT * FROM {MANIFEST}", "airflow", "platform", 2),
    (f"ALTER TABLE {MATCHES} EXECUTE optimize", "airflow", "platform", 1),
])
def test_n5_foreign_writes_without_procedure_deductions(sql, user, attr, ops):
    _, report = report_for({"queries": {0: [(sql, user)]}}, {MATCHES: 1})
    assert report["maintenance_procedure_ops"] == {"platform": []}
    assert report["maintenance_procedure_ops_total"] == 0
    assert report[f"{attr}_ops_total"] == report["blocking_ops_total"] == ops
    assert report["pass_reasons"] == ["N5"] and report["verdicts"]["N5"]["value"] == ops


def test_n5_deducts_only_the_platform_procedure_on_a_mixed_day():
    queries = {0: [(EXPIRE_SNAPSHOTS, "airflow"), (EXPIRE_SNAPSHOTS, "other"), (INSERT, "airflow")]}
    _, report = report_for({"queries": queries}, {MATCHES: 3})
    assert (report["platform_ops_total"], report["unknown_ops_total"]) == (2, 1)
    assert report["maintenance_procedure_ops"] == {"platform": [[MATCHES, "commit", 1]]}
    assert report["maintenance_procedure_ops_total"] == 1
    assert report["pass_reasons"] == ["N5"]
    assert report["blocking_ops_total"] == report["verdicts"]["N5"]["value"] == 2


def test_unparsed_platform_procedure_sql_blocks_even_with_a_write_observation():
    sql = EXPIRE_SNAPSHOTS + "; SELECT 1"
    records = list(build_day({"queries": {0: [(sql, "airflow")]}}))
    query = next(row for row in records if row["grain"] == "snapshot")
    query.update(stmt_type="alter_write", io_output_tables=[MATCHES], io_input_tables=[])
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 1})
    # An unparsed statement never qualifies for the procedure exemption, and
    # per the #2346 contract it contributes no attributed operations either:
    # the day is blocked by unparsed_fotmob itself, not by an N5 count.
    assert report["unparsed_queries"] == 1 and report["unparsed_fotmob"]
    assert report["pass_reasons"] == ["unparsed_fotmob"]
    assert report["maintenance_procedure_ops"] == {"platform": []}
    assert report["maintenance_procedure_ops_total"] == 0
    assert report["platform_ops_total"] == 0 and not report["pass"]


def test_saved_day_without_procedure_counts_deducts_zero():
    _, report = report_for({"queries": {0: [(EXPIRE_SNAPSHOTS, "airflow")]}}, {MATCHES: 1})
    del report["maintenance_procedure_ops"]
    del report["maintenance_procedure_ops_total"]
    checked = meter.evaluate_day(report)
    assert checked["maintenance_procedure_ops_total"] == 0
    assert checked["blocking_ops_total"] == checked["platform_ops_total"] == 1
    assert checked["pass_reasons"] == ["N5"]


@pytest.mark.parametrize(("count", "reason"), [(0, "commit_mismatch"), (2, "commit_mismatch"), (None, "commit_check_missing")])
def test_daily_commit_reconciliation_is_mandatory(count, reason):
    counts = {} if count is None else {MATCHES: count}
    _, report = report_for({"queries": {0: [(INSERT, "src_fotmob")]}}, counts)
    assert report["pass_reasons"] == [reason]
    check = report["commit_checks"][0]
    assert check["sql_commits"] == 1 and check["snapshot_commits"] == count
    assert check["difference"] == (None if count is None else count - 1)
    assert report["commit_mismatch"] == (count is not None)


@pytest.mark.parametrize(("name", "expected"), [
    ("fotmob_matches__stg_abc123", True),
    ("iceberg.bronze.fotmob_matches__stg_abc123", True),
    ("fotmob_c1_inputs__stg_0123456789abcdef0123456789abcdef", True),
    ("iceberg.bronze.fotmob_c1_inputs__stg_0123456789abcdef0123456789abcdef", True),
    ("fotmob_matches", False),
    ("fotmob_c1_inputs", False),
    ("fotmob_matches__stg_abcdefg", False),
    # Table identity is case-insensitive, like is_fotmob_io_table: an
    # uppercase suffix is the same staging table Trino would fold to lowercase.
    ("fotmob_matches__stg_ABCDEF", True),
    ("fotmob_matches__stg_abcde", False),
    ("iceberg.silver.fotmob_matches__stg_abc123", False),
])
def test_is_ephemeral_stage(name, expected):
    assert meter.is_ephemeral_stage(name) is expected


def test_commit_reconciliation_checks_only_persistent_write_targets():
    inputs = "iceberg.bronze.fotmob_c1_inputs"
    suffix = "0123456789abcdef0123456789abcdef"
    inputs_stage = f"{inputs}__stg_{suffix}"
    stages = [inputs_stage, f"iceberg.bronze.fotmob_c1_candidates__stg_{suffix}", MATCHES + "__stg_0123456789ab"]
    merge = (f"MERGE INTO {inputs} t USING {inputs_stage} s ON t.id=s.id "
             "WHEN MATCHED THEN UPDATE SET x=1")
    queries = {0: [], 1: [(merge, "src_fotmob")], 2: [(INSERT, "src_fotmob")]}
    for stage in stages:
        queries[0].extend([
            (f"CREATE TABLE {stage} AS SELECT 1 AS id", "src_fotmob"),
            (f"INSERT INTO {stage} VALUES (2)", "src_fotmob"),
        ])
    _, report = report_for({"tables": [inputs, MATCHES, *stages], "queries": queries}, {inputs: 1, MATCHES: 1})
    assert report["pass"] and report["commit_check_complete"] and not report["commit_mismatch"]
    assert [(row["table"], row["sql_commits"], row["snapshot_commits"]) for row in report["commit_checks"]] == [
        (inputs, 1, 1), (MATCHES, 1, 1),
    ]
    assert set(stages) <= set(report["discovered_tables"])
    assert all(table_row(report, stage)["commits"] == 2 for stage in stages)
    assert report["peak_evidence"]["N1"] == inputs + "@2026-11-01T00:01:00Z"


def test_ephemeral_only_day_has_no_commit_checks_or_source_norm_candidates():
    stage = SQUADS + "__stg_abc123"
    write = (f"UPDATE {stage} SET x=1", "src_fotmob")
    queries = {0: [write, write, (f"SELECT * FROM {stage}", "src_fotmob")]}
    _, report = report_for({"tables": [stage], "queries": queries})
    row = table_row(report, stage)
    assert (row["commits"], row["scan_writes"], row["scan_reads"]) == (2, 2, 1)
    assert report["commit_checks"] == [] and report["commit_check_complete"] is True
    assert report["pass"]
    assert report["peak_evidence"] == {"N1": None, "N2": None}
    for norm in ("N1", "N2", "N3", "N4"):
        assert report["verdicts"][norm]["value"] == 0
        assert report["verdicts"][norm]["evidence"] is None


def test_ephemeral_writes_keep_platform_and_unknown_attribution():
    stage = MATCHES + "__stg_0123456789ab"
    sql = f"INSERT INTO {stage} VALUES (1)"
    _, report = report_for({"tables": [stage], "queries": {0: [(sql, "airflow"), (sql, "other")]}})
    assert table_row(report, stage, "platform")["commits"] == report["platform_ops_total"] == 1
    assert table_row(report, stage, "unknown")["commits"] == report["unknown_ops_total"] == 1
    assert report["pass_reasons"] == ["N5"] and report["blocking_ops_total"] == 2


def test_snapshot_counts_are_strict_nonnegative_integers():
    for count in (True, -1, 1.0, "1"):
        with pytest.raises(ValueError):
            report_for({"queries": {0: [(INSERT, "src_fotmob")]}}, {MATCHES: count})


def test_verdict_rechecks_saved_counts_instead_of_trusting_pass_flags():
    _, report = report_for({"queries": {0: [(INSERT, "airflow")]}}, {MATCHES: 2})
    report.update(platform_ops_total=0, foreign_load=False, commit_mismatch=False)
    report["pass"] = True
    report["commit_checks"][0].update(match=True, difference=0)
    checked = meter.evaluate_day(report)
    assert checked["pass_reasons"] == ["N5", "commit_mismatch"]
    assert checked["platform_ops_total"] == 1
    assert checked["commit_checks"][0]["difference"] == 1


def test_discovery_adds_new_tables_and_escapes_underscore():
    new_table = "iceberg.bronze.fotmob_new_events"
    _, report = report_for({"new_tables": {10: [new_table, "fotmobX"]}, "queries": {11: [("SELECT * FROM fotmob_new_events", "src_fotmob")]}})
    assert new_table in report["discovered_tables"]
    assert table_row(report, new_table)["scan_reads"] == 1
    assert not any("fotmobX" in table for table in report["discovered_tables"])
    assert r"fotmob\_%" in meter.S2_SQL and "ESCAPE" in meter.S2_SQL
    assert meter.discovered_tables(["fotmob_matches", "fotmobX", "fotmob_"]) == [MATCHES]


def test_day_boundaries_do_not_count_following_day():
    records = [*build_day(), snapshot("before", READ, "2026-10-31T23:59:59.999Z"),
               snapshot("last", READ, "2026-11-01T23:59:59.999Z"), snapshot("after", READ, "2026-11-02T00:00:00Z")]
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={})
    assert table_row(report)["scan_reads"] == 1


def test_verdict_deterministic():
    records = list(build_day(quiet_spec()))
    first = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 2})
    second = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 2})
    assert meter.json_text(first) == meter.json_text(second)
    assert meter.json_text(meter.evaluate_day(first[1])) == meter.json_text(first[1])


def test_day_report_template_and_generated_schema():
    template = json.loads((FIXTURES / "day_report_template.json").read_text(encoding="utf-8"))
    _, generated = report_for(quiet_spec(), {MATCHES: 2})
    fields = {"grain", "day_utc", "generated_at", "producer_variant", "poll_interval_s", "coverage_minutes_total", "gap_intervals_total", "completeness", "discovered_tables", "unparsed_queries", "unparsed_fotmob", "n5_measurable", "platform_ops_total", "unknown_ops_total", "foreign_load", "tables", "platform_activity", "unknown_activity", "verdicts", "pass", "pass_reasons", "commit_checks", "commit_check_complete", "commit_mismatch"}
    for report in (template, generated):
        assert fields <= report.keys()
        assert report["grain"] == "day_report" and report["day_utc"] == DAY
        assert report["pass"] is True and report["pass_reasons"] == []
        assert report["coverage_minutes_total"] == 1440 and report["completeness"] == "full"
        assert meter.utc_datetime(report["generated_at"])
        assert set(report["verdicts"]) == {"N1", "N2", "N3", "N4", "N5"}
        for row in report["tables"]:
            assert row["grain"] == "day_table"
            assert row["user_attr"] in {"src_fotmob", "platform", "unknown"}
            assert row["scans_total"] == row["scan_reads"] + row["scan_writes"]
            assert all(type(row[key]) is int and row[key] >= 0 for key in ("commits", "scan_reads", "scan_writes", "scans_total", "coverage_minutes", "peak_commits_per_min", "peak_polls_per_min"))
        assert meter.evaluate_day(report)["pass"]


def test_snapshot_query_is_read_only_and_window_is_half_open():
    sql = meter.snapshot_sql(MATCHES, DAY)
    assert sql.startswith("SELECT count(*)")
    assert '"fotmob_matches$snapshots"' in sql
    assert ">= TIMESTAMP '2026-11-01 00:00:00'" in sql
    assert "< TIMESTAMP '2026-11-02 00:00:00'" in sql
    with pytest.raises(ValueError):
        meter.snapshot_sql('iceberg.bronze.fotmob_matches"; SELECT 1', DAY)


def write_raw(directory, records):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "20261101-000000-fixture.jsonl").write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


def maintenance_receipt(**changes):
    receipt = {
        "day_utc": DAY, "job": "compaction", "evidence_ref": "producer-run/17/statement/2",
        "query_id": "q_0000_0", "user": "airflow",
        "sql_sha256": hashlib.sha256(UPDATE.encode("utf-8")).hexdigest(),
        "operations": [[MATCHES, "commit"], [MATCHES, "scan_write"]],
    }
    return {**receipt, **changes}


def registry(*receipts):
    return {"day_utc": DAY, "receipts": list(receipts)}


def test_receipts_through_normal_cli(tmp_path, capsys):
    records = list(build_day({"queries": {0: [(UPDATE, "airflow")]}}))
    query = next(row for row in records if row["grain"] == "snapshot")
    records.append(dict(query, poll_id="second_poll", io_output_tables=[MATCHES], io_input_tables=[MATCHES]))
    write_raw(tmp_path / "raw", records)
    path = tmp_path / "maintenance-receipts" / f"{DAY}.json"
    path.parent.mkdir()
    path.write_text(json.dumps(registry(maintenance_receipt())), encoding="utf-8")
    counts = tmp_path / "s7.json"
    counts.write_text(json.dumps({"day_utc": DAY, "checked_at": "2026-11-02T00:05:00Z", "counts": {MATCHES: 1}}))
    common = ["--root", str(tmp_path), "--day", DAY]
    assert meter.main(["aggregate", *common, "--offline", "--snapshot-counts", str(counts)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["pass"] and report["foreign_load"]
    assert report["verdicts"]["N5"]["value"] == 0 and report["platform_ops_total"] == 2
    assert table_row(report, attr="platform")["scan_writes"] == report["commit_checks"][0]["sql_commits"] == 1
    assert report["maintenance_deductions"] == {"q_0000_0": maintenance_receipt()["operations"]}
    assert UPDATE not in json.dumps(report)
    report.update(allowed_platform_ops_total=100, blocking_ops_total=90, maintenance_deductions={}, foreign_load=False)
    (tmp_path / "day" / f"{DAY}.json").write_text(json.dumps(report))
    assert meter.main(["verdict", *common]) == 0
    checked = json.loads(capsys.readouterr().out)
    assert checked["pass"] and checked["foreign_load"] and checked["allowed_platform_ops_total"] == 2
    assert meter.main(["line", *common]) == 0
    assert "platform_ops=2 unknown_ops=0 allowed_platform_ops=2 blocking_ops=0" in capsys.readouterr().out


@pytest.mark.parametrize("job", ["compaction", "janitor", "morning_report"])
def test_receipt_only_covers_its_own_query(job):
    receipt = maintenance_receipt(job=job)
    records = list(build_day({"queries": {0: [(UPDATE, user) for user in ("airflow", "airflow", "other", "src_fotmob")]}}))
    for row in records:
        if row.get("grain") == "snapshot":
            row.update(source=job, maintenance_receipt={**receipt, "query_id": row["query_id"]})
    _, legacy = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 4})
    assert legacy["pass_reasons"] == ["N5"] and legacy["verdicts"]["N5"]["value"] == 6
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 4}, maintenance_registry=registry(receipt))
    assert report["pass_reasons"] == ["N5"] and report["foreign_load"]
    assert (report["platform_ops_total"], report["unknown_ops_total"], report["allowed_platform_ops_total"]) == (4, 2, 2)
    assert report["verdicts"]["N5"]["value"] == 4
    assert report["verdicts"]["N1"]["value"] == report["verdicts"]["N4"]["value"] == 1


@pytest.mark.parametrize("bad_registry", [
    {"day_utc": "2026-11-02", "receipts": [maintenance_receipt()]},
    registry(maintenance_receipt(day_utc="2026-11-02")),
    registry(maintenance_receipt(job="airflow")), registry(maintenance_receipt(evidence_ref=" ")),
    registry(maintenance_receipt(), maintenance_receipt()),
    registry(maintenance_receipt(), maintenance_receipt(query_id="missing")),
    registry(maintenance_receipt(sql_sha256=hashlib.sha256((UPDATE + " ").encode()).hexdigest())),
    registry(maintenance_receipt(operations=[[MATCHES, "commit"]])),
    registry(maintenance_receipt(operations=[[MATCHES, "commit"]] * 2)),
    registry(maintenance_receipt(operations=[[MATCHES, "scan_read"], [MATCHES, "commit"]])),
    registry(maintenance_receipt(query_id="q_0000_2")), registry(maintenance_receipt(user="other")),
])
def test_invalid_registry_fails_closed(bad_registry):
    spec = {"queries": {0: [(UPDATE, "airflow"), (UPDATE, "airflow"), (UPDATE, "other")]}}
    _, report = meter.aggregate_day(build_day(spec), DAY, snapshot_counts={MATCHES: 3}, maintenance_registry=bad_registry)
    assert report["pass_reasons"] == ["N5", "maintenance_evidence_invalid"]
    assert report["verdicts"]["N5"]["value"] == 6 and not report["maintenance_deductions"]
    assert meter.evaluate_day(report) == report


@pytest.mark.parametrize(("spec", "counts", "reason"), [
    ({"gaps": {40}}, {MATCHES: 1}, "gap"),
    ({"n5_measurable": False}, {MATCHES: 1}, "n5_unmeasurable"),
    ({}, {}, "commit_check_missing"), ({}, {MATCHES: 2}, "commit_mismatch"),
])
def test_allowed_work_still_requires_full_measurement(spec, counts, reason):
    records = build_day({**spec, "queries": {0: [(UPDATE, "airflow")]}})
    _, report = meter.aggregate_day(records, DAY, snapshot_counts=counts, maintenance_registry=registry(maintenance_receipt()))
    assert report["pass_reasons"] == [reason] and report["verdicts"]["N5"]["value"] == 0
    assert report["foreign_load"] and report["commit_checks"][0]["sql_commits"] == 1


@pytest.mark.parametrize("change", [{"source": "also_airflow"}, {"user": "other"}, {"query": UPDATE + " "}])
def test_conflicting_observations_cannot_match(change):
    records = list(build_day({"queries": {0: [(UPDATE, "airflow")]}}))
    query = next(row for row in records if row["grain"] == "snapshot")
    records.append({**query, **change})
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 1}, maintenance_registry=registry(maintenance_receipt()))
    assert report["conflicting_query_ids"] == ["q_0000_0"]
    assert "maintenance_evidence_invalid" in report["pass_reasons"] and report["blocking_ops_total"] == 2


def test_receipt_must_cover_physical_inputs_too():
    records = list(build_day({"queries": {0: [(UPDATE, "airflow")]}}))
    query = next(row for row in records if row["grain"] == "snapshot")
    query.update(io_output_tables=[MATCHES], io_input_tables=[SQUADS])
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 1},
                                    maintenance_registry=registry(maintenance_receipt()))
    assert report["blocking_ops_total"] == 3 and not report["maintenance_deductions"]
    assert table_row(report, SQUADS, "platform")["scan_writes"] == 1


def test_producer_receipt_cannot_resolve_unparsed_sql():
    sql = "CALL synthetic.maintenance('iceberg.bronze.fotmob_matches')"
    records = list(build_day({"queries": {0: [(sql, "airflow")]}}))
    query = next(row for row in records if row["grain"] == "snapshot")
    query["io_output_tables"] = [MATCHES]
    receipt = maintenance_receipt(sql_sha256=hashlib.sha256(sql.encode()).hexdigest())
    _, report = meter.aggregate_day(records, DAY, maintenance_registry=registry(receipt))
    assert {"unparsed_fotmob", "maintenance_evidence_invalid"} <= set(report["pass_reasons"])
    assert not report["maintenance_deductions"]


def test_verdict_rejects_overallocated_observations():
    records = build_day({"queries": {0: [(UPDATE, "airflow")]}})
    _, report = meter.aggregate_day(records, DAY, snapshot_counts={MATCHES: 1}, maintenance_registry=registry(maintenance_receipt()))
    audit = report["maintenance_evidence"]
    audit["registry"]["receipts"].append(maintenance_receipt(query_id="extra"))
    audit["observed"]["extra"] = copy.deepcopy(audit["observed"]["q_0000_0"])
    checked = meter.evaluate_day(report)
    assert checked["maintenance_error"] == "observation_counts_mismatch"
    assert checked["blocking_ops_total"] == 2 and not checked["pass"]
    assert "allowed_platform_ops=0 blocking_ops=2" in meter.summary_line(report)


@pytest.mark.parametrize("content", ["null", "{", '{"day_utc":"stale","day_utc":"2026-11-01","receipts":[]}'])
def test_unreadable_registry_publishes_diagnostic_day(tmp_path, capsys, content):
    write_raw(tmp_path / "raw", build_day())
    path = tmp_path / "maintenance-receipts" / f"{DAY}.json"
    path.parent.mkdir()
    path.write_text(content)
    assert meter.main(["aggregate", "--day", DAY, "--root", str(tmp_path), "--offline"]) == 0
    assert json.loads(capsys.readouterr().out)["pass_reasons"] == ["maintenance_evidence_invalid"]


def test_cli_aggregate_verdict_line_offline(tmp_path, capsys):
    write_raw(tmp_path / "raw", build_day(quiet_spec()))
    counts = tmp_path / "synthetic-s7.json"
    counts.write_text(json.dumps({"day_utc": DAY, "checked_at": "2026-11-02T00:05:00Z", "counts": {MATCHES: 2}}), encoding="utf-8")
    common = ["--root", str(tmp_path), "--day", DAY]
    assert meter.main(["aggregate", *common, "--offline", "--snapshot-counts", str(counts)]) == 0
    assert json.loads(capsys.readouterr().out)["pass"]
    assert meter.main(["verdict", *common]) == 0
    assert json.loads(capsys.readouterr().out)["pass"]
    assert meter.main(["line", *common]) == 0
    assert capsys.readouterr().out.strip() == "Trino fotmob: day=2026-11-01 verdict=PASS coverage=1440/1440 tables=3 user=src_fotmob commits_day_max=2 scans_day_max=3 peak_commit_min=1 peak_poll_min=1 platform_ops=0 unknown_ops=0 unparsed=0 gaps=0"
    assert (tmp_path / "minute" / f"{DAY}.jsonl").exists()
    assert (tmp_path / "day" / f"{DAY}.json").exists()
    assert (tmp_path / "verdict" / f"{DAY}.json").exists()


def test_live_daily_cli_invokes_read_only_s7_path(tmp_path, monkeypatch, capsys):
    write_raw(tmp_path / "raw", build_day(quiet_spec()))
    calls = []

    def fake_poll(root, **kwargs):
        calls.append(kwargs)
        return {"day_utc": DAY, "checked_at": "2026-11-02T00:05:00Z", "counts": {MATCHES: 3}, "errors": {}}

    monkeypatch.setattr(meter, "poll", fake_poll)
    assert meter.main(["aggregate", "--root", str(tmp_path), "--day", DAY]) == 0
    report = json.loads(capsys.readouterr().out)
    assert calls == [{"snapshot_day": DAY, "snapshot_tables": [MATCHES]}]
    assert report["pass_reasons"] == ["commit_mismatch"]
    assert meter.main(["verdict", "--root", str(tmp_path), "--day", DAY]) == 0
    capsys.readouterr()
    assert meter.main(["line", "--root", str(tmp_path), "--day", DAY]) == 0
    assert "NOT_PASS(commit_mismatch)" in capsys.readouterr().out


def test_s7_failure_is_persisted_as_not_pass(tmp_path, monkeypatch, capsys):
    write_raw(tmp_path / "raw", build_day(quiet_spec()))

    def failing_poll(*args, **kwargs):
        raise RuntimeError("synthetic-sensitive-error-text")

    monkeypatch.setattr(meter, "poll", failing_poll)
    assert meter.main(["aggregate", "--root", str(tmp_path), "--day", DAY]) == 0
    output = capsys.readouterr()
    assert "synthetic-sensitive-error-text" not in output.out + output.err
    assert json.loads(output.out)["pass_reasons"] == ["commit_check_missing"]


@pytest.mark.parametrize("command", ["poll", "discover"])
def test_poll_and_discover_require_password_name_only(command, monkeypatch, capsys):
    monkeypatch.delenv(meter.PASSWORD_ENV, raising=False)
    assert meter.main([command]) == 1
    output = capsys.readouterr()
    assert meter.PASSWORD_ENV in output.err
    assert not output.out


def test_poll_password_contract_survives_unreadable_paths(monkeypatch, capsys):
    # CI reproduces this without stubs: Path.exists() raises PermissionError
    # (EACCES) on unreadable parents on Python <=3.11; the env diagnostics must
    # still win over any filesystem failure.
    from pathlib import Path as _Path

    def raising_exists(self):
        raise PermissionError("EACCES: unreadable parent")

    monkeypatch.delenv(meter.PASSWORD_ENV, raising=False)
    monkeypatch.setattr(_Path, "exists", raising_exists)
    assert meter.main(["poll"]) == 1
    output = capsys.readouterr()
    assert meter.PASSWORD_ENV in output.err
    assert not output.out


def test_qualify_reports_all_read_checks_when_environment_is_missing(monkeypatch, capsys):
    monkeypatch.delenv(meter.PASSWORD_ENV, raising=False)
    assert meter.main(["qualify"]) == 2
    report = json.loads(capsys.readouterr().out)
    assert set(report["checks"]) == {"K0", "K1", "K2", "K3"}
    assert not report["pass"]
    assert not any(check["pass"] for check in report["checks"].values())


def test_import_under_empty_environment_without_trino(tmp_path):
    script = """
import importlib.abc
import sys
class NoTrino(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'trino' or fullname.startswith('trino.'):
            raise AssertionError('eager Trino import')
sys.meta_path.insert(0, NoTrino())
from scrapers.fotmob import load_meter
assert load_meter.N1_MAX_COMMITS_PER_MIN == 1
assert load_meter.parse_sql('SELECT 1', []).operations == ()
"""
    result = subprocess.run([sys.executable, "-c", script], cwd=tmp_path, env={"PYTHONPATH": str(REPOSITORY), "PYTHONDONTWRITEBYTECODE": "1"}, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr


def fake_trino(monkeypatch, responder):
    statements = []

    class Cursor:
        query_id = "20261102_000000_synthetic"

        def execute(self, sql):
            statements.append(sql)
            assert sql.lstrip().startswith(("SELECT", "DESCRIBE"))
            rows = responder(sql)
            self.description = [(key,) for key in rows[0]] if rows else [("query_id",)]
            self.values = [tuple(row.values()) for row in rows]

        def fetchall(self):
            return self.values

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    module = types.ModuleType("trino")
    module.__path__ = []
    module.auth = types.ModuleType("trino.auth")
    module.auth.BasicAuthentication = lambda user, password: object()
    module.dbapi = types.ModuleType("trino.dbapi")
    module.dbapi.connect = lambda **kwargs: Connection()
    monkeypatch.setitem(sys.modules, "trino", module)
    monkeypatch.setitem(sys.modules, "trino.auth", module.auth)
    monkeypatch.setitem(sys.modules, "trino.dbapi", module.dbapi)
    monkeypatch.setenv(meter.PASSWORD_ENV, "synthetic-test-value")
    return statements


def test_connect_matches_platform_self_signed_contract(monkeypatch):
    captured = {}

    class Connection:
        def close(self):
            pass

    def connect(**kwargs):
        captured.update(kwargs)
        return Connection()

    module = types.ModuleType("trino")
    module.__path__ = []
    module.auth = types.ModuleType("trino.auth")
    module.auth.BasicAuthentication = lambda user, password: object()
    module.dbapi = types.ModuleType("trino.dbapi")
    module.dbapi.connect = connect
    monkeypatch.setitem(sys.modules, "trino", module)
    monkeypatch.setitem(sys.modules, "trino.auth", module.auth)
    monkeypatch.setitem(sys.modules, "trino.dbapi", module.dbapi)
    monkeypatch.setenv("TRINO_HTTP_SCHEME", "https")
    monkeypatch.setenv(meter.PASSWORD_ENV, "synthetic-test-value")
    meter._connect(module, "synthetic-test-value").close()
    # The shared coordinator serves a self-signed certificate; every platform
    # client (scrapers/base/trino_manager.py, scrapers/fotmob/trino_instrumentation.py)
    # disables verification, and the meter must match or every poll fails.
    assert captured["verify"] is False
    assert captured["http_scheme"] == "https"
    assert captured["user"] == meter.SOURCE_USER
    assert captured["request_timeout"] == 10 and captured["max_attempts"] == 1


def test_qualify_only_k0_k3_and_no_writes(monkeypatch):
    now = [meter.utc_datetime("2026-11-02T00:00:00Z")]

    def responder(sql):
        if sql == "SELECT current_timestamp AS now_utc":
            return [{"now_utc": now[0]}]
        if sql.startswith("DESCRIBE"):
            return [{"Column": name} for name in ("query_id", "user", "source", "query", "state", "created", "started", "end", "error_code")]
        if sql == meter.S6_SQL:
            return [{"user": "src_fotmob", "n": 2}, {"user": "airflow", "n": 1}]
        if sql == meter.S5_SQL:
            return [{"oldest": now[0] - timedelta(minutes=10), "newest": now[0], "n": 10}]
        return []

    statements = fake_trino(monkeypatch, responder)
    previous = None
    for index in range(3):
        now[0] = meter.utc_datetime("2026-11-02T00:00:00Z") + timedelta(minutes=index * 30)
        previous = meter.qualify(previous=previous, history_limit=100, peak=True, slow_log_verified=True, slow_log_threshold_s=5)
    assert set(previous["checks"]) == {"K0", "K1", "K2", "K3"}
    assert previous["pass"]
    assert len(statements) == 15
    assert not any("$snapshots" in sql for sql in statements)
    assert "synthetic-test-value" not in meter.json_text(previous)


def test_poll_uses_previous_watermark_and_writes_only_local_evidence(tmp_path, monkeypatch):
    now = meter.utc_datetime("2026-11-02T00:01:00Z")

    def responder(sql):
        if sql == "SELECT current_timestamp AS now_utc":
            return [{"now_utc": now}]
        if sql == meter.S2_SQL:
            return [{"table_name": "fotmob_matches"}, {"table_name": "fotmobX"}]
        if sql.startswith("SELECT query_id, state"):
            return [{"query_id": "previous_synthetic", "state": "FINISHED"}]
        return []

    statements = fake_trino(monkeypatch, responder)
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "producer-state.json").write_text(json.dumps({"query_id": "previous_synthetic", "polled_at": "2026-11-02T00:00:00Z"}), encoding="utf-8")
    qualification = {"poll_interval_s": 60, "retention_s": 600, "checks": {key: {"pass": True} for key in ("K0", "K1", "K2", "K3")}}
    result = meter.poll(tmp_path, qualification=qualification)
    assert result["watermark_ok"] and result["n5_measurable"]
    records = meter.read_raw((tmp_path / "raw").glob("*.jsonl"))
    assert len(records) == 1
    assert records[0]["discovered_tables"] == [MATCHES]
    assert records[0]["window_start"] == "2026-11-01T23:59:00Z"
    assert len(statements) == 4
    assert "synthetic-test-value" not in Path(result["raw_file"]).read_text(encoding="utf-8")


def test_rotation_only_removes_owned_old_artifacts(tmp_path):
    for folder in ("raw", "minute", "day", "verdict"):
        (tmp_path / folder).mkdir()
    old = tmp_path / "raw" / "20260901-000000-synthetic.jsonl"
    retained = tmp_path / "raw" / "20261101-000000-synthetic.jsonl"
    foreign = tmp_path / "raw" / "unrelated.jsonl"
    state = tmp_path / "raw" / "producer-state.json"
    for path in (old, retained, foreign, state):
        path.write_text("{}\n", encoding="utf-8")
    meter._rotate(tmp_path, meter.utc_datetime("2026-11-02T00:00:00Z"))
    assert not old.exists()
    assert all(path.exists() for path in (retained, foreign, state))
