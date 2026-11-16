"""Offline contract tests for the SofaScore endpoint journal SQL template.

Exercises ``render_journal_sql``/``summarize_journal`` against a synthetic
SQLite mirror of the ``iceberg.bronze.sofascore_schedule`` and
``iceberg.ops.sofascore_capture_manifest`` columns actually referenced by
``sofascore_endpoint_journal.sql``. No Trino, network or production fixtures.
"""

from __future__ import annotations

import inspect
import sqlite3
from datetime import datetime

import pytest
import sqlglot
import sqlglot.expressions as exp

from deploy.sofascore import endpoint_journal
from deploy.sofascore.endpoint_journal import (
    CORE_ENDPOINTS,
    JOURNAL_COLUMNS,
    SENTINEL_MATCH_ID,
    render_journal_sql,
    summarize_journal,
)

QUALIFIED_SCHEDULE = "iceberg.bronze.sofascore_schedule"
QUALIFIED_MANIFEST = "iceberg.ops.sofascore_capture_manifest"


def _iso(minute: int) -> str:
    return f"2026-03-01T10:{minute:02d}:00+00:00"


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE schedule ("
        "game_id TEXT, league TEXT, season TEXT,"
        " tournament_unique_tournament_id TEXT)"
    )
    connection.execute(
        "CREATE TABLE capture_manifest ("
        "target_id TEXT, target_type TEXT, endpoint TEXT,"
        " source_tournament_id TEXT, status TEXT, http_status INTEGER,"
        " updated_at TEXT, run_id TEXT, freshness_key TEXT)"
    )
    yield connection
    connection.close()


def _schedule_row(conn, game_id, league, season="2025", tid="17"):
    conn.execute(
        "INSERT INTO schedule VALUES (?,?,?,?)", (game_id, league, season, tid)
    )


def _manifest_row(
    conn,
    target_id,
    endpoint,
    status,
    updated_at,
    http_status=200,
    run_id="run-1",
    freshness_key="fk-1",
    source_tournament_id="17",
    target_type="event",
):
    conn.execute(
        "INSERT INTO capture_manifest VALUES (?,?,?,?,?,?,?,?,?)",
        (
            target_id,
            target_type,
            endpoint,
            source_tournament_id,
            status,
            http_status,
            updated_at,
            run_id,
            freshness_key,
        ),
    )


def _capture(conn, target_id, endpoint, status, minute, http_status=200):
    _manifest_row(
        conn, target_id, endpoint, status, _iso(minute), http_status=http_status
    )


def _capture_success_all(conn, target_id):
    for index, endpoint in enumerate(CORE_ENDPOINTS):
        _capture(conn, target_id, endpoint, "success", index)


def _run_journal(conn, match_ids):
    sql = render_journal_sql(match_ids)
    assert "{match_ids}" not in sql
    sql = sql.replace(QUALIFIED_SCHEDULE, "schedule").replace(
        QUALIFIED_MANIFEST, "capture_manifest"
    )
    cursor = conn.execute(sql)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _core_rows(rows, match_id):
    return [row for row in rows if row["match_id"] == match_id]


def _journal_row(match_id="m1", endpoint="event", status="collected", ts=_iso(0)):
    return {
        "match_id": match_id,
        "league": "ENG-PL",
        "season": "2025",
        "source_tournament_id": "17",
        "endpoint": endpoint,
        "status": status,
        "manifest_status": "success",
        "http_status": 200,
        "status_updated_at": ts,
        "observed_at": ts,
        "identity_conflict": 0,
    }


def test_sql_pipeline_on_synthetic_sqlite_tables(conn):
    _schedule_row(conn, "100", "ENG-PL")
    _capture_success_all(conn, "100")
    rows = _run_journal(conn, ["100"])
    assert set(JOURNAL_COLUMNS) <= set(rows[0])
    assert len(_core_rows(rows, "100")) == len(CORE_ENDPOINTS)
    assert {row["match_id"] for row in rows} == {"100", SENTINEL_MATCH_ID}
    summary = summarize_journal(rows)
    assert set(summary) == {"100"}
    assert summary["100"].capture_complete is True


def test_sentinel_rows_are_skipped_by_builtin_summary_guard():
    sentinel = dict(zip(JOURNAL_COLUMNS, [SENTINEL_MATCH_ID] + [None] * 10))
    assert summarize_journal([sentinel]) == {}


def test_partial_capture_missing_endpoint_incomplete(conn):
    _schedule_row(conn, "100", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "100", endpoint, "success", index)
    rows = _run_journal(conn, ["100"])
    match = summarize_journal(rows)["100"]
    assert len(_core_rows(rows, "100")) == 5
    assert len(match.statuses) == 5
    assert match.statuses["incidents"] == "not_collected"
    assert match.capture_complete is False
    assert match.closed_at is None


def test_retryable_failure_endpoint_is_not_collected(conn):
    _schedule_row(conn, "200", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "200", endpoint, "success", index)
    _capture(
        conn, "200", "incidents", "retryable_failure", 9, http_status=503
    )
    match = summarize_journal(_run_journal(conn, ["200"]))["200"]
    assert match.statuses["incidents"] == "not_collected"
    assert match.capture_complete is False
    assert match.closed_at is None


def test_all_success_complete_with_max_closed_at(conn):
    _schedule_row(conn, "300", "ENG-PL")
    for endpoint, minute in zip(CORE_ENDPOINTS, [4, 0, 2, 1, 3]):
        _capture(conn, "300", endpoint, "success", minute)
    match = summarize_journal(_run_journal(conn, ["300"]))["300"]
    assert match.capture_complete is True
    assert match.closed_at == _epoch(_iso(4))
    assert match.observed_at == _epoch(_iso(4))


def test_legitimate_empty_counts_as_collected_terminal(conn):
    _schedule_row(conn, "310", "ENG-PL")
    _capture(conn, "310", "event", "legitimate_empty", 0, http_status=204)
    for index, endpoint in enumerate(CORE_ENDPOINTS[1:], start=1):
        _capture(conn, "310", endpoint, "success", index)
    match = summarize_journal(_run_journal(conn, ["310"]))["310"]
    assert match.statuses["event"] == "collected"
    assert match.capture_complete is True
    assert match.closed_at == _epoch(_iso(4))


def test_not_supported_with_404_maps_to_source_404_terminal(conn):
    _schedule_row(conn, "400", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "400", endpoint, "success", index)
    _capture(conn, "400", "incidents", "not_supported", 9, http_status=404)
    match = summarize_journal(_run_journal(conn, ["400"]))["400"]
    assert match.statuses["incidents"] == "source_404"
    assert match.capture_complete is True
    assert match.closed_at == _epoch(_iso(9))


@pytest.mark.parametrize("http_status", [None, 500])
def test_not_supported_without_404_is_not_collected(conn, http_status):
    _schedule_row(conn, "410", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "410", endpoint, "success", index)
    _capture(
        conn, "410", "incidents", "not_supported", 9, http_status=http_status
    )
    match = summarize_journal(_run_journal(conn, ["410"]))["410"]
    assert match.statuses["incidents"] == "not_collected"
    assert match.capture_complete is False
    assert match.closed_at is None


def test_schema_error_is_quarantined_and_incomplete(conn):
    _schedule_row(conn, "500", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "500", endpoint, "success", index)
    _capture(conn, "500", "incidents", "schema_error", 9)
    match = summarize_journal(_run_journal(conn, ["500"]))["500"]
    assert match.statuses["incidents"] == "quarantine"
    assert match.capture_complete is False
    assert match.closed_at is None


def test_unknown_persisted_status_maps_to_invalid_and_raises(conn):
    _schedule_row(conn, "510", "ENG-PL")
    _capture(conn, "510", "event", "mystery_status", 0)
    rows = _run_journal(conn, ["510"])
    invalid = [
        row
        for row in _core_rows(rows, "510")
        if row["endpoint"] == "event"
    ]
    assert invalid[0]["status"] == "invalid"
    with pytest.raises(ValueError, match="invalid status"):
        summarize_journal(rows)


def test_latest_freshness_record_wins_and_summary_is_stable(conn):
    _schedule_row(conn, "600", "ENG-PL")
    for index, endpoint in enumerate(CORE_ENDPOINTS[:4]):
        _capture(conn, "600", endpoint, "success", index)
    _manifest_row(
        conn,
        "600",
        "incidents",
        "retryable_failure",
        _iso(0),
        http_status=503,
        run_id="run-0",
        freshness_key="fk-old",
    )
    _manifest_row(
        conn,
        "600",
        "incidents",
        "success",
        _iso(7),
        run_id="run-2",
        freshness_key="fk-new",
    )
    rows = _run_journal(conn, ["600"])
    first = summarize_journal(rows)
    second = summarize_journal(rows)
    match = first["600"]
    assert len(_core_rows(rows, "600")) == 5
    assert len(match.statuses) == 5
    assert match.statuses["incidents"] == "collected"
    assert match.capture_complete is True
    assert match.closed_at == _epoch(_iso(7))
    assert second["600"].closed_at == match.closed_at
    assert second["600"].statuses == match.statuses


def test_eng_and_ss_schedule_rows_collapse_to_single_match(conn):
    _schedule_row(conn, "700", "ENG-PL", tid="17")
    _schedule_row(conn, "700", "SS-99", tid="99")
    _capture_success_all(conn, "700")
    rows = _run_journal(conn, ["700"])
    summary = summarize_journal(rows)
    core = _core_rows(rows, "700")
    assert set(summary) == {"700"}
    assert len(core) == 5
    assert {row["source_tournament_id"] for row in core} == {"99"}
    assert summary["700"].capture_complete is True


def test_two_conflicting_ss_ids_raise_identity_conflict(conn):
    _schedule_row(conn, "710", "SS-1", tid="1")
    _schedule_row(conn, "710", "SS-2", tid="2")
    _capture_success_all(conn, "710")
    rows = _run_journal(conn, ["710"])
    assert {row["identity_conflict"] for row in _core_rows(rows, "710")} == {1}
    with pytest.raises(ValueError, match="identity conflict"):
        summarize_journal(rows)


def test_schedule_only_manifest_only_and_dual_ids_preserved(conn):
    _schedule_row(conn, "s-only", "ENG-PL", tid="17")
    _schedule_row(conn, "dual", "ENG-PL", tid="17")
    _capture_success_all(conn, "m-only")
    _capture_success_all(conn, "dual")
    _manifest_row(
        conn, "m-only", "event", "success", _iso(0), target_type="season"
    )
    _manifest_row(conn, "m-only", "odds", "success", _iso(0))
    rows = _run_journal(conn, ["s-only", "m-only", "dual"])
    summary = summarize_journal(rows)
    assert set(summary) == {"s-only", "m-only", "dual"}
    assert len(_core_rows(rows, "s-only")) == 5
    assert len(_core_rows(rows, "m-only")) == 5
    assert len(_core_rows(rows, "dual")) == 5
    assert summary["s-only"].capture_complete is False
    assert set(summary["s-only"].statuses.values()) == {"not_collected"}
    assert summary["m-only"].capture_complete is True
    assert summary["dual"].capture_complete is True
    assert "odds" not in summary["m-only"].statuses
    assert len([row for row in rows if row["match_id"] != SENTINEL_MATCH_ID]) == 15


def test_duplicate_output_pair_rejected():
    base = [
        _journal_row(endpoint=endpoint, ts=_iso(index))
        for index, endpoint in enumerate(CORE_ENDPOINTS)
    ]
    with pytest.raises(ValueError, match="duplicate journal row"):
        summarize_journal(base + [base[0]])


@pytest.mark.parametrize("ts", ["not-a-timestamp", "2026-03-01T10:00:00"])
def test_malformed_timestamps_rejected(ts):
    rows = [
        _journal_row(endpoint=endpoint, ts=_iso(index))
        for index, endpoint in enumerate(CORE_ENDPOINTS)
    ]
    rows[0]["status_updated_at"] = ts
    with pytest.raises(ValueError, match="ISO timestamp|timezone-aware"):
        summarize_journal(rows)


def test_empty_or_blank_match_ids_rejected():
    with pytest.raises(ValueError, match="at least one id"):
        render_journal_sql([])
    with pytest.raises(ValueError, match="non-empty string"):
        render_journal_sql(["   "])


def test_match_id_with_quote_is_escaped_and_round_trips(conn):
    tricky = "x'y"
    _schedule_row(conn, tricky, "SS-7", tid="7")
    _capture_success_all(conn, tricky)
    summary = summarize_journal(_run_journal(conn, [tricky]))
    assert set(summary) == {tricky}
    assert summary[tricky].capture_complete is True


def test_render_deduplicates_sorts_and_quotes_ids():
    sql = render_journal_sql(["b", " a ", "a"])
    assert "'a', 'b'" in sql
    assert "{match_ids}" not in sql


def test_rendered_sql_is_single_readonly_trino_statement():
    sql = render_journal_sql(["100"])
    statements = sqlglot.parse(sql, read="trino")
    assert len(statements) == 1
    mutations = tuple(
        node
        for node in (
            getattr(exp, name, None)
            for name in (
                "Insert",
                "Update",
                "Delete",
                "Merge",
                "Create",
                "Drop",
                "AlterTable",
            )
        )
        if node is not None
    )
    assert mutations
    for statement in statements:
        assert not list(statement.find_all(*mutations))
    assert sql.count(SENTINEL_MATCH_ID) == 1


def test_render_journal_sql_has_no_network_or_subprocess_surface():
    source = inspect.getsource(endpoint_journal)
    for token in ("subprocess", "socket", "urllib", "requests", "http.client"):
        assert token not in source
    assert isinstance(render_journal_sql(["100"]), str)
