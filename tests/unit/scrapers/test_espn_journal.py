"""ESPN request journal: DDL, rows and batched insert (#1500)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from scrapers.espn import journal
from scrapers.espn.transport_contracts import EndpointType, RequestLedgerEntry


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.statements = []

    def cursor(self):
        return FakeCursor(self.statements)


def _entry(status=200, origin="https://site.web.api.espn.com", error=None):
    return RequestLedgerEntry(
        url_fingerprint="a" * 64,
        endpoint=EndpointType.SUMMARY,
        attempts=1,
        status=status,
        direct_bytes=35000,
        proxy_bytes=0,
        latency_ms=470.5,
        raw_uri=None,
        content_hash=None,
        disposition="success" if status == 200 else "blocked_deferred",
        error=error,
        transport_origin=origin,
        requested_at="2026-09-25T12:00:01.250000+00:00",
        host=origin.split("//")[1],
        lane="live",
        step=0,
        content_encoding="gzip",
        origin_attempts=((origin, status),),
    )


@pytest.mark.unit
def test_ddl_has_every_journal_field_and_date_partition():
    conn = FakeConnection()
    journal.ensure_journal_table(conn)
    ddl = conn.statements[-1]
    assert journal.JOURNAL_TABLE == "iceberg.ops.espn_request_journal_v1"
    assert "CREATE TABLE IF NOT EXISTS iceberg.ops.espn_request_journal_v1" in ddl
    for column in (
        "requested_at",
        "host",
        "lane",
        "step",
        "content_encoding",
        "origin_attempts",
        "status",
        "attempts",
        "direct_bytes",
        "latency_ms",
        "transport_origin",
    ):
        assert f"{column} " in ddl
    assert "partitioning = ARRAY['request_date']" in ddl


@pytest.mark.unit
def test_rows_carry_address_status_and_date():
    rows = journal.journal_rows(
        [_entry(), _entry(403, "https://site.api.espn.com", "it's 403")],
        run_id="run-1",
        task_id="summary",
    )
    assert [(r["host"], r["status"]) for r in rows] == [
        ("site.web.api.espn.com", 200),
        ("site.api.espn.com", 403),
    ]
    assert rows[0]["request_date"] == date(2026, 9, 25)
    assert isinstance(rows[0]["requested_at"], datetime)
    assert rows[0]["origin_attempts"] == '[["https://site.web.api.espn.com", 200]]'


@pytest.mark.unit
def test_flush_inserts_rows_in_one_batch_with_quoting():
    rows = journal.journal_rows(
        [_entry(), _entry(403, "https://site.api.espn.com", "it's 403")],
        run_id="run-1",
        task_id="summary",
    )
    conn = FakeConnection()
    assert journal.flush_journal(conn, rows) == 2
    assert len(conn.statements) == 1
    sql = conn.statements[0]
    assert sql.startswith("INSERT INTO iceberg.ops.espn_request_journal_v1 (")
    assert "DATE '2026-09-25'" in sql
    assert "TIMESTAMP '2026-09-25 12:00:01.250000'" in sql
    assert "'it''s 403'" in sql
    assert sql.count("), (") == 1
