"""Daily DQ checks of the ESPN bronze tables (#1505).

Every check runs on DuckDB (Trino -> DuckDB via sqlglot) over synthetic
rows: it stays silent on clean data and names its own breakage.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from scrapers.espn.quality import (
    DOWNGRADE_REJECTED_LINE,
    build_quality_sql,
    build_summary_loads_sql,
    render_quality_lines,
)

pytestmark = pytest.mark.unit

DAY = "2026-10-11"
AT = datetime(2026, 10, 11, 6)
FETCHED = datetime(2026, 10, 11, 5)


def _db():
    pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA ops")
    con.execute(
        "CREATE TABLE bronze.espn_match (competition_slug varchar, event_id bigint, "
        "played_final boolean, disposition varchar, home_score integer, away_score integer, "
        "_ingested_at timestamp, _source_fetched_at timestamp)"
    )
    for table in ("espn_match_lineup", "espn_team_stats", "espn_match_events"):
        con.execute(
            f"CREATE TABLE bronze.{table} (event_id bigint, _ingested_at timestamp, "
            "_source_fetched_at timestamp)"
        )
    con.execute(
        "CREATE TABLE ops.espn_request_journal_v1 (request_date date, url_fingerprint varchar, "
        "endpoint varchar, disposition varchar)"
    )
    return con


def _match(con, event_id, slug="eng.1", disposition="captured", *, played=True,
           score=(1, 0), at=AT, fetched=FETCHED):
    con.execute(
        "INSERT INTO bronze.espn_match VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [slug, event_id, played, disposition, *score, at, fetched],
    )


def _clean(con):
    for event_id in range(1, 11):
        _match(con, event_id, disposition="valid_empty" if event_id == 1 else "captured")
        con.execute("INSERT INTO bronze.espn_match_lineup VALUES (?, ?, ?)", [event_id, AT, FETCHED])
        con.execute(
            "INSERT INTO ops.espn_request_journal_v1 VALUES (DATE '2026-10-11', ?, 'summary', 'success')",
            [f"fp{event_id}"],
        )
    # A small tournament above the share: fewer than 5 matches never alarms.
    for event_id in range(20, 23):
        _match(con, event_id, slug="ger.2", disposition="source_malformed", score=(0, 0))
    # Not played, not written that day, cache hits of the journal: outside.
    _match(con, 30, played=False, disposition=None, score=(None, None))
    _match(con, 31, disposition="source_malformed", at=AT - timedelta(days=1))
    for _ in range(3):
        con.execute(
            "INSERT INTO ops.espn_request_journal_v1 VALUES (DATE '2026-10-11', 'fp1', 'summary', 'cache_hit')"
        )


def _lines(con):
    import sqlglot

    rows = []
    for sql in (build_quality_sql(DAY), build_summary_loads_sql(DAY)):
        duck = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        duck = duck.replace("iceberg.bronze.", "bronze.").replace("iceberg.ops.", "ops.")
        rows.extend(con.execute(duck).fetchall())
    return render_quality_lines(DAY, rows)


def test_clean_day_prints_only_the_downgrade_stub():
    con = _db()
    _clean(con)
    assert _lines(con) == [DOWNGRADE_REJECTED_LINE]


@pytest.mark.parametrize(
    "breakage, expected",
    [
        (
            lambda con: [_match(con, 40 + i, disposition="valid_empty") for i in range(3)],
            "• ESPN DQ 11.10: доля valid_empty > 20 % (от 5 матчей): eng.1 4/13 ‼️",
        ),
        (
            lambda con: [_match(con, 40 + i, disposition="source_malformed") for i in range(3)],
            "• ESPN DQ 11.10: доля source_malformed > 20 % (от 5 матчей): eng.1 3/13 ‼️",
        ),
        (
            lambda con: [_match(con, 40 + i, disposition="lineup_anomaly") for i in range(3)],
            "• ESPN DQ 11.10: доля lineup_anomaly > 20 % (от 5 матчей): eng.1 3/13 ‼️",
        ),
        (
            lambda con: _match(con, 5, slug="eng.1", at=AT - timedelta(days=3)),
            "• ESPN DQ 11.10: дубли event_id в espn_match: 1 ‼️",
        ),
        (
            lambda con: con.execute(
                "INSERT INTO bronze.espn_match_lineup VALUES (1, ?, ?)", [AT, AT + timedelta(hours=1)]
            ),
            "• ESPN DQ 11.10: _ingested_at < _source_fetched_at в espn_match_lineup: 1 из 11 ‼️",
        ),
        (
            lambda con: _match(con, 50, score=(None, 0)),
            "• ESPN DQ 11.10: сыгранный без счёта (NULL вместо 0) eng.1: 1 из 11 ‼️",
        ),
        (
            lambda con: [
                con.execute(
                    "INSERT INTO ops.espn_request_journal_v1 VALUES "
                    "(DATE '2026-10-11', 'fp2', 'summary', 'success')"
                )
                for _ in range(2)
            ],
            "• ESPN DQ 11.10: summary качали > 2 раз за сутки: 1 матч(ей) ‼️",
        ),
    ],
    ids=["valid_empty", "source_malformed", "lineup_anomaly", "duplicate", "ingest_order",
         "null_score", "summary_loads"],
)
def test_each_check_catches_its_breakage(breakage, expected):
    con = _db()
    _clean(con)
    breakage(con)
    assert _lines(con) == [expected, DOWNGRADE_REJECTED_LINE]


def test_failed_query_is_named_not_hidden():
    assert render_quality_lines(DAY, None) == [
        "• ESPN DQ 11.10: не посчитано ⚠️", DOWNGRADE_REJECTED_LINE,
    ]
