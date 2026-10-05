"""SQL of the ESPN history lane on DuckDB: live debt, capability, report (#1509).

Trino SQL is transpiled to DuckDB with sqlglot (as ``test_espn_criterion.py``)
and run over synthetic ``espn_match``, queue and request-journal rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scrapers.espn import history
from scrapers.espn.history_report import (
    FULL,
    NO_BOTH,
    NO_LINEUPS,
    NO_STATS,
    empty_defects,
    render_capability_sql,
    render_history_line,
    render_journal_sql,
    render_live_debt_sql,
    render_live_debt_rows_sql,
    render_queue_sql,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
DAY = "2026-09-28"
_MATCH = (
    "competition_slug", "season_year", "event_id", "kickoff", "status", "played_final",
    "terminal_nonplayed", "disposition", "lineup_state", "team_stats_state",
    "first_published_at", "status_checked_at", "duplicate_of",
)


@pytest.fixture()
def db():
    sqlglot = pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA iceberg.bronze")
    con.execute(
        "CREATE TABLE iceberg.bronze.espn_match (competition_slug varchar, season_year integer, "
        "event_id bigint, kickoff timestamp, status varchar, played_final boolean, "
        "terminal_nonplayed boolean, disposition varchar, lineup_state varchar, "
        "team_stats_state varchar, first_published_at timestamp, status_checked_at timestamp, "
        "duplicate_of varchar)"
    )
    history.ensure_queue_table(con)
    con.execute(
        "CREATE TABLE iceberg.ops.espn_request_journal_v1 (request_date date, lane varchar, "
        "attempts integer, direct_bytes bigint)"
    )

    def match(event_id, **kw):
        row = {
            "competition_slug": "eng.1", "season_year": 2026, "event_id": event_id,
            "kickoff": datetime(2026, 9, 27, 12), "status": "STATUS_FULL_TIME",
            "played_final": True, "terminal_nonplayed": False, "disposition": "captured",
            "lineup_state": "captured", "team_stats_state": "captured",
            "first_published_at": None, "status_checked_at": datetime(2026, 9, 27, 15),
            "duplicate_of": None,
        }
        row.update(kw)
        con.execute(
            f"INSERT INTO iceberg.bronze.espn_match VALUES ({', '.join('?' * len(_MATCH))})",
            [row[name] for name in _MATCH],
        )

    def run(sql):
        return con.execute(sqlglot.transpile(sql, read="trino", write="duckdb")[0]).fetchall()

    return type("Db", (), {"con": con, "match": staticmethod(match), "run": staticmethod(run)})


def test_live_debt_counts_only_unpublished_due_matches_of_live_targets(db) -> None:
    ago = lambda hours: datetime(2026, 9, 28, 12) - timedelta(hours=hours)  # noqa: E731
    db.match(1, kickoff=ago(20))                                        # debt
    db.match(2, kickoff=ago(20), status="STATUS_SCHEDULED", played_final=False,
             status_checked_at=ago(21))                                 # not re-read: debt
    db.match(3, kickoff=ago(20), first_published_at=ago(10))            # published
    db.match(4, kickoff=ago(10))                                        # too fresh (< 14 h)
    db.match(5, kickoff=ago(80))                                        # too old (> 72 h)
    db.match(6, kickoff=ago(20), status="STATUS_POSTPONED", played_final=False,
             status_checked_at=ago(18))                                 # postponed, confirmed
    db.match(7, kickoff=ago(20), status="STATUS_CANCELED", played_final=False,
             terminal_nonplayed=True, status_checked_at=ago(19))       # cancelled
    db.match(8, kickoff=ago(20), disposition="withdrawn")               # core 404
    db.match(9, kickoff=ago(20), duplicate_of="uefa.champions:2026")    # a duplicate
    db.match(10, kickoff=ago(20), competition_slug="concacaf.u23")      # not a live target

    targets = ["eng.1", "ger.2"]
    (debt,), = db.run(render_live_debt_sql(targets, NOW))
    rows = db.run(render_live_debt_rows_sql(["event_id"], targets, NOW))

    assert debt == 2
    assert {event_id for event_id, in rows} == {1, 2}


def test_capability_is_read_from_the_season_data(db) -> None:
    history.save_rows(db.con, [
        history.QueueRow(slug, year, 1, history.DONE)
        for slug, year in (("eng.1", 2015), ("fifa.world", 2010), ("eng.1", 2005), ("ger.2", 2016))
    ])
    for event_id in range(10):  # full season, one empty lineup = a defect
        db.match(100 + event_id, season_year=2015,
                 lineup_state="valid_empty" if event_id == 0 else "captured")
    for event_id in range(4):   # World Cup 2010: empty lineups, full statistics
        db.match(200 + event_id, competition_slug="fifa.world", season_year=2010,
                 lineup_state="valid_empty")
    for event_id in range(4):   # no team statistics in the season
        db.match(300 + event_id, season_year=2005, team_stats_state="valid_empty")
    for event_id in range(2):   # neither
        db.match(400 + event_id, competition_slug="ger.2", season_year=2016,
                 lineup_state="valid_empty", team_stats_state="valid_empty")
    db.match(500, season_year=2026, lineup_state="valid_empty")  # live season: not history

    rows = db.run(render_capability_sql())

    assert [(row[0], row[1], row[7]) for row in rows] == [
        ("eng.1", 2005, NO_STATS),
        ("eng.1", 2015, FULL),
        ("fifa.world", 2010, NO_LINEUPS),
        ("ger.2", 2016, NO_BOTH),
    ]
    # Only the empty lineup of the full season is a defect.
    assert empty_defects(rows) == 1


def test_history_line_from_the_queue_and_the_journal(db) -> None:
    today = datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc)
    yesterday = today - timedelta(days=1)

    def row(slug, year, kind, state, at, **kw):
        return history.QueueRow(slug, year, kind, state, updated_at=at, **kw)

    history.save_rows(db.con, [
        row("eng.1", 0, 0, history.DONE, yesterday),
        row("eng.1", 2015, 1, history.DONE, today, matches=380, done=380),
        row("eng.1", 2014, 1, history.DONE, yesterday),
        row("eng.1", 2013, 1, history.RED, today, failed=2),
        row("uefa.champions", 2010, 1, history.DONE, today),
        row("uefa.champions", 2010, 5, history.LISTED, today),
        row("uefa.champions", 2011, 0, history.PENDING, yesterday),
        row("ger.2", 0, 1, history.RED, today, attempts=2, last_error="seasons: 503"),
    ])
    for state, matches, at in (("idle", 300, today), ("live_debt", 80, today),
                               ("live_debt", 5, today), ("budget", 999, yesterday)):
        history.append_run_row(db.con, history.QueueRow("(run)", 0, 0, state, matches=matches, updated_at=at))
    db.con.execute(
        "INSERT INTO iceberg.ops.espn_request_journal_v1 VALUES "
        "(DATE '2026-09-28', 'history', 1, 20480), (DATE '2026-09-28', 'history', 1, 20480), "
        "(DATE '2026-09-28', 'history', 0, 0), (DATE '2026-09-28', 'live', 1, 999999), "
        "(DATE '2026-09-27', 'history', 1, 999999)"
    )

    (queue,) = db.run(render_queue_sql(DAY))
    (journal,) = db.run(render_journal_sql(DAY))

    # Seasons: eng.1 2015 (finished today), 2014 (finished yesterday), 2013
    # (red), UCL 2010 (a type still listed), UCL 2011 (types not read); the
    # failed season list of ger.2 is red too.
    assert tuple(queue) == (5, 1, 2, 2, 385, 2)
    assert tuple(journal) == (2, 40960)
    line = render_history_line(DAY, queue, journal, defects=3)
    assert line == (
        "• ESPN история 28.09: сезонов готово за сутки 1 (всего готово 2 из 5), красных 2, "
        "матчей 385, запросов 2, 40 КБ (≈ 0.0 запр. и 0 КБ на матч), пауз из-за актуалки 2; "
        "пустых частей в полных сезонах 3 ‼️"
    )
    assert render_history_line(DAY, (0, 0, 0, 0, 0, 0), None) == "• ESPN история 28.09: очередь пуста"
    assert "красных 1" in render_history_line(DAY, (0, 0, 0, 1, 0, 0), None)
