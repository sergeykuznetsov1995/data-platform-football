"""#1476: WhoScored criterion SQL executed on real rows (Trino SQL -> DuckDB)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")

from dags.scripts import whoscored_criterion as criterion  # noqa: E402

NOW = datetime(2026, 9, 29, 12, 0, 0)
KICKOFF = datetime(2026, 9, 28, 6, 0, 0)  # deadline 2026-09-29 08:00
DEADLINE = KICKOFF + timedelta(hours=26)


def _connection():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA iceberg.bronze")
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_schedule_current (
            league VARCHAR, season VARCHAR, game_id BIGINT, game VARCHAR,
            stage_id BIGINT, status BIGINT, date TIMESTAMP,
            is_lineup_confirmed BOOLEAN, tournament_id BIGINT,
            region_name VARCHAR, tournament_name VARCHAR,
            _ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_match_ingest_manifest (
            league VARCHAR, season VARCHAR, game_id BIGINT, state VARCHAR,
            batch_id VARCHAR, raw_uri VARCHAR, events_count BIGINT,
            lineups_count BIGINT, fetched_at TIMESTAMP, completed_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE VIEW iceberg.bronze.whoscored_match_ingest_latest AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY league, season, game_id ORDER BY completed_at DESC
            ) AS rn
            FROM iceberg.bronze.whoscored_match_ingest_manifest
        ) WHERE rn = 1
        """
    )
    for table in ("whoscored_schedule", "whoscored_matches", "whoscored_events"):
        con.execute(
            f"CREATE TABLE iceberg.bronze.{table} "
            "(league VARCHAR, season VARCHAR, _ingested_at TIMESTAMP)"
        )
    return con


def _game(
    con,
    game_id,
    *,
    stage_id=100,
    status=6,
    tournament_id=2,
    kickoff=KICKOFF,
    league="ENG-Premier League",
):
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_schedule_current VALUES "
        "(?, '2627', ?, ?, ?, ?, ?, TRUE, ?, 'England', 'Premier League', ?)",
        [
            league,
            game_id,
            f"game {game_id}",
            stage_id,
            status,
            kickoff,
            tournament_id,
            NOW - timedelta(hours=1),
        ],
    )


def _attempt(
    con,
    game_id,
    state,
    at,
    *,
    events=100,
    lineups=22,
    league="ENG-Premier League",
):
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_match_ingest_manifest VALUES "
        "(?, '2627', ?, ?, ?, 's3://raw/x', ?, ?, ?, ?)",
        [
            league,
            game_id,
            state,
            f"ws2-{game_id}-{state}-{at.isoformat()}",
            events,
            lineups,
            at,
            at,
        ],
    )


def _query(con):
    def run(sql):
        rendered = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        return con.execute(rendered).fetchall()

    return run


def _world(con):
    _game(con, 1)  # ok: collected 20 h after kickoff
    _attempt(con, 1, "success", KICKOFF + timedelta(hours=20))
    _game(con, 2)  # late: first success after the deadline
    _attempt(con, 2, "success", DEADLINE + timedelta(minutes=5))
    _game(con, 3)  # missing: never attempted
    _game(con, 4)  # ceiling: two "not available" verdicts, available stage
    _attempt(con, 4, "not_available", KICKOFF + timedelta(hours=3))
    _attempt(con, 4, "not_available", KICKOFF + timedelta(hours=25))
    _game(con, 5)  # missing: one NA verdict, its 72 h re-probe is pending
    _attempt(con, 5, "not_available", KICKOFF + timedelta(hours=3))
    _game(con, 6)  # missing: success without events is not collected
    _attempt(con, 6, "success", KICKOFF + timedelta(hours=3), events=0)
    _game(con, 7, status=7)  # cancelled: out
    _game(con, 8, tournament_id=999)  # not class A: out
    _game(con, 9, kickoff=NOW - timedelta(hours=3))  # deadline ahead: out
    _game(con, 10, kickoff=NOW - timedelta(days=9))  # before the window: out
    # Stage 200 is unavailable (2 NA with a lineup, 0 success): out entirely.
    for game_id in (20, 21):
        _game(con, game_id, stage_id=200)
        _attempt(con, game_id, "not_available", KICKOFF + timedelta(hours=3))
    # Stage 300 has no manifest row at all (unknown): its game is missing,
    # never silently dropped by a NULL ceiling.
    _game(con, 30, stage_id=300)
    # First success counts even when a later re-ingest exists.
    _game(con, 11)
    _attempt(con, 11, "success", KICKOFF + timedelta(hours=10))
    _attempt(con, 11, "success", DEADLINE + timedelta(hours=10))


@pytest.mark.unit
def test_daily_criterion_grades_each_denominator_game():
    con = _connection()
    _world(con)

    rows = _query(con)(criterion.render_daily_criterion_sql(NOW))
    days = criterion.day_results(rows)

    assert [(d.day, d.due, d.ok, d.late, d.missing, d.ceiling) for d in days] == [
        ("2026-09-29", 8, 2, 1, 4, 1)
    ]
    assert days[0].pct == Decimal("28.6")
    assert days[0].meets_target is False


@pytest.mark.unit
def test_missed_and_overdue_lists():
    con = _connection()
    _world(con)
    query = _query(con)

    missed = query(criterion.render_missed_sql(NOW))
    overdue = query(criterion.render_overdue_sql(NOW))

    assert sorted(row[3] for row in missed) == [2, 3, 5, 6, 30]
    assert missed[0][0] == "England / Premier League"
    assert sorted(row[2] for row in overdue) == [3, 5, 6, 30]


@pytest.mark.unit
@pytest.mark.parametrize("stage_available", [False, True])
def test_postponed_league_one_two_games_are_not_overdue(stage_available):
    # #1602: these four source rows have status=2 / elapsed=Post, no lineup
    # and no manifest. A played-but-missing control must still turn red.
    con = _connection()
    query = _query(con)
    for league, tournament_id, stage_id, game_ids in (
        ("WS-252-8", 8, 25589, (1989368, 1989372, 1989376)),
        ("WS-252-9", 9, 25590, (1990128,)),
    ):
        for game_id in game_ids:
            _game(con, game_id, league=league, tournament_id=tournament_id,
                  stage_id=stage_id, status=2)
        if stage_available:
            _game(con, stage_id, league=league, tournament_id=tournament_id,
                  stage_id=stage_id, kickoff=NOW - timedelta(days=9))
            _attempt(con, stage_id, "success", NOW - timedelta(days=8), league=league)
    con.execute("UPDATE iceberg.bronze.whoscored_schedule_current "
                "SET is_lineup_confirmed=FALSE WHERE status=2")
    _game(con, 1)

    days = criterion.day_results(query(criterion.render_daily_criterion_sql(NOW)))
    assert [(d.due, d.missing, d.ceiling) for d in days] == [(1, 1, 0)]
    assert [row[3] for row in query(criterion.render_missed_sql(NOW))] == [1]
    assert [row[2] for row in query(criterion.render_overdue_sql(NOW))] == [1]


@pytest.mark.unit
def test_rescheduled_game_uses_new_date_then_returns_to_deadline_checks():
    con = _connection()
    query = _query(con)
    _game(con, 1989350, status=2)
    _game(con, 1989350, status=1, kickoff=NOW + timedelta(days=10))
    con.execute("UPDATE iceberg.bronze.whoscored_schedule_current "
                "SET _ingested_at=? WHERE status=1", [NOW])
    for render in (criterion.render_daily_criterion_sql, criterion.render_missed_sql,
                   criterion.render_overdue_sql):
        assert query(render(NOW)) == []

    # After the replacement fixture is played, its new deadline applies.
    con.execute("UPDATE iceberg.bronze.whoscored_schedule_current "
                "SET status=6, date=?, _ingested_at=? WHERE status=1",
                [NOW - timedelta(hours=3), NOW + timedelta(minutes=1)])
    assert query(criterion.render_overdue_sql(NOW)) == []
    after_deadline = NOW + timedelta(hours=24)
    days = criterion.day_results(query(criterion.render_daily_criterion_sql(after_deadline)))
    assert [(d.due, d.missing) for d in days] == [(1, 1)]
    assert [row[3] for row in query(criterion.render_missed_sql(after_deadline))] == [1989350]
    assert [row[2] for row in query(criterion.render_overdue_sql(after_deadline))] == [1989350]


@pytest.mark.unit
def test_all_collected_on_time_is_one_hundred_percent():
    con = _connection()
    _game(con, 1)
    _attempt(con, 1, "success", KICKOFF + timedelta(hours=2))

    days = criterion.day_results(_query(con)(criterion.render_daily_criterion_sql(NOW)))

    assert days[0].pct == Decimal("100.0")
    assert days[0].meets_target is True
    assert _query(con)(criterion.render_overdue_sql(NOW)) == []


@pytest.mark.unit
def test_schedule_freshness_flags_old_and_absent_partitions():
    con = _connection()
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_schedule VALUES "
        "('ENG-Premier League', '2627', ?), ('WS-252-29', '2627', ?)",
        [NOW - timedelta(hours=5), NOW - timedelta(days=55)],
    )
    partitions = [
        ("ENG-Premier League", "2627"),
        ("WS-252-29", "2627"),
        ("WS-182-77", "2627"),
    ]

    older, absent = criterion.stale_schedule_partitions(_query(con), partitions, NOW)

    assert older == ["WS-252-29=2627 (1320h)"]
    assert absent == ["WS-182-77=2627"]


@pytest.mark.unit
def test_schedule_freshness_finished_season_needs_rows_not_age():
    # #1601: a finished season is not re-read daily; 56 h old rows are fine,
    # missing rows are still an error; an active season keeps the 48 h rule.
    con = _connection()
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_schedule VALUES "
        "('ENG-Premier League', '2526', ?), ('ENG-Premier League', '2627', ?)",
        [NOW - timedelta(hours=56), NOW - timedelta(hours=56)],
    )
    partitions = [
        ("ENG-Premier League", "2526"),
        ("ENG-Premier League", "2627"),
        ("INT-World Cup", "2026"),
    ]
    inactive = {("ENG-Premier League", "2526"), ("INT-World Cup", "2026")}

    older, absent = criterion.stale_schedule_partitions(
        _query(con), partitions, NOW, inactive
    )

    assert older == ["ENG-Premier League=2627 (56h)"]
    assert absent == ["INT-World Cup=2026"]


@pytest.mark.unit
def test_inactive_partitions_only_from_explicit_false():
    # A report without ``is_active`` (older format) or with None stays strict.
    report = {
        "scopes": [
            {"scope": "A=2526", "competition_id": "A", "season_id": "2526",
             "is_active": False},
            {"scope": "A=2627", "competition_id": "A", "season_id": "2627",
             "is_active": True},
            {"scope": "B=2526", "competition_id": "B", "season_id": "2526",
             "is_active": None},
            {"scope": "C=2526", "competition_id": "C", "season_id": "2526"},
        ]
    }

    assert criterion.inactive_partitions(report) == {("A", "2526")}


@pytest.mark.unit
def test_content_age_is_measured_over_denominator_partitions_only():
    con = _connection()
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_events VALUES "
        "('ENG-Premier League', '2627', ?), ('WS-999-1', '2627', ?)",
        [NOW - timedelta(hours=60), NOW - timedelta(hours=1)],
    )

    age = criterion.content_age_hours(
        _query(con), "whoscored_events", [("ENG-Premier League", "2627")], NOW
    )

    assert age == pytest.approx(60.0)


@pytest.mark.unit
def test_denominator_partitions_exclude_probe_scopes():
    report = {
        "scopes": [
            {"scope": "ENG-Premier League=2627", "competition_id": "ENG-Premier League",
             "season_id": "2627"},
            {"scope": "WS-206-63=2526", "competition_id": "WS-206-63",
             "season_id": "2526"},
        ]
    }

    assert criterion.denominator_partitions(report) == [("ENG-Premier League", "2627")]


@pytest.mark.unit
def test_sql_constants_carry_the_class_a_list_and_placeholders():
    from scrapers.whoscored.catalog import CLASS_A_TOURNAMENT_IDS

    ids = ", ".join(str(value) for value in sorted(CLASS_A_TOURNAMENT_IDS))
    for sql in (criterion.DAILY_CRITERION_SQL, criterion.MISSED_SQL, criterion.OVERDUE_SQL):
        assert f"tournament_id IN ({ids})" in sql
        assert "TIMESTAMP '{now}'" in sql
        assert "INTERVAL '26' HOUR" in sql
        assert "status NOT IN (2, 5, 7)" in sql
    with pytest.raises(ValueError):
        criterion.render_daily_criterion_sql(datetime.now().astimezone())
