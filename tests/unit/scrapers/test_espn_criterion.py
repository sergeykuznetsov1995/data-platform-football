"""Daily ESPN "played -> Bronze within 24 h" meter (#1505).

The SQL itself runs on DuckDB (Trino -> DuckDB via sqlglot, as the Understat
meter test) over synthetic ``espn_match`` rows; the rows of the republished
match come from the #1503 row builder, so the carried ``first_published_at``
is the one the writer would commit.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from scrapers.espn.bronze_rows import BatchStamp, MatchPayload, match_row
from scrapers.espn.criterion import (
    DAILY_CRITERION_SQL,
    DayResult,
    day_result,
    pct,
    render_daily_criterion_sql,
    render_summary_line,
    render_wave_duration_sql,
    render_wave_line,
    summarize_days,
    targets_literal,
)
from scrapers.espn.denominator import load_denominator

pytestmark = pytest.mark.unit

DAY = "2026-10-11"
KICK = datetime(2026, 10, 10, 15, 0)  # deadline 11.10 17:00
TARGETS = load_denominator().targets()
YOUTH = "concacaf.u23"
WOMEN = "aus.w.1"

MATCH_DDL = (
    "CREATE TABLE bronze.espn_match (competition_slug varchar, event_id bigint, "
    "kickoff timestamp, status varchar, played_final boolean, terminal_nonplayed boolean, "
    "disposition varchar, lineup_state varchar, team_stats_state varchar, "
    "first_published_at timestamp, status_checked_at timestamp, duplicate_of varchar)"
)
_COLUMNS = (
    "competition_slug", "event_id", "kickoff", "status", "played_final",
    "terminal_nonplayed", "disposition", "lineup_state", "team_stats_state",
    "first_published_at", "status_checked_at", "duplicate_of",
)


def _row(event_id, *, slug="eng.1", kickoff=KICK, published=timedelta(hours=20),
         status="STATUS_FULL_TIME", played=True, nonplayed=False, disposition="captured",
         lineup="captured", team="captured", checked=timedelta(hours=3), duplicate_of=None):
    return {
        "competition_slug": slug, "event_id": event_id, "kickoff": kickoff,
        "status": status, "played_final": played, "terminal_nonplayed": nonplayed,
        "disposition": disposition, "lineup_state": lineup, "team_stats_state": team,
        "first_published_at": kickoff + published if published is not None else None,
        "status_checked_at": kickoff + checked if checked is not None else None,
        "duplicate_of": duplicate_of,
    }


def _not_played(event_id, status, *, checked, nonplayed=False, disposition=None, **kw):
    return _row(event_id, status=status, played=False, nonplayed=nonplayed,
                disposition=disposition, lineup="pending", team="pending", published=None,
                checked=checked, **kw)


@pytest.fixture()
def meter():
    sqlglot = pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute(MATCH_DDL)

    def load(rows):
        con.executemany(
            f"INSERT INTO bronze.espn_match VALUES ({', '.join('?' * len(_COLUMNS))})",
            [[row[name] for name in _COLUMNS] for row in rows],
        )

    def run(day=DAY):
        sql = sqlglot.transpile(
            render_daily_criterion_sql(day, TARGETS), read="trino", write="duckdb"
        )[0].replace("iceberg.bronze.", "bronze.")
        rows = con.execute(sql).fetchall()
        return day_result(day, rows), {row[0]: row[1:] for row in rows}

    run.load = load
    return run


def test_on_time_late_and_every_exclusion_on_a_synthetic_day(meter):
    meter.load([
        _row(1),                                              # on time
        _row(2, published=timedelta(hours=27)),               # published after the deadline
        _row(3, published=timedelta(hours=26)),               # exactly at the deadline: on time
        _row(4, lineup="malformed", published=None),          # Summary malformed: miss
        _row(5, team="pending", published=None),              # no Summary yet: miss
        _row(6, lineup="valid_empty", team="valid_empty"),    # legitimately empty: on time
        # POSTPONED confirmed after kickoff: out of due.
        _not_played(7, "STATUS_POSTPONED", checked=timedelta(hours=3)),
        # POSTPONED seen only before kickoff, never after kickoff + 2 h: a miss.
        _not_played(8, "STATUS_POSTPONED", checked=timedelta(hours=-5)),
        # CANCELED confirmed after kickoff: out of due.
        _not_played(9, "STATUS_CANCELED", checked=timedelta(hours=1), nonplayed=True),
        # Still SCHEDULED, read after kickoff + 2 h: a miss, but checked.
        _not_played(10, "STATUS_SCHEDULED", checked=timedelta(hours=4)),
        # Withdrawn (core 404): its own count, out of due.
        _not_played(11, "STATUS_SCHEDULED", checked=timedelta(hours=-1), disposition="withdrawn"),
        # Youth / women tournaments and duplicates are outside the percentage.
        _row(12, slug=YOUTH, published=timedelta(hours=40)),
        _row(13, slug=WOMEN, published=None),
        _row(14, duplicate_of="espn:1", published=None),
        # Moved: counts from its new kickoff (deadline 13.10), not in 11.10.
        _row(15, kickoff=datetime(2026, 10, 12, 12), published=None, played=False,
             status="STATUS_SCHEDULED", lineup="pending", team="pending", disposition="moved"),
        # Other tournament on the same day.
        _row(16, slug="ger.2", published=timedelta(hours=2)),
    ])

    result, by_slug = meter()

    assert result == DayResult(DAY, due=9, ok=4, withdrawn=1, nonplayed=2, unchecked=1)
    assert by_slug["eng.1"] == (8, 3, 1, 2, 1)
    assert by_slug["ger.2"] == (1, 1, 0, 0, 0)
    assert YOUTH not in by_slug and WOMEN not in by_slug
    moved, _ = meter("2026-10-13")
    assert (moved.due, moved.ok, moved.unchecked) == (1, 0, 0)


def test_unconfirmed_status_after_kickoff_is_a_miss(meter):
    # The collection stopped: the stored status is from before kickoff.
    meter.load([
        _not_played(1, "STATUS_SCHEDULED", checked=timedelta(hours=-20)),
        _not_played(2, "STATUS_SCHEDULED", checked=None),
    ])
    result, _ = meter()
    assert (result.due, result.ok, result.unchecked) == (2, 0, 2)
    assert result.pct == Decimal("0.0")


def test_republication_does_not_move_first_published_at():
    from tests.unit.scrapers.test_espn_bronze_rows import RAW
    from tests.unit.scrapers.test_espn_probes import _parse

    schedule, summary = _parse("summary_eng1_2020.json")
    first = BatchStamp("b1", datetime(2026, 10, 11, 9))
    later = BatchStamp("b2", datetime(2026, 10, 12, 9))
    row1 = match_row(schedule, summary, raw=RAW, stamp=first)
    assert row1["first_published_at"] == first.ingested_at
    # The wave carries the stored value into the next batch of the match.
    payload = MatchPayload(schedule, summary, RAW, first_published_at=row1["first_published_at"])
    row2 = match_row(schedule, summary, raw=RAW, stamp=later,
                     first_published_at=payload.first_published_at)
    assert row2["first_published_at"] == first.ingested_at
    assert row2["_ingested_at"] == later.ingested_at
    # Not published yet: no Summary, or a malformed part.
    assert match_row(schedule, None, raw=RAW, stamp=first)["first_published_at"] is None
    broken = _parse("summary_eng1_2020.json")[1]
    from dataclasses import replace
    from scrapers.espn.parser_contracts import EntityParseState
    malformed = replace(broken, lineup_state=EntityParseState.MALFORMED)
    assert match_row(schedule, malformed, raw=RAW, stamp=first)["first_published_at"] is None


def test_days_without_deadlines_are_neutral_and_do_not_break_the_streak():
    days = [
        DayResult("2026-10-10", due=0, ok=0),
        DayResult("2026-10-11", due=10, ok=10),
        DayResult("2026-10-12", due=0, ok=0),
        DayResult("2026-10-13", due=8, ok=8),
        DayResult("2026-10-14", due=12, ok=12),
    ]
    assert summarize_days(days) == 3
    assert summarize_days(days + [DayResult("2026-10-15", due=10, ok=9)]) == 0


def test_streak_threshold_uses_the_exact_ratio_not_the_rounded_pct():
    near_miss = DayResult("2026-10-11", due=96, ok=95)
    assert near_miss.pct == Decimal("99.0")
    assert summarize_days([near_miss]) == 0
    assert summarize_days([DayResult("2026-10-11", due=100, ok=99)]) == 1
    assert pct(0, 0) is None


def test_summary_line_names_zero_played_as_no_data():
    empty = render_summary_line(DayResult(DAY, 0, 0), 0)
    assert empty == "• ESPN за сутки 11.10: нет сыгранных в новых таблицах ⚠️ (серия 0)"
    assert "%" not in empty
    line = render_summary_line(DayResult(DAY, 200, 199, withdrawn=2, unchecked=1), 3)
    assert line == (
        "• ESPN: сыгранных за сутки 11.10 200, ≤ 24 ч 99.5 % (199/200), серия 3 дн. "
        "(веха 1: 3 дня ≥ 99 %) ✅; статус не перепроверен после kickoff + 2 ч: 1 "
        "(снято с календаря 2)"
    )
    assert render_summary_line(DayResult(DAY, 96, 95), 0).endswith("‼️")


def test_wave_duration_sql_and_line(meter):
    sqlglot = pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA ops")
    con.execute(
        "CREATE TABLE ops.espn_wave_tournament_v1 (run_id varchar, wave_started_at timestamp, "
        "wave_finished_at timestamp, slug varchar, season_year integer, state varchar, "
        "matches integer, first_error varchar)"
    )
    start = datetime(2026, 10, 11)
    for hour, minutes, state in ((0, 30, "green"), (6, 40, "red"), (12, 20, "green")):
        begun = start + timedelta(hours=hour)
        con.execute(
            "INSERT INTO ops.espn_wave_tournament_v1 VALUES (?, ?, ?, '(wave)', NULL, ?, 0, NULL)",
            [f"r{hour}", begun, begun + timedelta(minutes=minutes), state],
        )
        con.execute(
            "INSERT INTO ops.espn_wave_tournament_v1 VALUES (?, ?, ?, 'eng.1', 2026, ?, 1, NULL)",
            [f"r{hour}", begun, begun + timedelta(hours=5), state],
        )
    sql = sqlglot.transpile(render_wave_duration_sql(DAY), read="trino", write="duckdb")[0]
    (row,) = con.execute(sql.replace("iceberg.ops.", "ops.")).fetchall()
    assert (row[0], row[1], row[3]) == (3, 1, 2400)
    assert 1800 <= row[2] <= 2400
    assert render_wave_line(DAY, (3, 1, 2400, 2400)) == (
        "• ESPN волны 11.10: 3, красных 1, p95 40 мин (макс. 40 мин; порог 60) ✅"
    )
    assert render_wave_line(DAY, (4, 0, 3700, 3900)).endswith("‼️")
    assert render_wave_line(DAY, None) == "• ESPN волны 11.10: волн нет"


def test_sql_shape_and_target_list():
    sql = render_daily_criterion_sql(DAY, {"eng.1", "arg.1"})
    assert "competition_slug IN ('arg.1', 'eng.1')" in sql
    assert "kickoff + INTERVAL '26' HOUR >= TIMESTAMP '2026-10-11 00:00:00'" in sql
    assert "duplicate_of IS NULL" in DAILY_CRITERION_SQL
    assert len(TARGETS) == 163
    with pytest.raises(ValueError):
        targets_literal([])
    with pytest.raises(ValueError):
        render_daily_criterion_sql("11.10.2026", TARGETS)
