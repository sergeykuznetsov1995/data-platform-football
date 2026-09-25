"""The match-deadline rule shared by the refresh queue and the meter (#1359)."""

import sqlite3

import pytest

from scrapers.sofascore.match_deadline import (
    MATCH_DEADLINE_SQL,
    match_deadline,
    match_deadline_sql,
)

START = 1_787_900_000
DAY = 24 * 3600

CASES = [
    # (change, deadline)
    (START + 45 * 60, START + 45 * 60 + DAY),          # exactly 45 min: the source end
    (START + 4 * 3600, START + 4 * 3600 + DAY),        # exactly 4 h: the source end
    (START + 45 * 60 - 1, START + 2 * 3600 + DAY),     # too early: start + 2 h
    (START + 4 * 3600 + 1, START + 2 * 3600 + DAY),    # too late: start + 2 h
    (None, START + 2 * 3600 + DAY),                    # no change timestamp
    (START + 100 * 60, START + 100 * 60 + DAY),        # inside the window
]


@pytest.mark.unit
@pytest.mark.parametrize("change, deadline", CASES)
def test_match_deadline_boundaries(change, deadline):
    assert match_deadline(START, change) == deadline
    # Bronze stores both timestamps as double.
    assert match_deadline(float(START), None if change is None else float(change)) == deadline


@pytest.mark.unit
def test_match_deadline_without_a_start_is_none():
    assert match_deadline(None, START) is None


@pytest.mark.unit
@pytest.mark.parametrize("change, deadline", CASES)
def test_the_sql_expression_computes_the_same_deadline(change, deadline):
    connection = sqlite3.connect(":memory:")
    row = connection.execute(
        f"SELECT {match_deadline_sql('s', 'c')} FROM (SELECT ? AS s, ? AS c)",
        (float(START), None if change is None else float(change)),
    ).fetchone()

    assert int(row[0]) == deadline


@pytest.mark.unit
def test_the_sql_defaults_to_the_schedule_columns():
    assert match_deadline_sql() == MATCH_DEADLINE_SQL.format(
        start="start_timestamp", change="changes_change_timestamp"
    )
