"""Executable phase classification for Silver FBref events (#901)."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SQL = ROOT / "dags" / "sql" / "silver" / "fbref_match_events.sql"

pytestmark = pytest.mark.unit


def test_regulation_extra_time_and_shootout_are_separate():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE schedule (
            match_url VARCHAR, score VARCHAR, _ingested_at TIMESTAMP,
            _batch_id VARCHAR
        )"""
    )
    con.execute(
        """CREATE TABLE events (
            match_id VARCHAR, minute VARCHAR, event_type VARCHAR,
            player VARCHAR, player_id VARCHAR, team VARCHAR, team_side VARCHAR,
            secondary_player VARCHAR, secondary_player_id VARCHAR,
            _ingested_at TIMESTAMP, _batch_id VARCHAR,
            league VARCHAR, season BIGINT, source_season_id VARCHAR
        )"""
    )
    con.execute(
        "INSERT INTO schedule VALUES ('/en/matches/aaaaaaaa/Test', '(4) 1–1 (5)', TIMESTAMP '2026-07-01', 'b1')"
    )
    con.executemany(
            "INSERT INTO events VALUES ('aaaaaaaa', ?, ?, ?, ?, 'Home', 'home', NULL, NULL, TIMESTAMP '2026-07-01', 'b1', 'ENG-Premier League', 2024, '2024-2025')",
        [
            ("70", "penalty", "Regular Kicker", "p1"),
            ("105", "goal", "ET Scorer", "p2"),
            ("", "penalty", "Shootout Kicker", "p3"),
        ],
    )
    sql = SQL.read_text(encoding="utf-8")
    sql = sql.replace("iceberg.bronze.fbref_schedule", "schedule")
    sql = sql.replace("iceberg.bronze.fbref_match_events", "events")
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")
    out = con.execute(sql).fetch_df().set_index("player")

    assert out.loc["Regular Kicker", "event_phase"] == "regulation"
    assert bool(out.loc["Regular Kicker", "is_shootout"]) is False
    assert out.loc["ET Scorer", "event_phase"] == "extra_time"
    assert out.loc["Shootout Kicker", "event_phase"] == "shootout"
    assert bool(out.loc["Shootout Kicker", "is_shootout"]) is True
    con.close()
