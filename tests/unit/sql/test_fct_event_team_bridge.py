"""WhoScored event team identity must bridge numeric ids through schedule."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


pytestmark = pytest.mark.unit

SQL_PATH = (
    Path(__file__).resolve().parents[3] / "dags" / "sql" / "gold" / "fct_event.sql"
)
SQL = SQL_PATH.read_text(encoding="utf-8")


def _duckdb_sql() -> str:
    replacements = {
        "iceberg.silver.whoscored_events_spadl": "silver_whoscored_events_spadl",
        "iceberg.bronze.whoscored_schedule_current": "bronze_whoscored_schedule_current",
        "iceberg.silver.xref_team": "silver_xref_team",
        "iceberg.silver.xref_player": "silver_xref_player",
        "iceberg.silver.xref_match": "silver_xref_match",
    }
    rendered = SQL
    for source, target in replacements.items():
        rendered = rendered.replace(source, target)
    return rendered


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE silver_whoscored_events_spadl (
            event_id VARCHAR,
            match_id VARCHAR,
            team_id_raw VARCHAR,
            team_name_raw VARCHAR,
            player_id_raw VARCHAR,
            period VARCHAR,
            expanded_minute INTEGER,
            x DOUBLE,
            y DOUBLE,
            end_x DOUBLE,
            end_y DOUBLE,
            action_canonical VARCHAR,
            action_source VARCHAR,
            action_version VARCHAR,
            _action_confidence VARCHAR,
            _action_source_note VARCHAR,
            outcome_success BOOLEAN,
            league VARCHAR,
            season VARCHAR
        );
        CREATE TABLE bronze_whoscored_schedule_current (
            league VARCHAR,
            season VARCHAR,
            home_team_id BIGINT,
            home_team VARCHAR,
            away_team_id BIGINT,
            away_team VARCHAR
        );
        CREATE TABLE silver_xref_team (
            canonical_id VARCHAR,
            source VARCHAR,
            source_id VARCHAR,
            league VARCHAR,
            season VARCHAR,
            confidence VARCHAR
        );
        CREATE TABLE silver_xref_player (
            canonical_id VARCHAR,
            source VARCHAR,
            source_id VARCHAR,
            league VARCHAR,
            season VARCHAR
        );
        CREATE TABLE silver_xref_match (
            canonical_id VARCHAR,
            source VARCHAR,
            source_id VARCHAR,
            league VARCHAR,
            season VARCHAR,
            confidence VARCHAR
        );
        """
    )
    return conn


def _insert_event(
    conn: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
    match_id: str,
    team_id: int,
    short_name: str,
    league: str,
) -> None:
    conn.execute(
        """
        INSERT INTO silver_whoscored_events_spadl VALUES (
            ?, ?, ?, ?, NULL, 'SecondHalf', 55,
            50.0, 40.0, 60.0, 45.0,
            'pass', 'whoscored_spadl_proprietary_v1', 'v1',
            'high', 'Pass', true, ?, '2526'
        )
        """,
        [event_id, match_id, str(team_id), short_name, league],
    )
    conn.execute(
        "INSERT INTO silver_xref_match VALUES (?, 'whoscored', ?, ?, '2526', 'date_team_match')",
        [f"canonical_{match_id}", match_id, league],
    )


def test_bridge_contract_is_manifest_filtered_and_conflict_safe():
    assert SQL.count("FROM iceberg.bronze.whoscored_schedule_current") == 2
    assert "COUNT(DISTINCT ws_team_name) AS team_name_count" in SQL
    assert "WHEN wsn.team_name_count = 1 THEN wsn.ws_team_name" in SQL
    assert "WHEN wsn.team_name_count IS NULL THEN e.team_name_raw" in SQL
    assert "wsn.ws_team_id = e.team_id_raw" in SQL


def test_short_event_names_resolve_through_numeric_schedule_ids_without_fanout():
    conn = _connection()
    try:
        samples = [
            (
                "e1",
                "m1",
                101,
                "Man City",
                "Manchester City",
                "manchester_city",
                "ENG-Premier League",
            ),
            (
                "e2",
                "m2",
                102,
                "PSG",
                "Paris Saint-Germain",
                "paris_saint_germain",
                "FRA-Ligue 1",
            ),
            (
                "e3",
                "m3",
                103,
                "Bayern",
                "Bayern Munich",
                "bayern_munich",
                "GER-Bundesliga",
            ),
            ("e4", "m4", 104, "RBL", "RB Leipzig", "rb_leipzig", "GER-Bundesliga"),
        ]
        for event_id, match_id, team_id, short, full, canonical, league in samples:
            _insert_event(
                conn,
                event_id=event_id,
                match_id=match_id,
                team_id=team_id,
                short_name=short,
                league=league,
            )
            conn.execute(
                "INSERT INTO bronze_whoscored_schedule_current VALUES (?, '2526', ?, ?, 999, 'Other')",
                [league, team_id, full],
            )
            # Repeated fixtures are normal; they must not duplicate event rows.
            conn.execute(
                "INSERT INTO bronze_whoscored_schedule_current VALUES (?, '2526', ?, ?, 998, 'Other Two')",
                [league, team_id, full],
            )
            conn.execute(
                "INSERT INTO silver_xref_team VALUES (?, 'whoscored', ?, ?, '2526', 'name_alias')",
                [canonical, full, league],
            )

        cursor = conn.execute(_duckdb_sql())
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        by_event = {row[columns.index("event_id")]: row for row in rows}

        assert len(rows) == 4
        for event_id, _match_id, _team_id, _short, _full, canonical, _league in samples:
            assert by_event[event_id][columns.index("team_id")] == canonical
    finally:
        conn.close()


def test_conflicting_numeric_mapping_preserves_event_but_resolves_no_team():
    conn = _connection()
    try:
        _insert_event(
            conn,
            event_id="conflict",
            match_id="m-conflict",
            team_id=104,
            short_name="RBL",
            league="GER-Bundesliga",
        )
        conn.execute(
            """
            INSERT INTO bronze_whoscored_schedule_current VALUES
                ('GER-Bundesliga', '2526', 104, 'RB Leipzig', 1, 'Other'),
                ('GER-Bundesliga', '2526', 104, 'RasenBallsport Leipzig', 2, 'Other Two')
            """
        )
        conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
                ('rb_leipzig', 'whoscored', 'RB Leipzig', 'GER-Bundesliga', '2526', 'name_alias'),
                ('bad_short_fallback', 'whoscored', 'RBL', 'GER-Bundesliga', '2526', 'name_alias')
            """
        )

        cursor = conn.execute(_duckdb_sql())
        row = cursor.fetchone()
        columns = [item[0] for item in cursor.description]

        assert row is not None
        assert row[columns.index("event_id")] == "conflict"
        assert row[columns.index("team_id")] is None
    finally:
        conn.close()
