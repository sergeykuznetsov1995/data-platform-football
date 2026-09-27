"""#1474: WhoScored repository SQL executed on real rows (Trino SQL -> DuckDB)."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")

from scrapers.whoscored.parsers import PARSER_VERSION  # noqa: E402
from scrapers.whoscored.repository import WhoScoredRepository  # noqa: E402

LEAGUE = "ENG-League Two"
SEASON = "2526"
NOW = datetime.now()


def _connection():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA iceberg.bronze")
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_schedule_current (
            league VARCHAR, season VARCHAR, game_id BIGINT, game VARCHAR,
            date TIMESTAMP, status BIGINT, match_is_opta BOOLEAN,
            is_lineup_confirmed BOOLEAN, stage_id BIGINT,
            home_score BIGINT, away_score BIGINT, _ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_match_ingest_manifest (
            league VARCHAR, season VARCHAR, game_id BIGINT, state VARCHAR,
            batch_id VARCHAR, raw_uri VARCHAR, payload_sha256 VARCHAR,
            parser_version VARCHAR, availability_version VARCHAR,
            retry_after TIMESTAMP, attempt_no BIGINT,
            fetched_at TIMESTAMP, completed_at TIMESTAMP
        )
        """
    )
    return con


def _game(con, game_id, stage_id, days_ago, *, lineup=True):
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_schedule_current VALUES "
        "(?, ?, ?, ?, ?, 6, TRUE, ?, ?, 1, 0, ?)",
        [
            LEAGUE,
            SEASON,
            game_id,
            f"game {game_id}",
            NOW - timedelta(days=days_ago),
            lineup,
            stage_id,
            NOW,
        ],
    )


def _attempt(con, game_id, state, fetched_days_ago):
    fetched = NOW - timedelta(days=fetched_days_ago)
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_match_ingest_manifest VALUES "
        "(?, ?, ?, ?, ?, 's3://raw/x', 'sha', ?, NULL, NULL, 1, ?, ?)",
        [
            LEAGUE,
            SEASON,
            game_id,
            state,
            f"ws2-{game_id}-{state}-{fetched_days_ago}",
            PARSER_VERSION,
            fetched,
            fetched,
        ],
    )


def _candidates(con):
    con.execute(
        """
        CREATE OR REPLACE VIEW iceberg.bronze.whoscored_match_ingest_latest AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY league, season, game_id ORDER BY fetched_at DESC
            ) AS rn
            FROM iceberg.bronze.whoscored_match_ingest_manifest
        ) WHERE rn = 1
        """
    )

    def execute_query(sql):
        rendered = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        return con.execute(rendered).fetchall()

    trino = MagicMock()
    trino.execute_query.side_effect = execute_query
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    return [
        candidate.game_id
        for candidate in repository.list_match_candidates(LEAGUE, SEASON, limit=300)
    ]


@pytest.mark.unit
def test_available_stage_offers_new_games_and_one_na_reprobe_after_72h():
    con = _connection()
    for game_id, days_ago in ((1, 10), (2, 9), (3, 8), (4, 7), (5, 6)):
        _game(con, game_id, 100, days_ago)
    _game(con, 6, 100, 1, lineup=False)
    _attempt(con, 1, "success", 9)
    # 2: never fetched.
    _attempt(con, 3, "not_available", 4)  # one NA, 4 days ago -> re-probe
    _attempt(con, 4, "not_available", 1)  # one NA, yesterday -> wait
    _attempt(con, 5, "not_available", 10)
    _attempt(con, 5, "not_available", 5)  # two NA -> final

    assert _candidates(con) == [3, 2]


@pytest.mark.unit
def test_unavailable_stage_reprobes_its_latest_game_every_30_days():
    con = _connection()
    # Stage 200: two NA with a lineup long ago -> unavailable, probe is due.
    _game(con, 10, 200, 50)
    _game(con, 11, 200, 60)
    for game_id in (10, 11):
        _attempt(con, game_id, "not_available", 45)
        _attempt(con, game_id, "not_available", 40)
    # Stage 300: unavailable, last verdict 5 days ago -> no probe yet.
    _game(con, 20, 300, 20)
    _game(con, 21, 300, 25)
    for game_id in (20, 21):
        _attempt(con, game_id, "not_available", 5)

    # The latest game of stage 200 is the probe even with two NA verdicts.
    assert _candidates(con) == [10]


@pytest.mark.unit
def test_unknown_stage_offers_only_its_latest_played_game():
    con = _connection()
    _game(con, 30, 400, 2)
    _game(con, 31, 400, 3)
    _game(con, 32, 400, 4)

    assert _candidates(con) == [30]


def _scope_views_sql():
    trino = MagicMock()
    trino.table_exists.return_value = True
    trino.execute_query.return_value = []
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    repository._ensure_scope_schema(create_views=True)
    return [call.args[0] for call in trino._execute.call_args_list]


def _scope_bundle(con, batch_id, group, counts, completed_days_ago):
    import json

    completed = NOW - timedelta(days=completed_days_ago)
    con.execute(
        "INSERT INTO iceberg.bronze.whoscored_scope_ingest_manifest VALUES "
        "(?, ?, ?, ?, 'success', ?, ?, ?)",
        [LEAGUE, SEASON, group, batch_id, json.dumps(counts), completed, completed],
    )


@pytest.mark.unit
def test_stage_stats_stay_current_across_the_season_stages_split():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA iceberg.bronze")
    con.execute(
        """
        CREATE TABLE iceberg.bronze.whoscored_scope_ingest_manifest (
            league VARCHAR, season VARCHAR, entity_group VARCHAR,
            batch_id VARCHAR, state VARCHAR, entity_counts_json VARCHAR,
            completed_at TIMESTAMP, _ingested_at TIMESTAMP
        )
        """
    )
    for table in ("whoscored_team_stage_stats", "whoscored_schedule"):
        con.execute(
            f"CREATE TABLE iceberg.bronze.{table} (league VARCHAR, "
            "season VARCHAR, entity_key VARCHAR, _scope_batch_id VARCHAR)"
        )
    for sql in _scope_views_sql():
        if (
            "whoscored_scope_ingest_latest_success AS" in sql
            or "whoscored_team_stage_stats_current AS" in sql
            or "whoscored_schedule_current AS" in sql
        ):
            con.execute(sqlglot.transpile(sql, read="trino", write="duckdb")[0])

    def rows(table):
        return sorted(
            row[0]
            for row in con.execute(
                f"SELECT _scope_batch_id FROM iceberg.bronze.{table}_current"
            ).fetchall()
        )

    def insert(table, batch_id):
        con.execute(
            f"INSERT INTO iceberg.bronze.{table} VALUES (?, ?, 'k', ?)",
            [LEAGUE, SEASON, batch_id],
        )

    # Pre-split season bundle published schedule and stage statistics.
    _scope_bundle(
        con,
        "wss2-old",
        "season",
        {"whoscored_schedule": 1, "whoscored_team_stage_stats": 1},
        10,
    )
    insert("whoscored_schedule", "wss2-old")
    insert("whoscored_team_stage_stats", "wss2-old")
    # First daily bundle after the split: schedule only.
    _scope_bundle(con, "wss2-daily", "season", {"whoscored_schedule": 1}, 1)
    insert("whoscored_schedule", "wss2-daily")

    assert rows("whoscored_schedule") == ["wss2-daily"]
    assert rows("whoscored_team_stage_stats") == ["wss2-old"]

    # The weekly stages bundle replaces the pre-split stage statistics.
    _scope_bundle(con, "wss2-stages", "stages", {"whoscored_team_stage_stats": 1}, 0)
    insert("whoscored_team_stage_stats", "wss2-stages")

    assert rows("whoscored_team_stage_stats") == ["wss2-stages"]
    assert rows("whoscored_schedule") == ["wss2-daily"]


@pytest.mark.unit
def test_stages_bundle_cannot_shrink_the_pre_split_stage_snapshot():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [],
        [('{"whoscored_schedule": 5, "whoscored_team_stage_stats": 3}',)],
    ]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    with pytest.raises(ValueError, match="published snapshot cannot shrink"):
        repository.commit_scope_bundle(
            league=LEAGUE,
            season=SEASON,
            entity_group="stages",
            datasets={"whoscored_team_stage_stats": [{"entity_key": "a"}]},
            distinct_keys={"whoscored_team_stage_stats": "entity_key"},
            payload_sha256="a" * 64,
            raw_uris=["s3://raw/season.html.gz"],
        )

    previous_sql = trino.execute_query.call_args_list[1].args[0]
    assert "entity_group" not in previous_sql
    assert (
        "json_extract_scalar(entity_counts_json, '$.whoscored_team_stage_stats')"
        " IS NOT NULL"
    ) in previous_sql
