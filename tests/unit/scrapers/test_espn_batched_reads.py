from collections import Counter
from datetime import datetime, timedelta, timezone

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from scrapers.espn.deep_store import DEPTH_COLUMNS, DEPTH_TABLE, Depth, load_depths
from scrapers.espn.recheck import build_candidates_sql, select_candidates


@pytest.fixture
def db():
    with duckdb.connect(":memory:") as conn:
        conn.execute("ATTACH ':memory:' AS iceberg")
        yield conn


def test_depth_read_scopes_exact_editions_and_rejects_duplicate_rows(db):
    db.execute("CREATE SCHEMA iceberg.ops")
    columns = ", ".join(f"{name} {kind}" for name, kind in DEPTH_COLUMNS)
    db.execute(f"CREATE TABLE {DEPTH_TABLE} ({columns})")
    insert = f"INSERT INTO {DEPTH_TABLE} (competition_slug, source_season_year, plays_count)"
    db.execute(f"{insert} VALUES ('a', 2024, 2), ('a', 2025, 3), ('b', 2025, 1), "
               "('b', 2024, 5), ('b', 2024, 6)")
    reads = []

    class Reader:
        def execute_query(self, sql, params):
            reads.append(sql)
            return db.execute(sql, params).fetchall()

    reader = Reader()
    assert load_depths(reader, []) == {}
    assert load_depths(reader, [('unknown', None)]) == {}
    assert not reads

    editions = [('a', 2024), ('a', 2025), ('b', 2025), ('missing', 2024),
                ('a', 2024), ('unknown', None)]
    assert load_depths(reader, editions) == {
        ('a', 2024): Depth('a', 2024, plays_count=2),
        ('a', 2025): Depth('a', 2025, plays_count=3),
        ('b', 2025): Depth('b', 2025, plays_count=1),
        ('missing', 2024): None,
    }
    assert len(reads) == 1
    with pytest.raises(ValueError, match="duplicate depth registry key"):
        load_depths(reader, editions + [('b', 2024)])
    assert len(reads) == 2


def test_candidates_single_match_scan_and_overlapping_kinds(db):
    db.execute("CREATE SCHEMA iceberg.bronze")
    db.execute("""CREATE TABLE iceberg.bronze.espn_match (
        competition_slug varchar, season_year integer, event_id bigint, kickoff timestamp,
        played_final boolean, lineup_state varchar, team_stats_state varchar, events_state varchar,
        disposition varchar, rechecked_at timestamp, first_published_at timestamp)""")
    now = datetime(2026, 11, 20, tzinfo=timezone.utc)
    midnight = now.replace(tzinfo=None)
    day = timedelta(days=1)
    tick = timedelta(microseconds=1)

    def match(slug, event, age, *, published=None, lineup="captured", team_stats="captured",
              disposition="valid", year=2026, played=True, checked=None):
        first = midnight - published if published is not None else None
        return (slug, year, event, midnight - age, played, lineup, team_stats, "captured",
                disposition, checked, first)

    db.executemany("INSERT INTO iceberg.bronze.espn_match VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        # Exactly half of league a captured lineups, including a different season at day 30.
        match("a", 100, 10 * day, published=day, lineup="absent"),
        match("a", 101, 30 * day, year=2025),
        match("a", 120, 0 * day, published=3 * day, lineup="absent"),
        match("a", 140, 8 * day, published=day, lineup="pending", disposition="valid_empty"),
        match("a", 160, 8 * day, published=day, played=False, lineup="absent"),
        match("a", 161, 30 * day + tick, lineup="absent"),
        # Recheck age endpoints are inclusive. Prior rechecks and NULL join keys are ineligible.
        match("b", 201, 7 * day, disposition="lineup_anomaly"),
        match("b", 202, 7 * day - tick, disposition="valid_empty"),
        match("b", 203, 10 * day + tick, disposition="valid_empty"),
        match("b", 204, 8 * day, disposition="valid_empty", checked=midnight),
        match("b", 205, 8 * day, disposition="valid_empty", lineup=None),
        match(None, 220, 8 * day, disposition="valid_empty", published=day),
        # Samples include the newer publication boundary and exclude the older one.
        match("c", 300, 12 * day, published=2 * day),
        match("c", 320, 12 * day, published=2 * day - tick),
        match("c", 340, 12 * day, published=4 * day),
        match("c", 360, 12 * day, published=4 * day - tick),
        match("c", 380, 12 * day, published=day - tick),
        match("c", 400, 12 * day, published=3 * day - tick),
        match("c", 401, 12 * day, published=day),
        # NULL team stats contribute zero to the league share. One of three is below threshold.
        match("d", 501, 8 * day, team_stats="absent"),
        match("d", 502, 20 * day, team_stats=None),
        match("d", 503, 20 * day),
    ])
    sql = build_candidates_sql(now)
    tree = sqlglot.parse_one(sql, read="trino")
    assert sum(table.name == "espn_match" for table in tree.find_all(exp.Table)) == 1
    duck_sql = tree.sql(dialect="duckdb")
    plan = dict(db.execute("EXPLAIN " + duck_sql).fetchall())["physical_plan"]
    assert plan.count("SEQ_SCAN") == 1, plan
    rows = db.execute(duck_sql).fetchall()
    assert Counter(rows) == Counter([
        ("a", 2026, 100, "recheck"), ("a", 2026, 100, "sample_24h"),
        ("a", 2026, 120, "sample_72h"), ("a", 2026, 140, "sample_24h"),
        ("b", 2026, 201, "recheck"), (None, 2026, 220, "sample_24h"),
        ("c", 2026, 320, "sample_24h"), ("c", 2026, 360, "sample_72h"),
    ])
    assert select_candidates(rows)[100] == ("a", 2026, "recheck")
