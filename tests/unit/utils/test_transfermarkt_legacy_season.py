import hashlib
import json
import sqlite3
from contextlib import closing

import pytest

from utils import transfermarkt_legacy_season


def _digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest().upper()


@pytest.fixture
def audit(monkeypatch):
    table = "iceberg.bronze.transfermarkt_coaches"
    rows = [("TM-BRP4", "2526", "test-batch", value) for value in ("z", "a")]
    hashes = [_digest(json.dumps(row, separators=(",", ":"))) for row in rows]
    monkeypatch.setitem(
        transfermarkt_legacy_season._BATCHES, table,
        (_digest("test-batch"), len(rows), _digest("".join(sorted(hashes)))),
    )
    predicate = transfermarkt_legacy_season.historical_season_predicate(
        table, "bronze", "competitions", "editions", "'current'"
    )
    # SQLite uses a JSON array for the synthetic physical row and aggregate.
    predicate = predicate.replace(
        "json_format(CAST((SELECT r.*) AS JSON))",
        "json_array(r.league, r.season, r._batch_id, r.payload)",
    )
    predicate = predicate.replace("array_agg(", "json_group_array(")
    with closing(sqlite3.connect(":memory:")) as db:
        db.create_function("to_utf8", 1, lambda s: None if s is None else s.encode("utf-8"))
        db.create_function("sha256", 1, lambda b: None if b is None else hashlib.sha256(b).digest())
        db.create_function("to_hex", 1, lambda b: None if b is None else b.hex().upper())
        db.create_function("array_sort", 1, lambda a: json.dumps(sorted(json.loads(a))))
        db.create_function("array_join", 2, lambda a, sep: sep.join(json.loads(a)))
        db.executescript("""
            CREATE TABLE bronze (league, season, _batch_id, payload);
            CREATE TABLE competitions (registry_snapshot_id, competition_id, canonical_competition_id);
            CREATE TABLE editions (registry_snapshot_id, competition_id, edition_id,
                canonical_season, edition_label, season_format, source_url);
            INSERT INTO competitions VALUES
                ('tm-discovery-e44a99b60bb12ac25ac59641', 'BRP4', NULL),
                ('current', 'BRP4', 'TM-BRP4');
            INSERT INTO editions VALUES
                ('tm-discovery-e44a99b60bb12ac25ac59641', 'BRP4', '2025', '2526', '25/26', 'split_year', 'test-url'),
                ('current', 'BRP4', '2025', '2026', '2026', 'single_year', 'test-url');
        """)
        db.executemany("INSERT INTO bronze VALUES (?, ?, ?, ?)", rows)
        yield db, f"SELECT count(*) FROM bronze b WHERE COALESCE(({predicate}), FALSE)"


@pytest.mark.parametrize("mutation", [
    "UPDATE bronze SET league = NULL",
    "UPDATE bronze SET season = '2026'",
    "UPDATE bronze SET _batch_id = NULL",
    "UPDATE bronze SET payload = 'unattested'",
    "UPDATE bronze SET payload = 'z'",
    "DELETE FROM bronze WHERE payload = 'a'",
    "INSERT INTO bronze SELECT * FROM bronze",
    "UPDATE editions SET source_url = 'different' WHERE registry_snapshot_id = 'current'",
    "UPDATE editions SET source_url = NULL WHERE registry_snapshot_id = 'current'",
    "UPDATE editions SET source_url = ''",
    "UPDATE editions SET edition_id = '2026' WHERE registry_snapshot_id = 'current'",
    "UPDATE editions SET edition_label = '2526' WHERE season_format = 'split_year'",
    "UPDATE editions SET season_format = 'split_year' WHERE registry_snapshot_id = 'current'",
    "UPDATE editions SET canonical_season = '2526' WHERE registry_snapshot_id = 'current'",
    "UPDATE competitions SET canonical_competition_id = 'TM-OTHER' WHERE registry_snapshot_id <> 'current'",
    "UPDATE competitions SET competition_id = 'OTHER' WHERE registry_snapshot_id = 'current'",
    "INSERT INTO competitions VALUES ('current', 'BRP4', 'TM-OTHER')",
    """INSERT INTO editions SELECT registry_snapshot_id, competition_id, edition_id,
        canonical_season, 'other', season_format, source_url FROM editions WHERE registry_snapshot_id = 'current'""",
])
def test_requires_exact_mapping_batch_and_row(audit, mutation):
    db, query = audit
    assert db.execute(query).fetchone() == (2,)
    db.execute(mutation)
    assert db.execute(query).fetchone() == (0,)


@pytest.mark.parametrize(("name", "size"), [
    ("coaches", 8), ("players", 81), ("market_value_history", 41), ("transfers", 685),
])
def test_limits_candidate_before_hash_aggregation(name, size):
    sql = transfermarkt_legacy_season.historical_season_predicate(
        f"iceberg.bronze.transfermarkt_{name}", "bronze FOR VERSION AS OF 11",
        "competitions FOR VERSION AS OF 12", "editions FOR VERSION AS OF 13", "'new'"
    )
    assert "SELECT r.* FROM bronze FOR VERSION AS OF 11 r" in sql
    assert f"LIMIT {size + 1}\n" in sql
    assert f"SELECT count(*) = {size}\n" in sql
    assert "FROM editions FOR VERSION AS OF 13 e" in sql
    assert "JOIN competitions FOR VERSION AS OF 12 c" in sql


def test_unknown_table_fails_closed():
    assert transfermarkt_legacy_season.historical_season_predicate(
        "transfermarkt_coaches", "bronze", "competitions", "editions", "'current'"
    ) == "FALSE"
