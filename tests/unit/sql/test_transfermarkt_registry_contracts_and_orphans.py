"""Focused contracts for the Transfermarkt registry and lossless MV Gold."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / "dags" / "sql" / "silver"
GOLD = ROOT / "dags" / "sql" / "gold"
OM = ROOT / "configs" / "openmetadata" / "descriptions"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _duckdb_sql(path: Path) -> str:
    rendered = sqlglot.transpile(_text(path), read="trino", write="duckdb")[0]
    return (
        rendered.replace("iceberg.bronze.", "bronze.")
        .replace("iceberg.silver.", "silver.")
        .replace("iceberg.gold.", "gold.")
    )


@pytest.mark.parametrize(
    ("filename", "natural_key"),
    [
        ("transfermarkt_competitions_v2.sql", ("competition_id",)),
        (
            "transfermarkt_competition_editions_v2.sql",
            ("competition_id", "edition_id"),
        ),
        (
            "transfermarkt_player_contract_observations_v2.sql",
            ("competition_id", "edition_id", "team_id", "player_id", "observed_at"),
        ),
    ],
)
def test_new_silver_models_have_explicit_dedup_grain(filename, natural_key):
    sql = _text(SILVER / filename)
    partition = sql.split("PARTITION BY", 1)[1].split("ORDER BY", 1)[0]
    for column in natural_key:
        assert column in partition
    assert "ROW_NUMBER()" in sql
    assert "_ingested_at DESC" in sql
    assert "source_body_hash DESC" in sql


def test_edition_model_preserves_single_and_split_year_values():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute(
        '''CREATE TABLE bronze.transfermarkt_competition_editions (
            competition_id varchar, edition_id varchar, edition_label varchar,
            canonical_season varchar, season_format varchar, start_date varchar,
            end_date varchar, active boolean, "current" boolean,
            participant_count varchar, participant_hash varchar,
            source_url varchar, discovered_at varchar, registry_snapshot_id varchar,
            source_body_hash varchar, parser_revision varchar,
            schema_revision varchar, fetched_at timestamp, cycle_id varchar,
            scope_id varchar, _ingested_at timestamp, _batch_id varchar
        )'''
    )
    con.executemany(
        "INSERT INTO bronze.transfermarkt_competition_editions VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "GB1", "2025", "2025/26", "2526", "split_year",
                "2025-08-01", "2026-05-31", True, True, "20", "gb1",
                "/gb1", "2026-07-10 00:00:00", "old", "old", "p1", "s1",
                "2026-07-10 00:00:00", "discovery", "old:GB1:2025",
                "2026-07-10 00:00:00", "old",
            ),
            # Same natural key: the latest complete snapshot must win.
            (
                "GB1", "2025", "2025/26", "2526", "split_year",
                "2025-08-01", "2026-05-31", False, False, "20", "gb1-new",
                "/gb1", "2026-07-11 00:00:00", "new", "new", "p2", "s1",
                "2026-07-11 00:00:00", "discovery", "new:GB1:2025",
                "2026-07-11 00:00:00", "new",
            ),
            (
                "FIWC", "2026", "2026", "2026", "single_year",
                "2026-06-01", "2026-07-31", True, True, "48", "fiwc",
                "/fiwc", "2026-07-11 00:00:00", "new", "wc", "p2", "s1",
                "2026-07-11 00:00:00", "discovery", "new:FIWC:2026",
                "2026-07-11 00:00:00", "new",
            ),
        ],
    )

    rows = con.execute(
        _duckdb_sql(SILVER / "transfermarkt_competition_editions_v2.sql")
    ).fetchall()
    names = [column[0] for column in con.description]
    by_id = {row[1]: dict(zip(names, row)) for row in rows}

    assert len(rows) == 2
    assert by_id["2025"]["canonical_season"] == "2526"
    assert by_id["2025"]["participant_hash"] == "gb1-new"
    assert by_id["2026"]["canonical_season"] == "2026"
    assert by_id["2026"]["season_format"] == "single_year"


def test_contract_model_keeps_explicit_empty_without_carry_forward():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        '''CREATE TABLE bronze.transfermarkt_player_contract_observations (
            competition_id varchar, edition_id varchar, team_id varchar,
            team_name varchar, player_id varchar, contract_until varchar,
            observed_at timestamp, applicability_status varchar, source_url varchar,
            source_body_hash varchar, parser_revision varchar, schema_revision varchar,
            fetched_at timestamp, cycle_id varchar, scope_id varchar,
            _ingested_at timestamp, _batch_id varchar
        )'''
    )
    con.execute(
        '''CREATE TABLE silver.transfermarkt_player_xref_global_v2 (
            player_id varchar, canonical_id varchar, resolution_status varchar
        )'''
    )
    con.executemany(
        "INSERT INTO bronze.transfermarkt_player_contract_observations VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "GB1", "2025", "10", "Club", "100", "2028-06-30",
                "2026-07-11 00:00:00", "ok", "/squad", "old", "p", "s",
                "2026-07-11 00:00:30", "cycle", "GB1:2025",
                "2026-07-11 00:01:00", "a",
            ),
            (
                "GB1", "2025", "10", "Club", "100", None,
                "2026-07-11 00:00:00", "authoritative_empty", "/squad", "new",
                "p", "s", "2026-07-11 00:00:30", "cycle", "GB1:2025",
                "2026-07-11 00:02:00", "b",
            ),
        ],
    )
    con.execute(
        "INSERT INTO silver.transfermarkt_player_xref_global_v2 "
        "VALUES ('100', 'fb_100', 'resolved')"
    )

    row = con.execute(
        _duckdb_sql(SILVER / "transfermarkt_player_contract_observations_v2.sql")
    ).fetchone()
    result = dict(zip([column[0] for column in con.description], row))

    assert result["canonical_id"] == "fb_100"
    assert result["contract_until"] is None
    assert result["applicability_status"] == "authoritative_empty"
    assert result["source_body_hash"] == "new"


def _bootstrap_market_value(con, transfermarkt_table: str) -> None:
    con.execute("CREATE SCHEMA silver")
    con.execute(
        '''CREATE TABLE silver.xref_player (
            canonical_id varchar, source varchar, source_id varchar,
            league varchar, season varchar, confidence varchar
        )'''
    )
    con.executemany(
        "INSERT INTO silver.xref_player VALUES (?,?,?,?,?,?)",
        [
            ("fb_1", "fotmob", "1", "ENG", "2526", "exact"),
            ("fb_a", "fotmob", "2", "ENG", "2526", "exact"),
            ("fb_b", "fotmob", "2", "ENG", "2526", "exact"),
        ],
    )
    con.execute(
        '''CREATE TABLE silver.fotmob_player_market_value_history (
            player_id varchar, value_date date, market_value_eur bigint,
            currency varchar, _bronze_ingested_at timestamp,
            league varchar, season varchar
        )'''
    )
    con.executemany(
        "INSERT INTO silver.fotmob_player_market_value_history VALUES (?,?,?,?,?,?,?)",
        [
            ("1", date(2026, 1, 1), 10, "EUR", "2026-07-11", "ENG", "2526"),
            ("2", date(2026, 1, 1), 20, "EUR", "2026-07-11", "ENG", "2526"),
            ("3", date(2026, 1, 1), 30, "EUR", "2026-07-11", "ENG", "2526"),
        ],
    )
    con.execute(
        f'''CREATE TABLE silver.{transfermarkt_table} (
            player_id varchar, canonical_id varchar, mv_date date,
            value_eur bigint, _bronze_ingested_at timestamp
        )'''
    )
    con.executemany(
        f"INSERT INTO silver.{transfermarkt_table} VALUES (?,?,?,?,?)",
        [
            ("1", "fb_1", date(2026, 1, 1), 11, "2026-07-11"),
            ("2", None, date(2026, 1, 1), 22, "2026-07-11"),
        ],
    )


@pytest.mark.parametrize(
    ("sql_name", "transfermarkt_table"),
    [
        ("fct_player_market_value.sql", "transfermarkt_market_value_history"),
        ("fct_player_market_value_v2.sql", "transfermarkt_market_value_points_v2"),
    ],
)
def test_market_value_gold_retains_unresolved_and_ambiguous_players(
    sql_name, transfermarkt_table
):
    con = duckdb.connect(":memory:")
    _bootstrap_market_value(con, transfermarkt_table)

    rows = con.execute(_duckdb_sql(GOLD / sql_name)).fetchall()
    names = [column[0] for column in con.description]
    facts = [dict(zip(names, row)) for row in rows]
    keys = {(fact["player_id"], fact["source"]) for fact in facts}

    assert len(facts) == 5
    assert ("fb_1", "fotmob") in keys
    assert ("fb_1", "transfermarkt") in keys
    assert ("fm_2", "fotmob") in keys  # ambiguous xref is lossless, not fanout
    assert ("fm_3", "fotmob") in keys  # no xref
    assert ("tm_2", "transfermarkt") in keys
    assert len(keys) == len(facts)


def test_market_value_gold_uses_source_aware_natural_key():
    for sql_name in ("fct_player_market_value.sql", "fct_player_market_value_v2.sql"):
        sql = _text(GOLD / sql_name)
        partition = sql.rsplit("PARTITION BY", 1)[1].split("ORDER BY", 1)[0]
        assert "player_id" in partition
        assert "valuation_date" in partition
        assert "source" in partition
        assert "LEFT JOIN xref_fotmob" in sql
        assert "CONCAT('fm_'" in sql
        assert "CONCAT('tm_'" in sql


@pytest.mark.parametrize(
    "name",
    [
        "bronze_transfermarkt_competitions.yaml",
        "bronze_transfermarkt_competition_editions.yaml",
        "bronze_transfermarkt_player_contract_observations.yaml",
        "silver_transfermarkt_competitions_v2.yaml",
        "silver_transfermarkt_competition_editions_v2.yaml",
        "silver_transfermarkt_player_contract_observations_v2.yaml",
    ],
)
def test_openmetadata_contract_documents_operational_invariants(name):
    spec = yaml.safe_load(_text(OM / name))
    description = spec["table"]["description"].lower()
    assert "grain" in description
    assert "natural key" in description
    assert "lineage" in description
    assert "freshness" in description
    assert "completeness" in description
    assert spec["table"]["fullyQualifiedName"].startswith(
        "trino_iceberg.iceberg."
    )
