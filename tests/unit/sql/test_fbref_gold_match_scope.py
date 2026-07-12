"""All FBref match-grain Gold branches stop at configured dim_match scope."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "dags/sql/gold"

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "filename",
    [
        "fct_match_officials.sql",
        "fct_player_match.sql.j2",
        "fct_player_match_audit.sql",
        "fct_match_timeline.sql",
        "fct_lineup.sql",
        "fct_shot.sql",
        "fct_team_match_audit.sql",
        "fct_manager_stint.sql",
    ],
)
def test_fbref_match_fact_has_dim_match_scope(filename):
    sql = (GOLD / filename).read_text(encoding="utf-8")
    assert "JOIN iceberg.gold.dim_match" in sql, filename


def test_out_of_config_official_is_excluded_executably():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE silver_officials (
            match_id VARCHAR, role VARCHAR, official_name VARCHAR,
            league VARCHAR, season VARCHAR
        )"""
    )
    con.execute(
        """CREATE TABLE xref_referee (
            source VARCHAR, source_id VARCHAR, canonical_id VARCHAR,
            league VARCHAR, season VARCHAR
        )"""
    )
    con.execute("CREATE TABLE dim_match (match_id VARCHAR)")
    con.execute(
        """INSERT INTO silver_officials VALUES
            ('in-scope', 'referee', 'Ref One', 'League', '2526'),
            ('out-scope', 'referee', 'Ref Two', 'Other', '2526')"""
    )
    con.execute("INSERT INTO dim_match VALUES ('in-scope')")
    con.execute(
        """INSERT INTO xref_referee VALUES
            ('fbref', 'Ref One', 'ref_one', 'League', '2526'),
            ('fbref', 'Ref Two', 'ref_two', 'Other', '2526')"""
    )
    sql = (GOLD / "fct_match_officials.sql").read_text(encoding="utf-8")
    sql = sql.replace(
        "iceberg.silver.fbref_match_officials", "silver_officials"
    ).replace(
        "iceberg.silver.xref_referee", "xref_referee"
    ).replace(
        "iceberg.gold.dim_match", "dim_match"
    )

    rows = con.execute(sql).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "in-scope"
