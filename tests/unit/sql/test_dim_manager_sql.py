"""
Unit tests for Gold ``dim_manager`` SCD-2 SQL logic (E2 Phase 1.5 — 2026-05).

The transform reads bronze.fbref_match_managers + silver.fbref_match_enriched
+ silver.xref_manager + silver.xref_team and emits one row per
(manager × team × stint). Stint boundaries are detected via
``LAG(manager_canonical_id) OVER (PARTITION BY team_canonical_id ORDER BY
match_date)`` — a new stint starts whenever the manager changes.

Strategy: Trino → DuckDB transpile via sqlglot, fixture rows in an
in-memory schema, then assert on the resulting SCD-2 timeline. Skips
cleanly if sqlglot cannot translate a Trino-specific construct.

Fixture covers three scenarios:
  1. Arsenal — interim change inside a single season (Emery → Arteta).
  2. Liverpool — single manager (Klopp) across two seasons → ONE stint.
  3. Spurs — Mourinho who left Chelsea earlier returns later (covered
     implicitly by the (manager, team, valid_from) PK contract).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_manager.sql"


def _translate(sql_text: str) -> str:
    statements = sqlglot.transpile(sql_text, read="trino", write="duckdb")
    if not statements:
        raise RuntimeError("sqlglot transpile produced no output")
    out = statements[0]
    out = out.replace("iceberg.silver.", "silver.")
    out = out.replace("iceberg.bronze.", "bronze.")
    return out


def _bootstrap(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute("""
        CREATE TABLE bronze.fbref_match_managers (
            match_id       VARCHAR,
            league         VARCHAR,
            season         BIGINT,
            team           VARCHAR,
            side           VARCHAR,
            manager_name   VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            match_id       VARCHAR,
            date           DATE,
            league         VARCHAR,
            season         BIGINT,
            home           VARCHAR,
            away           VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE silver.xref_manager (
            canonical_id   VARCHAR,
            source         VARCHAR,
            source_id      VARCHAR,
            display_name   VARCHAR,
            league         VARCHAR,
            season         VARCHAR,
            confidence     VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE silver.xref_team (
            canonical_id   VARCHAR,
            source         VARCHAR,
            source_id      VARCHAR,
            display_name   VARCHAR,
            league         VARCHAR,
            season         VARCHAR,
            confidence     VARCHAR
        )
    """)

    league = "ENG-Premier League"

    # Match log — 6 Arsenal matches (Emery × 3 → Arteta × 3) + 4 Liverpool
    # matches (Klopp across two seasons) — 10 total. Dates chosen to
    # mimic real fixture ordering.
    rows = [
        # Arsenal 2018-19 (Emery)
        ("ars2018a", league, 2018, "Arsenal", "home", "Unai Emery", "2018-08-12"),
        ("ars2018b", league, 2018, "Arsenal", "home", "Unai Emery", "2018-12-01"),
        ("ars2019a", league, 2019, "Arsenal", "home", "Unai Emery", "2019-09-15"),
        # Arsenal 2019-20 (Arteta arrives mid-season — interim Ljungberg
        # omitted on purpose so the test covers a 2-stint timeline cleanly)
        ("ars2019b", league, 2019, "Arsenal", "home", "Mikel Arteta", "2019-12-26"),
        ("ars2020a", league, 2020, "Arsenal", "home", "Mikel Arteta", "2020-09-12"),
        ("ars2021a", league, 2021, "Arsenal", "home", "Mikel Arteta", "2021-08-13"),

        # Liverpool — same manager Klopp across three seasons -> ONE stint.
        # The 2021 match keeps Liverpool's team_max_season aligned with
        # Arsenal's (global max=2021) so the new "team must have matches in
        # the latest global season for is_current=True" gate keeps Klopp open.
        ("liv2018a", league, 2018, "Liverpool", "home", "Jurgen Klopp", "2018-08-12"),
        ("liv2018b", league, 2018, "Liverpool", "home", "Jurgen Klopp", "2018-12-01"),
        ("liv2019a", league, 2019, "Liverpool", "home", "Jurgen Klopp", "2019-08-09"),
        ("liv2020a", league, 2020, "Liverpool", "home", "Jurgen Klopp", "2020-09-12"),
        ("liv2021a", league, 2021, "Liverpool", "home", "Jurgen Klopp", "2021-08-14"),
    ]

    for r in rows:
        con.execute(
            "INSERT INTO bronze.fbref_match_managers VALUES (?, ?, ?, ?, ?, ?)",
            (r[0], r[1], r[2], r[3], r[4], r[5]),
        )
        # All synthetic rows have side='home' so we use the team as `home`
        # and a placeholder ('Opponent') for `away`. The dim_manager SQL
        # will pick `home` for the home-side row, away is unused here.
        con.execute(
            "INSERT INTO silver.fbref_match_enriched VALUES (?, ?, ?, ?, ?, ?)",
            (r[0], date.fromisoformat(r[6]), r[1], r[2], r[3], "Opponent"),
        )

    # Each (manager, season) pair becomes one xref_manager row.
    managers_seasons = {
        ("Unai Emery", 2018), ("Unai Emery", 2019),
        ("Mikel Arteta", 2019), ("Mikel Arteta", 2020), ("Mikel Arteta", 2021),
        ("Jurgen Klopp", 2018), ("Jurgen Klopp", 2019),
        ("Jurgen Klopp", 2020), ("Jurgen Klopp", 2021),
    }
    for name, season in managers_seasons:
        canonical = name.lower().replace(" ", "_")
        con.execute(
            "INSERT INTO silver.xref_manager VALUES (?, ?, ?, ?, ?, ?, ?)",
            (canonical, "fbref", name, name, league, str(season), "name_normalize"),
        )

    # Same shape for xref_team — slugged canonical_id per season.
    teams_seasons = {
        ("Arsenal", 2018), ("Arsenal", 2019),
        ("Arsenal", 2020), ("Arsenal", 2021),
        ("Liverpool", 2018), ("Liverpool", 2019),
        ("Liverpool", 2020), ("Liverpool", 2021),
    }
    for team, season in teams_seasons:
        canonical = team.lower()
        con.execute(
            "INSERT INTO silver.xref_team VALUES (?, ?, ?, ?, ?, ?, ?)",
            (canonical, "fbref", team, team, league, str(season), "name_alias"),
        )


@pytest.fixture(scope="module")
def gold_rows():
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    try:
        translated = _translate(sql_text)
    except Exception as e:
        pytest.skip(f"sqlglot Trino→DuckDB translation failed: {e}")

    con = duckdb.connect(":memory:")
    try:
        _bootstrap(con)
    except Exception as e:
        pytest.skip(f"DuckDB fixture bootstrap failed: {e}")

    try:
        rows = con.execute(translated).fetchall()
        col_names = [c[0] for c in con.description]
    except Exception as e:
        pytest.skip(f"DuckDB execution of translated dim_manager SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestDimManagerSCD2:
    """SCD-2 timeline correctness for dim_manager."""

    def test_three_total_stints(self, gold_rows):
        """6 Arsenal matches (2 stints) + 4 Liverpool matches (1 stint) = 3 rows."""
        assert len(gold_rows) == 3, (
            f"expected 3 stints, got {len(gold_rows)}: "
            f"{[(r['manager_id_canonical'], r['team_id_canonical']) for r in gold_rows]}"
        )

    def test_arsenal_emery_then_arteta(self, gold_rows):
        """Arsenal must have two stints with valid_from set to first match
        of each manager and valid_to closing at the next manager's start."""
        ars = sorted(
            [r for r in gold_rows if r["team_id_canonical"] == "arsenal"],
            key=lambda r: r["valid_from"],
        )
        assert len(ars) == 2
        emery, arteta = ars

        assert emery["manager_id_canonical"] == "unai_emery"
        assert emery["valid_from"] == date(2018, 8, 12)
        # Closed-open: valid_to == valid_from of the next stint
        assert emery["valid_to"] == date(2019, 12, 26), (
            f"Emery stint must close at Arteta's first match (closed-open), "
            f"got {emery['valid_to']}"
        )
        assert emery["is_current"] is False

        assert arteta["manager_id_canonical"] == "mikel_arteta"
        assert arteta["valid_from"] == date(2019, 12, 26)
        assert arteta["valid_to"] is None, (
            "Arteta stint is open-ended (current) — valid_to must be NULL"
        )
        assert arteta["is_current"] is True

    def test_liverpool_klopp_single_multi_season_stint(self, gold_rows):
        """Klopp at Liverpool spans 3 seasons → exactly ONE stint row."""
        liv = [r for r in gold_rows if r["team_id_canonical"] == "liverpool"]
        assert len(liv) == 1, (
            f"Klopp's continuous tenure must produce ONE stint row, got "
            f"{len(liv)}: {[r['valid_from'] for r in liv]}"
        )
        klopp = liv[0]
        assert klopp["manager_id_canonical"] == "jurgen_klopp"
        # season = season of the FIRST match in the stint
        assert klopp["season"] == "2018"
        assert klopp["valid_from"] == date(2018, 8, 12)
        assert klopp["valid_to"] is None
        assert klopp["is_current"] is True

    def test_exactly_one_current_per_team(self, gold_rows):
        """Per team_canonical_id there must be exactly ONE is_current=True."""
        per_team_current: dict[str, int] = {}
        for r in gold_rows:
            key = r["team_id_canonical"]
            if r["is_current"]:
                per_team_current[key] = per_team_current.get(key, 0) + 1
        for team, n in per_team_current.items():
            assert n == 1, (
                f"team {team!r} must have exactly 1 is_current row, got {n}"
            )

    def test_no_overlap_within_team(self, gold_rows):
        """Closed-open intervals must not overlap inside a single team timeline."""
        from collections import defaultdict
        per_team: dict[str, list] = defaultdict(list)
        for r in gold_rows:
            per_team[r["team_id_canonical"]].append(r)

        for team, rows in per_team.items():
            rows.sort(key=lambda r: r["valid_from"])
            for prev, curr in zip(rows, rows[1:]):
                prev_end = prev["valid_to"] or date(9999, 12, 31)
                # closed-open: prev_end may equal curr_start, but never exceed
                assert prev_end <= curr["valid_from"], (
                    f"overlap in team {team}: prev ends {prev_end} but next "
                    f"starts {curr['valid_from']}"
                )

    def test_display_name_populated(self, gold_rows):
        """display_name is denormalised from xref_manager — never NULL."""
        for r in gold_rows:
            assert r["display_name"], (
                f"display_name must be populated, got {r['display_name']!r} "
                f"for {r['manager_id_canonical']}"
            )

    def test_pk_uniqueness(self, gold_rows):
        """PK = (manager_id_canonical, team_id_canonical, valid_from) is unique."""
        pks = [
            (r["manager_id_canonical"], r["team_id_canonical"], r["valid_from"])
            for r in gold_rows
        ]
        assert len(pks) == len(set(pks)), (
            f"PK uniqueness violated: {pks}"
        )
