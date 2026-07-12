"""
Unit tests for Gold ``fct_manager_stint`` SCD-2 SQL logic (issue #429).

The stint logic is resurrected from the pre-#433 ``dim_manager.sql``
(`git show 735e727:dags/sql/gold/dim_manager.sql`) — these tests are the
matching resurrection of the pre-#433 ``test_dim_manager_sql.py``, adapted
to the star-schema column names (``manager_id`` / ``team_id`` instead of
``*_id_canonical``, no ``display_name``) plus the new ``matches_in_charge``
column.

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
  2. Liverpool — single manager (Klopp) across multiple seasons → ONE stint.
  3. Wolves (dirty fixture) — issue #200 scorebox dupes + same-date tie.

NOTE on the dirty fixture: the pre-#433 version inserted xref season
``'2022'`` while the SQL joins on the slug ``'2223'`` — the INNER JOIN
matched nothing and the #200 regression tests passed on an EMPTY result
(tautology, see memory feedback_fixture_must_mirror_live_enums). Fixed
here: slug season + an explicit non-empty assertion.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_manager_stint.sql"


def _translate(sql_text: str) -> str:
    sql_text = sql_text.replace(
        "iceberg.gold.dim_match",
        "(SELECT DISTINCT match_id FROM silver.fbref_match_enriched)",
    )
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
            manager_name   VARCHAR,
            source_season_id VARCHAR
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

    # Match log — 6 Arsenal matches (Emery × 3 → Arteta × 3) + 5 Liverpool
    # matches (Klopp across four seasons) — 11 total. Dates chosen to
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

        # Liverpool — same manager Klopp across four seasons -> ONE stint.
        # The 2021 match keeps Liverpool's team_max_season aligned with
        # Arsenal's (global max=2021) so the "team must have matches in
        # the latest global season for is_current=True" gate keeps Klopp open.
        ("liv2018a", league, 2018, "Liverpool", "home", "Jurgen Klopp", "2018-08-12"),
        ("liv2018b", league, 2018, "Liverpool", "home", "Jurgen Klopp", "2018-12-01"),
        ("liv2019a", league, 2019, "Liverpool", "home", "Jurgen Klopp", "2019-08-09"),
        ("liv2020a", league, 2020, "Liverpool", "home", "Jurgen Klopp", "2020-09-12"),
        ("liv2021a", league, 2021, "Liverpool", "home", "Jurgen Klopp", "2021-08-14"),
    ]

    for r in rows:
        con.execute(
            "INSERT INTO bronze.fbref_match_managers VALUES (?, ?, ?, ?, ?, ?, ?)",
            (r[0], r[1], r[2], r[3], r[4], r[5], None),
        )
        # All synthetic rows have side='home' so we use the team as `home`
        # and a placeholder ('Opponent') for `away`. The stint SQL will pick
        # `home` for the home-side row, away is unused here.
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
            (canonical, "fbref", name, name, league, f"{season % 100:02d}{(season + 1) % 100:02d}", "name_normalize"),
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
            (canonical, "fbref", team, team, league, f"{season % 100:02d}{(season + 1) % 100:02d}", "name_alias"),
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
        pytest.skip(f"DuckDB execution of translated fct_manager_stint SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


pytestmark = pytest.mark.unit


class TestFctManagerStintSCD2:
    """SCD-2 timeline correctness for fct_manager_stint."""

    def test_three_total_stints(self, gold_rows):
        """6 Arsenal matches (2 stints) + 5 Liverpool matches (1 stint) = 3 rows."""
        assert len(gold_rows) == 3, (
            f"expected 3 stints, got {len(gold_rows)}: "
            f"{[(r['manager_id'], r['team_id']) for r in gold_rows]}"
        )

    def test_arsenal_emery_then_arteta(self, gold_rows):
        """Arsenal must have two stints with valid_from set to first match
        of each manager and valid_to closing at the next manager's start."""
        ars = sorted(
            [r for r in gold_rows if r["team_id"] == "arsenal"],
            key=lambda r: r["valid_from"],
        )
        assert len(ars) == 2
        emery, arteta = ars

        assert emery["manager_id"] == "unai_emery"
        assert emery["valid_from"] == date(2018, 8, 12)
        # Closed-open: valid_to == valid_from of the next stint
        assert emery["valid_to"] == date(2019, 12, 26), (
            f"Emery stint must close at Arteta's first match (closed-open), "
            f"got {emery['valid_to']}"
        )
        assert emery["is_current"] is False

        assert arteta["manager_id"] == "mikel_arteta"
        assert arteta["valid_from"] == date(2019, 12, 26)
        assert arteta["valid_to"] is None, (
            "Arteta stint is open-ended (current) — valid_to must be NULL"
        )
        assert arteta["is_current"] is True

    def test_liverpool_klopp_single_multi_season_stint(self, gold_rows):
        """Klopp at Liverpool spans 4 seasons → exactly ONE stint row."""
        liv = [r for r in gold_rows if r["team_id"] == "liverpool"]
        assert len(liv) == 1, (
            f"Klopp's continuous tenure must produce ONE stint row, got "
            f"{len(liv)}: {[r['valid_from'] for r in liv]}"
        )
        klopp = liv[0]
        assert klopp["manager_id"] == "jurgen_klopp"
        # season = season of the FIRST match in the stint
        assert klopp["season"] == "1819"  # #404: slug of first stint season (2018)
        assert klopp["valid_from"] == date(2018, 8, 12)
        assert klopp["valid_to"] is None
        assert klopp["is_current"] is True

    def test_matches_in_charge(self, gold_rows):
        """matches_in_charge counts deduped matches inside each stint."""
        per_stint = {
            (r["manager_id"], r["team_id"]): r["matches_in_charge"]
            for r in gold_rows
        }
        assert per_stint[("unai_emery", "arsenal")] == 3
        assert per_stint[("mikel_arteta", "arsenal")] == 3
        assert per_stint[("jurgen_klopp", "liverpool")] == 5

    def test_exactly_one_current_per_team(self, gold_rows):
        """Per team_id there must be exactly ONE is_current=True."""
        per_team_current: dict[str, int] = {}
        for r in gold_rows:
            key = r["team_id"]
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
            per_team[r["team_id"]].append(r)

        for team, rows in per_team.items():
            rows.sort(key=lambda r: r["valid_from"])
            for prev, curr in zip(rows, rows[1:]):
                prev_end = prev["valid_to"] or date(9999, 12, 31)
                # closed-open: prev_end may equal curr_start, but never exceed
                assert prev_end <= curr["valid_from"], (
                    f"overlap in team {team}: prev ends {prev_end} but next "
                    f"starts {curr['valid_from']}"
                )

    def test_pk_uniqueness(self, gold_rows):
        """PK = (manager_id, team_id, valid_from) is unique."""
        pks = [
            (r["manager_id"], r["team_id"], r["valid_from"])
            for r in gold_rows
        ]
        assert len(pks) == len(set(pks)), (
            f"PK uniqueness violated: {pks}"
        )


def _bootstrap_dirty(con) -> None:
    """Bootstrap a fixture that reproduces issue #200: duplicate scorebox rows
    plus a same-date manager tie that would split a stint into two segments
    sharing the same valid_from (= MIN match_date) → PK collision.

    Wolves timeline (one team, isolated):
      * 2022-08-06  O'Neil ×3 identical scorebox dupes  (start of stint 1)
      * 2022-09-01  O'Neil                              (stint 1 continues)
      * 2022-11-01  Lopetegui ×2 identical dupes        (new manager …)
      * 2022-11-01  O'Neil                              ← pathological tie:
                    a SECOND distinct manager credited on the SAME date.

    On the un-deduped SQL the tied 2022-11-01 rows (two distinct managers)
    make the islands-and-gaps SUM produce two stints whose MIN(match_date)
    both equal 2022-11-01 → duplicate PK. Collapsing to one row per
    (team, match_date) removes the tie (ORDER BY manager_canonical_id keeps
    'gary_oneil') and the whole timeline becomes ONE O'Neil stint.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute("""
        CREATE TABLE bronze.fbref_match_managers (
            match_id VARCHAR, league VARCHAR, season BIGINT,
            team VARCHAR, side VARCHAR, manager_name VARCHAR,
            source_season_id VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            match_id VARCHAR, date DATE, league VARCHAR,
            season BIGINT, home VARCHAR, away VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_manager (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR, confidence VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_team (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR, confidence VARCHAR
        )
    """)

    league = "ENG-Premier League"
    # (match_id, season, manager_name, date) — side is always 'home'.
    rows = [
        # stint 1: O'Neil — first match has 3 identical scorebox dupes
        ("wol1", 2022, "Gary O'Neil", "2022-08-06"),
        ("wol1", 2022, "Gary O'Neil", "2022-08-06"),
        ("wol1", 2022, "Gary O'Neil", "2022-08-06"),
        ("wol2", 2022, "Gary O'Neil", "2022-09-01"),
        # stint 2: Lopetegui — first match has 2 identical dupes
        ("wol3", 2022, "Julen Lopetegui", "2022-11-01"),
        ("wol3", 2022, "Julen Lopetegui", "2022-11-01"),
        # pathological tie: a second distinct manager on the SAME date 2022-11-01
        ("wol4", 2022, "Gary O'Neil", "2022-11-01"),
    ]
    for mid, season, mgr, d in rows:
        con.execute(
            "INSERT INTO bronze.fbref_match_managers VALUES (?, ?, ?, ?, ?, ?, ?)",
            (mid, league, season, "Wolverhampton Wanderers", "home", mgr, None),
        )
    # fbref_match_enriched: one row per distinct match_id.
    for mid, d in {("wol1", "2022-08-06"), ("wol2", "2022-09-01"),
                   ("wol3", "2022-11-01"), ("wol4", "2022-11-01")}:
        con.execute(
            "INSERT INTO silver.fbref_match_enriched VALUES (?, ?, ?, ?, ?, ?)",
            (mid, date.fromisoformat(d), league, 2022,
             "Wolverhampton Wanderers", "Opponent"),
        )
    # Season slug '2223' (bronze bigint 2022 → '2223' via the LPAD/MOD slug
    # expression in the SQL). The pre-#433 test inserted '2022' here, which
    # never matched the JOIN — empty result, tautological test.
    for name in {"Gary O'Neil", "Julen Lopetegui"}:
        canonical = name.lower().replace(" ", "_").replace("'", "")
        con.execute(
            "INSERT INTO silver.xref_manager VALUES (?, ?, ?, ?, ?, ?, ?)",
            (canonical, "fbref", name, name, league, "2223", "name_normalize"),
        )
    con.execute(
        "INSERT INTO silver.xref_team VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("wolverhampton_wanderers", "fbref", "Wolverhampton Wanderers",
         "Wolverhampton Wanderers", league, "2223", "name_alias"),
    )


@pytest.fixture(scope="module")
def dirty_gold_rows():
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    try:
        translated = _translate(sql_text)
    except Exception as e:
        pytest.skip(f"sqlglot Trino→DuckDB translation failed: {e}")

    con = duckdb.connect(":memory:")
    try:
        _bootstrap_dirty(con)
    except Exception as e:
        pytest.skip(f"DuckDB dirty fixture bootstrap failed: {e}")

    try:
        rows = con.execute(translated).fetchall()
        col_names = [c[0] for c in con.description]
    except Exception as e:
        pytest.skip(f"DuckDB execution of translated fct_manager_stint SQL failed: {e}")

    return [dict(zip(col_names, r)) for r in rows]


class TestFctManagerStintDedup:
    """Issue #200: duplicate scorebox rows + same-date manager tie must not
    break the SCD-2 PK. Passes only after the (team, match_date) collapse
    is in place."""

    def test_dirty_fixture_not_empty(self, dirty_gold_rows):
        """Guard against the pre-#433 tautology: the dirty fixture MUST
        produce rows — an empty result means the xref JOIN is broken."""
        assert dirty_gold_rows, "dirty fixture produced 0 rows — xref JOIN broken"

    def test_single_oneil_stint_after_dedup(self, dirty_gold_rows):
        """The (team, match_date) collapse keeps 'gary_oneil' on the tied
        2022-11-01 date → ONE continuous O'Neil stint of 3 deduped matches."""
        assert len(dirty_gold_rows) == 1, (
            f"expected 1 stint after dedup, got {len(dirty_gold_rows)}: "
            f"{[(r['manager_id'], r['valid_from']) for r in dirty_gold_rows]}"
        )
        stint = dirty_gold_rows[0]
        assert stint["manager_id"] == "gary_oneil"
        assert stint["valid_from"] == date(2022, 8, 6)
        assert stint["matches_in_charge"] == 3

    def test_pk_unique_with_duplicate_and_tied_rows(self, dirty_gold_rows):
        pks = [
            (r["manager_id"], r["team_id"], r["valid_from"])
            for r in dirty_gold_rows
        ]
        assert len(pks) == len(set(pks)), (
            f"PK collision on dup/tied bronze rows (issue #200): {pks}"
        )

    def test_no_valid_from_collision_per_team(self, dirty_gold_rows):
        """No two stints of the same team may share a valid_from."""
        from collections import Counter
        per_team = Counter(
            (r["team_id"], r["valid_from"]) for r in dirty_gold_rows
        )
        dups = {k: n for k, n in per_team.items() if n > 1}
        assert not dups, f"two stints share a valid_from (issue #200): {dups}"


def test_non_world_cup_single_year_keeps_source_season():
    """EURO 2024 is season ``2024``; it must never become ``2425``."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA silver")
    con.execute("""
        CREATE TABLE bronze.fbref_match_managers (
            match_id VARCHAR, league VARCHAR, season BIGINT,
            team VARCHAR, side VARCHAR, manager_name VARCHAR,
            source_season_id VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.fbref_match_enriched (
            match_id VARCHAR, date DATE, league VARCHAR,
            season BIGINT, home VARCHAR, away VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_manager (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR,
            confidence VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_team (
            canonical_id VARCHAR, source VARCHAR, source_id VARCHAR,
            display_name VARCHAR, league VARCHAR, season VARCHAR,
            confidence VARCHAR
        )
    """)
    league = "INT-European Championship"
    con.execute(
        "INSERT INTO bronze.fbref_match_managers VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("euro-final", league, 2024, "Spain", "home", "Luis de la Fuente", "2024"),
    )
    con.execute(
        "INSERT INTO silver.fbref_match_enriched VALUES (?, ?, ?, ?, ?, ?)",
        ("euro-final", date(2024, 7, 14), league, 2024, "Spain", "England"),
    )
    con.execute(
        "INSERT INTO silver.xref_manager VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("luis_de_la_fuente", "fbref", "Luis de la Fuente", "Luis de la Fuente", league, "2024", "name_normalize"),
    )
    con.execute(
        "INSERT INTO silver.xref_team VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("spain", "fbref", "Spain", "Spain", league, "2024", "name_alias"),
    )

    translated = _translate(SQL_PATH.read_text(encoding="utf-8"))
    row = con.execute(translated).fetchone()
    columns = [column[0] for column in con.description]

    assert row is not None
    assert dict(zip(columns, row))["season"] == "2024"
