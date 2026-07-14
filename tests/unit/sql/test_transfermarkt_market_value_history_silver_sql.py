"""
Unit tests for Silver ``transfermarkt_market_value_history`` SQL — the #835
historization.

Bronze writes a player's FULL career MV graph into EVERY season snapshot, so one
(player_id, mv_date) repeats across all season partitions. The Silver SQL now:

  1. collapses cross-season duplicates to ONE row per (player_id, mv_date)
     (freshest ``_ingested_at`` wins);
  2. DERIVES the football season from ``mv_date`` (Aug–Jun, short-form '2122'),
     NOT the bronze snapshot season;
  3. joins canonical per derived season → a player who LEFT the APL (absent from
     the current snapshot) still gets canonical for the seasons they played in.

Points in a season with no non-orphan xref row (before a player joined / after
they left) keep ``canonical_id = NULL``. The per-season two-pass dedup (#788)
keeps (canonical_id, mv_date) unique by construction.

Strategy: Trino → DuckDB transpile via sqlglot, bootstrap bronze + xref
fixtures, execute, assert (same shape as test_transfermarkt_transfers_silver_sql).
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest


sqlglot = pytest.importorskip("sqlglot")
duckdb = pytest.importorskip("duckdb")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver"
    / "transfermarkt_market_value_history.sql"
)

LEAGUE = "ENG-Premier League"

# Two ingest timestamps — the later one must win the cross-season collapse.
T0 = datetime(2026, 6, 1, 3, 0, 0)
T1 = datetime(2026, 6, 2, 3, 0, 0)


def _translate() -> str:
    sql_text = SQL_PATH.read_text(encoding="utf-8")
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
        CREATE TABLE bronze.transfermarkt_market_value_history (
            player_id     VARCHAR,
            mv_date       DATE,
            value_eur     BIGINT,
            club_name     VARCHAR,
            age           INTEGER,
            league        VARCHAR,
            season        VARCHAR,
            _ingested_at  TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE silver.xref_player (
            canonical_id  VARCHAR,
            source        VARCHAR,
            source_id     VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            confidence    VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE silver.transfermarkt_competitions_v2 (
            competition_id            VARCHAR,
            canonical_competition_id  VARCHAR,
            season_format             VARCHAR,
            registry_snapshot_id      VARCHAR
        )
    """)

    # #948: non-slotted publish registry holds MULTIPLE snapshots per
    # competition → the SQL must GROUP BY + only trust 'single_year' when ALL
    # snapshots agree.
    con.executemany(
        "INSERT INTO silver.transfermarkt_competitions_v2 VALUES (?, ?, ?, ?)",
        [
            # Calendar-year league (no canonical mapping → league = 'TM-2DVB'),
            # consistent 'single_year' across both snapshots.
            ("2DVB", None, "single_year", "snap-1"),
            ("2DVB", None, "single_year", "snap-2"),
            # Registry-mapped split-year league (like live GB1 → APL).
            ("GB1", LEAGUE, "split_year", "snap-1"),
            ("GB1", LEAGUE, "split_year", "snap-2"),
            # Conflicting formats between snapshots → guard → split_year.
            ("CONF", None, "single_year", "snap-1"),
            ("CONF", None, "split_year", "snap-2"),
            # NULL format in one snapshot must BREAK the single_year consensus
            # (strict COUNT(*) guard; MIN/MAX would have ignored the NULL).
            ("NULLF", None, None, "snap-1"),
            ("NULLF", None, "single_year", "snap-2"),
            # TWO different competition_ids mapped to ONE canonical key with
            # conflicting formats → collapsed per canonical key, guard →
            # split_year on the MATCHED join path.
            ("CUP1", "XX-Some Cup", "single_year", "snap-1"),
            ("CUP2", "XX-Some Cup", "split_year", "snap-1"),
        ],
    )

    # (player_id, mv_date, value_eur, club_name, age, bronze_season, ingested_at)
    bronze_rows = [
        # P_CURRENT — current-squad player. The 2024-09 point is re-emitted in
        # BOTH the '2425' and '2526' snapshots → must collapse to one, newest
        # (T1, 50M) wins over the older copy (T0, 49M).
        ("P_CURRENT", date(2024, 9, 1), 49_000_000, "Club A", 23, "2425", T0),
        ("P_CURRENT", date(2024, 9, 1), 50_000_000, "Club A", 23, "2526", T1),
        ("P_CURRENT", date(2025, 9, 1), 80_000_000, "Club A", 24, "2526", T1),
        # P_LEFT — player who LEFT the APL (NOT in the current '2526' snapshot;
        # only the backfilled '2223' snapshot has them). Their 2022-09 point is
        # the #835 win: derived season '2223' matches the historized xref.
        ("P_LEFT", date(2021, 9, 1), 20_000_000, "Club B", 25, "2223", T0),
        ("P_LEFT", date(2022, 9, 1), 30_000_000, "Club B", 26, "2223", T0),
        # P_ORPH — only an orphan xref row exists → canonical stays NULL.
        ("P_ORPH", date(2025, 9, 1), 5_000_000, "Youth FC", 19, "2526", T0),
    ]
    for pid, mv_date, value_eur, club, age, bronze_season, ts in bronze_rows:
        con.execute(
            "INSERT INTO bronze.transfermarkt_market_value_history "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (pid, mv_date, value_eur, club, age, LEAGUE, bronze_season, ts),
        )

    # #948: calendar-league / registry-format bronze rows. Legacy dual-write
    # labels these seasons '2024'/'2025' already; the mv_date-derived season
    # must follow the league's season_format, not the split-year formula.
    extra_bronze_rows = [
        # TM-2DVB — single_year in the registry → season = calendar year.
        # Sep 2024 (split formula would say '2425') and Mar 2025 ('2425' too).
        ("P_CAL", date(2024, 9, 1), 1_000_000, "Club C", 21, "TM-2DVB", "2024", T0),
        ("P_CAL", date(2025, 3, 1), 2_000_000, "Club C", 21, "TM-2DVB", "2025", T0),
        # TM-CONF — snapshots disagree on the format → fallback split_year.
        ("P_CONF", date(2024, 9, 1), 3_000_000, "Club D", 22, "TM-CONF", "2024", T0),
        # TM-UNKN — not in the registry at all → fallback split_year.
        ("P_UNKN", date(2024, 9, 1), 4_000_000, "Club E", 23, "TM-UNKN", "2024", T0),
        # TM-NULLF — {NULL, 'single_year'} snapshots → strict guard → split_year.
        ("P_NULLF", date(2024, 9, 1), 5_000_000, "Club F", 24, "TM-NULLF", "2024", T0),
        # XX-Some Cup — canonical-mapped league (join MATCHES) whose two
        # competition_ids disagree on the format → guard → split_year.
        ("P_CUP", date(2024, 9, 1), 6_000_000, "Club G", 25, "XX-Some Cup", "2024", T0),
    ]
    for pid, mv_date, value_eur, club, age, league, bronze_season, ts in extra_bronze_rows:
        con.execute(
            "INSERT INTO bronze.transfermarkt_market_value_history "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (pid, mv_date, value_eur, club, age, league, bronze_season, ts),
        )

    # xref_player — historized per season. max(season) is '2526' (so the OLD
    # scoped logic would have given P_LEFT canonical=NULL everywhere → dropped).
    con.executemany(
        "INSERT INTO silver.xref_player VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("fb_curr", "transfermarkt", "P_CURRENT", LEAGUE, "2425", "exact"),
            ("fb_curr", "transfermarkt", "P_CURRENT", LEAGUE, "2526", "exact"),
            # P_LEFT resolved ONLY in 2223 — not the current season.
            ("fb_left", "transfermarkt", "P_LEFT", LEAGUE, "2223", "exact"),
            # Orphan bridge → excluded by confidence <> 'orphan'.
            ("tm_P_ORPH", "transfermarkt", "P_ORPH", LEAGUE, "2526", "orphan"),
        ],
    )


@pytest.fixture(scope="module")
def silver_rows():
    try:
        translated = _translate()
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
        pytest.skip(
            f"DuckDB execution of translated market_value_history SQL failed: {e}"
        )

    return [dict(zip(col_names, r)) for r in rows]


def _by_point(rows, player_id, mv_date):
    matches = [
        r for r in rows
        if r["player_id"] == player_id and r["mv_date"] == mv_date
    ]
    assert len(matches) == 1, (
        f"expected exactly one row for ({player_id}, {mv_date}), got {len(matches)}"
    )
    return matches[0]


pytestmark = pytest.mark.unit


class TestMvHistoryHistorization:

    def test_cross_season_collapse_freshest_wins(self, silver_rows):
        """The 2024-09 point exists in two bronze snapshots → one Silver row,
        newest ingest wins."""
        row = _by_point(silver_rows, "P_CURRENT", date(2024, 9, 1))
        assert row["value_eur"] == 50_000_000

    def test_season_derived_from_mv_date(self, silver_rows):
        """season comes from mv_date (football Aug–Jun), not bronze.season."""
        assert _by_point(silver_rows, "P_CURRENT", date(2024, 9, 1))["season"] == "2425"
        assert _by_point(silver_rows, "P_CURRENT", date(2025, 9, 1))["season"] == "2526"
        assert _by_point(silver_rows, "P_LEFT", date(2021, 9, 1))["season"] == "2122"
        assert _by_point(silver_rows, "P_LEFT", date(2022, 9, 1))["season"] == "2223"

    def test_left_apl_player_covered(self, silver_rows):
        """#835 core goal: a player absent from the current snapshot still gets
        canonical for the season they played in (here 2022-09 → '2223')."""
        row = _by_point(silver_rows, "P_LEFT", date(2022, 9, 1))
        assert row["canonical_id"] == "fb_left"

    def test_canonical_null_outside_xref_season(self, silver_rows):
        """A point in a season with no non-orphan xref row stays NULL (the row
        is still kept — LEFT JOIN)."""
        row = _by_point(silver_rows, "P_LEFT", date(2021, 9, 1))
        assert row["canonical_id"] is None

    def test_current_player_canonical_per_season(self, silver_rows):
        assert _by_point(silver_rows, "P_CURRENT", date(2024, 9, 1))["canonical_id"] == "fb_curr"
        assert _by_point(silver_rows, "P_CURRENT", date(2025, 9, 1))["canonical_id"] == "fb_curr"

    def test_orphan_canonical_null(self, silver_rows):
        row = _by_point(silver_rows, "P_ORPH", date(2025, 9, 1))
        assert row["canonical_id"] is None


class TestMvHistorySeasonFormat:
    """#948: season derivation must respect the league's season_format from the
    publish registry (silver.transfermarkt_competitions_v2)."""

    def test_single_year_league_season_is_calendar_year(self, silver_rows):
        """A single_year league gets the 4-digit calendar year, not the
        split-year slug ('2425')."""
        assert _by_point(silver_rows, "P_CAL", date(2024, 9, 1))["season"] == "2024"
        assert _by_point(silver_rows, "P_CAL", date(2025, 3, 1))["season"] == "2025"

    def test_single_year_league_has_no_split_slug_seasons(self, silver_rows):
        """No phantom split slugs for the calendar league: every TM-2DVB season
        equals str(mv_date.year)."""
        cal_rows = [r for r in silver_rows if r["league"] == "TM-2DVB"]
        assert cal_rows, "expected TM-2DVB rows in the output"
        for r in cal_rows:
            assert r["season"] == str(r["mv_date"].year), r

    def test_unknown_league_falls_back_to_split_year(self, silver_rows):
        """A league absent from the registry mapping keeps the historical
        split-year behaviour bit-for-bit."""
        row = _by_point(silver_rows, "P_UNKN", date(2024, 9, 1))
        assert row["season"] == "2425"

    def test_conflicting_snapshot_formats_fall_back_to_split_year(self, silver_rows):
        """Registry snapshots disagreeing on season_format → the agreement
        guard falls back to split_year."""
        row = _by_point(silver_rows, "P_CONF", date(2024, 9, 1))
        assert row["season"] == "2425"

    def test_null_snapshot_format_falls_back_to_split_year(self, silver_rows):
        """{NULL, 'single_year'} in the registry must BREAK the consensus:
        the strict COUNT(*) guard treats NULL as disagreement (MIN/MAX would
        have silently ignored it)."""
        row = _by_point(silver_rows, "P_NULLF", date(2024, 9, 1))
        assert row["season"] == "2425"

    def test_conflicting_canonical_mapped_formats_fall_back_to_split_year(self, silver_rows):
        """Two competition_ids mapped to ONE canonical key with conflicting
        formats → collapsed per canonical key, guard → split_year. Unlike the
        TM-CONF case this league name ('XX-Some Cup') only exists via the
        canonical mapping, so the join MATCHED and the guard itself (not a
        join miss) produced the fallback; one output row proves the GROUP BY
        collapsed both registry rows (no join fan-out)."""
        row = _by_point(silver_rows, "P_CUP", date(2024, 9, 1))
        assert row["season"] == "2425"

    def test_single_year_league_canonical_stays_null(self, silver_rows):
        """Calendar leagues have no xref rows → canonical_id stays NULL (the
        rows are still kept — LEFT JOIN)."""
        for r in silver_rows:
            if r["league"] == "TM-2DVB":
                assert r["canonical_id"] is None, r


class TestMvHistoryGrain:

    def test_one_row_per_player_mvdate(self, silver_rows):
        grain = [(r["player_id"], r["mv_date"]) for r in silver_rows]
        assert len(grain) == len(set(grain)), f"duplicate (player_id, mv_date): {grain}"

    def test_no_canonical_mvdate_duplicates(self, silver_rows):
        """The DoD ERROR gate: (canonical_id, mv_date) unique among non-NULL."""
        keyed = [
            (r["canonical_id"], r["mv_date"])
            for r in silver_rows if r["canonical_id"] is not None
        ]
        assert len(keyed) == len(set(keyed)), f"dup (canonical_id, mv_date): {keyed}"

    def test_columns_contract(self, silver_rows):
        expected = {
            "player_id", "canonical_id", "mv_date", "value_eur",
            "club_name", "age", "_bronze_ingested_at", "league", "season",
        }
        assert set(silver_rows[0].keys()) == expected
