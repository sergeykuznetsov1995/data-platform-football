"""
Unit tests for FBref silver dedup keys (issue #463).

Executes ``dags/sql/silver/fbref_player_season_profile.sql`` and
``fbref_keeper_profile.sql`` against an in-memory DuckDB (text-substitution
pass for table names — LPAD/MOD/TRY_CAST are DuckDB-compatible) and
regex-checks the dedup keys of all five FBref silver files.

Bug under test (issue #463, verified live 2026-06-12):
  * Season profiles deduped by (player_id, league, season) WITHOUT squad —
    FBref emits one row per (player, squad), so a winter transfer inside the
    league (111 live player-seasons, e.g. Danny Ings 2022/23 Aston Villa →
    West Ham) silently dropped one club's stats, nondeterministically.
  * lineups/events PARTITION BY a nullable player_id — NULL rows would
    collapse to one survivor per match (defensive fix, 0 NULL rows live).
  * No deterministic tiebreaker — ties inside one batch picked an arbitrary
    row between rebuilds.

Fixtures mirror the live canary (memory: feedback_fixture_must_mirror_live_enums):
Danny Ings, player_id 07802f7f, ENG-Premier League, bronze season 2022 →
Aston Villa 824 min / 6 goals, West Ham United 775 min / 2 goals.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_DIR = PROJECT_ROOT / "dags" / "sql" / "silver"

PLAYER_PROFILE_SQL = SILVER_DIR / "fbref_player_season_profile.sql"
KEEPER_PROFILE_SQL = SILVER_DIR / "fbref_keeper_profile.sql"
MATCH_LINEUPS_SQL = SILVER_DIR / "fbref_match_lineups.sql"
MATCH_EVENTS_SQL = SILVER_DIR / "fbref_match_events.sql"
MATCH_ENRICHED_SQL = SILVER_DIR / "fbref_match_enriched.sql"

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation
# ---------------------------------------------------------------------------

_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.fbref_player_stats":       "bronze_fbref_player_stats",
    "iceberg.bronze.fbref_player_shooting":    "bronze_fbref_player_shooting",
    "iceberg.bronze.fbref_player_playingtime": "bronze_fbref_player_playingtime",
    "iceberg.bronze.fbref_player_misc":        "bronze_fbref_player_misc",
    "iceberg.bronze.fbref_keeper_keeper":      "bronze_fbref_keeper_keeper",
    "iceberg.silver.fbref_player_identity":    "silver_fbref_player_identity",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    # Trino normalize(..., NFD) -> DuckDB strip_accents(...) for ASCII/diacritic
    # identity predicates exercised here.
    sql = sql.replace(", NFD)", ")").replace("normalize(", "strip_accents(")
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")
    return sql


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


# ---------------------------------------------------------------------------
# Fixture schemas — column sets mirror tests/fixtures/bronze_schemas.json
# (all stat columns varchar in Bronze; season is year-start BIGINT)
# ---------------------------------------------------------------------------

_COMMON = ["player", "player_id", "nation", "pos", "squad", "age", "born",
           "league", "season", "source_season_id", "_ingested_at", "_batch_id"]

_STATS_COLS = _COMMON + [
    "mp", "starts", "min", "90s", "gls", "ast", "g+a", "g-pk", "pk", "pkatt",
    "crdy", "crdr", "gls_1", "ast_1", "g+a_1", "g-pk_1", "g+a-pk",
]
_SHOOTING_COLS = _COMMON + ["sh", "sot", "sot%", "sh/90", "sot/90", "g/sh", "g/sot"]
_PLAYINGTIME_COLS = _COMMON + [
    "mn/mp", "min%", "mn/start", "compl", "subs", "mn/sub", "unsub", "ppm",
    "ong", "onga", "+/-", "+/-90", "on-off",
]
_MISC_COLS = _COMMON + [
    "2crdy", "fls", "fld", "off", "crs", "int", "tklw", "pkwon", "pkcon", "og",
]
_KEEPER_COLS = _COMMON + [
    "ga", "ga90", "sota", "saves", "save%", "w", "d", "l", "cs", "cs%",
    "pkatt", "pka", "pksv", "pkm", "save%_1",
]

_TABLES = {
    "bronze_fbref_player_stats":       _STATS_COLS,
    "bronze_fbref_player_shooting":    _SHOOTING_COLS,
    "bronze_fbref_player_playingtime": _PLAYINGTIME_COLS,
    "bronze_fbref_player_misc":        _MISC_COLS,
    "bronze_fbref_keeper_keeper":      _KEEPER_COLS,
}


def _ddl(table: str, cols: list[str]) -> str:
    defs = []
    for c in cols:
        if c == "_ingested_at":
            typ = "TIMESTAMP"
        elif c == "season":
            typ = "BIGINT"
        else:
            typ = "VARCHAR"
        defs.append(f'"{c}" {typ}')
    return f"CREATE TABLE {table} ({', '.join(defs)})"


@pytest.fixture()
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    for table, cols in _TABLES.items():
        con.execute(_ddl(table, cols))
    con.execute(
        """
        CREATE TABLE silver_fbref_player_identity (
            player_id VARCHAR, player_name VARCHAR, team_name VARCHAR,
            league VARCHAR, season VARCHAR, is_synthetic BOOLEAN,
            id_resolution VARCHAR, id_evidence_datasets VARCHAR[]
        )
        """
    )
    yield con
    con.close()


def _insert(con, table: str, **values):
    cols = ", ".join(f'"{c}"' for c in values)
    placeholders = ", ".join("?" for _ in values)
    con.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
        list(values.values()),
    )


_INGS = dict(player="Danny Ings", player_id="07802f7f", nation="eng ENG",
             pos="FW", league="ENG-Premier League", season=2022)

T1 = "2026-01-01 00:00:00"
T2 = "2026-02-01 00:00:00"


def _run_profile(con, sql_path: Path):
    # Seed the materialised identity dependency in the same shape as the
    # production transform: preserve native IDs and give residual blank IDs a
    # deterministic noid_ identity.
    con.execute("DELETE FROM silver_fbref_player_identity")
    con.execute(
        """
        INSERT INTO silver_fbref_player_identity
        SELECT DISTINCT
               CASE
                   WHEN NULLIF(TRIM(player_id), '') IS NOT NULL
                       THEN TRIM(player_id)
                   ELSE 'noid_' || md5(
                       league || '|' || CAST(season AS VARCHAR) || '|'
                       || lower(TRIM(player)) || '|' || lower(TRIM(squad))
                   )
               END,
               player, squad, league,
               lpad(CAST(season % 100 AS VARCHAR), 2, '0')
                 || lpad(CAST((season + 1) % 100 AS VARCHAR), 2, '0'),
               NULLIF(TRIM(player_id), '') IS NULL,
               CASE
                   WHEN NULLIF(TRIM(player_id), '') IS NULL
                       THEN 'synthetic_residual'
                   ELSE 'source_native'
               END,
               ['player_stats']
        FROM bronze_fbref_player_stats
        WHERE NULLIF(TRIM(player), '') IS NOT NULL
        """
    )
    sql = _translate(_read(sql_path))
    return con.execute(
        f"SELECT * FROM ({sql}) ORDER BY squad"
    ).fetch_df()


# ---------------------------------------------------------------------------
# fbref_player_season_profile — winter-transfer dedup
# ---------------------------------------------------------------------------


class TestPlayerSeasonProfileDedup:

    def test_winter_transfer_keeps_both_squads(self, duck_conn):
        # Arrange: one player, two squads, same (league, season), same batch
        for squad, minutes, gls in [("Aston Villa", "824", "6"),
                                    ("West Ham United", "775", "2")]:
            _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                    squad=squad, min=minutes, gls=gls,
                    _ingested_at=T1, _batch_id="batch-1")

        # Act
        df = _run_profile(duck_conn, PLAYER_PROFILE_SQL)

        # Assert: one silver row per (player, squad) — neither club dropped
        assert len(df) == 2, (
            f"winter transfer must keep one row per squad, got {len(df)}"
        )
        by_squad = {r["squad"]: r for _, r in df.iterrows()}
        assert by_squad["Aston Villa"]["goals"] == 6
        assert by_squad["West Ham United"]["goals"] == 2

    def test_no_cross_squad_fanout_in_joins(self, duck_conn):
        # Arrange: 2-squad player in stats AND shooting → naive JOIN on
        # (player_id, league, season) would fan out 2×2 and/or mix metrics
        for squad, minutes, gls, sh in [("Aston Villa", "824", "6", "30"),
                                        ("West Ham United", "775", "2", "11")]:
            _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                    squad=squad, min=minutes, gls=gls,
                    _ingested_at=T1, _batch_id="batch-1")
            _insert(duck_conn, "bronze_fbref_player_shooting", **_INGS,
                    squad=squad, sh=sh,
                    _ingested_at=T1, _batch_id="batch-1")

        # Act
        df = _run_profile(duck_conn, PLAYER_PROFILE_SQL)

        # Assert: exactly 2 rows, each squad joined to ITS OWN shooting row
        assert len(df) == 2, f"JOIN fan-out: expected 2 rows, got {len(df)}"
        by_squad = {r["squad"]: r for _, r in df.iterrows()}
        assert by_squad["Aston Villa"]["shots"] == 30
        assert by_squad["West Ham United"]["shots"] == 11

    def test_rescrape_latest_batch_wins(self, duck_conn):
        # Arrange: same (player, squad) re-scraped — later batch corrects goals
        _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                squad="Aston Villa", gls="5",
                _ingested_at=T1, _batch_id="batch-1")
        _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                squad="Aston Villa", gls="6",
                _ingested_at=T2, _batch_id="batch-2")

        # Act
        df = _run_profile(duck_conn, PLAYER_PROFILE_SQL)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["goals"] == 6, "latest _ingested_at must survive"

    def test_ingested_at_tie_broken_by_batch_id(self, duck_conn):
        # Arrange: two batches share _ingested_at → tiebreaker = max _batch_id
        _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                squad="Aston Villa", gls="5",
                _ingested_at=T1, _batch_id="batch-a")
        _insert(duck_conn, "bronze_fbref_player_stats", **_INGS,
                squad="Aston Villa", gls="6",
                _ingested_at=T1, _batch_id="batch-b")

        # Act
        df = _run_profile(duck_conn, PLAYER_PROFILE_SQL)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["goals"] == 6, (
            "tie on _ingested_at must resolve deterministically (max _batch_id)"
        )


# ---------------------------------------------------------------------------
# fbref_keeper_profile — same key, GK spine
# ---------------------------------------------------------------------------


class TestKeeperProfileDedup:

    def test_winter_transfer_keeps_both_squads(self, duck_conn):
        # Arrange: GK transferred mid-season (1 live case in keeper_keeper)
        gk = dict(_INGS, player="Test Keeper", player_id="ab12cd34", pos="GK")
        for squad, saves in [("Aston Villa", "40"), ("West Ham United", "15")]:
            _insert(duck_conn, "bronze_fbref_player_stats", **gk, squad=squad,
                    _ingested_at=T1, _batch_id="batch-1")
            _insert(duck_conn, "bronze_fbref_keeper_keeper", **gk, squad=squad,
                    saves=saves, _ingested_at=T1, _batch_id="batch-1")

        # Act
        df = _run_profile(duck_conn, KEEPER_PROFILE_SQL)

        # Assert: both squads survive, keeper stats not cross-mixed
        assert len(df) == 2, (
            f"GK winter transfer must keep one row per squad, got {len(df)}"
        )
        by_squad = {r["squad"]: r for _, r in df.iterrows()}
        assert by_squad["Aston Villa"]["saves"] == 40
        assert by_squad["West Ham United"]["saves"] == 15

    def test_blank_keeper_id_uses_materialised_identity(self, duck_conn):
        gk = dict(
            _INGS,
            player="Missing Id Keeper",
            player_id="",
            pos="GK",
            squad="Identity FC",
            _ingested_at=T1,
            _batch_id="batch-blank",
        )
        _insert(duck_conn, "bronze_fbref_player_stats", **gk)
        _insert(
            duck_conn,
            "bronze_fbref_keeper_keeper",
            **gk,
            saves="7",
        )

        df = _run_profile(duck_conn, KEEPER_PROFILE_SQL)

        assert len(df) == 1
        assert df.iloc[0]["player_id"].startswith("noid_")
        assert df.iloc[0]["player_id_resolution"] == "synthetic_residual"
        assert bool(df.iloc[0]["player_id_is_synthetic"]) is True
        assert df.iloc[0]["saves"] == 7


# ---------------------------------------------------------------------------
# Regex sanity — dedup keys of all five FBref silver files
# ---------------------------------------------------------------------------


def _partition_by_clauses(sql: str) -> list[str]:
    body = _strip_comments(sql)
    return re.findall(r"PARTITION BY\s+(.+?)\s+ORDER BY", body, re.DOTALL | re.IGNORECASE)


class TestDedupKeys:

    @pytest.mark.parametrize("path", [PLAYER_PROFILE_SQL, KEEPER_PROFILE_SQL],
                             ids=["player_profile", "keeper_profile"])
    def test_profile_partition_by_includes_squad(self, path):
        clauses = _partition_by_clauses(_read(path))
        assert clauses, f"no dedup windows found in {path.name}"
        for clause in clauses:
            assert "squad" in clause, (
                f"{path.name}: PARTITION BY missing squad → winter-transfer "
                f"rows silently dropped (#463): {clause!r}"
            )

    @pytest.mark.parametrize("path", [PLAYER_PROFILE_SQL, KEEPER_PROFILE_SQL],
                             ids=["player_profile", "keeper_profile"])
    def test_profile_joins_include_squad(self, path):
        body = _strip_comments(_read(path))
        join_aliases = re.findall(r"LEFT JOIN\s+(\w+)", body, re.IGNORECASE)
        assert join_aliases, f"no LEFT JOINs found in {path.name}"
        for alias in join_aliases:
            assert re.search(rf"s\.squad\s*=\s*{alias}\.squad", body), (
                f"{path.name}: JOIN to {alias} missing squad predicate → "
                f"2×2 fan-out for multi-squad players (#463)"
            )

    @pytest.mark.parametrize("path", [MATCH_LINEUPS_SQL, MATCH_EVENTS_SQL],
                             ids=["lineups", "events"])
    def test_null_safe_player_key(self, path):
        clauses = [c for c in _partition_by_clauses(_read(path))
                   if "player" in c.lower()]
        assert clauses, f"no player dedup window found in {path.name}"
        for clause in clauses:
            if path == MATCH_LINEUPS_SQL:
                assert "resolved_player_id" in clause, (
                    f"{path.name}: identity must resolve blank IDs before "
                    f"deduplication: {clause!r}"
                )
            else:
                assert re.search(
                    r"COALESCE\s*\(\s*player_id\s*,\s*player\s*\)",
                    clause,
                    re.IGNORECASE,
                ), (
                    f"{path.name}: player key must be NULL-safe — "
                    f"COALESCE(player_id, player) (#463): {clause!r}"
                )

    def test_enriched_dedup_keys_mirror_detail_tables(self):
        """fbref_match_enriched re-implements the events/lineups dedup CTEs —
        keys MUST stay in sync with the detail silver files."""
        body = _strip_comments(_read(MATCH_ENRICHED_SQL))
        player_clauses = [
            c for c in re.findall(
                r"PARTITION BY\s+(.+?)\s+ORDER BY", body, re.DOTALL | re.IGNORECASE)
            if "player" in c.lower()
        ]
        assert len(player_clauses) == 2, (
            "expected events_dedup + lineups_dedup windows in fbref_match_enriched"
        )
        for clause in player_clauses:
            assert re.search(r"COALESCE\s*\(\s*player_id\s*,\s*player\s*\)",
                             clause, re.IGNORECASE), (
                f"fbref_match_enriched dedup key must be NULL-safe (#463): {clause!r}"
            )

    @pytest.mark.parametrize("path", [
        PLAYER_PROFILE_SQL, KEEPER_PROFILE_SQL,
        MATCH_LINEUPS_SQL, MATCH_EVENTS_SQL, MATCH_ENRICHED_SQL,
    ], ids=["player_profile", "keeper_profile", "lineups", "events", "enriched"])
    def test_deterministic_tiebreaker(self, path):
        """Every dedup window must order by _ingested_at DESC, _batch_id DESC —
        ties inside one batch otherwise pick an arbitrary survivor per rebuild."""
        body = _strip_comments(_read(path))
        windows = re.findall(
            r"PARTITION BY\s+.+?ORDER BY\s+(.+?)\s*\)\s*AS\s+rn",
            body, re.DOTALL | re.IGNORECASE)
        assert windows, f"no dedup windows found in {path.name}"
        for order in windows:
            assert re.search(r"_ingested_at\s+DESC\s*,\s*_batch_id\s+DESC",
                             order, re.IGNORECASE), (
                f"{path.name}: dedup ORDER BY lacks deterministic tiebreaker "
                f"(_ingested_at DESC, _batch_id DESC) (#463): {order!r}"
            )
