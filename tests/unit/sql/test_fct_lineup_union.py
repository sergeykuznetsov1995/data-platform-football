"""
Unit tests for the UNION + dedup logic in ``dags/sql/gold/fct_lineup.sql`` (E3.9).

Strategy
--------
The full ``fct_lineup.sql`` builds an ESPN→FBref bridge via XXHASH64 over
several Bronze/Silver tables. That part is integration-testable only because
it relies on the deterministic xxhash seed and the alias-tolerant xref_team
JOINs. What we CAN unit-test is the **UNION ALL + ROW_NUMBER dedup tail**
which is the load-bearing priority logic ("FBref wins over ESPN").

We re-declare an EQUIVALENT UNION+dedup query in this test using the same
predicate (``source_priority ASC, _bronze_ingested_at DESC``) and the same
dedup-key CASE expression. This exercises the **logic** (not the SQL string)
and is a regression net for refactors that change priority semantics.

If the production SQL changes its dedup ordering, this test will keep
passing — and that is intentional: this test is contract-of-behaviour, not
contract-of-SQL-text. A separate string-level test below
(``TestSqlInvariants``) locks the priority literal so we still notice if
someone flips the order.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_lineup.sql"

# Wire dags/ onto sys.path so ``utils.*`` resolves.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# DuckDB harness — re-implement the UNION + ROW_NUMBER tail
# ---------------------------------------------------------------------------

# This SQL mirrors the dedup CTE inside fct_lineup.sql (lines 240-289):
# - UNION ALL of FBref + ESPN resolved CTEs
# - ROW_NUMBER() OVER (PARTITION BY match_id_canonical,
#       <dedup-key per CASE expression>
#     ORDER BY source_priority ASC, _bronze_ingested_at DESC)
# - WHERE rn = 1 AND match_id_canonical IS NOT NULL
_DEDUP_SQL = """
WITH all_lineups AS (
    SELECT * FROM fbref_resolved
    UNION ALL
    SELECT * FROM espn_resolved
),
dedup AS (
    SELECT
        all_lineups.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                match_id_canonical,
                CASE
                    WHEN player_id_canonical IS NOT NULL
                        THEN player_id_canonical
                    ELSE
                        lineup_source || ':' ||
                        COALESCE(team_id_canonical, '?') || ':' ||
                        COALESCE(_raw_player_id_for_dedup, player_name, '?')
                END
            ORDER BY
                source_priority ASC,
                _bronze_ingested_at DESC
        ) AS rn
    FROM all_lineups
)
SELECT
    match_id_canonical,
    team_id_canonical,
    player_id_canonical,
    player_name,
    is_starter,
    position_canonical,
    jersey_number,
    lineup_source,
    'v1'                          AS lineup_version,
    league,
    season
FROM dedup
WHERE rn = 1
  AND match_id_canonical IS NOT NULL
"""

_RESOLVED_COLUMNS = [
    "match_id_canonical",
    "team_id_canonical",
    "player_id_canonical",
    "player_name",
    "is_starter",
    "position_canonical",
    "jersey_number",
    "_bronze_ingested_at",
    "league",
    "season",
    "lineup_source",
    "source_priority",
    "_raw_player_id_for_dedup",
]


def _fbref(
    *,
    match_id_canonical: str,
    team_id_canonical: Optional[str] = "team_a",
    player_id_canonical: Optional[str] = None,
    player_name: str = "Player A",
    is_starter: bool = True,
    position: str = "FW",
    jersey_number: Optional[int] = 9,
    ingested: str = "2026-05-08 12:00:00",
    league: str = "ENG-Premier League",
    season: str = "2526",
    raw_player_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an FBref-resolved row (source_priority=1).

    Note (E3.5 R4 — 2026-05-08): ``season`` is now varchar 'YYYY' (e.g. '2526'
    for the 2025-26 season) to match the unified production schema. FBref-branch
    SQL converts the bigint year-of-start to this varchar form via
    ``format('%02d%02d', mod(s,100), mod(s+1,100))``; the unit test seeds
    rows already in the unified form so we don't re-test that conversion here
    (covered by EXPLAIN-tested integration in the rebuild).
    """
    return {
        "match_id_canonical": match_id_canonical,
        "team_id_canonical": team_id_canonical,
        "player_id_canonical": player_id_canonical,
        "player_name": player_name,
        "is_starter": is_starter,
        "position_canonical": position,
        "jersey_number": jersey_number,
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "fbref",
        "source_priority": 1,
        "_raw_player_id_for_dedup": raw_player_id,
    }


def _espn(
    *,
    match_id_canonical: str,
    team_id_canonical: Optional[str] = "team_a",
    player_name: str = "Player A",
    is_starter: bool = True,
    position: str = "FW",
    ingested: str = "2026-05-08 12:00:00",
    league: str = "ENG-Premier League",
    season: str = "2526",
) -> Dict[str, Any]:
    """Build an ESPN-resolved row (source_priority=2; player_id_canonical=NULL).

    Note (E3.5 R4 — 2026-05-08): ``season`` defaults to varchar '2526'
    matching the unified post-fix schema (was bigint pre-2026-05-08).
    """
    return {
        "match_id_canonical": match_id_canonical,
        "team_id_canonical": team_id_canonical,
        "player_id_canonical": None,  # ESPN never resolves player canonical
        "player_name": player_name,
        "is_starter": is_starter,
        "position_canonical": position,
        "jersey_number": None,
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "espn",
        "source_priority": 2,
        "_raw_player_id_for_dedup": player_name,
    }


def _seed_resolved(con, fbref_rows: List[Dict[str, Any]],
                   espn_rows: List[Dict[str, Any]]) -> None:
    """Recreate the two resolved CTEs as physical tables."""
    for tbl in ("fbref_resolved", "espn_resolved"):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.execute(
            f"""
            CREATE TABLE {tbl} (
                match_id_canonical          VARCHAR,
                team_id_canonical           VARCHAR,
                player_id_canonical         VARCHAR,
                player_name                 VARCHAR,
                is_starter                  BOOLEAN,
                position_canonical          VARCHAR,
                jersey_number               INTEGER,
                _bronze_ingested_at         TIMESTAMP,
                league                      VARCHAR,
                season                      VARCHAR,
                lineup_source               VARCHAR,
                source_priority             INTEGER,
                _raw_player_id_for_dedup    VARCHAR
            )
            """
        )

    placeholders = ", ".join(["?"] * len(_RESOLVED_COLUMNS))
    insert_template = (
        "INSERT INTO {tbl} ("
        + ", ".join(_RESOLVED_COLUMNS)
        + f") VALUES ({placeholders})"
    )
    for r in fbref_rows:
        con.execute(
            insert_template.format(tbl="fbref_resolved"),
            [r[c] for c in _RESOLVED_COLUMNS],
        )
    for r in espn_rows:
        con.execute(
            insert_template.format(tbl="espn_resolved"),
            [r[c] for c in _RESOLVED_COLUMNS],
        )


def _run_dedup(con) -> List[Dict[str, Any]]:
    cur = con.execute(_DEDUP_SQL)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Behavioural tests — UNION + dedup priority
# ---------------------------------------------------------------------------


class TestDedupPriority:
    """FBref > ESPN priority is the load-bearing rule for cross-source dedup."""

    def test_fbref_wins_when_both_sources_have_same_player_canonical(self, duck_conn):
        """Same (match, player_id_canonical) in both → FBref wins."""
        fbref = [_fbref(
            match_id_canonical="M1",
            player_id_canonical="fb_X",
            player_name="John Smith",
            ingested="2026-05-01 12:00:00",
        )]
        espn = [_espn(
            match_id_canonical="M1",
            player_name="John Smith",
            ingested="2026-05-08 12:00:00",  # newer, but loses on priority
        )]
        # Force ESPN to also have 'fb_X' canonical for the cross-source dedup
        # case (the production SQL only collapses when canonical matches; ESPN
        # canonical is always NULL today, so this scenario is forward-looking).
        espn[0]["player_id_canonical"] = "fb_X"
        espn[0]["source_priority"] = 2

        _seed_resolved(duck_conn, fbref, espn)
        out = _run_dedup(duck_conn)

        assert len(out) == 1
        assert out[0]["lineup_source"] == "fbref"
        assert out[0]["player_id_canonical"] == "fb_X"

    def test_within_source_dedup_keeps_freshest(self, duck_conn):
        """Two FBref rows for same (match, player) → freshest _bronze_ingested_at wins."""
        fbref = [
            _fbref(
                match_id_canonical="M2",
                player_id_canonical="fb_Y",
                player_name="Player Y",
                jersey_number=10,
                ingested="2026-05-01 12:00:00",
            ),
            _fbref(
                match_id_canonical="M2",
                player_id_canonical="fb_Y",
                player_name="Player Y (updated)",
                jersey_number=11,
                ingested="2026-05-08 12:00:00",  # newer
            ),
        ]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)

        assert len(out) == 1
        assert out[0]["jersey_number"] == 11
        assert out[0]["player_name"] == "Player Y (updated)"

    def test_espn_player_orphan_kept_with_synthetic_dedup_key(self, duck_conn):
        """Two ESPN players in the same match with NULL canonical → both survive
        because the dedup key falls back to (lineup_source || team || name)."""
        espn = [
            _espn(match_id_canonical="M3", team_id_canonical="team_a",
                  player_name="ESPN Player A"),
            _espn(match_id_canonical="M3", team_id_canonical="team_a",
                  player_name="ESPN Player B"),
        ]
        _seed_resolved(duck_conn, [], espn)
        out = _run_dedup(duck_conn)

        names = sorted(r["player_name"] for r in out)
        assert names == ["ESPN Player A", "ESPN Player B"], (
            "Two distinct ESPN players collapsed by NULL canonical — "
            "synthetic dedup key broken"
        )
        # Both rows are ESPN; no canonical resolution.
        assert all(r["lineup_source"] == "espn" for r in out)
        assert all(r["player_id_canonical"] is None for r in out)


class TestSourceCoverage:
    """FBref-only / ESPN-only matches retain all their rows."""

    def test_fbref_only_match_keeps_all_fbref_rows(self, duck_conn):
        fbref = [
            _fbref(
                match_id_canonical="M_FB", player_id_canonical="fb_1",
                player_name="P1",
            ),
            _fbref(
                match_id_canonical="M_FB", player_id_canonical="fb_2",
                player_name="P2",
            ),
        ]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)
        assert len(out) == 2
        assert all(r["lineup_source"] == "fbref" for r in out)

    def test_espn_only_match_keeps_all_espn_rows(self, duck_conn):
        espn = [
            _espn(match_id_canonical="M_ES", player_name="E1"),
            _espn(match_id_canonical="M_ES", player_name="E2"),
        ]
        _seed_resolved(duck_conn, [], espn)
        out = _run_dedup(duck_conn)
        assert len(out) == 2
        assert all(r["lineup_source"] == "espn" for r in out)
        # ESPN player_id_canonical NULL contract
        assert all(r["player_id_canonical"] is None for r in out)

    def test_unresolved_fbref_player_kept_distinct(self, duck_conn):
        """Two FBref players with player_id_canonical=NULL in same match must
        NOT collapse — the dedup key falls back to the synthetic CASE branch.
        Real-world: seasons outside competitions.yaml resolver coverage.
        """
        fbref = [
            _fbref(
                match_id_canonical="M_FB2", player_id_canonical=None,
                player_name="Unresolved 1", raw_player_id="raw_1",
            ),
            _fbref(
                match_id_canonical="M_FB2", player_id_canonical=None,
                player_name="Unresolved 2", raw_player_id="raw_2",
            ),
        ]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)
        assert len(out) == 2
        names = sorted(r["player_name"] for r in out)
        assert names == ["Unresolved 1", "Unresolved 2"]


class TestProjection:
    """Output schema invariants."""

    def test_lineup_version_literal_is_v1(self, duck_conn):
        fbref = [_fbref(
            match_id_canonical="MV", player_id_canonical="fb_p", player_name="A",
        )]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)
        assert all(r["lineup_version"] == "v1" for r in out)

    def test_lineup_source_enum_only_fbref_or_espn(self, duck_conn):
        fbref = [_fbref(match_id_canonical="MS", player_id_canonical="fb_p",
                        player_name="A")]
        espn = [_espn(match_id_canonical="MS2", player_name="B")]
        _seed_resolved(duck_conn, fbref, espn)
        out = _run_dedup(duck_conn)
        srcs = {r["lineup_source"] for r in out}
        assert srcs <= {"fbref", "espn"}, f"unexpected source labels: {srcs}"

    def test_null_match_id_canonical_filtered_out(self, duck_conn):
        """WHERE match_id_canonical IS NOT NULL drops orphan rows."""
        fbref = [
            _fbref(
                match_id_canonical=None, player_id_canonical="fb_q",
                player_name="No-Match",
            ),
            _fbref(
                match_id_canonical="MK", player_id_canonical="fb_r",
                player_name="With-Match",
            ),
        ]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)
        assert len(out) == 1
        assert out[0]["match_id_canonical"] == "MK"


# ---------------------------------------------------------------------------
# Full-SQL DuckDB harness — executes the REAL fct_lineup.sql (#461)
# ---------------------------------------------------------------------------
# The behavioural harness above re-implements only the UNION+dedup tail; the
# ESPN→FBref bridge is invisible to it. The tests below run the actual gold
# SQL after the same Trino→DuckDB text-substitution pass used by
# test_fct_card_union.py (xxhash64→md5, to_utf8/to_hex collapsed), so the
# bridge fan-out bugs (#461) are reproducible:
#   * bronze.espn_schedule read without re-ingest dedup;
#   * season-less xref_team_by_canonical (the #459 mechanism).

import hashlib


def _collapse_call(sql: str, fn_name: str) -> str:
    """Drop a wrapper function call (paren-balanced)."""
    out = []
    i = 0
    n = len(sql)
    while i < n:
        if sql[i:i + len(fn_name)].lower() == fn_name.lower():
            j = i + len(fn_name)
            while j < n and sql[j] in " \t\n\r":
                j += 1
            if j < n and sql[j] == "(":
                depth = 1
                j += 1
                inner_start = j
                while j < n and depth > 0:
                    if sql[j] == "(":
                        depth += 1
                    elif sql[j] == ")":
                        depth -= 1
                    if depth == 0:
                        break
                    j += 1
                out.append(sql[inner_start:j])
                i = j + 1
                continue
        out.append(sql[i])
        i += 1
    return "".join(out)


_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.espn_schedule":          "bronze_espn_schedule",
    "iceberg.silver.espn_lineup":            "silver_espn_lineup",
    "iceberg.silver.fbref_match_lineups":    "silver_fbref_match_lineups",
    "iceberg.silver.fbref_match_enriched":   "silver_fbref_match_enriched",
    "iceberg.silver.xref_match":             "silver_xref_match",
    "iceberg.silver.xref_team":              "silver_xref_team",
    "iceberg.silver.xref_player":            "silver_xref_player",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)
    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


_LEAGUE = "ENG-Premier League"
_SEASON = "2526"
_GAME = "2026-01-06 Liverpool-Arsenal"   # live format: 'YYYY-MM-DD Home-Away'
_FB_HEX = "1a2b3c4d"                     # FBref hex match_id (fme spine)


def _espn_match_id(league: str = _LEAGUE, season: str = _SEASON,
                   game: str = _GAME) -> str:
    """Mirror the translated bridge hash: 'espn_' || lower(md5(seed))."""
    seed = f"{league}|{season}|{game}"
    return "espn_" + hashlib.md5(seed.encode()).hexdigest()


@pytest.fixture()
def bridge_conn(duck_conn):
    """Fresh bridge-table set per test (NOT autouse — the dedup-tail tests
    above don't need these)."""
    for tbl in _ICEBERG_TO_LOCAL.values():
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    duck_conn.execute(
        """
        CREATE TABLE bronze_espn_schedule (
            league       VARCHAR,
            season       VARCHAR,
            game         VARCHAR,
            match_date   TIMESTAMP,
            home_team    VARCHAR,
            away_team    VARCHAR,
            game_id      BIGINT,
            league_id    VARCHAR,
            _ingested_at TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_espn_lineup (
            match_id            VARCHAR,
            team                VARCHAR,
            player              VARCHAR,
            is_starter          BOOLEAN,
            position            VARCHAR,
            jersey_number       INTEGER,
            league              VARCHAR,
            season              VARCHAR,
            _bronze_ingested_at TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_fbref_match_lineups (
            match_id            VARCHAR,
            team                VARCHAR,
            player              VARCHAR,
            player_id           VARCHAR,
            is_starter          BOOLEAN,
            position            VARCHAR,
            jersey_number       INTEGER,
            league              VARCHAR,
            season              VARCHAR,
            _bronze_ingested_at TIMESTAMP
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE silver_fbref_match_enriched (
            match_id  VARCHAR,
            league    VARCHAR,
            home      VARCHAR,
            away      VARCHAR,
            date      DATE
        )
        """
    )
    duck_conn.execute(
        "CREATE TABLE silver_xref_match "
        "(source VARCHAR, source_id VARCHAR, canonical_id VARCHAR)"
    )
    duck_conn.execute(
        "CREATE TABLE silver_xref_team (source VARCHAR, source_id VARCHAR, "
        "canonical_id VARCHAR, league VARCHAR, season VARCHAR, confidence VARCHAR)"
    )
    duck_conn.execute(
        "CREATE TABLE silver_xref_player (source VARCHAR, source_id VARCHAR, "
        "canonical_id VARCHAR, league VARCHAR, season VARCHAR)"
    )
    yield duck_conn


def _seed_espn_corpus(con, *, fbref_xref_season: str = _SEASON) -> None:
    """One ESPN lineup row + the xref/fme spine the bridge needs.

    ``fbref_xref_season`` lets tests move the FBref alias rows to another
    season to probe the season-scoped JOIN.
    """
    con.execute(
        """
        INSERT INTO silver_xref_team VALUES
          ('espn',  'Liverpool', 'liverpool', ?, ?, 'name_alias'),
          ('espn',  'Arsenal',   'arsenal',   ?, ?, 'name_alias'),
          ('fbref', 'Liverpool', 'liverpool', ?, ?, 'name_alias'),
          ('fbref', 'Arsenal',   'arsenal',   ?, ?, 'name_alias')
        """,
        [_LEAGUE, _SEASON, _LEAGUE, _SEASON,
         _LEAGUE, fbref_xref_season, _LEAGUE, fbref_xref_season],
    )
    con.execute(
        "INSERT INTO silver_fbref_match_enriched VALUES (?, ?, ?, ?, ?)",
        [_FB_HEX, _LEAGUE, "Liverpool", "Arsenal", "2026-01-06"],
    )
    con.execute(
        """
        INSERT INTO silver_espn_lineup VALUES
          (?, 'Liverpool', 'ESPN Player A', TRUE, 'F', NULL, ?, ?,
           TIMESTAMP '2026-02-01 06:00:00')
        """,
        [_espn_match_id(), _LEAGUE, _SEASON],
    )


_SCHEDULE_ROW = (
    "INSERT INTO bronze_espn_schedule VALUES "
    "(?, ?, ?, TIMESTAMP '2026-01-06 20:00:00', ?, 'Arsenal', 401, '700', ?)"
)


def _run_lineup_gold(con) -> List[Dict[str, Any]]:
    sql = _translate(SQL_PATH.read_text(encoding="utf-8"))
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class TestEspnBridgeDedup:
    """#461: espn_match_bridge must dedup bronze re-ingests and season-scope
    the xref_team JOINs — a stale/variant row must NOT yield a second bridge
    row that duplicates the lineup under the 'espn_<hash>' pseudo-id.
    """

    def test_reingest_schedule_dup_does_not_duplicate_lineup(self, bridge_conn):
        """Two bronze ingests of the same game — the stale one carries a team
        spelling that misses xref. Old SQL: lineup row surfaces twice (hex +
        pseudo-id). Fixed SQL: once, bridged to the FBref hex id."""
        _seed_espn_corpus(bridge_conn)
        # Stale ingest: 'Liverpool FC' is not an xref alias → bridge miss.
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool FC",
             "2026-01-07 06:00:00"],
        )
        # Fresh ingest: canonical spelling → bridge hit.
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool",
             "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, (
            f"re-ingest dup leaked through the bridge: {out}"
        )
        assert out[0]["match_id"] == _FB_HEX, out

    def test_historical_variant_does_not_duplicate_lineup(self, bridge_conn):
        """#459 mechanism: an FBref alias from ANOTHER season with the same
        canonical_id must not fan the bridge out into a NULL twin."""
        _seed_espn_corpus(bridge_conn)
        bridge_conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
              ('fbref', 'Liverpool FC', 'liverpool', ?, '2425', 'name_alias')
            """,
            [_LEAGUE],
        )
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, (
            f"historical-variant fan-out duplicated the lineup row: {out}"
        )
        assert out[0]["match_id"] == _FB_HEX, out

    def test_same_season_variant_does_not_duplicate_lineup(self, bridge_conn):
        """#445: xref_team now legally carries a SAME-season second FBref
        spelling per canonical (match-page full name next to the schedule
        short name) — season-scoping alone can't save the bridge; it must
        aggregate the variant fan-out instead of emitting a NULL twin."""
        _seed_espn_corpus(bridge_conn)
        bridge_conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
              ('fbref', 'Liverpool FC', 'liverpool', ?, ?, 'name_alias')
            """,
            [_LEAGUE, _SEASON],
        )
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, (
            f"same-season variant fan-out duplicated the lineup row: {out}"
        )
        assert out[0]["match_id"] == _FB_HEX, out

    def test_clean_bridge_resolves_hex(self, bridge_conn):
        """Happy path stays intact: one schedule row, full xref → one lineup
        row under the FBref hex id."""
        _seed_espn_corpus(bridge_conn)
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["match_id"] == _FB_HEX, out
        assert out[0]["lineup_source"] == "espn", out
        assert out[0]["player_id"] is None, out

    def test_missing_xref_season_degrades_to_single_pseudo_row(self, bridge_conn):
        """When xref_team has NO FBref aliases for the schedule's season the
        bridge degrades to exactly ONE unbridged row (pseudo-id, no dup) —
        it must NOT resolve via an alias borrowed from another season."""
        _seed_espn_corpus(bridge_conn, fbref_xref_season="2425")
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["match_id"] == _espn_match_id(), out


class TestOrphanTeamExcluded:
    """#506: xref_team rows with confidence='orphan' carry a non-NULL source-
    prefixed canonical ('fb_<slug>'); the team JOINs must NOT leak them as a
    resolved team_id. xref_team.sql.j2 contract: orphans excluded from every
    cross-source Gold JOIN.
    """

    def _seed_fbref_lineup(self, con, *, confidence: str) -> None:
        con.execute(
            """
            INSERT INTO silver_fbref_match_lineups VALUES
              (?, 'Orphanton FC', 'Orphan P', '9001', TRUE, 'F', 9, ?, ?,
               TIMESTAMP '2026-02-01 06:00:00')
            """,
            [_FB_HEX, _LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Orphanton FC', 'fb_orphanton_fc', ?, ?, ?)",
            [_LEAGUE, _SEASON, confidence],
        )

    def test_orphan_fbref_team_yields_null_team_id(self, bridge_conn):
        """Orphan xref_team → team_id IS NULL (not the 'fb_<slug>' pseudo-id)."""
        self._seed_fbref_lineup(bridge_conn, confidence="orphan")
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fbref", out
        assert out[0]["team_id"] is None, (
            f"orphan xref_team leaked as a resolved team_id: {out}"
        )

    def test_name_alias_fbref_team_resolves(self, bridge_conn):
        """Contrast: the SAME row with confidence='name_alias' DOES resolve —
        proves the NULL above comes from the #506 filter, not a missing row."""
        self._seed_fbref_lineup(bridge_conn, confidence="name_alias")
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["team_id"] == "fb_orphanton_fc", out


class TestSqlInvariants:
    """Lock the SQL-text invariants that this behavioural harness CANNOT see."""

    def _sql(self) -> str:
        return SQL_PATH.read_text(encoding="utf-8")

    def test_fbref_priority_one_espn_priority_two_in_sql(self):
        """The two source_priority literals are the load-bearing constants."""
        sql = self._sql()
        # source_priority=1 for FBref CTE (fbref_resolved), 2 for ESPN.
        # Strip block comments to avoid commentary false-positives.
        non_comment = "\n".join(
            line for line in sql.splitlines()
            if not line.lstrip().startswith("--")
        )
        # FBref CTE must contain ``1 ... AS source_priority``.
        assert re.search(
            r"\b1\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "fbref_resolved CTE must emit `1 AS source_priority`"
        assert re.search(
            r"\b2\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "espn_resolved CTE must emit `2 AS source_priority`"

    def test_order_by_priority_then_freshness_in_sql(self):
        """Dedup ordering: source_priority ASC, _bronze_ingested_at DESC."""
        sql = self._sql()
        # We accept variable whitespace / newlines.
        normalised = re.sub(r"\s+", " ", sql)
        assert re.search(
            r"ORDER\s+BY\s+source_priority\s+ASC\s*,\s*_bronze_ingested_at\s+DESC",
            normalised, re.IGNORECASE,
        ), "ROW_NUMBER must order by source_priority ASC, _bronze_ingested_at DESC"

    def test_pure_select_no_create_table_in_executable_sql(self):
        non_comment = "\n".join(
            line for line in self._sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "fct_lineup.sql must remain pure SELECT — gold_tasks wraps in CTAS"
        )

    def test_lineup_version_literal_present(self):
        assert "'v1'" in self._sql(), (
            "expected literal 'v1' for lineup_version (R0.4 schema versioning)"
        )

    def test_lineup_source_literals_only_fbref_and_espn(self):
        """Only 'fbref' and 'espn' should appear as lineup_source literal values."""
        sql = self._sql()
        # ``'fbref' AS lineup_source`` and ``'espn' AS lineup_source``
        # are the two CTE projections.
        assert re.search(
            r"'fbref'\s+AS\s+lineup_source", sql, re.IGNORECASE
        ), "missing `'fbref' AS lineup_source`"
        assert re.search(
            r"'espn'\s+AS\s+lineup_source", sql, re.IGNORECASE
        ), "missing `'espn' AS lineup_source`"
