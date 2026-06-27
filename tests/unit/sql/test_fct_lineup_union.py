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
    UNION ALL
    SELECT * FROM sofascore_resolved
    UNION ALL
    SELECT * FROM fotmob_resolved
    UNION ALL
    SELECT * FROM whoscored_resolved
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
    is_captain,
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
    "is_captain",
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
        "is_captain": None,  # FBref lineups carry no captaincy (bridge-enriched)
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
    """Build an ESPN-resolved row (source_priority=5; player_id_canonical=NULL).

    #693: ESPN moved from priority 2 to 5 (tail) when SofaScore/FotMob/WhoScored
    were added as fuller sources. ESPN never wins dedup (NULL player_id), so the
    exact tail value is immaterial — only that FBref(1) and SofaScore(2) precede it.

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
        "is_captain": None,  # ESPN matchsheet carries no captaincy
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "espn",
        "source_priority": 5,  # #693: ESPN parked at tail
        "_raw_player_id_for_dedup": player_name,
    }


def _sofascore(
    *,
    match_id_canonical: str,
    team_id_canonical: Optional[str] = "team_a",
    player_id_canonical: Optional[str] = None,
    is_starter: bool = True,
    position: str = "F",
    is_captain: Optional[bool] = None,
    ingested: str = "2026-05-08 12:00:00",
    league: str = "ENG-Premier League",
    season: str = "2526",
    raw_player_id: str = "ss_native_1",
) -> Dict[str, Any]:
    """Build a SofaScore-resolved row (source_priority=2; #693).

    SofaScore resolves a REAL player_id_canonical via xref_player (unlike ESPN),
    so cross-source dedup against FBref fires. is_captain is native here (from
    the /lineups overlay). No jersey_number / player_name in the SS aggregate.
    ``raw_player_id`` mirrors the native SofaScore player_id used as the dedup
    fallback key when player_id_canonical is NULL.
    """
    return {
        "match_id_canonical": match_id_canonical,
        "team_id_canonical": team_id_canonical,
        "player_id_canonical": player_id_canonical,
        "player_name": None,  # SS aggregate has no player_name column
        "is_starter": is_starter,
        "position_canonical": position,
        "jersey_number": None,
        "is_captain": is_captain,
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "sofascore",
        "source_priority": 2,
        "_raw_player_id_for_dedup": raw_player_id,
    }


def _fotmob(
    *,
    match_id_canonical: str,
    team_id_canonical: Optional[str] = "team_a",
    player_id_canonical: Optional[str] = None,
    is_starter: bool = True,
    position: str = "11",
    jersey_number: Optional[int] = 7,
    ingested: str = "2026-05-08 12:00:00",
    league: str = "ENG-Premier League",
    season: str = "2526",
    raw_player_id: str = "fm_native_1",
) -> Dict[str, Any]:
    """Build a FotMob-resolved row (source_priority=3; #693).

    FotMob resolves a real player_id and carries jersey_number (shirtNumber) +
    is_starter; is_captain is NULL (lineup_json has no captaincy). position is
    the FotMob positionId CODE as varchar.
    """
    return {
        "match_id_canonical": match_id_canonical,
        "team_id_canonical": team_id_canonical,
        "player_id_canonical": player_id_canonical,
        "player_name": None,
        "is_starter": is_starter,
        "position_canonical": position,
        "jersey_number": jersey_number,
        "is_captain": None,
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "fotmob",
        "source_priority": 3,
        "_raw_player_id_for_dedup": raw_player_id,
    }


def _whoscored(
    *,
    match_id_canonical: str,
    team_id_canonical: Optional[str] = "team_a",
    player_id_canonical: Optional[str] = None,
    is_starter: bool = True,
    ingested: str = "2026-05-08 12:00:00",
    league: str = "ENG-Premier League",
    season: str = "2526",
    raw_player_id: str = "ws_native_1",
) -> Dict[str, Any]:
    """Build a WhoScored-resolved row (source_priority=4; #693).

    Thinnest source: real player_id + is_starter (inferred) only — position,
    is_captain, jersey_number are all NULL (WhoScored events have no lineup block).
    """
    return {
        "match_id_canonical": match_id_canonical,
        "team_id_canonical": team_id_canonical,
        "player_id_canonical": player_id_canonical,
        "player_name": None,
        "is_starter": is_starter,
        "position_canonical": None,
        "jersey_number": None,
        "is_captain": None,
        "_bronze_ingested_at": ingested,
        "league": league,
        "season": season,
        "lineup_source": "whoscored",
        "source_priority": 4,
        "_raw_player_id_for_dedup": raw_player_id,
    }


def _seed_resolved(con, fbref_rows: List[Dict[str, Any]],
                   espn_rows: List[Dict[str, Any]],
                   sofascore_rows: Optional[List[Dict[str, Any]]] = None,
                   fotmob_rows: Optional[List[Dict[str, Any]]] = None,
                   whoscored_rows: Optional[List[Dict[str, Any]]] = None) -> None:
    """Recreate the resolved CTEs as physical tables (all 5 sources)."""
    sofascore_rows = sofascore_rows or []
    fotmob_rows = fotmob_rows or []
    whoscored_rows = whoscored_rows or []
    for tbl in ("fbref_resolved", "espn_resolved", "sofascore_resolved",
                "fotmob_resolved", "whoscored_resolved"):
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
                is_captain                  BOOLEAN,
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
    for r in sofascore_rows:
        con.execute(
            insert_template.format(tbl="sofascore_resolved"),
            [r[c] for c in _RESOLVED_COLUMNS],
        )
    for r in fotmob_rows:
        con.execute(
            insert_template.format(tbl="fotmob_resolved"),
            [r[c] for c in _RESOLVED_COLUMNS],
        )
    for r in whoscored_rows:
        con.execute(
            insert_template.format(tbl="whoscored_resolved"),
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

    def test_fbref_wins_over_sofascore_same_canonical(self, duck_conn):
        """#693: same (match, player_id_canonical) in FBref + SofaScore →
        FBref wins on source_priority (1 < 2), even though SofaScore is fresher.
        Unlike ESPN, SofaScore HAS a real canonical so the dedup actually fires.
        """
        fbref = [_fbref(
            match_id_canonical="MX", player_id_canonical="fb_Z",
            player_name="Z", ingested="2026-05-01 12:00:00",
        )]
        sofa = [_sofascore(
            match_id_canonical="MX", player_id_canonical="fb_Z",
            is_captain=True, ingested="2026-05-08 12:00:00",  # newer, still loses
        )]
        _seed_resolved(duck_conn, fbref, [], sofa)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fbref", out
        assert out[0]["player_id_canonical"] == "fb_Z", out

    def test_sofascore_beats_espn(self, duck_conn):
        """#693: SofaScore (priority 2) outranks ESPN (priority 5) when both
        resolve to the same canonical (forward-looking — ESPN canonical is NULL
        today, so we force it here to lock the ordering)."""
        sofa = [_sofascore(
            match_id_canonical="ME", player_id_canonical="fb_Q",
            ingested="2026-05-01 12:00:00",
        )]
        espn = [_espn(match_id_canonical="ME", player_name="Q",
                      ingested="2026-05-08 12:00:00")]
        espn[0]["player_id_canonical"] = "fb_Q"
        _seed_resolved(duck_conn, [], espn, sofa)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "sofascore", out

    def test_priority_order_fbref_sofascore_fotmob(self, duck_conn):
        """#693: all four sources on the same canonical (match, player) →
        FBref(1) > SofaScore(2) > FotMob(3) > ESPN(5). FBref must win."""
        cid = "fb_W"
        fbref = [_fbref(match_id_canonical="MP", player_id_canonical=cid,
                        player_name="W")]
        sofa = [_sofascore(match_id_canonical="MP", player_id_canonical=cid)]
        fot = [_fotmob(match_id_canonical="MP", player_id_canonical=cid)]
        espn = [_espn(match_id_canonical="MP", player_name="W")]
        espn[0]["player_id_canonical"] = cid
        _seed_resolved(duck_conn, fbref, espn, sofa, fot)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fbref", out

    def test_fotmob_beats_espn(self, duck_conn):
        """FotMob(3) outranks ESPN(5) on a shared canonical."""
        cid = "fb_V"
        fot = [_fotmob(match_id_canonical="MV2", player_id_canonical=cid,
                       ingested="2026-05-01 12:00:00")]
        espn = [_espn(match_id_canonical="MV2", player_name="V",
                      ingested="2026-05-08 12:00:00")]
        espn[0]["player_id_canonical"] = cid
        _seed_resolved(duck_conn, [], espn, [], fot)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fotmob", out

    def test_fotmob_beats_whoscored(self, duck_conn):
        """FotMob(3) outranks WhoScored(4) on a shared canonical (#693)."""
        cid = "fb_U"
        fot = [_fotmob(match_id_canonical="MU", player_id_canonical=cid)]
        ws = [_whoscored(match_id_canonical="MU", player_id_canonical=cid)]
        _seed_resolved(duck_conn, [], [], None, fot, ws)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fotmob", out

    def test_whoscored_beats_espn(self, duck_conn):
        """WhoScored(4) outranks ESPN(5) on a shared canonical."""
        cid = "fb_T"
        ws = [_whoscored(match_id_canonical="MT", player_id_canonical=cid,
                         ingested="2026-05-01 12:00:00")]
        espn = [_espn(match_id_canonical="MT", player_name="T",
                      ingested="2026-05-08 12:00:00")]
        espn[0]["player_id_canonical"] = cid
        _seed_resolved(duck_conn, [], espn, None, None, ws)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "whoscored", out


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

    def test_sofascore_only_match_keeps_ss_rows(self, duck_conn):
        """#693: a match SofaScore covers but FBref/ESPN don't → SS rows survive
        with a resolved player_id_canonical (the value-add over ESPN's NULL)."""
        sofa = [
            _sofascore(match_id_canonical="M_SS", player_id_canonical="fb_a",
                       raw_player_id="111"),
            _sofascore(match_id_canonical="M_SS", player_id_canonical="fb_b",
                       raw_player_id="222"),
        ]
        _seed_resolved(duck_conn, [], [], sofa)
        out = _run_dedup(duck_conn)
        assert len(out) == 2, out
        assert all(r["lineup_source"] == "sofascore" for r in out)
        assert sorted(r["player_id_canonical"] for r in out) == ["fb_a", "fb_b"]

    def test_sofascore_orphan_players_kept_distinct(self, duck_conn):
        """Two SofaScore players with NULL canonical in the same match must NOT
        collapse — dedup key falls back to (source || team || native_id)."""
        # Distinct positions make the two rows distinguishable in the projection;
        # the dedup key still falls back to (source || team || native_id).
        sofa = [
            _sofascore(match_id_canonical="M_SS2", player_id_canonical=None,
                       raw_player_id="111", position="GK"),
            _sofascore(match_id_canonical="M_SS2", player_id_canonical=None,
                       raw_player_id="222", position="ST"),
        ]
        _seed_resolved(duck_conn, [], [], sofa)
        out = _run_dedup(duck_conn)
        assert len(out) == 2, out  # NOT collapsed by shared NULL canonical
        assert all(r["lineup_source"] == "sofascore" for r in out)
        assert sorted(r["position_canonical"] for r in out) == ["GK", "ST"]

    def test_sofascore_native_captain_survives(self, duck_conn):
        """#693: on an SS-only match the native is_captain is projected (the
        final SQL COALESCEs native first, then the #439 bridge)."""
        sofa = [_sofascore(match_id_canonical="M_SS3",
                           player_id_canonical="fb_cap", is_captain=True)]
        _seed_resolved(duck_conn, [], [], sofa)
        out = _run_dedup(duck_conn)
        assert len(out) == 1, out
        assert out[0]["is_captain"] is True, out


class TestProjection:
    """Output schema invariants."""

    def test_lineup_version_literal_is_v1(self, duck_conn):
        fbref = [_fbref(
            match_id_canonical="MV", player_id_canonical="fb_p", player_name="A",
        )]
        _seed_resolved(duck_conn, fbref, [])
        out = _run_dedup(duck_conn)
        assert all(r["lineup_version"] == "v1" for r in out)

    def test_lineup_source_enum_all_five(self, duck_conn):
        fbref = [_fbref(match_id_canonical="MS", player_id_canonical="fb_p",
                        player_name="A")]
        espn = [_espn(match_id_canonical="MS2", player_name="B")]
        sofa = [_sofascore(match_id_canonical="MS3", player_id_canonical="fb_c")]
        fot = [_fotmob(match_id_canonical="MS4", player_id_canonical="fb_d")]
        ws = [_whoscored(match_id_canonical="MS5", player_id_canonical="fb_e")]
        _seed_resolved(duck_conn, fbref, espn, sofa, fot, ws)
        out = _run_dedup(duck_conn)
        srcs = {r["lineup_source"] for r in out}
        assert srcs <= {"fbref", "espn", "sofascore", "fotmob", "whoscored"}, (
            f"unexpected source labels: {srcs}"
        )

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
    "iceberg.silver.sofascore_player_match_aggregate":
        "silver_sofascore_player_match_aggregate",
    "iceberg.silver.fotmob_lineup":          "silver_fotmob_lineup",
    "iceberg.silver.whoscored_lineup":       "silver_whoscored_lineup",
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
        "(source VARCHAR, source_id VARCHAR, canonical_id VARCHAR, "
        "league VARCHAR, season VARCHAR, confidence VARCHAR)"
    )
    duck_conn.execute(
        # #693: sofascore_resolved reads team_name/is_starter/position/
        # is_captain/_bronze_ingested_at on top of the #439 captain columns.
        "CREATE TABLE silver_sofascore_player_match_aggregate "
        "(match_id VARCHAR, player_id VARCHAR, team_name VARCHAR, "
        "is_starter BOOLEAN, position VARCHAR, is_captain BOOLEAN, "
        "_bronze_ingested_at TIMESTAMP, league VARCHAR, season VARCHAR)"
    )
    duck_conn.execute(
        # #693: FotMob lineup source (fotmob_resolved reads these columns).
        "CREATE TABLE silver_fotmob_lineup "
        "(match_id VARCHAR, player_id VARCHAR, player_name VARCHAR, "
        "team_name VARCHAR, is_home BOOLEAN, is_starter BOOLEAN, "
        "is_captain BOOLEAN, position VARCHAR, jersey_number INTEGER, "
        "_bronze_ingested_at TIMESTAMP, league VARCHAR, season VARCHAR)"
    )
    duck_conn.execute(
        # #693: WhoScored inferred lineup (whoscored_resolved reads these).
        "CREATE TABLE silver_whoscored_lineup "
        "(match_id VARCHAR, player_id VARCHAR, team_name VARCHAR, "
        "is_starter BOOLEAN, is_captain BOOLEAN, position VARCHAR, "
        "jersey_number INTEGER, _bronze_ingested_at TIMESTAMP, "
        "league VARCHAR, season VARCHAR)"
    )
    duck_conn.execute(
        "CREATE TABLE silver_xref_team (source VARCHAR, source_id VARCHAR, "
        "canonical_id VARCHAR, league VARCHAR, season VARCHAR, confidence VARCHAR)"
    )
    duck_conn.execute(
        "CREATE TABLE silver_xref_player (source VARCHAR, source_id VARCHAR, "
        "canonical_id VARCHAR, league VARCHAR, season VARCHAR, "
        "display_name VARCHAR, raw_team_name VARCHAR)"
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


class TestFbrefTeamCrossSeason:
    """#729: fbref_match_lineups.team carries SHORT names ('Tottenham') that
    xref_team only holds for the current season (2526); older seasons hold only
    the FULL name ('Tottenham Hotspur'). The FBref team JOIN must resolve
    season-agnostically (xref_team_dedup) — a season-keyed JOIN dropped 2967
    older-season rows to NULL team_id and broke the no_nulls(team_id) ERROR gate.
    """

    def _seed_lineup(self, con, *, team: str, season: str) -> None:
        con.execute(
            """
            INSERT INTO silver_fbref_match_lineups VALUES
              (?, ?, 'Some Player', 'p_729', TRUE, 'F', 7, ?, ?,
               TIMESTAMP '2026-02-01 06:00:00')
            """,
            [_FB_HEX, team, _LEAGUE, season],
        )

    def test_short_name_in_other_season_still_resolves(self, bridge_conn):
        """Lineup 'Tottenham' in 1617; xref_team has the short spelling only in
        2526 and the full spelling in 1617. Season-agnostic dedup resolves it."""
        self._seed_lineup(bridge_conn, team="Tottenham", season="1617")
        bridge_conn.execute(
            """
            INSERT INTO silver_xref_team VALUES
              ('fbref', 'Tottenham',         'tottenham_hotspur', ?, '2526', 'name_alias'),
              ('fbref', 'Tottenham Hotspur', 'tottenham_hotspur', ?, '1617', 'name_alias')
            """,
            [_LEAGUE, _LEAGUE],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fbref", out
        assert out[0]["team_id"] == "tottenham_hotspur", (
            f"#729 regression — short name in a non-matching season dropped to "
            f"NULL team_id: {out}"
        )

    def test_resolves_when_only_other_season_spelling_exists(self, bridge_conn):
        """Pure #729: xref_team has ONLY ('Tottenham', 2526); lineup is 1617.
        A season-keyed JOIN would miss entirely; the dedup resolves it."""
        self._seed_lineup(bridge_conn, team="Tottenham", season="1617")
        bridge_conn.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Tottenham', 'tottenham_hotspur', ?, '2526', 'name_alias')",
            [_LEAGUE],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["team_id"] == "tottenham_hotspur", out

    def test_orphan_short_name_still_excluded(self, bridge_conn):
        """The season-agnostic dedup must STILL honour the #506 orphan filter:
        an orphan-confidence xref_team row never leaks as a resolved team_id."""
        self._seed_lineup(bridge_conn, team="Tottenham", season="1617")
        bridge_conn.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Tottenham', 'fb_tottenham', ?, '2526', 'orphan')",
            [_LEAGUE],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["team_id"] is None, (
            f"orphan xref_team leaked through the season-agnostic dedup: {out}"
        )


class TestCaptainEnrichment:
    """#439: is_captain is sourced from SofaScore /lineups via xref_match +
    xref_player. A FBref lineup row whose canonical (match, player) matches a
    SofaScore entry inherits its captaincy; absent coverage stays NULL.
    """

    @staticmethod
    def _seed_fbref_side(con) -> None:
        con.execute(
            """
            INSERT INTO silver_fbref_match_lineups VALUES
              (?, 'Liverpool', 'Mo Salah', 'p99', TRUE, 'F', 11, ?, ?,
               TIMESTAMP '2026-02-01 06:00:00')
            """,
            [_FB_HEX, _LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) "
            "VALUES ('fbref', 'p99', 'fb_p99', ?, ?)",
            [_LEAGUE, _SEASON],
        )

    def _seed_sofascore_side(self, con, *, is_captain) -> None:
        # SofaScore game 'ss1' bridges to the SAME hex canonical; player 'ssp'
        # resolves to the SAME canonical 'fb_p99' as the FBref lineup row.
        con.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('sofascore', 'ss1', ?, ?, ?, 'date_team_match')",
            [_FB_HEX, _LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) "
            "VALUES ('sofascore', 'ssp', 'fb_p99', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_sofascore_player_match_aggregate "
            "(match_id, player_id, league, season, is_captain) "
            "VALUES ('ss1', 'ssp', ?, ?, ?)",
            [_LEAGUE, _SEASON, is_captain],
        )

    def test_captain_true_enriches_fbref_row(self, bridge_conn):
        self._seed_fbref_side(bridge_conn)
        self._seed_sofascore_side(bridge_conn, is_captain=True)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["player_id"] == "fb_p99", out
        assert out[0]["is_captain"] is True, out

    def test_non_captain_resolves_to_false(self, bridge_conn):
        """A resolved SofaScore non-captain → is_captain False (known), not NULL."""
        self._seed_fbref_side(bridge_conn)
        self._seed_sofascore_side(bridge_conn, is_captain=False)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["is_captain"] is False, out

    def test_no_sofascore_coverage_leaves_null(self, bridge_conn):
        """FBref player absent from SofaScore /lineups → is_captain NULL."""
        self._seed_fbref_side(bridge_conn)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["is_captain"] is None, out


class TestEspnPlayerResolution:
    """#692: ESPN lineup player_id resolves via silver.xref_player.

    The espn_resolved CTE LEFT JOINs xref_player (source='espn') keyed on
    (display_name, raw_team_name, league, season) — no native ESPN player_id
    exists, so the resolver matches by name+team and surfaces the canonical_id.
    """

    def test_espn_player_resolves_to_canonical(self, bridge_conn):
        """A matching xref_player ESPN row → player_id carries its canonical."""
        _seed_espn_corpus(bridge_conn)
        # Canonical xref_player row for the seeded ESPN lineup player.
        # Column order: source, source_id, canonical_id, league, season,
        # display_name, raw_team_name.
        bridge_conn.execute(
            "INSERT INTO silver_xref_player VALUES "
            "('espn', 'ESPN Player A|Liverpool', 'fb_saka', ?, ?, "
            "'ESPN Player A', 'Liverpool')",
            [_LEAGUE, _SEASON],
        )
        # Schedule row so the bridge maps the espn match → FBref hex.
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["player_id"] == "fb_saka", out
        assert out[0]["lineup_source"] == "espn", out

    def test_espn_player_unresolved_stays_null(self, bridge_conn):
        """No matching xref_player row → player_id NULL (unresolved ESPN)."""
        _seed_espn_corpus(bridge_conn)
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["player_id"] is None, out

    def test_espn_xref_player_join_does_not_fan_out(self, bridge_conn):
        """A wrong-season xref_player row must NOT match (league+season
        predicate present → no fan-out, footgun #205)."""
        _seed_espn_corpus(bridge_conn)
        # xref_player row for a DIFFERENT season → must not join.
        bridge_conn.execute(
            "INSERT INTO silver_xref_player VALUES "
            "('espn', 'ESPN Player A|Liverpool', 'fb_saka', ?, '2425', "
            "'ESPN Player A', 'Liverpool')",
            [_LEAGUE],
        )
        bridge_conn.execute(
            _SCHEDULE_ROW,
            [_LEAGUE, _SEASON, _GAME, "Liverpool", "2026-02-01 06:00:00"],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["player_id"] is None, out


class TestSofaScoreSource:
    """#693: SofaScore is a FULL lineup source now, not just the #439 captain
    overlay. A SofaScore-covered match contributes rows carrying a resolved
    player_id (the value-add over ESPN's NULL) plus native is_starter /
    is_captain / position. Exercised through the real fct_lineup.sql.
    """

    def _seed(self, con, *, is_starter, is_captain) -> None:
        con.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('sofascore', 'ss9', 'ss_canon_match', ?, ?, 'date_team_match')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('sofascore', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) VALUES "
            "('sofascore', 'ssp9', 'fb_ss9', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_sofascore_player_match_aggregate "
            "(match_id, player_id, team_name, is_starter, position, is_captain, "
            "league, season) "
            "VALUES ('ss9', 'ssp9', 'Liverpool', ?, 'F', ?, ?, ?)",
            [is_starter, is_captain, _LEAGUE, _SEASON],
        )

    def test_sofascore_only_row_resolves_with_real_ids(self, bridge_conn):
        self._seed(bridge_conn, is_starter=True, is_captain=True)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        r = out[0]
        assert r["lineup_source"] == "sofascore", r
        assert r["match_id"] == "ss_canon_match", r
        assert r["player_id"] == "fb_ss9", r          # resolved, NOT NULL (vs ESPN)
        assert r["team_id"] == "liverpool", r
        assert r["is_starter"] is True, r
        assert r["is_captain"] is True, r             # native captain
        assert r["jersey_number"] is None, r          # SofaScore has no jersey

    def test_sofascore_substitute_is_not_starter(self, bridge_conn):
        self._seed(bridge_conn, is_starter=False, is_captain=False)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["is_starter"] is False, out
        assert out[0]["is_captain"] is False, out

    def test_fbref_wins_over_sofascore_full_sql(self, bridge_conn):
        """When FBref and SofaScore both cover the same canonical (match, player)
        the FBref row wins dedup; SofaScore still supplies is_captain via the
        bridge (COALESCE native, bridge)."""
        # FBref side: player p99 -> fb_p99 at the FBref hex match.
        bridge_conn.execute(
            """
            INSERT INTO silver_fbref_match_lineups VALUES
              (?, 'Liverpool', 'Mo Salah', 'p99', TRUE, 'F', 11, ?, ?,
               TIMESTAMP '2026-02-01 06:00:00')
            """,
            [_FB_HEX, _LEAGUE, _SEASON],
        )
        bridge_conn.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        bridge_conn.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) "
            "VALUES ('fbref', 'p99', 'fb_p99', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        # SofaScore side: match ss1 -> SAME hex; player ssp -> SAME fb_p99.
        bridge_conn.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('sofascore', 'ss1', ?, ?, ?, 'date_team_match')",
            [_FB_HEX, _LEAGUE, _SEASON],
        )
        bridge_conn.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) "
            "VALUES ('sofascore', 'ssp', 'fb_p99', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        bridge_conn.execute(
            "INSERT INTO silver_sofascore_player_match_aggregate "
            "(match_id, player_id, team_name, is_starter, position, is_captain, "
            "league, season) "
            "VALUES ('ss1', 'ssp', 'Liverpool', TRUE, 'F', TRUE, ?, ?)",
            [_LEAGUE, _SEASON],
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out                       # deduped, not 2 rows
        assert out[0]["lineup_source"] == "fbref", out  # FBref priority wins
        assert out[0]["player_id"] == "fb_p99", out
        assert out[0]["jersey_number"] == 11, out       # FBref schema richer
        assert out[0]["is_captain"] is True, out        # from SofaScore (native→bridge)


class TestFotmobSource:
    """#693: FotMob as a full lineup source. A FotMob-covered match contributes
    rows with a resolved player_id, is_starter, and jersey_number (shirtNumber);
    is_captain is NULL (lineup_json has no captaincy). Real fct_lineup.sql."""

    def _seed(self, con, *, is_starter, jersey) -> None:
        con.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('fotmob', 'fm9', 'fm_canon_match', ?, ?, 'date_team_match')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fotmob', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) VALUES "
            "('fotmob', 'fmp9', 'fb_fm9', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_fotmob_lineup "
            "(match_id, player_id, player_name, team_name, is_home, is_starter, "
            "is_captain, position, jersey_number, league, season) "
            "VALUES ('fm9', 'fmp9', 'Mo Salah', 'Liverpool', true, ?, NULL, "
            "'11', ?, ?, ?)",
            [is_starter, jersey, _LEAGUE, _SEASON],
        )

    def test_fotmob_only_row_resolves_with_jersey(self, bridge_conn):
        self._seed(bridge_conn, is_starter=True, jersey=11)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        r = out[0]
        assert r["lineup_source"] == "fotmob", r
        assert r["match_id"] == "fm_canon_match", r
        assert r["player_id"] == "fb_fm9", r            # resolved
        assert r["team_id"] == "liverpool", r
        assert r["is_starter"] is True, r
        assert r["jersey_number"] == 11, r              # FotMob shirtNumber
        assert r["is_captain"] is None, r               # no captaincy in lineup_json

    def test_fotmob_sub_is_not_starter(self, bridge_conn):
        self._seed(bridge_conn, is_starter=False, jersey=30)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["is_starter"] is False, out


class TestWhoscoredSource:
    """#693: WhoScored as a (thin) full lineup source — real player_id +
    inferred is_starter; position/captain/jersey NULL. Real fct_lineup.sql."""

    def _seed(self, con, *, is_starter) -> None:
        con.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('whoscored', 'ws9', 'ws_canon_match', ?, ?, 'date_team_match')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('whoscored', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) VALUES "
            "('whoscored', 'wsp9', 'fb_ws9', ?, ?)",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_whoscored_lineup "
            "(match_id, player_id, team_name, is_starter, is_captain, position, "
            "jersey_number, league, season) "
            "VALUES ('ws9', 'wsp9', 'Liverpool', ?, NULL, NULL, NULL, ?, ?)",
            [is_starter, _LEAGUE, _SEASON],
        )

    def test_whoscored_only_row_resolves(self, bridge_conn):
        self._seed(bridge_conn, is_starter=True)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        r = out[0]
        assert r["lineup_source"] == "whoscored", r
        assert r["match_id"] == "ws_canon_match", r
        assert r["player_id"] == "fb_ws9", r           # resolved (better than ESPN NULL)
        assert r["team_id"] == "liverpool", r
        assert r["is_starter"] is True, r
        assert r["position"] is None, r                 # not derivable from events
        assert r["is_captain"] is None, r
        assert r["jersey_number"] is None, r

    def test_whoscored_subbed_on_is_not_starter(self, bridge_conn):
        self._seed(bridge_conn, is_starter=False)
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["is_starter"] is False, out


class TestFbrefCoveredDuplicateDrop:
    """#819: a non-FBref row in a match FBref ALREADY covers, whose player did
    NOT resolve to a real 'fb_' canonical, is a pure duplicate of the
    authoritative FBref lineup — it survives dedup only because its player key
    (NULL / orphan) cannot collapse against the FBref twin, and it inflates the
    player_id NULL/orphan share. fct_lineup.sql drops these via the
    fbref_covered_matches filter. Resolved ('fb_…') rows and rows in matches
    FBref does NOT cover are kept (no over-drop).
    """

    @staticmethod
    def _seed_fbref_cover(con, *, player_id="p1", canonical="fb_p1") -> None:
        """One FBref lineup row at the FBref hex match → that match is covered."""
        con.execute(
            """
            INSERT INTO silver_fbref_match_lineups VALUES
              (?, 'Liverpool', 'FB Player', ?, TRUE, 'F', 9, ?, ?,
               TIMESTAMP '2026-02-01 06:00:00')
            """,
            [_FB_HEX, player_id, _LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fbref', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_player "
            "(source, source_id, canonical_id, league, season) "
            "VALUES ('fbref', ?, ?, ?, ?)",
            [player_id, canonical, _LEAGUE, _SEASON],
        )

    @staticmethod
    def _seed_fotmob_at_match(
        con, *, fm_game, canonical_match, fm_player_id, xref_canonical=None
    ) -> None:
        """A FotMob lineup row bridged (via xref_match) to ``canonical_match``.

        If ``xref_canonical`` is None the player has NO xref_player row →
        player_id_canonical resolves to NULL (the unresolved case).
        """
        con.execute(
            "INSERT INTO silver_xref_match VALUES "
            "('fotmob', ?, ?, ?, ?, 'date_team_match')",
            [fm_game, canonical_match, _LEAGUE, _SEASON],
        )
        con.execute(
            "INSERT INTO silver_xref_team VALUES "
            "('fotmob', 'Liverpool', 'liverpool', ?, ?, 'name_alias')",
            [_LEAGUE, _SEASON],
        )
        if xref_canonical is not None:
            con.execute(
                "INSERT INTO silver_xref_player "
                "(source, source_id, canonical_id, league, season) "
                "VALUES ('fotmob', ?, ?, ?, ?)",
                [fm_player_id, xref_canonical, _LEAGUE, _SEASON],
            )
        con.execute(
            "INSERT INTO silver_fotmob_lineup "
            "(match_id, player_id, player_name, team_name, is_home, is_starter, "
            "is_captain, position, jersey_number, league, season) "
            "VALUES (?, ?, 'Sub Player', 'Liverpool', true, FALSE, NULL, "
            "'30', 30, ?, ?)",
            [fm_game, fm_player_id, _LEAGUE, _SEASON],
        )

    def test_unresolved_fotmob_in_fbref_covered_match_dropped(self, bridge_conn):
        """The core #819 case: FBref covers the match; an UNRESOLVED FotMob row
        for the SAME canonical match is dropped, leaving only the FBref row."""
        self._seed_fbref_cover(bridge_conn)
        self._seed_fotmob_at_match(
            bridge_conn, fm_game="fm1", canonical_match=_FB_HEX,
            fm_player_id="fmUNRES", xref_canonical=None,
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fbref", out
        assert out[0]["player_id"] == "fb_p1", out

    def test_unresolved_fotmob_in_uncovered_match_kept(self, bridge_conn):
        """No FBref lineup for the match → not covered → the unresolved FotMob
        row survives with NULL player_id (the filter must not over-drop)."""
        self._seed_fotmob_at_match(
            bridge_conn, fm_game="fm2", canonical_match="fm_only_match",
            fm_player_id="fmUNRES2", xref_canonical=None,
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 1, out
        assert out[0]["lineup_source"] == "fotmob", out
        assert out[0]["match_id"] == "fm_only_match", out
        assert out[0]["player_id"] is None, out

    def test_resolved_fotmob_in_covered_match_kept(self, bridge_conn):
        """A FotMob row in a FBref-covered match whose player RESOLVES to a
        'fb_' canonical that the FBref lineup does not carry must survive (it
        adds real coverage; only unresolved duplicates are dropped)."""
        self._seed_fbref_cover(bridge_conn, player_id="p1", canonical="fb_p1")
        self._seed_fotmob_at_match(
            bridge_conn, fm_game="fm3", canonical_match=_FB_HEX,
            fm_player_id="fmRES", xref_canonical="fb_other",
        )
        out = _run_lineup_gold(bridge_conn)
        assert len(out) == 2, out
        by_src = {r["lineup_source"]: r for r in out}
        assert by_src["fbref"]["player_id"] == "fb_p1", out
        assert by_src["fotmob"]["player_id"] == "fb_other", out


class TestSqlInvariants:
    """Lock the SQL-text invariants that this behavioural harness CANNOT see."""

    def _sql(self) -> str:
        return SQL_PATH.read_text(encoding="utf-8")

    def test_source_priority_literals_in_sql(self):
        """The source_priority literals are the load-bearing dedup constants:
        FBref=1, SofaScore=2, ESPN=5 (#693). Strip block comments to avoid
        commentary false-positives."""
        sql = self._sql()
        non_comment = "\n".join(
            line for line in sql.splitlines()
            if not line.lstrip().startswith("--")
        )
        assert re.search(
            r"\b1\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "fbref_resolved CTE must emit `1 AS source_priority`"
        assert re.search(
            r"\b2\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "sofascore_resolved CTE must emit `2 AS source_priority`"
        assert re.search(
            r"\b3\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "fotmob_resolved CTE must emit `3 AS source_priority`"
        assert re.search(
            r"\b4\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "whoscored_resolved CTE must emit `4 AS source_priority`"
        assert re.search(
            r"\b5\b\s+AS\s+source_priority", non_comment, re.IGNORECASE
        ), "espn_resolved CTE must emit `5 AS source_priority` (#693 tail)"

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

    def test_lineup_source_literals_all_five(self):
        """fbref / espn / sofascore / fotmob / whoscored lineup_source CTEs (#693)."""
        sql = self._sql()
        for src in ("fbref", "espn", "sofascore", "fotmob", "whoscored"):
            assert re.search(
                rf"'{src}'\s+AS\s+lineup_source", sql, re.IGNORECASE
            ), f"missing `'{src}' AS lineup_source`"

    def test_espn_branch_resolves_player_via_xref_player(self):
        """#692: espn_resolved must LEFT JOIN xref_player (source='espn') keyed
        on display_name + raw_team_name, replacing the hardcoded NULL canonical.
        """
        normalised = re.sub(r"\s+", " ", self._sql())
        assert re.search(
            r"xref_player\s+\w+\s+ON\s+\w+\.source\s*=\s*'espn'",
            normalised, re.IGNORECASE,
        ), "espn_resolved must LEFT JOIN silver.xref_player on source='espn'"
        # The JOIN keys on the ESPN identity (name + team), not a native id.
        assert re.search(r"\.display_name\s*=", normalised, re.IGNORECASE), (
            "ESPN xref_player JOIN must key on display_name"
        )
        assert re.search(r"\.raw_team_name\s*=", normalised, re.IGNORECASE), (
            "ESPN xref_player JOIN must key on raw_team_name"
        )
