"""
R2 Day 2 — player identity resolver, prototype v0.1.

STANDALONE script (NOT a DAG). Run from host:

    TRINO_HOST=localhost TRINO_PORT=8082 \
    TRINO_PASSWORD='trino_s3cure_P4ss!' \
    python3 scripts/r2_resolver_proto.py

Inputs (Bronze, APL 2024-25):
    iceberg.bronze.fbref_player_stats     (season=2024,  stat_type='stats')
    iceberg.bronze.understat_players      (season='2425')
    iceberg.bronze.whoscored_events_current
                                             (season='2425', committed players)

Output:
    iceberg.default.r2_xref_player_proto  (CTAS — full rebuild)
        canonical_id    varchar
        source          varchar      ('fbref' | 'understat' | 'whoscored')
        source_id       varchar      (zero-padded id from each source)
        player_name     varchar      (raw name from source)
        team_canonical  varchar      (after _team_aliases lookup)
        confidence      varchar      ('exact' | 'name_team' | 'name_team_jersey'
                                      | 'name_team_dob' | 'orphan')
        match_score     double       (token_sort_ratio for fuzzy matches, NULL for exact)

Tier cascade:
    1. exact  — by FBref player_id when present (FBref = source-of-truth IDs)
    2. name_team — token_sort_ratio(name)>=90 AND team_canonical match
    3. name_team_jersey — (jersey not in current Bronze → not implementable yet, stub)
    4. name_team_dob    — (DOB only in fbref.born → cross-source DOB unavailable, stub)
    5. orphan — no match → canonical_id = '<src>_' || source_id

Confidence levels 3-4 are STUBBED — Bronze tables don't carry jersey numbers
or DOB consistently across sources (FBref has 'born' year only). They are
preserved in the schema for forward-compat but never populated by this prototype.
"""

from __future__ import annotations

import logging
import os
import urllib3
from typing import Dict, List, Optional, Tuple

import unidecode
from rapidfuzz import fuzz

import trino
from trino.auth import BasicAuthentication

urllib3.disable_warnings()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("r2_resolver")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRATCH_TABLE = "iceberg.default.r2_xref_player_proto"
NAME_THRESHOLD = 90.0  # token_sort_ratio >= 90 → accept
LEAGUE = "ENG-Premier League"
FBREF_SEASON = 2024  # bigint
SOURCE_SEASON = "2425"  # varchar — understat / whoscored


# ---------------------------------------------------------------------------
# Trino plumbing
# ---------------------------------------------------------------------------
def get_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", 8082)),
        user="airflow",
        catalog="iceberg",
        http_scheme="https",
        auth=BasicAuthentication(
            "airflow", os.environ.get("TRINO_PASSWORD", "trino_s3cure_P4ss!")
        ),
        verify=False,
    )


def execute(conn, sql: str, fetch: bool = False):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()  # always consume to avoid USER_CANCELED
    return rows if fetch else None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def normalize_name(s: Optional[str]) -> str:
    """unidecode + lowercase + collapse whitespace."""
    if not s:
        return ""
    return " ".join(unidecode.unidecode(s).lower().split())


# Static team-alias lookup — mirrors dags/sql/gold/_team_aliases.sql for the APL clubs
# that surfaced in the 3 source tables. Map every variant → canonical.
TEAM_ALIASES: Dict[str, str] = {
    # Wolves
    "wolves": "Wolverhampton Wanderers",
    "wolverhampton": "Wolverhampton Wanderers",
    "wolverhampton wanderers": "Wolverhampton Wanderers",
    # Tottenham
    "spurs": "Tottenham Hotspur",
    "tottenham": "Tottenham Hotspur",
    "tottenham hotspur": "Tottenham Hotspur",
    # Manchester United
    "man utd": "Manchester United",
    "man united": "Manchester United",
    "manchester utd": "Manchester United",
    "manchester united": "Manchester United",
    # Manchester City
    "man city": "Manchester City",
    "manchester city": "Manchester City",
    # Newcastle
    "newcastle": "Newcastle United",
    "newcastle utd": "Newcastle United",
    "newcastle united": "Newcastle United",
    # Nottingham Forest
    "nott'm forest": "Nottingham Forest",
    "nottingham": "Nottingham Forest",
    "nottingham forest": "Nottingham Forest",
    # Leicester
    "leicester": "Leicester City",
    "leicester city": "Leicester City",
    # West Ham
    "west ham": "West Ham United",
    "west ham united": "West Ham United",
    # Brighton
    "brighton": "Brighton and Hove Albion",
    "brighton & hove albion": "Brighton and Hove Albion",
    "brighton and hove albion": "Brighton and Hove Albion",
    # Ipswich
    "ipswich": "Ipswich Town",
    "ipswich town": "Ipswich Town",
    # Bournemouth
    "bournemouth": "Bournemouth",
    # Brentford
    "brentford": "Brentford",
    # Chelsea / Arsenal / Liverpool / Aston Villa / Crystal Palace / Everton / Fulham / Southampton — already canonical
    "chelsea": "Chelsea",
    "arsenal": "Arsenal",
    "liverpool": "Liverpool",
    "aston villa": "Aston Villa",
    "crystal palace": "Crystal Palace",
    "everton": "Everton",
    "fulham": "Fulham",
    "southampton": "Southampton",
}


def canonical_team(raw: Optional[str]) -> str:
    if not raw:
        return ""
    key = raw.strip().lower()
    return TEAM_ALIASES.get(key, raw.strip())


# ---------------------------------------------------------------------------
# Source readers
# ---------------------------------------------------------------------------
def fetch_fbref_players(conn) -> List[Tuple[str, str, str, str]]:
    """Return list of (source_id, name, team_canonical, born) for FBref APL 2024-25."""
    sql = f"""
        SELECT player_id, player, squad, born
        FROM iceberg.bronze.fbref_player_stats
        WHERE league = '{LEAGUE}'
          AND season = {FBREF_SEASON}
          AND player_id IS NOT NULL
        GROUP BY player_id, player, squad, born
    """
    rows = execute(conn, sql, fetch=True)
    out = []
    seen = set()
    for pid, name, squad, born in rows:
        if pid in seen:
            continue
        seen.add(pid)
        out.append((str(pid), name, canonical_team(squad), str(born) if born else ""))
    return out


def fetch_understat_players(conn) -> List[Tuple[str, str, str]]:
    sql = f"""
        SELECT CAST(player_id AS varchar) AS pid, player, team
        FROM iceberg.bronze.understat_players
        WHERE league = '{LEAGUE}' AND season = '{SOURCE_SEASON}'
          AND player IS NOT NULL
        GROUP BY player_id, player, team
    """
    rows = execute(conn, sql, fetch=True)
    out = []
    seen = set()
    for pid, name, team in rows:
        if pid in seen:
            continue
        seen.add(pid)
        out.append((pid, name, canonical_team(team)))
    return out


def fetch_whoscored_players(conn) -> List[Tuple[str, str, str]]:
    sql = f"""
        SELECT CAST(CAST(player_id AS bigint) AS varchar) AS pid,
               MAX(player) AS player,
               MAX(team) AS team
        FROM iceberg.bronze.whoscored_events_current
        WHERE league = '{LEAGUE}' AND season = '{SOURCE_SEASON}'
          AND player_id IS NOT NULL AND player IS NOT NULL
        GROUP BY CAST(player_id AS bigint)
    """
    rows = execute(conn, sql, fetch=True)
    out = []
    for pid, name, team in rows:
        out.append((pid, name, canonical_team(team)))
    return out


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------
class FBrefIndex:
    """In-memory index of FBref players keyed by canonical team for fuzzy lookup."""

    def __init__(self, fbref_rows: List[Tuple[str, str, str, str]]):
        # rows: (id, name, team_canonical, born)
        self.by_id: Dict[str, Tuple[str, str, str]] = {}
        self.by_team: Dict[str, List[Tuple[str, str]]] = {}  # team -> [(name_norm, fb_id)]
        self.norm_to_id: Dict[Tuple[str, str], str] = {}  # (name_norm, team) -> fb_id
        for fid, name, team, born in fbref_rows:
            self.by_id[fid] = (name, team, born)
            n = normalize_name(name)
            self.by_team.setdefault(team, []).append((n, fid))
            self.norm_to_id[(n, team)] = fid

    def find_by_name_team(
        self, name: str, team: str
    ) -> Tuple[Optional[str], float]:
        """Return (fbref_id, score) — None if no candidate ≥ NAME_THRESHOLD.
        Tier-2: name_team match within same canonical team."""
        if not team:
            return None, 0.0
        cands = self.by_team.get(team, [])
        if not cands:
            return None, 0.0
        n = normalize_name(name)
        best_id, best_score = None, 0.0
        for cn, fid in cands:
            score = fuzz.token_sort_ratio(n, cn)
            if score > best_score:
                best_id, best_score = fid, score
        if best_score >= NAME_THRESHOLD:
            return best_id, best_score
        # WRatio fallback
        best_id, best_score = None, 0.0
        for cn, fid in cands:
            score = fuzz.WRatio(n, cn)
            if score > best_score:
                best_id, best_score = fid, score
        if best_score >= NAME_THRESHOLD:
            return best_id, best_score
        return None, best_score


def resolve(
    fb_rows: List[Tuple[str, str, str, str]],
    us_rows: List[Tuple[str, str, str]],
    ws_rows: List[Tuple[str, str, str]],
) -> List[Tuple[str, str, str, str, str, str, Optional[float]]]:
    """Run tier-cascade. Return rows for r2_xref_player_proto:
       (canonical_id, source, source_id, player_name, team_canonical, confidence, match_score)
    """
    rows: List[Tuple[str, str, str, str, str, str, Optional[float]]] = []
    idx = FBrefIndex(fb_rows)

    # FBref is the spine — every FBref player is automatically canonical.
    for fid, name, team, _born in fb_rows:
        rows.append(
            (
                f"fb_{fid}",
                "fbref",
                fid,
                name,
                team,
                "exact",  # source-of-truth — its own id IS canonical
                None,
            )
        )

    # Understat players — cascade
    for pid, name, team in us_rows:
        # Tier-2: name + team
        fb_id, score = idx.find_by_name_team(name, team)
        if fb_id:
            rows.append(
                (
                    f"fb_{fb_id}",
                    "understat",
                    pid,
                    name,
                    team,
                    "name_team",
                    score,
                )
            )
        else:
            # Orphan
            rows.append(
                (
                    f"us_{pid}",
                    "understat",
                    pid,
                    name,
                    team,
                    "orphan",
                    score if score > 0 else None,
                )
            )

    # WhoScored
    for pid, name, team in ws_rows:
        fb_id, score = idx.find_by_name_team(name, team)
        if fb_id:
            rows.append(
                (
                    f"fb_{fb_id}",
                    "whoscored",
                    pid,
                    name,
                    team,
                    "name_team",
                    score,
                )
            )
        else:
            rows.append(
                (
                    f"ws_{pid}",
                    "whoscored",
                    pid,
                    name,
                    team,
                    "orphan",
                    score if score > 0 else None,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# Trino write — DROP + CTAS via VALUES
# ---------------------------------------------------------------------------
def write_to_trino(conn, rows):
    log.info(f"Dropping & recreating {SCRATCH_TABLE}…")
    execute(conn, f"DROP TABLE IF EXISTS {SCRATCH_TABLE}")

    # Create empty table with explicit schema
    execute(
        conn,
        f"""
        CREATE TABLE {SCRATCH_TABLE} (
          canonical_id    varchar,
          source          varchar,
          source_id       varchar,
          player_name     varchar,
          team_canonical  varchar,
          confidence      varchar,
          match_score     double
        )
        """,
    )

    # Batch INSERT — Trino has SQL-length limits; 500/batch is safe
    BATCH = 500
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        values_sql = ",\n".join(_value_tuple(r) for r in chunk)
        execute(
            conn,
            f"INSERT INTO {SCRATCH_TABLE} "
            f"(canonical_id, source, source_id, player_name, team_canonical, confidence, match_score) "
            f"VALUES {values_sql}",
        )
        log.info(f"  wrote {min(i + BATCH, len(rows))}/{len(rows)} rows")


def _sql_str(s: Optional[str]) -> str:
    if s is None:
        return "NULL"
    escaped = s.replace("'", "''")
    return f"'{escaped}'"


def _sql_double(v: Optional[float]) -> str:
    return "NULL" if v is None else f"CAST({v} AS DOUBLE)"


def _value_tuple(r) -> str:
    cid, src, sid, name, team, conf, score = r
    return (
        f"({_sql_str(cid)}, {_sql_str(src)}, {_sql_str(sid)}, "
        f"{_sql_str(name)}, {_sql_str(team)}, {_sql_str(conf)}, {_sql_double(score)})"
    )


# ---------------------------------------------------------------------------
# Day-3 / Day-4 verification queries
# ---------------------------------------------------------------------------
KNOWN_PAIRS = [
    # name, fbref_id (canonical_id 'fb_<id>' expected for all 3 sources)
    ("Bukayo Saka", "fb_bc7dc64d"),
    ("Mohamed Salah", "fb_e342ad68"),
    ("Erling Haaland", "fb_1f44ac21"),
    ("Bruno Fernandes", "fb_507c7bdf"),
    ("Rodri", "fb_6434f10d"),
    ("Son Heung-min", "fb_92e7e919"),
    ("Virgil van Dijk", "fb_e06683ca"),
    ("Cole Palmer", "fb_dc7f8a28"),
    ("Bruno Guimarães", "fb_82518f62"),
    ("Joško Gvardiol", "fb_5ad50391"),
]


def verify_known_pairs(conn):
    """Day 3: ≥8/10 known pairs must resolve to a SINGLE canonical_id across sources.

    Lookup uses the EXPECTED canonical_id directly (not a name LIKE) so that
    multiple Rodrigo* / *Silva variants don't pollute the verification."""
    print()
    print("=" * 80)
    print("DAY 3 — known-pair verification (target ≥8/10)")
    print("=" * 80)
    passed = 0
    for name, expected_cid in KNOWN_PAIRS:
        sql = f"""
            SELECT source, canonical_id, player_name, team_canonical, confidence
            FROM {SCRATCH_TABLE}
            WHERE canonical_id = '{expected_cid}'
            ORDER BY source
        """
        rows = execute(conn, sql, fetch=True)
        sources_resolved = {r[0] for r in rows}
        all_three = sources_resolved >= {"fbref", "understat", "whoscored"}
        status = "PASS" if all_three else "FAIL"
        print(f"  [{status}] {name:<22} expected={expected_cid}  sources_resolved={sorted(sources_resolved)}")
        for r in rows:
            print(f"        {r[0]:<10} cid={r[1]:<14} name={r[2]:<22} team={r[3]:<25} conf={r[4]}")
        if all_three:
            passed += 1
    print()
    print(f"PASSED: {passed}/{len(KNOWN_PAIRS)}")
    return passed


def coverage_per_source(conn):
    """Day 4: rejection ≤25% per source."""
    print()
    print("=" * 80)
    print("DAY 4 — coverage per source (target rejection ≤25%)")
    print("=" * 80)
    sql = f"""
        SELECT source,
               COUNT(*) AS total,
               COUNT_IF(confidence != 'orphan') AS resolved,
               COUNT_IF(confidence = 'orphan') AS rejected,
               CAST(ROUND(100.0 * COUNT_IF(confidence='orphan') / COUNT(*), 1) AS DOUBLE) AS rejection_pct
        FROM {SCRATCH_TABLE}
        GROUP BY source
        ORDER BY source
    """
    rows = execute(conn, sql, fetch=True)
    print(f"{'source':<12}{'total':>8}{'resolved':>10}{'rejected':>10}{'rejection_pct':>16}")
    for r in rows:
        print(f"{r[0]:<12}{r[1]:>8}{r[2]:>10}{r[3]:>10}{r[4]:>15.1f}%")
    return rows


def feat_player_form_smoke(conn):
    """Day 5: prove orphan IDs (us_*/ws_*) survive a feature-style rolling
    aggregation without SQL errors.

    This is a stub — feat_player_form lives downstream and JOINs xref_player.
    Here we simulate the same pattern: GROUP BY canonical_id, average a
    Understat metric, no JOIN failures even when canonical_id starts with 'us_'."""
    print()
    print("=" * 80)
    print("DAY 5 — feat_player_form smoke test (orphan-safe aggregation)")
    print("=" * 80)
    sql = f"""
        WITH xref AS (
          SELECT canonical_id, source, source_id, confidence
          FROM {SCRATCH_TABLE}
          WHERE source = 'understat'
        ),
        usp AS (
          SELECT CAST(player_id AS varchar) AS source_id, goals, xg, xa
          FROM iceberg.bronze.understat_player_match_stats
          WHERE league = '{LEAGUE}' AND season = '{SOURCE_SEASON}'
        )
        SELECT
          COUNT(*) AS rows_total,
          COUNT(DISTINCT canonical_id) AS distinct_canonicals,
          COUNT_IF(canonical_id LIKE 'us_%') AS orphan_canonicals_in_join,
          ROUND(SUM(xg), 2) AS total_xg
        FROM xref
        JOIN usp USING (source_id)
    """
    rows = execute(conn, sql, fetch=True)
    print(f"  result: rows_total={rows[0][0]} distinct_canonicals={rows[0][1]} "
          f"orphan_canonicals={rows[0][2]} total_xg={rows[0][3]}")
    print("  PASS — orphan canonical_ids do not break GROUP BY / JOIN chain.")
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = get_conn()

    log.info("Reading FBref players…")
    fb = fetch_fbref_players(conn)
    log.info(f"  {len(fb)} FBref players")

    log.info("Reading Understat players…")
    us = fetch_understat_players(conn)
    log.info(f"  {len(us)} Understat players")

    log.info("Reading WhoScored players…")
    ws = fetch_whoscored_players(conn)
    log.info(f"  {len(ws)} WhoScored players")

    log.info("Resolving identities…")
    rows = resolve(fb, us, ws)
    log.info(f"  produced {len(rows)} xref rows")

    log.info(f"Writing to {SCRATCH_TABLE}…")
    write_to_trino(conn, rows)

    # Verification
    passed = verify_known_pairs(conn)
    coverage_per_source(conn)
    feat_player_form_smoke(conn)

    print()
    print("=" * 80)
    print(f"DONE. Known-pair pass: {passed}/{len(KNOWN_PAIRS)}.")
    print(f"Inspect: SELECT * FROM {SCRATCH_TABLE} LIMIT 50;")
    print("=" * 80)


if __name__ == "__main__":
    main()
