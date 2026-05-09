"""
Player identity resolver — production-quality (E1, T3)
======================================================

Production port of the R2 spike prototype (``scripts/r2_resolver_proto.py``).
Resolves cross-source player identities into a single ``canonical_id`` and
materialises ``iceberg.silver.xref_player`` via Trino INSERT.

Pipeline
--------
1. Read Bronze for the FBref / Understat / WhoScored sources (Trino).
2. FBref is the spine — every FBref player becomes a canonical row
   (``canonical_id = 'fb_<player_id>'``, ``confidence = 'exact'``).
3. Understat / WhoScored cascade:

    - ``exact``     — when ``source_id`` matches an FBref player_id
      (rare cross-source).
    - ``name_team`` — fuzzy match on canonical-team bucket using
      ``rapidfuzz.fuzz.token_sort_ratio`` after ``unidecode + lower``;
      threshold ≥ 90.
    - ``name_team_jersey`` / ``name_team_dob`` — STUBS (Bronze does not carry
      jersey or cross-source DOB consistently). Reserved in schema.
    - ``orphan`` — no match: ``canonical_id = '<src>_' || source_id`` where
      ``src`` ∈ {``us``, ``ws``, ``ss``}. ``ss`` (SofaScore) reserved for
      R0.2 follow-up.

4. Idempotent rewrite of ``iceberg.silver.xref_player``: DROP + CREATE +
   batched INSERT (500 rows per VALUES tuple to stay within Trino SQL
   length limits).
5. Known-pair regression guard — 10 hand-picked APL 2024-25 players must
   resolve into a single ``canonical_id`` across all 3 sources. Pass-rate
   < 8/10 raises :class:`ResolverError`.

Why Python (not pure SQL)?
--------------------------
``rapidfuzz.fuzz.token_sort_ratio`` and ``unidecode`` cannot be expressed
in Trino SQL. Total input on APL 2024-25 is ~1700 rows (562 FBref + 562
Understat + 491 WhoScored), so an in-memory pass is trivial — we only use
Trino for IO, never for the fuzzy matching itself.

Why no dependency on ``scrapers/*``?
------------------------------------
Importing ``scrapers/__init__.py`` from an Airflow task pulls in
nodriver / selenium / soccerdata / curl_cffi, which would push DAG-parse
RAM to ~1.5 GB and break the scheduler memory cap. This module imports
only ``trino`` (DBAPI) at module level and lazy-imports ``rapidfuzz`` /
``unidecode`` inside :func:`run_resolver` so DAG-parse stays cheap.

Public API contract (frozen for T4 DAG integration)
---------------------------------------------------
* :func:`run_resolver` — full pipeline; returns a summary dict.
* :func:`normalize_name` — pure helper; testable without Trino.
* :func:`fuzzy_match_score` — pure helper; rapidfuzz wrapper.
* :func:`canonical_team_for_resolver` — wrapper over
  :func:`utils.medallion_config.get_canonical_team_name` with a sensible
  fallback (raw-name passthrough) so unmapped clubs still get *some* team
  bucket and aren't silently dropped from the cascade.
* :func:`cascade_resolve` — pure tier-cascade evaluator over an in-memory
  spine index. Used both by :func:`run_resolver` and by unit tests.
* :class:`ResolverError` — raised on regression (known-pair pass-rate
  below threshold).
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import trino as trino_lib

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------
DEFAULT_TARGET_TABLE = 'iceberg.silver.xref_player'

#: token_sort_ratio threshold (0-100). 90 was tuned on the R2 algo spike —
#: catches typical accent / dash / order-of-tokens variants without leaking
#: cross-player false positives. Do NOT lower without rerunning the spike.
NAME_THRESHOLD: float = 90.0

#: Sources covered at E1. ``sofascore`` and ``fotmob`` are deferred to the
#: R0.2 follow-up (Bronze tables for those sources do not yet expose a
#: stable per-player id row). ``sofifa`` has its own season-alignment
#: problem (FIFA seasons != football seasons) and is a separate effort.
SOURCES: Tuple[str, ...] = ('fbref', 'understat', 'whoscored')

#: Default batch size for ``INSERT INTO ... VALUES (...)``. 500 fits
#: comfortably under Trino's default ``query.max-length`` (≈ 16 MB) for
#: our per-row payload size (~150 bytes).
DEFAULT_CHUNK_SIZE = 500

#: Known APL 2024-25 pairs the resolver MUST resolve to a single
#: canonical_id across all three sources. Pulled from R2 spike — hard-coded
#: rather than configurable so a regression in alias or threshold tuning
#: surfaces immediately. Kept in sync with ``scripts/r2_resolver_proto.py``
#: (KNOWN_PAIRS at the bottom).
KNOWN_PAIRS: Tuple[Tuple[str, str], ...] = (
    ('Bukayo Saka', 'fb_bc7dc64d'),
    ('Mohamed Salah', 'fb_e342ad68'),
    ('Erling Haaland', 'fb_1f44ac21'),
    ('Bruno Fernandes', 'fb_507c7bdf'),
    ('Rodri', 'fb_6434f10d'),
    ('Son Heung-min', 'fb_92e7e919'),
    ('Virgil van Dijk', 'fb_e06683ca'),
    ('Cole Palmer', 'fb_dc7f8a28'),
    ('Bruno Guimarães', 'fb_82518f62'),
    ('Joško Gvardiol', 'fb_5ad50391'),
)

#: Below this pass-rate the resolver raises ResolverError. 8/10 is the
#: target codified in docs/research/R2_player_resolver.md.
KNOWN_PAIR_MIN_PASS = 8


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class ResolverError(RuntimeError):
    """Raised when the resolver fails a regression / quality gate.

    The intent is for an Airflow task to translate this into a failed
    DAG-run rather than silently producing a degraded ``xref_player`` table.
    """


# ---------------------------------------------------------------------------
# Pure helpers (testable without Trino)
# ---------------------------------------------------------------------------
def normalize_name(s: Optional[str]) -> str:
    """Normalize a player name for fuzzy comparison.

    ``unidecode`` strips diacritics ("Joško" -> "Josko"), then we lowercase
    and collapse whitespace. Lazy-imported because ``unidecode`` is ~2 MB and
    not needed by Airflow's DAG-parse step.

    Args:
        s: Raw name. ``None`` and empty string both return ``""``.

    Returns:
        Normalized form. Order of tokens is NOT canonicalized here —
        :func:`fuzzy_match_score` uses ``token_sort_ratio`` which handles
        token-order invariance (so ``"Heung-Min Son"`` and ``"Son Heung-min"``
        score 100 after normalisation).
    """
    if not s:
        return ""
    # Lazy import — keeps DAG-parse <50ms and avoids unidecode loading
    # its 600 KB data table at import time.
    from unidecode import unidecode  # type: ignore

    return " ".join(unidecode(s).lower().split())


def fuzzy_match_score(name_a: Optional[str], name_b: Optional[str]) -> float:
    """``token_sort_ratio`` of two names, after :func:`normalize_name`.

    Returns 0.0 if either input is empty / None — callers can rely on
    "anything ≥ NAME_THRESHOLD is a real match".
    """
    a = normalize_name(name_a)
    b = normalize_name(name_b)
    if not a or not b:
        return 0.0
    from rapidfuzz import fuzz  # type: ignore

    return float(fuzz.token_sort_ratio(a, b))


def canonical_team_for_resolver(
    raw_team: Optional[str],
    source: str,
) -> Optional[str]:
    """Resolve a raw team name via :mod:`utils.medallion_config`.

    Behaviour:
      * Empty / None input  -> returns ``None``.
      * Mapped raw -> returns canonical_name (e.g. "Wolves" -> "Wolverhampton
        Wanderers").
      * Unmapped raw -> returns the raw_name *stripped* (acts as identity).
        This is intentional: an unmapped club still groups its own players
        into a single bucket, so a within-team fuzzy lookup still works
        even if the alias YAML missed an entry. Cross-source mismatches
        ("Wolves" vs "Wolverhampton Wanderers") obviously won't reconcile,
        but they would also fail in the legacy SQL flow — same blast radius.

    Args:
        raw_team: As stored in Bronze (e.g. ``Spurs``, ``Wolverhampton``).
        source: One of :data:`SOURCES` — passed through to
            ``medallion_config.get_canonical_team_name`` so the right
            per-source alias bucket is consulted (plus ``_generic``).
    """
    if not raw_team:
        return None
    raw = raw_team.strip()
    if not raw:
        return None
    # Lazy import — medallion_config opens YAML on first call.
    from utils.medallion_config import get_canonical_team_name

    canonical = get_canonical_team_name(raw, source=source)
    return canonical if canonical else raw


# ---------------------------------------------------------------------------
# Spine index + cascade
# ---------------------------------------------------------------------------
class _FBrefSpine:
    """In-memory FBref player index, keyed by canonical team.

    Two lookup paths:
      * ``by_id``       — exact match on FBref ``player_id``.
      * ``by_team``     — list of (normalized_name, player_id) pairs per
        canonical team, used for fuzzy lookup within a team bucket.
    """

    __slots__ = ('by_id', 'by_team', 'norm_to_id')

    def __init__(self, fbref_rows: List[Dict[str, Any]]):
        # row keys: 'player_id', 'player_name', 'canonical_team' (already
        # canonicalised by caller — keeps cascade pure of YAML access).
        self.by_id: Dict[str, Dict[str, Any]] = {}
        self.by_team: Dict[str, List[Tuple[str, str]]] = {}
        self.norm_to_id: Dict[Tuple[str, str], str] = {}

        for row in fbref_rows:
            pid = row['player_id']
            if not pid or pid in self.by_id:
                continue
            self.by_id[pid] = row
            team = row.get('canonical_team') or ''
            n = normalize_name(row.get('player_name'))
            if team:
                self.by_team.setdefault(team, []).append((n, pid))
                self.norm_to_id[(n, team)] = pid

    def find_by_id(self, source_id: str) -> Optional[str]:
        """Tier-1: exact FBref player_id match."""
        return source_id if source_id in self.by_id else None

    def find_by_name_team(
        self,
        name: Optional[str],
        canonical_team: Optional[str],
    ) -> Tuple[Optional[str], float]:
        """Tier-2: fuzzy name match within a canonical-team bucket.

        Returns ``(player_id, score)`` if best score ≥ :data:`NAME_THRESHOLD`,
        else ``(None, best_score_seen)`` — the raw best score is preserved
        so orphan rows can still record "we tried, here's how close it got"
        for downstream debugging.
        """
        if not canonical_team:
            return None, 0.0
        cands = self.by_team.get(canonical_team)
        if not cands:
            return None, 0.0

        n = normalize_name(name)
        if not n:
            return None, 0.0

        # Lazy import inside the hot path — but only on first call per
        # process. Subsequent calls benefit from import cache.
        from rapidfuzz import fuzz  # type: ignore

        best_id: Optional[str] = None
        best_score = 0.0
        for cn, fid in cands:
            score = float(fuzz.token_sort_ratio(n, cn))
            if score > best_score:
                best_id, best_score = fid, score
        if best_score >= NAME_THRESHOLD:
            return best_id, best_score
        return None, best_score


def cascade_resolve(
    candidate: Dict[str, Any],
    spine: _FBrefSpine,
) -> Tuple[str, str, Optional[float]]:
    """Run the tier cascade for a single non-FBref candidate row.

    Args:
        candidate: dict with keys ``source``, ``source_id``, ``player_name``,
            ``canonical_team``.
        spine: prebuilt :class:`_FBrefSpine`.

    Returns:
        ``(canonical_id, confidence, match_score)``.

        * ``confidence='exact'``     -> ``match_score`` is None (FBref id hit).
        * ``confidence='name_team'`` -> ``match_score`` is the rapidfuzz score.
        * ``confidence='orphan'``    -> ``match_score`` is best score seen
          (may be None if team bucket was empty, else float).

    The function is a *pure* function over ``(candidate, spine)`` — kept
    that way deliberately so unit tests can assert exact behaviour without
    any Trino mock.
    """
    src = candidate['source']
    sid = str(candidate['source_id'])
    name = candidate.get('player_name')
    team = candidate.get('canonical_team')

    # Tier-1: exact id (FBref-id collision across sources is rare but
    # checked first so we don't waste a fuzzy pass).
    fid = spine.find_by_id(sid)
    if fid:
        return f'fb_{fid}', 'exact', None

    # Tier-2: name + canonical-team fuzzy match.
    fid, score = spine.find_by_name_team(name, team)
    if fid:
        return f'fb_{fid}', 'name_team', score

    # Tiers 3-4 are stubs (no jersey / DOB cross-source data) — fall through
    # to orphan. Schema reserves the confidence labels for forward-compat.
    prefix = _orphan_prefix(src)
    return f'{prefix}_{sid}', 'orphan', (score if score > 0 else None)


def _orphan_prefix(source: str) -> str:
    """Map source name to orphan canonical_id prefix.

    Hard-coded — keeps tier-cascade insulated from string-mangling logic
    and means a typo in a source name surfaces as an immediate KeyError.
    """
    return {
        'understat': 'us',
        'whoscored': 'ws',
        'sofascore': 'ss',
    }[source]


# ---------------------------------------------------------------------------
# Trino plumbing
# ---------------------------------------------------------------------------
def _get_trino_connection(
    catalog: str = 'iceberg',
) -> 'trino_lib.dbapi.Connection':
    """Mirrors :func:`utils.silver_tasks._get_trino_connection`.

    Duplicated rather than imported because :mod:`silver_tasks` carries
    ~700 lines of Silver-specific logic; the duplication is small (40 lines)
    and keeps this module self-contained for unit testing.
    """
    host = os.environ.get('TRINO_HOST', 'trino')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    if password:
        port = int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,  # nosec B501 — self-signed Trino cert; см. configs/trino/config.properties
        )

    port = int(os.environ.get('TRINO_PORT', 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
    )


def _execute(conn, sql: str, fetch: bool = False):
    """Execute a single SQL statement and consume results.

    Critical: *every* DDL/DML must call ``fetchall()``, otherwise Trino
    treats the missing fetch as a client-side cancellation and the next
    query returns USER_CANCELED. See CLAUDE.md / data-platform memory.
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        if fetch:
            return cur.fetchall()
        try:
            cur.fetchall()
        except Exception:
            pass
        return None
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Bronze readers
# ---------------------------------------------------------------------------
# NOTE on column names: query layout is taken verbatim from
# scripts/r2_resolver_proto.py which was successfully run on Bronze APL
# 2024-25 (see docs/research/R2_player_resolver.md). Bronze schemas:
#   * fbref_player_stats     -> player_id, player, squad, season(int), league
#   * understat_players      -> player_id, player, team, season(varchar), league
#   * whoscored_events       -> player_id, player, team, season(varchar), league
# (squad/team naming difference is REAL — FBref calls it "squad").
# Re-discovered column names would be a regression so they are pinned here.


def _seasons_in_clause(seasons: List[Any]) -> str:
    """Render a list of season values as a Trino IN clause body.

    Mixes int and string values verbatim — caller is responsible for type:
    FBref Bronze stores season as bigint; Understat/WhoScored store it as
    varchar (e.g. '2425'). Trino will type-coerce inside IN if needed.
    """
    parts: List[str] = []
    for s in seasons:
        if isinstance(s, int):
            parts.append(str(s))
        else:
            # Whitelist literal — season strings are 4-digit slugs ('2425')
            # or alphanumeric. Refuse anything else outright (defense in depth
            # — quotes, comments, statement separators all get blocked here).
            ss = str(s)
            if not re.fullmatch(r"[A-Za-z0-9_]+", ss):
                raise ValueError(f"unsafe season literal: {ss!r}")
            parts.append(f"'{ss}'")
    return ', '.join(parts)


def _fetch_fbref_players(
    conn, league: str, fbref_seasons: List[int]
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT player_id, player, squad, league, CAST(season AS varchar) AS season
        FROM iceberg.bronze.fbref_player_stats
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(fbref_seasons)})
          AND stat_type = 'stats'
          AND player_id IS NOT NULL
        GROUP BY player_id, player, squad, league, CAST(season AS varchar)
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, squad, lg, season in rows:
        if pid in seen:
            continue
        seen.add(pid)
        out.append(
            {
                'source': 'fbref',
                'player_id': str(pid),
                'source_id': str(pid),
                'player_name': name,
                'raw_team_name': squad,
                'canonical_team': canonical_team_for_resolver(squad, 'fbref'),
                'league': lg,
                'season': season,
            }
        )
    return out


def _fetch_understat_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT CAST(player_id AS varchar) AS pid, player, team, league, season
        FROM iceberg.bronze.understat_players
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player IS NOT NULL
        GROUP BY player_id, player, team, league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, team, lg, season in rows:
        if pid in seen:
            continue
        seen.add(pid)
        out.append(
            {
                'source': 'understat',
                'source_id': pid,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'understat'),
                'league': lg,
                'season': season,
            }
        )
    return out


def _fetch_whoscored_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT CAST(CAST(player_id AS bigint) AS varchar) AS pid,
               MAX(player) AS player,
               MAX(team) AS team,
               league,
               season
        FROM iceberg.bronze.whoscored_events
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player_id IS NOT NULL
          AND player IS NOT NULL
        GROUP BY CAST(player_id AS bigint), league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    for pid, name, team, lg, season in rows:
        out.append(
            {
                'source': 'whoscored',
                'source_id': pid,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'whoscored'),
                'league': lg,
                'season': season,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Trino write helpers
# ---------------------------------------------------------------------------
def _sql_escape(s: str) -> str:
    """Escape an apostrophe inside a single-quoted SQL literal."""
    return s.replace("'", "''")


def _sql_str(s: Optional[str]) -> str:
    if s is None:
        return 'NULL'
    return f"'{_sql_escape(str(s))}'"


def _sql_double(v: Optional[float]) -> str:
    if v is None:
        return 'NULL'
    return f"CAST({float(v)} AS DOUBLE)"


def _value_tuple(row: Dict[str, Any]) -> str:
    return (
        '('
        f"{_sql_str(row['canonical_id'])}, "
        f"{_sql_str(row['source'])}, "
        f"{_sql_str(row['source_id'])}, "
        f"{_sql_str(row['display_name'])}, "
        f"{_sql_str(row['league'])}, "
        f"{_sql_str(row['season'])}, "
        f"{_sql_str(row['confidence'])}, "
        f"{_sql_double(row['match_score'])}, "
        f"{_sql_str(row['raw_team_name'])}, "
        f"{_sql_str(row['canonical_team'])}"
        ')'
    )


def _create_target_table(conn, target_table: str) -> None:
    """DROP + CREATE the Iceberg target. Partitioned by (league, season)."""
    _execute(conn, f"DROP TABLE IF EXISTS {target_table}")
    _execute(
        conn,
        f"""
        CREATE TABLE {target_table} (
            canonical_id   varchar,
            source         varchar,
            source_id      varchar,
            display_name   varchar,
            league         varchar,
            season         varchar,
            confidence     varchar,
            match_score    double,
            raw_team_name  varchar,
            canonical_team varchar
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['league', 'season']
        )
        """,
    )


def _insert_rows(
    conn,
    target_table: str,
    rows: List[Dict[str, Any]],
    chunk_size: int,
) -> int:
    """Batched ``INSERT INTO ... VALUES (...), (...)``.

    Returns rows actually inserted.
    """
    written = 0
    cols = (
        'canonical_id, source, source_id, display_name, league, season, '
        'confidence, match_score, raw_team_name, canonical_team'
    )
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        values_sql = ',\n'.join(_value_tuple(r) for r in chunk)
        _execute(
            conn,
            f"INSERT INTO {target_table} ({cols}) VALUES {values_sql}",
        )
        written += len(chunk)
        logger.info(
            "  wrote %d/%d rows",
            min(i + chunk_size, len(rows)),
            len(rows),
        )
    return written


# ---------------------------------------------------------------------------
# Build the materialised rows
# ---------------------------------------------------------------------------
def _resolve_all(
    fb_rows: List[Dict[str, Any]],
    us_rows: List[Dict[str, Any]],
    ws_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    """Apply the cascade across all 3 sources. Returns (rows, per_source_stats).

    ``per_source_stats`` shape:
        {'fbref':     {'total': N, 'resolved': N, 'orphan': 0},
         'understat': {'total': N, 'resolved': X, 'orphan': Y},
         'whoscored': {'total': N, 'resolved': X, 'orphan': Y}}
    """
    spine = _FBrefSpine(fb_rows)
    out: List[Dict[str, Any]] = []
    stats: Dict[str, Dict[str, int]] = {
        s: {'total': 0, 'resolved': 0, 'orphan': 0} for s in SOURCES
    }

    # FBref spine: every FBref player IS canonical.
    for row in fb_rows:
        out.append(
            {
                'canonical_id': f"fb_{row['player_id']}",
                'source': 'fbref',
                'source_id': row['source_id'],
                'display_name': row['player_name'],
                'league': row['league'],
                'season': row['season'],
                'confidence': 'exact',
                'match_score': None,
                'raw_team_name': row['raw_team_name'],
                'canonical_team': row['canonical_team'],
            }
        )
        stats['fbref']['total'] += 1
        stats['fbref']['resolved'] += 1

    # Cascade for non-FBref sources.
    for src_rows in (us_rows, ws_rows):
        for row in src_rows:
            cid, conf, score = cascade_resolve(row, spine)
            out.append(
                {
                    'canonical_id': cid,
                    'source': row['source'],
                    'source_id': row['source_id'],
                    'display_name': row['player_name'],
                    'league': row['league'],
                    'season': row['season'],
                    'confidence': conf,
                    'match_score': score,
                    'raw_team_name': row['raw_team_name'],
                    'canonical_team': row['canonical_team'],
                }
            )
            stats[row['source']]['total'] += 1
            if conf == 'orphan':
                stats[row['source']]['orphan'] += 1
            else:
                stats[row['source']]['resolved'] += 1

    return out, stats


def _verify_known_pairs(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Return (passed, total). A pair "passes" iff the expected canonical_id
    appears with at least one row from each of FBref, Understat, WhoScored.

    Done in-memory rather than as a Trino query so the regression check
    works even when the INSERT step is mocked out (e.g. unit tests).
    """
    by_cid: Dict[str, set] = {}
    for r in rows:
        by_cid.setdefault(r['canonical_id'], set()).add(r['source'])
    passed = 0
    for _, expected_cid in KNOWN_PAIRS:
        sources = by_cid.get(expected_cid, set())
        if {'fbref', 'understat', 'whoscored'} <= sources:
            passed += 1
    return passed, len(KNOWN_PAIRS)


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------
def _slug_to_fbref_year(slug: int) -> int:
    """Convert season slug (e.g. 2425 for 2024-25) to FBref year-of-start (2024).

    YAML/Bronze varchar use slug; FBref Bronze stores integer year-of-start.
    Mapping: ``slug // 100 + 2000``.

        2122 -> 2021
        2425 -> 2024
        2526 -> 2025

    Raises:
        ValueError: if slug is not a 4-digit ``yyXX`` value.
    """
    if slug < 100:
        raise ValueError(f"season slug must be 4-digit yyXX (got {slug})")
    return (slug // 100) + 2000


def _split_seasons(slugs: List[int]) -> Tuple[List[int], List[str]]:
    """Map slug list ``[2425]`` -> ``(fbref=[2024], legacy=['2425'])``.

    Conventions across the platform:
      * YAML / public resolver API     -> 4-digit slug ``yyXX`` (``2425``).
      * FBref Bronze ``season`` (bigint) -> year-of-start (``2024``).
      * Understat / WhoScored Bronze ``season`` (varchar) -> slug (``'2425'``).

    The split is what lets FBref filters use ``season IN (2024)`` while
    Understat/WhoScored filters use ``season IN ('2425')`` from a single
    YAML-derived input list.
    """
    fbref: List[int] = []
    legacy: List[str] = []
    for slug in slugs:
        slug_int = int(slug)
        fbref.append(_slug_to_fbref_year(slug_int))
        legacy.append(f"{slug_int:04d}")
    return fbref, legacy


def _default_seasons_from_config(league: str) -> List[int]:
    """Pull every configured season for the given league out of competitions.yaml.

    Failure modes:
      * League not in catalog -> KeyError (loud — caller passed a typo).
      * League present but seasons list empty -> raises ResolverError
        (silent empty would produce a 0-row xref, masking a real config bug).
    """
    from utils.medallion_config import get_competition_seasons

    seasons = get_competition_seasons(league)
    if not seasons:
        raise ResolverError(
            f"competitions.yaml has no seasons for league={league!r} — "
            "refusing to materialise an empty xref_player."
        )
    return list(seasons)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def run_resolver(
    target_table: str = DEFAULT_TARGET_TABLE,
    league: str = 'ENG-Premier League',
    seasons: Optional[List[int]] = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    drop_before_insert: bool = True,
) -> Dict[str, Any]:
    """Full pipeline: read 3 Bronze sources -> resolve -> write Iceberg.

    Args:
        target_table: Fully-qualified Iceberg table (catalog.schema.table).
        league: League id from ``competitions.yaml``. Currently only
            ``ENG-Premier League`` is in scope (E1 baseline).
        seasons: List of season slugs (e.g. ``[2425]`` for 2024-25). Use
            slug format (``yyXX``), NOT year-of-start. ``None`` means
            "all configured seasons for ``league``" pulled from
            ``competitions.yaml``.
        chunk_size: Rows per ``INSERT VALUES`` batch.
        drop_before_insert: If True (default), full rebuild semantics —
            DROP TABLE + CREATE + INSERT. Set False only if you want to
            run the resolver as a smoke-test without rewriting Iceberg
            (mostly useful in dual-run validation).

    Returns:
        Summary dict::

            {
                'target_table': 'iceberg.silver.xref_player',
                'rows_inserted': 1615,
                'per_source': {
                    'fbref':     {'total': 562, 'resolved': 562,
                                  'orphan': 0,  'rejection_pct': 0.0},
                    'understat': {'total': 562, 'resolved': 546,
                                  'orphan': 16, 'rejection_pct': 2.8},
                    'whoscored': {'total': 491, 'resolved': 475,
                                  'orphan': 16, 'rejection_pct': 3.3},
                },
                'known_pair_pass_rate': '10/10',
                'duration_sec': 7.4,
            }

    Raises:
        ResolverError: when the known-pair regression check returns
            < KNOWN_PAIR_MIN_PASS (8/10).
    """
    started = time.time()

    if seasons is None:
        seasons = _default_seasons_from_config(league)
    fbref_seasons, source_seasons = _split_seasons(seasons)

    logger.info(
        "Starting xref_player resolver: target=%s league=%s "
        "seasons=%s (fbref=%s, source=%s)",
        target_table,
        league,
        seasons,
        fbref_seasons,
        source_seasons,
    )

    conn = _get_trino_connection()
    try:
        logger.info("Reading FBref players ...")
        fb = _fetch_fbref_players(conn, league, fbref_seasons)
        logger.info("  %d FBref players", len(fb))

        logger.info("Reading Understat players ...")
        us = _fetch_understat_players(conn, league, source_seasons)
        logger.info("  %d Understat players", len(us))

        logger.info("Reading WhoScored players ...")
        ws = _fetch_whoscored_players(conn, league, source_seasons)
        logger.info("  %d WhoScored players", len(ws))

        logger.info("Resolving identities ...")
        rows, stats = _resolve_all(fb, us, ws)
        logger.info("  produced %d xref rows", len(rows))

        # Regression guard — done before INSERT so a failure aborts without
        # touching the Iceberg table.
        # Bind to dedicated names so the per-source `total` rebind below
        # cannot shadow these (was a real bug — summary showed e.g. "10/491").
        known_passed, known_total = _verify_known_pairs(rows)
        if known_passed < KNOWN_PAIR_MIN_PASS:
            raise ResolverError(
                f"Known-pair regression: {known_passed}/{known_total} passed, "
                f"target ≥{KNOWN_PAIR_MIN_PASS}/{known_total}. "
                "Inspect alias YAML / threshold tuning before retrying."
            )

        rows_inserted = 0
        if drop_before_insert:
            logger.info("Rewriting Iceberg target %s ...", target_table)
            _create_target_table(conn, target_table)
            rows_inserted = _insert_rows(conn, target_table, rows, chunk_size)
            logger.info("  inserted %d rows into %s", rows_inserted, target_table)
        else:
            logger.info(
                "drop_before_insert=False — skipping Iceberg write "
                "(dry-run / smoke mode)."
            )

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Build summary
    per_source: Dict[str, Dict[str, Any]] = {}
    for src, st in stats.items():
        total = st['total']
        rejection_pct = (
            round(100.0 * st['orphan'] / total, 2) if total else 0.0
        )
        per_source[src] = {
            'total': total,
            'resolved': st['resolved'],
            'orphan': st['orphan'],
            'rejection_pct': rejection_pct,
        }

    summary = {
        'target_table': target_table,
        'rows_inserted': rows_inserted,
        'per_source': per_source,
        'known_pair_pass_rate': f"{known_passed}/{known_total}",
        'duration_sec': round(time.time() - started, 2),
    }
    logger.info("Resolver summary: %s", summary)
    return summary
