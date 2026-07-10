#!/usr/bin/env python3
"""
SofaScore Scraper Runner Script
===============================

Standalone script to run SofaScore scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supported entities:
- ``schedule``        : per-round schedule + final scores (default)
- ``league_table``    : standings snapshot
- ``player_ratings``  : per-match player ratings (Opta 0.0-10.0) via
                       the public ``/api/v1/event/{id}/lineups`` endpoint.
                       Daily DAG passes the full set of finished matches;
                       writer uses ``replace_partitions=['league', 'season']``
                       so each run refreshes the partition wholesale.
- ``shotmap``         : per-shot coords + xG + situation via
                       ``/api/v1/event/{id}/shotmap`` (issue #22).
- ``event_player_stats``: per-(match, player) Opta-rich stats via
                       ``/api/v1/event/{id}/player/{pid}/statistics`` (#21).
                       Player ids are resolved from
                       ``bronze.sofascore_player_ratings`` — that table
                       must be fresh before this entity runs.
- ``match_stats``     : team-level per-(match, period, stat) long-form
                       rows from ``/api/v1/event/{id}/statistics`` (#25).

Exit codes:
    0 — scrape completed successfully (>= 1 row written)
    1 — hard failure (exception raised, runner crashed; or a CLI parse error
        — unknown/typo'd flag, invalid value — #512, kept off exit 2 so the
        DAG wrapper does not mistake it for a fallback soft-success)
    2 — graceful R0.2B_FALLBACK: lineups endpoint unavailable
        (HTTP 403 / proxy quota empty / repeated timeouts).
        DataFrame is empty, nothing written to bronze. The Gold-layer
        E4.4 schema-stub path keys off this exit code so the medallion
        pipeline keeps building without ratings.
"""

import argparse
import json
import logging
import os
import sys
import warnings
from typing import List, Optional

warnings.filterwarnings('ignore', category=DeprecationWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class _ArgparseError(Exception):
    """Raised by _StrictArgumentParser.error so main() returns exit 1."""


class _StrictArgumentParser(argparse.ArgumentParser):
    """argparse exits 2 on a CLI parse error (bad/unknown flag, wrong type).
    The DAG bash wrapper maps exit 2 to a SofaScore fallback soft-success, so
    a flag typo would silently no-op the task (#512). Funnel every parse error
    through a catchable exception → main() returns hard-failure exit 1.
    """

    def error(self, message):
        self.print_usage(sys.stderr)
        raise _ArgparseError(message)


# Entities runnable from this script. Kept as constants so we can wire
# the same names into Airflow without round-tripping through magic strings.
ENTITY_SCHEDULE = 'schedule'
ENTITY_LEAGUE_TABLE = 'league_table'
ENTITY_PLAYER_RATINGS = 'player_ratings'
ENTITY_SHOTMAP = 'shotmap'
ENTITY_EVENT_PLAYER_STATS = 'event_player_stats'
ENTITY_MATCH_STATS = 'match_stats'
# #751 PR1 — consolidated per-match capture: ONE Camoufox nav/match feeds BOTH
# player_ratings and event_player_stats from the same /lineups (+/event) payload.
ENTITY_MATCH_CAPTURE = 'match_capture'
# #751 PR3 + PR3b — per-player capture: ONE Camoufox nav/player writes BOTH
# player_profile (bio SSR'd in __NEXT_DATA__) and player_season_stats (Season-tab
# picker capture). Replaces the dead tls player_profile/player_season_stats pass.
ENTITY_PLAYER_CAPTURE = 'player_capture'

VALID_ENTITIES = {
    ENTITY_SCHEDULE,
    ENTITY_LEAGUE_TABLE,
    ENTITY_PLAYER_RATINGS,
    ENTITY_SHOTMAP,
    ENTITY_EVENT_PLAYER_STATS,
    ENTITY_MATCH_STATS,
    ENTITY_MATCH_CAPTURE,
    ENTITY_PLAYER_CAPTURE,
}

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a bronze.sofascore_* (league, season) partition below this share of its
# existing rows, so a partial/failed scrape can't wipe a good partition.
# COUNT(*) (no replace_guard_key) — each (league, season) is scraped full-state.
# ReplaceGuardError → exit 3; bypass with --force-replace. NOTE: the append-only
# event endpoint (shotmap / event_player_stats / match_stats) is NOT guarded —
# it has no replace_partitions (rows preserved across runs, #69).
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'SOFASCORE_REPLACE_GUARD'


def _trino_connect():
    """Open a Trino dbapi connection from env. Returns None on import error."""
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return None

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        return trino.dbapi.connect(
            host=os.environ.get('TRINO_HOST', 'trino'),
            port=int(os.environ.get('TRINO_PORT', 8443)),
            user=user,
            catalog='iceberg',
            http_scheme='https',
            auth=trino_auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8080)),
        user=user,
        catalog='iceberg',
    )


def _existing_match_ids_in_bronze(
    table: str,
    league: str,
    season_short: str,
    id_col: str = 'match_id',
) -> set:
    """Return distinct ``id_col`` strings already materialised in
    ``iceberg.bronze.<table>`` for the given partition. ``id_col`` defaults to
    ``match_id``; the venue table keys its rows by ``game_id`` (#847).

    Returns an empty set when the table does not yet exist (first run)
    or any Trino-side error occurs — caller then treats input as fully
    new. Issue #69 skip-existing path.
    """
    conn = _trino_connect()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        sql = (
            f"SELECT DISTINCT CAST({id_col} AS varchar) "
            f"FROM iceberg.bronze.{table} "
            f"WHERE league = ? AND CAST(season AS varchar) = ?"
        )
        cur.execute(sql, (league, season_short))
        rows = cur.fetchall()
        return {r[0] for r in rows if r and r[0] is not None}
    except Exception as e:
        logger.warning(
            "skip-existing probe on bronze.%s failed (%s) — treating "
            "all input match_ids as new.",
            table, e,
        )
        return set()


def _resolve_match_ids_from_bronze(
    league: str,
    season: str,
    limit: Optional[int],
) -> List[str]:
    """Pull finished match ids straight from ``bronze.sofascore_schedule``.

    Avoids re-hitting SofaScore for the schedule when we already have a
    fresh copy in the lakehouse. Returns ``[]`` when the table is missing
    or empty — the caller will then emit ``R0.2B_FALLBACK``.
    """
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return []

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    try:
        if password:
            conn = trino.dbapi.connect(
                host=os.environ.get('TRINO_HOST', 'trino'),
                port=int(os.environ.get('TRINO_PORT', 8443)),
                user=user,
                catalog='iceberg',
                http_scheme='https',
                auth=trino_auth.BasicAuthentication(user, password),
                verify=False,
            )
        else:
            conn = trino.dbapi.connect(
                host=os.environ.get('TRINO_HOST', 'trino'),
                port=int(os.environ.get('TRINO_PORT', 8080)),
                user=user,
                catalog='iceberg',
            )

        cur = conn.cursor()
        # #840: Bronze auto-passthrough renamed home_score->home_score_current,
        # date->start_timestamp. COALESCE bridges pre-#840 partitions; if only
        # one schema's columns exist the other reference raises and the caller's
        # except-branch falls back to the capture resolver.
        sql = (
            "SELECT CAST(game_id AS varchar) AS gid "
            "FROM iceberg.bronze.sofascore_schedule "
            "WHERE league = ? AND CAST(season AS varchar) = ? "
            "  AND COALESCE(home_score, home_score_current) IS NOT NULL "
            "ORDER BY COALESCE(start_timestamp, to_unixtime(date)) DESC"
        )
        if limit:
            # Trino dialect: LIMIT goes in SQL; bind params don't bind it.
            sql = sql + f" LIMIT {int(limit)}"
        cur.execute(sql, (league, season))
        rows = cur.fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        logger.warning(
            "Could not resolve match_ids from bronze (%s) — "
            "falling back to soccerdata schedule fetch.", e,
        )
        return []


# Fallback column shape for an EMPTY existing partition (fresh season / Trino
# unreachable). #840: Bronze is auto-passthrough now, so the live column set
# evolves — _read_existing_schedule reads it dynamically via SELECT *; this list
# only shapes the empty DataFrame (which _merge_schedule_partition replaces with
# the captured rows wholesale anyway).
_SCHEDULE_COLUMNS = ['game_id', 'league', 'season']


def _read_existing_schedule(league: str, season: str):
    """Read the existing ``bronze.sofascore_schedule`` (league, season) partition
    into a DataFrame so the captured window can be MERGED with it rather than
    replacing it (#761). Camoufox capture only surfaces a window of events
    (current round + recent finished + upcoming), so a straight
    ``replace_partitions`` would trip the completeness guard once the partition
    has accumulated more than the window. Returns an EMPTY DataFrame when the
    table/partition is missing or Trino is unreachable — the caller then saves
    the captured rows as-is (a fresh-season partition is empty anyway).
    """
    import pandas as pd

    empty = pd.DataFrame(columns=_SCHEDULE_COLUMNS)
    conn = _trino_connect()
    if conn is None:
        return empty
    try:
        cur = conn.cursor()
        # #840: schema-agnostic SELECT * — Bronze auto-passthrough evolves the
        # column set; _merge_schedule_partition reindexes to the captured columns,
        # so a full-season capture rewrites the partition cleanly on the #840
        # transition (partial captures then merge in the new schema unchanged).
        cur.execute(
            "SELECT * FROM iceberg.bronze.sofascore_schedule "
            "WHERE league = ? AND CAST(season AS varchar) = ?",
            (league, season),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        logger.warning(
            "Could not read existing schedule partition (league=%s season=%s): "
            "%s — saving captured rows as-is.", league, season, e,
        )
        return empty


def _merge_schedule_partition(existing, captured):
    """Union an existing schedule partition with a freshly-captured window,
    keyed by ``game_id`` with the captured row winning (fresh scores). The
    result is never smaller than ``existing``, so the completeness guard passes
    even when the capture only surfaced a window of the season (#761). An empty
    ``existing`` (fresh season / first run) just returns the captured rows.
    """
    import pandas as pd

    if existing is None or existing.empty:
        return captured.reset_index(drop=True)
    existing = existing.reindex(columns=captured.columns)
    return (
        pd.concat([existing, captured], ignore_index=True)
        .drop_duplicates(subset='game_id', keep='last')
        .reset_index(drop=True)
    )


def _read_existing_partition(table: str, league: str, season: str):
    """Read an existing ``bronze.<table>`` (league, season) partition into a
    DataFrame so freshly-captured rows can be MERGED with it before a
    ``replace_partitions`` save (#842 incremental match_capture — generalises
    ``_read_existing_schedule`` #761). Returns an EMPTY DataFrame when the
    table/partition is missing or Trino is unreachable — the caller then saves
    the captured rows as-is, and the completeness guard still protects a
    non-empty partition from being replaced by a partial frame.
    """
    import pandas as pd

    empty = pd.DataFrame()
    conn = _trino_connect()
    if conn is None:
        return empty
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM iceberg.bronze.{table} "
            "WHERE league = ? AND CAST(season AS varchar) = ?",
            (league, season),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        logger.warning(
            "Could not read existing bronze.%s partition (league=%s "
            "season=%s): %s — saving captured rows as-is.",
            table, league, season, e,
        )
        return empty


def _merge_match_partition(existing, captured, key: str):
    """Union an existing per-match partition with freshly-captured rows, keyed
    by ``key`` (``match_id``; ``game_id`` for venue) with the captured match
    winning wholesale — its existing rows are dropped before the concat so a
    re-captured match never carries stale partial rows. Same completeness-guard
    rationale as ``_merge_schedule_partition`` (#761): union >= existing, so the
    guard passes even though the capture only fetched the NEW matches (#842).
    """
    import pandas as pd

    if existing is None or existing.empty:
        return captured.reset_index(drop=True)
    captured_keys = set(captured[key].astype(str))
    existing = existing.reindex(columns=captured.columns)
    keep = existing[~existing[key].astype(str).isin(captured_keys)]
    return pd.concat([keep, captured], ignore_index=True)


def _filter_new_match_ids(
    match_ids: List[str],
    league: str,
    season_short: str,
    season_str: str,
) -> tuple:
    """#842 incremental match_capture: drop match_ids already materialised in
    ``bronze.sofascore_venue`` — the LAST table the capture pass writes (#847).
    Probing the first table (player_ratings) made a mid-save crash invisible:
    a Trino restart between saves left ratings/eps/stats committed and
    shotmap/venue missing (APL 16/17), and the plain rerun skipped the match —
    only ``--force-replace`` repaired it. Keying on the last table means a
    half-written match IS re-captured on rerun. Finished-match data is
    immutable, so a skipped match never needs re-capturing. Trade-off: a match
    whose event payload carried no venue at all is re-probed each run (rare,
    and bounded — the recapture is one match of proxy spend, vs a permanent
    data hole the other way round).

    Returns ``(new_ids, skipped_count)``. Probe failure / missing table →
    empty existing set → nothing skipped (first run captures everything).
    """
    existing = _existing_match_ids_in_bronze(
        'sofascore_venue', league, season_short, id_col='game_id')
    if not existing:
        existing = _existing_match_ids_in_bronze(
            'sofascore_venue', league, season_str, id_col='game_id')
    new_ids = [m for m in match_ids if str(m) not in existing]
    return new_ids, len(match_ids) - len(new_ids)


def _fallback_exit_code(reason: str) -> int:
    """Pick the runner exit code for a soft-fallback.

    An active block — SofaScore refused us (http_403/429/5xx) or a transport
    error — is a real failure → exit 1, which the DAG bash wrapper lets turn the
    task red (mirrors the ESPN/SoFIFA runners, #466). A genuinely empty result —
    no matches to scrape (``no_match_ids``) or an empty page with NO http error
    (``empty_payload``) — stays exit 2, mapped to a soft green by the wrapper so
    an off-season / no-fixtures day never fails the daily pipeline. (#790)
    """
    if reason and (reason.startswith('http_') or reason == 'transport_error'):
        return 1
    return 2


def _run_player_ratings(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    """R0.2b player-ratings entrypoint. Returns process exit code."""
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]  # ratings scrape is single-league per invocation
    # Schedule writer stores season as the soccerdata short form (e.g. "2526")
    season_str = str(season)
    # Use the canonical helper for BOTH resolve and label. The old inline
    # formula only shifted (start-year → short), while the scraper labels rows
    # via _season_to_short (which passes already-short tokens through). For a
    # short-form --season (e.g. 2122) the two diverged: resolve fetched the +1
    # season while the label stayed put, silently mislabelling the partition by
    # +1 (#888).
    season_short = _season_label(league, season_str)

    logger.info(
        "R0.2b player_ratings: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    # 1) Pre-resolve match_ids from bronze.sofascore_schedule — avoids a
    #    fresh schedule scrape on every run.
    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids:
        # try with int-form season too — just in case the writer used int
        match_ids = _resolve_match_ids_from_bronze(league, season_str, limit)

    if match_ids:
        logger.info("Resolved %d match_ids from bronze.sofascore_schedule",
                    len(match_ids))
    else:
        # bronze schedule is empty (e.g. fresh season — soccerdata schedule is
        # Turnstile-blocked). Defer to the Camoufox capture resolver inside the
        # scraper session below (#757 B2) before declaring R0.2B fallback.
        logger.warning(
            "bronze.sofascore_schedule empty for league=%s season=%s — will "
            "resolve finished match_ids via Camoufox capture (#757).",
            league, season_short,
        )

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 "
            "without residential proxy.", proxy_file,
        )
        proxy_file = None

    results = {
        'entity': ENTITY_PLAYER_RATINGS,
        'tables': [],
        'rows': 0,
        'matches_attempted': len(match_ids),
        'matches_with_ratings': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    try:
        with SofaScoreScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            if not match_ids:
                # #757 B2: discover finished match_ids via Camoufox capture when
                # bronze.sofascore_schedule is empty (Turnstile-blocked soccerdata).
                match_ids = scraper.resolve_finished_match_ids_via_capture(
                    league, int(season),
                )
                if not match_ids:
                    logger.error(
                        "%s: no match_ids from bronze OR capture for "
                        "league=%s season=%s.",
                        R0_2B_FALLBACK_MARKER, league, season_short,
                    )
                    results['fallback'] = True
                    results['fallback_reason'] = 'no_match_ids'
                    results['errors'].append(
                        f'{R0_2B_FALLBACK_MARKER}: no_match_ids'
                    )
                    _write_results(output_path, results)
                    return 2
                if limit:
                    match_ids = match_ids[: int(limit)]
                results['matches_attempted'] = len(match_ids)
                logger.info("Resolved %d finished match_ids via capture",
                            len(match_ids))

            df = scraper.read_player_ratings(
                league=league,
                season=int(season),
                match_ids=match_ids,
                limit=limit,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789

            if df is None or df.empty:
                # Look at scraper's last fetch error to classify the
                # fallback reason.
                last_err = getattr(scraper, '_last_lineup_error', None)
                reason = 'empty_payload'
                if last_err:
                    status = last_err.get('status')
                    if status == 403:
                        reason = 'http_403'
                    elif status == 429:
                        reason = 'http_429'
                    elif status is None:
                        reason = 'transport_error'
                    else:
                        reason = f'http_{status}'

                logger.error(
                    "%s: SofaScore ratings unavailable — reason=%s detail=%s",
                    R0_2B_FALLBACK_MARKER, reason, last_err,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(
                    f'{R0_2B_FALLBACK_MARKER}: {reason}'
                )
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='sofascore_player_ratings',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=(
                    None if force_replace else _MIN_REPLACE_RATIO
                ),
            )
            results['tables'].append(table_path)
            results['rows'] = int(len(df))
            results['matches_with_ratings'] = int(df['match_id'].nunique())
            logger.info(
                "Saved %d rating rows for %d matches -> %s",
                results['rows'], results['matches_with_ratings'], table_path,
            )

    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("player_ratings scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_match_capture(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    """#751 PR1+PR2 — consolidated per-match capture entrypoint.

    ONE Camoufox navigation per match feeds FOUR Bronze tables from the same
    captured ``/lineups`` + ``/event`` + ``/statistics`` + ``/shotmap`` payloads:
    ``sofascore_player_ratings``, ``sofascore_event_player_stats``,
    ``sofascore_match_stats``, ``sofascore_event_shotmap`` — replacing four
    separate Turnstile-blocked passes. The secondary tables come essentially
    free with the ratings capture (no per-player ``/player/{pid}/statistics``
    nor per-event ``/statistics`` REST calls). statistics/shotmap are
    best-effort — an empty frame is skipped.

    #842 incremental: matches already in ``bronze.sofascore_player_ratings``
    are skipped (finished-match data is immutable; re-capturing the whole
    season daily burned ~1.6 GB of residential proxy per run). Only the NEW
    matches are captured; each frame is then MERGED with its existing
    partition (union by ``match_id``/``game_id``, #761 pattern) so the
    ``replace_partitions=['league', 'season']`` save + completeness guard
    keep their full-state semantics. ``--force-replace`` restores the old
    full re-capture (and disarms the guard) for backfills/repairs.

    Exit codes: 0 ok (incl. the nothing-new no-op) / 2 R0.2B_FALLBACK
    (nothing captured) / 3 ReplaceGuard / 1 hard failure.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]
    season_str = str(season)
    # Use the canonical helper for BOTH resolve and label. The old inline
    # formula only shifted (start-year → short), while the scraper labels rows
    # via _season_to_short (which passes already-short tokens through). For a
    # short-form --season (e.g. 2122) the two diverged: resolve fetched the +1
    # season while the label stayed put, silently mislabelling the partition by
    # +1 (#888).
    season_short = _season_label(league, season_str)

    logger.info(
        "match_capture: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids:
        match_ids = _resolve_match_ids_from_bronze(league, season_str, limit)
    if match_ids:
        logger.info("Resolved %d match_ids from bronze.sofascore_schedule",
                    len(match_ids))
    else:
        logger.warning(
            "bronze.sofascore_schedule empty for league=%s season=%s — will "
            "resolve finished match_ids via Camoufox capture (#757).",
            league, season_short,
        )

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 without "
            "residential proxy.", proxy_file,
        )
        proxy_file = None

    results = {
        'entity': ENTITY_MATCH_CAPTURE,
        'tables': [],
        'rows': 0,                  # player_ratings rows (primary)
        'matches_with_ratings': 0,
        'eps_rows': 0,
        'eps_matches': 0,
        'match_stats_rows': 0,
        'match_stats_matches': 0,
        'shotmap_rows': 0,
        'shotmap_matches': 0,
        'venue_rows': 0,
        'venue_matches': 0,
        'matches_total': 0,             # resolved before skip-existing (#842)
        'matches_skipped_existing': 0,  # already in bronze → not re-captured
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    # #842 skip-existing: don't re-capture matches already in bronze. When
    # nothing is new (off-season / no fixtures since yesterday) exit 0 before
    # even opening the scraper session — zero proxy bytes spent.
    if match_ids and not force_replace:
        total = len(match_ids)
        match_ids, skipped = _filter_new_match_ids(
            match_ids, league, season_short, season_str)
        results['matches_total'] = total
        results['matches_skipped_existing'] = skipped
        if skipped:
            logger.info(
                "match_capture skip-existing: %d/%d matches already in "
                "bronze.sofascore_venue; capturing %d new.",
                skipped, total, len(match_ids),
            )
        if not match_ids:
            logger.info(
                "match_capture: all %d matches already captured — nothing "
                "to do, partitions left untouched.", total,
            )
            _write_results(output_path, results)
            return 0

    try:
        with SofaScoreScraper(
            leagues=[league], seasons=[season], proxy_file=proxy_file,
        ) as scraper:
            if not match_ids:
                match_ids = scraper.resolve_finished_match_ids_via_capture(
                    league, int(season),
                )
                if not match_ids:
                    logger.error(
                        "%s: no match_ids from bronze OR capture for "
                        "league=%s season=%s.",
                        R0_2B_FALLBACK_MARKER, league, season_short,
                    )
                    results['fallback'] = True
                    results['fallback_reason'] = 'no_match_ids'
                    results['errors'].append(
                        f'{R0_2B_FALLBACK_MARKER}: no_match_ids')
                    # #879: the capture-resolve above spent real camoufox
                    # bytes — report them even on this early exit.
                    results['traffic'] = scraper.get_traffic_stats()
                    _write_results(output_path, results)
                    return 2
                if limit:
                    match_ids = match_ids[: int(limit)]
                logger.info("Resolved %d finished match_ids via capture",
                            len(match_ids))
                # #842 skip-existing for the capture-resolved path too (bronze
                # schedule empty but ratings may still hold prior matches).
                if not force_replace:
                    total = len(match_ids)
                    match_ids, skipped = _filter_new_match_ids(
                        match_ids, league, season_short, season_str)
                    results['matches_total'] = total
                    results['matches_skipped_existing'] = skipped
                    if not match_ids:
                        logger.info(
                            "match_capture: all %d matches already captured "
                            "— nothing to do, partitions left untouched.",
                            total,
                        )
                        results['traffic'] = scraper.get_traffic_stats()  # #879
                        _write_results(output_path, results)
                        return 0

            frames = scraper.read_match_capture(
                league=league, season=int(season),
                match_ids=match_ids, limit=limit,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789 + #879 camoufox
            ratings_df = frames.get('player_ratings')
            eps_df = frames.get('event_player_stats')
            stats_df = frames.get('match_stats')
            shot_df = frames.get('event_shotmap')
            venue_df = frames.get('venue')
            ratings_empty = ratings_df is None or ratings_df.empty
            eps_empty = eps_df is None or eps_df.empty
            stats_empty = stats_df is None or stats_df.empty
            shot_empty = shot_df is None or shot_df.empty
            venue_empty = venue_df is None or venue_df.empty

            if ratings_empty and eps_empty:
                last_err = getattr(scraper, '_last_lineup_error', None)
                reason = 'empty_payload'
                if last_err:
                    status = last_err.get('status')
                    if status == 403:
                        reason = 'http_403'
                    elif status == 429:
                        reason = 'http_429'
                    elif status is None:
                        reason = 'transport_error'
                    else:
                        reason = f'http_{status}'
                logger.error(
                    "%s: SofaScore match_capture unavailable — reason=%s detail=%s",
                    R0_2B_FALLBACK_MARKER, reason, last_err,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            min_ratio = None if force_replace else _MIN_REPLACE_RATIO

            def _merged(df, table, key):
                """#842: the captured frame holds only NEW matches (skip-
                existing above) — union it with the existing partition so the
                replace_partitions save keeps prior matches and the guard
                passes. --force-replace = write the captured frame as-is."""
                if force_replace:
                    return df
                return _merge_match_partition(
                    _read_existing_partition(table, league, season_short),
                    df, key,
                )

            # player_ratings — full-state refresh (+ completeness guard), as the
            # standalone ratings entity does.
            if not ratings_empty:
                ratings_df = _merged(
                    ratings_df, 'sofascore_player_ratings', 'match_id')
                rpath = scraper.save_to_iceberg(
                    df=ratings_df,
                    table_name='sofascore_player_ratings',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(rpath)
                results['rows'] = int(len(ratings_df))
                results['matches_with_ratings'] = int(
                    ratings_df['match_id'].nunique())
                logger.info("Saved %d rating rows -> %s", results['rows'], rpath)

            # event_player_stats — same merged full-state refresh from the SAME
            # capture pass (#751; #842 merges instead of re-capturing).
            if not eps_empty:
                eps_df = _merged(
                    eps_df, 'sofascore_event_player_stats', 'match_id')
                epath = scraper.save_to_iceberg(
                    df=eps_df,
                    table_name='sofascore_event_player_stats',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(epath)
                results['eps_rows'] = int(len(eps_df))
                results['eps_matches'] = int(eps_df['match_id'].nunique())
                logger.info("Saved %d eps rows -> %s", results['eps_rows'], epath)

            # match_stats — same merged full-state refresh from the SAME
            # capture pass (#751 PR2; #842 merges instead of re-capturing).
            if not stats_empty:
                stats_df = _merged(
                    stats_df, 'sofascore_match_stats', 'match_id')
                spath = scraper.save_to_iceberg(
                    df=stats_df,
                    table_name='sofascore_match_stats',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(spath)
                results['match_stats_rows'] = int(len(stats_df))
                results['match_stats_matches'] = int(stats_df['match_id'].nunique())
                logger.info("Saved %d match_stats rows -> %s",
                            results['match_stats_rows'], spath)

            # event_shotmap — written LAST: the shotmap tab is the flakiest XHR,
            # so if its completeness guard trips (exit 3) the other three tables
            # are already committed.
            if not shot_empty:
                shot_df = _merged(
                    shot_df, 'sofascore_event_shotmap', 'match_id')
                shpath = scraper.save_to_iceberg(
                    df=shot_df,
                    table_name='sofascore_event_shotmap',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(shpath)
                results['shotmap_rows'] = int(len(shot_df))
                results['shotmap_matches'] = int(shot_df['match_id'].nunique())
                logger.info("Saved %d shotmap rows -> %s",
                            results['shotmap_rows'], shpath)

            # venue (#753) — one row per match from the SAME capture pass;
            # full-state refresh like the others. Best-effort: empty when the
            # event payload carried no venue.
            if not venue_empty:
                venue_df = _merged(venue_df, 'sofascore_venue', 'game_id')
                vpath = scraper.save_to_iceberg(
                    df=venue_df,
                    table_name='sofascore_venue',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(vpath)
                results['venue_rows'] = int(len(venue_df))
                results['venue_matches'] = int(venue_df['game_id'].nunique())
                logger.info("Saved %d venue rows -> %s",
                            results['venue_rows'], vpath)

    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("match_capture scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_player_capture(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    """#751 PR3 + PR3b — per-player capture entrypoint (profile + season stats).

    ONE Camoufox navigation per player writes ``sofascore_player_profile`` (bio
    SSR'd in ``__NEXT_DATA__``) AND ``sofascore_player_season_stats`` (the target
    competition's season-aggregate stats, captured by driving the Season tab +
    season-picker) — replacing the dead Turnstile-blocked tls passes. Both are
    full-state (``replace_partitions=['league', 'season']`` + completeness
    guard): every run re-captures the player universe and rewrites the partition.

    Season-stats is secondary: it can be a strict subset of profile (the picker
    misses for some transferred players), so its save is skipped (not a fallback)
    when empty — profile still succeeds. The DAG row-floor WARNs on low coverage.

    Exit codes: 0 ok / 2 R0.2B_FALLBACK (no profile captured) / 3 ReplaceGuard /
    1 hard failure.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]
    season_str = str(season)
    # Use the canonical helper for BOTH resolve and label. The old inline
    # formula only shifted (start-year → short), while the scraper labels rows
    # via _season_to_short (which passes already-short tokens through). For a
    # short-form --season (e.g. 2122) the two diverged: resolve fetched the +1
    # season while the label stayed put, silently mislabelling the partition by
    # +1 (#888).
    season_short = _season_label(league, season_str)

    logger.info(
        "player_capture: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 without "
            "residential proxy.", proxy_file,
        )
        proxy_file = None

    results = {
        'entity': ENTITY_PLAYER_CAPTURE,
        'tables': [],
        'rows': 0,                  # player_profile rows
        'profile_players': 0,
        'season_stats_rows': 0,     # player_season_stats rows (#751 PR3b)
        'season_stats_players': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    try:
        with SofaScoreScraper(
            leagues=[league], seasons=[season], proxy_file=proxy_file,
        ) as scraper:
            frames = scraper.read_player_capture(
                league=league, season=int(season), limit=limit,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789 + #879 camoufox
            profile_df = frames.get('player_profile')
            profile_empty = profile_df is None or profile_df.empty

            if profile_empty:
                last_err = getattr(scraper, '_last_lineup_error', None)
                reason = 'empty_payload'
                if last_err:
                    status = last_err.get('status')
                    if status == 403:
                        reason = 'http_403'
                    elif status == 429:
                        reason = 'http_429'
                    elif status is None:
                        reason = 'transport_error'
                    else:
                        reason = f'http_{status}'
                logger.error(
                    "%s: SofaScore player_capture unavailable — reason=%s detail=%s",
                    R0_2B_FALLBACK_MARKER, reason, last_err,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            min_ratio = None if force_replace else _MIN_REPLACE_RATIO

            # player_profile — full-state refresh (+ completeness guard).
            ppath = scraper.save_to_iceberg(
                df=profile_df,
                table_name='sofascore_player_profile',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=min_ratio,
            )
            results['tables'].append(ppath)
            results['rows'] = int(len(profile_df))
            results['profile_players'] = int(profile_df['player_id'].nunique())
            logger.info("Saved %d player_profile rows -> %s",
                        results['rows'], ppath)

            # player_season_stats — secondary (#751 PR3b, Season-tab picker). A
            # strict subset of profile (the picker can miss for transferred /
            # multi-competition players) → a WARN floor in the DAG, not a hard
            # fail. Skip the save entirely when empty so an off day doesn't wipe
            # a good partition (don't fall back — profile already succeeded).
            season_df = frames.get('player_season_stats')
            if season_df is not None and not season_df.empty:
                spath = scraper.save_to_iceberg(
                    df=season_df,
                    table_name='sofascore_player_season_stats',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(spath)
                results['season_stats_rows'] = int(len(season_df))
                results['season_stats_players'] = int(
                    season_df['player_id'].nunique())
                logger.info("Saved %d player_season_stats rows -> %s",
                            results['season_stats_rows'], spath)
            else:
                logger.warning(
                    "player_season_stats empty (Season-tab picker captured no "
                    "%s overall) — skipping save; profile still written.", league)

    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("player_capture scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_event_endpoint(
    *,
    entity: str,
    table_name: str,
    scraper_method: str,
    pk_col: str,
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    extra_kwargs: Optional[dict] = None,
) -> int:
    """Generic event-grain runner: shotmap, event_player_stats, match_stats.

    Flow:

    1. Resolve finished match_ids from ``bronze.sofascore_schedule``.
    2. Skip match_ids already in ``bronze.<table_name>`` (issue #69).
    3. Loop over remaining matches, call ``scraper.<scraper_method>(...)``.
    4. Write to ``iceberg.bronze.<table_name>`` in APPEND mode
       (delta-only; replace_partitions is unsafe here).
    5. Return exit code 2 on empty payload (R0.2B_FALLBACK semantics).

    ``extra_kwargs`` is forwarded to the scraper method (e.g.
    ``player_ids`` for event_player_stats).
    """
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]
    season_str = str(season)
    # Use the canonical helper for BOTH resolve and label. The old inline
    # formula only shifted (start-year → short), while the scraper labels rows
    # via _season_to_short (which passes already-short tokens through). For a
    # short-form --season (e.g. 2122) the two diverged: resolve fetched the +1
    # season while the label stayed put, silently mislabelling the partition by
    # +1 (#888).
    season_short = _season_label(league, season_str)

    logger.info(
        "%s: league=%s season=%s (short=%s) limit=%s",
        entity, league, season, season_short, limit,
    )

    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids:
        match_ids = _resolve_match_ids_from_bronze(league, season_str, limit)

    results = {
        'entity': entity,
        'tables': [],
        'rows': 0,
        'matches_attempted': len(match_ids),
        'matches_with_rows': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    if not match_ids:
        logger.error(
            "%s: no match_ids in bronze.sofascore_schedule for "
            "league=%s season=%s — run schedule scrape first.",
            R0_2B_FALLBACK_MARKER, league, season_short,
        )
        results['fallback'] = True
        results['fallback_reason'] = 'no_match_ids_in_bronze'
        results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: no_match_ids')
        _write_results(output_path, results)
        return 2

    # Skip-existing (#69): match_ids already in this endpoint's bronze
    # are immutable past-result data — refetching wastes the proxy budget.
    # First run (table absent) returns empty set → fetch all.
    existing = _existing_match_ids_in_bronze(table_name, league, season_short)
    if not existing:
        existing = _existing_match_ids_in_bronze(table_name, league, season_str)
    matches_total = len(match_ids)
    new_match_ids = [m for m in match_ids if str(m) not in existing]
    skipped = matches_total - len(new_match_ids)
    logger.info(
        "%s skip-existing: %d/%d matches already in bronze.%s; fetching %d new.",
        entity, skipped, matches_total, table_name, len(new_match_ids),
    )
    results['matches_skipped_existing'] = skipped
    results['matches_attempted'] = len(new_match_ids)

    if not new_match_ids:
        logger.info(
            "%s: no new match_ids to fetch (bronze.%s already covers all "
            "schedule matches for league=%s season=%s).",
            entity, table_name, league, season_short,
        )
        results['skipped_existing'] = True
        _write_results(output_path, results)
        return 0

    match_ids = new_match_ids

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 "
            "without residential proxy.", proxy_file,
        )
        proxy_file = None

    try:
        with SofaScoreScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            method = getattr(scraper, scraper_method)
            kwargs = {
                'league': league,
                'season': int(season),
                'match_ids': match_ids,
                'limit': limit,
            }
            kwargs.update(extra_kwargs or {})
            df = method(**kwargs)
            results['traffic'] = scraper.get_traffic_stats()  # #789

            if df is None or df.empty:
                last_err = getattr(scraper, '_last_endpoint_error', None)
                reason = 'empty_payload'
                if last_err:
                    status = last_err.get('status')
                    if status == 403:
                        reason = 'http_403'
                    elif status == 429:
                        reason = 'http_429'
                    elif status is None:
                        reason = 'transport_error'
                    else:
                        reason = f'http_{status}'

                logger.error(
                    "%s: %s unavailable — reason=%s detail=%s",
                    R0_2B_FALLBACK_MARKER, entity, reason, last_err,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            # Skip-existing guarantees the fetched DataFrame contains only
            # NEW match_ids (no overlap with bronze) → safe APPEND
            # without replace_partitions. Past matches in bronze are
            # preserved across runs. Issue #69.
            table_path = scraper.save_to_iceberg(
                df=df,
                table_name=table_name,
                partition_cols=['league', 'season'],
            )
            results['tables'].append(table_path)
            results['rows'] = int(len(df))
            if pk_col in df.columns:
                results['matches_with_rows'] = int(df[pk_col].nunique())
            logger.info(
                "Saved %d %s rows -> %s",
                results['rows'], entity, table_path,
            )

    except Exception as e:
        logger.error("%s scrape failed hard: %s", entity, e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_shotmap(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
) -> int:
    """#22 — per-shot xG / coords / situation. Reads finished match_ids
    from bronze.sofascore_schedule and writes to
    ``iceberg.bronze.sofascore_event_shotmap``.
    """
    return _run_event_endpoint(
        entity=ENTITY_SHOTMAP,
        table_name='sofascore_event_shotmap',
        scraper_method='read_shotmap',
        pk_col='match_id',
        leagues=leagues,
        season=season,
        limit=limit,
        output_path=output_path,
    )


def _run_event_player_stats(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
) -> int:
    """#21 — per-(match, player) Opta-rich stats. Reads
    ``(match_id, player_id)`` pairs from
    ``bronze.sofascore_player_ratings`` and writes to
    ``iceberg.bronze.sofascore_event_player_stats``.

    Note: ``limit`` is interpreted as *match count*, not request count.
    Each match averages ~25 played players; at 20 req/min that's
    roughly 75 seconds per match.
    """
    return _run_event_endpoint(
        entity=ENTITY_EVENT_PLAYER_STATS,
        table_name='sofascore_event_player_stats',
        scraper_method='read_event_player_stats',
        pk_col='match_id',
        leagues=leagues,
        season=season,
        limit=limit,
        output_path=output_path,
    )


def _run_match_stats(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
) -> int:
    """#25 — team-level per-(match, period, stat) statistics.
    One HTTP call per match; long-form rows so the Bronze table doesn't
    need re-shaping when SofaScore introduces a new metric.
    """
    return _run_event_endpoint(
        entity=ENTITY_MATCH_STATS,
        table_name='sofascore_match_stats',
        scraper_method='read_match_stats',
        pk_col='match_id',
        leagues=leagues,
        season=season,
        limit=limit,
        output_path=output_path,
    )


def _write_results(path: str, payload: dict) -> None:
    """Persist runner results to disk for Airflow XCom pickup."""
    try:
        with open(path, 'w') as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        logger.warning("Could not write results to %s: %s", path, e)
    # Also dump to stdout so BashOperator XCom can capture it.
    try:
        print(json.dumps(payload, default=str))
    except Exception:
        pass
    # Residential-proxy traffic per-run log + ops persist (#789 Phase 2). One
    # grep-friendly "PROXY_TRAFFIC source=sofascore total=… MB" line + one row in
    # iceberg.ops.proxy_traffic_runs. Counts the tls REST path AND, since #879,
    # the Camoufox capture sessions (rx+tx) — still a slight lower bound (the
    # tls share counts response bodies only). Passive — never fails the run.
    traffic = payload.get('traffic')
    if isinstance(traffic, dict) and traffic:
        try:
            from utils.proxy_traffic import (
                log_traffic_summary,
                record_traffic_run,
                summarize_result_traffic,
            )
            summary = summarize_result_traffic('sofascore', traffic)
            log_traffic_summary(summary)
            record_traffic_run(
                summary,
                dag_run_id=os.environ.get('AIRFLOW_CTX_DAG_RUN_ID', ''),
            )
        except Exception as e:  # noqa: BLE001 — logging must not fail the run
            logger.warning("proxy-traffic log failed: %s", e)


def _run_legacy(
    leagues: List[str],
    season: int,
    output_path: str,
    force_replace: bool = False,
) -> int:
    """Original behaviour: scrape schedule + league_table."""
    results = {
        'entity': 'all',
        'tables': [],
        'schedule_rows': 0,
        'league_table_rows': 0,
        'errors': [],
    }
    guard_refused = False

    try:
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.sofascore import SofaScoreScraper

        # read_schedule now captures via Camoufox (#761), which needs the
        # residential proxy or SofaScore Turnstile-403s every event.
        proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
        if not os.path.exists(proxy_file):
            logger.warning(
                "Proxy file %s not found — SofaScore schedule capture is likely "
                "to 403 without a residential proxy.", proxy_file,
            )
            proxy_file = None

        with SofaScoreScraper(
            leagues=leagues, seasons=[season], proxy_file=proxy_file,
        ) as scraper:
            try:
                import pandas as pd

                captured = scraper.read_schedule()
                if captured is not None and not captured.empty:
                    # Merge the captured window with the existing partition so a
                    # partial capture never shrinks it (#761). Per (league,
                    # season): union by game_id, captured row wins (fresh
                    # scores). union >= existing → the completeness guard passes.
                    parts = []
                    for (lg, sea), grp in captured.groupby(
                        ['league', 'season'], sort=False,
                    ):
                        existing = _read_existing_schedule(lg, str(sea))
                        parts.append(_merge_schedule_partition(existing, grp))
                    df = pd.concat(parts, ignore_index=True)

                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofascore_schedule',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['schedule_rows'] = len(df)
                    logger.info(
                        "Saved %d schedule rows (captured %d, merged with "
                        "existing partition)", len(df), len(captured),
                    )
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: schedule: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Schedule scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            try:
                df = scraper.read_league_table()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofascore_league_table',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['league_table_rows'] = len(df)
                    logger.info(f"Saved {len(df)} league table rows")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: league_table: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"League table scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            results['traffic'] = scraper.get_traffic_stats()  # #879 camoufox bytes
    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    # Exit 3 when the guard refused any save (distinct from the exit-0 path) so
    # an operator can spot a refused guard in the BashOperator (#583).
    return 3 if guard_refused else 0


def main():
    parser = _StrictArgumentParser(description='Run SofaScore scraper')
    parser.add_argument(
        '--entity',
        type=str,
        default='all',
        help=(
            'Which entity to scrape. One of: schedule, league_table, '
            'player_ratings, all (default: all -> schedule + league_table). '
            'player_ratings is R0.2b extension.'
        ),
    )
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues',
    )
    # Keep legacy --league alias (singular) for player_ratings clarity.
    parser.add_argument(
        '--league',
        type=str,
        default=None,
        help='Single league override (used for player_ratings)',
    )
    parser.add_argument(
        '--season',
        type=int,
        default=2024,
        help='Season year (e.g. 2024 for 24-25, 2526 for 25-26 short)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Smoke-test cap: max number of matches to fetch ratings for',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/sofascore_result.json',
        help='Output file for results',
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.',
    )
    try:
        args = parser.parse_args()
    except _ArgparseError as exc:
        logger.error("Invalid CLI arguments: %s — failing hard (not a fallback)", exc)
        return 1

    if args.league:
        leagues = [args.league]
    else:
        leagues = [l.strip() for l in args.leagues.split(',')]

    # #920 bridge: single_year tournaments must never inherit the club-formula
    # season (July 2026 -> 2025) — the sid resolve would no-op every daily run
    # while the tournament is live. Mixed club+WC calls can't carry two
    # seasons -> WC is dropped (dedicated call), as in the other runners.
    if 'INT-World Cup' in leagues:
        from utils.medallion_config import get_active_single_year_season
        _wc_season = get_active_single_year_season('INT-World Cup')
        if len(leagues) > 1:
            logger.warning(
                "INT-World Cup dropped from mixed call (needs its own season; "
                "leagues=%s). Scrape it with --league 'INT-World Cup'.", leagues)
            leagues = [l for l in leagues if l != 'INT-World Cup']
        elif _wc_season is None:
            logger.warning(
                "INT-World Cup is out of its tournament window — nothing to "
                "scrape; exiting 0.")
            _write_results(args.output, {'entity': args.entity, 'tables': [],
                                         'errors': [],
                                         'skipped': 'out_of_window'})
            return 0
        elif int(args.season) != int(_wc_season):
            logger.info(
                "INT-World Cup: overriding --season %s -> %s (active "
                "single_year season, #920 bridge).", args.season, _wc_season)
            args.season = _wc_season

    entity = args.entity.lower()
    if entity not in VALID_ENTITIES and entity != 'all':
        logger.error(
            "Invalid --entity %s. Must be one of %s or 'all'.",
            entity, sorted(VALID_ENTITIES),
        )
        return 1

    logger.info(
        "Starting SofaScore scraper: entity=%s leagues=%s season=%s limit=%s",
        entity, leagues, args.season, args.limit,
    )

    if entity == ENTITY_MATCH_CAPTURE:
        return _run_match_capture(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
        )

    if entity == ENTITY_PLAYER_CAPTURE:
        return _run_player_capture(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
        )

    if entity == ENTITY_PLAYER_RATINGS:
        return _run_player_ratings(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
        )

    if entity == ENTITY_SHOTMAP:
        return _run_shotmap(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
        )

    if entity == ENTITY_EVENT_PLAYER_STATS:
        return _run_event_player_stats(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
        )

    if entity == ENTITY_MATCH_STATS:
        return _run_match_stats(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
        )

    # Default: legacy schedule+league_table flow.
    return _run_legacy(
        leagues=leagues,
        season=args.season,
        output_path=args.output,
        force_replace=args.force_replace,
    )


if __name__ == '__main__':
    sys.exit(main())
