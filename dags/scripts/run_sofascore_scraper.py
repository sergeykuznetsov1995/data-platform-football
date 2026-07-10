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
# #751 PR1 — consolidated per-match capture: one warmed browser session feeds
# both player_ratings and event_player_stats from the same /lineups (+/event).
ENTITY_MATCH_CAPTURE = 'match_capture'
# #751 PR3 + PR3b — one warmed session writes both player_profile and exact
# target-season player_season_stats. Replaces the blocked tls passes.
ENTITY_PLAYER_CAPTURE = 'player_capture'
_MATCH_CAPTURE_STATUS_TABLE = "sofascore_match_capture_status"

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

    A missing table is a legitimate first run and returns an empty set. Other
    Trino errors are raised: treating storage unavailability as "no rows" can
    turn a local hiccup into a full-season residential-proxy re-capture.
    """
    conn = _trino_connect()
    if conn is None:
        raise RuntimeError("Trino unavailable during skip-existing probe")
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
        message = str(e).upper()
        if any(
            marker in message
            for marker in (
                "TABLE_NOT_FOUND",
                "TABLE NOT FOUND",
                "DOES NOT EXIST",
            )
        ):
            logger.info("bronze.%s does not exist yet — first capture.", table)
            return set()
        raise RuntimeError(f"skip-existing probe on bronze.{table} failed: {e}") from e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _existing_complete_capture_ids(
    league: str,
    season: str,
) -> Optional[set]:
    """Read completed match ids from the endpoint-status manifest.

    ``None`` means the manifest table has not been created yet and activates the
    one-time legacy seed path. An empty set means the table exists but this
    partition has no completed matches. Operational errors fail closed.
    """
    conn = _trino_connect()
    if conn is None:
        raise RuntimeError("Trino unavailable during capture-manifest probe")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CAST(match_id AS varchar), capture_complete "
            f"FROM iceberg.bronze.{_MATCH_CAPTURE_STATUS_TABLE} "
            "WHERE league = ? AND CAST(season AS varchar) = ?",
            (league, season),
        )
        rows = cur.fetchall()
        if not rows:
            # Table exists, but this partition has never been migrated.
            return None
        return {
            str(row[0])
            for row in rows
            if row and row[0] is not None and bool(row[1])
        }
    except Exception as exc:
        message = str(exc).upper()
        if any(
            marker in message
            for marker in ("TABLE_NOT_FOUND", "TABLE NOT FOUND", "DOES NOT EXIST")
        ):
            return None
        raise RuntimeError(f"capture-manifest probe failed: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _resolve_match_ids_from_bronze(
    league: str,
    season: str,
    limit: Optional[int],
) -> List[str]:
    """Pull finished match ids straight from ``bronze.sofascore_schedule``.

    Avoids re-hitting SofaScore for the schedule when we already have a
    fresh copy in the lakehouse. Returns ``[]`` only when the table is missing
    or the partition is genuinely empty. Operational storage errors are raised
    so they cannot trigger a paid source rediscovery/full-season capture.
    """
    conn = _trino_connect()
    if conn is None:
        raise RuntimeError("Trino unavailable during schedule match-id probe")
    try:
        cur = conn.cursor()
        # Canonical post-#840 Bronze schema. A live event already has a score, so
        # completion MUST use SofaScore's status instead of score presence.
        # Referencing old+new names in COALESCE was not a schema bridge: Trino
        # resolves every identifier first and failed when either column was absent.
        sql = (
            "SELECT CAST(game_id AS varchar) AS gid "
            "FROM iceberg.bronze.sofascore_schedule "
            "WHERE league = ? AND CAST(season AS varchar) = ? "
            "  AND status_type = 'finished' "
            "ORDER BY start_timestamp DESC"
        )
        if limit:
            # Trino dialect: LIMIT goes in SQL; bind params don't bind it.
            sql = sql + f" LIMIT {int(limit)}"
        cur.execute(sql, (league, season))
        rows = cur.fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        message = str(e).upper()
        if any(
            marker in message
            for marker in ("TABLE_NOT_FOUND", "TABLE NOT FOUND", "DOES NOT EXIST")
        ):
            logger.info("bronze.sofascore_schedule does not exist yet.")
            return []
        raise RuntimeError(f"schedule match-id probe failed: {e}") from e
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    if "game_id" not in existing.columns:
        logger.warning(
            "Existing schedule partition lacks game_id; using captured frame only.",
        )
        return captured.reset_index(drop=True)
    captured_columns = set(captured.columns)
    columns = list(existing.columns) + [
        col for col in captured.columns if col not in existing.columns
    ]
    existing = existing.reindex(columns=columns)
    captured = captured.reindex(columns=columns)
    # When SofaScore omits an evolving field from the whole current window, keep
    # its last known value for overlapping games. Fresh columns still win when
    # they are actually present in the captured schema.
    absent_columns = [
        col
        for col in existing.columns
        if col not in captured_columns and col != "game_id"
    ]
    if absent_columns:
        prior = existing.drop_duplicates("game_id", keep="last").copy()
        prior.index = prior["game_id"].astype(str)
        for col in absent_columns:
            captured[col] = captured["game_id"].astype(str).map(prior[col])
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
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    if key not in existing.columns:
        logger.warning(
            "Existing partition lacks merge key %s; using captured frame only.",
            key,
        )
        return captured.reset_index(drop=True)
    captured_keys = set(captured[key].astype(str))
    captured_columns = set(captured.columns)
    absent_columns = [
        column
        for column in existing.columns
        if column not in captured_columns and column != key
    ]
    if absent_columns:
        common = set(existing.columns) & set(captured.columns)
        row_keys = [key]
        if "shot_id" in common:
            row_keys.append("shot_id")
        elif "player_id" in common:
            row_keys.append("player_id")
            if "team_side" in common:
                row_keys.append("team_side")
        elif {"period", "stat_group"} <= common:
            row_keys.extend(["period", "stat_group"])
            if "key" in common:
                row_keys.append("key")
            elif "name" in common:
                row_keys.append("name")

        # Preserve values only for columns absent from the entire fresh schema.
        # Temporary string keys avoid int/varchar mismatches from Trino frames.
        left = captured.copy()
        right = existing[row_keys + absent_columns].drop_duplicates(
            row_keys,
            keep="last",
        )
        temp_keys = []
        for index, row_key in enumerate(row_keys):
            temp = f"__merge_key_{index}"
            temp_keys.append(temp)
            left[temp] = left[row_key].astype(str)
            right[temp] = right[row_key].astype(str)
        prior_names = {
            column: f"__prior_{column}"
            for column in absent_columns
        }
        right = right[temp_keys + absent_columns].rename(columns=prior_names)
        left = left.merge(right, on=temp_keys, how="left")
        for column, prior in prior_names.items():
            left[column] = left[prior]
        captured = left.drop(columns=temp_keys + list(prior_names.values()))

    columns = list(existing.columns) + [
        col for col in captured.columns if col not in existing.columns
    ]
    existing = existing.reindex(columns=columns)
    captured = captured.reindex(columns=columns)
    keep = existing[~existing[key].astype(str).isin(captured_keys)]
    return pd.concat([keep, captured], ignore_index=True)


def _compatible_legacy_season_alias(
    league: str,
    season,
    season_short: str,
) -> Optional[str]:
    """Return a raw legacy partition only when it means the same season.

    Integer ``2023`` canonically means 23/24 and its old raw partition ``2023``
    is a safe alias. Integer ``2021`` canonically means 21/22, but string
    ``"2021"`` is the valid short token for 20/21, so probing it would mix two
    seasons and relabel old matches. Single-year tournaments remain literal.
    """
    from scrapers.sofascore.scraper import _season_label

    raw = str(season)
    if raw == season_short:
        return None
    if _season_label(league, raw) == season_short:
        return raw
    return None


def _filter_new_match_ids(
    match_ids: List[str],
    league: str,
    season_short: str,
    season_alias: Optional[str],
) -> tuple:
    """Filter by the endpoint-status manifest.

    Returns ``(new_ids, skipped_count, legacy_seed_ids, manifest_missing)``. On
    the first deploy,
    when the manifest table is absent, the old lineup-derived intersection seeds
    already completed matches without a one-time full-season re-download. New
    matches receive pending rows before source access; after that the manifest is
    authoritative and row-bearing data tables are never used as status flags.
    """
    manifest_ids = _existing_complete_capture_ids(league, season_short)
    if manifest_ids is not None:
        if season_alias:
            legacy_partition = _existing_complete_capture_ids(league, season_alias)
            if legacy_partition is not None:
                manifest_ids |= legacy_partition
        new_ids = [match_id for match_id in match_ids if str(match_id) not in manifest_ids]
        return new_ids, len(match_ids) - len(new_ids), set(), False

    # One-time compatibility seed for deployments that predate the manifest.
    probes = (
        ("sofascore_player_ratings", "match_id"),
        ("sofascore_event_player_stats", "match_id"),
        ("sofascore_match_stats", "match_id"),
        ("sofascore_event_shotmap", "match_id"),
        ("sofascore_venue", "game_id"),
    )
    completed_sets = []
    for table, id_col in probes:
        ids = _existing_match_ids_in_bronze(
            table,
            league,
            season_short,
            id_col=id_col,
        )
        if season_alias:
            ids |= _existing_match_ids_in_bronze(
                table,
                league,
                season_alias,
                id_col=id_col,
            )
        completed_sets.append(ids)
    existing = set.intersection(*completed_sets) if completed_sets else set()
    new_ids = [m for m in match_ids if str(m) not in existing]
    return new_ids, len(match_ids) - len(new_ids), existing, True


def _manifest_frame(
    scraper,
    *,
    pending_ids,
    complete_ids,
    league: str,
    season: str,
):
    """Build status rows for a manifest migration/preflight write."""
    import pandas as pd

    rows = []
    for match_id in sorted({str(value) for value in complete_ids}):
        rows.append(
            {
                "match_id": match_id,
                "event_status": "legacy_complete",
                "lineups_status": "legacy_complete",
                "statistics_status": "legacy_complete",
                "shotmap_status": "legacy_complete",
                "capture_complete": True,
                "league": league,
                "season": season,
            }
        )
    for match_id in sorted(
        {str(value) for value in pending_ids}
        - {str(value) for value in complete_ids}
    ):
        rows.append(
            {
                "match_id": match_id,
                "event_status": "pending",
                "lineups_status": "pending",
                "statistics_status": "pending",
                "shotmap_status": "pending",
                "capture_complete": False,
                "league": league,
                "season": season,
            }
        )
    frame = pd.DataFrame(rows)
    return scraper._add_metadata(frame, "match_capture_status")


def _prepare_capture_manifest(
    scraper,
    *,
    pending_ids,
    complete_ids,
    league: str,
    season: str,
) -> None:
    """Create the manifest before first source access, including pending rows."""
    frame = _manifest_frame(
        scraper,
        pending_ids=pending_ids,
        complete_ids=complete_ids,
        league=league,
        season=season,
    )
    if frame.empty:
        return
    scraper.save_to_iceberg(
        df=frame,
        table_name=_MATCH_CAPTURE_STATUS_TABLE,
        partition_cols=["league", "season"],
        replace_partitions=["league", "season"],
        min_replace_ratio=None,
    )


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
    # Preserve the integer start-year contract while keeping single-year
    # competitions on their literal partition (for example WC 2026 -> 2026).
    season_short = _season_label(league, season)
    season_alias = _compatible_legacy_season_alias(
        league,
        season,
        season_short,
    )

    logger.info(
        "R0.2b player_ratings: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    # 1) Pre-resolve match_ids from bronze.sofascore_schedule — avoids a
    #    fresh schedule scrape on every run.
    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids and season_alias:
        # try with int-form season too — just in case the writer used int
        match_ids = _resolve_match_ids_from_bronze(league, season_alias, limit)

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
    scraper = None
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
                    results['traffic'] = scraper.get_traffic_stats()
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
        if scraper is not None:
            results['traffic'] = scraper.get_traffic_stats()
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

    One warmed Camoufox session feeds five data tables plus a status manifest
    from the same captured
    ``/lineups`` + ``/event`` + ``/statistics`` + ``/shotmap`` payloads:
    ``sofascore_player_ratings``, ``sofascore_event_player_stats``,
    ``sofascore_match_stats``, ``sofascore_event_shotmap`` and
    ``sofascore_venue`` — replacing separate
    separate Turnstile-blocked passes. The secondary tables come essentially
    free with the ratings capture (no per-player ``/player/{pid}/statistics``
    nor per-event ``/statistics`` REST calls). statistics/shotmap are
    best-effort — an empty frame is skipped.

    #842 incremental: the endpoint-status manifest skips terminally completed
    matches (finished-match data is immutable; re-capturing the whole season
    daily burned ~1.6 GB of residential proxy per run). It distinguishes valid
    empty/404 answers from retryable misses. Only incomplete/new matches are
    captured; each frame is merged with its existing partition.
    ``--force-replace`` restores a deliberate full re-capture for repairs.

    Exit codes: 0 ok (incl. the nothing-new no-op) / 2 R0.2B_FALLBACK
    (nothing captured) / 3 ReplaceGuard / 1 hard failure.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]
    season_short = _season_label(league, season)
    season_alias = _compatible_legacy_season_alias(
        league,
        season,
        season_short,
    )

    logger.info(
        "match_capture: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    try:
        match_ids = _resolve_match_ids_from_bronze(league, season_short, None)
        if not match_ids and season_alias:
            match_ids = _resolve_match_ids_from_bronze(league, season_alias, None)
    except Exception as exc:
        message = f"Schedule match-id probe failed: {exc}"
        logger.error(message)
        _write_results(
            output_path,
            {
                "entity": ENTITY_MATCH_CAPTURE,
                "tables": [],
                "errors": [message],
                "fallback": False,
            },
        )
        return 1
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
        'capture_status_rows': 0,
        'matches_complete': 0,
        'matches_total': 0,             # resolved before skip-existing (#842)
        'matches_skipped_existing': 0,  # already in bronze → not re-captured
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }
    legacy_seed_ids = set()
    manifest_missing = False
    pending_manifest_ids = []

    # #842 skip-existing: don't re-capture manifest-complete matches. When
    # nothing is new (off-season / no fixtures since yesterday) exit 0 before
    # even opening the scraper session — zero proxy bytes spent.
    if match_ids and not force_replace:
        total = len(match_ids)
        try:
            match_ids, skipped, legacy_seed_ids, manifest_missing = _filter_new_match_ids(
                match_ids, league, season_short, season_alias
            )
        except Exception as exc:
            msg = f"Local completion-state probe failed: {exc}"
            logger.error(msg)
            results["errors"].append(msg)
            _write_results(output_path, results)
            return 1
        results["matches_total"] = total
        results["matches_skipped_existing"] = skipped
        pending_manifest_ids = list(match_ids)
        if limit:
            match_ids = match_ids[: int(limit)]
        if skipped:
            logger.info(
                "match_capture skip-existing: %d/%d matches complete in the "
                "status manifest; capturing %d new.",
                skipped, total, len(match_ids),
            )
        if not match_ids and not manifest_missing:
            logger.info(
                "match_capture: all %d matches already captured — nothing "
                "to do, partitions left untouched.", total,
            )
            _write_results(output_path, results)
            return 0
    elif match_ids and limit:
        match_ids = match_ids[: int(limit)]

    scraper = None
    try:
        with SofaScoreScraper(
            leagues=[league], seasons=[season], proxy_file=proxy_file,
        ) as scraper:
            if manifest_missing:
                _prepare_capture_manifest(
                    scraper,
                    pending_ids=pending_manifest_ids,
                    complete_ids=legacy_seed_ids,
                    league=league,
                    season=season_short,
                )
                if not match_ids:
                    logger.info(
                        "Seeded completion manifest for %d legacy matches; "
                        "nothing new to capture.",
                        len(legacy_seed_ids),
                    )
                    _write_results(output_path, results)
                    return 0
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
                logger.info("Resolved %d finished match_ids via capture",
                            len(match_ids))
                # #842 skip-existing for the capture-resolved path too (bronze
                # schedule empty but ratings may still hold prior matches).
                if not force_replace:
                    total = len(match_ids)
                    (
                        match_ids,
                        skipped,
                        legacy_seed_ids,
                        manifest_missing,
                    ) = _filter_new_match_ids(
                        match_ids,
                        league,
                        season_short,
                        season_alias,
                    )
                    results['matches_total'] = total
                    results['matches_skipped_existing'] = skipped
                    pending_manifest_ids = list(match_ids)
                    if manifest_missing:
                        _prepare_capture_manifest(
                            scraper,
                            pending_ids=pending_manifest_ids,
                            complete_ids=legacy_seed_ids,
                            league=league,
                            season=season_short,
                        )
                    if limit:
                        match_ids = match_ids[: int(limit)]
                    if not match_ids:
                        logger.info(
                            "match_capture: all %d matches already captured "
                            "— nothing to do, partitions left untouched.",
                            total,
                        )
                        results['traffic'] = scraper.get_traffic_stats()  # #879
                        _write_results(output_path, results)
                        return 0
                elif limit:
                    match_ids = match_ids[: int(limit)]

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
            status_df = frames.get('capture_status')
            ratings_empty = ratings_df is None or ratings_df.empty
            eps_empty = eps_df is None or eps_df.empty
            stats_empty = stats_df is None or stats_df.empty
            shot_empty = shot_df is None or shot_df.empty
            venue_empty = venue_df is None or venue_df.empty
            if status_df is None or status_df.empty:
                # Backward-compatible seam for older/custom scraper builds. The
                # in-repo scraper always returns explicit endpoint states.
                status_df = _manifest_frame(
                    scraper,
                    pending_ids=match_ids,
                    complete_ids=[],
                    league=league,
                    season=season_short,
                )
            status_empty = status_df is None or status_df.empty
            all_status_terminal = not status_empty and bool(
                status_df['capture_complete'].fillna(False).astype(bool).all()
            )

            if ratings_empty and eps_empty and not all_status_terminal:
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
            elif ratings_empty and eps_empty:
                logger.warning(
                    "All %d match captures are terminally empty/not available; "
                    "committing status without fallback.",
                    len(status_df),
                )

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

            # event_shotmap — best-effort optional payload, saved before the
            # lineup-derived completion markers below.
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

            # Save the two lineup-derived data tables after optional payloads;
            # the explicit status manifest below is the only completion marker.
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

            # Ratings remains the primary data table, but is not used as status.
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

            # Endpoint-status manifest is the final commit. Complete terminal
            # answers (including optional 404/empty JSON) are skipped forever;
            # transient misses and interrupted saves remain eligible for retry.
            if not status_empty:
                status_df = _merged(
                    status_df,
                    _MATCH_CAPTURE_STATUS_TABLE,
                    'match_id',
                )
                cpath = scraper.save_to_iceberg(
                    df=status_df,
                    table_name=_MATCH_CAPTURE_STATUS_TABLE,
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=min_ratio,
                )
                results['tables'].append(cpath)
                results['capture_status_rows'] = int(len(status_df))
                results['matches_complete'] = int(
                    status_df.loc[
                        status_df['capture_complete'].fillna(False).astype(bool),
                        'match_id',
                    ].nunique()
                )

    except ReplaceGuardError as e:
        if scraper is not None:
            results['traffic'] = scraper.get_traffic_stats()
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        if scraper is not None:
            results['traffic'] = scraper.get_traffic_stats()
        logger.error("match_capture scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    if results['fallback']:
        return _fallback_exit_code(results['fallback_reason'])
    return 0


def _run_player_capture(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    """#751 PR3 + PR3b — per-player capture entrypoint (profile + season stats).

    One navigation warms each bounded browser session. It writes
    ``sofascore_player_profile`` plus the exact target competition/season stats;
    later players use same-origin JSON fetches instead of another SPA load. Both
    outputs are full-state (``replace_partitions=['league', 'season']`` plus the
    completeness guard).

    Season stats are secondary and may be a strict subset of profiles when the
    target competition exposes no aggregate. An empty stats frame is skipped
    without discarding valid profiles; the DAG row-floor warns on low coverage.

    Exit codes: 0 ok / 2 R0.2B_FALLBACK (no profile captured) / 3 ReplaceGuard /
    1 hard failure.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER, _season_label

    league = leagues[0]
    season_short = _season_label(league, season)

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

            # player_season_stats — secondary (#751 PR3b). A player may expose no
            # aggregate for the exact target competition/season, so coverage is
            # a WARN floor in the DAG rather than a hard failure. Skip an empty
            # save so an off day cannot wipe a good partition.
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
                    "player_season_stats empty (no exact %s season aggregate) — "
                    "skipping save; profile still written.", league)

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
    season_short = _season_label(league, season)
    season_alias = _compatible_legacy_season_alias(
        league,
        season,
        season_short,
    )

    logger.info(
        "%s: league=%s season=%s (short=%s) limit=%s",
        entity, league, season, season_short, limit,
    )

    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids and season_alias:
        match_ids = _resolve_match_ids_from_bronze(league, season_alias, limit)

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
    if not existing and season_alias:
        existing = _existing_match_ids_in_bronze(
            table_name,
            league,
            season_alias,
        )
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
    entity: str = "all",
) -> int:
    """Scrape schedule and/or league table.

    The default daily path requests both through one tournament snapshot. An
    explicit ``--entity schedule`` or ``league_table`` only calls that reader;
    the old dispatch silently fetched both regardless of the CLI selection.
    """
    want_schedule = entity in ("all", ENTITY_SCHEDULE)
    want_table = entity in ("all", ENTITY_LEAGUE_TABLE)
    results = {
        "entity": entity,
        "tables": [],
        "schedule_rows": 0,
        "league_table_rows": 0,
        "errors": [],
    }
    guard_refused = False
    source_failed = False

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
            captured = None
            league_table = None
            try:
                if want_schedule and want_table:
                    captured, league_table = scraper.read_tournament_snapshot()
                elif want_schedule:
                    captured = scraper.read_schedule()
                elif want_table:
                    league_table = scraper.read_league_table()
            except Exception as e:
                error_msg = f"Tournament source capture failed: {e}"
                logger.error(error_msg, exc_info=True)
                results["errors"].append(error_msg)
                source_failed = True

            if want_schedule:
                try:
                    import pandas as pd

                    if captured is not None and not captured.empty:
                        # Merge the captured window with the existing partition so a
                        # partial capture never shrinks it (#761). Per (league,
                        # season): union by game_id, captured row wins (fresh
                        # scores). union >= existing → the completeness guard passes.
                        parts = []
                        for (lg, sea), grp in captured.groupby(
                            ["league", "season"],
                            sort=False,
                        ):
                            existing = _read_existing_schedule(lg, str(sea))
                            parts.append(_merge_schedule_partition(existing, grp))
                        df = pd.concat(parts, ignore_index=True)

                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name="sofascore_schedule",
                            partition_cols=["league", "season"],
                            replace_partitions=["league", "season"],
                            min_replace_ratio=(
                                None if force_replace else _MIN_REPLACE_RATIO
                            ),
                        )
                        results["tables"].append(table_path)
                        results["schedule_rows"] = len(df)
                        logger.info(
                            "Saved %d schedule rows (captured %d, merged with "
                            "existing partition)",
                            len(df),
                            len(captured),
                        )
                    else:
                        msg = "Schedule capture returned no rows"
                        logger.error(msg)
                        results["errors"].append(msg)
                        source_failed = True
                except ReplaceGuardError as e:
                    msg = f"{REPLACE_GUARD_MARKER}: schedule: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)
                    guard_refused = True
                except Exception as e:
                    error_msg = f"Schedule scraping failed: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    source_failed = True

            if want_table:
                try:
                    df = league_table
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name="sofascore_league_table",
                            partition_cols=["league", "season"],
                            replace_partitions=["league", "season"],
                            min_replace_ratio=(
                                None if force_replace else _MIN_REPLACE_RATIO
                            ),
                        )
                        results["tables"].append(table_path)
                        results["league_table_rows"] = len(df)
                        logger.info(f"Saved {len(df)} league table rows")
                    else:
                        msg = "League-table capture returned no rows"
                        logger.error(msg)
                        results["errors"].append(msg)
                        source_failed = True
                except ReplaceGuardError as e:
                    msg = f"{REPLACE_GUARD_MARKER}: league_table: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)
                    guard_refused = True
                except Exception as e:
                    error_msg = f"League table scraping failed: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    source_failed = True
            results["traffic"] = scraper.get_traffic_stats()  # #879 camoufox bytes
    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    # Exit 3 when the guard refused any save (distinct from the exit-0 path) so
    # an operator can spot a refused guard in the BashOperator (#583).
    if guard_refused:
        return 3
    return 1 if source_failed else 0


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
        leagues = [league.strip() for league in args.leagues.split(",")]

    # #920 bridge (generalized Phase 3: any single_year tournament):
    # tournaments must never inherit the club-formula season (July 2026 ->
    # 2025) — the sid resolve would no-op every daily run while the
    # tournament is live. Mixed calls can't carry two seasons ->
    # tournaments are dropped (dedicated call), as in the other runners.
    from utils.medallion_config import (
        get_active_season, is_single_year_competition,
    )
    _tournaments = [
        league for league in leagues if is_single_year_competition(league)
    ]
    if _tournaments and len(leagues) > 1:
        logger.warning(
            "Single-year tournaments %s dropped from mixed call (each needs "
            "its own season; leagues=%s). Scrape them with dedicated "
            "--league calls.", _tournaments, leagues)
        leagues = [league for league in leagues if league not in _tournaments]
        if not leagues:
            logger.warning(
                "No leagues left after dropping tournaments; exiting 0.")
            _write_results(args.output, {'entity': args.entity, 'tables': [],
                                         'errors': [],
                                         'skipped': 'mixed_tournaments_dropped'})
            return 0
    elif _tournaments:
        _t_league = leagues[0]
        _t_season = get_active_season(_t_league)
        if _t_season is None:
            logger.warning(
                "%s is out of its tournament window — nothing to "
                "scrape; exiting 0.", _t_league)
            _write_results(args.output, {'entity': args.entity, 'tables': [],
                                         'errors': [],
                                         'skipped': 'out_of_window'})
            return 0
        elif int(args.season) != int(_t_season):
            logger.info(
                "%s: overriding --season %s -> %s (active "
                "single_year season, #920 bridge).",
                _t_league, args.season, _t_season)
            args.season = _t_season

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
        entity=entity,
    )


if __name__ == '__main__':
    sys.exit(main())
