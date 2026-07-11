#!/usr/bin/env python3
"""
SofaScore Scraper Runner Script
===============================

Standalone script to run SofaScore scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supported entities:
- ``schedule``        : per-round schedule + final scores (default)
- ``league_table``    : standings snapshot
- ``match_capture``   : canonical raw-first per-event engine. One warmed
                       session jointly captures event, lineups/ratings,
                       player/team statistics, shotmap and incidents, then
                       incrementally merges every table by its natural key.
- ``player_ratings``, ``shotmap``, ``event_player_stats``, ``match_stats``:
                       compatibility aliases of ``match_capture``; they never
                       invoke a standalone endpoint runner.
- ``player_capture``  : shared profile and target-season statistics engine.

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
from datetime import datetime, timezone
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

# Keep the stable marker/exit code for writer DQ or replace-guard refusals. All
# live SofaScore paths now publish natural-keyed deltas through Iceberg MERGE;
# ``--force-replace`` means re-capture source endpoints, not rewrite a partition.
REPLACE_GUARD_MARKER = 'SOFASCORE_REPLACE_GUARD'


def _paid_capture_blocker(capture_runtime) -> str:
    """Explain why starting a paid browser would be unsafe for this runtime."""
    if capture_runtime.engine.budget is None:
        return (
            capture_runtime.budget_error
            or 'verified SofaScore provider-byte canary is unavailable'
        )
    return (
        'warmed Camoufox provider-meter token is not wired to the filtering '
        'proxy for every browser byte'
    )


def _season_freshness_key(
    capture_runtime,
    *,
    force_replace: bool,
    offline_replay: bool,
) -> str:
    key = (
        os.environ.get("SOFASCORE_SEASON_FRESHNESS_KEY", "").strip()
        or "day-" + datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    if force_replace and not offline_replay:
        return f"repair-{capture_runtime.engine.run_id}"
    return key


def _season_max_pages() -> int:
    value = int(os.environ.get("SOFASCORE_SEASON_MAX_PAGES", "50"))
    if value < 1:
        raise ValueError("SOFASCORE_SEASON_MAX_PAGES must be a positive integer")
    return value


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


def _source_context(league: str, season, canonical_season: str) -> tuple[int, int]:
    """Resolve registry-owned source IDs; never discover metadata via proxy."""
    from scrapers.sofascore.catalog import CatalogError, SofaScoreCatalog

    catalog = SofaScoreCatalog.load()
    tournament = catalog.competition(league)
    if not tournament.capture_allowed:
        raise CatalogError(f'{league} is not capture-eligible')
    source_season = None
    for token in (canonical_season, season):
        source_season = catalog.resolve_source_season(
            tournament.unique_tournament_id, token
        )
        if source_season is not None:
            break
    if source_season is None:
        raise CatalogError(
            f'{league} season {canonical_season!r} has no discovered SofaScore id'
        )
    return tournament.unique_tournament_id, source_season.season_id


def _complete_manifest_records_for_projection(
    manifest_store,
    endpoint_specs,
    pipeline_results,
    *,
    endpoints=('event', 'lineups', 'statistics', 'shotmap', 'incidents'),
):
    """Load every long endpoint state for each touched event.

    Endpoint resume returns only newly processed endpoints. Projecting that
    subset would overwrite the compatibility row with ``missing`` for endpoints
    that were already terminal, regressing ``capture_complete`` to false.
    """
    target_ids = sorted({
        result.manifest.key.target_id for result in pipeline_results
    })
    records = []
    missing = []
    for target_id in target_ids:
        for endpoint in endpoints:
            spec = endpoint_specs.get((target_id, endpoint))
            if spec is None:
                missing.append((target_id, endpoint, 'spec'))
                continue
            record = manifest_store.get(spec.key)
            if record is None:
                missing.append((target_id, endpoint, 'manifest'))
                continue
            records.append(record)
    if missing:
        raise RuntimeError(
            'compatibility projection lacks canonical endpoint states: '
            + repr(missing)
        )
    return records


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
                "incidents_status": "legacy_complete",
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
                "incidents_status": "pending",
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
        natural_keys=['league', 'season', 'match_id'],
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


def _materialize_endpoint_results(scraper, results, *, league: str, season: str):
    """Build every match DataFrame from the raw/replay engine datasets."""
    import pandas as pd

    from dags.utils.sofascore_dq import (
        validate_event_participants,
        validate_lineup_semantics,
        validate_season_alignment,
        validate_table_rows,
    )
    from scrapers.sofascore.adapters import project_legacy_match_status

    rows_by_dataset = {}
    for result in results:
        raw = result.raw
        for name, dataset in result.datasets.items():
            for row in dataset.rows:
                enriched = dict(row)
                enriched.update({
                    'source_tournament_id': result.manifest.key.source_tournament_id,
                    'source_season_id': result.manifest.key.source_season_id,
                    'raw_content_hash': (
                        raw.content_hash if raw else result.manifest.raw_content_hash
                    ),
                    'raw_blob_key': (
                        raw.blob_key if raw else result.manifest.raw_blob_key
                    ),
                })
                rows_by_dataset.setdefault(name, []).append(enriched)

    # The lineups payload supplies player stats while the event payload supplies
    # team identities. Join them in memory without another source request.
    participant_lookup = {
        (str(row.get('match_id')), row.get('team_side')): row
        for row in rows_by_dataset.get('event_participants', [])
    }
    for row in rows_by_dataset.get('event_player_stats', []):
        side = 'home' if row.get('is_home') is True else 'away'
        participant = participant_lookup.get((str(row.get('match_id')), side), {})
        row['team_id'] = row.get('team_id') or participant.get('team_id')
        row['team_name'] = row.get('team_name') or participant.get('name')

    entity_types = {
        'player_ratings': 'player_ratings',
        'lineups': 'lineups',
        'event_player_stats': 'event_player_stats',
        'match_stats': 'match_stats',
        'event_shotmap': 'event_shotmap',
        'venue': 'venue',
        'events': 'events',
        'event_participants': 'event_participants',
        'incidents': 'incidents',
    }
    frames = {}
    for name, entity_type in entity_types.items():
        rows = rows_by_dataset.get(name, [])
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame['league'] = league
            frame['season'] = season
            frame = scraper._add_metadata(frame, entity_type)
        frames[name] = frame

    status_rows = project_legacy_match_status(
        [result.manifest for result in results],
        league=league,
        season=season,
        endpoints=('event', 'lineups', 'statistics', 'shotmap', 'incidents'),
    )
    status = pd.DataFrame(status_rows)
    frames['capture_status'] = (
        scraper._add_metadata(status, 'match_capture_status')
        if not status.empty else status
    )

    dq_tables = {
        'events': 'bronze.sofascore_events',
        'event_participants': 'bronze.sofascore_event_participants',
        'lineups': 'bronze.sofascore_lineups',
        'incidents': 'bronze.sofascore_incidents',
    }
    for dataset, table in dq_tables.items():
        frame = frames[dataset]
        if not frame.empty:
            validate_table_rows(table, frame.to_dict('records')).require()
    if not frames['lineups'].empty:
        validate_lineup_semantics(
            frames['lineups'].to_dict('records')
        ).require()
    if not frames['event_participants'].empty:
        validate_event_participants(
            frames['event_participants'].to_dict('records')
        ).require()
    source_season_ids = {
        str(result.manifest.key.source_season_id)
        for result in results
    }
    if len(source_season_ids) != 1:
        raise ValueError(
            'match materialization received mixed SofaScore seasons: '
            + repr(sorted(source_season_ids))
        )
    expected_source_season_id = next(iter(source_season_ids))
    for name, frame in frames.items():
        if name == 'capture_status' or frame.empty:
            continue
        validate_season_alignment(
            frame.to_dict('records'),
            expected_source_season_id=expected_source_season_id,
            expected_canonical_season=season,
        ).require()
    return frames


def _run_match_capture(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
    capture_runtime=None,
    offline_replay: bool = False,
) -> int:
    """#751 PR1+PR2 — consolidated per-match capture entrypoint.

    One warmed Camoufox session feeds five data tables plus a status manifest
    from the same captured
    ``/lineups`` + ``/event`` + ``/statistics`` + ``/shotmap`` payloads:
    ``sofascore_player_ratings``, ``sofascore_event_player_stats``,
    ``sofascore_match_stats``, ``sofascore_event_shotmap`` and
    ``sofascore_venue`` — replacing separate Turnstile-blocked passes. The
    secondary tables come essentially
    free with the ratings capture (no per-player ``/player/{pid}/statistics``
    nor per-event ``/statistics`` REST calls). statistics/shotmap are
    best-effort — an empty frame is skipped.

    #842 incremental: the endpoint-status manifest skips terminally completed
    matches (finished-match data is immutable; re-capturing the whole season
    daily burned ~1.6 GB of residential proxy per run). It distinguishes valid
    empty/404 answers from retryable misses. Only incomplete/new matches are
    captured; each frame is incrementally merged by its natural key.
    ``--force-replace`` deliberately re-captures endpoints for repairs while
    retaining the same incremental writer contract.

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

    if len(leagues) != 1:
        error = (
            'capture requires exactly one registry competition per logical '
            f'task; received {leagues!r}'
        )
        logger.error(error)
        _write_results(
            output_path,
            {
                'entity': ENTITY_MATCH_CAPTURE,
                'tables': [],
                'errors': [f'competition_scope: {error}'],
                'traffic': {
                    'paid_proxy_bytes': 0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'requests': 0,
                },
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
        'event_rows': 0,
        'participant_rows': 0,
        'incident_rows': 0,
        'incident_matches': 0,
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
    endpoint_plan = None
    endpoint_specs = {}
    pipeline_results = []

    if capture_runtime is not None:
        try:
            from scrapers.sofascore.pipeline import (
                EVENT_PATHS,
                build_event_spec,
                endpoint_resume_plan,
            )

            if not match_ids:
                raise RuntimeError(
                    'bronze schedule has no finished event ids; tournament '
                    'schedule raw/replay is not implemented in the common '
                    'capture engine, refusing browser/source fallback'
                )
            source_tournament_id, source_season_id = _source_context(
                league, season, season_short
            )
            production_capture = True
            freshness_key = (
                f"repair-{capture_runtime.engine.run_id}"
                if force_replace else 'final'
            )
            specs = [
                build_event_spec(
                    source_tournament_id=source_tournament_id,
                    source_season_id=source_season_id,
                    target_id=match_id,
                    endpoint=endpoint,
                    freshness_key=freshness_key,
                    paid_proxy=production_capture,
                )
                for match_id in match_ids
                for endpoint in EVENT_PATHS
            ]
            endpoint_specs = {
                (spec.key.target_id, spec.key.endpoint): spec for spec in specs
            }
            endpoint_plan = (
                {str(match_id): tuple(EVENT_PATHS) for match_id in match_ids}
                if force_replace or offline_replay
                else endpoint_resume_plan(capture_runtime.manifest_store, specs)
            )
            total = len(match_ids)
            match_ids = [
                match_id for match_id in match_ids
                if str(match_id) in endpoint_plan
            ]
            results['matches_total'] = total
            results['matches_skipped_existing'] = total - len(match_ids)
            if match_ids and not offline_replay and production_capture:
                # The verified policy and provider-meter token must wrap the
                # warmed browser before a single byte moves. Until the checked-
                # in canary is verified and the proxy-filter token is wired,
                # fail closed rather than silently bypassing the hard budget.
                raise RuntimeError(
                    'production paid capture disabled: '
                    + _paid_capture_blocker(capture_runtime)
                )
            if not match_ids:
                results['traffic'] = {
                    'paid_proxy_bytes': 0,
                    'paid_proxy_mb': 0.0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'request_count': 0,
                    'cache_hit_rate': 1.0,
                    'endpoint_completeness': 1.0,
                }
                _write_results(output_path, results)
                return 0
        except Exception as exc:
            results['errors'].append(f'capture_engine: {exc}')
            results['traffic'] = {
                'paid_proxy_bytes': 0,
                'paid_proxy_mb': 0.0,
                'browser_sessions': 0,
                'browser_navigations': 0,
                'request_count': 0,
            }
            _write_results(output_path, results)
            return 1

    # #842 skip-existing: don't re-capture manifest-complete matches. When
    # nothing is new (off-season / no fixtures since yesterday) exit 0 before
    # even opening the scraper session — zero proxy bytes spent.
    if match_ids and not force_replace and capture_runtime is None:
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

            if offline_replay:
                from scrapers.sofascore.pipeline import replay_event_specs

                replay_specs = [
                    endpoint_specs[(str(match_id), endpoint)]
                    for match_id in match_ids
                    for endpoint in endpoint_plan[str(match_id)]
                ]
                pipeline_results = replay_event_specs(
                    capture_runtime, replay_specs
                )
                frames = _materialize_endpoint_results(
                    scraper,
                    pipeline_results,
                    league=league,
                    season=season_short,
                )
            else:
                frames = scraper.read_match_capture(
                    league=league, season=int(season),
                    match_ids=match_ids, limit=limit,
                    endpoint_names_by_match=endpoint_plan,
                )
                if capture_runtime is not None:
                    from scrapers.sofascore.pipeline import (
                        ingest_prefetched_records,
                    )

                    pipeline_results = ingest_prefetched_records(
                        capture_runtime,
                        specs=endpoint_specs,
                        records=frames.get('raw_records') or {},
                    )
                    expected = {
                        (str(match_id), endpoint)
                        for match_id in match_ids
                        for endpoint in endpoint_plan[str(match_id)]
                    }
                    observed = {
                        (result.manifest.key.target_id,
                         result.manifest.key.endpoint)
                        for result in pipeline_results
                    }
                    if expected - observed:
                        raise RuntimeError(
                            'raw-first capture lost endpoint records: '
                            + repr(sorted(expected - observed))
                        )
                    frames = _materialize_endpoint_results(
                        scraper,
                        pipeline_results,
                        league=league,
                        season=season_short,
                    )
            results['traffic'] = scraper.get_traffic_stats()  # #789 + #879 camoufox
            ratings_df = frames.get('player_ratings')
            lineup_df = frames.get('lineups')
            eps_df = frames.get('event_player_stats')
            stats_df = frames.get('match_stats')
            shot_df = frames.get('event_shotmap')
            venue_df = frames.get('venue')
            event_df = frames.get('events')
            participant_df = frames.get('event_participants')
            incident_df = frames.get('incidents')
            status_df = frames.get('capture_status')
            ratings_empty = ratings_df is None or ratings_df.empty
            lineup_empty = lineup_df is None or lineup_df.empty
            eps_empty = eps_df is None or eps_df.empty
            stats_empty = stats_df is None or stats_df.empty
            shot_empty = shot_df is None or shot_df.empty
            venue_empty = venue_df is None or venue_df.empty
            event_empty = event_df is None or event_df.empty
            participant_empty = participant_df is None or participant_df.empty
            incident_empty = incident_df is None or incident_df.empty
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

            # Every captured frame is a delta over immutable finished matches.
            # Iceberg MERGE updates/inserts only its natural keys: no pandas
            # full-partition read, no partition rewrite, and no shrink window.
            if not stats_empty:
                spath = scraper.save_to_iceberg(
                    df=stats_df,
                    table_name='sofascore_match_stats',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'period',
                        'stat_group', 'statistic_key',
                    ],
                )
                results['tables'].append(spath)
                results['match_stats_rows'] = int(len(stats_df))
                results['match_stats_matches'] = int(stats_df['match_id'].nunique())
                logger.info("Saved %d match_stats rows -> %s",
                            results['match_stats_rows'], spath)

            # event_shotmap — best-effort optional payload, saved before the
            # lineup-derived completion markers below.
            if not shot_empty:
                shpath = scraper.save_to_iceberg(
                    df=shot_df,
                    table_name='sofascore_event_shotmap',
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'match_id', 'shot_id'],
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
                vpath = scraper.save_to_iceberg(
                    df=venue_df,
                    table_name='sofascore_venue',
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'game_id'],
                )
                results['tables'].append(vpath)
                results['venue_rows'] = int(len(venue_df))
                results['venue_matches'] = int(venue_df['game_id'].nunique())
                logger.info("Saved %d venue rows -> %s",
                            results['venue_rows'], vpath)

            if not event_empty:
                evpath = scraper.save_to_iceberg(
                    df=event_df,
                    table_name='sofascore_events',
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'match_id'],
                )
                results['tables'].append(evpath)
                results['event_rows'] = int(len(event_df))

            if not participant_empty:
                eppath = scraper.save_to_iceberg(
                    df=participant_df,
                    table_name='sofascore_event_participants',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'team_id',
                    ],
                )
                results['tables'].append(eppath)
                results['participant_rows'] = int(len(participant_df))

            if not incident_empty:
                ipath = scraper.save_to_iceberg(
                    df=incident_df,
                    table_name='sofascore_incidents',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'incident_id',
                    ],
                )
                results['tables'].append(ipath)
                results['incident_rows'] = int(len(incident_df))
                results['incident_matches'] = int(
                    incident_df['match_id'].nunique()
                )

            # Save the two lineup-derived data tables after optional payloads;
            # the explicit status manifest below is the only completion marker.
            if not lineup_empty:
                lpath = scraper.save_to_iceberg(
                    df=lineup_df,
                    table_name='sofascore_lineups',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'player_id',
                    ],
                )
                results['tables'].append(lpath)
                results['lineup_rows'] = int(len(lineup_df))
                results['lineup_matches'] = int(lineup_df['match_id'].nunique())
                if 'is_unused_substitute' in lineup_df.columns:
                    results['unused_substitutes'] = int(
                        lineup_df['is_unused_substitute'].fillna(False).astype(bool).sum()
                    )

            if not eps_empty:
                epath = scraper.save_to_iceberg(
                    df=eps_df,
                    table_name='sofascore_event_player_stats',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'player_id',
                    ],
                )
                results['tables'].append(epath)
                results['eps_rows'] = int(len(eps_df))
                results['eps_matches'] = int(eps_df['match_id'].nunique())
                logger.info("Saved %d eps rows -> %s", results['eps_rows'], epath)

            # Ratings remains the primary data table, but is not used as status.
            if not ratings_empty:
                rpath = scraper.save_to_iceberg(
                    df=ratings_df,
                    table_name='sofascore_player_ratings',
                    partition_cols=['league', 'season'],
                    natural_keys=[
                        'league', 'season', 'match_id', 'player_id',
                    ],
                )
                results['tables'].append(rpath)
                results['rows'] = int(len(ratings_df))
                results['matches_with_ratings'] = int(
                    ratings_df['match_id'].nunique())
                logger.info("Saved %d rating rows -> %s", results['rows'], rpath)

            # Endpoint-status manifest is the final commit. Complete terminal
            # answers (including optional 404/empty JSON) are skipped forever;
            # transient misses and interrupted saves remain eligible for retry.
            if not status_empty and not pipeline_results:
                cpath = scraper.save_to_iceberg(
                    df=status_df,
                    table_name=_MATCH_CAPTURE_STATUS_TABLE,
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'match_id'],
                )
                results['tables'].append(cpath)
                results['capture_status_rows'] = int(len(status_df))
                results['matches_complete'] = int(
                    status_df.loc[
                        status_df['capture_complete'].fillna(False).astype(bool),
                        'match_id',
                    ].nunique()
                )

            if pipeline_results:
                from dags.utils.sofascore_dq import (
                    CaptureExpectation,
                    validate_manifest_completeness,
                )
                from scrapers.sofascore.pipeline import (
                    finalize_materialized_results,
                )

                finalize_materialized_results(
                    capture_runtime, pipeline_results
                )
                observations = []
                expectations = []
                for result in pipeline_results:
                    key = result.manifest.key
                    committed = capture_runtime.manifest_store.get(key)
                    if committed is None:
                        raise RuntimeError(
                            f'manifest commit missing for {key.stable_id()}'
                        )
                    observations.append({
                        **committed.key.__dict__,
                        'state': committed.status.value,
                        'updated_at': committed.updated_at,
                        'attempt': committed.attempts,
                    })
                    expectations.append(CaptureExpectation(
                        endpoint=key.endpoint,
                        target_type=key.target_type,
                        target_id=key.target_id,
                        source_tournament_id=key.source_tournament_id,
                        source_season_id=key.source_season_id,
                        freshness_key=key.freshness_key,
                    ))
                from scrapers.sofascore.adapters import (
                    project_legacy_match_status,
                )

                compatibility_rows = project_legacy_match_status(
                    _complete_manifest_records_for_projection(
                        capture_runtime.manifest_store,
                        endpoint_specs,
                        pipeline_results,
                    ),
                    league=league,
                    season=season_short,
                    endpoints=(
                        'event', 'lineups', 'statistics',
                        'shotmap', 'incidents',
                    ),
                )
                if compatibility_rows:
                    import pandas as pd

                    compatibility = scraper._add_metadata(
                        pd.DataFrame(compatibility_rows),
                        'match_capture_status',
                    )
                    cpath = scraper.save_to_iceberg(
                        df=compatibility,
                        table_name=_MATCH_CAPTURE_STATUS_TABLE,
                        partition_cols=['league', 'season'],
                        natural_keys=['league', 'season', 'match_id'],
                    )
                    results['tables'].append(cpath)
                    results['capture_status_rows'] = len(compatibility)
                    results['matches_complete'] = int(
                        compatibility.loc[
                            compatibility['capture_complete'].astype(bool),
                            'match_id',
                        ].nunique()
                    )
                validate_manifest_completeness(
                    expectations, observations
                ).require()
                results['endpoint_completeness'] = 1.0
                results['replay_cache'] = capture_runtime.engine.metrics.snapshot()

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
    capture_runtime=None,
    offline_replay: bool = False,
) -> int:
    """#751 PR3 + PR3b — per-player capture entrypoint (profile + season stats).

    One navigation warms each bounded browser session. It writes
    ``sofascore_player_profile`` plus the exact target competition/season stats;
    later players use same-origin JSON fetches instead of another SPA load. Both
    outputs are natural-keyed deltas written through Iceberg MERGE.

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

    # The canonical player path can fully replay/no-op before any browser is
    # considered. Profiles and aggregates are mutable during a season, so the
    # scheduled weekly task receives a stable ISO-week freshness key: Airflow
    # retries and same-week reruns are exact zero-traffic resume hits, while a
    # new weekly snapshot remains eligible for refresh.
    if capture_runtime is not None:
        scraper = None
        try:
            from dags.utils.sofascore_dq import (
                validate_minimum_coverage,
                validate_season_alignment,
            )
            from scrapers.sofascore.pipeline import (
                PLAYER_PATHS,
                build_player_spec,
                endpoint_resume_plan,
                finalize_materialized_results,
                materialize_player_datasets,
                replay_player_specs,
            )
            from scrapers.sofascore.season_pipeline import (
                plan_season_partition,
                squad_player_ids,
            )

            source_tournament_id, source_season_id = _source_context(
                league, season, season_short
            )
            season_plan = plan_season_partition(
                capture_runtime.raw_store,
                capture_runtime.manifest_store,
                source_tournament_id=source_tournament_id,
                source_season_id=source_season_id,
                freshness_key=_season_freshness_key(
                    capture_runtime,
                    force_replace=False,
                    offline_replay=offline_replay,
                ),
                event_freshness_key='final',
                paid_proxy=True,
                max_pages=_season_max_pages(),
            )
            if not season_plan.complete:
                raise RuntimeError(
                    'season/squad manifest is incomplete; refusing a partial '
                    'player universe'
                )
            registered_ids = set(
                squad_player_ids(capture_runtime.raw_store, season_plan)
            )
            with SofaScoreScraper(
                leagues=[league], seasons=[season], proxy_file=None,
            ) as scraper:
                observed_ids = {
                    str(value)
                    for value in scraper._resolve_player_ids_from_bronze(
                        league, season_short, limit=None
                    )
                }
                player_ids = sorted(
                    registered_ids | observed_ids,
                    key=int,
                )
                if limit:
                    player_ids = player_ids[: int(limit)]
                if not player_ids:
                    raise RuntimeError(
                        'player universe is empty; refusing source/browser '
                        'fallback outside the common manifest'
                    )

                freshness_key = (
                    os.environ.get('SOFASCORE_PLAYER_FRESHNESS_KEY', '').strip()
                    or 'week-'
                    + datetime.now(timezone.utc).strftime('%G-W%V')
                )
                if force_replace and not offline_replay:
                    freshness_key = f'repair-{capture_runtime.engine.run_id}'
                specs = [
                    build_player_spec(
                        source_tournament_id=source_tournament_id,
                        source_season_id=source_season_id,
                        target_id=player_id,
                        endpoint=endpoint,
                        freshness_key=freshness_key,
                        paid_proxy=True,
                    )
                    for player_id in player_ids
                    for endpoint in PLAYER_PATHS
                ]
                pending = endpoint_resume_plan(
                    capture_runtime.manifest_store, specs
                )
                results['players_total'] = len(player_ids)
                results['registered_players'] = len(registered_ids)
                results['players_skipped_existing'] = sum(
                    1 for player_id in player_ids if str(player_id) not in pending
                )
                if not offline_replay and not force_replace and not pending:
                    results['traffic'] = {
                        'paid_proxy_bytes': 0,
                        'paid_proxy_mb': 0.0,
                        'browser_sessions': 0,
                        'browser_navigations': 0,
                        'request_count': 0,
                        'cache_hit_rate': 1.0,
                        'endpoint_completeness': 1.0,
                    }
                    _write_results(output_path, results)
                    return 0
                if not offline_replay:
                    raise RuntimeError(
                        'production paid player capture disabled: '
                        + _paid_capture_blocker(capture_runtime)
                    )

                replayed = replay_player_specs(capture_runtime, specs)
                frames = materialize_player_datasets(
                    scraper,
                    replayed,
                    league=league,
                    season=season_short,
                )
                profile_df = frames['player_profile']
                season_df = frames['player_season_stats']
                profile_ids = (
                    set(profile_df['player_id'].astype(str))
                    if not profile_df.empty else set()
                )
                validate_minimum_coverage(
                    'player_profile',
                    profile_ids,
                    {str(player_id) for player_id in player_ids},
                    threshold=0.95,
                ).require()
                for frame in (profile_df, season_df):
                    if not frame.empty:
                        validate_season_alignment(
                            frame.to_dict('records'),
                            expected_source_season_id=source_season_id,
                            expected_canonical_season=season_short,
                        ).require()

                import pandas as pd

                universe_df = scraper._add_metadata(
                    pd.DataFrame(
                        [
                            {
                                'player_id': player_id,
                                'in_registered_squad': player_id in registered_ids,
                                'observed_in_match': player_id in observed_ids,
                                'source_tournament_id': str(source_tournament_id),
                                'source_season_id': str(source_season_id),
                                'league': league,
                                'season': season_short,
                            }
                            for player_id in player_ids
                        ]
                    ),
                    'player_universe',
                )
                validate_season_alignment(
                    universe_df.to_dict('records'),
                    expected_source_season_id=source_season_id,
                    expected_canonical_season=season_short,
                ).require()
                upath = scraper.save_to_iceberg(
                    df=universe_df,
                    table_name='sofascore_player_universe',
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'player_id'],
                )
                results['tables'].append(upath)
                results['universe_players'] = len(universe_df)

                ppath = scraper.save_to_iceberg(
                    df=profile_df,
                    table_name='sofascore_player_profile',
                    partition_cols=['league', 'season'],
                    natural_keys=['league', 'season', 'player_id'],
                )
                results['tables'].append(ppath)
                results['rows'] = len(profile_df)
                results['profile_players'] = profile_df['player_id'].nunique()
                if not season_df.empty:
                    spath = scraper.save_to_iceberg(
                        df=season_df,
                        table_name='sofascore_player_season_stats',
                        partition_cols=['league', 'season'],
                        natural_keys=[
                            'league', 'season', 'player_id',
                            'unique_tournament_id', 'sofascore_season_id',
                        ],
                    )
                    results['tables'].append(spath)
                    results['season_stats_rows'] = len(season_df)
                    results['season_stats_players'] = season_df[
                        'player_id'
                    ].nunique()

                finalize_materialized_results(capture_runtime, replayed)
                results['traffic'] = capture_runtime.engine.metrics.snapshot()
                results['endpoint_completeness'] = results['traffic'][
                    'endpoint_completeness'
                ]
                _write_results(output_path, results)
                return 0
        except Exception as exc:
            results['errors'].append(f'capture_engine: {exc}')
            results['traffic'] = (
                capture_runtime.engine.metrics.snapshot()
                if capture_runtime is not None
                else {
                    'paid_proxy_bytes': 0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'request_count': 0,
                }
            )
            _write_results(output_path, results)
            return 1

    logger.info(
        "player_capture legacy test seam: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 without "
            "residential proxy.", proxy_file,
        )
        proxy_file = None

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

            # Profile/stat rows are natural-keyed deltas. MERGE avoids a full
            # season rewrite and cannot shrink a previously complete universe.
            ppath = scraper.save_to_iceberg(
                df=profile_df,
                table_name='sofascore_player_profile',
                partition_cols=['league', 'season'],
                natural_keys=['league', 'season', 'player_id'],
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
                    natural_keys=[
                        'league', 'season', 'player_id',
                        'unique_tournament_id', 'sofascore_season_id',
                    ],
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


def _run_season_capture_engine(
    leagues: List[str],
    season: int,
    output_path: str,
    *,
    force_replace: bool,
    entity: str,
    capture_runtime,
    offline_replay: bool,
) -> int:
    """Materialize schedule/standings through the canonical raw manifest.

    Planning is network-free and follows only locally committed page-chain,
    participant, squad and event/referee evidence. An exact terminal plan is a
    zero-byte no-op. Offline replay validates every retained payload, performs
    the two incremental Bronze MERGEs, and only then finalizes deferred
    normalized manifest records.
    """
    import pandas as pd

    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.pipeline import finalize_materialized_results
    from scrapers.sofascore.scraper import _season_label
    from scrapers.sofascore.season_pipeline import (
        materialize_season_partition,
        plan_season_partition,
        replay_season_specs,
    )

    league = leagues[0]
    canonical_season = _season_label(league, season)
    results = {
        "entity": entity,
        "tables": [],
        "schedule_rows": 0,
        "league_table_rows": 0,
        "errors": [],
    }
    try:
        source_tournament_id, source_season_id = _source_context(
            league, season, canonical_season
        )
        freshness_key = _season_freshness_key(
            capture_runtime,
            force_replace=force_replace,
            offline_replay=offline_replay,
        )
        plan = plan_season_partition(
            capture_runtime.raw_store,
            capture_runtime.manifest_store,
            source_tournament_id=source_tournament_id,
            source_season_id=source_season_id,
            freshness_key=freshness_key,
            event_freshness_key="final",
            paid_proxy=True,
            max_pages=_season_max_pages(),
        )
        results.update(
            {
                "source_tournament_id": source_tournament_id,
                "source_season_id": source_season_id,
                "freshness_key": freshness_key,
                "planned_endpoints": len(plan.specs),
                "pending_endpoints": len(plan.pending_keys),
                "schedule_events": len(plan.schedule_event_ids),
                "teams": len(plan.team_ids),
                "referees": len(plan.referee_ids),
            }
        )
        if plan.complete and not offline_replay and not force_replace:
            results["endpoint_completeness"] = 1.0
            results["traffic"] = {
                "paid_proxy_bytes": 0,
                "paid_proxy_mb": 0.0,
                "browser_sessions": 0,
                "browser_navigations": 0,
                "request_count": 0,
                "cache_hit_rate": 1.0,
                "endpoint_completeness": 1.0,
            }
            _write_results(output_path, results)
            return 0
        if not offline_replay:
            raise RuntimeError(
                "production paid tournament capture disabled: "
                + _paid_capture_blocker(capture_runtime)
            )

        replayed = replay_season_specs(
            capture_runtime.engine,
            plan.specs,
        )
        materialized = materialize_season_partition(
            plan,
            replayed,
            canonical_league=league,
            canonical_season=canonical_season,
        )
        if materialized.endpoint_completeness != 1.0:
            raise RuntimeError(
                "offline season replay did not cover every planned endpoint"
            )

        with SofaScoreScraper(
            leagues=[league], seasons=[season], proxy_file=None
        ) as scraper:
            # The season endpoints are one atomic logical capture. Even a
            # compatibility ``--entity schedule``/``league_table`` invocation
            # publishes both normalized datasets before their shared manifest
            # is finalized; otherwise one endpoint could be marked complete
            # without its Bronze MERGE.
            if materialized.schedule_rows:
                schedule_df = scraper._add_metadata(
                    pd.DataFrame(materialized.schedule_rows), "schedule"
                )
                path = scraper.save_to_iceberg(
                    df=schedule_df,
                    table_name="sofascore_schedule",
                    partition_cols=["league", "season"],
                    natural_keys=["league", "season", "game_id"],
                )
                results["tables"].append(path)
                results["schedule_rows"] = len(schedule_df)
            if materialized.standings_rows:
                standings_df = scraper._add_metadata(
                    pd.DataFrame(materialized.standings_rows), "league_table"
                )
                path = scraper.save_to_iceberg(
                    df=standings_df,
                    table_name="sofascore_league_table",
                    partition_cols=["league", "season"],
                    natural_keys=["league", "season", "group", "team"],
                )
                results["tables"].append(path)
                results["league_table_rows"] = len(standings_df)

        # This must be the last state mutation. If either MERGE above fails,
        # normalized endpoints remain replayable and no network retry is needed.
        finalize_materialized_results(capture_runtime, replayed)
        committed = plan_season_partition(
            capture_runtime.raw_store,
            capture_runtime.manifest_store,
            source_tournament_id=source_tournament_id,
            source_season_id=source_season_id,
            freshness_key=freshness_key,
            event_freshness_key="final",
            paid_proxy=True,
            max_pages=_season_max_pages(),
        )
        if not committed.complete:
            raise RuntimeError(
                "season manifest stayed nonterminal after successful Bronze MERGEs"
            )
        results["pending_endpoints"] = 0
        results["endpoint_completeness"] = 1.0
        results["traffic"] = capture_runtime.engine.metrics.snapshot()
        _write_results(output_path, results)
        return 0
    except ReplaceGuardError as exc:
        results["errors"].append(f"{REPLACE_GUARD_MARKER}: {exc}")
        results["traffic"] = capture_runtime.engine.metrics.snapshot()
        _write_results(output_path, results)
        return 3
    except Exception as exc:
        logger.error("season capture engine failed: %s", exc, exc_info=True)
        results["errors"].append(f"capture_engine: {exc}")
        results["traffic"] = capture_runtime.engine.metrics.snapshot()
        _write_results(output_path, results)
        return 1


def _run_legacy(
    leagues: List[str],
    season: int,
    output_path: str,
    force_replace: bool = False,
    entity: str = "all",
    capture_runtime=None,
    offline_replay: bool = False,
) -> int:
    """Scrape schedule and/or league table.

    The default daily path requests both through one tournament snapshot. An
    explicit ``--entity schedule`` or ``league_table`` only calls that reader;
    the old dispatch silently fetched both regardless of the CLI selection.
    """
    if capture_runtime is not None:
        return _run_season_capture_engine(
            leagues,
            season,
            output_path,
            force_replace=force_replace,
            entity=entity,
            capture_runtime=capture_runtime,
            offline_replay=offline_replay,
        )

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

    if offline_replay:
        results['errors'].append(
            'offline tournament replay requires the common capture runtime'
        )
        _write_results(output_path, results)
        return 1

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
                    if captured is not None and not captured.empty:
                        table_path = scraper.save_to_iceberg(
                            df=captured,
                            table_name="sofascore_schedule",
                            partition_cols=["league", "season"],
                            natural_keys=['league', 'season', 'game_id'],
                        )
                        results["tables"].append(table_path)
                        results["schedule_rows"] = len(captured)
                        logger.info(
                            "Incrementally merged %d schedule rows",
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
                            natural_keys=['league', 'season', 'group', 'team'],
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


def main(argv=None):
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
        type=str,
        default='2024',
        help='Canonical season or registry alias (2024, 2526, 2025/26, named)',
    )
    parser.add_argument(
        '--allow-inactive-season',
        action='store_true',
        help='Backfill-only: resolve an explicit historical/named registry season.',
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
        '--offline-replay',
        action='store_true',
        help='Disable source access and rebuild only from committed raw JSON.',
    )
    parser.add_argument(
        '--raw-store-uri',
        default=None,
        help='Override SOFASCORE_RAW_STORE_URI for capture/replay.',
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Re-capture source endpoints even when their manifest state is '
             'terminal; writes remain incremental natural-key merges.',
    )
    try:
        args = parser.parse_args(argv)
    except _ArgparseError as exc:
        logger.error("Invalid CLI arguments: %s — failing hard (not a fallback)", exc)
        return 1

    season_token = args.season.strip()
    if season_token.isdigit() and 1900 <= int(season_token) <= 2098:
        # Preserve the established CLI meaning: 2021 is the 2021/22 start
        # year for split-year leagues, while registry aliases such as 2526
        # and named seasons remain exact strings.
        args.season = int(season_token)
    else:
        args.season = season_token

    if args.league:
        leagues = [args.league]
    else:
        leagues = [league.strip() for league in args.leagues.split(",")]

    # Fail before Trino/source/proxy access for every CLI alias.  The registry
    # can discover any football competition, but capture is restricted to an
    # enabled, evidenced, operator-approved adult men's tournament.
    try:
        from scrapers.sofascore.catalog import SofaScoreCatalog

        catalog = SofaScoreCatalog.load()
        denied = []
        for league in leagues:
            tournament = catalog.competition(league)
            if not tournament.capture_allowed:
                denied.append(
                    f"{league}: enabled={tournament.enabled}; "
                    + '; '.join(tournament.activation_eligibility.reasons)
                )
        if denied:
            raise ValueError(' | '.join(denied))
    except Exception as exc:
        logger.error("SofaScore activation guard denied capture: %s", exc)
        _write_results(
            args.output,
            {
                'entity': args.entity,
                'tables': [],
                'errors': [f'activation_guard: {exc}'],
                'traffic': {
                    'paid_proxy_bytes': 0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'requests': 0,
                },
            },
        )
        return 1

    if len(leagues) != 1:
        error = (
            'capture requires exactly one registry competition per logical '
            f'task; received {leagues!r}'
        )
        logger.error(error)
        _write_results(
            args.output,
            {
                'entity': args.entity,
                'tables': [],
                'errors': [f'competition_scope: {error}'],
                'traffic': {
                    'paid_proxy_bytes': 0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'requests': 0,
                },
            },
        )
        return 1

    # #920 bridge (generalized Phase 3: any single_year tournament):
    # tournaments must never inherit the club-formula season (July 2026 ->
    # 2025) — the sid resolve would no-op every daily run while the
    # tournament is live. Every DAG task is already single-competition.
    from utils.medallion_config import (
        get_active_season, is_single_year_competition,
    )
    _is_tournament = is_single_year_competition(leagues[0])
    if _is_tournament and not args.allow_inactive_season:
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
        elif str(args.season).strip().isdigit() and int(args.season) != int(
            _t_season
        ):
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

    capture_runtime = None
    try:
        from scrapers.sofascore.pipeline import build_capture_runtime

        capture_runtime = build_capture_runtime(
            run_id=(
                os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')
                or os.environ.get('SOFASCORE_RUN_ID')
                or f'manual-{os.getpid()}'
            ),
            task_id=(
                os.environ.get('AIRFLOW_CTX_TASK_ID')
                or f'cli-{entity}'
            ),
            raw_store_uri=args.raw_store_uri,
        )
    except Exception as exc:
        logger.error('SofaScore capture runtime failed closed: %s', exc)
        _write_results(
            args.output,
            {
                'entity': entity,
                'tables': [],
                'errors': [f'capture_runtime: {exc}'],
                'traffic': {
                    'paid_proxy_bytes': 0,
                    'browser_sessions': 0,
                    'browser_navigations': 0,
                    'requests': 0,
                },
            },
        )
        return 1

    if entity in {
        ENTITY_MATCH_CAPTURE,
        ENTITY_PLAYER_RATINGS,
        ENTITY_SHOTMAP,
        ENTITY_EVENT_PLAYER_STATS,
        ENTITY_MATCH_STATS,
    }:
        if entity != ENTITY_MATCH_CAPTURE:
            logger.info(
                "Legacy entity %s is an alias of the unified match_capture engine",
                entity,
            )
        return _run_match_capture(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
            capture_runtime=capture_runtime,
            offline_replay=args.offline_replay,
        )

    if entity == ENTITY_PLAYER_CAPTURE:
        return _run_player_capture(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
            capture_runtime=capture_runtime,
            offline_replay=args.offline_replay,
        )

    # Default: legacy schedule+league_table flow.
    return _run_legacy(
        leagues=leagues,
        season=args.season,
        output_path=args.output,
        force_replace=args.force_replace,
        entity=entity,
        capture_runtime=capture_runtime,
        offline_replay=args.offline_replay,
    )


if __name__ == '__main__':
    sys.exit(main())
