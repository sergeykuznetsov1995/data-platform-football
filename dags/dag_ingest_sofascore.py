"""
SofaScore Data Ingestion DAG
============================

Airflow DAG for scraping football statistics from SofaScore.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Runs daily through ``dag_master_pipeline`` and has no independent schedule, so
the residential-proxy source is never invoked twice for the same day.

One source = one DAG (#782): the former weekly ``dag_ingest_sofascore_players``
is folded in here as a gated branch (pattern #710 MatchHistory / #716 ClubElo —
parametrize the ingest DAG instead of spawning a second one).

Data collected:
- Match schedule + league table (daily)
- Per-match capture: player_ratings, event_player_stats, match_stats, shotmap (daily)
- Per-player profile + season-aggregate stats (weekly — heavy, gated; see below)

The per-player capture (~526 players; since #842 one navigation plus in-page
fetches per bounded session) still is not worth running daily, so a
``ShortCircuitOperator`` gates it to Saturday master-runs or an explicit manual
``run_players=True`` invocation.

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

import os
from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.config import (
    CURRENT_SEASON,
    SCHEDULES,
    DAG_TAGS,
    scale_floor_for_league as _scale,
)
from utils.default_args import SCRAPER_ARGS
from utils.ingest_helpers import league_slug as _league_slug
from utils.ingest_helpers import load_result as _load_result
from utils.medallion_config import (
    get_active_season,
    get_competition_seasons,
    is_single_year_competition,
)

from scrapers.sofascore.catalog import CatalogError, SofaScoreCatalog


SCHEDULE_RESULT_PATH = '/tmp/sofascore_result.json'
# #751 PR1+PR2 — one consolidated Camoufox capture per match writes ALL FOUR
# per-match tables: player_ratings, event_player_stats, match_stats, shotmap
# (replaces four separate tls passes).
MATCH_CAPTURE_RESULT_PATH = '/tmp/sofascore_match_capture_result.json'
# #782 — per-player profile + season_stats capture (formerly the weekly
# dag_ingest_sofascore_players) now runs here behind the Saturday/manual gate.
PLAYER_CAPTURE_RESULT_PATH = '/tmp/sofascore_player_capture_result.json'

# Discovery and activation are deliberately separate: the registry contains
# every tournament SofaScore exposes, while only entries with enabled=true and
# a canonical_id may shape this DAG.  Keep this local to SofaScore — global
# utils.config.LEAGUES remains authoritative for the other ingestion sources.
def _load_active_sofascore_leagues() -> List[str]:
    try:
        catalog = SofaScoreCatalog.load()
        leagues = list(catalog.enabled_competition_ids())
    except CatalogError as exc:
        raise AirflowException(
            f"SofaScore registry is missing or invalid: {exc}"
        ) from exc

    if not leagues:
        raise AirflowException(
            "SofaScore registry has no usable active tournaments "
            "(expected enabled=true with a non-empty canonical_id)"
        )

    # An activation must be complete across source and canonical metadata.
    # Otherwise row-floor scaling and single-year routing would silently use
    # club defaults (or fail only after a paid scrape had already started).
    unusable: List[str] = []
    for league in leagues:
        try:
            seasons = get_competition_seasons(league)
        except KeyError:
            unusable.append(f"{league!r} is absent from competitions.yaml")
            continue
        if not seasons:
            unusable.append(f"{league!r} has no canonical seasons configured")
            continue

        try:
            source_tournament = catalog.competition(league)
        except CatalogError:
            unusable.append(f"{league!r} has no SofaScore registry mapping")
            continue
        source_seasons = {
            season.canonical_season
            for season in source_tournament.seasons
            if season.activatable and season.canonical_season is not None
        }
        configured_seasons = {str(season) for season in seasons}
        if not source_seasons:
            unusable.append(
                f"{league!r} has no activatable SofaScore seasons"
            )
            continue
        if not source_seasons & configured_seasons:
            unusable.append(
                f"{league!r} has no SofaScore season matching "
                "competitions.yaml"
            )
            continue

        if is_single_year_competition(league):
            active_season = get_active_season(league)
            target_season = (
                str(active_season) if active_season is not None else None
            )
        else:
            target_season = (
                f"{CURRENT_SEASON % 100:02d}"
                f"{(CURRENT_SEASON + 1) % 100:02d}"
            )
        if target_season is not None and target_season not in source_seasons:
            unusable.append(
                f"{league!r} scheduled season {target_season!r} is missing "
                "from the SofaScore registry"
            )
    if unusable:
        raise AirflowException(
            "SofaScore registry contains unusable active tournaments: "
            + "; ".join(unusable)
        )
    return leagues


SOFASCORE_LEAGUES = _load_active_sofascore_leagues()


# #920 Phase 1: split the active SofaScore registry scope into club
# (split_year) and tournament
# (single_year, e.g. INT-World Cup) leagues. Club leagues stay batched in one
# call (they share the same club-formula season — no reason to fan them out).
# Each tournament league needs its own dedicated single-league task, because
# (a) the runner's #920 bridge requires a dedicated call to resolve its
# active season correctly (a mixed call drops it), and (b) match_capture's
# runner only ever reads leagues[0]. New tournaments onboard via
# the registry activation flag alone — no further DAG changes.
CLUB_LEAGUES = [
    lg for lg in SOFASCORE_LEAGUES if not is_single_year_competition(lg)
]
TOURNAMENT_LEAGUES = [
    lg for lg in SOFASCORE_LEAGUES if is_single_year_competition(lg)
]

# The consolidated schedule and weekly player branches are still club-shaped.
# Surface a clear import error instead of failing later at CLUB_LEAGUES[0].
if not CLUB_LEAGUES:
    raise AirflowException(
        "SofaScore registry has active tournaments, but none is a usable "
        "split-year competition required by the club/player ingestion branch"
    )

# Preserve the historical EPL task ids/result paths even when another
# split-year competition is activated ahead of it in the ID-sorted registry.
_LEGACY_PRIMARY_CLUB = 'ENG-Premier League'
PRIMARY_CLUB_LEAGUE = (
    _LEGACY_PRIMARY_CLUB
    if _LEGACY_PRIMARY_CLUB in CLUB_LEAGUES
    else CLUB_LEAGUES[0]
)
CLUB_LEAGUES = [PRIMARY_CLUB_LEAGUE] + [
    lg for lg in CLUB_LEAGUES if lg != PRIMARY_CLUB_LEAGUE
]


def _env_int(name: str):
    """Read a positive int from ENV; empty/unparseable/non-positive → None."""
    raw = os.environ.get(name, '').strip()
    if not raw:
        return None
    try:
        v = int(raw)
    except ValueError:
        return None
    return v if v > 0 else None


# Smoke/dev cap for the player capture — None = full coverage (~526 players).
# Issue #69 convention.
PLAYER_CAPTURE_LIMIT = _env_int('SS_PLAYER_CAPTURE_LIMIT')


def _limit_arg(limit) -> str:
    return f"--limit {int(limit)}" if limit else ""


# #920 Phase 2: validate_data floors derived from the same calibrated bases
# as utils.config.PER_LEAGUE_FLOOR_BASES instead of inline literals. Keys are
# the run-JSON summary fields (this validator reads the current run's output,
# not Trino — see validate_bronze_freshness for the staleness guard).
# unit 'match' scales with the competition's scheduled match count, 'team'
# with team_count. WARN-only semantics unchanged.
_SS_FLOOR_BASES: Dict[str, tuple] = {
    'schedule_rows': ('match', 100),
    'league_table_rows': ('team', 10),
    'player_ratings_rows': ('match', 300),
    'shotmap_rows': ('match', 300),
    'event_player_stats_rows': ('match', 10_000),
    'match_stats_rows': ('match', 10_000),
    'venue_rows': ('match', 300),
}


def _summed_club_floors() -> Dict[str, int]:
    """WARN floors for the club batch, summed over CLUB_LEAGUES.

    The club JSON is batch-granular (one result file for the whole batch), so
    per-league resolution inside it is impossible without a runner refactor —
    the sum is the honest floor. For the current CLUB_LEAGUES ==
    ['ENG-Premier League'] every value equals the historical literal exactly
    (100 / 10 / 300 / 300 / 10000 / 10000 / 300).
    """
    return {
        k: sum(_scale(u, b, lg) for lg in CLUB_LEAGUES)
        for k, (u, b) in _SS_FLOOR_BASES.items()
    }


def _competition_floors(league: str) -> Dict[str, int]:
    """Per-competition floors for independently captured result files."""
    return {
        key: _scale(unit, base, league)
        for key, (unit, base) in _SS_FLOOR_BASES.items()
    }


def _capture_noop(capture_result: Dict[str, Any]) -> bool:
    """#842 incremental match_capture: True when the run resolved matches but
    skipped them ALL (already in bronze) with no fallback/errors — a clean
    no-op that wrote nothing by design (off-season / no new finished matches).
    """
    return bool(
        capture_result
        and not capture_result.get('fallback')
        and not (capture_result.get('errors') or [])
        and capture_result.get('matches_total', 0) > 0
        and capture_result.get('matches_skipped_existing', 0)
        >= capture_result.get('matches_total', 0)
    )


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality across both scrape tasks (schedule+league_table
    and player_ratings).
    """
    import logging

    logger = logging.getLogger(__name__)

    schedule_result = _load_result(SCHEDULE_RESULT_PATH, logger)
    # #751 PR1+PR2: ratings + event_player_stats + match_stats + shotmap now all
    # come from ONE consolidated capture run (single result file carrying
    # `rows`/`matches_with_ratings`, `eps_rows`/`eps_matches`,
    # `match_stats_rows`/`match_stats_matches`, `shotmap_rows`/`shotmap_matches`).
    capture_result = _load_result(MATCH_CAPTURE_RESULT_PATH, logger)

    if not schedule_result:
        raise AirflowException(
            f"Schedule results file {SCHEDULE_RESULT_PATH} missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'schedule_rows': schedule_result.get('schedule_rows', 0),
            'league_table_rows': schedule_result.get('league_table_rows', 0),
            'player_ratings_rows': capture_result.get('rows', 0),
            'player_ratings_matches': capture_result.get('matches_with_ratings', 0),
            'player_ratings_fallback': capture_result.get('fallback', False),
            'shotmap_rows': capture_result.get('shotmap_rows', 0),
            'shotmap_matches': capture_result.get('shotmap_matches', 0),
            'shotmap_fallback': capture_result.get('fallback', False),
            'event_player_stats_rows': capture_result.get('eps_rows', 0),
            'event_player_stats_matches': capture_result.get('eps_matches', 0),
            'event_player_stats_fallback': capture_result.get('fallback', False),
            'match_stats_rows': capture_result.get('match_stats_rows', 0),
            'match_stats_matches': capture_result.get('match_stats_matches', 0),
            'match_stats_fallback': capture_result.get('fallback', False),
            # venue (#753) — one row per match from the same capture pass.
            'venue_rows': capture_result.get('venue_rows', 0),
            'venue_matches': capture_result.get('venue_matches', 0),
            'venue_fallback': capture_result.get('fallback', False),
            # #842 incremental capture bookkeeping.
            'matches_total': capture_result.get('matches_total', 0),
            'matches_skipped_existing': capture_result.get(
                'matches_skipped_existing', 0),
            'tables': (
                schedule_result.get('tables', [])
                + capture_result.get('tables', [])
            ),
        }
    }

    errors: List[str] = []
    errors.extend(schedule_result.get('errors', []) or [])
    errors.extend(capture_result.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['league_table_rows'],
            validation['summary']['player_ratings_rows'],
            validation['summary']['shotmap_rows'],
            validation['summary']['event_player_stats_rows'],
            validation['summary']['match_stats_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Minimum thresholds (#920 Phase 2: derived per competitions.yaml volumes,
    # not APL literals — see _SS_FLOOR_BASES).
    floors = _summed_club_floors()
    capture_floors = _competition_floors(PRIMARY_CLUB_LEAGUE)
    if validation['summary']['schedule_rows'] < floors['schedule_rows']:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['league_table_rows'] < floors['league_table_rows']:
        validation['warnings'].append("Low league_table row count - possible scraping issue")

    # #842 incremental match_capture: a clean run that skipped every resolved
    # match (already in bronze) legitimately reports 0 captured rows — the
    # partitions were left untouched, so the capture row-floors below don't
    # apply. Schedule/league_table floors above still do (that task refreshes
    # daily regardless).
    capture_noop = _capture_noop(capture_result)
    if capture_noop:
        logger.info(
            "match_capture skip-existing no-op: all %d matches already in "
            "bronze — capture row-floors skipped.",
            validation['summary']['matches_total'],
        )

    # APL has ~300 matches/season; ratings emit ~25K rows. A count below the
    # floor means we scraped at most a handful of matches → DAG defect or
    # hard CF block.
    if not capture_noop and (
        validation['summary']['player_ratings_rows']
        < capture_floors['player_ratings_rows']
    ):
        if validation['summary']['player_ratings_fallback']:
            validation['warnings'].append(
                f"player_ratings R0.2B_FALLBACK: rows="
                f"{validation['summary']['player_ratings_rows']} matches="
                f"{validation['summary']['player_ratings_matches']}"
            )
            # Fallback is a soft failure — keep status non-failed so dependent
            # DAGs see partial_success, not hard-fail.
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_ratings row count: "
                f"{validation['summary']['player_ratings_rows']} "
                f"< {capture_floors['player_ratings_rows']}"
            )

    # Shotmap: full APL season ≈ 380 matches × ~25 shots/match ≈ 9.5K rows.
    # WARN-only floor (issue #69; covers first few gameweeks too).
    if not capture_noop and (
        validation['summary']['shotmap_rows'] < capture_floors['shotmap_rows']
    ):
        if validation['summary']['shotmap_fallback']:
            validation['warnings'].append(
                f"shotmap R0.2B_FALLBACK: rows="
                f"{validation['summary']['shotmap_rows']} matches="
                f"{validation['summary']['shotmap_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low shotmap row count: "
                f"{validation['summary']['shotmap_rows']} "
                f"< {capture_floors['shotmap_rows']}"
            )

    # event_player_stats: full APL season ≈ 380 matches × ~25 played players
    # ≈ 9.5K rows. WARN-only floor (issue #69).
    if not capture_noop and (
        validation['summary']['event_player_stats_rows']
        < capture_floors['event_player_stats_rows']
    ):
        if validation['summary']['event_player_stats_fallback']:
            validation['warnings'].append(
                f"event_player_stats R0.2B_FALLBACK: rows="
                f"{validation['summary']['event_player_stats_rows']} matches="
                f"{validation['summary']['event_player_stats_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low event_player_stats row count: "
                f"{validation['summary']['event_player_stats_rows']} "
                f"< {capture_floors['event_player_stats_rows']}"
            )

    # match_stats: full APL season ≈ 380 matches × 3 periods × ~30 stats
    # ≈ 34K rows. WARN-only floor (issue #69).
    if not capture_noop and (
        validation['summary']['match_stats_rows']
        < capture_floors['match_stats_rows']
    ):
        if validation['summary']['match_stats_fallback']:
            validation['warnings'].append(
                f"match_stats R0.2B_FALLBACK: rows="
                f"{validation['summary']['match_stats_rows']} matches="
                f"{validation['summary']['match_stats_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low match_stats row count: "
                f"{validation['summary']['match_stats_rows']} "
                f"< {capture_floors['match_stats_rows']}"
            )

    # venue (#753): one row per match → full APL season ≈ 380 rows. WARN-only
    # floor in line with shotmap. Was previously unvalidated — a
    # silently-empty venue capture never surfaced.
    if not capture_noop and (
        validation['summary']['venue_rows'] < capture_floors['venue_rows']
    ):
        if validation['summary']['venue_fallback']:
            validation['warnings'].append(
                f"venue R0.2B_FALLBACK: rows="
                f"{validation['summary']['venue_rows']} matches="
                f"{validation['summary']['venue_matches']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low venue row count: "
                f"{validation['summary']['venue_rows']} "
                f"< {capture_floors['venue_rows']}"
            )

    # player_season_stats + player_profile are validated by validate_player_data
    # (the gated weekly branch below), not here.

    # Additional split-year competitions share the schedule snapshot but have
    # independent match-capture tasks/result files. Validate each one against
    # its own floor so the legacy primary result is neither over-counted nor
    # allowed to hide a dead secondary capture.
    for _club_league in CLUB_LEAGUES[1:]:
        _slug = _league_slug(_club_league)
        _club_floors = _competition_floors(_club_league)
        _club_capture = _load_result(
            f'/tmp/sofascore_match_capture_result_{_slug}.json', logger)
        _club_summary: Dict[str, Any] = {}
        if not _club_capture:
            validation['warnings'].append(
                f"{_club_league}: match_capture result file "
                "missing/unreadable (runner died before writing?)"
            )
        elif (
            not _club_capture.get('skipped')
            and not _capture_noop(_club_capture)
        ):
            for err in _club_capture.get('errors') or []:
                validation['warnings'].append(f"{_club_league}: {err}")
            for key, field in (
                ('player_ratings_rows', 'rows'),
                ('shotmap_rows', 'shotmap_rows'),
                ('event_player_stats_rows', 'eps_rows'),
                ('match_stats_rows', 'match_stats_rows'),
                ('venue_rows', 'venue_rows'),
            ):
                rows = _club_capture.get(field, 0)
                _club_summary[key] = rows
                if rows < _club_floors[key]:
                    validation['warnings'].append(
                        f"{_club_league}: low {key}: "
                        f"{rows} < {_club_floors[key]}"
                    )
        if _club_summary:
            validation['summary'][f'club_{_slug}'] = _club_summary

    # #920 Phase 2: tournament legs — the Phase-1 fan-out writes one result
    # file per single_year league; until now nothing read them, so a dead
    # tournament scrape hid behind a green club batch. WARN-only by design:
    # promotion to the club fail rule is a deliberate follow-up after the
    # first tournament window runs green end-to-end.
    for _t_league in TOURNAMENT_LEAGUES:
        _slug = _league_slug(_t_league)
        _t_floors = {
            k: _scale(u, b, _t_league)
            for k, (u, b) in _SS_FLOOR_BASES.items()
        }
        _t_summary: Dict[str, Any] = {}

        _t_schedule = _load_result(f'/tmp/sofascore_result_{_slug}.json', logger)
        # The runner ALWAYS writes its output file — out-of-window no-ops
        # write the 'skipped' marker. A missing/unreadable file therefore
        # means the runner died before writing (OOM/timeout after the bash
        # rm -f) — WARN, don't silently pass (review hardening). Only the
        # explicit 'skipped' marker is the healthy silent state.
        if not _t_schedule:
            validation['warnings'].append(
                f"{_t_league}: schedule result file missing/unreadable "
                f"(runner died before writing?)"
            )
        elif not _t_schedule.get('skipped'):
            for err in _t_schedule.get('errors') or []:
                validation['warnings'].append(f"{_t_league}: {err}")
            for key in ('schedule_rows', 'league_table_rows'):
                rows = _t_schedule.get(key, 0)
                _t_summary[key] = rows
                if rows < _t_floors[key]:
                    validation['warnings'].append(
                        f"{_t_league}: low {key}: {rows} < {_t_floors[key]}"
                    )

        _t_capture = _load_result(
            f'/tmp/sofascore_match_capture_result_{_slug}.json', logger)
        if not _t_capture:
            validation['warnings'].append(
                f"{_t_league}: match_capture result file missing/unreadable "
                f"(runner died before writing?)"
            )
        elif (
            not _t_capture.get('skipped')
            and not _capture_noop(_t_capture)
        ):
            for err in _t_capture.get('errors') or []:
                validation['warnings'].append(f"{_t_league}: {err}")
            for key, field in (
                ('player_ratings_rows', 'rows'),
                ('shotmap_rows', 'shotmap_rows'),
                ('event_player_stats_rows', 'eps_rows'),
                ('match_stats_rows', 'match_stats_rows'),
                ('venue_rows', 'venue_rows'),
            ):
                rows = _t_capture.get(field, 0)
                _t_summary[key] = rows
                if rows < _t_floors[key]:
                    validation['warnings'].append(
                        f"{_t_league}: low {key}: {rows} < {_t_floors[key]}"
                    )

        if _t_summary:
            validation['summary'][f'tournament_{_slug}'] = _t_summary

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


def validate_bronze_freshness(**context) -> None:
    """Telegram-alert when bronze.sofascore_* stops refreshing (issue #751).

    The scrape tasks soft-exit (R0.2B_FALLBACK, exit 2) when SofaScore's
    anti-bot returns 403, so the DAG stays green while data silently goes
    stale (match-data stalled 26 days before anyone noticed). ``validate_data``
    only checks the row_count of the *current* run's JSON output — pre-existing
    stale rows still pass that floor. A direct MAX(_ingested_at) freshness
    check is what surfaces a multi-day ingestion stall.

    WARNING-severity (not ERROR) on purpose: the goal is to stop being silent
    (ping Telegram), not to hard-fail the DAG while the scraper fix (FlareSolverr
    migration, PR B) lands. Promote to ERROR after that yields green runs.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    # #842 incremental match_capture: on a clean skip-existing no-op the match
    # tables legitimately do not change. Schedule + standings are still expected
    # to refresh daily and must never be hidden by that no-op.
    capture_result = _load_result(MATCH_CAPTURE_RESULT_PATH, logger)
    capture_noop = _capture_noop(capture_result)
    if capture_noop:
        logger.info(
            "validate_bronze_freshness: match-grain freshness checks skipped — capture "
            "was a clean no-op (all %d matches already in bronze).",
            capture_result.get("matches_total", 0),
        )

    # Global table freshness (MAX(_ingested_at), no season filter) — robust to
    # SofaScore's varchar season slug and catches any ingestion stall. 48h gives
    # one missed daily run of grace before alerting.
    checks = [
        CHECK.freshness(
            "bronze.sofascore_schedule",
            ts_col="_ingested_at",
            max_age_hours=48,
            severity="WARNING",
        ),
        CHECK.freshness(
            "bronze.sofascore_league_table",
            ts_col="_ingested_at",
            max_age_hours=48,
            severity="WARNING",
        ),
        # #711: a partial /statistics capture (proxy degradation) can write
        # match_stats rows that carry the group + numeric values but NO stat
        # label (name/stat_name empty) — the row-count floors in validate_data
        # still pass, yet every stat is anonymous so Silver can't tell which
        # row is "Fouls". Assert most rows are labelled. Historic damage:
        # APL seasons 2122-2425 (~99% empty) surfaces here until re-scraped.
        CHECK.coverage(
            'bronze.sofascore_match_stats',
            condition="COALESCE(name, '') <> '' OR COALESCE(stat_name, '') <> ''",
            warn_threshold=0.99, error_threshold=0.95, severity='WARNING',
            name='match_stats_labelled',
        ),
    ]
    if not capture_noop:
        checks.extend(
            [
                CHECK.freshness(
                    "bronze.sofascore_match_stats",
                    ts_col="_ingested_at",
                    max_age_hours=48,
                    severity="WARNING",
                ),
                CHECK.freshness(
                    "bronze.sofascore_event_player_stats",
                    ts_col="_ingested_at",
                    max_age_hours=48,
                    severity="WARNING",
                ),
                CHECK.freshness(
                    "bronze.sofascore_player_ratings",
                    ts_col="_ingested_at",
                    max_age_hours=48,
                    severity="WARNING",
                ),
            ]
        )
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_bronze_freshness: %s", report.summary())
    telegram_dq_summary(report, header='SofaScore Bronze freshness')


# ---------------------------------------------------------------------------
# Per-player capture (#782) — folded from the former weekly players DAG.
# ---------------------------------------------------------------------------

def _gate_player_capture(**context) -> bool:
    """ShortCircuitOperator hook — TRUE means "run the per-player capture".

    The ~526-player capture is heavy (hours, residential proxy), so it must not
    run every day. It runs only when:

      - a manual "Trigger DAG w/ config" sets ``run_players=True`` (on demand); or
      - the daily master-pipeline run falls on Saturday (weekly cadence).

    Skipped otherwise.
    Returning False short-circuits the downstream player tasks to ``skipped``.
    """
    import logging

    logger = logging.getLogger(__name__)

    params = context.get('params') or {}
    if params.get('run_players'):
        logger.info("run_players=True → running per-player capture on demand.")
        return True

    # Master passes its stable data-interval boundary in dag_run.conf. This is
    # intentionally not child start_date: queue delays/retries must not move the
    # weekly branch to another weekday.
    dag_run = context.get("dag_run")
    run_conf = getattr(dag_run, "conf", None) or {}
    master_boundary = run_conf.get("master_data_interval_end")
    if isinstance(master_boundary, str):
        try:
            master_boundary = datetime.fromisoformat(
                master_boundary.replace("Z", "+00:00")
            )
        except ValueError:
            logger.warning(
                "Invalid master_data_interval_end=%r; using local context.",
                master_boundary,
            )
            master_boundary = None
    external_boundary = (
        master_boundary or getattr(dag_run, "start_date", None)
        if getattr(dag_run, "external_trigger", False)
        else None
    )
    run_boundary = (
        external_boundary
        or context.get("data_interval_end")
        or context.get("logical_date")
        or context.get("execution_date")
    )
    if run_boundary is not None and run_boundary.weekday() == 5:  # Saturday
        logger.info("Saturday run → running weekly per-player capture.")
        return True

    logger.info("Not Saturday and not forced → skip per-player capture.")
    return False


def validate_player_data(**context) -> Dict[str, Any]:
    """Row-floor + fallback validation for the consolidated player capture."""
    import logging

    logger = logging.getLogger(__name__)
    result = _load_result(PLAYER_CAPTURE_RESULT_PATH, logger)

    if not result:
        raise AirflowException(
            f"player_capture results file {PLAYER_CAPTURE_RESULT_PATH} "
            f"missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'player_profile_rows': result.get('rows', 0),
            'player_profile_players': result.get('profile_players', 0),
            'player_season_stats_rows': result.get('season_stats_rows', 0),
            'player_season_stats_players': result.get('season_stats_players', 0),
            'fallback': result.get('fallback', False),
            'tables': result.get('tables', []),
        },
    }

    errors: List[str] = result.get('errors', []) or []
    if errors:
        validation['warnings'] = list(errors)
        total_rows = validation['summary']['player_profile_rows']
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # APL ≈ 526 active players → 1 profile row each. WARN-only floor = 400 (issue
    # #69); a fallback keeps the DAG non-failed (soft).
    rows = validation['summary']['player_profile_rows']
    if rows < 400:
        if validation['summary']['fallback']:
            validation['warnings'].append(
                f"player_profile R0.2B_FALLBACK: rows={rows} "
                f"players={validation['summary']['player_profile_players']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_profile row count: {rows} < 400")

    # player_season_stats (#751 PR3b) — a strict subset of profile (the Season
    # picker can miss for transferred/multi-competition players). WARN-only
    # floor: low coverage never fails the run, it just flags a possible picker
    # regression. 300 is a conservative floor below ~526 active APL players.
    season_rows = validation['summary']['player_season_stats_rows']
    if season_rows < 300:
        validation['warnings'].append(
            f"Low player_season_stats row count: {season_rows} < 300 "
            f"(Season-tab picker coverage)")

    logger.info("Player data validation complete: %s", validation['status'])
    logger.info("Summary: %s", validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


def validate_player_freshness(**context) -> None:
    """Hard-fail when the player bronze tables stop refreshing (#751).

    The scrape task soft-exits (R0.2B_FALLBACK, exit 2) when SofaScore's anti-bot
    returns 403, so the DAG stays green while data silently goes stale. A direct
    MAX(_ingested_at) check surfaces a multi-week stall. ERROR-severity: a stale
    table fails the task (the Telegram summary fires first). 8-day window gives
    one missed weekly run of grace.

    Only runs when the gate let the player capture through (Saturday / manual),
    so it never fires on weekday daily runs that skip the player branch.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    checks = [
        CHECK.freshness(
            'bronze.sofascore_player_profile',
            ts_col='_ingested_at', max_age_hours=192, severity='ERROR',
        ),
        CHECK.freshness(
            'bronze.sofascore_player_season_stats',
            ts_col='_ingested_at', max_age_hours=192, severity='ERROR',
        ),
    ]
    # raise_on_error=False so the Telegram summary lands before we re-raise on
    # ERROR-severity failures (same pattern as dag_transform_e4).
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_player_freshness: %s", report.summary())
    telegram_dq_summary(report, header='SofaScore player Bronze freshness')

    if report.errors:
        raise AirflowException(
            f"SofaScore player Bronze freshness failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors)
        )


# Build arguments for bash command — club leagues only (see CLUB_LEAGUES
# above); tournament leagues get their own dedicated task below.
leagues_str = ','.join(CLUB_LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_sofascore',
    default_args=SCRAPER_ARGS,
    description="Ingest football statistics from SofaScore (matches daily + players weekly)",
    schedule=SCHEDULES.get("dag_ingest_sofascore"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofascore', ['scraping', 'sofascore', 'bronze']),
    max_active_runs=1,
    params={
        'leagues': SOFASCORE_LEAGUES,
        # UI-configurable season for the 10-season backfill (#711, epic #708).
        # Default = CURRENT_SEASON so the daily scheduled run is unchanged;
        # override via "Trigger DAG w/ config" to ingest a past season. The
        # season is the APL start year (2016 = 2016/17); the runner derives the
        # soccerdata short form ("1617") from it.
        'season': Param(
            default=CURRENT_SEASON,
            type='integer',
            minimum=2000,
            maximum=CURRENT_SEASON,
            title='Season (start year)',
            description=(
                'APL season start year (2016 = 2016/17). Default = current '
                'season for the daily run. Override here to backfill a past '
                'season (2016…2024 closes the 10-season history → unblocks '
                'fouls home-vs-away for #558).'
            ),
        ),
        # #782: force the heavy per-player capture in this run. Normally it
        # auto-runs on the Saturday master-pipeline invocation (see
        # _gate_player_capture); set True via "Trigger DAG w/ config" for an
        # on-demand refresh.
        'run_players': False,
    },
    doc_md="""
    ## SofaScore Data Ingestion

    This DAG scrapes football statistics from SofaScore.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores, venues (daily)
    - **Per-match capture**: player_ratings, event_player_stats, match_stats,
      shotmap — one warmed session plus exact API fetches (daily)
    - **Per-player**: profile + season-aggregate stats (weekly, gated — see below)

    ### One source = one DAG (#782)

    The former weekly `dag_ingest_sofascore_players` is folded in here. The
    per-player capture (~526 players; one nav + in-page fetches since #842)
    is gated by a `ShortCircuitOperator`:

    - auto-runs on the **Saturday master-pipeline run** (weekly cadence);
    - or on demand via **"Trigger DAG w/ config"** with `run_players=true`;
    - **skipped** on weekday runs.

    ### Daily limits (issue #69)

    No per-endpoint cap by default. Override via ENV on dev/staging:
    `SS_SHOTMAP_LIMIT`, `SS_EPS_LIMIT`, `SS_MATCH_STATS_LIMIT`,
    `SS_PLAYER_CAPTURE_LIMIT` (positive int → cap).

    ### Incremental full-state refresh (#842)

    The consolidated `match_capture` captures only matches not marked complete
    in `bronze.sofascore_match_capture_status` (finished-match data is immutable;
    re-capturing the whole season daily burned ~1.6 GB of residential proxy).
    Endpoint states distinguish terminal empty/404 from transient misses. Data
    frames are merged with the existing partition and rewritten
    (`replace_partitions=['league','season']` + completeness guard). A day
    with no new finished matches is a clean no-op (zero proxy spend).

    **Manual full re-capture**: run the scraper with `--force-replace`
    (bypasses skip-existing AND the guard), or `TRUNCATE
    iceberg.bronze.sofascore_<table>` via `make shell-trino`, then trigger
    the DAG.

    ### Notes

    - Uses soccerdata library wrapper
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_sofascore_data',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {SCHEDULE_RESULT_PATH} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {{{{ params.season }}}} \\
    --output {SCHEDULE_RESULT_PATH}
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    # #920 Phase 1: one dedicated schedule task per tournament league (e.g.
    # INT-World Cup) — a mixed --leagues call would drop it (the runner's
    # #920 bridge needs a dedicated single-league call to resolve the active
    # tournament season). Reuses the same `{{ params.season }}` Jinja value
    # as the club task above; the runner itself resolves it to the real
    # tournament year, or cleanly no-ops (exit 0) outside the tournament
    # window — so this task stays in the graph year-round as a cheap no-op
    # rather than appearing/disappearing with the calendar. Once canonical
    # metadata is ready, onboarding the next tournament is an activation flag
    # change in the SofaScore registry — no global league-list change.
    tournament_schedule_tasks = {}
    for _t_league in TOURNAMENT_LEAGUES:
        _t_slug = _league_slug(_t_league)
        tournament_schedule_tasks[_t_league] = BashOperator(
            task_id=f'scrape_sofascore_data_{_t_slug}',
            bash_command=f"""
cd /opt/airflow && \\
rm -f /tmp/sofascore_result_{_t_slug}.json && \\
python dags/scripts/run_sofascore_scraper.py \\
    --leagues "{_t_league}" \\
    --season {{{{ params.season }}}} \\
    --output /tmp/sofascore_result_{_t_slug}.json
""",
            env={
                'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
                'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
                'HOME': '/home/airflow',
            },
            append_env=True,
        )

    # #751 PR1 — consolidated per-match capture: one Camoufox navigation per
    # match writes both player_ratings and event_player_stats.
    # from the same /lineups (+/event) payload. Depends on freshly written
    # bronze.sofascore_schedule (runner reads finished match_ids there; falls
    # back to capture discovery when empty). Exit 2 = graceful R0.2B_FALLBACK
    # (soft success so validate_data runs); exit 3 = completeness-guard refusal
    # (propagates as a real failure).
    # #920 Phase 1: one match_capture task per active registry league (club +
    # every tournament) — the runner only ever reads leagues[0]
    # (_run_match_capture), so a single shared task could never cover more
    # than one league regardless of how many are configured. The club
    # league keeps the original task_id/output path (MATCH_CAPTURE_RESULT_PATH)
    # so validate_data()'s club-calibrated row floors and
    # TestSeasonRenderedFromParams stay byte-identical; every other league
    # gets its own task + output path, while tournament result files are read
    # by validate_data()
    # (per-competition floors are #920 Phase 2, out of scope here).
    match_capture_tasks = {}
    for _league in SOFASCORE_LEAGUES:
        _is_primary_club = _league == PRIMARY_CLUB_LEAGUE
        _mc_slug = _league_slug(_league)
        _mc_task_id = (
            'scrape_match_capture'
            if _is_primary_club
            else f'scrape_match_capture_{_mc_slug}'
        )
        _mc_output = (
            MATCH_CAPTURE_RESULT_PATH
            if _is_primary_club
            else f'/tmp/sofascore_match_capture_result_{_mc_slug}.json'
        )
        match_capture_tasks[_league] = BashOperator(
            task_id=_mc_task_id,
            bash_command=f"""
cd /opt/airflow && \\
rm -f {_mc_output} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity match_capture \\
    --league "{_league}" \\
    --season {{{{ params.season }}}} \\
    --output {_mc_output}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (match_capture) — propagating as soft success."
    exit 0
fi
exit $rc
""",
            env={
                'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
                'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
                'HOME': '/home/airflow',
            },
            append_env=True,
        )

    # Kept as a plain name for the (unchanged) club-only downstream wiring
    # below (player_capture is explicitly out of #920 Phase 1 scope and
    # stays keyed off the club league only, as before).
    scrape_match_capture_task = match_capture_tasks[PRIMARY_CLUB_LEAGUE]

    # #751 PR2: shotmap (#22) + match_stats (#25) no longer have their own tls
    # tasks — both come from exact API calls in the consolidated capture above.

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    # Freshness gate over the Bronze tables themselves (issue #751). Runs
    # all_done so a 403 soft-fail upstream still triggers the staleness alert.
    validate_bronze_freshness_task = PythonOperator(
        task_id='validate_bronze_freshness',
        python_callable=validate_bronze_freshness,
        trigger_rule='all_done',
    )

    # ---- Per-player capture (#782) — gated to Saturday / manual -------------
    # Folded from the former weekly dag_ingest_sofascore_players. The gate
    # short-circuits the player tasks to `skipped` on weekday runs.
    gate_player_capture_task = ShortCircuitOperator(
        task_id='gate_player_capture',
        python_callable=_gate_player_capture,
        # all_done: player ids come from bronze.sofascore_player_ratings (written
        # by match_capture); even a soft-failed match_capture leaves prior rows,
        # so let the gate decide regardless of upstream state.
        trigger_rule='all_done',
    )

    scrape_player_capture_task = BashOperator(
        task_id='scrape_player_capture',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {PLAYER_CAPTURE_RESULT_PATH} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_capture \\
    --league "{PRIMARY_CLUB_LEAGUE}" \\
    --season {{{{ params.season }}}} \\
    {_limit_arg(PLAYER_CAPTURE_LIMIT)} \\
    --output {PLAYER_CAPTURE_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (player_capture) — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    validate_player_data_task = PythonOperator(
        task_id='validate_player_data',
        python_callable=validate_player_data,
        trigger_rule='all_done',
    )

    validate_player_freshness_task = PythonOperator(
        task_id='validate_player_freshness',
        python_callable=validate_player_freshness,
        trigger_rule='all_done',
    )

    # Matches chain (daily): one club schedule batch feeds one dedicated
    # match-capture task per active split-year competition. The legacy primary
    # keeps its original task id/result path.
    for _club_league in CLUB_LEAGUES:
        scrape_data_task >> match_capture_tasks[_club_league]
        match_capture_tasks[_club_league] >> validate_data_task
    validate_data_task >> validate_bronze_freshness_task

    # #920 Phase 1: same chain per tournament league, in parallel with the
    # club chain above — each tournament's own schedule task feeds its own
    # match_capture task, both converging on the shared validate_data_task
    # (trigger_rule='all_done' already tolerates multiple upstreams).
    for _t_league in TOURNAMENT_LEAGUES:
        tournament_schedule_tasks[_t_league] >> match_capture_tasks[_t_league]
        match_capture_tasks[_t_league] >> validate_data_task

    # Per-player branch (weekly/manual), gated after match_capture so the player
    # ids in bronze.sofascore_player_ratings are fresh; skipped on weekday runs.
    # The Saturday master-pipeline boundary intentionally opens the gate.
    scrape_match_capture_task >> gate_player_capture_task >> scrape_player_capture_task
    scrape_player_capture_task >> validate_player_data_task >> validate_player_freshness_task
