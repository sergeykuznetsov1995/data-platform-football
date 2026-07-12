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

import hashlib
from datetime import datetime
from pathlib import Path
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


SCHEDULE_RESULT_PATH = "/tmp/sofascore_result.json"
# #751 PR1+PR2 — one consolidated warmed Camoufox session writes ALL FOUR
# per-match tables: player_ratings, event_player_stats, match_stats, shotmap
# (replaces four separate tls passes).
MATCH_CAPTURE_RESULT_PATH = "/tmp/sofascore_match_capture_result.json"
# #782 — per-player profile + season_stats capture (formerly the weekly
# dag_ingest_sofascore_players) now runs here behind the Saturday/manual gate.
PLAYER_CAPTURE_RESULT_PATH = "/tmp/sofascore_player_capture_result.json"
RESULT_ROOT = "/tmp/sofascore"
# BashOperator gets ``run_id`` through a templated environment variable (not
# interpolated into shell source).  The hash is therefore both shell-safe and
# identical to :func:`_result_path` below.  A retry of the same DagRun reuses
# its own files, while another run can never observe them.
RESULT_DIR_BASH = r"""
SOFASCORE_RESULT_TOKEN="$(python -c 'import hashlib, os; print(hashlib.sha256(os.environ["SOFASCORE_DAG_RUN_ID"].encode("utf-8")).hexdigest())')"
SOFASCORE_RESULT_DIR="/tmp/sofascore/${SOFASCORE_RESULT_TOKEN}"
mkdir -p "$SOFASCORE_RESULT_DIR"
""".strip()
SEASON_PLAN_XCOM = "{{ ti.xcom_pull(task_ids='prepare_sofascore_season_plan') }}"
TARGET_PLAN_XCOM = "{{ ti.xcom_pull(task_ids='prepare_sofascore_target_plan') }}"
PLAYER_PLAN_XCOM = "{{ ti.xcom_pull(task_ids='prepare_sofascore_player_plan') }}"


def _context_run_id(context: Dict[str, Any]) -> str | None:
    """Read Airflow's immutable DagRun id without trusting a result payload."""

    value = context.get("run_id")
    if value is None:
        value = getattr(context.get("ti"), "run_id", None)
    if value is None:
        value = getattr(context.get("dag_run"), "run_id", None)
    token = str(value).strip() if value is not None else ""
    return token or None


def _result_path(legacy_path: str, context: Dict[str, Any]) -> str:
    """Return the current DagRun's private result path.

    The legacy path is retained only for direct unit-call compatibility where
    no Airflow context exists.  Real Airflow callbacks always provide run_id.
    """

    run_id = _context_run_id(context)
    if run_id is None:
        return legacy_path
    token = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return str(Path(RESULT_ROOT) / token / Path(legacy_path).name)


def _require_successful_producers(
    context: Dict[str, Any], task_ids: List[str]
) -> None:
    """Fail an ``all_done`` validator unless every required producer won.

    A producer can write a JSON file and then fail (for example a post-write
    completeness guard).  Run-scoped paths prevent stale cross-run reads;
    checking TaskInstance state also prevents accepting that failed attempt.
    """

    dag_run = context.get("dag_run")
    if dag_run is None:
        return
    getter = getattr(dag_run, "get_task_instance", None)
    if getter is None:
        # Direct unit hooks may use a tiny DagRun stand-in. Real Airflow
        # DagRun objects always expose this DB-backed lookup.
        return
    failed: List[str] = []
    for task_id in task_ids:
        try:
            task_instance = getter(task_id)
        except Exception:  # Airflow/DB lookup failure is fail-closed below.
            task_instance = None
        state = getattr(task_instance, "state", None)
        if str(state).lower() != "success":
            failed.append(f"{task_id}={state or 'missing'}")
    if failed:
        raise AirflowException(
            "Required SofaScore producer did not succeed: " + ", ".join(failed)
        )


def _schedule_task_id(league: str) -> str:
    if league == PRIMARY_CLUB_LEAGUE:
        return "scrape_sofascore_data"
    return f"scrape_sofascore_data_{_league_slug(league)}"


def _match_capture_task_id(league: str) -> str:
    if league == PRIMARY_CLUB_LEAGUE:
        return "scrape_match_capture"
    return f"scrape_match_capture_{_league_slug(league)}"


def _player_capture_task_id(league: str) -> str:
    if league == PRIMARY_CLUB_LEAGUE:
        return "scrape_player_capture"
    return f"scrape_player_capture_{_league_slug(league)}"


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
            unusable.append(f"{league!r} has no activatable SofaScore seasons")
            continue
        if not source_seasons & configured_seasons:
            unusable.append(
                f"{league!r} has no SofaScore season matching competitions.yaml"
            )
            continue

        if is_single_year_competition(league):
            active_season = get_active_season(league)
            target_season = str(active_season) if active_season is not None else None
        else:
            target_season = (
                f"{CURRENT_SEASON % 100:02d}{(CURRENT_SEASON + 1) % 100:02d}"
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


# Split active registry scope by season convention for floor/window logic.
# Source IDs, manifests and Bronze partitions are competition-scoped, so every
# competition is fanned out to its own schedule and match tasks.
CLUB_LEAGUES = [lg for lg in SOFASCORE_LEAGUES if not is_single_year_competition(lg)]
TOURNAMENT_LEAGUES = [lg for lg in SOFASCORE_LEAGUES if is_single_year_competition(lg)]

# The consolidated schedule and weekly player branches are still club-shaped.
# Surface a clear import error instead of failing later at CLUB_LEAGUES[0].
if not CLUB_LEAGUES:
    raise AirflowException(
        "SofaScore registry has active tournaments, but none is a usable "
        "split-year competition required by the club/player ingestion branch"
    )

# Preserve the historical EPL task ids/result paths even when another
# split-year competition is activated ahead of it in the ID-sorted registry.
_LEGACY_PRIMARY_CLUB = "ENG-Premier League"
PRIMARY_CLUB_LEAGUE = (
    _LEGACY_PRIMARY_CLUB if _LEGACY_PRIMARY_CLUB in CLUB_LEAGUES else CLUB_LEAGUES[0]
)
CLUB_LEAGUES = [PRIMARY_CLUB_LEAGUE] + [
    lg for lg in CLUB_LEAGUES if lg != PRIMARY_CLUB_LEAGUE
]


def _dag_season_arg(league: str) -> str:
    """Return a club Jinja value or the configured calendar-year season."""

    if not is_single_year_competition(league):
        return "{{ params.season }}"
    configured = set()
    for value in get_competition_seasons(league):
        try:
            configured.add(int(value))
        except (TypeError, ValueError):
            continue
    source = SofaScoreCatalog.load().competition(league)
    available = set()
    for season in source.seasons:
        try:
            if season.activatable and season.canonical_season is not None:
                available.add(int(season.canonical_season))
        except (TypeError, ValueError):
            continue
    candidates = configured & available
    if not candidates:
        raise AirflowException(
            f"{league!r} has no shared configured/source calendar-year season"
        )
    # Parse-time topology must not depend on a tournament's live date window:
    # after its grace period the task remains importable and becomes a manifest
    # no-op. The latest configured edition is the stable scheduled partition.
    return str(max(candidates))


# #920 Phase 2: validate_data floors derived from the same calibrated bases
# as utils.config.PER_LEAGUE_FLOOR_BASES instead of inline literals. Keys are
# the run-JSON summary fields (this validator reads the current run's output,
# not Trino — see validate_bronze_freshness for the staleness guard).
# unit 'match' scales with the competition's scheduled match count, 'team'
# with team_count. WARN-only semantics unchanged.
_SS_FLOOR_BASES: Dict[str, tuple] = {
    "schedule_rows": ("match", 100),
    "league_table_rows": ("team", 10),
    "player_ratings_rows": ("match", 300),
    "shotmap_rows": ("match", 300),
    "event_player_stats_rows": ("match", 10_000),
    "match_stats_rows": ("match", 10_000),
    "venue_rows": ("match", 300),
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
        key: _scale(unit, base, league) for key, (unit, base) in _SS_FLOOR_BASES.items()
    }


def _capture_noop(capture_result: Dict[str, Any]) -> bool:
    """#842 incremental match_capture: True when the run resolved matches but
    skipped them ALL (already in bronze) with no fallback/errors — a clean
    no-op that wrote nothing by design (off-season / no new finished matches).
    """
    return bool(
        capture_result
        and not capture_result.get("fallback")
        and not (capture_result.get("errors") or [])
        and capture_result.get("matches_total", 0) > 0
        and capture_result.get("matches_skipped_existing", 0)
        >= capture_result.get("matches_total", 0)
    )


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality across both scrape tasks (schedule+league_table
    and player_ratings).
    """
    import logging

    logger = logging.getLogger(__name__)

    _require_successful_producers(
        context,
        [
            *[_schedule_task_id(league) for league in SOFASCORE_LEAGUES],
            *[_match_capture_task_id(league) for league in SOFASCORE_LEAGUES],
        ],
    )
    schedule_path = _result_path(SCHEDULE_RESULT_PATH, context)
    capture_path = _result_path(MATCH_CAPTURE_RESULT_PATH, context)
    schedule_result = _load_result(schedule_path, logger)
    # #751 PR1+PR2: ratings + event_player_stats + match_stats + shotmap now all
    # come from ONE consolidated capture run (single result file carrying
    # `rows`/`matches_with_ratings`, `eps_rows`/`eps_matches`,
    # `match_stats_rows`/`match_stats_matches`, `shotmap_rows`/`shotmap_matches`).
    capture_result = _load_result(capture_path, logger)

    if not schedule_result:
        raise AirflowException(
            f"Schedule results file {schedule_path} missing or unreadable"
        )
    if not capture_result:
        raise AirflowException(
            f"Match capture results file {capture_path} missing or unreadable"
        )
    primary_errors = [
        *list(schedule_result.get("errors") or []),
        *list(capture_result.get("errors") or []),
    ]
    if (
        schedule_result.get("skipped")
        or schedule_result.get("fallback")
        or capture_result.get("skipped")
        or capture_result.get("fallback")
        or primary_errors
    ):
        raise AirflowException(
            "Primary SofaScore capture did not complete successfully: "
            f"errors={primary_errors!r}"
        )

    validation = {
        "status": "success",
        "warnings": [],
        "summary": {
            "schedule_rows": schedule_result.get("schedule_rows", 0),
            "league_table_rows": schedule_result.get("league_table_rows", 0),
            "player_ratings_rows": capture_result.get("rows", 0),
            "player_ratings_matches": capture_result.get("matches_with_ratings", 0),
            "player_ratings_fallback": capture_result.get("fallback", False),
            "shotmap_rows": capture_result.get("shotmap_rows", 0),
            "shotmap_matches": capture_result.get("shotmap_matches", 0),
            "shotmap_fallback": capture_result.get("fallback", False),
            "event_player_stats_rows": capture_result.get("eps_rows", 0),
            "event_player_stats_matches": capture_result.get("eps_matches", 0),
            "event_player_stats_fallback": capture_result.get("fallback", False),
            "match_stats_rows": capture_result.get("match_stats_rows", 0),
            "match_stats_matches": capture_result.get("match_stats_matches", 0),
            "match_stats_fallback": capture_result.get("fallback", False),
            # venue (#753) — one row per match from the same capture pass.
            "venue_rows": capture_result.get("venue_rows", 0),
            "venue_matches": capture_result.get("venue_matches", 0),
            "venue_fallback": capture_result.get("fallback", False),
            # #842 incremental capture bookkeeping.
            "matches_total": capture_result.get("matches_total", 0),
            "matches_skipped_existing": capture_result.get(
                "matches_skipped_existing", 0
            ),
            "tables": (
                schedule_result.get("tables", []) + capture_result.get("tables", [])
            ),
        },
    }

    errors: List[str] = []
    errors.extend(schedule_result.get("errors", []) or [])
    errors.extend(capture_result.get("errors", []) or [])
    if errors:
        validation["warnings"] = errors
        total_rows = sum(
            [
                validation["summary"]["schedule_rows"],
                validation["summary"]["league_table_rows"],
                validation["summary"]["player_ratings_rows"],
                validation["summary"]["shotmap_rows"],
                validation["summary"]["event_player_stats_rows"],
                validation["summary"]["match_stats_rows"],
            ]
        )
        validation["status"] = "partial_success" if total_rows > 0 else "failed"

    # Minimum thresholds (#920 Phase 2: derived per competitions.yaml volumes,
    # not APL literals — see _SS_FLOOR_BASES).
    floors = _summed_club_floors()
    capture_floors = _competition_floors(PRIMARY_CLUB_LEAGUE)
    if validation["summary"]["schedule_rows"] < floors["schedule_rows"]:
        validation["warnings"].append(
            "Low schedule row count - possible scraping issue"
        )

    if validation["summary"]["league_table_rows"] < floors["league_table_rows"]:
        validation["warnings"].append(
            "Low league_table row count - possible scraping issue"
        )

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
            validation["summary"]["matches_total"],
        )

    # Full-season volume floors are diagnostic only: early rounds and small
    # competitions can be legitimately smaller. Required endpoint state is a
    # hard gate in ``run_sofascore_dq``; fallback/errors already failed above.
    for summary_key, floor_key in (
        ("player_ratings_rows", "player_ratings_rows"),
        ("shotmap_rows", "shotmap_rows"),
        ("event_player_stats_rows", "event_player_stats_rows"),
        ("match_stats_rows", "match_stats_rows"),
        ("venue_rows", "venue_rows"),
    ):
        rows = validation["summary"][summary_key]
        if not capture_noop and rows < capture_floors[floor_key]:
            validation["warnings"].append(
                f"Low {summary_key}: {rows} < {capture_floors[floor_key]}"
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
        _club_schedule_path = _result_path(
            f"/tmp/sofascore_result_{_slug}.json", context
        )
        _club_capture_path = _result_path(
            f"/tmp/sofascore_match_capture_result_{_slug}.json", context
        )
        _club_schedule = _load_result(_club_schedule_path, logger)
        _club_capture = _load_result(
            _club_capture_path, logger
        )
        _club_summary: Dict[str, Any] = {}
        if not _club_schedule:
            raise AirflowException(
                f"{_club_league}: schedule result file {_club_schedule_path} "
                "missing or unreadable"
            )
        if (
            _club_schedule.get("skipped")
            or _club_schedule.get("fallback")
            or _club_schedule.get("errors")
        ):
            raise AirflowException(
                f"{_club_league}: schedule producer returned skipped/fallback/errors"
            )
        elif not _club_schedule.get("skipped"):
            for err in _club_schedule.get("errors") or []:
                validation["warnings"].append(f"{_club_league}: {err}")
            for key in ("schedule_rows", "league_table_rows"):
                rows = _club_schedule.get(key, 0)
                _club_summary[key] = rows
                if rows < _club_floors[key]:
                    validation["warnings"].append(
                        f"{_club_league}: low {key}: {rows} < {_club_floors[key]}"
                    )
        if not _club_capture:
            raise AirflowException(
                f"{_club_league}: match_capture result file "
                f"{_club_capture_path} missing or unreadable"
            )
        if (
            _club_capture.get("skipped")
            or _club_capture.get("fallback")
            or _club_capture.get("errors")
        ):
            raise AirflowException(
                f"{_club_league}: match capture returned skipped/fallback/errors"
            )
        elif not _club_capture.get("skipped") and not _capture_noop(_club_capture):
            for err in _club_capture.get("errors") or []:
                validation["warnings"].append(f"{_club_league}: {err}")
            for key, field in (
                ("player_ratings_rows", "rows"),
                ("shotmap_rows", "shotmap_rows"),
                ("event_player_stats_rows", "eps_rows"),
                ("match_stats_rows", "match_stats_rows"),
                ("venue_rows", "venue_rows"),
            ):
                rows = _club_capture.get(field, 0)
                _club_summary[key] = rows
                if rows < _club_floors[key]:
                    validation["warnings"].append(
                        f"{_club_league}: low {key}: {rows} < {_club_floors[key]}"
                    )
        if _club_summary:
            validation["summary"][f"club_{_slug}"] = _club_summary

    # #920 Phase 2: tournament legs — every enabled competition has a required
    # result. Full-season row floors stay warnings because an early-stage or
    # small tournament can legitimately be below them; ``run_sofascore_dq``
    # below hard-fails canonical manifest completeness for every planned event.
    for _t_league in TOURNAMENT_LEAGUES:
        _slug = _league_slug(_t_league)
        _t_floors = {
            k: _scale(u, b, _t_league) for k, (u, b) in _SS_FLOOR_BASES.items()
        }
        _t_summary: Dict[str, Any] = {}

        _t_schedule_path = _result_path(
            f"/tmp/sofascore_result_{_slug}.json", context
        )
        _t_capture_path = _result_path(
            f"/tmp/sofascore_match_capture_result_{_slug}.json", context
        )
        _t_schedule = _load_result(_t_schedule_path, logger)
        # The runner ALWAYS writes its output file — out-of-window no-ops
        # write the 'skipped' marker. A missing/unreadable file therefore
        # means the runner died before writing (OOM/timeout after the bash
        # rm -f) — WARN, don't silently pass (review hardening). Only the
        # explicit 'skipped' marker is the healthy silent state.
        if not _t_schedule:
            raise AirflowException(
                f"{_t_league}: schedule result file {_t_schedule_path} "
                "missing or unreadable"
            )
        if not _t_schedule.get("skipped") and (
            _t_schedule.get("fallback") or _t_schedule.get("errors")
        ):
            raise AirflowException(
                f"{_t_league}: schedule producer returned fallback/errors"
            )
        elif not _t_schedule.get("skipped"):
            for err in _t_schedule.get("errors") or []:
                validation["warnings"].append(f"{_t_league}: {err}")
            for key in ("schedule_rows", "league_table_rows"):
                rows = _t_schedule.get(key, 0)
                _t_summary[key] = rows
                if rows < _t_floors[key]:
                    validation["warnings"].append(
                        f"{_t_league}: low {key}: {rows} < {_t_floors[key]}"
                    )

        _t_capture = _load_result(_t_capture_path, logger)
        if not _t_capture:
            raise AirflowException(
                f"{_t_league}: match_capture result file {_t_capture_path} "
                "missing or unreadable"
            )
        if not _t_capture.get("skipped") and (
            _t_capture.get("fallback") or _t_capture.get("errors")
        ):
            raise AirflowException(
                f"{_t_league}: match capture returned fallback/errors"
            )
        elif not _t_capture.get("skipped") and not _capture_noop(_t_capture):
            for err in _t_capture.get("errors") or []:
                validation["warnings"].append(f"{_t_league}: {err}")
            for key, field in (
                ("player_ratings_rows", "rows"),
                ("shotmap_rows", "shotmap_rows"),
                ("event_player_stats_rows", "eps_rows"),
                ("match_stats_rows", "match_stats_rows"),
                ("venue_rows", "venue_rows"),
            ):
                rows = _t_capture.get(field, 0)
                _t_summary[key] = rows
                if rows < _t_floors[key]:
                    validation["warnings"].append(
                        f"{_t_league}: low {key}: {rows} < {_t_floors[key]}"
                    )

        if _t_summary:
            validation["summary"][f"tournament_{_slug}"] = _t_summary

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation["warnings"]:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation["status"] == "failed":
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


def run_sofascore_dq(**context) -> Dict[str, Any]:
    """Hard barrier over the canonical manifest-completeness result.

    Each runner writes ``endpoint_completeness=1`` only after
    ``validate_manifest_completeness(...).require()`` has accepted every
    planned target and the normalized commit has succeeded.  Checking every
    competition result here makes the signed plan's ``run_sofascore_dq`` task
    real and keeps warning-only volume floors from hiding a missing endpoint.
    """

    import logging

    logger = logging.getLogger(__name__)
    _require_successful_producers(context, ["validate_data"])
    checked: List[str] = []
    for league in SOFASCORE_LEAGUES:
        slug = _league_slug(league)
        schedule_legacy = (
            SCHEDULE_RESULT_PATH
            if league == PRIMARY_CLUB_LEAGUE
            else f"/tmp/sofascore_result_{slug}.json"
        )
        capture_legacy = (
            MATCH_CAPTURE_RESULT_PATH
            if league == PRIMARY_CLUB_LEAGUE
            else f"/tmp/sofascore_match_capture_result_{slug}.json"
        )
        for phase, legacy_path in (
            ("season", schedule_legacy),
            ("match", capture_legacy),
        ):
            path = _result_path(legacy_path, context)
            result = _load_result(path, logger)
            if not result:
                raise AirflowException(
                    f"{league} {phase} DQ result {path} is missing or unreadable"
                )
            if result.get("skipped"):
                if league not in TOURNAMENT_LEAGUES:
                    raise AirflowException(
                        f"{league} {phase} cannot be skipped in production"
                    )
                continue
            if (
                result.get("fallback")
                or result.get("errors")
                or result.get("endpoint_completeness") != 1.0
            ):
                raise AirflowException(
                    f"{league} {phase} canonical endpoint completeness failed"
                )
            checked.append(f"{league}:{phase}")
    return {"status": "success", "checked": checked, "count": len(checked)}


def validate_bronze_freshness(**context) -> None:
    """Telegram-alert when bronze.sofascore_* stops refreshing (issue #751).

    The producer and result validator now fail hard on 403/fallback. This
    independent Bronze check still catches a scheduler/storage stall where no
    current rows were committed. ``validate_data``
    only checks the row_count of the *current* run's JSON output — pre-existing
    stale rows still pass that floor. A direct MAX(_ingested_at) freshness
    check is what surfaces a multi-day ingestion stall.

    WARNING severity is diagnostic here because the preceding producer,
    endpoint-manifest and committed-state gates already fail hard. A clean
    endpoint no-op may legitimately leave immutable match tables unchanged.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    # #842 incremental match_capture: on a clean skip-existing no-op the match
    # tables legitimately do not change. Schedule + standings are still expected
    # to refresh daily and must never be hidden by that no-op.
    capture_result = _load_result(
        _result_path(MATCH_CAPTURE_RESULT_PATH, context), logger
    )
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
            "bronze.sofascore_match_stats",
            condition="COALESCE(name, '') <> '' OR COALESCE(stat_name, '') <> ''",
            warn_threshold=0.99,
            error_threshold=0.95,
            severity="WARNING",
            name="match_stats_labelled",
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
    telegram_dq_summary(report, header="SofaScore Bronze freshness")


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

    _require_successful_producers(
        context,
        [_match_capture_task_id(league) for league in SOFASCORE_LEAGUES],
    )

    params = context.get("params") or {}
    if params.get("run_players"):
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
    _require_successful_producers(
        context,
        [_player_capture_task_id(league) for league in SOFASCORE_LEAGUES],
    )
    primary_path = _result_path(PLAYER_CAPTURE_RESULT_PATH, context)
    result = _load_result(primary_path, logger)

    if not result:
        raise AirflowException(
            f"player_capture results file {primary_path} "
            f"missing or unreadable"
        )
    if result.get("skipped") or result.get("fallback") or result.get("errors"):
        raise AirflowException(
            "Primary player capture returned skipped/fallback/errors"
        )

    validation = {
        "status": "success",
        "warnings": [],
        "summary": {
            "player_profile_rows": result.get("rows", 0),
            "player_profile_players": result.get("profile_players", 0),
            "player_season_stats_rows": result.get("season_stats_rows", 0),
            "player_season_stats_players": result.get("season_stats_players", 0),
            "fallback": result.get("fallback", False),
            "tables": result.get("tables", []),
        },
    }

    errors: List[str] = result.get("errors", []) or []
    if errors:
        validation["warnings"] = list(errors)
        total_rows = validation["summary"]["player_profile_rows"]
        validation["status"] = "partial_success" if total_rows > 0 else "failed"

    # APL ≈ 526 active players → 1 profile row each. This legacy row floor is
    # diagnostic; fallback/errors already fail above and the 95% universe
    # coverage gate below is authoritative.
    rows = validation["summary"]["player_profile_rows"]
    if rows < 400:
        if validation["summary"]["fallback"]:
            validation["warnings"].append(
                f"player_profile R0.2B_FALLBACK: rows={rows} "
                f"players={validation['summary']['player_profile_players']}"
            )
            if validation["status"] == "success":
                validation["status"] = "partial_success"
        else:
            validation["warnings"].append(f"Low player_profile row count: {rows} < 400")

    # player_season_stats (#751 PR3b) — a strict subset of profile (the Season
    # picker can miss for transferred/multi-competition players). WARN-only
    # floor: low coverage never fails the run, it just flags a possible picker
    # regression. 300 is a conservative floor below ~526 active APL players.
    season_rows = validation["summary"]["player_season_stats_rows"]
    if season_rows < 300:
        validation["warnings"].append(
            f"Low player_season_stats row count: {season_rows} < 300 "
            f"(Season-tab picker coverage)"
        )

    for league in SOFASCORE_LEAGUES:
        if league == PRIMARY_CLUB_LEAGUE:
            continue
        slug = _league_slug(league)
        path = _result_path(
            f"/tmp/sofascore_player_capture_result_{slug}.json", context
        )
        extra = _load_result(path, logger)
        if not extra:
            raise AirflowException(
                f"{league}: player_capture result file {path} missing or unreadable"
            )
        if extra.get("skipped"):
            continue
        if extra.get("fallback") or extra.get("errors"):
            raise AirflowException(
                f"{league}: player capture returned fallback/errors"
            )
        for error in extra.get("errors") or []:
            validation["warnings"].append(f"{league}: {error}")
        universe = int(extra.get("players_total") or 0)
        profiles = int(extra.get("profile_players") or 0)
        coverage = 1.0 if universe == 0 else profiles / universe
        validation["summary"][f"player_{slug}"] = {
            "players_total": universe,
            "profile_players": profiles,
            "profile_coverage": coverage,
            "season_stats_players": int(extra.get("season_stats_players") or 0),
        }
        if universe == 0 or coverage < 0.95 or extra.get("errors"):
            validation["warnings"].append(
                f"{league}: profile coverage {profiles}/{universe} "
                f"({coverage:.2%}) is below 95% or capture reported errors"
            )
            validation["status"] = "failed"

    logger.info("Player data validation complete: %s", validation["status"])
    logger.info("Summary: %s", validation["summary"])
    if validation["warnings"]:
        logger.warning("Warnings: %s", validation["warnings"])

    if validation["status"] == "failed":
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


def validate_player_freshness(**context) -> None:
    """Hard-fail when the player bronze tables stop refreshing (#751).

    The producer/validator already fail hard on fallback. A direct
    MAX(_ingested_at) check additionally surfaces a storage/scheduler stall.
    ERROR-severity: a stale table fails the task (the Telegram summary fires
    first). 8-day window gives one missed weekly run of grace.

    Only runs when the gate let the player capture through (Saturday / manual),
    so it never fires on weekday daily runs that skip the player branch.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    checks = [
        CHECK.freshness(
            "bronze.sofascore_player_profile",
            ts_col="_ingested_at",
            max_age_hours=192,
            severity="ERROR",
        ),
        CHECK.freshness(
            "bronze.sofascore_player_season_stats",
            ts_col="_ingested_at",
            max_age_hours=192,
            severity="ERROR",
        ),
    ]
    # raise_on_error=False so the Telegram summary lands before we re-raise on
    # ERROR-severity failures (same pattern as dag_transform_e4).
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_player_freshness: %s", report.summary())
    telegram_dq_summary(report, header="SofaScore player Bronze freshness")

    if report.errors:
        raise AirflowException(
            f"SofaScore player Bronze freshness failed: {len(report.errors)} error(s). "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors)
        )


# DAG definition
with DAG(
    dag_id="dag_ingest_sofascore",
    default_args=SCRAPER_ARGS,
    description="Ingest football statistics from SofaScore (matches daily + players weekly)",
    schedule=SCHEDULES.get("dag_ingest_sofascore"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get("sofascore", ["scraping", "sofascore", "bronze"]),
    max_active_runs=1,
    # proxy_filter grants one SofaScore lease at a time.  Serializing this DAG
    # avoids avoidable lease rejection/retry storms without an operator-owned
    # Airflow pool and still reuses one warmed session inside each task.
    max_active_tasks=1,
    params={
        "leagues": SOFASCORE_LEAGUES,
        # UI-configurable season for the 10-season backfill (#711, epic #708).
        # Default = CURRENT_SEASON so the daily scheduled run is unchanged;
        # override via "Trigger DAG w/ config" to ingest a past season. The
        # season is the APL start year (2016 = 2016/17); the runner derives the
        # soccerdata short form ("1617") from it.
        "season": Param(
            default=CURRENT_SEASON,
            type="integer",
            minimum=2000,
            maximum=CURRENT_SEASON,
            title="Season (start year)",
            description=(
                "APL season start year (2016 = 2016/17). Default = current "
                "season for the daily run. Override here to backfill a past "
                "season (2016…2024 closes the 10-season history → unblocks "
                "fouls home-vs-away for #558)."
            ),
        ),
        # #782: force the heavy per-player capture in this run. Normally it
        # auto-runs on the Saturday master-pipeline invocation (see
        # _gate_player_capture); set True via "Trigger DAG w/ config" for an
        # on-demand refresh.
        "run_players": False,
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

    After the gate, a third immutable `players` plan rereads squads and the
    freshly committed match-player Bronze rows. Match and player targets can
    therefore never share a stale pre-match snapshot.

    ### Raw-first incremental capture (#842)

    The consolidated `match_capture` captures only matches not marked complete
    in `bronze.sofascore_match_capture_status` (finished-match data is immutable;
    re-capturing the whole season daily burned ~1.6 GB of residential proxy).
    The canonical long manifest distinguishes success, legitimate empty,
    unsupported, retryable failure and schema error per endpoint. Exact JSON is
    retained once and can be replayed with networking disabled. Bronze writes
    are natural-key Iceberg MERGEs; no full partition is read or rewritten. A
    completed same-freshness run is a clean no-op (zero proxy/browser spend).

    **Manual repair**: run the shared runner/backfill with `--force-replace`;
    writes remain incremental and the manifest retains raw lineage.

    ### Notes

    - Discovery is direct JSON with proxy environment disabled.
    - Capture activation is registry-gated to reviewed adult men's tournaments.
    """,
) as dag:
    _competition_season_args = " ".join(
        f'--competition-season "{league}={_dag_season_arg(league)}"'
        for league in SOFASCORE_LEAGUES
    )
    prepare_season_plan_task = BashOperator(
        task_id="prepare_sofascore_season_plan",
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/prepare_sofascore_workload.py \\
    --dag-id dag_ingest_sofascore \\
    --run-id "{{{{ run_id }}}}" \\
    --phase season \\
    {_competition_season_args}
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
        },
        append_env=True,
        do_xcom_push=True,
    )

    prepare_target_plan_task = BashOperator(
        task_id="prepare_sofascore_target_plan",
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/prepare_sofascore_workload.py \\
    --dag-id dag_ingest_sofascore \\
    --run-id "{{{{ run_id }}}}" \\
    --phase targets \\
    {_competition_season_args}
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
        },
        append_env=True,
        do_xcom_push=True,
    )

    prepare_player_plan_task = BashOperator(
        task_id="prepare_sofascore_player_plan",
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/prepare_sofascore_workload.py \\
    --dag-id dag_ingest_sofascore \\
    --run-id "{{{{ run_id }}}}" \\
    --phase players \\
    {_competition_season_args}
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
        },
        append_env=True,
        do_xcom_push=True,
    )

    scrape_data_task = BashOperator(
        task_id="scrape_sofascore_data",
        bash_command=f"""
cd /opt/airflow && \\
{RESULT_DIR_BASH}
rm -f "$SOFASCORE_RESULT_DIR/{Path(SCHEDULE_RESULT_PATH).name}" && \\
python dags/scripts/run_sofascore_scraper.py \\
    --league "{PRIMARY_CLUB_LEAGUE}" \\
    --season {{{{ params.season }}}} \\
    --workload-plan "{SEASON_PLAN_XCOM}" \\
    --output "$SOFASCORE_RESULT_DIR/{Path(SCHEDULE_RESULT_PATH).name}"
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
            "SOFASCORE_DAG_RUN_ID": "{{ run_id }}",
        },
        append_env=True,
    )

    # Preserve the primary EPL task ID/path; add one registry-driven schedule
    # task for every other club or tournament.
    schedule_tasks = {PRIMARY_CLUB_LEAGUE: scrape_data_task}
    for _schedule_league in SOFASCORE_LEAGUES:
        if _schedule_league == PRIMARY_CLUB_LEAGUE:
            continue
        _schedule_slug = _league_slug(_schedule_league)
        _schedule_season = _dag_season_arg(_schedule_league)
        schedule_tasks[_schedule_league] = BashOperator(
            task_id=_schedule_task_id(_schedule_league),
            bash_command=f"""
cd /opt/airflow && \\
{RESULT_DIR_BASH}
rm -f "$SOFASCORE_RESULT_DIR/sofascore_result_{_schedule_slug}.json" && \\
python dags/scripts/run_sofascore_scraper.py \\
    --league "{_schedule_league}" \\
    --season {_schedule_season} \\
    --workload-plan "{SEASON_PLAN_XCOM}" \\
    --output "$SOFASCORE_RESULT_DIR/sofascore_result_{_schedule_slug}.json"
""",
            env={
                "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
                "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
                "HOME": "/home/airflow",
                "SOFASCORE_DAG_RUN_ID": "{{ run_id }}",
            },
            append_env=True,
        )

    # #751 PR1 — consolidated per-match capture: one warmed Camoufox session
    # writes both player_ratings and event_player_stats.
    # from the same /lineups (+/event) payload. Depends on freshly written
    # bronze.sofascore_schedule (runner reads finished match_ids there; falls
    # back to capture discovery when empty). Any non-zero runner exit remains a
    # real producer failure; the all_done validator checks TaskInstance state.
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
        _mc_season = _dag_season_arg(_league)
        _mc_task_id = _match_capture_task_id(_league)
        _mc_output = (
            Path(MATCH_CAPTURE_RESULT_PATH).name
            if _is_primary_club
            else f"sofascore_match_capture_result_{_mc_slug}.json"
        )
        match_capture_tasks[_league] = BashOperator(
            task_id=_mc_task_id,
            bash_command=f"""
cd /opt/airflow && \\
{RESULT_DIR_BASH}
rm -f "$SOFASCORE_RESULT_DIR/{_mc_output}" && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity match_capture \\
    --league "{_league}" \\
    --season {_mc_season} \\
    --workload-plan "{TARGET_PLAN_XCOM}" \\
    --output "$SOFASCORE_RESULT_DIR/{_mc_output}"
""",
            env={
                "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
                "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
                "HOME": "/home/airflow",
                "SOFASCORE_DAG_RUN_ID": "{{ run_id }}",
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
        task_id="validate_data",
        python_callable=validate_data,
        trigger_rule="all_done",
    )

    run_sofascore_dq_task = PythonOperator(
        task_id="run_sofascore_dq",
        python_callable=run_sofascore_dq,
    )

    # Freshness gate over the Bronze tables themselves (issue #751). Runs
    # all_done so a failed validator still emits the independent staleness alert.
    validate_bronze_freshness_task = PythonOperator(
        task_id="validate_bronze_freshness",
        python_callable=validate_bronze_freshness,
        trigger_rule="all_done",
    )

    # ---- Per-player capture (#782) — gated to Saturday / manual -------------
    # Folded from the former weekly dag_ingest_sofascore_players. The gate
    # short-circuits the player tasks to `skipped` on weekday runs.
    gate_player_capture_task = ShortCircuitOperator(
        task_id="gate_player_capture",
        python_callable=_gate_player_capture,
        # all_done lets the hook inspect every match producer state and fail
        # explicitly instead of spending proxy bytes on a stale player universe.
        trigger_rule="all_done",
    )

    scrape_player_capture_task = BashOperator(
        task_id="scrape_player_capture",
        bash_command=f"""
cd /opt/airflow && \\
{RESULT_DIR_BASH}
rm -f "$SOFASCORE_RESULT_DIR/{Path(PLAYER_CAPTURE_RESULT_PATH).name}" && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_capture \\
    --league "{PRIMARY_CLUB_LEAGUE}" \\
    --season {{{{ params.season }}}} \\
    --workload-plan "{PLAYER_PLAN_XCOM}" \\
    --output "$SOFASCORE_RESULT_DIR/{Path(PLAYER_CAPTURE_RESULT_PATH).name}"
""",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
            "HOME": "/home/airflow",
            "SOFASCORE_DAG_RUN_ID": "{{ run_id }}",
        },
        append_env=True,
    )

    player_capture_tasks = {
        PRIMARY_CLUB_LEAGUE: scrape_player_capture_task,
    }
    for _player_league in SOFASCORE_LEAGUES:
        if _player_league == PRIMARY_CLUB_LEAGUE:
            continue
        _player_slug = _league_slug(_player_league)
        _player_season = _dag_season_arg(_player_league)
        _player_output = f"sofascore_player_capture_result_{_player_slug}.json"
        player_capture_tasks[_player_league] = BashOperator(
            task_id=_player_capture_task_id(_player_league),
            bash_command=f"""
cd /opt/airflow && \\
{RESULT_DIR_BASH}
rm -f "$SOFASCORE_RESULT_DIR/{_player_output}" && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_capture \\
    --league "{_player_league}" \\
    --season {_player_season} \\
    --workload-plan "{PLAYER_PLAN_XCOM}" \\
    --output "$SOFASCORE_RESULT_DIR/{_player_output}"
""",
            env={
                "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
                "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
                "HOME": "/home/airflow",
                "SOFASCORE_DAG_RUN_ID": "{{ run_id }}",
            },
            append_env=True,
        )

    validate_player_data_task = PythonOperator(
        task_id="validate_player_data",
        python_callable=validate_player_data,
        trigger_rule="all_done",
    )

    validate_player_freshness_task = PythonOperator(
        task_id="validate_player_freshness",
        python_callable=validate_player_freshness,
        trigger_rule="all_done",
    )

    # Season and target plans are immutable phase snapshots. Match target IDs
    # are planned only after all season raw/manifest expansion has committed.
    for _schedule_task in schedule_tasks.values():
        prepare_season_plan_task >> _schedule_task
        _schedule_task >> prepare_target_plan_task

    # Matches chain (daily): one dedicated schedule feeds one match capture.
    for _club_league in CLUB_LEAGUES:
        prepare_target_plan_task >> match_capture_tasks[_club_league]
        match_capture_tasks[_club_league] >> validate_data_task
    validate_data_task >> run_sofascore_dq_task >> validate_bronze_freshness_task

    # #920 Phase 1: same chain per tournament league, in parallel with the
    # club chain above — each tournament's own schedule task feeds its own
    # match_capture task, both converging on the shared validate_data_task
    # (trigger_rule='all_done' already tolerates multiple upstreams).
    for _t_league in TOURNAMENT_LEAGUES:
        prepare_target_plan_task >> match_capture_tasks[_t_league]
        match_capture_tasks[_t_league] >> validate_data_task

    # Per-player branch (weekly/manual). Wait for every competition's match
    # capture and the weekly/manual gate, then create a fresh signed universe
    # from squads + newly committed match Bronze before any player lease.
    for _match_task in match_capture_tasks.values():
        _match_task >> gate_player_capture_task
    gate_player_capture_task >> prepare_player_plan_task
    for _player_task in player_capture_tasks.values():
        prepare_player_plan_task >> _player_task
        _player_task >> validate_player_data_task
    validate_player_data_task >> validate_player_freshness_task
