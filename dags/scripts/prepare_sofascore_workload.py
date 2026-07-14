#!/usr/bin/env python3
"""Prepare immutable network-free SofaScore season/match/player plans."""

# ruff: noqa: E402 -- executable DAG helper must add the repo root first.

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
try:
    sys.path.remove(str(ROOT))
except ValueError:
    pass
sys.path.insert(0, str(ROOT))

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.pipeline import (
    EVENT_PATHS,
    PLAYER_PATHS,
    build_capture_runtime,
    build_event_spec,
    build_player_spec,
)
from scrapers.sofascore.raw_store import RawPayloadNotFound
from scrapers.sofascore.scraper import SofaScoreScraper, _season_label
from scrapers.sofascore.season_pipeline import (
    plan_season_partition,
    squad_player_ids,
)
from scrapers.sofascore.workload_plan import (
    SeasonWorkload,
    load_verified_workload_policy,
    production_season_shape,
    team_count_band,
)
from scrapers.sofascore.workload_runtime import (
    PartitionWorkload,
    build_partitioned_plan,
    load_plan,
    plan_path_for_run,
    write_plan,
)


VALID_PHASES = frozenset({"season", "targets", "players"})
# #946 4d: the weekly player capture is the single most expensive phase, so the
# club leagues are spread over ``modulus`` Saturdays instead of all running on
# every one.  Both defaults are env-tunable because turning the rotation on/off
# must NOT rotate the runtime fingerprint (config, not code).  Below
# MIN_LEAGUES the rotation collapses to modulus=1 (everything due every week) —
# today's two-league cadence therefore stays byte-for-byte unchanged.
PLAYER_ROTATION_MODULUS_DEFAULT = 4
PLAYER_ROTATION_MIN_LEAGUES_DEFAULT = 10


@dataclass(frozen=True)
class CompetitionSeason:
    league: str
    season: str


def _positive_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def player_rotation_modulus(club_league_count: int) -> int:
    """Effective rotation modulus for the current club scope.

    A small scope gains nothing from spreading (each league still needs its
    weekly refresh), so the rotation only engages once the club scope reaches
    ``SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES``.
    """

    modulus = _positive_env_int(
        "SOFASCORE_PLAYER_ROTATION_MODULUS", PLAYER_ROTATION_MODULUS_DEFAULT
    )
    min_leagues = _positive_env_int(
        "SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES", PLAYER_ROTATION_MIN_LEAGUES_DEFAULT
    )
    if club_league_count < min_leagues:
        return 1
    return modulus


def player_rotation_cohort(league: str, *, modulus: int) -> int:
    """Stable rotation slot of one league.

    SHA-256 of the canonical id, not :func:`hash` (PYTHONHASHSEED-salted, so it
    would differ between the DAG gate process and the planner subprocess) and
    not the league's index in the active scope (onboarding one league would
    reshuffle everybody else's slot).
    """

    if isinstance(modulus, bool) or not isinstance(modulus, int) or modulus < 1:
        raise ValueError("rotation modulus must be a positive integer")
    digest = hashlib.sha256(str(league).encode("utf-8")).hexdigest()
    return int(digest, 16) % modulus


def player_rotation_due(
    league: str,
    *,
    rotation_date: Optional[date],
    club_league_count: int,
    is_tournament: bool = False,
    force: bool = False,
) -> bool:
    """True when this league's players must be captured in ``rotation_date``'s week.

    ``rotation_date=None`` means "no rotation" (a caller that does not pass a
    date keeps the pre-#946 behaviour: every league every run).  A manual
    ``force`` run and cup tournaments (their player universe is short-lived and
    explosive) are always due.
    """

    if force or is_tournament or rotation_date is None:
        return True
    modulus = player_rotation_modulus(club_league_count)
    if modulus == 1:
        return True
    # A continuous, Monday-aligned week counter — NOT the ISO week number. In a
    # 53-week ISO year ``iso_week % modulus`` collides week 53 with week 1 of the
    # next year (53 % 4 == 1 == 1 % 4), double-booking one cohort and starving
    # another. ``(toordinal() - 1) // 7`` increments by exactly one per ISO week
    # and buckets Monday–Sunday identically, so two dates in the same ISO week
    # still resolve to the same cohort (the gate/planner determinism invariant).
    week_index = (rotation_date.toordinal() - 1) // 7
    return player_rotation_cohort(league, modulus=modulus) == week_index % modulus


def _parse_rotation_boundary(value: str) -> Optional[date]:
    """Parse the rotation boundary as a plain date or a full ISO datetime.

    The rotation gate resolves the master ``data_interval_end`` (a datetime) and
    the DAG passes that same boundary to the planner, so the planner must accept
    both ``YYYY-MM-DD`` and a timezone-aware ISO datetime, taking its date.
    """

    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _trino_connect():
    try:
        import trino
        from trino.auth import BasicAuthentication
    except ImportError as exc:
        raise RuntimeError(
            "Trino client is required to prepare SofaScore targets"
        ) from exc
    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    if password:
        return trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8443")),
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=user,
        catalog="iceberg",
    )


def _missing_table(exc: BaseException) -> bool:
    message = str(exc).upper()
    return any(
        marker in message
        for marker in ("TABLE_NOT_FOUND", "TABLE NOT FOUND", "DOES NOT EXIST")
    )


def _query_partition_ids(
    table: str,
    column: str,
    *,
    league: str,
    season: str,
    predicate: str = "",
) -> set[str]:
    connection = _trino_connect()
    try:
        cursor = connection.cursor()
        sql = (
            f"SELECT DISTINCT CAST({column} AS varchar) "
            f"FROM iceberg.bronze.{table} "
            "WHERE league = ? AND CAST(season AS varchar) = ?"
            + (f" AND ({predicate})" if predicate else "")
        )
        cursor.execute(sql, (league, season))
        return {str(row[0]) for row in cursor.fetchall() if row and row[0] is not None}
    except Exception as exc:
        if _missing_table(exc):
            return set()
        raise RuntimeError(f"workload probe failed on bronze.{table}: {exc}") from exc
    finally:
        connection.close()


def _finished_match_ids(league: str, season: str) -> set[str]:
    return _query_partition_ids(
        "sofascore_schedule",
        "game_id",
        league=league,
        season=season,
        predicate="status_type = 'finished'",
    )


def _observed_player_ids(league: str, season: str) -> set[str]:
    """Read the match-derived Bronze universe, never an older universe snapshot.

    The production runner owns the schema-aware UNION over lineups, event
    player stats, ratings and incident actors.  Reusing it here keeps planning
    and the pre-capture race check byte-for-byte consistent while deliberately
    excluding ``sofascore_player_universe``: that table belongs to an earlier
    player phase and would make a new plan stale by construction.
    """

    with SofaScoreScraper(leagues=[league], seasons=[season]) as scraper:
        return {
            str(value)
            for value in scraper._resolve_player_ids_from_bronze(
                league,
                season,
                limit=None,
            )
        }


def _needs_network(runtime, spec) -> bool:
    existing = runtime.manifest_store.get(spec.key)
    if (existing and existing.is_terminal) or not spec.supported:
        return False
    try:
        _, raw = runtime.raw_store.load_bytes(spec.raw_target)
    except RawPayloadNotFound:
        return True
    return not (200 <= raw.http_status < 300 and raw.http_status != 204)


def _pending_targets(runtime, ids: Iterable[str], build_specs) -> tuple[str, ...]:
    pending = []
    for target_id in sorted({str(value) for value in ids}, key=int):
        if any(_needs_network(runtime, spec) for spec in build_specs(target_id)):
            pending.append(target_id)
    return tuple(pending)


def _season_freshness_key() -> str:
    return os.environ.get(
        "SOFASCORE_SEASON_FRESHNESS_KEY", ""
    ).strip() or "day-" + datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _player_freshness_key() -> str:
    return os.environ.get(
        "SOFASCORE_PLAYER_FRESHNESS_KEY", ""
    ).strip() or "week-" + datetime.now(timezone.utc).strftime("%G-W%V")


def prepare_workload_plan(
    *,
    dag_id: str,
    base_run_id: str,
    phase: str,
    competition_seasons: Sequence[CompetitionSeason],
    artifact_path: os.PathLike[str] | str,
    output_path: Optional[os.PathLike[str] | str] = None,
    raw_store_uri: Optional[str] = None,
    manifest_backend: Optional[str] = None,
    force_replace: bool = False,
    allow_inactive_season: bool = False,
    players_rotation_date: Optional[date] = None,
    players_force: bool = False,
) -> Path:
    """Snapshot local work, sign it, and atomically persist one phase plan."""

    if phase not in VALID_PHASES:
        raise ValueError(f"phase must be one of {sorted(VALID_PHASES)}")
    if not str(base_run_id).strip() or "::" in str(base_run_id):
        raise ValueError("base_run_id must be non-empty and cannot contain '::'")
    phase_run_id = f"{base_run_id}::{phase}"
    season_freshness = (
        f"repair-{base_run_id}" if force_replace else _season_freshness_key()
    )
    player_freshness = (
        f"repair-{base_run_id}" if force_replace else _player_freshness_key()
    )
    match_freshness = f"repair-{base_run_id}" if force_replace else "final"
    freshness_keys = {
        "season": season_freshness,
        "match": match_freshness,
        "player": player_freshness,
    }
    policy = load_verified_workload_policy(artifact_path)
    destination = (
        Path(output_path) if output_path else plan_path_for_run(dag_id, phase_run_id)
    )
    if destination.exists():
        existing = load_plan(destination)
        if (
            existing.dag_id != dag_id
            or existing.run_id != phase_run_id
            or existing.artifact_id != policy.artifact_id
        ):
            raise RuntimeError(
                "existing immutable workload plan has different provenance"
            )
        return destination
    runtime = build_capture_runtime(
        run_id=phase_run_id,
        task_id=f"prepare-{phase}",
        raw_store_uri=raw_store_uri,
        manifest_backend=manifest_backend,
    )
    catalog = SofaScoreCatalog.load()
    from utils.medallion_config import (
        get_active_season,
        get_season_team_count,
        is_single_year_competition,
    )

    workloads: list[PartitionWorkload] = []
    max_pages = int(os.environ.get("SOFASCORE_SEASON_MAX_PAGES", "50"))
    club_league_count = sum(
        1 for item in competition_seasons if not is_single_year_competition(item.league)
    )
    for item in competition_seasons:
        tournament = catalog.competition(item.league)
        if not tournament.capture_allowed:
            raise RuntimeError(f"{item.league} is not capture-eligible")
        canonical = _season_label(item.league, item.season)
        if not allow_inactive_season:
            if (
                is_single_year_competition(item.league)
                and get_active_season(item.league) is None
            ):
                workloads.append(
                    PartitionWorkload(
                        item.league,
                        canonical,
                        tournament.unique_tournament_id,
                    )
                )
                continue
        if phase == "players" and not player_rotation_due(
            item.league,
            rotation_date=players_rotation_date,
            club_league_count=club_league_count,
            is_tournament=is_single_year_competition(item.league),
            force=players_force,
        ):
            # A league outside this week's cohort is dropped from the signed
            # plan entirely — no Trino/squad reads, and its capture task (which
            # the DAG's rotation gate short-circuits) would fail loudly on the
            # missing partition if it ever ran anyway.
            continue
        source_season = catalog.resolve_source_season(
            tournament.unique_tournament_id, canonical
        )
        if source_season is None:
            raise RuntimeError(
                f"{item.league} {canonical} has no discovered SofaScore season"
            )
        season_plan = plan_season_partition(
            runtime.raw_store,
            runtime.manifest_store,
            source_tournament_id=tournament.unique_tournament_id,
            source_season_id=source_season.season_id,
            freshness_key=season_freshness,
            event_freshness_key="final",
            paid_proxy=True,
            max_pages=max_pages,
        )
        # The class is keyed by the season's byte-driving shape, not by the
        # tournament: format comes from the discovered registry season, the
        # team-count band from competitions.yaml.  An unconfigured season raises
        # MedallionConfigError here and fails the phase — a silently skipped
        # league is exactly the fail-open we refuse.
        shape = production_season_shape(
            season_format=source_season.format,
            team_count_band=team_count_band(
                get_season_team_count(item.league, canonical)
            ),
            max_pages_per_direction=max_pages,
        )
        if phase == "season":
            workloads.append(
                PartitionWorkload(
                    item.league,
                    canonical,
                    tournament.unique_tournament_id,
                    season_workload=SeasonWorkload(
                        tournament.unique_tournament_id,
                        source_season.season_id,
                        shape,
                        pending=bool(season_plan.missing_raw_keys) or force_replace,
                    ),
                )
            )
            continue
        if season_plan.missing_raw_keys:
            raise RuntimeError(
                f"{item.league} season raw is incomplete; {phase} cannot be planned"
            )
        matches = _finished_match_ids(item.league, canonical)
        def event_specs(target_id: str):
            return tuple(
                build_event_spec(
                    source_tournament_id=tournament.unique_tournament_id,
                    source_season_id=source_season.season_id,
                    target_id=target_id,
                    endpoint=endpoint,
                    freshness_key=match_freshness,
                    paid_proxy=True,
                )
                for endpoint in EVENT_PATHS
            )

        pending_matches = _pending_targets(runtime, matches, event_specs)
        if phase == "targets":
            # This snapshot is intentionally match-only.  Player evidence is
            # not stable until every match allocation has committed Bronze.
            workloads.append(
                PartitionWorkload(
                    item.league,
                    canonical,
                    tournament.unique_tournament_id,
                    pending_match_ids=pending_matches,
                )
            )
            continue

        if pending_matches:
            raise RuntimeError(
                f"{item.league} match raw/manifest is incomplete; "
                "players cannot be planned"
            )
        registered = set(squad_player_ids(runtime.raw_store, season_plan))
        observed = _observed_player_ids(item.league, canonical)
        universe = tuple(sorted(registered | observed, key=int))
        if not universe:
            raise RuntimeError(
                f"{item.league} player universe is empty after squads and matches"
            )

        def player_specs(target_id: str):
            return tuple(
                build_player_spec(
                    source_tournament_id=tournament.unique_tournament_id,
                    source_season_id=source_season.season_id,
                    target_id=target_id,
                    endpoint=endpoint,
                    freshness_key=player_freshness,
                    paid_proxy=True,
                )
                for endpoint in PLAYER_PATHS
            )

        workloads.append(
            PartitionWorkload(
                item.league,
                canonical,
                tournament.unique_tournament_id,
                player_universe_ids=universe,
                pending_player_ids=_pending_targets(runtime, universe, player_specs),
            )
        )
    plan = build_partitioned_plan(
        policy,
        dag_id=dag_id,
        run_id=phase_run_id,
        freshness_keys=freshness_keys,
        partitions=workloads,
    )
    return write_plan(destination, plan)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", default="dag_ingest_sofascore")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--phase", required=True, choices=sorted(VALID_PHASES))
    parser.add_argument(
        "--competition-season",
        action="append",
        required=True,
        help="LEAGUE=CANONICAL_SEASON; repeat for every enabled competition",
    )
    parser.add_argument(
        "--artifact",
        default=os.environ.get("SOFASCORE_PROXY_BUDGET_ARTIFACT", ""),
    )
    parser.add_argument("--output")
    parser.add_argument("--raw-store-uri")
    parser.add_argument("--manifest-backend")
    parser.add_argument("--force-replace", action="store_true")
    parser.add_argument("--allow-inactive-season", action="store_true")
    parser.add_argument(
        "--players-rotation-date",
        help=(
            "Run boundary as YYYY-MM-DD or a full ISO datetime (the master "
            "data-interval end the rotation gate uses); its week selects the "
            "club cohort of the players phase. Omitted = no rotation (every "
            "league is due)."
        ),
    )
    parser.add_argument(
        "--players-force",
        action="store_true",
        help="Plan every league's players regardless of the weekly rotation.",
    )
    args = parser.parse_args(argv)
    if not args.artifact:
        parser.error("--artifact or SOFASCORE_PROXY_BUDGET_ARTIFACT is required")
    rotation_date = None
    if args.players_rotation_date:
        rotation_date = _parse_rotation_boundary(args.players_rotation_date)
        if rotation_date is None:
            parser.error(
                "--players-rotation-date must be YYYY-MM-DD or an ISO datetime"
            )
    pairs = []
    for token in args.competition_season:
        if "=" not in token:
            parser.error("--competition-season must be LEAGUE=SEASON")
        league, season = token.rsplit("=", 1)
        pairs.append(CompetitionSeason(league.strip(), season.strip()))
    path = prepare_workload_plan(
        dag_id=args.dag_id,
        base_run_id=args.run_id,
        phase=args.phase,
        competition_seasons=pairs,
        artifact_path=args.artifact,
        output_path=args.output,
        raw_store_uri=args.raw_store_uri,
        manifest_backend=args.manifest_backend,
        force_replace=args.force_replace,
        allow_inactive_season=args.allow_inactive_season,
        players_rotation_date=rotation_date,
        players_force=args.players_force,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
