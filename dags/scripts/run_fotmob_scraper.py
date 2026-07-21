#!/usr/bin/env python3
"""
FotMob Scraper Runner Script
============================

Standalone script to run the FotMob scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Pure HTTP — FotMob's public ``/api/data`` endpoints require no browser, no
Cloudflare bypass and no cookies.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a bronze.fotmob_* (league, season) partition below this share of its
# existing rows, so a partial/failed scrape can't wipe a good partition.
# COUNT(*) (no replace_guard_key) — each (league, season) is scraped full-state.
# ReplaceGuardError → exit 3; bypass with --force-replace.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = "FOTMOB_REPLACE_GUARD"


# (results_key, iceberg_table, read_callable) for every Bronze entity.
# read_callable signature: (scraper, league, season) -> Optional[DataFrame]
ENTITIES = [
    ("schedule", "fotmob_schedule", lambda s, lg, se: s.read_schedule(lg, se)),
    (
        "team_stats",
        "fotmob_team_stats",
        lambda s, lg, se: s.read_team_season_stats(lg, se),
    ),
    (
        "player_stats",
        "fotmob_player_stats",
        lambda s, lg, se: s.read_player_season_stats("goals", lg, se),
    ),
    (
        "team_profile",
        "fotmob_team_profile",
        lambda s, lg, se: s.read_team_profile(lg, se),
    ),
    ("team_squad", "fotmob_team_squad", lambda s, lg, se: s.read_team_squad(lg, se)),
    (
        "team_leaderboards",
        "fotmob_team_leaderboards",
        lambda s, lg, se: s.read_team_leaderboards(lg, se),
    ),
    ("transfers", "fotmob_transfers", lambda s, lg, se: s.read_transfers(lg, se)),
    (
        "match_details",
        "fotmob_match_details",
        lambda s, lg, se: s.read_match_details(lg, se),
    ),
    (
        "player_details",
        "fotmob_player_details",
        lambda s, lg, se: s.read_player_details(lg, se),
    ),
]


NATIVE_MODES = ("discover", "daily", "backfill", "replay")
NATIVE_ENTITIES = frozenset(
    {"season", "leaderboards", "matches", "teams", "players", "transfers"}
)
DEFAULT_NATIVE_ENTITIES = frozenset({"season", "leaderboards"})
NATIVE_PRIMARY_COUNTS = {
    "competition_catalog": "competitions",
    "competition_seasons": "seasons",
    "season_bundle": "matches",
    "leaderboards": "rows",
    "transfer_events": "events",
    "match_payloads": "rows",
    "team_snapshots": "teams",
    "player_snapshots": "players",
    "scope_completion": "scopes",
    "competition_completion": "competitions",
    "current_views": "views",
}


def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    """Publish a report atomically so validators never observe partial JSON."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _safe_run_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "run"


def _parse_scopes(values: Iterable[str]) -> tuple[tuple[int, str], ...]:
    """Parse repeatable/comma-separated exact ``competition_id=season`` scopes."""

    scopes: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    for raw_group in values:
        for raw_item in str(raw_group or "").split(","):
            item = raw_item.strip()
            if not item:
                continue
            competition, separator, season = item.partition("=")
            if not separator or not competition.isascii() or not competition.isdigit():
                raise ValueError(
                    f"invalid --scope {item!r}; expected numeric ID=exact-season"
                )
            if not season or season != season.strip():
                raise ValueError(
                    f"invalid --scope {item!r}; season must be a non-empty exact source key"
                )
            identity = (int(competition), season)
            if identity not in seen:
                scopes.append(identity)
                seen.add(identity)
    return tuple(scopes)


def _parse_native_entities(value: str) -> frozenset[str]:
    selected = {
        item.strip().lower() for item in str(value or "").split(",") if item.strip()
    }
    if not selected:
        return DEFAULT_NATIVE_ENTITIES
    unknown = sorted(selected - NATIVE_ENTITIES)
    if unknown:
        raise ValueError(
            f"unknown native entities: {', '.join(unknown)}; expected one of "
            f"{', '.join(sorted(NATIVE_ENTITIES))}"
        )
    if "players" in selected:
        # Player identities come from observed team squads. Fetching players
        # without teams would silently turn the requested entity into a no-op.
        selected.add("teams")
    return frozenset(selected)


def _outstanding_targets(operation) -> int:
    """Return requested targets without an explicit terminal disposition."""

    intentional_not_available = min(
        int(operation.not_available),
        int(operation.metadata.get("intentional_not_available") or 0),
    )
    resolved = (
        int(operation.succeeded) + int(operation.skipped) + intentional_not_available
    )
    return max(0, int(operation.attempted) - resolved)


def _identity_hash(values: Iterable[Any]) -> str:
    material = "\0".join(sorted(str(value) for value in values)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _native_output_payload(report) -> dict[str, Any]:
    payload = report.as_dict()
    tables: list[str] = []
    rows: dict[str, int] = {}
    errors: list[str] = []
    for operation in report.operations:
        tables.extend(operation.tables)
        primary_key = NATIVE_PRIMARY_COUNTS.get(operation.entity)
        primary_count = (
            operation.counts.get(primary_key, operation.succeeded)
            if primary_key
            else operation.succeeded
        )
        rows[operation.entity] = rows.get(operation.entity, 0) + int(primary_count)
        errors.extend(operation.errors)
        errors.extend(f"retryable: {item}" for item in operation.retryable)
        errors.extend(f"terminal: {item}" for item in operation.terminal)
    payload.update(
        {
            # Additive compatibility for existing validators/report readers.
            "tables": list(dict.fromkeys(tables)),
            "rows": rows,
            "errors": errors,
            "complete": report.status == "success",
        }
    )
    return payload


# Buffered repository of the running native service. finish() is the normal
# flush point, but an exception escaping _run_native (or the driver's SIGTERM
# at unit timeout) would otherwise drop up to batch_size-1 already-paid-for
# targets; main() salvage-flushes through this handle before reporting failure.
_ACTIVE_NATIVE_SERVICE = None


def _salvage_flush() -> None:
    """Best-effort durability for buffered commits on an abnormal exit."""

    service = _ACTIVE_NATIVE_SERVICE
    if service is None:
        return
    try:
        tables = service.repository.flush()
        if tables:
            logger.warning("salvage flush persisted buffered commits: %s", tables)
    except Exception:
        logger.exception("salvage flush after runner failure also failed")


def _build_native_service(args, run_id: str):
    """Construct production dependencies lazily; legacy unit tests stay isolated."""

    from scrapers.fotmob.planner import RunMode, TransportBudget
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.repository import FotMobRepository
    from scrapers.fotmob.service import FotMobIngestService
    from scrapers.fotmob.transport import FotMobTransport
    from scrapers.utils.rate_limiter import RateLimiter

    raw_store = (
        FotMobRawStore.from_uri(args.raw_store_uri)
        if args.raw_store_uri
        else FotMobRawStore.from_env(optional=False)
    )
    limiter = RateLimiter(
        max_requests=args.requests_per_minute,
        window_seconds=60,
        # Avoid an initial 30-request burst when several workers start.
        burst_size=max(1, args.workers),
    )
    transport = FotMobTransport(
        raw_store=raw_store,
        max_attempts=args.max_attempts,
        rate_limiter=limiter,
    )
    repository = FotMobRepository(
        batch_size=args.commit_batch_size,
        max_buffered_rows=args.max_buffered_rows,
    )
    budget = TransportBudget(
        max_requests=args.max_requests,
        max_direct_bytes=int(args.max_direct_mib * 1024 * 1024),
        max_proxy_bytes=int(args.max_proxy_mib * 1024 * 1024),
    )
    service = FotMobIngestService(
        transport=transport,
        repository=repository,
        mode=RunMode(args.mode),
        budget=budget,
        run_id=run_id,
        max_workers=args.workers,
    )
    global _ACTIVE_NATIVE_SERVICE
    _ACTIVE_NATIVE_SERVICE = service
    return service, raw_store


def _run_native(args, *, service=None, raw_store=None) -> tuple[int, dict[str, Any]]:
    """Run source-native discovery and a deterministic, budgeted work plan."""

    from scrapers.fotmob.planner import (
        MANDATORY_COMPETITION_IDS,
        RunMode,
        deterministic_plan_signature,
        plan_seasons,
    )
    from scrapers.fotmob.service import OperationResult
    from scrapers.fotmob.transport import canonicalize_target

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = args.run_id or (f"fotmob-{datetime.now(timezone.utc):%Y%m%dT%H%M%S.%fZ}")
    if service is None:
        service, raw_store = _build_native_service(args, run_id)
    operations = []

    def finish() -> tuple[int, dict[str, Any]]:
        # Buffered commits are only durable once flushed. Every exit path of
        # this run — completion, budget cut, empty catalog — goes through
        # finish(), so the flush belongs here and its failure must turn the
        # run red instead of silently dropping targets.
        flush_operation = OperationResult("commit_flush", attempted=1)
        try:
            flush_operation.tables.extend(service.repository.flush())
            flush_operation.succeeded = 1
        except Exception as exc:
            flush_operation.errors.append(
                f"commit flush: {type(exc).__name__}: {exc}"
            )
        operations.append(flush_operation)

        view_operation = OperationResult("current_views", attempted=1)
        try:
            views = service.repository.ensure_current_views()
            view_operation.succeeded = 1
            view_operation.tables.extend(views)
            view_operation.counts["views"] = len(views)
        except Exception as exc:
            view_operation.errors.append(
                f"current view refresh: {type(exc).__name__}: {exc}"
            )
        operations.append(view_operation)
        report = service.report(operations, started_at)
        return (0 if report.ok else 1), _native_output_payload(report)

    explicit_scopes = _parse_scopes(args.scope)
    explicit_ids = {competition_id for competition_id, _ in explicit_scopes}
    entities = _parse_native_entities(args.entities)

    catalog = service.discover_catalog()
    operations.append(catalog.operation)
    if catalog.discovery is None:
        _, payload = finish()
        return 1, payload

    classifications = list(catalog.classifications)
    catalog_ids = {item.competition.competition_id for item in classifications}
    scope_validation = OperationResult("scope_validation")
    unknown_ids = sorted(explicit_ids - catalog_ids)
    if unknown_ids:
        scope_validation.errors.append(
            "requested competition IDs are absent from allLeagues: "
            + ",".join(map(str, unknown_ids))
        )
    by_id = {item.competition.competition_id: item for item in classifications}
    for competition_id in sorted(explicit_ids & catalog_ids):
        classification = by_id[competition_id]
        if classification.decision.value != "included":
            scope_validation.errors.append(
                f"requested competition {competition_id} is "
                f"{classification.decision.value}: {classification.reason}"
            )
    if scope_validation.errors:
        operations.append(scope_validation)

    candidates = [
        item
        for item in classifications
        if (
            (not explicit_ids or item.competition.competition_id in explicit_ids)
            and item.decision.value == "included"
        )
    ]
    candidates.sort(
        key=lambda item: (
            item.competition.competition_id not in MANDATORY_COMPETITION_IDS,
            item.competition.competition_id,
        )
    )
    discovery_plan = OperationResult("competition_discovery_plan")
    if args.competition_limit:
        deferred = candidates[args.competition_limit :]
        candidates = candidates[: args.competition_limit]
        discovery_plan.skipped = len(deferred)
        discovery_plan.metadata["deferred_competition_ids"] = [
            item.competition.competition_id for item in deferred
        ]
    discovery_plan.counts["planned_competitions"] = len(candidates)
    operations.append(discovery_plan)

    seasons = []
    selected_fetches = {}
    max_attempts = max(1, int(getattr(service.transport, "max_attempts", 1)))
    discovery_capacity = service.ledger.remaining_requests // max_attempts
    budget_deferred = candidates[discovery_capacity:]
    candidates = candidates[:discovery_capacity]
    if budget_deferred:
        discovery_plan.skipped += len(budget_deferred)
        discovery_plan.retryable.append(
            f"request budget deferred {len(budget_deferred)} competitions"
        )
        discovery_plan.metadata["budget_deferred_competition_ids"] = [
            item.competition.competition_id for item in budget_deferred
        ]
    for discovered in service.discover_competitions(candidates):
        operations.append(discovered.operation)
        seasons.extend(discovered.seasons)
        if discovered.selected_bundle is not None and discovered.fetch is not None:
            selected_fetches[discovered.selected_bundle.scope.identity[:2]] = (
                discovered.fetch
            )

    if args.mode == RunMode.DISCOVER.value:
        return finish()

    discovered_identities = {season.identity for season in seasons}
    missing_scopes = sorted(set(explicit_scopes) - discovered_identities)
    if missing_scopes:
        scope_validation.errors.append(
            "requested exact scopes were not advertised by FotMob: "
            + ",".join(f"{comp}={season}" for comp, season in missing_scopes)
        )
        if scope_validation not in operations:
            operations.append(scope_validation)

    mode = RunMode(args.mode)
    scope_entities = frozenset({"season", *(entities - {"transfers"})})
    scope_plan_signature = deterministic_plan_signature(
        scope_entities,
        policy={
            "match_policy": "finished_only",
            "leaderboard_policy": "all_advertised",
            "team_policy": "global_observed_snapshot",
            "player_policy": "global_observed_snapshot",
        },
    )
    previously_complete: set[tuple[int, str]] = set()
    if mode == RunMode.BACKFILL:
        previously_complete = service.repository.completed_scope_keys(
            scope_plan_signature
        )
    elif mode == RunMode.REPLAY:
        for season in seasons:
            target = canonicalize_target(
                "leagues",
                {
                    "id": season.competition_id,
                    "season": season.source_season_key,
                },
            )
            if raw_store is not None and raw_store.has_target(target):
                previously_complete.add(season.identity)

    work = plan_seasons(
        classifications,
        seasons,
        mode=mode,
        previously_successful=previously_complete,
        explicit_scopes=(explicit_scopes or None),
    )
    daily_scope_times = {}
    if mode == RunMode.DAILY:
        daily_scope_times = service.repository.scope_completion_times(
            scope_plan_signature
        )
        work.sort(
            key=lambda work_item: (
                daily_scope_times.get(work_item.identity, datetime.min),
                work_item.priority,
                work_item.competition_id,
            )
        )
    work_plan = OperationResult(
        "season_work_plan",
        attempted=len(work),
        metadata={
            "scope_plan_signature": scope_plan_signature,
            "scope_entities": sorted(scope_entities),
            "already_complete_scopes": len(previously_complete),
            "daily_completion_timestamps": len(daily_scope_times),
        },
    )
    if not work and mode in {RunMode.DAILY, RunMode.REPLAY}:
        work_plan.errors.append(
            f"{mode.value} discovered no eligible exact season targets"
        )
    if args.season_limit:
        deferred = work[args.season_limit :]
        work = work[: args.season_limit]
        work_plan.skipped += len(deferred)
        work_plan.metadata["limit_deferred_scopes"] = [
            f"{item.competition_id}={item.source_season_key}" for item in deferred
        ]
    work_plan.counts["planned_scopes"] = len(work)
    operations.append(work_plan)

    # Complete one exact scope end-to-end before starting the next.  This is
    # the fairness boundary that prevents a season-first pass from consuming
    # the whole request budget and permanently starving child entities.
    for work_index, item in enumerate(work):
        scope_key = f"{item.competition_id}={item.source_season_key}"
        scope_operations = []
        operation, bundle = service.sync_season(
            item.competition_id,
            item.source_season_key,
            prefetched=selected_fetches.get(item.identity),
        )
        operations.append(operation)
        scope_operations.append(operation)
        if bundle is None:
            work_plan.retryable.append(f"scope {scope_key} has no season bundle")
        else:
            if "leaderboards" in entities:
                leaderboard_operation = service.sync_leaderboards(bundle)
                operations.append(leaderboard_operation)
                scope_operations.append(leaderboard_operation)

            if "matches" in entities:
                capacity = service.ledger.remaining_requests // max_attempts
                per_run_limit = args.match_limit or len(bundle.matches)
                match_operation = service.sync_match_payloads(
                    bundle,
                    limit=min(per_run_limit, capacity),
                )
                operations.append(match_operation)
                scope_operations.append(match_operation)

            player_ids: set[int] = set()
            if "teams" in entities:
                capacity = service.ledger.remaining_requests // max_attempts
                per_run_limit = args.team_limit or len(bundle.teams)
                team_operation, player_ids = service.sync_team_snapshots(
                    bundle,
                    limit=min(per_run_limit, capacity),
                )
                operations.append(team_operation)
                scope_operations.append(team_operation)

            if "players" in entities:
                capacity = service.ledger.remaining_requests // max_attempts
                build_reserve = 0 if args.next_build_id else max_attempts
                per_run_limit = args.player_limit or len(player_ids)
                player_operation = service.sync_player_snapshots(
                    player_ids,
                    build_id=(args.next_build_id or None),
                    limit=min(
                        per_run_limit,
                        max(0, capacity - build_reserve),
                    ),
                )
                operations.append(player_operation)
                scope_operations.append(player_operation)

        outstanding = {
            item.entity: _outstanding_targets(item)
            for item in scope_operations
            if _outstanding_targets(item)
        }
        scope_ok = (
            bool(bundle is not None)
            and all(item.ok for item in scope_operations)
            and not outstanding
        )
        if scope_ok and bundle is not None:
            descriptors = (
                (
                    *bundle.player_categories,
                    *bundle.team_categories,
                )
                if "leaderboards" in entities
                else ()
            )
            expected_matches = (
                [
                    match.get("match_id")
                    for match in bundle.matches
                    if match.get("finished")
                ]
                if "matches" in entities
                else []
            )
            expected_teams = (
                [team.get("team_id") for team in bundle.teams]
                if "teams" in entities
                else []
            )
            coverage = {
                "scope_entities": sorted(scope_entities),
                "leaderboard_identity_hash": _identity_hash(
                    (
                        descriptor.participant_type,
                        descriptor.name,
                        descriptor.fetch_all_url,
                        descriptor.source_order,
                    )
                    for descriptor in descriptors
                ),
                "match_identity_hash": _identity_hash(expected_matches),
                "team_identity_hash": _identity_hash(expected_teams),
                "player_identity_hash": _identity_hash(
                    player_ids if "players" in entities else ()
                ),
            }
            counts = {
                "leaderboards": len(descriptors),
                "matches": len(expected_matches),
                "teams": len(expected_teams),
                "players": len(player_ids) if "players" in entities else 0,
            }
            completion = OperationResult(
                "scope_completion",
                attempted=1,
                metadata={"scope": scope_key, **coverage},
            )
            try:
                completion.tables.extend(
                    service.record_scope_completion(
                        item.competition_id,
                        item.source_season_key,
                        plan_signature=scope_plan_signature,
                        coverage=coverage,
                        counts=counts,
                    )
                )
                completion.succeeded = 1
                completion.counts["scopes"] = 1
                work_plan.succeeded += 1
            except Exception as exc:
                completion.errors.append(
                    f"scope {scope_key}: {type(exc).__name__}: {exc}"
                )
            operations.append(completion)
        else:
            work_plan.retryable.append(
                f"scope {scope_key} incomplete; outstanding={outstanding}"
            )
            work_plan.metadata.setdefault("incomplete_scopes", []).append(
                {"scope": scope_key, "outstanding": outstanding}
            )

        if (
            not scope_ok
            and service.ledger.remaining_requests < max_attempts
            and work_index + 1 < len(work)
        ):
            deferred = work[work_index + 1 :]
            work_plan.skipped += len(deferred)
            work_plan.metadata["budget_deferred_scopes"] = [
                f"{deferred_item.competition_id}={deferred_item.source_season_key}"
                for deferred_item in deferred
            ]
            work_plan.retryable.append(
                f"request budget deferred {len(deferred)} remaining scopes"
            )
            break

    if "transfers" in entities:
        transfer_window = "1year" if mode == RunMode.DAILY else "all"
        transfer_signature = deterministic_plan_signature(
            {"transfers"},
            policy={"window": transfer_window, "pagination": "unique_hits"},
        )
        completed_transfer_ids = set()
        transfer_completion_times = {}
        if mode == RunMode.BACKFILL:
            completed_transfer_ids = service.repository.completed_competition_ids(
                transfer_signature
            )
        elif mode == RunMode.DAILY:
            transfer_completion_times = service.repository.competition_completion_times(
                transfer_signature
            )
        competition_ids = [
            item.competition.competition_id
            for item in candidates
            if item.competition.competition_id not in completed_transfer_ids
        ]
        if mode == RunMode.DAILY:
            competition_ids.sort(
                key=lambda competition_id: (
                    transfer_completion_times.get(competition_id, datetime.min),
                    competition_id,
                )
            )
        transfer_plan = OperationResult(
            "transfer_work_plan",
            attempted=len(competition_ids),
            metadata={
                "plan_signature": transfer_signature,
                "window": transfer_window,
                "already_complete_competitions": len(completed_transfer_ids),
                "daily_completion_timestamps": len(transfer_completion_times),
            },
        )
        operations.append(transfer_plan)
        for index, competition_id in enumerate(competition_ids):
            capacity = service.ledger.remaining_requests // max_attempts
            max_pages = min(args.transfer_max_pages, capacity)
            if not max_pages:
                deferred = competition_ids[index:]
                transfer_plan.skipped += len(deferred)
                transfer_plan.retryable.append(
                    f"request budget deferred {len(deferred)} transfer streams"
                )
                transfer_plan.metadata["budget_deferred_competition_ids"] = deferred
                break
            transfer_operation = service.sync_transfers(
                competition_id,
                max_pages=max_pages,
                recent_only=(mode == RunMode.DAILY),
            )
            operations.append(transfer_operation)
            expected_hits = transfer_operation.metadata.get("source_hits")
            observed_events = int(transfer_operation.counts.get("events") or 0)
            complete_stream = (
                transfer_operation.ok
                and expected_hits is not None
                and observed_events >= int(expected_hits)
            )
            if complete_stream:
                completion = OperationResult(
                    "competition_completion",
                    attempted=1,
                    metadata={
                        "competition_id": competition_id,
                        "window": transfer_window,
                    },
                )
                try:
                    completion.tables.extend(
                        service.record_competition_completion(
                            competition_id,
                            plan_signature=transfer_signature,
                            coverage={
                                "window": transfer_window,
                                "source_hits": int(expected_hits),
                                "observed_events": observed_events,
                            },
                            counts={"events": observed_events},
                        )
                    )
                    completion.succeeded = 1
                    completion.counts["competitions"] = 1
                    transfer_plan.succeeded += 1
                except Exception as exc:
                    completion.errors.append(
                        f"competition {competition_id}: {type(exc).__name__}: {exc}"
                    )
                operations.append(completion)
            else:
                transfer_plan.retryable.append(
                    f"competition {competition_id} transfer stream incomplete"
                )

    rc, payload = finish()
    payload["selection"] = {
        "entities": sorted(entities),
        "explicit_scopes": [
            f"{competition_id}={season}" for competition_id, season in explicit_scopes
        ],
        "competition_limit": args.competition_limit,
        "season_limit": args.season_limit,
        "scope_plan_signature": scope_plan_signature,
    }
    return rc, payload


def _run_legacy(args) -> tuple[int, dict[str, Any]]:
    leagues = [league.strip() for league in args.leagues.split(",")]
    selected = {e.strip() for e in args.entities.split(",") if e.strip()}
    entities = [e for e in ENTITIES if not selected or e[0] in selected]

    # #920 bridge (generalized Phase 3: any single_year tournament):
    # tournaments must never inherit the club-formula season (July 2026 ->
    # 2025): FotMob answers a season='2025' WC request with CURRENT
    # tournament data, silently mislabelling the partition. Mixed calls
    # can't carry two seasons -> tournaments are dropped (dedicated call),
    # mirroring run_whoscored_scraper / run_fbref_scraper.
    from utils.medallion_config import (
        get_active_season,
        is_single_year_competition,
    )

    _tournaments = [league for league in leagues if is_single_year_competition(league)]
    if _tournaments and len(leagues) > 1:
        logger.warning(
            f"Single-year tournaments {_tournaments} dropped from mixed call "
            f"(each needs its own season; leagues={leagues}). Scrape them "
            f"with dedicated --leagues calls."
        )
        leagues = [league for league in leagues if league not in _tournaments]
        if not leagues:
            logger.warning("No leagues left after dropping tournaments; exiting 0.")
            payload = {
                "tables": [],
                "rows": {},
                "errors": [],
                "skipped": "mixed_tournaments_dropped",
            }
            return 0, payload
    elif _tournaments:
        _t_league = leagues[0]
        _t_season = get_active_season(_t_league)
        if _t_season is None:
            logger.warning(
                f"{_t_league} is out of its tournament window — nothing to "
                "scrape; exiting 0."
            )
            payload = {
                "tables": [],
                "rows": {},
                "errors": [],
                "skipped": "out_of_window",
            }
            return 0, payload
        elif int(args.season) != int(_t_season):
            logger.info(
                f"{_t_league}: overriding --season {args.season} -> "
                f"{_t_season} (active single_year season, #920 bridge)."
            )
            args.season = _t_season

    logger.info(f"Starting FotMob scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Entities: {[e[0] for e in entities]}")

    results = {
        "tables": [],
        "rows": {},
        "errors": [],
    }
    guard_refused = False

    try:
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.fotmob import FotMobScraper

        with FotMobScraper(
            leagues=leagues, seasons=[args.season], full_players=args.full_players
        ) as scraper:
            for key, table_name, read_fn in entities:
                row_count = 0
                for league in leagues:
                    try:
                        df = read_fn(scraper, league, args.season)
                        if df is not None and not df.empty:
                            table_path = scraper.save_to_iceberg(
                                df=df,
                                table_name=table_name,
                                partition_cols=["league", "season"],
                                replace_partitions=["league", "season"],
                                min_replace_ratio=(
                                    None if args.force_replace else _MIN_REPLACE_RATIO
                                ),
                            )
                            results["tables"].append(table_path)
                            row_count += len(df)
                            logger.info(f"Saved {len(df)} {key} rows for {league}")
                    except ReplaceGuardError as e:
                        # Guard refused this (entity, league) save — a partial
                        # scrape would shrink the partition. Record + continue
                        # other entities/leagues; exit 3 at the end (#583).
                        msg = f"{REPLACE_GUARD_MARKER}: {key}/{league}: {e}"
                        logger.error(msg)
                        results["errors"].append(msg)
                        guard_refused = True
                    except Exception as e:
                        error_msg = f"{key} scraping for {league} failed: {e}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
                results["rows"][key] = row_count
                # Legacy flat keys (kept for backward-compatible consumers)
                results[f"{key}_rows"] = row_count

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results["errors"].append(str(e))
        return 1, results

    total_rows = sum(results["rows"].values())
    logger.info(
        f"Scraper complete: {total_rows} total rows across "
        f"{len(results['rows'])} entities"
    )
    print(json.dumps(results))
    # Exit 3 when the completeness guard refused any save (distinct from the
    # exit-0 path) so an operator can spot a refused guard in the BashOperator.
    return (3 if guard_refused else 0), results


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run FotMob scraper")
    parser.add_argument(
        "--mode",
        choices=NATIVE_MODES,
        help="Enable source-native discovery/ingestion. Omit for legacy mode.",
    )
    parser.add_argument("--run-id", default="", help="Stable external run id")
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        metavar="ID=SEASON",
        help="Exact native scope; repeat or comma-separate (e.g. 47=2025/2026)",
    )
    parser.add_argument(
        "--raw-store-uri",
        default="",
        help="Required in native mode; defaults to FOTMOB_RAW_STORE_URI",
    )
    parser.add_argument("--max-requests", type=int, default=2000)
    parser.add_argument("--max-direct-mib", type=float, default=256.0)
    parser.add_argument("--max-proxy-mib", type=float, default=0.0)
    parser.add_argument("--requests-per-minute", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--commit-batch-size",
        type=int,
        default=50,
        help=(
            "Targets buffered into one Iceberg commit per table. 1 commits "
            "every target separately (one single-row data file per target)."
        ),
    )
    parser.add_argument(
        "--max-buffered-rows",
        type=int,
        default=100_000,
        help=(
            "Physical rows buffered before an early flush. The 20k default "
            "of the repository flushed every ~4 matches once field-inventory "
            "rows piled up, defeating --commit-batch-size. Only effective "
            "with --commit-batch-size > 1 (batch size 1 writes unbuffered)."
        ),
    )
    parser.add_argument("--competition-limit", type=int, default=0)
    parser.add_argument("--season-limit", type=int, default=0)
    parser.add_argument("--match-limit", type=int, default=0)
    parser.add_argument("--team-limit", type=int, default=0)
    parser.add_argument("--player-limit", type=int, default=0)
    parser.add_argument(
        "--next-build-id",
        default="",
        help="Optional exact FotMob Next build id (required for offline player replay)",
    )
    parser.add_argument("--transfer-max-pages", type=int, default=250)
    parser.add_argument(
        "--leagues",
        type=str,
        default="ENG-Premier League",
        help="Legacy mode: comma-separated list of leagues",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=2025,
        help="Legacy mode: season start year",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Atomic report path; default is run-specific under /tmp",
    )
    parser.add_argument(
        "--entities",
        type=str,
        default="",
        help="Comma-separated entity subset (mode-specific)",
    )
    parser.add_argument(
        "--force-replace",
        action="store_true",
        help="Legacy mode: bypass the replace completeness guard",
    )
    parser.add_argument(
        "--full-players",
        action="store_true",
        help="Legacy mode: refresh all player details",
    )
    return parser


def _validate_args(parser: argparse.ArgumentParser, args) -> None:
    positive = {
        "--max-requests": args.max_requests,
        "--max-direct-mib": args.max_direct_mib,
        "--requests-per-minute": args.requests_per_minute,
        "--max-attempts": args.max_attempts,
        "--workers": args.workers,
        "--transfer-max-pages": args.transfer_max_pages,
        "--max-buffered-rows": args.max_buffered_rows,
    }
    for name, value in positive.items():
        if value <= 0:
            parser.error(f"{name} must be positive")
    for name, value in {
        "--competition-limit": args.competition_limit,
        "--season-limit": args.season_limit,
        "--match-limit": args.match_limit,
        "--team-limit": args.team_limit,
        "--player-limit": args.player_limit,
        "--max-proxy-mib": args.max_proxy_mib,
    }.items():
        if value < 0:
            parser.error(f"{name} must be non-negative")
    if args.max_proxy_mib != 0:
        parser.error("--max-proxy-mib must remain 0 for the direct-only pipeline")
    if args.next_build_id and not re.fullmatch(r"[A-Za-z0-9_-]+", args.next_build_id):
        parser.error("--next-build-id contains unsupported characters")
    if args.workers > 16:
        parser.error("--workers must be <= 16")


def _sigterm_to_exception(signum, frame):
    """The driver's unit timeout sends TERM (then KILL after 30s). Raising here
    routes shutdown through main()'s failure path: salvage flush + a real
    report instead of a silent NO_REPORT kill."""

    raise RuntimeError(f"terminated by signal {signum}")


def main():
    parser = _argument_parser()
    args = parser.parse_args()
    _validate_args(parser, args)
    run_id = args.run_id or (f"fotmob-{datetime.now(timezone.utc):%Y%m%dT%H%M%S.%fZ}")
    output = args.output or f"/tmp/fotmob_result_{_safe_run_id(run_id)}.json"
    args.run_id = run_id
    try:
        signal.signal(signal.SIGTERM, _sigterm_to_exception)
    except ValueError:
        pass  # not in the main thread (unit-test harness) — keep default
    try:
        if args.mode:
            rc, payload = _run_native(args)
        else:
            rc, payload = _run_legacy(args)
    except (ValueError, RuntimeError) as exc:
        logger.error("FotMob runner configuration/runtime failure: %s", exc)
        _salvage_flush()
        payload = {
            "run_id": run_id,
            "mode": args.mode or "legacy",
            "status": "incomplete",
            "complete": False,
            "tables": [],
            "rows": {},
            "errors": [f"{type(exc).__name__}: {exc}"],
        }
        rc = 1
    except Exception as exc:
        logger.exception("Unexpected FotMob runner failure")
        _salvage_flush()
        payload = {
            "run_id": run_id,
            "mode": args.mode or "legacy",
            "status": "incomplete",
            "complete": False,
            "tables": [],
            "rows": {},
            "errors": [f"{type(exc).__name__}: {exc}"],
        }
        rc = 1
    _write_json_atomic(output, payload)
    logger.info("FotMob report: %s", output)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return rc


if __name__ == "__main__":
    sys.exit(main())
