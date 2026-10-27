"""One bounded journal-led history chunk, using the normal service contracts."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from . import history_state as state
from .parsers import PARSER_VERSION
from .repository import STAGE_XG_FEED_POLICY


def inventory(catalog, repository, *, today):
    coverage = repository.history_coverage()
    scopes = state.queue(catalog, today=today)
    for item in scopes:
        season = item["season"]
        row = coverage.get(
            (season.scope.competition_id, season.scope.season_id),
            dict(schedule_ready=False, closeable=0, closed=0, unknown_stages=0, closed_day=0),
        ).copy()
        missing = set(season.stage_ids) - set(row.get("stage_ids", ()))
        row["unknown_stages"] += len(missing) or int(not season.stage_ids)
        if missing:
            row["schedule_ready"] = False
        item["coverage"] = row
    return scopes


def final_feeds(repository, season, *, moment) -> bool:
    snapshot = repository.latest_stage_feed_snapshot(
        season.scope.competition_id, season.scope.season_id,
    )
    if (not snapshot or snapshot.get("parser_version") != PARSER_VERSION
            or snapshot.get("feed_policy") != STAGE_XG_FEED_POLICY or not season.stage_ids):
        return False
    checks = snapshot.get("feed_checks", {})
    for sid in season.stage_ids:
        check = checks.get(str(sid), {})
        try:
            checked = datetime.fromisoformat(check.get("checked_at", ""))
        except (TypeError, ValueError):
            return False
        if (check.get("final") is not True or checked.tzinfo is None or checked > moment
                or not all(f"{sid}:{suffix}" in snapshot.get("feed_states", {}) for suffix in
                           ("team:xg-teamstats:summary", "player:xg-stats:summary"))):
            return False
    return True


def run_chunk(*, catalog, repository, checkpoint, service_cls, enabled,
              current_running, consume, collect_traffic=None, now=None):
    """Scan deferred/closed scopes without source calls; run one due scope.

    Leases remember scheduling only. Neither an empty due query nor a green
    result can mark a scope complete. Every new chunk reselects from Bronze.
    """
    if not enabled():
        return {"reason": "disabled", "attempted": 0}
    if current_running():
        return {"reason": "current_running", "attempted": 0}
    moment = now or datetime.now(timezone.utc)
    scopes = inventory(catalog, repository, today=moment.date())
    gate = state.first_wave_ready(scopes)
    second = [s["coverage"] for s in scopes if s["wave"] == 2]
    second_ready = all(c["schedule_ready"] and not c["unknown_stages"]
                       and c["closed"] == c["closeable"] for c in second)
    eligible = [s for s in scopes if s["wave"] == 1 or
                (gate and (s["wave"] == 2 or second_ready))]
    while eligible:
        reservation = checkpoint.reserve(eligible, now=moment.timestamp())
        if reservation is None:
            break
        item, token = reservation
        eligible = [s for s in eligible if s["key"] != item["key"]]
        season = item["season"]
        league, slug = season.scope.competition_id, season.scope.season_id
        started = time.monotonic()
        failed, did_work, attempted = True, False, 0
        try:
            if current_running() or not enabled():
                failed = False
                return {"reason": "current_running_or_disabled", "attempted": 0}
            schedule_due = not item["coverage"]["schedule_ready"]
            matches, players, previews, feeds_due = [], [], [], False
            if not schedule_due:
                matches = repository.list_match_candidates(league, slug, limit=1, lane="history")
                players = repository.list_profile_candidates(
                    scopes=[season.scope], limit=state.CHUNK_SIZE, once=True,
                )
                if item["wave"] == 1:
                    previews = repository.list_preview_candidates(
                        league, slug, limit=state.CHUNK_SIZE, historical=True,
                    )
                ended = (season.end is not None and season.end < moment.date()) or season.is_active is False
                feeds_due = ended and not final_feeds(repository, season, moment=moment)
            if not (schedule_due or matches or players or previews or feeds_due):
                failed = False
                continue
            with service_cls(season, catalog=catalog, repository=repository) as service:
                try:
                    service.probe_egress()
                    if schedule_due:
                        consume(service.sync_schedule())
                    else:
                        # Previews have an independent backlog, so a successful
                        # match cannot hide a preview failure on the next run.
                        # Each chunk touches at most 25 distinct match pages.
                        if matches:
                            probe = (item["coverage"]["closed"] == 0
                                     and item["coverage"]["unknown_stages"] > 0)
                            result = service.sync_matches(
                                limit=1 if probe else state.CHUNK_SIZE, lane="history",
                            )
                            consume(result)
                            attempted = result.attempted
                        elif previews:
                            consume(service.sync_previews(
                                match_ids=[p["game_id"] for p in previews], historical=True,
                            ))
                        if players:
                            consume(service.sync_profiles(
                                player_ids=sorted(players), limit=state.CHUNK_SIZE,
                                candidate_scopes=[season],
                            ))
                        if feeds_due:
                            consume(service.sync_stage_feeds())
                    did_work = True
                finally:
                    if collect_traffic is not None:
                        collect_traffic(service, season.scope)
            refreshed = inventory(catalog, repository, today=moment.date())
            failed = False
            return {"scope": item["key"], "wave": item["wave"], "attempted": attempted,
                    "did_work": did_work, "duration_seconds": time.monotonic() - started,
                    "pace": state.pace(refreshed, parked=checkpoint.parked(now=moment.timestamp()))}
        finally:
            checkpoint.release(item["key"], token,
                               now=moment.timestamp() + time.monotonic() - started,
                               failed=failed, idle=not did_work, duration=time.monotonic() - started)
    return {"reason": "idle", "attempted": 0,
            "pace": state.pace(scopes, parked=checkpoint.parked(now=moment.timestamp()))}
