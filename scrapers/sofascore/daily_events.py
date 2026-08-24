"""Source daily event lists as ``bronze.sofascore_schedule`` evidence.

The all-men refresh lane (#1218, lane F) has no season pages for the ~1490
tournaments outside ``competitions.yaml``.  Instead it asks the source for
the football events of one UTC day (``/sport/football/scheduled-events/{date}``
plus the ``/inverse`` list of the same day in the other time zones), keeps the
events of ready campaign tournaments and writes them as schedule rows tagged
``league=SS-<id>`` / ``season=<canonical>`` from the campaign snapshot, so the
existing match phase can pick finished matches straight from Bronze.

Every list is fetched through the metered discovery client (source
``sofascore_discovery``) and kept in the raw store first, as the exact
response bytes the browser received.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Collection, Iterable, Mapping, Optional

from dags.utils.sofascore_dq import validate_schedule_rows
from scrapers.sofascore.camoufox_capture import normalize_event
from scrapers.sofascore.discovery import DiscoverySchemaError
from scrapers.sofascore.raw_store import (
    PayloadTarget,
    RawPayloadRecord,
    RawPayloadStore,
)


log = logging.getLogger(__name__)

SCHEDULED_EVENTS_ENDPOINT = "scheduled_events"
SCHEDULED_EVENTS_TARGET_TYPE = "date"
SCHEDULED_EVENTS_FRESHNESS_KEY = "daily"
# A daily list is not tournament-scoped; the raw-store target identity still
# needs both source ids, so they are pinned to the "no tournament" sentinel.
_NO_TOURNAMENT_ID = "0"
_REQUEST_BASE_URL = "https://www.sofascore.com/api/v1"


class DailyEventsSchemaError(DiscoverySchemaError):
    """Daily events no longer carry the tournament/season/id shape we map."""


@dataclass(frozen=True)
class FetchedEvent:
    """One source event together with the raw record of the list it came from."""

    event: Mapping[str, Any]
    raw: RawPayloadRecord


def scheduled_events_paths(day: date) -> tuple[tuple[str, str], ...]:
    """Return ``(path, target_id)`` for the day list and its inverse list."""

    iso = day.isoformat()
    base = f"/sport/football/scheduled-events/{iso}"
    return ((base, iso), (f"{base}/inverse", f"{iso}-inverse"))


def fetch_daily_events(
    client: Any, dates: Iterable[date], raw_store: RawPayloadStore
) -> list[FetchedEvent]:
    """Fetch every daily list via ``client.get_json_bytes`` into the raw store.

    The exact response bytes are stored before the payload is validated so a
    schema surprise from the source leaves replayable evidence behind.
    """

    fetched: list[FetchedEvent] = []
    for day in dates:
        for path, target_id in scheduled_events_paths(day):
            body, payload = client.get_json_bytes(path)
            record = raw_store.store_bytes(
                PayloadTarget(
                    source_tournament_id=_NO_TOURNAMENT_ID,
                    source_season_id=_NO_TOURNAMENT_ID,
                    target_type=SCHEDULED_EVENTS_TARGET_TYPE,
                    target_id=target_id,
                    endpoint=SCHEDULED_EVENTS_ENDPOINT,
                    freshness_key=SCHEDULED_EVENTS_FRESHNESS_KEY,
                ),
                body,
                request_url=f"{_REQUEST_BASE_URL}{path}",
                http_status=200,
            )
            events = payload.get("events") if isinstance(payload, Mapping) else None
            if not isinstance(events, list):
                raise DiscoverySchemaError(f"{path} has no events list")
            fetched.extend(
                FetchedEvent(event=event, raw=record)
                for event in events
                if isinstance(event, Mapping)
            )
    return fetched


def _nested_int(value: Any, *keys: str) -> Optional[int]:
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _snapshot_index(
    snapshot: Mapping[str, Any],
) -> dict[int, tuple[str, dict[int, str]]]:
    """Map ready ``unique_tournament_id`` -> (capture_key, {season_id: canonical})."""

    index: dict[int, tuple[str, dict[int, str]]] = {}
    for tournament in snapshot.get("tournaments", ()):
        if tournament.get("metadata_status") != "ready":
            continue
        seasons = {
            int(season["source_season_id"]): str(season["canonical_season"])
            for season in tournament.get("seasons", ())
            if season.get("canonical_season")
        }
        index[int(tournament["unique_tournament_id"])] = (
            str(tournament["capture_key"]),
            seasons,
        )
    return index


def schedule_rows_from_events(
    events: Iterable[FetchedEvent],
    snapshot: Mapping[str, Any],
    exclude_leagues: Collection[int],
) -> tuple[list[dict], dict[str, int]]:
    """Turn daily events into schedule rows for ready snapshot tournaments.

    ``exclude_leagues`` holds the source ``unique_tournament_id`` values of the
    configured leagues that the daily ingest already covers.  Those events and
    the ones of tournaments absent from the snapshot (other genders, youth,
    amateur) are out of scope and counted in ``excluded``; a ``season.id`` the
    snapshot does not know yet is counted in ``unknown_seasons`` for the
    metadata wave to pick up.  An event without ``id``,
    ``tournament.uniqueTournament.id`` or ``season.id`` is ``malformed``: that
    is schema drift, not a foreign tournament, and fails the day (the raw list
    is already kept).  A game listed more than once (D-1 and D lists overlap
    across time zones) keeps its last, freshest copy.
    """

    index = _snapshot_index(snapshot)
    excluded_ids = {int(value) for value in exclude_leagues}
    counters = {
        "events": 0,
        "matched": 0,
        "excluded": 0,
        "unknown_seasons": 0,
        "malformed": 0,
    }
    rows_by_game: dict[int, dict] = {}
    for item in events:
        counters["events"] += 1
        event_id = _nested_int(item.event, "id")
        tournament_id = _nested_int(item.event, "tournament", "uniqueTournament", "id")
        season_id = _nested_int(item.event, "season", "id")
        if event_id is None or tournament_id is None or season_id is None:
            counters["malformed"] += 1
            continue
        entry = None if tournament_id in excluded_ids else index.get(tournament_id)
        if entry is None:
            counters["excluded"] += 1
            continue
        capture_key, seasons = entry
        canonical_season = seasons.get(season_id)
        if canonical_season is None:
            counters["unknown_seasons"] += 1
            continue
        row = normalize_event(dict(item.event))
        row.update(
            {
                "league": capture_key,
                "season": canonical_season,
                "source_tournament_id": str(tournament_id),
                "source_season_id": str(season_id),
                "raw_content_hash": item.raw.content_hash,
                "raw_blob_key": item.raw.blob_key,
                "raw_request_url": item.raw.request_url,
                "raw_fetched_at": item.raw.fetched_at,
                "raw_endpoint": item.raw.endpoint,
                "raw_target_id": item.raw.target_id,
            }
        )
        rows_by_game[row["game_id"]] = row
        counters["matched"] += 1
    if counters["malformed"]:
        raise DailyEventsSchemaError(
            f"{counters['malformed']} of {counters['events']} daily events lack "
            f"id, tournament.uniqueTournament.id or season.id: {counters}"
        )
    if counters["events"] and not counters["matched"]:
        # Legitimate (a day of configured/foreign tournaments only), but a
        # "green and empty" outcome must be visible.
        log.warning(
            "no daily event matched a ready campaign tournament: %s", counters
        )
    rows = list(rows_by_game.values())
    validate_schedule_rows(rows).require()
    return rows, counters
