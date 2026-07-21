"""Raw/replay endpoint specs for season, team and referee payloads.

This module only describes and replays endpoints. It never opens a browser or
selects a proxy. Specs default to ``paid_proxy=True`` so any online production
caller must obtain the capture engine's verified budget authorization first;
fixtures/direct canaries opt out explicitly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

from dags.utils.sofascore_dq import validate_schedule_rows
from scrapers.sofascore.camoufox_capture import (
    extract_tournament_standings,
    normalize_event,
    normalize_standing,
)
from scrapers.sofascore.capture_engine import (
    CaptureResult,
    EndpointSpec,
    SchemaValidationError,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    ManifestKey,
    ManifestStatus,
    ManifestStore,
)
from scrapers.sofascore.raw_store import (
    PayloadTarget,
    RawPayloadNotFound,
    RawPayloadRecord,
    RawPayloadStore,
    RawStoreError,
)


BASE_URL = "https://www.sofascore.com/api/v1"
SCHEDULE_DIRECTIONS = frozenset({"last", "next"})

SEASON_PATHS = {
    "standings_total": (
        "/unique-tournament/{source_tournament_id}/season/"
        "{source_season_id}/standings/total"
    ),
    "rounds": (
        "/unique-tournament/{source_tournament_id}/season/{source_season_id}/rounds"
    ),
    "cup_trees": (
        "/unique-tournament/{source_tournament_id}/season/{source_season_id}/cuptrees"
    ),
    "participants": (
        "/unique-tournament/{source_tournament_id}/season/{source_season_id}/teams"
    ),
}
SQUADS_PATH = (
    "/team/{team_id}/unique-tournament/{source_tournament_id}/season/"
    "{source_season_id}/players"
)
REFEREE_PROFILE_PATH = "/referee/{referee_id}"


class SeasonPlanningError(RuntimeError):
    """Stored state cannot prove a safe, complete season endpoint plan."""


class SeasonMaterializationError(RuntimeError):
    """A planned season partition failed a fail-closed publication gate."""


@dataclass(frozen=True)
class SeasonPartitionPlan:
    """Network-free endpoint plan derived only from committed local state.

    ``pending_keys`` is deliberately independent from raw availability. A raw
    payload without a terminal manifest still needs replay; a manifest without
    the raw schedule page promised by ``hasNextPage`` is also pending. This
    makes an interrupted page chain resumable without guessing that it ended.
    """

    source_tournament_id: str
    source_season_id: str
    freshness_key: str
    event_freshness_key: str
    specs: tuple[EndpointSpec, ...]
    pending_keys: tuple[ManifestKey, ...]
    missing_raw_keys: tuple[ManifestKey, ...]
    schedule_event_ids: tuple[str, ...]
    team_ids: tuple[str, ...]
    referee_ids: tuple[str, ...]
    player_universe_evidence_gaps: tuple[str, ...] = ()
    placeholder_team_ids: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.pending_keys and not self.player_universe_evidence_gaps


@dataclass(frozen=True)
class SeasonPartitionMaterialization:
    """DQ-approved normalized rows plus endpoint-level immutable raw lineage."""

    canonical_league: str
    canonical_season: str
    schedule_rows: tuple[dict, ...]
    standings_rows: tuple[dict, ...]
    raw_lineage: Mapping[ManifestKey, Mapping[str, object]]
    endpoint_statuses: Mapping[ManifestKey, ManifestStatus]
    deferred_keys: tuple[ManifestKey, ...]

    @property
    def endpoint_completeness(self) -> float:
        if not self.endpoint_statuses:
            return 1.0
        deferred = set(self.deferred_keys)
        return sum(
            status.terminal or key in deferred
            for key, status in self.endpoint_statuses.items()
        ) / len(self.endpoint_statuses)


@dataclass(frozen=True)
class _StoredPayload:
    payload: Optional[object]
    raw: Optional[RawPayloadRecord]

    @property
    def has_valid_json(self) -> bool:
        return self.payload is not None and self.raw is not None


def _positive_id(value: str | int, label: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer")
    token = str(value).strip()
    try:
        parsed = int(token)
    except ValueError as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return str(parsed)


def _source_ids(
    source_tournament_id: str | int,
    source_season_id: str | int,
) -> tuple[str, str]:
    return (
        _positive_id(source_tournament_id, "source_tournament_id"),
        _positive_id(source_season_id, "source_season_id"),
    )


def _is_placeholder_team(team: object) -> bool:
    """True for SofaScore knockout-bracket stubs ("Winner of match N").

    Unresolved cup slots arrive as team objects with a real numeric id and
    ``disabled: true`` (real teams omit the flag or send ``false``; see the
    bronze ``*_team_disabled`` columns). Stubs never appear in participants
    and have no squads, so counting them keeps the season plan permanently
    incomplete (#946). ``type`` does not discriminate and the registry
    ``enabled`` flag is unrelated.
    """
    return isinstance(team, Mapping) and team.get("disabled") is True


def _schedule_schema(source_season_id: str):
    def validate(payload: object) -> bool:
        if not isinstance(payload, Mapping):
            return False
        events = payload.get("events")
        if not isinstance(events, list):
            return False
        has_next = payload.get("hasNextPage")
        # Missing pagination evidence is not proof that traversal ended.
        if not isinstance(has_next, bool):
            return False
        if not events and has_next is True:
            return False
        for event in events:
            if not isinstance(event, Mapping) or event.get("id") is None:
                return False
            season = event.get("season")
            if (
                not isinstance(season, Mapping)
                or str(season.get("id")) != source_season_id
            ):
                return False
            home = event.get("homeTeam")
            away = event.get("awayTeam")
            if not isinstance(home, Mapping) or not isinstance(away, Mapping):
                return False
            home_id = home.get("id")
            away_id = away.get("id")
            if home_id is None or away_id is None or str(home_id) == str(away_id):
                return False
            kickoff = event.get("startTimestamp")
            if isinstance(kickoff, bool) or not isinstance(kickoff, (int, float)):
                return False
        return True

    return validate


def _schedule_parser(
    *,
    source_tournament_id: str,
    source_season_id: str,
    direction: str,
    page: int,
):
    def parse(payload: object) -> list[dict]:
        rows: list[dict] = []
        seen: set[str] = set()
        for event in payload["events"]:
            event_id = str(event["id"])
            if event_id in seen:
                raise SchemaValidationError(
                    f"duplicate event {event_id} in {direction} page {page}"
                )
            seen.add(event_id)
            row = normalize_event(dict(event))
            row.update(
                {
                    "source_tournament_id": source_tournament_id,
                    "source_season_id": source_season_id,
                    "source_page_direction": direction,
                    "source_page": page,
                }
            )
            rows.append(row)
        validate_schedule_rows(rows).require()
        return rows

    return parse


def build_schedule_page_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    direction: str,
    page: int,
    freshness_key: str,
    paid_proxy: bool = True,
) -> EndpointSpec:
    """Build one normalized ``events/last|next/{page}`` raw/replay spec."""

    tournament, season = _source_ids(source_tournament_id, source_season_id)
    direction = str(direction).strip().lower()
    if direction not in SCHEDULE_DIRECTIONS:
        raise ValueError("schedule direction must be 'last' or 'next'")
    if isinstance(page, bool) or not isinstance(page, int) or page < 0:
        raise ValueError("schedule page must be a non-negative integer")
    endpoint = f"schedule_{direction}"
    path = f"/unique-tournament/{tournament}/season/{season}/events/{direction}/{page}"
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type="season_page",
            target_id=f"{direction}:{page}",
            endpoint=endpoint,
            freshness_key=str(freshness_key),
        ),
        url=f"{BASE_URL}{path}",
        schema_validator=_schedule_schema(season),
        empty_predicate=lambda payload: payload["events"] == [],
        parsers={
            "schedule": _schedule_parser(
                source_tournament_id=tournament,
                source_season_id=season,
                direction=direction,
                page=page,
            )
        },
        paid_proxy=paid_proxy,
        # The seed page has no predecessor. SofaScore returns 404 when that
        # direction has no events (for example ``next/0`` after a season has
        # ended), which is a legitimate empty direction. Later pages are only
        # planned after a preceding ``hasNextPage=true`` and therefore keep a
        # 404 resumable/failing.
        legitimate_empty_http_statuses=(204, 404) if page == 0 else (204,),
        # A page explicitly promised by the preceding ``hasNextPage`` is
        # required. A transient 404 must remain resumable, not become a cached
        # ``not_supported`` hole in the schedule chain.
        not_supported_http_statuses=(),
    )


def _standings_path(tournament: str, season: str) -> str:
    return SEASON_PATHS["standings_total"].format(
        source_tournament_id=tournament,
        source_season_id=season,
    )


def _standing_rows(
    payload: object,
    *,
    source_tournament_id: str,
    source_season_id: str,
) -> list[dict]:
    path = f"/api/v1{_standings_path(source_tournament_id, source_season_id)}"
    return extract_tournament_standings(
        {
            path: {
                "status": 200,
                "challenge": False,
                "json": payload,
            }
        },
        int(source_tournament_id),
        int(source_season_id),
    )


def _standings_schema(payload: object) -> bool:
    if not isinstance(payload, Mapping):
        return False
    standings = payload.get("standings")
    if not isinstance(standings, list):
        return False
    for block in standings:
        if not isinstance(block, Mapping):
            return False
        rows = block.get("rows")
        if not isinstance(rows, list):
            return False
        for row in rows:
            if not isinstance(row, Mapping):
                return False
            team = row.get("team")
            if (
                not isinstance(team, Mapping)
                or team.get("id") is None
                or not str(team.get("name") or "").strip()
            ):
                return False
    return True


def _standings_parser(
    *,
    source_tournament_id: str,
    source_season_id: str,
):
    def parse(payload: object) -> list[dict]:
        source_rows = _standing_rows(
            payload,
            source_tournament_id=source_tournament_id,
            source_season_id=source_season_id,
        )
        rows: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for source in source_rows:
            team_id = str(source["team"]["id"])
            row = normalize_standing(source)
            group = str(row.get("group") or "")
            key = (group, team_id)
            if key in seen:
                raise SchemaValidationError(
                    f"duplicate standings row group={group!r} team={team_id}"
                )
            seen.add(key)
            row.update(
                {
                    "team_id": team_id,
                    "source_tournament_id": source_tournament_id,
                    "source_season_id": source_season_id,
                }
            )
            rows.append(row)
        return rows

    return parse


def build_standings_total_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    freshness_key: str,
    paid_proxy: bool = True,
) -> EndpointSpec:
    tournament, season = _source_ids(source_tournament_id, source_season_id)
    path = _standings_path(tournament, season)
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type="season",
            target_id=season,
            endpoint="standings_total",
            freshness_key=str(freshness_key),
        ),
        url=f"{BASE_URL}{path}",
        schema_validator=_standings_schema,
        empty_predicate=lambda payload: (
            not _standing_rows(
                payload,
                source_tournament_id=tournament,
                source_season_id=season,
            )
        ),
        parsers={
            "league_table": _standings_parser(
                source_tournament_id=tournament,
                source_season_id=season,
            )
        },
        paid_proxy=paid_proxy,
    )


def _collection_schema(collection: str):
    return lambda payload: (
        isinstance(payload, Mapping) and isinstance(payload.get(collection), list)
    )


def _collection_empty(collection: str):
    return lambda payload: payload[collection] == []


def _raw_only_season_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    endpoint: str,
    collection: str,
    freshness_key: str,
    paid_proxy: bool = True,
) -> EndpointSpec:
    tournament, season = _source_ids(source_tournament_id, source_season_id)
    path = SEASON_PATHS[endpoint].format(
        source_tournament_id=tournament,
        source_season_id=season,
    )
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type="season",
            target_id=season,
            endpoint=endpoint,
            freshness_key=str(freshness_key),
        ),
        url=f"{BASE_URL}{path}",
        schema_validator=_collection_schema(collection),
        empty_predicate=_collection_empty(collection),
        parsers={},
        paid_proxy=paid_proxy,
        raw_only=True,
    )


def build_rounds_spec(**kwargs) -> EndpointSpec:
    return _raw_only_season_spec(endpoint="rounds", collection="rounds", **kwargs)


def build_cup_trees_spec(**kwargs) -> EndpointSpec:
    return _raw_only_season_spec(endpoint="cup_trees", collection="cupTrees", **kwargs)


def build_participants_spec(**kwargs) -> EndpointSpec:
    return _raw_only_season_spec(endpoint="participants", collection="teams", **kwargs)


def build_squad_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    team_id: str | int,
    freshness_key: str,
    paid_proxy: bool = True,
) -> EndpointSpec:
    tournament, season = _source_ids(source_tournament_id, source_season_id)
    team = _positive_id(team_id, "team_id")
    path = SQUADS_PATH.format(
        team_id=team,
        source_tournament_id=tournament,
        source_season_id=season,
    )
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type="season_team",
            target_id=team,
            endpoint="squads",
            freshness_key=str(freshness_key),
        ),
        url=f"{BASE_URL}{path}",
        schema_validator=_collection_schema("players"),
        empty_predicate=_collection_empty("players"),
        parsers={},
        paid_proxy=paid_proxy,
        raw_only=True,
    )


def _referee_schema(referee_id: str):
    def validate(payload: object) -> bool:
        if not isinstance(payload, Mapping) or "referee" not in payload:
            return False
        referee = payload.get("referee")
        return referee is None or (
            isinstance(referee, Mapping) and str(referee.get("id")) == referee_id
        )

    return validate


def build_referee_profile_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    referee_id: str | int,
    freshness_key: str,
    paid_proxy: bool = True,
) -> EndpointSpec:
    tournament, season = _source_ids(source_tournament_id, source_season_id)
    referee = _positive_id(referee_id, "referee_id")
    path = REFEREE_PROFILE_PATH.format(referee_id=referee)
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type="referee",
            target_id=referee,
            endpoint="referee_profile",
            freshness_key=str(freshness_key),
        ),
        url=f"{BASE_URL}{path}",
        schema_validator=_referee_schema(referee),
        empty_predicate=lambda payload: payload["referee"] is None,
        parsers={},
        paid_proxy=paid_proxy,
        raw_only=True,
    )


def build_season_specs(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    freshness_key: str,
    paid_proxy: bool = True,
    last_pages: Sequence[int] = (0,),
    next_pages: Sequence[int] = (0,),
) -> list[EndpointSpec]:
    """Build the normalized and season-scoped raw-only endpoint plan."""

    common = {
        "source_tournament_id": source_tournament_id,
        "source_season_id": source_season_id,
        "freshness_key": freshness_key,
        "paid_proxy": paid_proxy,
    }
    specs = [
        *(
            build_schedule_page_spec(direction="last", page=page, **common)
            for page in last_pages
        ),
        *(
            build_schedule_page_spec(direction="next", page=page, **common)
            for page in next_pages
        ),
        build_standings_total_spec(**common),
        build_rounds_spec(**common),
        build_cup_trees_spec(**common),
        build_participants_spec(**common),
    ]
    keys = [spec.key for spec in specs]
    if len(keys) != len(set(keys)):
        raise ValueError("season endpoint plan contains duplicate manifest keys")
    return specs


def _load_stored_payload(
    raw_store: RawPayloadStore,
    spec: EndpointSpec,
) -> _StoredPayload:
    """Load and validate a committed payload without any transport fallback."""

    try:
        body, raw = raw_store.load_bytes(spec.raw_target)
    except RawPayloadNotFound:
        return _StoredPayload(None, None)
    except RawStoreError as exc:
        raise SeasonPlanningError(
            f"cannot read stored raw for {spec.key.stable_id()}: {exc}"
        ) from exc

    # Retryable/error HTML and body-less 204 responses cannot describe page
    # continuation or expansion evidence. Their manifest status is considered
    # separately by the caller.
    if not 200 <= raw.http_status < 300 or raw.http_status == 204:
        return _StoredPayload(None, raw)
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SeasonPlanningError(
            f"stored raw is not JSON for {spec.key.stable_id()}"
        ) from exc
    try:
        valid = spec.schema_validator(payload)
    except Exception as exc:
        raise SeasonPlanningError(
            f"stored schema validation failed for {spec.key.stable_id()}: {exc}"
        ) from exc
    if valid is False:
        raise SeasonPlanningError(f"stored schema rejected for {spec.key.stable_id()}")
    return _StoredPayload(payload, raw)


def _assert_terminal_raw_consistency(
    spec: EndpointSpec,
    stored: _StoredPayload,
    manifest,
) -> None:
    if manifest is None or not manifest.is_terminal:
        return
    if manifest.status == ManifestStatus.NOT_SUPPORTED:
        return
    if stored.raw is None:
        raise SeasonPlanningError(
            f"terminal manifest lost its raw payload for {spec.key.stable_id()}"
        )
    if (
        manifest.raw_content_hash
        and manifest.raw_content_hash != stored.raw.content_hash
    ):
        raise SeasonPlanningError(
            f"manifest/raw lineage mismatch for {spec.key.stable_id()}"
        )
    if manifest.status == ManifestStatus.SUCCESS:
        if not stored.has_valid_json:
            raise SeasonPlanningError(
                f"success manifest has no usable JSON for {spec.key.stable_id()}"
            )
        if spec.empty_predicate(stored.payload):
            raise SeasonPlanningError(
                f"success manifest points at an empty payload for {spec.key.stable_id()}"
            )
    elif (
        manifest.status == ManifestStatus.LEGITIMATE_EMPTY
        and stored.has_valid_json
        and not spec.empty_predicate(stored.payload)
    ):
        raise SeasonPlanningError(
            "legitimate-empty manifest points at non-empty JSON for "
            f"{spec.key.stable_id()}"
        )


def _event_referee_from_stored_page(
    raw_store: RawPayloadStore,
    *,
    source_tournament_id: str,
    source_season_id: str,
    event_id: str,
    freshness_key: str,
) -> Optional[str]:
    target = PayloadTarget(
        source_tournament_id=source_tournament_id,
        source_season_id=source_season_id,
        target_type="event",
        target_id=event_id,
        endpoint="event",
        freshness_key=freshness_key,
    )
    try:
        body, raw = raw_store.load_bytes(target)
    except RawPayloadNotFound:
        return None
    except RawStoreError as exc:
        raise SeasonPlanningError(
            f"cannot read stored full event {event_id}: {exc}"
        ) from exc
    if not 200 <= raw.http_status < 300 or raw.http_status == 204:
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SeasonPlanningError(f"stored full event {event_id} is not JSON") from exc
    if not isinstance(payload, Mapping):
        raise SeasonPlanningError(f"stored full event {event_id} is not an object")
    event = payload.get("event", payload)
    if not isinstance(event, Mapping) or str(event.get("id")) != event_id:
        raise SeasonPlanningError(
            f"stored full event identity mismatch for event {event_id}"
        )
    season = event.get("season")
    if not isinstance(season, Mapping) or str(season.get("id")) != source_season_id:
        raise SeasonPlanningError(f"stored full event {event_id} has a season mismatch")
    referee = event.get("referee")
    if referee is None:
        return None
    if not isinstance(referee, Mapping) or referee.get("id") is None:
        raise SeasonPlanningError(
            f"stored full event {event_id} has invalid referee evidence"
        )
    try:
        return _positive_id(referee["id"], "referee_id")
    except ValueError as exc:
        raise SeasonPlanningError(
            f"stored full event {event_id} has invalid referee evidence"
        ) from exc


def plan_season_partition(
    raw_store: RawPayloadStore,
    manifest_store: ManifestStore,
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    freshness_key: str,
    event_freshness_key: Optional[str] = None,
    paid_proxy: bool = True,
    max_pages: int = 50,
) -> SeasonPartitionPlan:
    """Derive a resumable partition plan solely from raw/manifest state.

    Both schedule directions start at page zero and advance only when the
    stored, season-validated page says ``hasNextPage=true``. The next spec is
    appended before it is inspected, so an interrupted chain remains visibly
    pending. Hitting ``max_pages`` while the source still promises another page
    raises instead of silently truncating the season.
    """

    tournament, season = _source_ids(source_tournament_id, source_season_id)
    freshness = str(freshness_key).strip()
    if not freshness:
        raise ValueError("freshness_key must not be empty")
    event_freshness = str(event_freshness_key or freshness).strip()
    if not event_freshness:
        raise ValueError("event_freshness_key must not be empty")
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise ValueError("max_pages must be a positive integer")

    common = {
        "source_tournament_id": tournament,
        "source_season_id": season,
        "freshness_key": freshness,
        "paid_proxy": paid_proxy,
    }
    specs: list[EndpointSpec] = []
    pending: list[ManifestKey] = []
    missing_raw: list[ManifestKey] = []
    event_ids: list[str] = []
    scheduled_team_ids: set[str] = set()
    placeholder_team_ids: set[str] = set()
    embedded_referee_ids: set[str] = set()
    player_universe_evidence_gaps: list[str] = []

    def append_key_once(keys: list[ManifestKey], key: ManifestKey) -> None:
        if key not in keys:
            keys.append(key)

    def inspect(
        spec: EndpointSpec,
        *,
        required_schedule_page: bool = False,
    ) -> _StoredPayload:
        stored = _load_stored_payload(raw_store, spec)
        manifest = manifest_store.get(spec.key)
        _assert_terminal_raw_consistency(spec, stored, manifest)

        terminal = bool(manifest and manifest.is_terminal)
        accepted_without_json = bool(
            manifest
            and terminal
            and (
                manifest.status == ManifestStatus.NOT_SUPPORTED
                or (
                    manifest.status == ManifestStatus.LEGITIMATE_EMPTY
                    and stored.raw is not None
                )
            )
        )
        if required_schedule_page:
            # Schedule pages, including a page promised by its predecessor,
            # are never optional. A seed page may nevertheless be a body-less
            # terminal empty (204/404) when that whole direction has no events.
            # Promised later pages do not accept 404 in their endpoint policy.
            accepted_schedule_empty = bool(
                manifest
                and manifest.status == ManifestStatus.LEGITIMATE_EMPTY
                and stored.raw is not None
                and stored.raw.http_status in spec.legitimate_empty_http_statuses
            )
            satisfied = bool(
                manifest
                and (
                    (
                        stored.has_valid_json
                        and manifest.status
                        in {
                            ManifestStatus.SUCCESS,
                            ManifestStatus.LEGITIMATE_EMPTY,
                        }
                    )
                    or accepted_schedule_empty
                )
            )
            if not satisfied:
                append_key_once(pending, spec.key)
            if not stored.has_valid_json and not accepted_schedule_empty:
                append_key_once(missing_raw, spec.key)
        else:
            if not terminal:
                append_key_once(pending, spec.key)
            if not stored.has_valid_json and not accepted_without_json:
                append_key_once(missing_raw, spec.key)
        return stored

    for direction in ("last", "next"):
        page = 0
        while True:
            spec = build_schedule_page_spec(
                direction=direction,
                page=page,
                **common,
            )
            specs.append(spec)
            stored = inspect(spec, required_schedule_page=True)
            if not stored.has_valid_json:
                # Crucially, do not infer end-of-chain from an absent page.
                break
            payload = stored.payload
            for event in payload["events"]:
                event_id = _positive_id(event["id"], "event_id")
                if event_id not in event_ids:
                    event_ids.append(event_id)
                for side in ("homeTeam", "awayTeam"):
                    try:
                        team = event[side]
                        team_id = _positive_id(team["id"], f"{side}_id")
                    except (KeyError, TypeError, ValueError) as exc:
                        raise SeasonPlanningError(
                            f"schedule event {event_id} has invalid {side} evidence"
                        ) from exc
                    if _is_placeholder_team(team):
                        # #946: an unresolved bracket slot is evidence for the
                        # event, not for any team; never fan squads out of it.
                        placeholder_team_ids.add(team_id)
                        continue
                    scheduled_team_ids.add(team_id)
                referee = event.get("referee")
                if referee is not None:
                    if not isinstance(referee, Mapping) or referee.get("id") is None:
                        raise SeasonPlanningError(
                            f"schedule event {event_id} has invalid referee evidence"
                        )
                    try:
                        embedded_referee_ids.add(
                            _positive_id(referee["id"], "referee_id")
                        )
                    except ValueError as exc:
                        raise SeasonPlanningError(
                            f"schedule event {event_id} has invalid referee evidence"
                        ) from exc
            if payload.get("hasNextPage") is not True:
                break
            if page + 1 >= max_pages:
                raise SeasonPlanningError(
                    f"schedule {direction} chain exceeded max_pages={max_pages}"
                )
            page += 1

    standings = build_standings_total_spec(**common)
    rounds = build_rounds_spec(**common)
    cup_trees = build_cup_trees_spec(**common)
    participants = build_participants_spec(**common)
    base_specs = (standings, rounds, cup_trees, participants)
    specs.extend(base_specs)
    participants_payload: Optional[object] = None
    for spec in base_specs:
        stored = inspect(spec)
        if spec.key.endpoint == "participants" and stored.has_valid_json:
            participants_payload = stored.payload

    participant_team_ids: set[str] = set()
    if participants_payload is not None:
        for index, team in enumerate(participants_payload["teams"]):
            if not isinstance(team, Mapping) or team.get("id") is None:
                raise SeasonPlanningError(
                    f"participants team at index {index} has no source id"
                )
            try:
                participant_team_ids.add(_positive_id(team["id"], "team_id"))
            except ValueError as exc:
                raise SeasonPlanningError(
                    f"participants team at index {index} has invalid source id"
                ) from exc

        missing_participants = scheduled_team_ids - participant_team_ids
        if not participant_team_ids:
            player_universe_evidence_gaps.append(
                "participants returned no teams; squad universe is unproven"
            )
            append_key_once(pending, participants.key)
        elif missing_participants:
            missing_tokens = ",".join(sorted(missing_participants, key=int))
            player_universe_evidence_gaps.append(
                "participants omitted scheduled team ids: " + missing_tokens
            )
            append_key_once(pending, participants.key)

    # Schedule evidence is authoritative for teams that actually played, while
    # participants can add registered teams that do not appear in the captured
    # page window.  Planning the union prevents a partial participants payload
    # from silently shrinking the squad/profile universe.
    team_ids = scheduled_team_ids | participant_team_ids

    for team_id in sorted(team_ids, key=int):
        spec = build_squad_spec(team_id=team_id, **common)
        specs.append(spec)
        stored = inspect(spec)
        squad_manifest = manifest_store.get(spec.key)
        squad_not_supported = bool(
            squad_manifest
            and squad_manifest.status == ManifestStatus.NOT_SUPPORTED
            and stored.raw is not None
            and stored.raw.http_status in spec.not_supported_http_statuses
        )
        if stored.has_valid_json and not stored.payload["players"]:
            player_universe_evidence_gaps.append(
                f"scheduled/participating team {team_id} has an empty squad"
            )
            append_key_once(pending, spec.key)
        elif (
            not stored.has_valid_json
            and squad_manifest is not None
            and squad_manifest.is_terminal
            and not squad_not_supported
        ):
            player_universe_evidence_gaps.append(
                f"scheduled/participating team {team_id} has no usable squad evidence"
            )
            append_key_once(pending, spec.key)

    referee_ids = set(embedded_referee_ids)
    for event_id in event_ids:
        referee_id = _event_referee_from_stored_page(
            raw_store,
            source_tournament_id=tournament,
            source_season_id=season,
            event_id=event_id,
            freshness_key=event_freshness,
        )
        if referee_id is not None:
            referee_ids.add(referee_id)
    for referee_id in sorted(referee_ids, key=int):
        spec = build_referee_profile_spec(referee_id=referee_id, **common)
        specs.append(spec)
        inspect(spec)

    keys = [spec.key for spec in specs]
    if len(keys) != len(set(keys)):
        raise SeasonPlanningError("derived season plan has duplicate manifest keys")
    return SeasonPartitionPlan(
        source_tournament_id=tournament,
        source_season_id=season,
        freshness_key=freshness,
        event_freshness_key=event_freshness,
        specs=tuple(specs),
        pending_keys=tuple(pending),
        missing_raw_keys=tuple(missing_raw),
        schedule_event_ids=tuple(event_ids),
        team_ids=tuple(sorted(team_ids, key=int)),
        referee_ids=tuple(sorted(referee_ids, key=int)),
        player_universe_evidence_gaps=tuple(player_universe_evidence_gaps),
        placeholder_team_ids=tuple(sorted(placeholder_team_ids, key=int)),
    )


def _canonical_token(value: object, label: str) -> str:
    token = str(value).strip()
    if not token:
        raise ValueError(f"{label} must not be empty")
    return token


# Per-page raw-lineage columns attached by _row_with_partition_and_lineage,
# plus the parser's own page provenance (source_page/source_page_direction).
# Two schedule rows for the same match fetched from different pages of a live
# feed differ ONLY in these; the match payload itself is identical. Without
# the source_page pair here the cross-page dedup never fires: a live feed
# (e.g. World Cup knockouts) that shifts a settled match between page windows
# repeats it with a different page number and the identical-payload check
# reports a false "duplicate schedule natural key" conflict (#951).
_SCHEDULE_LINEAGE_COLUMNS = frozenset({
    "raw_content_hash",
    "raw_blob_key",
    "raw_request_url",
    "raw_fetched_at",
    "raw_endpoint",
    "raw_target_id",
    "source_page",
    "source_page_direction",
})


def _row_with_partition_and_lineage(
    source: Mapping[str, object],
    *,
    plan: SeasonPartitionPlan,
    result: CaptureResult,
    canonical_league: str,
    canonical_season: str,
    require_event_season: bool,
) -> dict:
    row = dict(source)
    for field, expected in (
        ("source_tournament_id", plan.source_tournament_id),
        ("source_season_id", plan.source_season_id),
    ):
        if str(row.get(field)) != expected:
            raise SeasonMaterializationError(
                f"{field} mismatch for endpoint {result.manifest.key.endpoint}: "
                f"expected={expected}, observed={row.get(field)!r}"
            )
    if require_event_season and str(row.get("season_id")) != plan.source_season_id:
        raise SeasonMaterializationError(
            "season mismatch in normalized schedule row: "
            f"expected={plan.source_season_id}, observed={row.get('season_id')!r}"
        )
    for field, expected in (
        ("league", canonical_league),
        ("season", canonical_season),
    ):
        current = row.get(field)
        if current not in (None, "") and str(current) != expected:
            raise SeasonMaterializationError(
                f"canonical {field} mismatch: expected={expected}, observed={current!r}"
            )
        row[field] = expected
    manifest = result.manifest
    if not manifest.raw_content_hash or not manifest.raw_blob_key:
        raise SeasonMaterializationError(
            f"normalized endpoint {manifest.key.endpoint} lost raw lineage"
        )
    row.update(
        {
            "raw_content_hash": manifest.raw_content_hash,
            "raw_blob_key": manifest.raw_blob_key,
            "raw_request_url": manifest.request_url,
            "raw_fetched_at": manifest.fetched_at,
            "raw_endpoint": manifest.key.endpoint,
            "raw_target_id": manifest.key.target_id,
        }
    )
    return row


def _assert_normalized_result_integrity(
    spec: EndpointSpec,
    result: CaptureResult,
    *,
    require_live_raw: bool,
) -> None:
    manifest = result.manifest
    expected_datasets = set(spec.parsers)
    if set(result.datasets) != expected_datasets:
        raise SeasonMaterializationError(
            f"normalized endpoint {spec.key.endpoint} datasets mismatch: "
            f"expected={sorted(expected_datasets)}, "
            f"observed={sorted(result.datasets)}"
        )
    parsed_rows = sum(dataset.row_count for dataset in result.datasets.values())
    if parsed_rows <= 0 or parsed_rows != manifest.row_count:
        raise SeasonMaterializationError(
            f"normalized endpoint {spec.key.endpoint} row_count mismatch: "
            f"manifest={manifest.row_count}, parsed={parsed_rows}"
        )
    if not manifest.raw_content_hash or not manifest.raw_blob_key:
        raise SeasonMaterializationError(
            f"normalized endpoint {spec.key.endpoint} has no manifest raw lineage"
        )
    raw = result.raw
    if require_live_raw and raw is None:
        raise SeasonMaterializationError(
            f"deferred endpoint {spec.key.endpoint} has no committed raw record"
        )
    if raw is not None:
        if raw.target != spec.raw_target:
            raise SeasonMaterializationError(
                f"normalized endpoint {spec.key.endpoint} raw target mismatch"
            )
        if (
            raw.content_hash != manifest.raw_content_hash
            or raw.blob_key != manifest.raw_blob_key
        ):
            raise SeasonMaterializationError(
                f"normalized endpoint {spec.key.endpoint} raw lineage mismatch"
            )
        if not 200 <= raw.http_status < 300 or raw.http_status == 204:
            raise SeasonMaterializationError(
                f"normalized endpoint {spec.key.endpoint} has invalid raw HTTP status"
            )


def _is_deferred_materialization(
    spec: EndpointSpec,
    result: CaptureResult,
) -> bool:
    """Recognize only the production sink's explicit pre-MERGE state."""

    manifest = result.manifest
    if not (
        manifest.status == ManifestStatus.RETRYABLE_FAILURE
        and manifest.error_type == "DeferredMaterialization"
    ):
        return False
    if spec.raw_only:
        raise SeasonMaterializationError(
            f"raw-only endpoint {spec.key.endpoint} cannot be deferred"
        )
    if (
        manifest.http_status is None
        or not 200 <= manifest.http_status < 300
        or manifest.http_status == 204
    ):
        raise SeasonMaterializationError(
            f"deferred endpoint {spec.key.endpoint} has invalid HTTP status"
        )
    _assert_normalized_result_integrity(spec, result, require_live_raw=True)
    return True


def materialize_season_partition(
    plan: SeasonPartitionPlan,
    results: Sequence[CaptureResult],
    *,
    canonical_league: str,
    canonical_season: str,
) -> SeasonPartitionMaterialization:
    """Aggregate a complete replay and enforce partition-wide natural keys."""

    league = _canonical_token(canonical_league, "canonical_league")
    season = _canonical_token(canonical_season, "canonical_season")
    if plan.player_universe_evidence_gaps:
        raise SeasonMaterializationError(
            "player universe evidence is incomplete: "
            + "; ".join(plan.player_universe_evidence_gaps)
        )
    expected_keys = [spec.key for spec in plan.specs]
    result_keys = [result.manifest.key for result in results]
    if len(result_keys) != len(set(result_keys)):
        raise SeasonMaterializationError("duplicate CaptureResult manifest key")
    if set(result_keys) != set(expected_keys) or len(result_keys) != len(expected_keys):
        missing = set(expected_keys) - set(result_keys)
        unexpected = set(result_keys) - set(expected_keys)
        raise SeasonMaterializationError(
            "capture results do not match the planned partition: "
            f"missing={len(missing)}, unexpected={len(unexpected)}"
        )
    by_key = {result.manifest.key: result for result in results}
    statuses: dict[ManifestKey, ManifestStatus] = {}
    lineage: dict[ManifestKey, dict[str, object]] = {}
    schedule_rows: list[dict] = []
    standings_rows: list[dict] = []
    deferred_keys: list[ManifestKey] = []

    for spec in plan.specs:
        result = by_key[spec.key]
        manifest = result.manifest
        statuses[spec.key] = manifest.status
        deferred = _is_deferred_materialization(spec, result)
        if not manifest.is_terminal and not deferred:
            raise SeasonMaterializationError(
                f"endpoint {spec.key.endpoint} is nonterminal: {manifest.status.value}"
            )
        if deferred:
            deferred_keys.append(spec.key)
        if spec.key.endpoint.startswith("schedule_") and (
            manifest.status == ManifestStatus.NOT_SUPPORTED
        ):
            raise SeasonMaterializationError(
                f"required schedule page is not supported: {spec.key.target_id}"
            )
        if manifest.raw_content_hash and manifest.raw_blob_key:
            lineage[spec.key] = {
                "raw_content_hash": manifest.raw_content_hash,
                "raw_blob_key": manifest.raw_blob_key,
                "request_url": manifest.request_url,
                "http_status": manifest.http_status,
                "fetched_at": manifest.fetched_at,
            }
        if spec.raw_only or manifest.status in {
            ManifestStatus.LEGITIMATE_EMPTY,
            ManifestStatus.NOT_SUPPORTED,
        }:
            if result.datasets:
                raise SeasonMaterializationError(
                    f"endpoint {spec.key.endpoint} unexpectedly emitted datasets"
                )
            continue

        _assert_normalized_result_integrity(
            spec,
            result,
            require_live_raw=deferred,
        )

        if spec.key.endpoint.startswith("schedule_"):
            dataset = result.datasets.get("schedule")
            if dataset is None:
                raise SeasonMaterializationError(
                    f"successful schedule page {spec.key.target_id} has no dataset"
                )
            schedule_rows.extend(
                _row_with_partition_and_lineage(
                    row,
                    plan=plan,
                    result=result,
                    canonical_league=league,
                    canonical_season=season,
                    require_event_season=True,
                )
                for row in dataset.rows
            )
        elif spec.key.endpoint == "standings_total":
            dataset = result.datasets.get("league_table")
            if dataset is None:
                raise SeasonMaterializationError(
                    "successful standings endpoint has no league_table dataset"
                )
            standings_rows.extend(
                _row_with_partition_and_lineage(
                    row,
                    plan=plan,
                    result=result,
                    canonical_league=league,
                    canonical_season=season,
                    require_event_season=False,
                )
                for row in dataset.rows
            )

    try:
        validate_schedule_rows(schedule_rows).require()
    except Exception as exc:
        raise SeasonMaterializationError(f"schedule skeleton DQ failed: {exc}") from exc

    # A live paginated events feed (an in-progress tournament, e.g. the World
    # Cup during its knockout stage) can return the SAME match on two DIFFERENT
    # pages when a sibling match settles between page fetches and shifts the
    # window. Those rows carry identical match payload and differ only in the
    # per-page raw-lineage columns, so collapse them. A duplicate that repeats
    # within one page (same raw_blob_key) is a parser defect, and a duplicate
    # whose match payload disagrees is a data conflict — both stay hard errors.
    def _schedule_payload(row: Mapping[str, object]) -> dict:
        return {k: v for k, v in row.items() if k not in _SCHEDULE_LINEAGE_COLUMNS}

    deduped_schedule: dict[tuple[str, str, str], dict] = {}
    for row in schedule_rows:
        event_id = row.get("game_id")
        if event_id in (None, ""):
            raise SeasonMaterializationError("schedule natural key has no game_id")
        key = (league, season, str(event_id))
        existing = deduped_schedule.get(key)
        if existing is not None:
            same_payload = _schedule_payload(existing) == _schedule_payload(row)
            cross_page = existing.get("raw_blob_key") != row.get("raw_blob_key")
            if same_payload and cross_page:
                continue
            raise SeasonMaterializationError(f"duplicate schedule natural key: {key}")
        deduped_schedule[key] = row
    schedule_rows = list(deduped_schedule.values())

    standings_keys: set[tuple[str, str, str, str]] = set()
    for row in standings_rows:
        team = row.get("team")
        group = row.get("group")
        if team in (None, ""):
            raise SeasonMaterializationError("standings natural key has no team")
        if group in (None, ""):
            raise SeasonMaterializationError("standings natural key has no group scope")
        key = (league, season, str(group), str(team))
        if key in standings_keys:
            raise SeasonMaterializationError(f"duplicate standings natural key: {key}")
        standings_keys.add(key)

    return SeasonPartitionMaterialization(
        canonical_league=league,
        canonical_season=season,
        schedule_rows=tuple(schedule_rows),
        standings_rows=tuple(standings_rows),
        raw_lineage=lineage,
        endpoint_statuses=statuses,
        deferred_keys=tuple(deferred_keys),
    )


def replay_season_specs(
    engine: SofaScoreCaptureEngine,
    specs: Sequence[EndpointSpec],
) -> list[CaptureResult]:
    """Revalidate JSON raw offline while preserving body-less terminal states."""

    # ``not_supported`` may intentionally have no JSON body, and a 204
    # legitimate-empty payload may intentionally not be JSON. Preserve those
    # already-terminal outcomes while force-replaying every JSON-backed endpoint.
    cache_specs: list[EndpointSpec] = []
    replay_specs: list[EndpointSpec] = []
    for spec in specs:
        existing = engine.manifest_store.get(spec.key)
        cache_only = bool(existing and existing.status == ManifestStatus.NOT_SUPPORTED)
        if existing and existing.status == ManifestStatus.LEGITIMATE_EMPTY:
            try:
                _, raw = engine.raw_store.load_bytes(spec.raw_target)
            except RawPayloadNotFound:
                raw = None
            cache_only = bool(
                raw and raw.http_status in spec.legitimate_empty_http_statuses
            )
        (cache_specs if cache_only else replay_specs).append(spec)

    replayed = engine.capture_many(
        replay_specs,
        offline=True,
        force_replay=True,
    )
    cached = engine.capture_many(
        cache_specs,
        offline=True,
        force_replay=False,
    )
    by_key = {result.manifest.key: result for result in (*replayed, *cached)}
    return [by_key[spec.key] for spec in specs]


def replay_season_partition(
    engine: SofaScoreCaptureEngine,
    plan: SeasonPartitionPlan,
    *,
    canonical_league: str,
    canonical_season: str,
) -> SeasonPartitionMaterialization:
    """Replay and materialize the whole stored partition with zero network."""

    results = replay_season_specs(engine, plan.specs)
    return materialize_season_partition(
        plan,
        results,
        canonical_league=canonical_league,
        canonical_season=canonical_season,
    )


def squad_player_ids(
    raw_store: RawPayloadStore,
    plan: SeasonPartitionPlan,
) -> tuple[str, ...]:
    """Resolve registered players from exact planned squad raw, without I/O.

    A source-supported season squad contributes registered players. SofaScore
    currently returns 404 for the season-scoped squad route; that explicit
    terminal ``not_supported`` state contributes no IDs and is not confused
    with missing/malformed evidence. The current-only ``/team/{id}/players``
    route is deliberately not used for historical season attribution.
    """
    if plan.player_universe_evidence_gaps:
        raise SeasonPlanningError(
            "player universe evidence is incomplete: "
            + "; ".join(plan.player_universe_evidence_gaps)
        )
    if not plan.team_ids:
        raise SeasonPlanningError("season plan has no team evidence")

    player_ids: set[str] = set()
    squad_team_ids: set[str] = set()
    for spec in plan.specs:
        if spec.key.endpoint != "squads":
            continue
        squad_team_ids.add(str(spec.key.target_id))
        stored = _load_stored_payload(raw_store, spec)
        if (
            stored.raw is not None
            and stored.raw.http_status in spec.not_supported_http_statuses
        ):
            continue
        if not stored.has_valid_json:
            raise SeasonPlanningError(
                f"planned squad {spec.key.target_id} has no valid raw payload"
            )
        for index, entry in enumerate(stored.payload["players"]):
            if not isinstance(entry, Mapping):
                raise SeasonPlanningError(
                    f"squad {spec.key.target_id} entry {index} is not an object"
                )
            player = entry.get("player", entry)
            if not isinstance(player, Mapping) or player.get("id") is None:
                raise SeasonPlanningError(
                    f"squad {spec.key.target_id} entry {index} has no player id"
                )
            try:
                player_ids.add(_positive_id(player["id"], "player_id"))
            except ValueError as exc:
                raise SeasonPlanningError(
                    f"squad {spec.key.target_id} entry {index} has invalid player id"
                ) from exc
    missing_squads = set(plan.team_ids) - squad_team_ids
    if missing_squads:
        raise SeasonPlanningError(
            "season plan omitted squad endpoints for team ids: "
            + ",".join(sorted(missing_squads, key=int))
        )
    return tuple(sorted(player_ids, key=int))


__all__ = [
    "BASE_URL",
    "REFEREE_PROFILE_PATH",
    "SEASON_PATHS",
    "SQUADS_PATH",
    "SeasonMaterializationError",
    "SeasonPartitionMaterialization",
    "SeasonPartitionPlan",
    "SeasonPlanningError",
    "build_cup_trees_spec",
    "build_participants_spec",
    "build_referee_profile_spec",
    "build_rounds_spec",
    "build_schedule_page_spec",
    "build_season_specs",
    "build_squad_spec",
    "build_standings_total_spec",
    "materialize_season_partition",
    "plan_season_partition",
    "replay_season_partition",
    "replay_season_specs",
    "squad_player_ids",
]
