"""One-pass offline ESPN Summary normalization."""

from __future__ import annotations

import hashlib
import math
import re
from types import MappingProxyType
from typing import Any, Mapping

from .models import CapabilityState, Competition, Edition
from .parser_common import (
    EspnParseError,
    canonical_json,
    decode_object,
    native_id,
    optional_bool,
    optional_nonnegative_int,
    optional_string,
    required_list,
    required_mapping,
    required_string,
    unknown_fields,
    utc_datetime,
)
from .parser_contracts import (
    EntityParseState,
    LINEUP_STAT_MAP_VERSION,
    LineupRow,
    MATCHSHEET_STAT_MAP_VERSION,
    MatchsheetRow,
    PARSER_VERSION,
    ScheduleRow,
    SummaryParseResult,
)


LINEUP_STAT_NAME_MAP: Mapping[str, str] = MappingProxyType(
    {
        "appearances": "appearances",
        "foulsCommitted": "fouls_committed",
        "foulsSuffered": "fouls_suffered",
        "goalAssists": "goal_assists",
        "goalsConceded": "goals_conceded",
        "offsides": "offsides",
        "ownGoals": "own_goals",
        "redCards": "red_cards",
        "saves": "saves",
        "shotsFaced": "shots_faced",
        "shotsOnTarget": "shots_on_target",
        "subIns": "sub_ins",
        "totalGoals": "total_goals",
        "totalShots": "total_shots",
        "yellowCards": "yellow_cards",
    }
)

MATCHSHEET_STAT_NAME_MAP: Mapping[str, str] = MappingProxyType(
    {
        "accurateCrosses": "accurate_crosses",
        "accurateLongBalls": "accurate_long_balls",
        "accuratePasses": "accurate_passes",
        "blockedShots": "blocked_shots",
        "crossPct": "cross_pct",
        "effectiveClearance": "effective_clearance",
        "effectiveTackles": "effective_tackles",
        "foulsCommitted": "fouls_committed",
        "fouls": "fouls_committed",
        "goalAssists": "goal_assists",
        "goalDifference": "goal_difference",
        "goalsConceded": "goals_conceded",
        "interceptions": "interceptions",
        "longballPct": "longball_pct",
        "offsides": "offsides",
        "passPct": "pass_pct",
        "penaltyKickGoals": "penalty_kick_goals",
        "penaltyKickShots": "penalty_kick_shots",
        "possessionPct": "possession_pct",
        "possession": "possession_pct",
        "redCards": "red_cards",
        "saves": "saves",
        "shotPct": "shot_pct",
        "shotsOnTarget": "shots_on_target",
        "tacklePct": "tackle_pct",
        "totalClearance": "total_clearance",
        "totalCrosses": "total_crosses",
        "totalGoals": "total_goals",
        "totalLongBalls": "total_long_balls",
        "totalPasses": "total_passes",
        "shots": "total_shots",
        "totalShots": "total_shots",
        "totalTackles": "total_tackles",
        "wonCorners": "won_corners",
        "cornerKicks": "won_corners",
        "yellowCards": "yellow_cards",
    }
)

# ESPN omitted Kai Jennings from AFC Wimbledon's otherwise explicit XI for
# event 761072. Independent match reports list him as the eleventh starter, so
# this exact canonical lineup source (rosters plus format) is known to be
# truncated. Discard its lineup; never synthesize the missing player or relax
# cardinality elsewhere.
_REVIEWED_TRUNCATED_LINEUPS: Mapping[
    str, tuple[str, int, tuple[tuple[int, int], ...]]
] = MappingProxyType(
    {
        "41c1ce43ba84ebb976040b5fb748f7a54c01d1d47ae5eff36193d74d9f289aad": (
            "18481:2025",
            761072,
            ((347, 11), (3802, 10)),
        ),
        "0d8f88f7e3486d1b40328e62f67c7484fe31373894c59a38490711b32f4960ef": (
            "19834:2026",
            401872737,
            ((124, 11), (3384, 10)),
        ),
    }
)

# ESPN published internally contradictory starter/substitution flags for these
# exact lineups. Preserve no player rows rather than guess which source flag is
# correct.
_REVIEWED_CONTRADICTORY_LINEUPS: Mapping[
    str, tuple[str, int, tuple[tuple[int, int], ...]]
] = MappingProxyType(
    {
        "287b2052375fe3ef2fc4fc24f8c69f0be23d20adac832d86a098fc194275985f": (
            "19778:2025",
            734179,
            ((2664, 11), (2728, 11)),
        ),
        "dc4b54fc66f6d2ce7b7004c8fcb6411e59ca9470ae6386be6c745a4dc933788c": (
            "19778:2025",
            734184,
            ((214, 11), (2641, 11)),
        ),
        "e3a51e4590879e092163e1fad6a377467b4f7cc361fd56d3c91fac4e7965c2f9": (
            "19831:2020",
            565756,
            ((2829, 11), (18210, 12)),
        ),
        "6083361093816508832ea3be234c8cf475e4a5725d9d47871e7ca262c2594345": (
            "19831:2020",
            599000,
            ((2875, 11), (2888, 11)),
        ),
        "d42c97067cc2ac708e6e0085dd5735b7ef8c899a06d0658ffd7b2061cb412274": (
            "19834:2026",
            401867393,
            ((367, 11), (22344, 11)),
        ),
        "3698912fea6e1545167896f6aa1571491f7e6210c5f2f1e2ec0f895384236b30": (
            "19915:2026",
            401841831,
            ((20684, 11), (22525, 11)),
        ),
    }
)

# One Club Friendly Summary contains a one-player roster for only one side.
# Bind the waiver to the complete canonical lineup source and event identity.
_REVIEWED_ONE_SIDED_LINEUPS: Mapping[str, tuple[str, int]] = MappingProxyType(
    {
        "54c233a36e49dee961703a659ef03de013d445659614f3e3450bad0e63ad9ced": (
            "19834:2026",
            401897918,
        )
    }
)


def _valid_empty_or_fail(capability: CapabilityState, entity: str) -> EntityParseState:
    if capability is CapabilityState.PROVEN:
        raise EspnParseError(f"proven {entity} section is absent or empty")
    if capability not in {
        CapabilityState.PARTIAL,
        CapabilityState.ABSENT,
        CapabilityState.UNKNOWN,
    }:
        raise EspnParseError(f"{entity} capability does not permit valid_empty")
    return EntityParseState.VALID_EMPTY


def _validate_context(
    competition: Competition, edition: Edition, event: ScheduleRow
) -> None:
    if not isinstance(competition, Competition) or not isinstance(edition, Edition):
        raise TypeError("competition and edition must be registry models")
    if not isinstance(event, ScheduleRow):
        raise TypeError("event must be a normalized ScheduleRow")
    if edition not in competition.editions:
        raise EspnParseError("edition is not promoted for this competition")
    if (
        event.competition_id != competition.espn_id
        or event.source_season_year != edition.source_season_year
        or event.scope_id != competition.scope_id(edition)
    ):
        raise EspnParseError("Summary parser context does not match schedule scope")


def _header_sides(
    payload: Mapping[str, Any], event: ScheduleRow
) -> tuple[Mapping[str, Any], dict[int, tuple[str, str]], dict[str, Any]]:
    header = required_mapping(payload.get("header"), "summary.header")
    header_id = native_id(header.get("id"), "summary.header.id")
    if header_id != event.event_id:
        raise EspnParseError("summary.header.id does not match schedule event_id")
    competitions = required_list(
        header.get("competitions"), "summary.header.competitions"
    )
    if len(competitions) != 1:
        raise EspnParseError("summary.header must have exactly one competition")
    header_competition = required_mapping(
        competitions[0], "summary.header.competitions[0]"
    )
    kickoff = utc_datetime(
        header_competition.get("date"), "summary.header.competitions[0].date"
    )
    if kickoff != event.kickoff:
        raise EspnParseError("Summary kickoff does not match normalized schedule event")
    competitors = required_list(
        header_competition.get("competitors"),
        "summary.header.competitions[0].competitors",
    )
    if len(competitors) != 2:
        raise EspnParseError("Summary header must have exactly two competitors")
    by_team: dict[int, tuple[str, str]] = {}
    by_side: dict[str, int] = {}
    nested_extras: dict[str, Any] = {}
    for index, raw_competitor in enumerate(competitors):
        field = f"summary.header.competitors[{index}]"
        competitor = required_mapping(raw_competitor, field)
        home_away = required_string(competitor.get("homeAway"), f"{field}.homeAway")
        if home_away not in {"home", "away"} or home_away in by_side:
            raise EspnParseError("Summary header must have unique home and away sides")
        team = required_mapping(competitor.get("team"), f"{field}.team")
        team_id = native_id(team.get("id"), f"{field}.team.id")
        team_name = required_string(
            team.get("displayName"), f"{field}.team.displayName"
        )
        if team_id in by_team:
            raise EspnParseError("Summary header team IDs must be distinct")
        by_team[team_id] = (home_away, team_name)
        by_side[home_away] = team_id
        competitor_extra = unknown_fields(competitor, ("homeAway", "team", "score"))
        team_extra = unknown_fields(team, ("id", "displayName"))
        if competitor_extra or team_extra:
            nested_extras[home_away] = {
                key: value
                for key, value in (
                    ("competitor", competitor_extra),
                    ("team", team_extra),
                )
                if value
            }
    expected = {event.home_team_id: "home", event.away_team_id: "away"}
    if {team_id: side for team_id, (side, _) in by_team.items()} != expected:
        raise EspnParseError("Summary header teams/homeAway do not match schedule")
    competition_extra = unknown_fields(
        header_competition, ("date", "competitors", "id", "status", "venue")
    )
    if competition_extra:
        nested_extras["competition"] = competition_extra
    return header_competition, by_team, nested_extras


def _team_block(
    raw: Any, field: str, by_team: Mapping[int, tuple[str, str]]
) -> tuple[int, str, str, Mapping[str, Any]]:
    block = required_mapping(raw, field)
    team = required_mapping(block.get("team"), f"{field}.team")
    team_id = native_id(team.get("id"), f"{field}.team.id")
    if team_id not in by_team:
        raise EspnParseError(f"{field}.team.id is not a Summary header team")
    side, header_name = by_team[team_id]
    if "homeAway" in block:
        block_side = required_string(block["homeAway"], f"{field}.homeAway")
        if block_side != side:
            raise EspnParseError(f"{field}.homeAway conflicts with native team ID")
    team_name = optional_string(team.get("displayName"), f"{field}.team.displayName")
    if team_name is not None and team_name != header_name:
        # Display strings are not identity; retain the section-local value.
        header_name = team_name
    return team_id, side, header_name, block


def _substitution_flag(value: Any, field: str) -> bool | None:
    if value is None or type(value) is bool:
        return value
    detail = required_mapping(value, field)
    if "didSub" not in detail:
        raise EspnParseError(f"{field}.didSub is required for substitution objects")
    return optional_bool(detail["didSub"], f"{field}.didSub")


def _substitution_minute(value: Any, field: str) -> int | None:
    if not isinstance(value, Mapping) or "clock" not in value:
        return None
    clock = required_mapping(value["clock"], f"{field}.clock")
    display = required_string(clock.get("displayValue"), f"{field}.clock.displayValue")
    parts = re.findall(r"\d{1,3}", display)
    return sum(int(part) for part in parts) if parts else None


def _legacy_substitutions(
    player: Mapping[str, Any],
    *,
    field: str,
    starter: bool | None,
    subbed_in: bool | None,
    subbed_out: bool | None,
) -> tuple[str | None, str | None]:
    events: list[Mapping[str, Any]] = []
    for key in ("subbedIn", "subbedOut"):
        value = player.get(key)
        if isinstance(value, Mapping) and value.get("didSub") is True:
            events.append(value)
    if not events and (subbed_in or subbed_out) and "plays" in player:
        plays = required_list(player["plays"], f"{field}.plays")
        for index, raw_play in enumerate(plays):
            play = required_mapping(raw_play, f"{field}.plays[{index}]")
            if play.get("substitution") is True:
                events.append(play)
    minutes = [
        minute
        for index, event in enumerate(events)
        if (minute := _substitution_minute(event, f"{field}.substitution[{index}]"))
        is not None
    ]
    sub_in: str | None
    if starter is True:
        sub_in = "start"
    elif subbed_in is True:
        sub_in = str(minutes[0]) if minutes else None
    else:
        sub_in = None
    if subbed_out is True:
        minute_index = 1 if subbed_in is True and len(minutes) > 1 else 0
        sub_out = str(minutes[minute_index]) if minutes else None
    elif (starter is True or subbed_in is True) and subbed_out is False:
        sub_out = "end"
    else:
        sub_out = None
    return sub_in, sub_out


def _parse_game_info(
    payload: Mapping[str, Any], event: ScheduleRow
) -> tuple[
    int | None,
    str | None,
    int | None,
    str | None,
    int | None,
    str | None,
    dict[str, Any],
    tuple[dict[str, Any], ...],
]:
    if "gameInfo" not in payload or payload["gameInfo"] is None:
        return None, None, None, None, None, None, {}, ()
    info = required_mapping(payload["gameInfo"], "summary.gameInfo")
    venue_id: int | None = None
    venue_name: str | None = None
    capacity: str | None = None
    venue_extra: dict[str, Any] = {}
    if "venue" in info and info["venue"] is not None:
        venue = required_mapping(info["venue"], "summary.gameInfo.venue")
        if "id" in venue and venue["id"] is not None:
            venue_id = native_id(venue["id"], "summary.gameInfo.venue.id")
        venue_name = optional_string(
            venue.get("fullName"), "summary.gameInfo.venue.fullName"
        )
        capacity_value = optional_nonnegative_int(
            venue.get("capacity"), "summary.gameInfo.venue.capacity"
        )
        capacity = str(capacity_value) if capacity_value is not None else None
        venue_extra = unknown_fields(venue, ("id", "fullName"))
        if (
            event.venue_id is not None
            and venue_id is not None
            and event.venue_id != venue_id
        ):
            raise EspnParseError("Summary venue ID conflicts with schedule venue ID")
    attendance = optional_nonnegative_int(
        info.get("attendance"), "summary.gameInfo.attendance"
    )
    referee_id: int | None = None
    referee_name: str | None = None
    ambiguous_officials: tuple[dict[str, Any], ...] = ()
    official_extras: list[dict[str, Any]] = []
    if "officials" in info:
        officials = required_list(info["officials"], "summary.gameInfo.officials")
        referees: list[tuple[int, Mapping[str, Any]]] = []
        for index, raw_official in enumerate(officials):
            official = required_mapping(
                raw_official, f"summary.gameInfo.officials[{index}]"
            )
            raw_position = official.get("position")
            if raw_position is None:
                # ESPN commonly emits fourth/reserve officials without a role.
                # With no explicit classification, preserve the full source row.
                official_extras.append(dict(official))
                continue
            position = required_mapping(
                raw_position, f"summary.gameInfo.officials[{index}].position"
            )
            label = position.get("name", position.get("displayName"))
            primary = isinstance(label, str) and label.strip().upper() in {
                "REFEREE",
                "MATCH REFEREE",
            }
            if primary:
                referees.append((index, official))
            else:
                # An explicit but unrecognized role cannot safely populate the
                # primary referee fields. Preserve the complete official row.
                official_extras.append(dict(official))
                continue
            official_extra = unknown_fields(official, ("id", "fullName", "position"))
            position_extra = unknown_fields(position, ("name", "displayName"))
            if official_extra or position_extra:
                official_extras.append(
                    {
                        **official_extra,
                        **({"position": position_extra} if position_extra else {}),
                    }
                )
            else:
                official_extras.append({})
        if len(referees) > 1:
            # A scalar referee column cannot represent multiple equally typed
            # source rows. Validate and preserve all officials without guessing.
            for index, referee in referees:
                if "id" in referee and referee["id"] is not None:
                    native_id(
                        referee["id"],
                        f"summary.gameInfo.officials[{index}].id",
                    )
                required_string(
                    referee.get("fullName"),
                    f"summary.gameInfo.officials[{index}].fullName",
                )
            official_extras = [
                dict(required_mapping(row, f"summary.gameInfo.officials[{index}]"))
                for index, row in enumerate(officials)
            ]
            ambiguous_officials = tuple(official_extras)
        elif referees:
            _, referee = referees[0]
            if "id" in referee and referee["id"] is not None:
                referee_id = native_id(referee["id"], "summary referee.id")
            referee_name = required_string(
                referee.get("fullName"), "summary referee.fullName"
            )
    extra = unknown_fields(info, ("venue", "attendance", "officials"))
    if venue_extra:
        extra["venue"] = venue_extra
    if any(official_extras):
        extra["officials"] = official_extras
    return (
        venue_id,
        venue_name,
        attendance,
        capacity,
        referee_id,
        referee_name,
        extra,
        ambiguous_officials,
    )


def _lineup_stat_entries(statistics: Any, field: str) -> list[tuple[str, Any, str]]:
    entries: list[tuple[str, Any, str]] = []
    if isinstance(statistics, Mapping):
        for raw_name in sorted(statistics, key=str):
            name = required_string(raw_name, f"{field} statistic name")
            raw_value = statistics[raw_name]
            item_field = f"{field}.{name}"
            if isinstance(raw_value, Mapping):
                mapped_name = raw_value.get("name", name)
                name = required_string(mapped_name, f"{item_field}.name")
                if "value" in raw_value:
                    raw_value = raw_value["value"]
                elif "displayValue" in raw_value:
                    raw_value = raw_value["displayValue"]
                else:
                    raw_value = None
            entries.append((name, raw_value, item_field))
        return entries
    rows = required_list(statistics, field)
    for index, raw_stat in enumerate(rows):
        stat = required_mapping(raw_stat, f"{field}[{index}]")
        name = required_string(stat.get("name"), f"{field}[{index}].name")
        value = stat.get("value")
        if value is None:
            value = stat.get("displayValue")
        entries.append((name, value, f"{field}[{index}]"))
    return entries


def _lineup_stat_values(sources: list[tuple[str, Any]], field: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for source_name, statistics in sources:
        for name, value, item_field in _lineup_stat_entries(
            statistics, f"{field}.{source_name}"
        ):
            target = LINEUP_STAT_NAME_MAP.get(name)
            if target is None:
                continue
            if isinstance(value, bool):
                raise EspnParseError(f"{item_field}.value must be numeric")
            if isinstance(value, (int, float)):
                normalized = float(value)
            elif (
                isinstance(value, str)
                and _NUMERIC_DISPLAY_RE.fullmatch(value.strip()) is not None
                and not value.strip().endswith("%")
            ):
                normalized = float(value.strip())
            else:
                raise EspnParseError(f"{item_field}.value must be numeric")
            if not math.isfinite(normalized):
                raise EspnParseError(f"{item_field}.value must be finite")
            existing = values.get(target)
            if existing is not None and existing != normalized:
                raise EspnParseError(
                    f"{field} has conflicting mapped statistic {target!r}: "
                    f"{existing} versus {normalized}"
                )
            values[target] = normalized
    return values


def _lineup(
    payload: Mapping[str, Any],
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
    by_team: Mapping[int, tuple[str, str]],
) -> tuple[tuple[LineupRow, ...], EntityParseState]:
    capability = edition.capabilities.lineup
    if "rosters" not in payload:
        return (), _valid_empty_or_fail(capability, "lineup")
    rosters = required_list(payload["rosters"], "summary.rosters")
    if not rosters:
        return (), _valid_empty_or_fail(capability, "lineup")
    lineup_source: dict[str, Any] = {"rosters": rosters}
    if "format" in payload:
        lineup_source["format"] = payload["format"]
    lineup_source_sha256 = hashlib.sha256(
        canonical_json(lineup_source).encode("utf-8")
    ).hexdigest()
    blocks: dict[int, tuple[str, str, Mapping[str, Any]]] = {}
    for index, raw_roster in enumerate(rosters):
        team_id, side, team_name, block = _team_block(
            raw_roster, f"summary.rosters[{index}]", by_team
        )
        if team_id in blocks:
            raise EspnParseError("Summary rosters contain a duplicate team ID")
        blocks[team_id] = (side, team_name, block)
    if set(blocks) != set(by_team):
        raise EspnParseError("Summary lineup must contain both event teams")
    roster_presence = ["roster" in block for _, _, block in blocks.values()]
    if not any(roster_presence):
        return (), _valid_empty_or_fail(capability, "lineup")
    if not all(roster_presence):
        reviewed_identity = _REVIEWED_ONE_SIDED_LINEUPS.get(lineup_source_sha256)
        if reviewed_identity == (event.scope_id, event.event_id):
            return (), _valid_empty_or_fail(capability, "lineup")
        raise EspnParseError(
            "Summary lineup rosters must exist for both or neither team"
        )

    rows: list[LineupRow] = []
    per_team_rows: dict[int, list[LineupRow]] = {}
    contradictory_substitution_semantics = False
    seen: set[tuple[int, int, int]] = set()
    for team_id, (side, team_name, block) in blocks.items():
        roster = required_list(
            block.get("roster"), f"summary.rosters[{team_id}].roster"
        )
        if not roster:
            raise EspnParseError("Summary lineup team roster must not be empty")
        team_rows: list[LineupRow] = []
        for index, raw_player in enumerate(roster):
            field = f"summary.rosters[{team_id}].roster[{index}]"
            player = required_mapping(raw_player, field)
            athlete = required_mapping(player.get("athlete"), f"{field}.athlete")
            athlete_id = native_id(athlete.get("id"), f"{field}.athlete.id")
            player_name = required_string(
                athlete.get("displayName"), f"{field}.athlete.displayName"
            )
            key = (event.event_id, team_id, athlete_id)
            if key in seen:
                raise EspnParseError(
                    "Summary lineup has duplicate event/team/athlete row"
                )
            seen.add(key)
            jersey_raw = athlete.get("jersey")
            if jersey_raw is None:
                jersey = None
            elif type(jersey_raw) is int and jersey_raw >= 0:
                jersey = str(jersey_raw)
            else:
                jersey = optional_string(jersey_raw, f"{field}.athlete.jersey")
            starter = optional_bool(player.get("starter"), f"{field}.starter")
            captain = optional_bool(player.get("captain"), f"{field}.captain")
            subbed_in = _substitution_flag(player.get("subbedIn"), f"{field}.subbedIn")
            subbed_out = _substitution_flag(
                player.get("subbedOut"), f"{field}.subbedOut"
            )
            if (starter is True and subbed_in is True) or (
                starter is False and subbed_out is True and subbed_in is not True
            ):
                contradictory_substitution_semantics = True
            sub_in, sub_out = _legacy_substitutions(
                player,
                field=field,
                starter=starter,
                subbed_in=subbed_in,
                subbed_out=subbed_out,
            )
            raw_position = player.get("position", athlete.get("position"))
            position: str | None = None
            if raw_position is not None:
                position_value = required_mapping(raw_position, f"{field}.position")
                position = optional_string(
                    position_value.get(
                        "name",
                        position_value.get(
                            "displayName", position_value.get("abbreviation")
                        ),
                    ),
                    f"{field}.position.name",
                )
            raw_formation_place = player.get("formationPlace")
            if raw_formation_place is None:
                formation_place = None
            elif type(raw_formation_place) is int and raw_formation_place >= 0:
                formation_place = str(raw_formation_place)
            else:
                formation_place = optional_string(
                    raw_formation_place, f"{field}.formationPlace"
                )
            stat_sources = [
                (name, player[name])
                for name in ("stats", "statistics")
                if name in player
            ]
            if len(stat_sources) == 2:
                statistics = {
                    "statistics": player["statistics"],
                    "stats": player["stats"],
                }
            elif stat_sources:
                statistics = stat_sources[0][1]
            else:
                statistics = []
            legacy_stats = _lineup_stat_values(stat_sources, f"{field}.statistics")
            substitution_fields = {
                key: value
                for key, value in player.items()
                if key
                in {
                    "subbedIn",
                    "subbedOut",
                    "substitution",
                    "substitutions",
                    "plays",
                }
            }
            extra = unknown_fields(
                player,
                (
                    "athlete",
                    "starter",
                    "captain",
                    "subbedIn",
                    "subbedOut",
                    "substitution",
                    "substitutions",
                    "statistics",
                    "stats",
                    "position",
                    "formationPlace",
                    "plays",
                ),
            )
            athlete_extra = unknown_fields(
                athlete, ("id", "displayName", "shortName", "jersey", "position")
            )
            if athlete_extra:
                extra["athlete"] = athlete_extra
            row = LineupRow(
                scope_id=event.scope_id,
                competition_id=competition.espn_id,
                event_id=event.event_id,
                source_season_year=edition.source_season_year,
                team_id=team_id,
                team=team_name,
                home_away=side,
                is_home=side == "home",
                athlete_id=athlete_id,
                player=player_name,
                jersey=jersey,
                position=position,
                formation_place=formation_place,
                starter=starter,
                captain=captain,
                subbed_in=subbed_in,
                subbed_out=subbed_out,
                sub_in=sub_in,
                sub_out=sub_out,
                appearances=legacy_stats.get("appearances"),
                fouls_committed=legacy_stats.get("fouls_committed"),
                fouls_suffered=legacy_stats.get("fouls_suffered"),
                goal_assists=legacy_stats.get("goal_assists"),
                goals_conceded=legacy_stats.get("goals_conceded"),
                offsides=legacy_stats.get("offsides"),
                own_goals=legacy_stats.get("own_goals"),
                red_cards=legacy_stats.get("red_cards"),
                saves=legacy_stats.get("saves"),
                shots_faced=legacy_stats.get("shots_faced"),
                shots_on_target=legacy_stats.get("shots_on_target"),
                sub_ins=legacy_stats.get("sub_ins"),
                total_goals=legacy_stats.get("total_goals"),
                total_shots=legacy_stats.get("total_shots"),
                yellow_cards=legacy_stats.get("yellow_cards"),
                substitutions_json=canonical_json(substitution_fields),
                statistics_json=canonical_json(statistics),
                stat_map_version=LINEUP_STAT_MAP_VERSION,
                league=event.league,
                season=event.season,
                game=event.game,
                parser_version=PARSER_VERSION,
                extra_json=canonical_json(extra),
            )
            rows.append(row)
            team_rows.append(row)
        per_team_rows[team_id] = team_rows

    explicit_starter_semantics = any(
        row.starter is not None
        for team_rows in per_team_rows.values()
        for row in team_rows
    )
    if explicit_starter_semantics:
        if any(
            row.starter is None
            for team_rows in per_team_rows.values()
            for row in team_rows
        ):
            raise EspnParseError(
                "explicit starter semantics require a starter flag for every athlete"
            )
        starter_counts = {
            team_id: sum(row.starter is True for row in team_rows)
            for team_id, team_rows in per_team_rows.items()
        }
        counts = tuple(starter_counts.values())
        conventional_xi = all(count == 11 for count in counts)
        small_sided_size: int | None = None
        if "format" in payload:
            match_format = required_mapping(payload["format"], "summary.format")
            configured_size = match_format.get("startersPerTeam")
            if configured_size is None and "regulation" in match_format:
                regulation = required_mapping(
                    match_format["regulation"], "summary.format.regulation"
                )
                configured_size = regulation.get("startersPerTeam")
            if configured_size is not None:
                if type(configured_size) is not int or not 1 <= configured_size <= 7:
                    raise EspnParseError(
                        "summary.format.startersPerTeam must be an integer from 1 to 7"
                    )
                small_sided_size = configured_size
        # Non-XI capture requires explicit source format evidence.
        balanced_small_sided = (
            small_sided_size is not None
            and len(set(counts)) == 1
            and counts[0] == small_sided_size
        )
        observed_identity = (
            event.scope_id,
            event.event_id,
            tuple(sorted(starter_counts.items())),
        )
        if contradictory_substitution_semantics:
            reviewed_identity = _REVIEWED_CONTRADICTORY_LINEUPS.get(
                lineup_source_sha256
            )
            if reviewed_identity == observed_identity:
                return (), _valid_empty_or_fail(capability, "lineup")
            raise EspnParseError(
                "Summary lineup has contradictory starter/substitution semantics"
            )
        if not conventional_xi and not balanced_small_sided:
            # Some ESPN competitions expose a sparse event-participant list in
            # ``rosters`` (for example, only the scorer) while still attaching
            # explicit starter flags.  Fewer than seven athlete rows cannot
            # field a conventional team.  Never publish those partial player
            # rows, but let a non-PROVEN capability preserve the event's
            # schedule and matchsheet.  Complete rosters with bad starter
            # semantics remain a hard error below, and explicit balanced
            # small-sided formats were accepted above.
            incomplete_conventional_roster = any(
                len(team_rows) < 7 for team_rows in per_team_rows.values()
            )
            if (
                incomplete_conventional_roster
                and small_sided_size is None
                and capability is not CapabilityState.PROVEN
            ):
                return (), _valid_empty_or_fail(capability, "lineup")
            reviewed_identity = _REVIEWED_TRUNCATED_LINEUPS.get(lineup_source_sha256)
            if (
                capability is not CapabilityState.PROVEN
                and reviewed_identity == observed_identity
            ):
                return (), _valid_empty_or_fail(capability, "lineup")
            raise EspnParseError(
                "explicit conventional lineup must contain 11 starters per team; "
                f"got {starter_counts}"
            )
    return (
        tuple(
            sorted(
                rows,
                key=lambda row: (row.home_away != "home", row.team_id, row.athlete_id),
            )
        ),
        EntityParseState.CAPTURED,
    )


_NUMERIC_DISPLAY_RE = re.compile(r"[+-]?\d+(?:\.\d+)?%?")


def _stat_scalar(stat: Mapping[str, Any], field: str) -> str:
    value = stat.get("value")
    if value is None:
        value = stat.get("displayValue")
    if isinstance(value, bool) or isinstance(value, (list, Mapping)) or value is None:
        raise EspnParseError(f"{field}.value must be a supported scalar value")
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise EspnParseError(f"{field}.value must be finite")
        return str(value)
    if isinstance(value, str):
        display = value.strip()
        if not display or _NUMERIC_DISPLAY_RE.fullmatch(display) is None:
            raise EspnParseError(f"{field}.value must be a numeric display scalar")
        return display
    raise EspnParseError(f"{field}.value must be a supported scalar value")


def _stat_values(statistics: list[Any], field: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for index, raw_stat in enumerate(statistics):
        stat = required_mapping(raw_stat, f"{field}[{index}]")
        name = required_string(stat.get("name"), f"{field}[{index}].name")
        target = MATCHSHEET_STAT_NAME_MAP.get(name)
        if target is None:
            continue
        scalar = _stat_scalar(stat, f"{field}[{index}]")
        if target in values:
            raise EspnParseError(f"{field} maps duplicate statistic {target!r}")
        values[target] = scalar
    return values


def _matchsheet(
    payload: Mapping[str, Any],
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
    by_team: Mapping[int, tuple[str, str]],
    game_info: tuple[
        int | None,
        str | None,
        int | None,
        str | None,
        int | None,
        str | None,
        dict[str, Any],
        tuple[dict[str, Any], ...],
    ],
) -> tuple[tuple[MatchsheetRow, ...], EntityParseState]:
    capability = edition.capabilities.matchsheet
    if "boxscore" not in payload:
        return (), _valid_empty_or_fail(capability, "matchsheet")
    boxscore = required_mapping(payload["boxscore"], "summary.boxscore")
    if "teams" not in boxscore:
        raise EspnParseError("summary.boxscore.teams is required when boxscore exists")
    teams = required_list(boxscore["teams"], "summary.boxscore.teams")
    if not teams:
        return (), _valid_empty_or_fail(capability, "matchsheet")
    blocks: dict[int, tuple[str, str, Mapping[str, Any]]] = {}
    for index, raw_team in enumerate(teams):
        team_id, side, team_name, block = _team_block(
            raw_team, f"summary.boxscore.teams[{index}]", by_team
        )
        if team_id in blocks:
            raise EspnParseError("Summary boxscore contains a duplicate team ID")
        blocks[team_id] = (side, team_name, block)
    if set(blocks) != set(by_team):
        raise EspnParseError("Summary matchsheet must contain both event teams")
    statistics_presence = ["statistics" in block for _, _, block in blocks.values()]
    if not any(statistics_presence):
        return (), _valid_empty_or_fail(capability, "matchsheet")
    if not all(statistics_presence):
        raise EspnParseError(
            "Summary matchsheet statistics must exist for both or neither team"
        )
    statistics_by_team = {
        team_id: required_list(
            block.get("statistics"),
            f"summary.boxscore.teams[{team_id}].statistics",
        )
        for team_id, (_, _, block) in blocks.items()
    }
    empty_statistics = {
        team_id for team_id, statistics in statistics_by_team.items() if not statistics
    }
    if len(empty_statistics) == len(statistics_by_team):
        return (), _valid_empty_or_fail(capability, "matchsheet")
    if empty_statistics:
        raise EspnParseError(
            "Summary matchsheet statistics must be empty for both or neither team"
        )

    (
        venue_id,
        venue_name,
        attendance,
        capacity,
        referee_id,
        referee_name,
        _,
        ambiguous_officials,
    ) = game_info
    roster_by_team: dict[int, str] = {}
    if "rosters" in payload:
        rosters = required_list(payload["rosters"], "summary.rosters")
        for index, raw_roster in enumerate(rosters):
            roster_team_id, _, _, roster_block = _team_block(
                raw_roster, f"summary.rosters[{index}]", by_team
            )
            if roster_team_id in roster_by_team:
                raise EspnParseError("Summary rosters contain a duplicate team ID")
            if "roster" in roster_block:
                roster_by_team[roster_team_id] = canonical_json(
                    required_list(
                        roster_block["roster"],
                        f"summary.rosters[{index}].roster",
                    )
                )
    score_by_team = {
        event.home_team_id: event.home_score,
        event.away_team_id: event.away_score,
    }
    rows: list[MatchsheetRow] = []
    for team_id, (side, team_name, block) in blocks.items():
        statistics = statistics_by_team[team_id]
        values = _stat_values(
            statistics, f"summary.boxscore.teams[{team_id}].statistics"
        )
        if not set(values).intersection(
            {"total_shots", "shots_on_target", "possession_pct"}
        ):
            raise EspnParseError(
                "Summary matchsheet team must contain a recognized core statistic"
            )
        extra = unknown_fields(
            block, ("team", "homeAway", "statistics", "displayOrder")
        )
        if ambiguous_officials:
            if "summaryGameInfo" in extra:
                raise EspnParseError(
                    "Summary matchsheet extra field collides with preserved gameInfo"
                )
            extra["summaryGameInfo"] = {"officials": ambiguous_officials}
        rows.append(
            MatchsheetRow(
                scope_id=event.scope_id,
                competition_id=competition.espn_id,
                event_id=event.event_id,
                source_season_year=edition.source_season_year,
                team_id=team_id,
                team=team_name,
                home_away=side,
                is_home=side == "home",
                score=score_by_team[team_id],
                accurate_crosses=values.get("accurate_crosses"),
                accurate_long_balls=values.get("accurate_long_balls"),
                accurate_passes=values.get("accurate_passes"),
                blocked_shots=values.get("blocked_shots"),
                capacity=capacity,
                cross_pct=values.get("cross_pct"),
                effective_clearance=values.get("effective_clearance"),
                effective_tackles=values.get("effective_tackles"),
                fouls_committed=values.get("fouls_committed"),
                goal_assists=values.get("goal_assists"),
                goal_difference=values.get("goal_difference"),
                goals_conceded=values.get("goals_conceded"),
                interceptions=values.get("interceptions"),
                longball_pct=values.get("longball_pct"),
                offsides=values.get("offsides"),
                pass_pct=values.get("pass_pct"),
                penalty_kick_goals=values.get("penalty_kick_goals"),
                penalty_kick_shots=values.get("penalty_kick_shots"),
                possession_pct=values.get("possession_pct"),
                red_cards=values.get("red_cards"),
                roster=roster_by_team.get(team_id),
                saves=values.get("saves"),
                shot_pct=values.get("shot_pct"),
                shots_on_target=values.get("shots_on_target"),
                tackle_pct=values.get("tackle_pct"),
                total_clearance=values.get("total_clearance"),
                total_crosses=values.get("total_crosses"),
                total_goals=values.get("total_goals"),
                total_long_balls=values.get("total_long_balls"),
                total_passes=values.get("total_passes"),
                total_shots=values.get("total_shots"),
                total_tackles=values.get("total_tackles"),
                won_corners=values.get("won_corners"),
                yellow_cards=values.get("yellow_cards"),
                corner_kicks=values.get("won_corners"),
                statistics_json=canonical_json(statistics),
                stat_map_version=MATCHSHEET_STAT_MAP_VERSION,
                venue_id=venue_id,
                venue=venue_name,
                attendance=attendance,
                referee_id=referee_id,
                referee=referee_name,
                league=event.league,
                season=event.season,
                game=event.game,
                parser_version=PARSER_VERSION,
                extra_json=canonical_json(extra),
            )
        )
    return (
        tuple(sorted(rows, key=lambda row: row.home_away != "home")),
        EntityParseState.CAPTURED,
    )


def parse_summary(
    raw: bytes,
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
) -> SummaryParseResult:
    """Decode one Summary exactly once and derive both Bronze entity shapes."""
    _validate_context(competition, edition, event)
    payload = decode_object(raw, "Summary")
    _, by_team, header_nested_extra = _header_sides(payload, event)
    game_info = _parse_game_info(payload, event)
    lineup, lineup_state = _lineup(
        payload,
        competition=competition,
        edition=edition,
        event=event,
        by_team=by_team,
    )
    matchsheet, matchsheet_state = _matchsheet(
        payload,
        competition=competition,
        edition=edition,
        event=event,
        by_team=by_team,
        game_info=game_info,
    )
    root_extra = unknown_fields(payload, ("header", "boxscore", "rosters", "gameInfo"))
    header_extra = unknown_fields(
        required_mapping(payload["header"], "summary.header"),
        ("id", "competitions", "season", "week", "league"),
    )
    extras: dict[str, Any] = {}
    if root_extra:
        extras.update(root_extra)
    if header_extra:
        extras["header"] = header_extra
    if header_nested_extra:
        extras["headerSections"] = header_nested_extra
    if "boxscore" in payload and isinstance(payload["boxscore"], Mapping):
        boxscore_extra = unknown_fields(payload["boxscore"], ("teams",))
        if boxscore_extra:
            extras["boxscore"] = boxscore_extra
    if "rosters" in payload and isinstance(payload["rosters"], list):
        roster_extras: dict[str, Any] = {}
        for index, raw_roster in enumerate(payload["rosters"]):
            if not isinstance(raw_roster, Mapping):
                continue
            block_extra = unknown_fields(raw_roster, ("homeAway", "team", "roster"))
            raw_team = raw_roster.get("team")
            team_extra = (
                unknown_fields(raw_team, ("id", "displayName"))
                if isinstance(raw_team, Mapping)
                else {}
            )
            if block_extra or team_extra:
                roster_extras[str(index)] = {
                    key: value
                    for key, value in (("roster", block_extra), ("team", team_extra))
                    if value
                }
        if roster_extras:
            extras["rosters"] = roster_extras
    if game_info[-2]:
        extras["gameInfo"] = game_info[-2]
    return SummaryParseResult(
        event_id=event.event_id,
        lineup=lineup,
        matchsheet=matchsheet,
        lineup_state=lineup_state,
        matchsheet_state=matchsheet_state,
        parser_version=PARSER_VERSION,
        extra_json=canonical_json(extras),
    )
