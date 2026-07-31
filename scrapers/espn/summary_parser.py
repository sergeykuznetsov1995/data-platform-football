"""One-pass offline ESPN Summary normalization."""

from __future__ import annotations

import re
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
    LineupRow,
    MatchsheetRow,
    PARSER_VERSION,
    ScheduleRow,
    SummaryParseResult,
)


def _valid_empty_or_fail(capability: CapabilityState, entity: str) -> EntityParseState:
    if capability is CapabilityState.PROVEN:
        raise EspnParseError(f"proven {entity} section is absent or empty")
    if capability not in {CapabilityState.PARTIAL, CapabilityState.ABSENT}:
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
) -> tuple[str | int | None, str | int | None]:
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
    sub_in: str | int | None
    if starter is True:
        sub_in = "start"
    elif subbed_in is True:
        sub_in = minutes[0] if minutes else None
    else:
        sub_in = None
    if subbed_out is True:
        minute_index = 1 if subbed_in is True and len(minutes) > 1 else 0
        sub_out = minutes[minute_index] if minutes else None
    elif (starter is True or subbed_in is True) and subbed_out is False:
        sub_out = "end"
    else:
        sub_out = None
    return sub_in, sub_out


def _parse_game_info(
    payload: Mapping[str, Any], event: ScheduleRow
) -> tuple[int | None, str | None, int | None, int | None, str | None, dict[str, Any]]:
    if "gameInfo" not in payload or payload["gameInfo"] is None:
        return None, None, None, None, None, {}
    info = required_mapping(payload["gameInfo"], "summary.gameInfo")
    venue_id: int | None = None
    venue_name: str | None = None
    if "venue" in info and info["venue"] is not None:
        venue = required_mapping(info["venue"], "summary.gameInfo.venue")
        if "id" in venue and venue["id"] is not None:
            venue_id = native_id(venue["id"], "summary.gameInfo.venue.id")
        venue_name = optional_string(
            venue.get("fullName"), "summary.gameInfo.venue.fullName"
        )
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
    if "officials" in info:
        officials = required_list(info["officials"], "summary.gameInfo.officials")
        referees: list[Mapping[str, Any]] = []
        for index, raw_official in enumerate(officials):
            official = required_mapping(
                raw_official, f"summary.gameInfo.officials[{index}]"
            )
            position = required_mapping(
                official.get("position"),
                f"summary.gameInfo.officials[{index}].position",
            )
            label = position.get("name", position.get("displayName"))
            if isinstance(label, str) and label.strip().upper() in {
                "REFEREE",
                "MATCH REFEREE",
            }:
                referees.append(official)
        if len(referees) > 1:
            raise EspnParseError("Summary contains multiple primary referees")
        if referees:
            referee = referees[0]
            if "id" in referee and referee["id"] is not None:
                referee_id = native_id(referee["id"], "summary referee.id")
            referee_name = required_string(
                referee.get("fullName"), "summary referee.fullName"
            )
    return (
        venue_id,
        venue_name,
        attendance,
        referee_id,
        referee_name,
        unknown_fields(info, ("venue", "attendance", "officials")),
    )


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

    rows: list[LineupRow] = []
    per_team_rows: dict[int, list[LineupRow]] = {}
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
            if "stats" in player and "statistics" in player:
                statistics = {
                    "statistics": player["statistics"],
                    "stats": player["stats"],
                }
            else:
                statistics = player.get("stats", player.get("statistics", []))
            if not isinstance(statistics, (list, Mapping)):
                raise EspnParseError(f"{field}.statistics must be an array or object")
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
                substitutions_json=canonical_json(substitution_fields),
                statistics_json=canonical_json(statistics),
                league=event.league,
                season=event.season,
                game=event.game,
                parser_version=PARSER_VERSION,
                extra_json=canonical_json(extra),
            )
            rows.append(row)
            team_rows.append(row)
        per_team_rows[team_id] = team_rows

    # Apply XI cardinality only when the response actually supplies a conventional
    # full roster with explicit starter flags for both sides.
    conventional = all(
        len(team_rows) >= 11 and all(row.starter is not None for row in team_rows)
        for team_rows in per_team_rows.values()
    )
    if conventional:
        for team_id, team_rows in per_team_rows.items():
            if sum(row.starter is True for row in team_rows) != 11:
                raise EspnParseError(
                    f"conventional lineup team {team_id} must have 11 starters"
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


def _stat_values(statistics: list[Any], field: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, raw_stat in enumerate(statistics):
        stat = required_mapping(raw_stat, f"{field}[{index}]")
        name = required_string(stat.get("name"), f"{field}[{index}].name")
        if name in values:
            raise EspnParseError(f"{field} contains duplicate statistic {name!r}")
        values[name] = stat.get("value", stat.get("displayValue"))
    return values


def _matchsheet(
    payload: Mapping[str, Any],
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
    by_team: Mapping[int, tuple[str, str]],
    game_info: tuple[
        int | None, str | None, int | None, int | None, str | None, dict[str, Any]
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

    venue_id, venue_name, attendance, referee_id, referee_name, _ = game_info
    score_by_team = {
        event.home_team_id: event.home_score,
        event.away_team_id: event.away_score,
    }
    rows: list[MatchsheetRow] = []
    for team_id, (side, team_name, block) in blocks.items():
        statistics = required_list(
            block.get("statistics"), f"summary.boxscore.teams[{team_id}].statistics"
        )
        if not statistics:
            raise EspnParseError("Summary matchsheet team must contain core statistics")
        values = _stat_values(
            statistics, f"summary.boxscore.teams[{team_id}].statistics"
        )
        if not set(values).intersection(
            {"shots", "totalShots", "shotsOnTarget", "possessionPct", "possession"}
        ):
            raise EspnParseError(
                "Summary matchsheet team must contain a recognized core statistic"
            )
        extra = unknown_fields(
            block, ("team", "homeAway", "statistics", "displayOrder")
        )
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
                total_shots=values.get("shots", values.get("totalShots")),
                shots_on_target=values.get("shotsOnTarget"),
                possession_pct=values.get("possessionPct", values.get("possession")),
                fouls_committed=values.get("foulsCommitted", values.get("fouls")),
                yellow_cards=values.get("yellowCards"),
                red_cards=values.get("redCards"),
                offsides=values.get("offsides"),
                corner_kicks=values.get("wonCorners", values.get("cornerKicks")),
                saves=values.get("saves"),
                statistics_json=canonical_json(statistics),
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
    if game_info[-1]:
        extras["gameInfo"] = game_info[-1]
    return SummaryParseResult(
        event_id=event.event_id,
        lineup=lineup,
        matchsheet=matchsheet,
        lineup_state=lineup_state,
        matchsheet_state=matchsheet_state,
        parser_version=PARSER_VERSION,
        extra_json=canonical_json(extras),
    )
