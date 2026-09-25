"""One-pass offline ESPN Summary normalization."""

from __future__ import annotations

import math
import re
from types import MappingProxyType
from typing import Any, Mapping

from .models import Competition, Edition
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
)
from .parser_contracts import (
    EntityParseState,
    LINEUP_ANOMALY_CLASSES,
    LINEUP_STAT_MAP_VERSION,
    LineupRow,
    MATCHSHEET_STAT_MAP_VERSION,
    MatchEventRow,
    MatchsheetRow,
    PARSER_VERSION,
    ScheduleRow,
    SummaryDisposition,
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


class _SourceMalformed(EspnParseError):
    """A Summary shape this parser cannot read without guessing.

    It stops the current match only: ``parse_summary`` turns it (and any
    ``EspnParseError`` of the shared helpers) into ``SOURCE_MALFORMED``.
    """


class _Anomalies:
    """Lineup anomaly classes collected while the rows are still written."""

    def __init__(self) -> None:
        self.classes: set[str] = set()

    def add(self, name: str) -> None:
        if name not in LINEUP_ANOMALY_CLASSES:
            raise ValueError(f"unknown lineup anomaly class {name!r}")
        self.classes.add(name)


# Summary blocks ESPN sends that are neither parsed nor kept (grill decision 3).
_DROPPED_BLOCKS = (
    "news",
    "article",
    "videos",
    "odds",
    "pickcenter",
    "lastFiveGames",
    "seasonseries",
    "standings",
    "broadcasts",
    "leaders",
)


def _validate_context(
    competition: Competition, edition: Edition, event: ScheduleRow
) -> None:
    # The context only names the match and its season; a mismatch is a caller
    # bug, not a source shape.
    if not isinstance(competition, Competition) or not isinstance(edition, Edition):
        raise TypeError("competition and edition must be registry models")
    if not isinstance(event, ScheduleRow):
        raise TypeError("event must be a normalized ScheduleRow")
    if (
        event.competition_id != competition.espn_id
        or event.source_season_year != edition.source_season_year
        or event.scope_id != competition.scope_id(edition)
    ):
        raise ValueError("Summary parser context does not match schedule scope")


def _count(value: Any, field: str) -> int | None:
    """Non-negative whole number from an ESPN int, integral float or digits."""

    if isinstance(value, Mapping):
        value = value.get("value", value.get("displayValue"))
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    if isinstance(value, str):
        value = value.strip()
    return optional_nonnegative_int(value, field)


def _linescores(competitor: Mapping[str, Any], field: str) -> tuple[int | None, ...]:
    if competitor.get("linescores") is None:
        return None, None, None
    periods = [
        _count(item, f"{field}.linescores[{index}]")
        for index, item in enumerate(
            required_list(competitor["linescores"], f"{field}.linescores")
        )
    ]
    first = periods[0] if periods else None
    second = periods[1] if len(periods) > 1 else None
    extra = [value for value in periods[2:] if value is not None]
    return first, second, sum(extra) if extra else None


def _header_sides(
    payload: Mapping[str, Any], event: ScheduleRow
) -> tuple[
    dict[int, tuple[str, str]], dict[int, dict[str, Any]], int | None, dict[str, Any]
]:
    header = required_mapping(payload.get("header"), "summary.header")
    header_id = native_id(header.get("id"), "summary.header.id")
    if header_id != event.event_id:
        raise _SourceMalformed("summary.header.id does not match schedule event_id")
    competitions = required_list(
        header.get("competitions"), "summary.header.competitions"
    )
    if len(competitions) != 1:
        raise _SourceMalformed("summary.header must have exactly one competition")
    header_competition = required_mapping(
        competitions[0], "summary.header.competitions[0]"
    )
    competitors = required_list(
        header_competition.get("competitors"),
        "summary.header.competitions[0].competitors",
    )
    if len(competitors) != 2:
        raise _SourceMalformed("Summary header must have exactly two competitors")
    by_team: dict[int, tuple[str, str]] = {}
    by_side: dict[str, int] = {}
    side_facts: dict[int, dict[str, Any]] = {}
    nested_extras: dict[str, Any] = {}
    for index, raw_competitor in enumerate(competitors):
        field = f"summary.header.competitors[{index}]"
        competitor = required_mapping(raw_competitor, field)
        home_away = required_string(competitor.get("homeAway"), f"{field}.homeAway")
        if home_away not in {"home", "away"} or home_away in by_side:
            raise _SourceMalformed(
                "Summary header must have unique home and away sides"
            )
        team = required_mapping(competitor.get("team"), f"{field}.team")
        team_id = native_id(team.get("id"), f"{field}.team.id")
        team_name = required_string(
            team.get("displayName"), f"{field}.team.displayName"
        )
        if team_id in by_team:
            raise _SourceMalformed("Summary header team IDs must be distinct")
        by_team[team_id] = (home_away, team_name)
        by_side[home_away] = team_id
        score_h1, score_h2, score_et = _linescores(competitor, field)
        side_facts[team_id] = {
            "score_h1": score_h1,
            "score_h2": score_h2,
            "score_et": score_et,
            "shootout_score": _count(
                competitor.get("shootoutScore"), f"{field}.shootoutScore"
            ),
            "aggregate_score": _count(
                competitor.get("aggregateScore"), f"{field}.aggregateScore"
            ),
            "advance": optional_bool(competitor.get("advance"), f"{field}.advance"),
        }
        competitor_extra = unknown_fields(
            competitor,
            (
                "homeAway",
                "team",
                "score",
                "linescores",
                "shootoutScore",
                "aggregateScore",
                "advance",
            ),
        )
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
        raise _SourceMalformed("Summary header teams/homeAway do not match schedule")
    leg = _count(header_competition.get("leg"), "summary.header.competitions[0].leg")
    competition_extra = unknown_fields(
        header_competition, ("date", "competitors", "id", "status", "venue", "leg")
    )
    if competition_extra:
        nested_extras["competition"] = competition_extra
    return by_team, side_facts, leg, nested_extras


def _team_block(
    raw: Any, field: str, by_team: Mapping[int, tuple[str, str]]
) -> tuple[int, str, str, Mapping[str, Any]]:
    block = required_mapping(raw, field)
    team = required_mapping(block.get("team"), f"{field}.team")
    team_id = native_id(team.get("id"), f"{field}.team.id")
    if team_id not in by_team:
        raise _SourceMalformed(f"{field}.team.id is not a Summary header team")
    side, header_name = by_team[team_id]
    if "homeAway" in block:
        block_side = required_string(block["homeAway"], f"{field}.homeAway")
        if block_side != side:
            raise _SourceMalformed(f"{field}.homeAway conflicts with native team ID")
    team_name = optional_string(team.get("displayName"), f"{field}.team.displayName")
    if team_name is not None and team_name != header_name:
        # Display strings are not identity; retain the section-local value.
        header_name = team_name
    return team_id, side, header_name, block


def _team_blocks(
    value: Any, field: str, by_team: Mapping[int, tuple[str, str]]
) -> dict[int, tuple[str, str, Mapping[str, Any]]]:
    """Section blocks by native team ID; a repeated team is unreadable."""

    blocks: dict[int, tuple[str, str, Mapping[str, Any]]] = {}
    for index, raw in enumerate(required_list(value, field)):
        team_id, side, team_name, block = _team_block(raw, f"{field}[{index}]", by_team)
        if team_id in blocks:
            raise _SourceMalformed(f"{field} contain a duplicate team ID")
        blocks[team_id] = (side, team_name, block)
    return blocks


def _substitution_flag(value: Any, field: str) -> bool | None:
    if value is None or type(value) is bool:
        return value
    detail = required_mapping(value, field)
    if "didSub" not in detail:
        raise _SourceMalformed(f"{field}.didSub is required for substitution objects")
    return optional_bool(detail["didSub"], f"{field}.didSub")


def _substitution_minute(value: Any, field: str) -> int | None:
    if not isinstance(value, Mapping) or "clock" not in value:
        return None
    clock = required_mapping(value["clock"], f"{field}.clock")
    display = required_string(clock.get("displayValue"), f"{field}.clock.displayValue")
    parts = re.findall(r"\d{1,3}", display)
    return sum(int(part) for part in parts) if parts else None


def _small_sided_size(payload: Mapping[str, Any]) -> int | None:
    if "format" not in payload:
        return None
    match_format = required_mapping(payload["format"], "summary.format")
    configured_size = match_format.get("startersPerTeam")
    if configured_size is None and "regulation" in match_format:
        regulation = required_mapping(
            match_format["regulation"], "summary.format.regulation"
        )
        configured_size = regulation.get("startersPerTeam")
    if configured_size is None:
        return None
    if type(configured_size) is not int or not 1 <= configured_size <= 7:
        raise _SourceMalformed(
            "summary.format.startersPerTeam must be an integer from 1 to 7"
        )
    return configured_size


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
    payload: Mapping[str, Any],
) -> tuple[int | None, str | None, int | None, str | None, dict[str, Any]]:
    """Venue, attendance and the referee; no gameInfo -> all NULL."""

    if "gameInfo" not in payload or payload["gameInfo"] is None:
        return None, None, None, None, {}
    info = required_mapping(payload["gameInfo"], "summary.gameInfo")
    venue_id: int | None = None
    venue_name: str | None = None
    venue_extra: dict[str, Any] = {}
    if "venue" in info and info["venue"] is not None:
        # The Summary venue wins over the schedule one: it is the later fetch.
        venue = required_mapping(info["venue"], "summary.gameInfo.venue")
        if "id" in venue and venue["id"] is not None:
            venue_id = native_id(venue["id"], "summary.gameInfo.venue.id")
        venue_name = optional_string(
            venue.get("fullName"), "summary.gameInfo.venue.fullName"
        )
        venue_extra = unknown_fields(venue, ("id", "fullName"))
    # ESPN answers 0 when it does not know the attendance.
    attendance = (
        optional_nonnegative_int(info.get("attendance"), "summary.gameInfo.attendance")
        or None
    )
    officials = [
        required_mapping(raw, f"summary.gameInfo.officials[{index}]")
        for index, raw in enumerate(
            required_list(info.get("officials", []), "summary.gameInfo.officials")
        )
    ]
    labelled: list[int] = []
    for index, official in enumerate(officials):
        if official.get("position") is None:
            # ESPN commonly emits fourth/reserve officials without a role.
            continue
        position = required_mapping(
            official["position"], f"summary.gameInfo.officials[{index}].position"
        )
        label = position.get("name", position.get("displayName"))
        if isinstance(label, str) and label.strip().upper() in {
            "REFEREE",
            "MATCH REFEREE",
        }:
            labelled.append(index)
    # The referee is the official ordered first (eng.1 2010 lists the same
    # referee three times, order 1-3), else the first one labelled Referee.
    referee_index = next(
        (index for index, row in enumerate(officials) if row.get("order") == 1),
        labelled[0] if labelled else None,
    )
    referee_name: str | None = None
    official_extras: list[dict[str, Any]] = []
    for index, official in enumerate(officials):
        if index != referee_index:
            official_extras.append(dict(official))
            continue
        field = f"summary.gameInfo.officials[{index}]"
        referee_name = optional_string(
            official.get("fullName", official.get("displayName")), f"{field}.fullName"
        )
        extra = unknown_fields(
            official, ("fullName", "displayName", "order", "position")
        )
        if isinstance(official.get("position"), Mapping):
            position_extra = unknown_fields(
                official["position"], ("name", "displayName")
            )
            if position_extra:
                extra["position"] = position_extra
        official_extras.append(extra)
    extra = unknown_fields(info, ("venue", "attendance", "officials"))
    if venue_extra:
        extra["venue"] = venue_extra
    if any(official_extras):
        extra["officials"] = official_extras
    return venue_id, venue_name, attendance, referee_name, extra


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
                raise _SourceMalformed(f"{item_field}.value must be numeric")
            if isinstance(value, (int, float)):
                normalized = float(value)
            elif (
                isinstance(value, str)
                and _NUMERIC_DISPLAY_RE.fullmatch(value.strip()) is not None
                and not value.strip().endswith("%")
            ):
                normalized = float(value.strip())
            else:
                raise _SourceMalformed(f"{item_field}.value must be numeric")
            if not math.isfinite(normalized):
                raise _SourceMalformed(f"{item_field}.value must be finite")
            existing = values.get(target)
            if existing is not None and existing != normalized:
                raise _SourceMalformed(
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
    blocks: Mapping[int, tuple[str, str, Mapping[str, Any]]],
    by_team: Mapping[int, tuple[str, str]],
    anomalies: _Anomalies,
) -> tuple[tuple[LineupRow, ...], EntityParseState]:
    rosters = {
        team_id: required_list(block["roster"], f"summary.rosters[{team_id}].roster")
        for team_id, (_, _, block) in blocks.items()
        if block.get("roster") is not None
    }
    # Only an answer without a single player is honestly empty (C6-F1).
    if not any(rosters.values()):
        return (), EntityParseState.VALID_EMPTY
    if any(not rosters.get(team_id) for team_id in by_team):
        anomalies.add("one_sided_roster")

    rows: list[LineupRow] = []
    per_team_rows: dict[int, list[LineupRow]] = {}
    seen: set[tuple[int, int, int]] = set()
    for team_id, roster in rosters.items():
        side, team_name, block = blocks[team_id]
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
                anomalies.add("duplicate_player")
                continue
            seen.add(key)
            # ESPN puts the shirt number on the roster entry (C6-F5).
            jersey_raw = player.get("jersey", athlete.get("jersey"))
            if jersey_raw is None:
                jersey = None
            elif type(jersey_raw) is int and jersey_raw >= 0:
                jersey = str(jersey_raw)
            else:
                jersey = optional_string(jersey_raw, f"{field}.jersey")
            starter = optional_bool(player.get("starter"), f"{field}.starter")
            subbed_in = _substitution_flag(player.get("subbedIn"), f"{field}.subbedIn")
            subbed_out = _substitution_flag(
                player.get("subbedOut"), f"{field}.subbedOut"
            )
            if (starter is True and subbed_in is True) or (
                starter is False and subbed_out is True and subbed_in is not True
            ):
                anomalies.add("contradictory_flags")
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
                    "jersey",
                    "starter",
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
        if block.get("formation") is not None:
            starters = [row for row in team_rows if row.starter is True]
            missing = sum(row.formation_place is None for row in starters)
            # No formationPlace at all (2005-2016 bodies) is the source norm.
            if 0 < missing < len(starters):
                anomalies.add("formation_place_missing")

    # Starter counts are checked only when ESPN sets starter flags at all.
    if any(row.starter is not None for row in rows):
        expected = _small_sided_size(payload) or 11
        if any(
            sum(row.starter is True for row in team_rows) != expected
            for team_rows in per_team_rows.values()
        ):
            anomalies.add("starters_not_11")
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
        raise _SourceMalformed(f"{field}.value must be a supported scalar value")
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise _SourceMalformed(f"{field}.value must be finite")
        return str(value)
    if isinstance(value, str):
        display = value.strip()
        if not display or _NUMERIC_DISPLAY_RE.fullmatch(display) is None:
            raise _SourceMalformed(f"{field}.value must be a numeric display scalar")
        return display
    raise _SourceMalformed(f"{field}.value must be a supported scalar value")


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
            raise _SourceMalformed(f"{field} maps duplicate statistic {target!r}")
        values[target] = scalar
    return values


def _matchsheet(
    payload: Mapping[str, Any],
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
    by_team: Mapping[int, tuple[str, str]],
    rosters: Mapping[int, tuple[str, str, Mapping[str, Any]]],
    side_facts: Mapping[int, Mapping[str, Any]],
    leg: int | None,
    game_info: tuple[int | None, str | None, int | None, str | None, dict[str, Any]],
    anomalies: _Anomalies,
) -> tuple[tuple[MatchsheetRow, ...], EntityParseState]:
    boxscore = required_mapping(payload.get("boxscore", {}), "summary.boxscore")
    blocks = (
        _team_blocks(boxscore["teams"], "summary.boxscore.teams", by_team)
        if "teams" in boxscore
        else {}
    )
    statistics_by_team = {
        team_id: required_list(
            block["statistics"], f"summary.boxscore.teams[{team_id}].statistics"
        )
        for team_id, (_, _, block) in blocks.items()
        if block.get("statistics") is not None
    }
    # Honestly empty only when neither team carries a statistic (C6-F1).
    if not any(statistics_by_team.values()):
        return (), EntityParseState.VALID_EMPTY
    if any(not statistics_by_team.get(team_id) for team_id in by_team):
        anomalies.add("one_sided_statistics")

    venue_id, venue_name, attendance, referee_name, _ = game_info
    rows: list[MatchsheetRow] = []
    for team_id, statistics in statistics_by_team.items():
        if not statistics:
            continue
        side, team_name, block = blocks[team_id]
        values = _stat_values(
            statistics, f"summary.boxscore.teams[{team_id}].statistics"
        )
        if not set(values).intersection(
            {"total_shots", "shots_on_target", "possession_pct"}
        ):
            raise _SourceMalformed(
                "Summary matchsheet team must contain a recognized core statistic"
            )
        roster_block = rosters.get(team_id, (None, None, {}))[2]
        roster = roster_block.get("roster")
        facts = side_facts[team_id]
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
                # Not played -> NULL, never a literal 0:0 (C6-F6).
                score=(
                    (
                        event.home_score
                        if team_id == event.home_team_id
                        else event.away_score
                    )
                    if event.played_final
                    else None
                ),
                accurate_crosses=values.get("accurate_crosses"),
                accurate_long_balls=values.get("accurate_long_balls"),
                accurate_passes=values.get("accurate_passes"),
                blocked_shots=values.get("blocked_shots"),
                cross_pct=values.get("cross_pct"),
                effective_clearance=values.get("effective_clearance"),
                effective_tackles=values.get("effective_tackles"),
                fouls_committed=values.get("fouls_committed"),
                interceptions=values.get("interceptions"),
                longball_pct=values.get("longball_pct"),
                offsides=values.get("offsides"),
                pass_pct=values.get("pass_pct"),
                penalty_kick_goals=values.get("penalty_kick_goals"),
                penalty_kick_shots=values.get("penalty_kick_shots"),
                possession_pct=values.get("possession_pct"),
                red_cards=values.get("red_cards"),
                roster=canonical_json(roster) if roster is not None else None,
                saves=values.get("saves"),
                shot_pct=values.get("shot_pct"),
                shots_on_target=values.get("shots_on_target"),
                tackle_pct=values.get("tackle_pct"),
                total_clearance=values.get("total_clearance"),
                total_crosses=values.get("total_crosses"),
                total_long_balls=values.get("total_long_balls"),
                total_passes=values.get("total_passes"),
                total_shots=values.get("total_shots"),
                total_tackles=values.get("total_tackles"),
                won_corners=values.get("won_corners"),
                yellow_cards=values.get("yellow_cards"),
                statistics_json=canonical_json(statistics),
                stat_map_version=MATCHSHEET_STAT_MAP_VERSION,
                venue_id=venue_id,
                venue=venue_name,
                attendance=attendance,
                referee=referee_name,
                formation=optional_string(
                    roster_block.get("formation"),
                    f"summary.rosters[{team_id}].formation",
                ),
                score_h1=facts["score_h1"],
                score_h2=facts["score_h2"],
                score_et=facts["score_et"],
                shootout_score=facts["shootout_score"],
                aggregate_score=facts["aggregate_score"],
                advance=facts["advance"],
                leg=leg,
                league=event.league,
                season=event.season,
                game=event.game,
                parser_version=PARSER_VERSION,
                extra_json=canonical_json(
                    unknown_fields(
                        block, ("team", "homeAway", "statistics", "displayOrder")
                    )
                ),
            )
        )
    return (
        tuple(sorted(rows, key=lambda row: row.home_away != "home")),
        EntityParseState.CAPTURED,
    )


_EVENT_KEYS = (
    "id",
    "type",
    "text",
    "period",
    "clock",
    "team",
    "participants",
    "scoringPlay",
    "redCard",
    "yellowCard",
    "penaltyKick",
    "ownGoal",
    "homeScore",
    "awayScore",
    "fieldPositionX",
    "fieldPositionY",
)


def _optional_number(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _SourceMalformed(f"{field} must be a number")
    return float(value)


def _optional_text(value: Any, field: str) -> str | None:
    if value is None or value == "":
        return None
    return required_string(value, field)


def _event_row(event_id: int, kind: str, raw: Any, field: str) -> MatchEventRow:
    record = required_mapping(raw, field)
    extra: dict[str, Any] = {}
    if kind == "commentary":
        play = required_mapping(record.get("play", {}), f"{field}.play")
        clock = record.get("time", play.get("clock"))
        extra.update(unknown_fields(record, ("sequence", "time", "text", "play")))
        field = f"{field}.play"
    else:
        play = record
        clock = record.get("clock")
    play_extra = unknown_fields(play, _EVENT_KEYS)
    clock = required_mapping(clock or {}, f"{field}.clock")
    event_type = required_mapping(play.get("type") or {}, f"{field}.type")
    if unknown_fields(event_type, ("id", "text")):
        play_extra["type"] = unknown_fields(event_type, ("id", "text"))
    team = required_mapping(play.get("team") or {}, f"{field}.team")
    team_id = native_id(team["id"], f"{field}.team.id") if "id" in team else None
    if team and team_id is None:
        # Commentary plays name the team without its ID.
        play_extra["team"] = dict(team)
    participants = [
        required_mapping(
            required_mapping(item, f"{field}.participants[{index}]").get("athlete"),
            f"{field}.participants[{index}].athlete",
        )
        for index, item in enumerate(
            required_list(play.get("participants", []), f"{field}.participants")
        )
    ]
    athlete_ids = [
        native_id(athlete["id"], f"{field}.participants.athlete.id")
        for athlete in participants
        if "id" in athlete
    ]
    if len(athlete_ids) != len(participants):
        play_extra["participants"] = play["participants"]
    if kind == "commentary" and play_extra:
        extra["play"] = play_extra
    elif play_extra:
        extra.update(play_extra)
    return MatchEventRow(
        event_id=event_id,
        kind=kind,
        play_id=_optional_text(play.get("id"), f"{field}.id"),
        sequence=optional_nonnegative_int(record.get("sequence"), f"{field}.sequence"),
        period=optional_nonnegative_int(
            required_mapping(play.get("period") or {}, f"{field}.period").get("number"),
            f"{field}.period.number",
        ),
        clock_value=_optional_number(clock.get("value"), f"{field}.clock.value"),
        clock_display=_optional_text(
            clock.get("displayValue"), f"{field}.clock.displayValue"
        ),
        team_id=team_id,
        athlete_ids=canonical_json(athlete_ids),
        type_id=_optional_text(event_type.get("id"), f"{field}.type.id"),
        type_text=_optional_text(event_type.get("text"), f"{field}.type.text"),
        text=_optional_text(record.get("text"), f"{field}.text"),
        x=_optional_number(play.get("fieldPositionX"), f"{field}.fieldPositionX"),
        y=_optional_number(play.get("fieldPositionY"), f"{field}.fieldPositionY"),
        scoring_play=optional_bool(play.get("scoringPlay"), f"{field}.scoringPlay"),
        red_card=optional_bool(play.get("redCard"), f"{field}.redCard"),
        yellow_card=optional_bool(play.get("yellowCard"), f"{field}.yellowCard"),
        penalty_kick=optional_bool(play.get("penaltyKick"), f"{field}.penaltyKick"),
        own_goal=optional_bool(play.get("ownGoal"), f"{field}.ownGoal"),
        home_score=optional_nonnegative_int(
            play.get("homeScore"), f"{field}.homeScore"
        ),
        away_score=optional_nonnegative_int(
            play.get("awayScore"), f"{field}.awayScore"
        ),
        extra_json=canonical_json(extra),
    )


def _events(payload: Mapping[str, Any], event_id: int) -> tuple[MatchEventRow, ...]:
    return tuple(
        _event_row(event_id, kind, raw, f"summary.{key}[{index}]")
        for key, kind in (("keyEvents", "key_event"), ("commentary", "commentary"))
        for index, raw in enumerate(
            required_list(payload.get(key, []), f"summary.{key}")
        )
    )


def parse_summary(
    raw: bytes,
    *,
    competition: Competition,
    edition: Edition,
    event: ScheduleRow,
) -> SummaryParseResult:
    """Decode one Summary once; every answer gets exactly one disposition.

    No response shape raises: an unreadable one becomes ``SOURCE_MALFORMED``
    with its reason, so the neighbouring matches of the scope still publish.
    ``TypeError``/``ValueError`` remain for caller bugs (wrong argument types
    or a context from another scope).
    """
    _validate_context(competition, edition, event)
    anomalies = _Anomalies()
    try:
        payload = decode_object(raw, "Summary")
        by_team, side_facts, leg, header_nested_extra = _header_sides(payload, event)
        rosters = (
            _team_blocks(payload["rosters"], "summary.rosters", by_team)
            if "rosters" in payload
            else {}
        )
        game_info = _parse_game_info(payload)
        lineup, lineup_state = _lineup(
            payload,
            competition=competition,
            edition=edition,
            event=event,
            blocks=rosters,
            by_team=by_team,
            anomalies=anomalies,
        )
        matchsheet, matchsheet_state = _matchsheet(
            payload,
            competition=competition,
            edition=edition,
            event=event,
            by_team=by_team,
            rosters=rosters,
            side_facts=side_facts,
            leg=leg,
            game_info=game_info,
            anomalies=anomalies,
        )
        events = _events(payload, event.event_id)
        extras: dict[str, Any] = {}
        header_extra = unknown_fields(
            payload["header"], ("id", "competitions", "season", "week", "league")
        )
        if header_extra:
            extras["header"] = header_extra
        if header_nested_extra:
            extras["headerSections"] = header_nested_extra
        boxscore_extra = unknown_fields(payload.get("boxscore", {}), ("teams",))
        if boxscore_extra:
            extras["boxscore"] = boxscore_extra
        roster_extras = {
            str(team_id): unknown_fields(
                block, ("homeAway", "team", "roster", "formation")
            )
            for team_id, (_, _, block) in rosters.items()
        }
        if any(roster_extras.values()):
            extras["rosters"] = {k: v for k, v in roster_extras.items() if v}
        if game_info[-1]:
            extras["gameInfo"] = game_info[-1]
        extra_json = canonical_json(extras)
    except EspnParseError as exc:
        # The raw body stays in the raw store; nothing of it is published.
        return SummaryParseResult(
            event_id=event.event_id,
            lineup=(),
            matchsheet=(),
            lineup_state=EntityParseState.MALFORMED,
            matchsheet_state=EntityParseState.MALFORMED,
            parser_version=PARSER_VERSION,
            extra_json="{}",
            disposition=SummaryDisposition.SOURCE_MALFORMED,
            reason=str(exc),
            anomalies=(),
            events=(),
            advance_team_id=None,
        )
    if (
        lineup_state is EntityParseState.VALID_EMPTY
        and matchsheet_state is EntityParseState.VALID_EMPTY
    ):
        disposition = SummaryDisposition.VALID_EMPTY
    elif anomalies.classes:
        disposition = SummaryDisposition.LINEUP_ANOMALY
    else:
        disposition = SummaryDisposition.CAPTURED
    advancing = [team_id for team_id, facts in side_facts.items() if facts["advance"]]
    return SummaryParseResult(
        event_id=event.event_id,
        lineup=lineup,
        matchsheet=matchsheet,
        lineup_state=lineup_state,
        matchsheet_state=matchsheet_state,
        parser_version=PARSER_VERSION,
        extra_json=extra_json,
        disposition=disposition,
        reason=None,
        anomalies=tuple(sorted(anomalies.classes)),
        events=events,
        advance_team_id=advancing[0] if len(advancing) == 1 else None,
    )
