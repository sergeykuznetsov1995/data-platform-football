"""Network-free parsers for Understat league, match and team payloads."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from html import unescape
import math
import re
from typing import Any, Optional

import pandas as pd

from .catalog import UnderstatScope


SHOT_SITUATIONS = {
    "OpenPlay": "Open Play",
    "FromCorner": "From Corner",
    "SetPiece": "Set Piece",
    "DirectFreekick": "Direct Freekick",
    "Penalty": "Penalty",
}
SHOT_BODY_PARTS = {
    "RightFoot": "Right Foot",
    "LeftFoot": "Left Foot",
    "Head": "Head",
    "OtherBodyPart": "Other Body Part",
    "OtherBodyParts": "Other Body Part",
}
SHOT_RESULTS = {
    "Goal": "Goal",
    "OwnGoal": "Own Goal",
    "BlockedShot": "Blocked Shot",
    "SavedShot": "Saved Shot",
    "MissedShots": "Missed Shot",
    "ShotOnPost": "Shot On Post",
}


class UnderstatSchemaDrift(ValueError):
    """The source added a field that the production contract does not audit."""


_DATE_FIELDS = frozenset({
    "id", "isResult", "h", "a", "goals", "xG", "datetime", "forecast",
})
_TEAM_DATE_FIELDS = _DATE_FIELDS | frozenset({"side", "result"})
_DATE_REQUIRED_FIELDS = _DATE_FIELDS - frozenset({"forecast"})
_TEAM_DATE_REQUIRED_FIELDS = _DATE_REQUIRED_FIELDS
_TEAM_FIELDS = frozenset({"id", "title", "history"})
_HISTORY_FIELDS = frozenset({
    "h_a", "xG", "xGA", "npxG", "npxGA", "ppda", "ppda_allowed", "deep",
    "deep_allowed", "scored", "missed", "xpts", "result", "date", "wins",
    "draws", "loses", "pts", "npxGD",
})
_PLAYER_FIELDS = frozenset({
    "id", "player_id", "player_name", "player", "games", "time", "goals", "xG",
    "assists", "xA", "shots", "key_passes", "yellow_cards", "red_cards",
    "position", "team_title", "npg", "npxG", "xGChain", "xGBuildup",
})
_ROSTER_FIELDS = frozenset({
    "id", "goals", "own_goals", "shots", "xG", "time", "player_id", "team_id",
    "position", "player", "h_a", "yellow_card", "red_card", "roster_in",
    "roster_out", "key_passes", "assists", "xA", "xGChain", "xGBuildup",
    "positionOrder",
})
_SHOT_FIELDS = frozenset({
    "id", "minute", "result", "X", "Y", "xG", "player", "h_a", "player_id",
    "situation", "season", "shotType", "match_id", "h_team", "a_team", "h_goals",
    "a_goals", "date", "player_assisted", "lastAction",
})
TEAM_BREAKDOWN_DIMENSIONS = frozenset({
    "situation", "formation", "gameState", "timing", "shotZone", "attackSpeed",
    "result",
})
_BREAKDOWN_DIMENSIONS = TEAM_BREAKDOWN_DIMENSIONS
_BREAKDOWN_FIELDS = frozenset({"stat", "time", "shots", "goals", "xG", "against"})
_LEAGUE_TOP_LEVEL_FIELDS = frozenset({"teams", "players", "dates"})
_MATCH_TOP_LEVEL_FIELDS = frozenset({"rosters", "shots", "tmpl"})
_TEAM_TOP_LEVEL_FIELDS = frozenset({"dates", "players", "statistics"})
_SIDE_FIELDS = frozenset({"id", "title", "short_title"})
_PAIR_FIELDS = frozenset({"h", "a"})
_FORECAST_FIELDS = frozenset({"w", "d", "l"})
_PPDA_FIELDS = frozenset({"att", "def"})
_AGAINST_FIELDS = frozenset({"shots", "goals", "xG"})
_PLAYER_REQUIRED_FIELDS = _PLAYER_FIELDS - frozenset({"player_id", "player"})
_BREAKDOWN_REQUIRED_FIELDS = frozenset({"shots", "goals", "xG", "against"})


def _reject_unknown(value: Mapping[str, Any], allowed: frozenset[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise UnderstatSchemaDrift(f"{path}: unknown fields {unknown}")


def _require_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise UnderstatSchemaDrift(f"{path}: expected an object")
    return value


def _require_list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise UnderstatSchemaDrift(f"{path}: expected a list")
    return value


def _require_fields(
    value: Mapping[str, Any], required: frozenset[str], path: str
) -> None:
    missing = sorted(required - set(value))
    if missing:
        raise UnderstatSchemaDrift(f"{path}: missing required fields {missing}")


def _validate_object(
    value: Any,
    *,
    path: str,
    allowed: frozenset[str],
    required: Optional[frozenset[str]] = None,
) -> Mapping[str, Any]:
    result = _require_mapping(value, path)
    _reject_unknown(result, allowed, path)
    _require_fields(result, required if required is not None else allowed, path)
    return result


def _validate_record_container(value: Any, path: str) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        records = list(value.values())
    elif isinstance(value, list):
        records = value
    else:
        raise UnderstatSchemaDrift(f"{path}: expected an object or list of records")
    result: list[Mapping[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise UnderstatSchemaDrift(f"{path}[{index}]: expected an object")
        result.append(record)
    return result


def _validate_keyed_records_or_empty_list(
    value: Any, path: str
) -> list[Mapping[str, Any]]:
    """Accept Understat's keyed object or its unpublished ``[]`` sentinel."""

    if isinstance(value, Mapping):
        records = list(value.values())
    elif isinstance(value, list) and not value:
        records = []
    else:
        raise UnderstatSchemaDrift(f"{path}: expected an object or empty list")
    result: list[Mapping[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise UnderstatSchemaDrift(f"{path}[{index}]: expected an object")
        result.append(record)
    return result


def _validate_match_record(match: Mapping[str, Any], path: str, *, team: bool) -> None:
    fields = _TEAM_DATE_FIELDS if team else _DATE_FIELDS
    required = _TEAM_DATE_REQUIRED_FIELDS if team else _DATE_REQUIRED_FIELDS
    _reject_unknown(match, fields, path)
    _require_fields(match, required, path)
    for side in ("h", "a"):
        _validate_object(
            match[side], path=f"{path}.{side}", allowed=_SIDE_FIELDS
        )
    for field in ("goals", "xG"):
        _validate_object(
            match[field], path=f"{path}.{field}", allowed=_PAIR_FIELDS
        )
    if "forecast" in match:
        _validate_object(
            match["forecast"], path=f"{path}.forecast", allowed=_FORECAST_FIELDS
        )


def _validate_enum(
    record: Mapping[str, Any], field: str, values: Mapping[str, str], path: str
) -> None:
    value = record.get(field)
    if value is not None and str(value) not in values:
        raise UnderstatSchemaDrift(
            f"{path}.{field}: unknown non-null value {value!r}"
        )


def _mapped_enum(
    value: Any, values: Mapping[str, str], path: str
) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value)
    try:
        return values[normalized]
    except KeyError as exc:
        raise UnderstatSchemaDrift(
            f"{path}: unknown non-null value {value!r}"
        ) from exc


def validate_league_payload(payload: Mapping[str, Any]) -> None:
    payload = _validate_object(
        payload,
        path="getLeagueData",
        allowed=_LEAGUE_TOP_LEVEL_FIELDS,
    )
    dates = _require_list(payload["dates"], "getLeagueData.dates")
    expected_history_sides: dict[tuple[str, str], str] = {}
    for index, match in enumerate(dates):
        match = _require_mapping(match, f"getLeagueData.dates[{index}]")
        _validate_match_record(match, f"getLeagueData.dates[{index}]", team=False)
        is_result = _boolean(
            match["isResult"],
            f"getLeagueData.dates[{index}].isResult",
        )
        if is_result is not True:
            continue
        date_value = str(match["datetime"]).strip()
        for side in ("h", "a"):
            team_id = str(match[side]["id"]).strip()
            key = (date_value, team_id)
            previous = expected_history_sides.get(key)
            if previous is not None and previous != side:
                raise UnderstatSchemaDrift(
                    "getLeagueData.dates: conflicting side mapping for "
                    f"date/team {key!r}"
                )
            expected_history_sides[key] = side
    players = _require_list(payload["players"], "getLeagueData.players")
    for index, player in enumerate(players):
        player = _require_mapping(player, f"getLeagueData.players[{index}]")
        _reject_unknown(player, _PLAYER_FIELDS, f"getLeagueData.players[{index}]")
        _require_fields(
            player, _PLAYER_REQUIRED_FIELDS, f"getLeagueData.players[{index}]"
        )
    teams = _validate_keyed_records_or_empty_list(
        payload["teams"], "getLeagueData.teams"
    )
    for index, team in enumerate(teams):
        team = _require_mapping(team, f"getLeagueData.teams[{index}]")
        _reject_unknown(team, _TEAM_FIELDS, f"getLeagueData.teams[{index}]")
        _require_fields(team, _TEAM_FIELDS, f"getLeagueData.teams[{index}]")
        histories = _require_list(
            team["history"], f"getLeagueData.teams[{index}].history"
        )
        for h_index, history in enumerate(histories):
            history_path = f"getLeagueData.teams[{index}].history[{h_index}]"
            history = _require_mapping(
                history, history_path
            )
            _reject_unknown(
                history,
                _HISTORY_FIELDS,
                history_path,
            )
            _require_fields(
                history,
                _HISTORY_FIELDS,
                history_path,
            )
            side = str(history["h_a"])
            if side not in {"h", "a"}:
                raise UnderstatSchemaDrift(
                    f"{history_path}.h_a: expected 'h' or 'a', got "
                    f"{history['h_a']!r}"
                )
            history_key = (
                str(history["date"]).strip(),
                str(team["id"]).strip(),
            )
            expected_side = expected_history_sides.get(history_key)
            if expected_side is None:
                raise UnderstatSchemaDrift(
                    f"{history_path}: date/team {history_key!r} is absent "
                    "from getLeagueData.dates"
                )
            if side != expected_side:
                raise UnderstatSchemaDrift(
                    f"{history_path}.h_a: expected {expected_side!r} for "
                    f"date/team {history_key!r}, got {side!r}"
                )
            for field in ("ppda", "ppda_allowed"):
                _validate_object(
                    history[field],
                    path=f"{history_path}.{field}",
                    allowed=_PPDA_FIELDS,
                )


def validate_match_payload(payload: Mapping[str, Any]) -> None:
    payload = _validate_object(
        payload, path="getMatchData", allowed=_MATCH_TOP_LEVEL_FIELDS
    )
    _require_mapping(payload["tmpl"], "getMatchData.tmpl")
    for container in ("rosters", "shots"):
        sides = _validate_object(
            payload[container], path=f"getMatchData.{container}", allowed=_PAIR_FIELDS
        )
        for side in ("h", "a"):
            _validate_record_container(
                sides[side], f"getMatchData.{container}.{side}"
            )
    for side in ("h", "a"):
        rosters = _require_mapping(payload["rosters"], "getMatchData.rosters")
        shots = _require_mapping(payload["shots"], "getMatchData.shots")
        for index, player in enumerate(
            _validate_record_container(rosters[side], f"getMatchData.rosters.{side}")
        ):
            path = f"getMatchData.rosters.{side}[{index}]"
            _reject_unknown(player, _ROSTER_FIELDS, path)
            _require_fields(
                player, _ROSTER_FIELDS, path
            )
            if str(player.get("h_a")) != side:
                raise UnderstatSchemaDrift(
                    f"{path}.h_a: expected outer side {side!r}, "
                    f"got {player.get('h_a')!r}"
                )
        for index, shot in enumerate(
            _validate_record_container(shots[side], f"getMatchData.shots.{side}")
        ):
            path = f"getMatchData.shots.{side}[{index}]"
            _reject_unknown(shot, _SHOT_FIELDS, path)
            _require_fields(shot, _SHOT_FIELDS, path)
            if str(shot.get("h_a")) != side:
                raise UnderstatSchemaDrift(
                    f"{path}.h_a: expected outer side {side!r}, "
                    f"got {shot.get('h_a')!r}"
                )
            _validate_enum(shot, "situation", SHOT_SITUATIONS, path)
            _validate_enum(shot, "shotType", SHOT_BODY_PARTS, path)
            _validate_enum(shot, "result", SHOT_RESULTS, path)


def validate_team_payload(payload: Mapping[str, Any]) -> None:
    payload = _validate_object(
        payload, path="getTeamData", allowed=_TEAM_TOP_LEVEL_FIELDS
    )
    dates = _require_list(payload["dates"], "getTeamData.dates")
    for index, match in enumerate(dates):
        match = _require_mapping(match, f"getTeamData.dates[{index}]")
        _validate_match_record(match, f"getTeamData.dates[{index}]", team=True)
    players = _require_list(payload["players"], "getTeamData.players")
    for index, player in enumerate(players):
        player = _require_mapping(player, f"getTeamData.players[{index}]")
        _reject_unknown(player, _PLAYER_FIELDS, f"getTeamData.players[{index}]")
        _require_fields(player, _PLAYER_REQUIRED_FIELDS, f"getTeamData.players[{index}]")
    statistics = _require_mapping(payload["statistics"], "getTeamData.statistics")
    _reject_unknown(statistics, _BREAKDOWN_DIMENSIONS, "getTeamData.statistics")
    if statistics:
        _require_fields(
            statistics, _BREAKDOWN_DIMENSIONS, "getTeamData.statistics"
        )
    for dimension, categories in statistics.items():
        categories = _require_mapping(
            categories, f"getTeamData.statistics.{dimension}"
        )
        for category, values in categories.items():
            values = _require_mapping(
                values, f"getTeamData.statistics.{dimension}.{category}"
            )
            _reject_unknown(
                values,
                _BREAKDOWN_FIELDS,
                f"getTeamData.statistics.{dimension}.{category}",
            )
            _require_fields(
                values,
                _BREAKDOWN_REQUIRED_FIELDS,
                f"getTeamData.statistics.{dimension}.{category}",
            )
            _validate_object(
                values["against"],
                path=f"getTeamData.statistics.{dimension}.{category}.against",
                allowed=_AGAINST_FIELDS,
            )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        values: Iterable[Any] = value.values()
    elif isinstance(value, list):
        values = value
    else:
        values = ()
    return [item for item in values if isinstance(item, Mapping)]


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return unescape(str(value))


def _is_empty_scalar(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _integer(value: Any, path: str = "source integer") -> Optional[int]:
    if _is_empty_scalar(value):
        return None
    if isinstance(value, bool):
        raise UnderstatSchemaDrift(f"{path}: invalid integer value {value!r}")
    try:
        if isinstance(value, str):
            return int(value.strip())
        numeric = float(value)
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError
        return int(numeric)
    except (TypeError, ValueError, OverflowError) as exc:
        raise UnderstatSchemaDrift(
            f"{path}: invalid integer value {value!r}"
        ) from exc


def _number(value: Any, path: str = "source number") -> Optional[float]:
    if _is_empty_scalar(value):
        return None
    if isinstance(value, bool):
        raise UnderstatSchemaDrift(f"{path}: invalid numeric value {value!r}")
    try:
        result = float(value)
        if not math.isfinite(result):
            raise ValueError
        return result
    except (TypeError, ValueError, OverflowError) as exc:
        raise UnderstatSchemaDrift(
            f"{path}: invalid numeric value {value!r}"
        ) from exc


def _boolean(value: Any, path: str = "source boolean") -> Optional[bool]:
    if _is_empty_scalar(value):
        return None
    if isinstance(value, bool):
        return value
    if value in (1, "1", "true", "True"):
        return True
    if value in (0, "0", "false", "False"):
        return False
    raise UnderstatSchemaDrift(f"{path}: invalid boolean value {value!r}")


def _scope_values(scope: UnderstatScope) -> dict[str, Any]:
    return {
        # The five legacy Bronze tables already expose league_id as VARCHAR.
        # Preserve that physical contract for schema evolution and create the
        # two native-only tables with the same cross-entity type.
        "league_id": str(scope.source_league_id),
        "league": scope.league,
        "season_id": scope.source_season_id,
        "season": scope.season,
        "source_season_id": scope.source_season_id,
    }


def _timestamp(value: Any, path: str = "source timestamp") -> Any:
    if _is_empty_scalar(value):
        return pd.NaT
    if not isinstance(value, str):
        raise UnderstatSchemaDrift(
            f"{path}: invalid timestamp value {value!r}"
        )
    normalized = value.strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", normalized):
        raise UnderstatSchemaDrift(
            f"{path}: invalid timestamp value {value!r}"
        )
    try:
        result = pd.to_datetime(
            normalized,
            format="%Y-%m-%d %H:%M:%S",
            exact=True,
            errors="raise",
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise UnderstatSchemaDrift(
            f"{path}: invalid timestamp value {value!r}"
        ) from exc
    if pd.isna(result):
        raise UnderstatSchemaDrift(
            f"{path}: invalid timestamp value {value!r}"
        )
    if not 1900 <= int(result.year) <= 2198:
        raise UnderstatSchemaDrift(
            f"{path}: invalid timestamp value {value!r}"
        )
    return result


def _game_id(date_value: Any, home_team: Any, away_team: Any) -> str:
    date_ts = _timestamp(date_value)
    prefix = "" if pd.isna(date_ts) else f"{date_ts.strftime('%Y-%m-%d')} "
    return f"{prefix}{_text(home_team) or ''}-{_text(away_team) or ''}"


def _frame(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=columns).convert_dtypes()
    result = pd.DataFrame.from_records(rows)
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result.loc[:, list(dict.fromkeys((*columns, *result.columns)))].convert_dtypes()


SCHEDULE_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "game_id", "game", "date", "home_team_id", "away_team_id",
    "home_team", "away_team", "away_team_code", "home_team_code",
    "home_goals", "away_goals", "home_xg", "away_xg", "is_result",
    "has_data", "forecast_home_win", "forecast_draw", "forecast_away_win", "url",
)


def parse_schedule(payload: Mapping[str, Any], scope: UnderstatScope) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for match in _records(payload.get("dates")):
        home = _mapping(match.get("h"))
        away = _mapping(match.get("a"))
        goals = _mapping(match.get("goals"))
        xg = _mapping(match.get("xG"))
        forecast = _mapping(match.get("forecast"))
        home_team = _text(home.get("title"))
        away_team = _text(away.get("title"))
        game_id = _integer(match.get("id"))
        has_data = (
            xg.get("h") not in (None, "", "0", 0)
            or xg.get("a") not in (None, "", "0", 0)
        )
        rows.append(
            {
                **_scope_values(scope),
                "game_id": game_id,
                "game": _game_id(match.get("datetime"), home_team, away_team),
                "date": _timestamp(match.get("datetime")),
                "home_team_id": _integer(home.get("id")),
                "away_team_id": _integer(away.get("id")),
                "home_team": home_team,
                "away_team": away_team,
                "away_team_code": _text(away.get("short_title")),
                "home_team_code": _text(home.get("short_title")),
                "home_goals": _integer(goals.get("h")),
                "away_goals": _integer(goals.get("a")),
                "home_xg": _number(xg.get("h")),
                "away_xg": _number(xg.get("a")),
                "is_result": _boolean(
                    match.get("isResult"), "getLeagueData.dates[].isResult"
                ),
                "has_data": has_data,
                "forecast_home_win": _number(forecast.get("w")),
                "forecast_draw": _number(forecast.get("d")),
                "forecast_away_win": _number(forecast.get("l")),
                "url": f"https://understat.com/match/{game_id}",
            }
        )
    return _frame(rows, SCHEDULE_COLUMNS)


PLAYER_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "team", "team_id", "source_team_title", "source_team_ids", "is_multi_team",
    "player", "player_id", "position", "matches", "minutes", "goals", "xg",
    "np_goals", "np_xg", "assists", "xa", "shots", "key_passes",
    "yellow_cards", "red_cards", "xg_chain", "xg_buildup",
)


def _player_metrics(player: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "player": _text(player.get("player_name", player.get("player"))),
        "player_id": _integer(player.get("id", player.get("player_id"))),
        "position": _text(player.get("position")),
        "matches": _integer(player.get("games")),
        "minutes": _integer(player.get("time")),
        "goals": _integer(player.get("goals")),
        "xg": _number(player.get("xG")),
        "np_goals": _integer(player.get("npg")),
        "np_xg": _number(player.get("npxG")),
        "assists": _integer(player.get("assists")),
        "xa": _number(player.get("xA")),
        "shots": _integer(player.get("shots")),
        "key_passes": _integer(player.get("key_passes")),
        "yellow_cards": _integer(player.get("yellow_cards")),
        "red_cards": _integer(player.get("red_cards")),
        "xg_chain": _number(player.get("xGChain")),
        "xg_buildup": _number(player.get("xGBuildup")),
    }


def parse_player_season_stats(
    payload: Mapping[str, Any], scope: UnderstatScope
) -> pd.DataFrame:
    teams = {
        _text(team.get("title")): _integer(team.get("id"))
        for team in _records(payload.get("teams"))
    }
    rows: list[dict[str, Any]] = []
    for player in _records(payload.get("players")):
        raw_team = _text(player.get("team_title")) or ""
        team_titles = tuple(part.strip() for part in raw_team.split(",") if part.strip())
        team_ids = tuple(teams.get(title) for title in team_titles)
        is_multi_team = len(team_titles) != 1
        rows.append(
            {
                **_scope_values(scope),
                "team": team_titles[0] if not is_multi_team else None,
                "team_id": team_ids[0] if not is_multi_team else None,
                "source_team_title": raw_team,
                "source_team_ids": ",".join(
                    str(item) for item in team_ids if item is not None
                ),
                "is_multi_team": is_multi_team,
                **_player_metrics(player),
            }
        )
    return _frame(rows, PLAYER_COLUMNS)


TEAM_MATCH_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "game_id", "game", "date", "home_team_id", "away_team_id", "home_team",
    "away_team", "away_team_code", "home_team_code",
    "home_points", "home_expected_points", "home_goals", "home_xg", "home_np_xg",
    "home_np_xg_difference", "home_ppda", "home_ppda_att", "home_ppda_def",
    "home_ppda_allowed_att", "home_ppda_allowed_def", "home_deep_completions",
    "home_deep_allowed", "away_points", "away_expected_points", "away_goals",
    "away_xg", "away_np_xg", "away_np_xg_difference", "away_ppda",
    "away_ppda_att", "away_ppda_def", "away_ppda_allowed_att",
    "away_ppda_allowed_def", "away_deep_completions", "away_deep_allowed",
)


def parse_team_match_stats(
    payload: Mapping[str, Any], scope: UnderstatScope
) -> pd.DataFrame:
    schedule: dict[int, dict[str, Any]] = {}
    match_by_date_team: dict[tuple[str, int], int] = {}
    for match in _records(payload.get("dates")):
        match_id = _integer(match.get("id"))
        if match_id is None:
            continue
        is_result = _boolean(
            match.get("isResult"),
            "getLeagueData.dates[].isResult",
        )
        if is_result is not True:
            continue
        home = _mapping(match.get("h"))
        away = _mapping(match.get("a"))
        home_team = _text(home.get("title"))
        away_team = _text(away.get("title"))
        date_value = str(match.get("datetime", ""))
        schedule[match_id] = {
            **_scope_values(scope),
            "game_id": match_id,
            "game": _game_id(date_value, home_team, away_team),
            "date": _timestamp(date_value),
            "home_team_id": _integer(home.get("id")),
            "away_team_id": _integer(away.get("id")),
            "home_team": home_team,
            "away_team": away_team,
            "away_team_code": _text(away.get("short_title")),
            "home_team_code": _text(home.get("short_title")),
        }
        for team in (home, away):
            team_id = _integer(team.get("id"))
            if team_id is not None:
                match_by_date_team[(date_value, team_id)] = match_id

    result: dict[int, dict[str, Any]] = {}
    for team in _records(payload.get("teams")):
        team_id = _integer(team.get("id"))
        if team_id is None:
            continue
        for history in _records(team.get("history")):
            match_id = match_by_date_team.get((str(history.get("date", "")), team_id))
            if match_id is None:
                continue
            row = result.setdefault(match_id, dict(schedule[match_id]))
            prefix = "home" if history.get("h_a") == "h" else "away"
            ppda = _mapping(history.get("ppda"))
            ppda_allowed = _mapping(history.get("ppda_allowed"))
            ppda_att = _integer(ppda.get("att"))
            ppda_def = _integer(ppda.get("def"))
            row.update(
                {
                    f"{prefix}_points": _integer(history.get("pts")),
                    f"{prefix}_expected_points": _number(history.get("xpts")),
                    f"{prefix}_goals": _integer(history.get("scored")),
                    f"{prefix}_xg": _number(history.get("xG")),
                    f"{prefix}_np_xg": _number(history.get("npxG")),
                    f"{prefix}_np_xg_difference": _number(history.get("npxGD")),
                    f"{prefix}_ppda": (
                        ppda_att / ppda_def
                        if ppda_att is not None and ppda_def not in (None, 0)
                        else None
                    ),
                    f"{prefix}_ppda_att": ppda_att,
                    f"{prefix}_ppda_def": ppda_def,
                    f"{prefix}_ppda_allowed_att": _integer(ppda_allowed.get("att")),
                    f"{prefix}_ppda_allowed_def": _integer(ppda_allowed.get("def")),
                    f"{prefix}_deep_completions": _integer(history.get("deep")),
                    f"{prefix}_deep_allowed": _integer(history.get("deep_allowed")),
                }
            )
    return _frame(list(result.values()), TEAM_MATCH_COLUMNS)


SHOT_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "game_id", "game", "date", "shot_id", "team_id", "team", "player_id",
    "player", "assist_player_id", "assist_player", "xg", "location_x",
    "location_y", "minute", "body_part", "situation", "result", "last_action",
)
PLAYER_MATCH_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "game_id", "game", "team", "team_id", "team_side", "player", "player_id",
    "position", "position_id", "minutes", "goals", "own_goals", "shots", "xg",
    "xg_chain", "xg_buildup", "assists", "xa", "key_passes", "yellow_cards",
    "red_cards", "roster_entry_id", "roster_entry_ids", "roster_in", "roster_out",
)


def _roster_rank(record: Mapping[str, Any]) -> tuple[int, int, int, str]:
    informative = sum(value not in (None, "", "0", 0) for value in record.values())
    return (
        _integer(record.get("time")) or 0,
        informative,
        -(_integer(record.get("positionOrder")) or 999),
        str(record.get("id", "")),
    )


def _deduplicate_rosters(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, Optional[int], Optional[int]], list[Mapping[str, Any]]] = (
        defaultdict(list)
    )
    rosters = _mapping(payload.get("rosters"))
    for side in ("h", "a"):
        for player in _records(rosters.get(side)):
            team_id = _integer(player.get("team_id"))
            player_id = _integer(player.get("player_id"))
            grouped[(side, team_id, player_id)].append(player)

    result: list[dict[str, Any]] = []
    for (side, team_id, player_id), candidates in sorted(
        grouped.items(), key=lambda item: tuple(str(part) for part in item[0])
    ):
        chosen = max(candidates, key=_roster_rank)
        roster_ids = sorted(
            {str(item.get("id")) for item in candidates if item.get("id") not in (None, "")},
            key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value),
        )
        result.append(
            {
                "team_side": side,
                "team_id": team_id,
                "player_id": player_id,
                "player": _text(chosen.get("player")),
                "position": _text(chosen.get("position")),
                "position_id": _integer(chosen.get("positionOrder")),
                "minutes": _integer(chosen.get("time")),
                "goals": _integer(chosen.get("goals")),
                "own_goals": _integer(chosen.get("own_goals")),
                "shots": _integer(chosen.get("shots")),
                "xg": _number(chosen.get("xG")),
                "xg_chain": _number(chosen.get("xGChain")),
                "xg_buildup": _number(chosen.get("xGBuildup")),
                "assists": _integer(chosen.get("assists")),
                "xa": _number(chosen.get("xA")),
                "key_passes": _integer(chosen.get("key_passes")),
                "yellow_cards": _integer(chosen.get("yellow_card")),
                "red_cards": _integer(chosen.get("red_card")),
                "roster_entry_id": _integer(chosen.get("id")),
                "roster_entry_ids": ",".join(roster_ids),
                "roster_in": _integer(chosen.get("roster_in")),
                "roster_out": _integer(chosen.get("roster_out")),
            }
        )
    return result


def parse_match_payload(
    payload: Mapping[str, Any],
    scope: UnderstatScope,
    schedule_row: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one HTTP payload once into both match-derived tables."""

    game_id = _integer(schedule_row.get("game_id"))
    game = _text(schedule_row.get("game"))
    team_names = {
        "h": _text(schedule_row.get("home_team")),
        "a": _text(schedule_row.get("away_team")),
    }
    team_ids = {
        "h": _integer(schedule_row.get("home_team_id")),
        "a": _integer(schedule_row.get("away_team_id")),
    }
    roster_rows = _deduplicate_rosters(payload)

    players_by_side_name: dict[tuple[str, str], set[int]] = defaultdict(set)
    for roster in roster_rows:
        if roster["player"] and roster["player_id"] is not None:
            players_by_side_name[(roster["team_side"], roster["player"])].add(
                roster["player_id"]
            )

    player_match_rows = [
        {
            **_scope_values(scope),
            "game_id": game_id,
            "game": game,
            "team": team_names.get(roster["team_side"]),
            **roster,
        }
        for roster in roster_rows
    ]

    shot_rows: list[dict[str, Any]] = []
    shots = _mapping(payload.get("shots"))
    for side in ("h", "a"):
        for shot in _records(shots.get(side)):
            shot_side = str(shot.get("h_a") or side)
            assister = _text(shot.get("player_assisted"))
            assister_ids = players_by_side_name.get((shot_side, assister or ""), set())
            shot_rows.append(
                {
                    **_scope_values(scope),
                    "game_id": game_id,
                    "game": game,
                    "date": _timestamp(shot.get("date")),
                    "shot_id": _integer(shot.get("id")),
                    "team_id": team_ids.get(shot_side),
                    "team": team_names.get(shot_side),
                    "player_id": _integer(shot.get("player_id")),
                    "player": _text(shot.get("player")),
                    "assist_player_id": next(iter(assister_ids)) if len(assister_ids) == 1 else None,
                    "assist_player": assister,
                    "xg": _number(shot.get("xG")),
                    "location_x": _number(shot.get("X")),
                    "location_y": _number(shot.get("Y")),
                    "minute": _integer(shot.get("minute")),
                    "body_part": _mapped_enum(
                        shot.get("shotType"), SHOT_BODY_PARTS, "shot.shotType"
                    ),
                    "situation": _mapped_enum(
                        shot.get("situation"), SHOT_SITUATIONS, "shot.situation"
                    ),
                    "result": _mapped_enum(
                        shot.get("result"), SHOT_RESULTS, "shot.result"
                    ),
                    "last_action": _text(shot.get("lastAction")),
                }
            )
    return _frame(shot_rows, SHOT_COLUMNS), _frame(player_match_rows, PLAYER_MATCH_COLUMNS)


PLAYER_TEAM_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "team", "team_id", "player", "player_id", "position", "matches", "minutes",
    "goals", "xg", "np_goals", "np_xg", "assists", "xa", "shots", "key_passes",
    "yellow_cards", "red_cards", "xg_chain", "xg_buildup",
)
BREAKDOWN_COLUMNS = (
    "league_id", "league", "season_id", "season", "source_season_id",
    "team", "team_id", "dimension", "category", "source_stat", "minutes",
    "shots", "goals", "xg", "against_shots", "against_goals", "against_xg",
)


def parse_team_payload(
    payload: Mapping[str, Any],
    scope: UnderstatScope,
    *,
    team_id: int,
    team_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    player_rows = [
        {
            **_scope_values(scope),
            "team": team_name,
            "team_id": team_id,
            **_player_metrics(player),
        }
        for player in _records(payload.get("players"))
    ]

    breakdown_rows: list[dict[str, Any]] = []
    for dimension, categories in _mapping(payload.get("statistics")).items():
        for category, values in _mapping(categories).items():
            values = _mapping(values)
            against = _mapping(values.get("against"))
            breakdown_rows.append(
                {
                    **_scope_values(scope),
                    "team": team_name,
                    "team_id": team_id,
                    "dimension": str(dimension),
                    "category": str(category),
                    "source_stat": _text(values.get("stat")),
                    "minutes": _integer(values.get("time")),
                    "shots": _integer(values.get("shots")),
                    "goals": _integer(values.get("goals")),
                    "xg": _number(values.get("xG")),
                    "against_shots": _integer(against.get("shots")),
                    "against_goals": _integer(against.get("goals")),
                    "against_xg": _number(against.get("xG")),
                }
            )
    return _frame(player_rows, PLAYER_TEAM_COLUMNS), _frame(
        breakdown_rows, BREAKDOWN_COLUMNS
    )


__all__ = [
    "TEAM_BREAKDOWN_DIMENSIONS",
    "UnderstatSchemaDrift",
    "parse_match_payload",
    "parse_player_season_stats",
    "parse_schedule",
    "parse_team_match_stats",
    "parse_team_payload",
    "validate_league_payload",
    "validate_match_payload",
    "validate_team_payload",
]
