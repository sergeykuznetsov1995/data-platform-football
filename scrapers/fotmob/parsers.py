"""Shape-driven, network-free parsers for FotMob JSON payloads."""

from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

from .catalog import CatalogShapeError, validate_selected_season
from .domain import (
    LeaderboardCategoryRef,
    ParseIssue,
    ScopeRef,
    SeasonBundle,
)


_MISSING = object()


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None and value is not _MISSING:
            return value
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _source_id(value: Any) -> Any:
    """Normalize numeric FotMob ids without inventing ids for other values."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _scope_columns(scope: Optional[ScopeRef]) -> dict[str, Any]:
    if scope is None:
        return {}
    return {
        "competition_id": scope.competition_id,
        "source_season_key": scope.source_season_key,
        "scope_stage_id": scope.stage_id,
    }


def inventory_json_paths(payload: Any) -> Tuple[str, ...]:
    """Return every observed JSON shape path, collapsing array indices to ``[]``.

    Container paths are included as well as leaves, including empty objects and
    arrays.  Scanning every array element means a late optional field cannot be
    hidden merely because it was absent from the first item.
    """

    paths: set[str] = set()

    def walk(value: Any, path: str) -> None:
        paths.add(path)
        if isinstance(value, Mapping):
            for key, child in value.items():
                walk(child, f"{path}.{key}")
        elif isinstance(value, list):
            child_path = f"{path}[]"
            paths.add(child_path)
            for child in value:
                walk(child, child_path)

    walk(payload, "$")
    return tuple(sorted(paths))


def _score_pair(value: Any) -> tuple[Optional[int], Optional[int]]:
    if not isinstance(value, str) or "-" not in value:
        return (None, None)
    left, right = value.split("-", 1)
    try:
        return (int(left.strip()), int(right.strip()))
    except ValueError:
        return (None, None)


def _match_row(
    match: Mapping[str, Any],
    scope: ScopeRef,
    *,
    source_path: str,
    inherited_stage: Optional[str] = None,
) -> dict[str, Any]:
    status = _mapping(match.get("status"))
    home = _mapping(match.get("home"))
    away = _mapping(match.get("away"))
    reason = _mapping(status.get("reason"))
    score_text = status.get("scoreStr")
    parsed_home, parsed_away = _score_pair(score_text)
    match_id = _source_id(_first_not_none(
        match.get("id"), match.get("matchId"), match.get("matchID")
    ))
    return {
        **_scope_columns(scope),
        "match_id": match_id,
        "utc_time": _first_not_none(status.get("utcTime"), match.get("matchDate")),
        "timezone": status.get("timezone"),
        "finished": _first_not_none(status.get("finished"), match.get("finished")),
        "started": _first_not_none(status.get("started"), match.get("started")),
        "cancelled": _first_not_none(status.get("cancelled"), match.get("cancelled")),
        "postponed": match.get("postponed"),
        "awarded": _first_not_none(status.get("awarded"), match.get("awarded")),
        "score_text": score_text,
        "status_reason_short": reason.get("short"),
        "status_reason_short_key": reason.get("shortKey"),
        "status_reason_long": reason.get("long"),
        "status_reason_long_key": reason.get("longKey"),
        "home_team_id": _source_id(_first_not_none(
            home.get("id"), match.get("homeTeamId"), match.get("homeTeamID")
        )),
        "home_team_name": _first_not_none(home.get("name"), match.get("homeTeam")),
        "home_team_short_name": _first_not_none(
            home.get("shortName"), match.get("homeTeamShortName")
        ),
        "away_team_id": _source_id(_first_not_none(
            away.get("id"), match.get("awayTeamId"), match.get("awayTeamID")
        )),
        "away_team_name": _first_not_none(away.get("name"), match.get("awayTeam")),
        "away_team_short_name": _first_not_none(
            away.get("shortName"), match.get("awayTeamShortName")
        ),
        "home_score": _first_not_none(home.get("score"), match.get("homeScore"), parsed_home),
        "away_score": _first_not_none(away.get("score"), match.get("awayScore"), parsed_away),
        "round_id": _first_not_none(match.get("round"), inherited_stage),
        "round_name": match.get("roundName"),
        "stage_id": _first_not_none(match.get("stage"), inherited_stage),
        "group_name": match.get("group"),
        "page_url": match.get("pageUrl"),
        "source_path": source_path,
    }


def _merge_rows(existing: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(existing)
    for key, value in incoming.items():
        if out.get(key) is None and value is not None:
            out[key] = value
    return out


def _parse_matches(
    payload: Mapping[str, Any], scope: ScopeRef
) -> tuple[Tuple[dict[str, Any], ...], list[ParseIssue]]:
    issues: list[ParseIssue] = []
    fixture_block = _mapping(payload.get("fixtures"))
    fixture_path = "$.fixtures"
    if not fixture_block:
        fixture_block = _mapping(payload.get("matches"))
        fixture_path = "$.matches"
    if not fixture_block:
        overview = _mapping(payload.get("overview"))
        raw_overview_matches = overview.get("leagueOverviewMatches", overview.get("matches"))
        if isinstance(raw_overview_matches, list):
            fixture_block = {"allMatches": raw_overview_matches}
            fixture_path = "$.overview.leagueOverviewMatches"

    raw_matches = fixture_block.get("allMatches")
    if not isinstance(raw_matches, list):
        raw_matches = _mapping(fixture_block.get("data")).get("allMatches")
    raw_matches = _list(raw_matches)

    rows: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    for index, value in enumerate(raw_matches):
        path = f"{fixture_path}.allMatches[{index}]"
        if not isinstance(value, Mapping):
            issues.append(ParseIssue("invalid_match", path, "match entry is not an object"))
            continue
        row = _match_row(value, scope, source_path=path)
        match_id = row["match_id"]
        if match_id is None:
            issues.append(ParseIssue("match_without_id", path, "match has no id/matchId"))
            continue
        if match_id in rows:
            issues.append(ParseIssue("duplicate_match_id", path, f"duplicate match id {match_id}"))
            rows[match_id] = _merge_rows(rows[match_id], row)
        else:
            rows[match_id] = row

    playoff = _mapping(payload.get("playoff"))
    for round_path, round_obj, round_kind in _iter_playoff_rounds(playoff):
        inherited_stage = _first_not_none(round_obj.get("stage"), round_kind)
        matchups = _list(round_obj.get("matchups"))
        if not matchups and isinstance(round_obj.get("matches"), list):
            matchups = [round_obj]
        for matchup_index, matchup_value in enumerate(matchups):
            matchup = _mapping(matchup_value)
            for match_index, match_value in enumerate(_list(matchup.get("matches"))):
                if not isinstance(match_value, Mapping):
                    continue
                path = f"$.playoff.{round_path}.matchups[{matchup_index}].matches[{match_index}]"
                row = _match_row(
                    match_value,
                    scope,
                    source_path=path,
                    inherited_stage=_first_not_none(matchup.get("stage"), inherited_stage),
                )
                match_id = row["match_id"]
                if match_id is None:
                    issues.append(ParseIssue("playoff_match_without_id", path, "match has no id"))
                    continue
                rows[match_id] = _merge_rows(rows[match_id], row) if match_id in rows else row
    return tuple(rows.values()), issues


def _legend_for_team(
    team: Mapping[str, Any], legend: Sequence[Any]
) -> tuple[Any, Optional[str], Optional[str]]:
    raw = team.get("qualColor")
    color: Optional[str]
    title: Optional[str]
    key: Optional[str]
    if isinstance(raw, Mapping):
        color = _first_not_none(raw.get("color"), raw.get("value"))
        title = _first_not_none(raw.get("name"), raw.get("title"))
        key = _first_not_none(raw.get("tKey"), raw.get("key"))
    else:
        color = raw
        title = None
        key = None
    position = _first_not_none(team.get("idx"), team.get("position"))
    for entry_value in legend:
        entry = _mapping(entry_value)
        indices = entry.get("indices") or []
        if (color is not None and entry.get("color") == color) or position in indices:
            title = title or entry.get("title")
            key = key or entry.get("tKey")
            color = color or entry.get("color")
            break
    return color, title, key


def _iter_table_contexts(payload: Mapping[str, Any]) -> Iterable[tuple[Mapping[str, Any], str]]:
    table_root = payload.get("table")
    root_path = "$.table"
    if not table_root:
        table_root = _mapping(payload.get("overview")).get("table")
        root_path = "$.overview.table"
    if isinstance(table_root, list) and table_root and all(
        isinstance(item, Mapping)
        and "data" not in item
        and "table" not in item
        and ("id" in item or "teamId" in item)
        for item in table_root
    ):
        details = _mapping(payload.get("details"))
        yield {
            "leagueId": details.get("id"),
            "leagueName": details.get("name"),
            "table": {"all": table_root},
        }, root_path
        return
    blocks = table_root if isinstance(table_root, list) else ([table_root] if isinstance(table_root, Mapping) else [])
    for block_index, block_value in enumerate(blocks):
        block = _mapping(block_value)
        data = _mapping(block.get("data")) or block
        composite = data.get("tables")
        if isinstance(composite, list) and composite:
            for table_index, context_value in enumerate(composite):
                if isinstance(context_value, Mapping):
                    yield context_value, f"{root_path}[{block_index}].data.tables[{table_index}]"
        elif data:
            yield data, f"{root_path}[{block_index}].data"


def _parse_standings(
    payload: Mapping[str, Any], scope: ScopeRef
) -> tuple[Tuple[dict[str, Any], ...], tuple[dict[str, Any], ...], list[ParseIssue]]:
    rows: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
    table_stages: list[dict[str, Any]] = []
    issues: list[ParseIssue] = []
    for context, path in _iter_table_contexts(payload):
        table_id = _source_id(context.get("leagueId"))
        table_name = _first_not_none(context.get("leagueName"), context.get("name"), context.get("group"))
        page_url = context.get("pageUrl")
        legend = _list(context.get("legend"))
        table = context.get("table")
        if isinstance(table, list):
            table_types: Mapping[str, Any] = {"all": table}
        else:
            table_types = _mapping(table)
        stable_component = table_id if table_id is not None else (table_name or path)
        table_stages.append({
            **_scope_columns(scope),
            "stage_id": f"table:{stable_component}",
            "source_stage_id": table_id,
            "stage_name": table_name,
            "stage_type": "table",
            "source_order": len(table_stages),
            "page_url": page_url,
            "legend": tuple(dict(x) for x in legend if isinstance(x, Mapping)),
            "source_path": path,
        })
        for table_type, team_values in table_types.items():
            if not isinstance(team_values, list):
                continue
            for index, team_value in enumerate(team_values):
                team_path = f"{path}.table.{table_type}[{index}]"
                if not isinstance(team_value, Mapping):
                    issues.append(ParseIssue("invalid_standing", team_path, "row is not an object"))
                    continue
                team = team_value
                team_id = _source_id(team.get("id", team.get("teamId")))
                position = _first_not_none(team.get("idx"), team.get("position"))
                goals_for, goals_against = _score_pair(team.get("scoresStr"))
                qualification_color, qualification_title, qualification_key = _legend_for_team(team, legend)
                row = {
                    **_scope_columns(scope),
                    "table_id": table_id,
                    "table_name": table_name,
                    "table_type": str(table_type),
                    "team_id": team_id,
                    "team_name": _first_not_none(team.get("name"), team.get("teamName")),
                    "team_short_name": team.get("shortName"),
                    "team_page_url": team.get("pageUrl"),
                    "position": position,
                    "played": team.get("played"),
                    "wins": team.get("wins"),
                    "draws": team.get("draws"),
                    "losses": team.get("losses"),
                    "goals_for": _first_not_none(team.get("goalsFor"), goals_for),
                    "goals_against": _first_not_none(team.get("goalsAgainst"), goals_against),
                    "goal_difference": _first_not_none(team.get("goalConDiff"), team.get("goalDifference")),
                    "points": team["pts"] if "pts" in team else team.get("points"),
                    "deduction": team.get("deduction"),
                    "ongoing": team.get("ongoing"),
                    "form": team.get("form"),
                    "qualification_color": qualification_color,
                    "qualification_title": qualification_title,
                    "qualification_key": qualification_key,
                    "source_path": team_path,
                }
                key = (table_id, table_name, str(table_type), team_id, position)
                if key in rows:
                    issues.append(ParseIssue("duplicate_standing", team_path, f"duplicate standing key {key!r}"))
                    rows[key] = _merge_rows(rows[key], row)
                else:
                    rows[key] = row
    return tuple(rows.values()), tuple(table_stages), issues


def _iter_playoff_rounds(
    playoff: Mapping[str, Any],
) -> Iterable[tuple[str, Mapping[str, Any], str]]:
    for index, value in enumerate(_list(playoff.get("rounds"))):
        if isinstance(value, Mapping):
            yield f"rounds[{index}]", value, "round"
    for index, value in enumerate(_list(playoff.get("special"))):
        if isinstance(value, Mapping):
            yield f"special[{index}]", value, "special"
    bronze = playoff.get("bronzeFinal")
    if isinstance(bronze, Mapping):
        yield "bronzeFinal", bronze, "bronze_final"


def _parse_playoffs(
    payload: Mapping[str, Any], scope: ScopeRef
) -> tuple[Tuple[dict[str, Any], ...], tuple[dict[str, Any], ...], list[ParseIssue]]:
    playoff = _mapping(payload.get("playoff"))
    rows: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
    stages: list[dict[str, Any]] = []
    issues: list[ParseIssue] = []
    for round_path, round_obj, round_kind in _iter_playoff_rounds(playoff):
        source_stage = _first_not_none(round_obj.get("stage"), round_kind)
        stage_id = f"playoff:{source_stage}"
        stages.append({
            **_scope_columns(scope),
            "stage_id": stage_id,
            "source_stage_id": source_stage,
            "stage_name": round_obj.get("name"),
            "stage_type": "playoff",
            "participant_count": round_obj.get("participantCount"),
            "source_order": len(stages),
            "source_path": f"$.playoff.{round_path}",
        })
        matchups = _list(round_obj.get("matchups"))
        # Some one-off round objects are themselves a matchup.
        if not matchups and any(key in round_obj for key in ("homeTeamId", "awayTeamId", "matches")):
            matchups = [round_obj]
        for index, matchup_value in enumerate(matchups):
            path = f"$.playoff.{round_path}.matchups[{index}]"
            if not isinstance(matchup_value, Mapping):
                issues.append(ParseIssue("invalid_playoff_matchup", path, "matchup is not an object"))
                continue
            matchup = matchup_value
            aggregate = _mapping(matchup.get("aggregatedResult"))
            matches = [item for item in _list(matchup.get("matches")) if isinstance(item, Mapping)]
            match_ids = tuple(
                value
                for value in (
                    _source_id(_first_not_none(
                        item.get("matchId"), item.get("matchID"), item.get("id")
                    ))
                    for item in matches
                )
                if value is not None
            )
            row = {
                **_scope_columns(scope),
                "stage_id": stage_id,
                "source_stage_id": _first_not_none(matchup.get("stage"), source_stage),
                "round_kind": round_kind,
                "draw_order": matchup.get("drawOrder"),
                "participant_count": round_obj.get("participantCount"),
                "best_of": matchup.get("bestOf"),
                "home_team_id": _source_id(matchup.get("homeTeamId")),
                "home_team_name": matchup.get("homeTeam"),
                "home_team_short_name": matchup.get("homeTeamShortName"),
                "away_team_id": _source_id(matchup.get("awayTeamId")),
                "away_team_name": matchup.get("awayTeam"),
                "away_team_short_name": matchup.get("awayTeamShortName"),
                "home_score": matchup.get("homeScore"),
                "away_score": matchup.get("awayScore"),
                "winner_team_id": _source_id(matchup.get("winner")),
                "aggregate_home_score": aggregate.get("homeScore"),
                "aggregate_away_score": aggregate.get("awayScore"),
                "aggregate_winner_team_id": _source_id(matchup.get("aggregatedWinner")),
                "aggregate_loser_team_id": _source_id(matchup.get("aggregatedLoser")),
                "tbd_home": matchup.get("tbdTeam1"),
                "tbd_away": matchup.get("tbdTeam2"),
                "match_ids": match_ids,
                "source_path": path,
            }
            key = (
                stage_id,
                matchup.get("drawOrder"),
                row["home_team_id"],
                row["away_team_id"],
                match_ids,
            )
            if key in rows:
                # ``special`` and ``bronzeFinal`` can expose the same matchup
                # in legacy and current shapes.  Merge them as source aliases.
                rows[key] = _merge_rows(rows[key], row)
            else:
                rows[key] = row
    return tuple(rows.values()), tuple(stages), issues


def _fixture_info(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    fixtures = _mapping(payload.get("fixtures"))
    return _mapping(_first_not_none(
        fixtures.get("fixtureInfo"),
        _mapping(fixtures.get("data")).get("fixtureInfo"),
    ))


def _parse_fixture_stages(payload: Mapping[str, Any], scope: ScopeRef) -> tuple[dict[str, Any], ...]:
    info = _fixture_info(payload)
    active = _mapping(info.get("activeRound"))
    rows: list[dict[str, Any]] = []
    for index, value in enumerate(_list(info.get("rounds"))):
        if not isinstance(value, Mapping):
            continue
        source_id = _first_not_none(value.get("roundId"), value.get("id"), index)
        rows.append({
            **_scope_columns(scope),
            "stage_id": f"fixture:{source_id}",
            "source_stage_id": source_id,
            "stage_name": _first_not_none(value.get("name"), value.get("localizedKey")),
            "localized_key": value.get("localizedKey"),
            "stage_type": "fixture_round",
            "is_active": str(active.get("roundId")) == str(source_id) if active else False,
            "source_order": index,
            "source_path": f"$.fixtures.fixtureInfo.rounds[{index}]",
        })
    return tuple(rows)


def _parse_stat_stages(payload: Mapping[str, Any], scope: ScopeRef) -> tuple[dict[str, Any], ...]:
    stats = _mapping(payload.get("stats"))
    rows: list[dict[str, Any]] = []
    for index, value in enumerate(_list(stats.get("seasonStatLinks"))):
        if not isinstance(value, Mapping):
            continue
        source_id = _first_not_none(value.get("StageId"), value.get("stageId"), 0)
        tournament_id = _first_not_none(value.get("TournamentId"), value.get("tournamentId"))
        rows.append({
            **_scope_columns(scope),
            "stage_id": f"stats:{source_id}:{tournament_id}",
            "source_stage_id": source_id,
            "stage_name": value.get("Name"),
            "stage_type": "stats",
            "template_id": _source_id(value.get("TemplateId")),
            "tournament_id": _source_id(tournament_id),
            "relative_path": value.get("RelativePath"),
            "source_order": index,
            "source_path": f"$.stats.seasonStatLinks[{index}]",
        })
    return tuple(rows)


def _category_refs(payload: Mapping[str, Any], participant_type: str) -> Tuple[LeaderboardCategoryRef, ...]:
    stats = _mapping(payload.get("stats"))
    values = _list(stats.get(participant_type))
    result: list[LeaderboardCategoryRef] = []
    for index, value in enumerate(values):
        if not isinstance(value, Mapping):
            continue
        order = value.get("order")
        result.append(LeaderboardCategoryRef(
            participant_type="player" if participant_type == "players" else "team",
            name=value.get("name"),
            header=value.get("header"),
            category=value.get("category"),
            fetch_all_url=value.get("fetchAllUrl"),
            localized_title_id=value.get("localizedTitleId"),
            source_order=order if isinstance(order, int) else index,
            preview_count=len(_list(value.get("topThree"))),
        ))
    return tuple(result)


def _team_candidate(
    team_id: Any,
    name: Any,
    short_name: Any,
    path: str,
) -> Optional[tuple[Any, dict[str, Any]]]:
    normalized_id = _source_id(team_id)
    if normalized_id is None:
        return None
    return normalized_id, {
        "team_id": normalized_id,
        "team_name": name,
        "team_short_name": short_name,
        "source_paths": (path,),
    }


def _team_universe(
    payload: Mapping[str, Any],
    scope: ScopeRef,
    matches: Sequence[Mapping[str, Any]],
    standings: Sequence[Mapping[str, Any]],
    playoffs: Sequence[Mapping[str, Any]],
) -> tuple[Tuple[dict[str, Any], ...], list[ParseIssue]]:
    teams: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    issues: list[ParseIssue] = []

    def add(candidate: Optional[tuple[Any, dict[str, Any]]]) -> None:
        if candidate is None:
            return
        team_id, row = candidate
        if team_id not in teams:
            teams[team_id] = {**_scope_columns(scope), **row}
            return
        existing = teams[team_id]
        for field in ("team_name",):
            if existing.get(field) and row.get(field) and existing[field] != row[field]:
                issues.append(ParseIssue(
                    "team_metadata_conflict",
                    row["source_paths"][0],
                    f"team {team_id} has conflicting {field}: {existing[field]!r} vs {row[field]!r}",
                ))
        merged = _merge_rows(existing, row)
        for field in ("team_name", "team_short_name"):
            variants = tuple(dict.fromkeys(
                value for value in (existing.get(field), row.get(field)) if value
            ))
            if len(variants) > 1:
                merged[f"{field}_variants"] = variants
        merged["source_paths"] = tuple(dict.fromkeys(existing["source_paths"] + row["source_paths"]))
        teams[team_id] = merged

    info = _fixture_info(payload)
    for index, value in enumerate(_list(info.get("teams"))):
        team = _mapping(value)
        add(_team_candidate(team.get("id"), team.get("name"), team.get("shortName"), f"$.fixtures.fixtureInfo.teams[{index}]"))
    for row in matches:
        add(_team_candidate(row.get("home_team_id"), row.get("home_team_name"), row.get("home_team_short_name"), str(row.get("source_path"))))
        add(_team_candidate(row.get("away_team_id"), row.get("away_team_name"), row.get("away_team_short_name"), str(row.get("source_path"))))
    for row in standings:
        add(_team_candidate(row.get("team_id"), row.get("team_name"), row.get("team_short_name"), str(row.get("source_path"))))
    for row in playoffs:
        add(_team_candidate(row.get("home_team_id"), row.get("home_team_name"), row.get("home_team_short_name"), str(row.get("source_path"))))
        add(_team_candidate(row.get("away_team_id"), row.get("away_team_name"), row.get("away_team_short_name"), str(row.get("source_path"))))
    return tuple(teams.values()), issues


def _dedupe_stages(stages: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    result: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for row_value in stages:
        row = dict(row_value)
        stage_id = str(row["stage_id"])
        result[stage_id] = _merge_rows(result[stage_id], row) if stage_id in result else row
    return tuple(result.values())


def parse_season_bundle(
    payload: Mapping[str, Any],
    scope: Optional[ScopeRef] = None,
    *,
    strict_selected_season: bool = True,
) -> SeasonBundle:
    """Parse a league response once into all season-level entity shapes."""

    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a mapping")
    details = _mapping(payload.get("details"))
    actual_id = _source_id(details.get("id"))
    selected_value = _first_not_none(
        details.get("selectedSeason"),
        _mapping(payload.get("overview")).get("selectedSeason"),
        _mapping(payload.get("overview")).get("season"),
    )
    if scope is None:
        if not isinstance(actual_id, int) or selected_value is None:
            raise CatalogShapeError(
                "scope is required when details.id/details.selectedSeason are missing"
            )
        scope = ScopeRef(actual_id, str(selected_value))
    if strict_selected_season:
        validate_selected_season(
            payload,
            scope.source_season_key,
            competition_id=scope.competition_id,
        )
    elif isinstance(actual_id, int) and actual_id != scope.competition_id:
        raise CatalogShapeError(
            f"requested competition {scope.competition_id}, response identifies {actual_id}"
        )

    matches, match_issues = _parse_matches(payload, scope)
    standings, table_stages, standing_issues = _parse_standings(payload, scope)
    playoffs, playoff_stages, playoff_issues = _parse_playoffs(payload, scope)
    fixture_stages = _parse_fixture_stages(payload, scope)
    stat_stages = _parse_stat_stages(payload, scope)
    stages = _dedupe_stages((*fixture_stages, *table_stages, *playoff_stages, *stat_stages))
    teams, team_issues = _team_universe(payload, scope, matches, standings, playoffs)
    player_categories = _category_refs(payload, "players")
    team_categories = _category_refs(payload, "teams")

    tabs = tuple(str(value) for value in _list(payload.get("tabs")))
    capabilities = {
        "advertised_tabs": tabs,
        "fixtures_advertised": "fixtures" in tabs,
        "table_advertised": "table" in tabs,
        "playoff_advertised": "playoff" in tabs,
        "stats_advertised": "stats" in tabs,
        "has_fixtures_section": isinstance(payload.get("fixtures"), Mapping),
        "has_table_section": isinstance(payload.get("table"), (Mapping, list)),
        "has_playoff_section": isinstance(payload.get("playoff"), Mapping),
        "has_stats_section": isinstance(payload.get("stats"), Mapping),
        "match_count": len(matches),
        "standing_count": len(standings),
        "playoff_matchup_count": len(playoffs),
        "team_count": len(teams),
        "player_category_count": len(player_categories),
        "team_category_count": len(team_categories),
    }
    issues = tuple(
        [*match_issues, *standing_issues, *playoff_issues, *team_issues]
    )
    return SeasonBundle(
        scope=scope,
        details=dict(details),
        capabilities=capabilities,
        matches=matches,
        standings=standings,
        stages=stages,
        playoffs=playoffs,
        teams=teams,
        player_categories=player_categories,
        team_categories=team_categories,
        json_paths=inventory_json_paths(payload),
        issues=issues,
    )


def _descriptor_value(
    descriptor: LeaderboardCategoryRef | Mapping[str, Any] | None,
    dataclass_name: str,
    mapping_name: str,
) -> Any:
    if isinstance(descriptor, LeaderboardCategoryRef):
        return getattr(descriptor, dataclass_name)
    if isinstance(descriptor, Mapping):
        return descriptor.get(mapping_name)
    return None


def parse_leaderboards(
    payload: Mapping[str, Any],
    *,
    participant_type: str,
    descriptor: LeaderboardCategoryRef | Mapping[str, Any] | None = None,
    scope: Optional[ScopeRef] = None,
) -> Tuple[dict[str, Any], ...]:
    """Parse every ``TopLists`` block, not just the first preview/list."""

    if participant_type not in {"player", "team"}:
        raise ValueError("participant_type must be 'player' or 'team'")
    top_lists = payload.get("TopLists")
    if not isinstance(top_lists, list):
        top_lists = _mapping(payload.get("data")).get("TopLists")
    rows: list[dict[str, Any]] = []
    for list_index, top_value in enumerate(_list(top_lists)):
        if not isinstance(top_value, Mapping):
            continue
        top = top_value
        stat_name = _first_not_none(
            top.get("StatName"),
            top.get("statName"),
            _descriptor_value(descriptor, "name", "name"),
        )
        header = _first_not_none(
            top.get("Title"),
            top.get("title"),
            _descriptor_value(descriptor, "header", "header"),
        )
        category = _first_not_none(
            top.get("Category"),
            top.get("category"),
            _descriptor_value(descriptor, "category", "category"),
        )
        stat_list = _first_not_none(top.get("StatList"), top.get("statList"), [])
        for item_index, item_value in enumerate(_list(stat_list)):
            if not isinstance(item_value, Mapping):
                continue
            item = item_value
            nested_stat = _mapping(item.get("stat"))
            participant_name = _first_not_none(
                item.get("ParticipantName"), item.get("participantName"), item.get("name")
            )
            team_name = _first_not_none(
                item.get("TeamName"), item.get("teamName"),
                participant_name if participant_type == "team" else None,
            )
            participant_id = _source_id(_first_not_none(
                item.get("ParticiantId"),  # FotMob's historic misspelling
                item.get("ParticipantId"),
                item.get("participantId"),
                item.get("id") if participant_type == "player" else None,
            ))
            team_id = _source_id(_first_not_none(
                item.get("TeamId"), item.get("teamId"),
                item.get("id") if participant_type == "team" else None,
            ))
            rows.append({
                **_scope_columns(scope),
                "participant_type": participant_type,
                "participant_id": participant_id,
                "participant_name": participant_name,
                "team_id": team_id,
                "team_name": team_name,
                "team_color": _first_not_none(item.get("TeamColor"), item.get("teamColor")),
                "country_code": _first_not_none(
                    item.get("ParticipantCountryCode"), item.get("ccode")
                ),
                "rank": _first_not_none(item.get("Rank"), item.get("rank")),
                "stat_value": _first_not_none(
                    item.get("StatValue"), item.get("value"), nested_stat.get("value")
                ),
                "sub_stat_value": item.get("SubStatValue"),
                "stat_value_count": item.get("StatValueCount"),
                "matches_played": _first_not_none(item.get("MatchesPlayed"), item.get("matchesPlayed")),
                "minutes_played": _first_not_none(item.get("MinutesPlayed"), item.get("minutesPlayed")),
                "stat_name": _first_not_none(stat_name, nested_stat.get("name")),
                "stat_category_header": header,
                "stat_category_group": category,
                "top_list_index": list_index,
                "source_item_index": item_index,
            })
    return tuple(rows)


def _club_fields(value: Any, prefix: str, transfer: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    if isinstance(value, Mapping):
        return (
            _first_not_none(value.get("name"), value.get("shortName")),
            _first_not_none(value.get("fullName"), value.get("name")),
            _source_id(value.get("id")),
        )
    return (
        value,
        transfer.get(f"{prefix}ClubFullName"),
        _source_id(transfer.get(f"{prefix}ClubId")),
    )


def _transfer_event_id(row: Mapping[str, Any]) -> str:
    identity = [
        row.get("player_id"),
        row.get("transfer_date"),
        row.get("from_club_id"),
        row.get("from_club"),
        row.get("to_club_id"),
        row.get("to_club"),
        row.get("transfer_type_key"),
        row.get("fee_value"),
    ]
    encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def parse_transfers(
    payloads: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    *,
    scope: Optional[ScopeRef] = None,
) -> Tuple[dict[str, Any], ...]:
    """Flatten and deduplicate one or more paginated transfer responses."""

    pages = [payloads] if isinstance(payloads, Mapping) else list(payloads)
    rows: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for page_index, payload in enumerate(pages):
        if not isinstance(payload, Mapping):
            raise TypeError("each transfer payload must be a mapping")
        transfer_values = payload.get("transfers")
        if not isinstance(transfer_values, list):
            transfer_values = _mapping(payload.get("data")).get("transfers")
        source_page = _first_not_none(payload.get("page"), page_index)
        for item_index, value in enumerate(_list(transfer_values)):
            if not isinstance(value, Mapping):
                continue
            transfer = value
            position = _mapping(transfer.get("position"))
            transfer_type_value = transfer.get("transferType")
            transfer_type = _mapping(transfer_type_value)
            fee_value_raw = transfer.get("fee")
            fee = _mapping(fee_value_raw)
            localized_fee_text = _first_not_none(
                transfer.get("localizedFeeText"), fee.get("localizedFeeText")
            )
            fee_text = _first_not_none(
                transfer.get("feeText"),
                fee.get("feeText"),
                localized_fee_text,
                fee.get("text"),
                fee.get("fallback"),
                fee_value_raw if isinstance(fee_value_raw, str) else None,
            )
            numeric_fee = _first_not_none(
                transfer.get("value"), fee.get("value"), transfer.get("amountEuroEstimated")
            )
            market_value_raw = transfer.get("marketValue")
            market_value = (
                _mapping(market_value_raw).get("value")
                if isinstance(market_value_raw, Mapping)
                else market_value_raw
            )
            from_name, from_full, from_id = _club_fields(
                transfer.get("fromClub"), "from", transfer
            )
            to_name, to_full, to_id = _club_fields(
                transfer.get("toClub"), "to", transfer
            )
            row = {
                **_scope_columns(scope),
                "player_id": _source_id(transfer.get("playerId", transfer.get("id"))),
                "player_name": _first_not_none(transfer.get("name"), transfer.get("playerName")),
                "position_label": _first_not_none(
                    position.get("label"), transfer.get("position") if isinstance(transfer.get("position"), str) else None
                ),
                "position_key": position.get("key"),
                "transfer_date": _first_not_none(transfer.get("transferDate"), transfer.get("date")),
                "from_club": from_name,
                "from_club_full_name": from_full,
                "from_club_id": from_id,
                "to_club": to_name,
                "to_club_full_name": to_full,
                "to_club_id": to_id,
                "fee_text": fee_text,
                "localized_fee_text": localized_fee_text,
                "fee_value": numeric_fee,
                "market_value": market_value,
                "on_loan": transfer.get("onLoan"),
                "transfer_type_key": transfer_type.get("localizationKey"),
                "transfer_type_text": _first_not_none(
                    transfer_type.get("text"),
                    transfer_type_value if isinstance(transfer_type_value, str) else None,
                ),
                "source_page": source_page,
                "source_item_index": item_index,
            }
            event_id = _transfer_event_id(row)
            row["transfer_event_id"] = event_id
            rows[event_id] = _merge_rows(rows[event_id], row) if event_id in rows else row
    return tuple(rows.values())


__all__ = [
    "inventory_json_paths",
    "parse_leaderboards",
    "parse_season_bundle",
    "parse_transfers",
]
