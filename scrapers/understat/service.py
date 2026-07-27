"""Scope-level native Understat extraction service."""

from __future__ import annotations

from datetime import date
import logging
from typing import Any, Mapping, Optional

import pandas as pd

from .catalog import (
    LEAGUE_BY_CANONICAL,
    UnderstatCatalog,
    UnderstatScope,
    current_source_season_id,
    season_slug as make_season_slug,
)
from .client import UnderstatClient
from .client import UnderstatHTTPError
from .parsers import (
    parse_match_payload,
    parse_player_season_stats,
    parse_schedule,
    parse_team_match_stats,
    parse_team_payload,
    validate_league_payload,
    validate_match_payload,
    validate_team_payload,
)


logger = logging.getLogger(__name__)


def _concat(frames: list[pd.DataFrame], empty: pd.DataFrame) -> pd.DataFrame:
    populated = [frame for frame in frames if not frame.empty]
    if not populated:
        return empty.copy()
    return pd.concat(populated, ignore_index=True).convert_dtypes()


def _match_payload_has_rows(payload: Mapping[str, Any]) -> bool:
    """Whether an HTTP-200 match envelope contains usable source records."""

    for block_name in ("shots", "rosters"):
        block = payload.get(block_name)
        if not isinstance(block, Mapping):
            continue
        for side in ("h", "a"):
            records = block.get(side)
            if isinstance(records, list) and records:
                return True
            if isinstance(records, Mapping) and records:
                return True
    return False


class UnderstatSource:
    """Fetch and parse exactly one canonical league-season scope at a time."""

    def __init__(self, client: UnderstatClient, *, today: Optional[date] = None):
        self.client = client
        self.today = today or date.today()
        self.catalog = UnderstatCatalog(client, today=self.today)

    def scrape_scope(
        self,
        league: str,
        season_slug: str,
        source_season_id: int,
        *,
        mode: str = "current",
        force_refresh: bool = False,
    ) -> dict[str, pd.DataFrame]:
        if mode not in {"current", "history", "reparse"}:
            raise ValueError("mode must be current, history, or reparse")
        definition = LEAGUE_BY_CANONICAL.get(league)
        if definition is None:
            raise ValueError(f"Unsupported Understat league: {league!r}")
        expected_slug = make_season_slug(source_season_id)
        if season_slug != expected_slug:
            raise ValueError(
                f"season/source mismatch: {season_slug!r} != {expected_slug!r} "
                f"for {source_season_id}"
            )

        scope = UnderstatScope(
            league=league,
            source_league=definition.source_league,
            source_league_id=definition.source_league_id,
            season=season_slug,
            source_season_id=source_season_id,
            is_closed=source_season_id < current_source_season_id(self.today),
            discovered=True,
        )
        refresh_scope = force_refresh or mode in {"current", "reparse"}
        try:
            league_payload = self.client.get_league_data(
                definition.source_league,
                source_season_id,
                force_refresh=refresh_scope,
            )
        except UnderstatHTTPError as exc:
            if exc.status_code != 404:
                raise
            logger.info(
                "Understat scope is not published: league=%s source_season_id=%s",
                league,
                source_season_id,
            )
            league_payload = {"dates": [], "players": [], "teams": {}}
        if not isinstance(league_payload, Mapping):
            raise TypeError("getLeagueData payload must be an object")
        validate_league_payload(league_payload)

        schedule = parse_schedule(league_payload, scope)
        players = parse_player_season_stats(league_payload, scope)
        team_match = parse_team_match_stats(league_payload, scope)

        empty_shots, empty_player_match = parse_match_payload({}, scope, {})
        shots: list[pd.DataFrame] = []
        player_matches: list[pd.DataFrame] = []
        result_mask = schedule["is_result"].fillna(False).astype(bool)
        schedule.loc[result_mask, "has_data"] = False
        played_rows: list[dict[str, Any]] = []
        for row in schedule[result_mask].to_dict(orient="records"):
            try:
                match_payload = self.client.get_match_data(
                    int(row["game_id"]),
                    # Current-season cache entries are never trusted forever:
                    # a transient empty/partial response must heal on the next run.
                    force_refresh=force_refresh or mode in {"current", "reparse"},
                )
            except UnderstatHTTPError as exc:
                if exc.status_code == 404:
                    logger.warning("Understat match %s has no payload", row["game_id"])
                    continue
                raise
            if not match_payload:
                continue
            validate_match_payload(match_payload)
            schedule.loc[
                schedule["game_id"] == row["game_id"], "has_data"
            ] = _match_payload_has_rows(match_payload)
            played_rows.append(row)
            shot_frame, player_frame = parse_match_payload(match_payload, scope, row)
            shots.append(shot_frame)
            player_matches.append(player_frame)

        empty_player_team, empty_breakdowns = parse_team_payload(
            {}, scope, team_id=0, team_name=""
        )
        player_team_frames: list[pd.DataFrame] = []
        breakdown_frames: list[pd.DataFrame] = []
        # A schedule-only future scope deliberately makes no per-team calls.
        if played_rows:
            teams = sorted(
                (
                    (int(team["id"]), str(team["title"]))
                    for team in _team_records(league_payload.get("teams"))
                    if team.get("id") not in (None, "") and team.get("title")
                ),
                key=lambda item: item[0],
            )
            for team_id, team_name in teams:
                team_payload = self.client.get_team_data(
                    team_name,
                    source_season_id,
                    force_refresh=refresh_scope or mode == "reparse",
                )
                validate_team_payload(team_payload)
                player_team, breakdowns = parse_team_payload(
                    team_payload,
                    scope,
                    team_id=team_id,
                    team_name=team_name,
                )
                player_team_frames.append(player_team)
                breakdown_frames.append(breakdowns)

        return {
            "understat_schedule": schedule,
            "understat_shots": _concat(shots, empty_shots),
            "understat_players": players,
            "understat_team_match_stats": team_match,
            "understat_player_match_stats": _concat(
                player_matches, empty_player_match
            ),
            "understat_player_team_season_stats": _concat(
                player_team_frames, empty_player_team
            ),
            "understat_team_season_breakdowns": _concat(
                breakdown_frames, empty_breakdowns
            ),
        }


def _team_records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        values = value.values()
    elif isinstance(value, list):
        values = value
    else:
        values = ()
    return [item for item in values if isinstance(item, Mapping)]


__all__ = ["UnderstatSource"]
