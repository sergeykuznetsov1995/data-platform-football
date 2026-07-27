"""Declarative Bronze contract for the native Understat ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class UnderstatTableContract:
    """One source-faithful table produced for a league-season scope."""

    table_name: str
    result_key: str
    reader_method: str
    natural_key: tuple[str, ...]
    required_columns: tuple[str, ...]


_SCOPE = ("league_id", "league", "season_id", "season", "source_season_id")

TABLE_CONTRACTS: tuple[UnderstatTableContract, ...] = (
    UnderstatTableContract(
        table_name="understat_schedule",
        result_key="schedule",
        reader_method="read_schedule",
        natural_key=("league", "season", "game_id"),
        required_columns=_SCOPE
        + (
            "game_id",
            "game",
            "date",
            "home_team_id",
            "away_team_id",
            "home_team",
            "away_team",
            "away_team_code",
            "home_team_code",
            "home_goals",
            "away_goals",
            "home_xg",
            "away_xg",
            "is_result",
            "has_data",
            "forecast_home_win",
            "forecast_draw",
            "forecast_away_win",
            "url",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_shots",
        result_key="shots",
        reader_method="read_shot_events",
        natural_key=("shot_id",),
        required_columns=_SCOPE
        + (
            "game_id",
            "game",
            "date",
            "shot_id",
            "team_id",
            "team",
            "player_id",
            "player",
            "assist_player_id",
            "assist_player",
            "xg",
            "location_x",
            "location_y",
            "minute",
            "body_part",
            "situation",
            "result",
            "last_action",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_players",
        result_key="player_stats",
        reader_method="read_player_season_stats",
        natural_key=("league", "season", "player_id"),
        required_columns=_SCOPE
        + (
            "team",
            "team_id",
            "source_team_title",
            "source_team_ids",
            "is_multi_team",
            "player",
            "player_id",
            "position",
            "matches",
            "minutes",
            "goals",
            "xg",
            "np_goals",
            "np_xg",
            "assists",
            "xa",
            "shots",
            "key_passes",
            "yellow_cards",
            "red_cards",
            "xg_chain",
            "xg_buildup",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_team_match_stats",
        result_key="team_match_stats",
        reader_method="read_team_match_stats",
        natural_key=("league", "season", "game_id"),
        required_columns=_SCOPE
        + (
            "game_id",
            "game",
            "date",
            "home_team_id",
            "away_team_id",
            "home_team",
            "away_team",
            "away_team_code",
            "home_team_code",
            "home_points",
            "home_expected_points",
            "home_goals",
            "home_xg",
            "home_np_xg",
            "home_np_xg_difference",
            "home_ppda",
            "home_ppda_att",
            "home_ppda_def",
            "home_ppda_allowed_att",
            "home_ppda_allowed_def",
            "home_deep_completions",
            "home_deep_allowed",
            "away_points",
            "away_expected_points",
            "away_goals",
            "away_xg",
            "away_np_xg",
            "away_np_xg_difference",
            "away_ppda",
            "away_ppda_att",
            "away_ppda_def",
            "away_ppda_allowed_att",
            "away_ppda_allowed_def",
            "away_deep_completions",
            "away_deep_allowed",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_player_match_stats",
        result_key="player_match_stats",
        reader_method="read_player_match_stats",
        natural_key=("league", "season", "game_id", "team_id", "player_id"),
        required_columns=_SCOPE
        + (
            "game_id",
            "game",
            "team",
            "team_id",
            "team_side",
            "player",
            "player_id",
            "position",
            "position_id",
            "minutes",
            "goals",
            "own_goals",
            "shots",
            "xg",
            "xg_chain",
            "xg_buildup",
            "assists",
            "xa",
            "key_passes",
            "yellow_cards",
            "red_cards",
            "roster_entry_id",
            "roster_entry_ids",
            "roster_in",
            "roster_out",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_player_team_season_stats",
        result_key="player_team_season_stats",
        reader_method="read_player_team_season_stats",
        natural_key=("league", "season", "team_id", "player_id"),
        required_columns=_SCOPE
        + (
            "team",
            "team_id",
            "player",
            "player_id",
            "position",
            "matches",
            "minutes",
            "goals",
            "xg",
            "np_goals",
            "np_xg",
            "assists",
            "xa",
            "shots",
            "key_passes",
            "yellow_cards",
            "red_cards",
            "xg_chain",
            "xg_buildup",
        ),
    ),
    UnderstatTableContract(
        table_name="understat_team_season_breakdowns",
        result_key="team_season_breakdowns",
        reader_method="read_team_season_breakdowns",
        natural_key=("league", "season", "team_id", "dimension", "category"),
        required_columns=_SCOPE
        + (
            "team",
            "team_id",
            "dimension",
            "category",
            "source_stat",
            "minutes",
            "shots",
            "goals",
            "xg",
            "against_shots",
            "against_goals",
            "against_xg",
        ),
    ),
)

TABLE_CONTRACT_BY_NAME: Mapping[str, UnderstatTableContract] = MappingProxyType(
    {contract.table_name: contract for contract in TABLE_CONTRACTS}
)


__all__ = [
    "TABLE_CONTRACTS",
    "TABLE_CONTRACT_BY_NAME",
    "UnderstatTableContract",
]
