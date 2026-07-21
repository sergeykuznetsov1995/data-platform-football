"""Contracts for the canonical FotMob competition-id registry."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.fotmob.constants import (
    LEAGUE_IDS,
    _load_league_ids,
    league_map_values_sql,
    render_fotmob_sql,
)


pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_SQL = PROJECT_ROOT / "dags" / "sql" / "silver"
MAP_CONSUMERS = {
    "fotmob_keeper_profile.sql",
    "fotmob_lineup.sql",
    "fotmob_manager_profile.sql",
    "fotmob_match_referee.sql",
    "fotmob_player_market_value_history.sql",
    "fotmob_player_match_aggregate.sql",
    "fotmob_player_profile.sql",
    "fotmob_player_season_profile.sql",
    "fotmob_team_leaderboards.sql",
    "fotmob_team_match.sql",
    "fotmob_team_profile.sql",
    "fotmob_team_standings.sql",
    "fotmob_transfers.sql",
    "xref_manager.sql.j2",
    "xref_match.sql",
    "xref_referee.sql.j2",
    "xref_team.sql.j2",
}


def test_registry_is_complete_and_keeps_the_legacy_public_shape():
    assert len(LEAGUE_IDS) == 14
    assert LEAGUE_IDS["ENG-Premier League"] == "47"
    assert LEAGUE_IDS["INT-World Cup"] == "77"
    assert LEAGUE_IDS["INT-Africa Cup of Nations"] == "289"
    assert len(set(LEAGUE_IDS.values())) == len(LEAGUE_IDS)


def test_registry_rejects_duplicate_source_ids(tmp_path):
    path = tmp_path / "competitions.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "competitions": [
                    {"league": "A", "competition_id": 47},
                    {"league": "B", "competition_id": 47},
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="already owned by 'A'"):
        _load_league_ids(path)


def test_sql_values_are_deterministic_and_sql_quoted():
    rendered = league_map_values_sql(indent="  ")
    assert rendered.startswith("(47, 'ENG-Premier League')")
    assert "\n  (48, 'ENG-Championship')" in rendered
    assert rendered.endswith("(44, 'INT-Copa America')")
    assert rendered.count("(") == 14


def test_renderer_only_expands_a_standalone_placeholder():
    sql = (
        "-- {{ fotmob_league_map_values_sql }} stays documentation\n"
        "WITH league_map(competition_id, league) AS (\n"
        "  VALUES\n"
        "    {{ fotmob_league_map_values_sql }}\n"
        ") SELECT * FROM league_map"
    )

    rendered = render_fotmob_sql(sql)

    assert "-- {{ fotmob_league_map_values_sql }} stays documentation" in rendered
    assert "    (47, 'ENG-Premier League')" in rendered
    assert "\n    (44, 'INT-Copa America')" in rendered
    assert rendered.count("{{ fotmob_league_map_values_sql }}") == 1


def test_every_silver_consumer_uses_the_registry_placeholder():
    actual = {
        path.name
        for path in SILVER_SQL.iterdir()
        if path.is_file()
        and "{{ fotmob_league_map_values_sql }}"
        in path.read_text(encoding="utf-8")
    }
    assert actual == MAP_CONSUMERS

    for filename in sorted(MAP_CONSUMERS):
        raw = (SILVER_SQL / filename).read_text(encoding="utf-8")
        assert raw.count("{{ fotmob_league_map_values_sql }}") == 1
        assert "(47," not in raw
        rendered = render_fotmob_sql(raw)
        assert "{{ fotmob_league_map_values_sql }}" not in rendered
        assert "(47, 'ENG-Premier League')" in rendered
        assert "(44, 'INT-Copa America')" in rendered
