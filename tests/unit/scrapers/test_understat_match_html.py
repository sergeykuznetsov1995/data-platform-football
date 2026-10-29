"""Recorded HTML and offline incremental enrichment regressions (#1439)."""

from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pandas as pd
import pytest

from scrapers.understat.catalog import UnderstatScope
from scrapers.understat.match_html import CARD_COLUMNS, parse_match_html, validate_card_counts
from scrapers.understat.parsers import UnderstatSchemaDrift, parse_match_payload
from tests.unit.scrapers.test_understat_incremental import LEAGUE


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "understat"


def recorded(game=29049):
    html = (FIXTURES / f"match_{game}.html").read_text()
    payload = json.loads((FIXTURES / f"match_{game}.json").read_text())
    row = {"game_id": game, "home_team_id": 83, "away_team_id": 80,
           "home_team": "Arsenal", "away_team": "Chelsea"}
    scope = UnderstatScope(LEAGUE, "EPL", 1, "2526", 2025, False, True)
    shots, players = parse_match_payload(payload, scope, row)
    return html, row, players, shots


@pytest.mark.parametrize("game,fid", [(29049, 1903399), (31209, 1983568)])
def test_recorded_html_identifiers_and_card_minutes(game, fid):
    html, row, players, shots = recorded(game)
    parsed = parse_match_html(html, row, players)
    assert parsed["whoscored_game_id"] == fid
    info = parsed["match_info"]
    for side in ("h", "a"):
        assert len(shots[(shots.team_id == int(info[side])) & (shots.result != "Own Goal")]) == int(info[f"{side}_shot"])
    if game == 29049:
        expected = json.loads((FIXTURES / "match_29049_expected.json").read_text())
        for color, column in zip(("yellow", "red"), CARD_COLUMNS):
            actual = {str(player): cards[column] for (_, player), cards in parsed["cards"].items() if cards[column]}
            assert actual == expected[color]


@pytest.mark.parametrize("change", ["identity", "missing_info", "missing_timeline", "unknown_player", "ambiguous_player", "minute"])
def test_reject_invalid_html(change):
    html, row, players, _ = recorded()
    if change == "identity":
        row["game_id"] = 1
    elif change == "missing_info":
        html = html.replace("var match_info", "var other_info")
    elif change == "missing_timeline":
        html = html.replace('class="timeline-block"', 'class="other-block"')
    elif change == "unknown_player":
        html = html.replace("/player/8497", "/player/999999")
    elif change == "ambiguous_player":
        players = pd.concat([players, players[players.player_id == 8497]])
    else:
        html = html.replace("58'", "unknown")
    with pytest.raises(UnderstatSchemaDrift, match="match HTML"):
        parse_match_html(html, row, players)


def test_repeated_cards_and_added_time_preserve_source_notation():
    html, row, players, _ = recorded()
    soup = BeautifulSoup(html, "html.parser")
    card = soup.select_one(".timeline-item-right .yellow-card").find_parent(class_="timeline-item")
    extra = deepcopy(card)
    extra.select_one(".timeline-time span").string = "90+4'"
    soup.select_one(".timeline-block").append(extra)
    parsed = parse_match_html(str(soup), row, players)
    assert parsed["cards"][("a", 8497)]["yellow_card_minutes"] == ["58'", "90+4'"]


def test_json_card_corrected_to_zero_rejects_stale_html():
    html, row, players, _ = recorded(31209)
    parsed = parse_match_html(html, row, players)
    validate_card_counts(parsed, players)
    card_player = next(player for (_, player), cards in parsed["cards"].items() if cards["yellow_card_minutes"])
    players.loc[players.player_id == card_player, "yellow_cards"] = 0
    with pytest.raises(UnderstatSchemaDrift, match="incomplete card timeline"):
        validate_card_counts(parsed, players)


def test_recorded_29049_yellow_before_red_can_publish():
    html, row, players, _ = recorded()
    parsed = parse_match_html(html, row, players)
    validate_card_counts(parsed, players)
    assert parsed["cards"][("a", 6382)] == {
        "yellow_card_minutes": ["66'"], "red_card_minutes": ["69'"]}


@pytest.mark.parametrize("change", ["no_red", "missing_red", "extra_red", "extra_yellow", "other_player"])
def test_dismissal_exception_rejects_other_card_mismatches(change):
    html, row, players, _ = recorded()
    parsed = parse_match_html(html, row, players)
    if change == "no_red":
        players.loc[players.player_id == 6382, "red_cards"] = 0
        parsed["cards"][("a", 6382)]["red_card_minutes"] = []
    elif change == "missing_red":
        parsed["cards"][("a", 6382)]["red_card_minutes"] = []
    elif change == "extra_red":
        parsed["cards"][("a", 6382)]["red_card_minutes"].append("70'")
    elif change == "extra_yellow":
        parsed["cards"][("a", 6382)]["yellow_card_minutes"].append("68'")
    else:
        players.loc[players.player_id == 8497, "yellow_cards"] = 0
    with pytest.raises(UnderstatSchemaDrift, match="incomplete card timeline"):
        validate_card_counts(parsed, players)


def test_empty_timeline_cannot_hide_roster_cards():
    html, row, players, _ = recorded(31209)
    soup = BeautifulSoup(html, "html.parser")
    soup.select_one(".timeline-block").clear()
    parsed = parse_match_html(str(soup), row, players)
    with pytest.raises(UnderstatSchemaDrift, match="incomplete card timeline"):
        validate_card_counts(parsed, players)
