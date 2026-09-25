"""Core league list parsing and the diff against denominator.tsv (#1499)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.espn.catalog_core import diff_catalog, parse_core_league_refs, propose_row
from scrapers.espn.denominator import DEFAULT_DENOMINATOR_PATH, load_denominator
from scrapers.espn.discovery import parse_competition_detail
from scrapers.espn.parser_common import EspnParseError

PROBES = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes"


def _load(name: str):
    return json.loads((PROBES / name).read_text(encoding="utf-8"))


@pytest.mark.unit
def test_core_list_yields_219_slugs():
    slugs = parse_core_league_refs(_load("leagues_core.json"))

    assert len(slugs) == 219
    assert "eng.1" in slugs and "afc.champions_qual" in slugs
    assert "sui.1" not in slugs


@pytest.mark.unit
def test_diff_against_the_file_has_nothing_new_and_gone_is_live_zero():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    diff = diff_catalog(denominator, parse_core_league_refs(_load("leagues_core.json")))

    assert diff.new == frozenset()
    assert diff.gone == frozenset(
        slug for slug, row in denominator.rows.items() if not row.live
    )
    assert diff.gone == {"bangabandhu.cup", "caf.championship_qual", "ind.2", "sui.1"}


@pytest.mark.unit
def test_new_slug_is_reported():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)

    diff = diff_catalog(denominator, set(denominator.rows) | {"xyz.1"})

    assert diff.new == {"xyz.1"} and diff.gone == frozenset()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"count": 218}, "count"),
        ({"pageCount": 2}, "one page"),
        ({"items": [{"$ref": "http://x/v2/sports/soccer/teams/1"}]}, "no league"),
        ({"items": None}, "items"),
    ],
)
def test_malformed_core_list_fails_closed(patch, message):
    payload = {**_load("leagues_core.json"), **patch}
    if "items" in patch and payload["items"] is not None:
        payload["count"] = len(payload["items"])

    with pytest.raises(EspnParseError, match=message):
        parse_core_league_refs(payload)


@pytest.mark.unit
def test_propose_row_classifies_recorded_details():
    cup = propose_row(
        parse_competition_detail(_load("league_detail_concacaf.champions_cup.json"))
    )
    u20 = propose_row(parse_competition_detail(_load("league_detail_fifa.world.u20.json")))
    qual = propose_row(
        parse_competition_detail(_load("league_detail_afc.champions_qual.json"))
    )

    assert (cup.tournament_class, cup.espn_gender, cup.in_target) == (
        "senior_official", "FEMALE", True,
    )
    assert cup.class_reason.startswith("manual:")
    assert (u20.tournament_class, u20.in_target) == ("youth", False)
    assert (qual.slug, qual.espn_id, qual.current_season_year, qual.in_target) == (
        "afc.champions_qual", 24452, 2026, True,
    )
    assert (qual.hidden, qual.live) == (False, True)
