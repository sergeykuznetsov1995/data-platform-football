"""The one saison_id <-> season rule of Transfermarkt (#1390)."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import pytest

from scrapers.transfermarkt.registry import (
    RegistryError,
    SeasonFormat,
    canonical_season,
    season_window_year,
)
from scrapers.transfermarkt.season import (
    SINGLE_YEAR,
    SPLIT_YEAR,
    SeasonRuleError,
    label_to_saison_id,
    label_to_season,
    saison_id_to_season,
    season_to_saison_id,
    split_year_bounds,
)

# Calendar cups as the site states them: excerpts of the discovery cache
# 17.07.2026 (title and the page's own <tm-competition-homepage season-id>).
# J.League Cup switched to a split year with the league.
FIXTURE = (
    Path(__file__).parents[2] / "fixtures" / "transfermarkt" / "season"
    / "calendar_cups.json"
)
_PAGES = json.loads(FIXTURE.read_text(encoding="utf-8"))["pages"]


def _cup(cid):
    page = _PAGES[cid]
    label = re.search(
        r"((?:\d{2}/\d{2})|(?:\d{4}))\s*\|", page["title"]
    ).group(1)
    season_id = int(re.search(r'season-id="(\d{4})"', page["homepage_tag"]).group(1))
    fmt = SINGLE_YEAR if len(label) == 4 else SPLIT_YEAR
    return cid, page["title"], label, fmt, season_id


CALENDAR_CUPS = tuple(_cup(cid) for cid in ("BRC", "CLI", "MLSP", "JAPC"))


@pytest.mark.parametrize(
    ("saison_id", "season_format", "season"),
    [
        (2025, SPLIT_YEAR, "2526"),
        (1991, SPLIT_YEAR, "9192"),
        (1999, SPLIT_YEAR, "9900"),
        (2025, SINGLE_YEAR, "2026"),
        (2027, SINGLE_YEAR, "2028"),
    ],
)
def test_saison_id_and_season_are_inverse(saison_id, season_format, season):
    assert saison_id_to_season(saison_id, season_format) == season
    assert season_to_saison_id(season, season_format) == saison_id


def test_two_digit_label_uses_the_century_window_not_2000():
    assert split_year_bounds("91/92") == (1991, 1992)
    assert split_year_bounds("25/26") == (2025, 2026)
    assert split_year_bounds("99/00") == (1999, 2000)
    # Next year is still this century; two years ahead is the last one.
    assert split_year_bounds("27/28", today=date(2026, 9, 26)) == (2027, 2028)
    assert split_year_bounds("28/29", today=date(2026, 9, 26)) == (1928, 1929)
    assert label_to_season("91/92", SPLIT_YEAR) == "9192"
    assert canonical_season("91/92", SeasonFormat.SPLIT_YEAR) == "9192"
    assert season_window_year("91/92", SeasonFormat.SPLIT_YEAR) == 1991


@pytest.mark.parametrize(("cid", "name", "label", "fmt", "page_saison_id"), CALENDAR_CUPS)
def test_calendar_cups_follow_the_league_rule(cid, name, label, fmt, page_saison_id):
    assert (cid, label, page_saison_id) in {
        ("BRC", "2026", 2025), ("CLI", "2026", 2025),
        ("MLSP", "2026", 2025), ("JAPC", "26/27", 2026),
    }
    season = label_to_season(label, fmt)
    assert season_to_saison_id(season, fmt) == page_saison_id, (cid, name)
    assert saison_id_to_season(page_saison_id, fmt) == season


def test_invalid_values_fail_closed():
    with pytest.raises(SeasonRuleError):
        saison_id_to_season("25", SPLIT_YEAR)
    with pytest.raises(SeasonRuleError):
        season_to_saison_id("25/26", SPLIT_YEAR)
    with pytest.raises(SeasonRuleError):
        label_to_season("2025", "unknown")
    with pytest.raises(RegistryError):
        canonical_season("25/27", SeasonFormat.SPLIT_YEAR)


@pytest.mark.parametrize(
    ("label", "season_format", "saison_id"),
    [
        ("1920/21", SPLIT_YEAR, 1920),
        ("1899/1900", SPLIT_YEAR, 1899),
        ("91/92", SPLIT_YEAR, 1991),
        ("25/26", SPLIT_YEAR, 2025),
        ("2026", SINGLE_YEAR, 2025),
    ],
)
def test_label_keeps_its_stated_century(label, season_format, saison_id):
    assert label_to_saison_id(label, season_format) == saison_id
