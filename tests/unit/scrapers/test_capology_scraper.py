"""Unit tests for scrapers.capology.

Covers the JS literal parser and helpers. Live HTTP path is covered by
the probe script + DAG smoke runs in a container.
"""

import pytest

from scrapers.capology import CapologyScraper
from scrapers.capology.scraper import (
    CAPOLOGY_LEAGUE_MAP,
    CAPOLOGY_SUPPORTED_CURRENCIES,
    R0_2B_FALLBACK_MARKER,
    _extract_anchor_text,
    _parse_row_block,
    _parse_salary_table,
    _season_long,
    _season_short,
    _slice_data_array,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestSeasonHelpers:
    @pytest.mark.parametrize("year,expected_long,expected_short", [
        (2024, '2024-2025', '2425'),
        (2025, '2025-2026', '2526'),
        (2099, '2099-2100', '9900'),
    ])
    def test_season_slugs(self, year, expected_long, expected_short):
        assert _season_long(year) == expected_long
        assert _season_short(year) == expected_short


class TestExtractAnchorText:
    @pytest.mark.parametrize("snippet,expected", [
        ("<a class='firstcol' href='/player/x-1/'><img/>Erling Haaland</a>", "Erling Haaland"),
        ("<a href='x'>Plain Name</a>", "Plain Name"),
        ("", None),
        (None, None),
        # Issue #84: HTML entities в rendered names. Без unescape Capology
        # отдаёт `Jake O&#39;Brien` и резолвер не находит FBref counterpart
        # (token_sort score падает на запятой/апострофе ниже 90).
        ("<a href='/p/jake-o-brien/'><img/>Jake O&#39;Brien</a>", "Jake O'Brien"),
        ("<a href='/p/matt-o-riley/'><img/>Matt O&#39;Riley</a>", "Matt O'Riley"),
        ("<a href='/p/x/'>Bj&ouml;rn Engels</a>", "Björn Engels"),
    ])
    def test_extract(self, snippet, expected):
        assert _extract_anchor_text(snippet) == expected


# ---------------------------------------------------------------------------
# JS literal parsing
# ---------------------------------------------------------------------------

class TestSliceDataArray:
    def test_happy_path(self):
        html = "junk before\nvar data = [\n  {'k': 1}, {'k': 2}\n];\nfooter"
        s = _slice_data_array(html)
        assert s is not None
        assert s.startswith('[')
        assert s.endswith(']')
        assert "'k': 1" in s
        assert "'k': 2" in s

    def test_returns_none_when_missing(self):
        assert _slice_data_array("no data here") is None

    def test_brackets_in_string_literals_ignored(self):
        # ScraperFC-style nested HTML in strings — `]` inside a string
        # must NOT close the array.
        html = (
            "var data = ["
            "{'name': '<a href=\"/p/[bracketed]/\">N</a>'},"
            "{'name': '<b>X</b>'}"
            "];"
        )
        s = _slice_data_array(html)
        assert s is not None
        assert s.count('{') == 2
        assert s.count('}') == 2


class TestParseRowBlock:
    def _haaland_block(self) -> str:
        return (
            "{"
            "'name': \"<a class='firstcol' href='/player/erling-haaland-36728/'>"
            "<img src='x'/>Erling Haaland</a>\","
            "'verified': \"<img src='x' class='verified-green'/>\","
            "'club': \"<a class='firstcol' href='/club/manchester-city/salaries/'>"
            "Manchester City</a>\","
            "'country': \"Norway\","
            "'position': \"F\","
            "'age': Math.round(\"24\"),"
            "'status': \"<span class='table-active'>Active</span>\","
            "'active': \"True\","
            "'loan': false,"
            "'weekly_gross_gbp': accounting.formatMoney(\"27300000\"/52, \"£ \", 0),"
            "'annual_gross_gbp': accounting.formatMoney(\"27300000\", \"£ \", 0),"
            "'weekly_net_gbp': accounting.formatMoney(\"15020000\"/52, \"£ \", 0),"
            "'annual_net_gbp': accounting.formatMoney(\"15020000\", \"£ \", 0),"
            "'bonus_gross_gbp': accounting.formatMoney(\"18200000\", \"£ \", 0),"
            "'bonus_net_gbp': accounting.formatMoney(\"10010000\", \"£ \", 0),"
            "'total_gross_gbp': accounting.formatMoney(\"45500000\", \"£ \", 0),"
            "'total_net_gbp': accounting.formatMoney(\"25030000\", \"£ \", 0),"
            "'adjusted_total_gross_gbp': accounting.formatMoney(\"45500000\", \"£ \", 0),"
            "'adjusted_total_net_gbp': accounting.formatMoney(\"25030000\", \"£ \", 0)"
            "}"
        )

    def test_happy_path(self):
        row = _parse_row_block(self._haaland_block())
        assert row is not None
        assert row['player_slug'] == 'erling-haaland-36728'
        assert row['player_name'] == 'Erling Haaland'
        assert row['club_slug'] == 'manchester-city'
        assert row['club_name'] == 'Manchester City'
        assert row['country_code'] == 'Norway'
        assert row['position'] == 'F'
        assert row['age'] == 24
        assert row['status'] == 'Active'
        assert row['verified'] is True
        assert row['active'] is True
        assert row['loan'] is False
        # weekly = annual // 52 (Capology stores the dividend in the
        # accounting.formatMoney call).
        assert row['weekly_gross_gbp'] == 27_300_000 // 52
        assert row['annual_gross_gbp'] == 27_300_000
        assert row['bonus_gross_gbp'] == 18_200_000
        assert row['total_gross_gbp'] == 45_500_000

    def test_country_as_image_html(self):
        block = (
            "{'name': \"<a href='/player/x-1/'>X</a>\","
            "'country': \"<img src='/static/flags/norway.svg'/>\","
            "'active': true, 'loan': false}"
        )
        row = _parse_row_block(block)
        assert row is not None
        assert row['country_code'] == 'norway'

    def test_missing_name_returns_none(self):
        block = "{'position': 'F'}"
        assert _parse_row_block(block) is None

    def test_active_as_quoted_string(self):
        block = (
            "{'name': \"<a href='/player/x-1/'>X</a>\","
            "'active': \"True\", 'loan': \"False\"}"
        )
        row = _parse_row_block(block)
        assert row['active'] is True
        assert row['loan'] is False

    def test_missing_optional_fields(self):
        block = "{'name': \"<a href='/player/x-1/'>X</a>\"}"
        row = _parse_row_block(block)
        assert row['weekly_gross_gbp'] is None
        assert row['annual_gross_gbp'] is None
        assert row['age'] is None


class TestParseSalaryTable:
    def test_picks_up_multiple_rows(self):
        block_a = (
            "{'name': \"<a href='/player/a-1/'>A</a>\","
            "'annual_gross_gbp': accounting.formatMoney(\"100000\", \"£ \", 0)}"
        )
        block_b = (
            "{'name': \"<a href='/player/b-2/'>B</a>\","
            "'annual_gross_gbp': accounting.formatMoney(\"200000\", \"£ \", 0)}"
        )
        html = f"var data = [{block_a}, {block_b}];"
        rows = _parse_salary_table(html)
        assert len(rows) == 2
        assert rows[0]['player_slug'] == 'a-1'
        assert rows[1]['player_slug'] == 'b-2'

    def test_no_array_returns_empty(self):
        assert _parse_salary_table("nothing useful here") == []


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_league_map(self):
        assert CAPOLOGY_LEAGUE_MAP['ENG-Premier League'] == 'premier-league'

    def test_supported_currencies(self):
        assert 'GBP' in CAPOLOGY_SUPPORTED_CURRENCIES

    def test_fallback_marker(self):
        assert R0_2B_FALLBACK_MARKER == 'CAPOLOGY_FALLBACK'


# ---------------------------------------------------------------------------
# Init smoke
# ---------------------------------------------------------------------------

class TestCapologyInit:
    def test_init_defaults(self):
        scr = CapologyScraper(leagues=['ENG-Premier League'], seasons=[2024])
        assert scr.SOURCE_NAME == 'capology'
        assert scr.DEFAULT_RATE_LIMIT == 10
        assert scr.currency == 'GBP'

    def test_init_currency_normalised_to_upper(self):
        scr = CapologyScraper(
            leagues=['ENG-Premier League'], seasons=[2024], currency='gbp',
        )
        assert scr.currency == 'GBP'
