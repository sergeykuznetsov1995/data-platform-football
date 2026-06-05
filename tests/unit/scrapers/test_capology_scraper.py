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
    _parse_contract_row,
    _parse_payroll_row,
    _parse_row_block,
    _parse_salary_table,
    _parse_transfer_row,
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
            "'adjusted_total_net_gbp': accounting.formatMoney(\"25030000\", \"£ \", 0),"
            # EUR/USD arrive inline in the same row (probe-confirmed); real
            # Haaland 2024/25 figures.
            "'weekly_gross_eur': accounting.formatMoney(\"31857680\"/52, \"€ \", 0),"
            "'annual_gross_eur': accounting.formatMoney(\"31857680\", \"€ \", 0),"
            "'weekly_gross_usd': accounting.formatMoney(\"35995050\"/52, \"$ \", 0),"
            "'annual_gross_usd': accounting.formatMoney(\"35995050\", \"$ \", 0)"
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
        # EUR/USD parsed from the same row (issue #195), same weekly = //52 rule.
        assert row['annual_gross_eur'] == 31_857_680
        assert row['weekly_gross_eur'] == 31_857_680 // 52
        assert row['annual_gross_usd'] == 35_995_050
        assert row['weekly_gross_usd'] == 35_995_050 // 52

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
        assert row['annual_gross_eur'] is None
        assert row['annual_gross_usd'] is None
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
        assert CAPOLOGY_SUPPORTED_CURRENCIES == ('GBP', 'EUR', 'USD')

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


# ---------------------------------------------------------------------------
# New APL data products (#321): payrolls / contracts / transfer-window.
# Blocks mirror the live row shapes — payrolls/transfer-window quote keys with
# `"`, contract-extensions with `'`; money is sometimes paren-wrapped; the
# positional payroll split d/f/k/m is Pro-locked (must NOT be extracted).
# ---------------------------------------------------------------------------

class TestPayrollRow:
    BLOCK = (
        '{'
        '"club": "<a class=\'firstcol\' href=\'/club/manchester-city/salaries/2024-2025/\'>'
        '<img/>Manchester City</a>",'
        '"club_code": "MCI",'
        '"weekly_gross_gbp": accounting.formatMoney(("226293600"/52), "£ ", 0),'
        '"annual_gross_gbp": accounting.formatMoney("226293600", "£ ", 0),'
        '"total_gross_gbp": accounting.formatMoney("244493600", "£ ", 0),'
        '"adjusted_total_gross_gbp": accounting.formatMoney("244493600", "£ ", 0),'
        '"d_gross_gbp": "<span class=\'footer-pro\'>Locked</span>",'
        '}'
    )

    def test_club_anchor_and_money(self):
        row = _parse_payroll_row(self.BLOCK)
        assert row['club_slug'] == 'manchester-city'
        assert row['club_name'] == 'Manchester City'
        assert row['club_code'] == 'MCI'
        assert row['total_gross_gbp'] == 244493600
        assert row['weekly_gross_gbp'] == 226293600 // 52  # JS ÷52 weekly view

    def test_pro_locked_position_split_not_extracted(self):
        row = _parse_payroll_row(self.BLOCK)
        assert 'd_gross_gbp' not in row  # Pro-locked → intentionally dropped


class TestContractRow:
    BLOCK = (
        '{'
        "'name': \"<a class='firstcol' href='/player/gabriel-magalhaes-1/'>"
        "<img/>Gabriel Magalh&atilde;es</a>\","
        "'club': \"<a href='/club/arsenal/salaries/2024-2025/'><img/>Arsenal</a>\","
        "'signed': moment(\"2025-06-06\").format(\"MMM D, YYYY\"),"
        "'expiration': moment(\"2029-06-30\").format(\"MMM D, YYYY\"),"
        "'years': \"5\","
        "'total_gross_gbp': accounting.formatMoney(\"10400000\", \"£ \", 0),"
        "'contract_total_gross_gbp': accounting.formatMoney(\"52000000\", \"£ \", 0),"
        '}'
    )

    def test_player_club_dates_and_money(self):
        row = _parse_contract_row(self.BLOCK)
        assert row['player_slug'] == 'gabriel-magalhaes-1'
        assert row['player_name'] == 'Gabriel Magalhães'  # entity-unescaped
        assert row['club_slug'] == 'arsenal'
        assert row['signed'] == '2025-06-06'        # unwrapped from moment(...)
        assert row['expiration'] == '2029-06-30'
        assert row['years'] == 5
        assert row['contract_total_gross_gbp'] == 52000000


class TestTransferRow:
    BLOCK = (
        '{'
        '"club": "<a class=\'firstcol\' href=\'/club/chelsea/transfer-window/2024-2025/\'>'
        '<img/>Chelsea</a>",'
        '"club_code": "CHE",'
        '"players": "30",'
        '"age": accounting.toFixed("23.5000000000", 1),'
        '"foreign": "18",'
        '"income_gbp": accounting.formatMoney("163620000", "£ ", 0),'
        '"balance_gbp": accounting.formatMoney("-42390000", "£ ", 0),'
        '}'
    )

    def test_club_counts_age_and_balance(self):
        row = _parse_transfer_row(self.BLOCK)
        assert row['club_slug'] == 'chelsea'
        assert row['club_code'] == 'CHE'
        assert row['players'] == 30
        assert row['age'] == 23.5             # accounting.toFixed float
        assert row['foreign'] == 18
        assert row['balance_gbp'] == -42390000  # net balance can be negative


class TestContractHistoryFloor:
    """Pre-2018 contract-extensions URLs serve current data → must be refused
    (no network) so a backfill can't write mislabelled dupes (#321)."""

    def test_pre_floor_season_returns_empty_without_fetch(self):
        scr = CapologyScraper(leagues=['ENG-Premier League'], seasons=[2017])
        df = scr.read_contract_extensions('ENG-Premier League', 2017)
        assert df.empty
        # schema preserved so the runner still soft-falls back cleanly
        assert 'contract_total_gross_gbp' in df.columns
