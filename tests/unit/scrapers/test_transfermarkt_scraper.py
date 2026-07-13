"""Unit tests for scrapers.transfermarkt.

Covers pure parsers, native contracts, and transport adapters.  The bounded
live path is exercised by ``scripts/research/bench_transfermarkt_fetch.py``.
"""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scrapers.transfermarkt import (
    TransfermarktScraper,
    materialize_legacy_market_value_history,
    materialize_legacy_transfers,
)
from scrapers.transfermarkt.scraper import (
    ConsecutiveFailureError,
    PartialScrapeError,
    R0_2B_FALLBACK_MARKER,
    TransfermarktError,
    _coerce_int,
    _extract_club_id_from_href,
    _parse_club_listing,
    _parse_coach_history,
    _parse_coach_profile,
    _parse_height_cm,
    _parse_mv_history,
    _parse_squad_page,
    _parse_tm_date,
    _parse_tm_money_eur,
    _parse_transfers,
    _season_window,
    _stint_overlaps_season,
)
from scrapers.transfermarkt.registry import (
    SeasonFormat,
    canonical_season,
    resolve_competition,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestSeasonSemantics:
    @pytest.mark.parametrize("year,expected", [
        (2024, '2425'),
        (2025, '2526'),
        (2099, '9900'),  # wrap
    ])
    def test_four_digit_year(self, year, expected):
        assert canonical_season(year, SeasonFormat.SPLIT_YEAR) == expected

    def test_single_year_is_not_changed(self):
        assert canonical_season('2026', SeasonFormat.SINGLE_YEAR) == '2026'


class TestParseMoneyEur:
    @pytest.mark.parametrize("raw,expected", [
        ('€ 45.00 m', 45_000_000),
        ('€45.00m', 45_000_000),
        ('€500k', 500_000),
        ('€ 1.20bn', 1_200_000_000),
        ('€1.20b', 1_200_000_000),
        ('€80,00m', 80_000_000),
        ('€1.234,50m', 1_234_500_000),
        ('?', None),
        ('-', None),
        ('', None),
        (None, None),
    ])
    def test_money_parse(self, raw, expected):
        assert _parse_tm_money_eur(raw) == expected


class TestParseHeightCm:
    @pytest.mark.parametrize("raw,expected", [
        ('1,89 m', 189),
        ('1.89m', 189),
        ('1,96 m', 196),
        ('', None),
        (None, None),
        ('N/A', None),
    ])
    def test_height_parse(self, raw, expected):
        assert _parse_height_cm(raw) == expected


class TestParseTmDate:
    @pytest.mark.parametrize("raw,expected", [
        ('Feb 25, 1999', date(1999, 2, 25)),
        ('Feb 25, 1999 (26)', date(1999, 2, 25)),  # trailing age
        ('December 9, 2025', date(2025, 12, 9)),
        ('2030-06-30', date(2030, 6, 30)),
        ('not a date', None),
        ('', None),
        (None, None),
    ])
    def test_date_parse(self, raw, expected):
        assert _parse_tm_date(raw) == expected


class TestCoerceInt:
    @pytest.mark.parametrize("raw,expected", [
        (16, 16),
        ('16', 16),
        ('age: 26', 26),
        (None, None),
        ('', None),
        ('not a number', None),
        (3.7, 3),
    ])
    def test_coerce(self, raw, expected):
        assert _coerce_int(raw) == expected


class TestExtractClubIdFromHref:
    @pytest.mark.parametrize("raw,expected", [
        ('/psgtm/transfers/verein/583/saison_id/2025', '583'),
        ('/manchester-city/startseite/verein/281/saison_id/2025', '281'),
        ('', None),
        (None, None),
        ('/players/no-club-id', None),
    ])
    def test_extract(self, raw, expected):
        assert _extract_club_id_from_href(raw) == expected


# ---------------------------------------------------------------------------
# HTML parsers
# ---------------------------------------------------------------------------

class TestParseClubListing:
    @staticmethod
    def _html(rows: str) -> str:
        return f'<table class="items"><tbody>{rows}</tbody></table>'

    def test_happy_path(self):
        rows = (
            '<tr>'
            '<td class="hauptlink no-border-links">'
            '<a href="/manchester-city/startseite/verein/281/saison_id/2025">Manchester City</a>'
            '</td></tr>'
            '<tr>'
            '<td class="hauptlink no-border-links">'
            '<a href="/fc-arsenal/startseite/verein/11/saison_id/2025">Arsenal FC</a>'
            '</td></tr>'
        )
        clubs = _parse_club_listing(self._html(rows))
        assert len(clubs) == 2
        assert clubs[0] == {
            'club_id': '281',
            'club_slug': 'manchester-city',
            'club_name': 'Manchester City',
            'href': '/manchester-city/startseite/verein/281/saison_id/2025',
        }

    def test_dedups_by_club_id(self):
        rows = (
            '<tr><td class="hauptlink no-border-links">'
            '<a href="/manchester-city/startseite/verein/281/saison_id/2025">Manchester City</a>'
            '</td></tr>'
            '<tr><td class="hauptlink no-border-links">'
            '<a href="/manchester-city/startseite/verein/281/saison_id/2025">Manchester City</a>'
            '</td></tr>'
        )
        assert len(_parse_club_listing(self._html(rows))) == 1

    def test_missing_table_returns_empty(self):
        assert _parse_club_listing('<html><body>no table</body></html>') == []

    def test_garbage_input(self):
        assert _parse_club_listing('') == []
        assert _parse_club_listing('<not-html>') == []


class TestParseSquadPage:
    @staticmethod
    def _html(rows: str) -> str:
        return f'<table class="items"><tbody>{rows}</tbody></table>'

    def test_happy_path_with_market_value(self):
        rows = (
            '<tr>'
            '<td class="zentriert">#25</td>'
            '<td class="hauptlink">'
            '<a href="/gianluigi-donnarumma/profil/spieler/315858">Gianluigi Donnarumma</a>'
            '</td>'
            '<td class="rechts hauptlink">€45.00m</td>'
            '</tr>'
        )
        players = _parse_squad_page(self._html(rows), club_id='281')
        assert len(players) == 1
        assert players[0]['player_id'] == '315858'
        assert players[0]['player_slug'] == 'gianluigi-donnarumma'
        assert players[0]['name'] == 'Gianluigi Donnarumma'
        assert players[0]['club_id'] == '281'
        assert players[0]['market_value_eur'] == 45_000_000

    def test_falls_back_to_td_rechts_when_hauptlink_missing(self):
        rows = (
            '<tr>'
            '<td class="hauptlink">'
            '<a href="/some-player/profil/spieler/999">Some Player</a>'
            '</td>'
            '<td class="rechts">€500k</td>'
            '</tr>'
        )
        players = _parse_squad_page(self._html(rows), club_id='281')
        assert len(players) == 1
        assert players[0]['market_value_eur'] == 500_000

    def test_missing_market_value_is_none(self):
        rows = (
            '<tr>'
            '<td class="hauptlink">'
            '<a href="/youth-player/profil/spieler/12345">Youth Player</a>'
            '</td></tr>'
        )
        players = _parse_squad_page(self._html(rows), club_id='281')
        assert len(players) == 1
        assert players[0]['market_value_eur'] is None

    def test_dedups_by_player_id(self):
        rows = (
            '<tr><td class="hauptlink">'
            '<a href="/p/profil/spieler/1">A</a></td></tr>'
            '<tr><td class="hauptlink">'
            '<a href="/p/profil/spieler/1">A</a></td></tr>'
        )
        assert len(_parse_squad_page(self._html(rows), club_id='281')) == 1

    def test_garbage(self):
        assert _parse_squad_page('', '281') == []
        assert _parse_squad_page('<no-table>', '281') == []


class TestParseSquadPageBio:
    """Header-driven bio extraction from the detailed (`/plus/1`) squad table.

    The column set differs by view (verified live 2026-07-01): the season TM
    considers current renders `Contract`; past seasons swap in `Current club`.
    The parser must map columns by <thead> text and tolerate either set.
    """

    @staticmethod
    def _html(headers: list, cells: str) -> str:
        thead = ''.join(f'<th>{h}</th>' for h in headers)
        return (
            f'<table class="items"><thead><tr>{thead}</tr></thead>'
            f'<tbody><tr>{cells}</tr></tbody></table>'
        )

    _PLAYER_TD = (
        '<td class="posrela"><table class="inline-table">'
        '<tr><td class="hauptlink">'
        '<a href="/david-raya/profil/spieler/262749">David Raya</a>'
        '</td></tr>'
        '<tr><td>Goalkeeper</td></tr>'
        '</table></td>'
    )

    def test_current_season_variant_with_contract(self):
        headers = [
            '#', 'Player', 'Date of birth/Age', 'Nat.', 'Height', 'Foot',
            'Joined', 'Signed from', 'Contract', 'Market value',
        ]
        cells = (
            '<td class="zentriert">1</td>'
            + self._PLAYER_TD +
            '<td class="zentriert">Sep 15, 1995 (30)</td>'
            '<td class="zentriert"><img title="Spain"/></td>'
            '<td class="zentriert">1,86m</td>'
            '<td class="zentriert">right</td>'
            '<td class="zentriert">Jul 4, 2024</td>'
            '<td class="zentriert"><img title="Brentford FC"/></td>'
            '<td class="zentriert">Jun 30, 2028</td>'
            '<td class="rechts hauptlink">€30.00m</td>'
        )
        players = _parse_squad_page(self._html(headers, cells), club_id='11')
        assert len(players) == 1
        p = players[0]
        assert p['player_id'] == '262749'
        assert p['name'] == 'David Raya'
        assert p['position'] == 'Goalkeeper'
        assert p['dob'] == date(1995, 9, 15)
        assert p['age'] == 30
        assert p['nationality'] == 'Spain'
        assert p['height_cm'] == 186
        assert p['foot'] == 'right'
        assert p['contract_until'] == date(2028, 6, 30)
        assert p['market_value_eur'] == 30_000_000

    def test_past_season_variant_without_contract(self):
        headers = [
            '#', 'Player', 'Date of birth/Age', 'Nat.', 'Current club',
            'Height', 'Foot', 'Joined', 'Signed from', 'Market value',
        ]
        cells = (
            '<td class="zentriert">1</td>'
            + self._PLAYER_TD +
            '<td class="zentriert">Sep 15, 1995 (30)</td>'
            '<td class="zentriert"><img title="Spain"/></td>'
            '<td class="zentriert"><img title="Arsenal FC"/></td>'
            '<td class="zentriert">1,86m</td>'
            '<td class="zentriert">right</td>'
            '<td class="zentriert">Jul 4, 2024</td>'
            '<td class="zentriert"><img title="Brentford FC"/></td>'
            '<td class="rechts hauptlink">€30.00m</td>'
        )
        players = _parse_squad_page(self._html(headers, cells), club_id='11')
        assert len(players) == 1
        p = players[0]
        assert p['dob'] == date(1995, 9, 15)
        assert p['height_cm'] == 186
        assert p['foot'] == 'right'
        assert p['contract_until'] is None  # column absent in this view

    def test_dash_and_empty_cells_are_none(self):
        headers = [
            '#', 'Player', 'Date of birth/Age', 'Nat.', 'Height', 'Foot',
            'Joined', 'Signed from', 'Contract', 'Market value',
        ]
        cells = (
            '<td class="zentriert">1</td>'
            + self._PLAYER_TD +
            '<td class="zentriert">-</td>'
            '<td class="zentriert"></td>'
            '<td class="zentriert"></td>'
            '<td class="zentriert">-</td>'
            '<td class="zentriert"></td>'
            '<td class="zentriert"></td>'
            '<td class="zentriert">-</td>'
            '<td class="rechts hauptlink">-</td>'
        )
        players = _parse_squad_page(self._html(headers, cells), club_id='11')
        p = players[0]
        assert p['dob'] is None
        assert p['age'] is None
        assert p['nationality'] is None
        assert p['height_cm'] is None
        assert p['foot'] is None
        assert p['contract_until'] is None
        assert p['market_value_eur'] is None

    def test_cell_count_mismatch_skips_bio_not_row(self):
        # A row whose top-level td count differs from the header count
        # (e.g. colspan separator quirks) must keep the stable core fields
        # and drop only the header-mapped bio.
        headers = [
            '#', 'Player', 'Date of birth/Age', 'Nat.', 'Height', 'Foot',
            'Joined', 'Signed from', 'Contract', 'Market value',
        ]
        cells = (
            '<td class="zentriert">1</td>'
            + self._PLAYER_TD +
            '<td class="zentriert">Sep 15, 1995 (30)</td>'
        )
        players = _parse_squad_page(self._html(headers, cells), club_id='11')
        assert len(players) == 1
        assert players[0]['player_id'] == '262749'
        assert players[0]['dob'] is None


# ---------------------------------------------------------------------------
# JSON parsers (ceapi endpoints)
# ---------------------------------------------------------------------------

class TestParseMvHistory:
    def test_happy_path(self):
        payload = {
            'list': [
                {
                    'x': 1425250800000, 'y': 300000, 'mw': '€300k',
                    'datum_mw': 'Mar 2, 2015', 'verein': 'Milan Primavera',
                    'age': '16',
                },
                {
                    'x': 1765234800000, 'y': 45000000, 'mw': '€45.00m',
                    'datum_mw': 'Dec 9, 2025', 'verein': 'Manchester City',
                    'age': '26',
                },
            ],
        }
        rows = _parse_mv_history(payload, '315858')
        assert len(rows) == 2
        assert rows[0]['mv_date'] == date(2015, 3, 2)
        assert rows[0]['value_eur'] == 300_000
        assert rows[0]['club_name'] == 'Milan Primavera'
        assert rows[0]['age'] == 16
        assert rows[1]['value_eur'] == 45_000_000

    def test_the_epoch_is_midnight_in_the_sources_own_timezone(self):
        # The epoch is midnight CET, which read as UTC lands on the day before.
        payload = {'list': [{
            'x': 1425250800000, 'y': 300000, 'mw': '€300k',
            'verein': 'Club', 'age': '16',
        }]}
        rows = _parse_mv_history(payload, '1')
        assert len(rows) == 1
        assert rows[0]['mv_date'] == date(2015, 3, 2)

    def test_the_machine_date_wins_over_the_hosts_rendering_of_it(self):
        # '03/05/2015' parses just as happily day-first as month-first; only the
        # epoch says which one the source meant.
        payload = {'list': [{
            'x': 1425250800000, 'datum_mw': '03/05/2015', 'y': 300000,
            'mw': '€300k', 'verein': 'Club', 'age': '16',
        }]}

        rows = _parse_mv_history(payload, '1')

        assert rows[0]['mv_date'] == date(2015, 3, 2)

    def test_empty_list(self):
        assert _parse_mv_history({'list': []}, '1') == []
        assert _parse_mv_history({}, '1') == []
        assert _parse_mv_history(None, '1') == []  # type: ignore[arg-type]


class TestParseDates:
    def test_the_day_first_rendering_of_the_com_host_is_understood(self):
        from scrapers.transfermarkt.scraper import _parse_tm_date

        # A day past the 12th proves which number the host puts first.
        assert _parse_tm_date('17/08/1993 (31)') == date(1993, 8, 17)
        assert _parse_tm_date('Aug 17, 1993') == date(1993, 8, 17)


class TestParseTransfers:
    def test_the_machine_date_wins_over_the_hosts_rendering_of_it(self):
        # .com renders 27 July 2020 as "27/07/2020" and .us as "Jul 27, 2020";
        # only dateUnformatted says which number is the day on every host.
        payload = {'transfers': [{
            'date': '27/07/2020',
            'dateUnformatted': '2020-07-27',
            'season': '20/21',
            'upcoming': False,
            'from': {
                'clubName': 'PSG',
                'href': '/psgtm/transfers/verein/583/saison_id/2020',
            },
            'to': {
                'clubName': 'Man City',
                'href': '/manchester-city/transfers/verein/281/saison_id/2020',
            },
            'fee': '€30.00m',
            'marketValue': '€40.00m',
        }]}

        rows = _parse_transfers(payload, '315858')

        assert rows[0]['transfer_date'] == date(2020, 7, 27)

    def test_happy_path(self):
        payload = {'transfers': [{
            'date': 'Sep 1, 2025',
            'season': '25/26',
            'upcoming': False,
            'from': {
                'clubName': 'PSG',
                'href': '/psgtm/transfers/verein/583/saison_id/2025',
            },
            'to': {
                'clubName': 'Man City',
                'href': '/manchester-city/transfers/verein/281/saison_id/2025',
            },
            'fee': '€30.00m',
            'marketValue': '€40.00m',
        }]}
        rows = _parse_transfers(payload, '315858')
        assert len(rows) == 1
        row = rows[0]
        assert row['player_id'] == '315858'
        assert row['transfer_date'] == date(2025, 9, 1)
        assert row['season'] == '25/26'
        assert row['event_season'] == '2526'
        assert len(row['transfer_id']) == 64
        assert row['from_club_id'] == '583'
        assert row['from_club_name'] == 'PSG'
        assert row['to_club_id'] == '281'
        assert row['to_club_name'] == 'Man City'
        assert row['fee_eur'] == 30_000_000
        assert row['market_value_eur'] == 40_000_000
        assert row['is_upcoming'] is False

    def test_upcoming_loan_with_no_fee(self):
        payload = {'transfers': [{
            'date': None,
            'season': '25/26',
            'upcoming': True,
            'from': {'clubName': 'A', 'href': '/verein/1/'},
            'to': {'clubName': 'B', 'href': '/verein/2/'},
            'fee': 'loan transfer',
            'marketValue': '?',
        }]}
        rows = _parse_transfers(payload, '1')
        assert len(rows) == 1
        assert rows[0]['fee_eur'] is None
        assert rows[0]['market_value_eur'] is None
        assert rows[0]['is_upcoming'] is True

    def test_empty(self):
        assert _parse_transfers({'transfers': []}, '1') == []
        assert _parse_transfers({}, '1') == []


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_tm_league_map_apl(self):
        competition = resolve_competition('ENG-Premier League')
        assert (competition.slug, competition.competition_id) == (
            'premier-league', 'GB1',
        )

    def test_fallback_marker(self):
        assert R0_2B_FALLBACK_MARKER == 'TM_FALLBACK'


# ---------------------------------------------------------------------------
# Init smoke (no I/O — just verifies the class instantiates cleanly)
# ---------------------------------------------------------------------------

class TestTransfermarktInit:
    def test_init_default_rate_limit(self):
        scr = TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )
        assert scr.SOURCE_NAME == 'transfermarkt'
        assert scr.DEFAULT_RATE_LIMIT == 12
        assert scr.leagues == ['ENG-Premier League']
        assert scr.seasons == [2025]
        assert scr._last_endpoint_error is None
        assert scr._rate_limiter.config.burst_size == 1
        assert scr._rate_limiter.config.max_requests == 12


# ---------------------------------------------------------------------------
# Consecutive-failure cap → raise, not partial frame (#457)
#
# A partial frame would be saved with replace_partitions=['league','season']
# by the runner, wiping the full bronze partition. Hitting the cap must
# therefore propagate instead of returning collected rows.
# ---------------------------------------------------------------------------

class TestConsecutiveFailureRaise:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )

    def test_read_mv_history_raises_on_consecutive_failures(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(tm, '_MIN_SUCCESS_RATIO', 0)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: None,
        )
        with pytest.raises(
            ConsecutiveFailureError,
            match='consecutive market_value_points failures',
        ):
            scraper.read_market_value_history(
                league='ENG-Premier League', season=2025,
                player_ids=['1', '2', '3', '4'],
            )

    def test_read_transfers_raises_on_consecutive_failures(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(tm, '_MIN_SUCCESS_RATIO', 0)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: None,
        )
        with pytest.raises(
            ConsecutiveFailureError, match='consecutive transfer_events failures',
        ):
            scraper.read_transfers(
                league='ENG-Premier League', season=2025,
                player_ids=['1', '2', '3', '4'],
            )

    def test_counter_reset_prevents_raise(self, scraper, monkeypatch):
        # 2 failures then a success, repeated — the counter never reaches
        # the cap of 3, so no raise and the frame materialises.
        # _MIN_SUCCESS_RATIO is disabled: this scenario (ratio 1/3) now
        # trips PartialScrapeError (#484), tested separately — here we
        # isolate the counter-reset behaviour of the consecutive cap.
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(tm, '_MIN_SUCCESS_RATIO', 0)
        responses = iter([None, None, {'list': [{'x': 1, 'y': 100}]}] * 3)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_mv_history', lambda payload, player_id: [
            {
                'player_id': player_id,
                'mv_date': date(2025, 1, 1),
                'value_eur': 100,
            },
        ])
        df = scraper.read_market_value_history(
            league='ENG-Premier League', season=2025,
            player_ids=[str(i) for i in range(9)],
        )
        assert not df.empty
        assert df['player_id'].nunique() == 3  # every 3rd fetch succeeded


# ---------------------------------------------------------------------------
# In-run success-ratio → raise on partial scrape (#484)
#
# Intermittent failures (e.g. every 2nd player fails) reset the consecutive
# counter and never trip the #457 cap, yet still return a half-empty frame
# that the runner would save with replace_partitions. A run whose
# success/attempted ratio falls below _MIN_SUCCESS_RATIO must raise instead.
# ---------------------------------------------------------------------------

class TestPartialScrapeRatio:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )

    def _patch_players_pipeline(self, monkeypatch, scraper, squad_responses):
        # One club per response; a None response = failed squad-page fetch.
        # Each fetched squad yields one player keyed by the club id.
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': str(i), 'club_slug': f'club-{i}', 'club_name': f'C{i}'}
            for i in range(len(squad_responses))
        ])
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [
            {'player_id': f'p{club_id}', 'player_slug': f'player-{club_id}',
             'name': f'P{club_id}', 'club_id': club_id,
             'market_value_eur': None},
        ])
        responses = iter(squad_responses)
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None:
                next(responses) if label == 'squad' else '<html/>',
        )

    def test_read_players_raises_on_low_squad_success_ratio(
        self, scraper, monkeypatch,
    ):
        # alternating squad failures: ratio 0.5 < 0.9
        self._patch_players_pipeline(
            monkeypatch, scraper, [None, '<html/>'] * 5,
        )
        with pytest.raises(PartialScrapeError, match='squad pages'):
            scraper.read_players(league='ENG-Premier League', season=2025)

    def test_read_players_passes_on_high_squad_success_ratio(
        self, scraper, monkeypatch,
    ):
        # 1 failure of 10 → ratio 0.9, exactly at threshold → no raise
        self._patch_players_pipeline(
            monkeypatch, scraper, [None] + ['<html/>'] * 9,
        )
        df = scraper.read_players(league='ENG-Premier League', season=2025)
        assert df['player_id'].nunique() == 9

    def test_read_mv_history_raises_on_low_success_ratio(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        responses = iter([None, {'list': [{'x': 1, 'y': 100}]}] * 5)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_mv_history', lambda payload, player_id: [
            {
                'player_id': player_id,
                'mv_date': date(2025, 1, 1),
                'value_eur': 100,
            },
        ])
        with pytest.raises(PartialScrapeError, match='market_value_points'):
            scraper.read_market_value_history(
                league='ENG-Premier League', season=2025,
                player_ids=[str(i) for i in range(10)],
            )

    def test_read_transfers_raises_on_low_success_ratio(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        responses = iter([None, {'transfers': [{
            'id': 'x',
            'date': 'Jul 1, 2025',
            'season': '25/26',
            'from': {'clubName': 'A'},
            'to': {'clubName': 'B'},
        }]}] * 5)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_transfers', lambda payload, player_id: [
            {
                'transfer_id': f't-{player_id}',
                'player_id': player_id,
                'fee_eur': 100,
            },
        ])
        with pytest.raises(PartialScrapeError, match='transfer_events'):
            scraper.read_transfers(
                league='ENG-Premier League', season=2025,
                player_ids=[str(i) for i in range(10)],
            )

    def test_total_failure_still_returns_empty_frame(
        self, scraper, monkeypatch,
    ):
        # 0 successes → empty frame → graceful TM_FALLBACK path (exit 2 in
        # the runner), NOT PartialScrapeError: nothing gets saved anyway.
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: None,
        )
        df = scraper.read_market_value_history(
            league='ENG-Premier League', season=2025,
            player_ids=['1', '2', '3'],
        )
        assert df.empty


# ---------------------------------------------------------------------------
# current_club_name must agree with current_club_id (#800)
#
# current_club_id is always the season squad club. The name must come from the
# same squad — NOT the player's scrape-time profile club. A player who later
# transferred away (Ederson: 281 Man City in 2018 → Fenerbahçe by 2025) would
# otherwise get id=281 but name='Fenerbahçe' — id and name about different clubs.
# ---------------------------------------------------------------------------

class TestReadPlayersCurrentClub:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2018],
        )

    def test_current_club_name_comes_from_season_squad(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': '281', 'club_slug': 'manchester-city',
             'club_name': 'Man City'},
        ])
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [
            {'player_id': '238223', 'player_slug': 'ederson',
             'name': 'Ederson', 'club_id': club_id, 'market_value_eur': None},
        ])
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None: '<html/>',
        )

        df = scraper.read_players(league='ENG-Premier League', season=2018)

        assert len(df) == 1
        row = df.iloc[0]
        assert row['current_club_id'] == '281'
        # Name must agree with the id → season squad club (#800).
        assert row['current_club_name'] == 'Man City'


# ---------------------------------------------------------------------------
# Contract observations are source-faithful
#
# Carry-forward belongs in a deterministic Silver model, not in the scraper:
# a missing current observation must not be silently rewritten from old Bronze.
# ---------------------------------------------------------------------------

class TestReadPlayersContractObservations:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )

    def _patch_pipeline(self, monkeypatch, scraper, squad_rows):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': '11', 'club_slug': 'fc-arsenal', 'club_name': 'Arsenal'},
        ])
        monkeypatch.setattr(
            tm, '_parse_squad_page', lambda html, club_id: squad_rows,
        )
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None: '<html/>',
        )

    def test_missing_contract_stays_missing(self, scraper, monkeypatch):
        self._patch_pipeline(monkeypatch, scraper, [
            {'player_id': '1', 'player_slug': 'a', 'name': 'A',
             'club_id': '11', 'market_value_eur': None,
             'contract_until': None},
            {'player_id': '2', 'player_slug': 'b', 'name': 'B',
             'club_id': '11', 'market_value_eur': None,
             'contract_until': None},
        ])
        df = scraper.read_players(league='ENG-Premier League', season=2025)
        by_id = df.set_index('player_id')
        assert by_id.loc['1', 'contract_until'] is None
        assert by_id.loc['2', 'contract_until'] is None

    def test_scraped_contract_is_preserved(self, scraper, monkeypatch):
        self._patch_pipeline(monkeypatch, scraper, [
            {'player_id': '1', 'player_slug': 'a', 'name': 'A',
             'club_id': '11', 'market_value_eur': None,
             'contract_until': date(2030, 6, 30)},
        ])
        df = scraper.read_players(league='ENG-Premier League', season=2025)
        assert df.iloc[0]['contract_until'] == date(2030, 6, 30)

    def test_scraper_has_no_contract_cache_resolver(self, scraper):
        assert not hasattr(scraper, '_resolve_contracts_from_bronze')


# ---------------------------------------------------------------------------
# Coach-bio reuse from bronze (proxy-traffic fix)
#
# Coach bios (dob/nationality) are immutable — a profile materialised by any
# earlier run must be reused instead of re-fetched, so a weekly run downloads
# profiles only for genuinely new appointments.
# ---------------------------------------------------------------------------

class TestReadCoachesBronzeReuse:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )

    def test_known_coach_skips_profile_fetch(self, scraper, monkeypatch):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': '11', 'club_slug': 'fc-arsenal', 'club_name': 'Arsenal'},
        ])
        monkeypatch.setattr(tm, '_parse_coach_history', lambda html, club_id: [
            {'coach_id': '100', 'coach_slug': 'known-coach', 'name': 'Known',
             'role': 'Manager', 'club_id': club_id},
            {'coach_id': '200', 'coach_slug': 'new-coach', 'name': 'New',
             'role': 'Manager', 'club_id': club_id},
        ])
        monkeypatch.setattr(
            tm, '_stint_overlaps_season', lambda stint, s, e: True,
        )
        monkeypatch.setattr(
            scraper, '_resolve_coach_bios_from_bronze',
            lambda: {'100': {'name': 'Known Coach',
                             'dob': date(1971, 1, 18),
                             'nationality': 'Spain'}},
        )
        monkeypatch.setattr(
            tm, '_parse_coach_profile',
            lambda payload, coach_id: {'name': 'New Coach',
                                       'dob': date(1980, 2, 2),
                                       'nationality': 'Italy'},
        )
        fetched = []

        def _fake_fetch(url, label='html', context=None):
            fetched.append(label)
            return '<html/>'

        monkeypatch.setattr(scraper, '_fetch_html', _fake_fetch)

        df = scraper.read_coaches(league='ENG-Premier League', season=2025)

        # Only the unknown coach costs a profile request.
        assert fetched.count('coach_profile') == 1
        by_id = df.set_index('coach_id')
        assert by_id.loc['100', 'name'] == 'Known Coach'
        assert by_id.loc['100', 'nationality'] == 'Spain'
        assert by_id.loc['200', 'name'] == 'New Coach'
        assert by_id.loc['200', 'dob'] == date(1980, 2, 2)


# ---------------------------------------------------------------------------
# Shared Trino DB-API stubs for bronze-lookup tests
# ---------------------------------------------------------------------------

class _StubCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


def _stub_trino_modules(cursor):
    """sys.modules stubs for the lazy ``import trino`` inside the scraper."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    stub_trino = MagicMock()
    stub_trino.dbapi.connect.return_value = conn
    return {'trino': stub_trino, 'trino.auth': stub_trino.auth}


# ---------------------------------------------------------------------------
# Deterministic LIMIT subset in the bronze roster resolver
# ---------------------------------------------------------------------------

class TestResolverDeterministicLimit:
    @pytest.fixture
    def scraper(self):
        return TransfermarktScraper(
            leagues=['ENG-Premier League'], seasons=[2025],
        )

    def test_limit_window_orders_numeric_in_python(self, scraper):
        # player_id is varchar; lexicographic order would pick the same ~100
        # ids on '1'/'2' forever (#620). Sort numerically in Python and slice
        # the window — the SQL itself carries no ORDER BY / LIMIT.
        from unittest.mock import patch
        import sys

        cursor = _StubCursor([('10',), ('2',), ('100',), ('3',)])
        with patch.dict(sys.modules, _stub_trino_modules(cursor)):
            ids = scraper._resolve_player_ids_from_bronze(
                'ENG-Premier League', '2526', limit=2,
            )
        assert ids == ['2', '3']  # numeric order [2,3,10,100], window [0:2]
        sql, _ = cursor.executed[0]
        assert 'transfermarkt_squad_memberships' in sql
        assert 'ORDER BY' not in sql
        assert 'LIMIT' not in sql

    def test_no_limit_returns_full_numeric_sorted(self, scraper):
        from unittest.mock import patch
        import sys

        cursor = _StubCursor([('10',), ('2',), ('1',)])
        with patch.dict(sys.modules, _stub_trino_modules(cursor)):
            ids = scraper._resolve_player_ids_from_bronze(
                'ENG-Premier League', '2526',
            )
        assert ids == ['1', '2', '10']
        sql, _ = cursor.executed[0]
        assert 'ORDER BY' not in sql
        assert 'LIMIT' not in sql

    def test_window_offset_rotates(self, scraper):
        # 250-player roster, limit=100 → 3 windows tile the roster, the last
        # wrapping around. Stub rows are reversed to prove Python sorts them.
        from unittest.mock import patch
        import sys

        rows = [(str(i),) for i in range(250, 0, -1)]
        sorted_ids = [str(i) for i in range(1, 251)]

        def _resolve(offset):
            cursor = _StubCursor(list(rows))
            with patch.dict(sys.modules, _stub_trino_modules(cursor)):
                return scraper._resolve_player_ids_from_bronze(
                    'ENG-Premier League', '2526', limit=100, window_offset=offset,
                )

        assert _resolve(0) == sorted_ids[0:100]
        assert _resolve(1) == sorted_ids[100:200]
        # offset 2 → start=(2*100)%250=200 → tail [200:250] + wrap head [0:50]
        assert _resolve(2) == sorted_ids[200:250] + sorted_ids[0:50]

    def test_window_smaller_than_limit_returns_all(self, scraper):
        from unittest.mock import patch
        import sys

        rows = [(str(i),) for i in range(50, 0, -1)]
        cursor = _StubCursor(rows)
        with patch.dict(sys.modules, _stub_trino_modules(cursor)):
            ids = scraper._resolve_player_ids_from_bronze(
                'ENG-Premier League', '2526', limit=100, window_offset=3,
            )
        assert ids == [str(i) for i in range(1, 51)]

    def test_native_missing_table_falls_back_to_legacy(self, scraper, monkeypatch):
        class Cursor(_StubCursor):
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
                if 'squad_memberships' in sql:
                    raise RuntimeError('TABLE_NOT_FOUND')

        cursor = Cursor([('2',), ('1',)])
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(scraper, '_bronze_connection', lambda: connection)

        ids = scraper._resolve_player_ids_from_bronze(
            'ENG-Premier League', '2526',
        )

        assert ids == ['1', '2']
        assert 'squad_memberships' in cursor.executed[0][0]
        assert 'transfermarkt_players' in cursor.executed[1][0]

    def test_non_missing_native_error_raises_instead_of_false_empty(
        self, scraper, monkeypatch,
    ):
        connection = MagicMock()
        connection.cursor.return_value.execute.side_effect = RuntimeError(
            'Trino unavailable',
        )
        monkeypatch.setattr(scraper, '_bronze_connection', lambda: connection)

        with pytest.raises(TransfermarktError, match='roster lookup failed'):
            scraper._resolve_player_ids_from_bronze(
                'ENG-Premier League', '2526',
            )


# ---------------------------------------------------------------------------
# Coach parsers (issue #434)
# ---------------------------------------------------------------------------

def _coach_profile_html(name: str, dob: str, nat: str) -> str:
    return (
        '<html><body>'
        f'<h1 class="data-header__headline-wrapper">{name}</h1>'
        f'<span itemprop="birthDate">{dob}</span>'
        f'<span itemprop="nationality">{nat}</span>'
        '</body></html>'
    )


class TestParseCoachProfile:
    def test_extracts_dob_nationality(self):
        import datetime

        html = _coach_profile_html('Pep Guardiola', 'Jan 18, 1971 (55)', 'Spain')
        bio = _parse_coach_profile(html, coach_id='5672')
        assert bio['coach_id'] == '5672'
        assert bio['name'] == 'Pep Guardiola'
        assert bio['dob'] == datetime.date(1971, 1, 18)
        assert bio['nationality'] == 'Spain'

    def test_missing_h1_returns_none(self):
        assert _parse_coach_profile('<html><body>x</body></html>', coach_id='1') is None

    def test_missing_bio_fields_degrade_to_none(self):
        html = (
            '<html><body><h1 class="data-header__headline-wrapper">Joe Coach</h1>'
            '</body></html>'
        )
        bio = _parse_coach_profile(html, coach_id='9')
        assert bio['name'] == 'Joe Coach'
        assert bio['dob'] is None
        assert bio['nationality'] is None


# ---------------------------------------------------------------------------
# Trainer-history parser + season window (issue #619)
# ---------------------------------------------------------------------------

def _history_row(slug, cid, name, appointed, left, dob='Jan 1, 1970'):
    """Mirror one live mitarbeiterhistorie "Detailed view" row (issue #793).

    Column 0 is an ``inline-table`` carrying a portrait link (SAME trainer href,
    NO text) THEN the name link, with the date of birth below. Then: Nat. (flag,
    no text) · Appointed · End · Time-in-post · Matches. The portrait-link-first
    + DOB-in-name-cell shape is exactly what silently emptied the table before
    #793, so the fixture must reproduce it (the old fixture assumed a flat
    name-link-first row and never failed against the real layout).
    """
    return (
        '<tr>'
        '<td>'
        '<table class="inline-table"><tr>'
        f'<td rowspan="2"><a href="/{slug}/profil/trainer/{cid}">'
        '<img src="portrait.png"/></a></td>'
        f'<td><a href="/{slug}/profil/trainer/{cid}">{name}</a></td>'
        '</tr>'
        f'<tr><td>{dob}</td></tr></table>'
        '</td>'
        '<td class="zentriert"><img title="England"/></td>'
        f'<td class="zentriert">{appointed}</td>'
        f'<td class="zentriert">{left}</td>'
        '<td class="zentriert">02 years 1 month</td>'
        '<td class="zentriert">80</td>'
        '</tr>'
    )


def _history_html(rows: str) -> str:
    return (
        '<html><body>'
        '<table class="items">'
        '<thead><tr>'
        '<th>Name/Date of birth</th><th>Nat.</th><th>Appointed</th>'
        '<th>End of time in post</th><th>Time in post</th><th>Matches</th>'
        '</tr></thead>'
        f'<tbody>{rows}</tbody>'
        '</table>'
        '</body></html>'
    )


class TestParseCoachHistory:
    def test_extracts_every_manager_with_dates(self):
        import datetime

        rows = (
            # incumbent (open-ended '-' end) + a mid-season caretaker spell
            _history_row('regis-le-bris', '39286', 'Le Bris, Régis',
                         'Jul 1, 2025', '-', dob='Mar 8, 1976')
            + _history_row('mike-dodds', '88888', 'Mike Dodds',
                           'Jan 10, 2025', 'Jun 30, 2025', dob='Sep 1, 1990')
        )
        out = _parse_coach_history(_history_html(rows), club_id='289')
        assert len(out) == 2, out
        by_id = {r['coach_id']: r for r in out}

        bris = by_id['39286']
        assert bris['coach_slug'] == 'regis-le-bris'
        # The name link, NOT the empty portrait link, supplies the name.
        assert bris['name'] == 'Le Bris, Régis'
        assert bris['appointed_date'] == datetime.date(2025, 7, 1)
        assert bris['left_date'] is None  # '-' → incumbent, open-ended
        assert bris['club_id'] == '289'

        # The caretaker spell is still CAPTURED as its own stint (the coverage
        # gap #619 closed); the detailed view has no role column, so it is not
        # role-labelled — every entry defaults to 'Manager'.
        dodds = by_id['88888']
        assert dodds['role'] == 'Manager'
        assert dodds['appointed_date'] == datetime.date(2025, 1, 10)
        assert dodds['left_date'] == datetime.date(2025, 6, 30)

    def test_portrait_link_first_still_extracts_coach(self):
        # Regression for #793: each row's FIRST trainer link is the empty
        # portrait <img> link; picking it gave a blank name and dropped every
        # coach. The name link (2nd, with text) must be used instead.
        row = _history_row('pep', '5672', 'Pep Guardiola', 'Jul 1, 2016', '-')
        out = _parse_coach_history(_history_html(row), club_id='281')
        assert len(out) == 1, out
        assert out[0]['name'] == 'Pep Guardiola'
        assert out[0]['coach_id'] == '5672'

    def test_dob_in_name_cell_not_read_as_appointed(self):
        # Regression for #793: the Name column also carries the DOB. Reading
        # dates from every descendant <td> mistook the DOB for the appointed
        # date — appointed must come from the Appointed column.
        import datetime
        row = _history_row('x', '7', 'X Coach', 'Aug 1, 2024', '-',
                           dob='Jan 18, 1971')
        out = _parse_coach_history(_history_html(row), club_id='9')
        assert out[0]['appointed_date'] == datetime.date(2024, 8, 1)
        assert out[0]['left_date'] is None

    def test_skips_rows_without_trainer_link(self):
        # A player link (spieler, not trainer) and a plain row must be ignored.
        noise = (
            '<tr><td class="hauptlink">'
            '<a href="/some-player/profil/spieler/12345">Some Player</a></td>'
            '<td>Jan 1, 2025</td></tr>'
            '<tr><td>section header</td></tr>'
        )
        out = _parse_coach_history(_history_html(noise), club_id='1')
        assert out == []

    def test_dedups_identical_stint(self):
        rows = (
            _history_row('pep', '5672', 'Pep Guardiola', 'Jul 1, 2016', '-')
            + _history_row('pep', '5672', 'Pep Guardiola', 'Jul 1, 2016', '-')
        )
        out = _parse_coach_history(_history_html(rows), club_id='281')
        assert len(out) == 1, out

    def test_role_defaults_to_manager(self):
        # The detailed view has no role/function column → always 'Manager'.
        row = _history_row('x', '7', 'X Coach', 'Aug 1, 2024', '-')
        out = _parse_coach_history(_history_html(row), club_id='9')
        assert out[0]['role'] == 'Manager'

    def test_garbage_input(self):
        assert _parse_coach_history('', club_id='1') == []
        assert _parse_coach_history('<not-html>', club_id='1') == []


class TestSeasonWindow:
    def test_apl_season_bounds(self):
        import datetime

        start, end = _season_window(2025, SeasonFormat.SPLIT_YEAR)
        assert start == datetime.date(2025, 7, 1)
        assert end == datetime.date(2026, 6, 30)


class TestStintOverlapsSeason:
    def _win(self):
        return _season_window(
            2025, SeasonFormat.SPLIT_YEAR,
        )  # (2025-07-01, 2026-06-30)

    def test_incumbent_appointed_before_window_kept(self):
        import datetime
        s, e = self._win()
        stint = {'appointed_date': datetime.date(2024, 7, 1), 'left_date': None}
        assert _stint_overlaps_season(stint, s, e) is True

    def test_mid_season_caretaker_kept(self):
        import datetime
        s, e = self._win()
        stint = {
            'appointed_date': datetime.date(2025, 11, 1),
            'left_date': datetime.date(2025, 11, 20),
        }
        assert _stint_overlaps_season(stint, s, e) is True

    def test_old_stint_dropped(self):
        import datetime
        s, e = self._win()
        stint = {
            'appointed_date': datetime.date(2017, 1, 1),
            'left_date': datetime.date(2019, 5, 1),
        }
        assert _stint_overlaps_season(stint, s, e) is False

    def test_future_stint_dropped(self):
        import datetime
        s, e = self._win()
        stint = {'appointed_date': datetime.date(2027, 1, 1), 'left_date': None}
        assert _stint_overlaps_season(stint, s, e) is False

    def test_fully_undated_stint_fails_closed(self):
        s, e = self._win()
        assert _stint_overlaps_season(
            {'appointed_date': None, 'left_date': None}, s, e
        ) is False


# ---------------------------------------------------------------------------
# Native Bronze contracts and pure dual-write projections
# ---------------------------------------------------------------------------

class TestNativeSquadContracts:
    def test_multi_club_player_keeps_both_memberships_and_legacy_rows(
        self, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        clubs = [
            {'club_id': '11', 'club_slug': 'arsenal', 'club_name': 'Arsenal'},
            {'club_id': '281', 'club_slug': 'man-city', 'club_name': 'Man City'},
        ]
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: clubs)
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [{
            'player_id': '7', 'player_slug': 'same-player', 'name': 'Same Player',
            'club_id': club_id, 'position': 'Midfield', 'market_value_eur': 10,
        }])
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None: '<html/>',
        )

        bundle = scraper.read_squad_data('ENG-Premier League', 2025)

        assert set(bundle) == {
            'memberships', 'attribute_observations',
            'contract_observations', 'legacy_players',
        }
        memberships = bundle['memberships']
        assert len(memberships) == 2
        assert set(memberships['club_id']) == {'11', '281'}
        assert set(memberships['club_slug']) == {'arsenal', 'man-city'}
        assert not memberships.duplicated(
            ['league', 'season', 'club_id', 'player_id']
        ).any()
        assert len(bundle['attribute_observations']) == 2
        assert len(bundle['legacy_players']) == 2

    def test_limit_stops_squad_traversal_immediately(self, monkeypatch):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': str(idx), 'club_slug': f'club-{idx}', 'club_name': f'C{idx}'}
            for idx in range(5)
        ])
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [{
            'player_id': club_id, 'player_slug': f'p-{club_id}',
            'name': f'P{club_id}', 'club_id': club_id,
        }])
        labels = []

        def _fetch(url, label='html', context=None):
            labels.append(label)
            return '<html/>'

        monkeypatch.setattr(scraper, '_fetch_html', _fetch)

        bundle = scraper.read_squad_data('ENG-Premier League', 2025, limit=1)

        assert labels == ['listing', 'squad']
        assert len(bundle['memberships']) == 1

    def test_zero_parsed_listing_is_typed_schema_error(self, monkeypatch):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        monkeypatch.setattr(scraper, '_fetch_html', lambda *args, **kwargs: '<html/>')
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [])

        bundle = scraper.read_squad_data('ENG-Premier League', 2025)

        assert bundle['memberships'].empty
        record = scraper.get_fetch_outcomes()['listing']['ENG-Premier League:2025']
        assert record['status'] == 'schema_error'


class TestNativeCareerFacts:
    def test_market_value_points_are_global_and_legacy_is_partitioned(
        self, monkeypatch,
    ):
        scraper = TransfermarktScraper()
        monkeypatch.setattr(scraper, '_fetch_json', lambda *args, **kwargs: {
            'list': [{
                'datum_mw': 'Jan 1, 2025', 'y': 1_000_000,
                'verein': 'Club', 'age': '20', 'mw': '€1m',
            }],
        })

        points = scraper.read_market_value_points(
            'ENG-Premier League', 2025, player_ids=['9'],
        )
        legacy = materialize_legacy_market_value_history(
            points, 'ENG-Premier League', 2025,
        )

        assert len(points) == 1
        assert 'league' not in points.columns
        assert 'season' not in points.columns
        assert legacy.iloc[0]['league'] == 'ENG-Premier League'
        assert legacy.iloc[0]['season'] == '2526'

    def test_transfer_event_season_survives_requested_season_projection(
        self, monkeypatch,
    ):
        scraper = TransfermarktScraper()
        monkeypatch.setattr(scraper, '_fetch_json', lambda *args, **kwargs: {
            'transfers': [{
                'id': 'source-1', 'date': 'Jul 1, 2020', 'season': '20/21',
                'from': {'clubName': 'A', 'href': '/verein/1/'},
                'to': {'clubName': 'B', 'href': '/verein/2/'},
                'fee': '€1.00m', 'marketValue': '€2.00m',
                'upcoming': False,
            }],
        })

        events = scraper.read_transfer_events(
            'ENG-Premier League', 2025, player_ids=['9'],
        )
        legacy = materialize_legacy_transfers(
            events, 'ENG-Premier League', 2025,
        )

        assert events.iloc[0]['event_season'] == '2021'
        assert 'season' not in events.columns
        assert 'league' not in events.columns
        assert legacy.iloc[0]['season'] == '2526'

    def test_transfer_ids_are_global_across_players(self):
        payload = {'transfers': [{
            'id': 'same-upstream-id', 'date': 'Jul 1, 2020', 'season': '20/21',
            'from': {'clubName': 'A', 'href': '/verein/1/'},
            'to': {'clubName': 'B', 'href': '/verein/2/'},
        }]}

        first = _parse_transfers(payload, '1')[0]['transfer_id']
        second = _parse_transfers(payload, '2')[0]['transfer_id']

        assert first != second
        assert first == _parse_transfers(payload, '1')[0]['transfer_id']

    def test_duplicate_player_ids_are_fetched_once(self, monkeypatch):
        scraper = TransfermarktScraper()
        calls = []

        def _fetch(url, label='json', context=None):
            calls.append(context['player_id'])
            return {'list': []}

        monkeypatch.setattr(scraper, '_fetch_json', _fetch)

        scraper.read_market_value_points(
            'ENG-Premier League', 2025, player_ids=['2', '1', '2', '1'],
        )

        assert calls == ['2', '1']

    def test_nonempty_source_that_parses_zero_is_schema_error(self, monkeypatch):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        monkeypatch.setattr(scraper, '_fetch_json', lambda *args, **kwargs: {
            'list': [{'datum_mw': 'not-a-date'}],
        })
        # Keep the threshold from masking the typed record in this one-key run.
        monkeypatch.setattr(tm, '_MIN_SUCCESS_RATIO', 0)

        frame = scraper.read_market_value_points(
            'ENG-Premier League', 2025, player_ids=['3'],
        )

        assert frame.empty
        record = scraper.get_fetch_outcomes()['market_value_points']['3']
        assert record['status'] == 'schema_error'

    @pytest.mark.parametrize('entry,expected_error', [
        (
            {'datum_mw': 'not-a-date', 'x': 'not-an-epoch', 'y': 100},
            'no valid date',
        ),
        (
            {'datum_mw': 'Jan 1, 2025', 'y': 'unknown'},
            'y is not numeric',
        ),
    ])
    def test_mv_semantic_required_fields_fail_closed(
        self, monkeypatch, entry, expected_error,
    ):
        scraper = TransfermarktScraper()
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda *args, **kwargs: {'list': [entry]},
        )

        frame = scraper.read_market_value_points(
            'ENG-Premier League', 2025, player_ids=['semantic-mv'],
        )

        assert frame.empty
        outcome = scraper.get_fetch_outcomes()[
            'market_value_points'
        ]['semantic-mv']
        assert outcome['status'] == 'schema_error'
        assert expected_error in outcome['error']

    def test_empty_transfer_object_is_schema_error(self, monkeypatch):
        scraper = TransfermarktScraper()
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda *args, **kwargs: {'transfers': [{}]},
        )

        frame = scraper.read_transfer_events(
            'ENG-Premier League', 2025, player_ids=['semantic-transfer'],
        )

        assert frame.empty
        outcome = scraper.get_fetch_outcomes()[
            'transfer_events'
        ]['semantic-transfer']
        assert outcome['status'] == 'schema_error'
        assert 'event season/date' in outcome['error']

    def test_mv_partial_parser_20_source_to_1_is_schema_error(
        self, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        source = [
            {'datum_mw': f'Jan {day}, 2025', 'y': day}
            for day in range(1, 21)
        ]
        monkeypatch.setattr(
            scraper, '_fetch_json', lambda *args, **kwargs: {'list': source},
        )
        monkeypatch.setattr(tm, '_parse_mv_history', lambda payload, player_id: [{
            'player_id': player_id,
            'mv_date': date(2025, 1, 1),
            'value_eur': 1,
        }])

        frame = scraper.read_market_value_points(
            'ENG-Premier League', 2025, player_ids=['8'],
        )

        assert frame.empty
        outcome = scraper.get_fetch_outcomes()['market_value_points']['8']
        assert outcome['status'] == 'schema_error'
        assert '20 source rows produced 1 parsed rows' in outcome['error']

    def test_transfer_partial_parser_20_source_to_1_is_schema_error(
        self, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        source = [
            {
                'id': str(index),
                'date': 'Jul 1, 2025',
                'season': '25/26',
                'from': {'clubName': 'A'},
                'to': {'clubName': 'B'},
            }
            for index in range(20)
        ]
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda *args, **kwargs: {'transfers': source},
        )
        monkeypatch.setattr(tm, '_parse_transfers', lambda payload, player_id: [{
            'transfer_id': 'one',
            'player_id': player_id,
            'event_season': '2526',
        }])

        frame = scraper.read_transfer_events(
            'ENG-Premier League', 2025, player_ids=['8'],
        )

        assert frame.empty
        outcome = scraper.get_fetch_outcomes()['transfer_events']['8']
        assert outcome['status'] == 'schema_error'
        assert '20 source rows produced 1 parsed rows' in outcome['error']


class TestNativeCoachContracts:
    def test_memberships_skip_listing_and_profile_is_fetched_once_per_coach(
        self, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm

        scraper = TransfermarktScraper()
        memberships = pd.DataFrame([
            {'club_id': '1', 'club_slug': 'a', 'club_name': 'A'},
            {'club_id': '2', 'club_slug': 'b', 'club_name': 'B'},
        ])
        monkeypatch.setattr(tm, '_parse_coach_history', lambda html, club_id: [{
            'coach_id': '10', 'coach_slug': 'same-coach', 'name': 'Coach',
            'role': 'Manager', 'club_id': club_id,
            'appointed_date': date(2025, 7, 1), 'left_date': None,
        }])
        monkeypatch.setattr(tm, '_parse_coach_profile', lambda html, coach_id: {
            'coach_id': coach_id, 'name': 'Coach Full',
            'dob': date(1970, 1, 1), 'nationality': 'Spain',
        })
        resolver = MagicMock(side_effect=AssertionError('must not query Trino'))
        monkeypatch.setattr(scraper, '_resolve_coach_bios_from_bronze', resolver)
        labels = []

        def _fetch(url, label='html', context=None):
            labels.append(label)
            return '<html/>'

        monkeypatch.setattr(scraper, '_fetch_html', _fetch)

        bundle = scraper.read_coach_data(
            'ENG-Premier League', 2025,
            memberships=memberships,
            coach_profile_cache={},
        )

        assert labels.count('listing') == 0
        assert labels.count('coach_history') == 2
        assert labels.count('coach_profile') == 1
        assert len(bundle['profiles']) == 1
        assert len(bundle['stints']) == 2
        assert len(bundle['legacy_coaches']) == 2
        resolver.assert_not_called()


class TestLegacyPlayerAgeParity:
    def test_the_printed_age_survives_into_the_legacy_projection(self):
        # The dual-write parity gate compares both projections' age. Recomputing
        # it "as of now" over pages that may be a day old (the scope cache lives
        # 24h) drifts by a year for anyone whose birthday fell in between.
        from scrapers.transfermarkt.scraper import materialize_legacy_players

        observations = pd.DataFrame([{
            'player_id': '1', 'player_slug': 'x', 'name': 'X', 'position': 'CB',
            'dob': date(1993, 8, 17), 'age': 31, 'height_cm': 180,
            'foot': 'right', 'nationality': 'RU',
            'contract_until': date(2026, 6, 30), 'market_value_eur': 100000,
            'league': 'TM-2DVB', 'season': '2024', 'club_id': '12043',
            'club_name': 'Dinamo',
        }])
        memberships = pd.DataFrame([{
            'league': 'TM-2DVB', 'season': '2024', 'club_id': '12043',
            'player_id': '1',
        }])

        legacy = materialize_legacy_players(memberships, observations)

        assert legacy.iloc[0]['age'] == 31

    def test_an_age_the_source_omits_is_derived_from_the_birth_date(self):
        from scrapers.transfermarkt.scraper import materialize_legacy_players

        observations = pd.DataFrame([{
            'player_id': '1', 'player_slug': 'x', 'name': 'X', 'position': 'CB',
            'dob': date(1993, 8, 17), 'age': None, 'height_cm': 180,
            'foot': 'right', 'nationality': 'RU',
            'contract_until': date(2026, 6, 30), 'market_value_eur': 100000,
            'league': 'TM-2DVB', 'season': '2024', 'club_id': '12043',
            'club_name': 'Dinamo',
        }])
        memberships = pd.DataFrame([{
            'league': 'TM-2DVB', 'season': '2024', 'club_id': '12043',
            'player_id': '1',
        }])

        legacy = materialize_legacy_players(memberships, observations)

        assert legacy.iloc[0]['age'] > 30


class TestMoneyLocale:
    def test_a_suffix_we_do_not_know_is_an_error_not_a_thousandfold_mistake(self):
        from scrapers.transfermarkt.scraper import (
            MoneyLocaleError, _parse_tm_money_eur,
        )

        assert _parse_tm_money_eur('€1,20 Mrd.') == 1_200_000_000
        assert _parse_tm_money_eur('€900Th.') == 900_000
        with pytest.raises(MoneyLocaleError, match='unknown magnitude'):
            _parse_tm_money_eur('€500 gazillion')

    def test_a_figure_in_another_currency_cannot_become_a_eur_column(self):
        from scrapers.transfermarkt.scraper import (
            MoneyLocaleError, _parse_tm_money_eur,
        )

        with pytest.raises(MoneyLocaleError, match='not EUR'):
            _parse_tm_money_eur('$30.00m')

    def test_the_absent_figures_stay_absent(self):
        from scrapers.transfermarkt.scraper import _parse_tm_money_eur

        assert _parse_tm_money_eur('-') is None
        assert _parse_tm_money_eur('?') is None
        assert _parse_tm_money_eur('€0') == 0
