"""Unit tests for scrapers.transfermarkt.

Covers pure parsers and helpers. End-to-end HTTP / Iceberg paths are
exercised by ``scripts/probe_transfermarkt.py`` (live) and the DAG smoke
runs in a container — not pytest.
"""

from datetime import date

import pytest

from scrapers.transfermarkt import TransfermarktScraper
from scrapers.transfermarkt.scraper import (
    ConsecutiveFailureError,
    R0_2B_FALLBACK_MARKER,
    TM_LEAGUE_MAP,
    _coerce_int,
    _extract_club_id_from_href,
    _parse_club_listing,
    _parse_height_cm,
    _parse_mv_history,
    _parse_player_profile,
    _parse_squad_page,
    _parse_tm_date,
    _parse_tm_money_eur,
    _parse_transfers,
    _season_short,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestSeasonShort:
    @pytest.mark.parametrize("year,expected", [
        (2024, '2425'),
        (2025, '2526'),
        (2099, '9900'),  # wrap
    ])
    def test_four_digit_year(self, year, expected):
        assert _season_short(year) == expected

    def test_str_input_treated_as_year(self):
        # `_season_short` always shortens — '2526' is treated as the (invalid
        # but defensible) year 2526 → '2627'. Callers should pass an int year.
        assert _season_short('2526') == '2627'


class TestParseMoneyEur:
    @pytest.mark.parametrize("raw,expected", [
        ('€ 45.00 m', 45_000_000),
        ('€45.00m', 45_000_000),
        ('€500k', 500_000),
        ('€ 1.20bn', 1_200_000_000),
        ('€1.20b', 1_200_000_000),
        # Note: TM .us uses period as decimal separator. European-format
        # comma-decimal (`€80,00m`) is not supported — would parse as 8B.
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


class TestParsePlayerProfile:
    def test_happy_path(self):
        html = """
        <html><body>
        <h1 class="data-header__headline-wrapper"><span>#25</span>Gianluigi Donnarumma</h1>
        <a class="data-header__market-value-wrapper">€ 45.00 m Last update: Dec 9, 2025</a>
        <span itemprop="birthDate">Feb 25, 1999 (26)</span>
        <span itemprop="height">1,96 m</span>
        <span itemprop="nationality">Italy</span>
        <dd class="detail-position__position">Goalkeeper</dd>
        <span class="data-header__club">Man City</span>
        <span class="info-table__content--regular">Foot:</span>
        <span class="info-table__content--bold">right</span>
        <span class="info-table__content--regular">Contract expires:</span>
        <span class="info-table__content--bold">Jun 30, 2030</span>
        </body></html>
        """
        bio = _parse_player_profile(html, '315858')
        assert bio is not None
        assert bio['player_id'] == '315858'
        assert bio['name'] == 'Gianluigi Donnarumma'
        assert bio['position'] == 'Goalkeeper'
        assert bio['dob'] == date(1999, 2, 25)
        assert bio['height_cm'] == 196
        assert bio['foot'] == 'right'
        assert bio['nationality'] == 'Italy'
        assert bio['current_club_name'] == 'Man City'
        assert bio['contract_until'] == date(2030, 6, 30)
        assert bio['market_value_eur'] == 45_000_000
        assert bio['market_value_last_update'] == date(2025, 12, 9)

    def test_missing_name_returns_none(self):
        # No <h1.data-header__headline-wrapper> means the page didn't load
        # or selector drifted — we'd rather propagate None than guess.
        assert _parse_player_profile('<html><body></body></html>', '1') is None

    def test_partial_fields_ok(self):
        html = '<h1 class="data-header__headline-wrapper">Player Name</h1>'
        bio = _parse_player_profile(html, '1')
        assert bio is not None
        assert bio['name'] == 'Player Name'
        assert bio['dob'] is None
        assert bio['height_cm'] is None
        assert bio['market_value_eur'] is None


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

    def test_datum_missing_falls_back_to_epoch_x(self):
        # Epoch ms is UTC; TM's `datum_mw` is wall-clock CET. When we
        # synthesise the date from `x` alone we land on the UTC day.
        payload = {'list': [{
            'x': 1425250800000, 'y': 300000, 'mw': '€300k',
            'verein': 'Club', 'age': '16',
        }]}
        rows = _parse_mv_history(payload, '1')
        assert len(rows) == 1
        assert rows[0]['mv_date'] == date(2015, 3, 1)

    def test_empty_list(self):
        assert _parse_mv_history({'list': []}, '1') == []
        assert _parse_mv_history({}, '1') == []
        assert _parse_mv_history(None, '1') == []  # type: ignore[arg-type]


class TestParseTransfers:
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
        assert TM_LEAGUE_MAP['ENG-Premier League'] == ('premier-league', 'GB1')

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

    def test_read_players_raises_on_consecutive_profile_failures(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': '11', 'club_slug': 'fc-arsenal', 'club_name': 'Arsenal'},
        ])
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [
            {'player_id': str(i), 'player_slug': f'player-{i}',
             'name': f'P{i}', 'club_id': club_id, 'market_value_eur': None}
            for i in range(5)
        ])
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None:
                None if label == 'profile' else '<html/>',
        )
        with pytest.raises(
            ConsecutiveFailureError, match='consecutive profile failures',
        ):
            scraper.read_players(league='ENG-Premier League', season=2025)

    def test_read_mv_history_raises_on_consecutive_failures(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: None,
        )
        with pytest.raises(
            ConsecutiveFailureError, match='consecutive mv_history failures',
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
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: None,
        )
        with pytest.raises(
            ConsecutiveFailureError, match='consecutive transfers failures',
        ):
            scraper.read_transfers(
                league='ENG-Premier League', season=2025,
                player_ids=['1', '2', '3', '4'],
            )

    def test_counter_reset_prevents_raise(self, scraper, monkeypatch):
        # 2 failures then a success, repeated — the counter never reaches
        # the cap of 3, so no raise and the frame materialises.
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        responses = iter([None, None, {'list': []}] * 3)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_mv_history', lambda payload, player_id: [
            {'player_id': player_id, 'value_eur': 100},
        ])
        df = scraper.read_market_value_history(
            league='ENG-Premier League', season=2025,
            player_ids=[str(i) for i in range(9)],
        )
        assert not df.empty
        assert df['player_id'].nunique() == 3  # every 3rd fetch succeeded
