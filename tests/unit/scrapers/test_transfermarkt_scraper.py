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
    PartialScrapeError,
    R0_2B_FALLBACK_MARKER,
    TM_LEAGUE_MAP,
    _coerce_int,
    _extract_club_id_from_href,
    _parse_club_listing,
    _parse_coach_history,
    _parse_coach_profile,
    _parse_height_cm,
    _parse_mv_history,
    _parse_player_profile,
    _parse_squad_page,
    _parse_staff_managers,
    _parse_tm_date,
    _parse_tm_money_eur,
    _parse_transfers,
    _season_short,
    _season_window,
    _stint_overlaps_season,
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
        # _MIN_SUCCESS_RATIO is disabled: this scenario (ratio 1/3) now
        # trips PartialScrapeError (#484), tested separately — here we
        # isolate the counter-reset behaviour of the consecutive cap.
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_MAX_CONSECUTIVE_FAILURES', 3)
        monkeypatch.setattr(tm, '_MIN_SUCCESS_RATIO', 0)
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

    def _patch_players_pipeline(self, monkeypatch, scraper, profile_responses):
        import scrapers.transfermarkt.scraper as tm
        monkeypatch.setattr(tm, '_parse_club_listing', lambda html: [
            {'club_id': '11', 'club_slug': 'fc-arsenal', 'club_name': 'Arsenal'},
        ])
        monkeypatch.setattr(tm, '_parse_squad_page', lambda html, club_id: [
            {'player_id': str(i), 'player_slug': f'player-{i}',
             'name': f'P{i}', 'club_id': club_id, 'market_value_eur': None}
            for i in range(len(profile_responses))
        ])
        responses = iter(profile_responses)
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None:
                next(responses) if label == 'profile' else '<html/>',
        )

    def test_read_players_raises_on_low_success_ratio(
        self, scraper, monkeypatch,
    ):
        # alternating failures: ratio 0.5 < 0.9, never 2 consecutive
        self._patch_players_pipeline(
            monkeypatch, scraper, [None, '<html/>'] * 5,
        )
        with pytest.raises(PartialScrapeError, match='player profiles'):
            scraper.read_players(league='ENG-Premier League', season=2025)

    def test_read_players_passes_on_high_success_ratio(
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
        responses = iter([None, {'list': []}] * 5)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_mv_history', lambda payload, player_id: [
            {'player_id': player_id, 'value_eur': 100},
        ])
        with pytest.raises(PartialScrapeError, match='mv_history'):
            scraper.read_market_value_history(
                league='ENG-Premier League', season=2025,
                player_ids=[str(i) for i in range(10)],
            )

    def test_read_transfers_raises_on_low_success_ratio(
        self, scraper, monkeypatch,
    ):
        import scrapers.transfermarkt.scraper as tm
        responses = iter([None, {'transfers': []}] * 5)
        monkeypatch.setattr(
            scraper, '_fetch_json',
            lambda url, label='json', context=None: next(responses),
        )
        monkeypatch.setattr(tm, '_parse_transfers', lambda payload, player_id: [
            {'player_id': player_id, 'fee_eur': 100},
        ])
        with pytest.raises(PartialScrapeError, match='transfers'):
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

    def test_current_club_name_comes_from_season_squad_not_profile(
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
        # Profile reports the club the player moved to after this season.
        monkeypatch.setattr(
            tm, '_parse_player_profile',
            lambda payload, player_id: {
                'name': 'Ederson', 'current_club_name': 'Fenerbahçe',
            },
        )
        monkeypatch.setattr(
            scraper, '_fetch_html',
            lambda url, label='html', context=None: '<html/>',
        )

        df = scraper.read_players(league='ENG-Premier League', season=2018)

        assert len(df) == 1
        row = df.iloc[0]
        assert row['current_club_id'] == '281'
        # Name must agree with the id → season squad club, not the profile.
        assert row['current_club_name'] == 'Man City'


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
    from unittest.mock import MagicMock

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


# ---------------------------------------------------------------------------
# Coach parsers (issue #434)
# ---------------------------------------------------------------------------

def _staff_row(slug: str, cid: str, name: str, role: str) -> str:
    """One staff person: name in an inline-table, role on the second line."""
    return (
        '<tr><td>'
        '<table class="inline-table"><tr>'
        '<td><img></td>'
        f'<td class="hauptlink"><a href="/{slug}/profil/trainer/{cid}">{name}</a></td>'
        '</tr><tr><td></td><td>'
        f'{role}</td></tr></table>'
        '</td><td class="zentriert">50</td></tr>'
    )


def _staff_html(coaching_rows: str) -> str:
    """A staff page: a 'Coaching Staff' section then an empty 'Management' one."""
    return (
        '<html><body>'
        '<div class="content-box-headline">Coaching Staff</div>'
        f'<table class="items"><tbody>{coaching_rows}</tbody></table>'
        '<div class="content-box-headline">Management</div>'
        '<table class="items"><tbody></tbody></table>'
        '</body></html>'
    )


class TestParseStaffManagers:
    def test_keeps_only_manager_role(self):
        rows = (
            _staff_row('pep-guardiola', '5672', 'Pep Guardiola', 'Manager')
            + _staff_row('kolo-toure', '56390', 'Kolo Touré', 'Assistant Manager')
            + _staff_row('richard-wright', '93678', 'Richard Wright', 'Goalkeeping Coach')
        )
        mgrs = _parse_staff_managers(_staff_html(rows), club_id='281')
        assert len(mgrs) == 1, mgrs
        m = mgrs[0]
        assert m['coach_id'] == '5672'
        assert m['coach_slug'] == 'pep-guardiola'
        assert m['name'] == 'Pep Guardiola'
        assert m['role'] == 'Manager'
        assert m['club_id'] == '281'

    def test_no_coaching_staff_section_returns_empty(self):
        html = (
            '<html><body><div class="content-box-headline">Management</div>'
            '<table class="items"></table></body></html>'
        )
        assert _parse_staff_managers(html, club_id='1') == []

    def test_dedups_by_coach_id(self):
        rows = (
            _staff_row('pep-guardiola', '5672', 'Pep Guardiola', 'Manager')
            + _staff_row('pep-guardiola', '5672', 'Pep Guardiola', 'Manager')
        )
        assert len(_parse_staff_managers(_staff_html(rows), club_id='281')) == 1

    def test_garbage_input(self):
        assert _parse_staff_managers('', club_id='1') == []
        assert _parse_staff_managers('<not-html>', club_id='1') == []


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

        start, end = _season_window(2025)
        assert start == datetime.date(2025, 7, 1)
        assert end == datetime.date(2026, 6, 30)


class TestStintOverlapsSeason:
    def _win(self):
        return _season_window(2025)  # (2025-07-01, 2026-06-30)

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

    def test_undated_stint_kept(self):
        s, e = self._win()
        assert _stint_overlaps_season(
            {'appointed_date': None, 'left_date': None}, s, e
        ) is True
