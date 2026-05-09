"""
Unit tests for WhoScoredScraper (hybrid soccerdata + FlareSolverr — 2026-04).

Architecture under test:

* ``scrape_schedule`` / ``scrape_missing_players`` / ``scrape_season_stages``
  — soccerdata-backed via ``EnhancedWhoScored``.
* ``scrape_events`` — bypasses soccerdata. Pulls ``(game_id, league, season,
  game)`` from ``iceberg.bronze.whoscored_schedule``, then fetches each
  match's ``matchCentreData`` via :class:`FlareSolverrClient` (CF resolved
  once per session) and parses with ``parse_matchcentre_to_events_df``.

These tests stub every cross-module call so the suite stays fully offline.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# -----------------------------------------------------------------------------
# Shared dependency / soccerdata mocks.
# -----------------------------------------------------------------------------
@pytest.fixture
def mock_base_dependencies():
    with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
         patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
         patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
         patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

        mock_rl.return_value = MagicMock()
        mock_rl.return_value.acquire.return_value = True
        mock_rp.return_value = MagicMock()
        mock_rp.return_value.execute.side_effect = (
            lambda f, *a, **k: f(*a, **k)
        )
        mock_cb.return_value = MagicMock()
        mock_cb.return_value.call.side_effect = (
            lambda f, *a, **k: f(*a, **k)
        )

        mock_iw_instance = MagicMock()
        mock_iw_instance.write_dataframe.return_value = (
            'iceberg.bronze.test'
        )
        mock_iw.return_value = mock_iw_instance
        yield mock_iw_instance


@pytest.fixture
def mock_enhanced_whoscored():
    """Patch ``EnhancedWhoScored`` (used for schedule / missing_players /
    season_stages). Returns the (cls_mock, instance_mock) pair so tests can
    set up per-method return values."""
    reader = MagicMock()
    reader.read_schedule.return_value = pd.DataFrame()
    reader.read_missing_players.return_value = pd.DataFrame()
    reader.read_season_stages.return_value = pd.DataFrame()

    cls_mock = MagicMock(return_value=reader)
    with patch(
        'scrapers.whoscored.whoscored_patched.EnhancedWhoScored', cls_mock
    ):
        yield cls_mock, reader


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestWhoScoredInit:
    """Constructor + class hierarchy."""

    def test_instantiation(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024, 2025]
        )

        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024, 2025]
        assert scraper.SOURCE_NAME == 'whoscored'

    def test_mro_includes_soccerdatascraper(self):
        """WhoScoredScraper inherits SoccerdataScraper (not SeleniumScraper)."""
        from scrapers.base.base_scraper import SoccerdataScraper, SeleniumScraper
        from scrapers.whoscored import WhoScoredScraper

        assert SoccerdataScraper in WhoScoredScraper.__mro__
        assert SeleniumScraper not in WhoScoredScraper.__mro__

    def test_legacy_single_season_kwarg(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], season=2025
        )
        assert scraper.seasons == [2025]


@pytest.mark.unit
class TestWhoScoredGetReader:
    """``_get_reader`` returns the patched soccerdata reader."""

    def test_get_reader_uses_enhanced_whoscored(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        cls_mock, reader = mock_enhanced_whoscored
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024]
        )

        result = scraper._get_reader()
        assert result is reader

        cls_mock.assert_called_once()
        call_kwargs = cls_mock.call_args.kwargs
        assert call_kwargs['leagues'] == ['ENG-Premier League']
        assert call_kwargs['seasons'] == [2024]
        assert call_kwargs['headless'] is True

    def test_get_reader_is_cached(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        cls_mock, _ = mock_enhanced_whoscored
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024]
        )
        scraper._get_reader()
        scraper._get_reader()
        assert cls_mock.call_count == 1


@pytest.mark.unit
class TestWhoScoredScrapeSchedule:
    def test_scrape_schedule_writes_correct_table(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        _, reader = mock_enhanced_whoscored
        idx = pd.MultiIndex.from_tuples(
            [('ENG-Premier League', '2425', 'Arsenal-Wolves')],
            names=['league', 'season', 'game'],
        )
        reader.read_schedule.return_value = pd.DataFrame(
            {'game_id': [1234567]}, index=idx
        )

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024]
        )

        save_mock = MagicMock(return_value='iceberg.bronze.whoscored_schedule')
        with patch.object(scraper, 'save_to_iceberg', save_mock):
            result = scraper.scrape_schedule()

        reader.read_schedule.assert_called_once()
        kwargs = save_mock.call_args.kwargs
        assert kwargs['table_name'] == 'whoscored_schedule'
        assert kwargs['partition_cols'] == ['league', 'season']
        assert result == {'schedule': 'iceberg.bronze.whoscored_schedule'}


@pytest.mark.unit
class TestWhoScoredScrapeMissingPlayersAndSeasonStages:
    def test_scrape_missing_players(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        _, reader = mock_enhanced_whoscored
        reader.read_missing_players.return_value = pd.DataFrame({
            'player': ['Foden'], 'reason': ['injury'],
        })

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024]
        )
        save_mock = MagicMock(
            return_value='iceberg.bronze.whoscored_missing_players'
        )
        with patch.object(scraper, 'save_to_iceberg', save_mock):
            result = scraper.scrape_missing_players()

        reader.read_missing_players.assert_called_once()
        kwargs = save_mock.call_args.kwargs
        assert kwargs['table_name'] == 'whoscored_missing_players'
        assert result == {
            'missing_players': 'iceberg.bronze.whoscored_missing_players'
        }

    def test_scrape_season_stages(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        _, reader = mock_enhanced_whoscored
        reader.read_season_stages.return_value = pd.DataFrame({
            'stage': ['Regular Season'],
        })

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2024]
        )
        save_mock = MagicMock(
            return_value='iceberg.bronze.whoscored_season_stages'
        )
        with patch.object(scraper, 'save_to_iceberg', save_mock):
            result = scraper.scrape_season_stages()

        reader.read_season_stages.assert_called_once()
        kwargs = save_mock.call_args.kwargs
        assert kwargs['table_name'] == 'whoscored_season_stages'
        assert result == {
            'season_stages': 'iceberg.bronze.whoscored_season_stages'
        }


# -----------------------------------------------------------------------------
# Events: FlareSolverr-backed flow.
# -----------------------------------------------------------------------------
def _make_meta_row(game_id: int, league: str, season: str, game_name: str):
    return (game_id, league, season, game_name)


def _events_df(mid: int) -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples(
        [('ENG-Premier League', '2425', f'game-{mid}')],
        names=['league', 'season', 'game'],
    )
    return pd.DataFrame(
        {'game_id': [mid], 'minute': [10], 'type': ['Goal']},
        index=idx,
    )


def _patch_events_pipeline(scraper, meta, fetch_side_effect):
    """Patch every cross-module dep used by scrape_events."""
    from scrapers.whoscored import scraper as scraper_mod
    from scrapers.whoscored import events_fetcher as ef_mod

    save_mock = MagicMock(return_value='iceberg.bronze.whoscored_events')
    client_instance = MagicMock()
    client_cls = MagicMock(return_value=client_instance)

    fetch_mock = MagicMock()
    if isinstance(fetch_side_effect, list) or callable(fetch_side_effect):
        fetch_mock.side_effect = fetch_side_effect
    elif isinstance(fetch_side_effect, BaseException) or (
        isinstance(fetch_side_effect, type)
        and issubclass(fetch_side_effect, BaseException)
    ):
        fetch_mock.side_effect = fetch_side_effect
    else:
        fetch_mock.return_value = fetch_side_effect

    parse_mock = MagicMock(side_effect=lambda data, **kw: _events_df(kw['game_id']))

    cms = [
        patch.object(scraper, '_read_events_metadata_from_bronze', return_value=meta),
        patch.object(scraper, '_fetch_existing_event_game_ids', return_value=set()),
        patch.object(scraper, '_close_reader'),
        patch.object(scraper, 'save_to_iceberg', save_mock),
        patch.object(scraper_mod, 'FlareSolverrClient', client_cls),
        patch.object(ef_mod, 'fetch_match_events_via_flaresolverr', fetch_mock),
        patch.object(ef_mod, 'parse_matchcentre_to_events_df', parse_mock),
    ]
    return cms, save_mock, client_instance, fetch_mock


@pytest.mark.unit
class TestWhoScoredScrapeEventsViaFlaresolverr:
    """End-to-end coverage of scrape_events on top of FlareSolverr."""

    def test_constructor_accepts_flaresolverr_url(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        s = WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            flaresolverr_url='http://x:1234',
        )
        assert s.flaresolverr_url == 'http://x:1234'

        s2 = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])
        assert s2.flaresolverr_url is None

    def test_session_lifecycle_create_and_destroy(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        meta = [
            _make_meta_row(1, 'ENG-Premier League', '2425', 'g1'),
            _make_meta_row(2, 'ENG-Premier League', '2425', 'g2'),
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, fetch_mock = _patch_events_pipeline(
            scraper, meta, fetch_side_effect={'events': [{}]}
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            result = scraper.scrape_events(match_ids=[1, 2], chunk_size=1)

        assert client.create_session.call_count >= 1
        assert client.destroy_session.call_count >= 1
        assert fetch_mock.call_count == 2
        assert save_mock.called
        assert result == {'events': 'iceberg.bronze.whoscored_events'}

    def test_session_recycle_every_n_matches(
        self, mock_base_dependencies, mock_enhanced_whoscored, monkeypatch
    ):
        from scrapers.whoscored import WhoScoredScraper

        monkeypatch.setattr(WhoScoredScraper, 'EVENTS_SESSION_RECREATE_EVERY', 2)
        meta = [
            _make_meta_row(i, 'ENG-Premier League', '2425', f'g{i}')
            for i in range(1, 6)
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, _, client, _ = _patch_events_pipeline(
            scraper, meta, fetch_side_effect={'events': [{}]}
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            scraper.scrape_events(match_ids=[1, 2, 3, 4, 5], chunk_size=10)

        # initial create + 2 mid-loop recycles = 3
        assert client.create_session.call_count == 3
        # 2 mid-loop destroys + 1 final destroy = 3
        assert client.destroy_session.call_count == 3

    def test_retries_on_cf_challenge_recycles_session(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        from scrapers.whoscored import WhoScoredScraper

        meta = [_make_meta_row(7, 'ENG-Premier League', '2425', 'g7')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, _ = _patch_events_pipeline(
            scraper, meta,
            fetch_side_effect=[
                FlareSolverrCFChallengeFailed('Cloudflare challenge'),
                {'events': [{}]},
            ],
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            scraper.scrape_events(match_ids=[7], chunk_size=1)

        # initial create + recycle on CF = at least 2
        assert client.create_session.call_count >= 2
        assert save_mock.called

    def test_retries_on_timeout_recycles_session(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.base.flaresolverr_client import FlareSolverrTimeout
        from scrapers.whoscored import WhoScoredScraper

        meta = [_make_meta_row(8, 'ENG-Premier League', '2425', 'g8')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, _ = _patch_events_pipeline(
            scraper, meta,
            fetch_side_effect=[
                FlareSolverrTimeout('timeout'),
                {'events': [{}]},
            ],
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            scraper.scrape_events(match_ids=[8], chunk_size=1)

        assert client.create_session.call_count >= 2
        assert save_mock.called

    def test_no_recycle_on_generic_error(
        self, mock_base_dependencies, mock_enhanced_whoscored, monkeypatch
    ):
        from scrapers.base.flaresolverr_client import FlareSolverrError
        from scrapers.whoscored import WhoScoredScraper

        monkeypatch.setattr(WhoScoredScraper, 'EVENTS_MAX_PROXY_RETRIES', 2)
        meta = [_make_meta_row(9, 'ENG-Premier League', '2425', 'g9')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, _ = _patch_events_pipeline(
            scraper, meta, fetch_side_effect=FlareSolverrError('boom'),
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            scraper.scrape_events(match_ids=[9], chunk_size=1)

        # generic errors do NOT recycle
        assert client.create_session.call_count == 1
        # match given up — nothing saved to Iceberg
        assert not save_mock.called

    def test_gives_up_after_max_retries(
        self, mock_base_dependencies, mock_enhanced_whoscored, monkeypatch
    ):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        from scrapers.whoscored import WhoScoredScraper

        monkeypatch.setattr(WhoScoredScraper, 'EVENTS_MAX_PROXY_RETRIES', 2)
        meta = [_make_meta_row(11, 'ENG-Premier League', '2425', 'g11')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, _ = _patch_events_pipeline(
            scraper, meta, fetch_side_effect=FlareSolverrCFChallengeFailed('CF'),
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            scraper.scrape_events(match_ids=[11], chunk_size=1)

        assert not save_mock.called
        # final destroy still ran
        assert client.destroy_session.called


@pytest.mark.unit
class TestFetchMatchEventsViaFlaresolverr:
    """Standalone tests for the events_fetcher entry-point."""

    def _html_with_data(self, n: int = 1) -> str:
        events = ','.join(['{"id":1}'] * n)
        return (
            '<html><script>'
            'require.config.params["args"] = {'
            'matchId: 123, '
            'matchCentreData: {"events":[' + events + '],"home":{},"away":{}}'
            '};'
            '</script></html>'
        )

    def test_returns_data_on_status_200(self):
        from scrapers.whoscored.events_fetcher import (
            fetch_match_events_via_flaresolverr,
        )

        client = MagicMock()
        client.get.return_value = {
            'html': self._html_with_data(),
            'cookies': [], 'userAgent': 'Mozilla', 'status': 200,
        }
        out = fetch_match_events_via_flaresolverr(client, 123, 'sess-1')
        assert out is not None
        assert 'events' in out

    def test_returns_none_on_non_200(self):
        from scrapers.whoscored.events_fetcher import (
            fetch_match_events_via_flaresolverr,
        )

        client = MagicMock()
        client.get.return_value = {
            'html': '<html></html>',
            'cookies': [], 'userAgent': 'Mozilla', 'status': 403,
        }
        assert fetch_match_events_via_flaresolverr(client, 123, 'sess-1') is None

    def test_returns_none_when_match_centre_missing(self):
        from scrapers.whoscored.events_fetcher import (
            fetch_match_events_via_flaresolverr,
        )

        client = MagicMock()
        client.get.return_value = {
            'html': '<html><body>no data</body></html>',
            'cookies': [], 'userAgent': 'Mozilla', 'status': 200,
        }
        assert fetch_match_events_via_flaresolverr(client, 123, 'sess-1') is None


# -----------------------------------------------------------------------------
# Events parser standalone unit tests.
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestEventsParser:
    def test_parse_matchcentre_minimal(self):
        from scrapers.whoscored.events_fetcher import (
            parse_matchcentre_to_events_df,
        )

        data = {
            'events': [
                {
                    'eventId': 1,
                    'minute': 10,
                    'second': 5,
                    'expandedMinute': 10,
                    'type': {'displayName': 'Pass'},
                    'outcomeType': {'displayName': 'Successful'},
                    'teamId': 13,
                    'playerId': 100,
                    'x': 50.0,
                    'y': 50.0,
                    'isTouch': True,
                    'period': {'displayName': 'FirstHalf'},
                },
            ],
            'playerIdNameDictionary': {'100': 'Player A'},
            'home': {'teamId': 13, 'name': 'Arsenal'},
            'away': {'teamId': 167, 'name': 'Manchester City'},
        }
        df = parse_matchcentre_to_events_df(
            data,
            league='ENG-Premier League',
            season='2425',
            game_id=999,
            game_name='2025-04-29 Arsenal-Manchester City',
        )

        assert not df.empty
        # Index = (league, season, game)
        assert df.index.names == ['league', 'season', 'game']
        # MultiIndex first row matches inputs
        idx0 = df.index[0]
        assert idx0 == (
            'ENG-Premier League', '2425',
            '2025-04-29 Arsenal-Manchester City',
        )
        # Player + team resolution
        assert df.iloc[0]['player'] == 'Player A'
        assert df.iloc[0]['team'] == 'Arsenal'
        # Nested-dict flattening for 'type' / 'period' / 'outcome_type'
        assert df.iloc[0]['type'] == 'Pass'
        assert df.iloc[0]['period'] == 'FirstHalf'
        assert df.iloc[0]['outcome_type'] == 'Successful'

    def test_parse_empty_returns_empty_df(self):
        from scrapers.whoscored.events_fetcher import (
            parse_matchcentre_to_events_df,
        )
        for bad in (None, {}, {'events': []}):
            df = parse_matchcentre_to_events_df(
                bad,
                league='ENG-Premier League',
                season='2425',
                game_id=1,
                game_name='x',
            )
            assert df.empty

    def test_parse_camelcase_to_snake(self):
        from scrapers.whoscored.events_fetcher import (
            parse_matchcentre_to_events_df,
        )
        data = {
            'events': [{'expandedMinute': 12, 'goalMouthY': 33.0}],
            'playerIdNameDictionary': {},
            'home': {'teamId': 1, 'name': 'H'},
            'away': {'teamId': 2, 'name': 'A'},
        }
        df = parse_matchcentre_to_events_df(
            data, league='X', season='2425', game_id=1, game_name='g',
        )
        assert df.iloc[0]['expanded_minute'] == 12
        assert df.iloc[0]['goal_mouth_y'] == 33.0


@pytest.mark.unit
class TestWhoScoredSeasonHelper:
    @pytest.mark.parametrize('season,expected', [
        (2024, '2425'),
        (2023, '2324'),
        (1999, '9900'),
        (2099, '9900'),
    ])
    def test_season_to_soccerdata_str(self, season, expected):
        from scrapers.whoscored.scraper import _season_to_soccerdata_str
        assert _season_to_soccerdata_str(season) == expected
