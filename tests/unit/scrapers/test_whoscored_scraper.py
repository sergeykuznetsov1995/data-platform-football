"""
Unit tests for WhoScoredScraper (FlareSolverr-backed — 2026-05).

Architecture under test:

* ``scrape_schedule`` / ``scrape_missing_players`` / ``scrape_season_stages``
  — soccerdata reader with HTTP transport swapped out for FlareSolverr via
  ``FlareSolverrWhoScoredReader`` (subclass of ``sd.WhoScored``).
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
    """Patch ``FlareSolverrWhoScoredReader`` (used for schedule /
    missing_players / season_stages). Returns the (cls_mock, instance_mock)
    pair so tests can set up per-method return values."""
    reader = MagicMock()
    reader.read_schedule.return_value = pd.DataFrame()
    reader.read_missing_players.return_value = pd.DataFrame()
    reader.read_season_stages.return_value = pd.DataFrame()

    cls_mock = MagicMock(return_value=reader)
    with patch(
        'scrapers.whoscored.flaresolverr_reader.FlareSolverrWhoScoredReader',
        cls_mock,
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
    """``_get_reader`` returns the patched FlareSolverr-backed reader."""

    def test_get_reader_uses_flaresolverr_reader(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        cls_mock, reader = mock_enhanced_whoscored
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            flaresolverr_url='http://flaresolverr:8191',
        )

        result = scraper._get_reader()
        assert result is reader

        cls_mock.assert_called_once()
        call_kwargs = cls_mock.call_args.kwargs
        assert call_kwargs['leagues'] == ['ENG-Premier League']
        assert call_kwargs['seasons'] == [2024]
        assert call_kwargs['flaresolverr_url'] == 'http://flaresolverr:8191'
        assert call_kwargs['proxy'] is None

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
class TestWhoScoredSerializeNestedColumns:
    """``_serialize_nested_columns`` defends against trino_manager's
    ``pd.isna(val)`` crash on list/dict values from soccerdata schedule."""

    def test_json_encodes_lists_and_dicts(self):
        from scrapers.whoscored import WhoScoredScraper
        df = pd.DataFrame({
            'game_id': [1, 2],
            'incidents': [[{'minute': '37', 'type': 1}], []],
            'bets': [{'home': 1.5}, {'away': 2.1}],
            'home_team': ['Arsenal', 'Chelsea'],
        })
        result = WhoScoredScraper._serialize_nested_columns(df)
        # nested cols serialised to JSON strings
        assert result['incidents'].tolist() == [
            '[{"minute": "37", "type": 1}]', '[]'
        ]
        assert result['bets'].tolist() == [
            '{"home": 1.5}', '{"away": 2.1}'
        ]
        # scalar cols unchanged
        assert result['home_team'].tolist() == ['Arsenal', 'Chelsea']
        assert result['game_id'].tolist() == [1, 2]

    def test_nulls_preserved_in_nested_columns(self):
        """NaN entries in a column with any list/dict must stay NaN."""
        from scrapers.whoscored import WhoScoredScraper
        import numpy as np
        df = pd.DataFrame({
            'incidents': [[{'a': 1}], np.nan, []],
        })
        result = WhoScoredScraper._serialize_nested_columns(df)
        assert result['incidents'][0] == '[{"a": 1}]'
        assert pd.isna(result['incidents'][1])
        assert result['incidents'][2] == '[]'


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

    def test_events_flush_serializes_qualifiers_lists(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        """Regression: the events save path must JSON-encode list columns
        (``qualifiers``) like the schedule path does, otherwise
        ``trino_manager._format_sql_value`` calls ``pd.isna(list)`` and crashes
        with "truth value of an empty array is ambiguous" on old-season events
        whose first event carries an empty ``qualifiers`` list."""
        from scrapers.whoscored import scraper as scraper_mod
        from scrapers.whoscored import events_fetcher as ef_mod
        from scrapers.whoscored import WhoScoredScraper

        meta = [_make_meta_row(9, 'ENG-Premier League', '2425', 'g9')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        def _events_df_qual(data, **kw):
            idx = pd.MultiIndex.from_tuples(
                [('ENG-Premier League', '2425', 'g9')],
                names=['league', 'season', 'game'],
            )
            return pd.DataFrame(
                {
                    'game_id': [kw['game_id']],
                    'type': ['Goal'],
                    'qualifiers': [[]],  # empty list — the crashing shape
                },
                index=idx,
            )

        save_mock = MagicMock(return_value='iceberg.bronze.whoscored_events')
        with patch.object(
            scraper, '_read_events_metadata_from_bronze', return_value=meta
        ), patch.object(
            scraper, '_fetch_existing_event_game_ids', return_value=set()
        ), patch.object(scraper, '_close_reader'), patch.object(
            scraper, 'save_to_iceberg', save_mock
        ), patch.object(
            scraper_mod, 'FlareSolverrClient',
            MagicMock(return_value=MagicMock()),
        ), patch.object(
            ef_mod, 'fetch_match_events_via_flaresolverr',
            MagicMock(return_value={'events': [{}]}),
        ), patch.object(
            ef_mod, 'parse_matchcentre_to_events_df',
            MagicMock(side_effect=_events_df_qual),
        ):
            scraper.scrape_events(match_ids=[9], chunk_size=1)

        assert save_mock.called
        saved_df = save_mock.call_args.kwargs['df']
        # qualifiers must reach save as a JSON string, not a Python list.
        assert saved_df['qualifiers'].tolist() == ['[]']

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

    def test_skip_existing_fetch_failure_raises_no_save(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        """Bug #467(1): a transient _fetch_existing_event_game_ids failure must
        propagate, not be swallowed — otherwise the whole season re-appends."""
        from scrapers.whoscored import WhoScoredScraper

        meta = [
            _make_meta_row(1, 'ENG-Premier League', '2425', 'g1'),
            _make_meta_row(2, 'ENG-Premier League', '2425', 'g2'),
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, fetch_mock = _patch_events_pipeline(
            scraper, meta, fetch_side_effect={'events': [{}]}
        )
        # Replace the default no-op _fetch_existing patch (cms[1]) with a
        # raising one to simulate a transient Trino / network error.
        raising = patch.object(
            scraper,
            '_fetch_existing_event_game_ids',
            side_effect=RuntimeError('trino unavailable'),
        )
        with cms[0], cms[2], cms[3], cms[4], cms[5], cms[6], raising:
            with pytest.raises(RuntimeError, match='trino unavailable'):
                scraper.scrape_events(
                    match_ids=[1, 2], chunk_size=1, skip_existing=True
                )

        # No matches fetched, nothing appended — we fail closed, Airflow retries.
        assert not save_mock.called
        assert fetch_mock.call_count == 0

    def test_tail_chunk_flushed_when_last_match_fails(
        self, mock_base_dependencies, mock_enhanced_whoscored, monkeypatch
    ):
        """Bug #467(2): the accumulated chunk must still be saved when the final
        match exhausts its retries (the old `i == total` flush was bypassed by
        `continue`)."""
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        from scrapers.whoscored import WhoScoredScraper

        monkeypatch.setattr(WhoScoredScraper, 'EVENTS_MAX_PROXY_RETRIES', 1)
        meta = [
            _make_meta_row(1, 'ENG-Premier League', '2425', 'g1'),
            _make_meta_row(2, 'ENG-Premier League', '2425', 'g2'),
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, fetch_mock = _patch_events_pipeline(
            scraper, meta,
            fetch_side_effect=[
                {'events': [{}]},
                FlareSolverrCFChallengeFailed('CF'),
            ],
        )
        # chunk_size > total → the ONLY save path is the post-loop flush.
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            result = scraper.scrape_events(match_ids=[1, 2], chunk_size=10)

        assert save_mock.call_count == 1
        saved_df = save_mock.call_args.kwargs['df']
        assert len(saved_df) == 1
        assert set(saved_df['game_id']) == {1}  # failed match 2 absent
        # `path` set via nonlocal → return is the table, not {}.
        assert result == {'events': 'iceberg.bronze.whoscored_events'}
        assert fetch_mock.call_count == 2

    def test_full_run_saves_via_post_loop_flush(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        """Regression guard: with chunk_size > total and every match succeeding,
        the single save comes from the post-loop flush and `path` is set."""
        from scrapers.whoscored import WhoScoredScraper

        meta = [
            _make_meta_row(i, 'ENG-Premier League', '2425', f'g{i}')
            for i in range(1, 6)
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2024])

        cms, save_mock, client, _ = _patch_events_pipeline(
            scraper, meta, fetch_side_effect={'events': [{}]}
        )
        with cms[0], cms[1], cms[2], cms[3], cms[4], cms[5], cms[6]:
            result = scraper.scrape_events(match_ids=[1, 2, 3, 4, 5], chunk_size=10)

        assert save_mock.call_count == 1
        assert len(save_mock.call_args.kwargs['df']) == 5
        assert result == {'events': 'iceberg.bronze.whoscored_events'}


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

    def test_parse_unresolved_player_id_is_nulled(self):
        """#878 regression (GER-2020): a player_id missing from
        playerIdNameDictionary used to survive ``replace()`` as a float,
        producing a mixed str/float ``player`` column that pyarrow refuses
        to write ("Expected bytes, got a 'float' object")."""
        import pandas as pd

        from scrapers.whoscored.events_fetcher import (
            parse_matchcentre_to_events_df,
        )
        data = {
            'events': [
                {'eventId': 1, 'teamId': 1, 'playerId': 100},
                # 999 is NOT in the dictionary — must become null, not 999.0
                {'eventId': 2, 'teamId': 2, 'playerId': 999},
            ],
            'playerIdNameDictionary': {'100': 'Player A'},
            'home': {'teamId': 1, 'name': 'H'},
            'away': {'teamId': 2, 'name': 'A'},
        }
        df = parse_matchcentre_to_events_df(
            data, league='X', season='2021', game_id=1, game_name='g',
        )
        assert df.iloc[0]['player'] == 'Player A'
        assert pd.isna(df.iloc[1]['player'])
        # Column must be pyarrow-safe: every value is str or null.
        assert all(
            isinstance(v, str) or pd.isna(v) for v in df['player'].tolist()
        )


@pytest.mark.unit
class TestWhoScoredSeasonHelper:
    """Mirrors soccerdata ``SeasonCode.MULTI_YEAR.parse`` for 4-digit tokens.

    The DAG passes SEASONS_STR short tokens ('2122,...,2526'); legacy callers
    pass year-start ints (2024). Both forms must land on the same bronze
    ``season`` value — the old year-only converter mapped '2526' → '2627'
    (nonexistent season), silently turning the daily events scrape into a
    no-op."""

    @pytest.mark.parametrize('season,expected', [
        # year-start form (legacy CLI --season)
        (2024, '2425'),
        (2023, '2324'),
        (1999, '9900'),
        (2099, '9900'),
        (2016, '1617'),
        # already-short YYZZ form (SEASONS_STR tokens) — passthrough
        (2526, '2526'),
        ('2526', '2526'),
        (2122, '2122'),
        (1617, '1617'),
        # ambiguous '2021' — soccerdata resolves as the 20/21 season
        (2021, '2021'),
        # year whose short form is NOT consecutive → year semantics
        (2020, '2021'),
    ])
    def test_season_to_soccerdata_str(self, season, expected):
        from scrapers.whoscored.scraper import _season_to_soccerdata_str
        assert _season_to_soccerdata_str(season) == expected


# -----------------------------------------------------------------------------
# Player profile: /Players/{id} biographical snapshot (issue #37).
# -----------------------------------------------------------------------------
# Minimal real info-label block captured by scripts/probe_whoscored_players.py.
_PLAYER_PROFILE_HTML = (
    '<div class="col12-lg-6"><span class="info-label">Name: </span>Rayan Aït-Nouri</div>'
    '<div class="col12-lg-6"><span class="info-label">Current Team: </span>'
    '<a href="/teams/167/show/england-manchester-city" class="team-link">Manchester City</a></div>'
    '<div class="col12-lg-6"><span class="info-label">Shirt Number: </span>\n  21\n</div>'
    '<div class="col12-lg-6"><span class="info-label">Age: </span>25 years old (<i>06-06-2001</i>)</div>'
    '<div class="col12-lg-6"><span class="info-label">Height: </span>180cm</div>'
    '<div class="col12-lg-6"><span class="info-label">Nationality: </span>'
    '<span class="iconize iconize-icon-left">Algeria <span class="ui-icon country flg-dz"></span></span></div>'
    '<div class="col12-lg-6"><span class="info-label">Positions: </span><span>'
    '<span style="display: inline-block;">Defender (Left),</span>'
    '<span style="display: inline-block;">Midfielder (Left)</span></span></div>'
)


@pytest.mark.unit
class TestPlayerProfileParse:
    """``parse_player_profile`` projects the DOM info-label block to a flat row."""

    def test_happy_path(self):
        from scrapers.whoscored.player_profile_fetcher import parse_player_profile

        row = parse_player_profile(
            _PLAYER_PROFILE_HTML, '355401', 'ENG-Premier League', '2526',
        )
        assert row['player_id'] == '355401'
        assert row['name'] == 'Rayan Aït-Nouri'
        assert row['current_team_id'] == '167'
        assert row['current_team_name'] == 'Manchester City'
        assert row['shirt_number'] == 21
        assert row['age'] == 25
        assert row['date_of_birth'] == '2001-06-06'  # DD-MM-YYYY -> ISO
        assert row['height_cm'] == 180
        assert row['nationality'] == 'Algeria'
        assert row['country_code'] == 'dz'
        assert 'Defender (Left)' in row['positions']
        assert row['league'] == 'ENG-Premier League'
        assert row['season'] == '2526'

    def test_garbage_returns_none(self):
        from scrapers.whoscored.player_profile_fetcher import parse_player_profile

        assert parse_player_profile(None, '1', 'L', '2526') is None
        assert parse_player_profile('', '1', 'L', '2526') is None
        assert parse_player_profile(
            '<html><body>no info-label here</body></html>', '1', 'L', '2526'
        ) is None

    def test_missing_fields_degrade_to_null(self):
        """A page with only some labels yields a row, absent fields are None."""
        from scrapers.whoscored.player_profile_fetcher import parse_player_profile

        html = (
            '<div><span class="info-label">Name: </span>John Doe</div>'
            '<div><span class="info-label">Height: </span>172cm</div>'
        )
        row = parse_player_profile(html, '99', 'ENG-Premier League', '2526')
        assert row['name'] == 'John Doe'
        assert row['height_cm'] == 172
        assert row['date_of_birth'] is None
        assert row['current_team_id'] is None
        assert row['nationality'] is None
        assert row['positions'] is None


@pytest.mark.unit
class TestScrapePlayerProfile:
    """End-to-end ``scrape_player_profile`` with FlareSolverr mocked offline."""

    def test_writes_player_profile_table(self, mock_base_dependencies):
        from scrapers.whoscored import WhoScoredScraper
        from scrapers.whoscored import scraper as scraper_mod

        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])

        client_instance = MagicMock()
        client_instance.get.return_value = {
            'status': 200, 'html': _PLAYER_PROFILE_HTML, 'cookies': [], 'userAgent': '',
        }
        client_cls = MagicMock(return_value=client_instance)
        save_mock = MagicMock(return_value='iceberg.bronze.whoscored_player_profile')

        with patch.object(scraper_mod, 'FlareSolverrClient', client_cls), \
             patch.object(scraper, 'save_to_iceberg', save_mock):
            result = scraper.scrape_player_profile(player_ids=['355401', '355402'])

        client_instance.create_session.assert_called()
        client_instance.destroy_session.assert_called()
        assert client_instance.get.call_count == 2  # one GET per player

        kwargs = save_mock.call_args.kwargs
        assert kwargs['table_name'] == 'whoscored_player_profile'
        assert kwargs['partition_cols'] == ['league', 'season']
        assert kwargs['replace_partitions'] == ['league', 'season']
        df = kwargs['df']
        assert len(df) == 2
        assert set(df['player_id']) == {'355401', '355402'}
        assert df['height_cm'].tolist() == [180, 180]
        assert df['season'].unique().tolist() == ['2526']
        assert df['_source'].iloc[0] == 'whoscored'
        assert df['_entity_type'].iloc[0] == 'player_profile'
        assert result == {'player_profile': 'iceberg.bronze.whoscored_player_profile'}

    def test_no_player_ids_returns_empty(self, mock_base_dependencies):
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])
        with patch.object(scraper, '_resolve_player_ids_from_bronze', return_value=[]):
            result = scraper.scrape_player_profile()
        assert result == {}


@pytest.mark.unit
class TestResolvePlayerIdsFromBronze:
    """player_id resolver uses the DOUBLE→BIGINT→varchar double-cast (footgun)."""

    def test_double_cast_sql_and_ids(self, mock_base_dependencies):
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])
        mgr_instance = MagicMock()
        mgr_instance._execute.return_value = [('355401',), ('355402',)]

        with patch(
            'scrapers.base.trino_manager.TrinoTableManager',
            return_value=mgr_instance,
        ):
            ids = scraper._resolve_player_ids_from_bronze(
                'ENG-Premier League', '2526', limit=5,
            )

        assert ids == ['355401', '355402']
        sql = mgr_instance._execute.call_args.args[0]
        assert 'CAST(CAST(player_id AS BIGINT) AS varchar)' in sql
        assert "season = '2526'" in sql
        assert "league = 'ENG-Premier League'" in sql
        assert 'LIMIT 5' in sql


# -----------------------------------------------------------------------------
# Lineups / ratings parser: matchCentreData home/away.players[] (issue #708).
# Field map mirrors socceraction's Opta WhoScored parser (extract_players /
# extract_playergamestats) — the same JSON the events parser already fetches.
# -----------------------------------------------------------------------------
def _mcd_fixture() -> dict:
    """Synthetic matchCentreData with the player-block shapes we parse."""
    return {
        'expandedMaxMinute': 94,
        'events': [{'id': 1}],
        'playerIdNameDictionary': {'11': 'Full Game', '12': 'Subbed Out'},
        'home': {
            'teamId': 26,
            'name': 'Liverpool',
            'incidentEvents': [],
            'players': [
                {   # starter, played the full game, MOTM
                    'playerId': 11, 'name': 'Full Game', 'shirtNo': 4,
                    'position': 'DC', 'isFirstEleven': True,
                    'isManOfTheMatch': True, 'age': 27, 'height': 191,
                    'weight': 84,
                    'stats': {'ratings': {'45': 6.9, '94': 7.71}},
                },
                {   # starter, subbed out at 70
                    'playerId': 12, 'name': 'Subbed Out', 'shirtNo': 10,
                    'position': 'FW', 'isFirstEleven': True,
                    'subbedOutExpandedMinute': 70,
                    'stats': {'ratings': {'70': 6.31}},
                },
                {   # bench, came on at 70
                    'playerId': 13, 'name': 'Sub In', 'shirtNo': 20,
                    'position': 'Sub', 'isFirstEleven': False,
                    'subbedInExpandedMinute': 70,
                    'stats': {'ratings': {'94': 6.05}},
                },
                {   # unused substitute — no events, no ratings
                    'playerId': 14, 'name': 'Unused Sub', 'shirtNo': 31,
                    'position': 'Sub', 'isFirstEleven': False,
                    'stats': {},
                },
            ],
        },
        'away': {
            'teamId': 167,
            'name': 'Manchester City',
            'incidentEvents': [
                {   # red card at 55 → minutes_played capped for player 21
                    'playerId': 21, 'expandedMinute': 55,
                    'cardType': {'value': 33, 'displayName': 'Red'},
                },
            ],
            'players': [
                {   # starter sent off at 55
                    'playerId': 21, 'name': 'Sent Off', 'shirtNo': 5,
                    'position': 'DC', 'isFirstEleven': True,
                    'stats': {'ratings': {'55': 5.2}},
                },
            ],
        },
    }


@pytest.mark.unit
class TestLineupsParser:
    def _parse(self, data):
        from scrapers.whoscored.events_fetcher import (
            parse_matchcentre_to_lineups_df,
        )
        return parse_matchcentre_to_lineups_df(
            data,
            league='ENG-Premier League',
            season='2526',
            game_id=1903117,
            game_name='2025-08-16 Liverpool-Manchester City',
        )

    def test_one_row_per_player_with_index(self):
        df = self._parse(_mcd_fixture())
        assert len(df) == 5
        assert df.index.names == ['league', 'season', 'game']
        assert set(df['game_id']) == {1903117}
        assert set(df['side']) == {'home', 'away'}
        assert set(df.loc[df['side'] == 'home', 'team']) == {'Liverpool'}
        assert set(df.loc[df['side'] == 'home', 'team_id']) == {26}

    def test_lineup_fields(self):
        df = self._parse(_mcd_fixture()).reset_index().set_index('player_id')
        assert bool(df.loc[11, 'is_starter']) is True
        assert bool(df.loc[13, 'is_starter']) is False
        assert bool(df.loc[11, 'is_man_of_the_match']) is True
        assert df.loc[11, 'shirt_no'] == 4
        assert df.loc[12, 'position'] == 'FW'
        assert df.loc[11, 'player'] == 'Full Game'

    def test_minutes_played_socceraction_semantics(self):
        df = self._parse(_mcd_fixture()).reset_index().set_index('player_id')
        assert df.loc[11, 'minutes_played'] == 94   # full game
        assert df.loc[12, 'minutes_played'] == 70   # starter subbed out
        assert df.loc[13, 'minutes_played'] == 24   # sub on 70 → 94
        assert df.loc[14, 'minutes_played'] == 0    # unused sub
        assert df.loc[21, 'minutes_played'] == 55   # red card at 55

    def test_rating_is_last_minute_value(self):
        df = self._parse(_mcd_fixture()).reset_index().set_index('player_id')
        assert df.loc[11, 'rating'] == pytest.approx(7.71)
        assert df.loc[12, 'rating'] == pytest.approx(6.31)
        assert pd.isna(df.loc[14, 'rating'])  # no ratings dict → NULL

    def test_garbage_returns_empty(self):
        assert self._parse(None).empty
        assert self._parse({}).empty
        assert self._parse({'events': [], 'home': {}, 'away': {}}).empty

    def test_numeric_columns_pinned_to_float64(self):
        """A chunk where every optional field is present must NOT come out
        int64 — the auto-created Iceberg column would be BIGINT and the next
        chunk carrying a NaN (float64) would fail the Trino INSERT."""
        df = self._parse(_mcd_fixture()).reset_index()
        for col in (
            'game_id', 'team_id', 'player_id', 'shirt_no', 'minutes_played',
            'rating', 'subbed_in_expanded_minute', 'subbed_out_expanded_minute',
            'height', 'weight', 'age',
        ):
            assert df[col].dtype == 'float64', col


# -----------------------------------------------------------------------------
# scrape_events: multi-season metadata + lineups-aware skip / dual append.
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestScrapeEventsSeasonsAndLineups:
    def test_metadata_sql_uses_short_tokens_verbatim(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        """SEASONS_STR short tokens must reach the schedule query unchanged —
        the old converter shifted them one season forward ('2526' → '2627'),
        so the daily events scrape matched zero schedule rows."""
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'], seasons=[2122, 2526],
        )
        mgr_instance = MagicMock()
        mgr_instance._execute.return_value = []
        with patch(
            'scrapers.base.trino_manager.TrinoTableManager',
            return_value=mgr_instance,
        ):
            scraper._read_events_metadata_from_bronze()

        sql = mgr_instance._execute.call_args.args[0]
        assert "'2122'" in sql and "'2526'" in sql
        assert "'2223'" not in sql and "'2627'" not in sql

    def test_existing_ids_table_parameter(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import WhoScoredScraper

        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])
        mgr_instance = MagicMock()
        mgr_instance._execute.return_value = [(1,)]
        with patch(
            'scrapers.base.trino_manager.TrinoTableManager',
            return_value=mgr_instance,
        ):
            out = scraper._fetch_existing_event_game_ids(table='whoscored_lineups')

        assert out == {1}
        sql = mgr_instance._execute.call_args.args[0]
        assert 'iceberg.bronze.whoscored_lineups' in sql
        # Scope filters stay paired so a canonical single-year season cannot
        # accidentally be cross-matched with a split-year competition.
        assert "league = 'ENG-Premier League' AND season = '2526'" in sql

    def test_lineup_only_match_refetched_without_events_reappend(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        """A match already in whoscored_events but missing from
        whoscored_lineups is refetched; only lineups rows are appended (events
        must NOT re-append — the events save is APPEND-only)."""
        from scrapers.whoscored import scraper as scraper_mod
        from scrapers.whoscored import events_fetcher as ef_mod
        from scrapers.whoscored import WhoScoredScraper

        meta = [
            _make_meta_row(1, 'ENG-Premier League', '2526', 'g1'),
            _make_meta_row(2, 'ENG-Premier League', '2526', 'g2'),
        ]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])
        save_mock = MagicMock(return_value='iceberg.bronze.whoscored_lineups')
        fetch_mock = MagicMock(return_value=_mcd_fixture())

        def _existing(table='whoscored_events'):
            return {1, 2} if table == 'whoscored_events' else {2}

        with patch.object(
            scraper, '_read_events_metadata_from_bronze', return_value=meta
        ), patch.object(
            scraper, '_fetch_existing_event_game_ids', side_effect=_existing
        ), patch.object(scraper, '_close_reader'), patch.object(
            scraper, 'save_to_iceberg', save_mock
        ), patch.object(
            scraper_mod, 'FlareSolverrClient', MagicMock(return_value=MagicMock())
        ), patch.object(
            ef_mod, 'fetch_match_events_via_flaresolverr', fetch_mock
        ):
            result = scraper.scrape_events(chunk_size=10)

        # only match 1 (events-done, lineups-missing) is fetched
        assert fetch_mock.call_count == 1
        # exactly one save — the lineups table; events untouched
        tables = [c.kwargs['table_name'] for c in save_mock.call_args_list]
        assert tables == ['whoscored_lineups']
        saved = save_mock.call_args.kwargs['df']
        assert set(saved['game_id']) == {1}
        assert result == {'lineups': 'iceberg.bronze.whoscored_lineups'}

    def test_new_match_appends_events_and_lineups(
        self, mock_base_dependencies, mock_enhanced_whoscored
    ):
        from scrapers.whoscored import scraper as scraper_mod
        from scrapers.whoscored import events_fetcher as ef_mod
        from scrapers.whoscored import WhoScoredScraper

        meta = [_make_meta_row(3, 'ENG-Premier League', '2526', 'g3')]
        scraper = WhoScoredScraper(leagues=['ENG-Premier League'], seasons=[2526])
        save_mock = MagicMock(
            side_effect=lambda **kw: f"iceberg.bronze.{kw['table_name']}"
        )
        fetch_mock = MagicMock(return_value=_mcd_fixture())
        parse_mock = MagicMock(side_effect=lambda data, **kw: _events_df(kw['game_id']))

        with patch.object(
            scraper, '_read_events_metadata_from_bronze', return_value=meta
        ), patch.object(
            scraper, '_fetch_existing_event_game_ids', return_value=set()
        ), patch.object(scraper, '_close_reader'), patch.object(
            scraper, 'save_to_iceberg', save_mock
        ), patch.object(
            scraper_mod, 'FlareSolverrClient', MagicMock(return_value=MagicMock())
        ), patch.object(
            ef_mod, 'fetch_match_events_via_flaresolverr', fetch_mock
        ), patch.object(
            ef_mod, 'parse_matchcentre_to_events_df', parse_mock
        ):
            result = scraper.scrape_events(chunk_size=10)

        tables = sorted(c.kwargs['table_name'] for c in save_mock.call_args_list)
        assert tables == ['whoscored_events', 'whoscored_lineups']
        assert result == {
            'events': 'iceberg.bronze.whoscored_events',
            'lineups': 'iceberg.bronze.whoscored_lineups',
        }
        # lineups rows carry the entity metadata for OM lineage
        lineups_df = next(
            c.kwargs['df'] for c in save_mock.call_args_list
            if c.kwargs['table_name'] == 'whoscored_lineups'
        )
        assert lineups_df['_entity_type'].iloc[0] == 'lineups'
        assert len(lineups_df) == 5


# -----------------------------------------------------------------------------
# scrape_player_profile: per-league partition tagging (multi-league fix).
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestScrapePlayerProfileMultiLeague:
    def test_rows_tagged_per_league_and_profiles_fetched_once(
        self, mock_base_dependencies
    ):
        """Old code tagged EVERY profile with leagues[0] — under a multi-league
        config all rows landed in the first league's partition. Rosters must be
        resolved per league and rows tagged accordingly; a player appearing in
        two leagues is fetched once but materialised in both partitions."""
        from scrapers.whoscored import WhoScoredScraper
        from scrapers.whoscored import scraper as scraper_mod

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League', 'ESP-La Liga'], seasons=[2526],
        )
        client_instance = MagicMock()
        client_instance.get.return_value = {
            'status': 200, 'html': _PLAYER_PROFILE_HTML,
            'cookies': [], 'userAgent': '',
        }
        save_mock = MagicMock(return_value='iceberg.bronze.whoscored_player_profile')

        def _resolve(league, season_str, limit=None):
            assert season_str == '2526'
            return {'ENG-Premier League': ['1', '2'],
                    'ESP-La Liga': ['2', '3']}[league]

        with patch.object(
            scraper_mod, 'FlareSolverrClient',
            MagicMock(return_value=client_instance),
        ), patch.object(
            scraper, '_resolve_player_ids_from_bronze', side_effect=_resolve
        ), patch.object(scraper, 'save_to_iceberg', save_mock):
            result = scraper.scrape_player_profile()

        # player '2' plays in both leagues but its page is fetched only once
        assert client_instance.get.call_count == 3

        df = save_mock.call_args.kwargs['df']
        assert len(df) == 4
        by_league = df.groupby('league')['player_id'].apply(set).to_dict()
        assert by_league == {
            'ENG-Premier League': {'1', '2'},
            'ESP-La Liga': {'2', '3'},
        }
        assert set(df['season']) == {'2526'}
        assert result == {
            'player_profile': 'iceberg.bronze.whoscored_player_profile'
        }
