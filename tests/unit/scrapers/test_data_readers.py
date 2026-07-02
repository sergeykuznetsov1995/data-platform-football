"""
Tests for FBrefDataReaderMixin failure scenarios.

Covers:
- _read_schedule_from_iceberg when Trino is down
- _batch_save_match_data JSON fallback when Trino fails
- scrape_combined_match_data pre-flight Trino probe
"""

import json
import os
import tempfile

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Minimal stub that inherits FBrefDataReaderMixin for isolated testing
# ---------------------------------------------------------------------------

class _StubScraper:
    """Minimal class that provides attributes expected by FBrefDataReaderMixin."""

    def __init__(self):
        self.leagues = ['ENG-Premier League']
        self.seasons = [2025]
        self._stats = {}
        self._iceberg_writer = None
        self._page_cache = {}
        self.use_nodriver = False
        self._nodriver_browser = None
        self.BATCH_SAVE_INTERVAL = 50

    def _add_metadata(self, df, entity_type):
        return df

    def _cleanup_after_league(self):
        pass

    def _extract_match_ids(self, schedule_df, max_matches):
        return []

    def _fetch_page(self, url, **kwargs):
        return None

    def save_to_iceberg(self, df, table_name, partition_cols=None,
                        replace_partitions=None):
        return f'iceberg.bronze.{table_name}'


# Dynamically mix in FBrefDataReaderMixin
from scrapers.fbref.data_readers import FBrefDataReaderMixin


class StubScraper(_StubScraper, FBrefDataReaderMixin):
    pass


# ===========================================================================
# Test: _read_schedule_from_iceberg — Trino down
# ===========================================================================

class TestReadScheduleFromIcebergTrinoDown:
    """Test that _read_schedule_from_iceberg logs properly when Trino is down."""

    def test_connection_refused_logged_as_error(self, caplog):
        scraper = StubScraper()
        mock_writer = MagicMock()
        mock_writer.table_exists.side_effect = Exception(
            "Connection refused on port 8443"
        )
        scraper._iceberg_writer = mock_writer

        import logging
        with caplog.at_level(logging.ERROR):
            result = scraper._read_schedule_from_iceberg('ENG-Premier League', 2025)

        assert result is None
        assert any('Trino unavailable' in msg for msg in caplog.messages)

    def test_trino_error_logged_with_traceback(self, caplog):
        scraper = StubScraper()
        mock_writer = MagicMock()

        # Simulate TrinoError (has trino in module path)
        from scrapers.base.trino_manager import TrinoError
        mock_writer.table_exists.side_effect = TrinoError("query failed")
        scraper._iceberg_writer = mock_writer

        import logging
        with caplog.at_level(logging.ERROR):
            result = scraper._read_schedule_from_iceberg('ENG-Premier League', 2025)

        assert result is None
        # Should log with exc_info=True (traceback)
        assert any('Trino' in msg or 'query' in msg for msg in caplog.messages)

    def test_unexpected_error_logged_as_warning(self, caplog):
        scraper = StubScraper()
        mock_writer = MagicMock()
        mock_writer.table_exists.side_effect = ValueError("bad data")
        scraper._iceberg_writer = mock_writer

        import logging
        with caplog.at_level(logging.WARNING):
            result = scraper._read_schedule_from_iceberg('ENG-Premier League', 2025)

        assert result is None
        assert any('unexpected error' in msg for msg in caplog.messages)


# ===========================================================================
# Test: _batch_save_match_data — JSON fallback
# ===========================================================================

class TestBatchSaveMatchDataFallback:
    """Test that _batch_save_match_data saves to JSON when Iceberg fails."""

    def test_fallback_json_created_on_trino_error(self, tmp_path):
        scraper = StubScraper()

        # Make save_to_iceberg raise connection error
        scraper.save_to_iceberg = MagicMock(
            side_effect=Exception("Connection refused on port 8443")
        )

        shot_events = [
            pd.DataFrame({
                'match_id': ['abc123'],
                'league': ['ENG-Premier League'],
                'season': [2025],
                'player': ['Haaland'],
                'xg': [0.8],
            })
        ]
        results = {}

        with patch('scrapers.fbref.data_readers.time') as mock_time:
            mock_time.time.return_value = 1234567890

            scraper._batch_save_match_data(
                all_shot_events=shot_events,
                all_match_events=[],
                all_lineups=[],
                results=results,
            )

        # Verify fallback file path recorded in results
        assert 'shot_events_fallback' in results
        fallback_path = results['shot_events_fallback']
        assert os.path.exists(fallback_path)

        # Verify data is readable
        df = pd.read_json(fallback_path, orient='records')
        assert len(df) == 1
        assert df['player'].iloc[0] == 'Haaland'

        # Clean up
        os.unlink(fallback_path)

    def test_lists_cleared_after_fallback(self):
        scraper = StubScraper()

        scraper.save_to_iceberg = MagicMock(
            side_effect=Exception("Failed to connect to Trino")
        )

        lineups = [
            pd.DataFrame({
                'match_id': ['m1'],
                'league': ['ENG-Premier League'],
                'season': [2025],
                'player': ['Saka'],
            })
        ]
        results = {}

        scraper._batch_save_match_data(
            all_shot_events=[],
            all_match_events=[],
            all_lineups=lineups,
            results=results,
        )

        # Lists should be cleared even on failure
        assert len(lineups) == 0

        # Clean up fallback file
        if 'lineups_fallback' in results:
            try:
                os.unlink(results['lineups_fallback'])
            except OSError:
                pass

    def test_successful_save_no_fallback(self):
        scraper = StubScraper()

        events = [
            pd.DataFrame({
                'match_id': ['m1'],
                'league': ['ENG-Premier League'],
                'season': [2025],
                'event_type': ['goal'],
            })
        ]
        results = {}

        scraper._batch_save_match_data(
            all_shot_events=[],
            all_match_events=events,
            all_lineups=[],
            results=results,
        )

        # Should save normally, no fallback
        assert 'match_events' in results
        assert 'match_events_fallback' not in results

    def test_match_tables_saved_with_match_id_replace_partitions(self):
        """#231/#216: every match table must use replace_partitions=['match_id']
        for per-match idempotency (matches lacking player_stats re-scrape every
        run). shot_events stays None — bronze.fbref_shot_events never exists."""
        scraper = StubScraper()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )

        def _row(**extra):
            return pd.DataFrame({
                'match_id': ['m1'], 'league': ['ENG-Premier League'],
                'season': [2025], **{k: [v] for k, v in extra.items()},
            })

        scraper._batch_save_match_data(
            all_shot_events=[],
            all_match_events=[_row(event_type='goal')],
            all_lineups=[_row(player='p1')],
            results={},
            all_match_team_stats=[_row(possession=55)],
            all_match_player_stats=[_row(player='p1', goals=1)],
            all_match_managers=[_row(side='home', manager_name='Régis Le Bris')],
        )

        by_table = {
            c.kwargs.get('table_name'): c.kwargs.get('replace_partitions')
            for c in scraper.save_to_iceberg.call_args_list
        }
        for table in (
            'fbref_match_events', 'fbref_lineups', 'fbref_match_team_stats',
            'fbref_match_player_stats', 'fbref_match_managers',
        ):
            assert by_table[table] == ['match_id'], table


# ===========================================================================
# Test: scrape_single_stat_type — season-grain replace_partitions (#536)
# ===========================================================================

class TestSingleStatReplacePartitions:
    """#536: scrape_single_stat_type must save with replace_partitions=
    ['league', 'season']. Without it the weekly single_stat DAG tasks
    plain-append a full copy of each (league, season) every run — observed
    45-50x bloat in fbref_player_{misc,shooting,playingtime}, fbref_team_*
    and fbref_keeper_* (same root cause as #468, different code path)."""

    @pytest.mark.parametrize('data_category, read_method', [
        ('player', 'read_player_season_stats'),
        ('team', 'read_team_season_stats'),
        ('keeper', 'read_keeper_stats'),
    ])
    @patch('scrapers.fbref.data_readers.time.sleep', return_value=None)
    def test_single_stat_saved_with_league_season_replace_partitions(
        self, _sleep, data_category, read_method
    ):
        scraper = StubScraper()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )
        df = pd.DataFrame({
            'squad': ['Arsenal'],
            'league': ['ENG-Premier League'],
            'season': [2025],
        })
        setattr(scraper, read_method, MagicMock(return_value=df))

        scraper.scrape_single_stat_type(
            stat_type='stats', data_category=data_category
        )

        assert scraper.save_to_iceberg.call_count == 1
        assert scraper.save_to_iceberg.call_args.kwargs.get(
            'replace_partitions'
        ) == ['league', 'season']

    @patch('scrapers.fbref.data_readers.time.sleep', return_value=None)
    def test_single_stat_arms_completeness_guard(self, _sleep):
        """#583: the season-grain save arms the completeness guard
        (min_replace_ratio=0.9, raw COUNT(*) — no replace_guard_key);
        force_replace=True disarms it."""
        scraper = StubScraper()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )
        df = pd.DataFrame({
            'squad': ['Arsenal'],
            'league': ['ENG-Premier League'],
            'season': [2025],
        })
        scraper.read_team_season_stats = MagicMock(return_value=df)

        scraper.scrape_single_stat_type(stat_type='stats', data_category='team')
        armed = scraper.save_to_iceberg.call_args.kwargs
        scraper.scrape_single_stat_type(
            stat_type='stats', data_category='team', force_replace=True
        )
        forced = scraper.save_to_iceberg.call_args.kwargs

        assert armed.get('min_replace_ratio') == 0.9
        assert 'replace_guard_key' not in armed
        assert forced.get('min_replace_ratio') is None


# ===========================================================================
# Test: scrape_combined_match_data — pre-flight Trino probe
# ===========================================================================

class TestCombinedMatchDataPreflightProbe:
    """Test that scrape_combined_match_data does a pre-flight Trino probe."""

    def test_trino_available_recorded_in_stats(self):
        scraper = StubScraper()

        mock_writer = MagicMock()
        mock_writer.table_exists.return_value = True
        scraper._iceberg_writer = mock_writer

        # Run — will exit early because _extract_match_ids returns []
        scraper.scrape_combined_match_data(max_matches=1)

        assert scraper._stats.get('trino_available') is True

    def test_trino_unavailable_recorded_in_stats(self):
        scraper = StubScraper()

        mock_writer = MagicMock()
        mock_writer.table_exists.side_effect = Exception("Connection refused")
        # Pre-flight probe uses _get_trino_manager().connection.cursor().execute()
        mock_writer._get_trino_manager.return_value.connection.cursor.return_value \
            .execute.side_effect = Exception("Connection refused")
        scraper._iceberg_writer = mock_writer

        scraper.scrape_combined_match_data(max_matches=1)

        assert scraper._stats.get('trino_available') is False

    def test_schedule_source_none_when_trino_down_and_no_file(self, caplog):
        scraper = StubScraper()

        # Trino probe fails
        mock_writer = MagicMock()
        mock_writer.table_exists.side_effect = Exception("Connection refused")
        scraper._iceberg_writer = mock_writer

        import logging
        with caplog.at_level(logging.WARNING):
            result = scraper.scrape_combined_match_data(max_matches=1)

        # No data should be returned
        assert result == {}
        # Schedule source should be 'none'
        assert scraper._stats.get('schedule_source') == 'none'


# ===========================================================================
# Test: TrinoTableManager — fast-fail cache
# ===========================================================================

class TestTrinoTableManagerFastFail:
    """Test that TrinoTableManager._trino_unreachable prevents retries."""

    def setup_method(self):
        """Reset class-level cache before each test."""
        from scrapers.base.trino_manager import TrinoTableManager
        TrinoTableManager._trino_unreachable = False

    def teardown_method(self):
        """Reset class-level cache after each test."""
        from scrapers.base.trino_manager import TrinoTableManager
        TrinoTableManager._trino_unreachable = False

    @patch('trino.dbapi.connect')
    def test_fast_fail_after_unreachable(self, mock_connect):
        from scrapers.base.trino_manager import TrinoTableManager, TrinoError

        # Simulate connection failure
        mock_connect.side_effect = Exception("Connection refused")

        manager = TrinoTableManager(host='trino', port=8443)

        with pytest.raises(TrinoError, match="Failed to connect"):
            _ = manager.connection

        # Now the class-level flag should be set
        assert TrinoTableManager._trino_unreachable is True

        # Second attempt should fail fast without retrying
        manager2 = TrinoTableManager(host='trino', port=8443)
        with pytest.raises(TrinoError, match="fast-fail"):
            _ = manager2.connection

        # connect should have been called only during the first manager's retries
        # (3 retries), not again for manager2
        assert mock_connect.call_count == 3

    @patch('trino.dbapi.connect')
    def test_successful_connection_resets_cache(self, mock_connect):
        from scrapers.base.trino_manager import TrinoTableManager

        # Set unreachable flag
        TrinoTableManager._trino_unreachable = True

        # But make connection succeed now
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Need to bypass the fast-fail for this test by resetting before connect
        TrinoTableManager._trino_unreachable = False

        manager = TrinoTableManager(host='trino', port=8443)
        _ = manager.connection

        # Flag should be reset
        assert TrinoTableManager._trino_unreachable is False


# ===========================================================================
# Test: _batch_save_match_data — retry double-append dedup (#468)
# ===========================================================================

class TestBatchSaveRetryDedup:
    """#468: a partial match retried within the same flush window must not
    save two copies of its frames — replace_partitions=['match_id'] DELETE
    only cleans prior table rows, not in-frame duplicates."""

    @staticmethod
    def _frame(match_id, marker):
        return pd.DataFrame({
            'match_id': [match_id], 'league': ['ENG-Premier League'],
            'season': [2025], 'pass_marker': [marker],
        })

    def test_partial_match_retry_saves_exactly_one_copy(self):
        scraper = StubScraper()
        scraper._fetch_page = MagicMock(return_value='<html></html>')
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )

        all_shot_events, all_match_events, all_lineups = [], [], []
        all_team_stats, all_player_stats, all_managers = [], [], []
        process_args = (
            'm1', 'ENG-Premier League', 2025,
            all_shot_events, all_match_events, all_lineups,
            all_team_stats, all_player_stats, all_managers,
        )

        # First pass: lineups/events parse OK but match_player_stats is
        # missing → the match is 'partial' and gets retried. Retry: full set.
        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.parse_shots_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_events_from_scorebox',
                   side_effect=[self._frame('m1', 'first'),
                                self._frame('m1', 'retry')]), \
             patch('scrapers.fbref.data_readers.parse_lineup_table',
                   side_effect=[self._frame('m1', 'first'),
                                self._frame('m1', 'retry')]), \
             patch('scrapers.fbref.data_readers.parse_team_match_stats_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_player_match_stats_tables',
                   side_effect=[None, self._frame('m1', 'retry')]), \
             patch('scrapers.fbref.data_readers.parse_match_managers',
                   return_value=None):
            first_pass = scraper._process_single_match(*process_args)
            retry_pass = scraper._process_single_match(*process_args)

        # Bug precondition reproduced: partial first pass, both copies buffered
        assert 'match_player_stats' not in first_pass
        assert 'match_player_stats' in retry_pass
        assert len(all_lineups) == 2

        scraper._batch_save_match_data(
            all_shot_events, all_match_events, all_lineups,
            results={},
            all_match_team_stats=all_team_stats,
            all_match_player_stats=all_player_stats,
            all_match_managers=all_managers,
        )

        saved = {
            c.kwargs['table_name']: c.kwargs['df']
            for c in scraper.save_to_iceberg.call_args_list
        }
        for table in ('fbref_lineups', 'fbref_match_events'):
            df = saved[table]
            assert len(df) == 1, f'{table}: duplicate rows saved for m1'
            assert df['pass_marker'].tolist() == ['retry'], (
                f'{table}: retry frame must win'
            )
        assert len(saved['fbref_match_player_stats']) == 1

    def test_batch_save_dedups_buffered_frames_per_match(self):
        scraper = StubScraper()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )
        lineups = [
            self._frame('m1', 'first'),
            self._frame('m1', 'retry'),
            self._frame('m2', 'first'),
            # Empty frame must be skipped, not crash the dedup loop
            pd.DataFrame(columns=['match_id', 'league', 'season', 'pass_marker']),
        ]

        scraper._batch_save_match_data(
            all_shot_events=[], all_match_events=[], all_lineups=lineups,
            results={},
        )

        df = scraper.save_to_iceberg.call_args.kwargs['df']
        assert sorted(df['match_id']) == ['m1', 'm2']
        assert df.set_index('match_id').loc['m1', 'pass_marker'] == 'retry'


# ===========================================================================
# Test: keeper match stats wiring (parse → buffer → batch save)
# ===========================================================================

class TestKeeperMatchStatsWiring:
    """fbref_match_keeper_stats: _process_single_match fills the buffer and
    _batch_save_match_data writes it with replace_partitions=['match_id']."""

    @staticmethod
    def _keeper_frame(match_id):
        return pd.DataFrame({
            'match_id': [match_id], 'league': ['ENG-Premier League'],
            'season': [2025], 'Player': ['Robin Roefs'],
            'gk_saves': ['2'], 'team_side': ['home'],
        })

    def test_process_single_match_fills_keeper_buffer(self):
        scraper = StubScraper()
        scraper._fetch_page = MagicMock(return_value='<html></html>')

        keeper_buffer = []
        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.parse_shots_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_events_from_scorebox',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_lineup_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_keeper_match_stats_tables',
                   return_value=self._keeper_frame('m1')) as keeper_mock:
            got = scraper._process_single_match(
                'm1', 'ENG-Premier League', 2025,
                [], [], [],
                all_match_keeper_stats=keeper_buffer,
            )

        keeper_mock.assert_called_once()
        assert 'match_keeper_stats' in got
        assert len(keeper_buffer) == 1
        assert keeper_buffer[0]['match_id'].iloc[0] == 'm1'

    def test_keeper_parser_skipped_without_buffer(self):
        """Backward compat: no keeper buffer → parser not invoked."""
        scraper = StubScraper()
        scraper._fetch_page = MagicMock(return_value='<html></html>')

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.parse_shots_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_events_from_scorebox',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_lineup_table',
                   return_value=None), \
             patch('scrapers.fbref.data_readers.parse_keeper_match_stats_tables',
                   return_value=self._keeper_frame('m1')) as keeper_mock:
            got = scraper._process_single_match(
                'm1', 'ENG-Premier League', 2025,
                [], [], [],
            )

        keeper_mock.assert_not_called()
        assert 'match_keeper_stats' not in got

    def test_batch_save_writes_keeper_table_with_match_id_replace(self):
        scraper = StubScraper()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )

        results = {}
        scraper._batch_save_match_data(
            [], [], [],
            results=results,
            all_match_keeper_stats=[self._keeper_frame('m1')],
        )

        calls = {
            c.kwargs['table_name']: c.kwargs
            for c in scraper.save_to_iceberg.call_args_list
        }
        assert 'fbref_match_keeper_stats' in calls
        assert calls['fbref_match_keeper_stats']['replace_partitions'] == ['match_id']
        assert calls['fbref_match_keeper_stats']['partition_cols'] == ['league', 'season']
        assert results['match_keeper_stats'] == 'iceberg.bronze.fbref_match_keeper_stats'


# ===========================================================================
# Test: scrape_combined_season_stats — one fetch per page, 9 tables
# ===========================================================================

class TestScrapeCombinedSeasonStats:
    """Combined season pass: 5 unique page fetches per (league, season),
    both player and squad tables parsed from the same HTML, each of the 9
    bronze tables saved independently with the completeness guard."""

    @staticmethod
    def _frame():
        return pd.DataFrame({'Player': ['Some Player'], 'value': [1]})

    def _scraper(self):
        scraper = StubScraper()
        scraper._fetch_page = MagicMock(return_value='<html></html>')
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )
        return scraper

    @pytest.mark.unit
    def test_five_fetches_nine_saves(self):
        scraper = self._scraper()

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.find_player_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.find_team_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.time.sleep'):
            result = scraper.scrape_combined_season_stats()

        # 5 unique season pages: stats, shooting, playingtime, misc, keepers
        assert scraper._fetch_page.call_count == 5
        urls = [c.args[0] for c in scraper._fetch_page.call_args_list]
        assert len(set(urls)) == 5

        saved_tables = sorted(
            c.kwargs['table_name']
            for c in scraper.save_to_iceberg.call_args_list
        )
        assert saved_tables == sorted([
            'fbref_player_stats', 'fbref_player_shooting',
            'fbref_player_playingtime', 'fbref_player_misc',
            'fbref_team_stats', 'fbref_team_shooting',
            'fbref_team_playingtime', 'fbref_team_misc',
            'fbref_keeper_keeper',
        ])
        assert len(result['tables']) == 9
        assert result['guard_refusals'] == []
        assert result['errors'] == []

        # Guard semantics identical to scrape_single_stat_type (#513/#583)
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['replace_partitions'] == ['league', 'season']
            assert c.kwargs['partition_cols'] == ['league', 'season']
            assert c.kwargs['min_replace_ratio'] == 0.9

    @pytest.mark.unit
    def test_force_replace_disables_guard(self):
        scraper = self._scraper()

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.find_player_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.find_team_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.time.sleep'):
            scraper.scrape_combined_season_stats(force_replace=True)

        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['min_replace_ratio'] is None

    @pytest.mark.unit
    def test_guard_refusal_does_not_block_other_tables(self):
        from scrapers.base.base_scraper import ReplaceGuardError

        scraper = self._scraper()

        def _save(df, table_name, **kw):
            if table_name == 'fbref_player_stats':
                raise ReplaceGuardError('new=2 < 90% of existing=380')
            return f'iceberg.bronze.{table_name}'

        scraper.save_to_iceberg = MagicMock(side_effect=_save)

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.find_player_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.find_team_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.time.sleep'):
            result = scraper.scrape_combined_season_stats()

        assert len(result['tables']) == 8
        assert 'player_stats' not in result['tables']
        assert len(result['guard_refusals']) == 1
        assert 'fbref_player_stats' in result['guard_refusals'][0]

    @pytest.mark.unit
    def test_playingtime_squad_url_fallback(self):
        """Squad playingtime missing on the player page → ONE extra fetch of
        the dedicated /playing_time/ squad URL."""
        scraper = self._scraper()

        # find_team_stats_table call order: stats, shooting,
        # playingtime (player page -> None), playingtime (squad page), misc
        team_results = [self._frame(), self._frame(), None,
                        self._frame(), self._frame()]

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.find_player_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.find_team_stats_table',
                   side_effect=team_results), \
             patch('scrapers.fbref.data_readers.time.sleep'):
            result = scraper.scrape_combined_season_stats()

        # 5 season pages + 1 squad playing_time fallback
        assert scraper._fetch_page.call_count == 6
        urls = [c.args[0] for c in scraper._fetch_page.call_args_list]
        assert any('playing_time' in u for u in urls)
        assert 'team_playingtime' in result['tables']
        assert len(result['tables']) == 9

    @pytest.mark.unit
    def test_fetch_failure_skips_page_not_run(self):
        """A failed page fetch skips that page's tables but the run and the
        other pages continue."""
        scraper = self._scraper()

        def _fetch(url, **kw):
            if '/shooting/' in url:
                return None
            return '<html></html>'

        scraper._fetch_page = MagicMock(side_effect=_fetch)

        with patch('scrapers.fbref.data_readers.extract_tables_from_comments',
                   return_value={}), \
             patch('scrapers.fbref.data_readers.find_player_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.find_team_stats_table',
                   side_effect=lambda *a, **k: self._frame()), \
             patch('scrapers.fbref.data_readers.time.sleep'):
            result = scraper.scrape_combined_season_stats()

        assert 'player_shooting' not in result['tables']
        assert 'team_shooting' not in result['tables']
        assert 'player_stats' in result['tables']
        assert len(result['tables']) == 7


# ===========================================================================
# Test: no-summary tombstones (#A5)
# ===========================================================================

class TestNoStatsTombstone:
    """Matches whose page lacks stats_*_summary in both passes of a run get a
    tombstone row in bronze.fbref_match_no_stats; after NO_STATS_TOMBSTONE_RUNS
    confirmed runs they join the incremental skip set."""

    @staticmethod
    def _writer_with_tombstones(no_stats_df):
        writer = MagicMock()
        writer.table_exists.side_effect = (
            lambda db, table: table == 'fbref_match_no_stats'
        )
        writer.read_table.return_value = no_stats_df
        return writer

    @pytest.mark.unit
    def test_skips_only_after_enough_confirmations(self):
        scraper = StubScraper()
        # m1 confirmed in 3 runs (>= threshold), m2 only in 2 → keep retrying
        scraper._iceberg_writer = self._writer_with_tombstones(pd.DataFrame({
            'match_id': ['m1'] * 3 + ['m2'] * 2,
        }))

        sets_ = scraper._load_match_id_sets('ENG-Premier League', 2025)

        assert sets_['no_stats'] == {'m1'}
        assert sets_['player_stats'] == set()

    @pytest.mark.unit
    def test_missing_tombstone_table_yields_empty_set(self):
        scraper = StubScraper()
        writer = MagicMock()
        writer.table_exists.return_value = False
        scraper._iceberg_writer = writer

        sets_ = scraper._load_match_id_sets('ENG-Premier League', 2025)

        assert sets_['no_stats'] == set()

    @pytest.mark.unit
    def test_existing_ids_union_includes_tombstones(self):
        scraper = StubScraper()
        scraper._load_match_id_sets = MagicMock(return_value={
            'player_stats': {'a'}, 'lineups': set(), 'no_stats': {'b'},
        })

        assert scraper._get_existing_match_ids('L', 2025) == {'a', 'b'}
