"""
Unit tests for FotMob skip-existing incremental fetch (traffic review).

``read_match_details`` / ``read_player_details`` must re-fetch only what is
missing from the existing Bronze partition and carry the rest over unchanged
(the partition is rewritten whole under ``replace_partitions``). Any Bronze
read failure falls back to the current full-scrape behaviour — never worse.
Mirrors the pure-logic style of ``test_fotmob_keep_last_good.py``.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


LEAGUE = 'ENG-Premier League'
SEASON = 2025


def _league_payload(match_ids):
    return {'fixtures': {'allMatches': [
        {'id': mid,
         'status': {'finished': True, 'utcTime': '2025-08-10T12:00:00Z',
                    'scoreStr': '1 - 0', 'reason': {'long': 'Full-Time'}},
         'home': {'name': 'H', 'id': 1}, 'away': {'name': 'A', 'id': 2}}
        for mid in match_ids
    ]}}


def _match_details(match_id):
    return {
        'page_url': f'/matches/h-vs-a/xx#{match_id}',
        'content': {
            'lineup': {'l': match_id}, 'matchFacts': {'events': [{'e': 1}]},
            'stats': {'s': match_id}, 'playerStats': {'p': 1},
            'shotmap': {'sm': 1}, 'h2h': {'h': 1}, 'momentum': {'m': 1},
        },
    }


def _existing_matches(rows):
    """Bronze partition frame: (match_id, stats_json) pairs, varchar ids."""
    return pd.DataFrame([
        {'match_id': str(mid), 'stats_json': stats, 'home_team': 'H',
         'league': LEAGUE, 'season': SEASON}
        for mid, stats in rows
    ])


def _player_payload(pid):
    return {'pageProps': {'data': {'id': pid, 'name': f'P{pid}'}}}


@pytest.fixture
def scraper():
    from scrapers.fotmob import FotMobScraper
    s = FotMobScraper(leagues=[LEAGUE], seasons=[SEASON])
    s._session = MagicMock()
    s._iceberg_writer = MagicMock()
    yield s
    s.close()


class TestMatchDetailsSkipExisting:

    def test_fetches_only_missing_and_empty(self, scraper):
        """Matches with a good stats_json are skipped; missing/empty are fetched."""
        scraper._iceberg_writer.read_table.return_value = _existing_matches([
            (101, '{"s": 101}'),   # good → skip
            (102, ''),             # empty → re-fetch
        ])
        fetched = []

        def fake_details(mid):
            fetched.append(mid)
            return _match_details(mid)

        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101, 102, 103])), \
             patch.object(scraper, '_fetch_match_details',
                          side_effect=fake_details):
            df = scraper.read_match_details(LEAGUE, SEASON)

        assert fetched == [102, 103]
        # merged partition: carried-over 101 + re-fetched 102 + new 103
        assert sorted(df['match_id'].astype(str)) == ['101', '102', '103']
        carried = df[df['match_id'].astype(str) == '101'].iloc[0]
        assert carried['stats_json'] == '{"s": 101}'

    def test_noop_when_partition_complete(self, scraper):
        """All finished matches already good in Bronze → no fetch, no write."""
        scraper._iceberg_writer.read_table.return_value = _existing_matches([
            (101, '{"s": 101}'), (102, '{"s": 102}'),
        ])
        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101, 102])), \
             patch.object(scraper, '_fetch_match_details') as fmd:
            df = scraper.read_match_details(LEAGUE, SEASON)

        assert df is None
        fmd.assert_not_called()

    def test_bronze_read_failure_falls_back_to_full_scrape(self, scraper):
        """Trino down / first run → full scrape, exactly today's behaviour."""
        scraper._iceberg_writer.read_table.side_effect = RuntimeError('no trino')
        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101, 102])), \
             patch.object(scraper, '_fetch_match_details',
                          side_effect=lambda mid: _match_details(mid)) as fmd:
            df = scraper.read_match_details(LEAGUE, SEASON)

        assert fmd.call_count == 2
        assert len(df) == 2

    def test_carries_over_rows_absent_from_schedule(self, scraper):
        """Existing Bronze rows never disappear, even if the schedule drops them."""
        scraper._iceberg_writer.read_table.return_value = _existing_matches([
            (999, '{"s": 999}'),   # good, but no longer in fixtures
        ])
        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101])), \
             patch.object(scraper, '_fetch_match_details',
                          side_effect=lambda mid: _match_details(mid)):
            df = scraper.read_match_details(LEAGUE, SEASON)

        assert sorted(df['match_id'].astype(str)) == ['101', '999']

    def test_failed_refetch_keeps_existing_identity_row(self, scraper):
        """A re-fetched match whose fetch fails keeps its old Bronze row (#544)."""
        scraper._iceberg_writer.read_table.return_value = _existing_matches([
            (102, ''),             # empty → re-fetch, which then fails
        ])
        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101, 102])), \
             patch.object(scraper, '_fetch_match_details',
                          side_effect=lambda mid: _match_details(mid) if mid == 101 else None):
            df = scraper.read_match_details(LEAGUE, SEASON)

        assert sorted(df['match_id'].astype(str)) == ['101', '102']

    def test_reads_partition_with_league_season_filter(self, scraper):
        scraper._iceberg_writer.read_table.return_value = pd.DataFrame()
        with patch.object(scraper, '_get_league_data',
                          return_value=_league_payload([101])), \
             patch.object(scraper, '_fetch_match_details',
                          side_effect=lambda mid: _match_details(mid)):
            scraper.read_match_details(LEAGUE, SEASON)

        kwargs = scraper._iceberg_writer.read_table.call_args.kwargs
        assert LEAGUE in kwargs['filter_expr']
        assert str(SEASON) in kwargs['filter_expr']


class TestPlayerDetailsSkipExisting:

    def _existing_players(self, pids):
        return pd.DataFrame([
            {'player_id': str(pid), 'name': f'P{pid}',
             'league': LEAGUE, 'season': SEASON}
            for pid in pids
        ])

    def test_fetches_only_new_players(self, scraper):
        scraper._iceberg_writer.read_table.return_value = self._existing_players([1])
        fetched = []

        def fake_payload(path):
            pid = int(path.rsplit('/', 1)[1])
            fetched.append(pid)
            return _player_payload(pid)

        with patch.object(scraper, '_player_ids_for_league',
                          return_value=[1, 2, 3]), \
             patch.object(scraper, '_fetch_next_data_payload',
                          side_effect=fake_payload):
            df = scraper.read_player_details(LEAGUE, SEASON)

        assert fetched == [2, 3]
        assert sorted(df['player_id'].astype(str)) == ['1', '2', '3']

    def test_merged_player_id_stays_string_typed(self, scraper):
        """Bronze player_id is varchar; the source hands back an int.

        Concatenating a re-read partition (str) with fresh rows (int) used to
        yield a mixed object column that pyarrow refused to write:
        "Expected bytes, got a 'int' object" (#930 canary, 2026-07-12).
        """
        scraper._iceberg_writer.read_table.return_value = self._existing_players([1])

        with patch.object(scraper, '_player_ids_for_league',
                          return_value=[1, 2]), \
             patch.object(scraper, '_fetch_next_data_payload',
                          side_effect=lambda p: _player_payload(int(p.rsplit('/', 1)[1]))):
            df = scraper.read_player_details(LEAGUE, SEASON)

        assert {type(value) for value in df['player_id']} == {str}
        assert sorted(df['player_id']) == ['1', '2']

    def test_noop_when_no_new_players(self, scraper):
        scraper._iceberg_writer.read_table.return_value = self._existing_players([1, 2])
        with patch.object(scraper, '_player_ids_for_league',
                          return_value=[1, 2]), \
             patch.object(scraper, '_fetch_next_data_payload') as fnd:
            df = scraper.read_player_details(LEAGUE, SEASON)

        assert df is None
        fnd.assert_not_called()

    def test_full_players_disables_skip(self):
        """full_players=True re-fetches everyone without touching Bronze."""
        from scrapers.fotmob import FotMobScraper
        scraper = FotMobScraper(leagues=[LEAGUE], seasons=[SEASON],
                                full_players=True)
        scraper._session = MagicMock()
        scraper._iceberg_writer = MagicMock()
        try:
            with patch.object(scraper, '_player_ids_for_league',
                              return_value=[1, 2]), \
                 patch.object(scraper, '_fetch_next_data_payload',
                              side_effect=lambda p: _player_payload(int(p.rsplit('/', 1)[1]))):
                df = scraper.read_player_details(LEAGUE, SEASON)
        finally:
            scraper.close()

        scraper._iceberg_writer.read_table.assert_not_called()
        assert len(df) == 2

    def test_bronze_read_failure_falls_back_to_full_scrape(self, scraper):
        scraper._iceberg_writer.read_table.side_effect = RuntimeError('no trino')
        with patch.object(scraper, '_player_ids_for_league',
                          return_value=[1, 2]), \
             patch.object(scraper, '_fetch_next_data_payload',
                          side_effect=lambda p: _player_payload(int(p.rsplit('/', 1)[1]))) as fnd:
            df = scraper.read_player_details(LEAGUE, SEASON)

        assert fnd.call_count == 2
        assert len(df) == 2
