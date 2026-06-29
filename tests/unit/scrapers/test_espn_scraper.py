"""
Tests for ESPNScraper.
"""

from pathlib import Path

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestESPNScraper:
    """Tests for ESPNScraper."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_iw.return_value = MagicMock()

            yield

    @pytest.fixture
    def make_scraper(self, mock_dependencies):
        """Factory: build an ESPNScraper for arbitrary seasons."""
        def _make(seasons):
            with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
                from scrapers.espn import ESPNScraper
                return ESPNScraper(leagues=['ENG-Premier League'], seasons=seasons)
        return _make

    @pytest.fixture
    def scraper(self, make_scraper):
        """Create ESPNScraper instance."""
        return make_scraper([2024])

    @staticmethod
    def _mock_reader():
        """A soccerdata-reader stand-in with the attributes the COVID-2021
        calendar seed touches (espn.py uses these exact names)."""
        reader = MagicMock()
        reader._selected_leagues = {'ENG-Premier League': 'eng.1'}
        reader.data_dir = Path('/tmp/espn_test')
        return reader

    def test_init(self, scraper):
        """Test ESPNScraper initialization."""
        assert scraper.SOURCE_NAME == 'espn'

    def test_league_ids(self, scraper):
        """Test ESPN league IDs are defined."""
        assert scraper.LEAGUE_IDS['ENG-Premier League'] == 'eng.1'
        assert scraper.LEAGUE_IDS['ESP-La Liga'] == 'esp.1'

    def test_standardize_schedule(self, scraper):
        """Test schedule standardization."""
        df = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
        })

        result = scraper._standardize_schedule(df)

        assert 'match_date' in result.columns
        assert 'home_goals' in result.columns

    def test_read_schedule_raises_on_reader_error(self, scraper):
        """Issue #466: read_schedule must propagate reader errors instead of
        returning None — a swallowed exception leaves the runner's
        results['errors'] empty -> exit 0 -> green DAG on total failure."""
        with patch.object(scraper, '_get_reader', return_value=MagicMock()), \
             patch.object(scraper, '_execute_with_resilience',
                          side_effect=RuntimeError('CF block')):
            with pytest.raises(RuntimeError, match='CF block'):
                scraper.read_schedule()

    def test_scrape_schedule_uses_replace_partitions(self, scraper):
        """Regression #347: scrape_schedule MUST pass replace_partitions=['league',
        'season'] so daily writes replace each partition instead of appending
        (else espn_schedule accumulates ~31x duplicates in the active season)."""
        # Arrange
        mock_schedule = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
            'league': ['ENG-Premier League'],
            'season': [2425],
        })

        # Act
        with patch.object(scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                scraper.scrape_schedule()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['replace_partitions'] == ['league', 'season']

    def test_scrape_schedule_arms_completeness_guard(self, scraper):
        """#583: scrape_schedule MUST arm the replace guard (min_replace_ratio
        0.9) so a partial scrape can't wipe a good espn_schedule partition."""
        # Arrange
        mock_schedule = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
            'league': ['ENG-Premier League'],
            'season': [2425],
        })

        # Act
        with patch.object(scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                scraper.scrape_schedule()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['min_replace_ratio'] == 0.9

    def test_read_lineup_skips_malformed_match(self, scraper):
        """#713: soccerdata's bulk read_lineup aborts the whole season on one
        match with malformed JSON (KeyError 'displayName'). read_lineup must
        iterate per match and skip only the broken one — good matches still land."""
        sched = pd.DataFrame({'game_id': [101, 102, 103]})
        good1 = pd.DataFrame({'player': ['A'], 'team': ['X']})
        good3 = pd.DataFrame({'player': ['B'], 'team': ['Y']})

        reader = MagicMock()

        def fake_lineup(match_id=None):
            if match_id == 102:
                raise KeyError('displayName')
            return good1 if match_id == 101 else good3

        reader.read_lineup.side_effect = fake_lineup

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_execute_with_resilience', return_value=sched), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            result = scraper.read_lineup()

        # Malformed match 102 skipped (no raise); 101 + 103 concatenated.
        assert result is not None
        assert len(result) == 2
        assert set(result['player']) == {'A', 'B'}

    def test_seed_2021_calendar_overrides_covid_anchor(self, make_scraper):
        """#817: soccerdata anchors the ESPN season calendar at July 1 of the
        start year (espn.py: 20{skey[:2]}0701). For 2020-21 (season code '2021')
        that's 2020-07-01, which falls INSIDE the COVID-extended 2019-20 PL
        season (ended 2020-07-26) — so ESPN returns the 2019-20 calendar. The
        seed must overwrite that calendar cache file with a post-COVID anchor
        (2020-08-01) so soccerdata reads the real 2020-21 dates."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_called_once()
        url = reader.get.call_args.args[0]
        seed_fp = reader.get.call_args.args[1]
        assert 'eng.1' in url
        assert 'dates=20200801' in url
        assert Path(seed_fp) == reader.data_dir / 'Schedule_eng.1_20200701.json'
        assert reader.get.call_args.kwargs['no_cache'] is True

    def test_seed_2021_calendar_noop_for_other_seasons(self, make_scraper):
        """The COVID workaround must touch ONLY season '2021' — the daily run
        (season '2526') and every other backfill season are left untouched."""
        scraper = make_scraper(['2526'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_not_called()

    def test_seed_2021_calendar_runs_at_most_once(self, make_scraper):
        """read_schedule and the per-match readers each seed defensively; the
        helper must be idempotent so it re-downloads the calendar at most once
        per process."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        scraper._seed_2021_season_calendar(reader)
        scraper._seed_2021_season_calendar(reader)

        reader.get.assert_called_once()

    def test_read_schedule_seeds_2021_calendar_before_read(self, make_scraper):
        """Wiring: read_schedule must seed the 2020-21 calendar before handing
        off to soccerdata — otherwise soccerdata reads the wrong (2019-20)
        calendar and the seed never takes effect."""
        scraper = make_scraper(['2021'])
        reader = self._mock_reader()

        with patch.object(scraper, '_get_reader', return_value=reader), \
             patch.object(scraper, '_seed_2021_season_calendar') as mock_seed, \
             patch.object(scraper, '_execute_with_resilience',
                          return_value=pd.DataFrame()), \
             patch.object(scraper, '_add_metadata', side_effect=lambda d, e: d):
            scraper.read_schedule()

        mock_seed.assert_called_once_with(reader)
