"""
Tests for UnderstatScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestUnderstatScraper:
    """Tests for UnderstatScraper."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_rp.return_value.execute.side_effect = lambda f, *a, **k: f(*a, **k)

            mock_cb.return_value = MagicMock()
            mock_cb.return_value.call.side_effect = lambda f, *a, **k: f(*a, **k)

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def mock_soccerdata_understat(self):
        """Mock soccerdata Understat reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_schedule.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'home_team': ['Arsenal'],
                'home_xg': [2.5],
            })
            reader.read_shot_events.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'player': ['Haaland'],
                'xg': [0.75],
                'result': ['Goal'],
            })
            reader.read_player_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'player': ['Haaland'],
                'xg': [15.5],
            })
            reader.read_team_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'team': ['Man City'],
                'xg': [75.5],
            })

            sd.Understat.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_understat):
        """Create UnderstatScraper instance."""
        from scrapers.understat import UnderstatScraper

        return UnderstatScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024]
        )

    def test_init(self, scraper):
        """Test UnderstatScraper initialization."""
        assert 'ENG-Premier League' in scraper.leagues
        assert 2024 in scraper.seasons

    def test_source_name(self, scraper):
        """Test source name is set correctly."""
        assert scraper.SOURCE_NAME == 'understat'

    def test_supported_leagues_filter(self, mock_dependencies, mock_soccerdata_understat):
        """Test that unsupported leagues are filtered out."""
        from scrapers.understat import UnderstatScraper

        scraper = UnderstatScraper(
            leagues=['ENG-Premier League', 'USA-MLS'],  # MLS not supported
            seasons=[2024]
        )

        assert 'ENG-Premier League' in scraper.leagues
        assert 'USA-MLS' not in scraper.leagues

    def test_read_schedule(self, scraper, mock_soccerdata_understat):
        """Test reading schedule with xG."""
        df = scraper.read_schedule()

        assert df is not None
        assert 'home_xg' in df.columns

    def test_read_shots(self, scraper, mock_soccerdata_understat):
        """Test reading shot events."""
        df = scraper.read_shot_events()

        assert df is not None
        assert 'xg' in df.columns
        assert 'result' in df.columns

    def test_read_player_stats(self, scraper, mock_soccerdata_understat):
        """Test reading player stats."""
        df = scraper.read_player_season_stats()

        assert df is not None
        assert 'xg' in df.columns

    def test_scrape_shots(self, scraper, mock_soccerdata_understat):
        """Test scraping shots."""
        result = scraper.scrape_shots()

        assert 'shots' in result

    def test_scrape_all(self, scraper, mock_soccerdata_understat):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)

    @pytest.mark.parametrize('method', [
        'read_schedule',
        'read_player_season_stats',
        'read_player_match_stats',
        'read_team_match_stats',
    ])
    def test_read_methods_raise_on_reader_error(self, scraper, method):
        """Issue #466: read_* must propagate reader errors instead of
        returning None — a swallowed exception leaves the runner's
        results['errors'] empty -> exit 0 -> green DAG on total failure."""
        with patch.object(scraper, '_get_reader', return_value=MagicMock()), \
             patch.object(scraper, '_execute_with_resilience',
                          side_effect=RuntimeError('boom')):
            with pytest.raises(RuntimeError, match='boom'):
                getattr(scraper, method)()

    def test_read_shot_events_raises_on_league_error(self, scraper,
                                                     mock_soccerdata_understat):
        """Issue #466: per-league loop must not silently skip a failed league."""
        with patch.object(scraper, '_execute_with_resilience',
                          side_effect=RuntimeError('boom')):
            with pytest.raises(RuntimeError, match='boom'):
                scraper.read_shot_events()

    # -- #444: assist_player_id re-derived from name -------------------------
    # soccerdata 1.8.8 fills shot ``assist_player_id`` from the roster-ROW id
    # (``player["id"]``), not the true player id. ``read_shot_events`` must
    # re-derive it from the assister NAME via this scrape's own shooter
    # (player→player_id) pairs, so Bronze no longer carries bogus roster ids.

    @staticmethod
    def _buggy_shots_df():
        return pd.DataFrame({
            'league': ['ENG-Premier League'] * 3,
            'season': [2024] * 3,
            'player': ['Mohamed Salah', 'Cody Gakpo', 'Virgil van Dijk'],
            'player_id': pd.array([11, 12, 13], dtype='Int64'),
            # row0: assisted by a shooter (Gakpo, id 12) -> remaps to 12
            # row1: no assist -> NA
            # row2: assisted by a NON-shooter (absent here) -> NA (honest)
            'assist_player': ['Cody Gakpo', None, 'Trent Alexander-Arnold'],
            'assist_player_id': pd.array([500001, None, 500002], dtype='Int64'),
            'xg': [0.3, 0.1, 0.05],
            'result': ['Goal', 'Saved Shot', 'Missed Shot'],
        })

    def test_assist_id_remapped_from_name(self, scraper, mock_soccerdata_understat):
        mock_soccerdata_understat.read_shot_events.return_value = self._buggy_shots_df()
        df = scraper.read_shot_events()
        row = df[df['player'] == 'Mohamed Salah'].iloc[0]
        assert row['assist_player_id'] == 12          # Cody Gakpo's true id
        assert row['assist_player'] == 'Cody Gakpo'   # name preserved

    def test_assist_no_assist_stays_na(self, scraper, mock_soccerdata_understat):
        mock_soccerdata_understat.read_shot_events.return_value = self._buggy_shots_df()
        df = scraper.read_shot_events()
        row = df[df['player'] == 'Cody Gakpo'].iloc[0]
        assert pd.isna(row['assist_player_id'])

    def test_assist_non_shooter_is_na_not_bogus(self, scraper, mock_soccerdata_understat):
        """Assister who took no shot in this scrape can't be derived → NA, NOT
        the bogus roster id that soccerdata produced."""
        mock_soccerdata_understat.read_shot_events.return_value = self._buggy_shots_df()
        df = scraper.read_shot_events()
        row = df[df['player'] == 'Virgil van Dijk'].iloc[0]
        assert pd.isna(row['assist_player_id'])

    def test_assist_no_roster_ids_survive(self, scraper, mock_soccerdata_understat):
        """None of the bogus roster ids (500001/500002) may remain anywhere."""
        mock_soccerdata_understat.read_shot_events.return_value = self._buggy_shots_df()
        df = scraper.read_shot_events()
        ids = set(df['assist_player_id'].dropna().tolist())
        assert ids == {12}


class TestUnderstatSupportedLeagues:
    """Tests for Understat supported leagues."""

    def test_supported_leagues_list(self):
        """Test supported leagues are defined."""
        from scrapers.understat import UnderstatScraper

        assert 'ENG-Premier League' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'ESP-La Liga' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'GER-Bundesliga' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'ITA-Serie A' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'FRA-Ligue 1' in UnderstatScraper.SUPPORTED_LEAGUES
