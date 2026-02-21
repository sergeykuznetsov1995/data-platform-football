"""
Tests for SoccerdataFBrefScraper.

Unit tests for the lightweight soccerdata-based FBref scraper with Tor support.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, PropertyMock


@pytest.fixture
def mock_base_dependencies():
    """Mock all base scraper dependencies."""
    with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
         patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
         patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
         patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

        mock_rl.return_value = MagicMock()
        mock_rl.return_value.acquire.return_value = True
        mock_rl.return_value.available_tokens = 10

        mock_rp.return_value = MagicMock()
        mock_rp.return_value.execute.side_effect = lambda f, *a, **k: f(*a, **k)

        mock_cb.return_value = MagicMock()
        mock_cb.return_value.call.side_effect = lambda f, *a, **k: f(*a, **k)
        mock_cb.return_value.state = 'closed'

        mock_iw_instance = MagicMock()
        mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
        mock_iw.return_value = mock_iw_instance

        yield {
            'rate_limiter': mock_rl,
            'retry_policy': mock_rp,
            'circuit_breaker': mock_cb,
            'iceberg_writer': mock_iw_instance,
        }


@pytest.fixture
def mock_soccerdata():
    """Mock soccerdata library."""
    mock_sd = MagicMock()

    # Mock FBref reader
    mock_reader = MagicMock()

    # Sample DataFrames
    mock_reader.read_schedule.return_value = pd.DataFrame({
        'date': ['2024-08-01', '2024-08-02'],
        'home_team': ['Arsenal', 'Chelsea'],
        'away_team': ['Liverpool', 'Man City'],
        'score': ['2-1', '0-0'],
    })

    mock_reader.read_player_season_stats.return_value = pd.DataFrame({
        'player': ['Saka', 'Salah', 'Haaland'],
        'team': ['Arsenal', 'Liverpool', 'Man City'],
        'goals': [10, 15, 20],
        'assists': [8, 10, 5],
    })

    mock_reader.read_team_season_stats.return_value = pd.DataFrame({
        'team': ['Arsenal', 'Liverpool', 'Man City'],
        'points': [50, 48, 55],
        'wins': [15, 14, 17],
    })

    mock_sd.FBref.return_value = mock_reader

    with patch.dict('sys.modules', {'soccerdata': mock_sd}):
        yield {
            'module': mock_sd,
            'reader': mock_reader,
        }


class TestSoccerdataFBrefScraperInit:
    """Tests for SoccerdataFBrefScraper initialization."""

    def test_init_with_default_params(self, mock_base_dependencies, mock_soccerdata):
        """Test initialization with default parameters."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert scraper.SOURCE_NAME == 'fbref'
        assert scraper.use_tor is True  # Default

    def test_init_with_tor_enabled(self, mock_base_dependencies, mock_soccerdata):
        """Test initialization with Tor enabled."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=True,
            tor_host='tor',
            tor_port=9050,
        )

        assert scraper.use_tor is True
        assert scraper.tor_host == 'tor'
        assert scraper.tor_port == 9050
        # Proxy should be configured for Tor
        assert scraper._sd_proxy is not None
        assert 'socks5h://' in scraper._sd_proxy or scraper._sd_proxy == 'tor'

    def test_init_with_tor_disabled(self, mock_base_dependencies, mock_soccerdata):
        """Test initialization with Tor disabled."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=False,
        )

        assert scraper.use_tor is False

    def test_init_with_custom_tor_host(self, mock_base_dependencies, mock_soccerdata):
        """Test initialization with custom Tor host."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=True,
            tor_host='custom-tor-host',
            tor_port=9150,
        )

        assert 'socks5h://custom-tor-host:9150' in scraper._sd_proxy

    def test_init_with_localhost_tor(self, mock_base_dependencies, mock_soccerdata):
        """Test initialization with localhost Tor."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=True,
            tor_host='localhost',
        )

        # Should use 'tor' shortcut for localhost
        assert scraper._sd_proxy == 'tor'


class TestSoccerdataFBrefScraperProxyConfig:
    """Tests for proxy configuration."""

    def test_configure_proxy_with_tor(self, mock_base_dependencies, mock_soccerdata):
        """Test proxy configuration with Tor."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=True,
            tor_host='tor',
            tor_port=9050,
        )

        proxy = scraper._configure_proxy()
        assert proxy == 'socks5h://tor:9050'

    def test_configure_proxy_without_tor_no_proxies(self, mock_base_dependencies, mock_soccerdata):
        """Test proxy configuration without Tor and no proxies."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            use_tor=False,
        )

        # No proxy should be set
        proxy = scraper._configure_proxy()
        assert proxy is None


class TestSoccerdataFBrefScraperReader:
    """Tests for soccerdata reader initialization."""

    def test_get_reader_lazy_init(self, mock_base_dependencies, mock_soccerdata):
        """Test that reader is lazily initialized."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Reader should not be created yet
        assert scraper._reader is None

        # Get reader should create it
        reader = scraper._get_reader()
        assert reader is not None

        # Verify soccerdata.FBref was called
        mock_soccerdata['module'].FBref.assert_called_once()

    def test_get_reader_caches_instance(self, mock_base_dependencies, mock_soccerdata):
        """Test that reader instance is cached."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        reader1 = scraper._get_reader()
        reader2 = scraper._get_reader()

        # Should be the same instance
        assert reader1 is reader2
        # FBref should only be called once
        assert mock_soccerdata['module'].FBref.call_count == 1

    def test_reset_reader(self, mock_base_dependencies, mock_soccerdata):
        """Test that reset_reader clears the cached instance."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Create reader
        scraper._get_reader()
        assert scraper._reader is not None

        # Reset reader
        scraper._reset_reader()
        assert scraper._reader is None


class TestSoccerdataFBrefScraperReadSchedule:
    """Tests for read_schedule method."""

    def test_read_schedule_success(self, mock_base_dependencies, mock_soccerdata):
        """Test successful schedule reading."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_schedule()

        assert df is not None
        assert not df.empty
        assert 'home_team' in df.columns
        assert 'away_team' in df.columns
        # Metadata should be added
        assert '_source' in df.columns
        assert df['_source'].iloc[0] == 'fbref'

    def test_read_schedule_empty_result(self, mock_base_dependencies, mock_soccerdata):
        """Test schedule reading with empty result."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        mock_soccerdata['reader'].read_schedule.return_value = pd.DataFrame()

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_schedule()
        assert df is None

    def test_read_schedule_error_handling(self, mock_base_dependencies, mock_soccerdata):
        """Test schedule reading with error."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        mock_soccerdata['reader'].read_schedule.side_effect = Exception("Network error")

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_schedule()
        assert df is None


class TestSoccerdataFBrefScraperReadPlayerStats:
    """Tests for read_player_season_stats method."""

    def test_read_player_stats_success(self, mock_base_dependencies, mock_soccerdata):
        """Test successful player stats reading."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_player_season_stats('standard')

        assert df is not None
        assert not df.empty
        assert 'player' in df.columns
        assert 'goals' in df.columns
        assert '_source' in df.columns
        assert 'stat_type' in df.columns

    def test_read_player_stats_stat_type_mapping(self, mock_base_dependencies, mock_soccerdata):
        """Test stat type mapping from internal to soccerdata names."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Test 'stats' maps to 'standard'
        scraper.read_player_season_stats('stats')
        call_kwargs = mock_soccerdata['reader'].read_player_season_stats.call_args[1]
        assert call_kwargs['stat_type'] == 'standard'

        # Test 'gca' maps to 'goal_shot_creation'
        mock_soccerdata['reader'].read_player_season_stats.reset_mock()
        scraper.read_player_season_stats('gca')
        call_kwargs = mock_soccerdata['reader'].read_player_season_stats.call_args[1]
        assert call_kwargs['stat_type'] == 'goal_shot_creation'


class TestSoccerdataFBrefScraperReadTeamStats:
    """Tests for read_team_season_stats method."""

    def test_read_team_stats_success(self, mock_base_dependencies, mock_soccerdata):
        """Test successful team stats reading."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_team_season_stats('standard')

        assert df is not None
        assert not df.empty
        assert 'team' in df.columns
        assert 'points' in df.columns
        assert '_source' in df.columns


class TestSoccerdataFBrefScraperReadKeeperStats:
    """Tests for read_keeper_stats method."""

    def test_read_keeper_stats_success(self, mock_base_dependencies, mock_soccerdata):
        """Test successful keeper stats reading."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        # Mock keeper stats response
        mock_soccerdata['reader'].read_player_season_stats.return_value = pd.DataFrame({
            'player': ['Raya', 'Alisson'],
            'team': ['Arsenal', 'Liverpool'],
            'saves': [50, 45],
        })

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = scraper.read_keeper_stats('keeper')

        assert df is not None
        assert not df.empty
        assert 'stat_type' in df.columns
        assert df['stat_type'].iloc[0] == 'keeper'

    def test_read_keeper_stats_advanced(self, mock_base_dependencies, mock_soccerdata):
        """Test reading advanced keeper stats."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper.read_keeper_stats('keeper_adv')

        # Should call with 'keepers_adv' stat type
        call_kwargs = mock_soccerdata['reader'].read_player_season_stats.call_args[1]
        assert call_kwargs['stat_type'] == 'keepers_adv'


class TestSoccerdataFBrefScraperScrapeSingleStatType:
    """Tests for scrape_single_stat_type method."""

    def test_scrape_single_stat_type_player(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping single player stat type."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_single_stat_type(
            stat_type='shooting',
            data_category='player',
        )

        assert isinstance(result, dict)
        # Should contain the table path
        assert 'player_shooting' in result

    def test_scrape_single_stat_type_team(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping single team stat type."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_single_stat_type(
            stat_type='passing',
            data_category='team',
        )

        assert isinstance(result, dict)
        assert 'team_passing' in result

    def test_scrape_single_stat_type_keeper(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping single keeper stat type."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        # Mock keeper stats response
        mock_soccerdata['reader'].read_player_season_stats.return_value = pd.DataFrame({
            'player': ['Raya'],
            'saves': [50],
        })

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_single_stat_type(
            stat_type='keeper',
            data_category='keeper',
        )

        assert isinstance(result, dict)
        assert 'keeper_keeper' in result

    def test_scrape_single_stat_type_empty_result(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping with empty result."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        mock_soccerdata['reader'].read_player_season_stats.return_value = pd.DataFrame()

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_single_stat_type(
            stat_type='shooting',
            data_category='player',
        )

        assert result == {}


class TestSoccerdataFBrefScraperScrapeMatchData:
    """Tests for scrape_match_data method."""

    def test_scrape_match_data_schedule(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping schedule data."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_match_data(data_type='schedule')

        assert isinstance(result, dict)
        assert 'schedule' in result

    def test_scrape_match_data_unsupported_type(self, mock_base_dependencies, mock_soccerdata):
        """Test scraping unsupported match data type."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # shot_events, match_events, lineups are not supported by soccerdata
        result = scraper.scrape_match_data(data_type='shot_events')

        # Should return empty dict with warning (logged)
        assert result == {}


class TestSoccerdataFBrefScraperScrapeAll:
    """Tests for scrape_all method."""

    def test_scrape_all_basic(self, mock_base_dependencies, mock_soccerdata):
        """Test full scrape with basic options."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_all(
            include_extended_stats=False,
            include_keeper_stats=False,
            include_team_stats_extended=False,
        )

        assert isinstance(result, dict)
        # Should have schedule, team_stats, player_stats
        assert len(result) >= 1

    def test_scrape_all_with_extended_stats(self, mock_base_dependencies, mock_soccerdata):
        """Test full scrape with extended stats."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        result = scraper.scrape_all(
            include_extended_stats=True,
            include_keeper_stats=True,
            include_team_stats_extended=True,
        )

        assert isinstance(result, dict)
        # Should have multiple tables
        assert len(result) > 3


class TestSoccerdataFBrefScraperContextManager:
    """Tests for context manager usage."""

    def test_context_manager(self, mock_base_dependencies, mock_soccerdata):
        """Test using scraper as context manager."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        with SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        ) as scraper:
            assert scraper is not None
            df = scraper.read_schedule()
            assert df is not None

    def test_close_clears_reader(self, mock_base_dependencies, mock_soccerdata):
        """Test that close() clears the reader."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        scraper = SoccerdataFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Create reader
        scraper._get_reader()
        assert scraper._reader is not None

        # Close should clear it
        scraper.close()
        assert scraper._reader is None


class TestSoccerdataFBrefScraperConstants:
    """Tests for scraper constants."""

    def test_stat_types_defined(self, mock_base_dependencies, mock_soccerdata):
        """Test that stat types are defined."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        assert hasattr(SoccerdataFBrefScraper, 'PLAYER_STAT_TYPES')
        assert hasattr(SoccerdataFBrefScraper, 'TEAM_STAT_TYPES')
        assert hasattr(SoccerdataFBrefScraper, 'KEEPER_STAT_TYPES')

    def test_source_name(self, mock_base_dependencies, mock_soccerdata):
        """Test source name is fbref."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        assert SoccerdataFBrefScraper.SOURCE_NAME == 'fbref'

    def test_default_rate_limit(self, mock_base_dependencies, mock_soccerdata):
        """Test default rate limit is set."""
        from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

        assert hasattr(SoccerdataFBrefScraper, 'DEFAULT_RATE_LIMIT')
        assert SoccerdataFBrefScraper.DEFAULT_RATE_LIMIT > 0
