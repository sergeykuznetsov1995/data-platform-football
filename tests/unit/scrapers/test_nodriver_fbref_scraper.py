"""
Unit tests for NodriverFBrefScraper.

Tests the Cloudflare Turnstile bypass scraper for FBref.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd


class TestNodriverFBrefScraperInit:
    """Tests for NodriverFBrefScraper initialization."""

    @pytest.mark.unit
    def test_init_default_values(self):
        """Test default initialization values."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()

        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert scraper.proxy_file is None
        assert scraper.headless is True
        assert scraper.use_xvfb is True
        assert scraper.cloudflare_wait == 90.0
        assert scraper.max_retries == 2  # Per-proxy retries (reduced from 5)
        assert scraper.cf_verify_max_retries == 12
        assert scraper.content_timeout == 45.0  # NodriverFBrefScraper.CONTENT_TIMEOUT

    @pytest.mark.unit
    def test_init_custom_values(self, tmp_path):
        """Test custom initialization values."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        # Create temporary proxy file
        proxy_file = tmp_path / "proxys.txt"
        proxy_file.write_text("pool.proxys.io:10000:user:pass")

        scraper = NodriverFBrefScraper(
            leagues=['ESP-La Liga', 'GER-Bundesliga'],
            seasons=[2023, 2024],
            proxy_file=str(proxy_file),
            headless=False,
            use_xvfb=False,
            cloudflare_wait=120.0,
            max_retries=10,
            cf_verify_max_retries=20,
            content_timeout=180.0,
        )

        assert scraper.leagues == ['ESP-La Liga', 'GER-Bundesliga']
        assert scraper.seasons == [2023, 2024]
        assert scraper.proxy_file == str(proxy_file)
        assert scraper.headless is False
        assert scraper.use_xvfb is False
        assert scraper.cloudflare_wait == 120.0
        assert scraper.max_retries == 10
        assert scraper.cf_verify_max_retries == 20
        assert scraper.content_timeout == 180.0

    @pytest.mark.unit
    def test_init_stats(self):
        """Test initial statistics are zero."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()

        assert scraper._stats['successes'] == 0
        assert scraper._stats['failures'] == 0
        assert scraper._stats['cloudflare_blocked'] == 0
        assert scraper._stats['proxy_rotations'] == 0


class TestNodriverFBrefScraperProxyManager:
    """Tests for proxy manager integration."""

    @pytest.mark.unit
    def test_init_proxy_manager_from_file(self, tmp_path):
        """Test proxy manager initialization from file."""
        # Create temporary proxy file
        proxy_file = tmp_path / "proxys.txt"
        proxy_file.write_text(
            "pool.proxys.io:10000:user:pass\n"
            "pool.proxys.io:10001:user:pass\n"
        )

        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper(proxy_file=str(proxy_file))

        assert scraper._proxy_manager is not None
        assert scraper._proxy_manager.total_count == 2

    @pytest.mark.unit
    def test_get_proxy_string_returns_nodriver_format(self, tmp_path):
        """Test that proxy string is in nodriver format."""
        proxy_file = tmp_path / "proxys.txt"
        proxy_file.write_text("pool.proxys.io:10000:user:pass")

        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper(proxy_file=str(proxy_file))
        proxy_str = scraper._get_proxy_string()

        assert proxy_str == "pool.proxys.io:10000:user:pass"
        assert scraper._stats['proxy_rotations'] == 1

    @pytest.mark.unit
    def test_get_proxy_string_no_proxies(self):
        """Test proxy string returns None when no proxies available."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        proxy_str = scraper._get_proxy_string()

        assert proxy_str is None


class TestNodriverFBrefScraperCloudflareDetection:
    """Tests for Cloudflare detection."""

    @pytest.fixture
    def scraper(self):
        """Create scraper instance."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper
        return NodriverFBrefScraper()

    @pytest.mark.unit
    def test_is_cloudflare_blocked_empty(self, scraper):
        """Test empty HTML is detected as blocked."""
        assert scraper._is_cloudflare_blocked("") is True
        assert scraper._is_cloudflare_blocked(None) is True

    @pytest.mark.unit
    def test_is_cloudflare_blocked_challenge_indicators(self, scraper):
        """Test Cloudflare challenge indicators are detected."""
        blocked_pages = [
            "<html><body>Just a moment...</body></html>",
            "<html><body>Checking your browser</body></html>",
            '<div class="cf-browser-verification">Verifying</div>',
            '<div id="challenge-running">Please wait</div>',
            "<html>Ray ID: abc123</html>",
            '<div class="cf-turnstile" data-sitekey="abc">Challenge</div>',
            '<script>cf_chl_opt = {}</script>',
        ]

        for html in blocked_pages:
            assert scraper._is_cloudflare_blocked(html) is True, f"Not detected: {html}"

    @pytest.mark.unit
    def test_is_cloudflare_blocked_normal_page(self, scraper):
        """Test normal pages are not detected as blocked."""
        normal_pages = [
            "<html><body>Welcome to FBref</body></html>",
            '<table id="stats_player">Player data</table>',
            "<html><head><title>Premier League Stats</title></head></html>",
        ]

        for html in normal_pages:
            assert scraper._is_cloudflare_blocked(html) is False, f"False positive: {html}"


class TestNodriverFBrefScraperFetchPage:
    """Tests for page fetching with Cloudflare bypass and proxy rotation."""

    @pytest.fixture
    def scraper(self):
        """Create scraper instance with mocked browser."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        # Mock browser
        scraper._browser = MagicMock()
        return scraper

    @pytest.mark.unit
    def test_fetch_page_success(self, scraper):
        """Test successful page fetch."""
        mock_html = '<html><body><table>Stats</table></body></html>'
        scraper._browser.get_page = MagicMock(return_value=mock_html)

        result = scraper._fetch_page("https://fbref.com/en/comps/")

        assert result == mock_html
        assert scraper._stats['successes'] == 1
        assert scraper._stats['failures'] == 0

    @pytest.mark.unit
    @patch('scrapers.nodriver_fbref_scraper.time.sleep')  # Mock sleep to speed up test
    def test_fetch_page_cloudflare_blocked_retries_with_new_proxy(self, mock_sleep, scraper):
        """Test Cloudflare blocked page retries with different proxies."""
        mock_html = '<html><body>Just a moment...</body></html>'
        scraper._browser.get_page = MagicMock(return_value=mock_html)

        # Mock _restart_browser to track calls
        scraper._restart_browser = MagicMock()

        result = scraper._fetch_page("https://fbref.com/en/comps/", max_cf_retries=3)

        assert result is None
        # Should retry 3 times with CF block
        assert scraper._stats['cloudflare_blocked'] == 3
        # Should restart browser after each CF block
        assert scraper._restart_browser.call_count == 3

    @pytest.mark.unit
    @patch('scrapers.nodriver_fbref_scraper.time.sleep')
    def test_fetch_page_cloudflare_success_after_retry(self, mock_sleep, scraper):
        """Test Cloudflare bypass succeeds after retry with new proxy."""
        blocked_html = '<html><body>Just a moment...</body></html>'
        success_html = '<html><body><table>Stats</table></body></html>'

        # First call: blocked, second call: success
        scraper._browser.get_page = MagicMock(side_effect=[blocked_html, success_html])
        scraper._restart_browser = MagicMock()

        result = scraper._fetch_page("https://fbref.com/en/comps/", max_cf_retries=3)

        assert result == success_html
        assert scraper._stats['cloudflare_blocked'] == 1
        assert scraper._stats['successes'] == 1
        # Should restart browser once after first CF block
        assert scraper._restart_browser.call_count == 1

    @pytest.mark.unit
    @patch('scrapers.nodriver_fbref_scraper.time.sleep')
    def test_fetch_page_empty_response_retries(self, mock_sleep, scraper):
        """Test empty response retries with new proxy."""
        scraper._browser.get_page = MagicMock(return_value="")
        scraper._restart_browser = MagicMock()

        result = scraper._fetch_page("https://fbref.com/en/comps/", max_cf_retries=3)

        assert result is None
        assert scraper._stats['failures'] == 3
        assert scraper._restart_browser.call_count == 3

    @pytest.mark.unit
    @patch('scrapers.nodriver_fbref_scraper.time.sleep')
    def test_fetch_page_exception_retries(self, mock_sleep, scraper):
        """Test exception during fetch retries with new proxy."""
        scraper._browser.get_page = MagicMock(side_effect=Exception("Network error"))
        scraper._restart_browser = MagicMock()

        result = scraper._fetch_page("https://fbref.com/en/comps/", max_cf_retries=3)

        assert result is None
        assert scraper._stats['failures'] == 3
        assert scraper._restart_browser.call_count == 3


class TestNodriverFBrefScraperReadMethods:
    """Tests for data reading methods."""

    @pytest.fixture
    def scraper(self):
        """Create scraper with mocked fetch."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )
        return scraper

    @pytest.mark.unit
    def test_read_schedule_success(self, scraper):
        """Test successful schedule reading."""
        # Mock HTML with schedule table
        mock_html = '''
        <html>
        <body>
        <table id="sched_2024-2025_9_1">
            <thead>
                <tr><th>Wk</th><th>Day</th><th>Date</th><th>Home</th><th>Away</th></tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>Sat</td><td>2024-08-17</td><td>Arsenal</td><td>Liverpool</td></tr>
            </tbody>
        </table>
        </body>
        </html>
        '''

        with patch.object(scraper, '_fetch_page', return_value=mock_html):
            with patch('scrapers.nodriver_fbref_scraper.find_schedule_table') as mock_find:
                mock_df = pd.DataFrame({
                    'Wk': ['1'],
                    'Day': ['Sat'],
                    'Date': ['2024-08-17'],
                    'Home': ['Arsenal'],
                    'Away': ['Liverpool'],
                })
                mock_find.return_value = mock_df

                result = scraper.read_schedule('ENG-Premier League', 2024)

        assert result is not None
        assert 'league' in result.columns
        assert 'season' in result.columns
        assert result['league'].iloc[0] == 'ENG-Premier League'
        assert result['season'].iloc[0] == 2024

    @pytest.mark.unit
    def test_read_schedule_fetch_failure(self, scraper):
        """Test schedule reading with fetch failure."""
        with patch.object(scraper, '_fetch_page', return_value=None):
            result = scraper.read_schedule('ENG-Premier League', 2024)

        assert result is None

    @pytest.mark.unit
    def test_read_player_season_stats_success(self, scraper):
        """Test successful player stats reading."""
        mock_html = '<html><body><table id="stats_standard">Stats</table></body></html>'

        with patch.object(scraper, '_fetch_page', return_value=mock_html):
            with patch('scrapers.nodriver_fbref_scraper.find_player_stats_table') as mock_find:
                mock_df = pd.DataFrame({
                    'Player': ['Player1', 'Player2'],
                    'Goals': [10, 5],
                })
                mock_find.return_value = mock_df

                result = scraper.read_player_season_stats('stats', 'ENG-Premier League', 2024)

        assert result is not None
        assert 'stat_type' in result.columns
        assert result['stat_type'].iloc[0] == 'stats'


class TestNodriverFBrefScraperScrapeStatType:
    """Tests for scrape_single_stat_type method."""

    @pytest.fixture
    def scraper(self):
        """Create scraper instance."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper
        return NodriverFBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

    @pytest.mark.unit
    def test_scrape_single_stat_type_player(self, scraper):
        """Test scraping single player stat type."""
        mock_df = pd.DataFrame({
            'Player': ['Player1'],
            'Goals': [10],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        with patch.object(scraper, 'read_player_season_stats', return_value=mock_df):
            result = scraper.scrape_single_stat_type('stats', 'player')

        assert result['rows'] == 1
        assert result['data'] is not None

    @pytest.mark.unit
    def test_scrape_single_stat_type_team(self, scraper):
        """Test scraping single team stat type."""
        mock_df = pd.DataFrame({
            'Squad': ['Arsenal'],
            'Goals': [50],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        with patch.object(scraper, 'read_team_season_stats', return_value=mock_df):
            result = scraper.scrape_single_stat_type('stats', 'team')

        assert result['rows'] == 1
        assert result['data'] is not None

    @pytest.mark.unit
    def test_scrape_single_stat_type_keeper(self, scraper):
        """Test scraping single keeper stat type."""
        mock_df = pd.DataFrame({
            'Player': ['Keeper1'],
            'Saves': [100],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        with patch.object(scraper, 'read_keeper_stats', return_value=mock_df):
            result = scraper.scrape_single_stat_type('keeper', 'keeper')

        assert result['rows'] == 1
        assert result['data'] is not None

    @pytest.mark.unit
    def test_scrape_single_stat_type_no_data(self, scraper):
        """Test scraping with no data returned."""
        with patch.object(scraper, 'read_player_season_stats', return_value=None):
            result = scraper.scrape_single_stat_type('stats', 'player')

        assert result['rows'] == 0
        assert result['data'] is None

    @pytest.mark.unit
    def test_scrape_single_stat_type_invalid_category(self, scraper):
        """Test scraping with invalid data category."""
        result = scraper.scrape_single_stat_type('stats', 'invalid_category')

        assert result['rows'] == 0


class TestNodriverFBrefScraperContextManager:
    """Tests for context manager behavior."""

    @pytest.mark.unit
    def test_context_manager_enter_exit(self):
        """Test context manager properly enters and exits."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        with NodriverFBrefScraper() as scraper:
            assert scraper is not None
            assert isinstance(scraper, NodriverFBrefScraper)

    @pytest.mark.unit
    def test_context_manager_closes_browser(self):
        """Test browser is closed on context manager exit."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        mock_browser = MagicMock()
        scraper._browser = mock_browser

        scraper.close()

        # Browser should be closed and reference set to None
        mock_browser.close_sync.assert_called_once()
        assert scraper._browser is None


class TestNodriverFBrefScraperStats:
    """Tests for statistics tracking."""

    @pytest.mark.unit
    def test_get_stats(self):
        """Test get_stats returns proper statistics."""
        from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

        scraper = NodriverFBrefScraper()
        scraper._stats['successes'] = 5
        scraper._stats['failures'] = 2
        scraper._stats['cloudflare_blocked'] = 1

        stats = scraper.get_stats()

        assert stats['successes'] == 5
        assert stats['failures'] == 2
        assert stats['cloudflare_blocked'] == 1
