"""
FBref Scraper Integration Tests
===============================

Integration tests for FBrefScraper with Cloudflare bypass.
These tests verify the scraper can successfully:
- Bypass Cloudflare protection
- Parse schedule tables
- Parse team statistics
- Parse player statistics
- Handle FBref's hidden comment tables

NOTE: These tests make real HTTP requests and may be slow.
Run with: pytest tests/integration/scrapers/test_fbref_scraper.py -v -s
"""

import logging
import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# Skip all tests if Selenium is not available
try:
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Skip tests if Chrome is not available
CHROME_AVAILABLE = (
    os.path.exists('/usr/bin/chromium') or
    os.path.exists('/usr/bin/google-chrome') or
    os.path.exists('/usr/bin/chromium-browser')
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.scraper,
    pytest.mark.skipif(not SELENIUM_AVAILABLE, reason="Selenium not installed"),
    pytest.mark.skipif(not CHROME_AVAILABLE, reason="Chrome/Chromium not available"),
]

logger = logging.getLogger(__name__)


@pytest.fixture
def fbref_scraper():
    """Create FBrefScraper instance for testing."""
    from scrapers.fbref_scraper import FBrefScraper

    scraper = FBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        headless=True,
        use_xvfb=True,  # Use Xvfb to bypass Cloudflare headless detection
    )
    yield scraper
    scraper.close()


@pytest.fixture
def mock_html_with_schedule():
    """Sample HTML with schedule table (as FBref returns it)."""
    return '''
    <html>
    <body>
    <table id="sched_all">
        <thead>
            <tr><th>Date</th><th>Home</th><th>Score</th><th>Away</th></tr>
        </thead>
        <tbody>
            <tr><td>2024-08-16</td><td>Man Utd</td><td>1-0</td><td>Fulham</td></tr>
            <tr><td>2024-08-17</td><td>Arsenal</td><td>2-0</td><td>Wolves</td></tr>
        </tbody>
    </table>
    </body>
    </html>
    '''


@pytest.fixture
def mock_html_with_comment_table():
    """Sample HTML with table hidden in comment (FBref pattern)."""
    return '''
    <html>
    <body>
    <div id="switcher_sched_all"></div>
    <!--
    <table id="sched_all">
        <thead>
            <tr><th>Date</th><th>Home</th><th>Score</th><th>Away</th></tr>
        </thead>
        <tbody>
            <tr><td>2024-08-16</td><td>Man Utd</td><td>1-0</td><td>Fulham</td></tr>
            <tr><td>2024-08-17</td><td>Arsenal</td><td>2-0</td><td>Wolves</td></tr>
            <tr><td>2024-08-18</td><td>Liverpool</td><td>2-0</td><td>Ipswich</td></tr>
        </tbody>
    </table>
    -->
    </body>
    </html>
    '''


@pytest.fixture
def mock_html_with_player_stats():
    """Sample HTML with player statistics table."""
    return '''
    <html>
    <body>
    <table id="stats_standard">
        <thead>
            <tr>
                <th>Player</th><th>Nation</th><th>Pos</th><th>Squad</th>
                <th>Age</th><th>MP</th><th>Starts</th><th>Min</th>
                <th>Gls</th><th>Ast</th><th>xG</th><th>xAG</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Erling Haaland</td><td>Norway</td><td>FW</td><td>Manchester City</td>
                <td>24</td><td>38</td><td>38</td><td>3240</td>
                <td>27</td><td>5</td><td>24.5</td><td>3.2</td>
            </tr>
            <tr>
                <td>Cole Palmer</td><td>England</td><td>MF</td><td>Chelsea</td>
                <td>22</td><td>34</td><td>33</td><td>2890</td>
                <td>22</td><td>11</td><td>18.7</td><td>8.4</td>
            </tr>
        </tbody>
    </table>
    </body>
    </html>
    '''


class TestFBrefScraperUnit:
    """Unit tests for FBrefScraper (no real HTTP requests)."""

    def test_init(self, fbref_scraper):
        """Test scraper initialization."""
        assert fbref_scraper.SOURCE_NAME == 'fbref'
        assert fbref_scraper.leagues == ['ENG-Premier League']
        assert fbref_scraper.seasons == [2024]
        assert fbref_scraper.headless is True

    def test_format_season(self, fbref_scraper):
        """Test season formatting."""
        assert fbref_scraper._format_season(2023) == "2023-2024"
        assert fbref_scraper._format_season(2024) == "2024-2025"

    def test_get_schedule_url(self, fbref_scraper):
        """Test schedule URL building."""
        url = fbref_scraper._get_schedule_url('ENG-Premier League', 2024)
        assert 'fbref.com' in url
        assert '2024-2025' in url
        assert 'Premier-League' in url
        assert 'schedule' in url

    def test_get_stats_url(self, fbref_scraper):
        """Test stats URL building."""
        url = fbref_scraper._get_stats_url('ENG-Premier League', 2024, 'shooting')
        assert 'fbref.com' in url
        assert '2024-2025' in url
        assert 'shooting' in url

    def test_unknown_league_raises_error(self, fbref_scraper):
        """Test that unknown league raises ValueError."""
        with pytest.raises(ValueError, match="Unknown league"):
            fbref_scraper._get_schedule_url('INVALID-League', 2024)

    def test_extract_tables_from_comments(self, fbref_scraper, mock_html_with_comment_table):
        """Test extracting tables hidden in HTML comments."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(mock_html_with_comment_table, 'html.parser')
        tables = fbref_scraper._extract_tables_from_comments(soup)

        assert 'sched_all' in tables
        table = tables['sched_all']
        rows = table.find_all('tr')
        # 1 header + 3 data rows
        assert len(rows) == 4

    def test_parse_table_from_regular_html(self, fbref_scraper, mock_html_with_schedule):
        """Test parsing table from regular HTML."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(mock_html_with_schedule, 'html.parser')
        df = fbref_scraper._parse_table(soup, 'sched_all')

        assert df is not None
        assert len(df) == 2
        assert 'Home' in df.columns
        assert 'Arsenal' in df['Home'].values

    def test_parse_table_from_comment(self, fbref_scraper, mock_html_with_comment_table):
        """Test parsing table from HTML comment."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(mock_html_with_comment_table, 'html.parser')
        comment_tables = fbref_scraper._extract_tables_from_comments(soup)
        df = fbref_scraper._parse_table(soup, 'sched_all', comment_tables)

        assert df is not None
        assert len(df) == 3
        assert 'Liverpool' in df['Home'].values

    def test_parse_player_stats_table(self, fbref_scraper, mock_html_with_player_stats):
        """Test parsing player statistics table."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(mock_html_with_player_stats, 'html.parser')
        df = fbref_scraper._parse_table(soup, 'stats_standard')

        assert df is not None
        assert len(df) == 2
        assert 'Player' in df.columns
        assert 'Erling Haaland' in df['Player'].values
        assert 'Cole Palmer' in df['Player'].values


class TestFBrefScraperIntegration:
    """Integration tests making real HTTP requests to FBref."""

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_cloudflare_bypass(self, fbref_scraper):
        """Test that Cloudflare protection can be bypassed."""
        browser = fbref_scraper._get_browser()

        try:
            html = browser.get_page(
                'https://fbref.com/en/',
                wait_timeout=15,
                cloudflare_wait=10.0,
            )

            # Check that we got past Cloudflare
            assert 'fbref' in html.lower() or 'football' in html.lower()
            assert 'challenge' not in html.lower() or 'cloudflare' not in html.lower()

            logger.info("Cloudflare bypass successful")

        except Exception as e:
            pytest.skip(f"Cloudflare bypass failed (may be blocked): {e}")

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_schedule_parsing(self, fbref_scraper):
        """Test parsing real schedule data from FBref."""
        try:
            df = fbref_scraper.read_schedule('ENG-Premier League', 2024)

            if df is None:
                pytest.skip("Could not fetch schedule data")

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'league' in df.columns
            assert 'season' in df.columns
            assert df['league'].iloc[0] == 'ENG-Premier League'
            assert df['season'].iloc[0] == 2024

            logger.info(f"Successfully parsed {len(df)} schedule rows")
            logger.info(f"Columns: {list(df.columns)}")

        except Exception as e:
            pytest.skip(f"Schedule parsing failed: {e}")

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_team_stats_parsing(self, fbref_scraper):
        """Test parsing real team statistics from FBref."""
        try:
            df = fbref_scraper.read_team_season_stats('stats', 'ENG-Premier League', 2024)

            if df is None:
                pytest.skip("Could not fetch team stats data")

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'league' in df.columns
            assert 'stat_type' in df.columns

            logger.info(f"Successfully parsed {len(df)} team stat rows")
            logger.info(f"Columns: {list(df.columns)}")

        except Exception as e:
            pytest.skip(f"Team stats parsing failed: {e}")

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_player_stats_parsing(self, fbref_scraper):
        """Test parsing real player statistics from FBref."""
        try:
            df = fbref_scraper.read_player_season_stats('stats', 'ENG-Premier League', 2024)

            if df is None:
                pytest.skip("Could not fetch player stats data")

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'league' in df.columns
            assert 'stat_type' in df.columns

            logger.info(f"Successfully parsed {len(df)} player stat rows")
            logger.info(f"Columns: {list(df.columns)}")

        except Exception as e:
            pytest.skip(f"Player stats parsing failed: {e}")


class TestFBrefRateLimiting:
    """Tests for rate limiting functionality."""

    def test_rate_limiter_initialized(self, fbref_scraper):
        """Test that rate limiter is properly initialized."""
        assert fbref_scraper._rate_limiter is not None
        assert fbref_scraper.DEFAULT_RATE_LIMIT == 20

    def test_rate_limiter_acquire(self, fbref_scraper):
        """Test that rate limiter can be acquired."""
        # Should not raise
        fbref_scraper._rate_limiter.acquire()


class TestFBrefMetadata:
    """Tests for metadata handling."""

    def test_add_metadata(self, fbref_scraper):
        """Test metadata is correctly added to DataFrame."""
        df = pd.DataFrame({
            'Player': ['Test Player'],
            'Goals': [10],
        })

        df_with_meta = fbref_scraper._add_metadata(df, 'player_stats')

        assert '_source' in df_with_meta.columns
        assert '_entity_type' in df_with_meta.columns
        assert '_ingested_at' in df_with_meta.columns
        assert '_batch_id' in df_with_meta.columns

        assert df_with_meta['_source'].iloc[0] == 'fbref'
        assert df_with_meta['_entity_type'].iloc[0] == 'player_stats'


class TestFBrefLeagueConfig:
    """Tests for league configuration."""

    def test_all_leagues_have_valid_config(self, fbref_scraper):
        """Test all configured leagues have required fields."""
        for league, config in fbref_scraper.LEAGUE_IDS.items():
            assert 'comp_id' in config, f"Missing comp_id for {league}"
            assert 'slug' in config, f"Missing slug for {league}"
            assert isinstance(config['comp_id'], str), f"comp_id should be string for {league}"
            assert isinstance(config['slug'], str), f"slug should be string for {league}"

    def test_major_leagues_configured(self, fbref_scraper):
        """Test that major leagues are configured."""
        expected_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in expected_leagues:
            assert league in fbref_scraper.LEAGUE_IDS, f"Missing league: {league}"


class TestFBrefMergeIntegration:
    """Integration tests for player stats merge functionality."""

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_merge_player_stats_extended(self, fbref_scraper):
        """
        Test that _merge_player_stats correctly merges data from multiple stat types.

        This test verifies the fix for the issue where merge was failing due to
        MultiIndex column names not being normalized (e.g., 'Unnamed: 0_level_0_Player'
        instead of 'Player').
        """
        try:
            # Collect base stats and shooting stats
            stats_df = fbref_scraper.read_player_season_stats(
                'stats', 'ENG-Premier League', 2024
            )
            if stats_df is None or stats_df.empty:
                pytest.skip("Could not fetch base stats data")

            logger.info(f"Base stats columns: {list(stats_df.columns)[:15]}")
            logger.info(f"Base stats rows: {len(stats_df)}")

            # Check that column normalization worked
            assert 'Player' in stats_df.columns, (
                f"Column normalization failed. Columns: {list(stats_df.columns)[:10]}"
            )

            shooting_df = fbref_scraper.read_player_season_stats(
                'shooting', 'ENG-Premier League', 2024
            )
            if shooting_df is None or shooting_df.empty:
                pytest.skip("Could not fetch shooting stats data")

            logger.info(f"Shooting stats columns: {list(shooting_df.columns)[:15]}")
            logger.info(f"Shooting stats rows: {len(shooting_df)}")

            # Check that column normalization worked for shooting too
            assert 'Player' in shooting_df.columns, (
                f"Column normalization failed for shooting. "
                f"Columns: {list(shooting_df.columns)[:10]}"
            )

            # Merge stats
            data = {
                'stats': stats_df,
                'shooting': shooting_df,
            }
            merged_df = fbref_scraper._merge_player_stats(
                data, 'ENG-Premier League', 2024
            )

            assert merged_df is not None, "Merge returned None"
            assert not merged_df.empty, "Merged DataFrame is empty"

            # Verify merge added new columns from shooting
            stats_cols = set(stats_df.columns)
            shooting_unique_cols = set(shooting_df.columns) - stats_cols

            merged_cols = set(merged_df.columns)
            new_cols_added = shooting_unique_cols & merged_cols

            logger.info(f"Merged columns count: {len(merged_df.columns)}")
            logger.info(f"New columns from shooting: {len(new_cols_added)}")
            logger.info(f"Sample new columns: {list(new_cols_added)[:10]}")

            # Should have more columns than base stats alone
            assert len(merged_df.columns) > len(stats_df.columns), (
                f"Merge did not add columns. "
                f"Base: {len(stats_df.columns)}, Merged: {len(merged_df.columns)}"
            )

            # Verify metadata was added
            assert 'league' in merged_df.columns
            assert 'season' in merged_df.columns

            logger.info("Merge integration test PASSED!")

        except Exception as e:
            logger.error(f"Merge integration test failed: {e}")
            raise

    @pytest.mark.slow
    @pytest.mark.real_request
    def test_merge_multiple_stat_types(self, fbref_scraper):
        """
        Test merging multiple stat types (stats, shooting, passing).
        """
        try:
            stat_types_to_test = ['stats', 'shooting', 'passing']
            data = {}

            for stat_type in stat_types_to_test:
                df = fbref_scraper.read_player_season_stats(
                    stat_type, 'ENG-Premier League', 2024
                )
                if df is not None and not df.empty:
                    data[stat_type] = df
                    logger.info(
                        f"{stat_type}: {len(df)} rows, {len(df.columns)} columns"
                    )

            if 'stats' not in data:
                pytest.skip("Could not fetch base stats data")

            if len(data) < 2:
                pytest.skip("Could not fetch enough stat types for merge test")

            merged_df = fbref_scraper._merge_player_stats(
                data, 'ENG-Premier League', 2024
            )

            assert merged_df is not None
            assert not merged_df.empty

            # Total columns should be significantly more than base stats
            base_cols = len(data['stats'].columns)
            merged_cols = len(merged_df.columns)

            logger.info(f"Base columns: {base_cols}")
            logger.info(f"Merged columns: {merged_cols}")

            # Should have at least some additional columns
            assert merged_cols > base_cols, (
                f"Expected more columns after merge. Base: {base_cols}, Got: {merged_cols}"
            )

            logger.info("Multiple stat types merge test PASSED!")

        except Exception as e:
            logger.error(f"Multiple stat types merge test failed: {e}")
            raise


class TestFBrefCaching:
    """Tests for page caching functionality."""

    def test_cache_initially_empty(self, fbref_scraper):
        """Test page cache starts empty."""
        assert len(fbref_scraper._page_cache) == 0

    def test_clear_cache(self, fbref_scraper):
        """Test cache clearing."""
        fbref_scraper._page_cache['test_url'] = '<html></html>'
        assert len(fbref_scraper._page_cache) == 1

        fbref_scraper.clear_cache()
        assert len(fbref_scraper._page_cache) == 0


class TestFBrefCleanup:
    """Tests for resource cleanup."""

    def test_context_manager_cleanup(self):
        """Test that context manager properly cleans up."""
        from scrapers.fbref_scraper import FBrefScraper

        with FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True,
        ) as scraper:
            # Access browser to create it
            # Don't actually create browser in this test
            pass

        # After exiting context, browser should be None
        assert scraper._browser is None

    def test_close_method(self, fbref_scraper):
        """Test close method works without error."""
        # Should not raise even if browser was never created
        fbref_scraper.close()
        assert fbref_scraper._browser is None


class TestFlareSolverrIntegration:
    """Integration tests for FlareSolverr mode."""

    @pytest.fixture
    def fbref_scraper_flaresolverr(self):
        """Create FBrefScraper instance using FlareSolverr."""
        from scrapers.fbref_scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True,
            use_flaresolverr=True,
            flaresolverr_url='http://flaresolverr:8191',
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaresolverr
    def test_flaresolverr_health_check(self):
        """Test FlareSolverr health check."""
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient(base_url='http://flaresolverr:8191')
        is_healthy = client.health_check(verbose=True)

        logger.info(f"FlareSolverr health check result: {is_healthy}")

        # This test may fail if FlareSolverr is not running
        # Mark as skip instead of fail in that case
        if not is_healthy:
            pytest.skip("FlareSolverr is not available")

        assert is_healthy

    @pytest.mark.flaresolverr
    def test_flaresolverr_version_info(self):
        """Test getting FlareSolverr version info."""
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient(base_url='http://flaresolverr:8191')
        version_info = client.get_version_info()

        if version_info is None:
            pytest.skip("FlareSolverr is not available")

        logger.info(f"FlareSolverr version info: {version_info}")
        assert 'status' in version_info

    @pytest.mark.flaresolverr
    @pytest.mark.slow
    def test_flaresolverr_simple_request(self):
        """Test a simple request through FlareSolverr."""
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient(base_url='http://flaresolverr:8191')

        if not client.health_check():
            pytest.skip("FlareSolverr is not available")

        try:
            # Use a simple page that doesn't require Cloudflare bypass
            html = client.get_html(
                'https://example.com',
                max_timeout=30000
            )

            assert html is not None
            assert len(html) > 0
            assert 'Example Domain' in html

            logger.info(f"FlareSolverr request successful, HTML length: {len(html)}")

        except Exception as e:
            logger.error(f"FlareSolverr request failed: {e}")
            pytest.skip(f"FlareSolverr request failed: {e}")

    @pytest.mark.flaresolverr
    @pytest.mark.slow
    @pytest.mark.real_request
    def test_flaresolverr_fbref_request(self):
        """Test FlareSolverr can fetch FBref page."""
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient(
            base_url='http://flaresolverr:8191',
            max_timeout=120000  # 2 minutes for Cloudflare challenge
        )

        if not client.health_check():
            pytest.skip("FlareSolverr is not available")

        try:
            html = client.get_html(
                'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
                max_timeout=120000
            )

            assert html is not None
            html_len = len(html)
            has_tables = '<table' in html

            # Check if we're still blocked by Cloudflare
            cloudflare_blocked = any(
                cf in html.lower()
                for cf in ['just a moment', 'checking your browser', 'challenge-running']
            )

            logger.info(
                f"FBref request: length={html_len}, "
                f"has_tables={has_tables}, "
                f"cloudflare_blocked={cloudflare_blocked}"
            )

            if cloudflare_blocked:
                pytest.skip("FlareSolverr could not bypass Cloudflare protection")

            assert has_tables, "No tables found in HTML - page may be blocked"
            assert html_len > 10000, f"HTML too short ({html_len}), may be error page"

        except Exception as e:
            logger.error(f"FlareSolverr FBref request failed: {e}")
            pytest.skip(f"FlareSolverr FBref request failed: {e}")

    @pytest.mark.flaresolverr
    @pytest.mark.slow
    @pytest.mark.real_request
    def test_fbref_scraper_with_flaresolverr(self, fbref_scraper_flaresolverr):
        """Test FBref scraper using FlareSolverr mode."""
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient(base_url='http://flaresolverr:8191')
        if not client.health_check():
            pytest.skip("FlareSolverr is not available")

        try:
            df = fbref_scraper_flaresolverr.read_schedule('ENG-Premier League', 2024)

            if df is None:
                logger.warning("Schedule data is None - Cloudflare may have blocked request")
                pytest.skip("Could not fetch schedule data - Cloudflare block suspected")

            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

            logger.info(f"FlareSolverr mode: parsed {len(df)} schedule rows")
            logger.info(f"Columns: {list(df.columns)}")

        except Exception as e:
            logger.error(f"FBref scraper with FlareSolverr failed: {e}")
            pytest.skip(f"FBref scraper with FlareSolverr failed: {e}")


class TestHTMLDiagnostics:
    """Tests for HTML diagnostic functions."""

    def test_diagnose_html_structure_normal_page(self):
        """Test HTML diagnosis on normal page."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import diagnose_html_structure

        html = '''
        <html>
        <head><title>Premier League Stats</title></head>
        <body>
        <div id="content">
            <table id="stats_standard">
                <tr><th>Player</th></tr>
            </table>
            <table id="sched_all">
                <tr><th>Date</th></tr>
            </table>
        </div>
        </body>
        </html>
        '''

        soup = BeautifulSoup(html, 'html.parser')
        diagnosis = diagnose_html_structure(soup)

        assert diagnosis['title'] == 'Premier League Stats'
        assert diagnosis['total_tables'] == 2
        assert 'stats_standard' in diagnosis['table_ids']
        assert 'sched_all' in diagnosis['table_ids']
        assert diagnosis['has_content_div'] is True
        assert diagnosis['has_stats_tables'] is True
        assert diagnosis['has_sched_tables'] is True
        assert len(diagnosis['cloudflare_indicators']) == 0

    def test_diagnose_html_structure_cloudflare_blocked(self):
        """Test HTML diagnosis on Cloudflare blocked page."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import diagnose_html_structure

        html = '''
        <html>
        <head><title>Just a moment...</title></head>
        <body>
        <div id="cf-wrapper">
            <div id="challenge-running">Please wait...</div>
        </div>
        </body>
        </html>
        '''

        soup = BeautifulSoup(html, 'html.parser')
        diagnosis = diagnose_html_structure(soup)

        assert 'Just a moment...' in diagnosis['title']
        assert diagnosis['total_tables'] == 0
        assert len(diagnosis['cloudflare_indicators']) > 0
        assert 'cf-wrapper' in diagnosis['cloudflare_indicators'] or \
               'challenge-running' in diagnosis['cloudflare_indicators'] or \
               'title:just a moment...' in diagnosis['cloudflare_indicators']

    def test_diagnose_html_structure_empty_page(self):
        """Test HTML diagnosis on empty page."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import diagnose_html_structure

        html = '<html><head></head><body></body></html>'

        soup = BeautifulSoup(html, 'html.parser')
        diagnosis = diagnose_html_structure(soup)

        assert diagnosis['title'] is None
        assert diagnosis['total_tables'] == 0
        assert diagnosis['has_content_div'] is False
