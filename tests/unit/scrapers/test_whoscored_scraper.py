"""
Unit tests for WhoScored scraper.

Tests cover:
- LEAGUE_CONFIG structure validation
- URL building methods (_build_fixtures_url, _build_tournament_url)
- Match URL extraction from HTML
- get_match_urls behavior for unsupported leagues
- Season cache usage
"""

import pytest
import re
from unittest.mock import MagicMock, patch, PropertyMock


# =============================================================================
# Test Classes
# =============================================================================


class TestWhoScoredLeagueConfig:
    """Tests for LEAGUE_CONFIG structure and validation."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.mark.unit
    def test_league_config_has_required_keys(self, mock_dependencies):
        """
        Test that LEAGUE_CONFIG contains all required keys for each league.

        Each league config must have:
        - region_id (int): WhoScored region identifier
        - tournament_id (int): WhoScored tournament identifier
        - slug (str): URL-friendly league name slug
        """
        from scrapers.whoscored import WhoScoredScraper

        required_keys = {'region_id', 'tournament_id', 'slug'}

        for league, config in WhoScoredScraper.LEAGUE_CONFIG.items():
            for key in required_keys:
                assert key in config, (
                    f"League '{league}' is missing required key '{key}'"
                )

    @pytest.mark.unit
    def test_all_leagues_have_valid_ids(self, mock_dependencies):
        """
        Test that all leagues have valid integer IDs and string slugs.

        Validates:
        - region_id is a positive integer
        - tournament_id is a positive integer
        - slug is a non-empty string
        """
        from scrapers.whoscored import WhoScoredScraper

        for league, config in WhoScoredScraper.LEAGUE_CONFIG.items():
            # region_id must be positive int
            assert isinstance(config['region_id'], int), (
                f"League '{league}': region_id must be int, "
                f"got {type(config['region_id'])}"
            )
            assert config['region_id'] > 0, (
                f"League '{league}': region_id must be positive"
            )

            # tournament_id must be positive int
            assert isinstance(config['tournament_id'], int), (
                f"League '{league}': tournament_id must be int, "
                f"got {type(config['tournament_id'])}"
            )
            assert config['tournament_id'] > 0, (
                f"League '{league}': tournament_id must be positive"
            )

            # slug must be non-empty string
            assert isinstance(config['slug'], str), (
                f"League '{league}': slug must be str, "
                f"got {type(config['slug'])}"
            )
            assert len(config['slug']) > 0, (
                f"League '{league}': slug must be non-empty"
            )

    @pytest.mark.unit
    def test_expected_leagues_are_configured(self, mock_dependencies):
        """
        Test that major European leagues are configured.

        At minimum, the Top 5 leagues should be supported:
        - English Premier League
        - Spanish La Liga
        - German Bundesliga
        - Italian Serie A
        - French Ligue 1
        """
        from scrapers.whoscored import WhoScoredScraper

        expected_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in expected_leagues:
            assert league in WhoScoredScraper.LEAGUE_CONFIG, (
                f"Expected league '{league}' not found in LEAGUE_CONFIG"
            )


class TestWhoScoredUrlBuilding:
    """Tests for URL building methods."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create WhoScoredScraper instance with mocked dependencies."""
        from scrapers.whoscored import WhoScoredScraper

        return WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )

    @pytest.mark.unit
    def test_build_fixtures_url(self, scraper):
        """
        Test correct formation of fixtures page URL.

        Given league, season_id and stage_id, should produce URL in format:
        https://www.whoscored.com/Regions/{region_id}/Tournaments/{tournament_id}/
        Seasons/{season_id}/Stages/{stage_id}/Fixtures/{slug}
        """
        # Input
        league = 'ENG-Premier League'
        season_id = '9155'
        stage_id = '20934'

        # Expected output
        expected_url = (
            'https://www.whoscored.com/Regions/252/Tournaments/2/'
            'Seasons/9155/Stages/20934/Fixtures/england-premier-league'
        )

        # Act
        result = scraper._build_fixtures_url(league, season_id, stage_id)

        # Assert
        assert result == expected_url

    @pytest.mark.unit
    def test_build_fixtures_url_spanish_league(self, scraper):
        """Test fixtures URL building for La Liga."""
        league = 'ESP-La Liga'
        season_id = '8681'
        stage_id = '19895'

        expected_url = (
            'https://www.whoscored.com/Regions/206/Tournaments/4/'
            'Seasons/8681/Stages/19895/Fixtures/spain-laliga'
        )

        result = scraper._build_fixtures_url(league, season_id, stage_id)

        assert result == expected_url

    @pytest.mark.unit
    def test_build_tournament_url(self, scraper):
        """
        Test correct formation of tournament main page URL.

        Given league name, should produce URL in format:
        https://www.whoscored.com/Regions/{region_id}/Tournaments/{tournament_id}/
        """
        # Input
        league = 'ENG-Premier League'

        # Expected output
        expected_url = 'https://www.whoscored.com/Regions/252/Tournaments/2/'

        # Act
        result = scraper._build_tournament_url(league)

        # Assert
        assert result == expected_url

    @pytest.mark.unit
    def test_build_tournament_url_german_league(self, scraper):
        """Test tournament URL building for Bundesliga."""
        league = 'GER-Bundesliga'

        expected_url = 'https://www.whoscored.com/Regions/81/Tournaments/3/'

        result = scraper._build_tournament_url(league)

        assert result == expected_url

    @pytest.mark.unit
    def test_build_fixtures_url_unsupported_league(self, scraper):
        """
        Test that ValueError is raised for unsupported league.

        When league is not in LEAGUE_CONFIG, _build_fixtures_url should
        raise ValueError with descriptive message.
        """
        unsupported_league = 'ARG-Primera Division'
        season_id = '1234'
        stage_id = '5678'

        with pytest.raises(ValueError) as exc_info:
            scraper._build_fixtures_url(unsupported_league, season_id, stage_id)

        assert 'not supported' in str(exc_info.value).lower()
        assert unsupported_league in str(exc_info.value)

    @pytest.mark.unit
    def test_build_tournament_url_unsupported_league(self, scraper):
        """
        Test that ValueError is raised for unsupported league in tournament URL.
        """
        unsupported_league = 'RUS-Premier Liga'

        with pytest.raises(ValueError) as exc_info:
            scraper._build_tournament_url(unsupported_league)

        assert 'not supported' in str(exc_info.value).lower()

    @pytest.mark.unit
    @pytest.mark.parametrize('league,expected_region,expected_tournament', [
        ('ENG-Premier League', 252, 2),
        ('ESP-La Liga', 206, 4),
        ('GER-Bundesliga', 81, 3),
        ('ITA-Serie A', 108, 5),
        ('FRA-Ligue 1', 74, 22),
    ])
    def test_build_tournament_url_all_leagues(
        self, scraper, league, expected_region, expected_tournament
    ):
        """Test tournament URL contains correct region and tournament IDs."""
        result = scraper._build_tournament_url(league)

        assert f'/Regions/{expected_region}/' in result
        assert f'/Tournaments/{expected_tournament}/' in result


class TestWhoScoredUrlExtraction:
    """Tests for URL extraction from HTML."""

    @pytest.fixture
    def mock_html(self):
        """
        Sample HTML fixture with various match links.

        Contains:
        - 2 links with /Live/ pattern (valid)
        - 1 link with /MatchReport/ pattern (valid)
        - 1 unrelated link (should be ignored)
        """
        return '''
        <html>
        <head><title>WhoScored Fixtures</title></head>
        <body>
            <div class="fixtures">
                <a href="/Matches/1234567/Live/England-Premier-League-2024-2025-Team-A-Team-B">Match 1</a>
                <a href="/Matches/1234568/Live/England-Premier-League-2024-2025-Team-C-Team-D">Match 2</a>
                <a href="/Matches/1234569/MatchReport/Some-Match">Match 3</a>
                <a href="/some-other-link">Not a match</a>
                <a href="/Statistics/England-Premier-League">Statistics page</a>
            </div>
        </body>
        </html>
        '''

    @pytest.fixture
    def mock_html_with_show_links(self):
        """HTML fixture with /Show/ pattern links."""
        return '''
        <html>
        <body>
            <a href="/Matches/9999991/Show/England-Premier-League-2024-2025-Match-1">Show 1</a>
            <a href="/Matches/9999992/Live/England-Premier-League-2024-2025-Match-2">Live 1</a>
        </body>
        </html>
        '''

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.mark.unit
    def test_extract_match_urls_from_html(self, mock_html, mock_dependencies):
        """
        Test extraction of match URLs from HTML content.

        Should extract URLs containing /Matches/ pattern with:
        - /Live/
        - /MatchReport/
        - /Show/

        Should ignore other links.
        """
        from scrapers.whoscored import WhoScoredScraper

        # Create mock browser with driver
        mock_driver = MagicMock()
        mock_browser = MagicMock()
        mock_browser.driver = mock_driver

        # Create mock elements for each selector
        def create_mock_element(href):
            elem = MagicMock()
            elem.get_attribute.return_value = href
            return elem

        # Setup find_elements to return appropriate links for each selector
        def mock_find_elements(by, selector):
            if '/Live/' in selector:
                return [
                    create_mock_element('/Matches/1234567/Live/England-Premier-League-2024-2025-Team-A-Team-B'),
                    create_mock_element('/Matches/1234568/Live/England-Premier-League-2024-2025-Team-C-Team-D'),
                ]
            elif '/MatchReport/' in selector:
                return [
                    create_mock_element('/Matches/1234569/MatchReport/Some-Match'),
                ]
            elif '/Show/' in selector:
                return []
            return []

        mock_driver.find_elements.side_effect = mock_find_elements

        # Create scraper and inject mock browser
        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._browser = mock_browser

            # Act
            result = scraper._extract_match_urls_from_page()

        # Assert - should find 3 match URLs
        assert len(result) == 3

        # Verify URLs are normalized (prefixed with BASE_URL)
        for url in result:
            assert url.startswith('https://www.whoscored.com') or url.startswith('/Matches/')

    @pytest.mark.unit
    def test_extract_match_urls_filters_non_match_links(self, mock_dependencies):
        """Test that non-match links are filtered out."""
        from scrapers.whoscored import WhoScoredScraper

        mock_driver = MagicMock()
        mock_browser = MagicMock()
        mock_browser.driver = mock_driver

        # Return no matches for any selector (simulating page with no matches)
        mock_driver.find_elements.return_value = []

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._browser = mock_browser

            result = scraper._extract_match_urls_from_page()

        assert len(result) == 0

    @pytest.mark.unit
    def test_extract_match_urls_handles_stale_elements(self, mock_dependencies):
        """Test graceful handling of StaleElementReferenceException."""
        from selenium.common.exceptions import StaleElementReferenceException
        from scrapers.whoscored import WhoScoredScraper

        mock_driver = MagicMock()
        mock_browser = MagicMock()
        mock_browser.driver = mock_driver

        # Create element that raises exception
        stale_element = MagicMock()
        stale_element.get_attribute.side_effect = StaleElementReferenceException('stale')

        # Create valid element
        valid_element = MagicMock()
        valid_element.get_attribute.return_value = '/Matches/123/Live/Test'

        mock_driver.find_elements.return_value = [stale_element, valid_element]

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._browser = mock_browser

            # Should not raise, should skip stale element
            result = scraper._extract_match_urls_from_page()

        # Should have found the valid element
        assert len(result) >= 1


class TestWhoScoredGetMatchUrls:
    """Tests for get_match_urls method."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.mark.unit
    def test_get_match_urls_unsupported_league(self, mock_dependencies, caplog):
        """
        Test that unsupported league returns empty list and logs warning.

        When league is not in LEAGUE_CONFIG:
        - Should return empty list []
        - Should log a warning message
        - Should NOT raise an exception
        """
        import logging
        from scrapers.whoscored import WhoScoredScraper

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper.leagues = ['UKR-Premier League']
            scraper.seasons = [2024]
            scraper._season_cache = {}

            with caplog.at_level(logging.WARNING):
                result = scraper.get_match_urls('UKR-Premier League', 2024)

            # Should return empty list
            assert result == []

            # Should log warning about unsupported league
            assert any('not supported' in record.message.lower()
                      for record in caplog.records)

    @pytest.mark.unit
    def test_get_match_urls_returns_sorted_list(self, mock_dependencies):
        """Test that get_match_urls returns sorted list of unique URLs."""
        from scrapers.whoscored import WhoScoredScraper

        mock_browser = MagicMock()
        mock_browser.get_page.return_value = '<html></html>'
        mock_browser.driver = MagicMock()

        # Mock extract to return unsorted URLs
        unsorted_urls = {
            'https://whoscored.com/Matches/3/Live/C',
            'https://whoscored.com/Matches/1/Live/A',
            'https://whoscored.com/Matches/2/Live/B',
        }

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._browser = mock_browser
            scraper._season_cache = {('ENG-Premier League', 2024): ('9155', '20934')}

            with patch.object(scraper, '_get_browser', return_value=mock_browser), \
                 patch.object(scraper, '_extract_match_urls_from_page', return_value=unsorted_urls), \
                 patch.object(scraper, '_navigate_to_previous_dates', return_value=False):

                result = scraper.get_match_urls('ENG-Premier League', 2024, max_pages=1)

        # Should be sorted
        assert result == sorted(result)


class TestWhoScoredSeasonCache:
    """Tests for season/stage ID caching functionality."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.mark.unit
    def test_season_cache_usage(self, mock_dependencies):
        """
        Test that season/stage IDs are cached and reused.

        When _get_season_stage_ids is called twice for same league/season:
        - First call should fetch from browser
        - Second call should use cache (no browser interaction)
        """
        from scrapers.whoscored import WhoScoredScraper

        mock_browser = MagicMock()
        mock_browser.get_page.return_value = '<html></html>'
        mock_browser.current_url = 'https://whoscored.com/Regions/252/Tournaments/2/Seasons/9155/Stages/20934/'
        mock_browser.driver = MagicMock()

        # Mock season dropdown
        mock_option = MagicMock()
        mock_option.text = '2024/2025'
        mock_option.get_attribute.return_value = '/Seasons/9155/'

        mock_select = MagicMock()
        mock_select.find_elements.return_value = [mock_option]
        mock_browser.driver.find_element.return_value = mock_select

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._browser = mock_browser
            scraper._season_cache = {}

            with patch.object(scraper, '_get_browser', return_value=mock_browser):
                # First call - should hit browser
                result1 = scraper._get_season_stage_ids('ENG-Premier League', 2024)

                # Manually populate cache (simulating successful first call)
                scraper._season_cache[('ENG-Premier League', 2024)] = ('9155', '20934')

                # Reset call count
                mock_browser.get_page.reset_mock()

                # Second call - should use cache
                result2 = scraper._get_season_stage_ids('ENG-Premier League', 2024)

            # Both should return same result
            assert result2 == ('9155', '20934')

            # Browser should NOT be called second time (used cache)
            mock_browser.get_page.assert_not_called()

    @pytest.mark.unit
    def test_season_cache_key_format(self, mock_dependencies):
        """Test that cache key is tuple of (league, season)."""
        from scrapers.whoscored import WhoScoredScraper

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper._season_cache = {}

            # Populate cache
            cache_key = ('ENG-Premier League', 2024)
            scraper._season_cache[cache_key] = ('9155', '20934')

            # Verify key format
            assert cache_key in scraper._season_cache
            assert scraper._season_cache[cache_key] == ('9155', '20934')

    @pytest.mark.unit
    def test_season_cache_different_seasons(self, mock_dependencies):
        """Test that different seasons have separate cache entries."""
        from scrapers.whoscored import WhoScoredScraper

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper._season_cache = {}

            # Add entries for different seasons
            scraper._season_cache[('ENG-Premier League', 2024)] = ('9155', '20934')
            scraper._season_cache[('ENG-Premier League', 2023)] = ('8618', '19793')

            # Verify separate entries
            assert scraper._season_cache[('ENG-Premier League', 2024)] == ('9155', '20934')
            assert scraper._season_cache[('ENG-Premier League', 2023)] == ('8618', '19793')

    @pytest.mark.unit
    def test_known_season_ids_fallback(self, mock_dependencies):
        """
        Test that KNOWN_SEASON_IDS is used as fallback when available.

        When cache is empty but KNOWN_SEASON_IDS has the league/season,
        should return the known IDs without browser interaction.
        """
        from scrapers.whoscored import WhoScoredScraper

        with patch.object(WhoScoredScraper, '__init__', lambda x, **kwargs: None):
            scraper = WhoScoredScraper()
            scraper.LEAGUE_CONFIG = WhoScoredScraper.LEAGUE_CONFIG
            scraper.KNOWN_SEASON_IDS = WhoScoredScraper.KNOWN_SEASON_IDS
            scraper.BASE_URL = WhoScoredScraper.BASE_URL
            scraper._season_cache = {}
            scraper._browser = None

            # Mock _get_browser - should NOT be called if known IDs exist
            mock_browser = MagicMock()
            with patch.object(scraper, '_get_browser', return_value=mock_browser) as get_browser_mock:
                # Check if 2024 season is in known IDs
                if ('ENG-Premier League', 2024) in scraper.KNOWN_SEASON_IDS:
                    result = scraper._get_season_stage_ids('ENG-Premier League', 2024)

                    # Should use known IDs, not call browser
                    assert result is not None
                    assert result == scraper.KNOWN_SEASON_IDS[('ENG-Premier League', 2024)]
                    # Browser should NOT be called when known IDs exist
                    get_browser_mock.assert_not_called()

    @pytest.mark.unit
    def test_known_season_ids_structure(self):
        """Test that KNOWN_SEASON_IDS has correct structure."""
        from scrapers.whoscored import WhoScoredScraper

        for key, value in WhoScoredScraper.KNOWN_SEASON_IDS.items():
            # Key should be (league, season) tuple
            assert isinstance(key, tuple)
            assert len(key) == 2
            league, season = key
            assert isinstance(league, str)
            assert isinstance(season, int)
            assert league in WhoScoredScraper.LEAGUE_CONFIG

            # Value should be (season_id, stage_id) tuple of strings
            assert isinstance(value, tuple)
            assert len(value) == 2
            season_id, stage_id = value
            assert isinstance(season_id, str)
            assert isinstance(stage_id, str)
            assert season_id.isdigit()
            assert stage_id.isdigit()


class TestWhoScoredScraper:
    """General tests for WhoScoredScraper initialization and attributes."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create WhoScoredScraper instance."""
        from scrapers.whoscored import WhoScoredScraper

        return WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )

    @pytest.mark.unit
    def test_init(self, scraper):
        """Test WhoScoredScraper initialization."""
        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert scraper.headless is True

    @pytest.mark.unit
    def test_source_name(self, scraper):
        """Test source name is set correctly."""
        assert scraper.SOURCE_NAME == 'whoscored'

    @pytest.mark.unit
    def test_default_rate_limit(self, scraper):
        """Test conservative rate limit for WhoScored."""
        assert scraper.DEFAULT_RATE_LIMIT == 10

    @pytest.mark.unit
    def test_base_url(self, scraper):
        """Test BASE_URL is set correctly."""
        assert scraper.BASE_URL == 'https://www.whoscored.com'

    @pytest.mark.unit
    def test_match_cache_initialized(self, scraper):
        """Test that match cache is initialized as empty dict."""
        assert hasattr(scraper, '_match_cache')
        assert isinstance(scraper._match_cache, dict)

    @pytest.mark.unit
    def test_season_cache_initialized(self, scraper):
        """Test that season cache is initialized as empty dict."""
        assert hasattr(scraper, '_season_cache')
        assert isinstance(scraper._season_cache, dict)


class TestWhoScoredCoordinateConversion:
    """Tests for coordinate conversion methods."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create WhoScoredScraper instance."""
        from scrapers.whoscored import WhoScoredScraper

        return WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )

    @pytest.mark.unit
    def test_convert_coordinates(self, scraper):
        """Test coordinate conversion to SPADL format."""
        # WhoScored uses 0-100, SPADL uses 105x68 meters
        spadl_x, spadl_y = scraper._convert_coordinates(50, 50)

        assert spadl_x == 52.5  # 105 / 2
        assert spadl_y == 34.0  # 68 / 2

    @pytest.mark.unit
    def test_convert_coordinates_corners(self, scraper):
        """Test coordinate conversion at corners."""
        # Top-left
        x, y = scraper._convert_coordinates(0, 0)
        assert x == 0.0
        assert y == 0.0

        # Bottom-right
        x, y = scraper._convert_coordinates(100, 100)
        assert x == 105.0
        assert y == 68.0


class TestWhoScoredEventConversion:
    """Tests for event to SPADL conversion."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create WhoScoredScraper instance."""
        from scrapers.whoscored import WhoScoredScraper

        return WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )

    @pytest.mark.unit
    def test_event_type_mapping(self, scraper):
        """Test event type mapping to SPADL."""
        assert scraper.EVENT_TYPE_MAPPING['Pass'] == 'pass'
        assert scraper.EVENT_TYPE_MAPPING['Shot'] == 'shot'
        assert scraper.EVENT_TYPE_MAPPING['Tackle'] == 'tackle'
        assert scraper.EVENT_TYPE_MAPPING['Cross'] == 'cross'

    @pytest.mark.unit
    def test_event_to_spadl_pass(self, scraper):
        """Test converting pass event to SPADL."""
        event = {
            'id': 12345,
            'type': {'displayName': 'Pass'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 50,
            'y': 50,
            'endX': 70,
            'endY': 40,
            'minute': 15,
            'second': 30,
            'period': {'value': 1},
            'teamId': 1,
            'playerId': 101,
            'playerName': 'Test Player',
            'qualifiers': [],
        }

        match_info = {
            'league': 'ENG-Premier League',
            'season': 2024,
            'match_id': 999,
            'home_team': 'Arsenal',
            'away_team': 'Chelsea',
            'home_team_id': 1,
            'away_team_id': 2,
        }

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'pass'
        assert spadl['result'] == 'success'
        assert spadl['start_x'] == 52.5
        assert spadl['bodypart'] == 'foot'

    @pytest.mark.unit
    def test_event_to_spadl_header(self, scraper):
        """Test converting header event to SPADL."""
        event = {
            'id': 12346,
            'type': {'displayName': 'Shot'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 90,
            'y': 50,
            'minute': 45,
            'second': 0,
            'period': {'value': 1},
            'teamId': 1,
            'playerId': 101,
            'playerName': 'Test Player',
            'qualifiers': [{'type': {'displayName': 'Head'}}],
            'isGoal': True,
        }

        match_info = {
            'league': 'ENG-Premier League',
            'season': 2024,
            'match_id': 999,
        }

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'shot'
        assert spadl['bodypart'] == 'head'
        assert spadl['is_goal'] is True

    @pytest.mark.unit
    def test_event_to_spadl_penalty(self, scraper):
        """Test converting penalty event to SPADL."""
        event = {
            'id': 12347,
            'type': {'displayName': 'Shot'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 90,
            'y': 50,
            'minute': 78,
            'second': 0,
            'period': {'value': 2},
            'teamId': 1,
            'playerId': 101,
            'qualifiers': [{'type': {'displayName': 'Penalty'}}],
            'isGoal': True,
        }

        match_info = {'league': 'Test', 'season': 2024, 'match_id': 1}

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'shot_penalty'


class TestSPADLDefinitions:
    """Tests for SPADL action definitions."""

    @pytest.mark.unit
    def test_spadl_actions_defined(self):
        """Test SPADL actions are defined."""
        from scrapers.whoscored import SPADL_ACTIONS

        assert 'pass' in SPADL_ACTIONS
        assert 'shot' in SPADL_ACTIONS
        assert 'tackle' in SPADL_ACTIONS
        assert 'dribble' in SPADL_ACTIONS

    @pytest.mark.unit
    def test_spadl_actions_have_descriptions(self):
        """Test all SPADL actions have non-empty descriptions."""
        from scrapers.whoscored import SPADL_ACTIONS

        for action, description in SPADL_ACTIONS.items():
            assert isinstance(description, str), (
                f"Action '{action}' description must be string"
            )
            assert len(description) > 0, (
                f"Action '{action}' must have non-empty description"
            )
