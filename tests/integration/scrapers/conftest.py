"""
Pytest fixtures for integration tests.

Provides:
- Network connectivity checks
- Tor availability checks
- Selenium/Chrome availability checks
- Real scraper instances with minimal parameters
- Delays between tests to respect rate limits
"""

import socket
import time
from typing import Optional

import pytest


# =============================================================================
# Dependency Checks
# =============================================================================

@pytest.fixture(scope="session")
def soccerdata_available() -> bool:
    """Check if soccerdata library is installed."""
    try:
        import soccerdata
        return True
    except ImportError:
        return False


@pytest.fixture
def skip_if_no_soccerdata(soccerdata_available):
    """Skip test if soccerdata is not installed."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")


# =============================================================================
# Network and Infrastructure Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def network_available() -> bool:
    """Check if network is available by connecting to a known host."""
    try:
        socket.create_connection(("www.google.com", 80), timeout=5)
        return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def tor_available() -> bool:
    """Check if Tor SOCKS proxy is available on port 9050."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('127.0.0.1', 9050))
        sock.close()
        return result == 0
    except Exception:
        return False


@pytest.fixture(scope="session")
def selenium_available() -> bool:
    """Check if Selenium with Chrome is available."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Chrome(options=options)
        driver.quit()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def undetected_chrome_available() -> bool:
    """Check if undetected-chromedriver is available."""
    try:
        import undetected_chromedriver as uc

        options = uc.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = uc.Chrome(options=options)
        driver.quit()
        return True
    except Exception:
        return False


# =============================================================================
# Skip Markers
# =============================================================================

@pytest.fixture
def skip_if_no_network(network_available):
    """Skip test if no network connectivity."""
    if not network_available:
        pytest.skip("No network connectivity")


@pytest.fixture
def skip_if_no_tor(tor_available):
    """Skip test if Tor is not available."""
    if not tor_available:
        pytest.skip("Tor not available on port 9050")


@pytest.fixture
def skip_if_no_selenium(selenium_available):
    """Skip test if Selenium is not available."""
    if not selenium_available:
        pytest.skip("Selenium with Chrome not available")


@pytest.fixture
def skip_if_no_undetected_chrome(undetected_chrome_available):
    """Skip test if undetected-chromedriver is not available."""
    if not undetected_chrome_available:
        pytest.skip("undetected-chromedriver not available")


# =============================================================================
# Rate Limiting Fixtures
# =============================================================================

@pytest.fixture
def integration_delay():
    """Add delay between tests to respect rate limits."""
    yield
    time.sleep(2)  # 2 seconds between tests


@pytest.fixture(scope="class")
def class_delay():
    """Add delay between test classes."""
    yield
    time.sleep(5)  # 5 seconds between classes


# =============================================================================
# Test Configuration
# =============================================================================

# Minimal test parameters to reduce load on external services
MINIMAL_LEAGUES = ['ENG-Premier League']
MINIMAL_SEASONS = [2024]
MINIMAL_VERSIONS = ['24']  # For SoFIFA


@pytest.fixture(scope="session")
def minimal_leagues():
    """Minimal leagues for testing."""
    return MINIMAL_LEAGUES


@pytest.fixture(scope="session")
def minimal_seasons():
    """Minimal seasons for testing."""
    return MINIMAL_SEASONS


@pytest.fixture(scope="session")
def minimal_versions():
    """Minimal FIFA versions for testing."""
    return MINIMAL_VERSIONS


# =============================================================================
# Scraper Fixtures (without Tor)
# =============================================================================

@pytest.fixture
def understat_scraper(soccerdata_available, minimal_leagues, minimal_seasons):
    """Understat scraper - no Tor required."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")

    from scrapers.understat_scraper import UnderstatScraper

    scraper = UnderstatScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def fotmob_scraper(soccerdata_available, minimal_leagues, minimal_seasons):
    """FotMob scraper - no Tor required."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")

    from scrapers.fotmob_scraper import FotMobScraper

    scraper = FotMobScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def espn_scraper(soccerdata_available, minimal_leagues, minimal_seasons):
    """ESPN scraper - no Tor required."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")

    from scrapers.espn_scraper import ESPNScraper

    scraper = ESPNScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def matchhistory_scraper(soccerdata_available, minimal_leagues, minimal_seasons):
    """MatchHistory scraper - no Tor required."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")

    from scrapers.matchhistory_scraper import MatchHistoryScraper

    scraper = MatchHistoryScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def clubelo_scraper(soccerdata_available, minimal_leagues):
    """ClubElo scraper - no Tor required (may have server issues)."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")

    from scrapers.clubelo_scraper import ClubEloScraper

    scraper = ClubEloScraper(
        leagues=minimal_leagues,
    )
    yield scraper
    scraper.close()


# =============================================================================
# Scraper Fixtures (with Tor)
# =============================================================================

@pytest.fixture
def fbref_scraper_with_tor(soccerdata_available, tor_available, minimal_leagues, minimal_seasons):
    """FBref scraper with Tor proxy."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")
    if not tor_available:
        pytest.skip("Tor not available on port 9050")

    from scrapers.fbref_scraper import FBrefScraper

    scraper = FBrefScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        proxy='socks5://127.0.0.1:9050',
    )
    yield scraper
    scraper.close()


@pytest.fixture
def sofascore_scraper_with_tor(soccerdata_available, tor_available, minimal_leagues, minimal_seasons):
    """SofaScore scraper with Tor proxy."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")
    if not tor_available:
        pytest.skip("Tor not available on port 9050")

    from scrapers.sofascore_scraper import SofaScoreScraper

    scraper = SofaScoreScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        proxy='socks5://127.0.0.1:9050',
    )
    yield scraper
    scraper.close()


@pytest.fixture
def sofifa_scraper_with_tor(soccerdata_available, tor_available, minimal_versions):
    """SoFIFA scraper with Tor proxy."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")
    if not tor_available:
        pytest.skip("Tor not available on port 9050")

    from scrapers.sofifa_scraper import SoFIFAScraper

    scraper = SoFIFAScraper(
        versions=minimal_versions,
        proxy='socks5://127.0.0.1:9050',
    )
    yield scraper
    scraper.close()


# =============================================================================
# Selenium Fixtures
# =============================================================================

@pytest.fixture
def whoscored_scraper(soccerdata_available, undetected_chrome_available, minimal_leagues, minimal_seasons):
    """WhoScored scraper with Selenium."""
    if not soccerdata_available:
        pytest.skip("soccerdata library not installed")
    if not undetected_chrome_available:
        pytest.skip("undetected-chromedriver not available")

    from scrapers.whoscored_scraper import WhoScoredScraper

    scraper = WhoScoredScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        headless=True,  # Use headless for CI
    )
    yield scraper
    scraper.close()


@pytest.fixture
def fbref_selenium_scraper(undetected_chrome_available, minimal_leagues, minimal_seasons):
    """FBref Selenium scraper (recommended)."""
    if not undetected_chrome_available:
        pytest.skip("undetected-chromedriver not available")

    from scrapers.fbref_selenium_scraper import FBrefSeleniumScraper

    scraper = FBrefSeleniumScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        headless=True,
        use_xvfb=True,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def fotmob_selenium_scraper(undetected_chrome_available, minimal_leagues, minimal_seasons):
    """FotMob Selenium scraper (recommended)."""
    if not undetected_chrome_available:
        pytest.skip("undetected-chromedriver not available")

    from scrapers.fotmob_selenium_scraper import FotMobSeleniumScraper

    scraper = FotMobSeleniumScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        headless=True,
        use_xvfb=True,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def matchhistory_direct_scraper(network_available, minimal_leagues, minimal_seasons):
    """MatchHistory direct scraper (recommended)."""
    if not network_available:
        pytest.skip("No network connectivity")

    from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper

    scraper = MatchHistoryDirectScraper(
        leagues=minimal_leagues,
        seasons=minimal_seasons,
        headless=True,
        use_xvfb=True,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def cloudflare_bypass(undetected_chrome_available):
    """CloudflareBypass instance for direct testing."""
    if not undetected_chrome_available:
        pytest.skip("undetected-chromedriver not available")

    from scrapers.base.cloudflare_bypass import CloudflareBypass

    bypass = CloudflareBypass(headless=True)
    yield bypass
    bypass.close()


# =============================================================================
# Utility Fixtures
# =============================================================================

@pytest.fixture
def proxy_manager_with_tor(tor_available):
    """ProxyManager configured with Tor."""
    if not tor_available:
        pytest.skip("Tor not available on port 9050")

    from scrapers.utils.proxy_manager import ProxyManager

    manager = ProxyManager(use_tor=True)
    yield manager


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: tests with real HTTP requests"
    )
    config.addinivalue_line(
        "markers", "slow: slow tests (>10 seconds)"
    )
    config.addinivalue_line(
        "markers", "cloudflare: tests requiring Selenium for Cloudflare bypass"
    )
    config.addinivalue_line(
        "markers", "flaky: tests that may fail due to external services"
    )
    config.addinivalue_line(
        "markers", "tor: tests requiring Tor proxy"
    )
    config.addinivalue_line(
        "markers", "soccerdata: tests requiring soccerdata library"
    )
