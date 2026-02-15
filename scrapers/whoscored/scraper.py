"""
WhoScored Scraper
=================

Main scraper class for WhoScored event data with Selenium browser automation.
Converts event data to SPADL (Soccer Player Action Description Language) format.

Source: https://www.whoscored.com

NOTE: WhoScored requires browser automation due to Cloudflare protection.
This scraper should be run with headless=False for best results.
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.base.cloudflare_bypass import CloudflareBypass
from scrapers.whoscored.constants import LEAGUE_CONFIG, KNOWN_SEASON_IDS, BASE_URL, EVENT_TYPE_MAPPING
from scrapers.whoscored.spadl_converter import event_to_spadl, convert_coordinates
from scrapers.whoscored.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


class WhoScoredScraper(SeleniumScraper):
    """
    Scraper for WhoScored event data using Selenium.

    WhoScored provides:
    - Detailed match events (passes, shots, tackles, etc.)
    - Player ratings
    - Match statistics
    - Heat maps data

    Events are converted to SPADL format for standardization.

    IMPORTANT: WhoScored uses Cloudflare protection. Use headless=False
    for better success rate.

    Usage:
        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=False  # Recommended
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'whoscored'
    DEFAULT_RATE_LIMIT = 10  # Very conservative due to Cloudflare

    # Re-export constants for backwards compatibility
    LEAGUE_CONFIG = LEAGUE_CONFIG
    KNOWN_SEASON_IDS = KNOWN_SEASON_IDS
    BASE_URL = BASE_URL
    EVENT_TYPE_MAPPING = EVENT_TYPE_MAPPING

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = False,  # Recommended False for WhoScored
        **kwargs
    ):
        """
        Initialize WhoScored scraper.

        Args:
            leagues: List of league names to scrape
            seasons: List of season years (e.g., 2024 for 2024/2025)
            headless: Whether to run browser headless (False recommended)
            **kwargs: Additional arguments passed to SeleniumScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            **kwargs
        )
        self._match_cache: Dict[str, Dict] = {}
        self._navigator: Optional[PageNavigator] = None
        # Backwards compatibility: expose season cache
        self._season_cache: Dict = {}

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with WhoScored-specific configuration."""
        if self._browser is None:
            # Get proxy from proxy_manager if available
            proxy_url = None
            if hasattr(self, 'proxy_manager') and self.proxy_manager:
                proxy_url = self.proxy_manager.get_proxy()
            elif hasattr(self, 'proxy') and self.proxy:
                proxy_url = self.proxy

            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=getattr(self, 'use_xvfb', False),
                proxy=proxy_url,
                page_load_timeout=60,  # WhoScored can be slow
                use_flaresolverr=getattr(self, 'use_flaresolverr', False),
                flaresolverr_url=getattr(self, 'flaresolverr_url', 'http://flaresolverr:8191'),
            )
        return self._browser

    def _get_navigator(self) -> PageNavigator:
        """Get page navigator instance."""
        if self._navigator is None:
            self._navigator = PageNavigator(
                browser=self._get_browser(),
                use_flaresolverr=getattr(self, 'use_flaresolverr', False),
            )
        return self._navigator

    # Delegate methods for backwards compatibility and testing
    def _build_fixtures_url(self, league: str, season_id: str, stage_id: str) -> str:
        """Build URL for fixtures page (delegated to PageNavigator)."""
        return self._get_navigator().build_fixtures_url(league, season_id, stage_id)

    def _build_tournament_url(self, league: str) -> str:
        """Build URL for tournament main page (delegated to PageNavigator)."""
        return self._get_navigator().build_tournament_url(league)

    def _get_season_stage_ids(self, league: str, season: int):
        """Get season/stage IDs (delegated to PageNavigator)."""
        return self._get_navigator().get_season_stage_ids(league, season)

    def _extract_match_urls_from_page(self):
        """Extract match URLs from current page (delegated to PageNavigator)."""
        return self._get_navigator().extract_match_urls_from_page()

    def _navigate_to_previous_dates(self) -> bool:
        """Navigate to previous dates (delegated to PageNavigator)."""
        return self._get_navigator().navigate_to_previous_dates()

    def _convert_coordinates(self, x: float, y: float):
        """Convert WhoScored coordinates to SPADL format."""
        return convert_coordinates(x, y)

    def _event_to_spadl(self, event, match_info):
        """Convert WhoScored event to SPADL format."""
        return event_to_spadl(event, match_info)

    def _navigate_to_match(self, match_url: str) -> bool:
        """
        Navigate to match page and wait for data to load.

        Args:
            match_url: Full match URL

        Returns:
            True if successful
        """
        browser = self._get_browser()

        try:
            html = browser.get_page(
                match_url,
                wait_for_selector='#player-table-statistics-body',
                wait_timeout=30,
                cloudflare_wait=10.0,
            )

            # Check if page loaded correctly
            if 'matchCentreData' in html or 'incidentEvents' in html:
                return True

            logger.warning(f"Match data not found on page: {match_url}")
            return False

        except Exception as e:
            logger.error(f"Error navigating to match: {e}")
            return False

    def _extract_match_data(self) -> Optional[Dict[str, Any]]:
        """
        Extract match data from page JavaScript.

        Returns:
            Match data dictionary or None if extraction failed
        """
        browser = self._get_browser()

        try:
            # Try to extract matchCentreData from page
            script = """
            if (typeof matchCentreData !== 'undefined') {
                return JSON.stringify(matchCentreData);
            }
            return null;
            """
            result = browser.execute_script(script)

            if result:
                return json.loads(result)

            # Alternative: extract from script tags
            page_source = browser.page_source
            pattern = r'matchCentreData\s*=\s*(\{.*?\});'
            match = re.search(pattern, page_source, re.DOTALL)

            if match:
                return json.loads(match.group(1))

            logger.warning("Could not extract matchCentreData")
            return None

        except Exception as e:
            logger.error(f"Error extracting match data: {e}")
            return None

    def read_match_events(
        self,
        match_url: str,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """
        Read and convert match events to SPADL format.

        Args:
            match_url: Full match URL
            league: League name
            season: Season year

        Returns:
            DataFrame with SPADL events or None if extraction failed
        """
        logger.info(f"Fetching WhoScored events: {match_url}")

        try:
            # Navigate to match page
            if not self._navigate_to_match(match_url):
                return None

            # Wait for dynamic content
            time.sleep(2)

            # Extract match data
            match_data = self._extract_match_data()
            if not match_data:
                return None

            # Extract match info
            home_team = match_data.get('home', {})
            away_team = match_data.get('away', {})

            match_info = {
                'league': league,
                'season': season,
                'match_id': match_data.get('matchId'),
                'match_date': match_data.get('startDate', '').split('T')[0],
                'home_team': home_team.get('name'),
                'away_team': away_team.get('name'),
                'home_team_id': home_team.get('teamId'),
                'away_team_id': away_team.get('teamId'),
            }

            # Convert events to SPADL
            events = match_data.get('events', [])
            spadl_events = []

            for event in events:
                try:
                    spadl_event = event_to_spadl(event, match_info)
                    spadl_events.append(spadl_event)
                except Exception as e:
                    logger.debug(f"Error converting event: {e}")
                    continue

            if spadl_events:
                df = pd.DataFrame(spadl_events)
                df = self._add_metadata(df, 'events_spadl')
                return df

            return None

        except Exception as e:
            logger.error(f"Error reading match events: {e}")
            return None

    def get_match_urls(
        self,
        league: str,
        season: int,
        max_pages: int = 50
    ) -> List[str]:
        """
        Get list of match URLs for a league and season.

        Navigates through the fixtures page and collects all match URLs
        by paginating backwards through the dates.

        Args:
            league: League name (e.g., 'ENG-Premier League')
            season: Season year (e.g., 2024 for 2024/2025 season)
            max_pages: Maximum number of pages to navigate (default: 50)

        Returns:
            List of unique match URLs sorted alphabetically
        """
        return self._get_navigator().get_match_urls(league, season, max_pages)

    def scrape_match(
        self,
        match_url: str,
        league: str,
        season: int
    ) -> Dict[str, str]:
        """
        Scrape a single match.

        Args:
            match_url: Full match URL
            league: League name
            season: Season year

        Returns:
            Dictionary with table path or empty dict if failed
        """
        df = self.read_match_events(match_url, league, season)

        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='whoscored_events_spadl',
                partition_cols=['league', 'season'],
            )
            return {'events': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all WhoScored data for configured leagues and seasons.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting WhoScored scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        all_events = []

        for league in self.leagues:
            for season in self.seasons:
                match_urls = self.get_match_urls(league, season)

                for url in match_urls:
                    try:
                        df = self.read_match_events(url, league, season)
                        if df is not None and not df.empty:
                            all_events.append(df)

                        # Rate limiting between matches
                        time.sleep(5)

                    except Exception as e:
                        logger.error(f"Error scraping match {url}: {e}")
                        continue

        if all_events:
            combined_df = pd.concat(all_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='whoscored_events_spadl',
                partition_cols=['league', 'season'],
            )
            results['events'] = table_path

        logger.info(f"WhoScored scrape complete: {list(results.keys())}")
        return results
