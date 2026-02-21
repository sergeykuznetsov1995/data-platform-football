"""
Page Navigator
==============

Handles page navigation and URL extraction for WhoScored scraper.
"""

import logging
import re
import time
from typing import Dict, List, Optional, Set, Tuple

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)

from scrapers.whoscored.constants import BASE_URL, LEAGUE_CONFIG, KNOWN_SEASON_IDS
from scrapers.base.browser import CloudflareBypass

logger = logging.getLogger(__name__)


class PageNavigator:
    """
    Handles WhoScored page navigation including:
    - Season/stage ID retrieval
    - Fixtures page navigation
    - Match URL extraction
    - Pagination through fixtures
    """

    def __init__(self, browser: CloudflareBypass):
        """
        Initialize navigator.

        Args:
            browser: CloudflareBypass browser instance
        """
        self._browser = browser
        # Cache for season/stage IDs: (league, season) -> (season_id, stage_id)
        self._season_cache: Dict[Tuple[str, int], Tuple[str, str]] = {}

    def build_fixtures_url(
        self,
        league: str,
        season_id: str,
        stage_id: str
    ) -> str:
        """
        Build URL for fixtures page.

        Args:
            league: League name
            season_id: WhoScored season ID
            stage_id: WhoScored stage ID

        Returns:
            Full URL to fixtures page

        Raises:
            ValueError: If league is not supported
        """
        config = LEAGUE_CONFIG.get(league)
        if not config:
            raise ValueError(f"League not supported: {league}")

        region_id = config['region_id']
        tournament_id = config['tournament_id']
        slug = config['slug']

        return (
            f"{BASE_URL}/Regions/{region_id}/Tournaments/{tournament_id}/"
            f"Seasons/{season_id}/Stages/{stage_id}/Fixtures/{slug}"
        )

    def build_tournament_url(self, league: str) -> str:
        """
        Build URL for tournament main page.

        Args:
            league: League name

        Returns:
            Full URL to tournament page

        Raises:
            ValueError: If league is not supported
        """
        config = LEAGUE_CONFIG.get(league)
        if not config:
            raise ValueError(f"League not supported: {league}")

        region_id = config['region_id']
        tournament_id = config['tournament_id']

        return f"{BASE_URL}/Regions/{region_id}/Tournaments/{tournament_id}/"

    def get_season_stage_ids(
        self,
        league: str,
        season: int
    ) -> Optional[Tuple[str, str]]:
        """
        Get season_id and stage_id for a given league and season.

        Args:
            league: League name
            season: Season year (e.g., 2024 for 2024/2025 season)

        Returns:
            Tuple of (season_id, stage_id) or None if not found
        """
        # Check cache first
        cache_key = (league, season)
        if cache_key in self._season_cache:
            logger.debug(f"Using cached season/stage IDs for {league} {season}")
            return self._season_cache[cache_key]

        # Check known fallback IDs
        if cache_key in KNOWN_SEASON_IDS:
            ids = KNOWN_SEASON_IDS[cache_key]
            self._season_cache[cache_key] = ids
            logger.info(f"Using known season/stage IDs for {league} {season}: {ids}")
            return ids

        # Try to fetch dynamically via Selenium
        tournament_url = self.build_tournament_url(league)

        try:
            logger.info(f"Fetching season/stage IDs for {league} {season}")

            # Navigate to tournament page
            self._browser.get_page(
                tournament_url,
                wait_for_selector='#seasons',
                wait_timeout=20,
                cloudflare_wait=10.0,
            )

            time.sleep(2)

            # Find the season dropdown
            try:
                season_select = self._browser.driver.find_element(
                    By.CSS_SELECTOR, '#seasons'
                )
            except NoSuchElementException:
                logger.error("Season dropdown not found")
                return None

            # Find all season options
            options = season_select.find_elements(By.TAG_NAME, 'option')

            # Format season string for matching (e.g., "2024/2025")
            season_str = f"{season}/{season + 1}"
            season_str_alt = str(season)

            target_option = None
            for option in options:
                option_text = option.text.strip()
                if season_str in option_text or option_text == season_str_alt:
                    target_option = option
                    break

            if not target_option:
                logger.error(
                    f"Season {season_str} not found. "
                    f"Available: {[o.text for o in options]}"
                )
                return None

            # Click on the option to navigate
            target_option.click()
            time.sleep(3)

            # Wait for page to load after selection
            self._browser._wait_for_cloudflare(5.0)

            # Extract season_id and stage_id from current URL
            current_url = self._browser.current_url
            logger.debug(f"Current URL after season selection: {current_url}")

            # URL format: /Regions/{region}/Tournaments/{tournament}/Seasons/{season_id}/Stages/{stage_id}/...
            season_match = re.search(r'/Seasons/(\d+)/Stages/(\d+)', current_url)

            if season_match:
                season_id = season_match.group(1)
                stage_id = season_match.group(2)

                self._season_cache[cache_key] = (season_id, stage_id)
                logger.info(
                    f"Found IDs for {league} {season}: "
                    f"season_id={season_id}, stage_id={stage_id}"
                )
                return season_id, stage_id

            # Alternative: try to extract from page source
            page_source = self._browser.page_source

            model_match = re.search(r'Stages/(\d+)/Fixtures', page_source)
            if model_match:
                stage_id = model_match.group(1)

                season_id_match = re.search(r'Seasons/(\d+)', page_source)
                if season_id_match:
                    season_id = season_id_match.group(1)
                    self._season_cache[cache_key] = (season_id, stage_id)
                    logger.info(
                        f"Found IDs (alt) for {league} {season}: "
                        f"season_id={season_id}, stage_id={stage_id}"
                    )
                    return season_id, stage_id

            logger.error(f"Could not extract season/stage IDs from URL: {current_url}")
            return None

        except TimeoutException as e:
            logger.error(f"Timeout getting season/stage IDs: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting season/stage IDs: {e}")
            return None

    def extract_match_urls_from_page(self) -> Set[str]:
        """
        Extract match URLs from current fixtures page.

        Returns:
            Set of unique match URLs
        """
        match_urls: Set[str] = set()

        try:
            # Find all match links - WhoScored uses different link patterns
            selectors = [
                'a[href*="/Matches/"][href*="/Live/"]',
                'a[href*="/Matches/"][href*="/MatchReport/"]',
                'a[href*="/Matches/"][href*="/Show/"]',
            ]

            for selector in selectors:
                try:
                    elements = self._browser.driver.find_elements(
                        By.CSS_SELECTOR, selector
                    )
                    for element in elements:
                        try:
                            href = element.get_attribute('href')
                            if href and '/Matches/' in href:
                                # Normalize URL
                                if not href.startswith('http'):
                                    href = f"{BASE_URL}{href}"
                                match_urls.add(href)
                        except StaleElementReferenceException:
                            continue
                except NoSuchElementException:
                    continue

            logger.debug(f"Found {len(match_urls)} match URLs on current page")
            return match_urls

        except Exception as e:
            logger.error(f"Error extracting match URLs: {e}")
            return match_urls

    def navigate_to_previous_dates(self) -> bool:
        """
        Click 'Previous' button to navigate to earlier fixtures.

        Returns:
            True if navigation successful, False otherwise
        """
        # WhoScored uses various selectors for the Previous button
        prev_selectors = [
            '.previous:not(.is-disabled)',
            'a.previous:not(.disabled)',
            'button[data-direction="prev"]:not([disabled])',
            '.dayChangeBtn.previous:not(.disabled)',
            '#date-controller .previous:not(.disabled)',
            'a[title="View previous week"]:not(.disabled)',
        ]

        for selector in prev_selectors:
            try:
                element = self._browser.driver.find_element(By.CSS_SELECTOR, selector)

                # Check if element is visible and enabled
                if element.is_displayed() and element.is_enabled():
                    # Check for disabled class
                    classes = element.get_attribute('class') or ''
                    if 'disabled' in classes or 'is-disabled' in classes:
                        continue

                    element.click()
                    time.sleep(2)

                    # Wait for Cloudflare if needed
                    self._browser._wait_for_cloudflare(3.0)

                    logger.debug("Successfully navigated to previous dates")
                    return True

            except NoSuchElementException:
                continue
            except Exception as e:
                logger.debug(f"Error clicking prev button ({selector}): {e}")
                continue

        logger.debug("Previous button not found or disabled - reached start of season")
        return False

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
        # Validate league
        if league not in LEAGUE_CONFIG:
            logger.warning(
                f"League not supported: {league}. "
                f"Supported: {list(LEAGUE_CONFIG.keys())}"
            )
            return []

        # Get season/stage IDs
        ids = self.get_season_stage_ids(league, season)
        if not ids:
            logger.error(f"Could not get season/stage IDs for {league} {season}")
            return []

        season_id, stage_id = ids

        # Build fixtures URL
        fixtures_url = self.build_fixtures_url(league, season_id, stage_id)
        logger.info(f"Fetching match URLs from: {fixtures_url}")

        all_match_urls: Set[str] = set()

        try:
            # Navigate to fixtures page
            self._browser.get_page(
                fixtures_url,
                wait_for_selector='body',
                wait_timeout=60,
                cloudflare_wait=30.0,
            )

            # Additional wait for dynamic content
            time.sleep(5)

            # Pagination loop
            pages_processed = 0
            consecutive_empty = 0

            while pages_processed < max_pages:
                # Extract match URLs from current page
                page_urls = self.extract_match_urls_from_page()
                new_urls = page_urls - all_match_urls

                if new_urls:
                    all_match_urls.update(new_urls)
                    consecutive_empty = 0
                    logger.info(
                        f"Page {pages_processed + 1}: found {len(new_urls)} new URLs, "
                        f"total: {len(all_match_urls)}"
                    )
                else:
                    consecutive_empty += 1
                    logger.debug(
                        f"Page {pages_processed + 1}: no new URLs "
                        f"(consecutive empty: {consecutive_empty})"
                    )

                    if consecutive_empty >= 5:
                        logger.info("Stopping: too many consecutive pages without new URLs")
                        break

                pages_processed += 1
                time.sleep(2)

                if not self.navigate_to_previous_dates():
                    logger.info(
                        f"Reached start of fixtures after {pages_processed} pages"
                    )
                    break

        except TimeoutException as e:
            logger.warning(f"Timeout during pagination: {e}")
            logger.info(f"Returning {len(all_match_urls)} URLs collected so far")
        except Exception as e:
            logger.error(f"Error getting match URLs: {e}")
            logger.info(f"Returning {len(all_match_urls)} URLs collected so far")

        result = sorted(list(all_match_urls))
        logger.info(f"Total match URLs for {league} {season}: {len(result)}")
        return result
