"""
FBref Scraper
=============

Main scraper class for FBref football statistics using Selenium with Cloudflare bypass.

Source: https://fbref.com

NOTE: FBref uses Cloudflare protection. This scraper uses undetected-chromedriver
for bypassing bot detection.

Memory Optimization Notes:
- Page cache is cleared after each league to prevent OOM
- gc.collect() is called after processing each league
- Browser is restarted after MAX_PAGES_BEFORE_BROWSER_RESTART pages
- Intermediate DataFrames are explicitly deleted after merge operations
"""

import gc
import logging
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.base.cloudflare_bypass import CloudflareBypass
from scrapers.fbref.constants import (
    BASE_URL,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
    PLAYER_MATCH_STAT_TYPES,
    DEFAULT_RATE_LIMIT,
)
from scrapers.fbref.url_builder import (
    format_season,
    get_schedule_url,
    get_stats_url,
)
from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    parse_table,
    find_schedule_table,
    find_team_stats_table,
    find_player_stats_table,
    parse_shots_table,
    parse_lineup_table,
    parse_events_from_scorebox,
    parse_team_match_stats_table,
    diagnose_html_structure,
)

logger = logging.getLogger(__name__)


class FBrefScraper(SeleniumScraper):
    """
    Scraper for FBref data using Selenium with Cloudflare bypass.

    FBref provides:
    - Match schedules and scores
    - Team season statistics
    - Player season statistics
    - Advanced metrics (xG, xA, etc.)

    IMPORTANT: FBref tables are often hidden in HTML comments.
    This scraper handles comment extraction automatically.

    Usage:
        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = DEFAULT_RATE_LIMIT

    # Memory management constants
    MAX_PAGES_BEFORE_BROWSER_RESTART = 30  # Restart browser to prevent memory/FD leaks
    MAX_CACHE_SIZE = 30  # Maximum pages to keep in cache

    # Re-export constants for backwards compatibility
    BASE_URL = BASE_URL
    LEAGUE_IDS = LEAGUE_IDS
    PLAYER_STAT_TYPES = PLAYER_STAT_TYPES
    TEAM_STAT_TYPES = TEAM_STAT_TYPES
    KEEPER_STAT_TYPES = KEEPER_STAT_TYPES
    PLAYER_MATCH_STAT_TYPES = PLAYER_MATCH_STAT_TYPES

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = True,
        use_xvfb: bool = True,
        proxy_file: Optional[str] = None,
        use_flaresolverr: bool = False,
        flaresolverr_url: str = "http://flaresolverr:8191",
        use_nodriver: bool = False,
        nodriver_cloudflare_wait: float = 30.0,
        **kwargs
    ):
        """
        Initialize FBref scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display to bypass Cloudflare headless detection
            proxy_file: Path to file with proxies (format: host:port:user:pass)
            use_flaresolverr: Use FlareSolverr instead of Selenium for Cloudflare bypass
            flaresolverr_url: URL of FlareSolverr service
            use_nodriver: Use nodriver instead of undetected-chromedriver
                         (better Cloudflare bypass, async API)
            nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (nodriver)
            **kwargs: Additional arguments for SeleniumScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            use_xvfb=use_xvfb,
            proxy_file=proxy_file,
            use_flaresolverr=use_flaresolverr,
            flaresolverr_url=flaresolverr_url,
            **kwargs
        )
        self.use_nodriver = use_nodriver
        self.nodriver_cloudflare_wait = nodriver_cloudflare_wait
        self._page_cache: Dict[str, str] = {}
        self._pages_fetched: int = 0  # Counter for browser restart
        self._nodriver_browser = None  # Separate instance for nodriver

    def _get_proxy_url(self) -> Optional[str]:
        """Get proxy URL from manager or direct proxy setting."""
        if self._proxy_manager and self._proxy_manager.total_count > 0:
            proxy_obj = self._proxy_manager.get_proxy()
            if proxy_obj:
                logger.info(
                    f"Using proxy for FBref: {proxy_obj.host}:{proxy_obj.port}"
                )
                return proxy_obj.url
        elif self.proxy:
            return self.proxy
        return None

    def _get_nodriver_browser(self):
        """Get nodriver browser with FBref-specific configuration."""
        if self._nodriver_browser is None:
            from scrapers.base.browser import get_nodriver_bypass
            NodriverBypass = get_nodriver_bypass()

            proxy_url = self._get_proxy_url()

            self._nodriver_browser = NodriverBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=proxy_url,
                cloudflare_wait=self.nodriver_cloudflare_wait,
                page_load_timeout=120.0,
                max_retries=3,
                use_cf_verify=True,  # Re-enabled with better timeout handling
            )
            logger.info(
                f"Initialized nodriver browser (headless={self.headless}, "
                f"cloudflare_wait={self.nodriver_cloudflare_wait}s)"
            )
        return self._nodriver_browser

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with FBref-specific configuration and proxy support."""
        if self._browser is None:
            proxy_url = self._get_proxy_url()

            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=proxy_url,
                page_load_timeout=120,
                use_flaresolverr=self.use_flaresolverr,
                flaresolverr_url=self.flaresolverr_url,
            )
        return self._browser

    # Delegate URL building for backwards compatibility
    def _format_season(self, season: int) -> str:
        """Format season year to FBref format."""
        return format_season(season)

    def _get_schedule_url(self, league: str, season: int) -> str:
        """Build URL for schedule/fixtures page."""
        return get_schedule_url(league, season)

    def _get_stats_url(
        self,
        league: str,
        season: int,
        stat_type: str,
        for_squads: bool = False
    ) -> str:
        """Build URL for statistics page."""
        return get_stats_url(league, season, stat_type, for_squads)

    # Delegate HTML parsing for backwards compatibility
    def _extract_tables_from_comments(
        self,
        soup: BeautifulSoup
    ) -> Dict[str, BeautifulSoup]:
        """Extract tables hidden in HTML comments."""
        return extract_tables_from_comments(soup)

    def _parse_table(
        self,
        soup: BeautifulSoup,
        table_id: str,
        comment_tables: Optional[Dict[str, BeautifulSoup]] = None
    ) -> Optional[pd.DataFrame]:
        """Parse HTML table to DataFrame."""
        return parse_table(soup, table_id, comment_tables)

    def _fetch_page(self, url: str, use_cache: bool = True) -> Optional[str]:
        """
        Fetch page HTML with caching support.

        Args:
            url: URL to fetch
            use_cache: Whether to use page cache

        Returns:
            Page HTML or None
        """
        if use_cache and url in self._page_cache:
            logger.debug(f"Using cached page: {url}")
            return self._page_cache[url]

        try:
            # Rate limiting
            self._rate_limiter.acquire()

            # Use nodriver if enabled
            if self.use_nodriver:
                html = self._fetch_page_nodriver(url)
            else:
                html = self._fetch_page_selenium(url)

            # Diagnostic logging
            if html:
                html_len = len(html)
                has_tables = '<table' in html
                has_cloudflare = any(cf in html.lower() for cf in [
                    'just a moment', 'checking your browser',
                    'cf-browser-verification', 'challenge-running'
                ])

                logger.info(
                    f"Page fetched: {url} | "
                    f"length={html_len}, has_tables={has_tables}, "
                    f"cloudflare_blocked={has_cloudflare}"
                )

                if has_cloudflare:
                    logger.warning(
                        f"Cloudflare challenge detected in response for {url}. "
                        f"HTML preview: {html[:500]}"
                    )
                    # Return None if page is still blocked
                    self._stats['failures'] += 1
                    return None

                if not has_tables and html_len < 5000:
                    logger.warning(
                        f"Page appears incomplete or blocked: {url}. "
                        f"HTML preview: {html[:500]}"
                    )
            else:
                logger.warning(f"Empty HTML returned for {url}")
                self._stats['failures'] += 1
                return None

            if use_cache:
                self._page_cache[url] = html
                self._manage_cache_size()

            self._stats['successes'] += 1
            self._maybe_restart_browser()
            return html

        except Exception as e:
            self._stats['failures'] += 1
            logger.error(f"Error fetching page {url}: {e}", exc_info=True)
            return None

    def _fetch_page_nodriver(self, url: str) -> Optional[str]:
        """Fetch page using nodriver (better Cloudflare bypass)."""
        browser = self._get_nodriver_browser()

        logger.info(f"Fetching page with nodriver: {url}")

        html = browser.get_page(
            url,
            wait_timeout=30,
            cloudflare_wait=self.nodriver_cloudflare_wait,
        )

        # get_page() already returns fully loaded HTML after Cloudflare bypass
        # No need to call page_source again (it can hang without timeout)
        if html:
            logger.info(f"HTML received from nodriver, length={len(html)} bytes")
        else:
            logger.warning(f"No HTML received from nodriver for {url}")

        return html

    def _fetch_page_selenium(self, url: str) -> Optional[str]:
        """Fetch page using Selenium/undetected-chromedriver."""
        browser = self._get_browser()

        logger.info(f"Fetching page: {url} (flaresolverr={self.use_flaresolverr})")

        html = browser.get_page(
            url,
            wait_timeout=30,
            cloudflare_wait=30.0,  # Reduced from 90s - early exit checks every 5s
        )

        # For FlareSolverr mode, HTML is already complete
        # For Selenium mode, wait for dynamic content and get updated source
        if not self.use_flaresolverr:
            # Additional wait for dynamic content
            time.sleep(2)
            # Get updated page source from Selenium
            html = browser.page_source

        return html

    def read_schedule(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read match schedule/fixtures.

        Args:
            league: League name (uses first configured if not specified)
            season: Season year (uses first configured if not specified)

        Returns:
            DataFrame with schedule data
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        url = get_schedule_url(league, season)
        logger.info(f"Fetching FBref schedule: {url}")

        html = self._fetch_page(url)
        if not html:
            logger.error(f"Failed to fetch HTML for schedule: {league} {season}")
            return None

        logger.info(f"Parsing HTML ({len(html)} bytes) with BeautifulSoup...")
        soup = BeautifulSoup(html, 'html.parser')
        logger.info("BeautifulSoup parsing complete")

        # Diagnostic logging
        logger.debug("Running HTML structure diagnosis...")
        diagnosis = diagnose_html_structure(soup)
        logger.debug("HTML diagnosis complete")
        logger.info(
            f"HTML diagnosis for schedule {league} {season}: "
            f"title='{diagnosis['title']}', "
            f"tables={diagnosis['total_tables']}, "
            f"table_ids={diagnosis['table_ids'][:5]}, "
            f"comments={diagnosis['comment_count']}, "
            f"cloudflare={diagnosis['cloudflare_indicators']}"
        )

        if diagnosis['cloudflare_indicators']:
            logger.error(
                f"Cloudflare block detected for {league} {season}. "
                f"Indicators: {diagnosis['cloudflare_indicators']}"
            )
            return None

        logger.debug("Extracting tables from HTML comments...")
        comment_tables = extract_tables_from_comments(soup)
        logger.debug(f"Found {len(comment_tables)} tables in comments")

        # Get league info for table ID
        season_str = format_season(season)
        league_info = LEAGUE_IDS.get(league, {})
        comp_id = league_info.get('comp_id', '9')

        logger.debug(f"Finding schedule table for season={season_str}, comp_id={comp_id}...")
        df = find_schedule_table(soup, comment_tables, season_str, comp_id)
        logger.debug(f"Schedule table found: {df is not None and not df.empty}")

        if df is None or df.empty:
            logger.warning(
                f"No schedule data found for {league} {season}. "
                f"Available tables: {diagnosis['table_ids']}, "
                f"Comment tables: {list(comment_tables.keys())}"
            )
            return None

        # Extract match URLs from the table HTML
        # (pd.read_html extracts text, not href URLs)
        from scrapers.fbref.html_parser import extract_match_urls_from_schedule
        match_urls = extract_match_urls_from_schedule(
            soup, comment_tables, season_str, comp_id
        )
        if match_urls:
            # Add match_url column to DataFrame
            df['match_url'] = df.index.map(match_urls)
            logger.info(f"Extracted {len(match_urls)} match URLs from schedule")

        # Add metadata
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'schedule')

        logger.info(f"Parsed {len(df)} schedule entries")
        return df

    def read_team_season_stats(
        self,
        stat_type: str = 'stats',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read team/squad statistics for a season.

        Args:
            stat_type: Type of statistics (stats, shooting, passing, etc.)
            league: League name
            season: Season year

        Returns:
            DataFrame with team stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        url = get_stats_url(league, season, stat_type, for_squads=True)
        logger.info(f"Fetching FBref team stats ({stat_type}): {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = find_team_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            logger.warning(
                f"No team stats found for {league} {season} ({stat_type})"
            )
            return None

        # Add metadata
        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'team_stats_{stat_type}')

        logger.info(f"Parsed {len(df)} team stat entries")
        return df

    def read_player_season_stats(
        self,
        stat_type: str = 'stats',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read player statistics for a season.

        Args:
            stat_type: Type of statistics (stats, shooting, passing, etc.)
            league: League name
            season: Season year

        Returns:
            DataFrame with player stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        url = get_stats_url(league, season, stat_type, for_squads=False)
        logger.info(f"Fetching FBref player stats ({stat_type}): {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = find_player_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            logger.warning(
                f"No player stats found for {league} {season} ({stat_type})"
            )
            return None

        # Clean player names (remove rank numbers)
        if 'Player' in df.columns:
            df['Player'] = df['Player'].astype(str).str.replace(
                r'^\d+\s*', '', regex=True
            )

        # Add metadata
        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'player_stats_{stat_type}')

        logger.info(f"Parsed {len(df)} player stat entries")
        return df

    def read_keeper_stats(
        self,
        stat_type: str = 'keeper',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read goalkeeper statistics for a season.

        Args:
            stat_type: Type of keeper statistics (keeper, keeper_adv)
            league: League name
            season: Season year

        Returns:
            DataFrame with goalkeeper stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        url = get_stats_url(league, season, stat_type, for_squads=False)
        logger.info(f"Fetching FBref keeper stats ({stat_type}): {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = find_player_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            logger.warning(
                f"No keeper stats found for {league} {season} ({stat_type})"
            )
            return None

        # Clean player names
        if 'Player' in df.columns:
            df['Player'] = df['Player'].astype(str).str.replace(
                r'^\d+\s*', '', regex=True
            )

        # Add metadata
        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'keeper_stats_{stat_type}')

        logger.info(f"Parsed {len(df)} keeper stat entries")
        return df

    def read_player_match_stats(
        self,
        match_id: str,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read player statistics for a specific match.

        Args:
            match_id: FBref match ID (extracted from schedule)
            league: League name (for metadata)
            season: Season year (for metadata)

        Returns:
            DataFrame with player match stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        url = f"{BASE_URL}/en/matches/{match_id}"
        logger.info(f"Fetching FBref match stats: {url}")

        html = self._fetch_page(url, use_cache=False)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        all_stats = []

        # Try to find player stats tables (home and away)
        # Table IDs are typically like stats_<team_id>_summary
        for table in soup.find_all('table'):
            table_id = table.get('id', '')
            if 'summary' in table_id.lower() and 'stats' in table_id.lower():
                df = parse_table(soup, table_id, comment_tables)
                if df is not None and not df.empty:
                    all_stats.append(df)

        # Also check comment tables
        for table_id in comment_tables:
            if 'summary' in table_id.lower() and 'stats' in table_id.lower():
                df = parse_table(soup, table_id, comment_tables)
                if df is not None and not df.empty:
                    all_stats.append(df)

        if not all_stats:
            logger.warning(f"No player match stats found for match {match_id}")
            return None

        # Combine home and away stats
        df = pd.concat(all_stats, ignore_index=True)

        # Add metadata
        df['match_id'] = match_id
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'player_match_stats')

        logger.info(f"Parsed {len(df)} player match stat entries")
        return df

    def read_shot_events(
        self,
        match_id: str,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read detailed shot events for a specific match.

        Shot events include:
        - xG (expected goals)
        - Shot coordinates
        - Shot type (foot, header, etc.)
        - Shot outcome (goal, saved, blocked, off target, etc.)
        - Shooter and assisting player
        - Minute

        Args:
            match_id: FBref match ID
            league: League name (for metadata)
            season: Season year (for metadata)

        Returns:
            DataFrame with shot events or None
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        url = f"{BASE_URL}/en/matches/{match_id}"
        logger.info(f"Fetching FBref shot events: {url}")

        html = self._fetch_page(url, use_cache=True)  # Cache since match page used for multiple reads
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = parse_shots_table(soup, comment_tables)

        if df is None or df.empty:
            logger.debug(f"No shot events found for match {match_id}")
            return None

        # Add metadata
        df['match_id'] = match_id
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'shot_events')

        logger.info(f"Parsed {len(df)} shot events for match {match_id}")
        return df

    def read_match_events(
        self,
        match_id: str,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read match events (goals, cards, substitutions).

        Events include:
        - Goals (with scorers and assisters)
        - Yellow and red cards
        - Substitutions
        - Penalties
        - Own goals

        Args:
            match_id: FBref match ID
            league: League name (for metadata)
            season: Season year (for metadata)

        Returns:
            DataFrame with match events or None
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        url = f"{BASE_URL}/en/matches/{match_id}"
        logger.info(f"Fetching FBref match events: {url}")

        html = self._fetch_page(url, use_cache=True)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        df = parse_events_from_scorebox(soup)

        if df is None or df.empty:
            logger.debug(f"No match events found for match {match_id}")
            return None

        # Add metadata
        df['match_id'] = match_id
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'match_events')

        logger.info(f"Parsed {len(df)} events for match {match_id}")
        return df

    def read_lineup(
        self,
        match_id: str,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read lineup/squad information for a specific match.

        Lineups include:
        - Starting XI
        - Substitutes
        - Player positions
        - Jersey numbers

        Args:
            match_id: FBref match ID
            league: League name (for metadata)
            season: Season year (for metadata)

        Returns:
            DataFrame with lineup data or None
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        url = f"{BASE_URL}/en/matches/{match_id}"
        logger.info(f"Fetching FBref lineups: {url}")

        html = self._fetch_page(url, use_cache=True)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        df = parse_lineup_table(soup)

        if df is None or df.empty:
            logger.debug(f"No lineup data found for match {match_id}")
            return None

        # Add metadata
        df['match_id'] = match_id
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'lineups')

        logger.info(f"Parsed {len(df)} lineup entries for match {match_id}")
        return df

    def read_team_match_stats(
        self,
        match_id: str,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read team-level statistics for a specific match.

        Team match stats include aggregated statistics for each team
        in the match (passes, shots, possession, etc.).

        Args:
            match_id: FBref match ID
            league: League name (for metadata)
            season: Season year (for metadata)

        Returns:
            DataFrame with team match stats or None
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        url = f"{BASE_URL}/en/matches/{match_id}"
        logger.info(f"Fetching FBref team match stats: {url}")

        html = self._fetch_page(url, use_cache=True)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = parse_team_match_stats_table(soup, comment_tables)

        if df is None or df.empty:
            logger.debug(f"No team match stats found for match {match_id}")
            return None

        # Add metadata
        df['match_id'] = match_id
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'team_match_stats')

        logger.info(f"Parsed team match stats for match {match_id}")
        return df

    def _merge_team_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """
        Merge multiple team stat DataFrames into one extended table.

        Similar to _merge_player_stats but for team/squad statistics.
        Joins on team name.

        Args:
            data: Dictionary mapping stat_type to DataFrame
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with extended team stats or None
        """
        base = data.get('stats')
        if base is None or base.empty:
            logger.warning("No base 'stats' data to merge for teams")
            return None

        logger.debug(f"Base team 'stats' columns: {list(base.columns)[:15]}...")

        # Identify join column (team identifier)
        team_col = self._find_join_column(base, ['Squad', 'Team', 'squad', 'team'])
        if not team_col:
            logger.warning(
                f"No suitable join column found for team merging. "
                f"Available columns: {list(base.columns)[:10]}..."
            )
            return base

        join_cols = [team_col]
        logger.info(f"Merging team stats on column: {team_col}")

        # Track columns that are already in base
        base_cols = set(base.columns)

        for stat_type, df in data.items():
            if stat_type == 'stats' or df is None or df.empty:
                continue

            logger.debug(
                f"Processing team {stat_type}, columns: {list(df.columns)[:10]}..."
            )

            # Find matching join column in this DataFrame
            df_team_col = self._find_join_column(df, [team_col, 'Squad', 'Team'])
            if not df_team_col:
                logger.warning(
                    f"Cannot merge team {stat_type}: no common join column. "
                    f"Looking for: {join_cols}, "
                    f"Available: {list(df.columns)[:10]}..."
                )
                continue

            # Rename if needed
            if df_team_col != team_col:
                df = df.rename(columns={df_team_col: team_col})

            # Get columns to merge (excluding join column and duplicates)
            merge_cols = [team_col]
            for col in df.columns:
                if col not in base_cols and col != team_col:
                    merge_cols.append(col)

            if len(merge_cols) > 1:
                try:
                    base = base.merge(
                        df[merge_cols],
                        on=team_col,
                        how='left',
                        suffixes=('', f'_{stat_type}')
                    )
                    base_cols = set(base.columns)
                    logger.debug(
                        f"Merged team {stat_type}: "
                        f"{len(merge_cols) - 1} new columns"
                    )
                except Exception as e:
                    logger.error(f"Error merging team {stat_type}: {e}")

        # Add league/season metadata
        base['league'] = league
        base['season'] = season

        logger.info(
            f"Merged team stats: {len(base)} rows, {len(base.columns)} columns"
        )
        return base

    def _find_join_column(
        self,
        df: pd.DataFrame,
        candidates: List[str]
    ) -> Optional[str]:
        """
        Find a join column by exact match or suffix match.

        Handles flattened MultiIndex columns like 'Standard_Player' -> matches 'Player'.

        Args:
            df: DataFrame to search in
            candidates: List of candidate column names

        Returns:
            Column name found in df, or None
        """
        # First try exact match
        for col in candidates:
            if col in df.columns:
                return col

        # Then try suffix match (for flattened MultiIndex that wasn't normalized)
        for col in candidates:
            for df_col in df.columns:
                if df_col.endswith(f'_{col}'):
                    return df_col

        return None

    def _merge_player_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """
        Merge multiple stat DataFrames into one extended table.

        Join on player + team combination, handling column conflicts.

        Args:
            data: Dictionary mapping stat_type to DataFrame
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with extended stats or None
        """
        base = data.get('stats')
        if base is None or base.empty:
            logger.warning("No base 'stats' data to merge")
            return None

        logger.debug(f"Base 'stats' columns: {list(base.columns)[:15]}...")

        # Identify join columns (looking for player identifier)
        join_cols = []
        player_col = self._find_join_column(
            base, ['Player', 'player', 'player_id']
        )
        if player_col:
            join_cols.append(player_col)

        # Add team if available
        team_col = self._find_join_column(base, ['Squad', 'Team', 'team'])
        if team_col:
            join_cols.append(team_col)

        if not join_cols:
            logger.warning(
                f"No suitable join columns found for merging. "
                f"Available columns: {list(base.columns)[:10]}..."
            )
            return base

        logger.info(f"Merging player stats on columns: {join_cols}")

        # Track columns that are already in base
        base_cols = set(base.columns)

        for stat_type, df in data.items():
            if stat_type == 'stats' or df is None or df.empty:
                continue

            logger.debug(
                f"Processing {stat_type}, columns: {list(df.columns)[:10]}..."
            )

            # Find matching join columns in this DataFrame using flexible search
            df_join_cols = []
            for base_col in join_cols:
                # Try exact match first
                if base_col in df.columns:
                    df_join_cols.append((base_col, base_col))
                else:
                    # Try to find equivalent column by name suffix
                    col_name = base_col.split('_')[-1]  # Get base name
                    found_col = self._find_join_column(
                        df, [col_name, base_col]
                    )
                    if found_col:
                        df_join_cols.append((base_col, found_col))

            if not df_join_cols:
                logger.warning(
                    f"Cannot merge {stat_type}: no common join columns. "
                    f"Looking for: {join_cols}, "
                    f"Available: {list(df.columns)[:10]}..."
                )
                continue

            # Rename df columns to match base for join
            rename_map = {
                df_col: base_col
                for base_col, df_col in df_join_cols
                if base_col != df_col
            }
            if rename_map:
                df = df.rename(columns=rename_map)
                logger.debug(f"Renamed columns for merge: {rename_map}")

            actual_join_cols = [base_col for base_col, _ in df_join_cols]

            # Get columns to merge (excluding join columns and duplicates)
            merge_cols = actual_join_cols.copy()
            for col in df.columns:
                if col not in base_cols and col not in actual_join_cols:
                    merge_cols.append(col)

            if len(merge_cols) > len(actual_join_cols):
                try:
                    base = base.merge(
                        df[merge_cols],
                        on=actual_join_cols,
                        how='left',
                        suffixes=('', f'_{stat_type}')
                    )
                    base_cols = set(base.columns)
                    logger.debug(
                        f"Merged {stat_type}: "
                        f"{len(merge_cols) - len(actual_join_cols)} new columns"
                    )
                except Exception as e:
                    logger.error(f"Error merging {stat_type}: {e}")

        # Add league/season metadata
        base['league'] = league
        base['season'] = season

        logger.info(
            f"Merged player stats: {len(base)} rows, {len(base.columns)} columns"
        )
        return base

    def _merge_keeper_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """
        Merge keeper and keeper_adv DataFrames.

        Args:
            data: Dictionary with 'keeper' and 'keeper_adv' DataFrames
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with keeper stats or None
        """
        base = data.get('keeper')
        if base is None or base.empty:
            return None

        # Identify join columns using flexible search
        join_cols = []
        player_col = self._find_join_column(
            base, ['Player', 'player', 'player_id']
        )
        if player_col:
            join_cols.append(player_col)

        team_col = self._find_join_column(base, ['Squad', 'Team', 'team'])
        if team_col:
            join_cols.append(team_col)

        if not join_cols:
            return base

        adv = data.get('keeper_adv')
        if adv is not None and not adv.empty:
            # Find matching join columns using flexible search
            adv_join_cols = []
            for base_col in join_cols:
                if base_col in adv.columns:
                    adv_join_cols.append((base_col, base_col))
                else:
                    col_name = base_col.split('_')[-1]
                    found_col = self._find_join_column(adv, [col_name, base_col])
                    if found_col:
                        adv_join_cols.append((base_col, found_col))

            if adv_join_cols:
                # Rename adv columns to match base for join
                rename_map = {
                    adv_col: base_col
                    for base_col, adv_col in adv_join_cols
                    if base_col != adv_col
                }
                if rename_map:
                    adv = adv.rename(columns=rename_map)

                actual_join_cols = [base_col for base_col, _ in adv_join_cols]
                base_cols = set(base.columns)
                merge_cols = actual_join_cols.copy()
                for col in adv.columns:
                    if col not in base_cols and col not in actual_join_cols:
                        merge_cols.append(col)

                if len(merge_cols) > len(actual_join_cols):
                    try:
                        base = base.merge(
                            adv[merge_cols],
                            on=actual_join_cols,
                            how='left'
                        )
                    except Exception as e:
                        logger.error(f"Error merging keeper_adv: {e}")

        base['league'] = league
        base['season'] = season
        return base

    def _extract_match_ids(
        self,
        schedule_df: pd.DataFrame,
        max_matches: Optional[int] = None
    ) -> List[str]:
        """
        Extract match IDs from schedule DataFrame.

        Args:
            schedule_df: Schedule DataFrame
            max_matches: Maximum number of matches to return (None for all)

        Returns:
            List of match IDs
        """
        match_ids = []

        if schedule_df is None or schedule_df.empty:
            return match_ids

        if 'match_id' in schedule_df.columns:
            match_ids = schedule_df['match_id'].dropna().tolist()
        elif 'match_url' in schedule_df.columns:
            # Extract from match_url column (added by read_schedule)
            for url in schedule_df['match_url'].dropna():
                if '/matches/' in str(url):
                    mid = str(url).split('/matches/')[-1].split('/')[0]
                    match_ids.append(mid)
            logger.info(f"Extracted {len(match_ids)} match IDs from match_url column")
        elif 'Match Report' in schedule_df.columns:
            # Fallback: Extract from Match Report column (if it contains URLs)
            for url in schedule_df['Match Report'].dropna():
                if '/matches/' in str(url):
                    mid = str(url).split('/matches/')[-1].split('/')[0]
                    match_ids.append(mid)

        if max_matches is not None:
            match_ids = match_ids[:max_matches]

        return match_ids

    def scrape_all(
        self,
        include_extended_stats: bool = True,
        include_match_stats: bool = False,
        include_keeper_stats: bool = True,
        include_shot_events: bool = True,
        include_match_events: bool = True,
        include_lineups: bool = True,
        include_team_match_stats: bool = False,
        include_team_stats_extended: bool = True,
        max_matches_per_league: int = 50,
    ) -> Dict[str, str]:
        """
        Scrape all FBref data for configured leagues and seasons.

        Collects:
        - Match schedules
        - Team statistics (standard)
        - Player statistics (standard)
        - Extended player statistics (merged from all stat_types)
        - Extended team statistics (merged from all stat_types)
        - Keeper statistics (basic + advanced)
        - Per-match player statistics (optional)
        - Shot events with xG and coordinates (new)
        - Match events: goals, cards, substitutions (new)
        - Lineups: starting XI and substitutes (new)
        - Team match statistics (new, optional - slow)

        Args:
            include_extended_stats: Collect extended player stats (all stat_types merged)
            include_match_stats: Collect per-match player stats (significantly slower)
            include_keeper_stats: Collect goalkeeper statistics
            include_shot_events: Collect shot events with xG data
            include_match_events: Collect match events (goals, cards, subs)
            include_lineups: Collect team lineups
            include_team_match_stats: Collect team-level match statistics (slow)
            include_team_stats_extended: Collect extended team stats (all stat_types merged)
            max_matches_per_league: Maximum matches to scrape per league/season

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FBref scrape: leagues={self.leagues}, "
            f"seasons={self.seasons}, extended={include_extended_stats}, "
            f"match_stats={include_match_stats}, keeper={include_keeper_stats}, "
            f"shot_events={include_shot_events}, match_events={include_match_events}, "
            f"lineups={include_lineups}, team_match_stats={include_team_match_stats}, "
            f"team_stats_extended={include_team_stats_extended}"
        )

        results = {}
        all_schedules = []
        all_team_stats = []
        all_team_stats_extended = []
        all_player_stats = []
        all_player_stats_extended = []
        all_keeper_stats = []
        all_match_stats = []
        all_shot_events = []
        all_match_events = []
        all_lineups = []
        all_team_match_stats = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Scrape schedule
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is not None and not schedule_df.empty:
                        all_schedules.append(schedule_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape team stats
                    team_df = self.read_team_season_stats('stats', league, season)
                    if team_df is not None and not team_df.empty:
                        all_team_stats.append(team_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape extended team stats (all stat_types)
                    if include_team_stats_extended:
                        team_data = {}
                        for stat_type in TEAM_STAT_TYPES:
                            df = self.read_team_season_stats(
                                stat_type, league, season
                            )
                            if df is not None and not df.empty:
                                team_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if team_data:
                            merged = self._merge_team_stats(
                                team_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_team_stats_extended.append(merged)
                            # Clean up intermediate data
                            del team_data
                            gc.collect()

                    # Scrape player stats (basic)
                    player_df = self.read_player_season_stats(
                        'stats', league, season
                    )
                    if player_df is not None and not player_df.empty:
                        all_player_stats.append(player_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape extended player stats (all stat_types)
                    if include_extended_stats:
                        player_data = {}
                        for stat_type in PLAYER_STAT_TYPES:
                            df = self.read_player_season_stats(
                                stat_type, league, season
                            )
                            if df is not None and not df.empty:
                                player_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if player_data:
                            merged = self._merge_player_stats(
                                player_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_player_stats_extended.append(merged)
                            # Clean up intermediate data
                            del player_data
                            gc.collect()

                    # Scrape keeper stats
                    if include_keeper_stats:
                        keeper_data = {}
                        for stat_type in KEEPER_STAT_TYPES:
                            df = self.read_keeper_stats(stat_type, league, season)
                            if df is not None and not df.empty:
                                keeper_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if keeper_data:
                            merged = self._merge_keeper_stats(
                                keeper_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_keeper_stats.append(merged)
                            # Clean up intermediate data
                            del keeper_data
                            gc.collect()

                    # Get match IDs for match-level data collection
                    collect_match_data = any([
                        include_match_stats,
                        include_shot_events,
                        include_match_events,
                        include_lineups,
                        include_team_match_stats,
                    ])

                    if collect_match_data and schedule_df is not None:
                        match_ids = self._extract_match_ids(
                            schedule_df, max_matches_per_league
                        )
                        logger.info(
                            f"Collecting match-level data for {len(match_ids)} matches"
                        )

                        for match_id in match_ids:
                            try:
                                # Player match stats
                                if include_match_stats:
                                    match_df = self.read_player_match_stats(
                                        match_id, league, season
                                    )
                                    if match_df is not None and not match_df.empty:
                                        all_match_stats.append(match_df)

                                # Shot events
                                if include_shot_events:
                                    shots_df = self.read_shot_events(
                                        match_id, league, season
                                    )
                                    if shots_df is not None and not shots_df.empty:
                                        all_shot_events.append(shots_df)

                                # Match events (goals, cards, subs)
                                if include_match_events:
                                    events_df = self.read_match_events(
                                        match_id, league, season
                                    )
                                    if events_df is not None and not events_df.empty:
                                        all_match_events.append(events_df)

                                # Lineups
                                if include_lineups:
                                    lineup_df = self.read_lineup(
                                        match_id, league, season
                                    )
                                    if lineup_df is not None and not lineup_df.empty:
                                        all_lineups.append(lineup_df)

                                # Team match stats
                                if include_team_match_stats:
                                    team_match_df = self.read_team_match_stats(
                                        match_id, league, season
                                    )
                                    if team_match_df is not None and not team_match_df.empty:
                                        all_team_match_stats.append(team_match_df)

                                time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                            except Exception as e:
                                logger.error(f"Error scraping match {match_id}: {e}")
                                continue

                except Exception as e:
                    logger.error(f"Error scraping {league} {season}: {e}")
                    continue
                finally:
                    # Memory cleanup after each league/season
                    self._cleanup_after_league()
                    logger.info(
                        f"Completed {league} {season}, memory cleaned up"
                    )

        # Save to Iceberg tables
        if all_schedules:
            combined_df = pd.concat(all_schedules, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_schedule',
                partition_cols=['league', 'season'],
            )
            results['schedule'] = table_path

        if all_team_stats:
            combined_df = pd.concat(all_team_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_stats',
                partition_cols=['league', 'season'],
            )
            results['team_stats'] = table_path

        if all_team_stats_extended:
            combined_df = pd.concat(all_team_stats_extended, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_stats_extended',
                partition_cols=['league', 'season'],
            )
            results['team_stats_extended'] = table_path

        if all_player_stats:
            combined_df = pd.concat(all_player_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats',
                partition_cols=['league', 'season'],
            )
            results['player_stats'] = table_path

        if all_player_stats_extended:
            combined_df = pd.concat(all_player_stats_extended, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats_extended',
                partition_cols=['league', 'season'],
            )
            results['player_stats_extended'] = table_path

        if all_keeper_stats:
            combined_df = pd.concat(all_keeper_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_keeper_stats',
                partition_cols=['league', 'season'],
            )
            results['keeper_stats'] = table_path

        if all_match_stats:
            combined_df = pd.concat(all_match_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_match_stats',
                partition_cols=['league', 'season'],
            )
            results['player_match_stats'] = table_path

        if all_shot_events:
            combined_df = pd.concat(all_shot_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_shot_events',
                partition_cols=['league', 'season'],
            )
            results['shot_events'] = table_path

        if all_match_events:
            combined_df = pd.concat(all_match_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_match_events',
                partition_cols=['league', 'season'],
            )
            results['match_events'] = table_path

        if all_lineups:
            combined_df = pd.concat(all_lineups, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_lineups',
                partition_cols=['league', 'season'],
            )
            results['lineups'] = table_path

        if all_team_match_stats:
            combined_df = pd.concat(all_team_match_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_match_stats',
                partition_cols=['league', 'season'],
            )
            results['team_match_stats'] = table_path

        logger.info(f"FBref scrape complete: {list(results.keys())}")
        return results

    def clear_cache(self) -> None:
        """Clear page cache and force garbage collection."""
        cache_size = len(self._page_cache)
        self._page_cache.clear()
        gc.collect()
        logger.info(f"Page cache cleared ({cache_size} pages), garbage collected")

    def _manage_cache_size(self) -> None:
        """Manage cache size to prevent memory issues."""
        if len(self._page_cache) > self.MAX_CACHE_SIZE:
            # Remove oldest entries (first half of cache)
            keys_to_remove = list(self._page_cache.keys())[:len(self._page_cache) // 2]
            for key in keys_to_remove:
                del self._page_cache[key]
            logger.info(f"Cache trimmed: removed {len(keys_to_remove)} old entries")

    def _maybe_restart_browser(self) -> None:
        """Restart browser if page limit reached to prevent memory leaks."""
        self._pages_fetched += 1
        if self._pages_fetched >= self.MAX_PAGES_BEFORE_BROWSER_RESTART:
            logger.info(
                f"Restarting browser after {self._pages_fetched} pages to prevent memory leaks"
            )
            self._close_browser()
            self._pages_fetched = 0
            gc.collect()

    def _close_browser(self) -> None:
        """Close browser and clean up resources."""
        # Close Selenium browser
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception as e:
                logger.warning(f"Error closing Selenium browser: {e}")
            self._browser = None

        # Close nodriver browser
        if self._nodriver_browser is not None:
            try:
                self._nodriver_browser.close_sync()
            except Exception as e:
                logger.warning(f"Error closing nodriver browser: {e}")
            self._nodriver_browser = None

    def _cleanup_after_league(self) -> None:
        """Clean up memory after processing a league."""
        self.clear_cache()
        logger.info("Memory cleanup after league processing")

    # =========================================================================
    # Memory-efficient methods for per-stat_type collection
    # =========================================================================

    def scrape_single_stat_type(
        self,
        stat_type: str,
        data_category: str,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape single stat_type for all leagues/seasons.

        Instead of merging all stat_types into one huge DataFrame (which causes OOM),
        this method collects only one stat_type and saves it to a separate Iceberg table.

        Args:
            stat_type: One of PLAYER_STAT_TYPES, TEAM_STAT_TYPES, or KEEPER_STAT_TYPES
                       (e.g., 'stats', 'shooting', 'passing', 'passing_types', 'gca',
                        'defense', 'possession', 'playingtime', 'misc',
                        'keeper', 'keeper_adv')
            data_category: One of 'player', 'team', or 'keeper'

        Returns:
            Dictionary mapping '{data_category}_{stat_type}' to Iceberg table path
        """
        logger.info(
            f"Starting single stat_type scrape: category={data_category}, "
            f"stat_type={stat_type}, leagues={self.leagues}, seasons={self.seasons}"
        )

        all_data = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    df = None

                    if data_category == 'player':
                        df = self.read_player_season_stats(stat_type, league, season)
                    elif data_category == 'team':
                        df = self.read_team_season_stats(stat_type, league, season)
                    elif data_category == 'keeper':
                        df = self.read_keeper_stats(stat_type, league, season)
                    else:
                        logger.error(f"Unknown data_category: {data_category}")
                        continue

                    if df is not None and not df.empty:
                        all_data.append(df)
                        logger.info(
                            f"Collected {len(df)} rows for {data_category}_{stat_type} "
                            f"({league}, {season})"
                        )

                    # Rate limiting between requests
                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                except Exception as e:
                    logger.error(
                        f"Error collecting {data_category}_{stat_type} "
                        f"for {league} {season}: {e}"
                    )
                    continue

            # Memory cleanup after each league
            self._cleanup_after_league()

        results = {}

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_name = f'fbref_{data_category}_{stat_type}'

            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name=table_name,
                partition_cols=['league', 'season'],
            )

            key = f'{data_category}_{stat_type}'
            results[key] = table_path

            logger.info(
                f"Saved {len(combined_df)} rows to {table_name} "
                f"(memory-efficient single stat_type mode)"
            )
        else:
            logger.warning(
                f"No data collected for {data_category}_{stat_type}"
            )

        return results

    def scrape_combined_match_data(
        self,
        max_matches: Optional[int] = 50,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape ALL match-level data in one pass.

        Collects shot_events, match_events, and lineups simultaneously
        by visiting each match page only once. This reduces HTTP requests
        by 3x compared to separate scrape_match_data() calls.

        Optimization:
        - Before: 3 separate passes × N matches = 3N page loads
        - After: 1 pass × N matches = N page loads (3x reduction)

        Args:
            max_matches: Maximum number of matches per league/season (default 50)

        Returns:
            Dictionary mapping data_type to Iceberg table path
            Keys: 'shot_events', 'match_events', 'lineups'
        """
        logger.info(
            f"Starting combined match data scrape: "
            f"max_matches={max_matches}, leagues={self.leagues}, seasons={self.seasons}"
        )

        all_shot_events = []
        all_match_events = []
        all_lineups = []

        total_matches_processed = 0
        total_pages_fetched = 0

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Get schedule to extract match IDs
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is None or schedule_df.empty:
                        logger.warning(
                            f"No schedule found for {league} {season}, "
                            f"skipping match data collection"
                        )
                        continue

                    logger.info(f"Extracting match IDs from schedule ({len(schedule_df)} rows)...")
                    match_ids = self._extract_match_ids(schedule_df, max_matches)
                    logger.info(
                        f"Collecting combined match data for {len(match_ids)} matches "
                        f"({league}, {season})"
                    )

                    # Restart browser after schedule fetch to avoid navigation issues
                    # (browser can become unresponsive after Cloudflare bypass)
                    if self.use_nodriver and self._nodriver_browser is not None:
                        logger.info("Restarting nodriver browser before match page scraping...")
                        self._nodriver_browser.restart_browser()

                    for idx, match_id in enumerate(match_ids):
                        logger.info(f"Processing match {idx+1}/{len(match_ids)}: {match_id}")
                        try:
                            # Fetch match page ONCE (will be cached for all three reads)
                            # read_shot_events, read_match_events, read_lineup all use
                            # the same URL with use_cache=True

                            # Shot events
                            shots_df = self.read_shot_events(match_id, league, season)
                            if shots_df is not None and not shots_df.empty:
                                all_shot_events.append(shots_df)

                            # Match events (goals, cards, subs)
                            events_df = self.read_match_events(match_id, league, season)
                            if events_df is not None and not events_df.empty:
                                all_match_events.append(events_df)

                            # Lineups
                            lineup_df = self.read_lineup(match_id, league, season)
                            if lineup_df is not None and not lineup_df.empty:
                                all_lineups.append(lineup_df)

                            total_matches_processed += 1
                            total_pages_fetched += 1  # Only 1 page per match (cached)

                            # Restart browser after each match to avoid navigation issues
                            if self.use_nodriver and self._nodriver_browser is not None:
                                self._nodriver_browser.restart_browser()

                            # Rate limiting between matches
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        except Exception as e:
                            logger.error(
                                f"Error collecting combined data for match {match_id}: {e}"
                            )
                            continue

                except Exception as e:
                    logger.error(
                        f"Error processing {league} {season} for combined match data: {e}"
                    )
                    continue
                finally:
                    # Memory cleanup after each league/season
                    self._cleanup_after_league()

        results = {}

        # Save shot events
        if all_shot_events:
            combined_df = pd.concat(all_shot_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_shot_events',
                partition_cols=['league', 'season'],
            )
            results['shot_events'] = table_path
            logger.info(f"Saved {len(combined_df)} shot events rows")

        # Save match events
        if all_match_events:
            combined_df = pd.concat(all_match_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_match_events',
                partition_cols=['league', 'season'],
            )
            results['match_events'] = table_path
            logger.info(f"Saved {len(combined_df)} match events rows")

        # Save lineups
        if all_lineups:
            combined_df = pd.concat(all_lineups, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_lineups',
                partition_cols=['league', 'season'],
            )
            results['lineups'] = table_path
            logger.info(f"Saved {len(combined_df)} lineups rows")

        logger.info(
            f"Combined match data scrape complete: "
            f"{total_matches_processed} matches processed, "
            f"{total_pages_fetched} pages fetched (3x reduction vs separate calls), "
            f"tables saved: {list(results.keys())}"
        )

        return results

    def scrape_match_data(
        self,
        data_type: str,
        max_matches: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape match-level data for all leagues/seasons.

        Collects one type of match-level data at a time:
        - schedule: Match schedules and results
        - shot_events: Shot events with xG data
        - match_events: Goals, cards, substitutions
        - lineups: Starting XI and substitutes

        Args:
            data_type: One of 'schedule', 'shot_events', 'match_events', 'lineups'
            max_matches: Maximum number of matches per league/season (None = all)

        Returns:
            Dictionary mapping data_type to Iceberg table path
        """
        logger.info(
            f"Starting match data scrape: type={data_type}, "
            f"max_matches={max_matches}, leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        if data_type == 'schedule':
            # Schedule doesn't need match IDs, collect directly
            all_schedules = []

            for league in self.leagues:
                for season in self.seasons:
                    try:
                        df = self.read_schedule(league, season)
                        if df is not None and not df.empty:
                            all_schedules.append(df)
                            logger.info(
                                f"Collected {len(df)} schedule rows "
                                f"({league}, {season})"
                            )
                        time.sleep(1)  # Reduced from 3s - rate limiter handles main delays
                    except Exception as e:
                        logger.error(
                            f"Error collecting schedule for {league} {season}: {e}"
                        )
                        continue

            if all_schedules:
                combined_df = pd.concat(all_schedules, ignore_index=True)
                table_path = self.save_to_iceberg(
                    df=combined_df,
                    table_name='fbref_schedule',
                    partition_cols=['league', 'season'],
                )
                results['schedule'] = table_path
                logger.info(f"Saved {len(combined_df)} schedule rows")

            return results

        # For other data types, we need match IDs from schedule first
        all_data = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Get schedule to extract match IDs
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is None or schedule_df.empty:
                        logger.warning(
                            f"No schedule found for {league} {season}, "
                            f"skipping match data collection"
                        )
                        continue

                    match_ids = self._extract_match_ids(schedule_df, max_matches)
                    logger.info(
                        f"Collecting {data_type} for {len(match_ids)} matches "
                        f"({league}, {season})"
                    )

                    for match_id in match_ids:
                        try:
                            df = None

                            if data_type == 'shot_events':
                                df = self.read_shot_events(match_id, league, season)
                            elif data_type == 'match_events':
                                df = self.read_match_events(match_id, league, season)
                            elif data_type == 'lineups':
                                df = self.read_lineup(match_id, league, season)
                            else:
                                logger.error(f"Unknown data_type: {data_type}")
                                break

                            if df is not None and not df.empty:
                                all_data.append(df)

                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        except Exception as e:
                            logger.error(
                                f"Error collecting {data_type} for match {match_id}: {e}"
                            )
                            continue

                except Exception as e:
                    logger.error(
                        f"Error processing {league} {season} for {data_type}: {e}"
                    )
                    continue
                finally:
                    # Memory cleanup after each league
                    self._cleanup_after_league()

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_name = f'fbref_{data_type}'

            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name=table_name,
                partition_cols=['league', 'season'],
            )

            results[data_type] = table_path
            logger.info(
                f"Saved {len(combined_df)} {data_type} rows "
                f"(memory-efficient match data mode)"
            )
        else:
            logger.warning(f"No data collected for {data_type}")

        return results


# Backwards compatibility alias
FBrefSeleniumScraper = FBrefScraper
