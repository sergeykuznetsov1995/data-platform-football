"""
FBref Selenium Scraper
======================

Scraper for FBref football statistics using Selenium with Cloudflare bypass.
FBref provides comprehensive football statistics including:
- Match schedules and results
- Player statistics (standard, shooting, passing, etc.)
- Team statistics

Source: https://fbref.com

NOTE: FBref uses Cloudflare protection. This scraper uses undetected-chromedriver
for bypassing bot detection.
"""

import logging
import re
import time
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.base.cloudflare_bypass import CloudflareBypass

logger = logging.getLogger(__name__)


class FBrefSeleniumScraper(SeleniumScraper):
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
        scraper = FBrefSeleniumScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = 20  # requests per minute (FBref is less strict)

    BASE_URL = 'https://fbref.com'

    # League configuration with competition IDs and URL slugs
    LEAGUE_IDS = {
        'ENG-Premier League': {'comp_id': '9', 'slug': 'Premier-League'},
        'ESP-La Liga': {'comp_id': '12', 'slug': 'La-Liga'},
        'GER-Bundesliga': {'comp_id': '20', 'slug': 'Bundesliga'},
        'ITA-Serie A': {'comp_id': '11', 'slug': 'Serie-A'},
        'FRA-Ligue 1': {'comp_id': '13', 'slug': 'Ligue-1'},
        'UEFA-Champions League': {'comp_id': '8', 'slug': 'Champions-League'},
        'UEFA-Europa League': {'comp_id': '19', 'slug': 'Europa-League'},
        'INT-World Cup': {'comp_id': '1', 'slug': 'World-Cup'},
    }

    # Available stat types for players
    PLAYER_STAT_TYPES = [
        'stats',           # Standard stats
        'shooting',        # Shooting stats
        'passing',         # Passing stats
        'passing_types',   # Pass types
        'gca',             # Goal and shot creation
        'defense',         # Defensive actions
        'possession',      # Possession stats
        'playingtime',     # Playing time
        'misc',            # Miscellaneous
    ]

    # Available stat types for teams (squads)
    TEAM_STAT_TYPES = [
        'stats',           # Standard stats
        'shooting',        # Shooting stats
        'passing',         # Passing stats
        'passing_types',   # Pass types
        'gca',             # Goal and shot creation
        'defense',         # Defensive actions
        'possession',      # Possession stats
        'playingtime',     # Playing time
        'misc',            # Miscellaneous
    ]

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = True,
        use_xvfb: bool = True,
        **kwargs
    ):
        """
        Initialize FBref scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display to bypass Cloudflare headless detection
            **kwargs: Additional arguments for SeleniumScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            **kwargs
        )
        self.use_xvfb = use_xvfb
        self._page_cache: Dict[str, str] = {}

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with FBref-specific configuration."""
        if self._browser is None:
            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=self.proxy,
                page_load_timeout=45,
            )
        return self._browser

    def _format_season(self, season: int) -> str:
        """
        Format season year to FBref format.

        FBref uses format like '2023-2024' for full seasons.

        Args:
            season: Season start year (e.g., 2023 for 2023-2024 season)

        Returns:
            Formatted season string
        """
        return f"{season}-{season + 1}"

    def _get_schedule_url(self, league: str, season: int) -> str:
        """
        Build URL for schedule/fixtures page.

        Args:
            league: League name
            season: Season year

        Returns:
            Full URL to schedule page
        """
        league_info = self.LEAGUE_IDS.get(league)
        if not league_info:
            raise ValueError(f"Unknown league: {league}")

        comp_id = league_info['comp_id']
        slug = league_info['slug']
        season_str = self._format_season(season)

        return f"{self.BASE_URL}/en/comps/{comp_id}/{season_str}/schedule/{season_str}-{slug}-Scores-and-Fixtures"

    def _get_stats_url(self, league: str, season: int, stat_type: str, for_squads: bool = False) -> str:
        """
        Build URL for statistics page.

        Args:
            league: League name
            season: Season year
            stat_type: Type of statistics
            for_squads: True for team stats, False for player stats

        Returns:
            Full URL to stats page
        """
        league_info = self.LEAGUE_IDS.get(league)
        if not league_info:
            raise ValueError(f"Unknown league: {league}")

        comp_id = league_info['comp_id']
        slug = league_info['slug']
        season_str = self._format_season(season)

        if for_squads:
            return f"{self.BASE_URL}/en/comps/{comp_id}/{season_str}/{stat_type}/{season_str}-{slug}-Stats"
        else:
            return f"{self.BASE_URL}/en/comps/{comp_id}/{season_str}/{stat_type}/{season_str}-{slug}-Stats"

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
            browser = self._get_browser()

            # Rate limiting
            self._rate_limiter.acquire()

            html = browser.get_page(
                url,
                wait_timeout=20,
                cloudflare_wait=8.0,
            )

            # Additional wait for dynamic content
            time.sleep(2)

            # Get updated page source
            html = browser.page_source

            if use_cache:
                self._page_cache[url] = html

            self._stats['successes'] += 1
            return html

        except Exception as e:
            self._stats['failures'] += 1
            logger.error(f"Error fetching page {url}: {e}")
            return None

    def _extract_tables_from_comments(self, soup: BeautifulSoup) -> Dict[str, BeautifulSoup]:
        """
        Extract tables hidden in HTML comments.

        FBref hides some tables in HTML comments for lazy loading.
        This method finds and parses those hidden tables.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            Dictionary mapping table ID to table soup
        """
        tables = {}

        # Find all comments
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))

        for comment in comments:
            comment_text = str(comment)

            # Check if comment contains a table
            if '<table' in comment_text:
                # Parse the comment as HTML
                comment_soup = BeautifulSoup(comment_text, 'html.parser')
                table = comment_soup.find('table')

                if table:
                    table_id = table.get('id', '')
                    if table_id:
                        tables[table_id] = table
                        logger.debug(f"Extracted table from comment: {table_id}")

        return tables

    def _parse_table(
        self,
        soup: BeautifulSoup,
        table_id: str,
        comment_tables: Optional[Dict[str, BeautifulSoup]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Parse HTML table to DataFrame.

        Args:
            soup: BeautifulSoup object
            table_id: ID of the table to parse
            comment_tables: Tables extracted from comments

        Returns:
            DataFrame or None
        """
        # First try to find table in regular HTML
        table = soup.find('table', id=table_id)

        # If not found, check comment tables
        if table is None and comment_tables:
            table = comment_tables.get(table_id)

        if table is None:
            logger.debug(f"Table not found: {table_id}")
            return None

        try:
            # Parse table with pandas
            html_str = str(table)
            dfs = pd.read_html(StringIO(html_str), flavor='bs4')

            if dfs:
                df = dfs[0]

                # Handle multi-level columns
                if isinstance(df.columns, pd.MultiIndex):
                    # Flatten multi-level columns
                    df.columns = ['_'.join(col).strip('_') for col in df.columns.values]

                # Remove summary rows (usually contain 'Squad Total' or similar)
                if 'Squad' in df.columns:
                    df = df[~df['Squad'].str.contains('Total|Average', na=False, case=False)]

                return df

        except Exception as e:
            logger.error(f"Error parsing table {table_id}: {e}")

        return None

    def read_schedule(self, league: str = None, season: int = None) -> Optional[pd.DataFrame]:
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

        url = self._get_schedule_url(league, season)
        logger.info(f"Fetching FBref schedule: {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = self._extract_tables_from_comments(soup)

        # Try multiple possible table IDs (note: FBref uses dashes in season, e.g. sched_2024-2025_9_1)
        season_str = self._format_season(season)  # e.g., "2024-2025"
        league_info = self.LEAGUE_IDS.get(league, {})
        comp_id = league_info.get('comp_id', '9')

        table_ids = [
            'sched_all',
            'sched_ks_all',
            f'sched_{season_str}_{comp_id}_1',  # e.g., sched_2024-2025_9_1
        ]

        df = None
        for table_id in table_ids:
            df = self._parse_table(soup, table_id, comment_tables)
            if df is not None and not df.empty:
                break

        if df is None or df.empty:
            # Try finding any table with 'sched' in ID from regular HTML
            all_tables = soup.find_all('table', id=lambda x: x and 'sched' in x.lower())
            for table in all_tables:
                table_id = table.get('id')
                df = self._parse_table(soup, table_id, comment_tables)
                if df is not None and not df.empty:
                    logger.debug(f"Found schedule table: {table_id}")
                    break

        if df is None or df.empty:
            # Try parsing any table with 'sched' in ID from comments
            for key in comment_tables.keys():
                if 'sched' in key.lower():
                    df = self._parse_table(soup, key, comment_tables)
                    if df is not None and not df.empty:
                        break

        if df is None or df.empty:
            logger.warning(f"No schedule data found for {league} {season}")
            return None

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

        url = self._get_stats_url(league, season, stat_type, for_squads=True)
        logger.info(f"Fetching FBref team stats ({stat_type}): {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = self._extract_tables_from_comments(soup)

        # Possible table IDs for squad stats
        table_ids = [
            f'stats_squads_{stat_type}_for',
            f'stats_squads_standard_for',
            f'stats_squads_{stat_type}',
        ]

        df = None
        for table_id in table_ids:
            df = self._parse_table(soup, table_id, comment_tables)
            if df is not None and not df.empty:
                break

        if df is None or df.empty:
            # Try finding table with 'squads' in ID
            for key in comment_tables.keys():
                if 'squads' in key.lower() and stat_type in key.lower():
                    df = self._parse_table(soup, key, comment_tables)
                    if df is not None and not df.empty:
                        break

        if df is None or df.empty:
            logger.warning(f"No team stats found for {league} {season} ({stat_type})")
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

        url = self._get_stats_url(league, season, stat_type, for_squads=False)
        logger.info(f"Fetching FBref player stats ({stat_type}): {url}")

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = self._extract_tables_from_comments(soup)

        # Possible table IDs for player stats
        table_ids = [
            f'stats_{stat_type}',
            f'stats_standard',
            f'stats_{stat_type}_all',
        ]

        df = None
        for table_id in table_ids:
            df = self._parse_table(soup, table_id, comment_tables)
            if df is not None and not df.empty:
                break

        if df is None or df.empty:
            # Try finding any stats table
            all_tables = soup.find_all('table')
            for table in all_tables:
                table_id = table.get('id', '')
                if 'stats' in table_id.lower() and 'squad' not in table_id.lower():
                    try:
                        html_str = str(table)
                        dfs = pd.read_html(StringIO(html_str), flavor='bs4')
                        if dfs and not dfs[0].empty:
                            df = dfs[0]
                            if isinstance(df.columns, pd.MultiIndex):
                                df.columns = ['_'.join(col).strip('_') for col in df.columns.values]
                            break
                    except Exception:
                        continue

        if df is None or df.empty:
            logger.warning(f"No player stats found for {league} {season} ({stat_type})")
            return None

        # Clean player names (remove rank numbers)
        if 'Player' in df.columns:
            df['Player'] = df['Player'].astype(str).str.replace(r'^\d+\s*', '', regex=True)

        # Add metadata
        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'player_stats_{stat_type}')

        logger.info(f"Parsed {len(df)} player stat entries")
        return df

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all FBref data for configured leagues and seasons.

        Collects:
        - Match schedules
        - Team statistics (standard)
        - Player statistics (standard)

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FBref scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        all_schedules = []
        all_team_stats = []
        all_player_stats = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Scrape schedule
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is not None and not schedule_df.empty:
                        all_schedules.append(schedule_df)

                    # Rate limit pause between requests
                    time.sleep(3)

                    # Scrape team stats
                    team_df = self.read_team_season_stats('stats', league, season)
                    if team_df is not None and not team_df.empty:
                        all_team_stats.append(team_df)

                    time.sleep(3)

                    # Scrape player stats
                    player_df = self.read_player_season_stats('stats', league, season)
                    if player_df is not None and not player_df.empty:
                        all_player_stats.append(player_df)

                    time.sleep(3)

                except Exception as e:
                    logger.error(f"Error scraping {league} {season}: {e}")
                    continue

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

        if all_player_stats:
            combined_df = pd.concat(all_player_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats',
                partition_cols=['league', 'season'],
            )
            results['player_stats'] = table_path

        logger.info(f"FBref scrape complete: {list(results.keys())}")
        return results

    def clear_cache(self) -> None:
        """Clear page cache."""
        self._page_cache.clear()
        logger.debug("Page cache cleared")
