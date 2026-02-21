"""
FBref Data Reader Mixin
========================

All ``read_*`` methods that parse FBref HTML pages into DataFrames,
plus the memory-efficient ``scrape_single_stat_type``,
``scrape_match_data``, and ``scrape_combined_match_data`` helpers.
"""

import gc
import logging
import time
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.fbref.constants import (
    BASE_URL,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
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


class FBrefDataReaderMixin:
    """
    Mixin providing all ``read_*`` and batch-scrape methods for FBrefScraper.

    Expects the host class to provide (via other mixins / SeleniumScraper):
        - self.leagues, self.seasons
        - self._fetch_page(url, use_cache=...)
        - self._add_metadata(df, entity_type)
        - self._cleanup_after_league()
        - self._extract_match_ids(schedule_df, max_matches)
        - self._merge_team_stats(data, league, season)
        - self._merge_player_stats(data, league, season)
        - self._merge_keeper_stats(data, league, season)
        - self.save_to_iceberg(df, table_name, partition_cols)
        - self.use_nodriver, self._nodriver_browser
    """

    # ------------------------------------------------------------------
    # Season-level readers
    # ------------------------------------------------------------------

    def read_schedule(
        self,
        league: str = None,
        season: int = None,
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
        logger.debug(f"Fetching FBref schedule: {url}")

        html = self._fetch_page(url)
        if not html:
            logger.error(f"Failed to fetch HTML for schedule: {league} {season}")
            return None

        logger.debug(f"Parsing HTML ({len(html)} bytes) with BeautifulSoup...")
        soup = BeautifulSoup(html, 'html.parser')
        logger.debug("BeautifulSoup parsing complete")

        # Diagnostic logging
        logger.debug("Running HTML structure diagnosis...")
        diagnosis = diagnose_html_structure(soup)
        logger.debug("HTML diagnosis complete")
        logger.debug(
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
        df['match_url'] = df.index.map(match_urls) if match_urls else None
        if match_urls:
            logger.debug(f"Extracted {len(match_urls)} match URLs from schedule")
        else:
            logger.warning(f"No match URLs extracted from schedule HTML for {league} {season}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref team stats ({stat_type}): {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref player stats ({stat_type}): {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref keeper stats ({stat_type}): {url}")

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

    # ------------------------------------------------------------------
    # Match-level readers
    # ------------------------------------------------------------------

    def read_player_match_stats(
        self,
        match_id: str,
        league: str = None,
        season: int = None,
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
        logger.debug(f"Fetching FBref match stats: {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref shot events: {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref match events: {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref lineups: {url}")

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
        season: int = None,
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
        logger.debug(f"Fetching FBref team match stats: {url}")

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

    # ------------------------------------------------------------------
    # Memory-efficient batch methods
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Combined match data: helpers
    # ------------------------------------------------------------------

    # Batch save interval — save accumulated data every N matches
    # to prevent data loss on crash and limit memory usage
    BATCH_SAVE_INTERVAL = 50

    def _process_single_match(
        self,
        match_id: str,
        league: str,
        season: int,
        all_shot_events: List[pd.DataFrame],
        all_match_events: List[pd.DataFrame],
        all_lineups: List[pd.DataFrame],
    ) -> bool:
        """
        Process a single match page: extract shots, events, lineups.

        Parses HTML once with BeautifulSoup and calls parsers directly,
        avoiding 3x redundant BS4 parsing that read_* methods would do.

        Returns True if at least one data type was successfully extracted.
        """
        url = f"{BASE_URL}/en/matches/{match_id}"
        html = self._fetch_page(url, use_cache=True)
        if not html:
            return False

        # ONE BS4 parse + ONE comment table extraction (instead of 3x)
        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)
        got_data = False

        # Shot events (needs comment_tables for shots table)
        shots_df = parse_shots_table(soup, comment_tables)
        if shots_df is not None and not shots_df.empty:
            shots_df['match_id'] = match_id
            shots_df['league'] = league
            shots_df['season'] = season
            shots_df = self._add_metadata(shots_df, 'shot_events')
            all_shot_events.append(shots_df)
            got_data = True

        # Match events (from scorebox — no comment_tables needed)
        events_df = parse_events_from_scorebox(soup)
        if events_df is not None and not events_df.empty:
            events_df['match_id'] = match_id
            events_df['league'] = league
            events_df['season'] = season
            events_df = self._add_metadata(events_df, 'match_events')
            all_match_events.append(events_df)
            got_data = True

        # Lineups (from div.lineup — no comment_tables needed)
        lineup_df = parse_lineup_table(soup)
        if lineup_df is not None and not lineup_df.empty:
            lineup_df['match_id'] = match_id
            lineup_df['league'] = league
            lineup_df['season'] = season
            lineup_df = self._add_metadata(lineup_df, 'lineups')
            all_lineups.append(lineup_df)
            got_data = True

        # Free memory: decompose soup tree and remove from cache
        soup.decompose()
        del comment_tables
        self._page_cache.pop(url, None)

        return got_data

    def _batch_save_match_data(
        self,
        all_shot_events: List[pd.DataFrame],
        all_match_events: List[pd.DataFrame],
        all_lineups: List[pd.DataFrame],
        results: Dict[str, str],
        batch_label: str = "",
    ) -> None:
        """
        Save accumulated match data to Iceberg and clear the lists.

        This is called periodically (every BATCH_SAVE_INTERVAL matches)
        and at the end of processing to prevent data loss on crash.
        """
        saved_count = 0

        if all_shot_events:
            combined_df = pd.concat(all_shot_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_shot_events',
                partition_cols=['league', 'season'],
            )
            results['shot_events'] = table_path
            saved_count += len(combined_df)
            logger.info(f"Batch save{batch_label}: {len(combined_df)} shot events rows")
            all_shot_events.clear()

        if all_match_events:
            combined_df = pd.concat(all_match_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_match_events',
                partition_cols=['league', 'season'],
            )
            results['match_events'] = table_path
            saved_count += len(combined_df)
            logger.info(f"Batch save{batch_label}: {len(combined_df)} match events rows")
            all_match_events.clear()

        if all_lineups:
            combined_df = pd.concat(all_lineups, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_lineups',
                partition_cols=['league', 'season'],
            )
            results['lineups'] = table_path
            saved_count += len(combined_df)
            logger.info(f"Batch save{batch_label}: {len(combined_df)} lineups rows")
            all_lineups.clear()

        if saved_count > 0:
            gc.collect()

    def _get_existing_match_ids(self, league: str, season: int) -> set:
        """Query Iceberg for match_ids already collected.

        Uses fbref_lineups as the source of truth because it is reliably
        populated for every scraped match (unlike shot_events which can be
        empty when no shot data is available on the page).
        """
        try:
            if not hasattr(self, '_iceberg_writer') or self._iceberg_writer is None:
                from scrapers.base.iceberg_writer import IcebergWriter
                self._iceberg_writer = IcebergWriter()

            if not self._iceberg_writer.table_exists('bronze', 'fbref_lineups'):
                return set()

            df = self._iceberg_writer.read_table(
                database='bronze',
                table='fbref_lineups',
                columns=['match_id'],
                filter_expr=f"league = '{league}' AND season = {season}",
            )
            if df is not None and not df.empty:
                ids = set(df['match_id'].unique())
                logger.info(
                    f"Found {len(ids)} existing match IDs in Iceberg (lineups) "
                    f"for {league} {season}"
                )
                return ids
        except Exception as e:
            logger.warning(f"Could not query existing match IDs: {e}")
        return set()

    def _read_schedule_from_iceberg(self, league: str, season: int) -> Optional[pd.DataFrame]:
        """Read schedule from Iceberg (saved by schedule_task) instead of HTTP to FBref."""
        try:
            if not hasattr(self, '_iceberg_writer') or self._iceberg_writer is None:
                from scrapers.base.iceberg_writer import IcebergWriter
                self._iceberg_writer = IcebergWriter()

            if not self._iceberg_writer.table_exists('bronze', 'fbref_schedule'):
                logger.warning("Iceberg: fbref_schedule table does not exist")
                return None

            df = self._iceberg_writer.read_table(
                database='bronze',
                table='fbref_schedule',
                filter_expr=f"league = '{league}' AND season = {season}",
            )

            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                logger.warning(
                    f"Iceberg: fbref_schedule has no rows for {league} season={season}"
                )
                return None

            if not isinstance(df, pd.DataFrame):
                logger.warning(
                    f"Iceberg: read_table returned {type(df).__name__}, expected DataFrame"
                )
                return None

            if 'match_url' not in df.columns:
                logger.warning(
                    "Iceberg: fbref_schedule missing 'match_url' column — "
                    "re-run schedule_task with updated NodriverFBrefScraper"
                )
                return None

            # Проверяем что match_url не все NULL (старые данные до добавления колонки)
            non_null_urls = df['match_url'].dropna()
            if non_null_urls.empty:
                logger.warning(
                    f"Iceberg: fbref_schedule has {len(df)} rows but all match_url are NULL"
                )
                return None

            logger.info(
                f"Iceberg: read {len(df)} schedule rows for {league} {season} "
                f"({len(non_null_urls)} with match_url)"
            )
            return df

        except Exception as e:
            logger.warning(f"Iceberg: could not read schedule: {e}")
        return None

    # ------------------------------------------------------------------
    # Combined match data: main method
    # ------------------------------------------------------------------

    def scrape_combined_match_data(
        self,
        max_matches: Optional[int] = 50,
        incremental: bool = True,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape ALL match-level data in one pass.

        Collects shot_events, match_events, and lineups simultaneously
        by visiting each match page only once. This reduces HTTP requests
        by 3x compared to separate scrape_match_data() calls.

        Features:
        - Parse Once: single BS4 parse per match (3x reduction)
        - Incremental: skips matches already in Iceberg (via shot_events table)
        - Batch saving every BATCH_SAVE_INTERVAL matches (prevents data loss)
        - Failed match retry with browser restart (recovers ~50-70%)

        Args:
            max_matches: Maximum number of matches per league/season (default 50)
            incremental: Skip matches already in Iceberg (default True)

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
        results = {}

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Try Iceberg first (schedule already saved by schedule_task)
                    schedule_df = self._read_schedule_from_iceberg(league, season)

                    if schedule_df is not None and not schedule_df.empty:
                        logger.info(
                            f"Using schedule from Iceberg for {league} {season} "
                            f"({len(schedule_df)} rows)"
                        )
                    else:
                        # Fallback: HTTP request to FBref
                        logger.warning(
                            f"Schedule not in Iceberg for {league} {season}, "
                            f"falling back to FBref HTTP"
                        )
                        schedule_df = self.read_schedule(league, season)

                    if schedule_df is None or schedule_df.empty:
                        logger.warning(
                            f"No schedule found for {league} {season}, "
                            f"skipping match data collection"
                        )
                        continue

                    logger.info(f"Extracting match IDs from schedule ({len(schedule_df)} rows)...")
                    match_ids = self._extract_match_ids(schedule_df, max_matches)

                    # Incremental: skip matches already in Iceberg
                    if incremental:
                        existing_ids = self._get_existing_match_ids(league, season)
                        new_match_ids = [
                            mid for mid in match_ids if mid not in existing_ids
                        ]
                        logger.info(
                            f"Incremental: {len(match_ids)} total, "
                            f"{len(existing_ids)} already scraped, "
                            f"{len(new_match_ids)} new matches to process"
                        )
                        match_ids = new_match_ids

                    if not match_ids:
                        logger.info(
                            f"No new matches to process for {league} {season}"
                        )
                        continue

                    logger.info(
                        f"Collecting combined match data for {len(match_ids)} matches "
                        f"({league}, {season})"
                    )

                    # Restart browser after schedule fetch to avoid navigation issues
                    # (browser can become unresponsive after Cloudflare bypass)
                    if self.use_nodriver and self._nodriver_browser is not None:
                        logger.info("Restarting nodriver browser before match page scraping...")
                        self._nodriver_browser.restart_browser()

                    failed_match_ids = []

                    for idx, match_id in enumerate(match_ids):
                        logger.info(f"Processing match {idx+1}/{len(match_ids)}: {match_id}")
                        try:
                            got_data = self._process_single_match(
                                match_id, league, season,
                                all_shot_events, all_match_events, all_lineups,
                            )

                            if got_data:
                                total_matches_processed += 1
                            else:
                                failed_match_ids.append(match_id)
                                logger.warning(
                                    f"No data extracted for match {match_id}, "
                                    f"will retry later"
                                )

                            total_pages_fetched += 1

                            # Rate limiting between matches
                            time.sleep(1)

                            # Batch save every N matches to prevent data loss
                            if (idx + 1) % self.BATCH_SAVE_INTERVAL == 0:
                                self._batch_save_match_data(
                                    all_shot_events, all_match_events, all_lineups,
                                    results,
                                    batch_label=f" (after {idx+1}/{len(match_ids)} matches)",
                                )

                        except Exception as e:
                            logger.error(
                                f"Error collecting combined data for match {match_id}: {e}"
                            )
                            failed_match_ids.append(match_id)
                            continue

                    # Retry failed matches with browser restart
                    if failed_match_ids:
                        logger.info(
                            f"Retrying {len(failed_match_ids)} failed matches "
                            f"with browser restart ({league}, {season})"
                        )
                        if self.use_nodriver and self._nodriver_browser is not None:
                            self._nodriver_browser.restart_browser()

                        recovered = 0
                        for match_id in failed_match_ids:
                            try:
                                got_data = self._process_single_match(
                                    match_id, league, season,
                                    all_shot_events, all_match_events, all_lineups,
                                )
                                if got_data:
                                    recovered += 1
                                    total_matches_processed += 1
                                time.sleep(1)
                            except Exception as e:
                                logger.debug(f"Retry failed for match {match_id}: {e}")

                        logger.info(
                            f"Retry complete: recovered {recovered}/{len(failed_match_ids)} matches"
                        )

                    # Save remaining data after each league/season
                    self._batch_save_match_data(
                        all_shot_events, all_match_events, all_lineups,
                        results,
                        batch_label=f" (end of {league} {season})",
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing {league} {season} for combined match data: {e}"
                    )
                    # Save whatever we have so far
                    self._batch_save_match_data(
                        all_shot_events, all_match_events, all_lineups,
                        results,
                        batch_label=f" (error recovery for {league} {season})",
                    )
                    continue
                finally:
                    # Memory cleanup after each league/season
                    self._cleanup_after_league()

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
                    # Try Iceberg first (schedule already saved by schedule_task)
                    schedule_df = self._read_schedule_from_iceberg(league, season)

                    if schedule_df is not None and not schedule_df.empty:
                        logger.info(
                            f"Using schedule from Iceberg for {league} {season} "
                            f"({len(schedule_df)} rows)"
                        )
                    else:
                        # Fallback: HTTP request to FBref
                        logger.warning(
                            f"Schedule not in Iceberg for {league} {season}, "
                            f"falling back to FBref HTTP"
                        )
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
