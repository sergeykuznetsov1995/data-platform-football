"""
FBref Data Reader Mixin
========================

All ``read_*`` methods that parse FBref HTML pages into DataFrames,
plus the memory-efficient ``scrape_single_stat_type``,
``scrape_match_data``, and ``scrape_combined_match_data`` helpers.
"""

import gc
import json
import logging
import os
import time
from typing import Dict, List, Optional, Set

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
    parse_player_match_stats_tables,
    parse_keeper_match_stats_tables,
    parse_match_managers,
    parse_match_officials,
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

        html = self._fetch_page(url, page_type='schedule')
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

        # match_url is populated inside find_schedule_table → parse_table
        # (mapped before row filtering so URLs stay aligned with fixtures, #241).
        if 'match_url' not in df.columns or not df['match_url'].notna().any():
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

        html = self._fetch_page(url, page_type='team_stat')
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

        html = self._fetch_page(url, page_type='player_stat')
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
            stat_type: Type of keeper statistics (keeper)
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

        html = self._fetch_page(url, page_type='keeper_stat')
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

        html = self._fetch_page(url, use_cache=False, page_type='match')
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

        html = self._fetch_page(url, use_cache=True, page_type='match')  # Cache since match page used for multiple reads
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

        html = self._fetch_page(url, use_cache=True, page_type='match')
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

        html = self._fetch_page(url, use_cache=True, page_type='match')
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = parse_lineup_table(soup, comment_tables=comment_tables)

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

        html = self._fetch_page(url, use_cache=True, page_type='match')
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
        force_replace: bool = False,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape single stat_type for all leagues/seasons.

        Instead of merging all stat_types into one huge DataFrame (which causes OOM),
        this method collects only one stat_type and saves it to a separate Iceberg table.

        Args:
            stat_type: One of PLAYER_STAT_TYPES, TEAM_STAT_TYPES, or KEEPER_STAT_TYPES
                       (e.g., 'stats', 'shooting', 'passing', 'passing_types', 'gca',
                        'defense', 'possession', 'playingtime', 'misc',
                        'keeper')
            data_category: One of 'player', 'team', or 'keeper'
            force_replace: Bypass the completeness guard (#513/#583). When False
                (default) a partial scrape that would shrink the (league, season)
                partition below 90% of its existing rows is refused
                (ReplaceGuardError). Set True for a deliberate first backfill.

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

            # #536: full-state per (league, season) — without replace_partitions
            # the weekly single_stat DAG tasks plain-append a full copy every
            # run (45-50x bloat in fbref_player_{misc,shooting,playingtime},
            # team_* and keeper_*). Mirrors scrape_all (#468) and the combined
            # match path; the DELETE only targets the (league, season) keys in
            # this frame, leaving other partitions untouched.
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name=table_name,
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                # Completeness guard (#513/#583): refuse a partial scrape that
                # would shrink the (league, season) partition below 90% of its
                # existing rows (full-state season stats → raw COUNT(*)).
                min_replace_ratio=(None if force_replace else 0.9),
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
    # Combined season stats: one fetch per page, all tables parsed
    # ------------------------------------------------------------------

    # Season page plan: (url_stat_type, [(data_category, stat_type), ...]).
    # Player and squad stats share the same URL for 'stats'/'shooting'/'misc'
    # (url_builder.get_stats_url builds an identical URL for both) — the squad
    # tables sit in the main DOM and the player table hides in an HTML
    # comment, so ONE fetch feeds BOTH bronze tables. 'playingtime' is the
    # only split case (player /playingtime/ vs squad /playing_time/):
    # _parse_season_page falls back to the squad URL when the squad table is
    # missing from the player page. 5 fetches replace the 9 the separate
    # single_stat tasks used to make.
    _SEASON_PAGE_PLAN = [
        ('stats', (('player', 'stats'), ('team', 'stats'))),
        ('shooting', (('player', 'shooting'), ('team', 'shooting'))),
        ('playingtime', (('player', 'playingtime'), ('team', 'playingtime'))),
        ('misc', (('player', 'misc'), ('team', 'misc'))),
        ('keeper', (('keeper', 'keeper'),)),
    ]

    def _parse_season_page(
        self,
        league: str,
        season: int,
        url_stat_type: str,
        extracts,
    ) -> Dict[str, pd.DataFrame]:
        """Fetch ONE season stats page and parse all its tables.

        Returns {'{category}_{stat_type}': DataFrame} for every extract that
        produced data. Missing tables are logged and skipped, except the
        squad playingtime table which gets one extra fetch of the dedicated
        squad URL (/playing_time/) before giving up.
        """
        url = get_stats_url(league, season, url_stat_type, for_squads=False)
        logger.debug(f"Fetching FBref season page ({url_stat_type}): {url}")

        html = self._fetch_page(url, page_type='season_stat')
        if not html:
            logger.warning(
                f"Season page fetch failed for {league} {season} ({url_stat_type})"
            )
            return {}

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        out: Dict[str, pd.DataFrame] = {}
        for category, stat_type in extracts:
            if category == 'team':
                df = find_team_stats_table(soup, comment_tables, stat_type)
                if (df is None or df.empty) and url_stat_type == 'playingtime':
                    # Squad playing time lives on its own URL (/playing_time/)
                    # if it's absent from the player page — one extra fetch.
                    squad_url = get_stats_url(
                        league, season, stat_type, for_squads=True
                    )
                    if squad_url != url:
                        logger.warning(
                            f"Squad playingtime table missing on {url}, "
                            f"fetching dedicated squad URL"
                        )
                        squad_html = self._fetch_page(
                            squad_url, page_type='season_stat'
                        )
                        if squad_html:
                            squad_soup = BeautifulSoup(squad_html, 'html.parser')
                            squad_comments = extract_tables_from_comments(squad_soup)
                            df = find_team_stats_table(
                                squad_soup, squad_comments, stat_type
                            )
            else:
                # 'player' and 'keeper' both use the player-table finder
                df = find_player_stats_table(soup, comment_tables, stat_type)

            if df is None or df.empty:
                logger.warning(
                    f"No {category}_{stat_type} table for {league} {season} "
                    f"on season page '{url_stat_type}'"
                )
                continue

            # Clean player names (remove rank numbers) — mirrors read_*
            if category in ('player', 'keeper') and 'Player' in df.columns:
                df['Player'] = df['Player'].astype(str).str.replace(
                    r'^\d+\s*', '', regex=True
                )

            df['league'] = league
            df['season'] = season
            df['stat_type'] = stat_type
            df = self._add_metadata(df, f'{category}_stats_{stat_type}')
            out[f'{category}_{stat_type}'] = df

        return out

    def scrape_combined_season_stats(
        self,
        force_replace: bool = False,
    ) -> Dict[str, object]:
        """Scrape ALL season stats (player + team + keeper) in one pass.

        Replaces the nine separate single_stat runs: each unique season page
        is downloaded once (5 pages per league/season instead of 9) and every
        table on it feeds its own bronze table. Reuses the HTTP fast-path in
        _fetch_page, so only the first request pays the CF-bypass cost.

        Each of the 9 tables is saved independently with the same
        replace_partitions + completeness guard semantics as
        scrape_single_stat_type — a ReplaceGuardError on one table does not
        block the others.

        Args:
            force_replace: Bypass the completeness guard (#513/#583) for a
                deliberate first backfill.

        Returns:
            {'tables': {key: iceberg_path}, 'guard_refusals': [msg],
             'errors': [msg]}
        """
        from scrapers.base.base_scraper import ReplaceGuardError

        logger.info(
            f"Starting combined season stats scrape: "
            f"leagues={self.leagues}, seasons={self.seasons}"
        )

        buffers: Dict[str, List[pd.DataFrame]] = {}

        for league in self.leagues:
            for season in self.seasons:
                for url_stat_type, extracts in self._SEASON_PAGE_PLAN:
                    try:
                        parsed = self._parse_season_page(
                            league, season, url_stat_type, extracts
                        )
                    except Exception as e:
                        logger.error(
                            f"Error parsing season page '{url_stat_type}' "
                            f"for {league} {season}: {e}"
                        )
                        continue

                    for key, df in parsed.items():
                        buffers.setdefault(key, []).append(df)
                        logger.info(
                            f"Collected {len(df)} rows for {key} "
                            f"({league}, {season})"
                        )

                    # Rate limiting between pages (mirrors single_stat)
                    time.sleep(1)

            self._cleanup_after_league()

        results: Dict[str, str] = {}
        guard_refusals: List[str] = []
        errors: List[str] = []

        for key, frames in buffers.items():
            combined_df = pd.concat(frames, ignore_index=True)
            table_name = f'fbref_{key}'
            try:
                # Same semantics as scrape_single_stat_type (#536, #513/#583)
                table_path = self.save_to_iceberg(
                    df=combined_df,
                    table_name=table_name,
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=(None if force_replace else 0.9),
                )
                results[key] = table_path
                logger.info(f"Saved {len(combined_df)} rows to {table_name}")
            except ReplaceGuardError as e:
                msg = f"{table_name}: {e}"
                guard_refusals.append(msg)
                logger.error(f"Replace guard refused {table_name}: {e}")
            except Exception as e:
                msg = f"{table_name}: {e}"
                errors.append(msg)
                logger.error(f"Error saving {table_name}: {e}", exc_info=True)

        expected_keys = {
            f'{cat}_{stat}'
            for _, extracts in self._SEASON_PAGE_PLAN
            for cat, stat in extracts
        }
        missing = sorted(expected_keys - set(buffers))
        if missing:
            logger.warning(f"No data collected for: {missing}")

        return {
            'tables': results,
            'guard_refusals': guard_refusals,
            'errors': errors,
        }

    # ------------------------------------------------------------------
    # Combined match data: helpers
    # ------------------------------------------------------------------

    # Batch save interval — save accumulated data every N matches
    # to prevent data loss on crash and limit memory usage.
    # 50→20: reduce memory pressure (OOM killer hit 2G scheduler limit at ~200 matches)
    BATCH_SAVE_INTERVAL = 20

    def _process_single_match(
        self,
        match_id: str,
        league: str,
        season: int,
        all_shot_events: List[pd.DataFrame],
        all_match_events: List[pd.DataFrame],
        all_lineups: List[pd.DataFrame],
        all_match_team_stats: List[pd.DataFrame] = None,
        all_match_player_stats: List[pd.DataFrame] = None,
        all_match_managers: List[pd.DataFrame] = None,
        all_match_officials: List[pd.DataFrame] = None,
        all_match_keeper_stats: List[pd.DataFrame] = None,
    ) -> Set[str]:
        """
        Process a single match page: extract shots, events, lineups,
        team match stats, player match stats, and keeper match stats.

        Parses HTML once with BeautifulSoup and calls parsers directly,
        avoiding redundant BS4 parsing that read_* methods would do.

        Returns set of successfully extracted data type names
        (e.g. {'lineups', 'match_player_stats'}).  Empty set on total failure.
        """
        url = f"{BASE_URL}/en/matches/{match_id}"
        html = self._fetch_page(url, use_cache=True, page_type='match')
        if not html:
            return set()

        # ONE BS4 parse + ONE comment table extraction
        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)
        got_types: Set[str] = set()

        # Shot events (needs comment_tables for shots table)
        shots_df = parse_shots_table(soup, comment_tables)
        if shots_df is not None and not shots_df.empty:
            shots_df['match_id'] = match_id
            shots_df['league'] = league
            shots_df['season'] = season
            shots_df = self._add_metadata(shots_df, 'shot_events')
            all_shot_events.append(shots_df)
            got_types.add('shot_events')

        # Match events (from scorebox — no comment_tables needed)
        events_df = parse_events_from_scorebox(soup)
        if events_df is not None and not events_df.empty:
            events_df['match_id'] = match_id
            events_df['league'] = league
            events_df['season'] = season
            events_df = self._add_metadata(events_df, 'match_events')
            all_match_events.append(events_df)
            got_types.add('match_events')

        # Lineups (positions enriched from stats summary comment_tables)
        lineup_df = parse_lineup_table(soup, comment_tables=comment_tables)
        if lineup_df is not None and not lineup_df.empty:
            lineup_df['match_id'] = match_id
            lineup_df['league'] = league
            lineup_df['season'] = season
            lineup_df = self._add_metadata(lineup_df, 'lineups')
            all_lineups.append(lineup_df)
            got_types.add('lineups')

        # Team match stats (div#team_stats + div#team_stats_extra)
        if all_match_team_stats is not None:
            team_stats_df = parse_team_match_stats_table(soup, comment_tables)
            if team_stats_df is not None and not team_stats_df.empty:
                team_stats_df['match_id'] = match_id
                team_stats_df['league'] = league
                team_stats_df['season'] = season
                team_stats_df = self._add_metadata(team_stats_df, 'match_team_stats')
                all_match_team_stats.append(team_stats_df)
                got_types.add('match_team_stats')

        # Player match stats (stats_*_summary tables)
        if all_match_player_stats is not None:
            player_match_df = parse_player_match_stats_tables(soup, comment_tables)
            if player_match_df is not None and not player_match_df.empty:
                player_match_df['match_id'] = match_id
                player_match_df['league'] = league
                player_match_df['season'] = season
                player_match_df = self._add_metadata(player_match_df, 'match_player_stats')
                all_match_player_stats.append(player_match_df)
                got_types.add('match_player_stats')

        # Match managers (scorebox info-table — one row per side)
        if all_match_managers is not None:
            managers_df = parse_match_managers(soup)
            if managers_df is not None and not managers_df.empty:
                managers_df['match_id'] = match_id
                managers_df['league'] = league
                managers_df['season'] = season
                managers_df = self._add_metadata(managers_df, 'match_managers')
                all_match_managers.append(managers_df)
                got_types.add('match_managers')

        # Match officials (scorebox_meta — one wide row: referee/ar1/ar2/4th/var)
        if all_match_officials is not None:
            officials_df = parse_match_officials(soup)
            if officials_df is not None and not officials_df.empty:
                officials_df['match_id'] = match_id
                officials_df['league'] = league
                officials_df['season'] = season
                officials_df = self._add_metadata(officials_df, 'match_officials')
                all_match_officials.append(officials_df)
                got_types.add('match_officials')

        # Keeper match stats (keeper_stats_{team_id} tables — basic GK
        # columns still populated after the Apr-2026 FBref restriction)
        if all_match_keeper_stats is not None:
            keeper_df = parse_keeper_match_stats_tables(soup, comment_tables)
            if keeper_df is not None and not keeper_df.empty:
                keeper_df['match_id'] = match_id
                keeper_df['league'] = league
                keeper_df['season'] = season
                keeper_df = self._add_metadata(keeper_df, 'match_keeper_stats')
                all_match_keeper_stats.append(keeper_df)
                got_types.add('match_keeper_stats')

        # Free memory: decompose soup tree and remove from cache
        soup.decompose()
        del comment_tables
        self._page_cache.pop(url, None)

        return got_types

    def _save_fallback_json(
        self,
        df: pd.DataFrame,
        data_type: str,
        results: Dict[str, str],
    ) -> None:
        """Save DataFrame to JSON fallback when Iceberg/Trino is unavailable."""
        ts = int(time.time())
        path = f'/tmp/fbref_batch_{data_type}_{ts}.json'
        try:
            df.to_json(path, orient='records', date_format='iso')
            results[f'{data_type}_fallback'] = path
            logger.warning(
                f"Saved {len(df)} {data_type} rows to JSON fallback: {path}"
            )
        except Exception as fallback_err:
            logger.error(f"Failed to save JSON fallback for {data_type}: {fallback_err}")

    def _batch_save_match_data(
        self,
        all_shot_events: List[pd.DataFrame],
        all_match_events: List[pd.DataFrame],
        all_lineups: List[pd.DataFrame],
        results: Dict[str, str],
        batch_label: str = "",
        all_match_team_stats: List[pd.DataFrame] = None,
        all_match_player_stats: List[pd.DataFrame] = None,
        all_match_managers: List[pd.DataFrame] = None,
        all_match_officials: List[pd.DataFrame] = None,
        all_match_keeper_stats: List[pd.DataFrame] = None,
    ) -> None:
        """
        Save accumulated match data to Iceberg and clear the lists.

        This is called periodically (every BATCH_SAVE_INTERVAL matches)
        and at the end of processing to prevent data loss on crash.

        On Trino/connection errors, saves data to JSON fallback files
        so collected data is not lost.
        """
        saved_count = 0

        # 4th element = replace_partitions key. Matches that lack player_stats
        # (the skip-source) are re-scraped every run; their other parsers
        # (events/lineups/team_stats) succeed and plain append accumulated
        # duplicate rows (#231 — lineups/events ~19% bloat, same root as #216).
        # ['match_id'] deletes that match's old rows before inserting the fresh
        # full set — idempotent per match without wiping the rest of the
        # league/season partition (the delete filter only targets match_ids
        # present in the batch, so a table that fails to parse is left alone).
        # shot_events stays None: bronze.fbref_shot_events never exists
        # (FBref Feb-2026 restriction).
        save_items = [
            (all_shot_events, 'fbref_shot_events', 'shot_events', None),
            (all_match_events, 'fbref_match_events', 'match_events', ['match_id']),
            (all_lineups, 'fbref_lineups', 'lineups', ['match_id']),
            (all_match_team_stats, 'fbref_match_team_stats', 'match_team_stats', ['match_id']),
            (all_match_player_stats, 'fbref_match_player_stats', 'match_player_stats', ['match_id']),
            (all_match_managers, 'fbref_match_managers', 'match_managers', ['match_id']),
            (all_match_officials, 'fbref_match_officials', 'match_officials', ['match_id']),
            (all_match_keeper_stats, 'fbref_match_keeper_stats', 'match_keeper_stats', ['match_id']),
        ]

        for data_list, table_name, result_key, replace_keys in save_items:
            if not data_list:
                continue

            # Retry passes (#468) and duplicate schedule rows can append a
            # second frame for the same match before the buffer flushes
            # (the ['match_id'] DELETE only cleans prior table rows, not
            # in-frame duplicates). Each frame is a full parse uniformly
            # tagged with one match_id — keep only the newest per match.
            latest_by_match = {}
            for frame in data_list:
                if frame.empty:
                    continue
                latest_by_match[frame['match_id'].iloc[0]] = frame
            if not latest_by_match:
                data_list.clear()
                continue
            combined_df = pd.concat(latest_by_match.values(), ignore_index=True)
            try:
                table_path = self.save_to_iceberg(
                    df=combined_df,
                    table_name=table_name,
                    partition_cols=['league', 'season'],
                    replace_partitions=replace_keys,
                )
                results[result_key] = table_path
                saved_count += len(combined_df)
                logger.info(
                    f"Batch save{batch_label}: {len(combined_df)} {result_key} rows"
                )
            except Exception as e:
                error_str = str(e)
                is_conn_error = any(
                    msg in error_str
                    for msg in ('Connection refused', 'Connection reset',
                                'Failed to connect', 'TrinoError')
                )
                if is_conn_error:
                    logger.error(
                        f"Trino unavailable during batch save of {result_key}: {e}"
                    )
                else:
                    logger.error(
                        f"Error saving {result_key} to Iceberg: {e}",
                        exc_info=True,
                    )
                self._save_fallback_json(combined_df, result_key, results)
            finally:
                data_list.clear()

        if saved_count > 0:
            gc.collect()

    def _load_match_id_sets(self, league: str, season: int) -> Dict[str, set]:
        """Read match_id sets from authoritative bronze tables.

        Returns:
            {'player_stats': set[str], 'lineups': set[str]} — either set may
            be empty if the table is missing or unreadable.

        ``player_stats`` is the authoritative skip-source: it's written last
        in ``_batch_save_match_data`` and is also what Silver depends on. If
        a match has lineups but no player_stats, it was scraped by an old
        version of the pipeline (before match_player_stats was added) and
        must be re-scraped — see :meth:`_get_existing_match_ids`.
        """
        result = {'player_stats': set(), 'lineups': set()}

        try:
            if not hasattr(self, '_iceberg_writer') or self._iceberg_writer is None:
                from scrapers.base.iceberg_writer import IcebergWriter
                self._iceberg_writer = IcebergWriter()
        except Exception as e:
            logger.warning(f"IcebergWriter init failed: {e}")
            return result

        filter_expr = f"league = '{league}' AND season = {season}"

        for table, key in (
            ('fbref_match_player_stats', 'player_stats'),
            ('fbref_lineups', 'lineups'),
        ):
            try:
                if not self._iceberg_writer.table_exists('bronze', table):
                    logger.info(f"Table {table} does not exist (first run?)")
                    continue
                df = self._iceberg_writer.read_table(
                    database='bronze',
                    table=table,
                    columns=['match_id'],
                    filter_expr=filter_expr,
                )
                if df is not None and not df.empty:
                    result[key] = set(df['match_id'].astype(str).unique())
            except Exception as e:
                logger.warning(f"Could not read {table}: {e}")

        return result

    def _get_existing_match_ids(
        self,
        league: str,
        season: int,
        schedule_df: Optional[pd.DataFrame] = None,  # noqa: ARG002
    ) -> set:
        """Return set of match_ids safe to skip.

        Skip rule: a match is skipped iff its match_id is present in
        ``fbref_match_player_stats``. That table is the last write of
        ``_batch_save_match_data`` and is the authoritative signal that the
        full 5-way single-pass parse succeeded.

        Why not include ``fbref_lineups`` in a union (as previous Hybrid
        attempt did)? Pre-existing rows in ``lineups`` were written by older
        scraper versions before ``match_player_stats`` extraction was added
        (see e.g. EPL 2016-2021: 380 lineups vs 0 player_stats). A union
        skip would silently lock those matches out of ever getting
        per-player stats. Keeping the rule strictly tied to player_stats
        preserves correctness while still skipping all matches the new
        pipeline has fully ingested.

        ``schedule_df`` is accepted for API compatibility but not used —
        the rule is no longer date-dependent.
        """
        sets = self._load_match_id_sets(league, season)
        stats_ids = sets['player_stats']
        lineup_ids = sets['lineups']

        logger.info(
            f"Existing IDs (player_stats authoritative): "
            f"player_stats={len(stats_ids)}, lineups={len(lineup_ids)}, "
            f"skip={len(stats_ids)} for {league} {season}"
        )
        return stats_ids

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

            # fbref_schedule is INSERT-only: every schedule_task run appends
            # the full season fixture list (~380 rows). Without dedup the
            # table has 10x copies for active seasons (e.g. EPL 2025: 4335
            # rows / 310 unique). Keep the latest version of each match_url
            # (FBref edits the schedule for postponements/reschedules).
            raw_count = len(df)
            if '_ingested_at' in df.columns:
                df = df.sort_values('_ingested_at', kind='mergesort')
            df = df.drop_duplicates(subset=['match_url'], keep='last')
            non_null_urls = df['match_url'].dropna()

            logger.info(
                f"Iceberg: read {raw_count} schedule rows for {league} {season}, "
                f"deduped to {len(df)} ({len(non_null_urls)} with match_url)"
            )
            return df

        except Exception as e:
            # Distinguish Trino connectivity errors from data errors
            error_str = str(e)
            is_connection_error = any(
                msg in error_str
                for msg in ('Connection refused', 'Connection reset',
                            'Connection aborted', 'Failed to connect')
            )
            if is_connection_error:
                logger.error(
                    f"Iceberg: Trino unavailable — cannot read schedule: {e}",
                    exc_info=True,
                )
            elif 'TrinoError' in type(e).__name__ or 'trino' in type(e).__module__:
                logger.error(
                    f"Iceberg: Trino query error reading schedule: {e}",
                    exc_info=True,
                )
            else:
                logger.warning(
                    f"Iceberg: unexpected error reading schedule: {e}",
                    exc_info=True,
                )
        return None

    def _read_schedule_from_file(
        self, league: str, season: int
    ) -> Optional[pd.DataFrame]:
        """Read schedule from JSON file saved by schedule_task."""
        safe_league = league.replace(' ', '_').replace('-', '_')
        path = f'/tmp/fbref_schedule_{safe_league}_{season}.json'

        if not os.path.exists(path):
            logger.debug(f"Schedule JSON not found: {path}")
            return None

        try:
            df = pd.read_json(path, orient='records')
            if df.empty:
                logger.warning(f"Schedule JSON is empty: {path}")
                return None

            if 'match_url' not in df.columns:
                logger.warning(f"Schedule JSON missing 'match_url' column: {path}")
                return None

            non_null_urls = df['match_url'].dropna()
            if non_null_urls.empty:
                logger.warning(
                    f"Schedule JSON has {len(df)} rows but all match_url are NULL"
                )
                return None

            logger.info(
                f"File: read {len(df)} schedule rows for {league} {season} "
                f"from {path} ({len(non_null_urls)} with match_url)"
            )
            return df

        except Exception as e:
            logger.warning(f"File: could not read schedule from {path}: {e}")
            return None

    # ------------------------------------------------------------------
    # Combined match data: main method
    # ------------------------------------------------------------------

    def scrape_combined_match_data(
        self,
        max_matches: Optional[int] = 50,
        incremental: bool = True,
        deadline_minutes: int = 230,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape ALL match-level data in one pass.

        Collects shot_events, match_events, lineups, match_team_stats,
        and match_player_stats simultaneously by visiting each match page
        only once.

        Features:
        - Parse Once: single BS4 parse per match
        - Incremental: skips matches already in Iceberg (via lineups table)
        - Batch saving every BATCH_SAVE_INTERVAL matches (prevents data loss)
        - Failed match retry with browser restart (recovers ~50-70%)

        Args:
            max_matches: Maximum number of matches per league/season (default 50)
            incremental: Skip matches already in Iceberg (default True)

        Returns:
            Dictionary mapping data_type to Iceberg table path
            Keys: 'shot_events', 'match_events', 'lineups',
                  'match_team_stats', 'match_player_stats'
        """
        logger.info(
            f"Starting combined match data scrape: "
            f"max_matches={max_matches}, leagues={self.leagues}, seasons={self.seasons}"
        )

        # Pre-flight Trino probe: fail fast if Trino is unreachable
        # (avoids 18+ seconds of retries per league/season in _read_schedule_from_iceberg)
        try:
            if not hasattr(self, '_iceberg_writer') or self._iceberg_writer is None:
                from scrapers.base.iceberg_writer import IcebergWriter
                self._iceberg_writer = IcebergWriter()

            trino_mgr = self._iceberg_writer._get_trino_manager()
            cursor = trino_mgr.connection.cursor()
            cursor.execute('SELECT 1')
            cursor.fetchall()
            cursor.close()
            self._stats['trino_available'] = True
            logger.info("Pre-flight Trino probe: OK")
        except Exception as e:
            self._stats['trino_available'] = False
            logger.warning(
                f"Pre-flight Trino probe failed: {e}. "
                f"Will rely on file fallback for schedule."
            )

        _deadline = time.time() + deadline_minutes * 60

        all_shot_events = []
        all_match_events = []
        all_lineups = []
        all_match_team_stats = []
        all_match_player_stats = []
        all_match_managers = []
        all_match_officials = []
        all_match_keeper_stats = []

        total_matches_processed = 0
        total_pages_fetched = 0
        total_league_seasons = 0
        skipped_league_seasons = 0
        results = {}

        # Shared kwargs for _batch_save_match_data
        batch_kw = dict(
            all_match_team_stats=all_match_team_stats,
            all_match_player_stats=all_match_player_stats,
            all_match_managers=all_match_managers,
            all_match_officials=all_match_officials,
            all_match_keeper_stats=all_match_keeper_stats,
        )

        for league in self.leagues:
            for season in self.seasons:
                total_league_seasons += 1
                try:
                    # 3-level fallback: file → Iceberg → HTTP
                    schedule_df = self._read_schedule_from_file(league, season)

                    if schedule_df is not None and not schedule_df.empty:
                        self._stats['schedule_source'] = 'file'
                        logger.info(
                            f"Using schedule from file for {league} {season} "
                            f"({len(schedule_df)} rows)"
                        )
                    else:
                        schedule_df = self._read_schedule_from_iceberg(
                            league, season
                        )
                        if schedule_df is not None and not schedule_df.empty:
                            self._stats['schedule_source'] = 'iceberg'
                            logger.info(
                                f"Using schedule from Iceberg for {league} "
                                f"{season} ({len(schedule_df)} rows)"
                            )
                        else:
                            self._stats['schedule_source'] = 'none'
                            logger.error(
                                f"Schedule not available from file/Iceberg for {league} {season}. "
                                f"Ensure schedule_task completed. Skipping."
                            )
                            skipped_league_seasons += 1
                            self._stats['failures'] = self._stats.get('failures', 0) + 1
                            self._stats['skipped_league_seasons'] = skipped_league_seasons
                            continue

                    if schedule_df is None or schedule_df.empty:
                        self._stats['schedule_source'] = 'none'
                        logger.warning(
                            f"No schedule found for {league} {season}, "
                            f"skipping match data collection"
                        )
                        skipped_league_seasons += 1
                        self._stats['failures'] = self._stats.get('failures', 0) + 1
                        self._stats['skipped_league_seasons'] = skipped_league_seasons
                        continue

                    logger.info(f"Extracting match IDs from schedule ({len(schedule_df)} rows)...")
                    match_ids = self._extract_match_ids(schedule_df, max_matches)
                    del schedule_df  # Free ~1MB DataFrame

                    # Incremental: skip matches already in fbref_match_player_stats.
                    # See _get_existing_match_ids — that table is the
                    # authoritative output of the combined-pass pipeline.
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
                        self._nodriver_browser.restart_browser(reason='post_schedule')

                    failed_match_ids = []
                    # Matches where page loaded but match_player_stats was missing
                    # (e.g. lineups parsed OK, but stats_*_summary table absent).
                    # These need a retry too — without it, holes in
                    # fbref_match_player_stats persist across DAG runs.
                    partial_match_ids = []
                    # 20→50: with per-URL retries and proxy rotation most
                    # transient failures recover; keep circuit breaker only as
                    # a safety net for systemic outages (proxy pool dead, CF ban).
                    MAX_CONSECUTIVE_FAILURES = 50
                    consecutive_failures = 0

                    for idx, match_id in enumerate(match_ids):
                        logger.info(f"Processing match {idx+1}/{len(match_ids)}: {match_id}")
                        try:
                            got_types = self._process_single_match(
                                match_id, league, season,
                                all_shot_events, all_match_events, all_lineups,
                                all_match_team_stats, all_match_player_stats,
                                all_match_managers,
                                all_match_officials,
                                all_match_keeper_stats,
                            )

                            if got_types:
                                total_matches_processed += 1
                                consecutive_failures = 0
                                if 'match_player_stats' not in got_types:
                                    partial_match_ids.append(match_id)
                                    logger.warning(
                                        f"Partial data for match {match_id}: "
                                        f"got {got_types}, missing match_player_stats"
                                    )
                            else:
                                failed_match_ids.append(match_id)
                                consecutive_failures += 1
                                logger.warning(
                                    f"No data extracted for match {match_id}, "
                                    f"will retry later ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES} consecutive failures)"
                                )
                                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                                    logger.error(
                                        f"Circuit breaker: {consecutive_failures} consecutive failures. "
                                        f"Stopping match processing for {league} {season}."
                                    )
                                    break

                            total_pages_fetched += 1

                            # Rate limiting between matches
                            time.sleep(0.5)

                            # Check internal deadline
                            if time.time() > _deadline:
                                logger.warning(
                                    f"Deadline {deadline_minutes}m reached after {idx+1}/{len(match_ids)} matches. "
                                    f"Saving {total_matches_processed} matches processed so far."
                                )
                                break

                            # Batch save every N matches to prevent data loss
                            if (idx + 1) % self.BATCH_SAVE_INTERVAL == 0:
                                self._batch_save_match_data(
                                    all_shot_events, all_match_events, all_lineups,
                                    results,
                                    batch_label=f" (after {idx+1}/{len(match_ids)} matches)",
                                    **batch_kw,
                                )
                                # Aggressive memory reclaim after batch save
                                # (scheduler has only 2G limit — OOM killer target)
                                gc.collect()

                        except Exception as e:
                            logger.error(
                                f"Error collecting combined data for match {match_id}: {e}"
                            )
                            failed_match_ids.append(match_id)
                            continue

                    # Retry failed + partial matches with browser restart.
                    # Partial = page loaded but match_player_stats missing;
                    # without retry these holes persist across DAG runs.
                    retry_ids = failed_match_ids + partial_match_ids
                    if retry_ids and time.time() <= _deadline:
                        logger.info(
                            f"Retrying {len(retry_ids)} matches "
                            f"({len(failed_match_ids)} failed + {len(partial_match_ids)} partial) "
                            f"with browser restart ({league}, {season})"
                        )
                        if self.use_nodriver and self._nodriver_browser is not None:
                            self._nodriver_browser.restart_browser(reason='retry_failed_matches')

                        recovered = 0
                        for match_id in retry_ids:
                            try:
                                got_types = self._process_single_match(
                                    match_id, league, season,
                                    all_shot_events, all_match_events, all_lineups,
                                    all_match_team_stats, all_match_player_stats,
                                    all_match_managers,
                                    all_match_officials,
                                    all_match_keeper_stats,
                                )
                                if got_types and 'match_player_stats' in got_types:
                                    recovered += 1
                                    if match_id in failed_match_ids:
                                        total_matches_processed += 1
                                time.sleep(1)
                            except Exception as e:
                                logger.debug(f"Retry failed for match {match_id}: {e}")

                        logger.info(
                            f"Retry complete: recovered {recovered}/{len(retry_ids)} matches (player_stats)"
                        )
                    # Save remaining data after each league/season
                    self._batch_save_match_data(
                        all_shot_events, all_match_events, all_lineups,
                        results,
                        batch_label=f" (end of {league} {season})",
                        **batch_kw,
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
                        **batch_kw,
                    )
                    continue
                finally:
                    # Memory cleanup after each league/season
                    self._cleanup_after_league()

        if skipped_league_seasons == total_league_seasons and total_league_seasons > 0:
            logger.error(
                f"All {total_league_seasons} league/season combinations were skipped "
                f"(schedule unavailable). No match data collected."
            )

        logger.info(
            f"Combined match data scrape complete: "
            f"{total_matches_processed} matches processed, "
            f"{total_pages_fetched} pages fetched, "
            f"skipped {skipped_league_seasons}/{total_league_seasons} league/seasons, "
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

                # JSON fallback for match_all_data (Trino-independent)
                for league in self.leagues:
                    for season in self.seasons:
                        league_df = combined_df[
                            (combined_df['league'] == league)
                            & (combined_df['season'] == season)
                        ]
                        if not league_df.empty:
                            safe_league = league.replace(' ', '_').replace('-', '_')
                            path = f'/tmp/fbref_schedule_{safe_league}_{season}.json'
                            league_df.to_json(
                                path, orient='records', date_format='iso'
                            )
                            logger.info(
                                f"Schedule JSON fallback: {path} "
                                f"({len(league_df)} rows)"
                            )

                table_path = self.save_to_iceberg(
                    df=combined_df,
                    table_name='fbref_schedule',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                )
                results['schedule'] = table_path
                logger.info(f"Saved {len(combined_df)} schedule rows")

            return results

        # For other data types, we need match IDs from schedule first
        all_data = []
        total_league_seasons = 0
        skipped_league_seasons = 0

        for league in self.leagues:
            for season in self.seasons:
                total_league_seasons += 1
                try:
                    # 3-level fallback: file → Iceberg → HTTP
                    schedule_df = self._read_schedule_from_file(league, season)

                    if schedule_df is not None and not schedule_df.empty:
                        logger.info(
                            f"Using schedule from file for {league} {season} "
                            f"({len(schedule_df)} rows)"
                        )
                    else:
                        schedule_df = self._read_schedule_from_iceberg(
                            league, season
                        )
                        if schedule_df is not None and not schedule_df.empty:
                            logger.info(
                                f"Using schedule from Iceberg for {league} "
                                f"{season} ({len(schedule_df)} rows)"
                            )
                        else:
                            logger.error(
                                f"Schedule not available from file/Iceberg for {league} {season}. "
                                f"Ensure schedule_task completed. Skipping."
                            )
                            skipped_league_seasons += 1
                            self._stats['failures'] = self._stats.get('failures', 0) + 1
                            continue

                    if schedule_df is None or schedule_df.empty:
                        logger.warning(
                            f"No schedule found for {league} {season}, "
                            f"skipping match data collection"
                        )
                        skipped_league_seasons += 1
                        self._stats['failures'] = self._stats.get('failures', 0) + 1
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

        if skipped_league_seasons == total_league_seasons and total_league_seasons > 0:
            logger.error(
                f"All {total_league_seasons} league/season combinations were skipped "
                f"for {data_type} (schedule unavailable). No data collected."
            )

        return results
