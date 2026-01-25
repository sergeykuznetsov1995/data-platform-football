"""
FBref Scraper (DEPRECATED)
==========================

Scraper for FBref statistics including schedules, player stats,
team stats, and advanced metrics (xG, passing, shooting, etc.).

Source: https://fbref.com

.. deprecated::
    This scraper uses soccerdata which is blocked by Cloudflare.
    Use FBrefSeleniumScraper instead for reliable data collection.
"""

import logging
import warnings
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)

# Deprecation warning
warnings.warn(
    "FBrefScraper is deprecated due to Cloudflare blocking. "
    "Use FBrefSeleniumScraper instead for reliable data collection.",
    DeprecationWarning,
    stacklevel=2
)


class FBrefScraper(SoccerdataScraper):
    """
    Scraper for FBref football statistics.

    FBref provides comprehensive statistics including:
    - Match schedules and results
    - Player statistics (standard, shooting, passing, etc.)
    - Team statistics
    - Advanced metrics (xG, xA, progressive passes, etc.)

    Usage:
        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = 20  # FBref has strict rate limits

    # Available stat types
    STAT_TYPES = [
        'standard',
        'shooting',
        'passing',
        'passing_types',
        'gca',  # Goal and shot creation
        'defense',
        'possession',
        'playing_time',
        'misc',
        'keeper',
        'keeper_adv',
    ]

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        stat_types: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize FBref scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape
            stat_types: List of stat types to scrape (default: all)
        """
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self.stat_types = stat_types or self.STAT_TYPES
        self._reader = None

    def _get_reader(self):
        """Get soccerdata FBref reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.FBref(
                    leagues=self.leagues,
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    def read_schedule(self) -> Optional[pd.DataFrame]:
        """
        Read match schedule and results.

        Returns:
            DataFrame with match schedule
        """
        reader = self._get_reader()
        logger.info("Fetching FBref schedule")

        try:
            df = self._execute_with_resilience(reader.read_schedule)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'schedule')

            return df

        except Exception as e:
            logger.error(f"Error reading schedule: {e}")
            return None

    def read_team_season_stats(
        self,
        stat_type: str = 'standard'
    ) -> Optional[pd.DataFrame]:
        """
        Read team-level season statistics.

        Args:
            stat_type: Type of statistics to read

        Returns:
            DataFrame with team stats
        """
        reader = self._get_reader()
        logger.info(f"Fetching FBref team stats: {stat_type}")

        try:
            df = self._execute_with_resilience(
                reader.read_team_season_stats,
                stat_type=stat_type
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df['stat_type'] = stat_type
                df = self._add_metadata(df, f'team_stats_{stat_type}')

            return df

        except Exception as e:
            logger.error(f"Error reading team stats ({stat_type}): {e}")
            return None

    def read_player_season_stats(
        self,
        stat_type: str = 'standard'
    ) -> Optional[pd.DataFrame]:
        """
        Read player-level season statistics.

        Args:
            stat_type: Type of statistics to read

        Returns:
            DataFrame with player stats
        """
        reader = self._get_reader()
        logger.info(f"Fetching FBref player stats: {stat_type}")

        try:
            df = self._execute_with_resilience(
                reader.read_player_season_stats,
                stat_type=stat_type
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df['stat_type'] = stat_type
                df = self._add_metadata(df, f'player_stats_{stat_type}')

            return df

        except Exception as e:
            logger.error(f"Error reading player stats ({stat_type}): {e}")
            return None

    def read_team_match_stats(
        self,
        stat_type: str = 'summary'
    ) -> Optional[pd.DataFrame]:
        """
        Read team-level match statistics.

        Args:
            stat_type: Type of match stats

        Returns:
            DataFrame with team match stats
        """
        reader = self._get_reader()
        logger.info(f"Fetching FBref team match stats: {stat_type}")

        try:
            df = self._execute_with_resilience(
                reader.read_team_match_stats,
                stat_type=stat_type
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df['stat_type'] = stat_type
                df = self._add_metadata(df, f'team_match_stats_{stat_type}')

            return df

        except Exception as e:
            logger.error(f"Error reading team match stats ({stat_type}): {e}")
            return None

    def read_player_match_stats(
        self,
        stat_type: str = 'summary'
    ) -> Optional[pd.DataFrame]:
        """
        Read player-level match statistics.

        Args:
            stat_type: Type of match stats

        Returns:
            DataFrame with player match stats
        """
        reader = self._get_reader()
        logger.info(f"Fetching FBref player match stats: {stat_type}")

        try:
            df = self._execute_with_resilience(
                reader.read_player_match_stats,
                stat_type=stat_type
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df['stat_type'] = stat_type
                df = self._add_metadata(df, f'player_match_stats_{stat_type}')

            return df

        except Exception as e:
            logger.error(f"Error reading player match stats ({stat_type}): {e}")
            return None

    def read_lineup(self, match_id: str = None) -> Optional[pd.DataFrame]:
        """
        Read lineup data for matches.

        Args:
            match_id: Optional specific match ID

        Returns:
            DataFrame with lineup data
        """
        reader = self._get_reader()
        logger.info("Fetching FBref lineups")

        try:
            df = self._execute_with_resilience(reader.read_lineup)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'lineups')

            return df

        except Exception as e:
            logger.error(f"Error reading lineups: {e}")
            return None

    def read_shot_events(self) -> Optional[pd.DataFrame]:
        """
        Read shot-level event data.

        Returns:
            DataFrame with shot events
        """
        reader = self._get_reader()
        logger.info("Fetching FBref shot events")

        try:
            df = self._execute_with_resilience(reader.read_shot_events)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'shot_events')

            return df

        except Exception as e:
            logger.error(f"Error reading shot events: {e}")
            return None

    def read_standings(self) -> Optional[pd.DataFrame]:
        """
        Read league standings.

        Returns:
            DataFrame with standings
        """
        reader = self._get_reader()
        logger.info("Fetching FBref standings")

        try:
            df = self._execute_with_resilience(reader.read_standings)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'standings')

            return df

        except Exception as e:
            logger.error(f"Error reading standings: {e}")
            return None

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fbref_schedule',
                partition_cols=['league', 'season'],
            )
            return {'schedule': table_path}
        return {}

    def scrape_team_stats(self) -> Dict[str, str]:
        """Scrape team season statistics for all stat types."""
        results = {}
        all_data = []

        for stat_type in self.stat_types:
            df = self.read_team_season_stats(stat_type)
            if df is not None and not df.empty:
                all_data.append(df)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_stats',
                partition_cols=['league', 'season'],
            )
            results['team_stats'] = table_path

        return results

    def scrape_player_stats(self) -> Dict[str, str]:
        """Scrape player season statistics for all stat types."""
        results = {}
        all_data = []

        for stat_type in self.stat_types:
            df = self.read_player_season_stats(stat_type)
            if df is not None and not df.empty:
                all_data.append(df)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats',
                partition_cols=['league', 'season'],
            )
            results['player_stats'] = table_path

        return results

    def scrape_shot_events(self) -> Dict[str, str]:
        """Scrape shot-level events."""
        df = self.read_shot_events()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fbref_shots',
                partition_cols=['league', 'season'],
            )
            return {'shots': table_path}
        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all FBref data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FBref scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Scrape schedule
        schedule_results = self.scrape_schedule()
        results.update(schedule_results)

        # Scrape team stats
        team_results = self.scrape_team_stats()
        results.update(team_results)

        # Scrape player stats
        player_results = self.scrape_player_stats()
        results.update(player_results)

        # Scrape shot events
        shots_results = self.scrape_shot_events()
        results.update(shots_results)

        logger.info(f"FBref scrape complete: {list(results.keys())}")
        return results
