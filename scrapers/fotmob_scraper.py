"""
FotMob Scraper (DEPRECATED)
===========================

Scraper for FotMob match data, lineups, and events.

Source: https://www.fotmob.com

.. deprecated::
    This scraper uses soccerdata which cannot obtain session cookies.
    Use FotMobSeleniumScraper instead for reliable data collection.
"""

import logging
import warnings
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)

# Deprecation warning
warnings.warn(
    "FotMobScraper is deprecated due to session cookie issues. "
    "Use FotMobSeleniumScraper instead for reliable data collection.",
    DeprecationWarning,
    stacklevel=2
)


class FotMobScraper(SoccerdataScraper):
    """
    Scraper for FotMob football data.

    FotMob provides:
    - Match schedules and results
    - Lineups and formations
    - Match events (goals, cards, substitutions)
    - Team and player statistics

    Usage:
        scraper = FotMobScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fotmob'
    DEFAULT_RATE_LIMIT = 30

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None

    def _get_reader(self):
        """Get soccerdata FotMob reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.FotMob(
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
        logger.info("Fetching FotMob schedule")

        try:
            df = self._execute_with_resilience(reader.read_schedule)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'schedule')

            return df

        except Exception as e:
            logger.error(f"Error reading schedule: {e}")
            return None

    def read_team_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team season statistics.

        Returns:
            DataFrame with team stats
        """
        reader = self._get_reader()
        logger.info("Fetching FotMob team stats")

        try:
            df = self._execute_with_resilience(reader.read_team_season_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'team_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading team stats: {e}")
            return None

    def read_player_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player season statistics.

        Returns:
            DataFrame with player stats
        """
        reader = self._get_reader()
        logger.info("Fetching FotMob player stats")

        try:
            df = self._execute_with_resilience(reader.read_player_season_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading player stats: {e}")
            return None

    def read_lineup(self) -> Optional[pd.DataFrame]:
        """
        Read match lineups.

        Returns:
            DataFrame with lineup data
        """
        reader = self._get_reader()
        logger.info("Fetching FotMob lineups")

        try:
            df = self._execute_with_resilience(reader.read_lineup)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'lineups')

            return df

        except Exception as e:
            logger.error(f"Error reading lineups: {e}")
            return None

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fotmob_schedule',
                partition_cols=['league', 'season'],
            )
            return {'schedule': table_path}
        return {}

    def scrape_team_stats(self) -> Dict[str, str]:
        """Scrape team stats."""
        df = self.read_team_season_stats()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fotmob_team_stats',
                partition_cols=['league', 'season'],
            )
            return {'team_stats': table_path}
        return {}

    def scrape_player_stats(self) -> Dict[str, str]:
        """Scrape player stats."""
        df = self.read_player_season_stats()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fotmob_player_stats',
                partition_cols=['league', 'season'],
            )
            return {'player_stats': table_path}
        return {}

    def scrape_lineups(self) -> Dict[str, str]:
        """Scrape match lineups."""
        df = self.read_lineup()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='fotmob_lineups',
                partition_cols=['league', 'season'],
            )
            return {'lineups': table_path}
        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all FotMob data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FotMob scrape: leagues={self.leagues}, seasons={self.seasons}"
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

        # Scrape lineups
        lineup_results = self.scrape_lineups()
        results.update(lineup_results)

        logger.info(f"FotMob scrape complete: {list(results.keys())}")
        return results
