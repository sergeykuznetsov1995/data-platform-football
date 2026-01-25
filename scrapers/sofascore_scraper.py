"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class SofaScoreScraper(SoccerdataScraper):
    """
    Scraper for SofaScore football data.

    SofaScore provides:
    - Live match data and scores
    - Detailed match statistics
    - Player ratings
    - Heatmaps and position data

    Usage:
        scraper = SofaScoreScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'sofascore'
    DEFAULT_RATE_LIMIT = 20  # SofaScore can be strict

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None

    def _get_reader(self):
        """Get soccerdata SofaScore reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.SofaScore(
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
        logger.info("Fetching SofaScore schedule")

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
        logger.info("Fetching SofaScore team stats")

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
        logger.info("Fetching SofaScore player stats")

        try:
            df = self._execute_with_resilience(reader.read_player_season_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading player stats: {e}")
            return None

    def read_team_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team match-level statistics.

        Returns:
            DataFrame with team match stats
        """
        reader = self._get_reader()
        logger.info("Fetching SofaScore team match stats")

        try:
            df = self._execute_with_resilience(reader.read_team_match_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'team_match_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading team match stats: {e}")
            return None

    def read_player_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player match-level statistics with ratings.

        Returns:
            DataFrame with player match stats
        """
        reader = self._get_reader()
        logger.info("Fetching SofaScore player match stats")

        try:
            df = self._execute_with_resilience(reader.read_player_match_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_match_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading player match stats: {e}")
            return None

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_schedule',
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
                table_name='sofascore_team_stats',
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
                table_name='sofascore_player_stats',
                partition_cols=['league', 'season'],
            )
            return {'player_stats': table_path}
        return {}

    def scrape_player_ratings(self) -> Dict[str, str]:
        """Scrape player match ratings."""
        df = self.read_player_match_stats()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_player_ratings',
                partition_cols=['league', 'season'],
            )
            return {'player_ratings': table_path}
        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all SofaScore data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting SofaScore scrape: leagues={self.leagues}, seasons={self.seasons}"
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

        # Scrape player ratings
        ratings_results = self.scrape_player_ratings()
        results.update(ratings_results)

        logger.info(f"SofaScore scrape complete: {list(results.keys())}")
        return results
