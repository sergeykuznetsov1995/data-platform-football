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
                self._reader = sd.Sofascore(
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

    def read_league_table(self) -> Optional[pd.DataFrame]:
        """
        Read league table (standings).

        Returns:
            DataFrame with league standings
        """
        reader = self._get_reader()
        logger.info("Fetching SofaScore league table")

        try:
            df = self._execute_with_resilience(reader.read_league_table)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'league_table')

            return df

        except Exception as e:
            logger.error(f"Error reading league table: {e}")
            return None

    def read_team_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team season statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns league table instead.
        """
        return self.read_league_table()

    def read_player_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player season statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore player stats not available in soccerdata")
        return None

    def read_team_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team match-level statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore team match stats not available in soccerdata")
        return None

    def read_player_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player match-level statistics with ratings.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore player match stats not available in soccerdata")
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

    def scrape_league_table(self) -> Dict[str, str]:
        """Scrape league table (standings)."""
        df = self.read_league_table()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_league_table',
                partition_cols=['league', 'season'],
            )
            return {'league_table': table_path}
        return {}

    def scrape_team_stats(self) -> Dict[str, str]:
        """Scrape team stats (alias for league table)."""
        return self.scrape_league_table()

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

        # Scrape league table (standings)
        table_results = self.scrape_league_table()
        results.update(table_results)

        logger.info(f"SofaScore scrape complete: {list(results.keys())}")
        return results
