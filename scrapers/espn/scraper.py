"""
ESPN Scraper
============

Scraper for ESPN football data including schedules and results.

Source: https://www.espn.com
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class ESPNScraper(SoccerdataScraper):
    """
    Scraper for ESPN football data.

    ESPN provides:
    - Match schedules and results
    - Basic team information

    Usage:
        scraper = ESPNScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'espn'
    DEFAULT_RATE_LIMIT = 30

    # ESPN league ID mapping
    LEAGUE_IDS = {
        'ENG-Premier League': 'eng.1',
        'ESP-La Liga': 'esp.1',
        'GER-Bundesliga': 'ger.1',
        'ITA-Serie A': 'ita.1',
        'FRA-Ligue 1': 'fra.1',
        'USA-MLS': 'usa.1',
        'ENG-Championship': 'eng.2',
        'ENG-League One': 'eng.3',
        'ENG-League Two': 'eng.4',
    }

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None

    def _get_reader(self):
        """Get soccerdata ESPN reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.ESPN(
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
        logger.info("Fetching ESPN schedule")

        try:
            df = self._execute_with_resilience(reader.read_schedule)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'schedule')

            return df

        except Exception as e:
            # Issue #466: propagate instead of returning None — a swallowed
            # error leaves the runner's results['errors'] empty -> exit 0 ->
            # green DAG while Bronze silently goes stale.
            logger.error(f"Error reading schedule: {e}")
            raise

    def _standardize_schedule(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize schedule column names.

        Args:
            df: Raw schedule DataFrame

        Returns:
            Standardized DataFrame
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        # Common column renames
        column_mapping = {
            'date': 'match_date',
            'home_team': 'home_team',
            'away_team': 'away_team',
            'home_score': 'home_goals',
            'away_score': 'away_goals',
            'venue': 'venue',
            'attendance': 'attendance',
        }

        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and old_col != new_col:
                df = df.rename(columns={old_col: new_col})

        return df

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()

        if df is not None and not df.empty:
            df = self._standardize_schedule(df)
            table_path = self.save_to_iceberg(
                df=df,
                table_name='espn_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'schedule': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all ESPN data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting ESPN scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Scrape schedule
        schedule_results = self.scrape_schedule()
        results.update(schedule_results)

        logger.info(f"ESPN scrape complete: {list(results.keys())}")
        return results
