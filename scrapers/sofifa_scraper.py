"""
SoFIFA Scraper
==============

Scraper for SoFIFA FIFA video game player attributes and ratings.

Source: https://sofifa.com
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class SoFIFAScraper(SoccerdataScraper):
    """
    Scraper for SoFIFA player ratings and attributes.

    SoFIFA provides FIFA video game data including:
    - Player overall ratings
    - Detailed attribute breakdowns
    - Player positions and work rates
    - Contract and wage information
    - Historical rating changes

    Usage:
        scraper = SoFIFAScraper(versions=['24', '23'])
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'sofifa'
    DEFAULT_RATE_LIMIT = 30

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        versions: str = 'latest',
        **kwargs
    ):
        """
        Initialize SoFIFA scraper.

        Args:
            leagues: List of leagues (used for filtering)
            seasons: List of seasons (not used, use versions instead)
            versions: FIFA versions - "latest", "all", or version IDs from URL
        """
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self.versions = versions
        self._reader = None

    def _get_reader(self):
        """Get soccerdata SoFIFA reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.SoFIFA(
                    versions=self.versions,
                    **self._sd_kwargs
                )
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    def read_players(self) -> Optional[pd.DataFrame]:
        """
        Read player ratings and attributes.

        Returns:
            DataFrame with player data
        """
        reader = self._get_reader()
        logger.info(f"Fetching SoFIFA player data for versions: {self.versions}")

        try:
            df = self._execute_with_resilience(reader.read_players)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'players')

            return df

        except Exception as e:
            logger.error(f"Error reading player data: {e}")
            return None

    def read_teams(self) -> Optional[pd.DataFrame]:
        """
        Read team data.

        Returns:
            DataFrame with team data
        """
        reader = self._get_reader()
        logger.info("Fetching SoFIFA team data")

        try:
            df = self._execute_with_resilience(reader.read_teams)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'teams')

            return df

        except Exception as e:
            logger.error(f"Error reading team data: {e}")
            return None

    def _process_player_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean player data.

        Args:
            df: Raw player DataFrame

        Returns:
            Processed DataFrame
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        # Parse contract dates if present
        if 'joined' in df.columns:
            df['joined'] = pd.to_datetime(df['joined'], errors='coerce')

        if 'contract_valid_until' in df.columns:
            df['contract_valid_until'] = pd.to_numeric(
                df['contract_valid_until'], errors='coerce'
            )

        # Parse wage and value
        for col in ['wage_eur', 'value_eur']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Calculate derived metrics
        if 'overall' in df.columns and 'potential' in df.columns:
            df['potential_diff'] = df['potential'] - df['overall']

        return df

    def filter_by_league(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter players by league if leagues are specified.

        Args:
            df: Player DataFrame

        Returns:
            Filtered DataFrame
        """
        if not self.leagues or df is None or df.empty:
            return df

        # SoFIFA returns league names in the same format we use (e.g., 'ENG-Premier League')
        # No mapping needed
        if 'league' in df.columns:
            return df[df['league'].isin(self.leagues)]

        return df

    def scrape_players(self) -> Dict[str, str]:
        """Scrape player data."""
        df = self.read_players()

        if df is not None and not df.empty:
            df = self._process_player_data(df)
            df = self.filter_by_league(df)

            if not df.empty:
                table_path = self.save_to_iceberg(
                    df=df,
                    table_name='sofifa_players',
                    partition_cols=['version'] if 'version' in df.columns else None,
                )
                return {'players': table_path}

        return {}

    def scrape_teams(self) -> Dict[str, str]:
        """Scrape team data."""
        df = self.read_teams()

        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofifa_teams',
                partition_cols=['version'] if 'version' in df.columns else None,
            )
            return {'teams': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all SoFIFA data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(f"Starting SoFIFA scrape: versions={self.versions}")

        results = {}

        # Scrape players
        player_results = self.scrape_players()
        results.update(player_results)

        # Scrape teams
        team_results = self.scrape_teams()
        results.update(team_results)

        logger.info(f"SoFIFA scrape complete: {list(results.keys())}")
        return results


# Common FIFA attribute groups for analysis
PACE_ATTRIBUTES = ['acceleration', 'sprint_speed']
SHOOTING_ATTRIBUTES = ['positioning', 'finishing', 'shot_power', 'long_shots', 'volleys', 'penalties']
PASSING_ATTRIBUTES = ['vision', 'crossing', 'free_kick_accuracy', 'short_passing', 'long_passing', 'curve']
DRIBBLING_ATTRIBUTES = ['agility', 'balance', 'reactions', 'ball_control', 'dribbling', 'composure']
DEFENDING_ATTRIBUTES = ['interceptions', 'heading_accuracy', 'marking', 'standing_tackle', 'sliding_tackle']
PHYSICAL_ATTRIBUTES = ['jumping', 'stamina', 'strength', 'aggression']

GOALKEEPER_ATTRIBUTES = [
    'gk_diving', 'gk_handling', 'gk_kicking',
    'gk_positioning', 'gk_reflexes'
]
