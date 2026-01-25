"""
Match History Scraper (DEPRECATED)
==================================

Scraper for historical match data from football-data.co.uk.
Provides extensive historical results, odds, and statistics.

Source: https://www.football-data.co.uk/

.. deprecated::
    This scraper uses soccerdata which is blocked by the server (503 errors).
    Use MatchHistoryDirectScraper instead for reliable data collection.
"""

import logging
import warnings
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)

# Deprecation warning
warnings.warn(
    "MatchHistoryScraper is deprecated due to server blocking (503 errors). "
    "Use MatchHistoryDirectScraper instead for reliable data collection.",
    DeprecationWarning,
    stacklevel=2
)


class MatchHistoryScraper(SoccerdataScraper):
    """
    Scraper for historical match data from football-data.co.uk.

    Provides:
    - Match results (home/away goals)
    - Half-time scores
    - Betting odds from multiple bookmakers
    - Match statistics (shots, corners, fouls, cards)

    Usage:
        scraper = MatchHistoryScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'matchhistory'
    DEFAULT_RATE_LIMIT = 30

    # League code mapping
    LEAGUE_MAPPING = {
        'ENG-Premier League': 'ENG-Premier League',
        'ENG-Championship': 'ENG-Championship',
        'ESP-La Liga': 'ESP-La Liga',
        'GER-Bundesliga': 'GER-Bundesliga',
        'ITA-Serie A': 'ITA-Serie A',
        'FRA-Ligue 1': 'FRA-Ligue 1',
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
        """Get soccerdata MatchHistory reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.MatchHistory(
                    leagues=self.leagues or list(self.LEAGUE_MAPPING.values()),
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    def read_games(self) -> Optional[pd.DataFrame]:
        """
        Read match results and statistics.

        Returns:
            DataFrame with match data
        """
        reader = self._get_reader()

        logger.info(
            f"Fetching MatchHistory data for {len(self.leagues)} leagues, "
            f"{len(self.seasons)} seasons"
        )

        try:
            df = self._execute_with_resilience(reader.read_games)

            if df is not None and not df.empty:
                df = df.reset_index()

                # Standardize column names
                df = self._standardize_columns(df)

                df = self._add_metadata(df, 'match_results')

            return df

        except Exception as e:
            logger.error(f"Error reading MatchHistory games: {e}")
            return None

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for consistency."""
        column_mapping = {
            'Date': 'match_date',
            'HomeTeam': 'home_team',
            'AwayTeam': 'away_team',
            'FTHG': 'home_goals',
            'FTAG': 'away_goals',
            'FTR': 'result',  # H/D/A
            'HTHG': 'home_goals_ht',
            'HTAG': 'away_goals_ht',
            'HTR': 'result_ht',
            'Referee': 'referee',
            'HS': 'home_shots',
            'AS': 'away_shots',
            'HST': 'home_shots_on_target',
            'AST': 'away_shots_on_target',
            'HF': 'home_fouls',
            'AF': 'away_fouls',
            'HC': 'home_corners',
            'AC': 'away_corners',
            'HY': 'home_yellow',
            'AY': 'away_yellow',
            'HR': 'home_red',
            'AR': 'away_red',
            # Betting odds columns
            'B365H': 'odds_home_b365',
            'B365D': 'odds_draw_b365',
            'B365A': 'odds_away_b365',
            'BWH': 'odds_home_bw',
            'BWD': 'odds_draw_bw',
            'BWA': 'odds_away_bw',
            'PSH': 'odds_home_ps',
            'PSD': 'odds_draw_ps',
            'PSA': 'odds_away_ps',
        }

        # Rename columns that exist
        rename_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_cols)

        return df

    def calculate_odds_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate average odds and implied probabilities.

        Args:
            df: DataFrame with odds columns

        Returns:
            DataFrame with additional odds statistics
        """
        odds_cols_home = [c for c in df.columns if c.startswith('odds_home_')]
        odds_cols_draw = [c for c in df.columns if c.startswith('odds_draw_')]
        odds_cols_away = [c for c in df.columns if c.startswith('odds_away_')]

        if odds_cols_home:
            df['odds_home_avg'] = df[odds_cols_home].mean(axis=1)
            df['prob_home_implied'] = 1 / df['odds_home_avg']

        if odds_cols_draw:
            df['odds_draw_avg'] = df[odds_cols_draw].mean(axis=1)
            df['prob_draw_implied'] = 1 / df['odds_draw_avg']

        if odds_cols_away:
            df['odds_away_avg'] = df[odds_cols_away].mean(axis=1)
            df['prob_away_implied'] = 1 / df['odds_away_avg']

        return df

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all match history data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting MatchHistory scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Read games
        df = self.read_games()

        if df is not None and not df.empty:
            # Calculate odds statistics
            df = self.calculate_odds_stats(df)

            table_path = self.save_to_iceberg(
                df=df,
                table_name='matchhistory_results',
                partition_cols=['league', 'season'],
            )
            results['match_results'] = table_path

        logger.info(f"MatchHistory scrape complete: {list(results.keys())}")
        return results


class MatchHistoryOddsScraper(SoccerdataScraper):
    """
    Specialized scraper for betting odds from football-data.co.uk.

    Provides detailed odds analysis and implied probabilities.
    """

    SOURCE_NAME = 'matchhistory_odds'

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)

    def _get_reader(self):
        """Get soccerdata MatchHistory reader."""
        try:
            import soccerdata as sd
            return sd.MatchHistory(
                leagues=self.leagues,
                seasons=self.seasons,
                **self._sd_kwargs
            )
        except ImportError:
            logger.error("soccerdata library not installed")
            raise

    def read_odds(self) -> Optional[pd.DataFrame]:
        """
        Read betting odds data.

        Returns:
            DataFrame with odds from multiple bookmakers
        """
        reader = self._get_reader()

        try:
            df = self._execute_with_resilience(reader.read_games)

            if df is not None and not df.empty:
                df = df.reset_index()

                # Select only odds-related columns
                odds_cols = ['league', 'season', 'Date', 'HomeTeam', 'AwayTeam']
                odds_cols += [c for c in df.columns if any(
                    c.startswith(p) for p in ['B365', 'BW', 'IW', 'PS', 'WH', 'VC']
                )]

                df = df[[c for c in odds_cols if c in df.columns]]

                # Rename basic columns
                df = df.rename(columns={
                    'Date': 'match_date',
                    'HomeTeam': 'home_team',
                    'AwayTeam': 'away_team',
                })

                df = self._add_metadata(df, 'betting_odds')

            return df

        except Exception as e:
            logger.error(f"Error reading odds data: {e}")
            return None

    def scrape_all(self) -> Dict[str, str]:
        """Scrape betting odds data."""
        logger.info("Starting odds scrape")

        df = self.read_odds()

        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='matchhistory_odds',
                partition_cols=['league', 'season'],
            )
            return {'betting_odds': table_path}

        return {}
