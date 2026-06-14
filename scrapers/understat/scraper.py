"""
Understat Scraper
=================

Scraper for Understat xG data including shots, player stats,
and team statistics.

Source: https://understat.com
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class UnderstatScraper(SoccerdataScraper):
    """
    Scraper for Understat xG statistics.

    Understat provides:
    - Shot-level xG data with coordinates
    - Player xG/xA statistics
    - Team xG statistics
    - Match-level xG data

    Coverage: Top 5 European leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1)

    Usage:
        scraper = UnderstatScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'understat'
    DEFAULT_RATE_LIMIT = 30

    # Understat only covers these leagues
    SUPPORTED_LEAGUES = [
        'ENG-Premier League',
        'ESP-La Liga',
        'GER-Bundesliga',
        'ITA-Serie A',
        'FRA-Ligue 1',
        'RUS-Premier League',
    ]

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        # Filter to only supported leagues
        if leagues:
            leagues = [l for l in leagues if l in self.SUPPORTED_LEAGUES]
        else:
            leagues = self.SUPPORTED_LEAGUES[:5]  # Default to top 5

        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None

    def _get_reader(self):
        """Get soccerdata Understat reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.Understat(
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
        Read match schedule with xG data.

        Returns:
            DataFrame with schedule and xG
        """
        reader = self._get_reader()
        logger.info("Fetching Understat schedule")

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

    def read_player_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player season statistics.

        Returns:
            DataFrame with player xG/xA stats
        """
        reader = self._get_reader()
        logger.info("Fetching Understat player season stats")

        try:
            df = self._execute_with_resilience(reader.read_player_season_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading player stats: {e}")
            raise

    def read_player_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player match-level statistics.

        Returns:
            DataFrame with player match stats
        """
        reader = self._get_reader()
        logger.info("Fetching Understat player match stats")

        try:
            df = self._execute_with_resilience(reader.read_player_match_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_match_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading player match stats: {e}")
            raise

    def read_shot_events(self) -> Optional[pd.DataFrame]:
        """
        Read shot-level event data with xG.

        Note: soccerdata has a bug with multiple leagues, so we fetch per league.

        Returns:
            DataFrame with shot events including coordinates and xG
        """
        import soccerdata as sd

        logger.info("Fetching Understat shot events")

        all_shots = []

        # Fetch shots per league to avoid soccerdata bug with multiple leagues
        for league in self.leagues:
            try:
                logger.info(f"Fetching shots for {league}")
                reader = sd.Understat(
                    leagues=[league],
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
                df = self._execute_with_resilience(reader.read_shot_events)

                if df is not None and not df.empty:
                    df = df.reset_index()
                    all_shots.append(df)

            except Exception as e:
                # Issue #466: a failed league must fail the run, not be
                # silently skipped — old partitions stay intact (runner saves
                # with replace_partitions only on success).
                logger.error(f"Error reading shots for {league}: {e}")
                raise

        if not all_shots:
            return None

        df = pd.concat(all_shots, ignore_index=True)
        df = self._add_metadata(df, 'shots')
        return df

    def read_team_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team match-level statistics.

        Returns:
            DataFrame with team match stats
        """
        reader = self._get_reader()
        logger.info("Fetching Understat team match stats")

        try:
            df = self._execute_with_resilience(reader.read_team_match_stats)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'team_match_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading team match stats: {e}")
            raise

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='understat_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'schedule': table_path}
        return {}

    def scrape_player_stats(self) -> Dict[str, str]:
        """Scrape player season stats."""
        df = self.read_player_season_stats()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='understat_players',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'player_stats': table_path}
        return {}

    def scrape_shots(self) -> Dict[str, str]:
        """Scrape shot events."""
        df = self.read_shot_events()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='understat_shots',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'shots': table_path}
        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all Understat data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting Understat scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Scrape schedule
        schedule_results = self.scrape_schedule()
        results.update(schedule_results)

        # Scrape player stats
        player_results = self.scrape_player_stats()
        results.update(player_results)

        # Scrape shot events
        shots_results = self.scrape_shots()
        results.update(shots_results)

        logger.info(f"Understat scrape complete: {list(results.keys())}")
        return results
