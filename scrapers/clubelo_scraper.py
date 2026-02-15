"""
ClubElo Scraper
===============

Scraper for ClubElo historical ELO ratings.
ClubElo provides ELO ratings for football clubs calculated using
a chess-like rating system.

Source: http://clubelo.com
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class ClubEloScraper(SoccerdataScraper):
    """
    Scraper for ClubElo historical ELO ratings.

    ClubElo provides:
    - Historical ELO ratings for clubs
    - Current ELO rankings
    - ELO history over time

    Usage:
        scraper = ClubEloScraper(leagues=['ENG-Premier League'])
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'clubelo'
    DEFAULT_RATE_LIMIT = 60  # ClubElo is quite permissive

    # Club ELO doesn't use standard league codes
    LEAGUE_MAPPING = {
        'ENG-Premier League': 'ENG',
        'ESP-La Liga': 'ESP',
        'GER-Bundesliga': 'GER',
        'ITA-Serie A': 'ITA',
        'FRA-Ligue 1': 'FRA',
        'NED-Eredivisie': 'NED',
        'POR-Primeira Liga': 'POR',
        'RUS-Premier League': 'RUS',
        'TUR-Super Lig': 'TUR',
        'UKR-Premier League': 'UKR',
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
        """Get soccerdata ClubElo reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.ClubElo(**self._sd_kwargs)
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    def read_by_date(
        self,
        date_val: Optional[date] = None
    ) -> Optional[pd.DataFrame]:
        """
        Read ELO ratings for a specific date.

        Args:
            date_val: Date to get ratings for (default: today)

        Returns:
            DataFrame with ELO ratings
        """
        reader = self._get_reader()

        if date_val is None:
            date_val = datetime.now()
        elif isinstance(date_val, date) and not isinstance(date_val, datetime):
            # soccerdata >= 1.8.8 requires datetime, not date
            date_val = datetime.combine(date_val, datetime.min.time())

        logger.info(f"Fetching ClubElo ratings for {date_val}")

        try:
            df = self._execute_with_resilience(reader.read_by_date, date_val)

            if df is not None and not df.empty:
                df = df.reset_index()

                # Add date column
                df['rating_date'] = date_val

                # Filter by leagues if specified
                if self.leagues:
                    country_codes = [
                        self.LEAGUE_MAPPING.get(league, league[:3])
                        for league in self.leagues
                    ]
                    if 'country' in df.columns:
                        df = df[df['country'].isin(country_codes)]

                df = self._add_metadata(df, 'elo_ratings')

            return df

        except Exception as e:
            logger.error(f"Error reading ClubElo by date: {e}")
            return None

    def read_team_history(
        self,
        team: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Optional[pd.DataFrame]:
        """
        Read historical ELO ratings for a specific team.

        Args:
            team: Team name as used in ClubElo
            start_date: Start date for history
            end_date: End date for history

        Returns:
            DataFrame with ELO history
        """
        reader = self._get_reader()

        logger.info(f"Fetching ClubElo history for {team}")

        try:
            df = self._execute_with_resilience(reader.read_team_history, team)

            if df is not None and not df.empty:
                df = df.reset_index()

                # Filter by date range if specified
                if 'from' in df.columns:
                    if start_date:
                        df = df[df['from'] >= pd.Timestamp(start_date)]
                    if end_date:
                        df = df[df['from'] <= pd.Timestamp(end_date)]

                df['team'] = team
                df = self._add_metadata(df, 'elo_history')

            return df

        except Exception as e:
            logger.error(f"Error reading team history for {team}: {e}")
            return None

    def scrape_current_ratings(self) -> Dict[str, str]:
        """
        Scrape current ELO ratings for all clubs.

        Returns:
            Dictionary with table path
        """
        df = self.read_by_date()

        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='clubelo_ratings',
                partition_cols=['rating_date'],
            )
            return {'current_ratings': table_path}

        return {}

    def scrape_historical_ratings(
        self,
        days_back: int = 365
    ) -> Dict[str, str]:
        """
        Scrape historical ELO ratings for past N days.

        Args:
            days_back: Number of days to scrape

        Returns:
            Dictionary with table path
        """
        all_data = []
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        current_date = start_date
        while current_date <= end_date:
            df = self.read_by_date(current_date)
            if df is not None and not df.empty:
                all_data.append(df)

            # Sample weekly for historical data
            current_date += timedelta(days=7)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='clubelo_ratings_historical',
                partition_cols=['rating_date'],
            )
            return {'historical_ratings': table_path}

        return {}

    def scrape_team_histories(
        self,
        teams: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """
        Scrape ELO history for specific teams.

        Args:
            teams: List of team names to scrape

        Returns:
            Dictionary with table path
        """
        if not teams:
            logger.warning("No teams specified for history scraping")
            return {}

        all_data = []
        for team in teams:
            df = self.read_team_history(team)
            if df is not None and not df.empty:
                all_data.append(df)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='clubelo_team_history',
                partition_cols=['team'],
            )
            return {'team_history': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all ClubElo data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(f"Starting ClubElo scrape")

        results = {}

        # Scrape current ratings
        current_results = self.scrape_current_ratings()
        results.update(current_results)

        logger.info(f"ClubElo scrape complete: {list(results.keys())}")
        return results


# Default teams for English clubs (top 20)
TOP_ENGLISH_CLUBS = [
    'Manchester City',
    'Arsenal',
    'Liverpool',
    'Chelsea',
    'Manchester United',
    'Tottenham',
    'Newcastle United',
    'Brighton',
    'Aston Villa',
    'West Ham',
    'Brentford',
    'Crystal Palace',
    'Fulham',
    'Wolverhampton',
    'Bournemouth',
    'Nottingham Forest',
    'Everton',
    'Leicester',
    'Leeds United',
    'Southampton',
]
