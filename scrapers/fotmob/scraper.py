"""
FotMob Scraper
==============

Pure-HTTP scraper for FotMob football data. The public ``/api/data``
endpoints return JSON without Cloudflare gating or session cookies.

Provides:
- Match schedules and results
- Team season statistics
- Player season statistics

Source: https://www.fotmob.com
"""

import json
import logging
import time
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from scrapers.base.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class FotMobScraper(BaseScraper):
    """
    Pure-HTTP scraper for FotMob data.

    FotMob exposes a public ``/api/data/leagues`` endpoint that returns
    JSON with fixtures, league table, and player stat leaderboards in a
    single payload. No Cloudflare bypass, no cookies, minimal UA.

    Usage:
        scraper = FotMobScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fotmob'
    DEFAULT_RATE_LIMIT = 30  # requests per minute

    BASE_URL = 'https://www.fotmob.com'
    API_BASE = 'https://www.fotmob.com/api/data'

    # League configuration with FotMob league IDs
    LEAGUE_IDS = {
        'ENG-Premier League': '47',
        'ESP-La Liga': '87',
        'GER-Bundesliga': '54',
        'ITA-Serie A': '55',
        'FRA-Ligue 1': '53',
        'ENG-Championship': '48',
        'NED-Eredivisie': '57',
        'POR-Primeira Liga': '61',
        'UEFA-Champions League': '42',
        'UEFA-Europa League': '73',
    }

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        """
        Initialize FotMob scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            **kwargs,
        )
        self._session: Optional[requests.Session] = None

    def _format_season(self, season: int) -> str:
        """
        Format season year to FotMob format.

        FotMob uses format like '2023/2024' for season parameter.

        Args:
            season: Season start year (e.g., 2023 for 2023-2024 season)

        Returns:
            Formatted season string
        """
        return f"{season}/{season + 1}"

    def _get_session(self) -> requests.Session:
        """Get or create requests session for FotMob's public JSON API."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                'User-Agent': 'Mozilla/5.0 (compatible; data-platform-football/1.0)',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': self.BASE_URL,
                'Origin': self.BASE_URL,
            })
        return self._session

    def _fetch_api_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON data from FotMob API.

        Args:
            endpoint: API endpoint (e.g., 'leagues')
            params: Query parameters
            retry_count: Number of retries

        Returns:
            JSON response data or None
        """
        url = f"{self.API_BASE}/{endpoint}"
        session = self._get_session()

        for attempt in range(retry_count):
            try:
                # Rate limiting
                self._rate_limiter.acquire()

                response = session.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    self._stats['successes'] += 1
                    return response.json()

                logger.warning(
                    f"FotMob API returned {response.status_code} for {url} "
                    f"params={params}"
                )
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")

        self._stats['failures'] += 1
        return None

    def _get_league_data(self, league: str, season: int) -> Optional[Dict[str, Any]]:
        """
        Get league data from FotMob API.

        Args:
            league: League name
            season: Season year

        Returns:
            League data dictionary or None
        """
        league_id = self.LEAGUE_IDS.get(league)
        if not league_id:
            logger.error(f"Unknown league: {league}")
            return None

        season_str = self._format_season(season)

        return self._fetch_api_json(
            'leagues',
            params={
                'id': league_id,
                'season': season_str,
            }
        )

    def read_schedule(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read match schedule/fixtures from FotMob.

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

        logger.info(f"Fetching FotMob schedule: {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        try:
            # Extract matches from league data
            matches = []

            # FotMob stores matches in 'fixtures' section with 'allMatches'
            # Fallback to 'matches' for backwards compatibility
            match_data = data.get('fixtures', {}) or data.get('matches', {})

            all_matches = match_data.get('allMatches', [])
            if not all_matches:
                all_matches = match_data.get('data', {}).get('allMatches', [])

            for match in all_matches:
                status = match.get('status', {})
                score_str = status.get('scoreStr', '')

                # Parse score from "1 - 0" format
                home_score = None
                away_score = None
                if score_str and ' - ' in score_str:
                    try:
                        parts = score_str.split(' - ')
                        home_score = int(parts[0])
                        away_score = int(parts[1])
                    except (ValueError, IndexError):
                        pass

                match_info = {
                    'match_id': match.get('id'),
                    'date': status.get('utcTime'),
                    'home_team': match.get('home', {}).get('name'),
                    'home_team_id': match.get('home', {}).get('id'),
                    'away_team': match.get('away', {}).get('name'),
                    'away_team_id': match.get('away', {}).get('id'),
                    'home_score': home_score,
                    'away_score': away_score,
                    'is_finished': status.get('finished', False),
                    'round': match.get('round'),
                    'round_name': match.get('roundName'),
                }
                matches.append(match_info)

            if not matches:
                logger.warning(f"No matches found for {league} {season}")
                return None

            df = pd.DataFrame(matches)

            # Add metadata
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'schedule')

            logger.info(f"Parsed {len(df)} schedule entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing schedule data: {e}")
            return None

    def read_team_season_stats(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read team/squad statistics for a season.

        Args:
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

        logger.info(f"Fetching FotMob team stats: {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        try:
            # Extract team standings/stats
            teams = []

            table_data = data.get('table', [])
            if table_data:
                # Handle different table formats
                if isinstance(table_data, list) and table_data:
                    table = table_data[0] if isinstance(table_data[0], dict) else {'data': {'table': {'all': table_data}}}
                    standings = table.get('data', {}).get('table', {}).get('all', [])

                    if not standings:
                        standings = table.get('table', {}).get('all', [])

                    if not standings and isinstance(table_data[0], dict):
                        # Direct table format
                        standings = table_data

                    for team in standings:
                        team_info = {
                            'team_id': team.get('id'),
                            'team_name': team.get('name'),
                            'position': team.get('idx') or team.get('position'),
                            'played': team.get('played'),
                            'wins': team.get('wins'),
                            'draws': team.get('draws'),
                            'losses': team.get('losses'),
                            'goals_for': team.get('scoresStr', '').split('-')[0] if team.get('scoresStr') else team.get('goalsFor'),
                            'goals_against': team.get('scoresStr', '').split('-')[-1] if team.get('scoresStr') else team.get('goalsAgainst'),
                            'goal_diff': team.get('goalConDiff'),
                            'points': team.get('pts') or team.get('points'),
                            'form': team.get('form'),
                        }
                        teams.append(team_info)

            if not teams:
                logger.warning(f"No team stats found for {league} {season}")
                return None

            df = pd.DataFrame(teams)

            # Add metadata
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'team_stats')

            logger.info(f"Parsed {len(df)} team stat entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing team stats: {e}")
            return None

    def read_player_season_stats(
        self,
        stat_type: str = 'goals',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read player statistics for a season.

        Args:
            stat_type: Type of statistics (goals, assists, rating, etc.)
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

        logger.info(f"Fetching FotMob player stats ({stat_type}): {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        try:
            # Extract player stats from league data
            players = []

            stats_data = data.get('stats', {})
            if stats_data:
                # FotMob has different stat categories in 'players' list
                stat_categories = stats_data.get('players', []) or stats_data.get('topLists', [])

                for category in stat_categories:
                    cat_name = category.get('header', '') or category.get('name', '')
                    # FotMob uses 'topThree' for top players in each category
                    player_list = category.get('topThree', []) or category.get('topPlayers', [])

                    for player in player_list:
                        stat_info = player.get('stat', {})
                        player_info = {
                            'player_id': player.get('id'),
                            'player_name': player.get('name'),
                            'team_id': player.get('teamId'),
                            'team_name': player.get('teamName'),
                            'country_code': player.get('ccode'),
                            'stat_category': cat_name,
                            'stat_name': stat_info.get('name'),
                            'stat_value': player.get('value') or stat_info.get('value'),
                            'rank': player.get('rank'),
                        }
                        players.append(player_info)

            if not players:
                logger.warning(f"No player stats found for {league} {season}")
                return None

            df = pd.DataFrame(players)

            # Add metadata
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'player_stats')

            logger.info(f"Parsed {len(df)} player stat entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing player stats: {e}")
            return None

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all FotMob data for configured leagues and seasons.

        Collects:
        - Match schedules
        - Team statistics
        - Player statistics

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FotMob scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        all_schedules = []
        all_team_stats = []
        all_player_stats = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Scrape schedule
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is not None and not schedule_df.empty:
                        all_schedules.append(schedule_df)

                    # Rate limit pause between requests
                    time.sleep(2)

                    # Scrape team stats
                    team_df = self.read_team_season_stats(league, season)
                    if team_df is not None and not team_df.empty:
                        all_team_stats.append(team_df)

                    time.sleep(2)

                    # Scrape player stats
                    player_df = self.read_player_season_stats('goals', league, season)
                    if player_df is not None and not player_df.empty:
                        all_player_stats.append(player_df)

                    time.sleep(2)

                except Exception as e:
                    logger.error(f"Error scraping {league} {season}: {e}")
                    continue

        # Save to Iceberg tables
        if all_schedules:
            combined_df = pd.concat(all_schedules, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fotmob_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['schedule'] = table_path

        if all_team_stats:
            combined_df = pd.concat(all_team_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fotmob_team_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['team_stats'] = table_path

        if all_player_stats:
            combined_df = pd.concat(all_player_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fotmob_player_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['player_stats'] = table_path

        logger.info(f"FotMob scrape complete: {list(results.keys())}")
        return results

    def close(self) -> None:
        """Cleanup resources."""
        if self._session:
            self._session.close()
            self._session = None
