"""
SoFIFA Scraper
==============

Scraper for SoFIFA FIFA video game player attributes and ratings.

Source: https://sofifa.com
"""

import logging
from typing import Dict, List, Optional, Union

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
        versions: Union[str, int, List[int]] = 'latest',
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
        """Get the FlareSolverr-backed SoFIFA reader.

        sofifa.com sits behind a Cloudflare Turnstile that vanilla soccerdata
        (seleniumbase / UC) does not clear; FlareSolverr v3.4.6 (Chromium 142)
        does. The subclass keeps soccerdata's parsing and only swaps the HTTP
        transport. ``FLARESOLVERR_URL`` overrides the default endpoint.
        """
        if self._reader is None:
            import os
            from scrapers.sofifa.flaresolverr_reader import FlareSolverrSoFIFAReader
            self._reader = FlareSolverrSoFIFAReader(
                flaresolverr_url=os.environ.get(
                    'FLARESOLVERR_URL', 'http://flaresolverr:8191'
                ),
                versions=self.versions,
                leagues=self.leagues,
                **self._sd_kwargs,
            )
        return self._reader

    def get_traffic_stats(self) -> dict:
        """FlareSolverr proxy-traffic audit for this run (issue #616).

        All SoFIFA reads share one FlareSolverr session held by the reader;
        ``read_player_ratings`` (~545 pages/edition) dominates. ``fs_response_*``
        is a lower bound on residential-proxy MB, not the proxy MB itself
        (Camoufox fetches sub-resources through the proxy and returns only the
        rendered HTML) — see ``docs/research/flaresolverr-proxy-traffic-audit.md``.
        """
        reader = self._reader
        if reader is not None and getattr(reader, "_fs_client", None) is not None:
            return reader._fs_client.get_traffic_stats()
        return {}

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
            # Issue #466: propagate instead of returning None — a swallowed
            # error leaves the runner's results['errors'] empty -> exit 0 ->
            # green DAG while Bronze silently goes stale.
            logger.error(f"Error reading player data: {e}")
            raise

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
            raise

    def read_player_ratings(self) -> Optional[pd.DataFrame]:
        """Read per-player FIFA attribute ratings (issue #42).

        Pulls one sofifa.com player page per (player, version) and parses
        overall/potential + 34 detailed attributes + 5 GK skills (upstream
        loop) plus the main-6 card aggregates, market value / wage / release
        clause, contract dates and profile header (position / dob / height /
        weight / nationality). ~545 player pages per APL edition — slow
        (FlareSolverr solves a fresh Turnstile per session rotation).
        """
        reader = self._get_reader()
        logger.info(
            f"Fetching SoFIFA player ratings for versions: {self.versions}"
        )
        try:
            df = self._execute_with_resilience(reader.read_player_ratings)
            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'player_ratings')
            return df
        except Exception as e:
            logger.error(f"Error reading player ratings: {e}")
            raise

    def read_team_ratings(self) -> Optional[pd.DataFrame]:
        """Read per-team FIFA ratings (overall/attack/midfield/defence + the
        build-up / chance-creation / defence sub-ratings).

        One league-level page lists every team, so this is a single FlareSolverr
        request — well below the per-session tab-crash ceiling.
        """
        reader = self._get_reader()
        logger.info("Fetching SoFIFA team ratings")
        try:
            df = self._execute_with_resilience(reader.read_team_ratings)
            if df is not None and not df.empty:
                df = df.reset_index()
                # The team-ratings page carries no team_id; enrich it from the
                # teams lookup (same session) so the column is not all-NULL.
                df = self._enrich_team_id(df, reader)
                df = self._add_metadata(df, 'team_ratings')
            return df
        except Exception as e:
            logger.error(f"Error reading team ratings: {e}")
            raise

    @staticmethod
    def _enrich_team_id(df: pd.DataFrame, reader) -> pd.DataFrame:
        """Left-join sofifa ``team_id`` onto a team-level frame via read_teams.

        team_ratings/team pages share (league, team, fifa_edition) but only the
        teams listing exposes team_id. Best-effort: on any failure the frame is
        returned unchanged (team_id simply stays absent).
        """
        if 'team_id' in df.columns:
            return df
        try:
            teams = reader.read_teams()
            if teams is None or teams.empty:
                return df
            teams = teams.reset_index()
            if 'team_id' not in teams.columns:
                return df
            keys = [c for c in ('league', 'team', 'fifa_edition')
                    if c in df.columns and c in teams.columns]
            if not keys:
                return df
            lookup = teams[keys + ['team_id']].drop_duplicates(keys)
            return df.merge(lookup, on=keys, how='left')
        except Exception as e:
            logger.warning(f"team_id enrichment skipped: {e}")
            return df

    def read_versions(self) -> Optional[pd.DataFrame]:
        """Read the SoFIFA catalogue of FIFA releases + rating updates.

        Single request — the full editions list (version_id per fifa_edition /
        update). Lets downstream resolve a version label to its sofifa id.
        """
        reader = self._get_reader()
        logger.info("Fetching SoFIFA versions catalogue")
        try:
            df = self._execute_with_resilience(reader.read_versions)
            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'versions')
            return df
        except Exception as e:
            logger.error(f"Error reading versions: {e}")
            raise

    def read_leagues(self) -> Optional[pd.DataFrame]:
        """Read the league -> sofifa league_id lookup for the selected leagues.

        Single request. Small reference table (one row per configured league).
        """
        reader = self._get_reader()
        logger.info("Fetching SoFIFA leagues lookup")
        try:
            df = self._execute_with_resilience(reader.read_leagues)
            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'leagues')
            return df
        except Exception as e:
            logger.error(f"Error reading leagues: {e}")
            raise

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

    def _process_rating_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast SoFIFA rating columns to nullable Int64 (they arrive as str).

        Every numeric-looking column except the identity / date / text columns
        is coerced to ``Int64`` (memory ``feedback_sofifa_pipeline_full.md``);
        a column that does not parse at all is left untouched.
        """
        if df is None or df.empty:
            return df

        df = df.copy()
        string_cols = {
            'player', 'fifa_edition', 'update', 'joined', 'dob', 'position',
            'nationality', 'preferred_foot', 'body_type', 'real_face',
            'best_position', 'playstyles', 'specialities', '_source',
            '_entity_type', '_batch_id',
        }
        for col in df.columns:
            if col in string_cols or col == 'player_id':
                continue
            coerced = pd.to_numeric(df[col], errors='coerce')
            if coerced.notna().any():
                df[col] = coerced.astype('Int64')

        # joined -> date (sofifa shows 'Sep 1, 2020')
        if 'joined' in df.columns:
            df['joined'] = pd.to_datetime(df['joined'], errors='coerce')

        if 'overall_rating' in df.columns and 'potential' in df.columns:
            df['potential_diff'] = df['potential'] - df['overall_rating']

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
                part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                table_path = self.save_to_iceberg(
                    df=df,
                    table_name='sofifa_players',
                    partition_cols=part,
                    replace_partitions=part,
                )
                return {'players': table_path}

        return {}

    def scrape_teams(self) -> Dict[str, str]:
        """Scrape team data."""
        df = self.read_teams()

        if df is not None and not df.empty:
            part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofifa_teams',
                partition_cols=part,
                replace_partitions=part,
            )
            return {'teams': table_path}

        return {}

    def scrape_player_ratings(self) -> Dict[str, str]:
        """Scrape per-player FIFA attribute ratings to sofifa_player_ratings."""
        df = self.read_player_ratings()

        if df is not None and not df.empty:
            df = self._process_rating_data(df)
            if not df.empty:
                part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                table_path = self.save_to_iceberg(
                    df=df,
                    table_name='sofifa_player_ratings',
                    partition_cols=part,
                    replace_partitions=part,
                )
                return {'player_ratings': table_path}

        return {}

    def scrape_team_ratings(self) -> Dict[str, str]:
        """Scrape per-team FIFA ratings to sofifa_team_ratings."""
        df = self.read_team_ratings()
        if df is not None and not df.empty:
            part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofifa_team_ratings',
                partition_cols=part,
                replace_partitions=part,
            )
            return {'team_ratings': table_path}
        return {}

    def scrape_versions(self) -> Dict[str, str]:
        """Scrape the FIFA editions catalogue to sofifa_versions."""
        df = self.read_versions()
        if df is not None and not df.empty:
            part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofifa_versions',
                partition_cols=part,
                replace_partitions=part,
            )
            return {'versions': table_path}
        return {}

    def scrape_leagues(self) -> Dict[str, str]:
        """Scrape the league -> sofifa league_id lookup to sofifa_leagues.

        No ``fifa_edition`` column, so it is unpartitioned; replace on the
        ``league`` key to keep weekly runs idempotent (no append-duplication).
        """
        df = self.read_leagues()
        if df is not None and not df.empty:
            repl = ['league'] if 'league' in df.columns else None
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofifa_leagues',
                partition_cols=None,
                replace_partitions=repl,
            )
            return {'leagues': table_path}
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

        # Scrape team ratings
        results.update(self.scrape_team_ratings())

        # Scrape versions catalogue + leagues lookup (cheap, single request each)
        results.update(self.scrape_versions())
        results.update(self.scrape_leagues())

        # Scrape per-player attribute ratings (issue #42)
        ratings_results = self.scrape_player_ratings()
        results.update(ratings_results)

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
