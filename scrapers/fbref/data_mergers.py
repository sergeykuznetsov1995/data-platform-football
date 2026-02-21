"""
FBref Data Merger Mixin
========================

Methods for merging multiple stat-type DataFrames into wide tables
and extracting match IDs from schedule DataFrames.
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class FBrefDataMergerMixin:
    """
    Mixin providing data-merging helpers for FBrefScraper.

    Expects the host class to provide:
        - (nothing beyond standard Python)
    """

    # ------------------------------------------------------------------
    # Column helpers
    # ------------------------------------------------------------------

    def _find_join_column(
        self,
        df: pd.DataFrame,
        candidates: List[str],
    ) -> Optional[str]:
        """
        Find a join column by exact match or suffix match.

        Handles flattened MultiIndex columns like 'Standard_Player' -> matches 'Player'.

        Args:
            df: DataFrame to search in
            candidates: List of candidate column names

        Returns:
            Column name found in df, or None
        """
        # First try exact match
        for col in candidates:
            if col in df.columns:
                return col

        # Then try suffix match (for flattened MultiIndex that wasn't normalized)
        for col in candidates:
            for df_col in df.columns:
                if df_col.endswith(f'_{col}'):
                    return df_col

        return None

    # ------------------------------------------------------------------
    # Team stats merge
    # ------------------------------------------------------------------

    def _merge_team_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int,
    ) -> Optional[pd.DataFrame]:
        """
        Merge multiple team stat DataFrames into one extended table.

        Similar to _merge_player_stats but for team/squad statistics.
        Joins on team name.

        Args:
            data: Dictionary mapping stat_type to DataFrame
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with extended team stats or None
        """
        base = data.get('stats')
        if base is None or base.empty:
            logger.warning("No base 'stats' data to merge for teams")
            return None

        logger.debug(f"Base team 'stats' columns: {list(base.columns)[:15]}...")

        # Identify join column (team identifier)
        team_col = self._find_join_column(base, ['Squad', 'Team', 'squad', 'team'])
        if not team_col:
            logger.warning(
                f"No suitable join column found for team merging. "
                f"Available columns: {list(base.columns)[:10]}..."
            )
            return base

        join_cols = [team_col]
        logger.info(f"Merging team stats on column: {team_col}")

        # Track columns that are already in base
        base_cols = set(base.columns)

        for stat_type, df in data.items():
            if stat_type == 'stats' or df is None or df.empty:
                continue

            logger.debug(
                f"Processing team {stat_type}, columns: {list(df.columns)[:10]}..."
            )

            # Find matching join column in this DataFrame
            df_team_col = self._find_join_column(df, [team_col, 'Squad', 'Team'])
            if not df_team_col:
                logger.warning(
                    f"Cannot merge team {stat_type}: no common join column. "
                    f"Looking for: {join_cols}, "
                    f"Available: {list(df.columns)[:10]}..."
                )
                continue

            # Rename if needed
            if df_team_col != team_col:
                df = df.rename(columns={df_team_col: team_col})

            # Get columns to merge (excluding join column and duplicates)
            merge_cols = [team_col]
            for col in df.columns:
                if col not in base_cols and col != team_col:
                    merge_cols.append(col)

            if len(merge_cols) > 1:
                try:
                    base = base.merge(
                        df[merge_cols],
                        on=team_col,
                        how='left',
                        suffixes=('', f'_{stat_type}')
                    )
                    base_cols = set(base.columns)
                    logger.debug(
                        f"Merged team {stat_type}: "
                        f"{len(merge_cols) - 1} new columns"
                    )
                except Exception as e:
                    logger.error(f"Error merging team {stat_type}: {e}")

        # Add league/season metadata
        base['league'] = league
        base['season'] = season

        logger.info(
            f"Merged team stats: {len(base)} rows, {len(base.columns)} columns"
        )
        return base

    # ------------------------------------------------------------------
    # Player stats merge
    # ------------------------------------------------------------------

    def _merge_player_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int,
    ) -> Optional[pd.DataFrame]:
        """
        Merge multiple stat DataFrames into one extended table.

        Join on player + team combination, handling column conflicts.

        Args:
            data: Dictionary mapping stat_type to DataFrame
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with extended stats or None
        """
        base = data.get('stats')
        if base is None or base.empty:
            logger.warning("No base 'stats' data to merge")
            return None

        logger.debug(f"Base 'stats' columns: {list(base.columns)[:15]}...")

        # Identify join columns (looking for player identifier)
        join_cols = []
        player_col = self._find_join_column(
            base, ['Player', 'player', 'player_id']
        )
        if player_col:
            join_cols.append(player_col)

        # Add team if available
        team_col = self._find_join_column(base, ['Squad', 'Team', 'team'])
        if team_col:
            join_cols.append(team_col)

        if not join_cols:
            logger.warning(
                f"No suitable join columns found for merging. "
                f"Available columns: {list(base.columns)[:10]}..."
            )
            return base

        logger.info(f"Merging player stats on columns: {join_cols}")

        # Track columns that are already in base
        base_cols = set(base.columns)

        for stat_type, df in data.items():
            if stat_type == 'stats' or df is None or df.empty:
                continue

            logger.debug(
                f"Processing {stat_type}, columns: {list(df.columns)[:10]}..."
            )

            # Find matching join columns in this DataFrame using flexible search
            df_join_cols = []
            for base_col in join_cols:
                # Try exact match first
                if base_col in df.columns:
                    df_join_cols.append((base_col, base_col))
                else:
                    # Try to find equivalent column by name suffix
                    col_name = base_col.split('_')[-1]  # Get base name
                    found_col = self._find_join_column(
                        df, [col_name, base_col]
                    )
                    if found_col:
                        df_join_cols.append((base_col, found_col))

            if not df_join_cols:
                logger.warning(
                    f"Cannot merge {stat_type}: no common join columns. "
                    f"Looking for: {join_cols}, "
                    f"Available: {list(df.columns)[:10]}..."
                )
                continue

            # Rename df columns to match base for join
            rename_map = {
                df_col: base_col
                for base_col, df_col in df_join_cols
                if base_col != df_col
            }
            if rename_map:
                df = df.rename(columns=rename_map)
                logger.debug(f"Renamed columns for merge: {rename_map}")

            actual_join_cols = [base_col for base_col, _ in df_join_cols]

            # Get columns to merge (excluding join columns and duplicates)
            merge_cols = actual_join_cols.copy()
            for col in df.columns:
                if col not in base_cols and col not in actual_join_cols:
                    merge_cols.append(col)

            if len(merge_cols) > len(actual_join_cols):
                try:
                    base = base.merge(
                        df[merge_cols],
                        on=actual_join_cols,
                        how='left',
                        suffixes=('', f'_{stat_type}')
                    )
                    base_cols = set(base.columns)
                    logger.debug(
                        f"Merged {stat_type}: "
                        f"{len(merge_cols) - len(actual_join_cols)} new columns"
                    )
                except Exception as e:
                    logger.error(f"Error merging {stat_type}: {e}")

        # Add league/season metadata
        base['league'] = league
        base['season'] = season

        logger.info(
            f"Merged player stats: {len(base)} rows, {len(base.columns)} columns"
        )
        return base

    # ------------------------------------------------------------------
    # Keeper stats merge
    # ------------------------------------------------------------------

    def _merge_keeper_stats(
        self,
        data: Dict[str, pd.DataFrame],
        league: str,
        season: int,
    ) -> Optional[pd.DataFrame]:
        """
        Merge keeper and keeper_adv DataFrames.

        Args:
            data: Dictionary with 'keeper' and 'keeper_adv' DataFrames
            league: League name
            season: Season year

        Returns:
            Merged DataFrame with keeper stats or None
        """
        base = data.get('keeper')
        if base is None or base.empty:
            return None

        # Identify join columns using flexible search
        join_cols = []
        player_col = self._find_join_column(
            base, ['Player', 'player', 'player_id']
        )
        if player_col:
            join_cols.append(player_col)

        team_col = self._find_join_column(base, ['Squad', 'Team', 'team'])
        if team_col:
            join_cols.append(team_col)

        if not join_cols:
            return base

        adv = data.get('keeper_adv')
        if adv is not None and not adv.empty:
            # Find matching join columns using flexible search
            adv_join_cols = []
            for base_col in join_cols:
                if base_col in adv.columns:
                    adv_join_cols.append((base_col, base_col))
                else:
                    col_name = base_col.split('_')[-1]
                    found_col = self._find_join_column(adv, [col_name, base_col])
                    if found_col:
                        adv_join_cols.append((base_col, found_col))

            if adv_join_cols:
                # Rename adv columns to match base for join
                rename_map = {
                    adv_col: base_col
                    for base_col, adv_col in adv_join_cols
                    if base_col != adv_col
                }
                if rename_map:
                    adv = adv.rename(columns=rename_map)

                actual_join_cols = [base_col for base_col, _ in adv_join_cols]
                base_cols = set(base.columns)
                merge_cols = actual_join_cols.copy()
                for col in adv.columns:
                    if col not in base_cols and col not in actual_join_cols:
                        merge_cols.append(col)

                if len(merge_cols) > len(actual_join_cols):
                    try:
                        base = base.merge(
                            adv[merge_cols],
                            on=actual_join_cols,
                            how='left'
                        )
                    except Exception as e:
                        logger.error(f"Error merging keeper_adv: {e}")

        base['league'] = league
        base['season'] = season
        return base

    # ------------------------------------------------------------------
    # Match ID extraction
    # ------------------------------------------------------------------

    def _extract_match_ids(
        self,
        schedule_df: pd.DataFrame,
        max_matches: Optional[int] = None,
    ) -> List[str]:
        """
        Extract match IDs from schedule DataFrame.

        Args:
            schedule_df: Schedule DataFrame
            max_matches: Maximum number of matches to return (None for all)

        Returns:
            List of match IDs
        """
        match_ids = []

        if schedule_df is None or schedule_df.empty:
            return match_ids

        if 'match_id' in schedule_df.columns:
            match_ids = schedule_df['match_id'].dropna().tolist()
        elif 'match_url' in schedule_df.columns:
            # Extract from match_url column (added by read_schedule)
            for url in schedule_df['match_url'].dropna():
                if '/matches/' in str(url):
                    mid = str(url).split('/matches/')[-1].split('/')[0]
                    match_ids.append(mid)
            logger.info(f"Extracted {len(match_ids)} match IDs from match_url column")
        elif 'Match Report' in schedule_df.columns:
            # Fallback: Extract from Match Report column (if it contains URLs)
            for url in schedule_df['Match Report'].dropna():
                if '/matches/' in str(url):
                    mid = str(url).split('/matches/')[-1].split('/')[0]
                    match_ids.append(mid)

        if max_matches is not None:
            match_ids = match_ids[:max_matches]

        return match_ids
