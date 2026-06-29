"""
FBref Data Merger Mixin
========================

Helper for extracting match IDs from schedule DataFrames.
"""

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class FBrefDataMergerMixin:
    """
    Mixin providing schedule/match-ID helpers for FBrefScraper.

    Expects the host class to provide:
        - (nothing beyond standard Python)
    """

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

        # Deduplicate match IDs preserving order
        seen = set()
        unique_ids = []
        for mid in match_ids:
            if mid not in seen:
                seen.add(mid)
                unique_ids.append(mid)
        if len(unique_ids) < len(match_ids):
            logger.info(f"Deduplicated match IDs: {len(match_ids)} -> {len(unique_ids)} unique")
        match_ids = unique_ids

        if max_matches is not None:
            match_ids = match_ids[:max_matches]

        return match_ids
