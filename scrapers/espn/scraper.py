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

    def _sanitize_match_cache(self, reader, gid) -> bool:
        """Drop roster players with an empty athlete record from the cached ESPN
        match JSON so soccerdata can parse the rest of the match.

        ESPN occasionally ships a substitute whose ``athlete`` is just
        ``{'links': ...}`` — no displayName/id — and soccerdata then raises
        ``KeyError: 'displayName'`` for the WHOLE match (#713; e.g. game 480573,
        West Ham–Stoke 2018-04-16, one nameless Stoke sub). Returns True if any
        player was removed (so the caller can retry the parse).
        """
        import json
        fp = reader.data_dir / f"Summary_{int(gid)}.json"
        if not fp.exists():
            return False
        try:
            data = json.loads(fp.read_text())
        except Exception:
            return False
        changed = False
        for r in data.get("rosters", []):
            roster = r.get("roster")
            if not roster:
                continue
            kept = [p for p in roster if "displayName" in p.get("athlete", {})]
            if len(kept) != len(roster):
                r["roster"] = kept
                changed = True
        if changed:
            fp.write_text(json.dumps(data))
        return changed

    def _read_per_match(self, method_name: str, entity: str) -> Optional[pd.DataFrame]:
        """Read a per-match entity (lineup/matchsheet) match-by-match.

        soccerdata's bulk ``read_lineup``/``read_matchsheet`` abort the WHOLE
        season if a single match has malformed ESPN JSON — e.g.
        ``KeyError: 'displayName'`` when a player lacks ``athlete.displayName``,
        seen in some older seasons. Iterating per ``match_id`` and skipping only
        the broken matches keeps the rest of the season instead of losing it all
        (resilience for #713 historical backfill).
        """
        reader = self._get_reader()
        logger.info(f"Fetching ESPN {entity} (per-match, resilient)")
        sched = self._execute_with_resilience(reader.read_schedule)
        if sched is None or sched.empty:
            return None
        game_ids = sched.reset_index()['game_id'].dropna().tolist()
        method = getattr(reader, method_name)
        frames: List[pd.DataFrame] = []
        skipped: List = []
        for gid in game_ids:
            try:
                d = method(match_id=int(gid))
                if d is not None and not d.empty:
                    frames.append(d)
            except Exception as e:
                # One roster player with an empty athlete record (only 'links',
                # no displayName) makes soccerdata KeyError on the WHOLE match.
                # Drop the nameless player(s) from the cached match JSON and retry
                # once, so the other ~35 players in that match survive (#713).
                salvaged = False
                if self._sanitize_match_cache(reader, gid):
                    try:
                        d = method(match_id=int(gid))
                        if d is not None and not d.empty:
                            frames.append(d)
                        salvaged = True
                        logger.info(
                            f"{entity}: salvaged match {gid} after dropping nameless player(s)"
                        )
                    except Exception as e2:
                        e = e2
                if not salvaged:
                    skipped.append(gid)
                    logger.warning(f"{entity}: skipping match {gid} (malformed ESPN data: {e})")
        if skipped:
            logger.warning(
                f"{entity}: skipped {len(skipped)}/{len(game_ids)} matches with malformed data"
            )
        if not frames:
            return None
        return pd.concat(frames)

    def read_lineup(self) -> Optional[pd.DataFrame]:
        """Read per-match lineups (one row per player per game)."""
        df = self._read_per_match('read_lineup', 'lineup')
        if df is not None and not df.empty:
            df = df.reset_index()
            df = self._add_metadata(df, 'lineup')
            # bronze.espn_lineup declares these as varchar, but soccerdata
            # returns int/float (when present) or NaN — coerce to nullable
            # string (NaN -> None) to match the existing Iceberg schema.
            for col in (
                'league', 'season', 'game', 'team', 'player',
                'position', 'formation_place', 'sub_in', 'sub_out',
            ):
                if col in df.columns:
                    df[col] = df[col].astype('string').where(df[col].notna(), None)
        return df

    def read_matchsheet(self) -> Optional[pd.DataFrame]:
        """Read match-level team stats + venue (one row per game per team)."""
        df = self._read_per_match('read_matchsheet', 'matchsheet')
        if df is not None and not df.empty:
            df = df.reset_index()
            df = self._add_metadata(df, 'matchsheet')
            # bronze.espn_matchsheet declares every stat column as varchar,
            # but soccerdata returns numeric/NaN — coerce every object column
            # to nullable string (NaN -> None); keep is_home (bool),
            # attendance (bigint) and _ingested_at (timestamp) at their types.
            for col in df.columns:
                if col in ('is_home', 'attendance', '_ingested_at'):
                    continue
                df[col] = df[col].astype('string').where(df[col].notna(), None)
        return df

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
                # Completeness guard (#583): refuse a replace that shrinks the
                # partition below 90% of its existing rows (one row per match).
                min_replace_ratio=0.9,
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
