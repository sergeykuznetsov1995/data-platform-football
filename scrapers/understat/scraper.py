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


def _install_understat_roster_patch() -> None:
    """Patch soccerdata 1.8.8's Understat._read_match for list-shaped rosters.

    Understat's per-match ``rosters["h"]/["a"]`` are normally a dict keyed by
    player id, but for some matches (observed GER-Bundesliga 2024/25) the site
    returns a *list* instead. Stock ``_read_match`` does
    ``next(iter(rosters[side].values()))`` and downstream code iterates
    ``rostersData.values()`` — both raise ``'list' object has no attribute
    'values'`` and kill the whole shots / player_match_stats scrape. We
    normalize a list roster to a dict keyed by player id so both this method and
    its downstream consumers parse cleanly. Faithful reimplementation of the
    v1.8.8 method with only that normalization added; applied once at import.
    """
    import json

    try:
        import soccerdata.understat as _us
    except Exception:
        # soccerdata not importable as a real package (e.g. mocked in unit
        # tests). The patch is a runtime-only fix; skip silently.
        return

    if getattr(_us.Understat, "_dpf_roster_patch", False):
        return

    def _read_match(self, url, match_id):
        self._ensure_cookies()
        try:
            api_url = _us.UNDERSTAT_URL + f"/getMatchData/{match_id}"
            filepath = self.data_dir / f"match_{match_id}.json"
            reader = self._request_api(api_url, filepath)
            data = json.load(reader)

            home_team_name = self._extract_team_name(data["tmpl"]["home"])
            away_team_name = self._extract_team_name(data["tmpl"]["away"])
            rosters = data["rosters"]
            # Normalize list-shaped rosters to a dict keyed by player id.
            for side in ("h", "a"):
                if isinstance(rosters.get(side), list):
                    rosters[side] = {
                        str(p.get("id", i)): p
                        for i, p in enumerate(rosters[side])
                    }
            # A match with an empty roster (a data-less fixture that slipped the
            # schedule filter) has no usable team ids — skip it like a failed
            # fetch; the callers do ``if data is None: continue``.
            if not rosters.get("h") or not rosters.get("a"):
                logger.warning(
                    "Skipping match %s: empty roster (data-less fixture)",
                    match_id,
                )
                return None
            home_team_id = next(iter(rosters["h"].values()))["team_id"]
            away_team_id = next(iter(rosters["a"].values()))["team_id"]

            match_info = {
                "h": home_team_id,
                "a": away_team_id,
                "team_h": home_team_name,
                "team_a": away_team_name,
            }
            return {
                "match_info": match_info,
                "rostersData": rosters,
                "shotsData": data["shots"],
            }
        except ConnectionError:
            return None

    _us.Understat._read_match = _read_match
    _us.Understat._dpf_roster_patch = True


_install_understat_roster_patch()


class UnderstatScraper(SoccerdataScraper):
    """
    Scraper for Understat xG statistics.

    Understat provides:
    - Shot-level xG data with coordinates
    - Player xG/xA statistics
    - Team xG statistics
    - Match-level xG data

    Coverage: Top 5 European leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1)

    Note: Understat's per-match endpoint (getMatchData) is fetched by soccerdata
    without any inter-request delay — a full-season backfill fires ~380 requests
    back-to-back (known limitation, lives inside the library). Steady-state runs
    only fetch new matches thanks to the persistent page cache.

    Usage:
        scraper = UnderstatScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        df = scraper.read_schedule()
    """

    SOURCE_NAME = 'understat'
    DEFAULT_RATE_LIMIT = 30

    # Understat only covers these leagues. The site also has RUS-Premier League
    # (RFPL), but soccerdata 1.8.8 has no league_dict entry for it — enabling
    # it requires a custom ~/soccerdata/config/league_dict.json (lives in the
    # soccerdata_cache volume) before adding it here.
    SUPPORTED_LEAGUES = [
        'ENG-Premier League',
        'ESP-La Liga',
        'GER-Bundesliga',
        'ITA-Serie A',
        'FRA-Ligue 1',
    ]

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        # Filter to only supported leagues
        if leagues:
            unsupported = [l for l in leagues if l not in self.SUPPORTED_LEAGUES]
            if unsupported:
                logger.warning(
                    f"Dropping leagues not covered by Understat: {unsupported}"
                )
            leagues = [l for l in leagues if l in self.SUPPORTED_LEAGUES]
            if not leagues:
                raise ValueError(
                    f"No supported Understat leagues left after filtering "
                    f"(dropped: {unsupported}); supported: {self.SUPPORTED_LEAGUES}"
                )
        else:
            leagues = list(self.SUPPORTED_LEAGUES)

        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None
        # Season-rollover guard state: soccerdata caches the seasons index
        # (getStatData -> leagues.json) FOREVER, and the cache volume persists.
        # A frozen index hides a new season -> read_seasons() returns empty ->
        # every read_* returns an empty frame. Refresh it once per run (23 KB).
        self._leagues_index_refreshed = False
        # True after read_schedule() has downloaded a fresh league JSON for this
        # run — lets later read_* calls reuse it via force_cache instead of
        # re-downloading (~95 KB wire per league per call).
        self._league_json_fresh = False

    def _refresh_leagues_index(self, reader) -> None:
        """Re-download the seasons index once per run (season-rollover guard)."""
        if not self._leagues_index_refreshed:
            # Private soccerdata API — precedent #444; rewrites leagues.json.
            reader._read_leagues(no_cache=True)
            self._leagues_index_refreshed = True

    def _get_reader(self):
        """Get soccerdata Understat reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                reader = sd.Understat(
                    leagues=self.leagues,
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
                self._refresh_leagues_index(reader)
                self._reader = reader
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
            # Current-season league JSON is now fresh in the page cache —
            # subsequent read_* calls in this run may force_cache it.
            self._league_json_fresh = True

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
            # Reuse the league JSON downloaded by read_schedule() this run
            # instead of re-fetching it (~95 KB wire per league).
            df = self._execute_with_resilience(
                reader.read_player_season_stats,
                force_cache=self._league_json_fresh,
            )

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
                # Season-rollover guard also when this method runs standalone
                # (no-op if _get_reader already refreshed the index this run).
                self._refresh_leagues_index(reader)
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

        # #444: soccerdata 1.8.8 builds shot `assist_player_id` from the
        # roster-ROW id (`player["id"]`, range 414509…793112) instead of the
        # true player id (understat.py:580) — so the column never matches
        # xref_player and assist resolution downstream was 100% NULL. Same family
        # as the per-league workaround above. The assister NAME (`assist_player`)
        # IS correct, so re-derive the id from this scrape's own shooter
        # (player→player_id) pairs. Assisters who took no shot here stay NA — an
        # honest NULL beats a bogus roster id (Gold fills the rest by name).
        # Keyed per (league, season) so multi-league frames can't cross-match
        # namesakes from another league.
        remap_cols = {'player', 'player_id', 'assist_player', 'assist_player_id'}
        if remap_cols.issubset(df.columns):
            name_to_id = dict(zip(
                zip(df['league'], df['season'], df['player']),
                df['player_id'],
            ))
            assist_keys = pd.Series(
                list(zip(df['league'], df['season'], df['assist_player'])),
                index=df.index,
            )
            df['assist_player_id'] = assist_keys.map(name_to_id).astype('Int64')

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
            # Reuse the league JSON downloaded by read_schedule() this run
            # instead of re-fetching it (~95 KB wire per league).
            df = self._execute_with_resilience(
                reader.read_team_match_stats,
                force_cache=self._league_json_fresh,
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'team_match_stats')

            return df

        except Exception as e:
            logger.error(f"Error reading team match stats: {e}")
            raise

    # (read method, bronze table, results key) — the 5-table source contract.
    # Keep in sync with dags/scripts/run_understat_scraper.py, the Airflow
    # orchestrator (same tables plus per-table error collection / exit codes).
    TABLE_SPECS = [
        ('read_schedule', 'understat_schedule', 'schedule'),
        ('read_shot_events', 'understat_shots', 'shots'),
        ('read_player_season_stats', 'understat_players', 'player_stats'),
        ('read_team_match_stats', 'understat_team_match_stats',
         'team_match_stats'),
        ('read_player_match_stats', 'understat_player_match_stats',
         'player_match_stats'),
    ]

    def scrape_all(
        self, min_replace_ratio: Optional[float] = 0.9
    ) -> Dict[str, str]:
        """
        Scrape all 5 Understat bronze tables (ad-hoc/manual path).

        Previously covered only 3 of the 5 tables and saved WITHOUT the
        replace-completeness guard — now mirrors the runner: full contract,
        guard armed by default. Pass ``min_replace_ratio=None`` for a
        deliberate first backfill (mirrors the runner's ``--force-replace``).
        An empty frame raises — same fail-closed stance as the runner (an
        empty scrape means the season is missing from the source).

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting Understat scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        for method_name, table_name, key in self.TABLE_SPECS:
            df = getattr(self, method_name)()
            if df is None or df.empty:
                raise ValueError(f"{table_name}: empty scrape result (0 rows)")
            results[key] = self.save_to_iceberg(
                df=df,
                table_name=table_name,
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=min_replace_ratio,
            )

        logger.info(f"Understat scrape complete: {list(results.keys())}")
        return results
