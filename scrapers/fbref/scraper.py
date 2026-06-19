"""
FBref Scraper
=============

Main scraper class for FBref football statistics using Selenium with Cloudflare bypass.

Source: https://fbref.com

NOTE: FBref uses Cloudflare protection. This scraper uses undetected-chromedriver
for bypassing bot detection.

Memory Optimization Notes:
- Page cache is cleared after each league to prevent OOM
- gc.collect() is called after processing each league
- Browser is restarted after MAX_PAGES_BEFORE_BROWSER_RESTART pages
- Intermediate DataFrames are explicitly deleted after merge operations

Architecture:
- FBrefBrowserMixin  — browser lifecycle, page fetching, cache (browser_manager.py)
- FBrefDataReaderMixin — all read_* methods and batch scrape helpers (data_readers.py)
- FBrefDataMergerMixin — stat merging and match-ID extraction (data_mergers.py)
- FBrefScraper (this file) — __init__, close, scrape_all, URL/HTML helpers
"""

import gc
import logging
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.fbref.browser_manager import FBrefBrowserMixin
from scrapers.fbref.data_readers import FBrefDataReaderMixin
from scrapers.fbref.data_mergers import FBrefDataMergerMixin
from scrapers.fbref.constants import (
    BASE_URL,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
    PLAYER_MATCH_STAT_TYPES,
    DEFAULT_RATE_LIMIT,
)
from scrapers.fbref.url_builder import (
    format_season,
    get_schedule_url,
    get_stats_url,
)
from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    parse_table,
)

logger = logging.getLogger(__name__)


def _env_num(name: str, default, cast):
    """Read a numeric tunable from env, falling back to `default`.

    Issue #624: cold-start / fallback thresholds are env-tunable so ops can
    dampen browser restart amplification on a bad-proxy day without a code
    change. Absent, empty, or unparseable values use `default`.
    """
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r — using default %r", name, raw, default)
        return default


class FBrefScraper(
    FBrefBrowserMixin,
    FBrefDataReaderMixin,
    FBrefDataMergerMixin,
    SeleniumScraper,
):
    """
    Scraper for FBref data using Selenium with Cloudflare bypass.

    FBref provides:
    - Match schedules and scores
    - Team season statistics
    - Player season statistics
    - Advanced metrics (xG, xA, etc.)

    IMPORTANT: FBref tables are often hidden in HTML comments.
    This scraper handles comment extraction automatically.

    Usage:
        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = DEFAULT_RATE_LIMIT

    # Memory management constants
    # 100→40→150→200: network blocking (nodriver_bypass.BLOCKED_URL_PATTERNS) reduces Chrome
    # memory usage by blocking images/CSS/fonts, allowing less frequent restarts.
    # Fewer restarts = fewer Cloudflare bypasses = less proxy traffic (each CF = ~1-2 MB).
    # 200 is safe with 12G scheduler memory + aggressive cache trim (MAX_CACHE_SIZE=5).
    MAX_PAGES_BEFORE_BROWSER_RESTART = 200
    # 30→5: page cache barely helps with unique match URLs — aggressive trim saves ~25 MB
    MAX_CACHE_SIZE = 5
    # 1→5: with aggressive timeout-ban (proxy_manager.timeout_ban_threshold=1),
    # dead proxies are removed from rotation after 1 timeout, so retrying picks
    # fresh ones. 5 attempts cycle through enough alive proxies to salvage the
    # match even when ~50% of the pool is unhealthy.
    MAX_SLOW_PROXY_RETRIES = 5

    # Re-export constants for backwards compatibility
    BASE_URL = BASE_URL
    LEAGUE_IDS = LEAGUE_IDS
    PLAYER_STAT_TYPES = PLAYER_STAT_TYPES
    TEAM_STAT_TYPES = TEAM_STAT_TYPES
    KEEPER_STAT_TYPES = KEEPER_STAT_TYPES
    PLAYER_MATCH_STAT_TYPES = PLAYER_MATCH_STAT_TYPES

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = True,
        use_xvfb: bool = True,
        proxy_file: Optional[str] = None,
        use_nodriver: bool = False,
        nodriver_cloudflare_wait: float = 30.0,
        **kwargs
    ):
        """
        Initialize FBref scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display to bypass Cloudflare headless detection
            proxy_file: Path to file with proxies (format: host:port:user:pass)
            use_nodriver: Use nodriver instead of undetected-chromedriver
                         (better Cloudflare bypass, async API)
            nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (nodriver)
            **kwargs: Additional arguments for SeleniumScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            use_xvfb=use_xvfb,
            proxy_file=proxy_file,
            **kwargs
        )
        self.use_nodriver = use_nodriver
        self.nodriver_cloudflare_wait = nodriver_cloudflare_wait
        self._page_cache: Dict[str, str] = {}
        self._pages_fetched: int = 0  # Counter for browser restart
        self._nodriver_browser = None  # Separate instance for nodriver

        # Real traffic accumulator base — preserves bytes across browser
        # restarts (each new nodriver browser resets its counter to 0).
        # Flushed in _close_browser() before browser goes away.
        self._real_traffic_base_bytes: int = 0
        self._real_traffic_base_requests: int = 0
        # Issue #44: per-resource-type breakdown + CF/restart counters,
        # also flushed in _close_browser() so per-process totals survive
        # browser restarts.
        from collections import Counter as _Counter
        self._real_traffic_base_bytes_by_rtype: _Counter = _Counter()
        self._real_traffic_base_requests_by_rtype: _Counter = _Counter()
        self._cf_challenge_attempts_base: int = 0
        self._cf_challenges_passed_base: int = 0
        self._cf_challenges_failed_base: int = 0
        self._restart_reasons_base: _Counter = _Counter()
        # Issue #116: cumulative count of loadingFinished events that found
        # no cached resource_type (diagnostic for CDP cache hit-rate).
        self._resource_type_cache_misses_base: int = 0
        # Issue #616: per-URL byte/request breakdown, also flushed across
        # browser restarts so the top-consumer audit covers the whole run.
        self._real_traffic_base_bytes_by_url: _Counter = _Counter()
        self._real_traffic_base_requests_by_url: _Counter = _Counter()

        # Consecutive fetch failure tracking for automatic proxy rotation
        # 3→15: higher MAX_SLOW_PROXY_RETRIES already handles dead proxies per URL;
        # this counter is a last-resort systemic guard, don't let it restart the
        # browser on a transient series of dead proxies.
        self._consecutive_fetch_failures: int = 0
        # Issue #624: cold-start / fallback thresholds are env-tunable so a
        # bad-proxy day can be dampened without a code change. Defaults below
        # reproduce the prior hardcoded behaviour exactly.
        self.MAX_CONSECUTIVE_FAILURES = _env_num("FBREF_MAX_CONSECUTIVE_FAILURES", 15, int)
        self.MAX_SLOW_PROXY_RETRIES = _env_num(
            "FBREF_MAX_SLOW_PROXY_RETRIES", self.MAX_SLOW_PROXY_RETRIES, int
        )
        self.SLOW_PROXY_THRESHOLD = _env_num("FBREF_SLOW_PROXY_THRESHOLD", 15.0, float)
        self._current_proxy_obj = None  # Track current proxy for result recording

        # HTTP session state (for match pages after initial CF bypass)
        self._http_session = None
        self._http_cookies_time: Optional[float] = None
        self._http_request_count: int = 0
        # Issue #624: proxy the curl_cffi session was minted on (sanitized). The
        # fallback diag compares it against the current nodriver proxy — a drift
        # means cf_clearance is IP-bound to a now-rotated proxy (mismatch), not a
        # TLS / cookie-expiry failure.
        self._http_proxy_minted: Optional[str] = None
        self.HTTP_COOKIE_TTL_MINUTES = _env_num("FBREF_HTTP_COOKIE_TTL_MINUTES", 25, float)
        self.HTTP_MAX_REQUESTS = _env_num("FBREF_HTTP_MAX_REQUESTS", 150, int)

    # ------------------------------------------------------------------
    # URL helper delegates (backwards compatibility)
    # ------------------------------------------------------------------

    def _format_season(self, season: int) -> str:
        """Format season year to FBref format."""
        return format_season(season)

    def _get_schedule_url(self, league: str, season: int) -> str:
        """Build URL for schedule/fixtures page."""
        return get_schedule_url(league, season)

    def _get_stats_url(
        self,
        league: str,
        season: int,
        stat_type: str,
        for_squads: bool = False,
    ) -> str:
        """Build URL for statistics page."""
        return get_stats_url(league, season, stat_type, for_squads)

    # ------------------------------------------------------------------
    # HTML parsing helper delegates (backwards compatibility)
    # ------------------------------------------------------------------

    def _extract_tables_from_comments(
        self,
        soup: BeautifulSoup,
    ) -> Dict[str, BeautifulSoup]:
        """Extract tables hidden in HTML comments."""
        return extract_tables_from_comments(soup)

    def _parse_table(
        self,
        soup: BeautifulSoup,
        table_id: str,
        comment_tables: Optional[Dict[str, BeautifulSoup]] = None,
    ) -> Optional[pd.DataFrame]:
        """Parse HTML table to DataFrame."""
        return parse_table(soup, table_id, comment_tables)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _cleanup_after_league(self) -> None:
        """Clean up memory after processing a league."""
        self.clear_cache()
        logger.info("Memory cleanup after league processing")

    def close(self) -> None:
        """Close browser, HTTP session, shared Xvfb, and clean up all resources."""
        self._http_session = None
        self._http_cookies_time = None
        self._http_request_count = 0
        self._close_all()  # Closes browser + shared Xvfb
        super().close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    def scrape_all(
        self,
        include_extended_stats: bool = True,
        include_match_stats: bool = False,
        include_keeper_stats: bool = True,
        include_shot_events: bool = True,
        include_match_events: bool = True,
        include_lineups: bool = True,
        include_team_match_stats: bool = False,
        include_team_stats_extended: bool = True,
        max_matches_per_league: int = 50,
    ) -> Dict[str, str]:
        """
        Scrape all FBref data for configured leagues and seasons.

        Collects:
        - Match schedules
        - Team statistics (standard)
        - Player statistics (standard)
        - Extended player statistics (merged from all stat_types)
        - Extended team statistics (merged from all stat_types)
        - Keeper statistics (basic + advanced)
        - Per-match player statistics (optional)
        - Shot events with xG and coordinates (new)
        - Match events: goals, cards, substitutions (new)
        - Lineups: starting XI and substitutes (new)
        - Team match statistics (new, optional - slow)

        Args:
            include_extended_stats: Collect extended player stats (all stat_types merged)
            include_match_stats: Collect per-match player stats (significantly slower)
            include_keeper_stats: Collect goalkeeper statistics
            include_shot_events: Collect shot events with xG data
            include_match_events: Collect match events (goals, cards, subs)
            include_lineups: Collect team lineups
            include_team_match_stats: Collect team-level match statistics (slow)
            include_team_stats_extended: Collect extended team stats (all stat_types merged)
            max_matches_per_league: Maximum matches to scrape per league/season

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FBref scrape: leagues={self.leagues}, "
            f"seasons={self.seasons}, extended={include_extended_stats}, "
            f"match_stats={include_match_stats}, keeper={include_keeper_stats}, "
            f"shot_events={include_shot_events}, match_events={include_match_events}, "
            f"lineups={include_lineups}, team_match_stats={include_team_match_stats}, "
            f"team_stats_extended={include_team_stats_extended}"
        )

        results = {}
        all_schedules = []
        all_team_stats = []
        all_team_stats_extended = []
        all_player_stats = []
        all_player_stats_extended = []
        all_keeper_stats = []
        all_match_stats = []
        all_shot_events = []
        all_match_events = []
        all_lineups = []
        all_team_match_stats = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    # Scrape schedule
                    schedule_df = self.read_schedule(league, season)
                    if schedule_df is not None and not schedule_df.empty:
                        all_schedules.append(schedule_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape team stats
                    team_df = self.read_team_season_stats('stats', league, season)
                    if team_df is not None and not team_df.empty:
                        all_team_stats.append(team_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape extended team stats (all stat_types)
                    if include_team_stats_extended:
                        team_data = {}
                        for stat_type in TEAM_STAT_TYPES:
                            df = self.read_team_season_stats(
                                stat_type, league, season
                            )
                            if df is not None and not df.empty:
                                team_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if team_data:
                            merged = self._merge_team_stats(
                                team_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_team_stats_extended.append(merged)
                            # Clean up intermediate data
                            del team_data
                            gc.collect()

                    # Scrape player stats (basic)
                    player_df = self.read_player_season_stats(
                        'stats', league, season
                    )
                    if player_df is not None and not player_df.empty:
                        all_player_stats.append(player_df)

                    time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                    # Scrape extended player stats (all stat_types)
                    if include_extended_stats:
                        player_data = {}
                        for stat_type in PLAYER_STAT_TYPES:
                            df = self.read_player_season_stats(
                                stat_type, league, season
                            )
                            if df is not None and not df.empty:
                                player_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if player_data:
                            merged = self._merge_player_stats(
                                player_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_player_stats_extended.append(merged)
                            # Clean up intermediate data
                            del player_data
                            gc.collect()

                    # Scrape keeper stats
                    if include_keeper_stats:
                        keeper_data = {}
                        for stat_type in KEEPER_STAT_TYPES:
                            df = self.read_keeper_stats(stat_type, league, season)
                            if df is not None and not df.empty:
                                keeper_data[stat_type] = df
                            time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                        if keeper_data:
                            merged = self._merge_keeper_stats(
                                keeper_data, league, season
                            )
                            if merged is not None and not merged.empty:
                                all_keeper_stats.append(merged)
                            # Clean up intermediate data
                            del keeper_data
                            gc.collect()

                    # Get match IDs for match-level data collection
                    collect_match_data = any([
                        include_match_stats,
                        include_shot_events,
                        include_match_events,
                        include_lineups,
                        include_team_match_stats,
                    ])

                    if collect_match_data and schedule_df is not None:
                        match_ids = self._extract_match_ids(
                            schedule_df, max_matches_per_league
                        )
                        logger.info(
                            f"Collecting match-level data for {len(match_ids)} matches"
                        )

                        for match_id in match_ids:
                            try:
                                # Player match stats
                                if include_match_stats:
                                    match_df = self.read_player_match_stats(
                                        match_id, league, season
                                    )
                                    if match_df is not None and not match_df.empty:
                                        all_match_stats.append(match_df)

                                # Shot events
                                if include_shot_events:
                                    shots_df = self.read_shot_events(
                                        match_id, league, season
                                    )
                                    if shots_df is not None and not shots_df.empty:
                                        all_shot_events.append(shots_df)

                                # Match events (goals, cards, subs)
                                if include_match_events:
                                    events_df = self.read_match_events(
                                        match_id, league, season
                                    )
                                    if events_df is not None and not events_df.empty:
                                        all_match_events.append(events_df)

                                # Lineups
                                if include_lineups:
                                    lineup_df = self.read_lineup(
                                        match_id, league, season
                                    )
                                    if lineup_df is not None and not lineup_df.empty:
                                        all_lineups.append(lineup_df)

                                # Team match stats
                                if include_team_match_stats:
                                    team_match_df = self.read_team_match_stats(
                                        match_id, league, season
                                    )
                                    if team_match_df is not None and not team_match_df.empty:
                                        all_team_match_stats.append(team_match_df)

                                time.sleep(1)  # Reduced from 3s - rate limiter handles main delays

                            except Exception as e:
                                logger.error(f"Error scraping match {match_id}: {e}")
                                continue

                except Exception as e:
                    logger.error(f"Error scraping {league} {season}: {e}")
                    continue
                finally:
                    # Memory cleanup after each league/season
                    self._cleanup_after_league()
                    logger.info(
                        f"Completed {league} {season}, memory cleaned up"
                    )

        # Save to Iceberg tables
        if all_schedules:
            combined_df = pd.concat(all_schedules, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['schedule'] = table_path

        if all_team_stats:
            combined_df = pd.concat(all_team_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['team_stats'] = table_path

        if all_team_stats_extended:
            combined_df = pd.concat(all_team_stats_extended, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_stats_extended',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['team_stats_extended'] = table_path

        if all_player_stats:
            combined_df = pd.concat(all_player_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['player_stats'] = table_path

        if all_player_stats_extended:
            combined_df = pd.concat(all_player_stats_extended, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_stats_extended',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['player_stats_extended'] = table_path

        if all_keeper_stats:
            combined_df = pd.concat(all_keeper_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_keeper_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['keeper_stats'] = table_path

        if all_match_stats:
            combined_df = pd.concat(all_match_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_player_match_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['match_id'],
            )
            results['player_match_stats'] = table_path

        if all_shot_events:
            combined_df = pd.concat(all_shot_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_shot_events',
                partition_cols=['league', 'season'],
                replace_partitions=['match_id'],
            )
            results['shot_events'] = table_path

        if all_match_events:
            combined_df = pd.concat(all_match_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_match_events',
                partition_cols=['league', 'season'],
                replace_partitions=['match_id'],
            )
            results['match_events'] = table_path

        if all_lineups:
            combined_df = pd.concat(all_lineups, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_lineups',
                partition_cols=['league', 'season'],
                replace_partitions=['match_id'],
            )
            results['lineups'] = table_path

        if all_team_match_stats:
            combined_df = pd.concat(all_team_match_stats, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='fbref_team_match_stats',
                partition_cols=['league', 'season'],
                replace_partitions=['match_id'],
            )
            results['team_match_stats'] = table_path

        stats = self.get_stats()
        bytes_total = stats.get('bytes_downloaded', 0)
        pages_total = stats.get('pages_downloaded', 0)
        logger.info(
            f"FBref traffic summary: {pages_total} pages, "
            f"{bytes_total/1024/1024:.1f} MB downloaded"
        )
        for ptype, pbytes in stats.get('bytes_by_page_type', {}).items():
            logger.info(f"  {ptype}: {pbytes/1024/1024:.1f} MB")

        logger.info(f"FBref scrape complete: {list(results.keys())}")
        return results
