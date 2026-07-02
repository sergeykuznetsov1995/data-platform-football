"""
MatchHistory Scraper
====================

Scraper for historical match data from football-data.co.uk.
Uses direct HTTP requests for CSV files with Selenium fallback.

Source: https://www.football-data.co.uk/

NOTE: football-data.co.uk sometimes blocks automated requests.
This scraper uses standard requests first, then falls back to
Selenium if that fails.
"""

import json
import logging
import os
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.base.browser import CloudflareBypass

logger = logging.getLogger(__name__)


class _NotModified:
    """Sentinel: server answered 304 — CSV unchanged since the last ingest."""

    def __repr__(self) -> str:
        return '<NOT_MODIFIED>'


# Distinct from None (= fetch failed): a 304 must NOT trigger the Selenium
# fallback and must NOT be reported as a scrape error — it is a clean no-op.
NOT_MODIFIED = _NotModified()

class MatchHistoryScraper(SeleniumScraper):
    """
    Scraper for historical match data from football-data.co.uk.

    Provides:
    - Match results (home/away goals)
    - Half-time scores
    - Betting odds from multiple bookmakers
    - Match statistics (shots, corners, fouls, cards)

    The scraper tries direct HTTP requests first, then falls back
    to Selenium if the server blocks the request.

    Usage:
        scraper = MatchHistoryScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'matchhistory'
    DEFAULT_RATE_LIMIT = 30  # requests per minute

    BASE_URL = 'https://www.football-data.co.uk'

    # League code mapping for football-data.co.uk
    LEAGUE_CODES = {
        'ENG-Premier League': 'E0',
        'ENG-Championship': 'E1',
        'ENG-League 1': 'E2',
        'ENG-League 2': 'E3',
        'ESP-La Liga': 'SP1',
        'ESP-Segunda': 'SP2',
        'GER-Bundesliga': 'D1',
        'GER-2. Bundesliga': 'D2',
        'ITA-Serie A': 'I1',
        'ITA-Serie B': 'I2',
        'FRA-Ligue 1': 'F1',
        'FRA-Ligue 2': 'F2',
        'NED-Eredivisie': 'N1',
        'BEL-Pro League': 'B1',
        'POR-Primeira Liga': 'P1',
        'TUR-Super Lig': 'T1',
        'GRE-Super League': 'G1',
        'SCO-Premiership': 'SC0',
    }

    # Standard column mapping for consistency
    COLUMN_MAPPING = {
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
        'WHH': 'odds_home_wh',
        'WHD': 'odds_draw_wh',
        'WHA': 'odds_away_wh',
        'VCH': 'odds_home_vc',
        'VCD': 'odds_draw_vc',
        'VCA': 'odds_away_vc',
        # Legacy Betbrain aggregates (Bb*): football-data.co.uk dropped these
        # columns around 2019, so they only match when backfilling seasons
        # <= 2018/19. Modern files ship AHh / Max* / Avg* instead — those pass
        # through raw (lowercased) and are read as-is by the silver SQL.
        # Asian handicap odds
        'BbAHh': 'asian_handicap_home',
        'BbAH': 'asian_handicap_line',
        # Over/Under odds
        'BbOU': 'over_under_line',
        'BbMxOver': 'max_over_odds',
        'BbAvOver': 'avg_over_odds',
        'BbMxUnder': 'max_under_odds',
        'BbAvUnder': 'avg_under_odds',
    }

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = True,
        use_xvfb: bool = True,
        force_refresh: bool = False,
        **kwargs
    ):
        """
        Initialize MatchHistory scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            headless: Run browser in headless mode (for Selenium fallback)
            use_xvfb: Use Xvfb virtual display
            force_refresh: Skip conditional-request headers — always
                re-download the CSV even if unchanged (deliberate re-ingest)
            **kwargs: Additional arguments for SeleniumScraper
        """
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            **kwargs
        )
        self.use_xvfb = use_xvfb
        self.force_refresh = force_refresh
        self._session: Optional[requests.Session] = None
        # Conditional-GET validators (ETag/Last-Modified) per URL, persisted on
        # the soccerdata_cache volume so the daily run answers 304 (0 bytes)
        # for an unchanged season CSV instead of re-downloading ~200 KB.
        self._http_meta_path = (
            Path(os.environ.get('SOCCERDATA_DIR', str(Path.home() / 'soccerdata')))
            / 'matchhistory_http_meta.json'
        )
        self._http_meta: Optional[Dict[str, Dict[str, str]]] = None
        # Validators seen this run; merged into the store ONLY after the
        # Iceberg save succeeds (commit_http_meta) — otherwise a failed write
        # (footgun #183) would leave the store claiming data we never landed,
        # and every later run would 304-skip it forever.
        self._pending_http_meta: Dict[str, Dict[str, str]] = {}

    def _get_browser(self) -> CloudflareBypass:
        """Get browser for Selenium fallback."""
        if self._browser is None:
            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=self.proxy,
                page_load_timeout=45,
            )
        return self._browser

    def _get_session(self) -> requests.Session:
        """Get or create requests session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
        return self._session

    def _load_http_meta(self) -> Dict[str, Dict[str, str]]:
        """Load persisted conditional-GET validators (fail-open on any error)."""
        if self._http_meta is None:
            try:
                with open(self._http_meta_path, 'r') as f:
                    self._http_meta = json.load(f)
            except FileNotFoundError:
                self._http_meta = {}
            except Exception as e:
                logger.warning(f"Unreadable HTTP meta store {self._http_meta_path}: {e}")
                self._http_meta = {}
        return self._http_meta

    def commit_http_meta(self) -> None:
        """
        Persist validators collected this run. Call ONLY after the Iceberg
        save succeeded — see the note in __init__.
        """
        if not self._pending_http_meta:
            return
        meta = self._load_http_meta()
        meta.update(self._pending_http_meta)
        try:
            self._http_meta_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._http_meta_path, 'w') as f:
                json.dump(meta, f, indent=0)
            logger.info(
                f"Committed HTTP meta for {len(self._pending_http_meta)} URL(s) "
                f"to {self._http_meta_path}"
            )
        except Exception as e:
            # Fail-open: losing validators only costs a re-download next run.
            logger.warning(f"Could not persist HTTP meta store: {e}")
        self._pending_http_meta = {}

    def _format_season(self, season: int) -> str:
        """
        Format season year to football-data.co.uk format.

        Format: "2425" for 2024-2025 season.

        Args:
            season: Season start year (e.g., 2024 for 2024-2025 season)

        Returns:
            Formatted season string (e.g., "2425")
        """
        start_year = str(season)[-2:]  # Last 2 digits
        end_year = str(season + 1)[-2:]  # Last 2 digits of next year
        return f"{start_year}{end_year}"

    def _get_csv_url(self, league: str, season: int) -> Optional[str]:
        """
        Build URL for CSV file.

        Args:
            league: League name
            season: Season year

        Returns:
            Full URL to CSV file or None if league unknown
        """
        league_code = self.LEAGUE_CODES.get(league)
        if not league_code:
            logger.error(f"Unknown league: {league}")
            return None

        season_str = self._format_season(season)
        return f"{self.BASE_URL}/mmz4281/{season_str}/{league_code}.csv"

    def _fetch_csv_with_requests(self, url: str) -> Union[pd.DataFrame, _NotModified, None]:
        """
        Fetch CSV data using requests library.

        Sends conditional-request headers (If-None-Match/If-Modified-Since)
        when validators from a previous successful ingest are stored;
        football-data.co.uk serves static CSVs and answers 304 with an empty
        body, so an unchanged file costs ~0 bytes.

        Args:
            url: URL to CSV file

        Returns:
            DataFrame with CSV data, NOT_MODIFIED on 304, or None on failure
        """
        session = self._get_session()

        try:
            # Rate limiting
            self._rate_limiter.acquire()

            headers = {}
            if not self.force_refresh:
                known = self._load_http_meta().get(url, {})
                if known.get('etag'):
                    headers['If-None-Match'] = known['etag']
                if known.get('last_modified'):
                    headers['If-Modified-Since'] = known['last_modified']

            response = session.get(url, timeout=30, headers=headers)

            if response.status_code == 304:
                self._stats['successes'] += 1
                logger.info(f"Not modified (304), skipping: {url}")
                return NOT_MODIFIED

            if response.status_code == 200:
                self._stats['successes'] += 1

                # Modern files (season >= 2425) are UTF-8 with a BOM; older
                # ones are latin-1. The server sends no charset, so decode
                # explicitly instead of trusting response.text (which assumes
                # latin-1 and mojibakes accented names in modern files).
                try:
                    text = response.content.decode('utf-8-sig')
                except UnicodeDecodeError:
                    text = response.content.decode('latin-1')

                # Parse CSV
                df = pd.read_csv(
                    StringIO(text),
                    on_bad_lines='skip',
                )

                validators = {}
                if response.headers.get('ETag'):
                    validators['etag'] = response.headers['ETag']
                if response.headers.get('Last-Modified'):
                    validators['last_modified'] = response.headers['Last-Modified']
                if validators:
                    self._pending_http_meta[url] = validators

                return df

            elif response.status_code == 503:
                logger.warning(f"Service unavailable (503): {url}")
                return None

            elif response.status_code == 403:
                logger.warning(f"Forbidden (403): {url}")
                return None

            else:
                logger.warning(f"Unexpected status {response.status_code}: {url}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            return None

    def _fetch_csv_with_selenium(self, url: str) -> Optional[pd.DataFrame]:
        """
        Fetch CSV data using Selenium as fallback.

        Args:
            url: URL to CSV file

        Returns:
            DataFrame with CSV data or None
        """
        logger.info(f"Using Selenium fallback for: {url}")

        try:
            browser = self._get_browser()

            # Navigate to page
            browser.get_page(
                url,
                wait_timeout=20,
                cloudflare_wait=5.0,
            )

            # Wait for content
            time.sleep(2)

            # Get page source (CSV content)
            content = browser.page_source

            # Extract CSV from HTML if wrapped
            if '<pre>' in content:
                import re
                match = re.search(r'<pre[^>]*>(.*?)</pre>', content, re.DOTALL)
                if match:
                    content = match.group(1)

            # Clean HTML entities if present
            if '&' in content:
                import html
                content = html.unescape(content)

            # Parse CSV
            df = pd.read_csv(
                StringIO(content),
                encoding='utf-8',
                on_bad_lines='skip',
            )

            self._stats['successes'] += 1
            return df

        except Exception as e:
            logger.error(f"Selenium fetch error: {e}")
            self._stats['failures'] += 1
            return None

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names for consistency.

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with standardized column names
        """
        # football-data.co.uk CSV ships a UTF-8 BOM on the first header. requests
        # decodes the body as latin-1, so the BOM bytes EF BB BF surface as the
        # literal chars '\xef\xbb\xbf' (not a single '﻿'); strip both forms so
        # 'Div' is not stored as the artifact column 'ï»¿div' (issue #309).
        df = df.rename(columns=self._strip_bom)

        # Rename columns that exist
        rename_cols = {k: v for k, v in self.COLUMN_MAPPING.items() if k in df.columns}
        df = df.rename(columns=rename_cols)

        return df

    @staticmethod
    def _strip_bom(col):
        """Strip a leading UTF-8 BOM (latin-1 mojibake or single-char form)."""
        if isinstance(col, str):
            for bom in ('﻿', '\xef\xbb\xbf'):
                if col.startswith(bom):
                    return col[len(bom):]
        return col

    def read_games(
        self,
        league: str = None,
        season: int = None
    ) -> Union[pd.DataFrame, _NotModified, None]:
        """
        Read match results and statistics.

        Args:
            league: League name (uses first configured if not specified)
            season: Season year (uses first configured if not specified)

        Returns:
            DataFrame with match data, NOT_MODIFIED if the season CSV is
            unchanged since the last ingest (clean no-op), or None on failure
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        url = self._get_csv_url(league, season)
        if not url:
            return None

        logger.info(f"Fetching MatchHistory data: {url}")

        # Try requests first
        df = self._fetch_csv_with_requests(url)

        # Unchanged CSV: clean no-op for this (league, season) — the existing
        # partition already holds this data. Not a failure: no Selenium.
        if df is NOT_MODIFIED:
            logger.info(f"CSV not modified for {league} {season} — skipping")
            return NOT_MODIFIED

        # Fallback to Selenium if requests failed
        if df is None:
            logger.info("Requests failed, trying Selenium fallback")
            df = self._fetch_csv_with_selenium(url)

        if df is None or df.empty:
            self._stats['failures'] += 1
            logger.warning(f"No data found for {league} {season}")
            return None

        # Standardize columns
        df = self._standardize_columns(df)

        # Add league and season info
        df['league'] = league
        df['season'] = season

        # Add metadata
        df = self._add_metadata(df, 'match_results')

        logger.info(f"Parsed {len(df)} match entries")
        return df

    def calculate_odds_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate average odds and implied probabilities.

        Args:
            df: DataFrame with odds columns

        Returns:
            DataFrame with additional odds statistics
        """
        df = df.copy()

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

        # Calculate overround (bookmaker margin)
        if all(col + '_avg' in df.columns for col in ['odds_home', 'odds_draw', 'odds_away']):
            df['overround'] = (
                df['prob_home_implied'] +
                df['prob_draw_implied'] +
                df['prob_away_implied']
            )

        return df

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all match history data for configured leagues and seasons.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting MatchHistory scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        all_matches = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    df = self.read_games(league, season)

                    if df is NOT_MODIFIED:
                        continue

                    if df is not None and not df.empty:
                        # Calculate odds statistics
                        df = self.calculate_odds_stats(df)
                        all_matches.append(df)

                    # Rate limit pause between requests
                    time.sleep(2)

                except Exception as e:
                    logger.error(f"Error scraping {league} {season}: {e}")
                    continue

        # Save to Iceberg tables
        if all_matches:
            combined_df = pd.concat(all_matches, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='matchhistory_results',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            results['match_results'] = table_path
            # Data landed — safe to remember the validators for 304 skips.
            self.commit_http_meta()

        logger.info(f"MatchHistory scrape complete: {list(results.keys())}")
        return results

    def close(self) -> None:
        """Cleanup resources."""
        if self._session:
            self._session.close()
            self._session = None

        super().close()
