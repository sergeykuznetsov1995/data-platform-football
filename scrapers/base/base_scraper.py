"""
Base Scraper
============

Abstract base class for all football data scrapers.
Provides common functionality for rate limiting, retries, circuit breaker,
and Iceberg table writing.
"""

import logging
import os
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.utils.rate_limiter import RateLimiter, get_rate_limiter
from scrapers.utils.retry_policy import RetryPolicy, get_retry_policy
from scrapers.utils.circuit_breaker import CircuitBreaker, get_circuit_breaker, PyBreakerError
from scrapers.utils.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


class ReplaceGuardError(Exception):
    """A ``replace_partitions`` save was refused by the completeness guard.

    Raised by :meth:`BaseScraper.save_to_iceberg` when ``min_replace_ratio`` is
    set and the new frame holds fewer rows (or distinct ``replace_guard_key``
    values) than that share of the existing partition — i.e. the save would
    shrink the partition (silent partial scrape, ``--limit`` smoke run). A
    dedicated type lets callers tell a refused guard apart from a hard write
    failure (#513).
    """


class ScraperConfig:
    """Configuration for scrapers loaded from YAML."""

    def __init__(self, config_path: Optional[str] = None):
        self.config: Dict[str, Any] = {}

        if config_path:
            self.load(config_path)

    def load(self, config_path: str) -> None:
        """Load configuration from YAML file."""
        path = Path(config_path)
        if path.exists():
            with open(path, 'r') as f:
                self.config = yaml.safe_load(f) or {}
            logger.info(f"Loaded config from {config_path}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default

            if value is None:
                return default

        return value


class BaseScraper(ABC):
    """
    Abstract base class for football data scrapers.

    All scrapers should inherit from this class and implement:
    - scrape_all(): Main method to scrape all data
    - Various data-specific methods

    Features:
    - Rate limiting with token bucket algorithm
    - Retry policy with exponential backoff
    - Circuit breaker for failure handling
    - Iceberg table writing
    - Proxy rotation support
    - Metadata tracking
    """

    # Class-level configuration
    SOURCE_NAME: str = 'base'
    DEFAULT_RATE_LIMIT: int = 20  # requests per minute
    DEFAULT_RETRY_ATTEMPTS: int = 3

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        config_path: Optional[str] = None,
        proxy: Optional[str] = None,
        proxy_file: Optional[str] = None,
        no_cache: bool = False,
        rate_limit: Optional[int] = None,
    ):
        """
        Initialize scraper.

        Args:
            leagues: List of leagues to scrape (e.g., ['ENG-Premier League'])
            seasons: List of seasons to scrape (e.g., [2023, 2024])
            config_path: Path to YAML config file
            proxy: Proxy server URL
            proxy_file: Path to file with proxies (format: host:port:user:pass)
            no_cache: Disable caching
            rate_limit: Custom rate limit (requests per minute)
        """
        self.leagues = leagues or []
        self.seasons = seasons or []
        self.proxy = proxy
        self.proxy_file = proxy_file
        self.no_cache = no_cache

        # Load configuration
        self.config = ScraperConfig(config_path)

        # Initialize components
        self._rate_limiter = self._create_rate_limiter(rate_limit)
        self._retry_policy = self._create_retry_policy()
        self._circuit_breaker = self._create_circuit_breaker()
        self._iceberg_writer = IcebergWriter()
        self._proxy_manager: Optional[ProxyManager] = None

        # Initialize proxy manager from file or single proxy URL
        if proxy_file and os.path.exists(proxy_file):
            self._proxy_manager = ProxyManager(rotation_strategy='random')
            count = self._proxy_manager.load_from_file_custom_format(proxy_file)
            logger.info(f"Loaded {count} proxies from {proxy_file}")
            # Pre-validate proxies to filter out dead ones early
            if count > 10:
                stats = self._proxy_manager.validate_proxies(timeout=5.0)
                logger.info(
                    f"Proxy pre-validation: {stats['alive']} alive, "
                    f"{stats['dead']} dead out of {stats['total']}"
                )
        elif proxy:
            self._proxy_manager = ProxyManager()
            self._proxy_manager.add_proxy_url(proxy)

        # Tracking
        self._batch_id = str(uuid.uuid4())
        self._stats = {
            'requests': 0,
            'successes': 0,
            'failures': 0,
            'tables_written': [],
            'bytes_downloaded': 0,       # HTML-only bytes (from _track_download)
            'pages_downloaded': 0,
            'bytes_by_page_type': {},
            # Real proxy traffic (via CDP Network events) — includes all
            # resources actually fetched through proxy, before blocking.
            'real_bytes_downloaded': 0,
            'real_requests_count': 0,
            # Issue #124: curl_cffi HTTP fast-path (FBref _fetch_page_http).
            # Bypasses CDP, so its bytes don't appear in real_bytes_*.
            'http_bytes_downloaded': 0,
            'http_html_bytes_downloaded': 0,
            'http_requests_count': 0,
            'http_bytes_by_resource_type': {},
            'http_requests_by_resource_type': {},
        }

        logger.info(
            f"Initialized {self.SOURCE_NAME} scraper: "
            f"leagues={len(self.leagues)}, seasons={len(self.seasons)}"
        )

    def _create_rate_limiter(self, rate_limit: Optional[int]) -> RateLimiter:
        """Create rate limiter for this scraper."""
        if rate_limit:
            return RateLimiter(max_requests=rate_limit, window_seconds=60)
        return get_rate_limiter(self.SOURCE_NAME)

    def _create_retry_policy(self) -> RetryPolicy:
        """Create retry policy for this scraper."""
        return get_retry_policy('standard')

    def _create_circuit_breaker(self) -> CircuitBreaker:
        """Create circuit breaker for this scraper."""
        return get_circuit_breaker(self.SOURCE_NAME, name=self.SOURCE_NAME)

    def _execute_with_resilience(
        self,
        func,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with rate limiting, retry, and circuit breaker.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            Exception: If all retries exhausted or circuit open
        """
        # Wait for rate limiter
        self._rate_limiter.acquire()

        self._stats['requests'] += 1

        try:
            # Execute through circuit breaker with retry
            result = self._circuit_breaker.call(
                self._retry_policy.execute,
                func,
                *args,
                **kwargs
            )
            self._stats['successes'] += 1
            return result

        except PyBreakerError:
            self._stats['failures'] += 1
            logger.error(
                f"Circuit breaker open for {self.SOURCE_NAME}, "
                f"waiting for recovery"
            )
            raise

        except Exception as e:
            self._stats['failures'] += 1
            logger.error(f"Request failed after retries: {e}")
            raise

    def _add_metadata(
        self,
        df: pd.DataFrame,
        entity_type: str
    ) -> pd.DataFrame:
        """
        Add standard metadata columns to DataFrame.

        Args:
            df: Input DataFrame
            entity_type: Type of entity (e.g., 'schedule', 'player_stats')

        Returns:
            DataFrame with metadata columns
        """
        if df.empty:
            return df

        df = df.copy()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = entity_type
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id

        return df

    def save_to_iceberg(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        database: str = 'bronze',
        replace_partitions: Optional[List[str]] = None,
        min_replace_ratio: Optional[float] = None,
        replace_guard_key: Optional[str] = None,
        natural_keys: Optional[List[str]] = None,
    ) -> str:
        """
        Save DataFrame to Iceberg table in Bronze layer.

        Args:
            df: DataFrame to save
            table_name: Target table name (e.g., 'fbref_schedule')
            partition_cols: Columns to partition by
            database: Target database (default: 'bronze')
            replace_partitions: When set (e.g. ``['league', 'season']``),
                deletes existing rows matching the unique partition-key
                tuples present in ``df`` BEFORE inserting. Gives
                partition-replace semantics instead of plain append.
                Used by schedule writers to prevent INSERT-only duplication.
            min_replace_ratio: Opt-in completeness guard for
                ``replace_partitions`` saves (#513). When set (e.g. ``0.9``),
                the existing partition rows that ``delete_filter`` would delete
                are counted via Trino; if the new frame holds fewer than this
                share the save is refused with :class:`ReplaceGuardError`
                (nothing deleted or written). Guards a partial scrape from
                wiping a good partition (precedent: ClubElo wipe #283/#314).
                Requires ``replace_partitions`` (raises ``ValueError`` without
                it — "guarding" a plain append is meaningless). The count is
                aggregate over all partition tuples in ``df``; exact for a
                single-partition frame.
            replace_guard_key: Column to ``COUNT(DISTINCT ...)`` for the guard
                instead of raw rows. Use when per-key row counts vary (e.g.
                Transfermarkt mv_history/transfers carry variable per-player
                timeline lengths → count distinct ``player_id``). ``None`` →
                ``COUNT(*)``.
            natural_keys: Optional natural key for an incremental Iceberg
                ``MERGE``. Mutually exclusive with ``replace_partitions``.

        Returns:
            Full table identifier
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping save to {table_name}")
            return f"iceberg.{database}.{table_name}"

        if natural_keys and replace_partitions:
            raise ValueError(
                "natural_keys and replace_partitions are mutually exclusive"
            )

        # Convert partition cols to spec format
        partition_spec = None
        if partition_cols:
            partition_spec = [(col, 'identity') for col in partition_cols]

        delete_filter = (
            self._build_partition_delete_filter(df, replace_partitions)
            if replace_partitions else None
        )

        if min_replace_ratio is not None:
            self._enforce_replace_guard(
                df, database, table_name, delete_filter,
                min_replace_ratio, replace_guard_key,
            )

        table_path = self._iceberg_writer.write_dataframe(
            df=df,
            database=database,
            table=table_name,
            partition_spec=partition_spec,
            source=self.SOURCE_NAME,
            delete_filter=delete_filter,
            merge_keys=natural_keys,
        )

        self._stats['tables_written'].append(table_path)
        logger.info(f"Saved {len(df)} rows to {table_path}")

        return table_path

    @staticmethod
    def _build_partition_delete_filter(
        df: pd.DataFrame,
        partition_cols: List[str],
    ) -> str:
        """Build a SQL WHERE clause matching every (partition_cols) tuple
        present in ``df``.

        String values are single-quote-escaped; numeric values stay raw.
        NaN/None partition values are dropped.

        Raises ValueError when ``replace_partitions`` was requested but cannot be
        honoured (missing columns, or no non-NULL partition values in a non-empty
        frame). Previously these silently fell back to a plain append, which
        accumulates duplicates run after run — a footgun, so now it fails loud
        (#314 p.3). ``df`` is guaranteed non-empty here (``save_to_iceberg``
        short-circuits empty frames before calling).
        """
        missing = [c for c in partition_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"replace_partitions={partition_cols} requested but DataFrame is "
                f"missing columns: {missing}. Refusing to fall back to a plain "
                f"append (would accumulate duplicates)."
            )

        unique = df[partition_cols].drop_duplicates().dropna()
        if unique.empty:
            raise ValueError(
                f"replace_partitions={partition_cols} requested but every "
                f"partition-key value is NULL in a non-empty frame. Refusing to "
                f"fall back to a plain append (would accumulate duplicates)."
            )

        clauses = []
        for _, row in unique.iterrows():
            parts = []
            for col in partition_cols:
                val = row[col]
                if isinstance(val, str):
                    safe = val.replace("'", "''")
                    parts.append(f"{col} = '{safe}'")
                else:
                    parts.append(f"{col} = {val}")
            clauses.append('(' + ' AND '.join(parts) + ')')
        return ' OR '.join(clauses)

    def _enforce_replace_guard(
        self,
        df: pd.DataFrame,
        database: str,
        table_name: str,
        delete_filter: Optional[str],
        min_replace_ratio: float,
        replace_guard_key: Optional[str],
    ) -> None:
        """Refuse a replace-partitions save that would shrink the partition.

        Opt-in completeness guard (#513): compares the new frame against the
        existing partition (the rows ``delete_filter`` would delete) and raises
        :class:`ReplaceGuardError` when the new frame holds fewer than
        ``min_replace_ratio`` of the existing count. Skips (warning) when the
        existing count is unavailable — a first run, or an unreachable Trino;
        the save itself needs Trino, so a dead cluster cannot wipe anything.
        ``df`` is guaranteed non-empty (``save_to_iceberg`` short-circuits empty
        frames before calling).
        """
        if not 0.0 < min_replace_ratio <= 1.0:
            raise ValueError(
                f"min_replace_ratio must be in (0, 1], got {min_replace_ratio!r}"
            )
        if delete_filter is None:
            raise ValueError(
                "min_replace_ratio requires replace_partitions; refusing to "
                "'guard' a plain append (it would still accumulate duplicates)."
            )
        if replace_guard_key is not None and replace_guard_key not in df.columns:
            raise ValueError(
                f"replace_guard_key={replace_guard_key!r} not in DataFrame columns"
            )

        existing = self._count_existing_partition(
            database, table_name, delete_filter, replace_guard_key,
        )
        if not existing:  # None (unavailable) or 0 (empty partition / first run)
            logger.warning(
                "Replace guard skipped for %s.%s (existing count unavailable or "
                "zero) — proceeding with save.", database, table_name,
            )
            return

        new = (
            int(df[replace_guard_key].nunique())
            if replace_guard_key else len(df)
        )
        if new < min_replace_ratio * existing:
            unit = f"distinct {replace_guard_key}" if replace_guard_key else "rows"
            raise ReplaceGuardError(
                f"new={new} {unit} < {min_replace_ratio:.0%} of "
                f"existing={existing} for {database}.{table_name} — refusing "
                f"replace_partitions save (would shrink the partition)"
            )

    def _count_existing_partition(
        self,
        database: str,
        table_name: str,
        delete_filter: str,
        key: Optional[str],
    ) -> Optional[int]:
        """COUNT the existing rows ``delete_filter`` would delete, or ``None``.

        Returns ``None`` (guard skips) when the table does not yet exist or
        Trino is unreachable. Reuses the exact ``delete_filter`` so it measures
        the rows ``save_to_iceberg`` is about to DELETE. Counts ``DISTINCT key``
        when ``key`` is given, else raw rows (``COUNT(DISTINCT *)`` is invalid
        Trino SQL, so the branch is required).
        """
        try:
            if not self._iceberg_writer.table_exists(database, table_name):
                return None
            agg = f"count(DISTINCT {key})" if key else "count(*)"
            catalog = self._iceberg_writer.catalog
            sql = (
                f"SELECT {agg} FROM {catalog}.{database}.{table_name} "
                f"WHERE {delete_filter}"
            )
            rows = self._iceberg_writer._get_trino_manager().execute_query(sql)
            if rows and rows[0] and rows[0][0] is not None:
                return int(rows[0][0])
            return None
        except Exception as e:
            logger.warning(
                "Could not count existing partition for %s.%s: %s",
                database, table_name, e,
            )
            return None

    @abstractmethod
    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all data for configured leagues and seasons.

        Returns:
            Dictionary mapping data type to Iceberg table path
            e.g., {'schedule': 'iceberg.bronze.fbref_schedule', ...}
        """
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        bytes_total = self._stats.get('bytes_downloaded', 0)
        if bytes_total >= 1024 * 1024 * 1024:
            traffic_human = f"{bytes_total / 1024 / 1024 / 1024:.2f} GB"
        elif bytes_total >= 1024 * 1024:
            traffic_human = f"{bytes_total / 1024 / 1024:.1f} MB"
        elif bytes_total >= 1024:
            traffic_human = f"{bytes_total / 1024:.1f} KB"
        else:
            traffic_human = f"{bytes_total} B"

        return {
            **self._stats,
            'traffic_human': traffic_human,
            'circuit_breaker_state': self._circuit_breaker.state,
            'rate_limiter_tokens': self._rate_limiter.available_tokens,
        }

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._batch_id = str(uuid.uuid4())
        self._stats = {
            'requests': 0,
            'successes': 0,
            'failures': 0,
            'tables_written': [],
            'bytes_downloaded': 0,
            'pages_downloaded': 0,
            'bytes_by_page_type': {},
        }

    def close(self) -> None:
        """Cleanup resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


class SoccerdataScraper(BaseScraper):
    """
    Base class for scrapers using the soccerdata library.

    Provides common functionality for soccerdata-based sources.
    """

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)

        # #920 Phase 3: install the repo's league_dict fragment BEFORE the
        # lazy `import soccerdata` in _get_reader — soccerdata merges
        # ~/soccerdata/config/league_dict.json exactly once, at import time.
        from scrapers.base.soccerdata_config import ensure_league_dict
        ensure_league_dict(required_leagues=self.leagues)

        # soccerdata specific options
        self._sd_kwargs = {
            'no_cache': self.no_cache,
        }

        if self.proxy:
            self._sd_kwargs['proxy'] = self.proxy

    def _get_reader(self):
        """Get soccerdata reader instance. Override in subclass."""
        raise NotImplementedError("Subclass must implement _get_reader")

    def _safe_call(self, method_name: str, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Safely call a soccerdata reader method.

        Args:
            method_name: Name of the reader method
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            DataFrame or None if error
        """
        reader = self._get_reader()

        if not hasattr(reader, method_name):
            logger.error(f"Reader has no method: {method_name}")
            return None

        method = getattr(reader, method_name)

        try:
            return self._execute_with_resilience(method, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error calling {method_name}: {e}")
            return None


class SeleniumScraper(BaseScraper):
    """
    Base class for scrapers requiring Selenium browser automation.

    Used for sources with Cloudflare protection or JavaScript rendering.

    Uses Selenium with undetected-chromedriver for Cloudflare bypass.
    """

    def __init__(
        self,
        headless: bool = True,
        use_xvfb: bool = False,
        proxy_file: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Selenium scraper.

        Args:
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb for virtual display
            proxy_file: Path to file with proxies
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(proxy_file=proxy_file, **kwargs)

        self.headless = headless
        self.use_xvfb = use_xvfb
        self._browser = None

    def _get_browser(self):
        """Get or create browser instance with proxy support."""
        if self._browser is None:
            from scrapers.base.browser import CloudflareBypass

            # Get proxy URL from manager or direct proxy
            proxy_url = None
            if self._proxy_manager and self._proxy_manager.total_count > 0:
                proxy_obj = self._proxy_manager.get_proxy()
                if proxy_obj:
                    proxy_url = proxy_obj.url
                    logger.info(f"Using proxy: {proxy_obj.host}:{proxy_obj.port}")
            elif self.proxy:
                proxy_url = self.proxy

            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=proxy_url,
            )

        return self._browser

    def close(self) -> None:
        """Cleanup browser."""
        if self._browser is not None:
            self._browser.close()
            self._browser = None

        super().close()
