"""
Base Scraper
============

Abstract base class for all football data scrapers.
Provides common functionality for rate limiting, retries, circuit breaker,
and Iceberg table writing.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
import yaml

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.utils.rate_limiter import RateLimiter, get_rate_limiter
from scrapers.utils.retry_policy import RetryPolicy, get_retry_policy
from scrapers.utils.circuit_breaker import CircuitBreaker, get_circuit_breaker, PyBreakerError
from scrapers.utils.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


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
            no_cache: Disable caching
            rate_limit: Custom rate limit (requests per minute)
        """
        self.leagues = leagues or []
        self.seasons = seasons or []
        self.proxy = proxy
        self.no_cache = no_cache

        # Load configuration
        self.config = ScraperConfig(config_path)

        # Initialize components
        self._rate_limiter = self._create_rate_limiter(rate_limit)
        self._retry_policy = self._create_retry_policy()
        self._circuit_breaker = self._create_circuit_breaker()
        self._iceberg_writer = IcebergWriter()
        self._proxy_manager: Optional[ProxyManager] = None

        if proxy:
            self._proxy_manager = ProxyManager()
            self._proxy_manager.add_proxy_url(proxy)

        # Tracking
        self._batch_id = str(uuid.uuid4())
        self._stats = {
            'requests': 0,
            'successes': 0,
            'failures': 0,
            'tables_written': [],
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
    ) -> str:
        """
        Save DataFrame to Iceberg table in Bronze layer.

        Args:
            df: DataFrame to save
            table_name: Target table name (e.g., 'fbref_schedule')
            partition_cols: Columns to partition by
            database: Target database (default: 'bronze')

        Returns:
            Full table identifier
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping save to {table_name}")
            return f"iceberg.{database}.{table_name}"

        # Convert partition cols to spec format
        partition_spec = None
        if partition_cols:
            partition_spec = [(col, 'identity') for col in partition_cols]

        table_path = self._iceberg_writer.write_dataframe(
            df=df,
            database=database,
            table=table_name,
            partition_spec=partition_spec,
            source=self.SOURCE_NAME,
        )

        self._stats['tables_written'].append(table_path)
        logger.info(f"Saved {len(df)} rows to {table_path}")

        return table_path

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
        return {
            **self._stats,
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
    """

    def __init__(
        self,
        headless: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.headless = headless
        self._browser = None

    def _get_browser(self):
        """Get or create browser instance."""
        if self._browser is None:
            from scrapers.base.cloudflare_bypass import CloudflareBypass

            self._browser = CloudflareBypass(
                headless=self.headless,
                proxy=self.proxy,
            )

        return self._browser

    def close(self) -> None:
        """Cleanup browser."""
        if self._browser is not None:
            self._browser.close()
            self._browser = None

        super().close()
