"""
Proxy Manager
=============

Manages proxy rotation for web scraping.
Supports HTTP proxies, SOCKS proxies, and Tor network.
"""

import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ProxyType(Enum):
    """Proxy types supported."""
    HTTP = 'http'
    HTTPS = 'https'
    SOCKS4 = 'socks4'
    SOCKS5 = 'socks5'
    TOR = 'tor'


@dataclass
class Proxy:
    """Proxy configuration."""
    host: str
    port: int
    proxy_type: ProxyType = ProxyType.HTTP
    username: Optional[str] = None
    password: Optional[str] = None

    # Statistics
    success_count: int = 0
    failure_count: int = 0
    last_used: float = 0.0
    is_banned: bool = False

    @property
    def url(self) -> str:
        """Get proxy URL."""
        auth = ''
        if self.username and self.password:
            auth = f'{self.username}:{self.password}@'

        protocol = self.proxy_type.value
        if self.proxy_type == ProxyType.TOR:
            protocol = 'socks5'

        return f'{protocol}://{auth}{self.host}:{self.port}'

    @property
    def requests_proxies(self) -> Dict[str, str]:
        """Get proxy dict for requests library."""
        return {
            'http': self.url,
            'https': self.url,
        }

    @property
    def selenium_args(self) -> List[str]:
        """Get proxy args for Selenium."""
        return [f'--proxy-server={self.url}']

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 1.0
        return self.success_count / total

    def record_success(self) -> None:
        """Record successful request."""
        self.success_count += 1
        self.last_used = time.time()
        self.is_banned = False

    def record_failure(self) -> None:
        """Record failed request."""
        self.failure_count += 1
        self.last_used = time.time()

    def mark_banned(self) -> None:
        """Mark proxy as banned."""
        self.is_banned = True
        logger.warning(f"Proxy {self.host}:{self.port} marked as banned")


@dataclass
class ProxyManagerConfig:
    """Configuration for proxy manager."""
    rotation_strategy: str = 'round_robin'  # round_robin, random, weighted
    min_success_rate: float = 0.5
    ban_threshold: int = 5  # Consecutive failures to ban
    cooldown_seconds: float = 60.0  # Min time between proxy uses
    use_tor: bool = False
    tor_control_port: int = 9051
    tor_socks_port: int = 9050


class ProxyManager:
    """
    Manages a pool of proxies with rotation and health tracking.

    Usage:
        manager = ProxyManager()
        manager.add_proxy('proxy1.example.com', 8080)
        manager.add_proxy('proxy2.example.com', 8080)

        proxy = manager.get_proxy()
        # Make request
        manager.record_result(proxy, success=True)
    """

    def __init__(
        self,
        rotation_strategy: str = 'round_robin',
        min_success_rate: float = 0.5,
        use_tor: bool = False,
    ):
        """
        Initialize proxy manager.

        Args:
            rotation_strategy: How to select proxies ('round_robin', 'random', 'weighted')
            min_success_rate: Minimum success rate before banning proxy
            use_tor: Whether to use Tor network
        """
        self.config = ProxyManagerConfig(
            rotation_strategy=rotation_strategy,
            min_success_rate=min_success_rate,
            use_tor=use_tor,
        )

        self._proxies: List[Proxy] = []
        self._current_index = 0
        self._consecutive_failures: Dict[str, int] = {}

        if use_tor:
            self._setup_tor()

    def _setup_tor(self) -> None:
        """Set up Tor proxy."""
        tor_proxy = Proxy(
            host='127.0.0.1',
            port=self.config.tor_socks_port,
            proxy_type=ProxyType.TOR,
        )
        self._proxies.append(tor_proxy)
        logger.info("Tor proxy configured")

    def add_proxy(
        self,
        host: str,
        port: int,
        proxy_type: ProxyType = ProxyType.HTTP,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        Add a proxy to the pool.

        Args:
            host: Proxy host
            port: Proxy port
            proxy_type: Type of proxy
            username: Optional auth username
            password: Optional auth password
        """
        proxy = Proxy(
            host=host,
            port=port,
            proxy_type=proxy_type,
            username=username,
            password=password,
        )
        self._proxies.append(proxy)
        logger.info(f"Added proxy {host}:{port}")

    def add_proxy_url(self, url: str) -> None:
        """
        Add a proxy from URL string.

        Args:
            url: Proxy URL (e.g., 'http://user:pass@host:port')
        """
        parsed = urlparse(url)

        proxy_type = ProxyType.HTTP
        if parsed.scheme == 'https':
            proxy_type = ProxyType.HTTPS
        elif parsed.scheme == 'socks4':
            proxy_type = ProxyType.SOCKS4
        elif parsed.scheme == 'socks5':
            proxy_type = ProxyType.SOCKS5

        self.add_proxy(
            host=parsed.hostname or '',
            port=parsed.port or 8080,
            proxy_type=proxy_type,
            username=parsed.username,
            password=parsed.password,
        )

    def load_from_file(self, filepath: str) -> int:
        """
        Load proxies from file (one URL per line).

        Args:
            filepath: Path to proxy file

        Returns:
            Number of proxies loaded
        """
        count = 0
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        self.add_proxy_url(line)
                        count += 1
                    except Exception as e:
                        logger.warning(f"Failed to parse proxy: {line}: {e}")

        logger.info(f"Loaded {count} proxies from {filepath}")
        return count

    def get_proxy(self) -> Optional[Proxy]:
        """
        Get next available proxy based on rotation strategy.

        Returns:
            Proxy instance or None if no proxies available
        """
        available = [p for p in self._proxies if not p.is_banned]

        if not available:
            logger.warning("No available proxies")
            return None

        if self.config.rotation_strategy == 'random':
            return random.choice(available)

        elif self.config.rotation_strategy == 'weighted':
            # Weight by success rate
            weights = [p.success_rate for p in available]
            return random.choices(available, weights=weights, k=1)[0]

        else:  # round_robin
            self._current_index = self._current_index % len(available)
            proxy = available[self._current_index]
            self._current_index += 1
            return proxy

    def record_result(self, proxy: Proxy, success: bool) -> None:
        """
        Record request result for a proxy.

        Args:
            proxy: Proxy used
            success: Whether request was successful
        """
        proxy_key = f"{proxy.host}:{proxy.port}"

        if success:
            proxy.record_success()
            self._consecutive_failures[proxy_key] = 0
        else:
            proxy.record_failure()
            self._consecutive_failures[proxy_key] = (
                self._consecutive_failures.get(proxy_key, 0) + 1
            )

            # Check if should ban
            if self._consecutive_failures[proxy_key] >= self.config.ban_threshold:
                proxy.mark_banned()
            elif proxy.success_rate < self.config.min_success_rate:
                proxy.mark_banned()

    def unban_all(self) -> None:
        """Unban all proxies."""
        for proxy in self._proxies:
            proxy.is_banned = False
        self._consecutive_failures.clear()
        logger.info("All proxies unbanned")

    @property
    def available_count(self) -> int:
        """Get count of available (non-banned) proxies."""
        return sum(1 for p in self._proxies if not p.is_banned)

    @property
    def total_count(self) -> int:
        """Get total proxy count."""
        return len(self._proxies)

    def get_stats(self) -> Dict[str, Any]:
        """Get proxy pool statistics."""
        return {
            'total': self.total_count,
            'available': self.available_count,
            'banned': self.total_count - self.available_count,
            'proxies': [
                {
                    'host': p.host,
                    'port': p.port,
                    'success_rate': p.success_rate,
                    'is_banned': p.is_banned,
                }
                for p in self._proxies
            ]
        }

    def rotate_tor_identity(self) -> bool:
        """
        Rotate Tor identity (get new exit node).

        Returns:
            True if successful
        """
        if not self.config.use_tor:
            return False

        try:
            from stem import Signal
            from stem.control import Controller

            with Controller.from_port(port=self.config.tor_control_port) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                logger.info("Tor identity rotated")
                return True
        except ImportError:
            logger.warning("stem library not installed, cannot rotate Tor identity")
            return False
        except Exception as e:
            logger.error(f"Failed to rotate Tor identity: {e}")
            return False


# Convenience function to create proxy manager
def create_proxy_manager(
    proxy_urls: Optional[List[str]] = None,
    use_tor: bool = False,
    rotation_strategy: str = 'round_robin',
) -> ProxyManager:
    """
    Create and configure a proxy manager.

    Args:
        proxy_urls: List of proxy URLs to add
        use_tor: Whether to use Tor
        rotation_strategy: Rotation strategy

    Returns:
        Configured ProxyManager
    """
    manager = ProxyManager(
        rotation_strategy=rotation_strategy,
        use_tor=use_tor,
    )

    if proxy_urls:
        for url in proxy_urls:
            manager.add_proxy_url(url)

    return manager
