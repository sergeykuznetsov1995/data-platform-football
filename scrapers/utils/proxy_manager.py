"""
Proxy Manager
=============

Manages proxy rotation for web scraping.
Supports HTTP proxies, SOCKS proxies, and Tor network.

Features:
- Cooldown between proxy uses to avoid detection
- Detailed statistics by error type (rate_limit, forbidden, cloudflare)
- Success rate tracking per proxy
- Automatic proxy banning after consecutive failures
"""

import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Types of errors for proxy statistics."""
    RATE_LIMIT = 'rate_limit'      # 429 Too Many Requests
    FORBIDDEN = 'forbidden'         # 403 Forbidden
    CLOUDFLARE = 'cloudflare'       # Cloudflare challenge
    TIMEOUT = 'timeout'             # Request timeout
    CONNECTION = 'connection'       # Connection error
    UNKNOWN = 'unknown'             # Other errors


class ProxyType(Enum):
    """Proxy types supported."""
    HTTP = 'http'
    HTTPS = 'https'
    SOCKS4 = 'socks4'
    SOCKS5 = 'socks5'
    TOR = 'tor'


@dataclass
class Proxy:
    """Proxy configuration with detailed statistics."""
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

    # Detailed error statistics
    error_counts: Dict[str, int] = field(default_factory=dict)
    # Response time tracking
    response_times: List[float] = field(default_factory=list)
    max_response_times: int = 100  # Keep last N response times

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

    def record_failure(self, error_type: str = 'unknown') -> None:
        """
        Record failed request with error type.

        Args:
            error_type: Type of error (rate_limit, forbidden, cloudflare, etc.)
        """
        self.failure_count += 1
        self.last_used = time.time()
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def record_response_time(self, response_time: float) -> None:
        """
        Record response time for performance tracking.

        Args:
            response_time: Response time in seconds
        """
        self.response_times.append(response_time)
        # Keep only the last N response times
        if len(self.response_times) > self.max_response_times:
            self.response_times = self.response_times[-self.max_response_times:]

    @property
    def avg_response_time(self) -> float:
        """Calculate average response time."""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    def mark_banned(self) -> None:
        """Mark proxy as banned."""
        self.is_banned = True
        logger.warning(f"Proxy {self.host}:{self.port} marked as banned")

    def get_error_summary(self) -> str:
        """Get summary of errors for this proxy."""
        if not self.error_counts:
            return "no errors"
        return ", ".join(f"{k}:{v}" for k, v in self.error_counts.items())


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

    # Additional settings for FBref scraping
    cloudflare_ban_threshold: int = 2  # Ban faster for Cloudflare blocks
    rate_limit_cooldown_multiplier: float = 2.0  # Longer cooldown after rate limit


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
        cooldown_seconds: float = 60.0,
    ):
        """
        Initialize proxy manager.

        Args:
            rotation_strategy: How to select proxies ('round_robin', 'random', 'weighted')
            min_success_rate: Minimum success rate before banning proxy
            use_tor: Whether to use Tor network
            cooldown_seconds: Minimum time between uses of the same proxy
        """
        self.config = ProxyManagerConfig(
            rotation_strategy=rotation_strategy,
            min_success_rate=min_success_rate,
            use_tor=use_tor,
            cooldown_seconds=cooldown_seconds,
        )

        self._proxies: List[Proxy] = []
        self._current_index = 0
        self._consecutive_failures: Dict[str, int] = {}
        self._error_type_counts: Dict[str, Dict[str, int]] = {}  # proxy_key -> error_type -> count

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
        logger.debug(f"Added proxy {host}:{port}")

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

    def load_from_file_custom_format(
        self,
        filepath: str,
        proxy_type: ProxyType = ProxyType.HTTP
    ) -> int:
        """
        Load proxies from file with host:port:user:pass format.

        This format is commonly used by residential proxy providers.

        Args:
            filepath: Path to proxy file
            proxy_type: Type of proxy (default: HTTP)

        Returns:
            Number of proxies loaded
        """
        count = 0
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                parts = line.split(':')
                if len(parts) >= 4:
                    host = parts[0]
                    port = parts[1]
                    username = parts[2]
                    # Password may contain colons, join remaining parts
                    password = ':'.join(parts[3:])

                    try:
                        self.add_proxy(
                            host=host,
                            port=int(port),
                            proxy_type=proxy_type,
                            username=username,
                            password=password
                        )
                        count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse proxy line: {line}: {e}")
                elif len(parts) == 2:
                    # Simple host:port format without auth
                    try:
                        self.add_proxy(
                            host=parts[0],
                            port=int(parts[1]),
                            proxy_type=proxy_type
                        )
                        count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse proxy line: {line}: {e}")
                else:
                    logger.warning(f"Invalid proxy format: {line}")

        logger.info(f"Loaded {count} proxies from {filepath} (custom format)")
        return count

    def get_proxy(self, respect_cooldown: bool = True) -> Optional[Proxy]:
        """
        Get next available proxy based on rotation strategy.

        Args:
            respect_cooldown: Whether to respect cooldown period between uses

        Returns:
            Proxy instance or None if no proxies available
        """
        available = [p for p in self._proxies if not p.is_banned]

        if not available:
            logger.warning("No available proxies")
            return None

        now = time.time()

        # Filter by cooldown if enabled
        if respect_cooldown and self.config.cooldown_seconds > 0:
            cooled_down = [
                p for p in available
                if now - p.last_used >= self.config.cooldown_seconds
            ]
            if cooled_down:
                available = cooled_down
            else:
                # All proxies in cooldown - find one with shortest remaining cooldown
                logger.debug(
                    f"All {len(available)} proxies in cooldown, "
                    f"selecting one with shortest wait"
                )
                available = sorted(available, key=lambda p: p.last_used)

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

    def record_result(
        self,
        proxy: Proxy,
        success: bool,
        error_type: Optional[str] = None,
        response_time: Optional[float] = None,
    ) -> None:
        """
        Record request result for a proxy.

        Args:
            proxy: Proxy used
            success: Whether request was successful
            error_type: Type of error if failed (rate_limit, forbidden, cloudflare, etc.)
            response_time: Response time in seconds
        """
        proxy_key = f"{proxy.host}:{proxy.port}"

        # Record response time if provided
        if response_time is not None:
            proxy.record_response_time(response_time)

        if success:
            proxy.record_success()
            self._consecutive_failures[proxy_key] = 0
        else:
            error_type = error_type or 'unknown'
            proxy.record_failure(error_type)
            self._consecutive_failures[proxy_key] = (
                self._consecutive_failures.get(proxy_key, 0) + 1
            )

            # Track error types per proxy
            if proxy_key not in self._error_type_counts:
                self._error_type_counts[proxy_key] = {}
            self._error_type_counts[proxy_key][error_type] = (
                self._error_type_counts[proxy_key].get(error_type, 0) + 1
            )

            # Ban faster for Cloudflare blocks
            cloudflare_failures = proxy.error_counts.get('cloudflare', 0)
            if cloudflare_failures >= self.config.cloudflare_ban_threshold:
                logger.warning(
                    f"Proxy {proxy_key} banned due to {cloudflare_failures} Cloudflare blocks"
                )
                proxy.mark_banned()
                return

            # Check if should ban based on consecutive failures
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

    def get_http_proxy_url(self) -> Optional[str]:
        """
        Get HTTP proxy URL for requests/soccerdata libraries.

        Returns proxy in format: http://user:pass@host:port
        This format is compatible with requests library and soccerdata.

        Returns:
            HTTP proxy URL string or None if no proxies available
        """
        proxy = self.get_proxy()
        if proxy:
            if proxy.username and proxy.password:
                return f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            return f"http://{proxy.host}:{proxy.port}"
        return None

    def get_nodriver_proxy_string(self) -> Optional[str]:
        """
        Get proxy string in nodriver format: host:port:user:pass

        This format is used by NodriverBypass._parse_proxy() method.

        Returns:
            Proxy string in format host:port:user:pass or host:port if no auth
        """
        proxy = self.get_proxy()
        if proxy:
            if proxy.username and proxy.password:
                return f"{proxy.host}:{proxy.port}:{proxy.username}:{proxy.password}"
            return f"{proxy.host}:{proxy.port}"
        return None

    def get_nodriver_proxy_dict(self) -> Optional[Dict[str, Any]]:
        """
        Get proxy as dictionary for nodriver.

        Returns:
            Dict with keys: host, port, username, password (optional)
            Returns None if no proxies available
        """
        proxy = self.get_proxy()
        if proxy:
            result = {
                'host': proxy.host,
                'port': proxy.port,
            }
            if proxy.username and proxy.password:
                result['username'] = proxy.username
                result['password'] = proxy.password
            return result
        return None

    def get_current_proxy(self) -> Optional[Proxy]:
        """
        Get current proxy without rotating to next one.

        Useful for sticky sessions where you need the same proxy
        for multiple requests.

        Returns:
            Current Proxy instance or None if no proxies available
        """
        available = [p for p in self._proxies if not p.is_banned]
        if not available:
            return None

        if self.config.rotation_strategy == 'round_robin':
            idx = (self._current_index - 1) % len(available)
            return available[idx] if available else None
        return available[0] if available else None

    @property
    def available_count(self) -> int:
        """Get count of available (non-banned) proxies."""
        return sum(1 for p in self._proxies if not p.is_banned)

    @property
    def total_count(self) -> int:
        """Get total proxy count."""
        return len(self._proxies)

    def get_stats(self) -> Dict[str, Any]:
        """Get proxy pool statistics with detailed metrics."""
        now = time.time()
        return {
            'total': self.total_count,
            'available': self.available_count,
            'banned': self.total_count - self.available_count,
            'in_cooldown': sum(
                1 for p in self._proxies
                if not p.is_banned and now - p.last_used < self.config.cooldown_seconds
            ),
            'error_type_totals': self._get_error_type_totals(),
            'proxies': [
                {
                    'host': p.host,
                    'port': p.port,
                    'success_rate': round(p.success_rate, 3),
                    'is_banned': p.is_banned,
                    'success_count': p.success_count,
                    'failure_count': p.failure_count,
                    'avg_response_time': round(p.avg_response_time, 2),
                    'error_counts': p.error_counts,
                    'seconds_since_use': round(now - p.last_used, 1) if p.last_used > 0 else None,
                }
                for p in self._proxies
            ]
        }

    def _get_error_type_totals(self) -> Dict[str, int]:
        """Get total error counts by type across all proxies."""
        totals: Dict[str, int] = {}
        for proxy in self._proxies:
            for error_type, count in proxy.error_counts.items():
                totals[error_type] = totals.get(error_type, 0) + count
        return totals

    def get_best_proxies(self, limit: int = 10) -> List[Proxy]:
        """
        Get top performing proxies by success rate.

        Args:
            limit: Maximum number of proxies to return

        Returns:
            List of best performing proxies
        """
        available = [p for p in self._proxies if not p.is_banned]
        sorted_proxies = sorted(
            available,
            key=lambda p: (p.success_rate, -p.avg_response_time),
            reverse=True
        )
        return sorted_proxies[:limit]

    def get_cooldown_status(self) -> Dict[str, float]:
        """
        Get cooldown status for all proxies.

        Returns:
            Dict mapping proxy key to seconds remaining in cooldown (0 if available)
        """
        now = time.time()
        status = {}
        for proxy in self._proxies:
            if proxy.is_banned:
                continue
            remaining = max(0, self.config.cooldown_seconds - (now - proxy.last_used))
            proxy_key = f"{proxy.host}:{proxy.port}"
            status[proxy_key] = round(remaining, 1)
        return status

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


def classify_error(error_str: str) -> str:
    """
    Classify error string into error type.

    Args:
        error_str: Error message string

    Returns:
        Error type: 'rate_limit', 'forbidden', 'cloudflare', 'timeout', 'connection', 'unknown'
    """
    error_lower = error_str.lower()

    # Rate limiting
    if '429' in error_lower or 'rate limit' in error_lower or 'too many' in error_lower:
        return ErrorType.RATE_LIMIT.value

    # Forbidden/blocked
    if '403' in error_lower or 'forbidden' in error_lower or 'access denied' in error_lower:
        return ErrorType.FORBIDDEN.value

    # Cloudflare challenge
    if any(x in error_lower for x in [
        'cloudflare', 'challenge', 'captcha', 'turnstile',
        'checking your browser', 'just a moment'
    ]):
        return ErrorType.CLOUDFLARE.value

    # Timeout
    if 'timeout' in error_lower or 'timed out' in error_lower:
        return ErrorType.TIMEOUT.value

    # Connection errors
    if any(x in error_lower for x in [
        'connection', 'refused', 'reset', 'network', 'unreachable'
    ]):
        return ErrorType.CONNECTION.value

    return ErrorType.UNKNOWN.value


# Convenience function to create proxy manager
def create_proxy_manager(
    proxy_urls: Optional[List[str]] = None,
    use_tor: bool = False,
    rotation_strategy: str = 'round_robin',
    cooldown_seconds: float = 60.0,
) -> ProxyManager:
    """
    Create and configure a proxy manager.

    Args:
        proxy_urls: List of proxy URLs to add
        use_tor: Whether to use Tor
        rotation_strategy: Rotation strategy
        cooldown_seconds: Minimum time between uses of the same proxy

    Returns:
        Configured ProxyManager
    """
    manager = ProxyManager(
        rotation_strategy=rotation_strategy,
        use_tor=use_tor,
        cooldown_seconds=cooldown_seconds,
    )

    if proxy_urls:
        for url in proxy_urls:
            manager.add_proxy_url(url)

    return manager
