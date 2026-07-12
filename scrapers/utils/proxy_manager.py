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
- Pre-validation via TCP connect test (filter dead proxies at load time)
"""

import logging
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    banned_at: Optional[float] = None  # time.time() when banned; None if not banned

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
    def masked_url(self) -> str:
        """Get proxy URL with credentials masked for safe logging."""
        auth = ''
        if self.username and self.password:
            auth = '****:****@'

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
        self.banned_at = None

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
        self.banned_at = time.time()
        logger.warning(f"Proxy {self.masked_url} marked as banned")

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
    unban_cooldown_seconds: float = 600.0  # Auto-return a banned proxy to the pool after N seconds (0 = never)
    use_tor: bool = False
    tor_control_port: int = 9051
    tor_socks_port: int = 9050

    # Additional settings for FBref scraping
    cloudflare_ban_threshold: int = 2  # Ban faster for Cloudflare blocks
    timeout_ban_threshold: int = 1  # Ban after first timeout — dead proxies should not linger in rotation
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
        unban_cooldown_seconds: float = 600.0,
    ):
        """
        Initialize proxy manager.

        Args:
            rotation_strategy: How to select proxies ('round_robin', 'random', 'weighted')
            min_success_rate: Minimum success rate before banning proxy
            use_tor: Whether to use Tor network
            cooldown_seconds: Minimum time between uses of the same proxy
            unban_cooldown_seconds: Auto-return a banned proxy to the pool after
                this many seconds without a manual unban_all() (0 = never)
        """
        self.config = ProxyManagerConfig(
            rotation_strategy=rotation_strategy,
            min_success_rate=min_success_rate,
            use_tor=use_tor,
            cooldown_seconds=cooldown_seconds,
            unban_cooldown_seconds=unban_cooldown_seconds,
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
                        logger.warning(f"Failed to parse proxy URL: {e}")

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
                        logger.warning(f"Failed to parse proxy line: {parts[0]}:{parts[1]}:****: {e}")
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
                        logger.warning(f"Failed to parse proxy line: {parts[0]}:{parts[1]}: {e}")
                else:
                    logger.warning("Invalid proxy format (unexpected field count)")

        logger.info(f"Loaded {count} proxies from {filepath} (custom format)")
        return count

    def validate_proxies(
        self,
        timeout: float = 5.0,
        max_workers: int = 50,
        ban_failed: bool = True,
    ) -> Dict[str, int]:
        """
        Pre-validate all proxies with a fast TCP connect test.

        Tests each proxy by opening a TCP socket to host:port.
        Dead proxies (connection refused/timeout) are banned immediately
        so they won't be used during scraping.

        Args:
            timeout: TCP connect timeout in seconds (default 5s).
            max_workers: Max parallel validation threads (default 50).
            ban_failed: Whether to ban proxies that fail validation.

        Returns:
            Dict with counts: {'alive': N, 'dead': N, 'total': N}
        """
        if not self._proxies:
            return {'alive': 0, 'dead': 0, 'total': 0}

        def _test_proxy(proxy: Proxy) -> bool:
            """Test if proxy accepts TCP connections."""
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect((proxy.host, proxy.port))
                sock.close()
                return True
            except (socket.timeout, ConnectionRefusedError, OSError):
                return False

        alive = 0
        dead = 0
        total = len(self._proxies)

        logger.info(f"Validating {total} proxies (TCP connect, timeout={timeout}s)...")

        with ThreadPoolExecutor(max_workers=min(max_workers, total)) as executor:
            future_to_proxy = {
                executor.submit(_test_proxy, proxy): proxy
                for proxy in self._proxies
            }
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    is_alive = future.result()
                    if is_alive:
                        alive += 1
                    else:
                        dead += 1
                        if ban_failed:
                            proxy.mark_banned()
                except Exception:
                    dead += 1
                    if ban_failed:
                        proxy.mark_banned()

        logger.info(
            f"Proxy validation complete: {alive} alive, {dead} dead "
            f"out of {total} total"
        )
        return {'alive': alive, 'dead': dead, 'total': total}

    def _reactivate_expired_bans(self) -> None:
        """Auto-unban proxies whose ban cooldown has elapsed (#552).

        Lazy per-proxy recovery: a banned proxy returns to the pool once
        ``unban_cooldown_seconds`` have passed since it was banned, without a
        manual ``unban_all()``. Mirrors ``unban_all`` semantics (clears the
        consecutive-failure counter) but keeps cumulative ``error_counts`` so a
        genuinely dead proxy re-bans on its first probe while a victim of a
        transient site outage gets a fresh ``ban_threshold`` budget.
        """
        cooldown = self.config.unban_cooldown_seconds
        if cooldown <= 0:
            return
        now = time.time()
        for proxy in self._proxies:
            if (
                proxy.is_banned
                and proxy.banned_at is not None
                and now - proxy.banned_at >= cooldown
            ):
                proxy.is_banned = False
                proxy.banned_at = None
                self._consecutive_failures[f"{proxy.host}:{proxy.port}"] = 0
                logger.info(
                    f"Proxy {proxy.masked_url} auto-unbanned after "
                    f"{cooldown:.0f}s cooldown"
                )

    def get_proxy(self, respect_cooldown: bool = True) -> Optional[Proxy]:
        """
        Get next available proxy based on rotation strategy.

        Args:
            respect_cooldown: Whether to respect cooldown period between uses

        Returns:
            Proxy instance or None if no proxies available
        """
        self._reactivate_expired_bans()
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

            # Ban immediately on timeout — dead proxies waste ~10s each and
            # round-robin keeps picking them until ban_threshold=5 consecutive.
            timeout_failures = proxy.error_counts.get('timeout', 0)
            if timeout_failures >= self.config.timeout_ban_threshold:
                logger.warning(
                    f"Proxy {proxy_key} banned after {timeout_failures} timeout(s)"
                )
                proxy.mark_banned()
                return

            # Check if should ban based on consecutive failures
            total_attempts = proxy.success_count + proxy.failure_count
            if self._consecutive_failures[proxy_key] >= self.config.ban_threshold:
                proxy.mark_banned()
            elif (
                total_attempts >= self.config.ban_threshold
                and proxy.success_rate < self.config.min_success_rate
            ):
                # Only apply the success-rate gate once the proxy has enough
                # attempts for the rate to be meaningful — otherwise a single
                # transient failure (0/1=0 < min_success_rate) permabans a fresh
                # proxy and can wipe the whole pool in one rotation pass (#470).
                proxy.mark_banned()

    def unban_all(self) -> None:
        """Unban all proxies."""
        for proxy in self._proxies:
            proxy.is_banned = False
            proxy.banned_at = None
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
                    'proxy': p.masked_url,
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
