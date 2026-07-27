"""Small, injectable HTTP client for Understat's JSON endpoints."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import random
import time
from typing import Any, Optional
from urllib.parse import quote


UNDERSTAT_URL = "https://understat.com"
API_HEADERS = {"X-Requested-With": "XMLHttpRequest"}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class UnderstatHTTPError(RuntimeError):
    def __init__(self, url: str, status_code: int):
        super().__init__(f"Understat request failed: HTTP {status_code} for {url}")
        self.url = url
        self.status_code = status_code


class UnderstatPayloadError(RuntimeError):
    pass


class _EvenPacer:
    """Evenly space requests so bursts cannot exceed the configured RPM."""

    def __init__(
        self,
        requests_per_minute: int,
        *,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
    ):
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self.interval = 60.0 / requests_per_minute
        self.monotonic = monotonic
        self.sleep = sleep
        self._last_request: Optional[float] = None

    def acquire(self) -> None:
        now = self.monotonic()
        if self._last_request is not None:
            delay = self.interval - (now - self._last_request)
            if delay > 0:
                self.sleep(delay)
                now = self.monotonic()
        self._last_request = now


class UnderstatClient:
    """Requests-shaped client with cookie bootstrap, pacing, retry and cache.

    ``session`` only needs a ``get`` method, which keeps the source fully
    deterministic in unit tests. ``sleep`` and clocks are injectable for the
    same reason.
    """

    def __init__(
        self,
        *,
        session: Any = None,
        base_url: str = UNDERSTAT_URL,
        requests_per_minute: int = 30,
        max_attempts: int = 5,
        timeout_seconds: float = 30.0,
        backoff_base_seconds: float = 1.0,
        cache_dir: Optional[Path | str] = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        jitter: Callable[[float, float], float] = random.uniform,
    ):
        if session is None:
            import requests

            session = requests.Session()
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.max_attempts = max_attempts
        self.timeout_seconds = timeout_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self.sleep = sleep
        self.now = now
        self.jitter = jitter
        self._pacer = _EvenPacer(
            requests_per_minute,
            monotonic=monotonic,
            sleep=sleep,
        )
        self._cookies_initialized = False
        self.request_count = 0

        headers = getattr(self.session, "headers", None)
        if headers is not None and hasattr(headers, "update"):
            headers.update(
                {
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                }
            )

    def get_stat_data(self, *, force_refresh: bool = True) -> dict[str, Any]:
        return self._get_json("/getStatData", "stat.json", force_refresh=force_refresh)

    def close(self) -> None:
        close = getattr(self.session, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "UnderstatClient":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def get_league_data(
        self,
        source_league: str,
        source_season_id: int,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        slug = quote(source_league, safe="_-")
        return self._get_json(
            f"/getLeagueData/{slug}/{source_season_id}",
            f"league_{slug}_{source_season_id}.json",
            force_refresh=force_refresh,
        )

    def get_match_data(
        self, match_id: int | str, *, force_refresh: bool = False
    ) -> dict[str, Any]:
        match = quote(str(match_id), safe="")
        return self._get_json(
            f"/getMatchData/{match}",
            f"match_{match}.json",
            force_refresh=force_refresh,
        )

    def get_team_data(
        self,
        team_name: str,
        source_season_id: int,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        slug = quote(team_name.replace(" ", "_"), safe="_-")
        return self._get_json(
            f"/getTeamData/{slug}/{source_season_id}",
            f"team_{slug}_{source_season_id}.json",
            force_refresh=force_refresh,
        )

    def _get_json(
        self, path: str, cache_name: str, *, force_refresh: bool
    ) -> dict[str, Any]:
        cache_path = self.cache_dir / cache_name if self.cache_dir is not None else None
        if cache_path is not None and cache_path.exists() and not force_refresh:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise UnderstatPayloadError(f"Cached payload is not an object: {cache_path}")
            return payload

        self._ensure_cookies()
        url = self.base_url + path
        response = self._get_with_retry(url, headers=API_HEADERS)
        try:
            payload = response.json()
        except Exception as exc:
            raise UnderstatPayloadError(f"Invalid JSON from {url}") from exc
        if not isinstance(payload, dict):
            raise UnderstatPayloadError(f"Expected an object from {url}")

        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = cache_path.with_suffix(cache_path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary.replace(cache_path)
        return payload

    def _ensure_cookies(self) -> None:
        if self._cookies_initialized:
            return
        self._get_with_retry(self.base_url, headers={})
        self._cookies_initialized = True

    def _get_with_retry(self, url: str, *, headers: Mapping[str, str]) -> Any:
        last_error: Optional[BaseException] = None
        for attempt in range(1, self.max_attempts + 1):
            self._pacer.acquire()
            self.request_count += 1
            try:
                response = self.session.get(
                    url,
                    headers=dict(headers),
                    timeout=self.timeout_seconds,
                )
                status = int(getattr(response, "status_code", 0))
                if 200 <= status < 300:
                    return response
                error = UnderstatHTTPError(url, status)
                if status not in RETRYABLE_STATUSES or attempt == self.max_attempts:
                    raise error
                last_error = error
                self.sleep(self._retry_delay(response, attempt))
            except UnderstatHTTPError:
                raise
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    raise
                self.sleep(self._backoff_delay(attempt))
        if last_error is not None:  # defensive; loop always returns or raises
            raise last_error
        raise RuntimeError("Understat retry loop terminated unexpectedly")

    def _retry_delay(self, response: Any, attempt: int) -> float:
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After") if isinstance(headers, Mapping) else None
        parsed = self._parse_retry_after(retry_after)
        return max(parsed or 0.0, self._backoff_delay(attempt))

    def _backoff_delay(self, attempt: int) -> float:
        base = self.backoff_base_seconds * (2 ** (attempt - 1))
        return base + self.jitter(0.0, self.backoff_base_seconds)

    def _parse_retry_after(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            pass
        try:
            parsed = parsedate_to_datetime(str(value))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, (parsed - self.now()).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


__all__ = [
    "API_HEADERS",
    "UNDERSTAT_URL",
    "UnderstatClient",
    "UnderstatHTTPError",
    "UnderstatPayloadError",
]
