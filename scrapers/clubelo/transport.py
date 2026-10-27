"""HTTP transport for clubelo.com pages (#1462).

The ClubElo API is dead (grill 24.09.2026), so collection reads the site's HTML
pages. This transport is the single, polite way to fetch them:

- https only, ``Accept-Encoding: gzip`` (brotli is not needed, R-53);
- a white list of paths: ``/Ranking``, ``/Results``, ``/login/`` and a single
  path segment ``/{slug}``. Anything else (``/{slug}/Results`` answers 500,
  M-14) raises ``ValueError`` before a request is made;
- one request per ``min_interval`` seconds in one thread (a parameter, not a
  ``rate_limiter.py`` preset);
- redirects are NOT followed: a club without a page answers ``302 → /``, the
  caller turns it into ``no_page``;
- retries only on network errors and 5xx (``retries`` extra attempts,
  ``retry_pause`` seconds apart);
- a single 403 + ``cf-mitigated: challenge`` bootstraps clearance via the
  dedicated lease and FlareSolverr; raw bytes still come only from requests;
- other 403 / 429 / a ``cf-mitigated`` header mean the site blocks us:
  ``ClubEloBlocked`` stops the whole run, no retry;
- ``wire_bytes`` counts the compressed bytes read from the socket, the body is
  decompressed here; a response sent without gzip is gzipped by us
  (``gzip_by='us'``) so the raw store always keeps gzip bytes.

``get_source`` is the one switch between the HTML site and a future paid API
(M-12): only ``'html'`` exists today.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import re
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://clubelo.com"
FIXED_PATHS = frozenset({"/Ranking", "/Results", "/login/"})
# One path segment: no "/", "\\", query, fragment or percent-escape (an
# escaped "%2F" or "%2e%2e" would smuggle a second segment or a dot-segment).
_SLUG_PATH = re.compile(r"^/[^/\\?#%\s]+$")
BLOCK_STATUSES = frozenset({403, 429})


class ClubEloBlocked(RuntimeError):
    """The site answered 403/429 or a Cloudflare mitigation — stop the run."""


class ClubEloFetchError(RuntimeError):
    """Network error or 5xx that survived every retry."""


def get_source(source: str = "html") -> str:
    """The single point where the ClubElo data source is chosen (M-12)."""

    if source == "html":
        return source
    if source == "api":
        raise NotImplementedError(
            "ClubElo API source is not implemented: api.clubelo.com is closed; "
            "switching to a paid API is a separate owner decision"
        )
    raise ValueError(f"unknown ClubElo source: {source!r}")


def check_path(path: str) -> str:
    """Return ``path`` if it is on the white list, else raise ``ValueError``."""

    if path in FIXED_PATHS:
        return path
    if _SLUG_PATH.match(path) and path.strip("/."):
        return path
    raise ValueError(f"path is not on the ClubElo white list: {path!r}")


@dataclass(frozen=True)
class Page:
    """One HTTP answer. ``body`` is the decoded HTML, ``body_gz`` its gzip."""

    path: str
    status: int
    fetched_at: datetime
    wire_bytes: int
    content_encoding: str
    gzip_by: Optional[str]
    location: Optional[str]
    body: bytes
    body_gz: bytes

    @property
    def sha256(self) -> str:
        """sha256 of the decoded body — stable across gzip streams."""

        return hashlib.sha256(self.body).hexdigest()


class ClubEloTransport:
    """Polite, counted access to clubelo.com (see module docstring)."""

    def __init__(
        self,
        session: requests.Session,
        *,
        min_interval: float = 1.0,
        timeout: float = 30.0,
        retries: int = 2,
        retry_pause: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        base_url: str = BASE_URL,
    ) -> None:
        self.session = session
        # Never inherit HTTP(S)_PROXY or netrc from the worker environment.
        self.session.trust_env = False
        self.session.proxies = {}
        self._bootstrap_attempted = False
        self._lease = None
        self._solver = None
        self._solver_session = None
        self._closed = False
        self.min_interval = min_interval
        self.timeout = timeout
        self.retries = retries
        self.retry_pause = retry_pause
        self._sleep = sleep
        self._clock = clock
        self.base_url = base_url.rstrip("/")
        self._last_request: Optional[float] = None
        self.requests = 0
        self.wire_bytes = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        """Release browser, lease and HTTP pools, including partial bootstrap."""
        if self._closed:
            return
        self._closed = True
        actions = []
        if self._solver is not None:
            if self._solver_session is not None:
                actions.append(lambda: self._solver.destroy_session(self._solver_session))
            actions.append(self._solver.close)
        if self._lease is not None:
            actions.append(self._lease.close)
        actions.append(self.session.close)
        for action in actions:
            try:
                action()
            except Exception:
                logger.warning("ClubElo transport cleanup failed")

    def _bootstrap(self):
        from scrapers.base.flaresolverr_client import FlareSolverrClient
        from scrapers.clubelo.proxy_lease import ClubEloLease

        self._bootstrap_attempted = True
        try:
            if self.base_url != BASE_URL:
                raise ValueError("invalid ClubElo origin")
            self._lease = ClubEloLease()
            self._lease.acquire()
            self._solver = FlareSolverrClient(
                url=os.environ.get("CLUBELO_FLARESOLVERR_URL", "http://flaresolverr:8191"),
                default_timeout=90, default_max_timeout_ms=60000,
            )
            self._solver_session = "clubelo-" + uuid.uuid4().hex
            self._solver.create_session(self._solver_session, proxy_url=self._lease.proxy_url)
            solution = self._solver.get(BASE_URL + "/Ranking", self._solver_session)
            cookies, ua = solution.get("cookies"), solution.get("userAgent")
            if (solution.get("status") != 200 or not isinstance(ua, str) or not ua.strip()
                    or any(ord(c) < 32 or ord(c) > 126 for c in ua)
                    or not isinstance(cookies, list) or not cookies):
                raise ValueError("invalid clearance")
            for cookie in cookies:
                if (not isinstance(cookie, dict)
                        or not isinstance(cookie.get("name"), str) or not cookie["name"]
                        or not isinstance(cookie.get("value"), str) or not cookie["value"]
                        or cookie.get("domain", "clubelo.com") not in {"clubelo.com", ".clubelo.com"}
                        or cookie.get("path", "/") != "/"
                        or any(ord(c) < 33 or ord(c) > 126 or c in ';,'
                               for c in cookie["name"] + cookie["value"])):
                    raise ValueError("invalid clearance cookie")
            if not any(c["name"] == "cf_clearance" for c in cookies):
                raise ValueError("missing clearance cookie")
            self.session.cookies.clear()
            for cookie in cookies:
                self.session.cookies.set(cookie["name"], cookie["value"],
                                         domain="clubelo.com", path="/", secure=True)
            self.session.headers["User-Agent"] = ua
            self.session.proxies = {"https": self._lease.proxy_url}
        except Exception as exc:
            # Do not include gateway/solver errors: they can contain credentials.
            self.close()
            reason = str(exc) if isinstance(exc, ValueError) and str(exc).startswith(
                "ClubElo proxy control token") else "lease or solver failed"
            raise ClubEloBlocked("Cloudflare bootstrap: " + reason) from None

    def _pace(self) -> None:
        if self._last_request is not None:
            wait = self.min_interval - (self._clock() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        self._last_request = self._clock()

    def get(self, path: str) -> Page:
        """Fetch one white-listed path; see the module docstring for rules."""

        check_path(path)
        if self._closed:
            raise ClubEloBlocked("ClubElo transport is closed")
        url = self.base_url + path
        last_error = ""
        for attempt in range(self.retries + 1):
            if attempt:
                self._sleep(self.retry_pause)
            self._pace()
            self.requests += 1
            try:
                response = self.session.get(
                    url,
                    headers={"Accept-Encoding": "gzip"},
                    timeout=self.timeout,
                    allow_redirects=False,
                    stream=True,
                )
            except requests.RequestException as exc:
                last_error = type(exc).__name__
                logger.warning("ClubElo %s attempt %d: %s", path, attempt + 1, last_error)
                continue
            try:
                status = int(response.status_code)
                headers = response.headers
                if (status == 403 and headers.get("cf-mitigated", "").lower() == "challenge"
                        and not self._bootstrap_attempted):
                    response.close()
                    self._bootstrap()
                    return self.get(path)
                if status in BLOCK_STATUSES or "cf-mitigated" in headers:
                    raise ClubEloBlocked(
                        f"{path}: HTTP {status}, site blocked the request"
                    )
                try:
                    wire = response.raw.read(decode_content=False)
                except Exception as exc:  # urllib3 errors mid-body are network errors
                    last_error = type(exc).__name__
                    continue
            finally:
                response.close()
            self.wire_bytes += len(wire)
            if status >= 500:
                last_error = f"HTTP {status}"
                logger.warning("ClubElo %s attempt %d: HTTP %d", path, attempt + 1, status)
                continue
            try:
                return _page(path, status, headers, wire)
            except (OSError, EOFError, zlib.error) as exc:
                # A truncated/corrupt gzip body is a transfer error: retry it.
                last_error = f"broken gzip body: {type(exc).__name__}: {exc}"
                logger.warning("ClubElo %s attempt %d: %s", path, attempt + 1, last_error)
                continue
        raise ClubEloFetchError(f"{path}: {last_error} after {self.retries + 1} attempts")


def _page(path: str, status: int, headers, wire: bytes) -> Page:
    encoding = (headers.get("content-encoding") or "").strip().lower()
    if encoding == "gzip":
        body, body_gz, gzip_by = gzip.decompress(wire), wire, "wire"
    elif encoding in ("", "identity"):
        body, gzip_by = wire, "us"
        body_gz = gzip.compress(wire, mtime=0)
    else:
        raise ClubEloFetchError(f"{path}: unexpected content-encoding {encoding!r}")
    return Page(
        path=path,
        status=status,
        fetched_at=datetime.now(timezone.utc).replace(tzinfo=None),
        wire_bytes=len(wire),
        content_encoding=encoding or "identity",
        gzip_by=gzip_by,
        location=headers.get("location"),
        body=body,
        body_gz=body_gz,
    )
