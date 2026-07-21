"""
FlareSolverr Client
===================

Lightweight HTTP client for the FlareSolverr REST API. Used by Cloudflare-protected
scrapers (e.g. WhoScored events) to fetch HTML through a long-lived browser session
that holds CF state inside the FlareSolverr container.
"""

import base64
import binascii
import json
import logging
import re
import uuid
from collections import Counter
from typing import Optional, Tuple
from urllib.parse import unquote, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

MAX_XHR_BATCH_URLS = 8
_SENSITIVE_VARIANT_DEPTH = 2
_MAX_SENSITIVE_VARIANTS_PER_VALUE = 64


class FlareSolverrError(Exception):
    """Generic FlareSolverr operation error."""

    pass


class FlareSolverrRuntimeIdentityError(FlareSolverrError):
    """Raised before browser output is accepted from an unexpected runtime."""

    pass


class FlareSolverrTimeout(FlareSolverrError):
    """Raised when the FlareSolverr endpoint times out or is unreachable."""

    pass


class FlareSolverrResponseTooLarge(FlareSolverrError):
    """Raised when the fixed browser endpoint enforces a response byte cap."""

    pass


class FlareSolverrCFChallengeFailed(FlareSolverrError):
    """Raised when FlareSolverr cannot solve a Cloudflare/Turnstile challenge."""

    pass


class FlareSolverrTabCrashed(FlareSolverrError):
    """Raised when FlareSolverr's internal Chromium tab crashes.

    Distinct from a CF challenge failure: the fix is to recreate the session
    (fresh browser tab), not to re-solve a challenge. SoFIFA's SPA pages crash
    the Chromium 142 tab unpredictably during long iteration — callers rotate
    the session on this error (see ``FlareSolverrSoFIFAReader._fs_get_with_recovery``).
    """

    pass


class FlareSolverrErrorPage(FlareSolverrError):
    """Raised when FlareSolverr returns a Chromium net-error page, not site HTML.

    The browser renders its own error page (HTTP 200) for transport failures
    such as ``ERR_NO_SUPPORTED_PROXIES``. Callers must raise rather than cache
    it, or soccerdata's ``read_*(max_age=…)`` would reuse the poisoned cache
    for days (#655).
    """

    pass


# Only explicit anti-bot evidence may authorize the Cloudflare route.  Upstream
# wraps every request.get failure (including ordinary WebDriver/network errors)
# in the generic phrase ``Error solving the challenge``; matching the bare word
# ``challenge`` would therefore misclassify internal HTTP 500s as Cloudflare.
_CF_EXPLICIT_MARKERS = (
    "cloudflare",
    "turnstile",
    "failed to bypass challenge",
)

#: Substrings in a FlareSolverr error message that indicate the Chromium tab
#: crashed (vs. a CF challenge). Kept narrow to avoid mislabelling other errors.
_TAB_CRASH_MARKERS = ("tab crashed", "target crashed", "page crashed", "renderer")

#: Markers in returned HTML that flag a Chromium net-error page rather than site
#: content. FlareSolverr serves these as HTTP 200 (e.g. ERR_NO_SUPPORTED_PROXIES
#: when proxy creds are embedded in the URL, #647). ``chrome-error://`` is the
#: error page's base URL and ``neterror`` its template id/class — both confirmed
#: present in the live poisoned page (#655) and absent from real sofifa/whoscored
#: HTML, so they reliably catch any ERR_* code with zero false positives.
_CHROMIUM_ERROR_MARKERS = (
    "chrome-error://",
    "neterror",
    "ERR_NO_SUPPORTED_PROXIES",
)


def _is_explicit_cf_failure(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _CF_EXPLICIT_MARKERS)


def is_chromium_error_page(html: str) -> bool:
    """True if ``html`` is a Chromium network-error page, not real site content."""
    return any(marker in html for marker in _CHROMIUM_ERROR_MARKERS)


def _proxy_payload(proxy_url: str) -> dict:
    """Build the FlareSolverr ``proxy`` object from a proxy URL.

    Chromium's ``--proxy-server`` rejects credentials embedded in the URL
    (``http://user:pass@host:port`` → ``ERR_NO_SUPPORTED_PROXIES``: every
    proxied fetch silently returns a browser error page with HTTP 200, so
    no exception is raised and reactive rotation never fires — see #647).
    FlareSolverr takes auth as separate ``username`` / ``password`` fields,
    so split them out of the netloc and pass a credential-free URL.
    """
    parts = urlsplit(proxy_url)
    if not (parts.username or parts.password):
        return {"url": proxy_url}
    netloc = parts.hostname or ""
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    clean_url = urlunsplit(
        (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
    )
    return {
        "url": clean_url,
        # ``ProxyFilterClient`` percent-encodes its short-lived lease token in
        # userinfo.  urlsplit intentionally leaves that encoding intact, while
        # FlareSolverr expects the original credentials in separate fields.
        "username": unquote(parts.username or ""),
        "password": unquote(parts.password or ""),
    }


def _normalise_url_key(url: str) -> str:
    """Collapse a URL to ``host/path`` (query + fragment dropped).

    Groups repeated calls to one endpoint under a single key so the per-URL
    traffic counter (issue #616) stays bounded regardless of cache-busting
    query params (``?d=`` on WhoScored data, ``?r=&set=true`` on SoFIFA).
    Kept local so this lightweight client never imports a browser runtime.
    Returns '' for falsy input.
    """
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        if parts.netloc:
            return f"{parts.netloc}{parts.path}"
        return url.split("?", 1)[0].split("#", 1)[0]
    except Exception:
        return url.split("?", 1)[0]


def describe_proxy_mode(proxy_url: Optional[str]) -> str:
    """Human-readable FlareSolverr proxy mode for logs (issue #616).

    Makes the proxy decision visible at session start so an accidental
    re-enable (via ``PROXY_FILTER_URL`` or a non-empty proxy-file) shows up
    in logs instead of being silent. Never echoes residential credentials —
    a residential URL carries ``user:pass`` so we only name the mode.
    """
    if not proxy_url:
        return "PROXY-LESS (FlareSolverr solves CF directly)"
    if "proxy_filter" in proxy_url:
        return "via ad-tech filter (#652)"
    return "via residential proxy"


def _sensitive_request_values(
    payload: dict, *, endpoint_url: Optional[str] = None
) -> tuple[str, ...]:
    """Return request values that must never enter logs or exceptions."""

    values: list[object] = [endpoint_url, payload.get("session"), payload.get("url")]
    urls = payload.get("urls")
    if isinstance(urls, list):
        values.extend(urls)
    proxy = payload.get("proxy")
    if isinstance(proxy, dict):
        values.extend(proxy.get(field) for field in ("url", "username", "password"))
    return tuple(value for value in values if type(value) is str and value)


def _escaped_sensitive_variants(value: str) -> set[str]:
    """Cover a bounded two-level closure of common diagnostic renderings."""

    def render_once(item: str) -> set[str]:
        rendered = (
            json.dumps(item, ensure_ascii=True),
            repr(item),
            repr(item.encode("utf-8")),
        )
        variants: set[str] = set()
        for candidate in rendered:
            variants.add(candidate)
            if (
                len(candidate) >= 2
                and candidate[0] in {'"', "'"}
                and candidate[-1] == candidate[0]
            ):
                variants.add(candidate[1:-1])
            elif (
                len(candidate) >= 3
                and candidate[0] == "b"
                and candidate[1] in {'"', "'"}
                and candidate[-1] == candidate[1]
            ):
                variants.add(candidate[2:-1])
        return {variant for variant in variants if variant}

    variants = {value}
    frontier = {value}
    for _ in range(_SENSITIVE_VARIANT_DEPTH):
        expanded = {
            rendered
            for item in frontier
            for rendered in render_once(item)
            if rendered not in variants
        }
        remaining = _MAX_SENSITIVE_VARIANTS_PER_VALUE - len(variants)
        if remaining <= 0:
            break
        if len(expanded) > remaining:
            expanded = set(
                sorted(expanded, key=lambda item: (len(item), item))[:remaining]
            )
        variants.update(expanded)
        frontier = expanded
        if not frontier:
            break
    return variants


def _redact_request_values(
    value: object,
    payload: dict,
    *,
    endpoint_url: Optional[str] = None,
) -> str:
    """Remove endpoint, source, session and proxy values from diagnostic text."""

    variants = {
        variant
        for sensitive in _sensitive_request_values(payload, endpoint_url=endpoint_url)
        for variant in _escaped_sensitive_variants(sensitive)
    }
    text = str(value)
    if not variants:
        return text
    # One substitution pass prevents a short credential from corrupting the
    # replacement marker or exposing text introduced by an earlier replacement.
    pattern = re.compile(
        "|".join(
            re.escape(variant) for variant in sorted(variants, key=len, reverse=True)
        )
    )
    return pattern.sub("<redacted-request-value>", text)


class FlareSolverrClient:
    """HTTP wrapper around FlareSolverr `/v1` and `/health` endpoints."""

    #: Number of top per-URL consumers surfaced by get_traffic_stats().
    _TOP_URLS_N = 25
    _MAX_XHR_BATCH_URLS = MAX_XHR_BATCH_URLS
    _RUNTIME_IDENTITY_PATH = "/v1/whoscored/runtime-identity"

    def __init__(
        self,
        url: str = "http://flaresolverr:8191",
        default_timeout: float = 90.0,
        default_max_timeout_ms: int = 60_000,
        *,
        expected_version: Optional[str] = None,
        expected_extension_sha256: Optional[str] = None,
    ) -> None:
        if (expected_version is None) != (expected_extension_sha256 is None):
            raise ValueError(
                "expected FlareSolverr version and extension SHA-256 must be "
                "configured together"
            )
        if expected_version is not None:
            if type(expected_version) is not str or not expected_version:
                raise ValueError("expected FlareSolverr version must be a string")
            if (
                type(expected_extension_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", expected_extension_sha256) is None
            ):
                raise ValueError(
                    "expected FlareSolverr extension SHA-256 must be lowercase hex"
                )
        self.url = url.rstrip("/")
        self.default_timeout = default_timeout
        self.default_max_timeout_ms = default_max_timeout_ms
        self._expected_version = expected_version
        self._expected_extension_sha256 = expected_extension_sha256
        self._session: Optional[requests.Session] = None
        self._auto_session_id: Optional[str] = None
        # Traffic-audit counters (issue #616). ``fs_response_bytes`` is the
        # payload FlareSolverr returns to us (rendered HTML + JSON envelope) —
        # a LOWER BOUND on residential-proxy traffic, NOT the proxy MB itself:
        # Camoufox downloads images/CSS/JS/XHR through the proxy and returns
        # only the rendered HTML. ``sessions_created`` ≈ CF cold-starts, the
        # real traffic driver (each new session re-solves the CF challenge).
        self._fs_response_bytes = 0
        self._requests = 0
        self._bytes_by_url: Counter = Counter()
        self._requests_by_url: Counter = Counter()
        self._sessions_created = 0
        self._cf_challenge_failures = 0
        self._last_post_bytes = 0

    def _verify_runtime_identity(self, data: dict, *, endpoint_path: str) -> None:
        """Fail before interpreting or accounting output from a stale runtime."""

        if self._expected_version is None:
            return
        if (
            data.get("version") != self._expected_version
            or data.get("extension_sha256") != self._expected_extension_sha256
        ):
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr runtime identity does not match the attested "
                "WhoScored extension"
            )
        if endpoint_path not in {"/v1/xhr", "/v1/xhr/batch"}:
            return
        status = data.get("status")
        expected_fields = {
            "status",
            "message",
            "startTimestamp",
            "endTimestamp",
            "version",
            "extension_sha256",
        }
        if status == "ok":
            expected_fields.add("solution")
        if (
            set(data) != expected_fields
            or status not in {"ok", "error"}
            or type(data.get("message")) is not str
            or type(data.get("startTimestamp")) is not int
            or type(data.get("endTimestamp")) is not int
            or data["startTimestamp"] < 0
            or data["endTimestamp"] < data["startTimestamp"]
        ):
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr WhoScored endpoint returned an invalid identity envelope"
            )

    def _preflight_runtime_identity(self) -> None:
        """Verify the immutable extension before any browser/source side effect."""

        if self._expected_version is None:
            return
        endpoint = f"{self.url}{self._RUNTIME_IDENTITY_PATH}"
        try:
            response = self.session.get(endpoint, timeout=self.default_timeout)
        except requests.exceptions.RequestException as exc:
            safe_error = _redact_request_values(
                exc,
                {},
                endpoint_url=self.url,
            )
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr runtime identity preflight failed: " + safe_error
            ) from None
        if not response.ok:
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr runtime identity preflight returned an HTTP error"
            )
        try:
            data = response.json()
        except ValueError:
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr runtime identity preflight is not JSON"
            ) from None
        if (
            not isinstance(data, dict)
            or set(data) != {"status", "version", "extension_sha256"}
            or data.get("status") != "ok"
        ):
            raise FlareSolverrRuntimeIdentityError(
                "FlareSolverr runtime identity preflight envelope is invalid"
            )
        self._verify_runtime_identity(data, endpoint_path=self._RUNTIME_IDENTITY_PATH)

    @property
    def session(self) -> requests.Session:
        """Lazily build a requests Session.

        Only idempotent GETs (currently ``/health``) are retried.  A POST to
        FlareSolverr's ``/v1`` is *not* an ordinary API write: ``request.get``
        drives a complete browser navigation and ``sessions.create`` starts a
        browser.  Retrying either below the scraper's explicit recovery state
        machine multiplies traffic and can leave orphan sessions behind.
        """
        if self._session is None:
            session = requests.Session()
            # An operation labelled as direct FlareSolverr must not inherit a
            # worker-wide HTTP(S)_PROXY.  Paid browser sessions receive their
            # proxy explicitly in ``sessions.create``.
            session.trust_env = False
            retry = Retry(
                total=2,
                backoff_factor=1.5,
                status_forcelist=(502, 503, 504),
                allowed_methods=frozenset(["GET"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._session = session
        return self._session

    @session.setter
    def session(self, value: requests.Session) -> None:
        self._session = value

    @session.deleter
    def session(self) -> None:
        self._session = None

    def _post(
        self,
        payload: dict,
        timeout: Optional[float] = None,
        *,
        endpoint_path: str = "/v1",
    ) -> dict:
        """POST to one fixed API endpoint and translate protocol errors."""
        endpoint = f"{self.url}{endpoint_path}"
        cmd = payload.get("cmd", "?")
        endpoint_type = {
            "/v1": "api",
            "/v1/xhr": "xhr",
            "/v1/xhr/batch": "xhr_batch",
        }.get(endpoint_path, "custom")
        logger.debug("FlareSolverr POST (endpoint_type=%s, cmd=%s)", endpoint_type, cmd)

        # The GET has no browser/session/source side effect.  A stale service
        # is rejected before sessions.create or request.get can spend proxy
        # bytes; the POST response is checked again below to detect restarts.
        self._preflight_runtime_identity()

        request_error: Optional[FlareSolverrError] = None
        try:
            response = self.session.post(
                endpoint,
                json=payload,
                timeout=timeout or self.default_timeout,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            error_type = type(e).__name__
            safe_error = _redact_request_values(e, payload, endpoint_url=self.url)
            logger.warning(
                "FlareSolverr transport error "
                "(endpoint_type=%s, cmd=%s, error_type=%s): %s",
                endpoint_type,
                cmd,
                error_type,
                safe_error,
            )
            request_error = FlareSolverrTimeout(
                f"FlareSolverr unreachable ({error_type}): {safe_error}"
            )
        except requests.exceptions.RequestException as e:
            error_type = type(e).__name__
            safe_error = _redact_request_values(e, payload, endpoint_url=self.url)
            logger.warning(
                "FlareSolverr request error "
                "(endpoint_type=%s, cmd=%s, error_type=%s): %s",
                endpoint_type,
                cmd,
                error_type,
                safe_error,
            )
            request_error = FlareSolverrError(
                f"FlareSolverr request failed ({error_type}): {safe_error}"
            )
        if request_error is not None:
            # Raise outside the ``except`` suite so neither ``__cause__`` nor
            # ``__context__`` retains the original URL-bearing exception.
            raise request_error from None

        data: object = None
        if self._expected_version is not None:
            try:
                data = response.json()
            except ValueError as e:
                error_type = type(e).__name__
                safe_error = _redact_request_values(e, payload, endpoint_url=self.url)
                raise FlareSolverrRuntimeIdentityError(
                    "FlareSolverr identity response is not JSON "
                    f"({error_type}): {safe_error}"
                ) from None
            if not isinstance(data, dict):
                raise FlareSolverrRuntimeIdentityError(
                    "FlareSolverr identity response is not a JSON object"
                )
            self._verify_runtime_identity(data, endpoint_path=endpoint_path)

        if not response.ok:
            body = response.text
            safe_body = _redact_request_values(body, payload, endpoint_url=self.url)[
                :300
            ]
            logger.warning(
                "FlareSolverr HTTP %s (endpoint_type=%s, cmd=%s): %s",
                response.status_code,
                endpoint_type,
                cmd,
                safe_body,
            )
            if response.status_code == 413:
                raise FlareSolverrResponseTooLarge(
                    f"FlareSolverr HTTP 413: {safe_body}"
                )
            if _is_explicit_cf_failure(safe_body):
                self._cf_challenge_failures += 1
                raise FlareSolverrCFChallengeFailed(
                    f"FlareSolverr HTTP {response.status_code}: {safe_body}"
                )
            raise FlareSolverrError(
                f"FlareSolverr HTTP {response.status_code}: {safe_body}"
            )

        if data is None:
            json_error: Optional[FlareSolverrError] = None
            try:
                data = response.json()
            except ValueError as e:
                error_type = type(e).__name__
                safe_error = _redact_request_values(e, payload, endpoint_url=self.url)
                json_error = FlareSolverrError(
                    f"FlareSolverr returned non-JSON ({error_type}): {safe_error}"
                )
            if json_error is not None:
                # JSON decoder exceptions can retain document excerpts. Do not keep
                # them as exception context for URL- or credential-bearing requests.
                raise json_error from None
        if not isinstance(data, dict):
            raise FlareSolverrError("FlareSolverr returned a non-object JSON response")

        if data.get("status") == "error":
            message = str(data.get("message", "unknown error"))
            safe_message = _redact_request_values(
                message, payload, endpoint_url=self.url
            )
            lowered = message.lower()
            if any(marker in lowered for marker in _TAB_CRASH_MARKERS):
                logger.warning(f"FlareSolverr tab crashed (cmd={cmd}): {safe_message}")
                raise FlareSolverrTabCrashed(safe_message)
            if _is_explicit_cf_failure(safe_message):
                self._cf_challenge_failures += 1
                logger.warning(
                    f"FlareSolverr CF challenge failed (cmd={cmd}): {safe_message}"
                )
                raise FlareSolverrCFChallengeFailed(safe_message)
            logger.warning(f"FlareSolverr error (cmd={cmd}): {safe_message}")
            raise FlareSolverrError(safe_message)

        # Count only successful responses: bytes FlareSolverr returned to us.
        # ``_last_post_bytes`` bridges to get() for per-URL attribution.
        self._last_post_bytes = len(response.content or b"")
        self._fs_response_bytes += self._last_post_bytes
        return data

    def health(self) -> bool:
        """Return True if /health responds 200, False on any error."""
        try:
            response = self.session.get(
                f"{self.url}/health",
                timeout=self.default_timeout,
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def create_session(self, session_id: str, proxy_url: Optional[str] = None) -> None:
        """Create a named FlareSolverr browser session, optionally bound to a proxy."""
        payload: dict = {"cmd": "sessions.create", "session": session_id}
        if proxy_url:
            payload["proxy"] = _proxy_payload(proxy_url)
        self._post(payload)
        # A fresh session re-solves the Cloudflare challenge → a cold-start.
        # Counting these is the cheapest signal for the dominant traffic driver
        # (issue #616): SoFIFA rotates every 4 requests, WhoScored every 8–10.
        self._sessions_created += 1
        logger.info(f"FlareSolverr session created ({describe_proxy_mode(proxy_url)})")

    def destroy_session(self, session_id: str) -> None:
        """Destroy a session; idempotent — never raises if the session is gone."""
        payload = {"cmd": "sessions.destroy", "session": session_id}
        try:
            self._post(payload)
            logger.info("FlareSolverr session destroyed")
        except FlareSolverrError as e:
            safe_error = _redact_request_values(e, payload, endpoint_url=self.url)
            logger.debug(f"FlareSolverr destroy_session ignored: {safe_error}")

    def destroy_session_strict(self, session_id: str) -> None:
        """Destroy a supervised session and surface every remote failure."""

        payload = {"cmd": "sessions.destroy", "session": session_id}
        self._post(payload)
        logger.info("Supervised FlareSolverr session destroyed")

    def list_sessions(self) -> list[str]:
        """List active FlareSolverr session IDs."""
        data = self._post({"cmd": "sessions.list"})
        return list(data.get("sessions", []))

    def get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
        return_only_cookies: bool = False,
        disable_media: bool = False,
    ) -> dict:
        """GET via FlareSolverr; returns the `solution` subdict (html, cookies, ...)."""
        payload: dict = {
            "cmd": "request.get",
            "url": url,
            "session": session_id,
            "maxTimeout": max_timeout_ms or self.default_max_timeout_ms,
        }
        if return_only_cookies:
            payload["returnOnlyCookies"] = True
        if disable_media:
            # Supported by FlareSolverr 3.x.  This blocks images, stylesheets
            # and fonts in the browser while preserving the document and the
            # scripts required to solve Cloudflare challenges.
            payload["disableMedia"] = True

        timeout = (
            (max_timeout_ms / 1000.0 + 30.0) if max_timeout_ms else self.default_timeout
        )
        data = self._post(payload, timeout=timeout)
        # Per-URL traffic attribution (issue #616). Only successful fetches
        # reach here (_post raises on CF / error), so a failed page is never
        # booked as a request.
        self._record_url_request(url, self._last_post_bytes)
        solution = data.get("solution") or {}
        return {
            "html": solution.get("response", ""),
            "cookies": solution.get("cookies", []),
            "userAgent": solution.get("userAgent", ""),
            "status": solution.get("status", 0),
        }

    def xhr_get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
    ) -> dict:
        """Run the restricted WhoScored same-origin XHR extension.

        The extension accepts no arbitrary JavaScript, headers or HTTP method;
        it only performs a fixed credentialed GET for allow-listed structured
        feeds inside an existing ``ws-*`` browser session.
        """

        timeout_ms = max_timeout_ms or self.default_max_timeout_ms
        data = self._post(
            {
                "url": url,
                "session": session_id,
                "maxTimeout": timeout_ms,
            },
            timeout=timeout_ms / 1000.0 + 30.0,
            endpoint_path="/v1/xhr",
        )
        solution = data.get("solution") or {}
        decoded = self._decode_xhr_solution(solution, expected_url=url)
        self._record_url_request(url, self._last_post_bytes)
        return decoded

    def xhr_get_many(
        self,
        urls: list[str],
        session_id: str,
        max_timeout_ms: Optional[int] = None,
    ) -> list[dict]:
        """Run one restricted, bounded same-origin WhoScored XHR batch.

        The server fixes concurrency and response limits.  This client accepts
        no headers, method, JavaScript, proxy or cookie controls and verifies
        that every response is present and remains in request order.
        """

        if not 1 <= len(urls) <= self._MAX_XHR_BATCH_URLS:
            raise ValueError(
                f"xhr_get_many requires 1 to {self._MAX_XHR_BATCH_URLS} URLs"
            )
        if len(set(urls)) != len(urls):
            raise ValueError("xhr_get_many URLs must be unique")
        timeout_ms = max_timeout_ms or self.default_max_timeout_ms
        data = self._post(
            {
                "urls": list(urls),
                "session": session_id,
                "maxTimeout": timeout_ms,
            },
            timeout=timeout_ms / 1000.0 + 30.0,
            endpoint_path="/v1/xhr/batch",
        )
        solution = data.get("solution") or {}
        raw_responses = solution.get("responses")
        if not isinstance(raw_responses, list) or len(raw_responses) != len(urls):
            raise FlareSolverrError("FlareSolverr XHR batch is incomplete")

        responses: list[dict] = []
        total_source_bytes = 0
        for expected_url, raw_response in zip(urls, raw_responses):
            if not isinstance(raw_response, dict):
                raise FlareSolverrError("FlareSolverr XHR batch item is invalid")
            if raw_response.get("requestedUrl") != expected_url:
                raise FlareSolverrError("FlareSolverr XHR batch order is invalid")
            if raw_response.get("ok") is False:
                kind = raw_response.get("kind")
                if kind not in {
                    "response_too_large",
                    "timeout",
                    "source_redirect_rejected",
                    "fetch_failed",
                }:
                    raise FlareSolverrError(
                        "FlareSolverr XHR batch item error is invalid"
                    )
                decoded = {"ok": False, "kind": str(kind), "responseBytes": 0}
                responses.append(decoded)
                continue
            if raw_response.get("ok") is not True:
                raise FlareSolverrError("FlareSolverr XHR batch item status is invalid")
            decoded = {
                "ok": True,
                **self._decode_xhr_solution(raw_response, expected_url=expected_url),
            }
            responses.append(decoded)
            total_source_bytes += int(decoded["responseBytes"])

        declared_total = solution.get("responseBytes")
        if (
            isinstance(declared_total, bool)
            or not isinstance(declared_total, int)
            or declared_total != total_source_bytes
        ):
            raise FlareSolverrError(
                "FlareSolverr XHR batch byte count does not match its bodies"
            )

        # ``_post`` observes one envelope. Attribute all of its bytes across
        # logical source URLs deterministically, while keeping one request per
        # URL in the existing traffic report.
        remaining = self._last_post_bytes
        denominator = sum(max(1, item["responseBytes"]) for item in responses)
        for index, (url, item) in enumerate(zip(urls, responses)):
            if index == len(responses) - 1:
                attributed = remaining
            else:
                attributed = (
                    self._last_post_bytes * max(1, item["responseBytes"]) // denominator
                )
                remaining -= attributed
            self._record_url_request(url, attributed)
        return responses

    @staticmethod
    def _decode_xhr_solution(solution: dict, *, expected_url: str) -> dict:
        if not isinstance(solution, dict):
            raise FlareSolverrError("FlareSolverr XHR solution is invalid")
        encoded = solution.get("responseBase64")
        if not isinstance(encoded, str):
            raise FlareSolverrError("FlareSolverr XHR response has no base64 body")
        try:
            body = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise FlareSolverrError(
                "FlareSolverr XHR response body is not valid base64"
            ) from exc
        declared_size = solution.get("responseBytes")
        if (
            isinstance(declared_size, bool)
            or not isinstance(declared_size, int)
            or declared_size < 0
        ):
            raise FlareSolverrError(
                "FlareSolverr XHR response has invalid responseBytes"
            )
        size = declared_size
        if size != len(body):
            raise FlareSolverrError(
                "FlareSolverr XHR response byte count does not match its body"
            )
        headers = solution.get("headers") or {}
        if not isinstance(headers, dict):
            raise FlareSolverrError("FlareSolverr XHR response headers are invalid")
        status = solution.get("status")
        if (
            isinstance(status, bool)
            or not isinstance(status, int)
            or not 100 <= status <= 599
        ):
            raise FlareSolverrError("FlareSolverr XHR response status is invalid")
        final_url = solution.get("finalUrl")
        if not isinstance(final_url, str) or final_url != expected_url:
            raise FlareSolverrError(
                "FlareSolverr XHR response final URL does not match its request"
            )

        return {
            "content": body,
            "headers": {str(key): str(value) for key, value in headers.items()},
            "finalUrl": final_url,
            "status": status,
            "responseBytes": size,
        }

    def _record_url_request(self, url: str, response_bytes: int) -> None:
        self._requests += 1
        url_key = _normalise_url_key(url)
        if url_key:
            self._bytes_by_url[url_key] += max(0, int(response_bytes))
            self._requests_by_url[url_key] += 1

    def get_traffic_stats(self) -> dict:
        """Per-scrape proxy-traffic audit summary (issue #616).

        ``fs_response_*`` is the FlareSolverr payload returned to us — a LOWER
        BOUND on residential-proxy bytes, not the proxy MB itself (sub-resources
        are fetched by Camoufox and never returned). The true per-match proxy
        MB is measured at the container/proxy level on the VM; see
        ``docs/research/flaresolverr-proxy-traffic-audit.md``. ``sessions_created``
        ≈ CF cold-starts, the dominant traffic driver.
        """
        top = sorted(self._bytes_by_url.items(), key=lambda kv: kv[1], reverse=True)[
            : self._TOP_URLS_N
        ]
        return {
            "fs_response_bytes": self._fs_response_bytes,
            "fs_response_mb": round(self._fs_response_bytes / 1024 / 1024, 4),
            "requests": self._requests,
            "sessions_created": self._sessions_created,
            "cf_challenge_failures": self._cf_challenge_failures,
            "top_traffic_urls": [
                {
                    "url": key,
                    "bytes": size,
                    "mb": round(size / 1024 / 1024, 4),
                    "requests": int(self._requests_by_url.get(key, 0)),
                }
                for key, size in top
            ],
        }

    def close(self) -> None:
        """Release this client's HTTP connection pool.

        Browser sessions are owned by callers and should normally be destroyed
        explicitly before this method is called.  The context-manager session
        is the sole session owned by this client, so clean it up defensively.
        """

        if self._auto_session_id is not None:
            try:
                self.destroy_session(self._auto_session_id)
            except Exception as exc:
                logger.debug(
                    "Could not destroy FlareSolverr context session (%s)",
                    type(exc).__name__,
                )
            finally:
                self._auto_session_id = None
        session = self._session
        self._session = None
        if session is not None:
            session.close()

    def __enter__(self) -> Tuple["FlareSolverrClient", str]:
        session_id = f"fs-{uuid.uuid4().hex[:8]}"
        try:
            self.create_session(session_id)
        except Exception:
            # A lost create response may still have created the server-side
            # browser. The deterministic id makes best-effort cleanup safe.
            try:
                self.destroy_session(session_id)
            finally:
                self.close()
            raise
        self._auto_session_id = session_id
        return self, session_id

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._auto_session_id is not None:
            try:
                self.destroy_session(self._auto_session_id)
            except Exception as e:
                logger.warning(
                    f"FlareSolverr __exit__ destroy_session failed ({type(e).__name__})"
                )
            finally:
                self._auto_session_id = None
        self.close()
        return False
