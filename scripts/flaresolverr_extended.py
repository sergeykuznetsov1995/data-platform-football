#!/usr/bin/env python3
"""FlareSolverr 3.4.6 with restricted WhoScored production hardening.

The stock FlareSolverr API can navigate a browser, but it cannot issue the
same-origin ``fetch`` used by WhoScored's structured-statistics pages. This
entrypoint imports the upstream image application and adds one fixed single-
URL operation::

    POST /v1/xhr
    {"url": "https://www.whoscored.com/statisticsfeed/...",
     "session": "ws-...", "maxTimeout": 60000}

It also wraps the exact upstream 3.4.6 ``_evil_logic`` implementation so the
stock ``disableMedia`` CDP list keeps its image/CSS/font patterns and gains a
fixed audio/video suffix list before navigation. There is no API control for
that extension; incompatible upstream code or a failed CDP command prevents
navigation and fails the request closed.

The same fixed operation is also available in a bounded batch form::

    POST /v1/xhr/batch
    {"urls": ["https://www.whoscored.com/statisticsfeed/..."],
     "session": "ws-...", "maxTimeout": 60000}

The batch accepts at most eight unique URLs and runs at most four same-origin
GETs concurrently.  Per-response and aggregate byte ceilings are server-side
constants. Malformed requests fail before browser execution; runtime item
failures are returned without bodies beside successful items so the caller can
durably cache successes, retry only failures and still reject partial publish.

It deliberately is *not* a generic browser automation API.  Callers cannot
choose a method, headers, credentials, JavaScript, proxy, redirect policy, or
response limit.  The target must be one of the known WhoScored structured-feed
paths and the browser session must already exist.  Keeping imports of Bottle
and FlareSolverr inside ``create_app``/``main`` makes the validation and browser
result contract unit-testable on hosts that do not install those dependencies.

This file is intended to be mounted read-only at ``/app/flaresolverr_extended.py``
in the official ``ghcr.io/flaresolverr/flaresolverr:v3.4.6`` image and used as
its entrypoint.  Merely adding the file does not alter or restart a service.
"""

from __future__ import annotations

import base64
import binascii
import functools
import hashlib
import inspect
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_BATCH_URLS = 8
MAX_BATCH_RESPONSE_BYTES = 8 * 1024 * 1024
BATCH_CONCURRENCY = 4
# All WhoScored browser XHRs share this process.  Pace their *actual browser
# starts* here, after the caller-side token bucket, so four capacity workers
# cannot turn independently safe batches into a 16-request source burst.  A
# 546 ms interval is 109.89 starts/minute; the measured useful/attempt ratio
# still leaves the 144k page-units/day production gate with bounded headroom.
GLOBAL_XHR_MIN_START_INTERVAL_MS = 546
# A paced launch must still have this much of the caller's absolute deadline
# available at the instant the fixed browser script invokes ``fetch``.  This
# prevents a queued request from touching the source when it has no realistic
# chance to finish inside its end-to-end budget.
XHR_MIN_EXECUTION_MARGIN_MS = 500
WAITRESS_THREADS = 8
DEFAULT_TIMEOUT_MS = 60_000
MAX_TIMEOUT_MS = 120_000
MIN_TIMEOUT_MS = 1_000
ALLOWED_PATH_PATTERNS = (
    re.compile(r"\A/statisticsfeed/1/get(?:team|player)statistics\Z"),
    re.compile(r"\A/stagestatfeed/[1-9][0-9]*/stageteams/\Z"),
)
_PAYLOAD_FIELDS = frozenset({"url", "session", "maxTimeout"})
_BATCH_PAYLOAD_FIELDS = frozenset({"urls", "session", "maxTimeout"})
_SESSION_RE = re.compile(r"\Aws-[A-Za-z0-9][A-Za-z0-9_-]{0,92}\Z")
_CONTROL_OR_SPACE_RE = re.compile(r"[\x00-\x20\x7f]")
_FEED_PATH_RE = re.compile(r"\A/[A-Za-z0-9_/-]+\Z")
_HEADER_NAME_RE = re.compile(r"\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
_HEADER_VALUE_CONTROL_RE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")


class _XhrStartPacer:
    """Arbitrate the real browser ``fetch`` invocation process-wide.

    The lock stays held while the fixed synchronous launch script crosses the
    WebDriver boundary and invokes ``fetch``.  The next launch is paced from
    the *completion* of that command, which is conservatively later than its
    fetch invocation.  Delayed commands therefore reduce throughput instead
    of turning old absolute reservations into burst credit.
    """

    def __init__(
        self,
        *,
        interval_ms: int = GLOBAL_XHR_MIN_START_INTERVAL_MS,
        execution_margin_ms: int = XHR_MIN_EXECUTION_MARGIN_MS,
        monotonic: Any = time.monotonic,
        sleep: Any = time.sleep,
    ) -> None:
        if isinstance(interval_ms, bool) or not isinstance(interval_ms, int):
            raise ValueError("XHR pacing interval must be an integer")
        if interval_ms < 1:
            raise ValueError("XHR pacing interval must be positive")
        if (
            isinstance(execution_margin_ms, bool)
            or not isinstance(execution_margin_ms, int)
            or execution_margin_ms < 1
        ):
            raise ValueError("XHR execution margin must be a positive integer")
        self.interval_ms = interval_ms
        self.execution_margin_ms = execution_margin_ms
        self._interval_seconds = interval_ms / 1_000.0
        self._execution_margin_seconds = execution_margin_ms / 1_000.0
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._last_launch_completed: float | None = None

    def launch(self, *, deadline: float, starter: Any) -> Any:
        """Run one fixed browser starter at a globally safe actual-start point."""

        if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
            raise ValueError("XHR pacing deadline must be numeric")
        if not callable(starter):
            raise ValueError("XHR pacing starter must be callable")

        latest_safe_start = float(deadline) - self._execution_margin_seconds
        lock_wait = latest_safe_start - float(self._monotonic())
        if lock_wait <= 0 or not self._lock.acquire(timeout=lock_wait):
            raise XhrEndpointError(
                "Timed out waiting for the global WhoScored source pace.",
                http_status=504,
            )

        attempted = False
        try:
            while True:
                now = float(self._monotonic())
                safe_start = now
                if self._last_launch_completed is not None:
                    safe_start = max(
                        safe_start,
                        self._last_launch_completed + self._interval_seconds,
                    )
                if safe_start > latest_safe_start:
                    raise XhrEndpointError(
                        "Timed out waiting for the global WhoScored source pace.",
                        http_status=504,
                    )
                delay = safe_start - now
                if delay <= 0:
                    break
                self._sleep(delay)

            # Recheck after sleep: scheduler delay must fail closed rather than
            # launching without the fixed execution margin.
            if float(self._monotonic()) > latest_safe_start:
                raise XhrEndpointError(
                    "Timed out waiting for the global WhoScored source pace.",
                    http_status=504,
                )
            attempted = True
            return starter()
        finally:
            if attempted:
                # This conservative anchor is later than the synchronous JS
                # fetch invocation, so every subsequent actual start is at
                # least the fixed interval apart even if this command was late.
                self._last_launch_completed = float(self._monotonic())
            self._lock.release()


_XHR_START_PACER = _XhrStartPacer()

PINNED_FLARESOLVERR_VERSION = "3.4.6"
# Read once and fail startup if the exact mounted helper cannot identify
# itself. Capacity clients compare this frozen process identity with the host
# file on every cleanup poll, catching a changed mount without a restart.
EXTENSION_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
if re.fullmatch(r"[0-9a-f]{64}", EXTENSION_SHA256) is None:
    raise RuntimeError("Could not establish the FlareSolverr extension identity.")
# Exact ``inspect.getsource(flaresolverr_service._evil_logic)`` digest from
# ghcr.io/flaresolverr/flaresolverr:v3.4.6.  The wrapper relies on navigation
# going through ``driver.get`` after the stock CDP blocklist call, so source
# drift must stop startup rather than weaken the resource policy.
_UPSTREAM_EVIL_LOGIC_SHA256 = (
    "b638d94bad18e6d67022865d9bcecfe07aa4bb4e03cb6129b2157dda9462e24b"
)
# Exact ``inspect.getsource(sessions.SessionsStorage)`` digest from the same
# image.  The lifecycle proxy below depends on create registering only after
# ``get_webdriver`` and destroy removing the session before ``driver.quit``.
_UPSTREAM_SESSIONS_STORAGE_SHA256 = (
    "c1818d4525aa0642820311a636b996baa6004c2ab9464b22081c0b1a71afc5cd"
)
# The safe controller preserves this exact v3.4.6 response/error contract while
# removing request and response DTOs from logs.
_UPSTREAM_CONTROLLER_V1_SHA256 = (
    "343f1dcf39ef7fcd684a6cc152e828469d8e25e7ec94faa463cf1ee4edcba69d"
)
_MEDIA_PATCH_MARKER = "_whoscored_disable_media_extension"
_STORAGE_PROXY_MARKER = "_whoscored_capacity_lifecycle_proxy"
_SAFE_CONTROLLER_MARKER = "_whoscored_safe_v1_controller"
_SAFE_LOG_FACTORY_MARKER = "_whoscored_safe_log_factory"
_SENSITIVE_VARIANT_DEPTH = 2
_MAX_SENSITIVE_VARIANTS_PER_VALUE = 64
_CAPACITY_OWNER_RE = re.compile(r"\A[a-z0-9]{16,32}\Z")
_CAPACITY_SESSION_RE = re.compile(r"\Aws-cap-([a-z0-9]{16,32})-(?=.)")
_LOG_URL_RE = re.compile(r"(?:https?|socks[45]?)://\S+")
_LOG_SESSION_RE = re.compile(r"\bws-[A-Za-z0-9][A-Za-z0-9_-]{0,96}\b")
_AUDIO_VIDEO_EXTENSIONS = (
    "mp4",
    "webm",
    "m3u8",
    "mov",
    "m4v",
    "avi",
    "mpeg",
    "mpg",
    "ogv",
    "mp3",
    "wav",
    "ogg",
    "aac",
    "m4a",
    "flac",
)
AUDIO_VIDEO_BLOCK_PATTERNS = tuple(
    pattern
    for extension in _AUDIO_VIDEO_EXTENSIONS
    for pattern in (f"*.{extension}", f"*.{extension.upper()}")
)


class DisableMediaPatchError(RuntimeError):
    """The pinned upstream disableMedia contract cannot be extended safely."""


class _DisableMediaDriverProxy:
    """Append fixed media patterns and forbid navigation until CDP succeeds."""

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self.blocklist_applied = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._driver, name)

    def execute_cdp_cmd(self, command: str, params: Any) -> Any:
        if command != "Network.setBlockedURLs":
            return self._driver.execute_cdp_cmd(command, params)
        if not isinstance(params, Mapping):
            raise DisableMediaPatchError(
                "Upstream Network.setBlockedURLs parameters are not an object."
            )
        urls = params.get("urls")
        if not isinstance(urls, list) or not all(
            isinstance(item, str) and item for item in urls
        ):
            raise DisableMediaPatchError(
                "Upstream Network.setBlockedURLs has no string URL list."
            )
        extended = list(urls)
        known = set(extended)
        extended.extend(
            pattern for pattern in AUDIO_VIDEO_BLOCK_PATTERNS if pattern not in known
        )
        forwarded = dict(params)
        forwarded["urls"] = extended
        result = self._driver.execute_cdp_cmd(command, forwarded)
        self.blocklist_applied = True
        return result

    def get(self, url: str) -> Any:
        if not self.blocklist_applied:
            raise DisableMediaPatchError(
                "Refusing browser navigation before the extended disableMedia "
                "blocklist is active."
            )
        return self._driver.get(url)


def _disable_media_enabled(req: Any, upstream_service: Any) -> bool:
    utils_module = getattr(upstream_service, "utils", None)
    getter = getattr(utils_module, "get_config_disable_media", None)
    if not callable(getter):
        raise DisableMediaPatchError(
            "Upstream disableMedia configuration getter is unavailable."
        )
    configured = getter()
    if not isinstance(configured, bool):
        raise DisableMediaPatchError(
            "Upstream disableMedia configuration is not boolean."
        )
    override = getattr(req, "disableMedia", None)
    if override is None:
        return configured
    if not isinstance(override, bool):
        raise DisableMediaPatchError("Request disableMedia value is not boolean.")
    return override


def _install_disable_media_extension(
    upstream_service: Any,
    *,
    version: str,
) -> None:
    """Install the exact 3.4.6 ``_evil_logic`` resource-blocking wrapper.

    This is intentionally a server-side patch with no request/API surface. It
    preserves the stock images/stylesheets/fonts list and appends only fixed
    audio/video suffixes. Any incompatible version, source, signature, config,
    CDP call, or navigation path fails closed.
    """

    if version != PINNED_FLARESOLVERR_VERSION:
        raise DisableMediaPatchError(
            f"Unsupported FlareSolverr version {version!r}; expected "
            f"{PINNED_FLARESOLVERR_VERSION!r}."
        )
    original = getattr(upstream_service, "_evil_logic", None)
    if not callable(original):
        raise DisableMediaPatchError("Upstream _evil_logic is unavailable.")
    marker = getattr(original, _MEDIA_PATCH_MARKER, None)
    expected_marker = (
        PINNED_FLARESOLVERR_VERSION,
        _UPSTREAM_EVIL_LOGIC_SHA256,
    )
    if marker is not None:
        if marker != expected_marker:
            raise DisableMediaPatchError(
                "An incompatible disableMedia patch is already installed."
            )
        return

    try:
        source = inspect.getsource(original)
        signature = inspect.signature(original)
    except (OSError, TypeError, ValueError) as exc:
        raise DisableMediaPatchError("Could not inspect upstream _evil_logic.") from exc
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    if digest != _UPSTREAM_EVIL_LOGIC_SHA256:
        raise DisableMediaPatchError(
            "Upstream _evil_logic source does not match pinned FlareSolverr 3.4.6."
        )
    parameters = tuple(signature.parameters.values())
    if tuple(parameter.name for parameter in parameters) != (
        "req",
        "driver",
        "method",
    ) or any(
        parameter.kind
        not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
        for parameter in parameters
    ):
        raise DisableMediaPatchError(
            "Upstream _evil_logic signature does not match the pinned contract."
        )

    @functools.wraps(original)
    def extended_evil_logic(req: Any, driver: Any, method: str) -> Any:
        if not _disable_media_enabled(req, upstream_service):
            return original(req, driver, method)
        proxy = _DisableMediaDriverProxy(driver)
        result = original(req, proxy, method)
        if not proxy.blocklist_applied:
            raise DisableMediaPatchError(
                "Upstream disableMedia flow returned without applying its blocklist."
            )
        return result

    setattr(extended_evil_logic, _MEDIA_PATCH_MARKER, expected_marker)
    upstream_service._evil_logic = extended_evil_logic


class CapacitySessionLifecycleError(RuntimeError):
    """The pinned capacity-session lifecycle contract cannot be guaranteed."""


@dataclass
class _OwnerLifecycleState:
    active: set[str] = field(default_factory=set)
    pending_create: set[str] = field(default_factory=set)
    pending_destroy: set[str] = field(default_factory=set)
    failed_create: set[str] = field(default_factory=set)
    failed_destroy: set[str] = field(default_factory=set)
    failure_generation: int = 0


class _SensitiveLogValues:
    """Reference-count secrets only while their request is being handled."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._pattern: re.Pattern[str] | None = None

    @contextmanager
    def scope(self, *values: Any):
        variants = {
            variant
            for value in values
            if isinstance(value, str) and value
            for variant in _sensitive_log_variants(value)
        }
        if variants:
            with self._lock:
                for variant in variants:
                    self._counts[variant] = self._counts.get(variant, 0) + 1
                self._pattern = None
        try:
            yield
        finally:
            if variants:
                with self._lock:
                    for variant in variants:
                        remaining = self._counts[variant] - 1
                        if remaining:
                            self._counts[variant] = remaining
                        else:
                            self._counts.pop(variant)
                    self._pattern = None

    def snapshot(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._counts, key=len, reverse=True))

    def redact(self, text: str) -> str:
        with self._lock:
            if not self._counts:
                return text
            if self._pattern is None:
                self._pattern = re.compile(
                    "|".join(
                        re.escape(variant)
                        for variant in sorted(self._counts, key=len, reverse=True)
                    )
                )
            pattern = self._pattern
        return pattern.sub("<redacted>", text)


_SENSITIVE_LOG_VALUES = _SensitiveLogValues()


def _capacity_owner(session_id: Any) -> str | None:
    if not isinstance(session_id, str):
        return None
    match = _CAPACITY_SESSION_RE.match(session_id)
    return match.group(1) if match is not None else None


def _sensitive_log_variants(value: str) -> set[str]:
    """Return a bounded two-level closure of common log serialisations."""

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


@contextmanager
def _sensitive_request_scope(req: Any):
    """Keep every request-controlled secret redacted for this request only."""

    values = [getattr(req, "session", None), getattr(req, "url", None)]
    proxy = getattr(req, "proxy", None)
    if isinstance(proxy, Mapping):
        values.extend(proxy.get(key) for key in ("url", "username", "password"))
    with _SENSITIVE_LOG_VALUES.scope(*values):
        yield


def _redact_log_text(value: Any) -> str:
    try:
        text = str(value)
    except Exception:
        return "<unavailable-log-message>"
    text = _SENSITIVE_LOG_VALUES.redact(text)
    text = _LOG_URL_RE.sub("<redacted-url>", text)
    text = _LOG_SESSION_RE.sub("<redacted-session>", text)
    return text


def _install_safe_logging() -> None:
    """Sanitise every future LogRecord, including records from dependencies."""

    original_factory = logging.getLogRecordFactory()
    marker = getattr(original_factory, _SAFE_LOG_FACTORY_MARKER, None)
    expected_marker = (PINNED_FLARESOLVERR_VERSION,)
    if marker is not None:
        if marker != expected_marker:
            raise CapacitySessionLifecycleError(
                "An incompatible safe logging factory is already installed."
            )
        return

    def safe_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = original_factory(*args, **kwargs)
        try:
            rendered = record.getMessage()
        except Exception:
            rendered = "<unavailable-log-message>"
        record.msg = _redact_log_text(rendered)
        record.args = ()
        # Tracebacks and stack strings can repeat exception messages containing
        # URLs or credentials after the message itself has been sanitised.
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return record

    setattr(safe_factory, _SAFE_LOG_FACTORY_MARKER, expected_marker)
    logging.setLogRecordFactory(safe_factory)


def _install_safe_v1_controller(upstream_service: Any, *, version: str) -> None:
    """Replace the pinned upstream controller's unsafe DTO logging."""

    if version != PINNED_FLARESOLVERR_VERSION:
        raise CapacitySessionLifecycleError(
            f"Unsupported FlareSolverr version {version!r}; expected "
            f"{PINNED_FLARESOLVERR_VERSION!r}."
        )
    original = getattr(upstream_service, "controller_v1_endpoint", None)
    if not callable(original):
        raise CapacitySessionLifecycleError("Upstream /v1 controller is unavailable.")
    marker = getattr(original, _SAFE_CONTROLLER_MARKER, None)
    expected_marker = (
        PINNED_FLARESOLVERR_VERSION,
        _UPSTREAM_CONTROLLER_V1_SHA256,
    )
    if marker is not None:
        if marker != expected_marker:
            raise CapacitySessionLifecycleError(
                "An incompatible safe /v1 controller is already installed."
            )
        return

    try:
        source = inspect.getsource(original)
        signature = inspect.signature(original)
    except (OSError, TypeError, ValueError) as exc:
        raise CapacitySessionLifecycleError(
            "Could not inspect upstream /v1 controller."
        ) from exc
    if hashlib.sha256(source.encode("utf-8")).hexdigest() != (
        _UPSTREAM_CONTROLLER_V1_SHA256
    ):
        raise CapacitySessionLifecycleError(
            "Upstream /v1 controller does not match pinned FlareSolverr 3.4.6."
        )
    parameters = tuple(signature.parameters.values())
    if len(parameters) != 1 or parameters[0].name != "req":
        raise CapacitySessionLifecycleError(
            "Upstream /v1 controller signature does not match the pinned contract."
        )
    handler = getattr(upstream_service, "_controller_v1_handler", None)
    response_type = getattr(upstream_service, "V1ResponseBase", None)
    status_error = getattr(upstream_service, "STATUS_ERROR", None)
    utils_module = getattr(upstream_service, "utils", None)
    if (
        not callable(handler)
        or not callable(response_type)
        or status_error is None
        or not callable(getattr(utils_module, "get_flaresolverr_version", None))
    ):
        raise CapacitySessionLifecycleError(
            "Pinned upstream /v1 controller dependencies are unavailable."
        )

    @functools.wraps(original)
    def safe_controller(req: Any) -> Any:
        with _sensitive_request_scope(req):
            start_ts = int(time.time() * 1_000)
            logging.info("Incoming request => POST /v1")
            try:
                result = handler(req)
            except Exception as exc:
                result = response_type({})
                result.__error_500__ = True
                result.status = status_error
                # Preserve the exact upstream API response while keeping the
                # value out of every server log record.
                result.message = "Error: " + str(exc)
                logging.error("FlareSolverr /v1 request failed")
            result.startTimestamp = start_ts
            result.endTimestamp = int(time.time() * 1_000)
            result.version = utils_module.get_flaresolverr_version()
            logging.info(
                "Response in %s s",
                (result.endTimestamp - result.startTimestamp) / 1_000,
            )
            return result

    setattr(safe_controller, _SAFE_CONTROLLER_MARKER, expected_marker)
    upstream_service.controller_v1_endpoint = safe_controller


class _TrackingSessionsStorage:
    """Thread-safe owner lifecycle tracking around pinned SessionsStorage."""

    def __init__(
        self,
        delegate: Any,
        *,
        platform_version_getter: Any,
        execution_locks: Any | None = None,
    ) -> None:
        self._delegate = delegate
        self._platform_version_getter = platform_version_getter
        # Resolve the process registry at construction time (after module
        # import) so lifecycle cleanup and paced XHR execution use the exact
        # same per-session lease.
        self._execution_locks = execution_locks or _SESSION_LOCKS
        self._state_lock = threading.RLock()
        self._session_locks_guard = threading.Lock()
        self._session_locks: dict[str, threading.RLock] = {}
        self._states: dict[str, _OwnerLifecycleState] = {}
        self._retained_destroy: dict[str, Any] = {}
        self._scheduled_destroy: set[str] = set()
        setattr(
            self,
            _STORAGE_PROXY_MARKER,
            (PINNED_FLARESOLVERR_VERSION, _UPSTREAM_SESSIONS_STORAGE_SHA256),
        )
        for session_id in delegate.session_ids():
            owner = _capacity_owner(session_id)
            if owner is not None:
                self._state(owner).active.add(session_id)

    @property
    def sessions(self) -> Any:
        return self._delegate.sessions

    @property
    def execution_locks(self) -> Any:
        return self._execution_locks

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def _state(self, owner: str) -> _OwnerLifecycleState:
        with self._state_lock:
            return self._states.setdefault(owner, _OwnerLifecycleState())

    def _session_lock(self, session_id: str) -> threading.RLock:
        with self._session_locks_guard:
            return self._session_locks.setdefault(session_id, threading.RLock())

    def exists(self, session_id: str) -> bool:
        return self._delegate.exists(session_id)

    def session_ids(self) -> list[str]:
        return self._delegate.session_ids()

    def create(
        self,
        session_id: str | None = None,
        proxy: dict[str, Any] | None = None,
        force_new: bool | None = False,
    ) -> tuple[Any, bool]:
        owner = _capacity_owner(session_id)
        if session_id is None:
            return self._delegate.create(session_id, proxy, force_new)
        if owner is None:
            # Named ordinary sessions use the same re-entrant lifecycle lock.
            # In particular, never let delegate.create(force_new=True) call
            # delegate.destroy directly and bypass the active-XHR lease.
            with self._session_lock(session_id):
                if force_new:
                    self.destroy(session_id)
                if self._delegate.exists(session_id):
                    return self._delegate.sessions[session_id], False
                return self._delegate.create(session_id, proxy, False)
        with self._session_lock(session_id):
            if force_new:
                self.destroy(session_id)
            if self._delegate.exists(session_id):
                session = self._delegate.sessions[session_id]
                with self._state_lock:
                    self._state(owner).active.add(session_id)
                return session, False
            with self._state_lock:
                state = self._state(owner)
                if session_id in state.failed_destroy:
                    state.failed_create.add(session_id)
                    state.failure_generation += 1
                    raise CapacitySessionLifecycleError(
                        "A capacity session is still waiting for destroy retry."
                    )
                state.pending_create.add(session_id)
            try:
                session, fresh = self._delegate.create(session_id, proxy, False)
                if self._delegate.sessions.get(session_id) is not session:
                    raise CapacitySessionLifecycleError(
                        "Upstream create returned before session registration."
                    )
            except BaseException:
                with self._state_lock:
                    state = self._state(owner)
                    state.pending_create.discard(session_id)
                    state.failed_create.add(session_id)
                    if self._delegate.sessions.get(session_id) is not None:
                        state.active.add(session_id)
                    state.failure_generation += 1
                raise
            with self._state_lock:
                state = self._state(owner)
                state.pending_create.discard(session_id)
                state.failed_create.discard(session_id)
                state.active.add(session_id)
            return session, fresh

    def destroy(self, session_id: str) -> bool:
        owner = _capacity_owner(session_id)
        if owner is None:
            with self._session_lock(session_id):
                with self._execution_locks.acquire(session_id, self, None):
                    return self._delegate.destroy(session_id)
        with self._session_lock(session_id):
            # Wait for any active launch/collection before removing the
            # session from upstream storage or calling close/quit.  Cleanup is
            # scheduled on a daemon thread, so this does not block its HTTP
            # acknowledgement.
            with self._execution_locks.acquire(session_id, self, None):
                with self._state_lock:
                    state = self._state(owner)
                    session = self._retained_destroy.get(session_id)
                    if session is None:
                        session = self._delegate.sessions.pop(session_id, None)
                    if session is None:
                        state.active.discard(session_id)
                        state.pending_destroy.discard(session_id)
                        return False
                    state.active.discard(session_id)
                    state.failed_destroy.discard(session_id)
                    state.pending_destroy.add(session_id)
                    self._retained_destroy[session_id] = session
                try:
                    if self._platform_version_getter() == "nt":
                        session.driver.close()
                    session.driver.quit()
                except BaseException:
                    with self._state_lock:
                        state = self._state(owner)
                        state.pending_destroy.discard(session_id)
                        state.failed_destroy.add(session_id)
                        state.failure_generation += 1
                    raise
                with self._state_lock:
                    state = self._state(owner)
                    state.pending_destroy.discard(session_id)
                    state.failed_destroy.discard(session_id)
                    self._retained_destroy.pop(session_id, None)
                return True

    def get(self, session_id: str, ttl: Any = None) -> tuple[Any, bool]:
        session, fresh = self.create(session_id)
        if ttl is not None and not fresh and session.lifetime() > ttl:
            session, fresh = self.create(session_id, force_new=True)
        return session, fresh

    def _start_cleanup_threads(self, targets: list[str]) -> None:
        def destroy_one(session_id: str) -> None:
            try:
                self.destroy(session_id)
            except BaseException:
                logging.warning("Capacity browser session cleanup failed")
            finally:
                with self._state_lock:
                    self._scheduled_destroy.discard(session_id)

        for session_id in targets:
            threading.Thread(
                target=destroy_one,
                args=(session_id,),
                name="capacity-session-cleanup",
                daemon=True,
            ).start()

    def schedule_owner_cleanup(self, owner: str) -> bool:
        """Start daemon retries without waiting for WebDriver close/quit."""

        with self._state_lock:
            state = self._state(owner)
            targets = sorted(
                (state.active | state.failed_destroy) - self._scheduled_destroy
            )
            self._scheduled_destroy.update(targets)
        self._start_cleanup_threads(targets)
        with self._state_lock:
            return bool(targets or self._scheduled_destroy)

    def snapshot_then_schedule_owner_cleanup(self, owner: str) -> dict[str, int | bool]:
        """Freeze evidence before any cleanup attempt can change generation."""

        with self._state_lock:
            state = self._state(owner)
            snapshot = self._owner_snapshot_locked(owner, state)
            targets = sorted(
                (state.active | state.failed_destroy) - self._scheduled_destroy
            )
            self._scheduled_destroy.update(targets)
        self._start_cleanup_threads(targets)
        return snapshot

    def _owner_snapshot_locked(
        self, owner: str, state: _OwnerLifecycleState
    ) -> dict[str, int | bool]:
        scheduled = any(
            _capacity_owner(session_id) == owner
            for session_id in self._scheduled_destroy
        )
        return {
            "active": len(state.active),
            "pending_create": len(state.pending_create),
            "pending_destroy": len(state.pending_destroy),
            "failed_create": len(state.failed_create),
            "failed_destroy": len(state.failed_destroy),
            "failure_generation": state.failure_generation,
            "cleanup_scheduled": scheduled,
        }

    def owner_snapshot(self, owner: str) -> dict[str, int | bool]:
        with self._state_lock:
            state = self._state(owner)
            return self._owner_snapshot_locked(owner, state)


def _install_capacity_session_tracking(
    upstream_service: Any,
    *,
    version: str,
) -> _TrackingSessionsStorage:
    """Install the exact v3.4.6 storage lifecycle proxy idempotently."""

    if version != PINNED_FLARESOLVERR_VERSION:
        raise CapacitySessionLifecycleError(
            f"Unsupported FlareSolverr version {version!r}; expected "
            f"{PINNED_FLARESOLVERR_VERSION!r}."
        )
    storage = getattr(upstream_service, "SESSIONS_STORAGE", None)
    marker = getattr(storage, _STORAGE_PROXY_MARKER, None)
    expected_marker = (
        PINNED_FLARESOLVERR_VERSION,
        _UPSTREAM_SESSIONS_STORAGE_SHA256,
    )
    if marker is not None:
        if marker != expected_marker or not isinstance(
            storage, _TrackingSessionsStorage
        ):
            raise CapacitySessionLifecycleError(
                "An incompatible capacity lifecycle proxy is already installed."
            )
        return storage
    if storage is None:
        raise CapacitySessionLifecycleError("Upstream session storage is unavailable.")
    try:
        source = inspect.getsource(type(storage))
    except (OSError, TypeError) as exc:
        raise CapacitySessionLifecycleError(
            "Could not inspect upstream SessionsStorage."
        ) from exc
    if hashlib.sha256(source.encode("utf-8")).hexdigest() != (
        _UPSTREAM_SESSIONS_STORAGE_SHA256
    ):
        raise CapacitySessionLifecycleError(
            "Upstream SessionsStorage does not match pinned FlareSolverr 3.4.6."
        )
    required = ("sessions", "exists", "session_ids", "get", "create", "destroy")
    if any(not hasattr(storage, name) for name in required):
        raise CapacitySessionLifecycleError(
            "Pinned upstream SessionsStorage interface is incomplete."
        )
    utils_module = getattr(upstream_service, "utils", None)
    if utils_module is None or not hasattr(utils_module, "PLATFORM_VERSION"):
        raise CapacitySessionLifecycleError(
            "Upstream platform state is unavailable for safe session destroy."
        )
    proxy = _TrackingSessionsStorage(
        storage,
        platform_version_getter=lambda: utils_module.PLATFORM_VERSION,
    )
    upstream_service.SESSIONS_STORAGE = proxy
    return proxy


_CAPACITY_CLEANUP_RESPONSE_FIELDS = frozenset(
    {
        "status",
        "version",
        "extension_sha256",
        "active",
        "pending_create",
        "pending_destroy",
        "failed_create",
        "failed_destroy",
        "failure_generation",
        "cleanup_scheduled",
    }
)


def _capacity_cleanup_response(
    *, status: str, snapshot: Mapping[str, int | bool] | None = None
) -> dict[str, Any]:
    state = snapshot or {}
    return {
        "status": status,
        "version": PINNED_FLARESOLVERR_VERSION,
        "extension_sha256": EXTENSION_SHA256,
        "active": int(state.get("active", 0)),
        "pending_create": int(state.get("pending_create", 0)),
        "pending_destroy": int(state.get("pending_destroy", 0)),
        "failed_create": int(state.get("failed_create", 0)),
        "failed_destroy": int(state.get("failed_destroy", 0)),
        "failure_generation": int(state.get("failure_generation", 0)),
        "cleanup_scheduled": bool(state.get("cleanup_scheduled", False)),
    }


def handle_capacity_session_cleanup(
    payload: Any,
    *,
    storage: Any,
) -> tuple[dict[str, Any], int]:
    """Validate, schedule exact-owner cleanup, and return a secret-free snapshot."""

    if type(payload) is not dict or set(payload) != {"owner"}:
        return _capacity_cleanup_response(status="error"), 400
    owner = payload.get("owner")
    if type(owner) is not str or _CAPACITY_OWNER_RE.fullmatch(owner) is None:
        return _capacity_cleanup_response(status="error"), 400
    if not isinstance(storage, _TrackingSessionsStorage):
        return _capacity_cleanup_response(status="error"), 503
    try:
        snapshot = storage.snapshot_then_schedule_owner_cleanup(owner)
        # Protocol acknowledgement, not a count of live worker threads.  A
        # valid cleanup poll is successfully scheduled even when the exact
        # owner already has zero sessions.
        snapshot["cleanup_scheduled"] = True
    except Exception:
        logging.error("Capacity browser session cleanup scheduling failed")
        return _capacity_cleanup_response(status="error"), 500
    return _capacity_cleanup_response(status="ok", snapshot=snapshot), 200


class XhrEndpointError(Exception):
    """A safe error that can be returned by the HTTP endpoint."""

    def __init__(self, message: str, *, http_status: int = 400) -> None:
        super().__init__(message)
        self.http_status = http_status


@dataclass(frozen=True)
class XhrRequest:
    url: str
    session: str
    timeout_ms: int


@dataclass(frozen=True)
class XhrBatchRequest:
    urls: tuple[str, ...]
    session: str
    timeout_ms: int


def _validate_whoscored_feed_url(value: Any) -> str:
    """Return a canonical, narrowly allow-listed WhoScored feed URL."""

    if not isinstance(value, str) or not value or len(value) > 8_192:
        raise XhrEndpointError("Request parameter 'url' must be a non-empty string.")
    if _CONTROL_OR_SPACE_RE.search(value) or "\\" in value:
        raise XhrEndpointError("Request parameter 'url' contains forbidden characters.")

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise XhrEndpointError("Request parameter 'url' is malformed.") from exc

    if parsed.scheme != "https":
        raise XhrEndpointError("Only HTTPS WhoScored feed URLs are allowed.")
    if parsed.username is not None or parsed.password is not None:
        raise XhrEndpointError("URL credentials are forbidden.")
    if port is not None:
        raise XhrEndpointError("Explicit URL ports are forbidden.")
    if parsed.netloc != "www.whoscored.com" or parsed.hostname != "www.whoscored.com":
        raise XhrEndpointError("Only www.whoscored.com is allowed.")
    if parsed.fragment:
        raise XhrEndpointError("URL fragments are forbidden.")
    if _FEED_PATH_RE.fullmatch(parsed.path) is None or "//" in parsed.path:
        raise XhrEndpointError("The URL path contains forbidden characters.")
    if not any(pattern.fullmatch(parsed.path) for pattern in ALLOWED_PATH_PATTERNS):
        raise XhrEndpointError("The URL path is not an allowed WhoScored feed.")

    # Preserve the exact query string: it carries the source's feed filters.
    # The strict netloc/path checks above and browser-side same-origin mode keep
    # it from becoming an SSRF or an open redirect primitive.
    return value


def _validate_payload(value: Any) -> XhrRequest:
    if not isinstance(value, Mapping):
        raise XhrEndpointError("A JSON object request body is required.")

    fields = set(value)
    extra = fields - _PAYLOAD_FIELDS
    missing = {"url", "session"} - fields
    if extra:
        raise XhrEndpointError(
            "Unsupported request parameter(s): "
            + ", ".join(sorted(map(str, extra)))
            + "."
        )
    if missing:
        raise XhrEndpointError(
            "Missing request parameter(s): " + ", ".join(sorted(missing)) + "."
        )

    session = value.get("session")
    if not isinstance(session, str) or _SESSION_RE.fullmatch(session) is None:
        raise XhrEndpointError(
            "Request parameter 'session' must be an existing ws-* session ID."
        )

    timeout_ms = value.get("maxTimeout", DEFAULT_TIMEOUT_MS)
    if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int):
        raise XhrEndpointError("Request parameter 'maxTimeout' must be an integer.")
    if not MIN_TIMEOUT_MS <= timeout_ms <= MAX_TIMEOUT_MS:
        raise XhrEndpointError(
            f"Request parameter 'maxTimeout' must be between {MIN_TIMEOUT_MS} "
            f"and {MAX_TIMEOUT_MS} milliseconds."
        )

    return XhrRequest(
        url=_validate_whoscored_feed_url(value.get("url")),
        session=session,
        timeout_ms=timeout_ms,
    )


def _validate_batch_payload(value: Any) -> XhrBatchRequest:
    if not isinstance(value, Mapping):
        raise XhrEndpointError("A JSON object request body is required.")

    fields = set(value)
    extra = fields - _BATCH_PAYLOAD_FIELDS
    missing = {"urls", "session"} - fields
    if extra:
        raise XhrEndpointError(
            "Unsupported request parameter(s): "
            + ", ".join(sorted(map(str, extra)))
            + "."
        )
    if missing:
        raise XhrEndpointError(
            "Missing request parameter(s): " + ", ".join(sorted(missing)) + "."
        )

    session = value.get("session")
    if not isinstance(session, str) or _SESSION_RE.fullmatch(session) is None:
        raise XhrEndpointError(
            "Request parameter 'session' must be an existing ws-* session ID."
        )
    raw_urls = value.get("urls")
    if not isinstance(raw_urls, list) or not 1 <= len(raw_urls) <= MAX_BATCH_URLS:
        raise XhrEndpointError(
            f"Request parameter 'urls' must contain 1 to {MAX_BATCH_URLS} URLs."
        )
    urls = tuple(_validate_whoscored_feed_url(url) for url in raw_urls)
    if len(set(urls)) != len(urls):
        raise XhrEndpointError("Request parameter 'urls' contains duplicates.")

    timeout_ms = value.get("maxTimeout", DEFAULT_TIMEOUT_MS)
    if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int):
        raise XhrEndpointError("Request parameter 'maxTimeout' must be an integer.")
    if not MIN_TIMEOUT_MS <= timeout_ms <= MAX_TIMEOUT_MS:
        raise XhrEndpointError(
            f"Request parameter 'maxTimeout' must be between {MIN_TIMEOUT_MS} "
            f"and {MAX_TIMEOUT_MS} milliseconds."
        )
    return XhrBatchRequest(urls=urls, session=session, timeout_ms=timeout_ms)


# URL, limits, deadline, and opaque operation identifiers are trusted server
# arguments, never interpolated source and never HTTP fields.  This synchronous
# fixed script invokes ``fetch`` before returning its acknowledgement, allowing
# Python to hold the process-global pacing lock across the actual browser start.
# The response Promise remains in a per-page private registry for collection by
# a second fixed script after the global lock has been released.
XHR_SCRIPT = r"""
const targetUrl = arguments[0];
const maxBytesPerResponse = arguments[1];
const maxAggregateBytes = arguments[2];
const deadlineEpochMs = arguments[3];
const minimumExecutionMarginMs = arguments[4];
const operationKey = arguments[5];
const itemIndex = arguments[6];
const registryProperty = "__whoscoredRestrictedXhrV1";
const allowedPaths = [
  /^\/statisticsfeed\/1\/get(?:team|player)statistics$/,
  /^\/stagestatfeed\/[1-9][0-9]*\/stageteams\/$/
];
const failure = (kind, started = false) => ({
  ok: false,
  started,
  kind,
  error: kind === "timeout" ? "fetch_timeout" : kind
});

try {
  const requested = new URL(targetUrl);
  if (requested.origin !== "https://www.whoscored.com") {
    throw new Error("forbidden_origin");
  }
  if (!allowedPaths.some((pattern) => pattern.test(requested.pathname))) {
    throw new Error("forbidden_path");
  }
  const siteConfig = window.require && window.require.config &&
    window.require.config.params && window.require.config.params.site;
  if (!siteConfig || siteConfig.gSiteHeaderName !== "Model-last-Mode" ||
      typeof siteConfig.gSiteHeaderValue !== "string" ||
      !/^[A-Za-z0-9+/]{43}=$/.test(siteConfig.gSiteHeaderValue)) {
    return failure("source_header_unavailable");
  }
  if (!Number.isSafeInteger(deadlineEpochMs) ||
      !Number.isSafeInteger(minimumExecutionMarginMs) ||
      deadlineEpochMs - Date.now() < minimumExecutionMarginMs) {
    return failure("timeout");
  }
  if (typeof operationKey !== "string" ||
      !/^xhr-[a-f0-9]{32}$/.test(operationKey) ||
      !Number.isSafeInteger(itemIndex) || itemIndex < 0 || itemIndex >= 8) {
    throw new Error("invalid_operation");
  }

  let registry = window[registryProperty];
  if (registry === undefined) {
    registry = Object.create(null);
    Object.defineProperty(window, registryProperty, {
      value: registry,
      configurable: true
    });
  }
  if (!registry || Object.getPrototypeOf(registry) !== null) {
    throw new Error("invalid_registry");
  }
  let operation = registry[operationKey];
  if (operation === undefined) {
    operation = {
      consumedBytes: 0,
      aggregateTooLarge: false,
      controllers: new Set(),
      entries: Object.create(null),
      maxAggregateBytes
    };
    registry[operationKey] = operation;
  }
  if (operation.maxAggregateBytes !== maxAggregateBytes ||
      operation.entries[itemIndex] !== undefined) {
    throw new Error("invalid_operation");
  }

  const controller = new AbortController();
  let deadlineExpired = false;
  let reader = null;
  let itemBytes = 0;
  const timer = setTimeout(() => {
    deadlineExpired = true;
    controller.abort();
  }, Math.max(1, deadlineEpochMs - Date.now()));
  operation.controllers.add(controller);

  // This is the globally arbitrated event.  There is no await, timer, queue,
  // or caller-controlled pacing value between the final deadline check and
  // the fixed fetch invocation.
  let responsePromise;
  try {
    if (deadlineEpochMs - Date.now() < minimumExecutionMarginMs) {
      clearTimeout(timer);
      operation.controllers.delete(controller);
      return failure("timeout");
    }
    responsePromise = fetch(requested.href, {
      method: "GET",
      credentials: "same-origin",
      mode: "cors",
      // Never follow redirects: an allow-list check after following is too
      // late to prevent browser egress outside the fixed source origin.
      redirect: "error",
      headers: {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Model-last-Mode": siteConfig.gSiteHeaderValue
      },
      signal: controller.signal
    });
  } catch (error) {
    responsePromise = Promise.reject(error);
  }

  operation.entries[itemIndex] = (async () => {
    try {
      const response = await responsePromise;
      const finalUrl = new URL(response.url);
      if (finalUrl.origin !== "https://www.whoscored.com") {
        throw new Error("forbidden_final_origin");
      }
      if (!allowedPaths.some((pattern) => pattern.test(finalUrl.pathname))) {
        throw new Error("forbidden_final_path");
      }

      const chunks = [];
      if (response.body && response.body.getReader) {
        reader = response.body.getReader();
        while (true) {
          const item = await reader.read();
          if (item.done) break;
          itemBytes += item.value.byteLength;
          operation.consumedBytes += item.value.byteLength;
          if (operation.consumedBytes > maxAggregateBytes) {
            operation.aggregateTooLarge = true;
            for (const activeController of operation.controllers) {
              activeController.abort();
            }
            throw new Error("aggregate_too_large");
          }
          if (itemBytes > maxBytesPerResponse) {
            await reader.cancel("response_too_large");
            throw new Error("response_too_large");
          }
          chunks.push(item.value);
        }
      } else {
        throw new Error("response_stream_unavailable");
      }

      const body = new Uint8Array(itemBytes);
      let offset = 0;
      for (const chunk of chunks) {
        body.set(chunk, offset);
        offset += chunk.byteLength;
      }
      let binary = "";
      const encodeChunk = 0x8000;
      for (let position = 0; position < body.length; position += encodeChunk) {
        binary += String.fromCharCode.apply(
          null,
          body.subarray(position, Math.min(position + encodeChunk, body.length))
        );
      }
      return {
        ok: true,
        requestedUrl: targetUrl,
        finalUrl: finalUrl.href,
        status: response.status,
        headers: Object.fromEntries(response.headers.entries()),
        bodyBase64: btoa(binary),
        responseBytes: itemBytes
      };
    } catch (error) {
      const message = error && error.message;
      const aborted = error && error.name === "AbortError";
      const kind = operation.aggregateTooLarge || message === "aggregate_too_large" ?
        "aggregate_too_large" :
        (message === "response_too_large" ? "response_too_large" :
        ((message === "forbidden_final_origin" ||
          message === "forbidden_final_path") ? "source_redirect_rejected" :
         (aborted && deadlineExpired ? "timeout" : "fetch_failed")));
      return {
        ok: false,
        requestedUrl: targetUrl,
        kind,
        error: kind === "timeout" ? "fetch_timeout" : kind
      };
    } finally {
      clearTimeout(timer);
      operation.controllers.delete(controller);
      if (reader) {
        try { reader.releaseLock(); } catch (_) {}
      }
    }
  })();
  return {ok: true, started: true, itemIndex};
} catch (error) {
  return failure("fetch_failed");
}
""".strip()


# Batch launches use the same immutable start script.  Python sends at most
# four starts before collecting that wave, preserving the documented browser
# concurrency ceiling while independent sessions can overlap their I/O.
BATCH_XHR_SCRIPT = XHR_SCRIPT


XHR_COLLECT_SCRIPT = r"""
const operationKey = arguments[0];
const itemIndexes = arguments[1];
const finishOperation = arguments[2];
const done = arguments[arguments.length - 1];
const registryProperty = "__whoscoredRestrictedXhrV1";

(async () => {
  try {
    const registry = window[registryProperty];
    const operation = registry && registry[operationKey];
    if (!operation || !Array.isArray(itemIndexes)) {
      throw new Error("missing_operation");
    }
    const entries = itemIndexes.map((itemIndex) => operation.entries[itemIndex]);
    if (entries.some((entry) => !entry || typeof entry.then !== "function")) {
      throw new Error("missing_operation_item");
    }
    const responses = await Promise.all(entries);
    for (const itemIndex of itemIndexes) delete operation.entries[itemIndex];
    if (operation.aggregateTooLarge) {
      done({
        ok: false,
        kind: "aggregate_too_large",
        error: "aggregate_too_large"
      });
      return;
    }
    const responseBytes = responses.reduce(
      (total, response) => total +
        (response && response.ok === true ? response.responseBytes : 0),
      0
    );
    done({ok: true, responses, responseBytes});
  } catch (error) {
    done({ok: false, kind: "fetch_failed", error: "fetch_failed"});
  } finally {
    if (finishOperation) {
      const registry = window[registryProperty];
      if (registry) delete registry[operationKey];
    }
  }
})();
""".strip()


XHR_ABORT_SCRIPT = r"""
const operationKey = arguments[0];
const registryProperty = "__whoscoredRestrictedXhrV1";
const registry = window[registryProperty];
const operation = registry && registry[operationKey];
if (operation) {
  for (const controller of operation.controllers) controller.abort();
  delete registry[operationKey];
}
return true;
""".strip()


class _SessionLocks:
    """Serialize WebDriver calls per FlareSolverr session."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    @contextmanager
    def acquire(
        self,
        session_id: str,
        storage: Any,
        timeout_s: float | None,
    ):
        with self._guard:
            # Do not retain locks for sessions the upstream storage destroyed.
            for stale_id, stale_lock in tuple(self._locks.items()):
                if (
                    stale_id != session_id
                    and not stale_lock.locked()
                    and not storage.exists(stale_id)
                ):
                    self._locks.pop(stale_id, None)
            lock = self._locks.setdefault(session_id, threading.Lock())

        acquired = (
            lock.acquire()
            if timeout_s is None
            else lock.acquire(timeout=max(timeout_s, 0.0))
        )
        if not acquired:
            raise XhrEndpointError(
                "Timed out waiting for the WhoScored browser session.",
                http_status=504,
            )
        try:
            yield
        finally:
            lock.release()
            with self._guard:
                if not storage.exists(session_id) and not lock.locked():
                    self._locks.pop(session_id, None)


_SESSION_LOCKS = _SessionLocks()


def _execute_browser_fetch(
    driver: Any,
    request_data: XhrRequest,
    *,
    deadline: float,
    deadline_epoch_ms: int,
    pacer: _XhrStartPacer,
) -> Mapping[str, Any]:
    """Launch one paced fixed fetch, then collect its stored Promise."""

    old_timeout = driver.timeouts.script
    operation_key = f"xhr-{secrets.token_hex(16)}"
    finished = False
    try:
        acknowledgement = _launch_browser_fetch(
            driver,
            url=request_data.url,
            max_aggregate_bytes=MAX_RESPONSE_BYTES,
            deadline=deadline,
            deadline_epoch_ms=deadline_epoch_ms,
            operation_key=operation_key,
            item_index=0,
            pacer=pacer,
        )
        if acknowledgement.get("ok") is not True:
            return acknowledgement
        collected = _collect_browser_fetches(
            driver,
            operation_key=operation_key,
            item_indexes=(0,),
            finish_operation=True,
            deadline=deadline,
            timeout_message="WhoScored browser fetch timed out.",
        )
        finished = True
        if collected.get("ok") is not True:
            return collected
        responses = collected.get("responses")
        if not isinstance(responses, list) or len(responses) != 1:
            raise XhrEndpointError(
                "Browser returned an invalid XHR result.", http_status=502
            )
        result = responses[0]
    finally:
        if not finished:
            _abort_browser_fetches(driver, operation_key)
        try:
            driver.set_script_timeout(old_timeout)
        except Exception:
            logging.warning("Could not restore FlareSolverr session script timeout")

    if not isinstance(result, Mapping):
        raise XhrEndpointError(
            "Browser returned an invalid XHR result.", http_status=502
        )
    return result


def _execute_browser_batch_fetch(
    driver: Any,
    request_data: XhrBatchRequest,
    *,
    deadline: float,
    deadline_epoch_ms: int,
    pacer: _XhrStartPacer,
) -> Mapping[str, Any]:
    """Launch fixed paced fetches in waves capped at four active requests."""

    old_timeout = driver.timeouts.script
    operation_key = f"xhr-{secrets.token_hex(16)}"
    finished = False
    try:
        responses: list[Any] = []
        response_bytes = 0
        for wave_start in range(0, len(request_data.urls), BATCH_CONCURRENCY):
            wave_urls = request_data.urls[
                wave_start : wave_start + BATCH_CONCURRENCY
            ]
            wave_indexes = tuple(
                range(wave_start, wave_start + len(wave_urls))
            )
            for item_index, url in zip(wave_indexes, wave_urls):
                acknowledgement = _launch_browser_fetch(
                    driver,
                    url=url,
                    max_aggregate_bytes=MAX_BATCH_RESPONSE_BYTES,
                    deadline=deadline,
                    deadline_epoch_ms=deadline_epoch_ms,
                    operation_key=operation_key,
                    item_index=item_index,
                    pacer=pacer,
                )
                if acknowledgement.get("ok") is not True:
                    return acknowledgement

            final_wave = wave_start + len(wave_urls) == len(request_data.urls)
            collected = _collect_browser_fetches(
                driver,
                operation_key=operation_key,
                item_indexes=wave_indexes,
                finish_operation=final_wave,
                deadline=deadline,
                timeout_message="WhoScored browser batch timed out.",
            )
            if collected.get("ok") is not True:
                return collected
            wave_responses = collected.get("responses")
            wave_bytes = collected.get("responseBytes")
            if (
                not isinstance(wave_responses, list)
                or len(wave_responses) != len(wave_indexes)
                or isinstance(wave_bytes, bool)
                or not isinstance(wave_bytes, int)
                or wave_bytes < 0
            ):
                raise XhrEndpointError(
                    "Browser returned an invalid XHR batch result.", http_status=502
                )
            responses.extend(wave_responses)
            response_bytes += wave_bytes
        result = {
            "ok": True,
            "responses": responses,
            "responseBytes": response_bytes,
        }
        finished = True
    finally:
        if not finished:
            _abort_browser_fetches(driver, operation_key)
        try:
            driver.set_script_timeout(old_timeout)
        except Exception:
            logging.warning("Could not restore FlareSolverr session script timeout")

    if not isinstance(result, Mapping):
        raise XhrEndpointError(
            "Browser returned an invalid XHR batch result.", http_status=502
        )
    return result


def _launch_browser_fetch(
    driver: Any,
    *,
    url: str,
    max_aggregate_bytes: int,
    deadline: float,
    deadline_epoch_ms: int,
    operation_key: str,
    item_index: int,
    pacer: _XhrStartPacer,
) -> Mapping[str, Any]:
    """Invoke the fixed synchronous launch while holding global arbitration."""

    try:
        acknowledgement = pacer.launch(
            deadline=deadline,
            starter=lambda: driver.execute_script(
                XHR_SCRIPT,
                url,
                MAX_RESPONSE_BYTES,
                max_aggregate_bytes,
                deadline_epoch_ms,
                XHR_MIN_EXECUTION_MARGIN_MS,
                operation_key,
                item_index,
            ),
        )
    except XhrEndpointError:
        raise
    except Exception as exc:
        if "timeout" in type(exc).__name__.lower():
            raise XhrEndpointError(
                "WhoScored browser fetch timed out.", http_status=504
            ) from exc
        raise XhrEndpointError(
            "WhoScored browser fetch could not be started.", http_status=502
        ) from exc
    if not isinstance(acknowledgement, Mapping):
        raise XhrEndpointError(
            "Browser returned an invalid XHR launch result.", http_status=502
        )
    if acknowledgement.get("ok") is True and (
        acknowledgement.get("started") is not True
        or acknowledgement.get("itemIndex") != item_index
    ):
        raise XhrEndpointError(
            "Browser returned an invalid XHR launch result.", http_status=502
        )
    return acknowledgement


def _collect_browser_fetches(
    driver: Any,
    *,
    operation_key: str,
    item_indexes: tuple[int, ...],
    finish_operation: bool,
    deadline: float,
    timeout_message: str,
) -> Mapping[str, Any]:
    """Await already-started browser Promises within the absolute deadline."""

    remaining_seconds = deadline - time.monotonic()
    if remaining_seconds <= 0:
        raise XhrEndpointError(timeout_message, http_status=504)
    # Do not reset or extend the request budget at the collection phase. Every
    # underlying fetch and Selenium's callback wait share the same unchanged
    # absolute end-to-end deadline.
    driver.set_script_timeout(remaining_seconds)
    try:
        result = driver.execute_async_script(
            XHR_COLLECT_SCRIPT,
            operation_key,
            list(item_indexes),
            finish_operation,
        )
    except Exception as exc:
        if "timeout" in type(exc).__name__.lower():
            raise XhrEndpointError(timeout_message, http_status=504) from exc
        raise XhrEndpointError(
            "WhoScored browser fetch could not be collected.", http_status=502
        ) from exc
    if not isinstance(result, Mapping):
        raise XhrEndpointError(
            "Browser returned an invalid XHR collection result.", http_status=502
        )
    return result


def _abort_browser_fetches(driver: Any, operation_key: str) -> None:
    """Best-effort removal and abort of a failed fixed browser operation."""

    try:
        driver.execute_script(XHR_ABORT_SCRIPT, operation_key)
    except Exception:
        logging.warning("Could not abort failed WhoScored browser fetch operation")


def _normalise_browser_result(
    result: Mapping[str, Any], *, expected_url: str | None = None
) -> dict[str, Any]:
    if result.get("ok") is not True:
        kind = result.get("kind")
        if kind == "response_too_large":
            raise XhrEndpointError(
                f"WhoScored feed exceeds the {MAX_RESPONSE_BYTES}-byte limit.",
                http_status=413,
            )
        if kind == "aggregate_too_large":
            raise XhrEndpointError(
                f"WhoScored batch exceeds the {MAX_BATCH_RESPONSE_BYTES}-byte limit.",
                http_status=413,
            )
        if kind == "timeout":
            raise XhrEndpointError(
                "WhoScored browser fetch timed out.", http_status=504
            )
        if kind == "source_header_unavailable":
            raise XhrEndpointError(
                "WhoScored page request header is unavailable.", http_status=502
            )
        if kind == "source_redirect_rejected":
            raise XhrEndpointError(
                "WhoScored feed redirected outside the allow-list.", http_status=502
            )
        raise XhrEndpointError("WhoScored browser fetch failed.", http_status=502)

    try:
        final_url = _validate_whoscored_feed_url(result.get("finalUrl"))
    except XhrEndpointError as exc:
        raise XhrEndpointError(
            "Browser returned a forbidden final XHR URL.", http_status=502
        ) from exc
    if expected_url is not None and final_url != expected_url:
        raise XhrEndpointError(
            "Browser returned an unexpected final XHR URL.", http_status=502
        )
    encoded = result.get("bodyBase64")
    if not isinstance(encoded, str):
        raise XhrEndpointError("Browser returned an invalid XHR body.", http_status=502)
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise XhrEndpointError(
            "Browser returned an invalid base64 XHR body.", http_status=502
        ) from exc
    if len(decoded) > MAX_RESPONSE_BYTES:
        raise XhrEndpointError(
            f"WhoScored feed exceeds the {MAX_RESPONSE_BYTES}-byte limit.",
            http_status=413,
        )

    declared_size = result.get("responseBytes")
    if (
        isinstance(declared_size, bool)
        or not isinstance(declared_size, int)
        or declared_size != len(decoded)
    ):
        raise XhrEndpointError(
            "Browser XHR byte count does not match its body.", http_status=502
        )

    status = result.get("status")
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or not 100 <= status <= 599
    ):
        raise XhrEndpointError(
            "Browser returned an invalid HTTP status.", http_status=502
        )

    raw_headers = result.get("headers")
    if not isinstance(raw_headers, Mapping) or len(raw_headers) > 128:
        raise XhrEndpointError("Browser returned invalid XHR headers.", http_status=502)
    headers: dict[str, str] = {}
    header_bytes = 0
    for raw_name, raw_value in raw_headers.items():
        if not isinstance(raw_name, str) or not isinstance(raw_value, str):
            raise XhrEndpointError(
                "Browser returned invalid XHR headers.", http_status=502
            )
        if _HEADER_NAME_RE.fullmatch(
            raw_name
        ) is None or _HEADER_VALUE_CONTROL_RE.search(raw_value):
            raise XhrEndpointError(
                "Browser returned invalid XHR headers.", http_status=502
            )
        header_bytes += len(raw_name.encode("utf-8")) + len(raw_value.encode("utf-8"))
        if header_bytes > 64 * 1024:
            raise XhrEndpointError(
                "Browser returned oversized XHR headers.", http_status=502
            )
        headers[raw_name] = raw_value

    return {
        "responseBase64": encoded,
        "responseBytes": declared_size,
        "headers": headers,
        "finalUrl": final_url,
        "status": status,
    }


def _normalise_browser_batch_result(
    result: Mapping[str, Any], requested_urls: tuple[str, ...]
) -> dict[str, Any]:
    """Validate every result before exposing any body from a browser batch."""

    if result.get("ok") is not True:
        # Reuse the single-response error mapping. It raises before inspecting
        # response fields whenever ``ok`` is false.
        _normalise_browser_result(result)
        raise AssertionError("unreachable")

    raw_responses = result.get("responses")
    if not isinstance(raw_responses, list) or len(raw_responses) != len(requested_urls):
        raise XhrEndpointError(
            "Browser returned an incomplete XHR batch.", http_status=502
        )

    responses: list[dict[str, Any]] = []
    total_bytes = 0
    for expected_url, raw_response in zip(requested_urls, raw_responses):
        if not isinstance(raw_response, Mapping):
            raise XhrEndpointError(
                "Browser returned an invalid XHR batch item.", http_status=502
            )
        if raw_response.get("requestedUrl") != expected_url:
            raise XhrEndpointError(
                "Browser returned an out-of-order XHR batch item.", http_status=502
            )
        item_ok = raw_response.get("ok")
        if item_ok is False:
            allowed_fields = {"ok", "requestedUrl", "kind", "error"}
            if set(raw_response) - allowed_fields:
                raise XhrEndpointError(
                    "Browser returned data for a failed XHR batch item.",
                    http_status=502,
                )
            kind = raw_response.get("kind")
            expected_error = {
                "response_too_large": "response_too_large",
                "timeout": "fetch_timeout",
                "source_redirect_rejected": "source_redirect_rejected",
                "fetch_failed": "fetch_failed",
            }.get(kind)
            if expected_error is None or raw_response.get("error") != expected_error:
                raise XhrEndpointError(
                    "Browser returned an invalid XHR batch item error.",
                    http_status=502,
                )
            responses.append({"ok": False, "requestedUrl": expected_url, "kind": kind})
            continue
        if item_ok is not True:
            raise XhrEndpointError(
                "Browser returned an invalid XHR batch item status.", http_status=502
            )
        normalised = _normalise_browser_result(raw_response, expected_url=expected_url)
        total_bytes += int(normalised["responseBytes"])
        if total_bytes > MAX_BATCH_RESPONSE_BYTES:
            raise XhrEndpointError(
                f"WhoScored batch exceeds the {MAX_BATCH_RESPONSE_BYTES}-byte limit.",
                http_status=413,
            )
        responses.append({"ok": True, "requestedUrl": expected_url, **normalised})

    declared_size = result.get("responseBytes")
    if (
        isinstance(declared_size, bool)
        or not isinstance(declared_size, int)
        or declared_size != total_bytes
    ):
        raise XhrEndpointError(
            "Browser XHR batch byte count does not match its bodies.", http_status=502
        )
    return {"responses": responses, "responseBytes": total_bytes}


def _version(version_getter: Any) -> str:
    try:
        return str(version_getter())
    except Exception:
        return "unknown"


def handle_xhr_request(
    payload: Any,
    *,
    storage: Any,
    version_getter: Any,
    locks: _SessionLocks | None = None,
    pacer: _XhrStartPacer | None = None,
) -> tuple[dict[str, Any], int]:
    """Validate and execute one endpoint request; returns JSON body and HTTP status."""

    request_started = time.monotonic()
    start_ms = int(time.time() * 1_000)
    api_version = _version(version_getter)
    lock_registry = (
        locks
        if locks is not None
        else getattr(storage, "execution_locks", _SESSION_LOCKS)
    )
    start_pacer = pacer if pacer is not None else _XHR_START_PACER
    try:
        request_data = _validate_payload(payload)
        if not storage.exists(request_data.session):
            raise XhrEndpointError(
                "The requested WhoScored browser session does not exist.",
                http_status=404,
            )

        # Both clocks are captured at handler entry. Queueing for the session,
        # global source pace, browser launch, and response collection therefore
        # consume one unchanged end-to-end deadline.
        deadline = request_started + request_data.timeout_ms / 1_000.0
        deadline_epoch_ms = start_ms + request_data.timeout_ms
        with lock_registry.acquire(
            request_data.session,
            storage,
            max(0.0, deadline - time.monotonic()),
        ):
            if not storage.exists(request_data.session):
                raise XhrEndpointError(
                    "The requested WhoScored browser session no longer exists.",
                    http_status=404,
                )
            remaining_ms = int((deadline - time.monotonic()) * 1_000)
            if remaining_ms < 1:
                raise XhrEndpointError(
                    "Timed out waiting for the WhoScored browser session.",
                    http_status=504,
                )
            session = storage.sessions.get(request_data.session)
            if session is None:
                raise XhrEndpointError(
                    "The requested WhoScored browser session no longer exists.",
                    http_status=404,
                )
            browser_request = XhrRequest(
                url=request_data.url,
                session=request_data.session,
                timeout_ms=remaining_ms,
            )
            solution = _normalise_browser_result(
                _execute_browser_fetch(
                    session.driver,
                    browser_request,
                    deadline=deadline,
                    deadline_epoch_ms=deadline_epoch_ms,
                    pacer=start_pacer,
                ),
                expected_url=request_data.url,
            )

        end_ms = int(time.time() * 1_000)
        return (
            {
                "status": "ok",
                "message": "WhoScored browser XHR completed.",
                "solution": solution,
                "startTimestamp": start_ms,
                "endTimestamp": end_ms,
                "version": api_version,
            },
            200,
        )
    except XhrEndpointError as exc:
        return (
            {
                "status": "error",
                "message": str(exc),
                "startTimestamp": start_ms,
                "endTimestamp": int(time.time() * 1_000),
                "version": api_version,
            },
            exc.http_status,
        )
    except Exception:
        logging.exception("Unexpected failure in restricted WhoScored XHR endpoint")
        return (
            {
                "status": "error",
                "message": "Unexpected WhoScored browser XHR failure.",
                "startTimestamp": start_ms,
                "endTimestamp": int(time.time() * 1_000),
                "version": api_version,
            },
            500,
        )


def handle_xhr_batch_request(
    payload: Any,
    *,
    storage: Any,
    version_getter: Any,
    locks: _SessionLocks | None = None,
    pacer: _XhrStartPacer | None = None,
) -> tuple[dict[str, Any], int]:
    """Validate a bounded batch and return explicit per-item runtime outcomes."""

    request_started = time.monotonic()
    start_ms = int(time.time() * 1_000)
    api_version = _version(version_getter)
    lock_registry = (
        locks
        if locks is not None
        else getattr(storage, "execution_locks", _SESSION_LOCKS)
    )
    start_pacer = pacer if pacer is not None else _XHR_START_PACER
    try:
        request_data = _validate_batch_payload(payload)
        if not storage.exists(request_data.session):
            raise XhrEndpointError(
                "The requested WhoScored browser session does not exist.",
                http_status=404,
            )

        deadline = request_started + request_data.timeout_ms / 1_000.0
        deadline_epoch_ms = start_ms + request_data.timeout_ms
        with lock_registry.acquire(
            request_data.session,
            storage,
            max(0.0, deadline - time.monotonic()),
        ):
            if not storage.exists(request_data.session):
                raise XhrEndpointError(
                    "The requested WhoScored browser session no longer exists.",
                    http_status=404,
                )
            remaining_ms = int((deadline - time.monotonic()) * 1_000)
            if remaining_ms < 1:
                raise XhrEndpointError(
                    "Timed out waiting for the WhoScored browser session.",
                    http_status=504,
                )
            session = storage.sessions.get(request_data.session)
            if session is None:
                raise XhrEndpointError(
                    "The requested WhoScored browser session no longer exists.",
                    http_status=404,
                )
            browser_request = XhrBatchRequest(
                urls=request_data.urls,
                session=request_data.session,
                timeout_ms=remaining_ms,
            )
            solution = _normalise_browser_batch_result(
                _execute_browser_batch_fetch(
                    session.driver,
                    browser_request,
                    deadline=deadline,
                    deadline_epoch_ms=deadline_epoch_ms,
                    pacer=start_pacer,
                ),
                request_data.urls,
            )

        end_ms = int(time.time() * 1_000)
        return (
            {
                "status": "ok",
                "message": "WhoScored browser XHR batch completed.",
                "solution": solution,
                "startTimestamp": start_ms,
                "endTimestamp": end_ms,
                "version": api_version,
            },
            200,
        )
    except XhrEndpointError as exc:
        return (
            {
                "status": "error",
                "message": str(exc),
                "startTimestamp": start_ms,
                "endTimestamp": int(time.time() * 1_000),
                "version": api_version,
            },
            exc.http_status,
        )
    except Exception:
        logging.exception("Unexpected failure in restricted WhoScored XHR batch")
        return (
            {
                "status": "error",
                "message": "Unexpected WhoScored browser XHR batch failure.",
                "startTimestamp": start_ms,
                "endTimestamp": int(time.time() * 1_000),
                "version": api_version,
            },
            500,
        )


def create_app() -> Any:
    """Import the upstream app lazily and register the restricted route."""

    from bottle import request, response

    import flaresolverr as upstream
    import flaresolverr_service
    import utils

    upstream_version = str(utils.get_flaresolverr_version())
    _install_safe_logging()
    _install_safe_v1_controller(
        flaresolverr_service,
        version=upstream_version,
    )
    _install_capacity_session_tracking(
        flaresolverr_service,
        version=upstream_version,
    )
    _install_disable_media_extension(
        flaresolverr_service,
        version=upstream_version,
    )

    @upstream.app.post("/v1/xhr")
    def controller_xhr() -> dict[str, Any]:
        content_type = (request.content_type or "").split(";", 1)[0].lower()
        if content_type != "application/json":
            body, status = handle_xhr_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = "Content-Type application/json is required."
            response.status = 415
            return body
        if request.content_length is None or request.content_length < 1:
            body, status = handle_xhr_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = "A non-empty Content-Length is required."
            response.status = 411
            return body
        if request.content_length > MAX_REQUEST_BYTES:
            body, status = handle_xhr_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = f"Request body exceeds {MAX_REQUEST_BYTES} bytes."
            response.status = 413
            return body
        try:
            payload = request.json
        except Exception:
            payload = None
        body, status = handle_xhr_request(
            payload,
            storage=flaresolverr_service.SESSIONS_STORAGE,
            version_getter=utils.get_flaresolverr_version,
        )
        response.status = status
        return body

    @upstream.app.post("/v1/xhr/batch")
    def controller_xhr_batch() -> dict[str, Any]:
        content_type = (request.content_type or "").split(";", 1)[0].lower()
        if content_type != "application/json":
            body, _ = handle_xhr_batch_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = "Content-Type application/json is required."
            response.status = 415
            return body
        if request.content_length is None or request.content_length < 1:
            body, _ = handle_xhr_batch_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = "A non-empty Content-Length is required."
            response.status = 411
            return body
        if request.content_length > MAX_REQUEST_BYTES:
            body, _ = handle_xhr_batch_request(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
                version_getter=utils.get_flaresolverr_version,
            )
            body["message"] = f"Request body exceeds {MAX_REQUEST_BYTES} bytes."
            response.status = 413
            return body
        try:
            payload = request.json
        except Exception:
            payload = None
        body, status = handle_xhr_batch_request(
            payload,
            storage=flaresolverr_service.SESSIONS_STORAGE,
            version_getter=utils.get_flaresolverr_version,
        )
        response.status = status
        return body

    @upstream.app.post("/v1/whoscored/capacity-sessions/cleanup")
    def controller_capacity_session_cleanup() -> dict[str, Any]:
        content_type = (request.content_type or "").split(";", 1)[0].lower()
        if content_type != "application/json":
            body, _ = handle_capacity_session_cleanup(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
            )
            response.status = 415
            return body
        if request.content_length is None or request.content_length < 1:
            body, _ = handle_capacity_session_cleanup(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
            )
            response.status = 411
            return body
        if request.content_length > MAX_REQUEST_BYTES:
            body, _ = handle_capacity_session_cleanup(
                None,
                storage=flaresolverr_service.SESSIONS_STORAGE,
            )
            response.status = 413
            return body
        try:
            payload = request.json
        except Exception:
            payload = None
        body, status = handle_capacity_session_cleanup(
            payload,
            storage=flaresolverr_service.SESSIONS_STORAGE,
        )
        response.status = status
        return body

    return upstream.app


def main() -> None:
    """Start the upstream 3.4.6 service with its plugins and the extra route."""

    if sys.version_info < (3, 9):
        raise RuntimeError("Python 3.9 or newer is required.")

    # All container-only imports remain below this point.
    import certifi
    from bottle import ServerAdapter, run
    from bottle_plugins import prometheus_plugin
    from bottle_plugins.error_plugin import error_plugin
    from bottle_plugins.logger_plugin import logger_plugin

    import flaresolverr_service
    import utils

    if os.name == "nt":
        import multiprocessing

        multiprocessing.freeze_support()

    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    os.environ["SSL_CERT_FILE"] = certifi.where()

    log_level = os.environ.get("LOG_LEVEL", "info").upper()
    log_file = os.environ.get("LOG_FILE")
    server_host = os.environ.get("HOST", "0.0.0.0")
    server_port = int(os.environ.get("PORT", 8191))
    logger_format = "%(asctime)s %(levelname)-8s %(message)s"
    if log_level == "DEBUG":
        logger_format = "%(asctime)s %(levelname)-8s ReqId %(thread)s %(message)s"
    logging.basicConfig(
        format=logger_format,
        level=log_level,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    if log_file:
        log_file = os.path.realpath(log_file)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        logging.getLogger().addHandler(logging.FileHandler(log_file))

    _install_safe_logging()

    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("selenium.webdriver.remote.remote_connection").setLevel(
        logging.WARNING
    )
    logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)
    upstream_version = str(utils.get_flaresolverr_version())
    logging.info(
        "FlareSolverr %s with restricted WhoScored XHR",
        upstream_version,
    )

    # Install before the upstream browser self-test or any request handling.
    # ``create_app`` repeats this idempotently so embedding the app without
    # ``main`` is protected by the same startup contract.
    _install_safe_v1_controller(
        flaresolverr_service,
        version=upstream_version,
    )
    _install_capacity_session_tracking(
        flaresolverr_service,
        version=upstream_version,
    )
    _install_disable_media_extension(
        flaresolverr_service,
        version=upstream_version,
    )
    utils.get_current_platform()
    flaresolverr_service.test_browser_installation()
    app = create_app()
    app.install(logger_plugin)
    app.install(error_plugin)
    prometheus_plugin.setup()
    app.install(prometheus_plugin.prometheus_plugin)

    class WaitressServerPoll(ServerAdapter):
        def run(self, handler: Any) -> None:
            from waitress import serve

            serve(
                handler,
                host=self.host,
                port=self.port,
                asyncore_use_poll=True,
                threads=WAITRESS_THREADS,
            )

    run(
        app,
        host=server_host,
        port=server_port,
        quiet=True,
        server=WaitressServerPoll,
    )


if __name__ == "__main__":
    main()
