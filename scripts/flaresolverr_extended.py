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
import logging
import os
import re
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit


MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_BATCH_URLS = 8
MAX_BATCH_RESPONSE_BYTES = 8 * 1024 * 1024
BATCH_CONCURRENCY = 4
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

PINNED_FLARESOLVERR_VERSION = "3.4.6"
# Exact ``inspect.getsource(flaresolverr_service._evil_logic)`` digest from
# ghcr.io/flaresolverr/flaresolverr:v3.4.6.  The wrapper relies on navigation
# going through ``driver.get`` after the stock CDP blocklist call, so source
# drift must stop startup rather than weaken the resource policy.
_UPSTREAM_EVIL_LOGIC_SHA256 = (
    "b638d94bad18e6d67022865d9bcecfe07aa4bb4e03cb6129b2157dda9462e24b"
)
_MEDIA_PATCH_MARKER = "_whoscored_disable_media_extension"
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


# URL and limits are execute_async_script arguments, never interpolated into
# this source.  No caller-controlled script, method, headers, or fetch options
# exist in the endpoint contract.
XHR_SCRIPT = r"""
const targetUrl = arguments[0];
const maxBytes = arguments[1];
const timeoutMs = arguments[2];
const done = arguments[arguments.length - 1];

(async () => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let reader = null;
  try {
    const requested = new URL(targetUrl);
    if (requested.origin !== "https://www.whoscored.com") {
      throw new Error("forbidden_origin");
    }
    const allowedPaths = [
      /^\/statisticsfeed\/1\/get(?:team|player)statistics$/,
      /^\/stagestatfeed\/[1-9][0-9]*\/stageteams\/$/
    ];
    if (!allowedPaths.some((pattern) => pattern.test(requested.pathname))) {
      throw new Error("forbidden_path");
    }
    // WhoScored publishes a per-page request token in a fixed RequireJS config
    // and its own statistics XHR adds it as Model-last-Mode.  Read only that
    // exact server-provided field; callers cannot supply a name or value.
    const siteConfig = window.require && window.require.config &&
      window.require.config.params && window.require.config.params.site;
    if (!siteConfig || siteConfig.gSiteHeaderName !== "Model-last-Mode" ||
        typeof siteConfig.gSiteHeaderValue !== "string" ||
        !/^[A-Za-z0-9+/]{43}=$/.test(siteConfig.gSiteHeaderValue)) {
      throw new Error("source_header_unavailable");
    }
    const response = await fetch(requested.href, {
      method: "GET",
      credentials: "same-origin",
      mode: "cors",
      // Never follow redirects: final-URL validation after a follow is too
      // late to prevent browser egress to a hostile redirect target.  Exact
      // source URL migrations must be mapped by trusted Python code instead.
      redirect: "error",
      headers: {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Model-last-Mode": siteConfig.gSiteHeaderValue
      },
      signal: controller.signal
    });

    const finalUrl = new URL(response.url);
    if (finalUrl.origin !== "https://www.whoscored.com") {
      throw new Error("forbidden_final_origin");
    }
    if (!allowedPaths.some((pattern) => pattern.test(finalUrl.pathname))) {
      throw new Error("forbidden_final_path");
    }

    const chunks = [];
    let total = 0;
    if (response.body && response.body.getReader) {
      reader = response.body.getReader();
      while (true) {
        const item = await reader.read();
        if (item.done) break;
        total += item.value.byteLength;
        if (total > maxBytes) {
          await reader.cancel("response_too_large");
          done({ok: false, kind: "response_too_large", error: "response_too_large"});
          return;
        }
        chunks.push(item.value);
      }
    } else {
      // arrayBuffer() cannot enforce the hard ceiling while downloading.
      throw new Error("response_stream_unavailable");
    }

    const body = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    let binary = "";
    const encodeChunk = 0x8000;
    for (let index = 0; index < body.length; index += encodeChunk) {
      binary += String.fromCharCode.apply(
        null, body.subarray(index, Math.min(index + encodeChunk, body.length))
      );
    }

    done({
      ok: true,
      finalUrl: finalUrl.href,
      status: response.status,
      headers: Object.fromEntries(response.headers.entries()),
      bodyBase64: btoa(binary),
      responseBytes: total
    });
  } catch (error) {
    const aborted = error && error.name === "AbortError";
    const sourceHeaderMissing = error && error.message === "source_header_unavailable";
    const sourceRedirectRejected = error &&
      (error.message === "forbidden_final_origin" ||
       error.message === "forbidden_final_path");
    done({
      ok: false,
      kind: aborted ? "timeout" :
        (sourceHeaderMissing ? "source_header_unavailable" :
         (sourceRedirectRejected ? "source_redirect_rejected" : "fetch_failed")),
      error: aborted ? "fetch_timeout" :
        (sourceHeaderMissing ? "source_header_unavailable" :
         (sourceRedirectRejected ? "source_redirect_rejected" : "fetch_failed"))
    });
  } finally {
    clearTimeout(timer);
    if (reader) {
      try { reader.releaseLock(); } catch (_) {}
    }
  }
})();
""".strip()


# This is intentionally a second fixed script rather than caller-provided
# JavaScript.  URL, count, concurrency and byte limits are positional values
# supplied by the trusted server implementation.  The HTTP API exposes only
# ``urls``, the existing session ID and the bounded overall timeout.
BATCH_XHR_SCRIPT = r"""
const targetUrls = arguments[0];
const maxBytesPerResponse = arguments[1];
const maxAggregateBytes = arguments[2];
const timeoutMs = arguments[3];
const concurrency = arguments[4];
const done = arguments[arguments.length - 1];

(async () => {
  const allowedPaths = [
    /^\/statisticsfeed\/1\/get(?:team|player)statistics$/,
    /^\/stagestatfeed\/[1-9][0-9]*\/stageteams\/$/
  ];
  // consumedBytes is monotonic across successes and failures. This bounds
  // actual source bytes read by the whole batch, not merely returned bodies.
  let consumedBytes = 0;
  let successBytes = 0;
  let aggregateTooLarge = false;
  let nextIndex = 0;
  const results = new Array(targetUrls.length);
  const controllers = new Set();
  let deadlineExpired = false;
  const timer = setTimeout(() => {
    deadlineExpired = true;
    for (const controller of controllers) controller.abort();
  }, timeoutMs);

  try {
    const siteConfig = window.require && window.require.config &&
      window.require.config.params && window.require.config.params.site;
    if (!siteConfig || siteConfig.gSiteHeaderName !== "Model-last-Mode" ||
        typeof siteConfig.gSiteHeaderValue !== "string" ||
        !/^[A-Za-z0-9+/]{43}=$/.test(siteConfig.gSiteHeaderValue)) {
      throw new Error("source_header_unavailable");
    }

    const fetchOne = async (index) => {
      const targetUrl = targetUrls[index];
      if (aggregateTooLarge) return;
      if (deadlineExpired) {
        results[index] = {
          ok: false,
          requestedUrl: targetUrl,
          kind: "timeout",
          error: "fetch_timeout"
        };
        return;
      }
      const controller = new AbortController();
      controllers.add(controller);
      let reader = null;
      let itemBytes = 0;
      try {
        const requested = new URL(targetUrl);
        if (requested.origin !== "https://www.whoscored.com") {
          throw new Error("forbidden_origin");
        }
        if (!allowedPaths.some((pattern) => pattern.test(requested.pathname))) {
          throw new Error("forbidden_path");
        }
        const response = await fetch(requested.href, {
          method: "GET",
          credentials: "same-origin",
          mode: "cors",
          // Reject before following any redirect; see the single-fetch
          // contract above. A rejected fetch returns no source body.
          redirect: "error",
          headers: {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Model-last-Mode": siteConfig.gSiteHeaderValue
          },
          signal: controller.signal
        });

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
            consumedBytes += item.value.byteLength;
            if (consumedBytes > maxAggregateBytes) {
              aggregateTooLarge = true;
              for (const activeController of controllers) activeController.abort();
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
        results[index] = {
          ok: true,
          requestedUrl: targetUrl,
          finalUrl: finalUrl.href,
          status: response.status,
          headers: Object.fromEntries(response.headers.entries()),
          bodyBase64: btoa(binary),
          responseBytes: itemBytes
        };
        successBytes += itemBytes;
      } catch (error) {
        const message = error && error.message;
        const aborted = error && error.name === "AbortError";
        const kind = aggregateTooLarge || message === "aggregate_too_large" ?
          "aggregate_too_large" :
          (message === "response_too_large" ? "response_too_large" :
          ((message === "forbidden_final_origin" ||
            message === "forbidden_final_path") ? "source_redirect_rejected" :
           (aborted && deadlineExpired ? "timeout" : "fetch_failed")));
        results[index] = {
          ok: false,
          requestedUrl: targetUrl,
          kind,
          error: kind === "timeout" ? "fetch_timeout" : kind
        };
      } finally {
        controllers.delete(controller);
        if (reader) {
          try { reader.releaseLock(); } catch (_) {}
        }
      }
    };

    const worker = async () => {
      while (true) {
        if (aggregateTooLarge) return;
        const index = nextIndex++;
        if (index >= targetUrls.length) return;
        await fetchOne(index);
      }
    };
    await Promise.all(
      Array.from(
        {length: Math.min(concurrency, targetUrls.length)},
        () => worker()
      )
    );
    if (aggregateTooLarge) {
      done({
        ok: false,
        kind: "aggregate_too_large",
        error: "aggregate_too_large"
      });
      return;
    }
    done({
      ok: true,
      responses: results,
      responseBytes: successBytes
    });
  } catch (error) {
    const aborted = error && error.name === "AbortError";
    const sourceHeaderMissing = error && error.message === "source_header_unavailable";
    done({
      ok: false,
      kind: aborted ? "timeout" :
        (sourceHeaderMissing ? "source_header_unavailable" : "fetch_failed"),
      error: aborted ? "fetch_timeout" :
        (sourceHeaderMissing ? "source_header_unavailable" : "fetch_failed")
    });
  } finally {
    clearTimeout(timer);
    for (const controller of controllers) controller.abort();
  }
})();
""".strip()


class _SessionLocks:
    """Serialize WebDriver calls per FlareSolverr session."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    @contextmanager
    def acquire(self, session_id: str, storage: Any, timeout_s: float):
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

        if not lock.acquire(timeout=max(timeout_s, 0.0)):
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


def _execute_browser_fetch(driver: Any, request_data: XhrRequest) -> Mapping[str, Any]:
    """Execute the fixed fetch and restore the session's prior script timeout."""

    old_timeout = driver.timeouts.script
    # Give Selenium a small delivery margin after the in-page AbortController.
    driver.set_script_timeout(request_data.timeout_ms / 1_000.0 + 2.0)
    try:
        try:
            result = driver.execute_async_script(
                XHR_SCRIPT,
                request_data.url,
                MAX_RESPONSE_BYTES,
                request_data.timeout_ms,
            )
        except Exception as exc:
            if "timeout" in type(exc).__name__.lower():
                raise XhrEndpointError(
                    "WhoScored browser fetch timed out.", http_status=504
                ) from exc
            raise XhrEndpointError(
                "WhoScored browser fetch could not be executed.", http_status=502
            ) from exc
    finally:
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
    driver: Any, request_data: XhrBatchRequest
) -> Mapping[str, Any]:
    """Execute one bounded concurrent batch under the existing session lock."""

    old_timeout = driver.timeouts.script
    driver.set_script_timeout(request_data.timeout_ms / 1_000.0 + 2.0)
    try:
        try:
            result = driver.execute_async_script(
                BATCH_XHR_SCRIPT,
                list(request_data.urls),
                MAX_RESPONSE_BYTES,
                MAX_BATCH_RESPONSE_BYTES,
                request_data.timeout_ms,
                BATCH_CONCURRENCY,
            )
        except Exception as exc:
            if "timeout" in type(exc).__name__.lower():
                raise XhrEndpointError(
                    "WhoScored browser batch timed out.", http_status=504
                ) from exc
            raise XhrEndpointError(
                "WhoScored browser batch could not be executed.", http_status=502
            ) from exc
    finally:
        try:
            driver.set_script_timeout(old_timeout)
        except Exception:
            logging.warning("Could not restore FlareSolverr session script timeout")

    if not isinstance(result, Mapping):
        raise XhrEndpointError(
            "Browser returned an invalid XHR batch result.", http_status=502
        )
    return result


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
) -> tuple[dict[str, Any], int]:
    """Validate and execute one endpoint request; returns JSON body and HTTP status."""

    start_ms = int(time.time() * 1_000)
    api_version = _version(version_getter)
    lock_registry = locks or _SESSION_LOCKS
    try:
        request_data = _validate_payload(payload)
        if not storage.exists(request_data.session):
            raise XhrEndpointError(
                "The requested WhoScored browser session does not exist.",
                http_status=404,
            )

        deadline = time.monotonic() + request_data.timeout_ms / 1_000.0
        with lock_registry.acquire(
            request_data.session,
            storage,
            request_data.timeout_ms / 1_000.0,
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
                _execute_browser_fetch(session.driver, browser_request),
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
) -> tuple[dict[str, Any], int]:
    """Validate a bounded batch and return explicit per-item runtime outcomes."""

    start_ms = int(time.time() * 1_000)
    api_version = _version(version_getter)
    lock_registry = locks or _SESSION_LOCKS
    try:
        request_data = _validate_batch_payload(payload)
        if not storage.exists(request_data.session):
            raise XhrEndpointError(
                "The requested WhoScored browser session does not exist.",
                http_status=404,
            )

        deadline = time.monotonic() + request_data.timeout_ms / 1_000.0
        with lock_registry.acquire(
            request_data.session,
            storage,
            request_data.timeout_ms / 1_000.0,
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
                _execute_browser_batch_fetch(session.driver, browser_request),
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

    _install_disable_media_extension(
        flaresolverr_service,
        version=str(utils.get_flaresolverr_version()),
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
