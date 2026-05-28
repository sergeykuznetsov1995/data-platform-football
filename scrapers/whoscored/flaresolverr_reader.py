"""
WhoScored reader that fetches pages through FlareSolverr instead of Selenium.

soccerdata 1.9.0 ships with seleniumbase + undetected-chromedriver for
WhoScored, but the resulting headless driver no longer survives the
WhoScored Cloudflare challenge — ``script_timeout`` fires before bypass
and the 5×retry full-driver-restart loop in
``BaseSeleniumReader._download_and_save`` leaves ``bronze.whoscored_schedule``
empty. FlareSolverr (Camoufox-based, already used by
:mod:`scrapers.whoscored.events_fetcher` for per-match events) passes the
challenge reliably, so this subclass keeps all of soccerdata's WhoScored
parsing (``read_schedule``, ``read_season_stages``, ``read_missing_players``)
intact and only swaps the HTTP transport layer.

Wire up via env var ``FLARESOLVERR_URL`` (default ``http://flaresolverr:8191``)
or the ``flaresolverr_url`` kwarg on ``WhoScoredScraper``.
"""
from __future__ import annotations

import html as html_module
import io
import json
import logging
import re
import time
import uuid
from pathlib import Path
from typing import IO, Any, Iterable, List, Optional, Union

import soccerdata as sd

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrTimeout,
)

logger = logging.getLogger(__name__)

# Recreate the FlareSolverr session every N requests. WhoScored's Cloudflare
# marks the FS-issued cookies after roughly 5–10 requests
# (see ``WhoScoredScraper.EVENTS_SESSION_RECREATE_EVERY = 10`` in scraper.py).
# Schedule is lighter (~12 fetches per league/season: 1 stages page + ~10
# monthly fixture JSONs), so 8 stays comfortably below the observed ceiling.
SESSION_RECREATE_EVERY = 8

_PRE_BODY_RE = re.compile(r"<pre[^>]*>(.*?)</pre>", re.DOTALL | re.IGNORECASE)
_BODY_RE = re.compile(r"<body[^>]*>(.*?)</body>", re.DOTALL | re.IGNORECASE)


def _extract_pre_body(html: str) -> str:
    """Strip Chromium's HTML wrapper around raw JSON responses.

    FlareSolverr renders all responses through Chromium. Two wrapping styles
    appear in the wild:

    * ``<html><body><pre>{...}</pre></body></html>`` — Chromium's default
      pretty-printer for ``application/json``.
    * ``<html><head></head><body>{...}</body></html>`` — when the server
      reports a content type other than ``application/json`` (WhoScored's
      ``/tournaments/{id}/data/?d=...`` does this).

    We try the ``<pre>`` form first (HTML-unescape the inner text), then the
    plain ``<body>`` form, then pass through unchanged.
    """
    match = _PRE_BODY_RE.search(html)
    if match:
        return html_module.unescape(match.group(1))
    match = _BODY_RE.search(html)
    if match:
        # WhoScored's no-<pre> case: <body> directly contains the raw JSON
        # without HTML escapes. Strip leading/trailing whitespace to keep
        # the cache file byte-identical to what the source served.
        return match.group(1).strip()
    return html  # already plain — pass through


_OPEN_TO_CLOSE = {"{": "}", "[": "]"}

# Match an unquoted JS object key followed by ':'.
# Anchored on '{' or ',' so we don't touch label-style colons elsewhere.
# Covers identifier-style keys (``type:``) and numeric keys (``2025:``,
# ``15:``) — WhoScored uses both in ``wsCalendar.mask``.
_UNQUOTED_KEY_RE = re.compile(
    r"([{,]\s*)([A-Za-z_$][A-Za-z0-9_$]*|[0-9]+)\s*:"
)
# Match single-quoted string literals (with backslash escapes).
_SINGLE_QUOTED_STR_RE = re.compile(r"'((?:[^'\\]|\\.)*)'")
# Match ``(new Date(...)).toString()`` expressions that WhoScored embeds in
# globals like ``wsCalendar``. soccerdata only consumes the ``mask`` field
# (pure JSON), so stripping these expressions to ``null`` keeps the blob
# JSON-parseable without losing data the caller actually reads.
_JS_NEW_DATE_RE = re.compile(r"\(new Date\([^)]*\)\)\.toString\(\)")


def _rewrite_single_quoted_str(inner: str) -> str:
    """Convert the body of a single-quoted JS string to a double-quoted JSON string.

    Single-quoted JS strings escape ``'`` as ``\\'``; in a double-quoted JSON
    string the literal ``'`` is unescaped, so we must drop the backslash —
    otherwise ``json.loads`` rejects ``\\'`` as an invalid escape. Bare ``"``
    inside a single-quoted JS string is literal; in JSON it must be escaped.
    """
    inner = inner.replace("\\'", "'")  # JS-only escape → literal "'"
    inner = inner.replace('"', '\\"')  # literal " → JSON escape
    return '"' + inner + '"'


def _js_literal_to_json(blob: str) -> str:
    """Convert a JS object/array literal to a JSON-parseable string.

    WhoScored embeds globals like ``var allRegions = [{type:1, name:'Africa'}]``
    — JS literal syntax with **unquoted object keys** and **single-quoted
    strings** (incl. ``\\'`` for escaped apostrophes like ``'Papa John\\'s'``).
    Both shapes are illegal in JSON, so ``json.loads`` rejects the blob raw.

    This helper performs two targeted rewrites:

    1. Quote unquoted object keys (``{foo:`` → ``{"foo":``).
    2. Convert single-quoted strings to double-quoted, fixing escape semantics
       at the same time (see :func:`_rewrite_single_quoted_str`).

    Not a general JS-AST parser — does NOT handle JS-only literals
    (``undefined`` / ``NaN``), trailing commas, computed property names, or
    comments. Adequate for soccerdata's WhoScored fixtures.
    """
    # Strip JS-only expressions BEFORE key/string rewrites so the
    # unquoted-key regex doesn't get confused by ``Date(``-style argument
    # lists. soccerdata's WhoScored only reads ``mask`` from wsCalendar,
    # so dropping the date fields to ``null`` is safe.
    blob = _JS_NEW_DATE_RE.sub("null", blob)
    blob = _UNQUOTED_KEY_RE.sub(r'\1"\2":', blob)
    blob = _SINGLE_QUOTED_STR_RE.sub(
        lambda m: _rewrite_single_quoted_str(m.group(1)),
        blob,
    )
    return blob


def _extract_js_var(html: str, var_name: str) -> Any:
    """Extract a top-level JS variable assignment from an HTML page.

    Matches ``var <name> = {...}`` / ``<name> = {...}`` / ``var <name> = [...]``
    and returns the parsed JSON value. Used by soccerdata's WhoScored reader
    to harvest globals like ``wsCalendar`` (object) and ``allRegions`` (array)
    — under stock Selenium these are fetched via
    ``driver.execute_script("return " + var)``; we read them off the rendered
    HTML instead.

    The scan is string-aware (mirrors
    :func:`scrapers.whoscored.events_fetcher._extract_matchcentre_from_html`)
    so brackets inside string literals don't unbalance the count.

    Raises:
        ValueError: variable name not found, or brackets are unbalanced.
    """
    pattern = re.compile(rf"(?:var\s+)?{re.escape(var_name)}\s*=\s*([\{{\[])")
    m = pattern.search(html)
    if not m:
        raise ValueError(f"JS variable {var_name!r} not found in HTML")

    open_char = m.group(1)
    close_char = _OPEN_TO_CLOSE[open_char]
    start = m.end() - 1  # position of the opening bracket
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == open_char:
            depth += 1
        elif c == close_char:
            depth -= 1
            if depth == 0:
                blob = html[start : i + 1]
                try:
                    return json.loads(blob)
                except json.JSONDecodeError:
                    # WhoScored embeds JS object literals (unquoted keys,
                    # single-quoted strings) rather than JSON. Normalise and
                    # retry — only raise if even the relaxed parse fails.
                    try:
                        return json.loads(_js_literal_to_json(blob))
                    except json.JSONDecodeError as e:
                        raise ValueError(
                            f"JS variable {var_name!r} JSON decode failed: {e}"
                        ) from e
    raise ValueError(f"JS variable {var_name!r} brackets unmatched")


class FlareSolverrWhoScoredReader(sd.WhoScored):
    """soccerdata WhoScored reader that fetches HTML via FlareSolverr."""

    SESSION_RECREATE_EVERY = SESSION_RECREATE_EVERY
    # Max consecutive CF-challenge / timeout retries with session rotation
    # before propagating the error. 3 covers the observed worst-case of two
    # challenges in a row during long schedule iteration; higher values risk
    # infinite loops on persistently failing URLs.
    _max_recoveries: int = 3

    def __init__(
        self,
        flaresolverr_url: str = "http://flaresolverr:8191",
        proxy: Optional[str] = None,
        max_timeout_ms: int = 90_000,
        leagues: Optional[Union[str, List[str]]] = None,
        seasons: Optional[Union[str, int, Iterable[Union[str, int]]]] = None,
        no_cache: bool = False,
        no_store: bool = False,
        data_dir: Optional[Path] = None,
        session_recreate_every: int = SESSION_RECREATE_EVERY,
    ):
        # Open the FlareSolverr session before sd.WhoScored.__init__ runs so
        # that any early _download_and_save call (sd.WhoScored constructor
        # itself doesn't make one, but stay symmetric with FlareSolverrSoFIFAReader).
        self._fs_client = FlareSolverrClient(url=flaresolverr_url)
        self._session_id = self._new_session_id()
        self._max_timeout_ms = max_timeout_ms
        self._proxy_url = proxy
        self._request_count = 0
        self._session_recreate_every = session_recreate_every
        self._session_closed = False
        self._fs_client.create_session(self._session_id, proxy_url=proxy)
        logger.info("FlareSolverr WhoScored session %s created", self._session_id)

        kw: dict = dict(
            leagues=leagues,
            seasons=seasons,
            no_cache=no_cache,
            no_store=no_store,
            proxy=None,  # proxy now lives inside the FlareSolverr session
        )
        if data_dir is not None:
            kw["data_dir"] = data_dir
        super().__init__(**kw)

    @staticmethod
    def _new_session_id() -> str:
        return f"whoscored-{uuid.uuid4().hex[:8]}"

    @classmethod
    def _all_leagues(cls) -> dict[str, str]:
        """Delegate league lookup to ``sd.WhoScored``.

        soccerdata's ``BaseReader._all_leagues`` keys ``LEAGUE_DICT`` by
        ``cls.__name__``. Our subclass would search for
        ``'FlareSolverrWhoScoredReader'`` and find nothing — every league would
        be rejected as invalid. Forward the call to the canonical parent.
        """
        return sd.WhoScored._all_leagues()

    def _init_webdriver(self):
        # Selenium not needed — FlareSolverr owns the browser. Returning None
        # keeps ``self._driver`` unset; any code that still tries to dereference
        # it indicates a code path we forgot to migrate.
        return None

    def _validate_page(self, url: str) -> str:
        # Defensive: in BaseSeleniumReader this is called after driver.get(url).
        # Our overridden _download_and_save bypasses the driver path, so this
        # should never run. Fail loud rather than NPE on self._driver.
        raise RuntimeError(
            "_validate_page should not be called in FlareSolverrWhoScoredReader "
            f"(url={url})"
        )

    def _maybe_recreate_session(self) -> None:
        if self._request_count <= 0:
            return
        if self._request_count % self._session_recreate_every != 0:
            return
        self._rotate_session(
            reason=f"scheduled after {self._request_count} requests"
        )

    def _rotate_session(self, reason: str) -> None:
        """Destroy current session and create a fresh one. Idempotent on errors."""
        old = self._session_id
        try:
            self._fs_client.destroy_session(old)
        except Exception as e:
            logger.warning("destroy_session(%s) before rotation failed: %s", old, e)
        self._session_id = self._new_session_id()
        self._fs_client.create_session(self._session_id, proxy_url=self._proxy_url)
        logger.info(
            "Rotated FlareSolverr session %s -> %s (%s)",
            old,
            self._session_id,
            reason,
        )

    def _fs_get_with_recovery(self, url: str) -> dict:
        """Call ``fs_client.get(url)`` with session rotation on CF / timeout."""
        attempt = 0
        while True:
            try:
                return self._fs_client.get(
                    url,
                    self._session_id,
                    max_timeout_ms=self._max_timeout_ms,
                )
            except (FlareSolverrTimeout, FlareSolverrCFChallengeFailed) as e:
                attempt += 1
                if attempt > self._max_recoveries:
                    logger.error(
                        "Recovery exhausted (%d attempts) for %s: %s",
                        attempt,
                        url,
                        e,
                    )
                    raise
                reason = (
                    "CF challenge timeout"
                    if isinstance(e, FlareSolverrCFChallengeFailed)
                    else "FS timeout"
                )
                # Linear backoff (3s, 6s, 9s) — gives FlareSolverr Chromium
                # time to release the prior tab's memory before we hit it again.
                backoff = 3.0 * attempt
                logger.warning(
                    "%s on %s (attempt %d/%d), sleeping %.1fs then rotating session: %s",
                    reason,
                    url,
                    attempt,
                    self._max_recoveries,
                    backoff,
                    e,
                )
                time.sleep(backoff)
                self._rotate_session(reason=f"{reason} attempt {attempt}")

    def _download_and_save(
        self,
        url: str,
        filepath: Optional[Path] = None,
        var: Optional[Union[str, Iterable[str]]] = None,
    ) -> IO[bytes]:
        """Fetch ``url`` through FlareSolverr and adapt the body to soccerdata's contract.

        Mirrors ``BaseSeleniumReader._download_and_save`` (``_common.py:591``):

        * ``var=None`` → return ``document.body.innerHTML`` bytes. For ``.json``
          filepaths we additionally strip Chromium's ``<pre>`` wrapper so the
          cached file is valid JSON.
        * ``var="<name>"`` → return ``json.dumps(<value>).encode()``, where
          ``<value>`` is the JS variable extracted from the rendered HTML
          (rather than ``driver.execute_script("return " + var)``).
        """
        self._maybe_recreate_session()
        response = self._fs_get_with_recovery(url)
        html = response.get("html") or ""
        status = response.get("status", 0)

        if "_cf_chl_opt" in html or "Just a moment" in html:
            raise FlareSolverrCFChallengeFailed(
                f"Cloudflare challenge HTML returned for {url} (status={status})"
            )
        if not html:
            raise ConnectionError(f"FlareSolverr returned empty body for {url}")

        extraction_failed = False
        if var is None:
            if filepath is not None and str(filepath).endswith(".json"):
                body = _extract_pre_body(html).encode("utf-8")
            else:
                body = html.encode("utf-8")
        else:
            if not isinstance(var, str):
                raise NotImplementedError(
                    "FlareSolverrWhoScoredReader supports only single string var names"
                )
            try:
                value = _extract_js_var(html, var)
            except ValueError as e:
                # soccerdata's Selenium path returns json.dumps(None) when the
                # JS variable is missing (JavascriptException) — keep the same
                # contract so downstream parsing handles "no data" uniformly.
                logger.warning("JS var %s missing on %s: %s", var, url, e)
                value = None
                extraction_failed = True
            body = json.dumps(value).encode("utf-8")

        # Skip cache write on extraction failure: a cached ``null`` would
        # make every subsequent run short-circuit to the same failure
        # without re-fetching, so the only path out is wiping the cache
        # by hand. Letting the failure propagate keeps recovery automatic.
        if not self.no_store and filepath is not None and not extraction_failed:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with filepath.open(mode="wb") as fh:
                fh.write(body)

        self._request_count += 1
        return io.BytesIO(body)

    def close(self) -> None:
        if self._session_closed:
            return
        try:
            self._fs_client.destroy_session(self._session_id)
            logger.info("Destroyed FlareSolverr session %s", self._session_id)
        except Exception as e:
            logger.warning(
                "destroy_session(%s) at close failed: %s", self._session_id, e
            )
        finally:
            self._session_closed = True

    def __enter__(self) -> "FlareSolverrWhoScoredReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
