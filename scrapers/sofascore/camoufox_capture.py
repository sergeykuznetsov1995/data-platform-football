"""
Camoufox passive-capture transport for SofaScore (issue #757, path P2).

Why this module exists
----------------------
SofaScore's data API is gated by Cloudflare Turnstile (proven in #757). A cold
request — ``tls_requests`` (#751), Chromium-FlareSolverr (#751), byparr/Firefox
(#755) — gets ``403 {"reason":"challenge"}``. The only thing that works is a
real **Firefox (Camoufox) driven through a residential proxy**, where Turnstile
passes invisibly and the SPA's own XHRs to **same-origin**
``www.sofascore.com/api/v1/*`` return real JSON. A naive in-page ``fetch()``
still 403s (it lacks the token header the SPA attaches), so we cannot replay
arbitrary URLs — we must let the SPA fire its own requests and **capture the
responses** (``page.on("response")``). See ``scripts/research/
probe_sofascore_capture.py`` for the spike that established all of this.

This transport drives Camoufox via Playwright, navigates SofaScore SPA pages,
nudges the deep tabs (Lineups / Statistics / Shotmap) so the SPA fetches them,
and returns the captured JSON keyed by entity. Downstream parsing reuses the
existing helpers on ``SofaScoreScraper`` (``_flatten_lineup_side``,
``_flatten_shotmap``, ``_build_lineup_overlay_lookup``).

Heavy deps (camoufox/playwright) are imported lazily inside ``__enter__`` so this
module stays cheap to import (unit tests exercise the pure helpers only).

Operational requirements (see Dockerfile once integrated):
  - ``pip install 'camoufox[geoip]'`` and ``python -m camoufox fetch``
  - **playwright < 1.60** (1.60 crashes Camoufox on page errors — camoufox#617)
  - a residential proxy (Turnstile 403s on a datacenter IP)
  - Xvfb (already in the image) for ``headless="virtual"``
"""
from __future__ import annotations

import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)

BASE = "https://www.sofascore.com"
_API_PATH = "/api/v1/"
# Binary/static endpoints under /api/v1 that are not JSON data — skip them.
_NON_DATA_SEGMENTS = ("/image", "/flag", "/logo", "/jersey")

_CONSENT_BUTTONS = ("Consent", "AGREE", "Agree", "Accept all", "I Accept", "Got it")
# Tabs whose XHRs fire only on interaction (lineups often loads eagerly anyway).
_EVENT_TABS = ("Lineups", "Statistics", "Player statistics", "Shotmap")

# Canonical per-event endpoints we want out of the capture buffer.
_EVENT_ENDPOINTS = {
    "event": "/api/v1/event/{eid}",
    "lineups": "/api/v1/event/{eid}/lineups",
    "statistics": "/api/v1/event/{eid}/statistics",
    "shotmap": "/api/v1/event/{eid}/shotmap",
    "incidents": "/api/v1/event/{eid}/incidents",
}


# --------------------------------------------------------------------------- #
#  Pure helpers (unit-tested without a browser)                               #
# --------------------------------------------------------------------------- #
def response_path(url: str) -> str:
    """Strip scheme+host and query → just the ``/api/v1/...`` path."""
    return re.sub(r"^https?://[^/]+", "", url.split("?")[0])


def is_data_api_url(url: str) -> bool:
    """True for a SofaScore ``/api/v1`` JSON-data response (host-agnostic: the
    SPA uses www.sofascore.com, the public host is api.sofascore.com), excluding
    binary endpoints (crests/flags/logos/jerseys)."""
    if "sofascore.com" not in url or _API_PATH not in url:
        return False
    path = response_path(url)
    return not any(seg in path for seg in _NON_DATA_SEGMENTS)


def is_challenge(body) -> bool:
    """True if the body is SofaScore's ``{"error":{"reason":"challenge"}}``."""
    return (
        isinstance(body, dict)
        and isinstance(body.get("error"), dict)
        and body["error"].get("reason") == "challenge"
    )


def event_url(event_id) -> str:
    return f"{BASE}/event/{event_id}"


def select_event_endpoints(buffer: Dict[str, dict], event_id) -> Dict[str, dict]:
    """From a capture ``buffer`` (path -> {status, json, challenge}), return the
    canonical per-event endpoints that came back as real JSON, keyed by entity
    name (``event``/``lineups``/``statistics``/``shotmap``/``incidents``)."""
    out: Dict[str, dict] = {}
    for name, tmpl in _EVENT_ENDPOINTS.items():
        rec = buffer.get(tmpl.format(eid=event_id))
        if rec and rec.get("status") == 200 and rec.get("challenge") is False and rec.get("json") is not None:
            out[name] = rec["json"]
    return out


_EVENTS_PATH_RE = re.compile(r"/api/v1/.+/events/(?:last|next)/\d+$")


def extract_events(buffer: Dict[str, dict]) -> list:
    """Collect SofaScore event objects from any ``.../events/last|next/N``
    responses in a capture ``buffer``, de-duplicated by event id. These feed
    schedule + event-id resolution for player_ratings (replaces the dead
    soccerdata schedule reader)."""
    seen = {}
    for path, rec in buffer.items():
        if not _EVENTS_PATH_RE.search(path):
            continue
        if rec.get("status") != 200 or rec.get("challenge") is not False:
            continue
        obj = rec.get("json")
        events = obj.get("events") if isinstance(obj, dict) else None
        if not isinstance(events, list):
            continue
        for ev in events:
            if isinstance(ev, dict) and ev.get("id") is not None:
                seen[ev["id"]] = ev
    return list(seen.values())


def normalize_event(ev: dict) -> dict:
    """Flatten a SofaScore event object to the schedule row shape we persist."""
    status = (ev.get("status") or {}).get("type")
    return {
        "event_id": str(ev.get("id")),
        "status": status,
        "start_timestamp": ev.get("startTimestamp"),
        "home_team": (ev.get("homeTeam") or {}).get("name"),
        "away_team": (ev.get("awayTeam") or {}).get("name"),
        "home_score": (ev.get("homeScore") or {}).get("current"),
        "away_score": (ev.get("awayScore") or {}).get("current"),
    }


def finished_event_ids(events: list) -> list:
    """Event ids (as str) for events whose status is ``finished`` — the matches
    that have lineups/ratings worth scraping."""
    return [
        str(ev["id"])
        for ev in events
        if isinstance(ev, dict)
        and ev.get("id") is not None
        and (ev.get("status") or {}).get("type") == "finished"
    ]


def parse_proxy_line(line: str) -> Optional[dict]:
    """``host:port:user:pass`` → a Playwright/Camoufox proxy dict (creds split
    out — browsers reject creds embedded in the URL). Returns ``None`` for a
    blank/comment/malformed line."""
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    if len(parts) < 4:
        return None
    host, port, user = parts[0], parts[1], parts[2]
    password = ":".join(parts[3:])  # password may contain colons
    return {"server": f"http://{host}:{port}", "username": user, "password": password}


# --------------------------------------------------------------------------- #
#  Camoufox capture session                                                   #
# --------------------------------------------------------------------------- #
class SofascoreCamoufoxCapture:
    """A warmed Camoufox session that captures SofaScore /api/v1 JSON.

    Usage::

        with SofascoreCamoufoxCapture(proxy=parse_proxy_line(line)) as cap:
            data = cap.capture_event(12345678)   # {'lineups': {...}, ...}
    """

    def __init__(
        self,
        proxy: Optional[dict] = None,
        *,
        geoip: bool = True,
        headless="virtual",
        settle_ms: int = 8000,
        tab_wait_ms: int = 2500,
        nav_timeout_ms: int = 60000,
    ) -> None:
        self._proxy = proxy
        self._geoip = geoip
        self._headless = headless
        self._settle_ms = settle_ms
        self._tab_wait_ms = tab_wait_ms
        self._nav_timeout_ms = nav_timeout_ms
        self._cm = None
        self._browser = None
        self._page = None
        self._buffer: Dict[str, dict] = {}

    # -- lifecycle -------------------------------------------------------- #
    def __enter__(self) -> "SofascoreCamoufoxCapture":
        from camoufox.sync_api import Camoufox  # lazy: heavy (Firefox)

        kwargs = {"headless": self._headless}
        if self._proxy:
            kwargs["proxy"] = self._proxy
            kwargs["geoip"] = self._geoip  # match locale/timezone to proxy exit
        else:
            logger.warning(
                "SofascoreCamoufoxCapture started WITHOUT a proxy — Turnstile 403s "
                "on a datacenter IP; data endpoints will be empty."
            )
        self._cm = Camoufox(**kwargs)
        self._browser = self._cm.__enter__()
        self._page = self._browser.new_page()
        self._page.on("response", self._on_response)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._cm is not None:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:  # noqa: BLE001 — browser teardown is best-effort
                logger.warning("Camoufox teardown failed", exc_info=True)
        return False

    # -- capture ---------------------------------------------------------- #
    def _on_response(self, resp) -> None:
        url = resp.url
        if not is_data_api_url(url):
            return
        rec = {"status": resp.status, "json": None, "challenge": None}
        try:
            obj = resp.json()
            rec["json"] = obj
            rec["challenge"] = is_challenge(obj)
        except Exception:  # noqa: BLE001 — non-JSON / body unavailable
            pass
        self._buffer[response_path(url)] = rec

    def capture_event(
        self,
        event_id,
        required=("lineups",),
        max_attempts: int = 3,
    ) -> Dict[str, dict]:
        """Navigate the match page, nudge the deep tabs, and return the captured
        per-event endpoints (only those that came back as real JSON).

        Capture is timing-flaky: a deep-tab XHR (or its body-read) can race and
        drop an endpoint — the live runner saw ~1/2 lineup misses on a single
        pass (#757). When any ``required`` endpoint is missing we re-navigate
        (up to ``max_attempts``), widening the settle window each retry so a
        slow page still fires the XHR. ``required=()`` takes one pass.
        """
        result: Dict[str, dict] = {}
        required = tuple(required or ())
        for attempt in range(1, max_attempts + 1):
            self._buffer = {}
            # Widen the settle window on retries — a slow page can drop the
            # tab XHR on the first, tighter pass.
            self._navigate(event_url(event_id), extra_settle_ms=(attempt - 1) * 2000)
            self._click_tabs()
            self._page.wait_for_timeout(self._tab_wait_ms)
            result = select_event_endpoints(self._buffer, event_id)

            missing_required = [r for r in required if r not in result]
            if not missing_required:
                break
            if attempt < max_attempts:
                logger.info(
                    "sofascore capture event=%s attempt %d/%d missing "
                    "required=%s — retrying",
                    event_id, attempt, max_attempts, missing_required,
                )

        missing = [k for k in _EVENT_ENDPOINTS if k not in result]
        logger.info("sofascore capture event=%s got=%s missing=%s",
                    event_id, sorted(result), missing)
        return result

    def capture_buffer(self, nav_url: str) -> Dict[str, dict]:
        """Navigate ``nav_url`` and return the whole capture buffer (path -> rec).
        Used for non-event pages (tournament events lists, standings)."""
        self._buffer = {}
        self._navigate(nav_url)
        return dict(self._buffer)

    # -- internals -------------------------------------------------------- #
    def _navigate(self, url: str, extra_settle_ms: int = 0) -> None:
        self._page.goto(url, wait_until="domcontentloaded", timeout=self._nav_timeout_ms)
        self._dismiss_consent()
        self._page.wait_for_timeout(self._settle_ms + extra_settle_ms)

    def _dismiss_consent(self) -> None:
        for name in _CONSENT_BUTTONS:
            try:
                self._page.get_by_role("button", name=re.compile(name, re.I)).first.click(timeout=2000)
                self._page.wait_for_timeout(1000)
                return
            except Exception:
                pass

    def _click_tabs(self) -> None:
        for label in _EVENT_TABS:
            try:
                self._page.get_by_text(label, exact=False).first.click(timeout=3000)
                self._page.wait_for_timeout(self._tab_wait_ms)
            except Exception:
                pass
