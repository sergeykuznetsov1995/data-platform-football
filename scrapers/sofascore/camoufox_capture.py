"""
Camoufox passive-capture transport for SofaScore (issue #757, path P2).

Why this module exists
----------------------
SofaScore's data API is gated by Cloudflare Turnstile (proven in #757). A cold
request — ``tls_requests`` (#751), Chromium-FlareSolverr (#751), byparr/Firefox
(#755) — gets ``403 {"reason":"challenge"}``. The only thing that works is a
real **Firefox (Camoufox) driven through a residential proxy**, where Turnstile
passes invisibly and the SPA's own XHRs to **same-origin**
``www.sofascore.com/api/v1/*`` return real JSON. An in-page ``fetch()`` 403s
only while Turnstile is UNSOLVED (datacenter IP / cold page); once a navigation
has passed the challenge, a same-origin fetch carries the clearance cookie and
succeeds — proven by :meth:`paginate_tournament_season` (#824) and now the
basis of the cheap per-match path :meth:`fetch_event` (#842). Navigation +
passive capture (``page.on("response")``) remains the warm-up and fallback.
See ``scripts/research/probe_sofascore_capture.py`` for the original spike.

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

import json
import logging
import os
import re
from collections import Counter
from typing import Dict, Optional

from scrapers.sofascore._flatten import _auto_flatten

logger = logging.getLogger(__name__)

BASE = "https://www.sofascore.com"
_API_PATH = "/api/v1/"
# Binary/static endpoints under /api/v1 that are not JSON data — skip them.
_NON_DATA_SEGMENTS = ("/image", "/flag", "/logo", "/jersey")

_CONSENT_BUTTONS = ("Consent", "AGREE", "Agree", "Accept all", "I Accept", "Got it")
# Tabs whose XHRs fire only on interaction (lineups often loads eagerly anyway).
_EVENT_TABS = ("Lineups", "Statistics", "Player statistics", "Shotmap")
# Stable data-testid per tab label (live-proven 2026-06-22, #751 PR2). Clicking
# the Statistics tab fires BOTH /statistics (re-fetch, body captured) AND
# /shotmap. A plain get_by_text('Statistics') grabbed a non-tab label element
# (the match page renders two "Statistics" nodes) and fired nothing.
_TAB_TESTIDS = {
    "Lineups": "tab-lineups",
    "Statistics": "tab-statistics",
    # Player-page Season tab (#751 PR3b) — fires /statistics/seasons + the
    # default competition's season-statistics/overall.
    "Season": "tab-season",
}

# Canonical per-event endpoints we want out of the capture buffer.
_EVENT_ENDPOINTS = {
    "event": "/api/v1/event/{eid}",
    "lineups": "/api/v1/event/{eid}/lineups",
    "statistics": "/api/v1/event/{eid}/statistics",
    "shotmap": "/api/v1/event/{eid}/shotmap",
    "incidents": "/api/v1/event/{eid}/incidents",
}

# Deep-tab label → endpoint name(s) an in-page fetch must pull to match what a
# navigation that clicked that tab would have captured (#842). Both the
# Statistics and Player-statistics tabs ride on /statistics.
_TAB_FETCH_NAMES = {
    "Lineups": ("lineups",),
    "Statistics": ("statistics",),
    "Player statistics": ("statistics",),
    "Shotmap": ("shotmap",),
}


def fetch_names_for_tabs(tabs) -> tuple:
    """Endpoint names an in-page fetch pass must pull for these deep tabs
    (#842). ``event`` and ``lineups`` always ride along: navigation mode gets
    them for free on page load, ``event`` carries homeTeam/awayTeam (the team
    mapping for event_player_stats) and ``lineups`` is the required primary
    payload. Unknown labels are ignored; order follows ``_EVENT_ENDPOINTS``
    for determinism."""
    wanted = {"event", "lineups"}
    for tab in tabs or ():
        wanted.update(_TAB_FETCH_NAMES.get(tab, ()))
    return tuple(n for n in _EVENT_ENDPOINTS if n in wanted)

# Non-essential resource types we abort via page.route to cut proxy bytes (#842).
# The SPA only needs its own JS to run + fire the /api/v1 XHRs, so document /
# script / xhr / fetch / websocket must pass; images / fonts / media / CSS do not.
_BLOCK_RESOURCE_TYPES = frozenset({"image", "media", "font", "stylesheet"})
# Cloudflare / Turnstile assets — always pass, even css/font/img served by the
# challenge iframe. The bare resource-type block would otherwise abort them and
# starve a visible challenge, degrading the bypass. Checked BEFORE the type block.
_ALLOW_URL_SUBSTRINGS = (
    "challenges.cloudflare.com",
    "turnstile.cloudflare.com",
    "/cdn-cgi/challenge-platform/",
)
# Analytics / tracking hosts to abort regardless of type (ported from
# nodriver_bypass.BLOCKED_URL_PATTERNS, globs -> substrings). Do NOT add
# challenges/turnstile.cloudflare.com here — they are required for the bypass.
_BLOCK_URL_SUBSTRINGS = (
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "googlesyndication.com", "googleadservices.com", "googletagservices.com",
    "facebook.net", "facebook.com/tr", "twitter.com/i/", "platform.twitter.com",
    "amazon-adsystem.com", "adsafeprotected.com", "adsrvr.org",
    "scorecardresearch.com", "quantserve.com", "cloudflareinsights.com",
    "newrelic.com", "nr-data.net", "hotjar.com", "segment.io", "mixpanel.com",
    "snap.licdn.com", "bat.bing.com", "hs-scripts.com", "hs-analytics.net",
)


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


def merge_capture(existing: Optional[dict], new: dict) -> dict:
    """Choose the better of two capture records for the same ``/api/v1`` path.

    A record carrying real JSON always beats one that does not. On a retry
    re-navigation the browser serves already-fetched endpoints as ``304 Not
    Modified`` with an empty body (``json=None``); without this guard that 304
    would clobber the good ``200``+JSON capture from an earlier pass — exactly
    why ``statistics``/``shotmap`` vanished on retry while ``lineups`` (the
    ``required`` endpoint that forced the retry) survived (#751 PR2). A
    body-read race (a large XHR firing twice, once with ``json=None``) is
    covered by the same rule, order-independent."""
    if existing is None:
        return new
    if existing.get("json") is not None and new.get("json") is None:
        return existing
    return new


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


def should_block_request(resource_type: str, url: str) -> bool:
    """True → abort this request to save proxy bytes (#842): non-essential static
    (image/media/font/stylesheet) or a known analytics/tracking host.

    Never blocks document/script/xhr/fetch/websocket — the SPA needs them to run
    and fire the ``/api/v1`` XHRs — nor the ``/api/v1`` data endpoints, nor any
    Cloudflare/Turnstile asset (the challenge iframe pulls its own css/font/img,
    which the bare resource-type block would otherwise abort and starve the
    bypass). Both guards run before the type block, so a crest served under
    ``/api/v1/.../image`` is still blocked as an image while the 5 JSON payloads
    and challenge assets always pass."""
    if is_data_api_url(url):
        return False
    low = url.lower()
    if any(s in low for s in _ALLOW_URL_SUBSTRINGS):
        return False
    if resource_type in _BLOCK_RESOURCE_TYPES:
        return True
    return any(s in low for s in _BLOCK_URL_SUBSTRINGS)


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
    """Flatten a SofaScore event object to a ``bronze.sofascore_schedule`` row.

    #840: keep the whole event as-is (auto-passthrough, source-key names:
    ``start_timestamp``, ``home_team_name``/``away_team_name``,
    ``home_score_current``/``away_score_current``, ``round_info_round``,
    ``status_type``, ...). ``game_id`` stays a hard-coded int anchor. The
    :meth:`SofaScoreScraper.read_schedule` caller only tags ``league``/``season``
    + lineage now; type derivations (epoch->timestamp, round->bigint) and
    renames move downstream to the schedule consumers (xref_match, team_match,
    shots). The legacy soccerdata-only ``week``/``game`` placeholders are dropped
    (they were always NULL and are not source fields).
    """
    row = {"game_id": int(ev["id"])}
    _auto_flatten(ev, row)
    return row


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


def extract_tournament_events(buffer: Dict[str, dict], ut_id) -> list:
    """Collect SofaScore events for a SPECIFIC unique-tournament from a capture
    ``buffer``'s ``/events/{round,last,next}/N`` responses, de-duplicated by id.

    Filtering by ``ut_id`` is REQUIRED, not optional: a league page also fires a
    *featured* OTHER tournament's ``/events/last|next`` (#757 B0 proved a naive
    last|next capture on the EPL page grabbed unrelated ut=16 events). We match
    only ``/unique-tournament/{ut_id}/season/{sid}/events/...`` paths.
    """
    pat = re.compile(
        rf"/api/v1/unique-tournament/{int(ut_id)}/season/\d+/events/(?:round|last|next)/\d+$"
    )
    seen: Dict = {}
    for path, rec in buffer.items():
        if not pat.search(path):
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


def extract_tournament_seasons_map(buffer: Dict[str, dict], ut_id) -> Dict[str, int]:
    """``{year_label: season_id}`` from a captured
    ``/unique-tournament/{ut_id}/seasons`` response (e.g. ``'25/26' -> 76986``).

    Lets :meth:`SofaScoreScraper.read_league_table` map a target season year to
    its SofaScore ``season_id`` — the ``/standings/total`` path is keyed by
    ``season_id``, not by year, and the standings JSON itself carries no season.
    Returns ``{}`` when the seasons list wasn't captured / came back challenged.
    Mirrors :func:`extract_player_seasons_map` for the tournament endpoint."""
    rec = buffer.get(f"/api/v1/unique-tournament/{int(ut_id)}/seasons")
    if not rec or rec.get("status") != 200 or rec.get("challenge") is not False:
        return {}
    obj = rec.get("json")
    if not isinstance(obj, dict):
        return {}
    out: Dict[str, int] = {}
    for s in obj.get("seasons") or []:
        if isinstance(s, dict) and s.get("year") is not None and s.get("id") is not None:
            out[str(s["year"])] = int(s["id"])
    return out


def extract_tournament_standings(buffer: Dict[str, dict], ut_id, season_id) -> list:
    """The ``rows`` of the TOTAL standings table for a SPECIFIC
    ``(ut_id, season_id)`` from a capture ``buffer``'s
    ``/unique-tournament/{ut}/season/{sid}/standings/total`` response.

    The exact ``season_id`` in the path is the season-guard: a tournament page
    that has rolled to the NEXT season off-season fires ``/standings/total`` for
    a DIFFERENT sid, so requiring our target sid keeps it out of the current
    partition (the caller then writes nothing). Returns ``[]`` when the standings
    XHR for this sid wasn't captured / was challenged / carried no rows."""
    rec = buffer.get(
        f"/api/v1/unique-tournament/{int(ut_id)}/season/{int(season_id)}/standings/total"
    )
    if not rec or rec.get("status") != 200 or rec.get("challenge") is not False:
        return []
    obj = rec.get("json")
    standings = obj.get("standings") if isinstance(obj, dict) else None
    if not isinstance(standings, list) or not standings:
        return []
    # Prefer the 'total' table; fall back to the first block.
    block = next((s for s in standings if isinstance(s, dict)
                  and s.get("type") == "total"), standings[0])
    rows = block.get("rows") if isinstance(block, dict) else None
    return rows if isinstance(rows, list) else []


def normalize_standing(row: dict) -> dict:
    """Flatten a SofaScore standings row to a ``bronze.sofascore_league_table``
    row (business columns only; the caller adds ``league`` / ``season`` /
    metadata and casts the counts to nullable bigint). ``gd`` is derived — the
    API row has no goal-difference field. Mirrors :func:`normalize_event`."""
    gf = row.get("scoresFor")
    ga = row.get("scoresAgainst")
    return {
        "team": (row.get("team") or {}).get("name"),
        "mp": row.get("matches"),
        "w": row.get("wins"),
        "d": row.get("draws"),
        "l": row.get("losses"),
        "gf": gf,
        "ga": ga,
        "gd": (gf - ga) if gf is not None and ga is not None else None,
        "pts": row.get("points"),
    }


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
#  Per-player capture helpers (issue #751 PR3) — profile snapshot              #
# --------------------------------------------------------------------------- #
# Live-proven (scripts/research/probe_sofascore_player.py, 2026-06-22): the bio
# is SSR'd at __NEXT_DATA__.props.pageProps.player (NOT an XHR), so the profile
# capture needs no Turnstile-gated data XHR. Season-aggregate stats (which DO
# need the Season tab + a season-picker for transferred/multi-competition
# players) are deferred to PR3b — see memory/feedback_sofascore_player_page_capture.
def player_url(player_id) -> str:
    """A player page URL with a DUMMY slug — the SPA redirects to the real
    ``/football/player/{real-slug}/{id}`` by trailing id (#751 PR3 spike), so we
    never need to know the slug. Bare ``/player/{id}`` (no slug) does NOT load."""
    return f"{BASE}/player/x/{player_id}"


def extract_player_from_next_data(next_data, player_id) -> Optional[dict]:
    """Pull the SSR'd player bio object out of a parsed ``__NEXT_DATA__`` blob.

    The bio lives at ``props.pageProps.player`` (live-proven) and carries exactly
    the fields :meth:`SofaScoreScraper._flatten_player_profile` reads under the
    ``player`` key (name/slug/position/height/preferredFoot/dateOfBirthTimestamp/
    team). Returns the player dict only when its ``id`` matches ``player_id`` (so
    a wrong-page SSR never yields a mismatched row); ``None`` otherwise."""
    if not isinstance(next_data, dict):
        return None
    page_props = (next_data.get("props") or {}).get("pageProps")
    if not isinstance(page_props, dict):
        return None
    player = page_props.get("player")
    if not isinstance(player, dict):
        return None
    pid = player.get("id")
    if pid is None or str(pid) != str(player_id):
        return None
    return player


# --------------------------------------------------------------------------- #
#  Per-player SEASON STATS helpers (issue #751 PR3b) — Season-tab picker       #
# --------------------------------------------------------------------------- #
# Clicking the Season tab fires /statistics/seasons (a year→season_id map) plus
# one /unique-tournament/{ut}/season/{sid}/statistics/overall for whatever
# competition the page defaults to. For transferred/multi-competition players
# that default is NOT EPL (live-proven: Paquetá → World Cup), so the capture
# also drives the season-picker to select the target tournament — firing a
# SECOND overall XHR. These pure helpers pick the right one out of the buffer
# without any tls fetch (the old tls season-id resolution is no longer needed).
_PLAYER_SEASON_STATS_RE = re.compile(
    r"/api/v1/player/(\d+)/unique-tournament/(\d+)/season/(\d+)/statistics/overall$"
)
_PLAYER_SEASONS_LIST_RE = re.compile(r"/api/v1/player/(\d+)/statistics/seasons$")


def season_short_to_label(season_short) -> str:
    """Map a soccerdata short season (``'2526'``) to SofaScore's year label
    (``'25/26'``) used in ``/statistics/seasons``. A 4-digit token is split;
    anything else (e.g. an already-formatted ``'25/26'``) is returned unchanged.
    """
    s = str(season_short)
    if len(s) == 4 and s.isdigit():
        return f"{s[:2]}/{s[2:]}"
    return s


def extract_player_seasons_map(buffer: Dict[str, dict], player_id) -> Dict[int, Dict[str, int]]:
    """``{unique_tournament_id: {year_label: season_id}}`` from the captured
    ``/player/{id}/statistics/seasons`` response (probe FACT 2). Lets us map a
    target ``(ut, year_label)`` to its SofaScore ``season_id`` without a tls
    fetch. Returns ``{}`` when the seasons list wasn't captured / was challenged.
    """
    rec = buffer.get(f"/api/v1/player/{player_id}/statistics/seasons")
    if not rec or rec.get("status") != 200 or rec.get("challenge") is not False:
        return {}
    obj = rec.get("json")
    if not isinstance(obj, dict):
        return {}
    out: Dict[int, Dict[str, int]] = {}
    for entry in obj.get("uniqueTournamentSeasons") or []:
        if not isinstance(entry, dict):
            continue
        ut = (entry.get("uniqueTournament") or {}).get("id")
        if ut is None:
            continue
        years: Dict[str, int] = {}
        for s in entry.get("seasons") or []:
            if isinstance(s, dict) and s.get("year") is not None and s.get("id") is not None:
                years[str(s["year"])] = int(s["id"])
        out[int(ut)] = years
    return out


def select_player_season_stats(
    buffer: Dict[str, dict],
    player_id,
    target_ut,
    target_season_id=None,
) -> Optional[tuple]:
    """Pick the season-statistics/overall XHR for the target unique-tournament
    out of a player-page capture ``buffer``. Returns ``(ut_id, season_id,
    payload)`` or ``None``.

    Only real JSON (200, not a challenge) for THIS ``player_id`` and
    ``target_ut`` is considered — so the default-tab overall of a different
    competition (e.g. World Cup for a transferred player) is never mistaken for
    the EPL row. When ``target_season_id`` is known (resolved via
    :func:`extract_player_seasons_map`) it acts as a season-guard: only an exact
    ``(ut, season)`` match qualifies. Without a guard the most recent season_id
    wins (deterministic)."""
    candidates = []
    for path, rec in buffer.items():
        m = _PLAYER_SEASON_STATS_RE.search(path)
        if not m or str(m.group(1)) != str(player_id):
            continue
        if rec.get("status") != 200 or rec.get("challenge") is not False:
            continue
        payload = rec.get("json")
        if not isinstance(payload, dict):
            continue
        ut, sid = int(m.group(2)), int(m.group(3))
        if ut != int(target_ut):
            continue
        if target_season_id is not None and sid != int(target_season_id):
            continue
        candidates.append((ut, sid, payload))
    if not candidates:
        return None
    candidates.sort(key=lambda c: c[1])  # most-recent season_id last
    return candidates[-1]


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
        block_resources: bool = True,
    ) -> None:
        self._proxy = proxy
        self._geoip = geoip
        self._headless = headless
        self._settle_ms = settle_ms
        self._tab_wait_ms = tab_wait_ms
        self._nav_timeout_ms = nav_timeout_ms
        self._block_resources = block_resources
        self._cm = None
        self._browser = None
        self._page = None
        self._buffer: Dict[str, dict] = {}
        # Proxy-byte accounting (#842) — rx+tx tallied from request.sizes(), the
        # same metric the issue measured (~4.2 MB/match before blocking).
        self._bytes_total = 0
        self._bytes_at_event_start = 0
        self._bytes_by_type: Counter = Counter()
        self._blocked_count = 0
        self._blocked_at_event_start = 0

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
        # Cut proxy bytes by aborting images/fonts/media/CSS/analytics (#842).
        # Env kill-switch lets ops disable without a redeploy if Turnstile 403s.
        env = os.environ.get("SOFASCORE_BLOCK_RESOURCES")
        block = self._block_resources if env is None else env.strip().lower() not in ("0", "false", "no")
        if block:
            # Trade-off: enabling page.route disables the browser HTTP cache, so a
            # capture_event retry re-fetches script/document instead of serving
            # them from cache. Still a net win — image/media/font/CSS dwarf the
            # re-fetched JS — but the canary must measure bytes/match WITH retries.
            self._page.route("**/*", self._maybe_block)
        self._page.on("response", self._on_response)
        self._page.on("requestfinished", self._on_request_finished)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        top = ", ".join(f"{t}={b // 1024}KB" for t, b in self._bytes_by_type.most_common(4))
        logger.info("sofascore capture session total=%.1fMB blocked=%d top=[%s]",
                    self._bytes_total / 1_048_576, self._blocked_count, top)
        if self._cm is not None:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:  # noqa: BLE001 — browser teardown is best-effort
                logger.warning("Camoufox teardown failed", exc_info=True)
        return False

    # -- byte accounting / resource blocking (#842) ----------------------- #
    def _maybe_block(self, route) -> None:
        """page.route handler: abort non-essential requests, pass the rest.
        Never lets a routing error stall a navigation (falls back to continue)."""
        try:
            req = route.request
            if should_block_request(req.resource_type, req.url):
                self._blocked_count += 1
                route.abort()
                return
            route.continue_()
        except Exception:  # noqa: BLE001 — routing must not break the capture
            try:
                route.continue_()
            except Exception:
                pass

    def _on_request_finished(self, req) -> None:
        """Tally rx+tx bytes per resource type (aborted requests never fire this,
        so blocked resources correctly count as zero)."""
        try:
            s = req.sizes()
            n = (s.get("responseBodySize", 0) + s.get("responseHeadersSize", 0)
                 + s.get("requestBodySize", 0) + s.get("requestHeadersSize", 0))
            self._bytes_by_type[req.resource_type] += n
            self._bytes_total += n
        except Exception:  # noqa: BLE001 — sizes() can race on teardown
            pass

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
        path = response_path(url)
        # Never let a later empty/304 record overwrite a good earlier capture
        # (see merge_capture — re-nav serves cached endpoints as 304/no-body).
        self._buffer[path] = merge_capture(self._buffer.get(path), rec)

    def capture_event(
        self,
        event_id,
        required=("lineups",),
        max_attempts: int = 3,
        tabs=_EVENT_TABS,
    ) -> Dict[str, dict]:
        """Navigate the match page, nudge the deep tabs, and return the captured
        per-event endpoints (only those that came back as real JSON).

        Capture is timing-flaky: a deep-tab XHR (or its body-read) can race and
        drop an endpoint — the live runner saw ~1/2 lineup misses on a single
        pass (#757). When any ``required`` endpoint is missing we re-navigate
        (up to ``max_attempts``), widening the settle window each retry so a
        slow page still fires the XHR. ``required=()`` takes one pass.

        ``tabs`` narrows which deep tabs to click — a ratings-only caller passes
        ``tabs=("Lineups",)`` to avoid fetching Statistics/Shotmap it won't use
        (saves proxy bytes + time). Defaults to all of ``_EVENT_TABS``.
        """
        result: Dict[str, dict] = {}
        required = tuple(required or ())
        # Reset the buffer ONCE per event, then accumulate across retries. A
        # re-navigation serves already-captured endpoints as 304/no-body, so a
        # per-attempt reset would drop a good capture from an earlier pass
        # (statistics/shotmap vanished while the required lineups survived —
        # #751 PR2). merge_capture keeps the best record per path.
        self._buffer = {}
        for attempt in range(1, max_attempts + 1):
            # Widen the settle window on retries — a slow page can drop the
            # tab XHR on the first, tighter pass.
            self._navigate(event_url(event_id), extra_settle_ms=(attempt - 1) * 2000)
            self._click_tabs(tabs)
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

        self._log_event_capture(event_id, result, kind="capture")
        return result

    # In-page fetch JS (#842): same-origin, so it carries the Turnstile
    # clearance cookie; returns status + raw body text so Python parses it
    # deterministically (no settle-window race on page.on("response")).
    _FETCH_JS = """async (path) => {
        try {
            const r = await fetch(path, {credentials: 'include',
                headers: {'accept': 'application/json'}});
            const t = await r.text();
            return {status: r.status, body: t};
        } catch (e) { return {status: null, body: null, error: String(e)}; }
    }"""

    def fetch_event(
        self,
        event_id,
        names=None,
        fetch_wait_ms: int = 250,
    ) -> Dict[str, dict]:
        """Pull the per-event endpoints via same-origin in-page ``fetch`` on
        the ALREADY-NAVIGATED page — no re-navigation (#842).

        A navigation costs ~2 MB of residential proxy per match: ``page.route``
        (resource blocking) disables the HTTP cache, so every nav re-downloads
        the SPA JS bundle. The session's first navigation already passed
        Turnstile, so a same-origin fetch carries the clearance cookie — the
        exact mechanism live-proven by :meth:`paginate_tournament_season`
        (#824). Only the JSON endpoints travel (~0.1-0.2 MB/match).

        Each response is parsed HERE and merged into the buffer with the same
        record shape as :meth:`_on_response` (which also fires for the fetch —
        ``merge_capture`` keeps the best record per path), then read back via
        :func:`select_event_endpoints`. A challenged endpoint (Turnstile
        clearance expired) or a failed fetch just stays missing — the caller
        falls back to a full :meth:`capture_event` navigation, which re-solves
        Turnstile for the fetches that follow. MUST be called after a
        navigation in this session (the fetch needs the sofascore.com origin).
        """
        names = tuple(names or _EVENT_ENDPOINTS)
        self._buffer = {}
        for name in names:
            tmpl = _EVENT_ENDPOINTS.get(name)
            if tmpl is None:
                continue
            path = tmpl.format(eid=event_id)
            try:
                res = self._page.evaluate(self._FETCH_JS, path) or {}
            except Exception as e:  # noqa: BLE001 — one endpoint mustn't kill the pass
                logger.info("sofascore fetch event=%s %s failed: %s",
                            event_id, name, e)
                res = {}
            rec = {"status": res.get("status"), "json": None, "challenge": None}
            body = res.get("body")
            if body:
                try:
                    obj = json.loads(body)
                    rec["json"] = obj
                    rec["challenge"] = is_challenge(obj)
                except Exception:  # noqa: BLE001 — non-JSON body
                    pass
            self._buffer[path] = merge_capture(self._buffer.get(path), rec)
            try:
                self._page.wait_for_timeout(fetch_wait_ms)
            except Exception:  # noqa: BLE001 — pacing must not break the pass
                pass
        result = select_event_endpoints(self._buffer, event_id)
        self._log_event_capture(event_id, result, kind="fetch")
        return result

    def _log_event_capture(self, event_id, result, kind: str) -> None:
        """Per-event capture summary + byte/block deltas (#842 accounting)."""
        missing = [k for k in _EVENT_ENDPOINTS if k not in result]
        delta = self._bytes_total - self._bytes_at_event_start
        self._bytes_at_event_start = self._bytes_total
        blocked = self._blocked_count - self._blocked_at_event_start
        self._blocked_at_event_start = self._blocked_count
        logger.info("sofascore %s event=%s got=%s missing=%s bytes=%.0fKB blocked=%d",
                    kind, event_id, sorted(result), missing, delta / 1024, blocked)

    def capture_player(self, player_id, *, season_picker_label=None) -> Dict:
        """Navigate a player page, read the SSR'd bio from ``__NEXT_DATA__``, and
        (when ``season_picker_label`` is given) drive the Season tab + picker to
        capture the target competition's season-aggregate stats — all in ONE
        navigation (#751 PR3 profile + PR3b season stats).

        Returns ``{'profile': <bio dict | None>, 'season_buffer': {path: rec}}``.
        The bio is server-rendered (``props.pageProps.player``), NOT an XHR. The
        ``season_buffer`` is the raw season-stats + ``/statistics/seasons``
        captures; the caller selects the target ``(ut, season)`` via the pure,
        season-guarded :func:`select_player_season_stats` /
        :func:`extract_player_seasons_map`. ``season_picker_label`` is the
        tournament display text to click in the picker (e.g. ``'Premier
        League'``) — ``None`` keeps the PR3 profile-only behaviour."""
        self._buffer = {}
        self._navigate(player_url(player_id))
        profile = self._extract_player_next_data(player_id)

        season_buffer: Dict[str, dict] = {}
        if season_picker_label:
            # Same nav: open the Season tab (fires /statistics/seasons + the
            # default competition's overall), then switch the picker to the
            # target tournament so ITS overall fires too.
            self._click_tabs(("Season",))
            self._drive_season_picker(season_picker_label)
            self._page.wait_for_timeout(self._tab_wait_ms)
            season_buffer = {
                p: r for p, r in self._buffer.items()
                if _PLAYER_SEASON_STATS_RE.search(p) or _PLAYER_SEASONS_LIST_RE.search(p)
            }

        delta = self._bytes_total - self._bytes_at_event_start
        self._bytes_at_event_start = self._bytes_total
        blocked = self._blocked_count - self._blocked_at_event_start
        self._blocked_at_event_start = self._blocked_count
        logger.info("sofascore capture player=%s profile=%s season_xhrs=%d bytes=%.0fKB blocked=%d",
                    player_id, profile is not None, len(season_buffer), delta / 1024, blocked)
        return {"profile": profile, "season_buffer": season_buffer}

    def _drive_season_picker(self, tournament_label: str) -> None:
        """Best-effort: open the season-statistics TOURNAMENT dropdown and select
        the target competition so its season-statistics/overall XHR fires.

        The default Season tab shows the player's PRIMARY competition — NOT
        necessarily EPL for a transferred/multi-competition player (live-proven
        Paquetá → World Cup, #751 PR3b). Live-proven DOM (probe): the widget has
        a tournament dropdown — ``button.dropdown__button[aria-haspopup=
        "listbox"]`` whose label is the current competition NAME (it has letters;
        the sibling SEASON dropdown shows only a year). Clicking it opens a
        ``ul.dropdown__list`` of ``li[role="option"]`` tournaments; clicking the
        target option fires its overall XHR (Paquetá → EPL ut=17/season 76986
        confirmed). Tournament names are NOT localized (English even under a
        non-EN proxy), so matching the option by text is locale-safe. A miss just
        leaves the default tab — :func:`select_player_season_stats` then returns
        ``None`` for that player (a WARN, not a crash). JS clicks bypass
        Playwright actionability (same trick as :meth:`_click_tabs`)."""
        try:
            opened = self._page.evaluate(
                """() => {
                    // The tournament dropdown trigger (NOT the year/season one):
                    // a dropdown__button whose label carries letters.
                    const btn = [...document.querySelectorAll(
                            'button.dropdown__button[aria-haspopup="listbox"],'
                            + '[aria-haspopup="listbox"]')]
                        .find(e => /[a-z]/i.test((e.innerText||'')));
                    if (btn) { btn.click(); return true; }
                    return false;
                }"""
            )
            if not opened:
                logger.info("sofascore season-picker: tournament dropdown not found")
                return
            self._page.wait_for_timeout(self._tab_wait_ms)
            clicked = self._page.evaluate(
                """(label) => {
                    // ONLY the dropdown options — never the page's plain
                    // 'Premier League' <a> link (that navigates, fires nothing).
                    const want = label.toLowerCase();
                    const opts = [...document.querySelectorAll(
                        'li[role="option"], .dropdown__listItem')];
                    let opt = opts.find(
                        e => (e.innerText||'').trim().toLowerCase() === want);
                    if (!opt) opt = opts.find(
                        e => (e.innerText||'').toLowerCase().includes(want));
                    if (opt) { opt.click(); return true; }
                    return false;
                }""",
                tournament_label,
            )
            logger.info("sofascore season-picker select %r -> %s",
                        tournament_label, clicked)
            self._page.wait_for_timeout(self._tab_wait_ms)
        except Exception as e:  # noqa: BLE001 — picker driving is best-effort
            logger.info("sofascore season-picker drive failed: %s", e)

    def fetch_player(
        self,
        player_id,
        target_ut=None,
        target_year=None,
        fetch_wait_ms: int = 250,
    ) -> Dict:
        """Pull a player's bio + season-aggregate stats via same-origin
        in-page ``fetch`` — no navigation (#842). Returns the same
        ``{'profile', 'season_buffer'}`` shape as :meth:`capture_player`.

        The player page SSRs ``props.pageProps.player`` from the very same
        ``/api/v1/player/{id}`` payload (live-probed 2026-07-02: identical
        keys AND values), so the fetch replaces reading ``__NEXT_DATA__``.
        Season stats skip the Season-tab picker entirely:
        ``/statistics/seasons`` resolves the target ``(ut, year)`` →
        ``season_id`` in Python, then the exact ``statistics/overall`` is
        fetched — more precise than the picker, which missed for transferred
        players (#751 PR3b). A challenged/failed bio fetch returns
        ``profile=None`` — the caller falls back to a full
        :meth:`capture_player` navigation (re-solves Turnstile). MUST be
        called after a navigation in this session (same-origin fetch).
        """
        pid = str(player_id)
        buffer: Dict[str, dict] = {}

        def _fetch(path: str) -> dict:
            try:
                res = self._page.evaluate(self._FETCH_JS, path) or {}
            except Exception as e:  # noqa: BLE001 — one endpoint mustn't kill the pass
                logger.info("sofascore fetch player=%s %s failed: %s",
                            pid, path, e)
                res = {}
            rec = {"status": res.get("status"), "json": None, "challenge": None}
            body = res.get("body")
            if body:
                try:
                    obj = json.loads(body)
                    rec["json"] = obj
                    rec["challenge"] = is_challenge(obj)
                except Exception:  # noqa: BLE001 — non-JSON body
                    pass
            buffer[path] = rec
            try:
                self._page.wait_for_timeout(fetch_wait_ms)
            except Exception:  # noqa: BLE001 — pacing must not break the pass
                pass
            return rec

        rec = _fetch(f"/api/v1/player/{pid}")
        profile = None
        if (rec.get("status") == 200 and rec.get("challenge") is False
                and isinstance(rec.get("json"), dict)):
            p = rec["json"].get("player")
            profile = p if isinstance(p, dict) else None

        season_buffer: Dict[str, dict] = {}
        if profile is not None and target_ut is not None:
            _fetch(f"/api/v1/player/{pid}/statistics/seasons")
            sid = (extract_player_seasons_map(buffer, pid)
                   .get(int(target_ut)) or {}).get(str(target_year))
            if sid is not None:
                _fetch(
                    f"/api/v1/player/{pid}/unique-tournament/{int(target_ut)}"
                    f"/season/{int(sid)}/statistics/overall")
            season_buffer = {
                p: r for p, r in buffer.items()
                if _PLAYER_SEASON_STATS_RE.search(p)
                or _PLAYER_SEASONS_LIST_RE.search(p)
            }

        delta = self._bytes_total - self._bytes_at_event_start
        self._bytes_at_event_start = self._bytes_total
        blocked = self._blocked_count - self._blocked_at_event_start
        self._blocked_at_event_start = self._blocked_count
        logger.info(
            "sofascore fetch player=%s profile=%s season_xhrs=%d "
            "bytes=%.0fKB blocked=%d",
            pid, profile is not None, len(season_buffer), delta / 1024, blocked)
        return {"profile": profile, "season_buffer": season_buffer}

    def capture_buffer(self, nav_url: str) -> Dict[str, dict]:
        """Navigate ``nav_url`` and return the whole capture buffer (path -> rec).
        Used for non-event pages (tournament events lists, standings)."""
        self._buffer = {}
        self._navigate(nav_url)
        return dict(self._buffer)

    def capture_tournament(self, nav_url: str) -> Dict[str, dict]:
        """Navigate a league page, nudge it toward FINISHED results (Matches /
        previous rounds), and return the whole capture buffer. The caller runs
        ``extract_tournament_events(buffer, ut_id)`` to pull the match list.

        The default tournament view loads only the upcoming round; finished
        matches need an interaction to fire their ``/events/{round,last}`` XHR
        (#757 B1). Interaction is best-effort — a miss yields the default round.
        """
        self._buffer = {}
        self._navigate(nav_url)
        # Scroll to trigger lazy widgets, then nudge toward finished matches.
        try:
            self._page.mouse.wheel(0, 4000)
            self._page.wait_for_timeout(self._tab_wait_ms)
        except Exception:
            pass
        self._nudge_results()
        # Let late nudge-triggered XHR (and their body reads) settle into the
        # buffer before we snapshot it — without this the by-date events race.
        self._page.wait_for_timeout(self._settle_ms)
        return dict(self._buffer)

    def paginate_tournament_season(
        self, ut_id, season_id, max_pages: int = 25,
    ) -> Dict[str, dict]:
        """Page a SPECIFIC season's finished events on the ALREADY-NAVIGATED
        tournament page, so a historical-season backfill sees the target
        season's matches (#824).

        The landing only fires the CURRENT/next season's ``/events/...`` XHR, so
        :func:`extract_tournament_events` finds nothing for a past season. Here
        we drive an in-page ``fetch`` of ``/unique-tournament/{ut}/season/{sid}/
        events/last/{page}`` for page=0.. until a page returns no events or
        ``hasNextPage`` is false. The page already passed Turnstile on
        navigation, so a same-origin fetch carries the clearance cookie; each
        response is captured by :meth:`_on_response` into ``self._buffer`` (keyed
        by path) and read back by :func:`extract_tournament_events`. Returns the
        whole buffer. Best-effort: a failed/challenged page stops the loop,
        leaving whatever paged in. Call within the SAME capture session right
        after a tournament navigation (no re-nav — that would re-warm Turnstile
        and burn the rate-limit budget)."""
        base = (
            f"/api/v1/unique-tournament/{int(ut_id)}/season/{int(season_id)}"
            "/events/last/"
        )
        pages = 0
        for page in range(max_pages):
            try:
                res = self._page.evaluate(
                    """async (path) => {
                        try {
                            const r = await fetch(path, {credentials: 'include',
                                headers: {'accept': 'application/json'}});
                            if (!r.ok) return {ok: false, count: 0, more: false};
                            const j = await r.json();
                            const ev = Array.isArray(j.events) ? j.events : [];
                            return {ok: true, count: ev.length,
                                    more: !!j.hasNextPage};
                        } catch (e) { return {ok: false, count: 0, more: false}; }
                    }""",
                    base + str(page),
                )
            except Exception as e:  # noqa: BLE001 — paging is best-effort
                logger.info("sofascore season-paging ut=%s sid=%s page=%s: %s",
                            ut_id, season_id, page, e)
                break
            self._page.wait_for_timeout(self._tab_wait_ms)
            if not res or not res.get("ok") or not res.get("count"):
                break
            pages += 1
            if not res.get("more"):
                break
        logger.info("sofascore season-paging ut=%s sid=%s: %d page(s).",
                    ut_id, season_id, pages)
        # Let the last response bodies settle into the buffer before snapshot.
        self._page.wait_for_timeout(self._settle_ms)
        return dict(self._buffer)

    def _nudge_results(self) -> None:
        """Surface FINISHED matches: open the 'Matches' top-nav, then switch to
        the BY DATE view (centres on recent/finished matches for an in-progress
        season, firing /events/last|next + round XHR).

        The Matches section is collapsed by default (Standings is the landing
        view), so its toggles (``data-testid=tab-date/tab-round``) render
        ``display:none`` and a Playwright ``.click()`` times out as non-actionable.
        A JS ``.click()`` bypasses actionability and follows the SPA's own
        handlers — live-proven on the EPL page (#757 B1: 10→30 captured events).
        Best-effort: a miss just leaves the default round.
        """
        # tab-date/tab-round are embedded on the landing page's matches widget
        # (display:none until activated). Click each DIRECTLY by its stable
        # data-testid — clicking the 'Matches' top-nav first re-mounts the
        # section and the toggles vanish before the next click lands. Render
        # timing varies by proxy exit, so wait for each toggle to mount first.
        for tid in ("tab-date", "tab-round"):
            sel = '[data-testid="' + tid + '"]'
            try:
                self._page.wait_for_function(
                    "() => !!document.querySelector('" + sel + "')", timeout=8000,
                )
                hit = self._page.evaluate(
                    "() => { const e = document.querySelector('" + sel + "');"
                    " if (e) { e.click(); return true; } return false; }"
                )
                logger.info("sofascore nudge %s -> %s", tid, hit)
                self._page.wait_for_timeout(self._tab_wait_ms)
            except Exception as e:  # noqa: BLE001 — nudge is best-effort
                logger.info("sofascore nudge %s: not mounted (%s)", tid, e)

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

    def _click_tabs(self, tabs=_EVENT_TABS) -> None:
        for label in tabs:
            clicked = False
            # Prefer the stable data-testid via a JS click (bypasses Playwright
            # actionability and follows the SPA's own handler — same trick as
            # _nudge_results). get_by_text is a fallback for tabs without a known
            # testid, but it can grab a non-tab label node (#751 PR2).
            testid = _TAB_TESTIDS.get(label)
            if testid:
                try:
                    clicked = bool(self._page.evaluate(
                        "(tid) => { const e = document.querySelector("
                        "'[data-testid=\"' + tid + '\"]');"
                        " if (e) { e.scrollIntoView(); e.click(); return true; }"
                        " return false; }",
                        testid,
                    ))
                except Exception:
                    clicked = False
            if not clicked:
                try:
                    self._page.get_by_text(label, exact=False).first.click(timeout=3000)
                    clicked = True
                except Exception:
                    pass
            if clicked:
                self._page.wait_for_timeout(self._tab_wait_ms)

    def _extract_player_next_data(self, player_id) -> Optional[dict]:
        """Read ``__NEXT_DATA__`` off the player page and dig the SSR'd bio out of
        it (see module-level :func:`extract_player_from_next_data`)."""
        try:
            text = self._page.evaluate(
                "() => { const e = document.getElementById('__NEXT_DATA__');"
                " return e ? e.textContent : null; }"
            )
        except Exception:  # noqa: BLE001
            logger.info("sofascore __NEXT_DATA__ read failed", exc_info=True)
            return None
        if not text:
            return None
        try:
            return extract_player_from_next_data(json.loads(text), player_id)
        except Exception:  # noqa: BLE001 — malformed SSR JSON
            logger.info("sofascore __NEXT_DATA__ parse failed", exc_info=True)
            return None
