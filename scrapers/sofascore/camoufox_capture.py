"""
Camoufox passive-capture transport for SofaScore (issue #757, path P2).

Why this module exists
----------------------
SofaScore's data API is gated by Cloudflare Turnstile (proven in #757). A cold
datacenter request gets ``403 {"reason":"challenge"}``. The verified capture is a
real **Firefox (Camoufox) driven through a residential proxy**, where Turnstile
passes invisibly and the SPA's own XHRs to **same-origin**
``www.sofascore.com/api/v1/*`` return real JSON. An in-page ``fetch()`` 403s
only while Turnstile is UNSOLVED (datacenter IP / cold page); once a navigation
has passed the challenge, a same-origin fetch carries the clearance cookie and
succeeds — proven by :meth:`paginate_tournament_season` (#824) and now the
basis of the cheap per-match path :meth:`fetch_event` (#842). Navigation +
passive capture (``page.on("response")``) remains the warm-up and fallback.
The live-probe shapes are preserved as offline fixtures under
``tests/fixtures/sofascore_*``.

This transport drives Camoufox via Playwright, uses one SPA navigation to obtain
Turnstile clearance, then requests exact same-origin JSON paths. Downstream
parsing reuses the existing helpers on ``SofaScoreScraper``
(``_flatten_lineup_side``, ``_flatten_event_player_stats_from_lineups``,
``_flatten_match_stats`` and ``_flatten_shotmap``).

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
from typing import Callable, Dict, Optional

from scrapers.sofascore._flatten import _auto_flatten

logger = logging.getLogger(__name__)

BASE = "https://www.sofascore.com"
_API_PATH = "/api/v1/"
# Binary/static endpoints under /api/v1 that are not JSON data — skip them.
_NON_DATA_SEGMENTS = ("/image", "/flag", "/logo", "/jersey")

_CONSENT_BUTTONS = ("Consent", "AGREE", "Agree", "Accept all", "I Accept", "Got it")
# Logical endpoint groups retained for the existing caller API. No DOM clicks
# are performed; labels map directly to exact JSON endpoints.
_EVENT_TABS = ("Lineups", "Statistics", "Shotmap", "Incidents")
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
    "Shotmap": ("shotmap",),
    "Incidents": ("incidents",),
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
# Analytics / tracking hosts to abort regardless of type. Do NOT add
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

    def _quality(rec: dict) -> int:
        # A real endpoint answer always outranks a later challenge/error JSON;
        # any parsed JSON in turn outranks an empty 304/body-read race.
        if (
            rec.get("status") == 200
            and rec.get("challenge") is False
            and rec.get("json") is not None
        ):
            return 3
        if rec.get("status") in (204, 404):
            return 2
        if rec.get("json") is not None:
            return 1
        return 0

    return existing if _quality(existing) > _quality(new) else new


def event_url(event_id) -> str:
    return f"{BASE}/event/{event_id}"


def select_event_endpoints(buffer: Dict[str, dict], event_id) -> Dict[str, dict]:
    """From a capture ``buffer`` (path -> {status, json, challenge}), return the
    canonical per-event endpoints that came back as real JSON, keyed by entity
    name (``event``/``lineups``/``statistics``/``shotmap``)."""
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
    # #913: season.year is a str for clubs ('25/26') but an INT for
    # single-year cups (2026 on INT-World Cup) — bronze season_year is
    # varchar, and a mixed-type batch breaks pa.Table.from_pandas (same
    # class of bug as the #840 match_stats home/away fix). Pin to str.
    if row.get("season_year") is not None:
        row["season_year"] = str(row["season_year"])
    # Frozen compatibility superset for fresh Bronze bootstrap.  Static Trino
    # SQL resolves every COALESCE operand before execution, so merely writing
    # the new source-key names makes a clean install fail on the absent legacy
    # columns.  Keep both names nullable until the downstream migration drops
    # the compatibility operands.
    row.setdefault("home_team", row.get("home_team_name"))
    row.setdefault("away_team", row.get("away_team_name"))
    row.setdefault("home_score", row.get("home_score_current"))
    row.setdefault("away_score", row.get("away_score_current"))
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
    # A league fires ONE 'total' block; a group tournament fires one 'total'
    # block PER GROUP (INT-World Cup 2026 = 12 blocks, #913). Collect them all
    # and stamp each row with its block's group name — taking only the first
    # block silently collapses the cup to Group A.
    blocks = [s for s in standings if isinstance(s, dict)
              and s.get("type") == "total"]
    if not blocks and isinstance(standings[0], dict):
        blocks = [standings[0]]
    multi_group = len(blocks) > 1
    rows: list = []
    for block in blocks:
        block_rows = block.get("rows")
        if not isinstance(block_rows, list):
            continue
        group_name = block.get("name") if multi_group else None
        for row in block_rows:
            if multi_group and isinstance(row, dict) and not row.get("group"):
                row = {**row, "group": group_name}
            rows.append(row)
    return rows


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
        # group support for WC (Фаза 4 #913). Try to pull from row if the
        # standings response includes per-group info (e.g. for INT-World Cup).
        # Natural-key scope: a team may appear in more than one stage/block.
        # Keep total-table rows non-null for Iceberg MERGE; Silver maps this
        # sentinel back to NULL for the public group_id contract.
        "group": row.get("group") or "__total__",
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
# The bio is SSR'd at __NEXT_DATA__.props.pageProps.player and is also available
# from the exact ``/api/v1/player/{id}`` path once the session is warm.
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
#  Per-player season-stat helpers (issue #751 PR3b)                            #
# --------------------------------------------------------------------------- #
# ``/statistics/seasons`` provides the year→season-id map. The capture resolves
# the requested tournament/year in Python and fetches only that exact
# ``statistics/overall`` path, avoiding the page's often-wrong default
# competition for transferred players.
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
        request_limiter: Optional[Callable[[], object]] = None,
    ) -> None:
        self._proxy = proxy
        self._geoip = geoip
        self._headless = headless
        self._settle_ms = settle_ms
        self._tab_wait_ms = tab_wait_ms
        self._nav_timeout_ms = nav_timeout_ms
        self._block_resources = block_resources
        self._effective_block_resources = block_resources
        self._request_limiter = request_limiter
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
        self._navigation_count = 0
        self._api_fetch_count = 0

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
        try:
            self._browser = self._cm.__enter__()
            self._page = self._browser.new_page()
            # Cut proxy bytes by aborting images/fonts/media/CSS/analytics (#842).
            # Env kill-switch lets ops disable without a redeploy if Turnstile 403s.
            env = os.environ.get("SOFASCORE_BLOCK_RESOURCES")
            block = self._block_resources if env is None else env.strip().lower() not in ("0", "false", "no")
            self._effective_block_resources = block
            # The route is always installed: besides optional static blocking it
            # paces every actual /api/v1 request, including passive SPA XHRs.
            # Trade-off: page.route disables the browser HTTP cache, which is why
            # the canary measures the routed production configuration.
            self._page.route("**/*", self._maybe_block)
            self._page.on("response", self._on_response)
            self._page.on("requestfinished", self._on_request_finished)
        except BaseException as e:
            # A failed start (proxy connect refused, geoip lookup, new_page crash)
            # leaves the sync-playwright loop running in this thread; without a
            # teardown the NEXT session in the same process dies with "Sync API
            # inside the asyncio loop" (#879, live-hit on ESP-2016/GER-2022).
            # Camoufox.__exit__ tolerates a browser that never opened.
            try:
                self._cm.__exit__(type(e), e, e.__traceback__)
            except Exception:  # noqa: BLE001 — teardown is best-effort
                logger.warning("Camoufox teardown after failed start also failed",
                               exc_info=True)
            self._cm = None
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        top = ", ".join(f"{t}={b // 1024}KB" for t, b in self._bytes_by_type.most_common(4))
        logger.info(
            "sofascore capture session total=%.1fMB nav=%d api_fetch=%d "
            "blocked=%d top=[%s]",
            self._bytes_total / 1_048_576, self._navigation_count,
            self._api_fetch_count, self._blocked_count, top,
        )
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
            if is_data_api_url(req.url):
                try:
                    self._before_source_request()
                except Exception:  # noqa: BLE001 - a refused limiter is fail-closed
                    self._blocked_count += 1
                    route.abort()
                    return
            if (
                self._effective_block_resources
                and should_block_request(req.resource_type, req.url)
            ):
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
    def _before_source_request(self) -> None:
        """Pace every explicit source request, not merely each logical event."""
        limiter = getattr(self, "_request_limiter", None)
        if limiter is not None and limiter() is False:
            raise RuntimeError("SofaScore request rate limiter refused a request")

    def _on_response(self, resp) -> None:
        url = resp.url
        if not is_data_api_url(url):
            return
        rec = {
            "status": resp.status,
            "json": None,
            "challenge": None,
            "body": None,
            "headers": {},
        }
        try:
            body = resp.body()
            if not isinstance(body, bytes):
                body = bytes(body)
            rec["body"] = body
            rec["headers"] = dict(resp.headers or {})
            obj = json.loads(body.decode('utf-8'))
            rec["json"] = obj
            rec["challenge"] = is_challenge(obj)
        except Exception:  # noqa: BLE001 — non-JSON / body unavailable
            try:
                obj = resp.json()
                rec["json"] = obj
                rec["challenge"] = is_challenge(obj)
            except Exception:
                pass
        path = response_path(url)
        # Never let a later empty/304 record overwrite a good earlier capture
        # (see merge_capture — re-nav serves cached endpoints as 304/no-body).
        self._buffer[path] = merge_capture(self._buffer.get(path), rec)

    def capture_event(
        self,
        event_id,
        required=("lineups",),
        max_attempts: int = 2,
        tabs=_EVENT_TABS,
        names=None,
    ) -> Dict[str, dict]:
        """Warm the match page and return requested per-event JSON endpoints.

        Navigation is only the Turnstile warm-up. After it settles, endpoints
        missing from the passive page load are requested directly by exact API
        path. This replaces localized/tab DOM clicks and their duplicate XHRs.
        ``tabs`` now declares which endpoint families the caller wants.
        """
        result: Dict[str, dict] = {}
        required = tuple(required or ())
        # Reset the buffer ONCE per event, then accumulate across retries. A
        # re-navigation serves already-captured endpoints as 304/no-body, so a
        # per-attempt reset would drop a good capture from an earlier pass
        # (statistics/shotmap vanished while the required lineups survived —
        # #751 PR2). merge_capture keeps the best record per path.
        self._buffer = {}
        wanted = tuple(names) if names is not None else fetch_names_for_tabs(tabs)
        for attempt in range(1, max_attempts + 1):
            # Widen the settle window on retries so the challenge has time to
            # settle before the exact API fetches.
            self._navigate(event_url(event_id), extra_settle_ms=(attempt - 1) * 2000)
            result = select_event_endpoints(self._buffer, event_id)
            for name in wanted:
                if name in result:
                    continue
                path = _EVENT_ENDPOINTS[name].format(eid=event_id)
                self.fetch_api_json(path)
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
            return {status: r.status, body: t,
                headers: Object.fromEntries(r.headers.entries())};
        } catch (e) { return {status: null, body: null, error: String(e)}; }
    }"""

    def fetch_event(
        self,
        event_id,
        names=None,
        fetch_wait_ms: int = 0,
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
            self._api_fetch_count += 1
            try:
                res = self._page.evaluate(self._FETCH_JS, path) or {}
            except Exception as e:  # noqa: BLE001 — one endpoint mustn't kill the pass
                logger.info("sofascore fetch event=%s %s failed: %s",
                            event_id, name, e)
                res = {}
            rec = {
                "status": res.get("status"),
                "json": None,
                "challenge": None,
                "body": None,
                "headers": res.get("headers") or {},
            }
            body = res.get("body")
            if body:
                rec["body"] = body.encode('utf-8')
                try:
                    obj = json.loads(body)
                    rec["json"] = obj
                    rec["challenge"] = is_challenge(obj)
                except Exception:  # noqa: BLE001 — non-JSON body
                    pass
            self._buffer[path] = merge_capture(self._buffer.get(path), rec)
            if fetch_wait_ms > 0:
                try:
                    self._page.wait_for_timeout(fetch_wait_ms)
                except Exception:  # noqa: BLE001 — pacing must not break the pass
                    pass
        result = select_event_endpoints(self._buffer, event_id)
        self._log_event_capture(event_id, result, kind="fetch")
        return result

    def fetch_api_json(self, path: str) -> Optional[dict]:
        """Fetch an arbitrary same-origin ``/api/v1/...`` ``path`` on the
        ALREADY-NAVIGATED page and return its buffer-shaped capture record
        (``{'status', 'json', 'challenge'}``), or ``None`` when the in-page
        fetch itself failed (#879).

        Rides the session's Turnstile clearance exactly like
        :meth:`paginate_tournament_season` / :meth:`fetch_event`; the record is
        also merged into the live buffer so snapshot-based extractors see it.
        Callers feed the record to the season-guarded extractors as a
        one-entry buffer (``extract_tournament_seasons_map({path: rec}, ut)``)
        — a 200 record with no usable rows is a legitimate-empty answer, a
        ``None`` return is a transport failure worth retrying.
        """
        self._api_fetch_count += 1
        try:
            res = self._page.evaluate(self._FETCH_JS, path) or {}
        except Exception as e:  # noqa: BLE001 — a probe fetch mustn't kill the run
            logger.info("sofascore fetch_api_json %s failed: %s", path, e)
            return None
        rec = {
            "status": res.get("status"),
            "json": None,
            "challenge": None,
            "body": None,
            "headers": res.get("headers") or {},
        }
        body = res.get("body")
        if body:
            rec["body"] = body.encode('utf-8')
            try:
                obj = json.loads(body)
                rec["json"] = obj
                rec["challenge"] = is_challenge(obj)
            except Exception:  # noqa: BLE001 — non-JSON body
                pass
        self._buffer[path] = merge_capture(self._buffer.get(path), rec)
        return rec

    def event_endpoint_records(self, event_id, names=None) -> Dict[str, dict]:
        """Return exact response records for raw-first processing."""
        records: Dict[str, dict] = {}
        for name in tuple(names or _EVENT_ENDPOINTS):
            template = _EVENT_ENDPOINTS.get(name)
            if template is None:
                continue
            record = self._buffer.get(template.format(eid=event_id))
            if isinstance(record, dict):
                records[name] = dict(record)
        return records

    def event_endpoint_states(self, event_id, names=None) -> Dict[str, str]:
        """Classify the last response for each event endpoint.

        ``success`` and ``not_available`` are terminal source answers. All other
        states are retryable transport/source failures. Keeping this distinction
        lets the runner persist a completion manifest without treating an
        optional 404 as a permanent daily re-download or a transient 5xx as a
        completed match.
        """
        names = tuple(names or _EVENT_ENDPOINTS)
        states: Dict[str, str] = {}
        for name in names:
            tmpl = _EVENT_ENDPOINTS.get(name)
            if tmpl is None:
                continue
            rec = self._buffer.get(tmpl.format(eid=event_id))
            if not isinstance(rec, dict):
                states[name] = "missing"
                continue
            status = rec.get("status")
            if (
                status == 200
                and rec.get("challenge") is False
                and rec.get("json") is not None
            ):
                states[name] = "success"
            elif status in (204, 404):
                states[name] = "not_available"
            elif rec.get("challenge") is True or status == 403:
                states[name] = "blocked"
            elif status == 429:
                states[name] = "rate_limited"
            elif isinstance(status, int) and status >= 500:
                states[name] = "server_error"
            else:
                states[name] = "transport_error"
        return states

    def _log_event_capture(self, event_id, result, kind: str) -> None:
        """Per-event capture summary + byte/block deltas (#842 accounting)."""
        missing = [k for k in _EVENT_ENDPOINTS if k not in result]
        delta = self._bytes_total - self._bytes_at_event_start
        self._bytes_at_event_start = self._bytes_total
        blocked = self._blocked_count - self._blocked_at_event_start
        self._blocked_at_event_start = self._blocked_count
        logger.info("sofascore %s event=%s got=%s missing=%s bytes=%.0fKB blocked=%d",
                    kind, event_id, sorted(result), missing, delta / 1024, blocked)

    def capture_player(
        self,
        player_id,
        *,
        target_ut=None,
        target_year=None,
    ) -> Dict:
        """Warm the player page once, then fetch the exact target-season APIs.

        The old fallback clicked a localized Season tab and guessed which of two
        dropdowns was the tournament picker. That was brittle for transferred
        players and also fetched the default competition before the target one.
        The navigation still supplies the SSR profile and Turnstile clearance;
        deterministic same-origin requests resolve the exact ``(ut, season)``.
        """
        self._buffer = {}
        self._navigate(player_url(player_id))
        profile = self._extract_player_next_data(player_id)
        return self.fetch_player(
            player_id,
            target_ut=target_ut,
            target_year=target_year,
            profile=profile,
        )

    def fetch_player(
        self,
        player_id,
        target_ut=None,
        target_year=None,
        fetch_wait_ms: int = 0,
        profile=None,
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
            self._api_fetch_count += 1
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
            if fetch_wait_ms > 0:
                try:
                    self._page.wait_for_timeout(fetch_wait_ms)
                except Exception:  # noqa: BLE001 — pacing must not break the pass
                    pass
            return rec

        if profile is None:
            rec = _fetch(f"/api/v1/player/{pid}")
            if (
                rec.get("status") == 200
                and rec.get("challenge") is False
                and isinstance(rec.get("json"), dict)
            ):
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

    def paginate_tournament_season(
        self,
        ut_id,
        season_id,
        max_pages: int = 25,
        *,
        include_next: bool = False,
    ) -> Dict[str, dict]:
        """Fetch a SPECIFIC season's events on the ALREADY-NAVIGATED page.

        The landing only fires the CURRENT/next season's ``/events/...`` XHR, so
        :func:`extract_tournament_events` finds nothing for a past season. We
        request the exact ``events/last`` pages and, for schedule callers,
        ``events/next`` pages. Each response is parsed and merged synchronously
        through :meth:`fetch_api_json`; the old response-listener + fixed sleeps
        raced body reads and added 2.5 seconds per page plus an 8-second tail.

        ``include_next=False`` is the traffic-minimal finished-match path.
        Schedule capture enables it so future fixtures are not lost when the
        brittle DOM tab-click nudge is removed.
        """
        prefix = (
            f"/api/v1/unique-tournament/{int(ut_id)}/season/{int(season_id)}/events"
        )
        directions = ("last", "next") if include_next else ("last",)
        page_counts = {}
        for direction in directions:
            pages = 0
            for page in range(max_pages):
                path = f"{prefix}/{direction}/{page}"
                rec = self.fetch_api_json(path)
                if (
                    not rec
                    or rec.get("status") != 200
                    or rec.get("challenge") is not False
                ):
                    break
                obj = rec.get("json")
                events = obj.get("events") if isinstance(obj, dict) else None
                if not isinstance(events, list) or not events:
                    break
                pages += 1
                if not obj.get("hasNextPage"):
                    break
            page_counts[direction] = pages
        logger.info(
            "sofascore season-paging ut=%s sid=%s pages=%s",
            ut_id,
            season_id,
            page_counts,
        )
        return dict(self._buffer)

    # -- internals -------------------------------------------------------- #
    def _navigate(self, url: str, extra_settle_ms: int = 0) -> None:
        self._before_source_request()
        self._navigation_count += 1
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
