"""One-session Camoufox data plane for exact SofaScore JSON endpoints.

The production manifest/capture engine decides which endpoint is missing and
owns retries, raw persistence and normalization. This module only maintains one
residential browser session, proves an official same-origin anchor, rate-limits
every browser request and returns exact response bytes. Heavy browser imports
remain lazy so DAG parsing and offline replay stay network/browser free.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
from collections import Counter
from typing import Callable, Dict, Optional
from urllib.parse import urlsplit

from scrapers.sofascore._flatten import _auto_flatten

logger = logging.getLogger(__name__)

BASE = "https://www.sofascore.com"
_API_PATH = "/api/v1/"
_EXACT_ANCHOR_URL = f"{BASE}/"
# Keep the explicit root slash. Browsers canonicalize an origin-only URL to
# ``/`` in ``Response.url``; omitting it makes our no-redirect equality check
# reject a valid direct 200 response even though no redirect occurred.
_CANARY_EXIT_PROBE_URL = "https://api.ipify.org/?format=json"
# Binary/static endpoints under /api/v1 that are not JSON data — skip them.
_NON_DATA_SEGMENTS = ("/image", "/flag", "/logo", "/jersey")

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
    # Browser/extension background filter-list updates observed by the exact
    # paid canary. They are unrelated to SofaScore/Turnstile and otherwise
    # consume both limiter tokens and blocked proxy attempts during warm-up.
    "ublockorigin.github.io", "ublockorigin.pages.dev", "cdn.jsdelivr.net",
    "raw.githubusercontent.com", "malware-filter.gitlab.io", "pgl.yoyo.org",
    "curbengh.github.io", "malware-filter.pages.dev", "publicsuffix.org",
    "accounts.google.com",
)


# --------------------------------------------------------------------------- #
#  Pure helpers (unit-tested without a browser)                               #
# --------------------------------------------------------------------------- #
def _anchor_url_evidence(value: object) -> dict[str, object]:
    """Return URL-shape evidence without retaining secrets or the full URL."""

    evidence: dict[str, object] = {
        "type": type(value).__name__,
        "scheme": "",
        "hostname": "",
        "port": None,
        "path": "",
        "query_present": False,
        "fragment_present": False,
        "userinfo_present": False,
    }
    try:
        parsed = urlsplit(str(value))
        evidence.update(
            {
                "scheme": parsed.scheme.lower(),
                "hostname": (parsed.hostname or "").lower(),
                "path": parsed.path,
                "query_present": bool(parsed.query),
                "fragment_present": bool(parsed.fragment),
                "userinfo_present": (
                    parsed.username is not None or parsed.password is not None
                ),
            }
        )
        try:
            evidence["port"] = parsed.port
        except ValueError:
            evidence["port"] = "invalid"
    except (TypeError, ValueError):
        pass
    return evidence


def _is_exact_anchor_url(value: object) -> bool:
    """Accept only the canonical official root HTML anchor."""

    evidence = _anchor_url_evidence(value)
    return (
        isinstance(value, str)
        and evidence["scheme"] == "https"
        and evidence["hostname"] == "www.sofascore.com"
        and evidence["port"] in (None, 443)
        and evidence["path"] == "/"
        and evidence["query_present"] is False
        and evidence["fragment_present"] is False
        and evidence["userinfo_present"] is False
    )


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


# --------------------------------------------------------------------------- #
#  Camoufox capture session                                                   #
# --------------------------------------------------------------------------- #
class SofascoreCamoufoxCapture:
    """A single warmed Camoufox session for exact SofaScore JSON."""

    def __init__(
        self,
        proxy: Optional[dict] = None,
        *,
        geoip: bool = True,
        headless="virtual",
        nav_timeout_ms: int = 60000,
        block_resources: bool = True,
        request_limiter: Optional[Callable[[], object]] = None,
    ) -> None:
        self._proxy = proxy
        self._geoip = geoip
        self._headless = headless
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
        self._bytes_by_type: Counter = Counter()
        self._blocked_count = 0
        self._navigation_count = 0
        self._api_fetch_count = 0
        self._source_request_count = 0
        # Once the paid session is warmed, production raw-first capture locks the
        # page down to one explicitly authorised JSON path at a time.  This stops
        # SPA polling/featured-tournament XHRs from spending provider bytes that
        # have no EndpointSpec/raw-manifest owner.
        self._exact_only = False
        self._block_api_during_warm = False
        self._allowed_api_path: Optional[str] = None
        self._allowed_external_url: Optional[str] = None

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
        Any routing/validation/limiter error fails closed before provider I/O."""

        def abort() -> None:
            self._blocked_count += 1
            try:
                route.abort()
            except Exception:  # noqa: BLE001 - already handled is still closed
                pass

        try:
            req = route.request
            if self._block_api_during_warm and is_data_api_url(req.url):
                abort()
                return
            if self._exact_only:
                allowed = (
                    is_data_api_url(req.url)
                    and response_path(req.url) == self._allowed_api_path
                ) or req.url == self._allowed_external_url or any(
                    value in req.url.lower() for value in _ALLOW_URL_SUBSTRINGS
                )
                if not allowed:
                    abort()
                    return
            if (
                self._effective_block_resources
                and should_block_request(req.resource_type, req.url)
            ):
                abort()
                return
            # This is the last gate before real I/O. It observes documents,
            # scripts and challenge assets as well as JSON, while blocked
            # requests correctly consume neither limiter capacity nor bytes.
            self._before_source_request()
            route.continue_()
        except Exception:  # noqa: BLE001 - never turn validation errors into I/O
            abort()

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
        self._source_request_count += 1

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

    def fetch_api_json(self, path: str) -> Optional[dict]:
        """Fetch one exact ``/api/v1/...`` path in the warmed browser context.

        SofaScore's edge currently permits the JSON as a browser document but
        returns 403 to fetch/APIRequestContext on the same residential session.
        Reuse the one warmed page and navigate it to each exact JSON document.
        The route gate still paces and authorizes every request, while the
        provider lease remains the hard byte boundary.  ``Response.body``
        retains exact bytes without a JavaScript/base64 round-trip.

        Return the buffer-shaped capture record
        (``{'status', 'json', 'challenge'}``), or ``None`` when the in-page
        fetch itself failed (#879).

        A 200 record with no usable rows is a legitimate-empty answer;
        ``None`` is a transport failure worth retrying. The exact bytes are
        returned for raw storage and offline replay.
        """
        path = response_path(str(path))
        self._api_fetch_count += 1
        previous_allowed = self._allowed_api_path
        if self._exact_only:
            self._allowed_api_path = path
        try:
            self._navigation_count += 1
            response = self._page.goto(
                f"{BASE}{path}",
                wait_until="commit",
                timeout=self._nav_timeout_ms,
            )
        except Exception as e:  # noqa: BLE001 — a probe fetch mustn't kill the run
            logger.info(
                "sofascore fetch_api_json %s failed: %s",
                path,
                type(e).__name__,
            )
            return None
        finally:
            if self._exact_only:
                self._allowed_api_path = previous_allowed
        if response is None:
            return None
        try:
            status = int(response.status)
            body = response.body()
            headers = dict(response.headers or {})
        except Exception as e:  # noqa: BLE001 — response read can fail on teardown
            logger.info(
                "sofascore fetch_api_json %s response failed: %s",
                path,
                type(e).__name__,
            )
            return None
        rec = {
            "status": status,
            "json": None,
            "challenge": None,
            "body": body,
            "headers": headers,
        }
        try:
            obj = json.loads(body.decode("utf-8"))
            rec["json"] = obj
            rec["challenge"] = is_challenge(obj)
        except Exception:  # noqa: BLE001 — exact body may be non-JSON/binary
            pass
        self._buffer[path] = merge_capture(self._buffer.get(path), rec)
        return rec

    def warm_exact_json(self, nav_url: str) -> None:
        """Warm one browser origin, then allow only authorised exact JSON.

        The official root is a regular SofaScore HTML document with a CSP that
        permits same-origin API fetches (entity URLs can be 404 SPA shells).
        Live evidence showed that ``/football`` resolves to this root; opening
        the root directly avoids a hidden redirect and its unpaced proxy bytes.
        Exact-only routing is enabled before navigation:
        only its document and required Cloudflare challenge assets may load;
        SPA/static requests and passive APIs are aborted. A successful logical
        session therefore has one navigation followed only by exact fetches.

        A caller must invoke this method from inside an already budget-authorised
        transport request so the warm-up bytes are attributed to that first
        endpoint.
        """

        parsed = urlsplit(str(nav_url))
        if parsed.scheme != "https" or parsed.hostname not in {
            "www.sofascore.com",
            "api.sofascore.com",
        }:
            raise ValueError("SofaScore warm URL must be an official HTTPS URL")
        self._exact_only = True
        self._block_api_during_warm = True
        self._allowed_api_path = None
        previous_external = self._allowed_external_url
        self._allowed_external_url = _EXACT_ANCHOR_URL
        try:
            self._navigate_exact_origin(_EXACT_ANCHOR_URL)
        finally:
            self._allowed_external_url = previous_external
            self._block_api_during_warm = False

    def _navigate_exact_origin(self, url: str) -> None:
        """Load and prove the one lightweight official-origin anchor."""

        if not _is_exact_anchor_url(url):
            raise ValueError("SofaScore anchor must be canonical official HTTPS")
        source_before = self._source_request_count
        self._navigation_count += 1
        response = self._page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=self._nav_timeout_ms,
        )
        status = int(response.status) if response is not None else 0
        final_url_evidence = _anchor_url_evidence(
            response.url if response is not None else None
        )
        anchor_url_ok = bool(
            response is not None and _is_exact_anchor_url(response.url)
        )
        origin_ok = self._page.evaluate("() => location.origin") == BASE
        source_request_delta = self._source_request_count - source_before
        if (
            response is None
            or not anchor_url_ok
            or status != 200
            or not origin_ok
            or source_request_delta != 1
        ):
            raise RuntimeError(
                "SofaScore exact anchor did not prove official origin: "
                f"status={status} anchor_url_ok={anchor_url_ok} "
                f"origin_ok={origin_ok} source_request_delta={source_request_delta} "
                "final_url_evidence="
                + json.dumps(
                    final_url_evidence,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )

    def probe_proxy_exit(self) -> str:
        """Return a validated canary exit IP without retaining its response.

        The filtering proxy independently permits this fixed host only for an
        isolated ``sofascore_canary`` lease.  Exact-only routing opens the one
        URL only for the duration of this fetch. Redirects and any non-IP body
        fail closed; diagnostics never interpolate the response value.
        """

        previous = self._allowed_external_url
        self._allowed_external_url = _CANARY_EXIT_PROBE_URL
        try:
            response = self._page.evaluate(
                """async (url) => {
                    try {
                        const r = await fetch(url, {
                            credentials: 'omit', cache: 'no-store',
                            redirect: 'manual', headers: {'accept': 'application/json'}
                        });
                        return {status: r.status, body: await r.text(),
                            url: r.url, redirected: r.redirected};
                    } catch (e) {
                        return {status: null, body: null, url: null,
                            redirected: false};
                    }
                }""",
                _CANARY_EXIT_PROBE_URL,
            ) or {}
        finally:
            self._allowed_external_url = previous
        if (
            response.get("status") != 200
            or response.get("url") != _CANARY_EXIT_PROBE_URL
            or response.get("redirected") is not False
        ):
            raise RuntimeError("SofaScore canary exit probe was not a direct HTTP 200")
        try:
            payload = json.loads(response.get("body") or "")
            raw_ip = payload.get("ip") if isinstance(payload, dict) else None
            return str(ipaddress.ip_address(raw_ip))
        except (TypeError, ValueError, json.JSONDecodeError):
            raise RuntimeError(
                "SofaScore canary exit probe returned no valid IP"
            ) from None
