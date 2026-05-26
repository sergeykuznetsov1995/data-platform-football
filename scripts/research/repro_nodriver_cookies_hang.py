"""
Repro: does nodriver 0.48.1 still hang on the next page.get() after we
extract cookies from a freshly-bypassed FBref tab?

History: scrapers/fbref/browser_manager.py:524-527 disabled an HTTP-session
fast path because cookies.get_all() corrupted the event loop and the next
page.get() hung ~40s. That comment was written against an older nodriver
(likely 0.32-ish). We are on 0.48.1 now.

We test 3 cookie-extraction methods. Each runs in a FRESH browser to keep
event-loop state from leaking between phases. We also measure a "baseline"
phase (no extraction) so we know what a healthy next-page latency looks like
on this proxy/IP/CF state.

Methods:
 (a) await browser.cookies.get_all()                            # high-level API
 (b) await page.send(nodriver.cdp.network.get_cookies())        # raw CDP
 (c) await page.evaluate("document.cookie")                     # JS string (no HttpOnly!)
 (d) await page.send(_get_cookies_raw_generator())              # raw CDP, bypass Cookie.from_json
 (e) (d) wrapped in a websocket recv/send trap to reveal which
     session/id Chromium answers on — surfaces accumulation effect
     by running TWO sequential extracts on the same browser, the way
     production scraper does for 10 matches in a row.

Run inside the airflow-webserver container:
  docker exec -i airflow-webserver bash -c "cd /opt/airflow && \
    python scripts/research/repro_nodriver_cookies_hang.py"
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any

# Configure root logging so we see NodriverBypass [DIAG] lines if it stalls.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("repro")

import nodriver  # noqa: E402
import nodriver.cdp.network as cdp_network  # noqa: E402

# Reuse the same bypass path the production scraper uses, so we test the
# same CF workflow + slow_proxy_threshold + cf-verify plugin.
sys.path.insert(0, "/opt/airflow")
from scrapers.base.browser.nodriver_bypass import NodriverBypass  # noqa: E402

PROXY_FILE = "/opt/airflow/proxys.txt"
REPORT_PATH = "/tmp/nodriver_cookies_repro_v2.json"

LANDING_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
NEXT_URL = (
    "https://fbref.com/en/comps/9/2024-2025/2024-2025-Premier-League-Stats"
)

HANG_THRESHOLD_S = 20.0  # ">20s" = the bug reproduced (original comment said 40s)
CF_COOKIE_NAMES = {"cf_clearance", "__cf_bm", "cf_chl_opt"}
LANDING_RETRIES = 3              # if CF block still on first landing, retry on same browser
LANDING_RETRY_WAIT = 8.0         # seconds between landing retries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_proxy_url(path: str) -> str:
    """First non-empty line of proxys.txt, formatted as http://user:pass@host:port."""
    line = ""
    for raw in Path(path).read_text().splitlines():
        raw = raw.strip()
        if raw and not raw.startswith("#"):
            line = raw
            break
    if not line:
        raise RuntimeError(f"No proxies in {path}")
    parts = line.split(":")
    if len(parts) < 4:
        raise RuntimeError(f"Unexpected proxy format: {line[:30]}")
    host, port, user = parts[0], parts[1], parts[2]
    password = ":".join(parts[3:])
    return f"http://{user}:{password}@{host}:{port}"


def mask_proxy(url: str) -> str:
    if "@" not in url:
        return url
    return "http://****:****@" + url.split("@", 1)[1]


async def fresh_bypass(proxy: str) -> NodriverBypass:
    """Spin up a clean NodriverBypass with the same args we use in production."""
    bp = NodriverBypass(
        headless=True,
        proxy=proxy,
        cloudflare_wait=90.0,     # bumped from 30s; production uses 90s for stat tasks
        page_load_timeout=120.0,  # generous; we measure latency separately
        slow_proxy_threshold=0,   # disable SlowProxyError — we want raw timings
        max_retries=1,            # no retries; we want a single deterministic shot
        use_cf_verify=True,
    )
    await bp.start()
    return bp


async def land_and_pass_cf(bp: NodriverBypass, url: str) -> dict[str, Any]:
    """Hit landing URL; wait for CF + table presence. Retry on CF block.

    The first repro showed cf_blocked=true on landing even after
    wait_for_cloudflare. Production scraper has the same intermittency.
    We retry up to LANDING_RETRIES times on the SAME browser (so the
    CF challenge cookies can stack) before giving up.
    """
    info: dict[str, Any] = {"attempts": []}
    overall_t0 = time.monotonic()
    for attempt in range(LANDING_RETRIES):
        t0 = time.monotonic()
        try:
            html = await asyncio.wait_for(
                bp.get(url, wait_for_cloudflare=True), timeout=180
            )
            elapsed = round(time.monotonic() - t0, 2)
            has_table = "<table" in html
            cf_blocked = any(
                m in html.lower()
                for m in ("just a moment", "cf-browser-verification", "challenge-running")
            )
            info["attempts"].append({
                "attempt": attempt + 1,
                "seconds": elapsed,
                "html_size": len(html),
                "has_table": has_table,
                "cf_blocked": cf_blocked,
            })
            if has_table and not cf_blocked:
                # success — CF passed AND real page rendered
                break
            if attempt < LANDING_RETRIES - 1:
                log.info(
                    f"  landing attempt {attempt + 1} blocked "
                    f"(html={len(html)}b, has_table={has_table}, cf={cf_blocked}); "
                    f"waiting {LANDING_RETRY_WAIT}s and retrying"
                )
                await asyncio.sleep(LANDING_RETRY_WAIT)
        except Exception as e:  # noqa: BLE001
            info["attempts"].append({
                "attempt": attempt + 1,
                "seconds": round(time.monotonic() - t0, 2),
                "error": f"{type(e).__name__}: {e}",
            })
            if attempt < LANDING_RETRIES - 1:
                await asyncio.sleep(LANDING_RETRY_WAIT)
    last = info["attempts"][-1] if info["attempts"] else {}
    info["landing_seconds"] = round(time.monotonic() - overall_t0, 2)
    info["html_size"] = last.get("html_size", 0)
    info["has_table"] = last.get("has_table", False)
    info["cf_blocked"] = last.get("cf_blocked", True)
    info["landing_error"] = last.get("error")
    return info


async def time_next_page_get(bp: NodriverBypass, url: str) -> dict[str, Any]:
    """Measure how long the SECOND page.get() takes — this is where the bug shows."""
    t0 = time.monotonic()
    out: dict[str, Any] = {}
    try:
        # Use bp._page.get directly so we measure ONLY the navigation, not the
        # post-load polling + CF wait + DOM scraping in NodriverBypass.get().
        await asyncio.wait_for(bp._page.get(url), timeout=90)
        elapsed = time.monotonic() - t0
        out["next_page_get_seconds"] = round(elapsed, 2)
        out["hang_detected"] = elapsed > HANG_THRESHOLD_S
        # Also try to read readyState as a quick sanity check.
        try:
            rs = await asyncio.wait_for(
                bp._page.evaluate("document.readyState", return_by_value=True),
                timeout=5,
            )
            out["readyState"] = getattr(rs, "value", rs)
        except Exception as e:  # noqa: BLE001
            out["readyState_error"] = f"{type(e).__name__}: {e}"
    except asyncio.TimeoutError:
        elapsed = time.monotonic() - t0
        out["next_page_get_seconds"] = round(elapsed, 2)
        out["hang_detected"] = True
        out["timeout"] = True
    except Exception as e:  # noqa: BLE001
        elapsed = time.monotonic() - t0
        out["next_page_get_seconds"] = round(elapsed, 2)
        out["hang_detected"] = elapsed > HANG_THRESHOLD_S
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ---------------------------------------------------------------------------
# Cookie extraction methods
# ---------------------------------------------------------------------------

async def extract_via_get_all(bp: NodriverBypass) -> dict[str, Any]:
    """Method (a): browser.cookies.get_all() — the path the comment blames."""
    res: dict[str, Any] = {"method": "browser.cookies.get_all"}
    t0 = time.monotonic()
    try:
        cookies = await asyncio.wait_for(bp._browser.cookies.get_all(), timeout=5.0)
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        names = []
        cf_names = []
        for c in cookies:
            n = c.name if hasattr(c, "name") else c.get("name", "")
            names.append(n)
            if n in CF_COOKIE_NAMES:
                cf_names.append(n)
        res["cookies_count"] = len(cookies)
        res["cf_cookies"] = sorted(set(cf_names))
        res["sample_names"] = names[:10]
    except asyncio.TimeoutError:
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = "TimeoutError after 5s"
    except Exception as e:  # noqa: BLE001
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = f"{type(e).__name__}: {e}"
    return res


async def extract_via_cdp_raw(bp: NodriverBypass) -> dict[str, Any]:
    """Method (b): raw CDP Network.getCookies via page.send()."""
    res: dict[str, Any] = {"method": "page.send(cdp_network.get_cookies)"}
    t0 = time.monotonic()
    try:
        cookies = await asyncio.wait_for(
            bp._page.send(cdp_network.get_cookies()), timeout=5.0
        )
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        # Response is a list of nodriver.cdp.network.Cookie objects.
        names = []
        cf_names = []
        for c in cookies or []:
            n = getattr(c, "name", None) or (c.get("name") if isinstance(c, dict) else "")
            names.append(n)
            if n in CF_COOKIE_NAMES:
                cf_names.append(n)
        res["cookies_count"] = len(cookies or [])
        res["cf_cookies"] = sorted(set(cf_names))
        res["sample_names"] = names[:10]
    except asyncio.TimeoutError:
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = "TimeoutError after 5s"
    except Exception as e:  # noqa: BLE001
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = f"{type(e).__name__}: {e}"
    return res


def _get_cookies_raw_generator(urls=None):
    """CDP generator returning RAW dict cookies, bypassing Cookie.from_json.

    Mirrors nodriver.cdp.network.get_cookies() but returns json['cookies']
    directly. nodriver 0.48.1 has a broken Cookie.from_json that raises
    TypeError on Chromium 120 responses, which kills the event loop via
    the unhandled exception in Connection._listener.
    """
    params: dict = {}
    if urls is not None:
        params['urls'] = [i for i in urls]
    cmd_dict = {'method': 'Network.getCookies', 'params': params}
    json = yield cmd_dict
    return json.get('cookies', []) if isinstance(json, dict) else []


async def extract_via_cdp_raw_safe(bp: NodriverBypass) -> dict[str, Any]:
    """Method (d): raw CDP, bypass Cookie.from_json parser entirely.

    This is the workaround for the nodriver 0.48.1 bug confirmed on
    2026-05-23: methods (a) browser.cookies.get_all and (b) page.send(
    cdp_network.get_cookies()) both invoke Cookie.from_json which raises
    TypeError on Chromium 120's response, corrupting the event loop.
    """
    res: dict[str, Any] = {"method": "page.send(custom raw get_cookies)"}
    t0 = time.monotonic()
    try:
        cookies = await asyncio.wait_for(
            bp._page.send(_get_cookies_raw_generator(urls=["https://fbref.com/"])),
            timeout=10.0,
        )
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        # cookies is a list of plain dicts from Chromium
        names = []
        cf_names = []
        cookies = cookies or []
        for c in cookies:
            n = c.get("name", "") if isinstance(c, dict) else ""
            names.append(n)
            if n in CF_COOKIE_NAMES:
                cf_names.append(n)
        res["cookies_count"] = len(cookies)
        res["cf_cookies"] = sorted(set(cf_names))
        res["sample_names"] = names[:10]
        # cf_clearance value length is a useful sanity check (~150-300 chars)
        for c in cookies:
            if isinstance(c, dict) and c.get("name") == "cf_clearance":
                v = c.get("value", "")
                res["cf_clearance_value_length"] = len(v) if isinstance(v, str) else 0
                break
    except asyncio.TimeoutError:
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = "TimeoutError after 10s"
    except Exception as e:  # noqa: BLE001
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = f"{type(e).__name__}: {e}"
    return res


async def extract_via_js_eval(bp: NodriverBypass) -> dict[str, Any]:
    """Method (c): document.cookie. HttpOnly cookies (incl. cf_clearance) are NOT returned."""
    res: dict[str, Any] = {"method": "page.evaluate(document.cookie)"}
    t0 = time.monotonic()
    try:
        raw = await asyncio.wait_for(
            bp._page.evaluate("document.cookie", return_by_value=True), timeout=5.0
        )
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        s = getattr(raw, "value", raw) if hasattr(raw, "value") else raw
        s = s if isinstance(s, str) else (str(s) if s else "")
        names = []
        cf_names = []
        for kv in s.split(";"):
            kv = kv.strip()
            if not kv or "=" not in kv:
                continue
            n = kv.split("=", 1)[0].strip()
            names.append(n)
            if n in CF_COOKIE_NAMES:
                cf_names.append(n)
        res["cookies_count"] = len(names)
        res["cf_cookies"] = sorted(set(cf_names))
        res["sample_names"] = names[:10]
        res["note"] = (
            "document.cookie does NOT expose HttpOnly cookies (cf_clearance is HttpOnly "
            "on FBref) — even an empty cf_cookies list here is expected."
        )
    except asyncio.TimeoutError:
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = "TimeoutError after 5s"
    except Exception as e:  # noqa: BLE001
        res["extract_seconds"] = round(time.monotonic() - t0, 2)
        res["error"] = f"{type(e).__name__}: {e}"
    return res


# ---------------------------------------------------------------------------
# Method (e): websocket trap + sequential extracts (issue #57)
# ---------------------------------------------------------------------------

def install_ws_trap(connection) -> tuple[list[dict], Any]:
    """Wrap connection.websocket recv/send to log every raw CDP message.

    `connection` is a nodriver Tab (which inherits Connection); its
    `_websocket` attribute holds the websockets.asyncio ClientConnection.

    Surfaces:
      - On which sessionId the response to Network.getCookies actually arrives
        (issue #57 hypothesis: it answers on a different session than _page.send
        is listening on).
      - Whether unhandled-id responses or broken events accumulate between
        the first and the second extract on the same browser (production
        does 10 matches in a row on one browser).

    Returns (captured, uninstall). `captured` is a list of dicts, mutated
    live as messages flow.
    """
    captured: list[dict] = []
    ws = connection._websocket  # actual websockets ClientConnection
    if ws is None:
        log.warning("install_ws_trap: connection._websocket is None — trap skipped")
        return captured, lambda: None
    orig_recv = ws.recv
    orig_send = ws.send

    def _summarize(msg) -> dict | None:
        try:
            data = json.loads(msg) if isinstance(msg, (str, bytes, bytearray)) else None
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        return {
            "method": data.get("method"),
            "session_id": data.get("sessionId"),
            "id": data.get("id"),
            "has_error": "error" in data,
            "is_response": "id" in data and "method" not in data,
            "size": len(msg) if hasattr(msg, "__len__") else 0,
        }

    async def trap_recv(*args, **kwargs):
        msg = await orig_recv(*args, **kwargs)
        info = _summarize(msg)
        if info is not None:
            info["direction"] = "recv"
            info["ts"] = round(time.monotonic(), 4)
            captured.append(info)
        return msg

    async def trap_send(msg, *args, **kwargs):
        info = _summarize(msg)
        if info is not None:
            info["direction"] = "send"
            info["ts"] = round(time.monotonic(), 4)
            captured.append(info)
        return await orig_send(msg, *args, **kwargs)

    # Instance-level attribute shadows the bound method on the class.
    ws.recv = trap_recv  # type: ignore[method-assign]
    ws.send = trap_send  # type: ignore[method-assign]

    def uninstall():
        # Delete instance attrs so the original bound methods on the class
        # are exposed again.
        for attr in ("recv", "send"):
            try:
                delattr(ws, attr)
            except Exception:  # noqa: BLE001
                pass

    return captured, uninstall


def _analyze_trap(captured: list[dict], window: tuple[float, float]) -> dict:
    """Return diagnostic summary for the window [start_ts, end_ts]."""
    lo, hi = window
    in_window = [m for m in captured if lo <= m.get("ts", 0) <= hi]

    # Outbound Network.getCookies (request) — pick the LAST one in window.
    sent_get_cookies = [
        m for m in in_window
        if m.get("direction") == "send" and m.get("method") == "Network.getCookies"
    ]
    request = sent_get_cookies[-1] if sent_get_cookies else None
    req_id = request.get("id") if request else None
    req_session = request.get("session_id") if request else None

    # Inbound response to that id.
    response = None
    if req_id is not None:
        for m in in_window:
            if m.get("direction") == "recv" and m.get("id") == req_id:
                response = m
                break

    # Other inbound noise during the window — useful for accumulation diag.
    inbound_methods: dict[str, int] = {}
    for m in in_window:
        if m.get("direction") == "recv" and m.get("method"):
            inbound_methods[m["method"]] = inbound_methods.get(m["method"], 0) + 1

    sessions_seen = sorted({
        str(m.get("session_id"))
        for m in in_window
        if m.get("session_id") is not None
    })

    return {
        "msgs_in_window": len(in_window),
        "request_id": req_id,
        "request_session_id": req_session,
        "response_present": response is not None,
        "response_session_id": response.get("session_id") if response else None,
        "session_mismatch": (
            response is not None
            and req_session != response.get("session_id")
        ),
        "inbound_event_counts": inbound_methods,
        "distinct_sessions_seen": sessions_seen,
    }


async def extract_via_cdp_raw_safe_trapped(bp: NodriverBypass) -> dict[str, Any]:
    """Method (e): method (d) extract wrapped in websocket trap.

    Runs TWO extracts in sequence on the SAME browser — between them we
    navigate to NEXT_URL — to surface the production accumulation effect
    (10 matches in a row on one scraper instance).
    """
    res: dict[str, Any] = {"method": "page.send(custom raw) + ws trap + 2× sequential"}
    # Trap on the tab's websocket — bp._page is a nodriver Tab (Connection).
    captured, uninstall = install_ws_trap(bp._page)
    try:
        # --- Extract #1 (immediately after landing) ---------------------------
        t0 = time.monotonic()
        attempt1: dict[str, Any] = {"label": "extract_1_after_landing"}
        try:
            cookies = await asyncio.wait_for(
                bp._page.send(_get_cookies_raw_generator(urls=["https://fbref.com/"])),
                timeout=8.0,
            )
            attempt1["extract_seconds"] = round(time.monotonic() - t0, 2)
            cookies = cookies or []
            cf_names = [
                c.get("name") for c in cookies
                if isinstance(c, dict) and c.get("name") in CF_COOKIE_NAMES
            ]
            attempt1["cookies_count"] = len(cookies)
            attempt1["cf_cookies"] = sorted(set(cf_names))
        except asyncio.TimeoutError:
            attempt1["extract_seconds"] = round(time.monotonic() - t0, 2)
            attempt1["error"] = "TimeoutError after 8s"
        except Exception as e:  # noqa: BLE001
            attempt1["extract_seconds"] = round(time.monotonic() - t0, 2)
            attempt1["error"] = f"{type(e).__name__}: {e}"
        attempt1["trap"] = _analyze_trap(captured, (t0, time.monotonic()))
        res["attempt_1"] = attempt1
        log.info(
            f"  extract#1: {attempt1.get('extract_seconds')}s, "
            f"cookies={attempt1.get('cookies_count')}, "
            f"req_session={attempt1['trap'].get('request_session_id')}, "
            f"resp_present={attempt1['trap'].get('response_present')}, "
            f"resp_session={attempt1['trap'].get('response_session_id')}, "
            f"mismatch={attempt1['trap'].get('session_mismatch')}, "
            f"error={attempt1.get('error')}"
        )

        # --- Navigation to NEXT_URL (mimics production multi-match loop) -----
        nav_t0 = time.monotonic()
        try:
            await asyncio.wait_for(bp._page.get(NEXT_URL), timeout=90)
            res["intermediate_nav_seconds"] = round(time.monotonic() - nav_t0, 2)
        except Exception as e:  # noqa: BLE001
            res["intermediate_nav_error"] = f"{type(e).__name__}: {e}"
            res["intermediate_nav_seconds"] = round(time.monotonic() - nav_t0, 2)
        # Small settle so Chromium issues Network.responseReceivedExtraInfo
        # events that historically broke Cookie.from_json.
        await asyncio.sleep(2.0)

        # --- Extract #2 (after second navigation, same browser) --------------
        t1 = time.monotonic()
        attempt2: dict[str, Any] = {"label": "extract_2_after_navigation"}
        try:
            cookies = await asyncio.wait_for(
                bp._page.send(_get_cookies_raw_generator(urls=["https://fbref.com/"])),
                timeout=8.0,
            )
            attempt2["extract_seconds"] = round(time.monotonic() - t1, 2)
            cookies = cookies or []
            cf_names = [
                c.get("name") for c in cookies
                if isinstance(c, dict) and c.get("name") in CF_COOKIE_NAMES
            ]
            attempt2["cookies_count"] = len(cookies)
            attempt2["cf_cookies"] = sorted(set(cf_names))
        except asyncio.TimeoutError:
            attempt2["extract_seconds"] = round(time.monotonic() - t1, 2)
            attempt2["error"] = "TimeoutError after 8s"
        except Exception as e:  # noqa: BLE001
            attempt2["extract_seconds"] = round(time.monotonic() - t1, 2)
            attempt2["error"] = f"{type(e).__name__}: {e}"
        attempt2["trap"] = _analyze_trap(captured, (t1, time.monotonic()))
        res["attempt_2"] = attempt2
        log.info(
            f"  extract#2: {attempt2.get('extract_seconds')}s, "
            f"cookies={attempt2.get('cookies_count')}, "
            f"req_session={attempt2['trap'].get('request_session_id')}, "
            f"resp_present={attempt2['trap'].get('response_present')}, "
            f"resp_session={attempt2['trap'].get('response_session_id')}, "
            f"mismatch={attempt2['trap'].get('session_mismatch')}, "
            f"error={attempt2.get('error')}"
        )

        # Surface the production-relevant signal as top-level keys so the
        # bench-like conclusion logic in main() can reason about hang.
        # We treat method (e) as "succeeded" iff BOTH extracts returned
        # cookies — that is what production needs.
        a1, a2 = res["attempt_1"], res["attempt_2"]
        ok_1 = bool(a1.get("cookies_count")) and not a1.get("error")
        ok_2 = bool(a2.get("cookies_count")) and not a2.get("error")
        res["extract_seconds"] = (a1.get("extract_seconds") or 0) + (a2.get("extract_seconds") or 0)
        res["cookies_count"] = (a2 if ok_2 else a1).get("cookies_count", 0)
        res["cf_cookies"] = (a2 if ok_2 else a1).get("cf_cookies", [])
        if not ok_1 or not ok_2:
            res["error"] = (
                f"extract_1={'OK' if ok_1 else a1.get('error') or 'no cookies'}, "
                f"extract_2={'OK' if ok_2 else a2.get('error') or 'no cookies'}"
            )

        # Bound the message log to avoid bloating the report — keep first
        # 50 and last 50 messages for inspection.
        msgs = captured
        if len(msgs) > 100:
            res["captured_first_50"] = msgs[:50]
            res["captured_last_50"] = msgs[-50:]
            res["captured_total"] = len(msgs)
        else:
            res["captured_all"] = msgs
            res["captured_total"] = len(msgs)
    finally:
        uninstall()
    return res


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------

async def run_phase(
    label: str,
    proxy: str,
    extract_fn,  # async (bp) -> dict | None for baseline
) -> dict[str, Any]:
    """Open browser → land on FBref → optionally extract cookies → time next page.get()."""
    log.info("=" * 70)
    log.info(f"PHASE: {label}")
    log.info("=" * 70)
    bp = None
    phase: dict[str, Any] = {"phase": label}
    try:
        bp = await fresh_bypass(proxy)
        landing = await land_and_pass_cf(bp, LANDING_URL)
        phase["landing"] = landing

        if extract_fn is not None:
            extract = await extract_fn(bp)
            phase.update(extract)
            log.info(
                f"  extract took {extract.get('extract_seconds')}s, "
                f"cookies_count={extract.get('cookies_count')}, "
                f"cf_cookies={extract.get('cf_cookies')}, "
                f"error={extract.get('error')}"
            )
        else:
            phase["method"] = "baseline_no_extraction"

        nxt = await time_next_page_get(bp, NEXT_URL)
        phase.update(nxt)
        log.info(
            f"  next page.get() took {nxt.get('next_page_get_seconds')}s "
            f"(hang_detected={nxt.get('hang_detected')})"
        )
    except Exception as e:  # noqa: BLE001
        phase["phase_error"] = f"{type(e).__name__}: {e}"
        phase["traceback"] = traceback.format_exc(limit=4)
        log.exception("phase failed")
    finally:
        if bp is not None:
            try:
                await bp.close()
            except Exception as e:  # noqa: BLE001
                log.warning(f"close error: {e}")
    return phase


async def main() -> None:
    proxy = load_proxy_url(PROXY_FILE)
    log.info(f"Using proxy {mask_proxy(proxy)}")
    log.info(f"nodriver version: 0.48.1 (pip)")

    report: dict[str, Any] = {
        "nodriver_version": "0.48.1",
        "proxy_masked": mask_proxy(proxy),
        "landing_url": LANDING_URL,
        "next_url": NEXT_URL,
        "hang_threshold_seconds": HANG_THRESHOLD_S,
    }

    # Minimal repro: only baseline + method (d). RAM budget is tight,
    # methods (a)/(b)/(c) are already documented as broken/useless in
    # docs/research/fbref-scraper-speedup.md from the first run.
    report["baseline"] = await run_phase("baseline", proxy, extract_fn=None)
    report["baseline_page_get_seconds"] = report["baseline"].get(
        "next_page_get_seconds"
    )

    # Method D: raw CDP, bypassing Cookie.from_json parser.
    report["method_d_cdp_raw_safe"] = await run_phase(
        "method_d_cdp_raw_safe", proxy, extract_via_cdp_raw_safe
    )

    # Method E (issue #57): method (d) wrapped in websocket trap +
    # TWO sequential extracts on the same browser. The first extract
    # mimics method (d). The second extract — after a navigation —
    # reproduces the production accumulation pattern (10-match loop).
    # Per-attempt trap analysis shows whether Network.getCookies
    # response arrives on the same sessionId we sent on.
    report["method_e_trap_2x"] = await run_phase(
        "method_e_trap_2x", proxy, extract_via_cdp_raw_safe_trapped
    )

    # Conclusion
    baseline = report["baseline"].get("next_page_get_seconds") or 0
    candidates = []
    for key in ("method_d_cdp_raw_safe", "method_e_trap_2x"):
        m = report[key]
        nxt = m.get("next_page_get_seconds")
        hang = m.get("hang_detected", True)
        if not hang and m.get("cookies_count", 0) > 0 and not m.get("error"):
            # we also penalise huge slowdown vs baseline (>3x)
            slowdown_ok = (nxt is None) or (baseline == 0) or (nxt <= max(20.0, baseline * 3))
            if slowdown_ok:
                candidates.append((key, nxt))

    if candidates:
        # Prefer the method that returned cf_clearance and has the lowest next-page latency.
        candidates_with_cf = [
            (k, n) for (k, n) in candidates
            if "cf_clearance" in report[k].get("cf_cookies", [])
        ]
        pool = candidates_with_cf or candidates
        best = min(pool, key=lambda kv: (kv[1] if kv[1] is not None else 1e9))
        report["conclusion"] = (
            f"{best[0]} works (next_page_get={best[1]}s, "
            f"baseline={baseline}s, hang_threshold={HANG_THRESHOLD_S}s)"
        )
    else:
        report["conclusion"] = "all methods hang or failed"

    Path(REPORT_PATH).write_text(json.dumps(report, indent=2, default=str))
    print("\n========= REPRO REPORT =========")
    print(json.dumps(report, indent=2, default=str))
    print(f"\nReport written to: {REPORT_PATH}")


if __name__ == "__main__":
    try:
        # nodriver expects to manage its own loop via nodriver.loop()
        # but in a fresh process asyncio.run() is fine.
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(1)
