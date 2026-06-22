"""
Probe: capture SofaScore /api/v1 JSON via a Camoufox/Firefox SPA session
(issue #757 — chosen path P2 "Firefox-capture").

Findings so far
---------------
- The SPA calls **same-origin** ``www.sofascore.com/api/v1/*`` (not
  api.sofascore.com). Images/flags under /api/v1 return 200, but every **data**
  endpoint (lineups, statistics, shotmap, incidents, event, seasons, events,
  standings) returns ``403 {"reason":"challenge"}`` — even for the real SPA's
  own XHRs, and for an in-page same-origin fetch. The page still renders because
  event+incidents are SSR'd into __NEXT_DATA__.
- So the data API is gated by **Cloudflare Turnstile**: a valid token must ride
  on the XHR. Our automated Camoufox on the **datacenter VM IP** did not get a
  valid token → 403. Turnstile frequently passes *invisibly* on a residential
  IP, which would let the SPA's data XHRs (and in-page fetch) succeed.

This probe therefore supports ``--use-proxy`` to route Camoufox through the first
residential proxy in proxys.txt and re-test whether the data endpoints come back
as real JSON. That is the make-or-break test for P2.

Needs: camoufox + `python -m camoufox fetch`, and **playwright < 1.60**
(1.60 crashes Camoufox on page errors — camoufox#617).

Run (inside airflow-scheduler — 4G):
  docker compose exec airflow-scheduler \\
    python -u /opt/airflow/scripts/research/probe_sofascore_capture.py --use-proxy --event-id 12345678

Self-contained: only camoufox + stdlib. Never imports scrapers/__init__.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter
from typing import Dict, Optional
from urllib.parse import urlsplit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("probe_capture")

API_PATH = "/api/v1/"
DOD_PATH = "/api/v1/unique-tournament/17/seasons"
DEFAULT_EVENT = 15186878
_DEEP_HINTS = ("/lineups", "/statistics", "/shotmap", "/incidents", "/player/")
_CF_MARKERS = ("just a moment", "verify you are human", "/cdn-cgi/challenge")


def _first_proxy(proxy_file: str) -> Optional[dict]:
    """First ``host:port:user:pass`` line → Camoufox/Playwright proxy dict."""
    try:
        with open(proxy_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) < 4:
                    continue
                host, port, user = parts[0], parts[1], parts[2]
                password = ":".join(parts[3:])
                return {"server": f"http://{host}:{port}", "username": user, "password": password}
    except FileNotFoundError:
        log.warning("proxy file %s not found", proxy_file)
    return None


def _is_challenge(obj) -> Optional[bool]:
    if not isinstance(obj, dict):
        return None
    err = obj.get("error")
    if isinstance(err, dict) and err.get("reason") == "challenge":
        return True
    return False


def _diag(page, label: str) -> None:
    try:
        html = page.content()
    except Exception as e:
        log.info("[diag %s] content() failed: %s", label, e)
        return
    low = html.lower()
    cf = [m for m in _CF_MARKERS if m in low]
    log.info("[diag %s] url=%s title=%r len=%d cf=%s",
             label, page.url, (page.title() or "")[:60], len(html), cf)


def _dismiss_consent(page) -> None:
    for name in ("Consent", "AGREE", "Agree", "Accept all", "I Accept", "Got it"):
        try:
            page.get_by_role("button", name=re.compile(name, re.I)).first.click(timeout=2500)
            log.info("clicked consent button: %s", name)
            page.wait_for_timeout(1500)
            return
        except Exception:
            pass


def _click_tabs(page) -> None:
    """Trigger the deep tabs so the SPA fires their XHRs (lineups/statistics/
    shotmap) WITH its own token header — passive capture then sees real JSON."""
    for label in ("Lineups", "Statistics", "Player statistics", "Shotmap", "Player ratings"):
        try:
            page.get_by_text(label, exact=False).first.click(timeout=3000)
            page.wait_for_timeout(2500)
            log.info("clicked tab: %s", label)
        except Exception:
            pass


def _inpage_fetch(page, path: str) -> dict:
    return page.evaluate(
        """async (p) => {
            try {
                const r = await fetch(p, {headers: {'accept': 'application/json'}});
                const t = await r.text();
                let challenge = null;
                try { challenge = (JSON.parse(t).error || {}).reason === 'challenge'; } catch(e) {}
                return {status: r.status, len: t.length, challenge, head: t.slice(0, 140)};
            } catch (e) { return {error: String(e)}; }
        }""",
        path,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture SofaScore /api/v1 via Camoufox (#757 P2)")
    ap.add_argument("--event-id", type=int, default=DEFAULT_EVENT)
    ap.add_argument("--headless", default="virtual", help="virtual|true|false")
    ap.add_argument("--settle", type=int, default=10000)
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--use-proxy", action="store_true")
    args = ap.parse_args()

    headless = {"true": True, "false": False}.get(str(args.headless).lower(), "virtual")
    eid = args.event_id
    proxy = _first_proxy(args.proxy_file) if args.use_proxy else None

    from camoufox.sync_api import Camoufox

    captures: Dict[str, dict] = {}
    host_hits: Counter = Counter()

    log.info("=" * 70)
    log.info("SofaScore P2 Firefox-capture spike (#757) — event %s", eid)
    log.info("proxy: %s", urlsplit(proxy["server"]).netloc if proxy else "direct (VM IP)")
    log.info("=" * 70)

    cam_kwargs = {"headless": headless}
    if proxy:
        cam_kwargs["proxy"] = proxy
        cam_kwargs["geoip"] = True  # match locale/timezone to the proxy exit (Turnstile signal)

    with Camoufox(**cam_kwargs) as browser:
        page = browser.new_page()

        def on_request(req):
            host_hits[urlsplit(req.url).netloc] += 1

        def on_response(resp):
            url = resp.url
            if API_PATH not in url or "sofascore.com" not in url:
                return
            key = re.sub(r"^https?://[^/]+", "", url.split("?")[0])
            rec = {"status": resp.status, "json": None, "challenge": None, "size": None}
            try:
                obj = resp.json()
                rec["json"] = obj
                rec["challenge"] = _is_challenge(obj)
                rec["size"] = len(json.dumps(obj))
            except Exception:
                pass
            captures[key] = rec
            if "/image" in key or "/flag" in key:
                return  # don't spam the log with crest/flag CDN hits
            tag = "DoD" if DOD_PATH in url else ("deep" if any(h in url for h in _DEEP_HINTS) else "    ")
            log.info("[%s] %s -> %s challenge=%s size=%s", tag, key, rec["status"], rec["challenge"], rec["size"])

        page.on("request", on_request)
        page.on("response", on_response)

        murl = f"https://www.sofascore.com/event/{eid}"
        log.info(">>> goto match page: %s", murl)
        page.goto(murl, wait_until="domcontentloaded", timeout=60000)
        _diag(page, "post-load")
        _dismiss_consent(page)
        page.wait_for_timeout(args.settle)
        _diag(page, "post-consent")

        # Trigger deep tabs so the SPA fetches lineups/statistics/shotmap itself.
        _click_tabs(page)
        page.wait_for_timeout(3000)

        log.info("-" * 70)
        log.info("IN-PAGE FETCH control (naive, no SPA token — expected 403):")
        tests = {
            "seasons(DoD)": DOD_PATH,
            "lineups(deep)": f"/api/v1/event/{eid}/lineups",
            "statistics(deep)": f"/api/v1/event/{eid}/statistics",
            "shotmap(deep)": f"/api/v1/event/{eid}/shotmap",
        }
        inpage = {}
        for name, path in tests.items():
            res = _inpage_fetch(page, path)
            inpage[name] = res
            log.info("   %-16s %s -> %s", name, path, res)

    # ---- verdict ----
    log.info("=" * 70)
    log.info("hosts contacted (top 12): %s", host_hits.most_common(12))
    real = {k: v for k, v in captures.items()
            if v["status"] == 200 and v["challenge"] is False and "/image" not in k and "/flag" not in k}
    deep = {k: v for k, v in real.items() if any(h in k for h in _DEEP_HINTS)}
    log.info("passively captured %d /api/v1 responses; %d real DATA; %d real DEEP",
             len(captures), len(real), len(deep))
    for k in sorted(real):
        log.info("   REAL %s (size=%s)", k, real[k]["size"])

    inpage_ok = [n for n, r in inpage.items() if r.get("status") == 200 and not r.get("challenge")]
    log.info("in-page fetch OK (200, non-challenge): %s", inpage_ok)

    if inpage_ok or deep:
        log.info("RESULT: PASS — P2 works%s. in-page-fetch OK=%d, passive-deep=%d.",
                 " (with proxy)" if proxy else "", len(inpage_ok), len(deep))
        return 0
    log.info("RESULT: FAIL — data endpoints still 403 challenge%s "
             "(Turnstile token not obtained).", " even via residential proxy" if proxy else "")
    return 1


if __name__ == "__main__":
    sys.exit(main())
