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


def _first_proxy(proxy_file: str, index: int = 0) -> Optional[dict]:
    """The ``index``-th valid ``host:port:user:pass`` line → Camoufox/Playwright
    proxy dict. ``index`` lets us skip a dead sticky-session exit."""
    valid = []
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
                valid.append({"server": f"http://{host}:{port}", "username": user, "password": password})
    except FileNotFoundError:
        log.warning("proxy file %s not found", proxy_file)
        return None
    if not valid:
        return None
    return valid[index % len(valid)]


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


def _collect_event_dicts(obj, out=None) -> list:
    """Recursively collect SofaScore event-like dicts (have homeTeam+awayTeam or a
    startTimestamp) embedded anywhere in a parsed __NEXT_DATA__ blob — tells us
    whether (and which) schedule is SSR'd rather than fetched via XHR."""
    if out is None:
        out = []
    if isinstance(obj, dict):
        if ("homeTeam" in obj and "awayTeam" in obj) or "startTimestamp" in obj:
            out.append(obj)
        for v in obj.values():
            _collect_event_dicts(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _collect_event_dicts(v, out)
    return out


def _event_ut_id(ev) -> Optional[int]:
    """unique-tournament id embedded on a SofaScore event dict, if present."""
    if not isinstance(ev, dict):
        return None
    return ((ev.get("tournament") or {}).get("uniqueTournament") or {}).get("id")


def _sample_events(events, src) -> None:
    for ev in events[:4]:
        if isinstance(ev, dict):
            log.info("   [%s] id=%s status=%s ut=%s %s vs %s", src,
                     ev.get("id"), (ev.get("status") or {}).get("type"),
                     _event_ut_id(ev),
                     (ev.get("homeTeam") or {}).get("name"),
                     (ev.get("awayTeam") or {}).get("name"))


def _nudge_tournament(page, wait_ms: int = 2500) -> None:
    """Click likely controls so the SPA fetches FINISHED matches (results / past
    rounds) instead of only the default upcoming round (#757 B0 interaction)."""
    # First: hunt the REAL controls (SofaScore hangs data-testid on everything).
    try:
        info = page.evaluate(
            """() => {
                const out = {round: [], tabs: [], links: []};
                const seen = new Set();
                for (const e of document.querySelectorAll('button,[role=button],[role=tab],a,[data-testid]')) {
                    const tx = (e.innerText||'').trim().slice(0,24);
                    const al = (e.getAttribute('aria-label')||'').slice(0,32);
                    const ti = (e.getAttribute('data-testid')||'');
                    const blob = (tx+' '+al+' '+ti);
                    if (/round|prev|next|result|fixture|arrow|chevron/i.test(blob)) {
                        const k = 'r:'+tx+al+ti;
                        if (!seen.has(k)) { seen.add(k); out.round.push({tx, al, ti}); }
                    }
                    if (['Matches','Results','Fixtures','Standings','Overview'].includes(tx)) {
                        const k = 't:'+tx; if (!seen.has(k)) { seen.add(k); out.tabs.push({tx, ti}); }
                    }
                }
                for (const a of document.querySelectorAll('a[href]')) {
                    const h = a.getAttribute('href')||'';
                    if (/\\/(matches|results|fixtures)/i.test(h) && !seen.has('h:'+h)) {
                        seen.add('h:'+h); out.links.push(h.slice(0,80));
                    }
                }
                const tids = [];
                const ts = new Set();
                for (const e of document.querySelectorAll('[data-testid]')) {
                    const t = e.getAttribute('data-testid');
                    if (t && !ts.has(t)) { ts.add(t); tids.push(t); }
                }
                return {round: out.round.slice(0,25), tabs: out.tabs,
                        links: out.links.slice(0,15), testids: tids.slice(0,120)};
            }"""
        )
        log.info("DOM round/results controls: %s", info.get("round"))
        log.info("DOM tab elements: %s", info.get("tabs"))
        log.info("DOM matches/results links: %s", info.get("links"))
        log.info("DOM all data-testids: %s", info.get("testids"))
    except Exception as e:
        log.info("control dump failed: %s", e)
    # The Matches section is collapsed by default (Standings shown) so its view
    # toggles (tab-date/tab-round) render display:none → Playwright .click()
    # times out. JS .click() bypasses actionability and follows the SPA's own
    # handlers. Open the 'Matches' top-nav, then switch to the BY DATE view
    # (surfaces recent/finished matches for an in-progress season). (#757 B1)
    # Click tab-date/tab-round DIRECTLY by data-testid (clicking the 'Matches'
    # top-nav re-mounts the section and the toggles vanish before the next
    # click). Wait for each to mount — render timing varies by proxy exit.
    for tid in ("tab-date", "tab-round"):
        sel = '[data-testid="' + tid + '"]'
        try:
            page.wait_for_function(
                "() => !!document.querySelector('" + sel + "')", timeout=8000)
            hit = page.evaluate(
                "() => { const e=document.querySelector('" + sel + "');"
                " if(e){e.click();return true;} return false; }")
            log.info("nudge %s -> %s", tid, hit)
            page.wait_for_timeout(wait_ms)
        except Exception as e:
            log.info("nudge %s: not mounted (%s)", tid, e)


def _probe_tournament(page, ut_id: int, captures: Dict[str, dict], settle_ms: int,
                      nav_url: str = "") -> int:
    """Navigate a league page and report HOW the match list (schedule) loads —
    XHR ``/api/v1/.../events/last|next`` vs SSR ``__NEXT_DATA__`` (#757 B0 gate)."""
    # /unique-tournament/{id} 404s; the canonical URL needs the country/slug.
    url = nav_url or f"https://www.sofascore.com/unique-tournament/{ut_id}"
    log.info(">>> goto tournament page: %s", url)
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    _dismiss_consent(page)
    page.wait_for_timeout(settle_ms)
    _diag(page, "tournament-post-load")
    # Scroll to nudge any lazy events XHR the SPA fires on viewport.
    try:
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(3000)
    except Exception:
        pass
    # Interact: surface FINISHED matches (Matches nav -> by-date/by-round view).
    _nudge_tournament(page)

    log.info("-" * 70)
    # XHR events scoped to the TARGET tournament (round/last/next under its ut_id).
    tgt_re = re.compile(rf"/unique-tournament/{ut_id}/season/\d+/events/")
    tgt_events, tgt_paths = {}, []
    for k, v in captures.items():
        if not tgt_re.search(k) or v.get("status") != 200:
            continue
        obj = v.get("json")
        evs = obj.get("events") if isinstance(obj, dict) else None
        if not isinstance(evs, list):
            continue
        tgt_paths.append(k)
        for ev in evs:
            if isinstance(ev, dict) and ev.get("id") is not None:
                tgt_events[ev["id"]] = ev
    log.info("XHR events for TARGET ut=%s: %d events across %d paths",
             ut_id, len(tgt_events), len(tgt_paths))
    for k in sorted(tgt_paths):
        log.info("   XHR-path %s", k)
    _sample_events(list(tgt_events.values()), "XHR")
    finished_xhr = [e for e in tgt_events.values()
                    if (e.get("status") or {}).get("type") == "finished"]
    log.info("   -> finished via XHR: %d", len(finished_xhr))

    # Also note the generic last|next (may be a DIFFERENT featured tournament).
    other = {k for k, v in captures.items()
             if re.search(r"/events/(last|next)/\d+$", k) and not tgt_re.search(k)}
    if other:
        log.info("NOTE: %d generic events/last|next paths are NOT the target ut "
                 "(featured block) — extract_events must filter by ut_id: %s",
                 len(other), sorted(other))

    # __NEXT_DATA__ SSR check (scoped to target ut where derivable).
    ssr_tgt = []
    try:
        nd = page.evaluate(
            "() => { const e = document.getElementById('__NEXT_DATA__'); "
            "return e ? e.textContent : null; }"
        )
        if nd:
            ssr_all = _collect_event_dicts(json.loads(nd))
            ssr_tgt = [e for e in ssr_all
                       if _event_ut_id(e) in (ut_id, None)]
            log.info("__NEXT_DATA__ present (len=%d); event-like dicts: %d total, "
                     "%d target-ut", len(nd), len(ssr_all), len(ssr_tgt))
            _sample_events(ssr_tgt, "SSR")
        else:
            log.info("__NEXT_DATA__ NOT present on tournament page")
    except Exception as e:
        log.info("__NEXT_DATA__ parse failed: %s", e)

    log.info("=" * 70)
    if tgt_events:
        log.info("RESULT: XHR — target ut=%s schedule via /events/{round,last,next} "
                 "(%d events, %d finished). B1: capture buffer, filter by ut_id.",
                 ut_id, len(tgt_events), len(finished_xhr))
        return 0
    if ssr_tgt:
        log.info("RESULT: SSR — no target-ut XHR, but %d target-ut event dicts in "
                 "__NEXT_DATA__. B1: parse __NEXT_DATA__ (B4).", len(ssr_tgt))
        return 0
    log.info("RESULT: FAIL — no events via XHR or __NEXT_DATA__ (Turnstile not "
             "solved / wrong URL / events behind interaction).")
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture SofaScore /api/v1 via Camoufox (#757 P2)")
    ap.add_argument("--event-id", type=int, default=DEFAULT_EVENT)
    ap.add_argument("--mode", choices=("event", "tournament"), default="event",
                    help="event: probe a match page (default). tournament: probe a "
                         "league page to learn how the match list (schedule) loads "
                         "— XHR /events/last|next vs SSR __NEXT_DATA__ (#757 B0).")
    ap.add_argument("--tournament-id", type=int, default=17,
                    help="SofaScore unique-tournament id (17=EPL) for --mode tournament.")
    ap.add_argument("--tournament-url", default="",
                    help="Override the league page URL for --mode tournament "
                         "(e.g. canonical slug URL); blank → /unique-tournament/{id}.")
    ap.add_argument("--headless", default="virtual", help="virtual|true|false")
    ap.add_argument("--settle", type=int, default=10000)
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--proxy-index", type=int, default=0,
                    help="Which valid proxy line to use (skip a dead exit).")
    ap.add_argument("--use-proxy", action="store_true")
    args = ap.parse_args()

    headless = {"true": True, "false": False}.get(str(args.headless).lower(), "virtual")
    eid = args.event_id
    proxy = _first_proxy(args.proxy_file, args.proxy_index) if args.use_proxy else None

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

        if args.mode == "tournament":
            return _probe_tournament(page, args.tournament_id, captures,
                                     args.settle, args.tournament_url)

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
