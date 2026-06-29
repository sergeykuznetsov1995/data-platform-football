"""
Probe: SofaScore per-player capture (issue #751 PR3).

Pins the three facts the PR3 spike left open (see
memory/feedback_sofascore_player_page_capture.md):

  1. The exact dig path to the player bio object inside ``__NEXT_DATA__``
     (profile is SSR'd, NOT fetched as ``/api/v1/player/{id}`` XHR).
  2. The DEFAULT ``(unique_tournament, season)`` on the Season tab — does it
     equal our target (EPL=17, 2025/26)? Matters for season-stats targeting.
  3. That clicking ``data-testid=tab-season`` actually fires
     ``/player/{id}/unique-tournament/{ut}/season/{sid}/statistics/overall``
     (+ ``/statistics/seasons`` for season→id mapping).

Read-only: navigates + inspects, writes nothing to Iceberg.

Needs: camoufox + `python -m camoufox fetch`, playwright < 1.60, a residential
proxy (Turnstile 403s on the datacenter VM IP). Run inside airflow-scheduler:

  docker compose exec airflow-scheduler \\
    python -u /opt/airflow/scripts/research/probe_sofascore_player.py \\
      --use-proxy --player-id 1416535

Self-contained: only camoufox + stdlib. Never imports scrapers/__init__.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("probe_player")

API_PATH = "/api/v1/"
DEFAULT_PLAYER = 1416535  # an APL player (spike reference)
_CF_MARKERS = ("just a moment", "verify you are human", "/cdn-cgi/challenge")


def _first_proxy(proxy_file: str, index: int = 0) -> Optional[dict]:
    """The ``index``-th valid ``host:port:user:pass`` line → proxy dict."""
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
                valid.append({"server": f"http://{host}:{port}",
                              "username": user, "password": password})
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


def _click_tab(page, testid: str, label: str, wait_ms: int = 3000) -> bool:
    """Click a tab by its stable data-testid (JS click bypasses actionability and
    follows the SPA's own handler — same trick used in production capture). Falls
    back to get_by_text. Returns whether a click landed."""
    sel = '[data-testid="' + testid + '"]'
    try:
        page.wait_for_function(
            "() => !!document.querySelector('" + sel + "')", timeout=8000)
        hit = page.evaluate(
            "() => { const e=document.querySelector('" + sel + "');"
            " if(e){e.scrollIntoView();e.click();return true;} return false; }")
        log.info("click tab testid=%s -> %s", testid, hit)
        page.wait_for_timeout(wait_ms)
        if hit:
            return True
    except Exception as e:
        log.info("tab testid=%s not mounted (%s)", testid, e)
    try:
        page.get_by_text(label, exact=False).first.click(timeout=3000)
        log.info("click tab text=%s -> ok (fallback)", label)
        page.wait_for_timeout(wait_ms)
        return True
    except Exception as e:
        log.info("tab text=%s fallback failed (%s)", label, e)
    return False


def _dump_testids(page) -> None:
    """List the tab data-testids on the player page (confirm tab-season exists)."""
    try:
        tids = page.evaluate(
            """() => {
                const out = []; const seen = new Set();
                for (const e of document.querySelectorAll('[data-testid]')) {
                    const t = e.getAttribute('data-testid') || '';
                    if (t && !seen.has(t)) { seen.add(t); out.push(t); }
                }
                return out.slice(0, 150);
            }"""
        )
        tab_tids = [t for t in tids if "tab" in t.lower() or "season" in t.lower()]
        log.info("DOM tab/season data-testids: %s", tab_tids)
        log.info("DOM all data-testids (capped): %s", tids)
    except Exception as e:
        log.info("testid dump failed: %s", e)


def _find_player_objects(obj, pid: int, path: str = "", out=None) -> List[Tuple[str, dict]]:
    """Recursively collect dicts that look like THE player bio (id == pid AND a
    'name'), recording the JSON path to each. Tells us exactly where the SSR'd
    bio lives in __NEXT_DATA__ so production can dig it deterministically."""
    if out is None:
        out = []
    if isinstance(obj, dict):
        oid = obj.get("id")
        same_id = (oid == pid) or (str(oid) == str(pid))
        if same_id and "name" in obj and ("slug" in obj or "dateOfBirthTimestamp" in obj):
            out.append((path or "<root>", obj))
        for k, v in obj.items():
            _find_player_objects(v, pid, f"{path}.{k}" if path else k, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _find_player_objects(v, pid, f"{path}[{i}]", out)
    return out


_SEASON_STATS_RE = re.compile(
    r"/api/v1/player/(\d+)/unique-tournament/(\d+)/season/(\d+)/statistics/overall$")
_SEASONS_LIST_RE = re.compile(r"/api/v1/player/(\d+)/statistics/seasons$")


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe SofaScore per-player capture (#751 PR3)")
    ap.add_argument("--player-id", type=int, default=DEFAULT_PLAYER)
    ap.add_argument("--headless", default="virtual", help="virtual|true|false")
    ap.add_argument("--settle", type=int, default=10000)
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--proxy-index", type=int, default=0)
    ap.add_argument("--use-proxy", action="store_true")
    args = ap.parse_args()

    headless = {"true": True, "false": False}.get(str(args.headless).lower(), "virtual")
    pid = args.player_id
    proxy = _first_proxy(args.proxy_file, args.proxy_index) if args.use_proxy else None

    from camoufox.sync_api import Camoufox

    captures: Dict[str, dict] = {}
    host_hits: Counter = Counter()

    log.info("=" * 70)
    log.info("SofaScore PR3 per-player capture probe (#751) — player %s", pid)
    log.info("proxy: %s", urlsplit(proxy["server"]).netloc if proxy else "direct (VM IP)")
    log.info("=" * 70)

    cam_kwargs = {"headless": headless}
    if proxy:
        cam_kwargs["proxy"] = proxy
        cam_kwargs["geoip"] = True

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
                return
            tag = "PLR " if "/player/" in key else "    "
            log.info("[%s] %s -> %s challenge=%s size=%s",
                     tag, key, rec["status"], rec["challenge"], rec["size"])

        page.on("request", on_request)
        page.on("response", on_response)

        # Dummy slug — the SPA resolves to the real slug by trailing id (spike).
        purl = f"https://www.sofascore.com/player/x/{pid}"
        log.info(">>> goto player page (dummy slug): %s", purl)
        page.goto(purl, wait_until="domcontentloaded", timeout=60000)
        _diag(page, "post-load")
        _dismiss_consent(page)
        page.wait_for_timeout(args.settle)
        _diag(page, "post-consent (expect redirect to real slug)")

        _dump_testids(page)

        # --- 1. profile from __NEXT_DATA__ (NOT an XHR) -------------------- #
        log.info("-" * 70)
        log.info("FACT 1 — player bio location in __NEXT_DATA__:")
        try:
            nd_text = page.evaluate(
                "() => { const e=document.getElementById('__NEXT_DATA__');"
                " return e ? e.textContent : null; }")
            if nd_text:
                nd = json.loads(nd_text)
                log.info("__NEXT_DATA__ present (len=%d)", len(nd_text))
                pp = (nd.get("props") or {}).get("pageProps")
                if isinstance(pp, dict):
                    log.info("props.pageProps keys: %s", sorted(pp.keys()))
                found = _find_player_objects(nd, pid)
                log.info("found %d candidate bio object(s) with id==%s", len(found), pid)
                for path, pobj in found[:4]:
                    log.info("   PATH __NEXT_DATA__.%s -> name=%r slug=%r pos=%r "
                             "height=%r foot=%r dob=%s team=%r",
                             path, pobj.get("name"), pobj.get("slug"),
                             pobj.get("position"), pobj.get("height"),
                             pobj.get("preferredFoot"),
                             pobj.get("dateOfBirthTimestamp"),
                             (pobj.get("team") or {}).get("name"))
                if not found:
                    log.info("   NO bio object with id==%s found — dump pageProps "
                             "shape for manual inspection:", pid)
                    log.info("   %s", json.dumps(pp, default=str)[:1500] if pp else None)
            else:
                log.info("__NEXT_DATA__ NOT present on player page")
        except Exception as e:
            log.info("__NEXT_DATA__ parse failed: %s", e)

        # --- 2+3. Season tab → season-statistics XHR ---------------------- #
        log.info("-" * 70)
        log.info("FACT 3 — clicking tab-season to fire season-statistics XHR:")
        _click_tab(page, "tab-season", "Season")
        page.wait_for_timeout(args.settle)

        # --- 4. season/tournament SELECTOR DOM (for non-EPL-default players) #
        # When the default season tab is a non-target competition, we must drive
        # the tournament/season picker to select EPL. Dump candidate controls.
        log.info("-" * 70)
        log.info("FACT 4 — broad clickable inventory on the Season tab "
                 "(find the tournament/season picker):")
        try:
            inv = page.evaluate(
                """() => {
                    const out = []; const seen = new Set();
                    for (const e of document.querySelectorAll(
                            'button,[role=button],[role=combobox],[role=listbox],'
                            + '[role=option],a,svg,[tabindex]')) {
                        const tx = (e.innerText||'').trim().slice(0,36);
                        if (!tx) continue;
                        const ti = e.getAttribute('data-testid')||'';
                        const role = e.getAttribute('role')||'';
                        const k = tx+'|'+ti+'|'+e.tagName;
                        if (seen.has(k)) continue; seen.add(k);
                        out.push({tx, ti, role, tag: e.tagName});
                    }
                    return out.slice(0, 70);
                }"""
            )
            for el in inv:
                log.info("   click? %s", el)
        except Exception as e:
            log.info("   inventory dump failed: %s", e)

        # Try to OPEN a picker: click the element whose text is the current
        # tournament/season label, then re-check whether EPL options appear.
        log.info("FACT 4b — attempt to open a season picker + look for EPL option:")
        try:
            opened = page.evaluate(
                """() => {
                    // Heuristic: the season switcher shows the current season label
                    // (e.g. 'World Cup 2026' or just '2026'/'25/26'). Click the
                    // smallest clickable ancestor bearing such a label.
                    const cand = [...document.querySelectorAll('button,[role=button],[tabindex]')]
                        .find(e => /\\b(20\\d\\d|\\d\\d\\/\\d\\d)\\b/.test((e.innerText||'').trim())
                                   && (e.innerText||'').trim().length < 30);
                    if (cand) { cand.click(); return (cand.innerText||'').trim().slice(0,30); }
                    return null;
                }"""
            )
            log.info("   opened picker via label=%r", opened)
            page.wait_for_timeout(2500)
            epl = page.evaluate(
                """() => [...document.querySelectorAll('*')]
                    .filter(e => /premier league/i.test((e.innerText||''))
                                 && (e.innerText||'').trim().length < 40
                                 && e.children.length <= 1)
                    .map(e => ({tx:(e.innerText||'').trim().slice(0,30),
                                ti:e.getAttribute('data-testid')||'', tag:e.tagName}))
                    .slice(0, 12)"""
            )
            log.info("   EPL-bearing options after opening picker: %s", epl)
        except Exception as e:
            log.info("   picker-open attempt failed: %s", e)

    # ---- report ----
    log.info("=" * 70)
    log.info("hosts contacted (top 12): %s", host_hits.most_common(12))
    player_paths = sorted(k for k in captures if f"/player/{pid}/" in k or k == f"/api/v1/player/{pid}")
    log.info("captured %d player XHR paths:", len(player_paths))
    for k in player_paths:
        v = captures[k]
        log.info("   %s -> %s challenge=%s size=%s", k, v["status"], v["challenge"], v["size"])

    # bare /api/v1/player/{id} should NOT appear (SSR'd) — confirm the spike claim.
    bare = f"/api/v1/player/{pid}"
    log.info("FACT (bio is SSR, not XHR): bare %s captured? %s",
             bare, bare in captures)

    # FACT 2 — default (ut, season) on the season tab.
    log.info("-" * 70)
    season_hits = []
    for k, v in captures.items():
        m = _SEASON_STATS_RE.search(k)
        if m and v.get("status") == 200 and v.get("challenge") is False:
            season_hits.append((int(m.group(2)), int(m.group(3)), k))
    log.info("FACT 2 — season-statistics/overall XHRs captured: %d", len(season_hits))
    for ut, sid, k in season_hits:
        log.info("   ut=%s season_id=%s (EPL target ut=17) -> %s", ut, sid, k)
    seasons_list = [k for k in captures if _SEASONS_LIST_RE.search(k)]
    log.info("FACT 2b — /statistics/seasons captured (season→id map)? %s", seasons_list)
    if seasons_list:
        sl = captures[seasons_list[0]].get("json")
        # surface the unique-tournament/season ids so we can map slug→id w/o tls.
        log.info("   seasons payload (capped): %s", json.dumps(sl, default=str)[:1200])

    ok = bool(season_hits) and any(ut == 17 for ut, _, _ in season_hits)
    if ok:
        log.info("RESULT: PASS — bio in __NEXT_DATA__ + EPL season-stats XHR captured.")
        return 0
    log.info("RESULT: PARTIAL/FAIL — see facts above (no EPL season-stats XHR, "
             "or Turnstile not solved).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
