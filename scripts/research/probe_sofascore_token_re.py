"""
Probe: how does the SofaScore SPA authenticate its api.sofascore.com XHRs?
(issue #757 — token reverse-engineering)

Background
----------
Every *cold* GET to ``api.sofascore.com/api/v1/*`` returns
``{"error":{"code":403,"reason":"challenge"}}`` — proven for ``tls_requests``
(#751), Chromium-FlareSolverr (#751) and byparr/Firefox (#755). The #755 spike
also showed that loading ``www.sofascore.com`` through byparr returns a real
200 SPA but sets **no** ``cf_clearance`` / ``__cf_bm`` cookie — so the gate is
**not** a standard Cloudflare clearance cookie, it is application-layer on the
API itself. Hypothesis (Kirill52300 public RE, 2026-04): the SPA adds a signed
header (``x-requested-with`` dynamic token and/or ``x-captcha`` JWT) to every
API XHR; a naive cold request omits it → 403.

The only way to confirm is to *watch the SPA make its own API calls*. This
probe drives a real browser (DrissionPage / Chromium with the platform's proven
anti-fingerprint config), navigates the APL tournament page (which fires the
DoD endpoint ``unique-tournament/17/seasons``), and uses DrissionPage's network
listener to capture every ``api.sofascore.com`` XHR: its **request headers**,
**status**, and a **response preview**. From that we learn:

  1. Does Chromium even pass SofaScore's edge here (or do we need Firefox)?
  2. Do the SPA's own API XHRs succeed in-browser (200 + JSON)?
  3. WHICH header(s)/cookie(s) the SPA attaches that a cold request lacks
     → the exact thing to replicate (replay) or capture (SPA-scrape).

This is observation only — it makes no claim to *solve* the gate, it reveals
the mechanism so we can choose the integration path (replay vs SPA-capture).

Run (inside airflow-scheduler — 4G; webserver 1G → Chrome OOM):
  docker compose exec airflow-scheduler \\
    python /opt/airflow/scripts/research/probe_sofascore_token_re.py

  # through a residential proxy (first proxys.txt line); default: direct
  docker compose exec airflow-scheduler \\
    python /opt/airflow/scripts/research/probe_sofascore_token_re.py --use-proxy

Self-contained: imports DrissionPage via the platform's browser helper, never
pulls scrapers/__init__ at module import (heavy nodriver/selenium stack).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger("probe_token_re")

# DoD endpoint (#757) and a page that triggers it on load (APL = u-tournament 17).
DOD_PATH = "/api/v1/unique-tournament/17/seasons"
TOURNAMENT_URL = "https://www.sofascore.com/tournament/football/england/premier-league/17"
API_HOST_FILTER = "api.sofascore.com/api/v1"

# Header names worth singling out in the capture (lower-cased compare).
_INTEREST_HEADERS = (
    "x-requested-with", "x-captcha", "authorization", "cookie",
    "referer", "origin", "user-agent", "baggage", "if-none-match",
)


def _first_proxy(proxy_file: str) -> Optional[str]:
    """First ``host:port:user:pass`` line from proxys.txt (or None)."""
    try:
        with open(proxy_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and len(line.split(":")) >= 4:
                    return line
    except FileNotFoundError:
        log.warning("proxy file %s not found", proxy_file)
    return None


def _build_page(proxy: Optional[str]):
    """Construct a live ChromiumPage reusing DrissionPageBypass's proven
    anti-fingerprint options + Xvfb, but WITHOUT its get_page (which quits the
    page) — we need the page alive to attach the network listener."""
    from scrapers.base.browser.drissionpage_bypass import (
        DrissionPageBypass,
        _import_drissionpage,
    )

    ChromiumPage, _ = _import_drissionpage()
    bypass = DrissionPageBypass(proxy=proxy, headless=False, use_xvfb=True)
    bypass._start_xvfb()
    options = bypass._create_options()
    page = ChromiumPage(options)
    page.set.timeouts(base=60.0, page_load=60.0, script=30)
    return page, bypass


def _summarise_headers(headers: dict) -> dict:
    """Keep only the interesting request headers; mask cookie/JWT values but
    keep their *names* (that's the signal — which token is present)."""
    out = {}
    for k, v in (headers or {}).items():
        kl = k.lower()
        if kl not in _INTEREST_HEADERS:
            continue
        if kl == "cookie":
            names = [c.split("=", 1)[0].strip() for c in str(v).split(";") if c.strip()]
            out["cookie(names)"] = names
        elif kl in ("x-captcha", "authorization"):
            out[kl] = f"<present, len={len(str(v))}>"
        else:
            out[kl] = v
    return out


def _preview_body(body) -> str:
    """First 200 chars of a captured response body, JSON-aware."""
    if body is None:
        return "<no body>"
    if isinstance(body, (dict, list)):
        return json.dumps(body)[:200]
    return str(body)[:200]


def _is_challenge(body) -> Optional[bool]:
    """True if the body is SofaScore's challenge error, False if real data,
    None if undecidable."""
    try:
        obj = body if isinstance(body, dict) else json.loads(body)
    except (TypeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    err = obj.get("error")
    if isinstance(err, dict) and err.get("reason") == "challenge":
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Observe SofaScore SPA API auth (#757)")
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument("--use-proxy", action="store_true")
    ap.add_argument("--listen-seconds", type=float, default=25.0)
    args = ap.parse_args()

    proxy = _first_proxy(args.proxy_file) if args.use_proxy else None

    log.info("=" * 70)
    log.info("SofaScore token-RE observation (#757)")
    log.info("page: %s", TOURNAMENT_URL)
    log.info("DoD endpoint to catch: %s", DOD_PATH)
    log.info("proxy: %s", "<residential, first line>" if proxy else "direct (container IP)")
    log.info("=" * 70)

    page, bypass = _build_page(proxy)
    captured: List[dict] = []
    try:
        # Listen BEFORE navigation so the SPA's first burst of API XHRs is caught.
        page.listen.start(API_HOST_FILTER)
        log.info("navigating (listener armed on %s) ...", API_HOST_FILTER)
        page.get(TOURNAMENT_URL)

        # Pass CF if an interstitial is shown (reuses the platform's wait+click).
        html = page.html or ""
        if bypass._is_cloudflare_blocked(html):
            log.info("CF interstitial detected — waiting for challenge ...")
            if not bypass._wait_for_cloudflare(page):
                log.warning("CF challenge NOT passed by Chromium (may need Firefox/byparr)")
        else:
            log.info("no CF interstitial on the SPA page (Chromium passed edge)")

        # Drain the listener for a fixed window — collect every api/v1 XHR.
        deadline = time.time() + args.listen_seconds
        for packet in page.listen.steps(timeout=args.listen_seconds):
            try:
                req = getattr(packet, "request", None)
                resp = getattr(packet, "response", None)
                body = getattr(resp, "body", None) if resp else None
                rec = {
                    "url": packet.url,
                    "method": getattr(req, "method", None) or packet.method,
                    "status": getattr(resp, "status", None) if resp else None,
                    "req_headers": _summarise_headers(getattr(req, "headers", {}) or {}),
                    "challenge": _is_challenge(body),
                    "body_preview": _preview_body(body),
                }
                captured.append(rec)
                tag = "DoD" if DOD_PATH in packet.url else "   "
                log.info("[%s] %s %s -> %s challenge=%s",
                         tag, rec["method"], packet.url, rec["status"], rec["challenge"])
            except Exception as e:  # noqa: BLE001 — keep draining on a bad packet
                log.warning("packet parse error: %s", e)
            if time.time() > deadline:
                break

        page.listen.stop()
    finally:
        try:
            page.quit()
        except Exception:
            pass
        bypass._stop_xvfb()

    # ---- verdict ----
    log.info("=" * 70)
    log.info("captured %d api/v1 XHR(s)", len(captured))
    if not captured:
        log.info("RESULT: NO api XHRs captured — Chromium likely blocked at the edge, "
                 "or the SPA fired none in the window. Next: try byparr/Firefox engine.")
        return 1

    dod = [r for r in captured if DOD_PATH in r["url"]]
    real = [r for r in captured if r["challenge"] is False and r["status"] == 200]
    log.info("api XHRs that returned real data (200, non-challenge): %d/%d",
             len(real), len(captured))

    if real:
        sample = real[0]
        log.info("--- AUTH SIGNAL: request headers on a SUCCESSFUL api XHR ---")
        log.info(json.dumps(sample["req_headers"], indent=2, ensure_ascii=False))
        log.info("These headers (minus the cold ones) are what a cold request lacks.")

    if dod:
        d = dod[0]
        ok = d["challenge"] is False and d["status"] == 200
        log.info("DoD endpoint captured: status=%s challenge=%s -> %s",
                 d["status"], d["challenge"], "PASS" if ok else "FAIL")
        log.info("DoD body preview: %s", d["body_preview"])
        return 0 if ok else 1

    log.info("DoD endpoint not fired by this page; but %d other api XHRs seen "
             "(headers above show the auth scheme). Treat as partial.", len(captured))
    return 0 if real else 1


if __name__ == "__main__":
    sys.exit(main())
