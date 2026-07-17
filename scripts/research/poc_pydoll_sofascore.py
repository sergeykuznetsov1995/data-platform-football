"""PoC: can Pydoll (Chrome/CDP) replace camoufox+playwright for SofaScore?

Research probe for issue #969. NOT production code — lives outside the SofaScore
runtime-fingerprint allowlist so it cannot invalidate the verified proxy budget.

The one decisive question: SofaScore's edge serves /api/v1 JSON as a *document*
(200 on a Firefox navigation) but returns 403 to fetch/XHR on the same session,
which forces one page navigation per JSON (126 navigations for a 25-match batch).
Does a Chrome CDP session hit the same 403-on-fetch wall, or does Chrome's
in-page fetch return 200 (which would let Pydoll collapse navigations into
cheap fetches)?

Tier A (this file, --mode tier_a): free, datacenter egress, no residential proxy.
  Measures browser mechanics + the fetch-vs-navigation status crux. Host egress
  is 403-poisoned for SofaScore, so the transport verdict here is PROVISIONAL:
  a 403 cannot be attributed to "Chrome fails" vs "datacenter IP blocked".
Tier B (--mode tier_b --proxy ...): paid, residential lease. The authoritative test.

Run with the throwaway venv: /root/.venvs/dpf-pydoll/bin/python
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import time
from pathlib import Path

from pydoll.browser.chromium import Chrome
from pydoll.browser.options import ChromiumOptions

BASE = "https://www.sofascore.com"
# Endpoints mirror the verified match_batch_25 class (event has the shotmap sibling).
API_PATHS = ["/api/v1/event/{id}", "/api/v1/event/{id}/shotmap"]
# The full 5-endpoint set the production match_batch_25 class captures per match.
SWEEP_PATHS = [
    "/api/v1/event/{id}",
    "/api/v1/event/{id}/incidents",
    "/api/v1/event/{id}/lineups",
    "/api/v1/event/{id}/shotmap",
    "/api/v1/event/{id}/statistics",
]


def _chrome_rss_kib() -> int:
    """Sum RSS of all live chrome processes (rough footprint signal, no psutil)."""
    try:
        out = subprocess.run(
            ["ps", "-C", "chrome", "-o", "rss="],
            capture_output=True, text=True, timeout=10,
        ).stdout
        return sum(int(x) for x in out.split())
    except Exception:
        return -1


async def _probe_navigation(tab, url: str) -> dict:
    """Fetch the JSON path as a browser document (the camoufox strategy)."""
    result = {"method": "navigation", "url": url}
    try:
        await tab.go_to(url, timeout=60)
        info = await tab.execute_script(
            "JSON.stringify({"
            "  ct: document.contentType || '',"
            "  title: document.title || '',"
            "  len: (document.body ? document.body.innerText.length : 0),"
            "  head: (document.body ? document.body.innerText.slice(0, 180) : '')"
            "})",
            return_by_value=True,
        )
        payload = _unwrap(info)
        doc = json.loads(payload) if payload else {}
        head = doc.get("head", "")
        result.update(
            content_type=doc.get("ct"),
            body_len=doc.get("len"),
            looks_like_json=head.lstrip().startswith("{"),
            is_challenge=_is_challenge(doc.get("title", ""), head),
            head=head,
        )
    except Exception as exc:  # noqa: BLE001 - probe records failures as data
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


async def _fetch_sweep(tab, event_id: str) -> list[dict]:
    """Fetch all 5 production endpoints for one match via in-page fetch, timed.

    This is the core comparison: camoufox needs one navigation per JSON
    (5 navigations/match); if Chrome fetch works, this is 5 cheap fetches
    from a single already-loaded page.
    """
    out = []
    for tmpl in SWEEP_PATHS:
        path = tmpl.format(id=event_id)
        t0 = time.perf_counter()
        res = await _probe_fetch(tab, f"{BASE}{path}")
        res["endpoint"] = path.split("/")[-1] if path.count("/") > 4 else "event"
        res["seconds"] = round(time.perf_counter() - t0, 3)
        out.append(res)
    return out


async def _probe_fetch(tab, url: str) -> dict:
    """Fetch the JSON path via in-page fetch() from the www.sofascore.com origin."""
    result = {"method": "in_page_fetch", "url": url}
    script = (
        "(async () => {"
        "  try {"
        "    const r = await fetch(%s, {credentials: 'include',"
        "      headers: {'x-requested-with': 'XMLHttpRequest'}});"
        "    const t = await r.text();"
        "    return JSON.stringify({status: r.status, len: t.length,"
        "      head: t.slice(0, 180)});"
        "  } catch (e) { return JSON.stringify({error: String(e)}); }"
        "})()" % json.dumps(url)
    )
    try:
        raw = await tab.execute_script(
            script, return_by_value=True, await_promise=True
        )
        payload = _unwrap(raw)
        doc = json.loads(payload) if payload else {}
        result.update(
            status=doc.get("status"),
            body_len=doc.get("len"),
            looks_like_json=str(doc.get("head", "")).lstrip().startswith("{"),
            head=doc.get("head"),
            fetch_error=doc.get("error"),
        )
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _unwrap(evaluate_response) -> str:
    """Pull the string value out of a CDP Runtime.evaluate response."""
    if isinstance(evaluate_response, str):
        return evaluate_response
    if isinstance(evaluate_response, dict):
        r = evaluate_response.get("result", evaluate_response)
        if isinstance(r, dict):
            return r.get("result", {}).get("value", r.get("value", ""))
    return getattr(getattr(evaluate_response, "result", None), "value", "") or ""


def _is_challenge(title: str, head: str) -> bool:
    blob = f"{title} {head}".lower()
    return any(s in blob for s in ("just a moment", "challenge", "cf-", "checking your browser"))


def _curl_cffi_crosscheck(event_id: str) -> dict:
    """Cross-check with curl_cffi chrome_133 (separate dpf-test venv).

    Separates a TLS/JA3 rejection from a session/Turnstile rejection: if the
    perfect-TLS client also gets 403, the block is IP/edge-level, not browser fp.
    """
    url = f"{BASE}/api/v1/event/{event_id}"
    code = """
import json
try:
    from curl_cffi import requests as cr
    r = cr.get(URL, impersonate='chrome131',
        headers={'x-requested-with': 'XMLHttpRequest', 'referer': REFERER},
        timeout=30)
    print(json.dumps({'status': r.status_code, 'len': len(r.content)}))
except Exception as e:
    print(json.dumps({'error': '%s: %s' % (type(e).__name__, e)}))
""".replace("URL", repr(url)).replace("REFERER", repr(BASE))
    try:
        out = subprocess.run(
            ["/root/.venvs/dpf-test/bin/python", "-c", code],
            capture_output=True, text=True, timeout=60,
        )
        line = (out.stdout or out.stderr).strip().splitlines()[-1]
        return json.loads(line)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


async def run_probe(
    event_id: str, headless: bool, proxy: str | None, proxy_label: str,
    sweep: bool = False, block_images: bool = False,
) -> dict:
    report: dict = {
        "mode": "tier_b" if proxy else "tier_a",
        "engine": "pydoll (Chrome/CDP)",
        "event_id": event_id,
        "proxy_used": bool(proxy),
        "proxy_label": proxy_label,
        "note": (
            "datacenter egress, no residential proxy — transport verdict is "
            "PROVISIONAL (403 cannot be split from IP block)"
            if not proxy else "residential proxy — authoritative transport test"
        ),
    }

    opts = ChromiumOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    if headless:
        opts.add_argument("--headless=new")
    if block_images:
        # Cheap analog of the production resource block-list (#842): drop the
        # heaviest landing assets (images) so landing bytes are comparable.
        opts.add_argument("--blink-settings=imagesEnabled=false")
    if proxy:
        # pydoll strips creds from the arg and wires up auto proxy-auth.
        opts.add_argument(f"--proxy-server={proxy}")

    # CDP wire-byte meter (approximates provider down-bytes).
    down_bytes = {"total": 0, "requests": 0}

    def _on_finished(event: dict) -> None:
        try:
            down_bytes["total"] += int(event["params"]["encodedDataLength"])
            down_bytes["requests"] += 1
        except Exception:  # noqa: BLE001
            pass

    t0 = time.perf_counter()
    async with Chrome(options=opts) as browser:
        tab = await browser.start()
        startup_s = time.perf_counter() - t0
        report["startup_seconds"] = round(startup_s, 3)
        await tab.enable_network_events()
        await tab.on("Network.loadingFinished", _on_finished)
        try:
            report["browser_version"] = str(await browser.get_version())
        except Exception as exc:  # noqa: BLE001
            report["browser_version"] = f"err: {exc}"

        # 1) Land on the SPA origin, let pydoll auto-solve Turnstile if present.
        landing = {"step": "landing", "url": BASE}
        try:
            await tab.enable_auto_solve_cloudflare_captcha()
            tn0 = time.perf_counter()
            await tab.go_to(BASE, timeout=90)
            landing["nav_seconds"] = round(time.perf_counter() - tn0, 3)
            info = await tab.execute_script(
                "JSON.stringify({title: document.title||'',"
                " head:(document.body?document.body.innerText.slice(0,180):'')})",
                return_by_value=True,
            )
            doc = json.loads(_unwrap(info) or "{}")
            landing.update(
                title=doc.get("title"),
                is_challenge=_is_challenge(doc.get("title", ""), doc.get("head", "")),
                reached_spa="sofascore" in (doc.get("title", "").lower()),
            )
        except Exception as exc:  # noqa: BLE001
            landing["error"] = f"{type(exc).__name__}: {exc}"
        report["landing"] = landing
        report["rss_kib_after_landing"] = _chrome_rss_kib()
        report["landing_wire_down_bytes"] = down_bytes["total"]
        landing_bytes = down_bytes["total"]

        # 2) The crux: navigation vs in-page fetch, per endpoint.
        probes = []
        for tmpl in API_PATHS:
            path = tmpl.format(id=event_id)
            url = f"{BASE}{path}"
            probes.append(await _probe_navigation(tab, url))
            probes.append(await _probe_fetch(tab, url))
        report["probes"] = probes

        # 2b) Optional: full 5-endpoint fetch sweep (the production match shape).
        if sweep:
            report["fetch_sweep"] = await _fetch_sweep(tab, event_id)

        report["wire_down_bytes_total"] = down_bytes["total"]
        report["data_wire_down_bytes"] = down_bytes["total"] - landing_bytes
        report["wire_response_count"] = down_bytes["requests"]

    report["total_seconds"] = round(time.perf_counter() - t0, 3)
    # 3) TLS cross-check (own egress, no browser) — only meaningful on Tier A.
    if not proxy:
        report["curl_cffi_chrome131"] = _curl_cffi_crosscheck(event_id)
    return report


def _proxy_from_pool(index: int) -> tuple[str, str]:
    """Read one host:port:user:pass line and return (proxy_url, safe_label)."""
    line = Path("/root/proxys.txt").read_text().splitlines()[index].strip()
    host, port, user, pwd = line.split(":", 3)
    return f"http://{user}:{pwd}@{host}:{port}", f"pool#{index} {host}:{port}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Pydoll SofaScore PoC (#969)")
    ap.add_argument("--mode", choices=["tier_a", "tier_b"], default="tier_a")
    ap.add_argument("--event-id", default="14023964", help="SofaScore event id (EPL cohort)")
    ap.add_argument("--proxy", default=None, help="proxy-server URL for tier_b")
    ap.add_argument("--proxy-from-pool", type=int, default=None,
                    help="index into /root/proxys.txt (keeps creds out of argv)")
    ap.add_argument("--no-headless", action="store_true")
    ap.add_argument("--sweep", action="store_true",
                    help="fetch all 5 production endpoints (times each)")
    ap.add_argument("--block-images", action="store_true",
                    help="disable images (cheap analog of #842 resource block)")
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    proxy, label = args.proxy, (args.proxy or "none")
    if args.proxy_from_pool is not None:
        proxy, label = _proxy_from_pool(args.proxy_from_pool)
    if args.mode == "tier_b" and not proxy:
        ap.error("tier_b requires --proxy or --proxy-from-pool")

    report = asyncio.run(
        run_probe(args.event_id, headless=not args.no_headless,
                  proxy=proxy, proxy_label=label,
                  sweep=args.sweep, block_images=args.block_images)
    )
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
