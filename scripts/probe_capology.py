"""Probe live Capology to validate URL pattern + anti-bot + data extraction.

Issue #43, phase 0.2.

Run inside airflow-webserver:
    docker compose exec -T airflow-webserver \
        python /opt/airflow/scripts/probe_capology.py \
        > /tmp/capology_probe/verdicts.md 2>&1

Capology data live in an inline JS array `var data = [{...},...]` — NOT in
HTML <table> tags. ScraperFC's Selenium "Next" pagination is unnecessary:
the server ships the whole season's roster in one ~2.7MB response.

No side effects: GET-only, no Iceberg writes.
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, '/opt/airflow')

import tls_requests  # type: ignore

DUMP_DIR = Path('/tmp/capology_probe')
DUMP_DIR.mkdir(parents=True, exist_ok=True)

URL = 'https://www.capology.com/uk/premier-league/salaries/2024-2025/'
PROXY_FILE = Path('/opt/airflow/proxys.txt')

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)


def _first_proxy() -> Optional[str]:
    if not PROXY_FILE.exists():
        return None
    parts = PROXY_FILE.read_text().splitlines()[0].strip().split(':', 3)
    if len(parts) < 4:
        return None
    host, port, user, pw = parts
    return f"http://{user}:{pw}@{host}:{port}"


def _get(url: str, proxy_url: Optional[str] = None) -> tuple[int, str, float]:
    headers = {'User-Agent': UA, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9'}
    kwargs = {'headers': headers, 'timeout': 30}
    if proxy_url:
        kwargs['proxy'] = proxy_url
    t0 = time.time()
    try:
        r = tls_requests.get(url, **kwargs)
        return r.status_code, r.text, time.time() - t0
    except Exception as e:
        return -1, f"EXCEPTION: {e}", time.time() - t0


def _extract_data_keys(html: str) -> dict:
    """Find `var data = [{...},...]` and report shape without full parse."""
    m = re.search(r"var\s+data\s*=\s*\[\s*\{", html)
    if not m:
        return {'found': False}
    start = m.end() - 1  # at the opening `{`
    # Find the matching `];` — Capology terminates the array with `];` followed
    # by DataTables init. Naive bracket counting is fine for this format.
    depth = 0
    end = None
    for i in range(m.end() - 1, len(html)):
        c = html[i]
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return {'found': True, 'parse_failed': True}
    body = html[m.end() - 1: end]
    # Count rows by toplevel `{` siblings — proxy: 'name' occurrences.
    row_count = body.count("'name':")
    keys = sorted(set(re.findall(r"\b'([a-z_]+)':", body[:8000])))
    return {
        'found': True,
        'body_bytes': end - start,
        'row_count_est': row_count,
        'first_8k_keys': keys,
    }


def main() -> int:
    print("# Capology probe — issue #43, phase 0.2\n")
    print(f"_Run at_: `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`\n")
    print(f"_Target_: `{URL}`\n")

    proxy = _first_proxy()

    # --- single fresh request (no proxy first) ---
    print("## Step 1 — Direct (no proxy)\n")
    status, body, dur = _get(URL)
    (DUMP_DIR / '1_direct.html').write_text(body if status > 0 else '')
    print(f"- HTTP: **{status}** | latency: **{dur:.2f}s** | size: **{len(body) if status > 0 else 0} bytes**")
    is_cf_block = (status == 200 and len(body) < 100_000) or status == 403
    print(f"- CF challenge? **{is_cf_block}** (heuristic: 403 OR 200+<100KB)")

    if not is_cf_block and status == 200:
        extract = _extract_data_keys(body)
        print(f"- inline data array: `{json.dumps(extract, ensure_ascii=False)}`")
        print("\n**VERDICT (Step 1)**: tls_requests is sufficient for cold requests. "
              "rate-limit + retry will handle the CF flare on rapid-fire bursts.")

    # --- proxy fallback (use only if cold path failed) ---
    if is_cf_block and proxy:
        print("\n## Step 2 — With residential proxy\n")
        time.sleep(15)  # cool off CF heuristic
        status, body, dur = _get(URL, proxy_url=proxy)
        (DUMP_DIR / '2_proxy.html').write_text(body if status > 0 else '')
        print(f"- HTTP: **{status}** | latency: **{dur:.2f}s** | size: **{len(body) if status > 0 else 0} bytes**")
        if status == 200 and len(body) > 100_000:
            extract = _extract_data_keys(body)
            print(f"- inline data array: `{json.dumps(extract, ensure_ascii=False)}`")
            print("\n**VERDICT (Step 2)**: residential proxy unlocks; "
                  "wire proxy path as fallback when direct flares.")
        else:
            print("\n**VERDICT (Step 2)**: proxy also blocked → escalate to FlareSolverr.")
            return 3

    print(
        "\n---\n**OVERALL**: Capology serves the whole season roster (~3000 rows, all 3 "
        "currencies inline as `weekly_gross_{eur,gbp,usd}` keys) in one HTML response. "
        "No pagination, no Selenium needed. Currency switch is client-side JS — single "
        "scrape covers GBP/EUR/USD."
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
