"""Probe FotMob path candidates to confirm working endpoint for /api/data/leagues.

Run inside airflow-webserver (or any container with `requests` + network):
    docker compose exec -T airflow-webserver \
        python /opt/airflow/scripts/probe_fotmob_paths.py

No side effects: only GET requests. Reports status, content length, and
top-level JSON keys for each variant. Success = any variant returns 200 with
{fixtures, table, stats} present.
"""
from __future__ import annotations

import re
import sys
from urllib.parse import urlencode

import requests

LEAGUE_ID = 47  # English Premier League
SEASON_SLASH = '2024/2025'
HEADERS = {'User-Agent': 'curl/8.0'}

REQUIRED_KEYS = {'fixtures', 'table', 'stats'}

VARIANTS: list[tuple[str, str]] = [
    ('A', f'https://www.fotmob.com/api/data/leagues?{urlencode({"id": LEAGUE_ID, "season": SEASON_SLASH})}'),
    ('B', f'https://www.fotmob.com/api/leagues?{urlencode({"id": LEAGUE_ID, "season": SEASON_SLASH})}'),
    ('C', f'https://www.fotmob.com/api/data/leagues?{urlencode({"id": LEAGUE_ID})}'),
    ('D', f'https://www.fotmob.com/api/data/leagues?{urlencode({"id": LEAGUE_ID, "season": "2024-2025"})}'),
]


def probe(label: str, url: str) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except requests.RequestException as e:
        return {'label': label, 'url': url, 'error': str(e)}

    info = {
        'label': label,
        'url': url,
        'status': r.status_code,
        'content_length': len(r.content),
    }
    if r.status_code == 200:
        try:
            payload = r.json()
        except ValueError:
            info['error'] = 'non-JSON 200'
            info['preview'] = r.text[:200]
            return info
        keys = set(payload.keys()) if isinstance(payload, dict) else set()
        info['top_keys'] = sorted(keys)
        info['has_required'] = REQUIRED_KEYS.issubset(keys)
    else:
        info['preview'] = r.text[:200]
    return info


def next_data_fallback() -> dict:
    """Variant E: scrape buildId from /leagues/47/overview page, hit _next/data."""
    overview_url = 'https://www.fotmob.com/leagues/47/overview/premier-league'
    try:
        r = requests.get(overview_url, headers=HEADERS, timeout=20)
    except requests.RequestException as e:
        return {'label': 'E', 'error': f'overview fetch: {e}'}

    m = re.search(r'/_next/data/([^/"]+)/', r.text)
    if not m:
        return {'label': 'E', 'overview_status': r.status_code, 'error': 'buildId not found in HTML'}

    build_id = m.group(1)
    json_url = f'https://www.fotmob.com/_next/data/{build_id}/en/leagues/47/overview/premier-league.json'
    return probe('E', json_url) | {'build_id': build_id}


def main() -> int:
    print('=== FotMob path probe ===')
    print(f'league_id={LEAGUE_ID} season={SEASON_SLASH} UA={HEADERS["User-Agent"]}')
    print()

    results = [probe(label, url) for label, url in VARIANTS]
    results.append(next_data_fallback())

    winners = []
    for r in results:
        label = r.get('label')
        if 'error' in r and 'status' not in r:
            print(f'[{label}] ERR  {r.get("url", "")}  → {r["error"]}')
            continue
        status = r.get('status')
        clen = r.get('content_length', '-')
        top = r.get('top_keys')
        ok = r.get('has_required', False)
        mark = '✓' if ok else ('200' if status == 200 else 'x')
        print(f'[{label}] {mark} status={status} bytes={clen} url={r["url"]}')
        if top is not None:
            print(f'       top_keys={top[:12]}{"..." if len(top) > 12 else ""}')
            if ok:
                winners.append(r)
        elif 'preview' in r:
            print(f'       preview={r["preview"]!r}')

    print()
    if winners:
        print(f'WINNER: variant {winners[0]["label"]} → {winners[0]["url"]}')
        return 0
    print('NO WINNER: no variant returned 200 with {fixtures, table, stats}.')
    return 1


if __name__ == '__main__':
    sys.exit(main())
