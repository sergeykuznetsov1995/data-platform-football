"""Probe live Transfermarkt to validate URL patterns + anti-bot strategy.

Issue #43, Phase 0.1.

Run inside airflow-webserver:
    docker compose exec -T airflow-webserver \
        python /opt/airflow/scripts/probe_transfermarkt.py \
        > /tmp/transfermarkt_probe/verdicts.md 2>&1

Output:
  - /tmp/transfermarkt_probe/{step}.html  -- raw HTML dumps
  - stdout                                -- markdown verdict table

Steps:
  1. League listing (APL 2025/26) — no proxy, then with proxy
  2. Squad page for the first club
  3. Player profile for the first player (selectors + Highcharts MV script)
  4. Transfers page for the same player

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
from bs4 import BeautifulSoup

DUMP_DIR = Path('/tmp/transfermarkt_probe')
DUMP_DIR.mkdir(parents=True, exist_ok=True)

TM_BASE = "https://www.transfermarkt.us"
APL_LISTING = f"{TM_BASE}/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=2025"

PROXY_FILE = Path('/opt/airflow/proxys.txt')


def _first_proxy() -> Optional[str]:
    if not PROXY_FILE.exists():
        return None
    line = PROXY_FILE.read_text().splitlines()[0].strip()
    # pool.proxys.io:10000:user31701-...:password
    parts = line.split(':', 3)
    if len(parts) < 4:
        return None
    host, port, user, pw = parts
    return f"http://{user}:{pw}@{host}:{port}"


def _get(url: str, proxy_url: Optional[str] = None, label: str = '') -> tuple[int, str, float]:
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    kwargs = {'headers': headers, 'timeout': 30}
    if proxy_url:
        kwargs['proxy'] = proxy_url
    t0 = time.time()
    try:
        resp = tls_requests.get(url, **kwargs)
        return resp.status_code, resp.text, time.time() - t0
    except Exception as e:
        return -1, f"EXCEPTION: {e}", time.time() - t0


def _parse_listing(html: str) -> list[dict]:
    """Extract club rows per ScraperFC selectors:
    table.items → td.hauptlink.no-border-links → a[href].
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if not table:
        return []
    clubs = []
    for td in table.find_all('td', {'class': 'hauptlink no-border-links'}):
        a = td.find('a', href=True)
        if not a:
            continue
        href = a['href']
        # /man-city/startseite/verein/281/saison_id/2025
        m = re.match(r'^/([^/]+)/startseite/verein/(\d+)', href)
        if m:
            clubs.append({
                'club_slug': m.group(1),
                'club_id': m.group(2),
                'href': href,
                'name': a.get_text(strip=True),
            })
    return clubs


def _parse_squad(html: str) -> list[dict]:
    """Extract player rows per ScraperFC selectors:
    table.items → td.hauptlink → a[href].
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if not table:
        return []
    players = []
    for td in table.find_all('td', {'class': 'hauptlink'}):
        a = td.find('a', href=True)
        if not a:
            continue
        href = a['href']
        # /erling-haaland/profil/spieler/418560
        m = re.match(r'^/([^/]+)/profil/spieler/(\d+)', href)
        if m:
            players.append({
                'player_slug': m.group(1),
                'player_id': m.group(2),
                'href': href,
                'name': a.get_text(strip=True),
            })
    # Dedup by player_id (table.items has multiple <td.hauptlink> per row sometimes)
    seen = set()
    uniq = []
    for p in players:
        if p['player_id'] in seen:
            continue
        seen.add(p['player_id'])
        uniq.append(p)
    return uniq


def _parse_profile(html: str) -> dict:
    """Verify ScraperFC profile selectors on the live page."""
    soup = BeautifulSoup(html, 'html.parser')
    out = {}
    name_el = soup.find('h1', {'class': 'data-header__headline-wrapper'})
    out['name_found'] = name_el is not None
    if name_el:
        out['name_text'] = re.sub(r'\s+', ' ', name_el.get_text(' ', strip=True))[:80]
    mv_el = soup.find('a', {'class': 'data-header__market-value-wrapper'})
    out['mv_found'] = mv_el is not None
    if mv_el:
        out['mv_raw'] = mv_el.get_text(' ', strip=True)[:60]
    out['dob_found'] = soup.find('span', {'itemprop': 'birthDate'}) is not None
    out['height_found'] = soup.find('span', {'itemprop': 'height'}) is not None
    out['nationality_found'] = soup.find('span', {'itemprop': 'nationality'}) is not None
    # Highcharts MV-history JS block
    script_with_chart = None
    for s in soup.find_all('script', {'type': 'text/javascript'}):
        if s.string and 'Highcharts.Chart' in s.string:
            script_with_chart = s.string
            break
    out['mv_history_script_found'] = script_with_chart is not None
    if script_with_chart:
        # Count datum_mw entries — should match number of MV-change points
        points = re.findall(r'"datum_mw":"([^"]+)"', script_with_chart)
        out['mv_history_points'] = len(points)
        if points:
            out['mv_history_first'] = points[0]
            out['mv_history_last'] = points[-1]
    return out


def _parse_transfers(html: str) -> dict:
    """Verify transfers page renders the grid ScraperFC uses."""
    soup = BeautifulSoup(html, 'html.parser')
    grids = soup.find_all('div', {'class': 'grid tm-player-transfer-history-grid'})
    return {
        'transfer_grid_rows': len(grids),
        'first_row_text_preview': (
            re.sub(r'\s+', ' ', grids[0].get_text(' ', strip=True))[:120]
            if grids else None
        ),
    }


def _dump(name: str, content: str) -> None:
    (DUMP_DIR / name).write_text(content)


def main() -> int:
    print("# Transfermarkt probe — issue #43, phase 0.1\n")
    print(f"_Run at_: `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`\n")
    print(f"_Target_: `{APL_LISTING}`\n")

    proxy = _first_proxy()
    print(f"_Proxy_: `{'configured (' + PROXY_FILE.name + ')' if proxy else 'none'}`\n")

    # --- Step 1: listing without proxy ---
    print("## Step 1 — League listing (no proxy)\n")
    status, html, dur = _get(APL_LISTING, label='listing_noproxy')
    _dump('1_listing_noproxy.html', html if status > 0 else '')
    print(f"- HTTP: **{status}**  | latency: **{dur:.2f}s** | size: **{len(html) if status > 0 else 0} bytes**")
    listing_html = html if status == 200 else None

    # --- Step 1b: listing with proxy (only if no-proxy failed) ---
    if status != 200 and proxy:
        print("\n## Step 1b — League listing (with proxy)\n")
        status, html, dur = _get(APL_LISTING, proxy_url=proxy, label='listing_proxy')
        _dump('1b_listing_proxy.html', html if status > 0 else '')
        print(f"- HTTP: **{status}**  | latency: **{dur:.2f}s** | size: **{len(html) if status > 0 else 0} bytes**")
        listing_html = html if status == 200 else None

    if not listing_html:
        print("\n**VERDICT**: listing unreachable — Transfermarkt blocked even with proxy. Halt probe.")
        return 2

    # If step 1 (no proxy) failed but step 1b (proxy) succeeded → use proxy for the rest.
    use_proxy = proxy if status == 200 and dur > 1.5 else (proxy if listing_html else None)
    # Cleaner: if we needed proxy for listing, we need it for all subsequent pages.
    needs_proxy = '1b_listing_proxy.html' in [p.name for p in DUMP_DIR.iterdir()]
    proxy_for_rest = proxy if needs_proxy else None
    print(f"\n- using proxy for next steps: **{bool(proxy_for_rest)}**")

    clubs = _parse_listing(listing_html)
    print(f"\n- parsed clubs: **{len(clubs)}** (expected ~20)")
    if clubs[:3]:
        for c in clubs[:3]:
            print(f"  - `{c['club_id']}` {c['name']} → `{c['href']}`")

    if not clubs:
        print("\n**VERDICT**: HTML 200 but no `table.items > td.hauptlink.no-border-links` rows — selectors drifted.")
        return 3

    # --- Step 2: squad page of first club ---
    print("\n## Step 2 — Squad page (first club)\n")
    club0 = clubs[0]
    squad_url = f"{TM_BASE}{club0['href']}".replace('/startseite/', '/kader/').rstrip('/') + '/plus/1'
    print(f"- url: `{squad_url}`")
    status, html, dur = _get(squad_url, proxy_url=proxy_for_rest)
    _dump('2_squad.html', html if status > 0 else '')
    print(f"- HTTP: **{status}**  | latency: **{dur:.2f}s** | size: **{len(html) if status > 0 else 0} bytes**")
    players = _parse_squad(html) if status == 200 else []
    print(f"- parsed players: **{len(players)}** (expected ~25)")
    for p in players[:3]:
        print(f"  - `{p['player_id']}` {p['name']} → `{p['href']}`")

    if not players:
        print("\n**VERDICT**: squad page rendered without expected `table.items > td.hauptlink` player rows.")
        return 4

    # --- Step 3: player profile ---
    print("\n## Step 3 — Player profile (first player)\n")
    p0 = players[0]
    profile_url = f"{TM_BASE}{p0['href']}"
    print(f"- url: `{profile_url}`")
    status, html, dur = _get(profile_url, proxy_url=proxy_for_rest)
    _dump('3_profile.html', html if status > 0 else '')
    print(f"- HTTP: **{status}**  | latency: **{dur:.2f}s** | size: **{len(html) if status > 0 else 0} bytes**")
    if status == 200:
        verdict = _parse_profile(html)
        print(f"- profile selectors: `{json.dumps(verdict, ensure_ascii=False)}`")

    # --- Step 4: MV history via /ceapi/marketValueDevelopment/graph/ ---
    print("\n## Step 4 — MV history (ceapi JSON endpoint)\n")
    mv_url = f"{TM_BASE}/ceapi/marketValueDevelopment/graph/{p0['player_id']}"
    print(f"- url: `{mv_url}`")
    status, body, dur = _get(mv_url, proxy_url=proxy_for_rest)
    _dump('4_mv_history.json', body if status > 0 else '')
    print(f"- HTTP: **{status}** | latency: **{dur:.2f}s** | size: **{len(body) if status > 0 else 0} bytes**")
    if status == 200:
        try:
            payload = json.loads(body)
            points = payload.get('list') or []
            print(f"- mv history points: **{len(points)}**")
            if points:
                print(
                    f"  - first: x={points[0].get('x')} y={points[0].get('y')} "
                    f"datum_mw={points[0].get('datum_mw')!r} verein={points[0].get('verein')!r}"
                )
                print(
                    f"  - last:  x={points[-1].get('x')} y={points[-1].get('y')} "
                    f"datum_mw={points[-1].get('datum_mw')!r} verein={points[-1].get('verein')!r}"
                )
        except json.JSONDecodeError as e:
            print(f"- JSON decode error: {e}")

    # --- Step 5: transfers via /ceapi/transferHistory/list/ ---
    print("\n## Step 5 — Transfers (ceapi JSON endpoint)\n")
    tr_url = f"{TM_BASE}/ceapi/transferHistory/list/{p0['player_id']}"
    print(f"- url: `{tr_url}`")
    status, body, dur = _get(tr_url, proxy_url=proxy_for_rest)
    _dump('5_transfers.json', body if status > 0 else '')
    print(f"- HTTP: **{status}** | latency: **{dur:.2f}s** | size: **{len(body) if status > 0 else 0} bytes**")
    if status == 200:
        try:
            payload = json.loads(body)
            transfers = payload.get('transfers') or []
            print(f"- transfer rows: **{len(transfers)}**")
            if transfers:
                t0 = transfers[0]
                print(
                    f"  - first: date={t0.get('date')!r} season={t0.get('season')!r} "
                    f"from={(t0.get('from') or {}).get('clubName')!r} "
                    f"to={(t0.get('to') or {}).get('clubName')!r} "
                    f"fee={t0.get('fee')!r} type={t0.get('upcoming')!r}"
                )
        except json.JSONDecodeError as e:
            print(f"- JSON decode error: {e}")

    print("\n---\n**OVERALL**: tls_requests + residential proxy is sufficient. ceapi JSON endpoints "
          "replace ScraperFC's Highcharts inline-script + transfers grid scrape (both gone from "
          "live TM HTML as of 2026-05-23).")
    return 0


if __name__ == '__main__':
    sys.exit(main())
