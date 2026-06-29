#!/usr/bin/env python3
"""Probe live WhoScored ``/Players/{id}`` pages for biographical attributes.

Issue #37, Phase 1 (de-risk). Decides build-vs-wontfix the same way issue #38
(Understat ``/player/{id}``) was decided — that one turned out wontfix because
the page carried no biography. WhoScored, unlike SofaScore's clean JSON API, is
an HTML page behind Cloudflare fetched through FlareSolverr, so the real field
set (esp. ``weight_kg`` / ``preferred_foot``) is unverified until we look.

Run inside airflow-scheduler (FlareSolverr lives in-cluster; scheduler has 4G):
    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/probe_whoscored_players.py --limit 10

Output:
  - /tmp/whoscored_players_probe/{id}.html  -- raw rendered HTML dumps
  - stdout                                  -- markdown verdict table

Exit codes:
  0  -> bio present:  >=8 pages fetched AND height parsed on >=6   (BUILD)
  2  -> bio absent / sparse                                        (WONTFIX signal)
  1  -> hard failure (no player_ids, FlareSolverr unreachable, ...)

No side effects: GET-only, no Iceberg writes.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, '/opt/airflow')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('probe_whoscored_players')

DUMP_DIR = Path('/tmp/whoscored_players_probe')

PLAYER_URL_PRIMARY = "https://www.whoscored.com/Players/{pid}/Show"
PLAYER_URL_FALLBACK = "https://www.whoscored.com/Players/{pid}"

# Heuristic field detectors. WhoScored renders the player info box server-side;
# values may sit in DOM text and/or an inline JS object. We stay loose on
# purpose — the point of the probe is to LEARN the real shape, not to parse it
# correctly yet.
_RE_HEIGHT_CM = re.compile(r'(\d{2,3})\s*cm', re.I)
_RE_WEIGHT_KG = re.compile(r'(\d{2,3})\s*kg', re.I)
_RE_DOB_ISO = re.compile(r'(\d{4}-\d{2}-\d{2})')
_RE_TITLE = re.compile(r'<title>(.*?)</title>', re.I | re.S)

# Embedded-JSON markers: WhoScored match pages carry matchCentreData; player
# pages historically expose ``defaultWsPlayerId`` and a player object. Report
# whether anything JSON-shaped is present so we know parser strategy upfront.
_JSON_MARKERS = ('defaultWsPlayerId', '"playerId"', 'matchCentreData', '"isManager"')


def _resolve_player_ids(limit: int) -> List[Tuple[str, Optional[str]]]:
    """DISTINCT (player_id, player_name) from bronze.whoscored_events.

    ``player_id`` there is ``DOUBLE`` — the double-cast footgun: a plain
    ``CAST AS varchar`` yields ``'9.5408E4'``. Cast through ``BIGINT`` first
    (CLAUDE.md → top footguns).
    """
    from scrapers.base.trino_manager import TrinoTableManager

    sql = (
        "SELECT DISTINCT CAST(CAST(player_id AS BIGINT) AS varchar) AS pid, player "
        "FROM iceberg.bronze.whoscored_events "
        "WHERE player_id IS NOT NULL AND player IS NOT NULL "
        f"LIMIT {int(limit)}"
    )
    mgr = TrinoTableManager()
    rows = mgr._execute(sql, fetch=True) or []
    out: List[Tuple[str, Optional[str]]] = []
    for r in rows:
        if r and r[0]:
            out.append((str(r[0]), r[1] if len(r) > 1 else None))
    return out


def _detect_fields(html: str) -> dict:
    """Best-effort presence/value detection for the target biographical fields."""
    lower = html.lower()
    title_m = _RE_TITLE.search(html)
    name = title_m.group(1).strip() if title_m else None

    height_m = _RE_HEIGHT_CM.search(html)
    weight_m = _RE_WEIGHT_KG.search(html)
    dob_m = _RE_DOB_ISO.search(html)

    return {
        'name': name,
        'height_cm': int(height_m.group(1)) if height_m else None,
        'weight_kg': int(weight_m.group(1)) if weight_m else None,
        'date_of_birth': dob_m.group(1) if dob_m else None,
        # Keyword presence — value extraction deferred to Phase 2 once we see the markup.
        'has_height_kw': 'height' in lower,
        'has_weight_kw': 'weight' in lower,
        'has_nationality_kw': 'nationality' in lower,
        'has_foot_kw': ('footed' in lower or 'preferred foot' in lower or 'preferredfoot' in lower),
        'has_embedded_json': any(m.lower() in lower for m in _JSON_MARKERS),
    }


def _fetch(client, session_id: str, pid: str) -> Tuple[int, str]:
    """Try ``/Players/{id}/Show`` then bare ``/Players/{id}``; return (status, html)."""
    status, html = 0, ''
    for tmpl in (PLAYER_URL_PRIMARY, PLAYER_URL_FALLBACK):
        url = tmpl.format(pid=pid)
        sol = client.get(url, session_id, max_timeout_ms=120_000)
        status = sol.get('status') or 0
        html = sol.get('html') or ''
        if status == 200 and html:
            return status, html
        logger.warning("  pid=%s url=%s -> status=%s len=%d", pid, url, status, len(html))
    return status, html


def main() -> int:
    parser = argparse.ArgumentParser(description='Probe WhoScored /Players/{id} pages')
    parser.add_argument('--limit', type=int, default=10, help='Number of players to probe')
    parser.add_argument(
        '--flaresolverr-url', type=str,
        default=os.environ.get('FLARESOLVERR_URL', 'http://flaresolverr:8191'),
    )
    args = parser.parse_args()

    DUMP_DIR.mkdir(parents=True, exist_ok=True)

    # Lazy import — mirrors run_whoscored_scraper.py; avoids pulling the heavy
    # scrapers/__init__.py side-effects at module-parse time.
    from scrapers.base.flaresolverr_client import (
        FlareSolverrClient,
        FlareSolverrError,
        describe_proxy_mode,
    )

    try:
        players = _resolve_player_ids(args.limit)
    except Exception as e:
        logger.error("Could not resolve player_ids from bronze.whoscored_events: %s", e)
        return 1
    if not players:
        logger.error("No player_ids resolved — is bronze.whoscored_events populated?")
        return 1
    logger.info("Resolved %d player_ids to probe", len(players))

    client = FlareSolverrClient(url=args.flaresolverr_url)
    if not client.health():
        logger.error("FlareSolverr not healthy at %s", args.flaresolverr_url)
        return 1

    # Proxy-less by default (WhoScored is proxy-less by design, #616); honour
    # PROXY_FILTER_URL if the ad-tech filter is wired in this env (#652).
    proxy_url = os.environ.get('PROXY_FILTER_URL') or None
    session_id = f"wsp-probe-{uuid.uuid4().hex[:8]}"
    client.create_session(session_id, proxy_url=proxy_url)
    logger.info("FS session %s (proxy mode: %s)", session_id, describe_proxy_mode(proxy_url))

    results: List[dict] = []
    try:
        for pid, name in players:
            try:
                status, html = _fetch(client, session_id, pid)
            except FlareSolverrError as e:
                logger.warning("  pid=%s FlareSolverr error: %s", pid, e)
                results.append({'pid': pid, 'name': name, 'fetched': False})
                continue
            if status != 200 or not html:
                results.append({'pid': pid, 'name': name, 'fetched': False})
                continue
            (DUMP_DIR / f"{pid}.html").write_text(html, encoding='utf-8')
            fields = _detect_fields(html)
            fields.update({'pid': pid, 'name_events': name, 'fetched': True})
            results.append(fields)
            logger.info(
                "  pid=%-8s fetched bytes=%-7d height=%s dob=%s json=%s",
                pid, len(html), fields['height_cm'], fields['date_of_birth'],
                fields['has_embedded_json'],
            )
    finally:
        client.destroy_session(session_id)

    # -------- Verdict --------
    fetched = [r for r in results if r.get('fetched')]
    n = len(results)
    n_fetched = len(fetched)

    def _count(key) -> int:
        return sum(1 for r in fetched if r.get(key))

    rows = [
        ('fetched (200 OK)', n_fetched, ''),
        ('height_cm parsed', _count('height_cm'),
         next((str(r['height_cm']) for r in fetched if r.get('height_cm')), '')),
        ('weight_kg parsed', _count('weight_kg'),
         next((str(r['weight_kg']) for r in fetched if r.get('weight_kg')), '')),
        ('date_of_birth parsed', _count('date_of_birth'),
         next((str(r['date_of_birth']) for r in fetched if r.get('date_of_birth')), '')),
        ('"Height" keyword', _count('has_height_kw'), ''),
        ('"Weight" keyword', _count('has_weight_kw'), ''),
        ('"Nationality" keyword', _count('has_nationality_kw'), ''),
        ('foot keyword (footed/preferredFoot)', _count('has_foot_kw'), ''),
        ('embedded JSON marker', _count('has_embedded_json'), ''),
    ]

    print()
    print(f"## WhoScored /Players/{{id}} probe — {n_fetched}/{n} pages fetched")
    print()
    print("| signal | found (k/N fetched) | sample |")
    print("|---|---|---|")
    for label, cnt, sample in rows:
        print(f"| {label} | {cnt}/{n_fetched} | {sample} |")
    print()
    print(f"HTML dumps: {DUMP_DIR}/*.html")

    # Gate: enough pages AND a real height value on a majority → BUILD.
    height_hits = _count('height_cm')
    if n_fetched >= 8 and height_hits >= 6:
        print("\nVERDICT: BUILD — biography present (>=8 fetched, height on >=6).")
        return 0
    print(
        "\nVERDICT: WONTFIX signal — bio absent / sparse "
        f"({n_fetched} fetched, height on {height_hits}). "
        "Inspect HTML dumps before deciding."
    )
    return 2


if __name__ == '__main__':
    sys.exit(main())
