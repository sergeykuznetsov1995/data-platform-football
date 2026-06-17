"""Probe live Transfermarkt COACH (trainer) pages — issue #434, Phase C0.

Validates staff-page + coach-profile selectors before writing read_coaches.
Reuses the transport/proxy helpers from probe_transfermarkt.py. GET-only, no
Iceberg writes.

Run inside a container (proxys.txt mounted at repo root):
    docker compose exec -T airflow-scheduler \
        python /opt/airflow/scripts/probe_transfermarkt_coaches.py

Output:
  - /tmp/transfermarkt_probe/c{step}_*.html  -- raw HTML dumps
  - stdout                                   -- markdown verdict

Steps:
  1. League listing (reuse) → first club
  2. Staff page  /{club_slug}/mitarbeiter/verein/{club_id} — find trainer links
     + try to read role / dob / nationality straight off the staff table
  3. Coach profile /{slug}/profil/trainer/{id} — confirm dob/nationality itemprops
  4. Trainer-history /{club_slug}/mitarbeiterhistorie/verein/{club_id} (issue
     #619) — confirm the URL + run the REAL scraper parser _parse_coach_history
     against live HTML (caretakers / mid-season stints with appointed/left dates)
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/scripts')

from bs4 import BeautifulSoup  # noqa: E402

from probe_transfermarkt import (  # noqa: E402
    APL_LISTING,
    DUMP_DIR,
    TM_BASE,
    _dump,
    _first_proxy,
    _get,
    _parse_listing,
)

# Issue #619: validate the REAL scraper parser against live trainer-history HTML.
from scrapers.transfermarkt.scraper import (  # noqa: E402
    _CLUB_COACH_HISTORY_PATH,
    _parse_coach_history,
)

_TRAINER_HREF_RE = re.compile(r'^/([^/]+)/profil/trainer/(\d+)')


def _parse_staff(html: str) -> list[dict]:
    """Find coach rows on a club staff page: links to /profil/trainer/<id>.

    Also captures the row's full text so we can eyeball whether role / dob /
    nationality are present inline (like the squad page carries them).
    """
    soup = BeautifulSoup(html, 'html.parser')
    coaches = []
    for a in soup.find_all('a', href=True):
        m = _TRAINER_HREF_RE.match(a['href'])
        if not m:
            continue
        # Walk up to the enclosing <tr> for role/context.
        row = a.find_parent('tr')
        row_text = (
            re.sub(r'\s+', ' ', row.get_text(' ', strip=True))[:160]
            if row else ''
        )
        coaches.append({
            'slug': m.group(1),
            'coach_id': m.group(2),
            'name': a.get_text(strip=True),
            'href': a['href'],
            'row_text': row_text,
        })
    seen, uniq = set(), []
    for c in coaches:
        if c['coach_id'] in seen:
            continue
        seen.add(c['coach_id'])
        uniq.append(c)
    return uniq


def _parse_coach_profile(html: str) -> dict:
    """Verify coach-profile selectors mirror the player-profile ones."""
    soup = BeautifulSoup(html, 'html.parser')
    out: dict = {}
    name_el = soup.find('h1', {'class': 'data-header__headline-wrapper'})
    out['name_found'] = name_el is not None
    if name_el:
        out['name_text'] = re.sub(r'\s+', ' ', name_el.get_text(' ', strip=True))[:80]
    dob_el = soup.find('span', {'itemprop': 'birthDate'})
    out['dob_itemprop_found'] = dob_el is not None
    if dob_el:
        out['dob_text'] = dob_el.get_text(' ', strip=True)[:40]
    nat_el = soup.find('span', {'itemprop': 'nationality'})
    out['nationality_itemprop_found'] = nat_el is not None
    if nat_el:
        out['nationality_text'] = nat_el.get_text(' ', strip=True)[:40]
    return out


def main() -> int:
    print("# Transfermarkt COACH probe — issue #434, phase C0\n")
    print(f"_Run at_: `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`\n")

    proxy = _first_proxy()
    print(f"_Proxy_: `{'configured' if proxy else 'none'}`\n")

    # --- Step 1: listing → first club ---
    print("## Step 1 — League listing\n")
    status, html, dur = _get(APL_LISTING, proxy_url=proxy)
    print(f"- HTTP **{status}** | {dur:.2f}s | {len(html) if status > 0 else 0} bytes")
    if status != 200:
        print("\n**VERDICT**: listing unreachable — halt.")
        return 2
    clubs = _parse_listing(html)
    print(f"- parsed clubs: **{len(clubs)}**")
    if not clubs:
        print("\n**VERDICT**: no clubs parsed — selectors drifted.")
        return 3
    club0 = clubs[0]
    print(f"- first club: `{club0['club_id']}` {club0['name']}")

    # --- Step 2: staff page ---
    print("\n## Step 2 — Staff page (mitarbeiter)\n")
    staff_url = (
        f"{TM_BASE}/{club0['club_slug']}/mitarbeiter/verein/{club0['club_id']}"
        f"/saison_id/2025"
    )
    print(f"- url: `{staff_url}`")
    status, html, dur = _get(staff_url, proxy_url=proxy)
    _dump('c2_staff.html', html if status > 0 else '')
    print(f"- HTTP **{status}** | {dur:.2f}s | {len(html) if status > 0 else 0} bytes")
    coaches = _parse_staff(html) if status == 200 else []
    print(f"- parsed trainer links: **{len(coaches)}**")
    for c in coaches[:6]:
        print(f"  - `{c['coach_id']}` {c['name']} → `{c['href']}`")
        print(f"    row: {c['row_text']!r}")
    if not coaches:
        print("\n**VERDICT**: staff page has no `/profil/trainer/<id>` links — selector/URL drift.")
        return 4

    # --- Step 3: coach profile ---
    print("\n## Step 3 — Coach profile\n")
    c0 = coaches[0]
    prof_url = f"{TM_BASE}{c0['href']}"
    print(f"- url: `{prof_url}`")
    status, html, dur = _get(prof_url, proxy_url=proxy)
    _dump('c3_coach_profile.html', html if status > 0 else '')
    print(f"- HTTP **{status}** | {dur:.2f}s | {len(html) if status > 0 else 0} bytes")
    if status == 200:
        verdict = _parse_coach_profile(html)
        print(f"- coach profile selectors: `{json.dumps(verdict, ensure_ascii=False)}`")

    # --- Step 4: trainer-history (issue #619) ---
    print("\n## Step 4 — Trainer-history (mitarbeiterhistorie)\n")
    history_url = TM_BASE + _CLUB_COACH_HISTORY_PATH.format(
        club_slug=club0['club_slug'], club_id=club0['club_id'],
    )
    print(f"- url: `{history_url}`")
    status, html, dur = _get(history_url, proxy_url=proxy)
    _dump('c4_trainer_history.html', html if status > 0 else '')
    print(f"- HTTP **{status}** | {dur:.2f}s | {len(html) if status > 0 else 0} bytes")
    if status == 200:
        # Run the SHIPPING parser so the probe tests real code, not a fork.
        stints = _parse_coach_history(html, club_id=club0['club_id'])
        print(f"- _parse_coach_history → **{len(stints)}** stint(s)")
        for s in stints[:8]:
            print(
                f"  - `{s['coach_id']}` {s['name']} | role={s['role']!r} "
                f"| appointed={s['appointed_date']} left={s['left_date']}"
            )
        dated = sum(1 for s in stints if s['appointed_date'])
        if not stints:
            print("\n**VERDICT Step 4**: history page parsed 0 stints — URL or "
                  "table.items selector drifted; inspect c4_trainer_history.html "
                  "and fix _parse_coach_history before the prod run.")
        elif dated == 0:
            print("\n**VERDICT Step 4**: stints found but NO appointed dates parsed "
                  "— the season-window filter would keep everything; fix the date "
                  "cell extraction.")
        else:
            print(f"\n**VERDICT Step 4**: OK — {dated}/{len(stints)} stints carry an "
                  "appointed date; caretakers/mid-season coaches are captured.")
    else:
        print("\n**VERDICT Step 4**: history page unreachable — confirm the "
              "mitarbeiterhistorie URL pattern.")

    print("\n---\n**NOTE**: Step 2/3 (snapshot) is the #434 path; issue #619 uses "
          "Step 4 (trainer-history) so mid-season/caretaker coaches reach "
          "dim_manager. read_coaches now harvests from the Step-4 page.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
