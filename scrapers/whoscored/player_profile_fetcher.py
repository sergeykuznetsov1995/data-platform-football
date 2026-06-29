"""FlareSolverr-backed WhoScored player-profile fetcher + parser.

WhoScored exposes time-invariant player attributes on ``/Players/{id}/Show``.
Unlike match events (a ``matchCentreData`` JSON literal), the player info box is
plain server-rendered DOM — a column of ``<div>``s each holding
``<span class="info-label">Label: </span>value`` (confirmed by the issue-#37
probe, ``scripts/probe_whoscored_players.py``). So this module:

1. Fetches ``/Players/{id}/Show`` HTML through a :class:`FlareSolverrClient`
   session (Cloudflare handled by the FlareSolverr service);
2. Parses the ``info-label`` block with BeautifulSoup into a flat Bronze row.

**Fields WhoScored actually publishes** (probe verdict, 10/10 players): name,
current team (+id), shirt number, age, date of birth, height, nationality
(+country flag code), positions. It does NOT publish ``weight_kg`` or
``preferred_foot`` — those columns from the issue's wishlist are absent here and
stay the job of the FotMob / SofaScore profile sources.

Use as::

    from scrapers.base.flaresolverr_client import FlareSolverrClient
    from scrapers.whoscored.player_profile_fetcher import (
        fetch_player_profile_html,
        parse_player_profile,
    )

    client = FlareSolverrClient(...)
    session_id = client.create_session(...)
    html = fetch_player_profile_html(client, "355401", session_id)
    row = parse_player_profile(html, "355401", "ENG-Premier League", "2526")
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, Optional

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from scrapers.base.flaresolverr_client import FlareSolverrClient

logger = logging.getLogger(__name__)

# ``/Players/{id}/Show`` redirects to the full slugged URL; FlareSolverr follows
# the redirect and returns the rendered target page. The bare ``/Players/{id}``
# form 404s for some ids, so we always request ``/Show``.
PLAYER_URL = "https://www.whoscored.com/Players/{}/Show"

# Columns projected into bronze.whoscored_player_profile (before run metadata).
ANCHOR_COLS = [
    'player_id', 'name', 'current_team_id', 'current_team_name',
    'shirt_number', 'age', 'date_of_birth', 'height_cm',
    'nationality', 'country_code', 'positions', 'league', 'season',
]

_RE_HEIGHT_CM = re.compile(r'(\d{2,3})\s*cm', re.I)
# DOB renders inside an <i> as DD-MM-YYYY, e.g. ``06-06-2001`` (NOT ISO).
_RE_DOB = re.compile(r'\b(\d{2})-(\d{2})-(\d{4})\b')
_RE_LEADING_INT = re.compile(r'(\d+)')
_RE_TEAM_ID = re.compile(r'/teams/(\d+)/')


def fetch_player_profile_html(
    client: "FlareSolverrClient",
    player_id: str,
    session_id: str,
    max_timeout_ms: int = 120_000,
) -> Optional[str]:
    """Fetch a WhoScored player page through FlareSolverr; return rendered HTML.

    Returns ``None`` on a non-200 status. ``FlareSolverrError`` /
    ``FlareSolverrTimeout`` / ``FlareSolverrCFChallengeFailed`` propagate to the
    caller for retry / session-rotation handling (same contract as the events
    fetcher).
    """
    url = PLAYER_URL.format(player_id)
    solution = client.get(url, session_id, max_timeout_ms=max_timeout_ms)
    status = solution.get("status")
    if status != 200:
        logger.warning(
            "WhoScored: FS returned status=%s for player_id=%s", status, player_id
        )
        return None
    return solution.get("html") or None


def _text_after_label(parent) -> str:
    """Text content of an ``info-label`` row minus the label itself.

    ``<div><span class="info-label">Height: </span>180cm</div>`` → ``"180cm"``.
    """
    full = parent.get_text(" ", strip=True)
    label = parent.find("span", class_="info-label")
    lab = label.get_text(" ", strip=True) if label else ""
    return full[len(lab):].strip() if lab and full.startswith(lab) else full


def _to_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    m = _RE_LEADING_INT.search(value)
    return int(m.group(1)) if m else None


def parse_player_profile(
    html: Optional[str],
    player_id: str,
    league: str,
    season: str,
) -> Optional[Dict]:
    """Project a WhoScored player page into a flat Bronze row.

    Keyed off the ``info-label`` text (not DOM order) so a layout reshuffle or a
    missing field degrades to NULL rather than mis-aligning. Returns ``None``
    when no ``info-label`` block is present (not a player page / markup drift).
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    fields = {
        label.get_text(strip=True).rstrip(":").strip().lower(): label.parent
        for label in soup.select("span.info-label")
    }
    if not fields:
        return None

    row: Dict = {col: None for col in ANCHOR_COLS}
    row['player_id'] = str(player_id)
    row['league'] = league
    row['season'] = season

    if 'name' in fields:
        row['name'] = _text_after_label(fields['name']) or None

    if 'current team' in fields:
        anchor = fields['current team'].find("a", class_="team-link")
        if anchor is not None:
            row['current_team_name'] = anchor.get_text(strip=True) or None
            tm = _RE_TEAM_ID.search(anchor.get("href", "") or "")
            if tm:
                row['current_team_id'] = tm.group(1)

    if 'shirt number' in fields:
        row['shirt_number'] = _to_int(_text_after_label(fields['shirt number']))

    if 'age' in fields:
        parent = fields['age']
        row['age'] = _to_int(_text_after_label(parent))
        i_tag = parent.find("i")
        if i_tag is not None:
            dm = _RE_DOB.search(i_tag.get_text(strip=True))
            if dm:
                dd, mm, yyyy = dm.groups()
                row['date_of_birth'] = f"{yyyy}-{mm}-{dd}"  # normalise to ISO

    if 'height' in fields:
        hm = _RE_HEIGHT_CM.search(_text_after_label(fields['height']))
        if hm:
            row['height_cm'] = int(hm.group(1))

    if 'nationality' in fields:
        iconize = fields['nationality'].find("span", class_="iconize")
        if iconize is not None:
            row['nationality'] = iconize.get_text(strip=True) or None
            flag = iconize.find("span", class_="country")
            if flag is not None:
                for cls in flag.get("class", []):
                    if cls.startswith("flg-"):
                        row['country_code'] = cls[len("flg-"):]  # 'dz', 'gb-eng'
                        break

    if 'positions' in fields:
        positions = re.sub(r"\s+", " ", _text_after_label(fields['positions'])).strip()
        row['positions'] = positions or None

    return row
