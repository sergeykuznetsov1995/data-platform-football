"""
Transfermarkt Scraper
=====================

Bronze ingest for Transfermarkt — player snapshots (`bronze.transfermarkt_players`),
transfer events (`bronze.transfermarkt_transfers`), and market-value history
(`bronze.transfermarkt_market_value_history`).

NOTICE
------
URL patterns and DataFrame column shapes informed by oseymour/ScraperFC
(`src/ScraperFC/transfermarkt.py`, GPL-3.0). All code in this module is
written independently from scratch against the live Transfermarkt HTML and
the ``ceapi`` JSON endpoints. The two parts of ScraperFC that broke between
its last release and 2026-05-23 (inline Highcharts script for MV history,
and the ``div.grid.tm-player-transfer-history-grid`` block) were replaced
with the JSON endpoints that the live site now exposes:

  - /ceapi/marketValueDevelopment/graph/{player_id}  →  MV history
  - /ceapi/transferHistory/list/{player_id}          →  transfer events

See ``scripts/research/bench_transfermarkt_fetch.py`` for the bounded live
benchmark that validates the production transport and parser contracts.

Source: https://www.transfermarkt.com
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

from scrapers.base.base_scraper import BaseScraper
from scrapers.transfermarkt.client import (
    ProxyFilterLeaseProvider,
    TransfermarktHttpClient,
)
from scrapers.transfermarkt.models import (
    FetchOutcome,
    FetchRecord,
    FetchStatus,
    TransfermarktError,
)
from scrapers.transfermarkt.registry import (
    CompetitionRecord,
    SeasonFormat,
    canonical_season,
    deterministic_scope_id,
    resolve_competition,
    season_window_year,
)
from scrapers.utils.proxy_manager import ProxyManager
from scrapers.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Endpoints & constants
# ---------------------------------------------------------------------------

# The .com host is what the registry crawl reads and what every residential
# exit serves reliably; the .us mirror carries the same English content but a
# share of exits cannot reach it, which exhausts a scope's retries.
_TM_BASE = "https://www.transfermarkt.com"

# {club_slug}/kader/verein/{club_id}/saison_id/{year}/plus/1  (the /plus/1
# variant exposes the wider, detailed squad table — same selector contract
# as the default page but with extra metadata columns visible.)
_CLUB_SQUAD_PATH = "/{club_slug}/kader/verein/{club_id}/saison_id/{year}/plus/1"
# JSON, no auth, no proxy required cookie-wise (but TM CF still requires proxy).
_PLAYER_MV_HISTORY_PATH = "/ceapi/marketValueDevelopment/graph/{player_id}"
_PLAYER_TRANSFERS_PATH = "/ceapi/transferHistory/list/{player_id}"
_COACH_PROFILE_PATH = "/{coach_slug}/profil/trainer/{coach_id}"
# Club trainer-history (issue #619). The staff page above is a single
# end-of-season snapshot → it misses mid-season replacements and caretakers
# (who ARE in the FBref spine because FBref attributes them to specific
# matches). This page lists EVERY manager the club has had, with appointed /
# end-of-tenure dates and a role; we keep the rows whose tenure overlaps the
# requested season. No saison_id — the page is full history, filtered in
# _parse_coach_history.  The production-client fixture/live validation is in
# scripts/research/bench_transfermarkt_fetch.py.
_CLUB_COACH_HISTORY_PATH = (
    "/{club_slug}/mitarbeiterhistorie/verein/{club_id}/plus/1"
)

R0_2B_FALLBACK_MARKER = 'TM_FALLBACK'

PARSER_REVISION = os.environ.get('TM_PARSER_VERSION', 'registry-v1')
SCHEMA_REVISION = os.environ.get('TM_SCHEMA_VERSION', '3')

# Per-player loops abort after this many fetch failures in a row (burned
# proxies, CF lockout). Module-level so tests can monkeypatch it.
_MAX_CONSECUTIVE_FAILURES = 50

# Per-player loops abort when the in-run success/attempted ratio falls
# below this threshold. Module-level so tests can monkeypatch it.
_MIN_SUCCESS_RATIO = 0.9


class ConsecutiveFailureError(RuntimeError):
    """Raised when a per-player loop hits the consecutive-failure cap.

    Propagating (instead of returning a partial frame) protects the
    existing bronze partition from a replace_partitions wipe (#457).
    """


class PartialScrapeError(RuntimeError):
    """Raised when the in-run success ratio falls below ``_MIN_SUCCESS_RATIO``.

    Intermittent failures (e.g. every 2nd player fails) reset the
    consecutive counter and never trip the #457 cap, yet still produce a
    half-empty frame that the runner would save with replace_partitions.
    Total failure (0 successes) is NOT raised here — the empty frame takes
    the graceful TM_FALLBACK path and nothing gets saved (#484).
    """


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def _season_window(
    season: int | str,
    season_format: SeasonFormat | str,
) -> Tuple[date, date]:
    """Return explicit split-year or single-year bounds for coach histories."""

    y = int(season)
    fmt = SeasonFormat(str(
        season_format.value
        if isinstance(season_format, SeasonFormat) else season_format
    ))
    if fmt is SeasonFormat.SPLIT_YEAR:
        return date(y, 7, 1), date(y + 1, 6, 30)
    if fmt is SeasonFormat.SINGLE_YEAR:
        return date(y, 1, 1), date(y, 12, 31)
    raise TransfermarktError('unknown season_format blocks coach-history crawl')


def _competition_listing_url(
    competition: CompetitionRecord,
    edition_id: str | int,
) -> str:
    """Build a listing URL from the discovered source URL, preserving route type."""

    parsed = urlsplit(competition.source_url)
    path = parsed.path.rstrip('/')
    if not path:
        raise TransfermarktError(
            f'{competition.competition_id}: source_url has no path'
        )
    return urlunsplit((
        'https', 'www.transfermarkt.com', f'{path}/plus/',
        f'saison_id={edition_id}', '',
    ))


def _stint_overlaps_season(
    stint: Dict, win_start: date, win_end: date
) -> bool:
    """True if a manager-stint's [appointed, left] interval overlaps the season
    window (issue #619). A missing ``left`` means the incumbent (open-ended);
    a missing ``appointed`` means unbounded-left. A row with BOTH dates missing
    is rejected: treating malformed rows as all-season stints causes needless
    paid profile requests and false manager memberships.
    """
    appointed = stint.get('appointed_date')
    left = stint.get('left_date')
    if appointed is None and left is None:
        return False
    return (appointed is None or appointed <= win_end) and \
           (left is None or left >= win_start)


_INT_RE = re.compile(r'-?\d+')


def _coerce_int(raw) -> Optional[int]:
    """Loose int coerce — `None`/``''``/non-numeric → None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    s = str(raw).strip()
    if not s:
        return None
    m = _INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except (TypeError, ValueError):
        return None


# The currency symbol and the magnitude suffix are host/locale properties. A
# figure in another currency cannot become a *_eur column, and a suffix we do
# not know is a thousandfold error waiting to happen — both are schema errors,
# never a silently wrong number.
_TM_MONEY_RE = re.compile(
    r'(?P<currency>€|£|\$|US\$)\s*(?P<amount>[\d.,]+)\s*(?P<unit>[A-Za-z.]*)',
)
_TM_MONEY_UNITS = {
    '': 1,
    'k': 1_000,
    'th': 1_000,
    'th.': 1_000,
    'tsd': 1_000,
    'tsd.': 1_000,
    'thousand': 1_000,
    'm': 1_000_000,
    'mio': 1_000_000,
    'mio.': 1_000_000,
    'mill': 1_000_000,
    'million': 1_000_000,
    'b': 1_000_000_000,
    'bn': 1_000_000_000,
    'mrd': 1_000_000_000,
    'mrd.': 1_000_000_000,
    'billion': 1_000_000_000,
}


class MoneyLocaleError(TransfermarktError):
    """The source stated a figure this parser must not guess at."""


def _normalise_decimal_number(raw: str) -> Optional[Decimal]:
    """Parse either TM's dot-decimal or comma-decimal representation exactly."""

    value = raw.strip().replace(' ', '')
    if not value:
        return None
    if ',' in value and '.' in value:
        # The right-most separator is decimal; the other is grouping.
        decimal_sep = ',' if value.rfind(',') > value.rfind('.') else '.'
        grouping_sep = '.' if decimal_sep == ',' else ','
        value = value.replace(grouping_sep, '').replace(decimal_sep, '.')
    elif ',' in value or '.' in value:
        sep = ',' if ',' in value else '.'
        parts = value.split(sep)
        if len(parts) > 2:
            # Repeated separators are grouping except for a final 1-2 digit
            # decimal component (e.g. ``1.234.567,89`` handled above).
            value = ''.join(parts)
        elif len(parts) == 2:
            whole, fraction = parts
            value = (
                whole + fraction
                if len(fraction) == 3
                else f"{whole}.{fraction}"
            )
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _parse_tm_money_eur(raw) -> Optional[int]:
    """Parse Transfermarkt money strings → integer EUR.

    Examples::
        '€ 45.00 m'   → 45_000_000
        '€500k'       → 500_000
        '€1.20bn'     → 1_200_000_000
        '?' / '-' / '' → None

    Both ``€80.00m`` and ``€80,00m`` are accepted.  ``Decimal`` is used so
    large values never pass through binary floating point.
    """
    if raw is None:
        return None
    s = str(raw)
    m = _TM_MONEY_RE.search(s)
    if not m:
        return None
    currency = m.group('currency')
    if currency != '€':
        raise MoneyLocaleError(
            f'source stated a figure in {currency}, not EUR: {s!r}'
        )
    num = _normalise_decimal_number(m.group('amount'))
    if num is None:
        return None
    unit = (m.group('unit') or '').lower()
    if unit not in _TM_MONEY_UNITS:
        raise MoneyLocaleError(f'unknown magnitude suffix in {s!r}')
    return int(num * Decimal(_TM_MONEY_UNITS[unit]))


_HEIGHT_RE = re.compile(r'(\d+[,.]\d+)\s*m')


def _parse_height_cm(raw) -> Optional[int]:
    """``'1,89 m'`` / ``'1.89m'`` → ``189``."""
    if not raw:
        return None
    m = _HEIGHT_RE.search(str(raw))
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(',', '.')) * 100)
    except ValueError:
        return None


# .com renders dates day-first ('17/08/1993'); .us renders them as 'Aug 17, 1993'.
_TM_DATE_FORMATS = (
    '%b %d, %Y', '%B %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y',
)


# The market-value epoch is midnight in the source's own timezone; read as UTC
# it lands on the previous day.
_TM_TZ = ZoneInfo('Europe/Berlin')


def _market_value_date(entry: Dict) -> Optional[date]:
    """Read a market-value point's date from the machine field, not the rendered one.

    ``datum_mw`` is localised by host, and a day-first rendering parses just as
    happily as a month-first one — silently swapping day and month. The epoch in
    ``x`` says the same date unambiguously.
    """

    x_ms = entry.get('x')
    if isinstance(x_ms, (int, float)) and not isinstance(x_ms, bool):
        try:
            return datetime.fromtimestamp(float(x_ms) / 1000.0, _TM_TZ).date()
        except (OSError, OverflowError, ValueError):
            pass
    return _parse_tm_date(entry.get('datum_mw'))


def _transfer_date(entry: Dict) -> Optional[date]:
    """Read a transfer's date from the machine field, not the rendered one.

    ``date`` is localised by host (``Jan 1, 2026`` on .us, ``01/01/2026`` on
    .com, where day and month cannot be told apart), while ``dateUnformatted``
    is ISO on every host.
    """

    return _parse_tm_date(
        entry.get('dateUnformatted') or entry.get('date')
    )


def _parse_tm_date(raw) -> Optional[date]:
    """Parse the small set of date formats TM exposes. ``None`` on fail."""
    if not raw:
        return None
    s = str(raw).strip()
    # Strip trailing parenthesised age (`Feb 25, 1999 (26)` → `Feb 25, 1999`).
    s = re.sub(r'\s*\(\d+\)\s*$', '', s)
    for fmt in _TM_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Parsers (pure, easily unit-testable)
# ---------------------------------------------------------------------------

_CLUB_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/startseite/verein/(?P<id>\d+)')
_PLAYER_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/profil/spieler/(?P<id>\d+)')
_COACH_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/profil/trainer/(?P<id>\d+)')


def _parse_club_listing(html: str) -> List[Dict]:
    """Extract club rows from the league `startseite` page.

    Selectors validated by ``scripts/research/bench_transfermarkt_fetch.py``:
        table.items > td.hauptlink.no-border-links > a[href]
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if not table:
        return []

    clubs: List[Dict] = []
    seen: set = set()
    for td in table.find_all('td', {'class': 'hauptlink no-border-links'}):
        a = td.find('a', href=True)
        if not a:
            continue
        m = _CLUB_HREF_RE.match(a['href'])
        if not m:
            continue
        club_id = m.group('id')
        if club_id in seen:
            continue
        seen.add(club_id)
        clubs.append({
            'club_id': club_id,
            'club_slug': m.group('slug'),
            'club_name': a.get_text(strip=True),
            'href': a['href'],
        })
    return clubs


# Header text (lowercased) → bio field for the detailed (`/plus/1`) squad
# table. The column SET varies by view: TM renders `Contract` only for the
# season IT considers current and swaps in `Current club` for past seasons
# (verified live 2026-07-01), so columns are mapped by <thead> text, not by
# position.
_SQUAD_HEADER_FIELDS = {
    'date of birth/age': 'dob',
    'nat.': 'nationality',
    'height': 'height_cm',
    'foot': 'foot',
    'contract': 'contract_until',
}

_AGE_IN_PARENS_RE = re.compile(r'\((\d+)\)\s*$')


def _parse_squad_page(html: str, club_id: str) -> List[Dict]:
    """Extract per-player rows from a club's detailed (`/plus/1`) squad page.

    Selectors:
        table.items > thead > th          → header-driven column map
        table.items > tbody > tr (per-player rows)
            td.hauptlink > a              → player link (slug, id, name)
            td.posrela inline-table       → position (second inline row)
            td.rechts                     → market value (right-aligned)

    The `/plus/1` view already carries the full bio — dob/age, nationality,
    height, foot and (for the TM-current season) contract expiry — which is
    why the former per-player profile fetch (~530 req/run, ~58 MB residential
    proxy) is gone. Bio cells are read via ``_SQUAD_HEADER_FIELDS``; a page
    without the expected headers degrades to the stable core fields
    (id, slug, name, market_value_eur) with bio fields ``None``.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if not table:
        return []

    # Header-driven column map: index → bio field.
    col_fields: Dict[int, str] = {}
    n_headers = 0
    thead = table.find('thead')
    if thead:
        headers = [
            th.get_text(' ', strip=True).lower()
            for th in thead.find_all('th')
        ]
        n_headers = len(headers)
        col_fields = {
            i: _SQUAD_HEADER_FIELDS[h]
            for i, h in enumerate(headers)
            if h in _SQUAD_HEADER_FIELDS
        }

    players: List[Dict] = []
    seen: set = set()
    body = table.find('tbody') or table
    for tr in body.find_all('tr', recursive=False):
        hauptlink = tr.find('td', {'class': 'hauptlink'})
        if not hauptlink:
            continue
        a = hauptlink.find('a', href=True)
        if not a:
            continue
        m = _PLAYER_HREF_RE.match(a['href'])
        if not m:
            continue
        pid = m.group('id')
        if pid in seen:
            continue
        seen.add(pid)

        # Right-most cell is normally the market value column.
        mv_eur = None
        for td in tr.find_all('td', {'class': 'rechts hauptlink'}):
            mv_eur = _parse_tm_money_eur(td.get_text(' ', strip=True))
            if mv_eur is not None:
                break
        if mv_eur is None:
            # Fallback: scan all td.rechts cells for a money pattern.
            for td in tr.find_all('td', {'class': 'rechts'}):
                mv = _parse_tm_money_eur(td.get_text(' ', strip=True))
                if mv is not None:
                    mv_eur = mv
                    break

        row: Dict = {
            'player_id': pid,
            'player_slug': m.group('slug'),
            'name': a.get_text(strip=True),
            'club_id': str(club_id),
            'market_value_eur': mv_eur,
            'position': None,
            'dob': None,
            'age': None,
            'height_cm': None,
            'foot': None,
            'nationality': None,
            'contract_until': None,
        }

        # Position: second row of the inline name/position table.
        pos_td = tr.find('td', {'class': 'posrela'})
        inline = pos_td.find('table', {'class': 'inline-table'}) if pos_td else None
        if inline:
            inline_trs = inline.find_all('tr')
            if len(inline_trs) >= 2:
                row['position'] = inline_trs[-1].get_text(' ', strip=True) or None

        # Bio cells by header index. Only trust the map when the row's
        # top-level cell count matches the header count (colspan rows,
        # e.g. separators, would otherwise misalign every field).
        cells = tr.find_all('td', recursive=False)
        if col_fields and len(cells) == n_headers:
            for idx, field in col_fields.items():
                td = cells[idx]
                text = td.get_text(' ', strip=True)
                if field == 'dob':
                    row['dob'] = _parse_tm_date(text)
                    age_m = _AGE_IN_PARENS_RE.search(text)
                    row['age'] = int(age_m.group(1)) if age_m else None
                elif field == 'nationality':
                    img = td.find('img')
                    row['nationality'] = (
                        (img.get('title') or img.get('alt') or '').strip() or None
                        if img else None
                    )
                elif field == 'height_cm':
                    row['height_cm'] = _parse_height_cm(text)
                elif field == 'foot':
                    row['foot'] = text.lower() if text not in ('', '-') else None
                elif field == 'contract_until':
                    row['contract_until'] = _parse_tm_date(text)

        players.append(row)
    return players


def _parse_coach_profile(html: str, coach_id: str) -> Optional[Dict]:
    """Extract dob + nationality from a coach (`trainer`) profile page.

    Same ``data-header`` itemprop selectors as the player profile; the bounded
    production-client benchmark validates the headline/bio structure.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    name_el = soup.find('h1', {'class': 'data-header__headline-wrapper'})
    if not name_el:
        return None
    full_name = re.sub(
        r'^#\d+\s*', '', re.sub(r'\s+', ' ', name_el.get_text(' ', strip=True))
    )

    dob_el = soup.find('span', {'itemprop': 'birthDate'})
    dob = _parse_tm_date(dob_el.get_text(' ', strip=True)) if dob_el else None

    nat_el = soup.find('span', {'itemprop': 'nationality'})
    nationality = nat_el.get_text(' ', strip=True) if nat_el else None

    return {
        'coach_id': str(coach_id),
        'name': full_name,
        'dob': dob,
        'nationality': nationality,
    }


def _parse_coach_history(html: str, club_id: str) -> List[Dict]:
    """Extract every manager-stint from a club trainer-history page (issue #619).

    The ``mitarbeiterhistorie`` "Detailed view" (``/plus/1``) renders a
    ``table.items`` with one top-level ``tbody > tr`` per stint. Confirmed live
    column layout (header): ``Name/Date of birth · Nat. · Appointed · End of
    time in post · Time in post · Matches · W · D · L · PPG``. The first column
    embeds an ``inline-table`` holding a portrait link AND a name link — both
    point at ``/{slug}/profil/trainer/{id}`` but only the name link carries
    text. Returns one dict per stint:
    ``{coach_id, coach_slug, name, role, appointed_date, left_date, club_id}``.
    The caller filters by season window and dedups coach_id.

    Two live-layout traps this parser handles (both silently emptied the table
    before #793):

    - **Portrait link first.** Picking the first trainer link grabs the empty
      portrait ``<img>`` link → blank name → the row was dropped. We take the
      first trainer link that actually has text.
    - **DOB in the name cell.** The Name column also holds the date of birth, so
      reading dates from every descendant ``<td>`` mistook the DOB for the
      appointed date. We read dates from the row's DIRECT ``<td>`` cells, skip
      the first (Name/DoB), and take Appointed then End in order.

    The detailed view has no role/function column, so role defaults to
    ``'Manager'`` — caretakers are still CAPTURED (each as its own stint), they
    just are not role-labelled here (#793 followup).
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if table is None:
        return []
    body = table.find('tbody') or table

    # Fail closed on layout drift.  Dates are meaningful only when their
    # columns are identified by header; scanning arbitrary cells previously
    # mistook DOB/time-in-post values for stint boundaries.
    thead = table.find('thead')
    if thead is None:
        return []
    headers = [
        re.sub(r'\s+', ' ', th.get_text(' ', strip=True)).strip().lower()
        for th in thead.find_all('th')
    ]

    def _header_index(*names: str) -> Optional[int]:
        wanted = {name.lower() for name in names}
        return next((i for i, header in enumerate(headers) if header in wanted), None)

    appointed_idx = _header_index('appointed', 'appointed on', 'start date')
    left_idx = _header_index(
        'end of time in post', 'left', 'left on', 'end date', 'until'
    )
    if appointed_idx is None or left_idx is None:
        return []
    role_idx = _header_index('function', 'role', 'position')

    out: List[Dict] = []
    seen_stints: set = set()
    # Top-level rows only: each row embeds an inline-table whose nested <tr>/<td>
    # would otherwise be walked as phantom rows / stray date cells.
    for tr in body.find_all('tr', recursive=False):
        link = next(
            (a for a in tr.find_all('a', href=True)
             if _COACH_HREF_RE.match(a['href']) and a.get_text(strip=True)),
            None,
        )
        if link is None:
            continue
        m = _COACH_HREF_RE.match(link['href'])
        coach_id = m.group('id')
        name = link.get_text(strip=True)

        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(headers):
            continue
        appointed = _parse_tm_date(
            cells[appointed_idx].get_text(' ', strip=True)
        )
        left = _parse_tm_date(cells[left_idx].get_text(' ', strip=True))
        # Both missing means the row cannot be scoped to any season and must
        # not trigger a coach-profile request.
        if appointed is None and left is None:
            continue
        role = 'Manager'
        if role_idx is not None:
            parsed_role = cells[role_idx].get_text(' ', strip=True)
            if parsed_role and parsed_role != '-':
                role = parsed_role

        # De-dupe identical stint rows (a manager can appear once per spell).
        stint_key = (coach_id, appointed, left)
        if stint_key in seen_stints:
            continue
        seen_stints.add(stint_key)

        out.append({
            'coach_id': coach_id,
            'coach_slug': m.group('slug'),
            'name': name,
            'role': role,
            'appointed_date': appointed,
            'left_date': left,
            'club_id': str(club_id),
        })
    return out


def _parse_mv_history(payload: dict, player_id: str) -> List[Dict]:
    """Project ``/ceapi/marketValueDevelopment/graph/{id}`` JSON to flat rows.

    Source row::
        {"x": 1425250800000, "y": 300000, "mw": "€300k",
         "datum_mw": "Mar 2, 2015", "verein": "Milan Primavera",
         "age": "16", "wappen": "https://..."}

    Output row::
        {player_id, mv_date, value_eur, club_name, age, mv_raw}
    """
    if not isinstance(payload, dict):
        return []
    rows: List[Dict] = []
    for entry in payload.get('list') or []:
        if not isinstance(entry, dict):
            continue
        mv_date = _market_value_date(entry)
        if mv_date is None:
            continue
        rows.append({
            'player_id': str(player_id),
            'mv_date': mv_date,
            'value_eur': _coerce_int(entry.get('y')),
            'club_name': entry.get('verein'),
            'age': _coerce_int(entry.get('age')),
            'mv_raw': entry.get('mw'),
        })
    return rows


_CEAPI_CLUB_HREF_RE = re.compile(r'/verein/(\d+)')
_EVENT_SEASON_RE = re.compile(r'^(?P<start>\d{2}|\d{4})\s*[/_-]\s*(?P<end>\d{2}|\d{4})$')


def _extract_club_id_from_href(href) -> Optional[str]:
    if not href:
        return None
    m = _CEAPI_CLUB_HREF_RE.search(str(href))
    return m.group(1) if m else None


def _normalise_event_season(raw, transfer_date: Optional[date]) -> Optional[str]:
    """Normalise a source event season to the Bronze ``2526`` convention."""

    if raw is not None:
        value = str(raw).strip()
        match = _EVENT_SEASON_RE.match(value)
        if match:
            start = match.group('start')[-2:]
            end = match.group('end')[-2:]
            return f"{start}{end}"
        if value.isdigit() and len(value) == 4:
            # A four-digit year is a season start; an already-short ``2526``
            # is distinguishable because its halves are consecutive.
            first, second = int(value[:2]), int(value[2:])
            if second == (first + 1) % 100:
                return value
            return canonical_season(value, SeasonFormat.SPLIT_YEAR)
    if transfer_date is None:
        return None
    start_year = (
        transfer_date.year if transfer_date.month >= 7 else transfer_date.year - 1
    )
    return canonical_season(start_year, SeasonFormat.SPLIT_YEAR)


def _stable_transfer_id(
    entry: Dict,
    player_id: str,
    occurrence: int = 0,
) -> str:
    """Return source ID or a deterministic identity hash.

    Mutable display fields (fee, market value, upcoming flag) are excluded so
    an event keeps its ID as the source enriches it.  ``occurrence`` only
    distinguishes truly identical source rows when no source ID is exposed.
    """

    source_id = None
    for key in ('transferId', 'transfer_id', 'id'):
        source_id = entry.get(key)
        if source_id is not None and str(source_id).strip():
            source_id = str(source_id).strip()
            break
    else:
        source_id = None

    from_d = entry.get('from') if isinstance(entry.get('from'), dict) else {}
    to_d = entry.get('to') if isinstance(entry.get('to'), dict) else {}
    identity = {
        # Namespace even source IDs by player: the CEAPI does not document
        # global uniqueness and Gold uses transfer_id as a standalone PK.
        'player_id': str(player_id),
        'source_id': source_id,
    }
    if source_id is None:
        from_id = _extract_club_id_from_href(from_d.get('href'))
        to_id = _extract_club_id_from_href(to_d.get('href'))
        parsed_date = _transfer_date(entry)
        identity.update({
            'event_season': _normalise_event_season(
                entry.get('season'), parsed_date,
            ),
            'transfer_date': parsed_date.isoformat() if parsed_date else None,
            'from_club': from_id or from_d.get('clubName'),
            'to_club': to_id or to_d.get('clubName'),
            'occurrence': occurrence,
        })
    canonical = json.dumps(
        identity, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
    ).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()


def _parse_transfers(payload: dict, player_id: str) -> List[Dict]:
    """Project ``/ceapi/transferHistory/list/{id}`` JSON to flat rows.

    Source row (truncated)::
        {"date": "Sep 1, 2025", "season": "25/26", "upcoming": false,
         "from": {"clubName": "PSG",   "href": "/.../verein/583/saison_id/2025"},
         "to":   {"clubName": "Man City", "href": "/.../verein/281/..."},
         "fee": "€30.00m",
         "marketValue": "€40.00m"}

    Output row mirrors ``bronze.transfermarkt_transfers``.
    """
    if not isinstance(payload, dict):
        return []
    rows: List[Dict] = []
    identity_occurrences: Dict[str, int] = defaultdict(int)
    for entry in payload.get('transfers') or []:
        if not isinstance(entry, dict):
            continue
        transfer_date = _transfer_date(entry)
        event_season = _normalise_event_season(entry.get('season'), transfer_date)
        from_d = entry.get('from') if isinstance(entry.get('from'), dict) else {}
        to_d = entry.get('to') if isinstance(entry.get('to'), dict) else {}
        fee_text = entry.get('fee')

        identity_base = json.dumps({
            'event_season': event_season,
            'transfer_date': transfer_date.isoformat() if transfer_date else None,
            'from': {
                'id': _extract_club_id_from_href(from_d.get('href')),
                'name': from_d.get('clubName'),
            },
            'to': {
                'id': _extract_club_id_from_href(to_d.get('href')),
                'name': to_d.get('clubName'),
            },
        }, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        occurrence = identity_occurrences[identity_base]
        identity_occurrences[identity_base] += 1

        # ``upcoming=True`` events have neither real fee nor finalised date —
        # we still record them so downstream features see pending moves.
        rows.append({
            'transfer_id': _stable_transfer_id(entry, str(player_id), occurrence),
            'player_id': str(player_id),
            'transfer_date': transfer_date,
            'event_season': event_season,
            # Parser-level compatibility only.  Native frames select
            # ``event_season``; the legacy projection sets its partition
            # season from the requested scrape and never overwrites native.
            'season': entry.get('season'),
            'from_club_id': _extract_club_id_from_href(from_d.get('href')),
            'from_club_name': from_d.get('clubName'),
            'to_club_id': _extract_club_id_from_href(to_d.get('href')),
            'to_club_name': to_d.get('clubName'),
            'fee_text': fee_text,
            'fee_eur': _parse_tm_money_eur(fee_text),
            'market_value_eur': _parse_tm_money_eur(entry.get('marketValue')),
            'is_upcoming': bool(entry.get('upcoming', False)),
        })
    return rows


def _source_row_semantic_error(label: str, entry: Dict) -> Optional[str]:
    """Validate facts required to materialise a native career natural key."""

    if label in {'mv_history', 'market_value_points'}:
        if _market_value_date(entry) is None:
            return 'market-value row has no valid date or epoch x'

        raw_value = entry.get('y')
        if isinstance(raw_value, bool) or raw_value is None:
            return 'market-value row y is not numeric'
        try:
            numeric_value = Decimal(str(raw_value).replace(',', '').strip())
        except InvalidOperation:
            return 'market-value row y is not numeric'
        if (
            not numeric_value.is_finite()
            or numeric_value < 0
            or numeric_value != numeric_value.to_integral_value()
        ):
            return 'market-value row y must be a non-negative integer value'
        return None

    if label in {'transfers', 'transfer_events'}:
        upcoming = entry.get('upcoming', False)
        if not isinstance(upcoming, bool):
            return 'transfer upcoming flag is not boolean'
        transfer_date = _transfer_date(entry)
        event_season = _normalise_event_season(
            entry.get('season'), transfer_date,
        )
        if event_season is None:
            return 'transfer has no valid event season/date'
        if transfer_date is None and not upcoming:
            return 'non-upcoming transfer has no valid transfer date'

        def _meaningful_club(side: str) -> bool:
            club = entry.get(side)
            if not isinstance(club, dict):
                return False
            if _extract_club_id_from_href(club.get('href')) is not None:
                return True
            name = str(club.get('clubName') or '').strip()
            return name not in {'', '-', '?'}

        if not (_meaningful_club('from') or _meaningful_club('to')):
            return 'transfer has no meaningful from/to club facts'
        return None

    return f'no semantic validator registered for endpoint {label!r}'


def _career_collection_error(
    label: str,
    player_id: str,
    source_rows,
    parsed_rows,
) -> Optional[str]:
    """Prove a career payload can be safely used for exact-key replacement.

    A partially understood payload is not a partial success.  Every source row
    must produce exactly one parsed row, belong to the requested player, and
    have a unique native natural key.  Otherwise replacing that player's
    existing history could silently delete facts.
    """

    if not isinstance(source_rows, list):
        return 'source collection is not a list'
    if not isinstance(parsed_rows, list):
        return 'parser result is not a list'
    for index, entry in enumerate(source_rows):
        if not isinstance(entry, dict):
            return f'source row {index} is not an object'
        if problem := _source_row_semantic_error(label, entry):
            return f'source row {index}: {problem}'
    if len(parsed_rows) != len(source_rows):
        return (
            f'{len(source_rows)} source rows produced {len(parsed_rows)} '
            'parsed rows'
        )

    key_fields = (
        ('player_id', 'mv_date')
        if label in {'mv_history', 'market_value_points'}
        else ('transfer_id',)
    )
    seen = set()
    for index, row in enumerate(parsed_rows):
        if not isinstance(row, dict):
            return f'parsed row {index} is not an object'
        if str(row.get('player_id')) != str(player_id):
            return f'parsed row {index} has a different player_id'
        key = tuple(row.get(field) for field in key_fields)
        if any(value is None for value in key):
            return f'parsed row {index} has null natural key {key_fields}'
        if key in seen:
            return f'duplicate parsed natural key {key!r}'
        seen.add(key)
    return None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

_METADATA_COLUMNS = ['_source', '_entity_type', '_ingested_at', '_batch_id']
_SCOPE_LINEAGE_COLUMNS = [
    'source_competition_id', 'source_edition_id', 'source_url',
    'source_body_hash', 'fetched_at', 'parser_revision', 'schema_revision',
    'cycle_id', 'scope_id',
]

_DEFAULT_DECODED_BUDGET_MB = {
    'players': 10.0,
    'market_value_history': 4.0,
    'transfers': 8.0,
    'coaches': 6.0,
}
_DEFAULT_REQUEST_ATTEMPT_BUDGET = {
    'players': 26,
    'market_value_history': 120,
    'transfers': 120,
    'coaches': 50,
}

SQUAD_MEMBERSHIP_COLUMNS = [
    'competition_id', 'edition_id', 'league', 'season', 'club_id', 'club_slug',
    'club_name', 'player_id', 'player_slug', 'player_name', 'observed_at',
    *_SCOPE_LINEAGE_COLUMNS,
]
PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS = [
    'player_id', 'player_slug', 'name', 'position', 'dob', 'age', 'height_cm',
    'foot', 'nationality', 'contract_until', 'market_value_eur', 'league',
    'season', 'competition_id', 'edition_id', 'club_id', 'club_name',
    'observed_at', *_SCOPE_LINEAGE_COLUMNS,
]
PLAYER_CONTRACT_OBSERVATION_COLUMNS = [
    'competition_id', 'edition_id', 'team_id', 'team_name', 'player_id',
    'contract_until', 'observed_at', 'applicability_status', 'source_url',
    'source_body_hash', 'fetched_at', 'parser_revision', 'schema_revision',
    'cycle_id', 'scope_id',
]
MARKET_VALUE_POINT_COLUMNS = [
    'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
    *_SCOPE_LINEAGE_COLUMNS,
]
TRANSFER_EVENT_COLUMNS = [
    'transfer_id', 'player_id', 'transfer_date', 'event_season',
    'from_club_id', 'from_club_name', 'to_club_id', 'to_club_name',
    'fee_text', 'fee_eur', 'market_value_eur', 'is_upcoming',
    *_SCOPE_LINEAGE_COLUMNS,
]
COACH_PROFILE_COLUMNS = [
    'coach_id', 'coach_slug', 'name', 'dob', 'nationality',
    *_SCOPE_LINEAGE_COLUMNS,
]
COACH_STINT_COLUMNS = [
    'club_id', 'club_name', 'coach_id', 'coach_slug', 'name', 'role',
    'appointed_date', 'left_date', *_SCOPE_LINEAGE_COLUMNS,
]

LEGACY_PLAYER_COLUMNS = [
    'player_id', 'player_slug', 'name', 'position', 'dob', 'age', 'height_cm',
    'foot', 'nationality', 'contract_until', 'market_value_eur',
    'market_value_last_update', 'current_club_id', 'current_club_name',
    'league', 'season',
]
LEGACY_MV_COLUMNS = [
    'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
    'league', 'season',
]
LEGACY_TRANSFER_COLUMNS = [
    'player_id', 'transfer_date', 'season', 'from_club_id', 'from_club_name',
    'to_club_id', 'to_club_name', 'fee_text', 'fee_eur', 'market_value_eur',
    'is_upcoming', 'league',
]
LEGACY_COACH_COLUMNS = [
    'coach_id', 'coach_slug', 'name', 'role', 'dob', 'nationality',
    'current_club_id', 'current_club_name', 'league', 'season',
]

_NULLABLE_INTEGER_COLUMNS = {
    'age', 'height_cm', 'market_value_eur', 'value_eur', 'fee_eur',
}
_NULLABLE_BOOLEAN_COLUMNS = {'is_upcoming'}


def _apply_nullable_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep null integer/boolean values out of pandas' float coercion."""

    for column in _NULLABLE_INTEGER_COLUMNS.intersection(frame.columns):
        frame[column] = pd.to_numeric(frame[column], errors='coerce').astype('Int64')
    for column in _NULLABLE_BOOLEAN_COLUMNS.intersection(frame.columns):
        frame[column] = frame[column].astype('boolean')
    return frame


def _with_metadata(
    rows: List[Dict],
    columns: List[str],
    *,
    entity_type: str,
    batch_id: str,
    ingested_at: Optional[datetime] = None,
) -> pd.DataFrame:
    """Build a schema-stable DataFrame, including typed empty results."""

    frame = pd.DataFrame(rows).reindex(columns=columns)
    for column in _METADATA_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.Series(dtype='object') if frame.empty else None
    if not frame.empty:
        now = ingested_at or datetime.utcnow()
        frame['_source'] = 'transfermarkt'
        frame['_entity_type'] = entity_type
        frame['_ingested_at'] = now
        frame['_batch_id'] = batch_id
    return _apply_nullable_dtypes(
        frame.reindex(columns=columns + _METADATA_COLUMNS)
    )


def _normalise_requested_season(
    season,
    season_format: SeasonFormat | str,
) -> str:
    value = str(season)
    if (
        len(value) == 4
        and value.isdigit()
        and int(value[2:]) == (int(value[:2]) + 1) % 100
    ):
        return value
    return canonical_season(value, season_format)


def _ensure_metadata(frame: pd.DataFrame, entity_type: str) -> pd.DataFrame:
    """Preserve native ingest metadata while changing the entity projection."""

    out = frame.copy()
    for column in _METADATA_COLUMNS:
        if column not in out.columns:
            out[column] = None
    if not out.empty:
        out['_source'] = out['_source'].fillna('transfermarkt')
        out['_entity_type'] = entity_type
        out['_ingested_at'] = out['_ingested_at'].fillna(datetime.utcnow())
    return out


def materialize_legacy_players(
    memberships: pd.DataFrame,
    attribute_observations: pd.DataFrame,
) -> pd.DataFrame:
    """Pure dual-write adapter to legacy ``transfermarkt_players``.

    Native memberships retain every multi-club relationship.  The transitional
    legacy projection preserves the historical club-player surface for
    rollback/parity; it never collapses a multi-club player globally.
    """

    if attribute_observations is None or attribute_observations.empty:
        return pd.DataFrame(columns=LEGACY_PLAYER_COLUMNS + _METADATA_COLUMNS)
    observations = attribute_observations.copy()
    membership_keys = ['league', 'season', 'club_id', 'player_id']
    if memberships is not None and not memberships.empty:
        valid_memberships = memberships.reindex(
            columns=membership_keys,
        ).drop_duplicates()
        observations = observations.merge(
            valid_memberships,
            on=membership_keys,
            how='inner',
        )
    for column in ['observed_at', 'club_id']:
        if column not in observations.columns:
            observations[column] = None
    observations['_club_sort'] = observations['club_id'].astype(str)
    observations = observations.sort_values(
        ['league', 'season', 'player_id', '_club_sort', 'observed_at'],
        ascending=[True, True, True, True, False],
        na_position='last',
        kind='mergesort',
    ).drop_duplicates(
        ['league', 'season', 'player_id', 'club_id'], keep='first',
    )

    # Age is a fact the source prints; recomputing it "as of now" over pages
    # that may be a day old (the scope cache lives 24h) drifts by a year for any
    # player whose birthday falls in between, and the dual-write parity gate
    # compares the two projections' age. Derive it only when the source omits it.
    today = datetime.utcnow().date()
    ages = []
    for _, row in observations.iterrows():
        age = row.get('age')
        dob = row.get('dob')
        if pd.isna(age) and isinstance(dob, date):
            age = (
                today.year - dob.year
                - ((today.month, today.day) < (dob.month, dob.day))
            )
        ages.append(age)
    observations['age'] = ages
    observations['market_value_last_update'] = None
    observations['current_club_id'] = observations['club_id']
    observations['current_club_name'] = observations['club_name']
    observations = _ensure_metadata(observations, 'players')
    return _apply_nullable_dtypes(
        observations.reindex(columns=LEGACY_PLAYER_COLUMNS + _METADATA_COLUMNS)
    )


def materialize_legacy_market_value_history(
    points: pd.DataFrame,
    league: str,
    season,
    season_format: SeasonFormat | str,
) -> pd.DataFrame:
    """Pure global-point → legacy scrape-partition projection."""

    if points is None or points.empty:
        return pd.DataFrame(columns=LEGACY_MV_COLUMNS + _METADATA_COLUMNS)
    out = points.copy()
    out['league'] = league
    out['season'] = _normalise_requested_season(season, season_format)
    out = _ensure_metadata(out, 'market_value_history')
    return _apply_nullable_dtypes(
        out.reindex(columns=LEGACY_MV_COLUMNS + _METADATA_COLUMNS)
    )


def materialize_legacy_transfers(
    events: pd.DataFrame,
    league: str,
    season,
    season_format: SeasonFormat | str,
) -> pd.DataFrame:
    """Pure global-event → legacy scrape-partition projection.

    The requested partition season is written only to this copy.  Native
    ``event_season`` remains the season supplied by the transfer payload.
    """

    if events is None or events.empty:
        return pd.DataFrame(columns=LEGACY_TRANSFER_COLUMNS + _METADATA_COLUMNS)
    out = events.copy()
    out['league'] = league
    out['season'] = _normalise_requested_season(season, season_format)
    out = _ensure_metadata(out, 'transfers')
    return _apply_nullable_dtypes(
        out.reindex(columns=LEGACY_TRANSFER_COLUMNS + _METADATA_COLUMNS)
    )


def materialize_legacy_coaches(
    profiles: pd.DataFrame,
    stints: pd.DataFrame,
    league: str,
    season,
    season_format: SeasonFormat | str,
) -> pd.DataFrame:
    """Pure global profile/stint → legacy league-season coach projection."""

    if stints is None or stints.empty:
        return pd.DataFrame(columns=LEGACY_COACH_COLUMNS + _METADATA_COLUMNS)
    win_start, win_end = _season_window(int(season), season_format)
    scoped = stints[
        stints.apply(
            lambda row: _stint_overlaps_season(row.to_dict(), win_start, win_end),
            axis=1,
        )
    ].copy()
    if scoped.empty:
        return pd.DataFrame(columns=LEGACY_COACH_COLUMNS + _METADATA_COLUMNS)

    profile_cols = ['coach_id', 'name', 'dob', 'nationality']
    available_profiles = (
        profiles.reindex(columns=profile_cols).drop_duplicates('coach_id')
        if profiles is not None else pd.DataFrame(columns=profile_cols)
    )
    out = scoped.merge(
        available_profiles,
        on='coach_id',
        how='left',
        suffixes=('_stint', '_profile'),
    )
    out['name'] = out['name_profile'].combine_first(out['name_stint'])
    out['current_club_id'] = out['club_id']
    out['current_club_name'] = out['club_name']
    out['league'] = league
    out['season'] = _normalise_requested_season(season, season_format)
    out = out.sort_values(
        ['club_id', 'coach_id', 'appointed_date'],
        ascending=[True, True, False],
        na_position='last',
        kind='mergesort',
    ).drop_duplicates(['club_id', 'coach_id'], keep='first')
    out = _ensure_metadata(out, 'coaches')
    return out.reindex(columns=LEGACY_COACH_COLUMNS + _METADATA_COLUMNS)

class TransfermarktScraper(BaseScraper):
    """Bronze ingest for Transfermarkt.

    Public entry points:
      - ``read_players(league, season, limit)`` → snapshot per-player
      - ``read_market_value_history(league, season, player_ids, limit)``
      - ``read_transfers(league, season, player_ids, limit)``
      - ``scrape_all()`` → writes only ``players`` (transfers + MV history
        depend on a fresh ``bronze.transfermarkt_players``, so the runner
        orchestrates them sequentially).
    """

    SOURCE_NAME = 'transfermarkt'
    # Cloudflare-protected; the bounded production benchmark validates the
    # conservative 12 req/min policy.
    DEFAULT_RATE_LIMIT = 12

    # Public pure projections used by the consolidated dual-write runner.
    materialize_legacy_players = staticmethod(materialize_legacy_players)
    materialize_legacy_coaches = staticmethod(materialize_legacy_coaches)
    materialize_legacy_market_value_history = staticmethod(
        materialize_legacy_market_value_history
    )
    materialize_legacy_transfers = staticmethod(materialize_legacy_transfers)

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs,
    ):
        # BaseScraper validates every proxy eagerly when a file holds >10
        # entries.  Production has ~999 residential endpoints, so doing that
        # for every short-lived entity run wastes hundreds of TCP probes.  Load
        # the pool here without validation; the real request outcome is the
        # authoritative health check.
        proxy_file = kwargs.pop('proxy_file', None)
        proxy = kwargs.pop('proxy', None)
        proxy_control_url = kwargs.pop(
            'proxy_control_url', os.environ.get('TM_PROXY_CONTROL_URL'),
        )
        competition_records = kwargs.pop('competition_records', None)
        traffic_ledger = kwargs.pop('traffic_ledger', None)
        # A league larger than one cycle's byte cap can only be finished across
        # several cycles, and only if the pages already paid for are reused.
        response_cache = kwargs.pop('response_cache', None)
        cache_ttl_seconds = kwargs.pop('cache_ttl_seconds', None)
        canonical_season_override = kwargs.pop('canonical_season', None)
        retry_budget_raw = kwargs.pop(
            'retry_budget', os.environ.get('TM_RETRY_BUDGET'),
        )
        retry_budget = (
            None if retry_budget_raw is None else int(retry_budget_raw)
        )
        if retry_budget is not None and retry_budget < 0:
            raise ValueError('retry_budget must be non-negative')
        lease_metadata = kwargs.pop('lease_metadata', None)
        lease_ttl_seconds = int(kwargs.pop(
            'lease_ttl_seconds', os.environ.get('TM_PROXY_LEASE_TTL_SECONDS', '3600'),
        ))
        requested_rate = kwargs.pop('rate_limit', None)
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            proxy=proxy,
            proxy_file=None,
            rate_limit=None,
            **kwargs,
        )
        require_metered = os.environ.get(
            'TM_REQUIRE_METERED_PROXY', 'false',
        ).strip().lower() in {'1', 'true', 'yes', 'on'}
        if require_metered and not proxy_control_url:
            raise TransfermarktError(
                'TM_REQUIRE_METERED_PROXY requires TM_PROXY_CONTROL_URL; '
                'direct/unmetered fallback is forbidden'
            )
        if proxy_control_url and proxy:
            raise TransfermarktError(
                'proxy lease mode is exclusive with an explicit upstream proxy'
            )
        self.proxy_file = None if proxy_control_url else proxy_file
        if competition_records is None:
            raw_registry = os.environ.get('TM_COMPETITION_RECORDS_JSON', '').strip()
            if raw_registry:
                decoded_registry = json.loads(raw_registry)
                if not isinstance(decoded_registry, list):
                    raise TransfermarktError(
                        'TM_COMPETITION_RECORDS_JSON must be a JSON list'
                    )
                competition_records = tuple(
                    CompetitionRecord.from_mapping(item)
                    for item in decoded_registry
                )
        self._competition_records = (
            tuple(competition_records) if competition_records is not None else None
        )
        if self.proxy_file and os.path.exists(self.proxy_file):
            # Each entity is a short-lived process.  A round-robin pool would
            # restart at proxy 0 every time and repeatedly pay for the same
            # burned prefix; random initial selection distributes runs while
            # the HTTP client remains sticky after success.
            manager = ProxyManager(rotation_strategy='random')
            count = manager.load_from_file_custom_format(self.proxy_file)
            self._proxy_manager = manager
            logger.info(
                "Loaded %d Transfermarkt proxies without eager pre-validation",
                count,
            )

        # Base's unknown-source preset is 10/min with burst=10, and passing a
        # custom rate creates burst=N.  TM must never burst: one initial token,
        # then a steady maximum of 12 requests/minute.
        if requested_rate is None:
            effective_rate = self.DEFAULT_RATE_LIMIT
        else:
            try:
                effective_rate = max(
                    1, min(self.DEFAULT_RATE_LIMIT, int(requested_rate)),
                )
            except (TypeError, ValueError) as exc:
                raise ValueError('rate_limit must be an integer') from exc
        self._rate_limiter = RateLimiter(
            max_requests=effective_rate,
            window_seconds=60,
            burst_size=1,
        )

        lease_provider = (
            ProxyFilterLeaseProvider(proxy_control_url)
            if proxy_control_url else None
        )
        if lease_metadata is None:
            lease_metadata = {
                'dag_id': os.environ.get('TM_DAG_ID', ''),
                'run_id': os.environ.get('TM_RUN_ID', ''),
                'task_id': os.environ.get('TM_TASK_ID', ''),
                'scope': os.environ.get('TM_SCOPE_ID', ''),
            }
        self._canonical_season = str(canonical_season_override or '').strip()
        self._response_cache = response_cache
        self._cache_ttl_seconds = (
            float(cache_ttl_seconds) if cache_ttl_seconds else None
        )
        self._http_client = TransfermarktHttpClient(
            proxy_manager=(None if lease_provider else self._proxy_manager),
            proxy=(None if lease_provider else self.proxy),
            lease_provider=lease_provider,
            traffic_ledger=traffic_ledger,
            retry_budget=retry_budget,
            lease_metadata=lease_metadata,
            lease_ttl_seconds=lease_ttl_seconds,
            rate_limiter=self._rate_limiter,
            cache=response_cache,
            timeout_seconds=12,
            circuit_failures=5,
        )
        self._last_outcome: Optional[FetchOutcome] = None
        self._fetch_records: Dict[str, Dict[str, FetchRecord]] = defaultdict(dict)
        self._scope_capture: Optional[Dict] = None
        self._materialization_failure_streak = 0
        self._materialization_circuit_open = False
        self._environment_decoded_budget_started = False
        self._environment_request_budget_started = False

    @property
    def _last_endpoint_error(self) -> Optional[Dict]:
        """Legacy read-only projection of the most recent typed failure."""

        outcome = self._last_outcome
        if outcome is None or outcome.is_success:
            return None
        return {
            'label': outcome.label,
            'status': outcome.status_code,
            'outcome': outcome.status.value,
            'kind': outcome.status.value,
            'error': outcome.error,
            **dict(outcome.context),
        }

    def _resolve_scope(self, competition: str, edition_id: int | str) -> Dict:
        """Resolve one exact source scope from the central registry."""

        record = resolve_competition(
            competition,
            records=self._competition_records,
        )
        if not record.crawl_eligible:
            raise TransfermarktError(
                f'{record.competition_id}: classification blocks crawl: '
                f'{record.crawl_block_reason}'
            )
        edition = str(edition_id).strip()
        if not edition:
            raise TransfermarktError('edition_id is required')
        # The source offsets some calendar leagues' saison_id from the season it
        # names (saison_id 2023 is the 2024 season), so a registry-planned scope
        # states its season and the edition id is only a fallback.
        canonical = self._canonical_season or canonical_season(
            edition, record.season_format,
        )
        return {
            'record': record,
            'competition_id': record.competition_id,
            'competition_slug': record.slug,
            'edition_id': edition,
            'season_format': record.season_format,
            'canonical_season': canonical,
            'season_year': season_window_year(
                edition, record.season_format, canonical,
            ),
            'compatibility_league': (
                record.canonical_competition_id
                or f'TM-{record.competition_id}'
            ),
            'scope_id': deterministic_scope_id(record.competition_id, edition),
        }

    def _begin_operation_budget(self, operation: str) -> None:
        decoded_env = os.environ.get('TM_DECODED_BODY_BUDGET_MB')
        request_env = os.environ.get('TM_REQUEST_BUDGET')

        if decoded_env is not None:
            # Explicit decoded budget is aggregate across every method called
            # on this scraper (the live benchmark sets 15 MiB once).
            if not self._environment_decoded_budget_started:
                self._http_client.set_decoded_body_budget(
                    int(float(decoded_env) * 1024 * 1024)
                )
                self._environment_decoded_budget_started = True
        else:
            self._http_client.set_decoded_body_budget(
                int(_DEFAULT_DECODED_BUDGET_MB[operation] * 1024 * 1024)
            )

        if request_env is not None:
            # Explicit request budget is likewise aggregate.  If only the byte
            # env is set, request limits remain per-operation instead of being
            # accidentally frozen to the first method's default.
            if not self._environment_request_budget_started:
                self._http_client.begin_request_scope(
                    request_attempt_budget=int(request_env),
                )
                self._environment_request_budget_started = True
        else:
            self._http_client.begin_request_scope(
                request_attempt_budget=_DEFAULT_REQUEST_ATTEMPT_BUDGET[operation],
            )

    def get_traffic_stats(self) -> Dict:
        """Residential-proxy bytes seen this run (#789).

        Lower bound: response-body bytes only (tls_requests gives no transport
        framing), but Transfermarkt is a JSON/HTML fetch (no browser
        sub-resources) so the body dominates the billed bytes. Shape mirrors
        ``FlareSolverrClient.get_traffic_stats`` so ``utils.proxy_traffic`` can
        consume either uniformly.
        """
        stats = self._http_client.get_traffic_stats()
        stats['endpoint_outcome_counts'] = {
            status.value: sum(
                1
                for by_source in self._fetch_records.values()
                for record in by_source.values()
                if record.status == status
            )
            for status in FetchStatus
        }
        if self._materialization_circuit_open:
            stats['circuit_state'] = 'open'
            stats['circuit_breaker_state'] = 'open'
        self._stats['requests'] = stats['request_attempts']
        self._stats['successes'] = stats['successful_attempts']
        self._stats['failures'] = stats['failed_attempts']
        return stats

    def get_fetch_outcomes(self) -> Dict[str, Dict[str, Dict]]:
        """Checkpoint records keyed by endpoint then stable source ID."""

        return {
            endpoint: {
                source_id: record.as_dict()
                for source_id, record in sorted(records.items())
            }
            for endpoint, records in sorted(self._fetch_records.items())
        }

    def get_fetch_outcomes_envelope(self) -> Dict:
        return {
            'schema_version': os.environ.get('TM_SCHEMA_VERSION', '2'),
            'outcomes': self.get_fetch_outcomes(),
        }

    def get_scope_capture(self) -> Optional[Dict]:
        """Return listing/squad participant evidence for the exact scope."""

        if self._scope_capture is None:
            return None
        return json.loads(json.dumps(self._scope_capture, sort_keys=True))

    # -- HTTP plumbing (mirrors scrapers/sofascore/scraper.py:279 contract) --

    @staticmethod
    def _source_id(context: Optional[Dict]) -> str:
        context = context or {}
        for key in ('player_id', 'coach_id', 'club_id'):
            if context.get(key) is not None:
                return str(context[key])
        if context.get('league') is not None:
            return f"{context['league']}:{context.get('season', '')}"
        return 'global'

    def _store_fetch_outcome(
        self,
        outcome: FetchOutcome,
        *,
        row_count: int = 0,
    ) -> None:
        self._last_outcome = outcome
        source_id = self._source_id(dict(outcome.context))
        self._fetch_records[outcome.label][source_id] = FetchRecord(
            status=outcome.status,
            row_count=int(row_count),
            payload_hash=outcome.payload_hash,
            error=outcome.error,
            status_code=outcome.status_code,
            attempts=outcome.attempts,
        )

    def _record_materialized_rows(
        self,
        label: str,
        context: Dict,
        row_count: int,
    ) -> None:
        source_id = self._source_id(context)
        record = self._fetch_records.get(label, {}).get(source_id)
        if record is not None:
            self._fetch_records[label][source_id] = FetchRecord(
                status=record.status,
                row_count=int(row_count),
                payload_hash=record.payload_hash,
                error=record.error,
                status_code=record.status_code,
                attempts=record.attempts,
            )
            if record.status in (FetchStatus.OK, FetchStatus.VALID_EMPTY):
                self._materialization_failure_streak = 0

    def _mark_authoritative_empty(self, label: str, context: Dict) -> None:
        """Mark a schema-valid source collection that is itself empty."""

        source_id = self._source_id(context)
        record = self._fetch_records.get(label, {}).get(source_id)
        if record is not None:
            self._fetch_records[label][source_id] = FetchRecord(
                status=FetchStatus.VALID_EMPTY,
                row_count=0,
                payload_hash=record.payload_hash,
                error=None,
                status_code=record.status_code,
                attempts=record.attempts,
            )
        self._materialization_failure_streak = 0

    def _mark_schema_error(
        self,
        label: str,
        context: Dict,
        error: str,
    ) -> None:
        """Convert structurally valid HTTP into an authoritative parse failure."""

        source_id = self._source_id(context)
        record = self._fetch_records.get(label, {}).get(source_id)
        if record is None:
            record = FetchRecord(
                status=FetchStatus.SCHEMA_ERROR,
                row_count=0,
                payload_hash=None,
                error=error,
                status_code=200,
                attempts=0,
            )
        else:
            record = FetchRecord(
                status=FetchStatus.SCHEMA_ERROR,
                row_count=0,
                payload_hash=record.payload_hash,
                error=error,
                status_code=record.status_code,
                attempts=record.attempts,
            )
        self._fetch_records[label][source_id] = record
        self._materialization_failure_streak += 1
        if self._materialization_failure_streak >= 5:
            self._materialization_circuit_open = True
            self._http_client.close()
        last = self._last_outcome
        if (
            last is not None
            and last.label == label
            and self._source_id(dict(last.context)) == source_id
        ):
            self._last_outcome = last.with_status(
                FetchStatus.SCHEMA_ERROR,
                error=error,
            )
        else:
            self._last_outcome = FetchOutcome(
                status=FetchStatus.SCHEMA_ERROR,
                status_code=record.status_code,
                error=error,
                attempts=record.attempts,
                label=label,
                context=context,
                payload_hash=record.payload_hash,
            )

    @staticmethod
    def _endpoint_validator(label: str, as_json: bool):
        if as_json:
            required = {
                'mv_history': 'list',
                'market_value_points': 'list',
                'transfers': 'transfers',
                'transfer_events': 'transfers',
            }.get(label)
            if required is None:
                return None

            def _validate_json(payload):
                value = payload.get(required) if isinstance(payload, dict) else None
                if not isinstance(value, list):
                    return f"expected list field {required!r}"
                for index, entry in enumerate(value):
                    if not isinstance(entry, dict):
                        return f"{required}[{index}] is not an object"
                    if required == 'transfers':
                        for club_side in ('from', 'to'):
                            club = entry.get(club_side)
                            if club is not None and not isinstance(club, dict):
                                return (
                                    f"transfers[{index}].{club_side} is not an object"
                                )
                    if problem := _source_row_semantic_error(label, entry):
                        return f'{required}[{index}]: {problem}'
                return None

            return _validate_json

        if label not in {'listing', 'squad', 'coach_history', 'coach_profile'}:
            return None

        def _validate_html(html):
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, 'html.parser')
            if label == 'coach_profile':
                if soup.find('h1', {'class': 'data-header__headline-wrapper'}) is None:
                    return 'missing coach profile headline'
            elif soup.find('table', {'class': 'items'}) is None:
                return 'missing table.items'
            elif label == 'listing' and soup.find('a', href=_CLUB_HREF_RE) is None:
                return 'listing has no club links'
            elif label == 'squad' and soup.find('a', href=_PLAYER_HREF_RE) is None:
                return 'squad has no player links'
            elif label == 'coach_history' and not _parse_coach_history(
                html, club_id='validator',
            ):
                return 'coach history has no header-mapped dated trainer rows'
            return None

        return _validate_html

    def _fetch_endpoint_outcome(
        self,
        url: str,
        as_json: bool,
        max_attempts: int = 6,
        label: str = 'endpoint',
        context: Optional[Dict] = None,
    ) -> FetchOutcome:
        if self._materialization_circuit_open:
            outcome = FetchOutcome(
                status=FetchStatus.RETRY_EXHAUSTED,
                error='in-run parser circuit is open after five schema failures',
                label=label,
                context=context or {},
            )
            self._store_fetch_outcome(outcome)
            return outcome
        outcome = self._http_client.fetch(
            url,
            as_json=as_json,
            max_attempts=max_attempts,
            label=label,
            context=context,
            validator=self._endpoint_validator(label, as_json),
            cache_key=url if self._response_cache is not None else None,
            cache_ttl_seconds=self._cache_ttl_seconds,
        )
        self._store_fetch_outcome(outcome)
        return outcome

    def _fetch_endpoint(
        self,
        url: str,
        as_json: bool,
        max_attempts: int = 6,
        label: str = 'endpoint',
        context: Optional[Dict] = None,
    ):
        """Legacy payload/``None`` adapter over the typed fetch contract."""

        outcome = self._fetch_endpoint_outcome(
            url,
            as_json=as_json,
            max_attempts=max_attempts,
            label=label,
            context=context,
        )
        return outcome.value if outcome.status == FetchStatus.OK else None

    def _fetch_html(self, url: str, label: str = 'html', context=None) -> Optional[str]:
        return self._fetch_endpoint(url, as_json=False, label=label, context=context)

    def _fetch_json(self, url: str, label: str = 'json', context=None) -> Optional[dict]:
        return self._fetch_endpoint(url, as_json=True, label=label, context=context)

    def close(self) -> None:
        self._http_client.close()
        super().close()

    # ---------------------- bronze resolver ----------------------------------

    def _bronze_connection(self):
        """Trino DB-API connection for bronze lookups (env-driven auth)."""
        import os

        import trino
        import trino.auth as trino_auth

        user = os.environ.get('TRINO_USER', 'airflow')
        password = os.environ.get('TRINO_PASSWORD')

        if password:
            return trino.dbapi.connect(
                host=os.environ.get('TRINO_HOST', 'trino'),
                port=int(os.environ.get('TRINO_PORT', 8443)),
                user=user,
                catalog='iceberg',
                http_scheme='https',
                auth=trino_auth.BasicAuthentication(user, password),
                verify=False,
            )
        return trino.dbapi.connect(
            host=os.environ.get('TRINO_HOST', 'trino'),
            port=int(os.environ.get('TRINO_PORT', 8080)),
            user=user,
            catalog='iceberg',
        )

    def _resolve_player_ids_from_bronze(
        self,
        league: str,
        season_short: str,
        limit: Optional[int] = None,
        window_offset: int = 0,
    ) -> List[str]:
        """DISTINCT player_id from native memberships, with rollout fallback.

        Mirrors ``scrapers/sofascore/scraper.py:_resolve_player_ids_from_bronze``
        — the dependent entities (``transfers``, ``mv_history``) need a fresh
        roster from the ``players`` entity before they can fan out per-player.

        ``player_id`` is a varchar Transfermarkt id, so a SQL ``ORDER BY
        player_id`` sorts lexicographically and the same ~100 ids on '1'/'2'
        win every run — ids on 3-9 are never scraped (#620). We instead sort
        the full roster numerically in Python (a ``SELECT DISTINCT`` cannot
        ``ORDER BY`` a cast expression that is not in the select list) and,
        when ``limit`` is set, return a *rotating window*: run ``window_offset``
        scrapes the next contiguous block of ``limit`` players, wrapping
        around, so the whole roster is covered over ``ceil(n/limit)`` runs and
        mv_history + transfers sample the SAME window.
        """
        native_sql = (
            "SELECT DISTINCT player_id "
            "FROM iceberg.bronze.transfermarkt_squad_memberships "
            "WHERE league = ? AND season = ?"
        )
        legacy_sql = (
            "SELECT DISTINCT player_id "
            "FROM iceberg.bronze.transfermarkt_players "
            "WHERE league = ? AND season = ?"
        )

        def _query(sql: str):
            conn = self._bronze_connection()
            cur = conn.cursor()
            cur.execute(sql, (league, season_short))
            return cur.fetchall()

        try:
            rows = _query(native_sql)
        except Exception as exc:
            rendered = str(exc).upper()
            if not (
                'TABLE_NOT_FOUND' in rendered
                or 'DOES NOT EXIST' in rendered
                or 'NOT FOUND' in rendered
            ):
                raise TransfermarktError(
                    "native Transfermarkt roster lookup failed; refusing "
                    f"legacy fallback: {exc}"
                ) from exc
            try:
                rows = _query(legacy_sql)
            except Exception as legacy_exc:
                raise TransfermarktError(
                    f"legacy Transfermarkt roster lookup failed: {legacy_exc}"
                ) from legacy_exc
        roster = [str(row[0]) for row in rows if row and row[0]]

        # Numeric order (non-numeric ids — none expected — sort last, stably).
        roster.sort(key=lambda p: (0, int(p)) if p.isdigit() else (1, p))
        if not limit or len(roster) <= limit:
            return roster
        n = len(roster)
        start = (int(window_offset) * int(limit)) % n
        return (roster + roster)[start:start + int(limit)]

    def _resolve_coach_bios_from_bronze(self) -> Dict[str, Dict]:
        """``coach_id → {name, dob, nationality}`` from bronze, all seasons.

        Coach bios are immutable, so a profile materialised by ANY earlier
        run can be reused instead of re-fetching ~20-40 profile pages every
        weekly run.  Native profile cache is queried first.  Only an explicit
        TABLE_NOT_FOUND falls back to the transitional legacy table; other
        database failures abort rather than triggering a costly mass refetch.
        """
        def _rows(sql: str):
            conn = self._bronze_connection()
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()

        native_sql = (
            "SELECT coach_id, max(name), max(dob), max(nationality) "
            "FROM iceberg.bronze.transfermarkt_coach_profiles "
            "WHERE dob IS NOT NULL OR nationality IS NOT NULL "
            "GROUP BY coach_id"
        )
        legacy_sql = (
            "SELECT coach_id, max(name), max(dob), max(nationality) "
            "FROM iceberg.bronze.transfermarkt_coaches "
            "WHERE dob IS NOT NULL OR nationality IS NOT NULL "
            "GROUP BY coach_id"
        )
        try:
            rows = _rows(native_sql)
        except Exception as exc:
            rendered = str(exc).upper()
            missing = (
                'TABLE_NOT_FOUND' in rendered
                or 'DOES NOT EXIST' in rendered
                or 'NOT FOUND' in rendered
            )
            if not missing:
                raise TransfermarktError(
                    f"native coach-profile cache lookup failed: {exc}"
                ) from exc
            logger.info("Native coach profile table absent; trying legacy cache")
            try:
                rows = _rows(legacy_sql)
            except Exception as legacy_exc:
                legacy_rendered = str(legacy_exc).upper()
                if (
                    'TABLE_NOT_FOUND' in legacy_rendered
                    or 'DOES NOT EXIST' in legacy_rendered
                    or 'NOT FOUND' in legacy_rendered
                ):
                    return {}
                raise TransfermarktError(
                    f"legacy coach-profile cache lookup failed: {legacy_exc}"
                ) from legacy_exc
        return {
            str(row[0]): {
                'name': row[1], 'dob': row[2], 'nationality': row[3],
            }
            for row in rows
            if row and row[0]
        }

    # ---------------------- read_* entry points ------------------------------

    def read_squad_data(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Fetch one league-season squad once and return native + legacy views.

        Native natural keys are ``(league, season, club_id, player_id)`` for
        ``memberships`` and ``(league, season, club_id, player_id,
        observed_at)`` for ``attribute_observations``.  Multi-club players keep
        every membership.  ``legacy_players`` is a pure transitional
        projection, so dual-write never repeats HTTP.
        """
        scope = self._resolve_scope(league, season)
        competition = scope['record']
        league = scope['compatibility_league']
        season_short = scope['canonical_season']
        self._begin_operation_budget('players')
        listing_url = _competition_listing_url(
            competition, scope['edition_id'],
        )
        self._scope_capture = {
            'schema_version': 1,
            'scope_id': scope['scope_id'],
            'competition_id': scope['competition_id'],
            'edition_id': scope['edition_id'],
            'competition_type': competition.competition_type.value,
            'gender': competition.gender.value,
            'team_type': competition.team_type.value,
            'age_category': competition.age_category.value,
            'listing_status': 'pending',
            'listing_source_url': listing_url,
            'listing_source_body_hash': None,
            'expected_team_ids': [],
            'observed_team_ids': [],
            'endpoint_status_by_team': {},
            'fetched_at': datetime.now(timezone.utc).isoformat(),
        }

        def _empty_bundle() -> Dict[str, pd.DataFrame]:
            memberships = _with_metadata(
                [], SQUAD_MEMBERSHIP_COLUMNS,
                entity_type='squad_memberships', batch_id=self._batch_id,
            )
            observations = _with_metadata(
                [], PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS,
                entity_type='player_attribute_observations',
                batch_id=self._batch_id,
            )
            contracts = _with_metadata(
                [], PLAYER_CONTRACT_OBSERVATION_COLUMNS,
                entity_type='player_contract_observations',
                batch_id=self._batch_id,
            )
            contracts.attrs['fetch_status'] = (
                'not_applicable'
                if competition.team_type.value == 'national_team'
                else 'retry_exhausted'
            )
            return {
                'memberships': memberships,
                'attribute_observations': observations,
                'contract_observations': contracts,
                'legacy_players': materialize_legacy_players(
                    memberships, observations,
                ),
            }

        # Step 1 — exact discovered competition/edition listing → teams.
        listing_html = self._fetch_html(
            listing_url,
            label='listing',
            context={
                'league': league,
                'season': season,
                'competition_id': scope['competition_id'],
                'edition_id': scope['edition_id'],
                'scope': scope['scope_id'],
            },
        )
        if listing_html is None:
            listing_record = self._fetch_records.get('listing', {}).get(
                f'{league}:{season}',
            )
            self._scope_capture['listing_status'] = (
                listing_record.status.value
                if listing_record is not None else 'retry_exhausted'
            )
            self._scope_capture['fetched_at'] = (
                datetime.now(timezone.utc).isoformat()
            )
            logger.error(
                "%s: league listing fetch failed for %s/%s.",
                R0_2B_FALLBACK_MARKER, league, season,
            )
            return _empty_bundle()

        clubs = _parse_club_listing(listing_html)
        self._scope_capture['listing_source_body_hash'] = (
            self._last_outcome.payload_hash
            if self._last_outcome is not None
            else competition.source_body_hash
        )
        self._record_materialized_rows(
            'listing', {'league': league, 'season': season}, len(clubs),
        )
        if not clubs:
            self._scope_capture['listing_status'] = 'schema_error'
            self._scope_capture['fetched_at'] = (
                datetime.now(timezone.utc).isoformat()
            )
            self._mark_schema_error(
                'listing',
                {'league': league, 'season': season},
                'listing table produced zero clubs; selector/layout drift',
            )
            logger.error(
                "%s: listing parsed zero clubs for %s/%s",
                R0_2B_FALLBACK_MARKER, league, season,
            )
            return _empty_bundle()
        expected_team_ids = [str(club['club_id']) for club in clubs]
        self._scope_capture.update({
            'listing_status': 'ok',
            'expected_team_ids': expected_team_ids,
            'endpoint_status_by_team': {
                team_id: 'not_attempted' for team_id in expected_team_ids
            },
        })
        logger.info(
            "TM listing: %d clubs for league=%s season=%s",
            len(clubs), league, season,
        )

        # Step 2 — per-club detailed squad page → full player rows. The
        # `/plus/1` table carries the whole bio, so this is the LAST HTTP
        # hop: the former per-player profile loop (~530 req / ~58 MB
        # residential proxy per weekly run) is gone.
        squad_players: List[Dict] = []
        squad_successes = 0
        clubs_attempted = 0
        for club in clubs:
            clubs_attempted += 1
            squad_url = (
                f"{_TM_BASE}"
                + _CLUB_SQUAD_PATH.format(
                    club_slug=club['club_slug'],
                    club_id=club['club_id'],
                    year=scope['edition_id'],
                )
            )
            html = self._fetch_html(
                squad_url,
                label='squad',
                context={
                    'club_id': club['club_id'],
                    'competition_id': scope['competition_id'],
                    'edition_id': scope['edition_id'],
                    'scope': scope['scope_id'],
                },
            )
            if html is None:
                record = self._fetch_records.get('squad', {}).get(
                    str(club['club_id']),
                )
                self._scope_capture['endpoint_status_by_team'][
                    str(club['club_id'])
                ] = (
                    record.status.value
                    if record is not None else 'retry_exhausted'
                )
                continue
            parsed_players = _parse_squad_page(html, club_id=club['club_id'])
            self._record_materialized_rows(
                'squad', {'club_id': club['club_id']}, len(parsed_players),
            )
            if not parsed_players:
                self._scope_capture['endpoint_status_by_team'][
                    str(club['club_id'])
                ] = 'schema_error'
                self._mark_schema_error(
                    'squad', {'club_id': club['club_id']},
                    'squad table produced zero players; selector/layout drift',
                )
                continue
            squad_successes += 1
            self._scope_capture['endpoint_status_by_team'][
                str(club['club_id'])
            ] = 'ok'
            payload_hash = (
                self._last_outcome.payload_hash
                if self._last_outcome is not None else None
            )
            for p in parsed_players:
                p['current_club_name'] = club['club_name']
                p['club_slug'] = club['club_slug']
                p['_source_url'] = squad_url
                p['_source_body_hash'] = payload_hash
                squad_players.append(p)
            # Smoke/dry-run limits must stop paid traversal as soon as enough
            # rows exist, rather than downloading every remaining club page.
            if limit and len(squad_players) >= int(limit):
                break

        self._scope_capture['observed_team_ids'] = [
            team_id for team_id in expected_team_ids
            if self._scope_capture['endpoint_status_by_team'][team_id] == 'ok'
        ]
        self._scope_capture['fetched_at'] = datetime.now(timezone.utc).isoformat()

        if not squad_players:
            logger.warning(
                "%s: zero players harvested across %d clubs.",
                R0_2B_FALLBACK_MARKER, len(clubs),
            )
            return _empty_bundle()

        # A partial squad sweep would be saved with replace_partitions and
        # shrink the bronze partition — abort instead (#484 semantics, moved
        # from the former profile loop to the squad loop).
        if squad_successes < _MIN_SUCCESS_RATIO * clubs_attempted:
            raise PartialScrapeError(
                f"only {squad_successes}/{clubs_attempted} squad pages fetched "
                f"(< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect "
                f"existing partition"
            )

        if limit:
            squad_players = squad_players[: int(limit)]

        logger.info("TM squad pages → %d players", len(squad_players))

        observed_at = datetime.utcnow()
        membership_rows: List[Dict] = []
        observation_rows: List[Dict] = []
        contract_rows: List[Dict] = []
        for sp in squad_players:
            lineage = {
                'source_competition_id': scope['competition_id'],
                'source_edition_id': scope['edition_id'],
                'source_url': sp.get('_source_url') or competition.source_url,
                'source_body_hash': (
                    sp.get('_source_body_hash') or competition.source_body_hash
                ),
                'fetched_at': observed_at,
                'parser_revision': PARSER_REVISION,
                'schema_revision': SCHEMA_REVISION,
                'cycle_id': os.environ.get('TM_RUN_ID', self._batch_id),
                'scope_id': scope['scope_id'],
            }
            membership_rows.append({
                'competition_id': scope['competition_id'],
                'edition_id': scope['edition_id'],
                'league': league,
                'season': season_short,
                'club_id': str(sp['club_id']),
                'club_slug': sp.get('club_slug'),
                'club_name': sp.get('current_club_name'),
                'player_id': str(sp['player_id']),
                'player_slug': sp.get('player_slug'),
                'player_name': sp.get('name'),
                'observed_at': observed_at,
                **lineage,
            })
            observation_rows.append({
                'player_id': sp['player_id'],
                'player_slug': sp['player_slug'],
                'name': sp['name'],
                'position': sp.get('position'),
                'dob': sp.get('dob'),
                'age': sp.get('age'),
                'height_cm': sp.get('height_cm'),
                'foot': sp.get('foot'),
                'nationality': sp.get('nationality'),
                'contract_until': sp.get('contract_until'),
                'market_value_eur': sp.get('market_value_eur'),
                'league': league,
                'season': season_short,
                'competition_id': scope['competition_id'],
                'edition_id': scope['edition_id'],
                'club_id': str(sp['club_id']),
                'club_name': sp.get('current_club_name'),
                'observed_at': observed_at,
                **lineage,
            })
            if competition.team_type.value == 'club':
                contract_rows.append({
                    'competition_id': scope['competition_id'],
                    'edition_id': scope['edition_id'],
                    'team_id': str(sp['club_id']),
                    'team_name': sp.get('current_club_name'),
                    'player_id': str(sp['player_id']),
                    'contract_until': sp.get('contract_until'),
                    'observed_at': observed_at,
                    'applicability_status': 'ok',
                    'source_url': lineage['source_url'],
                    'source_body_hash': lineage['source_body_hash'],
                    'fetched_at': observed_at,
                    'parser_revision': PARSER_REVISION,
                    'schema_revision': SCHEMA_REVISION,
                    'cycle_id': lineage['cycle_id'],
                    'scope_id': scope['scope_id'],
                })
        memberships = _with_metadata(
            membership_rows, SQUAD_MEMBERSHIP_COLUMNS,
            entity_type='squad_memberships', batch_id=self._batch_id,
            ingested_at=observed_at,
        ).drop_duplicates([
            'competition_id', 'edition_id', 'club_id', 'player_id',
        ])
        observations = _with_metadata(
            observation_rows, PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS,
            entity_type='player_attribute_observations', batch_id=self._batch_id,
            ingested_at=observed_at,
        ).drop_duplicates(
            [
                'competition_id', 'edition_id', 'club_id', 'player_id',
                'observed_at',
            ]
        )
        contracts = _with_metadata(
            contract_rows, PLAYER_CONTRACT_OBSERVATION_COLUMNS,
            entity_type='player_contract_observations', batch_id=self._batch_id,
            ingested_at=observed_at,
        ).drop_duplicates([
            'competition_id', 'edition_id', 'team_id', 'player_id',
            'observed_at',
        ])
        contracts.attrs['fetch_status'] = (
            'ok' if competition.team_type.value == 'club' else 'not_applicable'
        )
        legacy = materialize_legacy_players(memberships, observations)

        logger.info(
            "Materialised %d transfermarkt players (league=%s season=%s)",
            len(memberships), league, season,
        )
        return {
            'memberships': memberships,
            'attribute_observations': observations,
            'contract_observations': contracts,
            'legacy_players': legacy,
        }

    def read_squad_memberships(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        squad_data: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> pd.DataFrame:
        """Return native membership grain without a second fetch when bundled."""

        data = squad_data or self.read_squad_data(league, season, limit)
        return data['memberships']

    def read_player_attribute_observations(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        squad_data: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> pd.DataFrame:
        data = squad_data or self.read_squad_data(league, season, limit)
        return data['attribute_observations']

    def read_player_contract_observations(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        squad_data: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> pd.DataFrame:
        """Return contract facts from the same paid squad responses."""

        data = squad_data or self.read_squad_data(league, season, limit)
        return data['contract_observations']

    def read_players(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Legacy adapter; one squad fetch, then a pure one-player projection."""

        return self.read_squad_data(league, season, limit)['legacy_players']

    def read_coach_data(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        *,
        clubs: Optional[List[Dict]] = None,
        memberships: Optional[pd.DataFrame] = None,
        coach_profile_cache: Optional[Mapping[str, Mapping]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Return global coach profiles/stints plus one legacy projection.

        Natural keys are ``coach_id`` for profiles and
        ``(club_id, coach_id, appointed_date, left_date)`` for stints.  Passing
        the native squad ``memberships`` reuses club ids/slugs and eliminates a
        duplicate league-listing request.  ``coach_profile_cache`` is an
        optional public injection point: ``None`` resolves persisted Bronze,
        while an explicit mapping (including ``{}`` for a cold-cache GET-only
        benchmark) performs no Trino cache lookup.
        """
        scope = self._resolve_scope(league, season)
        competition = scope['record']
        league = scope['compatibility_league']
        # A stint is dated, so the window it must overlap is the season the
        # registry states — never the saison_id, which the source offsets from
        # it for calendar leagues.
        season = scope['season_year']
        self._begin_operation_budget('coaches')

        def _empty_bundle() -> Dict[str, pd.DataFrame]:
            profiles = _with_metadata(
                [], COACH_PROFILE_COLUMNS,
                entity_type='coach_profiles', batch_id=self._batch_id,
            )
            stints = _with_metadata(
                [], COACH_STINT_COLUMNS,
                entity_type='coach_stints', batch_id=self._batch_id,
            )
            return {
                'profiles': profiles,
                'stints': stints,
                'legacy_coaches': materialize_legacy_coaches(
                    profiles, stints, league, season,
                    scope['season_format'],
                ),
            }

        # Step 1 — use the already-fetched squad membership dimension when
        # available.  Only the standalone legacy call needs a league listing.
        if clubs is None and memberships is not None and not memberships.empty:
            club_columns = ['club_id', 'club_slug', 'club_name']
            reusable = memberships.reindex(columns=club_columns).drop_duplicates('club_id')
            clubs = [
                {
                    'club_id': str(row['club_id']),
                    'club_slug': row['club_slug'],
                    'club_name': row['club_name'],
                }
                for _, row in reusable.iterrows()
                if pd.notna(row['club_id']) and pd.notna(row['club_slug'])
            ]
        if clubs is None:
            listing_url = _competition_listing_url(
                competition, scope['edition_id'],
            )
            listing_html = self._fetch_html(
                listing_url, label='listing',
                context={
                    'league': league,
                    'season': season,
                    'competition_id': scope['competition_id'],
                    'edition_id': scope['edition_id'],
                    'scope': scope['scope_id'],
                },
            )
            if listing_html is None:
                logger.error(
                    "%s: league listing fetch failed for coaches %s/%s.",
                    R0_2B_FALLBACK_MARKER, league, season,
                )
                return _empty_bundle()
            clubs = _parse_club_listing(listing_html)
            self._record_materialized_rows(
                'listing', {'league': league, 'season': season}, len(clubs),
            )
            if not clubs:
                self._mark_schema_error(
                    'listing', {'league': league, 'season': season},
                    'listing table produced zero clubs; selector/layout drift',
                )
                return _empty_bundle()
        else:
            clubs = list(clubs)
        if limit:
            clubs = clubs[: int(limit)]
        logger.info("TM coaches: %d clubs for %s/%s", len(clubs), league, season)

        # Step 2 — per-club trainer-history page → every manager whose tenure
        # overlaps the season (issue #619). The old staff snapshot kept only the
        # end-of-season head coach, missing mid-season replacements and
        # caretakers; the history page lists them all with appointed/left dates.
        # Dedup coach_id per club so Step 3 fetches each bio once.
        win_start, win_end = _season_window(
            season, scope['season_format'],
        )
        managers: List[Dict] = []
        stint_rows: List[Dict] = []
        history_successes = 0
        for club in clubs:
            history_url = (
                f"{_TM_BASE}"
                + _CLUB_COACH_HISTORY_PATH.format(
                    club_slug=club['club_slug'],
                    club_id=club['club_id'],
                )
            )
            html = self._fetch_html(
                history_url, label='coach_history',
                context={'club_id': club['club_id']},
            )
            if html is None:
                continue
            parsed_stints = _parse_coach_history(html, club_id=club['club_id'])
            history_hash = (
                self._last_outcome.payload_hash
                if self._last_outcome is not None else None
            )
            history_fetched_at = datetime.utcnow()
            self._record_materialized_rows(
                'coach_history', {'club_id': club['club_id']}, len(parsed_stints),
            )
            if not parsed_stints:
                self._mark_schema_error(
                    'coach_history', {'club_id': club['club_id']},
                    'coach-history table produced zero dated stints',
                )
                continue
            history_successes += 1
            seen_coach: set = set()
            for stint in parsed_stints:
                stint_rows.append({
                    **stint,
                    'club_name': club.get('club_name'),
                    'source_competition_id': scope['competition_id'],
                    'source_edition_id': scope['edition_id'],
                    'source_url': history_url,
                    'source_body_hash': history_hash,
                    'fetched_at': history_fetched_at,
                    'parser_revision': PARSER_REVISION,
                    'schema_revision': SCHEMA_REVISION,
                    'cycle_id': os.environ.get('TM_RUN_ID', self._batch_id),
                    'scope_id': scope['scope_id'],
                })
                if not _stint_overlaps_season(stint, win_start, win_end):
                    continue
                if stint['coach_id'] in seen_coach:
                    continue
                seen_coach.add(stint['coach_id'])
                managers.append({
                    'coach_id': stint['coach_id'],
                    'coach_slug': stint['coach_slug'],
                    'name': stint['name'],
                    'role': stint['role'],
                    'club_id': stint['club_id'],
                    'current_club_name': club['club_name'],
                    # The club's history page is what proves this coach worked
                    # here; a profile fetch, when it succeeds, supersedes it.
                    '_source_url': history_url,
                    '_source_body_hash': history_hash,
                })

        if not managers:
            logger.warning(
                "%s: zero coaches harvested across %d clubs.",
                R0_2B_FALLBACK_MARKER, len(clubs),
            )
            # A valid page can have no season-overlapping coach while still
            # yielding authoritative historical stints.
            if not stint_rows:
                return _empty_bundle()

        if clubs and history_successes < _MIN_SUCCESS_RATIO * len(clubs):
            raise PartialScrapeError(
                f"only {history_successes}/{len(clubs)} coach-history pages "
                f"fetched (< {_MIN_SUCCESS_RATIO:.0%})"
            )

        logger.info(
            "TM trainer-history → %d in-season coaches to enrich", len(managers)
        )

        # Step 3 — per-coach profile page → dob + nationality. Coach bios
        # are immutable, so profiles already materialised in bronze (any
        # season) are reused instead of re-fetched — a typical weekly run
        # downloads only genuinely new appointments (proxy-traffic fix).
        if coach_profile_cache is None:
            known_bios = self._resolve_coach_bios_from_bronze()
        else:
            # An identity row carries no bio, so it is not one to reuse.
            known_bios = {
                str(coach_id): dict(profile)
                for coach_id, profile in coach_profile_cache.items()
                if profile.get('dob') is not None
                or profile.get('nationality') is not None
            }
        resolved_bios: Dict[str, Dict] = dict(known_bios)
        unique_managers = list({
            str(manager['coach_id']): manager for manager in managers
        }.values())
        profile_rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        attempted = 0
        reused = 0
        for idx, mgr in enumerate(unique_managers, start=1):
            bio = resolved_bios.get(mgr['coach_id'])
            if bio is not None:
                reused += 1
            else:
                attempted += 1
                profile_url = (
                    f"{_TM_BASE}"
                    + _COACH_PROFILE_PATH.format(
                        coach_slug=mgr['coach_slug'], coach_id=mgr['coach_id'],
                    )
                )
                payload = self._fetch_html(
                    profile_url, label='coach_profile',
                    context={'coach_id': mgr['coach_id']},
                )
                if payload is None:
                    consecutive_failures += 1
                    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                        raise ConsecutiveFailureError(
                            f"{consecutive_failures} consecutive coach-profile "
                            f"failures at {idx}/{len(unique_managers)} — aborting to "
                            f"protect existing partition"
                        )
                    # His history page named him, so he existed here whether or
                    # not his bio page answered.  The row states the identity
                    # and leaves the bio empty; ``_resolve_coach_bios_from_bronze``
                    # reads no bio out of it, so the next cycle refetches.
                    bio = {}
                else:
                    consecutive_failures = 0
                    successes += 1
                    bio = _parse_coach_profile(payload, mgr['coach_id']) or {}
                    mgr['_source_url'] = profile_url
                    mgr['_source_body_hash'] = (
                        self._last_outcome.payload_hash
                        if self._last_outcome is not None else None
                    )
                    self._record_materialized_rows(
                        'coach_profile', {'coach_id': mgr['coach_id']},
                        1 if bio else 0,
                    )
                resolved_bios[mgr['coach_id']] = bio
            profile_rows.append({
                'coach_id': mgr['coach_id'],
                'coach_slug': mgr['coach_slug'],
                'name': bio.get('name') or mgr['name'],
                'dob': bio.get('dob'),
                'nationality': bio.get('nationality'),
                'source_competition_id': scope['competition_id'],
                'source_edition_id': scope['edition_id'],
                'source_url': (
                    mgr.get('_source_url') or competition.source_url
                ),
                'source_body_hash': (
                    mgr.get('_source_body_hash') or competition.source_body_hash
                ),
                'fetched_at': datetime.utcnow(),
                'parser_revision': PARSER_REVISION,
                'schema_revision': SCHEMA_REVISION,
                'cycle_id': os.environ.get('TM_RUN_ID', self._batch_id),
                'scope_id': scope['scope_id'],
            })

        if reused:
            logger.info(
                "coach bios reused from bronze: %d/%d", reused, len(unique_managers),
            )

        if not profile_rows and not stint_rows:
            logger.warning(
                "%s: zero coach_profile rows materialised.",
                R0_2B_FALLBACK_MARKER,
            )
            return _empty_bundle()

        if attempted and successes < _MIN_SUCCESS_RATIO * attempted:
            raise PartialScrapeError(
                f"only {successes}/{attempted} coach profiles fetched "
                f"(< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect partition"
            )

        ingested_at = datetime.utcnow()
        profiles = _with_metadata(
            profile_rows, COACH_PROFILE_COLUMNS,
            entity_type='coach_profiles', batch_id=self._batch_id,
            ingested_at=ingested_at,
        ).sort_values('coach_id', kind='mergesort').drop_duplicates(
            'coach_id', keep='first',
        )
        stints = _with_metadata(
            stint_rows, COACH_STINT_COLUMNS,
            entity_type='coach_stints', batch_id=self._batch_id,
            ingested_at=ingested_at,
        ).drop_duplicates(
            ['club_id', 'coach_id', 'appointed_date', 'left_date']
        )
        legacy = materialize_legacy_coaches(
            profiles, stints, league, season, scope['season_format'],
        )

        logger.info(
            "Materialised %d transfermarkt coaches (league=%s season=%s)",
            len(legacy), league, season,
        )
        return {
            'profiles': profiles,
            'stints': stints,
            'legacy_coaches': legacy,
        }

    def read_coach_profiles(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        coach_data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        data = coach_data or self.read_coach_data(
            league, season, limit, **kwargs,
        )
        return data['profiles']

    def read_coach_stints(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
        coach_data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        data = coach_data or self.read_coach_data(
            league, season, limit, **kwargs,
        )
        return data['stints']

    def read_coaches(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Legacy adapter over one native coach bundle fetch."""

        return self.read_coach_data(league, season, limit)['legacy_coaches']

    def _read_player_endpoint_rows(
        self,
        *,
        league: str,
        season: int,
        player_ids: Optional[List[str]],
        limit: Optional[int],
        window_offset: int,
        label: str,
        path_template: str,
        parser,
        columns: List[str],
        entity_type: str,
    ) -> pd.DataFrame:
        """Shared per-player fan-out with completeness and early-stop guards."""

        scope = self._resolve_scope(league, season)
        league = scope['compatibility_league']
        season_short = scope['canonical_season']
        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit, window_offset=window_offset,
            )
        if not player_ids:
            logger.warning(
                "%s: no player_ids resolved for %s (%s/%s).",
                R0_2B_FALLBACK_MARKER, label, league, season_short,
            )
            return _with_metadata(
                [], columns, entity_type=entity_type, batch_id=self._batch_id,
            )
        selected_ids = list(dict.fromkeys(str(pid) for pid in player_ids))
        if limit:
            selected_ids = selected_ids[: int(limit)]

        rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        required_successes = int(math.ceil(_MIN_SUCCESS_RATIO * len(selected_ids)))
        for idx, pid in enumerate(selected_ids, start=1):
            url = f"{_TM_BASE}" + path_template.format(player_id=pid)
            payload = self._fetch_json(
                url, label=label, context={
                    'player_id': pid,
                    'competition_id': scope['competition_id'],
                    'edition_id': scope['edition_id'],
                    'scope': scope['scope_id'],
                },
            )
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    raise ConsecutiveFailureError(
                        f"{consecutive_failures} consecutive {label} "
                        f"failures at {idx}/{len(selected_ids)} — aborting "
                        f"to protect existing partition"
                    )
            else:
                source_field = (
                    'list'
                    if label in {'mv_history', 'market_value_points'}
                    else 'transfers'
                )
                source_rows = payload.get(source_field)
                try:
                    parsed_rows = parser(payload, pid)
                except Exception as exc:  # parser failures are typed schema drift
                    parsed_rows = []
                    collection_error = (
                        f'parser raised {type(exc).__name__}: {exc}'
                    )
                else:
                    collection_error = _career_collection_error(
                        label, pid, source_rows, parsed_rows,
                    )

                if collection_error is not None:
                    consecutive_failures += 1
                    self._mark_schema_error(
                        label, {'player_id': pid}, collection_error,
                    )
                    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                        raise ConsecutiveFailureError(
                            f"{consecutive_failures} consecutive {label} parser "
                            f"failures at {idx}/{len(selected_ids)}"
                        )
                elif source_rows == []:
                    consecutive_failures = 0
                    successes += 1
                    self._mark_authoritative_empty(
                        label, {'player_id': pid},
                    )
                else:
                    consecutive_failures = 0
                    successes += 1
                    fetched_at = datetime.utcnow()
                    payload_hash = (
                        self._last_outcome.payload_hash
                        if self._last_outcome is not None else None
                    )
                    for row in parsed_rows:
                        row.update({
                            'source_competition_id': scope['competition_id'],
                            'source_edition_id': scope['edition_id'],
                            'source_url': url,
                            'source_body_hash': payload_hash,
                            'fetched_at': fetched_at,
                            'parser_revision': PARSER_REVISION,
                            'schema_revision': SCHEMA_REVISION,
                            'cycle_id': os.environ.get(
                                'TM_RUN_ID', self._batch_id,
                            ),
                            'scope_id': scope['scope_id'],
                        })
                    rows.extend(parsed_rows)
                    self._record_materialized_rows(
                        label, {'player_id': pid}, len(parsed_rows),
                    )

            remaining = len(selected_ids) - idx
            if successes + remaining < required_successes:
                # The 90% target is mathematically unreachable.  Do not spend
                # paid traffic on requests that cannot make the run writable.
                if successes:
                    raise PartialScrapeError(
                        f"only {successes}/{idx} {label} payloads fetched and "
                        f"{remaining} remain; {_MIN_SUCCESS_RATIO:.0%} target "
                        "is unreachable"
                    )
                break
            if idx % 50 == 0:
                logger.info("%s progress: %d/%d", label, idx, len(selected_ids))

        if successes and successes < required_successes:
            raise PartialScrapeError(
                f"only {successes}/{len(selected_ids)} {label} payloads "
                f"fetched (< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect "
                "existing partition"
            )
        frame = _with_metadata(
            rows, columns, entity_type=entity_type, batch_id=self._batch_id,
        )
        return frame

    def read_market_value_points(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        window_offset: int = 0,
    ) -> pd.DataFrame:
        """Return global market-value facts keyed by ``(player_id, mv_date)``.

        ``league``/``season`` select the roster only and are intentionally not
        copied into the global output.
        """
        self._begin_operation_budget('market_value_history')
        df = self._read_player_endpoint_rows(
            league=league,
            season=season,
            player_ids=player_ids,
            limit=limit,
            window_offset=window_offset,
            label='market_value_points',
            path_template=_PLAYER_MV_HISTORY_PATH,
            parser=_parse_mv_history,
            columns=MARKET_VALUE_POINT_COLUMNS,
            entity_type='market_value_points',
        )
        if not df.empty:
            df = df.sort_values(
                ['player_id', 'mv_date'], kind='mergesort',
            ).drop_duplicates(['player_id', 'mv_date'], keep='last')
        logger.info(
            "Materialised %d mv_history rows for %d players",
            len(df), df['player_id'].nunique() if not df.empty else 0,
        )
        return df

    def read_market_value_history(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        window_offset: int = 0,
    ) -> pd.DataFrame:
        """Legacy adapter over global market-value points."""

        points = self.read_market_value_points(
            league, season, player_ids, limit, window_offset,
        )
        scope = self._resolve_scope(league, season)
        return materialize_legacy_market_value_history(
            points, scope['compatibility_league'], scope['season_year'],
            scope['season_format'],
        )

    def read_transfer_events(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        window_offset: int = 0,
    ) -> pd.DataFrame:
        """Return global transfer facts keyed by globally unique transfer_id.

        ``event_season`` is derived only from the payload/date and is never
        overwritten by the requested roster season.
        """
        self._begin_operation_budget('transfers')
        df = self._read_player_endpoint_rows(
            league=league,
            season=season,
            player_ids=player_ids,
            limit=limit,
            window_offset=window_offset,
            label='transfer_events',
            path_template=_PLAYER_TRANSFERS_PATH,
            parser=_parse_transfers,
            columns=TRANSFER_EVENT_COLUMNS,
            entity_type='transfer_events',
        )
        if not df.empty:
            df = df.sort_values(
                ['transfer_id', 'player_id'], kind='mergesort',
            ).drop_duplicates('transfer_id', keep='last')
        logger.info(
            "Materialised %d transfer rows for %d players",
            len(df), df['player_id'].nunique() if not df.empty else 0,
        )
        return df

    def read_transfers(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        window_offset: int = 0,
    ) -> pd.DataFrame:
        """Legacy adapter over global transfer events."""

        events = self.read_transfer_events(
            league, season, player_ids, limit, window_offset,
        )
        scope = self._resolve_scope(league, season)
        return materialize_legacy_transfers(
            events, scope['compatibility_league'], scope['season_year'],
            scope['season_format'],
        )

    # ---------------------- BaseScraper contract -----------------------------

    def scrape_all(self) -> Dict[str, str]:
        """Anchor entity only — ``read_players``.

        ``read_transfers`` / ``read_market_value_history`` depend on a fresh
        ``bronze.transfermarkt_players`` table and are orchestrated by
        ``dags/scripts/run_transfermarkt_scraper.py`` as separate runs.
        """
        results: Dict[str, str] = {}
        for league in self.leagues:
            for season in self.seasons:
                df = self.read_players(league=league, season=season)
                if df.empty:
                    continue
                # Completeness guard (#513): refuse a replace that would shrink
                # the partition below 90% of the existing distinct players.
                table_path = self.save_to_iceberg(
                    df,
                    'transfermarkt_players',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                    min_replace_ratio=0.9,
                    replace_guard_key='player_id',
                )
                results[f'players_{league}_{season}'] = table_path
        return results
