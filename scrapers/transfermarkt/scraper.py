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

See ``memory/feedback_transfermarkt_antibot_probe.md`` and
``scripts/probe_transfermarkt.py`` for the probe that validated this setup.

Source: https://www.transfermarkt.us
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from scrapers.base.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Endpoints & constants
# ---------------------------------------------------------------------------

# .us mirror is the most stable English-language host; same content as .com.
_TM_BASE = "https://www.transfermarkt.us"

# {league_slug}/startseite/wettbewerb/{comp_id}/plus/?saison_id={year}
_LEAGUE_LISTING_PATH = (
    "/{league_slug}/startseite/wettbewerb/{comp_id}/plus/?saison_id={year}"
)
# {club_slug}/kader/verein/{club_id}/saison_id/{year}/plus/1  (the /plus/1
# variant exposes the wider, detailed squad table — same selector contract
# as the default page but with extra metadata columns visible.)
_CLUB_SQUAD_PATH = "/{club_slug}/kader/verein/{club_id}/saison_id/{year}/plus/1"
# /{player_slug}/profil/spieler/{player_id}
_PLAYER_PROFILE_PATH = "/{player_slug}/profil/spieler/{player_id}"
# JSON, no auth, no proxy required cookie-wise (but TM CF still requires proxy).
_PLAYER_MV_HISTORY_PATH = "/ceapi/marketValueDevelopment/graph/{player_id}"
_PLAYER_TRANSFERS_PATH = "/ceapi/transferHistory/list/{player_id}"
# Coaches (issue #434). Staff page lists the whole backroom; we keep only the
# "Coaching Staff" section row whose role is exactly "Manager" (head coach).
_CLUB_STAFF_PATH = "/{club_slug}/mitarbeiter/verein/{club_id}/saison_id/{year}"
_COACH_PROFILE_PATH = "/{coach_slug}/profil/trainer/{coach_id}"

# Canonical league-slug + competition-id mapping. MVP: APL only. Extend with
# {'ESP-La Liga': ('laliga', 'ES1'), ...} when the issue's scope widens.
TM_LEAGUE_MAP: Dict[str, Tuple[str, str]] = {
    'ENG-Premier League': ('premier-league', 'GB1'),
}

R0_2B_FALLBACK_MARKER = 'TM_FALLBACK'

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

def _season_short(season: int) -> str:
    """``2025`` → ``'2526'`` (matches Bronze/Silver partition convention)."""
    s = str(int(season))
    if len(s) == 4 and s.isdigit():
        return f"{s[2:4]}{(int(s[2:4]) + 1) % 100:02d}"
    return s


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


_TM_MV_RE = re.compile(r'€\s*([\d.,]+)\s*(k|m|bn|b)?', re.IGNORECASE)


def _parse_tm_money_eur(raw) -> Optional[int]:
    """Parse Transfermarkt money strings → integer EUR.

    Examples::
        '€ 45.00 m'   → 45_000_000
        '€500k'       → 500_000
        '€1.20bn'     → 1_200_000_000
        '?' / '-' / '' → None

    Decimal separator is the period (US/UK convention TM uses on .us); the
    grouping comma is ignored.
    """
    if raw is None:
        return None
    s = str(raw)
    m = _TM_MV_RE.search(s)
    if not m:
        return None
    num_str = m.group(1).replace(',', '')
    try:
        num = float(num_str)
    except ValueError:
        return None
    unit = (m.group(2) or '').lower()
    multiplier = {
        '': 1,
        'k': 1_000,
        'm': 1_000_000,
        'b': 1_000_000_000,
        'bn': 1_000_000_000,
    }.get(unit, 1)
    return int(num * multiplier)


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


_TM_DATE_FORMATS = ('%b %d, %Y', '%B %d, %Y', '%Y-%m-%d', '%d.%m.%Y')


def _parse_tm_date(raw) -> Optional[date]:
    """Parse the small set of date formats TM exposes on .us. ``None`` on fail."""
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

    Selectors (verified by ``scripts/probe_transfermarkt.py`` 2026-05-23):
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


def _parse_squad_page(html: str, club_id: str) -> List[Dict]:
    """Extract per-player rows from a club's squad page.

    Selectors:
        table.items > tbody > tr (per-player rows)
            td.hauptlink > a  → player link (slug, id, name)
            td.zentriert     → numeric/centered cells (varies by column)
            td.rechts        → market value (right-aligned)

    The squad table layout has shifted several times; we only commit to the
    fields that have a stable selector (id, slug, name, market_value_eur).
    Profile-page fetch fills in age/height/foot/dob/nationality/contract.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'items'})
    if not table:
        return []

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

        players.append({
            'player_id': pid,
            'player_slug': m.group('slug'),
            'name': a.get_text(strip=True),
            'club_id': str(club_id),
            'market_value_eur': mv_eur,
        })
    return players


def _parse_player_profile(html: str, player_id: str) -> Optional[Dict]:
    """Extract bio + current-state snapshot from a player profile page.

    Selectors (verified by probe 2026-05-23):
        h1.data-header__headline-wrapper                       → display name (incl. shirt#)
        a.data-header__market-value-wrapper                    → MV current + ``Last update: <date>``
        span[itemprop=birthDate|height|nationality]            → bio fields
        span.data-header__club                                 → current club name
        dd.detail-position__position                           → primary position
        span.data-header__label  (with 'Contract expires:')    → contract end date
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')

    name_el = soup.find('h1', {'class': 'data-header__headline-wrapper'})
    if not name_el:
        return None

    # Strip optional leading shirt #25 prefix.
    raw_name = re.sub(r'\s+', ' ', name_el.get_text(' ', strip=True))
    full_name = re.sub(r'^#\d+\s*', '', raw_name)

    mv_el = soup.find('a', {'class': 'data-header__market-value-wrapper'})
    mv_raw = mv_el.get_text(' ', strip=True) if mv_el else None
    mv_eur = _parse_tm_money_eur(mv_raw)
    mv_last_update = None
    if mv_raw:
        m = re.search(r'Last update:\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})', mv_raw)
        if m:
            mv_last_update = _parse_tm_date(m.group(1))

    dob_el = soup.find('span', {'itemprop': 'birthDate'})
    dob = _parse_tm_date(dob_el.get_text(' ', strip=True)) if dob_el else None

    h_el = soup.find('span', {'itemprop': 'height'})
    height_cm = _parse_height_cm(h_el.get_text(' ', strip=True)) if h_el else None

    nat_el = soup.find('span', {'itemprop': 'nationality'})
    nationality = nat_el.get_text(' ', strip=True) if nat_el else None

    pos_el = soup.find('dd', {'class': 'detail-position__position'})
    position = pos_el.get_text(' ', strip=True) if pos_el else None

    club_el = soup.find('span', {'class': 'data-header__club'})
    current_club = club_el.get_text(' ', strip=True) if club_el else None

    foot = None
    contract_until = None
    for label_el in soup.find_all('span', {'class': 'info-table__content--regular'}):
        label = label_el.get_text(' ', strip=True).rstrip(':').lower()
        value_el = label_el.find_next_sibling('span', {'class': 'info-table__content--bold'})
        if value_el is None:
            continue
        value = value_el.get_text(' ', strip=True)
        if label == 'foot':
            foot = value.lower() if value else None
        elif 'contract expires' in label:
            contract_until = _parse_tm_date(value)

    return {
        'player_id': str(player_id),
        'name': full_name,
        'position': position,
        'dob': dob,
        'height_cm': height_cm,
        'foot': foot,
        'nationality': nationality,
        'current_club_name': current_club,
        'contract_until': contract_until,
        'market_value_eur': mv_eur,
        'market_value_last_update': mv_last_update,
    }


def _parse_staff_managers(html: str, club_id: str) -> List[Dict]:
    """Extract the head coach(es) from a club staff (`mitarbeiter`) page.

    The staff page groups people under section headers
    (``div.content-box-headline``): "Coaching Staff", "Management", "Medical
    department", … Each person sits in a ``table.inline-table`` whose first
    cell links to ``/{slug}/profil/trainer/{id}`` and whose second line is the
    role. We keep ONLY the "Coaching Staff" section rows whose role is exactly
    ``"Manager"`` (the head coach) — assistants / GK coaches / analysts are
    dropped. Verified by scripts/probe_transfermarkt_coaches.py (2026-06-16).
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    hdr = soup.find(
        lambda t: bool(t.get('class'))
        and any('content-box-headline' in c for c in (t.get('class') or []))
        and 'Coaching Staff' in t.get_text()
    )
    if hdr is None:
        return []
    table = hdr.find_next('table')
    if table is None:
        return []

    managers: List[Dict] = []
    seen: set = set()
    for a in table.find_all('a', href=True):
        m = _COACH_HREF_RE.match(a['href'])
        if not m:
            continue
        coach_id = m.group('id')
        if coach_id in seen:
            continue
        name = a.get_text(strip=True)
        inline = a.find_parent('table', {'class': 'inline-table'})
        full = (
            re.sub(r'\s+', ' ', inline.get_text(' ', strip=True))
            if inline else name
        )
        role = full.replace(name, '').strip()
        if role != 'Manager':
            continue
        seen.add(coach_id)
        managers.append({
            'coach_id': coach_id,
            'coach_slug': m.group('slug'),
            'name': name,
            'role': role,
            'club_id': str(club_id),
        })
    return managers


def _parse_coach_profile(html: str, coach_id: str) -> Optional[Dict]:
    """Extract dob + nationality from a coach (`trainer`) profile page.

    Same ``data-header`` itemprop selectors as the player profile (verified by
    probe 2026-06-16): h1 headline, span[itemprop=birthDate|nationality].
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
        mv_date = _parse_tm_date(entry.get('datum_mw'))
        if mv_date is None:
            # Fallback: derive from `x` epoch-ms.
            x_ms = entry.get('x')
            if isinstance(x_ms, (int, float)):
                try:
                    mv_date = datetime.utcfromtimestamp(x_ms / 1000.0).date()
                except (OSError, OverflowError, ValueError):
                    mv_date = None
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


def _extract_club_id_from_href(href) -> Optional[str]:
    if not href:
        return None
    m = _CEAPI_CLUB_HREF_RE.search(str(href))
    return m.group(1) if m else None


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
    for entry in payload.get('transfers') or []:
        if not isinstance(entry, dict):
            continue
        transfer_date = _parse_tm_date(entry.get('date'))
        season_short = entry.get('season')  # TM ships '25/26' style — kept raw
        from_d = entry.get('from') or {}
        to_d = entry.get('to') or {}
        fee_text = entry.get('fee')

        # ``upcoming=True`` events have neither real fee nor finalised date —
        # we still record them so downstream features see pending moves.
        rows.append({
            'player_id': str(player_id),
            'transfer_date': transfer_date,
            'season': season_short,
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


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

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
    # Cloudflare-protected; per probe 2026-05-23 the residential proxy
    # tolerates ~12 req/min comfortably (latency stays <1.5s).
    DEFAULT_RATE_LIMIT = 12

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs,
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._last_endpoint_error: Optional[Dict] = None

    # -- HTTP plumbing (mirrors scrapers/sofascore/scraper.py:279 contract) --

    def _build_tls_session(self):
        """Create a ``tls_requests.Client`` bound to the next residential
        proxy. Cloudflare blocks direct egress (probe: no-proxy = 403).
        """
        import tls_requests

        proxy_url = None
        proxy_obj = None
        if self._proxy_manager is not None and self._proxy_manager.total_count > 0:
            proxy_obj = self._proxy_manager.get_proxy()
            if proxy_obj is not None:
                proxy_url = proxy_obj.url
        elif self.proxy:
            proxy_url = self.proxy

        client = (
            tls_requests.Client(proxy=proxy_url)
            if proxy_url else tls_requests.Client()
        )
        return client, proxy_obj

    def _fetch_endpoint(
        self,
        url: str,
        as_json: bool,
        max_attempts: int = 3,
        label: str = 'endpoint',
        context: Optional[Dict] = None,
    ):
        """Generic GET with proxy rotation + rate-limit + retry.

        Returns the decoded JSON dict (``as_json=True``) or the raw HTML
        string (``as_json=False``). Returns ``None`` on persistent failure
        or legitimate 404; the last failure reason is parked on
        ``self._last_endpoint_error`` for the runner's fallback classifier.
        """
        import tls_requests
        from scrapers.utils.proxy_manager import ErrorType

        last_status = None
        last_error = None
        for attempt in range(1, max_attempts + 1):
            self._rate_limiter.acquire()
            self._stats['requests'] += 1

            client, proxy_obj = self._build_tls_session()
            try:
                resp = client.get(url, timeout=(5.0, 12.0))
                last_status = resp.status_code
                if resp.status_code == 200:
                    if proxy_obj is not None:
                        proxy_obj.record_success()
                    self._stats['successes'] += 1
                    if as_json:
                        try:
                            return resp.json()
                        except Exception as e:
                            last_error = f"json_decode: {e}"
                            break
                    return resp.text
                if resp.status_code == 403:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.FORBIDDEN.value)
                    last_error = "HTTP 403 (CF block / proxy IP burned)"
                elif resp.status_code == 429:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.RATE_LIMIT.value)
                    last_error = "HTTP 429 rate-limited"
                    time.sleep(2 ** attempt)
                elif resp.status_code == 404:
                    logger.info(
                        "%s not exposed (%s) — 404", label, context or url,
                    )
                    self._stats['successes'] += 1
                    return None
                else:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.UNKNOWN.value)
                    last_error = f"HTTP {resp.status_code}"
            except tls_requests.exceptions.RequestException as e:  # type: ignore[attr-defined]
                if proxy_obj is not None:
                    proxy_obj.record_failure(ErrorType.CONNECTION.value)
                last_error = f"transport: {type(e).__name__}: {e}"
            except Exception as e:
                if proxy_obj is not None:
                    proxy_obj.record_failure(ErrorType.UNKNOWN.value)
                last_error = f"{type(e).__name__}: {e}"
            finally:
                try:
                    client.close()
                except Exception:
                    pass

            logger.warning(
                "%s attempt %d/%d failed (%s): %s",
                label, attempt, max_attempts, context or url, last_error,
            )

        self._stats['failures'] += 1
        self._last_endpoint_error = {
            'label': label,
            'status': last_status,
            'error': last_error,
            **(context or {}),
        }
        return None

    def _fetch_html(self, url: str, label: str = 'html', context=None) -> Optional[str]:
        return self._fetch_endpoint(url, as_json=False, label=label, context=context)

    def _fetch_json(self, url: str, label: str = 'json', context=None) -> Optional[dict]:
        return self._fetch_endpoint(url, as_json=True, label=label, context=context)

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
    ) -> List[str]:
        """DISTINCT player_id from ``bronze.transfermarkt_players``.

        Mirrors ``scrapers/sofascore/scraper.py:_resolve_player_ids_from_bronze``
        — the dependent entities (``transfers``, ``mv_history``) need a fresh
        roster from the ``players`` entity before they can fan out per-player.

        With ``limit`` the subset is ordered by player_id: a deterministic
        sample keeps the partition's distinct-player count stable across
        runs (replace-guard input, #484/#486) and makes mv_history and
        transfers sample the SAME players.
        """
        try:
            conn = self._bronze_connection()
            cur = conn.cursor()
            sql = (
                "SELECT DISTINCT player_id "
                "FROM iceberg.bronze.transfermarkt_players "
                "WHERE league = ? AND season = ?"
            )
            if limit:
                sql = sql + f" ORDER BY player_id LIMIT {int(limit)}"
            cur.execute(sql, (league, season_short))
            rows = cur.fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception as e:
            logger.warning(
                "Could not resolve transfermarkt player_ids from bronze: %s", e,
            )
            return []

    # ---------------------- read_* entry points ------------------------------

    def read_players(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Listing → squad pages → per-player profile pages.

        Returns one row per (league, season, player_id). ``limit`` caps the
        total number of players the scraper materialises (useful for smoke
        runs); ``None`` means full season.
        """
        anchor_cols = [
            'player_id', 'player_slug', 'name', 'position', 'dob', 'age',
            'height_cm', 'foot', 'nationality', 'contract_until',
            'market_value_eur', 'market_value_last_update',
            'current_club_id', 'current_club_name',
            'league', 'season',
        ]

        if league not in TM_LEAGUE_MAP:
            logger.warning(
                "%s: league %s not in TM_LEAGUE_MAP — skipping.",
                R0_2B_FALLBACK_MARKER, league,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        league_slug, comp_id = TM_LEAGUE_MAP[league]
        season_short = _season_short(season)

        # Step 1 — league listing → clubs.
        listing_url = (
            f"{_TM_BASE}"
            + _LEAGUE_LISTING_PATH.format(
                league_slug=league_slug, comp_id=comp_id, year=int(season),
            )
        )
        listing_html = self._fetch_html(
            listing_url,
            label='listing',
            context={'league': league, 'season': season},
        )
        if listing_html is None:
            logger.error(
                "%s: league listing fetch failed for %s/%s.",
                R0_2B_FALLBACK_MARKER, league, season,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        clubs = _parse_club_listing(listing_html)
        logger.info(
            "TM listing: %d clubs for league=%s season=%s",
            len(clubs), league, season,
        )

        # Step 2 — per-club squad page → players (id + slug + name + MV).
        squad_players: List[Dict] = []
        for club in clubs:
            squad_url = (
                f"{_TM_BASE}"
                + _CLUB_SQUAD_PATH.format(
                    club_slug=club['club_slug'],
                    club_id=club['club_id'],
                    year=int(season),
                )
            )
            html = self._fetch_html(
                squad_url,
                label='squad',
                context={'club_id': club['club_id']},
            )
            if html is None:
                continue
            for p in _parse_squad_page(html, club_id=club['club_id']):
                p['current_club_name'] = club['club_name']
                squad_players.append(p)

        if not squad_players:
            logger.warning(
                "%s: zero players harvested across %d clubs.",
                R0_2B_FALLBACK_MARKER, len(clubs),
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if limit:
            squad_players = squad_players[: int(limit)]

        logger.info("TM squad pages → %d players to enrich", len(squad_players))

        # Step 3 — per-player profile page → enrich bio fields.
        rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        today = datetime.utcnow().date()
        for idx, sp in enumerate(squad_players, start=1):
            profile_url = (
                f"{_TM_BASE}"
                + _PLAYER_PROFILE_PATH.format(
                    player_slug=sp['player_slug'], player_id=sp['player_id'],
                )
            )
            payload = self._fetch_html(
                profile_url,
                label='profile',
                context={'player_id': sp['player_id']},
            )
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    raise ConsecutiveFailureError(
                        f"{consecutive_failures} consecutive profile failures "
                        f"at {idx}/{len(squad_players)} — aborting to protect "
                        f"existing partition"
                    )
                continue
            consecutive_failures = 0
            successes += 1
            bio = _parse_player_profile(payload, sp['player_id']) or {}

            # Merge squad-level MV / club_id with profile-level enrichment.
            row = {
                'player_id': sp['player_id'],
                'player_slug': sp['player_slug'],
                'name': bio.get('name') or sp['name'],
                'position': bio.get('position'),
                'dob': bio.get('dob'),
                'age': (
                    (today.year - bio['dob'].year
                     - ((today.month, today.day) < (bio['dob'].month, bio['dob'].day)))
                    if bio.get('dob') else None
                ),
                'height_cm': bio.get('height_cm'),
                'foot': bio.get('foot'),
                'nationality': bio.get('nationality'),
                'contract_until': bio.get('contract_until'),
                'market_value_eur': (
                    bio.get('market_value_eur') or sp.get('market_value_eur')
                ),
                'market_value_last_update': bio.get('market_value_last_update'),
                'current_club_id': sp['club_id'],
                'current_club_name': (
                    bio.get('current_club_name') or sp.get('current_club_name')
                ),
            }
            rows.append(row)

            if idx % 50 == 0:
                logger.info("profile progress: %d/%d", idx, len(squad_players))

        if not rows:
            logger.warning(
                "%s: zero player_profile rows materialised.",
                R0_2B_FALLBACK_MARKER,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if successes < _MIN_SUCCESS_RATIO * len(squad_players):
            raise PartialScrapeError(
                f"only {successes}/{len(squad_players)} player profiles "
                f"fetched (< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect "
                f"existing partition"
            )

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'players'
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d transfermarkt players (league=%s season=%s)",
            len(df), league, season,
        )
        return df

    def read_coaches(
        self,
        league: str,
        season: int,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Listing → club staff pages → head-coach profile pages (issue #434).

        Returns one row per (league, season, coach_id) for the head coach
        (role == "Manager") of every club in the league-season. Feeds
        bronze.transfermarkt_coaches → silver → gold.dim_manager nationality/dob
        enrichment. ``limit`` caps clubs visited (smoke runs).
        """
        anchor_cols = [
            'coach_id', 'coach_slug', 'name', 'role', 'dob', 'nationality',
            'current_club_id', 'current_club_name', 'league', 'season',
        ]

        if league not in TM_LEAGUE_MAP:
            logger.warning(
                "%s: league %s not in TM_LEAGUE_MAP — skipping coaches.",
                R0_2B_FALLBACK_MARKER, league,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        league_slug, comp_id = TM_LEAGUE_MAP[league]
        season_short = _season_short(season)

        # Step 1 — league listing → clubs.
        listing_url = (
            f"{_TM_BASE}"
            + _LEAGUE_LISTING_PATH.format(
                league_slug=league_slug, comp_id=comp_id, year=int(season),
            )
        )
        listing_html = self._fetch_html(
            listing_url, label='listing',
            context={'league': league, 'season': season},
        )
        if listing_html is None:
            logger.error(
                "%s: league listing fetch failed for coaches %s/%s.",
                R0_2B_FALLBACK_MARKER, league, season,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        clubs = _parse_club_listing(listing_html)
        if limit:
            clubs = clubs[: int(limit)]
        logger.info("TM coaches: %d clubs for %s/%s", len(clubs), league, season)

        # Step 2 — per-club staff page → head coach (role == "Manager").
        managers: List[Dict] = []
        for club in clubs:
            staff_url = (
                f"{_TM_BASE}"
                + _CLUB_STAFF_PATH.format(
                    club_slug=club['club_slug'],
                    club_id=club['club_id'],
                    year=int(season),
                )
            )
            html = self._fetch_html(
                staff_url, label='staff',
                context={'club_id': club['club_id']},
            )
            if html is None:
                continue
            for mgr in _parse_staff_managers(html, club_id=club['club_id']):
                mgr['current_club_name'] = club['club_name']
                managers.append(mgr)

        if not managers:
            logger.warning(
                "%s: zero head coaches harvested across %d clubs.",
                R0_2B_FALLBACK_MARKER, len(clubs),
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        logger.info("TM staff pages → %d head coaches to enrich", len(managers))

        # Step 3 — per-coach profile page → dob + nationality.
        rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        for idx, mgr in enumerate(managers, start=1):
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
                        f"failures at {idx}/{len(managers)} — aborting to "
                        f"protect existing partition"
                    )
                continue
            consecutive_failures = 0
            successes += 1
            bio = _parse_coach_profile(payload, mgr['coach_id']) or {}
            rows.append({
                'coach_id': mgr['coach_id'],
                'coach_slug': mgr['coach_slug'],
                'name': bio.get('name') or mgr['name'],
                'role': mgr['role'],
                'dob': bio.get('dob'),
                'nationality': bio.get('nationality'),
                'current_club_id': mgr['club_id'],
                'current_club_name': mgr.get('current_club_name'),
            })

        if not rows:
            logger.warning(
                "%s: zero coach_profile rows materialised.",
                R0_2B_FALLBACK_MARKER,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if successes < _MIN_SUCCESS_RATIO * len(managers):
            raise PartialScrapeError(
                f"only {successes}/{len(managers)} coach profiles fetched "
                f"(< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect partition"
            )

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'coaches'
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d transfermarkt coaches (league=%s season=%s)",
            len(df), league, season,
        )
        return df

    def read_market_value_history(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Per-player MV timeline via ``/ceapi/marketValueDevelopment/graph``.

        ``player_ids`` defaults to DISTINCT from ``bronze.transfermarkt_players``
        for the requested (league, season). Empty roster → empty DF (runner
        emits ``TM_FALLBACK`` exit code 2).
        """
        anchor_cols = [
            'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
            'league', 'season',
        ]
        season_short = _season_short(season)

        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit,
            )
        if not player_ids:
            logger.warning(
                "%s: no player_ids resolved for mv_history (%s/%s).",
                R0_2B_FALLBACK_MARKER, league, season_short,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])
        if limit:
            player_ids = list(player_ids)[: int(limit)]

        rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        for idx, pid in enumerate(player_ids, start=1):
            url = f"{_TM_BASE}" + _PLAYER_MV_HISTORY_PATH.format(player_id=pid)
            payload = self._fetch_json(
                url, label='mv_history', context={'player_id': pid},
            )
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    raise ConsecutiveFailureError(
                        f"{consecutive_failures} consecutive mv_history "
                        f"failures at {idx}/{len(player_ids)} — aborting to "
                        f"protect existing partition"
                    )
                continue
            consecutive_failures = 0
            successes += 1
            rows.extend(_parse_mv_history(payload, pid))
            if idx % 50 == 0:
                logger.info("mv_history progress: %d/%d", idx, len(player_ids))

        if not rows:
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if successes < _MIN_SUCCESS_RATIO * len(player_ids):
            raise PartialScrapeError(
                f"only {successes}/{len(player_ids)} mv_history payloads "
                f"fetched (< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect "
                f"existing partition"
            )

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'market_value_history'
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id
        logger.info(
            "Materialised %d mv_history rows for %d players",
            len(df), df['player_id'].nunique(),
        )
        return df

    def read_transfers(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Per-player transfer events via ``/ceapi/transferHistory/list``.

        Same roster-resolution semantics as ``read_market_value_history``.
        """
        anchor_cols = [
            'player_id', 'transfer_date', 'season',
            'from_club_id', 'from_club_name',
            'to_club_id', 'to_club_name',
            'fee_text', 'fee_eur', 'market_value_eur', 'is_upcoming',
        ]
        season_short = _season_short(season)

        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit,
            )
        if not player_ids:
            logger.warning(
                "%s: no player_ids resolved for transfers (%s/%s).",
                R0_2B_FALLBACK_MARKER, league, season_short,
            )
            return pd.DataFrame(columns=anchor_cols + ['league', 'season', '_ingested_at'])
        if limit:
            player_ids = list(player_ids)[: int(limit)]

        rows: List[Dict] = []
        consecutive_failures = 0
        successes = 0
        for idx, pid in enumerate(player_ids, start=1):
            url = f"{_TM_BASE}" + _PLAYER_TRANSFERS_PATH.format(player_id=pid)
            payload = self._fetch_json(
                url, label='transfers', context={'player_id': pid},
            )
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    raise ConsecutiveFailureError(
                        f"{consecutive_failures} consecutive transfers "
                        f"failures at {idx}/{len(player_ids)} — aborting to "
                        f"protect existing partition"
                    )
                continue
            consecutive_failures = 0
            successes += 1
            rows.extend(_parse_transfers(payload, pid))
            if idx % 50 == 0:
                logger.info("transfers progress: %d/%d", idx, len(player_ids))

        if not rows:
            return pd.DataFrame(columns=anchor_cols + ['league', 'season', '_ingested_at'])

        if successes < _MIN_SUCCESS_RATIO * len(player_ids):
            raise PartialScrapeError(
                f"only {successes}/{len(player_ids)} transfers payloads "
                f"fetched (< {_MIN_SUCCESS_RATIO:.0%}) — aborting to protect "
                f"existing partition"
            )

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'transfers'
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id
        logger.info(
            "Materialised %d transfer rows for %d players",
            len(df), df['player_id'].nunique(),
        )
        return df

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
