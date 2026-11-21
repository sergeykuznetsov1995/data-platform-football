"""Pure, I/O-free Transfermarkt parsers (layer 3 of the rewrite, #2400/#2403).

Every function here maps recorded source bytes to rows without network,
filesystem, environment, or scraper state. Behaviour is frozen by the
stage-A characterization tests; move code verbatim, never rewrite.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from scrapers.transfermarkt.models import TransfermarktError
from scrapers.transfermarkt.registry import SeasonFormat, canonical_season


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

_TM_DATE_FORMATS = (
    '%b %d, %Y', '%B %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y',
)
_TM_UNKNOWN_DATE_SENTINELS = frozenset({'0000-00-00'})
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


def _transfer_date_is_unknown(entry: Dict) -> bool:
    """True when the source itself says the exact transfer day is unknown."""

    raw = entry.get('dateUnformatted') or entry.get('date')
    if raw:
        return str(raw).strip() in _TM_UNKNOWN_DATE_SENTINELS
    # A blank value is still the source speaking. Both machine fields missing
    # is not: a renamed date field would land here for every row and null out
    # every date behind a green scope, so let that keep aborting.
    return 'dateUnformatted' in entry or 'date' in entry


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
# Money
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Integers and height
# ---------------------------------------------------------------------------


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


_HEIGHT_RE = re.compile(r"(\d)\s*[,.':\u201a]\s*(\d{1,2})\s*m")


def _parse_height_cm(raw) -> Optional[int]:
    """``'1,89 m'`` / ``'1.89m'`` / ``"1'89m"`` → ``189``; ``'1,8m'`` → ``180``."""
    if not raw:
        return None
    m = _HEIGHT_RE.search(str(raw))
    if not m:
        return None
    return int(m.group(1)) * 100 + int(m.group(2).ljust(2, '0'))


# ---------------------------------------------------------------------------
# Season windows and coach stints
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


# ---------------------------------------------------------------------------
# Player and coach links
# ---------------------------------------------------------------------------


_PLAYER_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/profil/spieler/(?P<id>\d+)')
_COACH_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/profil/trainer/(?P<id>\d+)')


# ---------------------------------------------------------------------------
# Coach history
# ---------------------------------------------------------------------------


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


def _coach_history_page_is_empty(html: str) -> bool:
    """True when a club's staff page renders but nobody is on record.

    Amateur and youth clubs (#1051: Stade Mouscronnois, 2de Nationale FFA)
    serve their normal ``mitarbeiterhistorie`` page — club headline and all —
    with no stint table and no trainer anchor anywhere.  A drifted layout
    would still leave a trainer link for the parser to trip on, and an error
    page would not carry the club headline.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    if soup.find('h1', {'class': 'data-header__headline-wrapper'}) is None:
        return False
    if soup.find('table', {'class': 'items'}) is not None:
        return False
    return soup.find('a', href=_COACH_HREF_RE) is None


# ---------------------------------------------------------------------------
# Squads and contracts
# ---------------------------------------------------------------------------


_SQUAD_HEADER_FIELDS = {
    'date of birth/age': 'dob',
    'nat.': 'nationality',
    'height': 'height_cm',
    'foot': 'foot',
    'contract': 'contract_until',
    'joined': 'joined_at',
    'signed from': 'signed_from',
}

_AGE_IN_PARENS_RE = re.compile(r'\((\d+)\)\s*$')

# Squad-context fields of one player at one club in one season — the
# ``squad_memberships`` payload read from the same `/plus/1` row.
SQUAD_CONTEXT_FIELDS = (
    'shirt_number', 'joined_at', 'signed_from_club_id', 'signed_from_club_name',
    'signed_from_fee_text', 'signed_from_fee_eur', 'is_on_loan',
    'loan_from_club_id', 'loan_from_club_name', 'loan_until', 'is_captain',
    'injury_text', 'injury_return_date', 'nationality_secondary',
)
# A flag TM renders for an unknown country; it is not a nationality.
_NATIONALITY_PLACEHOLDERS = frozenset({'', '-', 'n/a'})
_VEREIN_ID_RE = re.compile(r'/verein/(\d+)')
# `Signed from` link title: ``"<club>: Ablöse <fee>"`` (German word even on .com).
_SIGNED_FROM_TITLE_RE = re.compile(r'^(?P<club>.*?)\s*:\s*Ablöse\s*(?P<fee>.*)$', re.S)
_LOAN_TITLE_RE = re.compile(
    r'^On loan from\s+(?P<club>.+?)(?:\s+until\s+(?P<until>\S+))?\s*$', re.S)


_INJURY_TITLE_RE = re.compile(
    r'^(?P<text>.*?)\s+-\s+Return(?:\s+expected\s+on\s+(?P<back>\S+))?', re.S)


def _parse_loan_marker(tr) -> Optional[Dict]:
    """``On loan from <club> until <date>`` badge next to the player's name.

    Past-season pages reuse the same badge for ``Returned after loan spell…``;
    that is a departure note, not a loan.
    """
    for span in tr.find_all('span', {'class': 'wechsel-kader-wappen'}):
        a = span.find('a', href=True)
        m = _LOAN_TITLE_RE.match(((a.get('title') if a else None) or '').strip())
        if not m:
            continue
        club = _VEREIN_ID_RE.search(a['href'])
        return {'loan_from_club_id': club.group(1) if club else None,
                'loan_from_club_name': m.group('club').strip() or None,
                'loan_until': _parse_tm_date(m.group('until'))}
    return None


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

    The same row also yields ``SQUAD_CONTEXT_FIELDS`` (#1406): shirt number,
    Joined / Signed from, the loan / captain / injury badges of the name cell
    and a second nationality flag.
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
    club_view = 'signed_from' in col_fields.values()

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
            '_contract_status': 'parse_error' if 'contract_until' in col_fields.values() else 'not_rendered',
            **dict.fromkeys(SQUAD_CONTEXT_FIELDS),
        }

        # Shirt number (`#` column, no header text): a number or `-`.
        rn = tr.find('div', {'class': 'rn_nummer'})
        rn_text = rn.get_text(strip=True) if rn else ''
        row['shirt_number'] = int(rn_text) if rn_text.isdigit() else None

        # Name-cell badges. A club page (it has `Signed from`) states loan
        # status for every row; a national-team page does not.
        if club_view:
            loan = _parse_loan_marker(tr)
            row['is_on_loan'] = loan is not None
            row.update(loan or {})
        row['is_captain'] = tr.find('span', {'class': 'kapitaenicon-table'}) is not None
        injury = tr.find('span', {'class': 'verletzt-table'})
        if injury is not None:
            m = _INJURY_TITLE_RE.match((injury.get('title') or '').strip())
            row['injury_text'] = (
                m.group('text') if m else (injury.get('title') or '')
            ).strip() or None
            row['injury_return_date'] = _parse_tm_date(m.group('back')) if m else None

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
                    flags = [
                        name for name in (
                            (img.get('title') or img.get('alt') or '').strip()
                            for img in td.find_all('img')
                        )
                        if name.lower() not in _NATIONALITY_PLACEHOLDERS
                    ]
                    row['nationality'] = flags[0] if flags else None
                    row['nationality_secondary'] = flags[1] if len(flags) > 1 else None
                elif field == 'height_cm':
                    row['height_cm'] = _parse_height_cm(text)
                elif field == 'foot':
                    row['foot'] = text.lower() if text not in ('', '-') else None
                elif field == 'contract_until':
                    row['contract_until'] = _parse_tm_date(text)
                    row['_contract_status'] = ('ok' if row['contract_until'] is not None else
                                               'unknown' if text in ('', '-', '–') else 'parse_error')
                elif field == 'joined_at':
                    row['joined_at'] = _parse_tm_date(text)
                elif field == 'signed_from':
                    row.update(_parse_signed_from(td))

        players.append(row)
    return players


def _parse_player_contract_profile(html, player_id):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    canonical = soup.find('link', rel='canonical', href=True)
    if canonical is None or not re.search(
            r'/profil/spieler/' + re.escape(str(player_id)) + r'(?:/|$)',
            urlsplit(canonical['href']).path):
        return None
    if soup.find('h1', class_='data-header__headline-wrapper') is None:
        return None

    club = soup.find('span', class_='data-header__club')
    link = club.select_one('a[href]') if club is not None else None
    contract_until = None
    status = 'unknown'
    header = next((label for label in soup.find_all('span', class_='data-header__label')
                   if 'contract expires' in label.get_text(' ', strip=True).rstrip(':').lower()), None)
    if header is not None:
        value = header.find('span', class_='data-header__content')
        if value is not None:
            text = value.get_text(' ', strip=True)
            if text and text not in {'-', '–'}:
                contract_until = _parse_tm_date(text)
                status = 'ok' if contract_until is not None else 'parse_error'
    else:
        for label in soup.find_all('span', class_='info-table__content--regular'):
            if 'contract expires' not in label.get_text(' ', strip=True).rstrip(':').lower():
                continue
            value = label.find_next_sibling('span', class_='info-table__content--bold')
            if value is None:
                continue
            text = value.get_text(' ', strip=True)
            if text and text not in {'-', '–'}:
                contract_until = _parse_tm_date(text)
                status = 'ok' if contract_until is not None else 'parse_error'
            break
    return {
        'player_id': player_id,
        'current_club_id': _extract_club_id_from_href(link['href']) if link is not None else None,
        'current_club_name': club.get_text(' ', strip=True) if club is not None else None,
        'contract_until': contract_until,
        'contract_status': status,
    }


def _parse_signed_from(td) -> Dict:
    """`Signed from` cell → origin club id/name and the stated fee."""
    out = {'signed_from_club_id': None, 'signed_from_club_name': None,
           'signed_from_fee_text': None, 'signed_from_fee_eur': None}
    a = td.find('a', href=True)
    img = td.find('img')
    if a is not None:
        m = _VEREIN_ID_RE.search(a['href'])
        out['signed_from_club_id'] = m.group(1) if m else None
    title = ((a.get('title') if a is not None else None) or '').strip()
    m = _SIGNED_FROM_TITLE_RE.match(title)
    name = m.group('club') if m else ((img.get('title') or img.get('alt')) if img else None)
    out['signed_from_club_name'] = (name or '').strip() or None
    fee = m.group('fee').strip() if m else ''
    if fee:
        out['signed_from_fee_text'] = fee
        if fee.lower() == 'free transfer':
            out['signed_from_fee_eur'] = 0
        else:
            try:
                out['signed_from_fee_eur'] = _parse_tm_money_eur(fee)
            except MoneyLocaleError:
                out['signed_from_fee_eur'] = None
    return out


def _squad_page_is_empty_roster(soup) -> bool:
    """True when the club page rendered but TM lists no players for the season.

    Dissolved/amateur clubs (e.g. FC Lienden, saison 2011) serve a normal club
    page — header present — with no squad table, or a squad table without a
    single data row. That is data truth. The verdict is bound to the squad
    container, not to the whole page: sidebar widgets may carry player links
    on an empty club page, and a table that still has data rows the parser
    could not read stays a schema error (selector drift).

    A data row is a ``tr`` of the table body with more than one ``td`` — the
    one-cell colspan "no entries" row is not data.
    """
    if soup.find('h1', {'class': 'data-header__headline-wrapper'}) is None:
        return False
    table = soup.find('table', {'class': 'items'})
    if table is None:
        return True
    body = table.find('tbody') or table
    return not any(
        len(tr.find_all('td', recursive=False)) > 1
        for tr in body.find_all('tr', recursive=False)
    )


def contract_status(row):
    """Legacy snapshots retain their original semantics for receipt verification."""
    return row.get('_contract_status', 'ok')


# ---------------------------------------------------------------------------
# Transfer events
# ---------------------------------------------------------------------------


_CEAPI_CLUB_HREF_RE = re.compile(r'/verein/(\d+)')
_EVENT_SEASON_RE = re.compile(r'^(?P<start>\d{2}|\d{4})\s*[/_-]\s*(?P<end>\d{2}|\d{4})$')


def _extract_club_id_from_href(href) -> Optional[str]:
    if not href:
        return None
    m = _CEAPI_CLUB_HREF_RE.search(str(href))
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Market value / transfers
# ---------------------------------------------------------------------------

def _image_id(raw, path: str) -> Optional[str]:
    """Read a numeric source ID only from the observed image path."""
    if not isinstance(raw, str):
        return None
    match = re.fullmatch(r'/' + re.escape(path) + r'/([0-9]+)(?:_[0-9]+)?\.png', urlsplit(raw).path)
    return match.group(1) if match else None


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
            'value_eur': None if entry.get('mw') == '-' else _coerce_int(entry.get('y')),
            'is_value_removed': entry.get('mw') == '-' if entry.get('mw') is not None else None,
            'club_id': _image_id(entry.get('wappen'), 'wappen/profil'),
            'mv_last_change': _parse_tm_date(payload.get('last_change')),
            'club_name': entry.get('verein'),
            'age': _coerce_int(entry.get('age')),
            'mv_raw': entry.get('mw'),
        })
    return rows


def _normalise_event_season(raw, transfer_date: Optional[date]) -> Optional[str]:
    """Normalise a source event season to the Bronze ``2526`` convention.

    The season a transfer belongs to (July rule) is not an edition's
    ``saison_id``; that rule lives in ``season.py`` (#1390).
    """

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
        if isinstance(fee_text, str):
            from bs4 import BeautifulSoup
            fee_text = BeautifulSoup(fee_text, 'html.parser').get_text(' ', strip=True)

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
            'from_is_special': from_d.get('isSpecial') if isinstance(from_d.get('isSpecial'), bool) else None,
            'to_is_special': to_d.get('isSpecial') if isinstance(to_d.get('isSpecial'), bool) else None,
            'from_country_flag': from_d.get('countryFlag'),
            'to_country_flag': to_d.get('countryFlag'),
            'from_country_id': _image_id(from_d.get('countryFlag'), 'flagge/verysmall'),
            'to_country_id': _image_id(to_d.get('countryFlag'), 'flagge/verysmall'),
            'is_loan_end': fee_text.startswith('End of loan') if isinstance(fee_text, str) else None,
            'fee_text': fee_text,
            'fee_eur': _parse_tm_money_eur(fee_text),
            'market_value_eur': _parse_tm_money_eur(entry.get('marketValue')),
            'is_upcoming': bool(entry.get('upcoming', False)),
        })
    return rows


# ---------------------------------------------------------------------------
# Row validators
# ---------------------------------------------------------------------------

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
        if (
            transfer_date is None
            and not upcoming
            and not _transfer_date_is_unknown(entry)
        ):
            raw_date = entry.get('dateUnformatted') or entry.get('date')
            return (
                'non-upcoming transfer has an unreadable transfer date '
                f'{str(raw_date)!r}'
            )

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


_CLUB_HREF_RE = re.compile(r'^/(?P<slug>[^/]+)/startseite/verein/(?P<id>\d+)')


def endpoint_validator(label, as_json):
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

    if label not in {
        'listing', 'teilnehmer', 'squad', 'coach_history', 'coach_profile', 'club_staff',
    }:
        return None

    def _validate_html(html):
        from bs4 import BeautifulSoup

        if label == 'club_staff':
            from scrapers.transfermarkt.staff import parse_staff
            try:
                parse_staff(html, 'validator')
            except ValueError as exc:
                return str(exc)
            return None
        soup = BeautifulSoup(html, 'html.parser')
        if label == 'coach_profile':
            if soup.find('h1', {'class': 'data-header__headline-wrapper'}) is None:
                return 'missing coach profile headline'
        elif soup.find('table', {'class': 'items'}) is None:
            if (
                label in ('listing', 'teilnehmer')
                and all(
                    label == 'teilnehmer'
                    and table.find('select', attrs={'name': 'saison_id'}) is not None
                    for table in soup.find_all('table')
                )
                and soup.find('a', href=_CLUB_HREF_RE) is None
                and soup.find('link', rel='alternate') is not None
            ):
                # Empty-shell competition page (#1025): no data tables
                # or club links. Participant pages may have a season
                # filter table (AFCN 2027). Accept the fetch so the caller can
                # decide whether it is the authoritative empty listing
                # for the requested competition.
                return None
            if label == 'coach_history' and _coach_history_page_is_empty(
                html,
            ):
                # A club with nobody on record (#1051) — same reasoning,
                # the caller decides it is authoritatively empty.
                return None
            # Dissolved/amateur clubs render a normal club page with no
            # squad table at all for old seasons (e.g. FC Lienden 2011/12)
            # — that is data truth, not selector drift.
            if label == 'squad' and _squad_page_is_empty_roster(soup):
                return None
            return 'missing table.items'
        elif label == 'listing' and soup.find('a', href=_CLUB_HREF_RE) is None:
            return 'listing has no club links'
        elif label == 'squad' and soup.find('a', href=_PLAYER_HREF_RE) is None:
            if _squad_page_is_empty_roster(soup):
                return None
            return 'squad has no player links'
        elif label == 'coach_history' and not _parse_coach_history(
            html, club_id='validator',
        ):
            return 'coach history has no header-mapped dated trainer rows'
        return None

    return _validate_html
