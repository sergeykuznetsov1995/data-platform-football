"""
Capology Scraper
================

Bronze ingest for Capology player salaries
(``bronze.capology_player_salaries``).

NOTICE
------
URL pattern and DataFrame column shape informed by oseymour/ScraperFC
(``src/ScraperFC/capology.py``, GPL-3.0). All code in this module is
written independently from scratch against the live Capology HTML.

ScraperFC drives Capology through headless Selenium with per-currency
JS clicks and "Next" pagination — none of that is necessary against the
live site (probe 2026-05-23, ``memory/feedback_capology_antibot_probe.md``):

  - tls_requests cold path returns the whole season roster (~526 rows
    for APL 2024/25) in a single ~2.7MB response.
  - The salary table is NOT in ``<table>`` markup — Capology embeds an
    inline JS array ``var data = [{...}, ...];`` that DataTables.js
    renders client-side.
  - Each row carries all three currencies (eur/gbp/usd) inline — no
    extra HTTP needed when scope widens beyond MVP-GBP.

Source: https://www.capology.com
"""

from __future__ import annotations

import html
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Endpoints & constants
# ---------------------------------------------------------------------------

_CAPOLOGY_BASE = "https://www.capology.com"
_SALARIES_PATH = "/uk/{league_slug}/salaries/{season_long}/"
_PAYROLLS_PATH = "/uk/{league_slug}/payrolls/{season_long}/"
_CONTRACTS_PATH = "/uk/{league_slug}/contract-extensions/{season_long}/"
_TRANSFERS_PATH = "/uk/{league_slug}/transfer-window/{season_long}/"

CAPOLOGY_LEAGUE_MAP: Dict[str, str] = {
    'ENG-Premier League': 'premier-league',
}

CAPOLOGY_SUPPORTED_CURRENCIES = ('GBP', 'EUR', 'USD')

# All three currencies arrive inline in the same `var data = [...]` row as
# `{base}_{gbp,eur,usd}` keys (probe-confirmed) — no extra HTTP. We extract the
# full symmetric set so the Bronze schema stays inspectable and stays one row
# per player (wide columns, currency partition stays 'GBP').
CAPOLOGY_MONEY_BASES = (
    'weekly_gross', 'annual_gross',
    'weekly_net', 'annual_net',
    'bonus_gross', 'bonus_net',
    'total_gross', 'total_net',
    'adjusted_total_gross', 'adjusted_total_net',
)
CAPOLOGY_MONEY_CURRENCIES = ('gbp', 'eur', 'usd')

# Club-level payroll table money bases (probe-confirmed 2026-06-05, APL).
# The page also carries a positional split d/f/k/m (defenders / forwards /
# keepers / midfielders), but those cells are Capology-Pro-locked
# (`<span class='footer-pro'>Locked</span>`) on the public page — never real
# data — so they are intentionally NOT extracted.
CAPOLOGY_PAYROLL_BASES = (
    'weekly_gross', 'weekly_net', 'annual_gross', 'annual_net',
    'bonus_gross', 'bonus_net', 'total_gross', 'total_net',
    'adjusted_total_gross', 'adjusted_total_net',
)

# Player-level contract-extensions table money bases (probe-confirmed).
# Adds `contract_total_*` (full contract value) on top of the salary set.
CAPOLOGY_CONTRACT_BASES = (
    'weekly_gross', 'weekly_net', 'annual_gross', 'annual_net',
    'bonus_gross', 'bonus_net', 'total_gross', 'total_net',
    'adjusted_total_gross', 'adjusted_total_net',
    'contract_total_gross', 'contract_total_net',
)

# Club-level transfer-window money bases — net balances, NOT weekly-divided.
CAPOLOGY_TRANSFER_MONEY_BASES = ('income', 'expense', 'balance', 'adjbalance')

# Capology contract-extensions has no genuine pre-2018-19 history (older season
# URLs silently serve the current extensions). Backfill floor guards against
# writing mislabelled current data under an old season partition.
CONTRACT_HISTORY_FLOOR = 2018

R0_2B_FALLBACK_MARKER = 'CAPOLOGY_FALLBACK'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _season_long(season: int) -> str:
    """``2024`` → ``'2024-2025'`` (Capology URL convention)."""
    return f"{int(season)}-{int(season) + 1}"


def _season_short(season: int) -> str:
    """``2024`` → ``'2425'`` (Bronze partition convention)."""
    s = str(int(season))
    return f"{s[2:4]}{(int(s[2:4]) + 1) % 100:02d}"


# Regex for inner row blocks; we deliberately avoid a full-blown JS parser.
_ROW_BLOCK_RE = re.compile(
    r"\{[\s\S]*?\}", re.MULTILINE,
)


def _slice_data_array(html: str) -> Optional[str]:
    """Return the JS substring that starts at ``var data = [`` and ends
    immediately before the corresponding closing ``];``.

    Naive bracket counting works on Capology's data array because the
    only embedded brackets are inside single-quoted string literals (HTML
    snippets like ``<a class='...'>``). The probe established that the
    only square-brackets inside the array body live inside such strings,
    where they would balance themselves; we still guard with a max-scan
    so a pathological page doesn't run away.
    """
    m = re.search(r"var\s+data\s*=\s*\[", html)
    if not m:
        return None
    start = m.end() - 1  # at the opening `[`
    depth = 0
    end = None
    # Capology's `var data = [...]` block is ~2MB; cap scan at 8MB just in case.
    max_scan = min(len(html), start + 8 * 1024 * 1024)
    in_str = False
    quote_char = ''
    i = start
    while i < max_scan:
        c = html[i]
        if in_str:
            if c == '\\':
                i += 2
                continue
            if c == quote_char:
                in_str = False
        else:
            if c in ("'", '"'):
                in_str = True
                quote_char = c
            elif c == '[':
                depth += 1
            elif c == ']':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        i += 1
    if end is None:
        return None
    return html[start:end]


_NAME_HREF_RE = re.compile(r"href='/player/([a-z0-9\-]+)/")
_CLUB_HREF_RE = re.compile(r"href='/club/([a-z0-9\-]+)/")
_NAME_TEXT_RE = re.compile(r"</img>([^<]+)</a>")
_FLAG_RE = re.compile(r"flags/([a-z\-]+)\.svg", re.IGNORECASE)
_VERIFIED_RE = re.compile(r"verified-green", re.IGNORECASE)
_MONEY_RE = re.compile(r"accounting\.formatMoney\(\s*\"?(\-?[\d.]+)")
_PLAYER_NAME_FROM_A_RE = re.compile(r">([^<>][^<>]*)</a>")


def _extract_anchor_text(html_snippet: str) -> Optional[str]:
    """``<a class='...'>...<img/>Erling Haaland</a>`` → ``Erling Haaland``.

    HTML entities в Capology rendered names (``Jake O&#39;Brien``,
    ``Bj&ouml;rn``) идут как `&#NN;` / `&name;` — без unescape резолвер
    видит сырой `Jake O&#39;Brien` и token_sort vs FBref ``Jake O'Brien``
    падает ниже 90 (issue #84, 4 orphans фиксились этой строкой).
    """
    if not html_snippet:
        return None
    # The display text is whatever sits between the trailing img and </a>.
    # Fall back to last >...< chunk if there's no img.
    m = re.search(r'>([^<>]+)</a>', html_snippet)
    return html.unescape(m.group(1).strip()) if m else None


def _parse_row_block(block: str) -> Optional[Dict]:
    """Project one ``{...}`` row from the inline JS array.

    Capology's row literal is JS, not JSON; we extract by field-by-field
    regex (more robust than `json.loads` after quote normalisation, given
    the embedded HTML strings + ``accounting.formatMoney(...)`` calls).
    """
    out: Dict[str, object] = {}

    # `name` → HTML <a>; extract slug + display name.
    name_match = re.search(r"'name'\s*:\s*\"([^\"]*)\"", block)
    if name_match:
        name_html = name_match.group(1)
        slug_m = _NAME_HREF_RE.search(name_html)
        out['player_slug'] = slug_m.group(1) if slug_m else None
        out['player_name'] = _extract_anchor_text(name_html)
    else:
        return None

    # `club` → HTML <a> or plain string.
    club_match = re.search(r"'club'\s*:\s*\"([^\"]*)\"", block)
    if club_match:
        club_html = club_match.group(1)
        slug_m = _CLUB_HREF_RE.search(club_html)
        out['club_slug'] = slug_m.group(1) if slug_m else None
        out['club_name'] = _extract_anchor_text(club_html) or club_html
    else:
        out['club_slug'] = None
        out['club_name'] = None

    # `country` → plain string ("Norway") on the live site; older dumps wrap
    # it in an <img class='flag'> snippet. Handle both.
    country_match = re.search(r"'country'\s*:\s*\"([^\"]*)\"", block)
    if country_match:
        country_val = country_match.group(1)
        if '<img' in country_val:
            flag = _FLAG_RE.search(country_val)
            out['country_code'] = flag.group(1) if flag else None
        else:
            out['country_code'] = country_val or None
    else:
        out['country_code'] = None

    # `verified` → either an HTML <img> with verified-green icon, or a
    # plain "True"/"False" string (Capology has shipped both shapes).
    verified_match = re.search(r"'verified'\s*:\s*\"([^\"]*)\"", block)
    if verified_match:
        val = verified_match.group(1)
        out['verified'] = (
            'verified-green' in val.lower() or val.lower() == 'true'
        )
    else:
        out['verified'] = False

    # `age` is wrapped in ``Math.round("24")``; extract the numeric literal.
    age_match = re.search(r"'age'\s*:\s*Math\.round\(\s*\"?([\d\.\-]+)", block)
    if age_match:
        try:
            out['age'] = int(float(age_match.group(1)))
        except ValueError:
            out['age'] = None
    else:
        m = re.search(r"'age'\s*:\s*\"?(-?\d+)", block)
        out['age'] = int(m.group(1)) if m else None

    # `position` (plain literal) + `status` (sometimes HTML span).
    for field, key in [('position', 'position'), ('status', 'status')]:
        m = re.search(
            rf"'{field}'\s*:\s*(?:\"([^\"]*)\"|'([^']*)')", block,
        )
        val = (m.group(1) or m.group(2)) if m else None
        if val and '<' in val:
            txt = re.search(r'>([^<>]+)</', val)
            val = txt.group(1).strip() if txt else None
        out[key] = val

    # Booleans arrive either as JS literal (`true`/`false`) or quoted string
    # (`"True"`/`"False"`).
    for field, key in [('active', 'active'), ('loan', 'loan')]:
        m = re.search(
            rf"'{field}'\s*:\s*(true|false|\"True\"|\"False\")", block,
        )
        if not m:
            out[key] = None
            continue
        out[key] = m.group(1).strip('"').lower() == 'true'

    # Salary fields — all three currencies (GBP/EUR/USD) arrive inline in the
    # same row as `{base}_{gbp,eur,usd}` keys, so we extract the full symmetric
    # set in one pass (no extra HTTP). Currency partition still tags 'GBP'.
    for base in CAPOLOGY_MONEY_BASES:
        for ccy in CAPOLOGY_MONEY_CURRENCIES:
            field = f'{base}_{ccy}'
            m = re.search(
                rf"'{field}'\s*:\s*accounting\.formatMoney\(\s*\"?(\-?[\d.]+)",
                block,
            )
            if m:
                raw = m.group(1)
                try:
                    # Some fields are wrapped in a divide expression
                    # (e.g. ``"27300000"/52``) — the value we capture is
                    # already the dividend, so floor-divide by 52 for weekly.
                    if base.startswith('weekly_'):
                        out[field] = int(float(raw)) // 52
                    else:
                        out[field] = int(float(raw))
                except (TypeError, ValueError):
                    out[field] = None
            else:
                out[field] = None

    return out


def _iter_row_blocks(data_block: str):
    """Yield each top-level ``{...}`` row literal from a sliced data array.

    Bracket counter mirrors `_slice_data_array` but for `{` / `}`, while
    ignoring string literals so embedded `}` in HTML strings doesn't trip us.
    Shared by every Capology table parser (salaries / payrolls / contracts /
    transfer-window) — the row-block framing is identical across products.
    """
    depth = 0
    in_str = False
    quote_char = ''
    start = None
    for i, c in enumerate(data_block):
        if in_str:
            if c == '\\':
                continue
            if c == quote_char:
                in_str = False
            continue
        if c in ("'", '"'):
            in_str = True
            quote_char = c
            continue
        if c == '{':
            if depth == 0:
                start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and start is not None:
                yield data_block[start:i + 1]
                start = None


# Field helpers tolerant to BOTH quote styles: the `salaries` /
# `contract-extensions` pages quote keys with `'`, while `payrolls` /
# `transfer-window` use `"`. The money call is also sometimes wrapped in an
# extra paren — `formatMoney(("226293600"/52), ...)` — so allow an optional `(`.
_Q = r"['\"]"  # opening/closing key quote (either kind)


def _money_field(block: str, field: str, weekly: bool = False) -> Optional[int]:
    """``'{field}': accounting.formatMoney("27300000"/52, ...)`` → int.

    Captures the raw dividend; weekly fields are stored ÷52 (the JS divides
    the annual figure by 52 for the weekly view).
    """
    m = re.search(
        rf"{_Q}{re.escape(field)}{_Q}\s*:\s*accounting\.formatMoney\(\s*\(?\s*\"?(-?[\d.]+)",
        block,
    )
    if not m:
        return None
    try:
        val = int(float(m.group(1)))
        return val // 52 if weekly else val
    except (TypeError, ValueError):
        return None


def _str_field(block: str, field: str) -> Optional[str]:
    """Plain string value for ``'{field}': "..."`` (either quote style)."""
    m = re.search(
        rf"{_Q}{re.escape(field)}{_Q}\s*:\s*(?:\"([^\"]*)\"|'([^']*)')", block,
    )
    if not m:
        return None
    val = m.group(1) if m.group(1) is not None else m.group(2)
    return val if val != '' else None


def _int_field(block: str, field: str) -> Optional[int]:
    """Integer value, tolerating ``Math.round("24")`` and quoted/plain forms."""
    m = re.search(
        rf"{_Q}{re.escape(field)}{_Q}\s*:\s*(?:Math\.round\(\s*)?\"?(-?\d+)", block,
    )
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _float_field(block: str, field: str) -> Optional[float]:
    """Float value, tolerating ``accounting.toFixed("27.6000", ...)`` wrap."""
    m = re.search(
        rf"{_Q}{re.escape(field)}{_Q}\s*:\s*(?:accounting\.toFixed\(\s*)?\"?(-?[\d.]+)",
        block,
    )
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _moment_date_field(block: str, field: str) -> Optional[str]:
    """``'expiration': moment("2029-06-30").format(...)`` → ``'2029-06-30'``.

    Capology wraps contract dates in a moment.js call; the ISO date is the
    first quoted arg. Returned as a plain ISO string for Bronze.
    """
    m = re.search(
        rf"{_Q}{re.escape(field)}{_Q}\s*:\s*moment\(\s*\"([0-9\-]+)\"", block,
    )
    return m.group(1) if m else None


def _parse_salary_table(html: str) -> List[Dict]:
    """Extract per-(player, club) salary rows from the Capology HTML."""
    return _parse_table(html, _parse_row_block)


def _parse_table(html: str, row_fn) -> List[Dict]:
    """Slice the inline `var data` array and project each row via ``row_fn``."""
    data_block = _slice_data_array(html)
    if not data_block:
        return []
    rows: List[Dict] = []
    for block in _iter_row_blocks(data_block):
        parsed = row_fn(block)
        if parsed:
            rows.append(parsed)
    return rows


def _money_cols(block: str, bases) -> Dict[str, object]:
    """Build the ``{base}_{ccy}`` money map for one row across all currencies."""
    out: Dict[str, object] = {}
    for base in bases:
        weekly = base.startswith('weekly_')
        for ccy in CAPOLOGY_MONEY_CURRENCIES:
            field = f'{base}_{ccy}'
            out[field] = _money_field(block, field, weekly=weekly)
    return out


def _club_anchor(block: str) -> tuple[Optional[str], Optional[str]]:
    """``"club": "<a href='/club/{slug}/...'>Name</a>"`` → (slug, name)."""
    club_html = _str_field(block, 'club')
    if not club_html:
        return None, None
    slug_m = _CLUB_HREF_RE.search(club_html)
    return (
        slug_m.group(1) if slug_m else None,
        _extract_anchor_text(club_html) or club_html,
    )


def _parse_payroll_row(block: str) -> Optional[Dict]:
    """Project one club-level payroll row."""
    slug, name = _club_anchor(block)
    if slug is None and name is None:
        return None
    out: Dict[str, object] = {
        'club_slug': slug,
        'club_name': name,
        'club_code': _str_field(block, 'club_code'),
    }
    out.update(_money_cols(block, CAPOLOGY_PAYROLL_BASES))
    return out


def _parse_contract_row(block: str) -> Optional[Dict]:
    """Project one player-level contract-extension row."""
    name_html = _str_field(block, 'name')
    if not name_html:
        return None
    slug_m = _NAME_HREF_RE.search(name_html)
    club_slug, club_name = _club_anchor(block)
    out: Dict[str, object] = {
        'player_slug': slug_m.group(1) if slug_m else None,
        'player_name': _extract_anchor_text(name_html),
        'club_slug': club_slug,
        'club_name': club_name,
        'signed': _moment_date_field(block, 'signed'),
        'expiration': _moment_date_field(block, 'expiration'),
        'years': _int_field(block, 'years'),
    }
    out.update(_money_cols(block, CAPOLOGY_CONTRACT_BASES))
    return out


def _parse_transfer_row(block: str) -> Optional[Dict]:
    """Project one club-level transfer-window row (net spend balances)."""
    slug, name = _club_anchor(block)
    if slug is None and name is None:
        return None
    out: Dict[str, object] = {
        'club_slug': slug,
        'club_name': name,
        'club_code': _str_field(block, 'club_code'),
        'players': _int_field(block, 'players'),
        'age': _float_field(block, 'age'),
        'foreign': _int_field(block, 'foreign'),
    }
    for base in CAPOLOGY_TRANSFER_MONEY_BASES:
        for ccy in CAPOLOGY_MONEY_CURRENCIES:
            field = f'{base}_{ccy}'
            out[field] = _money_field(block, field)
    return out


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class CapologyScraper(BaseScraper):
    """Bronze ingest for Capology salaries.

    Public entry point: ``read_player_salaries(league, season, currency, limit)``.
    """

    SOURCE_NAME = 'capology'
    DEFAULT_RATE_LIMIT = 10  # sub-CF-flare; probe established >5 req/s trips CF

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        currency: str = 'GBP',
        **kwargs,
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self.currency = currency.upper()
        if self.currency not in CAPOLOGY_SUPPORTED_CURRENCIES:
            logger.warning(
                "Currency %s outside MVP scope (%s) — proceeding anyway "
                "(scraper extracts whatever the inline JS row exposes).",
                self.currency, CAPOLOGY_SUPPORTED_CURRENCIES,
            )
        self._last_endpoint_error: Optional[Dict] = None

    # ----------------------- HTTP plumbing -----------------------------------

    def _build_tls_session(self):
        """Cold path = no proxy; optional proxy fallback if probe pivots."""
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

    def _fetch_html(
        self,
        url: str,
        max_attempts: int = 3,
        label: str = 'salaries',
        context: Optional[Dict] = None,
    ) -> Optional[str]:
        """GET with rate-limit + retry; returns None on persistent failure.

        Treats <100KB-body 200s as a soft CF flare and retries with a
        backoff (Capology serves ~2.7MB on the happy path).
        """
        import time

        import tls_requests
        from scrapers.utils.proxy_manager import ErrorType

        last_status = None
        last_error = None
        for attempt in range(1, max_attempts + 1):
            self._rate_limiter.acquire()
            self._stats['requests'] += 1

            client, proxy_obj = self._build_tls_session()
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            try:
                resp = client.get(url, headers=headers, timeout=(5.0, 30.0))
                last_status = resp.status_code
                body = resp.text
                if resp.status_code == 200 and len(body) >= 100_000:
                    if proxy_obj is not None:
                        proxy_obj.record_success()
                    self._stats['successes'] += 1
                    return body
                # CF flare: 200 with tiny challenge body OR explicit 403.
                if resp.status_code == 200 and len(body) < 100_000:
                    last_error = (
                        f"CF flare (200 / {len(body)}B challenge body)"
                    )
                    time.sleep(5 * attempt)
                elif resp.status_code == 403:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.FORBIDDEN.value)
                    last_error = "HTTP 403 (CF block)"
                    time.sleep(5 * attempt)
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

    # ----------------------- read_* entry points -----------------------------

    def read_player_salaries(
        self,
        league: str,
        season: int,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        currency = (currency or self.currency).upper()
        money_cols = [
            f'{base}_{ccy}'
            for base in CAPOLOGY_MONEY_BASES
            for ccy in CAPOLOGY_MONEY_CURRENCIES
        ]
        anchor_cols = [
            'player_slug', 'player_name', 'club_slug', 'club_name',
            'country_code', 'age', 'position', 'status',
            'active', 'loan', 'verified',
            *money_cols,
            'currency', 'league', 'season',
        ]

        if league not in CAPOLOGY_LEAGUE_MAP:
            logger.warning(
                "%s: league %s not in CAPOLOGY_LEAGUE_MAP — skipping.",
                R0_2B_FALLBACK_MARKER, league,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        league_slug = CAPOLOGY_LEAGUE_MAP[league]
        season_long = _season_long(season)
        season_short = _season_short(season)
        url = (
            f"{_CAPOLOGY_BASE}"
            + _SALARIES_PATH.format(
                league_slug=league_slug, season_long=season_long,
            )
        )

        html = self._fetch_html(
            url, label='salaries',
            context={'league': league, 'season': season, 'currency': currency},
        )
        if html is None:
            logger.error(
                "%s: salaries fetch failed for %s/%s/%s.",
                R0_2B_FALLBACK_MARKER, league, season, currency,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        rows = _parse_salary_table(html)
        if not rows:
            logger.warning(
                "%s: zero rows parsed from Capology HTML (size=%d) for %s/%s.",
                R0_2B_FALLBACK_MARKER, len(html), league, season,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if limit:
            rows = rows[: int(limit)]

        df = pd.DataFrame(rows)
        df['currency'] = currency
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_salaries'
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d Capology salary rows for %s/%s (%s)",
            len(df), league, season_short, currency,
        )
        return df

    def _read_product(
        self,
        league: str,
        season: int,
        path_tmpl: str,
        label: str,
        entity_type: str,
        row_fn,
        base_cols: List[str],
        money_cols: List[str],
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Shared fetch→parse→frame path for the club/contract products.

        Mirrors `read_player_salaries`: returns an empty (but correctly-typed)
        frame on out-of-scope league / fetch failure / zero parsed rows so the
        ingest runner can soft-fall back instead of raising.
        """
        cols = base_cols + money_cols + ['league', 'season', '_ingested_at']
        if league not in CAPOLOGY_LEAGUE_MAP:
            logger.warning(
                "%s: league %s not in CAPOLOGY_LEAGUE_MAP — skipping %s.",
                R0_2B_FALLBACK_MARKER, league, label,
            )
            return pd.DataFrame(columns=cols)

        league_slug = CAPOLOGY_LEAGUE_MAP[league]
        season_long = _season_long(season)
        season_short = _season_short(season)
        url = _CAPOLOGY_BASE + path_tmpl.format(
            league_slug=league_slug, season_long=season_long,
        )

        html = self._fetch_html(
            url, label=label, context={'league': league, 'season': season},
        )
        if html is None:
            logger.error(
                "%s: %s fetch failed for %s/%s.",
                R0_2B_FALLBACK_MARKER, label, league, season,
            )
            return pd.DataFrame(columns=cols)

        rows = _parse_table(html, row_fn)
        if not rows:
            logger.warning(
                "%s: zero %s rows parsed (size=%d) for %s/%s.",
                R0_2B_FALLBACK_MARKER, label, len(html), league, season,
            )
            return pd.DataFrame(columns=cols)

        if limit:
            rows = rows[: int(limit)]

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season_short
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = entity_type
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d Capology %s rows for %s/%s",
            len(df), label, league, season_short,
        )
        return df

    def read_team_payrolls(
        self, league: str, season: int, limit: Optional[int] = None,
    ) -> pd.DataFrame:
        money = [
            f'{base}_{ccy}'
            for base in CAPOLOGY_PAYROLL_BASES
            for ccy in CAPOLOGY_MONEY_CURRENCIES
        ]
        return self._read_product(
            league, season, _PAYROLLS_PATH, 'payrolls', 'team_payrolls',
            _parse_payroll_row,
            base_cols=['club_slug', 'club_name', 'club_code'],
            money_cols=money, limit=limit,
        )

    def read_contract_extensions(
        self, league: str, season: int, limit: Optional[int] = None,
    ) -> pd.DataFrame:
        money = [
            f'{base}_{ccy}'
            for base in CAPOLOGY_CONTRACT_BASES
            for ccy in CAPOLOGY_MONEY_CURRENCIES
        ]
        # Capology's contract-extensions history starts at 2018-19; for older
        # season URLs the page ignores the param and serves the CURRENT
        # extensions (verified 2026-06-05: 2014-2017 == 2025-26, 59/59 players).
        # Refuse pre-floor seasons so a backfill can't write mislabelled dupes.
        if int(season) < CONTRACT_HISTORY_FLOOR:
            logger.warning(
                "%s: contract_extensions has no real history before %d-%d "
                "(Capology serves current data for %s) — skipping.",
                R0_2B_FALLBACK_MARKER, CONTRACT_HISTORY_FLOOR,
                CONTRACT_HISTORY_FLOOR + 1, season,
            )
            cols = [
                'player_slug', 'player_name', 'club_slug', 'club_name',
                'signed', 'expiration', 'years',
            ] + money + ['league', 'season', '_ingested_at']
            return pd.DataFrame(columns=cols)
        return self._read_product(
            league, season, _CONTRACTS_PATH, 'contract-extensions',
            'contract_extensions', _parse_contract_row,
            base_cols=[
                'player_slug', 'player_name', 'club_slug', 'club_name',
                'signed', 'expiration', 'years',
            ],
            money_cols=money, limit=limit,
        )

    def read_transfer_window(
        self, league: str, season: int, limit: Optional[int] = None,
    ) -> pd.DataFrame:
        money = [
            f'{base}_{ccy}'
            for base in CAPOLOGY_TRANSFER_MONEY_BASES
            for ccy in CAPOLOGY_MONEY_CURRENCIES
        ]
        return self._read_product(
            league, season, _TRANSFERS_PATH, 'transfer-window',
            'transfer_window', _parse_transfer_row,
            base_cols=['club_slug', 'club_name', 'club_code',
                       'players', 'age', 'foreign'],
            money_cols=money, limit=limit,
        )

    # ----------------------- BaseScraper contract ----------------------------

    def scrape_all(self) -> Dict[str, str]:
        results: Dict[str, str] = {}
        for league in self.leagues:
            for season in self.seasons:
                df = self.read_player_salaries(
                    league=league, season=season, currency=self.currency,
                )
                if df.empty:
                    continue
                table_path = self.save_to_iceberg(
                    df,
                    'capology_player_salaries',
                    partition_cols=['league', 'season', 'currency'],
                    replace_partitions=['league', 'season', 'currency'],
                )
                results[
                    f'player_salaries_{league}_{season}_{self.currency}'
                ] = table_path
        return results
