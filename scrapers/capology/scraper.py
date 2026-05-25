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

CAPOLOGY_LEAGUE_MAP: Dict[str, str] = {
    'ENG-Premier League': 'premier-league',
}

CAPOLOGY_SUPPORTED_CURRENCIES = ('GBP',)

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
    """``<a class='...'>...<img/>Erling Haaland</a>`` → ``Erling Haaland``."""
    if not html_snippet:
        return None
    # The display text is whatever sits between the trailing img and </a>.
    # Fall back to last >...< chunk if there's no img.
    m = re.search(r'>([^<>]+)</a>', html_snippet)
    return m.group(1).strip() if m else None


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

    # Salary fields — currently MVP scope = GBP only, but we extract
    # whatever GBP variants exist so the schema stays inspectable.
    for field in (
        'weekly_gross_gbp', 'annual_gross_gbp',
        'weekly_net_gbp', 'annual_net_gbp',
        'bonus_gross_gbp', 'bonus_net_gbp',
        'total_gross_gbp', 'total_net_gbp',
        'adjusted_total_gross_gbp', 'adjusted_total_net_gbp',
    ):
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
                if field.startswith('weekly_'):
                    out[field] = int(float(raw)) // 52
                else:
                    out[field] = int(float(raw))
            except (TypeError, ValueError):
                out[field] = None
        else:
            out[field] = None

    return out


def _parse_salary_table(html: str) -> List[Dict]:
    """Extract per-(player, club) salary rows from the Capology HTML."""
    data_block = _slice_data_array(html)
    if not data_block:
        return []

    # Walk `data_block` finding sibling top-level `{...}` row literals.
    # Bracket counter mirrors `_slice_data_array` but for `{` / `}`, while
    # ignoring string literals so embedded `}` in HTML strings doesn't trip us.
    rows: List[Dict] = []
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
                block = data_block[start:i + 1]
                parsed = _parse_row_block(block)
                if parsed:
                    rows.append(parsed)
                start = None
    return rows


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
        anchor_cols = [
            'player_slug', 'player_name', 'club_slug', 'club_name',
            'country_code', 'age', 'position', 'status',
            'active', 'loan', 'verified',
            'weekly_gross_gbp', 'annual_gross_gbp',
            'weekly_net_gbp', 'annual_net_gbp',
            'bonus_gross_gbp', 'bonus_net_gbp',
            'total_gross_gbp', 'total_net_gbp',
            'adjusted_total_gross_gbp', 'adjusted_total_net_gbp',
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
