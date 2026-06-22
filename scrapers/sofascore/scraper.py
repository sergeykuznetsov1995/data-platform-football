"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import logging
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper


# camelCase -> snake_case for Bronze column names.
_CAMEL_RE_1 = re.compile(r'([A-Z]+)([A-Z][a-z])')
_CAMEL_RE_2 = re.compile(r'([a-z\d])([A-Z])')


def _camel_to_snake(name: str) -> str:
    """Convert a camelCase / PascalCase key to snake_case.

    Examples:
        ``goalsPrevented`` -> ``goals_prevented``
        ``XGOnTarget``     -> ``xg_on_target``
        ``totalAttemptAssist`` -> ``total_attempt_assist``
    """
    s1 = _CAMEL_RE_1.sub(r'\1_\2', name)
    return _CAMEL_RE_2.sub(r'\1_\2', s1).lower()


def _coerce_scalar(v):
    """Coerce a JSON value to a Bronze-safe scalar.

    SofaScore stats often nest a structure like
    ``{"value": 3, "previousValue": 2, ...}`` for richer UI rendering.
    For Bronze we only keep the canonical ``value``; richer payloads
    can be re-derived from raw JSON if ever needed.
    """
    if isinstance(v, dict):
        # Most common SofaScore shape: {"key": "...", "value": ...}.
        if 'value' in v:
            return _coerce_scalar(v['value'])
        return None
    if isinstance(v, (list, tuple)):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if v is None:
        return None
    # String — try numeric upcast (SofaScore returns e.g. "12.4" for some
    # rating sub-stats). Fall back to the raw string.
    s = str(v).strip()
    if not s:
        return None
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except (TypeError, ValueError):
        return s

logger = logging.getLogger(__name__)


# SofaScore public REST API
_SOFASCORE_API = "https://api.sofascore.com/api/v1"
_LINEUPS_PATH = "/event/{event_id}/lineups"
_SHOTMAP_PATH = "/event/{event_id}/shotmap"
_EVENT_PLAYER_STATS_PATH = "/event/{event_id}/player/{player_id}/statistics"
_MATCH_STATS_PATH = "/event/{event_id}/statistics"
_PLAYER_PROFILE_PATH = "/player/{player_id}"
_PLAYER_SEASON_STATS_PATH = (
    "/player/{player_id}/unique-tournament/{ut_id}/season/{season_id}/"
    "statistics/overall"
)
_TOURNAMENT_SEASONS_PATH = "/unique-tournament/{ut_id}/seasons"


# SofaScore "unique-tournament" id per soccerdata league key. Discovered
# via `/api/v1/unique-tournament/{id}/seasons` probes during issue #19;
# stable since at least 2024. Missing league → runtime fallback lookup.
SOFASCORE_TOURNAMENT_MAP: Dict[str, int] = {
    'ENG-Premier League': 17,
    'ESP-La Liga': 8,
    'GER-Bundesliga': 35,
    'ITA-Serie A': 23,
    'FRA-Ligue 1': 34,
}

# Canonical SofaScore league-page slug per soccerdata league key. The browser
# capture nav URL needs country/competition slug + id — /unique-tournament/{id}
# alone 404s (#757 B0). Only EPL is live-verified; the rest follow SofaScore's
# /tournament/<slug>/<ut_id> pattern and should be confirmed before use.
SOFASCORE_TOURNAMENT_SLUG: Dict[str, str] = {
    'ENG-Premier League': 'football/england/premier-league',
    'ESP-La Liga': 'football/spain/laliga',
    'GER-Bundesliga': 'football/germany/bundesliga',
    'ITA-Serie A': 'football/italy/serie-a',
    'FRA-Ligue 1': 'football/france/ligue-1',
}

# R0.2b — graceful-fallback marker emitted when the lineups endpoint
# is structurally unavailable (HTTP 403 / quota empty / repeated timeouts).
# Downstream (E4.4 schema-stub path) keys off this marker to keep the
# Gold layer building without ratings instead of failing the DAG.
R0_2B_FALLBACK_MARKER = "R0.2B_FALLBACK"


class SofaScoreScraper(SoccerdataScraper):
    """
    Scraper for SofaScore football data.

    SofaScore provides:
    - Live match data and scores
    - Detailed match statistics
    - Player ratings
    - Heatmaps and position data

    Usage:
        scraper = SofaScoreScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'sofascore'
    DEFAULT_RATE_LIMIT = 20  # SofaScore can be strict

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None

    def _get_reader(self):
        """Get soccerdata SofaScore reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.Sofascore(
                    leagues=self.leagues,
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    def read_schedule(self) -> Optional[pd.DataFrame]:
        """
        Read match schedule and results.

        Returns:
            DataFrame with match schedule
        """
        reader = self._get_reader()
        logger.info("Fetching SofaScore schedule")

        try:
            df = self._execute_with_resilience(reader.read_schedule)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'schedule')

            return df

        except Exception as e:
            logger.error(f"Error reading schedule: {e}")
            return None

    def read_league_table(self) -> Optional[pd.DataFrame]:
        """
        Read league table (standings).

        Returns:
            DataFrame with league standings
        """
        reader = self._get_reader()
        logger.info("Fetching SofaScore league table")

        try:
            df = self._execute_with_resilience(reader.read_league_table)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._add_metadata(df, 'league_table')

            return df

        except Exception as e:
            logger.error(f"Error reading league table: {e}")
            return None

    def read_team_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team season statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns league table instead.
        """
        return self.read_league_table()

    def read_team_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read team match-level statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore team match stats not available in soccerdata")
        return None

    def read_player_match_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player match-level statistics with ratings.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore player match stats not available in soccerdata")
        return None

    # ------------------------------------------------------------------
    # R0.2b — player_ratings (Opta scale 0.0–10.0)
    # ------------------------------------------------------------------

    def _resolve_match_ids(
        self,
        league: str,
        season: int,
    ) -> List[str]:
        """Pull finished match_ids for (league, season) from the
        already-scraped soccerdata schedule.

        Falls back to ``read_schedule()`` if a single instance is asked
        for ratings without prior schedule scrape.
        """
        df = self.read_schedule()
        if df is None or df.empty:
            # soccerdata schedule is Turnstile-blocked (#757) — fall back to
            # discovering finished matches via the Camoufox capture transport.
            return self.resolve_finished_match_ids_via_capture(league, season)

        df = df.copy()
        # Coerce season to soccerdata 'YYZZ' string format if int passed.
        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        if 'league' in df.columns and 'season' in df.columns:
            mask = (df['league'] == league) & (
                df['season'].astype(str).isin([season_short, season_str])
            )
            df = df[mask]

        # Keep only finished matches (have a score) — schedule writer
        # leaves home_score NaN for unplayed games.
        if 'home_score' in df.columns:
            df = df[df['home_score'].notna()]

        if 'game_id' not in df.columns:
            return []

        return [str(int(g)) for g in df['game_id'].dropna().tolist()]

    def _build_tls_session(self):
        """Create a tls_requests.Client bound to the next residential
        proxy, mirroring the JA3/JA4 fingerprint workaround the rest of
        the platform relies on.
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

        client = tls_requests.Client(proxy=proxy_url) if proxy_url else tls_requests.Client()
        return client, proxy_obj

    def _fetch_json_endpoint(
        self,
        url: str,
        max_attempts: int = 3,
        label: str = 'endpoint',
        context: Optional[Dict] = None,
    ) -> Optional[dict]:
        """Generic GET → JSON over SofaScore's public REST API with proxy
        rotation, rate-limit, retry, and graceful 404.

        Parameters
        ----------
        url : str
            Fully-qualified request URL.
        max_attempts : int
            Retry budget; matches ``_fetch_lineup_payload`` historical
            behaviour (3 attempts, exponential backoff on 429).
        label : str
            Short tag used in log lines (e.g. ``"lineups"``, ``"shotmap"``).
        context : dict | None
            Extra fields stored on ``self._last_endpoint_error`` for
            R0.2B_FALLBACK classification by the runner (e.g.
            ``{'event_id': '123'}``).

        Returns
        -------
        dict | None
            Parsed JSON on 200. ``None`` on 404 (legitimate-empty) or
            after exhausted attempts (structural failure).
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
                # (connect, read) — keep wall-clock per attempt < 15s so a
                # hung proxy rotates instead of stalling the whole backfill
                # (issue #30).
                resp = client.get(url, timeout=(5.0, 8.0))
                last_status = resp.status_code
                if resp.status_code == 200:
                    if proxy_obj is not None:
                        proxy_obj.record_success()
                    self._stats['successes'] += 1
                    try:
                        return resp.json()
                    except Exception as parse_err:  # pragma: no cover - defensive
                        last_error = f"json_decode: {parse_err}"
                        logger.warning(
                            "%s payload not JSON (%s): %s",
                            label, context or url, parse_err,
                        )
                        break
                if resp.status_code == 403:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.FORBIDDEN.value)
                    last_error = "HTTP 403 (likely TLS fingerprint / IP block)"
                elif resp.status_code == 429:
                    if proxy_obj is not None:
                        proxy_obj.record_failure(ErrorType.RATE_LIMIT.value)
                    last_error = "HTTP 429 rate-limited"
                    time.sleep(2 ** attempt)
                elif resp.status_code == 404:
                    # Some events / players / seasons don't expose the
                    # resource (cancelled match, retired player) — treat
                    # as legitimate empty.
                    logger.info("%s not exposed (%s) — 404", label, context or url)
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
        # Surface the structural reason so the runner can decide whether
        # to emit the R0.2B_FALLBACK marker. Stored under a single rolling
        # attribute so any endpoint helper can classify the last failure.
        self._last_endpoint_error = {
            'label': label,
            'status': last_status,
            'error': last_error,
            **(context or {}),
        }
        return None

    def _fetch_lineup_payload(
        self,
        event_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        """Fetch /event/{id}/lineups JSON via the generic endpoint helper.

        Thin wrapper around :meth:`_fetch_json_endpoint` that preserves
        the historical ``self._last_lineup_error`` attribute the
        R0.2B player_ratings runner classifies fallbacks against.
        """
        url = f"{_SOFASCORE_API}{_LINEUPS_PATH.format(event_id=event_id)}"
        payload = self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='lineups',
            context={'event_id': event_id},
        )
        if payload is None:
            err = getattr(self, '_last_endpoint_error', None)
            if err is not None:
                self._last_lineup_error = {
                    'event_id': event_id,
                    'status': err.get('status'),
                    'error': err.get('error'),
                }
        return payload

    @staticmethod
    def _flatten_lineup_side(
        match_id: str,
        side: str,
        side_payload: dict,
    ) -> List[Dict]:
        """Project SofaScore's nested player-list into flat rows.

        Schema per row:
            match_id, player_id, team_side, rating, position
        """
        rows: List[Dict] = []
        if not isinstance(side_payload, dict):
            return rows

        for entry in side_payload.get('players', []) or []:
            if not isinstance(entry, dict):
                continue
            player = entry.get('player') or {}
            stats = entry.get('statistics') or {}

            pid = player.get('id')
            if pid is None:
                continue

            raw_rating = stats.get('rating')
            try:
                rating_val = float(raw_rating) if raw_rating is not None else None
            except (TypeError, ValueError):
                rating_val = None
            # SofaScore reports 0.0 for players who didn't enter the pitch
            # — it's not a rating, treat as NULL (matches Opta semantics).
            if rating_val is not None and rating_val == 0.0:
                rating_val = None

            # Position priority: per-event role (e.g. "G","D","M","F"),
            # falling back to the player's nominal position string.
            position = (
                entry.get('position')
                or player.get('position')
                or None
            )

            player_id_str = (
                str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
            )

            rows.append({
                'match_id': str(match_id),
                'player_id': player_id_str,
                'team_side': side,
                'rating': rating_val,
                'position': position,
            })

        return rows

    @staticmethod
    def _build_lineup_overlay_lookup(
        lineup_payload: dict,
    ) -> Dict[str, Dict[str, object]]:
        """Map ``player_id -> {is_home, captain, substitute,
        position_specific}`` from a ``/event/{id}/lineups`` payload.

        The ``.../player/{pid}/statistics`` endpoint returns ``extra:
        null`` and no ``statistics.position`` (verified live 2026-06-05,
        #301), so these four anchor columns are 100% NULL when sourced
        from there alone. ``/lineups`` carries them per player:

        - ``is_home`` — derived from the side (home -> True, away -> False).
        - ``captain`` — ``entry['captain']`` is present (``True``) only on
          the captain's entry; absent elsewhere -> ``bool(...)`` yields
          ``False`` for every other named player.
        - ``substitute`` — ``entry['substitute']`` is a real bool on every
          entry (starters ``False``, bench ``True``).
        - ``position_specific`` — the per-event line ``entry['position']``
          (``'G'/'D'/'M'/'F'``).

        Player ids mirror :meth:`_flatten_lineup_side`'s ``str(int(pid))``
        normalisation so the lookup keys match the ``pids`` resolved from
        ``bronze.sofascore_player_ratings``. Returns ``{}`` for an empty /
        non-dict payload (no raise).
        """
        lookup: Dict[str, Dict[str, object]] = {}
        if not isinstance(lineup_payload, dict):
            return lookup

        for side in ('home', 'away'):
            side_payload = lineup_payload.get(side) or {}
            if not isinstance(side_payload, dict):
                continue
            for entry in side_payload.get('players', []) or []:
                if not isinstance(entry, dict):
                    continue
                player = entry.get('player') or {}
                pid = player.get('id')
                if pid is None:
                    continue
                player_id_str = (
                    str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
                )
                lookup[player_id_str] = {
                    'is_home': side == 'home',
                    'captain': bool(entry.get('captain')),
                    'substitute': bool(entry.get('substitute')),
                    'position_specific': entry.get('position') or None,
                }

        return lookup

    @staticmethod
    def _apply_lineup_overlay(
        row: Dict,
        overlay: Optional[Dict[str, object]],
    ) -> None:
        """Fill ``is_home/captain/substitute/position_specific`` on a
        stats ``row`` in-place from a per-player lineup ``overlay``.

        Fill-if-None: only writes where ``row`` is currently ``None`` and
        ``overlay`` provides a non-None value. If SofaScore ever starts
        returning a populated ``extra`` block on the statistics endpoint,
        that primary source wins and the overlay stays a pure backfill.
        ``overlay=None`` (player absent from lineups) leaves the row
        untouched.
        """
        if not overlay:
            return
        for col in ('is_home', 'captain', 'substitute', 'position_specific'):
            if row.get(col) is None and overlay.get(col) is not None:
                row[col] = overlay[col]

    def _camoufox_proxy(self) -> Optional[dict]:
        """Build a Camoufox/Playwright proxy dict (creds split out — browsers
        reject creds embedded in the URL) from the configured residential
        proxy. Returns ``None`` when none is configured; SofaScore's Turnstile
        then 403s every data XHR (#757), so a proxy is required in production.
        Mirrors :meth:`_build_tls_session`'s proxy selection.
        """
        proxy_obj = None
        if self._proxy_manager is not None and self._proxy_manager.total_count > 0:
            proxy_obj = self._proxy_manager.get_proxy()
        if proxy_obj is None:
            logger.warning(
                "No residential proxy configured for SofaScore capture — "
                "Turnstile will 403 every data endpoint (#757)."
            )
            return None
        d = {'server': f'http://{proxy_obj.host}:{proxy_obj.port}'}
        if proxy_obj.username and proxy_obj.password:
            d['username'] = proxy_obj.username
            d['password'] = proxy_obj.password
        logger.info("SofaScore capture proxy: %s", proxy_obj.masked_url)
        return d

    def _iter_match_captures(
        self,
        match_ids: List[str],
        tabs=("Lineups",),
        required=("lineups",),
    ):
        """Yield ``(match_id, endpoints)`` by capturing each match page through
        ONE warmed Camoufox session (issue #757, path P2). ``endpoints`` holds
        whichever of ``event/lineups/statistics/shotmap/incidents`` came back as
        real JSON (see ``camoufox_capture.select_event_endpoints``).

        Generalises the ratings-only lineup iterator so the daily consolidated
        path (#751 PR1) also gets the ``event`` payload (``homeTeam``/
        ``awayTeam`` — team mapping for ``event_player_stats``) from the SAME
        single navigation. Replaces the dead ``tls_requests`` REST path:
        SofaScore's API is Cloudflare-Turnstile-gated and only a real Firefox
        (Camoufox) behind a residential proxy passes it; the SPA fires its own
        XHRs and we capture the responses. ``endpoints`` without ``lineups``
        means the capture missed (Turnstile not solved / proxy dead). The
        generator owns the browser session — the caller MUST ``.close()`` it
        (via try/finally) so it tears down even on an early circuit-breaker break.
        """
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture

        proxy = self._camoufox_proxy()
        with SofascoreCamoufoxCapture(proxy=proxy) as cap:
            for mid in match_ids:
                self._rate_limiter.acquire()
                self._stats['requests'] += 1
                try:
                    endpoints = cap.capture_event(
                        str(mid), tabs=tabs, required=required,
                    )
                except Exception as e:  # noqa: BLE001 — one bad event mustn't kill the loop
                    logger.warning("camoufox capture failed for event=%s: %s", mid, e)
                    endpoints = {}
                if not endpoints.get('lineups'):
                    self._stats['failures'] += 1
                    self._last_lineup_error = {
                        'event_id': str(mid),
                        'status': None,
                        'error': 'lineups_not_captured',
                    }
                else:
                    self._stats['successes'] += 1
                yield str(mid), endpoints

    def _iter_lineup_payloads(self, match_ids: List[str]):
        """Yield ``(match_id, lineups_payload | None)`` — the ratings-only view
        over :meth:`_iter_match_captures` (keeps ``read_player_ratings``
        unchanged: it clicks just the Lineups tab and consumes ``lineups``)."""
        captures = self._iter_match_captures(match_ids, tabs=("Lineups",))
        try:
            for mid, endpoints in captures:
                yield mid, ((endpoints or {}).get('lineups'))
        finally:
            close = getattr(captures, 'close', None)
            if callable(close):
                close()  # tear down the Camoufox session

    def resolve_finished_match_ids_via_capture(
        self, league: str, season: int,
    ) -> List[str]:
        """Resolve finished match_ids for ``(league, season)`` by capturing the
        SofaScore league page through Camoufox (#757 B1).

        The soccerdata/tls schedule path is Turnstile-blocked, so when
        ``bronze.sofascore_schedule`` is empty (e.g. a fresh season) we navigate
        the league page, let the SPA fire its ``/events/{round,last}`` XHR, and
        pull the finished matches from the captured JSON. Returns ``[]`` when the
        league has no SofaScore slug/ut_id, capture fails, or no finished match
        is on the page (off-season). ``season`` is currently informational — the
        league page serves the CURRENT season (the new-season daily use case);
        past-season resolution still relies on ``bronze.sofascore_schedule``.
        """
        from scrapers.sofascore.camoufox_capture import (
            SofascoreCamoufoxCapture,
            extract_tournament_events,
            finished_event_ids,
        )

        ut_id = self._resolve_unique_tournament_id(league)
        slug = SOFASCORE_TOURNAMENT_SLUG.get(league)
        if ut_id is None or slug is None:
            logger.warning(
                "No SofaScore slug/ut_id for league=%s — capture schedule "
                "resolution skipped.", league,
            )
            return []

        nav_url = f"https://www.sofascore.com/tournament/{slug}/{ut_id}"
        proxy = self._camoufox_proxy()
        try:
            self._rate_limiter.acquire()
            with SofascoreCamoufoxCapture(proxy=proxy) as cap:
                buffer = cap.capture_tournament(nav_url)
        except Exception as e:  # noqa: BLE001 — capture failure must not crash the run
            logger.warning("capture schedule failed for league=%s: %s", league, e)
            return []

        events = extract_tournament_events(buffer, ut_id)
        match_ids = finished_event_ids(events)
        logger.info(
            "Capture schedule league=%s season=%s: %d ut=%d events, %d finished.",
            league, season, len(events), ut_id, len(match_ids),
        )
        return match_ids

    def read_player_ratings(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-match player ratings (Opta scale 0.0–10.0) from the
        SofaScore REST API.

        Parameters
        ----------
        league : str
            soccerdata-style league key, e.g. ``"ENG-Premier League"``.
        season : int
            Season (any format ``read_schedule`` understands —
            ``2526`` / ``"2526"`` / ``2025``).
        match_ids : list[str] | None
            Optional explicit match list. When ``None`` we resolve
            finished matches from ``bronze.sofascore_schedule`` via
            soccerdata.
        limit : int | None
            Smoke-test cap on the number of matches to fetch.

        Returns
        -------
        pd.DataFrame
            Columns: ``match_id, player_id, team_side, rating, position,
            league, season, _ingested_at``. Empty frame on graceful
            fallback (caller logs ``R0.2B_FALLBACK``).
        """
        cols = [
            'match_id', 'player_id', 'team_side', 'rating', 'position',
            'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        if not match_ids:
            logger.warning(
                "No match_ids resolved for league=%s season=%s — "
                "ratings scrape skipped.",
                league, season,
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        if limit:
            match_ids = list(match_ids)[: int(limit)]

        logger.info(
            "Fetching SofaScore lineups for %d matches (league=%s season=%s)",
            len(match_ids), league, season,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        # Capture is expensive (browser nav + proxy bytes, ×3 internal retries
        # per match); 10 consecutive misses ≈ dead proxy / Turnstile not solved
        # — bail early to save proxy budget rather than grind all match_ids.
        max_consecutive = 10

        # #757: lineups now come from the Camoufox capture transport (the
        # tls_requests REST path is Turnstile-blocked). The generator owns one
        # warmed browser session for all matches — close it on early break.
        payloads = self._iter_lineup_payloads(match_ids)
        try:
            for idx, (mid, payload) in enumerate(payloads, start=1):
                if payload is None:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive:
                        logger.error(
                            "%s: %d consecutive lineup capture failures — aborting "
                            "scrape early to preserve proxy budget.",
                            R0_2B_FALLBACK_MARKER, consecutive_failures,
                        )
                        break
                    continue

                consecutive_failures = 0
                for side in ('home', 'away'):
                    all_rows.extend(self._flatten_lineup_side(
                        match_id=str(mid),
                        side=side,
                        side_payload=payload.get(side) or {},
                    ))

                if idx % 25 == 0:
                    logger.info("Lineups progress: %d/%d matches", idx, len(match_ids))
        finally:
            close = getattr(payloads, 'close', None)
            if callable(close):
                close()  # tear down the Camoufox session

        if not all_rows:
            logger.warning(
                "%s: zero rating rows materialised across %d match attempts.",
                R0_2B_FALLBACK_MARKER, len(match_ids),
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows, columns=[
            'match_id', 'player_id', 'team_side', 'rating', 'position',
        ])
        df['league'] = league
        # Match the slug used by the schedule writer (soccerdata short form
        # 'YYZZ', e.g. 2025 -> '2526'). Mismatch would split the partition
        # and break replace_partitions dedup — see issue #27.
        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_ratings'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d player-rating rows across %d unique matches",
            len(df), df['match_id'].nunique(),
        )
        return df

    # ------------------------------------------------------------------
    # #751 PR1 — consolidated per-match capture (one nav → ratings + eps)
    # ------------------------------------------------------------------

    def read_match_capture(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """ONE Camoufox capture pass per match → both ``player_ratings`` and
        ``event_player_stats`` frames (#751 PR1).

        Replaces two separate Turnstile-blocked passes with a single navigation
        per match: the captured ``/lineups`` payload yields ratings (via
        :meth:`_flatten_lineup_side`) AND per-player Opta stats (via
        :meth:`_flatten_event_player_stats_from_lineups`); ``team_id``/
        ``team_name`` come from the captured ``/event`` payload (best-effort —
        NULL when ``event`` wasn't captured on that nav).

        Returns ``{'player_ratings': df, 'event_player_stats': df}``; both empty
        on graceful fallback (caller emits ``R0.2B_FALLBACK``). Season slug is
        coerced to the soccerdata short form (``2526``) so the partition matches
        the schedule writer (#27).
        """
        ratings_cols = [
            'match_id', 'player_id', 'team_side', 'rating', 'position',
            'league', 'season',
        ]
        eps_cols = [
            'match_id', 'player_id', 'team_id', 'team_name', 'is_home',
            'position', 'position_specific', 'captain', 'substitute',
            'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        empty = {
            'player_ratings': pd.DataFrame(columns=ratings_cols + ['_ingested_at']),
            'event_player_stats': pd.DataFrame(columns=eps_cols + ['_ingested_at']),
        }

        if not match_ids:
            logger.warning(
                "No match_ids resolved for match_capture (league=%s season=%s).",
                league, season,
            )
            return empty

        if limit:
            match_ids = list(match_ids)[: int(limit)]

        logger.info(
            "match_capture: capturing %d matches (league=%s season=%s)",
            len(match_ids), league, season,
        )

        ratings_rows: List[Dict] = []
        eps_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 10  # ~dead proxy / Turnstile not solved — bail early.

        captures = self._iter_match_captures(match_ids, tabs=("Lineups",))
        try:
            for idx, (mid, endpoints) in enumerate(captures, start=1):
                lineups = (endpoints or {}).get('lineups')
                if lineups is None:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive:
                        logger.error(
                            "%s: %d consecutive lineup capture failures — "
                            "aborting match_capture early to preserve proxy budget.",
                            R0_2B_FALLBACK_MARKER, consecutive_failures,
                        )
                        break
                    continue

                consecutive_failures = 0
                event_payload = (endpoints or {}).get('event')
                for side in ('home', 'away'):
                    ratings_rows.extend(self._flatten_lineup_side(
                        match_id=str(mid),
                        side=side,
                        side_payload=lineups.get(side) or {},
                    ))
                eps_rows.extend(self._flatten_event_player_stats_from_lineups(
                    str(mid), lineups, event_payload,
                ))

                if idx % 25 == 0:
                    logger.info("match_capture progress: %d/%d matches",
                                idx, len(match_ids))
        finally:
            close = getattr(captures, 'close', None)
            if callable(close):
                close()  # tear down the Camoufox session

        out: Dict[str, pd.DataFrame] = {}

        if ratings_rows:
            rdf = pd.DataFrame(ratings_rows, columns=[
                'match_id', 'player_id', 'team_side', 'rating', 'position',
            ])
            rdf['league'] = league
            rdf['season'] = season_short
            rdf['_ingested_at'] = datetime.utcnow()
            rdf['_source'] = self.SOURCE_NAME
            rdf['_entity_type'] = 'player_ratings'
            rdf['_batch_id'] = self._batch_id
            out['player_ratings'] = rdf
        else:
            out['player_ratings'] = empty['player_ratings']

        if eps_rows:
            edf = pd.DataFrame(eps_rows)
            edf['league'] = league
            edf['season'] = season_short
            edf['_ingested_at'] = datetime.utcnow()
            edf['_source'] = self.SOURCE_NAME
            edf['_entity_type'] = 'event_player_stats'
            edf['_batch_id'] = self._batch_id
            out['event_player_stats'] = edf
        else:
            out['event_player_stats'] = empty['event_player_stats']

        if not ratings_rows and not eps_rows:
            logger.warning(
                "%s: match_capture materialised zero rows across %d match attempts.",
                R0_2B_FALLBACK_MARKER, len(match_ids),
            )

        logger.info(
            "match_capture: %d rating rows + %d eps rows across %d matches",
            len(out['player_ratings']), len(out['event_player_stats']),
            len(match_ids),
        )
        return out

    # ------------------------------------------------------------------
    # #22 event shotmap — per-shot xG / coords / situation / body part
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_shotmap(match_id: str, payload: dict) -> List[Dict]:
        """Project the ``shotmap`` block into one row per shot.

        Schema per row:
            match_id, shot_id, player_id, team_id, is_home, minute,
            added_time, period, situation, shot_type, body_part,
            outcome, x, y, goal_x, goal_y, xg, xgot
        """
        rows: List[Dict] = []
        if not isinstance(payload, dict):
            return rows

        shots = payload.get('shotmap') or []
        if not isinstance(shots, list):
            return rows

        def _f(v):
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def _i(v):
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        for shot in shots:
            if not isinstance(shot, dict):
                continue

            sid = shot.get('id')
            if sid is None:
                # Fall back to composite (match, time, player) so that
                # downstream PK stays unique even when SofaScore omits id.
                player = shot.get('player') or {}
                sid = (
                    f"{match_id}-"
                    f"{shot.get('time', 'NA')}-"
                    f"{player.get('id', 'NA')}-"
                    f"{shot.get('addedTime', 0)}"
                )
            shot_id_str = (
                str(int(sid)) if isinstance(sid, (int, float)) and not isinstance(sid, bool)
                else str(sid)
            )

            player = shot.get('player') or {}
            pid = player.get('id')
            player_id_str = (
                str(int(pid)) if isinstance(pid, (int, float)) and pid is not None
                else (str(pid) if pid is not None else None)
            )

            coords = shot.get('playerCoordinates') or {}
            goal = shot.get('goalMouthCoordinates') or {}

            rows.append({
                'match_id': str(match_id),
                'shot_id': shot_id_str,
                'player_id': player_id_str,
                'team_id': _i(shot.get('teamId') or (shot.get('team') or {}).get('id')),
                'is_home': bool(shot.get('isHome')) if shot.get('isHome') is not None else None,
                'minute': _i(shot.get('time')),
                'added_time': _i(shot.get('addedTime')),
                'period': _i(shot.get('reversedPeriodCount') or shot.get('period')),
                # SofaScore taxonomy: incidentType=goal/miss/save/post/block,
                # shotType=header/leftFoot/rightFoot/other,
                # bodyPart=head/leftFoot/rightFoot/other (richer than shotType
                # but not always populated), situation=open-play/corner/free-kick/penalty,
                # goalType (populated for incidentType=goal): regular/own/penalty.
                'shot_type': shot.get('shotType') or None,
                'situation': shot.get('situation') or None,
                'body_part': shot.get('bodyPart') or None,
                'outcome': shot.get('incidentType') or None,
                'goal_type': shot.get('goalType') or None,
                'x': _f(coords.get('x')),
                'y': _f(coords.get('y')),
                'goal_x': _f(goal.get('x')),
                'goal_y': _f(goal.get('y')),
                'xg': _f(shot.get('xg') if shot.get('xg') is not None else shot.get('expectedGoals')),
                'xgot': _f(shot.get('xgot') if shot.get('xgot') is not None else shot.get('expectedGoalsOnTarget')),
            })

        return rows

    def _fetch_shotmap_payload(
        self,
        event_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        url = f"{_SOFASCORE_API}{_SHOTMAP_PATH.format(event_id=event_id)}"
        return self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='shotmap',
            context={'event_id': event_id},
        )

    def read_shotmap(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-shot data (coords + xG + situation + body part) from
        ``/api/v1/event/{id}/shotmap`` for the given match ids.

        Returns an empty DataFrame on graceful fallback (runner emits
        ``R0.2B_FALLBACK`` and exits with code 2).
        """
        cols = [
            'match_id', 'shot_id', 'player_id', 'team_id', 'is_home',
            'minute', 'added_time', 'period', 'shot_type', 'situation',
            'body_part', 'outcome', 'goal_type', 'x', 'y', 'goal_x',
            'goal_y', 'xg', 'xgot', 'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        if not match_ids:
            logger.warning(
                "No match_ids resolved for shotmap (league=%s season=%s).",
                league, season,
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        if limit:
            match_ids = list(match_ids)[: int(limit)]

        logger.info(
            "Fetching SofaScore shotmap for %d matches (league=%s season=%s)",
            len(match_ids), league, season,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 100

        for idx, mid in enumerate(match_ids, start=1):
            payload = self._fetch_shotmap_payload(str(mid))
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive:
                    logger.error(
                        "%s: %d consecutive shotmap fetch failures — aborting.",
                        R0_2B_FALLBACK_MARKER, consecutive_failures,
                    )
                    break
                continue

            consecutive_failures = 0
            all_rows.extend(self._flatten_shotmap(str(mid), payload))

            if idx % 25 == 0:
                logger.info("Shotmap progress: %d/%d matches", idx, len(match_ids))

        if not all_rows:
            logger.warning(
                "%s: zero shotmap rows materialised across %d matches.",
                R0_2B_FALLBACK_MARKER, len(match_ids),
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows)
        df['league'] = league

        # Match the slug used by the schedule writer (soccerdata short form
        # 'YYZZ', e.g. 2025 -> '2526'). Mismatch would split the partition
        # and break replace_partitions dedup — see issue #27.
        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'event_shotmap'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d shot rows across %d unique matches",
            len(df), df['match_id'].nunique(),
        )
        return df

    # ------------------------------------------------------------------
    # #21 event_player_stats — per-match per-player Opta-rich metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_event_player_stats(
        match_id: str,
        player_id: str,
        payload: dict,
    ) -> Optional[Dict]:
        """Project the per-(match, player) ``statistics`` block into a
        single flat row.

        Schema per row: ``match_id, player_id, team_id, is_home,
        position, position_specific, captain, substitute, <40+ snake_case
        Opta metrics>``. Unknown SofaScore keys auto-flatten through
        :func:`_camel_to_snake`. We tag the entity_type / source / batch
        downstream of this helper.
        """
        if not isinstance(payload, dict):
            return None

        player = payload.get('player') or {}
        team = payload.get('team') or {}
        stats = payload.get('statistics') or {}
        extra = payload.get('extra') or {}

        row: Dict = {
            'match_id': str(match_id),
            'player_id': str(player_id),
            'team_id': team.get('id'),
            'team_name': team.get('name'),
            'is_home': bool(extra.get('isHome')) if extra.get('isHome') is not None else None,
            'position': payload.get('position') or player.get('position') or None,
            'position_specific': stats.get('position') or None,
            'captain': bool(extra.get('captain')) if extra.get('captain') is not None else None,
            'substitute': bool(extra.get('substitute')) if extra.get('substitute') is not None else None,
        }

        # Auto-flatten every numeric/scalar statistic. Drop the redundant
        # `position` re-export (already projected above) and the rating
        # alias we surface explicitly below.
        for raw_key, raw_val in stats.items():
            if raw_key == 'position':
                continue
            snake = _camel_to_snake(str(raw_key))
            if snake in row:
                # Don't overwrite anchor columns (player_id, team_id, ...).
                continue
            row[snake] = _coerce_scalar(raw_val)

        # Convenience aliases — rating is the most-queried metric.
        if 'rating' not in row and stats.get('rating') is not None:
            try:
                row['rating'] = float(stats['rating'])
            except (TypeError, ValueError):
                row['rating'] = None

        return row

    @staticmethod
    def _flatten_event_player_stats_from_lineups(
        match_id: str,
        lineups_payload: dict,
        event_payload: Optional[dict] = None,
    ) -> List[Dict]:
        """Project the captured ``/lineups`` payload into per-(match, player)
        Opta-stat rows — the Camoufox-capture replacement for the dead
        ``/event/{id}/player/{pid}/statistics`` per-player calls (#751).

        Live-verified 2026-06-22 (#751): each ``/lineups`` player entry carries
        the full per-match ``statistics`` block (33 Opta metrics) PLUS the
        anchors the dedicated endpoint omitted — ``is_home`` (from the side),
        ``captain``/``substitute``/``position`` (from the entry). So unlike the
        per-player path (which needed a /lineups overlay to back-fill those NULL
        anchors, #301), this single payload populates them directly.

        ``team_id``/``team_name`` are absent from ``/lineups``; they come from
        the captured ``event_payload`` (``homeTeam``/``awayTeam``). A ``None``
        event payload leaves them NULL. Stat keys auto-flatten through
        ``_camel_to_snake`` + ``_coerce_scalar`` — identical rules to
        :meth:`_flatten_event_player_stats`, so the Bronze schema is unchanged.
        """
        rows: List[Dict] = []
        if not isinstance(lineups_payload, dict):
            return rows

        ev = event_payload if isinstance(event_payload, dict) else {}
        team_by_side = {
            'home': ev.get('homeTeam') or {},
            'away': ev.get('awayTeam') or {},
        }

        for side in ('home', 'away'):
            side_payload = lineups_payload.get(side) or {}
            if not isinstance(side_payload, dict):
                continue
            team = team_by_side.get(side) or {}
            for entry in side_payload.get('players', []) or []:
                if not isinstance(entry, dict):
                    continue
                player = entry.get('player') or {}
                pid = player.get('id')
                if pid is None:
                    continue
                stats = entry.get('statistics') or {}

                player_id_str = (
                    str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
                )
                row: Dict = {
                    'match_id': str(match_id),
                    'player_id': player_id_str,
                    'team_id': team.get('id'),
                    'team_name': team.get('name'),
                    'is_home': side == 'home',
                    'position': entry.get('position') or player.get('position') or None,
                    'position_specific': entry.get('position') or None,
                    'captain': bool(entry.get('captain')),
                    'substitute': bool(entry.get('substitute')),
                }

                # Auto-flatten every numeric/scalar statistic (mirrors
                # _flatten_event_player_stats). Skip the `position` re-export
                # and never overwrite an anchor column.
                for raw_key, raw_val in stats.items():
                    if raw_key == 'position':
                        continue
                    snake = _camel_to_snake(str(raw_key))
                    if snake in row:
                        continue
                    row[snake] = _coerce_scalar(raw_val)

                if 'rating' not in row and stats.get('rating') is not None:
                    try:
                        row['rating'] = float(stats['rating'])
                    except (TypeError, ValueError):
                        row['rating'] = None

                rows.append(row)

        return rows

    def _fetch_event_player_stats_payload(
        self,
        event_id: str,
        player_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        url = f"{_SOFASCORE_API}{_EVENT_PLAYER_STATS_PATH.format(event_id=event_id, player_id=player_id)}"
        return self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='event_player_stats',
            context={'event_id': event_id, 'player_id': player_id},
        )

    def _resolve_match_players_from_bronze(
        self,
        league: str,
        season_short: str,
    ) -> Dict[str, List[str]]:
        """Group player_ids by match_id from bronze.sofascore_player_ratings.

        Returns ``{match_id: [player_id, ...]}``. Empty dict if Trino
        unavailable or the ratings partition is missing — caller emits
        the R0.2B_FALLBACK marker.
        """
        try:
            import os
            import trino
            import trino.auth as trino_auth
        except ImportError as e:  # pragma: no cover - import guard
            logger.error("trino client unavailable: %s", e)
            return {}

        user = os.environ.get('TRINO_USER', 'airflow')
        password = os.environ.get('TRINO_PASSWORD')

        try:
            if password:
                conn = trino.dbapi.connect(
                    host=os.environ.get('TRINO_HOST', 'trino'),
                    port=int(os.environ.get('TRINO_PORT', 8443)),
                    user=user,
                    catalog='iceberg',
                    http_scheme='https',
                    auth=trino_auth.BasicAuthentication(user, password),
                    verify=False,
                )
            else:
                conn = trino.dbapi.connect(
                    host=os.environ.get('TRINO_HOST', 'trino'),
                    port=int(os.environ.get('TRINO_PORT', 8080)),
                    user=user,
                    catalog='iceberg',
                )

            cur = conn.cursor()
            cur.execute(
                "SELECT match_id, player_id "
                "FROM iceberg.bronze.sofascore_player_ratings "
                "WHERE league = ? AND CAST(season AS varchar) = ?",
                (league, season_short),
            )
            rows = cur.fetchall()
        except Exception as e:
            logger.warning(
                "Could not resolve (match, player) pairs from bronze: %s", e,
            )
            return {}

        grouped: Dict[str, List[str]] = {}
        for mid, pid in rows:
            if mid is None or pid is None:
                continue
            grouped.setdefault(str(mid), []).append(str(pid))
        return grouped

    def read_event_player_stats(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        player_ids_by_match: Optional[Dict[str, List[str]]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-(match, player) Opta-rich stats from
        ``/api/v1/event/{id}/player/{pid}/statistics``.

        Player ids are resolved from ``bronze.sofascore_player_ratings``
        (players who actually entered the pitch) unless explicitly
        provided — calling SofaScore with random pids returns 404 and
        wastes the proxy budget.
        """
        anchor_cols = [
            'match_id', 'player_id', 'team_id', 'team_name', 'is_home',
            'position', 'position_specific', 'captain', 'substitute',
            'league', 'season',
        ]

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        if player_ids_by_match is None:
            player_ids_by_match = self._resolve_match_players_from_bronze(
                league, season_short,
            )

        if not player_ids_by_match:
            logger.warning(
                "No (match, player) pairs in bronze.sofascore_player_ratings "
                "for league=%s season=%s — event_player_stats skipped.",
                league, season_short,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if match_ids is not None:
            wanted = {str(m) for m in match_ids}
            player_ids_by_match = {
                m: p for m, p in player_ids_by_match.items() if m in wanted
            }

        if limit:
            # Cap by *match count* (not request count) so smoke runs stay
            # predictable. A single match ≈ 25 played players ≈ 25 HTTP
            # calls; rate-limited to 20 req/min → ~1.25 min/match.
            wanted = list(player_ids_by_match.keys())[: int(limit)]
            player_ids_by_match = {m: player_ids_by_match[m] for m in wanted}

        total_calls = sum(len(p) for p in player_ids_by_match.values())
        logger.info(
            "Fetching SofaScore event_player_stats: %d matches, %d "
            "(match, player) calls (league=%s season=%s)",
            len(player_ids_by_match), total_calls, league, season,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 200

        call_idx = 0
        lineup_misses = 0
        for mid, pids in player_ids_by_match.items():
            # The statistics endpoint returns `extra: null` and no
            # `statistics.position`, so is_home/captain/substitute/
            # position_specific must be back-filled from /lineups (#301).
            # One extra fetch per match (~2.5% overhead vs the per-player
            # stat calls). A miss leaves those anchors NULL — graceful,
            # and does NOT count toward the stat-endpoint breaker below.
            lineup_payload = self._fetch_lineup_payload(str(mid))
            if lineup_payload is None:
                lineup_misses += 1
            overlay_lookup = self._build_lineup_overlay_lookup(lineup_payload or {})
            for pid in pids:
                call_idx += 1
                payload = self._fetch_event_player_stats_payload(str(mid), str(pid))
                if payload is None:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive:
                        logger.error(
                            "%s: %d consecutive event_player_stats failures — "
                            "aborting early.",
                            R0_2B_FALLBACK_MARKER, consecutive_failures,
                        )
                        break
                    continue

                consecutive_failures = 0
                row = self._flatten_event_player_stats(str(mid), str(pid), payload)
                if row is not None:
                    self._apply_lineup_overlay(row, overlay_lookup.get(str(pid)))
                    all_rows.append(row)

                if call_idx % 100 == 0:
                    logger.info(
                        "event_player_stats progress: %d/%d calls",
                        call_idx, total_calls,
                    )
            else:
                continue
            break  # propagate inner break on circuit-breaker trip

        if not all_rows:
            logger.warning(
                "%s: zero event_player_stats rows materialised (calls=%d).",
                R0_2B_FALLBACK_MARKER, total_calls,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows)
        df['league'] = league
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'event_player_stats'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d event_player_stats rows across %d unique matches",
            len(df), df['match_id'].nunique(),
        )
        if lineup_misses:
            logger.warning(
                "Lineup overlay missing for %d/%d matches — those rows keep "
                "NULL is_home/captain/substitute/position_specific (#301).",
                lineup_misses, len(player_ids_by_match),
            )
        return df

    # ------------------------------------------------------------------
    # #25 match_stats — per-(period, group, stat) team-level metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_match_stats(match_id: str, payload: dict) -> List[Dict]:
        """Project ``/event/{id}/statistics`` into long-form rows.

        SofaScore returns ``statistics: [{period, groups: [{groupName,
        statisticsItems: [...]}, ...]}, ...]`` — we emit one row per
        ``(match_id, period, stat_group, stat_name)`` so Silver can
        pivot without unnesting JSON. Both raw text values
        (``home``/``away`` — e.g. ``"55%"``, ``"3 (1)"``) and numeric
        canonicals (``homeValue``/``awayValue``) are surfaced.
        """
        rows: List[Dict] = []
        if not isinstance(payload, dict):
            return rows

        periods = payload.get('statistics') or []
        if not isinstance(periods, list):
            return rows

        def _f(v):
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        for period_block in periods:
            if not isinstance(period_block, dict):
                continue
            period = period_block.get('period') or 'ALL'

            for group_block in (period_block.get('groups') or []):
                if not isinstance(group_block, dict):
                    continue
                stat_group = group_block.get('groupName')

                for item in (group_block.get('statisticsItems') or []):
                    if not isinstance(item, dict):
                        continue
                    rows.append({
                        'match_id': str(match_id),
                        'period': str(period),
                        'stat_group': stat_group,
                        'stat_name': item.get('name'),
                        'stat_key': item.get('key') or item.get('statisticsType'),
                        'home_value': _f(item.get('homeValue')),
                        'away_value': _f(item.get('awayValue')),
                        'home_text': (
                            str(item.get('home')) if item.get('home') is not None else None
                        ),
                        'away_text': (
                            str(item.get('away')) if item.get('away') is not None else None
                        ),
                        'compare_code': item.get('compareCode'),
                        'value_type': item.get('valueType'),
                    })

        return rows

    def _fetch_match_stats_payload(
        self,
        event_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        url = f"{_SOFASCORE_API}{_MATCH_STATS_PATH.format(event_id=event_id)}"
        return self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='match_stats',
            context={'event_id': event_id},
        )

    def read_match_stats(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-match team-level statistics from
        ``/api/v1/event/{id}/statistics`` and emit long-form rows.

        Long-form (one row per (match, period, group, stat)) is chosen
        over wide-form because SofaScore evolves its stat catalogue
        without notice — adding a new metric must not require a Bronze
        schema migration.
        """
        cols = [
            'match_id', 'period', 'stat_group', 'stat_name', 'stat_key',
            'home_value', 'away_value', 'home_text', 'away_text',
            'compare_code', 'value_type', 'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        if not match_ids:
            logger.warning(
                "No match_ids resolved for match_stats (league=%s season=%s).",
                league, season,
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        if limit:
            match_ids = list(match_ids)[: int(limit)]

        logger.info(
            "Fetching SofaScore match_stats for %d matches (league=%s season=%s)",
            len(match_ids), league, season,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 100

        for idx, mid in enumerate(match_ids, start=1):
            payload = self._fetch_match_stats_payload(str(mid))
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive:
                    logger.error(
                        "%s: %d consecutive match_stats failures — aborting.",
                        R0_2B_FALLBACK_MARKER, consecutive_failures,
                    )
                    break
                continue

            consecutive_failures = 0
            all_rows.extend(self._flatten_match_stats(str(mid), payload))

            if idx % 25 == 0:
                logger.info("match_stats progress: %d/%d matches", idx, len(match_ids))

        if not all_rows:
            logger.warning(
                "%s: zero match_stats rows materialised across %d matches.",
                R0_2B_FALLBACK_MARKER, len(match_ids),
            )
            return pd.DataFrame(columns=cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows)
        df['league'] = league

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'match_stats'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d match_stats rows across %d unique matches",
            len(df), df['match_id'].nunique(),
        )
        return df

    # ------------------------------------------------------------------
    # #24 player_season_stats — season-aggregate Opta metrics per player
    # ------------------------------------------------------------------

    def _resolve_unique_tournament_id(self, league: str) -> Optional[int]:
        return SOFASCORE_TOURNAMENT_MAP.get(league)

    def _resolve_season_id(
        self,
        league: str,
        season: int,
    ) -> Optional[Tuple[int, int]]:
        """Map ``(league, season)`` to SofaScore's
        ``(unique_tournament_id, season_id)``.

        Cached on the scraper instance — one HTTP lookup per (league,
        season) pair. Returns ``None`` when SofaScore doesn't expose a
        season matching the soccerdata short slug (e.g. preseason gap).
        """
        if not hasattr(self, '_season_id_cache'):
            self._season_id_cache: Dict[Tuple[str, str], Tuple[int, int]] = {}

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        cache_key = (league, season_short)
        if cache_key in self._season_id_cache:
            return self._season_id_cache[cache_key]

        ut_id = self._resolve_unique_tournament_id(league)
        if ut_id is None:
            logger.warning(
                "No SOFASCORE_TOURNAMENT_MAP entry for league=%s — "
                "season_id resolution skipped.", league,
            )
            return None

        url = f"{_SOFASCORE_API}{_TOURNAMENT_SEASONS_PATH.format(ut_id=ut_id)}"
        payload = self._fetch_json_endpoint(
            url=url,
            max_attempts=3,
            label='tournament_seasons',
            context={'league': league, 'ut_id': ut_id},
        )
        if not payload:
            return None

        # The "year" field is the official slug, e.g. "25/26".
        # soccerdata short slug "2526" maps via the leading 4 digits.
        if len(season_short) == 4:
            target_year = f"{season_short[0:2]}/{season_short[2:4]}"
        else:
            target_year = season_short

        for entry in (payload.get('seasons') or []):
            if not isinstance(entry, dict):
                continue
            if entry.get('year') == target_year:
                sid = entry.get('id')
                if sid is None:
                    continue
                pair = (int(ut_id), int(sid))
                self._season_id_cache[cache_key] = pair
                return pair

        logger.warning(
            "SofaScore season for league=%s year=%s not found "
            "(ut_id=%d).", league, target_year, ut_id,
        )
        return None

    def _resolve_player_ids_from_bronze(
        self,
        league: str,
        season_short: str,
        limit: Optional[int] = None,
    ) -> List[str]:
        """DISTINCT player_id from bronze.sofascore_player_ratings."""
        try:
            import os
            import trino
            import trino.auth as trino_auth
        except ImportError as e:  # pragma: no cover
            logger.error("trino client unavailable: %s", e)
            return []

        user = os.environ.get('TRINO_USER', 'airflow')
        password = os.environ.get('TRINO_PASSWORD')

        try:
            if password:
                conn = trino.dbapi.connect(
                    host=os.environ.get('TRINO_HOST', 'trino'),
                    port=int(os.environ.get('TRINO_PORT', 8443)),
                    user=user,
                    catalog='iceberg',
                    http_scheme='https',
                    auth=trino_auth.BasicAuthentication(user, password),
                    verify=False,
                )
            else:
                conn = trino.dbapi.connect(
                    host=os.environ.get('TRINO_HOST', 'trino'),
                    port=int(os.environ.get('TRINO_PORT', 8080)),
                    user=user,
                    catalog='iceberg',
                )

            cur = conn.cursor()
            sql = (
                "SELECT DISTINCT CAST(player_id AS varchar) "
                "FROM iceberg.bronze.sofascore_player_ratings "
                "WHERE league = ? AND CAST(season AS varchar) = ? "
                "  AND rating IS NOT NULL"
            )
            if limit:
                sql = sql + f" LIMIT {int(limit)}"
            cur.execute(sql, (league, season_short))
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
        except Exception as e:
            logger.warning(
                "Could not resolve player_ids from bronze: %s", e,
            )
            return []

    @staticmethod
    def _flatten_player_season_stats(
        player_id: str,
        ut_id: int,
        season_id: int,
        payload: dict,
    ) -> Optional[Dict]:
        """Project the per-(player, season) season-aggregate stats."""
        if not isinstance(payload, dict):
            return None

        team = payload.get('team') or {}
        stats = payload.get('statistics') or {}
        if not isinstance(stats, dict):
            stats = {}

        row: Dict = {
            'player_id': str(player_id),
            'unique_tournament_id': int(ut_id),
            'sofascore_season_id': int(season_id),
            'team_id': team.get('id'),
            'team_name': team.get('name'),
        }
        for raw_key, raw_val in stats.items():
            if not isinstance(raw_key, str):
                continue
            col = _camel_to_snake(raw_key)
            if col in row:
                col = f'stat_{col}'
            row[col] = _coerce_scalar(raw_val)
        return row

    def _fetch_player_season_stats_payload(
        self,
        player_id: str,
        ut_id: int,
        season_id: int,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        url = f"{_SOFASCORE_API}{_PLAYER_SEASON_STATS_PATH.format(player_id=player_id, ut_id=ut_id, season_id=season_id)}"
        return self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='player_season_stats',
            context={
                'player_id': player_id,
                'ut_id': ut_id,
                'season_id': season_id,
            },
        )

    def read_player_season_stats(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-(player, season) Opta-aggregate stats from
        ``/api/v1/player/{pid}/unique-tournament/{ut}/season/{s}/
        statistics/overall``.
        """
        anchor_cols = [
            'player_id', 'unique_tournament_id', 'sofascore_season_id',
            'team_id', 'team_name', 'league', 'season',
        ]

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        season_ids = self._resolve_season_id(league, season)
        if season_ids is None:
            logger.warning(
                "%s: cannot resolve SofaScore season_id for league=%s "
                "season=%s — player_season_stats skipped.",
                R0_2B_FALLBACK_MARKER, league, season,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        ut_id, sofa_season_id = season_ids

        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit,
            )

        if not player_ids:
            logger.warning(
                "%s: no player_ids resolved for league=%s season=%s.",
                R0_2B_FALLBACK_MARKER, league, season_short,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if limit:
            player_ids = list(player_ids)[: int(limit)]

        logger.info(
            "Fetching SofaScore player_season_stats for %d players "
            "(league=%s season=%s ut=%d sid=%d)",
            len(player_ids), league, season, ut_id, sofa_season_id,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 100

        for idx, pid in enumerate(player_ids, start=1):
            payload = self._fetch_player_season_stats_payload(
                str(pid), ut_id, sofa_season_id,
            )
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive:
                    logger.error(
                        "%s: %d consecutive player_season_stats failures.",
                        R0_2B_FALLBACK_MARKER, consecutive_failures,
                    )
                    break
                continue

            consecutive_failures = 0
            row = self._flatten_player_season_stats(
                str(pid), ut_id, sofa_season_id, payload,
            )
            if row is not None:
                all_rows.append(row)

            if idx % 100 == 0:
                logger.info(
                    "player_season_stats progress: %d/%d players",
                    idx, len(player_ids),
                )

        if not all_rows:
            logger.warning(
                "%s: zero player_season_stats rows materialised across "
                "%d players.",
                R0_2B_FALLBACK_MARKER, len(player_ids),
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows)
        df['league'] = league
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_season_stats'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d player_season_stats rows for %d players",
            len(df), df['player_id'].nunique(),
        )
        return df

    # ------------------------------------------------------------------
    # #23 player_profile — snapshot (height, foot, dob, nationality, ...)
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_player_profile(payload: dict) -> Optional[Dict]:
        """Project ``/player/{id}`` payload into a snapshot row.

        SofaScore wraps the relevant data under a top-level ``player``
        key — we project a fixed set of identity / biographical fields.
        Unlike the stats-flatteners we don't auto-flatten the rest of
        the player object: it contains marketing fields (userCount,
        retiredStatus) we don't need in Bronze.
        """
        if not isinstance(payload, dict):
            return None

        player = payload.get('player')
        if not isinstance(player, dict):
            return None

        pid = player.get('id')
        if pid is None:
            return None

        dob_ts = player.get('dateOfBirthTimestamp')
        dob = None
        if isinstance(dob_ts, (int, float)) and dob_ts > 0:
            try:
                dob = datetime.utcfromtimestamp(int(dob_ts)).date().isoformat()
            except (OverflowError, OSError, ValueError):
                dob = None

        nationality = player.get('nationality')
        country = player.get('country') or {}
        if not nationality and isinstance(country, dict):
            nationality = country.get('name')

        team = player.get('team') or {}

        return {
            'player_id': str(int(pid)) if isinstance(pid, (int, float)) else str(pid),
            'name': player.get('name'),
            'short_name': player.get('shortName'),
            'slug': player.get('slug'),
            'position': player.get('position'),
            'jersey_number': player.get('jerseyNumber'),
            'shirt_number': player.get('shirtNumber'),
            'height_cm': player.get('height'),
            'preferred_foot': player.get('preferredFoot'),
            'date_of_birth': dob,
            'nationality': nationality,
            'country_code': (country or {}).get('alpha2') if isinstance(country, dict) else None,
            'current_team_id': team.get('id'),
            'current_team_name': team.get('name'),
            'retired': bool(player.get('retired')) if player.get('retired') is not None else None,
        }

    def _fetch_player_profile_payload(
        self,
        player_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        url = f"{_SOFASCORE_API}{_PLAYER_PROFILE_PATH.format(player_id=player_id)}"
        return self._fetch_json_endpoint(
            url=url,
            max_attempts=max_attempts,
            label='player_profile',
            context={'player_id': player_id},
        )

    def read_player_profile(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch per-player biographical snapshot from
        ``/api/v1/player/{id}``.

        Snapshot grain: 1 row per (player_id) per (league, season)
        partition. Cross-source validation against FotMob T4
        ``gold.dim_player_attributes`` lives in Silver, not here.
        """
        anchor_cols = [
            'player_id', 'name', 'short_name', 'slug', 'position',
            'jersey_number', 'shirt_number', 'height_cm', 'preferred_foot',
            'date_of_birth', 'nationality', 'country_code',
            'current_team_id', 'current_team_name', 'retired',
            'league', 'season',
        ]

        season_str = str(season)
        if len(season_str) == 4 and season_str.isdigit():
            season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
        else:
            season_short = season_str

        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit,
            )

        if not player_ids:
            logger.warning(
                "%s: no player_ids resolved for player_profile "
                "(league=%s season=%s).",
                R0_2B_FALLBACK_MARKER, league, season_short,
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        if limit:
            player_ids = list(player_ids)[: int(limit)]

        logger.info(
            "Fetching SofaScore player_profile for %d players (league=%s season=%s)",
            len(player_ids), league, season,
        )

        all_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 100

        for idx, pid in enumerate(player_ids, start=1):
            payload = self._fetch_player_profile_payload(str(pid))
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive:
                    logger.error(
                        "%s: %d consecutive player_profile failures.",
                        R0_2B_FALLBACK_MARKER, consecutive_failures,
                    )
                    break
                continue

            consecutive_failures = 0
            row = self._flatten_player_profile(payload)
            if row is not None:
                all_rows.append(row)

            if idx % 100 == 0:
                logger.info(
                    "player_profile progress: %d/%d players",
                    idx, len(player_ids),
                )

        if not all_rows:
            logger.warning(
                "%s: zero player_profile rows materialised across %d players.",
                R0_2B_FALLBACK_MARKER, len(player_ids),
            )
            return pd.DataFrame(columns=anchor_cols + ['_ingested_at'])

        df = pd.DataFrame(all_rows)
        df['league'] = league
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_profile'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d player_profile rows for %d players",
            len(df), df['player_id'].nunique(),
        )
        return df

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'schedule': table_path}
        return {}

    def scrape_league_table(self) -> Dict[str, str]:
        """Scrape league table (standings)."""
        df = self.read_league_table()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_league_table',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            return {'league_table': table_path}
        return {}

    def scrape_team_stats(self) -> Dict[str, str]:
        """Scrape team stats (alias for league table)."""
        return self.scrape_league_table()

    def scrape_player_ratings(
        self,
        league: Optional[str] = None,
        season: Optional[int] = None,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, str]:
        """Scrape per-match player ratings via the lineups REST endpoint.

        Bronze layout: ``iceberg.bronze.sofascore_player_ratings`` is
        partitioned by ``(league, season)``. The daily DAG passes the
        full set of finished matches for the season, so we pass
        ``replace_partitions=['league', 'season']`` to replace each
        partition wholesale and avoid append-only drift — see
        ``memory/feedback_replace_partitions_required.md``.
        """
        target_league = league or (self.leagues[0] if self.leagues else None)
        target_season = season if season is not None else (
            self.seasons[0] if self.seasons else None
        )
        if not target_league or target_season is None:
            logger.error(
                "scrape_player_ratings: league/season unresolved — "
                "leagues=%s seasons=%s", self.leagues, self.seasons,
            )
            return {}

        df = self.read_player_ratings(
            league=target_league,
            season=int(target_season),
            match_ids=match_ids,
            limit=limit,
        )
        if df is None or df.empty:
            return {}

        table_path = self.save_to_iceberg(
            df=df,
            table_name='sofascore_player_ratings',
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
        )
        return {'player_ratings': table_path}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all SofaScore data.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting SofaScore scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Scrape schedule
        schedule_results = self.scrape_schedule()
        results.update(schedule_results)

        # Scrape league table (standings)
        table_results = self.scrape_league_table()
        results.update(table_results)

        logger.info(f"SofaScore scrape complete: {list(results.keys())}")
        return results
