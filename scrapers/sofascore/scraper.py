"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import logging
import os
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper


# Bronze-flatten helpers live in a lightweight stdlib-only module so the
# capture layer (camoufox_capture) can reuse them without importing this heavy
# module (#840). Re-exported here for existing callers/tests.
from scrapers.sofascore._flatten import (  # noqa: E402
    _MAX_FLATTEN_DEPTH,
    _auto_flatten,
    _camel_to_snake,
    _coerce_scalar,
)

logger = logging.getLogger(__name__)


# SofaScore public REST API
_SOFASCORE_API = "https://api.sofascore.com/api/v1"
_LINEUPS_PATH = "/event/{event_id}/lineups"
_SHOTMAP_PATH = "/event/{event_id}/shotmap"
_EVENT_PLAYER_STATS_PATH = "/event/{event_id}/player/{player_id}/statistics"
_MATCH_STATS_PATH = "/event/{event_id}/statistics"


# SofaScore "unique-tournament" id per soccerdata league key. Discovered
# via `/api/v1/unique-tournament/{id}/seasons` probes during issue #19;
# stable since at least 2024. Missing league → runtime fallback lookup.
SOFASCORE_TOURNAMENT_MAP: Dict[str, int] = {
    'ENG-Premier League': 17,
    'ESP-La Liga': 8,
    'GER-Bundesliga': 35,
    'ITA-Serie A': 23,
    'FRA-Ligue 1': 34,
    'INT-World Cup': 16,
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
    'INT-World Cup': 'football/world/world-championship',
}

# R0.2b — graceful-fallback marker emitted when the lineups endpoint
# is structurally unavailable (HTTP 403 / quota empty / repeated timeouts).
# Downstream (E4.4 schema-stub path) keys off this marker to keep the
# Gold layer building without ratings instead of failing the DAG.
R0_2B_FALLBACK_MARKER = "R0.2B_FALLBACK"


def _season_to_short(season) -> str:
    """Normalize a season token to soccerdata's short 'YYZZ' form.

    Mirrors ``scrapers/whoscored/scraper.py::_season_to_soccerdata_str``:
    already-short tokens pass through ('2526' -> '2526'; the ambiguous
    '2021' resolves to the 20/21 season, like soccerdata), year-start
    values convert (2024 -> '2425', 1999 -> '9900'). The old inline
    conversion mapped '2526' -> '2627' (a nonexistent season), silently
    no-op'ing scrapes triggered with the documented short form.
    Non-4-digit tokens pass through unchanged (legacy behaviour of the
    inline ``else`` branch this helper replaces).
    """
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        return s
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return s
    if s[2:] == "99":
        return "9900"
    return s[-2:] + f"{(int(s[-2:]) + 1) % 100:02d}"


def _is_single_year(league: str, season) -> bool:
    """True when (league, season) is a single_year competition per
    ``competitions.yaml`` (INT-World Cup 2026, #913). Delegates to the shared
    scraper helper (#920 Phase 3 — one implementation for all scrapers)."""
    from scrapers.utils.competition_format import is_single_year
    return is_single_year(league, season)


def _season_label(league: str, season) -> str:
    """Bronze ``season`` partition label for (league, season).

    Club leagues use the soccerdata short form (``'2526'``); single_year
    competitions use the literal year (``'2026'`` — INT-World Cup, #913).
    The label MUST match the schedule writer, else ``replace_partitions``
    dedup splits the partition (#27) — ``_season_to_short(2026)`` would
    mislabel WC rows as ``'2627'``.
    """
    if _is_single_year(league, season):
        return str(int(season))
    return _season_to_short(season)


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
    # Camoufox capture attempts per league (schedule / league_table). Each
    # attempt gets a FRESH residential proxy — the weekend top-5 backfill lost
    # whole league-seasons to single transient proxy failures (#879).
    _CAPTURE_ATTEMPTS = 3

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None
        # Residential-proxy traffic audit (#789 + #879). Passive:
        # - response-body bytes of the tls_requests REST path
        #   (`_fetch_json_endpoint`), keyed by host;
        # - rx+tx bytes of each Camoufox capture session (schedule/standings/
        #   lineups), folded in at session teardown via `_camoufox_session`.
        # Surfaced via get_traffic_stats().
        self._proxy_bytes: int = 0
        self._proxy_bytes_by_host: Dict[str, int] = defaultdict(int)
        self._camoufox_bytes: int = 0

    def get_traffic_stats(self) -> Dict:
        """Residential-proxy bytes seen this run (#789 tls path + #879 Camoufox).

        ``proxy_response_bytes/mb`` cover BOTH transports; ``camoufox_bytes/mb``
        break the browser share out. Units caveat: the Camoufox share counts
        rx+tx including headers (``request.sizes()``), the tls share counts
        response bodies only — so the total is still a slight lower bound of
        billable traffic. Shape mirrors ``FlareSolverrClient.get_traffic_stats``
        so ``utils.proxy_traffic`` can consume it uniformly.
        """
        by_host = sorted(
            self._proxy_bytes_by_host.items(), key=lambda kv: -kv[1]
        )
        total = self._proxy_bytes + self._camoufox_bytes
        return {
            'proxy_response_bytes': total,
            'proxy_response_mb': round(total / 1024 / 1024, 4),
            'camoufox_bytes': self._camoufox_bytes,
            'camoufox_mb': round(self._camoufox_bytes / 1024 / 1024, 4),
            'requests': int(self._stats.get('requests', 0)),
            'top_traffic_urls': [
                {
                    'url': host,
                    'bytes': nbytes,
                    'mb': round(nbytes / 1024 / 1024, 4),
                }
                for host, nbytes in by_host[:10]
            ],
        }

    @contextmanager
    def _camoufox_session(self, proxy: Optional[dict]):
        """Open a ``SofascoreCamoufoxCapture`` session and fold its rx+tx bytes
        into the traffic audit on teardown (#879).

        The accumulation lives in ``finally`` (not after the ``with``) because
        generator callers (`_iter_match_captures`/`_iter_player_captures`) can
        be ``.close()``d mid-iteration — GeneratorExit skips any code after the
        block. The lazy import keeps the existing test seam
        (``patch('scrapers.sofascore.camoufox_capture.SofascoreCamoufoxCapture')``)
        working.
        """
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture

        cap = None
        try:
            with SofascoreCamoufoxCapture(proxy=proxy) as cap:
                yield cap
        finally:
            if cap is not None:
                try:
                    nbytes = int(getattr(cap, '_bytes_total', 0) or 0)
                except (TypeError, ValueError):
                    nbytes = 0
                if nbytes > 0:
                    self._camoufox_bytes += nbytes
                    self._proxy_bytes_by_host['camoufox:www.sofascore.com'] += nbytes

    def _record_proxy_bytes(self, url: str, resp) -> None:
        """Accumulate response-body bytes for the residential-proxy audit (#789).

        Never raises — a passive traffic counter must not break a scrape.
        """
        try:
            nbytes = len(resp.content or b"")
        except Exception:  # noqa: BLE001 — counter must never break the fetch
            return
        self._proxy_bytes += nbytes
        self._proxy_bytes_by_host[urlsplit(url).netloc or url] += nbytes

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
        """Read the match schedule + results via Camoufox capture (#761).

        The soccerdata schedule reader is Turnstile-blocked (#757), so for each
        league we navigate its SofaScore tournament page, let the SPA fire its
        ``/events/{round,last,next}`` XHRs (nudged toward finished matches), and
        flatten the captured events into ``bronze.sofascore_schedule`` rows via
        :func:`camoufox_capture.normalize_event`. The league page serves the
        CURRENT season, so rows are labelled with ``self.seasons[0]`` in
        soccerdata short form (``'2526'``); the runner merges this captured
        window with the existing partition so a partial capture never shrinks it
        (the completeness guard would otherwise refuse the save).

        Returns ``None`` when nothing is captured (caller then skips the save,
        leaving the existing partition intact).
        """
        from scrapers.sofascore.camoufox_capture import (
            normalize_event,
            season_short_to_label,
        )

        if not self.seasons:
            logger.warning("read_schedule: no season configured — skipping.")
            return None

        # #913 Phase 1/2: single_year support for INT-World Cup etc.
        # Use medallion config so bronze 'season' is the correct slug ('2026')
        # and the capture year filter matches what SofaScore actually puts in
        # event.season.year for that competition.
        try:
            from dags.utils.medallion_config import get_competition_season_format
        except Exception:
            get_competition_season_format = None

        def _effective_slug_and_year(league: str, raw_season: int) -> tuple[str, str]:
            if get_competition_season_format is not None:
                try:
                    if get_competition_season_format(league, raw_season) == 'single_year':
                        s = str(raw_season)
                        return s, s
                except Exception:
                    pass
            # default (split-year club leagues)
            ss = _season_to_short(raw_season)
            return ss, season_short_to_label(ss)

        # per-league slug/year decided inside the loop using get_competition_season_format for single_year (#913)

        frames: List[pd.DataFrame] = []
        for league in self.leagues:
            ut_id = self._resolve_unique_tournament_id(league)
            slug = SOFASCORE_TOURNAMENT_SLUG.get(league)
            if ut_id is None or slug is None:
                logger.warning(
                    "No SofaScore slug/ut_id for league=%s — schedule capture "
                    "skipped.", league,
                )
                continue
            season_slug, target_year = _effective_slug_and_year(league, self.seasons[0])
            nav_url = f"https://www.sofascore.com/tournament/{slug}/{ut_id}"
            events = self._capture_schedule_events(
                nav_url, ut_id, league, season_slug, target_year)
            if not events:
                continue

            df = pd.DataFrame([normalize_event(ev) for ev in events])
            # #840: Bronze as-is — no epoch->timestamp / round->bigint conversion
            # here; the schedule consumers (xref_match, team_match, shots) derive
            # from the raw start_timestamp / round_info_round columns. Only tag
            # partition keys + lineage (added by _add_metadata below).
            df['league'] = league
            df['season'] = season_slug
            logger.info("Capture schedule league=%s season=%s: %d events.",
                        league, season_slug, len(df))
            frames.append(df)

        if not frames:
            return None
        out = pd.concat(frames, ignore_index=True)
        out = self._add_metadata(out, 'schedule')
        return out

    def _capture_schedule_events(
        self, nav_url: str, ut_id, league: str, season_short: str, target_year: str,
    ) -> list:
        """Capture ``target_year``'s events for one league page, retrying up to
        ``_CAPTURE_ATTEMPTS`` times on a FRESH proxy (#879).

        The weekend top-5 backfill lost whole league-seasons to single
        transient failures (proxy connect refused / NS_ERROR_* timeouts) and to
        an unresolved season sid — each attempt picks a new residential exit so
        one dead proxy can't silently no-op the unit. Retryable: a capture
        exception, an unresolved sid, a resolved sid whose events page never
        answered. NOT retried: a real 200 answer with zero events for the
        target season (legitimately empty, e.g. fixtures not yet published).
        """
        from scrapers.sofascore.camoufox_capture import extract_tournament_events

        for attempt in range(1, self._CAPTURE_ATTEMPTS + 1):
            proxy = self._camoufox_proxy()  # fresh exit per attempt (#879)
            sid = None
            buffer = {}
            try:
                self._rate_limiter.acquire()
                with self._camoufox_session(proxy) as cap:
                    buffer = cap.capture_tournament(nav_url)
                    # The landing serves only the CURRENT/next season; page the
                    # TARGET season's events in so a historical backfill is not
                    # empty (#824). For the current season this is a no-op-ish
                    # extra (same sid) the year-filter below keeps consistent.
                    sid = self._resolve_target_sid(cap, buffer, ut_id, target_year)
                    if sid is not None:
                        buffer = cap.paginate_tournament_season(ut_id, int(sid))
            except Exception as e:  # noqa: BLE001 — capture must not crash the run
                logger.warning(
                    "capture schedule failed for league=%s (attempt %d/%d): %s",
                    league, attempt, self._CAPTURE_ATTEMPTS, e)
                continue

            events = [
                ev for ev in extract_tournament_events(buffer, ut_id)
                if (ev.get('season') or {}).get('year') == target_year
            ]
            if events:
                return events
            if sid is None:
                logger.warning(
                    "Capture schedule league=%s: 0 events for season=%s "
                    "(year=%s) — season unresolved, page may serve a different "
                    "season (attempt %d/%d).",
                    league, season_short, target_year,
                    attempt, self._CAPTURE_ATTEMPTS,
                )
                continue
            page0 = (
                f"/api/v1/unique-tournament/{int(ut_id)}/season/{int(sid)}"
                "/events/last/0"
            )
            if self._rec_answered(buffer.get(page0)):
                # The season's own events page answered with nothing — a real
                # empty, not a capture miss. Don't burn retries on it.
                logger.warning(
                    "Capture schedule league=%s: 0 events for season=%s "
                    "(year=%s, sid=%s) — season served empty.",
                    league, season_short, target_year, sid,
                )
                return []
            logger.warning(
                "Capture schedule league=%s: target-season events page missing "
                "from capture (season=%s, sid=%s, attempt %d/%d).",
                league, season_short, sid, attempt, self._CAPTURE_ATTEMPTS,
            )
        return []

    def _capture_season_buffer(self, cap, buffer, ut_id, target_year):
        """Resolve ``target_year``'s SofaScore ``season_id`` from a landing
        ``buffer`` and page that season's events into the buffer so a historical
        season is captured (#824).

        The tournament landing only fires the CURRENT/next season's
        ``/events/...`` XHR, so :func:`extract_tournament_events` finds nothing
        for a past season. We resolve the target sid via
        :meth:`_resolve_target_sid` (buffer sources first, then an in-page
        ``/seasons`` fetch — #879), then drive
        :meth:`SofascoreCamoufoxCapture.paginate_tournament_season` on the same
        (already-navigated) page. Returns the extended buffer, or the original
        unchanged when the season can't be resolved (the caller's
        ``season.year`` filter then yields nothing → no save, no pollution)."""
        target_sid = self._resolve_target_sid(cap, buffer, ut_id, target_year)
        if target_sid is None:
            logger.warning(
                "Season %s unresolved from /seasons or events for ut=%s — "
                "page may serve a different season; skipping season paging.",
                target_year, ut_id,
            )
            return buffer
        return cap.paginate_tournament_season(ut_id, int(target_sid))

    def _fetch_api_rec(self, cap, path: str) -> Optional[dict]:
        """In-page fetch of an ``/api/v1`` ``path`` via the capture session
        (#879). getattr-guarded: test fakes may not implement the method.
        Returns the buffer-shaped record or ``None`` on any failure."""
        fetch = getattr(cap, 'fetch_api_json', None)
        if not callable(fetch):
            return None
        try:
            rec = fetch(path)
        except Exception as e:  # noqa: BLE001 — a probe fetch mustn't kill the run
            logger.info("in-page fetch %s failed: %s", path, e)
            return None
        return rec if isinstance(rec, dict) else None

    @staticmethod
    def _rec_answered(*recs) -> bool:
        """True when any capture record is a real 200, non-challenged answer —
        the 'legitimately empty' signal that stops a capture retry (#879).

        ``challenge is False`` (not merely falsy) and ``json is not None`` on
        purpose: a body-read race leaves ``{'status': 200, 'json': None,
        'challenge': None}`` (see ``merge_capture``) — that's a transport
        failure worth retrying, not an empty answer."""
        return any(
            isinstance(r, dict) and r.get('status') == 200
            and r.get('json') is not None and r.get('challenge') is False
            for r in recs if r is not None
        )

    def _resolve_target_sid(self, cap, buffer, ut_id, target_year) -> Optional[int]:
        """Resolve ``target_year``'s SofaScore ``season_id``: the captured
        ``/seasons`` map first, the captured events' own ``season.id`` second,
        and an in-page ``/seasons`` fetch last (#879 — neither buffer source
        fires reliably when the landing serves another season)."""
        from scrapers.sofascore.camoufox_capture import (
            extract_tournament_events,
            extract_tournament_seasons_map,
        )

        sid = extract_tournament_seasons_map(buffer, ut_id).get(target_year)
        if sid is not None:
            return int(sid)
        for ev in extract_tournament_events(buffer, ut_id):
            s = ev.get('season') or {}
            if s.get('year') == target_year and s.get('id') is not None:
                return int(s['id'])
        path = f"/api/v1/unique-tournament/{int(ut_id)}/seasons"
        rec = self._fetch_api_rec(cap, path)
        if rec is not None:
            sid = extract_tournament_seasons_map({path: rec}, ut_id).get(target_year)
            if sid is not None:
                logger.info(
                    "Season %s resolved via in-page /seasons fetch for ut=%s "
                    "(sid=%s).", target_year, ut_id, sid)
                return int(sid)
        return None

    def read_league_table(self) -> Optional[pd.DataFrame]:
        """Read league standings via Camoufox capture (#777).

        The soccerdata reader is Turnstile-blocked (#757), so we navigate the
        SofaScore tournament page — whose LANDING view is the standings table —
        and let the SPA fire ``/unique-tournament/{ut}/season/{sid}/standings/
        total``, then flatten the captured rows into
        ``bronze.sofascore_league_table`` via :func:`camoufox_capture.
        normalize_standing`. Rows are labelled with ``self.seasons[0]`` in
        soccerdata short form (``'2526'``), or the literal year (``'2026'``)
        for single_year competitions (#913 — the sid RESOLVE and the row LABEL
        must use the same convention, else WC rows land under ``'2627'``).

        The standings JSON carries no season, so the guard is the ``season_id``:
        we resolve the target year's sid (buffer sources first, then an in-page
        ``/seasons`` fetch — the map does NOT fire on the standings landing,
        #779) and accept ONLY standings for that exact sid. When the landing
        buffer lacks the target sid's standings XHR (historical season /
        off-season roll-over) the table is fetched in-page by that sid (#879) —
        same guard, no dependency on the SPA firing the XHR spontaneously.
        Returns ``None`` when nothing matches (caller then skips the save).
        """
        from scrapers.sofascore.camoufox_capture import (
            normalize_standing,
            season_short_to_label,
        )

        if not self.seasons:
            logger.warning("read_league_table: no season configured — skipping.")
            return None
        # #913: single_year aware (reuse same helper logic as schedule)
        try:
            from dags.utils.medallion_config import get_competition_season_format
        except Exception:
            get_competition_season_format = None

        def _effective_slug_and_year(league: str, raw_season: int) -> tuple[str, str]:
            if get_competition_season_format is not None:
                try:
                    if get_competition_season_format(league, raw_season) == 'single_year':
                        s = str(raw_season)
                        return s, s
                except Exception:
                    pass
            ss = _season_to_short(raw_season)
            return ss, season_short_to_label(ss)

        frames: List[pd.DataFrame] = []
        for league in self.leagues:
            ut_id = self._resolve_unique_tournament_id(league)
            slug = SOFASCORE_TOURNAMENT_SLUG.get(league)
            if ut_id is None or slug is None:
                logger.warning(
                    "No SofaScore slug/ut_id for league=%s — league_table "
                    "capture skipped.", league,
                )
                continue
            season_slug, target_y = _effective_slug_and_year(league, self.seasons[0])
            nav_url = f"https://www.sofascore.com/tournament/{slug}/{ut_id}"
            rows = self._capture_league_table_rows(
                nav_url, ut_id, league, season_slug, target_y)
            if not rows:
                continue

            df = pd.DataFrame([normalize_standing(r) for r in rows])
            for col in ('mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'pts'):
                df[col] = df[col].astype('Int64')          # nullable bigint
            df['league'] = league
            df['season'] = season_slug
            logger.info("Capture league_table league=%s season=%s: %d rows.",
                        league, season_slug, len(df))
            frames.append(df)

        if not frames:
            return None
        out = pd.concat(frames, ignore_index=True)
        out = self._add_metadata(out, 'league_table')
        return out

    def _capture_league_table_rows(
        self, nav_url: str, ut_id, league: str, season_short: str, target_year: str,
    ) -> list:
        """Capture ``target_year``'s standings rows for one league page,
        retrying up to ``_CAPTURE_ATTEMPTS`` times on a FRESH proxy (#879).

        The weekend top-5 backfill lost league-seasons to single transient
        failures (proxy connect refused / NS_ERROR_* timeouts) — each attempt
        picks a new residential exit so one dead proxy can't no-op the unit.
        Retryable: a capture exception, an unresolved sid, standings that never
        answered. NOT retried: a real 200 answer with zero rows for our sid.
        """
        from scrapers.sofascore.camoufox_capture import extract_tournament_standings

        for attempt in range(1, self._CAPTURE_ATTEMPTS + 1):
            proxy = self._camoufox_proxy()  # fresh exit per attempt (#879)
            sid = None
            rows = []
            std_path = None
            fetched_rec = None
            buffer = {}
            try:
                self._rate_limiter.acquire()
                with self._camoufox_session(proxy) as cap:
                    buffer = cap.capture_buffer(nav_url)
                    sid = self._resolve_target_sid(cap, buffer, ut_id, target_year)
                    if sid is not None:
                        std_path = (
                            f"/api/v1/unique-tournament/{int(ut_id)}/season/"
                            f"{int(sid)}/standings/total"
                        )
                        rows = extract_tournament_standings(buffer, ut_id, sid)
                        if not rows:
                            # The landing only fires the DEFAULT season's
                            # standings XHR — fetch the TARGET sid's in-page
                            # (same session, same Turnstile clearance, #879).
                            fetched_rec = self._fetch_api_rec(cap, std_path)
                            if fetched_rec is not None:
                                rows = extract_tournament_standings(
                                    {std_path: fetched_rec}, ut_id, sid)
            except Exception as e:  # noqa: BLE001 — capture must not crash the run
                logger.warning(
                    "capture league_table failed for league=%s "
                    "(attempt %d/%d): %s",
                    league, attempt, self._CAPTURE_ATTEMPTS, e)
                continue
            if rows:
                return rows
            if sid is None:
                logger.warning(
                    "Capture league_table league=%s: season %s (year=%s) "
                    "unresolved from /seasons or events — page may serve a "
                    "different season (attempt %d/%d).",
                    league, season_short, target_year,
                    attempt, self._CAPTURE_ATTEMPTS,
                )
                continue
            if self._rec_answered(buffer.get(std_path), fetched_rec):
                logger.warning(
                    "Capture league_table league=%s: 0 standings rows for "
                    "season=%s (sid=%s).", league, season_short, sid,
                )
                return []
            logger.warning(
                "Capture league_table league=%s: standings for sid=%s never "
                "answered (season=%s, attempt %d/%d).",
                league, sid, season_short, attempt, self._CAPTURE_ATTEMPTS,
            )
        return []

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
        season_short = _season_to_short(season_str)

        if 'league' in df.columns and 'season' in df.columns:
            mask = (df['league'] == league) & (
                df['season'].astype(str).isin([season_short, season_str])
            )
            df = df[mask]

        # Keep only finished matches (have a score) — unplayed games have no
        # score. #840: read_schedule now emits the raw source key
        # ``home_score_current`` (was the derived ``home_score``).
        score_col = next(
            (c for c in ('home_score_current', 'home_score') if c in df.columns),
            None,
        )
        if score_col is not None:
            df = df[df[score_col].notna()]

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
                self._record_proxy_bytes(url, resp)  # #789
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

        #840: keep each lineup entry's own fields as-is (captain, substitute,
        shirt_number, ... — previously dropped). ``rating`` stays raw (the
        0.0-means-"did-not-play" -> NULL rule moved to Silver, which already
        applies it); ``position`` keeps the per-event -> nominal fallback. The
        nested ``statistics`` Opta block is deliberately NOT duplicated here — it
        is captured in full by ``event_player_stats`` from the SAME /lineups
        payload, so no source field is lost. The ``player`` identity object is
        skipped (its id is the anchor).

        Schema per row:
            match_id, player_id, team_side, rating, position, + entry fields.
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

            player_id_str = (
                str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
            )

            row: Dict = {
                'match_id': str(match_id),
                'player_id': player_id_str,
                'team_side': side,
                # rating raw (Silver drops 0.0); position per-event or nominal.
                'rating': _coerce_scalar(stats.get('rating')),
                'position': entry.get('position') or player.get('position') or None,
            }
            _auto_flatten(entry, row, skip=('player', 'statistics'))
            rows.append(row)

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
        session_max: int = 120,
        proxy_fail_max: int = 4,
    ):
        """Yield ``(match_id, endpoints)`` by capturing each match page through a
        Camoufox session, restarted on a FRESH proxy every ``session_max``
        matches OR after ``proxy_fail_max`` consecutive failures (issue #757 path
        P2; #829, #832). ``endpoints`` holds whichever of
        ``event/lineups/statistics/shotmap/incidents`` came back as real JSON
        (see ``camoufox_capture.select_event_endpoints``).

        Two failure modes abort a long full-season backfill (380 matches) if the
        session never restarts:
        - Firefox accumulates memory across navigations and the browser dies
          ~200 page loads in (#829) — covered by the ``session_max`` restart.
        - the single residential proxy can die mid-run (``NS_ERROR_PROXY_*`` /
          ``CONNECTION_REFUSED``); since one proxy is picked per session, every
          later capture then fails and the consecutive-failure breaker aborts at
          ~half the season (#832). Picking a FRESH proxy on each (re)start and
          restarting after ``proxy_fail_max`` consecutive failures keeps the run
          alive across a dead proxy. The handful of matches captured during the
          dead-proxy burst are skipped (yielded empty) — re-running the season
          backfills them under the completeness guard.

        The daily run (a handful of matches) crosses neither threshold, so it
        keeps its single-session behaviour. Generalises the ratings-only lineup
        iterator so the daily consolidated path (#751 PR1) also gets the
        ``event`` payload (``homeTeam``/``awayTeam`` — team mapping for
        ``event_player_stats``) from the SAME navigation. Replaces the dead
        ``tls_requests`` REST path: SofaScore's API is Cloudflare-Turnstile-gated
        and only a real Firefox (Camoufox) behind a residential proxy passes it;
        the SPA fires its own XHRs and we capture the responses. ``endpoints``
        without ``lineups`` means the capture missed (Turnstile not solved /
        proxy dead). The generator owns the browser session — the caller MUST
        ``.close()`` it (via try/finally) so it tears down even on an early
        circuit-breaker break.

        #842 in-page fetch: only the session's FIRST match navigates (solving
        Turnstile; ~2 MB — page.route disables the HTTP cache, so every nav
        re-downloads the SPA bundle). Every later match pulls just its JSON
        endpoints via same-origin ``fetch_event`` (~0.1-0.2 MB). A fetch that
        raises or misses a ``required`` endpoint (clearance expired, transient
        miss) falls back to a full ``capture_event`` navigation for that match,
        which re-solves Turnstile for the fetches that follow. Kill-switch
        ``SOFASCORE_INPAGE_FETCH=0`` restores nav-per-match (mirrors
        ``SOFASCORE_BLOCK_RESOURCES``).
        """
        from scrapers.sofascore.camoufox_capture import fetch_names_for_tabs

        env = os.environ.get('SOFASCORE_INPAGE_FETCH')
        use_fetch = env is None or env.strip().lower() not in ('0', 'false', 'no')
        fetch_names = fetch_names_for_tabs(tabs)

        match_ids = [str(m) for m in match_ids]
        n = len(match_ids)
        i = 0
        while i < n:
            proxy = self._camoufox_proxy()  # fresh proxy per (re)start (#832)
            if i:
                logger.info(
                    "match_capture: restarting Camoufox session on a fresh proxy "
                    "at match %d/%d (#829 memory / #832 proxy rotation).", i, n,
                )
            in_session = 0
            consec_fail = 0
            warmed = False  # True after this session's first full navigation
            with self._camoufox_session(proxy) as cap:
                while i < n and in_session < session_max and consec_fail < proxy_fail_max:
                    mid = match_ids[i]
                    self._rate_limiter.acquire()
                    self._stats['requests'] += 1
                    endpoints = None
                    if use_fetch and warmed:
                        try:
                            endpoints = cap.fetch_event(mid, names=fetch_names)
                        except Exception as e:  # noqa: BLE001 — degrade to a full nav
                            logger.info(
                                "in-page fetch failed for event=%s: %s", mid, e)
                            endpoints = None
                        if endpoints is not None and any(
                                r not in endpoints for r in required):
                            logger.info(
                                "in-page fetch missed a required endpoint for "
                                "event=%s — falling back to full navigation.", mid)
                            endpoints = None
                    if endpoints is None:
                        try:
                            endpoints = cap.capture_event(
                                mid, tabs=tabs, required=required,
                            )
                            warmed = True
                        except Exception as e:  # noqa: BLE001 — one bad event mustn't kill the loop
                            logger.warning("camoufox capture failed for event=%s: %s", mid, e)
                            endpoints = {}
                    if not endpoints.get('lineups'):
                        self._stats['failures'] += 1
                        self._last_lineup_error = {
                            'event_id': mid,
                            'status': None,
                            'error': 'lineups_not_captured',
                        }
                        consec_fail += 1
                    else:
                        self._stats['successes'] += 1
                        consec_fail = 0
                    yield mid, endpoints
                    i += 1
                    in_session += 1
            if consec_fail >= proxy_fail_max and i < n:
                logger.warning(
                    "match_capture: %d consecutive failures at match %d/%d — "
                    "likely a dead proxy; rotating proxy + restarting session "
                    "(#832).", consec_fail, i, n,
                )

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

    def _iter_player_captures(self, player_ids: List[str], season_picker_label=None,
                              target_ut=None, target_year=None):
        """Yield ``(player_id, capture)`` through ONE warmed Camoufox session
        (#751 PR3 + PR3b). ``capture`` is the ``{'profile', 'season_buffer'}``
        dict — the bio plus the season-stats capture. When
        ``season_picker_label`` is given (the target tournament's display name,
        e.g. ``'Premier League'``) the navigation path also drives the Season
        tab + picker in the SAME navigation. Mirrors
        :meth:`_iter_match_captures`. A ``{'profile': None}`` means the capture
        missed (page didn't render / proxy dead). The generator owns the browser
        session — the caller MUST ``.close()`` it (try/finally) so it tears down
        even on an early circuit-breaker break.

        #842 in-page fetch: only the FIRST player navigates (solves Turnstile,
        ~2 MB); every later player pulls ``/api/v1/player/{id}`` (+ the two
        season-stats endpoints resolved via ``target_ut``/``target_year``) via
        same-origin fetch (~30 KB). A fetch that raises or misses the profile
        falls back to a full ``capture_player`` navigation for that player.
        Kill-switch ``SOFASCORE_INPAGE_FETCH=0`` restores nav-per-player."""
        env = os.environ.get('SOFASCORE_INPAGE_FETCH')
        use_fetch = env is None or env.strip().lower() not in ('0', 'false', 'no')

        proxy = self._camoufox_proxy()
        warmed = False  # True after this session's first full navigation
        with self._camoufox_session(proxy) as cap:
            for pid in player_ids:
                self._rate_limiter.acquire()
                self._stats['requests'] += 1
                capture = None
                if use_fetch and warmed:
                    try:
                        capture = cap.fetch_player(
                            str(pid), target_ut=target_ut,
                            target_year=target_year)
                    except Exception as e:  # noqa: BLE001 — degrade to a full nav
                        logger.info(
                            "in-page fetch failed for player=%s: %s", pid, e)
                        capture = None
                    if capture is not None and not capture.get('profile'):
                        logger.info(
                            "in-page fetch missed profile for player=%s — "
                            "falling back to full navigation.", pid)
                        capture = None
                if capture is None:
                    try:
                        capture = cap.capture_player(
                            str(pid), season_picker_label=season_picker_label)
                        warmed = True
                    except Exception as e:  # noqa: BLE001 — one bad player mustn't kill the loop
                        logger.warning("camoufox capture failed for player=%s: %s", pid, e)
                        capture = {'profile': None, 'season_buffer': {}}
                if not capture.get('profile'):
                    self._stats['failures'] += 1
                    self._last_lineup_error = {
                        'event_id': None, 'player_id': str(pid),
                        'status': None, 'error': 'player_not_captured',
                    }
                else:
                    self._stats['successes'] += 1
                yield str(pid), capture

    def resolve_finished_match_ids_via_capture(
        self, league: str, season: int,
    ) -> List[str]:
        """Resolve finished match_ids for ``(league, season)`` by capturing the
        SofaScore league page through Camoufox (#757 B1).

        The soccerdata/tls schedule path is Turnstile-blocked, so we navigate
        the league page, page the TARGET ``season``'s ``/events/last`` XHR in
        (the landing serves only the current/next season — #824), and pull the
        finished matches from the captured JSON. ``season`` is a YEAR int (e.g.
        ``2024`` → 24/25): we resolve its SofaScore ``season_id`` and keep only
        events whose ``season.year`` matches, so a past-season backfill is not
        empty and a current-season page never mislabels another season's
        matches. Returns ``[]`` when the league has no SofaScore slug/ut_id,
        capture fails, the season is unresolved, or no finished match exists.
        """
        from scrapers.sofascore.camoufox_capture import (
            extract_tournament_events,
            finished_event_ids,
            season_short_to_label,
        )

        ut_id = self._resolve_unique_tournament_id(league)
        slug = SOFASCORE_TOURNAMENT_SLUG.get(league)
        if ut_id is None or slug is None:
            logger.warning(
                "No SofaScore slug/ut_id for league=%s — capture schedule "
                "resolution skipped.", league,
            )
            return []

        # season is a YEAR int (2024); the events carry the '24/25' year label
        # ('2026' literal for single_year competitions, #913).
        if _is_single_year(league, season):
            season_short = str(int(season))
            target_year = season_short              # WC events: year == '2026'
        else:
            season_short = _season_to_short(season)
            target_year = season_short_to_label(season_short)  # '2425' -> '24/25'

        nav_url = f"https://www.sofascore.com/tournament/{slug}/{ut_id}"
        proxy = self._camoufox_proxy()
        try:
            self._rate_limiter.acquire()
            with self._camoufox_session(proxy) as cap:
                buffer = cap.capture_tournament(nav_url)
                buffer = self._capture_season_buffer(
                    cap, buffer, ut_id, target_year)
        except Exception as e:  # noqa: BLE001 — capture failure must not crash the run
            logger.warning("capture schedule failed for league=%s: %s", league, e)
            return []

        events = extract_tournament_events(buffer, ut_id)
        events = [
            ev for ev in events
            if (ev.get('season') or {}).get('year') == target_year
        ]
        match_ids = finished_event_ids(events)
        logger.info(
            "Capture schedule league=%s season=%s (year=%s): %d ut=%d events, "
            "%d finished.",
            league, season, target_year, len(events), ut_id, len(match_ids),
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
        season_short = _season_label(league, season)
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
        """ONE Camoufox capture pass per match → five Bronze frames (#751 PR1+PR2, #753).

        Replaces four separate Turnstile-blocked passes with a single navigation
        per match. The same pass clicks all deep tabs and captures
        ``/lineups`` + ``/event`` + ``/statistics`` + ``/shotmap``:
          - ``player_ratings`` — :meth:`_flatten_lineup_side` over ``/lineups``;
          - ``event_player_stats`` — :meth:`_flatten_event_player_stats_from_lineups`
            over ``/lineups`` (per-player Opta block), with ``team_id``/
            ``team_name`` from ``/event`` (``homeTeam``/``awayTeam``);
          - ``match_stats`` — :meth:`_flatten_match_stats` over ``/statistics``;
          - ``event_shotmap`` — :meth:`_flatten_shotmap` over ``/shotmap``;
          - ``venue`` — :meth:`_flatten_event_venue` over ``/event`` (#753:
            one row per match, stadium/city/country/coords).

        statistics/shotmap/venue are best-effort: a pass that doesn't fire them
        just yields an empty frame for that table (the others still materialise).

        Returns ``{'player_ratings', 'event_player_stats', 'match_stats',
        'event_shotmap', 'venue'}``; all empty on graceful fallback (caller emits
        ``R0.2B_FALLBACK``). Season slug is coerced to the soccerdata short form
        (``2526``) so the partition matches the schedule writer (#27).
        """
        ratings_cols = [
            # #840: rating/position kept; entry-level fields now preserved too.
            'match_id', 'player_id', 'team_side', 'rating', 'position',
            'captain', 'substitute', 'shirt_number', 'league', 'season',
        ]
        eps_cols = [
            'match_id', 'player_id', 'team_id', 'team_name', 'is_home',
            'position', 'position_specific', 'captain', 'substitute',
            'league', 'season',
        ]
        match_stats_cols = [
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'match_id', 'period', 'stat_group', 'name', 'key',
            'statistics_type', 'home_value', 'away_value', 'home', 'away',
            'compare_code', 'value_type', 'render_type', 'league', 'season',
        ]
        shotmap_cols = [
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'match_id', 'shot_id', 'player_id', 'team_id', 'is_home',
            'id', 'time', 'added_time', 'reversed_period_count', 'period',
            'shot_type', 'situation', 'body_part', 'incident_type', 'goal_type',
            'player_coordinates_x', 'player_coordinates_y',
            'goal_mouth_coordinates_x', 'goal_mouth_coordinates_y',
            'xg', 'xgot', 'league', 'season',
        ]
        venue_cols = [
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'game_id', 'stadium_name', 'stadium_capacity', 'city_name',
            'country_name', 'country_alpha2',
            'venue_coordinates_latitude', 'venue_coordinates_longitude',
            'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        season_short = _season_label(league, season)

        empty = {
            'player_ratings': pd.DataFrame(columns=ratings_cols + ['_ingested_at']),
            'event_player_stats': pd.DataFrame(columns=eps_cols + ['_ingested_at']),
            'match_stats': pd.DataFrame(columns=match_stats_cols + ['_ingested_at']),
            'event_shotmap': pd.DataFrame(columns=shotmap_cols + ['_ingested_at']),
            'venue': pd.DataFrame(columns=venue_cols + ['_ingested_at']),
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
        stats_rows: List[Dict] = []
        shot_rows: List[Dict] = []
        venue_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 10  # ~dead proxy / Turnstile not solved — bail early.

        # Click ALL deep tabs so the SAME navigation also fires statistics +
        # shotmap (#751 PR2). `event` is required alongside lineups so team_id/
        # team_name (homeTeam/awayTeam) are populated, not NULL.
        captures = self._iter_match_captures(
            match_ids,
            tabs=("Lineups", "Statistics", "Player statistics", "Shotmap"),
            required=("lineups", "event"),
        )
        try:
            for idx, (mid, endpoints) in enumerate(captures, start=1):
                endpoints = endpoints or {}
                lineups = endpoints.get('lineups')
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
                event_payload = endpoints.get('event')
                for side in ('home', 'away'):
                    ratings_rows.extend(self._flatten_lineup_side(
                        match_id=str(mid),
                        side=side,
                        side_payload=lineups.get(side) or {},
                    ))
                eps_rows.extend(self._flatten_event_player_stats_from_lineups(
                    str(mid), lineups, event_payload,
                ))

                # venue (#753) — one row per match from the SAME `event`
                # capture; SofaScore carries the per-match stadium (historically
                # accurate, unlike FotMob's current-ground bias). Best-effort:
                # a match whose event payload omits venue yields no row.
                venue_row = self._flatten_event_venue(str(mid), event_payload)
                if venue_row:
                    venue_rows.append(venue_row)

                # Best-effort: a match page may not fire statistics/shotmap on
                # a given pass — absent keys just yield no rows for that table.
                statistics = endpoints.get('statistics')
                if statistics is not None:
                    stats_rows.extend(self._flatten_match_stats(str(mid), statistics))
                shotmap = endpoints.get('shotmap')
                if shotmap is not None:
                    shot_rows.extend(self._flatten_shotmap(str(mid), shotmap))

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

        # match_stats + event_shotmap come from the SAME capture pass (#751 PR2).
        def _tag(rows: List[Dict], entity_type: str) -> pd.DataFrame:
            df = pd.DataFrame(rows)
            df['league'] = league
            df['season'] = season_short
            df['_ingested_at'] = datetime.utcnow()
            df['_source'] = self.SOURCE_NAME
            df['_entity_type'] = entity_type
            df['_batch_id'] = self._batch_id
            return df

        out['match_stats'] = (
            _tag(stats_rows, 'match_stats') if stats_rows else empty['match_stats'])
        out['event_shotmap'] = (
            _tag(shot_rows, 'event_shotmap') if shot_rows else empty['event_shotmap'])
        out['venue'] = (
            _tag(venue_rows, 'venue') if venue_rows else empty['venue'])

        if not ratings_rows and not eps_rows:
            logger.warning(
                "%s: match_capture materialised zero rows across %d match attempts.",
                R0_2B_FALLBACK_MARKER, len(match_ids),
            )

        logger.info(
            "match_capture: %d ratings + %d eps + %d match_stats + %d shots "
            "+ %d venues across %d matches",
            len(out['player_ratings']), len(out['event_player_stats']),
            len(out['match_stats']), len(out['event_shotmap']),
            len(out['venue']), len(match_ids),
        )
        return out

    # ------------------------------------------------------------------
    # #22 event shotmap — per-shot xG / coords / situation / body part
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_shotmap(match_id: str, payload: dict) -> List[Dict]:
        """Project the ``shotmap`` block into one row per shot.

        #840: Bronze keeps EVERY source field. Only the primary key + identity
        anchors that Silver joins on (and that need type / format stabilisation)
        are hard-coded: ``match_id``, ``shot_id`` (composite fallback),
        ``player_id``, ``team_id``, ``is_home``. Every other scalar auto-flattens
        through :func:`_auto_flatten`, so new SofaScore fields land in Bronze
        automatically. Renames / derivations (``minute`` <- ``time``, ``x`` <-
        ``player_coordinates_x``, ``outcome`` <- ``incidentType``, the xg
        coalesce, ...) move to Silver.

        Nested objects flatten with a path prefix::

            playerCoordinates.x     -> player_coordinates_x
            goalMouthCoordinates.x  -> goal_mouth_coordinates_x
        """
        rows: List[Dict] = []
        if not isinstance(payload, dict):
            return rows

        shots = payload.get('shotmap') or []
        if not isinstance(shots, list):
            return rows

        def _i(v):
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        for shot in shots:
            if not isinstance(shot, dict):
                continue

            player = shot.get('player') or {}

            # --- PK: shot id, composite fallback when SofaScore omits id ---
            sid = shot.get('id')
            if sid is None:
                # Fall back to composite (match, time, player) so that
                # downstream PK stays unique even when SofaScore omits id.
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

            pid = player.get('id')
            player_id_str = (
                str(int(pid)) if isinstance(pid, (int, float)) and pid is not None
                else (str(pid) if pid is not None else None)
            )

            # Identity anchors set FIRST so _auto_flatten never clobbers them.
            row: Dict = {
                'match_id': str(match_id),
                'shot_id': shot_id_str,
                'player_id': player_id_str,
                'team_id': _i(shot.get('teamId') or (shot.get('team') or {}).get('id')),
                'is_home': bool(shot.get('isHome')) if shot.get('isHome') is not None else None,
            }

            # Auto-passthrough everything else. Skip identity objects already
            # projected as anchors (player.id -> player_id, team.id -> team_id).
            _auto_flatten(shot, row, skip=('player', 'team'))

            rows.append(row)

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
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'match_id', 'shot_id', 'player_id', 'team_id', 'is_home',
            'id', 'time', 'added_time', 'reversed_period_count', 'period',
            'shot_type', 'situation', 'body_part', 'incident_type', 'goal_type',
            'player_coordinates_x', 'player_coordinates_y',
            'goal_mouth_coordinates_x', 'goal_mouth_coordinates_y',
            'xg', 'xgot', 'league', 'season',
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
        season_short = _season_label(league, season)
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
        # The captured /event/{id} body nests the event object under "event"
        # ({"event": {homeTeam, awayTeam, ...}}); unwrap it (live-proven 2026-06-22,
        # #751 PR2 — this is why PR1's team_id came back NULL).
        if isinstance(ev.get('event'), dict):
            ev = ev['event']
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

    @staticmethod
    def _flatten_event_venue(match_id: str, event_payload) -> Optional[Dict]:
        """Project the captured ``/event/{id}`` venue block into ONE Bronze row (#753).

        SofaScore's ``event.venue`` records the stadium THIS match was played at,
        so it stays historically accurate for clubs that moved grounds (Everton →
        Goodison Park, Spurs → White Hart Lane) — exactly where FotMob's
        current-ground ``team_profile`` is wrong (see gold.dim_venue). Returns
        ``None`` when the payload carries no usable stadium.

        Defensive on shape: SofaScore nests ``stadium``/``city``/``country`` as
        ``{"name": ...}`` objects, but the issue documents a flat
        ``{stadium, city, country}`` form — ``_name`` accepts either.

        Live-verified 2026-06-23 (event 14023959, American Express Stadium): real
        shape is the NESTED form — ``stadium``/``city``/``country`` are
        ``{"name": ...}`` objects; ``city`` also carries country/id. Two caveats
        the issue got wrong: (1) ``venueCoordinates`` was ABSENT for that venue, so
        coords are sporadic and usually NULL — city/country are the reliable
        value-add, coords a bonus when present; (2) ``capacity`` IS in the payload
        (``stadium.capacity``) but stays FotMob-sourced (#750), so Silver ignores
        it — but Bronze now keeps it as ``stadium_capacity`` per #840 (all source
        fields preserved). Like the other capture flatteners the caller tags
        ``league``/``season``/lineage; this emits business columns only.
        """
        ev = event_payload if isinstance(event_payload, dict) else {}
        # The captured /event/{id} body nests the event under "event" (#751 PR2).
        if isinstance(ev.get('event'), dict):
            ev = ev['event']
        venue = ev.get('venue')
        if not isinstance(venue, dict):
            return None

        def _name(v):
            """SofaScore ``{"name": X}`` object → X; a bare string passes through."""
            return v.get('name') if isinstance(v, dict) else v

        # Row guard only — no usable stadium name → skip (unchanged contract).
        stadium = _name(venue.get('stadium'))
        if stadium is None or str(stadium).strip() == '':
            return None

        gid = ev.get('id')
        if gid is None:
            gid = match_id
        try:
            game_id = int(gid)
        except (TypeError, ValueError):
            game_id = None

        # #840: keep the whole venue block as-is. Nested {"name": ...} objects
        # flatten to stadium_name / city_name / country_name; venueCoordinates
        # to venue_coordinates_latitude/longitude (+ bonus stadium_capacity,
        # country_alpha2, ...). Silver renames back to
        # stadium/city/country/venue_latitude/venue_longitude.
        row: Dict = {'game_id': game_id}
        _auto_flatten(venue, row)
        return row

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

        season_short = _season_label(league, season)

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
                    # #840: only the position anchors are hard-coded; every
                    # statisticsItem field auto-flattens (source-key names:
                    # name, key, statistics_type, home/away, home_value/away_value,
                    # compare_code, value_type, render_type, ...). Silver renames
                    # stat_name<-name, stat_key<-key||statistics_type,
                    # home_text<-home, away_text<-away.
                    row: Dict = {
                        'match_id': str(match_id),
                        'period': str(period),
                        'stat_group': stat_group,
                    }
                    # #840: home/away are SofaScore *display* strings — "55%",
                    # "3 (1)", "91.6 km", "2.61" — heterogeneous units across
                    # stats. Pin them to str BEFORE _auto_flatten (whose
                    # `if col in out: continue` then leaves them untouched) so the
                    # Bronze column stays a stable varchar. Otherwise _coerce_scalar
                    # upcasts the numeric-looking ones (int/float) while "55%" stays
                    # str, yielding a mixed-type object column that the PyArrow ->
                    # Iceberg writer cannot serialize. Numeric canonicals live in
                    # home_value/away_value (clean doubles); Silver maps
                    # home_text<-home, away_text<-away.
                    for _disp in ('home', 'away'):
                        if item.get(_disp) is not None:
                            row[_disp] = str(item[_disp])
                    _auto_flatten(item, row)
                    rows.append(row)

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

        season_short = _season_label(league, season)
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

    # SofaScore unique-tournament id per league — used by capture-path
    # targeting (season picker + finished-match discovery).
    def _resolve_unique_tournament_id(self, league: str) -> Optional[int]:
        return SOFASCORE_TOURNAMENT_MAP.get(league)

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

    # ------------------------------------------------------------------
    # #23 player_profile — snapshot (height, foot, dob, nationality, ...)
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_player_profile(payload: dict) -> Optional[Dict]:
        """Project ``/player/{id}`` payload into a snapshot row.

        #840: Bronze keeps the whole ``player`` block as-is (auto-passthrough);
        only ``player_id`` is a hard-coded anchor. Renames/derivations move to
        Silver: ``height_cm`` <- ``height``, ``date_of_birth`` <-
        ``date_of_birth_timestamp``, ``country_code`` <- ``country.alpha2``,
        ``current_team_*`` <- ``team.*``, and the ``nationality`` <-
        ``country.name`` fallback. Extra/marketing fields the old fixed list
        dropped (``user_count``, ``retired_status``, name translations) are now
        preserved (source-as-is contract).
        """
        if not isinstance(payload, dict):
            return None

        player = payload.get('player')
        if not isinstance(player, dict):
            return None

        pid = player.get('id')
        if pid is None:
            return None

        row: Dict = {
            'player_id': str(int(pid)) if isinstance(pid, (int, float)) else str(pid),
        }
        # Nested `country`/`team` flatten to country_name/country_alpha2/team_id/
        # team_name/... ; `dateOfBirthTimestamp` stays raw (Silver -> date).
        _auto_flatten(player, row)
        return row

    # ------------------------------------------------------------------
    # #751 PR3 — per-player capture (biographical profile snapshot)
    # ------------------------------------------------------------------

    def read_player_capture(
        self,
        league: str,
        season: int,
        player_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Per-player capture → the player_profile Bronze frame (#751 PR3).

        Replaces the dead tls player_profile pass (403'd on Turnstile). #842:
        only the FIRST player navigates its page (solving Turnstile); every
        later player pulls ``/api/v1/player/{id}`` via same-origin in-page
        fetch (~30 KB vs ~2 MB per navigation) — the page SSRs the identical
        ``player`` object into ``__NEXT_DATA__``, so both paths feed
        :meth:`_flatten_player_profile` the same shape. A fetch miss falls
        back to a full navigation for that player.

        Season-aggregate stats (``player_season_stats``) come from the SAME
        pass (#751 PR3b): the fetch path resolves the exact ``(ut, season_id)``
        from ``/statistics/seasons`` and pulls its ``statistics/overall``;
        the navigation path drives the Season tab + a season-picker (the
        default Season tab is the player's PRIMARY competition, not
        necessarily EPL for a transferred player — live-proven Paquetá →
        World Cup). Either way the right overall is selected season-guarded
        via the pure :func:`select_player_season_stats` /
        :func:`extract_player_seasons_map`.

        Returns ``{'player_profile', 'player_season_stats'}``. The profile is the
        primary deliverable; season-stats may be a strict subset (the picker can
        miss for some players) — a WARN, not a failure. Empty frames on graceful
        fallback (caller emits ``R0.2B_FALLBACK``). Season slug is coerced to the
        soccerdata short form (``2526``) so the partition matches.
        """
        from scrapers.sofascore.camoufox_capture import (
            extract_player_seasons_map,
            season_short_to_label,
            select_player_season_stats,
        )

        profile_cols = [
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'player_id', 'id', 'name', 'short_name', 'slug', 'position',
            'jersey_number', 'shirt_number', 'height', 'preferred_foot',
            'date_of_birth_timestamp', 'nationality',
            'country_name', 'country_alpha2',
            'team_id', 'team_name', 'retired', 'league', 'season',
        ]
        season_cols = [
            'player_id', 'unique_tournament_id', 'sofascore_season_id',
            'team_id', 'team_name', 'league', 'season',
        ]

        season_short = _season_label(league, season)

        empty = {
            'player_profile': pd.DataFrame(columns=profile_cols + ['_ingested_at']),
            'player_season_stats': pd.DataFrame(columns=season_cols + ['_ingested_at']),
        }

        # Target competition for the Season-tab picker. Only an in-scope league
        # (known unique_tournament_id) drives the picker; otherwise profile-only.
        # 'ENG-Premier League' -> ut=17, picker label 'Premier League'.
        target_ut = SOFASCORE_TOURNAMENT_MAP.get(league)
        picker_label = league.split('-', 1)[-1] if target_ut else None
        target_season_label = season_short_to_label(season_short)

        if player_ids is None:
            player_ids = self._resolve_player_ids_from_bronze(
                league, season_short, limit=limit,
            )
        if not player_ids:
            logger.warning(
                "No player_ids resolved for player_capture (league=%s season=%s).",
                league, season_short,
            )
            return empty
        if limit:
            player_ids = list(player_ids)[: int(limit)]

        logger.info(
            "player_capture: capturing %d players (league=%s season=%s ut=%s)",
            len(player_ids), league, season_short, target_ut,
        )

        profile_rows: List[Dict] = []
        season_rows: List[Dict] = []
        consecutive_failures = 0
        max_consecutive = 10  # ~dead proxy / page not rendering — bail early.

        captures = self._iter_player_captures(
            player_ids, season_picker_label=picker_label,
            target_ut=target_ut, target_year=target_season_label)
        try:
            for idx, (pid, capture) in enumerate(captures, start=1):
                profile = (capture or {}).get('profile')
                if not profile:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive:
                        logger.error(
                            "%s: %d consecutive player capture failures — aborting "
                            "player_capture early to preserve proxy budget.",
                            R0_2B_FALLBACK_MARKER, consecutive_failures,
                        )
                        break
                    continue
                consecutive_failures = 0

                prow = self._flatten_player_profile({'player': profile})
                if prow is not None:
                    profile_rows.append(prow)

                # Season-aggregate stats (best-effort: the picker may miss for a
                # transferred player → no row for them, which is a WARN).
                if target_ut:
                    season_buffer = (capture or {}).get('season_buffer') or {}
                    if season_buffer:
                        seasons_map = extract_player_seasons_map(season_buffer, pid)
                        target_sid = seasons_map.get(target_ut, {}).get(
                            target_season_label)
                        sel = select_player_season_stats(
                            season_buffer, pid, target_ut, target_sid)
                        if sel is not None:
                            ut, sid, payload = sel
                            srow = self._flatten_player_season_stats(
                                pid, ut, sid, payload)
                            if srow is not None:
                                season_rows.append(srow)

                if idx % 25 == 0:
                    logger.info("player_capture progress: %d/%d players",
                                idx, len(player_ids))
        finally:
            close = getattr(captures, 'close', None)
            if callable(close):
                close()  # tear down the Camoufox session

        if not profile_rows:
            logger.warning(
                "%s: player_capture materialised zero rows across %d players.",
                R0_2B_FALLBACK_MARKER, len(player_ids),
            )
            return empty

        df = pd.DataFrame(profile_rows)
        df['league'] = league
        df['season'] = season_short
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_profile'
        df['_batch_id'] = self._batch_id

        result = {
            'player_profile': df,
            'player_season_stats': empty['player_season_stats'],
        }

        if season_rows:
            sdf = pd.DataFrame(season_rows)
            sdf['league'] = league
            sdf['season'] = season_short
            sdf['_ingested_at'] = datetime.utcnow()
            sdf['_source'] = self.SOURCE_NAME
            sdf['_entity_type'] = 'player_season_stats'
            sdf['_batch_id'] = self._batch_id
            result['player_season_stats'] = sdf

        logger.info(
            "player_capture: %d profile rows + %d season-stats rows across "
            "%d players", len(df), len(result['player_season_stats']),
            len(player_ids),
        )
        return result

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
