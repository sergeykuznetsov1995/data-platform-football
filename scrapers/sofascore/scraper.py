"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import hashlib
import json
import logging
import os
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
    _MAX_FLATTEN_DEPTH as _MAX_FLATTEN_DEPTH,
    _auto_flatten,
    _camel_to_snake,
    _coerce_scalar,
)
from scrapers.sofascore.catalog import SofaScoreCatalog

logger = logging.getLogger(__name__)


_TERMINAL_CAPTURE_STATES = frozenset({"success", "not_available"})


# Read-only source metadata comes from the discovery registry.  These computed
# dicts intentionally preserve the long-standing public imports while removing
# the second, manually maintained copy of tournament ids and navigation paths.
# Disabled competitions remain resolvable here: activation controls DAG scope,
# not whether an explicit/manual scraper call can address a known tournament.
_SOFASCORE_CATALOG = SofaScoreCatalog.load()
SOFASCORE_TOURNAMENT_MAP: Dict[str, int] = (
    _SOFASCORE_CATALOG.tournament_map(enabled_only=False)
)
SOFASCORE_TOURNAMENT_SLUG: Dict[str, str] = (
    _SOFASCORE_CATALOG.slug_map(enabled_only=False)
)

# R0.2b — graceful-fallback marker emitted when the lineups endpoint
# is structurally unavailable (HTTP 403 / quota empty / repeated timeouts).
# Downstream (E4.4 schema-stub path) keys off this marker to keep the
# Gold layer building without ratings instead of failing the DAG.
R0_2B_FALLBACK_MARKER = "R0.2B_FALLBACK"


def _season_to_short(season) -> str:
    """Normalize a season token to soccerdata's short 'YYZZ' form.

    Mirrors ``scrapers/whoscored/scraper.py::_season_to_soccerdata_str``:
    already-short tokens pass through ('2526' -> '2526'). Integer values in
    the plausible calendar-year range are unambiguously treated as start years
    (2021 -> '2122', 2024 -> '2425', 1999 -> '9900'); this matches the Airflow
    ``season`` Param contract. String tokens keep supporting soccerdata's short
    form, including the otherwise ambiguous ``'2021'`` -> 20/21. The old inline
    conversion mapped '2526' -> '2627' (a nonexistent season), silently
    no-op'ing scrapes triggered with the documented short form.
    Non-4-digit tokens pass through unchanged (legacy behaviour of the
    inline ``else`` branch this helper replaces).
    """
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        return s
    # Preserve the input type as the ambiguity boundary: Airflow/CLI passes an
    # int start year, while callers that intentionally mean the short 20/21
    # token can pass the string ``"2021"``. Converting to str before this check
    # was the source of the historical 2021/22 -> 2020/21 mislabelling.
    if isinstance(season, int) and 1900 <= season <= 2098:
        return s[-2:] + f"{(season + 1) % 100:02d}"
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


def _season_slug_and_target_year(league: str, season) -> Tuple[str, str]:
    """Return the Bronze partition slug and exact SofaScore API year.

    Split-year leagues use ``25/26`` while single-year tournaments keep the
    literal ``2026`` for both values. Centralising this prevents shared capture
    paths from converting tournament years to values such as ``20/26`` and
    replaces the former duplicated, broad-except config lookups.
    """
    from scrapers.sofascore.camoufox_capture import season_short_to_label

    if _is_single_year(league, season):
        label = str(int(season))
        return label, label
    label = _season_to_short(season)
    return label, season_short_to_label(label)


class SofaScoreScraper(SoccerdataScraper):
    """
    Scraper for SofaScore football data.

    SofaScore provides:
    - Live match data and scores
    - Detailed match statistics
    - Player ratings
    - Heatmaps and position data

    DAG, CLI and backfill consumers call the unified match/player capture
    readers and own their manifest-aware incremental writes.
    """

    SOURCE_NAME = 'sofascore'
    DEFAULT_RATE_LIMIT = 20  # SofaScore can be strict
    # Explicitly disable BaseScraper's standalone writer hook. ABCMeta treats
    # this non-callable override as implemented, while any accidental caller
    # fails closed instead of bypassing the common runner/manifest.
    scrape_all = None
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
        # Every network-capable entrypoint, including direct library use, is
        # activation-gated.  Discovery may catalog women/youth/reserve rows,
        # but an unreviewed or disabled tournament can never reach a browser.
        for league in self.leagues:
            tournament = _SOFASCORE_CATALOG.competition(league)
            if not tournament.capture_allowed:
                reasons = '; '.join(tournament.activation_eligibility.reasons)
                raise ValueError(
                    f"SofaScore capture denied for {league}: "
                    f"enabled={tournament.enabled}; {reasons}"
                )
        # Residential-proxy traffic audit (#879): rx+tx bytes of each Camoufox
        # session are folded in at teardown via ``_camoufox_session``.
        self._proxy_bytes_by_host: Dict[str, int] = defaultdict(int)
        self._camoufox_bytes: int = 0
        self._camoufox_sessions: int = 0
        self._browser_navigations: int = 0
        self._browser_api_fetches: int = 0
        self._browser_blocked_requests: int = 0
        self._fallback_navigations: int = 0
        self._last_raw_records: Dict[str, dict] = {}
        self._camoufox_proxy_objects: Dict[tuple, object] = {}
        self._last_camoufox_proxy_key: Optional[tuple] = None

    def get_traffic_stats(self) -> Dict:
        """Camoufox residential-proxy bytes seen by the compatibility reader.

        Provider-authoritative accounting lives in the common capture engine;
        this passive rx+tx view remains for browser diagnostics and legacy
        runner output. Keys remain stable for the shared traffic reporter.
        """
        by_host = sorted(
            self._proxy_bytes_by_host.items(), key=lambda kv: -kv[1]
        )
        total = self._camoufox_bytes
        return {
            'proxy_response_bytes': total,
            'proxy_response_mb': round(total / 1024 / 1024, 4),
            'camoufox_bytes': self._camoufox_bytes,
            'camoufox_mb': round(self._camoufox_bytes / 1024 / 1024, 4),
            'browser_sessions': self._camoufox_sessions,
            'browser_navigations': self._browser_navigations,
            'browser_api_fetches': self._browser_api_fetches,
            'browser_fallback_navigations': self._fallback_navigations,
            'browser_blocked_requests': self._browser_blocked_requests,
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
            with SofascoreCamoufoxCapture(
                proxy=proxy,
                request_limiter=self._acquire_camoufox_request,
            ) as cap:
                yield cap
        finally:
            if cap is not None:
                self._camoufox_sessions += 1
                try:
                    nbytes = int(getattr(cap, '_bytes_total', 0) or 0)
                except (TypeError, ValueError):
                    nbytes = 0
                if nbytes > 0:
                    self._camoufox_bytes += nbytes
                    self._proxy_bytes_by_host['camoufox:www.sofascore.com'] += nbytes
                for attr, target in (
                    ('_navigation_count', '_browser_navigations'),
                    ('_api_fetch_count', '_browser_api_fetches'),
                    ('_blocked_count', '_browser_blocked_requests'),
                ):
                    value = getattr(cap, attr, 0)
                    if isinstance(value, (int, float)) and value > 0:
                        setattr(self, target, getattr(self, target) + int(value))

    def _acquire_camoufox_request(self) -> None:
        """Rate-limit and count one actual Camoufox navigation/API request."""
        if self._rate_limiter.acquire() is False:
            raise RuntimeError("SofaScore request rate limiter refused a request")
        self._stats["requests"] += 1

    def read_schedule(self) -> Optional[pd.DataFrame]:
        """Read the match schedule + results via Camoufox capture (#761).

        The soccerdata schedule reader is Turnstile-blocked (#757), so for each
        league we navigate its tournament page once to establish Turnstile
        clearance, resolve the exact target ``season_id``, and fetch that
        season's ``events/last`` plus ``events/next`` JSON directly. Rows are
        labelled with ``self.seasons[0]`` in soccerdata short form (``'2526'``);
        the runner merges this captured window with the existing partition so a
        partial capture never shrinks it.

        Returns ``None`` when nothing is captured (caller then skips the save,
        leaving the existing partition intact).
        """
        from scrapers.sofascore.camoufox_capture import normalize_event

        if not self.seasons:
            logger.warning("read_schedule: no season configured — skipping.")
            return None
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
            season_slug, target_year = _season_slug_and_target_year(
                league,
                self.seasons[0],
            )
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
            from dags.utils.sofascore_dq import validate_schedule_rows

            validate_schedule_rows(df.to_dict('records')).require()
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
        """Capture only schedule events; see :meth:`_capture_tournament_snapshot`."""
        return self._capture_tournament_snapshot(
            nav_url,
            ut_id,
            league,
            season_short,
            target_year,
            want_events=True,
            want_standings=False,
        )["events"]

    def _capture_tournament_snapshot(
        self,
        nav_url: str,
        ut_id,
        league: str,
        season_short: str,
        target_year: str,
        *,
        want_events: bool,
        want_standings: bool,
    ) -> Dict[str, list]:
        """Capture target-season events and/or standings with one browser warm-up.

        ``None`` is the internal "not answered yet" sentinel; ``[]`` is a real
        200-empty answer. When both entities are requested, a successful entity
        is retained across retries so a fresh proxy only re-fetches the missing
        component. This is the shared transport for the daily tournament
        snapshot and the standalone readers.
        """
        from scrapers.sofascore.camoufox_capture import (
            extract_tournament_events,
            extract_tournament_standings,
        )

        events = None if want_events else []
        standings = None if want_standings else []
        events_by_id: Dict[object, dict] = {}

        for attempt in range(1, self._CAPTURE_ATTEMPTS + 1):
            if events is not None and standings is not None:
                break
            proxy = self._camoufox_proxy()
            sid = None
            buffer: Dict[str, dict] = {}
            std_path = None
            fetched_standings = None
            try:
                with self._camoufox_session(proxy) as cap:
                    # Navigation exists only to establish origin + Turnstile
                    # clearance. Target data comes from exact same-origin API
                    # calls, not DOM interactions with hidden tabs.
                    buffer = cap.capture_buffer(nav_url)
                    sid = self._resolve_target_sid(
                        cap,
                        buffer,
                        ut_id,
                        target_year,
                    )
                    if sid is not None and events is None:
                        buffer = cap.paginate_tournament_season(
                            ut_id,
                            int(sid),
                            include_next=True,
                        )
                        found = [
                            ev
                            for ev in extract_tournament_events(buffer, ut_id)
                            if (ev.get("season") or {}).get("year") == target_year
                        ]
                        for event in found:
                            events_by_id[event.get("id")] = event
                        event_prefix = (
                            f"/api/v1/unique-tournament/{int(ut_id)}/season/"
                            f"{int(sid)}/events"
                        )
                        # Both directions must answer before the schedule is
                        # complete: before kickoff ``last`` is empty while
                        # ``next`` contains every published fixture. Partial
                        # rows are retained across a fresh-proxy retry.
                        if all(
                            self._event_direction_complete(
                                buffer,
                                event_prefix,
                                direction,
                            )
                            for direction in ("last", "next")
                        ):
                            events = list(events_by_id.values())

                    if sid is not None and standings is None:
                        std_path = (
                            f"/api/v1/unique-tournament/{int(ut_id)}/season/"
                            f"{int(sid)}/standings/total"
                        )
                        rows = extract_tournament_standings(buffer, ut_id, sid)
                        if not rows:
                            fetched_standings = self._fetch_api_rec(cap, std_path)
                            if fetched_standings is not None:
                                rows = extract_tournament_standings(
                                    {std_path: fetched_standings},
                                    ut_id,
                                    sid,
                                )
                        if rows:
                            standings = rows
                        elif self._rec_answered(
                            buffer.get(std_path),
                            fetched_standings,
                        ):
                            standings = []
            except Exception as exc:  # noqa: BLE001 — rotate and retry
                self._record_camoufox_proxy_result(proxy, success=False)
                logger.warning(
                    "tournament capture failed league=%s attempt=%d/%d: %s",
                    league,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                    exc,
                )
                continue

            if sid is not None:
                attempt_complete = (
                    (not want_events or events is not None)
                    and (not want_standings or standings is not None)
                )
                self._record_camoufox_proxy_result(
                    proxy,
                    success=attempt_complete,
                    error_type="unknown",
                )

            if sid is None:
                self._record_camoufox_proxy_result(
                    proxy,
                    success=False,
                    error_type="unknown",
                )
                logger.warning(
                    "Tournament season unresolved league=%s season=%s "
                    "(year=%s, attempt %d/%d).",
                    league,
                    season_short,
                    target_year,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                )
                continue
            for path, record in buffer.items():
                if not isinstance(record, dict) or record.get('body') is None:
                    continue
                self._last_raw_records[
                    f'{int(ut_id)}:{int(sid)}:{path}'
                ] = {
                    'source_tournament_id': str(int(ut_id)),
                    'source_season_id': str(int(sid)),
                    'target_type': 'season',
                    'target_id': str(int(sid)),
                    'endpoint': path,
                    'league': league,
                    'season': season_short,
                    **dict(record),
                }
            if isinstance(fetched_standings, dict) and std_path:
                self._last_raw_records[
                    f'{int(ut_id)}:{int(sid)}:{std_path}'
                ] = {
                    'source_tournament_id': str(int(ut_id)),
                    'source_season_id': str(int(sid)),
                    'target_type': 'season',
                    'target_id': str(int(sid)),
                    'endpoint': std_path,
                    'league': league,
                    'season': season_short,
                    **dict(fetched_standings),
                }
            if events is None:
                logger.warning(
                    "Tournament events unanswered league=%s sid=%s (attempt %d/%d).",
                    league,
                    sid,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                )
            if standings is None:
                logger.warning(
                    "Tournament standings unanswered league=%s sid=%s (attempt %d/%d).",
                    league,
                    sid,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                )

        return {
            "events": (events if events is not None else list(events_by_id.values())),
            "standings": standings if standings is not None else [],
        }

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

    @staticmethod
    def _rec_terminal_empty(rec) -> bool:
        """True for an explicit no-page response from SofaScore.

        A finished season can expose every match through ``events/last`` while
        ``events/next/0`` returns a JSON 404 instead of a 200-empty list. That
        page-zero 404 is a terminal empty direction, not a proxy failure. A
        404 after a page that advertised ``hasNextPage`` remains incomplete and
        must still trigger a fresh-session retry.
        """
        if not isinstance(rec, dict):
            return False
        if rec.get("status") == 204:
            return rec.get("challenge") is not True
        obj = rec.get("json")
        error = obj.get("error") if isinstance(obj, dict) else None
        return (
            rec.get("status") == 404
            and rec.get("challenge") is False
            and isinstance(error, dict)
            and str(error.get("code")) == "404"
        )

    @classmethod
    def _event_direction_complete(
        cls,
        buffer: Dict[str, dict],
        event_prefix: str,
        direction: str,
        max_pages: int = 25,
    ) -> bool:
        """Return True only after a paginated event direction reaches its end.

        Page zero alone is not complete when ``hasNextPage`` is true. Accepting
        it after a later page failed silently truncated seasons and prevented a
        fresh-proxy retry.
        """
        for page in range(max_pages):
            rec = buffer.get(f"{event_prefix}/{direction}/{page}")
            if page == 0 and cls._rec_terminal_empty(rec):
                return True
            if not cls._rec_answered(rec):
                return False
            obj = rec.get("json")
            events = obj.get("events") if isinstance(obj, dict) else None
            if not isinstance(events, list):
                return False
            if obj.get("hasNextPage") is True:
                if not events:
                    return False
                continue
            return True
        return False

    def _resolve_target_sid(self, cap, buffer, ut_id, target_year) -> Optional[int]:
        """Resolve ``target_year``'s SofaScore ``season_id``: the captured
        discovery registry first, the captured ``/seasons`` map second, the
        captured events' own ``season.id`` third, and an in-page ``/seasons``
        fetch last (#879 — neither buffer source fires reliably when the
        landing serves another season)."""
        from scrapers.sofascore.camoufox_capture import (
            extract_tournament_events,
            extract_tournament_seasons_map,
        )

        sid = _SOFASCORE_CATALOG.resolve_season_id(ut_id, target_year)
        if sid is not None:
            return int(sid)

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
        from scrapers.sofascore.camoufox_capture import normalize_standing

        if not self.seasons:
            logger.warning("read_league_table: no season configured — skipping.")
            return None
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
            season_slug, target_y = _season_slug_and_target_year(
                league,
                self.seasons[0],
            )
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
        """Capture only standings; see :meth:`_capture_tournament_snapshot`."""
        return self._capture_tournament_snapshot(
            nav_url,
            ut_id,
            league,
            season_short,
            target_year,
            want_events=False,
            want_standings=True,
        )["standings"]

    def read_tournament_snapshot(
        self,
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Read schedule and standings with one Camoufox session per league.

        The daily runner always needs both entities. Calling the standalone
        readers used to load the same SPA twice (~2.0 MB + ~1.8 MB in production
        logs). This explicit combined API shares only the browser warm-up; data
        extraction, empty-state handling and output frames remain independent.
        """
        from scrapers.sofascore.camoufox_capture import (
            normalize_event,
            normalize_standing,
        )

        if not self.seasons:
            logger.warning("read_tournament_snapshot: no season configured.")
            return None, None

        schedule_frames: List[pd.DataFrame] = []
        standings_frames: List[pd.DataFrame] = []

        for league in self.leagues:
            ut_id = self._resolve_unique_tournament_id(league)
            slug = SOFASCORE_TOURNAMENT_SLUG.get(league)
            if ut_id is None or slug is None:
                logger.warning(
                    "No SofaScore slug/ut_id for league=%s — tournament "
                    "snapshot skipped.",
                    league,
                )
                continue
            season_short, target_year = _season_slug_and_target_year(
                league,
                self.seasons[0],
            )
            nav_url = f"https://www.sofascore.com/tournament/{slug}/{ut_id}"
            snapshot = self._capture_tournament_snapshot(
                nav_url,
                ut_id,
                league,
                season_short,
                target_year,
                want_events=True,
                want_standings=True,
            )

            events = snapshot["events"]
            if events:
                schedule = pd.DataFrame([normalize_event(ev) for ev in events])
                schedule["league"] = league
                schedule["season"] = season_short
                from dags.utils.sofascore_dq import validate_schedule_rows

                validate_schedule_rows(
                    schedule.to_dict('records')
                ).require()
                schedule_frames.append(schedule)
                logger.info(
                    "Tournament snapshot schedule league=%s season=%s: %d rows.",
                    league,
                    season_short,
                    len(schedule),
                )

            rows = snapshot["standings"]
            if rows:
                table = pd.DataFrame([normalize_standing(row) for row in rows])
                for col in ("mp", "w", "d", "l", "gf", "ga", "gd", "pts"):
                    table[col] = table[col].astype("Int64")
                table["league"] = league
                table["season"] = season_short
                standings_frames.append(table)
                logger.info(
                    "Tournament snapshot standings league=%s season=%s: %d rows.",
                    league,
                    season_short,
                    len(table),
                )

        schedule_out = None
        if schedule_frames:
            schedule_out = self._add_metadata(
                pd.concat(schedule_frames, ignore_index=True),
                "schedule",
            )
        standings_out = None
        if standings_frames:
            standings_out = self._add_metadata(
                pd.concat(standings_frames, ignore_index=True),
                "league_table",
            )
        return schedule_out, standings_out

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
        # Preserve the integer type so 2021 means the documented 2021/22 start
        # year while single-year tournaments retain their literal label.
        season_short = _season_label(league, season)
        season_tokens = [season_short]
        raw_token = str(season)
        # A raw start-year partition is a legacy alias only when normalising it
        # yields the exact same canonical season. In particular, int 2021 means
        # 21/22 while the string token "2021" is the real 20/21 season.
        if (
            raw_token != season_short
            and _season_label(league, raw_token) == season_short
        ):
            season_tokens.append(raw_token)

        if 'league' in df.columns and 'season' in df.columns:
            mask = (df['league'] == league) & (
                df['season'].astype(str).isin(season_tokens)
            )
            df = df[mask]

        # A live match already has ``home_score_current``; score presence is not
        # a completion signal. Prefer the source status and retain the old score
        # heuristic only for genuinely legacy frames that predate ``status_type``.
        if "status_type" in df.columns:
            df = df[df["status_type"] == "finished"]
        else:
            score_col = next(
                (c for c in ("home_score_current", "home_score") if c in df.columns),
                None,
            )
            if score_col is not None:
                df = df[df[score_col].notna()]

        if 'game_id' not in df.columns:
            return []

        return [str(int(g)) for g in df['game_id'].dropna().tolist()]

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
                # First-class lineup semantics.  An unused substitute is still
                # part of the player universe and must receive a profile even
                # though SofaScore supplies no statistics/rating for the match.
                'is_starter': not bool(entry.get('substitute')),
                'is_bench': bool(entry.get('substitute')),
                'is_unused_substitute': bool(entry.get('substitute')) and not bool(stats),
                'participation_status': (
                    'starter'
                    if not bool(entry.get('substitute'))
                    else ('substitute_used' if bool(stats) else 'unused_substitute')
                ),
            }
            _auto_flatten(entry, row, skip=('player', 'statistics'))
            rows.append(row)

        return rows

    def _camoufox_proxy(self) -> Optional[dict]:
        """Build a Camoufox/Playwright proxy dict (creds split out — browsers
        reject creds embedded in the URL) from the configured residential
        proxy. Returns ``None`` when none is configured; SofaScore's Turnstile
        then 403s every data XHR (#757), so a proxy is required in production.
        Reuses the platform proxy manager's rotation state.
        """
        proxy_obj = None
        if self._proxy_manager is not None and self._proxy_manager.total_count > 0:
            # Random pools may hand the just-failed endpoint straight back. Try
            # a few selections to get a genuinely different exit when possible.
            tries = min(max(int(self._proxy_manager.total_count), 1), 5)
            for _ in range(tries):
                candidate = self._proxy_manager.get_proxy()
                if candidate is None:
                    continue
                key = (
                    str(candidate.host),
                    int(candidate.port),
                    candidate.username or "",
                )
                proxy_obj = candidate
                if self._last_camoufox_proxy_key is None or key != self._last_camoufox_proxy_key:
                    break
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
        key = (str(proxy_obj.host), int(proxy_obj.port), proxy_obj.username or "")
        self._camoufox_proxy_objects[key] = proxy_obj
        self._last_camoufox_proxy_key = key
        logger.info("SofaScore capture proxy: %s", proxy_obj.masked_url)
        return d

    def _record_camoufox_proxy_result(
        self,
        proxy: Optional[dict],
        *,
        success: bool,
        error_type: str = "connection",
    ) -> None:
        """Feed browser outcomes back into ProxyManager health/cooldown state."""
        if not proxy or self._proxy_manager is None:
            return
        server = str(proxy.get("server") or "")
        parsed = urlsplit(server)
        key = (
            parsed.hostname or "",
            int(parsed.port or 0),
            proxy.get("username") or "",
        )
        proxy_obj = self._camoufox_proxy_objects.get(key)
        if proxy_obj is None:
            return
        record = getattr(self._proxy_manager, "record_result", None)
        try:
            if callable(record):
                record(
                    proxy_obj,
                    success=success,
                    error_type=None if success else error_type,
                )
            elif success:
                proxy_obj.record_success()
            else:
                proxy_obj.record_failure(error_type)
        except Exception:  # noqa: BLE001 — health telemetry must not break capture
            logger.debug("Could not record Camoufox proxy result", exc_info=True)

    @staticmethod
    def _event_endpoint_states(cap, event_id: str, endpoints: Dict, names) -> Dict[str, str]:
        """Read endpoint states from a real capture, with a fake-safe fallback."""
        states = {
            name: "success"
            for name in names
            if name in (endpoints or {})
        }
        read_states = getattr(cap, "event_endpoint_states", None)
        if callable(read_states):
            try:
                observed = read_states(event_id, names=names)
                if isinstance(observed, dict):
                    states.update(observed)
            except Exception:  # noqa: BLE001 — status telemetry must not break data
                logger.debug("Could not read event endpoint states", exc_info=True)
        return states

    @staticmethod
    def _merge_endpoint_states(*state_maps: Dict[str, str]) -> Dict[str, str]:
        """Merge retries without letting a transient miss erase a terminal answer."""
        merged: Dict[str, str] = {}
        for state_map in state_maps:
            for name, state in (state_map or {}).items():
                previous = merged.get(name)
                if previous == "success":
                    continue
                if state == "success" or previous not in _TERMINAL_CAPTURE_STATES:
                    merged[name] = state
                    continue
                # Keep an earlier terminal not_available over later transient
                # noise, but let a later success upgrade it (handled above).
        return merged

    def _iter_match_captures(
        self,
        match_ids: List[str],
        tabs=("Lineups",),
        required=("lineups",),
        session_max: int = 500,
        item_max_attempts: int = 2,
        endpoint_names_by_match: Optional[Dict[str, Tuple[str, ...]]] = None,
    ):
        """Yield ``(match_id, endpoints)`` by capturing each match page through a
        Camoufox session, restarted on a FRESH proxy every ``session_max``
        matches or immediately after a required-endpoint failure. The SAME match
        is retried on the fresh proxy up to ``item_max_attempts``; the old loop
        skipped four paid failures before rotating. ``endpoints`` holds whichever of
        ``event/lineups/statistics/shotmap`` came back as real JSON
        (see ``camoufox_capture.select_event_endpoints``).

        Two failure modes abort a long full-season backfill (380 matches) if the
        session never restarts:
        - Firefox accumulates memory across navigations and the browser dies
          ~200 page loads in (#829) — covered by the ``session_max`` restart.
        - the single residential proxy can die mid-run (``NS_ERROR_PROXY_*`` /
          ``CONNECTION_REFUSED``); a required-endpoint miss rotates immediately
          instead of spending more full navigations on the same exit.

        The daily run (a handful of matches) crosses neither threshold, so it
        keeps its single-session behaviour. The consolidated path gets the
        ``event`` payload (``homeTeam``/``awayTeam`` — team mapping for
        ``event_player_stats``) from the SAME navigation. The SPA fires its own
        XHRs and the session captures those responses. ``endpoints``
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
        item_attempts: Dict[str, int] = defaultdict(int)
        partials: Dict[str, Dict[str, dict]] = defaultdict(dict)
        partial_states: Dict[str, Dict[str, str]] = defaultdict(dict)
        partial_records: Dict[str, Dict[str, dict]] = defaultdict(dict)
        while i < n:
            proxy = self._camoufox_proxy()  # fresh proxy per (re)start (#832)
            if i:
                logger.info(
                    "match_capture: restarting Camoufox session on a fresh proxy "
                    "at match %d/%d (#829 memory / #832 proxy rotation).", i, n,
                )
            in_session = 0
            warmed = False  # True after this session's first full navigation
            rotate_current = False
            session_had_success = False
            session_had_failure = False
            try:
                with self._camoufox_session(proxy) as cap:
                    while i < n and in_session < session_max:
                        mid = match_ids[i]
                        current_fetch_names = tuple(
                            (endpoint_names_by_match or {}).get(mid, fetch_names)
                        )
                        current_required = tuple(
                            name for name in required if name in current_fetch_names
                        )
                        endpoints = None
                        fetch_partial: Dict[str, dict] = {}
                        fetch_records: Dict[str, dict] = {}
                        navigation_records: Dict[str, dict] = {}
                        fetch_states: Dict[str, str] = {}
                        navigation_states: Dict[str, str] = {}
                        fetch_attempted = use_fetch and warmed
                        if use_fetch and warmed:
                            try:
                                endpoints = cap.fetch_event(
                                    mid, names=current_fetch_names
                                )
                            except Exception as e:  # noqa: BLE001 — degrade to a full nav
                                logger.info(
                                    "in-page fetch failed for event=%s: %s", mid, e
                                )
                                endpoints = None
                            if endpoints is not None:
                                read_records = getattr(
                                    cap, 'event_endpoint_records', None
                                )
                                if callable(read_records):
                                    fetch_records = dict(
                                        read_records(
                                            mid, names=current_fetch_names
                                        ) or {}
                                    )
                                fetch_states = self._event_endpoint_states(
                                    cap,
                                    mid,
                                    endpoints,
                                    current_fetch_names,
                                )
                            if endpoints is not None and any(
                                fetch_states.get(name) not in _TERMINAL_CAPTURE_STATES
                                for name in current_required
                            ):
                                fetch_partial = endpoints
                                logger.info(
                                    "in-page fetch missed a required endpoint for "
                                    "event=%s — falling back to full navigation.",
                                    mid,
                                )
                                endpoints = None
                        if endpoints is None:
                            try:
                                if fetch_attempted:
                                    self._fallback_navigations += 1
                                endpoints = cap.capture_event(
                                    mid,
                                    tabs=tabs,
                                    names=current_fetch_names,
                                    required=current_required,
                                    max_attempts=1,
                                )
                                read_records = getattr(
                                    cap, 'event_endpoint_records', None
                                )
                                if callable(read_records):
                                    navigation_records = dict(
                                        read_records(
                                            mid, names=current_fetch_names
                                        ) or {}
                                    )
                                warmed = True
                                navigation_states = self._event_endpoint_states(
                                    cap,
                                    mid,
                                    endpoints,
                                    current_fetch_names,
                                )
                            except Exception as e:  # noqa: BLE001 — one bad event mustn't kill the loop
                                logger.warning(
                                    "camoufox capture failed for event=%s: %s", mid, e
                                )
                                endpoints = {}
                        combined = {
                            **partials[mid],
                            **fetch_partial,
                            **(endpoints or {}),
                        }
                        combined_records = {
                            **partial_records[mid],
                            **fetch_records,
                            **navigation_records,
                        }
                        combined_states = self._merge_endpoint_states(
                            partial_states[mid],
                            fetch_states,
                            navigation_states,
                            {
                                name: "success"
                                for name in current_fetch_names
                                if name in combined
                            },
                        )
                        optional_retry = [
                            name
                            for name in current_fetch_names
                            if name not in current_required
                            and combined_states.get(name)
                            not in _TERMINAL_CAPTURE_STATES
                        ]
                        if optional_retry and use_fetch and warmed:
                            # Optional misses must not trigger another ~2 MB SPA
                            # navigation/proxy rotation. Retry their exact JSON
                            # paths once on the already-cleared page instead.
                            try:
                                optional_payloads = cap.fetch_event(
                                    mid,
                                    names=tuple(optional_retry),
                                )
                                read_records = getattr(
                                    cap, 'event_endpoint_records', None
                                )
                                optional_records = (
                                    dict(read_records(
                                        mid, names=tuple(optional_retry)
                                    ) or {})
                                    if callable(read_records)
                                    else {}
                                )
                            except Exception as exc:  # noqa: BLE001 — status stays retryable
                                logger.info(
                                    "optional endpoint retry failed event=%s names=%s: %s",
                                    mid,
                                    optional_retry,
                                    exc,
                                )
                            else:
                                optional_states = self._event_endpoint_states(
                                    cap,
                                    mid,
                                    optional_payloads,
                                    optional_retry,
                                )
                                combined.update(optional_payloads or {})
                                combined_records.update(optional_records)
                                combined_states = self._merge_endpoint_states(
                                    combined_states,
                                    optional_states,
                                    {
                                        name: "success"
                                        for name in optional_retry
                                        if name in combined
                                    },
                                )
                        missing = [
                            name
                            for name in current_required
                            if combined_states.get(name)
                            not in _TERMINAL_CAPTURE_STATES
                        ]
                        if missing:
                            session_had_failure = True
                            self._stats["failures"] += 1
                            self._last_lineup_error = {
                                "event_id": mid,
                                "status": None,
                                "error": "lineups_not_captured",
                            }
                            item_attempts[mid] += 1
                            partials[mid] = combined
                            partial_states[mid] = combined_states
                            partial_records[mid] = combined_records
                            if item_attempts[mid] < item_max_attempts:
                                logger.warning(
                                    "match_capture event=%s missing=%s; rotating "
                                    "proxy and retrying the same match (%d/%d).",
                                    mid,
                                    missing,
                                    item_attempts[mid],
                                    item_max_attempts,
                                )
                                rotate_current = True
                                break
                        else:
                            session_had_success = True
                            self._stats["successes"] += 1
                        yield mid, {
                            **combined,
                            "_endpoint_states": combined_states,
                            "_endpoint_records": combined_records,
                        }
                        partials.pop(mid, None)
                        partial_states.pop(mid, None)
                        partial_records.pop(mid, None)
                        item_attempts.pop(mid, None)
                        i += 1
                        in_session += 1
                if session_had_failure:
                    self._record_camoufox_proxy_result(
                        proxy,
                        success=False,
                        error_type="unknown",
                    )
                elif session_had_success:
                    self._record_camoufox_proxy_result(proxy, success=True)
            except Exception as exc:  # session start/browser failure
                self._record_camoufox_proxy_result(proxy, success=False)
                if i >= n:
                    logger.warning(
                        "match_capture session teardown failed after all items: %s",
                        exc,
                    )
                    break
                mid = match_ids[i]
                item_attempts[mid] += 1
                logger.warning(
                    "match_capture session failed at event=%s (%d/%d): %s",
                    mid,
                    item_attempts[mid],
                    item_max_attempts,
                    exc,
                )
                if item_attempts[mid] >= item_max_attempts:
                    failed_capture = partials.pop(mid, {})
                    yield mid, {
                        **failed_capture,
                        "_endpoint_states": partial_states.pop(mid, {}),
                        "_endpoint_records": partial_records.pop(mid, {}),
                    }
                    item_attempts.pop(mid, None)
                    i += 1
            if rotate_current:
                continue

    def _iter_player_captures(
        self,
        player_ids: List[str],
        target_ut=None,
        target_year=None,
        session_max: int = 250,
        item_max_attempts: int = 2,
    ):
        """Yield ``(player_id, capture)`` through ONE warmed Camoufox session
        (#751 PR3 + PR3b). ``capture`` is the ``{'profile', 'season_buffer'}``
        dict — the bio plus the season-stats capture. Both warmed-navigation and
        in-page paths resolve the exact ``target_ut``/``target_year`` APIs; no
        localized tab/picker interaction is involved. A ``{'profile': None}``
        means the capture
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

        player_ids = [str(player_id) for player_id in player_ids]
        attempts: Dict[str, int] = defaultdict(int)
        i = 0
        while i < len(player_ids):
            proxy = self._camoufox_proxy()
            warmed = False
            in_session = 0
            rotate_current = False
            session_had_success = False
            session_had_failure = False
            try:
                with self._camoufox_session(proxy) as cap:
                    while i < len(player_ids) and in_session < session_max:
                        pid = player_ids[i]
                        capture = None
                        fetch_attempted = use_fetch and warmed
                        if use_fetch and warmed:
                            try:
                                capture = cap.fetch_player(
                                    str(pid),
                                    target_ut=target_ut,
                                    target_year=target_year,
                                )
                            except Exception as e:  # noqa: BLE001 — degrade to a full nav
                                logger.info(
                                    "in-page fetch failed for player=%s: %s", pid, e
                                )
                                capture = None
                            if capture is not None and not capture.get("profile"):
                                logger.info(
                                    "in-page fetch missed profile for player=%s — "
                                    "falling back to full navigation.",
                                    pid,
                                )
                                capture = None
                        if capture is None:
                            try:
                                if fetch_attempted:
                                    self._fallback_navigations += 1
                                capture = cap.capture_player(
                                    str(pid),
                                    target_ut=target_ut,
                                    target_year=target_year,
                                )
                                warmed = True
                            except Exception as e:  # noqa: BLE001 — one bad player mustn't kill the loop
                                logger.warning(
                                    "camoufox capture failed for player=%s: %s", pid, e
                                )
                                capture = {"profile": None, "season_buffer": {}}
                        if not capture.get("profile"):
                            session_had_failure = True
                            self._stats["failures"] += 1
                            self._last_lineup_error = {
                                "event_id": None,
                                "player_id": str(pid),
                                "status": None,
                                "error": "player_not_captured",
                            }
                            attempts[pid] += 1
                            if attempts[pid] < item_max_attempts:
                                logger.warning(
                                    "player_capture player=%s missed; rotating proxy and "
                                    "retrying (%d/%d).",
                                    pid,
                                    attempts[pid],
                                    item_max_attempts,
                                )
                                rotate_current = True
                                break
                        else:
                            session_had_success = True
                            self._stats["successes"] += 1
                        yield pid, capture
                        attempts.pop(pid, None)
                        i += 1
                        in_session += 1
                if session_had_failure:
                    self._record_camoufox_proxy_result(
                        proxy,
                        success=False,
                        error_type="unknown",
                    )
                elif session_had_success:
                    self._record_camoufox_proxy_result(proxy, success=True)
            except Exception as exc:
                self._record_camoufox_proxy_result(proxy, success=False)
                if i >= len(player_ids):
                    logger.warning(
                        "player_capture session teardown failed after all items: %s",
                        exc,
                    )
                    break
                pid = player_ids[i]
                attempts[pid] += 1
                logger.warning(
                    "player_capture session failed at player=%s (%d/%d): %s",
                    pid,
                    attempts[pid],
                    item_max_attempts,
                    exc,
                )
                if attempts[pid] >= item_max_attempts:
                    yield pid, {"profile": None, "season_buffer": {}}
                    attempts.pop(pid, None)
                    i += 1
            if rotate_current:
                continue

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
        for attempt in range(1, self._CAPTURE_ATTEMPTS + 1):
            proxy = self._camoufox_proxy()
            try:
                with self._camoufox_session(proxy) as cap:
                    buffer = cap.capture_buffer(nav_url)
                    sid = self._resolve_target_sid(
                        cap,
                        buffer,
                        ut_id,
                        target_year,
                    )
                    if sid is None:
                        self._record_camoufox_proxy_result(
                            proxy,
                            success=False,
                            error_type="unknown",
                        )
                        logger.warning(
                            "Finished-match season unresolved league=%s year=%s "
                            "(attempt %d/%d).",
                            league,
                            target_year,
                            attempt,
                            self._CAPTURE_ATTEMPTS,
                        )
                        continue
                    buffer = cap.paginate_tournament_season(ut_id, int(sid))
                    event_prefix = (
                        f"/api/v1/unique-tournament/{int(ut_id)}/season/"
                        f"{int(sid)}/events"
                    )
                    complete = self._event_direction_complete(
                        buffer,
                        event_prefix,
                        "last",
                    )
            except Exception as exc:  # noqa: BLE001 — rotate and retry
                self._record_camoufox_proxy_result(proxy, success=False)
                logger.warning(
                    "Finished-match capture failed league=%s attempt=%d/%d: %s",
                    league,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                    exc,
                )
                continue

            self._record_camoufox_proxy_result(
                proxy,
                success=complete,
                error_type="unknown",
            )
            if not complete:
                logger.warning(
                    "Finished-match pagination incomplete league=%s sid=%s "
                    "(attempt %d/%d).",
                    league,
                    sid,
                    attempt,
                    self._CAPTURE_ATTEMPTS,
                )
                continue

            events = [
                event
                for event in extract_tournament_events(buffer, ut_id)
                if (event.get("season") or {}).get("year") == target_year
            ]
            match_ids = finished_event_ids(events)
            logger.info(
                "Capture schedule league=%s season=%s (year=%s): %d ut=%d "
                "events, %d finished.",
                league,
                season,
                target_year,
                len(events),
                ut_id,
                len(match_ids),
            )
            return match_ids

        logger.error(
            "Finished-match pagination exhausted retries league=%s year=%s.",
            league,
            target_year,
        )
        return []

    # ------------------------------------------------------------------
    # #751 PR1 — consolidated per-match capture (one nav → ratings + eps)
    # ------------------------------------------------------------------

    def read_match_capture(
        self,
        league: str,
        season: int,
        match_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        endpoint_names_by_match: Optional[Dict[str, Tuple[str, ...]]] = None,
    ) -> Dict[str, object]:
        """ONE Camoufox capture pass per match → five Bronze frames (#751 PR1+PR2, #753).

        Replaces four separate Turnstile-blocked passes with a warmed browser
        session plus exact per-match API fetches. The same pass captures
        ``/lineups`` + ``/event`` + ``/statistics`` + ``/shotmap``:
          - ``player_ratings`` — :meth:`_flatten_lineup_side` over ``/lineups``;
          - ``event_player_stats`` — :meth:`_flatten_event_player_stats_from_lineups`
            over ``/lineups`` (per-player Opta block), with ``team_id``/
            ``team_name`` from ``/event`` (``homeTeam``/``awayTeam``);
          - ``match_stats`` — :meth:`_flatten_match_stats` over ``/statistics``;
          - ``event_shotmap`` — :meth:`_flatten_shotmap` over ``/shotmap``;
          - ``events`` / ``event_participants`` / ``venue`` from ``/event``;
          - ``incidents`` — goals/cards/substitutions/VAR from ``/incidents``;
          - ``venue`` — :meth:`_flatten_event_venue` over ``/event`` (#753:
            one row per match, stadium/city/country/coords).

        statistics/shotmap/venue are best-effort: a pass that doesn't fire them
        just yields an empty frame for that table (the others still materialise).

        Returns the five data frames plus ``capture_status`` (one endpoint-state
        row per attempted match). The status manifest distinguishes terminal
        empty/not-available answers from transient misses, so the runner can
        retry only genuinely incomplete matches without re-downloading valid
        no-shot/no-venue matches. Season slug is coerced to the soccerdata short
        form (``2526``) so the partition matches the schedule writer (#27).
        """
        ratings_cols = [
            # #840: rating/position kept; entry-level fields now preserved too.
            'match_id', 'player_id', 'team_side', 'rating', 'position',
            'captain', 'substitute', 'shirt_number', 'league', 'season',
        ]
        lineup_cols = [
            'match_id', 'player_id', 'team_side', 'position', 'captain',
            'substitute', 'shirt_number', 'is_starter', 'is_bench',
            'is_unused_substitute', 'participation_status', 'league', 'season',
        ]
        eps_cols = [
            'match_id', 'player_id', 'team_id', 'team_name', 'is_home',
            'position', 'position_specific', 'captain', 'substitute',
            'league', 'season',
        ]
        match_stats_cols = [
            # #840: source-key names (Bronze as-is); Silver renames/derives.
            'match_id', 'period', 'stat_group', 'statistic_key', 'name', 'key',
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
        event_cols = [
            'match_id', 'id', 'season_id', 'home_team_id', 'away_team_id',
            'start_timestamp', 'status_type', 'league', 'season',
        ]
        participant_cols = [
            'match_id', 'team_id', 'team_side', 'name', 'slug',
            'gender', 'team_type', 'league', 'season',
        ]
        incident_cols = [
            'match_id', 'incident_id', 'incident_order', 'incident_type',
            'time', 'added_time',
            'is_home', 'player_id', 'player_in_id', 'player_out_id',
            'incident_class', 'reason', 'league', 'season',
        ]
        status_cols = [
            'match_id', 'event_status', 'lineups_status',
            'statistics_status', 'shotmap_status', 'incidents_status',
            'capture_complete',
            'league', 'season',
        ]

        if match_ids is None:
            match_ids = self._resolve_match_ids(league, season)

        season_short = _season_label(league, season)

        empty = {
            'player_ratings': pd.DataFrame(columns=ratings_cols + ['_ingested_at']),
            'lineups': pd.DataFrame(columns=lineup_cols + ['_ingested_at']),
            'event_player_stats': pd.DataFrame(columns=eps_cols + ['_ingested_at']),
            'match_stats': pd.DataFrame(columns=match_stats_cols + ['_ingested_at']),
            'event_shotmap': pd.DataFrame(columns=shotmap_cols + ['_ingested_at']),
            'venue': pd.DataFrame(columns=venue_cols + ['_ingested_at']),
            'events': pd.DataFrame(columns=event_cols + ['_ingested_at']),
            'event_participants': pd.DataFrame(
                columns=participant_cols + ['_ingested_at']
            ),
            'incidents': pd.DataFrame(columns=incident_cols + ['_ingested_at']),
            'capture_status': pd.DataFrame(columns=status_cols + ['_ingested_at']),
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
        event_rows: List[Dict] = []
        participant_rows: List[Dict] = []
        incident_rows: List[Dict] = []
        status_rows: List[Dict] = []
        raw_records: Dict[str, dict] = {}
        consecutive_failures = 0
        max_consecutive = 10  # ~dead proxy / Turnstile not solved — bail early.

        # Request every endpoint family in the same pass. A terminal 404 counts
        # as answered; transient misses rotate once and remain incomplete in the
        # manifest so a later daily run can retry them.
        captures = self._iter_match_captures(
            match_ids,
            tabs=("Lineups", "Statistics", "Shotmap", "Incidents"),
            required=("lineups", "event"),
            endpoint_names_by_match=endpoint_names_by_match,
        )
        try:
            for idx, (mid, endpoints) in enumerate(captures, start=1):
                endpoints = endpoints or {}
                for endpoint_name, record in dict(
                    endpoints.get('_endpoint_records') or {}
                ).items():
                    raw_records[f'{mid}:{endpoint_name}'] = {
                        'match_id': str(mid),
                        'endpoint': str(endpoint_name),
                        **dict(record),
                    }
                endpoint_states = dict(endpoints.get("_endpoint_states") or {})
                endpoint_names = (
                    "event", "lineups", "statistics", "shotmap", "incidents"
                )
                requested_names = set(
                    (endpoint_names_by_match or {}).get(str(mid), endpoint_names)
                )
                for endpoint_name in endpoint_names:
                    if endpoint_name not in requested_names:
                        # endpoint-level resume only omits endpoints already
                        # terminal in the canonical long manifest.
                        endpoint_states[endpoint_name] = 'success'
                        continue
                    if endpoint_name in endpoints:
                        endpoint_states[endpoint_name] = "success"
                    else:
                        endpoint_states.setdefault(
                            endpoint_name,
                            # Compatibility for custom transports built before
                            # incidents was added. The production transport
                            # always reports an explicit state, so a real
                            # transport miss remains retryable.
                            "not_available"
                            if endpoint_name == "incidents"
                            and "_endpoint_states" not in endpoints
                            else "missing",
                        )
                status_rows.append(
                    {
                        "match_id": str(mid),
                        **{
                            f"{name}_status": endpoint_states[name]
                            for name in endpoint_names
                        },
                        "capture_complete": all(
                            endpoint_states[name] in _TERMINAL_CAPTURE_STATES
                            for name in endpoint_names
                        ),
                    }
                )
                event_payload = endpoints.get("event")

                # Materialise independent endpoints even if lineups missed. The
                # proxy bytes for event/statistics/shotmap have already been
                # spent; dropping valid payloads here created avoidable holes.
                venue_row = self._flatten_event_venue(str(mid), event_payload)
                if venue_row:
                    venue_rows.append(venue_row)
                event_row = self._flatten_full_event(str(mid), event_payload)
                if event_row:
                    event_rows.append(event_row)
                participant_rows.extend(
                    self._flatten_event_participants(str(mid), event_payload)
                )
                statistics = endpoints.get("statistics")
                if statistics is not None:
                    stats_rows.extend(self._flatten_match_stats(str(mid), statistics))
                shotmap = endpoints.get("shotmap")
                if shotmap is not None:
                    shot_rows.extend(self._flatten_shotmap(str(mid), shotmap))
                incidents = endpoints.get('incidents')
                if incidents is not None:
                    incident_rows.extend(
                        self._flatten_incidents(str(mid), incidents)
                    )

                lineups = endpoints.get("lineups")
                if lineups is None:
                    if endpoint_states.get("lineups") in _TERMINAL_CAPTURE_STATES:
                        # A terminal 404/204 is a completed source answer, not a
                        # dead proxy. Keep its manifest row and continue.
                        consecutive_failures = 0
                        continue
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
                for side in ("home", "away"):
                    ratings_rows.extend(
                        self._flatten_lineup_side(
                            match_id=str(mid),
                            side=side,
                            side_payload=lineups.get(side) or {},
                        )
                    )
                eps_rows.extend(
                    self._flatten_event_player_stats_from_lineups(
                        str(mid),
                        lineups,
                        event_payload,
                    )
                )

                if idx % 25 == 0:
                    logger.info("match_capture progress: %d/%d matches",
                                idx, len(match_ids))
        finally:
            close = getattr(captures, 'close', None)
            if callable(close):
                close()  # tear down the Camoufox session

        out: Dict[str, object] = {}
        out['raw_records'] = raw_records

        if ratings_rows:
            rdf = pd.DataFrame(ratings_rows)
            anchors = list(ratings_cols)
            rdf = rdf.reindex(
                columns=anchors + [c for c in rdf.columns if c not in anchors],
            )
            rdf["league"] = league
            rdf["season"] = season_short
            rdf["_ingested_at"] = datetime.utcnow()
            rdf["_source"] = self.SOURCE_NAME
            rdf["_entity_type"] = "player_ratings"
            rdf["_batch_id"] = self._batch_id
            out["player_ratings"] = rdf

            # The exact same /lineups payload is materialised a second time at
            # its natural grain.  This is a zero-network projection and keeps
            # starters, used bench and unused substitutes queryable without
            # inferring participation from a nullable rating.
            lineup_df = rdf.drop(columns=['rating'], errors='ignore').copy()
            lineup_df['_entity_type'] = 'lineups'
            out['lineups'] = lineup_df
        else:
            out['player_ratings'] = empty['player_ratings']
            out['lineups'] = empty['lineups']

        if eps_rows:
            edf = pd.DataFrame(eps_rows)
            edf = edf.reindex(
                columns=eps_cols + [c for c in edf.columns if c not in eps_cols],
            )
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
        def _tag(
            rows: List[Dict],
            entity_type: str,
            required_columns: List[str],
        ) -> pd.DataFrame:
            df = pd.DataFrame(rows)
            df = df.reindex(
                columns=(
                    required_columns
                    + [column for column in df.columns if column not in required_columns]
                ),
            )
            df['league'] = league
            df['season'] = season_short
            df['_ingested_at'] = datetime.utcnow()
            df['_source'] = self.SOURCE_NAME
            df['_entity_type'] = entity_type
            df['_batch_id'] = self._batch_id
            return df

        out['match_stats'] = (
            _tag(stats_rows, 'match_stats', match_stats_cols)
            if stats_rows else empty['match_stats'])
        out['event_shotmap'] = (
            _tag(
                shot_rows,
                'event_shotmap',
                shotmap_cols + ['minute', 'x', 'y'],
            ) if shot_rows else empty['event_shotmap'])
        out['venue'] = (
            _tag(
                venue_rows,
                'venue',
                venue_cols + [
                    'stadium', 'city', 'country',
                    'venue_latitude', 'venue_longitude',
                ],
            ) if venue_rows else empty['venue'])
        out['events'] = (
            _tag(event_rows, 'events', event_cols)
            if event_rows else empty['events']
        )
        out['event_participants'] = (
            _tag(
                participant_rows,
                'event_participants',
                participant_cols,
            ) if participant_rows else empty['event_participants']
        )
        out['incidents'] = (
            _tag(incident_rows, 'incidents', incident_cols)
            if incident_rows else empty['incidents']
        )
        out['capture_status'] = (
            _tag(status_rows, 'match_capture_status', status_cols)
            if status_rows
            else empty['capture_status']
        )

        from dags.utils.sofascore_dq import (
            validate_event_participants,
            validate_lineup_semantics,
        )

        if not out['lineups'].empty:
            validate_lineup_semantics(
                out['lineups'].to_dict('records')
            ).require()
        if not out['event_participants'].empty:
            validate_event_participants(
                out['event_participants'].to_dict('records')
            ).require()
        if not out['event_player_stats'].empty:
            from dags.utils.sofascore_dq import validate_minimum_coverage

            appeared = {
                (str(row.get('match_id')), str(row.get('player_id')))
                for row in out['event_player_stats'].to_dict('records')
                if (row.get('minutes_played') or 0) > 0
            }
            rated = {
                (str(row.get('match_id')), str(row.get('player_id')))
                for row in out['player_ratings'].to_dict('records')
                if (row.get('rating') or 0) > 0
            }
            validate_minimum_coverage(
                'player_rating', rated, appeared, threshold=0.95
            ).require()

        if not ratings_rows and not eps_rows:
            all_terminal = bool(status_rows) and all(
                bool(row.get("capture_complete")) for row in status_rows
            )
            if all_terminal:
                logger.warning(
                    "match_capture received terminal empty/not-available answers "
                    "for all %d attempted matches.",
                    len(status_rows),
                )
            else:
                logger.warning(
                    "%s: match_capture materialised zero rows across %d attempts.",
                    R0_2B_FALLBACK_MARKER,
                    len(match_ids),
                )

        logger.info(
            "match_capture: %d ratings + %d eps + %d match_stats + %d shots "
            "+ %d incidents + %d events + %d venues across %d matches",
            len(out['player_ratings']), len(out['event_player_stats']),
            len(out['match_stats']), len(out['event_shotmap']),
            len(out['incidents']), len(out['events']), len(out['venue']),
            len(match_ids),
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

        fallback_occurrences: Dict[str, int] = defaultdict(int)
        for shot in shots:
            if not isinstance(shot, dict):
                continue

            player = shot.get('player') or {}

            # --- PK: shot id, composite fallback when SofaScore omits id ---
            sid = shot.get('id')
            if sid is None:
                # Fall back to composite (match, time, player) so that
                # downstream PK stays unique even when SofaScore omits id.
                fallback_id = (
                    f"{match_id}-"
                    f"{shot.get('time', 'NA')}-"
                    f"{player.get('id', 'NA')}-"
                    f"{shot.get('addedTime', 0)}"
                )
                fallback_occurrences[fallback_id] += 1
                occurrence = fallback_occurrences[fallback_id]
                # Preserve the historical id for the first shot; only genuine
                # collisions receive a deterministic source-order suffix.
                sid = fallback_id if occurrence == 1 else f"{fallback_id}-{occurrence}"
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

            row.setdefault('minute', row.get('time'))
            row.setdefault('x', row.get('player_coordinates_x'))
            row.setdefault('y', row.get('player_coordinates_y'))

            rows.append(row)

        return rows

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
        # `position` re-export (already projected above).
        for raw_key, raw_val in stats.items():
            if raw_key == 'position':
                continue
            snake = _camel_to_snake(str(raw_key))
            if snake in row:
                # Don't overwrite anchor columns (player_id, team_id, ...).
                continue
            row[snake] = _coerce_scalar(raw_val)

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
        the full per-match ``statistics`` block (33 Opta metrics) plus
        ``is_home`` (from the side) and the entry's
        ``captain``/``substitute``/``position`` anchors. This single payload
        populates them directly.

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

                rows.append(row)

        return rows

    @staticmethod
    def _unwrap_event_payload(payload) -> Dict:
        """Return the source event object from either API envelope shape."""
        value = payload if isinstance(payload, dict) else {}
        if isinstance(value.get('event'), dict):
            return value['event']
        return value

    @classmethod
    def _flatten_full_event(cls, match_id: str, payload) -> Optional[Dict]:
        """Preserve every scalar match-metadata field at one-row/event grain."""
        event = cls._unwrap_event_payload(payload)
        if not event:
            return None
        row: Dict = {'match_id': str(match_id)}
        _auto_flatten(event, row)
        for anchor in (
            'id', 'season_id', 'home_team_id', 'away_team_id',
            'start_timestamp', 'status_type',
        ):
            row.setdefault(anchor, None)
        return row

    @classmethod
    def _flatten_event_participants(cls, match_id: str, payload) -> List[Dict]:
        """Return home/away participant teams from the full event payload."""
        event = cls._unwrap_event_payload(payload)
        rows: List[Dict] = []
        for side, source_key in (('home', 'homeTeam'), ('away', 'awayTeam')):
            team = event.get(source_key)
            if not isinstance(team, dict) or team.get('id') is None:
                continue
            team_id = team.get('id')
            row: Dict = {
                'match_id': str(match_id),
                'team_id': str(int(team_id)) if isinstance(team_id, (int, float)) else str(team_id),
                'team_side': side,
                'name': team.get('name'),
                'gender': team.get('gender'),
                'team_type': team.get('teamType') or team.get('type'),
            }
            _auto_flatten(team, row)
            rows.append(row)
        return rows

    @staticmethod
    def _flatten_incidents(match_id: str, payload) -> List[Dict]:
        """Normalize goals, cards, substitutions and VAR without dropping raw.

        Exact source JSON is retained by the raw store; this projection exposes
        all scalar/nested-object fields and a deterministic natural key when an
        incident has no source ``id``.
        """
        if not isinstance(payload, dict):
            return []
        incidents = payload.get('incidents')
        if not isinstance(incidents, list):
            return []
        rows: List[Dict] = []
        for index, incident in enumerate(incidents):
            if not isinstance(incident, dict):
                continue
            source_id = incident.get('id')
            if source_id is None:
                canonical = json.dumps(
                    incident,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(',', ':'),
                ).encode('utf-8')
                source_id = f"derived-{index}-{hashlib.sha256(canonical).hexdigest()[:16]}"
            row: Dict = {
                'match_id': str(match_id),
                'incident_id': str(source_id),
                'incident_order': index,
                'incident_type': str(
                    incident.get('incidentType')
                    or incident.get('type')
                    or 'unknown'
                ),
            }
            _auto_flatten(incident, row)
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
        row.setdefault('stadium', row.get('stadium_name'))
        row.setdefault('city', row.get('city_name'))
        row.setdefault('country', row.get('country_name'))
        row.setdefault(
            'venue_latitude', row.get('venue_coordinates_latitude')
        )
        row.setdefault(
            'venue_longitude', row.get('venue_coordinates_longitude')
        )
        return row

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

            for group_index, group_block in enumerate(
                period_block.get('groups') or []
            ):
                if not isinstance(group_block, dict):
                    continue
                stat_group = str(group_block.get('groupName') or 'ungrouped')

                for item_index, item in enumerate(
                    group_block.get('statisticsItems') or []
                ):
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
                        # Stable non-null natural key for incremental MERGE.
                        # Source keys win; the positional fallback remains
                        # deterministic within the source-ordered payload.
                        'statistic_key': str(
                            item.get('key')
                            or item.get('statisticsType')
                            or item.get('name')
                            or f'{group_index}:{item_index}'
                        ),
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
                    row.setdefault('stat_name', row.get('name'))
                    row.setdefault(
                        'stat_key', row.get('key') or row.get('statistics_type')
                    )
                    row.setdefault('home_text', row.get('home'))
                    row.setdefault('away_text', row.get('away'))
                    rows.append(row)

        return rows

    # SofaScore unique-tournament id per league — used for exact season/API
    # targeting and finished-match discovery.
    def _resolve_unique_tournament_id(self, league: str) -> Optional[int]:
        return SOFASCORE_TOURNAMENT_MAP.get(league)

    def _resolve_player_ids_from_bronze(
        self,
        league: str,
        season_short: str,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Resolve the complete match participant universe.

        Ratings are not a player-universe table: unused substitutes commonly
        have a null rating.  Prefer the first-class lineup table, then the
        lineup-derived event-player rows, and retain ratings as a deployment
        compatibility source. Incident actors (including substitutions and
        assists) close match-only gaps. Missing optional tables are discovered
        through ``information_schema`` before building the UNION, so a rolling
        upgrade never turns a missing table into an empty paid player capture.
        """
        try:
            import os
            import trino
            import trino.auth as trino_auth
        except ImportError as e:  # pragma: no cover
            raise RuntimeError("trino client unavailable for player universe") from e

        user = os.environ.get('TRINO_USER', 'airflow')
        password = os.environ.get('TRINO_PASSWORD')
        conn = None

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
                "SELECT table_name FROM iceberg.information_schema.tables "
                "WHERE table_schema = 'bronze' AND table_name IN "
                "('sofascore_lineups', 'sofascore_event_player_stats', "
                "'sofascore_player_ratings', 'sofascore_incidents')"
            )
            available = {str(row[0]) for row in cur.fetchall() if row and row[0]}
            ordered = (
                'sofascore_lineups',
                'sofascore_event_player_stats',
                'sofascore_player_ratings',
            )
            sources = [table for table in ordered if table in available]
            fragments = [
                (
                    "SELECT CAST(player_id AS varchar) AS player_id "
                    f"FROM iceberg.bronze.{table} "
                    "WHERE league = ? AND CAST(season AS varchar) = ? "
                    "AND player_id IS NOT NULL"
                )
                for table in sources
            ]
            if 'sofascore_incidents' in available:
                cur.execute(
                    "SELECT column_name FROM iceberg.information_schema.columns "
                    "WHERE table_schema = 'bronze' "
                    "AND table_name = 'sofascore_incidents'"
                )
                incident_columns = {
                    str(row[0]) for row in cur.fetchall() if row and row[0]
                }
                for column in (
                    'player_id',
                    'player_in_id',
                    'player_out_id',
                    'assist1_id',
                    'assist2_id',
                ):
                    if column not in incident_columns:
                        continue
                    fragments.append(
                        f"SELECT CAST({column} AS varchar) AS player_id "
                        "FROM iceberg.bronze.sofascore_incidents "
                        "WHERE league = ? AND CAST(season AS varchar) = ? "
                        f"AND {column} IS NOT NULL"
                    )
            if not fragments:
                logger.warning("No SofaScore match-player Bronze tables exist yet")
                return []
            union = " UNION ALL ".join(fragments)
            sql = f"SELECT DISTINCT player_id FROM ({union}) players ORDER BY player_id"
            if limit:
                sql = sql + f" LIMIT {int(limit)}"
            params = tuple(
                value
                for _fragment in fragments
                for value in (league, season_short)
            )
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
        except Exception as e:
            raise RuntimeError(
                f"could not resolve player_ids from bronze: {e}"
            ) from e
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

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
        row.setdefault('height_cm', row.get('height'))
        dob_timestamp = row.get('date_of_birth_timestamp')
        if dob_timestamp is not None:
            try:
                row.setdefault(
                    'date_of_birth',
                    datetime.utcfromtimestamp(int(dob_timestamp)).date().isoformat(),
                )
            except (OverflowError, TypeError, ValueError):
                row.setdefault('date_of_birth', None)
        else:
            row.setdefault('date_of_birth', None)
        row.setdefault('country_code', row.get('country_alpha2'))
        row.setdefault('nationality', None)
        row.setdefault('current_team_id', row.get('team_id'))
        row.setdefault('current_team_name', row.get('team_name'))
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

        Replaces the blocked tls player_profile pass. #842: only the first
        player in each bounded browser session navigates (solving Turnstile);
        later players pull ``/api/v1/player/{id}`` via same-origin in-page
        fetch (~30 KB vs ~2 MB per navigation) — the page SSRs the identical
        ``player`` object into ``__NEXT_DATA__``, so both paths feed
        :meth:`_flatten_player_profile` the same shape. A fetch miss falls
        back to a full navigation for that player.

        Season-aggregate stats (``player_season_stats``) come from the SAME
        pass (#751 PR3b): the fetch path resolves the exact ``(ut, season_id)``
        from ``/statistics/seasons`` and pulls its ``statistics/overall``.
        This avoids the page's default competition, which is not necessarily
        the requested league for a transferred player. The right overall is
        selected season-guarded
        via the pure :func:`select_player_season_stats` /
        :func:`extract_player_seasons_map`.

        Returns ``{'player_profile', 'player_season_stats'}``. The profile is the
        primary deliverable; season-stats may be a strict subset when no exact
        aggregate exists — a WARN, not a failure. Empty frames on graceful
        fallback (caller emits ``R0.2B_FALLBACK``). Season slug is coerced to the
        soccerdata short form (``2526``) so the partition matches.
        """
        from scrapers.sofascore.camoufox_capture import (
            extract_player_seasons_map,
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

        season_short, target_season_label = _season_slug_and_target_year(
            league,
            season,
        )

        empty = {
            'player_profile': pd.DataFrame(columns=profile_cols + ['_ingested_at']),
            'player_season_stats': pd.DataFrame(columns=season_cols + ['_ingested_at']),
        }

        # Target competition for exact season-statistics API resolution.
        target_ut = self._resolve_unique_tournament_id(league)
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
            player_ids,
            target_ut=target_ut,
            target_year=target_season_label,
        )
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

                # Season-aggregate stats are best-effort: no exact tournament/
                # season aggregate means no row for that player (a WARN).
                if target_ut:
                    season_buffer = (capture or {}).get('season_buffer') or {}
                    if season_buffer:
                        seasons_map = extract_player_seasons_map(season_buffer, pid)
                        target_sid = seasons_map.get(target_ut, {}).get(
                            target_season_label
                        )
                        # Without the exact target sid, selector fallback means
                        # "latest available". Labelling that payload with the
                        # requested partition silently contaminates seasons.
                        if target_sid is not None:
                            sel = select_player_season_stats(
                                season_buffer, pid, target_ut, target_sid
                            )
                            if sel is not None:
                                ut, sid, payload = sel
                                srow = self._flatten_player_season_stats(
                                    pid, ut, sid, payload
                                )
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

        from dags.utils.sofascore_dq import validate_minimum_coverage

        validate_minimum_coverage(
            'player_profile',
            {str(row.get('player_id')) for row in profile_rows},
            {str(player_id) for player_id in player_ids},
            threshold=0.95,
        ).require()

        df = pd.DataFrame(profile_rows)
        profile_compat_cols = [
            'height_cm', 'date_of_birth', 'country_code',
            'current_team_id', 'current_team_name',
        ]
        df = df.reindex(
            columns=(
                profile_cols
                + profile_compat_cols
                + [
                    column
                    for column in df.columns
                    if column not in profile_cols + profile_compat_cols
                ]
            ),
        )
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
