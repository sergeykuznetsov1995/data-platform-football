"""
WhoScored Scraper
=================

FlareSolverr-backed scraper for WhoScored (2026-05 architecture):

* **schedule / missing_players / season_stages** — soccerdata reader with
  its HTTP transport swapped out for FlareSolverr via
  :class:`scrapers.whoscored.flaresolverr_reader.FlareSolverrWhoScoredReader`.
  Same parsing/normalisation as upstream soccerdata; only the per-URL fetch
  goes through the FS-managed Chromium session.
* **events** — bypasses soccerdata entirely. game_ids and game metadata
  are pulled from ``iceberg.bronze.whoscored_schedule`` (populated by
  ``scrape_schedule`` in this same scraper). Each match's
  ``matchCentreData`` JSON is fetched through
  :class:`scrapers.base.flaresolverr_client.FlareSolverrClient`, then parsed
  into the standard events DataFrame via
  :func:`scrapers.whoscored.events_fetcher.parse_matchcentre_to_events_df`.

Why FlareSolverr everywhere: the seleniumbase driver soccerdata used to
ship no longer survives the WhoScored Cloudflare challenge — script_timeout
fires before bypass and the 5×retry full-driver-restart loop never writes
to Iceberg. FlareSolverr (Camoufox) keeps a single Cloudflare-cleared
browser session open and serves each request as a short HTTP fetch.

Source: https://www.whoscored.com
"""

import json
import logging
import os
import uuid
from typing import Dict, List, Optional, Tuple

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper
from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrTimeout,
)

logger = logging.getLogger(__name__)


def _season_to_soccerdata_str(season: int) -> str:
    """Convert int year (2024) to soccerdata short season format ('2425')."""
    start = int(season) % 100
    end = (int(season) + 1) % 100
    return f"{start:02d}{end:02d}"


class WhoScoredScraper(SoccerdataScraper):
    """
    Hybrid soccerdata + FlareSolverr scraper for WhoScored.

    Tables produced (Bronze, partitioned by ``(league, season)``):

    * ``whoscored_schedule`` — fixtures + status + integer ``game_id``.
    * ``whoscored_missing_players`` — pre-match injury / suspension list.
    * ``whoscored_season_stages`` — cup vs league stage metadata.
    * ``whoscored_events`` — per-match Opta events (~1500-2000 rows/match).

    Usage::

        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024, 2025],
        )
        scraper.scrape_all()
    """

    SOURCE_NAME = 'whoscored'
    DEFAULT_RATE_LIMIT = 10  # conservative — Cloudflare under selenium

    # Per-match retry budget when FlareSolverr surfaces a CF challenge or timeout.
    # Rotation happens reactively — on failure — by destroying the FS session and
    # creating a new one with a fresh proxy.
    EVENTS_MAX_PROXY_RETRIES = 3
    # Recreate the FlareSolverr session every N matches. Empirical: FS
    # solves CF on session creation, but the same cookie set gets flagged
    # by Cloudflare after ~5–10 WhoScored requests, so anything above ~10
    # turns each subsequent request into a 60 s challenge timeout.
    EVENTS_SESSION_RECREATE_EVERY = 10

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        season: Optional[int] = None,
        headless: bool = True,
        flaresolverr_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        if seasons is None and season is not None:
            seasons = [season]
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)

        self.headless = headless
        self.flaresolverr_url = flaresolverr_url
        self._reader = None
        # Tracks the currently-active proxy so record_result() can credit it.
        self._current_proxy_obj = None

    # ---------- Reader ----------

    def _get_reader(self):
        """Build the FlareSolverr-backed soccerdata WhoScored reader once.

        Used for schedule / missing_players / season_stages. Events scraping
        uses its own FlareSolverr session via :meth:`scrape_events` —
        keeping the two sessions independent simplifies session-rotation
        bookkeeping.
        """
        if self._reader is None:
            from scrapers.whoscored.flaresolverr_reader import (
                FlareSolverrWhoScoredReader,
            )

            fs_url = self.flaresolverr_url or os.environ.get(
                "FLARESOLVERR_URL", "http://flaresolverr:8191"
            )
            self._reader = FlareSolverrWhoScoredReader(
                flaresolverr_url=fs_url,
                proxy=self._build_proxy_url(),
                leagues=self.leagues,
                seasons=self.seasons,
            )
        return self._reader

    def _close_reader(self) -> None:
        """Release the FlareSolverr session held by the reader, if any.

        Called before events scraping opens its own FS session so we don't
        leak a long-lived schedule session on the FS service.
        """
        if self._reader is None:
            return
        try:
            close = getattr(self._reader, "close", None)
            if callable(close):
                close()
                logger.info("WhoScored: schedule FlareSolverr session closed")
        except Exception as e:
            logger.warning(f"WhoScored: error closing reader: {e}")
        finally:
            self._reader = None

    # ---------- Scrape methods ----------

    def _save(
        self,
        df: Optional[pd.DataFrame],
        table_name: str,
        entity_type: str,
    ) -> Optional[str]:
        """Reset MultiIndex, attach metadata, write Bronze partition."""
        if df is None or df.empty:
            logger.warning(f"WhoScored: empty DataFrame for {table_name}")
            return None
        df = df.reset_index()
        df = self._serialize_nested_columns(df)
        df = self._add_metadata(df, entity_type)
        return self.save_to_iceberg(
            df=df,
            table_name=table_name,
            partition_cols=['league', 'season'],
            replace_partitions=['league', 'season'],
        )

    @staticmethod
    def _serialize_nested_columns(df: pd.DataFrame) -> pd.DataFrame:
        """JSON-encode columns that contain ``list`` / ``dict`` values.

        soccerdata's WhoScored ``read_schedule`` returns nested types in
        columns like ``incidents`` and ``bets``. The Bronze schema stores
        them as ``varchar``, and ``trino_manager._format_sql_value`` calls
        ``pd.isna(val)`` which raises ``ValueError`` on array-shaped values.
        Serialising to JSON strings here keeps the cache file shape stable
        and lets downstream Trino INSERTs round-trip the data unchanged.
        """
        for col in df.columns:
            non_null = df[col].dropna()
            if non_null.empty:
                continue
            has_nested = non_null.apply(
                lambda v: isinstance(v, (list, dict))
            ).any()
            if not has_nested:
                continue
            df[col] = df[col].apply(
                lambda v: json.dumps(v, default=str)
                if isinstance(v, (list, dict))
                else v
            )
        return df

    def scrape_schedule(self) -> Dict[str, str]:
        """Fixtures for all configured (league, season) pairs."""
        logger.info("WhoScored: read_schedule()")
        df = self._safe_call('read_schedule')
        path = self._save(df, 'whoscored_schedule', 'schedule')
        return {'schedule': path} if path else {}

    def scrape_missing_players(self) -> Dict[str, str]:
        """Injuries / suspensions per match (pre-game)."""
        logger.info("WhoScored: read_missing_players()")
        df = self._safe_call('read_missing_players')
        path = self._save(df, 'whoscored_missing_players', 'missing_players')
        return {'missing_players': path} if path else {}

    def scrape_season_stages(self) -> Dict[str, str]:
        """Cup-vs-league stage metadata."""
        logger.info("WhoScored: read_season_stages()")
        df = self._safe_call('read_season_stages')
        path = self._save(df, 'whoscored_season_stages', 'season_stages')
        return {'season_stages': path} if path else {}

    # ---------- Events: FlareSolverr-based ----------

    def _read_events_metadata_from_bronze(
        self, target_season: Optional[str] = None
    ) -> List[Tuple[int, str, str, str]]:
        """Pull (game_id, league, season, game) tuples from bronze.whoscored_schedule.

        Replaces a Cloudflare-prone ``read_schedule`` round-trip during events
        scraping. Filters by current ``self.leagues`` + ``self.seasons``.

        Args:
            target_season: Optional 'YYZZ' season; if set, restricts the
                returned tuples to that season only.
        """
        from scrapers.base.trino_manager import TrinoTableManager

        leagues = list(self.leagues or [])
        season_strs = [_season_to_soccerdata_str(s) for s in (self.seasons or [])]
        if target_season:
            season_strs = [s for s in season_strs if s == target_season]
        if not leagues or not season_strs:
            return []

        leagues_in = ", ".join(f"'{l}'" for l in leagues)
        seasons_in = ", ".join(f"'{s}'" for s in season_strs)
        sql = (
            "SELECT game_id, league, season, game "
            "FROM iceberg.bronze.whoscored_schedule "
            f"WHERE league IN ({leagues_in}) AND season IN ({seasons_in}) "
            "AND game_id IS NOT NULL"
        )
        mgr = TrinoTableManager()
        try:
            rows = mgr._execute(sql, fetch=True)
        except Exception as e:
            msg = str(e)
            if 'TABLE_NOT_FOUND' in msg or 'does not exist' in msg:
                logger.warning("bronze.whoscored_schedule does not exist yet")
                return []
            raise
        out: List[Tuple[int, str, str, str]] = []
        for r in rows or []:
            if r[0] is None:
                continue
            out.append((int(r[0]), str(r[1]), str(r[2]), str(r[3])))
        return out

    def scrape_events(
        self,
        match_ids: Optional[List[int]] = None,
        chunk_size: int = 50,
        skip_existing: bool = True,
        max_matches: Optional[int] = None,
    ) -> Dict[str, str]:
        """Per-match Opta events via FlareSolverr, with resumable incremental save.

        Unlike the previous soccerdata-backed implementation, this method:

        * Pulls ``(game_id, league, season, game)`` tuples directly from
          ``iceberg.bronze.whoscored_schedule`` (no ``read_schedule`` retry
          loop). The schedule task must have run successfully first.
        * Fetches each match's ``matchCentreData`` JSON through
          :class:`FlareSolverrClient` (single CF challenge per session).
        * Parses JSON into the soccerdata events schema in-process — no
          on-disk cache, no soccerdata invocation for events.

        Args:
            match_ids: Optional explicit list. If None, picks latest season
                from bronze schedule.
            chunk_size: Save to Iceberg every N matches (default 50).
            skip_existing: If True, skip game_ids already in bronze.
            max_matches: Optional cap (smoke / verification runs).
        """
        from scrapers.whoscored.events_fetcher import (
            fetch_match_events_via_flaresolverr,
            parse_matchcentre_to_events_df,
        )

        # 1. Resolve game_ids + per-match metadata.
        target_season_str = (
            _season_to_soccerdata_str(max(self.seasons))
            if self.seasons and match_ids is None
            else None
        )
        meta = self._read_events_metadata_from_bronze(target_season_str)
        if not meta:
            logger.warning(
                "WhoScored: no rows in bronze.whoscored_schedule — run "
                "scrape_schedule first. Skipping events."
            )
            return {}

        meta_by_id: Dict[int, Tuple[int, str, str, str]] = {m[0]: m for m in meta}
        if match_ids is not None:
            ids = [int(mid) for mid in match_ids if int(mid) in meta_by_id]
        else:
            ids = list(meta_by_id.keys())

        # 2. Resume — skip already-saved.
        if skip_existing and ids:
            try:
                done = self._fetch_existing_event_game_ids()
                if done:
                    before = len(ids)
                    ids = [mid for mid in ids if mid not in done]
                    logger.info(
                        f"WhoScored: skip_existing — {before - len(ids)} of "
                        f"{before} already in bronze, {len(ids)} remaining"
                    )
            except Exception as e:
                logger.warning(f"WhoScored: skip_existing check failed: {e}")

        if max_matches is not None:
            ids = ids[: int(max_matches)]
            logger.info(f"WhoScored: capped to first {len(ids)} matches")

        if not ids:
            logger.info("WhoScored: nothing to fetch (all matches already in bronze)")
            return {
                'events': f'{self._iceberg_writer.catalog}.bronze.whoscored_events'
            }

        # 3. Make sure the soccerdata selenium driver is gone before we open
        # a FlareSolverr session (saves on container memory).
        self._close_reader()

        # 4. Iterate via FlareSolverr.
        total = len(ids)
        logger.info(
            f"WhoScored: fetching events via FlareSolverr for {total} matches "
            f"(recreate session every {self.EVENTS_SESSION_RECREATE_EVERY}, "
            f"rotate proxy reactively on CF challenge / timeout)"
        )

        path: Optional[str] = None
        chunk: List[pd.DataFrame] = []

        def _pick_proxy_url() -> Optional[str]:
            """Pull a proxy from ProxyManager (preferred) or fall back to
            the legacy single ``self.proxy`` env."""
            if self._proxy_manager and self._proxy_manager.total_count > 0:
                proxy_obj = self._proxy_manager.get_proxy()
                if proxy_obj:
                    self._current_proxy_obj = proxy_obj
                    return proxy_obj.url
            if self.proxy:
                return self._build_proxy_url()
            return None

        fs_url = self.flaresolverr_url or os.environ.get(
            "FLARESOLVERR_URL", "http://flaresolverr:8191"
        )
        client = FlareSolverrClient(url=fs_url)
        session_id = f"whoscored-{uuid.uuid4().hex[:8]}"
        client.create_session(session_id, proxy_url=_pick_proxy_url())
        logger.info(
            f"WhoScored: FlareSolverr session started — {session_id} via {fs_url}"
        )

        def _recycle_session() -> None:
            """Destroy current FS session and open a fresh one with a new proxy.

            Mutates the enclosing ``session_id`` (via ``nonlocal``). Used both
            for the periodic mid-loop recycle and as recovery after a CF
            challenge / timeout.
            """
            nonlocal session_id
            try:
                client.destroy_session(session_id)
            except FlareSolverrError:
                pass
            session_id = f"whoscored-{uuid.uuid4().hex[:8]}"
            client.create_session(session_id, proxy_url=_pick_proxy_url())

        try:
            for i, mid in enumerate(ids, 1):
                meta_row = meta_by_id[mid]
                _, league, season, game_name = meta_row

                # Periodic FS session recycle — guards against FS bug #1128
                # (cookies stale after long sessions) and the FS memory leak.
                if i > 1 and (i - 1) % self.EVENTS_SESSION_RECREATE_EVERY == 0:
                    logger.info(
                        f"WhoScored: recycling FS session at match {i}/{total}"
                    )
                    _recycle_session()

                # Per-match fetch with CF/timeout → recreate session → retry.
                data = None
                for attempt in range(self.EVENTS_MAX_PROXY_RETRIES):
                    try:
                        data = fetch_match_events_via_flaresolverr(
                            client, mid, session_id
                        )
                        if (
                            self._proxy_manager
                            and self._current_proxy_obj is not None
                        ):
                            self._proxy_manager.record_result(
                                self._current_proxy_obj, success=True
                            )
                        break
                    except (FlareSolverrTimeout, FlareSolverrCFChallengeFailed) as e:
                        error_type = (
                            'cf_challenge'
                            if isinstance(e, FlareSolverrCFChallengeFailed)
                            else 'timeout'
                        )
                        if (
                            self._proxy_manager
                            and self._current_proxy_obj is not None
                        ):
                            self._proxy_manager.record_result(
                                self._current_proxy_obj,
                                success=False,
                                error_type=error_type,
                            )
                        logger.warning(
                            f"WhoScored: {error_type} on match {mid} "
                            f"(attempt {attempt + 1}/"
                            f"{self.EVENTS_MAX_PROXY_RETRIES}): {e}"
                        )
                        _recycle_session()
                    except FlareSolverrError as e:
                        logger.warning(
                            f"WhoScored: FlareSolverr error on match {mid} "
                            f"(attempt {attempt + 1}/"
                            f"{self.EVENTS_MAX_PROXY_RETRIES}): {e}"
                        )

                if data is None:
                    logger.warning(
                        f"WhoScored: gave up on match {mid} after retries "
                        f"({i}/{total})"
                    )
                    continue

                df = parse_matchcentre_to_events_df(
                    data,
                    league=league,
                    season=season,
                    game_id=mid,
                    game_name=game_name,
                )
                if df is not None and not df.empty:
                    chunk.append(df.reset_index())
                else:
                    logger.warning(
                        f"WhoScored: no events parsed for game_id={mid} "
                        f"({i}/{total})"
                    )

                if len(chunk) >= chunk_size or i == total:
                    if chunk:
                        combined = pd.concat(chunk, ignore_index=True)
                        combined = self._add_metadata(combined, 'events')
                        path = self.save_to_iceberg(
                            df=combined,
                            table_name='whoscored_events',
                            partition_cols=['league', 'season'],
                        )
                        logger.info(
                            f"WhoScored: saved chunk @ {i}/{total} "
                            f"({len(combined)} rows)"
                        )
                        chunk = []
        finally:
            try:
                client.destroy_session(session_id)
            except FlareSolverrError as e:
                logger.warning(f"WhoScored: final FS session destroy failed: {e}")

        if path is None:
            logger.warning("WhoScored: events scrape produced no rows")
            return {}
        return {'events': path}

    # ---------- Helpers ----------

    def _build_proxy_url(self) -> Optional[str]:
        """Convert ``self.proxy`` (``host:port:user:pass``) to an HTTP proxy URL."""
        if not self.proxy:
            return None
        parts = self.proxy.split(':')
        if len(parts) == 4:
            host, port, user, pw = parts
            return f"http://{user}:{pw}@{host}:{port}"
        return self.proxy

    def _fetch_existing_event_game_ids(self) -> set:
        """Query bronze.whoscored_events for already-saved game_ids."""
        from scrapers.base.trino_manager import TrinoTableManager
        season_strs = [
            _season_to_soccerdata_str(s) for s in (self.seasons or [])
        ]
        leagues = list(self.leagues or [])
        if not season_strs or not leagues:
            return set()
        leagues_in = ", ".join(f"'{l}'" for l in leagues)
        seasons_in = ", ".join(f"'{s}'" for s in season_strs)
        sql = (
            f"SELECT DISTINCT game_id "
            f"FROM iceberg.bronze.whoscored_events "
            f"WHERE league IN ({leagues_in}) AND season IN ({seasons_in})"
        )
        mgr = TrinoTableManager()
        try:
            rows = mgr._execute(sql, fetch=True)
        except Exception as e:
            msg = str(e)
            if 'TABLE_NOT_FOUND' in msg or 'does not exist' in msg:
                return set()
            raise
        return {int(r[0]) for r in (rows or []) if r[0] is not None}

    def scrape_all(self) -> Dict[str, str]:
        """Full ingest: schedule → missing_players → season_stages → events."""
        logger.info(
            f"WhoScored scrape_all: leagues={self.leagues}, seasons={self.seasons}"
        )
        results: Dict[str, str] = {}
        results.update(self.scrape_schedule())
        results.update(self.scrape_missing_players())
        results.update(self.scrape_season_stages())
        results.update(self.scrape_events())
        logger.info(f"WhoScored scrape_all done: {list(results.keys())}")
        return results
