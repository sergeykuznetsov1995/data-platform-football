"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


# SofaScore public REST API
_SOFASCORE_API = "https://api.sofascore.com/api/v1"
_LINEUPS_PATH = "/event/{event_id}/lineups"

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

    def read_player_season_stats(self) -> Optional[pd.DataFrame]:
        """
        Read player season statistics.

        Note: Sofascore doesn't have this method in soccerdata.
        Returns None.
        """
        logger.info("Sofascore player stats not available in soccerdata")
        return None

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
            return []

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

    def _fetch_lineup_payload(
        self,
        event_id: str,
        max_attempts: int = 3,
    ) -> Optional[dict]:
        """Fetch /event/{id}/lineups JSON with proxy rotation + retry.

        Returns ``None`` after exhausting attempts. Uses the platform's
        rate limiter to stay well under SofaScore's burst threshold.
        """
        import tls_requests
        from scrapers.utils.proxy_manager import ErrorType

        url = f"{_SOFASCORE_API}{_LINEUPS_PATH.format(event_id=event_id)}"

        last_status = None
        last_error = None
        for attempt in range(1, max_attempts + 1):
            self._rate_limiter.acquire()
            self._stats['requests'] += 1

            client, proxy_obj = self._build_tls_session()
            try:
                resp = client.get(url, timeout=20)
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
                            "Lineups payload not JSON for event %s: %s",
                            event_id, parse_err,
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
                    # Not all events expose lineups (cancelled / TBD) —
                    # treat as legitimate empty.
                    logger.info("Lineups not exposed for event %s (404)", event_id)
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
                "Lineups attempt %d/%d failed for event %s: %s",
                attempt, max_attempts, event_id, last_error,
            )

        self._stats['failures'] += 1
        # Surface the structural reason so the runner can decide whether
        # to emit the R0.2B_FALLBACK marker.
        self._last_lineup_error = {
            'event_id': event_id,
            'status': last_status,
            'error': last_error,
        }
        return None

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
        max_consecutive = 100  # preventive; live probe 2026-05-18 (#19) shows endpoint stable

        for idx, mid in enumerate(match_ids, start=1):
            payload = self._fetch_lineup_payload(str(mid))
            if payload is None:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive:
                    logger.error(
                        "%s: %d consecutive lineup fetch failures — aborting "
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
        df['season'] = str(season)
        df['_ingested_at'] = datetime.utcnow()
        df['_source'] = self.SOURCE_NAME
        df['_entity_type'] = 'player_ratings'
        df['_batch_id'] = self._batch_id

        logger.info(
            "Materialised %d player-rating rows across %d unique matches",
            len(df), df['match_id'].nunique(),
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
            )
            return {'league_table': table_path}
        return {}

    def scrape_team_stats(self) -> Dict[str, str]:
        """Scrape team stats (alias for league table)."""
        return self.scrape_league_table()

    def scrape_player_stats(self) -> Dict[str, str]:
        """Scrape player stats."""
        df = self.read_player_season_stats()
        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='sofascore_player_stats',
                partition_cols=['league', 'season'],
            )
            return {'player_stats': table_path}
        return {}

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
