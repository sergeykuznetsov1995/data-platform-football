"""
ESPN Scraper
============

Scraper for ESPN football data including schedules and results.

Source: https://www.espn.com
"""

import json
import logging
from typing import Dict, List, Optional, Set

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper

logger = logging.getLogger(__name__)


class ESPNScraper(SoccerdataScraper):
    """
    Scraper for ESPN football data.

    ESPN provides:
    - Match schedules and results
    - Per-match lineups and match sheets (team stats + venue)

    League name -> ESPN league id mapping lives in soccerdata
    (``soccerdata._config.LEAGUE_DICT``).

    Usage:
        scraper = ESPNScraper(
            leagues=['ENG-Premier League'],
            seasons=[2023, 2024]
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'espn'
    DEFAULT_RATE_LIMIT = 30

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._reader = None
        # #817: guards the one-shot COVID-season calendar seed (see
        # _seed_2021_season_calendar) so it re-downloads at most once per run.
        self._covid_2021_seeded = False
        # game_ids whose cached pre-kickoff Summary stub was already
        # re-downloaded this run (see _prepare_summary_fetch) — bounds the
        # heal path to one request per match per run.
        self._stale_refetched: Set[int] = set()

    def _get_reader(self):
        """Get soccerdata ESPN reader."""
        if self._reader is None:
            try:
                import soccerdata as sd
                self._reader = sd.ESPN(
                    leagues=self.leagues,
                    seasons=self.seasons,
                    **self._sd_kwargs
                )
            except ImportError:
                logger.error("soccerdata library not installed")
                raise
        return self._reader

    # ESPN public scoreboard API (matches soccerdata.espn.ESPN_API).
    _ESPN_API = "http://site.api.espn.com/apis/site/v2/sports/soccer"

    def _seed_2021_season_calendar(self, reader) -> None:
        """Work around a soccerdata anchor bug for the COVID-disrupted 2020-21
        season (#817).

        soccerdata's ``read_schedule`` discovers a season's match dates from the
        ESPN scoreboard at July 1 of the start year (``espn.py``:
        ``start_date = 20{skey[:2]}0701``). For season code ``'2021'`` that is
        ``2020-07-01`` — which falls INSIDE the COVID-extended 2019-20 Premier
        League season (it ran to 2020-07-26). ESPN therefore returns the
        *2019-20* calendar, and the "2020-21" scrape silently lands a byte-copy
        of 2019-20 under partition ``season='2021'`` (no real 2020-21 data).

        Fix: overwrite soccerdata's calendar cache file for that anchor with the
        scoreboard from a post-COVID anchor (``2020-08-01``), whose ESPN calendar
        is the real 2020-21 season (2020-09-12 … 2021-05-23). soccerdata then
        reads the correct dates and fetches the right matches. ``MAXAGE=None``
        means the seeded file is trusted on every subsequent read this run.

        No-op for every other season (the daily run uses the current season).
        """
        if self._covid_2021_seeded:
            return
        if '2021' not in {str(s) for s in (self.seasons or [])}:
            return
        for league_id in reader._selected_leagues.values():
            # soccerdata's calendar cache path for the buggy July-1 anchor.
            seed_fp = reader.data_dir / f"Schedule_{league_id}_20200701.json"
            url = f"{self._ESPN_API}/{league_id}/scoreboard?dates=20200801"
            logger.info(
                "ESPN #817: seeding real 2020-21 calendar for %s "
                "(post-COVID anchor 2020-08-01 -> %s)",
                league_id, seed_fp.name,
            )
            # no_cache=True forces a fresh download even if a stale (2019-20)
            # calendar file is already cached; reuses soccerdata's get() so the
            # request goes through the same proxy/retry machinery.
            reader.get(url, seed_fp, no_cache=True)
        self._covid_2021_seeded = True

    def read_schedule(self) -> Optional[pd.DataFrame]:
        """
        Read match schedule and results.

        Returns:
            DataFrame with match schedule
        """
        reader = self._get_reader()
        self._seed_2021_season_calendar(reader)
        logger.info("Fetching ESPN schedule")

        try:
            df = self._execute_with_resilience(reader.read_schedule)

            if df is not None and not df.empty:
                df = df.reset_index()
                df = self._enrich_schedule_from_scoreboards(df, reader)
                df = self._add_metadata(df, 'schedule')

            return df

        except Exception as e:
            # Issue #466: propagate instead of returning None — a swallowed
            # error leaves the runner's results['errors'] empty -> exit 0 ->
            # green DAG while Bronze silently goes stale.
            logger.error(f"Error reading schedule: {e}")
            raise

    # Result columns joined onto the schedule from the scoreboard JSONs.
    _SCHEDULE_RESULT_COLS = ('home_goals', 'away_goals', 'status', 'venue',
                             'attendance')

    def _enrich_schedule_from_scoreboards(
        self, df: pd.DataFrame, reader
    ) -> pd.DataFrame:
        """Join score/status/venue/attendance onto the schedule from the
        scoreboard JSONs ``read_schedule`` just fetched/cached.

        soccerdata's ESPN reader extracts only date/teams/game_id, so without
        this bronze.espn_schedule carries no result columns at all. Zero extra
        traffic: only cache files already written for the schedule's own match
        dates are read. Values stay nullable strings (bronze is stringly
        typed; silver casts). A missing/broken cache file leaves NULLs — this
        never fails the schedule save.
        """
        required = {'date', 'league_id', 'game_id'}
        if not required.issubset(df.columns):
            logger.warning(
                "schedule enrichment skipped — missing columns %s",
                sorted(required - set(df.columns)),
            )
            return df

        df = df.copy()
        events: Dict[int, Dict] = {}
        dates = pd.to_datetime(df['date'], utc=True, errors='coerce')
        pairs = {
            (str(lid), d.strftime('%Y%m%d'))
            for lid, d in zip(df['league_id'], dates)
            if pd.notna(d) and pd.notna(lid)
        }
        for lkey, day in sorted(pairs):
            fp = reader.data_dir / f"Schedule_{lkey}_{day}.json"
            if not fp.exists():
                continue
            try:
                data = json.loads(fp.read_text())
            except Exception:
                continue
            for e in data.get('events', []):
                try:
                    comp = e['competitions'][0]
                    comps = comp.get('competitors', [])
                    home = next(
                        (c for c in comps if c.get('homeAway') == 'home'), None
                    )
                    away = next(
                        (c for c in comps if c.get('homeAway') == 'away'), None
                    )
                    events[int(e['id'])] = {
                        'home_goals': (home or {}).get('score'),
                        'away_goals': (away or {}).get('score'),
                        'status': ((e.get('status') or {}).get('type')
                                   or {}).get('name'),
                        'venue': (comp.get('venue') or {}).get('fullName'),
                        'attendance': comp.get('attendance'),
                    }
                except Exception:
                    continue

        if not events:
            logger.warning(
                "schedule: no scoreboard cache files readable — result "
                "columns stay NULL"
            )
        for col in self._SCHEDULE_RESULT_COLS:
            df[col] = [
                events.get(int(g), {}).get(col) if pd.notna(g) else None
                for g in df['game_id']
            ]
            df[col] = df[col].astype('string').where(df[col].notna(), None)
        return df

    def _sanitize_match_cache(self, reader, gid) -> bool:
        """Drop roster players with an empty athlete record from the cached ESPN
        match JSON so soccerdata can parse the rest of the match.

        ESPN occasionally ships a substitute whose ``athlete`` is just
        ``{'links': ...}`` — no displayName/id — and soccerdata then raises
        ``KeyError: 'displayName'`` for the WHOLE match (#713; e.g. game 480573,
        West Ham–Stoke 2018-04-16, one nameless Stoke sub). Returns True if any
        player was removed (so the caller can retry the parse).
        """
        fp = reader.data_dir / f"Summary_{int(gid)}.json"
        if not fp.exists():
            return False
        try:
            data = json.loads(fp.read_text())
        except Exception:
            return False
        changed = False
        for r in data.get("rosters", []):
            roster = r.get("roster")
            if not roster:
                continue
            kept = [p for p in roster if "displayName" in p.get("athlete", {})]
            if len(kept) != len(roster):
                r["roster"] = kept
                changed = True
        if changed:
            fp.write_text(json.dumps(data))
        return changed

    def _read_per_match(
        self,
        method_name: str,
        entity: str,
        skip_existing: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Read a per-match entity (lineup/matchsheet) match-by-match.

        soccerdata's bulk ``read_lineup``/``read_matchsheet`` abort the WHOLE
        season if a single match has malformed ESPN JSON — e.g.
        ``KeyError: 'displayName'`` when a player lacks ``athlete.displayName``,
        seen in some older seasons. Iterating per ``match_id`` and skipping only
        the broken matches keeps the rest of the season instead of losing it all
        (resilience for #713 historical backfill).

        Traffic/staleness contract:
        - unplayed matches are never fetched — their Summary would be cached
          forever (soccerdata MAXAGE=None) as a pre-kickoff stub, permanently
          masking the real lineups/stats;
        - a cached stub for an already-played match is re-downloaded once per
          run (heals a poisoned cache; see _prepare_summary_fetch);
        - ``reader.read_schedule`` is memoized for the duration of the loop:
          soccerdata re-runs it inside EVERY per-match call, and in a live
          season that re-downloads every date scoreboard with no_cache=True —
          O(matches x match days) HTTP requests without the memo;
        - with ``skip_existing`` games already materialised in bronze are
          dropped up front (#842 pattern), so a steady-state daily run costs
          only the new matches.
        """
        reader = self._get_reader()
        # #817: per-match readers also discover game_ids via read_schedule, so
        # the COVID-season calendar must be seeded here too (idempotent).
        self._seed_2021_season_calendar(reader)
        logger.info(f"Fetching ESPN {entity} (per-match, resilient)")
        sched_raw = self._execute_with_resilience(reader.read_schedule)
        if sched_raw is None or sched_raw.empty:
            return None
        sched = sched_raw.reset_index()
        sched = sched[sched['game_id'].notna()]

        kickoff = pd.to_datetime(sched['date'], utc=True, errors='coerce')
        played = sched[kickoff.notna() & (kickoff < pd.Timestamp.now(tz='UTC'))]
        if len(played) < len(sched):
            logger.info(
                f"{entity}: {len(sched) - len(played)}/{len(sched)} matches "
                f"not played yet — deferred to a later run"
            )
        sched = played

        if skip_existing:
            existing = self._existing_game_keys(
                f'espn_{entity}',
                sched,
                # A pre-fix matchsheet stub row (venue only, no stats) must
                # not count as ingested — probe on a stats column so the stub
                # is re-scraped and healed.
                non_null_col='total_shots' if entity == 'matchsheet' else None,
            )
            if existing:
                before = len(sched)
                sched = sched[~sched['game'].isin(existing)]
                logger.info(
                    f"{entity}: skip-existing dropped {before - len(sched)}/"
                    f"{before} already-ingested matches"
                )
        if sched.empty:
            logger.info(f"{entity}: no new matches to fetch (no-op)")
            return None

        method = getattr(reader, method_name)
        frames: List[pd.DataFrame] = []
        skipped: List = []
        # Memoize the schedule while iterating — soccerdata's per-match
        # readers call read_schedule() again on EVERY invocation.
        orig_read_schedule = reader.read_schedule
        reader.read_schedule = lambda *args, **kwargs: sched_raw
        try:
            for row in sched.itertuples(index=False):
                gid = int(row.game_id)
                self._prepare_summary_fetch(reader, row.league_id, gid)
                try:
                    d = method(match_id=gid)
                    if d is not None and not d.empty:
                        frames.append(d)
                except ConnectionError as e:
                    # soccerdata already retried the download 5x internally —
                    # transient outage, not malformed data. The next run picks
                    # the match up again (it never reached bronze, so
                    # skip-existing won't drop it).
                    skipped.append(gid)
                    logger.warning(
                        f"{entity}: skipping match {gid} (network error: {e})"
                    )
                except Exception as e:
                    # One roster player with an empty athlete record (only
                    # 'links', no displayName) makes soccerdata KeyError on the
                    # WHOLE match. Drop the nameless player(s) from the cached
                    # match JSON and retry once, so the other ~35 players in
                    # that match survive (#713).
                    salvaged = False
                    if self._sanitize_match_cache(reader, gid):
                        try:
                            d = method(match_id=gid)
                            if d is not None and not d.empty:
                                frames.append(d)
                            salvaged = True
                            logger.info(
                                f"{entity}: salvaged match {gid} after "
                                f"dropping nameless player(s)"
                            )
                        except Exception as e2:
                            e = e2
                    if not salvaged:
                        skipped.append(gid)
                        logger.warning(
                            f"{entity}: skipping match {gid} "
                            f"(malformed ESPN data: {e})"
                        )
        finally:
            reader.read_schedule = orig_read_schedule
        if skipped:
            logger.warning(
                f"{entity}: skipped {len(skipped)}/{len(sched)} matches"
            )
        if not frames:
            return None
        return pd.concat(frames)

    def _prepare_summary_fetch(self, reader, league_id, gid: int) -> None:
        """Pace and pre-heal the Summary download for one match.

        - Summary not cached yet: the per-match reader is about to download
          it — take a rate-limiter slot (soccerdata's own rate_limit is 0 and
          ``_execute_with_resilience`` never sees these inner requests).
        - Summary cached WITHOUT any roster for an already-played match: a
          pre-kickoff stub that soccerdata would trust forever (MAXAGE=None).
          Force one re-download per run via ``reader.get`` so the request
          reuses soccerdata's session/retry machinery.
        """
        fp = reader.data_dir / f"Summary_{gid}.json"
        if not fp.exists():
            self._rate_limiter.acquire()
            return
        if gid in self._stale_refetched or not self._summary_is_stub(fp):
            return
        self._stale_refetched.add(gid)
        self._rate_limiter.acquire()
        url = f"{self._ESPN_API}/{league_id}/summary?event={gid}"
        logger.info(f"re-fetching pre-kickoff Summary stub for match {gid}")
        reader.get(url, fp, no_cache=True)

    @staticmethod
    def _summary_is_stub(fp) -> bool:
        """True when the cached match JSON carries no lineup at all — the
        shape ESPN serves before kickoff. Unreadable file -> False (leave it
        to the normal parse/sanitize path)."""
        try:
            data = json.loads(fp.read_text())
        except Exception:
            return False
        return not any(
            r.get("roster") for r in data.get("rosters", [])
            if isinstance(r, dict)
        )

    def _existing_game_keys(
        self,
        table: str,
        sched: pd.DataFrame,
        non_null_col: Optional[str] = None,
    ) -> Optional[Set[str]]:
        """Game keys already materialised in ``bronze.<table>`` for the
        (league, season) pairs in ``sched`` — the skip-existing probe
        (pattern #842; SofaScore/FBref key theirs the same way).

        ``non_null_col`` narrows the probe to rows where that column is
        filled. Returns ``None`` when the probe fails — the caller then
        treats every match as new, which is duplicate-safe because the saves
        replace per (league, season, game).
        """
        try:
            if not self._iceberg_writer.table_exists('bronze', table):
                return set()
            trino = self._iceberg_writer._get_trino_manager()
            catalog = self._iceberg_writer.catalog
            keys: Set[str] = set()
            pairs = sched[['league', 'season']].drop_duplicates()
            for lg, ss in pairs.itertuples(index=False):
                lg_esc = str(lg).replace("'", "''")
                ss_esc = str(ss).replace("'", "''")
                where = (
                    f"league = '{lg_esc}' "
                    f"AND CAST(season AS varchar) = '{ss_esc}'"
                )
                if non_null_col:
                    where += f" AND {non_null_col} IS NOT NULL"
                rows = trino.execute_query(
                    f"SELECT DISTINCT game FROM {catalog}.bronze.{table} "
                    f"WHERE {where}"
                )
                keys.update(r[0] for r in rows if r and r[0])
            return keys
        except Exception as e:
            logger.warning(
                f"skip-existing probe on bronze.{table} failed ({e}) — "
                f"treating all matches as new"
            )
            return None

    def read_lineup(self, skip_existing: bool = True) -> Optional[pd.DataFrame]:
        """Read per-match lineups (one row per player per game)."""
        df = self._read_per_match(
            'read_lineup', 'lineup', skip_existing=skip_existing
        )
        if df is not None and not df.empty:
            df = df.reset_index()
            df = self._add_metadata(df, 'lineup')
            # bronze.espn_lineup declares these as varchar, but soccerdata
            # returns int/float (when present) or NaN — coerce to nullable
            # string (NaN -> None) to match the existing Iceberg schema.
            for col in (
                'league', 'season', 'game', 'team', 'player',
                'position', 'formation_place', 'sub_in', 'sub_out',
            ):
                if col in df.columns:
                    df[col] = df[col].astype('string').where(df[col].notna(), None)
        return df

    def read_matchsheet(self, skip_existing: bool = True) -> Optional[pd.DataFrame]:
        """Read match-level team stats + venue (one row per game per team)."""
        df = self._read_per_match(
            'read_matchsheet', 'matchsheet', skip_existing=skip_existing
        )
        if df is not None and not df.empty:
            df = df.reset_index()
            # The raw roster blob duplicates espn_lineup row-by-row and only
            # bloats the partition — never ship it to bronze.
            df = df.drop(columns=['roster'], errors='ignore')
            df = self._add_metadata(df, 'matchsheet')
            # bronze.espn_matchsheet declares every stat column as varchar,
            # but soccerdata returns numeric/NaN — coerce every object column
            # to nullable string (NaN -> None); keep is_home (bool),
            # attendance (bigint) and _ingested_at (timestamp) at their types.
            for col in df.columns:
                if col in ('is_home', 'attendance', '_ingested_at'):
                    continue
                df[col] = df[col].astype('string').where(df[col].notna(), None)
        return df

    def _standardize_schedule(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize schedule column names.

        Args:
            df: Raw schedule DataFrame

        Returns:
            Standardized DataFrame
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        # Result columns (home_goals, venue, ...) arrive pre-named from
        # _enrich_schedule_from_scoreboards; only the date needs renaming.
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'match_date'})

        return df

    def scrape_schedule(self) -> Dict[str, str]:
        """Scrape match schedule."""
        df = self.read_schedule()

        if df is not None and not df.empty:
            df = self._standardize_schedule(df)
            table_path = self.save_to_iceberg(
                df=df,
                table_name='espn_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                # Completeness guard (#583): refuse a replace that shrinks the
                # partition below 90% of its existing rows (one row per match).
                min_replace_ratio=0.9,
            )
            return {'schedule': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape the ESPN schedule (schedule-only convenience entrypoint).

        The production path is ``dags/scripts/run_espn_scraper.py``, which
        writes all three bronze tables (schedule + lineup + matchsheet) and
        wires the completeness guard / skip-existing flags per entity.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting ESPN scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        # Scrape schedule
        schedule_results = self.scrape_schedule()
        results.update(schedule_results)

        logger.info(f"ESPN scrape complete: {list(results.keys())}")
        return results
