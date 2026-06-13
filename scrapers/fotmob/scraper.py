"""
FotMob Scraper
==============

Scraper for FotMob football data using FotMob's public ``/api/data`` JSON
endpoints. No browser / Cloudflare bypass required — all endpoints are public
HTTP (see issue #36; rotation ``/api`` → ``/api/data``).

FotMob provides comprehensive football statistics including:
- Match schedules and results
- Team and player season statistics / leaderboards
- Team profiles and squads
- Transfers
- Per-match details (lineups, events, shotmap, ...) via the Next.js
  ``_next/data`` slug-form payload
- Per-player details (career, market values, trophies, ...)

Source: https://www.fotmob.com
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from scrapers.base.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


# Content-derived JSON columns of fotmob_match_details. A failed re-fetch leaves
# these empty for a match; under replace_partitions that empties out a row that
# previously held good data (issue #544 — a re-scrape wiped stats_json for 10
# matches that Bronze still had identity rows for). Keep-last-good below backfills
# them from the existing Bronze partition so good payloads are never overwritten
# by an empty re-scrape.
_PRESERVE_JSON_COLS = (
    'stats_json',
    'player_stats_json',
    'lineup_json',
    'events_json',
    'match_facts_json',
    'shotmap_json',
    'h2h_json',
    'momentum_json',
)


def _is_empty_json_value(value: Any) -> bool:
    """True for FotMob JSON-column values that carry no payload.

    Covers Python ``None``, pandas NaN, and the empty-ish JSON strings the
    scraper / Trino can produce (``''``, ``'null'``, ``'{}'``, ``'[]'``).
    """
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip() in ('', 'null', '{}', '[]')
    return False


def _backfill_empty_json(
    new_df: pd.DataFrame,
    existing_df: Optional[pd.DataFrame],
    key: str = 'match_id',
    cols: tuple = _PRESERVE_JSON_COLS,
) -> pd.DataFrame:
    """Keep-last-good merge for FotMob match-detail JSON columns (issue #544).

    For every row in ``new_df`` whose ``col`` value is empty (see
    :func:`_is_empty_json_value`), fill it from the row with the same ``key`` in
    ``existing_df`` — but only when the existing value is itself non-empty. This
    prevents a failed re-fetch (empty payload) from overwriting a previously-good
    Bronze row under ``replace_partitions``.

    Pure function: ``new_df`` is not mutated. ``key`` is matched as a string on
    both sides (Bronze stores ``match_id`` as varchar; the scraper may build it
    as int). A missing/empty ``existing_df`` returns ``new_df`` unchanged.
    """
    if existing_df is None or len(existing_df) == 0 or key not in new_df.columns:
        return new_df
    if key not in existing_df.columns:
        return new_df

    out = new_df.copy()
    ex = existing_df.drop_duplicates(subset=[key]).copy()
    ex[key] = ex[key].astype(str)
    ex = ex.set_index(key)
    keys_str = out[key].astype(str)

    for col in cols:
        if col not in out.columns or col not in ex.columns:
            continue
        empty_mask = out[col].map(_is_empty_json_value)
        if not empty_mask.any():
            continue
        mapped = keys_str[empty_mask].map(ex[col])
        mapped = mapped[mapped.map(lambda v: not _is_empty_json_value(v))]
        if len(mapped):
            out.loc[mapped.index, col] = mapped.values
    return out


class FotMobScraper(BaseScraper):
    """
    Scraper for FotMob data using public ``/api/data`` JSON endpoints.

    Usage:
        scraper = FotMobScraper(leagues=['ENG-Premier League'], seasons=[2025])
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'fotmob'
    DEFAULT_RATE_LIMIT = 30  # requests per minute

    BASE_URL = 'https://www.fotmob.com'
    API_BASE = 'https://www.fotmob.com/api/data'

    # League configuration with FotMob league IDs
    LEAGUE_IDS = {
        'ENG-Premier League': '47',
        'ESP-La Liga': '87',
        'GER-Bundesliga': '54',
        'ITA-Serie A': '55',
        'FRA-Ligue 1': '53',
        'ENG-Championship': '48',
        'NED-Eredivisie': '57',
        'POR-Primeira Liga': '61',
        'UEFA-Champions League': '42',
        'UEFA-Europa League': '73',
    }

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        """
        Initialize FotMob scraper.

        Args:
            leagues: List of leagues to scrape
            seasons: List of seasons to scrape (e.g., [2024, 2025])
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(leagues=leagues, seasons=seasons, **kwargs)
        self._session: Optional[requests.Session] = None
        self._build_id: Optional[str] = None
        self._team_data_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    # ------------------------------------------------------------------ #
    # HTTP plumbing
    # ------------------------------------------------------------------ #

    def _format_season(self, season: int) -> str:
        """Format season year to FotMob format (e.g. 2023 -> '2023/2024')."""
        return f"{season}/{season + 1}"

    def _get_session(self) -> requests.Session:
        """Get or create a plain requests session (no cookies needed)."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': self.BASE_URL,
                'Origin': self.BASE_URL,
            })
        return self._session

    def _fetch_api_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON from a FotMob endpoint.

        Args:
            endpoint: API endpoint name (e.g. 'leagues') resolved against
                ``API_BASE``, OR a fully-qualified ``http(s)://`` URL (used for
                ``data.fotmob.com`` leaderboard ``fetchAllUrl`` and the
                ``_next/data`` payloads).
            params: Query parameters
            retry_count: Number of retries

        Returns:
            Parsed JSON or None.
        """
        url = endpoint if endpoint.startswith('http') else f"{self.API_BASE}/{endpoint}"
        session = self._get_session()

        for attempt in range(retry_count):
            try:
                self._rate_limiter.acquire()
                response = session.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    self._stats['successes'] += 1
                    return response.json()

                logger.warning(f"FotMob API returned {response.status_code} for {url}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {url}: {e}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {url}: {e}")

        self._stats['failures'] += 1
        return None

    def _get_league_data(self, league: str, season: int) -> Optional[Dict[str, Any]]:
        """Get league payload (`/api/data/leagues`)."""
        league_id = self.LEAGUE_IDS.get(league)
        if not league_id:
            logger.error(f"Unknown league: {league}")
            return None

        return self._fetch_api_json(
            'leagues',
            params={'id': league_id, 'season': self._format_season(season)},
        )

    def _get_build_id(self) -> Optional[str]:
        """Fetch and cache the Next.js ``buildId`` from the homepage.

        ``buildId`` rotates on every FotMob deploy, so it must never be
        hard-coded — it is required to address ``_next/data`` payloads.
        """
        if self._build_id:
            return self._build_id

        session = self._get_session()
        try:
            self._rate_limiter.acquire()
            resp = session.get(self.BASE_URL, timeout=30)
            match = re.search(r'"buildId":"([^"]+)"', resp.text)
            if match:
                self._build_id = match.group(1)
                logger.info(f"Resolved FotMob buildId={self._build_id}")
            else:
                logger.error("Could not find buildId in FotMob homepage")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching FotMob homepage for buildId: {e}")

        return self._build_id

    def _fetch_next_data_payload(self, path: str) -> Optional[Dict[str, Any]]:
        """Fetch a ``/_next/data/<buildId><path>.json`` payload.

        ``buildId`` rotates on every FotMob deploy, which can happen mid-run
        (a full ingest takes ~45 min). A stale ``buildId`` makes every
        subsequent ``_next/data`` URL 404, so on a failed fetch we refresh the
        cached ``buildId`` once and retry — keeping long runs resilient.

        Args:
            path: Path beginning with ``/`` relative to the build root, e.g.
                ``/players/24011`` or ``/matches/<slug>/<short_id>``.
        """
        build_id = self._get_build_id()
        if not build_id:
            return None
        payload = self._fetch_api_json(
            f"{self.BASE_URL}/_next/data/{build_id}{path}.json"
        )
        if payload is not None:
            return payload

        # Possible buildId rotation — refresh once and retry.
        self._build_id = None
        build_id = self._get_build_id()
        if not build_id:
            return None
        return self._fetch_api_json(
            f"{self.BASE_URL}/_next/data/{build_id}{path}.json"
        )

    def _get_team_data(self, team_id: Any) -> Optional[Dict[str, Any]]:
        """Get a team payload (`/api/data/teams?id=`), cached per run."""
        key = str(team_id)
        if key not in self._team_data_cache:
            self._team_data_cache[key] = self._fetch_api_json('teams', params={'id': key})
        return self._team_data_cache[key]

    def _team_ids_for_league(self, league: str, season: int) -> List[int]:
        """Extract team ids from the league standings table."""
        data = self._get_league_data(league, season)
        if not data:
            return []
        try:
            table = data.get('table') or []
            if not table:
                return []
            standings = table[0].get('data', {}).get('table', {}).get('all', [])
            return [t['id'] for t in standings if t.get('id') is not None]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Error extracting team ids for {league} {season}: {e}")
            return []

    @staticmethod
    def _jdump(value: Any) -> Optional[str]:
        """Serialise a nested JSON value to a string column, None-safe."""
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, default=str)

    @staticmethod
    def _position_label(value: Any) -> Optional[str]:
        """FotMob ``positionDescription`` is an object; keep the readable
        primary-position label for the scalar ``position_description`` column
        (e.g. ``{primaryPosition: {label: 'Right Back'}}`` -> 'Right Back')."""
        if isinstance(value, dict):
            return (value.get('primaryPosition') or {}).get('label')
        return value

    @staticmethod
    def _date_str(value: Any) -> Optional[str]:
        """FotMob date fields may be ``{utcTime, timezone}`` objects; keep the
        ISO ``utcTime`` for plain date columns (e.g. ``contract_end``)."""
        if isinstance(value, dict):
            return value.get('utcTime')
        return value

    # ------------------------------------------------------------------ #
    # Daily league-level entities (schedule / team_stats / player_stats)
    # ------------------------------------------------------------------ #

    def read_schedule(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read match schedule/fixtures from FotMob.

        Args:
            league: League name (uses first configured if not specified)
            season: Season year (uses first configured if not specified)

        Returns:
            DataFrame with schedule data
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob schedule: {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        try:
            # Extract matches from league data
            matches = []

            # FotMob stores matches in 'fixtures' section with 'allMatches'
            # Fallback to 'matches' for backwards compatibility
            match_data = data.get('fixtures', {}) or data.get('matches', {})

            all_matches = match_data.get('allMatches', [])
            if not all_matches:
                all_matches = match_data.get('data', {}).get('allMatches', [])

            for match in all_matches:
                status = match.get('status', {})
                score_str = status.get('scoreStr', '')

                # Parse score from "1 - 0" format
                home_score = None
                away_score = None
                if score_str and ' - ' in score_str:
                    try:
                        parts = score_str.split(' - ')
                        home_score = int(parts[0])
                        away_score = int(parts[1])
                    except (ValueError, IndexError):
                        pass

                match_info = {
                    'match_id': match.get('id'),
                    'date': status.get('utcTime'),
                    'home_team': match.get('home', {}).get('name'),
                    'home_team_id': match.get('home', {}).get('id'),
                    'away_team': match.get('away', {}).get('name'),
                    'away_team_id': match.get('away', {}).get('id'),
                    'home_score': home_score,
                    'away_score': away_score,
                    'is_finished': status.get('finished', False),
                    'round': match.get('round'),
                    'round_name': match.get('roundName'),
                }
                matches.append(match_info)

            if not matches:
                logger.warning(f"No matches found for {league} {season}")
                return None

            df = pd.DataFrame(matches)

            # Add metadata
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'schedule')

            logger.info(f"Parsed {len(df)} schedule entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing schedule data: {e}")
            return None

    def read_team_season_stats(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read team/squad statistics for a season.

        Args:
            league: League name
            season: Season year

        Returns:
            DataFrame with team stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob team stats: {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        try:
            # Extract team standings/stats
            teams = []

            table_data = data.get('table', [])
            if table_data:
                # Handle different table formats
                if isinstance(table_data, list) and table_data:
                    table = table_data[0] if isinstance(table_data[0], dict) else {'data': {'table': {'all': table_data}}}
                    standings = table.get('data', {}).get('table', {}).get('all', [])

                    if not standings:
                        standings = table.get('table', {}).get('all', [])

                    if not standings and isinstance(table_data[0], dict):
                        # Direct table format
                        standings = table_data

                    for team in standings:
                        team_info = {
                            'team_id': team.get('id'),
                            'team_name': team.get('name'),
                            'position': team.get('idx') or team.get('position'),
                            'played': team.get('played'),
                            'wins': team.get('wins'),
                            'draws': team.get('draws'),
                            'losses': team.get('losses'),
                            'goals_for': team.get('scoresStr', '').split('-')[0] if team.get('scoresStr') else team.get('goalsFor'),
                            'goals_against': team.get('scoresStr', '').split('-')[-1] if team.get('scoresStr') else team.get('goalsAgainst'),
                            'goal_diff': team.get('goalConDiff'),
                            'points': team.get('pts') or team.get('points'),
                            'form': team.get('form'),
                        }
                        teams.append(team_info)

            if not teams:
                logger.warning(f"No team stats found for {league} {season}")
                return None

            df = pd.DataFrame(teams)

            # Add metadata
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'team_stats')

            logger.info(f"Parsed {len(df)} team stat entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing team stats: {e}")
            return None

    def read_player_season_stats(
        self,
        stat_type: str = 'goals',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read full per-player season stats across all leaderboard categories.

        Walks ``stats.players[]`` and follows each category's ``fetchAllUrl``
        (``data.fotmob.com/stats/.../<cat>.json``) to fetch the COMPLETE player
        list (``TopLists[0].StatList[]``), not just the top-3 preview — yielding
        one long-format row per (participant_id × stat_name). Downstream Silver
        (``fotmob_player_season_profile`` / ``fotmob_keeper_profile``) and the
        xref player resolver depend on ``participant_id`` + ``minutes_played``.

        Args:
            stat_type: Kept for call-site compatibility; ignored — every
                category is fetched.
            league: League name
            season: Season year

        Returns:
            DataFrame with player stats
        """
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)

        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob player stats ({stat_type}): {league} {season}")

        data = self._get_league_data(league, season)
        if not data:
            return None

        categories = (data.get('stats') or {}).get('players') or []
        if not categories:
            logger.warning(f"No player leaderboard categories for {league} {season}")
            return None

        try:
            rows = []
            for cat in categories:
                url = cat.get('fetchAllUrl')
                if not url:
                    continue
                payload = self._fetch_api_json(url)
                top_lists = (payload or {}).get('TopLists') or []
                if not top_lists:
                    continue
                top = top_lists[0]
                header = top.get('Title') or cat.get('header')
                group = top.get('Category') or cat.get('category')
                stat_name = top.get('StatName') or cat.get('name')
                for item in top.get('StatList') or []:
                    rows.append({
                        # NB: FotMob misspells the player id key as 'ParticiantId'
                        # (no second 'p') — matches bronze.fotmob_player_details.player_id.
                        'participant_id': item.get('ParticiantId'),
                        'participant_name': item.get('ParticipantName'),
                        'team_id': item.get('TeamId'),
                        'team_name': item.get('TeamName'),
                        'country_code': item.get('ParticipantCountryCode'),
                        'rank': item.get('Rank'),
                        'stat_value': item.get('StatValue'),
                        'sub_stat_value': item.get('SubStatValue'),
                        'stat_value_count': item.get('StatValueCount'),
                        'matches_played': item.get('MatchesPlayed'),
                        'minutes_played': item.get('MinutesPlayed'),
                        'stat_category_header': header,
                        'stat_category_group': group,
                        'stat_name': stat_name,
                    })

            if not rows:
                logger.warning(f"No player stats found for {league} {season}")
                return None

            df = pd.DataFrame(rows)
            df['league'] = league
            df['season'] = season
            df = self._add_metadata(df, 'player_stats')

            logger.info(f"Parsed {len(df)} player stat entries")
            return df

        except Exception as e:
            logger.error(f"Error parsing player stats: {e}")
            return None

    # ------------------------------------------------------------------ #
    # Team-level entities (profile / squad) — /api/data/teams?id=
    # ------------------------------------------------------------------ #

    def read_team_profile(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read one profile row per team in the league (`/api/data/teams`)."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob team profiles: {league} {season}")
        team_ids = self._team_ids_for_league(league, season)
        if not team_ids:
            logger.warning(f"No team ids for {league} {season}")
            return None

        rows = []
        for tid in team_ids:
            data = self._get_team_data(tid)
            if not data:
                continue
            details = data.get('details') or {}
            overview = data.get('overview') or {}
            history = data.get('history') or {}

            venue = ((overview.get('venue') or {}).get('widget') or {}).get('name')
            tables = history.get('tables') or {}
            historic = tables.get('historic') if isinstance(tables, dict) else None

            rows.append({
                'team_id': details.get('id') or tid,
                'team_name': details.get('name'),
                'short_name': details.get('shortName'),
                'country': details.get('country'),
                'venue': venue,
                'overview_season': overview.get('season'),
                'overview_table_position': self._overview_table_position(overview, details.get('id') or tid),
                'next_match': self._jdump(overview.get('nextMatch')),
                'last_match': self._jdump(overview.get('lastMatch')),
                'history_seasons_count': str(len(historic)) if isinstance(historic, list) else None,
            })

        if not rows:
            logger.warning(f"No team profiles parsed for {league} {season}")
            return None

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'team_profile')
        logger.info(f"Parsed {len(df)} team profiles")
        return df

    @staticmethod
    def _overview_table_position(overview: Dict[str, Any], team_id: Any) -> Optional[str]:
        """Best-effort league position lookup from the overview standings."""
        try:
            table = overview.get('table') or []
            standings = table[0].get('data', {}).get('table', {}).get('all', [])
            for row in standings:
                if row.get('id') == team_id:
                    pos = row.get('idx') or row.get('position')
                    return str(pos) if pos is not None else None
        except (KeyError, IndexError, TypeError):
            pass
        return None

    def read_team_squad(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read squad members (players + coach) for every team in the league."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob team squads: {league} {season}")
        team_ids = self._team_ids_for_league(league, season)
        if not team_ids:
            logger.warning(f"No team ids for {league} {season}")
            return None

        rows = []
        for tid in team_ids:
            data = self._get_team_data(tid)
            if not data:
                continue
            details = data.get('details') or {}
            team_id = details.get('id') or tid
            team_name = details.get('name')

            sections = (data.get('squad') or {}).get('squad') or []
            for section in sections:
                section_role = section.get('title')
                for m in section.get('members') or []:
                    member_role = m.get('role') or {}
                    injury = m.get('injury') or {}
                    rows.append({
                        'team_id': team_id,
                        'team_name': team_name,
                        'role': section_role,
                        'player_id': m.get('id'),
                        'player_name': m.get('name'),
                        'shirt_number': m.get('shirtNumber'),
                        'position_id': m.get('positionId'),
                        'country': m.get('cname'),
                        'country_code': m.get('ccode'),
                        'age': m.get('age'),
                        'height_cm': m.get('height'),
                        'date_of_birth': m.get('dateOfBirth'),
                        'rating': m.get('rating'),
                        'goals': m.get('goals'),
                        'assists': m.get('assists'),
                        'penalties': m.get('penalties'),
                        'red_cards': m.get('rcards'),
                        'yellow_cards': m.get('ycards'),
                        'injury_text': injury.get('expectedReturn') or injury.get('text'),
                        'exclude_from_ranking': m.get('excludeFromRanking'),
                        'position_label_key': member_role.get('key'),
                        'position_label_fallback': member_role.get('fallback'),
                    })

        if not rows:
            logger.warning(f"No squad members parsed for {league} {season}")
            return None

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'team_squad')
        logger.info(f"Parsed {len(df)} squad members")
        return df

    # ------------------------------------------------------------------ #
    # Team leaderboards — data.fotmob.com/stats/.../<cat>.json (TopLists)
    # ------------------------------------------------------------------ #

    def read_team_leaderboards(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read team-side stat leaderboards across all categories."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob team leaderboards: {league} {season}")
        data = self._get_league_data(league, season)
        if not data:
            return None

        categories = (data.get('stats') or {}).get('teams') or []
        if not categories:
            logger.warning(f"No team leaderboard categories for {league} {season}")
            return None

        rows = []
        for cat in categories:
            url = cat.get('fetchAllUrl')
            if not url:
                continue
            payload = self._fetch_api_json(url)
            top_lists = (payload or {}).get('TopLists') or []
            if not top_lists:
                continue
            top = top_lists[0]
            header = top.get('Title') or cat.get('header')
            group = top.get('Category') or cat.get('category')
            stat_name = top.get('StatName') or cat.get('name')
            for item in top.get('StatList') or []:
                rows.append({
                    'participant_name': item.get('ParticipantName'),
                    'team_id': item.get('TeamId'),
                    'team_name': item.get('TeamName'),
                    'team_color': item.get('TeamColor'),
                    'country_code': item.get('ParticipantCountryCode'),
                    'rank': item.get('Rank'),
                    'stat_value': item.get('StatValue'),
                    'sub_stat_value': item.get('SubStatValue'),
                    'stat_value_count': item.get('StatValueCount'),
                    'matches_played': item.get('MatchesPlayed'),
                    'minutes_played': item.get('MinutesPlayed'),
                    'stat_category_header': header,
                    'stat_category_group': group,
                    'stat_name': stat_name,
                })

        if not rows:
            logger.warning(f"No team leaderboard rows parsed for {league} {season}")
            return None

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'team_leaderboards')
        logger.info(f"Parsed {len(df)} team leaderboard rows")
        return df

    # ------------------------------------------------------------------ #
    # Transfers — /api/data/transfers?id=<league_id>
    # ------------------------------------------------------------------ #

    def read_transfers(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read the league transfer list (`/api/data/transfers`)."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        league_id = self.LEAGUE_IDS.get(league)
        if not league_id:
            logger.error(f"Unknown league: {league}")
            return None

        logger.info(f"Fetching FotMob transfers: {league} {season}")
        payload = self._fetch_api_json('transfers', params={'id': league_id})
        transfers = (payload or {}).get('transfers') or []
        if not transfers:
            logger.warning(f"No transfers for {league} {season}")
            return None

        rows = []
        for tr in transfers:
            position = tr.get('position') or {}
            transfer_type = tr.get('transferType') or {}
            fee = tr.get('fee')
            if isinstance(fee, dict):
                fee_text = fee.get('fallback') or fee.get('text')
                fee_value = fee.get('value')
            else:
                fee_text = fee if isinstance(fee, str) else None
                fee_value = tr.get('amountEuroEstimated')
            market_value = tr.get('marketValue')
            if isinstance(market_value, dict):
                market_value = market_value.get('value')

            rows.append({
                'player_id': tr.get('playerId'),
                'player_name': tr.get('name'),
                'position_label': position.get('label'),
                'position_key': position.get('key'),
                'transfer_date': tr.get('transferDate'),
                'from_club': tr.get('fromClub'),
                'from_club_full_name': tr.get('fromClubFullName'),
                'from_club_id': tr.get('fromClubId'),
                'to_club': tr.get('toClub'),
                'to_club_full_name': tr.get('toClubFullName'),
                'to_club_id': tr.get('toClubId'),
                'fee_text': fee_text,
                'fee_value': fee_value,
                'market_value': str(market_value) if market_value is not None else None,
                'on_loan': tr.get('onLoan'),
                'transfer_type_key': transfer_type.get('localizationKey'),
                'transfer_type_text': transfer_type.get('text'),
            })

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'transfers')
        logger.info(f"Parsed {len(df)} transfers")
        return df

    # ------------------------------------------------------------------ #
    # Match details — 2-step slug-form _next/data
    # ------------------------------------------------------------------ #

    def _fetch_match_details(self, match_id: Any) -> Optional[Dict[str, Any]]:
        """Resolve a match's ``_next/data`` payload via the 2-step slug path.

        Step 1: ``/api/data/match?id=`` -> ``pageUrl`` slug.
        Step 2: ``/_next/data/<buildId>/matches/<slug>/<short_id>.json``.
        """
        header = self._fetch_api_json('match', params={'id': str(match_id)})
        if not header:
            return None
        page_url = header.get('pageUrl')
        if not page_url:
            return None
        # "/matches/<slug>/<short_id>#<match_id>" -> "<slug>/<short_id>"
        slug_path = page_url.split('#')[0].strip('/')
        if slug_path.startswith('matches/'):
            slug_path = slug_path[len('matches/'):]
        content_payload = self._fetch_next_data_payload(f'/matches/{slug_path}')
        if not content_payload:
            return None
        content = (content_payload.get('pageProps') or {}).get('content')
        if content is None:
            return None
        return {'header': header, 'page_url': page_url, 'content': content}

    def read_match_details(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read per-match detail (one wide row per finished match)."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob match details: {league} {season}")
        data = self._get_league_data(league, season)
        if not data:
            return None

        match_data = data.get('fixtures', {}) or data.get('matches', {})
        all_matches = match_data.get('allMatches', []) or \
            match_data.get('data', {}).get('allMatches', [])
        finished = [m for m in all_matches if (m.get('status') or {}).get('finished')]
        if not finished:
            logger.warning(f"No finished matches for {league} {season}")
            return None

        rows = []
        for i, match in enumerate(finished):
            mid = match.get('id')
            details = self._fetch_match_details(mid)
            if not details:
                logger.warning(f"No match details for match_id={mid}")
                continue
            content = details['content']
            status = match.get('status') or {}
            score_str = status.get('scoreStr', '')
            home_score = away_score = None
            if score_str and ' - ' in score_str:
                try:
                    home_score, away_score = (int(x) for x in score_str.split(' - '))
                except (ValueError, IndexError):
                    pass
            match_facts = content.get('matchFacts') or {}
            reason = status.get('reason')
            rows.append({
                'match_id': mid,
                'home_team': match.get('home', {}).get('name'),
                'home_team_id': match.get('home', {}).get('id'),
                'away_team': match.get('away', {}).get('name'),
                'away_team_id': match.get('away', {}).get('id'),
                'match_date': status.get('utcTime'),
                'status': reason.get('long') if isinstance(reason, dict) else None,
                'home_score': home_score,
                'away_score': away_score,
                'page_url': details['page_url'],
                'lineup_json': self._jdump(content.get('lineup')),
                'events_json': self._jdump(match_facts.get('events')),
                'match_facts_json': self._jdump(match_facts),
                'stats_json': self._jdump(content.get('stats')),
                'player_stats_json': self._jdump(content.get('playerStats')),
                'shotmap_json': self._jdump(content.get('shotmap')),
                'h2h_json': self._jdump(content.get('h2h')),
                'momentum_json': self._jdump(content.get('momentum')),
            })
            if (i + 1) % 50 == 0:
                logger.info(f"  match details progress: {i + 1}/{len(finished)}")

        if not rows:
            logger.warning(f"No match details parsed for {league} {season}")
            return None

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season

        # #544 keep-last-good: under replace_partitions a match whose content
        # re-fetched empty (stats_json = None) would overwrite a previously-good
        # Bronze row. Backfill the content JSON columns from the existing Bronze
        # partition so good payloads survive a failed re-scrape. Defensive: any
        # read failure (first run, table absent, Trino down) falls back to the
        # freshly-scraped frame unchanged — never worse than today's behaviour.
        try:
            existing = self._iceberg_writer.read_table(
                'bronze',
                'fotmob_match_details',
                columns=['match_id', *_PRESERVE_JSON_COLS],
                filter_expr=f"league = '{league}' AND season = {int(season)}",
            )
            before = df
            df = _backfill_empty_json(df, existing)
            preserved = sum(
                1 for c in _PRESERVE_JSON_COLS if c in df.columns
                for a, b in zip(before[c], df[c]) if a != b
            )
            if preserved:
                logger.warning(
                    "keep-last-good: preserved %d existing JSON value(s) for "
                    "%s %s where the re-scrape returned empty (#544)",
                    preserved, league, season,
                )
        except Exception as e:  # noqa: BLE001 — defensive, must never block save
            logger.warning(
                "keep-last-good skipped for %s %s (%s) — using fresh scrape",
                league, season, e,
            )

        df = self._add_metadata(df, 'match_details')
        logger.info(f"Parsed {len(df)} match details")
        return df

    # ------------------------------------------------------------------ #
    # Player details — /_next/data/<buildId>/players/<id>.json
    # ------------------------------------------------------------------ #

    def _player_ids_for_league(self, league: str, season: int) -> List[int]:
        """Collect unique player ids from every team squad in the league."""
        player_ids: List[int] = []
        seen = set()
        for tid in self._team_ids_for_league(league, season):
            data = self._get_team_data(tid)
            if not data:
                continue
            sections = (data.get('squad') or {}).get('squad') or []
            for section in sections:
                for m in section.get('members') or []:
                    pid = m.get('id')
                    if pid is not None and pid not in seen:
                        seen.add(pid)
                        player_ids.append(pid)
        return player_ids

    def read_player_details(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """Read per-player detail (one wide row per squad player)."""
        league = league or (self.leagues[0] if self.leagues else None)
        season = season or (self.seasons[0] if self.seasons else None)
        if not league or not season:
            logger.error("League and season must be specified")
            return None

        logger.info(f"Fetching FotMob player details: {league} {season}")
        player_ids = self._player_ids_for_league(league, season)
        if not player_ids:
            logger.warning(f"No player ids for {league} {season}")
            return None

        rows = []
        for i, pid in enumerate(player_ids):
            payload = self._fetch_next_data_payload(f'/players/{pid}')
            d = ((payload or {}).get('pageProps') or {}).get('data') if payload else None
            if not d:
                logger.warning(f"No player details for player_id={pid}")
                continue
            primary_team = d.get('primaryTeam') or {}
            main_league = d.get('mainLeague') or {}
            rows.append({
                'player_id': d.get('id') or pid,
                'name': d.get('name'),
                'birth_date': d.get('birthDate'),
                'is_coach': d.get('isCoach'),
                'is_captain': d.get('isCaptain'),
                'gender': d.get('gender'),
                'primary_team_id': primary_team.get('teamId'),
                'primary_team_name': primary_team.get('teamName'),
                'position_description': self._position_label(d.get('positionDescription')),
                'main_league_id': main_league.get('leagueId'),
                'main_league_name': main_league.get('leagueName'),
                'contract_end': self._date_str(d.get('contractEnd')),
                'player_information_json': self._jdump(d.get('playerInformation')),
                'injury_information_json': self._jdump(d.get('injuryInformation')),
                'trophies_json': self._jdump(d.get('trophies')),
                'career_history_json': self._jdump(d.get('careerHistory')),
                'stat_seasons_json': self._jdump(d.get('statSeasons')),
                'first_season_stats_json': self._jdump(d.get('firstSeasonStats')),
                'recent_matches_json': self._jdump(d.get('recentMatches')),
                'market_values_json': self._jdump(d.get('marketValues')),
                'traits_json': self._jdump(d.get('traits')),
                'meta_json': self._jdump(d.get('meta')),
                'coach_stats_json': self._jdump(d.get('coachStats')),
                'next_match_json': self._jdump(d.get('nextMatch')),
            })
            if (i + 1) % 100 == 0:
                logger.info(f"  player details progress: {i + 1}/{len(player_ids)}")

        if not rows:
            logger.warning(f"No player details parsed for {league} {season}")
            return None

        df = pd.DataFrame(rows)
        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'player_details')
        logger.info(f"Parsed {len(df)} player details")
        return df

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all FotMob data for configured leagues and seasons.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting FotMob scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        # (read callable, table_name) for every entity
        entities = [
            (lambda lg, se: self.read_schedule(lg, se), 'fotmob_schedule'),
            (lambda lg, se: self.read_team_season_stats(lg, se), 'fotmob_team_stats'),
            (lambda lg, se: self.read_player_season_stats('goals', lg, se), 'fotmob_player_stats'),
            (lambda lg, se: self.read_team_profile(lg, se), 'fotmob_team_profile'),
            (lambda lg, se: self.read_team_squad(lg, se), 'fotmob_team_squad'),
            (lambda lg, se: self.read_team_leaderboards(lg, se), 'fotmob_team_leaderboards'),
            (lambda lg, se: self.read_transfers(lg, se), 'fotmob_transfers'),
            (lambda lg, se: self.read_match_details(lg, se), 'fotmob_match_details'),
            (lambda lg, se: self.read_player_details(lg, se), 'fotmob_player_details'),
        ]

        results: Dict[str, str] = {}
        for read_fn, table_name in entities:
            frames = []
            for league in self.leagues:
                for season in self.seasons:
                    try:
                        df = read_fn(league, season)
                        if df is not None and not df.empty:
                            frames.append(df)
                    except Exception as e:
                        logger.error(f"Error scraping {table_name} {league} {season}: {e}")
            if frames:
                combined = pd.concat(frames, ignore_index=True)
                results[table_name] = self.save_to_iceberg(
                    df=combined,
                    table_name=table_name,
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                )

        logger.info(f"FotMob scrape complete: {list(results.keys())}")
        return results

    def close(self) -> None:
        """Cleanup resources."""
        if self._session:
            self._session.close()
            self._session = None
        super().close()
