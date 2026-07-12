"""
SofaScore Scraper
=================

Scraper for SofaScore match data, live scores, and statistics.

Source: https://www.sofascore.com
"""

import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

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


class SofaScoreScraper(SoccerdataScraper):
    """Activation-gated SofaScore parsers and Iceberg materialization helpers.

    Network access belongs exclusively to the raw-first capture engine. This
    class deliberately exposes no standalone browser or endpoint reader.
    """

    SOURCE_NAME = 'sofascore'
    DEFAULT_RATE_LIMIT = 20  # SofaScore can be strict
    # Explicitly disable BaseScraper's standalone writer hook. ABCMeta treats
    # this non-callable override as implemented, while any accidental caller
    # fails closed instead of bypassing the common runner/manifest.
    scrape_all = None
    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        **kwargs
    ):
        if kwargs.get('proxy') or kwargs.get('proxy_file'):
            raise ValueError(
                'SofaScore proxy access is owned by the common capture engine'
            )
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
        ``_camel_to_snake`` + ``_coerce_scalar`` so unknown source metrics are
        retained without a second endpoint request.
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
