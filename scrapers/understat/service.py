"""Scope-level native Understat extraction service."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from urllib.parse import quote

import pandas as pd

from .catalog import (
    LEAGUE_BY_CANONICAL,
    UnderstatCatalog,
    UnderstatScope,
    current_source_season_id,
    season_slug as make_season_slug,
)
from .client import UnderstatClient
from .client import UnderstatHTTPError
from .closed_check import league_payload_hashes
from .parsers import (
    UnderstatSchemaDrift,
    parse_match_payload,
    parse_player_season_stats,
    parse_schedule,
    parse_team_match_stats,
    parse_team_payload,
    validate_league_payload,
    validate_match_payload,
    validate_team_payload,
)


logger = logging.getLogger(__name__)


def _concat(frames: list[pd.DataFrame], empty: pd.DataFrame) -> pd.DataFrame:
    populated = [frame for frame in frames if not frame.empty]
    if not populated:
        return empty.copy()
    return pd.concat(populated, ignore_index=True).convert_dtypes()


def _match_payload_has_rows(payload: Mapping[str, Any]) -> bool:
    """Whether an HTTP-200 match envelope contains usable source records."""

    for block_name in ("shots", "rosters"):
        block = payload.get(block_name)
        if not isinstance(block, Mapping):
            continue
        for side in ("h", "a"):
            records = block.get(side)
            if isinstance(records, list) and records:
                return True
            if isinstance(records, Mapping) and records:
                return True
    return False


class UnderstatSource:
    """Fetch and parse exactly one canonical league-season scope at a time."""

    def __init__(self, client: UnderstatClient, *, today: Optional[date] = None):
        self.client = client
        self.today = today or date.today()
        self.catalog = UnderstatCatalog(client, today=self.today)
        # #1431: fingerprints of the last parsed league response.
        self.last_league_hashes: dict[str, str] = {}

    def _validate(
        self,
        validator: Callable[..., None],
        payload: Mapping[str, Any],
        cache_name: str,
        **kwargs: Any,
    ) -> None:
        """Run a payload validator; on drift keep the response, then re-raise.

        #1428 (R-02): the regular cache file is overwritten by the next run,
        so a drifted response is copied to ``<cache_dir>/schema_drift/``.
        Saving is best-effort and never replaces the drift exception.
        """

        try:
            validator(payload, **kwargs)
        except UnderstatSchemaDrift:
            self._save_drift_payload(payload, cache_name)
            raise

    def _save_drift_payload(self, payload: Any, cache_name: str) -> None:
        cache_dir = getattr(self.client, "cache_dir", None)
        if cache_dir is None:
            return
        try:
            folder = Path(cache_dir) / "schema_drift"
            folder.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            target = folder / f"{stamp}_{cache_name}"
            target.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
        except Exception:  # the drift itself stays the verdict
            logger.warning(
                "Unable to save Understat schema-drift payload %s",
                cache_name,
                exc_info=True,
            )
            return
        logger.warning("Understat schema-drift payload saved to %s", target)

    def _scope(self, league: str, season_slug: str, source_season_id: int):
        definition = LEAGUE_BY_CANONICAL.get(league)
        if definition is None:
            raise ValueError(f"Unsupported Understat league: {league!r}")
        expected_slug = make_season_slug(source_season_id)
        if season_slug != expected_slug:
            raise ValueError(
                f"season/source mismatch: {season_slug!r} != {expected_slug!r} "
                f"for {source_season_id}"
            )
        return definition, UnderstatScope(
            league=league,
            source_league=definition.source_league,
            source_league_id=definition.source_league_id,
            season=season_slug,
            source_season_id=source_season_id,
            is_closed=source_season_id < current_source_season_id(self.today),
            discovered=True,
        )

    def league_snapshot(
        self,
        league: str,
        season_slug: str,
        source_season_id: int,
    ) -> dict[str, str]:
        """Fingerprint one fresh league response exactly as ``scrape_scope`` does.

        One ``getLeagueData`` request. A 404 returns ``{}`` (not published):
        for a closed scope that never matches a baseline, so the caller falls
        back to the full path, which fails as an unpublished closed scope.
        """

        definition, scope = self._scope(league, season_slug, source_season_id)
        try:
            league_payload = self.client.get_league_data(
                definition.source_league,
                source_season_id,
                force_refresh=True,
            )
        except UnderstatHTTPError as exc:
            if exc.status_code != 404:
                raise
            logger.warning(
                "Understat closed scope league response is not published: "
                "league=%s source_season_id=%s",
                league,
                source_season_id,
            )
            return {}
        if not isinstance(league_payload, Mapping):
            raise TypeError("getLeagueData payload must be an object")
        self._validate(
            validate_league_payload,
            league_payload,
            _league_cache_name(definition.source_league, source_season_id),
        )
        return league_payload_hashes(
            {
                "schedule": parse_schedule(league_payload, scope),
                "players": parse_player_season_stats(league_payload, scope),
                "team_match_stats": parse_team_match_stats(league_payload, scope),
            }
        )

    def scrape_scope(
        self,
        league: str,
        season_slug: str,
        source_season_id: int,
        *,
        mode: str = "current",
        force_refresh: bool = False,
    ) -> dict[str, pd.DataFrame]:
        if mode not in {"current", "history", "reparse"}:
            raise ValueError("mode must be current, history, or reparse")
        definition, scope = self._scope(league, season_slug, source_season_id)
        refresh_scope = force_refresh or mode in {"current", "reparse"}
        try:
            league_payload = self.client.get_league_data(
                definition.source_league,
                source_season_id,
                force_refresh=refresh_scope,
            )
        except UnderstatHTTPError as exc:
            if exc.status_code != 404:
                raise
            logger.info(
                "Understat scope is not published: league=%s source_season_id=%s",
                league,
                source_season_id,
            )
            league_payload = {"dates": [], "players": [], "teams": {}}
        if not isinstance(league_payload, Mapping):
            raise TypeError("getLeagueData payload must be an object")
        self._validate(
            validate_league_payload,
            league_payload,
            _league_cache_name(definition.source_league, source_season_id),
        )

        schedule = parse_schedule(league_payload, scope)
        players = parse_player_season_stats(league_payload, scope)
        team_match = parse_team_match_stats(league_payload, scope)
        # #1431: fingerprint before has_data is rewritten from match responses.
        self.last_league_hashes = league_payload_hashes(
            {"schedule": schedule, "players": players, "team_match_stats": team_match}
        )

        empty_shots, empty_player_match = parse_match_payload({}, scope, {})
        shots: list[pd.DataFrame] = []
        player_matches: list[pd.DataFrame] = []
        result_mask = schedule["is_result"].fillna(False).astype(bool)
        schedule.loc[result_mask, "has_data"] = False
        played_rows: list[dict[str, Any]] = []
        for row in schedule[result_mask].to_dict(orient="records"):
            try:
                match_payload = self.client.get_match_data(
                    int(row["game_id"]),
                    # Current-season cache entries are never trusted forever:
                    # a transient empty/partial response must heal on the next run.
                    force_refresh=force_refresh or mode in {"current", "reparse"},
                )
            except UnderstatHTTPError as exc:
                if exc.status_code == 404:
                    logger.warning("Understat match %s has no payload", row["game_id"])
                    continue
                raise
            if not match_payload:
                continue
            self._validate(
                validate_match_payload,
                match_payload,
                f"match_{quote(str(row['game_id']), safe='')}.json",
            )
            schedule.loc[
                schedule["game_id"] == row["game_id"], "has_data"
            ] = _match_payload_has_rows(match_payload)
            played_rows.append(row)
            shot_frame, player_frame = parse_match_payload(match_payload, scope, row)
            shots.append(shot_frame)
            player_matches.append(player_frame)

        empty_player_team, empty_breakdowns = parse_team_payload(
            {}, scope, team_id=0, team_name=""
        )
        player_team_frames: list[pd.DataFrame] = []
        breakdown_frames: list[pd.DataFrame] = []
        # A schedule-only future scope deliberately makes no per-team calls.
        if played_rows:
            # #1428: a team without a played match (empty history) has no
            # team page yet; asking for it only returns empty statistics.
            teams = sorted(
                (
                    (int(team["id"]), str(team["title"]))
                    for team in _team_records(league_payload.get("teams"))
                    if team.get("history")
                ),
                key=lambda item: item[0],
            )
            for team_id, team_name in teams:
                team_payload = self.client.get_team_data(
                    team_name,
                    source_season_id,
                    force_refresh=refresh_scope or mode == "reparse",
                )
                self._validate(
                    validate_team_payload,
                    team_payload,
                    _team_cache_name(team_name, source_season_id),
                    source_season_id=source_season_id,
                )
                player_team, breakdowns = parse_team_payload(
                    team_payload,
                    scope,
                    team_id=team_id,
                    team_name=team_name,
                )
                player_team_frames.append(player_team)
                breakdown_frames.append(breakdowns)

        return {
            "understat_schedule": schedule,
            "understat_shots": _concat(shots, empty_shots),
            "understat_players": players,
            "understat_team_match_stats": team_match,
            "understat_player_match_stats": _concat(
                player_matches, empty_player_match
            ),
            "understat_player_team_season_stats": _concat(
                player_team_frames, empty_player_team
            ),
            "understat_team_season_breakdowns": _concat(
                breakdown_frames, empty_breakdowns
            ),
        }


def _league_cache_name(source_league: str, source_season_id: int) -> str:
    """Same file name as ``UnderstatClient.get_league_data`` caches."""

    return f"league_{quote(source_league, safe='_-')}_{source_season_id}.json"


def _team_cache_name(team_name: str, source_season_id: int) -> str:
    """Same file name as ``UnderstatClient.get_team_data`` caches."""

    slug = quote(team_name.replace(" ", "_"), safe="_-")
    return f"team_{slug}_{source_season_id}.json"


def _team_records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        values = value.values()
    elif isinstance(value, list):
        values = value
    else:
        values = ()
    return [item for item in values if isinstance(item, Mapping)]


__all__ = ["UnderstatSource"]
