"""
FlareSolverr-backed WhoScored events fetcher.

soccerdata's ``read_events`` internally uses ``BaseSeleniumReader`` (seleniumbase
+ undetected-chromedriver), and during 2026-04 the WhoScored Cloudflare
challenge stopped resolving inside the selenium ``script_timeout`` window —
``read_events`` falls into a 5×retry full-driver-restart loop and never writes
to Iceberg (data is buffered in memory until the very end).

This module reproduces just the bit of soccerdata that we need:
1. Fetch ``/Matches/{match_id}/Live`` HTML through a :class:`FlareSolverrClient`
   session (Cloudflare challenge handled by the FlareSolverr service);
2. Parse the inline ``require.config.params['args'].matchCentreData`` literal
   off the rendered HTML;
3. Convert it into the same DataFrame shape that
   ``soccerdata.WhoScored.read_events(output_fmt='events')`` returns
   (columns from :data:`COLS_EVENTS`).

This bypasses ``BaseSeleniumReader._download_and_save`` entirely — no
``read_schedule`` round-trip, no selenium driver during events scraping.

Use as::

    from scrapers.base.flaresolverr_client import FlareSolverrClient
    from scrapers.whoscored.events_fetcher import (
        fetch_match_events_via_flaresolverr,
        parse_matchcentre_to_events_df,
    )

    client = FlareSolverrClient(...)
    session_id = client.create_session()
    data = fetch_match_events_via_flaresolverr(client, match_id=1903158,
                                               session_id=session_id)
    df = parse_matchcentre_to_events_df(
        data,
        league='ENG-Premier League',
        season='2526',
        game_id=1903158,
        game_name='2026-04-29 Arsenal-Chelsea',
    )
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from scrapers.base.flaresolverr_client import FlareSolverrClient

logger = logging.getLogger(__name__)


# Mirrors ``soccerdata.whoscored.COLS_EVENTS`` (output_fmt='events'). Kept as a
# local copy so we don't have to import soccerdata into the events flow.
# Keep in sync with the upstream when soccerdata bumps versions.
COLS_EVENTS: Dict[str, Any] = {
    "game_id": np.nan,
    "period": np.nan,
    "minute": -1,
    "second": -1,
    "expanded_minute": -1,
    "type": np.nan,
    "outcome_type": np.nan,
    "team_id": np.nan,
    "team": np.nan,
    "player_id": np.nan,
    "player": np.nan,
    "x": np.nan,
    "y": np.nan,
    "end_x": np.nan,
    "end_y": np.nan,
    "goal_mouth_y": np.nan,
    "goal_mouth_z": np.nan,
    "blocked_x": np.nan,
    "blocked_y": np.nan,
    "qualifiers": [],
    "is_touch": False,
    "is_shot": False,
    "is_goal": False,
    "card_type": np.nan,
    "related_event_id": np.nan,
    "related_player_id": np.nan,
}


WHOSCORED_MATCH_URL = "https://www.whoscored.com/Matches/{}/Live"

# `matchCentreData` is assigned inside an inline <script> as
#   require.config.params["args"] = { matchId:..., matchCentreData: { ... } };
# Runtime ``page.evaluate("require.config.params['args'].matchCentreData")``
# is unreliable — the assignment runs late in the JS bundle, so a fresh
# evaluate often races and times out. Parsing the literal directly off the
# rendered HTML works regardless of script-execution timing.
_MATCHCENTRE_RE = re.compile(r"matchCentreData['\"]?\s*:\s*\{")


def _extract_matchcentre_from_html(html: str, match_id: int) -> Optional[dict]:
    """Find and parse the ``matchCentreData`` JSON object inside the page.

    Uses a brace-matching scan with string awareness so nested ``{`` / ``}``
    inside player names or qualifiers don't break extraction.
    """
    m = _MATCHCENTRE_RE.search(html)
    if not m:
        logger.warning(
            "matchCentreData literal not found in HTML for match %s", match_id
        )
        return None

    start = m.end() - 1  # index of the opening '{'
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                blob = html[start : i + 1]
                try:
                    return json.loads(blob)
                except json.JSONDecodeError as e:
                    logger.warning(
                        "matchCentreData JSON decode failed for match %s: %s",
                        match_id,
                        e,
                    )
                    return None
    logger.warning("matchCentreData braces unmatched for match %s", match_id)
    return None


def fetch_match_events_via_flaresolverr(
    client: "FlareSolverrClient",
    match_id: int,
    session_id: str,
    max_timeout_ms: int = 120_000,
) -> Optional[dict]:
    """Fetch a WhoScored match page through FlareSolverr and pull ``matchCentreData`` JSON.

    Args:
        client: An initialised :class:`FlareSolverrClient` instance.
        match_id: Integer WhoScored game id.
        session_id: FlareSolverrClient session id to reuse across requests.
        max_timeout_ms: Per-request timeout passed to FlareSolverr (milliseconds).

    Returns:
        Parsed ``matchCentreData`` dict, or ``None`` if the page returned a
        non-200 status / the literal was absent. ``FlareSolverrError``,
        ``FlareSolverrTimeout`` and ``FlareSolverrCFChallengeFailed`` propagate
        to the caller for retry/rotation handling.
    """
    url = WHOSCORED_MATCH_URL.format(match_id)
    solution = client.get(url, session_id, max_timeout_ms=max_timeout_ms)

    status = solution.get("status")
    if status != 200:
        logger.warning(
            "WhoScored: FS returned status=%s for match_id=%s", status, match_id
        )
        return None

    html = solution.get("html") or ""
    data = _extract_matchcentre_from_html(html, match_id)
    if not (isinstance(data, dict) and "events" in data):
        logger.warning(
            "matchCentreData not found / no events for match_id=%s", match_id
        )
        return None
    return data


_SNAKE_RE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_RE_2 = re.compile(r"__([A-Z])")
_SNAKE_RE_3 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake(name: str) -> str:
    """Mirror of ``soccerdata._common.standardize_colnames.to_snake``."""
    name = _SNAKE_RE_1.sub(r"\1_\2", name)
    name = _SNAKE_RE_2.sub(r"_\1", name)
    name = _SNAKE_RE_3.sub(r"\1_\2", name)
    return name.lower().replace("-", "_").replace(" ", "")


def _resolve_display_name(value: Any) -> Any:
    """soccerdata flattens ``{type, displayName, value}`` dicts to displayName."""
    if isinstance(value, dict):
        return value.get("displayName")
    return value


def parse_matchcentre_to_events_df(
    data: Optional[dict],
    league: str,
    season: str,
    game_id: int,
    game_name: str,
) -> pd.DataFrame:
    """Convert a ``matchCentreData`` dict to the soccerdata events schema.

    Reproduces ``soccerdata.WhoScored.read_events(output_fmt='events')`` for a
    single game without invoking soccerdata. The returned DataFrame uses the
    same MultiIndex (league, season, game) that downstream save logic expects
    after a ``reset_index()``.

    Args:
        data: Parsed ``matchCentreData`` JSON (output of
            :func:`fetch_match_events_via_flaresolverr`).
        league: Canonical league name (e.g. ``'ENG-Premier League'``).
        season: soccerdata ``YYZZ`` short form (e.g. ``'2526'``).
        game_id: Integer game id.
        game_name: Human-readable game tag matching what
            ``read_schedule()`` would produce, e.g.
            ``'2026-04-29 Arsenal-Chelsea'``.

    Returns:
        DataFrame with columns from :data:`COLS_EVENTS` + ``league``,
        ``season``, ``game`` (set as a 3-level MultiIndex). Empty DataFrame
        on missing/invalid data.
    """
    if not isinstance(data, dict) or "events" not in data:
        return pd.DataFrame()

    events = data.get("events") or []
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    df["game"] = game_name
    df["league"] = league
    df["season"] = season
    df["game_id"] = game_id

    df.columns = [_to_snake(c) for c in df.columns]

    player_names = {
        int(k): v for k, v in (data.get("playerIdNameDictionary") or {}).items()
    }
    team_names: Dict[int, str] = {}
    for side in ("home", "away"):
        side_obj = data.get(side) or {}
        if "teamId" in side_obj and "name" in side_obj:
            team_names[int(side_obj["teamId"])] = side_obj["name"]

    if "player_id" in df.columns and player_names:
        df["player"] = df["player_id"].replace(player_names)
    elif "player" not in df.columns:
        df["player"] = np.nan

    if "team_id" in df.columns and team_names:
        df["team"] = df["team_id"].replace(team_names)
    elif "team" not in df.columns:
        df["team"] = np.nan

    for col in ("outcome_type", "card_type", "type", "period"):
        if col in df.columns:
            df[col] = df[col].apply(_resolve_display_name)

    for col, default in COLS_EVENTS.items():
        if col not in df.columns:
            if isinstance(default, list):
                df[col] = [list(default) for _ in range(len(df))]
            else:
                df[col] = default

    df = df.set_index(["league", "season", "game"]).sort_index()
    df = df[list(COLS_EVENTS.keys())]
    return df
