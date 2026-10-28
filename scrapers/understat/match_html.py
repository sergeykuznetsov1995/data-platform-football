"""Source-faithful parsing of fields available only on match HTML."""

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup
import pandas as pd

from .parsers import UnderstatSchemaDrift


CARD_COLUMNS = ("yellow_card_minutes", "red_card_minutes")


def parse_match_html(html: str, row: dict[str, Any], players: pd.DataFrame) -> dict[str, Any]:
    """Bind the recorded page to this match and card events to roster identities."""
    try:
        blocks = re.findall(r"\bvar\s+match_info\s*=\s*JSON\.parse\('([^']*)'\)", html)
        if len(blocks) != 1:
            raise ValueError("missing/ambiguous match_info")
        # Understat encodes JSON punctuation as JS hexadecimal escapes. Decode
        # only those escapes; unicode_escape would corrupt literal UTF-8 names.
        info = json.loads(re.sub(r"\\x([0-9a-fA-F]{2})",
                                 lambda m: chr(int(m[1], 16)), blocks[0]))
        for key, column in (("id", "game_id"), ("h", "home_team_id"), ("a", "away_team_id")):
            if int(info[key]) != int(row[column]):
                raise ValueError(f"match identity mismatch: {key}")
        fid = int(info["fid"]) if info.get("fid") not in (None, "", "0", 0) else None
        if fid is not None and fid <= 0:
            raise ValueError("invalid fid")
        soup = BeautifulSoup(html, "html.parser")
        timelines = soup.select(".timeline-block")
        if len(timelines) != 1:
            raise ValueError("missing/ambiguous timeline")
        cards: dict[tuple[str, int], dict[str, list[str]]] = {}
        for event in timelines[0].select(".timeline-item"):
            side = "a" if "timeline-item-right" in event.get("class", []) else "h"
            for icon in event.select(".yellow-card, .red-card"):
                group = icon.find_parent(class_="timeline-group")
                links = group.select("a.timeline-player-name") if group else []
                minute_node = event.select_one(".timeline-time")
                minute = minute_node.get_text(strip=True) if minute_node else ""
                if len(links) != 1 or not re.fullmatch(r"\d+(?:\+\d+)?'", minute):
                    raise ValueError("ambiguous card event")
                player_match = re.fullmatch(r"(?:https://understat\.com)?/player/(\d+)", links[0].get("href", ""))
                if player_match is None:
                    raise ValueError("invalid card player link")
                player_id = int(player_match[1])
                roster = players[(players["player_id"] == player_id) & (players["team_side"] == side)]
                if len(roster) != 1 or int(roster.iloc[0]["team_id"]) != int(info[side]):
                    raise ValueError("card player absent/ambiguous in roster")
                column = CARD_COLUMNS[0] if "yellow-card" in icon.get("class", []) else CARD_COLUMNS[1]
                cards.setdefault((side, player_id), {key: [] for key in CARD_COLUMNS})[column].append(minute)
        return {"whoscored_game_id": fid, "cards": cards, "match_info": info}
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        raise UnderstatSchemaDrift(f"match HTML: {exc}") from exc


def validate_card_counts(parsed: dict[str, Any], players: pd.DataFrame) -> None:
    """Refuse publication on either missing or surplus HTML events.

    The recorded 29049 pair itself has a surplus yellow event (Pedro Neto).
    Extraction remains testable independently; this mismatch cannot publish.
    """
    for player in players.to_dict(orient="records"):
        events = parsed["cards"].get((player["team_side"], int(player["player_id"])), {})
        for counter, column in zip(("yellow_cards", "red_cards"), CARD_COLUMNS):
            count = player[counter]
            if pd.isna(count) or int(count) != len(events.get(column, [])):
                raise UnderstatSchemaDrift(f"match HTML: incomplete card timeline: {counter}")
