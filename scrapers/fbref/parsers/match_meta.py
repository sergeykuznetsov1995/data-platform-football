"""Extract the fields published in an FBref match scorebox."""

import re
from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup, Comment, Tag

from scrapers.fbref.parsers.id_extractors import PLAYER_ID_PATTERN, TEAM_ID_PATTERN
from scrapers.fbref.parsers.table_parser import extract_tables_from_comments


def _text(tag: Tag | None) -> str | None:
    if tag is None:
        return None
    return " ".join(tag.get_text(" ", strip=True).split()) or None


def _team_blocks(scorebox: Tag) -> tuple[Tag | None, Tag | None]:
    # Indexed IDs identify a side even if one block is missing or reordered.
    home = scorebox.find(id="sb_team_0")
    away = scorebox.find(id="sb_team_1")
    if home is not None and away is not None:
        return home, away
    blocks = scorebox.find_all("div", class_="scorebox_team", recursive=False)
    if not blocks:
        # Legacy pages put each squad directly under the scorebox alongside
        # venue/officials and two event columns. Only team blocks have a
        # squad link or a score; some results-only pages have bare scores.
        blocks = [
            block
            for block in scorebox.find_all("div", recursive=False)
            if not {"scorebox_meta", "event"}.intersection(block.get("class", ()))
            and (
                block.find("a", href=TEAM_ID_PATTERN) is not None
                or block.select_one(".score") is not None
                or "score" in block.get("class", ())
            )
        ]
    if len(blocks) != 2:
        return home, away
    first, second = blocks
    if home is second or away is first:
        return second, first
    return first, second


def _label_siblings(label: Tag):
    for item in label.next_siblings:
        if isinstance(item, Tag) and (
            item.name in {"br", "strong"} or item.find("strong") is not None
        ):
            break
        if not isinstance(item, Comment):
            yield item


def _label_value(block: Tag | None, name: str) -> tuple[str | None, str | None]:
    """Read only this label's siblings, including a linked player ID if present."""
    if block is None:
        return None, None
    for label in block.find_all("strong"):
        if label.get_text(strip=True).rstrip(":").casefold() != name:
            continue
        texts = []
        player_id = None
        for item in _label_siblings(label):
            if isinstance(item, Tag):
                texts.append(item.get_text(" ", strip=True))
                link = (
                    item if item.name == "a" else item.find("a", href=PLAYER_ID_PATTERN)
                )
                if link is not None and (
                    match := PLAYER_ID_PATTERN.search(link.get("href", ""))
                ):
                    player_id = match.group(1)
            else:
                texts.append(str(item))
        value = " ".join(" ".join(texts).split()).lstrip(": ").strip()
        if value.casefold() in {"", "-", "–", "—", "n/a"}:
            return None, None
        return value, player_id
    return None, None


def _fill_shared_captains(soup: BeautifulSoup, meta: Tag, row: dict) -> None:
    # Multiple shared labels can appear in either order. Keep the evidence
    # for each player together so conflicting team or lineup IDs stay visible.
    candidates: dict[str, str | None] = {}
    sides_by_player: dict[str, set[str]] = defaultdict(set)
    for label in meta.find_all("strong"):
        if label.get_text(strip=True).rstrip(":").casefold() != "captain":
            continue
        players = {}
        teams = set()
        for field in _label_siblings(label):
            if not isinstance(field, Tag):
                continue
            links = [field] if field.name == "a" else field.find_all("a", href=True)
            for link in links:
                href = link.get("href", "")
                if match := PLAYER_ID_PATTERN.search(href):
                    players[match.group(1)] = _text(link)
                elif match := TEAM_ID_PATTERN.search(href):
                    teams.add(match.group(1))
        if len(players) == 1:
            player_id, name = next(iter(players.items()))
            candidates[player_id] = name
            for team_id in teams:
                sides = {
                    side
                    for side in ("home", "away")
                    if row[f"{side}_team_id"] == team_id
                }
                sides_by_player[player_id].update(sides or {"unknown"})
    if not candidates:
        return
    comment_tables = extract_tables_from_comments(soup)
    for side, slot in (("home", "a"), ("away", "b")):
        own_captain = row[f"{side}_captain_id"]
        if own_captain in candidates:
            sides_by_player[own_captain].add(side)
        members = soup.select(f"div.lineup#{slot} tr, #lineup_{slot} tr")
        team_id = row[f"{side}_team_id"]
        if team_id:
            summary_id = f"stats_{team_id}_summary"
            members.extend(
                soup.select(f'table[id="{summary_id}"] [data-stat="player"]')
            )
            for table in comment_tables:
                if table.get("id") == summary_id:
                    members.extend(table.select('[data-stat="player"]'))
        for member in members:
            link = member.find("a", href=PLAYER_ID_PATTERN)
            if link:
                player_id = PLAYER_ID_PATTERN.search(link["href"]).group(1)
                if player_id in candidates:
                    sides_by_player[player_id].add(side)
    for side in ("home", "away"):
        if row[f"{side}_captain"] or row[f"{side}_captain_id"]:
            continue
        matched = [
            player_id
            for player_id in candidates
            if sides_by_player[player_id] == {side}
        ]
        if len(matched) == 1:
            player_id = matched[0]
            row[f"{side}_captain_id"] = player_id
            row[f"{side}_captain"] = candidates[player_id]


def _team_fields(block: Tag | None) -> dict[str, str | float | int | None]:
    team = team_id = goals = xg = captain = captain_id = None
    if block is not None:
        # The crest sometimes links to the same squad with no visible name.
        for link in block.find_all("a", href=TEAM_ID_PATTERN):
            match = TEAM_ID_PATTERN.search(link["href"])
            team_id = match.group(1)
            team = _text(link)
            if team:
                break
        score = (
            block if "score" in block.get("class", ()) else block.select_one(".score")
        )
        value = _text(score)
        if value and value.isdecimal():
            goals = int(value)
        value = _text(block.select_one(".score_xg"))
        if value and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value):
            xg = float(value)
        captain, captain_id = _label_value(block, "captain")
    return {
        "team": team,
        "team_id": team_id,
        "goals": goals,
        "xg": xg,
        "captain": captain,
        "captain_id": captain_id,
    }


def parse_match_meta(soup: BeautifulSoup) -> pd.DataFrame | None:
    """One row per match with explicit source fields, including scoreless previews."""
    scorebox = soup.select_one(".scorebox, #scorebox")
    if scorebox is None:
        return None
    meta = scorebox.select_one(".scorebox_meta")
    venue, _ = _label_value(meta, "venue")
    attendance, _ = _label_value(meta, "attendance")
    attendance_number = None
    if attendance and re.fullmatch(
        r"(?:[0-9]+|[0-9]{1,3}(?:[, ][0-9]{3})+)", attendance
    ):
        attendance_number = int(attendance.replace(",", "").replace(" ", ""))
    row = {"venue": venue, "attendance": attendance_number}
    for side, block in zip(("home", "away"), _team_blocks(scorebox)):
        row.update(
            {f"{side}_{field}": value for field, value in _team_fields(block).items()}
        )
    if meta is not None and (
        row["home_captain"] is None or row["away_captain"] is None
    ):
        _fill_shared_captains(soup, meta, row)
    # A venue alone cannot validate a scored page with unidentifiable sides.
    # Let the caller classify published but unparsed data as schema drift.
    scores = [
        score for score in scorebox.select(".score") if (_text(score) or "").isdecimal()
    ]
    if len(scores) == 2 and (row["home_goals"] is None or row["away_goals"] is None):
        return None
    if all(value is None for value in row.values()):
        return None
    # The first fixture in a new table can lack every optional numeric value.
    # Explicit nullable dtypes keep that fixture from creating VARCHAR columns.
    dtypes = dict.fromkeys(row, "string")
    dtypes.update(
        attendance="Int64",
        home_goals="Int64",
        away_goals="Int64",
        home_xg="Float64",
        away_xg="Float64",
    )
    return pd.DataFrame([row]).astype(dtypes)
