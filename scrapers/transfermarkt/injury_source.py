"""Source fields and episode arithmetic adapted from felipeall/transfermarkt-api.

At 71aa75ecd708e5c8c283c4af5968dd370777b3f8 the services players/injuries.py
and players/absences.py use reason, dates, duration and competition as published.
The strict envelope and duration checks come from the Football #2285 adapter.
See licenses/transfermarkt-api-felipeall-MIT.txt.
"""

from __future__ import annotations

from datetime import date
import json
import re


class InjurySectionError(ValueError):
    """Fixed, source-independent failure reason suitable for a lane outcome."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def identity(*parts):
    return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))


def _integer(value, *, minimum=0):
    if type(value) is not int or not minimum <= value <= 2**63 - 1:
        raise InjurySectionError("invalid_source_integer")
    return value


def _iso_date(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value
    ):
        raise InjurySectionError("invalid_injury_date")
    return date.fromisoformat(value)


def _text(value):
    if not isinstance(value, str) or not value.strip():
        raise InjurySectionError("invalid_injury_text")
    return value


def _competition_id(value):
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9_-]+", value):
        raise InjurySectionError("invalid_absence_competition")
    return value


def api_history(body, player, *, collection="injuries"):
    """Consume a full player response; the donor paginates this source locally."""
    doc = json.loads(body)
    if (
        not isinstance(doc, dict)
        or doc.get("success") is not True
        or doc.keys() - {"data", "success", "message"}
    ):
        raise InjurySectionError("invalid_history_envelope")
    data = doc.get("data")
    absent = collection == "absences"
    expected = {"playerId", collection}
    if absent:
        expected.add("competitionIds")
    if (
        not isinstance(data, dict)
        or data.keys() != expected
        or str(data["playerId"]) != player
        or not isinstance(data[collection], list)
    ):
        raise InjurySectionError("invalid_player_history")
    records = []
    for item in data[collection]:
        if not isinstance(item, dict) or str(item.get("playerId")) != player:
            raise InjurySectionError("history_player_mismatch")
        raw = _text(item["name"])
        season = _integer(item["seasonId"], minimum=1800)
        if season > 2199:
            raise InjurySectionError("invalid_history_season")
        start = _iso_date(item["start"])
        end = _iso_date(item["end"]) if item.get("end") is not None else None
        if end is not None and end < start:
            raise InjurySectionError("episode_date_order")
        details = item.get("durationDetails")
        if details is not None and not isinstance(details, dict):
            raise InjurySectionError("invalid_episode_duration")
        duration = (details or {}).get("days")
        days = (end - start).days + 1 if end is not None else None
        if duration is not None:
            _integer(duration)
            # tmapi duration excludes the first day; display counts both endpoints.
            if days is not None and days != duration + 1:
                raise InjurySectionError("episode_duration_conflict")
            days = _integer(duration + 1)
        games = item.get("missedGamesCount")
        row = dict(
            player_id=player,
            season_id=season,
            season_label=None,
            until_date=end.isoformat() if end else None,
            days=days,
            duration_days=duration,
            games_missed=_integer(games) if games is not None else None,
        )
        since = start.isoformat()
        if absent:
            kind = absence_kind(raw)
            if kind in {"injury", "risk_of_suspension"}:
                raise InjurySectionError("non_absence_in_history")
            competition = _competition_id(item["competitionId"])
            club = item.get("clubId")
            if club is not None and not re.fullmatch(r"[1-9][0-9]*", str(club)):
                raise InjurySectionError("invalid_published_club")
            possible = item.get("possibleMissingGamesCount")
            row.update(
                club_id=str(club) if club is not None else None,
                competition_id=competition,
                absence_id=_integer(item["absenceId"], minimum=1),
                absence_kind=kind,
                reason_raw=raw,
                since=since,
                absence_identity=identity(since, raw, competition),
                possible_missing_games=_integer(possible)
                if possible is not None
                else None,
            )
            source_season = item.get("season")
            if source_season is not None:
                if (
                    not isinstance(source_season, dict)
                    or source_season.get("id") != season
                ):
                    raise InjurySectionError("conflicting_absence_season")
                row["season_label"] = _text(source_season["display"])
        else:
            row.update(
                episode_key=identity(player, raw, since),
                injury=raw,
                injury_id=_integer(item["injuryId"], minimum=1),
                category=_text(item["category"]),
                from_date=since,
                affected_clubs_json=None,
                page_number=1,
                source_next_url=None,
            )
        records.append(row)
    if absent:
        competition_ids = data["competitionIds"]
        if not isinstance(competition_ids, list) or {
            _competition_id(c) for c in competition_ids
        } != {r["competition_id"] for r in records}:
            raise InjurySectionError("unreconciled_absence_competitions")
    return records


def absence_kind(text, *, source_group=None):
    """Classify mixed club rows only when the published reason supports the class."""
    reason = " ".join(text.casefold().replace("-", " ").split())
    if reason == "risk of suspension":
        return "risk_of_suspension"
    if reason in {
        "suspension",
        "suspended",
        "red card suspension",
        "yellow card suspension",
        "indirect card suspension",
        "disciplinary suspension",
        "suspension for doping",
        "red card ban",
        "yellow card ban",
        "suspended by club",
        "suspension through sports court",
    }:
        return "suspension"
    if re.fullmatch(
        r"suspension (?:through|due to) [0-9]+(?:st|nd|rd|th) yellow card", reason
    ):
        return "suspension"
    if reason in {
        "national call up",
        "national team call up",
        "international duty",
        "national team duty",
        "called up to national team",
    }:
        return "national_callup"
    if reason in {
        "personal leave",
        "special leave",
        "parental leave",
        "personal reasons",
        "international clearance",
        "not eligible to play",
        "no eligibility",
        "olympic games",
        "not registered",
        "visa issues",
    }:
        return "other"
    if re.search(
        r"\b(injur\w*|tear|torn|surgery|fracture|illness|ill|strain|bruise|contusion|"
        r"sprain|inflam\w*|concussion|knock|tendinitis|rupture|coronavirus|covid)\b",
        reason,
    ):
        return "injury"
    if re.fullmatch(
        r"(?:knee|thigh|muscular|muscle|groin|hamstring|ankle|hip|back|calf|"
        r"shoulder|foot|achilles tendon|hip flexor) (?:problems|pain|stiffness)",
        reason,
    ):
        return "injury"
    if source_group == "injuries":
        return "injury"
    raise InjurySectionError("unclassified_absence_reason")
