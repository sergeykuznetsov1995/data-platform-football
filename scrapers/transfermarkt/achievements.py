"""Parse saved Transfermarkt achievements. Adaptation notices are in
docs/operations/transfermarkt-achievements.md.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, fields
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Tag


ACHIEVEMENT_PARSER_REVISION = "achievements-v1"
ACHIEVEMENT_SCHEMA_REVISION = 1


class AchievementParseError(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class Achievement:
    recipient_type: str
    recipient_id: str
    title: str
    award_id: str | None
    trophy_id: str | None
    season_id: str | None
    season: str | None
    result: str | None
    club_id: str | None
    club_name: str | None
    competition_id: str | None
    source_row_id: str

    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def natural_key(self) -> tuple:
        return tuple(getattr(self, field) for field in ACHIEVEMENT_NATURAL_KEY)


ACHIEVEMENT_COLUMNS = tuple(field.name for field in fields(Achievement))
ACHIEVEMENT_NATURAL_KEY = (
    "recipient_type", "recipient_id", "award_id", "trophy_id", "competition_id",
    "title", "season_id", "season", "result", "source_row_id",
)


@dataclass(frozen=True)
class Reconciliation:
    source_group_id: str
    award_id: str | None
    title: str | None
    published_count: int | None
    detail_count: int


@dataclass(frozen=True)
class AchievementResult:
    recipient_type: str
    recipient_id: str
    status: str
    rows: tuple[Achievement, ...]
    reconciliation: tuple[Reconciliation, ...] = ()
    completeness_proof: str | None = None
    parser_revision: str = ACHIEVEMENT_PARSER_REVISION
    schema_revision: int = ACHIEVEMENT_SCHEMA_REVISION

    def canonical_json(self) -> str:
        return json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        )


def _text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AchievementParseError("invalid_text")
    return " ".join(value.split()) or None


def _id(value: object, *, zero_absent=False, empty_absent=False, token=False) -> str | None:
    if value is None or (empty_absent and value == ""):
        return None
    if type(value) is int and value >= 0:
        value = str(value)
    if isinstance(value, str):
        if re.fullmatch(r"[0-9]+", value):
            canonical = value.lstrip("0") or "0"
            return None if zero_absent and canonical == "0" else canonical
        if token and re.fullmatch(r"(?=.*[A-Za-z])[A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*", value):
            return value
    raise AchievementParseError("invalid_id")


def _owner(recipient_type, recipient_id) -> str:
    if recipient_type not in ("player", "club", "coach"):
        raise AchievementParseError("invalid_recipient_type")
    owner = _id(recipient_id)
    if owner in (None, "0"):
        raise AchievementParseError("invalid_recipient_id")
    return owner


def confirmed_empty_achievements(*, recipient_type, recipient_id, completeness_proof=None):
    owner = _owner(recipient_type, recipient_id)
    if not isinstance(completeness_proof, str) or not completeness_proof.strip():
        raise AchievementParseError("missing_completeness_proof")
    return AchievementResult(
        recipient_type, owner, "observed_empty", (), completeness_proof=completeness_proof.strip()
    )


def _object(value, reason):
    if not isinstance(value, dict):
        raise AchievementParseError(reason)
    return value


def _nested(row, name):
    value = row.get(name)
    return {} if value is None else _object(value, "invalid_" + name)


def _agree(*values, reason):
    populated = {value for value in values if value is not None}
    if len(populated) > 1:
        raise AchievementParseError(reason)
    return next(iter(populated), None)


def _count(value) -> int:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        try:
            return int(value)
        except ValueError:
            pass
    raise AchievementParseError("invalid_count")


def _json_rows(source, kind, owner):
    data = _object(source, "invalid_json_source")
    if "success" in data or "data" in data:
        if data.get("success") is not True:
            raise AchievementParseError("unsuccessful_envelope")
        data = _object(data.get("data"), "invalid_data")
    details = data.get("achievements")
    if not isinstance(details, list):
        raise AchievementParseError("invalid_achievements")

    groups = {}
    aggregates = data.get("aggregated", [])
    if not isinstance(aggregates, list):
        raise AchievementParseError("invalid_aggregated")
    for index, item in enumerate(aggregates):
        group = _object(item, "invalid_aggregate")
        award = _id(group.get("id"))
        if award is None or award in groups:
            raise AchievementParseError("invalid_aggregate_id")
        published = _agree(
            *(_count(group[key]) for key in ("count", "receivedCount", "amount", "number", "total") if key in group),
            reason="aggregate_count_conflict",
        )
        groups[award] = (f"aggregated:{index}", _text(group.get("name")), published)

    trophies = {}
    published_trophies = data.get("trophies", [])
    if not isinstance(published_trophies, list):
        raise AchievementParseError("invalid_trophies")
    for item in published_trophies:
        trophy = _object(item, "invalid_trophy")
        trophy_id = _id(trophy.get("id"), zero_absent=True)
        if trophy_id is None or trophy_id in trophies:
            raise AchievementParseError("invalid_trophy_id")
        trophies[trophy_id] = _text(trophy.get("name"))

    rows = []
    counts = Counter()
    observations = Counter()
    for index, item in enumerate(details):
        row = _object(item, "invalid_row")
        award = _id(row.get("id"))
        info = _nested(row, "details")
        trophy = _agree(
            _id(row.get("trophyId"), zero_absent=True),
            _id(info.get("trophyId"), zero_absent=True), reason="trophy_id_conflict",
        )
        title = _text(row.get("name"))
        if title is None and award in groups:
            title = groups[award][1]
        if title is None:
            title = trophies.get(trophy)
        if title is None:
            raise AchievementParseError("missing_title")
        player = _id(info.get("playerId"), zero_absent=True)
        if kind == "player" and player is not None and player != owner:
            raise AchievementParseError("owner_conflict")

        season = _nested(row, "season")
        season_id = _agree(
            _id(row.get("seasonId")), _id(season.get("id")),
            _id(info.get("seasonId"), zero_absent=True), reason="season_id_conflict"
        )
        club = _nested(row, "club")
        club_id = _agree(
            _id(club.get("id"), empty_absent=True),
            _id(row.get("clubId"), empty_absent=True),
            _id(info.get("clubId"), empty_absent=True, zero_absent=True),
            reason="club_id_conflict",
        )
        competition = _nested(row, "competition")
        competition_id = _agree(
            _id(competition.get("id"), empty_absent=True, token=True),
            _id(row.get("competitionId"), empty_absent=True, token=True),
            _id(info.get("competitionId"), empty_absent=True, zero_absent=True, token=True),
            reason="competition_id_conflict",
        )
        observation = _agree(
            *(_id(row.get(key), token=True) for key in
              ("sourceRowId", "source_row_id", "rowId", "row_id", "observationId", "observation_id")),
            reason="row_id_conflict",
        )
        locator = f"achievements:{index}"
        if observation is not None:
            observations[observation] += 1
            occurrence = observations[observation]
            locator = f"source:{observation}" if occurrence == 1 else f"source:{observation}:{occurrence}"
        display = season.get("display")
        if display is None:
            display = season.get("name")
        if display is None:
            display = season.get("year")
        if type(display) is int and display >= 0:
            display = str(display)
        result = _agree(_text(row.get("result")), _text(info.get("result")), reason="result_conflict")
        club_name = _text(club.get("name")) or _text(row.get("clubName"))
        rows.append(Achievement(
            kind, owner, title, award, trophy, season_id, _text(display), result,
            club_id, club_name, competition_id, locator,
        ))
        counts[award] += 1

    reconciled = []
    for award, (locator, title, published) in groups.items():
        if (published is None and not counts[award]) or (published is not None and published != counts[award]):
            raise AchievementParseError("aggregate_count_mismatch")
        reconciled.append(Reconciliation(locator, award, title, published, counts[award]))
    return AchievementResult(
        kind, owner, "observed_present" if rows else "observed_empty", tuple(rows), tuple(reconciled)
    )


_HEADING = "h1,h2,h3,h4,.content-box-headline,.table-header"
_TITLE = re.compile(r"([0-9]+)\s*[x×]\s+(.+)", re.IGNORECASE)
_SEASON = re.compile(r"(?:[0-9]{4}|[0-9]{2})(?:\s*[/–-]\s*(?:[0-9]{4}|[0-9]{2}))?")


def _label(node: Tag) -> str:
    return " ".join(node.get_text(" ", strip=True).split())


def _box_layouts(soup):
    grouped = []
    sections = []
    for index, box in enumerate(soup.select(".box")):
        headings = [
            node for node in box.select(_HEADING)
            if node.find_parent(class_="box") is box and not node.find_parent("table")
            and node.select_one(_HEADING) is None
        ]
        labels = [_label(node) for node in headings]
        matches = [s for s in labels if s.casefold() == "all titles" or _TITLE.fullmatch(s)]
        table_rows = [r for r in box.select("tr") if r.find_parent(class_="box") is box]
        if not matches:
            # Other profile boxes are common. A season table or count-like
            # heading in an unrecognised section may contain lost achievements.
            for tr in table_rows:
                cell = tr.find(["td", "th"], recursive=False)
                if cell and (_SEASON.fullmatch(_label(cell)) or _TITLE.fullmatch(_label(cell))):
                    raise AchievementParseError("unknown_coach_section")
            if any(re.match(r"(?:[0-9]+\s*[x×]|\S+[x×]\s+)", s, re.I) for s in labels):
                raise AchievementParseError("invalid_coach_title")
            continue
        if len(labels) != 1:
            raise AchievementParseError("ambiguous_coach_layout")
        for tr in table_rows:
            table = tr.find_parent("table")
            if table is None or table.find_parent(class_="box") is not box:
                raise AchievementParseError("invalid_coach_row")
        for cell in box.select("td,th"):
            if cell.find_parent(class_="box") is box and cell.parent.name != "tr":
                raise AchievementParseError("invalid_coach_row")
        section = (index, headings[0], table_rows)
        if matches[0].casefold() == "all titles":
            grouped.append(section)
        else:
            sections.append(section)
    if len(grouped) > 1 or (grouped and sections):
        raise AchievementParseError("ambiguous_coach_layout")
    if not grouped and not sections:
        raise AchievementParseError("missing_coach_layout")
    return grouped or sections


def _competition_from_links(node):
    ids = []
    for link in node.select('a[href*="/wettbewerb/"]'):
        try:
            path = urlsplit(link["href"]).path
        except ValueError as exc:
            raise AchievementParseError("invalid_competition_link") from exc
        match = re.search(r"/wettbewerb/([^/]+)(?:/saison_id/[0-9]+)?/?$", path)
        if not match:
            raise AchievementParseError("invalid_competition_link")
        ids.append(_id(match[1], token=True))
    return _agree(*ids, reason="competition_id_conflict")


def _team_cell(cell):
    ids = []
    seasons = []
    names = []
    printed_names = []
    links = cell.select("a")
    for link in links:
        if not link.has_attr("href"):
            raise AchievementParseError("invalid_club_link")
        if "/wettbewerb/" in link["href"]:
            continue
        try:
            url = urlsplit(link["href"])
        except ValueError as exc:
            raise AchievementParseError("invalid_club_link") from exc
        if url.scheme and (url.scheme not in ("http", "https") or not url.netloc):
            raise AchievementParseError("invalid_club_link")
        match = re.fullmatch(
            r"(?:/[^/]+)*/(?:startseite|profil)/verein/([0-9]+)(?:/saison_id/([0-9]+))?/?",
            url.path,
        )
        if not match or _id(match[1], zero_absent=True) is None:
            raise AchievementParseError("invalid_club_link")
        ids.append(_id(match[1]))
        seasons.append(_id(match[2]))
        if name := _label(link):
            printed_names.append(name)
        names.append(_text(link.get("title")))
    club_id = _agree(*ids, reason="club_id_conflict")
    if links and club_id is None:
        raise AchievementParseError("invalid_club_link")
    season_id = _agree(*seasons, reason="season_id_conflict")
    img = cell.find("img")
    logo_name = (_text(img.get("alt")) or _text(img.get("title"))) if img else None
    if printed_names:
        name = _agree(*printed_names, reason="club_name_conflict")
        note_parts = []
        for part in cell.strings:
            anchor = part.find_parent("a")
            if anchor is None or "/wettbewerb/" in anchor.get("href", ""):
                note_parts.append(str(part))
        return club_id, name, _text(" ".join(note_parts)), season_id
    name = _label(cell) or next((name for name in names if name), logo_name)
    return club_id, name, None, season_id


def _coach_detail(cells, owner, title, locator, competition_id):
    if len(cells) < 2 or any(cell.name != "td" for cell in cells):
        raise AchievementParseError("invalid_coach_row")
    season = _label(cells[0])
    if not _SEASON.fullmatch(season):
        raise AchievementParseError("invalid_coach_season")
    team = cells[1]
    club_id, club_name, note, season_id = _team_cell(team)
    extras = cells[2:]
    # Some tables put a linked crest and the team's printed name in
    # adjacent cells. Both links must identify the same team.
    if team.find("img") and not _label(team) and extras:
        candidate = extras[0]
        if not candidate.select_one('a[href*="/wettbewerb/"]') and (
            candidate.find("a") or _label(candidate) == club_name or club_name is None
        ):
            second_id, second_name, note, second_season = _team_cell(candidate)
            club_id = _agree(club_id, second_id, reason="club_id_conflict")
            season_id = _agree(season_id, second_season, reason="season_id_conflict")
            club_name = second_name or club_name
            extras = extras[1:]
    if not club_name and not club_id:
        raise AchievementParseError("invalid_coach_row")
    if len(extras) > 1 or any(cell.select_one('a[href*="/verein/"]') for cell in extras):
        raise AchievementParseError("invalid_coach_row")
    result = " ".join(part for part in (note, _label(extras[0]) if extras else None) if part) or None
    return Achievement(
        "coach", owner, title, None, None, season_id, season, result,
        club_id, club_name, competition_id, locator,
    )


def _coach_rows(source, owner):
    if not isinstance(source, str):
        raise AchievementParseError("invalid_html_source")
    soup = BeautifulSoup(source, "html.parser")
    if soup.select_one("#challenge-form, .g-recaptcha") or (
        soup.title and re.search(r"access denied|just a moment|captcha|blocked", _label(soup.title), re.I)
    ):
        raise AchievementParseError("blocked_html")
    rows = []
    reconciled = []
    for box_index, heading_node, table_rows in _box_layouts(soup):
        section = f"box:{box_index}"
        grouped = _label(heading_node).casefold() == "all titles"
        group = _TITLE.fullmatch(_label(heading_node))
        group_competition = _competition_from_links(heading_node)
        group_locator = section
        first = len(rows)

        def reconcile():
            actual = len(rows) - first
            if actual == 0:
                raise AchievementParseError("missing_coach_details")
            published = _count(group[1])
            if published != actual:
                raise AchievementParseError("aggregate_count_mismatch")
            reconciled.append(Reconciliation(group_locator, None, group[2], published, actual))

        for index, tr in enumerate(table_rows):
            cells = tr.find_all(["td", "th"], recursive=False)
            labels = [_label(cell) for cell in cells]
            if cells and all(cell.name == "th" for cell in cells) and (
                labels[0].casefold() in ("season", "year")
                and any(label.casefold() in ("club", "team") for label in labels[1:])
            ):
                continue
            locator = f"{section}:row:{index}"
            populated = [label for label in labels if label]
            header = _TITLE.fullmatch(populated[0]) if len(populated) == 1 else None
            if header:
                if not grouped:
                    raise AchievementParseError("ambiguous_coach_layout")
                if group:
                    reconcile()
                group, group_locator, first = header, locator, len(rows)
                group_competition = _competition_from_links(tr)
                continue
            if group is None:
                raise AchievementParseError("missing_coach_title")
            competition_id = _agree(group_competition, _competition_from_links(tr), reason="competition_id_conflict")
            rows.append(_coach_detail(cells, owner, group[2], locator, competition_id))
        if group is None:
            raise AchievementParseError("missing_coach_title")
        reconcile()
    return AchievementResult("coach", owner, "observed_present", tuple(rows), tuple(reconciled))


def parse_achievements(source, *, recipient_type, recipient_id) -> AchievementResult:
    owner = _owner(recipient_type, recipient_id)
    if recipient_type == "coach":
        return _coach_rows(source, owner)
    return _json_rows(source, recipient_type, owner)
