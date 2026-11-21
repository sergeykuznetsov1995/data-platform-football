"""Offline Transfermarkt rumour assertions; no network or persistence.

Adapted from omkarcloud/transfermarkt-scraper ``parse_rumour_rows``:
commit ad660278c0c95bd46c2d0553658fcd14ebd9cb8f,
transfermarkt/competitions.py, blob 1abc32204ff6345f8dffa4707be9276f2850726c.
Copyright (c) 2026 Chetan Jain, MIT; full notice:
scrapers/transfermarkt/licenses/transfermarkt-scraper-omkarcloud-MIT.txt.
Adaptations: strict capture validation, immutable typed observations, explicit
competition/clock, canonical JSON, historical/correction keys and reconciliation.
Rumours are source assertions, never confirmed transfers or match records.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Literal
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag


PARSER_VERSION = "transfermarkt-rumours/1"
SCHEMA_VERSION = "transfer_rumour_assertion/1"
ENTITY_TYPE = "transfer_rumour_assertion"
OBSERVATION_KEY_FIELDS = (
    "rumour_identity", "subject_id", "observed_at", "correction_identity"
)
ASSERTION_KEY_FIELDS = ("rumour_identity", "subject_id")
_BASE = "https://www.transfermarkt.com"
_HOSTS = {"transfermarkt.com", "www.transfermarkt.com",
          "transfermarkt.de", "www.transfermarkt.de",
          "transfermarkt.co.uk", "www.transfermarkt.co.uk"}
_HEADERS = {
    "Player": "player", "Interested club": "interested_club",
    "Most recent source": "source", "Last reply": "reply",
    "Nation": "nationalities", "Nat.": "nationalities", "Age": "age",
    "Club": "current_club", "Current club": "current_club",
    "Assessment": "probability", "User assessment": "probability",
}
_NULLS = {"", "-", "—"}


class RumourParseError(ValueError):
    """An atomic capture failure with a stable machine-readable reason."""

    def __init__(self, reason: str, *, row: int | None = None):
        self.reason = reason
        self.row = row
        super().__init__(reason if row is None else f"{reason} (row {row})")


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _hash(value: object) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _text(node: Tag | None) -> str:
    return " ".join(node.stripped_strings) if node is not None else ""


def _url(raw: str) -> str:
    if (not isinstance(raw, str) or not raw or
            re.search(r"[\s\\\x00-\x1f\x7f]", raw) or
            re.search(r"%(?![0-9a-fA-F]{2})", raw)):
        raise RumourParseError("invalid_url")
    try:
        parts = urlsplit(raw)
        if raw.startswith("/") and not raw.startswith("//"):
            parts = urlsplit(_BASE + raw)
        if (parts.scheme not in {"http", "https"} or
                parts.hostname not in _HOSTS or parts.username is not None or
                parts.password is not None or parts.port is not None):
            raise ValueError
        decoded = unquote(parts.path)
        if (not decoded.startswith("/") or "\\" in decoded or
                re.search(r"[\x00-\x20\x7f]", decoded) or
                any(p in {".", ".."} for p in decoded.split("/"))):
            raise ValueError
        return urlunsplit(("https", "www.transfermarkt.com", parts.path,
                           urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True))), parts.fragment))
    except (ValueError, UnicodeError) as exc:
        raise RumourParseError("invalid_url") from exc


def _clock(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value
    ):
        raise RumourParseError("invalid_observed_at")
    if value.endswith("-00:00"):
        raise RumourParseError("invalid_observed_at")
    try:
        # datetime accepts offset minutes >= 60 by normalizing; reject those.
        if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, OverflowError) as exc:
        raise RumourParseError("invalid_observed_at") from exc


@dataclass(frozen=True)
class SourceEntity:
    id: str
    name: str
    url: str


@dataclass(frozen=True)
class CompetitionContext:
    id: str
    name: str
    url: str


def _competition(context: CompetitionContext) -> CompetitionContext:
    if (not isinstance(context, CompetitionContext) or
            not isinstance(context.id, str) or
            not re.fullmatch(r"[A-Z0-9]+", context.id) or
            not isinstance(context.name, str) or not context.name.strip()):
        raise RumourParseError("invalid_competition")
    try:
        url = _url(context.url)
        match = re.search(r"/wettbewerb/([A-Z0-9]+)(?:/|$)", urlsplit(url).path)
        if match is None or match.group(1) != context.id:
            raise ValueError
    except (RumourParseError, ValueError) as exc:
        raise RumourParseError("invalid_competition") from exc
    return CompetitionContext(context.id, " ".join(context.name.split()), url)


def _entity(cell: Tag | None, kind: str, *, required: bool = False) -> SourceEntity | None:
    reason = "invalid_player" if kind == "spieler" else "invalid_club"
    if cell is None or (_text(cell) in _NULLS and not cell.find(["a", "img"])):
        if required:
            raise RumourParseError(reason)
        return None
    candidates = []
    for anchor in cell.select("a[href]"):
        href = anchor.get("href", "")
        url = _url(href)
        if f"/{kind}/" not in href:
            continue
        match = re.search(rf"/{kind}/([1-9][0-9]*)(?:/|$)", urlsplit(url).path)
        if match is None:
            raise RumourParseError(reason)
        name = _text(anchor) or str(anchor.get("title", "")).strip()
        if not name:
            img = anchor.find("img")
            if img is not None:
                name = str(img.get("title") or img.get("alt") or "").strip()
        candidates.append((match.group(1), name, url))
    if not candidates or len({c[0] for c in candidates}) != 1:
        raise RumourParseError(reason)
    named = [c for c in candidates if c[1]]
    if not named:
        raise RumourParseError(reason)
    return SourceEntity(*named[0])


def _interested_competition(cell: Tag) -> SourceEntity | None:
    refs = []
    for anchor in cell.select("a[href]"):
        href = anchor.get("href", "")
        url = _url(href)
        if "/wettbewerb/" not in href:
            continue
        match = re.search(r"/wettbewerb/([A-Z0-9]+)(?:/|$)", urlsplit(url).path)
        name = _text(anchor) or str(anchor.get("title", "")).strip()
        if match is None or not name:
            raise RumourParseError("invalid_competition")
        refs.append(SourceEntity(match.group(1), name, url))
    if len({r.id for r in refs}) > 1:
        raise RumourParseError("invalid_competition")
    return refs[0] if refs else None


def _date(cell: Tag | None) -> str | None:
    value = _text(cell)
    if value in _NULLS:
        return None
    # Source labels may accompany a date. Exactly one complete date is required.
    tokens = re.findall(
        r"(?<![\w\d])(?:\d{4}-\d{2}-\d{2}|\d{1,2}\.\d{1,2}\.\d{4}|"
        r"[A-Za-z]{3} \d{1,2}, \d{4})(?![\w\d])", value
    )
    if len(tokens) != 1:
        raise RumourParseError("invalid_date")
    token = tokens[0]
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", token):
            return date.fromisoformat(token).isoformat()
        if "." in token:
            day, month, year = map(int, token.split("."))
        else:
            months = {m: n for n, m in enumerate(
                ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1)}
            month_name, day_text, year_text = token.split()
            month, day, year = months[month_name], int(day_text.rstrip(",")), int(year_text)
        return date(year, month, day).isoformat()
    except (ValueError, KeyError) as exc:
        raise RumourParseError("invalid_date") from exc


def _probability(cell: Tag | None) -> float | None:
    value = _text(cell)
    if value in _NULLS:
        return None
    if not re.fullmatch(r"\d+(?:[.,]\d+)?\s*%", value):
        raise RumourParseError("invalid_probability")
    try:
        number = Decimal(value.rstrip("%").strip().replace(",", "."))
        if not 0 <= number <= 100:
            raise InvalidOperation
        return float(number)
    except InvalidOperation as exc:
        raise RumourParseError("invalid_probability") from exc


def _age(cell: Tag | None) -> int | None:
    value = _text(cell)
    if value in _NULLS:
        return None
    if not re.fullmatch(r"[0-9]{1,3}", value):
        raise RumourParseError("invalid_age")
    return int(value)


def _position(cell: Tag) -> str | None:
    node = cell.select_one(".position")
    if node is None:
        inline = cell.select_one("table.inline-table")
        rows = inline.find_all("tr") if inline is not None else []
        node = rows[1] if len(rows) == 2 else None
    value = _text(node)
    return value if value not in _NULLS else None


def _flags(cell: Tag | None) -> tuple[str, ...]:
    if cell is None:
        return ()
    values = []
    for img in cell.select("img"):
        value = str(img.get("title") or img.get("alt") or "").strip()
        if not value:
            raise RumourParseError("invalid_nationality")
        if value not in values:
            values.append(value)
    if not values and _text(cell) not in _NULLS:
        raise RumourParseError("invalid_nationality")
    return tuple(values)


def _thread(cell: Tag | None) -> str | None:
    if cell is None:
        return None
    links = [_url(a["href"]) for a in cell.select("a[href]")]
    return links[0] if links else None


@dataclass(frozen=True)
class RumourObservation:
    entity_type: Literal["transfer_rumour_assertion"]
    parser_version: str
    schema_version: str
    competition: CompetitionContext
    player: SourceEntity
    subject_id: str
    position: str | None
    nationalities: tuple[str, ...]
    age: int | None
    current_club: SourceEntity | None
    interested_club: SourceEntity
    interested_club_competition: SourceEntity | None
    last_source_date: str | None
    last_reply_date: str | None
    probability_percent: float | None
    thread_link: str | None
    rumour_identity: str
    observed_at: str
    content_hash: str
    correction_identity: str
    observation_key: str

    def to_dict(self) -> dict:
        return json.loads(_json(asdict(self)))


@dataclass(frozen=True)
class RumourCapture:
    entity_type: Literal["transfer_rumour_assertion"]
    parser_version: str
    schema_version: str
    competition: CompetitionContext
    observed_at: str
    status: Literal["observed_present", "observed_empty"]
    observations: tuple[RumourObservation, ...]
    duplicate_count: int

    def to_dict(self) -> dict:
        return json.loads(_json(asdict(self)))

    def to_json(self) -> str:
        return _json(self.to_dict())


def _rows(html: str) -> list[dict[str, Tag]]:
    if not isinstance(html, str) or not html.strip():
        raise RumourParseError("blank_html")
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table.items")
    if not tables:
        raise RumourParseError("missing_table")
    if len(tables) != 1:
        raise RumourParseError("ambiguous_table")
    table = tables[0]
    if any(isinstance(c, Tag) and c.name not in {"thead", "tbody", "caption", "colgroup"}
           for c in table.children):
        raise RumourParseError("invalid_table_structure")
    if any(str(c).strip() for c in table.children if not isinstance(c, Tag)):
        raise RumourParseError("invalid_table_structure")
    heads = table.find_all("thead", recursive=False)
    bodies = table.find_all("tbody", recursive=False)
    if len(heads) != 1 or len(bodies) != 1:
        raise RumourParseError("invalid_table_structure")
    header_rows = heads[0].find_all("tr", recursive=False)
    if (len(header_rows) != 1 or
            any(isinstance(c, Tag) and c.name != "tr" for c in heads[0].children) or
            any(str(c).strip() for c in heads[0].children if not isinstance(c, Tag))):
        raise RumourParseError("invalid_headers")
    if (any(isinstance(c, Tag) and c.name not in {"th", "td"} for c in header_rows[0].children) or
            any(str(c).strip() for c in header_rows[0].children if not isinstance(c, Tag))):
        raise RumourParseError("invalid_headers")
    header_cells = header_rows[0].find_all(["th", "td"], recursive=False)
    names = [_text(c) for c in header_cells]
    if any(n not in _HEADERS for n in names):
        raise RumourParseError("unknown_header")
    headers = [_HEADERS[n] for n in names]
    if len(headers) != len(set(headers)):
        raise RumourParseError("duplicate_header")
    if not {"player", "interested_club"}.issubset(headers):
        raise RumourParseError("missing_header")
    if any(c.get("colspan", "1") != "1" or c.get("rowspan", "1") != "1" for c in header_cells):
        raise RumourParseError("invalid_headers")
    # Reject unaccounted outer rows/cells instead of accidentally emitting empty.
    if any(r.parent not in (heads[0], bodies[0])
           for r in table.find_all("tr") if r.find_parent("table") is table):
        raise RumourParseError("invalid_table_structure")
    body = bodies[0]
    if any(isinstance(c, Tag) and c.name != "tr" for c in body.children):
        raise RumourParseError("invalid_table_structure")
    if any(str(c).strip() for c in body.children if not isinstance(c, Tag)):
        raise RumourParseError("invalid_table_structure")
    rows = body.find_all("tr", recursive=False)
    output = []
    for index, row in enumerate(rows, 1):
        cells = row.find_all("td", recursive=False)
        empty_cells = row.select(".no-entries")
        if empty_cells or "no-entries" in row.get("class", []):
            valid = (len(rows) == 1 and len(cells) == 1 and
                     not any(isinstance(c, Tag) and c.name != "td" for c in row.children) and
                     not any(str(c).strip() for c in row.children if not isinstance(c, Tag)) and
                     "no-entries" in cells[0].get("class", []) and
                     cells[0].get("colspan") == str(len(headers)) and
                     cells[0].get("rowspan", "1") == "1" and
                     _text(cells[0]) in {"No entries available", "No entries available."} and
                     not cells[0].find(["a", "table", "img"]))
            if not valid:
                raise RumourParseError("malformed_empty_row", row=index)
            return []
        if (len(cells) != len(headers) or
                any(c.get("colspan", "1") != "1" or c.get("rowspan", "1") != "1" for c in cells) or
                any(isinstance(c, Tag) and c.name != "td" for c in row.children) or
                any(str(c).strip() for c in row.children if not isinstance(c, Tag))):
            raise RumourParseError("invalid_row_structure", row=index)
        output.append(dict(zip(headers, cells)))
    return output


def parse_rumours(
    html: str, *, observed_at: str, competition: CompetitionContext
) -> RumourCapture:
    """Parse one supplied capture atomically; pagination/lifecycle are out of scope.

    UTC timestamps always have six fractional digits. A correction has the same
    assertion and clock but a different content hash/key; no old object is mutated.
    """
    clock = _clock(observed_at)
    context = _competition(competition)
    observations = {}
    duplicates = 0
    for index, row in enumerate(_rows(html), 1):
        try:
            player = _entity(row["player"], "spieler", required=True)
            interested = _entity(row["interested_club"], "verein", required=True)
            assert player is not None and interested is not None
            source_link, reply_link = _thread(row.get("source")), _thread(row.get("reply"))
            thread = source_link or reply_link
            fields = {
                "entity_type": ENTITY_TYPE, "competition": asdict(context),
                "player": asdict(player), "subject_id": f"transfermarkt:player:{player.id}",
                "position": _position(row["player"]),
                "nationalities": _flags(row.get("nationalities")),
                "age": _age(row.get("age")),
                "current_club": None, "interested_club": asdict(interested),
                "interested_club_competition": None,
                "last_source_date": _date(row.get("source")),
                "last_reply_date": _date(row.get("reply")),
                "probability_percent": _probability(row.get("probability")),
                "thread_link": thread,
            }
            current = _entity(row.get("current_club"), "verein")
            interested_comp = _interested_competition(row["interested_club"])
            fields["current_club"] = asdict(current) if current else None
            fields["interested_club_competition"] = asdict(interested_comp) if interested_comp else None
            content_hash = _hash(fields)
            identity = f"thread:{thread}" if thread else f"surrogate:sha256:{content_hash}"
            correction = f"sha256:{content_hash}"
            key_values = (identity, fields["subject_id"], clock, correction)
            key = _hash(dict(zip(OBSERVATION_KEY_FIELDS, key_values)))
            observation = RumourObservation(
                **{**fields, "competition": context, "player": player,
                   "current_club": current, "interested_club": interested,
                   "interested_club_competition": interested_comp},
                parser_version=PARSER_VERSION, schema_version=SCHEMA_VERSION,
                rumour_identity=identity, observed_at=clock, content_hash=content_hash,
                correction_identity=correction, observation_key=key,
            )
            assertion_key = (identity, observation.subject_id)
            previous = observations.get(assertion_key)
            if previous is not None:
                if previous != observation:
                    raise RumourParseError("conflicting_duplicate")
                duplicates += 1
            else:
                observations[assertion_key] = observation
        except RumourParseError as exc:
            raise RumourParseError(exc.reason, row=index) from exc
    ordered = tuple(observations[k] for k in sorted(observations))
    return RumourCapture(ENTITY_TYPE, PARSER_VERSION, SCHEMA_VERSION, context,
                         clock, "observed_present" if ordered else "observed_empty", ordered, duplicates)
