"""Lossless, transport-free representation of one FBref HTML page.

The typed FBref parsers intentionally expose a curated dataframe contract.
This module sits underneath them and records every source table before a
typed adapter gets a chance to discard or rename anything.  It never performs
I/O and is therefore safe for parser-version replay from durable raw storage.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse

from bs4 import BeautifulSoup, Comment, Tag


# v6 (#1331): logical table/row/dataset identities are independent of the raw
# snapshot and DOM/comment order. The physical table instance retains both.
# v5 (#1331): tfoot rows are preserved as totals with an explicit is_total flag.
# v4 (#949): the v3 label was reused across c75879b, which began emitting an
# absence reason on empty/restricted/not_applicable page tables and on the
# __page__ manifest.  Bumping the version lets a re-parse lay down fresh
# reason-carrying manifests instead of colliding with the reason-less v3 rows
# written by the pre-c75879b production parser (whose evidence stands under the
# same v3 label and cannot be overwritten).
from scrapers.fbref.policy import PAGE_DOCUMENT_VERSION  # canonical definition
# v2 (#1331): generic cells carry an explicit is_total flag for tfoot rows.
BRONZE_TABLE_CONTRACT_VERSION = "fbref-bronze-contract-v2"

# High-confidence source containers only. Page kinds whose table inventory is
# legitimately heterogeneous still use the universal material-table gate
# below; a newly published table is preserved as UNKNOWN and does not fail.
_REQUIRED_TABLE_PREFIXES: Mapping[str, Tuple[str, ...]] = {
    "competition": ("seasons",),
    "schedule": ("sched",),
}
_ZERO_TABLE_SEMANTIC_PAGE_KINDS = frozenset({"competition", "season"})


class Availability(str, Enum):
    """Source availability vocabulary shared by every FBref page kind."""

    AVAILABLE = "available"
    EMPTY = "empty"
    RESTRICTED = "restricted"
    NOT_APPLICABLE = "not_applicable"
    DUPLICATE = "duplicate"
    LAYOUT_ONLY = "layout_only"
    UNKNOWN = "unknown"
    ERROR = "error"


_ENTITY_PATHS = {
    "players": "player_id",
    "squads": "squad_id",
    "matches": "match_id",
    "comps": "competition_id",
    "countries": "country_id",
}
_KNOWN_TABLE_PREFIXES = (
    "seasons",
    "sched",
    "standings",
    "stats_",
    "keeper_",
    "roster",
    "lineup",
    "shots",
    "team_stats",
    "player_stats",
)
_RESTRICTED_RE = re.compile(
    r"(?:not available|restricted|limited competitions|data (?:is|are) unavailable)",
    re.IGNORECASE,
)
_PLAYER_ID_RE = re.compile(r"^[0-9a-f]{8}$", re.IGNORECASE)
_PLAYER_LINK_RE = re.compile(
    r"^(?:https://(?:www\.)?fbref\.com)?/en/players/[0-9a-f]{8}/", re.IGNORECASE
)
_COMPETITION_HISTORY_LINK_RE = re.compile(
    r"^(?:https://(?:www\.)?fbref\.com)?/en/comps/[0-9]+/history/", re.IGNORECASE
)


def _sha256(*parts: object) -> str:
    payload = "\0".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _text(tag: Optional[Tag]) -> str:
    return "" if tag is None else tag.get_text(" ", strip=True)


def _matches_player_profile_url(value: object, player_id: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    hostname = str(parsed.hostname or "").casefold()
    if parsed.scheme.casefold() != "https" or hostname not in {
        "fbref.com",
        "www.fbref.com",
    }:
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return (
        len(parts) == 4
        and parts[0].casefold() == "en"
        and parts[1].casefold() == "players"
        and parts[2].casefold() == player_id.casefold()
        and bool(parts[3].strip())
    )


def _has_verified_zero_table_player_profile(
    html: str,
    *,
    target_id: str,
    source_ids: Optional[Mapping[str, object]],
) -> bool:
    """Accept a real tableless profile without accepting arbitrary shells."""

    player_id = str((source_ids or {}).get("player_id") or "").strip()
    if (
        not _PLAYER_ID_RE.fullmatch(player_id)
        or target_id != f"fbref:player:{player_id}"
    ):
        return False

    soup = BeautifulSoup(html or "", "html.parser")
    canonical_matches = any(
        "canonical"
        in {
            str(item).strip().casefold()
            for item in (
                link.get("rel")
                if isinstance(link.get("rel"), (list, tuple))
                else str(link.get("rel") or "").split()
            )
        }
        and _matches_player_profile_url(link.get("href"), player_id)
        for link in soup.find_all("link", href=True)
    )
    og_url_matches = any(
        str(tag.get("property") or "").strip().casefold() == "og:url"
        and _matches_player_profile_url(tag.get("content"), player_id)
        for tag in soup.find_all("meta")
    )
    og_type_is_athlete = any(
        str(tag.get("property") or "").strip().casefold() == "og:type"
        and str(tag.get("content") or "").strip().casefold() == "athlete"
        for tag in soup.find_all("meta")
    )
    profile_meta = soup.find(id="meta")
    heading = (
        profile_meta.find("h1") if isinstance(profile_meta, Tag) else None
    )
    return (
        canonical_matches
        and og_url_matches
        and og_type_is_athlete
        and isinstance(heading, Tag)
        and bool(_text(heading))
    )


def _matches_target_url(value: object, expected_url: str) -> bool:
    """Compare a source-advertised URL with the target's own canonical URL."""

    expected = urlparse(str(expected_url or "").strip())
    expected_path = expected.path.rstrip("/").casefold()
    if not expected_path:
        return False
    parsed = urlparse(str(value or "").strip())
    return (
        parsed.scheme.casefold() == "https"
        and str(parsed.hostname or "").casefold()
        in {"fbref.com", "www.fbref.com"}
        and parsed.path.rstrip("/").casefold() == expected_path
    )


def response_owns_target_page(html: str, *, canonical_url: str) -> bool:
    """Prove a table-free response is the target's own page, not a shell.

    A page with no tables and a generic 200 error shell have the same coarse
    shape, so a caller that wants to retire a target on "no tables" must first
    demand positive evidence the source itself supplies: the page advertises
    *this* target's canonical address in both its ``link rel=canonical`` and its
    ``og:url``, and it carries the source's own header block.  A proxy or error
    page cannot produce that triple without being the requested page.

    This is the page-kind-agnostic sibling of
    ``_has_verified_zero_table_player_profile``; it deliberately omits
    ``og:type``, which differs per page kind (``athlete`` for players,
    ``Organization`` for squads).
    """

    if not canonical_url:
        return False
    soup = BeautifulSoup(html or "", "html.parser")
    canonical_matches = any(
        "canonical"
        in {
            str(item).strip().casefold()
            for item in (
                link.get("rel")
                if isinstance(link.get("rel"), (list, tuple))
                else str(link.get("rel") or "").split()
            )
        }
        and _matches_target_url(link.get("href"), canonical_url)
        for link in soup.find_all("link", href=True)
    )
    og_url_matches = any(
        str(tag.get("property") or "").strip().casefold() == "og:url"
        and _matches_target_url(tag.get("content"), canonical_url)
        for tag in soup.find_all("meta")
    )
    page_meta = soup.find(id="meta")
    heading = page_meta.find("h1") if isinstance(page_meta, Tag) else None
    return (
        canonical_matches
        and og_url_matches
        and isinstance(heading, Tag)
        and bool(_text(heading))
    )


def _has_verified_zero_table_season_stats_page(
    html: str,
    *,
    canonical_url: Optional[str],
) -> bool:
    """Accept a real tableless season-stats page without accepting a shell.

    FBref publishes a season stat route (keepers, misc, playingtime, ...) for a
    competition edition that has no rows yet: a full 200 response whose body is
    the page header and nothing else.  The generic contract cannot tell that
    from a proxy/error shell by shape alone, so the page has to prove it is this
    target's own address -- the same positive evidence the archive branch of
    ``_historical_contract_rejection`` demands.

    The typed season gate (``typed_bronze.parse_season_stats_html``) accepts the
    very same proof, so a page that clears this gate cannot fall over on
    ``TypedBronzeError`` one step later.
    """

    return response_owns_target_page(html, canonical_url=canonical_url or "")


def _entity_ids(tag: Tag) -> Dict[str, str]:
    """Extract stable source IDs without interpreting display text."""

    found: Dict[str, str] = {}
    for anchor in tag.find_all("a", href=True):
        path = urlparse(str(anchor.get("href") or "")).path
        parts = [part for part in path.split("/") if part]
        if "en" in parts:
            parts = parts[parts.index("en") + 1 :]
        if len(parts) < 2:
            continue
        kind, source_id = parts[0], parts[1]
        field_name = _ENTITY_PATHS.get(kind)
        if field_name and source_id:
            found.setdefault(field_name, source_id)
    return found


def _span(cell: Tag, name: str) -> int:
    """Read a colspan/rowspan that FBref does not always emit as a number.

    Its player pages ship ``colspan=""`` on every wages row header, and one cell
    per page comes out of the markup as ``colspan='class="'`` — an unquoted
    attribute the parser folds into the span. Trusting the value crashed the
    generic layer and discarded the whole page, which is the one thing a
    lossless capture must never do: a broken span is a rendering hint, not data.
    """
    raw = str(cell.get(name) or "").strip()
    return max(1, int(raw)) if raw.isdigit() else 1


def _expand_header_grid(header_rows: Sequence[Tag]) -> List[List[str]]:
    """Expand rowspan/colspan headers into a rectangular text grid."""

    grid: List[List[str]] = []
    occupied: Dict[Tuple[int, int], str] = {}
    for row_index, row in enumerate(header_rows):
        rendered: List[str] = []
        column = 0
        for cell in row.find_all(["th", "td"], recursive=False):
            while (row_index, column) in occupied:
                rendered.append(occupied[(row_index, column)])
                column += 1
            value = _text(cell)
            colspan = _span(cell, "colspan")
            rowspan = _span(cell, "rowspan")
            for offset in range(colspan):
                rendered.append(value)
                for future_row in range(row_index + 1, row_index + rowspan):
                    occupied[(future_row, column + offset)] = value
            column += colspan
        while (row_index, column) in occupied:
            rendered.append(occupied[(row_index, column)])
            column += 1
        grid.append(rendered)
    return grid


def _header_path(grid: Sequence[Sequence[str]], column: int) -> Tuple[str, ...]:
    output: List[str] = []
    for row in grid:
        if column >= len(row):
            continue
        value = row[column].strip()
        if value and (not output or output[-1] != value):
            output.append(value)
    return tuple(output)


@dataclass(frozen=True)
class PageCell:
    cell_id: str
    row_id: str
    cell_index: int
    data_stat: Optional[str]
    raw_header_path: Tuple[str, ...]
    raw_value: str
    entity_ids: Mapping[str, str] = field(default_factory=dict)

    def to_record(self) -> dict:
        record = asdict(self)
        record["raw_header_path"] = json.dumps(
            list(self.raw_header_path), ensure_ascii=False
        )
        record["entity_ids"] = json.dumps(
            dict(self.entity_ids), ensure_ascii=False, sort_keys=True
        )
        return record


@dataclass(frozen=True)
class PageRow:
    row_id: str
    source_row_index: int
    entity_ids: Mapping[str, str]
    cells: Tuple[PageCell, ...]
    is_total: bool = False


@dataclass(frozen=True)
class PageTable:
    table_instance_id: str
    source_table_id: Optional[str]
    table_id: str
    source_location: str
    source_ordinal: int
    availability: Availability
    schema_signature: str
    content_signature: str
    duplicate_of: Optional[str]
    caption: Optional[str]
    header_paths: Tuple[Tuple[str, ...], ...]
    rows: Tuple[PageRow, ...]
    reason: Optional[str] = None
    manifest_suffix: str = ""

    @property
    def dataset(self) -> str:
        return f"table:{quote(self.table_id, safe='')}{self.manifest_suffix}"

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def inventory_record(self, page: "PageDocument") -> dict:
        return {
            "target_id": page.target_id,
            "page_kind": page.page_kind,
            "content_hash": page.content_hash,
            "parser_version": page.parser_version,
            "table_instance_id": self.table_instance_id,
            "source_table_id": self.source_table_id,
            "table_id": self.table_id,
            "source_location": self.source_location,
            "source_ordinal": self.source_ordinal,
            "availability": self.availability.value,
            "schema_signature": self.schema_signature,
            "content_signature": self.content_signature,
            "duplicate_of": self.duplicate_of,
            "caption": self.caption,
            "row_count": self.row_count,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PageDocument:
    target_id: str
    page_kind: str
    content_hash: str
    parser_version: str
    tables: Tuple[PageTable, ...]
    errors: Tuple[str, ...] = ()

    def inventory_records(self) -> List[dict]:
        return [table.inventory_record(self) for table in self.tables]

    def cell_records(self) -> List[dict]:
        records: List[dict] = []
        for table in self.tables:
            for row in table.rows:
                for cell in row.cells:
                    records.append({
                        "target_id": self.target_id,
                        "page_kind": self.page_kind,
                        "content_hash": self.content_hash,
                        "parser_version": self.parser_version,
                        "table_instance_id": table.table_instance_id,
                        "table_id": table.table_id,
                        "row_id": row.row_id,
                        "source_row_index": row.source_row_index,
                        "is_total": row.is_total,
                        **cell.to_record(),
                    })
        return records


def _documents(html: str) -> Iterable[Tuple[str, BeautifulSoup, Optional[Tag]]]:
    root = BeautifulSoup(html or "", "html.parser")
    yield "dom", root, None
    for index, comment in enumerate(
        root.find_all(string=lambda value: isinstance(value, Comment))
    ):
        markup = str(comment)
        if "<table" in markup.lower():
            yield f"comment:{index}", BeautifulSoup(markup, "html.parser"), comment.parent


def _table_rows(table: Tag) -> Tuple[List[List[str]], List[Tuple[Tag, bool]]]:
    thead = table.find("thead")
    if thead is not None:
        header_rows = thead.find_all("tr")
    else:
        header_rows = []
        for row in table.find_all("tr"):
            if row.find_parent("tfoot") is not None:
                continue
            direct = row.find_all(["th", "td"], recursive=False)
            if direct and not any(cell.name == "td" for cell in direct):
                header_rows.append(row)
            else:
                break
    grid = _expand_header_grid(header_rows)
    body = table.find("tbody")
    candidates: List[Tuple[Tag, bool]] = []
    if body is not None:
        sections = [
            section
            for section in table.find_all(["tbody", "tfoot"])
            if section is body or section.name == "tfoot"
        ]
        for section in sections:
            candidates.extend(
                (row, section.name == "tfoot")
                for row in section.find_all("tr", recursive=False)
            )
    else:
        for element in table.find_all(["tr", "tfoot"], recursive=False):
            if element.name == "tfoot":
                candidates.extend(
                    (row, True)
                    for row in element.find_all("tr", recursive=False)
                )
            else:
                candidates.append((element, False))
    header_identity = {id(row) for row in header_rows}
    data_rows = [
        (row, is_total)
        for row, is_total in candidates
        if id(row) not in header_identity
        and "thead" not in set(row.get("class") or [])
    ]
    return grid, data_rows


def _anonymous_table_id(
    page_kind: str,
    container: Optional[Tag],
    caption: Optional[str],
    schema_signature: str,
) -> str:
    # The first header of a match lineup contains the team and formation.
    # FBref identifies the two logical lineups by their enclosing divs.
    if page_kind == "match" and container is not None and container.name == "div":
        container_id = container.get("id")
        if container_id in ("a", "b") and "lineup" in (container.get("class") or ()):
            return f"lineup_{container_id}"
        if container_id == "team_stats":
            return "team_stats"
    if caption:
        return "caption_" + _sha256(" ".join(caption.casefold().split()))[:20]
    return "anon_" + schema_signature[:20]


def _is_known_table(
    table: Tag, table_id: str, data_stats: Tuple[str, ...], page_kind: str,
) -> bool:
    lowered = table_id.lower()
    if page_kind == "match" and not table.get("id"):
        if lowered in ("lineup_a", "lineup_b"):
            return table.find("a", href=_PLAYER_LINK_RE) is not None
        if lowered == "team_stats":
            return any(
                _text(heading).casefold() in {"possession", "shots on target", "saves", "cards"}
                for heading in table.find_all("th")
            )
    if page_kind == "competition_index" and lowered.startswith("comps_"):
        # Source index sections describe named competitions and their seasons.
        return {"league_name", "gender", "minseason", "maxseason"}.issubset(data_stats) and any(
            _COMPETITION_HISTORY_LINK_RE.match(str(link["href"]))
            for link in table.select('[data-stat="league_name"] a[href]')
        )
    if re.fullmatch(r"results[0-9-]+_(?:overall|home_away)", lowered):
        # These source standings include group and playoff variants. Demand
        # their identifying columns before classifying an unfamiliar layout.
        return "rank" in data_stats and "team" in data_stats and (
            "games" in data_stats or "home_games" in data_stats
        )
    return any(lowered == prefix or lowered.startswith(prefix) for prefix in _KNOWN_TABLE_PREFIXES)


def _with_manifest_suffix(table: PageTable, suffix: str) -> PageTable:
    # Rows in repeated logical tables can describe exactly the same team.
    # Apply the table discriminator to the rows and cells as well.
    rows = []
    for row in table.rows:
        row_id = _sha256(row.row_id, suffix)
        cells = tuple(
            replace(cell, row_id=row_id, cell_id=_sha256(row_id, cell.cell_index, cell.data_stat or ""))
            for cell in row.cells
        )
        rows.append(replace(row, row_id=row_id, cells=cells))
    return replace(table, rows=tuple(rows), manifest_suffix=suffix)


def _name_repeated_tables(tables: List[PageTable]) -> None:
    """Disambiguate repeated IDs once, even if recovery later writes a prefix."""
    by_id: dict[str, list[int]] = defaultdict(list)
    for index, table in enumerate(tables):
        by_id[table.table_id].append(index)

    for indices in by_id.values():
        if len(indices) == 1:
            continue
        counts: Counter[str] = Counter()
        # Different captions on the same source ID identify different tables
        # (e.g. Atlas group vs third-place ranking). If even the captions are
        # identical, retain every instance; sort by evidence, not DOM position.
        ordered = sorted(
            indices,
            key=lambda i: (
                tables[i].availability == Availability.DUPLICATE,
                tables[i].schema_signature,
                tables[i].content_signature,
            ),
        )
        for index in ordered:
            table = tables[index]
            caption = " ".join((table.caption or "").casefold().split())
            suffix = ":caption:" + _sha256(caption)[:20] if caption else ""
            counts[suffix] += 1
            if counts[suffix] > 1:
                suffix += f":instance:{counts[suffix]}"
            if suffix:
                tables[index] = _with_manifest_suffix(table, suffix)


def _parse_table(
    table: Tag,
    *,
    target_id: str,
    page_kind: str,
    content_hash: str,
    location: str,
    comment_parent: Optional[Tag],
    ordinal: int,
    seen_content: Mapping[Tuple[str, str], str],
) -> PageTable:
    header_grid, row_candidates = _table_rows(table)
    width = max((len(row) for row in header_grid), default=0)
    header_paths = tuple(_header_path(header_grid, column) for column in range(width))
    # Repeated body rows carry the same ``data-stat`` values.  Keeping only
    # first occurrence makes this a schema fingerprint instead of a row-count
    # fingerprint while preserving source column order.
    data_stats = tuple(dict.fromkeys(
        str(cell.get("data-stat") or "").strip()
        for cell in table.find_all(["th", "td"])
        if str(cell.get("data-stat") or "").strip()
    ))
    schema_signature = _sha256(header_paths, data_stats)
    source_table_id = str(table.get("id") or "").strip() or None
    caption = _text(table.find("caption", recursive=False)) or None
    container = table.parent
    if container is None or container.name == "[document]":
        container = comment_parent
    table_id = source_table_id or _anonymous_table_id(page_kind, container, caption, schema_signature)
    markup = re.sub(r"\s+", " ", str(table)).strip()
    content_signature = hashlib.sha256(markup.encode("utf-8")).hexdigest()
    table_instance_id = _sha256(
        target_id, content_hash, location, ordinal, table_id
    )

    rows: List[PageRow] = []
    identity_counts: Counter = Counter()
    for row_index, (row, is_total) in enumerate(row_candidates):
        direct_cells = row.find_all(["th", "td"], recursive=False)
        values = [_text(cell) for cell in direct_cells]
        if not direct_cells or not any(values):
            continue
        row_entities = _entity_ids(row)
        # Use native row IDs or linked entities where possible. Season headers
        # distinguish successive years for one club in player and wage tables.
        source_row_id = str(row.get("id") or "").strip()
        if source_row_id:
            stable_identity = {"source_row_id": source_row_id}
        elif row_entities:
            stable_identity = dict(row_entities)
            for cell, value in zip(direct_cells, values):
                if cell.name == "th" and cell.get("data-stat") in ("season", "year", "year_id"):
                    stable_identity["source_season"] = value
                    break
        else:
            stable_identity = {"values": values}
        # Body IDs stay independent of totals even when tfoot precedes tbody.
        if is_total:
            stable_identity = {**stable_identity, "source_section": "tfoot"}
        identity = json.dumps(stable_identity, sort_keys=True)
        # Even source keys can repeat. Keep each occurrence addressable.
        occurrence = identity_counts[identity]
        identity_counts[identity] += 1
        row_id = _sha256(target_id, table_id, identity, occurrence)
        cells: List[PageCell] = []
        for cell_index, cell in enumerate(direct_cells):
            data_stat = str(cell.get("data-stat") or "").strip() or None
            cell_entities = _entity_ids(cell)
            cells.append(PageCell(
                cell_id=_sha256(row_id, cell_index, data_stat or ""),
                row_id=row_id,
                cell_index=cell_index,
                data_stat=data_stat,
                raw_header_path=_header_path(header_grid, cell_index),
                raw_value=values[cell_index],
                entity_ids=cell_entities,
            ))
        rows.append(PageRow(
            row_id=row_id,
            source_row_index=row_index,
            entity_ids=row_entities,
            cells=tuple(cells),
            is_total=is_total,
        ))

    duplicate_of = seen_content.get((table_id, content_signature))
    semantic = bool(data_stats or any(header_paths))
    parent = table.parent if isinstance(table.parent, Tag) else None
    restriction_scope = (
        parent
        if parent is not None and parent.name not in {"body", "html", "[document]"}
        else table
    )
    restricted = bool(_RESTRICTED_RE.search(_text(restriction_scope)))
    if duplicate_of:
        availability = Availability.DUPLICATE
        reason = "identical_table_already_inventoried"
    elif restricted and not rows:
        availability = Availability.RESTRICTED
        reason = "source_restriction_marker"
    elif not semantic:
        availability = Availability.LAYOUT_ONLY
        reason = "no_semantic_headers_or_data_stat"
    elif not rows:
        availability = Availability.EMPTY
        reason = "semantic_table_has_no_rows"
    elif _is_known_table(table, table_id, data_stats, page_kind):
        availability = Availability.AVAILABLE
        reason = None
    else:
        availability = Availability.UNKNOWN
        reason = "unclassified_source_table"

    return PageTable(
        table_instance_id=table_instance_id,
        source_table_id=source_table_id,
        table_id=table_id,
        source_location=location,
        source_ordinal=ordinal,
        availability=availability,
        schema_signature=schema_signature,
        content_signature=content_signature,
        duplicate_of=duplicate_of,
        caption=caption,
        header_paths=header_paths,
        rows=tuple(rows),
        reason=reason,
    )


def parse_page_document(
    html: str,
    *,
    target_id: str,
    page_kind: str,
    source_ids: Optional[Mapping[str, object]] = None,
    content_hash: Optional[str] = None,
    canonical_url: Optional[str] = None,
    parser_version: str = PAGE_DOCUMENT_VERSION,
) -> PageDocument:
    """Inventory every source table without transport or persistence access."""

    if not target_id or not page_kind:
        raise ValueError("target_id and page_kind are required")
    raw_hash = content_hash or hashlib.sha256((html or "").encode("utf-8")).hexdigest()
    tables: List[PageTable] = []
    errors: List[str] = []
    seen_content: Dict[Tuple[str, str], str] = {}
    ordinal = 0
    for location, document, comment_parent in _documents(html):
        for table in document.find_all("table"):
            try:
                parsed = _parse_table(
                    table,
                    target_id=target_id,
                    page_kind=page_kind,
                    content_hash=raw_hash,
                    location=location,
                    comment_parent=comment_parent,
                    ordinal=ordinal,
                    seen_content=seen_content,
                )
            except Exception as exc:  # one malformed table must not hide others
                errors.append(
                    f"{location}[{ordinal}]:{type(exc).__name__}:{str(exc)[:500]}"
                )
                ordinal += 1
                continue
            tables.append(parsed)
            if parsed.availability != Availability.DUPLICATE:
                seen_content[(parsed.table_id, parsed.content_signature)] = parsed.table_instance_id
            ordinal += 1

    _name_repeated_tables(tables)

    # Generic Bronze inventories source structure; page-kind parsers own its
    # meaning. Competition card grids, single-match season pages and a stat
    # route of an edition with no rows yet are valid zero-table shapes, while
    # an empty semantic table is still evidence that must be persisted with
    # Availability.EMPTY (not rejected pre-semantics).
    if not tables:
        valid_zero_table_page = (
            page_kind in _ZERO_TABLE_SEMANTIC_PAGE_KINDS
            or page_kind == "player"
            and _has_verified_zero_table_player_profile(
                html,
                target_id=target_id,
                source_ids=source_ids,
            )
            or page_kind == "season_stats"
            and _has_verified_zero_table_season_stats_page(
                html,
                canonical_url=canonical_url,
            )
        )
        if not valid_zero_table_page:
            errors.append("page_contract:no_tables")
    elif tables:
        required_prefixes = _REQUIRED_TABLE_PREFIXES.get(page_kind, ())
        material_ids = {
            table.table_id.casefold()
            for table in tables
            if table.availability
            not in {Availability.DUPLICATE, Availability.LAYOUT_ONLY}
        }
        if required_prefixes and not any(
            table_id.startswith(prefix)
            for prefix in required_prefixes
            for table_id in material_ids
        ):
            errors.append(
                "page_contract:required_table_missing:"
                + "|".join(required_prefixes)
            )
    return PageDocument(
        target_id=target_id,
        page_kind=page_kind,
        content_hash=raw_hash,
        parser_version=parser_version,
        tables=tuple(tables),
        errors=tuple(errors),
    )


__all__ = [
    "Availability",
    "BRONZE_TABLE_CONTRACT_VERSION",
    "PAGE_DOCUMENT_VERSION",
    "PageCell",
    "PageDocument",
    "PageRow",
    "PageTable",
    "parse_page_document",
    "response_owns_target_page",
]
