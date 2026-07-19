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
from collections import Counter
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Tag


# v4 (#949): the v3 label was reused across c75879b, which began emitting an
# absence reason on empty/restricted/not_applicable page tables and on the
# __page__ manifest.  Bumping the version lets a re-parse lay down fresh
# reason-carrying manifests instead of colliding with the reason-less v3 rows
# written by the pre-c75879b production parser (whose evidence stands under the
# same v3 label and cannot be overwritten).
PAGE_DOCUMENT_VERSION = "fbref-page-document-v4"
BRONZE_TABLE_CONTRACT_VERSION = "fbref-bronze-contract-v1"

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
                        **cell.to_record(),
                    })
        return records


def _documents(html: str) -> Iterable[Tuple[str, BeautifulSoup]]:
    root = BeautifulSoup(html or "", "html.parser")
    yield "dom", root
    for index, comment in enumerate(
        root.find_all(string=lambda value: isinstance(value, Comment))
    ):
        markup = str(comment)
        if "<table" in markup.lower():
            yield f"comment:{index}", BeautifulSoup(markup, "html.parser")


def _table_rows(table: Tag) -> Tuple[List[List[str]], List[Tag]]:
    thead = table.find("thead")
    if thead is not None:
        header_rows = thead.find_all("tr")
    else:
        header_rows = []
        for row in table.find_all("tr"):
            direct = row.find_all(["th", "td"], recursive=False)
            if direct and not any(cell.name == "td" for cell in direct):
                header_rows.append(row)
            else:
                break
    grid = _expand_header_grid(header_rows)
    body = table.find("tbody")
    candidates = (body or table).find_all("tr", recursive=False)
    header_identity = {id(row) for row in header_rows}
    data_rows = [
        row for row in candidates
        if id(row) not in header_identity
        and "thead" not in set(row.get("class") or [])
    ]
    return grid, data_rows


def _is_known_table(table_id: str) -> bool:
    lowered = table_id.lower()
    return any(lowered == prefix or lowered.startswith(prefix) for prefix in _KNOWN_TABLE_PREFIXES)


def _parse_table(
    table: Tag,
    *,
    target_id: str,
    content_hash: str,
    location: str,
    ordinal: int,
    seen_content: Mapping[str, str],
) -> PageTable:
    header_grid, body_rows = _table_rows(table)
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
    table_id = source_table_id or f"anon_{ordinal}_{schema_signature[:12]}"
    markup = re.sub(r"\s+", " ", str(table)).strip()
    content_signature = hashlib.sha256(markup.encode("utf-8")).hexdigest()
    table_instance_id = _sha256(
        target_id, content_hash, location, ordinal, table_id
    )
    caption = _text(table.find("caption")) or None

    rows: List[PageRow] = []
    identity_counts: Counter = Counter()
    for row_index, row in enumerate(body_rows):
        direct_cells = row.find_all(["th", "td"], recursive=False)
        values = [_text(cell) for cell in direct_cells]
        if not direct_cells or not any(values):
            continue
        row_entities = _entity_ids(row)
        stable_identity = row_entities or {
            "values": "\x1f".join(values),
            "ordinal": str(row_index),
        }
        identity = json.dumps(stable_identity, sort_keys=True)
        # FBref repeats the same entities across rows of one table: a player's
        # standard stats carry two rows for the same club, his wages table one
        # row per season for the same club. Entity identity alone is therefore
        # not a key — the colliding row_ids reached Iceberg as duplicate MERGE
        # source rows (MERGE_TARGET_ROW_MULTIPLE_MATCHES) and the page could not
        # be written at all. The first occurrence keeps its id, so rows already
        # in Bronze stay addressable; later ones carry their occurrence.
        occurrence = identity_counts[identity]
        identity_counts[identity] += 1
        row_id = _sha256(table_instance_id, identity) if occurrence == 0 else (
            _sha256(table_instance_id, identity, occurrence)
        )
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
        ))

    duplicate_of = seen_content.get(content_signature)
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
    elif _is_known_table(table_id):
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
    parser_version: str = PAGE_DOCUMENT_VERSION,
) -> PageDocument:
    """Inventory every source table without transport or persistence access."""

    if not target_id or not page_kind:
        raise ValueError("target_id and page_kind are required")
    raw_hash = content_hash or hashlib.sha256((html or "").encode("utf-8")).hexdigest()
    tables: List[PageTable] = []
    errors: List[str] = []
    seen_content: Dict[str, str] = {}
    ordinal = 0
    for location, document in _documents(html):
        for table in document.find_all("table"):
            try:
                parsed = _parse_table(
                    table,
                    target_id=target_id,
                    content_hash=raw_hash,
                    location=location,
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
                seen_content[parsed.content_signature] = parsed.table_instance_id
            ordinal += 1

    # Generic Bronze inventories source structure; page-kind parsers own its
    # meaning. Competition card grids and single-match season pages are valid
    # zero-table shapes, while an empty semantic table is still evidence that
    # must be persisted with Availability.EMPTY (not rejected pre-semantics).
    if not tables:
        valid_zero_table_page = (
            page_kind in _ZERO_TABLE_SEMANTIC_PAGE_KINDS
            or page_kind == "player"
            and _has_verified_zero_table_player_profile(
                html,
                target_id=target_id,
                source_ids=source_ids,
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
]
