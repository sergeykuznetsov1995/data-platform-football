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
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Tag


PAGE_DOCUMENT_VERSION = "fbref-page-document-v1"


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


def _sha256(*parts: object) -> str:
    payload = "\0".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _text(tag: Optional[Tag]) -> str:
    return "" if tag is None else tag.get_text(" ", strip=True)


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
            colspan = max(1, int(cell.get("colspan") or 1))
            rowspan = max(1, int(cell.get("rowspan") or 1))
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
        row_id = _sha256(table_instance_id, json.dumps(stable_identity, sort_keys=True))
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
    "PAGE_DOCUMENT_VERSION",
    "PageCell",
    "PageDocument",
    "PageRow",
    "PageTable",
    "parse_page_document",
]
