"""Offline published attendance aggregates from saved Transfermarkt pages."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Iterable
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

ATTENDANCE_PARSER_REVISION = 'attendance-v2'
ATTENDANCE_SCHEMA_REVISION = '1'
ATTENDANCE_TABLE = 'iceberg.bronze.transfermarkt_attendance_observations'
ATTENDANCE_KEY = ('competition_id', 'edition_id', 'club_id', 'observed_at')
ATTENDANCE_STATUSES = frozenset({
    'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
    'unknown', 'unavailable', 'error',
})
ATTENDANCE_COLUMNS = (
    'competition_id', 'edition_id', 'club_id', 'observed_at',
    'club_name', 'stadium_name', 'stadium_url',
    'capacity', 'capacity_raw', 'spectators_total', 'spectators_total_raw',
    'spectators_average', 'spectators_average_raw', 'matches', 'matches_raw',
    'sold_out', 'sold_out_raw', 'capacity_filled_raw',
    'observation_status', 'status_reason', 'final_season', 'final_season_proof',
    'source_url', 'source_capture_id', 'source_body_hash', 'captured_at',
    'extracted_at', 'parser_revision', 'schema_revision', 'scope_id', 'run_id',
    'universe_version', '_source', '_entity_type', '_batch_id', '_ingested_at',
)
_LINEAGE_FIELDS = (
    'source_url', 'source_capture_id', 'source_body_hash', 'captured_at',
    'extracted_at', 'parser_revision', 'schema_revision', 'scope_id', 'run_id',
    'universe_version', '_batch_id', '_ingested_at',
)
_FORBIDDEN_MATCH_FIELDS = frozenset({'match_id', 'match_date', 'matchday_id', 'fixture_id'})
_COMPETITION_RE = re.compile(r'[A-Za-z0-9_-]+')
_DIGITS_RE = re.compile(r'[0-9]+')
# The club cell links the club by its profile page or, since 2026-11-28, by its
# season schedule; both carry the same club id.
_CLUB_HREF_RE = re.compile(r'^/[^/]+/(?:startseite|spielplan)/verein/([1-9][0-9]*)(?:[/?#].*)?$')
_CLUB_PATHS = ('/startseite/verein/', '/spielplan/verein/')
_STADIUM_PATH = '/stadion/verein/'
_ATTENDANCE_PATH_RE = re.compile(
    r'^/[^/]+/besucherzahlen/wettbewerb/([A-Za-z0-9_-]+)/saison_id/([0-9]+)(?:/plus/[0-9]+)?/?$'
)
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_MISSING = frozenset({'', '-', '?', 'unknown', 'n/a'})
_HEADER_ALIASES = {
    '#': 'rank', 'rank': 'rank', 'stadium': 'stadium', 'stadion': 'stadium',
    'capacity': 'capacity', 'kapazität': 'capacity',
    'spectators': 'spectators_total', 'zuschauer': 'spectators_total',
    'average': 'spectators_average', 'schnitt': 'spectators_average',
    'matches': 'matches', 'spiele': 'matches',
    'sold out': 'sold_out', 'sold-out': 'sold_out', 'ausv.': 'sold_out',
    'capacity filled': 'capacity_filled', 'auslastung': 'capacity_filled',
}


class AttendanceParseError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class AttendancePage:
    url: str
    body: bytes
    capture_id: str
    body_hash: str
    captured_at: str
    status_code: int = 200

    def __post_init__(self):
        if not isinstance(self.url, str) or not self.url:
            raise AttendanceParseError('source_url_invalid')
        if not isinstance(self.body, bytes):
            raise AttendanceParseError('body_must_be_bytes')
        if not isinstance(self.capture_id, str) or not self.capture_id:
            raise AttendanceParseError('capture_id_invalid')
        if not isinstance(self.body_hash, str) or not _SHA256_RE.fullmatch(self.body_hash):
            raise AttendanceParseError('body_hash_invalid')
        if hashlib.sha256(self.body).hexdigest() != self.body_hash:
            raise AttendanceParseError('body_hash_mismatch')
        if isinstance(self.status_code, bool) or not isinstance(self.status_code, int) \
                or not 100 <= self.status_code <= 599:
            raise AttendanceParseError('http_status_invalid')
        canonical_timestamp(self.captured_at)


@dataclass(frozen=True)
class AttendanceRow:
    competition_id: str
    edition_id: str
    club_id: str
    observed_at: str
    club_name: str
    stadium_name: str | None
    stadium_url: str | None
    capacity: int | None
    capacity_raw: str
    spectators_total: int | None
    spectators_total_raw: str
    spectators_average: int | None
    spectators_average_raw: str
    matches: int | None
    matches_raw: str
    sold_out: int | None
    sold_out_raw: str
    capacity_filled_raw: str
    observation_status: str
    status_reason: str
    source_url: str
    source_capture_id: str
    source_body_hash: str
    captured_at: str

    @property
    def natural_key(self) -> tuple[str, ...]:
        return self.competition_id, self.edition_id, self.club_id, self.observed_at

    def semantic_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AttendanceResult:
    status: str
    rows: tuple[AttendanceRow, ...] = ()
    status_reason: str = 'ok'
    proof: str | None = None
    page_count: int = 0
    reconciled_duplicates: int = 0

    def __post_init__(self):
        if self.status not in ATTENDANCE_STATUSES:
            raise AttendanceParseError('status_invalid')
        if not isinstance(self.status_reason, str) or not self.status_reason:
            raise AttendanceParseError('status_reason_missing')
        if self.status in {'observed_empty', 'not_applicable'} and not self.proof:
            raise AttendanceParseError('proof_missing')
        if self.status not in {'observed_empty', 'not_applicable'} and self.proof is not None:
            raise AttendanceParseError('proof_unexpected')
        if self.status in {'observed_empty', 'not_applicable', 'unavailable', 'error'} and self.rows:
            raise AttendanceParseError('rows_unexpected_for_status')
        if self.status in {'observed_present', 'explicit_none'} and not self.rows:
            raise AttendanceParseError('rows_missing_for_present')
        if self.status == 'explicit_none' \
                and any(row.observation_status != 'explicit_none' for row in self.rows):
            raise AttendanceParseError('status_rows_mismatch')
        if self.status == 'observed_present' \
                and not any(row.observation_status == 'observed_present' for row in self.rows):
            raise AttendanceParseError('status_rows_mismatch')


@dataclass(frozen=True)
class AttendanceExtraction:
    result: AttendanceResult
    scope_id: str
    run_id: str
    universe_version: str
    extracted_at: str
    final_season: bool = False
    final_season_proof: str | None = None

    def __post_init__(self):
        for field in ('scope_id', 'run_id', 'universe_version'):
            value = getattr(self, field)
            if not isinstance(value, str) or not value:
                raise AttendanceParseError(f'{field}_missing')
        canonical_timestamp(self.extracted_at)
        if not isinstance(self.final_season, bool):
            raise AttendanceParseError('final_season_invalid')
        if self.final_season and not self.final_season_proof:
            raise AttendanceParseError('final_season_proof_missing')
        if not self.final_season and self.final_season_proof is not None:
            raise AttendanceParseError('final_season_proof_unexpected')

    @property
    def batch_id(self) -> str:
        digest = hashlib.sha256(_canonical_json({'run_id': self.run_id})).hexdigest()[:32]
        return f'tm-attendance-batch-{digest}'

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        rendered = []
        for source in sorted(self.result.rows, key=lambda row: row.natural_key):
            values = source.semantic_dict()
            values.update({
                'final_season': self.final_season,
                'final_season_proof': self.final_season_proof,
                'extracted_at': self.extracted_at,
                'parser_revision': ATTENDANCE_PARSER_REVISION,
                'schema_revision': ATTENDANCE_SCHEMA_REVISION,
                'scope_id': self.scope_id,
                'run_id': self.run_id,
                'universe_version': self.universe_version,
                '_source': 'transfermarkt',
                '_entity_type': 'attendance',
                '_batch_id': self.batch_id,
                '_ingested_at': self.extracted_at,
            })
            rendered.append({column: values[column] for column in ATTENDANCE_COLUMNS})
        return tuple(rendered)

    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json(self.rows)).hexdigest()


def canonical_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise AttendanceParseError('timestamp_invalid')
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as exc:
        raise AttendanceParseError('timestamp_invalid') from exc
    if moment.tzinfo is None or moment.utcoffset() is None or moment.isoformat() != value:
        raise AttendanceParseError('timestamp_invalid')
    return value


def canonical_club_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise AttendanceParseError('club_id_invalid')
    rendered = str(value)
    if not _DIGITS_RE.fullmatch(rendered) or int(rendered) <= 0:
        raise AttendanceParseError('club_id_invalid')
    return str(int(rendered))


def _competition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise AttendanceParseError('competition_id_invalid')
    rendered = str(value)
    if not _COMPETITION_RE.fullmatch(rendered):
        raise AttendanceParseError('competition_id_invalid')
    return rendered


def _edition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise AttendanceParseError('edition_id_invalid')
    rendered = str(value)
    if not _DIGITS_RE.fullmatch(rendered) or not 1800 <= int(rendered) <= 2199:
        raise AttendanceParseError('edition_id_invalid')
    return str(int(rendered))


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _canonical_url(value: str) -> str:
    parts = urlsplit(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ''))


def _validate_attendance_url(value: str, *, host: str, path: str,
                             competition: str, edition: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in {'http', 'https'} or parts.netloc != host \
            or parts.username is not None or parts.password is not None:
        raise AttendanceParseError('pagination_identity_invalid')
    match = _ATTENDANCE_PATH_RE.fullmatch(parts.path)
    if match is None or match.group(1) != competition or match.group(2) != edition \
            or parts.path.rstrip('/') != path.rstrip('/'):
        raise AttendanceParseError('pagination_identity_invalid')
    query = parse_qs(parts.query, keep_blank_values=True)
    if set(query) - {'page'}:
        raise AttendanceParseError('pagination_identity_invalid')
    if 'page' in query:
        page = query['page']
        if len(page) != 1 or not _DIGITS_RE.fullmatch(page[0]) or int(page[0]) <= 0:
            raise AttendanceParseError('pagination_identity_invalid')
    return _canonical_url(value)


def _integer(raw: str, field: str) -> int | None:
    normalized = ' '.join(raw.replace('\u00a0', ' ').split())
    if normalized.lower() in _MISSING:
        return None
    compact = normalized.replace(' ', '')
    if not re.fullmatch(r'[0-9.,]+', compact):
        raise AttendanceParseError(f'{field}_invalid')
    if compact.startswith(('.', ',')) or compact.endswith(('.', ',')) \
            or ('.' in compact and ',' in compact) or '..' in compact or ',,' in compact:
        raise AttendanceParseError(f'{field}_invalid')
    groups = re.split(r'[.,]', compact)
    if len(groups) > 1 and (len(groups[0]) > 3 or any(len(group) != 3 for group in groups[1:])):
        raise AttendanceParseError(f'{field}_invalid')
    value = int(''.join(groups))
    if value < 0:
        raise AttendanceParseError(f'{field}_invalid')
    return value


def _validate_capacity_filled(raw: str) -> None:
    normalized = ' '.join(raw.replace('\u00a0', ' ').split())
    if normalized.lower() in _MISSING:
        return
    if re.fullmatch(r'[0-9]+(?:[.,][0-9]+)?\s*%', normalized) is None:
        raise AttendanceParseError('capacity_filled_invalid')


def _headers(table) -> list[str]:
    thead = table.find('thead', recursive=False)
    tbody = table.find('tbody', recursive=False)
    if thead is None or tbody is None:
        raise AttendanceParseError('attendance_table_structure_changed')
    fields = []
    capacity_seen = False
    for th in thead.find_all('th'):
        header = ' '.join(th.get_text(' ', strip=True).lower().split())
        field = _HEADER_ALIASES.get(header)
        if field == 'capacity':
            if capacity_seen:
                field = 'capacity_filled'
            capacity_seen = True
        fields.append(field)
    required = {'stadium', 'capacity', 'spectators_total', 'spectators_average'}
    if not fields or None in fields or required - set(fields) \
            or len(set(fields)) != len(fields):
        raise AttendanceParseError('attendance_headers_changed')
    return fields


def _next_url(soup, current_url: str) -> str | None:
    links = soup.select('li.naechste-seite a[href], a[rel~=next][href]')
    targets = {urljoin(current_url, anchor['href']) for anchor in links}
    if len(targets) > 1:
        raise AttendanceParseError('pagination_next_ambiguous')
    return targets.pop() if targets else None


def next_attendance_url(body: bytes, current_url: str) -> str | None:
    try:
        html = body.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise AttendanceParseError('invalid_utf8') from exc
    return _next_url(BeautifulSoup(html, 'html.parser'), current_url)


def _row(tr, fields: list[str], *, competition: str, edition: str,
         page: AttendancePage, observed_at: str | None = None) -> AttendanceRow | None:
    cells = tr.find_all('td', recursive=False)
    if len(cells) != len(fields):
        raise AttendanceParseError('attendance_row_shape_changed')
    values = {field: cells[index] for index, field in enumerate(fields) if field != 'rank'}
    club_cell = values['stadium']
    club_links = []
    for anchor in club_cell.find_all('a', href=True):
        match = _CLUB_HREF_RE.match(anchor['href'])
        if match is not None:
            club_links.append((match.group(1), anchor))
        elif any(path in anchor['href'] for path in _CLUB_PATHS):
            raise AttendanceParseError('club_identity_invalid')
    if not club_links:
        summary = ' '.join(cells[0].get_text(' ', strip=True).lower().split())
        if summary in {'total', 'total:', 'insgesamt', 'insgesamt:'}:
            return None
        raise AttendanceParseError('club_identity_invalid')
    # The crest and the club name can both carry the club link.
    if (len({club_id for club_id, _ in club_links}) != 1
            or len({anchor['href'] for _, anchor in club_links}) != 1):
        raise AttendanceParseError('club_identity_invalid')
    club_id = club_links[0][0]
    club_name = next((name for name in (
        ' '.join(anchor.get_text(' ', strip=True).split()) for _, anchor in club_links) if name), '')
    inline = club_cell.find('table', class_='inline-table')
    detail_rows = inline.find_all('tr') if inline is not None else []
    detail_cells = [cell for row in detail_rows for cell in row.find_all('td', recursive=False)]
    stadium_cells = [cell for cell in detail_cells
                     if any(_STADIUM_PATH in anchor['href'] for anchor in cell.find_all('a', href=True))]
    # Two published layouts put the stadium above the club name: with a crest
    # beside the stadium, or without one.  The stadium row names it by link.
    if not club_name or len(stadium_cells) > 1 or (
            not stadium_cells and (len(detail_rows) != 2 or len(detail_cells) != 2)):
        raise AttendanceParseError('attendance_club_layout_changed')
    stadium_cell = stadium_cells[0] if stadium_cells else detail_rows[0]
    stadium_name = stadium_cell.get_text(' ', strip=True)
    if stadium_name in _MISSING:
        stadium_name = None
    stadium_anchor = stadium_cell.find('a', href=True)
    stadium_url = urljoin(page.url, stadium_anchor['href']) if stadium_anchor is not None else None
    raw = {}
    parsed = {}
    for field in ('capacity', 'spectators_total', 'spectators_average', 'matches', 'sold_out'):
        cell = values.get(field)
        raw[field] = cell.get_text(' ', strip=True) if cell is not None else ''
        parsed[field] = _integer(raw[field], field)
    filled = values.get('capacity_filled')
    filled_raw = filled.get_text(' ', strip=True) if filled is not None else ''
    _validate_capacity_filled(filled_raw)
    present = any(parsed[field] is not None
                  for field in ('spectators_total', 'spectators_average'))
    if present:
        observation_status, status_reason = 'observed_present', 'published_values_present'
    else:
        attendance_raw = [raw['spectators_total'], raw['spectators_average']]
        normalized = [' '.join(value.lower().split()) for value in attendance_raw]
        if any(value in {'', '?', 'unknown'} for value in normalized):
            observation_status, status_reason = 'unknown', 'published_values_unknown'
        else:
            observation_status, status_reason = 'explicit_none', 'published_values_missing'
    return AttendanceRow(
        competition, edition, canonical_club_id(club_id), observed_at or page.captured_at,
        club_name, stadium_name, stadium_url,
        parsed['capacity'], raw['capacity'],
        parsed['spectators_total'], raw['spectators_total'],
        parsed['spectators_average'], raw['spectators_average'],
        parsed['matches'], raw['matches'], parsed['sold_out'], raw['sold_out'],
        filled_raw, observation_status, status_reason,
        _canonical_url(page.url), page.capture_id, page.body_hash, page.captured_at,
    )


def _parse_page(page: AttendancePage, *, competition: str,
                edition: str, observed_at: str | None = None) -> tuple[list[AttendanceRow], str | None]:
    try:
        html = page.body.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise AttendanceParseError('invalid_utf8') from exc
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.select('table.items')
    if len(tables) > 1:
        # The page can carry another grid, such as the league table, beside the
        # attendance grid.  Only the grid headed by the stadium column qualifies.
        tables = [table for table in tables if _is_attendance_grid(table)]
    if len(tables) != 1:
        raise AttendanceParseError('attendance_table_missing' if not tables
                                   else 'attendance_table_ambiguous')
    table = tables[0]
    fields = _headers(table)
    tbody = table.find('tbody', recursive=False)
    rows = [_row(tr, fields, competition=competition, edition=edition, page=page,
                 observed_at=observed_at)
            for tr in tbody.find_all('tr', recursive=False)]
    return [row for row in rows if row is not None], _next_url(soup, page.url)


def _is_attendance_grid(table) -> bool:
    """The attendance grid is the one headed by the stadium column."""
    thead = table.find('thead', recursive=False)
    if thead is None:
        return False
    heads = (' '.join(th.get_text(' ', strip=True).lower().split()) for th in thead.find_all('th'))
    return any(_HEADER_ALIASES.get(head) == 'stadium' for head in heads)


def parse_attendance(pages: Iterable[AttendancePage], *, start_url: str,
                     competition_id: Any, edition_id: Any,
                     observed_at: Any = None) -> AttendanceResult:
    competition = _competition_id(competition_id)
    edition = _edition_id(edition_id)
    observation = canonical_timestamp(observed_at) if observed_at is not None else None
    start_parts = urlsplit(start_url)
    start = _validate_attendance_url(
        start_url, host=start_parts.netloc, path=start_parts.path,
        competition=competition, edition=edition,
    )
    page_map = {}
    for page in pages:
        url = _canonical_url(page.url)
        if url in page_map:
            raise AttendanceParseError('duplicate_capture_url')
        page_map[url] = page
    if not page_map:
        raise AttendanceParseError('pages_missing')

    current = start
    visited = set()
    rows_by_club: dict[str, AttendanceRow] = {}
    reconciled = 0
    while current is not None:
        if current in visited:
            raise AttendanceParseError('pagination_cycle')
        if len(visited) >= 100:
            raise AttendanceParseError('pagination_limit')
        visited.add(current)
        _validate_attendance_url(
            current, host=start_parts.netloc, path=start_parts.path,
            competition=competition, edition=edition,
        )
        page = page_map.get(current)
        if page is None:
            raise AttendanceParseError('pagination_page_missing')
        if page.status_code != 200:
            return AttendanceResult('unavailable', status_reason=f'http_{page.status_code}',
                                    page_count=len(visited))
        page_rows, next_url = _parse_page(
            page, competition=competition, edition=edition, observed_at=observation)
        for row in page_rows:
            previous = rows_by_club.get(row.club_id)
            if previous is None:
                rows_by_club[row.club_id] = row
            elif previous.semantic_dict() == row.semantic_dict():
                reconciled += 1
            else:
                raise AttendanceParseError('attendance_club_conflict')
        current = next_url
    rows = tuple(rows_by_club.values())
    if not rows:
        return AttendanceResult('unknown', status_reason='empty_layout_unproven',
                                page_count=len(visited), reconciled_duplicates=reconciled)
    if any(row.observation_status == 'observed_present' for row in rows):
        status, reason = 'observed_present', 'ok'
    elif all(row.observation_status == 'explicit_none' for row in rows):
        status, reason = 'explicit_none', 'all_rows_explicit_none'
    else:
        status, reason = 'unknown', 'rows_unresolved'
    return AttendanceResult(status, rows, status_reason=reason, page_count=len(visited),
                            reconciled_duplicates=reconciled)


def extract_attendance(pages: Iterable[AttendancePage], *, start_url: str,
                       competition_id: Any, edition_id: Any, scope_id: str,
                       run_id: str, universe_version: str, extracted_at: str,
                       final_season: bool = False,
                       final_season_proof: str | None = None) -> AttendanceExtraction:
    try:
        result = parse_attendance(pages, start_url=start_url,
                                  competition_id=competition_id, edition_id=edition_id)
    except AttendanceParseError as exc:
        result = AttendanceResult('error', status_reason=exc.reason)
    return AttendanceExtraction(
        result, scope_id, run_id, universe_version, extracted_at,
        final_season, final_season_proof,
    )


def _status_result(status: str, reason: str, proof: str | None = None) -> AttendanceResult:
    if not isinstance(reason, str) or not reason:
        raise AttendanceParseError('status_reason_missing')
    return AttendanceResult(status, status_reason=reason, proof=proof)


def confirmed_empty_attendance(*, completeness_proof: str) -> AttendanceResult:
    return _status_result('observed_empty', 'explicit_empty_proof', completeness_proof)


def not_applicable_attendance(*, applicability_proof: str) -> AttendanceResult:
    return _status_result('not_applicable', 'explicit_applicability_proof', applicability_proof)


def unknown_attendance(reason: str) -> AttendanceResult:
    return _status_result('unknown', reason)


def unavailable_attendance(reason: str) -> AttendanceResult:
    return _status_result('unavailable', reason)


def error_attendance(reason: str) -> AttendanceResult:
    return _status_result('error', reason)


def build_attendance_dq(extraction: AttendanceExtraction,
                        expected_club_ids: Iterable[Any]) -> dict[str, Any]:
    expected = [canonical_club_id(value) for value in expected_club_ids]
    if len(set(expected)) != len(expected):
        raise AttendanceParseError('expected_clubs_duplicate')
    expected_set = set(expected)
    rows = extraction.rows
    result = extraction.result
    errors = set()
    keys = set()
    actual_set = set()
    status_counts = {}
    for row in rows:
        if set(row) != set(ATTENDANCE_COLUMNS):
            errors.add('row_columns_mismatch')
        if _FORBIDDEN_MATCH_FIELDS & set(row):
            errors.add('match_fields_forbidden')
        key = tuple(row[column] for column in ATTENDANCE_KEY)
        if key in keys:
            errors.add('duplicate_natural_key')
        keys.add(key)
        actual_set.add(row['club_id'])
        status = row['observation_status']
        if status in {'unknown', 'unavailable', 'error'}:
            errors.add('row_status_unresolved')
        elif status not in {'observed_present', 'explicit_none'}:
            errors.add('row_status_invalid')
        status_counts[status] = status_counts.get(status, 0) + 1
        if row['captured_at'] != row['observed_at']:
            errors.add('observation_timestamp_mismatch')
        for field in _LINEAGE_FIELDS:
            if not isinstance(row[field], str) or not row[field]:
                errors.add('lineage_incomplete')
        if row['_batch_id'] != extraction.batch_id \
                or row['_ingested_at'] != extraction.extracted_at:
            errors.add('batch_identity_mismatch')
        try:
            _validate_capacity_filled(row['capacity_filled_raw'])
        except AttendanceParseError:
            errors.add('capacity_filled_invalid')
        for field in ('capacity', 'spectators_total', 'spectators_average', 'matches', 'sold_out'):
            value = row[field]
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                errors.add('attendance_units_invalid')
            try:
                normalized = _integer(row[f'{field}_raw'], field)
            except AttendanceParseError:
                errors.add('attendance_raw_invalid')
            else:
                if normalized != value:
                    errors.add('attendance_raw_mismatch')
    if len(rows) != len(actual_set):
        errors.add('duplicate_club_observation')
    missing = sorted(expected_set - actual_set, key=int)
    unexpected = sorted(actual_set - expected_set, key=int)
    if result.status in {'observed_present', 'explicit_none'}:
        if missing:
            errors.add('participants_missing')
        if unexpected:
            errors.add('participants_unexpected')
        if not rows:
            errors.add('rows_missing_for_present')
    else:
        if rows and result.status != 'unknown':
            errors.add('rows_unexpected_for_status')
        if result.status == 'observed_empty' and expected_set:
            errors.add('participants_missing')
        if result.status in {'unknown', 'unavailable', 'error'}:
            errors.add('unresolved_capture')
    if extraction.final_season and not extraction.final_season_proof:
        errors.add('final_season_proof_missing')
    return {
        'passed': not errors,
        'structural_passed': not errors,
        'capture_status': result.status,
        'status_counts': dict(sorted(status_counts.items())),
        'expected_clubs': len(expected_set),
        'observed_clubs': len(actual_set),
        'row_count': len(rows),
        'page_count': result.page_count,
        'reconciled_duplicates': result.reconciled_duplicates,
        'missing_club_ids': missing,
        'unexpected_club_ids': unexpected,
        'final_season': extraction.final_season,
        'canonical_sha256': extraction.canonical_sha256(),
        'errors': sorted(errors),
    }


__all__ = [
    'ATTENDANCE_COLUMNS', 'ATTENDANCE_KEY', 'ATTENDANCE_PARSER_REVISION',
    'ATTENDANCE_SCHEMA_REVISION', 'ATTENDANCE_STATUSES', 'ATTENDANCE_TABLE',
    'AttendanceExtraction', 'AttendancePage', 'AttendanceParseError',
    'AttendanceResult', 'AttendanceRow', 'build_attendance_dq',
    'canonical_club_id', 'canonical_timestamp', 'confirmed_empty_attendance',
    'error_attendance', 'extract_attendance', 'not_applicable_attendance',
    'next_attendance_url', 'parse_attendance', 'unavailable_attendance', 'unknown_attendance',
]
