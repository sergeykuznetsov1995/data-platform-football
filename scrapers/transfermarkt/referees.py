"""Offline referee profiles and published season aggregates from saved HTML."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
import re
from typing import Any

from bs4 import BeautifulSoup

REFEREE_PARSER_REVISION = 'referees-v2'
REFEREE_SCHEMA_REVISION = '1'
REFEREE_OBSERVATION_STATUSES = frozenset({
    'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
    'unknown', 'unavailable', 'error',
})
_DIRECT_STATUSES = frozenset({'observed_present', 'observed_empty', 'explicit_none'})
REFEREE_COUNT_METRICS = (
    'matches_officiated', 'yellow_cards', 'second_yellow_cards',
    'direct_red_cards', 'penalties_awarded',
)
REFEREE_AVERAGE_METRICS = (
    'average_yellow_cards', 'average_second_yellow_cards',
    'average_direct_red_cards', 'average_penalties_awarded',
)
REFEREE_METRICS = REFEREE_COUNT_METRICS + REFEREE_AVERAGE_METRICS
REFEREE_AGGREGATE_COLUMNS = (
    'referee_id', 'competition_id', 'edition_id', 'metric', 'value',
    'observation_status', 'status_reason', 'raw_value', 'source_row', 'source_column',
)
REFEREE_AGGREGATE_OBSERVATION_KEY = (
    'referee_id', 'competition_id', 'edition_id', 'metric', 'observed_at',
)

_DIGITS_RE = re.compile(r'[0-9]+')
_COMPETITION_RE = re.compile(r'[A-Za-z0-9]+')
_REFEREE_HREF_RE = re.compile(r'^/[^/]+/profil/schiedsrichter/([1-9][0-9]*)(?:[/?#].*)?$')
_COMPETITION_HREF_RE = re.compile(
    r'/startseite/(?:wettbewerb|pokalwettbewerb)/([A-Za-z0-9]+)(?:[/?#]|$)',
)
_DATE_FORMATS = ('%d/%m/%Y', '%d.%m.%Y', '%Y-%m-%d', '%b %d, %Y', '%B %d, %Y')
_DIRECTORY_HEADERS = {
    'referee': 'identity',
    'referees': 'identity',
    'debut': 'debut_date',
    'matches': 'matches_officiated',
    'appearances': 'matches_officiated',
    'yellow cards': 'yellow_cards',
    'yellow cards per match': 'average_yellow_cards',
    'yellow cards per game': 'average_yellow_cards',
    'second yellow cards': 'second_yellow_cards',
    'second yellow cards per match': 'average_second_yellow_cards',
    'second yellow cards per game': 'average_second_yellow_cards',
    'red cards': 'direct_red_cards',
    'direct red cards': 'direct_red_cards',
    'red cards per match': 'average_direct_red_cards',
    'red cards per game': 'average_direct_red_cards',
    'penalties': 'penalties_awarded',
    'penalty kicks': 'penalties_awarded',
    'penalties per match': 'average_penalties_awarded',
    'penalties per game': 'average_penalties_awarded',
    'penalty kicks per match': 'average_penalties_awarded',
    'penalty kicks per game': 'average_penalties_awarded',
    # Published since 2026-11-28.  The profile and the debut date already
    # carry these facts, so the directory reads this column past.
    'age at debut': 'age_at_debut',
}
_DIRECTORY_PASSED_COLUMNS = frozenset({'age_at_debut'})
_HISTORY_HEADERS = {
    'season': 'season',
    'competition': 'competition',
    **{key: value for key, value in _DIRECTORY_HEADERS.items() if value != 'identity'},
}


class RefereeParseError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class RefereeProfile:
    referee_id: str
    name: str
    nationality: str | None = None
    birth_date: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'referee_id', _referee_id(self.referee_id))
        if not isinstance(self.name, str) or not self.name.strip():
            raise RefereeParseError('profile_identity_missing')
        object.__setattr__(self, 'name', ' '.join(self.name.split()))

    def as_dict(self) -> dict[str, Any]:
        return {
            'referee_id': self.referee_id,
            'name': self.name,
            'nationality': self.nationality,
            'birth_date': self.birth_date,
        }


@dataclass(frozen=True)
class RefereeDirectoryEntry:
    referee_id: str
    name: str
    nationality: str | None = None
    debut_date: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'referee_id', _referee_id(self.referee_id))
        if not isinstance(self.name, str) or not self.name.strip():
            raise RefereeParseError('referee_identity_invalid')
        object.__setattr__(self, 'name', ' '.join(self.name.split()))

    def as_dict(self) -> dict[str, Any]:
        return {
            'referee_id': self.referee_id,
            'name': self.name,
            'nationality': self.nationality,
            'debut_date': self.debut_date,
        }


@dataclass(frozen=True)
class RefereeAggregate:
    referee_id: str
    competition_id: str
    edition_id: str
    metric: str
    value: Decimal | None
    observation_status: str
    status_reason: str
    raw_value: str
    source_row: int
    source_column: str

    def __post_init__(self):
        object.__setattr__(self, 'referee_id', _referee_id(self.referee_id))
        object.__setattr__(self, 'competition_id', _competition_id(self.competition_id))
        object.__setattr__(self, 'edition_id', _edition_id(self.edition_id))
        if self.metric not in REFEREE_METRICS:
            raise RefereeParseError('metric_unknown')
        if self.value is not None and not isinstance(self.value, Decimal):
            raise RefereeParseError('metric_value_invalid')
        if self.observation_status not in _DIRECT_STATUSES:
            raise RefereeParseError('status_invalid')
        if (self.observation_status == 'observed_present') != (self.value is not None):
            raise RefereeParseError('status_invalid')
        if not isinstance(self.status_reason, str) or not self.status_reason:
            raise RefereeParseError('status_reason_missing')
        if not isinstance(self.raw_value, str):
            raise RefereeParseError('raw_value_invalid')
        if isinstance(self.source_row, bool) or not isinstance(self.source_row, int) or self.source_row < 1:
            raise RefereeParseError('source_row_invalid')
        if not isinstance(self.source_column, str) or not self.source_column:
            raise RefereeParseError('source_column_invalid')

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return self.referee_id, self.competition_id, self.edition_id, self.metric

    def business_tuple(self) -> tuple[Any, ...]:
        return (
            self.referee_id, self.competition_id, self.edition_id, self.metric,
            self.value, self.observation_status, self.status_reason,
            self.raw_value, self.source_column,
        )

    def as_dict(self) -> dict[str, Any]:
        values = {column: getattr(self, column) for column in REFEREE_AGGREGATE_COLUMNS}
        values['value'] = format(self.value, 'f') if self.value is not None else None
        return values


@dataclass(frozen=True)
class RefereeDirectoryResult:
    competition_id: str
    edition_id: str
    referees: tuple[RefereeDirectoryEntry, ...]
    aggregates: tuple[RefereeAggregate, ...]
    reconciled_duplicates: int = 0
    completeness_proof: str | None = None
    parser_revision: str = REFEREE_PARSER_REVISION
    schema_revision: str = REFEREE_SCHEMA_REVISION

    def __post_init__(self):
        object.__setattr__(self, 'competition_id', _competition_id(self.competition_id))
        object.__setattr__(self, 'edition_id', _edition_id(self.edition_id))
        _result_revisions(self.parser_revision, self.schema_revision)
        if not self.referees and not self.completeness_proof:
            raise RefereeParseError('completeness_proof_missing')

    def canonical_json(self) -> str:
        return _canonical_json({
            'competition_id': self.competition_id,
            'edition_id': self.edition_id,
            'parser_revision': self.parser_revision,
            'schema_revision': self.schema_revision,
            'reconciled_duplicates': self.reconciled_duplicates,
            'completeness_proof': self.completeness_proof,
            'referees': [row.as_dict() for row in self.referees],
            'aggregates': [row.as_dict() for row in self.aggregates],
        })


@dataclass(frozen=True)
class RefereeAggregateResult:
    referee_id: str
    competition_id: str
    aggregates: tuple[RefereeAggregate, ...]
    reconciled_duplicates: int = 0
    completeness_proof: str | None = None
    parser_revision: str = REFEREE_PARSER_REVISION
    schema_revision: str = REFEREE_SCHEMA_REVISION

    def __post_init__(self):
        object.__setattr__(self, 'referee_id', _referee_id(self.referee_id))
        object.__setattr__(self, 'competition_id', _competition_id(self.competition_id))
        _result_revisions(self.parser_revision, self.schema_revision)
        if not self.aggregates and not self.completeness_proof:
            raise RefereeParseError('completeness_proof_missing')

    def canonical_json(self) -> str:
        return _canonical_json({
            'referee_id': self.referee_id,
            'competition_id': self.competition_id,
            'parser_revision': self.parser_revision,
            'schema_revision': self.schema_revision,
            'reconciled_duplicates': self.reconciled_duplicates,
            'completeness_proof': self.completeness_proof,
            'aggregates': [row.as_dict() for row in self.aggregates],
        })


def parse_referee_profile(html: Any, *, referee_id: Any) -> RefereeProfile:
    soup = _soup(html)
    headlines = soup.select('h1.data-header__headline-wrapper')
    if len(headlines) != 1 or not headlines[0].get_text(' ', strip=True):
        raise RefereeParseError('profile_identity_missing')
    fields = {}
    spans = soup.select(
        'div.info-table--equal-space > span.info-table__content, '
        'div.info-table > span.info-table__content',
    )
    for index in range(0, len(spans) - 1, 2):
        label = spans[index].get_text(' ', strip=True).rstrip(':').strip().lower()
        fields[label] = spans[index + 1].get_text(' ', strip=True)
    birth_date = None
    raw_birth = fields.get('date of birth/age', fields.get('date of birth'))
    if raw_birth:
        birth_date = _date(raw_birth.split('(', 1)[0].strip(), 'profile_birth_date_invalid')
    nationality = fields.get('citizenship') or None
    return RefereeProfile(_referee_id(referee_id), headlines[0].get_text(' ', strip=True), nationality, birth_date)


def parse_referee_directory_page(
    html: Any, *, competition_id: Any, edition_id: Any,
) -> RefereeDirectoryResult:
    competition = _competition_id(competition_id)
    edition = _edition_id(edition_id)
    table, rows = _table_rows(html)
    thead = table.find('thead', recursive=False)
    if thead is None:
        fields = ['identity', 'debut_date', *REFEREE_COUNT_METRICS]
    else:
        fields = _detailed_fields(thead, _DIRECTORY_HEADERS, 'directory_headers_changed')
        required = {'identity', 'debut_date', *REFEREE_COUNT_METRICS, *REFEREE_AVERAGE_METRICS}
        if (set(fields) - _DIRECTORY_PASSED_COLUMNS != required or len(set(fields)) != len(fields)
                or any(fields.count(field) != 1 for field in _DIRECTORY_PASSED_COLUMNS & set(fields))):
            raise RefereeParseError('directory_headers_changed')

    referees: dict[str, RefereeDirectoryEntry] = {}
    aggregates: dict[tuple[str, str, str, str], RefereeAggregate] = {}
    duplicates = 0
    for source_row, tr in enumerate(rows, 1):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(fields):
            raise RefereeParseError('directory_row_shape_changed')
        values = dict(zip(fields, cells))
        entry = _directory_entry(values['identity'], values['debut_date'])
        previous = referees.get(entry.referee_id)
        if previous is None:
            referees[entry.referee_id] = entry
        elif previous != entry:
            raise RefereeParseError('directory_conflict')
        else:
            duplicates += 1
        for metric in REFEREE_METRICS:
            if metric not in values:
                continue
            row = _aggregate_cell(
                values[metric], referee_id=entry.referee_id, competition_id=competition,
                edition_id=edition, metric=metric, source_row=source_row,
            )
            existing = aggregates.get(row.natural_key)
            if existing is None:
                aggregates[row.natural_key] = row
            elif existing.business_tuple() != row.business_tuple():
                raise RefereeParseError('directory_conflict')
    return _directory_result(competition, edition, referees, aggregates, duplicates)


def reconcile_referee_directory_pages(
    pages: Any, *, competition_id: Any, edition_id: Any, expected_page_count: Any,
) -> RefereeDirectoryResult:
    competition = _competition_id(competition_id)
    edition = _edition_id(edition_id)
    if isinstance(expected_page_count, bool) or not isinstance(expected_page_count, int):
        raise RefereeParseError('directory_pagination_incomplete')
    try:
        supplied = tuple(pages)
    except TypeError as exc:
        raise RefereeParseError('directory_pagination_incomplete') from exc
    if expected_page_count < 1 or len(supplied) != expected_page_count:
        raise RefereeParseError('directory_pagination_incomplete')

    referees: dict[str, RefereeDirectoryEntry] = {}
    aggregates: dict[tuple[str, str, str, str], RefereeAggregate] = {}
    duplicates = 0
    for html in supplied:
        result = parse_referee_directory_page(html, competition_id=competition, edition_id=edition)
        duplicates += result.reconciled_duplicates
        for entry in result.referees:
            previous = referees.get(entry.referee_id)
            if previous is None:
                referees[entry.referee_id] = entry
            elif previous != entry:
                raise RefereeParseError('directory_conflict')
            else:
                duplicates += 1
        for row in result.aggregates:
            existing = aggregates.get(row.natural_key)
            if existing is None:
                aggregates[row.natural_key] = row
            elif existing.business_tuple() != row.business_tuple():
                raise RefereeParseError('directory_conflict')
    return _directory_result(competition, edition, referees, aggregates, duplicates)


def parse_referee_season_aggregates(
    html: Any, *, referee_id: Any, competition_id: Any,
) -> RefereeAggregateResult:
    referee = _referee_id(referee_id)
    competition = _competition_id(competition_id)
    table, rows = _table_rows(html)
    thead = table.find('thead', recursive=False)
    compact = thead is None
    if compact:
        fields = ['season', 'competition_emblem', 'competition', *REFEREE_COUNT_METRICS]
    else:
        fields = _detailed_fields(thead, _HISTORY_HEADERS, 'history_headers_changed')
        required = {'season', 'competition', *REFEREE_COUNT_METRICS, *REFEREE_AVERAGE_METRICS}
        if set(fields) != required or fields.count('competition') != 2:
            raise RefereeParseError('history_headers_changed')
        for field in fields:
            if field != 'competition' and fields.count(field) != 1:
                raise RefereeParseError('history_headers_changed')

    seasons: dict[str, tuple[tuple[Any, ...], ...]] = {}
    aggregates: dict[tuple[str, str, str, str], RefereeAggregate] = {}
    duplicates = 0
    for source_row, tr in enumerate(rows, 1):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(fields):
            raise RefereeParseError('history_row_shape_changed')
        edition = _season_edition(cells[fields.index('season')].get_text(' ', strip=True))
        competition_cells = [cell for field, cell in zip(fields, cells) if field == 'competition']
        _check_competition(competition_cells, competition)
        current: list[RefereeAggregate] = []
        for metric in REFEREE_METRICS:
            if metric not in fields:
                continue
            cell = cells[fields.index(metric)]
            current.append(_aggregate_cell(
                cell, referee_id=referee, competition_id=competition, edition_id=edition,
                metric=metric, source_row=source_row,
            ))
        business = tuple(row.business_tuple() for row in current)
        if edition in seasons:
            if seasons[edition] != business:
                raise RefereeParseError('aggregate_conflict')
            duplicates += 1
            continue
        seasons[edition] = business
        for row in current:
            aggregates[row.natural_key] = row
    return RefereeAggregateResult(
        referee, competition, _sorted_aggregates(aggregates.values()), duplicates,
    )


def confirmed_empty_referee_directory(
    *, competition_id: Any, edition_id: Any, completeness_proof: Any,
) -> RefereeDirectoryResult:
    return RefereeDirectoryResult(
        _competition_id(competition_id), _edition_id(edition_id), (), (),
        completeness_proof=_completeness_proof(completeness_proof),
    )


def confirmed_empty_referee_season_aggregates(
    *, referee_id: Any, competition_id: Any, completeness_proof: Any,
) -> RefereeAggregateResult:
    return RefereeAggregateResult(
        _referee_id(referee_id), _competition_id(competition_id), (),
        completeness_proof=_completeness_proof(completeness_proof),
    )


def _soup(html: Any) -> BeautifulSoup:
    if not isinstance(html, str) or not html.strip():
        raise RefereeParseError('invalid_html')
    return BeautifulSoup(html, 'html.parser')


def _table_rows(html: Any):
    soup = _soup(html)
    tables = soup.select('table.items')
    if len(tables) != 1:
        raise RefereeParseError('referee_table_missing' if not tables else 'referee_table_ambiguous')
    table = tables[0]
    tbody = table.find('tbody', recursive=False)
    if tbody is None:
        raise RefereeParseError('referee_table_structure_changed')
    rows = tbody.find_all('tr', recursive=False)
    if not rows:
        raise RefereeParseError('empty_layout_unproven')
    return table, rows


def _detailed_fields(thead: Any, mapping: dict[str, str], reason: str) -> list[str]:
    headers = thead.find_all('th')
    if not headers:
        raise RefereeParseError(reason)
    fields = []
    for header in headers:
        text = ' '.join(header.get_text(' ', strip=True).lower().split())
        field = mapping.get(_header_label(text, header))
        if field is None:
            raise RefereeParseError(reason)
        try:
            span = int(header.get('colspan', 1))
        except (TypeError, ValueError) as exc:
            raise RefereeParseError(reason) from exc
        if span < 1:
            raise RefereeParseError(reason)
        fields.extend([field] * span)
    return fields


def _header_label(text: str, header: Any) -> str:
    """Name a column by its text, or by the title of the icon that replaced it.

    Since 2026-11-28 the directory names its card columns by icon only, and
    marks the per-match average of a card with "ø-" before the icon.
    """
    titles = {' '.join(icon['title'].lower().split()) for icon in header.find_all(title=True)}
    if len(titles) != 1 or '' in titles:
        return text
    if not text:
        return titles.pop()
    if text in {'ø', 'ø-'}:
        return f'{titles.pop()} per match'
    return text


def _directory_entry(identity_cell: Any, debut_cell: Any) -> RefereeDirectoryEntry:
    links = identity_cell.find_all('a', href=True)
    matches = [_REFEREE_HREF_RE.match(link['href']) for link in links]
    ids = {match.group(1) for match in matches if match is not None}
    if len(ids) != 1:
        raise RefereeParseError('referee_identity_invalid')
    referee = _referee_id(ids.pop())
    named = [link for link in links if _REFEREE_HREF_RE.match(link['href']) is not None]
    name = named[0].get_text(' ', strip=True) if named else ''
    nationality = None
    inline = identity_cell.find('table', class_='inline-table')
    if inline is not None:
        inline_rows = inline.find_all('tr')
        if len(inline_rows) > 1:
            nationality = inline_rows[1].get_text(' ', strip=True) or None
    if nationality is None:
        flag = identity_cell.select_one('img.flaggenrahmen')
        if flag is not None:
            nationality = flag.get('title') or flag.get('alt') or None
    raw_debut = debut_cell.get_text(' ', strip=True)
    debut = None if raw_debut in {'', '-', '?'} else _date(raw_debut, 'debut_date_invalid')
    return RefereeDirectoryEntry(referee, name, nationality, debut)


def _aggregate_cell(
    cell: Any, *, referee_id: str, competition_id: str, edition_id: str,
    metric: str, source_row: int,
) -> RefereeAggregate:
    raw = cell.get_text(' ', strip=True)
    average = metric in REFEREE_AVERAGE_METRICS
    if raw == '':
        value, status, reason = None, 'observed_empty', 'empty_cell'
    elif raw == '-':
        value, status, reason = None, 'explicit_none', 'dash'
    else:
        valid = re.fullmatch(r'[0-9]+(?:[.,][0-9]+)?' if average else r'[0-9]+', raw)
        if valid is None:
            raise RefereeParseError('metric_value_invalid')
        try:
            value = Decimal(raw.replace(',', '.'))
        except InvalidOperation as exc:
            raise RefereeParseError('metric_value_invalid') from exc
        if not value.is_finite() or value < 0:
            raise RefereeParseError('metric_value_invalid')
        status, reason = 'observed_present', 'numeric'
    return RefereeAggregate(
        referee_id, competition_id, edition_id, metric, value, status, reason,
        raw, source_row, metric,
    )


def _check_competition(cells: list[Any], expected: str):
    found = set()
    for cell in cells:
        for anchor in cell.find_all('a', href=True):
            match = _COMPETITION_HREF_RE.search(anchor['href'])
            if match is not None:
                found.add(_competition_id(match.group(1)))
    if len(found) != 1:
        raise RefereeParseError('competition_identity_invalid')
    if found != {expected}:
        raise RefereeParseError('competition_mismatch')


def _season_edition(raw: str) -> str:
    if _DIGITS_RE.fullmatch(raw):
        try:
            return _edition_id(raw)
        except RefereeParseError as exc:
            raise RefereeParseError('season_invalid') from exc
    match = re.fullmatch(r'([0-9]{2}|[0-9]{4})/([0-9]{2}|[0-9]{4})', raw)
    if match is None:
        raise RefereeParseError('season_invalid')

    def year(value: str) -> int:
        if len(value) == 4:
            return int(value)
        short = int(value)
        return 1900 + short if short >= 50 else 2000 + short

    start, end = year(match.group(1)), year(match.group(2))
    if end != start + 1:
        raise RefereeParseError('season_invalid')
    return _edition_id(start)


def _date(raw: str, reason: str) -> str:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    raise RefereeParseError(reason)


def _referee_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise RefereeParseError('referee_id_invalid')
    if isinstance(value, str) and not _DIGITS_RE.fullmatch(value):
        raise RefereeParseError('referee_id_invalid')
    number = int(value)
    if number < 1:
        raise RefereeParseError('referee_id_invalid')
    return str(number)


def _competition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise RefereeParseError('competition_id_invalid')
    rendered = str(value)
    if not _COMPETITION_RE.fullmatch(rendered):
        raise RefereeParseError('competition_id_invalid')
    return rendered.upper()


def _edition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise RefereeParseError('edition_id_invalid')
    rendered = str(value)
    if not _DIGITS_RE.fullmatch(rendered) or not 1800 <= int(rendered) <= 2199:
        raise RefereeParseError('edition_id_invalid')
    return str(int(rendered))


def _completeness_proof(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise RefereeParseError('completeness_proof_missing')
    return value


def _result_revisions(parser_revision: str, schema_revision: str):
    if parser_revision != REFEREE_PARSER_REVISION or schema_revision != REFEREE_SCHEMA_REVISION:
        raise RefereeParseError('revision_unsupported')


def _directory_result(competition, edition, referees, aggregates, duplicates):
    return RefereeDirectoryResult(
        competition, edition,
        tuple(sorted(referees.values(), key=lambda row: int(row.referee_id))),
        _sorted_aggregates(aggregates.values()), duplicates,
    )


def _sorted_aggregates(values) -> tuple[RefereeAggregate, ...]:
    return tuple(sorted(
        values,
        key=lambda row: (int(row.referee_id), row.competition_id, int(row.edition_id), row.metric),
    ))


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False,
    )


__all__ = [
    'REFEREE_AGGREGATE_COLUMNS', 'REFEREE_AGGREGATE_OBSERVATION_KEY',
    'REFEREE_AVERAGE_METRICS', 'REFEREE_COUNT_METRICS', 'REFEREE_METRICS',
    'REFEREE_OBSERVATION_STATUSES', 'REFEREE_PARSER_REVISION', 'REFEREE_SCHEMA_REVISION',
    'RefereeAggregate', 'RefereeAggregateResult', 'RefereeDirectoryEntry',
    'RefereeDirectoryResult', 'RefereeParseError', 'RefereeProfile',
    'confirmed_empty_referee_directory', 'confirmed_empty_referee_season_aggregates',
    'parse_referee_directory_page', 'parse_referee_profile',
    'parse_referee_season_aggregates', 'reconcile_referee_directory_pages',
]
