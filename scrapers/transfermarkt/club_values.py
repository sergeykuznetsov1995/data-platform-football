"""Published club-value points from Transfermarkt competition pages."""
from __future__ import annotations

import ast
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import re
from typing import Any, Mapping
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from scrapers.transfermarkt.scraper import (
    MoneyLocaleError, _TM_MONEY_RE, _TM_MONEY_UNITS,
    _normalise_decimal_number, _parse_tm_money_eur,
)


CLUB_VALUE_PARSER_REVISION = 'club-values-v1'
CLUB_VALUE_SCHEMA_REVISION = '1'
CLUB_VALUE_KEY = ('club_id', 'value_date', 'observed_at', 'source_point_id')
CLUB_VALUE_COLUMNS = (
    'competition_id', 'club_id', 'club_slug', 'club_name', 'value_date',
    'currency', 'value_amount', 'observation_status', 'source_point_id',
    'source_point_kind', 'source_value_raw', 'reconciliation_status', 'observed_at', 'source_url',
    'raw_capture_id', 'source_body_hash', 'captured_at', 'extracted_at',
    'parser_revision', 'schema_revision', 'scope_id', 'run_id', 'cycle_id',
    'universe_version', '_source', '_entity_type', '_batch_id', '_ingested_at',
)
CLUB_VALUE_STATUSES = frozenset({'observed_present', 'explicit_none', 'unknown'})
CLUB_VALUE_RECONCILIATION_STATUSES = frozenset({'matched', 'value_differs', 'single_evidence'})
_CLUB_LINK = re.compile(r'^/([^/]+)/startseite/verein/(\d+)(?:/|$)')
_COMPETITION_ID = re.compile(r'[A-Za-z0-9_-]+')
_PREVIOUS_HEADER = re.compile(r'^Value (\d{2}/\d{2}/\d{4})$')
_CREST_ID = re.compile(r'/(\d+)\.png(?:\?|$)')
_HEX_64 = re.compile(r'[0-9a-f]{64}')
_MISSING = frozenset({'', '-'})


class ClubValueParseError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _required(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ClubValueParseError(name + '_required')
    return value.strip()


def _timestamp(value: Any, name: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except ValueError as exc:
        raise ClubValueParseError(name + '_invalid') from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ClubValueParseError(name + '_invalid')
    return parsed.astimezone(timezone.utc).isoformat()


def _date(value: str, reason: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as exc:
        raise ClubValueParseError(reason) from exc


def _display_date(value: str) -> str:
    try:
        return datetime.strptime(value, '%d/%m/%Y').date().isoformat()
    except ValueError as exc:
        raise ClubValueParseError('chart_date_invalid') from exc


@dataclass(frozen=True)
class ClubValueCapture:
    competition_id: str
    source_url: str
    raw_capture_id: str
    source_body_hash: str
    captured_at: str
    extracted_at: str
    scope_id: str
    run_id: str
    cycle_id: str
    universe_version: str

    def __post_init__(self):
        competition = _required(self.competition_id, 'competition_id')
        if not _COMPETITION_ID.fullmatch(competition):
            raise ClubValueParseError('competition_id_invalid')
        for name in ('raw_capture_id', 'source_body_hash'):
            value = _required(getattr(self, name), name)
            if not _HEX_64.fullmatch(value):
                raise ClubValueParseError(name + '_invalid')
        for name in ('captured_at', 'extracted_at'):
            object.__setattr__(self, name, _timestamp(getattr(self, name), name))
        for name in ('scope_id', 'run_id', 'cycle_id', 'universe_version'):
            _required(getattr(self, name), name)
        parsed = urlsplit(self.source_url)
        if (parsed.scheme != 'https' or parsed.netloc != 'www.transfermarkt.com'
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ClubValueParseError('source_url_invalid')
        parts = parsed.path.strip('/').split('/')
        if (len(parts) not in {4, 6} or parts[1] != 'marktwerteverein'
                or parts[2] not in {'wettbewerb', 'pokalwettbewerb'}
                or parts[3] != competition):
            raise ClubValueParseError('source_url_competition_mismatch')
        if len(parts) == 6 and parts[4] != 'stichtag':
            raise ClubValueParseError('source_url_stichtag_invalid')
        if len(parts) == 6:
            _date(parts[5], 'source_url_stichtag_invalid')

    @property
    def requested_stichtag(self) -> str | None:
        parts = urlsplit(self.source_url).path.strip('/').split('/')
        return parts[5] if len(parts) == 6 else None

    @property
    def batch_id(self) -> str:
        identity = [CLUB_VALUE_PARSER_REVISION, self.raw_capture_id, self.source_body_hash]
        return 'tm-club-values-' + hashlib.sha256(canonical_json(identity)).hexdigest()[:32]


@dataclass(frozen=True)
class ClubValueExtraction:
    capture: ClubValueCapture
    available_dates: tuple[str, ...]
    value_dates: tuple[str, ...]
    club_ids: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]
    reconciled_table_points: int
    differing_table_points: int
    full_available_history: bool

    def __post_init__(self):
        if not self.club_ids or not self.rows:
            raise ClubValueParseError('club_values_empty')
        keys = [tuple(row[column] for column in CLUB_VALUE_KEY) for row in self.rows]
        if len(set(keys)) != len(keys):
            raise ClubValueParseError('club_value_observation_duplicate')
        if set(self.club_ids) != {row['club_id'] for row in self.rows}:
            raise ClubValueParseError('club_value_universe_mismatch')
        for row in self.rows:
            if set(row) != set(CLUB_VALUE_COLUMNS):
                raise ClubValueParseError('club_value_columns_changed')
            if row['observation_status'] not in CLUB_VALUE_STATUSES:
                raise ClubValueParseError('observation_status_invalid')
            if row['reconciliation_status'] not in CLUB_VALUE_RECONCILIATION_STATUSES:
                raise ClubValueParseError('reconciliation_status_invalid')
            if (row['observation_status'] == 'observed_present') != (row['value_amount'] is not None):
                raise ClubValueParseError('observation_amount_invalid')
            if row['value_amount'] is not None and (isinstance(row['value_amount'], bool)
                    or not isinstance(row['value_amount'], int) or row['value_amount'] < 0):
                raise ClubValueParseError('observation_amount_invalid')
            _date(row['value_date'], 'value_date_invalid')
            for column in CLUB_VALUE_COLUMNS:
                if column != 'value_amount' and (row[column] is None or row[column] == ''):
                    raise ClubValueParseError('lineage_missing:' + column)

    @property
    def status_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(row['observation_status'] for row in self.rows).items()))

    def club_rows(self, club_id: str) -> tuple[Mapping[str, Any], ...]:
        return tuple(row for row in self.rows if row['club_id'] == club_id)

    def club_status_counts(self, club_id: str) -> dict[str, int]:
        return dict(sorted(Counter(row['observation_status'] for row in self.club_rows(club_id)).items()))

    def plan_sha256(self) -> str:
        payload = {
            'capture': asdict(self.capture), 'available_dates': self.available_dates,
            'value_dates': self.value_dates, 'club_ids': self.club_ids,
            'rows': self.rows, 'reconciled_table_points': self.reconciled_table_points,
            'differing_table_points': self.differing_table_points,
            'full_available_history': self.full_available_history,
        }
        return hashlib.sha256(canonical_json(payload)).hexdigest()


def build_club_value_dq(extraction: ClubValueExtraction,
                        receipts: tuple[Mapping[str, Any], ...] | None = None) -> dict[str, Any]:
    keys = [tuple(row[column] for column in CLUB_VALUE_KEY) for row in extraction.rows]
    null_lineage = sorted({column for row in extraction.rows for column in CLUB_VALUE_COLUMNS
                           if column != 'value_amount' and (row[column] is None or row[column] == '')})
    result = {
        'structural_passed': not null_lineage and len(keys) == len(set(keys)),
        'expected_clubs': len(extraction.club_ids), 'expected_points': len(extraction.rows),
        'status_counts': extraction.status_counts,
        'duplicate_observations': len(keys) - len(set(keys)),
        'null_lineage_columns': null_lineage,
        'reconciled_table_points': extraction.reconciled_table_points,
        'differing_table_points': extraction.differing_table_points,
        'full_available_history': extraction.full_available_history,
    }
    if receipts is not None:
        by_club = {}
        for receipt in receipts:
            if receipt.get('verified') and receipt.get('receipt_readback') and receipt.get('physical_hash'):
                by_club[receipt.get('club_id')] = receipt
        missing = [club_id for club_id in extraction.club_ids if club_id not in by_club]
        wrong_points = [club_id for club_id, receipt in by_club.items()
                        if receipt.get('expected_points') != len(extraction.club_rows(club_id))]
        result.update(
            verified_write_coverage=(len(by_club) / len(extraction.club_ids)),
            missing_receipt_clubs=missing, wrong_receipt_point_clubs=sorted(wrong_points),
            reconciliation_passed=not missing and not wrong_points,
        )
    return result


def with_stichtag(source_url: str, value_date: str) -> str:
    value_date = _date(value_date, 'stichtag_invalid')
    parsed = urlsplit(source_url)
    parts = parsed.path.strip('/').split('/')
    if len(parts) != 4:
        raise ClubValueParseError('stichtag_base_url_invalid')
    base = parsed._replace(path=parsed.path.rstrip('/') + '/stichtag/' + value_date)
    return base.geturl()


def _js_value(script: str, key: str) -> Any:
    marker = "'" + key + "':"
    try:
        start = script.index(marker) + len(marker)
    except ValueError as exc:
        raise ClubValueParseError('chart_' + key + '_missing') from exc
    if start >= len(script) or script[start] not in '[{':
        raise ClubValueParseError('chart_' + key + '_invalid')
    stack = []
    quote = None
    escaped = False
    pairs = {']': '[', '}': '{'}
    for index in range(start, len(script)):
        char = script[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {'\'', '"'}:
            quote = char
        elif char in '[{':
            stack.append(char)
        elif char in ']}':
            if not stack or stack.pop() != pairs[char]:
                raise ClubValueParseError('chart_' + key + '_invalid')
            if not stack:
                raw = script[start:index + 1]
                raw = re.sub(r'\btrue\b', 'True', raw)
                raw = re.sub(r'\bfalse\b', 'False', raw)
                raw = re.sub(r'\bnull\b', 'None', raw)
                try:
                    return ast.literal_eval(raw)
                except (SyntaxError, ValueError) as exc:
                    raise ClubValueParseError('chart_' + key + '_invalid') from exc
    raise ClubValueParseError('chart_' + key + '_invalid')


def _money(raw: str) -> dict[str, Any]:
    text = ' '.join(raw.split())
    if text in _MISSING:
        return {'amount': None, 'status': 'explicit_none', 'raw': text}
    try:
        amount = _parse_tm_money_eur(text)
    except MoneyLocaleError as exc:
        raise ClubValueParseError('money_locale_changed') from exc
    if amount is None or amount < 0:
        raise ClubValueParseError('money_value_invalid')
    return {'amount': amount, 'status': 'observed_present', 'raw': text}


def _table(soup: BeautifulSoup) -> tuple[dict[str, dict[str, Any]], str]:
    table = soup.select_one('table.items')
    if table is None:
        raise ClubValueParseError('club_table_missing')
    headers = [th.get_text(' ', strip=True) for th in table.select('thead th')]
    if len(headers) != 7 or headers[:4] != ['#', 'wappen', 'Club', 'League'] or headers[5:] != ['Current value', '%']:
        raise ClubValueParseError('club_table_headers_changed')
    previous_match = _PREVIOUS_HEADER.fullmatch(headers[4])
    if previous_match is None:
        raise ClubValueParseError('previous_value_date_missing')
    previous_date = _display_date(previous_match.group(1))
    clubs: dict[str, dict[str, Any]] = {}
    names = set()
    for tr in table.select('tbody > tr'):
        cells = tr.find_all('td', recursive=False)
        if len(cells) != len(headers):
            raise ClubValueParseError('club_row_shape_changed')
        links = []
        for anchor in cells[2].find_all('a', href=True):
            match = _CLUB_LINK.match(anchor['href'])
            if match:
                links.append((anchor, match))
        if len(links) != 1:
            raise ClubValueParseError('club_identity_invalid')
        anchor, match = links[0]
        club_id = str(int(match.group(2)))
        name = ' '.join(anchor.get_text(' ', strip=True).split()) or anchor.get('title', '').strip()
        if not name or club_id in clubs or name in names:
            raise ClubValueParseError('club_identity_duplicate')
        names.add(name)
        clubs[club_id] = {
            'club_id': club_id, 'club_slug': match.group(1), 'club_name': name,
            'previous': _money(cells[4].get_text(' ', strip=True)),
            'current': _money(cells[5].get_text(' ', strip=True)),
        }
    if not clubs:
        raise ClubValueParseError('club_table_empty')
    return clubs, previous_date


def _available_dates(soup: BeautifulSoup, requested: str | None) -> tuple[str, ...]:
    values = []
    selected = []
    for option in soup.select('select[name="stichtag"] option'):
        value = _date(option.get('value', ''), 'stichtag_option_invalid')
        values.append(value)
        if option.has_attr('selected'):
            selected.append(value)
    if not values or len(set(values)) != len(values):
        raise ClubValueParseError('stichtag_options_invalid')
    dates = tuple(sorted(values))
    if requested is not None:
        if requested not in dates or (selected and selected != [requested]):
            raise ClubValueParseError('stichtag_selection_mismatch')
    return dates


def _chart(soup: BeautifulSoup, available: tuple[str, ...], requested: str | None) -> tuple[tuple[str, ...], list[Mapping[str, Any]]]:
    scripts = [script.get_text() for script in soup.find_all('script')
               if 'Highcharts.Chart' in script.get_text() and "'series':" in script.get_text()]
    if len(scripts) != 1:
        raise ClubValueParseError('club_chart_missing' if not scripts else 'club_chart_ambiguous')
    categories = tuple(_display_date(value) for value in _js_value(scripts[0], 'categories'))
    if (not categories or len(set(categories)) != len(categories)
            or tuple(sorted(categories)) != categories
            or any(value not in available for value in categories)
            or categories[-1] != available[-1]):
        raise ClubValueParseError('chart_categories_invalid')
    if requested is not None and categories[0] != requested:
        raise ClubValueParseError('chart_start_mismatch')
    series = _js_value(scripts[0], 'series')
    if not isinstance(series, list) or not series:
        raise ClubValueParseError('chart_series_empty')
    return categories, series


def _point(capture: ClubValueCapture, club: Mapping[str, Any], value_date: str,
           amount: int | None, status: str, kind: str, raw: str) -> dict[str, Any]:
    return {
        'competition_id': capture.competition_id,
        'club_id': club['club_id'], 'club_slug': club['club_slug'], 'club_name': club['club_name'],
        'value_date': value_date, 'currency': 'EUR', 'value_amount': amount,
        'observation_status': status,
        'source_point_id': f'{capture.competition_id}:{club["club_id"]}:{value_date}:EUR:{kind}',
        'source_point_kind': kind, 'source_value_raw': raw,
        'reconciliation_status': 'single_evidence', 'observed_at': capture.captured_at,
        'source_url': capture.source_url, 'raw_capture_id': capture.raw_capture_id,
        'source_body_hash': capture.source_body_hash, 'captured_at': capture.captured_at,
        'extracted_at': capture.extracted_at, 'parser_revision': CLUB_VALUE_PARSER_REVISION,
        'schema_revision': CLUB_VALUE_SCHEMA_REVISION, 'scope_id': capture.scope_id,
        'run_id': capture.run_id, 'cycle_id': capture.cycle_id,
        'universe_version': capture.universe_version,
        '_source': 'transfermarkt', '_entity_type': 'club_value_observation',
        '_batch_id': capture.batch_id, '_ingested_at': capture.extracted_at,
    }


def _display_value_matches(graph_amount: int, evidence: Mapping[str, Any]) -> bool:
    match = _TM_MONEY_RE.search(evidence['raw'])
    if match is None:
        return False
    displayed = _normalise_decimal_number(match.group('amount'))
    unit = (match.group('unit') or '').lower()
    if displayed is None or unit not in _TM_MONEY_UNITS:
        return False
    multiplier = _TM_MONEY_UNITS[unit]
    places = max(0, -displayed.as_tuple().exponent)
    rounded = (Decimal(graph_amount) / multiplier).quantize(
        Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return rounded == displayed


def _reconcile(capture: ClubValueCapture, club: Mapping[str, Any], row: dict[str, Any],
               evidence: Mapping[str, Any], kind: str) -> tuple[dict[str, Any], dict[str, Any] | None, bool]:
    amount = evidence['amount']
    same_missing = amount is None and row['observation_status'] != 'observed_present'
    same_value = (amount is not None and row['observation_status'] == 'observed_present'
                  and _display_value_matches(row['value_amount'], evidence))
    if same_missing or same_value:
        if amount is None:
            row['observation_status'] = 'explicit_none'
            row['value_amount'] = None
        row['source_point_kind'] += '+' + kind
        row['source_value_raw'] += ' | ' + evidence['raw']
        row['reconciliation_status'] = 'matched'
        return row, None, True
    row['reconciliation_status'] = 'value_differs'
    separate = _point(capture, club, row['value_date'], amount, evidence['status'], kind, evidence['raw'])
    separate['reconciliation_status'] = 'value_differs'
    return row, separate, False


def parse_club_values(html: Any, capture: ClubValueCapture) -> ClubValueExtraction:
    if not isinstance(html, str) or not html.strip():
        raise ClubValueParseError('invalid_html')
    if not isinstance(capture, ClubValueCapture):
        raise ClubValueParseError('capture_invalid')
    soup = BeautifulSoup(html, 'html.parser')
    clubs, previous_date = _table(soup)
    requested = capture.requested_stichtag
    if requested is not None and requested != previous_date:
        raise ClubValueParseError('stichtag_header_mismatch')
    available = _available_dates(soup, requested)
    categories, series = _chart(soup, available, requested)
    series_by_name = {}
    for item in series:
        if not isinstance(item, Mapping) or not isinstance(item.get('name'), str) or not item['name']:
            raise ClubValueParseError('chart_series_identity_invalid')
        if item['name'] in series_by_name:
            raise ClubValueParseError('chart_series_duplicate')
        series_by_name[item['name']] = item
    if set(series_by_name) != {club['club_name'] for club in clubs.values()}:
        raise ClubValueParseError('chart_table_clubs_differ')

    rows = []
    reconciled = 0
    differing = 0
    for club_id, club in clubs.items():
        item = series_by_name[club['club_name']]
        points = item.get('data')
        if not isinstance(points, list) or not points or len(points) > len(categories):
            raise ClubValueParseError('chart_series_length_invalid')
        for point in points:
            if not isinstance(point, Mapping):
                raise ClubValueParseError('chart_point_invalid')
            marker = point.get('marker')
            if isinstance(marker, Mapping):
                symbol = marker.get('symbol')
                if isinstance(symbol, str):
                    crest = _CREST_ID.search(symbol)
                    if crest and crest.group(1) != club_id:
                        raise ClubValueParseError('chart_table_club_id_mismatch')
        club_rows = []
        for value_date, point in zip(categories[-len(points):], points):
            raw = point.get('y')
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise ClubValueParseError('chart_point_value_invalid')
            club_rows.append(_point(capture, club, value_date, raw if raw > 0 else None,
                                    'observed_present' if raw > 0 else 'unknown',
                                    'highcharts', str(raw)))
        club_rows[-1], separate, matched = _reconcile(capture, club, club_rows[-1], club['current'], 'table_current')
        if separate is not None:
            club_rows.append(separate)
        reconciled += int(matched)
        differing += int(not matched)
        previous_index = next((index for index, row in enumerate(club_rows)
                               if row['value_date'] == previous_date and row['source_point_kind'] == 'highcharts'), None)
        if previous_index is None:
            club_rows.append(_point(capture, club, previous_date, club['previous']['amount'],
                                    club['previous']['status'], 'table_previous', club['previous']['raw']))
        else:
            club_rows[previous_index], separate, matched = _reconcile(
                capture, club, club_rows[previous_index], club['previous'], 'table_previous')
            if separate is not None:
                club_rows.append(separate)
            reconciled += int(matched)
            differing += int(not matched)
        rows.extend(club_rows)
    rows.sort(key=lambda row: (row['club_id'], row['value_date'], row['source_point_kind']))
    return ClubValueExtraction(
        capture=capture, available_dates=available, value_dates=categories,
        club_ids=tuple(sorted(clubs, key=int)), rows=tuple(rows), reconciled_table_points=reconciled,
        differing_table_points=differing,
        full_available_history=requested is not None and requested == available[0],
    )


__all__ = [
    'CLUB_VALUE_COLUMNS', 'CLUB_VALUE_KEY', 'CLUB_VALUE_PARSER_REVISION',
    'CLUB_VALUE_SCHEMA_REVISION', 'CLUB_VALUE_STATUSES', 'ClubValueCapture',
    'ClubValueExtraction', 'ClubValueParseError', 'build_club_value_dq',
    'canonical_json', 'parse_club_values', 'with_stichtag',
]
