"""Offline extraction and reconciliation of one competition's rumour listing."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from scrapers.transfermarkt.rumours import (
    OBSERVATION_KEY_FIELDS, PARSER_VERSION, SCHEMA_VERSION, CompetitionContext,
    RumourCapture, RumourPagination, RumourParseError, build_rumour_page_plan, parse_rumour_pagination,
    parse_rumours,
)


RUMOUR_KEY = tuple(OBSERVATION_KEY_FIELDS)
RUMOUR_COLUMNS = (
    'competition_id', 'page_number', 'player_id', 'player_name', 'player_url',
    'subject_id', 'interested_club_id', 'interested_club_name', 'interested_club_url',
    'interested_club_competition_id', 'interested_club_competition_name',
    'interested_club_competition_url', 'current_club_id', 'current_club_name',
    'current_club_url', 'position', 'nationalities_json', 'age', 'last_source_date',
    'last_reply_date', 'probability_percent', 'thread_link', 'rumour_identity',
    'observed_at', 'content_hash', 'correction_identity', 'observation_key',
    'assertion_json', 'source_page_url', 'raw_capture_id', 'source_body_hash',
    'captured_at', 'extracted_at', 'parser_revision', 'schema_revision',
    'scope_id', 'run_id', 'cycle_id', 'universe_version', '_source', '_entity_type',
    '_batch_id', '_ingested_at',
)
_REQUIRED = (
    'competition_id', 'page_number', 'player_id', 'player_name', 'player_url',
    'interested_club_id', 'interested_club_name', 'interested_club_url', *RUMOUR_KEY,
    'observation_key', 'content_hash', 'source_page_url', 'raw_capture_id',
    'source_body_hash', 'captured_at', 'extracted_at', 'parser_revision',
    'schema_revision', 'scope_id', 'run_id', 'cycle_id', 'universe_version',
    '_source', '_entity_type', '_batch_id', '_ingested_at',
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode('utf-8')).hexdigest()


def utc_clock(value: str | datetime) -> str:
    if isinstance(value, str):
        if (not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
                             r'(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])', value)
                or value.endswith('-00:00')):
            raise ValueError('invalid offset or precision in rumour clock')
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('timezone-aware clock required')
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _missing(value: Any) -> bool:
    return value is None or isinstance(value, str) and not value.strip()


def physical_hash(rows: Iterable[Mapping[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: tuple(row[field] for field in RUMOUR_KEY))
    return digest(ordered)


@dataclass(frozen=True)
class RumourPageExtraction:
    capture: RumourCapture
    pagination: RumourPagination
    page_number: int
    source_page_url: str
    raw_capture_id: str
    source_body_hash: str
    captured_at: str
    extracted_at: str
    scope_id: str
    run_id: str
    cycle_id: str
    universe_version: str

    def lineage(self) -> dict[str, Any]:
        return {
            'competition_id': self.capture.competition.id, 'page_number': self.page_number,
            'source_page_url': self.source_page_url, 'raw_capture_id': self.raw_capture_id,
            'source_body_hash': self.source_body_hash, 'captured_at': self.captured_at,
            'extracted_at': self.extracted_at, 'parser_revision': PARSER_VERSION,
            'schema_revision': SCHEMA_VERSION, 'scope_id': self.scope_id,
            'run_id': self.run_id, 'cycle_id': self.cycle_id,
            'universe_version': self.universe_version,
        }

    @property
    def batch_id(self) -> str:
        return 'tm-rumours-' + digest(self.lineage())

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        metadata = dict(self.lineage(), _source='transfermarkt',
                        _entity_type='transfer_rumour_assertion', _batch_id=self.batch_id,
                        _ingested_at=self.extracted_at)
        rows = []
        for observation in self.capture.observations:
            payload = observation.to_dict()
            row = dict(metadata, assertion_json=canonical_json(payload),
                       nationalities_json=canonical_json(payload['nationalities']))
            for name in ('player', 'interested_club', 'interested_club_competition', 'current_club'):
                entity = payload[name] or {}
                for field in ('id', 'name', 'url'):
                    row[f'{name}_{field}'] = entity.get(field)
            for field in (*RUMOUR_KEY, 'observation_key', 'content_hash', 'age', 'position',
                          'last_source_date', 'last_reply_date', 'probability_percent', 'thread_link'):
                row[field] = payload[field]
            if row['probability_percent'] is not None:
                row['probability_percent'] = float(row['probability_percent'])
            rows.append({field: row[field] for field in RUMOUR_COLUMNS})
        return tuple(rows)

    def pagination_payload(self) -> dict[str, Any]:
        return {'current_page': self.pagination.current_page, 'last_page': self.pagination.last_page,
                'links': sorted(dict(self.pagination.links).items())}

    def validate(self) -> None:
        if any(_missing(value) for value in self.lineage().values()):
            raise ValueError('missing required rumour page lineage')
        if not all(isinstance(value, str) and value.strip() for value in
                   (self.scope_id, self.run_id, self.cycle_id, self.universe_version)):
            raise ValueError('invalid rumour run lineage')
        if not re.fullmatch(r'[0-9a-f]+', self.raw_capture_id):
            raise ValueError('invalid hexadecimal raw_capture_id')
        if not re.fullmatch(r'[0-9a-f]{64}', self.source_body_hash):
            raise ValueError('invalid source_body_hash')
        if (type(self.page_number) is not int or self.page_number < 1
                or self.pagination.current_page != self.page_number
                or dict(self.pagination.links).get(self.page_number) != self.source_page_url):
            raise ValueError('rumour page number or canonical URL differs')
        if (self.captured_at != utc_clock(self.captured_at)
                or self.extracted_at != utc_clock(self.extracted_at)
                or self.captured_at != self.capture.observed_at or self.extracted_at < self.captured_at):
            raise ValueError('inconsistent rumour capture clocks')
        parsed = self.capture.to_dict()
        if (parsed['parser_version'] != PARSER_VERSION or parsed['schema_version'] != SCHEMA_VERSION
                or self.capture.status != ('observed_present' if self.capture.observations else 'observed_empty')
                or self.capture.duplicate_count < 0):
            raise ValueError('invalid rumour capture status or revision')
        for observation in self.capture.observations:
            if (observation.observed_at != self.captured_at
                    or observation.competition != self.capture.competition):
                raise ValueError('observation differs from rumour capture context')


def extract_rumour_page(html: str | bytes, *, competition: CompetitionContext, page_number: int,
                        source_page_url: str, raw_capture_id: str, source_body_hash: str,
                        captured_at: str | datetime, extracted_at: str | datetime,
                        scope_id: str, run_id: str, cycle_id: str,
                        universe_version: str) -> RumourPageExtraction:
    body = html.encode('utf-8') if isinstance(html, str) else html
    if not isinstance(body, bytes) or hashlib.sha256(body).hexdigest() != source_body_hash:
        raise ValueError('source_body_hash differs from supplied rumour body')
    text = body.decode('utf-8')
    captured = utc_clock(captured_at)
    capture = parse_rumours(text, competition=competition, observed_at=captured)
    pagination = parse_rumour_pagination(text, competition=competition, page_url=source_page_url)
    page = RumourPageExtraction(capture, pagination, page_number, source_page_url, raw_capture_id,
                                source_body_hash, captured, utc_clock(extracted_at), scope_id,
                                run_id, cycle_id, universe_version)
    page.validate()
    return page


def _complete_plan(pages: tuple[RumourPageExtraction, ...]) -> tuple[str, ...]:
    if not pages:
        raise ValueError('missing rumour pages')
    urls = tuple(build_rumour_page_plan(tuple(page.pagination for page in pages)))
    first = pages[0]
    context = (first.capture.competition, first.scope_id, first.run_id, first.cycle_id,
               first.universe_version)
    if tuple(page.page_number for page in pages) != tuple(range(1, len(urls) + 1)):
        raise ValueError('missing or repeated captured rumour page')
    for url, page in zip(urls, pages):
        if (page.pagination.last_page != len(urls) or page.source_page_url != url
                or (page.capture.competition, page.scope_id, page.run_id, page.cycle_id,
                    page.universe_version) != context):
            raise ValueError('inconsistent rumour feed context or page plan')
    return urls


@dataclass(frozen=True)
class RumourFeedExtraction:
    pages: tuple[RumourPageExtraction, ...]

    def __post_init__(self) -> None:
        pages = tuple(self.pages)
        for page in pages:
            if not isinstance(page, RumourPageExtraction):
                raise TypeError('rumour page extraction required')
            page.validate()
        pages = tuple(sorted(pages, key=lambda page: page.page_number))
        _complete_plan(pages)
        object.__setattr__(self, 'pages', pages)

    def plan_sha256(self) -> str:
        return digest([dict(page.lineage(), pagination=page.pagination_payload(),
                            capture=page.capture.to_dict(), rows=page.rows) for page in self.pages])


def page_receipt_fields(page: RumourPageExtraction, plan: str) -> dict[str, Any]:
    rows = page.rows
    return dict(
        page.lineage(), plan_sha256=plan, receipt_id='tm-rumours-receipt-' + digest([plan, page.page_number]),
        pagination_json=canonical_json(page.pagination_payload()), capture_status=page.capture.status,
        duplicate_count=page.capture.duplicate_count, source_rows=len(rows) + page.capture.duplicate_count,
        expected_assertions=len(rows), expected_subjects=len({row['subject_id'] for row in rows}),
        status_counts_json=canonical_json({page.capture.status: 1}),
        physical_hash=physical_hash(rows), committed_batch=page.batch_id,
    )


def build_rumour_dq(extraction: RumourFeedExtraction | Iterable[RumourPageExtraction],
                    receipts: Iterable[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    pages = (extraction.pages if isinstance(extraction, RumourFeedExtraction)
             else tuple(sorted(extraction, key=lambda page: page.page_number)))
    try:
        _complete_plan(pages)
        complete = True
    except (RumourParseError, ValueError):
        complete = False
    expected = max((page.pagination.last_page for page in pages), default=0)
    statuses = Counter(page.capture.status for page in pages)
    terminal = set()
    rows = []
    null_lineage = 0
    for page in pages:
        page_nulls = sum(_missing(value) for value in page.lineage().values())
        page_rows = page.rows
        page_nulls += sum(_missing(row[field]) for row in page_rows for field in _REQUIRED)
        null_lineage += page_nulls
        rows.extend(page_rows)
        try:
            page.validate()
        except (TypeError, ValueError):
            continue
        if not page_nulls:
            terminal.add(page.page_number)
    keys = {}
    for row in rows:
        key = tuple(row[field] for field in RUMOUR_KEY)
        keys.setdefault(key, set()).add(digest(row))
    conflicts = sum(len(payloads) > 1 for payloads in keys.values())
    duplicates = sum(page.capture.duplicate_count for page in pages)
    structural = (complete and len(terminal) == len(pages) and not null_lineage
                  and len(keys) == len(rows))
    result = {
        'expected_pages': expected if pages else None, 'terminal_pages': len(terminal),
        'complete_page_plan': complete,
        'source_rows': len(rows) + duplicates, 'retained_assertions': len(rows),
        'distinct_subjects': len({row['subject_id'] for row in rows}), 'status_counts': dict(statuses),
        'duplicate_count': duplicates,
        'conflict_count': conflicts, 'unique_natural_keys': len(keys),
        'duplicate_natural_keys': len(rows) - len(keys), 'null_required_lineage': null_lineage,
        'fresh_conclusive': len(terminal), 'unresolved': expected - len(terminal) if expected else None,
        'structural_passed': structural, 'receipt_coverage': {}, 'reconciliation_passed': None,
    }
    if receipts is not None:
        supplied = tuple(receipts)
        coverage = {number: False for number in range(1, expected + 1)}
        if structural:
            plan = RumourFeedExtraction(pages).plan_sha256()
            for page in pages:
                matches = [receipt for receipt in supplied if receipt.get('page_number') == page.page_number]
                if len(matches) != 1:
                    continue
                receipt = matches[0]
                fields = page_receipt_fields(page, plan)
                try:
                    fields['committed_at'] = utc_clock(receipt.get('committed_at'))
                except (TypeError, ValueError):
                    continue
                coverage[page.page_number] = (
                    receipt.get('verified') is True and receipt.get('receipt_readback') is True
                    and all(receipt.get(k) == v for k, v in fields.items())
                    and receipt.get('receipt_hash') == digest(fields))
        result.update(receipt_coverage=coverage, reconciliation_passed=(
            structural and len(supplied) == expected and all(coverage.values())))
    return result


def rumour_exclusion_sql() -> str:
    return """SELECT count(*) AS rumour_violations
FROM iceberg.bronze.transfermarkt_transfer_events e
WHERE e.source_url LIKE '%/geruechte/%'
   OR e.parser_revision LIKE 'transfermarkt-rumours/%'
   OR EXISTS (
       SELECT 1 FROM iceberg.ops.transfermarkt_rumour_receipts_v1 r
       WHERE r.source_body_hash = e.source_body_hash
   )"""
