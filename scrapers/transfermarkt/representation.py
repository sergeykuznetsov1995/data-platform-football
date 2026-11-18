"""Offline player representation observations from saved tmapi /players captures.

This module has no HTTP client or current-lane orchestration. A capture reader only
needs ``load_capture(capture_id)`` returning exact bytes and a raw manifest record.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Mapping, Protocol
from urllib.parse import parse_qs, urlsplit

REPRESENTATION_PARSER_REVISION = 'representation-v1'
REPRESENTATION_SCHEMA_REVISION = '1'
RELATION_TYPE = 'consultant_agency'
REPRESENTATION_TABLE = 'iceberg.bronze.transfermarkt_player_representation_observations'
STATUSES = frozenset({'assigned', 'explicit_none', 'unknown', 'missing_packet', 'error'})
CONCLUSIVE_STATUSES = frozenset({'assigned', 'explicit_none'})
REPRESENTATION_KEY = ('player_id', 'relation_type', 'representative_key', 'source_capture_id')
GROUP_KEY = ('player_id', 'relation_type', 'source_capture_id')
ASSOCIATION_FIELDS = (
    'assessment_manifest_id', 'universe_version', 'run_id', 'extracted_at',
    '_batch_id', '_ingested_at',
)
REPRESENTATION_COLUMNS = [
    'player_id', 'relation_type', 'representative_key', 'representative_id',
    'observation_status', 'status_reason', 'source_capture_id',
    'assessment_manifest_id', 'universe_version', 'run_id', 'extracted_at',
    'evidence_type', 'evidence_locator', 'source_url', 'raw_capture_id',
    'source_body_hash', 'source_endpoint', 'scope_id', 'cycle_id',
    'captured_at', 'observed_at', 'parser_revision', 'schema_revision',
    '_source', '_entity_type', '_batch_id', '_ingested_at',
]
TIMESTAMP_COLUMNS = frozenset({'captured_at', 'observed_at', 'extracted_at', '_ingested_at'})


class CaptureReader(Protocol):
    def load_capture(self, capture_id: str): ...


class RepresentationParseError(ValueError):
    """A saved packet cannot establish its requested identity or schema."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class UniversePlayer:
    player_id: str
    club_ids: tuple[str, ...] = ()
    scope_ids: tuple[str, ...] = ()
    competition_ids: tuple[str, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, 'player_id', canonical_player_id(self.player_id))
        for field in ('club_ids', 'scope_ids', 'competition_ids'):
            object.__setattr__(self, field, _canonical_string_tuple(getattr(self, field)))


@dataclass(frozen=True)
class RepresentationUniverse:
    universe_version: str
    players: tuple[UniversePlayer, ...]
    capture_ids: tuple[str, ...]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RepresentationUniverse:
        if not isinstance(value, Mapping):
            raise RepresentationParseError('universe is not an object')
        universe_version = value.get('universe_version')
        if not isinstance(universe_version, str) or not universe_version:
            raise RepresentationParseError('universe_version must be a nonempty string')
        raw_players = value.get('players')
        if not isinstance(raw_players, list):
            raise RepresentationParseError('universe players must be a list')
        players: dict[str, UniversePlayer] = {}
        for entry in raw_players:
            if not isinstance(entry, Mapping):
                raise RepresentationParseError('universe player entry is not an object')
            player = UniversePlayer(
                player_id=entry.get('player_id'),
                club_ids=entry.get('club_ids'),
                scope_ids=entry.get('scope_ids'),
                competition_ids=entry.get('competition_ids'),
            )
            previous = players.get(player.player_id)
            if previous is None:
                players[player.player_id] = player
            elif previous != player:
                raise RepresentationParseError('universe has conflicting player definitions')
        raw_capture_ids = value.get('capture_ids')
        if not isinstance(raw_capture_ids, list):
            raise RepresentationParseError('universe capture_ids must be a list')
        for capture_id in raw_capture_ids:
            if not isinstance(capture_id, str) or not _CAPTURE_ID_RE.fullmatch(capture_id):
                raise RepresentationParseError('universe capture ID must be a lowercase sha256 digest')
        return cls(
            universe_version,
            tuple(sorted(players.values(), key=lambda player: int(player.player_id))),
            tuple(sorted(set(raw_capture_ids))),
        )

    @property
    def assessment_manifest_id(self) -> str:
        manifest = {
            'universe_version': self.universe_version,
            'players': [
                {
                    'player_id': player.player_id,
                    'club_ids': list(player.club_ids),
                    'scope_ids': list(player.scope_ids),
                    'competition_ids': list(player.competition_ids),
                }
                for player in self.players
            ],
            'capture_ids': list(self.capture_ids),
        }
        return hashlib.sha256(canonical_json(manifest)).hexdigest()


@dataclass(frozen=True)
class RepresentationAssertion:
    player_id: str
    observation_status: str
    status_reason: str
    representative_id: str | None = None
    club_ids: tuple[str, ...] = ()
    evidence_type: str = 'tmapi_consultant_agency'
    evidence_locator: str = 'attributes.consultantAgencyId'

    def __post_init__(self):
        object.__setattr__(self, 'player_id', canonical_player_id(self.player_id))
        if self.observation_status not in STATUSES:
            raise RepresentationParseError('observation status is not known')
        if not isinstance(self.status_reason, str) or not self.status_reason:
            raise RepresentationParseError('status_reason must be a nonempty string')
        if self.observation_status == 'assigned':
            object.__setattr__(self, 'representative_id', canonical_player_id(self.representative_id))
        elif self.representative_id is not None:
            raise RepresentationParseError('representative ID is only allowed for assigned assertions')
        if self.observation_status == 'explicit_none' and self.evidence_type != 'player_profile_explicit_none':
            raise RepresentationParseError('explicit_none requires player_profile_explicit_none evidence')
        club_ids = tuple(self.club_ids)
        for club_id in club_ids:
            if not isinstance(club_id, str) or not club_id:
                raise RepresentationParseError('club IDs must be nonempty strings')
        object.__setattr__(self, 'club_ids', club_ids)
        for field in ('evidence_type', 'evidence_locator'):
            value = getattr(self, field)
            if not isinstance(value, str) or not value:
                raise RepresentationParseError(f'{field} must be a nonempty string')


@dataclass(frozen=True)
class RepresentationObservation:
    player_id: str
    relation_type: str
    representative_key: str
    source_capture_id: str
    observation_status: str
    status_reason: str
    assessment_manifest_id: str
    universe_version: str
    run_id: str
    extracted_at: str
    evidence_type: str
    evidence_locator: str
    parser_revision: str = REPRESENTATION_PARSER_REVISION
    schema_revision: str = REPRESENTATION_SCHEMA_REVISION
    representative_id: str | None = None
    source_url: str | None = None
    raw_capture_id: str | None = None
    source_body_hash: str | None = None
    source_endpoint: str | None = None
    scope_id: str | None = None
    cycle_id: str | None = None
    captured_at: str | None = None
    observed_at: str | None = None

    _CAPTURE_FIELDS = (
        'source_url', 'raw_capture_id', 'source_body_hash', 'source_endpoint',
        'scope_id', 'cycle_id',
    )

    def __post_init__(self):
        object.__setattr__(self, 'player_id', canonical_player_id(self.player_id))
        if self.relation_type != RELATION_TYPE:
            raise RepresentationParseError('relation type must be consultant_agency')
        if self.observation_status not in STATUSES:
            raise RepresentationParseError('observation status is not known')
        if self.parser_revision != REPRESENTATION_PARSER_REVISION:
            raise RepresentationParseError('parser revision is not supported')
        if self.schema_revision != REPRESENTATION_SCHEMA_REVISION:
            raise RepresentationParseError('schema revision is not supported')
        for field in ('status_reason', 'assessment_manifest_id', 'universe_version', 'run_id',
                      'evidence_type', 'evidence_locator', 'source_capture_id'):
            value = getattr(self, field)
            if not isinstance(value, str) or not value:
                raise RepresentationParseError(f'{field} must be a nonempty string')
        object.__setattr__(self, 'extracted_at', canonical_timestamp(self.extracted_at))
        if self.observation_status == 'assigned':
            object.__setattr__(self, 'representative_id', canonical_player_id(self.representative_id))
            if self.representative_key != f'agency:{self.representative_id}':
                raise RepresentationParseError('representative key must name the assigned agency')
        else:
            if self.representative_id is not None:
                raise RepresentationParseError('representative ID is only allowed for assigned observations')
            if self.representative_key != f'status:{self.observation_status}':
                raise RepresentationParseError('representative key must name the observation status')
        if self.observation_status == 'missing_packet':
            for field in (*self._CAPTURE_FIELDS, 'captured_at', 'observed_at'):
                if getattr(self, field) is not None:
                    raise RepresentationParseError(f'{field} must be NULL for missing_packet')
            digest = hashlib.sha256('|'.join((
                self.assessment_manifest_id, self.player_id, self.relation_type,
            )).encode('utf-8')).hexdigest()
            if self.source_capture_id != f'missing:{digest}':
                raise RepresentationParseError('missing_packet source_capture_id does not match its identity')
        else:
            for field in self._CAPTURE_FIELDS:
                value = getattr(self, field)
                if not isinstance(value, str) or not value:
                    raise RepresentationParseError(
                        f'{field} is required for capture-associated observations')
            if self.source_capture_id != self.raw_capture_id:
                raise RepresentationParseError('source_capture_id must equal raw_capture_id')
            object.__setattr__(self, 'captured_at', canonical_timestamp(self.captured_at))
            object.__setattr__(self, 'observed_at', canonical_timestamp(self.observed_at))
            if self.captured_at != self.observed_at:
                raise RepresentationParseError('captured_at must equal observed_at')
        if self.observation_status == 'explicit_none':
            if self.evidence_type != 'player_profile_explicit_none':
                raise RepresentationParseError('explicit_none requires player_profile_explicit_none evidence')
            self._require_profile_evidence_url()

    def _require_profile_evidence_url(self):
        parts = urlsplit(self.source_url)
        match = _PROFILE_PATH_RE.fullmatch(parts.path)
        if parts.netloc != 'www.transfermarkt.com' or match is None or match.group(2) != self.player_id:
            raise RepresentationParseError('explicit_none source_url must be the player profile page')

    @property
    def natural_key(self) -> tuple[str, ...]:
        return tuple(getattr(self, column) for column in REPRESENTATION_KEY)

    @property
    def group_key(self) -> tuple[str, ...]:
        return tuple(getattr(self, column) for column in GROUP_KEY)

    def as_dict(self) -> dict[str, Any]:
        return {column: getattr(self, column) for column in REPRESENTATION_COLUMNS
                if not column.startswith('_')}


@dataclass(frozen=True)
class RepresentationExtraction:
    universe: RepresentationUniverse
    run_id: str
    extracted_at: str
    observations: tuple[RepresentationObservation, ...]
    capture_reports: tuple[dict[str, Any], ...] = ()
    expected_groups: tuple[tuple[str, str, str], ...] = ()

    @property
    def batch_id(self) -> str:
        """Deterministic batch identity derived from the run identity.

        The ``tm-representation-batch-`` prefix is deliberately distinct from
        the ``tm-representation-`` run prefix.
        """
        digest = hashlib.sha256(canonical_json({'run_id': self.run_id})).hexdigest()[:32]
        return f'tm-representation-batch-{digest}'

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        batch_id = self.batch_id
        rendered: list[dict[str, Any]] = []
        for observation in sorted(self.observations, key=lambda item: item.natural_key):
            values = observation.as_dict()
            values['_source'] = 'transfermarkt'
            values['_entity_type'] = 'player_representation'
            values['_batch_id'] = batch_id
            values['_ingested_at'] = self.extracted_at
            rendered.append({column: values[column] for column in REPRESENTATION_COLUMNS})
        return tuple(rendered)

    def canonical_sha256(self) -> str:
        return hashlib.sha256(canonical_json(self.rows)).hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def representation_plan_sha256(extraction: RepresentationExtraction) -> str:
    plan = {
        'target_table': REPRESENTATION_TABLE,
        'rows': list(extraction.rows),
        'assessment_manifest_id': extraction.universe.assessment_manifest_id,
        'universe_version': extraction.universe.universe_version,
        'run_id': extraction.run_id,
        'extracted_at': extraction.extracted_at,
        'batch_id': extraction.batch_id,
    }
    return hashlib.sha256(canonical_json(plan)).hexdigest()


_DIGITS_RE = re.compile(r'[0-9]+')
_CAPTURE_ID_RE = re.compile(r'[0-9a-f]{64}')
_PROFILE_PATH_RE = re.compile(r'/([^/]+)/profil/spieler/([1-9][0-9]*)')


def canonical_player_id(value: Any) -> str:
    """Canonical positive decimal ID: positive int or ASCII digit string, no leading zeros."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise RepresentationParseError('ID must be a positive int or an ASCII digit string')
    if isinstance(value, str):
        if not _DIGITS_RE.fullmatch(value):
            raise RepresentationParseError('ID must be a positive int or an ASCII digit string')
        value = int(value)
    if value <= 0:
        raise RepresentationParseError('ID must be positive')
    return str(value)


def canonical_timestamp(value: Any) -> str:
    """Validate a timezone-aware ISO timestamp already in canonical form."""
    if not isinstance(value, str) or not value:
        raise RepresentationParseError('timestamp must be a timezone-aware ISO string')
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RepresentationParseError('timestamp is not a valid ISO string') from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise RepresentationParseError('timestamp must be timezone-aware')
    if moment.isoformat() != value:
        raise RepresentationParseError('timestamp is not in canonical form')
    return value


def _canonical_string_tuple(values: Any) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str) or not isinstance(values, (list, tuple)):
        raise RepresentationParseError('identifier list must contain nonempty strings')
    for item in values:
        if not isinstance(item, str) or not item:
            raise RepresentationParseError('identifier list must contain nonempty strings')
    return tuple(sorted(set(values)))


def _agency_status(attributes: Mapping[str, Any]) -> tuple[str, str, str | None]:
    if 'consultantAgencyId' not in attributes:
        return 'unknown', 'missing_field', None
    value = attributes['consultantAgencyId']
    if value is None:
        return 'unknown', 'null', None
    if isinstance(value, bool):
        return 'error', 'invalid_agency_boolean', None
    if isinstance(value, int):
        if value > 0:
            return 'assigned', 'positive_id', str(value)
        if value == 0:
            return 'unknown', 'zero', None
        return 'error', 'invalid_agency_negative_integer', None
    if isinstance(value, float):
        return 'error', 'invalid_agency_float', None
    if isinstance(value, str):
        if not value:
            return 'error', 'invalid_agency_empty_string', None
        if not _DIGITS_RE.fullmatch(value):
            return 'error', 'invalid_agency_nonnumeric_string', None
        if int(value) == 0:
            return 'unknown', 'zero', None
        return 'assigned', 'positive_id', str(int(value))
    return 'error', 'invalid_agency_type', None


def _player_assertion(player_id: str, row: Mapping[str, Any]) -> RepresentationAssertion:
    attributes = row.get('attributes')
    if not isinstance(attributes, Mapping):
        raise RepresentationParseError('representation player row lacks an attributes object')
    assignments = row.get('clubAssignments')
    if not isinstance(assignments, list):
        raise RepresentationParseError('representation player row lacks a clubAssignments list')
    club_ids: set[str] = set()
    for assignment in assignments:
        if not isinstance(assignment, Mapping):
            raise RepresentationParseError('club assignment is not an object')
        club_id = canonical_player_id(assignment.get('clubId'))
        if assignment.get('type') in ('current', 'additional'):
            club_ids.add(club_id)
    observation_status, status_reason, representative_id = _agency_status(attributes)
    return RepresentationAssertion(
        player_id=player_id,
        observation_status=observation_status,
        status_reason=status_reason,
        representative_id=representative_id,
        club_ids=tuple(sorted(club_ids, key=int)),
    )


def parse_representation_packet(payload: Any, *, expected_ids) -> dict[str, RepresentationAssertion]:
    expected = tuple(canonical_player_id(item) for item in expected_ids)
    if not 1 <= len(expected) <= 100:
        raise RepresentationParseError('representation packet requires 1..100 expected player IDs')
    if len(set(expected)) != len(expected):
        raise RepresentationParseError('representation packet expected player IDs must be unique')
    if not isinstance(payload, Mapping):
        raise RepresentationParseError('representation payload is not an object')
    if payload.get('success') is not True:
        raise RepresentationParseError('representation payload is not a successful envelope')
    data = payload.get('data')
    if not isinstance(data, list) or not data:
        raise RepresentationParseError('representation payload data is not a non-empty list')
    assertions: dict[str, RepresentationAssertion] = {}
    for row in data:
        if not isinstance(row, Mapping):
            raise RepresentationParseError('representation player row is not an object')
        player_id = canonical_player_id(row.get('id'))
        if player_id in assertions:
            raise RepresentationParseError('representation packet has a duplicate player ID')
        assertions[player_id] = _player_assertion(player_id, row)
    if set(assertions) != set(expected):
        raise RepresentationParseError('representation packet identity differs from the expected player IDs')
    return assertions


def _load_players_packet(captures: CaptureReader, capture_id: str) -> tuple[Any, list[str], Any]:
    """Load one capture and verify its raw /players evidence contract.

    Only fixed, source-independent reasons are raised so no arbitrary
    exception text can escape into a capture report.
    """
    body, raw = captures.load_capture(capture_id)
    if raw.status_code != 200:
        raise RepresentationParseError('unexpected_status_code')
    if raw.endpoint != 'current_players':
        raise RepresentationParseError('unexpected_endpoint')
    parts = urlsplit(raw.url)
    if parts.netloc != 'tmapi.transfermarkt.technology' or parts.path != '/players':
        raise RepresentationParseError('unexpected_url')
    if hashlib.sha256(body).hexdigest() != raw.content_hash:
        raise RepresentationParseError('body_hash_mismatch')
    expected_ids = parse_qs(parts.query).get('ids[]')
    if not expected_ids:
        raise RepresentationParseError('missing_request_ids')
    return raw, expected_ids, body


def extract_representation_observations(
    captures: CaptureReader,
    universe: RepresentationUniverse,
    *,
    extracted_at: datetime,
) -> RepresentationExtraction:
    if not isinstance(extracted_at, datetime):
        raise RepresentationParseError('extracted_at must be a datetime')
    if extracted_at.tzinfo is None or extracted_at.utcoffset() is None:
        raise RepresentationParseError('extracted_at must be timezone-aware')
    extracted_at_iso = extracted_at.astimezone(timezone.utc).isoformat()
    assessment_manifest_id = universe.assessment_manifest_id
    run_id = 'tm-representation-' + hashlib.sha256(canonical_json({
        'assessment_manifest_id': assessment_manifest_id,
        'extracted_at': extracted_at_iso,
    })).hexdigest()[:32]

    observations: list[RepresentationObservation] = []
    capture_reports: list[dict[str, Any]] = []
    expected_groups: set[tuple[str, str, str]] = set()
    requested_player_ids: set[str] = set()
    all_captures_reconciled = True

    for capture_id in universe.capture_ids:
        try:
            raw, expected_ids, body = _load_players_packet(captures, capture_id)
        except RepresentationParseError as exc:
            capture_reports.append(
                {'capture_id': capture_id, 'status': 'error', 'reason': exc.reason})
            all_captures_reconciled = False
            continue
        except Exception:
            capture_reports.append(
                {'capture_id': capture_id, 'status': 'error', 'reason': 'capture_load_failed'})
            all_captures_reconciled = False
            continue

        try:
            canonical_ids = tuple(canonical_player_id(value) for value in expected_ids)
        except RepresentationParseError:
            capture_reports.append({
                'capture_id': capture_id,
                'status': 'error',
                'reason': 'invalid_request_identity',
            })
            all_captures_reconciled = False
            continue

        requested_player_ids.update(canonical_ids)
        lineage = {
            'source_url': raw.url,
            'raw_capture_id': capture_id,
            'source_body_hash': raw.content_hash,
            'source_endpoint': raw.endpoint,
            'scope_id': raw.scope_id,
            'cycle_id': raw.cycle_id,
            'captured_at': raw.fetched_at,
            'observed_at': raw.fetched_at,
        }

        try:
            try:
                payload = json.loads(body.decode('utf-8'))
            except ValueError:
                raise RepresentationParseError('invalid_utf8_json') from None
            assertions = parse_representation_packet(payload, expected_ids=expected_ids)
        except RepresentationParseError as exc:
            for player_id in canonical_ids:
                observations.append(RepresentationObservation(
                    player_id=player_id,
                    relation_type=RELATION_TYPE,
                    representative_key='status:error',
                    source_capture_id=capture_id,
                    observation_status='error',
                    status_reason=exc.reason,
                    assessment_manifest_id=assessment_manifest_id,
                    universe_version=universe.universe_version,
                    run_id=run_id,
                    extracted_at=extracted_at_iso,
                    evidence_type='tmapi_consultant_agency',
                    evidence_locator='attributes.consultantAgencyId',
                    **lineage,
                ))
                expected_groups.add((player_id, RELATION_TYPE, capture_id))
            capture_reports.append(
                {'capture_id': capture_id, 'status': 'error', 'reason': exc.reason})
            all_captures_reconciled = False
            continue

        source_rows = payload.get('data') if isinstance(payload, Mapping) else None
        observed_count = 0
        for player_id in canonical_ids:
            assertion = assertions[player_id]
            if assertion.observation_status == 'assigned':
                representative_key = f'agency:{assertion.representative_id}'
            else:
                representative_key = f'status:{assertion.observation_status}'
            observations.append(RepresentationObservation(
                player_id=assertion.player_id,
                relation_type=RELATION_TYPE,
                representative_key=representative_key,
                source_capture_id=capture_id,
                observation_status=assertion.observation_status,
                status_reason=assertion.status_reason,
                assessment_manifest_id=assessment_manifest_id,
                universe_version=universe.universe_version,
                run_id=run_id,
                extracted_at=extracted_at_iso,
                evidence_type=assertion.evidence_type,
                evidence_locator=assertion.evidence_locator,
                representative_id=assertion.representative_id,
                **lineage,
            ))
            expected_groups.add((assertion.player_id, RELATION_TYPE, capture_id))
            observed_count += 1
        capture_reports.append({
            'capture_id': capture_id,
            'status': 'reconciled',
            'expected_count': len(expected_ids),
            'observed_count': observed_count,
            'source_row_count': len(source_rows) if isinstance(source_rows, list) else 0,
        })

    if all_captures_reconciled:
        for player in universe.players:
            if player.player_id in requested_player_ids:
                continue
            source_capture_id = 'missing:' + hashlib.sha256('|'.join((
                assessment_manifest_id, player.player_id, RELATION_TYPE,
            )).encode('utf-8')).hexdigest()
            observations.append(RepresentationObservation(
                player_id=player.player_id,
                relation_type=RELATION_TYPE,
                representative_key='status:missing_packet',
                source_capture_id=source_capture_id,
                observation_status='missing_packet',
                status_reason='missing_packet',
                assessment_manifest_id=assessment_manifest_id,
                universe_version=universe.universe_version,
                run_id=run_id,
                extracted_at=extracted_at_iso,
                evidence_type='universe_assessment',
                evidence_locator='universe.players',
            ))
            expected_groups.add((player.player_id, RELATION_TYPE, source_capture_id))

    return RepresentationExtraction(
        universe=universe,
        run_id=run_id,
        extracted_at=extracted_at_iso,
        observations=tuple(sorted(observations, key=lambda item: item.natural_key)),
        capture_reports=tuple(capture_reports),
        expected_groups=tuple(sorted(expected_groups)),
    )


def build_representation_dq(
    extraction: RepresentationExtraction,
    *,
    verified_receipt: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    max_age: timedelta = timedelta(hours=24),
) -> dict[str, Any]:
    """Deterministic, JSON-safe offline DQ report for one representation extraction.

    Every denominator is derived from the universe and the extraction itself;
    no counter is hardcoded and no capture store is consulted. ``passed`` and
    ``structural_passed`` describe structural integrity only, never production
    freshness or write SLOs.
    """
    if now is not None and (not isinstance(now, datetime)
                            or now.tzinfo is None or now.utcoffset() is None):
        raise RepresentationParseError('now must be a timezone-aware datetime')
    if not isinstance(max_age, timedelta) or max_age <= timedelta(0):
        raise RepresentationParseError('max_age must be a positive timedelta')

    universe = extraction.universe
    rows = extraction.rows
    observations = extraction.observations
    universe_players = {player.player_id for player in universe.players}
    universe_capture_ids = set(universe.capture_ids)
    expected_groups = {tuple(group) for group in extraction.expected_groups}
    manifest_id = universe.assessment_manifest_id
    batch_id = extraction.batch_id
    errors: set[str] = set()

    capture_reports = sorted(
        (dict(report) if isinstance(report, Mapping) else {}
         for report in extraction.capture_reports),
        key=lambda report: str(report.get('capture_id', '')),
    )
    capture_status_counts: dict[str, int] = {}
    report_by_capture: dict[str, Mapping[str, Any]] = {}
    for report in capture_reports:
        status = report.get('status')
        if isinstance(status, str) and status:
            capture_status_counts[status] = capture_status_counts.get(status, 0) + 1
        capture_id = report.get('capture_id')
        if not isinstance(capture_id, str) or not capture_id:
            errors.add('invalid_capture_report')
        elif capture_id in report_by_capture:
            errors.add('duplicate_capture_report')
        else:
            report_by_capture[capture_id] = report
        if status == 'error':
            errors.add('capture_report_error')
        elif (status == 'reconciled'
              and report.get('observed_count') != report.get('expected_count')):
            errors.add('capture_reconciliation_mismatch')
    reconciled_captures = {
        capture_id for capture_id, report in report_by_capture.items()
        if report.get('status') == 'reconciled'
    }
    if universe_capture_ids - set(report_by_capture):
        errors.add('missing_capture_report')
    if set(report_by_capture) - universe_capture_ids:
        errors.add('unexpected_capture_report')

    unique_rows: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        if row.get('_batch_id') != batch_id or row.get('_ingested_at') != extraction.extracted_at:
            errors.add('row_batch_mismatch')
        key = tuple(row[column] for column in REPRESENTATION_KEY)
        previous = unique_rows.get(key)
        if previous is None:
            unique_rows[key] = row
        elif previous != row:
            errors.add('conflicting_natural_key')
        else:
            errors.add('duplicate_natural_key')
    status_counts: dict[str, int] = {}
    for row in unique_rows.values():
        status = row['observation_status']
        status_counts[status] = status_counts.get(status, 0) + 1

    observations_by_group: dict[tuple[str, ...], list[RepresentationObservation]] = {}
    for observation in observations:
        if (observation.assessment_manifest_id != manifest_id
                or observation.universe_version != universe.universe_version
                or observation.run_id != extraction.run_id
                or observation.extracted_at != extraction.extracted_at):
            errors.add('observation_association_mismatch')
        if observation.observation_status == 'missing_packet':
            if any(getattr(observation, field) is not None
                   for field in (*observation._CAPTURE_FIELDS, 'captured_at', 'observed_at')):
                errors.add('capture_fields_present_for_missing')
        elif any(not isinstance(getattr(observation, field), str)
                 or not getattr(observation, field)
                 for field in observation._CAPTURE_FIELDS):
            errors.add('incomplete_raw_lineage')
        elif observation.captured_at is None or observation.observed_at is None:
            errors.add('incomplete_raw_lineage')
        observations_by_group.setdefault(observation.group_key, []).append(observation)

    observed_group_keys = set(observations_by_group)
    observed_players = {observation.player_id for observation in observations}
    if expected_groups - observed_group_keys:
        errors.add('missing_expected_group')
    if observed_group_keys - expected_groups:
        errors.add('unexpected_group')
    if observed_players - universe_players:
        errors.add('unexpected_player')

    for members in observations_by_group.values():
        if len({member.observation_status for member in members}) > 1:
            errors.add('contradictory_group_status')
        representative_ids_by_locator: dict[str, set[str | None]] = {}
        for member in members:
            if member.observation_status != 'assigned':
                continue
            representative_ids_by_locator.setdefault(
                member.evidence_locator, set()).add(member.representative_id)
        if any(len(ids) > 1 for ids in representative_ids_by_locator.values()):
            errors.add('ambiguous_representative')

    expected_groups_by_player: dict[str, set[tuple[str, ...]]] = {}
    for group in expected_groups:
        expected_groups_by_player.setdefault(group[0], set()).add(group)
    unexpected_groups_by_player: dict[str, int] = {}
    for group in observed_group_keys - expected_groups:
        unexpected_groups_by_player[group[0]] = unexpected_groups_by_player.get(group[0], 0) + 1

    def player_expected_groups(player_id: str) -> set[tuple[str, ...]]:
        return expected_groups_by_player.get(player_id, set())

    def group_is_conclusive(group: tuple[str, ...]) -> bool:
        if group not in expected_groups:
            return False
        members = observations_by_group.get(group)
        if not members:
            return False
        for member in members:
            if member.observation_status not in CONCLUSIVE_STATUSES:
                return False
            if (member.observation_status == 'explicit_none'
                    and member.evidence_type != 'player_profile_explicit_none'):
                return False
        return True

    conclusive_player_ids: set[str] = set()
    packet_player_count = 0
    for player_id in universe_players:
        groups = player_expected_groups(player_id)
        if not groups or player_id in unexpected_groups_by_player:
            continue
        if not all(group in observed_group_keys for group in groups):
            continue
        if all(group_is_conclusive(group) for group in groups):
            conclusive_player_ids.add(player_id)
        if all(not group[2].startswith('missing:') and group[2] in reconciled_captures
               for group in groups):
            packet_player_count += 1

    group_status_counts: dict[str, int] = {}
    for group in expected_groups:
        members = observations_by_group.get(group)
        if not members:
            classification = 'unobserved'
        else:
            statuses = sorted({member.observation_status for member in members})
            classification = statuses[0] if len(statuses) == 1 else 'mixed'
        group_status_counts[classification] = group_status_counts.get(classification, 0) + 1
    if unexpected_groups_by_player:
        group_status_counts['unexpected'] = sum(unexpected_groups_by_player.values())
    group_status_counts = dict(sorted(group_status_counts.items()))

    unresolved_group_keys = [group for group in expected_groups | observed_group_keys
                             if not group_is_conclusive(group)]
    unresolved_reasons: dict[str, dict[str, int]] = {}
    for group in unresolved_group_keys:
        for member in observations_by_group.get(group, ()):
            reasons = unresolved_reasons.setdefault(member.observation_status, {})
            reasons[member.status_reason] = reasons.get(member.status_reason, 0) + 1
    for capture_id in sorted(report_by_capture):
        if report_by_capture[capture_id].get('status') != 'error':
            continue
        reason = report_by_capture[capture_id].get('reason')
        if not isinstance(reason, str) or not reason:
            reason = 'unspecified'
        reasons = unresolved_reasons.setdefault('capture_error', {})
        reasons[reason] = reasons.get(reason, 0) + 1
    unresolved_reasons = {status: dict(sorted(reasons.items()))
                          for status, reasons in sorted(unresolved_reasons.items())}

    expected_player_count = len(universe_players)
    if expected_player_count == 0:
        errors.add('empty_universe')
    conclusive_count = len(conclusive_player_ids)
    packet_coverage = packet_player_count / expected_player_count if expected_player_count else None
    conclusive_coverage = conclusive_count / expected_player_count if expected_player_count else None

    receipt_diagnostics: list[str] = []
    verified_write_coverage: float | None = None
    if expected_player_count:
        verified_write_coverage = 0.0
        if verified_receipt is None:
            receipt_diagnostics.append('receipt_absent')
        elif not isinstance(verified_receipt, Mapping):
            receipt_diagnostics.append('receipt_not_a_mapping')
        else:
            def receipt_check(condition: bool, code: str) -> None:
                if not condition:
                    receipt_diagnostics.append(code)
            receipt_check(verified_receipt.get('verified') is True, 'receipt_not_verified')
            receipt_check(verified_receipt.get('receipt_readback') is True, 'receipt_readback_missing')
            receipt_check(verified_receipt.get('target_table') == REPRESENTATION_TABLE,
                          'receipt_target_mismatch')
            receipt_check(verified_receipt.get('plan_sha256') == representation_plan_sha256(extraction),
                          'receipt_plan_sha256_mismatch')
            receipt_check(verified_receipt.get('assessment_manifest_id') == manifest_id,
                          'receipt_manifest_mismatch')
            receipt_check(verified_receipt.get('universe_version') == universe.universe_version,
                          'receipt_universe_version_mismatch')
            receipt_check(verified_receipt.get('run_id') == extraction.run_id,
                          'receipt_run_mismatch')
            receipt_check(verified_receipt.get('committed_batch') == batch_id,
                          'receipt_batch_mismatch')
            receipt_check(verified_receipt.get('expected_players') == expected_player_count,
                          'receipt_expected_players_mismatch')
            receipt_check(verified_receipt.get('expected_groups') == len(expected_groups),
                          'receipt_expected_groups_mismatch')
            receipt_check(verified_receipt.get('row_count') == len(rows),
                          'receipt_row_count_mismatch')
            key_columns = verified_receipt.get('natural_key_columns',
                                               verified_receipt.get('key_columns'))
            if not isinstance(key_columns, (list, tuple)) or tuple(key_columns) != REPRESENTATION_KEY:
                receipt_diagnostics.append('receipt_natural_key_mismatch')
            for field, code in (('plan_sha256', 'receipt_plan_sha256_missing'),
                                ('physical_hash', 'receipt_physical_hash_missing'),
                                ('committed_at', 'receipt_committed_at_missing')):
                value = verified_receipt.get(field)
                receipt_check(isinstance(value, str) and bool(value), code)
            if not receipt_diagnostics:
                verified_write_coverage = 1.0

    fresh_conclusive_coverage: float | None = None
    freshness = 'not_evaluated'
    if now is not None:
        freshness = 'evaluated'
        if expected_player_count:
            now_utc = now.astimezone(timezone.utc)
            window_start = now_utc - max_age
            fresh_player_count = 0
            for player_id in conclusive_player_ids:
                is_fresh = True
                for group in player_expected_groups(player_id):
                    for member in observations_by_group.get(group, ()):
                        captured_at = member.captured_at
                        if not isinstance(captured_at, str) or not captured_at:
                            is_fresh = False
                            break
                        try:
                            captured_moment = datetime.fromisoformat(captured_at)
                        except ValueError:
                            is_fresh = False
                            break
                        if captured_moment.tzinfo is None or captured_moment.utcoffset() is None:
                            is_fresh = False
                            break
                        captured_moment = captured_moment.astimezone(timezone.utc)
                        if not window_start <= captured_moment <= now_utc:
                            is_fresh = False
                            break
                    if not is_fresh:
                        break
                if is_fresh:
                    fresh_player_count += 1
            fresh_conclusive_coverage = fresh_player_count / expected_player_count

    structural_passed = expected_player_count > 0 and not errors
    return {
        'passed': structural_passed,
        'structural_passed': structural_passed,
        'expected_players': expected_player_count,
        'expected_groups': len(expected_groups),
        'observation_rows': len(rows),
        'observed_players': len(observed_players),
        'observed_groups': len(observed_group_keys),
        'status_counts': dict(sorted(status_counts.items())),
        'group_status_counts': group_status_counts,
        'capture_status_counts': dict(sorted(capture_status_counts.items())),
        'packet_coverage': packet_coverage,
        'verified_write_coverage': verified_write_coverage,
        'receipt_diagnostics': receipt_diagnostics,
        'conclusive_coverage': conclusive_coverage,
        'fresh_conclusive_coverage': fresh_conclusive_coverage,
        'freshness': freshness,
        'unresolved_players': expected_player_count - conclusive_count,
        'unresolved_groups': len(unresolved_group_keys),
        'unresolved_reasons': unresolved_reasons,
        'errors': sorted(errors),
        'capture_reports': capture_reports,
    }
