"""Turn Transfermarkt captures into replayable Bronze observations.

Each lane records what a single source page establishes for its scope. Keeping
the capture lineage and evidence for empty results here lets DQ and write
planning operate on the same observations without contacting Transfermarkt.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import parse_qs, urlsplit

from scrapers.transfermarkt import parsing


CORE_PARSER_REVISION = 'core-v1'
CORE_SCHEMA_REVISION = '1'
CORE_STATUSES = frozenset({
    'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
    'unknown', 'unavailable', 'error',
})
CONCLUSIVE_CORE_STATUSES = frozenset({
    'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
})
_SEASON_RE = re.compile(r'^\d{4}$')
_ID_RE = re.compile(r'^[1-9][0-9]*$')
_CAPTURE_RE = re.compile(r'^[0-9a-f]{64}$')
_REFEREE_DIRECTORY_PATH = re.compile(
    r'/[A-Za-z0-9_-]+/schiedsrichter/(?:wettbewerb|pokalwettbewerb)/'
    r'([A-Za-z0-9]+)/saison_id/([0-9]{4})(?:/plus/1)?(?:/page/[0-9]+)?'
)
_ENDPOINTS = frozenset({
    'squad', 'market_value_points', 'transfer_events', 'coach_profile', 'coach_history',
    # stage C sections (#2405)
    'attendance', 'club_values', 'player_achievement', 'club_achievement', 'coach_achievement',
    'rumours', 'referee_directory', 'referee_profile', 'representation',
})
_OUTCOMES_WITH_ROWS = frozenset({'observed_present', 'explicit_none'})
_PROOF_STATUSES = frozenset({'observed_empty', 'not_applicable'})
_CONTRACT_STATUSES = {
    'ok': 'observed_present', 'unknown': 'unknown', 'parse_error': 'error',
    'not_rendered': 'not_applicable',
}
# Sections preserve the donor parser's verdict for each row.
_ENTITY_ROW_STATUSES = {
    'player_contract_observations': frozenset(_CONTRACT_STATUSES.values()),
    'attendance_observations': frozenset({'observed_present', 'explicit_none', 'unknown'}),
    'club_value_observations': frozenset({'observed_present', 'explicit_none', 'unknown'}),
    'referee_aggregate_observations': frozenset({'observed_present', 'observed_empty', 'explicit_none'}),
    'player_representation_observations': frozenset({'observed_present', 'explicit_none', 'unknown'}),
}
# Lanes whose outcome verdict summarises row PRESENCE (the donors' summarise
# rule). The other row-bearing section lanes are page containers: a referee
# page outcome certifies the page's entries while its rows carry metric
# verdicts, so a page of dashes is still an observed page.
_PRESENCE_ENTITIES = frozenset({
    'attendance_observations', 'club_value_observations',
    'player_representation_observations',
})

LINEAGE_COLUMNS = (
    'source_url', 'source_endpoint', 'raw_capture_id', 'source_capture_id',
    'source_body_hash', 'scope_id', 'cycle_id', 'run_id', 'universe_version',
    'captured_at', 'observed_at', 'extracted_at', 'parser_revision',
    'schema_revision', 'observation_status', 'status_reason',
)
META_COLUMNS = ('_source', '_entity_type', '_batch_id', '_ingested_at')

CORE_ENTITIES = (
    'squad_memberships', 'player_attribute_observations', 'player_contract_observations',
    'market_value_points', 'transfer_events', 'coach_profiles', 'coach_stints',
    # stage C sections (#2405)
    'attendance_observations', 'club_value_observations', 'achievement_observations',
    'rumour_observations', 'referee_aggregate_observations', 'referee_profiles',
    'player_representation_observations',
)
CORE_TABLES = {entity: f'transfermarkt_{entity}' for entity in CORE_ENTITIES}
CORE_NATURAL_KEYS = {
    'squad_memberships': ('league', 'season', 'club_id', 'player_id'),
    'player_attribute_observations': ('league', 'season', 'club_id', 'player_id', 'observed_at'),
    'player_contract_observations': ('league', 'season', 'club_id', 'player_id', 'observed_at'),
    'market_value_points': ('player_id', 'mv_date'),
    'transfer_events': ('transfer_id',),
    'coach_profiles': ('coach_id', 'observed_at'),
    'coach_stints': ('stint_id',),
    # stage C sections (#2405)
    'attendance_observations': ('competition_id', 'edition_id', 'club_id', 'observed_at'),
    'club_value_observations': ('club_id', 'value_date', 'observed_at', 'source_point_id'),
    'achievement_observations': ('recipient_type', 'recipient_id', 'source_capture_id', 'source_row_id'),
    'rumour_observations': ('rumour_identity', 'subject_id', 'observed_at', 'correction_identity'),
    'referee_aggregate_observations': ('referee_id', 'competition_id', 'edition_id', 'metric', 'observed_at'),
    'referee_profiles': ('referee_id', 'observed_at'),
    'player_representation_observations': (
        'player_id', 'relation_type', 'representative_key', 'source_capture_id',
    ),
}
CORE_SCOPE_COLUMNS = {
    'squad_memberships': ('league', 'season', 'club_id'),
    'player_attribute_observations': ('league', 'season', 'club_id'),
    'player_contract_observations': ('league', 'season', 'club_id'),
    'market_value_points': ('player_id',),
    'transfer_events': ('player_id',),
    'coach_profiles': ('coach_id',),
    'coach_stints': ('club_id',),
    # stage C sections (#2405)
    'attendance_observations': ('competition_id', 'edition_id'),
    'club_value_observations': ('club_id',),
    'achievement_observations': ('recipient_type', 'recipient_id'),
    'rumour_observations': ('competition_id',),
    'referee_aggregate_observations': ('competition_id', 'edition_id'),
    'referee_profiles': ('referee_id',),
    'player_representation_observations': ('player_id',),
}
CORE_WRITE_MODES = {
    'squad_memberships': 'replace',
    'player_attribute_observations': 'merge',
    'player_contract_observations': 'merge',
    'market_value_points': 'merge',
    'transfer_events': 'merge',
    'coach_profiles': 'merge',
    'coach_stints': 'replace',
    # stage C sections (#2405)
    'attendance_observations': 'replace',
    'club_value_observations': 'merge',
    'achievement_observations': 'merge',
    'rumour_observations': 'merge',
    'referee_aggregate_observations': 'merge',
    'referee_profiles': 'merge',
    'player_representation_observations': 'merge',
}

SQUAD_MEMBERSHIP_BUSINESS = (
    'league', 'season', 'club_id', 'club_name', 'player_id', 'player_slug',
    'player_name', 'observed_at', 'shirt_number', 'joined_at', 'signed_from_club_id',
    'signed_from_club_name', 'signed_from_fee_text', 'signed_from_fee_eur', 'is_on_loan',
    'loan_from_club_id', 'loan_from_club_name', 'loan_until', 'is_captain',
    'injury_text', 'injury_return_date', 'nationality_secondary',
)
ATTRIBUTE_BUSINESS = (
    'league', 'season', 'club_id', 'club_name', 'player_id', 'player_slug',
    'player_name', 'position', 'dob', 'age', 'height_cm', 'foot', 'nationality',
    'market_value_eur', 'observed_at',
)
CONTRACT_BUSINESS = (
    'league', 'season', 'club_id', 'player_id', 'contract_until', 'contract_status',
    'observed_at',
)
MARKET_VALUE_POINT_BUSINESS = (
    'player_id', 'mv_date', 'value_eur', 'is_value_removed', 'club_id',
    'mv_last_change', 'club_name', 'age', 'mv_raw',
)
TRANSFER_EVENT_BUSINESS = (
    'transfer_id', 'player_id', 'transfer_date', 'event_season', 'from_club_id',
    'from_club_name', 'to_club_id', 'to_club_name', 'from_is_special', 'to_is_special',
    'from_country_flag', 'to_country_flag', 'from_country_id', 'to_country_id',
    'is_loan_end', 'fee_text', 'fee_eur', 'market_value_eur', 'is_upcoming',
)
COACH_PROFILE_BUSINESS = ('coach_id', 'name', 'dob', 'nationality', 'observed_at')
COACH_STINT_BUSINESS = (
    'stint_id', 'club_id', 'coach_id', 'coach_slug', 'name', 'role',
    'appointed_date', 'left_date', 'observed_at',
)
ATTENDANCE_BUSINESS = (
    'competition_id', 'edition_id', 'club_id', 'observed_at', 'club_name',
    'stadium_name', 'stadium_url', 'capacity', 'capacity_raw', 'spectators_total',
    'spectators_total_raw', 'spectators_average', 'spectators_average_raw',
    'matches', 'matches_raw', 'sold_out', 'sold_out_raw', 'capacity_filled_raw',
    'final_season', 'final_season_proof',
)
CLUB_VALUE_BUSINESS = (
    'competition_id', 'club_id', 'club_slug', 'club_name', 'value_date', 'currency',
    'value_amount', 'source_point_id', 'source_point_kind', 'source_value_raw',
    'reconciliation_status', 'observed_at',
)
ACHIEVEMENT_BUSINESS = (
    'recipient_type', 'recipient_id', 'title', 'award_id', 'trophy_id', 'season_id',
    'season', 'result', 'club_id', 'club_name', 'competition_id', 'source_row_id',
)
RUMOUR_BUSINESS = (
    'competition_id', 'page_number', 'player_id', 'player_name', 'player_url', 'subject_id',
    'interested_club_id', 'interested_club_name', 'interested_club_url',
    'interested_club_competition_id', 'interested_club_competition_name',
    'interested_club_competition_url', 'current_club_id', 'current_club_name',
    'current_club_url', 'position', 'nationalities_json', 'age', 'last_source_date',
    'last_reply_date', 'probability_percent', 'thread_link', 'rumour_identity',
    'content_hash', 'correction_identity', 'observation_key', 'assertion_json',
    'source_page_url', 'observed_at',
)
REFEREE_AGGREGATE_BUSINESS = (
    'referee_id', 'competition_id', 'edition_id', 'metric', 'value', 'observation_status',
    'status_reason', 'raw_value', 'source_row', 'source_column', 'observed_at',
)
REFEREE_PROFILE_BUSINESS = ('referee_id', 'name', 'nationality', 'birth_date', 'observed_at')
REPRESENTATION_BUSINESS = (
    'player_id', 'relation_type', 'representative_key', 'representative_id', 'observation_status',
    'status_reason', 'evidence_type', 'evidence_locator', 'observed_at',
)
_BUSINESS_COLUMNS = {
    'squad_memberships': SQUAD_MEMBERSHIP_BUSINESS,
    'player_attribute_observations': ATTRIBUTE_BUSINESS,
    'player_contract_observations': CONTRACT_BUSINESS,
    'market_value_points': MARKET_VALUE_POINT_BUSINESS,
    'transfer_events': TRANSFER_EVENT_BUSINESS,
    'coach_profiles': COACH_PROFILE_BUSINESS,
    'coach_stints': COACH_STINT_BUSINESS,
    # stage C sections (#2405)
    'attendance_observations': ATTENDANCE_BUSINESS,
    'club_value_observations': CLUB_VALUE_BUSINESS,
    'achievement_observations': ACHIEVEMENT_BUSINESS,
    'rumour_observations': RUMOUR_BUSINESS,
    'referee_aggregate_observations': REFEREE_AGGREGATE_BUSINESS,
    'referee_profiles': REFEREE_PROFILE_BUSINESS,
    'player_representation_observations': REPRESENTATION_BUSINESS,
}
# observed_at serves both roles in the squad and coach tables.
CORE_COLUMNS = {
    entity: tuple(dict.fromkeys(business + LINEAGE_COLUMNS + META_COLUMNS))
    for entity, business in _BUSINESS_COLUMNS.items()
}
COLUMN_TYPES: Mapping[str, str] = {
    **dict.fromkeys((col for columns in CORE_COLUMNS.values() for col in columns), 'varchar'),
    **dict.fromkeys(('observed_at', 'captured_at', 'extracted_at', '_ingested_at'), 'timestamp'),
    **dict.fromkeys((
        'dob', 'joined_at', 'loan_until', 'injury_return_date', 'contract_until', 'mv_date',
        'mv_last_change', 'transfer_date', 'appointed_date', 'left_date', 'value_date', 'birth_date',
    ), 'date'),
    **dict.fromkeys((
        'age', 'height_cm', 'market_value_eur', 'shirt_number', 'signed_from_fee_eur',
        'value_eur', 'fee_eur', 'from_country_id', 'to_country_id',
        'capacity', 'spectators_total', 'spectators_average', 'matches', 'sold_out', 'value_amount',
        'page_number', 'source_row',
    ), 'bigint'),
    **dict.fromkeys((
        'is_on_loan', 'is_captain', 'is_value_removed', 'is_upcoming',
        'from_is_special', 'to_is_special', 'is_loan_end', 'final_season',
    ), 'boolean'),
}


class CoreParseError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _require_id(value: object, label: str) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise CoreParseError(f'invalid_{label}')
    return value


def _iso_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def canonical_timestamp(value: str) -> str:
    """Validate a tz-aware ISO string already in canonical UTC form.

    Microseconds are mandatory but may be zero: ``.000000`` must survive the
    round trip, while a timestamp without a fraction is not canonical.
    """
    try:
        moment = datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise CoreParseError('timestamp_not_canonical') from exc
    if (moment.tzinfo is None or not value.endswith('+00:00')
            or moment.isoformat(timespec='microseconds') != value):
        raise CoreParseError('timestamp_not_canonical')
    return value


@dataclass(frozen=True)
class SourceCapture:
    url: str
    endpoint: str
    body: bytes
    capture_id: str
    body_hash: str
    captured_at: str
    status_code: int = 200

    def __post_init__(self) -> None:
        if not _nonempty(self.url) or any(ch.isspace() for ch in self.url):
            raise CoreParseError('invalid_source_url')
        try:
            url = urlsplit(self.url)
            if url.scheme != 'https' or not url.hostname:
                raise ValueError
        except ValueError as exc:
            raise CoreParseError('invalid_source_url') from exc
        if not isinstance(self.endpoint, str) or self.endpoint not in _ENDPOINTS:
            raise CoreParseError('invalid_endpoint')
        if not isinstance(self.body, bytes):
            raise CoreParseError('invalid_body')
        if not isinstance(self.capture_id, str) or not _CAPTURE_RE.fullmatch(self.capture_id):
            raise CoreParseError('invalid_capture_id')
        if hashlib.sha256(self.body).hexdigest() != self.body_hash:
            raise CoreParseError('body_hash_mismatch')
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise CoreParseError('invalid_status_code')
        canonical_timestamp(self.captured_at)

    @property
    def text(self) -> str:
        return self.body.decode('utf-8')


@dataclass(frozen=True)
class ScopeContext:
    league: str
    season: str
    scope_id: str
    cycle_id: str
    club_id: str | None = None
    club_name: str | None = None
    player_id: str | None = None
    edition_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.league, str) or not re.fullmatch(r'[A-Z0-9]{2,6}', self.league):
            raise CoreParseError('invalid_league')
        if not isinstance(self.season, str) or not _SEASON_RE.fullmatch(self.season):
            raise CoreParseError('invalid_season')
        for field in ('scope_id', 'cycle_id'):
            if not _nonempty(getattr(self, field)):
                raise CoreParseError(f'invalid_{field}')
        for field in ('club_id', 'player_id'):
            value = getattr(self, field)
            if value is not None and (not isinstance(value, str) or not _ID_RE.fullmatch(value)):
                raise CoreParseError(f'invalid_{field}')
        if self.edition_id is not None and (
            not isinstance(self.edition_id, str) or not re.fullmatch(r'\d{1,4}', self.edition_id)
        ):
            raise CoreParseError('invalid_edition_id')


def _run_id(scope: ScopeContext, extracted_at: str) -> str:
    identity = {'scope_id': scope.scope_id, 'cycle_id': scope.cycle_id, 'extracted_at': extracted_at}
    return 'tm-core-' + hashlib.sha256(canonical_json(identity)).hexdigest()[:32]


def _batch_id(run_id: str) -> str:
    return 'tm-core-batch-' + hashlib.sha256(run_id.encode('utf-8')).hexdigest()[:32]


def _status_matches_rows(entity, status, rows) -> bool:
    if status in _PROOF_STATUSES:
        # A proof certifies an absence; no lane carries rows under it.
        return not rows
    if not rows:
        return status not in _OUTCOMES_WITH_ROWS
    allowed = _ENTITY_ROW_STATUSES.get(entity)
    contract = entity == 'player_contract_observations'
    if status not in _OUTCOMES_WITH_ROWS and (contract or allowed is None or status not in allowed):
        return False
    if allowed is None:
        return all(row.get('observation_status') == status for row in rows)
    verdicts = []
    for row in rows:
        verdict = row.get('observation_status')
        if not isinstance(verdict, str) or verdict not in allowed:
            return False
        verdicts.append(verdict)
        if (contract and status == 'observed_present' and verdict != status
                and verdict != _CONTRACT_STATUSES.get(row.get('contract_status'))):
            return False
    # A populated squad page carries each contract cell's own verdict.
    if contract and status == 'observed_present':
        return True
    # The donors' summarise rule for the aggregate verdict.
    if status == 'explicit_none':
        return all(verdict == 'explicit_none' for verdict in verdicts)
    if status == 'observed_present':
        return entity not in _PRESENCE_ENTITIES or 'observed_present' in verdicts
    return 'observed_present' not in verdicts


@dataclass(frozen=True)
class LaneOutcome:
    entity: str
    status: str
    status_reason: str
    rows: tuple[dict, ...] = ()
    capture: SourceCapture | None = None
    scope: ScopeContext | None = None
    extracted_at: str = ''
    run_id: str = ''
    universe_version: str = ''
    proof: str | None = None

    def __post_init__(self) -> None:
        if self.entity not in CORE_ENTITIES:
            raise CoreParseError('invalid_entity')
        if not isinstance(self.status, str) or self.status not in CORE_STATUSES:
            raise CoreParseError('invalid_status')
        if not _nonempty(self.status_reason):
            raise CoreParseError('invalid_status_reason')
        if self.status in _PROOF_STATUSES:
            if not _nonempty(self.proof):
                raise CoreParseError('missing_proof')
        elif self.proof is not None:
            raise CoreParseError('unexpected_proof')
        if not _status_matches_rows(self.entity, self.status, self.rows):
            raise CoreParseError('status_rows_mismatch')
        if self.capture is not None and (
            not isinstance(self.scope, ScopeContext) or not all(map(_nonempty, (
                self.extracted_at, self.run_id, self.universe_version,
            )))
        ):
            raise CoreParseError('incomplete_lineage')


def _render(
    entity: str, values: Mapping, scope: ScopeContext, capture: SourceCapture,
    extracted_at: str, run_id: str, universe_version: str, observation_status: str,
    status_reason: str, *, parser_revision: str = CORE_PARSER_REVISION,
    schema_revision: str = CORE_SCHEMA_REVISION,
) -> dict:
    row = {}
    for col in _BUSINESS_COLUMNS[entity]:
        value = values[col]
        if COLUMN_TYPES[col] == 'date' and isinstance(value, date):
            value = _iso_date(value)
        elif COLUMN_TYPES[col] == 'bigint' and isinstance(value, str) and value.isdigit():
            # Parser helpers sometimes hand numeric ids as strings; the
            # warehouse column is bigint and the physical read-back returns
            # an int, so the plan must carry the int form.
            value = int(value)
        row[col] = value
    row.update(
        source_url=capture.url, source_endpoint=capture.endpoint,
        raw_capture_id=capture.capture_id, source_capture_id=capture.capture_id,
        source_body_hash=capture.body_hash,
        scope_id=scope.scope_id, cycle_id=scope.cycle_id, run_id=run_id,
        universe_version=universe_version, captured_at=capture.captured_at,
        observed_at=capture.captured_at, extracted_at=extracted_at,
        parser_revision=parser_revision, schema_revision=schema_revision,
        observation_status=observation_status, status_reason=status_reason,
        _source='transfermarkt', _entity_type=entity, _batch_id=_batch_id(run_id),
        _ingested_at=extracted_at,
    )
    if any(value is not None and not isinstance(value, (str, int, bool)) for value in row.values()):
        raise CoreParseError('row_value_not_json_safe')
    return row


def _context(
    capture: SourceCapture, scope: ScopeContext, extracted_at: str, universe_version: str,
) -> dict:
    canonical_timestamp(extracted_at)
    if not _nonempty(universe_version):
        raise CoreParseError('invalid_universe_version')
    return dict(capture=capture, scope=scope, extracted_at=extracted_at,
                run_id=_run_id(scope, extracted_at), universe_version=universe_version)


# The parsers consume untrusted dates, numbers, nested JSON, and HTML.
_SOURCE_ERRORS = (
    ValueError, TypeError, KeyError, IndexError, AttributeError, OverflowError, OSError, RecursionError,
)


def extract_squad_observations(
    capture: SourceCapture, scope: ScopeContext, *, extracted_at: str, universe_version: str,
) -> tuple[LaneOutcome, ...]:
    from bs4 import BeautifulSoup

    if capture.endpoint != 'squad' or scope.club_id is None:
        raise CoreParseError('squad_requires_club_scope')
    context = _context(capture, scope, extracted_at, universe_version)
    entities = ('squad_memberships', 'player_attribute_observations', 'player_contract_observations')

    def page_outcomes(status: str, reason: str, proof: str | None = None) -> tuple[LaneOutcome, ...]:
        return tuple(LaneOutcome(entity, status, reason, proof=proof, **context) for entity in entities)

    try:
        html = capture.text
        soup = BeautifulSoup(html, 'html.parser')
        problem = parsing.endpoint_validator('squad', False)(html)
        empty_roster = parsing._squad_page_is_empty_roster(soup)
        if problem is not None and not empty_roster:
            return page_outcomes('error', 'schema:' + problem)
        players = parsing._parse_squad_page(html, scope.club_id)
    except _SOURCE_ERRORS:
        return page_outcomes('error', 'schema:invalid_squad_page')
    if not players:
        if not empty_roster:
            return page_outcomes('error', 'schema:unrecognized_roster')
        proof = (
            'empty_roster:squad_table_absent' if soup.select_one('table.items') is None
            else 'empty_roster:no_data_rows'
        )
        return page_outcomes('observed_empty', 'empty_roster', proof)

    by_entity = {entity: [] for entity in entities}
    for player in players:
        contract = player['_contract_status']
        values = {
            **player,
            'league': scope.league,
            'season': scope.season,
            'club_id': scope.club_id,
            'club_name': scope.club_name if scope.club_name is not None else player.get('club_name'),
            'player_id': _require_id(player.get('player_id'), 'player_id'),
            'observed_at': capture.captured_at,
            'contract_status': contract,
            'player_name': player['name'],
        }
        for entity in entities:
            if any(values[key] in (None, '') for key in CORE_NATURAL_KEYS[entity]):
                continue
            if entity == 'player_contract_observations':
                status = _CONTRACT_STATUSES[contract]
                reason = 'contract:' + contract
            else:
                status, reason = 'observed_present', 'squad_row'
            row = _render(entity, values, observation_status=status, status_reason=reason, **context)
            by_entity[entity].append(row)
    return tuple(
        LaneOutcome(entity, 'observed_present', 'squad_page', tuple(by_entity[entity]), **context)
        for entity in entities
    )


def extract_career_observations(
    capture: SourceCapture, scope: ScopeContext, *, extracted_at: str, universe_version: str,
) -> LaneOutcome:
    entity = capture.endpoint
    if entity not in ('market_value_points', 'transfer_events') or scope.player_id is None:
        raise CoreParseError('career_requires_player_scope')
    context = _context(capture, scope, extracted_at, universe_version)
    try:
        payload = json.loads(capture.text)
    except (ValueError, RecursionError):
        return LaneOutcome(entity, 'error', 'invalid_json', **context)
    try:
        problem = parsing.endpoint_validator(entity, True)(payload)
        if problem is not None:
            return LaneOutcome(entity, 'error', 'schema:' + problem, **context)
        if entity == 'market_value_points':
            parsed = parsing._parse_mv_history(payload, scope.player_id)
            collection = payload.get('list')
        else:
            parsed = parsing._parse_transfers(payload, scope.player_id)
            collection = payload.get('transfers')
        problem = parsing._career_collection_error(entity, scope.player_id, collection, parsed)
    except _SOURCE_ERRORS:
        return LaneOutcome(entity, 'error', 'collection:invalid_source_data', **context)
    if problem is not None:
        return LaneOutcome(entity, 'error', 'collection:' + problem, **context)
    if not parsed:
        return LaneOutcome(entity, 'observed_empty', 'empty_collection',
                           proof='empty_collection:' + entity, **context)
    rows = tuple(
        _render(entity, row, observation_status='observed_present',
                status_reason='career_page', **context)
        for row in parsed
    )
    return LaneOutcome(entity, 'observed_present', 'career_page', rows, **context)


def extract_coach_profile(
    capture: SourceCapture, scope: ScopeContext, *, extracted_at: str, universe_version: str,
) -> LaneOutcome:
    if capture.endpoint != 'coach_profile':
        raise CoreParseError('invalid_coach_profile_endpoint')
    match = re.search(r'/profil/trainer/(\d+)(?:/|$)', urlsplit(capture.url).path)
    if match is None:
        raise CoreParseError('coach_profile_url_has_no_id')
    coach_id = _require_id(match[1], 'coach_id')
    context = _context(capture, scope, extracted_at, universe_version)
    entity = 'coach_profiles'
    try:
        html = capture.text
        problem = parsing.endpoint_validator('coach_profile', False)(html)
        if problem is not None:
            return LaneOutcome(entity, 'error', 'schema:' + problem, **context)
        profile = parsing._parse_coach_profile(html, coach_id)
    except _SOURCE_ERRORS:
        return LaneOutcome(entity, 'error', 'schema:invalid_coach_profile', **context)
    if profile is None:
        return LaneOutcome(entity, 'error', 'profile_unrecognized', **context)
    values = {**profile, 'coach_id': coach_id, 'observed_at': capture.captured_at}
    row = _render(entity, values, observation_status='observed_present',
                  status_reason='coach_profile', **context)
    return LaneOutcome(entity, 'observed_present', 'coach_profile', (row,), **context)


def extract_coach_stints(
    capture: SourceCapture, scope: ScopeContext, *, extracted_at: str, universe_version: str,
) -> LaneOutcome:
    """Observe the club's complete manager history.

    The page always serves the full history; a season-window filter here
    would make the club-wide replacement delete every other season's rows.
    """
    if capture.endpoint != 'coach_history' or scope.club_id is None:
        raise CoreParseError('coach_history_requires_club_scope')
    context = _context(capture, scope, extracted_at, universe_version)
    entity = 'coach_stints'
    try:
        html = capture.text
        problem = parsing.endpoint_validator('coach_history', False)(html)
        if problem is not None:
            return LaneOutcome(entity, 'error', 'schema:' + problem, **context)
        stints = parsing._parse_coach_history(html, scope.club_id)
        if not stints:
            proof = ('empty_history:no_stints_on_record'
                     if parsing._coach_history_page_is_empty(html)
                     else 'empty_history:no_header_mapped_rows')
            return LaneOutcome(entity, 'observed_empty', 'empty_history', proof=proof, **context)
    except _SOURCE_ERRORS:
        return LaneOutcome(entity, 'error', 'schema:invalid_coach_history', **context)

    rows = []
    for stint in stints:
        identity = {
            'club_id': scope.club_id,
            'coach_id': _require_id(stint['coach_id'], 'coach_id'),
            'role': stint['role'],
            'appointed_date': _iso_date(stint['appointed_date']),
            'left_date': _iso_date(stint['left_date']),
        }
        values = {
            **stint, **identity,
            'stint_id': hashlib.sha256(canonical_json(identity)).hexdigest(),
            'observed_at': capture.captured_at,
        }
        rows.append(_render(entity, values, observation_status='observed_present',
                            status_reason='coach_history', **context))
    return LaneOutcome(entity, 'observed_present', 'coach_history', tuple(rows), **context)


def captureless_outcome(
    entity: str, scope: ScopeContext, *, status: str, reason: str, extracted_at: str,
    run_id: str, universe_version: str, proof: str | None = None,
) -> LaneOutcome:
    if status not in {'unknown', 'unavailable', 'error', 'not_applicable'}:
        raise CoreParseError('invalid_captureless_status')
    canonical_timestamp(extracted_at)
    if not _nonempty(run_id) or not _nonempty(universe_version):
        raise CoreParseError('incomplete_lineage')
    return LaneOutcome(entity, status, reason, proof=proof, scope=scope,
                       extracted_at=extracted_at, run_id=run_id,
                       universe_version=universe_version)


def core_ddl(entity: str) -> str:
    if entity not in CORE_ENTITIES:
        raise CoreParseError('invalid_entity')
    columns = []
    for col in CORE_COLUMNS[entity]:
        sql_type = COLUMN_TYPES[col]
        columns.append(f"    {col} {'timestamp(6)' if sql_type == 'timestamp' else sql_type}")
    definitions = ',\n'.join(columns)
    return (
        f'CREATE TABLE IF NOT EXISTS iceberg.bronze.{CORE_TABLES[entity]} (\n'
        f"{definitions}\n) WITH (format = 'PARQUET')"
    )


def plan_for_outcome(
    outcome: LaneOutcome, *, captured_at: str, scope_values: Mapping[str, str],
):
    if outcome.status not in CONCLUSIVE_CORE_STATUSES:
        raise ValueError('no plan for non-conclusive outcome')
    canonical_timestamp(captured_at)
    entity = outcome.entity
    scope_columns = CORE_SCOPE_COLUMNS[entity]
    if set(scope_values) != set(scope_columns) or not all(map(_nonempty, scope_values.values())):
        raise CoreParseError('invalid_scope_values')
    if any(row.get(col) != scope_values[col] for row in outcome.rows for col in scope_columns):
        raise CoreParseError('row_scope_mismatch')

    from scrapers.transfermarkt.native_writer import NativeWritePlan

    _check_scope_against_capture(entity, scope_values, outcome.capture)
    capture = outcome.capture
    evidence = ()
    if capture is not None:
        row = outcome.rows[0] if outcome.rows else {}
        evidence = (
            ('source_url', capture.url),
            ('source_endpoint', capture.endpoint),
            ('raw_capture_id', capture.capture_id),
            ('source_capture_id', capture.capture_id),
            ('source_body_hash', capture.body_hash),
            ('scope_id', outcome.scope.scope_id),
            ('cycle_id', outcome.scope.cycle_id),
            ('parser_revision', row.get('parser_revision', CORE_PARSER_REVISION)),
            ('schema_revision', row.get('schema_revision', CORE_SCHEMA_REVISION)),
        )
    return NativeWritePlan(
        entity=entity, table=CORE_TABLES[entity], ddl=core_ddl(entity),
        natural_keys=CORE_NATURAL_KEYS[entity], scope_columns=scope_columns,
        scope_values=tuple(scope_values[column] for column in scope_columns),
        rows=outcome.rows, captured_at=captured_at,
        write_mode=CORE_WRITE_MODES[entity],
        status=outcome.status, status_reason=outcome.status_reason,
        proof=outcome.proof or '',
        capture_id=capture.capture_id if capture is not None else '',
        evidence=evidence,
    )


def _referee_directory_identity(path: str) -> tuple[str, str]:
    match = _REFEREE_DIRECTORY_PATH.fullmatch(path)
    if match is None:
        raise CoreParseError('row_scope_mismatch')
    return match[1], match[2]


def _check_scope_against_capture(entity: str, scope_values: Mapping[str, str], capture) -> None:
    """Empty outcomes have no rows to cross-check; the capture URL still names
    the entity it belongs to, so a swapped club/player scope fails here."""
    if capture is None:
        return
    url = urlsplit(capture.url)
    path = url.path
    if entity == 'player_representation_observations':
        if (url.hostname != 'tmapi.transfermarkt.technology' or path != '/players'
                or scope_values['player_id'] not in parse_qs(url.query).get('ids[]', ())):
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'referee_aggregate_observations':
        if _referee_directory_identity(path) != (scope_values['competition_id'], scope_values['edition_id']):
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'referee_profiles':
        match = re.fullmatch(r'/-/profil/schiedsrichter/([1-9][0-9]*)', path)
        if match is None or match[1] != scope_values['referee_id']:
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'rumour_observations':
        match = re.fullmatch(r'/[A-Za-z0-9_-]+/geruechte/wettbewerb/([A-Za-z0-9_-]+)(?:/page/[0-9]+)?/?', path)
        if match is None or match[1] != scope_values['competition_id']:
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'achievement_observations':
        recipient_type = scope_values['recipient_type']
        recipient_id = scope_values['recipient_id']
        if capture.endpoint != f'{recipient_type}_achievement':
            raise CoreParseError('row_scope_mismatch')
        if recipient_type == 'coach':
            match = re.fullmatch(r'/[A-Za-z0-9-]+/erfolge/trainer/([1-9][0-9]*)', path)
            if url.hostname != 'www.transfermarkt.com' or match is None or match[1] != recipient_id:
                raise CoreParseError('row_scope_mismatch')
        else:
            match = re.fullmatch(r'/(player|club)/([1-9][0-9]*)/achievement', path)
            if (url.hostname != 'tmapi.transfermarkt.technology' or match is None
                    or match.groups() != (recipient_type, recipient_id)):
                raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'attendance_observations' and {'competition_id', 'edition_id'} <= scope_values.keys():
        match = re.fullmatch(
            r'/[A-Za-z0-9_-]+/besucherzahlen/wettbewerb/([A-Za-z0-9_-]+)/saison_id/([0-9]+)',
            path,
        )
        if match is None or match.groups() != (scope_values['competition_id'], scope_values['edition_id']):
            raise CoreParseError('row_scope_mismatch')
    for column, pattern in (
        ('club_id', r'/(?:kader|mitarbeiterhistorie)/verein/([1-9][0-9]*)'),
        ('player_id', r'/ceapi/(?:marketValueDevelopment/graph|transferHistory/list)/([1-9][0-9]*)'),
        ('coach_id', r'/profil/trainer/([1-9][0-9]*)'),
    ):
        if column not in scope_values:
            continue
        match = re.search(pattern, path)
        if match is not None and match.group(1) != scope_values[column]:
            raise CoreParseError('row_scope_mismatch')


def _receipt_diagnostics(receipt: Mapping | None) -> list[str]:
    if receipt is None:
        return ['receipt_absent']
    diagnostics = []
    if receipt.get('verified') is not True:
        diagnostics.append('receipt_not_verified')
    if receipt.get('receipt_readback') is not True:
        diagnostics.append('receipt_readback_missing')
    if not _nonempty(receipt.get('plan_sha256')):
        diagnostics.append('receipt_plan_missing')
    if not _nonempty(receipt.get('committed_batch')):
        diagnostics.append('receipt_batch_missing')
    return diagnostics


def build_core_dq(
    outcomes: Sequence[LaneOutcome], *, verified_receipt: Mapping | None = None,
) -> dict:
    errors = set()
    if not outcomes:
        errors.add('empty_universe')
    seen_outcomes = set()
    seen_keys = set()
    counts = {entity: Counter() for entity in CORE_ENTITIES}
    row_counts = dict.fromkeys(CORE_ENTITIES, 0)
    outcome_status_counts = Counter()
    for outcome in outcomes:
        entity, status = outcome.entity, outcome.status
        outcome_status_counts[status] += 1
        identity = (
            entity, outcome.scope.scope_id if outcome.scope else None,
            outcome.scope.cycle_id if outcome.scope else None,
            outcome.capture.capture_id if outcome.capture else None,
        )
        if entity == 'club_value_observations' and outcome.rows:
            identity += (outcome.rows[0].get('club_id'),)
        if entity == 'player_representation_observations' and outcome.scope:
            identity += (outcome.scope.player_id,)
        if identity in seen_outcomes:
            errors.add('duplicate_outcome')
        seen_outcomes.add(identity)
        if status not in CORE_STATUSES:
            errors.add('invalid_status')
        elif not _status_matches_rows(entity, status, outcome.rows):
            errors.add('status_rows_mismatch')
        if entity not in CORE_ENTITIES:
            errors.add('invalid_entity')
            continue
        counts[entity][status] += 1
        row_counts[entity] += len(outcome.rows)
        columns = set(CORE_COLUMNS[entity])
        for row in outcome.rows:
            if row.keys() != columns:
                errors.add('row_column_mismatch')
            if status == 'observed_present' and any(
                not _nonempty(row.get(col)) for col in LINEAGE_COLUMNS[:-2]
            ):
                errors.add('incomplete_lineage')
            key = (entity, *(row.get(col) for col in CORE_NATURAL_KEYS[entity]))
            if key in seen_keys:
                errors.add('duplicate_natural_key')
            if any(value is None or value == '' for value in key[1:]):
                errors.add('empty_natural_key')
            seen_keys.add(key)

    entities = {}
    for entity in sorted(counts):
        statuses = counts[entity]
        total = statuses.total()
        conclusive = sum(statuses[status] for status in CONCLUSIVE_CORE_STATUSES)
        entities[entity] = {
            'status_counts': dict(sorted(statuses.items())),
            'row_count': row_counts[entity],
            'conclusive_coverage': conclusive / total if total else 0.0,
        }
    diagnostics = _receipt_diagnostics(verified_receipt)
    return {
        'passed': not errors,
        'errors': sorted(errors),
        'outcome_status_counts': dict(sorted(outcome_status_counts.items())),
        'row_counts': dict(sorted(row_counts.items())),
        'entities': entities,
        'receipt_diagnostics': diagnostics,
        'verified_write_coverage': 0.0 if diagnostics else 1.0,
    }
