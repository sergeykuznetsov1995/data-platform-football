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
TRANSFERMARKT_HOST = 'www.transfermarkt.com'
TRANSFERMARKT_API_HOST = 'tmapi.transfermarkt.technology'
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
_SQUAD_ROUTE = (
    'squad', 'club_id', r'/[A-Za-z0-9_-]+/kader/verein/([1-9][0-9]*)/saison_id/([0-9]{4})(?:/plus/1)?/?',
)
_STAGE_B_ROUTES = {
    'squad_memberships': _SQUAD_ROUTE,
    'player_attribute_observations': _SQUAD_ROUTE,
    'player_contract_observations': _SQUAD_ROUTE,
    'coach_stints': ('coach_history', 'club_id',
                     r'/[A-Za-z0-9_-]+/mitarbeiterhistorie/verein/([1-9][0-9]*)(?:/plus/1)?/?'),
    'market_value_points': ('market_value_points', 'player_id',
                            r'/ceapi/marketValueDevelopment/graph/([1-9][0-9]*)'),
    'transfer_events': ('transfer_events', 'player_id', r'/ceapi/transferHistory/list/([1-9][0-9]*)'),
    'coach_profiles': ('coach_profile', 'coach_id', r'/[A-Za-z0-9_-]+/profil/trainer/([1-9][0-9]*)/?'),
}
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


def _row_facts_match_verdict(entity: str, row) -> bool:
    """Whether a row's verdict agrees with the facts the row itself carries.

    Each section's donor states these invariants for its own rows. They are
    repeated here because DQ and planning read the rows after the donor is
    gone, and the rows have stayed mutable.
    """
    verdict = row.get('observation_status')
    if entity == 'attendance_observations':
        # The donor: a club is observed exactly when a crowd figure is published.
        published = row.get('spectators_total') is not None or row.get('spectators_average') is not None
        if published:
            return verdict == 'observed_present'
        raw_cells = (row.get('spectators_total_raw'), row.get('spectators_average_raw'))
        unknown = any(isinstance(cell, str) and cell.strip().lower() in {'', '?', 'unknown'}
                      for cell in raw_cells)
        return verdict == ('unknown' if unknown else 'explicit_none')
    if entity == 'player_representation_observations':
        # The donor: only an assigned player names a representative, and an
        # explicit absence carries its own evidence.
        if verdict == 'observed_present':
            representative = row.get('representative_id')
            return (isinstance(representative, str) and _ID_RE.fullmatch(representative) is not None
                    and row.get('representative_key') == f'agency:{representative}')
        if row.get('representative_id') is not None or row.get('representative_key') != f'status:{verdict}':
            return False
        if verdict != 'explicit_none':
            return True
        if row.get('evidence_type') != 'player_profile_explicit_none' or not _nonempty(row.get('source_url')):
            return False
        try:
            url = urlsplit(row['source_url'])
        except ValueError:
            return False
        match = re.fullmatch(r'/[A-Za-z0-9_-]+/profil/spieler/([1-9][0-9]*)/?', url.path)
        return url.netloc == TRANSFERMARKT_HOST and match is not None and match[1] == row.get('player_id')
    if entity == 'referee_aggregate_observations':
        # The donor: a metric has a value exactly when it is observed present.
        value = row.get('value')
        if verdict == 'observed_present':
            return isinstance(value, str) and re.fullmatch(r'[0-9]+(?:[.,][0-9]+)?', value) is not None
        return value is None and row.get('raw_value') == ('' if verdict == 'observed_empty' else '-')
    if entity == 'club_value_observations':
        # The donor: a value is present exactly when it has a non-negative amount.
        amount = row.get('value_amount')
        if (amount is not None) != (verdict == 'observed_present'):
            return False
        return amount is None or (type(amount) is int and amount >= 0)
    return True


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
        # A populated page is an observed page: its rows carry that one
        # verdict, never an absence the page itself contradicts.
        return status == 'observed_present' and all(
            row.get('observation_status') == status for row in rows)
    verdicts = []
    for row in rows:
        verdict = row.get('observation_status')
        if not isinstance(verdict, str) or verdict not in allowed:
            return False
        verdicts.append(verdict)
        # The row's own facts must agree with its verdict.
        if not _row_facts_match_verdict(entity, row):
            return False
        # Each contract cell carries its own verdict, in both directions.
        if contract and verdict != _CONTRACT_STATUSES.get(row.get('contract_status')):
            return False
    # A populated squad page carries each contract cell's own verdict.
    if contract and status == 'observed_present':
        return True
    # The donors' summarise rule for the aggregate verdict. Rows of the other
    # section lanes are an observed page, never an aggregate absence.
    if status == 'explicit_none':
        return entity in _PRESENCE_ENTITIES and all(
            verdict == 'explicit_none' for verdict in verdicts)
    if status == 'observed_present':
        return entity not in _PRESENCE_ENTITIES or 'observed_present' in verdicts
    # unknown never hides a present row, and in a presence lane it needs a row
    # that is itself unresolved: rows that are all explicit_none are the
    # donor's explicit_none.
    return 'observed_present' not in verdicts and (
        entity not in _PRESENCE_ENTITIES or 'unknown' in verdicts)


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
    # An empty outcome has no row to name the lane's revisions.
    parser_revision: str = CORE_PARSER_REVISION
    schema_revision: str = CORE_SCHEMA_REVISION
    # The requested target inside a scope that may hold several; a failed
    # request keeps no capture to tell its targets apart in DQ.
    subject: str = ''
    # Every walked page of a multi-page snapshot as (source_url, capture_id,
    # captured_at, source_body_hash, next_url): the full lineage of a page,
    # including one that contributes no row but still has a clock, and the
    # link its own page gives to the next ('' for the terminal page). The
    # manifest is the walk: each page names the next one.
    pages: tuple[tuple[str, str, str, str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.entity not in CORE_ENTITIES:
            raise CoreParseError('invalid_entity')
        if not isinstance(self.status, str) or self.status not in CORE_STATUSES:
            raise CoreParseError('invalid_status')
        if not _nonempty(self.status_reason):
            raise CoreParseError('invalid_status_reason')
        if not _nonempty(self.parser_revision) or not _nonempty(self.schema_revision):
            raise CoreParseError('invalid_revision')
        if self.status in _PROOF_STATUSES:
            if not _nonempty(self.proof):
                raise CoreParseError('missing_proof')
        elif self.proof is not None:
            raise CoreParseError('unexpected_proof')
        if not _status_matches_rows(self.entity, self.status, self.rows):
            raise CoreParseError('status_rows_mismatch')
        pages = tuple(tuple(page) for page in self.pages)
        object.__setattr__(self, 'pages', pages)
        # A page is one capture, and a capture id names exactly one response.
        if (any(len(page) != 5 or not all(map(_nonempty, page[:4]))
                or not isinstance(page[4], str) for page in pages)
                or len({page[0] for page in pages}) != len(pages)
                or len({page[1] for page in pages}) != len(pages)):
            raise CoreParseError('invalid_page_evidence')
        for page in pages:
            canonical_timestamp(page[2])
            if not _CAPTURE_RE.fullmatch(page[3]):
                raise CoreParseError('invalid_page_evidence')
        if self.capture is not None and pages and not any(
                page[0] == self.capture.url and page[1] == self.capture.capture_id
                and page[2] == self.capture.captured_at
                and page[3] == self.capture.body_hash for page in pages):
            raise CoreParseError('capture_outside_page_evidence')
        if pages and any(page[4] != (pages[index + 1][0] if index + 1 < len(pages) else '')
                         for index, page in enumerate(pages)):
            raise CoreParseError('page_chain_broken')
        if any(
            row.get('parser_revision', self.parser_revision) != self.parser_revision
            or row.get('schema_revision', self.schema_revision) != self.schema_revision
            for row in self.rows
        ):
            raise CoreParseError('revision_differs_from_rows')
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
        if value is None:
            row[col] = None
            continue
        kind = COLUMN_TYPES[col]
        if kind == 'varchar' and not isinstance(value, str):
            raise CoreParseError('row_value_not_text')
        if kind == 'boolean' and not isinstance(value, bool):
            raise CoreParseError('row_value_not_boolean')
        if kind == 'bigint' and type(value) is not int:
            if not isinstance(value, str) or not re.fullmatch(r'[0-9]+', value):
                raise CoreParseError('row_value_not_integer')
            digits = value.lstrip('0') or '0'
            if len(digits) > 19:
                raise CoreParseError('row_value_exceeds_bigint')
            value = int(digits)
        if kind == 'date':
            if not isinstance(value, date):
                try:
                    value = date.fromisoformat(value)
                except (TypeError, ValueError) as exc:
                    raise CoreParseError('row_value_not_date') from exc
            value = date.isoformat(value)
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
    if any(type(value) is int and not -2 ** 63 <= value < 2 ** 63 for value in row.values()):
        raise CoreParseError('row_value_exceeds_bigint')
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

    def render(page_rows):
        for player in page_rows:
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

    try:
        render(players)
    except CoreParseError as exc:
        # A row the page cannot hold (an id that is not one, a value beyond
        # its column) is this capture's error, not the end of the run.
        return page_outcomes('error', exc.reason)
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
    try:
        rows = tuple(
            _render(entity, row, observation_status='observed_present',
                    status_reason='career_page', **context)
            for row in parsed
        )
    except CoreParseError as exc:
        # A value a row cannot hold is this capture's error, not the end of the run.
        return LaneOutcome(entity, 'error', exc.reason, **context)
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
    try:
        row = _render(entity, values, observation_status='observed_present',
                      status_reason='coach_profile', **context)
    except CoreParseError as exc:
        # A value the row cannot hold is this capture's error, not the end of the run.
        return LaneOutcome(entity, 'error', exc.reason, **context)
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
    try:
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
    except CoreParseError as exc:
        # A row the page cannot hold is this capture's error, not the end of the run.
        return LaneOutcome(entity, 'error', exc.reason, **context)
    return LaneOutcome(entity, 'observed_present', 'coach_history', tuple(rows), **context)


def captureless_outcome(
    entity: str, scope: ScopeContext, *, status: str, reason: str, extracted_at: str,
    run_id: str, universe_version: str, proof: str | None = None, subject: str = '',
) -> LaneOutcome:
    if status not in {'unknown', 'unavailable', 'error', 'not_applicable'}:
        raise CoreParseError('invalid_captureless_status')
    canonical_timestamp(extracted_at)
    if not _nonempty(run_id) or not _nonempty(universe_version):
        raise CoreParseError('incomplete_lineage')
    return LaneOutcome(entity, status, reason, proof=proof, scope=scope,
                       extracted_at=extracted_at, run_id=run_id,
                       universe_version=universe_version, subject=subject)


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
    if not _rows_match_revisions(outcome):
        raise CoreParseError('revision_differs_from_rows')
    # A writable row is exactly the entity's column set: a missing business
    # column would be written as a null over the committed value, and only
    # the physical read-back would notice, after the replacement.
    columns = set(CORE_COLUMNS[entity])
    if any(row.keys() != columns for row in outcome.rows):
        raise CoreParseError('row_column_mismatch')
    # Rows are mutable after construction: the verdict policy is applied to
    # the rows as they are now, not as they were when the outcome was built.
    if not _status_matches_rows(entity, outcome.status, outcome.rows):
        raise CoreParseError('status_rows_mismatch')

    from scrapers.transfermarkt.native_writer import NativeWritePlan

    capture = outcome.capture
    # The plan's clock is evidence, not a caller's choice: the newest walked
    # page of a snapshot, otherwise the capture itself.
    evidence_clock = (max(page[2] for page in outcome.pages) if outcome.pages
                      else capture.captured_at if capture is not None else None)
    if evidence_clock is not None and captured_at != evidence_clock:
        raise CoreParseError('captured_at_differs_from_evidence')
    _check_scope_against_capture(entity, scope_values, capture)
    _check_pages_against_scope(entity, scope_values, outcome.pages)
    evidence = ()
    if capture is not None:
        if entity == 'club_value_observations':
            competition = _club_value_competition(capture.url)
            if any(row['competition_id'] != competition for row in outcome.rows):
                raise CoreParseError('row_scope_mismatch')
        row = outcome.rows[0] if outcome.rows else {}
        evidence = (
            ('source_url', capture.url),
            ('source_endpoint', capture.endpoint),
            ('raw_capture_id', capture.capture_id),
            ('source_capture_id', capture.capture_id),
            ('source_body_hash', capture.body_hash),
            ('scope_id', outcome.scope.scope_id),
            ('cycle_id', outcome.scope.cycle_id),
            ('parser_revision', row.get('parser_revision', outcome.parser_revision)),
            ('schema_revision', row.get('schema_revision', outcome.schema_revision)),
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
        evidence=evidence, pages=outcome.pages,
    )


def _club_value_competition(url: str) -> str:
    match = re.fullmatch(
        r'/[A-Za-z0-9_-]+/marktwerteverein/(?:wettbewerb|pokalwettbewerb)/'
        r'([A-Za-z0-9_-]+)(?:/stichtag/[0-9]{4}-[0-9]{2}-[0-9]{2})?/?', urlsplit(url).path)
    if match is None:
        raise CoreParseError('row_scope_mismatch')
    return match[1]


def _representation_query_ids(query: str) -> list[str]:
    params = parse_qs(query, keep_blank_values=True)
    if params.keys() != {'ids[]'} or not all(_ID_RE.fullmatch(value) for value in params['ids[]']):
        return []
    return params['ids[]']


def _referee_directory_identity(path: str) -> tuple[str, str]:
    match = _REFEREE_DIRECTORY_PATH.fullmatch(path)
    if match is None:
        raise CoreParseError('row_scope_mismatch')
    return match[1], match[2]


def _rows_match_revisions(outcome: LaneOutcome) -> bool:
    """Rows stay mutable after construction: every consumer rechecks them."""
    return all(
        row.get('parser_revision', outcome.parser_revision) == outcome.parser_revision
        and row.get('schema_revision', outcome.schema_revision) == outcome.schema_revision
        for row in outcome.rows
    )


# Lanes whose route carries a query. The lane validates it itself (the
# players of a packet, the page of a walk); every other route has none.
_ROUTES_WITH_QUERY = frozenset({
    'player_representation_observations', 'attendance_observations', 'rumour_observations',
})


def _source_origin(entity: str, scope_values: Mapping[str, str]) -> str:
    """The one authority a lane's captures come from."""
    if entity == 'player_representation_observations' or (
            entity == 'achievement_observations' and scope_values.get('recipient_type') != 'coach'):
        return TRANSFERMARKT_API_HOST
    return TRANSFERMARKT_HOST


def saison_year(season: str) -> int:
    """Map a season slug or ending year to its Transfermarkt starting year."""
    if not isinstance(season, str) or not season.isdecimal():
        raise ValueError(f"Cannot infer a saison year from {season!r}; supply --edition-id")
    if len(season) == 4:
        first, second = int(season[:2]), int(season[2:])
        if second == (first + 1) % 100:
            candidate = 2000 + first
            return candidate if candidate <= date.today().year + 1 else 1900 + first
    return int(season) - 1


def _check_scope_against_capture(entity: str, scope_values: Mapping[str, str], capture) -> None:
    """Empty outcomes have no rows to cross-check; the capture URL still names
    the entity it belongs to, so a swapped club/player scope fails here."""
    if capture is None:
        return
    url = urlsplit(capture.url)
    path = url.path
    # The complete origin and route of the lane: https from its own authority
    # as a whole (another port or a user part is another source), no fragment,
    # and a query only where the lane's route has one.
    if (url.scheme != 'https' or url.netloc != _source_origin(entity, scope_values)
            or url.fragment or (url.query and entity not in _ROUTES_WITH_QUERY)):
        raise CoreParseError('row_scope_mismatch')
    if entity == 'player_representation_observations':
        if path != '/players' or scope_values['player_id'] not in _representation_query_ids(url.query):
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'referee_aggregate_observations':
        # The donor's directory URL carries its page in the path; a query or
        # fragment is not part of the supported route.
        if (url.query or url.fragment or _referee_directory_identity(path)
                != (scope_values['competition_id'], scope_values['edition_id'])):
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'referee_profiles':
        match = re.fullmatch(r'/-/profil/schiedsrichter/([1-9][0-9]*)', path)
        # The whole origin: another port of the right host is another source.
        if url.netloc != TRANSFERMARKT_HOST or url.query or url.fragment:
            raise CoreParseError('row_scope_mismatch')
        if match is None or match[1] != scope_values['referee_id']:
            raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'rumour_observations':
        # The donor's page-identity parser accepts exactly the forms its walk
        # can produce (trailing slash, /plus/1, ?page=N) on Transfermarkt hosts.
        from scrapers.transfermarkt import rumours

        try:
            rumours._page_ref(capture.url, scope_values['competition_id'])
        except rumours.RumourParseError as exc:
            raise CoreParseError('row_scope_mismatch') from exc
        return
    if entity == 'achievement_observations':
        recipient_type = scope_values['recipient_type']
        recipient_id = scope_values['recipient_id']
        if capture.endpoint != f'{recipient_type}_achievement':
            raise CoreParseError('row_scope_mismatch')
        if recipient_type == 'coach':
            match = re.fullmatch(r'/[A-Za-z0-9-]+/erfolge/trainer/([1-9][0-9]*)', path)
            if url.netloc != TRANSFERMARKT_HOST or match is None or match[1] != recipient_id:
                raise CoreParseError('row_scope_mismatch')
        else:
            match = re.fullmatch(r'/(player|club)/([1-9][0-9]*)/achievement', path)
            if (url.hostname != 'tmapi.transfermarkt.technology' or match is None
                    or match.groups() != (recipient_type, recipient_id)):
                raise CoreParseError('row_scope_mismatch')
        return
    if entity == 'attendance_observations' and {'competition_id', 'edition_id'} <= scope_values.keys():
        # The donor's own page validation: it accepts every form its walk can
        # produce (a trailing slash before ?page=N included) and nothing else.
        from scrapers.transfermarkt import attendance

        try:
            attendance._validate_attendance_url(
                capture.url, host=TRANSFERMARKT_HOST, path=path,
                competition=scope_values['competition_id'], edition=scope_values['edition_id'])
        except attendance.AttendanceParseError as exc:
            raise CoreParseError('row_scope_mismatch') from exc
    if entity in _STAGE_B_ROUTES:
        endpoint, column, pattern = _STAGE_B_ROUTES[entity]
        match = re.fullmatch(pattern, path)
        if capture.endpoint != endpoint or match is None or match[1] != scope_values[column]:
            raise CoreParseError('row_scope_mismatch')
        if endpoint == 'squad':
            try:
                year = saison_year(scope_values['season'])
            except (KeyError, ValueError) as exc:
                raise CoreParseError('row_scope_mismatch') from exc
            if int(match[2]) != year:
                raise CoreParseError('row_scope_mismatch')


def _check_pages_against_scope(entity: str, scope_values: Mapping[str, str], pages) -> None:
    """A replacement snapshot replaces its whole scope or nothing.

    Every page of its manifest must belong to the requested scope, and the
    manifest must be one walk from that scope's first page: a manifest that
    starts later certifies pages it never saw.
    """
    if not pages:
        return
    if entity == 'attendance_observations':
        from scrapers.transfermarkt import attendance

        for number, (url, *_) in enumerate(pages, start=1):
            parts = urlsplit(url)
            try:
                attendance._validate_attendance_url(
                    url, host=TRANSFERMARKT_HOST, path=parts.path,
                    competition=scope_values['competition_id'], edition=scope_values['edition_id'])
            except attendance.AttendanceParseError as exc:
                raise CoreParseError('page_scope_mismatch') from exc
            # The donor validated the query: at most one positive page number.
            if int(parse_qs(parts.query).get('page', ['1'])[0]) != number:
                raise CoreParseError('page_manifest_does_not_start_on_the_first_page')


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
            outcome.subject,
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
        if not _rows_match_revisions(outcome):
            errors.add('revision_mismatch')
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
