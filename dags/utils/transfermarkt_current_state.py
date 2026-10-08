"""Pure contracts for the current lane's signals and bounded work cursor.

This module performs no I/O. The caller persists the returned state only after
the matching raw/manifest evidence is durable. Observing a signal never claims
that its Bronze update has succeeded. This is not the global career debt queue.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from scrapers.transfermarkt.registry import deterministic_scope_id, RegistryError

SIGNALS_TABLE = 'iceberg.ops.transfermarkt_current_signals_v1'
CHECKS_TABLE = 'iceberg.ops.transfermarkt_current_checks_v1'
CURRENT_POLICY_VERSION = 'change-signals-v1'
DAILY_INTERVAL = timedelta(hours=20)
WEEKLY_INTERVAL = timedelta(days=7)
TOP_LEAGUES = ('GB1', 'ES1', 'IT1', 'L1', 'FR1')
DAILY_CHECKS = ('listing', 'players', 'injury')


class CurrentStateError(ValueError):
    """A current-lane record cannot safely establish freshness or completion."""


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CurrentStateError(f'{name} must be nonempty text')
    return value


def _time(value: datetime | str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except (ValueError, TypeError) as exc:
        raise CurrentStateError('timestamp is not ISO8601') from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CurrentStateError('timestamp must include a timezone')
    return parsed.astimezone(timezone.utc)


def _hash(name: str, value: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise CurrentStateError(f'{name} must be lowercase sha256')


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=lambda v: _time(v).isoformat())


def _mapping(cls: type, value: Mapping[str, Any]) -> dict[str, Any]:
    expected = {item.name for item in fields(cls)}
    if set(value) != expected:
        raise CurrentStateError(f'{cls.__name__} fields differ: {sorted(set(value) ^ expected)}')
    return dict(value)


@dataclass(frozen=True)
class SignalObservation:
    scope_id: str
    entity: str
    entity_id: str
    signature: str
    checked_at: datetime
    signal_version: str
    raw_capture_id: str
    raw_fetched_at: datetime
    source_url: str
    source_body_hash: str

    def __post_init__(self) -> None:
        for name in ('scope_id', 'entity_id', 'signal_version', 'raw_capture_id', 'source_url'):
            _text(name, getattr(self, name))
        if self.entity not in {'club', 'player', 'injury', 'listing'}:
            raise CurrentStateError('unsupported signal entity')
        for name in ('signature', 'source_body_hash'):
            _hash(name, getattr(self, name))
        for name in ('checked_at', 'raw_fetched_at'):
            object.__setattr__(self, name, _time(getattr(self, name)))
        if self.raw_fetched_at > self.checked_at:
            raise CurrentStateError('raw capture is newer than check')
        url = urlsplit(self.source_url)
        if url.scheme != 'https' or not url.netloc or url.username or url.password:
            raise CurrentStateError('source_url must be credential-free HTTPS')

    @property
    def key(self) -> tuple[str, str, str]:
        return self.scope_id, self.entity, self.entity_id

    def as_dict(self) -> dict[str, Any]:
        return json.loads(_json(asdict(self)))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'SignalObservation':
        return cls(**_mapping(cls, value))


@dataclass(frozen=True)
class SignalState:
    observation: SignalObservation
    applied_signature: str | None = None
    first_detected_at: datetime | None = None
    status: str = 'pending'
    result: str = ''
    bronze_manifest: str | None = None
    committed_at: datetime | None = None
    # Immutable serialized events retain superseded failures for #1395.
    superseded_evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.observation, SignalObservation):
            raise CurrentStateError('observation must be validated')
        if self.applied_signature is not None:
            _hash('applied_signature', self.applied_signature)
        if self.status not in {'pending', 'failed', 'applied'}:
            raise CurrentStateError('unsupported signal status')
        if self.first_detected_at is None:
            object.__setattr__(self, 'first_detected_at', self.observation.checked_at)
        else:
            object.__setattr__(self, 'first_detected_at', _time(self.first_detected_at))
        if self.first_detected_at > self.observation.checked_at:
            raise CurrentStateError('first detection is newer than latest check')
        if self.committed_at is not None:
            object.__setattr__(self, 'committed_at', _time(self.committed_at))
        if self.status == 'applied' and (
            self.applied_signature != self.observation.signature
            or not self.bronze_manifest or self.committed_at is None
        ):
            raise CurrentStateError('applied signal requires matching Bronze proof')
        if self.committed_at is not None and not self.bronze_manifest:
            raise CurrentStateError('committed_at requires a Bronze manifest')
        if self.status == 'failed' and not self.result:
            raise CurrentStateError('failed signal requires result evidence')
        object.__setattr__(self, 'superseded_evidence', tuple(self.superseded_evidence))
        for event in self.superseded_evidence:
            try:
                parsed = json.loads(event)
            except (TypeError, ValueError) as exc:
                raise CurrentStateError('superseded evidence must be JSON') from exc
            if not isinstance(parsed, dict):
                raise CurrentStateError('superseded evidence must contain objects')

    @property
    def pending(self) -> bool:
        return self.status != 'applied'

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        return json.loads(_json(value))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'SignalState':
        parsed = _mapping(cls, value)
        parsed['observation'] = SignalObservation.from_mapping(parsed['observation'])
        return cls(**parsed)

    @classmethod
    def from_ops_row(cls, value: Mapping[str, Any]) -> 'SignalState':
        """Decode the flat SQL row without defaults that invent fresh evidence."""
        names = {
            'scope_id', 'entity', 'entity_id', 'observed_signature', 'applied_signature',
            'first_detected_at', 'last_checked_at', 'signal_version', 'raw_capture_id',
            'raw_fetched_at', 'source_url', 'source_body_hash', 'status', 'result',
            'bronze_manifest', 'committed_at', 'superseded_evidence_json',
        }
        if set(value) != names:
            raise CurrentStateError('ops signal row fields differ')
        if value['first_detected_at'] is None:
            raise CurrentStateError('ops signal row lacks detection evidence')
        try:
            events = json.loads(value['superseded_evidence_json'])
        except (TypeError, ValueError) as exc:
            raise CurrentStateError('ops signal events are invalid JSON') from exc
        if not isinstance(events, list) or any(not isinstance(event, str) for event in events):
            raise CurrentStateError('ops signal events must be a string array')
        observation = SignalObservation(
            scope_id=value['scope_id'], entity=value['entity'], entity_id=value['entity_id'],
            signature=value['observed_signature'], checked_at=value['last_checked_at'],
            signal_version=value['signal_version'], raw_capture_id=value['raw_capture_id'],
            raw_fetched_at=value['raw_fetched_at'], source_url=value['source_url'],
            source_body_hash=value['source_body_hash'],
        )
        return cls(observation=observation, applied_signature=value['applied_signature'],
                   first_detected_at=value['first_detected_at'], status=value['status'],
                   result=value['result'], bronze_manifest=value['bronze_manifest'],
                   committed_at=value['committed_at'], superseded_evidence=tuple(events))


def mark_seen(previous: SignalState | None, observation: SignalObservation) -> SignalState:
    """Keep pending first detection; do not erase failure on a retry/check."""
    if previous is None:
        return SignalState(observation=observation)
    if previous.observation.key != observation.key:
        raise CurrentStateError('signal identity changed')
    if observation.checked_at < previous.observation.checked_at:
        raise CurrentStateError('out-of-order signal check')
    if (previous.observation.signature, previous.observation.signal_version) == (
        observation.signature, observation.signal_version,
    ):
        return replace(previous, observation=observation)
    if observation.checked_at == previous.observation.checked_at:
        raise CurrentStateError('conflicting signal at the same check timestamp')
    event = _json({
        'signature': previous.observation.signature,
        'signal_version': previous.observation.signal_version,
        'first_detected_at': previous.first_detected_at,
        'last_checked_at': previous.observation.checked_at,
        'status': previous.status, 'result': previous.result,
        'bronze_manifest': previous.bronze_manifest,
        'committed_at': previous.committed_at,
    })
    # Even a return to the old applied signature needs processing: the failed
    # intermediate capture may already have committed data before its ack.
    return SignalState(
        observation=observation, applied_signature=previous.applied_signature,
        first_detected_at=observation.checked_at,
        bronze_manifest=previous.bronze_manifest, committed_at=previous.committed_at,
        superseded_evidence=previous.superseded_evidence + (event,),
    )


def mark_failed(state: SignalState, result: str) -> SignalState:
    _text('result', result)
    if not state.pending:
        raise CurrentStateError('cannot fail an applied signal')
    return replace(state, status='failed', result=result)


def mark_applied(
    state: SignalState, *, signature: str, bronze_manifest: str, committed_at: datetime,
) -> SignalState:
    """A stale worker must never acknowledge a superseding observation."""
    if signature != state.observation.signature:
        raise CurrentStateError('Bronze proof does not match latest signal')
    _text('bronze_manifest', bronze_manifest)
    timestamp = _time(committed_at)
    if timestamp < state.first_detected_at:
        raise CurrentStateError('Bronze proof predates detection')
    return replace(state, applied_signature=signature, status='applied', result='committed',
                   bronze_manifest=bronze_manifest, committed_at=timestamp)


@dataclass(frozen=True)
class EditionCheck:
    scope_id: str
    check_kind: str
    cycle_id: str
    registry_snapshot_id: str
    denominator_hash: str
    checked_at: datetime
    status: str
    result: str
    raw_capture_ids: tuple[str, ...]
    expected_ids: int
    observed_ids: int

    def __post_init__(self) -> None:
        for name in ('scope_id', 'cycle_id', 'registry_snapshot_id'):
            _text(name, getattr(self, name))
        _hash('denominator_hash', self.denominator_hash)
        object.__setattr__(self, 'checked_at', _time(self.checked_at))
        if self.check_kind not in (*DAILY_CHECKS, 'daily_complete') or self.status not in {'complete', 'failed', 'uncertain'}:
            raise CurrentStateError('invalid edition check kind/status')
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0
               for v in (self.expected_ids, self.observed_ids)):
            raise CurrentStateError('check coverage must contain nonnegative integers')
        if not isinstance(self.raw_capture_ids, (list, tuple)):
            raise CurrentStateError('raw_capture_ids must be an array')
        object.__setattr__(self, 'raw_capture_ids', tuple(self.raw_capture_ids))
        for item in self.raw_capture_ids:
            _text('raw_capture_id', item)
        if self.status == 'complete' and (
            not self.raw_capture_ids or self.expected_ids != self.observed_ids
        ):
            raise CurrentStateError('complete check requires raw and full coverage')
        if self.status != 'complete':
            _text('result', self.result)

    def as_dict(self) -> dict[str, Any]:
        return json.loads(_json(asdict(self)))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'EditionCheck':
        return cls(**_mapping(cls, value))


def _sql(value: Any) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, datetime):
        return "TIMESTAMP '" + _time(value).replace(tzinfo=None).isoformat(sep=' ') + "'"
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_current_state_tables() -> tuple[str, str]:
    return (
        f'''CREATE TABLE IF NOT EXISTS {SIGNALS_TABLE} (
scope_id varchar, entity varchar, entity_id varchar, observed_signature varchar,
applied_signature varchar, first_detected_at timestamp(6), last_checked_at timestamp(6),
signal_version varchar, raw_capture_id varchar, raw_fetched_at timestamp(6),
source_url varchar, source_body_hash varchar, status varchar, result varchar,
bronze_manifest varchar, committed_at timestamp(6), superseded_evidence_json varchar
) WITH (format = 'PARQUET')''',
        f'''CREATE TABLE IF NOT EXISTS {CHECKS_TABLE} (
scope_id varchar, check_kind varchar, cycle_id varchar, registry_snapshot_id varchar,
denominator_hash varchar, checked_at timestamp(6), status varchar, result varchar,
raw_capture_ids_json varchar, expected_ids bigint, observed_ids bigint
) WITH (format = 'PARQUET')''',
    )


def _merge(table: str, values: Mapping[str, Any], keys: tuple[str, ...], condition: str = '') -> str:
    columns = ', '.join(values)
    projection = ', '.join(f'{_sql(value)} AS {name}' for name, value in values.items())
    join = ' AND '.join(f't.{key} = s.{key}' for key in keys)
    updates = ', '.join(f'{name} = s.{name}' for name in values if name not in keys)
    return (f'MERGE INTO {table} t USING (SELECT {projection}) s ON {join}\n'
            f'WHEN MATCHED{condition} THEN UPDATE SET {updates}\n'
            f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ('
            + ', '.join(f's.{name}' for name in values) + ')')


def _signal_values(state: SignalState) -> dict[str, Any]:
    obs = state.observation
    return {
        'scope_id': obs.scope_id, 'entity': obs.entity, 'entity_id': obs.entity_id,
        'observed_signature': obs.signature, 'applied_signature': state.applied_signature,
        'first_detected_at': state.first_detected_at, 'last_checked_at': obs.checked_at,
        'signal_version': obs.signal_version, 'raw_capture_id': obs.raw_capture_id,
        'raw_fetched_at': obs.raw_fetched_at, 'source_url': obs.source_url,
        'source_body_hash': obs.source_body_hash, 'status': state.status, 'result': state.result,
        'bronze_manifest': state.bronze_manifest, 'committed_at': state.committed_at,
        'superseded_evidence_json': _json(state.superseded_evidence),
    }


_SIGNAL_UPDATE_GUARD = (
    " AND s.last_checked_at >= t.last_checked_at AND (s.status <> 'applied'"
    ' OR (t.observed_signature = s.observed_signature'
    ' AND t.signal_version = s.signal_version))'
)


def build_signal_merge(state: SignalState) -> str:
    return _merge(SIGNALS_TABLE, _signal_values(state), ('scope_id', 'entity', 'entity_id'),
                  _SIGNAL_UPDATE_GUARD)


def build_signals_merge(states: Iterable[SignalState]) -> str:
    """One bounded transaction per packet; duplicate keys cannot race in MERGE."""
    packet = tuple(states)
    if not 1 <= len(packet) <= 300:
        raise CurrentStateError('signal packet must contain 1..300 states')
    if any(not isinstance(state, SignalState) for state in packet):
        raise CurrentStateError('signal packet must contain validated states')
    if len({state.observation.key for state in packet}) != len(packet):
        raise CurrentStateError('signal packet contains duplicate keys')
    rows = [_signal_values(state) for state in packet]
    columns = tuple(rows[0])
    projection = ', '.join(columns)
    values = ',\n'.join('(' + ', '.join(_sql(row[column]) for column in columns) + ')'
                          for row in rows)
    keys = ('scope_id', 'entity', 'entity_id')
    join = ' AND '.join(f't.{key} = s.{key}' for key in keys)
    updates = ', '.join(f'{column} = s.{column}' for column in columns if column not in keys)
    return (f'MERGE INTO {SIGNALS_TABLE} t USING (VALUES {values}) s ({projection}) ON {join}\n'
            f'WHEN MATCHED{_SIGNAL_UPDATE_GUARD} THEN UPDATE SET {updates}\n'
            f'WHEN NOT MATCHED THEN INSERT ({projection}) VALUES ('
            + ', '.join(f's.{column}' for column in columns) + ')')


def build_check_merge(check: EditionCheck) -> str:
    values = asdict(check)
    values['raw_capture_ids_json'] = _json(values.pop('raw_capture_ids'))
    return _merge(CHECKS_TABLE, values, ('scope_id', 'check_kind', 'cycle_id'))


@dataclass(frozen=True)
class ScopeCursor:
    scope_id: str
    policy_version: str = CURRENT_POLICY_VERSION
    generation: str = ''
    cold_complete: bool = False
    listing_checked_at: datetime | None = None
    players_checked_at: datetime | None = None
    injury_checked_at: datetime | None = None
    roster_completed_at: datetime | None = None
    last_work_at: datetime | None = None
    resume_json: str = '{}'

    def __post_init__(self) -> None:
        _text('scope_id', self.scope_id)
        if self.policy_version != CURRENT_POLICY_VERSION:
            raise CurrentStateError('cursor policy changed; explicit migration required')
        if not isinstance(self.cold_complete, bool):
            raise CurrentStateError('cold_complete must be boolean')
        for name in ('listing_checked_at', 'players_checked_at', 'injury_checked_at',
                     'roster_completed_at', 'last_work_at'):
            if getattr(self, name) is not None:
                object.__setattr__(self, name, _time(getattr(self, name)))
        try:
            resume = json.loads(self.resume_json)
        except (ValueError, TypeError) as exc:
            raise CurrentStateError('resume cursor must be JSON') from exc
        if not isinstance(resume, dict):
            raise CurrentStateError('resume cursor must be an object')
        if resume and not self.generation:
            raise CurrentStateError('resume cursor requires a signal generation')

    def as_dict(self) -> dict[str, Any]:
        return json.loads(_json(asdict(self)))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'ScopeCursor':
        return cls(**_mapping(cls, value))

    def daily_due(self, now: datetime) -> tuple[str, ...]:
        current = _time(now)
        return tuple(name for name in DAILY_CHECKS
                     if getattr(self, f'{name}_checked_at') is None
                     or current - getattr(self, f'{name}_checked_at') >= DAILY_INTERVAL)

    def roster_due(self, now: datetime) -> bool:
        return self.cold_complete and (
            self.roster_completed_at is None
            or _time(now) - self.roster_completed_at >= WEEKLY_INTERVAL
        )


def record_check(cursor: ScopeCursor, check: EditionCheck) -> ScopeCursor:
    """An uncertain/failed request advances attempt fairness, not freshness."""
    if cursor.scope_id != check.scope_id:
        raise CurrentStateError('check belongs to another cursor')
    if cursor.last_work_at is not None and check.checked_at < cursor.last_work_at:
        raise CurrentStateError('out-of-order cursor check')
    changes: dict[str, Any] = {'last_work_at': check.checked_at}
    if check.status == 'complete' and check.check_kind in DAILY_CHECKS:
        changes[f'{check.check_kind}_checked_at'] = check.checked_at
    return replace(cursor, **changes)


def daily_complete(cursor: ScopeCursor, now: datetime) -> bool:
    """24h coverage requires all three checks; planning uses a 20h target."""
    current = _time(now)
    return all(getattr(cursor, f'{name}_checked_at') is not None
               and timedelta(0) <= current - getattr(cursor, f'{name}_checked_at') <= timedelta(hours=24)
               for name in DAILY_CHECKS)


@dataclass(frozen=True)
class CurrentTarget:
    scope_id: str
    competition_id: str
    edition_id: str
    registry_snapshot_id: str
    tier: float
    queue_rank: int = 0

    @property
    def cold_priority(self) -> tuple[int, int, float, str, str]:
        return (self.queue_rank, TOP_LEAGUES.index(self.competition_id) if self.competition_id in TOP_LEAGUES
                else len(TOP_LEAGUES), self.tier, self.competition_id, self.edition_id)


@dataclass(frozen=True)
class CurrentTargets:
    targets: tuple[CurrentTarget, ...]
    denominator_competition_ids: tuple[str, ...]
    blocked_competitions: tuple[tuple[str, str], ...]


def _field(row: Any, name: str, default: Any = None) -> Any:
    return row.get(name, default) if isinstance(row, Mapping) else getattr(row, name, default)


def current_scope_targets(
    denominator_rows: Iterable[Any], registry_rows: Iterable[Mapping[str, Any]], *,
    quarantined: Mapping[str, str] | None = None,
) -> CurrentTargets:
    """Retain every live core competition in coverage, including missing ones.

    Inputs are the promoted registry query's validated row contract. Quarantine,
    inactive/unknown rows and missing current editions remain explicit blockers.
    """
    all_rows = {str(_field(row, 'competition_id')): row for row in denominator_rows}
    denominator = {key: row for key, row in all_rows.items()
                   if _field(row, 'live') is True and _field(row, 'competition_class')
                   in {'core_club', 'core_national'}}
    by_competition: dict[str, list[Mapping[str, Any]]] = {}
    for row in registry_rows:
        by_competition.setdefault(str(row.get('competition_id', '')), []).append(row)
    # Preserve the previous denominator planner's tail: newly discovered
    # eligible competitions must not disappear until the file is refreshed.
    selected = dict(denominator)
    for competition in by_competition:
        row = all_rows.get(competition)
        if row is None or (_field(row, 'live') is True
                           and _field(row, 'competition_class') in {'youth', 'reserve'}):
            selected[competition] = row
    targets: list[CurrentTarget] = []
    blocked = dict(quarantined or {})
    # Reuse the registry query's canonical field conversions and derived
    # classification; an "eligible" status string alone is no crawl proof.
    from .transfermarkt_scope_planner import _competition_from_joined_row, _edition_from_joined_row
    for competition, denominator_row in sorted(selected.items()):
        if competition in blocked:
            continue
        candidates = [row for row in by_competition.get(competition, ())
                      if row.get('is_current') is True]
        if not candidates:
            blocked[competition] = 'missing current edition in promoted registry'
            continue
        editions: dict[str, Mapping[str, Any]] = {}
        reason = ''
        for row in candidates:
            edition = str(row.get('edition_id') or '')
            if (not edition or not row.get('registry_snapshot_id')
                or row.get('competition_active') is not True or row.get('edition_active') is not True
                or row.get('classification_status') != 'eligible'):
                reason = 'inactive, unclassified or incomplete current registry row'
                break
            try:
                competition_record = _competition_from_joined_row(row)
                edition_record = _edition_from_joined_row(row)
            except (RegistryError, TypeError, ValueError) as exc:
                reason = f'invalid promoted registry row: {exc}'
                break
            if (not competition_record.crawl_eligible or not edition_record.active
                or not edition_record.current or competition_record.competition_id != edition_record.competition_id
                or competition_record.registry_snapshot_id != edition_record.registry_snapshot_id):
                reason = 'registry classification or edition evidence is not eligible'
                break
            if edition in editions and dict(editions[edition]) != dict(row):
                reason = 'conflicting current registry rows'
                break
            editions[edition] = row
        if reason:
            blocked[competition] = reason
            continue
        try:
            tier = float(_field(denominator_row, 'tier'))
            if not math.isfinite(tier) or tier < 0:
                tier = math.inf
        except (ValueError, TypeError):
            tier = math.inf
        for edition, row in sorted(editions.items()):
            targets.append(CurrentTarget(deterministic_scope_id(competition, edition), competition, edition,
                                         str(row['registry_snapshot_id']), tier,
                                         0 if competition in denominator else 1))
    return CurrentTargets(tuple(sorted(targets, key=lambda target: target.cold_priority)),
                          tuple(sorted(denominator)), tuple(sorted(blocked.items())))


@dataclass(frozen=True)
class CurrentWork:
    target: CurrentTarget
    kind: str
    checks_due: tuple[str, ...] = ()


def plan_current_work(
    targets: Iterable[CurrentTarget], cursors: Mapping[str, ScopeCursor], *,
    now: datetime, limit: int = 8, turn: int = 0,
) -> tuple[CurrentWork, ...]:
    """Interleave fresh checks and continuation, including limit=1 via turn.

    A successfully served scope advances its last_work_at; an error is recorded
    by the caller with that attempt time, so it cannot block all other scopes.
    Neither a batch count nor this planner replaces the runtime 45-minute limit.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise CurrentStateError('limit must be a positive integer')
    if isinstance(turn, bool) or not isinstance(turn, int) or turn < 0:
        raise CurrentStateError('turn must be a nonnegative integer')
    timestamp = _time(now)
    fresh: list[tuple[tuple[Any, ...], CurrentWork]] = []
    resume: list[tuple[tuple[Any, ...], CurrentWork]] = []
    minimum = datetime.min.replace(tzinfo=timezone.utc)
    seen: set[str] = set()
    for target in targets:
        if target.scope_id in seen:
            raise CurrentStateError('duplicate current target')
        seen.add(target.scope_id)
        cursor = cursors.get(target.scope_id, ScopeCursor(target.scope_id))
        if cursor.scope_id != target.scope_id:
            raise CurrentStateError('cursor belongs to another scope')
        due = cursor.daily_due(timestamp)
        last_work = cursor.last_work_at or minimum
        if due:
            oldest = min(getattr(cursor, f'{name}_checked_at') or minimum for name in due)
            # A never-successful scope must move behind due healthy scopes
            # after an attempted check, rather than owning datetime.min forever.
            fresh.append(((max(oldest, last_work), oldest, target.cold_priority), CurrentWork(target, 'signals', due)))
        if not cursor.cold_complete or json.loads(cursor.resume_json):
            resume.append(((last_work, target.cold_priority), CurrentWork(target, 'resume')))
        elif cursor.roster_due(timestamp):
            resume.append(((last_work, target.cold_priority), CurrentWork(target, 'weekly_roster')))
    queues = [[item[1] for item in sorted(fresh, key=lambda pair: pair[0])],
              [item[1] for item in sorted(resume, key=lambda pair: pair[0])]]
    chosen: list[CurrentWork] = []
    index = turn % 2
    while len(chosen) < limit and any(queues):
        selected = index if queues[index] else 1 - index
        chosen.append(queues[selected].pop(0))
        index = 1 - index
    return tuple(chosen)
