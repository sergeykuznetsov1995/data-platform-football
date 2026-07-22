"""Pure, fail-closed scope planning for Transfermarkt native v2.

The module intentionally has no Airflow or database imports.  A DAG task may
load the promoted registry, call :func:`plan_transfermarkt_scopes`, and map the
returned JSON-compatible payloads over one bounded TaskGroup.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any, Iterable, Literal, Mapping, Sequence

from scrapers.transfermarkt.registry import (
    ClassificationStatus,
    CompetitionRecord,
    EditionRecord,
    RegistryConflictError,
    RegistryError,
    canonical_season,
    deterministic_scope_id,
)
from utils.transfermarkt_scope_state import (
    CAPTURE_REVISION,
    SCOPE_COMPLETION_STATUS,
)


RESULT_ROOT = '/opt/airflow/logs/transfermarkt-native-v2'
MAX_BATCH_SIZE = 8
CURRENT_SCOPE_INTERVAL = timedelta(days=6)
# Half of every batch pays down career debt, half buys new coverage.  See
# ``_quota_order``: neither obligation may starve the other.
CAREER_DEBT_BATCH_SHARE = 0.5
ScopeSelectionMode = Literal['all_due', 'current_only', 'historical_only']
SCOPE_SELECTION_MODES = frozenset({
    'all_due', 'current_only', 'historical_only',
})

REGISTRY_STATE_TABLE = 'iceberg.ops.transfermarkt_registry_state_v2'
SCOPE_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_scope_manifest_v2'
COMPETITIONS_TABLE = 'iceberg.silver.transfermarkt_competitions_v2'
EDITIONS_TABLE = 'iceberg.silver.transfermarkt_competition_editions_v2'


class ScopePlanningError(ValueError):
    """The requested scope cannot be planned safely and unambiguously."""


@dataclass(frozen=True)
class ParentCycleLedger:
    """One shared proxy ledger identity for every child of a parent cycle."""

    parent_cycle_id: str
    ledger_id: str
    path: str

    def as_dict(self) -> dict[str, str]:
        return {
            'parent_cycle_id': self.parent_cycle_id,
            'ledger_id': self.ledger_id,
            'path': self.path,
        }


@dataclass(frozen=True)
class ScopePlan:
    """A bounded, deterministic set of mapped TaskGroup arguments."""

    parent_cycle_id: str
    selection_hash: str
    mapped_payloads: tuple[dict[str, Any], ...]
    continuation_required: bool
    remaining_count: int
    total_selected_count: int
    parent_ledger: ParentCycleLedger

    def as_dict(self) -> dict[str, Any]:
        """Return a value accepted by JSON XCom serializers."""

        return {
            'parent_cycle_id': self.parent_cycle_id,
            'selection_hash': self.selection_hash,
            'mapped_payloads': [dict(item) for item in self.mapped_payloads],
            'continuation_required': self.continuation_required,
            'remaining_count': self.remaining_count,
            'total_selected_count': self.total_selected_count,
            'parent_ledger': self.parent_ledger.as_dict(),
        }


@dataclass(frozen=True)
class RegistryScopeTarget:
    """One exact eligible registry scope required in a complete model slot.

    The target also carries the meaning the registry currently assigns to the
    scope.  A slot is assembled from manifests captured over months, across many
    registry snapshots, and a manifest stays valid evidence only while the
    registry still says the same thing about what it captured — same canonical
    identity, same classification.  Anything else must be crawled again.
    """

    scope_id: str
    competition_id: str
    edition_id: str
    current: bool
    canonical_competition_id: str = ''
    canonical_season: str = ''
    competition_type: str = ''
    team_type: str = ''
    gender: str = ''
    age_category: str = ''

    def as_dict(self) -> dict[str, Any]:
        return {
            'scope_id': self.scope_id,
            'competition_id': self.competition_id,
            'edition_id': self.edition_id,
            'current': self.current,
            'canonical_competition_id': self.canonical_competition_id,
            'canonical_season': self.canonical_season,
            'competition_type': self.competition_type,
            'team_type': self.team_type,
            'gender': self.gender,
            'age_category': self.age_category,
        }


@dataclass(frozen=True)
class _Candidate:
    competition: CompetitionRecord
    edition: EditionRecord
    last_success_at: datetime | None
    career_fetches_pending: int = 0
    explicit_order: int | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.competition.competition_id, self.edition.edition_id)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode('utf-8')).hexdigest()


def _required_text(name: str, value: Any) -> str:
    text = str(value).strip()
    if not text:
        raise ScopePlanningError(f'{name} is required')
    return text


def _as_utc(value: Any) -> datetime | None:
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(
                str(value).strip().replace('Z', '+00:00')
            )
        except ValueError as exc:
            raise ScopePlanningError(
                f'invalid registry timestamp: {value!r}'
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ScopePlanningError('registry timestamps must include a timezone')
    return parsed.astimezone(timezone.utc)


def _normalise_sequence(value: Any, *, name: str) -> tuple[Any, ...]:
    if value in (None, '', (), []):
        return ()
    if isinstance(value, str):
        values = tuple(item.strip() for item in value.split(',') if item.strip())
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = tuple(value)
    else:
        raise ScopePlanningError(f'params.{name} must be a list or string')
    return values


def _competition_from_joined_row(row: Mapping[str, Any]) -> CompetitionRecord:
    value = {
        'competition_id': row.get('competition_id'),
        'slug': row.get('slug'),
        'name': row.get('name'),
        'country': row.get('country'),
        'confederation': row.get('confederation'),
        'competition_type': row.get('competition_type'),
        'gender': row.get('gender'),
        'team_type': row.get('team_type'),
        'age_category': row.get('age_category'),
        'season_format': row.get('competition_season_format'),
        'active': row.get('competition_active'),
        'source_url': row.get('competition_source_url'),
        'discovered_at': row.get('competition_discovered_at'),
        'canonical_competition_id': row.get('canonical_competition_id'),
        'classification_evidence': row.get('classification_evidence', ()),
        'registry_snapshot_id': row.get('registry_snapshot_id'),
        'source_body_hash': row.get('competition_source_body_hash', ''),
        'parser_revision': row.get('competition_parser_revision', 'registry-v1'),
        'schema_revision': row.get('competition_schema_revision', '1'),
        'aliases': row.get('aliases', ()),
    }
    record = CompetitionRecord.from_mapping(value)
    supplied_status = row.get('classification_status')
    if (
        supplied_status not in (None, '')
        and str(supplied_status) != record.classification_status.value
    ):
        raise ScopePlanningError(
            f'{record.competition_id}: promoted classification_status '
            f'{supplied_status!r} disagrees with evidence-derived '
            f'{record.classification_status.value!r}'
        )
    return record


def _edition_from_joined_row(row: Mapping[str, Any]) -> EditionRecord:
    return EditionRecord.from_mapping({
        'competition_id': row.get('competition_id'),
        'edition_id': row.get('edition_id'),
        'edition_label': row.get('edition_label'),
        'canonical_season': row.get('canonical_season'),
        'season_format': row.get('edition_season_format'),
        'start_date': row.get('start_date'),
        'end_date': row.get('end_date'),
        'active': row.get('edition_active'),
        'current': row.get('is_current'),
        'participant_count': row.get('participant_count'),
        'participant_hash': row.get('participant_hash'),
        'source_url': row.get('edition_source_url'),
        'discovered_at': row.get('edition_discovered_at'),
        'registry_snapshot_id': row.get('registry_snapshot_id'),
        'source_body_hash': row.get('edition_source_body_hash', ''),
        'parser_revision': row.get('edition_parser_revision', 'registry-v1'),
        'schema_revision': row.get('edition_schema_revision', '1'),
    })


def _load_registry(
    *,
    competitions: Iterable[CompetitionRecord | Mapping[str, Any]] | None,
    editions: Iterable[EditionRecord | Mapping[str, Any]] | None,
    registry_rows: Iterable[Mapping[str, Any]] | None,
) -> tuple[
    dict[str, CompetitionRecord],
    dict[tuple[str, str], EditionRecord],
    dict[tuple[str, str], datetime | None],
    dict[tuple[str, str], int],
]:
    competition_map: dict[str, CompetitionRecord] = {}
    edition_map: dict[tuple[str, str], EditionRecord] = {}
    last_success: dict[tuple[str, str], datetime | None] = {}
    career_pending: dict[tuple[str, str], int] = {}

    def add_competition(record: CompetitionRecord) -> None:
        existing = competition_map.get(record.competition_id)
        if existing is not None and existing != record:
            raise ScopePlanningError(
                f'conflicting competition rows for {record.competition_id}'
            )
        competition_map[record.competition_id] = record

    def add_edition(record: EditionRecord) -> None:
        key = (record.competition_id, record.edition_id)
        existing = edition_map.get(key)
        if existing is not None and existing != record:
            raise ScopePlanningError(
                f'conflicting edition rows for {record.competition_id}/'
                f'{record.edition_id}'
            )
        edition_map[key] = record

    try:
        for value in competitions or ():
            add_competition(
                value
                if isinstance(value, CompetitionRecord)
                else CompetitionRecord.from_mapping(value)
            )
        for value in editions or ():
            add_edition(
                value
                if isinstance(value, EditionRecord)
                else EditionRecord.from_mapping(value)
            )
        for row in registry_rows or ():
            competition = _competition_from_joined_row(row)
            edition = _edition_from_joined_row(row)
            add_competition(competition)
            add_edition(edition)
            key = (competition.competition_id, edition.edition_id)
            observed = _as_utc(row.get('last_success_at'))
            if key in last_success and last_success[key] != observed:
                raise ScopePlanningError(
                    f'conflicting last_success_at rows for {key[0]}/{key[1]}'
                )
            last_success[key] = observed
            career_pending[key] = int(row.get('career_fetches_pending') or 0)
    except (RegistryError, RegistryConflictError) as exc:
        raise ScopePlanningError(str(exc)) from exc

    if not competition_map or not edition_map:
        raise ScopePlanningError('the promoted registry contains no scopes')
    orphan_editions = sorted(
        key for key in edition_map if key[0] not in competition_map
    )
    if orphan_editions:
        raise ScopePlanningError(
            f'edition has no competition row: {orphan_editions[0]}'
        )
    return competition_map, edition_map, last_success, career_pending


def _competition_aliases(record: CompetitionRecord) -> set[str]:
    values = {
        record.competition_id,
        record.slug,
        record.name,
        *record.aliases,
    }
    if record.canonical_competition_id:
        values.add(record.canonical_competition_id)
    return {str(value).strip().casefold() for value in values if str(value).strip()}


def _resolve_competition(
    value: Any,
    competitions: Mapping[str, CompetitionRecord],
) -> CompetitionRecord:
    needle = _required_text('competition selector', value).casefold()
    matches = [
        item for item in competitions.values()
        if needle in _competition_aliases(item)
    ]
    if len(matches) != 1:
        if not matches:
            raise ScopePlanningError(f'unknown competition selector: {value!r}')
        raise ScopePlanningError(f'ambiguous competition selector: {value!r}')
    return matches[0]


def _assert_crawlable(record: CompetitionRecord) -> None:
    if not record.active:
        raise ScopePlanningError(f'{record.competition_id}: competition is inactive')
    if record.classification_status is not ClassificationStatus.ELIGIBLE:
        raise ScopePlanningError(
            f'{record.competition_id}: classification blocks crawl: '
            f'{record.classification_status.value}; {record.crawl_block_reason}'
        )


def _resolve_edition(
    competition: CompetitionRecord,
    edition_selector: Any,
    editions: Mapping[tuple[str, str], EditionRecord],
) -> EditionRecord:
    raw = _required_text('edition selector', edition_selector)
    available = [
        edition for key, edition in editions.items()
        if key[0] == competition.competition_id
    ]
    # A calendar-year league offsets its saison_id from the season it names:
    # 2DVB's edition '2024' is season 2025, and season 2024 is edition '2023'.
    # Matching the three fields as one set makes every such selector ambiguous,
    # so read them narrowest first: the source's own key, then its label.
    matches: list[EditionRecord] = []
    for field in ('edition_id', 'edition_label', 'canonical_season'):
        matches = [
            item for item in available if getattr(item, field) == raw
        ]
        if matches:
            break
    if not matches:
        try:
            requested_canonical = canonical_season(
                raw, competition.season_format
            )
        except RegistryError as exc:
            raise ScopePlanningError(
                f'{competition.competition_id}: edition {raw!r} does not match '
                'the central registry season semantics'
            ) from exc
        matches = [
            item for item in available
            if item.canonical_season == requested_canonical
        ]
    if len(matches) != 1:
        if not matches:
            raise ScopePlanningError(
                f'{competition.competition_id}: unknown edition {raw!r}'
            )
        raise ScopePlanningError(
            f'{competition.competition_id}: ambiguous edition {raw!r}'
        )
    edition = matches[0]
    if not edition.active:
        raise ScopePlanningError(
            f'{competition.competition_id}/{edition.edition_id}: edition is inactive'
        )
    if edition.season_format is not competition.season_format:
        raise ScopePlanningError(
            f'{competition.competition_id}/{edition.edition_id}: '
            'competition/edition season_format mismatch'
        )
    return edition


def _scope_spec(value: Any) -> tuple[Any, Any]:
    if isinstance(value, Mapping):
        competition = value.get('competition_id', value.get('competition'))
        edition = value.get('edition_id', value.get('edition'))
        return competition, edition
    text = _required_text('scope selector', value)
    match = re.fullmatch(r'([^:/]+)[:/]([^:/]+)', text)
    if match is None:
        raise ScopePlanningError(
            'scope strings must be competition_id:edition_id'
        )
    return match.group(1), match.group(2)


def _is_due(candidate: _Candidate, now: datetime) -> bool:
    if candidate.last_success_at is None:
        return True
    # A cycle buys a bounded window of a roster's careers.  A scope that still
    # owes some is not finished with, whatever its last manifest was called —
    # and a historical edition is otherwise never asked for again, so the
    # careers it still owed would never be bought at all.
    if candidate.career_fetches_pending > 0:
        return True
    if candidate.edition.current:
        return candidate.last_success_at <= now - CURRENT_SCOPE_INTERVAL
    return False


def _assert_selection_mode(
    candidate: _Candidate,
    *,
    selection_mode: ScopeSelectionMode,
) -> None:
    """Keep scheduled current traffic disjoint from historical campaigns."""

    current = bool(candidate.edition.current)
    if selection_mode == 'current_only' and not current:
        raise ScopePlanningError(
            f'{candidate.competition.competition_id}/'
            f'{candidate.edition.edition_id}: historical scopes belong to '
            'dag_backfill_transfermarkt'
        )
    if selection_mode == 'historical_only' and current:
        raise ScopePlanningError(
            f'{candidate.competition.competition_id}/'
            f'{candidate.edition.edition_id}: current scopes belong to '
            'dag_ingest_transfermarkt'
        )


def _select_candidates(
    params: Mapping[str, Any],
    *,
    competitions: Mapping[str, CompetitionRecord],
    editions: Mapping[tuple[str, str], EditionRecord],
    last_success: Mapping[tuple[str, str], datetime | None],
    now: datetime,
    career_pending: Mapping[tuple[str, str], int] | None = None,
    batch_size: int = MAX_BATCH_SIZE,
    selection_mode: ScopeSelectionMode = 'all_due',
) -> list[_Candidate]:
    scopes = _normalise_sequence(params.get('scopes'), name='scopes')
    leagues = _normalise_sequence(params.get('leagues'), name='leagues')
    season = params.get('season')
    selected: list[_Candidate] = []

    if scopes:
        for position, spec in enumerate(scopes):
            competition_value, edition_value = _scope_spec(spec)
            competition = _resolve_competition(competition_value, competitions)
            _assert_crawlable(competition)
            edition = _resolve_edition(competition, edition_value, editions)
            candidate = _Candidate(
                competition=competition,
                edition=edition,
                last_success_at=last_success.get(
                    (competition.competition_id, edition.edition_id)
                ),
                career_fetches_pending=int((career_pending or {}).get(
                    (competition.competition_id, edition.edition_id), 0
                )),
                explicit_order=position,
            )
            _assert_selection_mode(candidate, selection_mode=selection_mode)
            selected.append(candidate)
    elif leagues:
        for position, league in enumerate(leagues):
            competition = _resolve_competition(league, competitions)
            _assert_crawlable(competition)
            if season not in (None, ''):
                chosen = _resolve_edition(competition, season, editions)
            else:
                current = [
                    item for key, item in editions.items()
                    if key[0] == competition.competition_id
                    and item.active and item.current
                ]
                if len(current) != 1:
                    raise ScopePlanningError(
                        f'{competition.competition_id}: leagues without season '
                        'require exactly one active current edition'
                    )
                chosen = current[0]
            candidate = _Candidate(
                competition=competition,
                edition=chosen,
                last_success_at=last_success.get(
                    (competition.competition_id, chosen.edition_id)
                ),
                career_fetches_pending=int((career_pending or {}).get(
                    (competition.competition_id, chosen.edition_id), 0
                )),
                explicit_order=position,
            )
            _assert_selection_mode(candidate, selection_mode=selection_mode)
            selected.append(candidate)
    else:
        if season not in (None, ''):
            raise ScopePlanningError('params.season requires leagues or scopes')
        blocked = sorted(
            item.competition_id
            for item in competitions.values()
            if item.active
            and item.classification_status in {
                ClassificationStatus.UNKNOWN,
                ClassificationStatus.CONFLICT,
            }
        )
        if blocked:
            raise ScopePlanningError(
                'active registry classifications block crawl: '
                + ', '.join(blocked)
            )
        for key, edition in editions.items():
            competition = competitions[key[0]]
            if not competition.crawl_eligible or not edition.active:
                continue
            if selection_mode == 'current_only' and not edition.current:
                continue
            if selection_mode == 'historical_only' and edition.current:
                continue
            candidate = _Candidate(
                competition=competition,
                edition=edition,
                last_success_at=last_success.get(key),
                career_fetches_pending=int((career_pending or {}).get(key, 0)),
            )
            if _is_due(candidate, now):
                selected.append(candidate)
        selected = _quota_order(selected, batch_size=batch_size)

    deduplicated: list[_Candidate] = []
    seen: set[tuple[str, str]] = set()
    for item in selected:
        if item.key in seen:
            continue
        seen.add(item.key)
        deduplicated.append(item)
    return deduplicated


def _quota_order(
    candidates: Sequence[_Candidate], *, batch_size: int,
) -> list[_Candidate]:
    """Share every batch between paying old debt and buying new coverage.

    The two obligations pull against each other and BOTH have to be reachable:

    * the cutover career-debt gate measures the debt of the WHOLE slot, so debt
      has to be paid down — a scope that owes careers is half-captured;
    * coverage has to grow, or the legacy floor (and the target) is never met.

    Neither may be a strict priority.  Debt-first starves coverage: a current
    edition re-earns debt every time it is refreshed, so the debt queue never
    empties and no new competition is ever bought.  Coverage-first starves debt:
    ~9.7k never-captured scopes would all have to be crawled before the first
    owed career is fetched.  So the batch is split by QUOTA — half its slots for
    debt, half for new coverage — and whichever side runs out donates its slots
    to the other.  Both goals then advance every single cycle.
    """

    debt = sorted(
        (item for item in candidates if item.career_fetches_pending > 0),
        key=lambda item: (
            -item.career_fetches_pending,
            item.last_success_at or datetime.min.replace(tzinfo=timezone.utc),
            item.competition.competition_id,
            item.edition.edition_id,
        ),
    )
    growth = sorted(
        (item for item in candidates if item.career_fetches_pending == 0),
        key=lambda item: (
            item.last_success_at is not None,
            item.last_success_at or datetime.min.replace(tzinfo=timezone.utc),
            item.competition.competition_id,
            item.edition.edition_id,
        ),
    )
    # The quota is INTERLEAVED, not blocked.  A batch of eight is planned, but
    # the parent byte cap only pays for about three scopes a day, so a block of
    # debt slots at the head of the batch would consume the whole day's budget
    # and coverage would never move — the same starvation, one level down.
    # Alternating the two duties makes both advance inside every real day's
    # spend, whatever the budget stops at.
    ordered: list[_Candidate] = []
    debt_index = growth_index = 0
    credit = CAREER_DEBT_BATCH_SHARE
    while debt_index < len(debt) or growth_index < len(growth):
        credit += CAREER_DEBT_BATCH_SHARE
        wants_debt = credit >= 1.0
        if wants_debt:
            credit -= 1.0
        if wants_debt and debt_index < len(debt):
            ordered.append(debt[debt_index])
            debt_index += 1
        elif growth_index < len(growth):
            ordered.append(growth[growth_index])
            growth_index += 1
        elif debt_index < len(debt):
            # One queue is empty: the other takes the whole batch.
            ordered.append(debt[debt_index])
            debt_index += 1
    return ordered


def eligible_registry_scopes(
    registry_rows: Iterable[Mapping[str, Any]],
) -> tuple[RegistryScopeTarget, ...]:
    """Return the full active senior-men target set for an A/B slot.

    Crawl planning may return only one bounded batch.  Slot promotion must use
    this complete registry-derived set so a small refresh can never replace a
    previously served competition with a partial table.
    """

    competitions, editions, _, _ = _load_registry(
        competitions=None,
        editions=None,
        registry_rows=registry_rows,
    )
    blocked = sorted(
        item.competition_id
        for item in competitions.values()
        if item.active
        and item.classification_status in {
            ClassificationStatus.UNKNOWN,
            ClassificationStatus.CONFLICT,
        }
    )
    if blocked:
        raise ScopePlanningError(
            'active registry classifications block slot coverage: '
            + ', '.join(blocked)
        )
    targets = tuple(
        RegistryScopeTarget(
            scope_id=deterministic_scope_id(*key),
            competition_id=key[0],
            edition_id=key[1],
            current=bool(edition.current),
            canonical_competition_id=(
                competitions[key[0]].canonical_competition_id
                or f'TM-{key[0]}'
            ),
            canonical_season=str(edition.canonical_season),
            competition_type=competitions[key[0]].competition_type.value,
            team_type=competitions[key[0]].team_type.value,
            gender=competitions[key[0]].gender.value,
            age_category=competitions[key[0]].age_category.value,
        )
        for key, edition in sorted(editions.items())
        if competitions[key[0]].crawl_eligible and edition.active
    )
    if not targets:
        raise ScopePlanningError('promoted registry has no eligible active scopes')
    return targets


def _result_paths(
    *,
    root: str,
    parent_hash: str,
    candidate: _Candidate,
    child_cycle_id: str,
) -> dict[str, str]:
    content_hash = _digest({
        'parent_hash': parent_hash,
        'scope_id': deterministic_scope_id(*candidate.key),
        'child_cycle_id': child_cycle_id,
        'registry_snapshot_id': candidate.edition.registry_snapshot_id,
        'source_body_hash': candidate.edition.source_body_hash,
    })
    base = (
        PurePosixPath(root)
        / 'cycles'
        / parent_hash
        / 'scopes'
        / deterministic_scope_id(*candidate.key)
        / content_hash
    )
    return {
        'base_dir': str(base),
        'capture_result': str(base / 'capture-result.json'),
        'scope_manifest': str(base / 'scope-manifest.json'),
        'entity_staging_dir': str(base / 'entities'),
    }


def plan_transfermarkt_scopes(
    params: Mapping[str, Any] | None,
    *,
    parent_cycle_id: str,
    competitions: Iterable[CompetitionRecord | Mapping[str, Any]] | None = None,
    editions: Iterable[EditionRecord | Mapping[str, Any]] | None = None,
    registry_rows: Iterable[Mapping[str, Any]] | None = None,
    now: datetime | None = None,
    max_batch_size: int = MAX_BATCH_SIZE,
    result_root: str = RESULT_ROOT,
    selection_mode: ScopeSelectionMode = 'all_due',
    resume_cycle_id: str | None = None,
) -> ScopePlan:
    """Plan one bounded mapping batch without network, SQL, or Airflow calls.

    Exact ``params.scopes`` take precedence over legacy ``leagues``/``season``
    defaults.  With no selector, only due scopes are returned, never-served
    scopes first and then the oldest successful current editions.
    """

    cycle_id = _required_text('parent_cycle_id', parent_cycle_id)
    resume_id = _required_text(
        'resume_cycle_id', resume_cycle_id or cycle_id,
    )
    if selection_mode not in SCOPE_SELECTION_MODES:
        raise ScopePlanningError(
            f'selection_mode must be one of {sorted(SCOPE_SELECTION_MODES)}'
        )
    if isinstance(max_batch_size, bool) or not 1 <= int(max_batch_size) <= 8:
        raise ScopePlanningError('max_batch_size must be between 1 and 8')
    batch_size = int(max_batch_size)
    root = PurePosixPath(_required_text('result_root', result_root))
    if not root.is_absolute():
        raise ScopePlanningError('result_root must be an absolute path')
    current_time = _as_utc(now or datetime.now(timezone.utc))
    assert current_time is not None

    competition_map, edition_map, last_success, career_pending = _load_registry(
        competitions=competitions,
        editions=editions,
        registry_rows=registry_rows,
    )
    selected = _select_candidates(
        params or {},
        competitions=competition_map,
        editions=edition_map,
        last_success=last_success,
        career_pending=career_pending,
        now=current_time,
        batch_size=batch_size,
        selection_mode=selection_mode,
    )
    selection_identity = [
        {
            'competition_id': item.competition.competition_id,
            'edition_id': item.edition.edition_id,
            'registry_snapshot_id': (
                item.edition.registry_snapshot_id
                or item.competition.registry_snapshot_id
            ),
        }
        for item in selected
    ]
    selection_hash = _digest(selection_identity)
    parent_hash = _digest({'parent_cycle_id': cycle_id})
    resume_hash = _digest({'resume_cycle_id': resume_id})
    ledger = ParentCycleLedger(
        parent_cycle_id=cycle_id,
        ledger_id=f'tm-ledger-{parent_hash[:24]}',
        path=str(
            root / 'cycles' / parent_hash / 'proxy-ledger.json'
        ),
    )
    remaining_count = max(0, len(selected) - batch_size)
    continuation_required = remaining_count > 0
    payloads: list[dict[str, Any]] = []
    for candidate in selected[:batch_size]:
        competition = candidate.competition
        edition = candidate.edition
        snapshot_id = (
            edition.registry_snapshot_id or competition.registry_snapshot_id
        )
        if not snapshot_id:
            raise ScopePlanningError(
                f'{competition.competition_id}/{edition.edition_id}: '
                'registry_snapshot_id is required for a mapped crawl'
            )
        if (
            competition.registry_snapshot_id
            and edition.registry_snapshot_id
            and competition.registry_snapshot_id != edition.registry_snapshot_id
        ):
            raise ScopePlanningError(
                f'{competition.competition_id}/{edition.edition_id}: '
                'competition/edition registry snapshot mismatch'
            )
        scope_id = deterministic_scope_id(
            competition.competition_id, edition.edition_id
        )
        child_hash = _digest({
            'resume_cycle_id': resume_id,
            'scope_id': scope_id,
            'registry_snapshot_id': snapshot_id,
        })
        child_cycle_id = f'tm-child-{child_hash[:24]}'
        payloads.append({
            'parent_cycle_id': cycle_id,
            'resume_cycle_id': resume_id,
            'child_cycle_id': child_cycle_id,
            'scope_id': scope_id,
            'competition_id': competition.competition_id,
            'edition_id': edition.edition_id,
            'canonical_competition_id': (
                competition.canonical_competition_id
                or f'TM-{competition.competition_id}'
            ),
            'canonical_season': edition.canonical_season,
            'competition_type': competition.competition_type.value,
            'season_format': competition.season_format.value,
            'source_url': edition.source_url,
            'registry_snapshot_id': snapshot_id,
            # The capture contract this scope is crawled under.  It has to be a
            # code revision, not batch content: the child wrapper writes it into
            # the manifest as ``capture_revision``, and a scope set may only be
            # assembled from manifests that share it.  Without an explicit value
            # the wrapper fell back to ``selection_hash`` — which is a digest of
            # this batch's candidate list — so every bounded batch produced its
            # own capture identity and no two batches could ever accumulate into
            # one slot.  The selection hash stays in the payload as crawl-plan
            # provenance; it is no longer an identity.
            'capture_revision': CAPTURE_REVISION,
            'selection_hash': selection_hash,
            'continuation_required': continuation_required,
            'remaining_count': remaining_count,
            'result_paths': _result_paths(
                root=str(root),
                parent_hash=resume_hash,
                candidate=candidate,
                child_cycle_id=child_cycle_id,
            ),
            'parent_ledger': ledger.as_dict(),
            # The mapped child receives the exact source-backed registry
            # records used by the planner.  This avoids falling back to a
            # bootstrap/static competition list inside the scraper process.
            'competition_record': competition.as_dict(),
            'edition_record': edition.as_dict(),
        })

    plan = ScopePlan(
        parent_cycle_id=cycle_id,
        selection_hash=selection_hash,
        mapped_payloads=tuple(payloads),
        continuation_required=continuation_required,
        remaining_count=remaining_count,
        total_selected_count=len(selected),
        parent_ledger=ledger,
    )
    # Keep the serialization guarantee close to the producer contract.
    json.dumps(plan.as_dict(), sort_keys=True)
    return plan


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_promoted_registry_query(
    *, registry_snapshot_id: str | None = None,
) -> str:
    """Build, but never execute, the promoted registry read query."""

    state_filter = "state_key = 'canonical'"
    snapshot_filter = ''
    if registry_snapshot_id is not None:
        snapshot = _required_text('registry_snapshot_id', registry_snapshot_id)
        # A campaign pins the logical registry snapshot, not the mutable
        # ``canonical`` pointer.  Promotion moves the previous canonical row to
        # ``history:<revision>`` before advancing that pointer, so an exact
        # pinned read must accept either representation of the same green,
        # promoted snapshot.  Unpinned discovery still reads canonical only.
        state_filter = (
            "(state_key = 'canonical' OR regexp_like("
            "state_key, '^history:[0-9]+$'))"
        )
        snapshot_filter = (
            ' AND registry_snapshot_id = ' + _sql_literal(snapshot)
        )
    return f"""WITH promoted AS (
    SELECT registry_snapshot_id
    FROM {REGISTRY_STATE_TABLE}
    WHERE {state_filter}
      AND status = 'promoted'
      AND unknown_active_count = 0{snapshot_filter}
    ORDER BY revision DESC
    LIMIT 1
), last_complete_scope AS (
    SELECT competition_id, edition_id,
           with_timezone(committed_at, 'UTC') AS last_success_at,
           COALESCE(TRY_CAST(json_extract_scalar(
               entity_manifest_json, '$.dq_evidence.career_fetches_pending'
           ) AS bigint), 0) AS career_fetches_pending
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY competition_id, edition_id
            ORDER BY committed_at DESC
        ) AS rn
        FROM {SCOPE_MANIFEST_TABLE}
        WHERE status = '{SCOPE_COMPLETION_STATUS}'
    )
    WHERE rn = 1
)
SELECT
    c.competition_id,
    c.slug,
    c.name,
    c.country,
    c.confederation,
    c.competition_type,
    c.gender,
    c.team_type,
    c.age_category,
    c.season_format AS competition_season_format,
    c.active AS competition_active,
    c.source_url AS competition_source_url,
    -- Iceberg stores these without a zone; discovery writes them in UTC.
    with_timezone(c.discovered_at, 'UTC') AS competition_discovered_at,
    c.canonical_competition_id,
    c.classification_status,
    c.classification_evidence,
    c.source_body_hash AS competition_source_body_hash,
    c.parser_revision AS competition_parser_revision,
    c.schema_revision AS competition_schema_revision,
    e.edition_id,
    e.edition_label,
    e.canonical_season,
    e.season_format AS edition_season_format,
    e.start_date,
    e.end_date,
    e.active AS edition_active,
    e.is_current,
    e.participant_count,
    e.participant_hash,
    e.source_url AS edition_source_url,
    with_timezone(e.discovered_at, 'UTC') AS edition_discovered_at,
    e.source_body_hash AS edition_source_body_hash,
    e.parser_revision AS edition_parser_revision,
    e.schema_revision AS edition_schema_revision,
    p.registry_snapshot_id,
    s.last_success_at,
    COALESCE(s.career_fetches_pending, 0) AS career_fetches_pending
FROM {COMPETITIONS_TABLE} c
JOIN {EDITIONS_TABLE} e
  ON e.competition_id = c.competition_id
JOIN promoted p
  ON p.registry_snapshot_id = c.registry_snapshot_id
 AND p.registry_snapshot_id = e.registry_snapshot_id
LEFT JOIN last_complete_scope s
  ON s.competition_id = c.competition_id
 AND s.edition_id = e.edition_id
ORDER BY c.competition_id, e.edition_id"""


__all__ = [
    'CURRENT_SCOPE_INTERVAL',
    'MAX_BATCH_SIZE',
    'RESULT_ROOT',
    'SCOPE_SELECTION_MODES',
    'ParentCycleLedger',
    'RegistryScopeTarget',
    'ScopePlan',
    'ScopePlanningError',
    'ScopeSelectionMode',
    'build_promoted_registry_query',
    'eligible_registry_scopes',
    'plan_transfermarkt_scopes',
]
