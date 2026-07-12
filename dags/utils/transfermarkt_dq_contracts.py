"""Type-aware completeness contracts for exact Transfermarkt scopes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence


class ScopeDQError(RuntimeError):
    pass


STRICT_TYPES = {
    'domestic_cup', 'continental_club', 'national_team_tournament',
}
SUPPORTED_TYPES = STRICT_TYPES | {'domestic_league'}
SUCCESS_STATUSES = {'ok', 'authoritative_empty', 'not_applicable'}
DOMESTIC_LEAGUE_MIN_PARTICIPANT_COVERAGE = 0.9
REQUIRED_NONEMPTY_ENTITIES = {
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
}
AUTHORITATIVE_EMPTY_ALLOWED_ENTITIES = {
    'market_value_points',
    'transfer_events',
    'coach_profiles',
    'coach_stints',
}
SUPPORTED_ENTITIES = (
    REQUIRED_NONEMPTY_ENTITIES | AUTHORITATIVE_EMPTY_ALLOWED_ENTITIES
)


@dataclass(frozen=True)
class ScopeDQInput:
    competition_type: str
    team_type: str
    current: bool
    expected_team_ids: tuple[str, ...]
    observed_team_ids: tuple[str, ...]
    endpoint_status_by_team: Mapping[str, str]
    entity_statuses: Mapping[str, str]
    fetched_at: datetime
    listing_status: str = 'ok'
    now: datetime | None = None


def contract_applicability(
    *, entity: str, competition_type: str, team_type: str,
) -> str:
    """Return the only allowed applicability state for a typed scope."""

    if competition_type not in SUPPORTED_TYPES:
        raise ScopeDQError(f'unsupported competition type {competition_type!r}')
    if team_type not in {'club', 'national_team'}:
        raise ScopeDQError(f'unsupported team type {team_type!r}')
    if entity == 'player_contract_observations' and team_type == 'national_team':
        return 'not_applicable'
    return 'applicable'


def entity_applicability_contract(
    *, entity: str, competition_type: str, team_type: str,
) -> dict[str, Any]:
    """Return the exact per-competition terminal-state contract for an entity."""

    applicability = contract_applicability(
        entity=entity,
        competition_type=competition_type,
        team_type=team_type,
    )
    if entity not in SUPPORTED_ENTITIES:
        raise ScopeDQError(f'unsupported entity contract {entity!r}')
    if applicability == 'not_applicable':
        return {
            'competition_type': competition_type,
            'team_type': team_type,
            'applicability': 'not_applicable',
            'allowed_statuses': ['not_applicable'],
            'minimum_rows': 0,
            'requires_authoritative_empty_evidence': False,
        }
    if entity in REQUIRED_NONEMPTY_ENTITIES:
        return {
            'competition_type': competition_type,
            'team_type': team_type,
            'applicability': 'applicable',
            'allowed_statuses': ['ok'],
            'minimum_rows': 1,
            'requires_authoritative_empty_evidence': False,
        }
    return {
        'competition_type': competition_type,
        'team_type': team_type,
        'applicability': 'applicable',
        'allowed_statuses': ['ok', 'authoritative_empty'],
        'minimum_rows': 0,
        'requires_authoritative_empty_evidence': True,
    }


def entity_applicability_contracts(
    *, entities: Iterable[str], competition_type: str, team_type: str,
) -> dict[str, dict[str, Any]]:
    return {
        entity: entity_applicability_contract(
            entity=entity,
            competition_type=competition_type,
            team_type=team_type,
        )
        for entity in sorted(set(entities))
    }


def validate_scope_capture(
    value: ScopeDQInput,
    *,
    expected_entities: Iterable[str],
    max_current_age: timedelta = timedelta(days=8),
) -> dict:
    if value.competition_type not in SUPPORTED_TYPES:
        raise ScopeDQError(
            f'unknown/unsupported classification blocks crawl: '
            f'{value.competition_type!r}'
        )
    if value.team_type not in {'club', 'national_team'}:
        raise ScopeDQError(f'unsupported team type {value.team_type!r}')
    if value.listing_status != 'ok':
        raise ScopeDQError(
            f'participant listing is not authoritative: {value.listing_status!r}'
        )
    expected = tuple(dict.fromkeys(str(team) for team in value.expected_team_ids))
    observed = tuple(dict.fromkeys(str(team) for team in value.observed_team_ids))
    if not expected:
        raise ScopeDQError('authoritative participant manifest is empty')
    expected_set = set(expected)
    observed_set = set(observed)
    endpoint_teams = {str(team) for team in value.endpoint_status_by_team}
    if endpoint_teams != expected_set:
        raise ScopeDQError(
            'participant endpoint manifest mismatch: '
            f'missing={sorted(expected_set - endpoint_teams)} '
            f'extra={sorted(endpoint_teams - expected_set)}'
        )
    successful_endpoints = {
        str(team)
        for team, status in value.endpoint_status_by_team.items()
        if status == 'ok'
    }
    if observed_set != successful_endpoints:
        raise ScopeDQError(
            'participant endpoints are not authoritative: '
            f'observed_only={sorted(observed_set - successful_endpoints)} '
            f'endpoint_only={sorted(successful_endpoints - observed_set)}'
        )
    missing = sorted(expected_set - observed_set)
    extra = sorted(observed_set - expected_set)
    strict = value.competition_type in STRICT_TYPES
    coverage = len(observed_set & expected_set) / len(expected_set)
    if extra or (strict and missing):
        raise ScopeDQError(
            f'participant mismatch: missing={missing} extra={extra}'
        )
    if (
        not strict
        and coverage < DOMESTIC_LEAGUE_MIN_PARTICIPANT_COVERAGE
    ):
        raise ScopeDQError(
            'domestic-league participant endpoint coverage is below '
            f'{DOMESTIC_LEAGUE_MIN_PARTICIPANT_COVERAGE:.0%}: '
            f'{len(observed_set)}/{len(expected_set)}'
        )
    required_entities = set(expected_entities)
    if set(value.entity_statuses) != required_entities:
        raise ScopeDQError(
            'entity manifest mismatch: '
            f'expected={sorted(required_entities)} '
            f'got={sorted(value.entity_statuses)}'
        )
    invalid = {
        entity: status
        for entity, status in value.entity_statuses.items()
        if status not in SUCCESS_STATUSES
    }
    if invalid:
        raise ScopeDQError(f'non-authoritative entity results: {invalid}')
    contracts = entity_applicability_contracts(
        entities=required_entities,
        competition_type=value.competition_type,
        team_type=value.team_type,
    )
    for entity, contract in contracts.items():
        status = value.entity_statuses[entity]
        if status in contract['allowed_statuses']:
            continue
        if entity == 'player_contract_observations':
            if contract['applicability'] == 'not_applicable':
                raise ScopeDQError(
                    'national-team contract observations must be not_applicable'
                )
            if status == 'not_applicable':
                raise ScopeDQError(
                    'club contract observations cannot be not_applicable'
                )
        raise ScopeDQError(
            f'{entity}: status {status!r} violates explicit '
            f'{value.competition_type}/{value.team_type} applicability contract'
        )
    now = value.now or datetime.now(timezone.utc)
    fetched = value.fetched_at
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    if value.current and now - fetched > max_current_age:
        raise ScopeDQError('current scope is stale')
    return {
        'passed': True,
        'competition_type': value.competition_type,
        'strict': strict,
        'participant_count': len(expected),
        'observed_participant_count': len(observed_set),
        'participant_coverage': coverage,
        'endpoint_coverage': coverage,
        'minimum_participant_coverage': (
            1.0 if strict else DOMESTIC_LEAGUE_MIN_PARTICIPANT_COVERAGE
        ),
        'fresh': True,
    }


def input_from_capture(
    capture: Mapping[str, Any],
    *,
    entity_statuses: Mapping[str, str],
    current: bool,
    now: datetime | None = None,
) -> ScopeDQInput:
    """Parse persisted runner evidence without accepting loose values."""

    if not isinstance(capture, Mapping):
        raise ScopeDQError('scope capture evidence must be an object')

    def _required_text(field: str) -> str:
        value = capture.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ScopeDQError(f'scope capture {field} is required')
        return value.strip()

    def _team_ids(field: str) -> tuple[str, ...]:
        value = capture.get(field)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise ScopeDQError(f'scope capture {field} must be a list')
        result = tuple(str(item).strip() for item in value)
        if any(not item for item in result):
            raise ScopeDQError(f'scope capture {field} contains an empty id')
        return result

    endpoint_statuses = capture.get('endpoint_status_by_team')
    if not isinstance(endpoint_statuses, Mapping):
        raise ScopeDQError(
            'scope capture endpoint_status_by_team must be an object'
        )
    parsed_endpoints = {
        str(team).strip(): str(status).strip()
        for team, status in endpoint_statuses.items()
    }
    if any(not team or not status for team, status in parsed_endpoints.items()):
        raise ScopeDQError('scope capture endpoint evidence contains an empty value')

    raw_fetched_at = capture.get('fetched_at')
    if isinstance(raw_fetched_at, datetime):
        fetched_at = raw_fetched_at
    elif isinstance(raw_fetched_at, str):
        try:
            fetched_at = datetime.fromisoformat(
                raw_fetched_at.strip().replace('Z', '+00:00')
            )
        except ValueError as exc:
            raise ScopeDQError('scope capture fetched_at is invalid') from exc
    else:
        raise ScopeDQError('scope capture fetched_at is required')

    # A parsed team list without exact response lineage is not authoritative
    # evidence and cannot be reused by a resumed child cycle.
    _required_text('listing_source_url')
    _required_text('listing_source_body_hash')

    return ScopeDQInput(
        competition_type=_required_text('competition_type'),
        team_type=_required_text('team_type'),
        current=bool(current),
        expected_team_ids=_team_ids('expected_team_ids'),
        observed_team_ids=_team_ids('observed_team_ids'),
        endpoint_status_by_team=parsed_endpoints,
        entity_statuses=dict(entity_statuses),
        fetched_at=fetched_at,
        listing_status=_required_text('listing_status'),
        now=now,
    )


def bidirectional_set_delta(
    left: Sequence[tuple], right: Sequence[tuple],
) -> dict[str, object]:
    """Small offline analogue of the two EXCEPT queries used at cutover."""

    left_set, right_set = set(left), set(right)
    left_only = sorted(left_set - right_set)
    right_only = sorted(right_set - left_set)
    return {
        'passed': not left_only and not right_only,
        'left_only': left_only,
        'right_only': right_only,
    }
