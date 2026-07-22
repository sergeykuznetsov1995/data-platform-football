"""Immutable Transfermarkt registry/scope manifests used by native-v2 cutover.

The legacy control plane proves one league/season.  This module adds the
source-wide evidence layer without weakening that rollback-compatible state:
every mapped child commits an exact scope manifest and a coordinator freezes
their sorted membership into one content-addressed ``scope_set_id``.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence


REGISTRY_STATE_TABLE = 'iceberg.ops.transfermarkt_registry_state_v2'
SCOPE_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_scope_manifest_v2'
SCOPE_SET_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_scope_set_manifest_v2'
PROXY_LEDGER_TABLE = 'iceberg.ops.transfermarkt_proxy_ledger_v2'
SCOPE_COMPLETION_STATUS = 'complete'
# One inactive slot is assembled cumulatively from many bounded paid batches:
# the promoted registry target is ~9.7k senior-men scopes and a single crawl
# batch buys at most eight of them.  The cap therefore bounds the manifest, not
# the crawl, and must stay above the whole eligible target.
MAX_SCOPE_SET_SIZE = 16_384
# The capture contract revision of a scope manifest.  It is a code/contract
# marker, not batch content: manifests captured under the same contract may be
# accumulated into one scope set across parent cycles.  Bump it whenever the
# capture payload (scope_capture, entity evidence) changes shape.
CAPTURE_REVISION = 'v2'


class ScopeManifestError(RuntimeError):
    """A child or scope-set manifest is incomplete or internally inconsistent."""


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class EntityEvidence:
    entity: str
    applicability_status: str
    expected_rows: int | None
    raw_rows: int
    dedup_rows: int
    key_hash: str
    content_hash: str
    dq_status: str
    decoded_bytes: int
    wire_bytes: int
    provider_metered_bytes: int
    requests: int
    retries: int
    cache_hits: int
    duration_ms: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'EntityEvidence':
        """Parse the on-disk/ops JSON contract without permissive defaults."""

        required = {
            'entity', 'applicability_status', 'expected_rows', 'raw_rows',
            'dedup_rows', 'key_hash', 'content_hash', 'dq_status',
            'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
            'requests', 'retries', 'cache_hits', 'duration_ms',
        }
        missing = sorted(required - set(value))
        if missing:
            raise ScopeManifestError(
                f'entity evidence is missing fields: {missing}'
            )
        expected_rows = value['expected_rows']
        item = cls(
            entity=str(value['entity']),
            applicability_status=str(value['applicability_status']),
            expected_rows=(
                None if expected_rows is None else int(expected_rows)
            ),
            raw_rows=int(value['raw_rows']),
            dedup_rows=int(value['dedup_rows']),
            key_hash=str(value['key_hash']),
            content_hash=str(value['content_hash']),
            dq_status=str(value['dq_status']),
            decoded_bytes=int(value['decoded_bytes']),
            wire_bytes=int(value['wire_bytes']),
            provider_metered_bytes=int(value['provider_metered_bytes']),
            requests=int(value['requests']),
            retries=int(value['retries']),
            cache_hits=int(value['cache_hits']),
            duration_ms=int(value['duration_ms']),
        )
        item.validate()
        return item

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def validate(self) -> None:
        allowed = {'ok', 'authoritative_empty', 'not_applicable'}
        if self.applicability_status not in allowed:
            raise ScopeManifestError(
                f'{self.entity}: invalid terminal status '
                f'{self.applicability_status!r}'
            )
        if self.dq_status != 'passed':
            raise ScopeManifestError(f'{self.entity}: DQ is not green')
        counters = (
            self.raw_rows, self.dedup_rows, self.decoded_bytes, self.wire_bytes,
            self.provider_metered_bytes, self.requests, self.retries,
            self.cache_hits, self.duration_ms,
        )
        if any(int(value) < 0 for value in counters):
            raise ScopeManifestError(f'{self.entity}: negative metric')
        if self.dedup_rows > self.raw_rows:
            raise ScopeManifestError(f'{self.entity}: dedup rows exceed raw rows')
        if self.expected_rows is None or int(self.expected_rows) != self.dedup_rows:
            raise ScopeManifestError(f'{self.entity}: expected rows differ from dedup')
        for label, value in (
            ('key', self.key_hash), ('content', self.content_hash),
        ):
            if not re.fullmatch(r'[0-9a-f]{64}', str(value or '')):
                raise ScopeManifestError(
                    f'{self.entity}: {label} hash is not lowercase sha256'
                )
        if self.applicability_status != 'ok' and (
            self.raw_rows != 0 or self.dedup_rows != 0
        ):
            raise ScopeManifestError(
                f'{self.entity}: terminal empty entity contains rows'
            )


@dataclass(frozen=True)
class ScopeManifest:
    parent_cycle_id: str
    child_cycle_id: str
    scope_id: str
    competition_id: str
    edition_id: str
    canonical_competition_id: str
    canonical_season: str
    registry_snapshot_id: str
    capture_revision: str
    parser_revision: str
    schema_revision: str
    reader_revision: int
    entities: tuple[EntityEvidence, ...]
    dq_evidence: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'ScopeManifest':
        required = {
            'parent_cycle_id', 'child_cycle_id', 'scope_id',
            'competition_id', 'edition_id', 'canonical_competition_id',
            'canonical_season', 'registry_snapshot_id', 'capture_revision',
            'parser_revision', 'schema_revision', 'reader_revision', 'entities',
            'dq_evidence',
        }
        missing = sorted(required - set(value))
        if missing:
            raise ScopeManifestError(
                f'scope manifest is missing fields: {missing}'
            )
        entities = value['entities']
        if not isinstance(entities, Sequence) or isinstance(entities, (str, bytes)):
            raise ScopeManifestError('scope manifest entities must be an array')
        dq_evidence = value['dq_evidence']
        if not isinstance(dq_evidence, Mapping):
            raise ScopeManifestError('scope manifest dq_evidence must be an object')
        return cls(
            parent_cycle_id=str(value['parent_cycle_id']),
            child_cycle_id=str(value['child_cycle_id']),
            scope_id=str(value['scope_id']),
            competition_id=str(value['competition_id']),
            edition_id=str(value['edition_id']),
            canonical_competition_id=str(value['canonical_competition_id']),
            canonical_season=str(value['canonical_season']),
            registry_snapshot_id=str(value['registry_snapshot_id']),
            capture_revision=str(value['capture_revision']),
            parser_revision=str(value['parser_revision']),
            schema_revision=str(value['schema_revision']),
            reader_revision=int(value['reader_revision']),
            entities=tuple(EntityEvidence.from_mapping(item) for item in entities),
            dq_evidence=dict(dq_evidence),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def digest(self) -> str:
        return stable_hash(asdict(self))

    def validate(self, expected_entities: Iterable[str]) -> None:
        required_strings = (
            self.parent_cycle_id, self.child_cycle_id, self.scope_id,
            self.competition_id, self.edition_id,
            self.canonical_competition_id, self.canonical_season,
            self.registry_snapshot_id, self.capture_revision,
            self.parser_revision, self.schema_revision,
        )
        if not all(str(value).strip() for value in required_strings):
            raise ScopeManifestError('scope manifest has an empty identity field')
        names = [item.entity for item in self.entities]
        if len(names) != len(set(names)):
            raise ScopeManifestError('scope manifest contains duplicate entities')
        expected = set(expected_entities)
        if set(names) != expected:
            raise ScopeManifestError(
                f'entity set mismatch: expected={sorted(expected)} got={sorted(names)}'
            )
        for item in self.entities:
            item.validate()
        self._validate_dq_evidence(expected)

    def _validate_roster_coverage(self, value: Mapping) -> None:
        """A career entity states how much of the roster this scope holds.

        One cycle buys a bounded window of a roster that runs to thousands, so
        'complete' alone says nothing about how much of the league is in the
        slot.  The count travels inside the manifest hash, where it cannot be
        edited after the fact.
        """
        coverage = value['roster_coverage']
        if not isinstance(coverage, Mapping):
            raise ScopeManifestError('roster coverage must be a mapping')
        pending = 0
        for entity, item in coverage.items():
            if not isinstance(item, Mapping):
                raise ScopeManifestError(
                    f'{entity}: roster coverage must be a mapping'
                )
            if set(item) != {'roster_size', 'selected', 'pending'}:
                raise ScopeManifestError(
                    f'{entity}: roster coverage has an unbound field set'
                )
            for name, number in item.items():
                if isinstance(number, bool) or not isinstance(number, int):
                    raise ScopeManifestError(
                        f'{entity}: roster coverage {name} must be an integer'
                    )
                if number < 0:
                    raise ScopeManifestError(
                        f'{entity}: roster coverage {name} must not be negative'
                    )
            pending += int(item['pending'])
        declared = value['career_fetches_pending']
        if isinstance(declared, bool) or not isinstance(declared, int):
            raise ScopeManifestError('career_fetches_pending must be an integer')
        if declared != pending:
            raise ScopeManifestError(
                f'career_fetches_pending states {declared}, the entities state '
                f'{pending}'
            )

    def _validate_dq_evidence(self, expected_entities: set[str]) -> None:
        """Recompute the participant contract embedded in the manifest hash."""

        value = self.dq_evidence
        required = {
            'status', 'registry_participant_count', 'edition_current',
            'scope_capture', 'entity_statuses', 'entity_contracts',
            'authoritative_empty_evidence', 'participant_contract',
        }
        # A manifest persisted before scopes stated their roster coverage is
        # still valid evidence of what it did capture; it simply said nothing
        # about what it still owed.  Nothing else may enter the hash unbound.
        coverage = {'roster_coverage', 'career_fetches_pending'}
        # A scope authorized by the standing policy carries that policy's hash
        # inside the manifest hash, where it cannot be edited after the fact:
        # the autonomous schedule replaced the one-shot approval packets, so
        # the policy is the only remaining authorization trace.  One-shot runs
        # keep their journal and simply omit the key.
        provenance = {'standing_policy_hash', 'silver_trigger_allowed'}
        optional = coverage | provenance
        if not isinstance(value, Mapping):
            raise ScopeManifestError('scope DQ evidence has an unbound field set')
        present = set(value)
        if not required <= present or present - required - optional:
            raise ScopeManifestError('scope DQ evidence has an unbound field set')
        stated = present & coverage
        if stated:
            if stated != coverage:
                raise ScopeManifestError(
                    'roster coverage must state both the entities and the total'
                )
            self._validate_roster_coverage(value)
        if 'standing_policy_hash' in present and not re.fullmatch(
            r'[0-9a-f]{64}', str(value['standing_policy_hash'] or ''),
        ):
            raise ScopeManifestError(
                'standing policy hash is not a lowercase sha256 digest'
            )
        if (
            'silver_trigger_allowed' in present
            and not isinstance(value['silver_trigger_allowed'], bool)
        ):
            raise ScopeManifestError(
                'silver_trigger_allowed must be a boolean'
            )
        if value['status'] != 'passed':
            raise ScopeManifestError('scope DQ evidence is not green')
        if not isinstance(value['edition_current'], bool):
            raise ScopeManifestError('scope DQ edition_current must be boolean')

        raw_registry_count = value['registry_participant_count']
        if raw_registry_count is None:
            registry_count = None
        elif isinstance(raw_registry_count, bool):
            raise ScopeManifestError('registry participant count is invalid')
        else:
            try:
                registry_count = int(raw_registry_count)
            except (TypeError, ValueError) as exc:
                raise ScopeManifestError(
                    'registry participant count is invalid'
                ) from exc
            if registry_count <= 0:
                raise ScopeManifestError('registry participant count is invalid')

        capture = value['scope_capture']
        capture_fields = {
            'schema_version', 'scope_id', 'competition_id', 'edition_id',
            'competition_type', 'gender', 'team_type', 'age_category',
            'listing_status', 'listing_source_url',
            'listing_source_body_hash', 'expected_team_ids',
            'observed_team_ids', 'endpoint_status_by_team', 'fetched_at',
        }
        if not isinstance(capture, Mapping) or set(capture) != capture_fields:
            raise ScopeManifestError('scope capture has an unbound field set')
        if capture['schema_version'] != 1:
            raise ScopeManifestError('scope capture schema version is unsupported')
        identity = {
            'scope_id': self.scope_id,
            'competition_id': self.competition_id,
            'edition_id': self.edition_id,
        }
        if any(str(capture[field]) != expected for field, expected in identity.items()):
            raise ScopeManifestError('scope capture identity differs from manifest')

        competition_type = str(capture['competition_type'])
        strict_types = {
            'domestic_cup', 'continental_club', 'national_team_tournament',
        }
        if competition_type not in strict_types | {'domestic_league'}:
            raise ScopeManifestError('scope capture competition type is unsupported')
        team_type = str(capture['team_type'])
        if team_type not in {'club', 'national_team'}:
            raise ScopeManifestError('scope capture team type is unsupported')
        if capture['gender'] != 'men' or capture['age_category'] != 'senior':
            raise ScopeManifestError('scope capture is not senior men')
        if capture['listing_status'] != 'ok':
            raise ScopeManifestError('scope participant listing is not authoritative')
        for field in ('listing_source_url', 'listing_source_body_hash'):
            if not isinstance(capture[field], str) or not capture[field].strip():
                raise ScopeManifestError(f'scope capture {field} is empty')
        fetched_at = capture['fetched_at']
        if not isinstance(fetched_at, str) or not fetched_at.strip():
            raise ScopeManifestError('scope capture fetched_at is empty')
        try:
            datetime.fromisoformat(fetched_at.strip().replace('Z', '+00:00'))
        except ValueError as exc:
            raise ScopeManifestError('scope capture fetched_at is invalid') from exc

        def team_ids(field: str) -> tuple[str, ...]:
            raw = capture[field]
            if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
                raise ScopeManifestError(f'scope capture {field} must be an array')
            result = tuple(str(item).strip() for item in raw)
            if not result or any(not item for item in result):
                raise ScopeManifestError(f'scope capture {field} is empty')
            if len(set(result)) != len(result):
                raise ScopeManifestError(f'scope capture {field} has duplicates')
            return result

        expected_teams = team_ids('expected_team_ids')
        observed_teams = team_ids('observed_team_ids')
        endpoints = capture['endpoint_status_by_team']
        if not isinstance(endpoints, Mapping):
            raise ScopeManifestError('scope endpoint evidence must be an object')
        endpoint_statuses = {
            str(team).strip(): str(status).strip()
            for team, status in endpoints.items()
        }
        if (
            any(not team or not status for team, status in endpoint_statuses.items())
            or set(endpoint_statuses) != set(expected_teams)
        ):
            raise ScopeManifestError('scope endpoint evidence differs from participants')
        successful_teams = {
            team for team, status in endpoint_statuses.items() if status == 'ok'
        }
        if set(observed_teams) != successful_teams:
            raise ScopeManifestError('observed participants differ from successful endpoints')
        coverage = len(set(observed_teams) & set(expected_teams)) / len(expected_teams)
        strict = competition_type in strict_types
        if strict and set(observed_teams) != set(expected_teams):
            raise ScopeManifestError('strict participant contract is incomplete')
        if not strict and coverage < 0.9:
            raise ScopeManifestError('league participant coverage is below 90%')
        if registry_count is not None and registry_count != len(expected_teams):
            raise ScopeManifestError('registry participant count drifted')

        statuses = value['entity_statuses']
        if not isinstance(statuses, Mapping) or set(statuses) != expected_entities:
            raise ScopeManifestError('scope DQ entity status set is incomplete')
        manifest_statuses = {
            item.entity: item.applicability_status for item in self.entities
        }
        if dict(statuses) != manifest_statuses:
            raise ScopeManifestError('scope DQ entity statuses differ from entities')

        from .transfermarkt_dq_contracts import entity_applicability_contracts

        expected_contracts = entity_applicability_contracts(
            entities=expected_entities,
            competition_type=competition_type,
            team_type=team_type,
        )
        contracts = value['entity_contracts']
        if not isinstance(contracts, Mapping) or dict(contracts) != expected_contracts:
            raise ScopeManifestError(
                'scope entity applicability contracts differ from classification'
            )
        entity_by_name = {item.entity: item for item in self.entities}
        for entity, contract in expected_contracts.items():
            status = manifest_statuses[entity]
            if status not in contract['allowed_statuses']:
                raise ScopeManifestError(
                    f'{entity}: terminal status violates applicability contract'
                )
            if status == 'ok' and entity_by_name[entity].dedup_rows < int(
                contract['minimum_rows']
            ):
                raise ScopeManifestError(
                    f'{entity}: applicable entity is below its row minimum'
                )

        empty_evidence = value['authoritative_empty_evidence']
        expected_empty = {
            entity for entity, status in manifest_statuses.items()
            if status == 'authoritative_empty'
        }
        if not isinstance(empty_evidence, Mapping) or set(empty_evidence) != expected_empty:
            raise ScopeManifestError(
                'authoritative-empty evidence set differs from entity statuses'
            )
        for entity, proof in empty_evidence.items():
            if (
                not isinstance(proof, Mapping)
                or set(proof) != {'kind', 'result_sha256'}
                or proof['kind'] not in {'typed_fetch_state', 'cache_complete'}
                or not re.fullmatch(r'[0-9a-f]{64}', str(proof['result_sha256']))
                or not expected_contracts[entity][
                    'requires_authoritative_empty_evidence'
                ]
            ):
                raise ScopeManifestError(
                    f'{entity}: authoritative-empty proof is invalid'
                )

        report = value['participant_contract']
        report_fields = {
            'passed', 'competition_type', 'strict', 'participant_count',
            'observed_participant_count', 'participant_coverage',
            'endpoint_coverage', 'minimum_participant_coverage', 'fresh',
        }
        if not isinstance(report, Mapping) or set(report) != report_fields:
            raise ScopeManifestError('participant DQ report has an unbound field set')
        expected_report = {
            'passed': True,
            'competition_type': competition_type,
            'strict': strict,
            'participant_count': len(expected_teams),
            'observed_participant_count': len(observed_teams),
            'participant_coverage': coverage,
            'endpoint_coverage': coverage,
            'minimum_participant_coverage': 1.0 if strict else 0.9,
            'fresh': True,
        }
        for field, expected in expected_report.items():
            actual = report[field]
            if isinstance(expected, float):
                try:
                    equal = abs(float(actual) - expected) <= 1e-12
                except (TypeError, ValueError):
                    equal = False
            else:
                equal = type(actual) is type(expected) and actual == expected
            if not equal:
                raise ScopeManifestError(
                    f'participant DQ report {field} differs from capture'
                )


@dataclass(frozen=True)
class ScopeSetManifest:
    scope_set_id: str
    registry_snapshot_id: str
    capture_revision: str
    parser_revision: str
    schema_revision: str
    reader_revision: int
    scope_digests: tuple[tuple[str, str], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            'scope_set_id': self.scope_set_id,
            'registry_snapshot_id': self.registry_snapshot_id,
            'capture_revision': self.capture_revision,
            'parser_revision': self.parser_revision,
            'schema_revision': self.schema_revision,
            'reader_revision': self.reader_revision,
            'scope_digests': [list(item) for item in self.scope_digests],
        }

    @classmethod
    def build(
        cls,
        manifests: Sequence[ScopeManifest],
        *,
        expected_entities: Iterable[str],
        reader_revision: int | None = None,
        registry_snapshot_id: str | None = None,
    ) -> 'ScopeSetManifest':
        """Freeze a set of child manifests into one content-addressed slot.

        The registry snapshot is an attribute of the SLOT, not of its members.
        A snapshot id is a hash over every page of the source registry, so any
        byte that moves on the site mints a new one — while a slot is assembled
        from bounded paid batches over months.  Requiring the members to share
        one snapshot meant the accumulated evidence was thrown away at every
        discovery run and the target could never be covered.  What binds a
        member to the slot is what it captured (capture/parser/schema revision)
        and the caller having already checked that the promoted registry still
        targets that scope with the same meaning.
        """

        if not manifests:
            raise ScopeManifestError('scope set cannot be empty')
        if len(manifests) > MAX_SCOPE_SET_SIZE:
            raise ScopeManifestError(
                f'scope set cannot exceed {MAX_SCOPE_SET_SIZE} scopes'
            )
        expected_entities = tuple(expected_entities)
        for item in manifests:
            item.validate(expected_entities)
        identity = {
            (
                item.capture_revision, item.parser_revision,
                item.schema_revision,
            )
            for item in manifests
        }
        if len(identity) != 1:
            raise ScopeManifestError(
                'scope manifests do not share capture/parser/schema revisions'
            )
        if registry_snapshot_id is None:
            snapshots = {item.registry_snapshot_id for item in manifests}
            if len(snapshots) != 1:
                raise ScopeManifestError(
                    'a scope set spanning registry snapshots must state which '
                    'promoted snapshot it is bound to'
                )
            registry = next(iter(snapshots))
        else:
            registry = str(registry_snapshot_id).strip()
            if not registry:
                raise ScopeManifestError('scope-set registry snapshot is empty')
        child_revisions = tuple(int(item.reader_revision) for item in manifests)
        if any(value < 0 for value in child_revisions):
            raise ScopeManifestError('child reader revision must be non-negative')
        if reader_revision is None:
            reader = max(child_revisions)
        else:
            if isinstance(reader_revision, bool):
                raise ScopeManifestError(
                    'scope-set reader revision must be a non-negative integer'
                )
            try:
                reader = int(reader_revision)
            except (TypeError, ValueError) as exc:
                raise ScopeManifestError(
                    'scope-set reader revision must be a non-negative integer'
                ) from exc
            if reader < 0:
                raise ScopeManifestError(
                    'scope-set reader revision must be a non-negative integer'
                )
        if any(value > reader for value in child_revisions):
            raise ScopeManifestError(
                'child reader revision exceeds scope-set reader revision'
            )
        scope_digests = tuple(sorted(
            ((item.scope_id, item.digest) for item in manifests),
            key=lambda item: item[0],
        ))
        if len({scope for scope, _ in scope_digests}) != len(scope_digests):
            raise ScopeManifestError('scope set contains duplicate scope ids')
        capture, parser, schema = next(iter(identity))
        payload = {
            'registry_snapshot_id': registry,
            'capture_revision': capture,
            'parser_revision': parser,
            'schema_revision': schema,
            'reader_revision': reader,
            'scope_digests': scope_digests,
        }
        return cls(
            scope_set_id=stable_hash(payload),
            registry_snapshot_id=registry,
            capture_revision=capture,
            parser_revision=parser,
            schema_revision=schema,
            reader_revision=reader,
            scope_digests=scope_digests,
        )


def aggregate_traffic(manifests: Sequence[ScopeManifest]) -> dict[str, int]:
    fields = (
        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes', 'requests',
        'retries', 'cache_hits', 'duration_ms',
    )
    return {
        field: sum(
            int(getattr(entity, field))
            for manifest in manifests
            for entity in manifest.entities
        )
        for field in fields
    }


def ddl_statements() -> list[str]:
    """Additive Trino DDL; execution is a separately approved production write."""

    return [
        f"""CREATE TABLE IF NOT EXISTS {REGISTRY_STATE_TABLE} (
            state_key varchar, registry_snapshot_id varchar, source_hash varchar,
            competition_count bigint, edition_count bigint,
            unknown_active_count bigint, status varchar, revision bigint,
            promoted_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {SCOPE_MANIFEST_TABLE} (
            parent_cycle_id varchar, child_cycle_id varchar, scope_id varchar,
            competition_id varchar, edition_id varchar,
            canonical_competition_id varchar, canonical_season varchar,
            registry_snapshot_id varchar, capture_revision varchar,
            parser_revision varchar, schema_revision varchar,
            reader_revision bigint, entity_manifest_json varchar,
            manifest_digest varchar, status varchar, committed_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {SCOPE_SET_MANIFEST_TABLE} (
            scope_set_id varchar, registry_snapshot_id varchar,
            capture_revision varchar, parser_revision varchar,
            schema_revision varchar, reader_revision bigint,
            scope_digests_json varchar, traffic_json varchar,
            status varchar, committed_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {PROXY_LEDGER_TABLE} (
            parent_cycle_id varchar, entity varchar,
            decoded_bytes bigint, wire_bytes bigint,
            provider_metered_bytes bigint, requests bigint, retries bigint,
            cache_hits bigint, duration_ms bigint, hard_limit_bytes bigint,
            soft_limit_bytes bigint, updated_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
    ]


def scope_set_cas_predicate(
    *, expected_reader_revision: int, expected_scope_set_id: str,
) -> str:
    """Exact predicate shared by cutover SQL and its unit tests."""

    if int(expected_reader_revision) < 0:
        raise ScopeManifestError('reader revision must be non-negative')
    if len(str(expected_scope_set_id)) != 64:
        raise ScopeManifestError('scope_set_id must be a sha256 digest')
    return (
        f'revision = {int(expected_reader_revision)} AND '
        f"approved_scope_set_id = '{expected_scope_set_id}'"
    )
