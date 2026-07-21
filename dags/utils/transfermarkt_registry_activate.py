"""Activate one immutable Transfermarkt Bronze registry snapshot.

The source registry is already typed in Bronze.  Activation therefore only
validates the exact raw-backed snapshot and advances an ops pointer with CAS;
it never builds a Silver staging copy.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
import hashlib
import json
import re
from typing import Any

from utils.transfermarkt_source import (
    COMPETITIONS_TABLE,
    EDITIONS_TABLE,
    PARTICIPANTS_TABLE,
    RAW_RESPONSES_TABLE,
    REGISTRY_STATE_TABLE,
)


_HEX_64 = re.compile(r'^[0-9a-f]{64}$')
_SNAPSHOT_ID = re.compile(r'^tm-discovery-[0-9a-f]{24}$')


class RegistryActivationError(RuntimeError):
    """The discovered snapshot cannot become the active crawl registry."""


class RegistryActivationConflict(RegistryActivationError):
    """The registry pointer changed while activation was in progress."""


def stable_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        default=str,
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def _literal(value: Any) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class ActiveRegistry:
    snapshot_id: str
    source_hash: str
    competition_count: int
    edition_count: int
    participant_count: int
    revision: int
    status: str = 'active'


@dataclass(frozen=True)
class RegistryActivationPlan:
    state: ActiveRegistry
    expected_revision: int
    statements: tuple[str, ...]
    plan_hash: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _validate_manifest(
    manifest: Mapping[str, Any],
    *,
    manifest_hash: str,
) -> ActiveRegistry:
    if stable_hash(manifest) != manifest_hash or not _HEX_64.fullmatch(manifest_hash):
        raise RegistryActivationError('registry manifest hash mismatch')
    snapshot_id = str(manifest.get('snapshot_id') or '')
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise RegistryActivationError('registry snapshot id is invalid')
    if manifest.get('status') != 'success' or manifest.get('promotable') is not True:
        raise RegistryActivationError('registry capture is not promotable')
    blocked = manifest.get('blocked_competition_ids')
    if not isinstance(blocked, Sequence) or isinstance(blocked, (str, bytes)):
        raise RegistryActivationError('blocked competition ids must be an array')
    if blocked:
        raise RegistryActivationError('unknown/conflicting competitions block activation')
    rows = manifest.get('rows')
    if not isinstance(rows, Mapping):
        raise RegistryActivationError('registry row evidence is missing')
    try:
        competition_count = int(rows['competitions'])
        edition_count = int(rows['competition_editions'])
        participant_count = int(rows['competition_participants'])
    except (KeyError, TypeError, ValueError) as exc:
        raise RegistryActivationError('registry row evidence is incomplete') from exc
    if min(competition_count, edition_count, participant_count) <= 0:
        raise RegistryActivationError('registry row counts must be positive')
    raw = manifest.get('raw_response_set')
    if not isinstance(raw, Mapping):
        raise RegistryActivationError('raw response-set evidence is missing')
    raw_set_id = str(raw.get('raw_payload_set_id') or '')
    if not _HEX_64.fullmatch(raw_set_id) or int(raw.get('response_count', 0)) <= 0:
        raise RegistryActivationError('raw response-set evidence is invalid')
    return ActiveRegistry(
        snapshot_id=snapshot_id,
        source_hash=manifest_hash,
        competition_count=competition_count,
        edition_count=edition_count,
        participant_count=participant_count,
        revision=0,
    )


def _bootstrap_sql() -> str:
    return f"""CREATE TABLE IF NOT EXISTS {REGISTRY_STATE_TABLE} (
    state_key varchar,
    registry_snapshot_id varchar,
    source_hash varchar,
    competition_count bigint,
    edition_count bigint,
    participant_count bigint,
    status varchar,
    revision bigint,
    activated_at timestamp(6)
) WITH (format = 'PARQUET')"""


def _dq_sql(state: ActiveRegistry) -> str:
    snapshot = _literal(state.snapshot_id)
    return f"""WITH competitions AS (
    SELECT * FROM {COMPETITIONS_TABLE}
    WHERE registry_snapshot_id = {snapshot}
), editions AS (
    SELECT * FROM {EDITIONS_TABLE}
    WHERE registry_snapshot_id = {snapshot}
), participants AS (
    SELECT * FROM {PARTICIPANTS_TABLE}
    WHERE registry_snapshot_id = {snapshot}
), raw_hashes AS (
    SELECT DISTINCT content_hash FROM {RAW_RESPONSES_TABLE}
    WHERE outcome IN ('ok', 'authoritative_empty')
)
SELECT
    (SELECT count(*) FROM competitions) AS competition_count,
    (SELECT count(DISTINCT competition_id) FROM competitions)
        AS competition_distinct_count,
    (SELECT count(*) FROM editions) AS edition_count,
    (SELECT count(DISTINCT ROW(competition_id, edition_id)) FROM editions)
        AS edition_distinct_count,
    (SELECT count(*) FROM participants) AS participant_count,
    (SELECT count(DISTINCT ROW(competition_id, edition_id, team_id))
     FROM participants) AS participant_distinct_count,
    (SELECT count(*) FROM competitions
     WHERE active AND classification_status IN ('unknown', 'conflict'))
        AS blocked_competition_count,
    (SELECT count(*) FROM editions e
     WHERE NOT EXISTS (
         SELECT 1 FROM competitions c
         WHERE c.competition_id = e.competition_id
     )) AS orphan_edition_count,
    (SELECT count(*) FROM participants p
     WHERE NOT EXISTS (
         SELECT 1 FROM editions e
         WHERE e.competition_id = p.competition_id
           AND e.edition_id = p.edition_id
     )) AS orphan_participant_count,
    (SELECT count(*) FROM competitions c
     WHERE NOT EXISTS (
         SELECT 1 FROM raw_hashes r WHERE r.content_hash = c.source_body_hash
     )) +
    (SELECT count(*) FROM editions e
     WHERE NOT EXISTS (
         SELECT 1 FROM raw_hashes r WHERE r.content_hash = e.source_body_hash
     )) +
    (SELECT count(*) FROM participants p
     WHERE NOT EXISTS (
         SELECT 1 FROM raw_hashes r WHERE r.content_hash = p.source_body_hash
     )) AS raw_lineage_violation_count"""


def _cas_sql(state: ActiveRegistry, *, expected_revision: int) -> str:
    promoted = expected_revision + 1
    return f"""MERGE INTO {REGISTRY_STATE_TABLE} target
USING (VALUES (
    'canonical', {_literal(state.snapshot_id)}, {_literal(state.source_hash)},
    BIGINT '{state.competition_count}', BIGINT '{state.edition_count}',
    BIGINT '{state.participant_count}', 'active',
    BIGINT '{expected_revision}', BIGINT '{promoted}'
)) source (
    state_key, registry_snapshot_id, source_hash, competition_count,
    edition_count, participant_count, status, expected_revision, new_revision
)
ON target.state_key = source.state_key
WHEN MATCHED AND target.revision = source.expected_revision THEN UPDATE SET
    registry_snapshot_id = source.registry_snapshot_id,
    source_hash = source.source_hash,
    competition_count = source.competition_count,
    edition_count = source.edition_count,
    participant_count = source.participant_count,
    status = source.status,
    revision = source.new_revision,
    activated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED AND source.expected_revision = 0 THEN INSERT (
    state_key, registry_snapshot_id, source_hash, competition_count,
    edition_count, participant_count, status, revision, activated_at
) VALUES (
    source.state_key, source.registry_snapshot_id, source.source_hash,
    source.competition_count, source.edition_count, source.participant_count,
    source.status, source.new_revision, CURRENT_TIMESTAMP
)"""


def _readback_sql() -> str:
    return f"""SELECT registry_snapshot_id, source_hash, competition_count,
       edition_count, participant_count, revision, status
FROM {REGISTRY_STATE_TABLE}
WHERE state_key = 'canonical'"""


def build_activation_plan(
    manifest: Mapping[str, Any],
    *,
    manifest_hash: str,
    expected_revision: int,
) -> RegistryActivationPlan:
    if isinstance(expected_revision, bool) or expected_revision < 0:
        raise RegistryActivationError('expected revision must be non-negative')
    state = _validate_manifest(manifest, manifest_hash=manifest_hash)
    state = ActiveRegistry(**{
        **asdict(state),
        'revision': expected_revision + 1,
    })
    statements = (
        _bootstrap_sql(),
        _dq_sql(state),
        _cas_sql(state, expected_revision=expected_revision),
        _readback_sql(),
    )
    identity = {
        'state': asdict(state),
        'expected_revision': expected_revision,
        'statement_hashes': [
            hashlib.sha256(sql.encode('utf-8')).hexdigest() for sql in statements
        ],
    }
    return RegistryActivationPlan(
        state=state,
        expected_revision=expected_revision,
        statements=statements,
        plan_hash=stable_hash(identity),
    )


def _column_name(value: Any) -> str:
    name = getattr(value, 'name', None)
    if name is not None:
        return str(name)
    if isinstance(value, Sequence) and value:
        return str(value[0])
    raise RegistryActivationError('invalid DB-API column metadata')


def _fetch_mappings(cursor: Any) -> list[dict[str, Any]]:
    rows = list(cursor.fetchall())
    columns = [_column_name(item) for item in (cursor.description or ())]
    if any(len(row) != len(columns) for row in rows):
        raise RegistryActivationError('invalid DB-API row shape')
    return [dict(zip(columns, row, strict=True)) for row in rows]


def apply_activation(plan: RegistryActivationPlan, connection: Any) -> ActiveRegistry:
    """Execute a validated plan and verify DQ plus CAS readback."""

    cursor = connection.cursor()
    try:
        cursor.execute(plan.statements[0])
        cursor.execute(plan.statements[1])
        dq_rows = _fetch_mappings(cursor)
        if len(dq_rows) != 1:
            raise RegistryActivationError('registry DQ returned no exact row')
        dq = dq_rows[0]
        expected = {
            'competition_count': plan.state.competition_count,
            'competition_distinct_count': plan.state.competition_count,
            'edition_count': plan.state.edition_count,
            'edition_distinct_count': plan.state.edition_count,
            'participant_count': plan.state.participant_count,
            'participant_distinct_count': plan.state.participant_count,
            'blocked_competition_count': 0,
            'orphan_edition_count': 0,
            'orphan_participant_count': 0,
            'raw_lineage_violation_count': 0,
        }
        if {key: int(dq.get(key, -1)) for key in expected} != expected:
            raise RegistryActivationError(f'registry DQ failed: {dq}')
        cursor.execute(plan.statements[2])
        cursor.execute(plan.statements[3])
        rows = _fetch_mappings(cursor)
        if len(rows) != 1:
            raise RegistryActivationConflict('registry CAS readback is ambiguous')
        row = rows[0]
        actual = ActiveRegistry(
            snapshot_id=str(row['registry_snapshot_id']),
            source_hash=str(row['source_hash']),
            competition_count=int(row['competition_count']),
            edition_count=int(row['edition_count']),
            participant_count=int(row['participant_count']),
            revision=int(row['revision']),
            status=str(row['status']),
        )
        if actual != plan.state:
            raise RegistryActivationConflict('registry CAS readback differs from plan')
        connection.commit()
        return actual
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
