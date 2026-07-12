"""Fail-closed publication of a discovered Transfermarkt registry snapshot.

Discovery writes immutable Bronze snapshots.  This module filters the two
existing Silver transforms to one exact snapshot, validates their complete
registry contract and only then advances the canonical registry pointer with
compare-and-swap semantics.  Older Bronze and Silver snapshots are retained,
so rollback is a metadata pointer change rather than a source re-fetch.

``publish_registry(..., apply=False)`` is side-effect free and returns every
SQL statement, including rollback SQL.  Production execution requires either
an injected SQL executor or a DB-API connection; the module opens no network
connection itself.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any


REGISTRY_STATE_TABLE = 'iceberg.ops.transfermarkt_registry_state_v2'
COMPETITIONS_TABLE = 'iceberg.silver.transfermarkt_competitions_v2'
EDITIONS_TABLE = 'iceberg.silver.transfermarkt_competition_editions_v2'

_SQL_ROOT = Path(__file__).resolve().parents[1] / 'sql' / 'silver'
_TRANSFORM_FILES = {
    'competitions': 'transfermarkt_competitions_v2.sql',
    'competition_editions': 'transfermarkt_competition_editions_v2.sql',
}
_HEX_64 = re.compile(r'^[a-f0-9]{64}$')
_SNAPSHOT_ID = re.compile(r'^tm-discovery-[a-f0-9]{24}$')

_COMPETITION_COLUMNS = (
    'competition_id', 'slug', 'name', 'country', 'confederation',
    'competition_type', 'gender', 'team_type', 'age_category',
    'season_format', 'active', 'source_url', 'discovered_at',
    'canonical_competition_id', 'classification_status',
    'classification_evidence', 'registry_snapshot_id', 'source_body_hash',
    'parser_revision', 'schema_revision', 'fetched_at', 'cycle_id',
    'scope_id', '_bronze_ingested_at', '_batch_id',
)
_EDITION_COLUMNS = (
    'competition_id', 'edition_id', 'edition_label', 'canonical_season',
    'season_format', 'start_date', 'end_date', 'active', 'is_current',
    'participant_count', 'participant_hash', 'source_url', 'discovered_at',
    'registry_snapshot_id', 'source_body_hash', 'parser_revision',
    'schema_revision', 'fetched_at', 'cycle_id', 'scope_id',
    '_bronze_ingested_at', '_batch_id',
)
_STATE_COLUMNS = (
    'state_key', 'registry_snapshot_id', 'source_hash', 'competition_count',
    'edition_count', 'unknown_active_count', 'status', 'revision',
)
# A competition's format is the one its current edition runs on; older editions
# keep theirs, so the two legitimately differ and only each edition's own
# format-vs-season agreement is checked (canonical_season_violations).
_DQ_FIELDS = (
    'competition_count', 'competition_distinct_count', 'edition_count',
    'edition_distinct_count', 'orphan_editions',
    'competitions_without_editions', 'current_edition_violations',
    'canonical_season_violations',
    'classification_evidence_violations', 'classification_field_violations',
    'unknown_active_count', 'content_mismatch_count',
)


class RegistryPublicationError(RuntimeError):
    """Base class for publication failures."""


class RegistryManifestError(RegistryPublicationError, ValueError):
    """Discovery evidence is incomplete, inconsistent, or not promotable."""


class RegistryDQError(RegistryPublicationError):
    """The exact Silver snapshot violates the registry data contract."""


class RegistryCasError(RegistryPublicationError):
    """The canonical registry revision changed or CAS readback did not match."""


def stable_hash(value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        default=str,
    ).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _required_text(value: Any, *, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise RegistryManifestError(f'{field} is required')
    if text != str(value):
        raise RegistryManifestError(f'{field} must not have outer whitespace')
    if any(character in text for character in ('\x00', '\n', '\r')):
        raise RegistryManifestError(f'{field} contains a control character')
    return text


def _integer(value: Any, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RegistryManifestError(f'{field} must be an integer')
    result = value
    if result < minimum:
        raise RegistryManifestError(
            f'{field} must be an integer >= {minimum}'
        )
    return result


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class RegistryState:
    state_key: str
    registry_snapshot_id: str
    source_hash: str
    competition_count: int
    edition_count: int
    unknown_active_count: int
    status: str
    revision: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'RegistryState':
        missing = [field for field in _STATE_COLUMNS if field not in value]
        if missing:
            raise RegistryCasError(f'registry state is missing fields: {missing}')
        return cls(
            state_key=str(value['state_key']),
            registry_snapshot_id=str(value['registry_snapshot_id'] or ''),
            source_hash=str(value['source_hash'] or ''),
            competition_count=int(value['competition_count'] or 0),
            edition_count=int(value['edition_count'] or 0),
            unknown_active_count=int(value['unknown_active_count'] or 0),
            status=str(value['status'] or ''),
            revision=int(value['revision']),
        )


@dataclass(frozen=True)
class RegistryPublicationPlan:
    snapshot_id: str
    snapshot_hash: str
    discovery_manifest_hash: str
    registry_manifest_hash: str
    competition_count: int
    edition_count: int
    expected_revision: int
    promoted_revision: int
    staging_tables: tuple[tuple[str, str], ...]
    transform_sql: tuple[tuple[str, str], ...]
    statements: tuple[str, ...]
    rollback_statements: tuple[str, ...]

    @property
    def manifest_hash(self) -> str:
        """Alias for the publication manifest bound to SQL and revision."""

        return self.registry_manifest_hash

    @property
    def sql_statements(self) -> tuple[str, ...]:
        return self.statements

    @property
    def rollback_sql(self) -> tuple[str, ...]:
        return self.rollback_statements

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RegistryPublicationResult:
    plan: RegistryPublicationPlan
    applied: bool
    previous_state: RegistryState | None = None
    promoted_state: RegistryState | None = None
    dq: tuple[tuple[str, int], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            'plan': self.plan.as_dict(),
            'applied': self.applied,
            'previous_state': (
                asdict(self.previous_state) if self.previous_state else None
            ),
            'promoted_state': (
                asdict(self.promoted_state) if self.promoted_state else None
            ),
            'dq': dict(self.dq),
        }


def _validate_manifest(
    manifest: Mapping[str, Any],
    *,
    manifest_hash: str,
    snapshot_id: str,
    competition_count: int,
    edition_count: int,
) -> tuple[str, str, int, int]:
    if not isinstance(manifest, Mapping):
        raise RegistryManifestError('discovery_manifest must be an object')
    expected_hash = _required_text(manifest_hash, field='manifest_hash')
    if not _HEX_64.fullmatch(expected_hash):
        raise RegistryManifestError('manifest_hash must be a sha256 digest')
    actual_hash = stable_hash(manifest)
    if actual_hash != expected_hash:
        raise RegistryManifestError('discovery manifest hash mismatch')

    expected_snapshot = _required_text(snapshot_id, field='snapshot_id')
    if not _SNAPSHOT_ID.fullmatch(expected_snapshot):
        raise RegistryManifestError('snapshot_id has an unsafe format')
    if manifest.get('snapshot_id') != expected_snapshot:
        raise RegistryManifestError('snapshot_id disagrees with discovery manifest')
    snapshot_hash = str(manifest.get('snapshot_hash') or '')
    if not _HEX_64.fullmatch(snapshot_hash):
        raise RegistryManifestError('snapshot_hash must be a sha256 digest')

    competitions = _integer(
        competition_count, field='competition_count', minimum=1,
    )
    editions = _integer(edition_count, field='edition_count', minimum=1)
    rows = manifest.get('rows')
    if not isinstance(rows, Mapping) or {
        'competitions': rows.get('competitions'),
        'competition_editions': rows.get('competition_editions'),
    } != {
        'competitions': competitions,
        'competition_editions': editions,
    }:
        raise RegistryManifestError('exact discovery row counts do not match')

    if manifest.get('status') != 'success' or bool(manifest.get('dry_run')):
        raise RegistryManifestError('only a successful written discovery can publish')
    if manifest.get('promotable') is not True:
        raise RegistryManifestError('discovery snapshot is not promotable')
    blocked = manifest.get('blocked_competition_ids')
    if not isinstance(blocked, Sequence) or isinstance(blocked, (str, bytes)):
        raise RegistryManifestError('blocked_competition_ids must be an array')
    if blocked:
        raise RegistryManifestError('unknown/conflicting competitions block promotion')

    classifications = manifest.get('classification_counts')
    if not isinstance(classifications, Mapping):
        raise RegistryManifestError('classification_counts must be an object')
    allowed_statuses = {'eligible', 'excluded', 'unknown', 'conflict'}
    if set(classifications) - allowed_statuses:
        raise RegistryManifestError('classification_counts has an unknown status')
    parsed_classifications = {
        key: _integer(value, field=f'classification_counts.{key}')
        for key, value in classifications.items()
    }
    if sum(parsed_classifications.values()) != competitions:
        raise RegistryManifestError('classification counts are partial')
    if parsed_classifications.get('unknown', 0) or parsed_classifications.get(
        'conflict', 0,
    ):
        raise RegistryManifestError('unknown/conflicting classification blocks promotion')

    hashes = manifest.get('hashes')
    if not isinstance(hashes, Mapping):
        raise RegistryManifestError('discovery entity hashes are missing')
    for entity in ('competitions', 'competition_editions', 'crawl_scopes'):
        if not _HEX_64.fullmatch(str(hashes.get(entity) or '')):
            raise RegistryManifestError(f'{entity} hash is missing or invalid')
    scopes = manifest.get('crawl_scopes')
    if not isinstance(scopes, Sequence) or isinstance(scopes, (str, bytes)):
        raise RegistryManifestError('crawl_scopes must be an array')
    if _integer(
        manifest.get('crawl_scope_count'), field='crawl_scope_count',
    ) != len(scopes):
        raise RegistryManifestError('crawl scope count is partial')
    if stable_hash(scopes) != hashes['crawl_scopes']:
        raise RegistryManifestError('crawl scope hash mismatch')

    page_count = _integer(manifest.get('page_count'), field='page_count', minimum=1)
    source_hashes = manifest.get('source_body_hashes')
    if (
        not isinstance(source_hashes, Sequence)
        or isinstance(source_hashes, (str, bytes))
        or len(source_hashes) < page_count
        or any(not _HEX_64.fullmatch(str(value)) for value in source_hashes)
    ):
        raise RegistryManifestError('source page evidence is partial')

    writes = manifest.get('writes')
    if not isinstance(writes, Sequence) or isinstance(writes, (str, bytes)):
        raise RegistryManifestError('Bronze write manifest is missing')
    expected_writes = {
        'iceberg.bronze.transfermarkt_competitions': competitions,
        'iceberg.bronze.transfermarkt_competition_editions': editions,
    }
    actual_writes: dict[str, int] = {}
    for item in writes:
        if not isinstance(item, Mapping):
            raise RegistryManifestError('Bronze write manifest row is invalid')
        table = str(item.get('table') or '')
        if table in actual_writes:
            raise RegistryManifestError(f'duplicate Bronze write evidence: {table}')
        actual_writes[table] = _integer(
            item.get('rows'), field=f'writes.{table}.rows',
        )
    if actual_writes != expected_writes:
        raise RegistryManifestError('Bronze writes are incomplete or out of scope')
    return expected_snapshot, snapshot_hash, competitions, editions


def _load_transform_sql(entity: str, *, sql_root: Path) -> str:
    try:
        path = sql_root / _TRANSFORM_FILES[entity]
    except KeyError as exc:
        raise RegistryPublicationError(f'unknown registry transform: {entity}') from exc
    try:
        value = path.read_text(encoding='utf-8').strip().rstrip(';')
    except OSError as exc:
        raise RegistryPublicationError(f'registry transform is unreadable: {path}') from exc
    marker = '\n)\nWHERE rn = 1'
    if value.count(marker) != 1:
        raise RegistryPublicationError(
            f'{path.name}: expected dedup transform shape was not found'
        )
    return value


def _snapshot_transform(sql: str, snapshot_id: str) -> str:
    """Put the snapshot predicate inside ROW_NUMBER, before key deduplication."""

    marker = '\n)\nWHERE rn = 1'
    head, tail = sql.split(marker, 1)
    return (
        head
        + '\n      AND b.registry_snapshot_id = '
        + _sql_literal(snapshot_id)
        + marker
        + tail
    )


def _merge_snapshot_sql(
    *, target: str, staging: str, columns: Sequence[str], keys: Sequence[str], tag: str,
) -> str:
    column_sql = ', '.join(columns)
    values_sql = ', '.join(f'source.{column}' for column in columns)
    predicate = ' AND '.join(
        [
            'target.registry_snapshot_id = source.registry_snapshot_id',
            *(f'target.{key} = source.{key}' for key in keys),
        ]
    )
    return f"""/* tm_registry_merge:{tag} */
MERGE INTO {target} AS target
USING {staging} AS source
ON {predicate}
WHEN NOT MATCHED THEN
    INSERT ({column_sql}) VALUES ({values_sql})"""


def _dq_sql(
    *,
    competitions_table: str,
    editions_table: str,
    snapshot_id: str,
    tag: str,
    compare_competitions: str | None = None,
    compare_editions: str | None = None,
) -> str:
    snapshot = _sql_literal(snapshot_id)
    mismatch = 'CAST(0 AS bigint)'
    if compare_competitions and compare_editions:
        c_columns = ', '.join(_COMPETITION_COLUMNS)
        e_columns = ', '.join(_EDITION_COLUMNS)
        mismatch = f"""(
            SELECT COUNT(*) FROM (
                (SELECT {c_columns} FROM c
                 EXCEPT SELECT {c_columns} FROM {compare_competitions})
                UNION ALL
                (SELECT {c_columns} FROM {compare_competitions}
                 EXCEPT SELECT {c_columns} FROM c)
                UNION ALL
                (SELECT {e_columns} FROM e
                 EXCEPT SELECT {e_columns} FROM {compare_editions})
                UNION ALL
                (SELECT {e_columns} FROM {compare_editions}
                 EXCEPT SELECT {e_columns} FROM e)
            ) differences
        )"""
    return f"""/* tm_registry_dq:{tag} */
WITH c AS (
    SELECT * FROM {competitions_table}
    WHERE registry_snapshot_id = {snapshot}
), e AS (
    SELECT * FROM {editions_table}
    WHERE registry_snapshot_id = {snapshot}
), current_counts AS (
    SELECT competition_id, count_if(is_current) AS current_count
    FROM e GROUP BY competition_id
)
SELECT
    (SELECT COUNT(*) FROM c) AS competition_count,
    (SELECT COUNT(DISTINCT competition_id) FROM c)
        AS competition_distinct_count,
    (SELECT COUNT(*) FROM e) AS edition_count,
    (SELECT COUNT(DISTINCT ROW(competition_id, edition_id)) FROM e)
        AS edition_distinct_count,
    (SELECT COUNT(*) FROM e
     WHERE competition_id NOT IN (SELECT competition_id FROM c))
        AS orphan_editions,
    (SELECT COUNT(*) FROM c
     WHERE competition_id NOT IN (SELECT competition_id FROM e))
        AS competitions_without_editions,
    (SELECT COUNT(*) FROM c
     LEFT JOIN current_counts cc USING (competition_id)
     WHERE COALESCE(cc.current_count, 0) <> 1)
        AS current_edition_violations,
    (SELECT count_if(
        TRY_CAST(edition_id AS integer) IS NULL
        OR NOT regexp_like(edition_id, '^(18|19|20|21)[0-9]{{2}}$')
        OR NOT regexp_like(canonical_season, '^[0-9]{{4}}$')
        OR season_format NOT IN ('split_year', 'single_year')
        OR (season_format = 'single_year' AND canonical_season <> edition_id)
        OR (season_format = 'split_year' AND canonical_season <> concat(
            substr(edition_id, 3, 2),
            lpad(CAST(mod(TRY_CAST(edition_id AS integer) + 1, 100) AS varchar), 2, '0')
        ))
        OR (is_current AND NOT active)
        OR (start_date IS NOT NULL AND end_date IS NOT NULL AND end_date < start_date)
    ) FROM e) AS canonical_season_violations,
    (SELECT count_if(
        classification_evidence IS NULL
        OR trim(classification_evidence) IN ('', '[]', 'null')
        OR TRY(json_parse(classification_evidence)) IS NULL
    ) FROM c) AS classification_evidence_violations,
    (SELECT count_if(
        classification_status NOT IN ('eligible', 'excluded', 'unknown', 'conflict')
        OR season_format NOT IN ('split_year', 'single_year')
        OR (classification_status = 'eligible' AND (
            gender <> 'men' OR team_type NOT IN ('club', 'national_team')
            OR age_category <> 'senior'
            OR competition_type NOT IN (
                'domestic_league', 'domestic_cup', 'continental_club',
                'national_team_tournament'
            )
        ))
    ) FROM c) AS classification_field_violations,
    (SELECT count_if(
        active AND classification_status IN ('unknown', 'conflict')
    ) FROM c) AS unknown_active_count,
    {mismatch} AS content_mismatch_count"""


def _state_read_sql(tag: str) -> str:
    return f"""/* tm_registry_state:{tag} */
SELECT {', '.join(_STATE_COLUMNS)}
FROM {REGISTRY_STATE_TABLE}
WHERE state_key = 'canonical'"""


def _history_sql(expected_revision: int) -> str:
    history_key = f'history:{expected_revision}'
    return f"""/* tm_registry_state:history */
MERGE INTO {REGISTRY_STATE_TABLE} AS target
USING (
    SELECT {_sql_literal(history_key)} AS state_key,
           registry_snapshot_id, source_hash, competition_count,
           edition_count, unknown_active_count, status, revision
    FROM {REGISTRY_STATE_TABLE}
    WHERE state_key = 'canonical' AND revision = {expected_revision}
) AS source
ON target.state_key = source.state_key
WHEN NOT MATCHED THEN INSERT (
    state_key, registry_snapshot_id, source_hash, competition_count,
    edition_count, unknown_active_count, status, revision, promoted_at
) VALUES (
    source.state_key, source.registry_snapshot_id, source.source_hash,
    source.competition_count, source.edition_count,
    source.unknown_active_count, source.status, source.revision, CURRENT_TIMESTAMP
)"""


def _cas_sql(
    *, snapshot_id: str, source_hash: str, competition_count: int,
    edition_count: int, expected_revision: int,
) -> str:
    promoted_revision = expected_revision + 1
    return f"""/* tm_registry_state:cas */
MERGE INTO {REGISTRY_STATE_TABLE} AS target
USING (VALUES (
    'canonical', {_sql_literal(snapshot_id)}, {_sql_literal(source_hash)},
    BIGINT '{competition_count}', BIGINT '{edition_count}', BIGINT '0',
    'promoted', BIGINT '{expected_revision}', BIGINT '{promoted_revision}'
)) AS source (
    state_key, registry_snapshot_id, source_hash, competition_count,
    edition_count, unknown_active_count, status,
    expected_revision, promoted_revision
)
ON target.state_key = source.state_key
WHEN MATCHED AND target.revision = source.expected_revision THEN UPDATE SET
    registry_snapshot_id = source.registry_snapshot_id,
    source_hash = source.source_hash,
    competition_count = source.competition_count,
    edition_count = source.edition_count,
    unknown_active_count = source.unknown_active_count,
    status = source.status,
    revision = source.promoted_revision,
    promoted_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED AND source.expected_revision = 0 THEN INSERT (
    state_key, registry_snapshot_id, source_hash, competition_count,
    edition_count, unknown_active_count, status, revision, promoted_at
) VALUES (
    source.state_key, source.registry_snapshot_id, source.source_hash,
    source.competition_count, source.edition_count,
    source.unknown_active_count, source.status,
    source.promoted_revision, CURRENT_TIMESTAMP
)"""


def _rollback_sql(*, expected_revision: int, promoted_revision: int) -> str:
    history_key = f'history:{expected_revision}'
    rollback_revision = promoted_revision + 1
    return f"""/* tm_registry_state:rollback */
MERGE INTO {REGISTRY_STATE_TABLE} AS target
USING (
    SELECT registry_snapshot_id, source_hash, competition_count,
           edition_count, unknown_active_count, status
    FROM {REGISTRY_STATE_TABLE}
    WHERE state_key = {_sql_literal(history_key)}
    UNION ALL
    SELECT CAST('' AS varchar), CAST('' AS varchar), BIGINT '0', BIGINT '0',
           BIGINT '0', CAST('uninitialized' AS varchar)
    WHERE {expected_revision} = 0
      AND NOT EXISTS (
          SELECT 1 FROM {REGISTRY_STATE_TABLE}
          WHERE state_key = {_sql_literal(history_key)}
      )
) AS previous
ON target.state_key = 'canonical'
WHEN MATCHED AND target.revision = {promoted_revision} THEN UPDATE SET
    registry_snapshot_id = previous.registry_snapshot_id,
    source_hash = previous.source_hash,
    competition_count = previous.competition_count,
    edition_count = previous.edition_count,
    unknown_active_count = previous.unknown_active_count,
    status = previous.status,
    revision = {rollback_revision},
    promoted_at = CURRENT_TIMESTAMP"""


def _build_plan(
    manifest: Mapping[str, Any],
    *,
    manifest_hash: str,
    snapshot_id: str,
    competition_count: int,
    edition_count: int,
    expected_revision: int,
    sql_root: Path,
) -> RegistryPublicationPlan:
    snapshot, snapshot_hash, competitions, editions = _validate_manifest(
        manifest,
        manifest_hash=manifest_hash,
        snapshot_id=snapshot_id,
        competition_count=competition_count,
        edition_count=edition_count,
    )
    revision = _integer(expected_revision, field='expected_revision')
    promoted_revision = revision + 1
    suffix = manifest_hash[:16]
    staging = {
        'competitions': f'{COMPETITIONS_TABLE}__publish_{suffix}',
        'competition_editions': f'{EDITIONS_TABLE}__publish_{suffix}',
    }
    transforms = {
        entity: _snapshot_transform(
            _load_transform_sql(entity, sql_root=sql_root), snapshot,
        )
        for entity in _TRANSFORM_FILES
    }
    transform_statements = tuple(
        statement
        for entity in ('competitions', 'competition_editions')
        for statement in (
            f'/* tm_registry_stage:drop:{entity} */\nDROP TABLE IF EXISTS {staging[entity]}',
            f'/* tm_registry_transform:{entity} */\n'
            f'CREATE TABLE {staging[entity]} AS\n{transforms[entity]}',
        )
    )
    staging_dq = _dq_sql(
        competitions_table=staging['competitions'],
        editions_table=staging['competition_editions'],
        snapshot_id=snapshot,
        tag='staging',
    )
    target_statements = (
        f'/* tm_registry_target:create:competitions */\n'
        f'CREATE TABLE IF NOT EXISTS {COMPETITIONS_TABLE} AS '
        f'SELECT * FROM {staging["competitions"]} WITH NO DATA',
        f'/* tm_registry_target:create:competition_editions */\n'
        f'CREATE TABLE IF NOT EXISTS {EDITIONS_TABLE} AS '
        f'SELECT * FROM {staging["competition_editions"]} WITH NO DATA',
        _merge_snapshot_sql(
            target=COMPETITIONS_TABLE,
            staging=staging['competitions'],
            columns=_COMPETITION_COLUMNS,
            keys=('competition_id',),
            tag='competitions',
        ),
        _merge_snapshot_sql(
            target=EDITIONS_TABLE,
            staging=staging['competition_editions'],
            columns=_EDITION_COLUMNS,
            keys=('competition_id', 'edition_id'),
            tag='competition_editions',
        ),
    )
    target_dq = _dq_sql(
        competitions_table=COMPETITIONS_TABLE,
        editions_table=EDITIONS_TABLE,
        snapshot_id=snapshot,
        tag='target',
        compare_competitions=staging['competitions'],
        compare_editions=staging['competition_editions'],
    )
    history = _history_sql(revision)
    cas = _cas_sql(
        snapshot_id=snapshot,
        source_hash=manifest_hash,
        competition_count=competitions,
        edition_count=editions,
        expected_revision=revision,
    )
    readback = _state_read_sql('readback')
    cleanup = tuple(
        f'/* tm_registry_stage:cleanup:{entity} */\nDROP TABLE IF EXISTS {table}'
        for entity, table in staging.items()
    )
    statements = (
        _state_read_sql('before'),
        *transform_statements,
        staging_dq,
        *target_statements,
        target_dq,
        *cleanup,
        history,
        cas,
        readback,
    )
    rollback = (
        _rollback_sql(
            expected_revision=revision,
            promoted_revision=promoted_revision,
        ),
        _state_read_sql('rollback_readback'),
    )
    registry_identity = {
        'snapshot_id': snapshot,
        'snapshot_hash': snapshot_hash,
        'discovery_manifest_hash': manifest_hash,
        'competition_count': competitions,
        'edition_count': editions,
        'expected_revision': revision,
        'promoted_revision': promoted_revision,
        'transform_hashes': {
            entity: hashlib.sha256(sql.encode('utf-8')).hexdigest()
            for entity, sql in transforms.items()
        },
        'statement_hashes': tuple(
            hashlib.sha256(sql.encode('utf-8')).hexdigest()
            for sql in statements
        ),
        'rollback_statement_hashes': tuple(
            hashlib.sha256(sql.encode('utf-8')).hexdigest()
            for sql in rollback
        ),
    }
    return RegistryPublicationPlan(
        snapshot_id=snapshot,
        snapshot_hash=snapshot_hash,
        discovery_manifest_hash=manifest_hash,
        registry_manifest_hash=stable_hash(registry_identity),
        competition_count=competitions,
        edition_count=editions,
        expected_revision=revision,
        promoted_revision=promoted_revision,
        staging_tables=tuple(staging.items()),
        transform_sql=tuple(transforms.items()),
        statements=statements,
        rollback_statements=rollback,
    )


class _SqlRunner:
    def __init__(
        self,
        *,
        executor: Callable[[str], Any] | Any | None,
        connection: Any | None,
    ) -> None:
        if (executor is None) == (connection is None):
            raise RegistryPublicationError(
                'apply=True requires exactly one executor or connection'
            )
        self._executor = executor
        self._cursor = connection.cursor() if connection is not None else None

    def close(self) -> None:
        if self._cursor is not None:
            self._cursor.close()

    @staticmethod
    def _column_name(item: Any) -> str:
        name = getattr(item, 'name', None)
        if name is not None:
            return str(name)
        if (
            isinstance(item, Sequence)
            and not isinstance(item, (str, bytes, bytearray))
            and item
        ):
            return str(item[0])
        raise RegistryPublicationError('DB-API column metadata is invalid')

    @staticmethod
    def _normalise(value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, Mapping):
            return [dict(value)]
        if (
            isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
        ):
            if not value:
                return []
            if all(isinstance(item, Mapping) for item in value):
                return [dict(item) for item in value]
        raise RegistryPublicationError(
            'injected executor must return mappings for SELECT statements'
        )

    def execute(self, sql: str) -> list[dict[str, Any]]:
        if self._cursor is not None:
            self._cursor.execute(sql)
            rows = list(self._cursor.fetchall())
            description = self._cursor.description or ()
            columns = [self._column_name(item) for item in description]
            if not rows:
                return []
            if not columns or any(len(row) != len(columns) for row in rows):
                raise RegistryPublicationError('DB-API result shape is invalid')
            return [dict(zip(columns, row, strict=True)) for row in rows]
        target = self._executor
        result = target(sql) if callable(target) else target.execute(sql)
        if hasattr(result, 'fetchall'):
            rows = list(result.fetchall())
            description = getattr(result, 'description', ()) or ()
            columns = [self._column_name(item) for item in description]
            return [dict(zip(columns, row, strict=True)) for row in rows]
        return self._normalise(result)


def _one_row(rows: Sequence[Mapping[str, Any]], *, label: str) -> Mapping[str, Any]:
    if len(rows) != 1:
        raise RegistryPublicationError(f'{label} must return exactly one row')
    return rows[0]


def _read_state(rows: Sequence[Mapping[str, Any]], *, allow_missing: bool) -> RegistryState | None:
    if not rows and allow_missing:
        return None
    value = _one_row(rows, label='registry state read')
    state = RegistryState.from_mapping(value)
    if state.state_key != 'canonical':
        raise RegistryCasError('registry state_key is not canonical')
    return state


def _validate_dq(
    rows: Sequence[Mapping[str, Any]],
    *,
    competition_count: int,
    edition_count: int,
    label: str,
) -> dict[str, int]:
    value = _one_row(rows, label=f'{label} registry DQ')
    missing = [field for field in _DQ_FIELDS if field not in value]
    if missing:
        raise RegistryDQError(f'{label} DQ is missing fields: {missing}')
    try:
        metrics = {field: int(value[field]) for field in _DQ_FIELDS}
    except (TypeError, ValueError, OverflowError) as exc:
        raise RegistryDQError(f'{label} DQ contains a non-integer metric') from exc
    expected = {
        'competition_count': competition_count,
        'competition_distinct_count': competition_count,
        'edition_count': edition_count,
        'edition_distinct_count': edition_count,
    }
    if any(metrics[field] != count for field, count in expected.items()):
        raise RegistryDQError(f'{label} exact snapshot counts do not match')
    violations = {
        field: value for field, value in metrics.items()
        if field not in expected and value != 0
    }
    if violations:
        raise RegistryDQError(f'{label} registry DQ failed: {violations}')
    return metrics


def _assert_state_before(
    state: RegistryState | None,
    *,
    expected_revision: int,
) -> None:
    if state is None:
        if expected_revision != 0:
            raise RegistryCasError(
                'registry state is missing but expected_revision is not zero'
            )
        return
    if state.revision != expected_revision:
        raise RegistryCasError(
            f'registry CAS drift: expected revision {expected_revision}, '
            f'found {state.revision}'
        )
    if state.status != 'promoted':
        raise RegistryCasError('existing canonical registry is not promoted')


def _assert_readback(state: RegistryState | None, plan: RegistryPublicationPlan) -> None:
    if state is None:
        raise RegistryCasError('registry CAS produced no canonical row')
    expected = {
        'registry_snapshot_id': plan.snapshot_id,
        'source_hash': plan.discovery_manifest_hash,
        'competition_count': plan.competition_count,
        'edition_count': plan.edition_count,
        'unknown_active_count': 0,
        'status': 'promoted',
        'revision': plan.promoted_revision,
    }
    actual = {field: getattr(state, field) for field in expected}
    if actual != expected:
        raise RegistryCasError(
            f'registry CAS readback mismatch: expected={expected} actual={actual}'
        )


def _assert_rollback_readback(
    state: RegistryState | None,
    previous: RegistryState | None,
    plan: RegistryPublicationPlan,
) -> None:
    if state is None:
        raise RegistryCasError('registry rollback produced no canonical row')
    if previous is None:
        expected = {
            'registry_snapshot_id': '',
            'source_hash': '',
            'competition_count': 0,
            'edition_count': 0,
            'unknown_active_count': 0,
            'status': 'uninitialized',
        }
        allowed_revisions = {plan.promoted_revision + 1}
    else:
        expected = {
            'registry_snapshot_id': previous.registry_snapshot_id,
            'source_hash': previous.source_hash,
            'competition_count': previous.competition_count,
            'edition_count': previous.edition_count,
            'unknown_active_count': previous.unknown_active_count,
            'status': previous.status,
        }
        allowed_revisions = {
            previous.revision,
            plan.promoted_revision + 1,
        }
    actual = {field: getattr(state, field) for field in expected}
    if actual != expected or state.revision not in allowed_revisions:
        raise RegistryCasError(
            'registry rollback readback mismatch: '
            f'expected={expected} revisions={sorted(allowed_revisions)} '
            f'actual={actual} revision={state.revision}'
        )


def publish_registry(
    discovery_manifest: Mapping[str, Any],
    *,
    manifest_hash: str,
    snapshot_id: str,
    competition_count: int,
    edition_count: int,
    expected_revision: int,
    apply: bool = False,
    executor: Callable[[str], Any] | Any | None = None,
    connection: Any | None = None,
    sql_root: str | Path | None = None,
) -> RegistryPublicationResult:
    """Plan or apply one exact registry publication.

    Manifest validation and SQL rendering always happen before an executor is
    touched.  In apply mode the canonical state is the final write and exact
    readback is mandatory; a zero-row CAS is therefore never a silent success.
    """

    root = Path(sql_root) if sql_root is not None else _SQL_ROOT
    plan = _build_plan(
        discovery_manifest,
        manifest_hash=manifest_hash,
        snapshot_id=snapshot_id,
        competition_count=competition_count,
        edition_count=edition_count,
        expected_revision=expected_revision,
        sql_root=root,
    )
    if not apply:
        return RegistryPublicationResult(plan=plan, applied=False)

    runner = _SqlRunner(executor=executor, connection=connection)
    previous: RegistryState | None = None
    cas_attempted = False
    try:
        previous = _read_state(
            runner.execute(plan.statements[0]), allow_missing=True,
        )
        _assert_state_before(previous, expected_revision=plan.expected_revision)

        staging_dq: dict[str, int] | None = None
        target_dq: dict[str, int] | None = None
        promoted: RegistryState | None = None
        for statement in plan.statements[1:]:
            if 'tm_registry_state:cas' in statement:
                # A coordinator failure can happen after the MERGE committed
                # but before its result was returned. Treat execution as a
                # possible pointer change and compensate on every later error.
                cas_attempted = True
            rows = runner.execute(statement)
            if 'tm_registry_dq:staging' in statement:
                staging_dq = _validate_dq(
                    rows,
                    competition_count=plan.competition_count,
                    edition_count=plan.edition_count,
                    label='staging',
                )
            elif 'tm_registry_dq:target' in statement:
                target_dq = _validate_dq(
                    rows,
                    competition_count=plan.competition_count,
                    edition_count=plan.edition_count,
                    label='target',
                )
            elif 'tm_registry_state:cas' in statement:
                if staging_dq is None or target_dq is None:
                    raise RegistryDQError('CAS reached before both DQ gates')
            elif 'tm_registry_state:readback' in statement:
                promoted = _read_state(rows, allow_missing=False)
                _assert_readback(promoted, plan)
        if staging_dq is None or target_dq is None or promoted is None:
            raise RegistryPublicationError('publication statement plan was incomplete')
        combined = {
            f'staging.{key}': value for key, value in staging_dq.items()
        }
        combined.update({
            f'target.{key}': value for key, value in target_dq.items()
        })
        combined_dq = tuple(sorted(combined.items()))
        return RegistryPublicationResult(
            plan=plan,
            applied=True,
            previous_state=previous,
            promoted_state=promoted,
            dq=combined_dq,
        )
    except Exception as exc:
        if cas_attempted:
            try:
                restored: RegistryState | None = None
                for statement in plan.rollback_statements:
                    rows = runner.execute(statement)
                    if 'tm_registry_state:rollback_readback' in statement:
                        restored = _read_state(rows, allow_missing=False)
                _assert_rollback_readback(restored, previous, plan)
            except Exception as rollback_exc:
                raise RegistryCasError(
                    'registry publication failed after CAS and automatic '
                    f'rollback was not proven: {rollback_exc}'
                ) from exc
        raise
    finally:
        runner.close()


__all__ = [
    'COMPETITIONS_TABLE',
    'EDITIONS_TABLE',
    'REGISTRY_STATE_TABLE',
    'RegistryCasError',
    'RegistryDQError',
    'RegistryManifestError',
    'RegistryPublicationError',
    'RegistryPublicationPlan',
    'RegistryPublicationResult',
    'RegistryState',
    'publish_registry',
    'stable_hash',
]
