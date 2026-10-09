"""Snapshot-pinned Bronze-only DQ for Transfermarkt historical campaigns."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
import json
import numbers
from typing import Any, Iterable, Mapping, Sequence

from utils import transfermarkt_bronze_dq as bronze_dq
from utils.transfermarkt_scope_state import SCOPE_MANIFEST_TABLE


BACKFILL_PIN_TABLES = tuple(sorted({
    *bronze_dq.NATIVE_BRONZE_SCOPE_COLUMNS,
    bronze_dq.COMPETITIONS_REGISTRY_TABLE,
    bronze_dq.EDITIONS_REGISTRY_TABLE,
    SCOPE_MANIFEST_TABLE,
}))
BACKFILL_ENTITY_TABLES = tuple(sorted(set(
    bronze_dq.ENTITY_BRONZE_TABLES.values()
)))

ENTITY_FINGERPRINT_COLUMNS = {
    'squad_memberships': (
        'league', 'season', 'club_id', 'club_name', 'player_id',
        'player_slug', 'player_name',
    ),
    'player_attribute_observations': (
        'player_id', 'player_slug', 'name', 'position', 'dob', 'age',
        'height_cm', 'foot', 'nationality', 'contract_until',
        'market_value_eur', 'league', 'season', 'club_id', 'club_name',
    ),
    'player_contract_observations': (
        'player_id', 'contract_until', 'team_id', 'team_name',
    ),
    'market_value_points': (
        'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
    ),
    'transfer_events': (
        'player_id', 'transfer_date', 'from_club_id', 'from_club_name',
        'to_club_id', 'to_club_name', 'fee_text', 'is_upcoming',
        'fee_eur', 'market_value_eur',
    ),
    'coach_profiles': (
        'coach_id', 'coach_slug', 'name', 'dob', 'nationality',
    ),
    'coach_stints': (
        'club_id', 'club_name', 'coach_id', 'coach_slug', 'name', 'role',
    ),
}


class BackfillDqError(RuntimeError):
    """Batch evidence is incomplete, corrupt, or cannot be pinned."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
        default=str,
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _quoted(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def pin_iceberg_snapshots(
    cur: Any,
    *,
    tables: Iterable[str] = BACKFILL_PIN_TABLES,
) -> dict[str, int]:
    """Pin every input once after the mapped batch has stopped writing."""

    pins: dict[str, int] = {}
    for table in sorted(set(tables)):
        try:
            catalog, schema, name = table.split('.', 2)
        except ValueError as exc:
            raise BackfillDqError(f'invalid Iceberg relation: {table!r}') from exc
        cur.execute(
            f'SELECT snapshot_id FROM {catalog}.{schema}."{name}$snapshots" '
            'ORDER BY committed_at DESC LIMIT 1'
        )
        rows = list(cur.fetchall())
        if len(rows) != 1 or len(rows[0]) != 1 or int(rows[0][0]) <= 0:
            raise BackfillDqError(
                f'no unique positive Iceberg snapshot for {table}'
            )
        pins[table] = int(rows[0][0])
    if set(pins) != set(BACKFILL_PIN_TABLES):
        raise BackfillDqError('backfill snapshot set is incomplete')
    return pins


def build_raw_lineage_sql(
    table: str,
    *,
    snapshot_id: int,
    child_cycle_ids: Sequence[str],
) -> str:
    if table not in BACKFILL_ENTITY_TABLES:
        raise BackfillDqError(f'raw-lineage table is not native Bronze: {table}')
    if int(snapshot_id) <= 0:
        raise BackfillDqError('raw-lineage snapshot must be positive')
    cycles = tuple(sorted({str(value).strip() for value in child_cycle_ids}))
    if not cycles or any(not value for value in cycles):
        raise BackfillDqError('raw-lineage query requires child cycle ids')
    values = ', '.join(_quoted(value) for value in cycles)
    return f"""SELECT DISTINCT raw_capture_id, source_body_hash, scope_id, cycle_id
FROM {table} FOR VERSION AS OF {int(snapshot_id)}
WHERE cycle_id IN ({values})"""


def build_manifest_entity_rows_sql(
    entity: str,
    *,
    snapshot_id: int,
    child_cycle_id: str,
    scope_id: str,
) -> str:
    table = bronze_dq.ENTITY_BRONZE_TABLES.get(entity)
    columns = ENTITY_FINGERPRINT_COLUMNS.get(entity)
    if table is None or columns is None:
        raise BackfillDqError(f'unknown manifest entity: {entity}')
    if int(snapshot_id) <= 0:
        raise BackfillDqError('manifest entity snapshot must be positive')
    projection = ', '.join(columns)
    return f"""SELECT {projection}
FROM {table} FOR VERSION AS OF {int(snapshot_id)}
WHERE cycle_id = {_quoted(child_cycle_id)}
  AND scope_id = {_quoted(scope_id)}"""


def _canonical_cell(value: Any) -> str:
    if value is None:
        return '__NULL__'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, numbers.Real):
        number = float(value)
        return str(int(number)) if number.is_integer() else repr(number)
    text = str(value)
    return '__NULL__' if text in {'nan', 'NaT', '<NA>', 'None'} else text


def _fingerprint_rows(rows: Sequence[Sequence[Any]]) -> tuple[int, str]:
    normalised = sorted({
        tuple(_canonical_cell(value) for value in row) for row in rows
    })
    return len(normalised), hashlib.sha256(json.dumps(
        normalised,
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')).hexdigest()


def verify_manifest_entity_fingerprints(
    cur: Any,
    *,
    pins: Mapping[str, int],
    manifests: Sequence[Any],
) -> dict[str, Any]:
    """Match every manifest count/hash to its exact pinned child-cycle rows."""

    checked: list[tuple[str, str, str, int, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for manifest in manifests:
        child_cycle_id = str(manifest.child_cycle_id)
        scope_id = str(manifest.scope_id)
        for evidence in manifest.entities:
            entity = str(evidence.entity)
            identity = (child_cycle_id, scope_id, entity)
            if identity in seen:
                raise BackfillDqError(
                    f'duplicate manifest entity identity: {identity}'
                )
            seen.add(identity)
            table = bronze_dq.ENTITY_BRONZE_TABLES.get(entity)
            if table is None or table not in pins:
                raise BackfillDqError(f'{entity}: pinned Bronze table is absent')
            receipts = getattr(manifest, 'dq_evidence', {}).get('historical_career_receipts', {}).get(entity)
            if receipts:
                rows = historical_career_rows(cur, entity, receipts)
            else:
                cur.execute(build_manifest_entity_rows_sql(
                    entity, snapshot_id=int(pins[table]),
                    child_cycle_id=child_cycle_id, scope_id=scope_id))
                rows = list(cur.fetchall())
            count, digest = _fingerprint_rows(rows)
            if (
                count != int(evidence.dedup_rows)
                or digest != str(evidence.key_hash)
            ):
                raise BackfillDqError(
                    f'{identity}: pinned Bronze count/hash differs from manifest'
                )
            checked.append((*identity, count, digest))
    return {
        'entity_count': len(checked),
        'relation_hash': stable_hash(checked),
        'row_count': sum(item[3] for item in checked),
    }


def _historical_physical_refs(receipt):
    refs = receipt.get('physical_refs')
    if not isinstance(refs, list) or not refs or any(
        not isinstance(ref, (list, tuple)) or len(ref) != 3
        or not isinstance(ref[0], str) or not ref[0]
        or not isinstance(ref[1], str) or not ref[1]
        or type(ref[2]) is not int or ref[2] < 0 for ref in refs
    ):
        raise BackfillDqError('historical career physical refs are invalid')
    if len({ref[0] for ref in refs}) != len(refs) or [ref[0] for ref in refs] != receipt.get('player_ids'):
        raise BackfillDqError('historical career physical player identity drifted')
    if sum(ref[2] for ref in refs) != receipt['row_count']:
        raise BackfillDqError('historical career physical row counts drifted')
    return refs


def historical_career_sql(entity, receipt, *, lineage=False):
    if entity not in {'market_value_points', 'transfer_events'}:
        raise BackfillDqError('historical receipt must reference a native player career')
    snapshot = receipt.get('snapshot_id')
    if type(snapshot) is not int or snapshot <= 0:
        raise BackfillDqError('historical career snapshot must be positive')
    refs = _historical_physical_refs(receipt)
    columns = 'raw_capture_id, source_body_hash, scope_id, cycle_id, player_id, _batch_id' if lineage else ', '.join(ENTITY_FINGERPRINT_COLUMNS[entity])
    return (f"SELECT {columns} FROM {bronze_dq.ENTITY_BRONZE_TABLES[entity]} FOR VERSION AS OF {snapshot} "
            "WHERE " + ' OR '.join(f"(player_id = {_quoted(pid)} AND _batch_id = {_quoted(batch)})"
                                   for pid, batch, _ in refs))


def historical_career_rows(cur, entity, receipts):
    """Verify each original snapshot, then choose each player's last full career."""
    by_player = {}
    for receipt in receipts:
        refs = _historical_physical_refs(receipt)
        if receipt['row_count'] == 0:
            rows = []
        else:
            cur.execute(historical_career_sql(entity, receipt))
            rows = list(cur.fetchall())
        count, digest = _fingerprint_rows(rows)
        if count != receipt['row_count'] or digest != receipt['key_hash']:
            raise BackfillDqError('original historical career count/hash drifted')
        grouped = {pid: [] for pid in receipt['player_ids']}
        for row in rows:
            pid = str(row[0])
            if pid not in grouped:
                raise BackfillDqError('historical career snapshot contains another player')
            grouped[pid].append(row)
        if any(len(grouped[pid]) != expected for pid, _, expected in refs):
            raise BackfillDqError('historical original player career count drifted')
        by_player.update(grouped)
    return [row for rows in by_player.values() for row in rows]


def _verify_original_empty_careers(raw_store, entity, receipt):
    """An absent original table is valid only with exact typed empty raw proof."""
    from datetime import timezone
    from scrapers.transfermarkt.client import _payload_hash
    empty_ids = {ref[0] for ref in _historical_physical_refs(receipt) if ref[2] == 0}
    proofs = receipt.get('empty_capture_refs', [])
    if not isinstance(proofs, list) or len(proofs) != len(empty_ids) or {
            str(proof.get('player_id', '')) for proof in proofs if isinstance(proof, Mapping)} != empty_ids:
        raise BackfillDqError('original empty career inventory drifted')
    for proof in proofs:
        if proof.get('status') != 'authoritative_empty' or proof.get('expected_rows') != 0:
            raise BackfillDqError('original empty career lacks typed complete outcome')
        stamp = datetime.fromisoformat(str(proof['captured_at']).replace('Z', '+00:00'))
        if stamp.tzinfo is None or len(str(proof.get('intent_sha256', ''))) != 64:
            raise BackfillDqError('original empty career clock/intent is invalid')
        verified = False
        for source in [*proof.get('raw_attempts', []), *proof.get('cache_sources', [])]:
            capture_id = source.get('capture_id')
            if not capture_id or str(source.get('url', '')).rstrip('/').rsplit('/', 1)[-1] != proof['player_id']:
                continue
            if source.get('envelope_id'):
                envelope = raw_store.load_attempt_envelope(source['envelope_id'])
                if envelope.capture_id != capture_id or envelope.outcome_kind != 'response':
                    raise BackfillDqError('original empty career envelope drifted')
            body, capture = raw_store.load_capture(capture_id)
            if capture.status_code != 200:
                continue
            captured_at = datetime.fromisoformat(capture.fetched_at.replace('Z', '+00:00'))
            if captured_at.astimezone(timezone.utc) != stamp.astimezone(timezone.utc):
                continue
            try:
                payload = json.loads(body.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                # Earlier paid retries are evidence too. A failed response
                # never proves empty, but must not hide the verified outcome.
                continue
            field = 'list' if entity == 'market_value_points' else 'transfers'
            if (isinstance(payload, dict) and payload.get(field) == []
                    and _payload_hash(payload) == proof.get('payload_hash')
                    and captured_at.astimezone(timezone.utc) == stamp.astimezone(timezone.utc)):
                verified = True
        if not verified:
            raise BackfillDqError('original typed empty career raw cannot be verified')


def verify_raw_lineage(
    cur: Any,
    *,
    pins: Mapping[str, int],
    child_cycle_ids: Sequence[str],
    raw_store: Any,
    attempt_envelopes: Sequence[Any],
    manifest_scope_cycles: Sequence[Sequence[str]],
    scope_statuses: Mapping[str, str],
    manifests: Sequence[Any] = (),
) -> dict[str, Any]:
    """Resolve every newly written Bronze capture to exact immutable bytes."""

    allowed: dict[str, tuple[str, str]] = {}
    for envelope in attempt_envelopes:
        if str(getattr(envelope, 'outcome_kind', '')) != 'response':
            continue
        capture_id = str(getattr(envelope, 'capture_id', '') or '')
        identity = (
            str(getattr(envelope, 'scope_id', '') or ''),
            str(getattr(envelope, 'cycle_id', '') or ''),
        )
        if len(capture_id) != 64 or not all(identity):
            raise BackfillDqError('attempt envelope response identity is incomplete')
        existing = allowed.get(capture_id)
        if existing is not None and existing != identity:
            raise BackfillDqError('capture is bound to conflicting attempt identities')
        allowed[capture_id] = identity

    captures: dict[str, str] = {}
    partial_capture_ids: set[str] = set()
    partial_inventory: dict[tuple[str, str, str], set[str]] = {}
    manifested = {
        (str(item[0]), str(item[1])) for item in manifest_scope_cycles
    }
    rows_by_table: dict[str, int] = {}
    cached_origins = {}
    for table in BACKFILL_ENTITY_TABLES:
        snapshot = pins.get(table)
        if snapshot is None:
            raise BackfillDqError(f'raw-lineage snapshot is absent: {table}')
        cur.execute(build_raw_lineage_sql(
            table,
            snapshot_id=int(snapshot),
            child_cycle_ids=child_cycle_ids,
        ))
        rows = list(cur.fetchall())
        for manifest in manifests:
            for entity, receipts in getattr(manifest, 'dq_evidence', {}).get('historical_career_receipts', {}).items():
                if bronze_dq.ENTITY_BRONZE_TABLES[entity] != table:
                    continue
                for receipt in receipts:
                    _verify_original_empty_careers(raw_store, entity, receipt)
                    if receipt['row_count']:
                        cur.execute(historical_career_sql(entity, receipt, lineage=True))
                        original_rows = list(cur.fetchall())
                        for row in original_rows:
                            if len(row) != 6:
                                raise BackfillDqError('original career lineage lacks physical player/batch identity')
                            capture_id, body_hash, original_scope, original_cycle, player_id, physical_batch = row
                            # A fresh capture must still belong to the exact paid
                            # attempt inventory. Only existing physical cache can
                            # rely on its original retained snapshot/raw proof.
                            if capture_id not in allowed and receipt.get('native_batch_id') and physical_batch != receipt['native_batch_id']:
                                origin = (str(original_scope), str(original_cycle))
                                if capture_id in cached_origins and cached_origins[capture_id] != origin:
                                    raise BackfillDqError('cached career capture ownership drifted')
                                cached_origins[capture_id] = origin
                        rows.extend(row[:4] for row in original_rows)
        rows_by_table[table] = len(rows)
        for row in rows:
            if len(row) != 4:
                raise BackfillDqError(f'{table}: malformed lineage row')
            capture_id = str(row[0] or '').strip()
            body_hash = str(row[1] or '').strip()
            scope_id = str(row[2] or '').strip()
            cycle_id = str(row[3] or '').strip()
            if len(capture_id) != 64 or len(body_hash) != 64:
                raise BackfillDqError(f'{table}: incomplete raw lineage')
            cached = cached_origins.get(capture_id) == (scope_id, cycle_id)
            if allowed.get(capture_id) != (scope_id, cycle_id) and not cached:
                raise BackfillDqError(
                    f'{table}: Bronze capture is outside exact attempt evidence'
                )
            if (scope_id, cycle_id) not in manifested and not cached:
                # A source attempt may have committed earlier entities before
                # a later endpoint failed.  The batch may close with that scope
                # retryable, provided every partial row is exact raw-backed;
                # its campaign-stable checkpoint will resume in a later batch.
                partial_capture_ids.add(capture_id)
                partial_inventory.setdefault(
                    (scope_id, cycle_id, table), set()
                ).add(capture_id)
            existing = captures.get(capture_id)
            if existing is not None and existing != body_hash:
                raise BackfillDqError(
                    f'{capture_id}: one capture has conflicting body hashes'
                )
            captures[capture_id] = body_hash

    for capture_id, expected_hash in sorted(captures.items()):
        try:
            body, record = raw_store.load_capture(capture_id)
        except Exception as exc:  # storage adapters expose source-specific errors
            raise BackfillDqError(
                f'raw capture cannot be verified: {capture_id}'
            ) from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if (
            str(getattr(record, 'capture_id', '')) != capture_id
            or str(getattr(record, 'content_hash', '')) != expected_hash
            or actual_hash != expected_hash
        ):
            raise BackfillDqError(
                f'raw capture hash differs from Bronze: {capture_id}'
            )
        if capture_id in cached_origins and cached_origins[capture_id] != (
            str(getattr(record, 'scope_id', '')), str(getattr(record, 'cycle_id', ''))
        ):
            raise BackfillDqError('cached career raw capture ownership differs from original snapshot')
    rendered_partial: list[dict[str, Any]] = []
    for (scope_id, cycle_id, table), capture_ids in sorted(
        partial_inventory.items()
    ):
        status = str(scope_statuses.get(scope_id) or "")
        if status not in {"retryable_error", "terminal_error", "unavailable"}:
            raise BackfillDqError(
                f"{scope_id}: partial Bronze rows lack a terminal/retryable scope state"
            )
        rendered_partial.append({
            "scope_id": scope_id,
            "child_cycle_id": cycle_id,
            "table": table,
            "scope_status": status,
            "capture_count": len(capture_ids),
            "capture_set_hash": stable_hash(sorted(capture_ids)),
        })
    return {
        'capture_count': len(captures),
        'capture_set_hash': stable_hash(sorted(captures.items())),
        'partial_capture_count': len(partial_capture_ids),
        'partial_capture_inventory': rendered_partial,
        'rows_by_table': rows_by_table,
    }


@dataclass(frozen=True)
class BackfillBatchDqReport:
    campaign_id: str
    batch_id: str
    registry_snapshot_id: str
    snapshot_ids: Mapping[str, int]
    child_cycle_ids: tuple[str, ...]
    bronze_checks: tuple[Mapping[str, Any], ...]
    raw_lineage: Mapping[str, Any]
    passed: bool

    @property
    def report_hash(self) -> str:
        return stable_hash(asdict(self))

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value['report_hash'] = self.report_hash
        return value


def run_backfill_batch_dq(
    cur: Any,
    *,
    campaign_id: str,
    batch_id: str,
    registry_snapshot_id: str,
    manifests: Sequence[Any],
    child_cycle_ids: Sequence[str],
    scope_bindings: Sequence[Sequence[str]],
    raw_store: Any,
    attempt_envelopes: Sequence[Any],
    scope_statuses: Mapping[str, str],
    pins: Mapping[str, int] | None = None,
) -> BackfillBatchDqReport:
    """Run existing scope-set DQ and the additional raw-lineage gate."""

    snapshots = dict(pins or pin_iceberg_snapshots(cur))
    fingerprints = verify_manifest_entity_fingerprints(cur, pins=snapshots, manifests=manifests)
    historical_presence = {
        (str(manifest.scope_id), str(evidence.entity)): int(evidence.dedup_rows)
        for manifest in manifests for evidence in manifest.entities
        if str(evidence.entity) in getattr(manifest, 'dq_evidence', {}).get('historical_career_receipts', {})
    }
    results = bronze_dq.run_bronze_dq(
        cur,
        registry_snapshot_id=registry_snapshot_id,
        pins=snapshots,
        zone='scope_set',
        manifests=manifests,
        scope_bindings=scope_bindings,
        **({'historical_presence': historical_presence} if historical_presence else {}),
    )
    errors = [item for item in results if item.severity == 'ERROR' and not item.passed]
    lineage = verify_raw_lineage(
        cur,
        pins=snapshots,
        child_cycle_ids=child_cycle_ids,
        raw_store=raw_store,
        attempt_envelopes=attempt_envelopes,
        manifest_scope_cycles=tuple(
            (str(item.scope_id), str(item.child_cycle_id))
            for item in manifests
        ),
        scope_statuses=scope_statuses,
        manifests=manifests,
    ) if child_cycle_ids else {
        'capture_count': 0,
        'capture_set_hash': stable_hash([]),
        'partial_capture_count': 0,
        'partial_capture_inventory': [],
        'rows_by_table': {},
    }
    if int(fingerprints['row_count']) > 0 and int(lineage['capture_count']) == 0:
        raise BackfillDqError(
            'non-empty captured manifests have no exact raw lineage'
        )
    rendered = tuple({
        'name': item.name,
        'kind': item.kind,
        'severity': item.severity,
        'passed': item.passed,
        'value': item.value,
        'details': item.details,
        'error': item.error,
    } for item in results) + ({
        'name': 'tm_backfill_exact_manifest_entity_fingerprints',
        'kind': 'exact_manifest_entity_fingerprints',
        'severity': 'ERROR',
        'passed': True,
        'value': fingerprints,
        'details': 'exact child-cycle entity count/hash matched',
        'error': None,
    },)
    return BackfillBatchDqReport(
        campaign_id=str(campaign_id),
        batch_id=str(batch_id),
        registry_snapshot_id=str(registry_snapshot_id),
        snapshot_ids=dict(sorted(snapshots.items())),
        child_cycle_ids=tuple(sorted(set(child_cycle_ids))),
        bronze_checks=rendered,
        raw_lineage=lineage,
        passed=not errors,
    )


__all__ = [
    'BACKFILL_ENTITY_TABLES',
    'BACKFILL_PIN_TABLES',
    'BackfillBatchDqReport',
    'BackfillDqError',
    'build_raw_lineage_sql',
    'build_manifest_entity_rows_sql',
    'pin_iceberg_snapshots',
    'run_backfill_batch_dq',
    'stable_hash',
    'verify_raw_lineage',
    'verify_manifest_entity_fingerprints',
]
