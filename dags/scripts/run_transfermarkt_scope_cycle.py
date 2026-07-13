#!/usr/bin/env python3
"""Run one exact Transfermarkt ``competition x edition`` child cycle.

The wrapper is intentionally sequential.  Four isolated Bronze runners share
one parent-cycle byte ledger, while their immutable results and the final scope
manifest stay content-addressed under the planner-provided result directory.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from dags.utils.transfermarkt_approval import (
    ApprovalDriftError,
    ApprovalJournal,
    ApprovalPacket,
    ApprovalStateError,
)
from dags.utils.transfermarkt_dq_contracts import (
    ScopeDQError,
    entity_applicability_contracts,
    input_from_capture,
    validate_scope_capture,
)
from dags.utils.transfermarkt_scope_state import (
    EntityEvidence,
    PROXY_LEDGER_TABLE,
    SCOPE_COMPLETION_STATUS,
    SCOPE_MANIFEST_TABLE,
    ScopeManifest,
    stable_hash,
)
from scrapers.transfermarkt.registry import (
    CompetitionRecord,
    EditionRecord,
    canonical_season,
    deterministic_scope_id,
    resolve_competition,
)


MIB = 1024 * 1024
RESPONSE_CACHE_ROOT = Path('/opt/airflow/logs/transfermarkt-native-v2/cache')
RESPONSE_CACHE_TTL_SECONDS = 24 * 60 * 60
HARD_BYTE_CAP = 15_728_640
SOFT_BYTE_STOP = 14_680_064
# This is the cycle-wide retry ledger, not a per-page cap: the source answers
# 502/504 in waves, and a scope fetches dozens of pages, so a handful of retries
# is spent by the first two pages. A failed attempt costs ~10 KiB, and the real
# bound on paid traffic is the 15 MiB byte cap, not the number of attempts.
PARENT_RETRY_LIMIT = 128
ENTITY_ORDER = (
    'players',
    'market_value_history',
    'transfers',
    'coaches',
)
EXPECTED_ENTITIES = (
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
    'market_value_points',
    'transfer_events',
    'coach_profiles',
    'coach_stints',
)
ENTITY_OUTPUTS = {
    'players': (
        ('memberships', 'squad_memberships'),
        ('attribute_observations', 'player_attribute_observations'),
        ('contract_observations', 'player_contract_observations'),
    ),
    'market_value_history': (
        ('market_value_points', 'market_value_points'),
    ),
    'transfers': (('transfer_events', 'transfer_events'),),
    'coaches': (
        ('profiles', 'coach_profiles'),
        ('stints', 'coach_stints'),
    ),
}
ENTITY_TABLES = {
    entity: f'iceberg.bronze.transfermarkt_{entity}'
    for entity in EXPECTED_ENTITIES
}
LEGACY_TABLES = {
    'iceberg.bronze.transfermarkt_players',
    'iceberg.bronze.transfermarkt_market_value_history',
    'iceberg.bronze.transfermarkt_transfers',
    'iceberg.bronze.transfermarkt_coaches',
}
OPS_WRITE_TABLES = {
    'iceberg.ops.transfermarkt_fetch_state',
    'iceberg.ops.proxy_traffic_runs',
    PROXY_LEDGER_TABLE,
}
# 'requests' counts attempts, not pages: a squad page that answers 504 twice
# costs three. A 20-club league already needs ~21 pages, so 26 attempts left no
# room for the source's failure waves and the entity died mid-league.
DEFAULT_ENTITY_LIMITS = {
    'players': {'decoded_bytes': 10 * MIB, 'requests': 150},
    'market_value_history': {'decoded_bytes': 4 * MIB, 'requests': 200},
    'transfers': {'decoded_bytes': 8 * MIB, 'requests': 200},
    'coaches': {'decoded_bytes': 14 * MIB, 'requests': 160},
}
_APPROVAL_FLAGS = {
    '--approval-journal',
    '--approval-packet-id',
    '--approval-packet-hash',
    '--approval-bundle-json',
    '--paid-proxy-approval-packet-id',
    '--paid-proxy-approval-packet-hash',
    '--production-write-approval-packet-id',
    '--production-write-approval-packet-hash',
}


class ScopeCycleError(RuntimeError):
    """The exact child cycle cannot produce promotion-ready evidence."""


@dataclass(frozen=True)
class ScopeIdentity:
    parent_cycle_id: str
    child_cycle_id: str
    competition_id: str
    edition_id: str
    canonical_competition_id: str
    canonical_season: str
    registry_snapshot_id: str
    capture_revision: str
    scope_id: str
    result_base_dir: str
    entity_dir: str
    scope_manifest_path: str
    parent_ledger_path: str
    competition_record: Mapping[str, Any]
    edition_current: bool
    edition_participant_count: int | None


@dataclass(frozen=True)
class ApprovalRef:
    packet_id: str
    packet_hash: str


@dataclass(frozen=True)
class EntityRun:
    parser_entity: str
    result_path: str
    result_sha256: str
    result: Mapping[str, Any]
    wall_clock_duration_ms: int
    resumed: bool


def required_write_tables(write_mode: str) -> set[str]:
    tables = set(ENTITY_TABLES.values()) | OPS_WRITE_TABLES | {
        SCOPE_MANIFEST_TABLE,
    }
    if write_mode == 'dual':
        tables |= LEGACY_TABLES | {
            'iceberg.ops.transfermarkt_dual_write_manifest_v2',
        }
    else:
        tables.add('iceberg.ops.transfermarkt_native_write_manifest_v2')
    return tables


def _required(value: Any, name: str) -> str:
    text = str(value or '').strip()
    if not text:
        raise ScopeCycleError(f'{name} is required')
    if any(character in text for character in ('\x00', '\n', '\r')):
        raise ScopeCycleError(f'{name} contains a control character')
    return text


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_value(value: str, *, name: str) -> Any:
    raw = _required(value, name)
    if raw.startswith(('{', '[')):
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ScopeCycleError(f'{name} is invalid JSON') from exc
    path = Path(raw)
    try:
        return json.loads(path.read_text('utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScopeCycleError(f'{name} file is unreadable') from exc


def _absolute_path(value: Any, name: str) -> str:
    path = Path(_required(value, name))
    if not path.is_absolute() or '..' in path.parts:
        raise ScopeCycleError(f'{name} must be an absolute normalized path')
    return str(path)


def _coalesce_exact(
    payload: Mapping[str, Any],
    key: str,
    explicit: Any,
) -> Any:
    supplied = payload.get(key)
    if explicit not in (None, '') and supplied not in (None, ''):
        if str(explicit) != str(supplied):
            raise ScopeCycleError(f'{key} differs between argv and payload')
    return explicit if explicit not in (None, '') else supplied


def _scope_identity(args: argparse.Namespace) -> ScopeIdentity:
    payload: Mapping[str, Any] = {}
    if args.payload_json:
        loaded = _load_json_value(args.payload_json, name='payload_json')
        if not isinstance(loaded, Mapping):
            raise ScopeCycleError('payload_json must contain an object')
        payload = loaded

    parent_cycle_id = _required(
        _coalesce_exact(payload, 'parent_cycle_id', args.parent_cycle_id),
        'parent_cycle_id',
    )
    child_cycle_id = _required(
        _coalesce_exact(payload, 'child_cycle_id', args.child_cycle_id),
        'child_cycle_id',
    )
    competition_id = _required(
        _coalesce_exact(payload, 'competition_id', args.competition_id),
        'competition_id',
    )
    edition_id = _required(
        _coalesce_exact(payload, 'edition_id', args.edition_id),
        'edition_id',
    )
    registry_snapshot_id = _required(
        _coalesce_exact(
            payload, 'registry_snapshot_id', args.registry_snapshot_id,
        ),
        'registry_snapshot_id',
    )
    scope_id = _required(
        _coalesce_exact(payload, 'scope_id', args.scope_id), 'scope_id',
    )
    expected_scope = deterministic_scope_id(competition_id, edition_id)
    if scope_id != expected_scope:
        raise ScopeCycleError(
            f'scope_id mismatch: expected {expected_scope}, got {scope_id}'
        )

    raw_competition = payload.get('competition_record')
    if raw_competition is not None:
        if not isinstance(raw_competition, Mapping):
            raise ScopeCycleError('competition_record must be an object')
        competition = CompetitionRecord.from_mapping(raw_competition)
    else:
        competition = resolve_competition(competition_id)
    if competition.competition_id != competition_id:
        raise ScopeCycleError('competition_record identity mismatch')
    if not competition.crawl_eligible:
        raise ScopeCycleError(
            f'{competition_id}: classification blocks crawl: '
            f'{competition.crawl_block_reason}'
        )
    if (
        competition.registry_snapshot_id
        and competition.registry_snapshot_id != registry_snapshot_id
    ):
        raise ScopeCycleError('competition registry snapshot mismatch')

    raw_edition = payload.get('edition_record')
    edition_current = args.refresh_mode == 'current'
    edition_participant_count = None
    canonical = _coalesce_exact(
        payload, 'canonical_season', args.canonical_season,
    )
    if raw_edition is not None:
        if not isinstance(raw_edition, Mapping):
            raise ScopeCycleError('edition_record must be an object')
        edition = EditionRecord.from_mapping(raw_edition)
        if (edition.competition_id, edition.edition_id) != (
            competition_id, edition_id,
        ):
            raise ScopeCycleError('edition_record identity mismatch')
        if edition.registry_snapshot_id != registry_snapshot_id:
            raise ScopeCycleError('edition registry snapshot mismatch')
        if edition.season_format != competition.season_format:
            raise ScopeCycleError('competition/edition season format mismatch')
        if canonical not in (None, '') and str(canonical) != edition.canonical_season:
            raise ScopeCycleError('canonical season differs from edition registry')
        canonical = edition.canonical_season
        edition_current = bool(edition.current)
        edition_participant_count = edition.participant_count
    # The source offsets some calendar leagues' saison_id from the season it
    # names (saison_id 2023 is labelled "2024"), so the registered edition is
    # checked against the label it was derived from, exactly as registry DQ
    # does. Only a caller who passed no edition record at all is left with the
    # edition id as the sole statement of its season.
    expected_canonical = canonical_season(
        edition.edition_label if raw_edition is not None else edition_id,
        competition.season_format,
    )
    canonical = _required(canonical or expected_canonical, 'canonical_season')
    if canonical != expected_canonical:
        raise ScopeCycleError(
            f'canonical season mismatch: expected {expected_canonical}, '
            f'got {canonical}'
        )
    canonical_competition_id = _required(
        payload.get('canonical_competition_id')
        or competition.canonical_competition_id
        or f'TM-{competition_id}',
        'canonical_competition_id',
    )

    result_paths = payload.get('result_paths') or {}
    if not isinstance(result_paths, Mapping):
        raise ScopeCycleError('result_paths must be an object')
    result_base_dir = _absolute_path(
        args.result_base_dir or result_paths.get('base_dir'),
        'result_base_dir',
    )
    planned_base = result_paths.get('base_dir')
    if planned_base and result_base_dir != _absolute_path(
        planned_base, 'result_paths.base_dir',
    ):
        raise ScopeCycleError('result_base_dir differs from planned path')
    entity_dir = _absolute_path(
        result_paths.get('entity_staging_dir')
        or str(Path(result_base_dir) / 'entities'),
        'entity_dir',
    )
    manifest_path = _absolute_path(
        result_paths.get('scope_manifest')
        or str(Path(result_base_dir) / 'scope-manifest.json'),
        'scope_manifest_path',
    )
    for child_path, name in (
        (entity_dir, 'entity_dir'),
        (manifest_path, 'scope_manifest_path'),
    ):
        try:
            Path(child_path).relative_to(result_base_dir)
        except ValueError as exc:
            raise ScopeCycleError(f'{name} escapes result_base_dir') from exc

    parent_ledger = payload.get('parent_ledger') or {}
    if not isinstance(parent_ledger, Mapping):
        raise ScopeCycleError('parent_ledger must be an object')
    ledger_parent = parent_ledger.get('parent_cycle_id')
    if ledger_parent and str(ledger_parent) != parent_cycle_id:
        raise ScopeCycleError('parent ledger cycle identity mismatch')
    parent_ledger_path = _absolute_path(
        args.parent_ledger_path or parent_ledger.get('path'),
        'parent_ledger_path',
    )
    competition_payload = competition.as_dict()
    competition_payload['registry_snapshot_id'] = registry_snapshot_id
    return ScopeIdentity(
        parent_cycle_id=parent_cycle_id,
        child_cycle_id=child_cycle_id,
        competition_id=competition_id,
        edition_id=edition_id,
        canonical_competition_id=canonical_competition_id,
        canonical_season=canonical,
        registry_snapshot_id=registry_snapshot_id,
        capture_revision=_required(
            payload.get('capture_revision')
            or payload.get('selection_hash')
            or f'native-v2:{parent_cycle_id}',
            'capture_revision',
        ),
        scope_id=scope_id,
        result_base_dir=result_base_dir,
        entity_dir=entity_dir,
        scope_manifest_path=manifest_path,
        parent_ledger_path=parent_ledger_path,
        competition_record=competition_payload,
        edition_current=edition_current,
        edition_participant_count=edition_participant_count,
    )


def approved_operation_argv(
    argv: Sequence[str],
    *,
    executable: str | None = None,
    script_path: str | None = None,
) -> tuple[str, ...]:
    """Return the exact action argv without self-referential approval refs."""

    # `python` resolves to a different alias depending on PATH (the DAG's env
    # finds /usr/local/bin/python, an interactive shell finds the one in
    # ~/.local/bin), and both are symlinks to the same interpreter. The approved
    # operation is the interpreter itself, not the name it was invoked by.
    result = [
        str(Path(executable or sys.executable).resolve()),
        script_path or str(Path(__file__).resolve()),
    ]
    values = list(argv)
    index = 0
    while index < len(values):
        value = values[index]
        flag = value.split('=', 1)[0]
        if flag in _APPROVAL_FLAGS:
            if '=' not in value:
                if index + 1 >= len(values):
                    raise ScopeCycleError(f'{flag} is missing its value')
                index += 2
            else:
                index += 1
            continue
        result.append(value)
        index += 1
    return tuple(result)


def _parse_ref_pair(value: str, name: str) -> tuple[str, str]:
    raw = _required(value, name)
    if raw.startswith(('{', '[')):
        decoded = json.loads(raw)
        if isinstance(decoded, Mapping):
            return (
                _required(decoded.get('paid_proxy'), f'{name}.paid_proxy'),
                _required(
                    decoded.get('production_write'),
                    f'{name}.production_write',
                ),
            )
        if isinstance(decoded, list) and len(decoded) == 2:
            return _required(decoded[0], name), _required(decoded[1], name)
        raise ScopeCycleError(f'{name} JSON must contain two approval refs')
    parts = tuple(item.strip() for item in raw.split(',') if item.strip())
    if len(parts) != 2:
        raise ScopeCycleError(
            f'{name} must contain paid_proxy,production_write refs'
        )
    return parts


def _approval_refs(args: argparse.Namespace) -> Mapping[str, ApprovalRef]:
    bundle: Mapping[str, Any] = {}
    if args.approval_bundle_json:
        loaded = _load_json_value(
            args.approval_bundle_json, name='approval_bundle_json',
        )
        if not isinstance(loaded, Mapping):
            raise ScopeCycleError('approval_bundle_json must contain an object')
        bundle = loaded

    paid_id = args.paid_proxy_approval_packet_id
    paid_hash = args.paid_proxy_approval_packet_hash
    write_id = args.production_write_approval_packet_id
    write_hash = args.production_write_approval_packet_hash
    if bundle:
        paid = bundle.get('paid_proxy') or {}
        write = bundle.get('production_write') or {}
        if not isinstance(paid, Mapping) or not isinstance(write, Mapping):
            raise ScopeCycleError('approval bundle actions must be objects')
        paid_id = paid_id or paid.get('packet_id')
        paid_hash = paid_hash or paid.get('packet_hash')
        write_id = write_id or write.get('packet_id')
        write_hash = write_hash or write.get('packet_hash')
    if args.approval_packet_id or args.approval_packet_hash:
        if not args.approval_packet_id or not args.approval_packet_hash:
            raise ScopeCycleError('approval packet id/hash aliases require both')
        ids = _parse_ref_pair(args.approval_packet_id, 'approval_packet_id')
        hashes = _parse_ref_pair(
            args.approval_packet_hash, 'approval_packet_hash',
        )
        paid_id, write_id = paid_id or ids[0], write_id or ids[1]
        paid_hash, write_hash = paid_hash or hashes[0], write_hash or hashes[1]
    return {
        'paid_proxy': ApprovalRef(
            _required(paid_id, 'paid proxy approval packet id'),
            _required(paid_hash, 'paid proxy approval packet hash'),
        ),
        'production_write': ApprovalRef(
            _required(write_id, 'production write approval packet id'),
            _required(write_hash, 'production write approval packet hash'),
        ),
    }


def _packet_from_record(
    journal: ApprovalJournal,
    ref: ApprovalRef,
    *,
    action: str,
) -> ApprovalPacket:
    record = journal.get(ref.packet_hash)
    if record.packet_id != ref.packet_id or record.action != action:
        raise ApprovalDriftError(f'{action} approval identity/action mismatch')
    try:
        payload = json.loads(record.canonical_json)
        packet = ApprovalPacket(**payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ApprovalStateError(f'{action} approval packet is unreadable') from exc
    if packet.packet_hash != ref.packet_hash:
        raise ApprovalDriftError(f'{action} approval journal hash drift')
    return packet


def _consume_approvals(
    args: argparse.Namespace,
    *,
    operation_argv: Sequence[str],
    entity_limits: Mapping[str, Mapping[str, int]],
) -> Mapping[str, ApprovalPacket]:
    refs = _approval_refs(args)
    journal = ApprovalJournal(_required(args.approval_journal, 'approval_journal'))
    packets = {
        action: _packet_from_record(journal, refs[action], action=action)
        for action in ('paid_proxy', 'production_write')
    }
    paid = packets['paid_proxy']
    write = packets['production_write']
    expected_requests = int(args.request_limit)
    if sum(item['requests'] for item in entity_limits.values()) > expected_requests:
        raise ScopeCycleError('entity request budgets exceed the cycle request limit')
    if (
        paid.byte_cap_bytes != int(args.cycle_budget_bytes)
        or paid.request_limit != expected_requests
        or paid.retry_limit != int(args.retry_limit)
        or paid.concurrency != 1
    ):
        raise ApprovalDriftError('paid_proxy approval limits differ from wrapper')
    if (
        write.byte_cap_bytes != 0
        or write.request_limit != 0
        or write.concurrency != 1
    ):
        raise ApprovalDriftError('production_write approval limits are unsafe')
    required_tables = {
        value.split('.')[-1] for value in required_write_tables(args.write_mode)
    }
    for action, packet in packets.items():
        approved_tables = {
            str(value).split('.')[-1] for value in packet.affected_tables
        }
        if not required_tables.issubset(approved_tables):
            missing = sorted(required_tables - approved_tables)
            raise ApprovalDriftError(
                f'{action} approval omits tables: {missing}'
            )
    for action, packet in packets.items():
        if tuple(packet.argv) != tuple(operation_argv):
            raise ApprovalDriftError(
                f'{action} approval argv differs from wrapper operation'
            )
        record = journal.get(packet.packet_hash)
        if record.status != 'approved':
            raise ApprovalStateError(
                f'{action} packet is not approved: {record.status}'
            )
    for action in ('paid_proxy', 'production_write'):
        packet = packets[action]
        journal.consume(
            packet,
            presented_hash=packet.packet_hash,
            execution_argv=operation_argv,
        )
    return packets


def _entity_limits(args: argparse.Namespace) -> Mapping[str, Mapping[str, int]]:
    limits = {
        name: dict(value) for name, value in DEFAULT_ENTITY_LIMITS.items()
    }
    if args.entity_limits_json:
        loaded = _load_json_value(
            args.entity_limits_json, name='entity_limits_json',
        )
        if not isinstance(loaded, Mapping) or set(loaded) != set(ENTITY_ORDER):
            raise ScopeCycleError(
                'entity_limits_json must define exactly the four parser entities'
            )
        for name, value in loaded.items():
            if not isinstance(value, Mapping):
                raise ScopeCycleError(f'{name} entity limits must be an object')
            limits[name] = {
                'decoded_bytes': int(value.get('decoded_bytes', 0)),
                'requests': int(value.get('requests', 0)),
            }
    for name, value in limits.items():
        maximum = DEFAULT_ENTITY_LIMITS[name]
        if not 0 < value['decoded_bytes'] <= maximum['decoded_bytes']:
            raise ScopeCycleError(f'{name} decoded byte budget is out of bounds')
        if not 0 < value['requests'] <= maximum['requests']:
            raise ScopeCycleError(f'{name} request budget is out of bounds')
    return limits


def _runner_argv(
    identity: ScopeIdentity,
    args: argparse.Namespace,
    parser_entity: str,
    temporary_output: str,
    limits: Mapping[str, int],
    retry_budget: int,
) -> tuple[str, ...]:
    runner = str(Path(__file__).with_name('run_transfermarkt_scraper.py'))
    command = [
        sys.executable,
        runner,
        '--entity',
        parser_entity,
        '--competition-id',
        identity.competition_id,
        '--edition-id',
        identity.edition_id,
        '--output',
        temporary_output,
        '--run-key',
        identity.child_cycle_id,
        '--cycle-ledger-key',
        identity.parent_cycle_id,
        '--cycle-budget-bytes',
        str(args.cycle_budget_bytes),
        '--decoded-body-budget-mb',
        format(Decimal(limits['decoded_bytes']) / Decimal(MIB), 'f'),
        '--request-budget',
        str(limits['requests']),
        '--retry-budget',
        str(retry_budget),
        '--expected-reader-revision',
        str(args.reader_revision),
        '--write-mode',
        args.write_mode,
        '--refresh-mode',
        args.refresh_mode,
    ]
    if parser_entity in {'market_value_history', 'transfers'}:
        command.extend(['--limit', str(args.career_window_limit)])
    if parser_entity == 'coaches':
        command.extend(
            ['--coach-history-ttl-days', str(args.coach_history_ttl_days)]
        )
    return tuple(command)


def _runner_environment(
    identity: ScopeIdentity,
    args: argparse.Namespace,
    packets: Mapping[str, ApprovalPacket],
    parser_entity: str,
    retry_budget: int,
) -> Mapping[str, str]:
    env = dict(os.environ)
    approval_context = {
        action: {
            'packet_id': packet.packet_id,
            'packet_hash': packet.packet_hash,
        }
        for action, packet in packets.items()
    }
    env.update({
        'TM_REQUIRE_METERED_PROXY': 'true',
        'TM_SCOPE_DQ_REQUIRED': 'true',
        'TM_EDITION_CURRENT': str(identity.edition_current).lower(),
        'TM_RUN_ID': identity.parent_cycle_id,
        'TM_PARENT_CYCLE_ID': identity.parent_cycle_id,
        'TM_CHILD_CYCLE_ID': identity.child_cycle_id,
        'TM_SCOPE_ID': identity.scope_id,
        'TM_COMPETITION_ID': identity.competition_id,
        'TM_EDITION_ID': identity.edition_id,
        'TM_CANONICAL_SEASON': identity.canonical_season,
        'TM_TASK_ID': f'transfermarkt_scope_cycle.{parser_entity}',
        'TM_CANDIDATE_SLOT': args.candidate_slot,
        'TM_READER_REVISION': str(args.reader_revision),
        'TM_PROXY_LEASE_TTL_SECONDS': str(args.lease_ttl_seconds),
        'TM_RETRY_BUDGET': str(retry_budget),
        'TM_PENDING_CHECKPOINT_TTL_DAYS': str(args.checkpoint_ttl_days),
        'TM_PENDING_CHECKPOINT_DIR': str(
            Path(identity.result_base_dir) / 'checkpoints'
        ),
        # A league can be larger than one cycle's byte cap, so the pages already
        # paid for must outlive the cycle that fetched them. The path is keyed by
        # scope, not by cycle, which is what lets the next cycle finish the job.
        'TM_RESPONSE_CACHE_PATH': str(
            RESPONSE_CACHE_ROOT / f'{identity.scope_id}.json'
        ),
        'TM_RESPONSE_CACHE_TTL_SECONDS': str(RESPONSE_CACHE_TTL_SECONDS),
        'TM_CYCLE_BUDGET_DIR': str(Path(identity.parent_ledger_path).parent),
        'TM_COMPETITION_RECORDS_JSON': _stable_json([
            identity.competition_record,
        ]),
        'TM_APPROVAL_CONTEXT': _stable_json(approval_context),
        'TM_PAID_PROXY_APPROVAL_PACKET_ID': packets['paid_proxy'].packet_id,
        'TM_PAID_PROXY_APPROVAL_PACKET_HASH': packets['paid_proxy'].packet_hash,
        'TM_PRODUCTION_WRITE_APPROVAL_PACKET_ID': (
            packets['production_write'].packet_id
        ),
        'TM_PRODUCTION_WRITE_APPROVAL_PACKET_HASH': (
            packets['production_write'].packet_hash
        ),
    })
    if identity.edition_participant_count is not None:
        env['TM_EXPECTED_PARTICIPANT_COUNT'] = str(
            identity.edition_participant_count
        )
    else:
        env.pop('TM_EXPECTED_PARTICIPANT_COUNT', None)
    return env


def _atomic_json(path: Path, payload: Mapping[str, Any], *, immutable: bool) -> None:
    encoded = (_stable_json(payload) + '\n').encode('utf-8')
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if existing == encoded:
            return
        if immutable:
            raise ScopeCycleError(f'immutable file conflicts with evidence: {path}')
    descriptor, temporary = tempfile.mkstemp(
        prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _load_json_file(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text('utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScopeCycleError(f'JSON evidence is unreadable: {path}') from exc
    if not isinstance(value, Mapping):
        raise ScopeCycleError(f'JSON evidence is not an object: {path}')
    return value


def _validate_result_identity(
    result: Mapping[str, Any],
    identity: ScopeIdentity,
    parser_entity: str,
) -> None:
    expected = {
        'entity': parser_entity,
        'run_key': identity.child_cycle_id,
        'cycle_ledger_key': identity.parent_cycle_id,
        'competition_id': identity.competition_id,
        'edition_id': identity.edition_id,
        'canonical_season': identity.canonical_season,
        'registry_snapshot_id': identity.registry_snapshot_id,
        'scope_id': identity.scope_id,
    }
    for field, value in expected.items():
        if str(result.get(field) or '') != str(value):
            raise ScopeCycleError(
                f'{parser_entity} result {field} mismatch: '
                f'{result.get(field)!r} != {value!r}'
            )
    if result.get('errors'):
        raise ScopeCycleError(f'{parser_entity} result contains errors')
    if result.get('fallback'):
        raise ScopeCycleError(f'{parser_entity} used fallback')
    if not result.get('native_write_complete'):
        raise ScopeCycleError(f'{parser_entity} native write is incomplete')


def _traffic_metrics(
    run: EntityRun,
    *,
    hard_cap: int,
    soft_stop: int,
) -> Mapping[str, int]:
    result = run.result
    traffic = result.get('traffic') or {}
    if not isinstance(traffic, Mapping) or not traffic.get('telemetry_available'):
        raise ScopeCycleError(f'{run.parser_entity} traffic telemetry is absent')
    if not result.get('provider_metering_available'):
        raise ScopeCycleError(
            f'{run.parser_entity} provider metering is not authoritative'
        )
    if int(traffic.get('hard_provider_byte_budget', -1)) != hard_cap:
        raise ScopeCycleError(f'{run.parser_entity} hard provider cap drift')
    if int(traffic.get('soft_provider_byte_stop', -1)) != soft_stop:
        raise ScopeCycleError(f'{run.parser_entity} soft provider stop drift')
    fields = {
        'decoded_bytes': result.get('decoded_response_body_bytes'),
        'wire_bytes': result.get('wire_response_bytes'),
        'provider_metered_bytes': result.get('provider_metered_bytes'),
        'requests': result.get('network_fetches'),
        'retries': result.get('retries'),
        'cache_hits': result.get('cache_hits'),
    }
    metrics: dict[str, int] = {}
    for field, value in fields.items():
        if value is None:
            raise ScopeCycleError(
                f'{run.parser_entity} exact {field} telemetry is absent'
            )
        metrics[field] = int(value)
        if metrics[field] < 0:
            raise ScopeCycleError(f'{run.parser_entity} has negative {field}')
    metrics['duration_ms'] = int(run.wall_clock_duration_ms)
    return metrics


def _manifest_rows(result: Mapping[str, Any], write_mode: str) -> Mapping[str, Any]:
    key = 'batch_manifest' if write_mode == 'dual' else 'native_write_manifest'
    complete_key = (
        'dual_write_complete'
        if write_mode == 'dual'
        else 'native_write_manifest_complete'
    )
    manifest = result.get(key)
    if not result.get(complete_key) or not isinstance(manifest, Mapping):
        raise ScopeCycleError(f'{result.get("entity")} write manifest is incomplete')
    if manifest.get('status') != 'success':
        raise ScopeCycleError(f'{result.get("entity")} write manifest is red')
    rows = manifest.get('rows') or ()
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ScopeCycleError('entity write manifest rows are invalid')
    indexed = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ScopeCycleError('entity write manifest row is invalid')
        entity = _required(row.get('entity'), 'write manifest entity')
        if entity in indexed:
            raise ScopeCycleError(f'duplicate write manifest entity: {entity}')
        indexed[entity] = row
    return indexed


def _validate_participant_capture(
    identity: ScopeIdentity,
    capture: Any,
    entity_statuses: Mapping[str, str],
) -> Mapping[str, Any]:
    """Validate source listing/squad evidence against the planned registry."""

    if not isinstance(capture, Mapping):
        raise ScopeCycleError('players scope_capture evidence is missing')
    classification = {
        'competition_id': identity.competition_id,
        'edition_id': identity.edition_id,
        'scope_id': identity.scope_id,
        'competition_type': identity.competition_record['competition_type'],
        'team_type': identity.competition_record['team_type'],
        'gender': 'men',
        'age_category': 'senior',
    }
    for field, expected in classification.items():
        if str(capture.get(field, '')).strip() != str(expected):
            raise ScopeCycleError(
                f'players scope_capture {field} differs from registry'
            )
    expected_teams = capture.get('expected_team_ids')
    if (
        identity.edition_participant_count is not None
        and isinstance(expected_teams, Sequence)
        and not isinstance(expected_teams, (str, bytes))
        and len(expected_teams) != identity.edition_participant_count
    ):
        raise ScopeCycleError(
            'players listing participant count differs from edition registry: '
            f'{len(expected_teams)}/{identity.edition_participant_count}'
        )
    try:
        return validate_scope_capture(
            input_from_capture(
                capture,
                entity_statuses=entity_statuses,
                current=identity.edition_current,
            ),
            expected_entities=entity_statuses,
        )
    except ScopeDQError as exc:
        raise ScopeCycleError(f'scope participant DQ failed: {exc}') from exc


def _validate_run_contract(
    run: EntityRun,
    identity: ScopeIdentity,
    args: argparse.Namespace,
) -> None:
    """Validate a child result completely before its checkpoint can advance."""

    _validate_result_identity(run.result, identity, run.parser_entity)
    _traffic_metrics(
        run,
        hard_cap=int(args.cycle_budget_bytes),
        soft_stop=int(args.soft_byte_stop_bytes),
    )
    manifest_rows = _manifest_rows(run.result, args.write_mode)
    outputs = run.result.get('outputs') or {}
    if not isinstance(outputs, Mapping):
        raise ScopeCycleError(f'{run.parser_entity} outputs are invalid')
    entity_statuses: dict[str, str] = {}
    for output_key, entity in ENTITY_OUTPUTS[run.parser_entity]:
        output = outputs.get(output_key)
        if not isinstance(output, Mapping):
            raise ScopeCycleError(f'{entity} output is missing')
        rows = int(output.get('rows', -1))
        applicability = output.get('applicability_status')
        if rows > 0:
            applicability = 'ok'
        if rows < 0 or applicability not in {
            'ok', 'authoritative_empty', 'not_applicable',
        }:
            raise ScopeCycleError(f'{entity} lacks verified terminal evidence')
        if applicability == 'ok':
            row = manifest_rows.get(entity)
            if not isinstance(row, Mapping) or row.get('status') != 'success':
                raise ScopeCycleError(f'{entity} write evidence is missing')
            if str(output.get('table') or '') != ENTITY_TABLES[entity]:
                raise ScopeCycleError(f'{entity} committed table is wrong')
        elif rows != 0:
            raise ScopeCycleError(f'{entity} terminal empty contains rows')
        entity_statuses[entity] = str(applicability)
    if run.parser_entity == 'players':
        _validate_participant_capture(
            identity, run.result.get('scope_capture'), entity_statuses,
        )


def _build_scope_manifest(
    identity: ScopeIdentity,
    args: argparse.Namespace,
    runs: Sequence[EntityRun],
) -> Mapping[str, Any]:
    if tuple(run.parser_entity for run in runs) != ENTITY_ORDER:
        raise ScopeCycleError('scope cycle has missing or out-of-order parser entities')
    evidence: list[EntityEvidence] = []
    parser_runs: dict[str, Any] = {}
    total_metrics = {
        field: 0 for field in (
            'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
            'requests', 'retries', 'cache_hits', 'duration_ms',
        )
    }
    empty_hash = stable_hash([])
    for run in runs:
        _validate_result_identity(run.result, identity, run.parser_entity)
        metrics = _traffic_metrics(
            run,
            hard_cap=int(args.cycle_budget_bytes),
            soft_stop=int(args.soft_byte_stop_bytes),
        )
        for field, value in metrics.items():
            total_metrics[field] += int(value)
        manifest_rows = _manifest_rows(run.result, args.write_mode)
        outputs = run.result.get('outputs') or {}
        if not isinstance(outputs, Mapping):
            raise ScopeCycleError(f'{run.parser_entity} outputs are invalid')
        parser_runs[run.parser_entity] = {
            'result_path': run.result_path,
            'result_sha256': run.result_sha256,
            'resumed': run.resumed,
            **metrics,
        }
        traffic_owner = ENTITY_OUTPUTS[run.parser_entity][0][1]
        for output_key, entity in ENTITY_OUTPUTS[run.parser_entity]:
            output = outputs.get(output_key)
            if not isinstance(output, Mapping):
                raise ScopeCycleError(f'{entity} output is missing')
            raw_rows = int(output.get('rows', -1))
            if raw_rows < 0:
                raise ScopeCycleError(f'{entity} row count is invalid')
            applicability = output.get('applicability_status')
            if raw_rows > 0:
                applicability = 'ok'
            if applicability not in {'ok', 'authoritative_empty', 'not_applicable'}:
                raise ScopeCycleError(
                    f'{entity} empty output lacks explicit terminal evidence'
                )
            if applicability != 'ok' and raw_rows != 0:
                raise ScopeCycleError(f'{entity} terminal empty contains rows')
            row = manifest_rows.get(entity)
            if applicability == 'ok':
                if not isinstance(row, Mapping) or row.get('status') != 'success':
                    raise ScopeCycleError(f'{entity} write evidence is missing')
                dedup_rows = int(row.get('native_rows', -1))
                key_hash = _required(row.get('native_hash'), f'{entity} hash')
                expected_table = ENTITY_TABLES[entity]
                if str(output.get('table') or '') != expected_table:
                    raise ScopeCycleError(
                        f'{entity} committed table differs from {expected_table}'
                    )
            else:
                dedup_rows = 0
                key_hash = empty_hash
            if dedup_rows < 0 or dedup_rows > raw_rows:
                raise ScopeCycleError(f'{entity} dedup row count is invalid')
            owned = metrics if entity == traffic_owner else {
                field: 0 for field in metrics
            }
            evidence.append(EntityEvidence(
                entity=entity,
                applicability_status=str(applicability),
                expected_rows=dedup_rows,
                raw_rows=raw_rows,
                dedup_rows=dedup_rows,
                key_hash=key_hash,
                content_hash=key_hash,
                dq_status='passed',
                **owned,
            ))

    if total_metrics['provider_metered_bytes'] > int(args.cycle_budget_bytes):
        raise ScopeCycleError('scope provider traffic exceeds the hard byte cap')
    if total_metrics['requests'] > int(args.request_limit):
        raise ScopeCycleError('scope requests exceed the approved limit')
    if total_metrics['retries'] > int(args.retry_limit):
        raise ScopeCycleError('scope retries exceed the approved limit')
    participant_dq = _validate_participant_capture(
        identity,
        runs[0].result.get('scope_capture'),
        {item.entity: item.applicability_status for item in evidence},
    )
    scope_capture = runs[0].result.get('scope_capture')
    if not isinstance(scope_capture, Mapping):
        raise ScopeCycleError('players scope_capture evidence is missing')
    entity_statuses = {
        item.entity: item.applicability_status for item in evidence
    }
    entity_contracts = entity_applicability_contracts(
        entities=entity_statuses,
        competition_type=str(identity.competition_record['competition_type']),
        team_type=str(identity.competition_record['team_type']),
    )
    authoritative_empty_evidence: dict[str, dict[str, str]] = {}
    for run in runs:
        outputs = run.result.get('outputs') or {}
        for output_key, entity in ENTITY_OUTPUTS[run.parser_entity]:
            output = outputs.get(output_key)
            if not isinstance(output, Mapping):
                raise ScopeCycleError(f'{entity} output is missing')
            rows = int(output.get('rows', -1))
            status = 'ok' if rows > 0 else str(
                output.get('applicability_status') or ''
            )
            if status != 'authoritative_empty':
                continue
            contract = entity_contracts[entity]
            if not contract['requires_authoritative_empty_evidence']:
                raise ScopeCycleError(
                    f'{entity}: authoritative empty violates applicability contract'
                )
            if (
                run.result.get('authoritative_empty') is True
                and run.result.get('valid_empty') is True
            ):
                kind = 'typed_fetch_state'
            elif (
                run.result.get('cache_only_materialization') is True
                and run.result.get('checkpoint_status') == 'cache_complete'
            ):
                kind = 'cache_complete'
            else:
                raise ScopeCycleError(
                    f'{entity}: authoritative empty lacks typed proof'
                )
            if not isinstance(run.result_sha256, str) or len(run.result_sha256) != 64:
                raise ScopeCycleError(f'{entity}: result evidence hash is invalid')
            authoritative_empty_evidence[entity] = {
                'kind': kind,
                'result_sha256': run.result_sha256,
            }
    dq_evidence = {
        'status': 'passed',
        'registry_participant_count': identity.edition_participant_count,
        'edition_current': bool(identity.edition_current),
        'scope_capture': dict(scope_capture),
        'entity_statuses': entity_statuses,
        'entity_contracts': entity_contracts,
        'authoritative_empty_evidence': authoritative_empty_evidence,
        'participant_contract': dict(participant_dq),
    }
    scope = ScopeManifest(
        parent_cycle_id=identity.parent_cycle_id,
        child_cycle_id=identity.child_cycle_id,
        scope_id=identity.scope_id,
        competition_id=identity.competition_id,
        edition_id=identity.edition_id,
        canonical_competition_id=identity.canonical_competition_id,
        canonical_season=identity.canonical_season,
        registry_snapshot_id=identity.registry_snapshot_id,
        capture_revision=identity.capture_revision,
        parser_revision=os.environ.get('TM_PARSER_VERSION', 'v2'),
        schema_revision=os.environ.get('TM_SCHEMA_VERSION', '2'),
        reader_revision=int(args.reader_revision),
        entities=tuple(evidence),
        dq_evidence=dq_evidence,
    )
    scope.validate(EXPECTED_ENTITIES)
    payload = asdict(scope)
    payload.update({
        'manifest_digest': scope.digest,
        'status': 'complete',
        'dq': {
            'status': 'passed',
            'expected_entities': list(EXPECTED_ENTITIES),
            'missing_entities': [],
            'participant_contract': dict(participant_dq),
            'silver_trigger_allowed': True,
        },
        'candidate_slot': args.candidate_slot,
        'write_mode': args.write_mode,
        'traffic': {
            'hard_provider_byte_budget': int(args.cycle_budget_bytes),
            'soft_provider_byte_stop': int(args.soft_byte_stop_bytes),
            'totals': total_metrics,
            'by_parser_entity': parser_runs,
            'decoded_mib': total_metrics['decoded_bytes'] / MIB,
            'wire_mib': total_metrics['wire_bytes'] / MIB,
            'provider_mib': total_metrics['provider_metered_bytes'] / MIB,
            'cache_hit_rate': (
                total_metrics['cache_hits']
                / (total_metrics['requests'] + total_metrics['cache_hits'])
                if total_metrics['requests'] + total_metrics['cache_hits']
                else 0.0
            ),
        },
    })
    return json.loads(_stable_json(payload))


def scope_manifest_merge_sql(manifest: Mapping[str, Any]) -> str:
    """Return the idempotent exact-scope ops MERGE used after local commit."""

    if manifest.get('status') != 'complete':
        raise ScopeCycleError('only a complete scope manifest may be persisted')
    if manifest.get('manifest_digest') != stable_hash({
        key: manifest[key]
        for key in ScopeManifest.__dataclass_fields__
    }):
        raise ScopeCycleError('scope manifest digest does not match its payload')

    def quoted(value: Any) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    entity_json = _stable_json({
        'entities': manifest['entities'],
        'dq_evidence': manifest['dq_evidence'],
    })
    return f"""MERGE INTO {SCOPE_MANIFEST_TABLE} t
USING (VALUES (
    {quoted(manifest['parent_cycle_id'])},
    {quoted(manifest['child_cycle_id'])},
    {quoted(manifest['scope_id'])},
    {quoted(manifest['competition_id'])},
    {quoted(manifest['edition_id'])},
    {quoted(manifest['canonical_competition_id'])},
    {quoted(manifest['canonical_season'])},
    {quoted(manifest['registry_snapshot_id'])},
    {quoted(manifest['capture_revision'])},
    {quoted(manifest['parser_revision'])},
    {quoted(manifest['schema_revision'])},
    {int(manifest['reader_revision'])},
    {quoted(entity_json)},
    {quoted(manifest['manifest_digest'])},
    {quoted(SCOPE_COMPLETION_STATUS)}
)) s(parent_cycle_id, child_cycle_id, scope_id, competition_id, edition_id,
     canonical_competition_id, canonical_season, registry_snapshot_id,
     capture_revision, parser_revision, schema_revision, reader_revision,
     entity_manifest_json, manifest_digest, status)
ON t.parent_cycle_id = s.parent_cycle_id
   AND t.child_cycle_id = s.child_cycle_id
   AND t.scope_id = s.scope_id
   AND t.manifest_digest = s.manifest_digest
WHEN MATCHED THEN UPDATE SET
    entity_manifest_json = s.entity_manifest_json,
    status = s.status
WHEN NOT MATCHED THEN INSERT (
    parent_cycle_id, child_cycle_id, scope_id, competition_id, edition_id,
    canonical_competition_id, canonical_season, registry_snapshot_id,
    capture_revision, parser_revision, schema_revision, reader_revision,
    entity_manifest_json, manifest_digest, status, committed_at
) VALUES (
    s.parent_cycle_id, s.child_cycle_id, s.scope_id, s.competition_id,
    s.edition_id, s.canonical_competition_id, s.canonical_season,
    s.registry_snapshot_id, s.capture_revision, s.parser_revision,
    s.schema_revision, s.reader_revision, s.entity_manifest_json,
    s.manifest_digest, s.status, CURRENT_TIMESTAMP
)"""


def persist_scope_manifest(
    manifest: Mapping[str, Any],
    *,
    connection_factory: Callable[[], Any] | None = None,
) -> str:
    """Persist one complete manifest; connection creation remains injectable."""

    sql = scope_manifest_merge_sql(manifest)
    if connection_factory is None:
        import trino
        from trino.auth import BasicAuthentication

        def connection_factory():
            user = os.environ.get('TRINO_USER', 'airflow')
            password = os.environ.get('TRINO_PASSWORD')
            options: dict[str, Any] = {
                'host': os.environ.get('TRINO_HOST', 'trino'),
                'port': int(os.environ.get('TRINO_PORT', '8443')),
                'user': user,
                'catalog': 'iceberg',
                'schema': 'ops',
                'http_scheme': 'https',
                'verify': False,
            }
            if password:
                options['auth'] = BasicAuthentication(user, password)
            return trino.dbapi.connect(**options)

    connection = connection_factory()
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        if cursor.description:
            cursor.fetchall()
    finally:
        try:
            cursor.close()
        finally:
            connection.close()
    return sql


def proxy_ledger_merge_sql(parent_ledger: Mapping[str, Any]) -> str:
    """Build one cumulative seven-row parent-cycle proxy ledger MERGE."""

    if int(parent_ledger.get('hard_provider_byte_budget', -1)) != HARD_BYTE_CAP:
        raise ScopeCycleError('parent proxy ledger hard cap is not production cap')
    if int(parent_ledger.get('soft_provider_byte_stop', -1)) != SOFT_BYTE_STOP:
        raise ScopeCycleError('parent proxy ledger soft stop is not production stop')
    parent_cycle_id = _required(
        parent_ledger.get('parent_cycle_id'), 'parent ledger cycle id',
    )
    by_entity = parent_ledger.get('by_entity') or {}
    if not isinstance(by_entity, Mapping) or set(by_entity) != set(EXPECTED_ENTITIES):
        raise ScopeCycleError('parent proxy ledger must contain seven entities')
    fields = (
        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes', 'requests',
        'retries', 'cache_hits', 'duration_ms',
    )
    rows = []
    for entity in EXPECTED_ENTITIES:
        item = by_entity[entity]
        if not isinstance(item, Mapping):
            raise ScopeCycleError(f'parent proxy ledger {entity} row is invalid')
        values = [int(item.get(field, -1)) for field in fields]
        if any(value < 0 for value in values):
            raise ScopeCycleError(f'parent proxy ledger {entity} has a negative metric')
        rows.append((entity, values))
    for index, field in enumerate(fields):
        if sum(values[index] for _, values in rows) != int(
            parent_ledger.get(field, -1)
        ):
            raise ScopeCycleError(
                f'parent proxy ledger total does not reconcile for {field}'
            )

    def quoted(value: Any) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    value_sql = ',\n'.join(
        '    ('
        + ', '.join([
            quoted(parent_cycle_id),
            quoted(entity),
            *(str(value) for value in values),
            str(HARD_BYTE_CAP),
            str(SOFT_BYTE_STOP),
        ])
        + ')'
        for entity, values in rows
    )
    return f"""MERGE INTO {PROXY_LEDGER_TABLE} t
USING (VALUES
{value_sql}
) s(parent_cycle_id, entity, decoded_bytes, wire_bytes,
     provider_metered_bytes, requests, retries, cache_hits, duration_ms,
     hard_limit_bytes, soft_limit_bytes)
ON t.parent_cycle_id = s.parent_cycle_id AND t.entity = s.entity
WHEN MATCHED THEN UPDATE SET
    decoded_bytes = s.decoded_bytes,
    wire_bytes = s.wire_bytes,
    provider_metered_bytes = s.provider_metered_bytes,
    requests = s.requests,
    retries = s.retries,
    cache_hits = s.cache_hits,
    duration_ms = s.duration_ms,
    hard_limit_bytes = s.hard_limit_bytes,
    soft_limit_bytes = s.soft_limit_bytes,
    updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    parent_cycle_id, entity, decoded_bytes, wire_bytes,
    provider_metered_bytes, requests, retries, cache_hits, duration_ms,
    hard_limit_bytes, soft_limit_bytes, updated_at
) VALUES (
    s.parent_cycle_id, s.entity, s.decoded_bytes, s.wire_bytes,
    s.provider_metered_bytes, s.requests, s.retries, s.cache_hits,
    s.duration_ms, s.hard_limit_bytes, s.soft_limit_bytes, CURRENT_TIMESTAMP
)"""


def persist_parent_proxy_ledger(
    parent_ledger: Mapping[str, Any],
    *,
    connection_factory: Callable[[], Any] | None = None,
) -> str:
    """Persist cumulative parent traffic after its JSON ledger is reconciled."""

    sql = proxy_ledger_merge_sql(parent_ledger)
    if connection_factory is None:
        import trino
        from trino.auth import BasicAuthentication

        def connection_factory():
            user = os.environ.get('TRINO_USER', 'airflow')
            password = os.environ.get('TRINO_PASSWORD')
            options: dict[str, Any] = {
                'host': os.environ.get('TRINO_HOST', 'trino'),
                'port': int(os.environ.get('TRINO_PORT', '8443')),
                'user': user,
                'catalog': 'iceberg',
                'schema': 'ops',
                'http_scheme': 'https',
                'verify': False,
            }
            if password:
                options['auth'] = BasicAuthentication(user, password)
            return trino.dbapi.connect(**options)

    connection = connection_factory()
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        if cursor.description:
            cursor.fetchall()
    finally:
        try:
            cursor.close()
        finally:
            connection.close()
    return sql


def _update_parent_ledger(
    identity: ScopeIdentity,
    manifest: Mapping[str, Any],
    *,
    hard_cap: int,
    soft_stop: int,
    request_limit: int,
    retry_limit: int,
) -> Mapping[str, Any]:
    path = Path(identity.parent_ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f'.{path.name}.lock')
    with lock_path.open('a+', encoding='utf-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            if path.exists():
                current = _load_json_file(path)
            else:
                current = {
                    'parent_cycle_id': identity.parent_cycle_id,
                    'hard_provider_byte_budget': int(hard_cap),
                    'soft_provider_byte_stop': int(soft_stop),
                    'request_limit': int(request_limit),
                    'retry_limit': int(retry_limit),
                    'scopes': {},
                }
            if (
                current.get('parent_cycle_id') != identity.parent_cycle_id
                or int(current.get('hard_provider_byte_budget', -1)) != hard_cap
                or int(current.get('soft_provider_byte_stop', -1)) != soft_stop
                or int(current.get('request_limit', -1)) != request_limit
                or int(current.get('retry_limit', -1)) != retry_limit
            ):
                raise ScopeCycleError('parent proxy ledger identity/budget mismatch')
            scopes = dict(current.get('scopes') or {})
            existing = scopes.get(identity.scope_id)
            entity_fields = (
                'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
                'requests', 'retries', 'cache_hits', 'duration_ms',
            )
            manifest_entities = manifest.get('entities') or ()
            scope_by_entity = {
                str(item['entity']): {
                    field: int(item[field]) for field in entity_fields
                }
                for item in manifest_entities
            }
            if set(scope_by_entity) != set(EXPECTED_ENTITIES):
                raise ScopeCycleError('scope manifest entity traffic is incomplete')
            entry = {
                'child_cycle_id': identity.child_cycle_id,
                'manifest_digest': manifest['manifest_digest'],
                **dict(manifest['traffic']['totals']),
                'by_entity': scope_by_entity,
            }
            if existing is not None and existing != entry:
                raise ScopeCycleError('parent proxy ledger scope evidence drift')
            scopes[identity.scope_id] = entry
            fields = entity_fields
            totals = {
                field: sum(int(item[field]) for item in scopes.values())
                for field in fields
            }
            by_entity = {
                entity: {
                    field: sum(
                        int(item['by_entity'][entity][field])
                        for item in scopes.values()
                    )
                    for field in fields
                }
                for entity in EXPECTED_ENTITIES
            }
            for field in fields:
                if sum(item[field] for item in by_entity.values()) != totals[field]:
                    raise ScopeCycleError(
                        f'parent entity ledger does not reconcile for {field}'
                    )
            if totals['provider_metered_bytes'] > hard_cap:
                raise ScopeCycleError('parent provider byte budget exceeded')
            if totals['requests'] > request_limit:
                raise ScopeCycleError('parent request limit exceeded')
            if totals['retries'] > retry_limit:
                raise ScopeCycleError('parent retry limit exceeded')
            payload = {
                'parent_cycle_id': identity.parent_cycle_id,
                'hard_provider_byte_budget': hard_cap,
                'soft_provider_byte_stop': soft_stop,
                'request_limit': request_limit,
                'retry_limit': retry_limit,
                'manifest_count': len(scopes),
                **totals,
                'by_entity': by_entity,
                'scopes': dict(sorted(scopes.items())),
            }
            _atomic_json(path, payload, immutable=False)
            return payload
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _parent_committed_totals(
    identity: ScopeIdentity,
    *,
    hard_cap: int,
    soft_stop: int,
    request_limit: int,
    retry_limit: int,
) -> Mapping[str, int]:
    """Read the committed parent totals under the same ledger lock."""

    path = Path(identity.parent_ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f'.{path.name}.lock')
    with lock_path.open('a+', encoding='utf-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_SH)
        try:
            if not path.exists():
                return {'requests': 0, 'retries': 0}
            current = _load_json_file(path)
            if (
                current.get('parent_cycle_id') != identity.parent_cycle_id
                or int(current.get('hard_provider_byte_budget', -1)) != hard_cap
                or int(current.get('soft_provider_byte_stop', -1)) != soft_stop
                or int(current.get('request_limit', -1)) != request_limit
                or int(current.get('retry_limit', -1)) != retry_limit
            ):
                raise ScopeCycleError('parent proxy ledger identity/budget mismatch')
            return {
                'requests': int(current.get('requests', 0)),
                'retries': int(current.get('retries', 0)),
            }
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _attempt_guard_path(identity: ScopeIdentity) -> Path:
    path = Path(identity.parent_ledger_path)
    return path.with_name(f'{path.name}.attempts')


def _attempt_guard_totals(
    identity: ScopeIdentity,
    *,
    request_limit: int,
    retry_limit: int,
) -> Mapping[str, int]:
    path = _attempt_guard_path(identity)
    if not path.exists():
        return {'requests': 0, 'retries': 0}
    payload = _load_json_file(path)
    if (
        payload.get('parent_cycle_id') != identity.parent_cycle_id
        or int(payload.get('request_limit', -1)) != request_limit
        or int(payload.get('retry_limit', -1)) != retry_limit
    ):
        raise ScopeCycleError('parent attempt guard identity/budget mismatch')
    return {
        'requests': int(payload.get('requests', -1)),
        'retries': int(payload.get('retries', -1)),
    }


def _record_attempt_guard(
    identity: ScopeIdentity,
    *,
    attempt_id: str,
    requests: int,
    retries: int,
    request_limit: int,
    retry_limit: int,
) -> Mapping[str, int]:
    """Account every launched process, including red attempts, exactly once."""

    path = _attempt_guard_path(identity)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f'.{path.name}.lock')
    with lock_path.open('a+', encoding='utf-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            if path.exists():
                payload = dict(_load_json_file(path))
            else:
                payload = {
                    'parent_cycle_id': identity.parent_cycle_id,
                    'request_limit': int(request_limit),
                    'retry_limit': int(retry_limit),
                    'attempts': {},
                }
            if (
                payload.get('parent_cycle_id') != identity.parent_cycle_id
                or int(payload.get('request_limit', -1)) != request_limit
                or int(payload.get('retry_limit', -1)) != retry_limit
            ):
                raise ScopeCycleError('parent attempt guard identity/budget mismatch')
            attempts = dict(payload.get('attempts') or {})
            entry = {'requests': int(requests), 'retries': int(retries)}
            if min(entry.values()) < 0:
                raise ScopeCycleError('parent attempt guard has a negative metric')
            existing = attempts.get(attempt_id)
            if existing is not None and existing != entry:
                raise ScopeCycleError('parent attempt guard evidence drift')
            attempts[attempt_id] = entry
            totals = {
                field: sum(int(item[field]) for item in attempts.values())
                for field in ('requests', 'retries')
            }
            payload.update({**totals, 'attempts': dict(sorted(attempts.items()))})
            _atomic_json(path, payload, immutable=False)
            return totals
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _checkpoint_identity(
    identity: ScopeIdentity,
    args: argparse.Namespace,
    entity_limits: Mapping[str, Mapping[str, int]],
) -> str:
    return stable_hash({
        **asdict(identity),
        'reader_revision': int(args.reader_revision),
        'candidate_slot': args.candidate_slot,
        'write_mode': args.write_mode,
        'cycle_budget_bytes': int(args.cycle_budget_bytes),
        'soft_byte_stop_bytes': int(args.soft_byte_stop_bytes),
        'request_limit': int(args.request_limit),
        'retry_limit': int(args.retry_limit),
        'career_window_limit': int(args.career_window_limit),
        'coach_history_ttl_days': int(args.coach_history_ttl_days),
        'lease_ttl_seconds': int(args.lease_ttl_seconds),
        'entity_limits': entity_limits,
    })


def _resume_runs(
    identity: ScopeIdentity,
    args: argparse.Namespace,
    checkpoint: Mapping[str, Any],
    checkpoint_identity: str,
) -> list[EntityRun]:
    if checkpoint.get('identity_hash') != checkpoint_identity:
        return []
    entries = checkpoint.get('entities') or {}
    if not isinstance(entries, Mapping):
        raise ScopeCycleError('scope checkpoint entity map is invalid')
    runs: list[EntityRun] = []
    for parser_entity in ENTITY_ORDER:
        item = entries.get(parser_entity)
        if not isinstance(item, Mapping) or item.get('status') != 'success':
            break
        path = Path(_required(item.get('result_path'), 'checkpoint result_path'))
        expected_path = Path(identity.entity_dir) / f'{parser_entity}.json'
        if path != expected_path or not path.is_file():
            raise ScopeCycleError(f'{parser_entity} checkpoint result is missing')
        digest = _sha256_file(path)
        if digest != item.get('result_sha256'):
            raise ScopeCycleError(f'{parser_entity} checkpoint hash mismatch')
        result = _load_json_file(path)
        run = EntityRun(
            parser_entity=parser_entity,
            result_path=str(path),
            result_sha256=digest,
            result=result,
            wall_clock_duration_ms=int(item.get('wall_clock_duration_ms', -1)),
            resumed=True,
        )
        if run.wall_clock_duration_ms < 0:
            raise ScopeCycleError(f'{parser_entity} checkpoint duration is invalid')
        _validate_run_contract(run, identity, args)
        runs.append(run)
    return runs


def run_scope_cycle(
    args: argparse.Namespace,
    *,
    operation_argv: Sequence[str],
    subprocess_runner: Callable[..., Any] = subprocess.run,
    manifest_writer: Callable[[Mapping[str, Any]], Any] = persist_scope_manifest,
    parent_ledger_writer: Callable[[Mapping[str, Any]], Any] = (
        persist_parent_proxy_ledger
    ),
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
) -> Mapping[str, Any]:
    identity = _scope_identity(args)
    limits = _entity_limits(args)
    checkpoint_path = Path(identity.result_base_dir) / 'scope-cycle-checkpoint.json'
    checkpoint_identity = _checkpoint_identity(identity, args, limits)
    checkpoint: Mapping[str, Any] = {}
    if checkpoint_path.exists():
        checkpoint = _load_json_file(checkpoint_path)
    runs = _resume_runs(identity, args, checkpoint, checkpoint_identity)

    manifest_path = Path(identity.scope_manifest_path)
    if manifest_path.exists():
        manifest = _load_json_file(manifest_path)
        scope_manifest_merge_sql(manifest)
        if len(runs) != len(ENTITY_ORDER):
            raise ScopeCycleError('final manifest exists without exact checkpoints')
        checkpoint_complete = (
            checkpoint.get('status') == 'complete'
            and checkpoint.get('manifest_digest') == manifest['manifest_digest']
        )
        if not checkpoint_complete:
            _consume_approvals(
                args, operation_argv=operation_argv, entity_limits=limits,
            )
            manifest_writer(manifest)
        parent_ledger = _update_parent_ledger(
            identity,
            manifest,
            hard_cap=int(args.cycle_budget_bytes),
            soft_stop=int(args.soft_byte_stop_bytes),
            request_limit=int(args.request_limit),
            retry_limit=int(args.retry_limit),
        )
        if not checkpoint_complete:
            parent_ledger_writer(parent_ledger)
            _atomic_json(checkpoint_path, {
                'version': 1,
                'identity_hash': checkpoint_identity,
                'entities': dict(checkpoint.get('entities') or {}),
                'status': 'complete',
                'manifest_path': str(manifest_path),
                'manifest_digest': manifest['manifest_digest'],
                'parent_ledger_sha256': _sha256_bytes(
                    (_stable_json(parent_ledger) + '\n').encode('utf-8')
                ),
            }, immutable=False)
        return manifest

    packets: Mapping[str, ApprovalPacket] = {}
    if len(runs) < len(ENTITY_ORDER):
        committed_preflight = _parent_committed_totals(
            identity,
            hard_cap=int(args.cycle_budget_bytes),
            soft_stop=int(args.soft_byte_stop_bytes),
            request_limit=int(args.request_limit),
            retry_limit=int(args.retry_limit),
        )
        attempts_preflight = _attempt_guard_totals(
            identity,
            request_limit=int(args.request_limit),
            retry_limit=int(args.retry_limit),
        )
        if max(
            committed_preflight['requests'], attempts_preflight['requests'],
        ) >= int(args.request_limit):
            raise ScopeCycleError('parent request limit exhausted before approval')
        if max(
            committed_preflight['retries'], attempts_preflight['retries'],
        ) >= int(args.retry_limit):
            raise ScopeCycleError('parent retry limit exhausted before approval')
    # Even a fully resumed capture still has one production ops MERGE ahead.
    packets = _consume_approvals(
        args, operation_argv=operation_argv, entity_limits=limits,
    )
    entity_checkpoint = dict(checkpoint.get('entities') or {})
    parent_totals = _parent_committed_totals(
        identity,
        hard_cap=int(args.cycle_budget_bytes),
        soft_stop=int(args.soft_byte_stop_bytes),
        request_limit=int(args.request_limit),
        retry_limit=int(args.retry_limit),
    )
    current_requests = 0
    current_retries = 0
    for run in runs:
        metrics = _traffic_metrics(
            run,
            hard_cap=int(args.cycle_budget_bytes),
            soft_stop=int(args.soft_byte_stop_bytes),
        )
        current_requests += metrics['requests']
        current_retries += metrics['retries']
    for run in runs:
        item = entity_checkpoint.get(run.parser_entity) or {}
        attempt_id = str(item.get('attempt_id') or '').strip()
        if attempt_id:
            metrics = _traffic_metrics(
                run,
                hard_cap=int(args.cycle_budget_bytes),
                soft_stop=int(args.soft_byte_stop_bytes),
            )
            _record_attempt_guard(
                identity,
                attempt_id=attempt_id,
                requests=metrics['requests'],
                retries=metrics['retries'],
                request_limit=int(args.request_limit),
                retry_limit=int(args.retry_limit),
            )
    for parser_entity in ENTITY_ORDER[len(runs):]:
        final_path = Path(identity.entity_dir) / f'{parser_entity}.json'
        if final_path.exists():
            raise ScopeCycleError(
                f'uncheckpointed immutable entity result exists: {final_path}'
            )
        final_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f'.{parser_entity}.', suffix='.tmp', dir=final_path.parent,
        )
        os.close(descriptor)
        temporary_path = Path(temporary)
        guard_totals = _attempt_guard_totals(
            identity,
            request_limit=int(args.request_limit),
            retry_limit=int(args.retry_limit),
        )
        used_requests = max(
            int(parent_totals['requests']) + current_requests,
            int(guard_totals['requests']),
        )
        used_retries = max(
            int(parent_totals['retries']) + current_retries,
            int(guard_totals['retries']),
        )
        remaining_requests = int(args.request_limit) - used_requests
        remaining_retries = int(args.retry_limit) - used_retries
        if remaining_requests <= 0:
            raise ScopeCycleError('parent request limit exhausted before paid I/O')
        if remaining_retries <= 0:
            raise ScopeCycleError('parent retry limit exhausted before paid I/O')
        process_limits = dict(limits[parser_entity])
        process_limits['requests'] = min(
            int(process_limits['requests']), remaining_requests,
        )
        command = _runner_argv(
            identity, args, parser_entity, temporary, process_limits,
            remaining_retries,
        )
        attempt_id = stable_hash({
            'paid_proxy_packet_hash': packets['paid_proxy'].packet_hash,
            'child_cycle_id': identity.child_cycle_id,
            'parser_entity': parser_entity,
        })
        started = monotonic_ns()
        try:
            completed = subprocess_runner(
                command,
                env=_runner_environment(
                    identity, args, packets, parser_entity, remaining_retries,
                ),
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                timeout=int(args.entity_timeout_seconds),
            )
        except subprocess.TimeoutExpired as exc:
            _record_attempt_guard(
                identity,
                attempt_id=attempt_id,
                requests=process_limits['requests'],
                retries=max(0, int(args.retry_limit) - used_retries),
                request_limit=int(args.request_limit),
                retry_limit=int(args.retry_limit),
            )
            raise ScopeCycleError(
                f'{parser_entity} exceeded {args.entity_timeout_seconds}s timeout'
            ) from exc
        duration_ms = max(0, (monotonic_ns() - started) // 1_000_000)
        if int(completed.returncode) != 0:
            try:
                failed_result = _load_json_file(temporary_path)
                failed_requests = int(
                    failed_result.get('network_fetches', -1)
                )
                failed_retries = int(failed_result.get('retries', -1))
                if failed_requests < 0 or failed_retries < 0:
                    raise ValueError('negative/missing failed traffic')
            except (ScopeCycleError, TypeError, ValueError):
                failed_requests = process_limits['requests']
                failed_retries = max(0, int(args.retry_limit) - used_retries)
            _record_attempt_guard(
                identity,
                attempt_id=attempt_id,
                requests=failed_requests,
                retries=failed_retries,
                request_limit=int(args.request_limit),
                retry_limit=int(args.retry_limit),
            )
            # The runner's captured output is the only account of why it failed,
            # and it is otherwise discarded with the subprocess.
            transcript = Path(identity.entity_dir) / f'{parser_entity}-failure.log'
            try:
                transcript.parent.mkdir(parents=True, exist_ok=True)
                transcript.write_text(
                    f'$ {" ".join(command)}\n\n'
                    f'--- stdout ---\n{completed.stdout or ""}\n'
                    f'--- stderr ---\n{completed.stderr or ""}\n',
                    encoding='utf-8',
                )
            except OSError:
                pass
            diagnostic = [
                line.strip()
                for line in str(completed.stderr or '').splitlines()
                if line.strip()
                and ('Error' in line or 'error' in line or 'Exception' in line)
            ]
            reason = ' | '.join(diagnostic[-2:])
            raise ScopeCycleError(
                f'{parser_entity} runner failed with exit '
                f'{completed.returncode}' + (f': {reason}' if reason else '')
                + f' (transcript: {transcript})'
            )
        result = _load_json_file(temporary_path)
        result_requests = int(result.get('network_fetches', -1))
        result_retries = int(result.get('retries', -1))
        if result_requests < 0 or result_retries < 0:
            result_requests = process_limits['requests']
            result_retries = max(0, int(args.retry_limit) - used_retries)
        attempt_totals = _record_attempt_guard(
            identity,
            attempt_id=attempt_id,
            requests=result_requests,
            retries=result_retries,
            request_limit=int(args.request_limit),
            retry_limit=int(args.retry_limit),
        )
        _validate_result_identity(result, identity, parser_entity)
        digest = _sha256_file(temporary_path)
        run = EntityRun(
            parser_entity=parser_entity,
            result_path=str(final_path),
            result_sha256=digest,
            result=result,
            wall_clock_duration_ms=int(duration_ms),
            resumed=False,
        )
        _validate_run_contract(run, identity, args)
        with temporary_path.open('rb') as handle:
            os.fsync(handle.fileno())
        os.replace(temporary_path, final_path)
        runs.append(run)
        run_metrics = _traffic_metrics(
            run,
            hard_cap=int(args.cycle_budget_bytes),
            soft_stop=int(args.soft_byte_stop_bytes),
        )
        current_requests += run_metrics['requests']
        current_retries += run_metrics['retries']
        if (
            int(parent_totals['requests']) + current_requests
            > int(args.request_limit)
        ):
            raise ScopeCycleError('parent request limit exceeded')
        if int(parent_totals['retries']) + current_retries > int(args.retry_limit):
            raise ScopeCycleError('parent retry limit exceeded')
        if attempt_totals['requests'] > int(args.request_limit):
            raise ScopeCycleError('parent attempt request limit exceeded')
        if attempt_totals['retries'] > int(args.retry_limit):
            raise ScopeCycleError('parent attempt retry limit exceeded')
        entity_checkpoint[parser_entity] = {
            'status': 'success',
            'result_path': str(final_path),
            'result_sha256': digest,
            'wall_clock_duration_ms': int(duration_ms),
            'command_sha256': stable_hash(command),
            'attempt_id': attempt_id,
        }
        _atomic_json(checkpoint_path, {
            'version': 1,
            'identity_hash': checkpoint_identity,
            'entities': entity_checkpoint,
            'status': 'in_progress',
        }, immutable=False)
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass

    manifest = _build_scope_manifest(identity, args, runs)
    _atomic_json(manifest_path, manifest, immutable=True)
    manifest_writer(manifest)
    parent_ledger = _update_parent_ledger(
        identity,
        manifest,
        hard_cap=int(args.cycle_budget_bytes),
        soft_stop=int(args.soft_byte_stop_bytes),
        request_limit=int(args.request_limit),
        retry_limit=int(args.retry_limit),
    )
    parent_ledger_writer(parent_ledger)
    _atomic_json(checkpoint_path, {
        'version': 1,
        'identity_hash': checkpoint_identity,
        'entities': entity_checkpoint,
        'status': 'complete',
        'manifest_path': str(manifest_path),
        'manifest_digest': manifest['manifest_digest'],
        'parent_ledger_sha256': _sha256_bytes(
            (_stable_json(parent_ledger) + '\n').encode('utf-8')
        ),
    }, immutable=False)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Run one exact Transfermarkt native-v2 scope cycle',
    )
    parser.add_argument('--payload-json')
    parser.add_argument('--parent-cycle-id')
    parser.add_argument('--child-cycle-id')
    parser.add_argument('--competition-id')
    parser.add_argument('--edition-id')
    parser.add_argument('--canonical-season')
    parser.add_argument('--registry-snapshot-id')
    parser.add_argument('--scope-id')
    parser.add_argument('--result-base-dir')
    parser.add_argument('--parent-ledger-path')
    parser.add_argument('--reader-revision', type=int, required=True)
    parser.add_argument('--candidate-slot', choices=('a', 'b'), required=True)
    parser.add_argument(
        '--write-mode', choices=('dual', 'native-only'), required=True,
    )
    parser.add_argument('--approval-journal', required=True)
    parser.add_argument('--approval-bundle-json')
    parser.add_argument('--approval-packet-id')
    parser.add_argument('--approval-packet-hash')
    parser.add_argument('--paid-proxy-approval-packet-id')
    parser.add_argument('--paid-proxy-approval-packet-hash')
    parser.add_argument('--production-write-approval-packet-id')
    parser.add_argument('--production-write-approval-packet-hash')
    parser.add_argument('--cycle-budget-bytes', type=int, default=HARD_BYTE_CAP)
    parser.add_argument('--soft-byte-stop-bytes', type=int, default=SOFT_BYTE_STOP)
    parser.add_argument(
        '--request-limit', type=int,
        default=sum(item['requests'] for item in DEFAULT_ENTITY_LIMITS.values()),
    )
    parser.add_argument('--retry-limit', type=int, default=PARENT_RETRY_LIMIT)
    parser.add_argument('--entity-limits-json')
    parser.add_argument('--career-window-limit', type=int, default=100)
    parser.add_argument('--coach-history-ttl-days', type=int, default=28)
    parser.add_argument('--checkpoint-ttl-days', type=int, default=35)
    parser.add_argument('--lease-ttl-seconds', type=int, default=3600)
    parser.add_argument('--entity-timeout-seconds', type=int, default=3600)
    parser.add_argument(
        '--refresh-mode', choices=('auto', 'current', 'historical', 'force'),
        default='current',
    )
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.reader_revision < 0:
        raise ScopeCycleError('reader_revision must be non-negative')
    if args.cycle_budget_bytes != HARD_BYTE_CAP:
        raise ScopeCycleError(f'cycle byte cap must equal {HARD_BYTE_CAP}')
    if args.soft_byte_stop_bytes != SOFT_BYTE_STOP:
        raise ScopeCycleError(f'soft byte stop must equal {SOFT_BYTE_STOP}')
    production_requests = sum(
        item['requests'] for item in DEFAULT_ENTITY_LIMITS.values()
    )
    if (
        args.request_limit != production_requests
        or args.retry_limit != PARENT_RETRY_LIMIT
    ):
        raise ScopeCycleError(
            'parent request/retry limits must equal '
            f'{production_requests}/{PARENT_RETRY_LIMIT}'
        )
    if not 1 <= args.career_window_limit <= 100:
        raise ScopeCycleError('career window limit must be between 1 and 100')
    for field in (
        'coach_history_ttl_days', 'checkpoint_ttl_days',
        'lease_ttl_seconds', 'entity_timeout_seconds',
    ):
        if int(getattr(args, field)) <= 0:
            raise ScopeCycleError(f'{field} must be positive')
    if args.lease_ttl_seconds > args.entity_timeout_seconds:
        raise ScopeCycleError('lease TTL cannot exceed the entity timeout')


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = tuple(sys.argv[1:] if argv is None else argv)
    parser = _parser()
    try:
        args = parser.parse_args(raw_argv)
        _validate_args(args)
        operation_argv = approved_operation_argv(raw_argv)
        manifest = run_scope_cycle(args, operation_argv=operation_argv)
        print(_stable_json({
            'status': 'complete',
            'scope_id': manifest['scope_id'],
            'manifest_digest': manifest['manifest_digest'],
            'scope_manifest': _scope_identity(args).scope_manifest_path,
        }))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI must fail closed
        failure = {
            'status': 'failed',
            'error_type': type(exc).__name__,
            'error': str(exc),
            'silver_trigger_allowed': False,
        }
        try:
            identity = _scope_identity(args)
            failure.update({
                'parent_cycle_id': identity.parent_cycle_id,
                'child_cycle_id': identity.child_cycle_id,
                'scope_id': identity.scope_id,
            })
            _atomic_json(
                Path(identity.result_base_dir) / 'scope-status.json',
                failure,
                immutable=False,
            )
        except Exception:  # noqa: BLE001 - preserve the original failure
            pass
        print(_stable_json(failure), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
