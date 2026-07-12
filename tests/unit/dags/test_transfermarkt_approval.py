from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from dags.utils.transfermarkt_approval import (
    ApprovalDriftError,
    ApprovalExpiredError,
    ApprovalJournal,
    ApprovalPacket,
    ApprovalStateError,
    ApprovalValidationError,
)


NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


def _packet(**changes) -> ApprovalPacket:
    values = {
        'packet_id': 'tm-paid-20260711-001',
        'action': 'paid_proxy',
        'argv': (
            'python',
            'scripts/research/bench_transfermarkt_fetch.py',
            '--cycle-budget-bytes',
            '15728640',
        ),
        'byte_cap_bytes': 15 * 1024 * 1024,
        'byte_cap_mib': Decimal('15'),
        'request_limit': 120,
        'retry_limit': 3,
        'concurrency': 1,
        'expected_duration_seconds': 900,
        'affected_tables': (),
        'affected_files': (
            '/tmp/bench_transfermarkt_native_v2_20260711.json',
        ),
        'stop_conditions': (
            'stop at the byte cap',
            'stop when the proxy circuit opens',
        ),
        'backup_commands': (
            ('sha256sum', '/tmp/bench_transfermarkt_native_v2_20260711.json'),
        ),
        'rollback_commands': (
            ('unlink', '/tmp/bench_transfermarkt_native_v2_20260711.json'),
        ),
    }
    values.update(changes)
    return ApprovalPacket(**values)


def _journal(storage=None, *, now=NOW) -> ApprovalJournal:
    return ApprovalJournal({} if storage is None else storage, clock=lambda: now)


def test_canonical_json_and_hash_are_stable():
    first = _packet(byte_cap_mib='15.0')
    second = _packet(byte_cap_mib=Decimal('15.000'))

    assert first.canonical_json == second.canonical_json
    assert first.packet_hash == second.packet_hash
    assert len(first.packet_hash) == 64
    assert ': ' not in first.canonical_json
    assert ', ' not in first.canonical_json


@pytest.mark.parametrize(
    ('field', 'value', 'error'),
    [
        ('byte_cap_mib', '14.9', 'different limits'),
        ('request_limit', 0, 'positive request limit'),
        ('retry_limit', -1, 'non-negative'),
        ('concurrency', 0, 'positive'),
        ('expected_duration_seconds', 0, 'positive'),
        ('stop_conditions', (), 'cannot be empty'),
        ('affected_files', (), 'at least one affected'),
    ],
)
def test_packet_rejects_incomplete_or_inconsistent_limits(field, value, error):
    changes = {field: value}
    if field == 'affected_files':
        changes['affected_tables'] = ()
    with pytest.raises(ApprovalValidationError, match=error):
        _packet(**changes)


@pytest.mark.parametrize(
    'argv',
    [
        ('bash', '-lc', 'python $SCRIPT'),
        ('python', '${SCRIPT}'),
        ('python', '`which python`'),
        ('python', '~/runner.py'),
    ],
)
def test_packet_rejects_shell_indirection(argv):
    with pytest.raises(ApprovalValidationError):
        _packet(argv=argv)


def test_production_write_can_declare_zero_network_budget():
    packet = _packet(
        packet_id='tm-write-20260711-001',
        action='production_write',
        byte_cap_bytes=0,
        byte_cap_mib=0,
        request_limit=0,
        affected_tables=('iceberg.bronze.transfermarkt_competitions',),
    )

    assert packet.byte_cap_bytes == 0
    assert packet.payload()['byte_cap_mib'] == '0'


def test_one_shot_happy_path_persists_all_transitions_in_mapping():
    storage = {}
    packet = _packet()
    journal = _journal(storage)

    issued = journal.issue(packet, expires_at=NOW + timedelta(minutes=10))
    approved = journal.approve(packet, presented_hash=packet.packet_hash)
    consumed = journal.consume(
        packet,
        presented_hash=packet.packet_hash,
        execution_argv=packet.argv,
    )

    assert issued.status == 'issued'
    assert approved.status == 'approved'
    assert consumed.status == 'consumed'
    assert storage[packet.packet_hash]['status'] == 'consumed'
    assert consumed.approved_at is not None
    assert consumed.consumed_at is not None

    with pytest.raises(ApprovalStateError, match='cannot consume'):
        journal.consume(
            packet,
            presented_hash=packet.packet_hash,
            execution_argv=packet.argv,
        )


def test_old_hash_and_packet_id_cannot_be_reissued():
    packet = _packet()
    journal = _journal()
    journal.issue(packet, expires_at=NOW + timedelta(minutes=10))

    with pytest.raises(ApprovalStateError, match='hash cannot be reissued'):
        journal.issue(packet, expires_at=NOW + timedelta(hours=1))
    with pytest.raises(ApprovalStateError, match='id cannot be reused'):
        journal.issue(
            replace(packet, concurrency=2),
            expires_at=NOW + timedelta(hours=1),
        )


def test_hash_or_packet_content_drift_is_rejected_before_approval():
    packet = _packet()
    journal = _journal()
    journal.issue(packet, expires_at=NOW + timedelta(minutes=10))

    with pytest.raises(ApprovalDriftError, match='presented hash'):
        journal.approve(packet, presented_hash='0' * 64)

    drifted = replace(packet, concurrency=2)
    with pytest.raises(ApprovalDriftError, match='presented hash'):
        journal.approve(drifted, presented_hash=packet.packet_hash)

    assert journal.get(packet.packet_hash).status == 'issued'


def test_production_command_drift_is_rejected_without_consuming_packet():
    packet = _packet()
    journal = _journal()
    journal.issue(packet, expires_at=NOW + timedelta(minutes=10))
    journal.approve(packet, presented_hash=packet.packet_hash)

    with pytest.raises(ApprovalDriftError, match='execution argv differs'):
        journal.consume(
            packet,
            presented_hash=packet.packet_hash,
            execution_argv=packet.argv + ('--write',),
        )

    assert journal.get(packet.packet_hash).status == 'approved'


def test_expired_packet_cannot_be_approved_or_consumed():
    packet = _packet()
    storage = {}
    _journal(storage).issue(packet, expires_at=NOW + timedelta(seconds=1))

    expired = _journal(storage, now=NOW + timedelta(seconds=1))
    with pytest.raises(ApprovalExpiredError):
        expired.approve(packet, presented_hash=packet.packet_hash)
    assert expired.get(packet.packet_hash).status == 'issued'


def test_failed_packet_is_terminal_and_keeps_failure_evidence():
    packet = _packet()
    journal = _journal()
    journal.issue(packet, expires_at=NOW + timedelta(minutes=10))
    journal.approve(packet, presented_hash=packet.packet_hash)
    failed = journal.fail(
        packet,
        presented_hash=packet.packet_hash,
        reason='proxy preflight returned HTTP 504',
    )

    assert failed.status == 'failed'
    assert failed.failure_reason == 'proxy preflight returned HTTP 504'
    with pytest.raises(ApprovalStateError, match='cannot consume'):
        journal.consume(
            packet,
            presented_hash=packet.packet_hash,
            execution_argv=packet.argv,
        )


def test_approval_api_never_executes_declared_commands(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError('approval API attempted command execution')

    monkeypatch.setattr('subprocess.run', forbidden)
    packet = _packet()
    journal = _journal()
    journal.issue(packet, expires_at=NOW + timedelta(minutes=10))
    journal.approve(packet, presented_hash=packet.packet_hash)
    journal.consume(
        packet,
        presented_hash=packet.packet_hash,
        execution_argv=packet.argv,
    )
