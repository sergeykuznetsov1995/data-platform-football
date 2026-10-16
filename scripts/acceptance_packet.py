#!/usr/bin/env python3
"""Build a read-only selection aid from an existing full acceptance snapshot.

This module never reads the ledger, evidence paths or handoffs, never executes
snapshot content, and cannot establish acceptance success. The CLI only reads its
snapshot input and writes a new artifact directory. The lead must use full
current definitions and evidence before any journal operation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys


class PacketError(ValueError):
    """The packet cannot safely represent the supplied input."""


STATUSES = {'closed', 'draft', 'on_hold', 'waiting', 'ready', 'failed',
            'unknown', 'observing'}
THREAD = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
WATCH = re.compile(r'[a-z0-9][a-z0-9_-]{0,79}')
INDEX_FIELDS = ('id', 'revision', 'series', 'source', 'status', 'started_at',
                'definition_confirmed', 'impact_hold', 'close_hold',
                'monitoring_stale', 'next_check_at')


def _require(condition, message):
    if not condition:
        raise PacketError(message)


def _instant(value, label):
    try:
        _require(isinstance(value, (str, datetime)), f'invalid timestamp: {label}')
        parsed = (datetime.fromisoformat(value.replace('Z', '+00:00'))
                  if isinstance(value, str) else value)
        _require(parsed.tzinfo is not None and parsed.utcoffset() is not None,
                 f'timestamp requires timezone: {label}')
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise PacketError(f'invalid timestamp: {label}') from exc


def _integer(value, label, minimum=0):
    _require(type(value) is int and value >= minimum, f'invalid integer: {label}')


def _text(value, label):
    _require(isinstance(value, str) and bool(value.strip()), f'missing text: {label}')


def _identity(owner_thread_id, watch_key):
    _require(isinstance(owner_thread_id, str) and THREAD.fullmatch(owner_thread_id),
             'owner_thread_id must be a canonical thread UUID')
    _require(isinstance(watch_key, str) and WATCH.fullmatch(watch_key), 'invalid watch_key')


def _encode(value):
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                           separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')
    except (TypeError, ValueError, UnicodeError) as exc:
        raise PacketError('input must contain finite JSON values') from exc


def _validate(snapshot):
    _require(isinstance(snapshot, dict), 'snapshot must be an object')
    _require(type(snapshot.get('schema')) is int and snapshot['schema'] == 1,
             'unsupported or missing snapshot schema')
    _integer(snapshot.get('revision'), 'snapshot.revision')
    _instant(snapshot.get('generated_at'), 'snapshot.generated_at')
    _require(isinstance(snapshot.get('acceptances'), list), 'acceptances must be a list')
    _require(isinstance(snapshot.get('handoffs'), dict), 'handoffs must be an object')
    if 'sources' in snapshot:
        _require(isinstance(snapshot['sources'], dict), 'sources must be an object')
    seen = set()
    for row in snapshot['acceptances']:
        _require(isinstance(row, dict), 'acceptance must be an object')
        _require(set(INDEX_FIELDS) | {'monitoring'} <= row.keys(),
                 'missing required acceptance fields')
        _text(row['id'], 'acceptance.id')
        _require(row['id'] not in seen, f'duplicate acceptance id: {row["id"]}')
        seen.add(row['id'])
        _integer(row['revision'], 'acceptance.revision', 1)
        _require(row['revision'] <= snapshot['revision'], 'row revision exceeds snapshot revision')
        _integer(row['series'], 'acceptance.series', 1)
        _text(row['source'], 'acceptance.source')
        _require(isinstance(row['status'], str) and row['status'] in STATUSES,
                 'invalid acceptance status')
        for name in ('definition_confirmed', 'impact_hold', 'close_hold', 'monitoring_stale'):
            _require(type(row[name]) is bool, f'{name} must be boolean')
        for name in ('started_at', 'next_check_at', 'checked_at'):
            if row.get(name) is not None:
                _instant(row[name], f'acceptance.{name}')
        monitoring = row['monitoring']
        _require(monitoring is None or isinstance(monitoring, dict),
                 'monitoring must be an object or null')
        if monitoring is not None:
            # An operational deadline is deliberately validated during selection:
            # a broken deadline is an explicit candidate, never an empty queue.
            _identity(monitoring.get('owner_thread_id'), monitoring.get('watch_key'))
            if 'bound_series' in monitoring:
                _integer(monitoring['bound_series'], 'monitoring.bound_series', 1)
    for source, handoff in snapshot['handoffs'].items():
        _text(source, 'handoff.source')
        _require(isinstance(handoff, dict), 'handoff must be an object')
        _integer(handoff.get('revision'), 'handoff.revision', 1)
        _require(handoff['revision'] <= snapshot['revision'],
                 'handoff revision exceeds snapshot revision')
        _instant(handoff.get('checked_at'), 'handoff.checked_at')
        _text(handoff.get('path'), 'handoff.path')
    _encode(snapshot)  # Reject non-JSON/nonfinite content, including omitted large fields.


def build_packet(snapshot, *, now, owner_thread_id, watch_key,
                 reconciliation_due_at=None, owner_status_due_at=None):
    """Select work without mutating input or opening any referenced path.

    ``next_check_at`` on a snapshot row describes a calculated evidence window;
    only a valid, current, owned ``monitoring.next_check_at`` schedules operational
    work. Missing global deadlines require the lead's check even for an empty
    acceptance list. This return value is not a proof of PASS or of completion.
    """
    _validate(snapshot)
    _identity(owner_thread_id, watch_key)
    current = _instant(now, 'now')
    index, candidates, attention, deadlines = [], [], [], []
    global_checks = {}
    for name, value in (('reconciliation', reconciliation_due_at),
                        ('owner_status', owner_status_due_at)):
        if value is None:
            global_checks[name] = {'status': 'unknown', 'due_at': None}
        else:
            deadline = _instant(value, name)
            text = deadline.isoformat() if isinstance(value, datetime) else value
            global_checks[name] = {'status': 'due' if deadline <= current else 'scheduled',
                                   'due_at': text}
            deadlines.append((deadline, text))
    for row in snapshot['acceptances']:
        if row['status'] == 'closed':
            continue
        item = {name: row[name] for name in INDEX_FIELDS}
        item['phase'] = ('started' if row['started_at'] is not None
                         and _instant(row['started_at'], 'started_at') <= current else 'waiting')
        monitoring = row['monitoring']
        item['monitoring'] = (None if monitoring is None else {
            name: monitoring.get(name) for name in (
                'owner_thread_id', 'watch_key', 'bound_series', 'next_check_at')})
        own = bool(monitoring and monitoring['owner_thread_id'] == owner_thread_id
                   and monitoring['watch_key'] == watch_key)
        item['assigned_to_owner'] = own
        reasons = []
        deadline = None
        if monitoring is None:
            reasons.append('missing_monitoring')
        else:
            if not own:
                reasons.append('foreign_assignment')
            if row['monitoring_stale'] or monitoring.get('bound_series') != row['series']:
                reasons.append('stale_binding')
            if 'next_check_at' not in monitoring:
                reasons.append('missing_operational_deadline')
            else:
                try:
                    deadline = _instant(monitoring['next_check_at'], 'monitoring.next_check_at')
                except PacketError:
                    reasons.append('invalid_operational_deadline')
        if not row['definition_confirmed']:
            reasons.append('unconfirmed_definition')
        if reasons:
            attention.append({'id': row['id'], 'reasons': list(reasons)})
        if own and deadline is not None and not reasons:
            deadlines.append((deadline, monitoring['next_check_at']))
            if deadline <= current:
                reasons.append('operational_due')
        if reasons:
            action = ('review_foreign_assignment' if monitoring and not own else
                      'lead_check' if reasons != ['operational_due'] else
                      'check_readiness' if item['phase'] == 'waiting' else 'check_evidence')
            candidates.append({'id': row['id'], 'revision': row['revision'],
                               'series': row['series'], 'reasons': reasons, 'action': action})
        index.append(item)
    unknown = any(value['status'] == 'unknown' for value in global_checks.values())
    global_due = any(value['status'] == 'due' for value in global_checks.values())
    needs_lead = unknown or bool(attention)
    return {
        'schema': 1, 'kind': 'acceptance_selection_packet', 'selection_only': True,
        'revision': snapshot['revision'], 'generated_at': snapshot['generated_at'],
        'built_at': current.isoformat(), 'owner_thread_id': owner_thread_id, 'watch_key': watch_key,
        'counts': {'total': len(snapshot['acceptances']), 'open': len(index),
                   'closed': len(snapshot['acceptances']) - len(index),
                   'candidates': len(candidates), 'attention': len(attention)},
        'index': index, 'candidates': candidates, 'attention': attention,
        'handoffs': [dict(source=source, **{name: handoff[name] for name in
                     ('revision', 'checked_at', 'path')})
                     for source, handoff in sorted(snapshot['handoffs'].items())],
        'global_checks': global_checks, 'needs_lead_check': needs_lead,
        'work_state': ('needs_lead_check' if needs_lead else
                       'due' if candidates or global_due else 'waiting'),
        'next_check_at': min(deadlines, key=lambda value: value[0])[1] if deadlines else None,
    }


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        _require(key not in value, f'duplicate JSON key: {key}')
        value[key] = item
    return value


def _reject_constant(value):
    raise PacketError(f'nonfinite JSON constant: {value}')


def _new_directory(path):
    """Open every ancestor without following symlinks, then mkdir exclusively."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parent_fd = os.open('/', flags)
    try:
        for component in path.parts[1:-1]:
            next_fd = os.open(component, flags, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
        os.mkdir(path.name, mode=0o700, dir_fd=parent_fd)
        return os.open(path.name, flags, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def _write_artifact(directory_fd, name, data):
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                 0o600, dir_fd=directory_fd)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fchmod(stream.fileno(), 0o444)
        os.fsync(stream.fileno())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True, type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--owner-thread', required=True)
    parser.add_argument('--watch-key', required=True)
    parser.add_argument('--now')
    parser.add_argument('--reconciliation-due-at')
    parser.add_argument('--owner-status-due-at')
    parser.add_argument('--max-output-bytes', type=int, default=32768)
    args = parser.parse_args(argv)
    try:
        _require(args.max_output_bytes > 0, 'output budget must be positive')
        raw = args.snapshot.read_bytes()
        snapshot = json.loads(raw, object_pairs_hook=_unique_object,
                              parse_constant=_reject_constant)
        packet = build_packet(snapshot, now=args.now or datetime.now(timezone.utc),
                              owner_thread_id=args.owner_thread, watch_key=args.watch_key,
                              reconciliation_due_at=args.reconciliation_due_at,
                              owner_status_due_at=args.owner_status_due_at)
        output_dir = Path(os.path.abspath(args.output_dir))
        _require(output_dir != Path('/'), 'output directory must be new')
        packet['raw_snapshot'] = {'path': str(output_dir / 'snapshot.json'),
                                  'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
        encoded = _encode(packet)
        stdout = encoded
        if len(stdout) > args.max_output_bytes:
            stdout = _encode({'path': str(output_dir / 'packet.json'),
                              'sha256': hashlib.sha256(encoded).hexdigest(),
                              'bytes': len(encoded), 'requires_full_read': True,
                              'candidate_count': len(packet['candidates'])})
        _require(len(stdout) <= args.max_output_bytes,
                 'output budget too small even for an explicit full-packet pointer')
        directory_fd = _new_directory(output_dir)
        try:
            _write_artifact(directory_fd, 'snapshot.json', raw)
            _write_artifact(directory_fd, 'packet.json', encoded)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        sys.stdout.write(stdout.decode('utf-8'))
        return 0
    except (PacketError, OSError, ValueError, UnicodeError, RecursionError) as exc:
        print(f'acceptance_packet: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
