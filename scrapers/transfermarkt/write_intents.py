"""Durable parsed career bundles, recoverable without another paid request."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import uuid


def _root():
    return Path(os.environ.get('TM_WRITE_INTENT_DIR', os.environ.get('TM_PENDING_CHECKPOINT_DIR', '/opt/airflow/logs/transfermarkt-checkpoints'))) / 'career-write-intents'


def _encode(value):
    import pandas as pd
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, datetime):
        return {'type': 'datetime', 'value': value.isoformat()}
    if isinstance(value, date):
        return {'type': 'date', 'value': value.isoformat()}
    if isinstance(value, Decimal):
        return {'type': 'decimal', 'value': str(value)}
    if hasattr(value, 'item'):
        value = value.item()
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _decode(value):
    if isinstance(value, dict):
        if value['type'] == 'datetime':
            return datetime.fromisoformat(value['value'])
        if value['type'] == 'date':
            return date.fromisoformat(value['value'])
        if value['type'] == 'decimal':
            return Decimal(value['value'])
        raise RuntimeError('career intent contains an unknown scalar type')
    return value


def pack_frames(frames):
    return {key: {'columns': list(frame.columns), 'dtypes': [str(dtype) for dtype in frame.dtypes],
        'rows': [[_encode(cell) for cell in row] for row in frame.itertuples(index=False, name=None)],
        'attrs': dict(frame.attrs)} for key, frame in frames.items()}


def unpack_frames(payload):
    import pandas as pd
    frames = {}
    for key, item in payload.items():
        frame = pd.DataFrame([[_decode(cell) for cell in row] for row in item['rows']], columns=item['columns'])
        for column, dtype in zip(item['columns'], item['dtypes'], strict=True):
            frame[column] = frame[column].astype(dtype)
        frame.attrs.update(item['attrs'])
        frames[key] = frame
    return frames


def save_intent(identity, frames, **evidence):
    payload = {'identity': dict(identity), 'frames': pack_frames(frames), 'evidence': evidence}
    body = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
    digest = hashlib.sha256(body.encode()).hexdigest()
    path = _root() / (digest + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text() != body:
            raise RuntimeError('career intent checksum collision')
        return path
    temporary = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('x') as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return path


def pending_intents(identity):
    matches = []
    for path in sorted(_root().glob('*.json')):
        body = path.read_text()
        if hashlib.sha256(body.encode()).hexdigest() != path.stem:
            raise RuntimeError('durable career write intent checksum differs; refusing paid retry')
        payload = json.loads(body)
        if resolution_for(path) is not None:
            continue
        if all(payload['identity'].get(key) == value for key, value in identity.items()):
            matches.append((path, payload))
    return matches


def finish_intent(path):
    path.unlink()


def snapshot_anchors(path, connection, outputs, *, capture_times=None):
    """Persist the pre-write warehouse boundary before any career mutation."""
    from scrapers.transfermarkt.writer import CAREER_TABLES, execute_statement
    path = Path(path)
    anchor_path = _root() / 'anchors' / path.name
    tables = sorted(output.table_name for output in outputs if output.table_name in CAREER_TABLES)
    if not tables:
        return {}
    if anchor_path.exists():
        wrapped = json.loads(anchor_path.read_text())
        body = json.dumps(wrapped['payload'], sort_keys=True, separators=(',', ':'))
        if hashlib.sha256(body.encode()).hexdigest() != wrapped['sha256']:
            raise RuntimeError('career snapshot anchor checksum differs')
        payload = wrapped['payload']
        if payload['intent_sha256'] != path.stem or sorted(payload['tables']) != tables:
            raise RuntimeError('career snapshot anchor identity differs')
    else:
        anchors = {}
        cur = connection.cursor()
        try:
            for table in tables:
                try:
                    execute_statement(cur, f'SELECT snapshot_id FROM iceberg.bronze."{table}$snapshots" ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1')
                    rows = cur.fetchall()
                except Exception as exc:
                    if not any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                        raise
                    rows = []
                anchors[table] = int(rows[0][0]) if rows else None
        finally:
            cur.close()
        from datetime import timezone
        clocks = {str(player): datetime.fromisoformat(stamp).astimezone(timezone.utc).isoformat(timespec='microseconds')
                  for player, stamp in (capture_times or {}).items()}
        payload = {'intent_sha256': path.stem, 'tables': anchors, 'capture_times': clocks}
        body = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        anchor_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = anchor_path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
        with temporary.open('x') as handle:
            json.dump({'payload': payload, 'sha256': hashlib.sha256(body.encode()).hexdigest()}, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, anchor_path)
        directory = os.open(anchor_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    # Registry protection precedes Bronze as well. Shared maintenance cannot
    # discard the new snapshot in the crash gap before capture_refs exists.
    from scrapers.transfermarkt.career_refs import persist_snapshot_anchors
    persist_snapshot_anchors(connection, payload)
    return payload['tables']


def read_snapshot_anchors(path):
    path = Path(path)
    anchor_path = _root() / 'anchors' / path.name
    if not anchor_path.exists():
        return {}
    wrapped = json.loads(anchor_path.read_text())
    payload = wrapped['payload']
    body = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    if hashlib.sha256(body.encode()).hexdigest() != wrapped['sha256'] or payload['intent_sha256'] != path.stem:
        raise RuntimeError('career snapshot anchor identity/checksum differs')
    return payload['tables']


def _immutable_record(path, payload):
    body = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
    wrapped = json.dumps({'payload': payload, 'sha256': hashlib.sha256(body.encode()).hexdigest()}, sort_keys=True, separators=(',', ':'))
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text() != wrapped:
            raise RuntimeError('immutable career resolution differs')
        return
    temporary = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('x') as handle:
        handle.write(wrapped)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _read_record(path):
    wrapped = json.loads(path.read_text())
    body = json.dumps(wrapped['payload'], sort_keys=True, separators=(',', ':'), allow_nan=False)
    if hashlib.sha256(body.encode()).hexdigest() != wrapped['sha256']:
        raise RuntimeError('career resolution checksum differs')
    return wrapped['payload']


def archive_complete(path, receipt, frames):
    """Keep actual successful raw journal and exact physical receipt for replay."""
    path = Path(path)
    body = path.read_text()
    if hashlib.sha256(body.encode()).hexdigest() != path.stem or receipt.get('verified') is not True:
        raise RuntimeError('complete career journal/receipt is unverified')
    archive = _root() / 'completed' / path.name
    if archive.exists():
        existing = _read_record(archive)
        if existing['intent_sha256'] != path.stem or existing['receipt']['bronze_manifest'] != receipt['bronze_manifest'] or existing['receipt']['outputs'] != receipt['outputs']:
            raise RuntimeError('completed career physical proof changed during acknowledgement retry')
        receipt = existing['receipt']
    else:
        _immutable_record(archive, {'intent_sha256': path.stem, 'journal': json.loads(body),
            'receipt': receipt, 'frames': pack_frames(frames)})
    for manifest in receipt['manifests']:
        for row in manifest['rows']:
            key = hashlib.sha256(json.dumps([receipt['manifest_cycle_id'], row['entity'], row['native_batch_id']], separators=(',', ':')).encode()).hexdigest()
            _immutable_record(_root() / 'completed' / 'by-unit' / (key + '.json'), {'intent_sha256': path.stem})
    return receipt


def completed_capture(cycle, entity, batch):
    key = hashlib.sha256(json.dumps([cycle, entity, batch], separators=(',', ':')).encode()).hexdigest()
    pointer = _root() / 'completed' / 'by-unit' / (key + '.json')
    if not pointer.exists():
        return None
    digest = _read_record(pointer)['intent_sha256']
    return completed_intent(_root() / (digest + '.json'))


def completed_intent(path):
    digest = Path(path).stem
    archive = _root() / 'completed' / (digest + '.json')
    if not archive.exists():
        return None
    value = _read_record(archive)
    original = json.dumps(value['journal'], sort_keys=True, separators=(',', ':'), allow_nan=False)
    if value['intent_sha256'] != digest or hashlib.sha256(original.encode()).hexdigest() != digest:
        raise RuntimeError('completed career original journal differs')
    return value


def retire_intent(path, resolution):
    path = Path(path)
    if resolution.get('intent_sha256') != path.stem or resolution.get('status') != 'superseded_partial_write':
        raise RuntimeError('career retirement does not match original intent')
    _immutable_record(_root() / 'resolutions' / path.name, resolution)


def resolution_for(path):
    path = Path(path)
    target = _root() / 'resolutions' / path.name
    if not target.exists():
        return None
    value = _read_record(target)
    if value.get('intent_sha256') != path.stem or value.get('status') != 'superseded_partial_write':
        raise RuntimeError('career resolution identity/status differs')
    return value


def retired_intents(identity):
    matches = []
    for path in sorted(_root().glob('*.json')):
        body = path.read_text()
        if hashlib.sha256(body.encode()).hexdigest() != path.stem:
            raise RuntimeError('retired career original journal checksum differs')
        journal = json.loads(body)
        if all(journal['identity'].get(key) == value for key, value in identity.items()):
            resolution = resolution_for(path)
            if resolution is not None:
                matches.append((path, journal, resolution))
    return matches
