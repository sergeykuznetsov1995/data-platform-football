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
        if all(payload['identity'].get(key) == value for key, value in identity.items()):
            matches.append((path, payload))
    return matches


def finish_intent(path):
    path.unlink()
