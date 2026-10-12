"""Durable history transport circuit; source rejections use their own policy."""
from __future__ import annotations

import fcntl
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

WINDOW = timedelta(minutes=10)
PAUSE = timedelta(minutes=15)
logger = logging.getLogger(__name__)


class HistoryTransportCircuit:
    """Serialize admission, one recovery probe, and idempotent attempt feedback.

    The journal is shared by planner and finalizer on the existing durable
    backfill result volume. A corrupt journal fails closed before paid work.
    """

    def __init__(self, root=None):
        self.root = Path(root or os.environ.get(
            'TM_HISTORY_CIRCUIT_DIR', '/opt/airflow/logs/transfermarkt-backfill/circuits'))

    def _update(self, stream_id, operation):
        if not stream_id.startswith('history-') or not stream_id[8:].isdigit():
            raise ValueError('invalid history stream identity')
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / (stream_id + '.json')
        with (self.root / (stream_id + '.lock')).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = json.loads(path.read_text()) if path.exists() else {
                'version': 1, 'failures': [], 'seen_attempts': [], 'alerts': []}
            if data.get('version') != 1:
                raise ValueError('unsupported history circuit journal')
            result = operation(data)
            temporary = path.with_suffix('.tmp')
            with temporary.open('w') as output:
                json.dump(data, output, sort_keys=True)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return result

    def admit(self, stream_id, *, now, reserve=False, owner=None):
        """Return (allowed, recovery_probe, retry_at); reserve probe atomically."""
        if reserve and not owner:
            raise ValueError("recovery probe requires a durable batch owner")
        def operation(data):
            paused = datetime.fromisoformat(data['paused_until']) if data.get('paused_until') else None
            probe = datetime.fromisoformat(data['probe_until']) if data.get('probe_until') else None
            if paused and now < paused:
                return False, False, paused
            if paused:
                if probe and now < probe:
                    return False, False, probe
                if reserve:
                    # Hard-killed probes cannot admit an uncontrolled batch.
                    data['probe_until'] = (now + PAUSE).isoformat()
                    data['probe_owner'] = owner
                return True, True, None
            return True, False, None
        return self._update(stream_id, operation)

    def authorized(self, stream_id, *, owner, now):
        def operation(data):
            if not data.get('paused_until'):
                return True
            paused = datetime.fromisoformat(data['paused_until'])
            probe = datetime.fromisoformat(data['probe_until']) if data.get('probe_until') else None
            return bool(now >= paused and probe and now < probe and data.get('probe_owner') == owner)
        return self._update(stream_id, operation)

    def feedback(self, stream_id, *, attempt_id, scope_id, transport_error, now, recovery_valid=True):
        def operation(data):
            if attempt_id in data['seen_attempts']:
                return data.get('paused_until')
            data['seen_attempts'].append(attempt_id)
            recovering = bool(data.get('paused_until'))
            if not transport_error and (not recovering or recovery_valid):
                if recovering and data.get('probe_until'):
                    data.pop('paused_until', None)
                    data.pop('probe_until', None)
                    data.pop('probe_owner', None)
                    data['failures'] = []
                return None
            failures = [entry for entry in data['failures']
                        if now - WINDOW <= datetime.fromisoformat(entry['at']) <= now]
            failures.append({'scope_id': scope_id, 'at': now.isoformat()})
            data['failures'] = failures
            if recovering or len({entry['scope_id'] for entry in failures}) >= 3:
                until = now + PAUSE
                data['paused_until'] = until.isoformat()
                data.pop('probe_until', None)
                data.pop('probe_owner', None)
                alert = {'kind': 'history_transport_pause', 'stream_id': stream_id,
                         'paused_until': until.isoformat(), 'at': now.isoformat(),
                         'scope_ids': sorted({entry['scope_id'] for entry in failures})}
                data['alerts'].append(alert)
                logger.error('Transfermarkt history transport alert: %s', json.dumps(alert, sort_keys=True))
            return data.get('paused_until')
        return self._update(stream_id, operation)
