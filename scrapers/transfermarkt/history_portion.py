"""Shared, durable admission for an opted-in historical work portion."""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import time
from dataclasses import asdict

from .models import TrafficBudgetExceeded

MAX_HISTORY_ATTEMPTS = 500
HISTORY_SETTLE_SECONDS = 120


class HistoryContinuation(RuntimeError):
    """Paid work stopped at a boundary; retain the exact unfinished work."""


def enabled():
    return bool(os.environ.get('TM_HISTORY_DEADLINE_AT'))


def recovery_probe():
    return os.environ.get('TM_HISTORY_RECOVERY_PROBE', '').lower() == 'true'


def request_limit():
    value = int(os.environ.get('TM_HISTORY_PORTION_REQUEST_LIMIT', MAX_HISTORY_ATTEMPTS))
    if not 1 <= value <= MAX_HISTORY_ATTEMPTS:
        raise ValueError('historical portion attempt limit must be in 1..500')
    return 1 if recovery_probe() else value


def remaining_seconds():
    value = datetime.fromisoformat(os.environ['TM_HISTORY_DEADLINE_AT'])
    if value.tzinfo is None:
        raise ValueError('historical portion deadline must include a timezone')
    from dags.utils.transfermarkt_current_timetable import remaining_work_seconds
    return remaining_work_seconds(value, datetime.now(timezone.utc))


def request_deadline():
    return time.monotonic() + remaining_seconds() - HISTORY_SETTLE_SECONDS


def career_admitted(client):
    """Leave room for the whole endpoint retry ladder and durable settlement."""
    from .client import _MAX_FETCH_ATTEMPTS
    stats = client.get_traffic_stats()
    cap = stats.get('request_attempt_budget')
    used = stats.get('request_attempts', 0)
    path = Path(os.environ['TM_HISTORY_ATTEMPT_LEDGER'])
    total = json.loads(path.read_text())['attempts'] if path.exists() else 0
    return (remaining_seconds() > HISTORY_SETTLE_SECONDS + 180
            and total + _MAX_FETCH_ATTEMPTS <= request_limit()
            and (cap is None or used + _MAX_FETCH_ATTEMPTS <= cap))


def _lease_journal(scope):
    import hashlib
    root = Path(os.environ.get('TM_HISTORY_LEASE_DIR', '/opt/airflow/logs/transfermarkt-native-v2/backfill/leases'))
    return root / (hashlib.sha256(str(scope).encode()).hexdigest() + '.json')


def _write_private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f'.{os.getpid()}.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w') as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def journal_lease(lease, scope, *, snapshot=None):
    if not enabled():
        return
    if not scope:
        raise ValueError('historical lease journal requires an exact scope')
    path = _lease_journal(scope)
    value = json.loads(path.read_text()) if path.exists() else {'scope': scope, 'leases': {}}
    if value.get('scope') != scope:
        raise ValueError('historical lease journal scope mismatch')
    record = value['leases'].get(lease.lease_id)
    if record is None:
        if snapshot is not None:
            raise ValueError('historical close lacks its original acquisition journal')
        record = {'lease': asdict(lease), 'portion_id': os.environ['TM_HISTORY_PORTION_ID'],
                  'attempt_ledger': os.environ['TM_HISTORY_ATTEMPT_LEDGER'],
                  'deadline_at': os.environ['TM_HISTORY_DEADLINE_AT'],
                  'provider_byte_grant': os.environ.get('TM_PROVIDER_BYTE_BUDGET'),
                  'grant_cycle_id': os.environ.get('TM_CYCLE_LEDGER_KEY'),
                  'grant': json.loads(os.environ.get('TM_HISTORY_GRANT_JSON', '{}')), 'status': 'active'}
    elif record['status'] == 'closed':
        if snapshot is None or record['traffic'] != asdict(snapshot):
            raise ValueError('closed historical lease evidence drifted')
        return
    elif record['lease'] != asdict(lease):
        raise ValueError('historical lease acquisition identity drifted')
    if snapshot is not None:
        record.update(status='closed', traffic=asdict(snapshot), reconciled_at=datetime.now(timezone.utc).isoformat(),
                      reconciliation_portion_id=os.environ['TM_HISTORY_PORTION_ID'])
        # Closed proof needs no credential. Leave the immutable counters for audit.
        record['lease'].pop('token', None)
        record['lease'].pop('proxy_url', None)
    value['leases'][lease.lease_id] = record
    _write_private(path, value)


def reconcile_leases(provider, scope):
    """A hard-killed lease can resume only after TTL and final gateway close."""
    if not enabled():
        return
    if not scope:
        raise ValueError('historical lease reconciliation requires an exact scope')
    path = _lease_journal(scope)
    if not path.exists():
        return
    value = json.loads(path.read_text())
    if value.get('scope') != scope:
        raise ValueError('historical lease journal scope mismatch')
    from .models import ProxyLease
    for record in value['leases'].values():
        if record['status'] == 'closed':
            continue
        lease = ProxyLease(**record['lease'])
        if lease.expires_at > time.time():
            raise HistoryContinuation('previous historical lease is still within its TTL')
        # close() checks close_complete, final counters and uncertainty. An
        # expired timestamp alone never authorizes another paid request.
        snapshot = provider.close(lease)
        journal_lease(lease, scope, snapshot=snapshot)


def reserve_attempt():
    """Reserve before paid GET, including retries; never reclaim crash attempts."""
    if not enabled():
        return
    path = Path(os.environ['TM_HISTORY_ATTEMPT_LEDGER'])
    if not path.is_absolute():
        raise ValueError('historical attempt ledger must be absolute')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix('.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            identity = os.environ['TM_HISTORY_PORTION_ID']
            value = json.loads(path.read_text()) if path.exists() else {
                'portion_id': identity, 'attempts': 0,
                'deadline_at': os.environ['TM_HISTORY_DEADLINE_AT'],
                'request_limit': request_limit(),
            }
            if value['portion_id'] != identity or value['deadline_at'] != os.environ['TM_HISTORY_DEADLINE_AT']:
                raise ValueError('historical attempt ledger identity drift')
            if value.get('request_limit', MAX_HISTORY_ATTEMPTS) != request_limit():
                raise ValueError('historical attempt ledger limit drift')
            used = value['attempts']
            if isinstance(used, bool) or not isinstance(used, int) or used < 0:
                raise ValueError('historical attempt ledger is corrupt')
            if used >= request_limit() or remaining_seconds() <= HISTORY_SETTLE_SECONDS:
                raise TrafficBudgetExceeded('historical portion boundary reached')
            value['attempts'] = used + 1
            temporary = path.with_suffix(f'.{os.getpid()}.tmp')
            with temporary.open('w') as handle:
                json.dump(value, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            descriptor = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


@contextmanager
def bounded_trino():
    """Clamp every Trino poll in this isolated process, including ops writes."""
    if not enabled():
        yield
        return
    import trino.dbapi
    from dags.scripts.run_transfermarkt_current import _bound_connection, _portion_alarm
    original = trino.dbapi.connect
    deadline = time.monotonic() + remaining_seconds()

    def connect(*args, **kwargs):
        if time.monotonic() >= deadline - 30:
            raise HistoryContinuation('historical portion reached commit boundary')
        return _bound_connection(original(*args, **kwargs), deadline, time.monotonic)

    trino.dbapi.connect = connect
    try:
        with _portion_alarm(max(.01, remaining_seconds() - 5)):
            yield
    finally:
        trino.dbapi.connect = original
