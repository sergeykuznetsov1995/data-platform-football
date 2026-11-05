"""Settled registry cohorts shared by bounded, independently terminal DAG runs."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from scrapers.transfermarkt.models import LeaseTrafficSnapshot, SharedTrafficLedger, stable_payload_hash
from scrapers.transfermarkt.discovery import PARSER_REVISION, SCHEMA_REVISION

SLOT_HOURS = (3, 5, 7, 11, 13, 15, 19, 21, 23)
SCHEDULE = '0 3,5,7,11,13,15,19,21,23 * * *'
RUN_SECONDS = 55 * 60
WORK_SECONDS = 45 * 60
MANUAL_DAG_ID = "dag_discover_transfermarkt_registry_manual"
DELIVERY_DRAIN_PATH = Path("/opt/airflow/gateway-state/delivery-drain-until")
JOB_PATH = Path('/opt/airflow/logs/transfermarkt-registry/cohorts/active.json')
ACTIVE_STAGES = {'ready', 'running', 'paused', 'source_complete'}


class DiscoveryPortionComplete(RuntimeError):
    """Source remains incomplete: release the slot without publication."""


def ensure_manual_delivery_admission(now: datetime, path: Path | None = None) -> None:
    """A newly starting manual worker must not race the delivery drain."""
    path = DELIVERY_DRAIN_PATH if path is None else path
    try:
        value = path.read_text(encoding='ascii').strip()
    except FileNotFoundError:
        return
    except (OSError, UnicodeError) as exc:
        raise ValueError('manual discovery delivery drain is unreadable') from exc
    if not value.isascii() or not value.isdecimal() or len(value) > 12 or int(value) <= 0:
        raise ValueError('manual discovery delivery drain is invalid')
    if now.timestamp() < int(value):
        raise ValueError('manual discovery cannot start during delivery drain')


def read_job(path: Path) -> dict[str, Any]:
    saved = json.loads(path.read_text())
    if not isinstance(saved, dict):
        raise ValueError('discovery cohort must be an object')
    digest = saved.pop('hash', None)
    if saved.get('version') != 1 or digest != stable_payload_hash(saved):
        raise ValueError('discovery cohort version or integrity differs')
    if not isinstance(saved.get('identity'), dict):
        raise ValueError('discovery cohort identity must be an object')
    if not isinstance(saved.get('status'), str) or saved['status'] not in {
        'ready', 'running', 'paused', 'writing', 'source_complete', 'published', 'failed', 'unsettled'
    }:
        raise ValueError('discovery cohort status is invalid')
    for field in ('job_id', 'checkpoint_path'):
        if not isinstance(saved.get(field), str) or not saved[field]:
            raise ValueError(f'discovery cohort {field} is invalid')
    if type(saved.get('portion')) is not int or saved['portion'] < 0:
        raise ValueError('discovery cohort portion is invalid')
    for field in ('started_at', 'updated_at'):
        value = saved.get(field)
        if not isinstance(value, str) or datetime.fromisoformat(value).tzinfo is None:
            raise ValueError(f'discovery cohort {field} is invalid')
    day = saved['identity'].get('cohort_day')
    if not isinstance(day, str) or datetime.strptime(day, '%Y-%m-%d').date().isoformat() != day:
        raise ValueError('discovery cohort day is invalid')
    for field in ('final_manifest', 'artifacts', 'sizing'):
        if field in saved and not isinstance(saved[field], dict):
            raise ValueError(f'discovery cohort {field} must be an object')
    for field in ('final_manifest', 'artifacts'):
        if any(not isinstance(value, str) for value in saved.get(field, {}).values()):
            raise ValueError(f'discovery cohort {field} values must be strings')
    traffic = saved.get('traffic')
    if traffic is not None:
        if not isinstance(traffic, dict):
            raise ValueError('discovery cohort traffic must be an object')
        entities = traffic.get('by_entity', {})
        if not isinstance(entities, dict) or any(not isinstance(value, dict) for value in entities.values()):
            raise ValueError('discovery cohort traffic entities must be objects')
    return saved


def write_job(path: Path, payload: dict[str, Any], write: Callable) -> None:
    write(path, {**payload, 'hash': stable_payload_hash(payload)})


def cohort_day(now: datetime) -> str:
    # An operational day starts after the delivery quiet window, at 03 UTC.
    return (now.astimezone(timezone.utc) - timedelta(hours=3)).date().isoformat()


def identity_for(*, previous_snapshot_id, mode, policy_hash, request_limit,
                 retry_limit, byte_cap_bytes, day, dry_run=False) -> dict[str, Any]:
    return {'previous_snapshot_id': previous_snapshot_id, 'mode': mode,
            'policy_hash': policy_hash, 'request_limit': request_limit,
            'retry_limit': retry_limit, 'byte_cap_bytes': byte_cap_bytes,
            'cohort_day': day, 'dry_run': bool(dry_run),
            'parser_revision': PARSER_REVISION, 'schema_revision': SCHEMA_REVISION}


def ensure_cohort_age(saved: dict[str, Any], now: datetime) -> None:
    started = datetime.fromisoformat(saved['started_at'])
    if started.tzinfo is None or not timedelta(0) <= now - started < timedelta(days=1):
        raise ValueError('discovery cohort expired or has invalid start time; no budget renewal')


def select_job(path: Path, identity: dict[str, Any], *, now: datetime, write: Callable) -> dict[str, Any]:
    """Keep one grant across slots; a new calendar slot never renews its caps."""
    if path.exists():
        saved = read_job(path)
        if saved['status'] == 'published':
            if saved['identity']['cohort_day'] == identity['cohort_day']:
                if saved.get('final_manifest', {}).get('snapshot_id') != identity['previous_snapshot_id']:
                    raise ValueError('published cohort marker does not match canonical registry')
                return saved
            # Preserve the completed accounting evidence before starting a new day.
            write_job(path.parent / saved['job_id'] / 'completed.json', saved, write)
        else:
            ensure_cohort_age(saved, now)
            expected = {**identity, 'cohort_day': saved['identity']['cohort_day']}
            if saved['identity'] != expected:
                raise ValueError('active discovery cohort registry/policy/parser/caps drift')
            if saved['status'] not in {'ready', 'paused', 'source_complete'}:
                raise ValueError('active discovery cohort has unresolved traffic or writes')
            return saved
    job_id = 'tm-discovery-job-' + stable_payload_hash(identity)[:24]
    saved = {'version': 1, 'job_id': job_id, 'identity': identity,
             'status': 'ready', 'portion': 0, 'traffic': None,
             'started_at': now.isoformat(), 'updated_at': now.isoformat(),
             'checkpoint_path': str(path.parent / job_id / 'checkpoint.json')}
    write_job(path, saved, write)
    return saved


def reservations(now: datetime, path: Path | None = None) -> tuple[tuple[datetime, datetime], ...]:
    """Pure read-only admission: corrupt/missing state reserves only daily bootstrap.

    Never freeze current forever because a registry marker cannot be read. A
    failed or unsettled job releases all future reservations; discovery itself
    remains fail-closed. A published cohort releases the rest of its day's slots.
    """
    now = now.astimezone(timezone.utc)
    path = JOB_PATH if path is None else path
    try:
        saved = read_job(path)
        day = saved['identity']['cohort_day']
        status = saved['status']
        updated = datetime.fromisoformat(saved['updated_at'])
        started = datetime.fromisoformat(saved['started_at'])
        active = (status in ACTIVE_STAGES
                  and timedelta(hours=-1) <= now - updated <= timedelta(days=1)
                  and timedelta(hours=-1) <= now - started < timedelta(days=1))
        completed = status == 'published' and day == cohort_day(now)
    except (OSError, ValueError, TypeError, KeyError):
        active = completed = False
    windows = []
    for offset in (0, 1):
        date = now + timedelta(days=offset)
        for hour in SLOT_HOURS:
            start = date.replace(hour=hour, minute=0, second=0, microsecond=0)
            # The next operational day's first slot always boots its new cohort.
            bootstrap = hour == 3 and cohort_day(start) != (cohort_day(now) if completed else '')
            if active or bootstrap:
                windows.append((start, start + timedelta(seconds=RUN_SECONDS)))
    return tuple(windows)


class DiscoveryContinuation:
    def __init__(self, path: Path, identity: dict[str, Any], ledger: SharedTrafficLedger,
                 write: Callable, *, now: Callable[[], datetime], run_id: str, max_portions: int = 24):
        self.path, self.identity, self.ledger, self.write = path, identity, ledger, write
        self.now, self.run_id = now, run_id
        self.saved = read_job(path)
        ensure_cohort_age(self.saved, now())
        if self.saved['identity'] != identity:
            raise ValueError('discovery continuation identity differs')
        if self.saved['status'] not in {'ready', 'paused'}:
            raise ValueError('discovery continuation is not settled and resumable')
        self.portion = self.saved['portion'] + 1
        if self.portion > max_portions:
            raise ValueError('discovery continuation portion limit exhausted')
        if self.saved['status'] == 'paused':
            for filename, digest in self.saved.get('artifacts', {}).items():
                if hashlib.sha256(Path(filename).read_bytes()).hexdigest() != digest:
                    raise ValueError('discovery continuation checkpoint changed after settlement')
            self._restore(self.saved['traffic'])
        self.save('running')

    def _restore(self, traffic: dict[str, Any]) -> None:
        self.ledger.observe_lease('discovery-previous-portions', LeaseTrafficSnapshot(
            up_bytes=traffic['provider_up_bytes'], down_bytes=traffic['provider_down_bytes']))
        for entity, item in traffic['by_entity'].items():
            for index in range(item['requests']):
                self.ledger.record_attempt(entity=entity,
                    decoded_bytes=item['decoded_bytes'] if index == 0 else 0,
                    provider_up_bytes=item['provider_up_bytes'] if index == 0 else 0,
                    provider_down_bytes=item['provider_down_bytes'] if index == 0 else 0,
                    retry=index < item['retries'], duration_seconds=0)
            if not item['requests']:
                self.ledger.record_unattributed_provider(entity=entity,
                    provider_up_bytes=item['provider_up_bytes'], provider_down_bytes=item['provider_down_bytes'])
            for _ in range(item['cache_hits']):
                self.ledger.record_cache_hit(entity=entity, duration_seconds=0)
            self.ledger.record_duration_overhead(entity=entity, duration_seconds=item['duration_seconds'])
        restored = self.ledger.snapshot()
        for key in ('requests', 'retries', 'provider_up_bytes', 'provider_down_bytes', 'decoded_bytes', 'cache_hits'):
            if restored[key] != traffic[key]:
                raise ValueError(f'discovery continuation traffic differs: {key}')

    def save(self, status: str, **evidence) -> None:
        checkpoint = Path(self.saved['checkpoint_path'])
        artifacts = [checkpoint, checkpoint.with_suffix('.failures.json')]
        self.saved.update(status=status, portion=self.portion, traffic=self.ledger.snapshot(),
                          last_run_id=self.run_id, updated_at=self.now().isoformat(),
                          artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts if p.exists()},
                          **evidence)
        write_job(self.path, self.saved, self.write)


def mark_published(path: Path, *, manifest_hash: str, snapshot_id: str, now: datetime, write: Callable) -> None:
    saved = read_job(path)
    final = saved.get('final_manifest', {})
    if saved['status'] != 'source_complete' or final.get('hash') != manifest_hash or final.get('snapshot_id') != snapshot_id:
        raise ValueError('publication does not match the completed discovery cohort')
    saved.update(status='published', updated_at=now.isoformat(), published_at=now.isoformat())
    write_job(path, saved, write)
