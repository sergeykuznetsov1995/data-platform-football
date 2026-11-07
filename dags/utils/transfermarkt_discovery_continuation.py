"""Settled registry cohorts shared by bounded, independently terminal DAG runs."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from scrapers.transfermarkt.models import LeaseTrafficSnapshot, SharedTrafficLedger, stable_payload_hash
from scrapers.transfermarkt.discovery import PARSER_REVISION, SCHEMA_REVISION

SLOT_HOURS = (3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 19, 21, 23)
SCHEDULE = '0 3,4,5,6,7,11,12,13,14,15,19,21,23 * * *'
RUN_SECONDS = 55 * 60
WORK_SECONDS = 45 * 60
MANUAL_DAG_ID = "dag_discover_transfermarkt_registry_manual"
DELIVERY_DRAIN_PATH = Path("/opt/airflow/gateway-state/delivery-drain-until")
JOB_PATH = Path('/opt/airflow/logs/transfermarkt-registry/cohorts/active.json')
ACTIVE_STAGES = {'ready', 'running', 'paused', 'source_complete'}

# One-time, narrowly pinned migration of a single audited legacy cohort
# stranded in 'running': its run claimed the slot under the old constructor,
# then failed during client setup ('retry_budget differs from the shared
# traffic ledger limit') before any paid I/O, leaving settled accounting
# resumable but the marker rejected. Pins identify the 2026-11-05 failure;
# gateway audit confirmed no failed-run requests and prior bytes match the
# cumulative ledger. See TRANSFERMARKT-DISCOVERY-2145.md for the audit basis.
# This is not configurable permission to recover arbitrary 'running' markers:
# anything that does not match every pin exactly stays rejected, and cohort
# age/identity checks still run first (no expiry bypass).
KNOWN_SETUP_FAILURE_RECOVERY = {
    'marker_hash': '0f04956acac89c73f86cdc6bd883c57cd581b1286577eafed7f85dde465d1afe',
    'paused_manifest': {'path': 'manifests/tm-registry-2f1b84aa92adbce7406f1163/transfermarkt-discovery-93986bc6272bd0b64bf7a658f3df9a22e62ca1614db40acc1b0e2f0ded8702a8.json',
                        'hash': '93986bc6272bd0b64bf7a658f3df9a22e62ca1614db40acc1b0e2f0ded8702a8'},
    'failed_manifest': {'path': 'manifests/tm-registry-8d50231f49f18d786c69e87b/transfermarkt-discovery-2f286ffc967f238b2b0979fb0fb47dfbb507f8f0e65fcee82386281e4cb207bc.json',
                        'hash': '2f286ffc967f238b2b0979fb0fb47dfbb507f8f0e65fcee82386281e4cb207bc'},
}
_RECOVERY_REASON = 'audited legacy setup failure before paid I/O'
_RECOVERY_TRAFFIC_FIELDS = ('requests', 'retries', 'provider_up_bytes',
                            'provider_down_bytes', 'provider_metered_bytes',
                            'decoded_bytes', 'cache_hits')
_RECOVERY_SETUP_ERROR = 'retry_budget differs from the shared traffic ledger limit'
# Counters the aggregate snapshot must prove against by_entity exactly.
_ENTITY_COUNTER_FIELDS = ('requests', 'retries', 'provider_up_bytes',
                          'provider_down_bytes', 'decoded_bytes', 'cache_hits')


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


def cohort_age(saved: dict[str, Any], now: datetime) -> timedelta | None:
    """Signed age of a cohort; None when its start time is unusable."""
    try:
        started = datetime.fromisoformat(saved['started_at'])
    except (TypeError, ValueError):
        return None
    if started.tzinfo is None:
        return None
    return now - started


def cohort_expired(saved: dict[str, Any], now: datetime) -> bool:
    """True only for a genuinely aged-out cohort (>= 24h old).

    An invalid or future start time is not 'expired': it stays fail-closed.
    """
    age = cohort_age(saved, now)
    return age is not None and age >= timedelta(days=1)


def ensure_cohort_age(saved: dict[str, Any], now: datetime) -> None:
    started = datetime.fromisoformat(saved['started_at'])
    if started.tzinfo is None or not timedelta(0) <= now - started < timedelta(days=1):
        raise ValueError('discovery cohort expired or has invalid start time; no budget renewal')


def _settled_traffic_snapshot(traffic: Any) -> bool:
    """True only for a complete, internally consistent cumulative snapshot.

    A paused marker is settled only with the full ledger snapshot: every
    cumulative counter plus the per-entity accounting that `_restore` replays.
    None, a partial object or malformed per-entity entries prove the
    accounting was never completely persisted, so the active marker must not
    be abandoned.

    The snapshot must also prove aggregate/entity consistency: every entity
    key is a non-empty string with the complete counter set, each aggregate
    requests/retries/decoded_bytes/provider_up_bytes/provider_down_bytes/
    cache_hits equals the sum over by_entity, and provider_metered_bytes
    equals aggregate provider_up_bytes + provider_down_bytes.  An empty
    by_entity ledger is therefore valid only when all those aggregate
    counters are zero; any positive aggregate with an empty ledger rejects.
    Per-entity duration_seconds must be finite and non-negative.
    """
    if not isinstance(traffic, dict):
        return False
    for field in _RECOVERY_TRAFFIC_FIELDS:
        value = traffic.get(field)
        if type(value) is not int or value < 0:
            return False
    entities = traffic.get('by_entity')
    if not isinstance(entities, dict):
        return False
    summed = dict.fromkeys(_ENTITY_COUNTER_FIELDS, 0)
    for key, item in entities.items():
        if not isinstance(key, str) or not key:
            return False
        if not isinstance(item, dict):
            return False
        for field in _ENTITY_COUNTER_FIELDS:
            value = item.get(field)
            if type(value) is not int or value < 0:
                return False
            summed[field] += value
        duration = item.get('duration_seconds')
        if (not isinstance(duration, (int, float)) or isinstance(duration, bool)
                or not math.isfinite(duration) or duration < 0):
            return False
    for field in _ENTITY_COUNTER_FIELDS:
        if traffic[field] != summed[field]:
            return False
    if traffic['provider_metered_bytes'] != (
            traffic['provider_up_bytes'] + traffic['provider_down_bytes']):
        return False
    return True


def _archive_expired_paused_cohort(path: Path, saved: dict[str, Any], write: Callable) -> None:
    """Preserve an expired settled cohort's evidence before replacement.

    Only a 'paused' marker whose recorded artifacts still match their settled
    hashes may roll over; missing or drifted evidence fails closed without
    writing.  Before anything is read or written the job_id itself must equal
    'tm-discovery-job-' + stable_payload_hash(identity)[:24], so a changed or
    traversal job_id is rejected up front.  The artifacts are bound to the
    cohort: the checkpoint must be
    exactly <active-marker-parent>/<job_id>/checkpoint.json and the artifact
    set exactly that file plus its .failures.json sibling, both absolute.
    The persisted traffic must be the complete settled snapshot that a
    continuation would restore, not merely present.  The archive keeps the
    marker payload exactly (checkpoint, started_at and cumulative traffic
    untouched); the caller then creates a genuinely new cohort with fresh
    started_at/job_id/checkpoint and traffic=None.  The old marker is never
    renewed or reset.
    """
    # Bind job_id to identity before any artifact read or archive write: a
    # changed or traversal job_id (e.g. '../other') can never redirect the
    # checkpoint binding, artifact reads or the <job_id>/expired.json target.
    expected_job_id = ('tm-discovery-job-'
                       + stable_payload_hash(saved.get('identity'))[:24])
    if saved['job_id'] != expected_job_id:
        raise ValueError('expired paused cohort job_id is not bound to its identity')
    artifacts = saved.get('artifacts')
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError('expired paused cohort has no settled artifact evidence')
    checkpoint = Path(saved['checkpoint_path'])
    expected_checkpoint = path.parent / saved['job_id'] / 'checkpoint.json'
    if not checkpoint.is_absolute() or checkpoint != expected_checkpoint:
        raise ValueError('expired paused cohort checkpoint is not bound to its cohort')
    expected = {str(checkpoint), str(checkpoint.with_suffix('.failures.json'))}
    if (set(artifacts) != expected
            or any(not Path(filename).is_absolute() for filename in artifacts)):
        raise ValueError('expired paused cohort artifacts do not match its checkpoint')
    if not _settled_traffic_snapshot(saved.get('traffic')):
        raise ValueError('expired paused cohort has no complete settled traffic accounting')
    for filename, digest in artifacts.items():
        try:
            actual = hashlib.sha256(Path(filename).read_bytes()).hexdigest()
        except OSError as exc:
            raise ValueError('expired paused cohort artifact is missing or unreadable') from exc
        if actual != digest:
            raise ValueError('expired paused cohort artifact changed after settlement')
    archive = path.parent / saved['job_id'] / 'expired.json'
    # Idempotent replay: a retry after a crash between the archive write and
    # the active-marker write must not rewrite the archive.  An existing
    # archive must parse under the same strict read_job and equal the marker
    # being archived exactly; anything else (corrupt or conflicting content)
    # fails closed and is never overwritten.
    if archive.exists():
        try:
            existing = read_job(archive)
        except (OSError, ValueError) as exc:
            raise ValueError('expired cohort archive is corrupt; refusing to overwrite') from exc
        if existing != saved:
            raise ValueError('expired cohort archive conflicts with the active marker')
        return
    write_job(archive, saved, write)


def _read_recovery_manifest(root: Path, pinned: dict[str, str]) -> dict[str, Any]:
    try:
        envelope = json.loads((root / pinned['path']).read_text())
    except (OSError, ValueError) as exc:
        raise ValueError('recovery manifest is missing or unreadable') from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get('manifest'), dict):
        raise ValueError('recovery manifest differs from the audited pin')
    manifest = envelope['manifest']
    if (envelope.get('manifest_hash') != pinned['hash']
            or stable_payload_hash(manifest) != pinned['hash']):
        raise ValueError('recovery manifest differs from the audited pin')
    return manifest


def _recover_known_setup_failure(path: Path, saved: dict[str, Any], *,
                                 now: datetime, write: Callable) -> None:
    """Re-settle the one audited legacy 'running' marker as 'paused'.

    Every fact is pinned: the marker payload hash binds the expected artifact
    paths and digests, and both manifest envelopes are bound by exact hashes.
    Any missing, corrupt or drifted evidence fails closed without writing.
    """
    descriptor = KNOWN_SETUP_FAILURE_RECOVERY
    checkpoint = Path(saved['checkpoint_path'])
    artifacts = saved.get('artifacts')
    expected = {str(checkpoint), str(checkpoint.with_suffix('.failures.json'))}
    if not isinstance(artifacts, dict) or set(artifacts) != expected:
        raise ValueError('recovery marker artifacts do not match its checkpoint')
    for filename, digest in artifacts.items():
        try:
            actual = hashlib.sha256(Path(filename).read_bytes()).hexdigest()
        except OSError as exc:
            raise ValueError('recovery marker artifact is missing or unreadable') from exc
        if actual != digest:
            raise ValueError('recovery marker checkpoint changed after settlement')
    root = path.parent.parent
    paused = _read_recovery_manifest(root, descriptor['paused_manifest'])
    failed = _read_recovery_manifest(root, descriptor['failed_manifest'])
    traffic = saved.get('traffic') or {}
    paused_traffic = paused.get('traffic') or {}
    if (paused.get('status') != 'paused'
            or paused.get('portion') != saved['portion'] - 1
            or any(paused_traffic.get(field) != traffic.get(field)
                   for field in _RECOVERY_TRAFFIC_FIELDS)):
        raise ValueError('recovery paused evidence does not match the stranded cohort')
    error = failed.get('error') or {}
    if (error.get('type') != 'ValueError'
            or error.get('message') != _RECOVERY_SETUP_ERROR
            or failed.get('source_status') != 'not_started'
            or failed.get('writes') != []
            or (failed.get('write_status') or {}).get('attempted') is not False
            or failed.get('run_id') != saved.get('last_run_id')):
        raise ValueError('recovery failure evidence does not match the stranded cohort')
    # Preserve portion, started_at, identity, traffic and artifacts exactly;
    # only the status, update time and recovery provenance change.
    saved.update(status='paused', updated_at=now.isoformat(),
                 recovery={'reason': _RECOVERY_REASON,
                           'marker_hash': descriptor['marker_hash'],
                           'paused_manifest_hash': descriptor['paused_manifest']['hash'],
                           'failed_manifest_hash': descriptor['failed_manifest']['hash']})
    write_job(path, saved, write)


def select_job(path: Path, identity: dict[str, Any], *, now: datetime, write: Callable) -> dict[str, Any]:
    """Keep one grant across slots; a new calendar slot never renews its caps."""
    # Compute the fresh cohort's job_id once, up front: it is both the
    # collision guard for the expired-paused rollover path below and the id
    # used for the normal fresh cohort creation at the end.
    job_id = 'tm-discovery-job-' + stable_payload_hash(identity)[:24]
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
            try:
                ensure_cohort_age(saved, now)
            except ValueError:
                # An expired 'paused' cohort (only that status, only genuinely
                # aged out) rolls over: its marker is archived verbatim under
                # <job_id>/expired.json after artifact verification, and a
                # fresh cohort is created below.  Every other expired status
                # and invalid/future timestamps fail closed as before.
                if saved['status'] != 'paused' or not cohort_expired(saved, now):
                    raise
                # Structural guard: the expired marker's identity must be a
                # dict with exactly the current identity key set before any
                # artifact read or archive write.  Keys are compared, not
                # values: the expired marker is historical, and the genuinely
                # new cohort intentionally uses current policy/parser/caps.
                if (not isinstance(saved.get('identity'), dict)
                        or set(saved['identity']) != set(identity)):
                    raise ValueError(
                        'expired paused cohort identity has an incomplete key set')
                # Collision guard: if the caller identity hashes to the same
                # job_id as the expired paused cohort, a rollover would archive
                # and then recreate a cohort in the same job_id/checkpoint
                # namespace.  Refuse before any artifact read or archive
                # write, leaving active.json untouched and no expired.json.
                if job_id == saved['job_id']:
                    raise ValueError(
                        'fresh discovery cohort job_id collides with the expired paused cohort')
                _archive_expired_paused_cohort(path, saved, write)
            else:
                expected = {**identity, 'cohort_day': saved['identity']['cohort_day']}
                if saved['identity'] != expected:
                    raise ValueError('active discovery cohort registry/policy/parser/caps drift')
                if (saved['status'] == 'running'
                        and stable_payload_hash(saved) == KNOWN_SETUP_FAILURE_RECOVERY['marker_hash']):
                    _recover_known_setup_failure(path, saved, now=now, write=write)
                    return saved
                if saved['status'] not in {'ready', 'paused', 'source_complete'}:
                    raise ValueError('active discovery cohort has unresolved traffic or writes')
                return saved
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
        self._claimed = False

    def mark_running(self) -> None:
        """Claim the cohort only immediately before paid work.

        Construction validates the settled cohort and restores its cumulative
        traffic, but the setup that follows (admission, lease/client
        construction) may still fail before any paid I/O.  Claiming 'running'
        at construction would strand the settled cohort and block its
        continuation; an explicit claim keeps unknown/unsettled traffic
        fail-closed while a setup failure leaves the cohort resumable.
        """
        if self._claimed:
            raise ValueError('discovery continuation is already running')
        self._claimed = True
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
