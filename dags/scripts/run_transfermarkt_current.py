"""One serialized, bounded portion of the signal-driven current lane.

The local cursor is a recovery journal, not a global career debt queue. Raw
receipts are revalidated before squad continuation; observed signals are only
acknowledged after the writer returns verified Bronze manifest evidence.
"""
from __future__ import annotations

from scrapers.transfermarkt.writer import execute_statement

import fcntl
import argparse
import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import time
import signal
from contextlib import contextmanager
from typing import Any, Mapping
from zoneinfo import ZoneInfo
from urllib.parse import parse_qs, urlsplit

import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / 'dags') not in sys.path:
    sys.path.append(str(ROOT / 'dags'))

from dags.utils.transfermarkt_current_state import (
    CURRENT_POLICY_VERSION, CurrentWork, EditionCheck, ScopeCursor, SignalObservation,
    SignalState, build_check_merge, build_current_state_tables,
    build_signal_merge, build_signals_merge, current_scope_targets, daily_complete, mark_applied,
    mark_seen, mark_failed, plan_current_work, record_check,
)
from dags.utils.transfermarkt_current_timetable import available_work_seconds
from scrapers.transfermarkt.current_capture import (
    ClubRosterSnapshot, FullRosterSnapshot, assemble_full_roster,
    club_listing_signatures, player_changes, semantic_signature,
)
from scrapers.transfermarkt.models import (
    FetchOutcome, FetchStatus, PRODUCTION_ENTITY_BUDGETS, SharedTrafficLedger, stable_payload_hash,
    SCOPE_HARD_PROVIDER_BYTE_CAP, SCOPE_SOFT_PROVIDER_BYTE_STOP,
    SCOPE_REQUEST_LIMIT, SCOPE_RETRY_LIMIT,
)
from scrapers.transfermarkt import tmapi
from scrapers.transfermarkt.client import redact_sensitive


class CurrentPortionError(RuntimeError):
    """An unqualified detector or invalid recovery evidence blocks paid I/O."""


_ENTITY_LABELS = {
    'players': ('listing', 'participants_api', 'teilnehmer', 'squad', 'current_players', 'current_injury'),
    'market_value_history': ('market_value_points', 'mv_history'),
    'transfers': ('transfer_events', 'transfers'),
    'coaches': ('coach_history', 'coach_profile'),
}


def _entity_used(data, scraper, entity, counter):
    current = scraper.get_traffic_stats().get('shared_traffic_ledger', {}).get('by_entity', {})
    previous = data.get('traffic_used_by_entity', {})
    return sum(int(source.get(label, {}).get(counter, 0))
               for source in (previous, current) for label in _ENTITY_LABELS[entity])


def _professional_assignment_changed(previous, signal):
    if 'assignments' not in previous:
        return set(previous.get('club_ids', [])) != set(signal.club_ids)
    def projection(assignments):
        return sorted((str(item[0]), item[1], item[2] or '') for item in assignments
                      if item[1] in {'current', 'additional'})
    return projection(previous['assignments']) != projection(signal.assignments)


def _install_entity_limits(scraper, data):
    """The native per-player readers may reset operation limits repeatedly."""
    def begin(operation):
        budget = PRODUCTION_ENTITY_BUDGETS[operation]
        decoded = int(budget['decoded_mb'] * 1024 * 1024) - _entity_used(data, scraper, operation, 'decoded_bytes')
        requests = int(budget['requests']) - _entity_used(data, scraper, operation, 'requests')
        if decoded <= 0 or requests <= 0:
            from scrapers.transfermarkt.models import TrafficBudgetExceeded
            raise TrafficBudgetExceeded(f'{operation} portion entity budget exhausted')
        scraper._http_client.set_decoded_body_budget(decoded)
        scraper._http_client.begin_request_scope(request_attempt_budget=requests)
    scraper._begin_operation_budget = begin


class _CurrentLedger(SharedTrafficLedger):
    """One scope's portion grant survives per-entity HTTP budget resets."""
    def __init__(self, used):
        provider = int(used.get('provider_metered_bytes', 0))
        super().__init__(hard_provider_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP - provider,
                         soft_provider_bytes=SCOPE_SOFT_PROVIDER_BYTE_STOP - provider,
                         retry_limit=SCOPE_RETRY_LIMIT - int(used.get('retries', 0)))
        self.remaining_attempts = SCOPE_REQUEST_LIMIT - int(used.get('requests', 0))

    def ensure_request_allowed(self, *, retry=False):
        super().ensure_request_allowed(retry=retry)
        if self.snapshot()['requests'] >= self.remaining_attempts:
            from scrapers.transfermarkt.models import TrafficBudgetExceeded
            raise TrafficBudgetExceeded('current scope attempt budget exhausted within portion')


def _utc(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None:
        raise CurrentPortionError('timezone-aware timestamp required')
    return parsed.astimezone(timezone.utc)


def _atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(value, sort_keys=True, allow_nan=False,
                      default=lambda item: item.isoformat()).encode()
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    directory_fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class _CurrentStore:
    """Light cursor index and independently durable scope/packet journals."""
    STORAGE_VERSION = 2

    def __init__(self, directory, started, now):
        self.directory, self.started = Path(directory), started
        self.now = now
        self.index_path = self.directory / 'cursor-v1.json'
        if self.index_path.exists():
            self.index = json.loads(self.index_path.read_text())
        else:
            self.index = {'version': CURRENT_POLICY_VERSION, 'storage_version': self.STORAGE_VERSION,
                          'scopes': {}, 'turn': 0}
        if self.index.get('version') != CURRENT_POLICY_VERSION:
            raise CurrentPortionError('current cursor policy migration required')
        version = self.index.get('storage_version', 1)
        if version == 1:
            # The first implementation was never activated before detector
            # qualification. Still preserve its local, raw-backed receipts.
            for scope_id, data in list(self.index['scopes'].items()):
                if data.get('cursor'):
                    ScopeCursor.from_mapping(data['cursor'])
                relative = self.scope_path(scope_id)
                self._strip_cache(data)
                _atomic(self.directory / relative, data)
                self.index['scopes'][scope_id] = {'scope_file': relative, 'cursor': data.get('cursor')}
            self.index.pop('player_facts', None)
            self.index['storage_version'] = self.STORAGE_VERSION
            _atomic(self.index_path, self.index)
        elif version != self.STORAGE_VERSION:
            raise CurrentPortionError('unsupported current storage version')
        self.active_scope_id, self.active_data = None, None
        self.player_facts = {}
        invalidations = {}
        self.fact_directory = self.directory / 'player-facts' / started.astimezone(ZoneInfo('Europe/Moscow')).date().isoformat()
        if self.fact_directory.exists():
            for path in sorted(self.fact_directory.glob('*.json')):
                packet = json.loads(path.read_text())
                if packet.get('version') != CURRENT_POLICY_VERSION:
                    raise CurrentPortionError('global player packet journal version differs')
                if packet.get('invalidated_player'):
                    invalidations[packet['invalidated_player']] = _utc(packet['invalidated_at'])
                    continue
                for player_id, fact in packet['facts'].items():
                    fetched = _utc(fact['raw_fetched_at'])
                    if not timedelta(0) <= started - fetched < timedelta(hours=20):
                        continue
                    previous = self.player_facts.get(player_id)
                    if previous is None or fetched > _utc(previous['raw_fetched_at']):
                        self.player_facts[player_id] = fact
        for player_id, at in invalidations.items():
            fact = self.player_facts.get(player_id)
            if fact is not None and _utc(fact['raw_fetched_at']) <= at:
                self.player_facts.pop(player_id)

    @staticmethod
    def scope_path(scope_id):
        return 'scopes/' + hashlib.sha256(scope_id.encode()).hexdigest() + '.json'

    def activate(self, scope_id):
        self.active_scope_id = scope_id
        relative = self.scope_path(scope_id)
        entry = self.index['scopes'].get(scope_id)
        if entry is not None and entry['scope_file'] != relative:
            raise CurrentPortionError('scope journal path is not its content-addressed identity')
        path = self.directory / relative
        if entry is not None and not path.exists():
            raise CurrentPortionError('scope journal referenced by cursor index is missing')
        self.active_data = json.loads(path.read_text()) if path.exists() else {}
        return self.active_data

    def _strip_cache(self, data):
        cache = data.get('response_cache', {})
        for url, entry in list(cache.items()):
            outcome = entry.get('outcome', {})
            label = outcome.get('label')
            fetched = _utc(outcome['raw_fetched_at']) if outcome.get('raw_fetched_at') else None
            age = self.now() - fetched if fetched else None
            ttl = timedelta(hours=48) if label == 'squad' else timedelta(hours=24)
            if fetched is None or age < timedelta(0) or age >= ttl:
                cache.pop(url)
                continue
            if label in _ENTITY_LABELS['players'] and label != 'squad' and (
                fetched.astimezone(ZoneInfo('Europe/Moscow')).date()
                    != self.now().astimezone(ZoneInfo('Europe/Moscow')).date()):
                cache.pop(url)
                continue
            # v4 can replay missing decoded values from its verified raw body.
            # Keeping the body here would multiply HTML by every fsync.
            outcome.pop('value', None)

    def save(self):
        if self.active_scope_id is not None:
            self._strip_cache(self.active_data)
            relative = self.scope_path(self.active_scope_id)
            _atomic(self.directory / relative, self.active_data)
            self.index['scopes'][self.active_scope_id] = {
                'scope_file': relative, 'cursor': self.active_data.get('cursor'),
                'denominator_hash': self.active_data.get('denominator_hash')}
        _atomic(self.index_path, self.index)

    def save_player_packet(self, facts, raw_capture_id):
        filename = hashlib.sha256(raw_capture_id.encode()).hexdigest() + '.json'
        fetched = _utc(next(iter(facts.values()))['raw_fetched_at'])
        directory = self.directory / 'player-facts' / fetched.astimezone(ZoneInfo('Europe/Moscow')).date().isoformat()
        _atomic(directory / filename, {'version': CURRENT_POLICY_VERSION, 'facts': facts})

    def invalidate_player_fact(self, player_id, at):
        directory = self.directory / 'player-facts' / at.astimezone(ZoneInfo('Europe/Moscow')).date().isoformat()
        path = directory / ('invalidated-' + player_id + '.json')
        _atomic(path, {'version': CURRENT_POLICY_VERSION, 'invalidated_player': player_id,
                       'invalidated_at': at.isoformat()})


def validate_qualification(qualification):
    """No default admission while the measured tmapi detector is unaccepted."""
    from scrapers.transfermarkt.signal_qualification import validate_qualification as qualify, QualificationError
    try:
        return qualify(qualification)
    except (QualificationError, OSError, ValueError) as exc:
        raise CurrentPortionError('tmapi detector qualification is pending or invalid: ' + str(exc)) from exc


def _production_factory(target, row, cache, deadline, used):
    """Use the existing lease/raw-first backend and its standing authorization."""
    from dags.scripts.run_transfermarkt_scope_cycle import validate_standing_policy_for_scope_cycle
    from dags.utils.transfermarkt_approval import load_standing_policy
    from dags.utils.transfermarkt_scope_planner import _competition_from_joined_row
    from scrapers.transfermarkt.scraper import TransfermarktScraper

    if os.environ.get('TM_STANDING_POLICY_ENABLED', '').lower() not in {'true', '1'}:
        raise CurrentPortionError('current lane requires the standing policy gate')
    if not os.environ.get('TM_PROXY_CONTROL_URL'):
        raise CurrentPortionError('current lane requires metered proxy leases')
    if os.environ.get('TRANSFERMARKT_REQUIRE_RAW_STORE', '').lower() not in {'true', '1'}:
        raise CurrentPortionError('current lane requires immutable raw storage')
    path = os.environ.get('TM_STANDING_POLICY_PATH', '/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json')
    policy = load_standing_policy(path)
    validate_standing_policy_for_scope_cycle(
        policy, write_mode=os.environ.get('TM_WRITE_MODE', 'dual'),
        cycle_budget_bytes=SCOPE_HARD_PROVIDER_BYTE_CAP,
        request_limit=SCOPE_REQUEST_LIMIT, retry_limit=SCOPE_RETRY_LIMIT,
    )
    allowed = {table.split('.')[-1] for table in policy.allowed_write_tables}
    if not {'transfermarkt_current_signals_v1', 'transfermarkt_current_checks_v1', 'transfermarkt_season_close_v1'} <= allowed:
        raise CurrentPortionError('standing policy does not authorize current ops tables')
    remaining = SCOPE_HARD_PROVIDER_BYTE_CAP - used.get('provider_metered_bytes', 0)
    soft = SCOPE_SOFT_PROVIDER_BYTE_STOP - used.get('provider_metered_bytes', 0)
    if soft <= 0 or used.get('requests', 0) >= SCOPE_REQUEST_LIMIT or used.get('retries', 0) >= SCOPE_RETRY_LIMIT:
        raise CurrentPortionError('unfinished scope reached its paid traffic cap')
    ledger = _CurrentLedger(used)
    scraper = TransfermarktScraper(
        leagues=[target.competition_id], seasons=[int(target.edition_id)],
        canonical_season=row['canonical_season'],
        competition_records=(_competition_from_joined_row(row),),
        response_cache=cache, resume_squad_cache=True, cache_ttl_seconds=86400,
        request_deadline_monotonic=deadline, traffic_ledger=ledger,
        lease_metadata={'dag_id': 'dag_ingest_transfermarkt', 'run_id': os.environ.get('TM_CHILD_CYCLE_ID', ''),
                        'task_id': 'current_portion', 'scope': target.scope_id},
    )
    return scraper


def _sql(scraper, statement):
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        execute_statement(cursor, statement)
        # Trino writes finish only after the response has been consumed.
        if hasattr(cursor, 'fetchall'):
            cursor.fetchall()
    finally:
        cursor.close()
        connection.close()


def _load_ops_signals(scraper, scope_id):
    from dags.utils.transfermarkt_current_state import SIGNALS_TABLE
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        execute_statement(cursor, f"SELECT * FROM {SIGNALS_TABLE} WHERE scope_id = '" + scope_id.replace("'", "''") + "'")
        rows = cursor.fetchall()
        names = [column[0] for column in cursor.description] if rows else []
        parsed = []
        for row in rows:
            value = dict(zip(names, row))
            # SQL timestamp(6) stores UTC without a timezone in this contract;
            # DB-API can return naive datetime values for those columns.
            for name in ('first_detected_at', 'last_checked_at', 'raw_fetched_at', 'committed_at'):
                if isinstance(value.get(name), datetime) and value[name].tzinfo is None:
                    value[name] = value[name].replace(tzinfo=timezone.utc)
            parsed.append(SignalState.from_ops_row(value))
        return parsed
    finally:
        cursor.close()
        connection.close()


def _existing_bronze_roster(scraper, competition_id, edition_id):
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        execute_statement(cursor, 'SELECT 1 FROM iceberg.bronze.transfermarkt_squad_memberships '
                       'WHERE competition_id = ? AND edition_id = ? LIMIT 1',
                       (competition_id, edition_id))
        return bool(cursor.fetchall())
    except Exception as exc:
        if any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist')):
            return False
        raise CurrentPortionError('existing Bronze baseline cannot be read') from exc
    finally:
        cursor.close()
        connection.close()


def seed_current_baseline(scraper, entry, preflight, verify_complete_scope):
    """Qualify an activation bundle with read-only physical/ops evidence.

    This is invoked by the qualified stock runtime, never a paid collection or
    a Bronze writer. Daily checks remain due; an old complete scope avoids a
    second cold career crawl. Partial scopes require their separate resume
    evidence and cannot be represented as a complete seed.
    """
    from dags.scripts.run_transfermarkt_scope_cycle import EXPECTED_ENTITIES
    from dags.utils.transfermarkt_scope_state import ScopeManifest
    from dags.utils.transfermarkt_current_write import verify_current_roster_baseline
    manifest = ScopeManifest.from_mapping(entry['scope_manifest'])
    manifest.validate(EXPECTED_ENTITIES)
    if manifest.dq_evidence.get('career_fetches_pending') != 0:
        raise CurrentPortionError('baseline_required: previous scope still has unverified career remainder')
    snapshot = FullRosterSnapshot.from_mapping(entry['snapshot'])
    if snapshot.scope_id != manifest.scope_id:
        raise CurrentPortionError('baseline_required: snapshot belongs to another complete scope')
    if verify_complete_scope is None:
        raise CurrentPortionError('baseline_required: complete-scope physical verifier is unavailable')
    complete_proof = verify_complete_scope(scraper, manifest, entry, preflight)
    digest, at = _proof(complete_proof)
    if digest != manifest.digest:
        raise CurrentPortionError('baseline_required: complete-scope proof differs from immutable manifest')
    receipts = entry['roster_receipts']
    roster_proof = verify_current_roster_baseline(scraper, snapshot, receipts, preflight)
    _, roster_at = _proof(roster_proof)
    signals = {}
    from scrapers.transfermarkt.scraper import _parse_squad_page
    store = scraper._http_client._raw_store
    for club in snapshot.clubs.values():
        body, raw = store.load_capture(club.raw_capture_id)
        if (raw.url != club.source_url or raw.scope_id != snapshot.scope_id
            or hashlib.sha256(body).hexdigest() != club.source_body_hash
            or semantic_signature(_parse_squad_page(body.decode('utf-8'), club.club_id))
                != semantic_signature([dict(row) for row in club.rows])):
            raise CurrentPortionError('baseline_required: warm roster differs from immutable full raw')
    for key, value in entry.get('signals', {}).items():
        state = SignalState.from_mapping(value)
        observation = state.observation
        if state.pending or observation.scope_id != snapshot.scope_id or observation.entity not in {'club', 'listing'}:
            raise CurrentPortionError('baseline_required: imported signature is unverified')
        if key != f'{observation.entity}:{observation.entity_id}':
            raise CurrentPortionError('baseline_required: signature key differs from source identity')
        body, raw = store.load_capture(observation.raw_capture_id)
        if (raw.scope_id != snapshot.scope_id or raw.url != observation.source_url
            or hashlib.sha256(body).hexdigest() != observation.source_body_hash):
            raise CurrentPortionError('baseline_required: listing signature has foreign raw ownership')
        clubs = club_listing_signatures(body.decode('utf-8'))
        signature = (clubs.get(observation.entity_id, {}).get('signature') if observation.entity == 'club'
                     else semantic_signature(sorted(snapshot.expected_team_ids)))
        if signature != observation.signature:
            raise CurrentPortionError('baseline_required: normalized listing signature differs')
        if observation.entity == 'club' and snapshot.clubs[observation.entity_id].signature != signature:
            raise CurrentPortionError('baseline_required: club snapshot generation differs from listing proof')
        signals[key] = state.as_dict()
    if {state['observation']['entity_id'] for state in signals.values()
        if state['observation']['entity'] == 'club'} != set(snapshot.expected_team_ids):
        raise CurrentPortionError('baseline_required: every retained club needs a verified listing signature')
    pinned_receipts = []
    for receipt in receipts:
        pins = {club_id: {'raw_capture_id': club.raw_capture_id, 'source_body_hash': club.source_body_hash,
                         'rows_signature': semantic_signature([dict(row) for row in club.rows])}
                for club_id, club in snapshot.clubs.items() if club.bronze_manifest == receipt['bronze_manifest']}
        pinned_receipts.append({**dict(receipt), 'club_receipts': pins})
    career_value_dates = {}
    ids = sorted({row['player_id'] for row in snapshot.rows}, key=int)
    connection = scraper._bronze_connection()
    cursor = connection.cursor()
    try:
        for offset in range(0, len(ids), 300):
            packet = ids[offset:offset + 300]
            placeholders = ','.join('?' for _ in packet)
            execute_statement(cursor, 'SELECT player_id, MAX(mv_date) FROM iceberg.bronze.transfermarkt_market_value_points '
                           f'WHERE player_id IN ({placeholders}) GROUP BY player_id', packet)
            for player_id, latest in cursor.fetchall():
                if latest is not None:
                    career_value_dates[str(player_id)] = str(latest)
    except Exception as exc:
        if not any(marker in str(exc).lower() for marker in ('table_not_found', 'table not found', 'does not exist')):
            raise CurrentPortionError('baseline_required: existing value dates cannot be verified') from exc
    finally:
        cursor.close()
        connection.close()
    return {'snapshot': snapshot.as_dict(), 'roster_receipts': pinned_receipts, 'roster_proof': roster_proof,
            'signals': signals, 'coaches_checked_at': at.isoformat(),
            'career_value_dates': career_value_dates, 'warm_baseline': True,
            'bootstrap_complete_proof': dict(complete_proof),
            'cursor': ScopeCursor(snapshot.scope_id, generation='bootstrap-' + manifest.digest,
                                  cold_complete=True, roster_completed_at=roster_at).as_dict()}


def _proof(value):
    if (not isinstance(value, Mapping) or value.get('verified') is not True
        or not value.get('bronze_manifest') or not value.get('committed_at')):
        raise CurrentPortionError('writer did not verify a committed Bronze manifest')
    return str(value['bronze_manifest']), _utc(value['committed_at'])


def _bound_connection(connection, deadline, clock):
    """Clamp every Trino HTTP poll, preserving this connection's auth/session."""
    remaining = deadline - clock() - 30
    if remaining <= 0:
        raise CurrentPortionError('deadline prohibits a new Bronze request')
    configured = getattr(connection, 'request_timeout', getattr(connection, '_request_timeout', 30))
    configured = max(configured) if isinstance(configured, tuple) else configured
    connection.request_timeout = min(float(configured), remaining)
    connection._request_timeout = connection.request_timeout
    connection.max_attempts = 1
    session = getattr(connection, '_http_session', None)
    if session is not None:
        request = session.request

        def bounded_request(method, url, **kwargs):
            left = deadline - clock() - 30
            if left <= 0:
                raise CurrentPortionError('deadline prohibits another Trino query poll')
            timeout = kwargs.get('timeout', connection.request_timeout)
            if isinstance(timeout, tuple):
                kwargs['timeout'] = tuple(min(float(value), left / len(timeout)) if value is not None
                                         else left / len(timeout) for value in timeout)
            else:
                kwargs['timeout'] = min(float(timeout), left) if timeout is not None else left
            return request(method, url, **kwargs)

        session.request = bounded_request
    return connection


class _Scope:
    def __init__(self, target, row, data, scraper, cycle_id, denominator_hash, save, now, clock, deadline,
                 preflight, roster_writer, career_writer, coach_writer, player_facts, save_player_packet,
                 baseline_verifier, invalidate_player_fact):
        self.target, self.row, self.data, self.scraper = target, row, data, scraper
        self.cycle_id, self.denominator_hash = cycle_id, denominator_hash
        self.save, self.now, self.clock, self.deadline = save, now, clock, deadline
        self.preflight = preflight
        self.player_facts = player_facts
        self.save_player_packet = save_player_packet
        self.invalidate_player_fact = invalidate_player_fact
        self.roster_writer, self.career_writer, self.coach_writer = roster_writer, career_writer, coach_writer
        self.cursor = ScopeCursor.from_mapping(data['cursor']) if 'cursor' in data else ScopeCursor(target.scope_id)
        self.scope = scraper._resolve_scope(target.competition_id, target.edition_id)
        self.signals = {key: SignalState.from_mapping(value) for key, value in data.get('signals', {}).items()}
        for state in _load_ops_signals(scraper, target.scope_id):
            key = f'{state.observation.entity}:{state.observation.entity_id}'
            if key not in self.signals or state.observation.checked_at >= self.signals[key].observation.checked_at:
                self.signals[key] = state
        self.resume = json.loads(self.cursor.resume_json)
        self.snapshot = FullRosterSnapshot.from_mapping(data['snapshot']) if data.get('snapshot') else None
        if self.snapshot is not None:
            from scrapers.transfermarkt.scraper import _parse_squad_page
            receipts = data.get('roster_receipts', [])
            for club in self.snapshot.clubs.values():
                raw, receipt = scraper._http_client._raw_store.load_capture(club.raw_capture_id)
                if (hashlib.sha256(raw).hexdigest() != club.source_body_hash
                    or receipt.scope_id != target.scope_id or receipt.url != club.source_url
                    or _utc(receipt.fetched_at) != club.raw_fetched_at
                    or semantic_signature(_parse_squad_page(raw.decode('utf-8'), club.club_id))
                        != semantic_signature([dict(row) for row in club.rows])):
                    raise CurrentPortionError('retained full roster differs from verified raw capture')
                matching = [proof for proof in receipts if proof.get('bronze_manifest') == club.bronze_manifest
                            and proof.get('club_receipts', {}).get(club.club_id, {}).get('raw_capture_id') == club.raw_capture_id]
                if not matching or not all(_proof(proof)[0] == club.bronze_manifest for proof in matching):
                    raise CurrentPortionError('retained club has no structured committed Bronze receipt')
            if baseline_verifier is not None and not data.get('roster_write_intent'):
                _proof(baseline_verifier(scraper, self.snapshot, receipts, preflight))
        if data.get('roster_write_intent'):
            self._recover_roster_write()

    def initialize_resume(self):
        if not self.cursor.generation:
            self.cursor = replace(self.cursor, generation=self.cycle_id)
        self.resume.setdefault('clubs', self.data.get('listing_clubs', {}))
        if self.snapshot is not None:
            self.resume.setdefault('squad_year', self.snapshot.squad_saison_id)

    def persist(self):
        for endpoint, ids in self.resume.get('careers', {}).items():
            self.resume['careers'][endpoint] = sorted(set(ids), key=int)
        self.cursor = replace(self.cursor, resume_json=json.dumps(self.resume, sort_keys=True,
                                                               default=lambda value: value.isoformat()))
        self.data['cursor'] = self.cursor.as_dict()
        self.data['signals'] = {key: value.as_dict() for key, value in self.signals.items()}
        if self.snapshot is not None:
            self.data['snapshot'] = self.snapshot.as_dict()
        self.save()

    def admission(self, reserve=60):
        if self.clock() + reserve >= self.deadline:
            raise CurrentPortionError('portion deadline: pending work retained')
        used = self.data.get('traffic_used', {})
        current = self.scraper.get_traffic_stats()
        total = used.get('requests', 0) + current.get('request_attempts', current.get('requests', 0))
        if total >= SCOPE_REQUEST_LIMIT:
            raise CurrentPortionError('scope attempt budget reached')

    def fetch(self, url, label, *, as_json=False, fresh=True, generation=None):
        self.admission()
        refresh = self.data.get('recovery_signal_refresh_required')
        epoch = refresh.get('reconciled_at') if isinstance(refresh, Mapping) else None
        fresh_generation = semantic_signature([self.now().astimezone(ZoneInfo('Europe/Moscow')).date().isoformat(),
                                               generation or self.cycle_id, epoch])
        self.scraper._cache_generation_by_url[url] = fresh_generation if fresh else (generation or self.cursor.generation)
        self.scraper._begin_operation_budget('players')
        outcome = self.scraper._fetch_endpoint_outcome(
            url, as_json=as_json, label=label,
            context={'scope': self.target.scope_id, 'cycle_id': self.scraper._current_child_cycle_id,
                     'competition_id': self.target.competition_id,
                     'edition_id': self.target.edition_id,
                     'traffic_reason': 'signal' if fresh else self.resume.get('reason', 'continuation')},
        )
        if outcome.status != FetchStatus.OK or not outcome.raw_capture_id or not outcome.raw_fetched_at or not outcome.raw_body_hash:
            raise CurrentPortionError(f'{label} has no successful raw-first evidence')
        # Verify the durable receipt, rather than trusting mutable cache JSON.
        raw, receipt = self.scraper._http_client._raw_store.load_capture(outcome.raw_capture_id)
        if (receipt.url != url or receipt.scope_id != self.target.scope_id
            or hashlib.sha256(raw).hexdigest() != outcome.raw_body_hash
            or _utc(receipt.fetched_at) != _utc(outcome.raw_fetched_at)):
            raise CurrentPortionError('source response lineage differs from durable raw receipt')
        return outcome

    def observe(self, entity, entity_id, signature, outcome, url, *, baseline_proof=None, checked_at=None,
                persist=True, write=True):
        key = f'{entity}:{entity_id}'
        observation = SignalObservation(
            self.target.scope_id, entity, str(entity_id), signature, checked_at or self.now(), CURRENT_POLICY_VERSION,
            outcome.raw_capture_id, outcome.raw_fetched_at, url, outcome.raw_body_hash,
        )
        if key not in self.signals and baseline_proof is not None:
            manifest, committed_at = _proof(baseline_proof)
            self.signals[key] = SignalState(observation=observation, applied_signature=signature,
                first_detected_at=min(committed_at, observation.checked_at), status='applied', result='baseline_verified',
                bronze_manifest=manifest, committed_at=committed_at)
        else:
            self.signals[key] = mark_seen(self.signals.get(key), observation)
        if write:
            _sql(self.scraper, build_signal_merge(self.signals[key]))
        if persist:
            self.persist()
        return self.signals[key]

    def check(self, kind, raws, expected, observed, *, status='complete', result='checked', checked_at=None):
        check = EditionCheck(self.target.scope_id, kind, self.cycle_id,
                             self.target.registry_snapshot_id, self.denominator_hash, checked_at or self.now(),
                             status, result, tuple(raws), expected, observed)
        _sql(self.scraper, build_check_merge(check))
        self.data.setdefault('checks', {})[kind] = check.as_dict()
        if checked_at is None:
            self.cursor = record_check(self.cursor, check)
        else:
            changes = {'last_work_at': self.now()}
            if status == 'complete':
                changes[f'{kind}_checked_at'] = check.checked_at
            self.cursor = replace(self.cursor, **changes)
        self.persist()

    def _player_fact(self, player_id, packets):
        """Reuse a global fact with explicit primary-scope raw ownership."""
        fact = self.player_facts.get(player_id)
        if fact is None:
            return None
        fetched = _utc(fact['raw_fetched_at'])
        refresh = self.data.get('recovery_signal_refresh_required')
        if isinstance(refresh, Mapping) and fetched < _utc(refresh['reconciled_at']):
            return None
        if (not timedelta(0) <= self.now() - fetched < timedelta(hours=20)
            or fetched.astimezone(ZoneInfo('Europe/Moscow')).date()
                != self.now().astimezone(ZoneInfo('Europe/Moscow')).date()):
            return None
        capture_id = fact['raw_capture_id']
        if capture_id not in packets:
            store = self.scraper._http_client._raw_store
            body, receipt = store.load_capture(capture_id)
            if (receipt.scope_id != fact['primary_scope_id'] or receipt.url != fact['source_url']
                or receipt.content_hash != fact['source_body_hash']
                or receipt.status_code != 200 or _utc(receipt.fetched_at) != fetched
                or hashlib.sha256(body).hexdigest() != fact['source_body_hash']):
                raise CurrentPortionError('global player fact differs from primary raw proof')
            ids = parse_qs(urlsplit(receipt.url).query).get('ids[]', [])
            parsed = tmapi.parse_player_signals(json.loads(body.decode('utf-8')), expected_ids=ids)
            envelopes = []
            for envelope_id in fact['raw_attempt_envelope_ids']:
                envelope = store.load_attempt_envelope(envelope_id)
                if (envelope.scope_id != receipt.scope_id or envelope.cycle_id != receipt.cycle_id
                    or envelope.url != receipt.url or envelope.endpoint != receipt.endpoint):
                    raise CurrentPortionError('global player fact attempt ownership differs')
                envelopes.append(envelope)
            if not envelopes or envelopes[-1].capture_id != capture_id:
                raise CurrentPortionError('global player fact lacks immutable successful attempt')
            for envelope in envelopes:
                self.scraper._http_client._cache_sources[envelope.envelope_id] = envelope
            self.scraper._http_client._record_cache_hit(label='current_players', duration_seconds=0)
            packets[capture_id] = (parsed, receipt)
        parsed, receipt = packets[capture_id]
        if player_id not in parsed or parsed[player_id].signature != fact['signature']:
            raise CurrentPortionError('cached global player fields differ from primary raw payload')
        outcome = FetchOutcome(status=FetchStatus.OK, cache_hit=True, raw_capture_id=capture_id,
            raw_body_hash=receipt.content_hash, raw_fetched_at=receipt.fetched_at,
            raw_uri=receipt.raw_uri, raw_attempt_envelope_ids=tuple(fact['raw_attempt_envelope_ids']))
        return parsed[player_id], outcome, receipt.url, fact['primary_scope_id']

    def _save_player_packet(self, parsed, outcome, url):
        facts = {}
        for player_id, signal in parsed.items():
            facts[player_id] = {
                'signature': signal.signature, 'raw_capture_id': outcome.raw_capture_id,
                'raw_fetched_at': outcome.raw_fetched_at, 'source_url': url,
                'source_body_hash': outcome.raw_body_hash, 'primary_scope_id': self.target.scope_id,
                'raw_attempt_envelope_ids': list(outcome.raw_attempt_envelope_ids),
            }
        self.save_player_packet(facts, outcome.raw_capture_id)
        self.player_facts.update(facts)
        self.persist()

    def listing(self):
        from scrapers.transfermarkt.scraper import (
            _competition_listing_url, _uses_participant_api, _TM_BASE,
            _COMPETITION_PARTICIPANTS_PATH, _parse_participant_table,
            _participant_page_is_for, _participant_page_is_empty,
            _listing_page_is_empty_shell,
        )
        from scrapers.transfermarkt.season import season_to_saison_id
        competition = self.scope['record']
        self.initialize_resume()
        raws = []
        if _uses_participant_api(competition):
            year = season_to_saison_id(self.scope['canonical_season'], competition.season_format)
            self.scope['saison_id'] = year
            api_url = tmapi.competition_clubs_url(self.target.competition_id, year)
            api, ids = None, None
            try:
                api = self.fetch(api_url, 'participants_api', as_json=True)
                ids = tmapi.parse_competition_clubs(api.value, competition_id=self.target.competition_id, saison_id=year)
            except (CurrentPortionError, tmapi.TmapiSchemaError):
                pass  # the existing /teilnehmer/ reserve remains usable
            url = _TM_BASE + _COMPETITION_PARTICIPANTS_PATH.format(
                competition_slug=competition.slug, competition_id=self.target.competition_id, year=year)
            outcome = self.fetch(url, 'teilnehmer')
            if not _participant_page_is_for(outcome.value, self.target.competition_id, year):
                raise CurrentPortionError('participant HTML does not prove edition')
            parsed = _parse_participant_table(outcome.value)
            page_ids = {str(club['club_id']) for club in parsed}
            self.data['participant_evidence'] = {
                'tmapi_ids': list(ids) if ids is not None else None, 'teilnehmer_ids': sorted(page_ids),
                'tmapi_only': sorted(set(ids or ()) - page_ids), 'teilnehmer_only': sorted(page_ids - set(ids or ())),
                'saison_id': year,
            }
            if ids == () and not parsed and _participant_page_is_empty(outcome.value, self.target.competition_id, year):
                if (self.snapshot is not None or self.scraper._bronze_scope_has_roster(
                        self.scope['compatibility_league'], self.scope['canonical_season'])):
                    raise CurrentPortionError('empty participants would erase an existing roster')
                raws = [api.raw_capture_id, outcome.raw_capture_id]
                self.data['authoritative_empty_scope'] = {'raw_capture_ids': raws,
                                                         'checked_at': self.now().isoformat()}
                self.resume = {}
                self.cursor = replace(self.cursor, cold_complete=True, roster_completed_at=self.now())
                self.check('listing', raws, 0, 0, result='tmapi+teilnehmer authoritative empty')
                self.check('players', raws, 0, 0, result='no participants; no player denominator')
                return
            self.data.pop('authoritative_empty_scope', None)
            if not ids:
                ids = tuple(sorted(page_ids, key=int))
            # The API may identify the main draw while HTML includes qualifiers.
            # Select API membership, but require semantic club rows for every ID.
            from bs4 import BeautifulSoup
            semantic = {}
            # Participant pages such as BRC split qualifiers and the main
            # draw over several items tables. Keep every proved API member.
            for table in BeautifulSoup(outcome.value, 'html.parser').find_all('table', class_='items'):
                for club_id, value in club_listing_signatures(str(table)).items():
                    if club_id in semantic and semantic[club_id] != value:
                        raise CurrentPortionError('participant tables disagree on club signature')
                    semantic[club_id] = value
            clubs = {str(club['club_id']): club for club in parsed if str(club['club_id']) in ids}
            if not ids or set(clubs) != set(ids) or not set(ids) <= set(semantic):
                raise CurrentPortionError('participants lack full club signal evidence')
            clubs = {club_id: {**club, 'signature': semantic[club_id]['signature']} for club_id, club in clubs.items()}
            if api is not None:
                raws.append(api.raw_capture_id)
        else:
            year = int(self.target.edition_id)
            url = _competition_listing_url(competition, self.target.edition_id)
            outcome = self.fetch(url, 'listing')
            if _listing_page_is_empty_shell(outcome.value, self.target.competition_id):
                if (self.snapshot is not None or self.scraper._bronze_scope_has_roster(
                        self.scope['compatibility_league'], self.scope['canonical_season'])):
                    raise CurrentPortionError('empty listing would erase an existing roster')
                raws = [outcome.raw_capture_id]
                self.data['authoritative_empty_scope'] = {'raw_capture_ids': raws,
                                                         'checked_at': self.now().isoformat()}
                self.resume = {}
                self.cursor = replace(self.cursor, cold_complete=True, roster_completed_at=self.now())
                self.check('listing', raws, 0, 0, result='authoritative empty source listing')
                self.check('players', raws, 0, 0, result='no participants; no player denominator')
                return
            clubs = club_listing_signatures(outcome.value)
            if not clubs:
                raise CurrentPortionError('listing is empty; no full roster proof')
            self.data.pop('authoritative_empty_scope', None)
        raws.append(outcome.raw_capture_id)
        previous_clubs = self.resume.get('clubs', {})
        required = set(self.resume.get('required', [])) & set(clubs)
        captured = {key: value for key, value in self.resume.get('captured', {}).items() if key in clubs}
        self.resume['capture_proofs'] = {key: value for key, value in self.resume.get('capture_proofs', {}).items() if key in clubs}
        for club_id, club in clubs.items():
            state = self.observe('club', club_id, club['signature'], outcome, url)
            if state.pending or self.snapshot is None or club_id not in self.snapshot.clubs:
                required.add(club_id)
            if previous_clubs.get(club_id, {}).get('signature') != club['signature']:
                captured.pop(club_id, None)
        self.resume.update(clubs=clubs, required=sorted(required), captured=captured,
                           squad_year=year, reason=self.resume.get('reason', 'cold' if self.snapshot is None else 'change'))
        self.data['listing_clubs'] = clubs
        if not self.cursor.generation:
            self.cursor = replace(self.cursor, generation=self.cycle_id)
        listing_key = f'listing:{self.target.competition_id}'
        baseline = (self.data.get('roster_proof') if listing_key not in self.signals and self.snapshot is not None
                    and set(self.snapshot.expected_team_ids) == set(clubs) else None)
        self.observe('listing', self.target.competition_id, semantic_signature(sorted(clubs)), outcome, url,
                     baseline_proof=baseline)
        self.check('listing', raws, len(clubs), len(clubs))

    def players(self):
        if self.data.get('authoritative_empty_scope'):
            self.check('players', self.data['authoritative_empty_scope']['raw_capture_ids'], 0, 0,
                       result='no participants; no player denominator')
            return
        if self.snapshot is None:
            # The player denominator becomes known only after the cold roster.
            raise CurrentPortionError('player signal denominator requires complete cold roster')
        self.initialize_resume()
        ids = sorted({row['player_id'] for row in self.snapshot.rows}, key=int)
        if not ids:
            listing = self.data.get('checks', {}).get('listing')
            if listing is None or listing['status'] != 'complete' or any(
                club.applicability_status != 'authoritative_empty' for club in self.snapshot.clubs.values()
            ):
                raise CurrentPortionError('empty player denominator has no complete participant/raw proof')
            self.check('players', listing['raw_capture_ids'], 0, 0,
                       checked_at=_utc(listing['checked_at']), result='all current clubs have proved empty rosters')
            return
        progress = self.data.get('player_check_progress', {})
        started_at = _utc(progress['started_at']) if progress.get('started_at') else None
        if (not started_at or progress.get('ids_hash') != semantic_signature(ids)
            or self.now() - started_at >= timedelta(hours=20)
            or self.now().astimezone(ZoneInfo('Europe/Moscow')).date() != started_at.astimezone(ZoneInfo('Europe/Moscow')).date()):
            progress = {'started_at': self.now().isoformat(), 'generation': self.cycle_id,
                        'ids_hash': semantic_signature(ids), 'offset': 0, 'raws': []}
            self.data['player_check_progress'] = progress
        raws = list(progress['raws'])
        required = set(self.resume.get('required', []))
        memberships = {}
        for row in self.snapshot.rows:
            memberships.setdefault(row['player_id'], set()).add(row['club_id'])
        packets = {}
        source_times = list(progress.get('source_times', []))
        source_scopes = dict(progress.get('source_scopes', {}))
        membership_validation = dict(progress.get('membership_validation', {}))
        for offset in range(progress['offset'], len(ids), 300):
            packet = ids[offset:offset + 300]
            facts = {player_id: self._player_fact(player_id, packets) for player_id in packet}
            missing = [player_id for player_id, fact in facts.items() if fact is None]
            if missing:
                url = tmapi.players_url(missing)
                outcome = self.fetch(url, 'current_players', as_json=True, generation=progress['generation'])
                parsed = tmapi.parse_player_signals(outcome.value, expected_ids=missing)
                self._save_player_packet(parsed, outcome, url)
                for player_id, signal in parsed.items():
                    facts[player_id] = signal, outcome, url, self.target.scope_id
            for player_id in packet:
                signal, outcome, url, primary_scope = facts[player_id]
                national = self.scope['record'].team_type.value == 'national_team'
                signal_roster_clubs = ({assignment[0] for assignment in signal.assignments if assignment[1] == 'nationalTeam'}
                                       if national else set(signal.club_ids))
                if national and not signal_roster_clubs:
                    membership_validation[player_id] = 'not_proven_by_tmapi'
                    # A last-played current tournament can contain a retired
                    # national player. Only its /plus/1 can remove membership.
                    signal_roster_clubs = set(memberships[player_id])
                elif national:
                    membership_validation[player_id] = 'assignment_signal_only; listing_and_plus1_prove_roster'
                raws.append(outcome.raw_capture_id)
                source_times.append(outcome.raw_fetched_at)
                source_scopes[outcome.raw_capture_id] = primary_scope
                previous = self.data.setdefault('player_values', {}).get(player_id)
                national_career_only = False
                if national and previous is not None:
                    old_assignments = [assignment for assignment in previous.get('assignments', []) if assignment[1] == 'nationalTeam']
                    new_assignments = [assignment for assignment in signal.assignments if assignment[1] == 'nationalTeam']
                    national_career_only = (
                        (not old_assignments or not new_assignments
                         or semantic_signature(old_assignments) == semantic_signature(new_assignments))
                        and previous.get('market_value_eur') == signal.market_value_eur
                        and previous.get('attributes_json') == signal.attributes_json)
                value_dates = self.data.get('career_value_dates', {})
                known_value_date = value_dates.get(player_id)
                date_only = previous is not None and (
                    previous.get('market_value_date') != signal.market_value_date
                    and semantic_signature({key: value for key, value in previous.items() if key != 'market_value_date'})
                    == semantic_signature({key: value for key, value in signal.as_dict().items() if key != 'market_value_date'}))
                player_rows = [row for row in self.snapshot.rows if row['player_id'] == player_id]
                baseline_proof = None
                if previous is None:
                    row = player_rows[0]
                    previous = {'market_value_eur': row.get('market_value_eur'),
                                'market_value_date': row.get('market_value_date'),
                                'club_ids': list(signal.club_ids) if national else list(memberships[player_id])}
                    # The first detector sample establishes its baseline.
                    # It is a change only when comparable full-roster fields
                    # differ. Preserve original Bronze proof/timestamps.
                    comparable_matches = (
                        self.data.get('roster_proof')
                        and signal_roster_clubs == memberships[player_id]
                        and all(row.get('market_value_eur') == signal.market_value_eur
                                and (national or (str(row['contract_until']) if row.get('contract_until') is not None else None) == signal.contract_until)
                                and (row.get('market_value_date') is None or str(row['market_value_date']) == signal.market_value_date)
                                for row in player_rows))
                    if comparable_matches:
                        if signal.market_value_present and signal.market_value_date is not None and (
                            (player_id in value_dates and known_value_date != signal.market_value_date)
                            or (self.data.get('warm_baseline') and player_id not in value_dates)):
                            date_only = True
                        else:
                            baseline_proof = self.data['roster_proof']
                state = self.observe('player', player_id, signal.signature, outcome, url,
                                     baseline_proof=baseline_proof, checked_at=_utc(outcome.raw_fetched_at),
                                     persist=False, write=False)
                if state.pending and date_only:
                    self.resume.setdefault('careers', {}).setdefault('market_value_points', []).append(player_id)
                    self.data.setdefault('player_roster_applied', {})[player_id] = signal.signature
                if state.pending and national_career_only:
                    careers = self.resume.setdefault('careers', {})
                    if previous.get('market_value_date') != signal.market_value_date:
                        careers.setdefault('market_value_points', []).append(player_id)
                    if _professional_assignment_changed(previous, signal):
                        careers.setdefault('transfer_events', []).append(player_id)
                    self.data.setdefault('player_roster_applied', {})[player_id] = signal.signature
                if state.pending and self.data.get('player_roster_applied', {}).get(player_id) != signal.signature:
                    self.resume['reason'] = 'change'
                    changed = memberships[player_id] | signal_roster_clubs
                    required.update(changed & set(self.resume.get('clubs', {})))
                    for club_id in changed:
                        self.resume.get('captured', {}).pop(club_id, None)
                        self.resume.setdefault('squad_generations', {})[club_id] = semantic_signature(
                            [state.first_detected_at.isoformat(), signal.signature])
                    careers = self.resume.setdefault('careers', {})
                    if (previous.get('market_value_eur'), previous.get('market_value_date')) != (signal.market_value_eur, signal.market_value_date):
                        careers.setdefault('market_value_points', []).append(player_id)
                    if _professional_assignment_changed(previous, signal):
                        careers.setdefault('transfer_events', []).append(player_id)
                self.data['player_values'][player_id] = signal.as_dict()
            self.resume['required'] = sorted(required)
            raws = list(dict.fromkeys(raws))
            self.data['unpublished_player_signals'] = sorted(set(
                self.data.get('unpublished_player_signals', [])) | set(packet), key=int)
            _sql(self.scraper, build_signals_merge(self.signals[f'player:{player_id}'] for player_id in packet))
            self.data['unpublished_player_signals'] = sorted(set(
                self.data['unpublished_player_signals']) - set(packet), key=int)
            progress.update(offset=offset + len(packet), raws=raws,
                            source_times=source_times, source_scopes=source_scopes,
                            membership_validation=membership_validation)
            self.persist()
        self.check('players', raws, len(ids), len(ids),
                   checked_at=min((_utc(value) for value in source_times), default=self.now()),
                   result=json.dumps({'checked': True, 'primary_packet_scopes': source_scopes,
                                      'national_membership_validation': membership_validation}, sort_keys=True))
        self.data.pop('player_check_progress', None)
        self.persist()

    def injuries(self):
        from bs4 import BeautifulSoup
        competition = self.scope['record']
        self.initialize_resume()
        if competition.team_type.value == 'national_team':
            listing = self.data.get('checks', {}).get('listing')
            if listing is None or listing['status'] != 'complete':
                raise CurrentPortionError('national injury non-applicability needs verified current participant proof')
            self.check('injury', listing['raw_capture_ids'], 0, 0,
                       checked_at=_utc(listing['checked_at']), result=json.dumps({
                           'applicability_status': 'not_applicable',
                           'rule': 'league_injury_listing:club_competitions',
                           'team_type': competition.team_type.value,
                           'registry_snapshot_id': self.target.registry_snapshot_id}, sort_keys=True))
            return
        namespace = 'pokalwettbewerb' if '/pokalwettbewerb/' in competition.source_url else 'wettbewerb'
        url = f'https://www.transfermarkt.com/{competition.slug}/verletztespieler/{namespace}/{self.target.competition_id}'
        outcome = self.fetch(url, 'current_injury')
        table = BeautifulSoup(outcome.value, 'html.parser').find('table', class_='items')
        if table is None:
            raise CurrentPortionError('injury page has no table; empty is unproven')
        for decorative in table.select('script, style, input'):
            decorative.decompose()
        rows = [' '.join(tr.get_text(' ', strip=True).split()) for tr in table.select('tbody > tr')]
        previous = self.signals.get(f'injury:{self.target.competition_id}')
        state = self.observe('injury', self.target.competition_id, semantic_signature(sorted(rows)), outcome, url)
        if state.pending and (previous is None or previous.observation.signature != state.observation.signature):
            self.resume['reason'] = 'change' if self.snapshot else 'cold'
            import re
            affected = {match.group(1) for anchor in table.find_all('a', href=True)
                        if (match := re.search(r'/verein/(\d+)', anchor['href']))}
            injured_players = {match.group(1) for anchor in table.find_all('a', href=True)
                               if (match := re.search(r'/spieler/(\d+)', anchor['href']))}
            if self.snapshot is not None:
                affected.update(row['club_id'] for row in self.snapshot.rows if row['player_id'] in injured_players)
            previous_affected = set(self.data.get('injury_affected_clubs', []))
            self.data['injury_affected_clubs'] = sorted(affected)
            affected.update(previous_affected)
            required = set(self.resume.get('required', [])) | (affected & set(self.resume.get('clubs', {})))
            self.resume['required'] = sorted(required)
            for club_id in affected:
                self.resume.get('captured', {}).pop(club_id, None)
                self.resume.setdefault('squad_generations', {})[club_id] = state.observation.signature
        self.check('injury', [outcome.raw_capture_id], len(rows), len(rows))
        if state.pending:
            state = mark_applied(state, signature=state.observation.signature,
                                 bronze_manifest='raw:' + outcome.raw_capture_id, committed_at=self.now())
            state = replace(state, result='raw_durable')
            self.signals[f'injury:{self.target.competition_id}'] = state
            _sql(self.scraper, build_signal_merge(state))
            self.persist()

    def acknowledge(self, proof, *, include_players=False, expected_signatures=None):
        manifest, at = _proof(proof)
        pending = [(key, state) for key, state in self.signals.items()
                   if state.pending and (include_players or state.observation.entity != 'player')
                   and (expected_signatures is None or expected_signatures.get(key) == state.observation.signature)
                   and (state.observation.entity != 'player' or self.data.get('player_roster_applied', {}).get(
                       state.observation.entity_id) == state.observation.signature)]
        for offset in range(0, len(pending), 300):
            updates = {key: mark_applied(state, signature=state.observation.signature,
                                        bronze_manifest=manifest, committed_at=at)
                       for key, state in pending[offset:offset + 300]}
            _sql(self.scraper, build_signals_merge(updates.values()))
            self.signals.update(updates)
        self.persist()

    def validate_player_business(self, snapshot):
        """Do not acknowledge a detector while a CDN serves old squad fields."""
        if self.row.get('is_current') is False:
            # Live player assignments describe the new edition. Closed-edition
            # squads retain their historical membership, with normal raw/DQ guards.
            return
        national = self.scope['record'].team_type.value == 'national_team'
        mismatches, failed = set(), []
        for key, state in self.signals.items():
            if state.observation.entity != 'player' or not state.pending:
                continue
            player_id = state.observation.entity_id
            values = self.data.get('player_values', {}).get(player_id)
            if values is None:
                body, raw = self.scraper._http_client._raw_store.load_capture(state.observation.raw_capture_id)
                packet_ids = parse_qs(urlsplit(raw.url).query).get('ids[]', [])
                values = tmapi.parse_player_signals(json.loads(body.decode('utf-8')), expected_ids=packet_ids)[player_id].as_dict()
            rows = [row for row in snapshot.rows if row['player_id'] == player_id]
            memberships = {row['club_id'] for row in rows}
            expected = set(values['club_ids']) & set(snapshot.expected_team_ids)
            changed = memberships if national else memberships | expected
            inconsistent = (not national and memberships != expected) or any(
                row['market_value_eur'] != values['market_value_eur']
                or (not national and (str(row['contract_until']) if row['contract_until'] is not None else None)
                    != values['contract_until']) for row in rows)
            if inconsistent:
                mismatches.update(changed)
                failed.append((key, mark_failed(state, 'source_mismatch: tmapi and complete squad fields disagree')))
                self.player_facts.pop(player_id, None)
                self.invalidate_player_fact(player_id, self.now())
        if not failed:
            return
        for club_id in mismatches:
            self.resume.get('captured', {}).pop(club_id, None)
            self.resume.setdefault('squad_generations', {})[club_id] = 'source-mismatch-' + self.cycle_id
            for url in list(self.data.get('response_cache', {})):
                if f'/verein/{club_id}/' in url and '/kader/' in url:
                    self.data['response_cache'].pop(url)
        for offset in range(0, len(failed), 300):
            _sql(self.scraper, build_signals_merge(state for _, state in failed[offset:offset + 300]))
        self.signals.update(dict(failed))
        self.persist()
        raise CurrentPortionError('source_mismatch: pending player changes have no matching complete squad proof')

    def roster(self, *, weekly=False):
        from scrapers.transfermarkt.scraper import _parse_squad_page, _CLUB_SQUAD_PATH, _TM_BASE, _squad_page_is_empty_roster
        self._require_reconciled_signals()
        if self.data.get('unpublished_player_signals'):
            raise CurrentPortionError('player signals are not committed to ops; roster update remains pending')
        if not self.resume.get('clubs'):
            self.listing()
        if self.data.get('authoritative_empty_scope'):
            return
        clubs = self.resume['clubs']
        if weekly:
            self.resume['required'] = sorted(clubs)
            self.resume['reason'] = 'weekly'
            self.resume['captured'] = {}
            self.resume['squad_generations'] = {club_id: self.cycle_id for club_id in clubs}
        required = self.resume.get('required', [])
        captured = self.resume.setdefault('captured', {})
        for club_id in required:
            self.admission()
            url = _TM_BASE + _CLUB_SQUAD_PATH.format(club_slug=clubs[club_id]['club_slug'], club_id=club_id,
                                                   year=self.resume['squad_year'])
            generation = semantic_signature([self.target.scope_id, club_id, clubs[club_id]['signature'],
                                             self.resume.get('squad_generations', {}).get(club_id, '')])
            existing = captured.get(club_id)
            if existing:
                saved = ClubRosterSnapshot.from_mapping(club_id, existing)
                if self._verified_pending_capture(saved, url, generation):
                    continue
                captured.pop(club_id, None)
                self.resume.get('capture_proofs', {}).pop(club_id, None)
            outcome = self.fetch(url, 'squad', fresh=False, generation=generation)
            rows = _parse_squad_page(outcome.value, club_id)
            applicability, empty_proof = 'ok', None
            if not rows:
                from bs4 import BeautifulSoup
                if (not _squad_page_is_empty_roster(BeautifulSoup(outcome.value, 'html.parser'))
                    or (self.snapshot is not None and club_id in self.snapshot.clubs and self.snapshot.clubs[club_id].rows)
                    or self.scraper._bronze_club_has_roster(self.scope['compatibility_league'],
                                                          self.scope['canonical_season'], club_id)):
                    raise CurrentPortionError('empty full squad has no authoritative proof')
                applicability = 'authoritative_empty'
                empty_proof = {'kind': 'typed_fetch_state', 'status': applicability,
                               'raw_capture_id': outcome.raw_capture_id, 'source_body_hash': outcome.raw_body_hash}
            saved = ClubRosterSnapshot(club_id, rows, outcome.raw_capture_id, _utc(outcome.raw_fetched_at),
                                      url, outcome.raw_body_hash, None, clubs[club_id]['signature'],
                                      applicability_status=applicability, authoritative_empty_proof=empty_proof)
            if self.scraper._http_client._cache[url]['outcome'].get('version') == 3:
                if not self._verified_pending_capture(saved, url, generation):
                    raise CurrentPortionError('legacy squad cache could not be bound to saved full rows')
            captured[club_id] = saved.as_dict()
            self.resume.setdefault('capture_proofs', {})[club_id] = self._capture_checkpoint(url)
            self.persist()
        membership_changed = self.snapshot is not None and set(self.snapshot.expected_team_ids) != set(clubs)
        if not required and self.snapshot is not None and not membership_changed:
            return
        snapshot = assemble_full_roster(
            self.snapshot, clubs, captured, required,
            scope_id=self.target.scope_id, competition_id=self.target.competition_id,
            edition_id=self.target.edition_id, squad_saison_id=self.resume['squad_year'],
            participant_membership_verified=True,
        )
        self.validate_player_business(snapshot)
        previous_rows = ([row for row in self.snapshot.rows if row['club_id'] in set(snapshot.expected_team_ids)]
                         if self.snapshot else [])
        for player_id, change in player_changes(previous_rows, snapshot.rows).items():
            for endpoint in change.endpoints:
                self.resume.setdefault('careers', {}).setdefault(endpoint, []).append(player_id)
        # Persist pending careers before the write, so a lost commit ack cannot
        # discard the work discovered from this full-roster difference.
        self.persist()
        intent = {'version': 1, 'candidate': snapshot.as_dict(), 'changed_club_ids': list(snapshot.changed_club_ids),
                  'baseline_hash': semantic_signature(self.data.get('snapshot')),
                  'write_cycle': self.cycle_id, 'write_at': self.now().isoformat(),
                  'write_preflight': dict(self.preflight),
                  'batch_id': getattr(self.scraper, '_batch_id', None),
                  'membership_changed': membership_changed,
                  'signal_signatures': {key: value.observation.signature for key, value in self.signals.items()},
                  'capture_proofs': {club_id: self._capture_checkpoint(snapshot.clubs[club_id].source_url)
                                     for club_id in snapshot.changed_club_ids}}
        self.data['roster_write_intent'] = {**intent, 'intent_hash': semantic_signature(intent)}
        self.persist()
        self._apply_roster_write(snapshot, self.cycle_id, membership_changed, intent['write_at'])

    def _capture_checkpoint(self, url):
        checkpoint = self.scraper._http_client._cache.get(url)
        if not isinstance(checkpoint, Mapping):
            raise CurrentPortionError('unfinished squad requires a raw attempt checkpoint')
        version = checkpoint.get('outcome', {}).get('version')
        if version != 4 and not (version == 3 and checkpoint.get('legacy_proof', {}).get('kind')
                                == 'legacy_v3_success_response'):
            raise CurrentPortionError('unfinished squad requires v4 or typed legacy raw attempt proof')
        value = json.loads(json.dumps(checkpoint))
        value['outcome'].pop('value', None)
        return value

    def _verified_pending_capture(self, saved, url, generation, checkpoint=None):
        from scrapers.transfermarkt.scraper import _parse_squad_page
        if self.now() - saved.raw_fetched_at >= timedelta(hours=48) or self.now() < saved.raw_fetched_at:
            return False
        if saved.signature != self.resume.get('clubs', {}).get(saved.club_id, {}).get('signature'):
            raise CurrentPortionError('unfinished squad signature differs from current club signal')
        if saved.source_url != url:
            raise CurrentPortionError('unfinished squad source URL differs from current club identity')
        if checkpoint is None:
            checkpoint = self.resume.get('capture_proofs', {}).get(saved.club_id)
        if checkpoint is not None:
            version = checkpoint.get('outcome', {}).get('version')
            if version != 4 and not (version == 3 and checkpoint.get('legacy_proof', {}).get('kind')
                                    == 'legacy_v3_success_response'):
                raise CurrentPortionError('unfinished squad has no typed raw attempt proof')
            try:
                FetchOutcome.from_checkpoint(checkpoint['outcome'])
            except (ValueError, TypeError, KeyError) as exc:
                raise CurrentPortionError('unfinished squad v4 attempt-chain checkpoint is corrupt') from exc
            self.scraper._http_client._cache[url] = checkpoint
        outcome = self.scraper._http_client._load_cached_outcome(
            url, as_json=False, label='squad', url=url,
            context={'scope': self.target.scope_id, 'cycle_id': self.scraper._current_child_cycle_id},
            cache_generation=generation)
        if outcome is None:
            return False
        if (outcome.raw_capture_id != saved.raw_capture_id or outcome.raw_body_hash != saved.source_body_hash
            or _utc(outcome.raw_fetched_at) != saved.raw_fetched_at
            or semantic_signature(_parse_squad_page(outcome.value, saved.club_id))
                != semantic_signature([dict(row) for row in saved.rows])):
            raise CurrentPortionError('unfinished squad parsed rows differ from verified raw attempt chain')
        checkpoint = self.scraper._http_client._cache[url]
        if checkpoint['outcome'].get('version') == 3:
            # Only the existing client publishes the verified legacy success
            # envelope. Keep the original transport count and v3 schema clean.
            proof = {'kind': 'legacy_v3_success_response',
                     'original_attempt_count': outcome.attempts,
                     'evidenced_attempt_count': len(outcome.raw_attempt_envelope_ids),
                     'prefix_unknown': outcome.attempts != len(outcome.raw_attempt_envelope_ids),
                     'raw_attempt_envelope_ids': list(outcome.raw_attempt_envelope_ids)}
            if checkpoint.get('legacy_proof') is not None and checkpoint['legacy_proof'] != proof:
                raise CurrentPortionError('legacy squad sidecar differs from verified successful attempt')
            checkpoint = {**checkpoint, 'legacy_proof': proof}
            self._verify_reconciliation_capture(saved, url, checkpoint)
            self.scraper._http_client._cache[url] = checkpoint
        self.scraper._http_client._record_cache_hit(label='squad', duration_seconds=0)
        return True

    def _recover_roster_write(self):
        intent = dict(self.data['roster_write_intent'])
        checksum = intent.pop('intent_hash', None)
        if (checksum != semantic_signature(intent) or intent.get('version') != 1
            or intent.get('baseline_hash') != semantic_signature(self.data.get('snapshot'))
            or intent.get('write_preflight') != dict(self.preflight)
            or not isinstance(intent.get('write_cycle'), str) or not intent['write_cycle']):
            raise CurrentPortionError('roster_write_intent identity/checksum differs; baseline remains blocked')
        candidate = dict(intent['candidate'])
        candidate['clubs'] = {key: ClubRosterSnapshot.from_mapping(key, value)
                              for key, value in candidate['clubs'].items()}
        snapshot = FullRosterSnapshot(**candidate, changed_club_ids=intent['changed_club_ids'])
        if (snapshot.scope_id != self.target.scope_id or snapshot.competition_id != self.target.competition_id
            or snapshot.edition_id != self.target.edition_id or snapshot.squad_saison_id != self.resume.get('squad_year')
            or set(snapshot.expected_team_ids) != set(self.resume.get('clubs', {}))):
            raise CurrentPortionError('roster_write_intent scope/participant identity differs')
        if (set(snapshot.changed_club_ids) != set(self.resume.get('required', []))
            or intent['membership_changed'] != (self.snapshot is not None
                and set(self.snapshot.expected_team_ids) != set(snapshot.expected_team_ids))):
            raise CurrentPortionError('roster_write_intent is not a complete authorized replacement')
        from scrapers.transfermarkt.scraper import _TM_BASE, _CLUB_SQUAD_PATH
        source_ages = {}
        for club_id, club in snapshot.clubs.items():
            if club_id not in snapshot.changed_club_ids:
                if self.snapshot is None or club.as_dict() != self.snapshot.clubs[club_id].as_dict():
                    raise CurrentPortionError('roster_write_intent retained baseline differs')
                source_ages[club_id] = {'raw_capture_id': club.raw_capture_id,
                    'raw_fetched_at': club.raw_fetched_at.isoformat(),
                    'source_age_seconds': (self.now() - club.raw_fetched_at).total_seconds(),
                    'source_url': club.source_url, 'source_body_hash': club.source_body_hash,
                    'proof_kind': 'retained_verified_baseline'}
                continue
            checkpoint = intent['capture_proofs'].get(club_id)
            generation = semantic_signature([self.target.scope_id, club_id, self.resume['clubs'][club_id]['signature'],
                                             self.resume.get('squad_generations', {}).get(club_id, '')])
            expected_url = _TM_BASE + _CLUB_SQUAD_PATH.format(
                club_slug=self.resume['clubs'][club_id]['club_slug'], club_id=club_id, year=snapshot.squad_saison_id)
            if (club.signature != self.resume['clubs'][club_id]['signature'] or checkpoint is None
                or checkpoint.get('cache_generation') != generation):
                raise CurrentPortionError('roster_write_intent source attempt evidence cannot be verified')
            source_ages[club_id] = self._verify_reconciliation_capture(club, expected_url, checkpoint)
        reconciliation = {'kind': 'complete_write_intent', 'original_write_cycle': intent['write_cycle'],
                          'original_write_at': intent['write_at'], 'intent_hash': checksum,
                          'reconciled_at': self.now().isoformat(), 'source_ages': source_ages}
        previous_batch = getattr(self.scraper, '_batch_id', None)
        self.scraper._batch_id = intent['batch_id']
        try:
            self._apply_roster_write(snapshot, intent['write_cycle'], intent['membership_changed'], intent['write_at'],
                                     reconciliation=reconciliation)
        finally:
            self.scraper._batch_id = previous_batch

    def _verify_reconciliation_capture(self, saved, expected_url, checkpoint):
        """Validate a complete write's historical inputs, independently of HTTP cache."""
        from scrapers.transfermarkt.scraper import _parse_squad_page
        version = checkpoint.get('outcome', {}).get('version')
        if version not in (3, 4):
            raise CurrentPortionError('write reconciliation requires typed raw attempt evidence')
        outcome = FetchOutcome.from_checkpoint(checkpoint['outcome'])
        legacy = checkpoint.get('legacy_proof') if version == 3 else None
        if version == 3:
            keys = {'kind', 'original_attempt_count', 'evidenced_attempt_count', 'prefix_unknown',
                    'raw_attempt_envelope_ids'}
            if (not isinstance(legacy, Mapping) or set(legacy) != keys
                or legacy['kind'] != 'legacy_v3_success_response'
                or type(legacy['original_attempt_count']) is not int or outcome.attempts < 1
                or legacy['original_attempt_count'] != outcome.attempts
                or type(legacy['evidenced_attempt_count']) is not int or legacy['evidenced_attempt_count'] != 1
                or type(legacy['prefix_unknown']) is not bool or legacy['prefix_unknown'] != (outcome.attempts != 1)
                or not isinstance(legacy['raw_attempt_envelope_ids'], list)
                or len(legacy['raw_attempt_envelope_ids']) != 1):
                raise CurrentPortionError('write reconciliation legacy sidecar/counts differ')
        envelope_ids = legacy['raw_attempt_envelope_ids'] if legacy is not None else outcome.raw_attempt_envelope_ids
        store = self.scraper._http_client._raw_store
        body, record = store.load_capture(saved.raw_capture_id)
        replay = body.decode('utf-8')
        age = (self.now() - saved.raw_fetched_at).total_seconds()
        if (age < 0 or saved.source_url != expected_url or record.url != expected_url
            or record.scope_id != self.target.scope_id or record.endpoint != 'squad' or record.status_code != 200
            or _utc(record.fetched_at) != saved.raw_fetched_at
            or record.content_hash != saved.source_body_hash or hashlib.sha256(body).hexdigest() != saved.source_body_hash
            or outcome.status != FetchStatus.OK or outcome.label != 'squad' or outcome.status_code != 200
            or outcome.raw_capture_id != saved.raw_capture_id or outcome.raw_body_hash != saved.source_body_hash
            or _utc(outcome.raw_fetched_at) != saved.raw_fetched_at or outcome.payload_hash != stable_payload_hash(replay)
            or semantic_signature(_parse_squad_page(replay, saved.club_id))
                != semantic_signature([dict(row) for row in saved.rows])):
            raise CurrentPortionError('write reconciliation raw identity/body/parsed rows differ')
        if saved.applicability_status == 'authoritative_empty':
            from bs4 import BeautifulSoup
            from scrapers.transfermarkt.scraper import _squad_page_is_empty_roster
            if not _squad_page_is_empty_roster(BeautifulSoup(replay, 'html.parser')):
                raise CurrentPortionError('write reconciliation empty roster has no authoritative source proof')
        envelopes = [store.load_attempt_envelope(key) for key in envelope_ids]
        previous = -1
        for envelope in envelopes:
            if (envelope.cycle_id != record.cycle_id or envelope.scope_id != record.scope_id
                or envelope.endpoint != record.endpoint or envelope.url != record.url
                or envelope.attempt <= previous):
                raise CurrentPortionError('write reconciliation attempt identity/order differs')
            previous = envelope.attempt
        final = envelopes[-1]
        if ((legacy is None and len(envelopes) != outcome.attempts) or final.outcome_kind != 'response'
            or final.capture_id != record.capture_id or final.status_code != 200
            or final.raw_body_hash != record.content_hash or final.attempt != record.attempt):
            raise CurrentPortionError('write reconciliation final attempt differs from successful raw capture')
        self.scraper._http_client._raw_captures[record.capture_id] = record
        for envelope in envelopes:
            self.scraper._http_client._cache_sources[envelope.envelope_id] = envelope
        return {'raw_capture_id': record.capture_id, 'raw_fetched_at': record.fetched_at,
                'source_age_seconds': age, 'source_url': record.url, 'source_body_hash': record.content_hash,
                'proof_kind': legacy['kind'] if legacy is not None else 'v4_complete_attempt_chain',
                'original_attempt_count': outcome.attempts, 'evidenced_attempt_count': len(envelopes),
                'prefix_unknown': legacy['prefix_unknown'] if legacy is not None else False}

    def _require_reconciled_signals(self):
        refresh = self.data.get('recovery_signal_refresh_required')
        if not refresh:
            return
        required = refresh.get('required_checks', ['listing', 'players']) if isinstance(refresh, Mapping) else ['listing', 'players']
        boundary = _utc(refresh['reconciled_at']) if isinstance(refresh, Mapping) else None
        for kind in required:
            stamp = getattr(self.cursor, kind + '_checked_at')
            if stamp is None or (boundary is not None and stamp < boundary):
                raise CurrentPortionError('write recovery requires fresh signals before roster or careers')
        self.data.pop('recovery_signal_refresh_required')
        self.persist()

    def _apply_roster_write(self, snapshot, write_cycle, membership_changed, write_at, *, reconciliation=None):
        self.admission()
        previous_write_at = getattr(self.scraper, '_current_roster_write_at', None)
        self.scraper._current_roster_write_at = _utc(write_at)
        previous_membership_flag = getattr(self.scraper, '_current_membership_changed', None)
        self.scraper._current_membership_changed = membership_changed
        try:
            proof = self.roster_writer(self.scraper, snapshot, self.preflight, write_cycle)
        finally:
            if previous_write_at is None:
                del self.scraper._current_roster_write_at
            else:
                self.scraper._current_roster_write_at = previous_write_at
            if previous_membership_flag is None:
                del self.scraper._current_membership_changed
            else:
                self.scraper._current_membership_changed = previous_membership_flag
        manifest, at = _proof(proof)
        if reconciliation is not None:
            proof = {**dict(proof), 'reconciliation': reconciliation}
            self.data.setdefault('roster_reconciliations', []).append({**reconciliation, 'bronze_manifest': manifest})
        self.data['roster_proof'] = dict(proof)
        self.data.setdefault('roster_receipts', []).append({**dict(proof), 'club_receipts': {
            club_id: {'raw_capture_id': snapshot.clubs[club_id].raw_capture_id,
                      'source_body_hash': snapshot.clubs[club_id].source_body_hash,
                      'rows_signature': semantic_signature([dict(row) for row in snapshot.clubs[club_id].rows])}
            for club_id in snapshot.changed_club_ids}})
        committed_clubs = {key: replace(club, bronze_manifest=manifest) if key in snapshot.changed_club_ids else club
                           for key, club in snapshot.clubs.items()}
        self.snapshot = replace(snapshot, clubs=committed_clubs)
        signatures = self.data['roster_write_intent']['signal_signatures']
        drifted = [state for key, state in self.signals.items() if signatures.get(key) != state.observation.signature]
        for key, state in self.signals.items():
            if state.observation.entity == 'player' and signatures.get(key) == state.observation.signature:
                self.data.setdefault('player_roster_applied', {})[state.observation.entity_id] = state.observation.signature
        self.cursor = replace(self.cursor, roster_completed_at=at)
        self.resume['required'], self.resume['captured'] = [], {}
        self.resume['capture_proofs'] = {}
        if reconciliation is not None or drifted:
            self.data['recovery_signal_refresh_required'] = (
                {'reconciled_at': reconciliation['reconciled_at'], 'required_checks': ['listing', 'players', 'injury']}
                if reconciliation is not None else True)
            self.data.pop('player_check_progress', None)
            self.cursor = replace(self.cursor, listing_checked_at=None, players_checked_at=None,
                                  injury_checked_at=None if reconciliation is not None else self.cursor.injury_checked_at)
            self.resume['required'] = sorted({state.observation.entity_id for state in drifted
                                              if state.observation.entity == 'club'
                                              and state.observation.entity_id in self.snapshot.clubs})
        self.data.pop('roster_write_intent', None)
        # Persist the verified replacement before acknowledging ops signals.
        self.persist()
        self.acknowledge(proof, expected_signatures=signatures)
        self.persist()

    def careers(self):
        self._require_reconciled_signals()
        if self.data.get('authoritative_empty_scope'):
            return
        pending = self.resume.setdefault('careers', {})
        for endpoint in ('market_value_points', 'transfer_events'):
            ids = sorted(set(pending.get(endpoint, [])), key=int)
            if not ids:
                continue
            self.admission()
            entity = 'market_value_history' if endpoint == 'market_value_points' else 'transfers'
            budget = PRODUCTION_ENTITY_BUDGETS[entity]
            decoded_start = self.scraper.get_traffic_stats().get('decoded_response_body_bytes')
            if not isinstance(decoded_start, int) or decoded_start < 0:
                raise CurrentPortionError('career decoded traffic is unavailable')
            soft_remaining = int(.75 * budget['decoded_mb'] * 1024 * 1024) - _entity_used(
                self.data, self.scraper, entity, 'decoded_bytes')
            if soft_remaining <= 0:
                continue
            self.scraper._begin_operation_budget(entity)
            path = '/ceapi/marketValueDevelopment/graph/' if endpoint == 'market_value_points' else '/ceapi/transferHistory/list/'
            for player_id in ids:
                state = self.signals.get(f'player:{player_id}')
                generation = semantic_signature([self.target.scope_id, endpoint, player_id,
                    state.observation.signature if state else self.cursor.generation,
                    state.first_detected_at.isoformat() if state else self.cursor.generation,
                    self.data.get('career_generations', {}).get(f'{endpoint}:{player_id}', '')])
                self.scraper._cache_generation_by_url['https://www.transfermarkt.com' + path + player_id] = generation
            # Use the exact existing HTTP cache generation, including scope
            # and the source-mismatch nonce. Archives cannot bypass that gate.
            self.scraper._current_career_signal_generations = {player_id:
                self.scraper._cache_generation_by_url['https://www.transfermarkt.com' + path + player_id]
                for player_id in ids}
            proof = self.career_writer(
                self.scraper, endpoint, ids, self.scope, self.preflight, self.cycle_id,
                decoded_body_soft_stop_bytes=decoded_start + soft_remaining,
            )
            if proof.get('status') in {'superseded_partial_write', 'supersession_deferred'}:
                window = proof.get('career_window', {})
                if window.get('processed_player_ids') != [] or set(window.get('deferred_player_ids', [])) != set(ids):
                    raise CurrentPortionError('terminal career failure changed exact requested remainder')
                if proof.get('status') == 'supersession_deferred':
                    self.data.setdefault('career_deferred', {})[endpoint] = dict(proof)
                    self.persist()
                    continue
                failures = self.data.setdefault('career_job_failures', {})
                digest = proof['intent_sha256']
                failure = {key: value for key, value in proof.items()
                    if key not in {'career_window', 'reconciled_without_http'}}
                if digest in failures and failures[digest] != failure:
                    raise CurrentPortionError('immutable superseded career failure differs')
                failures[digest] = failure
                for player in proof['retired_player_ids']:
                    state = self.signals.get(f'player:{player}')
                    generation = self.scraper._current_career_signal_generations.get(player)
                    if state is not None and state.pending and proof.get('signal_generations', {}).get(player) == generation:
                        failed = mark_failed(state, 'superseded_partial_write:' + digest)
                        _sql(self.scraper, build_signal_merge(failed))
                        self.signals[f'player:{player}'] = failed
                self.persist()
                continue  # No proof/ack/cold-complete for the original failed dual job.
            _proof(proof)
            window = proof.get('career_window')
            if not isinstance(window, Mapping):
                raise CurrentPortionError('career writer did not return an exact adaptive window')
            processed, deferred = window.get('processed_player_ids', []), window.get('deferred_player_ids', [])
            if set(processed) & set(deferred) or set(processed) | set(deferred) != set(ids):
                raise CurrentPortionError('career window does not partition requested players')
            pending[endpoint] = list(deferred)
            stale_generation_ids = [player for player in processed
                if proof.get('signal_generations', {}).get(player) is not None
                and proof['signal_generations'][player] != self.scraper._current_career_signal_generations.get(player)]
            mismatched_ids = list(stale_generation_ids)
            if endpoint == 'market_value_points':
                from scrapers.transfermarkt.scraper import _parse_mv_history
                outcomes = self.scraper.get_fetch_outcomes().get(endpoint, {})
                for player_id in processed:
                    raw_capture_id = proof.get('cached_capture_ids', {}).get(player_id) or outcomes.get(player_id, {}).get('raw_capture_id')
                    if raw_capture_id:
                        body, _ = self.scraper._http_client._raw_store.load_capture(raw_capture_id)
                        points = _parse_mv_history(json.loads(body.decode('utf-8')), player_id)
                        dates = [str(point['mv_date']) for point in points if point.get('mv_date') is not None]
                        self.data.setdefault('career_value_dates', {})[player_id] = max(dates, default=None)
                        expected = self.data.get('player_values', {}).get(player_id)
                        if expected and expected.get('market_value_present') and points:
                            latest = max(points, key=lambda point: str(point['mv_date']))
                            if (latest['value_eur'] != expected['market_value_eur']
                                or (expected.get('market_value_date') is not None
                                    and str(latest['mv_date']) != expected['market_value_date'])):
                                mismatched_ids.append(player_id)
                                url = 'https://www.transfermarkt.com' + path + player_id
                                self.data.get('response_cache', {}).pop(url, None)
                                self.data.setdefault('career_generations', {})[f'{endpoint}:{player_id}'] = self.cycle_id
                                key = f'player:{player_id}'
                                signal = self.signals.get(key)
                                if signal is not None and signal.pending:
                                    signal = mark_failed(signal, 'source_mismatch: CEAPI latest point differs from tmapi')
                                    _sql(self.scraper, build_signal_merge(signal))
                                    self.signals[key] = signal
            pending[endpoint] = sorted(set(deferred) | set(mismatched_ids), key=int)
            proof = {**dict(proof), 'collector_window': {
                'requested_player_ids': ids,
                'processed_player_ids': [player_id for player_id in processed if player_id not in mismatched_ids],
                'deferred_player_ids': list(pending[endpoint]), 'source_mismatch_ids': [player for player in mismatched_ids if player not in stale_generation_ids],
                'stale_generation_ids': stale_generation_ids}}
            self.data.setdefault('career_proofs', {})[endpoint] = dict(proof)
            self.data.setdefault('career_receipts', []).append(dict(proof))
            self.persist()
        if not any(pending.values()) and self.snapshot is not None:
            proofs = list(self.data.get('career_proofs', {}).values())
            if self.data.get('roster_proof'):
                proofs.append(self.data['roster_proof'])
            if proofs:
                proof = max(proofs, key=lambda item: _proof(item)[1])
                # A proof from a previous refresh cannot acknowledge a fresh
                # player observation. Leave it pending for this generation.
                eligible = [state for state in self.signals.values() if state.pending]
                if all(_proof(proof)[1] >= state.first_detected_at for state in eligible):
                    self.acknowledge(proof, include_players=True)

    def coaches(self):
        if self.data.get('authoritative_empty_scope'):
            return
        if self.coach_writer is None:
            self.resume['coaches_pending'] = True
            self.persist()
            return
        if self.snapshot is None:
            raise CurrentPortionError('coaches require complete committed roster')
        self.admission()
        selected = self.resume.get('coach_club_ids', list(self.snapshot.expected_team_ids))
        proof = self.coach_writer(self.scraper, self.scope, self.preflight, self.cycle_id, clubs=selected)
        _proof(proof)
        if 'deferred_club_ids' in proof:
            processed = [str(value) for value in proof.get('processed_club_ids', [])]
            deferred = [str(value) for value in proof['deferred_club_ids']]
            cached = [str(value) for value in proof.get('cached_club_ids', [])]
            if (set(processed) & set(deferred) or set(processed) | set(deferred) | set(cached) != set(selected)):
                raise CurrentPortionError('coach continuation does not cover every selected club')
            self.resume['coach_club_ids'] = deferred
            self.resume['coaches_pending'] = bool(deferred)
        elif proof.get('complete') is True:
            self.resume['coaches_pending'] = False
            self.resume['coach_club_ids'] = []
        else:
            raise CurrentPortionError('coach writer has no exact continuation evidence')
        self.data.setdefault('coach_proofs', []).append(dict(proof))
        if not self.resume.get('coaches_pending'):
            self.data['coaches_checked_at'] = self.now().isoformat()
        self.persist()


def run_current_portion(registry_rows, preflight, cycle_id, *, denominator_rows=None,
                        state_dir=None, qualification=None, max_seconds=2700,
                        max_scopes=8, scraper_factory=None, roster_writer=None,
                        career_writer=None, coach_writer=None, now_fn=None, monotonic_fn=None,
                        deadline_monotonic=None, baseline_verifier=None, bootstrap_verifier=None):
    """Collect real source work with offline dependency injection for integration QA."""
    if not preflight.get('paid_io_allowed') or preflight.get('write_mode') not in {'dual', 'native-only'}:
        raise CurrentPortionError('reader preflight did not authorize current collection')
    if qualification is None and os.environ.get('TM_CURRENT_QUALIFICATION_PATH'):
        qualification = json.loads(Path(os.environ['TM_CURRENT_QUALIFICATION_PATH']).read_text())
    qualification = validate_qualification(qualification)
    bootstrap_entries = {}
    bundle_path, bundle_hash = qualification.get('baseline_bundle_path'), qualification.get('baseline_bundle_sha256')
    if bundle_path or bundle_hash:
        if not bundle_path or not bundle_hash:
            raise CurrentPortionError('bootstrap bundle needs a path and immutable SHA256')
        body = Path(bundle_path).read_bytes()
        if hashlib.sha256(body).hexdigest() != bundle_hash:
            raise CurrentPortionError('bootstrap bundle checksum differs')
        bundle = json.loads(body)
        if bundle.get('version') != CURRENT_POLICY_VERSION or not isinstance(bundle.get('entries'), dict):
            raise CurrentPortionError('bootstrap bundle format differs')
        bootstrap_entries = bundle['entries']
    now = now_fn or (lambda: datetime.now(timezone.utc))
    clock = monotonic_fn or time.monotonic
    started = now()
    budget = available_work_seconds(started, max_seconds)
    report = {'policy_version': CURRENT_POLICY_VERSION, 'cycle_id': cycle_id,
              'started_at': started.isoformat(), 'portion_budget_seconds': budget,
              'scopes': [], 'daily_complete_scope_ids': [], 'full_scope_completed': False}
    if budget <= 0:
        return {**report, 'status': 'delivery_window'}
    deadline = min(clock() + budget, deadline_monotonic) if deadline_monotonic is not None else clock() + budget
    if denominator_rows is None:
        from scrapers.transfermarkt.denominator import load_denominator
        denominator_rows = list(load_denominator().rows.values())
    registry_rows = list(registry_rows)
    targets = current_scope_targets(denominator_rows, registry_rows)
    report['denominator_competition_ids'] = list(targets.denominator_competition_ids)
    report['blocked_competitions'] = dict(targets.blocked_competitions)
    report['extra_competition_ids'] = sorted({target.competition_id for target in targets.targets}
                                           - set(targets.denominator_competition_ids))
    report['current_targets'] = [{'scope_id': target.scope_id, 'competition_id': target.competition_id,
        'edition_id': target.edition_id, 'registry_snapshot_id': target.registry_snapshot_id,
        'queue_rank': getattr(target, 'queue_rank', 0)} for target in targets.targets]
    denominator_hash = semantic_signature({
        'competition_ids': targets.denominator_competition_ids,
        'current_editions': [target for target in report['current_targets'] if target['queue_rank'] == 0],
        'blocked_competitions': targets.blocked_competitions,
    })
    report['denominator_hash'] = denominator_hash
    if roster_writer is None or career_writer is None:
        from dags.utils.transfermarkt_current_write import write_current_roster, fetch_current_career
        roster_writer = roster_writer or write_current_roster
        career_writer = career_writer or fetch_current_career
    if baseline_verifier is None and roster_writer.__module__ == 'dags.utils.transfermarkt_current_write':
        from dags.utils.transfermarkt_current_write import verify_current_roster_baseline
        baseline_verifier = verify_current_roster_baseline
    if coach_writer is None and scraper_factory is None:
        from dags.utils.transfermarkt_current_write import fetch_current_coaches
        coach_writer = fetch_current_coaches
    factory = scraper_factory or _production_factory
    directory = Path(state_dir or os.environ.get('TM_CURRENT_STATE_DIR', '/opt/airflow/logs/transfermarkt-native-v2/current'))
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'writer.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = _CurrentStore(directory, started, now)
        state, player_facts = store.index, store.player_facts
        cursors = {key: ScopeCursor.from_mapping(data['cursor']) for key, data in state['scopes'].items() if data.get('cursor')}
        for scope_id, cursor in list(cursors.items()):
            if state['scopes'][scope_id].get('denominator_hash') != denominator_hash:
                cursors[scope_id] = replace(cursor, listing_checked_at=None, players_checked_at=None, injury_checked_at=None)
        from dags.utils.transfermarkt_season_handoff import closed_targets, ddl as season_ddl, mark_sql as season_mark_sql, TABLE as SEASON_TABLE
        prior_targets = state.get('current_targets', [])
        closing = {item.scope_id: item for item in closed_targets(prior_targets, registry_rows)}
        pending_closures = state.setdefault('season_closures', {})
        newly_detected_close = any(target.scope_id not in pending_closures for target in closing.values())
        for target in closing.values():
            pending_closures.setdefault(target.scope_id, {
                'scope_id': target.scope_id, 'competition_id': target.competition_id,
                'edition_id': target.edition_id, 'tier': target.tier})
        production_handoff = roster_writer.__module__ == 'dags.utils.transfermarkt_current_write'
        if production_handoff:
            from dags.utils import transfermarkt_native_v2 as season_control
            from scrapers.transfermarkt.registry import deterministic_scope_id
            connection = _bound_connection(season_control.connect(), deadline, clock)
            cursor = connection.cursor()
            try:
                cursor.execute(season_ddl())
                cursor.fetchall()
                cursor.execute(f"SELECT scope_id, competition_id, edition_id FROM {SEASON_TABLE} WHERE status='pending'")
                for scope_id, competition, edition in cursor.fetchall():
                    pending_closures.setdefault(str(scope_id), {'scope_id': str(scope_id),
                        'competition_id': str(competition), 'edition_id': str(edition), 'tier': 0})
            finally:
                cursor.close()
                connection.close()
        closing = {target.scope_id: target for target in closed_targets(pending_closures.values(), registry_rows)}
        state['current_targets'] = [{'scope_id': target.scope_id, 'competition_id': target.competition_id,
                                    'edition_id': target.edition_id, 'tier': target.tier}
                                   for target in targets.targets]
        ordinary = plan_current_work(targets.targets, cursors, now=started, limit=max_scopes, turn=state['turn'])
        closing_work = tuple(CurrentWork(target, 'season_close') for target in sorted(
            closing.values(), key=lambda target: (
                cursors.get(target.scope_id, ScopeCursor(target.scope_id)).last_work_at or datetime.min.replace(tzinfo=timezone.utc),
                target.cold_priority)))
        # A failed close cannot occupy every current slot on every portion.
        close_quota = max(1, max_scopes // 2) if ordinary else max_scopes
        if max_scopes == 1 and ordinary and state['turn'] % 2 and not newly_detected_close:
            work = ordinary[:1]
        else:
            work = closing_work[:close_quota] + ordinary[:max_scopes - min(close_quota, len(closing_work))]

        state['turn'] += 1
        save = store.save
        save()
        rows = {(row['competition_id'], str(row['edition_id'])): row for row in registry_rows}
        for work_index, item in enumerate(work):
            if clock() + 60 >= deadline:
                break
            target = item.target
            data = store.activate(target.scope_id)
            if data.get('denominator_hash') != denominator_hash and data.get('cursor'):
                cursor = ScopeCursor.from_mapping(data['cursor'])
                data['cursor'] = replace(cursor, listing_checked_at=None, players_checked_at=None, injury_checked_at=None).as_dict()
            data['denominator_hash'] = denominator_hash
            cache = data.setdefault('response_cache', {})
            result = {'scope_id': target.scope_id, 'kind': item.kind, 'status': 'pending'}
            child_cycle_id = 'tm-current-' + semantic_signature([cycle_id, target.scope_id, item.kind, work_index])[:32]
            result['child_cycle_id'] = child_cycle_id
            previous_child_cycle = os.environ.get('TM_CHILD_CYCLE_ID')
            scraper, scope = None, None
            try:
                if data.get('in_flight'):
                    raise CurrentPortionError('previous portion has unsettled traffic; reconcile immutable gateway evidence before paid retry')
                if data.get('grant_cycle_id') != cycle_id:
                    if data.get('grant_cycle_id'):
                        data.setdefault('closed_grants', []).append({
                            'cycle_id': data['grant_cycle_id'], 'traffic': data.get('traffic_used', {}),
                            'by_entity': data.get('traffic_used_by_entity', {}),
                        })
                    # #1652/#1658 resume in a new child grant. Keep every old
                    # receipt; refresh only the per-child admission counters.
                    data['grant_cycle_id'] = cycle_id
                    data['traffic_used'], data['traffic_used_by_entity'] = {}, {}
                data['in_flight'] = cycle_id
                save()
                os.environ['TM_CHILD_CYCLE_ID'] = child_cycle_id
                scraper = factory(target, rows[target.competition_id, target.edition_id], cache,
                                  deadline - 60, data.get('traffic_used', {}))
                scraper._current_deadline_monotonic = deadline
                scraper._current_child_cycle_id = child_cycle_id
                scraper._current_monotonic_fn = clock
                original_connection = scraper._bronze_connection

                def bounded_connection():
                    return _bound_connection(original_connection(), deadline, clock)

                scraper._bronze_connection = bounded_connection
                _install_entity_limits(scraper, data)
                for sql in build_current_state_tables():
                    _sql(scraper, sql)
                _sql(scraper, season_ddl())
                _sql(scraper, season_mark_sql(target, status='pending' if item.kind == 'season_close' else 'current', at=now()))
                if not data.get('snapshot') and not data.get('roster_write_intent'):
                    if target.scope_id in bootstrap_entries:
                        if bootstrap_verifier is None:
                            try:
                                from dags.utils.transfermarkt_current_write import verify_current_complete_scope
                                bootstrap_verifier = verify_current_complete_scope
                            except ImportError as exc:
                                raise CurrentPortionError('baseline_required: complete-scope verifier is unavailable') from exc
                        data.update(seed_current_baseline(scraper, bootstrap_entries[target.scope_id],
                                                          preflight, bootstrap_verifier))
                        save()
                    elif _existing_bronze_roster(scraper, target.competition_id, target.edition_id):
                        raise CurrentPortionError('baseline_required: existing Bronze scope has no qualified activation seed')
                scope = _Scope(target, rows[target.competition_id, target.edition_id], data, scraper,
                               cycle_id, denominator_hash, save, now, clock, deadline, preflight,
                               roster_writer, career_writer, coach_writer, player_facts, store.save_player_packet,
                               baseline_verifier, store.invalidate_player_fact)
                if item.kind == 'signals':
                    for kind in item.checks_due:
                        try:
                            getattr(scope, {'listing': 'listing', 'players': 'players', 'injury': 'injuries'}[kind])()
                        except Exception as exc:
                            error = type(exc).__name__ + ': ' + redact_sensitive(str(exc))
                            result.setdefault('check_failures', {})[kind] = error
                            scope.check(kind, [], 0, 0, status='uncertain', result=error)
                else:
                    if item.kind == 'season_close':
                        if not data.get('season_close_roster_captured'):
                            first_close_portion = not data.get('season_close_roster_started')
                            if first_close_portion:
                                scope.listing()
                                data['season_close_roster_started'] = True
                                scope.persist()
                            scope.roster(weekly=first_close_portion)
                            data['season_close_roster_captured'] = True
                            scope.persist()
                    else:
                        scope.roster(weekly=item.kind == 'weekly_roster')
                    # Weekly insurance discovers changes but does not repeat
                    # careers. New/changed IDs continue in a subsequent portion.
                    if item.kind != 'weekly_roster':
                        scope.careers()
                    coach_checked = data.get('coaches_checked_at')
                    if (not scope.cursor.cold_complete or scope.resume.get('coaches_pending')
                        or coach_checked is None or now() - _utc(coach_checked) >= timedelta(days=28)):
                        scope.coaches()
                    if not any(scope.resume.get('careers', {}).values()) and not scope.resume.get('required') and not scope.resume.get('coaches_pending'):
                        scope.cursor = replace(scope.cursor, cold_complete=True)
                        scope.resume = {}
                scope.cursor = replace(scope.cursor, last_work_at=now())
                scope.persist()
                if daily_complete(scope.cursor, now()):
                    report['daily_complete_scope_ids'].append(target.scope_id)
                result['status'] = 'complete' if not scope.resume else 'pending'
                if item.kind == 'season_close' and result['status'] == 'complete':
                    if not data.get('season_close_roster_captured'):
                        raise CurrentPortionError('season close requires a final committed full roster')
                    proof = {'cycle_id': cycle_id, 'captured_at': now().isoformat(),
                             'career_proofs': data.get('career_proofs', []),
                             'coach_proofs': data.get('coach_proofs', [])}
                    if scope.snapshot is not None:
                        proof.update(snapshot_sha256=semantic_signature(scope.snapshot.as_dict()),
                            club_receipts={club_id: {'raw_capture_id': club.raw_capture_id,
                                                   'source_body_hash': club.source_body_hash,
                                                   'bronze_manifest': club.bronze_manifest}
                                           for club_id, club in scope.snapshot.clubs.items()})
                    elif data.get('authoritative_empty_scope') and not scraper._bronze_scope_has_roster(
                            scope.scope['compatibility_league'], scope.scope['canonical_season']):
                        proof['authoritative_empty_scope'] = data['authoritative_empty_scope']
                        proof['physical_roster_empty'] = True
                    else:
                        raise CurrentPortionError('season close lacks committed roster or raw-proven physical emptiness')
                    _sql(scraper, season_mark_sql(target, status='complete', at=now(), proof=proof))
                    pending_closures.pop(target.scope_id, None)
                    data['season_close_handed_off_at'] = now().isoformat()
                    save()
                    result['historical_handoff'] = True
                if result.get('check_failures'):
                    result['status'] = 'uncertain'
            except Exception as exc:
                result['status'], result['error'] = 'failed', type(exc).__name__ + ': ' + redact_sensitive(str(exc))
                if 'baseline_required:' in str(exc):
                    result['status'] = 'baseline_required'
                if scope is not None:
                    scope.cursor = replace(scope.cursor, last_work_at=now())
                    scope.persist()
                else:
                    cursor = ScopeCursor.from_mapping(data['cursor']) if data.get('cursor') else ScopeCursor(target.scope_id)
                    data['cursor'] = replace(cursor, last_work_at=now()).as_dict()
                    save()
            finally:
                try:
                    if scraper is not None:
                        # Closing settles the final lease snapshot before accounting.
                        try:
                            scraper.close()
                            data.pop('in_flight', None)
                        except Exception as exc:
                            result['status'], result['error'] = 'failed', 'unsettled lease: ' + redact_sensitive(str(exc))
                        finally:
                            traffic = dict(scraper.get_traffic_stats())
                            ledger = traffic.get('shared_traffic_ledger', traffic)
                            entry = {'cycle_id': cycle_id, 'child_cycle_id': child_cycle_id, 'kind': item.kind,
                                     'captured_at': now().isoformat(), 'traffic': traffic}
                            entry['raw_captures'] = list(scraper.get_raw_capture_records())
                            entry['raw_attempts'] = list(scraper.get_raw_attempt_records())
                            entry['cache_sources'] = list(scraper.get_cache_source_records())
                            data.setdefault('traffic_receipts', []).append(entry)
                            used = data.setdefault('traffic_used', {})
                            for name in ('provider_metered_bytes', 'requests', 'retries', 'decoded_bytes'):
                                used[name] = used.get(name, 0) + int(ledger.get(name, 0))
                            accumulated = data.setdefault('traffic_used_by_entity', {})
                            for name, counters in ledger.get('by_entity', {}).items():
                                destination = accumulated.setdefault(name, {})
                                for counter in ('requests', 'retries', 'decoded_bytes', 'provider_bytes'):
                                    destination[counter] = destination.get(counter, 0) + int(counters.get(counter, 0))
                            result['traffic'] = traffic
                            save()
                    elif data.get('in_flight') == cycle_id:
                        # Factory failures before constructing a client cannot
                        # have issued paid HTTP. Preserve older unresolved runs.
                        data.pop('in_flight', None)
                        save()
                finally:
                    if previous_child_cycle is None:
                        os.environ.pop('TM_CHILD_CYCLE_ID', None)
                    else:
                        os.environ['TM_CHILD_CYCLE_ID'] = previous_child_cycle
            if data.get('roster_reconciliations'):
                result['roster_reconciliations'] = data['roster_reconciliations'][-1:]
            if data.get('recovery_signal_refresh_required'):
                result['fresh_signals_required'] = data['recovery_signal_refresh_required']
            report['scopes'].append(result)
        report['status'] = 'failed' if any(item['status'] in {'failed', 'baseline_required'} for item in report['scopes']) else 'pending'
        report['daily_complete_scope_ids'] = list(dict.fromkeys(report['daily_complete_scope_ids']))
        report['finished_at'] = now().isoformat()
        report_path = directory / ('portion-' + hashlib.sha256(cycle_id.encode()).hexdigest()[:24] + '.json')
        _atomic(report_path, report)
        report['report_path'] = str(report_path)
    return report


def main(argv=None):
    """Isolated source-image entry point; the DAG owns job/report destinations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args(argv)
    job = json.loads(args.job.read_text())
    now = datetime.now(timezone.utc)
    deadline_at = _utc(job['deadline_at'])
    remaining = (deadline_at - now).total_seconds()
    if remaining <= 0 or available_work_seconds(now) <= 0:
        report = {'policy_version': CURRENT_POLICY_VERSION, 'cycle_id': job['cycle_id'],
                  'status': 'deadline' if remaining <= 0 else 'delivery_window', 'scopes': [],
                  'full_scope_completed': False, 'started_at': now.isoformat()}
    else:
        try:
            with _portion_alarm(max(.01, remaining - 5)):
                report = run_current_portion(
                    job['registry_rows'], job['preflight'], job['cycle_id'],
                    qualification=job.get('qualification'), denominator_rows=job.get('denominator_rows'),
                    state_dir=job.get('state_dir'), max_seconds=min(2700, remaining),
                    max_scopes=job.get('max_scopes', 8),
                    deadline_monotonic=time.monotonic() + remaining,
                )
        except Exception as exc:
            report = {'policy_version': CURRENT_POLICY_VERSION, 'cycle_id': job['cycle_id'],
                      'status': 'failed', 'scopes': [], 'full_scope_completed': False,
                      'fatal_error': type(exc).__name__ + ': ' + redact_sensitive(str(exc))}
    if args.report.exists():
        if json.loads(args.report.read_text()) != report:
            raise CurrentPortionError('immutable portion result already exists with different evidence')
    else:
        _atomic(args.report, report)
    return 1 if report.get('fatal_error') else 0


@contextmanager
def _portion_alarm(seconds):
    """Absolute wall time for this isolated Linux CLI, including blocking I/O."""
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()

    def expired(signum, frame):
        raise CurrentPortionError('whole current portion reached its absolute deadline')

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, max(.001, previous_timer[0] - (time.monotonic() - started)),
                             previous_timer[1])


if __name__ == '__main__':
    raise SystemExit(main())
