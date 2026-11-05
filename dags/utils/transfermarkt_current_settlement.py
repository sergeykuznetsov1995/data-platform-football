"""Recover a completed current portion only from final durable gateway evidence."""
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import json
import time
from pathlib import Path


class CurrentSettlementError(ValueError):
    """A previous paid operation is not yet proven settled."""


def _integer(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CurrentSettlementError('invalid gateway byte counter')
    return value


def _events(directory):
    directory = Path(directory)
    paths = sorted(directory.glob('paid_requests.*.jsonl.gz'))
    current = directory / 'paid_requests.jsonl'
    if not current.is_file():
        raise CurrentSettlementError('current gateway ledger is missing')
    for path in paths + [current]:
        opener = gzip.open if path.suffix == '.gz' else open
        try:
            with opener(path, 'rt', encoding='utf-8') as handle:
                for line in handle:
                    if line.strip():
                        value = json.loads(line)
                        if not isinstance(value, dict):
                            raise ValueError('event is not an object')
                        yield value
        except (OSError, ValueError, EOFError) as exc:
            raise CurrentSettlementError('gateway ledger is incomplete or unreadable') from exc


class CurrentSettlement:
    """Bounded-memory ledger scan; keep only the requested child IDs."""
    def __init__(self, ledger_directory, *, events=None):
        self.directory = ledger_directory
        self.events = events

    def recover(self, data, *, scope_id, directory, now, save, deadline=None, clock=time.monotonic):
        cycle = data.get('in_flight')
        if not cycle:
            return
        report_path = Path(directory) / ('portion-' + hashlib.sha256(cycle.encode()).hexdigest()[:24] + '.json')
        try:
            report = json.loads(report_path.read_text())
        except (OSError, ValueError) as exc:
            raise CurrentSettlementError('previous portion has no completed report; retain in_flight') from exc
        if report.get('cycle_id') != cycle or not report.get('finished_at'):
            raise CurrentSettlementError('previous portion report identity/completion differs')
        entries = [entry for entry in report.get('scopes', []) if entry.get('scope_id') == scope_id]
        paid = {entry['child_cycle_id']: entry for entry in entries if 'traffic' in entry}
        if not paid or not any(str(e.get('error', '')).startswith('unsettled lease:') for e in paid.values()):
            raise CurrentSettlementError('previous portion does not prove a completed failed close')
        receipts = [r for r in data.get('traffic_receipts', []) if r.get('cycle_id') == cycle]
        by_child = {r.get('child_cycle_id'): r for r in receipts}
        if len(by_child) != len(receipts) or set(by_child) != set(paid):
            raise CurrentSettlementError('previous paid children and receipts differ')
        leases, seen = {}, {}
        for event in self.events if self.events is not None else _events(self.directory):
            if deadline is not None and clock() >= deadline:
                raise CurrentSettlementError('gateway reconciliation reached work deadline; retain in_flight')
            if event.get('run_id') not in paid:
                continue
            if event.get('source') != 'transfermarkt' or event.get('event_version') != 'paid-proxy-v2':
                raise CurrentSettlementError('unsupported gateway event schema or source')
            if event.get('event_type') not in {'lease_created', 'bytes', 'lease_closed', 'connect_rejected'}:
                raise CurrentSettlementError('unsupported gateway event type')
            if event.get('scope') != scope_id or event.get('dag_id') != 'dag_ingest_transfermarkt':
                raise CurrentSettlementError('gateway child scope identity differs')
            event_id = event.get('event_id')
            if not isinstance(event_id, str) or not event_id:
                raise CurrentSettlementError('gateway event identity is missing')
            if event_id in seen:
                if seen[event_id] != event:
                    raise CurrentSettlementError('gateway event identity drifted')
                continue
            seen[event_id] = event
            lease_id = event.get('lease_id')
            if not isinstance(lease_id, str) or not lease_id:
                raise CurrentSettlementError('gateway lease identity is missing')
            lease = leases.setdefault(lease_id, {'child': event['run_id'], 'created': False,
                                                'closed': False, 'up': 0, 'down': 0, 'total': None, 'uncertain': False})
            if lease['child'] != event['run_id']:
                raise CurrentSettlementError('gateway lease changed child identity')
            kind = event.get('event_type')
            if kind == 'lease_created':
                if lease['created'] or lease['closed']:
                    raise CurrentSettlementError('duplicate/out-of-order lease creation')
                lease['created'] = True
            elif kind == 'bytes':
                if not lease['created'] or lease['closed'] or event.get('direction') not in ('up', 'down'):
                    raise CurrentSettlementError('bytes outside proven lease lifecycle')
                lease[event['direction']] += _integer(event.get('bytes'))
                if event.get('host') == 'uncertain-read-ahead':
                    # filter_proxy._reap_expired_leases emits this exact marker
                    # only after no active tunnels remain and the entire retained
                    # reservation is durably charged. Uncertainty stays latched;
                    # this proves conservative settlement, not exact provider spend.
                    if (event.get('source') != 'transfermarkt' or event.get('event_version') != 'paid-proxy-v2'
                            or event['direction'] != 'down' or event['bytes'] <= 0):
                        raise CurrentSettlementError('invalid conservative settlement marker')
                    lease['closed'], lease['uncertain'] = True, True
                    lease['total'] = _integer(event.get('lease_total_bytes'))
            elif kind == 'lease_closed':
                if not lease['created'] or lease['closed']:
                    raise CurrentSettlementError('duplicate/out-of-order lease close')
                lease['closed'], lease['total'] = True, _integer(event.get('total_bytes'))
            elif kind == 'connect_rejected':
                # Gateway diagnostic only; CONNECT traffic has separate bytes
                # events. A rejection never substitutes for a terminal receipt.
                if (not lease['created'] or lease['closed'] or not isinstance(event.get('reason'), str)
                        or not event['reason'].strip()):
                    raise CurrentSettlementError('invalid connection rejection evidence')
        totals = {child: {'up': 0, 'down': 0, 'leases': [], 'uncertain_leases': []} for child in paid}
        for lease_id, lease in leases.items():
            if not lease['closed'] or lease['total'] != lease['up'] + lease['down']:
                raise CurrentSettlementError('gateway lease is open or final bytes differ')
            target = totals[lease['child']]
            target['up'] += lease['up']; target['down'] += lease['down']; target['leases'].append(lease_id)
            if lease['uncertain']:
                target['uncertain_leases'].append(lease_id)
        updated = deepcopy(data)
        delta_total = 0
        for receipt in [r for r in updated['traffic_receipts'] if r.get('cycle_id') == cycle]:
            child = receipt['child_cycle_id']; total = totals[child]
            if not total['leases']:
                raise CurrentSettlementError('paid child has no final gateway lease evidence')
            traffic = receipt['traffic']; ledger = traffic.get('shared_traffic_ledger', traffic)
            previous_up = _integer(ledger.get('provider_up_bytes', 0))
            previous_down = _integer(ledger.get('provider_down_bytes', 0))
            if _integer(ledger.get('provider_metered_bytes', 0)) != previous_up + previous_down:
                raise CurrentSettlementError('receipt provider counters disagree')
            up, down = total['up'] - previous_up, total['down'] - previous_down
            if min(up, down) < 0:
                raise CurrentSettlementError('gateway final counters are below recorded receipt')
            receipt['traffic_before_reconciliation'] = deepcopy(traffic)
            for item in (traffic, ledger) if traffic is not ledger else (ledger,):
                item.update(provider_up_bytes=total['up'], provider_down_bytes=total['down'],
                            provider_metered_bytes=total['up'] + total['down'])
            entity = ledger.setdefault('by_entity', {}).setdefault('lease_close', {})
            for key, value in [('provider_up_bytes', up), ('provider_down_bytes', down), ('provider_bytes', up + down)]:
                entity[key] = _integer(entity.get(key, 0)) + value
            receipt['settlement'] = {'at': now.isoformat(), 'lease_ids': sorted(total['leases']),
                                     'accounting_uncertain_lease_ids': sorted(total['uncertain_leases']),
                                     'counter_basis': 'conservatively_charged' if total['uncertain_leases'] else 'gateway_final',
                                     'gateway_event_ids': sorted(k for k, e in seen.items() if e['run_id'] == child)}
            delta_total += up + down
        if updated.get('grant_cycle_id') != cycle:
            raise CurrentSettlementError('unsettled receipt grant identity differs')
        used = updated.setdefault('traffic_used', {})
        used['provider_metered_bytes'] = _integer(used.get('provider_metered_bytes', 0)) + delta_total
        entity = updated.setdefault('traffic_used_by_entity', {}).setdefault('lease_close', {})
        entity['provider_bytes'] = _integer(entity.get('provider_bytes', 0)) + delta_total
        updated.setdefault('traffic_reconciliations', []).append({'cycle_id': cycle, 'at': now.isoformat(),
                                                                 'provider_delta_bytes': delta_total})
        updated.pop('in_flight')
        data.clear(); data.update(updated)
        save()
