"""Local qualification admission; caller holds the experiment's existing flock."""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

MSK = ZoneInfo('Europe/Moscow')


class GuardHalt(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


class GuardWait(ValueError):
    def __init__(self, until):
        self.until = until
        super().__init__('transport_backoff')


def quota_key(url):
    """Do not buy more responses by changing a club slug or query ordering."""
    parsed = urlsplit(url)
    club = re.fullmatch(r'/[^/]+/kader/verein/(\d+)(?:/saison_id/\d+)?/plus/1', parsed.path)
    path = f'/club/kader/verein/{club[1]}/plus/1' if club else parsed.path
    edition = re.fullmatch(r'/[^/]+/startseite/(wettbewerb|pokalwettbewerb)/([\w-]+)', path)
    if edition:
        path = f'/competition/startseite/{edition[1]}/{edition[2]}'
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path,
                       urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True))), ''))


def attempt_id(record):
    return record.get('envelope_id') or hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def _transport(record, proven=()):
    # An actual 5xx response without provenance is not a free source response.
    return (record.get('outcome_kind') == 'transport_error' or record.get('status_code') in (0, 407)
            or attempt_id(record) in proven)


class QualificationGuard:
    def __init__(self, experiment, now, write):
        self.path = Path(experiment) / 'qualification-guard.json'
        self.now, self.day, self.write = now, now.astimezone(MSK).date().isoformat(), write
        if self.path.exists():
            self.data = json.loads(self.path.read_text())
            self._validate()
            if self.data['version'] == 1:
                self.data.update(version=2, response_window=[], captcha_streaks={})
                self._restore_policy_history(Path(experiment))
                self.save()
        else:
            self.data = {'version': 2, 'days': {}, 'stop': None, 'response_window': [], 'captcha_streaks': {}}
            self._adopt_history(Path(experiment))
            self._restore_policy_history(Path(experiment))
            self.save()

    def _validate(self):
        if (type(self.data.get('version')) is not int or self.data['version'] not in (1, 2)
                or set(self.data) != ({'version', 'days', 'stop'} if self.data['version'] == 1 else
                                      {'version', 'days', 'stop', 'response_window', 'captcha_streaks'})
                or not isinstance(self.data['days'], dict)):
            raise ValueError('unsupported qualification guard')
        for day, urls in self.data['days'].items():
            if date.fromisoformat(day).isoformat() != day or not isinstance(urls, dict):
                raise ValueError('invalid qualification day')
            for url, counts in urls.items():
                if (not isinstance(url, str) or not url or not isinstance(counts, dict)
                        or not {'responses', 'transport'} <= set(counts)
                        or set(counts) - {'responses', 'transport', 'retry_after'}
                        or any(type(counts[k]) is not int or counts[k] < 0 for k in ('responses', 'transport'))):
                    raise ValueError('invalid qualification counters')
                if 'retry_after' in counts and datetime.fromisoformat(counts['retry_after']).tzinfo is None:
                    raise ValueError('invalid qualification retry clock')
        if self.data['version'] == 2:
            if (not isinstance(self.data['response_window'], list)
                    or not isinstance(self.data['captcha_streaks'], dict)
                    or any(not isinstance(url, str) or type(n) is not int or n < 0
                           for url, n in self.data['captcha_streaks'].items())):
                raise ValueError('invalid qualification CAPTCHA history')
            for event in self.data['response_window']:
                if (not isinstance(event, dict) or set(event) != {'at', 'captcha'}
                        or type(event['captcha']) is not bool
                        or datetime.fromisoformat(event['at']).tzinfo is None):
                    raise ValueError('invalid qualification response clock')
        stop = self.data['stop']
        if stop is not None and (not isinstance(stop, dict) or not stop.get('reason')
                                 or not isinstance(stop['reason'], str)):
            raise ValueError('invalid qualification stop')

    def save(self):
        self.write(self.path, self.data)

    def counts(self, url, day=None):
        return self.data['days'].setdefault(day or self.day, {}).setdefault(
            quota_key(url), {'responses': 0, 'transport': 0})

    def _adopt_history(self, experiment):
        """Import immutable attempts once; copies cannot replenish today's quota."""
        seen = set()
        for path in sorted(experiment.rglob('state.json')):
            state = json.loads(path.read_text())
            if (not isinstance(state, dict) or not isinstance(state.get('attempt_records'), list)
                    or not isinstance(state.get('identity'), str)
                    or not re.fullmatch('[0-9a-f]{64}', state['identity'])):
                continue
            if state.get('qualification_stop') and not self.data['stop']:
                self.data['stop'] = state['qualification_stop']
            recorded, days = {}, set()
            proven = state.get('qualification_transport_proofs', {})
            if not isinstance(proven, dict):
                raise ValueError('invalid qualification transport proofs')
            for record in state['attempt_records']:
                url = record.get('url')
                if not url:
                    continue
                stamp = record.get('observed_at')
                day = (state.get('qualification_day_msk') or
                       (datetime.fromisoformat(stamp.replace('Z', '+00:00')).astimezone(MSK).date().isoformat()
                        if stamp else datetime.fromtimestamp(path.stat().st_mtime, MSK).date().isoformat()))
                days.add(day)
                kind = 'transport' if _transport(record, proven) else 'responses'
                captcha = record.get('qualification_captcha', record.get('status_code') in (403, 405, 429))
                # New states exclude confirmed CAPTCHA from their URL quota.
                # Legacy states counted it; retain their reservation convention.
                recorded.setdefault(url, {'responses': 0, 'transport': 0})[kind] += int(
                    kind == 'transport' or not record.get('qualification_captcha', False))
                key = attempt_id(record)
                if key not in seen:
                    self.counts(url, day)[kind] += int(not captcha or kind == 'transport')
                    seen.add(key)
            # Keep unknown pre-I/O reservations; do not turn old ambiguity into I/O.
            fallback_day = (state.get('qualification_day_msk') or
                            (next(iter(days)) if len(days) == 1 else
                             datetime.fromtimestamp(path.stat().st_mtime, MSK).date().isoformat()))
            for url, counts in state.get('url_attempts', {}).items():
                for kind in ('responses', 'transport'):
                    self.counts(url, fallback_day)[kind] += max(
                        0, counts.get(kind, 0) - recorded.get(url, {}).get(kind, 0))
            for url, until in state.get('qualification_retry_after', {}).items():
                counts = self.counts(url, fallback_day)
                if ('retry_after' not in counts or
                        datetime.fromisoformat(until) > datetime.fromisoformat(counts['retry_after'])):
                    counts['retry_after'] = until
        self._validate()

    def _restore_policy_history(self, experiment):
        seen, records = set(), []
        for path in sorted(experiment.rglob('state.json')):
            state = json.loads(path.read_text())
            if (not isinstance(state, dict) or not isinstance(state.get('attempt_records'), list)
                    or not isinstance(state.get('identity'), str)
                    or not re.fullmatch('[0-9a-f]{64}', state['identity'])):
                continue
            for record in state['attempt_records']:
                key = attempt_id(record)
                if key in seen or not record.get('url'):
                    continue
                seen.add(key)
                stamp = record.get('qualification_observed_at') or record.get('observed_at')
                if stamp:
                    records.append((datetime.fromisoformat(stamp.replace('Z', '+00:00')), record,
                                    state.get('qualification_transport_proofs', {})))
        for stamp, record, proven in sorted(records, key=lambda item: item[0]):
            captcha = record.get('qualification_captcha', record.get('status_code') in (403, 405, 429))
            self._policy_response(record['url'], captcha, _transport(record, proven), stamp)

    def _policy_response(self, url, captcha, transport, stamp):
        key = quota_key(url)
        streaks = self.data['captcha_streaks']
        streaks[key] = streaks.get(key, 0) + 1 if captcha else 0
        if not transport:
            self.data['response_window'].append({'at': stamp.isoformat(), 'captcha': captcha})
        cutoff = self.now - timedelta(seconds=3600)
        self.data['response_window'] = [event for event in self.data['response_window']
                                       if datetime.fromisoformat(event['at']) > cutoff]

    def halt(self, reason, url=None):
        if not self.data.get('stop'):
            self.data['stop'] = {'reason': reason, 'url': url, 'observed_at': self.now.isoformat()}
            self.save()
        raise GuardHalt(self.data['stop']['reason'])

    def check(self, task, tasks, results, endpoint_limit, transport_limit):
        if self.data.get('stop'):
            raise GuardHalt(self.data['stop']['reason'])
        url = task['url']
        counts = self.counts(url)
        required = sum(t['key'] not in results and quota_key(t['url']) == quota_key(url) for t in tasks)
        if counts['responses'] + required > endpoint_limit:
            self.halt('url_response_limit', url)
        if counts['transport'] >= transport_limit:
            self.halt('transport_limit', url)
        until = counts.get('retry_after')
        if until and self.now < datetime.fromisoformat(until):
            raise GuardWait(until)

    def reserve(self, url):
        self.counts(url)['responses'] += 1
        self.save()

    def observe(self, url, attempts, status_delta, records, client, outcome, *, proven_transport=(), transport_limit=4):
        self.last_captcha = False
        blocked = any(status_delta.get(str(code), 0) for code in (403, 405, 429))
        blocked |= any(r.get('status_code') in (403, 405, 429) for r in records)
        blocked |= getattr(getattr(outcome, 'status', None), 'value', None) == 'blocked'
        if not blocked:
            for record in records:
                if record.get('status_code') == 200 and record.get('capture_id'):
                    body, _ = client._raw_store.load_capture(record['capture_id'])
                    text = body.decode('utf-8', errors='replace').lower()
                    blocked |= bool(re.search(r'<title>\s*human verification\s*</title>', text)
                                    and '.captcha.awswaf.com/' in text)
        self.last_captcha = blocked
        counts = self.counts(url)
        transport = max(sum(_transport(r, proven_transport) for r in records),
                        status_delta.get('0', 0) + status_delta.get('407', 0))
        if attempts:
            counts['responses'] += attempts - min(attempts, transport) - int(blocked) - 1
            counts['transport'] += min(attempts, transport)
            self._policy_response(url, blocked, bool(transport), self.now)
            if transport:
                delay = (10, 30, 60)[min(counts['transport'] - 1, 2)]
                counts['retry_after'] = (self.now + timedelta(seconds=delay)).isoformat()
        self.save()
        if self.data['captcha_streaks'].get(quota_key(url), 0) >= 3:
            self.halt('captcha_streak', url)
        window = self.data['response_window']
        if len(window) >= 100 and sum(event['captcha'] for event in window) * 100 > len(window):
            self.halt('captcha_rate', url)
        server_responses = sum(n for s, n in status_delta.items() if int(s) >= 500)
        proven_server_errors = sum(bool(r.get('status_code')) and r['status_code'] >= 500
                                  and _transport(r, proven_transport) for r in records)
        if server_responses > proven_server_errors:
            self.halt('unproven_site_5xx', url)
        if counts['transport'] >= transport_limit:
            self.halt('transport_limit', url)
        if not blocked and getattr(getattr(outcome, 'status', None), 'value', None) == 'schema_error':
            self.halt('schema_error', url)
        return blocked
