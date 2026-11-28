"""Transfermarkt stream capacity and restart-safe no-burst rate state.

Only the dedicated TM gateway enables these streams. A slot is permanent:
replacing its HTTP client or lease never changes the rate identity.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Mapping


@dataclass(frozen=True)
class TransfermarktStreams:
    history_streams: int = 0
    requests_per_minute: int = 12
    current_streams: int = 1

    def __post_init__(self):
        if type(self.history_streams) is not int or not 0 <= self.history_streams <= 3:
            raise ValueError('TM history streams must be in 0..3')
        if type(self.current_streams) is not int or self.current_streams != 1:
            raise ValueError('TM reserves exactly one current stream')
        if type(self.requests_per_minute) is not int or not 1 <= self.requests_per_minute <= 12:
            raise ValueError('TM per-stream requests/minute must be in 1..12')

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None):
        env = os.environ if env is None else env
        return cls(history_streams=int(env.get('TM_HISTORY_STREAMS', '0')),
                   requests_per_minute=int(env.get('TM_REQUESTS_PER_MINUTE', '12')))

    @property
    def current_capacity(self):
        return self.current_streams

    @property
    def history_capacity(self):
        return self.history_streams

    @property
    def total_streams(self):
        return 1 + self.history_streams

    def ids(self, source: str):
        if source == 'transfermarkt':
            return ('current-0',)
        if source == 'transfermarkt_backfill':
            return tuple(f'history-{i}' for i in range(self.history_streams))
        return ()

    @property
    def stream_ids(self):
        return self.ids('transfermarkt') + self.ids('transfermarkt_backfill')


def browser_profile(stream_id: str):
    """Use supported TLS-client Chrome profiles with matching browser headers.

    Each reserved stream has a stable, coherent header set. The alternating
    profiles never change during retries or lease renewal.
    """
    if stream_id == 'current-0':
        return 'chrome_133', 133
    if stream_id not in {'history-0', 'history-1', 'history-2'}:
        raise ValueError('invalid TM stream identity')
    version = {'history-0': 124, 'history-1': 120, 'history-2': 117}[stream_id]
    return f'chrome_{version}', version


class StreamRateState:
    SCHEMA_VERSION = 2
    SLOT_IDS = frozenset({'current-0', 'history-0', 'history-1', 'history-2'})

    def __init__(self, streams: TransfermarktStreams, now: float, saved=None):
        self.streams = streams
        self.last = {key: self._time(now) for key in sorted(self.SLOT_IDS)}
        self.slow = {key: 0.0 for key in self.last}
        self.pause = {key: 0.0 for key in self.last}
        self.source_slow = 0.0
        self.blocks = []
        if saved is not None:
            self._restore(saved)

    @staticmethod
    def _time(value):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid TM rate timestamp')
        return float(value)

    def _restore(self, saved):
        if not isinstance(saved, dict):
            raise ValueError('invalid TM rate state')
        if type(saved.get('schema_version')) is not int:
            raise ValueError('invalid TM rate state version')
        if saved.get('schema_version') == 1:
            if set(saved) != {'schema_version', 'last_granted_at_epoch'}:
                raise ValueError('invalid legacy TM rate state')
            last = self._time(saved['last_granted_at_epoch'])
            self.last = {key: max(value, last) for key, value in self.last.items()}
            return
        if set(saved) != {'schema_version', 'last', 'slow', 'pause', 'source_slow', 'blocks'} or saved['schema_version'] != 2:
            raise ValueError('unsupported TM rate state')
        allowed = self.SLOT_IDS
        for name in ('last', 'slow', 'pause'):
            values = saved[name]
            if not isinstance(values, dict) or set(values) != allowed:
                raise ValueError('invalid TM stream rate state')
            for key, value in values.items():
                stamp = self._time(value)
                # Temporarily disabling a stream cannot erase its cooldown.
                self.last.setdefault(key, 0.0)
                self.slow.setdefault(key, 0.0)
                self.pause.setdefault(key, 0.0)
                getattr(self, name)[key] = max(getattr(self, name)[key], stamp)
        self.source_slow = self._time(saved['source_slow'])
        if not isinstance(saved['blocks'], list) or len(saved['blocks']) > 12:
            raise ValueError('invalid TM block evidence')
        for item in saved['blocks']:
            if not isinstance(item, list) or len(item) != 2 or item[0] not in allowed:
                raise ValueError('invalid TM block evidence')
            self.blocks.append((item[0], self._time(item[1])))

    def dump(self):
        return dict(schema_version=2, last=self.last, slow=self.slow,
                    pause=self.pause, source_slow=self.source_slow,
                    blocks=[list(item) for item in self.blocks])

    def ready_at(self, stream_id, now):
        if stream_id not in self.streams.stream_ids:
            raise ValueError('stream is disabled')
        interval = 60.0 / self.streams.requests_per_minute
        if now < max(self.slow[stream_id], self.source_slow):
            interval *= 2
        return max(self.last[stream_id] + interval, self.pause[stream_id])

    def grant(self, stream_id, now):
        if now < self.ready_at(stream_id, now):
            raise ValueError('stream is not ready')
        self.last[stream_id] = float(now)

    def site_block(self, stream_id, now, *, status=0, challenge=False):
        if stream_id not in self.streams.stream_ids:
            raise ValueError('stream is disabled')
        if status not in (403, 429) and not challenge:
            return ()
        self.blocks = [(key, stamp) for key, stamp in self.blocks if stamp >= now - 600]
        self.blocks.append((stream_id, float(now)))
        self.blocks = self.blocks[-12:]
        self.slow[stream_id] = max(self.slow[stream_id], now + 1800)
        alerts = []
        if len({key for key, _ in self.blocks}) >= 2:
            self.source_slow = max(self.source_slow, now + 1800)
            alerts.append('transfermarkt_source_rate_halved')
        if sum(key == stream_id for key, _ in self.blocks) >= 4:
            self.pause[stream_id] = max(self.pause[stream_id], now + 900)
            alerts.append('transfermarkt_stream_paused')
        return tuple(alerts)

    def hold(self, now, seconds):
        """Pause every stream until one time; a source stop is such a pause."""
        until = self._time(now) + self._time(seconds)
        for key in self.pause:
            self.pause[key] = max(self.pause[key], until)
        return until

    def release(self, now):
        """Lift a source hold; no stream stays paused beyond it."""
        now = self._time(now)
        for key in self.pause:
            self.pause[key] = min(self.pause[key], now)


SOURCE_STOP_REASONS = frozenset({'human_verification', 'addresses_exhausted'})


class SourceStopState:
    """Durable record of one Transfermarkt source stop and of its single probe.

    Owner decision of 2026-11-28: a refusal on one address is replaced by
    another address, but the source is held for hours once it asks for a human
    or once every address is refused.  When the hold ends, exactly one probe
    request is admitted.  The hold itself is the streams' ordinary pause, so a
    gateway without this record still honours it; the record only decides when
    the probe may run.
    """

    SCHEMA_VERSION = 1
    STOP_SECONDS = 3 * 60 * 60
    PROBE_TIMEOUT_SECONDS = 120
    MAX_FAILED_PROBES = 3
    NEEDS_OWNER_SECONDS = 7 * 24 * 60 * 60
    _FIELDS = {'schema_version', 'reason', 'stopped_at', 'until', 'failed_probes',
               'probe_permit_id', 'probe_started_at', 'probe_sent', 'needs_owner'}

    def __init__(self, saved=None):
        self._clear()
        if saved is not None:
            self._restore(saved)

    def _clear(self):
        self.reason = None
        self.stopped_at = 0.0
        self.until = 0.0
        self.failed_probes = 0
        self.probe_permit_id = None
        self.probe_started_at = 0.0
        self.probe_sent = False
        self.needs_owner = False

    @property
    def active(self):
        return self.reason is not None

    @property
    def probing(self):
        return self.probe_permit_id is not None

    def arm(self, now, reason):
        """Start a stop, or extend one already held; none is ever shortened."""
        if reason not in SOURCE_STOP_REASONS:
            raise ValueError('invalid TM source stop reason')
        now = StreamRateState._time(now)
        if self.needs_owner:
            self.until = max(self.until, now + self.NEEDS_OWNER_SECONDS)
            return self.until
        until = now + self.STOP_SECONDS * 2 ** min(self.failed_probes, self.MAX_FAILED_PROBES - 1)
        if not self.active:
            self.reason, self.stopped_at = reason, now
        self.until = max(self.until, until)
        self.probe_permit_id, self.probe_started_at, self.probe_sent = None, 0.0, False
        return self.until

    def probe_due(self, now):
        return self.active and not self.probing and not self.needs_owner and now >= self.until

    def begin_probe(self, now, permit_id):
        """The permit about to be granted is the one probe of this stop."""
        if not self.probe_due(now):
            raise ValueError('TM source probe is not due')
        self.probe_permit_id = str(permit_id)
        self.probe_started_at = StreamRateState._time(now)
        self.probe_sent = False

    def probe_passed(self):
        """The probe reached the source and was heard: the stop is over."""
        if not self.probing:
            raise ValueError('TM source probe was not started')
        self._clear()

    def mark_sent(self):
        """The probe's permit was consumed: its request is on its way to the source."""
        if not self.probing:
            raise ValueError('TM source probe was not started')
        self.probe_sent = True

    def abandon_probe(self):
        """The probe was granted but never sent; the stop stays and a probe is due again."""
        if not self.probing or self.probe_sent:
            raise ValueError('TM source probe cannot be abandoned')
        self.probe_permit_id, self.probe_started_at, self.probe_sent = None, 0.0, False

    def probe_expired(self, now):
        """A sent probe that was never reported is no evidence of a refusal."""
        return self.probing and self.probe_sent and now >= self.probe_started_at + self.PROBE_TIMEOUT_SECONDS

    def probe_failed(self, now):
        """The probe was refused: hold longer, and stop probing after three."""
        if not self.probing:
            raise ValueError('TM source probe was not started')
        now = StreamRateState._time(now)
        self.failed_probes += 1
        self.probe_permit_id, self.probe_started_at, self.probe_sent = None, 0.0, False
        if self.failed_probes >= self.MAX_FAILED_PROBES:
            self.needs_owner = True
            self.until = now + self.NEEDS_OWNER_SECONDS
        else:
            self.until = now + self.STOP_SECONDS * 2 ** self.failed_probes
        return self.until

    def owner_resume(self):
        """The owner lifts a stop that exhausted its probes."""
        self._clear()

    def _restore(self, saved):
        if not isinstance(saved, dict) or set(saved) != self._FIELDS:
            raise ValueError('invalid TM source stop state')
        if type(saved['schema_version']) is not int or saved['schema_version'] != self.SCHEMA_VERSION:
            raise ValueError('unsupported TM source stop state')
        reason = saved['reason']
        permit = saved['probe_permit_id']
        if type(saved['probe_sent']) is not bool:
            raise ValueError('invalid TM source probe evidence')
        if reason is not None and reason not in SOURCE_STOP_REASONS:
            raise ValueError('invalid TM source stop reason')
        if type(saved['needs_owner']) is not bool or type(saved['failed_probes']) is not int \
                or not 0 <= saved['failed_probes'] <= self.MAX_FAILED_PROBES:
            raise ValueError('invalid TM source stop evidence')
        if permit is not None and (not isinstance(permit, str) or len(permit) != 64
                                   or any(c not in '0123456789abcdef' for c in permit)):
            raise ValueError('invalid TM source probe permit')
        self.reason, self.probe_permit_id = reason, permit
        self.needs_owner = saved['needs_owner']
        self.failed_probes = saved['failed_probes']
        self.stopped_at = StreamRateState._time(saved['stopped_at'])
        self.until = StreamRateState._time(saved['until'])
        self.probe_started_at = StreamRateState._time(saved['probe_started_at'])
        self.probe_sent = saved['probe_sent']
        if (self.active and (self.stopped_at == 0 or self.until < self.stopped_at)) \
                or (not self.active and (self.failed_probes or self.needs_owner or permit)) \
                or (self.probe_sent and permit is None):
            raise ValueError('inconsistent TM source stop state')

    def dump(self):
        return dict(schema_version=self.SCHEMA_VERSION, reason=self.reason, stopped_at=self.stopped_at,
                    until=self.until, failed_probes=self.failed_probes, probe_permit_id=self.probe_permit_id,
                    probe_started_at=self.probe_started_at, probe_sent=self.probe_sent,
                    needs_owner=self.needs_owner)


MOSCOW_OFFSET_SECONDS = 3 * 60 * 60


def calendar_day(now):
    """The Moscow calendar day of an epoch time; daily reports count by it."""
    moment = StreamRateState._time(now) + MOSCOW_OFFSET_SECONDS
    return time.strftime('%Y-%m-%d', time.gmtime(moment))


class RefusalCounters:
    """Source requests and the source's refusals of them, per Moscow calendar day.

    Both pass through the gateway: a request is a consumed permit, a refusal is
    the site's answer the client reports back.  Days are kept for five weeks.
    """

    SCHEMA_VERSION = 1
    KEPT_DAYS = 35
    _DAY = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    _FIELDS = {'schema_version', 'days'}

    def __init__(self, saved=None):
        self.days = {}
        if saved is not None:
            self._restore(saved)

    def _restore(self, saved):
        if not isinstance(saved, dict) or set(saved) != self._FIELDS:
            raise ValueError('invalid TM refusal counters')
        if type(saved['schema_version']) is not int or saved['schema_version'] != self.SCHEMA_VERSION:
            raise ValueError('unsupported TM refusal counters')
        if not isinstance(saved['days'], dict):
            raise ValueError('invalid TM refusal counters')
        for day, entry in saved['days'].items():
            if not isinstance(day, str) or not self._DAY.fullmatch(day):
                raise ValueError('invalid TM refusal day')
            try:
                date.fromisoformat(day)
            except ValueError:
                raise ValueError('invalid TM refusal day') from None
            if (not isinstance(entry, dict) or set(entry) != {'requests', 'refusals'}
                    or any(type(entry[name]) is not int or entry[name] < 0 for name in entry)
                    or entry['refusals'] > entry['requests']):
                raise ValueError('invalid TM refusal count')
            self.days[day] = dict(entry)
        self._prune()

    def _prune(self):
        if self.days:
            oldest = date.fromisoformat(max(self.days)) - timedelta(days=self.KEPT_DAYS - 1)
            for day in [day for day in self.days if date.fromisoformat(day) < oldest]:
                del self.days[day]

    def _bump(self, now, name):
        entry = self.days.setdefault(calendar_day(now), {'requests': 0, 'refusals': 0})
        entry[name] += 1
        self._prune()

    def count_request(self, now):
        self._bump(now, 'requests')

    def count_refusal(self, now):
        self._bump(now, 'refusals')

    def rate(self, day):
        """Refusals per 100 requests of one day; None is a day without requests."""
        entry = self.days.get(day)
        if not entry or not entry['requests']:
            return None
        return dict(requests=entry['requests'], refusals=entry['refusals'],
                    per_100=round(100.0 * entry['refusals'] / entry['requests'], 2))

    def dump(self):
        return dict(schema_version=self.SCHEMA_VERSION,
                    days={day: dict(entry) for day, entry in sorted(self.days.items())})


def refusal_rate(path, day):
    """Read one day's refusal rate from the gateway's counters; None when they are absent."""
    try:
        saved = json.loads(Path(path).read_bytes().decode('utf-8'))
    except (OSError, ValueError):
        return None
    try:
        return RefusalCounters(saved).rate(day)
    except ValueError:
        return None
