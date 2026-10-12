"""Transfermarkt stream capacity and restart-safe no-burst rate state.

Only the dedicated TM gateway enables these streams. A slot is permanent:
replacing its HTTP client or lease never changes the rate identity.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import os
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
        if sum(key == stream_id for key, _ in self.blocks) >= 3:
            self.pause[stream_id] = max(self.pause[stream_id], now + 900)
            alerts.append('transfermarkt_stream_paused')
        return tuple(alerts)
