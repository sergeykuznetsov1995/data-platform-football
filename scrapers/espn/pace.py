"""Fail-closed pace eligibility. All windows use aware UTC, HTTP-only milliseconds."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


@dataclass(frozen=True)
class PacePolicy:
    minimum_window_seconds: tuple[int, ...] = (7200, 86400, 86400)
    s3_max_seconds: int = 21600
    error_share: float = 0.005
    http_p95_limit_ms: float = 1500
    baseline_ratio: float = 1.5
    load_interval_seconds: int = 300
    history_quota_fraction: float = 0.8
    loaded_interval_fraction: float = 0.9
    decision_max_age_seconds: int = 300


def parse_pace_policy(raw: Mapping) -> PacePolicy:
    if not isinstance(raw, dict) or set(raw) != set(PacePolicy.__dataclass_fields__):
        raise ValueError("transport policy: pace keys mismatch")
    values = dict(raw)
    windows = values['minimum_window_seconds']
    if not isinstance(windows, list) or len(windows) != 3 or any(type(v) is not int or v <= 0 for v in windows):
        raise ValueError("transport policy: invalid pace windows")
    values['minimum_window_seconds'] = tuple(windows)
    for key, value in values.items():
        if key == 'minimum_window_seconds':
            continue
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"transport policy: invalid pace.{key}")
        if key in ('error_share', 'history_quota_fraction', 'loaded_interval_fraction') and value > 1:
            raise ValueError(f"transport policy: invalid pace.{key}")
        if key.endswith('_seconds') and type(value) is not int:
            raise ValueError(f"transport policy: invalid pace.{key}")
    return PacePolicy(**values)


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('pace timestamps must be timezone-aware')
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class PaceWindow:
    step: int
    started_at: datetime
    ended_at: datetime
    attempts: int | None
    count_429: int | None
    error_attempts: int | None
    http_p95_ms: float | None
    reset_count: int | None
    freshness_ok: bool | None
    coverage_ok: bool | None
    # coverage_ok attests complete metrics AND load evidence. Supply counts to
    # compute the latter explicitly; omitted counts require that attestation.
    eligible_intervals: int | None = None
    loaded_intervals: int | None = None
    hold_active: bool = False
    cooldown_active: bool = False


@dataclass(frozen=True)
class Decision:
    eligible: bool
    reason: str
    next_step: int | None


def evaluate(window: PaceWindow, baseline_p95_ms: float | None, policy) -> Decision:
    policy = getattr(policy, 'pace', policy)
    def no(reason):
        return Decision(False, reason, None)
    if type(window.step) is not int or window.step not in (0, 1, 2):
        return no('no_higher_step')
    start, end = utc(window.started_at), utc(window.ended_at)
    if (end - start).total_seconds() < policy.minimum_window_seconds[window.step]:
        return no('window_too_short')
    counts = (window.attempts, window.count_429, window.error_attempts, window.reset_count)
    if any(type(v) is not int or v < 0 for v in counts):
        return no('unknown_metrics')
    if window.attempts == 0:
        return no('empty_window')
    if window.error_attempts > window.attempts or window.count_429 > window.attempts:
        return no('invalid_counts')
    if window.coverage_ok is not True:
        return no('incomplete_coverage')
    if window.eligible_intervals is not None or window.loaded_intervals is not None:
        n, loaded = window.eligible_intervals, window.loaded_intervals
        if type(n) is not int or type(loaded) is not int or n <= 0 or not 0 <= loaded <= n:
            return no('insufficient_load')
        if loaded / n < policy.loaded_interval_fraction:
            return no('insufficient_load')
    if window.freshness_ok is not True:
        return no('freshness_unknown_or_low')
    if window.hold_active or window.cooldown_active:
        return no('protection_active')
    if window.reset_count:
        return no('reset_in_window')
    if window.count_429:
        return no('http429')
    if window.error_attempts / window.attempts >= policy.error_share:
        return no('error_share')
    p95 = window.http_p95_ms
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in (p95, baseline_p95_ms)):
        return no('unknown_latency')
    if p95 >= policy.http_p95_limit_ms:
        return no('http_p95_limit')
    if p95 > baseline_p95_ms * policy.baseline_ratio:
        return no('baseline_regression')
    return Decision(True, 'eligible', window.step + 1)
