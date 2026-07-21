from datetime import datetime, timedelta, timezone

import pytest

from dags.utils import transfermarkt_scheduler as scheduler


NOW = datetime(2026, 7, 21, 12, tzinfo=timezone.utc)


def _candidate(scope_id, queue, *, estimate=2_000_000, age=1, pending=0):
    return scheduler.CrawlCandidate(
        scope_id=scope_id,
        queue=queue,
        due_at=NOW - timedelta(days=age),
        pending_work=pending,
        estimated_provider_bytes=estimate,
    )


def test_wave_is_fair_and_rotates_start_queue():
    candidates = [
        _candidate(f'{queue}-{index}', queue, pending=index)
        for queue in scheduler.QUEUE_ORDER
        for index in range(3)
    ]

    first = scheduler.plan_wave(
        candidates, now=NOW, used_provider_bytes=0, max_wave_size=4,
    )
    assert [item.candidate.queue for item in first.scopes] == list(
        scheduler.QUEUE_ORDER
    )

    second = scheduler.plan_wave(
        candidates,
        now=NOW,
        used_provider_bytes=0,
        queue_cursor=first.next_queue_cursor + 1,
        max_wave_size=4,
    )
    assert second.scopes[0].candidate.queue == 'current'


def test_budget_admission_uses_guarded_estimate_and_never_exceeds_cap():
    cheap = _candidate('cheap', 'growth', estimate=1)
    expensive = _candidate(
        'expensive', 'debt', estimate=scheduler.SCOPE_HARD_PROVIDER_BYTE_CAP,
    )
    cap = scheduler.MIN_SCOPE_RESERVATION_BYTES + 1

    plan = scheduler.plan_wave(
        [cheap, expensive],
        now=NOW,
        used_provider_bytes=0,
        hard_provider_byte_cap=cap,
        soft_provider_byte_stop=cap - 1,
    )

    assert [item.candidate.scope_id for item in plan.scopes] == ['cheap']
    assert plan.reserved_provider_bytes == scheduler.MIN_SCOPE_RESERVATION_BYTES
    assert plan.used_provider_bytes + plan.reserved_provider_bytes <= cap


def test_unknown_scope_cost_reserves_full_scope_cap():
    candidate = scheduler.CrawlCandidate(
        scope_id='cold', queue='growth', due_at=NOW,
    )
    assert (
        scheduler.scope_reservation_bytes(candidate)
        == scheduler.SCOPE_HARD_PROVIDER_BYTE_CAP
    )


def test_soft_stop_returns_empty_wave_and_day_is_utc():
    local = datetime(2026, 7, 22, 1, tzinfo=timezone(timedelta(hours=3)))
    plan = scheduler.plan_wave(
        [_candidate('a', 'growth')],
        now=local,
        used_provider_bytes=scheduler.PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP,
    )
    assert not plan.scopes
    assert plan.day_key == '2026-07-21'


def test_duplicate_or_naive_candidates_fail_closed():
    candidate = _candidate('same', 'growth')
    with pytest.raises(scheduler.SchedulingError, match='duplicate'):
        scheduler.plan_wave(
            [candidate, candidate], now=NOW, used_provider_bytes=0,
        )
    with pytest.raises(scheduler.SchedulingError, match='timezone-aware'):
        scheduler.CrawlCandidate(
            scope_id='naive', queue='growth', due_at=datetime(2026, 1, 1),
        )
