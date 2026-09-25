"""ESPN VM gate: origins, one permit per request, lanes and auto-reset (#1500)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scrapers.espn.gate import TransportGate, load_transport_policy
from scrapers.espn.transport_contracts import (
    AllOriginsBlocked,
    DailyCapExceeded,
    ESPN_CORE_API_ORIGIN,
    ESPN_CORE_CLUSTER,
    ESPN_SITE_API_ORIGIN,
    ESPN_SITE_CLUSTER,
    ESPN_SITE_WEB_API_ORIGIN,
    LaneClosed,
)

WEB = ESPN_SITE_WEB_API_ORIGIN
SITE = ESPN_SITE_API_ORIGIN
START = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


class Clock:
    def __init__(self, start=START):
        self.now = start
        self.sleeps = []

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


def _gate(tmp_path, clock, lane="live", ceiling=0, policy=None):
    return TransportGate(
        policy or load_transport_policy(),
        tmp_path / "gate.json",
        lane,
        step_ceiling=ceiling,
        utcnow_fn=clock,
        sleep_fn=clock.sleep,
    )


@pytest.mark.unit
def test_403_on_primary_moves_next_requests_to_reserve_one_permit_each(tmp_path):
    """Criterion 1: a 403 is never retried; next requests use the reserve."""
    clock = Clock()
    gate = _gate(tmp_path, clock)
    # The reserve starts closed; a day later its probe is due and goes first.
    assert gate.choose_origin("site") == WEB
    clock.advance(86400)
    probe = gate.acquire("site")
    assert (probe.origin, probe.probe) == (SITE, True)
    gate.report(probe, status=200)  # the reserve is open again

    first = gate.acquire("site")
    assert first.origin == WEB and not first.probe
    outcome = gate.report(first, status=403)
    assert outcome.origin_closed and not outcome.all_blocked

    before = gate.snapshot()
    permits = [gate.acquire("site") for _ in range(3)]
    for permit in permits:
        gate.report(permit, status=200)
    assert [p.origin for p in permits] == [SITE] * 3
    assert all(not p.probe for p in permits)
    after = gate.snapshot()
    assert after["daily"]["live"]["requests"] - before["daily"]["live"]["requests"] == 3
    # Primary stays closed for 30 minutes, then exactly one probe.
    assert gate.choose_origin("site") == SITE
    clock.advance(1800)
    probe = gate.acquire("site")
    assert (probe.origin, probe.probe) == (WEB, True)
    assert gate.acquire("site").origin == SITE  # nobody else probes meanwhile
    gate.report(probe, status=200)
    assert gate.acquire("site").origin == WEB


@pytest.mark.unit
def test_closed_reserve_makes_primary_403_all_blocked_and_freezes_history(tmp_path):
    clock = Clock()
    live = _gate(tmp_path, clock)
    history = _gate(tmp_path, clock, lane="history")
    assert history.acquire("site").origin == WEB

    outcome = live.report(live.acquire("site"), status=403)
    assert outcome.all_blocked
    with pytest.raises(AllOriginsBlocked):
        live.acquire("site")
    with pytest.raises(LaneClosed):
        history.acquire("site")

    clock.advance(1799)
    with pytest.raises(AllOriginsBlocked):
        live.acquire("site")
    clock.advance(1)
    probe = live.acquire("site")  # pause over: one probe
    assert (probe.origin, probe.probe) == (WEB, True)
    live.report(probe, status=403)
    with pytest.raises(AllOriginsBlocked):
        live.acquire("site")
    clock.advance(300)  # next probe after 5 minutes
    probe = live.acquire("site")
    assert (probe.origin, probe.probe) == (WEB, True)
    live.report(probe, status=200)

    assert live.acquire("site").origin == WEB  # live first
    with pytest.raises(LaneClosed):  # history reopens last
        history.acquire("site")
    clock.advance(900)
    assert history.acquire("site").origin == WEB


@pytest.mark.unit
def test_429_drops_a_step_freezes_history_and_recovers_after_cooldown(tmp_path):
    """Criterion 2: auto-reset and step recovery."""
    clock = Clock()
    live = _gate(tmp_path, clock, ceiling=2)
    history = _gate(tmp_path, clock, lane="history", ceiling=2)
    assert live.acquire("site").step == 2

    outcome = live.report(live.acquire("site"), status=429)
    assert outcome.reset
    snapshot = live.snapshot()
    assert snapshot["step"] == 1 and snapshot["rate_per_minute"] == 120
    with pytest.raises(LaneClosed):
        history.acquire("site")
    permit = live.acquire("site")
    assert permit.step == 1
    # Slower pace: permits now 0.5 s apart.
    nxt = live.acquire("site")
    assert nxt.granted_at - permit.granted_at == pytest.approx(0.5)

    clock.advance(900)
    assert live.acquire("site").step == 2
    assert live.snapshot()["step"] == 2
    history.acquire("site")


@pytest.mark.unit
def test_three_403_in_a_minute_and_error_share_also_reset(tmp_path):
    clock2 = Clock()
    fast = _gate(tmp_path / "fast", clock2, ceiling=3)
    fast_permits = [fast.acquire("site") for _ in range(3)]
    results = [fast.report(p, status=403) for p in fast_permits]
    assert [r.reset for r in results] == [False, False, True]
    assert fast.snapshot()["step"] == 2

    clock3 = Clock()
    errors = _gate(tmp_path / "errors", clock3, ceiling=3)
    results = []
    for index in range(50):
        permit = errors.acquire("core")
        status = 503 if index in (10, 20) else 200
        results.append(errors.report(permit, status=status))
    # 2/50 = 4 % > 2 % with ≥ 50 requests in the window.
    assert results[-1].reset and not any(r.reset for r in results[:-1])
    assert errors.snapshot()["step"] == 2

    clock4 = Clock()
    quiet = _gate(tmp_path / "quiet", clock4, ceiling=3)
    for index in range(49):
        permit = quiet.acquire("core")
        assert not quiet.report(permit, timeout=index < 5).reset  # < 50 requests
    assert quiet.snapshot()["step"] == 3


@pytest.mark.unit
def test_two_resets_within_an_hour_hold_s0_for_six_hours_with_alert(tmp_path):
    clock = Clock()
    gate = _gate(tmp_path, clock, ceiling=3)
    gate.report(gate.acquire("site"), status=429)
    clock.advance(1000)  # cooldown over, back to S3
    assert gate.acquire("site").step == 3
    gate.report(gate.acquire("site"), status=429)
    snapshot = gate.snapshot()
    assert snapshot["step"] == 0
    assert snapshot["alerts"][-1]["kind"] == "double_reset"
    clock.advance(6 * 3600 - 1)
    assert gate.acquire("site").step == 0
    clock.advance(2)
    assert gate.acquire("site").step == 3


@pytest.mark.unit
def test_history_gets_at_most_half_of_the_minute_live_always_when_tokens(tmp_path):
    clock = Clock()
    history = _gate(tmp_path, clock, lane="history")
    live = _gate(tmp_path, clock, lane="live")
    granted = [history.acquire("site").granted_at for _ in range(31)]
    start = granted[0]
    # 30 history permits in the first minute, the 31st waits for the window.
    assert sum(1 for t in granted if t < start + 60) == 30
    assert granted[30] >= start + 60
    # Live is never refused while tokens exist: permits one second apart.
    first = live.acquire("site")
    second = live.acquire("site")
    assert second.granted_at - first.granted_at == pytest.approx(1.0)


@pytest.mark.unit
def test_daily_lane_cap_and_utc_date_rollover(tmp_path):
    raw = load_transport_policy()
    lanes = {
        "live": {"daily_requests": 2, "daily_bytes": 10**9},
        "history": {"daily_requests": 5, "daily_bytes": 100},
    }
    policy = type(raw)(**{**raw.__dict__, "lanes": lanes})
    clock = Clock(datetime(2026, 9, 25, 23, 59, 0, tzinfo=timezone.utc))
    live = _gate(tmp_path, clock, policy=policy)
    live.acquire("site")
    live.acquire("site")
    with pytest.raises(DailyCapExceeded):
        live.acquire("site")
    history = _gate(tmp_path, clock, lane="history", policy=policy)
    history.report(history.acquire("site"), status=200, direct_bytes=100)
    with pytest.raises(DailyCapExceeded):  # byte cap of the lane
        history.acquire("site")
    clock.advance(120)  # new UTC date
    assert live.acquire("site").origin == WEB
    assert history.acquire("site").origin == WEB


@pytest.mark.unit
def test_two_gates_on_one_file_share_pace_and_blocks(tmp_path):
    clock = Clock()
    one = _gate(tmp_path, clock)
    two = _gate(tmp_path, clock)
    a = one.acquire("site")
    b = two.acquire("site")
    c = one.acquire("site")
    assert [b.granted_at - a.granted_at, c.granted_at - b.granted_at] == [
        pytest.approx(1.0),
        pytest.approx(1.0),
    ]
    one.report(c, status=403)
    with pytest.raises(AllOriginsBlocked):
        two.acquire("site")


@pytest.mark.unit
def test_core_cluster_has_no_reserve_and_403_blocks_it_at_once(tmp_path):
    clock = Clock()
    gate = _gate(tmp_path, clock)
    permit = gate.acquire("core")
    assert permit.origin == ESPN_CORE_API_ORIGIN
    assert gate.report(permit, status=403).all_blocked
    with pytest.raises(AllOriginsBlocked):
        gate.acquire("core")
    assert gate.acquire("site").origin == WEB  # other cluster unaffected


@pytest.mark.unit
def test_origin_constants_keep_discovery_on_core_and_web_api():
    policy = load_transport_policy()
    assert ESPN_SITE_CLUSTER == ("site", WEB, SITE)
    assert ESPN_CORE_CLUSTER == ("core", ESPN_CORE_API_ORIGIN, None)
    assert policy.clusters == {
        name: (primary, reserve)
        for name, primary, reserve in (ESPN_SITE_CLUSTER, ESPN_CORE_CLUSTER)
    }
    assert policy.cluster_of("https://site.api.espn.com") == "site"
    with pytest.raises(ValueError):
        policy.cluster_of("https://www.espn.com")


@pytest.mark.unit
def test_reserve_is_probed_daily_even_while_primary_is_healthy(tmp_path):
    clock = Clock()
    gate = _gate(tmp_path, clock)
    for _ in range(3):
        permit = gate.acquire("site")
        assert permit.origin == WEB
        gate.report(permit, status=200)
    clock.advance(86400)
    probe = gate.acquire("site")
    assert (probe.origin, probe.probe) == (SITE, True)
    assert gate.acquire("site").origin == WEB  # one probe only
    outcome = gate.report(probe, status=403)
    assert outcome.origin_closed and not outcome.all_blocked
    assert gate.acquire("site").origin == WEB
    clock.advance(86000)  # the next reserve probe is a day after the 403
    assert gate.acquire("site").origin == WEB
    clock.advance(400)
    assert gate.acquire("site").probe


@pytest.mark.unit
def test_late_answer_of_a_request_admitted_before_the_block_does_not_reopen(tmp_path):
    clock = Clock()
    gate = _gate(tmp_path, clock)
    early = gate.acquire("site")
    blocked = gate.acquire("site")
    assert gate.report(blocked, status=403).all_blocked
    gate.report(early, status=200)  # in flight before the 403
    with pytest.raises(AllOriginsBlocked):
        gate.acquire("site")
    assert gate.snapshot()["all_blocked"]
    gate.report(early, status=403)  # a late 403 never shortens the pause
    clock.advance(1799)
    with pytest.raises(AllOriginsBlocked):
        gate.acquire("site")


@pytest.mark.unit
def test_a_waiting_request_rechecks_blocks_and_resets_after_its_wait(tmp_path):
    clock = Clock()
    other = _gate(tmp_path, clock, ceiling=1)
    in_flight = other.acquire("site")

    def block_during_wait(seconds):
        clock.sleep(seconds)
        other.report(in_flight, status=403)

    waiting = TransportGate(
        load_transport_policy(),
        tmp_path / "gate.json",
        "live",
        step_ceiling=1,
        utcnow_fn=clock,
        sleep_fn=block_during_wait,
    )
    with pytest.raises(AllOriginsBlocked):  # not sent to the closed origin
        waiting.acquire("site")

    clock2 = Clock()
    live = _gate(tmp_path / "reset", clock2, ceiling=1)
    first = live.acquire("site")

    def reset_during_wait(seconds):
        clock2.sleep(seconds)
        live.report(first, status=429)

    history = TransportGate(
        load_transport_policy(),
        tmp_path / "reset" / "gate.json",
        "history",
        step_ceiling=1,
        utcnow_fn=clock2,
        sleep_fn=reset_during_wait,
    )
    with pytest.raises(LaneClosed):  # history froze while it waited
        history.acquire("site")
