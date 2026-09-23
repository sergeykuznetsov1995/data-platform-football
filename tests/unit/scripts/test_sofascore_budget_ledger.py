"""#1349: stale zero-byte reservations are reaped, paid ones never are."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.proxy_filter.budget import (
    RESERVATION_TTL_SECONDS,
    ProxyBudgetExceeded,
    SharedBudgetLedger,
)

pytestmark = pytest.mark.unit

RUN = "scheduled__2026-09-22--T-S::season::alloc-1"


def _ledger(tmp_path, *, limit=1000, measured_max=1000):
    policy = SimpleNamespace(
        artifact_id="a" * 64,
        hard_run_bytes=limit,
        reservation_for=lambda endpoint: measured_max,
    )
    return SharedBudgetLedger(tmp_path / "proxy-budget-ledger.json", policy)


def _age(ledger, **created_ago_seconds):
    """Backdate the named reservations (by endpoint) in the ledger file."""
    payload = json.loads(ledger.path.read_text())
    now = datetime.now(timezone.utc)
    for item in payload["runs"][RUN]["reservations"].values():
        seconds = created_ago_seconds.get(item["endpoint"])
        if seconds is None:
            continue
        if seconds == "missing":
            item.pop("created_at", None)
        else:
            item["created_at"] = (now - timedelta(seconds=seconds)).isoformat()
    ledger.path.write_text(json.dumps(payload))


def test_leaked_zero_byte_reservation_blocks_the_run_until_reaped(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.reserve(RUN, "season_events")  # the leaked attempt's reservation
    with pytest.raises(ProxyBudgetExceeded, match="budget exhausted before endpoint"):
        ledger.reserve(RUN, "season_events")

    _age(ledger, season_events=RESERVATION_TTL_SECONDS + 1)
    token, amount = ledger.reserve(RUN, "season_events")

    assert amount == 1000
    snapshot = ledger.snapshot(RUN)
    assert len(snapshot["reservations"]) == 1
    [reaped] = snapshot["reaped"]
    assert reaped["endpoint"] == "season_events"
    assert reaped["reserved_bytes"] == 1000
    assert reaped["created_at"]
    assert reaped["reaped_at"]
    assert snapshot["spent_provider_bytes"] == 0
    assert ledger.finish(RUN, token, reported_provider_bytes=0) == 0


def test_paid_and_fresh_reservations_are_never_reaped(tmp_path):
    ledger = _ledger(tmp_path, limit=1000, measured_max=300)
    paid, _ = ledger.reserve(RUN, "paid")
    ledger.consume(RUN, paid, 10)
    ledger.reserve(RUN, "fresh")
    _age(ledger, paid=RESERVATION_TTL_SECONDS * 4, fresh=100)

    ledger.reserve(RUN, "next")

    snapshot = ledger.snapshot(RUN)
    assert snapshot["reaped"] == []
    assert sorted(item["endpoint"] for item in snapshot["reservations"].values()) == [
        "fresh",
        "next",
        "paid",
    ]
    assert snapshot["spent_provider_bytes"] == 10


def test_reservation_without_created_at_counts_as_expired(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.reserve(RUN, "legacy")
    _age(ledger, legacy="missing")

    ledger.reserve(RUN, "next")

    snapshot = ledger.snapshot(RUN)
    assert [item["endpoint"] for item in snapshot["reaped"]] == ["legacy"]
    assert snapshot["reaped"][0]["created_at"] is None
