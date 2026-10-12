"""Offline checks for durable daily transfer accounting and freshness."""

from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from scrapers.fotmob.transfers import (
    TRANSFER_MAX_DIRECT_MIB,
    TRANSFER_MAX_REQUESTS,
    TransferState,
    read_transfer_status,
)

NOW = datetime(2026, 10, 10, 0, 30, tzinfo=timezone.utc)


def test_missing_status_is_red_and_does_not_create_state(tmp_path):
    path = tmp_path / "absent" / "transfers.sqlite3"
    status = read_transfer_status(NOW, path=path)
    assert not status["daily_complete"]
    assert not status["budget_exhausted"]
    assert status["family_summary"]["status"] == "red"
    assert not path.parent.exists()


def test_each_competition_controls_freshness_and_exact_48h_boundary(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    state.record_catalog([47, 48], NOW, complete=True)
    state.record_completion(47, NOW)
    state.record_completion(48, NOW - timedelta(hours=48))
    assert read_transfer_status(NOW, path=path)["family_summary"]["status"] == "green"
    status = read_transfer_status(NOW + timedelta(seconds=1), path=path)
    assert status["family_summary"]["fresh_count"] == 1
    assert status["family_summary"]["stale_count"] == 1
    assert status["family_summary"]["status"] == "red"
    assert not status["daily_complete"]


def test_catalogue_change_and_stale_catalogue_cannot_be_green(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    state.record_catalog([47], NOW, complete=True)
    state.record_completion(47, NOW)
    assert read_transfer_status(NOW, path=path)["daily_complete"]
    state.record_catalog([47, 48], NOW, complete=True)
    assert read_transfer_status(NOW, path=path)["family_summary"]["unknown_count"] == 1
    state.record_completion(48, NOW)
    state.record_catalog([47, 48], NOW - timedelta(hours=49), complete=True)
    assert read_transfer_status(NOW, path=path)["family_summary"]["status"] == "red"
    state.record_catalog([47, 48], NOW, complete=False)
    assert not read_transfer_status(NOW, path=path)["daily_complete"]


def test_budget_retry_uses_only_remaining_daily_capacity(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    first = TransferState(path)
    lease = first.reserve(NOW)
    assert lease["requests"] == TRANSFER_MAX_REQUESTS
    assert TransferState(path).reserve(NOW) is None
    first.finalize(lease, requests=123, direct_bytes=2048)
    retry = TransferState(path)
    next_lease = retry.reserve(NOW + timedelta(minutes=30))
    assert next_lease["requests"] == TRANSFER_MAX_REQUESTS - 123
    assert next_lease["direct_bytes"] == TRANSFER_MAX_DIRECT_MIB * 1024 * 1024 - 2048
    retry.finalize(next_lease, requests=10, direct_bytes=512)
    budget = read_transfer_status(NOW, path=path)["daily_budget"]
    assert budget["requests"] == 133
    assert budget["direct_bytes"] == 2560
    assert not budget["reserved"]


def test_crash_reservation_is_fail_closed_until_next_day(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    state.reserve(NOW)
    restarted = TransferState(path)
    assert restarted.reserve(NOW) is None
    assert read_transfer_status(NOW, path=path)["budget_exhausted"]
    assert (
        restarted.reserve(NOW + timedelta(days=1))["requests"] == TRANSFER_MAX_REQUESTS
    )


def test_byte_overrun_is_visible_and_never_refunded(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    lease = state.reserve(NOW)
    state.finalize(lease, requests=10, direct_bytes=lease["direct_bytes"] + 3)
    status = read_transfer_status(NOW, path=path)
    assert status["daily_budget"]["direct_bytes"] == lease["direct_bytes"] + 3
    assert status["budget_exhausted"]
    assert state.reserve(NOW) is None


def test_corrupt_state_is_not_reset(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    path.write_bytes(b"not sqlite")
    with pytest.raises(sqlite3.DatabaseError):
        read_transfer_status(NOW, path=path)
    with pytest.raises(sqlite3.DatabaseError):
        TransferState(path)
    assert path.read_bytes() == b"not sqlite"


def test_less_than_retry_bound_remaining_means_exhausted(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    lease = state.reserve(NOW)
    state.finalize(lease, requests=TRANSFER_MAX_REQUESTS - 3, direct_bytes=0)
    assert read_transfer_status(NOW, path=path)["budget_exhausted"]
    assert state.reserve(NOW) is None
