"""The watchdog must free a latched gateway slot — and only a latched one."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "deploy/sofascore/gateway_lease_watchdog.py"
MODULE_NAME = "sofascore_gateway_lease_watchdog"


def _load_module():
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # ``dataclass`` resolves annotations through ``sys.modules[cls.__module__]``,
    # so the module has to be registered before its body runs.
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


watchdog = _load_module()


NOW = 1_700_000_000.0


def _lease(**overrides):
    """A live, healthy SofaScore lease as ``Lease.report()`` emits it."""

    lease = {
        "id": "lease-live",
        "source": "sofascore",
        "created_at": NOW - 300.0,
        "expires_at": NOW + 3300.0,
        "closed": False,
        "expired": False,
        "accounting_uncertain": False,
        "reserved_bytes": 0,
        "global_budget_escrow_bytes": 0,
        "active_tunnels": 1,
        "dag_id": "dag_ingest_sofascore",
        "run_id": "hist2024",
        "task_id": "scrape_match_capture_ita_serie_a",
    }
    lease.update(overrides)
    return lease


def _snapshot(leases, *, open_ids=None, last_event=None, **overrides):
    ids = frozenset(
        open_ids
        if open_ids is not None
        else [str(item.get("id")) for item in leases]
    )
    kwargs = {
        "bytes_payload": {"leases": list(leases)},
        "bytes_age_seconds": 1.0,
        "allocations": None,
        "wal_open_lease_ids": ids,
        "wal_last_event_at": dict(last_event or {}),
        "wal_size_bytes": 6 * 1024 * 1024,
        "now": NOW,
    }
    kwargs.update(overrides)
    return watchdog.Snapshot(**kwargs)


# --- the slot is free or legitimately busy: never touch the gateway ---------


def test_idle_gateway_is_left_alone():
    verdict = watchdog.evaluate(_snapshot([]))
    assert verdict.action == watchdog.NONE


def test_a_live_capture_holding_the_slot_is_never_restarted():
    # The single most expensive mistake this watchdog could make: killing a
    # running scrape because it happens to own the only lease.
    verdict = watchdog.evaluate(
        _snapshot([_lease()], last_event={"lease-live": NOW - 4.0})
    )
    assert verdict.action == watchdog.NONE


def test_a_lease_silent_but_mid_read_is_never_restarted():
    # An open tunnel means someone is blocked on a socket, however long the
    # WAL has been quiet — a slow provider read is not a ghost.
    verdict = watchdog.evaluate(
        _snapshot(
            [_lease(active_tunnels=2)],
            last_event={"lease-live": NOW - 4 * 3600.0},
        )
    )
    assert verdict.action == watchdog.NONE


def test_a_freshly_latched_lease_waits_out_the_grace_window():
    verdict = watchdog.evaluate(
        _snapshot(
            [
                _lease(
                    id="lease-new",
                    created_at=NOW - 5.0,
                    accounting_uncertain=True,
                    reserved_bytes=4096,
                    active_tunnels=0,
                )
            ]
        )
    )
    assert verdict.action == watchdog.NONE


def test_a_backfill_lease_never_counts_as_holding_the_slot():
    # It draws on a different provider pool; the gateway does not count it
    # against MAX_ACTIVE_LEASES, so neither may we.
    verdict = watchdog.evaluate(
        _snapshot(
            [
                _lease(
                    id="lease-backfill",
                    source="transfermarkt_backfill",
                    closed=True,
                    reserved_bytes=9999,
                    active_tunnels=0,
                )
            ]
        )
    )
    assert verdict.action == watchdog.NONE


# --- every face of the mine must be recognised -----------------------------


@pytest.mark.parametrize(
    "overrides, expected_fragment",
    [
        pytest.param(
            {"accounting_uncertain": True, "reserved_bytes": 4096, "active_tunnels": 0},
            "accounting-uncertain",
            id="latched-uncertain",
        ),
        pytest.param(
            {"closed": True, "reserved_bytes": 4096, "active_tunnels": 0},
            "closed",
            id="closed-with-retained-reservation",
        ),
        pytest.param(
            {
                "expired": True,
                "global_budget_escrow_bytes": 8192,
                "active_tunnels": 0,
            },
            "expired",
            id="expired-with-retained-escrow",
        ),
        pytest.param(
            {"expires_at": NOW - 120.0, "reserved_bytes": 1, "active_tunnels": 0},
            "past its TTL",
            id="past-ttl-but-dump-not-caught-up",
        ),
    ],
)
def test_each_latched_face_is_restarted(overrides, expected_fragment):
    lease = _lease(id="lease-stuck", **overrides)
    verdict = watchdog.evaluate(
        _snapshot([lease], last_event={"lease-stuck": NOW - 30.0})
    )
    assert verdict.action == watchdog.RESTART
    assert verdict.lease_id == "lease-stuck"
    assert expected_fragment in verdict.reason


def test_the_2026_07_28_history_wave_ghost_would_have_been_freed():
    """Regression built from the incident that killed the #1000 history wave.

    Lease ``9731de76…`` made its last gateway request at 11:52:50 UTC and was
    released only by the 12:56:36 restart.  It was neither closed nor expired
    nor accounting-uncertain — from the dump it looked exactly like a healthy
    open lease — so the earlier ``accounting_uncertain``-only predicate would
    have returned ``none`` while five leagues burned three attempts each on
    HTTP 429.
    """

    last_request = datetime(2026, 7, 28, 11, 52, 50, tzinfo=timezone.utc).timestamp()
    # 12:09 UTC — the moment the first league was refused the slot.
    now = datetime(2026, 7, 28, 12, 9, 46, tzinfo=timezone.utc).timestamp()
    ghost = {
        "id": "9731de76d3dac5b7d7e1adc0615e90f2",
        "source": "sofascore",
        "created_at": last_request - 600.0,
        "expires_at": last_request + 3000.0,
        "closed": False,
        "expired": False,
        "accounting_uncertain": False,
        "reserved_bytes": 0,
        "global_budget_escrow_bytes": 0,
        "active_tunnels": 0,
    }
    verdict = watchdog.evaluate(
        _snapshot(
            [ghost],
            last_event={ghost["id"]: last_request},
            now=now,
        )
    )
    assert verdict.action == watchdog.RESTART
    assert verdict.lease_id == ghost["id"]
    assert "no gateway request" in verdict.reason


def test_a_silence_shorter_than_the_threshold_is_not_yet_a_ghost():
    verdict = watchdog.evaluate(
        _snapshot(
            [_lease(id="lease-quiet", active_tunnels=0)],
            last_event={"lease-quiet": NOW - watchdog.SILENT_LEASE_SECONDS + 60.0},
        )
    )
    assert verdict.action == watchdog.NONE


def test_a_lease_with_no_recorded_activity_is_never_judged_by_silence():
    # Without a first event there is nothing to age it against; guessing here
    # would restart the gateway on a lease created moments ago.
    verdict = watchdog.evaluate(
        _snapshot([_lease(id="lease-unknown", active_tunnels=0)], last_event={})
    )
    assert verdict.action == watchdog.NONE


# --- refusing to act when a restart would do harm --------------------------


def test_a_stuck_lease_without_an_open_claim_freezes_instead_of_restarting():
    # The WAL replay is the only thing that releases the durable claim. If the
    # claim_intent is gone, restarting orphans the allocation permanently —
    # exactly how alloc-cd08d5ed was lost.
    verdict = watchdog.evaluate(
        _snapshot(
            [_lease(id="lease-stuck", closed=True, reserved_bytes=1, active_tunnels=0)],
            open_ids=[],
            last_event={"lease-stuck": NOW - 30.0},
        )
    )
    assert verdict.action == watchdog.FREEZE
    assert "claim_intent" in verdict.reason


def test_an_oversized_wal_freezes_because_the_replay_would_oom():
    verdict = watchdog.evaluate(
        _snapshot(
            [_lease(id="lease-stuck", closed=True, reserved_bytes=1, active_tunnels=0)],
            last_event={"lease-stuck": NOW - 30.0},
            wal_size_bytes=watchdog.WAL_OOM_RISK_BYTES + 1,
        )
    )
    assert verdict.action == watchdog.FREEZE
    assert "OOM" in verdict.reason


def test_a_stale_dump_alerts_rather_than_reading_it_as_health():
    verdict = watchdog.evaluate(
        _snapshot([], bytes_age_seconds=watchdog.STALE_SNAPSHOT_SECONDS + 1.0)
    )
    assert verdict.action == watchdog.ALERT
    assert "stale" in verdict.reason


def test_an_unreadable_dump_alerts():
    verdict = watchdog.evaluate(_snapshot([], bytes_payload=None))
    assert verdict.action == watchdog.ALERT


def _active_claim(age_seconds, **overrides):
    """``active_claim`` exactly as the gateway persists it.

    ``scrapers/sofascore/workload_plan.py:1633-1638`` writes these four keys and
    no others: there is no lease id to match against, and ``started_at`` is an
    ISO-8601 string rather than epoch seconds.  A fixture that invents either
    shape passes against a watchdog that cannot read the real journal.
    """

    claim = {
        "claim_token_hash": "c6a36352d7c44c58",
        "attempt_id_hash": "85499ee0669aa200",
        "start_spent_provider_bytes": 0,
        "started_at": datetime.fromtimestamp(
            NOW - age_seconds, tz=timezone.utc
        ).isoformat(),
    }
    claim.update(overrides)
    return claim


def _allocations(claim, allocation_id="alloc-cd08d5ed"):
    return {
        "runs": {"hist2024": {"allocations": {allocation_id: {"active_claim": claim}}}}
    }


def test_an_orphaned_durable_claim_alerts_because_a_restart_cannot_help_it():
    allocations = _allocations(_active_claim(4 * 3600.0))
    verdict = watchdog.evaluate(_snapshot([], allocations=allocations))
    assert verdict.action == watchdog.ALERT
    assert verdict.details["orphans"][0]["allocation_id"] == "alloc-cd08d5ed"


def test_a_young_active_claim_is_not_reported_as_orphaned():
    # The regression: read with a plain ``float()`` the ISO stamp yields no age
    # at all, so a claim opened a minute ago by a perfectly healthy run was
    # paged as "outlived its lease" — on every sample, forever.
    allocations = _allocations(_active_claim(60.0), allocation_id="alloc-fresh")
    verdict = watchdog.evaluate(_snapshot([], allocations=allocations))
    assert verdict.action == watchdog.NONE


def test_a_claim_whose_stamp_cannot_be_read_is_not_called_an_orphan():
    allocations = _allocations(_active_claim(60.0, started_at="not-a-timestamp"))
    verdict = watchdog.evaluate(_snapshot([], allocations=allocations))
    assert verdict.action == watchdog.NONE


def test_a_claim_naming_a_live_lease_is_skipped_whatever_its_age():
    # Forward compatibility only: today's gateway stamps no lease id, so this
    # branch never fires against the real journal.
    allocations = _allocations(
        _active_claim(4 * 3600.0, lease_id="lease-live"),
        allocation_id="alloc-attached",
    )
    verdict = watchdog.evaluate(_snapshot([_lease()], allocations=allocations))
    assert verdict.action == watchdog.NONE


# --- reading the WAL -------------------------------------------------------


def _wal_line(**fields):
    payload = {"event_version": watchdog.WAL_EVENT_VERSION}
    payload.update(fields)
    return json.dumps(payload)


def test_wal_state_reports_open_attempts_and_their_last_activity(tmp_path):
    started = datetime(2026, 7, 28, 11, 40, 0, tzinfo=timezone.utc)
    last = datetime(2026, 7, 28, 11, 52, 50, tzinfo=timezone.utc)
    wal = tmp_path / watchdog.WAL_FILE
    wal.write_text(
        "\n".join(
            [
                _wal_line(
                    event_type="claim_intent",
                    lease_id="lease-open",
                    occurred_at=started.isoformat(),
                ),
                _wal_line(
                    event_type="endpoint_finished",
                    lease_id="lease-open",
                    occurred_at=last.isoformat(),
                ),
                _wal_line(
                    event_type="claim_intent",
                    lease_id="lease-done",
                    occurred_at=started.isoformat(),
                ),
                _wal_line(
                    event_type="allocation_finished",
                    lease_id="lease-done",
                    occurred_at=last.isoformat(),
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    state = watchdog.read_wal_state(wal)
    assert state.open_lease_ids == frozenset({"lease-open"})
    assert state.last_event_at["lease-open"] == pytest.approx(last.timestamp())
    assert state.size_bytes == wal.stat().st_size


def test_an_unknown_wal_version_is_not_trusted_at_all(tmp_path):
    # Reporting a partial view would let a restart through on a WAL we cannot
    # read; reporting nothing downgrades every restart to a freeze.
    wal = tmp_path / watchdog.WAL_FILE
    wal.write_text(
        json.dumps({"event_version": "sofascore-allocation-wal-v2", "lease_id": "x"})
        + "\n",
        encoding="utf-8",
    )
    state = watchdog.read_wal_state(wal)
    assert state.open_lease_ids == frozenset()
    assert state.last_event_at == {}


def test_a_missing_wal_reports_nothing_rather_than_raising(tmp_path):
    state = watchdog.read_wal_state(tmp_path / "absent.jsonl")
    assert state.open_lease_ids == frozenset()
    assert state.size_bytes == 0


# --- confirming the restart actually helped --------------------------------


def test_recovery_count_is_read_from_the_gateway_boot_log():
    blob = (
        "2026-07-28 12:56:36,001 INFO filter_proxy: restored 5 durable paid byte events\n"
        "2026-07-28 12:56:36,002 INFO filter_proxy: recovered 1 crash-orphaned "
        "SofaScore allocation attempts\n"
    )
    assert watchdog.recovered_attempt_count(blob) == 1


def test_a_replay_that_released_nothing_is_not_a_recovery():
    blob = (
        "2026-07-28 12:56:36,002 INFO filter_proxy: recovered 0 crash-orphaned "
        "SofaScore allocation attempts\n"
    )
    assert watchdog.recovered_attempt_count(blob) == 0


def test_a_boot_log_without_the_replay_line_reports_unknown():
    assert watchdog.recovered_attempt_count("nothing to see here\n") is None


# --- not burying the new alert under the permanent ones --------------------


def test_an_unchanged_alert_is_not_repeated_every_sample():
    # Four allocation claims have been orphaned since 2026-07-23 and will stay
    # orphaned until a human clears them; at one sample every five seconds that
    # is 17k notifications a day.
    throttle = watchdog.AlertThrottle()
    message = "5 durable allocation claim(s) outlived their lease"
    assert throttle.should_send(message, NOW) is True
    assert throttle.should_send(message, NOW + 5.0) is False
    assert throttle.should_send(message, NOW + 3600.0) is False
    assert throttle.should_send(message, NOW + watchdog.ALERT_REPEAT_SECONDS) is True


def test_a_changed_alert_goes_out_immediately():
    throttle = watchdog.AlertThrottle()
    assert throttle.should_send("5 claims orphaned", NOW) is True
    assert throttle.should_send("6 claims orphaned", NOW + 5.0) is True


# --- refusing to restart the wrong container -------------------------------


def test_verify_container_refuses_a_container_that_does_not_mount_our_state(
    monkeypatch, tmp_path
):
    # sofascore_proxy_filter answers to the same DNS alias from a different
    # compose project; restarting it would change nothing and hide the mine.
    def fake_run(command, *, timeout=60.0):
        return type(
            "P",
            (),
            {"returncode": 0, "stdout": json.dumps([{"Source": "/somewhere/else"}]), "stderr": ""},
        )()

    monkeypatch.setattr(watchdog, "_run", fake_run)
    with pytest.raises(RuntimeError, match="does not mount"):
        watchdog.verify_container("sofascore_proxy_filter", tmp_path, "")


def test_verify_container_accepts_the_gateway_that_mounts_state_and_release(
    monkeypatch, tmp_path
):
    release = "/root/dpf-release-aa2ce3ab"

    def fake_run(command, *, timeout=60.0):
        mounts = [{"Source": str(tmp_path)}, {"Source": release}]
        return type(
            "P", (), {"returncode": 0, "stdout": json.dumps(mounts), "stderr": ""}
        )()

    monkeypatch.setattr(watchdog, "_run", fake_run)
    assert watchdog.verify_container("sofascore_gw_951", tmp_path, release) == (
        "sofascore_gw_951"
    )
