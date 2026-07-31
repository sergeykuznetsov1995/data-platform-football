"""Production orchestration contracts for the ESPN native pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading

import pytest

from scrapers.espn.operations import (
    LeaseConflict,
    LeaseLost,
    MemoryRequestPermitStore,
    MemoryScopeLeaseStore,
    OperationsError,
    PostgresEspnControlStore,
    PublicationFence,
    RunManifestEvidence,
    evaluate_alerts,
    plan_summary_batches,
    producer_state_failures,
    reduce_raw_checkpoints,
    seal_raw_batch_descriptor,
    seal_raw_checkpoint,
)


UTC = timezone.utc


def _raw_request(request_id: str, endpoint: str, *, event_id=None):
    return {
        "request_id": request_id,
        "scope_id": "700:2026",
        "endpoint": endpoint,
        "event_id": event_id,
        "url_fingerprint": "b" * 64,
        "raw_uri": f"s3://raw/{request_id.replace(':', '-')}.json.gz",
        "raw_sha256": "c" * 64,
        "fetched_at": "2026-07-31T12:00:00Z",
        "http_status": 200,
        "direct_bytes": 10,
        "proxy_bytes": 0,
        "query_start": "2026-07-01" if endpoint == "scoreboard" else None,
        "query_end": "2027-06-30" if endpoint == "scoreboard" else None,
    }


def test_scope_leases_are_atomic_sorted_and_retry_idempotent():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)

    leases = store.acquire_many(
        ("740:2026", "700:2026"),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=30),
    )
    retry = store.acquire_many(
        ("700:2026", "740:2026"),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now + timedelta(minutes=1),
        ttl=timedelta(minutes=30),
    )

    assert tuple(lease.scope_id for lease in leases) == (
        "700:2026",
        "740:2026",
    )
    assert retry == leases
    assert all(lease.epoch == 1 for lease in leases)


def test_scope_lease_batch_conflict_acquires_nothing():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    store.acquire_many(
        ("740:2026",),
        owner_id="other/run/1",
        plan_signature="b" * 64,
        now=now,
        ttl=timedelta(hours=1),
    )

    with pytest.raises(LeaseConflict, match="740:2026"):
        store.acquire_many(
            ("700:2026", "740:2026"),
            owner_id="dag/run/1",
            plan_signature="a" * 64,
            now=now,
            ttl=timedelta(minutes=30),
        )

    assert store.current("700:2026") is None


def test_expired_scope_lease_reclaims_with_new_epoch_and_fences_old_owner():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    old = store.acquire_many(
        ("700:2026",),
        owner_id="old/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=10),
    )[0]
    new = store.acquire_many(
        ("700:2026",),
        owner_id="new/run/1",
        plan_signature="b" * 64,
        now=now + timedelta(minutes=11),
        ttl=timedelta(minutes=10),
    )[0]

    assert new.epoch == old.epoch + 1
    with pytest.raises(LeaseLost, match="700:2026"):
        store.assert_owned(old, now=now + timedelta(minutes=11))
    store.assert_owned(new, now=now + timedelta(minutes=11))


def test_lease_renew_and_release_require_exact_epoch_and_are_idempotent():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    lease = store.acquire_many(
        ("700:2026",),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=10),
    )[0]

    renewed = store.renew(
        lease,
        now=now + timedelta(minutes=5),
        ttl=timedelta(minutes=30),
    )
    retry = store.renew(
        renewed,
        now=now + timedelta(minutes=5),
        ttl=timedelta(minutes=30),
    )

    assert renewed.expires_at == now + timedelta(minutes=35)
    assert retry == renewed
    store.release(renewed, now=now + timedelta(minutes=6))
    store.release(renewed, now=now + timedelta(minutes=6))
    assert store.current("700:2026") is None

    replacement = store.acquire_many(
        ("700:2026",),
        owner_id="other/run/1",
        plan_signature="b" * 64,
        now=now + timedelta(minutes=7),
        ttl=timedelta(minutes=10),
    )[0]
    assert replacement.epoch == lease.epoch + 1
    with pytest.raises(LeaseLost):
        store.release(renewed, now=now + timedelta(minutes=8))


def test_postgres_release_reads_exact_current_lease_before_update():
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    lease = MemoryScopeLeaseStore().acquire_many(
        ("700:2026",),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=10),
    )[0]

    class Cursor:
        rowcount = 1

        def __init__(self):
            self.sql = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            self.sql.append((sql, params))

        def fetchone(self):
            return (
                lease.scope_id,
                lease.owner_id,
                lease.plan_signature,
                lease.epoch,
                lease.token_sha256,
                lease.acquired_at,
                lease.expires_at,
            )

    cursor = Cursor()

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return cursor

        def close(self):
            pass

    PostgresEspnControlStore(lambda: Connection()).release(lease, now=now)

    assert any("FOR UPDATE" in sql for sql, _ in cursor.sql)
    assert any("SET expires_at" in sql for sql, _ in cursor.sql)


def test_same_owner_cannot_rebind_live_lease_to_a_different_plan():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    store.acquire_many(
        ("700:2026",),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=30),
    )

    with pytest.raises(LeaseConflict, match="plan"):
        store.acquire_many(
            ("700:2026",),
            owner_id="dag/run/1",
            plan_signature="b" * 64,
            now=now + timedelta(minutes=1),
            ttl=timedelta(minutes=30),
        )


def test_scope_lease_bundle_binds_atomically_to_exact_signed_plans():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    acquired = store.acquire_many(
        ("740:2026", "700:2026"),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(hours=1),
    )

    bound = store.bind_plans(
        acquired,
        {"700:2026": "b" * 64, "740:2026": "c" * 64},
        now=now,
    )

    assert [item.plan_signature for item in bound] == ["b" * 64, "c" * 64]
    assert [item.epoch for item in bound] == [1, 1]
    with pytest.raises(LeaseLost):
        store.assert_owned(acquired[0], now=now)
    store.assert_owned(bound[0], now=now)


def test_scope_plan_binding_rejects_partial_bundle_without_mutation():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    acquired = store.acquire_many(
        ("700:2026", "740:2026"),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(hours=1),
    )

    with pytest.raises(ValueError, match="scope set"):
        store.bind_plans(acquired, {"700:2026": "b" * 64}, now=now)

    assert store.read_owner_leases("dag/run/1") == acquired


def test_head_set_is_read_only_after_exact_bundle_lease_is_owned():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    old = store.acquire_many(
        ("700:2026",),
        owner_id="old/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=1),
    )
    replacement = store.acquire_many(
        ("700:2026",),
        owner_id="new/run/1",
        plan_signature="b" * 64,
        now=now + timedelta(minutes=2),
        ttl=timedelta(minutes=10),
    )

    with pytest.raises(LeaseLost):
        store.read_scope_heads_owned(old, now=now + timedelta(minutes=2))
    assert (
        store.read_scope_heads_owned(replacement, now=now + timedelta(minutes=2)) == {}
    )


def test_publication_guard_blocks_reclaim_until_manifest_boundary_exits():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    old = store.acquire_many(
        ("700:2026",),
        owner_id="old/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(minutes=1),
    )[0]
    entered = threading.Event()
    reclaimed = threading.Event()

    def reclaim():
        entered.set()
        store.acquire_many(
            ("700:2026",),
            owner_id="new/run/1",
            plan_signature="b" * 64,
            now=now + timedelta(minutes=2),
            ttl=timedelta(minutes=10),
        )
        reclaimed.set()

    with store.publication_guard(old, now=now + timedelta(seconds=30)):
        thread = threading.Thread(target=reclaim)
        thread.start()
        assert entered.wait(1)
        assert reclaimed.wait(0.05) is False

    thread.join(timeout=1)
    assert reclaimed.is_set()
    assert store.current("700:2026").epoch == old.epoch + 1


def test_run_evidence_identity_includes_dag_id_for_colliding_run_ids():
    store = MemoryScopeLeaseStore()
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    lease = store.acquire_many(
        ("700:2026",),
        owner_id="dag_ingest_espn/shared-run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(hours=1),
    )[0]
    for dag_id in ("dag_ingest_espn", "dag_repair_espn"):
        evidence = RunManifestEvidence(
            dag_id=dag_id,
            run_id="shared-run",
            attempt=1,
            scope_id="700:2026",
            plan_signature="a" * 64,
            registry_signature="b" * 64,
            state="noop",
            evidence_uri=f"s3://evidence/{dag_id}.json",
            evidence_sha256=("c" if dag_id == "dag_ingest_espn" else "d") * 64,
            recorded_at=now,
        )
        with store.publication_guard(lease, now=now) as fence:
            fence.record_evidence(evidence)

    daily = store.read_run_evidence(
        dag_id="dag_ingest_espn", run_id="shared-run", attempt=1
    )
    repair = store.read_run_evidence(
        dag_id="dag_repair_espn", run_id="shared-run", attempt=1
    )

    assert [item.dag_id for item in daily] == ["dag_ingest_espn"]
    assert [item.dag_id for item in repair] == ["dag_repair_espn"]


def test_postgres_evidence_read_binds_exact_dag_run_and_attempt():
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            executed.append((sql, params))

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def close(self):
            pass

    store = PostgresEspnControlStore(lambda: Connection())
    assert (
        store.read_run_evidence(
            dag_id="dag_backfill_espn", run_id="shared-run", attempt=3
        )
        == ()
    )
    sql, params = executed[-1]
    assert "WHERE dag_id = %s AND run_id = %s AND attempt = %s" in sql
    assert params == ("dag_backfill_espn", "shared-run", 3)


def test_postgres_migration_upgrades_existing_v2_evidence_identity():
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            executed.append((sql, params))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def close(self):
            pass

    PostgresEspnControlStore(lambda: Connection()).migrate()
    sql = "\n".join(statement for statement, _ in executed)

    assert "ADD COLUMN IF NOT EXISTS dag_id text" in sql
    assert "espn-legacy-v2-unknown" in sql
    assert "PRIMARY KEY (dag_id, run_id, attempt, scope_id)" in sql


def test_publication_fence_exposes_injected_publication_clock():
    published_at = datetime(2026, 7, 31, 12, 34, 56, tzinfo=UTC)
    fence = PublicationFence(
        lambda: None, lambda _head, _evidence: None, lambda: published_at
    )

    assert fence.publication_time() == published_at


def test_publication_fence_refreshes_clock_after_complete_boundary():
    before = datetime(2026, 7, 31, 12, 34, 56, tzinfo=UTC)
    after = datetime(2026, 7, 31, 12, 34, 57, tzinfo=UTC)
    observed = iter((before, after))
    fence = PublicationFence(
        lambda: None, lambda _head, _evidence: None, lambda: next(observed)
    )

    fence()

    assert fence.publication_time() == after


def test_two_wave_summary_batches_are_deterministic_bounded_descriptors():
    planned = plan_summary_batches(
        tuple(range(101, 202)),
        run_id="run-1",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        max_events=100,
    )

    assert [len(batch["event_ids"]) for batch in planned["batches"]] == [50, 50]
    assert planned["budget_used"] == 100
    assert planned["budget_exhausted"] is True
    assert planned["pending_event_ids"] == [201]
    assert all(batch["run_id"] == "run-1" for batch in planned["batches"])
    assert all(batch["scope_id"] == "700:2026" for batch in planned["batches"])
    assert all(batch["descriptor_sha256"] for batch in planned["batches"])


def test_mapped_scope_plan_covers_full_season_in_fifty_event_batches():
    planned = plan_summary_batches(
        tuple(range(1, 381)),
        run_id="backfill-run",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        max_events=1000,
    )

    assert sum(len(batch["event_ids"]) for batch in planned["batches"]) == 380
    assert max(len(batch["event_ids"]) for batch in planned["batches"]) == 50
    assert planned["pending_event_ids"] == []
    assert planned["budget_exhausted"] is False


def test_raw_checkpoint_reducer_is_deterministic_and_rejects_stale_identity():
    scoreboard = seal_raw_checkpoint(
        endpoint="scoreboard",
        run_id="run-1",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="scoreboard-1",
        requests=(_raw_request("scoreboard:one", "scoreboard"),),
    )
    summary = seal_raw_checkpoint(
        endpoint="summary",
        run_id="run-1",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="summary-1",
        requests=(_raw_request("summary:101", "summary", event_id=101),),
    )
    kwargs = dict(
        run_id="run-1",
        attempt=1,
        mode="daily",
        as_of="2026-07-31",
        registry_signature="d" * 64,
        plan_signature="a" * 64,
        selected_scopes=("700:2026",),
        expected_batches=(
            seal_raw_batch_descriptor(
                endpoint="scoreboard",
                run_id="run-1",
                attempt=1,
                scope_id="700:2026",
                plan_signature="a" * 64,
                batch_id="scoreboard-1",
                request_ids=("scoreboard:one",),
                event_ids=(),
            ),
            seal_raw_batch_descriptor(
                endpoint="summary",
                run_id="run-1",
                attempt=1,
                scope_id="700:2026",
                plan_signature="a" * 64,
                batch_id="summary-1",
                request_ids=("summary:101",),
                event_ids=(101,),
            ),
        ),
        checkpoints=(summary, scoreboard),
    )

    first = reduce_raw_checkpoints(**kwargs)
    second = reduce_raw_checkpoints(**kwargs)

    assert first == second
    assert first["kind"] == "espn-raw-run-manifest-v1"
    assert [item["endpoint"] for item in first["checkpoints"]] == [
        "scoreboard",
        "summary",
    ]
    assert first["manifest_sha256"]

    stale = seal_raw_checkpoint(
        endpoint="summary",
        run_id="old-run",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="summary-1",
        requests=(_raw_request("summary:101", "summary", event_id=101),),
    )
    with pytest.raises(OperationsError, match="identity"):
        reduce_raw_checkpoints(**{**kwargs, "checkpoints": (scoreboard, stale)})

    with pytest.raises(OperationsError, match="exactly"):
        reduce_raw_checkpoints(**{**kwargs, "checkpoints": (scoreboard,)})

    extra = seal_raw_checkpoint(
        endpoint="summary",
        run_id="run-1",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="summary-extra",
        requests=(_raw_request("summary:102", "summary", event_id=102),),
    )
    with pytest.raises(OperationsError, match="exactly"):
        reduce_raw_checkpoints(
            **{**kwargs, "checkpoints": (scoreboard, summary, extra)}
        )


def test_shared_request_permit_survives_task_client_recreation():
    gate = MemoryRequestPermitStore(requests_per_minute=30)
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)

    first = gate.acquire(now=now)
    # A new mapped task uses the same durable gate state, not a fresh local
    # four-token burst.
    second = gate.acquire(now=now)
    third = gate.acquire(now=now + timedelta(seconds=1))

    assert first == now
    assert second == now + timedelta(seconds=2)
    assert third == now + timedelta(seconds=4)


def test_zero_summary_map_skip_is_success_only_when_signed_count_is_zero():
    base = {"offline_parse": ("success",)}

    assert (
        producer_state_failures(
            {**base, "fetch_summary_batches": ("skipped",)},
            {"offline_parse": 1, "fetch_summary_batches": 0},
        )
        == ()
    )
    assert producer_state_failures(
        {**base, "fetch_summary_batches": ("skipped",)},
        {"offline_parse": 1, "fetch_summary_batches": 1},
    )


@pytest.mark.parametrize("bad_state", ["failed", "skipped", "upstream_failed"])
def test_terminal_state_reducer_rejects_one_bad_mapped_producer(bad_state):
    failures = producer_state_failures(
        {"fetch_summary_batches": ("success", bad_state, "success")},
        {"fetch_summary_batches": 3},
    )

    assert failures
    assert bad_state in failures[0]


@pytest.mark.parametrize("bad_state", ["failed", "skipped", "upstream_failed"])
@pytest.mark.parametrize(
    "task_id",
    [
        "validate_registry_and_admission",
        "acquire_scope_leases",
        "build_signed_scope_plans",
    ],
)
def test_terminal_rejects_non_success_singleton_control_producer(task_id, bad_state):
    failures = producer_state_failures({task_id: (bad_state,)}, {task_id: 1})

    assert failures == (f"{task_id}: expected 1 success states, got ['{bad_state}']",)


@pytest.mark.parametrize(
    ("used", "age", "expected"),
    [
        (79, 35, set()),
        (80, 35, {("warning", "request_budget_80")}),
        (100, 35, {("hard", "request_budget_100")}),
        (79, 36, {("hard", "freshness_36h")}),
    ],
)
def test_alert_threshold_boundaries_are_deterministic(used, age, expected):
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    snapshot = {
        "run_id": "run-1",
        "attempt": 1,
        "scope_id": "700:2026",
        "plan_signature": "a" * 64,
        "registry_signature": "b" * 64,
        "state": "complete",
        "last_complete_at": now - timedelta(hours=age),
        "direct_requests": used,
        "request_budget": 100,
        "proxy_bytes": 0,
        "lease_conflict": False,
        "unpromoted_current_season": False,
        "unresolved_discovery_diffs": False,
    }

    first = evaluate_alerts(snapshot, observed_at=now)
    second = evaluate_alerts(snapshot, observed_at=now)

    assert first == second
    assert {(item["severity"], item["code"]) for item in first} == expected


def test_hard_and_warning_alert_inputs_are_not_decorative():
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    alerts = evaluate_alerts(
        {
            "run_id": "run-1",
            "attempt": 2,
            "scope_id": "700:2027",
            "plan_signature": "a" * 64,
            "registry_signature": "b" * 64,
            "state": "schema_drift",
            "last_complete_at": now,
            "direct_requests": 80,
            "request_budget": 100,
            "proxy_bytes": 1,
            "lease_conflict": True,
            "unpromoted_current_season": True,
            "unresolved_discovery_diffs": True,
        },
        observed_at=now,
    )

    assert {(item["severity"], item["code"]) for item in alerts} == {
        ("hard", "schema_drift"),
        ("hard", "proxy_usage"),
        ("hard", "lease_conflict"),
        ("hard", "unpromoted_current_season"),
        ("warning", "request_budget_80"),
        ("warning", "unresolved_discovery_diffs"),
    }


def test_run_level_schema_alert_is_identity_bound_before_scope_admission():
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    alerts = evaluate_alerts(
        {
            "dag_id": "dag_ingest_espn",
            "run_id": "run-1",
            "attempt": 1,
            "scope_id": None,
            "subject_dag_id": None,
            "subject_run_id": None,
            "identity_kind": "admission",
            "identity_sha256": "e" * 64,
            "state": "schema_drift",
            "last_complete_at": None,
            "direct_requests": 0,
            "request_budget": 100,
            "proxy_bytes": 0,
            "lease_conflict": False,
            "unpromoted_current_season": False,
            "unresolved_discovery_diffs": False,
        },
        observed_at=now,
    )

    assert alerts[0]["scope_id"] is None
    assert alerts[0]["identity_kind"] == "admission"
    assert alerts[0]["identity_sha256"] == "e" * 64
    assert "plan_signature" not in alerts[0]
    assert {item["code"] for item in alerts} == {
        "schema_drift",
        "freshness_36h",
    }

    with pytest.raises(ValueError, match="sentinel"):
        evaluate_alerts(
            {
                "dag_id": "dag_ingest_espn",
                "run_id": "run-1",
                "attempt": 1,
                "scope_id": None,
                "subject_dag_id": None,
                "subject_run_id": None,
                "identity_kind": "admission",
                "identity_sha256": "0" * 64,
                "state": "schema_drift",
                "last_complete_at": None,
                "direct_requests": 0,
                "request_budget": 100,
                "proxy_bytes": 0,
                "lease_conflict": False,
                "unpromoted_current_season": False,
                "unresolved_discovery_diffs": False,
            },
            observed_at=now,
        )
