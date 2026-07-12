"""PostgreSQL regression tests for FBref control-plane lock ordering."""

from __future__ import annotations

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from scrapers.fbref.control import (
    ControlStore,
    LeaseLost,
    StateConflict,
    TargetLease,
)


pytestmark = pytest.mark.integration


def _postgres_uri() -> str:
    uri = os.getenv("FBREF_TEST_POSTGRES_URI", "").strip()
    if not uri and os.getenv("FBREF_TEST_POSTGRES_USE_AIRFLOW_DB") == "1":
        uri = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").strip()
    if not uri:
        pytest.skip("FBREF_TEST_POSTGRES_URI is not configured")
    return uri.replace("postgresql+psycopg2://", "postgresql://", 1)


def _connect(psycopg2, dsn: str, application_name: str):
    return psycopg2.connect(
        dsn,
        application_name=application_name,
        options="-c statement_timeout=5000",
    )


def _factory(psycopg2, dsn: str, application_name: str):
    return lambda _dsn: _connect(psycopg2, dsn, application_name)


def _wait_for_lock_waiters(admin, labels, futures) -> None:
    deadline = time.monotonic() + 3
    waiting = 0
    while time.monotonic() < deadline:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)
                FROM pg_stat_activity
                WHERE application_name = ANY(%s::text[])
                  AND wait_event_type = 'Lock'
                """,
                (list(labels),),
            )
            waiting = int(cursor.fetchone()[0])
        # PostgreSQL caches statistics snapshots inside a transaction.
        admin.rollback()
        if waiting == len(labels) or all(future.done() for future in futures):
            break
        time.sleep(0.02)
    early_failures = [
        repr(future.exception()) for future in futures if future.done()
    ]
    assert waiting == len(labels), (
        "workers did not all wait on crawl_run; "
        f"early results={early_failures}"
    )


def test_latest_content_guard_blocks_next_fetch_commit_until_stateful_exit():
    """B cannot commit while A still owns typed+stateful promotion scope."""

    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())
    target_id = f"fbref:stateful-fence:{uuid.uuid4()}"
    admin = _connect(psycopg2, dsn, "fbref-stateful-fence-admin")
    with admin:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES (%s, 'stateful-fence-test', 'succeeded', 1, 1024)
                """,
                (run_id,),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, refresh_policy,
                    state, last_content_hash
                ) VALUES (%s, 'player', %s, 'monthly', 'fetched', 'hash-a')
                """,
                (target_id, f"https://example.invalid/{target_id}"),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.fetch_attempt (
                    attempt_id, run_id, target_id, logical_refresh_id,
                    attempt_number, claim_token, lease_epoch, status,
                    content_hash, finished_at
                ) VALUES (%s, %s, %s, %s, 1, %s, 1, 'succeeded',
                          'hash-a', clock_timestamp())
                """,
                (
                    attempt_id,
                    run_id,
                    target_id,
                    refresh_id,
                    claim_token,
                ),
            )

    def commit_b():
        connection = _connect(psycopg2, dsn, "fbref-stateful-fence-b")
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE fbref_control.page_frontier
                        SET last_content_hash = 'hash-b'
                        WHERE target_id = %s
                        """,
                        (target_id,),
                    )
            return True
        finally:
            connection.close()

    store = ControlStore(dsn)
    executor = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with store.guard_latest_content(
            target_id, "hash-a", refresh_id
        ) as latest:
            assert latest is True
            future = executor.submit(commit_b)
            deadline = time.monotonic() + 2
            waiting = False
            while time.monotonic() < deadline and not future.done():
                with admin.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT wait_event_type
                        FROM pg_stat_activity
                        WHERE application_name = 'fbref-stateful-fence-b'
                        """
                    )
                    row = cursor.fetchone()
                admin.rollback()
                if row and row[0] == "Lock":
                    waiting = True
                    break
                time.sleep(0.02)
            assert waiting
            assert not future.done()
        assert future.result(timeout=5) is True
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
        admin.rollback()
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM fbref_control.fetch_attempt
                    WHERE attempt_id = %s
                    """,
                    (attempt_id,),
                )
                cursor.execute(
                    "DELETE FROM fbref_control.page_frontier WHERE target_id=%s",
                    (target_id,),
                )
                cursor.execute(
                    "DELETE FROM fbref_control.crawl_run WHERE run_id=%s",
                    (run_id,),
                )
        admin.close()


def test_abort_and_reaper_wait_on_run_before_downstream_rows():
    """Both terminal paths serialize at crawl_run without a lock cycle."""

    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    target_id = f"fbref:lock-order:{uuid.uuid4()}"
    claim_token = str(uuid.uuid4())
    reservation_id = str(uuid.uuid4())
    labels = {
        "abort": f"fbref-lock-abort-{uuid.uuid4()}",
        "reaper": f"fbref-lock-reaper-{uuid.uuid4()}",
    }

    admin = _connect(psycopg2, dsn, "fbref-lock-order-admin")
    locker = None
    executor = ThreadPoolExecutor(max_workers=2)
    futures = []
    try:
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.crawl_run (
                        run_id, run_type, status, request_limit, byte_limit,
                        requests_reserved, bytes_reserved
                    ) VALUES (%s, 'lock-order-test', 'running', 10, 10000, 1, 1024)
                    """,
                    (run_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, refresh_policy,
                        state, claim_token, lease_epoch, lease_run_id,
                        lease_refresh_id, leased_by, lease_expires_at
                    ) VALUES (
                        %s, 'lock-order-test', %s, 'lock-order-test',
                        'leased', %s, 1, %s, %s, 'lock-order-test',
                        clock_timestamp() - interval '1 minute'
                    )
                    """,
                    (
                        target_id,
                        f"https://example.invalid/{target_id}",
                        claim_token,
                        run_id,
                        refresh_id,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.budget_reservation (
                        reservation_id, run_id, logical_refresh_id,
                        requests_reserved, bytes_reserved
                    ) VALUES (%s, %s, %s, 1, 1024)
                    """,
                    (reservation_id, run_id, refresh_id),
                )

        locker = _connect(psycopg2, dsn, "fbref-lock-order-holder")
        with locker.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run_id,),
            )

        abort_store = ControlStore(
            dsn,
            connection_factory=_factory(psycopg2, dsn, labels["abort"]),
        )
        reaper_store = ControlStore(
            dsn,
            connection_factory=_factory(psycopg2, dsn, labels["reaper"]),
        )
        futures = [
            executor.submit(abort_store.abort_run, run_id),
            executor.submit(reaper_store.reap_expired_leases),
        ]

        _wait_for_lock_waiters(admin, labels.values(), futures)

        # While crawl_run is held, neither worker may hold a downstream row.
        probe = _connect(psycopg2, dsn, "fbref-lock-order-probe")
        try:
            with probe.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT target_id FROM fbref_control.page_frontier
                    WHERE target_id = %s FOR UPDATE NOWAIT
                    """,
                    (target_id,),
                )
                cursor.execute(
                    """
                    SELECT reservation_id
                    FROM fbref_control.budget_reservation
                    WHERE reservation_id = %s FOR UPDATE NOWAIT
                    """,
                    (reservation_id,),
                )
        finally:
            probe.rollback()
            probe.close()

        locker.rollback()
        locker.close()
        locker = None

        results = [future.result(timeout=8) for future in futures]
        assert any(result == 0 or result == 1 for result in results)
        assert any(isinstance(result, dict) for result in results)
        assert not any(future.exception() for future in futures)
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        executor.shutdown(wait=True, cancel_futures=True)
        admin.rollback()
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM fbref_control.budget_reservation
                    WHERE reservation_id = %s
                    """,
                    (reservation_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM fbref_control.page_frontier
                    WHERE target_id = %s
                    """,
                    (target_id,),
                )
                cursor.execute(
                    "DELETE FROM fbref_control.crawl_run WHERE run_id = %s",
                    (run_id,),
                )
        admin.close()


@pytest.mark.parametrize("allocator", ["due_cohort", "claim"])
def test_abort_and_allocator_serialize_before_frontier(allocator):
    """Cohort creation and claiming cannot invert abort's run lock."""

    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    run_id = str(uuid.uuid4())
    target_id = f"fbref:lock-order:{uuid.uuid4()}"
    labels = {
        "abort": f"fbref-lock-abort-{uuid.uuid4()}",
        "allocator": f"fbref-lock-{allocator}-{uuid.uuid4()}",
    }
    admin = _connect(psycopg2, dsn, "fbref-lock-allocator-admin")
    locker = None
    executor = ThreadPoolExecutor(max_workers=2)
    futures = []
    try:
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.crawl_run (
                        run_id, run_type, status, request_limit, byte_limit
                    ) VALUES (%s, 'lock-order-test', 'running', 10, 10000)
                    """,
                    (run_id,),
                )
                # Quarantined is intentionally ineligible. The regression is
                # that the allocator must wait on run before even scanning
                # and attempting to lock frontier candidates.
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, refresh_policy,
                        state
                    ) VALUES (
                        %s, 'lock-order-test', %s, 'lock-order-test',
                        'quarantined'
                    )
                    """,
                    (target_id, f"https://example.invalid/{target_id}"),
                )

        locker = _connect(psycopg2, dsn, "fbref-lock-allocator-holder")
        with locker.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run_id,),
            )

        abort_store = ControlStore(
            dsn,
            connection_factory=_factory(psycopg2, dsn, labels["abort"]),
        )
        allocator_store = ControlStore(
            dsn,
            connection_factory=_factory(
                psycopg2, dsn, labels["allocator"]
            ),
        )

        def allocate():
            if allocator == "due_cohort":
                return allocator_store.create_due_run_cohort(
                    run_id, page_kinds=["lock-order-test"], limit=1
                )
            return allocator_store.claim_targets(
                run_id, "lock-order-worker", limit=1
            )

        futures = [
            executor.submit(abort_store.abort_run, run_id),
            executor.submit(allocate),
        ]
        _wait_for_lock_waiters(admin, labels.values(), futures)

        probe = _connect(psycopg2, dsn, "fbref-lock-allocator-probe")
        try:
            with probe.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT target_id FROM fbref_control.page_frontier
                    WHERE target_id = %s FOR UPDATE NOWAIT
                    """,
                    (target_id,),
                )
        finally:
            probe.rollback()
            probe.close()

        locker.rollback()
        locker.close()
        locker = None

        abort_result = futures[0].result(timeout=8)
        assert abort_result["status"] == "failed"
        try:
            allocation_result = futures[1].result(timeout=8)
        except StateConflict:
            assert allocator == "due_cohort"
        else:
            assert allocation_result == []
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        executor.shutdown(wait=True, cancel_futures=True)
        admin.rollback()
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM fbref_control.page_frontier
                    WHERE target_id = %s
                    """,
                    (target_id,),
                )
                cursor.execute(
                    "DELETE FROM fbref_control.crawl_run WHERE run_id = %s",
                    (run_id,),
                )
        admin.close()


def test_abort_and_bind_reservation_serialize_before_attempt():
    """Binding cannot hold attempt while abort holds its reservation."""

    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    target_id = f"fbref:lock-order:{uuid.uuid4()}"
    claim_token = str(uuid.uuid4())
    reservation_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    labels = {
        "abort": f"fbref-lock-abort-{uuid.uuid4()}",
        "bind": f"fbref-lock-bind-{uuid.uuid4()}",
    }
    lease = TargetLease(
        attempt_id=attempt_id,
        run_id=run_id,
        target_id=target_id,
        logical_refresh_id=refresh_id,
        canonical_url=f"https://example.invalid/{target_id}",
        page_kind="lock-order-test",
        source_ids={},
        claim_token=claim_token,
        lease_epoch=1,
        attempt_number=1,
        leased_by="lock-order-test",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    admin = _connect(psycopg2, dsn, "fbref-lock-bind-admin")
    locker = None
    executor = ThreadPoolExecutor(max_workers=2)
    futures = []
    try:
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.crawl_run (
                        run_id, run_type, status, request_limit, byte_limit,
                        requests_reserved, bytes_reserved
                    ) VALUES (%s, 'lock-order-test', 'running', 10, 10000, 1, 1024)
                    """,
                    (run_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, refresh_policy,
                        state, claim_token, lease_epoch, lease_run_id,
                        lease_refresh_id, leased_by, lease_expires_at
                    ) VALUES (
                        %s, 'lock-order-test', %s, 'lock-order-test',
                        'leased', %s, 1, %s, %s, 'lock-order-test',
                        clock_timestamp() + interval '5 minutes'
                    )
                    """,
                    (
                        target_id,
                        lease.canonical_url,
                        claim_token,
                        run_id,
                        refresh_id,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.budget_reservation (
                        reservation_id, run_id, logical_refresh_id,
                        requests_reserved, bytes_reserved
                    ) VALUES (%s, %s, %s, 1, 1024)
                    """,
                    (reservation_id, run_id, refresh_id),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.fetch_attempt (
                        attempt_id, run_id, target_id, logical_refresh_id,
                        attempt_number, claim_token, lease_epoch, status
                    ) VALUES (%s, %s, %s, %s, 1, %s, 1, 'claimed')
                    """,
                    (
                        attempt_id,
                        run_id,
                        target_id,
                        refresh_id,
                        claim_token,
                    ),
                )

        locker = _connect(psycopg2, dsn, "fbref-lock-bind-holder")
        with locker.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id FROM fbref_control.crawl_run
                WHERE run_id = %s FOR UPDATE
                """,
                (run_id,),
            )

        abort_store = ControlStore(
            dsn,
            connection_factory=_factory(psycopg2, dsn, labels["abort"]),
        )
        bind_store = ControlStore(
            dsn,
            connection_factory=_factory(psycopg2, dsn, labels["bind"]),
        )
        futures = [
            executor.submit(abort_store.abort_run, run_id),
            executor.submit(bind_store.bind_reservation, lease, reservation_id),
        ]
        _wait_for_lock_waiters(admin, labels.values(), futures)

        probe = _connect(psycopg2, dsn, "fbref-lock-bind-probe")
        try:
            with probe.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT target_id FROM fbref_control.page_frontier
                    WHERE target_id = %s FOR UPDATE NOWAIT
                    """,
                    (target_id,),
                )
                cursor.execute(
                    """
                    SELECT reservation_id
                    FROM fbref_control.budget_reservation
                    WHERE reservation_id = %s FOR UPDATE NOWAIT
                    """,
                    (reservation_id,),
                )
                cursor.execute(
                    """
                    SELECT attempt_id FROM fbref_control.fetch_attempt
                    WHERE attempt_id = %s FOR UPDATE NOWAIT
                    """,
                    (attempt_id,),
                )
        finally:
            probe.rollback()
            probe.close()

        locker.rollback()
        locker.close()
        locker = None

        abort_result = futures[0].result(timeout=8)
        assert abort_result["status"] == "failed"
        try:
            assert futures[1].result(timeout=8) is None
        except LeaseLost:
            pass
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        executor.shutdown(wait=True, cancel_futures=True)
        admin.rollback()
        with admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM fbref_control.fetch_attempt
                    WHERE attempt_id = %s
                    """,
                    (attempt_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM fbref_control.budget_reservation
                    WHERE reservation_id = %s
                    """,
                    (reservation_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM fbref_control.page_frontier
                    WHERE target_id = %s
                    """,
                    (target_id,),
                )
                cursor.execute(
                    "DELETE FROM fbref_control.crawl_run WHERE run_id = %s",
                    (run_id,),
                )
        admin.close()
