"""Executable PostgreSQL evidence for persistent FBref metering."""

from __future__ import annotations

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest

from scrapers.fbref.control import ControlStore, StateConflict
from scrapers.fbref.fetcher import PersistentMeteredSessionReceipt
from scrapers.fbref.proxy_lease import METER_ID

from tests.integration.scrapers.test_fbref_control_admission import (  # noqa: F401
    isolated_postgres_uri,
)


pytestmark = pytest.mark.integration


def _seed_run_and_attempt(connection, *, byte_limit: int = 10_000):
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    target_id = f"fbref:persistent-test:{uuid.uuid4()}"
    claim_token = str(uuid.uuid4())
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit,
                    metadata
                ) VALUES (%s, 'current', 'running', 100, %s, %s::jsonb)
                """,
                (
                    run_id,
                    byte_limit,
                    json.dumps({"persistent_http_session": True}),
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, refresh_policy,
                    state, claim_token, lease_epoch, lease_run_id,
                    lease_refresh_id, leased_by, lease_expires_at
                ) VALUES (
                    %s, 'match', %s, 'daily', 'leased', %s, 1, %s, %s,
                    'persistent-test', clock_timestamp() + interval '10 minutes'
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
                INSERT INTO fbref_control.run_target (
                    run_id, target_id, logical_refresh_id, ordinal, status
                ) VALUES (%s, %s, %s, 0, 'leased')
                """,
                (run_id, target_id, refresh_id),
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
    return run_id, refresh_id, attempt_id


def _seed_attempt_for_run(connection, run_id: str, *, ordinal: int):
    refresh_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    target_id = f"fbref:persistent-test:{uuid.uuid4()}"
    claim_token = str(uuid.uuid4())
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, refresh_policy,
                    state, claim_token, lease_epoch, lease_run_id,
                    lease_refresh_id, leased_by, lease_expires_at
                ) VALUES (
                    %s, 'match', %s, 'daily', 'leased', %s, 1, %s, %s,
                    'persistent-test', clock_timestamp() + interval '10 minutes'
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
                INSERT INTO fbref_control.run_target (
                    run_id, target_id, logical_refresh_id, ordinal, status
                ) VALUES (%s, %s, %s, %s, 'leased')
                """,
                (run_id, target_id, refresh_id, ordinal),
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
    return refresh_id, attempt_id


def _open_session(store: ControlStore, run_id: str) -> str:
    return store.open_clearance_session(
        domain="fbref.com",
        session_version="persistent-integration-v1",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        run_id=run_id,
    )


class _FailAfterCursor:
    def __init__(self, cursor, owner, marker):
        self._cursor = cursor
        self._owner = owner
        self._marker = marker

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def execute(self, sql, params=None):
        result = self._cursor.execute(sql, params)
        normalized = " ".join(str(sql).split())
        if not self._owner.failed and self._marker in normalized:
            self._owner.failed = True
            raise RuntimeError(f"failpoint after {self._marker}")
        return result

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        self._cursor.close()


class _FailAfterConnection:
    def __init__(self, connection, owner, marker):
        self._connection = connection
        self._owner = owner
        self._marker = marker

    def cursor(self, *args, **kwargs):
        return _FailAfterCursor(
            self._connection.cursor(*args, **kwargs),
            self._owner,
            self._marker,
        )

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()


class _FailAfterFactory:
    def __init__(self, psycopg2, marker):
        self._psycopg2 = psycopg2
        self.marker = marker
        self.failed = False

    def __call__(self, dsn):
        return _FailAfterConnection(
            self._psycopg2.connect(dsn), self, self.marker
        )


class _PauseAfterCursor:
    def __init__(self, cursor, owner):
        self._cursor = cursor
        self._owner = owner

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def execute(self, sql, params=None):
        result = self._cursor.execute(sql, params)
        normalized = " ".join(str(sql).split())
        if (
            not self._owner.paused
            and "SELECT session_id FROM fbref_control.clearance_session "
            "WHERE run_id" in normalized
        ):
            self._owner.paused = True
            self._owner.reached.set()
            if not self._owner.release.wait(timeout=5):
                raise RuntimeError("abort race test pause timed out")
        return result

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        self._cursor.close()


class _PauseAfterConnection:
    def __init__(self, connection, owner):
        self._connection = connection
        self._owner = owner

    def cursor(self, *args, **kwargs):
        return _PauseAfterCursor(
            self._connection.cursor(*args, **kwargs), self._owner
        )

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()


class _PauseAfterSessionScanFactory:
    def __init__(self, psycopg2):
        self._psycopg2 = psycopg2
        self.reached = Event()
        self.release = Event()
        self.paused = False

    def __call__(self, dsn):
        return _PauseAfterConnection(self._psycopg2.connect(dsn), self)


def _prepare_page_settlement(connection, store):
    run_id, refresh_id, attempt_id = _seed_run_and_attempt(connection)
    reservation = store.reserve_budget(
        run_id,
        refresh_id,
        attempt_id=attempt_id,
        requests=3,
        bytes_=500,
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.fetch_attempt
                SET reservation_id = %s
                WHERE attempt_id = %s
                """,
                (reservation.reservation_id, attempt_id),
            )
    session_id = _open_session(store, run_id)
    tail = store.reserve_clearance_session_tail(
        run_id,
        session_id,
        bytes_reserved=100,
        baseline_provider_bytes=0,
    )
    page_kwargs = dict(
        attempt_id=attempt_id,
        requests_used=3,
        provider_billed_bytes=120,
        browser_bootstrap_attempts=1,
        browser_bootstrap_requests=1,
        browser_document_bytes=20,
        browser_asset_bytes=10,
        browser_unobserved_bytes=5,
        http_requests=2,
        http_wire_bytes=80,
        decoded_html_bytes=60,
        compressed_raw_bytes=40,
    )
    return run_id, session_id, reservation, tail, page_kwargs


def test_page_tail_idempotency_and_full_run_reconciliation(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    (
        run_id,
        session_id,
        reservation,
        _tail,
        page_kwargs,
    ) = _prepare_page_settlement(connection, store)
    first = store.settle_clearance_session_page(
        session_id, reservation.reservation_id, **page_kwargs
    )
    repeated = store.settle_clearance_session_page(
        session_id, reservation.reservation_id, **page_kwargs
    )
    assert first["idempotent"] is False
    assert repeated["idempotent"] is True
    with pytest.raises(StateConflict):
        store.settle_clearance_session_page(
            session_id,
            reservation.reservation_id,
            **{**page_kwargs, "provider_billed_bytes": 121},
        )

    receipt = PersistentMeteredSessionReceipt(
        session_id=session_id,
        meter=METER_ID,
        baseline_provider_bytes=0,
        page_provider_bytes=120,
        authoritative_provider_bytes=130,
        tail_provider_bytes=10,
    )
    settled = store.settle_clearance_session_tail(session_id, receipt)
    settled_again = store.settle_clearance_session_tail(session_id, receipt)
    assert settled["idempotent"] is False
    assert settled_again["idempotent"] is True
    store.close_clearance_session(session_id)

    assert store.assert_persistent_metering_reconciled(run_id) == {
        "run_id": run_id,
        "sessions": 1,
        "provider_billed_bytes": 130,
        "requests_used": 3,
        "pages": 1,
        "reconciled": True,
    }
    connection.close()


def test_two_persistent_sessions_reconcile_cumulative_baselines_to_run_totals(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, first_refresh, first_attempt = _seed_run_and_attempt(connection)

    def settle_session(
        refresh_id,
        attempt_id,
        *,
        baseline,
        page_bytes,
        tail_bytes,
    ):
        reservation = store.reserve_budget(
            run_id,
            refresh_id,
            attempt_id=attempt_id,
            requests=1,
            bytes_=500,
        )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fbref_control.fetch_attempt
                    SET reservation_id = %s
                    WHERE attempt_id = %s
                    """,
                    (reservation.reservation_id, attempt_id),
                )
        session_id = _open_session(store, run_id)
        store.reserve_clearance_session_tail(
            run_id,
            session_id,
            bytes_reserved=100,
            baseline_provider_bytes=baseline,
        )
        store.settle_clearance_session_page(
            session_id,
            reservation.reservation_id,
            attempt_id=attempt_id,
            requests_used=1,
            provider_billed_bytes=page_bytes,
            http_requests=1,
            http_wire_bytes=page_bytes,
            decoded_html_bytes=max(1, page_bytes // 2),
            compressed_raw_bytes=max(1, page_bytes // 3),
        )
        authoritative = page_bytes + tail_bytes
        store.settle_clearance_session_tail(
            session_id,
            PersistentMeteredSessionReceipt(
                session_id=session_id,
                meter=METER_ID,
                baseline_provider_bytes=baseline,
                page_provider_bytes=page_bytes,
                authoritative_provider_bytes=authoritative,
                tail_provider_bytes=tail_bytes,
            ),
        )
        store.close_clearance_session(session_id)
        return authoritative

    first_total = settle_session(
        first_refresh,
        first_attempt,
        baseline=0,
        page_bytes=120,
        tail_bytes=10,
    )
    second_refresh, second_attempt = _seed_attempt_for_run(
        connection, run_id, ordinal=1
    )
    second_total = settle_session(
        second_refresh,
        second_attempt,
        baseline=first_total,
        page_bytes=70,
        tail_bytes=5,
    )

    reconciled = store.assert_persistent_metering_reconciled(run_id)

    assert reconciled["sessions"] == 2
    assert reconciled["pages"] == 2
    assert reconciled["requests_used"] == 2
    assert reconciled["provider_billed_bytes"] == first_total + second_total
    connection.close()


@pytest.mark.parametrize(
    "marker",
    [
        "INSERT INTO fbref_control.budget_reservation",
        "UPDATE fbref_control.crawl_run",
        "INSERT INTO fbref_control.clearance_session_tail_reservation",
    ],
)
def test_tail_reservation_rolls_back_after_each_mutation(
    isolated_postgres_uri,  # noqa: F811
    marker,
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(connection)
    session_id = _open_session(store, run_id)
    fail_factory = _FailAfterFactory(psycopg2, marker)
    failing_store = ControlStore(
        isolated_postgres_uri, connection_factory=fail_factory
    )

    with pytest.raises(RuntimeError, match="failpoint after"):
        failing_store.reserve_clearance_session_tail(
            run_id,
            session_id,
            bytes_reserved=100,
            baseline_provider_bytes=0,
        )

    assert fail_factory.failed is True
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT bytes_reserved
            FROM fbref_control.crawl_run WHERE run_id = %s
            """,
            (run_id,),
        )
        assert cursor.fetchone()[0] == 0
        cursor.execute(
            """
            SELECT count(*)
            FROM fbref_control.clearance_session_tail_reservation
            WHERE session_id = %s
            """,
            (session_id,),
        )
        assert cursor.fetchone()[0] == 0
    connection.rollback()
    installed = store.reserve_clearance_session_tail(
        run_id,
        session_id,
        bytes_reserved=100,
        baseline_provider_bytes=0,
    )
    assert installed["status"] == "reserved"
    connection.close()


@pytest.mark.parametrize(
    "marker",
    [
        "INSERT INTO fbref_control.clearance_session_page_accounting",
        "UPDATE fbref_control.budget_reservation",
        "UPDATE fbref_control.crawl_run",
        "UPDATE fbref_control.clearance_session",
    ],
)
def test_page_settlement_rolls_back_after_each_mutation(
    isolated_postgres_uri,  # noqa: F811
    marker,
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    (
        run_id,
        session_id,
        reservation,
        _tail,
        page_kwargs,
    ) = _prepare_page_settlement(connection, store)
    fail_factory = _FailAfterFactory(psycopg2, marker)
    failing_store = ControlStore(
        isolated_postgres_uri, connection_factory=fail_factory
    )

    with pytest.raises(RuntimeError, match="failpoint after"):
        failing_store.settle_clearance_session_page(
            session_id, reservation.reservation_id, **page_kwargs
        )

    assert fail_factory.failed is True
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status FROM fbref_control.budget_reservation
            WHERE reservation_id = %s
            """,
            (reservation.reservation_id,),
        )
        assert cursor.fetchone()[0] == "reserved"
        cursor.execute(
            """
            SELECT requests_used, bytes_used, requests_reserved, bytes_reserved
            FROM fbref_control.crawl_run WHERE run_id = %s
            """,
            (run_id,),
        )
        assert cursor.fetchone() == (0, 0, 3, 600)
        cursor.execute(
            """
            SELECT count(*)
            FROM fbref_control.clearance_session_page_accounting
            WHERE session_id = %s
            """,
            (session_id,),
        )
        assert cursor.fetchone()[0] == 0
    connection.rollback()
    installed = store.settle_clearance_session_page(
        session_id, reservation.reservation_id, **page_kwargs
    )
    assert installed["idempotent"] is False
    connection.close()


@pytest.mark.parametrize(
    "marker",
    [
        "UPDATE fbref_control.budget_reservation",
        "UPDATE fbref_control.crawl_run",
        "UPDATE fbref_control.clearance_session SET provider_billed_bytes",
        "UPDATE fbref_control.clearance_session_tail_reservation",
    ],
)
def test_tail_settlement_rolls_back_after_each_mutation(
    isolated_postgres_uri,  # noqa: F811
    marker,
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    (
        run_id,
        session_id,
        reservation,
        tail,
        page_kwargs,
    ) = _prepare_page_settlement(connection, store)
    store.settle_clearance_session_page(
        session_id, reservation.reservation_id, **page_kwargs
    )
    receipt = PersistentMeteredSessionReceipt(
        session_id=session_id,
        meter=METER_ID,
        baseline_provider_bytes=0,
        page_provider_bytes=120,
        authoritative_provider_bytes=130,
        tail_provider_bytes=10,
    )
    fail_factory = _FailAfterFactory(psycopg2, marker)
    failing_store = ControlStore(
        isolated_postgres_uri, connection_factory=fail_factory
    )

    with pytest.raises(RuntimeError, match="failpoint after"):
        failing_store.settle_clearance_session_tail(session_id, receipt)

    assert fail_factory.failed is True
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status FROM fbref_control.budget_reservation
            WHERE reservation_id = %s
            """,
            (tail["reservation_id"],),
        )
        assert cursor.fetchone()[0] == "reserved"
        cursor.execute(
            """
            SELECT bytes_used, bytes_reserved
            FROM fbref_control.crawl_run WHERE run_id = %s
            """,
            (run_id,),
        )
        assert cursor.fetchone() == (120, 100)
        cursor.execute(
            """
            SELECT provider_billed_bytes
            FROM fbref_control.clearance_session WHERE session_id = %s
            """,
            (session_id,),
        )
        assert cursor.fetchone()[0] is None
        cursor.execute(
            """
            SELECT status
            FROM fbref_control.clearance_session_tail_reservation
            WHERE session_id = %s
            """,
            (session_id,),
        )
        assert cursor.fetchone()[0] == "reserved"
    connection.rollback()
    installed = store.settle_clearance_session_tail(session_id, receipt)
    assert installed["idempotent"] is False
    connection.close()


def test_control_session_close_rolls_back_and_retries_idempotently(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(connection)
    session_id = _open_session(store, run_id)
    fail_factory = _FailAfterFactory(
        psycopg2, "UPDATE fbref_control.clearance_session SET status"
    )
    failing_store = ControlStore(
        isolated_postgres_uri, connection_factory=fail_factory
    )

    with pytest.raises(RuntimeError, match="failpoint after"):
        failing_store.close_clearance_session(session_id, status="failed")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status FROM fbref_control.clearance_session
            WHERE session_id = %s
            """,
            (session_id,),
        )
        assert cursor.fetchone()[0] == "active"
    connection.rollback()
    store.close_clearance_session(session_id, status="failed")
    store.close_clearance_session(session_id, status="failed")
    connection.close()


def test_tail_over_reservation_is_charged_and_latches_terminal(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(
        connection, byte_limit=5
    )
    session_id = _open_session(store, run_id)
    store.reserve_clearance_session_tail(
        run_id,
        session_id,
        bytes_reserved=5,
        baseline_provider_bytes=0,
    )
    receipt = PersistentMeteredSessionReceipt(
        session_id=session_id,
        meter=METER_ID,
        baseline_provider_bytes=0,
        page_provider_bytes=0,
        authoritative_provider_bytes=10,
        tail_provider_bytes=10,
    )

    settled = store.settle_clearance_session_tail(session_id, receipt)

    assert settled["terminal"] is True
    assert settled["budget_exceeded_by_tail"] is True
    assert settled["tail_over_reservation"] is True
    run = store.get_run(run_id)
    assert run["bytes_used"] == 10
    assert run["bytes_reserved"] == 0
    assert run["budget_exceeded"] is True
    connection.close()


def test_tail_over_reservation_is_terminal_even_with_run_budget_headroom(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(
        connection, byte_limit=100
    )
    session_id = _open_session(store, run_id)
    store.reserve_clearance_session_tail(
        run_id,
        session_id,
        bytes_reserved=5,
        baseline_provider_bytes=0,
    )

    settled = store.settle_clearance_session_tail(
        session_id,
        PersistentMeteredSessionReceipt(
            session_id=session_id,
            meter=METER_ID,
            baseline_provider_bytes=0,
            page_provider_bytes=0,
            authoritative_provider_bytes=10,
            tail_provider_bytes=10,
        ),
    )

    assert settled["terminal"] is True
    assert settled["budget_exceeded_by_tail"] is False
    assert settled["tail_over_reservation"] is True
    assert store.get_run(run_id)["budget_exceeded"] is False
    connection.close()


def test_page_budget_latch_is_not_relabelled_as_tail_overrun(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    (
        run_id,
        session_id,
        reservation,
        _tail,
        page_kwargs,
    ) = _prepare_page_settlement(connection, store)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fbref_control.crawl_run
                SET byte_limit = 600
                WHERE run_id = %s
                """,
                (run_id,),
            )

    page = store.settle_clearance_session_page(
        session_id,
        reservation.reservation_id,
        **{**page_kwargs, "provider_billed_bytes": 650},
    )
    tail = store.settle_clearance_session_tail(
        session_id,
        PersistentMeteredSessionReceipt(
            session_id=session_id,
            meter=METER_ID,
            baseline_provider_bytes=0,
            page_provider_bytes=650,
            authoritative_provider_bytes=650,
            tail_provider_bytes=0,
        ),
    )

    assert page["budget_exceeded"] is True
    assert page["budget_exceeded_by_page"] is True
    assert page["page_over_reservation"] is True
    assert tail["terminal"] is True
    assert tail["budget_exceeded_before_tail"] is True
    assert tail["budget_exceeded_by_tail"] is False
    assert tail["tail_over_reservation"] is False
    connection.close()


def test_active_orphan_blocks_new_tail_and_abort_charges_reserve(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(connection)
    first_session = _open_session(store, run_id)
    store.reserve_clearance_session_tail(
        run_id,
        first_session,
        bytes_reserved=100,
        baseline_provider_bytes=0,
    )
    second_session = _open_session(store, run_id)

    with pytest.raises(StateConflict, match="still active"):
        store.reserve_clearance_session_tail(
            run_id,
            second_session,
            bytes_reserved=100,
            baseline_provider_bytes=0,
        )

    result = store.abort_run(run_id)
    assert result["reservations_settled"] == 1
    run = store.get_run(run_id)
    assert run["bytes_used"] == 100
    assert run["bytes_reserved"] == 0
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status
            FROM fbref_control.clearance_session_tail_reservation
            WHERE session_id = %s
            """,
            (first_session,),
        )
        assert cursor.fetchone()[0] == "aborted"
    connection.rollback()
    connection.close()


def test_page_settlement_and_abort_serialize_at_run_before_child_rows(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    admin = psycopg2.connect(isolated_postgres_uri)
    store = ControlStore(isolated_postgres_uri)
    (
        run_id,
        session_id,
        reservation,
        tail,
        page_kwargs,
    ) = _prepare_page_settlement(admin, store)
    locker = psycopg2.connect(isolated_postgres_uri)
    with locker.cursor() as cursor:
        cursor.execute(
            """
            SELECT run_id FROM fbref_control.crawl_run
            WHERE run_id = %s FOR UPDATE
            """,
            (run_id,),
        )

    labels = {
        "page": f"fbref-persistent-page-{uuid.uuid4()}",
        "abort": f"fbref-persistent-abort-{uuid.uuid4()}",
    }

    def factory(label):
        return lambda _dsn: psycopg2.connect(
            isolated_postgres_uri,
            application_name=label,
            options="-c statement_timeout=5000",
        )

    page_store = ControlStore(
        isolated_postgres_uri, connection_factory=factory(labels["page"])
    )
    abort_store = ControlStore(
        isolated_postgres_uri, connection_factory=factory(labels["abort"])
    )
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [
        executor.submit(
            page_store.settle_clearance_session_page,
            session_id,
            reservation.reservation_id,
            **page_kwargs,
        ),
        executor.submit(abort_store.abort_run, run_id),
    ]
    probe = None
    try:
        deadline = time.monotonic() + 3
        waiting = 0
        while time.monotonic() < deadline:
            with admin.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT count(*) FROM pg_stat_activity
                    WHERE application_name = ANY(%s::text[])
                      AND wait_event_type = 'Lock'
                    """,
                    (list(labels.values()),),
                )
                waiting = int(cursor.fetchone()[0])
            admin.rollback()
            if waiting == 2:
                break
            time.sleep(0.02)
        assert waiting == 2

        probe = psycopg2.connect(isolated_postgres_uri)
        with probe.cursor() as cursor:
            cursor.execute(
                """
                SELECT reservation_id
                FROM fbref_control.budget_reservation
                WHERE reservation_id IN (%s, %s)
                ORDER BY reservation_id FOR UPDATE NOWAIT
                """,
                (reservation.reservation_id, tail["reservation_id"]),
            )
            cursor.execute(
                """
                SELECT session_id FROM fbref_control.clearance_session
                WHERE session_id = %s FOR UPDATE NOWAIT
                """,
                (session_id,),
            )
            cursor.execute(
                """
                SELECT session_id
                FROM fbref_control.clearance_session_tail_reservation
                WHERE session_id = %s FOR UPDATE NOWAIT
                """,
                (session_id,),
            )
        probe.rollback()

        locker.rollback()
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result(timeout=8))
            except StateConflict as exc:
                outcomes.append(exc)
        assert any(isinstance(item, dict) and item.get("aborted") for item in outcomes)
        assert all(
            isinstance(item, (dict, StateConflict)) for item in outcomes
        )
    finally:
        locker.rollback()
        locker.close()
        if probe is not None:
            probe.rollback()
            probe.close()
        executor.shutdown(wait=True, cancel_futures=True)
        admin.close()


def test_open_clearance_session_cannot_race_past_abort(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    run_id, _refresh_id, _attempt_id = _seed_run_and_attempt(connection)
    pause_factory = _PauseAfterSessionScanFactory(psycopg2)
    abort_store = ControlStore(
        isolated_postgres_uri, connection_factory=pause_factory
    )
    open_store = ControlStore(isolated_postgres_uri)
    executor = ThreadPoolExecutor(max_workers=2)
    abort_future = executor.submit(abort_store.abort_run, run_id)
    open_future = None
    try:
        assert pause_factory.reached.wait(timeout=5)
        open_future = executor.submit(_open_session, open_store, run_id)
        # The opener must wait behind the abort's run-row fence.  Once the
        # abort commits, it observes `failed` and cannot insert an active row.
        time.sleep(0.1)
        assert open_future.done() is False
        pause_factory.release.set()

        assert abort_future.result(timeout=8)["aborted"] is True
        with pytest.raises(StateConflict, match="running"):
            open_future.result(timeout=8)

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM fbref_control.crawl_run WHERE run_id = %s",
                (run_id,),
            )
            assert cursor.fetchone()[0] == "failed"
            cursor.execute(
                """
                SELECT count(*) FROM fbref_control.clearance_session
                WHERE run_id = %s AND status = 'active'
                """,
                (run_id,),
            )
            assert cursor.fetchone()[0] == 0
        connection.rollback()
    finally:
        pause_factory.release.set()
        executor.shutdown(wait=True, cancel_futures=True)
        connection.close()
