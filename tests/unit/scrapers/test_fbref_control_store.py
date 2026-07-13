import inspect
import uuid
from datetime import datetime, timezone

import pytest

from scrapers.fbref.control import (
    BudgetExceeded,
    CohortTarget,
    ControlStore,
    ControlStoreConfigError,
    StateConflict,
    TargetLease,
    make_budget_reservation_id,
    make_control_run_id,
    make_logical_refresh_id,
    resolve_control_db_uri,
)
from scrapers.fbref.control.migrations import MIGRATIONS


class FakeCursor:
    def __init__(self, handler):
        self.handler = handler
        self.rows = []
        self.rowcount = 0
        self.description = None
        self.executions = []
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        self.rows, self.rowcount = self.handler(normalized, params)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, handler):
        self.fake_cursor = FakeCursor(handler)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, **_kwargs):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class FakeFactory:
    def __init__(self, handler):
        self.handler = handler
        self.connections = []

    def __call__(self, _dsn):
        connection = FakeConnection(self.handler)
        self.connections.append(connection)
        return connection


def test_control_uri_prefers_explicit_and_normalizes_airflow_driver():
    assert resolve_control_db_uri(
        {
            "FBREF_CONTROL_DB_URI": "postgres://fbref:pw@db/fbref",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///ignored.db",
        }
    ) == "postgresql://fbref:pw@db/fbref"
    assert resolve_control_db_uri(
        {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
                "postgresql+psycopg2://airflow:pw@postgres/airflow"
            )
        }
    ) == "postgresql://airflow:pw@postgres/airflow"
    with pytest.raises(ControlStoreConfigError, match="PostgreSQL"):
        ControlStore("sqlite:///airflow.db")


def test_airflow_and_attempt_ids_map_to_stable_uuids():
    first = make_control_run_id("scheduled__2026-07-11", dag_id="fbref")
    second = make_control_run_id("scheduled__2026-07-11", dag_id="fbref")
    assert first == second
    assert uuid.UUID(first).version == 5
    refresh = make_logical_refresh_id(first, "fbref:match:a071faa8")
    reservation = make_budget_reservation_id(refresh)
    assert uuid.UUID(refresh).version == 5
    assert uuid.UUID(reservation).version == 5


def test_migrations_are_advisory_locked_versioned_and_idempotent():
    installed = {}

    def handler(sql, params):
        if "SELECT version, name, checksum" in sql:
            return list(installed.values()), len(installed)
        if "INSERT INTO fbref_control.schema_migration" in sql:
            version, name, checksum = params
            installed[version] = {
                "version": version,
                "name": name,
                "checksum": checksum,
            }
            return [], 1
        return [], 0

    factory = FakeFactory(handler)
    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=factory,
    )
    assert store.migrate() == tuple(migration.version for migration in MIGRATIONS)
    assert store.migrate() == ()
    statements = [
        sql
        for connection in factory.connections
        for sql, _ in connection.fake_cursor.executions
    ]
    assert any("pg_advisory_xact_lock" in sql for sql in statements)
    assert all(connection.committed for connection in factory.connections)
    assert len(installed) == len(MIGRATIONS)


def test_schema_contains_all_control_entities_and_fencing_constraints():
    ddl = "\n".join(
        statement
        for migration in MIGRATIONS
        for statement in migration.statements
    ).lower()
    for table in (
        "crawl_run",
        "budget_reservation",
        "registry_snapshot",
        "competition_registry",
        "season_registry",
        "page_frontier",
        "run_target",
        "fetch_attempt",
        "dataset_manifest",
        "observation_processing",
        "clearance_session",
        "domain_throttle",
    ):
        assert f"fbref_control.{table}" in ddl
    assert "canonical_url text not null unique" in ddl
    assert "claim_token uuid" in ddl
    assert "lease_epoch" in ddl
    assert "provider_billed_bytes bigint" in ddl
    assert "http_request_count bigint" in ddl
    assert "http_status_history integer[]" in ddl
    assert "browser_bootstrap_attempts bigint" in ddl
    assert "browser_unobserved_bytes bigint" in ddl
    assert "stateful_parser_version text" in ddl
    assert "unique (run_id, logical_refresh_id)" not in ddl
    claim_source = inspect.getsource(ControlStore.claim_targets)
    assert "SKIP LOCKED" in claim_source
    assert "claim_token" in claim_source


def test_backfill_registry_selection_is_bounded_and_skips_completed_once():
    source = inspect.getsource(ControlStore.list_backfill_seasons)
    assert "LEFT JOIN fbref_control.page_frontier" in source
    assert "historical_once" in source
    assert "frontier.state = 'fetched'" in source
    assert "frontier.next_fetch_at IS NULL" in source
    assert "LIMIT %s" in source

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(lambda *_: ([], 0)),
    )
    with pytest.raises(ValueError, match="between 1 and 25"):
        store.list_backfill_seasons(limit=26)


def test_budget_is_atomic_per_attempt_and_allows_retry_of_logical_refresh():
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    attempts = [str(uuid.uuid4()), str(uuid.uuid4())]
    run = {
        "run_id": run_id,
        "status": "running",
        "request_limit": 2,
        "byte_limit": 200,
        "requests_reserved": 0,
        "bytes_reserved": 0,
        "requests_used": 0,
        "bytes_used": 0,
    }
    reservations = {}
    lock_events = []

    def handler(sql, params):
        if "FROM fbref_control.crawl_run" in sql and "FOR UPDATE" in sql:
            lock_events.append("run")
            return [dict(run)], 1
        if (
            "FROM fbref_control.budget_reservation" in sql
            and "reservation_id = %s" in sql
        ):
            if "FOR UPDATE" in sql:
                lock_events.append("reservation")
            row = reservations.get(params[0])
            return ([] if row is None else [dict(row)]), int(row is not None)
        if "INSERT INTO fbref_control.budget_reservation" in sql:
            reservation_id, selected_run, refresh, requests, bytes_ = params
            row = {
                "reservation_id": reservation_id,
                "run_id": selected_run,
                "logical_refresh_id": refresh,
                "requests_reserved": requests,
                "bytes_reserved": bytes_,
                "requests_used": None,
                "bytes_used": None,
                "status": "reserved",
            }
            reservations[reservation_id] = row
            return [dict(row)], 1
        if "SET requests_reserved = requests_reserved +" in sql:
            requests, bytes_, _ = params
            run["requests_reserved"] += requests
            run["bytes_reserved"] += bytes_
            return [], 1
        if "SET status = 'settled'" in sql:
            used_requests, used_bytes, reservation_id = params
            row = reservations[reservation_id]
            row.update(
                status="settled",
                requests_used=used_requests,
                bytes_used=used_bytes,
            )
            return [dict(row)], 1
        if "SET requests_reserved = requests_reserved -" in sql:
            reserved_requests, reserved_bytes, used_requests, used_bytes, *_ = params
            run["requests_reserved"] -= reserved_requests
            run["bytes_reserved"] -= reserved_bytes
            run["requests_used"] += used_requests
            run["bytes_used"] += used_bytes
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    first = store.reserve_budget(
        run_id,
        refresh_id,
        bytes_=100,
        attempt_id=attempts[0],
    )
    lock_events.clear()
    store.settle_budget(first.reservation_id, requests_used=1, bytes_used=80)
    assert lock_events == ["run", "reservation"]
    second = store.reserve_budget(
        run_id,
        refresh_id,
        bytes_=100,
        attempt_id=attempts[1],
    )
    duplicate = store.reserve_budget(
        run_id,
        refresh_id,
        bytes_=100,
        attempt_id=attempts[1],
    )

    assert first.reservation_id != second.reservation_id
    assert duplicate == second
    assert run["requests_used"] == 1
    assert run["requests_reserved"] == 1
    with pytest.raises(BudgetExceeded):
        store.reserve_budget(
            run_id,
            refresh_id,
            bytes_=1,
            attempt_id=str(uuid.uuid4()),
        )


def test_claim_is_bounded_and_returns_uuid_fence():
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    expiry = datetime(2026, 7, 11, 12, tzinfo=timezone.utc)

    def handler(sql, _params):
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            assert "FOR UPDATE" in sql
            return [{"status": "running"}], 1
        if "SELECT DISTINCT lease_run_id AS run_id" in sql:
            return [], 0
        if "SELECT target_id, claim_token, lease_epoch" in sql:
            return [], 0
        if "SELECT reservation.*" in sql:
            return [], 0
        if "UPDATE fbref_control.fetch_attempt AS attempt" in sql:
            return [], 0
        if "UPDATE fbref_control.run_target AS target" in sql:
            return [], 0
        if "UPDATE fbref_control.page_frontier" in sql and "lease_expires_at <=" in sql:
            return [], 0
        if "SELECT target.target_id" in sql:
            return [
                {
                    "target_id": "fbref:match:a071faa8",
                    "logical_refresh_id": refresh_id,
                    "canonical_url": "https://fbref.com/en/matches/a071faa8",
                    "page_kind": "match",
                    "source_ids": {"match_id": "a071faa8"},
                    "lease_epoch": 4,
                }
            ], 1
        if "RETURNING lease_expires_at" in sql:
            return [{"lease_expires_at": expiry}], 1
        if "UPDATE fbref_control.run_target" in sql:
            return [], 1
        if "COALESCE(max(attempt_number)" in sql:
            return [{"number": 2}], 1
        if "INSERT INTO fbref_control.fetch_attempt" in sql:
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    lease = store.claim_targets(run_id, "worker-1", limit=1)[0]
    assert lease.target_id == "fbref:match:a071faa8"
    assert lease.lease_epoch == 5
    assert lease.attempt_number == 2
    assert uuid.UUID(lease.claim_token).version == 4
    assert lease.lease_expires_at == expiry
    with pytest.raises(ValueError, match="between 1 and 25"):
        store.claim_targets(run_id, "worker-1", limit=26)


def test_due_cohort_uses_fifo_age_before_priority():
    selected = {}

    def handler(sql, _params):
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            selected["run_sql"] = sql
            return [{"status": "running"}], 1
        if "SELECT frontier.target_id" in sql:
            selected["sql"] = sql
            return [], 0
        if "SELECT COALESCE(max(ordinal)" in sql:
            return [{"next_ordinal": 0}], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    assert store.create_due_run_cohort(str(uuid.uuid4()), limit=5) == []
    assert "FOR UPDATE" in selected["run_sql"]
    sql = selected["sql"]
    ordering = sql[sql.rindex("ORDER BY CASE") :]
    assert ordering.index("frontier.created_at") < ordering.index(
        "frontier.priority DESC"
    )
    assert ordering.index("frontier.last_fetched_at IS NOT NULL") < ordering.index(
        "frontier.created_at"
    )
    assert "control_lane_rank <= %s" in sql
    assert "'competition_index', 'competition', 'season', 'schedule'" in sql
    assert "competition.gender = 'male'" in sql
    assert "competition.crawl_state = 'active'" in sql
    assert "season.is_current" in sql


def test_claim_rechecks_registry_scope_before_any_network_lease():
    source = inspect.getsource(ControlStore.claim_targets)

    assert "competition.gender = 'male'" in source
    assert "competition.lifecycle_state = 'present'" in source
    assert "competition.present" in source
    assert "season.lifecycle_state = 'present'" in source
    assert "season.present" in source
    assert "season.is_current" in source
    assert "frontier.refresh_policy = 'historical_once'" in source


def test_explicit_cohort_cannot_steal_target_from_active_run_or_canary():
    first_run = str(uuid.uuid4())
    canary_run = str(uuid.uuid4())
    target_id = "fbref:competition_index:all"
    memberships = {}

    def handler(sql, params):
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            return [{"status": "running"}], 1
        if "SELECT state FROM fbref_control.page_frontier" in sql:
            return [{"state": "queued"}], 1
        if "SELECT logical_refresh_id, ordinal" in sql:
            row = memberships.get((params[0], params[1]))
            return ([] if row is None else [dict(row)]), int(row is not None)
        if "SELECT outstanding.run_id" in sql:
            rows = [
                {"run_id": run_id}
                for (run_id, installed_target), _ in memberships.items()
                if installed_target == params[0] and run_id != params[1]
            ]
            return rows[:1], int(bool(rows))
        if "INSERT INTO fbref_control.run_target" in sql:
            run_id, installed_target, refresh, ordinal = params
            memberships[(run_id, installed_target)] = {
                "logical_refresh_id": refresh,
                "ordinal": ordinal,
            }
            return [], 1
        if "UPDATE fbref_control.page_frontier" in sql:
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    first = CohortTarget(target_id, str(uuid.uuid4()), 0)
    canary = CohortTarget(target_id, str(uuid.uuid4()), 0)

    assert store.create_run_cohort(first_run, [first]) == 1
    assert store.create_run_cohort(first_run, [first]) == 0
    with pytest.raises(StateConflict, match="already belongs to active run"):
        store.create_run_cohort(canary_run, [canary])

    assert list(memberships) == [(first_run, target_id)]


def test_recurring_frontier_policy_dominates_one_shot_and_requeues_upgrade():
    source = inspect.getsource(ControlStore.upsert_frontier_target)

    assert "refresh_policy NOT IN" in source
    assert "'historical_once', 'current_completed_once'" in source
    assert "THEN fbref_control.page_frontier.refresh_policy" in source
    assert "page_kind = 'match'" in source
    assert "EXCLUDED.refresh_policy = 'current_completed_once'" in source
    assert "page_kind = 'season'" in source
    assert "EXCLUDED.refresh_policy = 'historical_once'" in source
    assert "THEN clock_timestamp()" in source


def test_registry_transitions_close_out_of_scope_frontier_without_deletion():
    competition_source = inspect.getsource(ControlStore.reconcile_competitions)
    season_source = inspect.getsource(ControlStore.reconcile_seasons)
    summary_source = inspect.getsource(ControlStore.get_run_summary)

    assert "frontier_scope_closed" in competition_source
    assert "THEN 'quarantined'" in competition_source
    assert "ELSE 'skipped'" in competition_source
    assert "frontier.state <> 'leased'" in competition_source
    assert "frontier_scope_closed" in season_source
    assert "NOT season.is_current" in season_source
    assert "frontier.refresh_policy <> 'historical_once'" in season_source
    assert "frontier.state NOT IN" in summary_source
    assert "'skipped', 'quarantined', 'dead'" in summary_source


def test_registry_snapshot_rejects_metadata_change_for_same_identity():
    snapshot_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    fetched_at = datetime(2026, 7, 11, 12, tzinfo=timezone.utc)

    def handler(sql, _params):
        if "INSERT INTO fbref_control.registry_snapshot" in sql:
            return [], 0
        if "FROM fbref_control.registry_snapshot" in sql:
            return [{
                "run_id": run_id,
                "source": "fbref",
                "content_hash": "abc123",
                "successful": True,
                "fetched_at": fetched_at,
                "metadata": {"page_kind": "competition"},
            }], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    with pytest.raises(StateConflict, match="different evidence"):
        store.create_registry_snapshot(
            snapshot_id=snapshot_id,
            run_id=run_id,
            fetched_at=fetched_at,
            successful=True,
            content_hash="abc123",
            metadata={"page_kind": "competition_index"},
        )


def test_pipeline_candidates_require_per_observation_completion_fence():
    captured = {}

    def handler(sql, params):
        if "SELECT DISTINCT ON (target.ordinal)" in sql:
            captured["sql"] = sql
            captured["params"] = params
            return [], 0
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    store.list_replay_fetches(
        str(uuid.uuid4()),
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="discovery-v1",
        page_kinds=["schedule"],
    )

    assert "fbref_control.observation_processing" in captured["sql"]
    assert "observed.logical_refresh_id" in captured["sql"]
    assert "observed.status = 'succeeded'" in captured["sql"]
    assert "typed:__complete__" not in captured["sql"]
    assert captured["params"].count("typed-v3") == 3
    assert "discovery-v1" in captured["params"]


def test_parser_only_remediation_keeps_content_manifest_completion_key():
    captured = {}

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [], 0

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    store.list_replay_fetches(
        str(uuid.uuid4()),
        parser_version="remediation-v1",
        page_kinds=["match"],
    )

    assert "fbref_control.dataset_manifest" in captured["sql"]
    assert "manifest.dataset = '__page__'" in captured["sql"]


def test_latest_content_guard_holds_a_non_key_update_frontier_lock():
    refresh_id = str(uuid.uuid4())
    captured = []

    def handler(sql, params):
        captured.append((sql, params))
        if "FROM fbref_control.page_frontier" in sql:
            return [{"state": "fetched", "last_content_hash": "hash-1"}], 1
        if "FROM fbref_control.fetch_attempt" in sql:
            return [
                {
                    "logical_refresh_id": refresh_id,
                    "content_hash": "hash-1",
                }
            ], 1
        raise AssertionError(sql)

    factory = FakeFactory(handler)
    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=factory,
    )

    with store.guard_latest_content(
        "fbref:match:a071faa8", "hash-1", refresh_id
    ) as latest:
        assert latest is True
        assert factory.connections[0].committed is False

    assert any("FOR NO KEY UPDATE" in sql for sql, _ in captured)
    latest_sql = next(
        sql
        for sql, _ in captured
        if "FROM fbref_control.fetch_attempt" in sql
    )
    assert "ORDER BY lease_epoch DESC" in latest_sql
    assert all(params == ("fbref:match:a071faa8",) for _, params in captured)
    assert factory.connections[0].committed is True


def test_latest_content_guard_rejects_older_same_hash_observation():
    old_refresh = str(uuid.uuid4())
    latest_refresh = str(uuid.uuid4())

    def handler(sql, _params):
        if "FROM fbref_control.page_frontier" in sql:
            return [{"state": "fetched", "last_content_hash": "same"}], 1
        if "FROM fbref_control.fetch_attempt" in sql:
            return [
                {
                    "logical_refresh_id": latest_refresh,
                    "content_hash": "same",
                }
            ], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    with store.guard_latest_content(
        "fbref:match:a071faa8", "same", old_refresh
    ) as latest:
        assert latest is False


def test_latest_content_guard_rejects_old_a_after_a_b_a_replay():
    first_a_refresh = str(uuid.uuid4())
    latest_a_refresh = str(uuid.uuid4())

    def handler(sql, _params):
        if "FROM fbref_control.page_frontier" in sql:
            return [{"state": "fetched", "last_content_hash": "hash-a"}], 1
        if "FROM fbref_control.fetch_attempt" in sql:
            return [
                {
                    "logical_refresh_id": latest_a_refresh,
                    "content_hash": "hash-a",
                }
            ], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    with store.guard_latest_content(
        "fbref:match:a071faa8", "hash-a", first_a_refresh
    ) as latest:
        assert latest is False
    with store.guard_latest_content(
        "fbref:match:a071faa8", "hash-a", latest_a_refresh
    ) as latest:
        assert latest is True


def test_observation_claim_is_keyed_by_refresh_not_repeated_content_hash():
    refresh_id = str(uuid.uuid4())
    expiry = datetime(2026, 7, 11, 13, tzinfo=timezone.utc)
    state = {}

    def handler(sql, params):
        if "INSERT INTO fbref_control.observation_processing" in sql:
            state.setdefault(
                "row",
                {
                    "logical_refresh_id": refresh_id,
                    "parser_version": "page-v2",
                    "typed_parser_version": "typed-v3",
                    "stateful_parser_version": "discovery-v1",
                    "target_id": "fbref:match:a071faa8",
                    "content_hash": "same-hash",
                    "status": "pending",
                    "active_claim": False,
                },
            )
            return [], 1
        if "AS active_claim" in sql:
            return [dict(state["row"])], 1
        if "RETURNING lease_expires_at" in sql:
            state["row"].update(status="processing", active_claim=True)
            state["claim_token"] = params[0]
            return [{"lease_expires_at": expiry}], 1
        if "SET status = 'succeeded'" in sql:
            state["row"].update(status="succeeded", active_claim=False)
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    lease = store.claim_observation_processing(
        logical_refresh_id=refresh_id,
        target_id="fbref:match:a071faa8",
        content_hash="same-hash",
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="discovery-v1",
    )

    assert lease is not None
    assert lease.logical_refresh_id == refresh_id
    assert lease.content_hash == "same-hash"
    store.complete_observation_processing(
        lease, typed_status="succeeded", stateful_status="succeeded"
    )
    assert state["row"]["status"] == "succeeded"


def test_versioned_run_summary_uses_current_observation_and_parser_fences():
    executions = []
    run_id = str(uuid.uuid4())

    def handler(sql, params):
        executions.append((sql, params))
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "current"}], 1
        if "AS missing" in sql:
            return [{"count": 0}], 1
        return [], 0

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    summary = store.get_run_summary(
        run_id,
        parser_version="page-current",
        typed_parser_version="typed-current",
        stateful_parser_version="discovery-current",
    )

    assert summary is not None
    versioned_sql = "\n".join(
        sql for sql, _ in executions
        if "dataset_manifest AS manifest" in sql or "AS missing" in sql
    )
    assert "fbref_control.observation_processing" in versioned_sql
    assert "observed.logical_refresh_id" in versioned_sql
    assert "observed.status = 'succeeded'" in versioned_sql
    versioned_params = [
        value
        for _, params in executions
        for value in (params or ())
        if isinstance(value, str)
        and value in {
            "page-current", "typed-current", "discovery-current"
        }
    ]
    assert "page-current" in versioned_params
    assert "typed-current" in versioned_params
    assert "discovery-current" in versioned_params


def test_reaper_locks_frontier_before_conservative_reservation_settlement():
    run_id = str(uuid.uuid4())
    target_id = "fbref:match:a071faa8"
    refresh_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())
    lease_epoch = 7
    reservation = {
        "reservation_id": str(uuid.uuid4()),
        "run_id": run_id,
        "logical_refresh_id": refresh_id,
        "requests_reserved": 22,
        "bytes_reserved": 6 * 1024 * 1024,
        "requests_used": None,
        "bytes_used": None,
        "status": "reserved",
    }
    run = {
        "requests_reserved": 22,
        "bytes_reserved": 6 * 1024 * 1024,
        "requests_used": 0,
        "bytes_used": 0,
    }
    stages = []

    def handler(sql, params):
        if "SELECT DISTINCT lease_run_id AS run_id" in sql:
            stages.append("runs_discovered")
            return [{"run_id": run_id}], 1
        if "SELECT run_id FROM fbref_control.crawl_run" in sql:
            assert stages == ["runs_discovered"]
            assert params == ([run_id],)
            stages.append("run_locked")
            return [{"run_id": run_id}], 1
        if "SELECT target_id, claim_token, lease_epoch" in sql:
            assert stages == ["runs_discovered", "run_locked"]
            assert params == ([run_id],)
            stages.append("frontier_locked")
            return [
                {
                    "target_id": target_id,
                    "claim_token": claim_token,
                    "lease_epoch": lease_epoch,
                    "lease_run_id": run_id,
                    "lease_refresh_id": refresh_id,
                }
            ], 1
        if "SELECT reservation.*" in sql:
            assert stages == [
                "runs_discovered",
                "run_locked",
                "frontier_locked",
            ]
            assert params == (run_id, refresh_id)
            stages.append("reservation_locked")
            return [dict(reservation)], 1
        if "UPDATE fbref_control.budget_reservation" in sql:
            reservation.update(
                status="settled",
                requests_used=reservation["requests_reserved"],
                bytes_used=reservation["bytes_reserved"],
            )
            return [], 1
        if "SET requests_reserved = requests_reserved -" in sql:
            run["requests_reserved"] = 0
            run["bytes_reserved"] = 0
            run["requests_used"] = 22
            run["bytes_used"] = 6 * 1024 * 1024
            return [], 1
        if "UPDATE fbref_control.fetch_attempt" in sql:
            assert params == (
                run_id,
                target_id,
                refresh_id,
                claim_token,
                lease_epoch,
            )
            return [], 1
        if "UPDATE fbref_control.run_target" in sql:
            assert params == (run_id, target_id, refresh_id)
            return [], 1
        if "UPDATE fbref_control.page_frontier" in sql:
            assert params == (
                target_id,
                claim_token,
                lease_epoch,
                run_id,
                refresh_id,
            )
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    assert store.reap_expired_leases() == 1
    assert reservation["status"] == "settled"
    assert reservation["requests_used"] == 22
    assert reservation["bytes_used"] == 6 * 1024 * 1024
    assert stages == [
        "runs_discovered",
        "run_locked",
        "frontier_locked",
        "reservation_locked",
    ]
    assert run == {
        "requests_reserved": 0,
        "bytes_reserved": 0,
        "requests_used": 22,
        "bytes_used": 6 * 1024 * 1024,
    }


def test_abort_run_is_idempotent_settles_budget_and_releases_targets():
    run_id = str(uuid.uuid4())
    state = {"status": "running"}
    reservation = {
        "reservation_id": str(uuid.uuid4()),
        "run_id": run_id,
        "logical_refresh_id": str(uuid.uuid4()),
        "requests_reserved": 22,
        "bytes_reserved": 6 * 1024 * 1024,
        "requests_used": None,
        "bytes_used": None,
        "status": "reserved",
    }
    stages = []
    lock_orders = []

    def handler(sql, _params):
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            stages.clear()
            stages.append("run_locked")
            return [dict(state)], 1
        if "SELECT target_id FROM fbref_control.page_frontier" in sql:
            assert stages == ["run_locked"]
            stages.append("frontier_locked")
            rows = (
                [{"target_id": "fbref:match:a071faa8"}]
                if state["status"] == "running"
                else []
            )
            return rows, len(rows)
        if "SELECT * FROM fbref_control.budget_reservation" in sql:
            assert stages == ["run_locked", "frontier_locked"]
            stages.append("reservation_locked")
            lock_orders.append(tuple(stages))
            rows = [dict(reservation)] if reservation["status"] == "reserved" else []
            return rows, len(rows)
        if "UPDATE fbref_control.budget_reservation" in sql:
            reservation.update(
                status="settled",
                requests_used=reservation["requests_reserved"],
                bytes_used=reservation["bytes_reserved"],
            )
            return [], 1
        if "SET requests_reserved = requests_reserved -" in sql:
            return [], 1
        if "UPDATE fbref_control.fetch_attempt" in sql:
            return [], int(state["status"] == "running")
        if "UPDATE fbref_control.run_target" in sql:
            return [], 2 if state["status"] == "running" else 0
        if "UPDATE fbref_control.page_frontier" in sql:
            return [], int(state["status"] == "running")
        if "UPDATE fbref_control.clearance_session" in sql:
            return [], int(state["status"] == "running")
        if "SET status = 'failed'" in sql:
            changed = state["status"] == "running"
            state["status"] = "failed"
            return [], int(changed)
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    first = store.abort_run(
        run_id,
        error_class="AirflowDagFailure",
        error_message="terminal task failure",
    )
    second = store.abort_run(run_id)

    assert first == {
        "run_id": run_id,
        "status": "failed",
        "aborted": True,
        "reservations_settled": 1,
        "attempts_failed": 1,
        "targets_failed": 2,
        "targets_released": 1,
        "sessions_closed": 1,
    }
    assert second["status"] == "failed"
    assert second["reservations_settled"] == 0
    assert second["targets_released"] == 0
    assert reservation["status"] == "settled"
    assert state["status"] == "failed"
    assert lock_orders == [
        ("run_locked", "frontier_locked", "reservation_locked"),
        ("run_locked", "frontier_locked", "reservation_locked"),
    ]


def test_bind_reservation_uses_terminal_lock_order():
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    reservation_id = str(uuid.uuid4())
    lease = TargetLease(
        attempt_id=str(uuid.uuid4()),
        run_id=run_id,
        target_id="fbref:match:a071faa8",
        logical_refresh_id=refresh_id,
        canonical_url="https://fbref.com/en/matches/a071faa8",
        page_kind="match",
        source_ids={"match_id": "a071faa8"},
        claim_token=str(uuid.uuid4()),
        lease_epoch=3,
        attempt_number=1,
        leased_by="worker-1",
        lease_expires_at=datetime(2026, 7, 11, 13, tzinfo=timezone.utc),
    )
    stages = []

    def handler(sql, params):
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            assert params == (run_id,)
            stages.append("run_locked")
            return [{"status": "running"}], 1
        if "SELECT target_id FROM fbref_control.page_frontier" in sql:
            assert stages == ["run_locked"]
            stages.append("frontier_locked")
            return [{"target_id": lease.target_id}], 1
        if "FROM fbref_control.budget_reservation" in sql:
            assert stages == ["run_locked", "frontier_locked"]
            assert params == (reservation_id, run_id, refresh_id)
            stages.append("reservation_locked")
            return [{"logical_refresh_id": refresh_id}], 1
        if "UPDATE fbref_control.fetch_attempt" in sql:
            assert stages == [
                "run_locked",
                "frontier_locked",
                "reservation_locked",
            ]
            stages.append("attempt_locked")
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    store.bind_reservation(lease, reservation_id)

    assert stages == [
        "run_locked",
        "frontier_locked",
        "reservation_locked",
        "attempt_locked",
    ]


def test_abort_run_never_downgrades_a_succeeded_run():
    run_id = str(uuid.uuid4())
    executions = []

    def handler(sql, _params):
        executions.append(sql)
        if "SELECT status FROM fbref_control.crawl_run" in sql:
            return [{"status": "succeeded"}], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    result = store.abort_run(run_id)

    assert result["status"] == "succeeded"
    assert result["aborted"] is False
    assert len(executions) == 1


def test_raw_recovery_reattributes_network_metrics_to_reserved_source_attempt():
    run_id = str(uuid.uuid4())
    refresh_id = str(uuid.uuid4())
    source_attempt_id = str(uuid.uuid4())
    recovery_attempt_id = str(uuid.uuid4())
    lease = TargetLease(
        attempt_id=recovery_attempt_id,
        run_id=run_id,
        target_id="fbref:competition_index:all",
        logical_refresh_id=refresh_id,
        canonical_url="https://fbref.com/en/comps",
        page_kind="competition_index",
        source_ids={"competition_index": "all"},
        claim_token=str(uuid.uuid4()),
        lease_epoch=2,
        attempt_number=2,
        leased_by="recovery-worker",
        lease_expires_at=datetime(2026, 7, 11, 13, tzinfo=timezone.utc),
    )
    captured = {}

    def handler(sql, params):
        if "UPDATE fbref_control.page_frontier" in sql:
            return [{"target_id": lease.target_id}], 1
        if "UPDATE fbref_control.run_target" in sql:
            return [], 1
        if "SET status = 'succeeded'" in sql:
            captured["recovery"] = params
            return [], 1
        if "SET reservation_id = COALESCE" in sql:
            captured["source"] = params
            return [], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )
    store.complete_fetch(
        lease,
        http_status=200,
        content_hash="a" * 64,
        raw_manifest_key=f"raw/{refresh_id}.json",
        decoded_bytes=100,
        compressed_bytes=50,
        wire_bytes=123,
        provider_billed_bytes=200,
        latency_ms=10,
        transport_version="source-transport",
        session_version=str(uuid.uuid4()),
        recovered_from_attempt_id=source_attempt_id,
    )

    recovery = captured["recovery"]
    assert recovery[3:7] == (0, 0, 0, None)
    assert recovery[7:9] == (0, [])
    assert recovery[11:14] == ("raw-recovery", None, 0)
    source = captured["source"]
    assert source[0] == make_budget_reservation_id(source_attempt_id)
    assert source[1:8] == (
        200,
        "a" * 64,
        f"raw/{refresh_id}.json",
        100,
        50,
        123,
        200,
    )
    assert source[8:10] == (1, [200])
    summary_source = inspect.getsource(ControlStore.get_run_summary)
    assert "attempt.raw_manifest_key IS NOT NULL" in summary_source
    assert "sum(attempt.wire_bytes) FILTER" in summary_source
    assert "sum(attempt.http_request_count) FILTER" in summary_source
    assert "array_positions(" in summary_source
    assert "attempt.http_request_count - 1" in summary_source
    assert "500, 502, 503, 504" in summary_source


def test_a_failed_registry_snapshot_can_be_replaced_by_a_successful_retry():
    """A failed snapshot records what our parse did, not what the source said.
    Freezing it would poison the (parser, page) pair forever — the retry that
    fixes the parser could never record its result, and the target would stay
    stuck until the parser version changed."""
    snapshot_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    fetched_at = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
    updates = []

    def handler(sql, params):
        if "INSERT INTO fbref_control.registry_snapshot" in sql:
            return [], 0
        if "UPDATE fbref_control.registry_snapshot" in sql:
            updates.append(params)
            return [], 1
        if "FROM fbref_control.registry_snapshot" in sql:
            return [{
                "run_id": run_id,
                "source": "fbref",
                "content_hash": "abc123",
                "successful": False,
                "fetched_at": fetched_at,
                "metadata": {"page_kind": "competition"},
            }], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    store.create_registry_snapshot(
        snapshot_id=snapshot_id,
        run_id=run_id,
        fetched_at=fetched_at,
        successful=True,
        content_hash="abc123",
        metadata={"page_kind": "competition"},
    )

    assert len(updates) == 1
    assert True in updates[0]


def test_a_successful_registry_snapshot_stays_immutable():
    """Evidence about the source itself must never be rewritten."""
    snapshot_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    fetched_at = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)

    def handler(sql, _params):
        if "INSERT INTO fbref_control.registry_snapshot" in sql:
            return [], 0
        if "UPDATE fbref_control.registry_snapshot" in sql:
            raise AssertionError("a successful snapshot must not be updated")
        if "FROM fbref_control.registry_snapshot" in sql:
            return [{
                "run_id": run_id,
                "source": "fbref",
                "content_hash": "abc123",
                "successful": True,
                "fetched_at": fetched_at,
                "metadata": {"page_kind": "competition"},
            }], 1
        raise AssertionError(sql)

    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=FakeFactory(handler),
    )

    with pytest.raises(StateConflict, match="different evidence"):
        store.create_registry_snapshot(
            snapshot_id=snapshot_id,
            run_id=run_id,
            fetched_at=fetched_at,
            successful=False,
            content_hash="abc123",
            metadata={"page_kind": "competition"},
        )
