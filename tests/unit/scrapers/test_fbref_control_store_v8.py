import inspect
import json
import uuid
from datetime import datetime, timezone

import pytest

from scrapers.fbref.control import (
    CompetitionRegistryEntry,
    ControlStore,
    FrontierProvenance,
    FrontierTarget,
    SeasonRegistryEntry,
    StateConflict,
    TargetLease,
    make_frontier_provenance_id,
)
from scrapers.fbref.control.migrations import MIGRATIONS


class FakeCursor:
    def __init__(self, handler):
        self.handler = handler
        self.rows = []
        self.rowcount = 0
        self.description = None
        self.executions = []

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
        pass


class FakeConnection:
    def __init__(self, handler):
        self.fake_cursor = FakeCursor(handler)
        self.committed = False
        self.rolled_back = False

    def cursor(self, **_kwargs):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class FakeFactory:
    def __init__(self, handler):
        self.handler = handler
        self.connections = []

    def __call__(self, _dsn):
        connection = FakeConnection(self.handler)
        self.connections.append(connection)
        return connection


def make_store(handler):
    factory = FakeFactory(handler)
    return (
        ControlStore(
            "postgresql://airflow:pw@postgres/airflow",
            connection_factory=factory,
        ),
        factory,
    )


def competition_entry(index, *, gender="male"):
    return CompetitionRegistryEntry(
        competition_id=str(index),
        canonical_url=f"https://fbref.com/en/comps/{index}/Competition-{index}",
        name=f"Competition {index}",
        gender=gender,
        classification="domestic_league",
    )


def test_v8_is_append_only_schema_for_provenance_aliases_and_cancellation():
    assert tuple(migration.version for migration in MIGRATIONS) == tuple(
        range(1, 11)
    )
    migration = next(item for item in MIGRATIONS if item.version == 8)
    assert migration.version == 8
    ddl = "\n".join(migration.statements).lower()

    assert "fbref_control.frontier_provenance" in ddl
    assert "unique nulls not distinct" in ddl
    assert "carried_competition_id, carried_season_id" in ddl
    assert "before update or delete" in ddl
    assert "frontier provenance is append-only" in ddl
    assert "fbref_control.season_alias" in ddl
    assert "season_registry_one_current_idx" in ddl
    assert "where present and lifecycle_state = 'present' and is_current" in ddl
    dedupe = "control_migration_v8_current_dedupe"
    unique_index = "create unique index if not exists season_registry_one_current_idx"
    assert ddl.index(dedupe) < ddl.index(unique_index)
    assert "order by last_seen_at desc, first_seen_at desc" in ddl
    assert "season_id desc" in ddl
    assert "'reason', 'duplicate_present_current_season'" in ddl
    assert "set is_current = false" in ddl
    assert "delete from fbref_control.season_registry" not in ddl
    assert "registry_reconciliation_override" in ddl
    assert "registry override is append-only" in ddl
    assert "'claimed', 'succeeded', 'failed', 'expired', 'cancelled'" in ddl
    assert "fetch_attempt_unprocessed_raw_idx" in ddl
    assert "observation_processing_version_idx" in ddl


def test_v9_adds_singleton_expiring_publication_generation_lock():
    migration = next(item for item in MIGRATIONS if item.version == 9)
    ddl = "\n".join(migration.statements).lower()

    assert migration.name == "publication_generation_lock"
    assert "fbref_control.publication_lock" in ddl
    assert "source text primary key" in ddl
    assert "owner_run_id uuid not null" in ddl
    assert "references fbref_control.crawl_run" in ddl
    assert "expires_at timestamptz not null" in ddl
    assert "released_at timestamptz" in ddl
    assert "publication_lock_expiry_idx" in ddl


def test_v10_adds_typed_persistent_http_metering_evidence():
    migration = next(item for item in MIGRATIONS if item.version == 10)
    ddl = "\n".join(migration.statements).lower()

    assert migration.name == "persistent_http_metering"
    assert "clearance_session_page_accounting" in ddl
    assert "reservation_id uuid primary key" in ddl
    assert "attempt_id uuid not null unique" in ddl
    assert "provider_billed_bytes bigint not null" in ddl
    assert "evidence_sha256 text not null" in ddl
    assert "clearance_session_tail_reservation" in ddl
    assert "status in ('reserved', 'settled', 'aborted')" in ddl
    assert "authoritative_provider_bytes =" in ddl
    assert "page_provider_bytes + tail_provider_bytes" in ddl


def test_replay_pipeline_metrics_are_derived_and_atomically_anchored():
    run_id = str(uuid.uuid4())
    source_run_id = str(uuid.uuid4())
    updates = []

    def handler(sql, params):
        if sql.startswith(
            "SELECT status, run_type, metadata FROM fbref_control.crawl_run"
        ):
            return (
                [
                    {
                        "status": "running",
                        "run_type": "replay",
                        "metadata": {
                            "acceptance_replay": True,
                            "acceptance_replay_source_run_id": source_run_id,
                            "acceptance_trino_schema": "acceptance_seq",
                            "acceptance_persistence_mode": "sequential",
                        },
                    }
                ],
                1,
            )
        if sql.startswith("SELECT target.target_id"):
            assert params == (source_run_id,)
            return (
                [
                    {
                        "target_id": "fbref:match:m1",
                        "match_id": "m1",
                    },
                    {
                        "target_id": "fbref:match:m2",
                        "match_id": "m2",
                    },
                ],
                2,
            )
        if sql.startswith("UPDATE fbref_control.crawl_run"):
            updates.append(json.loads(params[0]))
            return ([], 1)
        raise AssertionError(sql)

    store, factory = make_store(handler)

    anchored = store.record_replay_pipeline_metrics(
        run_id,
        schema="acceptance_seq",
        mode="sequential",
        elapsed_seconds=12.5,
        statement_counts={"execute": 100, "execute_committing": 20},
    )

    assert anchored["control_run_id"] == run_id
    assert anchored["match_count"] == 2
    assert len(anchored["match_keys_sha256"]) == 64
    assert len(anchored["artifact_sha256"]) == 64
    stored = {key: value for key, value in anchored.items() if key != "idempotent"}
    assert updates == [{"pipeline_run_metrics": stored}]
    assert factory.connections[0].committed is True


def test_publication_lock_acquire_is_retry_idempotent_and_owner_fenced():
    owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    inserted = True

    def handler(sql, params):
        nonlocal inserted
        if sql.startswith("SELECT status FROM fbref_control.crawl_run"):
            return ([{"status": "running"}], 1)
        if sql.startswith("INSERT INTO fbref_control.publication_lock"):
            rowcount = int(inserted)
            inserted = False
            return ([], rowcount)
        if "FROM fbref_control.publication_lock" in sql and "FOR UPDATE" in sql:
            return (
                [
                    {
                        "source": "fbref",
                        "owner_run_id": owner,
                        "owner_dag_id": "dag_ingest_fbref",
                        "acquired_at": now,
                        "expires_at": now,
                        "released_at": None,
                        "active": True,
                    }
                ],
                1,
            )
        raise AssertionError(sql)

    store, _ = make_store(handler)
    first = store.acquire_publication_lock(
        owner, dag_id="dag_ingest_fbref"
    )
    retried = store.acquire_publication_lock(
        owner, dag_id="dag_ingest_fbref"
    )

    assert first["acquired"] is True
    assert first["idempotent"] is False
    assert retried["acquired"] is False
    assert retried["idempotent"] is True


def test_publication_lock_rejects_an_active_different_owner():
    contender = str(uuid.uuid4())
    active_owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    def handler(sql, params):
        if sql.startswith("SELECT status FROM fbref_control.crawl_run"):
            return ([{"status": "running"}], 1)
        if sql.startswith("INSERT INTO fbref_control.publication_lock"):
            return ([], 0)
        if "FROM fbref_control.publication_lock" in sql and "FOR UPDATE" in sql:
            return (
                [
                    {
                        "source": "fbref",
                        "owner_run_id": active_owner,
                        "owner_dag_id": "dag_ingest_fbref",
                        "acquired_at": now,
                        "expires_at": now,
                        "released_at": None,
                        "active": True,
                    }
                ],
                1,
            )
        raise AssertionError(sql)

    store, _ = make_store(handler)
    with pytest.raises(StateConflict, match="locked by another"):
        store.acquire_publication_lock(
            contender, dag_id="dag_backfill_fbref"
        )


def test_publication_lock_release_is_exact_owner_only():
    owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    def handler(sql, params):
        if sql.startswith("SELECT owner_run_id, released_at"):
            return ([{"owner_run_id": owner, "released_at": None}], 1)
        if sql.startswith("UPDATE fbref_control.publication_lock"):
            return ([{"released_at": now}], 1)
        raise AssertionError(sql)

    store, _ = make_store(handler)
    released = store.release_publication_lock(owner)

    assert released["released"] is True
    assert released["idempotent"] is False
    assert released["released_at"] == now


def test_publication_lock_renew_requires_the_active_exact_owner():
    owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    def handler(sql, params):
        if sql.startswith("SELECT owner_run_id, released_at"):
            return (
                [
                    {
                        "owner_run_id": owner,
                        "released_at": None,
                        "active": True,
                    }
                ],
                1,
            )
        if sql.startswith("UPDATE fbref_control.publication_lock"):
            return (
                [
                    {
                        "source": "fbref",
                        "owner_run_id": owner,
                        "owner_dag_id": "dag_ingest_fbref",
                        "acquired_at": now,
                        "expires_at": now,
                        "released_at": None,
                    }
                ],
                1,
            )
        raise AssertionError(sql)

    store, _ = make_store(handler)
    renewed = store.renew_publication_lock(owner)

    assert renewed["owner_run_id"] == owner


def test_publication_writer_assertion_requires_active_exact_owner():
    owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    captured = {}

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return (
            [
                {
                    "source": "fbref",
                    "owner_run_id": uuid.UUID(owner),
                    "owner_dag_id": "dag_ingest_fbref",
                    "acquired_at": now,
                    "expires_at": now,
                    "released_at": None,
                    "active": True,
                }
            ],
            1,
        )

    store, _ = make_store(handler)
    lock = store.assert_publication_lock_owner(owner)

    assert lock["owner_run_id"] == owner
    assert lock["active"] is True
    assert captured["params"] == ("fbref",)
    assert "expires_at > clock_timestamp()" in captured["sql"]
    assert "released_at IS NULL" in captured["sql"]


def test_publication_guard_holds_row_fence_through_external_write():
    owner = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    captured = {}

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return (
            [
                {
                    "source": "fbref",
                    "owner_run_id": uuid.UUID(owner),
                    "owner_dag_id": "dag_ingest_fbref",
                    "acquired_at": now,
                    "expires_at": now,
                    "released_at": None,
                    "active": True,
                }
            ],
            1,
        )

    store, factory = make_store(handler)
    external_write_completed = False
    with store.guard_publication_lock(owner) as lock:
        assert lock["owner_run_id"] == owner
        assert factory.connections[0].committed is False
        external_write_completed = True

    assert external_write_completed is True
    assert factory.connections[0].committed is True
    assert captured["params"] == ("fbref",)
    assert "FOR UPDATE" in captured["sql"]
    assert "expires_at > clock_timestamp()" in captured["sql"]


@pytest.mark.parametrize(
    ("lock", "run_id"),
    [
        (None, str(uuid.uuid4())),
        (
            {
                "owner_run_id": uuid.uuid4(),
                "released_at": None,
                "active": True,
            },
            str(uuid.uuid4()),
        ),
        (
            {
                "owner_run_id": uuid.uuid4(),
                "released_at": datetime.now(timezone.utc),
                "active": False,
            },
            None,
        ),
        (
            {
                "owner_run_id": uuid.uuid4(),
                "released_at": None,
                "active": False,
            },
            None,
        ),
    ],
)
def test_publication_writer_assertion_fails_closed(lock, run_id):
    owner = str(lock["owner_run_id"]) if run_id is None else run_id

    def handler(_sql, _params):
        return ([] if lock is None else [lock]), int(lock is not None)

    store, _ = make_store(handler)
    with pytest.raises(StateConflict, match="not owned"):
        store.assert_publication_lock_owner(owner)


def test_publication_scope_exports_aliases_and_fail_closed_male_eligibility():
    captured = {}

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "source_competition_id": "9",
                "source_season_id": "2425",
                "canonical_season_id": "2024-2025",
                "scope_kind": "alias",
                "eligible_male": True,
            }
        ], 1

    store, _ = make_store(handler)

    rows = store.list_publication_scope()

    assert rows[0]["canonical_season_id"] == "2024-2025"
    sql = captured["sql"]
    assert "fbref_control.season_alias" in sql
    assert "competition.gender = 'male'" in sql
    assert "competition.lifecycle_state IN ( 'present', 'missing_once' )" in sql
    assert "season.lifecycle_state = 'present'" in sql
    assert "AS eligible_male" in sql
    assert captured["params"] == ("fbref",)


def test_provenance_identity_preserves_many_scopes_for_same_edge():
    installed = {}
    executed = []

    def handler(sql, params):
        executed.append(sql)
        if "INSERT INTO fbref_control.frontier_provenance" in sql:
            key = (
                params[1],
                params[2],
                params[3],
                params[4],
                params[5],
                params[6],
                params[7],
            )
            installed.setdefault(
                key,
                {
                    "provenance_id": params[0],
                    "carried_competition_id": params[4],
                    "carried_season_id": params[5],
                    "logical_refresh_id": params[8],
                    "metadata": json.loads(params[9]),
                },
            )
            return [], 1
        if "SELECT provenance_id, carried_competition_id" in sql:
            return [dict(installed[tuple(params)])], 1
        raise AssertionError(sql)

    store, _ = make_store(handler)
    refresh_id = str(uuid.uuid4())
    common = {
        "parent_target_id": "fbref:player:global-parent",
        "child_target_id": "fbref:player:global-child",
        "relation": "page_link:player",
        "parent_content_hash": "a" * 64,
        "parser_version": "discovery-v8",
        "logical_refresh_id": refresh_id,
        "metadata": {"child_page_kind": "player"},
    }
    first = store.record_frontier_provenance(
        FrontierProvenance(
            **common,
            carried_competition_id="9",
            carried_season_id="2025-2026",
        )
    )
    second = store.record_frontier_provenance(
        FrontierProvenance(
            **common,
            carried_competition_id="12",
            carried_season_id="2025-2026",
        )
    )
    repeated = store.record_frontier_provenance(
        FrontierProvenance(
            **{
                **common,
                "logical_refresh_id": str(uuid.uuid4()),
            },
            carried_competition_id="9",
            carried_season_id="2025-2026",
        )
    )

    assert first != second
    assert repeated == first
    assert len(installed) == 2
    assert any(
        "carried_competition_id, carried_season_id, parent_content_hash"
        in sql
        for sql in executed
    )
    assert any("IS NOT DISTINCT FROM" in sql for sql in executed)
    assert first == make_frontier_provenance_id(
        parent_target_id=common["parent_target_id"],
        child_target_id=common["child_target_id"],
        relation=common["relation"],
        carried_competition_id="9",
        carried_season_id="2025-2026",
        parent_content_hash=common["parent_content_hash"],
        parser_version=common["parser_version"],
    )


def test_frontier_discovery_batch_reuses_one_transaction_and_is_idempotent():
    installed_targets = {}
    installed_edges = {}

    def handler(sql, params):
        if "FROM fbref_control.page_frontier" in sql and "FOR UPDATE" in sql:
            target_id, canonical_url = params
            rows = [
                dict(row)
                for row in installed_targets.values()
                if row["target_id"] == target_id
                or row["canonical_url"] == canonical_url
            ]
            return rows, len(rows)
        if "INSERT INTO fbref_control.page_frontier" in sql:
            installed_targets[params[0]] = {
                "target_id": params[0],
                "source": params[1],
                "page_kind": params[2],
                "canonical_url": params[3],
                "source_ids": json.loads(params[4]),
                "state": "queued",
            }
            return [], 1
        if "INSERT INTO fbref_control.frontier_provenance" in sql:
            key = tuple(params[1:8])
            installed_edges.setdefault(key, {
                "provenance_id": params[0],
                "carried_competition_id": params[4],
                "carried_season_id": params[5],
                "logical_refresh_id": params[8],
                "metadata": json.loads(params[9]),
            })
            return [], 1
        if "SELECT provenance_id, carried_competition_id" in sql:
            return [dict(installed_edges[tuple(params)])], 1
        raise AssertionError(sql)

    store, factory = make_store(handler)
    refresh_id = str(uuid.uuid4())
    targets = [
        FrontierTarget(
            target_id=f"fbref:player:{index:08d}",
            page_kind="player",
            canonical_url=f"https://fbref.com/en/players/{index:08d}/x",
            source_ids={"player_id": f"{index:08d}"},
            refresh_policy="monthly",
        )
        for index in reversed(range(50))
    ]
    edges = [
        FrontierProvenance(
            parent_target_id="fbref:match:parent",
            child_target_id=target.target_id,
            relation="page_link:player",
            carried_competition_id="9",
            carried_season_id="2025-2026",
            parent_content_hash="a" * 64,
            parser_version="discovery-v8",
            logical_refresh_id=refresh_id,
            metadata={"child_page_kind": "player"},
        )
        for target in reversed(targets)
    ]

    first = store.upsert_frontier_discovery_batch(
        targets=targets, provenance=edges
    )
    repeated = store.upsert_frontier_discovery_batch(
        targets=targets, provenance=edges
    )

    assert first == repeated == {"target_count": 50, "provenance_count": 50}
    assert len(factory.connections) == 2
    assert all(connection.committed for connection in factory.connections)
    assert len(installed_targets) == 50
    assert len(installed_edges) == 50
    for connection in factory.connections:
        executions = connection.fake_cursor.executions
        assert len(executions) == 200
        target_ids = [
            params[0]
            for sql, params in executions
            if "INSERT INTO fbref_control.page_frontier" in sql
        ]
        provenance_ids = [
            params[0]
            for sql, params in executions
            if "INSERT INTO fbref_control.frontier_provenance" in sql
        ]
        assert target_ids == sorted(target_ids)
        assert provenance_ids == sorted(provenance_ids)


def test_frontier_discovery_batch_rolls_back_a_late_identity_conflict():
    inserted = []

    def handler(sql, params):
        if "FROM fbref_control.page_frontier" in sql and "FOR UPDATE" in sql:
            if params[0] == "fbref:player:b":
                return [{
                    "target_id": "fbref:player:someone-else",
                    "source": "fbref",
                    "page_kind": "player",
                    "canonical_url": params[1],
                    "source_ids": {"player_id": "someone-else"},
                    "state": "queued",
                }], 1
            return [], 0
        if "INSERT INTO fbref_control.page_frontier" in sql:
            inserted.append(params[0])
            return [], 1
        raise AssertionError(sql)

    store, factory = make_store(handler)
    targets = [
        FrontierTarget(
            target_id=f"fbref:player:{suffix}",
            page_kind="player",
            canonical_url=f"https://fbref.com/en/players/{suffix}/x",
            source_ids={"player_id": suffix},
            refresh_policy="monthly",
        )
        for suffix in ("b", "a")
    ]

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_discovery_batch(targets=targets, provenance=[])

    assert inserted == ["fbref:player:a"]
    assert len(factory.connections) == 1
    assert factory.connections[0].rolled_back is True
    assert factory.connections[0].committed is False


def test_frontier_discovery_batch_rejects_unbounded_input_before_connecting():
    store, factory = make_store(lambda sql, params: ([], 0))
    target = FrontierTarget(
        target_id="fbref:player:a",
        page_kind="player",
        canonical_url="https://fbref.com/en/players/a/x",
        source_ids={"player_id": "a"},
        refresh_policy="monthly",
    )

    with pytest.raises(ValueError, match="target batch exceeds 1000"):
        store.upsert_frontier_discovery_batch(
            targets=[target] * 1001,
            provenance=[],
        )

    assert factory.connections == []


def test_lane_unprocessed_raw_includes_failed_source_runs_oldest_first():
    captured = {}
    raw = {
        "attempt_id": str(uuid.uuid4()),
        "run_id": str(uuid.uuid4()),
        "source_run_status": "failed",
        "source_run_type": "current",
        "target_id": "fbref:match:a071faa8",
        "logical_refresh_id": str(uuid.uuid4()),
        "content_hash": "b" * 64,
        "raw_manifest_key": "raw/fbref/example.json.zst",
        "page_kind": "match",
    }

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [raw], 1

    store, _ = make_store(handler)
    result = store.list_unprocessed_fetches(
        run_type="current",
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="stateful-v4",
        page_kinds=["match"],
        limit=10,
    )

    assert result == [raw]
    sql = captured["sql"]
    assert "source_run.status AS source_run_status" in sql
    assert "source_run.run_type AS source_run_type" in sql
    assert "source_run.status =" not in sql
    assert "source_run.run_type = %s" in sql
    assert "observed.parser_version = %s" in sql
    assert "observed.typed_parser_version = %s" in sql
    assert "observed.stateful_parser_version = %s" in sql
    assert "ORDER BY COALESCE( attempt.finished_at, attempt.started_at )" in sql
    assert captured["params"] == (
        "fbref",
        "current",
        ["match"],
        ["match"],
        "page-v2",
        "typed-v3",
        "stateful-v4",
        "page-v2",
        "typed-v3",
        "stateful-v4",
        "page-v2",
        "typed-v3",
        "stateful-v4",
        10,
    )


@pytest.mark.parametrize("run_type", ["replay", "publication", "unknown", ""])
def test_unprocessed_raw_recovery_rejects_unknown_lane_before_connecting(
    run_type,
):
    store, factory = make_store(lambda sql, params: ([], 0))

    with pytest.raises(ValueError, match="run_type must be current or backfill"):
        store.list_unprocessed_fetches(
            run_type=run_type,
            parser_version="page-v2",
            typed_parser_version="typed-v3",
            stateful_parser_version="stateful-v4",
        )

    assert factory.connections == []


def test_run_summary_rejects_unknown_recovery_lane():
    run_id = str(uuid.uuid4())

    def handler(sql, params):
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "manual"}], 1
        return [], 0

    store, factory = make_store(handler)

    with pytest.raises(
        StateConflict,
        match="crawl run_type must be current, backfill, replay, or publication",
    ):
        store.get_run_summary(run_id)

    assert factory.connections[0].rolled_back is True


def test_publication_summary_has_no_recovery_lane_but_remains_queryable():
    run_id = str(uuid.uuid4())
    raw_query = {}

    def handler(sql, params):
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "publication"}], 1
        if ") AS missing" in sql:
            return [{"count": 0}], 1
        if "AS global_sla_overdue_count" in sql:
            raw_query["params"] = params
            return [{
                "page_kind": "match",
                "run_count": 0,
                "lane_count": 0,
                "lane_sla_overdue_count": 0,
                "global_count": 2,
                "global_sla_overdue_count": 1,
                "run_oldest_raw_at": None,
                "lane_oldest_raw_at": None,
                "global_oldest_raw_at": datetime(
                    2026, 7, 14, 12, tzinfo=timezone.utc
                ),
            }], 1
        return [], 0

    store, _ = make_store(handler)

    summary = store.get_run_summary(run_id)

    assert summary["recovery_lane_run_type"] is None
    assert summary["lane_unprocessed_raw_count"] == 0
    assert summary["lane_unprocessed_raw_sla_overdue_count"] == 0
    assert summary["global_unprocessed_raw_count"] == 2
    assert summary["global_unprocessed_raw_sla_overdue_count"] == 1
    assert summary["unprocessed_raw_sla_overdue_count"] == 1
    assert raw_query["params"][:7] == (
        run_id,
        None,
        None,
        86_400,
        86_400,
        run_id,
        None,
    )


def test_run_fetch_selection_can_exclude_quarantined_before_limit():
    captured = {}
    run_id = str(uuid.uuid4())

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [], 0

    store, _ = make_store(handler)
    assert store.list_run_fetches(
        run_id,
        page_kinds=["season"],
        include_quarantined=False,
        limit=3,
    ) == []

    assert "frontier.state <> 'quarantined'" in captured["sql"]
    assert captured["params"][-2:] == (False, 3)

    assert store.list_run_fetches(
        run_id, page_kinds=["season"], limit=3
    ) == []
    assert captured["params"][-2:] == (True, 3)


def test_unprocessed_fetches_skip_only_provably_superseded_failed_observation():
    captured = {}

    def handler(sql, params):
        captured["sql"] = " ".join(sql.split())
        captured["params"] = params
        return [], 0

    store, _ = make_store(handler)
    assert store.list_unprocessed_fetches(
        run_type="current",
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="stateful-v4",
        page_kinds=["match"],
        limit=100,
    ) == []

    sql = captured["sql"]
    assert "failed_observed.status = 'failed'" in sql
    assert "newer_attempt.target_id = attempt.target_id" in sql
    assert "newer_attempt.lease_epoch > attempt.lease_epoch" in sql
    assert "newer_attempt.status = 'succeeded'" in sql
    assert "newer_observed.status = 'succeeded'" in sql
    assert "newer_observed.generic_status = 'succeeded'" in sql
    assert "newer_observed.typed_status IN ( 'succeeded', 'skipped' )" in sql
    assert "newer_observed.stateful_status IN ( 'succeeded', 'skipped' )" in sql
    assert "newer_observed.validation_status = 'succeeded'" in sql
    assert "newer_attempt.content_hash <> attempt.content_hash" not in sql
    # Exact parser triples are required for both the current failure and the
    # newer success. A different-version success must remain visible.
    assert captured["params"].count("page-v2") == 3
    assert captured["params"].count("typed-v3") == 3
    assert captured["params"].count("stateful-v4") == 3


def test_unprocessed_list_and_summary_use_the_same_superseded_rule():
    cohort = " ".join(inspect.getsource(ControlStore.list_unprocessed_fetches).split())
    summary = " ".join(inspect.getsource(ControlStore.get_run_summary).split())
    required = (
        "failed_observed.status = 'failed'",
        "newer_attempt.target_id = attempt.target_id",
        "newer_attempt.lease_epoch > attempt.lease_epoch",
        "newer_attempt.status = 'succeeded'",
        "newer_observed.status = 'succeeded'",
        "newer_observed.generic_status = 'succeeded'",
        "newer_observed.typed_status IN ( 'succeeded', 'skipped' )",
        "newer_observed.stateful_status IN ( 'succeeded', 'skipped' )",
        "newer_observed.validation_status = 'succeeded'",
    )

    for clause in required:
        assert clause in cohort
        assert clause in summary
    # This is what preserves A -> B -> A: chronology is the fenced lease
    # sequence, never a timestamp or a comparison of repeated content hashes.
    assert "newer_attempt.finished_at > attempt.finished_at" not in cohort
    assert "newer_attempt.content_hash <> attempt.content_hash" not in cohort
    assert "failed_observed.status <> 'succeeded'" not in cohort


def _faithful_recovery_visibility(
    *, old_status, newer, requested=("page-v2", "typed-v3", "stateful-v4")
):
    """Tiny in-memory copy of the SQL contract for adversarial fixtures."""

    if old_status != "failed":
        return True
    for item in newer:
        complete = (
            item["lease_epoch"] > 1
            and item["attempt_status"] == "succeeded"
            and item["versions"] == requested
            and item["status"] == "succeeded"
            and item["generic_status"] == "succeeded"
            and item["typed_status"] in {"succeeded", "skipped"}
            and item["stateful_status"] in {"succeeded", "skipped"}
            and item["validation_status"] == "succeeded"
        )
        if complete:
            return False
    return True


def _newer_recovery_observation(**overrides):
    item = {
        "lease_epoch": 2,
        "attempt_status": "succeeded",
        "versions": ("page-v2", "typed-v3", "stateful-v4"),
        "status": "succeeded",
        "generic_status": "succeeded",
        "typed_status": "succeeded",
        "stateful_status": "succeeded",
        "validation_status": "succeeded",
        "content_hash": "b" * 64,
    }
    item.update(overrides)
    return item


@pytest.mark.parametrize(
    ("old_status", "newer", "visible"),
    [
        ("failed", [], True),
        (
            "failed",
            [
                _newer_recovery_observation(
                    versions=("page-v9", "typed-v9", "stateful-v9")
                )
            ],
            True,
        ),
        (
            "failed",
            [_newer_recovery_observation(typed_status="processing")],
            True,
        ),
        (
            "failed",
            [_newer_recovery_observation(attempt_status="claimed")],
            True,
        ),
        (
            "processing",
            [_newer_recovery_observation()],
            True,
        ),
        (
            "failed",
            [
                _newer_recovery_observation(
                    lease_epoch=2, content_hash="b" * 64, status="failed"
                ),
                _newer_recovery_observation(
                    lease_epoch=3, content_hash="a" * 64
                ),
            ],
            False,
        ),
    ],
)
def test_recovery_supersession_behavior_with_faithful_fake(
    old_status, newer, visible
):
    # The final fixture is A -> B -> A. Repeating the old hash does not make
    # chronology ambiguous because the decision uses lease_epoch only.
    assert _faithful_recovery_visibility(
        old_status=old_status,
        newer=newer,
    ) is visible


def test_registry_unknown_gender_is_durably_quarantined_then_blocks_caller():
    fetched_at = datetime(2026, 7, 14, tzinfo=timezone.utc)
    persisted = {}

    def handler(sql, params):
        if "SELECT * FROM fbref_control.registry_snapshot" in sql:
            return [{
                "successful": True,
                "source": "fbref",
                "fetched_at": fetched_at,
            }], 1
        if "SELECT max(last_seen_at) AS latest" in sql:
            return [{"latest": None}], 1
        if "SELECT count(*) AS count" in sql:
            return [{"count": 0}], 1
        if "INSERT INTO fbref_control.competition_registry" in sql:
            persisted["competition_id"] = params[1]
            persisted["gender"] = params[4]
            persisted["crawl_state"] = params[7]
            return [], 1
        return [], 0

    store, factory = make_store(handler)

    with pytest.raises(
        StateConflict, match="durably quarantined unknown gender: 99"
    ):
        store.reconcile_competitions(
            str(uuid.uuid4()),
            [competition_entry(99, gender="unknown")],
        )

    assert persisted == {
        "competition_id": "99",
        "gender": "unknown",
        "crawl_state": "quarantined",
    }
    assert factory.connections[0].committed is True
    assert factory.connections[0].rolled_back is False


def test_registry_shrink_over_ten_percent_rolls_back_without_override():
    fetched_at = datetime(2026, 7, 14, tzinfo=timezone.utc)

    def handler(sql, _params):
        if "SELECT * FROM fbref_control.registry_snapshot" in sql:
            return [{
                "successful": True,
                "source": "fbref",
                "fetched_at": fetched_at,
            }], 1
        if "SELECT max(last_seen_at) AS latest" in sql:
            return [{"latest": None}], 1
        if "SELECT count(*) AS count" in sql:
            return [{"count": 100}], 1
        raise AssertionError(sql)

    store, factory = make_store(handler)
    with pytest.raises(StateConflict, match="100 -> 89"):
        store.reconcile_competitions(
            str(uuid.uuid4()),
            [competition_entry(index) for index in range(89)],
        )

    assert factory.connections[0].rolled_back is True
    statements = [sql for sql, _ in factory.connections[0].fake_cursor.executions]
    assert not any("UPDATE fbref_control.competition_registry" in sql for sql in statements)


def test_registry_shrink_override_is_durable_and_not_rewritten():
    fetched_at = datetime(2026, 7, 14, tzinfo=timezone.utc)
    captured = {}

    def handler(sql, params):
        if "SELECT * FROM fbref_control.registry_snapshot" in sql:
            return [{
                "successful": True,
                "source": "fbref",
                "fetched_at": fetched_at,
            }], 1
        if "SELECT max(last_seen_at) AS latest" in sql:
            return [{"latest": None}], 1
        if "SELECT count(*) AS count" in sql:
            return [{"count": 1}], 1
        if "registry_reconciliation_override" in sql:
            captured["sql"] = sql
            captured["params"] = params
            return [], 1
        return [], 0

    snapshot_id = str(uuid.uuid4())
    store, factory = make_store(handler)
    counts = store.reconcile_competitions(
        snapshot_id,
        [],
        shrink_override_reason="source announced league retirement",
    )

    assert counts["snapshot_shrink_overridden"] == 1
    assert captured["params"] == (
        snapshot_id,
        "source announced league retirement",
    )
    assert "ON CONFLICT (snapshot_id, override_type) DO NOTHING" in captured["sql"]
    assert factory.connections[0].committed is True


def test_competition_disappearance_debounces_two_accepted_snapshots():
    fetched_at = datetime(2026, 7, 14, tzinfo=timezone.utc)
    registry = {
        "consecutive_misses": 0,
        "lifecycle_state": "present",
        "present": True,
    }
    missing_sql = []

    def handler(sql, _params):
        if "SELECT * FROM fbref_control.registry_snapshot" in sql:
            return [{
                "successful": True,
                "source": "fbref",
                "fetched_at": fetched_at,
            }], 1
        if "SELECT max(last_seen_at) AS latest" in sql:
            return [{"latest": None}], 1
        if "SELECT count(*) AS count" in sql:
            return [{
                "count": int(registry["lifecycle_state"] != "disappeared")
            }], 1
        if "registry_reconciliation_override" in sql:
            return [], 1
        if "SET consecutive_misses = consecutive_misses + 1" in sql:
            missing_sql.append(sql)
            registry["consecutive_misses"] += 1
            if registry["consecutive_misses"] >= 2:
                registry.update(
                    lifecycle_state="disappeared",
                    present=False,
                )
            else:
                registry.update(
                    lifecycle_state="missing_once",
                    present=True,
                )
            return [], 1
        if "SET state = 'queued'" in sql:
            eligible = (
                registry["lifecycle_state"] in {"present", "missing_once"}
                and registry["present"]
            )
            return [], int(eligible)
        if "SET state = CASE" in sql:
            eligible = (
                registry["lifecycle_state"] in {"present", "missing_once"}
                and registry["present"]
            )
            return [], int(not eligible)
        return [], 0

    store, _ = make_store(handler)
    first = store.reconcile_competitions(
        str(uuid.uuid4()),
        [],
        shrink_override_reason="accepted empty source snapshot 1",
    )
    assert registry == {
        "consecutive_misses": 1,
        "lifecycle_state": "missing_once",
        "present": True,
    }
    assert first["frontier_scope_closed"] == 0

    second = store.reconcile_competitions(
        str(uuid.uuid4()),
        [],
        shrink_override_reason="accepted empty source snapshot 2",
    )
    assert registry == {
        "consecutive_misses": 2,
        "lifecycle_state": "disappeared",
        "present": False,
    }
    assert second["frontier_scope_closed"] == 1
    assert all(
        "present = consecutive_misses + 1 < 2" in sql
        for sql in missing_sql
    )


def test_season_snapshot_rejects_multiple_current_rows():
    entries = [
        SeasonRegistryEntry(
            competition_id="9",
            season_id=season,
            canonical_url=f"https://fbref.com/en/comps/9/{season}",
            is_current=True,
        )
        for season in ("2024-2025", "2025-2026")
    ]

    with pytest.raises(ValueError, match="more than one current season"):
        ControlStore._validated_seasons("9", entries)


def test_claim_sql_is_provenance_aware_and_has_no_unscoped_fail_open():
    captured = {}

    def handler(sql, _params):
        if "SELECT status, metadata FROM fbref_control.crawl_run" in sql:
            return [{"status": "running", "metadata": {}}], 1
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
            captured["sql"] = sql
            return [], 0
        raise AssertionError(sql)

    store, _ = make_store(handler)
    assert store.claim_targets(str(uuid.uuid4()), "worker-1") == []

    sql = captured["sql"]
    assert "fbref_control.frontier_provenance" in sql
    assert "LEFT JOIN scope_rollup AS scope" in sql
    assert "frontier.page_kind = 'competition_index'" in sql
    assert "scope.scope_count > 0" in sql
    assert "scope.competition_missing" in sql
    assert "scope.has_female" in sql
    assert "scope.has_unknown" in sql
    assert "scope.has_current_season" in sql
    assert "frontier.refresh_policy = 'historical_once'" in sql
    assert "competition.lifecycle_state NOT IN ( 'present', 'missing_once' )" in sql
    assert "NOT (frontier.source_ids ? 'competition_id')" not in sql


def test_scope_reconciliation_reopens_only_its_own_quarantine_and_keeps_evidence():
    statements = []

    def handler(sql, _params):
        statements.append(sql)
        if "SET state = 'queued'" in sql:
            return [{"target_id": "player:1"}, {"target_id": "player:2"}], 2
        if "SET state = 'quarantined'" in sql:
            return [
                {"reason": "female_gender"},
                {"reason": "unresolved_scope"},
            ], 2
        raise AssertionError(sql)

    store, factory = make_store(handler)
    counts = store.reconcile_frontier_scope()

    assert counts == {
        "reopened": 2,
        "quarantined": 2,
        "female_gender": 1,
        "unresolved_scope": 1,
        "total": 2,
    }
    reopen_sql, quarantine_sql = statements
    assert "frontier.last_error_class = 'ScopeQuarantined'" in reopen_sql
    assert "frontier.state NOT IN ('leased', 'dead')" in quarantine_sql
    assert "DELETE" not in " ".join(statements)
    assert "dataset_manifest" not in " ".join(statements)
    assert "fetch_attempt" not in " ".join(statements)
    assert factory.connections[0].committed is True


def test_run_summary_splits_current_historical_and_crawlable_scope_metrics():
    run_id = str(uuid.uuid4())
    executions = []

    def handler(sql, params):
        executions.append((sql, params))
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "current"}], 1
        if "AS current_pending_match_count" in sql:
            return [{
                "current_pending_match_count": 2,
                "historical_pending_match_count": 400,
            }], 1
        if sql.startswith("SELECT frontier.target_id"):
            return [
                {
                    "target_id": "fbref:match:096e63eb",
                    "competition_id": "602",
                    "season_id": "2025",
                    "state": "queued",
                    "last_error_class": None,
                    "last_error_message": None,
                },
                {
                    "target_id": "fbref:match:7eef879c",
                    "competition_id": "122",
                    "season_id": "2025",
                    "state": "retry",
                    "last_error_class": "ClearanceFailed",
                    "last_error_message": "session lost",
                },
            ], 2
        if ") AS missing" in sql:
            return [{"count": 0}], 1
        if "SELECT gender, count(*) AS count" in sql:
            return [
                {"gender": "male", "count": 10},
                {"gender": "female", "count": 3},
                {"gender": "unknown", "count": 1},
            ], 3
        if "FROM evaluated_scope" in sql:
            return [{
                "page_kind": "match",
                "sla_seconds": 86400,
                "total_targets": 1,
                "fresh_targets": 1,
                "stale_targets": 0,
                "never_fetched_targets": 0,
                "oldest_last_fetched_at": None,
            }], 1
        if "AS crawlable" in sql:
            return [
                {"scope_status": "female_gender", "crawlable": True, "count": 1},
                {"scope_status": "female_gender", "crawlable": False, "count": 7},
                {"scope_status": "eligible_male", "crawlable": True, "count": 20},
            ], 3
        return [], 0

    store, _ = make_store(handler)
    summary = store.get_run_summary(
        run_id,
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="stateful-v4",
    )

    assert summary["promotion_pending_match_count"] == 2
    assert summary["current_pending_match_count"] == 2
    assert summary["historical_pending_match_count"] == 400
    assert summary["current_pending_match_sample"] == [
        {
            "target_id": "fbref:match:096e63eb",
            "competition_id": "602",
            "season_id": "2025",
            "state": "queued",
            "last_error_class": None,
            "last_error_message": None,
        },
        {
            "target_id": "fbref:match:7eef879c",
            "competition_id": "122",
            "season_id": "2025",
            "state": "retry",
            "last_error_class": "ClearanceFailed",
            "last_error_message": "session lost",
        },
    ]
    assert summary["registry_gender_counts"] == {
        "male": 10,
        "female": 3,
        "unknown": 1,
    }
    assert summary["unknown_gender_registry_count"] == 1
    assert summary["frontier_scope_counts"]["female_gender"] == 8
    assert summary["crawlable_frontier_scope_counts"] == {
        "female_gender": 1,
        "eligible_male": 20,
    }
    assert summary["noncrawlable_frontier_scope_counts"] == {
        "female_gender": 7,
    }
    assert summary["current_scope_freshness"]["all_within_sla"] is True
    pending_sql = next(
        sql for sql, _ in executions if "AS current_pending_match_count" in sql
    )
    assert "season.is_current" in pending_sql
    assert "frontier.refresh_policy <> 'historical_once'" in pending_sql
    sample_sql, sample_params = next(
        (sql, params)
        for sql, params in executions
        if sql.startswith("SELECT frontier.target_id")
    )
    assert "ORDER BY frontier.priority DESC" in sample_sql
    assert "frontier.refresh_policy <> 'historical_once'" in sample_sql
    assert "left(frontier.last_error_message, 500)" in sample_sql
    assert "LIMIT %s" in sample_sql
    assert sample_params == (10,)
    freshness_sql = next(
        sql for sql, _ in executions if "FROM evaluated_scope" in sql
    )
    current_scope_sql = freshness_sql.split(
        "), current_scope AS (", 1
    )[1].split("), evaluated_scope AS (", 1)[0]
    normalized_current_scope_sql = " ".join(current_scope_sql.split())
    assert (
        "AND NOT ( frontier.page_kind = 'match' "
        "AND frontier.refresh_policy = 'historical_once' )"
        in normalized_current_scope_sql
    )
    assert "refresh_policy = 'current_completed_once'" in freshness_sql
    assert "state = 'fetched'" in freshness_sql
    assert "state IN ('queued', 'retry', 'leased')" in freshness_sql
    assert "COALESCE( last_fetched_at, created_at )" in freshness_sql


def test_run_summary_separates_concurrent_raw_from_run_owned_raw():
    run_id = str(uuid.uuid4())
    captured = {}

    def handler(sql, params):
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "current"}], 1
        if ") AS missing" in sql:
            return [{"count": 0}], 1
        if "AS global_sla_overdue_count" in sql:
            captured["sql"] = sql
            captured["params"] = params
            return [{
                "page_kind": "match",
                "run_count": 0,
                "lane_count": 3,
                "lane_sla_overdue_count": 1,
                "global_count": 5,
                "global_sla_overdue_count": 2,
                "run_oldest_raw_at": None,
                "lane_oldest_raw_at": datetime(
                    2026, 7, 15, 12, tzinfo=timezone.utc
                ),
                "global_oldest_raw_at": datetime(
                    2026, 7, 14, 12, tzinfo=timezone.utc
                ),
            }], 1
        return [], 0

    store, _ = make_store(handler)
    summary = store.get_run_summary(
        run_id,
        parser_version="page-v2",
        typed_parser_version="typed-v3",
        stateful_parser_version="stateful-v4",
    )

    assert summary["unprocessed_raw_count"] == 0
    assert summary["unprocessed_raw_by_page_kind"] == {}
    assert summary["recovery_lane_run_type"] == "current"
    assert summary["lane_unprocessed_raw_count"] == 3
    assert summary["lane_unprocessed_raw_sla_overdue_count"] == 1
    assert summary["lane_unprocessed_raw_by_page_kind"]["match"] == {
        "count": 3,
        "sla_overdue_count": 1,
        "oldest_raw_at": datetime(2026, 7, 15, 12, tzinfo=timezone.utc),
    }
    assert summary["global_unprocessed_raw_count"] == 5
    assert summary["global_unprocessed_raw_sla_overdue_count"] == 2
    assert summary["unprocessed_raw_sla_overdue_count"] == 2
    assert summary["global_unprocessed_raw_by_page_kind"]["match"][
        "count"
    ] == 5
    assert "attempt.run_id = %s" in captured["sql"]
    assert "source_run.run_type = %s" in captured["sql"]
    assert captured["sql"].count("%s") == len(captured["params"])
    assert captured["params"][:7] == (
        run_id,
        "current",
        "current",
        86_400,
        86_400,
        run_id,
        "current",
    )


def test_requeue_closes_claimed_attempt_as_cancelled():
    lease = TargetLease(
        attempt_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()),
        target_id="fbref:match:a071faa8",
        logical_refresh_id=str(uuid.uuid4()),
        canonical_url="https://fbref.com/en/matches/a071faa8",
        page_kind="match",
        source_ids={"match_id": "a071faa8"},
        claim_token=str(uuid.uuid4()),
        lease_epoch=4,
        attempt_number=1,
        leased_by="worker-1",
        lease_expires_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    captured = {}

    def handler(sql, params):
        if "UPDATE fbref_control.page_frontier" in sql:
            return [{"target_id": lease.target_id}], 1
        if "UPDATE fbref_control.run_target" in sql:
            return [], 1
        if "UPDATE fbref_control.fetch_attempt" in sql:
            captured["sql"] = sql
            captured["params"] = params
            return [], 1
        raise AssertionError(sql)

    store, factory = make_store(handler)
    assert store.requeue_unfetched_targets([lease]) == 1

    assert "SET status = 'cancelled'" in captured["sql"]
    assert "error_class = 'UnfetchedRequeue'" in captured["sql"]
    assert "finished_at = clock_timestamp()" in captured["sql"]
    assert captured["params"][0] == lease.attempt_id
    assert factory.connections[0].committed is True


def test_session_failure_retries_same_run_and_logical_refresh_immediately():
    lease = TargetLease(
        attempt_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()),
        target_id="fbref:match:7eef879c",
        logical_refresh_id=str(uuid.uuid4()),
        canonical_url="https://fbref.com/en/matches/7eef879c",
        page_kind="match",
        source_ids={"competition_id": "122", "season_id": "2025"},
        claim_token=str(uuid.uuid4()),
        lease_epoch=2,
        attempt_number=1,
        leased_by="live-waves",
        lease_expires_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )
    transitions = {}

    def handler(sql, params):
        if "UPDATE fbref_control.page_frontier" in sql:
            transitions["frontier"] = (sql, params)
            return [{"target_id": lease.target_id}], 1
        if "UPDATE fbref_control.run_target" in sql:
            transitions["run_target"] = (sql, params)
            return [], 1
        if "UPDATE fbref_control.fetch_attempt" in sql:
            transitions["attempt"] = (sql, params)
            return [], 1
        raise AssertionError(sql)

    store, factory = make_store(handler)
    store.retry_session_fetch(
        lease,
        error_class="ClearanceFailed",
        error_message="warm session rejected",
        http_status=403,
        wire_bytes=1234,
        http_request_count=1,
        http_status_history=[403],
        transport_version="http-v2",
        session_version="clearance-v3",
    )

    frontier_sql, frontier_params = transitions["frontier"]
    assert "SET state = %s" in frontier_sql
    assert "retry_after = CASE WHEN %s THEN NULL ELSE" in frontier_sql
    assert frontier_params[:6] == (
        "retry",
        False,
        0,
        403,
        "ClearanceFailed",
        "warm session rejected",
    )
    run_target_sql, run_target_params = transitions["run_target"]
    assert "SET status = %s" in run_target_sql
    assert run_target_params == (
        "retry",
        lease.run_id,
        lease.target_id,
        lease.logical_refresh_id,
    )
    attempt_sql, attempt_params = transitions["attempt"]
    assert "SET status = 'failed'" in attempt_sql
    assert attempt_params[:7] == (
        403,
        1234,
        None,
        1,
        [403],
        "ClearanceFailed",
        "warm session rejected",
    )
    claim_source = inspect.getsource(ControlStore.claim_targets)
    assert "target.status IN ('pending', 'retry')" in claim_source
    assert "frontier.state IN ('queued', 'retry')" in claim_source
    assert "WHERE logical_refresh_id = %s" in claim_source
    assert factory.connections[0].committed is True


def test_contract_quarantine_is_a_distinct_verdict_from_the_scope_sweep():
    captured = {}

    def handler(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [], 1

    store, factory = make_store(handler)
    applied = store.quarantine_contract_rejected_target(
        "fbref:season:76:2017",
        content_hash="c" * 64,
        reason="schedule_link_missing",
    )

    assert applied is True
    sql = captured["sql"]
    assert "SET state = 'quarantined'" in sql
    assert "next_fetch_at = NULL" in sql
    # A scope sweep reopens only its own verdict, so a parser verdict must not
    # borrow its error class or the next sweep would resurrect the target.
    assert "last_error_class = 'ParseContractQuarantined'" in sql
    assert "ScopeQuarantined" not in sql
    assert "state NOT IN ('leased', 'dead')" in sql
    # Fenced to the rejected bytes: a newer fetch must not be retired on a
    # verdict formed against content it already replaced.
    assert "last_content_hash = %s" in sql
    assert captured["params"] == (
        "schedule_link_missing",
        "fbref:season:76:2017",
        "c" * 64,
    )
    assert "DELETE" not in sql
    assert factory.connections[0].committed is True


def test_contract_quarantine_reports_when_the_target_was_not_retired():
    store, _ = make_store(lambda sql, params: ([], 0))

    assert (
        store.quarantine_contract_rejected_target(
            "fbref:season:76:2017",
            content_hash="c" * 64,
            reason="schedule_link_missing",
        )
        is False
    )


def test_retired_targets_leave_both_the_cohort_and_the_unprocessed_raw_gate():
    cohort = inspect.getsource(ControlStore.list_unprocessed_fetches)
    summary = inspect.getsource(ControlStore.get_run_summary)

    # Raw behind a retired target is out of scope by the same decision that
    # retired it.  Both readers must agree: clearing recovery while the run
    # summary still counts the same bytes only moves the failure downstream.
    assert "frontier.state <> 'quarantined'" in cohort
    assert "attempt.status = 'succeeded'" in cohort
    assert "frontier.state <> 'quarantined'" in summary
    assert "global_sla_overdue_count" in summary


def test_run_summary_excludes_terminal_quarantine_from_all_raw_validation_gates():
    executions = []
    run_id = str(uuid.uuid4())

    def handler(sql, params):
        executions.append((sql, params))
        if "SELECT * FROM fbref_control.crawl_run" in sql:
            return [{"run_id": run_id, "run_type": "current"}], 1
        if "AS missing" in sql:
            return [{"count": 0}], 1
        return [], 0

    store, _ = make_store(handler)
    store.get_run_summary(
        run_id,
        parser_version="page-current",
        typed_parser_version="typed-current",
        stateful_parser_version="discovery-current",
    )

    dataset_sql = next(
        sql for sql, _ in executions
        if "GROUP BY manifest.validation_status" in sql
    )
    unvalidated_sql = next(
        sql for sql, _ in executions if "AS missing" in sql
    )
    raw_sql = next(
        sql for sql, _ in executions if "AS global_sla_overdue_count" in sql
    )
    terminal_scope = (
        "JOIN fbref_control.page_frontier AS frontier "
        "ON frontier.target_id = attempt.target_id"
    )
    terminal_filter = "AND frontier.state <> 'quarantined'"

    for sql in (dataset_sql, unvalidated_sql, raw_sql):
        assert terminal_scope in sql
        assert terminal_filter in sql


BARE_SEASON_URL = "https://fbref.com/en/comps/12/La-Liga-Stats"
DATED_SEASON_URL = (
    "https://fbref.com/en/comps/12/2025-2026/2025-2026-La-Liga-Stats"
)
BARE_STATS_URL = "https://fbref.com/en/comps/12/stats/La-Liga-Stats"
DATED_STATS_URL = (
    "https://fbref.com/en/comps/12/2025-2026/stats/2025-2026-La-Liga-Stats"
)


ROLLED_OVER_REGISTRY = {
    "outgoing_url": DATED_SEASON_URL,
    "current_url": BARE_SEASON_URL,
}


def _rollover_handler(
    installed, *, registry=ROLLED_OVER_REGISTRY, released=True
):
    def handler(sql, params):
        if "FROM fbref_control.page_frontier" in sql and "FOR UPDATE" in sql:
            target_id, canonical_url = params
            rows = [
                dict(row)
                for row in installed
                if row["target_id"] == target_id
                or row["canonical_url"] == canonical_url
            ]
            return rows, len(rows)
        if "AS outgoing_url" in sql:
            return ([dict(registry)], 1) if registry else ([], 0)
        if "UPDATE fbref_control.page_frontier AS outgoing" in sql:
            return [], 1 if released else 0
        if "UPDATE fbref_control.page_frontier SET canonical_url" in sql:
            return [], 1
        if "INSERT INTO fbref_control.page_frontier" in sql:
            return [], 1
        raise AssertionError(sql)

    return handler


def _outgoing_season_row(**overrides):
    row = {
        "target_id": "fbref:season:12:2025-2026",
        "source": "fbref",
        "page_kind": "season",
        "canonical_url": BARE_SEASON_URL,
        "source_ids": {"competition_id": "12", "season_id": "2025-2026"},
        "state": "fetched",
    }
    row.update(overrides)
    return row


def _outgoing_stats_row(**overrides):
    return _outgoing_season_row(**{
        "target_id": "fbref:season_stats:12:2025-2026:standard",
        "page_kind": "season_stats",
        "canonical_url": BARE_STATS_URL,
        "source_ids": {
            "competition_id": "12",
            "season_id": "2025-2026",
            "stat_route": "standard",
        },
        **overrides,
    })


def _incoming_season_target():
    return FrontierTarget(
        target_id="fbref:season:12:2026-2027",
        page_kind="season",
        canonical_url=BARE_SEASON_URL,
        source_ids={"competition_id": "12", "season_id": "2026-2027"},
        refresh_policy="daily",
    )


def _incoming_stats_target():
    return FrontierTarget(
        target_id="fbref:season_stats:12:2026-2027:standard",
        page_kind="season_stats",
        canonical_url=BARE_STATS_URL,
        source_ids={
            "competition_id": "12",
            "season_id": "2026-2027",
            "stat_route": "standard",
        },
        refresh_policy="daily",
    )


def _releases(factory):
    return [
        (sql, params)
        for sql, params in factory.connections[0].fake_cursor.executions
        if sql.startswith("UPDATE fbref_control.page_frontier AS outgoing")
    ]


def _sentinel_parks(factory):
    return [
        (sql, params)
        for sql, params in factory.connections[0].fake_cursor.executions
        if sql.startswith("UPDATE fbref_control.page_frontier SET")
        and "'#superseded:'" in sql
    ]


def _registry_lookups(factory):
    return [
        (sql, params)
        for sql, params in factory.connections[0].fake_cursor.executions
        if "AS outgoing_url" in sql
    ]


def test_rollover_moves_the_outgoing_edition_onto_its_registry_url():
    store, factory = make_store(_rollover_handler([_outgoing_season_row()]))

    store.upsert_frontier_target(_incoming_season_target())

    lookup, lookup_params = _registry_lookups(factory)[0]
    # Never copy a registry row that is itself parked or unpublished, and never
    # take a URL the registry still assigns to the holder.
    assert "strpos(season.canonical_url, '#superseded:') = 0" in lookup
    assert "season.present AND season.lifecycle_state = 'present'" in lookup
    assert "season.canonical_url <> outgoing.canonical_url" in lookup
    assert lookup_params == ("fbref:season:12:2025-2026", BARE_SEASON_URL)

    releases = _releases(factory)
    assert len(releases) == 1
    sql, params = releases[0]
    # The registry is the only authority for the URL an outgoing season moved
    # to: never a sentinel, never a state change that would drop a live target
    # out of its run cohort, never a destination someone else already holds.
    assert "state" not in sql.split(" WHERE ")[0]
    assert "NOT EXISTS" in sql
    # The validators of the URL the target leaves must not survive the move.
    assert "last_etag = NULL" in sql
    # last_content_hash fences the observation already in flight for this
    # target, so the move must not clear it.
    assert "last_content_hash" not in sql
    assert params == (
        DATED_SEASON_URL,
        "fbref:season:12:2025-2026",
        BARE_SEASON_URL,
        DATED_SEASON_URL,
    )


def test_rollover_moves_an_outgoing_subpage_onto_its_derived_url():
    # season_registry addresses seasons, not their stats/schedule subpages, so
    # the outgoing subpage of a rolled-over edition had no destination at all
    # and fell straight through to StateConflict (#1133, comps 23/32/51).
    store, factory = make_store(_rollover_handler([_outgoing_stats_row()]))

    store.upsert_frontier_target(_incoming_stats_target())

    releases = _releases(factory)
    assert len(releases) == 1
    _, params = releases[0]
    assert params == (
        DATED_STATS_URL,
        "fbref:season_stats:12:2025-2026:standard",
        BARE_STATS_URL,
        DATED_STATS_URL,
    )


def test_rollover_release_never_touches_a_leased_holder():
    store, factory = make_store(
        _rollover_handler([_outgoing_season_row(state="leased")])
    )

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_incoming_season_target())
    assert _registry_lookups(factory) == []
    assert _releases(factory) == []


def test_rollover_release_is_limited_to_season_addressed_pages():
    # A squad/player/match address never carries a season segment, so a
    # collision there is a genuine identity clash, not a handover.
    store, factory = make_store(
        _rollover_handler([_outgoing_season_row(page_kind="squad")])
    )

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_incoming_season_target())
    assert _registry_lookups(factory) == []
    assert _releases(factory) == []


def test_a_subpage_without_a_proven_url_shape_is_never_moved():
    # No current season on a bare URL means the competition never proved the
    # segment and slug prefix its dated pages use; guessing them would point a
    # target at a page nobody has seen.
    store, factory = make_store(
        _rollover_handler(
            [_outgoing_stats_row()],
            registry={
                "outgoing_url": DATED_SEASON_URL,
                "current_url": None,
            },
        )
    )

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_incoming_stats_target())
    assert len(_registry_lookups(factory)) == 1
    assert _releases(factory) == []


def test_a_stale_edition_cannot_take_a_url_the_registry_still_assigns():
    store, factory = make_store(
        _rollover_handler([_outgoing_season_row()], released=False)
    )

    # The destination is already held by someone else, so nothing is released
    # and admission fails closed exactly as it did before the rollover fix.
    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_incoming_season_target())
    assert len(_releases(factory)) == 1


def test_a_rolled_over_holder_is_moved_rather_than_parked_on_a_sentinel():
    # An out-of-scope edition is quarantined, not deleted.  Parking it on a
    # sentinel would leave it on an address it no longer owns: canonicalisation
    # drops the fragment, so the next fetch would pull the new edition's page
    # under the old edition's identity.
    store, factory = make_store(
        _rollover_handler([_outgoing_stats_row(state="quarantined")])
    )

    store.upsert_frontier_target(_incoming_stats_target())

    assert len(_releases(factory)) == 1
    assert _sentinel_parks(factory) == []


def test_a_holder_the_registry_cannot_place_still_parks_on_the_sentinel():
    # The pre-#949 mis-mints (a /stats/ page minted as season 'stats') have no
    # registry row to move onto, so the sentinel remains their only release.
    store, factory = make_store(
        _rollover_handler(
            [_outgoing_season_row(
                target_id="fbref:season:12:stats",
                state="quarantined",
                source_ids={"competition_id": "12", "season_id": "stats"},
            )],
            registry=None,
        )
    )

    store.upsert_frontier_target(_incoming_season_target())

    assert _releases(factory) == []
    assert len(_sentinel_parks(factory)) == 1


@pytest.mark.parametrize(
    "holder, outgoing, current, expected",
    [
        # The season prefix is not the season id: tournament editions label
        # themselves by the year they finish, and the subpage must follow the
        # label its own edition actually uses.
        (
            "https://fbref.com/en/comps/646/keepers/Supercopa-de-Espana-Stats",
            "https://fbref.com/en/comps/646/2024-2025/"
            "2025-Supercopa-de-Espana-Stats",
            "https://fbref.com/en/comps/646/Supercopa-de-Espana-Stats",
            "https://fbref.com/en/comps/646/2024-2025/keepers/"
            "2025-Supercopa-de-Espana-Stats",
        ),
        # A schedule page keeps its own slug, only the prefix is borrowed.
        (
            "https://fbref.com/en/comps/12/schedule/"
            "La-Liga-Scores-and-Fixtures",
            DATED_SEASON_URL,
            BARE_SEASON_URL,
            "https://fbref.com/en/comps/12/2025-2026/schedule/"
            "2025-2026-La-Liga-Scores-and-Fixtures",
        ),
        # Already dated: there is nothing to hand over.
        (
            DATED_STATS_URL,
            DATED_SEASON_URL,
            BARE_SEASON_URL,
            None,
        ),
        # The current season is not on a bare URL, so the shape is unproven.
        (
            BARE_STATS_URL,
            DATED_SEASON_URL,
            "https://fbref.com/en/comps/12/2026-2027/2026-2027-La-Liga-Stats",
            None,
        ),
        # The two registry rows do not describe one competition slug.
        (
            BARE_STATS_URL,
            "https://fbref.com/en/comps/12/2025-2026/2025-2026-Segunda-Stats",
            BARE_SEASON_URL,
            None,
        ),
        # A different host is a different resource, never a handover.
        (
            "https://fbref.com.mirror/en/comps/12/stats/La-Liga-Stats",
            DATED_SEASON_URL,
            BARE_SEASON_URL,
            None,
        ),
    ],
)
def test_dated_subpage_urls_are_read_off_the_registry_never_guessed(
    holder, outgoing, current, expected
):
    assert ControlStore._dated_rollover_subpage_url(
        holder, outgoing, current
    ) == expected


def test_season_reconcile_releases_claimed_urls_before_it_upserts_them():
    fetched_at = datetime(2026, 8, 5, 6, 0, tzinfo=timezone.utc)
    snapshot = str(uuid.uuid4())

    def handler(sql, params):
        if "FROM fbref_control.registry_snapshot" in sql:
            return [{
                "snapshot_id": snapshot,
                "source": "fbref",
                "successful": True,
                "fetched_at": fetched_at,
            }], 1
        if "FROM fbref_control.competition_registry" in sql:
            return [{
                "crawl_state": "active",
                "lifecycle_state": "present",
                "present": True,
            }], 1
        if "max(last_seen_at)" in sql:
            return [{"latest": None}], 1
        return [], 0

    store, factory = make_store(handler)
    store.reconcile_seasons(
        snapshot,
        "12",
        [
            SeasonRegistryEntry(
                competition_id="12",
                season_id="2026-2027",
                canonical_url="https://fbref.com/en/comps/12/La-Liga-Stats",
                is_current=True,
            ),
            SeasonRegistryEntry(
                competition_id="12",
                season_id="2025-2026",
                canonical_url=(
                    "https://fbref.com/en/comps/12/2025-2026/"
                    "2025-2026-La-Liga-Stats"
                ),
            ),
        ],
    )

    statements = [
        sql for sql, _ in factory.connections[0].fake_cursor.executions
    ]
    release = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("UPDATE fbref_control.season_registry AS season")
        and "'#superseded:'" in sql
    )
    first_upsert = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("INSERT INTO fbref_control.season_registry")
    )
    # UNIQUE (source, canonical_url) is not an upsert arbiter: the handover has
    # to be released before the first row claims a URL someone else still owns.
    assert release < first_upsert
    sql, params = factory.connections[0].fake_cursor.executions[release]
    # The sentinel carries the parked row's own season id, so repeated
    # handovers of one URL can never collide on the constraint they release.
    assert (
        "'#superseded:' || season.competition_id || ':' || season.season_id"
        in sql
    )
    assert params[0] == ["2026-2027", "2025-2026"]
    assert params[2:] == ("fbref", "12")
