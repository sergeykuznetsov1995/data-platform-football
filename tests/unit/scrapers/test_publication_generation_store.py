import copy
import json
import uuid
from datetime import datetime, timezone

import pytest

from scrapers.fbref.control import ControlStore, StateConflict


class MemoryPublicationDB:
    def __init__(self):
        self.run = None
        self.lock = None
        self.now = datetime.now(timezone.utc)

    def snapshot(self):
        return copy.deepcopy((self.run, self.lock))

    def restore(self, snapshot):
        self.run, self.lock = snapshot

    def _run_row(self):
        return None if self.run is None else copy.deepcopy(self.run)

    def _lock_row(self):
        if self.lock is None:
            return None
        row = copy.deepcopy(self.lock)
        row["active"] = bool(
            row.pop("lease_valid", True) and row["released_at"] is None
        )
        return row

    def handle(self, sql, params):
        if sql.startswith("INSERT INTO fbref_control.crawl_run"):
            if self.run is None:
                self.run = {
                    "run_id": params[0],
                    "run_type": "publication",
                    "status": "pending",
                    "request_limit": 0,
                    "byte_limit": 0,
                    "metadata": json.loads(params[1]),
                }
                return [], 1
            return [], 0

        if (
            sql.startswith("SELECT run_id, run_type, status")
            and "FOR UPDATE" in sql
        ):
            if self.run is None or self.run["run_id"] != params[0]:
                return [], 0
            return [self._run_row()], 1

        if "SET status = 'running'" in sql:
            if self.run["status"] != "pending":
                return [], 0
            self.run["status"] = "running"
            return [], 1

        if sql.startswith("INSERT INTO fbref_control.publication_lock"):
            if self.lock is None:
                self.lock = {
                    "source": params[0],
                    "owner_run_id": params[1],
                    "owner_dag_id": params[2],
                    "acquired_at": self.now,
                    "expires_at": self.now,
                    "released_at": None,
                    "lease_valid": True,
                }
                return [], 1
            return [], 0

        if (
            sql.startswith("SELECT source, owner_run_id, owner_dag_id")
            and "FOR UPDATE" in sql
        ):
            if self.lock is None or self.lock["source"] != params[0]:
                return [], 0
            return [self._lock_row()], 1

        if (
            sql.startswith("UPDATE fbref_control.publication_lock")
            and "SET owner_run_id" in sql
        ):
            self.lock.update(
                {
                    "owner_run_id": params[0],
                    "owner_dag_id": params[1],
                    "acquired_at": self.now,
                    "expires_at": self.now,
                    "released_at": None,
                    "lease_valid": True,
                }
            )
            return [self._lock_row()], 1

        if "SET status = 'succeeded'" in sql:
            if self.run["status"] != "running":
                return [], 0
            self.run["status"] = "succeeded"
            self.run["metadata"] = json.loads(params[0])
            return [], 1

        if "SET status = 'failed'" in sql:
            if self.run["status"] != "running":
                return [], 0
            self.run["status"] = "failed"
            self.run["metadata"] = json.loads(params[0])
            return [], 1

        if (
            sql.startswith("UPDATE fbref_control.crawl_run")
            and "SET metadata = %s::jsonb" in sql
        ):
            self.run["metadata"] = json.loads(params[0])
            return [], 1

        if (
            sql.startswith("UPDATE fbref_control.publication_lock")
            and "SET expires_at" in sql
        ):
            self.lock["expires_at"] = self.now
            self.lock["lease_valid"] = True
            return [self._lock_row()], 1

        if (
            sql.startswith("UPDATE fbref_control.publication_lock")
            and "SET released_at" in sql
        ):
            if self.lock["released_at"] is not None:
                return [], 0
            self.lock["released_at"] = self.now
            return [{"released_at": self.now}], 1

        if (
            sql.startswith("SELECT run.run_id, run.run_type, run.status")
            and "LEFT JOIN" in sql
        ):
            source, generation_id = params
            if self.run is None or self.run["run_id"] != generation_id:
                return [], 0
            row = self._run_row()
            exact = (
                self.lock is not None
                and self.lock["source"] == source
                and self.lock["owner_run_id"] == generation_id
            )
            lock = self._lock_row() if exact else None
            for name in (
                "owner_run_id",
                "owner_dag_id",
                "acquired_at",
                "expires_at",
                "released_at",
                "active",
            ):
                row[name] = None if lock is None else lock[name]
            return [row], 1

        if (
            sql.startswith("SELECT run.run_id, run.run_type, run.status")
            and "JOIN fbref_control.crawl_run" in sql
        ):
            if self.run is None or self.lock is None:
                return [], 0
            row = self._run_row()
            row.update(self._lock_row())
            return [row], 1

        raise AssertionError(sql)


class FakeCursor:
    def __init__(self, database):
        self.database = database
        self.rows = []
        self.rowcount = 0
        self.description = None

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.rows, self.rowcount = self.database.handle(normalized, params)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, database):
        self.database = database
        self.before = database.snapshot()
        self.fake_cursor = FakeCursor(database)
        self.committed = False

    def cursor(self, **_kwargs):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.database.restore(self.before)

    def close(self):
        pass


class FakeFactory:
    def __init__(self, database):
        self.database = database
        self.connections = []

    def __call__(self, _dsn):
        connection = FakeConnection(self.database)
        self.connections.append(connection)
        return connection


def make_store():
    database = MemoryPublicationDB()
    factory = FakeFactory(database)
    store = ControlStore(
        "postgresql://airflow:pw@postgres/airflow",
        connection_factory=factory,
    )
    return store, database, factory


def test_publication_generation_full_lifecycle_is_exact_and_retry_safe():
    store, _database, factory = make_store()
    generation_id = str(uuid.uuid4())
    binding = {
        "data_interval_start": "2026-07-20T14:00:00+00:00",
        "data_interval_end": "2026-07-21T14:00:00+00:00",
        "runtime_fingerprint": "a" * 40,
        "owner": "isolated",
    }
    consumer = {"dag_id": "dag_master_pipeline", "run_id": "scheduled__1"}

    initialized = store.initialize_publication_generation(
        generation_id,
        dag_id="dag_trigger_fotmob_daily",
        binding=binding,
        source="fotmob",
    )
    retried = store.initialize_publication_generation(
        generation_id,
        dag_id="dag_trigger_fotmob_daily",
        binding=binding,
        source="fotmob",
    )

    assert initialized["phase"] == "writing"
    assert initialized["status"] == "running"
    assert initialized["active"] is True
    assert retried["idempotent"] is True

    with store.guard_publication_writer(
        generation_id, source="fotmob"
    ) as guarded:
        assert guarded["binding"] == binding
        assert factory.connections[-1].committed is False
    assert factory.connections[-1].committed is True

    candidate = {"status": "passed", "tables": ["team", "player"]}
    recorded = store.record_publication_candidate(
        generation_id, candidate=candidate, source="fotmob"
    )
    repeated = store.record_publication_candidate(
        generation_id, candidate=candidate, source="fotmob"
    )
    assert recorded["idempotent"] is False
    assert repeated["idempotent"] is True

    ready = store.seal_publication_generation(
        generation_id, source="fotmob"
    )
    assert ready["phase"] == "ready"
    assert ready["status"] == "succeeded"
    assert ready["active"] is True
    with pytest.raises(StateConflict, match="running writing"):
        with store.guard_publication_writer(generation_id, source="fotmob"):
            pass

    observed = store.get_publication_generation(
        generation_id, source="fotmob"
    )
    assert observed == {
        **observed,
        "generation_id": generation_id,
        "binding": binding,
        "phase": "ready",
        "status": "succeeded",
        "active": True,
        "lock_active": True,
    }

    claimed = store.claim_publication_generation(
        generation_id,
        consumer=consumer,
        binding=binding,
        source="fotmob",
    )
    assert claimed["phase"] == "consuming"
    with pytest.raises(StateConflict, match="another consumer"):
        store.claim_publication_generation(
            generation_id,
            consumer={"dag_id": "other", "run_id": "scheduled__2"},
            source="fotmob",
        )

    published = store.complete_publication_generation(
        generation_id,
        consumer=consumer,
        published=True,
        source="fotmob",
    )
    repeated_publish = store.complete_publication_generation(
        generation_id,
        consumer=consumer,
        published=True,
        source="fotmob",
    )
    assert published["phase"] == "published"
    assert published["released"] is True
    assert repeated_publish["idempotent"] is True
    assert store.get_publication_generation(
        generation_id, source="fotmob"
    )["active"] is False


def test_candidate_is_immutable_and_seal_requires_candidate():
    store, _database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding={"interval": "exact"},
        source="fotmob",
    )

    with pytest.raises(StateConflict, match="without a candidate"):
        store.seal_publication_generation(generation_id, source="fotmob")

    store.record_publication_candidate(
        generation_id, candidate={"status": "passed"}, source="fotmob"
    )
    with pytest.raises(StateConflict, match="recorded differently"):
        store.record_publication_candidate(
            generation_id, candidate={"status": "failed"}, source="fotmob"
        )


def test_generation_binding_is_immutable_and_claim_checks_exact_binding():
    store, _database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding={"interval": "expected", "runtime": "release-a"},
        source="fotmob",
    )
    with pytest.raises(StateConflict, match="immutable data"):
        store.initialize_publication_generation(
            generation_id,
            dag_id="writer",
            binding={"interval": "stale", "runtime": "release-a"},
            source="fotmob",
        )

    store.record_publication_candidate(
        generation_id, candidate={"status": "passed"}, source="fotmob"
    )
    store.seal_publication_generation(generation_id, source="fotmob")
    with pytest.raises(StateConflict, match="different exact binding"):
        store.claim_publication_generation(
            generation_id,
            consumer={"dag_id": "master", "run_id": "run"},
            binding={"interval": "stale", "runtime": "release-a"},
            source="fotmob",
        )


def test_wrong_or_stale_lock_fails_writer_closed():
    store, database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding={"interval": "exact"},
        source="fotmob",
    )

    database.lock["lease_valid"] = False
    with pytest.raises(StateConflict, match="not owned"):
        with store.guard_publication_writer(generation_id, source="fotmob"):
            pass

    database.lock["lease_valid"] = True
    database.lock["owner_run_id"] = str(uuid.uuid4())
    with pytest.raises(StateConflict, match="not owned"):
        with store.guard_publication_writer(generation_id, source="fotmob"):
            pass


def test_failed_writer_is_terminal_and_releases_only_when_safe():
    store, _database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding={"interval": "exact"},
        source="fotmob",
    )

    retained = store.fail_publication_generation(
        generation_id, safe_to_release=False, source="fotmob"
    )
    assert retained["phase"] == "failed"
    assert retained["status"] == "failed"
    assert retained["active"] is True
    assert retained["released"] is False
    with pytest.raises(StateConflict, match="phase=failed"):
        store.assert_no_active_publication_generation(source="fotmob")

    released = store.fail_publication_generation(
        generation_id, safe_to_release=True, source="fotmob"
    )
    assert released["idempotent"] is True
    assert released["released"] is True
    assert store.get_publication_generation(
        generation_id, source="fotmob"
    )["active"] is False


def test_ready_generation_can_be_abandoned_but_consuming_cannot():
    store, _database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding={"interval": "exact"},
        source="fotmob",
    )
    store.record_publication_candidate(
        generation_id, candidate={"status": "passed"}, source="fotmob"
    )
    store.seal_publication_generation(generation_id, source="fotmob")

    abandoned = store.complete_publication_generation(
        generation_id, published=False, source="fotmob"
    )
    assert abandoned["phase"] == "abandoned"
    assert abandoned["released"] is True


def test_ops_guard_blocks_every_active_phase_including_ready_and_consuming():
    store, _database, _factory = make_store()
    generation_id = str(uuid.uuid4())
    binding = {"interval": "exact"}
    store.initialize_publication_generation(
        generation_id,
        dag_id="writer",
        binding=binding,
        source="fotmob",
    )

    with pytest.raises(StateConflict, match="phase=writing"):
        store.assert_no_active_publication_generation(source="fotmob")

    store.record_publication_candidate(
        generation_id, candidate={"status": "passed"}, source="fotmob"
    )
    store.seal_publication_generation(generation_id, source="fotmob")
    with pytest.raises(StateConflict, match="phase=ready"):
        store.assert_no_active_publication_generation(source="fotmob")

    store.claim_publication_generation(
        generation_id,
        consumer={"dag_id": "master", "run_id": "run"},
        binding=binding,
        source="fotmob",
    )
    with pytest.raises(StateConflict, match="phase=consuming"):
        store.assert_no_active_publication_generation(source="fotmob")

    consumer = {"dag_id": "master", "run_id": "run"}
    store.complete_publication_generation(
        generation_id,
        consumer=consumer,
        published=True,
        source="fotmob",
    )
    safe = store.assert_no_active_publication_generation(source="fotmob")
    assert safe["phase"] == "published"
    assert safe["active"] is False
    assert safe["safe"] is True
