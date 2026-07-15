"""Real PostgreSQL semantics for FBref publication admission ordering."""

from __future__ import annotations

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit, urlunsplit

import pytest

from scrapers.fbref.control import ControlStore, StateConflict


pytestmark = pytest.mark.integration


def _postgres_uri() -> str:
    uri = os.getenv("FBREF_TEST_POSTGRES_URI", "").strip()
    if not uri and os.getenv("FBREF_TEST_POSTGRES_USE_AIRFLOW_DB") == "1":
        uri = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").strip()
    if not uri:
        pytest.skip("FBREF_TEST_POSTGRES_URI is not configured")
    return uri.replace("postgresql+psycopg2://", "postgresql://", 1)


class _RollbackOnlyConnection:
    """Let ControlStore delimit calls without committing the test fixture."""

    def __init__(self, connection):
        self._connection = connection

    def cursor(self, *args, **kwargs):
        return self._connection.cursor(*args, **kwargs)

    def commit(self):
        pass

    def rollback(self):
        self._connection.rollback()

    def close(self):
        pass


@pytest.fixture
def isolated_postgres_uri():
    """Create a disposable migrated database for committed race tests."""

    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql

    source_dsn = _postgres_uri()
    database = f"fbref_race_{uuid.uuid4().hex}"
    admin = psycopg2.connect(source_dsn)
    admin.autocommit = True
    try:
        try:
            with admin.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(database)
                    )
                )
        except psycopg2.errors.InsufficientPrivilege:
            pytest.skip("PostgreSQL role cannot create an isolated test database")
        parsed = urlsplit(source_dsn)
        isolated = urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                f"/{database}",
                parsed.query,
                parsed.fragment,
            )
        )
        ControlStore(isolated).migrate()
        yield isolated
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (database,),
            )
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(database)
                )
            )
        admin.close()


def test_current_matches_are_admitted_before_older_enrichment_backlog():
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    connection = psycopg2.connect(
        dsn,
        application_name="fbref-admission-semantic-test",
        options="-c statement_timeout=10000",
    )
    suffix = uuid.uuid4().hex
    run_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    male_competition = f"test-male-{suffix}"
    female_competition = f"test-female-{suffix}"
    unknown_competition = f"test-unknown-{suffix}"
    season_id = f"2026-{suffix}"
    index_target = f"fbref:test:index:{suffix}"
    match_targets = [
        f"fbref:test:match:{position}:{suffix}" for position in range(4)
    ]
    season_target = f"fbref:test:season:{suffix}"
    stats_target = f"fbref:test:season-stats:{suffix}"
    player_target = f"fbref:test:player:{suffix}"
    historical_target = f"fbref:test:historical-match:{suffix}"
    current_policy = f"semantic-current-{suffix}"

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES (%s, 'current-semantic-test', 'running', 100, 52428800)
                """,
                (run_id,),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.registry_snapshot (
                    snapshot_id, run_id, successful, fetched_at
                ) VALUES (%s, %s, true, clock_timestamp())
                """,
                (snapshot_id, run_id),
            )
            for competition_id, gender in (
                (male_competition, "male"),
                (female_competition, "female"),
                (unknown_competition, "unknown"),
            ):
                cursor.execute(
                    """
                    INSERT INTO fbref_control.competition_registry (
                        competition_id, canonical_url, name, gender,
                        classification, crawl_state, first_seen_at,
                        last_seen_at, first_snapshot_id, last_snapshot_id
                    ) VALUES (
                        %s, %s, %s, %s, 'test', 'active',
                        clock_timestamp(), clock_timestamp(), %s, %s
                    )
                    """,
                    (
                        competition_id,
                        f"https://example.invalid/{competition_id}",
                        competition_id,
                        gender,
                        snapshot_id,
                        snapshot_id,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.season_registry (
                        competition_id, season_id, canonical_url, label,
                        is_current, first_seen_at, last_seen_at,
                        first_snapshot_id, last_snapshot_id
                    ) VALUES (
                        %s, %s, %s, 'test', true,
                        clock_timestamp(), clock_timestamp(), %s, %s
                    )
                    """,
                    (
                        competition_id,
                        season_id,
                        f"https://example.invalid/{competition_id}/{season_id}",
                        snapshot_id,
                        snapshot_id,
                    ),
                )

            def insert_target(
                target_id,
                page_kind,
                source_ids,
                refresh_policy,
                created_at,
                priority=0,
            ):
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, source_ids,
                        refresh_policy, priority, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                    """,
                    (
                        target_id,
                        page_kind,
                        f"https://example.invalid/{target_id}",
                        json.dumps(source_ids),
                        refresh_policy,
                        priority,
                        created_at,
                        created_at,
                    ),
                )

            insert_target(
                index_target,
                "competition_index",
                {},
                current_policy,
                "1950-01-01T00:00:00Z",
            )
            for position, target_id in enumerate(match_targets):
                insert_target(
                    target_id,
                    "match",
                    {
                        "competition_id": male_competition,
                        "season_id": season_id,
                        "match_id": str(position),
                    },
                    current_policy,
                    f"196{position}-01-01T00:00:00Z",
                    65,
                )
            insert_target(
                season_target,
                "season",
                {
                    "competition_id": male_competition,
                    "season_id": season_id,
                },
                current_policy,
                "1940-01-01T00:00:00Z",
            )
            insert_target(
                stats_target,
                "season_stats",
                {
                    "competition_id": male_competition,
                    "season_id": season_id,
                },
                current_policy,
                "1930-01-01T00:00:00Z",
            )
            insert_target(
                player_target,
                "player",
                {
                    "competition_id": male_competition,
                    "season_id": season_id,
                },
                current_policy,
                "1900-01-01T00:00:00Z",
                100,
            )
            insert_target(
                historical_target,
                "match",
                {
                    "competition_id": male_competition,
                    "season_id": season_id,
                },
                "historical_once",
                "1890-01-01T00:00:00Z",
                1000,
            )
            for gender, competition_id in (
                ("female", female_competition),
                ("unknown", unknown_competition),
            ):
                insert_target(
                    f"fbref:test:{gender}-match:{suffix}",
                    "match",
                    {
                        "competition_id": competition_id,
                        "season_id": season_id,
                    },
                    current_policy,
                    "1880-01-01T00:00:00Z",
                    2000,
                )

        wrapped = _RollbackOnlyConnection(connection)
        store = ControlStore(dsn, connection_factory=lambda _dsn: wrapped)
        current_policies = [current_policy]

        first = store.create_due_run_cohort(
            run_id,
            refresh_policies=current_policies,
            limit=5,
        )
        assert [item.target_id for item in first] == [
            index_target,
            *match_targets,
        ]
        assert [item.ordinal for item in first] == list(range(5))

        second = store.create_due_run_cohort(
            run_id,
            refresh_policies=current_policies,
            limit=1,
        )
        third = store.create_due_run_cohort(
            run_id,
            refresh_policies=current_policies,
            limit=1,
        )
        fourth = store.create_due_run_cohort(
            run_id,
            refresh_policies=current_policies,
            limit=1,
        )
        assert [item.target_id for item in second] == [season_target]
        assert [item.target_id for item in third] == [stats_target]
        assert [item.target_id for item in fourth] == [player_target]
        selected = {
            *(item.target_id for item in first),
            second[0].target_id,
            third[0].target_id,
            fourth[0].target_id,
        }
        assert historical_target not in selected
        assert not any("female-match" in item for item in selected)
        assert not any("unknown-match" in item for item in selected)
    finally:
        connection.rollback()
        connection.close()


def test_two_runs_cannot_admit_the_same_frontier_target_concurrently(
    isolated_postgres_uri,
):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = isolated_postgres_uri
    suffix = uuid.uuid4().hex
    run_ids = (str(uuid.uuid4()), str(uuid.uuid4()))
    target_id = f"fbref:test:concurrent-index:{suffix}"
    refresh_policy = f"concurrent-current-{suffix}"
    setup = psycopg2.connect(dsn)
    try:
        with setup.cursor() as cursor:
            for run_id in run_ids:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.crawl_run (
                        run_id, run_type, status, request_limit, byte_limit
                    ) VALUES (%s, 'current-concurrency-test', 'running', 1, 1)
                    """,
                    (run_id,),
                )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, source_ids,
                    refresh_policy, priority
                    ) VALUES (%s, 'competition_index', %s, '{}'::jsonb,
                              %s, 1000000)
                    """,
                    (
                        target_id,
                        f"https://example.invalid/{target_id}",
                        refresh_policy,
                    ),
                )
        setup.commit()

        barrier = threading.Barrier(2)

        def admit(run_id):
            barrier.wait(timeout=5)
            return ControlStore(dsn).create_due_run_cohort(
                run_id,
                refresh_policies=[refresh_policy],
                limit=1,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            admitted = list(executor.map(admit, run_ids))

        assert sum(len(items) for items in admitted) == 1
        with setup.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)
                FROM fbref_control.run_target AS target
                JOIN fbref_control.crawl_run AS run USING (run_id)
                WHERE target.target_id = %s
                  AND target.status IN ('pending', 'leased', 'retry')
                  AND run.status IN ('pending', 'running')
                """,
                (target_id,),
            )
            assert cursor.fetchone()[0] == 1
    finally:
        setup.close()


def test_publication_guard_blocks_release_or_takeover_until_write_finishes(
    isolated_postgres_uri,
):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = isolated_postgres_uri
    run_id = str(uuid.uuid4())
    source = f"fbref-test-{uuid.uuid4().hex}"
    setup = psycopg2.connect(dsn)
    blocker = psycopg2.connect(dsn)
    store = ControlStore(dsn)
    try:
        with setup.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES (%s, 'publication-guard-test', 'running', 0, 0)
                """,
                (run_id,),
            )
        setup.commit()
        store.acquire_publication_lock(
            run_id,
            dag_id="fbref-publication-guard-test",
            source=source,
            ttl_seconds=60,
        )

        with store.guard_publication_lock(run_id, source=source):
            with blocker.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '250ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    cursor.execute(
                        """
                        SELECT source FROM fbref_control.publication_lock
                        WHERE source = %s FOR UPDATE
                        """,
                        (source,),
                    )
            blocker.rollback()

        with blocker.cursor() as cursor:
            cursor.execute(
                """
                SELECT source FROM fbref_control.publication_lock
                WHERE source = %s FOR UPDATE NOWAIT
                """,
                (source,),
            )
            assert cursor.fetchone()[0] == source
        blocker.rollback()
    finally:
        blocker.rollback()
        blocker.close()
        setup.close()


def test_raw_baseline_and_attempt_set_are_durably_anchored(
    isolated_postgres_uri,
):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = isolated_postgres_uri
    run_id = str(uuid.uuid4())
    setup = psycopg2.connect(dsn)
    try:
        with setup.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES (%s, 'raw-anchor-test', 'running', 0, 0)
                """,
                (run_id,),
            )
        setup.commit()

        store = ControlStore(dsn)
        evidence = {
            "schema_version": "fbref-raw-inventory-v2",
            "raw_root_sha256": "a" * 64,
            "object_count": 0,
            "encoded_bytes": 0,
            "fingerprint_sha256": "b" * 64,
            "baseline_sha256": "c" * 64,
        }
        barrier = threading.Barrier(2)

        def anchor_same_baseline():
            barrier.wait(timeout=5)
            return ControlStore(dsn).record_raw_baseline(run_id, evidence)

        with ThreadPoolExecutor(max_workers=2) as executor:
            anchored = list(
                executor.map(lambda _item: anchor_same_baseline(), range(2))
            )
        assert sorted(item["idempotent"] for item in anchored) == [False, True]
        assert store.get_raw_baseline(run_id) == evidence
        with pytest.raises(StateConflict, match="different raw baseline"):
            store.record_raw_baseline(
                run_id, {**evidence, "baseline_sha256": "d" * 64}
            )

        sealed = store.seal_raw_fetch_attempts(run_id)
        assert sealed["successful_attempt_count"] == 0
        assert sealed["idempotent"] is False
        assert store.seal_raw_fetch_attempts(run_id)["idempotent"] is True
        assert store.claim_targets(run_id, "late-worker") == []
    finally:
        setup.close()
