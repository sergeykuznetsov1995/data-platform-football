"""Ownership semantics against a disposable PostgreSQL database (never Airflow)."""

import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest

from scrapers.fbref.control import ControlStore, FrontierProvenance, FrontierTarget, SeasonAlias
from scrapers.fbref.control.migrations import MIGRATIONS, bootstrap_statements
from scrapers.fbref.control.store import _FRONTIER_SCOPE_CTE
from scrapers.fbref.daily_report import page_age_metrics
from scrapers.fbref.daily_report_reader import PAGE_AGES_SQL

pytestmark = pytest.mark.integration
FLAGS = ("has_current_ownership", "competition_missing", "has_female", "has_unknown",
         "inactive_competition", "invalid_season")
CUTOFF = "2026-10-26T11:00:00+00:00"


@pytest.fixture
def scope_db():
    # Deliberately separate from the existing Airflow/FBref integration DSNs.
    dsn = os.getenv("FBREF_SCOPE_TEST_POSTGRES_URI", "")
    if not dsn:
        pytest.skip("dedicated disposable FBREF_SCOPE_TEST_POSTGRES_URI is not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2.extras import RealDictCursor
    connection = psycopg2.connect(dsn, options="-c statement_timeout=5000")
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regnamespace('fbref_control')")
        if cursor.fetchone()[0] is not None:
            connection.close()
            pytest.fail("scope fixture refuses a database with an existing fbref_control schema")
    try:
        with connection:
            with connection.cursor() as cursor:
                for statement in bootstrap_statements():
                    cursor.execute(statement)
                for migration in MIGRATIONS:
                    if migration.version == 13:
                        continue
                    for statement in migration.statements:
                        cursor.execute(statement)
                snapshot = str(uuid.uuid4())
                cursor.execute("""INSERT INTO fbref_control.registry_snapshot
                    (snapshot_id, source, fetched_at, successful) VALUES (%s,'fbref',now(),true)""", (snapshot,))
                for cid, gender in (("9", "male"), ("10", "male"), ("11", "female"), ("12", "unknown")):
                    cursor.execute("""INSERT INTO fbref_control.competition_registry
                        (competition_id, canonical_url, name, gender, classification, crawl_state,
                         first_seen_at, last_seen_at, first_snapshot_id, last_snapshot_id)
                        VALUES (%s,%s,'League',%s,'domestic_league','active',now(),now(),%s,%s)""",
                        (cid, f"https://example.invalid/competition/{cid}", gender, snapshot, snapshot))
                    for sid in ("2025", "2026"):
                        cursor.execute("""INSERT INTO fbref_control.season_registry
                            (competition_id,season_id,canonical_url,is_current,first_seen_at,last_seen_at,
                             first_snapshot_id,last_snapshot_id)
                            VALUES (%s,%s,%s,%s,now(),now(),%s,%s)""",
                            (cid, sid, f"https://example.invalid/{cid}/{sid}", sid == "2026", snapshot, snapshot))
                cursor.execute("""INSERT INTO fbref_control.season_alias
                    (source,competition_id,alias,season_id,alias_kind)
                    VALUES ('fbref','9','current','2026','label')""")
                for target, kind, ids in (
                    ("a-alias", "season", {"competition_id": "9", "season_id": "current"}),
                    ("b-provenance", "player", {}), ("c-shared", "squad", {}),
                    ("d-female", "season", {"competition_id": "11", "season_id": "2026"}),
                    ("e-unknown", "season", {"competition_id": "12", "season_id": "2026"}),
                    ("f-invalid", "season", {"competition_id": "9", "season_id": "missing"}),
                    ("g-orphan", "player", {}), ("h-index", "competition_index", {}),
                    ("i-null", "competition", {"competition_id": None}),
                    ("j-empty", "season", {"competition_id": "", "season_id": ""}),
                    ("k-mixed", "squad", {"competition_id": "9", "season_id": "2026"}),
                    ("z-future", "season", {"competition_id": "9", "season_id": "2026"}),
                ):
                    cursor.execute("""INSERT INTO fbref_control.page_frontier
                        (target_id,page_kind,canonical_url,source_ids,refresh_policy,created_at,
                         last_fetched_at,last_content_hash)
                        VALUES (%s,%s,%s,%s::jsonb,'weekly',%s,%s,'current')""",
                        (target, kind, f"https://example.invalid/{target}", json.dumps(ids),
                         "2026-10-27" if target == "z-future" else "2026-10-01", CUTOFF))
                for child, cid in (("b-provenance", "9"), ("c-shared", "9"),
                                   ("c-shared", "10"), ("k-mixed", "11")):
                    cursor.execute("""INSERT INTO fbref_control.frontier_provenance
                        (provenance_id,parent_target_id,child_target_id,relation,
                         carried_competition_id,carried_season_id,parent_content_hash,parser_version)
                        VALUES (%s,'h-index',%s,'link',%s,'2026','hash','fixture')""",
                        (str(uuid.uuid4()), child, cid))
                for statement in MIGRATIONS[-1].statements:
                    cursor.execute(statement)
        store = ControlStore(dsn)
        yield connection, store, RealDictCursor
    finally:
        connection.rollback()
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS fbref_control CASCADE")
        connection.close()


def finish_backfill(store):
    while not store.backfill_page_scopes(batch_size=3)["done"]:
        pass


def assert_equivalent(connection, cursor_class):
    with connection:
        with connection.cursor(cursor_factory=cursor_class) as cursor:
            cursor.execute(_FRONTIER_SCOPE_CTE + " SELECT * FROM scope_rollup ORDER BY target_id")
            reference = {row["target_id"]: row for row in cursor.fetchall()}
            cursor.execute("SELECT * FROM fbref_control.page_scope_summary ORDER BY target_id")
            stored = cursor.fetchall()
            for row in stored:
                expected = reference.get(row["target_id"], {})
                assert row["is_complete"] and not row["pending"]
                assert {flag: row[flag] for flag in FLAGS} == {flag: expected.get(flag) for flag in FLAGS}
            cursor.execute(_FRONTIER_SCOPE_CTE + """ SELECT target_id,
                jsonb_agg(DISTINCT jsonb_build_object('competition_id',competition_id,
                                                     'season_id',season_id)) AS scopes
                FROM canonical_scope GROUP BY target_id""")
            scopes = {row["target_id"]: row["scopes"] for row in cursor.fetchall()}
            for row in stored:
                key = lambda scope: json.dumps(scope, sort_keys=True)
                assert sorted(row["scopes"], key=key) == sorted(scopes.get(row["target_id"], []), key=key)
            cursor.execute(PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
            report_rows = json.loads(cursor.fetchone()["case"])
            assert isinstance(report_rows, list)
            assert "z-future" not in {row["target_id"] for row in report_rows}
            # Compare the final metrics as well, replacing stored ownership/scopes
            # with the reference rollup rather than reproducing its SQL.
            baseline = []
            for row in report_rows:
                flags = reference.get(row["target_id"], {})
                ownership = (bool(flags.get("has_current_ownership"))
                             and flags.get("competition_missing") is False
                             and not flags.get("has_female")
                             and flags.get("has_unknown") is False
                             and flags.get("inactive_competition") is False
                             and flags.get("invalid_season") is False)
                baseline.append(dict(row, current_ownership=ownership,
                                     scopes=scopes.get(row["target_id"], [])))
            cursor.execute("SELECT * FROM fbref_control.competition_registry")
            competitions = cursor.fetchall()
            cursor.execute("SELECT * FROM fbref_control.season_registry WHERE is_current")
            seasons = cursor.fetchall()
            snapshot = {"competitions": competitions, "seasons": seasons}
            now = datetime.fromisoformat(CUTOFF)
            assert page_age_metrics(dict(snapshot, page_ages=report_rows), now) == page_age_metrics(
                dict(snapshot, page_ages=baseline), now)


def test_summary_backfill_is_resumable_and_equivalent_to_old_rollup(scope_db):
    connection, store, cursor_class = scope_db
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
            assert json.loads(cursor.fetchone()[0]) == {"error": "scope_summary_incomplete"}
    first = store.backfill_page_scopes(batch_size=2)
    second = ControlStore(store.db_uri).backfill_page_scopes(batch_size=2)
    assert first["processed"] == second["processed"] == 2
    assert first["after_target_id"] < second["after_target_id"]
    finish_backfill(store)
    assert store.backfill_page_scopes()["processed"] == 0
    assert_equivalent(connection, cursor_class)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL enable_seqscan=off")
            cursor.execute("""EXPLAIN (FORMAT JSON) SELECT DISTINCT carried_competition_id,carried_season_id
                FROM fbref_control.frontier_provenance WHERE child_target_id='c-shared'""")
            plan = json.dumps(cursor.fetchone()[0])
            assert "frontier_provenance_child_idx" in plan and "Seq Scan" not in plan
            cursor.execute("EXPLAIN (FORMAT JSON) " + PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
            assert "frontier_provenance" not in json.dumps(cursor.fetchone()[0])


def test_alias_registry_and_source_id_changes_keep_summary_exact(scope_db):
    connection, store, cursor_class = scope_db
    finish_backfill(store)
    store.upsert_season_alias(SeasonAlias("9", "current", "2025", alias_kind="label"))
    assert_equivalent(connection, cursor_class)
    store.upsert_frontier_target(FrontierTarget("a-alias", "season", "https://example.invalid/a-alias",
                                              {"competition_id": "10", "season_id": "2026"}, "weekly"))
    assert_equivalent(connection, cursor_class)
    for sql in (
        "UPDATE fbref_control.season_registry SET is_current=false WHERE competition_id='10'",
        "UPDATE fbref_control.competition_registry SET lifecycle_state='missing_once',present=false WHERE competition_id='9'",
        "UPDATE fbref_control.competition_registry SET gender='female' WHERE competition_id='10'",
    ):
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                cursor.execute(PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
                assert json.loads(cursor.fetchone()[0]) == {"error": "scope_summary_incomplete"}
        store.refresh_page_scopes()
        assert_equivalent(connection, cursor_class)


def edge(cid, suffix=""):
    return FrontierProvenance("h-index", "c-shared", "concurrent" + suffix, "hash", "fixture",
                              carried_competition_id=cid, carried_season_id="2026")


def test_concurrent_edges_backfill_and_rollback_do_not_lose_scopes(scope_db):
    connection, store, cursor_class = scope_db
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(store.record_frontier_provenance, edge(cid, cid)) for cid in ("11", "12")]
        futures.append(executor.submit(finish_backfill, store))
        for future in futures:
            future.result(timeout=15)
    store.record_frontier_provenance(edge("11", "11"))  # Idempotent retry.
    assert_equivalent(connection, cursor_class)
    with pytest.raises(RuntimeError, match="rollback"):
        with store._transaction() as cursor:
            store.record_frontier_provenance(edge("absent"), _cursor=cursor)
            raise RuntimeError("rollback")
    assert_equivalent(connection, cursor_class)


def test_registry_refresh_waits_for_scope_writer_and_keeps_new_membership(scope_db):
    connection, store, cursor_class = scope_db
    finish_backfill(store)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with store._transaction() as cursor:
            store.record_frontier_provenance(edge("12", "registry-race"), _cursor=cursor)
            # A registry transaction can mutate rows, but cannot finish its
            # summary enumeration while this writer owns the shared gate.
            def change_registry():
                other = ControlStore(store.db_uri)
                with other._registry_scope_transaction() as registry_cursor:
                    registry_cursor.execute("""UPDATE fbref_control.competition_registry
                        SET gender='male' WHERE competition_id='12'""")
                return True
            future = executor.submit(change_registry)
            store._refresh_scope_summaries(cursor, ["c-shared"])
        assert future.result(timeout=10)
    assert_equivalent(connection, cursor_class)


def test_direct_registry_edit_cannot_disappear_from_a_locked_invalidation(scope_db):
    import threading
    import time
    import psycopg2

    connection, store, cursor_class = scope_db
    finish_backfill(store)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE fbref_control.competition_registry SET gender='female' WHERE competition_id='9'")
    refreshed, release = threading.Event(), threading.Event()
    label = "fbref-scope-dirty-race-" + uuid.uuid4().hex
    class PausingStore(ControlStore):
        def _refresh_scope_summaries(self, cursor, target_ids):
            super()._refresh_scope_summaries(cursor, target_ids)
            refreshed.set()
            assert release.wait(5)

    def direct_edit():
        other = psycopg2.connect(store.db_uri, application_name=label,
                                 options="-c statement_timeout=5000")
        try:
            with other:
                with other.cursor() as cursor:
                    cursor.execute("UPDATE fbref_control.competition_registry SET gender='male' WHERE competition_id='9'")
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        refreshing = executor.submit(PausingStore(store.db_uri).refresh_page_scopes)
        try:
            assert refreshed.wait(5)
            editing = executor.submit(direct_edit)
            deadline = time.monotonic() + 3
            waiting = False
            while time.monotonic() < deadline and not editing.done():
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT wait_event_type FROM pg_stat_activity WHERE application_name=%s", (label,))
                        row = cursor.fetchone()
                        waiting = row is not None and row[0] == "Lock"
                if waiting:
                    break
                time.sleep(.01)
            # DO NOTHING would finish the edit here, losing its invalidation
            # when the refresher subsequently DELETEs its locked queue row.
            assert waiting and not editing.done()
        finally:
            release.set()
        refreshing.result(timeout=10)
        editing.result(timeout=10)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM fbref_control.page_scope_registry_dirty")
            assert cursor.fetchone()[0] == 1
    store.refresh_page_scopes()
    assert_equivalent(connection, cursor_class)


def test_registry_observation_timestamps_do_not_invalidate_ownership(scope_db):
    connection, store, cursor_class = scope_db
    finish_backfill(store)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE fbref_control.competition_registry SET last_seen_at=clock_timestamp()")
            cursor.execute("UPDATE fbref_control.season_registry SET last_seen_at=clock_timestamp()")
            cursor.execute("UPDATE fbref_control.season_alias SET last_seen_at=clock_timestamp()")
            cursor.execute("SELECT count(*) FROM fbref_control.page_scope_registry_dirty")
            assert cursor.fetchone()[0] == 0
    assert_equivalent(connection, cursor_class)
