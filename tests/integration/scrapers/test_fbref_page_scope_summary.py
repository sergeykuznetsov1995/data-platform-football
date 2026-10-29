"""Ownership semantics against a disposable PostgreSQL database (never Airflow)."""

import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest

from scrapers.fbref.control import ControlStore, FrontierProvenance, FrontierTarget, SeasonAlias
from scrapers.fbref.control.migrations import MIGRATIONS, bootstrap_statements
from scrapers.fbref.control.store import _FRONTIER_SCOPE_CTE
from scrapers.fbref.daily_report import page_age_line, page_age_metrics
from scrapers.fbref.daily_report_reader import PAGE_AGE_EXCLUSIONS_SQL, REGISTRY_SQL, page_ages_query

pytestmark = pytest.mark.integration
FLAGS = ("has_current_ownership", "competition_missing", "has_female", "has_unknown",
         "inactive_competition", "invalid_season")
CUTOFF = "2026-10-26T11:00:00+00:00"


# Frozen row contract: the oracle still runs the pre-aggregation SQL and Python path.
LEGACY_PAGE_AGES_SQL = """
SELECT CASE WHEN coalesce(bool_or(NOT coalesce(p.scope_summary_ready,false)),false)
  OR EXISTS (SELECT 1 FROM fbref_control.page_scope_registry_dirty)
 THEN jsonb_build_object('error','scope_summary_incomplete')::text
 ELSE coalesce(jsonb_agg(to_jsonb(p) - 'scope_summary_ready'), '[]'::jsonb)::text END
FROM (
 SELECT f.target_id,f.page_kind,f.canonical_url,f.state,f.refresh_policy,
        scope.is_complete AND NOT scope.pending AS scope_summary_ready,
        f.last_fetched_at, f.last_content_hash,
        EXISTS (SELECT 1 FROM fbref_control.dataset_manifest m
          WHERE m.target_id=f.target_id AND m.content_hash=f.last_content_hash
            AND m.dataset='__page__' AND m.parse_status='succeeded'
            AND m.persistence_status='succeeded' AND m.validation_status='succeeded'
            AND m.completed_at <= '{cutoff}'::timestamptz) AS processed,
        coalesce(scope.has_current_ownership,false)
          AND NOT coalesce(scope.competition_missing,true)
          AND NOT coalesce(scope.has_female,false)
          AND NOT coalesce(scope.has_unknown,true)
          AND NOT coalesce(scope.inactive_competition,true)
          AND NOT coalesce(scope.invalid_season,true) AS current_ownership,
        coalesce(scope.scopes, '[]'::jsonb) AS scopes
 FROM fbref_control.page_frontier f
 LEFT JOIN fbref_control.page_scope_summary scope USING (target_id)
 WHERE f.source='fbref' AND f.refresh_policy <> 'historical_once'
   AND f.state NOT IN ('dead','skipped','quarantined') AND f.created_at <= '{cutoff}'::timestamptz
) p
"""


def json_result(cursor):
    row = cursor.fetchone()
    return json.loads(next(iter(row.values())) if isinstance(row, dict) else row[0])


def report_snapshot(cursor):
    cursor.execute(REGISTRY_SQL.format(cutoff=CUTOFF))
    snapshot = json_result(cursor)
    snapshot["standings_inventory"] = []
    return snapshot


def aggregate_sql(cursor):
    return page_ages_query(report_snapshot(cursor), datetime.fromisoformat(CUTOFF))


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
                    if migration.version in (13, 15):
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
                    ("l-youth", "season", {"competition_id": "10", "season_id": "2026"}),
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
                for statement in next(m for m in MIGRATIONS if m.version == 13).statements:
                    cursor.execute(statement)
                for statement in next(m for m in MIGRATIONS if m.version == 15).statements:
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
            cursor.execute("SELECT competition_id, gender FROM fbref_control.competition_registry")
            genders = {row['competition_id']: row['gender'] for row in cursor.fetchall()}
            for row in stored:
                classes = {genders.get(scope['competition_id']) for scope in row['scopes']}
                if not classes or None in classes:
                    expected_class = 'unknown'
                elif {'male', 'female'} <= classes:
                    expected_class = 'mixed'
                elif 'unknown' in classes:
                    expected_class = 'unknown'
                else:
                    expected_class = next(iter(classes))
                assert row['gender_classified']
                assert row['gender_class'] == expected_class
            cursor.execute(LEGACY_PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
            report_rows = json_result(cursor)
            assert "z-future" not in {row["target_id"] for row in report_rows}
            # Also retain the original reference-rollup comparison for v13.
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
            snapshot = report_snapshot(cursor)
            now = datetime.fromisoformat(CUTOFF)
            expected = page_age_metrics(dict(snapshot, page_ages=baseline), now)
            assert page_age_metrics(dict(snapshot, page_ages=report_rows), now) == expected
            cursor.execute(page_ages_query(snapshot, now))
            aggregate = json_result(cursor)
            actual = page_age_metrics(dict(snapshot, page_ages=aggregate), now)
            assert actual == expected
            assert page_age_line({"page_ages": actual}) == page_age_line({"page_ages": expected})
            assert set(aggregate) == {"cutoff", "by_kind", "excluded", "standings_confirmed_seasons"}
            assert "scopes" not in json.dumps(aggregate) and "target_id" not in json.dumps(aggregate)


def test_summary_backfill_is_resumable_and_equivalent_to_old_rollup(scope_db):
    connection, store, cursor_class = scope_db
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(aggregate_sql(cursor))
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
            cursor.execute("EXPLAIN (FORMAT JSON) " + aggregate_sql(cursor))
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
                cursor.execute(aggregate_sql(cursor))
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


def test_concurrent_alias_updates_skip_locked_registry_invalidations(scope_db):
    import threading
    import psycopg2

    connection, store, cursor_class = scope_db
    finish_backfill(store)
    store.upsert_season_alias(SeasonAlias("10", "current", "2026", alias_kind="label"))
    store.upsert_frontier_target(FrontierTarget(
        "l-alias", "season", "https://example.invalid/l-alias",
        {"competition_id": "10", "season_id": "current"}, "weekly"))
    with connection:
        with connection.cursor() as cursor:
            # 99 sorts after both edited competitions, so its separate lock
            # cannot mask the original two-transaction queue-lock cycle.
            cursor.execute("""INSERT INTO fbref_control.page_scope_registry_dirty
                (source, competition_id) VALUES ('fbref','9'),('fbref','10'),('fbref','99')""")

    ready = threading.Barrier(2)
    class ConcurrentStore(ControlStore):
        def _refresh_registry_scopes(self, cursor):
            # Each alias trigger has locked its own pre-existing dirty row.
            # The old queue-wide FOR UPDATE now creates a two-transaction cycle.
            ready.wait(timeout=5)
            return super()._refresh_registry_scopes(cursor)

    def update_alias(cid):
        other = ConcurrentStore(store.db_uri, connection_factory=lambda dsn:
            psycopg2.connect(dsn, options="-c statement_timeout=5000"))
        other.upsert_season_alias(SeasonAlias(cid, "current", "2025", alias_kind="label"))

    try:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT competition_id FROM fbref_control.page_scope_registry_dirty
                WHERE competition_id='99' FOR UPDATE""")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(update_alias, cid) for cid in ("9", "10")]
            for future in futures:
                future.result(timeout=10)
    finally:
        # A skipped row must survive even if its lock holder rolls back.
        connection.rollback()

    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT competition_id, season_id FROM fbref_control.season_alias
                WHERE alias='current' ORDER BY competition_id""")
            assert cursor.fetchall() == [("10", "2025"), ("9", "2025")]
            cursor.execute("SELECT competition_id FROM fbref_control.page_scope_registry_dirty")
            assert cursor.fetchall() == [("99",)]
            cursor.execute(aggregate_sql(cursor))
            assert json.loads(cursor.fetchone()[0]) == {"error": "scope_summary_incomplete"}
    store.refresh_page_scopes()
    assert_equivalent(connection, cursor_class)


def test_page_age_aggregate_matches_rows_at_age_scope_and_standings_boundaries(scope_db):
    connection, store, cursor_class = scope_db
    now = datetime.fromisoformat(CUTOFF)
    with connection:
        with connection.cursor() as cursor:
            # Python's adult rules must remain authoritative even for male registries.
            cursor.execute("UPDATE fbref_control.competition_registry SET name='U21 League' WHERE competition_id='10'")
            for number, (kind, age, state, policy, suffix) in enumerate([
                ("season", None, "queued", "weekly", ""),
                ("season", -1, "queued", "weekly", ""),
                ("season", 0, "queued", "weekly", ""),
                ("season", 168, "queued", "weekly", ""),
                ("season", 168-1/3600, "queued", "weekly", ""),
                ("season", 168+1/3600, "queued", "weekly", ""),
                ("season", 10, "queued", "weekly", ""),
                ("season", 40, "queued", "weekly", ""),
                ("season", 100, "queued", "weekly", ""),
                ("squad", 672, "queued", "monthly", ""),
                ("squad", 0, "queued", "monthly", "/matchlogs/x"),
                ("squad", 0, "queued", "monthly", "/history?x=matchlogs"),
                ("squad", 0, "queued", "monthly", "?x=matchlogs"),
                ("player", 720, "queued", "weekly", ""),
                ("matchlog", 720-1/3600, "queued", "weekly", ""),
                ("schedule", 24, "queued", "daily", ""),
                ("season_stats", 168, "queued", "weekly", ""),
                ("season", 0, "dead", "historical_once", ""),
                ("season", 0, "skipped", "weekly", ""),
                ("season", 0, "quarantined", "weekly", ""),
                ("season", 0, "queued", "historical_once", ""),
            ]):
                target = f"metric-{number}"
                cursor.execute("""INSERT INTO fbref_control.page_frontier
                    (target_id,page_kind,canonical_url,source_ids,refresh_policy,state,created_at,
                     last_fetched_at,last_content_hash)
                    VALUES (%s,%s,%s,%s::jsonb,%s,%s,'2026-10-01',%s,'current')""",
                    (target, kind, f"https://example.invalid/en/squads/{target}{suffix}",
                     json.dumps({"competition_id": "11" if number == 11 else "9",
                                 "season_id": "current" if number != 11 else "2026"}), policy, state,
                     None if age is None else now-timedelta(hours=age)))
            # Competition pages need only an active competition; index needs no ownership.
            cursor.execute("""UPDATE fbref_control.page_frontier SET page_kind='competition',
                source_ids='{"competition_id":"9"}' WHERE target_id='i-null'""")
            for number, status, completed in ((2, "succeeded", now), (3, "failed", now),
                                               (4, "succeeded", now+timedelta(seconds=1)),
                                               (5, "succeeded", now)):
                cursor.execute("""INSERT INTO fbref_control.dataset_manifest
                    (target_id,content_hash,parser_version,dataset,availability,
                     parse_status,persistence_status,validation_status,completed_at)
                    VALUES (%s,'current','fixture','__page__','available',%s,'succeeded','succeeded',%s)""",
                    (f"metric-{number}", status, completed))
            # Duplicate successful parser versions must not multiply confirmed seasons.
            cursor.execute("""INSERT INTO fbref_control.dataset_manifest
                (target_id,content_hash,parser_version,dataset,availability,
                 parse_status,persistence_status,validation_status,completed_at)
                VALUES ('metric-2','current','second','__page__','available',
                        'succeeded','succeeded','succeeded',%s)""", (now,))
            for number, persistence, validation in ((6, "failed", "succeeded"), (7, "succeeded", "failed")):
                cursor.execute("""INSERT INTO fbref_control.dataset_manifest
                    (target_id,content_hash,parser_version,dataset,availability,
                     parse_status,persistence_status,validation_status,completed_at)
                    VALUES (%s,'current','fixture','__page__','available','succeeded',%s,%s,%s)""",
                    (f"metric-{number}", persistence, validation, now))
    finish_backfill(store)
    store.refresh_page_scopes()
    with connection:
        with connection.cursor(cursor_factory=cursor_class) as cursor:
            snapshot = report_snapshot(cursor)
            snapshot["standings_inventory"] = [
                {"target_id": f"metric-{number}", "content_hash": "old" if number == 5 else "current"}
                for number in (2, 2, 3, 4, 5, 6, 7)]
            cursor.execute(PAGE_AGE_EXCLUSIONS_SQL.replace("{cutoff}", CUTOFF))
            snapshot["page_age_exclusions"] = json_result(cursor)
            cursor.execute(LEGACY_PAGE_AGES_SQL.replace("{cutoff}", CUTOFF))
            rows = json_result(cursor)
            expected = page_age_metrics(dict(snapshot, page_ages=rows), now)
            cursor.execute(page_ages_query(snapshot, now))
            aggregate = json_result(cursor)
            actual = page_age_metrics(dict(snapshot, page_ages=aggregate), now)
            # Percentile algorithms may differ by the last floating point bit.
            assert actual.keys() == expected.keys()
            for key in actual.keys() - {"by_kind"}:
                assert actual[key] == expected[key]
            assert actual["by_kind"].keys() == expected["by_kind"].keys()
            for kind in actual["by_kind"]:
                assert actual["by_kind"][kind] == pytest.approx(expected["by_kind"][kind])
            assert actual["standings_confirmed_seasons"] == 1
            assert actual["by_kind"]["standings"]["total"] == 0
            assert actual["excluded"]["non_roster"] == 2
            # The old SELECT has no row order, hence no defined exclusion order.
            # Counts were compared above; align only their display order here.
            expected_line = dict(expected, excluded=dict(actual["excluded"]))
            assert page_age_line({"page_ages": actual}) == page_age_line({"page_ages": expected_line})
            assert set(aggregate) == {"cutoff", "by_kind", "excluded", "standings_confirmed_seasons"}
            assert len(aggregate["by_kind"]) < len(rows)
            assert "target_id" not in json.dumps(aggregate) and "scopes" not in json.dumps(aggregate)


@pytest.mark.parametrize("mutation", [
    "DELETE FROM fbref_control.page_scope_summary WHERE target_id='g-orphan'",
    "UPDATE fbref_control.page_scope_summary SET pending=true WHERE target_id='g-orphan'",
    "UPDATE fbref_control.page_scope_summary SET is_complete=false WHERE target_id='g-orphan'",
    "UPDATE fbref_control.competition_registry SET present=NOT present WHERE competition_id='9'",
])
def test_aggregate_guard_checks_even_pages_outside_report_scope(scope_db, mutation):
    connection, store, cursor_class = scope_db
    finish_backfill(store)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(mutation)
            cursor.execute(aggregate_sql(cursor))
            assert json_result(cursor) == {"error": "scope_summary_incomplete"}


def test_gender_backfill_covers_inherited_mixed_orphan_and_quarantined(scope_db):
    connection, store, cursor_class = scope_db
    finish_backfill(store)
    # Simulate the v15 upgrade over an already populated v13 summary.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE fbref_control.page_scope_summary SET gender_class='unknown', gender_classified=false")
    first = store.backfill_page_gender_classes(batch_size=2)
    assert first["processed"] == 2 and not first["done"]
    restarted = ControlStore(store.db_uri)
    while not restarted.backfill_page_gender_classes(batch_size=2)["done"]:
        pass
    assert restarted.backfill_page_gender_classes()["processed"] == 0
    with connection:
        with connection.cursor(cursor_factory=cursor_class) as cursor:
            cursor.execute("SELECT target_id, gender_class, gender_classified FROM fbref_control.page_scope_summary")
            rows = {r['target_id']: r for r in cursor.fetchall()}
            assert all(r['gender_classified'] for r in rows.values())
            assert rows['b-provenance']['gender_class'] == 'male'
            assert rows['c-shared']['gender_class'] == 'male'
            assert rows['d-female']['gender_class'] == 'female'
            assert rows['g-orphan']['gender_class'] == 'unknown'
            assert rows['k-mixed']['gender_class'] == 'mixed'
            cursor.execute("SELECT count(*) AS count FROM fbref_control.page_frontier")
            assert len(rows) == cursor.fetchone()['count']
    # New female-only provenance, without a direct ID, is classified at seeding.
    store.upsert_frontier_target(FrontierTarget('new-female', 'player', 'https://example.invalid/new-female', {}, 'weekly'))
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO fbref_control.frontier_provenance
                (provenance_id,parent_target_id,child_target_id,relation,
                 carried_competition_id,carried_season_id,parent_content_hash,parser_version)
                VALUES (%s,'h-index','new-female','link','11','2026','hash','fixture')""", (str(uuid.uuid4()),))
    # Direct SQL intentionally leaves pending; the normal writer's refresh resolves it.
    store.upsert_frontier_target(FrontierTarget('new-female', 'player', 'https://example.invalid/new-female', {}, 'weekly'))
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE fbref_control.page_frontier SET state='quarantined' WHERE target_id='new-female'")
            cursor.execute("SELECT gender_class FROM fbref_control.page_scope_summary WHERE target_id='new-female'")
            assert cursor.fetchone()[0] == 'female'
