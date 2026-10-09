"""Real disposable PostgreSQL: ordering, SIGTERM checkpoint, scope and queue.

Requires a dedicated localhost database named fbref1328. Never uses Airflow DB.
"""

import os
import signal
import subprocess
import sys
import uuid
from urllib.parse import urlsplit

import pytest
from psycopg2.extras import Json

from scrapers.fbref.control import ControlStore
from scrapers.fbref.control.models import FrontierTarget
from scrapers.fbref.history import HistoryCampaign
from scrapers.fbref.policy import (
    PAGE_DOCUMENT_VERSION,
    TYPED_BRONZE_PARSER_VERSION,
    DISCOVERY_PARSER_VERSION,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def db():
    uri = os.environ.get("FBREF_HISTORY_TEST_POSTGRES_URI", "")
    if not uri:
        pytest.skip("Dedicated FBREF_HISTORY_TEST_POSTGRES_URI not configured")
    parsed = urlsplit(uri)
    assert (
        parsed.hostname == "127.0.0.1"
        and parsed.port == 55438
        and parsed.path == "/fbref1328"
    )
    control = ControlStore(uri)
    with control._transaction() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS fbref_control CASCADE")
    assert control.migrate() == tuple(range(1, 13))
    return control


def run(control, kind="backfill"):
    rid = control.create_run(kind, request_limit=4096, byte_limit=2048 * 1024 * 1024)
    control.start_run(rid)
    return rid


def registry(control, pairs):
    snapshot = str(uuid.uuid4())
    with control._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.registry_snapshot(snapshot_id,successful,fetched_at) "
            "VALUES (%s,true,clock_timestamp())",
            (snapshot,),
        )
        for cid in sorted({cid for cid, _ in pairs}):
            cursor.execute(
                """INSERT INTO fbref_control.competition_registry
                (competition_id,canonical_url,name,gender,classification,crawl_state,
                 first_seen_at,last_seen_at,first_snapshot_id,last_snapshot_id)
                VALUES (%s,%s,'Adult League','male','league','active',clock_timestamp(),clock_timestamp(),%s,%s)""",
                (cid, f"https://fbref.com/en/comps/{cid}", snapshot, snapshot),
            )
        for cid, sid in pairs:
            cursor.execute(
                """INSERT INTO fbref_control.season_registry
                (competition_id,season_id,canonical_url,first_seen_at,last_seen_at,first_snapshot_id,last_snapshot_id)
                VALUES(%s,%s,%s,clock_timestamp(),clock_timestamp(),%s,%s)""",
                (
                    cid,
                    sid,
                    f"https://fbref.com/en/comps/{cid}/{sid}",
                    snapshot,
                    snapshot,
                ),
            )


def target(control, cid, sid, kind, suffix, provenance=False):
    tid = f"fbref:{kind}:{suffix}"
    control.upsert_frontier_target(
        FrontierTarget(
            target_id=tid,
            page_kind=kind,
            canonical_url=f"https://fbref.com/en/test/{suffix}",
            refresh_policy="historical_once",
            source_ids={} if provenance else {"competition_id": cid, "season_id": sid},
        )
    )
    return tid


def fetched(control, rid, tid, ordinal, parsed=True):
    refresh = str(uuid.uuid4())
    with control._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.run_target(run_id,target_id,logical_refresh_id,ordinal,status) "
            "VALUES(%s,%s,%s,%s,'succeeded')",
            (rid, tid, refresh, ordinal),
        )
        cursor.execute(
            """INSERT INTO fbref_control.fetch_attempt
            (attempt_id,run_id,target_id,logical_refresh_id,attempt_number,claim_token,lease_epoch,status,
             content_hash,raw_manifest_key,http_status,finished_at)
            VALUES(%s,%s,%s,%s,1,%s,1,'succeeded','hash','raw-proof',200,clock_timestamp())""",
            (str(uuid.uuid4()), rid, tid, refresh, str(uuid.uuid4())),
        )
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='fetched',last_content_hash='hash',"
            "last_fetched_at=clock_timestamp() WHERE target_id=%s",
            (tid,),
        )
        if parsed:
            cursor.execute(
                """INSERT INTO fbref_control.observation_processing
                (logical_refresh_id,parser_version,typed_parser_version,stateful_parser_version,target_id,content_hash,
                 status,generic_status,typed_status,stateful_status,validation_status,completed_at)
                VALUES(%s,%s,%s,%s,%s,'hash','succeeded','succeeded','succeeded','succeeded','succeeded',clock_timestamp())""",
                (
                    refresh,
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    DISCOVERY_PARSER_VERSION,
                    tid,
                ),
            )
    return refresh


def test_checkpoint_requires_descendant_typed_generic_and_stateful_proof(db):
    registry(db, [("1000", "2026-2027"), ("1001", "2026"), ("1000", "2017-2018")])
    rid = run(db)
    root = target(db, "1000", "2026-2027", "season", "root")
    child = target(db, "1000", "2026-2027", "match", "child", provenance=True)
    with db._transaction() as cursor:
        cursor.execute(
            """INSERT INTO fbref_control.frontier_provenance
            (provenance_id,parent_target_id,child_target_id,relation,carried_competition_id,carried_season_id,
             parent_content_hash,parser_version) VALUES(%s,%s,%s,'match','1000','2026-2027','hash',%s)""",
            (str(uuid.uuid4()), root, child, DISCOVERY_PARSER_VERSION),
        )
    fetched(db, rid, root, 0)
    refresh = fetched(db, rid, child, 1, parsed=False)
    campaign = HistoryCampaign(db)
    summary = campaign.reconcile()
    assert summary["years"]["2026"]["in_progress"] == 1
    assert campaign.select(rid)[0]["competition_id"] == "1000"
    with db._transaction() as cursor:
        cursor.execute(
            """INSERT INTO fbref_control.observation_processing
            (logical_refresh_id,parser_version,typed_parser_version,stateful_parser_version,target_id,content_hash,
             status,generic_status,typed_status,stateful_status,validation_status)
            VALUES(%s,%s,%s,%s,%s,'hash','failed','succeeded','failed','succeeded','failed')""",
            (
                refresh,
                PAGE_DOCUMENT_VERSION,
                TYPED_BRONZE_PARSER_VERSION,
                DISCOVERY_PARSER_VERSION,
                child,
            ),
        )
    assert campaign.reconcile()["years"]["2026"]["in_progress"] == 1
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.observation_processing SET status='succeeded',typed_status='succeeded',"
            "validation_status='succeeded' WHERE logical_refresh_id=%s",
            (refresh,),
        )
    assert campaign.reconcile()["years"]["2026"]["closed"] == 1
    assert campaign.select(run(db))[0]["competition_id"] == "1001"


def test_real_sigterm_then_fresh_worker_resumes_the_pinned_season_and_raw(db):
    registry(db, [("1000", "2026-2027"), ("1001", "2026-2027"), ("1000", "2017-2018")])
    rid = run(db)
    root = target(db, "1000", "2026-2027", "season", "root")
    child = target(db, "1000", "2026-2027", "match", "child")
    fetched(db, rid, root, 0)
    fetched(db, rid, child, 1, parsed=False)
    code = """import sys,time
from scrapers.fbref.control import ControlStore
from scrapers.fbref.history import HistoryCampaign
campaign=HistoryCampaign(ControlStore(sys.argv[1]))
campaign.reconcile()
campaign.select(sys.argv[2])
print('committed',flush=True)
time.sleep(300)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", code, db.db_uri, rid], stdout=subprocess.PIPE, text=True
    )
    try:
        assert process.stdout.readline().strip() == "committed"
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=10) == -signal.SIGTERM
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
    db.finish_run(rid, succeeded=False)
    fresh = ControlStore(db.db_uri)
    next_run = run(fresh)
    campaign = HistoryCampaign(fresh)
    assert campaign.reconcile()["years"]["2026"]["in_progress"] == 1
    assert campaign.select(next_run)[0]["season_id"] == "2026-2027"
    assert campaign.select(next_run)[0]["competition_id"] == "1000"
    raw = fresh.list_unprocessed_fetches(
        run_type="backfill",
        parser_version=PAGE_DOCUMENT_VERSION,
        typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
        stateful_parser_version=DISCOVERY_PARSER_VERSION,
        page_kinds=["season", "match"],
        history_run_id=next_run,
    )
    assert [item["target_id"] for item in raw] == [child]
    cohort = fresh.create_due_run_cohort(
        next_run, page_kinds=["season", "match"], refresh_policies=["historical_once"]
    )
    assert cohort == []  # fetched raw is recovered, completed root is never requeued


def test_campaign_scope_blocks_older_due_pages_and_includes_provenance(db):
    registry(db, [("1000", "2026-2027"), ("1001", "2026-2027"), ("1000", "2017-2018")])
    root = target(db, "1000", "2026-2027", "season", "root")
    old = target(db, "1000", "2017-2018", "match", "old")
    target(db, "1001", "2026-2027", "match", "other")
    campaign = HistoryCampaign(db)
    campaign.reconcile()
    rid = run(db)
    campaign.select(rid)
    cohort = db.create_due_run_cohort(
        rid, page_kinds=["season", "match"], refresh_policies=["historical_once"]
    )
    assert [item.target_id for item in cohort] == [root]
    assert old not in [item.target_id for item in cohort]


def test_busy_lock_queues_current_before_history_and_terminal_waiters_do_not_block(db):
    owner = run(db)
    history = run(db)
    current = run(db, "current")
    assert db.acquire_publication_lock(owner, dag_id="owner", ttl_seconds=1200)[
        "acquired"
    ]
    assert db.acquire_publication_lock(
        history, dag_id="history", ttl_seconds=1200, queue=True
    )["queued"]
    assert db.acquire_publication_lock(
        current, dag_id="dag_ingest_fbref", ttl_seconds=1200, queue=True
    )["queued"]
    db.release_publication_lock(owner)
    assert (
        db.acquire_publication_lock(
            history, dag_id="history", ttl_seconds=1200, queue=True
        )["acquired"]
        is False
    )
    assert db.acquire_publication_lock(
        current, dag_id="dag_ingest_fbref", ttl_seconds=1200, queue=True
    )["acquired"]
    db.release_publication_lock(current)
    assert db.acquire_publication_lock(
        history, dag_id="history", ttl_seconds=1200, queue=True
    )["acquired"]


def test_unknown_and_failed_parse_measurement_refuses_admission(db):
    campaign = HistoryCampaign(db)
    with pytest.raises(RuntimeError, match="No successful measured"):
        campaign.page_seconds()
    registry(db, [("1000", "2026-2027")])
    rid = run(db)
    tid = target(db, "1000", "2026-2027", "season", "root")
    fetched(db, rid, tid, 0, parsed=False)
    with pytest.raises(RuntimeError, match="No successful measured"):
        campaign.page_seconds()


def test_missing_adult_edition_blocks_deeper_years_until_source_discovery(db):
    registry(db, [("1000", "2026-2027"), ("1001", "2017-2018")])
    campaign = HistoryCampaign(db)
    rid = run(db)
    root = target(db, "1000", "2026-2027", "season", "root")
    fetched(db, rid, root, 0)
    summary = campaign.reconcile()
    assert summary["years"]["2026"]["missing"] == 1
    assert campaign.select(run(db)) == []


def test_discontinued_current_tournament_keeps_eligible_history_without_current_pages(
    db,
):
    registry(db, [("1000", "2017-2018")])
    with db._transaction() as cursor:
        cursor.execute("UPDATE fbref_control.season_registry SET is_current=true")
        cursor.execute(
            "UPDATE fbref_control.competition_registry SET crawl_state='skipped',metadata=%s::jsonb "
            "WHERE competition_id='1000'",
            (
                Json(
                    {
                        "current_scope_lifecycle": "discontinued",
                        "current_scope_reason": "source_catalog_ended",
                        "first_season": "1930",
                        "last_season": "2017-2018",
                    }
                ),
            ),
        )
    assert db.eligible_competitions() == []
    assert len(db.eligible_historical_competitions()) == 1
    historical = target(db, "1000", "2017-2018", "season", "historical")
    current = target(db, "1000", "2017-2018", "schedule", "current")
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET refresh_policy='six_hourly' WHERE target_id=%s",
            (current,),
        )
    campaign = HistoryCampaign(db)
    campaign.reconcile()
    rid = run(db)
    assert campaign.select(rid)[0]["season_id"] == "2017-2018"
    db.reconcile_frontier_scope()
    cohort = db.create_due_run_cohort(
        rid, page_kinds=["season", "schedule"], refresh_policies=["historical_once"]
    )
    assert [item.target_id for item in cohort] == [historical]
    assert db.get_frontier_target(current)["state"] == "quarantined"


def test_actual_fetch_plus_parse_timing_is_required_and_conservative(db):
    campaign = HistoryCampaign(db)
    rid = run(db)
    with pytest.raises(ValueError, match="fetch and parse"):
        campaign.record_timing(
            rid, {"fetch": {"claimed": 1, "wall_ms": 1000}, "parse": {"cohort_size": 1}}
        )
    campaign.record_timing(
        rid,
        {
            "fetch": {"claimed": 1, "wall_ms": 60000},
            "parse": {"cohort_size": 1, "wall_ms": 600000},
        },
    )
    assert campaign.page_seconds() == 660


def test_failed_queued_current_cannot_starve_history(db):
    owner = run(db)
    history = run(db)
    current = run(db, "current")
    db.acquire_publication_lock(owner, dag_id="owner", ttl_seconds=1200)
    db.acquire_publication_lock(history, dag_id="history", ttl_seconds=1200, queue=True)
    db.acquire_publication_lock(current, dag_id="current", ttl_seconds=1200, queue=True)
    db.finish_run(current, succeeded=False)
    db.release_publication_lock(owner)
    assert db.acquire_publication_lock(
        history, dag_id="history", ttl_seconds=1200, queue=True
    )["acquired"]


def test_alias_carried_raw_is_recovered_without_a_new_paid_claim(db):
    registry(db, [("1000", "2026-2027")])
    root = target(db, "1000", "2026-2027", "season", "root")
    child = target(db, "1000", "2026/27", "match", "alias-child")
    with db._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.season_alias(competition_id,alias,season_id) "
            "VALUES('1000','2026/27','2026-2027')"
        )
    old_run = run(db)
    fetched(db, old_run, root, 0)
    fetched(db, old_run, child, 1, parsed=False)
    db.finish_run(old_run, succeeded=False)
    campaign = HistoryCampaign(db)
    assert campaign.reconcile()["years"]["2026"]["in_progress"] == 1
    next_run = run(db)
    campaign.select(next_run)
    rows = db.list_unprocessed_fetches(
        run_type="backfill",
        parser_version=PAGE_DOCUMENT_VERSION,
        typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
        stateful_parser_version=DISCOVERY_PARSER_VERSION,
        history_run_id=next_run,
    )
    assert [row["target_id"] for row in rows] == [child]
    assert (
        db.create_due_run_cohort(next_run, refresh_policies=["historical_once"]) == []
    )


def catalog_proof(db, cid, edition_ids, successful=True):
    rid = run(db, "current")
    tid = target(db, cid, edition_ids[0], "competition", "catalog")
    refresh = fetched(db, rid, tid, 0)
    with db._transaction() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM fbref_control.fetch_attempt WHERE logical_refresh_id=%s",
            (refresh,),
        )
        attempt_id = (
            lambda row: next(iter(row.values())) if isinstance(row, dict) else row[0]
        )(cursor.fetchone())
        snapshot = str(uuid.uuid4())
        metadata = {
            "page_kind": "competition",
            "competition_id": cid,
            "history_raw": {
                "attempt_id": str(attempt_id),
                "target_id": tid,
                "manifest_key": "raw-proof",
                "logical_refresh_id": refresh,
                "content_hash": "hash",
            },
        }
        cursor.execute(
            "INSERT INTO fbref_control.registry_snapshot(snapshot_id,run_id,successful,fetched_at,content_hash,metadata) "
            "VALUES(%s,%s,true,clock_timestamp(),'hash',%s::jsonb)",
            (snapshot, rid, Json(metadata)),
        )
        for sid in edition_ids:
            cursor.execute(
                "INSERT INTO fbref_control.snapshot_season(snapshot_id,source,competition_id,season_id) "
                "VALUES(%s,'fbref',%s,%s)",
                (snapshot, cid, sid),
            )
        cursor.execute(
            "UPDATE fbref_control.competition_registry SET metadata=%s::jsonb WHERE competition_id=%s",
            (
                Json(
                    {
                        "first_season": "1930",
                        "last_season": edition_ids[0],
                        "advertised_current_season_id": edition_ids[0],
                    }
                ),
                cid,
            ),
        )
        if not successful:
            cursor.execute(
                "UPDATE fbref_control.observation_processing SET stateful_status='failed',status='failed' "
                "WHERE logical_refresh_id=%s",
                (refresh,),
            )
    return snapshot


def test_authoritative_world_cup_catalog_allows_2022_after_2026_without_fictional_2025(
    db,
):
    registry(db, [("1000", "2026"), ("1000", "2022"), ("1000", "2018")])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET is_current=true WHERE season_id='2026'"
        )
    snapshot = catalog_proof(db, "1000", ["2026", "2022", "2018"])
    campaign = HistoryCampaign(db)
    summary = campaign.reconcile()
    assert summary["years"]["2025"] == {"unavailable": 1}
    assert campaign.select(run(db))[0]["season_id"] == "2022"
    with db._transaction() as cursor:
        cursor.execute(
            "SELECT catalog_snapshot_id FROM fbref_control.history_campaign_season "
            "WHERE season_id='missing:2025'"
        )
        assert (
            str(
                (
                    lambda row: (
                        next(iter(row.values())) if isinstance(row, dict) else row[0]
                    )
                )(cursor.fetchone())
            )
            == snapshot
        )


def test_incomplete_catalog_parse_cannot_hide_a_missing_edition(db):
    registry(db, [("1000", "2026"), ("1000", "2022")])
    catalog_proof(db, "1000", ["2026", "2022"], successful=False)
    summary = HistoryCampaign(db).reconcile()
    assert summary["years"]["2025"] == {"missing": 1}


def test_catalog_advertised_available_edition_with_unhealthy_registry_remains_blocked(
    db,
):
    registry(db, [("1000", "2026"), ("1000", "2025"), ("1000", "2022")])
    catalog_proof(db, "1000", ["2026", "2025", "2022"])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET is_current=true WHERE season_id='2026'"
        )
        cursor.execute(
            "UPDATE fbref_control.season_registry SET present=false,lifecycle_state='missing_once' WHERE season_id='2025'"
        )
    campaign = HistoryCampaign(db)
    assert campaign.reconcile()["years"]["2025"] == {"missing": 1}
    assert campaign.select(run(db)) == []


def test_direct_match_only_season_seeds_match_and_closes_without_a_fabricated_season_root(
    db,
):
    from unittest.mock import MagicMock
    from scrapers.fbref.pipeline import FBrefPipeline, PipelineSettings

    registry(db, [("1000", "2026")])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET canonical_url=%s,metadata=%s::jsonb",
            (
                "https://fbref.com/en/matches/abcd1234/Final",
                Json({"direct_match_only": True}),
            ),
        )
    campaign = HistoryCampaign(db)
    campaign.reconcile()
    rid = run(db)
    selected = campaign.select(rid)
    pipeline = FBrefPipeline(
        db, MagicMock(), generic_writer=MagicMock(), typed_adapter=MagicMock()
    )
    pipeline.seed_historical_seasons(
        run_id=rid,
        settings=PipelineSettings(run_type="backfill", shard_size=1),
        seasons=selected,
    )
    tid = "fbref:match:abcd1234"
    assert db.get_frontier_target(tid)["page_kind"] == "match"
    with db._transaction() as cursor:
        cursor.execute(
            "SELECT logical_refresh_id FROM fbref_control.run_target WHERE run_id=%s AND target_id=%s",
            (rid, tid),
        )
        refresh = (
            lambda row: next(iter(row.values())) if isinstance(row, dict) else row[0]
        )(cursor.fetchone())
        cursor.execute(
            "UPDATE fbref_control.run_target SET status='succeeded' WHERE run_id=%s",
            (rid,),
        )
        cursor.execute(
            "INSERT INTO fbref_control.fetch_attempt(attempt_id,run_id,target_id,logical_refresh_id,attempt_number,claim_token,lease_epoch,status,content_hash,raw_manifest_key,http_status) "
            "VALUES(%s,%s,%s,%s,1,%s,1,'succeeded','hash','raw-proof',200)",
            (str(uuid.uuid4()), rid, tid, refresh, str(uuid.uuid4())),
        )
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='fetched',last_content_hash='hash' WHERE target_id=%s",
            (tid,),
        )
        cursor.execute(
            "INSERT INTO fbref_control.observation_processing(logical_refresh_id,parser_version,typed_parser_version,stateful_parser_version,target_id,content_hash,status,generic_status,typed_status,stateful_status,validation_status) "
            "VALUES(%s,%s,%s,%s,%s,'hash','succeeded','succeeded','succeeded','succeeded','succeeded')",
            (
                refresh,
                PAGE_DOCUMENT_VERSION,
                TYPED_BRONZE_PARSER_VERSION,
                DISCOVERY_PARSER_VERSION,
                tid,
            ),
        )
    assert campaign.reconcile()["years"]["2026"] == {"closed": 1}


def test_legacy_janitor_cannot_bypass_a_queued_current_between_sensor_pokes(db):
    from scrapers.fbref.control import StateConflict

    owner = run(db)
    current = run(db, "current")
    janitor = run(db, "maintenance")
    db.acquire_publication_lock(owner, dag_id="history", ttl_seconds=1200)
    assert db.acquire_publication_lock(
        current, dag_id="dag_ingest_fbref", ttl_seconds=1200, queue=True
    )["queued"]
    db.release_publication_lock(owner)
    with pytest.raises(StateConflict, match="locked by another control run"):
        db.acquire_publication_lock(
            janitor, dag_id="dag_iceberg_maintenance_daily", ttl_seconds=3600
        )
    assert db.acquire_publication_lock(
        current, dag_id="dag_ingest_fbref", ttl_seconds=1200, queue=True
    )["acquired"]
    db.release_publication_lock(current)
    assert db.acquire_publication_lock(
        janitor, dag_id="dag_iceberg_maintenance_daily", ttl_seconds=3600
    )["acquired"]


def test_committed_history_owner_retry_is_idempotent_while_current_is_queued(db):
    history = run(db)
    current = run(db, "current")
    assert db.acquire_publication_lock(
        history, dag_id="history", ttl_seconds=1200, queue=True
    )["acquired"]
    assert db.acquire_publication_lock(
        current, dag_id="current", ttl_seconds=1200, queue=True
    )["queued"]
    retry = db.acquire_publication_lock(
        history, dag_id="history", ttl_seconds=1200, queue=True
    )
    assert retry["idempotent"] and not retry["acquired"]
    db.release_publication_lock(history)
    assert db.acquire_publication_lock(
        current, dag_id="current", ttl_seconds=1200, queue=True
    )["acquired"]


@pytest.mark.parametrize("current", [False, True])
def test_proven_alias_to_current_or_closed_canonical_removes_persisted_phantom_barrier(
    db, current
):
    registry(db, [("1000", "2026"), ("1000", "2026-2027"), ("1000", "2025-2026")])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET canonical_url=canonical_url||'#superseded:2026' WHERE season_id='2026'"
        )
        if current:
            cursor.execute(
                "UPDATE fbref_control.season_registry SET is_current=true WHERE season_id='2026-2027'"
            )
    campaign = HistoryCampaign(db)
    assert campaign.reconcile()["years"]["2026"]["missing"] == 1
    snapshot = catalog_proof(db, "1000", ["2026-2027", "2025-2026"])
    with db._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.season_alias(competition_id,alias,season_id,alias_kind,last_snapshot_id) "
            "VALUES('1000','2026','2026-2027','label',%s)",
            (snapshot,),
        )
    if not current:
        rid = run(db)
        canonical_root = target(db, "1000", "2026-2027", "season", "canonical-root")
        fetched(db, rid, canonical_root, 0)
        parked_root = target(db, "1000", "2026", "season", "parked-root")
        with db._transaction() as cursor:
            cursor.execute(
                "UPDATE fbref_control.page_frontier SET canonical_url=canonical_url||'#superseded:canonical' "
                "WHERE target_id=%s",
                (parked_root,),
            )
    summary = campaign.reconcile()
    assert summary["years"]["2026"] == {"current_owned" if current else "closed": 1}
    assert campaign.select(run(db))[0]["season_id"] == "2025-2026"


def test_discontinued_history_does_not_contaminate_current_or_publication_freshness(db):
    registry(db, [("1000", "2017-2018"), ("1001", "2026-2027"), ("1001", "2025-2026")])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET is_current=true WHERE competition_id='1000' OR season_id='2026-2027'"
        )
        cursor.execute(
            "UPDATE fbref_control.competition_registry SET crawl_state='skipped',metadata=%s::jsonb WHERE competition_id='1000'",
            (
                Json(
                    {
                        "current_scope_lifecycle": "discontinued",
                        "current_scope_reason": "source_catalog_ended",
                    }
                ),
            ),
        )
    historical = [
        target(db, "1000", "2017-2018", kind, "retired-" + kind)
        for kind in ["season", "schedule", "season_stats", "standings", "squad"]
    ]
    active = target(
        db, "1001", "2026-2027", "season", "active-misclassified-historical"
    )
    noncurrent = target(db, "1001", "2025-2026", "schedule", "noncurrent")
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='fetched',last_content_hash='hash',"
            "last_fetched_at=clock_timestamp()-interval '30 days' WHERE target_id=ANY(%s)",
            (historical + [active, noncurrent],),
        )
    current_run = run(db, "current")
    summary = db.get_run_summary(
        current_run,
        parser_version=PAGE_DOCUMENT_VERSION,
        typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
        stateful_parser_version=DISCOVERY_PARSER_VERSION,
    )
    assert summary["current_scope_freshness"]["total_targets"] == 1
    assert summary["current_scope_freshness"]["stale_targets"] == 1
    assert summary["publication_scope_freshness"]["total_targets"] == 1
    assert summary["publication_scope_freshness"]["stale_targets"] == 1
    # A real active current page with the wrong policy remains visible above;
    # noncurrent and discontinued history are excluded from the meter only.
    historical_run = run(db)
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='queued' WHERE target_id=ANY(%s)",
            (historical,),
        )
    cohort = db.create_due_run_cohort(
        historical_run,
        page_kinds=["season", "schedule", "season_stats", "standings", "squad"],
        refresh_policies=["historical_once"],
    )
    assert {item.target_id for item in cohort} >= set(historical)


@pytest.mark.parametrize(
    "kind", ["season", "schedule", "season_stats", "standings", "squad"]
)
def test_shared_historical_page_requires_one_active_current_scope_for_all_freshness_counters(
    db, kind
):
    registry(db, [("1000", "2017-2018"), ("1001", "2017-2018"), ("1001", "2026-2027")])
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.season_registry SET is_current=true WHERE competition_id='1000' OR season_id='2026-2027'"
        )
        cursor.execute(
            "UPDATE fbref_control.competition_registry SET crawl_state='skipped',metadata=%s::jsonb WHERE competition_id='1000'",
            (
                Json(
                    {
                        "current_scope_lifecycle": "discontinued",
                        "current_scope_reason": "source_catalog_ended",
                    }
                ),
            ),
        )
    retired = target(db, "1000", "2017-2018", "match", "retired-parent")
    active_current = target(db, "1001", "2026-2027", "match", "current-parent")
    shared = target(db, "1001", "2017-2018", kind, "shared-page")
    with db._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.frontier_provenance(provenance_id,parent_target_id,child_target_id,relation,carried_competition_id,carried_season_id,parent_content_hash,parser_version) "
            "VALUES(%s,%s,%s,'child','1000','2017-2018','hash',%s)",
            (str(uuid.uuid4()), retired, shared, DISCOVERY_PARSER_VERSION),
        )
        # Historical match parents prove provenance and are outside current SLA.
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='quarantined' WHERE target_id=ANY(%s)",
            ([retired, active_current],),
        )
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='queued',last_content_hash='hash',last_fetched_at=clock_timestamp()-interval '30 days' WHERE target_id=%s",
            (shared,),
        )
    current_run = run(db, "current")

    def assert_meter(total, fresh):
        from scrapers.fbref.policy import PUBLICATION_FRESHNESS_PAGE_KINDS

        summary = db.get_run_summary(
            current_run,
            parser_version=PAGE_DOCUMENT_VERSION,
            typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
            stateful_parser_version=DISCOVERY_PARSER_VERSION,
        )
        for name, included in [
            ("current_scope_freshness", True),
            ("publication_scope_freshness", kind in PUBLICATION_FRESHNESS_PAGE_KINDS),
        ]:
            count = total if included else 0
            fresh_count = fresh if included else 0
            assert summary[name] == {
                "total_targets": count,
                "fresh_targets": fresh_count,
                "stale_targets": count - fresh_count,
                "never_fetched_targets": 0,
                "stale_never_fetched_targets": 0,
                "aged_targets": count - fresh_count,
                "all_within_sla": count == fresh_count,
            }
        observed = summary["freshness_by_page_kind"].get(kind)
        if total:
            assert observed["total_targets"] == total
            assert observed["fresh_targets"] == fresh
            assert observed["stale_targets"] == total - fresh
            assert observed["aged_targets"] == total - fresh
        else:
            assert observed is None

    # Active/noncurrent and discontinued/current cannot combine into ownership.
    assert_meter(0, 0)
    cohort = db.create_due_run_cohort(
        run(db), page_kinds=[kind], refresh_policies=["historical_once"]
    )
    assert shared in {item.target_id for item in cohort}
    with db._transaction() as cursor:
        cursor.execute(
            "INSERT INTO fbref_control.frontier_provenance(provenance_id,parent_target_id,child_target_id,relation,carried_competition_id,carried_season_id,parent_content_hash,parser_version) "
            "VALUES(%s,%s,%s,'child','1001','2026-2027','hash',%s)",
            (str(uuid.uuid4()), active_current, shared, DISCOVERY_PARSER_VERSION),
        )
    # A real active/current witness remains visible despite the wrong policy.
    assert_meter(1, 0)
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='fetched',last_fetched_at=clock_timestamp() WHERE target_id=%s",
            (shared,),
        )
    assert_meter(1, 1)


def test_active_competition_level_without_season_keeps_current_freshness_ownership(db):
    registry(db, [("1000", "2017-2018")])
    competition = target(db, "1000", None, "competition", "competition-no-season")
    with db._transaction() as cursor:
        cursor.execute(
            "UPDATE fbref_control.page_frontier SET state='fetched',last_fetched_at=clock_timestamp() WHERE target_id=%s",
            (competition,),
        )
    summary = db.get_run_summary(
        run(db, "current"),
        parser_version=PAGE_DOCUMENT_VERSION,
        typed_parser_version=TYPED_BRONZE_PARSER_VERSION,
        stateful_parser_version=DISCOVERY_PARSER_VERSION,
    )
    assert summary["current_scope_freshness"]["total_targets"] == 1
    assert summary["current_scope_freshness"]["fresh_targets"] == 1
    assert summary["publication_scope_freshness"]["total_targets"] == 1
    assert summary["publication_scope_freshness"]["fresh_targets"] == 1
