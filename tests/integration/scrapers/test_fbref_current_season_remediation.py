"""Real-PostgreSQL safety tests for FBref current-season remediation."""

from __future__ import annotations

import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from scrapers.fbref.control import (
    ControlStore,
    CurrentSeasonRemediationEvidence,
)
from scrapers.fbref.pipeline import (
    CurrentSeasonRemediationItem,
    FBrefPipeline,
    ParseWaveError,
)
from scrapers.fbref.raw_store import (
    RawPageStore,
    competition_index_target,
    competition_page_target,
)


pytestmark = pytest.mark.integration


def _postgres_uri() -> str:
    uri = os.getenv("FBREF_TEST_POSTGRES_URI", "").strip()
    if not uri:
        pytest.skip("FBREF_TEST_POSTGRES_URI is not configured")
    return uri.replace("postgresql+psycopg2://", "postgresql://", 1)


def _connect(psycopg2, dsn: str, application_name: str):
    return psycopg2.connect(
        dsn,
        application_name=application_name,
        options="-c statement_timeout=5000",
    )


def _wait_for_lock(admin, labels, futures) -> None:
    deadline = time.monotonic() + 3
    waiting = set()
    while time.monotonic() < deadline:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                SELECT application_name
                FROM pg_stat_activity
                WHERE application_name = ANY(%s::text[])
                  AND wait_event_type = 'Lock'
                """,
                (list(labels),),
            )
            waiting = {row[0] for row in cursor.fetchall()}
        admin.rollback()
        if waiting == set(labels) or all(future.done() for future in futures):
            break
        time.sleep(0.02)
    assert waiting == set(labels), {
        "waiting": waiting,
        "early": [repr(f.exception()) for f in futures if f.done()],
    }


def _seed_exact_evidence(connection, *, competition_id: str):
    index_run_id = str(uuid.uuid4())
    history_run_id = str(uuid.uuid4())
    writer_run_id = str(uuid.uuid4())
    index_snapshot_id = str(uuid.uuid4())
    index_refresh_id = str(uuid.uuid4())
    history_refresh_id = str(uuid.uuid4())
    index_attempt_id = str(uuid.uuid4())
    history_attempt_id = str(uuid.uuid4())
    index_target_id = f"fbref:test-index:{uuid.uuid4()}"
    history_target_id = f"fbref:test-history:{uuid.uuid4()}"
    index_manifest = f"manifests/fetches/{index_refresh_id}.json"
    history_manifest = f"manifests/fetches/{history_refresh_id}.json"
    advertised_href = f"https://fbref.com/en/comps/{competition_id}/Current-Stats"
    raw = {
        "attempt_id": index_attempt_id,
        "content_hash": "index-hash",
        "logical_refresh_id": index_refresh_id,
        "manifest_key": index_manifest,
        "target_id": index_target_id,
    }
    index = {
        "schema_version": "fbref-current-season-index-evidence-v1",
        "snapshot_id": index_snapshot_id,
        "run_id": index_run_id,
        "content_hash": "index-hash",
        "raw": raw,
        "advertised": {
            "label": "2026",
            "href": advertised_href,
            "season_id": "2026",
        },
    }
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES
                  (%s, 'index-test', 'succeeded', 1, 1024),
                  (%s, 'history-test', 'succeeded', 1, 1024),
                  (%s, 'writer-test', 'running', 1, 1024)
                """,
                (index_run_id, history_run_id, writer_run_id),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, source_ids,
                    refresh_policy, state, last_content_hash
                ) VALUES
                  (%s, 'competition_index', %s, '{}'::jsonb,
                   'daily', 'fetched', 'index-hash'),
                  (%s, 'competition', %s, %s::jsonb,
                   'weekly', 'fetched', 'history-hash')
                """,
                (
                    index_target_id,
                    f"https://example.invalid/{index_target_id}",
                    history_target_id,
                    f"https://fbref.com/en/comps/{competition_id}/history/x",
                    json.dumps({"competition_id": competition_id}),
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.fetch_attempt (
                    attempt_id, run_id, target_id, logical_refresh_id,
                    attempt_number, claim_token, lease_epoch, status,
                    content_hash, raw_manifest_key,
                    finished_at
                ) VALUES
                  (%s, %s, %s, %s, 1, %s, 1,
                   'succeeded', 'index-hash', %s,
                   clock_timestamp()),
                  (%s, %s, %s, %s, 1, %s, 1,
                   'succeeded', 'history-hash', %s,
                   clock_timestamp())
                """,
                (
                    index_attempt_id,
                    index_run_id,
                    index_target_id,
                    index_refresh_id,
                    str(uuid.uuid4()),
                    index_manifest,
                    history_attempt_id,
                    history_run_id,
                    history_target_id,
                    history_refresh_id,
                    str(uuid.uuid4()),
                    history_manifest,
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.registry_snapshot (
                    snapshot_id, run_id, source, content_hash, successful,
                    fetched_at, metadata
                ) VALUES (%s, %s, 'fbref', 'index-hash', true,
                          clock_timestamp(), %s::jsonb)
                """,
                (
                    index_snapshot_id,
                    index_run_id,
                    json.dumps(
                        {
                            "page_kind": "competition_index",
                            "raw": raw,
                        }
                    ),
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.competition_registry (
                    source, competition_id, canonical_url, name, gender,
                    classification, lifecycle_state, crawl_state, present,
                    first_seen_at, last_seen_at, first_snapshot_id,
                    last_snapshot_id, metadata
                ) VALUES (
                    'fbref', %s, %s, 'Concurrency Test', 'male',
                    'other:national_team', 'present', 'active', true,
                    clock_timestamp(), clock_timestamp(), %s, %s, %s::jsonb
                )
                """,
                (
                    competition_id,
                    f"https://fbref.com/en/comps/{competition_id}/history/x",
                    index_snapshot_id,
                    index_snapshot_id,
                    json.dumps(
                        {
                            "last_season": "2026",
                            "last_season_url": advertised_href,
                            "advertised_current_season_id": "2026",
                            "current_season_index": index,
                        }
                    ),
                ),
            )
    return CurrentSeasonRemediationEvidence(
        competition_id=competition_id,
        advertised_label="2026",
        advertised_href=advertised_href,
        advertised_season_id="2026",
        index_snapshot_id=index_snapshot_id,
        index_run_id=index_run_id,
        index_attempt_id=index_attempt_id,
        index_target_id=index_target_id,
        index_logical_refresh_id=index_refresh_id,
        index_content_hash="index-hash",
        index_raw_manifest_key=index_manifest,
        history_run_id=history_run_id,
        history_attempt_id=history_attempt_id,
        history_target_id=history_target_id,
        history_logical_refresh_id=history_refresh_id,
        history_content_hash="history-hash",
        history_raw_manifest_key=history_manifest,
    ), writer_run_id


def _cleanup_exact_evidence(connection, evidence, writer_run_id) -> None:
    connection.rollback()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM fbref_control.publication_lock
                WHERE source = 'fbref' AND owner_run_id = %s
                """,
                (writer_run_id,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.competition_registry
                WHERE source = 'fbref' AND competition_id = %s
                """,
                (evidence.competition_id,),
            )
            cursor.execute(
                "DELETE FROM fbref_control.registry_snapshot WHERE snapshot_id = %s",
                (evidence.index_snapshot_id,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.fetch_attempt
                WHERE attempt_id IN (%s, %s)
                """,
                (evidence.index_attempt_id, evidence.history_attempt_id),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.page_frontier
                WHERE target_id IN (%s, %s)
                """,
                (evidence.index_target_id, evidence.history_target_id),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.crawl_run
                WHERE run_id IN (%s, %s, %s)
                """,
                (
                    evidence.index_run_id,
                    evidence.history_run_id,
                    writer_run_id,
                ),
            )


def _seed_atomic_batch(connection, raw_store: RawPageStore):
    competition_ids = [
        str(800000 + uuid.uuid4().int % 100000),
        str(900000 + uuid.uuid4().int % 100000),
    ]
    index_run_id = str(uuid.uuid4())
    history_run_id = str(uuid.uuid4())
    index_snapshot_id = str(uuid.uuid4())
    index_html = (
        "<h2>National Team Qualification</h2><table><tbody>"
        + "".join(
            f"""
        <tr><th data-stat="league_name"><a
          href="/en/comps/{competition_id}/history/x"
        >Competition {competition_id}</a></th>
        <td data-stat="gender">M</td>
        <td data-stat="maxseason"><a
          href="/en/comps/{competition_id}/2026/Current-Stats"
        >2026</a></td></tr>
        """
            for competition_id in competition_ids
        )
        + "</tbody></table>"
    )
    index_record = raw_store.commit_fetch(
        competition_index_target(),
        index_html.encode(),
        logical_refresh_id=str(uuid.uuid4()),
        attempt_id=str(uuid.uuid4()),
        http_status=200,
    )
    index_raw = {
        "attempt_id": index_record.attempt_id,
        "content_hash": index_record.content_hash,
        "logical_refresh_id": index_record.logical_refresh_id,
        "manifest_key": raw_store.fetch_manifest_key(index_record.logical_refresh_id),
        "target_id": index_record.target_id,
    }
    histories = []
    items = []
    for position, competition_id in enumerate(competition_ids):
        history_url = f"https://fbref.com/en/comps/{competition_id}/history/x"
        history_html = (
            f"""
            <table id="seasons"><tbody><tr><th data-stat="season"><a
              href="/en/comps/{competition_id}/2022/Old-Stats"
            >2022 Old season</a></th></tr></tbody></table>
            """
            if position == 0
            else "<html><body>broken history contract</body></html>"
        )
        history_record = raw_store.commit_fetch(
            competition_page_target(competition_id, history_url),
            history_html.encode(),
            logical_refresh_id=str(uuid.uuid4()),
            attempt_id=str(uuid.uuid4()),
            http_status=200,
        )
        advertised_href = (
            f"https://fbref.com/en/comps/{competition_id}/2026/Current-Stats"
        )
        index = {
            "schema_version": "fbref-current-season-index-evidence-v1",
            "snapshot_id": index_snapshot_id,
            "run_id": index_run_id,
            "content_hash": index_record.content_hash,
            "raw": index_raw,
            "advertised": {
                "label": "2026",
                "href": advertised_href,
                "season_id": "2026",
            },
        }
        evidence = CurrentSeasonRemediationEvidence(
            competition_id=competition_id,
            advertised_label="2026",
            advertised_href=advertised_href,
            advertised_season_id="2026",
            index_snapshot_id=index_snapshot_id,
            index_run_id=index_run_id,
            index_attempt_id=index_record.attempt_id,
            index_target_id=index_record.target_id,
            index_logical_refresh_id=index_record.logical_refresh_id,
            index_content_hash=index_record.content_hash,
            index_raw_manifest_key=index_raw["manifest_key"],
            history_run_id=history_run_id,
            history_attempt_id=history_record.attempt_id,
            history_target_id=history_record.target_id,
            history_logical_refresh_id=history_record.logical_refresh_id,
            history_content_hash=history_record.content_hash,
            history_raw_manifest_key=raw_store.fetch_manifest_key(
                history_record.logical_refresh_id
            ),
        )
        histories.append((history_record, history_url, index))
        items.append(
            CurrentSeasonRemediationItem(
                evidence=evidence,
                history_html=history_html,
                history_record=history_record,
            )
        )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fbref_control.crawl_run (
                    run_id, run_type, status, request_limit, byte_limit
                ) VALUES
                  (%s, 'current', 'succeeded', 3, 4096),
                  (%s, 'current', 'succeeded', 3, 4096)
                """,
                (index_run_id, history_run_id),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.page_frontier (
                    target_id, page_kind, canonical_url, source_ids,
                    refresh_policy, state, last_content_hash
                ) VALUES (%s, 'competition_index', %s, '{}'::jsonb,
                          'daily', 'fetched', %s)
                """,
                (
                    index_record.target_id,
                    index_record.canonical_url,
                    index_record.content_hash,
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.fetch_attempt (
                    attempt_id, run_id, target_id, logical_refresh_id,
                    attempt_number, claim_token, lease_epoch, status,
                    content_hash, raw_manifest_key, finished_at
                ) VALUES (%s, %s, %s, %s, 1, %s, 1, 'succeeded',
                          %s, %s, clock_timestamp())
                """,
                (
                    index_record.attempt_id,
                    index_run_id,
                    index_record.target_id,
                    index_record.logical_refresh_id,
                    str(uuid.uuid4()),
                    index_record.content_hash,
                    index_raw["manifest_key"],
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.registry_snapshot (
                    snapshot_id, run_id, source, content_hash, successful,
                    fetched_at, metadata
                ) VALUES (%s, %s, 'fbref', %s, true,
                          clock_timestamp(), %s::jsonb)
                """,
                (
                    index_snapshot_id,
                    index_run_id,
                    index_record.content_hash,
                    json.dumps(
                        {
                            "page_kind": "competition_index",
                            "raw": index_raw,
                        }
                    ),
                ),
            )
            for item, (history_record, history_url, index) in zip(
                items, histories, strict=True
            ):
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, source_ids,
                        refresh_policy, state, last_content_hash
                    ) VALUES (%s, 'competition', %s, %s::jsonb,
                              'weekly', 'fetched', %s)
                    """,
                    (
                        history_record.target_id,
                        history_url,
                        json.dumps({"competition_id": item.evidence.competition_id}),
                        history_record.content_hash,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.page_frontier (
                        target_id, page_kind, canonical_url, source_ids,
                        refresh_policy, state, next_fetch_at,
                        last_http_status, last_error_class, last_error_message
                    ) VALUES (
                        %s, 'season_stats', %s, %s::jsonb,
                        'daily', 'dead', clock_timestamp(), 200,
                        'response_too_large', 'test oversized evidence'
                    )
                    """,
                    (
                        f"fbref:season_stats:{item.evidence.competition_id}:"
                        "2022:playingtime",
                        f"https://fbref.com/en/comps/"
                        f"{item.evidence.competition_id}/2022/playingtime/Old-Stats",
                        json.dumps(
                            {
                                "competition_id": item.evidence.competition_id,
                                "season_id": "2022",
                                "stat_type": "playingtime",
                            }
                        ),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.fetch_attempt (
                        attempt_id, run_id, target_id, logical_refresh_id,
                        attempt_number, claim_token, lease_epoch, status,
                        content_hash, raw_manifest_key, finished_at
                    ) VALUES (%s, %s, %s, %s, 1, %s, 1, 'succeeded',
                              %s, %s, clock_timestamp())
                    """,
                    (
                        history_record.attempt_id,
                        history_run_id,
                        history_record.target_id,
                        history_record.logical_refresh_id,
                        str(uuid.uuid4()),
                        history_record.content_hash,
                        item.evidence.history_raw_manifest_key,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.competition_registry (
                        source, competition_id, canonical_url, name, gender,
                        classification, lifecycle_state, crawl_state, present,
                        first_seen_at, last_seen_at, first_snapshot_id,
                        last_snapshot_id, metadata
                    ) VALUES (
                        'fbref', %s, %s, %s, 'male', 'other:national_team',
                        'present', 'active', true,
                        clock_timestamp() - interval '1 day',
                        clock_timestamp() - interval '1 day', %s, %s, %s::jsonb
                    )
                    """,
                    (
                        item.evidence.competition_id,
                        history_url,
                        f"Atomic {item.evidence.competition_id}",
                        index_snapshot_id,
                        index_snapshot_id,
                        json.dumps(
                            {
                                "last_season": "2026",
                                "last_season_url": item.evidence.advertised_href,
                                "advertised_current_season_id": "2026",
                                "current_season_index": index,
                            }
                        ),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.season_registry (
                        source, competition_id, season_id, canonical_url,
                        label, is_current, lifecycle_state, present,
                        first_seen_at, last_seen_at, first_snapshot_id,
                        last_snapshot_id, metadata
                    ) VALUES (
                        'fbref', %s, '2022', %s, '2022', true,
                        'present', true,
                        clock_timestamp() - interval '1 day',
                        clock_timestamp() - interval '1 day', %s, %s,
                        '{}'::jsonb
                    )
                    """,
                    (
                        item.evidence.competition_id,
                        f"https://fbref.com/en/comps/"
                        f"{item.evidence.competition_id}/2022/Old-Stats",
                        index_snapshot_id,
                        index_snapshot_id,
                    ),
                )
    return items, index_run_id, history_run_id


def _cleanup_atomic_batch(
    connection,
    items,
    index_run_id,
    history_run_id,
    *,
    extra_run_ids=(),
):
    competition_ids = [item.evidence.competition_id for item in items]
    run_ids = [index_run_id, history_run_id, *extra_run_ids]
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM fbref_control.snapshot_season
                WHERE source = 'fbref' AND competition_id = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.season_alias
                WHERE source = 'fbref' AND competition_id = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.season_registry
                WHERE source = 'fbref' AND competition_id = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.competition_registry
                WHERE source = 'fbref' AND competition_id = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.registry_snapshot
                WHERE run_id = ANY(%s::uuid[])
                """,
                (run_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.fetch_attempt
                WHERE run_id = ANY(%s::uuid[])
                """,
                (run_ids,),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.page_frontier
                WHERE target_id = %s
                   OR source_ids ->> 'competition_id' = ANY(%s::text[])
                """,
                (items[0].evidence.index_target_id, competition_ids),
            )
            cursor.execute(
                """
                DELETE FROM fbref_control.crawl_run
                WHERE run_id = ANY(%s::uuid[])
                """,
                (run_ids,),
            )


def test_remediation_guard_blocks_writer_index_rollover_and_stale_history():
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    store = ControlStore(dsn)
    store.migrate()
    competition_id = f"guard-{uuid.uuid4()}"
    admin = _connect(psycopg2, dsn, "fbref-remediation-admin")
    evidence, writer_run_id = _seed_exact_evidence(
        admin,
        competition_id=competition_id,
    )
    labels = {
        "writer": f"fbref-remediation-writer-{uuid.uuid4()}",
        "index": f"fbref-remediation-index-{uuid.uuid4()}",
        "history": f"fbref-remediation-history-{uuid.uuid4()}",
    }
    executor = ThreadPoolExecutor(max_workers=3)

    def acquire_writer():
        writer = ControlStore(
            dsn,
            connection_factory=lambda _dsn: _connect(psycopg2, dsn, labels["writer"]),
        )
        return writer.acquire_publication_lock(
            writer_run_id,
            dag_id="concurrency-test",
        )

    def update_index():
        connection = _connect(psycopg2, dsn, labels["index"])
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE fbref_control.competition_registry
                        SET metadata = '{}'::jsonb
                        WHERE source = 'fbref' AND competition_id = %s
                        """,
                        (competition_id,),
                    )
            return True
        finally:
            connection.close()

    def update_history():
        connection = _connect(psycopg2, dsn, labels["history"])
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE fbref_control.page_frontier
                        SET last_content_hash = 'newer-history-hash'
                        WHERE target_id = %s
                        """,
                        (evidence.history_target_id,),
                    )
            return True
        finally:
            connection.close()

    futures = []
    try:
        with store.current_season_remediation_transaction(evidence=[evidence]):
            futures = [
                executor.submit(acquire_writer),
                executor.submit(update_index),
                executor.submit(update_history),
            ]
            _wait_for_lock(admin, labels.values(), futures)
            assert not any(future.done() for future in futures)
        assert all(future.result(timeout=6) for future in futures)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
        _cleanup_exact_evidence(admin, evidence, writer_run_id)
        admin.close()


def test_failed_two_competition_batch_rolls_back_every_bronze_mutation(
    tmp_path,
):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    store = ControlStore(dsn)
    store.migrate()
    raw_store = RawPageStore.from_uri(tmp_path.as_uri())
    admin = _connect(psycopg2, dsn, "fbref-remediation-atomic-admin")
    items, index_run_id, history_run_id = _seed_atomic_batch(
        admin,
        raw_store,
    )
    competition_ids = [item.evidence.competition_id for item in items]

    try:
        with pytest.raises(ParseWaveError, match="Season discovery failed"):
            FBrefPipeline(store, raw_store).remediate_current_seasons(items)

        with admin.cursor() as cursor:
            cursor.execute(
                """
                SELECT competition_id, season_id
                FROM fbref_control.season_registry
                WHERE source = 'fbref'
                  AND competition_id = ANY(%s::text[])
                  AND present AND lifecycle_state = 'present' AND is_current
                ORDER BY competition_id, season_id
                """,
                (competition_ids,),
            )
            assert cursor.fetchall() == [
                (competition_id, "2022") for competition_id in sorted(competition_ids)
            ]
            cursor.execute(
                """
                SELECT count(*)
                FROM fbref_control.registry_snapshot
                WHERE run_id = %s
                  AND metadata ->> 'page_kind' = 'competition'
                """,
                (history_run_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                SELECT count(*)
                FROM fbref_control.season_registry
                WHERE source = 'fbref'
                  AND competition_id = ANY(%s::text[])
                  AND season_id = '2026'
                """,
                (competition_ids,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                SELECT count(*)
                FROM fbref_control.season_alias
                WHERE source = 'fbref'
                  AND competition_id = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                SELECT count(*)
                FROM fbref_control.page_frontier
                WHERE source = 'fbref' AND page_kind = 'season'
                  AND source_ids ->> 'competition_id' = ANY(%s::text[])
                """,
                (competition_ids,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                SELECT competition_id, is_current, refresh_policy, state,
                       next_fetch_at IS NOT NULL
                FROM (
                    SELECT season.competition_id, season.is_current,
                           frontier.refresh_policy, frontier.state,
                           frontier.next_fetch_at
                    FROM fbref_control.season_registry AS season
                    JOIN fbref_control.page_frontier AS frontier
                      ON frontier.source = season.source
                     AND frontier.source_ids ->> 'competition_id' =
                         season.competition_id
                     AND frontier.source_ids ->> 'season_id' = season.season_id
                    WHERE season.source = 'fbref'
                      AND season.competition_id = ANY(%s::text[])
                      AND season.season_id = '2022'
                      AND frontier.page_kind = 'season_stats'
                      AND frontier.source_ids ->> 'stat_type' = 'playingtime'
                ) AS old_scope
                ORDER BY competition_id
                """,
                (competition_ids,),
            )
            assert cursor.fetchall() == [
                (competition_id, True, "daily", "dead", True)
                for competition_id in sorted(competition_ids)
            ]
        admin.rollback()
    finally:
        admin.rollback()
        _cleanup_atomic_batch(
            admin,
            items,
            index_run_id,
            history_run_id,
        )
        admin.close()


def test_success_demotes_old_current_playingtime_from_proxy_lane(tmp_path):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    store = ControlStore(dsn)
    store.migrate()
    raw_store = RawPageStore.from_uri(tmp_path.as_uri())
    admin = _connect(psycopg2, dsn, "fbref-remediation-scope-admin")
    items, index_run_id, history_run_id = _seed_atomic_batch(
        admin,
        raw_store,
    )
    item = items[0]

    class RollbackScopeProof(Exception):
        pass

    try:
        with pytest.raises(RollbackScopeProof):
            with store.current_season_remediation_transaction(
                evidence=[item.evidence]
            ) as bound:
                result = FBrefPipeline(bound, raw_store).remediate_current_seasons(
                    [item]
                )
                assert result["competition_count"] == 1
                cursor = bound._bound_cursor
                cursor.execute(
                    """
                    SELECT season.is_current, season.present,
                           season.lifecycle_state,
                           frontier.refresh_policy, frontier.state,
                           frontier.next_fetch_at,
                           frontier.last_error_class,
                           frontier.last_error_message
                    FROM fbref_control.season_registry AS season
                    JOIN fbref_control.page_frontier AS frontier
                      ON frontier.source = season.source
                     AND frontier.source_ids ->> 'competition_id' =
                         season.competition_id
                     AND frontier.source_ids ->> 'season_id' = season.season_id
                    WHERE season.source = 'fbref'
                      AND season.competition_id = %s
                      AND season.season_id = '2022'
                      AND frontier.target_id = %s
                    """,
                    (
                        item.evidence.competition_id,
                        f"fbref:season_stats:{item.evidence.competition_id}:"
                        "2022:playingtime",
                    ),
                )
                old_scope = cursor.fetchone()
                assert old_scope == {
                    "is_current": False,
                    "present": True,
                    "lifecycle_state": "present",
                    "refresh_policy": "daily",
                    "state": "quarantined",
                    "next_fetch_at": None,
                    "last_error_class": "ScopeQuarantined",
                    "last_error_message": "noncurrent_season",
                }
                raise RollbackScopeProof
    finally:
        admin.rollback()
        _cleanup_atomic_batch(
            admin,
            items,
            index_run_id,
            history_run_id,
        )
        admin.close()


def test_same_history_index_change_and_revert_have_distinct_real_provenance(
    tmp_path,
):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = _postgres_uri()
    store = ControlStore(dsn)
    store.migrate()
    raw_store = RawPageStore.from_uri(tmp_path.as_uri())
    admin = _connect(psycopg2, dsn, "fbref-remediation-lineage-admin")
    items, index_run_id, history_run_id = _seed_atomic_batch(
        admin,
        raw_store,
    )
    item = items[0]
    competition_id = item.evidence.competition_id
    rollover_run_id = str(uuid.uuid4())
    advertised_label = "2030"
    advertised_href = f"https://fbref.com/en/comps/{competition_id}/2030/Current-Stats"
    index_html = f"""
    <h2>National Team Qualification</h2><table><tbody><tr>
      <th data-stat="league_name"><a
        href="/en/comps/{competition_id}/history/x"
      >Competition {competition_id}</a></th>
      <td data-stat="gender">M</td>
      <td data-stat="maxseason"><a
        href="/en/comps/{competition_id}/2030/Current-Stats"
      >2030</a></td>
    </tr></tbody></table>
    """
    rollover_record = raw_store.commit_fetch(
        competition_index_target(),
        index_html.encode(),
        logical_refresh_id=str(uuid.uuid4()),
        attempt_id=str(uuid.uuid4()),
        http_status=200,
    )
    rollover_snapshot_id = str(uuid.uuid4())
    rollover_raw = {
        "attempt_id": rollover_record.attempt_id,
        "content_hash": rollover_record.content_hash,
        "logical_refresh_id": rollover_record.logical_refresh_id,
        "manifest_key": raw_store.fetch_manifest_key(
            rollover_record.logical_refresh_id
        ),
        "target_id": rollover_record.target_id,
    }
    rollover_index = {
        "schema_version": "fbref-current-season-index-evidence-v1",
        "snapshot_id": rollover_snapshot_id,
        "run_id": rollover_run_id,
        "content_hash": rollover_record.content_hash,
        "raw": rollover_raw,
        "advertised": {
            "label": advertised_label,
            "href": advertised_href,
            "season_id": advertised_label,
        },
    }

    class RollbackLineageProof(Exception):
        pass

    try:
        with pytest.raises(RollbackLineageProof):
            with store.current_season_remediation_transaction(
                evidence=[item.evidence]
            ) as bound:
                pipeline = FBrefPipeline(bound, raw_store)
                pipeline._parse_competition(
                    history_run_id,
                    item.history_html,
                    item.history_record,
                    run_type="backfill",
                )
                cursor = bound._bound_cursor
                cursor.execute(
                    """
                    SELECT last_snapshot_id, metadata
                    FROM fbref_control.competition_registry
                    WHERE source = 'fbref' AND competition_id = %s
                    """,
                    (competition_id,),
                )
                first_row = cursor.fetchone()
                first_snapshot_id = first_row["last_snapshot_id"]
                first_metadata = first_row["metadata"]
                cursor.execute(
                    """
                    SELECT relation
                    FROM fbref_control.frontier_provenance
                    WHERE carried_competition_id = %s
                    ORDER BY relation
                    """,
                    (competition_id,),
                )
                first_relations = [row["relation"] for row in cursor.fetchall()]
                first_install_id = first_relations[0].rsplit(":install:", 1)[1]
                assert first_relations == [
                    f"page_link:season:install:{first_install_id}"
                ]

                rollover_metadata = dict(first_metadata)
                rollover_metadata.update(
                    {
                        "last_season": advertised_label,
                        "last_season_url": advertised_href,
                        "advertised_current_season_id": advertised_label,
                        "current_season_index": rollover_index,
                    }
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.crawl_run (
                        run_id, run_type, status, request_limit, byte_limit
                    ) VALUES (%s, 'current', 'succeeded', 1, 4096)
                    """,
                    (rollover_run_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.fetch_attempt (
                        attempt_id, run_id, target_id, logical_refresh_id,
                        attempt_number, claim_token, lease_epoch, status,
                        content_hash, raw_manifest_key, finished_at
                    ) VALUES (%s, %s, %s, %s, 1, %s, 2, 'succeeded',
                              %s, %s, clock_timestamp())
                    """,
                    (
                        rollover_record.attempt_id,
                        rollover_run_id,
                        rollover_record.target_id,
                        rollover_record.logical_refresh_id,
                        str(uuid.uuid4()),
                        rollover_record.content_hash,
                        rollover_raw["manifest_key"],
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fbref_control.registry_snapshot (
                        snapshot_id, run_id, source, content_hash, successful,
                        fetched_at, metadata
                    ) VALUES (%s, %s, 'fbref', %s, true,
                              clock_timestamp(), %s::jsonb)
                    """,
                    (
                        rollover_snapshot_id,
                        rollover_run_id,
                        rollover_record.content_hash,
                        json.dumps(
                            {
                                "page_kind": "competition_index",
                                "raw": rollover_raw,
                            }
                        ),
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET last_content_hash = %s
                    WHERE target_id = %s
                    """,
                    (
                        rollover_record.content_hash,
                        rollover_record.target_id,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.competition_registry
                    SET last_snapshot_id = %s, metadata = %s::jsonb
                    WHERE source = 'fbref' AND competition_id = %s
                    """,
                    (
                        rollover_snapshot_id,
                        json.dumps(rollover_metadata),
                        competition_id,
                    ),
                )

                pipeline._parse_competition(
                    history_run_id,
                    item.history_html,
                    item.history_record,
                    run_type="backfill",
                )
                cursor.execute(
                    """
                SELECT relation
                FROM fbref_control.frontier_provenance
                WHERE carried_competition_id = %s
                ORDER BY relation
                """,
                    (competition_id,),
                )
                rollover_relations = [row["relation"] for row in cursor.fetchall()]
                assert len(rollover_relations) == 2
                assert first_relations[0] in rollover_relations
                assert all(":install:" in relation for relation in rollover_relations)

                cursor.execute(
                    """
                    UPDATE fbref_control.page_frontier
                    SET last_content_hash = %s
                    WHERE target_id = %s
                    """,
                    (
                        item.evidence.index_content_hash,
                        item.evidence.index_target_id,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fbref_control.competition_registry
                    SET last_snapshot_id = %s, metadata = %s::jsonb
                    WHERE source = 'fbref' AND competition_id = %s
                    """,
                    (
                        first_snapshot_id,
                        json.dumps(first_metadata),
                        competition_id,
                    ),
                )
                pipeline._parse_competition(
                    history_run_id,
                    item.history_html,
                    item.history_record,
                    run_type="backfill",
                )
                cursor.execute(
                    """
                SELECT relation
                FROM fbref_control.frontier_provenance
                WHERE carried_competition_id = %s
                ORDER BY relation
                """,
                    (competition_id,),
                )
                assert [
                    row["relation"] for row in cursor.fetchall()
                ] == rollover_relations
                raise RollbackLineageProof
    finally:
        admin.rollback()
        _cleanup_atomic_batch(
            admin,
            items,
            index_run_id,
            history_run_id,
        )
        admin.close()
