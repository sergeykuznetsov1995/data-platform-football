"""Executable PostgreSQL semantics for FBref recovery supersession."""

from __future__ import annotations

import json
import os
import uuid

import pytest

from scrapers.fbref.control import ControlStore

# Reuse the repository's disposable, migrated PostgreSQL database fixture.
from tests.integration.scrapers.test_fbref_control_admission import (  # noqa: F401
    isolated_postgres_uri,
)


pytestmark = pytest.mark.integration

PAGE_VERSION = "page-v2"
TYPED_VERSION = "typed-v3"
STATEFUL_VERSION = "stateful-v4"
REQUESTED_VERSIONS = (PAGE_VERSION, TYPED_VERSION, STATEFUL_VERSION)


@pytest.fixture(autouse=True)
def _require_available_test_postgres():
    """Skip only when the explicitly configured test service is unavailable."""

    dsn = os.getenv("FBREF_TEST_POSTGRES_URI", "").strip()
    if not dsn and os.getenv("FBREF_TEST_POSTGRES_USE_AIRFLOW_DB") == "1":
        dsn = os.getenv(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ""
        ).strip()
    if not dsn:
        pytest.skip("FBref test PostgreSQL DSN is not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    normalized = dsn.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
    try:
        connection = psycopg2.connect(normalized, connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("configured FBref test PostgreSQL is unavailable")
    else:
        connection.close()


def _crawl_run(cursor, *, status="failed", run_type="current"):
    run_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO fbref_control.crawl_run (
            run_id, run_type, status, request_limit, byte_limit
        ) VALUES (%s, %s, %s, 200, 104857600)
        """,
        (run_id, run_type, status),
    )
    return run_id


def _seed_refresh(
    cursor,
    *,
    target_id,
    epoch,
    content_hash,
    observation=None,
    run_type="current",
    run_status="failed",
):
    run_id = _crawl_run(cursor, status=run_status, run_type=run_type)
    logical_refresh_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO fbref_control.page_frontier (
            target_id, page_kind, canonical_url, source_ids,
            refresh_policy, state, lease_epoch, last_content_hash
        ) VALUES (
            %s, 'match', %s, %s::jsonb, 'daily', 'fetched', %s, %s
        )
        ON CONFLICT (target_id) DO UPDATE
        SET state = 'fetched', lease_epoch = EXCLUDED.lease_epoch,
            last_content_hash = EXCLUDED.last_content_hash,
            claim_token = NULL, lease_run_id = NULL,
            lease_refresh_id = NULL, leased_by = NULL,
            lease_expires_at = NULL
        """,
        (
            target_id,
            f"https://fbref.com/en/matches/{target_id.rsplit(':', 1)[-1]}",
            json.dumps({"match_id": target_id.rsplit(":", 1)[-1]}),
            epoch,
            content_hash,
        ),
    )
    cursor.execute(
        """
        INSERT INTO fbref_control.run_target (
            run_id, target_id, logical_refresh_id, ordinal, status
        ) VALUES (%s, %s, %s, 0, 'succeeded')
        """,
        (run_id, target_id, logical_refresh_id),
    )
    cursor.execute(
        """
        INSERT INTO fbref_control.fetch_attempt (
            attempt_id, run_id, target_id, logical_refresh_id,
            attempt_number, claim_token, lease_epoch, status,
            http_status, content_hash, raw_manifest_key, finished_at
        ) VALUES (
            %s, %s, %s, %s, 1, %s, %s, 'succeeded',
            200, %s, %s, clock_timestamp()
        )
        """,
        (
            str(uuid.uuid4()),
            run_id,
            target_id,
            logical_refresh_id,
            str(uuid.uuid4()),
            epoch,
            content_hash,
            f"raw/{logical_refresh_id}.json.zst",
        ),
    )
    if observation is not None:
        versions = observation.get("versions", REQUESTED_VERSIONS)
        cursor.execute(
            """
            INSERT INTO fbref_control.observation_processing (
                logical_refresh_id, parser_version,
                typed_parser_version, stateful_parser_version,
                target_id, content_hash, status, generic_status,
                typed_status, stateful_status, validation_status,
                error_class, error_message, completed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s,
                CASE WHEN %s IN ('succeeded', 'failed')
                     THEN clock_timestamp() END
            )
            """,
            (
                logical_refresh_id,
                *versions,
                target_id,
                content_hash,
                observation["status"],
                observation["generic_status"],
                observation["typed_status"],
                observation["stateful_status"],
                observation["validation_status"],
                observation.get("error_class"),
                observation.get("error_message"),
                observation["status"],
            ),
        )
    return logical_refresh_id


def _failed_observation(**overrides):
    item = {
        "status": "failed",
        "generic_status": "succeeded",
        "typed_status": "failed",
        "stateful_status": "skipped",
        "validation_status": "failed",
        "error_class": "ParserError",
        "error_message": "fixture failure",
    }
    item.update(overrides)
    return item


def _complete_observation(**overrides):
    item = {
        "status": "succeeded",
        "generic_status": "succeeded",
        "typed_status": "succeeded",
        "stateful_status": "skipped",
        "validation_status": "succeeded",
    }
    item.update(overrides)
    return item


def _seed_active_lease(cursor, *, target_id, epoch):
    run_id = _crawl_run(cursor, status="running")
    refresh_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())
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
        UPDATE fbref_control.page_frontier
        SET state = 'leased', claim_token = %s, lease_epoch = %s,
            lease_run_id = %s, lease_refresh_id = %s,
            leased_by = 'integration-test',
            lease_expires_at = clock_timestamp() + interval '10 minutes'
        WHERE target_id = %s
        """,
        (claim_token, epoch, run_id, refresh_id, target_id),
    )


def test_recovery_skips_only_strictly_newer_exact_complete_success(
    isolated_postgres_uri,  # noqa: F811
):
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    labels = {}
    try:
        with connection.cursor() as cursor:
            labels["unresolved"] = _seed_refresh(
                cursor,
                target_id="fbref:match:unresolved",
                epoch=1,
                content_hash="1" * 64,
                observation=_failed_observation(),
            )
            labels["cancelled"] = _seed_refresh(
                cursor,
                target_id="fbref:match:cancelled",
                epoch=1,
                content_hash="c" * 64,
                run_status="cancelled",
            )
            backfill_refresh = _seed_refresh(
                cursor,
                target_id="fbref:match:backfill-only",
                epoch=1,
                content_hash="f" * 64,
                run_type="backfill",
            )

            active_target = "fbref:match:active"
            labels["active"] = _seed_refresh(
                cursor,
                target_id=active_target,
                epoch=1,
                content_hash="2" * 64,
            )
            _seed_active_lease(cursor, target_id=active_target, epoch=2)

            different_target = "fbref:match:different-version"
            labels["different_old"] = _seed_refresh(
                cursor,
                target_id=different_target,
                epoch=1,
                content_hash="3" * 64,
                observation=_failed_observation(),
            )
            labels["different_new"] = _seed_refresh(
                cursor,
                target_id=different_target,
                epoch=2,
                content_hash="4" * 64,
                observation=_complete_observation(
                    versions=("page-v9", "typed-v9", "stateful-v9")
                ),
            )

            partial_target = "fbref:match:partial"
            labels["partial_old"] = _seed_refresh(
                cursor,
                target_id=partial_target,
                epoch=1,
                content_hash="5" * 64,
                observation=_failed_observation(),
            )
            labels["partial_new"] = _seed_refresh(
                cursor,
                target_id=partial_target,
                epoch=2,
                content_hash="6" * 64,
                observation=_failed_observation(generic_status="pending"),
            )

            exact_target = "fbref:match:exact-complete"
            _seed_refresh(
                cursor,
                target_id=exact_target,
                epoch=1,
                content_hash="7" * 64,
                observation=_failed_observation(),
            )
            _seed_refresh(
                cursor,
                target_id=exact_target,
                epoch=2,
                content_hash="8" * 64,
                observation=_complete_observation(),
            )

            aba_target = "fbref:match:aba"
            _seed_refresh(
                cursor,
                target_id=aba_target,
                epoch=1,
                content_hash="a" * 64,
                observation=_failed_observation(),
            )
            _seed_refresh(
                cursor,
                target_id=aba_target,
                epoch=2,
                content_hash="b" * 64,
                observation=_complete_observation(),
            )
            labels["aba_newest"] = _seed_refresh(
                cursor,
                target_id=aba_target,
                epoch=3,
                content_hash="a" * 64,
                observation=_failed_observation(),
            )
            summary_run_id = _crawl_run(cursor, status="running")
        connection.commit()
    finally:
        connection.close()

    store = ControlStore(isolated_postgres_uri)
    rows = store.list_unprocessed_fetches(
        run_type="current",
        parser_version=PAGE_VERSION,
        typed_parser_version=TYPED_VERSION,
        stateful_parser_version=STATEFUL_VERSION,
        page_kinds=["match"],
        limit=100,
    )
    visible = {str(row["logical_refresh_id"]) for row in rows}
    expected = {
        labels["unresolved"],
        labels["cancelled"],
        labels["active"],
        labels["different_old"],
        labels["different_new"],
        labels["partial_old"],
        labels["partial_new"],
        labels["aba_newest"],
    }
    assert visible == expected

    backfill_rows = store.list_unprocessed_fetches(
        run_type="backfill",
        parser_version=PAGE_VERSION,
        typed_parser_version=TYPED_VERSION,
        stateful_parser_version=STATEFUL_VERSION,
        page_kinds=["match"],
        limit=100,
    )
    assert {
        str(row["logical_refresh_id"]) for row in backfill_rows
    } == {backfill_refresh}

    summary = store.get_run_summary(
        summary_run_id,
        parser_version=PAGE_VERSION,
        typed_parser_version=TYPED_VERSION,
        stateful_parser_version=STATEFUL_VERSION,
    )
    assert summary["unprocessed_raw_count"] == 0
    assert summary["recovery_lane_run_type"] == "current"
    assert summary["lane_unprocessed_raw_count"] == len(expected)
    assert summary["lane_unprocessed_raw_by_page_kind"]["match"][
        "count"
    ] == len(expected)
    assert summary["global_unprocessed_raw_count"] == len(expected) + 1
    assert summary["global_unprocessed_raw_by_page_kind"]["match"][
        "count"
    ] == len(expected) + 1
