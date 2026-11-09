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
    attempt_number=1,
    raw_age_seconds=None,
    run_id=None,
):
    run_id = run_id or _crawl_run(cursor, status=run_status, run_type=run_type)
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
    raw_finished_at = (
        "clock_timestamp()"
        if raw_age_seconds is None
        else "clock_timestamp() - (%s * interval '1 second')"
    )
    cursor.execute(
        f"""
        INSERT INTO fbref_control.fetch_attempt (
            attempt_id, run_id, target_id, logical_refresh_id,
            attempt_number, claim_token, lease_epoch, status,
            http_status, content_hash, raw_manifest_key, finished_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, 'succeeded',
            200, %s, %s, {raw_finished_at}
        )
        """,
        (
            str(uuid.uuid4()),
            run_id,
            target_id,
            logical_refresh_id,
            attempt_number,
            str(uuid.uuid4()),
            epoch,
            content_hash,
            f"raw/{logical_refresh_id}.json.zst",
            *(() if raw_age_seconds is None else (raw_age_seconds,)),
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


def _seed_failed_attempt(cursor, *, target_id, epoch):
    """A later failed attempt: it commits no raw, so nothing is superseded."""
    run_id = _crawl_run(cursor)
    logical_refresh_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO fbref_control.run_target (
            run_id, target_id, logical_refresh_id, ordinal, status
        ) VALUES (%s, %s, %s, 0, 'failed')
        """,
        (run_id, target_id, logical_refresh_id),
    )
    cursor.execute(
        """
        INSERT INTO fbref_control.fetch_attempt (
            attempt_id, run_id, target_id, logical_refresh_id,
            attempt_number, claim_token, lease_epoch, status,
            error_class, error_message, finished_at
        ) VALUES (
            %s, %s, %s, %s, 1, %s, %s, 'failed',
            'UpstreamUnavailable', 'fixture outage', clock_timestamp()
        )
        """,
        (
            str(uuid.uuid4()),
            run_id,
            target_id,
            logical_refresh_id,
            str(uuid.uuid4()),
            epoch,
        ),
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
    # #1332: the strictly-newer successes on "different-version" and
    # "partial" supersede the older raw, so only the newest raw of each
    # target is visible. The exact complete success still hides its own raw,
    # and A -> B -> A keeps its newest committed raw visible.
    expected = {
        labels["unresolved"],
        labels["cancelled"],
        labels["active"],
        labels["different_new"],
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
    # The oldest-raw timestamps come from the latest raw only.
    oldest_visible = min(row["finished_at"] for row in rows)
    assert summary["lane_unprocessed_raw_by_page_kind"]["match"][
        "oldest_raw_at"
    ] == oldest_visible
    assert summary["global_unprocessed_raw_by_page_kind"]["match"][
        "oldest_raw_at"
    ] == oldest_visible


def test_recovery_counts_and_selects_only_the_newest_raw_of_a_target(
    isolated_postgres_uri,  # noqa: F811
):
    """#1332: superseded raw is invisible to selection and every count."""
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(isolated_postgres_uri)
    labels = {}
    try:
        with connection.cursor() as cursor:
            # An old v5 shell under a newer successful v6 raw: the old raw
            # is superseded content even though nobody parsed it fully.
            superseded_target = "fbref:match:superseded-shell"
            _seed_refresh(
                cursor,
                target_id=superseded_target,
                epoch=1,
                content_hash="1" * 64,
                observation=_failed_observation(
                    error_class="IncompleteMatchPageError",
                    error_message="fixture shell",
                    versions=("page-v5", "typed-v5", "stateful-v5"),
                ),
            )
            labels["superseded_shell_new"] = _seed_refresh(
                cursor,
                target_id=superseded_target,
                epoch=2,
                content_hash="2" * 64,
                observation=_failed_observation(),
            )

            # A latest old-version shell nobody owns stays visible work...
            labels["shell_unowned"] = _seed_refresh(
                cursor,
                target_id="fbref:match:shell-unowned",
                epoch=1,
                content_hash="3" * 64,
                observation=_failed_observation(
                    error_class="IncompleteMatchPageError",
                    error_message="fixture shell",
                    versions=("page-v5", "typed-v5", "stateful-v5"),
                ),
            )
            # ...while a current-version shell with a scheduled refetch is
            # owned by a fresh request and stays excluded (#2051).
            shell_owned_target = "fbref:match:shell-owned"
            shell_owned_refresh = _seed_refresh(
                cursor,
                target_id=shell_owned_target,
                epoch=1,
                content_hash="4" * 64,
                observation=_failed_observation(
                    error_class="IncompleteMatchPageError",
                    error_message="fixture shell",
                ),
            )
            cursor.execute(
                """
                INSERT INTO fbref_control.incomplete_match_refetch (
                    target_id, attempts, logical_refresh_id
                ) VALUES (%s, 1, %s)
                """,
                (shell_owned_target, shell_owned_refresh),
            )
            cursor.execute(
                """
                UPDATE fbref_control.page_frontier SET state = 'queued'
                WHERE target_id = %s
                """,
                (shell_owned_target,),
            )

            # Three raw of one target: only the newest can be selected or
            # counted, whichever observations the older raw collected.
            multi_target = "fbref:match:multi-raw"
            _seed_refresh(
                cursor,
                target_id=multi_target,
                epoch=1,
                content_hash="5" * 64,
            )
            _seed_refresh(
                cursor,
                target_id=multi_target,
                epoch=2,
                content_hash="6" * 64,
                observation=_failed_observation(),
            )
            labels["multi_newest"] = _seed_refresh(
                cursor,
                target_id=multi_target,
                epoch=3,
                content_hash="7" * 64,
                observation=_failed_observation(),
            )

            # A latest transient Trino failure stays deferrable work...
            labels["trino_latest"] = _seed_refresh(
                cursor,
                target_id="fbref:match:trino-latest",
                epoch=1,
                content_hash="8" * 64,
                observation=_failed_observation(
                    error_class="TrinoConnectionError",
                    error_message="fixture outage",
                ),
            )
            # ...but the same failure on superseded raw disappears.
            trino_target = "fbref:match:trino-superseded"
            _seed_refresh(
                cursor,
                target_id=trino_target,
                epoch=1,
                content_hash="9" * 64,
                observation=_failed_observation(
                    error_class="TrinoConnectionError",
                    error_message="fixture outage",
                ),
            )
            labels["trino_new"] = _seed_refresh(
                cursor,
                target_id=trino_target,
                epoch=2,
                content_hash="a" * 64,
                observation=_failed_observation(),
            )

            # One lease epoch, two attempts: the later attempt_number wins.
            equal_target = "fbref:match:equal-epoch"
            _seed_refresh(
                cursor,
                target_id=equal_target,
                epoch=7,
                content_hash="b" * 64,
                attempt_number=1,
            )
            labels["equal_latest"] = _seed_refresh(
                cursor,
                target_id=equal_target,
                epoch=7,
                content_hash="c" * 64,
                attempt_number=2,
                observation=_failed_observation(),
            )

            # A failed later attempt commits no raw: the committed raw of
            # the earlier lease stays visible.
            failed_target = "fbref:match:failed-later"
            labels["failed_latest"] = _seed_refresh(
                cursor,
                target_id=failed_target,
                epoch=1,
                content_hash="d" * 64,
                observation=_failed_observation(),
            )
            _seed_failed_attempt(cursor, target_id=failed_target, epoch=2)

            # SLA accounting sees only the newest raw: a fresh newest raw
            # keeps the lane within SLA even though an older raw would be
            # overdue, and one genuinely overdue latest raw counts once.
            sla_target = "fbref:match:sla-superseded"
            _seed_refresh(
                cursor,
                target_id=sla_target,
                epoch=1,
                content_hash="e" * 64,
                raw_age_seconds=30 * 86_400,
            )
            labels["sla_newest"] = _seed_refresh(
                cursor,
                target_id=sla_target,
                epoch=2,
                content_hash="f" * 64,
                observation=_failed_observation(),
            )
            labels["sla_latest"] = _seed_refresh(
                cursor,
                target_id="fbref:match:sla-latest",
                epoch=1,
                content_hash="0" * 64,
                observation=_failed_observation(),
                raw_age_seconds=10 * 86_400,
            )
            backfill_refresh = _seed_refresh(
                cursor,
                target_id="fbref:match:backfill-newest",
                epoch=1,
                content_hash="1" * 32 + "2" * 32,
                run_type="backfill",
            )
            summary_run_id = _crawl_run(cursor, status="running")
            labels["run_owned"] = _seed_refresh(
                cursor,
                target_id="fbref:match:run-owned",
                epoch=1,
                content_hash="ab" * 32,
                observation=_failed_observation(),
                run_id=summary_run_id,
            )
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
        labels["superseded_shell_new"],
        labels["shell_unowned"],
        labels["multi_newest"],
        labels["trino_latest"],
        labels["trino_new"],
        labels["equal_latest"],
        labels["failed_latest"],
        labels["sla_newest"],
        labels["sla_latest"],
        labels["run_owned"],
    }
    assert visible == expected
    # The overdue latest raw is the oldest work and drains first.
    assert str(rows[0]["logical_refresh_id"]) == labels["sla_latest"]
    order = [
        (row["finished_at"] or row["started_at"], str(row["attempt_id"]))
        for row in rows
    ]
    assert order == sorted(order)

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
    assert summary["unprocessed_raw_count"] == 1
    run_match = summary["unprocessed_raw_by_page_kind"]["match"]
    run_owned_row = next(
        row for row in rows
        if str(row["logical_refresh_id"]) == labels["run_owned"]
    )
    assert run_match["count"] == 1
    assert run_match["oldest_raw_at"] == run_owned_row["finished_at"]
    assert summary["recovery_lane_run_type"] == "current"
    assert summary["lane_unprocessed_raw_count"] == len(expected)
    assert summary["lane_unprocessed_raw_by_page_kind"]["match"][
        "count"
    ] == len(expected)
    assert summary["global_unprocessed_raw_count"] == len(expected) + 1
    assert summary["global_unprocessed_raw_by_page_kind"]["match"][
        "count"
    ] == len(expected) + 1
    # Only the latest raw feeds the SLA clock: the 30-day-old superseded
    # raw is gone and the 10-day-old latest raw is the single overdue row.
    lane_match = summary["lane_unprocessed_raw_by_page_kind"]["match"]
    global_match = summary["global_unprocessed_raw_by_page_kind"]["match"]
    assert lane_match["sla_overdue_count"] == 1
    assert global_match["sla_overdue_count"] == 1
    assert summary["lane_unprocessed_raw_sla_overdue_count"] == 1
    assert summary["global_unprocessed_raw_sla_overdue_count"] == 1
    # The oldest-raw timestamps come from the latest raw only.
    oldest_visible = min(row["finished_at"] for row in rows)
    assert lane_match["oldest_raw_at"] == oldest_visible
    assert global_match["oldest_raw_at"] == oldest_visible
