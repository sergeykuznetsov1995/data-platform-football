from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from scrapers.whoscored.parsers import PARSER_VERSION as CORE_PARSER_VERSION
from scrapers.whoscored.repository import (
    PARSER_VERSION,
    BatchConflict,
    MatchCommit,
    WhoScoredRepository,
)


def _commit(*, lineups=(), lineups_available=False) -> MatchCommit:
    return MatchCommit(
        game_id=123,
        league="INT-World Cup",
        season="2026",
        game="Home-Away",
        payload_sha256="a" * 64,
        raw_uri="s3://raw/match.json.gz",
        events=({"source_event_id": 10},),
        lineups=lineups,
        lineups_available=lineups_available,
        transport_mode="direct_http",
    )


@pytest.mark.unit
def test_repository_uses_the_core_parser_version_and_injected_trino_for_writes():
    trino = MagicMock()

    repository = WhoScoredRepository(trino=trino)

    assert PARSER_VERSION == CORE_PARSER_VERSION
    assert repository.trino is trino
    assert repository.writer._get_trino_manager() is trino


@pytest.mark.unit
def test_current_views_keep_last_success_visible_after_a_newer_failure():
    trino = MagicMock()
    trino.table_exists.return_value = True
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._create_current_views()

    statements = [call.args[0] for call in trino._execute.call_args_list]
    success_view = next(
        sql for sql in statements if "whoscored_match_ingest_latest_success AS" in sql
    )
    current_view = next(
        sql for sql in statements if "whoscored_events_current AS" in sql
    )
    assert "WHERE state = 'success'" in success_view
    assert "JOIN iceberg.bronze.whoscored_match_ingest_latest_success m" in current_view


@pytest.mark.unit
def test_profile_current_view_matches_legacy_null_hashes_null_safely():
    trino = MagicMock()
    trino.table_exists.return_value = True
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._ensure_profile_schema()

    statements = [call.args[0] for call in trino._execute.call_args_list]
    profile_view = next(
        sql for sql in statements if "silver.whoscored_player_profile_current AS" in sql
    )
    assert "m.payload_sha256 = p.payload_sha256" in profile_view
    assert "m.payload_sha256 IS NULL" in profile_view
    assert "p.payload_sha256 IS NULL" in profile_view
    assert "IS NOT DISTINCT FROM" not in profile_view
    assert "m.parser_version = p.parser_version" in profile_view


@pytest.mark.unit
def test_commit_match_rejects_lineup_availability_contradictions_before_io():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="lineups_available disagrees"):
        repository.commit_match(_commit(lineups=(), lineups_available=True))

    trino.execute_query.assert_not_called()
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_idempotent_match_commit_verifies_manifest_and_physical_counts():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [(1, 0)],  # committed manifest counts
        [(0,)],  # events physical count was lost
        [(0,)],  # lineups physical count
    ]
    trino.table_exists.return_value = True
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(BatchConflict, match="manifest=.*physical=.*parser="):
        repository.commit_match(_commit())

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_profile_manifest_cannot_hide_a_missing_physical_version():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [(0,)],  # no physical profile version
        [(1,)],  # but a successful manifest exists
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(BatchConflict, match="committed but has 0 physical"):
        repository.commit_profile(
            player_id=99,
            profile={"name": "Player"},
            payload_sha256="b" * 64,
            raw_uri="s3://raw/profile.html.gz",
            transport_mode="direct_http",
        )

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_profile_candidates_only_refresh_success_and_due_retryable_states():
    trino = MagicMock()
    trino.execute_query.return_value = [(7,), (11,)]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    repository.ensure_schema = MagicMock()

    assert repository.list_profile_candidates(limit=2) == [7, 11]

    sql = trino.execute_query.call_args.args[0]
    assert "m.state = 'success'" in sql
    assert "m.parser_version IS DISTINCT FROM" in sql
    assert "m.state = 'retryable'" in sql
    assert "m.retry_after" in sql
    assert "<= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)" in sql
    # There is intentionally no generic parser-version branch: terminal rows
    # match neither the success refresh nor the due-retry branch.
    assert "OR m.parser_version IS DISTINCT FROM" not in sql


@pytest.mark.unit
@pytest.mark.parametrize(
    ("state", "retry_after", "completed"),
    [
        ("retryable", datetime(2026, 7, 11, 12, 0), False),
        ("terminal", None, True),
    ],
)
def test_record_profile_failure_writes_current_manifest_shape(
    state, retry_after, completed
):
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    repository.record_profile_failure(
        player_id=99,
        state=state,
        failure_code="timeout" if state == "retryable" else "http_status",
        error="source failed",
        retry_after=retry_after,
        transport_mode="direct_http",
        http_status=None if state == "retryable" else 404,
    )

    call = writer.write_dataframe.call_args
    row = call.args[0].iloc[0]
    assert call.kwargs["table"] == "whoscored_profile_ingest_manifest"
    assert call.kwargs["partition_spec"] == [("player_id", "bucket(32)")]
    assert set(row.index) == {
        "player_id",
        "payload_sha256",
        "raw_uri",
        "parser_version",
        "state",
        "http_status",
        "failure_code",
        "error",
        "attempt_no",
        "retry_after",
        "transport_mode",
        "proxy_mode",
        "direct_bytes",
        "paid_bytes",
        "fetched_at",
        "completed_at",
        "_entity_type",
    }
    assert row["player_id"] == 99
    assert row["state"] == state
    assert row["attempt_no"] == 1
    assert row["retry_after"] == retry_after or (
        retry_after is None and row["retry_after"] is None
    )
    assert (row["completed_at"] is not None) is completed
    assert row["_entity_type"] == "profile_manifest"


@pytest.mark.unit
def test_profile_failure_state_requires_consistent_backoff():
    repository = WhoScoredRepository(writer=MagicMock(), trino=MagicMock())

    with pytest.raises(ValueError, match="requires retry_after"):
        repository.record_profile_failure(
            player_id=1,
            state="retryable",
            failure_code="timeout",
            error="timeout",
            retry_after=None,
        )

    with pytest.raises(ValueError, match="cannot have retry_after"):
        repository.record_profile_failure(
            player_id=1,
            state="terminal",
            failure_code="http_status",
            error="gone",
            retry_after=datetime(2026, 7, 11),
        )


@pytest.mark.unit
def test_scope_snapshot_rejects_cross_scope_rows_before_replace():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="outside"):
        repository.write_scope_snapshot(
            table="whoscored_schedule",
            rows=[{"league": "ENG-Premier League", "game_id": 1}],
            league="INT-World Cup",
            season="2026",
            entity_type="schedule",
            distinct_key="game_id",
        )

    writer.write_dataframe.assert_not_called()
