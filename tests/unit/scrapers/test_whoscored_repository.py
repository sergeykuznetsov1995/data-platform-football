from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.parsers import PARSER_VERSION as CORE_PARSER_VERSION
from scrapers.whoscored.repository import (
    PARSER_VERSION,
    BatchConflict,
    ManifestFailure,
    MatchCommit,
    PreviewCommit,
    PreviewFailure,
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


def _preview_commit(*, rows=()) -> PreviewCommit:
    return PreviewCommit(
        game_id=456,
        league="INT-World Cup",
        season="2026",
        game="Home-Away",
        payload_sha256="c" * 64,
        raw_uri="s3://raw/preview.html.gz",
        missing_players=rows,
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
    assert "UNION ALL" not in current_view
    assert "d._game_batch_id IS NULL" not in current_view


@pytest.mark.unit
def test_preview_current_view_strictly_uses_latest_success_batch():
    trino = MagicMock()
    trino.table_exists.return_value = True
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._create_current_views()

    statements = [call.args[0] for call in trino._execute.call_args_list]
    success_view = next(
        sql
        for sql in statements
        if "whoscored_preview_ingest_latest_success AS" in sql
    )
    current_view = next(
        sql for sql in statements if "whoscored_missing_players_current AS" in sql
    )
    assert "WHERE state = 'success'" in success_view
    assert "m.batch_id = d._preview_batch_id" in current_view
    assert "UNION ALL" not in current_view
    assert "d._preview_batch_id IS NULL" not in current_view


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
def test_profile_candidates_are_scope_bounded_and_do_not_refetch_success():
    trino = MagicMock()
    trino.execute_query.return_value = [(7,), (11,)]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    repository.ensure_schema = MagicMock()
    scopes = [
        WhoScoredScope(
            "ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR
        ),
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR),
    ]

    assert repository.list_profile_candidates(scopes=scopes, limit=2) == [7, 11]

    sql = trino.execute_query.call_args.args[0]
    assert "league = 'ENG-Premier League' AND season = '2526'" in sql
    assert "league = 'INT-World Cup' AND season = '2026'" in sql
    assert "SELECT DISTINCT player_id" in sql
    assert "m.state = 'success'" not in sql
    assert "m.state = 'retryable'" in sql
    assert "m.retry_after" in sql
    assert "<= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)" in sql
    assert "m.state = 'parse_failed'" in sql
    assert "m.parser_version IS DISTINCT FROM" in sql
    assert "m.state = 'terminal'" not in sql
    assert "CASE WHEN m.player_id IS NULL THEN 0 ELSE 1 END" in sql
    assert "LIMIT 2" in sql


@pytest.mark.unit
def test_profile_candidates_require_a_nonempty_scope_and_honor_zero_limit():
    trino = MagicMock()
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    assert repository.list_profile_candidates(scopes=[], limit=200) == []
    assert repository.list_profile_candidates(
        scopes=[
            WhoScoredScope(
                "ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR
            )
        ],
        limit=0,
    ) == []
    trino.execute_query.assert_not_called()


@pytest.mark.unit
def test_match_candidates_replay_parser_failures_only_after_parser_change():
    trino = MagicMock()
    trino.execute_query.return_value = []
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    assert repository.list_match_candidates("INT-World Cup", "2026") == []

    sql = trino.execute_query.call_args.args[0]
    assert "m.state = 'retryable'" in sql
    assert "m.state = 'parse_failed'" in sql
    assert "m.parser_version IS DISTINCT FROM" in sql
    assert "m.state = 'terminal'" not in sql
    assert "m.state = 'success'" not in sql


@pytest.mark.unit
def test_preview_candidates_bound_success_refresh_and_honor_failure_backoff():
    trino = MagicMock()
    trino.execute_query.return_value = [
        (456, "Home-Away", datetime(2026, 7, 11), "Home", "Away", 3, True)
    ]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    candidates = repository.list_preview_candidates(
        "INT-World Cup", "2026", limit=10
    )

    assert candidates[0]["attempt_no"] == 3
    assert candidates[0]["force_refresh"] is True
    sql = trino.execute_query.call_args.args[0]
    assert "whoscored_preview_ingest_latest" in sql
    assert "m.state = 'retryable'" in sql
    assert "m.retry_after" in sql
    assert "WHEN m.state = 'retryable'" in sql
    assert "THEN COALESCE(m.attempt_no, 0) + 1" in sql
    assert "WHEN m.state = 'retryable' THEN TRUE" not in sql
    assert "ELSE 1" in sql
    assert "CURRENT_TIMESTAMP - INTERVAL '6' HOUR" in sql
    # Only a current-parser success close to kickoff may bypass raw after its
    # TTL. Older completed matches remain inside the recovery window for
    # unseen/retry/parser-stale work, but cannot trigger another network fetch.
    assert sql.count("AND s.date >= CAST(") == 2
    assert sql.count("CURRENT_TIMESTAMP - INTERVAL '3' HOUR") == 2
    assert "m.state = 'success'" in sql
    assert "m.state = 'parse_failed'" in sql
    assert "m.parser_version IS DISTINCT FROM" in sql
    assert "m.state = 'terminal'" not in sql


@pytest.mark.unit
def test_zero_row_preview_is_committed_only_through_manifest():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [],  # no successful manifest
        [(0,)],  # physical batch before commit
        [(0,)],  # zero-row batch is already physically complete
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit()

    assert repository.commit_preview(commit) == commit.batch_id

    writer.write_dataframe.assert_called_once()
    call = writer.write_dataframe.call_args
    assert call.kwargs["table"] == "whoscored_preview_ingest_manifest"
    row = call.args[0].iloc[0]
    assert row["state"] == "success"
    assert row["missing_players_count"] == 0
    assert row["batch_id"] == commit.batch_id


@pytest.mark.unit
def test_preview_payload_is_appended_before_manifest_commit():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [],  # no successful manifest
        [(0,)],  # physical batch before commit
        [(0,)],  # _write_preview_batch preflight
        [(1,)],  # physical batch after append
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit(
        rows=(
            {
                "league": "INT-World Cup",
                "season": "2026",
                "game_id": 456,
                "team": "Home",
                "player_id": 10,
                "player": "Player",
                "reason": "Injury",
                "status": "Out",
            },
        )
    )

    repository.commit_preview(commit)

    calls = writer.write_dataframe.call_args_list
    assert [call.kwargs["table"] for call in calls] == [
        "whoscored_missing_players",
        "whoscored_preview_ingest_manifest",
    ]
    payload = calls[0].args[0].iloc[0]
    assert payload["_preview_batch_id"] == commit.batch_id
    assert payload["_payload_sha256"] == commit.payload_sha256


@pytest.mark.unit
def test_unchanged_preview_refresh_advances_manifest_without_payload_duplicate():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [(1,)],  # same successful payload was committed before
        [(1,)],  # its physical batch is exact
        [(1,)],  # _write_preview_batch reuses it
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit(
        rows=(
            {
                "team": "Home",
                "player_id": 10,
                "player": "Player",
                "reason": "Injury",
                "status": "Out",
            },
        )
    )

    repository.commit_preview(commit)

    writer.write_dataframe.assert_called_once()
    assert writer.write_dataframe.call_args.kwargs["table"] == (
        "whoscored_preview_ingest_manifest"
    )


@pytest.mark.unit
def test_preview_failure_persists_backoff_without_touching_payload():
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=MagicMock())
    retry_after = datetime(2026, 7, 12)

    repository.record_preview_failure(
        PreviewFailure(
            game_id=456,
            league="INT-World Cup",
            season="2026",
            game="Home-Away",
            state="retryable",
            failure_code="timeout",
            error="timed out",
            retry_after=retry_after,
            attempt_no=2,
        )
    )

    call = writer.write_dataframe.call_args
    assert call.kwargs["table"] == "whoscored_preview_ingest_manifest"
    row = call.args[0].iloc[0]
    assert row["state"] == "retryable"
    assert row["missing_players_count"] is None
    assert row["retry_after"] == retry_after
    assert row["attempt_no"] == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    ("state", "retry_after", "completed"),
    [
        ("retryable", datetime(2026, 7, 11, 12, 0), False),
        ("terminal", None, True),
        ("parse_failed", None, True),
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

    with pytest.raises(ValueError, match="cannot have retry_after"):
        repository.record_profile_failure(
            player_id=1,
            state="parse_failed",
            failure_code="content",
            error="layout changed",
            retry_after=datetime(2026, 7, 11),
        )


@pytest.mark.unit
def test_match_parse_failure_manifest_keeps_raw_identity_for_offline_replay():
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=MagicMock())

    repository.record_failure(
        ManifestFailure(
            game_id=123,
            league="INT-World Cup",
            season="2026",
            state="parse_failed",
            failure_code="content",
            error="layout changed",
            retry_after=None,
            attempt_no=1,
            payload_sha256="a" * 64,
            raw_uri="s3://raw/match.html.gz",
        )
    )

    row = writer.write_dataframe.call_args.args[0].iloc[0]
    assert row["state"] == "parse_failed"
    assert row["payload_sha256"] == "a" * 64
    assert row["raw_uri"] == "s3://raw/match.html.gz"
    assert row["parser_version"] == PARSER_VERSION


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
