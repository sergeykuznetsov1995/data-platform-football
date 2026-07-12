from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import re
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.parsers import PARSER_VERSION as CORE_PARSER_VERSION
from scrapers.whoscored.repository import (
    PARSER_VERSION,
    BatchConflict,
    ManifestFailure,
    MatchCommit,
    ProfileCommit,
    PreviewCommit,
    PreviewFailure,
    WHOSCORED_BUSINESS_COLUMN_CONTRACTS,
    WHOSCORED_BUSINESS_TABLES,
    WhoScoredRepository,
)


class _CleanSchemaTrino:
    """Small DDL fake that applies CREATE IF NOT EXISTS and additive columns."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, str]] = {}
        self.statements: list[str] = []
        self.added: list[tuple[str, str, str]] = []

    def _execute(self, sql: str, **_kwargs):
        self.statements.append(sql)
        match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
            r"iceberg\.bronze\.([a-z0-9_]+)",
            sql,
            flags=re.IGNORECASE,
        )
        if match is None:
            return None
        table = match.group(1).lower()
        if table not in WHOSCORED_BUSINESS_COLUMN_CONTRACTS:
            self.tables.setdefault(table, {})
            return None
        parsed: dict[str, str] = {}
        for line in sql.splitlines():
            column = re.match(
                r'^\s*"([a-z0-9_]+)"\s+'
                r"([A-Z]+(?:\([0-9]+\))?)\s*,?\s*$",
                line,
            )
            if column:
                parsed[column.group(1)] = column.group(2)
        self.tables.setdefault(table, parsed)
        return None

    def get_table_columns(self, _schema: str, table: str):
        return dict(self.tables.get(table, {}))

    def add_column(self, _schema: str, table: str, column: str, data_type: str) -> None:
        self.tables.setdefault(table, {})[column] = data_type
        self.added.append((table, column, data_type))

    def table_exists(self, _schema: str, table: str) -> bool:
        return table in self.tables


def _commit(*, lineups=(), lineups_available=False) -> MatchCommit:
    return MatchCommit(
        game_id=123,
        league="INT-World Cup",
        season="2026",
        game="Home-Away",
        payload_sha256="a" * 64,
        raw_uri="s3://raw/match.json.gz",
        events=({"source_event_id": 10, "team_event_id": 1, "team_id": 1},),
        lineups=lineups,
        lineups_available=lineups_available,
        transport_mode="direct_http",
        dataset_statuses={
            "events": "available",
            "lineups": "available" if lineups else "empty",
        },
    )


def _preview_commit(
    *, game_id=456, digest="c" * 64, rows=(), datasets=None
) -> PreviewCommit:
    additional = datasets or {}
    statuses = {
        "missing_players": "available" if rows else "empty",
        **{
            name: "available" if values else "empty"
            for name, values in additional.items()
        },
    }
    return PreviewCommit(
        game_id=game_id,
        league="INT-World Cup",
        season="2026",
        game="Home-Away",
        payload_sha256=digest,
        raw_uri="s3://raw/preview.html.gz",
        missing_players=rows,
        transport_mode="direct_http",
        datasets=additional,
        dataset_statuses=statuses,
    )


@pytest.mark.unit
def test_repository_uses_the_core_parser_version_and_injected_trino_for_writes():
    trino = MagicMock()

    repository = WhoScoredRepository(trino=trino)

    assert PARSER_VERSION == CORE_PARSER_VERSION
    assert repository.trino is trino
    assert repository.writer._get_trino_manager() is trino


@pytest.mark.unit
def test_clean_schema_creates_all_business_tables_from_exact_contracts():
    trino = _CleanSchemaTrino()
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository.ensure_schema(create_views=False)

    assert set(WHOSCORED_BUSINESS_COLUMN_CONTRACTS) == set(WHOSCORED_BUSINESS_TABLES)
    for table, expected in WHOSCORED_BUSINESS_COLUMN_CONTRACTS.items():
        assert trino.tables[table] == expected
    assert not [
        item for item in trino.added if item[0] in WHOSCORED_BUSINESS_COLUMN_CONTRACTS
    ]


@pytest.mark.unit
def test_payload_json_exists_only_for_business_datasets_that_write_it():
    contracts = WHOSCORED_BUSINESS_COLUMN_CONTRACTS
    tables_with_payload_json = {
        table for table, columns in contracts.items() if "payload_json" in columns
    }

    assert tables_with_payload_json == {
        "whoscored_competitions",
        "whoscored_seasons",
        "whoscored_stages",
        "whoscored_player_stage_participations",
    }

    scope_tables_with_entity_key = {
        table
        for table, columns in contracts.items()
        if table.startswith("whoscored_") and "entity_key" in columns
    }
    assert scope_tables_with_entity_key == {
        "whoscored_match_incidents",
        "whoscored_match_bets",
        "whoscored_stage_standings",
        "whoscored_stage_forms",
        "whoscored_stage_streaks",
        "whoscored_stage_performance",
        "whoscored_team_stage_stats",
        "whoscored_player_stage_stats",
        "whoscored_referee_stage_stats",
    }


@pytest.mark.unit
def test_source_provenance_columns_match_real_parser_producers():
    contracts = WHOSCORED_BUSINESS_COLUMN_CONTRACTS
    source_raw_tables = {
        table for table, columns in contracts.items() if "source_raw_json" in columns
    }
    source_shape_tables = {
        table
        for table, columns in contracts.items()
        if "source_schema_fingerprint" in columns
    }

    # Match-stat rows are a long projection of one entity: they retain the
    # entity shape without duplicating its JSON for every scalar leaf. Player
    # and referee stage rows use the more precise record/document fingerprints.
    assert source_raw_tables == set(WHOSCORED_BUSINESS_TABLES) - {
        "whoscored_team_match_stats",
        "whoscored_player_match_stats",
        "whoscored_player_stage_stats",
        "whoscored_referee_stage_stats",
    }
    assert source_shape_tables == set(WHOSCORED_BUSINESS_TABLES) - {
        "whoscored_player_stage_stats",
        "whoscored_referee_stage_stats",
    }
    for table in (
        "whoscored_team_stage_stats",
        "whoscored_player_stage_stats",
        "whoscored_referee_stage_stats",
    ):
        assert {
            "record_schema_fingerprint",
            "document_schema_fingerprint",
        } <= set(contracts[table])


@pytest.mark.unit
def test_business_contract_adds_missing_columns_and_preserves_legacy_extras():
    trino = _CleanSchemaTrino()
    trino.tables["whoscored_events"] = {
        "game_id": "BIGINT",
        "legacy_event_blob": "VARCHAR",
    }
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._ensure_business_table_contract("whoscored_events")

    assert trino.tables["whoscored_events"]["legacy_event_blob"] == "VARCHAR"
    for column, data_type in WHOSCORED_BUSINESS_COLUMN_CONTRACTS[
        "whoscored_events"
    ].items():
        assert trino.tables["whoscored_events"][column] == data_type
    assert ("whoscored_events", "game_id", "BIGINT") not in trino.added
    assert all("DROP " not in sql.upper() for sql in trino.statements)


@pytest.mark.unit
def test_contract_normalizer_is_deterministic_for_all_null_columns():
    schedule = WhoScoredRepository._normalise_frame_types(
        pd.DataFrame(
            {
                "league": [None],
                "game_id": [None],
                "date": [None],
                "home_score": [None],
                "match_is_opta": [None],
            }
        ),
        table="whoscored_schedule",
    )
    schedule_schema = pa.Table.from_pandas(schedule, preserve_index=False).schema
    assert schedule_schema.field("league").type == pa.string()
    assert schedule_schema.field("game_id").type == pa.int64()
    assert schedule_schema.field("date").type == pa.timestamp("us")
    assert schedule_schema.field("home_score").type == pa.int64()
    assert schedule_schema.field("match_is_opta").type == pa.bool_()

    profile = WhoScoredRepository._normalise_frame_types(
        pd.DataFrame(
            {
                "date_of_birth": [None],
                "age": [None],
                "source_raw_json": [None],
            }
        ),
        table="whoscored_player_profile_versions",
    )
    profile_schema = pa.Table.from_pandas(profile, preserve_index=False).schema
    assert profile_schema.field("date_of_birth").type == pa.date32()
    assert profile_schema.field("age").type == pa.int32()
    assert profile_schema.field("source_raw_json").type == pa.string()


@pytest.mark.unit
def test_contract_keeps_v3_writes_compatible_with_existing_bronze_types():
    contracts = WHOSCORED_BUSINESS_COLUMN_CONTRACTS
    assert contracts["whoscored_events"]["second"] == "DOUBLE"
    assert contracts["whoscored_lineups"]["shirt_no"] == "DOUBLE"
    assert contracts["whoscored_lineups"]["minutes_played"] == "DOUBLE"
    assert contracts["whoscored_schedule"]["home_penalty_score"] == "VARCHAR"
    assert contracts["whoscored_schedule"]["period"] == "BIGINT"

    lineup = WhoScoredRepository._normalise_frame_types(
        pd.DataFrame({"age": [None], "rating": [None]}),
        table="whoscored_lineups",
    )
    lineup_schema = pa.Table.from_pandas(lineup, preserve_index=False).schema
    assert lineup_schema.field("age").type == pa.float64()
    assert lineup_schema.field("rating").type == pa.float64()


@pytest.mark.unit
def test_contract_normalizer_preserves_nullable_populated_values():
    normalised = WhoScoredRepository._normalise_frame_types(
        pd.DataFrame(
            {
                "related_team_event_id": [None, "7"],
                "is_touch": [None, "false"],
                "x": [None, "12.5"],
                "type": [None, "Pass"],
            }
        ),
        table="whoscored_events",
    )

    schema = pa.Table.from_pandas(normalised, preserve_index=False).schema
    assert schema.field("related_team_event_id").type == pa.int64()
    assert schema.field("is_touch").type == pa.bool_()
    assert schema.field("x").type == pa.float64()
    assert schema.field("type").type == pa.string()
    assert pd.isna(normalised.loc[0, "related_team_event_id"])
    assert normalised.loc[1, "related_team_event_id"] == 7
    assert pd.isna(normalised.loc[0, "is_touch"])
    assert bool(normalised.loc[1, "is_touch"]) is False
    assert normalised.loc[1, "x"] == 12.5
    assert normalised.loc[1, "type"] == "Pass"


@pytest.mark.unit
def test_contract_normalizer_rejects_unregistered_parser_columns():
    with pytest.raises(ValueError, match="outside its physical contract"):
        WhoScoredRepository._normalise_frame_types(
            pd.DataFrame({"game_id": [1], "silent_schema_drift": ["bad"]}),
            table="whoscored_events",
        )


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
    assert "UNION ALL" in current_view
    assert "d._game_batch_id IS NULL" in current_view
    assert "batch_id LIKE 'ws2-%'" in success_view


@pytest.mark.unit
def test_preview_current_view_strictly_uses_latest_success_batch():
    trino = MagicMock()
    trino.table_exists.return_value = True
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._create_current_views()

    statements = [call.args[0] for call in trino._execute.call_args_list]
    success_view = next(
        sql for sql in statements if "whoscored_preview_ingest_latest_success AS" in sql
    )
    current_view = next(
        sql for sql in statements if "whoscored_missing_players_current AS" in sql
    )
    assert "WHERE state = 'success'" in success_view
    assert "m.batch_id = d._preview_batch_id" in current_view
    assert "UNION ALL" in current_view
    assert "d._preview_batch_id IS NULL" in current_view
    assert "batch_id LIKE 'wsp2-%'" in success_view


@pytest.mark.unit
def test_profile_current_view_matches_legacy_null_hashes_null_safely():
    trino = MagicMock()
    trino.table_exists.return_value = True
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    repository._ensure_profile_schema()

    statements = [call.args[0] for call in trino._execute.call_args_list]
    roster_view = next(sql for sql in statements if "whoscored_player_roster AS" in sql)
    for source in (
        "whoscored_lineups_current",
        "whoscored_player_match_stats_current",
        "whoscored_events_current",
        "whoscored_player_stage_stats_current",
        "whoscored_missing_players_current",
        "whoscored_preview_lineups_current",
    ):
        assert source in roster_view
    assert "SELECT DISTINCT game_id, player_id, league, season, team" in roster_view
    profile_view = next(
        sql for sql in statements if "silver.whoscored_player_profile_current AS" in sql
    )
    assert "m.payload_sha256 = p.payload_sha256" in profile_view
    assert "m.payload_sha256 IS NULL" in profile_view
    assert "p.payload_sha256 IS NULL" in profile_view
    assert "IS NOT DISTINCT FROM" not in profile_view
    assert "m.parser_version = p.parser_version" in profile_view
    assert "PARTITION BY player_id" in profile_view
    assert "WHERE m.state = 'success'" in profile_view
    assert "JOIN latest m" in profile_view
    assert "m._profile_batch_id = p._profile_batch_id" in profile_view
    assert "WHERE physical_rn = 1" in profile_view


@pytest.mark.unit
def test_commit_matches_rejects_lineup_availability_contradictions_before_io():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="lineups_available disagrees"):
        repository.commit_matches((_commit(lineups=(), lineups_available=True),))

    trino.execute_query.assert_not_called()
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_commit_matches_rejects_cross_scope_batch_id_collision_before_io():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    first = _commit()
    second = replace(first, league="OTHER", season="2027")

    with pytest.raises(BatchConflict, match="match batch id collision"):
        repository.commit_matches((first, second))

    trino.execute_query.assert_not_called()
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_final_opta_match_rejects_a_valid_but_truncated_event_prefix():
    repository = WhoScoredRepository(writer=MagicMock(), trino=MagicMock())
    events = tuple(
        {
            "source_event_id": index + 1,
            "team_event_id": index + 1,
            "team_id": 1,
            "expanded_minute": index,
        }
        for index in range(22)
    )
    commit = replace(
        _commit(),
        events=events,
        is_opta=True,
        schedule_status=6,
        dataset_statuses={"events": "available", "lineups": "empty"},
    )

    with pytest.raises(ValueError, match="incomplete final Opta events"):
        repository.commit_matches((commit,))


@pytest.mark.unit
def test_final_opta_match_cannot_replace_a_larger_published_event_stream(
    monkeypatch,
):
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    events = tuple(
        {
            "source_event_id": index + 1,
            "team_event_id": index + 1,
            "team_id": 1,
            "expanded_minute": min(index, 95),
        }
        for index in range(100)
    )
    commit = replace(
        _commit(),
        events=events,
        is_opta=True,
        schedule_status=6,
        dataset_statuses={"events": "available", "lineups": "empty"},
    )
    monkeypatch.setattr(
        repository,
        "_current_dataset_counts",
        lambda _commits: {
            (commit.league, commit.season, commit.game_id): {"events": 420}
        },
    )

    with pytest.raises(BatchConflict, match="events completeness regression"):
        repository.commit_matches((commit,))

    trino.execute_query.assert_not_called()
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_completed_non_opta_match_cannot_shrink_any_published_dataset(monkeypatch):
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = replace(
        _commit(
            lineups=({"player_id": 10},),
            lineups_available=True,
        ),
        is_opta=False,
        schedule_status=6,
        dataset_statuses={"events": "available", "lineups": "available"},
    )
    monkeypatch.setattr(
        repository,
        "_current_dataset_counts",
        lambda _commits: {
            (commit.league, commit.season, commit.game_id): {"lineups": 2}
        },
    )

    with pytest.raises(BatchConflict, match="lineups completeness regression"):
        repository.commit_matches((commit,))

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_final_opta_guard_accepts_a_source_declared_shortened_match():
    repository = WhoScoredRepository(writer=MagicMock(), trino=MagicMock())
    events = tuple(
        {
            "source_event_id": index + 1,
            "team_event_id": index + 1,
            "team_id": 1,
            "expanded_minute": min(index, 59),
        }
        for index in range(60)
    )
    commit = replace(
        _commit(),
        events=events,
        is_opta=True,
        schedule_status=6,
        dataset_statuses={"events": "available", "lineups": "empty"},
        datasets={"matches": ({"expanded_max_minute": 60},)},
    )
    commit = replace(
        commit,
        dataset_statuses={
            **commit.dataset_statuses,
            "matches": "available",
        },
    )

    repository.validate_match_commit(commit)


@pytest.mark.unit
def test_idempotent_match_commit_verifies_manifest_and_physical_counts():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [(_commit().batch_id, 1, 0, '{"events":1,"lineups":0}', 1)],
        [],  # events physical batch was lost
        [],  # lineups physical batch
    ]
    trino.table_exists.return_value = True
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(BatchConflict, match="manifest=.*physical=.*parser="):
        repository.commit_matches((_commit(),))

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("latest_state", "latest_age", "expect_heartbeat"),
    [
        ("success", timedelta(days=1), False),
        ("success", timedelta(days=8), True),
        ("retryable", timedelta(hours=1), True),
        ("parse_failed", timedelta(hours=1), True),
    ],
)
def test_unchanged_match_refresh_advances_only_a_due_or_failed_manifest(
    latest_state, latest_age, expect_heartbeat
):
    commit = _commit()
    now = datetime.now()
    trino = MagicMock()
    trino.table_exists.return_value = True
    trino.execute_query.side_effect = [
        [
            (
                commit.batch_id,
                1,
                0,
                '{"events":1,"lineups":0}',
                2,
            )
        ],
        [(commit.batch_id, 1)],
        [],
        [(commit.batch_id, 1)],
        [],
        [
            (
                commit.league,
                commit.season,
                commit.game_id,
                commit.batch_id,
                latest_state,
                now - latest_age,
            )
        ],
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    assert repository.commit_matches([commit]) == (commit.batch_id,)

    if expect_heartbeat:
        writer.write_dataframe.assert_called_once()
        call = writer.write_dataframe.call_args
        assert call.kwargs["table"] == "whoscored_match_ingest_manifest"
        row = call.args[0].iloc[0]
        assert row["state"] == "success"
        assert row["batch_id"] == commit.batch_id
    else:
        writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_profile_manifest_cannot_hide_a_missing_physical_version():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [],  # no physical profile version
        [(99, "b" * 64, PARSER_VERSION, 0)],  # successful manifest exists
        [],  # no physical participation rows
    ]
    trino.table_exists.return_value = True
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(BatchConflict, match="committed but has 0 physical"):
        repository.commit_profiles(
            [
                ProfileCommit(
                    player_id=99,
                    profile={"name": "Player"},
                    payload_sha256="b" * 64,
                    raw_uri="s3://raw/profile.html.gz",
                    transport_mode="direct_http",
                )
            ]
        )

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_profile_commit_rejects_unavailable_participation_structure_before_io():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="participation dataset has invalid status"):
        repository.commit_profiles(
            [
                ProfileCommit(
                    player_id=99,
                    profile={"player_id": 99, "name": "Player"},
                    payload_sha256="b" * 64,
                    raw_uri="s3://raw/profile.html.gz",
                    transport_mode="direct_http",
                    participations_status="not_available",
                )
            ]
        )

    trino.execute_query.assert_not_called()
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("latest_state", "latest_age", "expect_heartbeat"),
    [
        ("success", timedelta(days=1), False),
        ("success", timedelta(days=91), True),
        ("retryable", timedelta(hours=1), True),
        ("parse_failed", timedelta(hours=1), True),
    ],
)
def test_unchanged_profile_refresh_advances_only_a_due_or_failed_manifest(
    latest_state, latest_age, expect_heartbeat
):
    digest = "b" * 64
    commit = ProfileCommit(
        player_id=99,
        profile={"player_id": 99, "name": "Player"},
        payload_sha256=digest,
        raw_uri="s3://raw/profile.html.gz",
        transport_mode="raw_cache",
    )
    now = datetime.now()
    trino = MagicMock()
    trino.table_exists.return_value = False
    trino.execute_query.side_effect = [
        [(99, digest, PARSER_VERSION, 1)],  # physical content version
        [(99, digest, PARSER_VERSION, 0)],  # any historical success
        [(99, digest, PARSER_VERSION, 1)],  # verified physical version
        [
            (
                99,
                digest,
                PARSER_VERSION,
                latest_state,
                commit.batch_id,
                now - latest_age,
            )
        ],
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    assert repository.commit_profiles([commit]) == (commit.batch_id,)

    if expect_heartbeat:
        writer.write_dataframe.assert_called_once()
        call = writer.write_dataframe.call_args
        assert call.kwargs["table"] == "whoscored_profile_ingest_manifest"
        row = call.args[0].iloc[0]
        assert row["state"] == "success"
        assert row["_profile_batch_id"] == commit.batch_id
    else:
        writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_profile_candidates_are_scope_bounded_and_do_not_refetch_success():
    trino = MagicMock()
    trino.execute_query.return_value = [(7,), (11,)]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    repository.ensure_schema = MagicMock()
    scopes = [
        WhoScoredScope("ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR),
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR),
    ]

    assert repository.list_profile_candidates(scopes=scopes, limit=2) == [7, 11]

    sql = trino.execute_query.call_args.args[0]
    assert "league = 'ENG-Premier League' AND season = '2526'" in sql
    assert "league = 'INT-World Cup' AND season = '2026'" in sql
    assert "SELECT DISTINCT player_id" in sql
    assert "m.state = 'success'" in sql
    assert "m._profile_batch_id NOT LIKE 'wspr2-%'" in sql
    assert "m.state = 'retryable'" in sql
    assert "m.retry_after" in sql
    assert "<= CAST(CURRENT_TIMESTAMP AS TIMESTAMP)" in sql
    assert "m.state = 'parse_failed'" in sql
    assert "m.parser_version IS DISTINCT FROM" in sql
    assert "m.state = 'terminal'" not in sql
    assert "WHEN m.player_id IS NULL THEN 0" in sql
    assert "m._profile_batch_id NOT LIKE 'wspr2-%' THEN 1" in sql
    assert "LIMIT 2" in sql


@pytest.mark.unit
def test_profile_candidates_require_a_nonempty_scope_and_honor_zero_limit():
    trino = MagicMock()
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    assert repository.list_profile_candidates(scopes=[], limit=200) == []
    assert (
        repository.list_profile_candidates(
            scopes=[
                WhoScoredScope("ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR)
            ],
            limit=0,
        )
        == []
    )
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
    assert "m.state = 'success'" in sql
    assert "m.batch_id NOT LIKE 'ws2-%'" in sql


@pytest.mark.unit
def test_backfill_match_candidates_explicitly_include_failed_manifest_states():
    trino = MagicMock()
    trino.execute_query.return_value = []
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    assert (
        repository.list_match_candidates("INT-World Cup", "2026", include_failed=True)
        == []
    )

    sql = trino.execute_query.call_args.args[0]
    assert "m.state IN ('terminal', 'parse_failed')" in sql


@pytest.mark.unit
def test_preview_candidates_bound_success_refresh_and_honor_failure_backoff():
    trino = MagicMock()
    trino.execute_query.return_value = [
        (456, "Home-Away", datetime(2026, 7, 11), "Home", "Away", 3, True)
    ]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)

    candidates = repository.list_preview_candidates("INT-World Cup", "2026", limit=10)

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
        [],  # no physical rows before commit
        [],  # zero-row batch is physically complete after commit
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit()

    assert repository.commit_previews((commit,)) == (commit.batch_id,)

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
    trino.execute_query.side_effect = [
        [],  # no successful manifest
        [],  # no physical rows before commit
        [(commit.batch_id, 1)],  # physical batch after append
    ]

    repository.commit_previews((commit,))

    calls = writer.write_dataframe.call_args_list
    assert [call.kwargs["table"] for call in calls] == [
        "whoscored_missing_players",
        "whoscored_preview_ingest_manifest",
    ]
    payload = calls[0].args[0].iloc[0]
    assert payload["_preview_batch_id"] == commit.batch_id
    assert payload["_payload_sha256"] == commit.payload_sha256


@pytest.mark.unit
def test_preview_chunk_writes_one_dataset_frame_and_one_manifest_frame():
    writer = MagicMock()
    trino = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commits = (
        _preview_commit(
            game_id=456,
            digest="c" * 64,
            rows=({"team": "Home", "player_id": 10},),
        ),
        _preview_commit(
            game_id=457,
            digest="d" * 64,
            rows=({"team": "Away", "player_id": 11},),
        ),
    )
    trino.execute_query.side_effect = [
        [],  # no successful manifests
        [],  # no physical batches
        [(commits[0].batch_id, 1), (commits[1].batch_id, 1)],
    ]

    assert repository.commit_previews(commits) == tuple(
        commit.batch_id for commit in commits
    )

    calls = writer.write_dataframe.call_args_list
    assert [call.kwargs["table"] for call in calls] == [
        "whoscored_missing_players",
        "whoscored_preview_ingest_manifest",
    ]
    payload = calls[0].args[0]
    manifest = calls[1].args[0]
    assert len(payload) == len(manifest) == 2
    assert set(payload["_preview_batch_id"]) == {commit.batch_id for commit in commits}
    assert set(manifest["game_id"]) == {456, 457}


@pytest.mark.unit
def test_preview_complete_orphan_is_reused_after_crash():
    writer = MagicMock()
    trino = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit(rows=({"team": "Home", "player_id": 10},))
    trino.execute_query.side_effect = [
        [],  # the prior attempt crashed before its manifest write
        [(commit.batch_id, 1)],  # complete orphan payload
        [(commit.batch_id, 1)],  # verification
    ]

    repository.commit_previews((commit,))

    writer.write_dataframe.assert_called_once()
    assert writer.write_dataframe.call_args.kwargs["table"] == (
        "whoscored_preview_ingest_manifest"
    )


@pytest.mark.unit
def test_preview_partial_orphan_fails_closed():
    writer = MagicMock()
    trino = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    commit = _preview_commit(
        rows=(
            {"team": "Home", "player_id": 10},
            {"team": "Home", "player_id": 11},
        )
    )
    trino.execute_query.side_effect = [
        [],
        [(commit.batch_id, 1)],
    ]

    with pytest.raises(BatchConflict, match="partial physical batch 1/2"):
        repository.commit_previews((commit,))

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_preview_not_available_dataset_cannot_hide_previous_snapshot():
    repository = WhoScoredRepository(writer=MagicMock(), trino=MagicMock())
    commit = _preview_commit(
        datasets={"preview_lineups": ()},
    )
    commit = replace(
        commit,
        dataset_statuses={
            "missing_players": "empty",
            "preview_lineups": "not_available",
        },
    )

    with pytest.raises(ValueError, match="not available"):
        repository.validate_preview_commit(commit)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("latest_state", "latest_age", "expect_heartbeat"),
    [
        ("success", timedelta(hours=1), False),
        ("success", timedelta(hours=7), True),
        ("retryable", timedelta(hours=1), True),
        ("parse_failed", timedelta(hours=1), True),
    ],
)
def test_unchanged_preview_refresh_advances_only_a_due_or_failed_manifest(
    latest_state, latest_age, expect_heartbeat
):
    trino = MagicMock()
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
    trino.execute_query.side_effect = [
        [
            (commit.batch_id, 1, '{"missing_players":1}', 1)
        ],  # same successful payload was committed before
        [(commit.batch_id, 1)],  # its physical batch is exact
        [(commit.batch_id, 1)],  # verification reuses it
        [
            (
                commit.league,
                commit.season,
                commit.game_id,
                commit.batch_id,
                latest_state,
                datetime.now(timezone.utc).replace(tzinfo=None) - latest_age,
            )
        ],
    ]

    repository.commit_previews((commit,))

    if expect_heartbeat:
        writer.write_dataframe.assert_called_once()
        assert writer.write_dataframe.call_args.kwargs["table"] == (
            "whoscored_preview_ingest_manifest"
        )
    else:
        writer.write_dataframe.assert_not_called()


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
def test_scope_bundle_rejects_cross_scope_rows_before_publish():
    trino = MagicMock()
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="outside"):
        repository.commit_scope_bundle(
            league="INT-World Cup",
            season="2026",
            entity_group="season",
            datasets={
                "whoscored_schedule": [{"league": "ENG-Premier League", "game_id": 1}]
            },
            distinct_keys={"whoscored_schedule": "game_id"},
            payload_sha256="a" * 64,
            raw_uris=["s3://raw/schedule.json.gz"],
        )

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_scope_bundle_authoritative_empty_cannot_shrink_published_snapshot():
    trino = MagicMock()
    trino.execute_query.side_effect = [
        [],
        [('{"whoscored_schedule":1}',)],
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(ValueError, match="published snapshot cannot shrink"):
        repository.commit_scope_bundle(
            league="INT-World Cup",
            season="2026",
            entity_group="season",
            datasets={"whoscored_schedule": []},
            distinct_keys={"whoscored_schedule": "game_id"},
            payload_sha256="a" * 64,
            raw_uris=["s3://raw/schedule.json.gz"],
            source_empty={"whoscored_schedule"},
        )

    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_scope_bundle_feed_states_are_canonical_and_idempotent():
    trino = MagicMock()
    trino.table_exists.return_value = True
    stored_counts = '{"whoscored_schedule":1}'
    stored_states = (
        '{"__feeds__":{"23752:player:summary":"empty",'
        '"23752:team:summary":"available"},'
        '"whoscored_schedule":"available"}'
    )
    trino.execute_query.side_effect = [
        [(stored_counts, stored_states)],
        [(1,)],
        [(stored_counts, stored_states)],
        [(1,)],
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)
    kwargs = {
        "league": "INT-World Cup",
        "season": "2026",
        "entity_group": "season",
        "datasets": {
            "whoscored_schedule": [
                {
                    "league": "INT-World Cup",
                    "season": "2026",
                    "game_id": 1,
                }
            ]
        },
        "distinct_keys": {"whoscored_schedule": "game_id"},
        "payload_sha256": "a" * 64,
        "raw_uris": ["s3://raw/schedule.json.gz"],
    }

    first = repository.commit_scope_bundle(
        **kwargs,
        feed_states={
            "23752:team:summary": "available",
            "23752:player:summary": "empty",
        },
    )
    second = repository.commit_scope_bundle(
        **kwargs,
        feed_states={
            "23752:player:summary": "empty",
            "23752:team:summary": "available",
        },
    )

    assert first == second
    assert first.startswith("wss2-")
    assert (
        trino.execute_query.call_args_list[0].args[0]
        == (trino.execute_query.call_args_list[2].args[0])
    )
    writer.write_dataframe.assert_not_called()


@pytest.mark.unit
def test_scope_bundle_feed_status_changes_batch_identity():
    trino = MagicMock()
    trino.table_exists.return_value = True
    counts = '{"whoscored_schedule":1}'
    trino.execute_query.side_effect = [
        [
            (
                counts,
                '{"__feeds__":{"23752:team:summary":"available"},'
                '"whoscored_schedule":"available"}',
            )
        ],
        [(1,)],
        [
            (
                counts,
                '{"__feeds__":{"23752:team:summary":"empty"},'
                '"whoscored_schedule":"available"}',
            )
        ],
        [(1,)],
    ]
    repository = WhoScoredRepository(writer=MagicMock(), trino=trino)
    kwargs = {
        "league": "INT-World Cup",
        "season": "2026",
        "entity_group": "season",
        "datasets": {"whoscored_schedule": [{"game_id": 1}]},
        "distinct_keys": {"whoscored_schedule": "game_id"},
        "payload_sha256": "a" * 64,
        "raw_uris": ["s3://raw/schedule.json.gz"],
    }

    available = repository.commit_scope_bundle(
        **kwargs,
        feed_states={"23752:team:summary": "available"},
    )
    empty = repository.commit_scope_bundle(
        **kwargs,
        feed_states={"23752:team:summary": "empty"},
    )

    assert available != empty


@pytest.mark.unit
def test_scope_bundle_fails_closed_when_stored_feed_states_disagree():
    trino = MagicMock()
    trino.execute_query.return_value = [
        (
            '{"whoscored_schedule":1}',
            '{"__feeds__":{"23752:team:summary":"empty"},'
            '"whoscored_schedule":"available"}',
        )
    ]
    writer = MagicMock()
    repository = WhoScoredRepository(writer=writer, trino=trino)

    with pytest.raises(BatchConflict, match="states: manifest="):
        repository.commit_scope_bundle(
            league="INT-World Cup",
            season="2026",
            entity_group="season",
            datasets={"whoscored_schedule": [{"game_id": 1}]},
            distinct_keys={"whoscored_schedule": "game_id"},
            payload_sha256="a" * 64,
            raw_uris=["s3://raw/schedule.json.gz"],
            feed_states={"23752:team:summary": "available"},
        )

    writer.write_dataframe.assert_not_called()
