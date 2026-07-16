"""Offline typed Bronze compatibility coverage for discovered FBref pages."""

from __future__ import annotations

import gzip
import inspect
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

import scrapers.fbref.typed_bronze as typed
import scrapers.fbref.match_parser as match_parser
from scrapers.fbref.match_parser import (
    DatasetParseResult,
    DatasetStatus,
    MatchPageParseError,
    MatchParseResult,
)


MATCH_FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "fbref"
    / "matches"
    / "0701e218.html.gz"
)

SCHEDULE_HTML = """
<html><body>
<!--
<table id="sched_all">
  <thead><tr>
    <th data-stat="gameweek">Wk</th>
    <th data-stat="date">Date</th>
    <th data-stat="home_team">Home</th>
    <th data-stat="away_team">Away</th>
    <th data-stat="score">Score</th>
    <th data-stat="match_report">Match Report</th>
  </tr></thead>
  <tbody><tr>
    <th data-stat="gameweek">1</th>
    <td data-stat="date">2025-08-16</td>
    <td data-stat="home_team">Alpha</td>
    <td data-stat="away_team">Beta</td>
    <td data-stat="score">2–1</td>
    <td data-stat="match_report">
      <a href="/en/matches/abcdef12/Alpha-Beta">Match Report</a>
    </td>
  </tr></tbody>
</table>
-->
</body></html>
"""

SEASON_STATS_HTML = """
<html><body>
<table id="stats_squads_standard_for">
  <thead><tr><th>Squad</th><th>MP</th></tr></thead>
  <tbody><tr>
    <th data-stat="squad">
      <a href="/en/squads/11111111/Alpha">Alpha</a>
    </th>
    <td data-stat="games">1</td>
  </tr></tbody>
</table>
<!--
<table id="stats_standard">
  <thead><tr><th>Player</th><th>Squad</th><th>MP</th></tr></thead>
  <tbody><tr>
    <th data-stat="player">
      <a href="/en/players/22222222/A-Player">1 A Player</a>
    </th>
    <td data-stat="squad">Alpha</td>
    <td data-stat="games">1</td>
  </tr></tbody>
</table>
-->
</body></html>
"""


# A relegation play-off page as FBref actually ships it: the event columns and
# the player summary tables are published but carry no rows, while the team
# stats are complete.  Bronze must read that as a source gap, not schema drift.
EMPTY_PLAYOFF_MATCH_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/11111111/Alpha">Alpha</a></strong></div>
  <div><strong><a href="/en/squads/22222222/Beta">Beta</a></strong></div>
</div>
<div class="event a" id="a"></div>
<div class="event b" id="b"></div>
<div id="team_stats">
  <table>
    <tr><th colspan="2">Possession</th></tr>
    <tr><td><strong>55%</strong></td><td><strong>45%</strong></td></tr>
  </table>
</div>
<table id="stats_11111111_summary">
  <thead><tr><th data-stat="player">Player</th></tr></thead>
  <tbody></tbody>
</table>
<table id="stats_22222222_summary">
  <thead><tr><th data-stat="player">Player</th></tr></thead>
  <tbody></tbody>
</table>
</body></html>
"""

SCORED_EVENT_MATCH_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/11111111/Alpha">Alpha</a></strong></div>
  <div><strong><a href="/en/squads/22222222/Beta">Beta</a></strong></div>
</div>
<div id="events_wrap">
  <div class="event a">
    <div>34&#x2bc; &mdash; 1:0</div>
    <div><a href="/en/players/33333333/A-Scorer">A Scorer</a></div>
  </div>
</div>
</body></html>
"""


class RecordingManager:
    """In-memory description of Trino calls; it performs no external I/O."""

    def __init__(self, *, fail_table: str | None = None) -> None:
        self.columns: dict[str, dict[str, str]] = {}
        self.writes: list[dict] = []
        self.fail_table = fail_table
        self.table_exists_calls: list[str] = []
        self.get_columns_calls: list[str] = []
        self.create_calls: list[str] = []
        self.add_calls: list[tuple[str, str, str]] = []
        self.fail_create_count = 0
        self.fail_describe_count = 0
        self.fail_add_count = 0

    def table_exists(self, schema: str, table: str) -> bool:
        assert schema == "bronze"
        self.table_exists_calls.append(table)
        return table in self.columns

    def arrow_schema_to_trino(self, arrow_schema: pa.Schema) -> dict[str, str]:
        columns = {}
        for field in arrow_schema:
            if pa.types.is_integer(field.type):
                columns[field.name] = "BIGINT"
            elif pa.types.is_floating(field.type):
                columns[field.name] = "DOUBLE"
            elif pa.types.is_timestamp(field.type):
                columns[field.name] = "TIMESTAMP"
            else:
                columns[field.name] = "VARCHAR"
        return columns

    def create_iceberg_table(
        self,
        schema: str,
        table: str,
        columns: dict[str, str],
        partition_columns=None,
    ) -> None:
        assert schema == "bronze"
        assert partition_columns == ["league", "season"]
        self.create_calls.append(table)
        if self.fail_create_count:
            self.fail_create_count -= 1
            raise RuntimeError(f"injected CREATE failure for {table}")
        self.columns[table] = dict(columns)

    def get_table_columns(self, schema: str, table: str) -> dict[str, str]:
        assert schema == "bronze"
        self.get_columns_calls.append(table)
        if self.fail_describe_count:
            self.fail_describe_count -= 1
            raise RuntimeError(f"injected DESCRIBE failure for {table}")
        return dict(self.columns[table])

    def add_column(
        self, schema: str, table: str, column: str, column_type: str
    ) -> None:
        assert schema == "bronze"
        self.add_calls.append((table, column, column_type))
        if self.fail_add_count:
            self.fail_add_count -= 1
            raise RuntimeError(f"injected ALTER failure for {table}.{column}")
        self.columns[table][column] = column_type

    def insert_dataframe_atomic(
        self,
        schema: str,
        table: str,
        frame: pd.DataFrame,
        batch_size: int = 1000,
        delete_filter: str | None = None,
        staging_id: str | None = None,
        single_statement_replace: bool = False,
        target_column_types: dict[str, str] | None = None,
    ) -> int:
        assert schema == "bronze"
        assert batch_size == 1000
        if table == self.fail_table:
            raise RuntimeError(f"injected failure for {table}")
        self.writes.append(
            {
                "table": table,
                "frame": frame.copy(),
                "delete_filter": delete_filter,
                "staging_id": staging_id,
                "single_statement_replace": single_statement_replace,
                "target_column_types": dict(target_column_types or {}),
            }
        )
        return len(frame)


@pytest.mark.unit
def test_compatibility_alias_is_projection_not_an_allowlist() -> None:
    known = typed.TypedSourceContext(
        source_competition_id="9",
        source_season_id="2025-2026",
    )
    unknown = typed.TypedSourceContext(
        source_competition_id="opaque-cup-id",
        source_season_id="edition-final",
        competition_name="Source Cup",
    )

    assert known.league == "ENG-Premier League"
    assert known.season == 2025
    assert unknown.league == "Source Cup"
    assert unknown.season is None
    assert typed.compatibility_league_alias("not-configured") == (
        "FBREF-not-configured"
    )


@pytest.mark.unit
def test_schedule_parse_and_retry_are_source_idempotent() -> None:
    context = typed.TypedSourceContext(
        source_competition_id="new-cup",
        source_season_id="edition-42",
        competition_name="New Cup",
        compatibility_season=2025,
    )
    parsed = typed.parse_schedule_html(SCHEDULE_HTML.encode(), context=context)

    assert parsed.status == DatasetStatus.AVAILABLE
    assert parsed.frame is not None
    assert parsed.frame.loc[0, "match_url"] == (
        "/en/matches/abcdef12/Alpha-Beta"
    )
    assert parsed.frame.loc[0, "league"] == "New Cup"
    assert parsed.frame.loc[0, "source_competition_id"] == "new-cup"
    assert parsed.frame.loc[0, "source_season_id"] == "edition-42"

    manager = RecordingManager()
    writer = typed.FBrefTypedBronzeWriter(manager)
    first = writer.persist_schedule(
        parsed,
        context=context,
        run_id="run-1",
        target_identity="schedule:new-cup:edition-42",
    )
    second = writer.persist_schedule(
        parsed,
        context=context,
        run_id="run-1-retry",
        target_identity="schedule:new-cup:edition-42",
    )

    assert first == second == {"schedule": 1}
    assert [call["table"] for call in manager.writes] == [
        "fbref_schedule",
        "fbref_schedule",
    ]
    assert manager.writes[0]["delete_filter"] == manager.writes[1][
        "delete_filter"
    ]
    assert "source_competition_id = 'new-cup'" in manager.writes[0][
        "delete_filter"
    ]
    assert manager.writes[0]["staging_id"] != manager.writes[1][
        "staging_id"
    ]
    assert all(
        call["single_statement_replace"] for call in manager.writes
    )
    assert manager.table_exists_calls == ["fbref_schedule"]
    assert manager.create_calls == ["fbref_schedule"]
    assert manager.get_columns_calls == ["fbref_schedule"]
    assert manager.writes[0]["target_column_types"] == manager.writes[1][
        "target_column_types"
    ]


@pytest.mark.unit
def test_typed_writer_caches_existing_schema_and_adds_new_columns() -> None:
    manager = RecordingManager()
    manager.columns["fbref_probe"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
        "value": "BIGINT",
    }
    writer = typed.FBrefTypedBronzeWriter(manager)
    original = pd.DataFrame(
        {"league": ["L"], "season": [2025], "value": [1]}
    )

    first = writer._ensure_table("fbref_probe", original)
    second = writer._ensure_table("fbref_probe", original)

    assert first == second
    assert manager.table_exists_calls == ["fbref_probe"]
    assert manager.get_columns_calls == ["fbref_probe"]
    assert manager.add_calls == []

    expanded = original.assign(new_metric=2.5)
    evolved = writer._ensure_table("fbref_probe", expanded)
    writer._ensure_table("fbref_probe", expanded)

    assert manager.add_calls == [("fbref_probe", "new_metric", "DOUBLE")]
    assert evolved["new_metric"] == "DOUBLE"
    assert manager.get_columns_calls == ["fbref_probe"]


@pytest.mark.unit
def test_typed_writer_describes_after_masked_false_existence_probe() -> None:
    class MaskedFalseManager(RecordingManager):
        def table_exists(self, schema: str, table: str) -> bool:
            assert schema == "bronze"
            self.table_exists_calls.append(table)
            return False

        def create_iceberg_table(
            self,
            schema: str,
            table: str,
            columns: dict[str, str],
            partition_columns=None,
        ) -> None:
            assert schema == "bronze"
            self.create_calls.append(table)
            # Model CREATE IF NOT EXISTS against a table that was present even
            # though the preceding metadata query transiently failed.
            if table not in self.columns:
                self.columns[table] = dict(columns)

    manager = MaskedFalseManager()
    manager.columns["fbref_probe"] = {
        "league": "VARCHAR",
        "season": "VARCHAR",
        "value": "DOUBLE",
    }
    writer = typed.FBrefTypedBronzeWriter(manager)
    frame = pd.DataFrame(
        {"league": ["L"], "season": [2025], "value": [1]}
    )

    installed = writer._ensure_table("fbref_probe", frame)

    assert manager.create_calls == ["fbref_probe"]
    assert manager.get_columns_calls == ["fbref_probe"]
    assert installed["season"] == "VARCHAR"
    assert installed["value"] == "DOUBLE"
    assert manager.add_calls == []


@pytest.mark.unit
def test_typed_writer_does_not_cache_failed_create_or_add() -> None:
    manager = RecordingManager()
    manager.fail_create_count = 1
    writer = typed.FBrefTypedBronzeWriter(manager)
    frame = pd.DataFrame(
        {"league": ["L"], "season": [2025], "value": [1]}
    )

    with pytest.raises(RuntimeError, match="CREATE failure"):
        writer._ensure_table("fbref_retry", frame)
    writer._ensure_table("fbref_retry", frame)

    assert manager.table_exists_calls == ["fbref_retry", "fbref_retry"]
    assert manager.create_calls == ["fbref_retry", "fbref_retry"]

    manager.fail_add_count = 1
    expanded = frame.assign(new_metric=2)
    with pytest.raises(RuntimeError, match="ALTER failure"):
        writer._ensure_table("fbref_retry", expanded)
    writer._ensure_table("fbref_retry", expanded)

    assert manager.add_calls[-2:] == [
        ("fbref_retry", "new_metric", "BIGINT"),
        ("fbref_retry", "new_metric", "BIGINT"),
    ]


@pytest.mark.unit
def test_typed_writer_rechecks_existence_after_failed_describe() -> None:
    manager = RecordingManager()
    manager.columns["fbref_retry"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
        "value": "BIGINT",
    }
    manager.fail_describe_count = 1
    writer = typed.FBrefTypedBronzeWriter(manager)
    frame = pd.DataFrame(
        {"league": ["L"], "season": [2025], "value": [1]}
    )

    with pytest.raises(RuntimeError, match="DESCRIBE failure"):
        writer._ensure_table("fbref_retry", frame)
    writer._ensure_table("fbref_retry", frame)

    assert manager.table_exists_calls == ["fbref_retry", "fbref_retry"]
    assert manager.get_columns_calls == ["fbref_retry", "fbref_retry"]


@pytest.mark.unit
def test_tournament_schedule_unions_group_and_knockout_tables() -> None:
    html = """
    <html><body>
      <table id="sched_group"><thead><tr>
        <th data-stat="date">Date</th><th data-stat="home_team">Home</th>
        <th data-stat="away_team">Away</th><th data-stat="match_report">Report</th>
      </tr></thead><tbody><tr>
        <th data-stat="date">2024-06-01</th><td data-stat="home_team">A</td>
        <td data-stat="away_team">B</td><td data-stat="match_report">
          <a href="/en/matches/aaaaaaaa/group">Match Report</a></td>
      </tr></tbody></table>
      <table id="sched_knockout"><thead><tr>
        <th data-stat="date">Date</th><th data-stat="home_team">Home</th>
        <th data-stat="away_team">Away</th><th data-stat="match_report">Report</th>
      </tr></thead><tbody><tr>
        <th data-stat="date">2024-06-15</th><td data-stat="home_team">C</td>
        <td data-stat="away_team">D</td><td data-stat="match_report">
          <a href="/en/matches/bbbbbbbb/final">Match Report</a></td>
      </tr></tbody></table>
    </body></html>
    """
    context = typed.TypedSourceContext("cup", "2024")

    parsed = typed.parse_schedule_html(html, context=context)

    assert parsed.status == DatasetStatus.AVAILABLE
    assert parsed.frame is not None
    assert set(parsed.frame["match_url"]) == {
        "/en/matches/aaaaaaaa/group",
        "/en/matches/bbbbbbbb/final",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "status",
    [
        DatasetStatus.EMPTY,
        DatasetStatus.RESTRICTED,
        DatasetStatus.NOT_APPLICABLE,
    ],
)
def test_empty_like_schedule_replaces_source_partition_with_zero_rows(
    status,
) -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    manager = RecordingManager()
    manager.columns["fbref_schedule"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
    }
    parsed = DatasetParseResult(dataset="schedule", status=status)

    counts = typed.FBrefTypedBronzeWriter(manager).persist_schedule(
        parsed,
        context=context,
        run_id="empty-schedule",
        target_identity="schedule:9:2025-2026",
    )

    assert counts == {"schedule": 0}
    assert len(manager.writes) == 1
    call = manager.writes[0]
    assert call["table"] == "fbref_schedule"
    assert call["frame"].empty
    assert "source_competition_id = '9'" in call["delete_filter"]
    assert "source_season_id = '2025-2026'" in call["delete_filter"]
    assert manager.columns["fbref_schedule"] == {
        "league": "VARCHAR",
        "season": "BIGINT",
        "source_competition_id": "VARCHAR",
        "source_season_id": "VARCHAR",
    }


@pytest.mark.unit
def test_empty_schedule_with_absent_table_is_successful_zero() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    manager = RecordingManager()

    counts = typed.FBrefTypedBronzeWriter(manager).persist_schedule(
        DatasetParseResult("schedule", DatasetStatus.EMPTY),
        context=context,
        run_id="empty-schedule",
        target_identity="schedule:9:2025-2026",
    )

    assert counts == {"schedule": 0}
    assert manager.writes == []

    # Negative existence is deliberately not cached: Trino's table_exists()
    # treats a failed metadata query as false, so a later observation must
    # probe again instead of silently preserving stale rows.
    writer = typed.FBrefTypedBronzeWriter(manager)
    empty = DatasetParseResult("schedule", DatasetStatus.EMPTY)
    writer.persist_schedule(
        empty,
        context=context,
        run_id="empty-schedule-1",
        target_identity="schedule:9:2025-2026",
    )
    writer.persist_schedule(
        empty,
        context=context,
        run_id="empty-schedule-2",
        target_identity="schedule:9:2025-2026",
    )
    assert manager.table_exists_calls == [
        "fbref_schedule",
        "fbref_schedule",
        "fbref_schedule",
    ]


@pytest.mark.unit
def test_empty_typed_replacement_caches_positive_table_metadata() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    manager = RecordingManager()
    manager.columns["fbref_schedule"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
        "source_competition_id": "VARCHAR",
        "source_season_id": "VARCHAR",
    }
    writer = typed.FBrefTypedBronzeWriter(manager)
    empty = DatasetParseResult("schedule", DatasetStatus.EMPTY)

    for run_id in ("empty-1", "empty-2"):
        writer.persist_schedule(
            empty,
            context=context,
            run_id=run_id,
            target_identity="schedule:9:2025-2026",
        )

    assert manager.table_exists_calls == ["fbref_schedule"]
    assert manager.get_columns_calls == ["fbref_schedule"]
    assert len(manager.writes) == 2


@pytest.mark.unit
@pytest.mark.parametrize("status", [DatasetStatus.ERROR, "unknown"])
def test_schedule_error_or_unknown_never_deletes_live_rows(status) -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    manager = RecordingManager()
    manager.columns["fbref_schedule"] = {"league": "VARCHAR"}

    with pytest.raises(typed.TypedBronzePersistenceError):
        typed.FBrefTypedBronzeWriter(manager).persist_schedule(
            DatasetParseResult("schedule", status),
            context=context,
            run_id="unsafe-schedule",
            target_identity="schedule:9:2025-2026",
        )

    assert manager.writes == []


@pytest.mark.unit
def test_opaque_season_keeps_nullable_bigint_legacy_contract() -> None:
    context = typed.TypedSourceContext("opaque-comp", "opaque-season")
    parsed = typed.parse_schedule_html(SCHEDULE_HTML, context=context)

    assert parsed.frame is not None
    assert str(parsed.frame["season"].dtype) == "Int64"
    assert parsed.frame["season"].isna().all()

    manager = RecordingManager()
    typed.FBrefTypedBronzeWriter(manager).persist_schedule(
        parsed,
        context=context,
        run_id="opaque-run",
        target_identity="opaque-target",
    )
    assert manager.columns["fbref_schedule"]["season"] == "BIGINT"


@pytest.mark.unit
def test_match_compatibility_tables_commit_availability_marker_last() -> None:
    html = gzip.decompress(MATCH_FIXTURE.read_bytes())
    context = typed.TypedSourceContext(
        source_competition_id="9",
        source_season_id="2025-2026",
    )
    enabled = {
        "match_events",
        "lineups",
        "match_team_stats",
        "match_player_stats",
    }
    parsed = typed.parse_match_html(
        html,
        match_id="0701e218",
        context=context,
        enabled_datasets=enabled,
    )

    assert not parsed.has_errors
    assert parsed.datasets["match_player_stats"].frame is not None
    player_frame = parsed.datasets["match_player_stats"].frame
    assert set(player_frame["source_competition_id"]) == {"9"}
    assert set(player_frame["source_season_id"]) == {"2025-2026"}

    manager = RecordingManager()
    writer = typed.FBrefTypedBronzeWriter(manager)
    counts = writer.persist_match(
        parsed,
        match_id="0701e218",
        context=context,
        run_id="match-run",
        target_identity="match:0701e218",
    )

    assert counts["match_player_stats"] == len(player_frame)
    assert manager.writes[-2]["table"] == "fbref_match_player_stats"
    assert manager.writes[-1]["table"] == "fbref_dataset_availability"
    assert all(
        call["delete_filter"].startswith("match_id = '0701e218'")
        for call in manager.writes
    )
    staging_ids = [call["staging_id"] for call in manager.writes]
    assert len(staging_ids) == len(set(staging_ids))
    assert all(value.startswith("fbref_") for value in staging_ids)


@pytest.mark.unit
def test_match_parser_miss_preserves_all_live_match_tables(monkeypatch) -> None:
    html = gzip.decompress(MATCH_FIXTURE.read_bytes())
    context = typed.TypedSourceContext("9", "2025-2026")
    monkeypatch.setattr(match_parser, "parse_lineup_table", lambda *_a, **_k: None)

    parsed = typed.parse_match_html(
        html,
        match_id="0701e218",
        context=context,
        enabled_datasets={"lineups", "match_player_stats"},
        require_player_contract=False,
    )
    manager = RecordingManager()
    manager.columns["fbref_lineups"] = {"match_id": "VARCHAR"}
    manager.columns["fbref_match_player_stats"] = {"match_id": "VARCHAR"}

    assert parsed.datasets["lineups"].status == DatasetStatus.ERROR
    assert parsed.datasets["lineups"].reason == "source_container_unparsed"
    with pytest.raises(MatchPageParseError):
        typed.FBrefTypedBronzeWriter(manager).persist_match(
            parsed,
            match_id="0701e218",
            context=context,
            run_id="schema-drift",
            target_identity="match:0701e218",
        )
    assert manager.writes == []


@pytest.mark.unit
def test_explicit_empty_events_container_is_safe_empty() -> None:
    parsed = match_parser.parse_match_html(
        "<html><body><div id='events_wrap'></div></body></html>",
        match_id="empty-events",
        league="ENG-Premier League",
        season=2025,
        enabled_datasets={"match_events"},
        require_player_contract=False,
    )

    assert parsed.status == DatasetStatus.AVAILABLE
    assert parsed.datasets["match_events"].status == DatasetStatus.EMPTY


@pytest.mark.unit
def test_empty_event_columns_are_a_source_gap_not_schema_drift() -> None:
    parsed = match_parser.parse_match_html(
        EMPTY_PLAYOFF_MATCH_HTML,
        match_id="5492b4b4",
        league="GER-Bundesliga",
        season=2017,
        enabled_datasets={"match_events"},
        require_player_contract=False,
    )

    assert parsed.status == DatasetStatus.AVAILABLE
    assert parsed.datasets["match_events"].status == DatasetStatus.EMPTY


@pytest.mark.unit
def test_populated_event_column_that_parses_to_nothing_is_still_drift() -> None:
    parsed = match_parser.parse_match_html(
        SCORED_EVENT_MATCH_HTML,
        match_id="scored-events",
        league="GER-Bundesliga",
        season=2017,
        enabled_datasets={"match_events"},
        require_player_contract=False,
        parser_overrides={"match_events": lambda *_a, **_k: None},
    )

    assert parsed.datasets["match_events"].status == DatasetStatus.ERROR
    assert parsed.datasets["match_events"].reason == "source_container_unparsed"


@pytest.mark.unit
def test_published_but_empty_player_tables_are_empty_not_a_contract_failure() -> None:
    parsed = match_parser.parse_match_html(
        EMPTY_PLAYOFF_MATCH_HTML,
        match_id="5492b4b4",
        league="GER-Bundesliga",
        season=2017,
        enabled_datasets={"match_player_stats", "match_team_stats"},
        require_player_contract=True,
    )

    assert not parsed.has_errors
    assert parsed.datasets["match_player_stats"].status == DatasetStatus.EMPTY
    assert parsed.datasets["match_team_stats"].status == DatasetStatus.AVAILABLE


@pytest.mark.unit
def test_unpublished_player_tables_still_fail_the_required_contract() -> None:
    parsed = match_parser.parse_match_html(
        "<html><body><div id='team_stats'>Possession</div></body></html>",
        match_id="no-player-tables",
        league="GER-Bundesliga",
        season=2017,
        enabled_datasets={"match_player_stats"},
        require_player_contract=True,
    )

    assert parsed.datasets["match_player_stats"].status == DatasetStatus.ERROR
    assert (
        parsed.datasets["match_player_stats"].reason
        == "required_dataset_contract_failed"
    )


@pytest.mark.unit
def test_populated_player_tables_that_parse_to_nothing_fail_the_contract() -> None:
    html = gzip.decompress(MATCH_FIXTURE.read_bytes())
    parsed = match_parser.parse_match_html(
        html,
        match_id="0701e218",
        league="ENG-Premier League",
        season=2025,
        enabled_datasets={"match_player_stats"},
        require_player_contract=True,
        parser_overrides={"match_player_stats": lambda *_a, **_k: None},
    )

    assert parsed.datasets["match_player_stats"].status == DatasetStatus.ERROR
    assert (
        parsed.datasets["match_player_stats"].reason
        == "required_dataset_contract_failed"
    )


@pytest.mark.unit
def test_persistence_failure_never_writes_player_completion_marker() -> None:
    html = gzip.decompress(MATCH_FIXTURE.read_bytes())
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = typed.parse_match_html(
        html,
        match_id="0701e218",
        context=context,
        enabled_datasets={
            "match_events",
            "lineups",
            "match_player_stats",
        },
    )
    manager = RecordingManager(fail_table="fbref_lineups")
    writer = typed.FBrefTypedBronzeWriter(manager)

    with pytest.raises(RuntimeError, match="injected failure"):
        writer.persist_match(
            parsed,
            match_id="0701e218",
            context=context,
            run_id="failed-run",
            target_identity="match:0701e218",
        )

    assert manager.writes
    assert manager.writes[-1]["table"] == "fbref_match_events"
    assert not any(
        call["table"] == "fbref_match_player_stats"
        for call in manager.writes
    )


@pytest.mark.unit
@pytest.mark.parametrize("unsafe_status", [DatasetStatus.ERROR, "unknown"])
def test_match_error_or_unknown_is_rejected_before_any_write(
    unsafe_status,
) -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets={
            "lineups": DatasetParseResult(
                "lineups",
                DatasetStatus.AVAILABLE,
                frame=pd.DataFrame({"match_id": ["unsafe-match"]}),
            ),
            "match_events": DatasetParseResult(
                "match_events", unsafe_status
            ),
            "match_player_stats": DatasetParseResult(
                "match_player_stats", DatasetStatus.EMPTY
            ),
        },
    )
    manager = RecordingManager()
    for table in typed.MATCH_DATASET_TABLES.values():
        manager.columns[table] = {"match_id": "VARCHAR"}

    with pytest.raises(
        (MatchPageParseError, typed.TypedBronzePersistenceError)
    ):
        typed.FBrefTypedBronzeWriter(manager).persist_match(
            parsed,
            match_id="unsafe-match",
            context=context,
            run_id="unsafe-match-run",
            target_identity="match:unsafe-match",
        )

    assert manager.writes == []


@pytest.mark.unit
def test_empty_match_replaces_each_dataset_and_availability_last() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    datasets = {
        name: DatasetParseResult(name, DatasetStatus.EMPTY)
        for name in typed.MATCH_DATASET_TABLES
    }
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets=datasets,
    )
    manager = RecordingManager()
    # One absent table proves that an already-empty dataset is also success.
    for name, table in typed.MATCH_DATASET_TABLES.items():
        if name != "match_officials":
            manager.columns[table] = {"match_id": "VARCHAR"}

    counts = typed.FBrefTypedBronzeWriter(manager).persist_match(
        parsed,
        match_id="empty-match",
        context=context,
        run_id="empty-match-run",
        target_identity="match:empty-match",
    )

    assert counts == {name: 0 for name in typed.MATCH_DATASET_TABLES}
    assert all(
        call["delete_filter"].startswith("match_id = 'empty-match'")
        for call in manager.writes
    )
    assert not any(
        call["table"] == "fbref_match_officials"
        for call in manager.writes
    )
    assert manager.writes[-2]["table"] == "fbref_match_player_stats"
    assert manager.writes[-1]["table"] == "fbref_dataset_availability"


@pytest.mark.unit
def test_available_lineup_and_empty_player_stats_commit_in_safe_order() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets={
            "lineups": DatasetParseResult(
                "lineups",
                DatasetStatus.AVAILABLE,
                frame=pd.DataFrame({"match_id": ["mixed-match"]}),
            ),
            "match_player_stats": DatasetParseResult(
                "match_player_stats", DatasetStatus.EMPTY
            ),
        },
    )
    manager = RecordingManager()
    manager.columns["fbref_lineups"] = {"match_id": "VARCHAR"}
    manager.columns["fbref_match_player_stats"] = {
        "match_id": "VARCHAR"
    }

    counts = typed.FBrefTypedBronzeWriter(manager).persist_match(
        parsed,
        match_id="mixed-match",
        context=context,
        run_id="mixed-match-run",
        target_identity="match:mixed-match",
    )

    assert counts == {"lineups": 1, "match_player_stats": 0}
    assert [call["table"] for call in manager.writes] == [
        "fbref_lineups",
        "fbref_match_player_stats",
        "fbref_dataset_availability",
    ]
    assert manager.writes[-2]["frame"].empty


@pytest.mark.unit
def test_match_does_not_clear_disabled_or_unparsed_datasets() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets={
            "lineups": DatasetParseResult("lineups", DatasetStatus.EMPTY),
            "match_managers": DatasetParseResult(
                "match_managers",
                DatasetStatus.NOT_APPLICABLE,
                reason="dataset_not_requested",
            ),
            "match_player_stats": DatasetParseResult(
                "match_player_stats", DatasetStatus.EMPTY
            ),
        },
    )
    manager = RecordingManager()
    for table in typed.MATCH_DATASET_TABLES.values():
        manager.columns[table] = {"match_id": "VARCHAR"}

    counts = typed.FBrefTypedBronzeWriter(manager).persist_match(
        parsed,
        match_id="scoped-match",
        context=context,
        run_id="scoped-match-run",
        target_identity="match:scoped-match",
    )

    assert counts == {"lineups": 0, "match_player_stats": 0}
    assert [call["table"] for call in manager.writes] == [
        "fbref_lineups",
        "fbref_match_player_stats",
        "fbref_dataset_availability",
    ]
    availability_call = manager.writes[-1]
    availability = availability_call["frame"]
    assert set(availability["dataset"]) == {
        "lineups", "match_player_stats"
    }
    assert "match_managers" not in availability_call["delete_filter"]


@pytest.mark.unit
def test_missing_optional_player_stats_uses_independent_completion() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets={
            "match_events": DatasetParseResult(
                "match_events",
                DatasetStatus.NOT_APPLICABLE,
                reason="source_container_not_published",
            ),
            "match_player_stats": DatasetParseResult(
                "match_player_stats",
                DatasetStatus.NOT_APPLICABLE,
                reason="source_container_not_published",
            ),
        },
    )
    manager = RecordingManager()

    counts = typed.FBrefTypedBronzeWriter(manager).persist_match(
        parsed,
        match_id="optional-match",
        context=context,
        run_id="optional-run",
        target_identity="match:optional-match",
    )

    assert counts == {}
    assert [call["table"] for call in manager.writes] == [
        "fbref_dataset_availability"
    ]
    assert set(manager.writes[0]["frame"]["dataset"]) == {
        "match_events", "match_player_stats"
    }


@pytest.mark.unit
def test_empty_match_failure_propagates_before_completion_replacement() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    datasets = {
        name: DatasetParseResult(name, DatasetStatus.EMPTY)
        for name in typed.MATCH_DATASET_TABLES
    }
    parsed = MatchParseResult(
        parser_version="test",
        parsed_at="2026-07-12T00:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets=datasets,
    )
    manager = RecordingManager(fail_table="fbref_lineups")
    for table in typed.MATCH_DATASET_TABLES.values():
        manager.columns[table] = {"match_id": "VARCHAR"}

    with pytest.raises(RuntimeError, match="injected failure"):
        typed.FBrefTypedBronzeWriter(manager).persist_match(
            parsed,
            match_id="failed-empty-match",
            context=context,
            run_id="failed-empty-match-run",
            target_identity="match:failed-empty-match",
        )

    assert not any(
        call["table"] == "fbref_match_player_stats"
        for call in manager.writes
    )


@pytest.mark.unit
def test_season_standard_page_reuses_existing_pure_finders() -> None:
    context = typed.TypedSourceContext(
        source_competition_id="9",
        source_season_id="2025-2026",
    )
    parsed = typed.parse_season_stats_html(
        SEASON_STATS_HTML,
        context=context,
        stat_route="standard",
    )

    assert set(parsed) == {"player_stats", "team_stats"}
    assert all(
        result.status == DatasetStatus.AVAILABLE
        for result in parsed.values()
    )
    player = parsed["player_stats"].frame
    assert player is not None
    assert player.loc[0, "Player"] == "A Player"
    assert player.loc[0, "player_id"] == "22222222"
    assert player.loc[0, "source_season_id"] == "2025-2026"

    manager = RecordingManager()
    counts = typed.FBrefTypedBronzeWriter(manager).persist_season_stats(
        parsed,
        context=context,
        run_id="season-run",
        target_identity="season-stats:9:2025-2026:standard",
    )

    assert counts == {"player_stats": 1, "team_stats": 1}
    assert {call["table"] for call in manager.writes} == {
        "fbref_player_stats",
        "fbref_team_stats",
    }
    assert all(
        "source_season_id = '2025-2026'" in call["delete_filter"]
        for call in manager.writes
    )


@pytest.mark.unit
def test_unpublished_optional_season_tables_skip_without_deleting_live() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = typed.parse_season_stats_html(
        """
        <html><body><main><h1>2025-2026 Premier League Stats</h1>
          <a href="/en/comps/9/history/Premier-League-Seasons">Seasons</a>
        </main></body></html>
        """,
        context=context,
        stat_route="shooting",
    )
    manager = RecordingManager()
    manager.columns["fbref_player_shooting"] = {"league": "VARCHAR"}
    manager.columns["fbref_team_shooting"] = {"league": "VARCHAR"}

    assert all(
        result.status == DatasetStatus.NOT_APPLICABLE
        for result in parsed.values()
    )
    counts = typed.FBrefTypedBronzeWriter(manager).persist_season_stats(
        parsed,
        context=context,
        run_id="missing-season-tables",
        target_identity="season:9:2025-2026:shooting",
    )
    assert counts == {}
    assert manager.writes == []


@pytest.mark.unit
def test_zero_table_source_shell_is_a_typed_contract_error() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")

    parsed = typed.parse_season_stats_html(
        "<html><body><p>temporary source shell</p></body></html>",
        context=context,
        stat_route="standard",
    )

    assert all(result.status == DatasetStatus.ERROR for result in parsed.values())
    assert {
        result.reason for result in parsed.values()
    } == {"zero_table_season_identity_missing"}
    with pytest.raises(
        typed.TypedBronzePersistenceError,
        match="Refusing season persistence; parser errors",
    ):
        typed.FBrefTypedBronzeWriter(RecordingManager()).persist_season_stats(
            parsed,
            context=context,
            run_id="invalid-zero-table-shell",
            target_identity="season:9:2025-2026:standard",
        )


@pytest.mark.unit
def test_present_but_empty_season_tables_clear_exact_partition() -> None:
    html = """
    <html><body>
      <table id="stats_shooting"><thead><tr><th>Player</th></tr></thead>
        <tbody></tbody></table>
      <table id="stats_squads_shooting_for"><thead><tr><th>Squad</th></tr></thead>
        <tbody></tbody></table>
    </body></html>
    """
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = typed.parse_season_stats_html(
        html,
        context=context,
        stat_route="shooting",
    )
    manager = RecordingManager()
    manager.columns["fbref_player_shooting"] = {"league": "VARCHAR"}
    manager.columns["fbref_team_shooting"] = {"league": "VARCHAR"}

    assert all(result.status == DatasetStatus.EMPTY for result in parsed.values())
    counts = typed.FBrefTypedBronzeWriter(manager).persist_season_stats(
        parsed,
        context=context,
        run_id="empty-season-tables",
        target_identity="season:9:2025-2026:shooting",
    )

    assert counts == {"player_shooting": 0, "team_shooting": 0}
    assert len(manager.writes) == 2


@pytest.mark.unit
def test_empty_season_route_clears_only_produced_datasets() -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = {
        "player_shooting": DatasetParseResult(
            "player_shooting", DatasetStatus.EMPTY
        ),
        "team_shooting": DatasetParseResult(
            "team_shooting", DatasetStatus.RESTRICTED
        ),
    }
    manager = RecordingManager()
    manager.columns["fbref_player_shooting"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
    }
    manager.columns["fbref_team_shooting"] = {
        "league": "VARCHAR",
        "season": "BIGINT",
    }
    # This dataset belongs to another route and must remain untouched.
    manager.columns["fbref_player_passing"] = {"league": "VARCHAR"}

    counts = typed.FBrefTypedBronzeWriter(manager).persist_season_stats(
        parsed,
        context=context,
        run_id="empty-shooting",
        target_identity="season-stats:9:2025-2026:shooting",
    )

    assert counts == {"player_shooting": 0, "team_shooting": 0}
    assert {call["table"] for call in manager.writes} == {
        "fbref_player_shooting",
        "fbref_team_shooting",
    }
    for table in ("fbref_player_shooting", "fbref_team_shooting"):
        assert manager.columns[table]["source_competition_id"] == "VARCHAR"
        assert manager.columns[table]["source_season_id"] == "VARCHAR"


@pytest.mark.unit
@pytest.mark.parametrize("unsafe_status", [DatasetStatus.ERROR, "unknown"])
def test_season_error_or_unknown_is_rejected_before_any_write(
    unsafe_status,
) -> None:
    context = typed.TypedSourceContext("9", "2025-2026")
    parsed = {
        "player_shooting": DatasetParseResult(
            "player_shooting",
            DatasetStatus.AVAILABLE,
            frame=pd.DataFrame({"Player": ["A Player"]}),
        ),
        "team_shooting": DatasetParseResult(
            "team_shooting", unsafe_status
        ),
    }
    manager = RecordingManager()
    manager.columns["fbref_player_shooting"] = {"Player": "VARCHAR"}
    manager.columns["fbref_team_shooting"] = {"Squad": "VARCHAR"}

    with pytest.raises(typed.TypedBronzePersistenceError):
        typed.FBrefTypedBronzeWriter(manager).persist_season_stats(
            parsed,
            context=context,
            run_id="unsafe-shooting",
            target_identity="season-stats:9:2025-2026:shooting",
        )

    assert manager.writes == []


@pytest.mark.unit
def test_module_has_no_transport_or_url_construction_dependency() -> None:
    source = inspect.getsource(typed)

    assert "url_builder" not in source
    assert "requests" not in source
    assert "FBrefFetcher" not in source
    assert "_fetch_page" not in source
