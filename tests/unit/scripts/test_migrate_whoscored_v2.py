from __future__ import annotations

import re
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "migrate_whoscored_v2.py"
SPEC = importlib.util.spec_from_file_location("migrate_whoscored_v2", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
migration = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migration
SPEC.loader.exec_module(migration)


class _TableTrino:
    def __init__(self, tables=(), *, columns=None, fail_on=None):
        self.tables = set(tables)
        self.columns = columns or {}
        self.fail_on = fail_on
        self.executed: list[str] = []

    def table_exists(self, schema, table):
        return table in self.tables

    def get_table_columns(self, schema, table):
        return self.columns.get(table, {})

    def execute_query(self, sql):
        return []

    def _execute(self, sql):
        compact = " ".join(sql.split())
        self.executed.append(compact)
        if self.fail_on and self.fail_on in compact:
            raise RuntimeError("forced DDL failure")
        rename = re.search(
            r"ALTER TABLE iceberg\.bronze\.([A-Za-z0-9_]+) RENAME TO ([A-Za-z0-9_]+)",
            compact,
        )
        if rename:
            source, target = rename.groups()
            self.tables.remove(source)
            self.tables.add(target)
            return
        drop = re.search(r"DROP TABLE iceberg\.bronze\.([A-Za-z0-9_]+)", compact)
        if drop:
            self.tables.discard(drop.group(1))


@pytest.mark.unit
def test_timestamp_suffix_is_valid_but_sql_fragments_are_not():
    assert migration._suffix("20260710153000") == "20260710153000"
    with pytest.raises(ValueError, match="unsafe migration suffix"):
        migration._suffix("20260710;DROP_TABLE")


@pytest.mark.unit
def test_projection_is_idempotent_for_an_already_v2_event_schema():
    columns = [
        "league",
        "season",
        "game_id",
        "_ingested_at",
        "_batch_id",
        "source_event_id",
        "_payload_sha256",
        "_parser_version",
        "_game_batch_id",
    ]

    projection = migration._projection("whoscored_events", columns)

    assert sum("AS \"source_event_id\"" in item for item in projection) == 1
    assert sum("AS \"_game_batch_id\"" in item for item in projection) == 1


@pytest.mark.unit
def test_legacy_batch_id_normalizes_mixed_physical_game_id_types():
    base_columns = [
        "league",
        "season",
        "game_id",
        "_ingested_at",
        "_batch_id",
    ]

    event_batch = next(
        item
        for item in migration._projection("whoscored_events", base_columns)
        if "AS _game_batch_id" in item
    )
    lineup_batch = next(
        item
        for item in migration._projection("whoscored_lineups", base_columns)
        if "AS _game_batch_id" in item
    )

    # Production legacy schemas used BIGINT for events.game_id and DOUBLE for
    # lineups.game_id.  Both must hash the canonical integer representation.
    expected_cast = 'CAST(CAST("game_id" AS BIGINT) AS VARCHAR)'
    assert expected_cast in event_batch
    assert expected_cast in lineup_batch
    assert event_batch == lineup_batch


@pytest.mark.unit
def test_legacy_preview_projection_adds_logical_commit_metadata():
    columns = [
        "league",
        "season",
        "game_id",
        "_ingested_at",
        "_batch_id",
    ]

    projection = migration._projection("whoscored_missing_players", columns)

    preview_batch = next(
        item for item in projection if "AS _preview_batch_id" in item
    )
    assert "legacy-preview-" in preview_batch
    assert 'CAST(CAST("game_id" AS BIGINT) AS VARCHAR)' in preview_batch
    assert "'legacy-v1' AS _parser_version" in projection
    assert "CAST(NULL AS VARCHAR) AS _payload_sha256" in projection


@pytest.mark.unit
def test_required_identity_keys_are_fail_closed():
    with pytest.raises(RuntimeError, match="game_id"):
        migration._available_keys(
            "whoscored_events",
            ["league", "season", "_ingested_at", "_batch_id"],
            migration.EVENT_KEY,
        )


@pytest.mark.unit
def test_shadow_build_refuses_to_silently_drop_null_scopes():
    table = "whoscored_events"
    trino = _TableTrino(
        {table},
        columns={
            table: {
                "league": "varchar",
                "season": "varchar",
                "game_id": "bigint",
                "_ingested_at": "timestamp(6)",
                "_batch_id": "varchar",
            }
        },
    )
    trino.execute_query = MagicMock(side_effect=[[(10,)], [(1,)]])

    with pytest.raises(RuntimeError, match="without league/season"):
        migration.build_shadow(trino, table, suffix="20260710")

    assert trino.executed == []


@pytest.mark.unit
def test_completed_shadow_is_reused_without_rewriting_it():
    table = "whoscored_events"
    shadow = f"{table}_v2_20260710"
    source_columns = {
        "league": "varchar",
        "season": "varchar",
        "game_id": "bigint",
        "_ingested_at": "timestamp(6)",
        "_batch_id": "varchar",
    }
    shadow_columns = {
        **source_columns,
        "source_event_id": "bigint",
        "_payload_sha256": "varchar",
        "_parser_version": "varchar",
        "_game_batch_id": "varchar",
    }
    trino = _TableTrino(
        {table, shadow}, columns={table: source_columns, shadow: shadow_columns}
    )
    trino.execute_query = MagicMock(
        side_effect=[
            [(10,)],
            [(0,)],
            [("ENG-Premier League", "2025-2026")],
            [(5,)],
            [(5,)],
            [("ENG-Premier League", "2025-2026")],
            [(5,)],
            [(0,)],
        ]
    )

    result = migration.build_shadow(trino, table, suffix="20260710")

    assert result == (shadow, 10, 5)
    assert trino.executed == []


@pytest.mark.unit
def test_swap_restores_source_name_when_second_rename_fails():
    table = "whoscored_events"
    shadow = f"{table}_v2_20260710"
    trino = _TableTrino(
        {table, shadow},
        fail_on=f"ALTER TABLE iceberg.bronze.{shadow} RENAME TO {table}",
    )

    with pytest.raises(RuntimeError, match="forced DDL failure"):
        migration.swap_shadow(trino, table, shadow, suffix="20260710")

    assert table in trino.tables
    assert f"{table}_legacy_20260710" not in trino.tables
    assert shadow in trino.tables


@pytest.mark.unit
def test_wrong_or_repeated_rollback_suffix_is_a_noop():
    trino = _TableTrino({migration.MATCH_MANIFEST_TABLE})

    assert migration.rollback(trino, "20260710") == []
    assert trino.executed == []
    assert migration.MATCH_MANIFEST_TABLE in trino.tables


@pytest.mark.unit
def test_rollback_restores_legacy_and_isolates_v2_state_once():
    table = "whoscored_events"
    backup = f"{table}_legacy_20260710"
    trino = _TableTrino({table, backup, migration.MATCH_MANIFEST_TABLE})

    restored = migration.rollback(trino, "20260710")

    assert restored == [table]
    assert table in trino.tables
    assert backup not in trino.tables
    assert f"{table}_v2_failed_20260710" in trino.tables
    assert migration.MATCH_MANIFEST_TABLE not in trino.tables
    assert f"{migration.MATCH_MANIFEST_TABLE}_v2_failed_20260710" in trino.tables
    executed_count = len(trino.executed)

    assert migration.rollback(trino, "20260710") == []
    assert len(trino.executed) == executed_count


@pytest.mark.unit
def test_profile_seed_uses_typed_nulls_and_correlated_aliases():
    source = "whoscored_player_profile"
    tables = {
        source,
        migration.PROFILE_VERSIONS_TABLE,
        migration.PROFILE_MANIFEST_TABLE,
    }
    trino = _TableTrino(
        tables,
        columns={
            source: {
                "player_id": "double",
                "name": "varchar",
                "_ingested_at": "timestamp(6)",
                "_batch_id": "varchar",
            }
        },
    )
    trino.execute_query = MagicMock(return_value=[(1,)])

    assert migration.seed_profiles(trino) == 1

    version_insert = trino.executed[0]
    assert 'CAST(NULL AS BIGINT) AS "current_team_id"' in version_insert
    assert "existing.player_id = CAST(ranked.player_id AS BIGINT)" in version_insert
    assert "WHERE parser_version = 'legacy-v1'" in version_insert
    assert "existing.player_id IS NULL" in version_insert
    assert "NOT EXISTS" not in version_insert
    assert "WHERE v.parser_version = 'legacy-v1'" in trino.executed[1]
    assert "SELECT v.player_id, v.payload_sha256" in trino.executed[1]
    assert "committed.player_id = v.player_id" in trino.executed[1]
    assert "committed.player_id IS NULL" in trino.executed[1]
    assert "NOT EXISTS" not in trino.executed[1]
    assert "CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))" in version_insert
    assert "CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))" in trino.executed[1]


@pytest.mark.unit
def test_match_seed_uses_trino_boolean_aggregate_and_naive_timestamp():
    tables = {
        "whoscored_events",
        "whoscored_lineups",
        "whoscored_schedule",
        migration.MATCH_MANIFEST_TABLE,
    }
    trino = _TableTrino(tables)
    trino.execute_query = MagicMock(return_value=[(1,)])

    assert migration.seed_match_manifest(trino) == 1

    statement = trino.executed[0]
    assert "COALESCE(BOOL_OR(s.match_is_opta), TRUE)" in statement
    assert "CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))" in statement
    assert "MAX(s.match_is_opta)" not in statement
    assert "committed.league = e.league" in statement
    assert "committed.season = e.season" in statement
    assert "committed.game_id = e.game_id" in statement
    assert "committed.game_id IS NULL" in statement
    assert "l.league = e.league" in statement
    assert "s.league = e.league" in statement
    assert "USING (league, season, game_id)" not in statement
    assert "NOT EXISTS" not in statement


@pytest.mark.unit
def test_preview_seed_commits_exact_legacy_nonempty_batches():
    tables = {
        "whoscored_missing_players",
        "whoscored_schedule",
        migration.PREVIEW_MANIFEST_TABLE,
    }
    trino = _TableTrino(tables)
    trino.execute_query = MagicMock(return_value=[(1,)])

    assert migration.seed_preview_manifest(trino) == 1

    statement = trino.executed[0]
    assert "MAX(p._preview_batch_id)" in statement
    assert "COUNT(*)" in statement
    assert "p._preview_batch_id IS NOT NULL" in statement
    assert "committed.game_id IS NULL" in statement
    assert "'legacy-v1'" in statement
    assert "'legacy', 'unknown', NULL, NULL, NULL, 1" in statement
    assert "CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))" in statement


@pytest.mark.unit
def test_preview_metadata_backfill_is_deterministic_and_idempotent():
    table = "whoscored_missing_players"
    columns = {
        "league": "varchar",
        "season": "varchar",
        "game_id": "bigint",
        "_preview_batch_id": "varchar",
        "_parser_version": "varchar",
    }
    trino = _TableTrino({table}, columns={table: columns})
    trino.execute_query = MagicMock(side_effect=[[(3,)], [(0,)]])

    assert migration.backfill_preview_metadata(trino) == 3

    statement = trino.executed[0]
    assert statement.startswith("UPDATE iceberg.bronze.whoscored_missing_players")
    assert "legacy-preview-" in statement
    assert "CAST(CAST(game_id AS BIGINT) AS VARCHAR)" in statement
    assert "COALESCE(_parser_version, 'legacy-v1')" in statement
    assert "WHERE _preview_batch_id IS NULL OR _parser_version IS NULL" in statement

    already_done = _TableTrino({table}, columns={table: columns})
    already_done.execute_query = MagicMock(return_value=[(0,)])
    assert migration.backfill_preview_metadata(already_done) == 0
    assert already_done.executed == []


@pytest.mark.unit
def test_dry_run_default_suffix_performs_no_ddl(monkeypatch, capsys):
    trino = MagicMock()
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})

    assert migration.main([], trino=trino) == 0

    trino._execute.assert_not_called()
    assert '"mode": "dry-run"' in capsys.readouterr().out


@pytest.mark.unit
def test_apply_failure_after_swap_triggers_automatic_rollback(monkeypatch):
    table = "whoscored_events"
    trino = _TableTrino({table})
    monkeypatch.setattr(migration, "TABLE_KEYS", {table: ("league", "season", "game_id")})
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})
    monkeypatch.setattr(
        migration,
        "build_shadow",
        lambda *args, **kwargs: (f"{table}_v2_run1", 10, 5),
    )
    monkeypatch.setattr(
        migration,
        "swap_shadow",
        lambda *args, **kwargs: f"{table}_legacy_run1",
    )
    rollback = MagicMock(return_value=[table])
    monkeypatch.setattr(migration, "rollback", rollback)

    class _BrokenRepository:
        def __init__(self, **kwargs):
            pass

        def ensure_schema(self, **_kwargs):
            raise RuntimeError("schema failure")

    monkeypatch.setattr(migration, "WhoScoredRepository", _BrokenRepository)

    with pytest.raises(RuntimeError, match="schema failure"):
        migration.main(
            ["--apply", "--confirm-quiescent", "--suffix", "run1"],
            trino=trino,
        )

    rollback.assert_called_once_with(trino, "run1")


@pytest.mark.unit
def test_resumed_completed_swap_still_triggers_automatic_rollback(monkeypatch):
    table = "whoscored_events"
    backup = f"{table}_legacy_run1"
    trino = _TableTrino({table, backup})
    trino.execute_query = MagicMock(return_value=[(5,)])
    monkeypatch.setattr(
        migration, "TABLE_KEYS", {table: ("league", "season", "game_id")}
    )
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})
    rollback = MagicMock(return_value=[table])
    monkeypatch.setattr(migration, "rollback", rollback)

    class _BrokenRepository:
        def __init__(self, **kwargs):
            pass

        def ensure_schema(self, **_kwargs):
            raise RuntimeError("schema failure")

    monkeypatch.setattr(migration, "WhoScoredRepository", _BrokenRepository)

    with pytest.raises(RuntimeError, match="schema failure"):
        migration.main(
            ["--apply", "--confirm-quiescent", "--suffix", "run1"],
            trino=trino,
        )

    rollback.assert_called_once_with(trino, "run1")


@pytest.mark.unit
def test_apply_refuses_suffix_with_prior_rollback_artifacts(monkeypatch):
    failed = "whoscored_events_v2_failed_run1"
    trino = _TableTrino({failed})
    monkeypatch.setattr(
        migration, "TABLE_KEYS", {"whoscored_events": migration.EVENT_KEY}
    )

    with pytest.raises(RuntimeError, match="choose a new --suffix"):
        migration.main(
            ["--apply", "--confirm-quiescent", "--suffix", "run1"],
            trino=trino,
        )

    assert trino.executed == []


@pytest.mark.unit
def test_apply_requires_explicit_quiescence_confirmation(monkeypatch):
    trino = MagicMock()
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})

    with pytest.raises(SystemExit, match="confirm-quiescent"):
        migration.main(["--apply", "--suffix", "run1"], trino=trino)

    trino._execute.assert_not_called()
