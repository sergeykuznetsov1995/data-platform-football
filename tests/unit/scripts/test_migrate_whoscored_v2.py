from __future__ import annotations

import importlib.util
import json
import re
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

    assert sum('AS "source_event_id"' in item for item in projection) == 1
    assert sum('AS "_game_batch_id"' in item for item in projection) == 1


@pytest.mark.unit
def test_event_projection_renames_team_local_relation_without_compatibility_column():
    projection = migration._projection(
        "whoscored_events",
        [
            "league",
            "season",
            "game_id",
            "related_event_id",
            "_ingested_at",
            "_batch_id",
        ],
    )

    assert 'CAST("related_event_id" AS BIGINT) AS "related_team_event_id"' in projection
    assert not any(
        item == '"related_event_id" AS "related_event_id"' for item in projection
    )
    assert any("AS team_event_id" in item for item in projection)


@pytest.mark.unit
def test_event_projection_rejects_ambiguous_old_and_new_relation_columns():
    with pytest.raises(RuntimeError, match="contains both legacy related_event_id"):
        migration._projection(
            "whoscored_events",
            ["related_event_id", "related_team_event_id"],
        )


@pytest.mark.unit
def test_event_migration_key_uses_renamed_relation_on_idempotent_rerun():
    columns = [
        "league",
        "season",
        "game_id",
        "source_event_id",
        "related_team_event_id",
        "_ingested_at",
        "_batch_id",
    ]

    keys = migration._available_keys("whoscored_events", columns, migration.EVENT_KEY)

    assert "related_team_event_id" in keys
    assert "related_event_id" not in keys


@pytest.mark.unit
def test_legacy_match_rows_keep_null_logical_batch_for_current_view_bridge():
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

    assert event_batch == "CAST(NULL AS VARCHAR) AS _game_batch_id"
    assert event_batch == lineup_batch

    existing_v2 = migration._projection(
        "whoscored_events",
        [*base_columns, "_parser_version", "_game_batch_id"],
    )
    normalised = next(item for item in existing_v2 if 'AS "_game_batch_id"' in item)
    assert "\"_game_batch_id\" LIKE 'ws2-%'" in normalised
    assert 'THEN "_game_batch_id"' in normalised
    assert "ELSE CAST(NULL AS VARCHAR)" in normalised


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

    preview_batch = next(item for item in projection if "AS _preview_batch_id" in item)
    assert preview_batch == "CAST(NULL AS VARCHAR) AS _preview_batch_id"
    assert "'legacy-v1' AS _parser_version" in projection
    assert "CAST(NULL AS VARCHAR) AS _payload_sha256" in projection


@pytest.mark.unit
def test_object_contract_covers_all_business_tables_manifests_and_views():
    objects = set(migration.REQUIRED_BRONZE_OBJECTS) | set(
        migration.REQUIRED_SILVER_OBJECTS
    )
    columns = {
        table: set(required)
        for table, required in {
            **migration.BUSINESS_REQUIRED_COLUMNS,
            **migration.MANIFEST_REQUIRED_COLUMNS,
        }.items()
    }
    trino = _TableTrino(objects, columns=columns)

    result = migration.inspect_object_contract(trino)

    assert result["passed"] is True
    assert result["business_table_count"] == 25
    assert result["bronze_view_count"] == 31
    assert not result["missing_commit_columns"]


@pytest.mark.unit
def test_object_contract_fails_closed_on_scope_manifest_drift():
    objects = set(migration.REQUIRED_BRONZE_OBJECTS) | set(
        migration.REQUIRED_SILVER_OBJECTS
    )
    columns = {
        table: set(required)
        for table, required in {
            **migration.BUSINESS_REQUIRED_COLUMNS,
            **migration.MANIFEST_REQUIRED_COLUMNS,
        }.items()
    }
    columns["whoscored_scope_ingest_manifest"].remove("dataset_states_json")

    result = migration.inspect_object_contract(_TableTrino(objects, columns=columns))

    assert result["passed"] is False
    assert result["missing_commit_columns"] == {
        "whoscored_scope_ingest_manifest": ["dataset_states_json"]
    }


@pytest.mark.unit
def test_incomplete_existing_v2_manifest_fails_integrity_preflight():
    table = "whoscored_match_ingest_manifest"
    trino = _TableTrino(
        {table},
        columns={table: {"batch_id", "raw_uri", "state"}},
    )
    trino.execute_query = MagicMock(return_value=[(3,)])

    result = migration.inspect_existing_v2_commits(trino)

    assert result["detected"] is True
    assert result["passed"] is False
    assert result["groups"]["match"]["manifest_rows"] == 3
    assert "entity_counts_json" in " ".join(result["errors"])
    assert "batch_id LIKE 'ws2-%'" in trino.execute_query.call_args.args[0]


@pytest.mark.unit
def test_v2_event_identity_preflight_matches_global_and_team_local_raw_ids():
    table = "whoscored_events"
    trino = _TableTrino(
        {table},
        columns={
            table: {
                "_game_batch_id",
                "game_id",
                "team_id",
                "source_event_id",
                "team_event_id",
                "related_team_event_id",
                "source_raw_json",
            }
        },
    )
    trino.execute_query = MagicMock(
        side_effect=[
            [(2,)],
            [(0, 2_944_339_663)],
            [(0,)],
            [(0,)],
        ]
    )
    result = {"errors": []}

    migration._inspect_match_event_identity(trino, result)

    assert result["errors"] == []
    assert result["event_identity"] == {
        "v2_rows": 2,
        "invalid_or_mismatched_rows": 0,
        "duplicate_source_ids": 0,
        "duplicate_team_event_ids": 0,
        "max_source_event_id": 2_944_339_663,
    }
    identity_sql = trino.execute_query.call_args_list[1].args[0]
    assert "'$.id'" in identity_sql
    assert "'$.eventId'" in identity_sql
    assert "'$.relatedEventId'" in identity_sql


@pytest.mark.unit
def test_v2_event_identity_preflight_fails_on_raw_mismatch_or_duplicate():
    table = "whoscored_events"
    trino = _TableTrino(
        {table},
        columns={
            table: {
                "_game_batch_id",
                "game_id",
                "team_id",
                "source_event_id",
                "team_event_id",
                "related_team_event_id",
                "source_raw_json",
            }
        },
    )
    trino.execute_query = MagicMock(side_effect=[[(2,)], [(1, 10)], [(1,)], [(1,)]])
    result = {"errors": []}

    migration._inspect_match_event_identity(trino, result)

    assert len(result["errors"]) == 3
    assert "do not preserve raw id/eventId" in result["errors"][0]
    assert "duplicate global source_event_id" in result["errors"][1]
    assert "duplicate team-local" in result["errors"][2]


class _PreviewIntegrityTrino(_TableTrino):
    batch_id = "wsp2-valid"
    counts_json = '{"preview_lineups":0,"missing_players":2,"preview_sections":1}'

    def __init__(self, *, missing_player_rows=2):
        tables = {
            "whoscored_preview_ingest_manifest",
            "whoscored_preview_ingest_latest_success",
            *migration.PREVIEW_TABLES,
            *(f"{table}_current" for table in migration.PREVIEW_TABLES),
        }
        columns = {
            "whoscored_preview_ingest_manifest": {
                "batch_id",
                "state",
                "raw_uri",
                "entity_counts_json",
            },
            "whoscored_preview_ingest_latest_success": {
                "batch_id",
                "raw_uri",
                "entity_counts_json",
            },
            **{table: {"_preview_batch_id"} for table in migration.PREVIEW_TABLES},
            **{
                f"{table}_current": {"_preview_batch_id"}
                for table in migration.PREVIEW_TABLES
            },
        }
        super().__init__(tables, columns=columns)
        self.missing_player_rows = missing_player_rows

    def execute_query(self, sql):
        compact = " ".join(sql.split())
        if (
            "SELECT COUNT(*) FROM iceberg.bronze.whoscored_preview_ingest_manifest"
            in compact
        ):
            return [(1,)]
        if "SELECT batch_id, raw_uri, entity_counts_json" in compact:
            return [(self.batch_id, "s3://raw/preview.html.gz", self.counts_json)]
        if "SELECT _preview_batch_id, COUNT(*)" in compact:
            table = re.search(r"FROM iceberg\.bronze\.([A-Za-z0-9_]+)", compact).group(
                1
            )
            base = table.removesuffix("_current")
            counts = {
                "whoscored_preview_lineups": 0,
                "whoscored_missing_players": self.missing_player_rows,
                "whoscored_preview_sections": 1,
            }
            count = counts[base]
            return [] if count == 0 else [(self.batch_id, count)]
        raise AssertionError(f"unexpected SQL: {compact}")


@pytest.mark.unit
def test_valid_existing_preview_commit_matches_physical_and_current_counts():
    result = migration._inspect_json_commit_group(
        _PreviewIntegrityTrino(),
        group="preview",
        manifest_table="whoscored_preview_ingest_manifest",
        latest_view="whoscored_preview_ingest_latest_success",
        tables=migration.PREVIEW_TABLES,
        prefix="wsp2-",
        manifest_batch_column="batch_id",
        physical_batch_column="_preview_batch_id",
    )

    assert result["detected"] is True
    assert result["errors"] == []
    assert result["manifest_batches"] == 1
    metrics = result["datasets"]["whoscored_missing_players"]["physical"]
    assert metrics["expected_rows"] == metrics["actual_rows"] == 2
    assert metrics["mismatch_count"] == 0


@pytest.mark.unit
def test_partial_existing_preview_batch_fails_closed_before_ddl():
    result = migration._inspect_json_commit_group(
        _PreviewIntegrityTrino(missing_player_rows=1),
        group="preview",
        manifest_table="whoscored_preview_ingest_manifest",
        latest_view="whoscored_preview_ingest_latest_success",
        tables=migration.PREVIEW_TABLES,
        prefix="wsp2-",
        manifest_batch_column="batch_id",
        physical_batch_column="_preview_batch_id",
    )

    assert result["errors"]
    assert (
        result["datasets"]["whoscored_missing_players"]["physical"]["mismatch_count"]
        == 1
    )


class _OldShapePreviewIntegrityTrino(_TableTrino):
    batch_ids = ("wsp2-old-a", "wsp2-old-b")

    def __init__(self, *, rows_per_batch=2):
        tables = {
            "whoscored_preview_ingest_manifest",
            "whoscored_preview_ingest_latest_success",
            "whoscored_missing_players",
            "whoscored_missing_players_current",
        }
        columns = {
            "whoscored_preview_ingest_manifest": {
                "batch_id",
                "state",
                "raw_uri",
                "missing_players_count",
            },
            "whoscored_preview_ingest_latest_success": {
                "batch_id",
                "raw_uri",
                "missing_players_count",
            },
            "whoscored_missing_players": {"_preview_batch_id"},
            "whoscored_missing_players_current": {"_preview_batch_id"},
        }
        super().__init__(tables, columns=columns)
        self.rows_per_batch = rows_per_batch

    def execute_query(self, sql):
        compact = " ".join(sql.split())
        if (
            "SELECT COUNT(*) FROM iceberg.bronze.whoscored_preview_ingest_manifest"
            in compact
        ):
            return [(2,)]
        if "SELECT batch_id, raw_uri, CAST(NULL AS VARCHAR)" in compact:
            return [
                (batch_id, f"s3://raw/{batch_id}.html.gz", None, 2)
                for batch_id in self.batch_ids
            ]
        if "SELECT _preview_batch_id, COUNT(*)" in compact:
            return [(batch_id, self.rows_per_batch) for batch_id in self.batch_ids]
        raise AssertionError(f"unexpected SQL: {compact}")


@pytest.mark.unit
def test_two_consistent_old_shape_preview_commits_pass_additive_preflight():
    result = migration._inspect_json_commit_group(
        _OldShapePreviewIntegrityTrino(),
        group="preview",
        manifest_table="whoscored_preview_ingest_manifest",
        latest_view="whoscored_preview_ingest_latest_success",
        tables=migration.PREVIEW_TABLES,
        prefix="wsp2-",
        manifest_batch_column="batch_id",
        physical_batch_column="_preview_batch_id",
        fallback_count_column="missing_players_count",
        fallback_dataset_key="missing_players",
    )

    assert result["errors"] == []
    assert result["manifest_rows"] == result["manifest_batches"] == 2
    missing = result["datasets"]["whoscored_missing_players"]
    assert missing["physical"]["expected_rows"] == 4
    assert missing["physical"]["actual_rows"] == 4
    for table in ("whoscored_preview_lineups", "whoscored_preview_sections"):
        assert result["datasets"][table]["physical"]["actual_rows"] == 0
        assert result["datasets"][table]["current"]["mismatch_count"] == 0


@pytest.mark.unit
def test_old_shape_preview_fallback_still_rejects_partial_physical_batch():
    result = migration._inspect_json_commit_group(
        _OldShapePreviewIntegrityTrino(rows_per_batch=1),
        group="preview",
        manifest_table="whoscored_preview_ingest_manifest",
        latest_view="whoscored_preview_ingest_latest_success",
        tables=migration.PREVIEW_TABLES,
        prefix="wsp2-",
        manifest_batch_column="batch_id",
        physical_batch_column="_preview_batch_id",
        fallback_count_column="missing_players_count",
        fallback_dataset_key="missing_players",
    )

    assert result["errors"]
    assert (
        result["datasets"]["whoscored_missing_players"]["physical"]["mismatch_count"]
        == 2
    )


class _ProfileIntegrityTrino(_TableTrino):
    def __init__(self):
        tables = {
            "whoscored_profile_ingest_manifest",
            *migration.PROFILE_TABLES,
            "whoscored_player_profile_current",
        }
        columns = {
            "whoscored_profile_ingest_manifest": {
                "_profile_batch_id",
                "player_id",
                "payload_sha256",
                "parser_version",
                "raw_uri",
                "participations_count",
                "state",
            },
            **{table: {"_profile_batch_id"} for table in migration.PROFILE_TABLES},
        }
        super().__init__(tables, columns=columns)

    def execute_query(self, sql):
        compact = " ".join(sql.split())
        if (
            "SELECT COUNT(*) FROM iceberg.bronze.whoscored_profile_ingest_manifest"
            in compact
        ):
            return [(1,)]
        if "SELECT _profile_batch_id, player_id" in compact:
            return [
                (
                    "wspr2-valid",
                    7,
                    "a" * 64,
                    "whoscored-v3",
                    "s3://raw/profile.html.gz",
                    2,
                )
            ]
        if "SELECT _profile_batch_id, COUNT(*)" in compact:
            table = re.search(r"FROM iceberg\.bronze\.([A-Za-z0-9_]+)", compact).group(
                1
            )
            count = 1 if table == migration.PROFILE_TABLES[0] else 2
            return [("wspr2-valid", count)]
        if "LEFT JOIN iceberg.silver.whoscored_player_profile_current" in compact:
            return [(0,)]
        raise AssertionError(f"unexpected SQL: {compact}")


@pytest.mark.unit
def test_profile_v2_marker_is_counted_and_validated_end_to_end():
    result = migration._inspect_profile_commits(_ProfileIntegrityTrino())

    assert result["detected"] is True
    assert result["manifest_rows"] == 1
    assert (
        result["datasets"][migration.PROFILE_TABLES[0]]["physical"]["actual_rows"] == 1
    )
    assert (
        result["datasets"][migration.PROFILE_TABLES[1]]["physical"]["actual_rows"] == 2
    )
    assert result["errors"] == []


@pytest.mark.unit
def test_apply_is_non_mutating_when_existing_v2_is_inconsistent(monkeypatch, capsys):
    trino = _TableTrino()
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})
    monkeypatch.setattr(
        migration,
        "inspect_existing_v2_commits",
        lambda manager: {
            "passed": False,
            "detected": True,
            "groups": {},
            "total_markers": 1,
            "errors": ["partial V2 batch"],
        },
    )

    result = migration.main(
        ["--apply", "--confirm-quiescent", "--suffix", "run3"],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert '"status": "blocked"' in capsys.readouterr().out


@pytest.mark.unit
def test_valid_existing_v2_is_allowed_and_checked_again_after_apply(
    monkeypatch, capsys
):
    trino = _TableTrino()
    valid = {
        "passed": True,
        "detected": True,
        "groups": {},
        "total_markers": 3,
        "errors": [],
    }
    inspect = MagicMock(side_effect=[valid, valid])
    monkeypatch.setattr(migration, "TABLE_KEYS", {})
    monkeypatch.setattr(migration, "capture_state", lambda manager: {"tables": {}})
    monkeypatch.setattr(migration, "inspect_existing_v2_commits", inspect)
    monkeypatch.setattr(
        migration,
        "inspect_object_contract",
        lambda manager: {"passed": True, "errors": []},
    )
    monkeypatch.setattr(migration, "seed_profiles", lambda manager: 0)

    class _Repository:
        def __init__(self, **kwargs):
            pass

        def ensure_schema(self, **kwargs):
            pass

    monkeypatch.setattr(migration, "WhoScoredRepository", _Repository)

    assert (
        migration.main(
            ["--apply", "--confirm-quiescent", "--suffix", "run3"], trino=trino
        )
        == 0
    )
    assert inspect.call_count == 2
    assert '"existing_v2_parity"' in capsys.readouterr().out


@pytest.mark.unit
def test_post_shadow_parity_detects_a_changed_physical_batch_fingerprint():
    before = {
        "groups": {
            "preview": {
                "detected": True,
                "manifest_rows": 1,
                "manifest_batches": 1,
                "manifest_fingerprint": "manifest",
                "datasets": {
                    "whoscored_missing_players": {
                        "physical": {
                            "actual_rows": 2,
                            "actual_batches": 1,
                            "actual_fingerprint": "before",
                        }
                    }
                },
            }
        }
    }
    after = json.loads(json.dumps(before))
    after["groups"]["preview"]["datasets"]["whoscored_missing_players"]["physical"][
        "actual_fingerprint"
    ] = "after"

    assert migration.compare_v2_integrity(before, after)["passed"] is False


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
        "team_event_id": "bigint",
        "related_team_event_id": "bigint",
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
            [],
        ]
    )

    result = migration.build_shadow(trino, table, suffix="20260710")

    assert result == (shadow, 10, 5)
    assert trino.executed == []


@pytest.mark.unit
def test_completed_shadow_preserves_every_existing_v2_batch_version():
    table = "whoscored_events"
    shadow = f"{table}_v2_run4"
    columns = {
        "league": "varchar",
        "season": "varchar",
        "game_id": "bigint",
        "source_event_id": "bigint",
        "team_event_id": "bigint",
        "related_team_event_id": "bigint",
        "_payload_sha256": "varchar",
        "_parser_version": "varchar",
        "_game_batch_id": "varchar",
        "_ingested_at": "timestamp(6)",
        "_batch_id": "varchar",
    }
    trino = _TableTrino({table, shadow}, columns={table: columns, shadow: columns})
    trino.execute_query = MagicMock(
        side_effect=[
            [("ws2-a", 2), ("ws2-b", 2)],
            [(6,)],
            [(0,)],
            [("ENG-Premier League", "2025-2026")],
            [(6,)],
            [(6,)],
            [("ENG-Premier League", "2025-2026")],
            [(6,)],
            [(0,)],
            [("ws2-a", 2), ("ws2-b", 2)],
        ]
    )

    assert migration.build_shadow(trino, table, suffix="run4") == (shadow, 6, 6)
    assert trino.executed == []
    expected_count_sql = trino.execute_query.call_args_list[4].args[0]
    assert "CASE WHEN \"_game_batch_id\" LIKE 'ws2-%'" in expected_count_sql


@pytest.mark.unit
def test_shadow_with_lost_v2_batch_is_never_reused():
    table = "whoscored_events"
    shadow = f"{table}_v2_run4"
    columns = {
        "league": "varchar",
        "season": "varchar",
        "game_id": "bigint",
        "source_event_id": "bigint",
        "team_event_id": "bigint",
        "related_team_event_id": "bigint",
        "_payload_sha256": "varchar",
        "_parser_version": "varchar",
        "_game_batch_id": "varchar",
        "_ingested_at": "timestamp(6)",
        "_batch_id": "varchar",
    }
    trino = _TableTrino({table, shadow}, columns={table: columns, shadow: columns})
    trino.execute_query = MagicMock(
        side_effect=[
            [("ws2-a", 2), ("ws2-b", 2)],
            [(6,)],
            [(0,)],
            [("ENG-Premier League", "2025-2026")],
            [(6,)],
            [(6,)],
            [("ENG-Premier League", "2025-2026")],
            [(6,)],
            [(0,)],
            [("ws2-b", 2)],
        ]
    )

    with pytest.raises(RuntimeError, match="does not preserve V2 logical batches"):
        migration.build_shadow(trino, table, suffix="run4")

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
    match_manifest = "whoscored_match_ingest_manifest"
    trino = _TableTrino({match_manifest})

    assert migration.rollback(trino, "20260710") == []
    assert trino.executed == []
    assert match_manifest in trino.tables


@pytest.mark.unit
def test_rollback_restores_legacy_and_isolates_v2_state_once():
    table = "whoscored_events"
    backup = f"{table}_legacy_20260710"
    match_manifest = "whoscored_match_ingest_manifest"
    trino = _TableTrino({table, backup, match_manifest})

    restored = migration.rollback(trino, "20260710")

    assert restored == [table]
    assert table in trino.tables
    assert backup not in trino.tables
    assert f"{table}_v2_failed_20260710" in trino.tables
    assert match_manifest not in trino.tables
    assert f"{match_manifest}_v2_failed_20260710" in trino.tables
    executed_count = len(trino.executed)

    assert migration.rollback(trino, "20260710") == []
    assert len(trino.executed) == executed_count


@pytest.mark.unit
def test_rollback_preserves_preexisting_v2_manifests_payloads_and_views():
    table = "whoscored_events"
    backup = f"{table}_legacy_run5"
    match_manifest = "whoscored_match_ingest_manifest"
    additive = "whoscored_matches"
    current_view = "whoscored_events_current"
    trino = _TableTrino({table, backup, match_manifest, additive, current_view})

    restored = migration.rollback(trino, "run5", preserve_v2_state=True)

    assert restored == [table]
    assert {match_manifest, additive, current_view} <= trino.tables
    assert not any(sql.startswith("DROP VIEW") for sql in trino.executed)
    assert f"{match_manifest}_v2_failed_run5" not in trino.tables


@pytest.mark.unit
def test_rollback_blocks_before_rename_when_active_has_post_cutover_v2_batch():
    table = "whoscored_events"
    backup = f"{table}_legacy_run6"
    trino = _TableTrino(
        {table, backup},
        columns={
            table: {"_game_batch_id"},
            backup: {"_game_batch_id"},
        },
    )
    trino.execute_query = MagicMock(
        side_effect=[
            [("ws2-before", 10), ("ws2-after", 12)],
            [("ws2-before", 10)],
        ]
    )

    with pytest.raises(RuntimeError, match="active V2 batches are newer"):
        migration.rollback(trino, "run6", preserve_v2_state=True)

    assert trino.executed == []
    assert {table, backup} <= trino.tables


@pytest.mark.unit
def test_rollback_drops_full_view_contract_and_isolates_all_v2_state():
    table = "whoscored_events"
    backup = f"{table}_legacy_run3"
    trino = _TableTrino({table, backup, *migration.ROLLBACK_STATE_TABLES})

    assert migration.rollback(trino, "run3") == [table]

    drop_views = [sql for sql in trino.executed if sql.startswith("DROP VIEW")]
    assert len(drop_views) == len(migration.BRONZE_VIEWS) + len(migration.SILVER_VIEWS)
    assert any("whoscored_catalog_latest_success" in sql for sql in drop_views)
    assert any("whoscored_scope_ingest_latest_success" in sql for sql in drop_views)
    assert any("whoscored_team_match_stats_current" in sql for sql in drop_views)
    for state_table in migration.ROLLBACK_STATE_TABLES:
        assert state_table not in trino.tables
        assert f"{state_table}_v2_failed_run3" in trino.tables


@pytest.mark.unit
def test_failed_artifact_inventory_includes_catalog_scope_and_participations():
    expected = {
        "whoscored_catalog_manifest_v2_failed_run3",
        "whoscored_scope_ingest_manifest_v2_failed_run3",
        "whoscored_player_stage_participations_v2_failed_run3",
    }
    trino = _TableTrino(expected)

    assert expected <= set(migration._failed_artifacts(trino, "run3"))


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
    monkeypatch.setattr(
        migration, "TABLE_KEYS", {table: ("league", "season", "game_id")}
    )
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

    rollback.assert_called_once_with(trino, "run1", preserve_v2_state=False)


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

    rollback.assert_called_once_with(trino, "run1", preserve_v2_state=False)


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
