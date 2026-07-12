from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "cleanup_whoscored_v2_migration.py"
SPEC = importlib.util.spec_from_file_location(
    "cleanup_whoscored_v2_migration", SCRIPT_PATH
)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cleanup
SPEC.loader.exec_module(cleanup)


class _Trino:
    def __init__(
        self,
        *,
        artifacts=(),
        deprecated=(),
        missing=(),
        missing_manifest_columns=None,
        visible_legacy_table=None,
        manifest_mismatch_table=None,
        catalog_quarantined=0,
        backup_metrics=None,
        backup_shortfalls=None,
    ):
        self.objects = {
            cleanup.BRONZE: set(cleanup.REQUIRED_BRONZE_OBJECTS) | set(artifacts),
            cleanup.SILVER: set(cleanup.REQUIRED_SILVER_OBJECTS),
        }
        self.objects[cleanup.BRONZE].update(deprecated)
        for qualified in missing:
            schema, table = qualified.split(".", 1)
            self.objects[schema].discard(table)
        self.columns = {
            table: set(required)
            for table, required in {
                **cleanup.BUSINESS_REQUIRED_COLUMNS,
                **cleanup.MANIFEST_REQUIRED_COLUMNS,
            }.items()
        }
        for table, columns in (missing_manifest_columns or {}).items():
            self.columns[table] -= set(columns)
        self.visible_legacy_table = visible_legacy_table
        self.manifest_mismatch_table = manifest_mismatch_table
        self.catalog_quarantined = catalog_quarantined
        self.backup_metrics = backup_metrics or {}
        self.backup_shortfalls = backup_shortfalls or {}
        self.executed: list[str] = []
        self.queries: list[str] = []

    def table_exists(self, schema, table):
        return table in self.objects.get(schema, set())

    def get_table_columns(self, schema, table):
        return self.columns.get(table, set())

    def execute_query(self, sql):
        compact = " ".join(sql.split())
        self.queries.append(compact)
        if "iceberg.information_schema.tables" in compact:
            return [
                (table,)
                for table in sorted(self.objects[cleanup.BRONZE])
                if "_legacy_" in table and cleanup._LEGACY_BACKUP_RE.fullmatch(table)
            ]
        for backup, metrics in self.backup_metrics.items():
            if f"iceberg.bronze.{backup}" in compact:
                if "COALESCE(a.row_count, 0) < b.row_count" in compact:
                    return [(self.backup_shortfalls.get(backup, 0),)]
                return [metrics]
        if "m.competitions_count" in compact:
            return [(10, 10, 20, 20, 30, 30, self.catalog_quarantined)]
        if "SUM(latest.participations_count)" in compact:
            return [(40, 40, 0)]
        if "SELECT (SELECT COUNT(*) FROM" in compact:
            return [(10, 10, 100, 100, 20, 20, 0, 0, 0, 6, 12, 12, 8, 8)]
        if "COALESCE(e.row_count, 0) <> m.events_count" in compact:
            return [(0,)]
        if "COALESCE(l.row_count, 0) <> m.lineups_count" in compact:
            return [(0,)]
        if "COALESCE(p.row_count, 0) <> m.missing_players_count" in compact:
            return [(0,)]
        if "json_extract_scalar" in compact:
            mismatch = int(
                self.manifest_mismatch_table is not None
                and f"iceberg.bronze.{self.manifest_mismatch_table}_current" in compact
            )
            return [(mismatch,)]
        if compact.startswith("SELECT COUNT(*) FROM"):
            legacy = int(
                self.visible_legacy_table is not None
                and f"iceberg.bronze.{self.visible_legacy_table}_current" in compact
            )
            return [(legacy,)]
        raise AssertionError(f"unexpected query: {compact}")

    def _execute(self, sql):
        compact = " ".join(sql.split())
        self.executed.append(compact)
        match = re.fullmatch(r"DROP TABLE iceberg\.bronze\.([A-Za-z0-9_]+)", compact)
        assert match is not None
        self.objects[cleanup.BRONZE].remove(match.group(1))


@pytest.mark.unit
def test_suffix_is_dynamic_but_strictly_identifier_safe():
    assert cleanup.confirmation_token("20260711run3") == (
        "drop-whoscored-migration-artifacts:20260711run3"
    )
    for unsafe in ("", "run-3", "run/3", "run3;DROP", "run3 space"):
        with pytest.raises(ValueError, match="unsafe cleanup suffix"):
            cleanup.artifact_names(unsafe)


@pytest.mark.unit
def test_artifact_names_cover_legacy_and_every_rollback_state_table():
    names = cleanup.artifact_names("run3")

    assert len(names) == 3 * len(cleanup.MIGRATED_TABLES) + len(cleanup.V2_STATE_TABLES)
    assert len(names) == len(set(names))
    assert "whoscored_events_legacy_run3" in names
    assert "whoscored_catalog_manifest_v2_failed_run3" in names
    assert "whoscored_scope_ingest_manifest_v2_failed_run3" in names
    assert "whoscored_player_stage_participations_v2_failed_run3" in names
    assert not (set(names) & cleanup.REQUIRED_BRONZE_OBJECTS)


@pytest.mark.unit
def test_complete_25_table_object_and_data_contract_passes():
    trino = _Trino()

    result = cleanup.inspect_current_state(trino)

    assert result["passed"] is True
    assert result["business_table_count"] == 25
    assert len(result["datasets"]) == 25
    assert not result["errors"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "missing",
    [
        "bronze.whoscored_catalog_manifest",
        "bronze.whoscored_scope_ingest_latest_success",
        "bronze.whoscored_team_match_stats_current",
        "bronze.whoscored_preview_sections_current",
        "silver.whoscored_player_profile_current",
    ],
)
def test_any_missing_contract_object_blocks_cleanup(missing):
    result = cleanup.inspect_current_state(_Trino(missing={missing}))

    assert result["passed"] is False
    assert missing in result["missing_objects"]


@pytest.mark.unit
def test_missing_manifest_provenance_column_blocks_cleanup():
    trino = _Trino(
        missing_manifest_columns={
            "whoscored_scope_ingest_manifest": {"dataset_states_json"}
        }
    )

    result = cleanup.inspect_current_state(trino)

    assert result["passed"] is False
    assert result["missing_commit_columns"] == {
        "whoscored_scope_ingest_manifest": ["dataset_states_json"]
    }


@pytest.mark.unit
def test_visible_scope_legacy_bridge_blocks_cutover():
    result = cleanup.inspect_current_state(
        _Trino(visible_legacy_table="whoscored_schedule")
    )

    assert result["passed"] is False
    assert (
        "whoscored_schedule_current still exposes legacy fallback rows"
        in result["errors"]
    )


@pytest.mark.unit
def test_any_of_the_19_dataset_manifest_mismatches_blocks_cutover():
    result = cleanup.inspect_current_state(
        _Trino(manifest_mismatch_table="whoscored_team_match_stats")
    )

    assert result["passed"] is False
    assert (
        "whoscored_team_match_stats_current counts differ from its manifest"
        in result["errors"]
    )


@pytest.mark.unit
def test_catalog_quarantine_blocks_cutover():
    result = cleanup.inspect_current_state(_Trino(catalog_quarantined=2))

    assert result["passed"] is False
    assert "catalog contains quarantined discoveries" in result["errors"]


@pytest.mark.unit
def test_default_mode_is_non_mutating_dry_run(capsys):
    trino = _Trino()

    assert cleanup.main(["--suffix", "run3"], trino=trino) == 0

    assert trino.executed == []
    output = capsys.readouterr().out
    assert '"mode": "dry-run"' in output
    assert '"status": "ready"' in output


@pytest.mark.unit
def test_apply_requires_suffix_bound_confirmation_before_connecting():
    with pytest.raises(SystemExit, match="drop-whoscored-migration-artifacts:run3"):
        cleanup.main(
            ["--suffix", "run3", "--apply", "--confirm", "wrong"],
            trino=MagicMock(),
        )


@pytest.mark.unit
def test_apply_drops_only_exact_artifacts_then_deprecated_active_tables(capsys):
    artifact = "whoscored_events_v2_failed_run3"
    trino = _Trino(
        artifacts={artifact},
        deprecated=set(cleanup.DEPRECATED_ACTIVE_TABLES),
    )

    result = cleanup.main(
        [
            "--suffix",
            "run3",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("run3"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == [
        f"DROP TABLE iceberg.bronze.{artifact}",
        "DROP TABLE iceberg.bronze.whoscored_season_stages",
        "DROP TABLE iceberg.bronze.whoscored_player_profile",
        "DROP TABLE iceberg.bronze.whoscored_player_assist_pairs",
    ]
    assert '"status": "success"' in capsys.readouterr().out


@pytest.mark.unit
def test_authoritative_backup_requires_key_subset_of_manifest_current():
    backup = "whoscored_schedule_legacy_run3"
    trino = _Trino(artifacts={backup}, backup_metrics={backup: (0, 0, 2, 1)})

    result = cleanup.inspect_authoritative_backup_keys(trino, "run3", [backup])

    assert result["passed"] is False
    assert "backup contains keys missing from active" in result["errors"][0]
    query = next(query for query in trino.queries if backup in query)
    assert "iceberg.bronze.whoscored_schedule_current" in query
    assert "EXCEPT" in query


@pytest.mark.unit
def test_event_backup_row_count_shortfall_blocks_cleanup():
    backup = "whoscored_events_legacy_run3"
    trino = _Trino(
        artifacts={backup},
        backup_metrics={backup: (0, 0, 0, 0)},
        backup_shortfalls={backup: 2},
    )

    result = cleanup.inspect_authoritative_backup_keys(trino, "run3", [backup])

    assert result["passed"] is False
    assert result["checked"][backup]["row_count_shortfalls"] == 2
    assert "current rows are fewer than legacy rows" in result["errors"][0]


@pytest.mark.unit
def test_authoritative_backup_active_growth_is_allowed_and_dropped():
    backup = "whoscored_schedule_legacy_run3"
    trino = _Trino(artifacts={backup}, backup_metrics={backup: (0, 0, 2, 0)})

    assert cleanup.drop_artifacts(trino, "run3", [backup]) == [backup]
    assert backup not in trino.objects[cleanup.BRONZE]


@pytest.mark.unit
def test_direct_drop_api_rejects_partial_present_backup_set():
    first = "whoscored_events_legacy_run3"
    second = "whoscored_schedule_legacy_run3"
    trino = _Trino(
        artifacts={first, second},
        backup_metrics={first: (0, 0, 0, 0), second: (0, 0, 0, 0)},
    )

    with pytest.raises(RuntimeError, match="all present authoritative backups"):
        cleanup.drop_artifacts(trino, "run3", [first])

    assert trino.executed == []


@pytest.mark.unit
def test_drop_api_rejects_active_unknown_or_duplicate_names():
    trino = _Trino()

    with pytest.raises(ValueError, match="non-artifact"):
        cleanup.drop_artifacts(trino, "run3", ["whoscored_events"])
    with pytest.raises(ValueError, match="duplicate"):
        cleanup.drop_artifacts(
            trino,
            "run3",
            ["whoscored_events_v2_run3", "whoscored_events_v2_run3"],
        )
    assert trino.executed == []


@pytest.mark.unit
def test_deprecated_active_drop_cannot_bypass_remaining_backup():
    backup = "whoscored_player_profile_legacy_run3"
    trino = _Trino(artifacts={backup}, deprecated={"whoscored_player_profile"})

    with pytest.raises(RuntimeError, match="backups must be removed"):
        cleanup.drop_deprecated_active(trino, "run3", ["whoscored_player_profile"])
    assert trino.executed == []


@pytest.mark.unit
def test_wrong_suffix_cannot_bypass_backup_bound_to_another_run(capsys):
    backup = "whoscored_player_profile_legacy_run3"
    trino = _Trino(artifacts={backup}, deprecated={"whoscored_player_profile"})

    result = cleanup.main(["--suffix", "run4"], trino=trino)

    assert result == 2
    assert trino.executed == []
    assert "another migration suffix" in capsys.readouterr().out


@pytest.mark.unit
def test_qualified_names_are_schema_and_suffix_bound():
    assert (
        cleanup._qualified(cleanup.SILVER, "whoscored_player_profile_current")
        == "iceberg.silver.whoscored_player_profile_current"
    )
    assert (
        cleanup._qualified(
            cleanup.BRONZE,
            "whoscored_events_legacy_run3",
            artifact_suffix="run3",
        )
        == "iceberg.bronze.whoscored_events_legacy_run3"
    )

    with pytest.raises(ValueError, match="outside"):
        cleanup._qualified(
            cleanup.BRONZE,
            "whoscored_events_legacy_other",
            artifact_suffix="run3",
        )
