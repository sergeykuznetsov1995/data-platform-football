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
        metrics=None,
        event_mismatch=0,
        lineup_mismatch=0,
        preview_mismatch=0,
        include_deprecated=True,
        backup_metrics=None,
    ):
        self.objects = {
            cleanup.BRONZE: set(cleanup.REQUIRED_BRONZE_OBJECTS) | set(artifacts),
            cleanup.SILVER: set(cleanup.REQUIRED_SILVER_OBJECTS),
        }
        if include_deprecated:
            self.objects[cleanup.BRONZE].add(cleanup.DEPRECATED_ACTIVE_TABLE)
        self.metrics = metrics or (
            10,
            10,
            100,
            100,
            20,
            20,
            0,
            0,
            0,
            6,
            12,
            12,
            8,
            8,
        )
        self.event_mismatch = event_mismatch
        self.lineup_mismatch = lineup_mismatch
        self.preview_mismatch = preview_mismatch
        self.backup_metrics = backup_metrics or {}
        self.executed: list[str] = []
        self.query_count = 0
        self.last_queries: list[str] = []

    def table_exists(self, schema, table):
        return table in self.objects.get(schema, set())

    def execute_query(self, sql):
        compact = " ".join(sql.split())
        self.last_queries.append(compact)
        for backup, metrics in self.backup_metrics.items():
            if backup in compact:
                return [metrics]
        self.query_count += 1
        if self.query_count == 1:
            return [self.metrics]
        if self.query_count == 2:
            return [(self.event_mismatch,)]
        if self.query_count == 3:
            return [(self.lineup_mismatch,)]
        if self.query_count == 4:
            return [(self.preview_mismatch,)]
        raise AssertionError(f"unexpected query: {sql}")

    def _execute(self, sql):
        compact = " ".join(sql.split())
        self.executed.append(compact)
        match = re.fullmatch(r"DROP TABLE iceberg\.bronze\.([A-Za-z0-9_]+)", compact)
        assert match is not None
        self.objects[cleanup.BRONZE].remove(match.group(1))


@pytest.mark.unit
def test_suffix_and_confirmation_are_exactly_allowlisted():
    assert cleanup.confirmation_token("20260710v2") == (
        "drop-whoscored-migration-artifacts:20260710v2"
    )
    assert cleanup.confirmation_token("20260710v2r2") == (
        "drop-whoscored-migration-artifacts:20260710v2r2"
    )
    for unsafe in (
        "20260710",
        "20260710r2",
        "20260710v2_extra",
        "20260710v2;DROP",
    ):
        with pytest.raises(ValueError, match="not allowlisted"):
            cleanup.artifact_names(unsafe)


@pytest.mark.unit
def test_artifact_allowlist_contains_only_exact_migration_names():
    names = cleanup.artifact_names("20260710v2")

    assert len(names) == 22
    assert len(set(names)) == len(names)
    assert "whoscored_events_legacy_20260710v2" in names
    assert "whoscored_events_v2_20260710v2" in names
    assert "whoscored_events_v2_failed_20260710v2" in names
    assert "whoscored_match_ingest_manifest_v2_failed_20260710v2" in names
    assert "whoscored_preview_ingest_manifest_v2_failed_20260710v2" in names
    assert not (set(names) & cleanup.REQUIRED_BRONZE_OBJECTS)


@pytest.mark.unit
def test_default_mode_is_a_non_mutating_dry_run(capsys):
    artifact = "whoscored_events_legacy_20260710v2"
    trino = _Trino(artifacts={artifact})

    assert cleanup.main(["--suffix", "20260710v2"], trino=trino) == 0

    assert trino.executed == []
    output = capsys.readouterr().out
    assert '"mode": "dry-run"' in output
    assert '"status": "ready"' in output
    assert artifact in output


@pytest.mark.unit
def test_apply_requires_the_suffix_bound_confirmation_before_connecting():
    with pytest.raises(SystemExit, match="drop-whoscored-migration-artifacts"):
        cleanup.main(
            ["--suffix", "20260710v2", "--apply", "--confirm", "wrong"],
            trino=MagicMock(),
        )


@pytest.mark.unit
def test_missing_current_view_blocks_cleanup_without_drop(capsys):
    artifact = "whoscored_events_legacy_20260710v2"
    trino = _Trino(artifacts={artifact})
    trino.objects[cleanup.BRONZE].remove("whoscored_events_current")

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2"),
        ],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert '"status": "blocked"' in capsys.readouterr().out


@pytest.mark.unit
@pytest.mark.parametrize(
    "missing_object",
    [
        "whoscored_preview_ingest_manifest",
        "whoscored_preview_ingest_latest",
        "whoscored_preview_ingest_latest_success",
        "whoscored_missing_players_current",
    ],
)
def test_strict_preview_objects_are_required(missing_object, capsys):
    trino = _Trino()
    trino.objects[cleanup.BRONZE].remove(missing_object)

    assert cleanup.main(["--suffix", "20260710v2"], trino=trino) == 2

    assert trino.executed == []
    assert missing_object in capsys.readouterr().out


@pytest.mark.unit
def test_manifest_current_count_mismatch_blocks_cleanup(capsys):
    artifact = "whoscored_events_legacy_20260710v2"
    trino = _Trino(
        artifacts={artifact},
        metrics=(10, 10, 101, 100, 20, 20, 0, 0, 0, 6, 12, 12, 8, 8),
    )

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2"),
        ],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert "manifest/current event row counts differ" in capsys.readouterr().out


@pytest.mark.unit
@pytest.mark.parametrize(
    ("metric_index", "expected_error"),
    [
        (6, "physical events still contain rows without a V2 batch id"),
        (7, "physical lineups still contain rows without a V2 batch id"),
        (8, "physical previews still contain rows without a preview batch id"),
    ],
)
def test_physical_null_batch_rows_block_cleanup(metric_index, expected_error, capsys):
    metrics = [10, 10, 100, 100, 20, 20, 0, 0, 0, 6, 12, 12, 8, 8]
    metrics[metric_index] = 1
    trino = _Trino(metrics=tuple(metrics))

    result = cleanup.main(["--suffix", "20260710v2"], trino=trino)

    assert result == 2
    assert trino.executed == []
    assert expected_error in capsys.readouterr().out


@pytest.mark.unit
def test_null_batch_guard_scans_physical_tables_not_current_views(capsys):
    trino = _Trino()

    assert cleanup.main(["--suffix", "20260710v2"], trino=trino) == 0

    count_sql = trino.last_queries[0]
    assert (
        "FROM iceberg.bronze.whoscored_events WHERE _game_batch_id IS NULL" in count_sql
    )
    assert (
        "FROM iceberg.bronze.whoscored_lineups WHERE _game_batch_id IS NULL"
        in count_sql
    )
    assert (
        "FROM iceberg.bronze.whoscored_missing_players WHERE _preview_batch_id IS NULL"
    ) in count_sql
    assert (
        "FROM iceberg.bronze.whoscored_events_current WHERE _game_batch_id IS NULL"
    ) not in count_sql
    capsys.readouterr()


@pytest.mark.unit
def test_preview_manifest_sum_must_match_current_rows(capsys):
    trino = _Trino(metrics=(10, 10, 100, 100, 20, 20, 0, 0, 0, 6, 13, 12, 8, 8))

    assert cleanup.main(["--suffix", "20260710v2"], trino=trino) == 2

    assert trino.executed == []
    assert "manifest/current preview row counts differ" in capsys.readouterr().out


@pytest.mark.unit
def test_per_game_preview_guard_includes_zero_row_snapshots(capsys):
    trino = _Trino(preview_mismatch=1)

    assert cleanup.main(["--suffix", "20260710v2"], trino=trino) == 2

    preview_sql = trino.last_queries[3]
    assert "LEFT JOIN" in preview_sql
    assert "COALESCE(p.row_count, 0) <> m.missing_players_count" in preview_sql
    assert "per-game preview counts differ from the manifest" in capsys.readouterr().out


@pytest.mark.unit
def test_apply_drops_only_discovered_exact_names(capsys):
    artifacts = {
        "whoscored_events_legacy_20260710v2",
        "whoscored_lineups_v2_failed_20260710v2",
    }
    trino = _Trino(artifacts=artifacts)

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert set(trino.executed) == {
        f"DROP TABLE iceberg.bronze.{table}" for table in artifacts
    }
    assert not (artifacts & trino.objects[cleanup.BRONZE])
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]
    assert '"status": "success"' in capsys.readouterr().out


@pytest.mark.unit
def test_drop_api_rejects_active_or_unknown_table_names():
    trino = _Trino()

    with pytest.raises(ValueError, match="non-artifact"):
        cleanup.drop_artifacts(
            trino,
            "20260710v2",
            ["whoscored_events"],
        )

    assert trino.executed == []


@pytest.mark.unit
def test_qualified_names_are_bound_to_the_exact_schema():
    assert (
        cleanup._qualified(cleanup.SILVER, "whoscored_player_profile_current")
        == "iceberg.silver.whoscored_player_profile_current"
    )

    with pytest.raises(ValueError, match="bronze.whoscored_player_profile_current"):
        cleanup._qualified(
            cleanup.BRONZE,
            "whoscored_player_profile_current",
        )


@pytest.mark.unit
def test_final_r2_apply_drops_deprecated_active_profile_after_artifacts(capsys):
    artifact = "whoscored_player_profile_legacy_20260710v2r2"
    trino = _Trino(artifacts={artifact})

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == [
        f"DROP TABLE iceberg.bronze.{artifact}",
        "DROP TABLE iceberg.bronze.whoscored_player_profile",
    ]
    output = capsys.readouterr().out
    assert '"dropped_deprecated_active": [' in output
    assert '"whoscored_player_profile"' in output


@pytest.mark.unit
def test_final_r2_active_profile_cleanup_is_idempotent(capsys):
    trino = _Trino(include_deprecated=False)

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == []
    assert '"dropped_deprecated_active": []' in capsys.readouterr().out


@pytest.mark.unit
def test_blocked_final_r2_never_drops_deprecated_active_profile(capsys):
    trino = _Trino(preview_mismatch=1)

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]
    assert '"status": "blocked"' in capsys.readouterr().out


@pytest.mark.unit
def test_first_suffix_cannot_drop_deprecated_active_via_direct_api():
    trino = _Trino()

    with pytest.raises(ValueError, match="non-deprecated active"):
        cleanup.drop_deprecated_active(
            trino,
            "20260710v2",
            [cleanup.DEPRECATED_ACTIVE_TABLE],
        )

    assert trino.executed == []


@pytest.mark.unit
def test_deprecated_active_direct_api_cannot_bypass_r2_backup_pair():
    backups = set(cleanup.AUTHORITATIVE_R2_BACKUPS)
    trino = _Trino(artifacts=backups)

    with pytest.raises(RuntimeError, match="authoritative backup guard failed"):
        cleanup.drop_deprecated_active(
            trino,
            "20260710v2r2",
            [cleanup.DEPRECATED_ACTIVE_TABLE],
        )

    assert trino.executed == []
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]


@pytest.mark.unit
def test_r2_authoritative_backups_require_backup_subset_of_active(capsys):
    schedule_backup = "whoscored_schedule_legacy_20260710v2r2"
    stages_backup = "whoscored_season_stages_legacy_20260710v2r2"
    trino = _Trino(
        artifacts={schedule_backup, stages_backup},
        backup_metrics={
            schedule_backup: (0, 0, 0, 0),
            stages_backup: (0, 0, 0, 0),
        },
    )

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == [
        f"DROP TABLE iceberg.bronze.{schedule_backup}",
        f"DROP TABLE iceberg.bronze.{stages_backup}",
        "DROP TABLE iceberg.bronze.whoscored_player_profile",
    ]
    schedule_sql = next(sql for sql in trino.last_queries if schedule_backup in sql)
    stages_sql = next(sql for sql in trino.last_queries if stages_backup in sql)
    assert schedule_sql.count(" EXCEPT ") == 2
    assert "TRY_CAST(game_id AS BIGINT) AS game_id" in schedule_sql
    assert stages_sql.count(" EXCEPT ") == 2
    assert "TRY_CAST(stage_id AS BIGINT) AS stage_id" in stages_sql
    assert '"status": "success"' in capsys.readouterr().out


@pytest.mark.unit
def test_r2_authoritative_backups_allow_normal_active_growth(capsys):
    schedule_backup = "whoscored_schedule_legacy_20260710v2r2"
    stages_backup = "whoscored_season_stages_legacy_20260710v2r2"
    trino = _Trino(
        artifacts={schedule_backup, stages_backup},
        backup_metrics={
            schedule_backup: (0, 0, 1, 0),
            stages_backup: (0, 0, 0, 0),
        },
    )

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == [
        f"DROP TABLE iceberg.bronze.{schedule_backup}",
        f"DROP TABLE iceberg.bronze.{stages_backup}",
        "DROP TABLE iceberg.bronze.whoscored_player_profile",
    ]
    assert '"active_only_keys": 1' in capsys.readouterr().out


@pytest.mark.unit
@pytest.mark.parametrize(
    "remaining_backup",
    [
        "whoscored_schedule_legacy_20260710v2r2",
        "whoscored_season_stages_legacy_20260710v2r2",
    ],
)
def test_partial_r2_authoritative_backup_set_blocks_every_drop(
    remaining_backup, capsys
):
    trino = _Trino(
        artifacts={remaining_backup},
        backup_metrics={remaining_backup: (0, 0, 0, 0)},
    )

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert remaining_backup in trino.objects[cleanup.BRONZE]
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]
    assert not any(remaining_backup in sql for sql in trino.last_queries)
    output = capsys.readouterr().out
    assert '"status": "blocked"' in output
    assert "authoritative r2 backups are only partially present" in output


@pytest.mark.unit
@pytest.mark.parametrize(
    ("backup", "metrics", "expected_error"),
    [
        (
            "whoscored_schedule_legacy_20260710v2r2",
            (1, 0, 0, 0),
            "null/invalid keys",
        ),
        (
            "whoscored_schedule_legacy_20260710v2r2",
            (0, 1, 0, 0),
            "null/invalid keys",
        ),
        (
            "whoscored_season_stages_legacy_20260710v2r2",
            (0, 0, 0, 2),
            "backup contains keys missing from active",
        ),
    ],
)
def test_r2_authoritative_backup_key_gap_blocks_every_drop(
    backup, metrics, expected_error, capsys
):
    backups = set(cleanup.AUTHORITATIVE_R2_BACKUPS)
    safe_metrics = {name: (0, 0, 0, 0) for name in backups}
    safe_metrics[backup] = metrics
    trino = _Trino(artifacts=backups, backup_metrics=safe_metrics)

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2r2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2r2"),
        ],
        trino=trino,
    )

    assert result == 2
    assert trino.executed == []
    assert backup in trino.objects[cleanup.BRONZE]
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]
    assert expected_error in capsys.readouterr().out


@pytest.mark.unit
def test_drop_api_rechecks_r2_authoritative_backup_immediately_before_ddl():
    backup = "whoscored_schedule_legacy_20260710v2r2"
    other_backup = "whoscored_season_stages_legacy_20260710v2r2"
    trino = _Trino(
        artifacts={backup, other_backup},
        backup_metrics={
            backup: (0, 0, 0, 1),
            other_backup: (0, 0, 0, 0),
        },
    )

    with pytest.raises(RuntimeError, match="authoritative backup guard failed"):
        cleanup.drop_artifacts(trino, "20260710v2r2", [backup, other_backup])

    assert trino.executed == []
    assert backup in trino.objects[cleanup.BRONZE]


@pytest.mark.unit
def test_drop_api_rejects_authoritative_backup_subset_before_key_queries():
    backups = set(cleanup.AUTHORITATIVE_R2_BACKUPS)
    requested = "whoscored_schedule_legacy_20260710v2r2"
    trino = _Trino(
        artifacts=backups,
        backup_metrics={name: (0, 0, 0, 0) for name in backups},
    )

    with pytest.raises(RuntimeError, match="must be requested together"):
        cleanup.drop_artifacts(trino, "20260710v2r2", [requested])

    assert trino.executed == []
    assert not any(requested in sql for sql in trino.last_queries)


@pytest.mark.unit
def test_idempotent_r2_without_artifacts_skips_authoritative_key_queries(capsys):
    trino = _Trino(include_deprecated=False)

    assert cleanup.main(["--suffix", "20260710v2r2"], trino=trino) == 0

    assert len(trino.last_queries) == 4
    assert not any("_legacy_20260710v2r2" in sql for sql in trino.last_queries)
    assert '"checked": {}' in capsys.readouterr().out


@pytest.mark.unit
def test_v2_cleanup_never_deletes_authoritative_r2_backup(capsys):
    v2_artifact = "whoscored_events_v2_failed_20260710v2"
    r2_backup = "whoscored_schedule_legacy_20260710v2r2"
    trino = _Trino(artifacts={v2_artifact, r2_backup})

    result = cleanup.main(
        [
            "--suffix",
            "20260710v2",
            "--apply",
            "--confirm",
            cleanup.confirmation_token("20260710v2"),
        ],
        trino=trino,
    )

    assert result == 0
    assert trino.executed == [f"DROP TABLE iceberg.bronze.{v2_artifact}"]
    assert r2_backup in trino.objects[cleanup.BRONZE]
    assert cleanup.DEPRECATED_ACTIVE_TABLE in trino.objects[cleanup.BRONZE]
    assert '"status": "success"' in capsys.readouterr().out
