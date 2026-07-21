from datetime import datetime, timedelta, timezone
import subprocess

import pytest

from scripts import fotmob_cleanup as mod
from scrapers.fotmob.repository import DEDUP_KEYS


NOW = datetime(2026, 7, 21, 12, tzinfo=timezone.utc)


def test_inventory_key_tracks_repository_dedup_contract():
    assert mod.INVENTORY_KEYS == DEDUP_KEYS["fotmob_field_inventory"]


class PlanClient:
    def __init__(self):
        self.sql = []

    def query(self, sql):
        self.sql.append(sql)
        if "cleanup:list-staging" in sql:
            return [
                ("fotmob_matches__stg_0123456789ab",),
                ("fotmob_matches__stg_../../unsafe",),
            ]
        if "cleanup:count:fotmob_matches__stg_0123456789ab" in sql:
            return [(7,)]
        if "cleanup:snapshot:fotmob_matches__stg_0123456789ab" in sql:
            return [("staging-snapshot-1", NOW - timedelta(hours=48))]
        if "cleanup:columns:fotmob_field_inventory" in sql:
            return [
                (name,)
                for name in (
                    *mod.INVENTORY_KEYS,
                    "_ingested_at",
                    "_target_batch_id",
                )
            ]
        if "cleanup:inventory-shape" in sql:
            return [(100, 75)]
        if "cleanup:count:fotmob_field_inventory" in sql:
            return [(100,)]
        if "cleanup:snapshot:fotmob_field_inventory" in sql:
            return [("inventory-snapshot-1", NOW - timedelta(hours=2))]
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        pass


def test_cleanup_plan_has_only_explicit_old_owned_targets():
    plan = mod.build_plan(
        PlanClient(),
        catalog="iceberg",
        schema="bronze",
        older_than_hours=24,
        clock=lambda: NOW,
    )
    assert [item["table"] for item in plan["staging_targets"]] == [
        "fotmob_matches__stg_0123456789ab"
    ]
    assert plan["rejected_candidates"] == [
        {
            "table": "fotmob_matches__stg_../../unsafe",
            "reason": "name_not_owned_by_fotmob_writer",
        }
    ]
    compact = plan["inventory_compaction"]
    assert compact["duplicate_rows"] == 25
    assert compact["action"] == "shadow_swap"
    assert plan["dry_run"] is True


def _plan(*, row_count=7):
    return {
        "schema_version": "fotmob-cleanup-plan-v1",
        "generated_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "catalog": "iceberg",
        "schema": "bronze",
        "dry_run": True,
        "staging_targets": [
            {
                "table": "fotmob_matches__stg_0123456789ab",
                "qualified_table": (
                    "iceberg.bronze.fotmob_matches__stg_0123456789ab"
                ),
                "row_count": row_count,
                "snapshot_id": "staging-snapshot-1",
                "last_snapshot_at": (NOW - timedelta(hours=48)).isoformat(),
                "action": "drop_table",
            }
        ],
        "inventory_compaction": {
            "action": "none",
            "source_table": mod.INVENTORY,
            "source_rows": 10,
            "distinct_rows": 10,
            "duplicate_rows": 0,
            "snapshot_id": "inventory-snapshot-1",
            "last_snapshot_at": (NOW - timedelta(hours=2)).isoformat(),
            "columns": [
                *mod.INVENTORY_KEYS,
                "_ingested_at",
                "_target_batch_id",
            ],
            "natural_key": list(mod.INVENTORY_KEYS),
            "order_columns": ["_ingested_at", "_target_batch_id"],
        },
    }


class ExecuteClient:
    def __init__(self, count):
        self.count = count
        self.sql = []

    def query(self, sql):
        self.sql.append(sql)
        if "cleanup:table-exists:" in sql:
            return [(1,)]
        if "cleanup:inventory-shape" in sql:
            return [(10, 10)]
        if "cleanup:count:fotmob_field_inventory" in sql:
            return [(10,)]
        if "cleanup:count:" in sql:
            return [(self.count,)]
        if "cleanup:snapshot:" in sql:
            if "fotmob_field_inventory" in sql:
                return [("inventory-snapshot-1", NOW - timedelta(hours=2))]
            return [("staging-snapshot-1", NOW - timedelta(hours=48))]
        if sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        pass


def test_execute_rechecks_metadata_before_any_drop():
    client = ExecuteClient(count=8)
    with pytest.raises(mod.CleanupError, match="metadata changed"):
        mod.execute_plan(client, _plan(), clock=lambda: NOW)
    assert not any(sql.startswith("DROP TABLE") for sql in client.sql)


def test_all_targets_are_preflighted_before_first_drop():
    plan = _plan()
    plan["staging_targets"].append(
        {
            "table": "fotmob_standings__stg_abcdef012345",
            "qualified_table": (
                "iceberg.bronze.fotmob_standings__stg_abcdef012345"
            ),
            "row_count": 7,
            "snapshot_id": "staging-snapshot-1",
            "last_snapshot_at": (NOW - timedelta(hours=48)).isoformat(),
            "action": "drop_table",
        }
    )

    class DriftOnSecond(ExecuteClient):
        def query(self, sql):
            if "cleanup:count:fotmob_standings" in sql:
                self.sql.append(sql)
                return [(999,)]
            return super().query(sql)

    client = DriftOnSecond(count=7)
    with pytest.raises(mod.CleanupError, match="metadata changed"):
        mod.execute_plan(client, plan, clock=lambda: NOW)
    assert not any(sql.startswith("DROP TABLE") for sql in client.sql)


def test_cleanup_rechecks_live_writer_state_and_stops_scheduler(tmp_path, monkeypatch):
    from scripts import fotmob_rollback as runtime

    arguments = type(
        "Args",
        (),
        {
            "env_file": tmp_path / "fotmob.env",
            "deployment_report": tmp_path / "deployment.json",
            "release_sha": "a" * 40,
            "project": "fotmob-airflow",
            "compose_file": tmp_path / "compose.yaml",
        },
    )()
    state = {
        "pause_states": {dag_id: True for dag_id in runtime.DAGS},
        "active_runs": {},
    }
    monkeypatch.setattr(
        runtime, "_deployment_context", lambda _args: {"git_sha": "a" * 40}
    )
    monkeypatch.setattr(runtime, "inspect_writer_state", lambda *_a, **_k: state)
    monkeypatch.setattr(runtime, "require_writers_stopped", lambda _state: None)
    monkeypatch.setattr(
        runtime, "_container_deploy_sha", lambda *_a, **_k: "a" * 40
    )
    monkeypatch.setattr(runtime, "_compose_environment", lambda _args: {})
    monkeypatch.setattr(runtime, "_compose_base", lambda _args: ("compose",))
    live_states = iter(
        [
            {"scheduler_running": True, "mounts_verified": True},
            {"scheduler_running": False, "mounts_verified": True},
        ]
    )
    monkeypatch.setattr(
        runtime,
        "validate_live_deployment",
        lambda *_args, **_kwargs: next(live_states),
    )
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    evidence = mod._quiesce_isolated_scheduler(arguments, run=run)
    assert evidence["scheduler_stopped"] is True
    assert calls == [("compose", "stop", "airflow-scheduler")]


@pytest.mark.parametrize(
    "initial_names",
    [
        {
            "fotmob_field_inventory__compact_0123456789ab",
            "fotmob_field_inventory__backup_0123456789ab",
        },
        {
            mod.INVENTORY,
            "fotmob_field_inventory__backup_0123456789ab",
        },
    ],
    ids=("lost-first-rename-response", "lost-second-rename-response"),
)
def test_inventory_swap_reconciles_ambiguous_committed_rename(initial_names):
    shadow = "fotmob_field_inventory__compact_0123456789ab"
    backup = "fotmob_field_inventory__backup_0123456789ab"
    columns = [*mod.INVENTORY_KEYS, "_ingested_at", "_target_batch_id"]
    spec = {
        "source_table": mod.INVENTORY,
        "source_rows": 6,
        "distinct_rows": 5,
        "snapshot_id": "original-snapshot",
        "last_snapshot_at": (NOW - timedelta(hours=2)).isoformat(),
        "columns": columns,
    }

    class RecoveryClient:
        def __init__(self):
            self.identities = {
                name: ("original" if name == backup else "compact")
                for name in initial_names
            }

        @property
        def names(self):
            return set(self.identities)

        def query(self, sql):
            if "cleanup:swap-name-state" in sql:
                return [(name,) for name in sorted(self.names)]
            if "cleanup:columns:" in sql:
                return [(column,) for column in columns]
            if "cleanup:count:" in sql:
                table = sql.split("cleanup:count:", 1)[1].splitlines()[0]
                return [(6 if self.identities[table] == "original" else 5,)]
            if "cleanup:snapshot:" in sql:
                table = sql.split("cleanup:snapshot:", 1)[1].splitlines()[0]
                identity = self.identities[table]
                return [
                    (
                        "original-snapshot" if identity == "original" else "compact-snapshot",
                        NOW - timedelta(hours=2) if identity == "original" else NOW,
                    )
                ]
            if "cleanup:inventory-compaction-diff:" in sql:
                return [(0,)]
            if sql.startswith("ALTER TABLE"):
                if f'"{mod.INVENTORY}" RENAME TO "{shadow}"' in sql:
                    self.identities[shadow] = self.identities.pop(mod.INVENTORY)
                elif f'"{backup}" RENAME TO "{mod.INVENTORY}"' in sql:
                    self.identities[mod.INVENTORY] = self.identities.pop(backup)
                return []
            if sql == f'DROP TABLE "iceberg"."bronze"."{shadow}"':
                self.identities.pop(shadow)
                return []
            raise AssertionError(f"unexpected SQL: {sql}")

    client = RecoveryClient()
    changed = mod._reconcile_inventory_swap_names(
        client,
        catalog="iceberg",
        schema="bronze",
        source=mod.INVENTORY,
        shadow=shadow,
        backup=backup,
        spec=spec,
    )
    assert changed is True
    assert client.names == {mod.INVENTORY}


def test_inventory_swap_refuses_name_only_recovery_from_stale_backup():
    shadow = "fotmob_field_inventory__compact_0123456789ab"
    backup = "fotmob_field_inventory__backup_0123456789ab"
    columns = [*mod.INVENTORY_KEYS, "_ingested_at", "_target_batch_id"]
    spec = {
        "source_table": mod.INVENTORY,
        "source_rows": 6,
        "distinct_rows": 5,
        "snapshot_id": "reviewed-snapshot",
        "last_snapshot_at": (NOW - timedelta(hours=2)).isoformat(),
        "columns": columns,
    }
    mutations = []

    class StaleClient:
        def query(self, sql):
            if "cleanup:swap-name-state" in sql:
                return [(mod.INVENTORY,), (backup,)]
            if "cleanup:count:" in sql:
                return [(6,)]
            if "cleanup:snapshot:" in sql:
                return [("stale-unreviewed-snapshot", NOW - timedelta(hours=2))]
            if sql.startswith(("ALTER TABLE", "DROP TABLE")):
                mutations.append(sql)
                return []
            raise AssertionError(f"unexpected SQL: {sql}")

    with pytest.raises(mod.CleanupError, match="not the reviewed inventory source"):
        mod._reconcile_inventory_swap_names(
            StaleClient(),
            catalog="iceberg",
            schema="bronze",
            source=mod.INVENTORY,
            shadow=shadow,
            backup=backup,
            spec=spec,
        )
    assert mutations == []


def test_pending_journal_allows_exact_finalize_after_backup_drop_response_loss():
    promoted = {
        "row_count": 5,
        "snapshot_id": "promoted-snapshot",
        "last_snapshot_at": NOW.isoformat(),
    }
    spec = {
        "source_table": mod.INVENTORY,
        "source_rows": 6,
        "distinct_rows": 5,
        "shadow_table": "fotmob_field_inventory__compact_0123456789ab",
        "backup_table": "fotmob_field_inventory__backup_0123456789ab",
    }

    class FinalizedClient:
        def query(self, sql):
            if "cleanup:swap-name-state" in sql:
                return [(mod.INVENTORY,)]
            if "cleanup:count:fotmob_field_inventory" in sql:
                return [(5,)]
            if "cleanup:snapshot:fotmob_field_inventory" in sql:
                return [("promoted-snapshot", NOW)]
            raise AssertionError(f"unexpected SQL: {sql}")

    result = mod._finalize_inventory_swap(
        FinalizedClient(),
        catalog="iceberg",
        schema="bronze",
        spec=spec,
        promoted_state=promoted,
    )
    assert result["backup_dropped_after_validation"] is True
    assert result["resumed_after_durable_journal"] is True


def test_cleanup_rerun_accepts_exact_admitted_scheduler_already_stopped(
    tmp_path, monkeypatch
):
    from scripts import fotmob_rollback as runtime

    arguments = type(
        "Args",
        (),
        {
            "env_file": tmp_path / "fotmob.env",
            "deployment_report": tmp_path / "deployment.json",
            "release_sha": "a" * 40,
            "project": "fotmob-airflow",
            "compose_file": tmp_path / "compose.yaml",
        },
    )()
    monkeypatch.setattr(
        runtime, "_deployment_context", lambda _args: {"git_sha": "a" * 40}
    )
    monkeypatch.setattr(
        runtime,
        "validate_live_deployment",
        lambda *_a, **_k: {
            "scheduler_running": False,
            "mounts_verified": True,
        },
    )
    monkeypatch.setattr(runtime, "_compose_environment", lambda _args: {})
    monkeypatch.setattr(runtime, "_compose_base", lambda _args: ("compose",))
    monkeypatch.setattr(
        runtime,
        "inspect_writer_state",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("cannot exec an already-stopped scheduler")
        ),
    )

    evidence = mod._quiesce_isolated_scheduler(
        arguments,
        run=lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("no stop subprocess is needed")
        ),
    )
    assert evidence["scheduler_stopped"] is True


def test_execute_drops_only_named_target_when_metadata_matches():
    client = ExecuteClient(count=7)
    result = mod.execute_plan(client, _plan(), clock=lambda: NOW)
    drops = [sql for sql in client.sql if sql.startswith("DROP TABLE")]
    assert drops == [
        'DROP TABLE "iceberg"."bronze"."fotmob_matches__stg_0123456789ab"'
    ]
    assert result["passed"] is True
    assert result["inventory_compaction"] == {"action": "none", "rows": 10}


def test_execute_rejects_wildcard_or_unowned_target():
    plan = _plan()
    plan["staging_targets"][0]["table"] = "fotmob_%"
    with pytest.raises(mod.CleanupError, match="invalid or duplicate"):
        mod.execute_plan(ExecuteClient(count=7), plan, clock=lambda: NOW)


def test_pause_evidence_must_cover_all_writers_and_be_fresh(tmp_path):
    evidence = tmp_path / "pause.json"
    evidence.write_text(
        __import__("json").dumps(
            {
                "passed": True,
                "generated_at": NOW.isoformat(),
                "paused": sorted(mod.PAUSED_DAGS),
                "pause_states": {dag_id: True for dag_id in mod.PAUSED_DAGS},
                "running_runs": {},
                "queued_runs": {},
                "catalog": "iceberg",
                "schema": "bronze",
                "project": "fotmob-airflow",
                "git_sha": "a" * 40,
            }
        )
    )
    kwargs = {
        "catalog": "iceberg",
        "schema": "bronze",
        "project": "fotmob-airflow",
        "release_sha": "a" * 40,
    }
    mod._validate_pause_evidence(evidence, clock=lambda: NOW, **kwargs)
    with pytest.raises(mod.CleanupError, match="older than one hour"):
        mod._validate_pause_evidence(
            evidence, clock=lambda: NOW + timedelta(hours=2), **kwargs
        )


def test_pause_evidence_rejects_future_or_wrong_stack(tmp_path):
    evidence = tmp_path / "pause.json"
    payload = {
        "passed": True,
        "generated_at": (NOW + timedelta(hours=1)).isoformat(),
        "paused": sorted(mod.PAUSED_DAGS),
        "pause_states": {dag_id: True for dag_id in mod.PAUSED_DAGS},
        "running_runs": {},
        "queued_runs": {},
        "catalog": "iceberg",
        "schema": "bronze",
        "project": "wrong",
        "git_sha": "a" * 40,
    }
    evidence.write_text(__import__("json").dumps(payload))
    with pytest.raises(mod.CleanupError, match="stack identity mismatch"):
        mod._validate_pause_evidence(
            evidence,
            clock=lambda: NOW,
            catalog="iceberg",
            schema="bronze",
            project="fotmob-airflow",
            release_sha="a" * 40,
        )
    payload["project"] = "fotmob-airflow"
    evidence.write_text(__import__("json").dumps(payload))
    with pytest.raises(mod.CleanupError, match="in the future"):
        mod._validate_pause_evidence(
            evidence,
            clock=lambda: NOW,
            catalog="iceberg",
            schema="bronze",
            project="fotmob-airflow",
            release_sha="a" * 40,
        )


def test_inventory_compaction_validates_shadow_before_swap_and_drops_only_backup():
    columns = [*mod.INVENTORY_KEYS, "_ingested_at", "_target_batch_id"]

    class CompactionClient:
        def __init__(self):
            self.sql = []
            self.names = {mod.INVENTORY}

        def query(self, sql):
            self.sql.append(sql)
            if "cleanup:columns:fotmob_field_inventory" in sql:
                return [(column,) for column in columns]
            if "cleanup:swap-name-state" in sql:
                return [(name,) for name in sorted(self.names)]
            if sql.startswith("CREATE TABLE"):
                self.names.add("fotmob_field_inventory__compact_0123456789ab")
                return []
            if "HAVING COUNT(*) > 1" in sql:
                return [(0,)]
            if "cleanup:inventory-compaction-diff:" in sql:
                return [(0,)]
            if "cleanup:count:fotmob_field_inventory__backup_" in sql:
                return [(6,)]
            if "cleanup:snapshot:fotmob_field_inventory__backup_" in sql:
                return [("original-snapshot", NOW - timedelta(hours=2))]
            if "cleanup:count:fotmob_field_inventory" in sql:
                return [(5,)]
            if "cleanup:snapshot:fotmob_field_inventory" in sql:
                return [("compact-snapshot", NOW)]
            if "SELECT COUNT(*) FROM" in sql and "__compact_" in sql:
                return [(5,)]
            if sql.startswith("ALTER TABLE"):
                if (
                    '"fotmob_field_inventory" RENAME TO '
                    '"fotmob_field_inventory__backup_0123456789ab"' in sql
                ):
                    self.names.remove(mod.INVENTORY)
                    self.names.add("fotmob_field_inventory__backup_0123456789ab")
                elif (
                    '"fotmob_field_inventory__compact_0123456789ab" '
                    'RENAME TO "fotmob_field_inventory"' in sql
                ):
                    self.names.remove("fotmob_field_inventory__compact_0123456789ab")
                    self.names.add(mod.INVENTORY)
                return []
            if "SELECT COUNT(*) FROM" in sql and '"fotmob_field_inventory"' in sql:
                return [(5,)]
            if sql.startswith("DROP TABLE"):
                self.names.discard("fotmob_field_inventory__backup_0123456789ab")
                return []
            raise AssertionError(f"unexpected SQL: {sql}")

        def close(self):
            pass

    client = CompactionClient()
    result = mod._execute_inventory_swap(
        client,
        catalog="iceberg",
        schema="bronze",
        spec={
            "action": "shadow_swap",
            "source_table": mod.INVENTORY,
            "source_rows": 6,
            "distinct_rows": 5,
            "snapshot_id": "original-snapshot",
            "last_snapshot_at": (NOW - timedelta(hours=2)).isoformat(),
            "columns": columns,
            "shadow_table": "fotmob_field_inventory__compact_0123456789ab",
            "backup_table": "fotmob_field_inventory__backup_0123456789ab",
        },
    )
    assert result["duplicates_removed"] == 1
    assert result["backup_dropped_after_validation"] is False
    statements = "\n".join(client.sql)
    assert statements.index("CREATE TABLE") < statements.index("ALTER TABLE")
    assert not [sql for sql in client.sql if sql.startswith("DROP TABLE")]
    assert client.names == {
        mod.INVENTORY,
        "fotmob_field_inventory__backup_0123456789ab",
    }


def test_inventory_swap_restores_source_when_final_validation_query_fails():
    columns = [*mod.INVENTORY_KEYS, "_ingested_at", "_target_batch_id"]

    class FailingValidationClient:
        def __init__(self):
            self.sql = []
            self.identities = {mod.INVENTORY: "original"}

        @property
        def names(self):
            return set(self.identities)

        def query(self, sql):
            self.sql.append(sql)
            if "cleanup:columns:fotmob_field_inventory" in sql:
                return [(column,) for column in columns]
            if "cleanup:swap-name-state" in sql:
                return [(name,) for name in sorted(self.names)]
            if sql.startswith("CREATE TABLE"):
                self.identities[
                    "fotmob_field_inventory__compact_0123456789ab"
                ] = "compact"
                return []
            if "HAVING COUNT(*) > 1" in sql:
                return [(0,)]
            if "cleanup:inventory-compaction-diff:" in sql:
                return [(0,)]
            if "cleanup:count:" in sql:
                table = sql.split("cleanup:count:", 1)[1].splitlines()[0]
                return [(6 if self.identities[table] == "original" else 5,)]
            if "cleanup:snapshot:" in sql:
                table = sql.split("cleanup:snapshot:", 1)[1].splitlines()[0]
                identity = self.identities[table]
                return [
                    (
                        "original-snapshot" if identity == "original" else "compact-snapshot",
                        NOW - timedelta(hours=2) if identity == "original" else NOW,
                    )
                ]
            if "SELECT COUNT(*) FROM" in sql and "__compact_" in sql:
                return [(5,)]
            if sql.startswith("ALTER TABLE"):
                if (
                    '"fotmob_field_inventory" RENAME TO '
                    '"fotmob_field_inventory__backup_0123456789ab"' in sql
                ):
                    self.identities[
                        "fotmob_field_inventory__backup_0123456789ab"
                    ] = self.identities.pop(mod.INVENTORY)
                elif (
                    '"fotmob_field_inventory__compact_0123456789ab" '
                    'RENAME TO "fotmob_field_inventory"' in sql
                ):
                    self.identities[mod.INVENTORY] = self.identities.pop(
                        "fotmob_field_inventory__compact_0123456789ab"
                    )
                elif (
                    '"fotmob_field_inventory" RENAME TO '
                    '"fotmob_field_inventory__compact_0123456789ab"' in sql
                ):
                    self.identities[
                        "fotmob_field_inventory__compact_0123456789ab"
                    ] = self.identities.pop(mod.INVENTORY)
                elif (
                    '"fotmob_field_inventory__backup_0123456789ab" '
                    'RENAME TO "fotmob_field_inventory"' in sql
                ):
                    self.identities[mod.INVENTORY] = self.identities.pop(
                        "fotmob_field_inventory__backup_0123456789ab"
                    )
                return []
            if sql.startswith("DROP TABLE"):
                self.identities.pop(
                    "fotmob_field_inventory__compact_0123456789ab", None
                )
                return []
            if "SELECT COUNT(*) FROM" in sql and '"fotmob_field_inventory"' in sql:
                raise RuntimeError("lost Trino response")
            raise AssertionError(f"unexpected SQL: {sql}")

    client = FailingValidationClient()
    with pytest.raises(mod.CleanupError, match="reviewed source restored"):
        mod._execute_inventory_swap(
            client,
            catalog="iceberg",
            schema="bronze",
            spec={
                "action": "shadow_swap",
                "source_table": mod.INVENTORY,
                "source_rows": 6,
                "distinct_rows": 5,
                "snapshot_id": "original-snapshot",
                "last_snapshot_at": (NOW - timedelta(hours=2)).isoformat(),
                "columns": columns,
                "shadow_table": "fotmob_field_inventory__compact_0123456789ab",
                "backup_table": "fotmob_field_inventory__backup_0123456789ab",
            },
        )
    alters = [sql for sql in client.sql if sql.startswith("ALTER TABLE")]
    assert alters[-2:] == [
        'ALTER TABLE "iceberg"."bronze"."fotmob_field_inventory" '
        'RENAME TO "fotmob_field_inventory__compact_0123456789ab"',
        'ALTER TABLE "iceberg"."bronze"."fotmob_field_inventory__backup_0123456789ab" '
        'RENAME TO "fotmob_field_inventory"',
    ]
    assert [sql for sql in client.sql if sql.startswith("DROP TABLE")] == [
        'DROP TABLE "iceberg"."bronze".'
        '"fotmob_field_inventory__compact_0123456789ab"'
    ]
    assert client.names == {mod.INVENTORY}
