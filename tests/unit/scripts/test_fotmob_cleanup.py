from datetime import datetime, timedelta, timezone
import subprocess

import pytest

from scripts import fotmob_cleanup as mod
from scrapers.fotmob.repository import DEDUP_KEYS


NOW = datetime(2026, 7, 21, 12, tzinfo=timezone.utc)


def _shared_consumer_pause():
    return {
        "shared_scheduler_container_id": "b" * 64,
        "schedule_owner": "isolated",
        "pause_states_before": {
            **mod.runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES,
            "dag_sofascore_pipeline": False,
        },
        "pause_states_after": dict(
            mod.runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES
        ),
        "active_runs": [],
        "active_task_instances": [],
        "atomic_metadata_transaction": True,
    }


def test_cleanup_pause_evidence_requires_atomic_shared_consumer_fence(tmp_path):
    evidence = tmp_path / "pause.json"
    evidence.write_text(
        __import__("json").dumps(
            {
                "passed": True,
                "paused": sorted(mod.PAUSED_DAGS),
                "pause_states": {dag_id: True for dag_id in mod.PAUSED_DAGS},
                "running_runs": {},
                "queued_runs": {},
                "catalog": "iceberg",
                "schema": "bronze",
                "project": "fotmob-airflow",
                "git_sha": "a" * 40,
                "generated_at": NOW.isoformat(),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(mod.CleanupError, match="shared_consumer_pause"):
        mod._validate_pause_evidence(
            evidence,
            clock=lambda: NOW,
            catalog="iceberg",
            schema="bronze",
            project="fotmob-airflow",
            release_sha="a" * 40,
        )


def test_cleanup_pause_evidence_covers_exact_six_writer_inventory(tmp_path):
    expected = {
        "dag_orchestrate_fotmob",
        "dag_ingest_fotmob",
        "dag_trigger_fotmob_daily",
        "dag_refresh_fotmob",
        "dag_backfill_fotmob",
        "dag_collect_fotmob_players",
    }
    assert mod.PAUSED_DAGS == expected

    evidence = tmp_path / "pause.json"
    payload = {
        "passed": True,
        "paused": sorted(expected - {"dag_backfill_fotmob"}),
        "pause_states": {
            dag_id: True for dag_id in expected - {"dag_backfill_fotmob"}
        },
        "running_runs": {},
        "queued_runs": {},
        "catalog": "iceberg",
        "schema": "bronze",
        "project": "fotmob-airflow",
        "git_sha": "a" * 40,
        "generated_at": NOW.isoformat(),
    }
    evidence.write_text(__import__("json").dumps(payload), encoding="utf-8")

    with pytest.raises(mod.CleanupError, match="every FotMob writer"):
        mod._validate_pause_evidence(
            evidence,
            clock=lambda: NOW,
            catalog="iceberg",
            schema="bronze",
            project="fotmob-airflow",
            release_sha="a" * 40,
        )


@pytest.mark.parametrize("active_key", ("running_runs", "queued_runs"))
def test_cleanup_pause_evidence_rejects_any_active_six_writer(tmp_path, active_key):
    evidence = tmp_path / "pause.json"
    payload = {
        "passed": True,
        "paused": sorted(mod.PAUSED_DAGS),
        "pause_states": {dag_id: True for dag_id in mod.PAUSED_DAGS},
        "running_runs": {},
        "queued_runs": {},
        "catalog": "iceberg",
        "schema": "bronze",
        "project": "fotmob-airflow",
        "git_sha": "a" * 40,
        "generated_at": NOW.isoformat(),
    }
    payload[active_key] = {"dag_backfill_fotmob": ["run-1"]}
    evidence.write_text(__import__("json").dumps(payload), encoding="utf-8")

    with pytest.raises(mod.CleanupError, match="active writer runs"):
        mod._validate_pause_evidence(
            evidence,
            clock=lambda: NOW,
            catalog="iceberg",
            schema="bronze",
            project="fotmob-airflow",
            release_sha="a" * 40,
        )


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
    assert plan["dynamic_catalog_evidence_action"] == "retain"
    assert plan["dynamic_catalog_evidence_objects"] == sorted(
        mod.PRESERVED_DYNAMIC_CATALOG_EVIDENCE
    )


def _plan(*, row_count=7):
    return {
        "schema_version": "fotmob-cleanup-plan-v1",
        "generated_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "catalog": "iceberg",
        "schema": "bronze",
        "dry_run": True,
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(
            mod.PRESERVED_DYNAMIC_CATALOG_EVIDENCE
        ),
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


def test_cleanup_live_readback_rejects_changed_shared_container(tmp_path, monkeypatch):
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
    monkeypatch.setattr(runtime, "_deployment_context", lambda _args: {"git_sha": "a" * 40})
    monkeypatch.setattr(
        runtime,
        "validate_live_deployment",
        lambda *_a, **_k: {"scheduler_running": False, "mounts_verified": True},
    )
    monkeypatch.setattr(runtime, "_compose_environment", lambda _args: {})
    monkeypatch.setattr(runtime, "_compose_base", lambda _args: ("compose",))
    monkeypatch.setattr(
        runtime,
        "inspect_shared_consumer_pause",
        lambda *_a, **_k: (_ for _ in ()).throw(
            runtime.RollbackError("shared scheduler identity changed after pause evidence")
        ),
    )

    with pytest.raises(mod.CleanupError, match="shared consumer live quiescence"):
        mod._quiesce_isolated_scheduler(
            arguments,
            shared_consumer_pause=_shared_consumer_pause(),
            run=lambda *_a, **_k: (_ for _ in ()).throw(
                AssertionError("scheduler is already stopped")
            ),
        )


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
                "shared_consumer_pause": _shared_consumer_pause(),
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
        "shared_consumer_pause": _shared_consumer_pause(),
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


class CleanupLease:
    def __init__(self, *, lose_at=0):
        self.checks = 0
        self.lose_at = lose_at

    def check(self):
        self.checks += 1
        if self.checks == self.lose_at:
            raise RuntimeError('writer lease lost')


def _isolated_plan():
    plan = _plan()
    plan['mode'] = 'isolated_staging_only'
    plan['older_than_hours'] = 24
    plan['inventory_compaction'] = {'source_table': mod.INVENTORY, 'action': 'retain'}
    return plan


def test_isolated_plan_never_reads_inventory():
    client = PlanClient()
    plan = mod.build_plan(client, catalog='iceberg', schema='bronze',
                          older_than_hours=24, clock=lambda: NOW, isolated_stack=True)
    assert plan['inventory_compaction'] == {'source_table': mod.INVENTORY, 'action': 'retain'}
    assert plan['mode'] == 'isolated_staging_only'
    assert not any('cleanup:inventory' in sql or 'fotmob_field_inventory' in sql
                   for sql in client.sql)


def test_isolated_execution_checks_same_lease_before_every_drop():
    client = ExecuteClient(7)
    lease = CleanupLease()
    report = mod.execute_isolated_plan(client, _isolated_plan(), lease=lease, clock=lambda: NOW)
    assert report['passed'] is True
    assert lease.checks >= 3
    assert [sql for sql in client.sql if sql.startswith('DROP TABLE')] == [
        'DROP TABLE "iceberg"."bronze"."fotmob_matches__stg_0123456789ab"']
    assert not any('fotmob_field_inventory' in sql for sql in client.sql)


def test_isolated_execution_rejects_lost_lease_before_mutation():
    client = ExecuteClient(7)
    with pytest.raises(RuntimeError, match='lease lost'):
        mod.execute_isolated_plan(client, _isolated_plan(), lease=CleanupLease(lose_at=2), clock=lambda: NOW)
    assert not any(sql.startswith('DROP TABLE') for sql in client.sql)


@pytest.mark.parametrize('alteration', ['age', 'compaction', 'mode', 'target'])
def test_isolated_execution_rejects_unreviewable_targets(alteration):
    plan = _isolated_plan()
    if alteration == 'age':
        plan['staging_targets'][0]['last_snapshot_at'] = (NOW - timedelta(hours=1)).isoformat()
    elif alteration == 'compaction':
        plan['inventory_compaction']['action'] = 'shadow_swap'
    elif alteration == 'mode':
        plan.pop('mode')
    else:
        plan['staging_targets'][0]['table'] = 'fotmob_matches'
    client = ExecuteClient(7)
    with pytest.raises(mod.CleanupError):
        mod.execute_isolated_plan(client, plan, lease=CleanupLease(), clock=lambda: NOW)
    assert not any(sql.startswith('DROP TABLE') for sql in client.sql)


def test_isolated_all_target_preflight_still_precedes_drop():
    plan = _isolated_plan()
    other = dict(plan['staging_targets'][0])
    other['table'] = 'fotmob_standings__stg_abcdef012345'
    other['qualified_table'] = 'iceberg.bronze.' + other['table']
    plan['staging_targets'].append(other)

    class Drift(ExecuteClient):
        def query(self, sql):
            if 'cleanup:count:fotmob_standings' in sql:
                self.sql.append(sql)
                return [(999,)]
            return super().query(sql)
    client = Drift(7)
    with pytest.raises(mod.CleanupError, match='metadata changed'):
        mod.execute_isolated_plan(client, plan, lease=CleanupLease(), clock=lambda: NOW)
    assert not any(sql.startswith('DROP TABLE') for sql in client.sql)


def test_isolated_cli_validates_reviewed_sha_before_remote_execution(tmp_path, monkeypatch):
    import json
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps(_isolated_plan()))
    output = tmp_path / 'result.json'
    calls = []
    monkeypatch.setattr(mod, '_now_dt', lambda: NOW)
    assert mod.main(['execute', '--isolated-stack', '--output', str(output),
                     '--plan', str(plan), '--plan-sha256', '0' * 64,
                     '--confirm', mod.CONFIRM_EXECUTE, '--release-sha', 'a' * 40,
                     '--release-root', str(tmp_path), '--scheduler-container-id', 'b' * 64],
                    run=lambda *a, **k: calls.append(a)) == 1
    assert 'SHA-256 mismatch' in json.loads(output.read_text())['error']
    assert calls == []


def _host_attestation(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace
    root = tmp_path / 'release'
    path = root / 'scripts/fotmob_cleanup.py'
    path.parent.mkdir(parents=True)
    content = b'# reviewed isolated cleanup\n'
    path.write_bytes(content)
    blob = hashlib.sha1(b'blob ' + str(len(content)).encode() + b'\0' + content).hexdigest()
    env = {'FOTMOB_ISOLATED_STACK': '1', 'ALERT_ENV': 'fotmob-isolated', 'TRINO_HOST': 'trino',
           'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': 'postgresql+psycopg2://airflow:secret@fotmob-airflow-metadb:5432/airflow'}
    container = {'Id': 'b' * 64, 'Name': '/fotmob-airflow-scheduler', 'State': {'Running': True},
                 'Config': {'Hostname': 'b' * 12, 'Env': [key + '=' + value for key, value in env.items()],
                            'Labels': {'com.docker.compose.project': 'fotmob-airflow',
                                       'com.docker.compose.service': 'airflow-scheduler'}},
                 'Mounts': [{'Destination': '/opt/airflow/' + relative, 'Source': str(root / relative),
                             'Type': 'bind', 'RW': False} for relative in mod.ISOLATED_RUNTIME_ROOTS]}
    args = SimpleNamespace(scheduler_container_id='b' * 64, project='fotmob-airflow',
                           release_root=root, release_sha='a' * 40)
    monkeypatch.setattr(mod.runtime_binding, '_inspect_container', lambda *a, **k: container)
    git_output = {'rev-parse': 'a' * 40, 'status': '', 'ls-files': '',
                  'ls-tree': '100644 blob ' + blob + '\tscripts/fotmob_cleanup.py\n'}
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        assert command[:1] == ('git',)
        return subprocess.CompletedProcess(command, 0, stdout=git_output[command[3]], stderr='')
    return args, container, git_output, calls, run


def test_isolated_host_attests_exact_readonly_runtime_without_writes(tmp_path, monkeypatch):
    args, _, _, calls, run = _host_attestation(tmp_path, monkeypatch)
    identity = mod._attest_isolated_host(args, run=run)
    assert identity['git_sha'] == 'a' * 40
    assert identity['scheduler_container_id'] == 'b' * 64
    assert '/opt/airflow/scripts/fotmob_cleanup.py' in identity['manifest']
    assert 'secret' not in str(identity)
    assert not any('checkout' in command or 'stop' in command for command in calls)


@pytest.mark.parametrize('change', ['id', 'project', 'service', 'stopped', 'isolated', 'ceremony',
                                     'disabled-lock', 'mount-rw', 'mount-source', 'overlay', 'sha',
                                     'dirty', 'ignored-code', 'blob'])
def test_isolated_host_rejects_unattested_stack(tmp_path, monkeypatch, change):
    args, container, git_output, _, run = _host_attestation(tmp_path, monkeypatch)
    if change == 'id':
        container['Id'] = 'c' * 64
    elif change == 'project':
        container['Config']['Labels']['com.docker.compose.project'] = 'data-platform'
    elif change == 'service':
        container['Config']['Labels']['com.docker.compose.service'] = 'airflow-webserver'
    elif change == 'stopped':
        container['State']['Running'] = False
    elif change == 'isolated':
        container['Config']['Env'][0] = 'FOTMOB_ISOLATED_STACK=0'
    elif change == 'ceremony':
        container['Config']['Env'].append('FOTMOB_DEPLOYMENT_REPORT_PATH=/report.json')
    elif change == 'disabled-lock':
        container['Config']['Env'].append('FOTMOB_WRITER_LOCK=false')
    elif change == 'mount-rw':
        container['Mounts'][0]['RW'] = True
    elif change == 'mount-source':
        container['Mounts'][0]['Source'] = '/other/tree/dags'
    elif change == 'overlay':
        container['Mounts'].append({'Destination': '/opt/airflow/scripts/fotmob_cleanup.py',
                                     'Source': '/old/cleanup.py', 'Type': 'bind', 'RW': False})
    elif change == 'sha':
        git_output['rev-parse'] = 'c' * 40
    elif change == 'dirty':
        git_output['status'] = ' M scripts/fotmob_cleanup.py'
    elif change == 'ignored-code':
        git_output['ls-files'] = 'scrapers/fotmob/old_untracked.py'
    else:
        (args.release_root / 'scripts/fotmob_cleanup.py').write_bytes(b'# altered\n')
    with pytest.raises(mod.CleanupError):
        mod._attest_isolated_host(args, run=run)


def _scheduler_request():
    import hashlib
    import json
    identity = {'scheduler_container_id': 'b' * 64, 'git_sha': 'a' * 40, 'binding_digest': 'd' * 64}
    plan = _isolated_plan()
    plan['isolated_runtime'] = {**identity, 'runtime_bytes_verified': True, 'scheduler_bound_client': True}
    plan['data_plane_identity'] = [['trino-node', 'http://trino:8080']]
    text = json.dumps(plan)
    return {'identity': identity, 'command': 'execute', 'catalog': 'iceberg', 'schema': 'bronze',
            'plan_text': text, 'plan_sha256': hashlib.sha256(text.encode()).hexdigest()}


@pytest.mark.parametrize('refusal', ['active', 'binding', 'busy', 'disabled', 'runtime-plan'])
def test_isolated_scheduler_refuses_before_drop(tmp_path, monkeypatch, refusal):
    from contextlib import contextmanager
    from dags.scripts import run_fotmob_scraper as runner
    request = _scheduler_request()
    client = ExecuteClient(7)
    attestations = []
    def attest(identity):
        attestations.append(identity)
        if refusal == 'binding' and len(attestations) > 1:
            raise mod.CleanupError('binding changed')
    @contextmanager
    def lock():
        if refusal == 'busy':
            raise RuntimeError('busy')
        yield False if refusal == 'disabled' else CleanupLease()
    def activity(_client):
        if refusal == 'active':
            raise mod.CleanupError('active writes')
        return {'trino_nodes': [['trino-node', 'http://trino:8080']]}
    monkeypatch.setattr(mod, '_attest_isolated_process', attest)
    monkeypatch.setattr(mod, '_isolated_activity', activity)
    monkeypatch.setattr(mod, '_connect_isolated_from_env', lambda **k: client)
    monkeypatch.setattr(mod, '_now_dt', lambda: NOW)
    monkeypatch.setattr(runner, '_writer_lock', lock)
    if refusal == 'runtime-plan':
        request['identity']['git_sha'] = 'c' * 40
    with pytest.raises((mod.CleanupError, RuntimeError)):
        mod._run_isolated_request(request)
    assert not any(sql.startswith('DROP TABLE') for sql in client.sql)


def test_isolated_cursor_has_live_lease_watcher_during_drop():
    from contextlib import contextmanager
    from types import SimpleNamespace
    events = []
    class Lease(CleanupLease):
        @contextmanager
        def watch(self, cancel):
            events.append('watch-start')
            yield
            events.append('watch-end')
    class Cursor:
        def cancel(self):
            events.append('cancel')
        def execute(self, sql):
            assert events[-1] == 'watch-start'
            events.append(sql)
        def fetchall(self):
            return []
        def close(self):
            events.append('close')
    client = SimpleNamespace(connection=SimpleNamespace(cursor=lambda: Cursor()))
    lease = Lease()
    mod._LeaseQueryClient(client, lease).query('DROP TABLE "iceberg"."bronze"."fotmob_matches__stg_123"')
    assert events[0] == 'watch-start'
    assert events[-2:] == ['watch-end', 'close']
    assert lease.checks == 2


def test_isolated_scheduler_holds_one_lease_across_all_preflight_and_drops(monkeypatch):
    from contextlib import contextmanager
    from dags.scripts import run_fotmob_scraper as runner
    held = [False]
    events = []
    lease = CleanupLease()
    class Client(ExecuteClient):
        def query(self, sql):
            assert held[0]
            events.append('drop' if sql.startswith('DROP TABLE') else 'preflight')
            return super().query(sql)
    client = Client(7)
    @contextmanager
    def lock():
        assert not held[0]
        held[0] = True
        events.append('acquired')
        try:
            yield lease
        finally:
            held[0] = False
            events.append('released')
    def activity(_client):
        assert held[0]
        events.append('activity')
        return {'trino_nodes': [['trino-node', 'http://trino:8080']]}
    monkeypatch.setattr(mod, '_attest_isolated_process', lambda _identity: None)
    monkeypatch.setattr(mod, '_isolated_activity', activity)
    monkeypatch.setattr(mod, '_connect_isolated_from_env', lambda **k: client)
    monkeypatch.setattr(mod, '_LeaseQueryClient', lambda c, _lease: c)
    monkeypatch.setattr(mod, '_now_dt', lambda: NOW)
    monkeypatch.setattr(runner, '_writer_lock', lock)
    report = mod._run_isolated_request(_scheduler_request())
    assert report['passed'] is True
    assert events[0] == 'acquired' and events[-1] == 'released'
    assert events.count('activity') == 2
    assert events.index('drop') > max(i for i, value in enumerate(events) if value == 'preflight')
    assert lease.checks >= 6


def test_isolated_plan_cannot_use_ceremony_execute_path():
    with pytest.raises(mod.CleanupError, match='requires --isolated-stack'):
        mod._validate_plan_shape(_isolated_plan(), clock=lambda: NOW)


@pytest.mark.parametrize('command', ['plan', 'execute'])
def test_isolated_cli_uses_only_attested_scheduler_bound_request(tmp_path, monkeypatch, command):
    import hashlib
    import json
    identity = {'scheduler_container_id': 'b' * 64, 'git_sha': 'a' * 40,
                'binding_digest': 'd' * 64, 'manifest': {}, 'hostname': 'b' * 12}
    monkeypatch.setattr(mod, '_attest_isolated_host', lambda args, **k: identity)
    monkeypatch.setattr(mod, '_now_dt', lambda: NOW)
    monkeypatch.setattr(mod, '_quiesce_isolated_scheduler', lambda *a, **k: pytest.fail('ceremony stop forbidden'))
    request = _scheduler_request()
    path = tmp_path / 'plan.json'
    path.write_text(request['plan_text'])
    output = tmp_path / 'result.json'
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        assert argv[:4] == ('docker', 'exec', '-i', 'b' * 64)
        payload = json.loads(kwargs['input'])
        assert payload['command'] == command
        if command == 'execute':
            assert payload['plan_text'].encode() == path.read_bytes()
            assert payload['plan_sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
        return subprocess.CompletedProcess(argv, 0, stdout='FOTMOB_ISOLATED_CLEANUP_JSON={"passed":true}\n')
    args = [command, '--isolated-stack', '--output', str(output), '--release-sha', 'a' * 40,
            '--release-root', str(tmp_path), '--scheduler-container-id', 'b' * 64]
    if command == 'execute':
        args.extend(['--plan', str(path), '--plan-sha256', request['plan_sha256'],
                     '--confirm', mod.CONFIRM_EXECUTE])
    assert mod.main(args, run=run, client_factory=lambda **k: pytest.fail('host client forbidden')) == 0
    assert len(calls) == 1
    assert json.loads(output.read_text())['passed'] is True


@pytest.mark.parametrize('change', ['binding', 'bytes', 'hostname'])
def test_isolated_process_rejects_runtime_drift(tmp_path, monkeypatch, change):
    import hashlib
    import socket
    content = tmp_path / 'tracked.py'
    content.write_text('reviewed')
    env = {'FOTMOB_ISOLATED_STACK': '1', 'ALERT_ENV': 'fotmob-isolated', 'TRINO_HOST': 'trino',
           'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': 'postgresql+psycopg2://airflow:secret@fotmob-airflow-metadb:5432/airflow'}
    identity = {'hostname': 'scheduler', 'binding_digest': mod._binding_digest(env),
                'manifest': {str(content): hashlib.sha256(content.read_bytes()).hexdigest()}}
    monkeypatch.setattr(mod.os, 'environ', env)
    monkeypatch.setattr(socket, 'gethostname', lambda: 'scheduler')
    mod._attest_isolated_process(identity)
    if change == 'binding':
        env['FBREF_CONTROL_DB_URI'] = 'postgresql://different-domain/db'
    elif change == 'bytes':
        content.write_text('changed')
    else:
        identity['hostname'] = 'other'
    with pytest.raises(mod.CleanupError):
        mod._attest_isolated_process(identity)


def test_sigkill_leaves_staging_for_a_new_scheduler_bound_cleanup_process(tmp_path):
    """SIGKILL cannot run finalizers; the next process uses durable reviewed state."""
    import json
    import signal
    import sys
    from pathlib import Path

    catalog = tmp_path / 'catalog.json'
    stage = 'fotmob_matches__stg_0123456789ab'
    unreviewed = 'fotmob_matches__stg_not_reviewed'
    state = {stage: {'row_count': 7, 'snapshot_id': 'staging-snapshot-1',
                     'last_snapshot_at': (NOW - timedelta(hours=48)).isoformat()},
             unreviewed: {'row_count': 3}, mod.INVENTORY: {'row_count': 10}}
    seed_code = """import json,sys
from pathlib import Path
path=Path(sys.argv[1])
path.write_text(sys.stdin.readline())
print('staging-ready',flush=True)
# Simulate the paused append before promotion. SIGKILL skips this finalizer.
try:
    sys.stdin.read(1)
finally:
    state=json.loads(path.read_text())
    state.pop('fotmob_matches__stg_0123456789ab',None)
    path.write_text(json.dumps(state))
"""
    seed = subprocess.Popen([sys.executable, '-c', seed_code, str(catalog)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    try:
        seed.stdin.write(json.dumps(state) + '\n')
        seed.stdin.flush()
        assert seed.stdout.readline().strip() == 'staging-ready'
        seed.kill()
        seed.communicate(timeout=10)
        assert seed.returncode == -signal.SIGKILL
    finally:
        if seed.poll() is None:
            seed.kill()
            seed.communicate(timeout=10)
    assert json.loads(catalog.read_text()) == state

    recovery_code = """import json,sys
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from scripts import fotmob_cleanup as mod
from dags.scripts import run_fotmob_scraper as runner
path=Path(sys.argv[1])
request=json.load(sys.stdin)
held=False
watching=False
events=[]
class Lease:
    def check(self):
        assert held
        events.append('check')
    @contextmanager
    def watch(self,cancel):
        global watching
        assert held and not watching
        watching=True
        events.append('watch-start')
        try:
            yield
        finally:
            watching=False
            events.append('watch-end')
@contextmanager
def lock():
    global held
    assert not held
    held=True
    events.append('acquired')
    try:
        yield Lease()
    finally:
        held=False
        events.append('released')
class Cursor:
    def execute(self,sql):
        assert held and watching
        state=json.loads(path.read_text())
        if 'cleanup:table-exists:' in sql:
            name=sql.split('cleanup:table-exists:',1)[1].splitlines()[0]
            self.rows=[(int(name in state),)]
        elif 'cleanup:count:' in sql:
            name=sql.split('cleanup:count:',1)[1].splitlines()[0]
            self.rows=[(state[name]['row_count'],)]
        elif 'cleanup:snapshot:' in sql:
            name=sql.split('cleanup:snapshot:',1)[1].splitlines()[0]
            self.rows=[(state[name]['snapshot_id'],state[name]['last_snapshot_at'])]
        elif sql.startswith('DROP TABLE'):
            assert events[-1]=='watch-start'
            name=sql.rsplit('.',1)[1].strip('"')
            assert name=='fotmob_matches__stg_0123456789ab'
            state.pop(name)
            path.write_text(json.dumps(state))
            events.append('drop:'+name)
            self.rows=[]
        else:
            raise AssertionError(sql)
    def fetchall(self):
        return self.rows
    def cancel(self):
        events.append('cancel')
    def close(self):
        pass
class Client:
    connection=None
    def __init__(self):
        self.connection=self
    def cursor(self):
        return Cursor()
    def close(self):
        pass
def attest(identity):
    if len(events):
        assert held
    events.append('attest')
def activity(client):
    assert held
    events.append('activity')
    return {'trino_nodes':[['trino-node','http://trino:8080']]}
mod._attest_isolated_process=attest
mod._isolated_activity=activity
mod._connect_isolated_from_env=lambda **kwargs: Client()
mod._now_dt=lambda: datetime.fromisoformat(json.loads(request['plan_text'])['generated_at'])
runner._writer_lock=lock
report=mod._run_isolated_request(request)
print(json.dumps({'report':report,'events':events}))
"""
    # Fresh interpreter, persistent catalog, real lease adapter; all backends fake.
    recovered = subprocess.run([sys.executable, '-c', recovery_code, str(catalog)],
                               input=json.dumps(_scheduler_request()), text=True,
                               capture_output=True, check=True, timeout=20,
                               cwd=Path(mod.__file__).resolve().parents[1])
    output = json.loads(recovered.stdout)
    assert output['report']['passed'] is True
    assert output['report']['dropped_staging'] == [
        {'table': stage, 'row_count': 7, 'already_absent': False}]
    assert json.loads(catalog.read_text()) == {key: value for key, value in state.items() if key != stage}
    events = output['events']
    assert events.count('acquired') == 1 and events.count('released') == 1
    assert events.count('drop:' + stage) == 1
    assert events.index('acquired') < events.index('drop:' + stage) < events.index('released')
    assert events[-1] == 'released'


def test_isolated_real_trino_transport_never_replays_drop_after_lease_loss(monkeypatch):
    import requests
    from scrapers.fotmob.writer_lock import WriterLockLost, writer_lock
    from tests.unit.scrapers.test_fotmob_writer_lock import Connection, fake_pg
    monkeypatch.setenv('TRINO_HOST', 'offline-only.invalid')
    monkeypatch.setenv('TRINO_HTTP_SCHEME', 'http')
    connection = Connection()
    fake_pg(monkeypatch, connection)
    factory = getattr(mod, '_connect_isolated_from_env', mod.connect_from_env)
    client = factory(catalog='iceberg', schema='bronze')
    attempts = []
    with writer_lock({}, key=1, wait_seconds=0, poll_seconds=.01) as lease:
        class OfflineSession(requests.Session):
            def post(self, url, **kwargs):
                attempts.append({'lost': lease._lost.is_set(), 'timeout': kwargs.get('timeout')})
                connection.owns = False
                assert lease._lost.wait(1), 'real heartbeat failed to revoke lease'
                raise requests.ConnectionError('lost response after DROP')
        client.connection._http_session = OfflineSession()
        with pytest.raises(WriterLockLost):
            mod._LeaseQueryClient(client, lease).query(
                'DROP TABLE "iceberg"."bronze"."fotmob_matches__stg_review"'
            )
    client.close()
    assert attempts == [{'lost': False, 'timeout': (3.0, 5.0)}]
