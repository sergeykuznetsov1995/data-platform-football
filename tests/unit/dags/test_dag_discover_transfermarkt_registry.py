from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
import importlib
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace

import pytest

from dags.utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
from dags.utils import transfermarkt_registry_publish as registry_publish


RUN_ID = "manual__2026-07-11T18:00:00+00:00"
SCHEDULED_RUN_ID = "scheduled__2026-08-01T02:00:00+00:00"
SNAPSHOT_ID = "tm-discovery-" + "a" * 24


@pytest.fixture
def dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    BashOperator._instances.clear()
    sys.modules.pop("dags.dag_discover_transfermarkt_registry", None)
    module = importlib.import_module("dags.dag_discover_transfermarkt_registry")
    yield module


def _configure_paths(module, monkeypatch, tmp_path: Path) -> dict[str, Path]:
    approval_root = (tmp_path / "approvals").resolve()
    state_root = (tmp_path / "state").resolve()
    output_root = state_root / "manifests"
    cache_path = state_root / "cache" / "http.json"
    approval_root.mkdir(parents=True)
    monkeypatch.setattr(module, "APPROVAL_ROOT", approval_root)
    monkeypatch.setattr(module, "STATE_ROOT", state_root)
    monkeypatch.setattr(module, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(module, "CACHE_PATH", cache_path)
    monkeypatch.setenv("TM_PROXY_CONTROL_URL", "http://proxy_filter:8899")
    return {
        "approval_root": approval_root,
        "state_root": state_root,
        "output_root": output_root,
        "cache_path": cache_path,
    }


def _packet(
    *,
    packet_id: str,
    action: str,
    argv: tuple[str, ...],
    affected_tables: tuple[str, ...],
    affected_files: tuple[str, ...],
) -> ApprovalPacket:
    cap = 15 * 1024 * 1024 if action == "paid_proxy" else 0
    requests = 1024 if action in {"paid_proxy", "production_write"} and cap else 0
    retries = 96 if requests else 0
    # The Bronze writer shares the discovery command's already bounded proxy
    # process, so its packet carries the same cap/attempt envelope.
    if action == "production_write" and "bronze" in packet_id:
        cap = 15 * 1024 * 1024
        requests = 1024
        retries = 96
    return ApprovalPacket(
        packet_id=packet_id,
        action=action,
        argv=argv,
        byte_cap_bytes=cap,
        byte_cap_mib=cap / 1024 / 1024,
        request_limit=requests,
        retry_limit=retries,
        concurrency=1,
        expected_duration_seconds=3600,
        affected_tables=affected_tables,
        affected_files=affected_files,
        stop_conditions=("stop on any cap, DQ, schema, or CAS failure",),
        backup_commands=(("trino", "--execute", "SELECT 1"),),
        rollback_commands=(("trino", "--execute", "SELECT 1"),),
    )


def _issue_approved(journal: ApprovalJournal, packet: ApprovalPacket) -> None:
    journal.issue(
        packet,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
    )
    journal.approve(packet, presented_hash=packet.packet_hash)


def _write_packet(path: Path, packet: ApprovalPacket) -> None:
    path.write_text(json.dumps(packet.payload()), encoding="utf-8")


def _discovery_approvals(module, paths: dict[str, Path], run_id: str = RUN_ID):
    cycle_id = module._cycle_id(run_id)
    checkpoint = paths["state_root"] / "checkpoints" / f"{cycle_id}.json"
    journal_path = paths["approval_root"] / "journal.json"
    paid_path = paths["approval_root"] / "paid.json"
    bronze_path = paths["approval_root"] / "bronze.json"
    promotion_path = paths["approval_root"] / "promotion.json"
    argv = module._discovery_argv(
        cycle_id=cycle_id,
        run_id=run_id,
        proxy_control_url="http://proxy_filter:8899",
        checkpoint=checkpoint,
        cache=paths["cache_path"],
        output_root=paths["output_root"],
        paid_packet=paid_path,
        bronze_packet=bronze_path,
        journal=journal_path,
    )
    files = (
        str(checkpoint),
        str(paths["cache_path"]),
        str(paths["output_root"]),
        str(journal_path),
    )
    paid = _packet(
        packet_id="registry-paid",
        action="paid_proxy",
        argv=argv,
        affected_tables=(),
        affected_files=files,
    )
    bronze = _packet(
        packet_id="registry-bronze",
        action="production_write",
        argv=argv,
        affected_tables=module.BRONZE_TABLES,
        affected_files=files,
    )
    journal = ApprovalJournal(journal_path)
    for path, packet in ((paid_path, paid), (bronze_path, bronze)):
        _write_packet(path, packet)
        _issue_approved(journal, packet)
    return {
        "cycle_id": cycle_id,
        "journal": journal,
        "journal_path": journal_path,
        "paid_path": paid_path,
        "paid": paid,
        "bronze_path": bronze_path,
        "bronze": bronze,
        "promotion_path": promotion_path,
    }


def _manifest(
    cycle_id: str,
    *,
    promotable: bool = True,
    fetched_at: str | None = "2026-07-11T18:30:00+00:00",
) -> tuple[dict, str]:
    scopes = [
        {
            "competition_id": "GB1",
            "edition_id": "2025",
            "scope_id": "tm-scope-gb1",
        }
    ]
    value = {
        "status": "success",
        "dry_run": False,
        "cycle_id": cycle_id,
        "snapshot_id": SNAPSHOT_ID,
        "snapshot_hash": "1" * 64,
        "page_count": 2,
        "source_body_hashes": ["2" * 64, "3" * 64],
        "rows": {"competitions": 2, "competition_editions": 3},
        "hashes": {
            "competitions": "4" * 64,
            "competition_editions": "5" * 64,
            "crawl_scopes": registry_publish.stable_hash(scopes),
        },
        "classification_counts": (
            {"eligible": 1, "excluded": 1}
            if promotable
            else {"eligible": 1, "unknown": 1}
        ),
        "blocked_competition_ids": [] if promotable else ["UNK"],
        "promotable": promotable,
        "crawl_scope_count": len(scopes),
        "crawl_scopes": scopes,
        "writes": [
            {
                "table": "iceberg.bronze.transfermarkt_competitions",
                "rows": 2,
            },
            {
                "table": ("iceberg.bronze.transfermarkt_competition_editions"),
                "rows": 3,
            },
        ],
    }
    if fetched_at is not None:
        value["fetched_at"] = fetched_at
    return value, registry_publish.stable_hash(value)


class _Ti:
    def __init__(self, result):
        self.result = result

    def xcom_pull(self, *, task_ids):
        assert task_ids == "discover_registry"
        return self.result


def _persist_discovery(
    module,
    paths,
    *,
    promotable=True,
    run_id=RUN_ID,
    fetched_at="2026-07-11T18:30:00+00:00",
):
    cycle_id = module._cycle_id(run_id)
    manifest, manifest_hash = _manifest(
        cycle_id, promotable=promotable, fetched_at=fetched_at
    )
    manifest_dir = paths["output_root"] / cycle_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"transfermarkt-discovery-{manifest_hash}.json"
    manifest_path.write_text(
        json.dumps({"manifest_hash": manifest_hash, "manifest": manifest}),
        encoding="utf-8",
    )
    result = json.dumps(
        {
            "status": "success",
            "manifest_path": str(manifest_path),
            "manifest_hash": manifest_hash,
        }
    )
    return manifest, manifest_hash, manifest_path, _Ti(result)


def test_dag_is_monthly_single_run_and_proxy_task_is_serialized(dag_module):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    assert dag_module.dag.dag_id == "dag_discover_transfermarkt_registry"
    assert dag_module.dag.schedule == "0 2 1 * *"
    assert dag_module.dag._dag_kwargs["max_active_runs"] == 1
    assert dag_module.dag._dag_kwargs["catchup"] is False

    bash = next(
        task for task in BashOperator._instances if task.task_id == "discover_registry"
    )
    assert bash._init_kwargs["pool"] == "transfermarkt_proxy"
    assert bash._init_kwargs["pool_slots"] == 1
    assert bash._init_kwargs["max_active_tis_per_dag"] == 1
    assert bash._init_kwargs["retries"] == 0
    assert bash._init_kwargs["do_xcom_push"] is True
    assert bash.append_env is True
    assert "run_transfermarkt_discovery.py" in bash.bash_command
    assert "--paid-proxy-approval-packet" in bash.bash_command
    assert "--production-write-approval-packet" in bash.bash_command
    assert "--approval-journal" in bash.bash_command
    assert "curl " not in bash.bash_command
    assert "--dry-run" not in bash.bash_command

    python = {
        task.task_id: task
        for task in PythonOperator._instances
        if task.task_id in {"prepare_discovery", "publish_registry"}
    }
    assert set(python) == {"prepare_discovery", "publish_registry"}
    assert all(task._init_kwargs["retries"] == 0 for task in python.values())


def test_prepare_requires_two_exact_approved_packets_without_consuming_them(
    dag_module,
    monkeypatch,
    tmp_path,
):
    paths = _configure_paths(dag_module, monkeypatch, tmp_path)
    approvals = _discovery_approvals(dag_module, paths)

    env = dag_module._prepare_discovery(
        paid_proxy_packet_path=str(approvals["paid_path"]),
        paid_proxy_packet_hash=approvals["paid"].packet_hash,
        bronze_write_packet_path=str(approvals["bronze_path"]),
        bronze_write_packet_hash=approvals["bronze"].packet_hash,
        promotion_write_packet_path=str(approvals["promotion_path"]),
        approval_journal=str(approvals["journal_path"]),
        dag_run=SimpleNamespace(run_id=RUN_ID),
    )

    assert env["TM_CYCLE_ID"] == approvals["cycle_id"]
    assert env["TM_PROXY_CONTROL_URL"] == "http://proxy_filter:8899"
    assert env["TM_REQUIRE_METERED_PROXY"] == "true"
    assert int(env["TM_REQUEST_LIMIT"]) == 1024
    assert int(env["TM_RETRY_LIMIT"]) == 96
    assert approvals["journal"].get(approvals["paid"].packet_hash).status == "approved"
    assert (
        approvals["journal"].get(approvals["bronze"].packet_hash).status == "approved"
    )


def test_prepare_blocks_stale_hash_and_direct_source_url_before_proxy(
    dag_module,
    monkeypatch,
    tmp_path,
):
    paths = _configure_paths(dag_module, monkeypatch, tmp_path)
    approvals = _discovery_approvals(dag_module, paths)
    kwargs = {
        "paid_proxy_packet_path": str(approvals["paid_path"]),
        "paid_proxy_packet_hash": "0" * 64,
        "bronze_write_packet_path": str(approvals["bronze_path"]),
        "bronze_write_packet_hash": approvals["bronze"].packet_hash,
        "promotion_write_packet_path": str(approvals["promotion_path"]),
        "approval_journal": str(approvals["journal_path"]),
        "dag_run": SimpleNamespace(run_id=RUN_ID),
    }
    with pytest.raises(Exception, match="presented discovery approval hash drift"):
        dag_module._prepare_discovery(**kwargs)

    monkeypatch.setenv("TM_PROXY_CONTROL_URL", "https://www.transfermarkt.com")
    kwargs["paid_proxy_packet_hash"] = approvals["paid"].packet_hash
    with pytest.raises(Exception, match="cannot be a source URL"):
        dag_module._prepare_discovery(**kwargs)


def test_unknown_classification_blocks_before_third_packet_or_connection(
    dag_module,
    monkeypatch,
    tmp_path,
):
    paths = _configure_paths(dag_module, monkeypatch, tmp_path)
    _, _, _, ti = _persist_discovery(dag_module, paths, promotable=False)
    opened = False

    def connect():
        nonlocal opened
        opened = True
        raise AssertionError("connection opened before manifest gate")

    with pytest.raises(Exception, match="not promotable"):
        dag_module._publish_registry(
            expected_revision=5,
            promotion_write_packet_path=str(
                paths["approval_root"] / "missing-promotion.json"
            ),
            approval_journal=str(paths["approval_root"] / "journal.json"),
            connection_factory=connect,
            dag_run=SimpleNamespace(run_id=RUN_ID),
            ti=ti,
        )
    assert opened is False


def test_promotion_consumes_exact_third_packet_before_opening_trino_and_persists(
    dag_module,
    monkeypatch,
    tmp_path,
):
    paths = _configure_paths(dag_module, monkeypatch, tmp_path)
    manifest, manifest_hash, manifest_path, ti = _persist_discovery(
        dag_module,
        paths,
    )
    planned = registry_publish.publish_registry(
        manifest,
        manifest_hash=manifest_hash,
        snapshot_id=SNAPSHOT_ID,
        competition_count=2,
        edition_count=3,
        expected_revision=5,
        apply=False,
    )
    publication_path = dag_module._publication_manifest_path(
        cycle_id=dag_module._cycle_id(RUN_ID),
        registry_manifest_hash=planned.plan.registry_manifest_hash,
    )
    journal_path = paths["approval_root"] / "journal.json"
    packet_path = paths["approval_root"] / "promotion.json"
    argv = dag_module._promotion_argv(
        run_id=RUN_ID,
        cycle_id=dag_module._cycle_id(RUN_ID),
        expected_revision=5,
        manifest_hash=manifest_hash,
        registry_manifest_hash=planned.plan.registry_manifest_hash,
    )
    tables = tuple(
        sorted(
            {
                registry_publish.COMPETITIONS_TABLE,
                registry_publish.EDITIONS_TABLE,
                registry_publish.REGISTRY_STATE_TABLE,
                *(table for _, table in planned.plan.staging_tables),
            }
        )
    )
    packet = _packet(
        packet_id="registry-promotion",
        action="production_write",
        argv=argv,
        affected_tables=tables,
        affected_files=tuple(
            sorted(
                (
                    str(journal_path.resolve()),
                    str(manifest_path.resolve()),
                    str(publication_path),
                )
            )
        ),
    )
    _write_packet(packet_path, packet)
    journal = ApprovalJournal(journal_path)
    _issue_approved(journal, packet)

    class Connection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    connection = Connection()

    def connect():
        assert journal.get(packet.packet_hash).status == "consumed"
        return connection

    applied = registry_publish.RegistryPublicationResult(
        plan=planned.plan,
        applied=True,
        dq=(("target.unknown_active_count", 0),),
    )
    calls = []

    def publish(*args, **kwargs):
        calls.append(kwargs)
        return applied if kwargs.get("apply") else planned

    result = dag_module._publish_registry(
        expected_revision=5,
        promotion_write_packet_path=str(packet_path),
        promotion_write_packet_hash=packet.packet_hash,
        approval_journal=str(journal_path),
        connection_factory=connect,
        publisher=publish,
        dag_run=SimpleNamespace(run_id=RUN_ID),
        ti=ti,
    )

    assert [call["apply"] for call in calls] == [False, False, True]
    assert result["status"] == "success"
    assert result["registry_snapshot_id"] == SNAPSHOT_ID
    assert result["registry_revision"] == 6
    assert result["publication_manifest_path"] == str(publication_path)
    assert publication_path.exists()
    persisted = json.loads(publication_path.read_text(encoding="utf-8"))
    assert persisted["manifest"]["promotion_approval_packet_hash"] == packet.packet_hash
    assert persisted["manifest"]["publication"]["applied"] is True
    assert journal.get(packet.packet_hash).status == "consumed"
    assert connection.closed is True


def test_missing_promotion_packet_cannot_open_trino(
    dag_module,
    monkeypatch,
    tmp_path,
):
    paths = _configure_paths(dag_module, monkeypatch, tmp_path)
    _, _, _, ti = _persist_discovery(dag_module, paths)
    opened = False

    def connect():
        nonlocal opened
        opened = True

    with pytest.raises(Exception, match="approval packet is invalid"):
        dag_module._publish_registry(
            expected_revision=5,
            promotion_write_packet_path=str(paths["approval_root"] / "missing.json"),
            approval_journal=str(paths["approval_root"] / "journal.json"),
            connection_factory=connect,
            dag_run=SimpleNamespace(run_id=RUN_ID),
            ti=ti,
        )
    assert opened is False


def _write_registry_policy(tmp_path: Path, **overrides) -> tuple[Path, str]:
    from dags.utils.transfermarkt_approval import load_standing_policy

    value = {
        "policy_version": 1,
        "dag_id": "dag_discover_transfermarkt_registry",
        "approved_by": "sergeykuznetsov1995",
        "approved_at": "2026-07-14T00:00:00Z",
        "expires_at": "2100-01-01T00:00:00Z",
        "paid_proxy": {
            "byte_cap_bytes": 15 * 1024 * 1024,
            "request_limit": 1024,
            "retry_limit": 96,
            "concurrency": 1,
        },
        "production_write": {
            "byte_cap_bytes": 0,
            "request_limit": 0,
            "retry_limit": 0,
            "concurrency": 1,
        },
        "allowed_write_tables": [
            "iceberg.bronze.transfermarkt_competition_editions",
            "iceberg.bronze.transfermarkt_competitions",
            "iceberg.ops.transfermarkt_registry_state_v2",
            "iceberg.silver.transfermarkt_competition_editions_v2",
            "iceberg.silver.transfermarkt_competitions_v2",
        ],
    }
    value.update(overrides)
    path = tmp_path / "standing_registry_policy.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path, load_standing_policy(path).policy_hash


class _StandingTi:
    def __init__(self, discovery_result, prepared):
        self.values = {
            "discover_registry": discovery_result,
            "prepare_discovery": prepared,
        }

    def xcom_pull(self, *, task_ids):
        return self.values[task_ids]


class _StandingConnection:
    def __init__(self, revision_rows):
        self.revision_rows = revision_rows
        self.executed: list[str] = []
        self.closed = False

    def cursor(self):
        connection = self

        class Cursor:
            def execute(self, sql):
                connection.executed.append(sql)

            def fetchall(self):
                return connection.revision_rows

            def close(self):
                pass

        return Cursor()

    def close(self):
        self.closed = True


def test_bash_command_switches_on_approval_mode(dag_module):
    from airflow.operators.bash import BashOperator

    bash = next(
        task for task in BashOperator._instances
        if task.task_id == "discover_registry"
    )
    command = bash.bash_command
    assert 'case "$TM_APPROVAL_MODE" in' in command
    standing = command.split("standing_policy)")[1].split(";;")[0]
    one_shot = command.split("one_shot)")[1].split(";;")[0]
    fallback = command.split("*)")[1].split(";;")[0]
    assert '--standing-policy "$TM_STANDING_POLICY_PATH"' in standing
    assert '--standing-policy-sha256 "$TM_STANDING_POLICY_SHA256"' in standing
    assert "approval-packet" not in standing
    assert "--approval-journal" not in standing
    assert '--paid-proxy-approval-packet "$TM_PAID_PACKET"' in one_shot
    assert '--production-write-approval-packet "$TM_BRONZE_PACKET"' in one_shot
    assert '--approval-journal "$TM_APPROVAL_JOURNAL"' in one_shot
    assert "--standing-policy" not in one_shot
    assert "exit 1" in fallback


def test_every_rendered_flag_exists_in_discovery_parser(dag_module):
    from airflow.operators.bash import BashOperator

    from dags.scripts import run_transfermarkt_discovery as runner

    bash = next(
        task for task in BashOperator._instances
        if task.task_id == "discover_registry"
    )
    rendered_flags = set(
        re.findall(r"(?m)^\s*(--[a-z0-9-]+)\b", bash.bash_command)
    )
    parser_flags = set(runner._parser()._option_string_actions)
    assert {"--standing-policy", "--standing-policy-sha256"} <= rendered_flags
    assert rendered_flags <= parser_flags


class TestStandingRegistryDiscovery:
    def _arm(self, module, monkeypatch, tmp_path, **overrides):
        paths = _configure_paths(module, monkeypatch, tmp_path)
        monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
        policy_path, policy_hash = _write_registry_policy(tmp_path, **overrides)
        monkeypatch.setattr(module, "STANDING_POLICY_PATH", str(policy_path))
        return paths, policy_path, policy_hash

    def _prepare_kwargs(self, module, paths, *, run_type="scheduled"):
        return {
            "paid_proxy_packet_path": "",
            "paid_proxy_packet_hash": "",
            "bronze_write_packet_path": "",
            "bronze_write_packet_hash": "",
            "promotion_write_packet_path": str(
                paths["approval_root"] / "registry-promotion.json"
            ),
            "approval_journal": str(paths["approval_root"] / "journal.json"),
            "dag_run": SimpleNamespace(
                run_id=SCHEDULED_RUN_ID, run_type=run_type
            ),
        }

    def test_scheduled_prepare_uses_the_standing_policy(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, policy_path, policy_hash = self._arm(
            dag_module, monkeypatch, tmp_path
        )

        env = dag_module._prepare_discovery(
            **self._prepare_kwargs(dag_module, paths)
        )

        assert env["TM_APPROVAL_MODE"] == "standing_policy"
        assert env["TM_STANDING_POLICY_PATH"] == str(policy_path)
        assert env["TM_STANDING_POLICY_SHA256"] == policy_hash
        assert env["TM_CYCLE_ID"] == dag_module._cycle_id(SCHEDULED_RUN_ID)
        assert int(env["TM_REQUEST_LIMIT"]) == 1024
        assert int(env["TM_RETRY_LIMIT"]) == 96
        assert env["TM_REQUIRE_METERED_PROXY"] == "true"
        assert "TM_PAID_PACKET" not in env
        assert "TM_BRONZE_PACKET" not in env
        assert "TM_APPROVAL_JOURNAL" not in env
        assert "TM_PAID_APPROVAL_PRESENTED_HASH" not in env
        assert "TM_WRITE_APPROVAL_PRESENTED_HASH" not in env
        assert not (paths["approval_root"] / "journal.json").exists()

    def test_gate_off_scheduled_prepare_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(dag_module, monkeypatch, tmp_path)
        monkeypatch.delenv("TM_STANDING_POLICY_ENABLED", raising=False)

        with pytest.raises(Exception, match="must be an absolute path"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )

    def test_manual_trigger_keeps_the_one_shot_ritual(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(dag_module, monkeypatch, tmp_path)

        with pytest.raises(Exception, match="must be an absolute path"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths, run_type="manual")
            )

    def test_explicit_packets_take_precedence_over_standing_policy(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(dag_module, monkeypatch, tmp_path)
        approvals = _discovery_approvals(
            dag_module, paths, run_id=SCHEDULED_RUN_ID
        )

        env = dag_module._prepare_discovery(
            paid_proxy_packet_path=str(approvals["paid_path"]),
            paid_proxy_packet_hash=approvals["paid"].packet_hash,
            bronze_write_packet_path=str(approvals["bronze_path"]),
            bronze_write_packet_hash=approvals["bronze"].packet_hash,
            promotion_write_packet_path=str(approvals["promotion_path"]),
            approval_journal=str(approvals["journal_path"]),
            dag_run=SimpleNamespace(
                run_id=SCHEDULED_RUN_ID, run_type="scheduled"
            ),
        )

        assert env["TM_APPROVAL_MODE"] == "one_shot"
        assert env["TM_PAID_PACKET"] == str(approvals["paid_path"])
        assert "TM_STANDING_POLICY_PATH" not in env
        assert "TM_STANDING_POLICY_SHA256" not in env

    @pytest.mark.parametrize(
        "paid_override",
        [
            {"byte_cap_bytes": 16 * 1024 * 1024},
            {"request_limit": 710},
            {"retry_limit": 400},
            {"concurrency": 2},
        ],
    )
    def test_standing_policy_caps_must_equal_discovery_limits(
        self, dag_module, monkeypatch, tmp_path, paid_override
    ):
        paid = {
            "byte_cap_bytes": 15 * 1024 * 1024,
            "request_limit": 1024,
            "retry_limit": 96,
            "concurrency": 1,
        }
        paid.update(paid_override)
        paths, _, _ = self._arm(
            dag_module, monkeypatch, tmp_path, paid_proxy=paid
        )

        with pytest.raises(
            Exception, match="caps differ from discovery limits"
        ):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )

    def test_standing_policy_wrong_dag_id_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(
            dag_module, monkeypatch, tmp_path,
            dag_id="dag_ingest_transfermarkt",
        )

        with pytest.raises(Exception, match="dag_id mismatch"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )

    def test_standing_policy_expired_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(
            dag_module, monkeypatch, tmp_path,
            approved_at="2026-01-01T00:00:00Z",
            expires_at="2026-02-01T00:00:00Z",
        )

        with pytest.raises(Exception, match="expired"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )

    def test_standing_policy_missing_file_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(dag_module, monkeypatch, tmp_path)
        monkeypatch.setattr(
            dag_module, "STANDING_POLICY_PATH", str(tmp_path / "absent.json")
        )

        with pytest.raises(Exception, match="unreadable"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )

    def test_standing_policy_missing_bronze_table_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths, _, _ = self._arm(
            dag_module, monkeypatch, tmp_path,
            allowed_write_tables=["iceberg.bronze.transfermarkt_competitions"],
        )

        with pytest.raises(Exception, match="omits write tables"):
            dag_module._prepare_discovery(
                **self._prepare_kwargs(dag_module, paths)
            )


class TestStandingRegistryPublication:
    def _arm(self, module, monkeypatch, tmp_path, **overrides):
        paths = _configure_paths(module, monkeypatch, tmp_path)
        monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
        policy_path, policy_hash = _write_registry_policy(tmp_path, **overrides)
        monkeypatch.setattr(module, "STANDING_POLICY_PATH", str(policy_path))
        manifest, manifest_hash, manifest_path, ti = _persist_discovery(
            module, paths, run_id=SCHEDULED_RUN_ID
        )
        return {
            "paths": paths,
            "policy_hash": policy_hash,
            "manifest": manifest,
            "manifest_hash": manifest_hash,
            "manifest_path": manifest_path,
            "discovery_result": ti.result,
        }

    def _publish(self, module, armed, *, connection, publisher,
                 prepared=None, expected_revision=0):
        prepared = prepared if prepared is not None else {
            "TM_APPROVAL_MODE": "standing_policy",
            "TM_STANDING_POLICY_SHA256": armed["policy_hash"],
        }
        return module._publish_registry(
            expected_revision=expected_revision,
            promotion_write_packet_path=str(
                armed["paths"]["approval_root"] / "registry-promotion.json"
            ),
            promotion_write_packet_hash="",
            approval_journal=str(
                armed["paths"]["approval_root"] / "journal.json"
            ),
            connection_factory=lambda: connection,
            publisher=publisher,
            dag_run=SimpleNamespace(
                run_id=SCHEDULED_RUN_ID, run_type="scheduled"
            ),
            ti=_StandingTi(armed["discovery_result"], prepared),
        )

    def test_standing_publication_reads_live_revision_and_swaps(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=False,
        )
        applied = registry_publish.RegistryPublicationResult(
            plan=planned.plan,
            applied=True,
            dq=(("target.unknown_active_count", 0),),
        )
        # promoted_at is naive timestamp(6) in the live table; the helper
        # must read it as UTC and accept a manifest captured after it.
        connection = _StandingConnection([(5, datetime(2026, 7, 1, 8, 0))])
        calls = []

        def publish(*args, **kwargs):
            calls.append(kwargs)
            if kwargs.get("apply"):
                assert kwargs["connection"] is connection
                return applied
            return planned

        result = self._publish(
            dag_module, armed, connection=connection, publisher=publish
        )

        assert [call.get("apply") for call in calls] == [False, False, True]
        assert [call["expected_revision"] for call in calls] == [0, 5, 5]
        assert connection.executed and "state_key = 'canonical'" in (
            connection.executed[0]
        )
        assert connection.closed is True
        assert result["status"] == "success"
        assert result["registry_revision"] == 6
        publication_path = Path(result["publication_manifest_path"])
        assert publication_path.exists()
        persisted = json.loads(publication_path.read_text(encoding="utf-8"))
        evidence = persisted["manifest"]
        assert evidence["approval_mode"] == "standing_policy"
        assert evidence["promotion_approval_packet_hash"] is None
        assert evidence["standing_policy"] == {
            "policy_hash": armed["policy_hash"],
            "policy_version": 1,
        }
        assert not (
            armed["paths"]["approval_root"] / "journal.json"
        ).exists()

    def test_concurrent_revision_change_fails_the_cas_swap(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=False,
        )
        connection = _StandingConnection(
            [(5, datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc))]
        )

        def publish(*args, **kwargs):
            if kwargs.get("apply"):
                # The canonical revision moved between the read and the swap:
                # the CAS MERGE matched nothing and the readback failed.
                raise registry_publish.RegistryCasError(
                    "registry CAS readback mismatch"
                )
            return planned

        with pytest.raises(
            registry_publish.RegistryCasError, match="readback mismatch"
        ):
            self._publish(
                dag_module, armed, connection=connection, publisher=publish
            )

        assert connection.closed is True
        cycle_dir = armed["paths"]["output_root"] / dag_module._cycle_id(
            SCHEDULED_RUN_ID
        )
        assert not list(cycle_dir.glob("transfermarkt-registry-publish-*"))

    def test_policy_drift_between_plan_and_publication_fails_closed(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        opened = False

        def connect():
            nonlocal opened
            opened = True

        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=0,
            apply=False,
        )

        with pytest.raises(Exception, match="drifted between plan"):
            dag_module._publish_registry(
                expected_revision=0,
                promotion_write_packet_path=str(
                    armed["paths"]["approval_root"] / "registry-promotion.json"
                ),
                promotion_write_packet_hash="",
                approval_journal=str(
                    armed["paths"]["approval_root"] / "journal.json"
                ),
                connection_factory=connect,
                publisher=lambda *a, **kw: planned,
                dag_run=SimpleNamespace(
                    run_id=SCHEDULED_RUN_ID, run_type="scheduled"
                ),
                ti=_StandingTi(
                    armed["discovery_result"],
                    {
                        "TM_APPROVAL_MODE": "standing_policy",
                        "TM_STANDING_POLICY_SHA256": "0" * 64,
                    },
                ),
            )
        assert opened is False

    def test_standing_publication_requires_a_standing_prepare(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=0,
            apply=False,
        )

        with pytest.raises(
            Exception, match="requires a standing prepare_discovery run"
        ):
            self._publish(
                dag_module,
                armed,
                connection=_StandingConnection([]),
                publisher=lambda *a, **kw: planned,
                prepared={"TM_APPROVAL_MODE": "one_shot"},
            )

    def test_standing_publication_requires_zero_revision_param(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=False,
        )

        with pytest.raises(Exception, match="must stay 0"):
            self._publish(
                dag_module,
                armed,
                connection=_StandingConnection([]),
                publisher=lambda *a, **kw: planned,
                expected_revision=5,
            )

    def test_policy_without_publication_tables_fails_before_the_swap(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(
            dag_module, monkeypatch, tmp_path,
            allowed_write_tables=[
                "iceberg.bronze.transfermarkt_competition_editions",
                "iceberg.bronze.transfermarkt_competitions",
            ],
        )
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=0,
            apply=False,
        )
        applied_calls = []

        def publish(*args, **kwargs):
            if kwargs.get("apply"):
                applied_calls.append(kwargs)
            return planned

        with pytest.raises(Exception, match="omits publication tables"):
            self._publish(
                dag_module,
                armed,
                connection=_StandingConnection([]),
                publisher=publish,
            )
        assert applied_calls == []

    def test_applied_plan_hash_mismatch_fails_loudly(
        self, dag_module, monkeypatch, tmp_path
    ):
        # apply=True re-renders the plan from disk; an SQL edit between the
        # table validation and the apply must not produce success evidence.
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=5,
            apply=False,
        )
        applied = registry_publish.RegistryPublicationResult(
            plan=dataclasses.replace(
                planned.plan, registry_manifest_hash="f" * 64
            ),
            applied=True,
            dq=(("target.unknown_active_count", 0),),
        )
        connection = _StandingConnection(
            [(5, datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc))]
        )

        def publish(*args, **kwargs):
            return applied if kwargs.get("apply") else planned

        with pytest.raises(
            Exception, match="plan changed between validation and apply"
        ):
            self._publish(
                dag_module, armed, connection=connection, publisher=publish
            )

        assert connection.closed is True
        cycle_dir = armed["paths"]["output_root"] / dag_module._cycle_id(
            SCHEDULED_RUN_ID
        )
        assert not list(cycle_dir.glob("transfermarkt-registry-publish-*"))

    def test_stale_manifest_replay_is_rejected_before_cas(
        self, dag_module, monkeypatch, tmp_path
    ):
        # Clearing publish_registry on an OLD scheduled run replays its old
        # manifest from XCom; the live promoted_at being newer than that
        # manifest's fetched_at must stop the republication before any CAS.
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        connection = _StandingConnection(
            [(7, datetime(2026, 7, 13, 8, 43, 33, tzinfo=timezone.utc))]
        )
        calls = []

        def publish(*args, **kwargs):
            calls.append(kwargs)
            return registry_publish.publish_registry(*args, **kwargs)

        with pytest.raises(Exception, match="stale discovery manifest"):
            self._publish(
                dag_module, armed, connection=connection, publisher=publish
            )

        # Only the caller's side-effect-free manifest gate ran; the standing
        # helper stopped before planning against the live revision.
        assert [call.get("apply") for call in calls] == [False]
        assert connection.closed is True
        cycle_dir = armed["paths"]["output_root"] / dag_module._cycle_id(
            SCHEDULED_RUN_ID
        )
        assert not list(cycle_dir.glob("transfermarkt-registry-publish-*"))

    def test_manifest_without_fetched_at_anchor_is_rejected(
        self, dag_module, monkeypatch, tmp_path
    ):
        paths = _configure_paths(dag_module, monkeypatch, tmp_path)
        monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
        policy_path, policy_hash = _write_registry_policy(tmp_path)
        monkeypatch.setattr(
            dag_module, "STANDING_POLICY_PATH", str(policy_path)
        )
        manifest, manifest_hash, _, ti = _persist_discovery(
            dag_module, paths, run_id=SCHEDULED_RUN_ID, fetched_at=None
        )
        opened = False

        def connect():
            nonlocal opened
            opened = True

        planned = registry_publish.publish_registry(
            manifest,
            manifest_hash=manifest_hash,
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=0,
            apply=False,
        )

        with pytest.raises(Exception, match="fetched_at anchor"):
            dag_module._publish_registry(
                expected_revision=0,
                promotion_write_packet_path=str(
                    paths["approval_root"] / "registry-promotion.json"
                ),
                promotion_write_packet_hash="",
                approval_journal=str(paths["approval_root"] / "journal.json"),
                connection_factory=connect,
                publisher=lambda *a, **kw: planned,
                dag_run=SimpleNamespace(
                    run_id=SCHEDULED_RUN_ID, run_type="scheduled"
                ),
                ti=_StandingTi(
                    ti.result,
                    {
                        "TM_APPROVAL_MODE": "standing_policy",
                        "TM_STANDING_POLICY_SHA256": policy_hash,
                    },
                ),
            )
        assert opened is False

    def test_staging_table_must_derive_from_a_silver_target(
        self, dag_module, monkeypatch, tmp_path
    ):
        # The ops state table is in the policy, but a __publish_ suffix does
        # not make it a legitimate staging base — only the Silver targets are.
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        planned = registry_publish.publish_registry(
            armed["manifest"],
            manifest_hash=armed["manifest_hash"],
            snapshot_id=SNAPSHOT_ID,
            competition_count=2,
            edition_count=3,
            expected_revision=0,
            apply=False,
        )
        tampered = registry_publish.RegistryPublicationResult(
            plan=dataclasses.replace(
                planned.plan,
                staging_tables=tuple(
                    (
                        entity,
                        registry_publish.REGISTRY_STATE_TABLE
                        + "__publish_"
                        + "a" * 16,
                    )
                    for entity, _ in planned.plan.staging_tables
                ),
            ),
            applied=False,
        )
        applied_calls = []

        def publish(*args, **kwargs):
            if kwargs.get("apply"):
                applied_calls.append(kwargs)
            return tampered

        with pytest.raises(
            Exception, match="not derived from an allowed table"
        ):
            self._publish(
                dag_module,
                armed,
                connection=_StandingConnection([]),
                publisher=publish,
            )
        assert applied_calls == []

    def test_gate_off_scheduled_publication_keeps_the_third_packet(
        self, dag_module, monkeypatch, tmp_path
    ):
        armed = self._arm(dag_module, monkeypatch, tmp_path)
        monkeypatch.delenv("TM_STANDING_POLICY_ENABLED", raising=False)
        opened = False

        def connect():
            nonlocal opened
            opened = True

        with pytest.raises(Exception, match="approval packet is invalid"):
            dag_module._publish_registry(
                expected_revision=0,
                promotion_write_packet_path=str(
                    armed["paths"]["approval_root"] / "missing.json"
                ),
                promotion_write_packet_hash="",
                approval_journal=str(
                    armed["paths"]["approval_root"] / "journal.json"
                ),
                connection_factory=connect,
                dag_run=SimpleNamespace(
                    run_id=SCHEDULED_RUN_ID, run_type="scheduled"
                ),
                ti=_StandingTi(armed["discovery_result"], None),
            )
        assert opened is False
