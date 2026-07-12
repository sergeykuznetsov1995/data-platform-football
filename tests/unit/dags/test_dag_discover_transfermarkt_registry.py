from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from dags.utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
from dags.utils import transfermarkt_registry_publish as registry_publish


RUN_ID = "manual__2026-07-11T18:00:00+00:00"
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
    retries = 12 if requests else 0
    # The Bronze writer shares the discovery command's already bounded proxy
    # process, so its packet carries the same cap/attempt envelope.
    if action == "production_write" and "bronze" in packet_id:
        cap = 15 * 1024 * 1024
        requests = 1024
        retries = 12
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


def _discovery_approvals(module, paths: dict[str, Path]):
    cycle_id = module._cycle_id(RUN_ID)
    checkpoint = paths["state_root"] / "checkpoints" / f"{cycle_id}.json"
    journal_path = paths["approval_root"] / "journal.json"
    paid_path = paths["approval_root"] / "paid.json"
    bronze_path = paths["approval_root"] / "bronze.json"
    promotion_path = paths["approval_root"] / "promotion.json"
    argv = module._discovery_argv(
        cycle_id=cycle_id,
        run_id=RUN_ID,
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


def _manifest(cycle_id: str, *, promotable: bool = True) -> tuple[dict, str]:
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
    return value, registry_publish.stable_hash(value)


class _Ti:
    def __init__(self, result):
        self.result = result

    def xcom_pull(self, *, task_ids):
        assert task_ids == "discover_registry"
        return self.result


def _persist_discovery(module, paths, *, promotable=True):
    cycle_id = module._cycle_id(RUN_ID)
    manifest, manifest_hash = _manifest(cycle_id, promotable=promotable)
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
    assert int(env["TM_RETRY_LIMIT"]) == 12
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
