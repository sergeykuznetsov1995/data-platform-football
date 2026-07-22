"""Contracts for inventory-driven WhoScored raw backup and restore checks."""

from __future__ import annotations

import json
import stat
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.whoscored.raw_store import RawStoreError, WhoScoredRawStore
from scripts.whoscored_raw_backup import (
    backup_inventory_object_descriptors,
    backup_object_key,
    backup_inventory,
    build_inventory,
    estimate_cutover_capacity,
    execute_restore_drill,
    fetch_backup_inventory,
    inventory_marker_key,
    list_backup_inventories,
    load_inventory,
    main,
    measure_store_metadata,
    open_store,
    revalidate_restore_drill_backup,
    restore_inventory,
    restore_drill_receipt_key,
    validate_backup_configuration,
    validate_inventory,
    validate_distinct_store_roots,
    verify_backup_store,
    verify_s3_compliance_retention,
    verify_store,
    write_inventory,
)


ROOT = Path(__file__).resolve().parents[3]


class _ConcurrencyProbe:
    def __init__(self, delay: float = 0.02):
        self._delay = delay
        self._lock = threading.Lock()
        self.active = 0
        self.maximum = 0
        self.started = 0

    def run(self, operation, *args):
        with self._lock:
            self.active += 1
            self.started += 1
            self.maximum = max(self.maximum, self.active)
        try:
            time.sleep(self._delay)
            return operation(*args)
        finally:
            with self._lock:
                self.active -= 1


class _S3Control:
    def __init__(
        self,
        sizes: dict[str, int],
        *,
        versioning: str = "Enabled",
        lock_enabled: str = "Enabled",
        mode: str = "COMPLIANCE",
        retain_until: datetime | None = None,
    ) -> None:
        self.sizes = sizes
        self.versioning = versioning
        self.lock_enabled = lock_enabled
        self.mode = mode
        self.retain_until = retain_until or (
            datetime.now(timezone.utc) + timedelta(days=2)
        )
        self.head_keys: list[str] = []

    def get_bucket_versioning(self, *, Bucket: str):
        assert Bucket
        return {"Status": self.versioning}

    def get_object_lock_configuration(self, *, Bucket: str):
        assert Bucket
        return {
            "ObjectLockConfiguration": {
                "ObjectLockEnabled": self.lock_enabled,
                "Rule": {
                    "DefaultRetention": {
                        "Mode": self.mode,
                        "Days": 30,
                    }
                },
            }
        }

    def head_object(self, *, Bucket: str, Key: str):
        assert Bucket
        self.head_keys.append(Key)
        if Key not in self.sizes:
            raise RuntimeError("NoSuchKey")
        return {
            "ContentLength": self.sizes[Key],
            "VersionId": "version-1",
            "ObjectLockMode": self.mode,
            "ObjectLockRetainUntilDate": self.retain_until,
        }


def _add_unique_objects(store: WhoScoredRawStore, count: int) -> None:
    for index in range(count):
        store._write_immutable_bytes(
            f"bulk/{index:03d}.raw",
            f"payload-{index:03d}".encode(),
        )


def test_versioned_recovery_reference_gates_backup_enablement():
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )

    assert "whoscored_raw_backup.py restore-drill" in runbook
    assert "whoscored_raw_backup.py list-inventories" in runbook
    assert "authenticates both exact inventory markers" in runbook
    assert "airflow pools set whoscored_storage_pool 1" in runbook
    restore_block = runbook.split("whoscored_raw_backup.py restore-drill", 1)[1].split(
        "The command authenticates", 1
    )[0]
    assert '--backup-uri "$WHOSCORED_BACKUP_DESTINATION_URI"' in restore_block
    assert "--store-uri" not in restore_block
    assert '--raw-source-uri "$WHOSCORED_RAW_STORE_URI"' in restore_block
    assert '--ops-source-uri "$WHOSCORED_OPS_STORE_URI"' in restore_block
    assert "--evidence-output" in restore_block
    assert "--apply --create-buckets" in restore_block
    assert "--entrypoint bash" in runbook
    assert "airflow-scheduler -euc" in runbook
    assert '--raw-inventory-key "$RAW_RECOVERY_INVENTORY_KEY"' in runbook
    assert '--ops-inventory-key "$OPS_RECOVERY_INVENTORY_KEY"' in runbook
    assert "WHOSCORED_BACKUP_RPO_HOURS" in runbook
    assert "WHOSCORED_BACKUP_RTO_HOURS" in runbook
    assert "WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH" in runbook
    assert "WHOSCORED_OPS_STORE_URI" in runbook
    assert "airflow dags unpause dag_backup_whoscored_storage" in runbook
    assert "airflow dags unpause dag_ingest_whoscored" in runbook


@pytest.mark.unit
@pytest.mark.parametrize(
    "role,prefix",
    [
        ("source", "WHOSCORED_BACKUP_SOURCE_S3_"),
        ("destination", "WHOSCORED_BACKUP_DESTINATION_S3_"),
        ("restore", "WHOSCORED_BACKUP_RESTORE_S3_"),
    ],
)
def test_s3_backup_roles_reject_partial_credentials(monkeypatch, role, prefix):
    for candidate in (
        "WHOSCORED_BACKUP_SOURCE_S3_",
        "WHOSCORED_BACKUP_DESTINATION_S3_",
        "WHOSCORED_BACKUP_RESTORE_S3_",
        "WHOSCORED_RAW_S3_",
        "S3_",
    ):
        monkeypatch.delenv(f"{candidate}ACCESS_KEY", raising=False)
        monkeypatch.delenv(f"{candidate}SECRET_KEY", raising=False)
    monkeypatch.setenv(f"{prefix}ACCESS_KEY", "only-one-half")

    with pytest.raises(ValueError, match="must be set together"):
        open_store("s3://bucket/prefix", role=role)


@pytest.mark.unit
def test_source_backup_uses_complete_raw_writer_pair_as_fallback(monkeypatch):
    captured = {}
    sentinel = object()

    for prefix in (
        "WHOSCORED_BACKUP_SOURCE_S3_",
        "WHOSCORED_RAW_S3_",
        "S3_",
    ):
        monkeypatch.delenv(f"{prefix}ACCESS_KEY", raising=False)
        monkeypatch.delenv(f"{prefix}SECRET_KEY", raising=False)
    monkeypatch.setenv("WHOSCORED_RAW_S3_ACCESS_KEY", "raw-access")
    monkeypatch.setenv("WHOSCORED_RAW_S3_SECRET_KEY", "raw-secret")

    def _filesystem(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr("scripts.whoscored_raw_backup.fs.S3FileSystem", _filesystem)
    store = open_store("s3://warehouse/raw/whoscored", role="source")

    assert store.filesystem is sentinel
    assert captured["access_key"] == "raw-access"
    assert captured["secret_key"] == "raw-secret"


def _source_store(tmp_path) -> WhoScoredRawStore:
    store = WhoScoredRawStore.from_uri((tmp_path / "source").as_uri())
    store._write_immutable_bytes("blobs/example-a.raw.gz", b"first")
    store._write_immutable_json(
        "target-history-v2/match/example/receipt.json",
        {"kind": "test-receipt", "blob": "blobs/example-a.raw.gz"},
    )
    return store


@pytest.mark.unit
def test_preflight_proves_current_append_and_reports_source_metadata(
    tmp_path, monkeypatch
):
    source = _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    destination_uri = (tmp_path / "destination").as_uri()
    monkeypatch.setattr(
        "scripts.whoscored_raw_backup.validate_distinct_store_roots",
        lambda *args, **kwargs: None,
    )

    first = validate_backup_configuration(source_uri, destination_uri, workers=2)
    second = validate_backup_configuration(source_uri, destination_uri, workers=2)
    metadata = measure_store_metadata(source)

    assert first["passed"] is True
    assert first["workers"] == 2
    assert first["destination_probe"] != second["destination_probe"]
    destination = WhoScoredRawStore.from_uri(destination_uri)
    assert destination._read_bytes(first["destination_probe"]) == b""
    assert destination._read_bytes(second["destination_probe"]) == b""
    assert metadata["object_count"] == 2
    assert metadata["total_bytes"] > 0
    assert metadata["snapshot_started_at"] <= metadata["snapshot_completed_at"]


@pytest.mark.unit
def test_inventory_backup_and_restore_verification_round_trip(tmp_path):
    source = _source_store(tmp_path)
    destination = WhoScoredRawStore.from_uri((tmp_path / "destination").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())

    dry_run = backup_inventory(source, destination, inventory, apply=False)

    assert dry_run["applied"] is False
    assert dry_run["would_copy_objects"] == inventory["object_count"]
    assert not (tmp_path / "destination").exists()

    applied = backup_inventory(source, destination, inventory, apply=True)
    verified = verify_store(destination, inventory)
    backup_verified = verify_backup_store(destination, inventory, require_marker=True)

    assert applied["copied_objects"] == inventory["object_count"]
    assert applied["inventory_key"].startswith("backup-inventories/")
    assert verified["passed"] is False
    assert backup_verified["passed"] is True
    assert backup_verified["missing"] == []
    assert backup_verified["corrupt"] == []
    assert not destination._exists(inventory["objects"][0]["path"])

    restored = WhoScoredRawStore.from_uri((tmp_path / "restored").as_uri())
    restore = restore_inventory(destination, restored, inventory, apply=True)
    assert restore["copied_objects"] == inventory["object_count"]
    assert verify_store(restored, inventory)["passed"] is True


@pytest.mark.unit
def test_off_host_inventory_is_listable_fetchable_and_cli_idempotent(tmp_path, capsys):
    source = _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    backup_uri = (tmp_path / "backup").as_uri()
    backup = WhoScoredRawStore.from_uri(backup_uri)
    inventory = build_inventory(source, source_uri=source_uri, workers=1)
    backup_inventory(source, backup, inventory, apply=True, workers=1)
    marker = inventory_marker_key(inventory)

    listed = list_backup_inventories(
        backup,
        expected_source_uri=source_uri,
    )
    fetched = fetch_backup_inventory(
        backup,
        marker,
        expected_source_uri=source_uri,
    )

    assert listed["passed"] is True
    assert listed["inventory_count"] == 1
    assert listed["inventories"][0]["inventory_key"] == marker
    assert fetched == inventory
    with pytest.raises(ValueError, match="does not match expected source"):
        fetch_backup_inventory(
            backup,
            marker,
            expected_source_uri=(tmp_path / "ops").as_uri(),
        )

    output = tmp_path / "recovery" / "inventory.json"
    fetch_args = [
        "fetch-inventory",
        "--store-uri",
        backup_uri,
        "--inventory-key",
        marker,
        "--expected-source-uri",
        source_uri,
        "--output",
        str(output),
    ]
    assert (
        main(
            [
                "list-inventories",
                "--store-uri",
                backup_uri,
                "--expected-source-uri",
                source_uri,
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["inventory_count"] == 1
    assert main(fetch_args) == 0
    assert (
        json.loads(capsys.readouterr().out)["inventory_sha256"]
        == inventory["inventory_sha256"]
    )
    assert main(fetch_args) == 0
    capsys.readouterr()
    assert load_inventory(output) == inventory


@pytest.mark.unit
def test_missing_ops_prefix_has_a_valid_empty_backup_contract(tmp_path):
    source_uri = (tmp_path / "missing-ops-prefix").as_uri()
    source = WhoScoredRawStore.from_uri(source_uri)
    destination = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())

    with pytest.raises(RawStoreError, match="Source inventory is empty"):
        build_inventory(source, source_uri=source_uri, workers=1)
    inventory = build_inventory(
        source,
        source_uri=source_uri,
        workers=1,
        allow_empty=True,
    )
    applied = backup_inventory(
        source,
        destination,
        inventory,
        apply=True,
        workers=1,
    )

    assert inventory["object_count"] == 0
    assert inventory["total_bytes"] == 0
    assert applied["copied_objects"] == 0
    assert (
        verify_backup_store(
            destination,
            inventory,
            require_marker=True,
            workers=1,
        )["passed"]
        is True
    )
    assert (
        list_backup_inventories(
            destination,
            expected_source_uri=source_uri,
        )["inventory_count"]
        == 1
    )


@pytest.mark.unit
def test_fetch_inventory_rejects_forged_marker_key(tmp_path):
    store = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())

    with pytest.raises(ValueError, match="exact backup-inventories marker"):
        fetch_backup_inventory(
            store,
            "../inventory.json",
            expected_source_uri=(tmp_path / "source").as_uri(),
        )


@pytest.mark.unit
def test_restore_rejects_nonempty_unrelated_destination(tmp_path):
    source = _source_store(tmp_path)
    backup = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    restored = WhoScoredRawStore.from_uri((tmp_path / "restored").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    backup_inventory(source, backup, inventory, apply=True)
    restored._write_immutable_bytes("unrelated/object", b"do-not-overwrite")

    with pytest.raises(RawStoreError, match="outside the inventory"):
        restore_inventory(backup, restored, inventory, apply=True)


@pytest.mark.unit
def test_restore_drill_generates_private_off_host_bound_raw_and_ops_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import whoscored_raw_backup as raw_backup

    raw_source_uri = "s3://football/raw/whoscored"
    ops_source_uri = "s3://football/ops/whoscored"
    backup_uri = "s3://remote-backup/whoscored"
    raw_restore_uri = "s3://recovery-raw/whoscored"
    ops_restore_uri = "s3://recovery-ops/whoscored"
    raw_source = WhoScoredRawStore.from_uri((tmp_path / "raw-source").as_uri())
    ops_source = WhoScoredRawStore.from_uri((tmp_path / "ops-source").as_uri())
    raw_source._write_immutable_bytes("raw/a", b"raw-a")
    raw_source._write_immutable_bytes("raw/b", b"raw-b")
    ops_source._write_immutable_bytes("ops/a", b"ops-a")
    backup = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    raw_restore = WhoScoredRawStore.from_uri((tmp_path / "raw-restore").as_uri())
    ops_restore = WhoScoredRawStore.from_uri((tmp_path / "ops-restore").as_uri())
    raw_inventory = build_inventory(raw_source, source_uri=raw_source_uri)
    ops_inventory = build_inventory(ops_source, source_uri=ops_source_uri)
    raw_backup_result = backup_inventory(raw_source, backup, raw_inventory, apply=True)
    ops_backup_result = backup_inventory(ops_source, backup, ops_inventory, apply=True)
    stores = {
        backup_uri: backup,
        raw_restore_uri: raw_restore,
        ops_restore_uri: ops_restore,
    }

    def opener(uri: str, **_kwargs):
        return stores[uri]

    for name, value in {
        "WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT": "primary.internal:8333",
        "WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT": "backup.example:443",
        "WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT": "recovery.example:443",
        "WHOSCORED_BACKUP_SOURCE_SITE_ID": "primary-site",
        "WHOSCORED_BACKUP_DESTINATION_SITE_ID": "backup-site",
        "WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE": "object-lock",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH", raising=False)
    release = {
        "parser_version": "whoscored-parser-v8",
        "manifest_sha256": "a" * 64,
        "code_tree_sha256": "b" * 64,
    }
    monkeypatch.setattr(
        raw_backup._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: release,
    )
    clock_start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(
        seconds=1
    )
    clock = iter((clock_start, clock_start + timedelta(minutes=1)))
    monkeypatch.setattr(raw_backup, "_restore_drill_utc_now", lambda: next(clock))
    monkeypatch.setattr(
        raw_backup,
        "revalidate_restore_drill_backup",
        lambda **_kwargs: {"status": "passed"},
    )
    evidence_path = (tmp_path / "restore-drill-evidence.json").absolute()

    result = execute_restore_drill(
        backup_uri=backup_uri,
        raw_source_uri=raw_source_uri,
        raw_inventory_key=raw_backup_result["inventory_key"],
        raw_restore_uri=raw_restore_uri,
        ops_source_uri=ops_source_uri,
        ops_inventory_key=ops_backup_result["inventory_key"],
        ops_restore_uri=ops_restore_uri,
        evidence_output=evidence_path,
        apply=True,
        workers=2,
        store_opener=opener,
    )

    evidence = json.loads(evidence_path.read_text(encoding="ascii"))
    proof = {
        name: value for name, value in evidence.items() if name != "off_host_receipt"
    }
    expected_key, expected_sha256 = restore_drill_receipt_key(proof)
    assert result["passed"] is True
    assert evidence["schema_version"] == 2
    assert evidence["off_host_receipt"] == {
        "key": expected_key,
        "sha256": expected_sha256,
    }
    assert backup._read_json(expected_key) == proof
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert evidence_path.read_bytes() == json.dumps(
        evidence,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    assert all(source["already_present_objects"] == 0 for source in evidence["sources"])
    assert all(source["exact_tree_match"] is True for source in evidence["sources"])

    exact_descriptors = [
        *backup_inventory_object_descriptors(raw_inventory),
        *backup_inventory_object_descriptors(ops_inventory),
        {
            "key": expected_key,
            "bytes": len(WhoScoredRawStore._render_json(proof)),
        },
    ]
    sizes = {
        f"whoscored/{item['key']}": int(item["bytes"]) for item in exact_descriptors
    }
    client = _S3Control(
        sizes,
        retain_until=clock_start + timedelta(hours=25),
    )
    live = revalidate_restore_drill_backup(
        backup_uri=backup_uri,
        evidence=evidence,
        backup_store=backup,
        s3_control_client=client,
        now=clock_start,
        workers=2,
    )
    assert live["status"] == "passed"
    assert live["capability"]["checked_object_count"] == len(sizes)
    assert live["expected_retained_objects"] == len(sizes)
    assert len(live["inventories"]) == 2
    assert all(
        item["checked_content_objects"] == item["expected_content_objects"]
        and item["checked_bytes"] == item["expected_content_bytes"]
        for item in live["inventories"]
    )

    deleted_key = backup_object_key(raw_inventory["objects"][0]["sha256"])
    backup.filesystem.delete_file(backup._path(deleted_key))
    with pytest.raises(RawStoreError, match="live off-host backup object verification"):
        revalidate_restore_drill_backup(
            backup_uri=backup_uri,
            evidence=evidence,
            backup_store=backup,
            s3_control_client=client,
            now=clock_start,
            workers=2,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw_restore_uri", "ops_restore_uri"),
    (
        ("s3://football/ops/whoscored/drill", "s3://recovery-ops/whoscored"),
        ("s3://recovery-raw/whoscored", "s3://football/raw/whoscored/drill"),
    ),
)
def test_restore_drill_rejects_cross_plane_target_before_open_or_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw_restore_uri: str,
    ops_restore_uri: str,
) -> None:
    for name, value in {
        "WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT": "primary.internal:8333",
        "WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT": "backup.example:443",
        "WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT": "recovery.example:443",
        "WHOSCORED_BACKUP_SOURCE_SITE_ID": "primary-site",
        "WHOSCORED_BACKUP_DESTINATION_SITE_ID": "backup-site",
        "WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE": "object-lock",
    }.items():
        monkeypatch.setenv(name, value)
    opened = False

    def opener(*_args, **_kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("unsafe restore preflight opened a store")

    with pytest.raises(ValueError, match="must not overlap either source"):
        execute_restore_drill(
            backup_uri="s3://remote-backup/whoscored",
            raw_source_uri="s3://football/raw/whoscored",
            raw_inventory_key="backup-inventories/not-reached.json",
            raw_restore_uri=raw_restore_uri,
            ops_source_uri="s3://football/ops/whoscored",
            ops_inventory_key="backup-inventories/not-reached.json",
            ops_restore_uri=ops_restore_uri,
            evidence_output=(tmp_path / "evidence.json").absolute(),
            apply=True,
            store_opener=opener,
        )

    assert opened is False


@pytest.mark.unit
def test_restore_refuses_content_without_completion_marker(tmp_path):
    source = _source_store(tmp_path)
    backup = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    restored = WhoScoredRawStore.from_uri((tmp_path / "restored").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    for item in inventory["objects"]:
        backup._write_immutable_bytes(
            backup_object_key(item["sha256"]),
            source._read_bytes(item["path"]),
        )

    with pytest.raises(RawStoreError, match="marker-verified"):
        restore_inventory(backup, restored, inventory, apply=True)
    assert not (tmp_path / "restored").exists()


@pytest.mark.unit
def test_backup_is_idempotent_and_rejects_immutable_collision(tmp_path):
    source = _source_store(tmp_path)
    destination = WhoScoredRawStore.from_uri((tmp_path / "destination").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    backup_inventory(source, destination, inventory, apply=True)

    repeated = backup_inventory(source, destination, inventory, apply=True)

    assert repeated["copied_objects"] == 0
    assert repeated["already_present_objects"] == inventory["object_count"]

    collision = backup_object_key(inventory["objects"][0]["sha256"])
    destination._write_bytes(collision, b"different")
    with pytest.raises(RawStoreError, match="Immutable backup collision"):
        backup_inventory(source, destination, inventory, apply=True)


@pytest.mark.unit
def test_backup_deduplicates_equal_payloads_without_overlapping_counts(tmp_path):
    source = _source_store(tmp_path)
    source._write_immutable_bytes("duplicate/example-a.raw.gz", b"first")
    destination = WhoScoredRawStore.from_uri((tmp_path / "destination").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())

    first = backup_inventory(source, destination, inventory, apply=True)
    second = backup_inventory(source, destination, inventory, apply=True)
    unique_hashes = len({item["sha256"] for item in inventory["objects"]})

    assert first["copied_objects"] == unique_hashes
    assert first["already_present_objects"] == 0
    assert second["copied_objects"] == 0
    assert second["already_present_objects"] == unique_hashes


@pytest.mark.unit
def test_backup_marker_is_published_only_after_verification(tmp_path, monkeypatch):
    source = _source_store(tmp_path)
    destination = WhoScoredRawStore.from_uri((tmp_path / "destination").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    monkeypatch.setattr(
        "scripts.whoscored_raw_backup.verify_backup_store",
        lambda *_args, **_kwargs: {"passed": False},
    )

    with pytest.raises(RawStoreError, match="verification failed"):
        backup_inventory(source, destination, inventory, apply=True)

    assert not list((tmp_path / "destination").rglob("backup-inventories/*.json"))


@pytest.mark.unit
def test_committed_backup_verification_requires_inventory_marker(tmp_path):
    source = _source_store(tmp_path)
    destination = WhoScoredRawStore.from_uri((tmp_path / "destination").as_uri())
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    for item in inventory["objects"]:
        destination._write_immutable_bytes(
            backup_object_key(item["sha256"]),
            source._read_bytes(item["path"]),
        )

    content_only = verify_backup_store(destination, inventory)
    committed = verify_backup_store(destination, inventory, require_marker=True)

    assert content_only["passed"] is True
    assert committed["passed"] is False
    assert committed["marker_present"] is False


@pytest.mark.unit
def test_verify_restore_reports_missing_and_corrupt_objects(tmp_path):
    source = _source_store(tmp_path)
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    destination = WhoScoredRawStore.from_uri((tmp_path / "restored").as_uri())
    for item in inventory["objects"]:
        destination._write_immutable_bytes(
            item["path"], source._read_bytes(item["path"])
        )
    first, second = [item["path"] for item in inventory["objects"]]
    destination.filesystem.delete_file(destination._path(first))
    destination._write_bytes(second, b"corrupt")

    report = verify_store(destination, inventory)

    assert report["passed"] is False
    assert report["missing"] == [first]
    assert report["corrupt"] == [second]


@pytest.mark.unit
def test_backup_rejects_equal_or_nested_store_roots(tmp_path):
    source = (tmp_path / "raw").as_uri()

    with pytest.raises(ValueError, match="equal or nested"):
        validate_distinct_store_roots(source, source + "/backup")
    with pytest.raises(ValueError, match="equal or nested"):
        validate_distinct_store_roots(source + "/child", source)

    validate_distinct_store_roots(
        source,
        (tmp_path / "off-host-fixture").as_uri(),
    )


@pytest.mark.unit
def test_production_backup_requires_distinct_endpoint_bucket_and_object_lock(
    monkeypatch,
):
    monkeypatch.setenv("WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT", "seaweedfs:8333")
    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT", "seaweedfs:8333")
    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", "object-lock")

    with pytest.raises(ValueError, match="distinct S3 endpoint and bucket"):
        validate_distinct_store_roots(
            "s3://warehouse/raw",
            "s3://other-bucket/backup",
            require_off_host=True,
        )

    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT", "backup.example:443")
    validate_distinct_store_roots(
        "s3://warehouse/raw",
        "s3://other-bucket/backup",
        require_off_host=True,
    )
    with pytest.raises(ValueError, match="distinct S3 endpoint and bucket"):
        validate_distinct_store_roots(
            "s3://warehouse/raw",
            "s3://warehouse/backup",
            require_off_host=True,
        )
    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", "versioned-worm")
    with pytest.raises(ValueError, match="provider-verified object-lock"):
        validate_distinct_store_roots(
            "s3://warehouse/raw",
            "s3://other-bucket/backup",
            require_off_host=True,
        )


@pytest.mark.unit
def test_s3_compliance_retention_checks_every_exact_object(monkeypatch):
    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", "object-lock")
    now = datetime(2026, 7, 22, 10, 0, tzinfo=timezone.utc)
    client = _S3Control(
        {"whoscored/a.raw": 3, "whoscored/marker.json": 7},
        retain_until=now + timedelta(hours=25),
    )

    report = verify_s3_compliance_retention(
        "s3://off-host-backup/whoscored",
        ({"key": "a.raw", "bytes": 3}, {"key": "marker.json", "bytes": 7}),
        client=client,
        now=now,
        workers=2,
    )

    assert report["status"] == "passed"
    assert report["checked_object_count"] == 2
    assert client.head_keys == ["whoscored/a.raw", "whoscored/marker.json"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("versioning", "versioning and COMPLIANCE"),
        ("lock", "versioning and COMPLIANCE"),
        ("governance", "versioning and COMPLIANCE"),
        ("short", "exact versioned COMPLIANCE"),
        ("missing", "unavailable"),
    ),
)
def test_s3_compliance_retention_fails_closed(
    monkeypatch: pytest.MonkeyPatch, mutation: str, message: str
) -> None:
    monkeypatch.setenv("WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", "object-lock")
    now = datetime(2026, 7, 22, 10, 0, tzinfo=timezone.utc)
    sizes = {} if mutation == "missing" else {"whoscored/a.raw": 3}
    client = _S3Control(
        sizes,
        versioning="Suspended" if mutation == "versioning" else "Enabled",
        lock_enabled="Disabled" if mutation == "lock" else "Enabled",
        mode="GOVERNANCE" if mutation == "governance" else "COMPLIANCE",
        retain_until=(
            now + timedelta(hours=23)
            if mutation == "short"
            else now + timedelta(hours=25)
        ),
    )

    with pytest.raises(RawStoreError, match=message):
        verify_s3_compliance_retention(
            "s3://off-host-backup/whoscored",
            ({"key": "a.raw", "bytes": 3},),
            client=client,
            now=now,
        )


@pytest.mark.unit
def test_inventory_file_has_a_tamper_evident_object_list(tmp_path):
    source = _source_store(tmp_path)
    inventory = build_inventory(source, source_uri=(tmp_path / "source").as_uri())
    output = tmp_path / "inventory.json"

    write_inventory(output, inventory)

    assert load_inventory(output) == inventory
    tampered = json.loads(output.read_text(encoding="utf-8"))
    tampered["objects"][0]["path"] = "../escaped"
    with pytest.raises(ValueError, match="Invalid or duplicate"):
        validate_inventory(tampered)


@pytest.mark.unit
def test_cli_inventory_and_verify_restore_exit_status(tmp_path, capsys):
    _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    inventory_path = tmp_path / "inventory.json"

    assert (
        main(["inventory", "--store-uri", source_uri, "--output", str(inventory_path)])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert (
        main(
            [
                "verify-restore",
                "--store-uri",
                source_uri,
                "--inventory",
                str(inventory_path),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["passed"] is True


@pytest.mark.unit
def test_inventory_hashing_is_bounded_and_deterministic(tmp_path, monkeypatch):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 18)
    original_read = source._read_bytes
    probe = _ConcurrencyProbe()
    monkeypatch.setattr(
        source,
        "_read_bytes",
        lambda path: probe.run(original_read, path),
    )

    parallel = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=3,
    )
    serial = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )

    assert 1 < probe.maximum <= 3
    assert parallel["objects"] == serial["objects"]
    assert parallel["objects_sha256"] == serial["objects_sha256"]
    assert [item["path"] for item in parallel["objects"]] == sorted(
        item["path"] for item in parallel["objects"]
    )


@pytest.mark.unit
def test_inventory_failure_stops_submitting_after_bounded_inflight_set(
    tmp_path, monkeypatch
):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 50)
    probe = _ConcurrencyProbe(delay=0.01)

    def fail_read(path):
        return probe.run(
            lambda _path: (_ for _ in ()).throw(RawStoreError("read failed")),
            path,
        )

    monkeypatch.setattr(source, "_read_bytes", fail_read)

    with pytest.raises(RawStoreError, match="read failed"):
        build_inventory(
            source,
            source_uri=(tmp_path / "source").as_uri(),
            workers=4,
        )

    assert probe.started <= 4
    assert probe.active == 0


@pytest.mark.unit
def test_backup_deduplicates_before_bounded_parallel_copy(tmp_path, monkeypatch):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 16)
    source._write_immutable_bytes("duplicate/first.raw", b"payload-000")
    inventory = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )
    destination = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    original_read = source._read_bytes
    probe = _ConcurrencyProbe()
    monkeypatch.setattr(
        source,
        "_read_bytes",
        lambda path: probe.run(original_read, path),
    )

    report = backup_inventory(
        source,
        destination,
        inventory,
        apply=False,
        workers=4,
    )

    unique_hashes = {item["sha256"] for item in inventory["objects"]}
    assert 1 < probe.maximum <= 4
    assert report["would_copy_objects"] == len(unique_hashes)
    assert report["checked_source_bytes"] == inventory["total_bytes"]
    assert report["elapsed_seconds"] >= 0
    assert report["objects_per_second"] >= 0
    assert report["mib_per_second"] >= 0


@pytest.mark.unit
def test_backup_source_failure_never_publishes_completion_marker(tmp_path, monkeypatch):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 12)
    inventory = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )
    destination = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    source._write_bytes(inventory["objects"][0]["path"], b"changed")

    with pytest.raises(RawStoreError, match="Source changed after inventory"):
        backup_inventory(
            source,
            destination,
            inventory,
            apply=True,
            workers=3,
        )

    marker_root = tmp_path / "backup" / "backup-inventories"
    assert not marker_root.exists() or not list(marker_root.rglob("*.json"))


@pytest.mark.unit
def test_backup_verification_reads_unique_content_with_bounded_workers(
    tmp_path, monkeypatch
):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 15)
    source._write_immutable_bytes("duplicate/again.raw", b"payload-002")
    inventory = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )
    backup = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    backup_inventory(source, backup, inventory, apply=True, workers=2)
    original_read = backup._read_bytes
    probe = _ConcurrencyProbe()
    monkeypatch.setattr(
        backup,
        "_read_bytes",
        lambda path: probe.run(original_read, path),
    )

    report = verify_backup_store(
        backup,
        inventory,
        require_marker=True,
        workers=3,
    )

    assert report["passed"] is True
    assert 1 < probe.maximum <= 3
    assert report["expected_content_objects"] == len(
        {item["sha256"] for item in inventory["objects"]}
    )
    assert report["checked_content_objects"] == report["expected_content_objects"]
    assert report["checked_bytes"] == report["expected_content_bytes"]


@pytest.mark.unit
def test_verify_reports_are_sorted_despite_out_of_order_completion(
    tmp_path, monkeypatch
):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 10)
    inventory = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )
    missing = [inventory["objects"][1]["path"], inventory["objects"][7]["path"]]
    corrupt = [inventory["objects"][3]["path"], inventory["objects"][8]["path"]]
    for path in missing:
        source.filesystem.delete_file(source._path(path))
    for path in corrupt:
        source._write_bytes(path, b"corrupt")
    original_read = source._read_bytes

    def delayed_read(path):
        numeric = sum(path.encode()) % 7
        time.sleep((7 - numeric) * 0.002)
        return original_read(path)

    monkeypatch.setattr(source, "_read_bytes", delayed_read)

    report = verify_store(source, inventory, workers=4)

    assert report["passed"] is False
    assert report["missing"] == sorted(missing)
    assert report["corrupt"] == sorted(corrupt)


@pytest.mark.unit
def test_restore_writes_are_bounded_and_readback_verified(tmp_path, monkeypatch):
    source = _source_store(tmp_path)
    _add_unique_objects(source, 14)
    inventory = build_inventory(
        source,
        source_uri=(tmp_path / "source").as_uri(),
        workers=1,
    )
    backup = WhoScoredRawStore.from_uri((tmp_path / "backup").as_uri())
    backup_inventory(source, backup, inventory, apply=True, workers=3)
    restored = WhoScoredRawStore.from_uri((tmp_path / "restored").as_uri())
    original_write = restored._write_immutable_bytes
    probe = _ConcurrencyProbe()
    monkeypatch.setattr(
        restored,
        "_write_immutable_bytes",
        lambda path, payload: probe.run(original_write, path, payload),
    )

    report = restore_inventory(
        backup,
        restored,
        inventory,
        apply=True,
        workers=3,
    )

    assert 1 < probe.maximum <= 3
    assert report["copied_objects"] == inventory["object_count"]
    assert verify_store(restored, inventory, workers=3)["passed"] is True


@pytest.mark.unit
def test_cli_workers_override_and_environment_are_validated(
    tmp_path, capsys, monkeypatch
):
    _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    inventory_path = tmp_path / "inventory.json"
    monkeypatch.setenv("WHOSCORED_BACKUP_WORKERS", "2")

    assert (
        main(
            [
                "inventory",
                "--store-uri",
                source_uri,
                "--output",
                str(inventory_path),
                "--workers",
                "5",
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["workers"] == 5

    monkeypatch.setenv("WHOSCORED_BACKUP_WORKERS", "65")
    with pytest.raises(SystemExit):
        main(
            [
                "verify-restore",
                "--store-uri",
                source_uri,
                "--inventory",
                str(inventory_path),
            ]
        )


@pytest.mark.unit
def test_capacity_check_gates_cutover_downtime(tmp_path, capsys):
    source = _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    inventory = build_inventory(
        source,
        source_uri=source_uri,
        workers=1,
    )
    created_at = datetime.fromisoformat(inventory["created_at"].replace("Z", "+00:00"))
    checked_at = created_at + timedelta(minutes=1)

    accepted = estimate_cutover_capacity(
        inventory,
        expected_source_uri=source_uri,
        max_inventory_age_hours=24,
        inventory_mib_per_second=10,
        backup_mib_per_second=10,
        verify_mib_per_second=10,
        fixed_cutover_overhead_seconds=30,
        max_downtime_seconds=60,
        now=checked_at,
    )
    rejected = estimate_cutover_capacity(
        inventory,
        expected_source_uri=source_uri,
        max_inventory_age_hours=24,
        inventory_mib_per_second=10,
        backup_mib_per_second=10,
        verify_mib_per_second=10,
        fixed_cutover_overhead_seconds=30,
        max_downtime_seconds=20,
        now=checked_at,
    )

    assert accepted["passed"] is True
    assert accepted["estimated_downtime_seconds"] >= 30
    assert rejected["passed"] is False
    assert rejected["headroom_seconds"] < 0
    inventory_path = tmp_path / "capacity-inventory.json"
    write_inventory(inventory_path, inventory)
    common_args = [
        "capacity-check",
        "--inventory",
        str(inventory_path),
        "--expected-source-uri",
        source_uri,
        "--current-store-uri",
        source_uri,
        "--max-inventory-age-hours",
        "24",
        "--inventory-mib-per-second",
        "10",
        "--backup-mib-per-second",
        "10",
        "--verify-mib-per-second",
        "10",
        "--fixed-cutover-overhead-seconds",
        "30",
    ]
    assert main([*common_args, "--max-downtime-seconds", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert main([*common_args, "--max-downtime-seconds", "20"]) == 2
    assert json.loads(capsys.readouterr().out)["passed"] is False
    with pytest.raises(ValueError, match="finite positive"):
        estimate_cutover_capacity(
            inventory,
            expected_source_uri=source_uri,
            max_inventory_age_hours=24,
            inventory_mib_per_second=0,
            backup_mib_per_second=10,
            verify_mib_per_second=10,
            fixed_cutover_overhead_seconds=0,
            max_downtime_seconds=60,
            now=checked_at,
        )


@pytest.mark.unit
def test_capacity_check_scales_for_current_object_and_byte_drift(tmp_path):
    source = _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    inventory = build_inventory(source, source_uri=source_uri, workers=1)
    checked_at = datetime.fromisoformat(inventory["created_at"].replace("Z", "+00:00"))
    args = {
        "expected_source_uri": source_uri,
        "max_inventory_age_hours": 24,
        "inventory_mib_per_second": 1,
        "backup_mib_per_second": 1,
        "verify_mib_per_second": 1,
        "fixed_cutover_overhead_seconds": 0,
        "max_downtime_seconds": 60,
        "now": checked_at,
    }

    baseline = estimate_cutover_capacity(inventory, **args)
    object_drift = estimate_cutover_capacity(
        inventory,
        current_object_count=inventory["object_count"] * 5,
        current_total_bytes=inventory["total_bytes"],
        **args,
    )
    byte_drift = estimate_cutover_capacity(
        inventory,
        current_object_count=inventory["object_count"],
        current_total_bytes=inventory["total_bytes"] * 3,
        **args,
    )

    assert baseline["capacity_scale"] == 1
    assert object_drift["capacity_scale"] == 5
    assert byte_drift["capacity_scale"] == 3
    assert object_drift["inventory_seconds"] == pytest.approx(
        baseline["inventory_seconds"] * 5,
        abs=0.002,
    )


@pytest.mark.unit
def test_capacity_check_rejects_wrong_stale_and_future_inventory(tmp_path):
    source = _source_store(tmp_path)
    source_uri = (tmp_path / "source").as_uri()
    inventory = build_inventory(source, source_uri=source_uri, workers=1)
    created_at = datetime.fromisoformat(inventory["created_at"].replace("Z", "+00:00"))
    rates = {
        "inventory_mib_per_second": 10,
        "backup_mib_per_second": 10,
        "verify_mib_per_second": 10,
        "fixed_cutover_overhead_seconds": 30,
        "max_downtime_seconds": 60,
    }

    with pytest.raises(ValueError, match="does not match expected source"):
        estimate_cutover_capacity(
            inventory,
            expected_source_uri=(tmp_path / "different").as_uri(),
            max_inventory_age_hours=24,
            now=created_at,
            **rates,
        )
    with pytest.raises(ValueError, match="stale"):
        estimate_cutover_capacity(
            inventory,
            expected_source_uri=source_uri,
            max_inventory_age_hours=24,
            now=created_at + timedelta(hours=25),
            **rates,
        )
    with pytest.raises(ValueError, match="future"):
        estimate_cutover_capacity(
            inventory,
            expected_source_uri=source_uri,
            max_inventory_age_hours=24,
            now=created_at - timedelta(seconds=1),
            **rates,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        estimate_cutover_capacity(
            inventory,
            expected_source_uri=source_uri,
            max_inventory_age_hours=24,
            now=datetime.now().replace(tzinfo=None),
            **rates,
        )
