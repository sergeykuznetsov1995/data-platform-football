from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]


def _compose() -> dict:
    return yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))


def test_seaweedfs_uses_supervised_production_components() -> None:
    services = _compose()["services"]

    assert services["seaweedfs-master"]["command"][0] == "master"
    assert services["seaweedfs-volume"]["command"][0] == "volume"
    assert services["seaweedfs-filer"]["command"][0] == "filer"
    assert services["seaweedfs"]["command"][0] == "s3"
    assert "mini" not in services["seaweedfs"]["command"]
    assert services["seaweedfs"]["depends_on"]["seaweedfs-filer"]["condition"] == (
        "service_healthy"
    )
    master_command = services["seaweedfs-master"]["command"]
    assert "-mdir=/data" in master_command
    assert "-mdir=/data/m9333" not in master_command
    for name in ("seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer"):
        assert services[name]["entrypoint"] == [
            "/usr/local/bin/seaweedfs-plane-entrypoint"
        ]
        assert any(
            "seaweedfs_plane_entrypoint.sh" in mount
            for mount in services[name]["volumes"]
        )


def test_seaweedfs_planes_keep_healthchecks_and_persistent_data() -> None:
    services = _compose()["services"]
    for name in ("seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer"):
        service = services[name]
        assert "healthcheck" in service
        data_mount = next(
            mount
            for mount in service["volumes"]
            if isinstance(mount, dict) and mount.get("target") == "/data"
        )
        assert data_mount["source"] == "seaweedfs_data"
        assert data_mount["volume"]["nocopy"] is True

    assert "healthcheck" in services["seaweedfs"]
    assert services["seaweedfs"]["command"][-3:] == [
        "-allowDeleteBucketNotEmpty=false",
        "-concurrentFileUploadLimit=16",
        "-concurrentUploadLimitMB=512",
    ]
    raw = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert "${SEAWEEDFS_DATA_VOLUME_NAME:-seaweedfs_data}" in raw


def test_recovery_uses_admin_identity_not_read_only_backup_reader() -> None:
    config = yaml.safe_load(
        (ROOT / "configs" / "seaweedfs" / "s3.config.json.example").read_text(
            encoding="utf-8"
        )
    )
    identities = {item["name"]: item for item in config["identities"]}

    assert "Admin" in identities["football-platform-admin"]["actions"]
    assert "Write" in identities["football-platform-admin"]["actions"]
    assert all(
        not action.startswith("Write")
        for action in identities["whoscored-backup-reader"]["actions"]
    )


def test_fresh_airflow_deploy_creates_configured_whoscored_pools() -> None:
    command = "\n".join(_compose()["services"]["airflow-init"]["command"])

    assert (
        'airflow pools set "$${WHOSCORED_DIRECT_POOL}" '
        '"$${WHOSCORED_SOURCE_POOL_SLOTS}"' in command
    )
    assert 'airflow pools set "$${WHOSCORED_BACKFILL_POOL}"' not in command
    assert 'case "$${WHOSCORED_SOURCE_POOL_SLOTS}"' in command
    assert 'airflow pools set "$${WHOSCORED_DQ_POOL}" 2' in command
    assert (
        '"$${WHOSCORED_BACKFILL_POOL}" != "$${WHOSCORED_DIRECT_POOL}"'
        in command
    )
    assert "daily and backfill must share one source pool" in command
    assert "airflow pools set 'whoscored_storage_pool' 1" in command


def test_daily_profile_hard_cap_is_available_to_airflow_tasks() -> None:
    environment = _compose()["x-airflow-common"]["environment"]

    assert environment["WHOSCORED_DAILY_PROFILE_MAX_LIMIT"] == (
        "${WHOSCORED_DAILY_PROFILE_MAX_LIMIT:-3000}"
    )


def test_airflow_tasks_share_one_fail_fast_whoscored_source_circuit() -> None:
    environment = _compose()["x-airflow-common"]["environment"]

    assert environment["WHOSCORED_SOURCE_CIRCUIT_PATH"] == (
        "/opt/airflow/logs/whoscored/source-circuit-v1.json"
    )
    assert environment["WHOSCORED_SOURCE_CIRCUIT_WAIT"] == "0"


def test_documented_whoscored_runtime_controls_reach_airflow_tasks() -> None:
    environment = _compose()["x-airflow-common"]["environment"]
    expected_defaults = {
        "WHOSCORED_DIRECT_POOL": "whoscored_direct_pool",
        "WHOSCORED_BACKFILL_POOL": "whoscored_direct_pool",
        "WHOSCORED_DQ_POOL": "whoscored_dq_pool",
        "WHOSCORED_SOURCE_POOL_SLOTS": "2",
        "WHOSCORED_DAILY_SLO_WINDOW": "30",
        "WHOSCORED_DAILY_SLO_MIN_SAMPLES": "20",
        "WHOSCORED_DAILY_P95_LIMIT_HOURS": "4",
        "WHOSCORED_DAILY_PROFILE_MAX_LIMIT": "3000",
        "WHOSCORED_BACKFILL_REQUEST_UNITS_PER_RUN": "3000",
        "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY": "86400",
        "WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS": "3",
        "WHOSCORED_RUN_RETENTION_DAYS": "90",
        "WHOSCORED_OPS_IO_ATTEMPTS": "4",
        "WHOSCORED_OPS_RETRY_BASE_SECONDS": "0.2",
        "WHOSCORED_RAW_IO_ATTEMPTS": "4",
        "WHOSCORED_RAW_RETRY_BASE_SECONDS": "0.2",
        "WHOSCORED_RAW_LOCK_TIMEOUT_SECONDS": "55",
        "WHOSCORED_RAW_SNAPSHOT_LOCK_TIMEOUT_SECONDS": "300",
        "WHOSCORED_BACKUP_WORKERS": "16",
        "WHOSCORED_BACKUP_LOCAL_RETENTION_DAYS": "14",
        "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE": "60",
        "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS": "20000",
        "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE": "60",
    }

    example_values: dict[str, str] = {}
    for raw_line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            example_values[key] = value

    for name, default in expected_defaults.items():
        assert example_values[name] == default
        assert environment[name] == f"${{{name}:-{default}}}"


def test_proxy_control_token_is_shared_without_hardcoded_secret() -> None:
    raw = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    services = _compose()["services"]

    assert "${PROXY_FILTER_CONTROL_TOKEN:-${SOFASCORE_PROXY_CONTROL_TOKEN:-}}" in raw
    assert services["proxy_filter"]["environment"]["PROXY_FILTER_CONTROL_TOKEN"] == (
        "${PROXY_FILTER_CONTROL_TOKEN:-${SOFASCORE_PROXY_CONTROL_TOKEN:-}}"
    )
    assert services["proxy_filter"]["environment"][
        "PROXY_FILTER_ALLOW_FILE_FALLBACK"
    ] == "false"


def test_backup_secrets_are_scoped_to_localexecutor_scheduler() -> None:
    services = _compose()["services"]
    scheduler_env = services["airflow-scheduler"]["environment"]

    for name in (
        "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY",
        "WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY",
        "WHOSCORED_BACKUP_DESTINATION_S3_ACCESS_KEY",
        "WHOSCORED_BACKUP_DESTINATION_S3_SECRET_KEY",
        "WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY",
        "WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY",
    ):
        assert name in scheduler_env
        assert name not in services["airflow-webserver"]["environment"]
        assert name not in services["airflow-init"]["environment"]


def test_s3_role_credentials_are_not_independently_mixed_in_compose() -> None:
    raw = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    assert (
        "WHOSCORED_RAW_S3_ACCESS_KEY: ${WHOSCORED_RAW_S3_ACCESS_KEY:-}"
        in raw
    )
    assert (
        "WHOSCORED_RAW_S3_SECRET_KEY: ${WHOSCORED_RAW_S3_SECRET_KEY:-}"
        in raw
    )
    assert "WHOSCORED_RAW_S3_ACCESS_KEY:-${S3_ACCESS_KEY}" not in raw
    assert "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY:-${S3_ACCESS_KEY}" not in raw


def test_example_keeps_off_host_backup_disabled_until_provisioned() -> None:
    values = {}
    for raw_line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value

    assert values["WHOSCORED_BACKUP_DESTINATION_URI"] == ""
    assert values["WHOSCORED_BACKUP_DESTINATION_SITE_ID"] == ""
    assert values["WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE"] == ""
