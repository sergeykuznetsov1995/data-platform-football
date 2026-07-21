from __future__ import annotations

from pathlib import Path
import re

import yaml


ROOT = Path(__file__).resolve().parents[3]
SEAWEEDFS_IMAGE = (
    "chrislusf/seaweedfs:4.36@sha256:"
    "800b2115c63236e8bd0e5d572dc25dd493dc2feed08b54a2a269dc0101c9d94a"
)
FLARESOLVERR_IMAGE = (
    "ghcr.io/flaresolverr/flaresolverr:v3.4.6@sha256:"
    "7962759d99d7e125e108e0f5e7f3cdbcd36161776d058d1d9b7153b92ef1af9e"
)
FLARESOLVERR_DERIVED_IMAGE = "data-platform-flaresolverr-whoscored:3.4.6"
CADDY_S3_PROXY_IMAGE = (
    "caddy:2.10-alpine@sha256:"
    "4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d"
)


def _compose() -> dict:
    return yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))


def _gateway_compose() -> dict:
    return yaml.safe_load(
        (ROOT / "deploy" / "whoscored" / "gateway.compose.yaml").read_text(
            encoding="utf-8"
        )
    )


def _volume_for_target(service: dict, target: str) -> dict | str:
    matches = []
    for volume in service.get("volumes", []):
        if isinstance(volume, dict) and volume.get("target") == target:
            matches.append(volume)
        elif isinstance(volume, str) and len(volume.split(":")) >= 2:
            if volume.split(":")[1] == target:
                matches.append(volume)
    assert len(matches) == 1
    return matches[0]


class _ComposeOverlayLoader(yaml.SafeLoader):
    pass


def _construct_override(loader: _ComposeOverlayLoader, node: yaml.Node) -> object:
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


_ComposeOverlayLoader.add_constructor("!override", _construct_override)


def _construct_reset(_loader: _ComposeOverlayLoader, _node: yaml.Node) -> None:
    return None


_ComposeOverlayLoader.add_constructor("!reset", _construct_reset)


def _supervised_overlay() -> dict:
    return yaml.load(
        (ROOT / "compose.seaweedfs-supervised.yaml").read_text(encoding="utf-8"),
        Loader=_ComposeOverlayLoader,
    )


def test_default_compose_preserves_legacy_weed_mini() -> None:
    services = _compose()["services"]

    assert services["seaweedfs"]["command"][0] == "mini"
    assert services["seaweedfs"]["entrypoint"] == [
        "/usr/local/bin/seaweedfs-legacy-entrypoint"
    ]
    assert "-dir=/data" in services["seaweedfs"]["command"]
    for flag in (
        "-s3=false",
        "-s3.port.iceberg=0",
        "-s3.iam=false",
        "-webdav=false",
        "-admin.ui=false",
        "-filer.disableDirListing=true",
        "-filer.ui.deleteDir=false",
    ):
        assert flag in services["seaweedfs"]["command"]
    assert "seaweedfs_data:/data" in services["seaweedfs"]["volumes"]
    assert services["seaweedfs"]["stop_grace_period"] == "120s"
    assert services["seaweedfs"]["networks"] == ["seaweedfs-control"]
    assert services["seaweedfs"]["deploy"]["resources"]["limits"]["memory"] == "4G"
    gateway = services["seaweedfs-s3"]
    assert gateway["command"][0] == "s3"
    assert "-filer=seaweedfs:8888" in gateway["command"]
    assert "-port.iceberg=0" in gateway["command"]
    assert "-iam=false" in gateway["command"]
    assert "seaweedfs_data:/data" not in gateway["volumes"]
    assert gateway["cap_drop"] == ["ALL"]
    assert set(gateway["networks"]) == {"seaweedfs-control"}
    assert gateway["networks"]["seaweedfs-control"]["aliases"] == [
        "seaweedfs-s3-internal"
    ]
    assert "ports" not in gateway
    proxy = services["seaweedfs-s3-proxy"]
    assert proxy["image"] == CADDY_S3_PROXY_IMAGE
    assert proxy["read_only"] is True
    assert proxy["user"] == "65534:65534"
    assert proxy["cap_drop"] == ["ALL"]
    assert proxy["cap_add"] == ["NET_BIND_SERVICE"]
    assert set(proxy["networks"]) == {"seaweedfs-control", "storage"}
    assert proxy["networks"]["storage"]["aliases"] == ["seaweedfs"]
    assert proxy["depends_on"]["seaweedfs-s3"]["condition"] == "service_healthy"
    assert all("s3.config.json" not in mount for mount in proxy["volumes"])
    assert all("/data" not in mount for mount in proxy["volumes"])
    assert _compose()["networks"]["seaweedfs-control"]["internal"] is True
    assert _compose()["volumes"]["seaweedfs_data"]["name"] == (
        "${SEAWEEDFS_DATA_VOLUME_NAME:?SEAWEEDFS_DATA_VOLUME_NAME is required}"
    )
    assert "seaweedfs-master" not in services
    assert "seaweedfs-volume" not in services
    assert "seaweedfs-filer" not in services


def test_control_network_documents_the_dedicated_host_trust_boundary() -> None:
    compose_source = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    runbook = (
        ROOT / "docs" / "operations" / "seaweedfs-topology-cutover.md"
    ).read_text(encoding="utf-8")
    normalized_runbook = " ".join(runbook.split())
    lock_source = (ROOT / "scripts" / "seaweedfs_lifecycle_lock.sh").read_text(
        encoding="utf-8"
    )

    assert "does NOT block host-to-container-IP traffic" in compose_source
    assert "single-tenant-host prerequisite" in runbook
    assert "every operating-system account and host" in runbook
    assert "process is a trusted storage principal" in runbook
    assert "untrusted local shell" in runbook
    assert "deployment **NO-GO**" in runbook
    assert (
        "does not claim that `internal` supplies that host boundary"
        in normalized_runbook
    )
    assert "externally enforced production preflight" in runbook
    assert "environment variable cannot waive the NO-GO" in runbook
    assert "All host accounts" in lock_source
    assert "not a hostile-host security boundary" in lock_source


def test_all_storage_lifecycle_paths_share_one_digest_pinned_image() -> None:
    paths = (
        ROOT / "compose.yaml",
        ROOT / "compose.seaweedfs-supervised.yaml",
        ROOT / "scripts" / "compose.sh",
        ROOT / "scripts" / "cutover_seaweedfs_topology.sh",
    )
    references = []
    for path in paths:
        references.extend(
            re.findall(re.escape(SEAWEEDFS_IMAGE), path.read_text(encoding="utf-8"))
        )

    assert len(references) == 7
    assert _compose()["services"]["seaweedfs"]["image"] == SEAWEEDFS_IMAGE
    assert _compose()["services"]["seaweedfs-s3"]["image"] == SEAWEEDFS_IMAGE
    overlay = _supervised_overlay()["services"]
    for name in ("seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer"):
        assert overlay[name]["image"] == SEAWEEDFS_IMAGE


def test_flaresolverr_runtime_is_digest_pinned_and_extension_is_read_only() -> None:
    service = _compose()["services"]["flaresolverr"]

    assert service["image"] == FLARESOLVERR_DERIVED_IMAGE
    assert service["build"] == {
        "context": ".",
        "dockerfile": "docker/images/flaresolverr-whoscored/Dockerfile",
    }
    assert service["read_only"] is True
    assert service["cap_add"] == []
    assert "volumes" not in service
    assert any(item.startswith("/tmp:rw,") for item in service["tmpfs"])
    assert any(item.startswith("/config:rw,") for item in service["tmpfs"])
    assert any(item.startswith("/app/.config:rw,") for item in service["tmpfs"])
    assert any(item.startswith("/app/.local:rw,") for item in service["tmpfs"])

    dockerfile = (ROOT / "docker/images/flaresolverr-whoscored/Dockerfile").read_text(
        encoding="utf-8"
    )
    entrypoint = (
        ROOT / "docker/images/flaresolverr-whoscored/entrypoint.sh"
    ).read_text(encoding="utf-8")
    dockerignore = (
        ROOT / "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore"
    ).read_text(encoding="utf-8")
    assert dockerignore == (ROOT / ".dockerignore").read_text(encoding="utf-8")
    assert f"FROM {FLARESOLVERR_IMAGE}" in dockerfile
    assert "COPY --chown=root:root scripts/flaresolverr_extended.py" in dockerfile
    assert "USER 1000:1000" in dockerfile
    assert "chmod 0555 /app" in dockerfile
    assert dockerignore.splitlines() == [
        "*",
        "!.dockerignore",
        "!docker/",
        "docker/*",
        "!docker/images/",
        "docker/images/*",
        "!docker/images/flaresolverr-whoscored/",
        "docker/images/flaresolverr-whoscored/*",
        "!docker/images/flaresolverr-whoscored/Dockerfile",
        "!docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
        "!docker/images/flaresolverr-whoscored/entrypoint.sh",
        "!scripts/",
        "scripts/*",
        "!scripts/flaresolverr_extended.py",
    ]
    assert "chromedriver.original" in dockerfile
    assert "sha256sum --check --status" in entrypoint
    assert "/usr/local/libexec/whoscored/flaresolverr_extended.py" in entrypoint


def test_routine_compose_forbids_production_gate_one_off_bypasses() -> None:
    wrapper = (ROOT / "scripts/compose.sh").read_text(encoding="utf-8")

    assert "reject_whoscored_production_one_off" in wrapper
    assert "airflow-scheduler|whoscored_proxy_filter" in wrapper
    assert "whoscored_paid_gateway" in wrapper
    assert "flaresolverr_whoscored_paid" in wrapper
    assert "Routine Compose run/exec is forbidden" in wrapper


def test_whoscored_services_use_distinct_payload_and_gated_final_targets() -> None:
    services = _compose()["services"]
    gateway_services = _gateway_compose()["services"]
    scheduler = services["airflow-scheduler"]
    paid_proxy = gateway_services["whoscored_proxy_filter"]
    paid_gateway = gateway_services["whoscored_paid_gateway"]
    shared_proxy = services["proxy_filter"]

    assert scheduler["build"]["target"] == "airflow-scheduler"
    assert scheduler["image"] == "data-platform-airflow-scheduler:2.11.2-whoscored"
    assert scheduler["cap_add"] == []
    expected_security_options = [
        "no-new-privileges:true",
        "apparmor=docker-default",
        "seccomp=builtin",
    ]
    assert scheduler["security_opt"] == expected_security_options
    assert paid_proxy["build"]["target"] == "airflow-whoscored-proxy"
    assert paid_proxy["image"] == (
        "${WHOSCORED_PROXY_IMAGE:-"
        "data-platform-airflow-whoscored-proxy:2.11.2-whoscored}"
    )
    assert paid_proxy["security_opt"] == expected_security_options
    assert paid_gateway["build"]["target"] == "airflow-whoscored-proxy"
    assert paid_gateway["image"] == paid_proxy["image"].replace(
        "WHOSCORED_PROXY_IMAGE", "WHOSCORED_GATEWAY_IMAGE"
    )
    assert paid_gateway["security_opt"] == expected_security_options
    assert services["flaresolverr"]["security_opt"] == expected_security_options
    assert gateway_services["flaresolverr_whoscored_paid"]["security_opt"] == (
        expected_security_options
    )
    assert shared_proxy["build"]["target"] == "airflow-base"

    dockerfile = (ROOT / "docker/images/airflow/Dockerfile").read_text(
        encoding="utf-8"
    )
    assert "FROM airflow-base AS airflow-scheduler-payload" in dockerfile
    assert "FROM airflow-base AS airflow-whoscored-proxy-payload" in dockerfile
    assert (
        "FROM airflow-scheduler-payload AS airflow-scheduler-test" in dockerfile
    )
    assert "FROM airflow-scheduler-payload AS airflow-scheduler" in dockerfile
    assert (
        "FROM airflow-whoscored-proxy-payload AS airflow-whoscored-proxy"
        in dockerfile
    )
    assert dockerfile.count(
        'ENTRYPOINT ["/usr/bin/dumb-init", "--", '
        '"/usr/local/bin/whoscored-production-entrypoint", "/entrypoint"]'
    ) == 2
    assert dockerfile.count("USER 50000:0") == 2
    assert dockerfile.count(
        "mv /usr/local/bin/python3.11 /usr/local/libexec/whoscored-python-real"
    ) == 2
    assert dockerfile.count(
        "/usr/local/bin/whoscored-production-python /usr/local/bin/python3.11"
    ) == 2
    assert (
        "cp --dereference /opt/legacy-scraper-venv/bin/python3.11" in dockerfile
    )


def test_opt_in_overlay_defines_supervised_components() -> None:
    services = _supervised_overlay()["services"]

    assert services["seaweedfs-master"]["command"][0] == "master"
    assert services["seaweedfs-volume"]["command"][0] == "volume"
    assert services["seaweedfs-filer"]["command"][0] == "filer"
    assert services["seaweedfs"]["command"][0] == "s3"
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

    assert services["seaweedfs-master"]["networks"] == ["seaweedfs-control"]
    assert services["seaweedfs-volume"]["networks"] == ["seaweedfs-control"]
    assert services["seaweedfs-filer"]["networks"] == ["seaweedfs-control"]
    assert services["seaweedfs"]["networks"]["seaweedfs-control"]["aliases"] == [
        "seaweedfs-s3-internal"
    ]

    raw = (ROOT / "compose.seaweedfs-supervised.yaml").read_text(encoding="utf-8")
    assert "cap_add: !override []" in raw
    assert "command: !override" in raw
    assert "volumes: !override" in raw
    assert "seaweedfs_legacy_entrypoint.sh" in raw


def test_seaweedfs_planes_keep_healthchecks_and_persistent_data() -> None:
    services = _supervised_overlay()["services"]
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
    raw = (ROOT / "compose.seaweedfs-supervised.yaml").read_text(encoding="utf-8")
    assert (
        "${SEAWEEDFS_DATA_VOLUME_NAME:?SEAWEEDFS_DATA_VOLUME_NAME is required}" in raw
    )
    for name in ("seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer"):
        assert "SEAWEEDFS_EXPECTED_INVENTORY_SHA256" in services[name]["environment"]


def test_recovery_uses_admin_identity_not_read_only_backup_reader() -> None:
    config = yaml.safe_load(
        (ROOT / "configs" / "seaweedfs" / "s3.config.json.example").read_text(
            encoding="utf-8"
        )
    )
    identities = {item["name"]: item for item in config["identities"]}

    assert identities["football-platform-admin"]["actions"] == ["Admin"]
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
    assert '"$${WHOSCORED_BACKFILL_POOL}" != "$${WHOSCORED_DIRECT_POOL}"' in command
    assert "daily and backfill must share one source pool" in command
    assert "airflow pools set 'whoscored_storage_pool' 1" in command


def test_fresh_checkout_prepares_writable_logs_before_airflow_and_proxies() -> None:
    services = {
        **_compose()["services"],
        **_gateway_compose()["services"],
    }
    initializer = services["airflow-log-init"]
    command = "\n".join(initializer["command"])

    assert initializer["user"] == "0:0"
    assert initializer["volumes"] == [
        "./logs:/opt/airflow/logs",
        "soccerdata_cache:/home/airflow/soccerdata",
    ]
    assert initializer["cap_drop"] == ["ALL"]
    assert {"CHOWN", "DAC_OVERRIDE", "FOWNER"} == set(initializer["cap_add"])
    assert "chown -R --no-dereference 50000:0 /opt/airflow/logs" in command
    assert "chmod 0770 /opt/airflow/logs" in command
    assert (
        "chown -R --no-dereference 50000:0 /home/airflow/soccerdata" in command
    )
    assert "chmod -R u+rwX,g+rwX,o-rwx /home/airflow/soccerdata" in command
    for name in ("airflow-init", "proxy_filter"):
        assert services[name]["depends_on"]["airflow-log-init"]["condition"] == (
            "service_completed_successfully"
        )
    assert "depends_on" not in _gateway_compose()["services"]["whoscored_proxy_filter"]


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


def test_paid_approval_and_pointer_roots_are_scheduler_read_only() -> None:
    compose = _compose()
    common = compose["x-airflow-common"]
    scheduler = compose["services"]["airflow-scheduler"]
    environment = scheduler["environment"]
    assert "WHOSCORED_PROXY_APPROVAL_PATH" not in common["environment"]
    assert "WHOSCORED_PROXY_APPROVAL_PATH" not in environment
    assert environment["WHOSCORED_PROXY_APPROVAL_ROOT"] == (
        "/opt/airflow/secure/whoscored-approvals"
    )
    for name in (
        "WHOSCORED_PROXY_CONTROL_TOKEN",
        "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
        "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        "WHOSCORED_PAID_ALERT_HMAC_SECRET",
        "WHOSCORED_PAID_ALERT_SECRET_PATH",
        "WHOSCORED_PAID_ALERT_BINDING_PATH",
        "WHOSCORED_PAID_ALERT_RECEIPT_ROOT",
    ):
        assert name not in common["environment"]
        assert name not in environment
        assert name not in compose["services"]["airflow-webserver"]["environment"]
    approval_mount = _volume_for_target(
        scheduler, "/opt/airflow/secure/whoscored-approvals"
    )
    assert isinstance(approval_mount, dict)
    assert approval_mount["source"].startswith("${WHOSCORED_PROXY_APPROVAL_HOST_DIR:?")
    assert approval_mount["read_only"] is True
    assert approval_mount["bind"]["create_host_path"] is False
    pointer_mount = _volume_for_target(
        scheduler, "/opt/airflow/secure/whoscored-scheduled-pointers"
    )
    assert isinstance(pointer_mount, dict)
    assert pointer_mount["source"].startswith(
        "${WHOSCORED_SCHEDULED_PAID_POINTER_HOST_DIR:?"
    )
    assert pointer_mount["read_only"] is True
    assert pointer_mount["bind"]["create_host_path"] is False
    assert environment["WHOSCORED_SCHEDULED_PAID_POINTER_ROOT"] == (
        "/opt/airflow/secure/whoscored-scheduled-pointers"
    )
    assert environment["WHOSCORED_PAID_BATCH_ENABLED"] == (
        "${WHOSCORED_PAID_BATCH_ENABLED:-0}"
    )
    for target in ("/opt/airflow/dags", "/opt/airflow/scrapers", "/opt/airflow/scripts"):
        code_mount = _volume_for_target(scheduler, target)
        assert isinstance(code_mount, dict)
        assert code_mount["read_only"] is True
        assert code_mount["bind"]["create_host_path"] is False
    for service_name in ("airflow-init", "airflow-webserver"):
        assert all(
            not isinstance(item, dict)
            or item.get("target") != "/opt/airflow/secure/whoscored-approvals"
            for item in compose["services"][service_name].get("volumes", [])
        )

    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert (
        "WHOSCORED_PROXY_APPROVAL_HOST_DIR="
        "/root/whoscored-954-runtime/proxy-approvals"
    ) in example
    assert "WHOSCORED_PROXY_APPROVAL_PATH=" not in example
    assert (
        "WHOSCORED_SCHEDULED_PAID_POINTER_HOST_DIR="
        "/root/whoscored-954-runtime/scheduled-pointers"
    ) in example
    assert "WHOSCORED_PAID_ALERT_SECRET_PATH=" not in example
    assert "WHOSCORED_PAID_ALERT_BINDING_PATH=" not in example


def test_fbref_geoip_database_is_a_scheduler_only_protected_input() -> None:
    compose = _compose()
    common = compose["x-airflow-common"]
    services = compose["services"]
    scheduler = services["airflow-scheduler"]
    expected_target = "/opt/airflow/secure/fbref-geoip/GeoLite2-City.mmdb"

    assert "PYTHONPATH" not in common["environment"]
    assert "PYTHONPATH" not in scheduler["environment"]
    assert scheduler["environment"]["FBREF_CAMOUFOX_GEOIP_DATABASE_PATH"] == (
        expected_target
    )
    mount = _volume_for_target(scheduler, expected_target)
    assert isinstance(mount, dict)
    assert mount == {
        "type": "bind",
        "source": (
            "${FBREF_CAMOUFOX_GEOIP_DATABASE_HOST_PATH:?set the protected "
            "pinned GeoLite database}"
        ),
        "target": expected_target,
        "read_only": True,
        "bind": {"create_host_path": False},
    }
    for service_name, service in services.items():
        if service_name == "airflow-scheduler":
            continue
        environment = service.get("environment") or {}
        volumes = service.get("volumes") or []
        assert "FBREF_CAMOUFOX_GEOIP_DATABASE_PATH" not in environment
        assert all(
            not isinstance(item, dict) or item.get("target") != expected_target
            for item in volumes
        )

    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert (
        "FBREF_CAMOUFOX_GEOIP_DATABASE_HOST_PATH="
        "/protected/path/fbref-geoip/GeoLite2-City.mmdb"
    ) in example


def test_proxy_control_plane_is_lease_only_and_secrets_are_not_hardcoded() -> None:
    raw = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    compose = _compose()
    services = compose["services"]
    transfermarkt_token = (
        "${TM_PROXY_CONTROL_TOKEN:-${PROXY_FILTER_CONTROL_TOKEN:-"
        "${SOFASCORE_PROXY_CONTROL_TOKEN:-}}}"
    )

    assert "${PROXY_FILTER_CONTROL_TOKEN:-${SOFASCORE_PROXY_CONTROL_TOKEN:-}}" in raw
    assert services["proxy_filter"]["environment"]["PROXY_FILTER_CONTROL_TOKEN"] == (
        "${PROXY_FILTER_CONTROL_TOKEN:-${SOFASCORE_PROXY_CONTROL_TOKEN:-}}"
    )
    assert services["proxy_filter"]["environment"]["TM_PROXY_CONTROL_TOKEN"] == (
        transfermarkt_token
    )
    assert compose["x-airflow-common"]["environment"][
        "TM_PROXY_CONTROL_TOKEN"
    ] == (
        transfermarkt_token
    )
    assert (
        services["proxy_filter"]["environment"]["PROXY_FILTER_ALLOW_FILE_FALLBACK"]
        == "false"
    )
    assert services["proxy_filter"]["environment"][
        "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH"
    ] == (
        "${WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH:-"
        "/opt/airflow/logs/proxy_filter/whoscored_campaigns.json}"
    )
    command = services["proxy_filter"]["command"]
    assert "--allow-legacy-noauth" not in command
    transfermarkt_index = command.index("--transfermarkt-dagrun-budget-bytes")
    assert command[transfermarkt_index + 1] == (
        "${PROXY_FILTER_TRANSFERMARKT_DAGRUN_BUDGET_BYTES:-0}"
    )
    index = command.index("--whoscored-campaign-ledger")
    assert command[index + 1] == (
        "${WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH:-"
        "/opt/airflow/logs/proxy_filter/whoscored_campaigns.json}"
    )
    volumes = services["proxy_filter"]["volumes"]
    assert "./dags:/opt/airflow/dags:ro" in volumes
    assert "./scripts:/opt/airflow/scripts:ro" in volumes
    assert "./scrapers:/opt/airflow/scrapers:ro" in volumes


def test_whoscored_paid_proxy_has_an_isolated_l7_application_boundary() -> None:
    compose = _compose()
    services = compose["services"]
    gateway_compose = _gateway_compose()
    gateway_services = gateway_compose["services"]
    common_airflow = compose["x-airflow-common"]["environment"]
    scheduler = services["airflow-scheduler"]["environment"]
    dedicated = gateway_services["whoscored_proxy_filter"]
    gateway = gateway_services["whoscored_paid_gateway"]
    paid_browser = gateway_services["flaresolverr_whoscored_paid"]

    assert gateway_compose["name"] == "whoscored-gw"
    assert all("profiles" not in service for service in gateway_services.values())
    assert not {
        "whoscored_proxy_filter",
        "whoscored_paid_gateway",
        "flaresolverr_whoscored_paid",
    } & services.keys()
    assert dedicated["environment"]["PROXY_POOL_JSON"] == (
        "${WHOSCORED_PROXY_POOL_JSON:-}"
    )
    assert dedicated["environment"]["PROXY_FILTER_CONTROL_TOKEN"] == (
        "${WHOSCORED_PROXY_FILTER_CONTROL_TOKEN:-}"
    )
    assert "WHOSCORED_PAID_PROXY_URL" not in common_airflow
    assert "WHOSCORED_PROXY_CONTROL_URL" not in common_airflow
    assert "WHOSCORED_PAID_GATEWAY_TOKEN" not in common_airflow
    assert "WHOSCORED_PROXY_CONTROL_TOKEN" not in common_airflow
    assert "WHOSCORED_PROXY_CONTROL_TOKEN" not in scheduler
    assert scheduler["WHOSCORED_PAID_GATEWAY_URL"] == (
        "${WHOSCORED_PAID_GATEWAY_URL:-http://whoscored_paid_gateway:8898}"
    )
    assert scheduler["WHOSCORED_PAID_GATEWAY_TOKEN"] == (
        "${WHOSCORED_PAID_GATEWAY_TOKEN:-}"
    )
    assert gateway["environment"]["WHOSCORED_PAID_GATEWAY_TOKEN"] == (
        "${WHOSCORED_PAID_GATEWAY_TOKEN:-}"
    )
    assert gateway["environment"]["WHOSCORED_PROXY_CONTROL_TOKEN"] == (
        "${WHOSCORED_PROXY_FILTER_CONTROL_TOKEN:-}"
    )
    assert gateway["environment"]["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"] == (
        "${WHOSCORED_PROXY_APPROVAL_HMAC_SECRET:-}"
    )
    assert gateway["environment"]["WHOSCORED_PAID_ALERT_HMAC_SECRET"] == (
        "${WHOSCORED_PAID_ALERT_HMAC_SECRET:-}"
    )
    assert paid_browser["environment"][
        "WHOSCORED_FLARESOLVERR_PAID_EXCLUSIVE"
    ] == "1"
    assert paid_browser["environment"][
        "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"
    ] == "${WHOSCORED_FLARESOLVERR_GATEWAY_SECRET:-}"
    assert "ports" not in paid_browser
    assert "volumes" not in paid_browser
    assert paid_browser["networks"] == ["whoscored-paid-browser"]
    assert set(gateway["networks"]) == {
        "whoscored-paid-api",
        "whoscored-paid-browser",
        "whoscored-paid-direct-egress",
    }
    assert set(dedicated["networks"]) == {
        "whoscored-paid-browser",
        "whoscored-paid-provider-egress",
    }
    assert "whoscored-paid-api" in services["airflow-scheduler"]["networks"]
    assert "backend" not in gateway["networks"]
    assert "backend" not in dedicated["networks"]
    assert compose["networks"]["whoscored-paid-api"] == {
        "name": "dp-whoscored-paid-api",
        "external": True,
    }
    for name in ("whoscored-paid-api", "whoscored-paid-browser"):
        assert gateway_compose["networks"][name]["internal"] is True
    for name in (
        "whoscored-paid-direct-egress",
        "whoscored-paid-provider-egress",
    ):
        assert gateway_compose["networks"][name].get("internal") is not True
    assert gateway["command"][-6:] == [
        "--proxy-url",
        "http://whoscored_proxy_filter:8900",
        "--proxy-control-url",
        "http://whoscored_proxy_filter:8899",
        "--flaresolverr-url",
        "http://flaresolverr_whoscored_paid:8191",
    ]
    command = dedicated["command"]
    assert command[command.index("--whoscored-state-marker") + 1] == (
        "/opt/airflow/state/whoscored-proxy-filter/"
        ".whoscored_state_initialized.json"
    )
    assert command[command.index("--daily-budget-bytes") + 1] == (
        "${WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES:?set exact provider-policy daily cap in decimal bytes}"
    )
    assert command[command.index("--max-lease-bytes") + 1] == (
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES:-2000000}"
    )
    assert command[command.index("--url-budget-bytes") + 1] == (
        "${WHOSCORED_PROXY_FILTER_URL_BUDGET_BYTES:-2000000}"
    )


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

    assert "WHOSCORED_RAW_S3_ACCESS_KEY: ${WHOSCORED_RAW_S3_ACCESS_KEY:-}" in raw
    assert "WHOSCORED_RAW_S3_SECRET_KEY: ${WHOSCORED_RAW_S3_SECRET_KEY:-}" in raw
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


def test_one_time_storage_isolation_covers_heavy_without_paid_profile() -> None:
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )
    rollout = runbook.split("### Targeted runtime deployment", 1)[0].rsplit(
        "```bash", 1
    )[1]

    assert '"${COMPOSE[@]}" --profile heavy down --remove-orphans' in rollout
    assert "--profile whoscored-paid" not in rollout
    assert "docker inspect whoscored_proxy_filter >/dev/null 2>&1" in rollout
    assert "whoscored_proxy_filter must be absent" in rollout
    assert rollout.count("docker volume inspect seaweedfs_data") >= 2
    assert "SEAWEEDFS_VOLUME_IDENTITY_SHA256" in rollout
    assert rollout.index("SEAWEEDFS_VOLUME_IDENTITY_SHA256") < rollout.index(
        '"${COMPOSE[@]}" --profile heavy down'
    )
    assert rollout.rindex("docker volume inspect seaweedfs_data") < rollout.index(
        '"${COMPOSE[@]}" up -d --no-recreate'
    )


def test_ready_promotion_names_every_local_build_service() -> None:
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )
    promotion = runbook.split("#### Future ready-v1 promotion", 1)[1].split(
        "Deploy that exact reviewed promotion SHA", 1
    )[0]
    documented = set(re.findall(r'--payload-image-id "([^=]+)=', promotion))
    services = {
        **_compose()["services"],
        **_gateway_compose()["services"],
    }
    locally_built_images = {
        service.get("image")
        for service in services.values()
        if "build" in service and service.get("image")
    }
    local_builds = {
        name
        for name, service in services.items()
        if "build" in service or service.get("image") in locally_built_images
    }

    assert documented == local_builds


def test_release_build_rejects_local_tool_caches() -> None:
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )
    promotion = runbook.split("#### Future ready-v1 promotion", 1)[1].split(
        "Deploy that exact reviewed promotion SHA", 1
    )[0]

    for cache_name in ("__pycache__", ".pytest_cache", ".ruff_cache"):
        assert f"-name {cache_name}" in promotion


def test_ready_promotion_uses_the_ci_pinned_buildkit_builder() -> None:
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )
    workflow = (ROOT / ".github" / "workflows" / "whoscored-ci.yml").read_text(
        encoding="utf-8"
    )
    promotion = runbook.split("#### Future ready-v1 promotion", 1)[1].split(
        "Deploy that exact reviewed promotion SHA", 1
    )[0]
    pinned = re.search(
        r"image=(moby/buildkit:v0\.31\.2@sha256:[0-9a-f]{64})", workflow
    )
    pinned_registry = re.search(
        r"(registry:2\.8\.3@sha256:[0-9a-f]{64})", workflow
    )
    payload_builds = promotion.split("AIRFLOW_BASE_STAGING_TAG=", 1)[1].split(
        "AIRFLOW_BASE_ID=", 1
    )[0]

    assert pinned is not None
    assert pinned_registry is not None
    assert f"BUILDKIT_IMAGE={pinned.group(1)}" in promotion
    assert f"LOOPBACK_REGISTRY_IMAGE={pinned_registry.group(1)}" in promotion
    assert "--publish 127.0.0.1:5000:5000" in promotion
    assert '"http://$LOOPBACK_REGISTRY/v2/"' in promotion
    assert '--driver-opt "image=$BUILDKIT_IMAGE"' in promotion
    assert "--driver-opt network=host" in promotion
    assert "--driver-opt provenance-add-gha=false" in promotion
    assert '[registry."127.0.0.1:5000"]' in promotion
    assert "  http = true" in promotion
    assert "  insecure = true" in promotion
    assert '--buildkitd-config "$BUILDKITD_CONFIG"' in promotion
    assert 'BUILDX_BUILD=("${BUILDX_CMD[@]}" --builder "$BUILDER" build)' in promotion
    assert promotion.count('"${BUILDX_BUILD[@]}" --platform linux/amd64') == 8
    assert '"${BUILDX_CMD[@]}" build --platform linux/amd64' not in promotion
    assert "--load" not in promotion
    assert payload_builds.count("--provenance=mode=max,version=v1 --push") == 6
    assert payload_builds.count("--metadata-file") == 6
    assert payload_builds.count('"${DOCKER_CMD[@]}" image tag') == 6
    for group in (
        "AIRFLOW_BASE",
        "SCHEDULER_PAYLOAD",
        "PROXY_PAYLOAD",
        "FLARESOLVERR",
        "JUPYTERHUB",
        "SUPERSET",
    ):
        assert f'--tag "${group}_STAGING_TAG"' in payload_builds
        assert f'--tag "${group}_TAG"' not in payload_builds
    assert '"${DOCKER_CMD[@]}" image push' not in promotion
    assert promotion.count('skopeo copy --all --preserve-digests') == 4
    assert promotion.index('test "$("${CLEAN_GIT[@]}" rev-parse HEAD^)"') < (
        promotion.index('skopeo copy --all --preserve-digests')
    )


def test_targeted_rollout_creates_and_starts_only_one_admitted_service() -> None:
    runbook = (ROOT / "docs" / "operations" / "whoscored-production.md").read_text(
        encoding="utf-8"
    )
    rollout = runbook.split("### Targeted runtime deployment", 1)[1].split(
        "##### Initialize the paid-filter state exactly once", 1
    )[0]
    for service, container_id_variable in (
        ("airflow-scheduler", "SCHEDULER_CONTAINER_ID"),
        ("flaresolverr", "FLARESOLVERR_CONTAINER_ID"),
    ):
        create = (
            '"${COMPOSE[@]}" up --no-start --no-deps --no-build --pull always '
            f"\\\n  {service}"
        )
        admission = f"--service {service} \\\n"
        receipt_id = f'.service == "{service}"'
        start = f'"${{DOCKER[@]}}" start "${container_id_variable}"'

        assert create in rollout
        assert rollout.index(create) < rollout.index(admission)
        assert rollout.index(admission) < rollout.index(receipt_id)
        assert rollout.index(receipt_id) < rollout.index(start)

    assert '"${COMPOSE[@]}" create' not in rollout
    assert '"${COMPOSE[@]}" start' not in rollout
