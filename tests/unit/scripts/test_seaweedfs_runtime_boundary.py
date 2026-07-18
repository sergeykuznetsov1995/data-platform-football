from __future__ import annotations

import base64
import json
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
IDENTITY_VALIDATOR = ROOT / "scripts/validate_seaweedfs_s3_identity_config.py"
NETWORK_AUDITOR = ROOT / "scripts/audit_seaweedfs_control_network.py"
RUNTIME_AUDITOR = ROOT / "scripts/audit_seaweedfs_runtime_container.py"


def _identity(
    name: str,
    access_key: str,
    secret_key: str,
    actions: list[str],
) -> dict:
    return {
        "name": name,
        "credentials": [{"accessKey": access_key, "secretKey": secret_key}],
        "actions": actions,
    }


def _rendered_model(config_path: Path) -> dict:
    environment = {
        "S3_ACCESS_KEY": "admin-access",
        "S3_SECRET_KEY": "admin-secret",
        "ICEBERG_WAREHOUSE": "warehouse",
        "WHOSCORED_RAW_S3_ACCESS_KEY": "raw-access",
        "WHOSCORED_RAW_S3_SECRET_KEY": "raw-secret",
    }
    scheduler_environment = {
        **environment,
        "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY": "backup-access",
        "WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY": "backup-secret",
    }
    mount = {
        "type": "bind",
        "source": str(config_path),
        "target": "/etc/seaweedfs/s3.config.json",
        "read_only": True,
    }
    return {
        "services": {
            "seaweedfs": {"volumes": [mount]},
            "seaweedfs-s3": {"volumes": [mount]},
            "airflow-init": {"environment": environment},
            "airflow-scheduler": {"environment": scheduler_environment},
            "airflow-webserver": {"environment": environment},
            "trino": {
                "environment": {
                    "S3_ACCESS_KEY": "admin-access",
                    "S3_SECRET_KEY": "admin-secret",
                }
            },
        }
    }


def _run_identity_validator(
    tmp_path: Path,
    *,
    identities: list[dict] | None = None,
    mutate_model=None,
) -> subprocess.CompletedProcess[str]:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir(mode=0o700, parents=True)
    config = secret_dir / "s3.config.json"
    config.write_text(
        json.dumps(
            {
                "identities": identities
                if identities is not None
                else [
                    _identity(
                        "platform-admin",
                        "admin-access",
                        "admin-secret",
                        ["Admin"],
                    ),
                    _identity(
                        "whoscored-raw",
                        "raw-access",
                        "raw-secret",
                        ["Read:warehouse", "List:warehouse", "Write:warehouse"],
                    ),
                    _identity(
                        "whoscored-backup",
                        "backup-access",
                        "backup-secret",
                        ["Read:warehouse", "List:warehouse"],
                    ),
                ]
            }
        ),
        encoding="utf-8",
    )
    config.chmod(0o600)
    model = _rendered_model(config)
    if mutate_model is not None:
        mutate_model(model)
    return subprocess.run(
        ["python3", str(IDENTITY_VALIDATOR)],
        input=json.dumps(model),
        text=True,
        capture_output=True,
        check=False,
    )


def test_rendered_s3_identity_boundary_accepts_exact_credentials(tmp_path) -> None:
    result = _run_identity_validator(tmp_path)

    assert result.returncode == 0, result.stderr
    fields = result.stdout.strip().split("|")
    assert len(fields) == 9
    assert fields[-1] == "v1"
    decoded = [base64.b64decode(value).decode() for value in fields[:-1]]
    assert decoded[1:4] == ["admin-access", "admin-secret", "warehouse"]
    assert decoded[4:] == [
        "raw-access",
        "raw-secret",
        "backup-access",
        "backup-secret",
    ]


@pytest.mark.parametrize(
    ("include_raw", "include_backup"),
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_rendered_s3_identity_boundary_accepts_exact_configured_roles(
    tmp_path, include_raw, include_backup
) -> None:
    identities = [
        _identity("platform-admin", "admin-access", "admin-secret", ["Admin"])
    ]
    if include_raw:
        identities.append(
            _identity(
                "whoscored-raw",
                "raw-access",
                "raw-secret",
                ["Read:warehouse", "List:warehouse", "Write:warehouse"],
            )
        )
    if include_backup:
        identities.append(
            _identity(
                "whoscored-backup",
                "backup-access",
                "backup-secret",
                ["Read:warehouse", "List:warehouse"],
            )
        )

    def mutate(model: dict) -> None:
        if not include_raw:
            for service_name in (
                "airflow-init",
                "airflow-scheduler",
                "airflow-webserver",
            ):
                environment = model["services"][service_name]["environment"]
                environment.pop("WHOSCORED_RAW_S3_ACCESS_KEY", None)
                environment.pop("WHOSCORED_RAW_S3_SECRET_KEY", None)
        if not include_backup:
            environment = model["services"]["airflow-scheduler"]["environment"]
            environment.pop("WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY", None)
            environment.pop("WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY", None)

    result = _run_identity_validator(
        tmp_path, identities=identities, mutate_model=mutate
    )

    assert result.returncode == 0, result.stderr


def test_rendered_s3_identity_boundary_rejects_stale_second_admin_credential(
    tmp_path,
) -> None:
    identities = [
        _identity("platform-admin", "admin-access", "admin-secret", ["Admin"]),
        _identity(
            "whoscored-raw",
            "raw-access",
            "raw-secret",
            ["Read:warehouse", "List:warehouse", "Write:warehouse"],
        ),
        _identity(
            "whoscored-backup",
            "backup-access",
            "backup-secret",
            ["Read:warehouse", "List:warehouse"],
        ),
    ]
    identities[0]["credentials"].append(
        {"accessKey": "stale-admin-access", "secretKey": "stale-admin-secret"}
    )

    result = _run_identity_validator(tmp_path, identities=identities)

    assert result.returncode != 0
    assert "exactly one credential" in result.stderr


def test_rendered_s3_identity_boundary_rejects_extra_admin_identity(tmp_path) -> None:
    identities = [
        _identity("platform-admin", "admin-access", "admin-secret", ["Admin"]),
        _identity(
            "whoscored-raw",
            "raw-access",
            "raw-secret",
            ["Read:warehouse", "List:warehouse", "Write:warehouse"],
        ),
        _identity(
            "whoscored-backup",
            "backup-access",
            "backup-secret",
            ["Read:warehouse", "List:warehouse"],
        ),
        _identity("stale-admin", "stale-access", "stale-secret", ["Admin"]),
    ]

    result = _run_identity_validator(tmp_path, identities=identities)

    assert result.returncode != 0
    assert "missing, extra or stale credentials" in result.stderr


@pytest.mark.parametrize(
    "mutation, expected_error",
    [
        (
            lambda identities: identities[0]["actions"].append("Write"),
            "exact least-privilege",
        ),
        (
            lambda identities: identities[0]["credentials"][0].update(
                {"staleKey": "stale-value"}
            ),
            "unexpected fields",
        ),
    ],
)
def test_rendered_s3_identity_boundary_rejects_extra_action_or_key(
    tmp_path, mutation, expected_error
) -> None:
    identities = [
        _identity("platform-admin", "admin-access", "admin-secret", ["Admin"]),
        _identity(
            "whoscored-raw",
            "raw-access",
            "raw-secret",
            ["Read:warehouse", "List:warehouse", "Write:warehouse"],
        ),
        _identity(
            "whoscored-backup",
            "backup-access",
            "backup-secret",
            ["Read:warehouse", "List:warehouse"],
        ),
    ]
    mutation(identities)

    result = _run_identity_validator(tmp_path, identities=identities)

    assert result.returncode != 0
    assert expected_error in result.stderr


@pytest.mark.parametrize(
    "identities, expected_error",
    [
        ([], "credentialed identity"),
        (
            [
                _identity("one", "admin-access", "admin-secret", ["Admin"]),
                _identity("two", "admin-access", "different", ["Admin"]),
            ],
            "duplicate",
        ),
        (
            [_identity("wrong", "other-access", "other-secret", ["Admin"])],
            "active platform",
        ),
    ],
)
def test_rendered_s3_identity_boundary_rejects_invalid_identity_sets(
    tmp_path, identities, expected_error
) -> None:
    result = _run_identity_validator(tmp_path, identities=identities)

    assert result.returncode != 0
    assert expected_error in result.stderr


def test_rendered_env_file_credentials_cannot_bypass_identity_mapping(tmp_path) -> None:
    def mutate(model: dict) -> None:
        for service_name in (
            "airflow-init",
            "airflow-scheduler",
            "airflow-webserver",
            "trino",
        ):
            model["services"][service_name]["environment"][
                "S3_ACCESS_KEY"
            ] = "env-file-access"
            model["services"][service_name]["environment"][
                "S3_SECRET_KEY"
            ] = "env-file-secret"

    result = _run_identity_validator(tmp_path, mutate_model=mutate)

    assert result.returncode != 0
    assert "active platform S3 credential" in result.stderr


def test_rendered_s3_identity_boundary_rejects_privilege_and_mount_drift(
    tmp_path,
) -> None:
    identities = [
        _identity("platform", "admin-access", "admin-secret", ["Admin"]),
        _identity("raw-admin", "raw-access", "raw-secret", ["Admin"]),
        _identity(
            "backup",
            "backup-access",
            "backup-secret",
            ["Read:warehouse", "List:warehouse"],
        ),
    ]
    result = _run_identity_validator(tmp_path / "privilege", identities=identities)
    assert result.returncode != 0
    assert "exact least-privilege" in result.stderr

    def mutate(model: dict) -> None:
        model["services"]["seaweedfs-s3"]["volumes"][0]["read_only"] = False

    result = _run_identity_validator(tmp_path / "mount", mutate_model=mutate)
    assert result.returncode != 0
    assert "mount is not protected" in result.stderr


def _network_model(**overrides) -> dict:
    model = {
        "Name": "dp-seaweedfs-control",
        "Driver": "bridge",
        "Scope": "local",
        "Internal": True,
        "Attachable": False,
        "Ingress": False,
        "Labels": {"com.docker.compose.network": "seaweedfs-control"},
        "Containers": {
            "a" * 64: {"Name": "seaweedfs"},
            "b" * 64: {"Name": "seaweedfs-s3-proxy"},
        },
    }
    model.update(overrides)
    return model


def _run_network_auditor(model: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            str(NETWORK_AUDITOR),
            "dp-seaweedfs-control",
            "seaweedfs",
            "seaweedfs-s3",
            "seaweedfs-s3-proxy",
        ],
        input=json.dumps([model]),
        text=True,
        capture_output=True,
        check=False,
    )


def test_control_network_auditor_accepts_exact_internal_membership() -> None:
    result = _run_network_auditor(_network_model())

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "drift",
    [
        {"Internal": False},
        {"Driver": "overlay"},
        {"Scope": "swarm"},
        {"Attachable": True},
        {"Labels": {}},
        {"Containers": {"c" * 64: {"Name": "airflow-scheduler"}}},
    ],
)
def test_control_network_auditor_rejects_isolation_or_membership_drift(
    drift,
) -> None:
    result = _run_network_auditor(_network_model(**drift))

    assert result.returncode != 0


def _runtime_documents() -> tuple[dict, list[dict]]:
    image_ref = "chrislusf/seaweedfs:4.36@sha256:" + "8" * 64
    command = ["master", "-ip=seaweedfs-master", "-port=9333"]
    entrypoint = ["/usr/local/bin/seaweedfs-plane-entrypoint"]
    rendered = {
        "name": "data-platform",
        "services": {
            "seaweedfs-master": {
                "image": image_ref,
                "hostname": "seaweedfs-master",
                "command": command,
                "entrypoint": entrypoint,
                "cap_drop": ["ALL"],
                "cap_add": ["CHOWN"],
                "security_opt": ["no-new-privileges:true"],
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "http://localhost:9333/status"],
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 12,
                    "start_period": "2.5s",
                },
                "restart": "unless-stopped",
                "stop_grace_period": "30s",
                "tmpfs": ["/tmp:rw,noexec,nosuid,size=16m"],
                "deploy": {
                    "resources": {
                        "limits": {"memory": "536870912", "cpus": "0.5"},
                        "reservations": {"memory": "134217728"},
                    }
                },
                "environment": {
                    "SEAWEEDFS_EXPECTED_INVENTORY_SHA256": "b" * 64,
                },
                "volumes": [
                    {
                        "type": "volume",
                        "source": "seaweedfs_data",
                        "target": "/data",
                    },
                    {
                        "type": "bind",
                        "source": "/reviewed/seaweedfs_plane_entrypoint.sh",
                        "target": "/usr/local/bin/seaweedfs-plane-entrypoint",
                        "read_only": True,
                    },
                ],
                "networks": {
                    "seaweedfs-control": {"aliases": ["seaweedfs-master"]}
                },
            }
        },
        "volumes": {"seaweedfs_data": {"name": "protected_data"}},
        "networks": {
            "seaweedfs-control": {"name": "dp-seaweedfs-control"}
        },
    }
    inspection = [
        {
            "Image": "sha256:" + "a" * 64,
            "Config": {
                "Image": image_ref,
                "Cmd": command,
                "Entrypoint": entrypoint,
                "Hostname": "seaweedfs-master",
                "User": "",
                "Labels": {
                    "com.docker.compose.project": "data-platform",
                    "com.docker.compose.service": "seaweedfs-master",
                },
                "Env": [f"SEAWEEDFS_EXPECTED_INVENTORY_SHA256={'b' * 64}"],
                "Healthcheck": {
                    "Test": ["CMD", "curl", "-f", "http://localhost:9333/status"],
                    "Interval": 10_000_000_000,
                    "Timeout": 5_000_000_000,
                    "Retries": 12,
                    "StartPeriod": 2_500_000_000,
                },
                "StopTimeout": 30,
            },
            "HostConfig": {
                "ReadonlyRootfs": False,
                "Privileged": False,
                "PublishAllPorts": False,
                "PortBindings": {},
                "CapAdd": ["CAP_CHOWN"],
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges:true"],
                "RestartPolicy": {
                    "Name": "unless-stopped",
                    "MaximumRetryCount": 0,
                },
                "Memory": 536_870_912,
                "MemoryReservation": 134_217_728,
                "NanoCpus": 500_000_000,
                "PidsLimit": 0,
                "Tmpfs": {"/tmp": "rw,noexec,nosuid,size=16m"},
            },
            "Mounts": [
                {
                    "Type": "volume",
                    "Name": "protected_data",
                    "Destination": "/data",
                    "RW": True,
                },
                {
                    "Type": "bind",
                    "Source": "/reviewed/seaweedfs_plane_entrypoint.sh",
                    "Destination": "/usr/local/bin/seaweedfs-plane-entrypoint",
                    "RW": False,
                },
            ],
            "NetworkSettings": {
                "Networks": {
                    "dp-seaweedfs-control": {
                        "Aliases": ["seaweedfs-master", "container-id"]
                    }
                }
            },
        }
    ]
    return rendered, inspection


def _run_runtime_auditor(
    rendered: dict, inspection: list[dict]
) -> subprocess.CompletedProcess[bytes]:
    documents = b"\n".join(
        base64.b64encode(json.dumps(document).encode())
        for document in (rendered, inspection)
    ) + b"\n"
    return subprocess.run(
        [
            "python3",
            str(RUNTIME_AUDITOR),
            "seaweedfs-master",
            "sha256:" + "a" * 64,
            "protected_data",
        ],
        input=documents,
        capture_output=True,
        check=False,
    )


def test_supervised_runtime_auditor_accepts_exact_container_identity() -> None:
    rendered, inspection = _runtime_documents()

    result = _run_runtime_auditor(rendered, inspection)

    assert result.returncode == 0, result.stderr.decode()


@pytest.mark.parametrize(
    "drift",
    [
        "command",
        "port",
        "mount",
        "network",
        "project",
        "healthcheck",
        "restart",
        "stop-signal",
        "stop-timeout",
        "tmpfs",
        "resources",
        "protected-env-extra",
        "protected-entrypoint-extra",
        "protected-env-duplicate",
    ],
)
def test_supervised_runtime_auditor_rejects_privilege_and_network_drift(
    drift,
) -> None:
    rendered, inspection = _runtime_documents()
    live = inspection[0]
    if drift == "command":
        live["Config"]["Cmd"] = ["master", "-port=19333"]
    elif drift == "port":
        live["HostConfig"]["PortBindings"] = {
            "9333/tcp": [{"HostIp": "0.0.0.0", "HostPort": "9333"}]
        }
    elif drift == "mount":
        live["Mounts"].append(
            {
                "Type": "bind",
                "Source": "/host",
                "Destination": "/host",
                "RW": True,
            }
        )
    elif drift == "network":
        live["NetworkSettings"]["Networks"]["dp-storage"] = {
            "Aliases": ["seaweedfs-master"]
        }
    elif drift == "project":
        live["Config"]["Labels"]["com.docker.compose.project"] = "wrong-project"
    elif drift == "healthcheck":
        live["Config"]["Healthcheck"]["Retries"] = 1
    elif drift == "restart":
        live["HostConfig"]["RestartPolicy"]["Name"] = "always"
    elif drift == "stop-signal":
        live["Config"]["StopSignal"] = "SIGKILL"
    elif drift == "stop-timeout":
        live["Config"]["StopTimeout"] = 5
    elif drift == "tmpfs":
        live["HostConfig"]["Tmpfs"]["/tmp"] += ",exec"
    elif drift == "protected-env-extra":
        live["Config"]["Env"].append("SEAWEEDFS_DATA_ROOT=/attacker")
    elif drift == "protected-entrypoint-extra":
        live["Config"]["Env"].append("SEAWEEDFS_IMAGE_ENTRYPOINT=/bin/false")
    elif drift == "protected-env-duplicate":
        live["Config"]["Env"].append(
            f"SEAWEEDFS_EXPECTED_INVENTORY_SHA256={'c' * 64}"
        )
    else:
        live["HostConfig"]["Memory"] = 0

    result = _run_runtime_auditor(rendered, inspection)

    assert result.returncode != 0
