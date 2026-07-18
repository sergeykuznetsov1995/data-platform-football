#!/usr/bin/env python3
"""Validate the rendered SeaweedFS S3 authentication boundary.

The rendered Compose model is read from stdin so values supplied through
``docker compose --env-file`` are checked with the same precedence as the
eventual deployment.  Successful output is a fixed, base64-encoded record for
the shell wrapper; credential values are never written to logs in plaintext.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, NoReturn


CONFIG_TARGET = "/etc/seaweedfs/s3.config.json"
EXPECTED_AIRFLOW_SERVICES = (
    "airflow-init",
    "airflow-scheduler",
    "airflow-webserver",
)
SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def require_mapping(value: Any, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(message)
    return value


def rendered_environment(
    services: dict[str, Any], service_name: str
) -> dict[str, str]:
    service = require_mapping(
        services.get(service_name), f"rendered service {service_name} is missing"
    )
    environment = require_mapping(
        service.get("environment"),
        f"rendered service {service_name} environment is missing",
    )
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in environment.items()):
        fail(f"rendered service {service_name} environment is malformed")
    return environment


def required_value(environment: dict[str, str], name: str, service: str) -> str:
    value = environment.get(name, "")
    if not value:
        fail(f"{name} is required in rendered service {service}")
    if any(ord(character) < 33 or ord(character) > 126 for character in value):
        fail(f"{name} contains unsupported characters")
    return value


def optional_pair(
    environment: dict[str, str], access_name: str, secret_name: str, service: str
) -> tuple[str, str] | None:
    access_key = environment.get(access_name, "")
    secret_key = environment.get(secret_name, "")
    if bool(access_key) != bool(secret_key):
        fail(f"{access_name} and {secret_name} must be set together")
    if not access_key:
        return None
    for name, value in ((access_name, access_key), (secret_name, secret_key)):
        if any(ord(character) < 33 or ord(character) > 126 for character in value):
            fail(f"{name} contains unsupported characters")
    return access_key, secret_key


def config_source(services: dict[str, Any]) -> Path:
    sources: set[str] = set()
    mounted_services = 0
    for service_name in ("seaweedfs", "seaweedfs-s3"):
        service = services.get(service_name)
        if service is None:
            continue
        service = require_mapping(service, f"rendered service {service_name} is malformed")
        mounts = service.get("volumes", [])
        if not isinstance(mounts, list):
            fail(f"rendered service {service_name} volumes are malformed")
        matches = [
            mount
            for mount in mounts
            if isinstance(mount, dict) and mount.get("target") == CONFIG_TARGET
        ]
        if len(matches) != 1:
            fail(f"rendered service {service_name} must mount one S3 identity config")
        mount = matches[0]
        source = mount.get("source")
        if (
            mount.get("type") != "bind"
            or mount.get("read_only") is not True
            or not isinstance(source, str)
            or not os.path.isabs(source)
        ):
            fail(f"rendered service {service_name} S3 identity mount is not protected")
        sources.add(source)
        mounted_services += 1
    if mounted_services == 0 or len(sources) != 1:
        fail("rendered SeaweedFS services do not share one S3 identity config")
    return Path(sources.pop())


def validate_private_file(path: Path) -> None:
    absolute_path = Path(os.path.abspath(path))
    try:
        if absolute_path.resolve(strict=True) != absolute_path:
            fail("SeaweedFS S3 identity config path must not contain symlinks")
    except OSError as error:
        fail(f"SeaweedFS S3 identity config cannot be resolved: {error.strerror}")
    try:
        directory_stat = absolute_path.parent.lstat()
        file_stat = absolute_path.lstat()
    except OSError as error:
        fail(f"SeaweedFS S3 identity config cannot be inspected: {error.strerror}")
    allowed_owners = {0, os.getuid()}
    for ancestor in (absolute_path.parent, *absolute_path.parent.parents):
        ancestor_stat = ancestor.lstat()
        writable = stat.S_IMODE(ancestor_stat.st_mode) & 0o022
        sticky_root_directory = (
            ancestor_stat.st_uid == 0 and bool(ancestor_stat.st_mode & stat.S_ISVTX)
        )
        if (
            not stat.S_ISDIR(ancestor_stat.st_mode)
            or stat.S_ISLNK(ancestor_stat.st_mode)
            or ancestor_stat.st_uid not in allowed_owners
            or (writable and not sticky_root_directory)
        ):
            fail("SeaweedFS S3 identity config path is not host-protected")
    if (
        not stat.S_ISDIR(directory_stat.st_mode)
        or stat.S_ISLNK(directory_stat.st_mode)
        or directory_stat.st_uid not in allowed_owners
        or stat.S_IMODE(directory_stat.st_mode) & 0o022
    ):
        fail("SeaweedFS S3 identity config directory is not host-protected")
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or stat.S_ISLNK(file_stat.st_mode)
        or file_stat.st_uid not in allowed_owners
        or stat.S_IMODE(file_stat.st_mode) & 0o077
    ):
        fail("SeaweedFS S3 identity config is not a private regular file")


def validate_identities(
    path: Path,
    *,
    admin_pair: tuple[str, str],
    bucket: str,
    raw_pair: tuple[str, str] | None,
    backup_pair: tuple[str, str] | None,
) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"SeaweedFS S3 identity config is unreadable or invalid: {type(error).__name__}")
    if not isinstance(payload, dict) or set(payload) != {"identities"}:
        fail("S3 identity config must contain only identities")
    identities = payload["identities"]
    if not isinstance(identities, list) or not identities:
        fail("S3 identity config must contain a credentialed identity")

    expected_identities = (
        (admin_pair, {"Admin"}, "active platform S3"),
        (
            raw_pair,
            {f"Read:{bucket}", f"List:{bucket}", f"Write:{bucket}"},
            "WhoScored raw",
        ),
        (
            backup_pair,
            {f"Read:{bucket}", f"List:{bucket}"},
            "WhoScored backup source",
        ),
    )
    expected_by_pair = {
        pair: (actions, label)
        for pair, actions, label in expected_identities
        if pair is not None
    }
    if len(expected_by_pair) != sum(
        pair is not None for pair, _actions, _label in expected_identities
    ):
        fail("rendered S3 roles must use distinct credentials")

    names: set[str] = set()
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    access_keys: set[str] = set()
    for identity_value in identities:
        identity = require_mapping(identity_value, "S3 identity must be an object")
        if set(identity) != {"name", "credentials", "actions"}:
            fail("S3 identity has unexpected fields")
        name = identity["name"]
        credentials = identity["credentials"]
        actions = identity["actions"]
        if not isinstance(credentials, list) or len(credentials) != 1:
            fail("S3 identity must contain exactly one credential")
        if (
            not isinstance(name, str)
            or SAFE_NAME.fullmatch(name) is None
            or name in names
            or not isinstance(actions, list)
            or not actions
            or any(not isinstance(action, str) or not action for action in actions)
            or len(set(actions)) != len(actions)
        ):
            fail("S3 identity is empty, duplicate or malformed")
        names.add(name)
        for credential_value in credentials:
            credential = require_mapping(
                credential_value, "S3 credential must be an object"
            )
            if set(credential) != {"accessKey", "secretKey"}:
                fail("S3 credential has unexpected fields")
            access_key = credential["accessKey"]
            secret_key = credential["secretKey"]
            if (
                not isinstance(access_key, str)
                or not isinstance(secret_key, str)
                or not access_key
                or not secret_key
                or any(ord(character) < 33 or ord(character) > 126 for character in access_key)
                or any(ord(character) < 33 or ord(character) > 126 for character in secret_key)
                or access_key.startswith("<")
                or secret_key.startswith("<")
                or access_key in access_keys
            ):
                fail("S3 credential is empty, templated, duplicate or malformed")
            access_keys.add(access_key)
            pairs[(access_key, secret_key)] = identity

    if admin_pair not in pairs:
        fail("active platform S3 credential is not an Admin identity")
    if set(pairs) != set(expected_by_pair):
        fail("S3 identity config contains missing, extra or stale credentials")
    for pair, (expected_actions, label) in expected_by_pair.items():
        identity = pairs.get(pair)
        if identity is None or set(identity["actions"]) != expected_actions:
            fail(f"{label} credential is not its exact least-privilege identity")


def encode(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def main() -> None:
    try:
        rendered = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        fail(f"rendered Compose model is invalid: {error.msg}")
    model = require_mapping(rendered, "rendered Compose model must be an object")
    services = require_mapping(model.get("services"), "rendered Compose services are missing")
    path = config_source(services)
    validate_private_file(path)

    airflow_envs = {
        name: rendered_environment(services, name) for name in EXPECTED_AIRFLOW_SERVICES
    }
    scheduler = airflow_envs["airflow-scheduler"]
    admin_pair = (
        required_value(scheduler, "S3_ACCESS_KEY", "airflow-scheduler"),
        required_value(scheduler, "S3_SECRET_KEY", "airflow-scheduler"),
    )
    bucket = required_value(scheduler, "ICEBERG_WAREHOUSE", "airflow-scheduler")
    if SAFE_NAME.fullmatch(bucket) is None:
        fail("ICEBERG_WAREHOUSE is invalid")
    for service_name, environment in airflow_envs.items():
        if (
            required_value(environment, "S3_ACCESS_KEY", service_name),
            required_value(environment, "S3_SECRET_KEY", service_name),
        ) != admin_pair or required_value(
            environment, "ICEBERG_WAREHOUSE", service_name
        ) != bucket:
            fail("rendered Airflow services disagree on active S3 identity")
    trino = rendered_environment(services, "trino")
    if (
        required_value(trino, "S3_ACCESS_KEY", "trino"),
        required_value(trino, "S3_SECRET_KEY", "trino"),
    ) != admin_pair:
        fail("rendered Trino and Airflow S3 identities differ")

    raw_pair = optional_pair(
        scheduler,
        "WHOSCORED_RAW_S3_ACCESS_KEY",
        "WHOSCORED_RAW_S3_SECRET_KEY",
        "airflow-scheduler",
    )
    backup_pair = optional_pair(
        scheduler,
        "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY",
        "WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY",
        "airflow-scheduler",
    )
    for service_name, environment in airflow_envs.items():
        if optional_pair(
            environment,
            "WHOSCORED_RAW_S3_ACCESS_KEY",
            "WHOSCORED_RAW_S3_SECRET_KEY",
            service_name,
        ) != raw_pair:
            fail("rendered Airflow services disagree on WhoScored raw identity")

    validate_identities(
        path,
        admin_pair=admin_pair,
        bucket=bucket,
        raw_pair=raw_pair,
        backup_pair=backup_pair,
    )
    raw_pair = raw_pair or ("", "")
    backup_pair = backup_pair or ("", "")
    values = (
        str(path),
        *admin_pair,
        bucket,
        *raw_pair,
        *backup_pair,
    )
    print("|".join((*map(encode, values), "v1")))


if __name__ == "__main__":
    main()
