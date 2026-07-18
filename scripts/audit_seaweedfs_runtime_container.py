#!/usr/bin/env python3
"""Compare one live protected SeaweedFS container with rendered Compose.

Stdin contains two base64-encoded JSON documents, one per line: the complete
rendered Compose model followed by ``docker inspect`` output.  This keeps the
model out of command-line arguments while still allowing a strict comparison.
"""

from __future__ import annotations

import base64
from decimal import Decimal, InvalidOperation
import json
import re
import sys
from typing import Any, NoReturn


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def mapping(value: Any, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(message)
    return value


def normalise_capabilities(value: Any) -> set[str]:
    if value is None:
        return set()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        fail("container capabilities are malformed")
    return {item if item.startswith("CAP_") else f"CAP_{item}" for item in value}


def duration_nanoseconds(value: Any) -> int:
    if not isinstance(value, str) or not value:
        fail("rendered duration is malformed")
    factors = {
        "h": Decimal(3_600_000_000_000),
        "m": Decimal(60_000_000_000),
        "s": Decimal(1_000_000_000),
        "ms": Decimal(1_000_000),
        "us": Decimal(1_000),
        "µs": Decimal(1_000),
        "ns": Decimal(1),
    }
    pattern = re.compile(r"([0-9]+(?:[.][0-9]+)?)(ms|us|µs|ns|h|m|s)")
    total = Decimal(0)
    offset = 0
    try:
        for match in pattern.finditer(value):
            if match.start() != offset:
                fail("rendered duration is malformed")
            total += Decimal(match.group(1)) * factors[match.group(2)]
            offset = match.end()
    except InvalidOperation:
        fail("rendered duration is malformed")
    if offset != len(value) or total != total.to_integral_value():
        fail("rendered duration is malformed")
    return int(total)


def expected_tmpfs(service: dict[str, Any]) -> dict[str, set[str]]:
    values = service.get("tmpfs", [])
    if not isinstance(values, list):
        fail("rendered tmpfs is malformed")
    result: dict[str, set[str]] = {}
    for value in values:
        if not isinstance(value, str):
            fail("rendered tmpfs is malformed")
        target, separator, options = value.partition(":")
        if not target.startswith("/") or target in result:
            fail("rendered tmpfs identity is invalid")
        if separator:
            option_set = set(options.split(","))
            if "" in option_set:
                fail("rendered tmpfs options are invalid")
        else:
            option_set = set()
        result[target] = option_set
    return result


def live_tmpfs(host: dict[str, Any]) -> dict[str, set[str]]:
    values = host.get("Tmpfs")
    if values is None:
        return {}
    values = mapping(values, "live tmpfs is malformed")
    result: dict[str, set[str]] = {}
    for target, options in values.items():
        if (
            not isinstance(target, str)
            or not target.startswith("/")
            or not isinstance(options, str)
        ):
            fail("live tmpfs identity is invalid")
        result[target] = set(options.split(",")) if options else set()
    return result


def validate_healthcheck(service: dict[str, Any], config: dict[str, Any]) -> None:
    expected = service.get("healthcheck")
    actual = config.get("Healthcheck")
    if expected is None:
        if actual not in (None, {}):
            fail("live healthcheck differs from rendered identity")
        return
    expected = mapping(expected, "rendered healthcheck is malformed")
    actual = mapping(actual, "live healthcheck is missing")
    expected_test = expected.get("test")
    if not isinstance(expected_test, list) or any(
        not isinstance(item, str) for item in expected_test
    ):
        fail("rendered healthcheck test is malformed")
    expected_values = {
        "Test": expected_test,
        "Interval": duration_nanoseconds(expected.get("interval", "0s")),
        "Timeout": duration_nanoseconds(expected.get("timeout", "0s")),
        "Retries": expected.get("retries", 0),
        "StartPeriod": duration_nanoseconds(expected.get("start_period", "0s")),
        "StartInterval": duration_nanoseconds(
            expected.get("start_interval", "0s")
        ),
    }
    actual_values = {
        key: actual.get(key, [] if key == "Test" else 0)
        for key in expected_values
    }
    if actual_values != expected_values:
        fail("live healthcheck differs from rendered identity")


def validate_restart_and_resources(
    service: dict[str, Any], config: dict[str, Any], host: dict[str, Any]
) -> None:
    restart = service.get("restart", "no")
    if not isinstance(restart, str) or not restart:
        fail("rendered restart policy is malformed")
    restart_name, separator, retry_text = restart.partition(":")
    retry_count = 0
    if separator:
        if restart_name != "on-failure" or not retry_text.isdigit():
            fail("rendered restart policy is malformed")
        retry_count = int(retry_text)
    actual_restart = mapping(
        host.get("RestartPolicy"), "live restart policy is missing"
    )
    if actual_restart != {
        "Name": restart_name,
        "MaximumRetryCount": retry_count,
    }:
        fail("live restart policy differs from rendered identity")

    expected_signal = service.get("stop_signal") or "SIGTERM"
    actual_signal = config.get("StopSignal") or "SIGTERM"
    if not isinstance(expected_signal, str) or not isinstance(actual_signal, str):
        fail("stop signal is malformed")
    if actual_signal.upper() != expected_signal.upper():
        fail("live stop signal differs from rendered identity")

    expected_stop = service.get("stop_grace_period")
    actual_stop = config.get("StopTimeout")
    if expected_stop is None:
        if actual_stop not in (None, 0):
            fail("live stop timeout differs from rendered identity")
    else:
        stop_ns = duration_nanoseconds(expected_stop)
        if stop_ns % 1_000_000_000 or actual_stop != stop_ns // 1_000_000_000:
            fail("live stop timeout differs from rendered identity")

    deploy = mapping(service.get("deploy", {}), "rendered deploy limits are malformed")
    resources = mapping(
        deploy.get("resources", {}), "rendered resources are malformed"
    )
    limits = mapping(resources.get("limits", {}), "rendered limits are malformed")
    reservations = mapping(
        resources.get("reservations", {}), "rendered reservations are malformed"
    )

    def integer_resource(container: dict[str, Any], key: str, default: int = 0) -> int:
        value = container.get(key, default)
        if value is None:
            value = default
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            fail("rendered resource limit is malformed")
        try:
            return int(value)
        except ValueError:
            fail("rendered resource limit is malformed")

    expected_memory = integer_resource(limits, "memory")
    expected_reservation = integer_resource(reservations, "memory")
    expected_pids = integer_resource(limits, "pids")
    cpus = limits.get("cpus", "0")
    try:
        expected_nano_cpus = int(Decimal(str(cpus)) * Decimal(1_000_000_000))
    except InvalidOperation:
        fail("rendered CPU limit is malformed")
    actual_resources = {
        "Memory": host.get("Memory", 0) or 0,
        "MemoryReservation": host.get("MemoryReservation", 0) or 0,
        "NanoCpus": host.get("NanoCpus", 0) or 0,
        "PidsLimit": host.get("PidsLimit", 0) or 0,
    }
    expected_resources = {
        "Memory": expected_memory,
        "MemoryReservation": expected_reservation,
        "NanoCpus": expected_nano_cpus,
        "PidsLimit": expected_pids,
    }
    if actual_resources != expected_resources:
        fail("live resource limits differ from rendered identity")


def read_documents() -> tuple[dict[str, Any], dict[str, Any]]:
    lines = sys.stdin.read().splitlines()
    if len(lines) != 2:
        fail("runtime audit requires exactly two encoded documents")
    decoded: list[Any] = []
    for line in lines:
        try:
            decoded.append(json.loads(base64.b64decode(line, validate=True)))
        except (ValueError, json.JSONDecodeError) as error:
            fail(f"runtime audit document is invalid: {type(error).__name__}")
    rendered = mapping(decoded[0], "rendered Compose model is malformed")
    inspection = decoded[1]
    if not isinstance(inspection, list) or len(inspection) != 1:
        fail("Docker inspection must contain exactly one container")
    return rendered, mapping(inspection[0], "Docker inspection is malformed")


def expected_mounts(
    rendered: dict[str, Any], service: dict[str, Any]
) -> dict[str, tuple[str, str, bool]]:
    volumes = service.get("volumes", [])
    if not isinstance(volumes, list):
        fail("rendered service mounts are malformed")
    top_volumes = mapping(rendered.get("volumes", {}), "rendered volumes are malformed")
    result: dict[str, tuple[str, str, bool]] = {}
    for mount_value in volumes:
        mount = mapping(mount_value, "rendered service mount is malformed")
        mount_type = mount.get("type")
        source = mount.get("source")
        target = mount.get("target")
        read_only = mount.get("read_only", False)
        if (
            mount_type not in {"bind", "volume"}
            or not isinstance(source, str)
            or not isinstance(target, str)
            or not isinstance(read_only, bool)
            or target in result
        ):
            fail("rendered service mount identity is invalid")
        if mount_type == "volume":
            volume = mapping(
                top_volumes.get(source), f"rendered volume {source} is missing"
            )
            concrete_source = volume.get("name", source)
            if not isinstance(concrete_source, str) or not concrete_source:
                fail("rendered volume has no concrete name")
            source = concrete_source
        result[target] = (mount_type, source, read_only)
    return result


def live_mounts(inspection: dict[str, Any]) -> dict[str, tuple[str, str, bool]]:
    mounts = inspection.get("Mounts")
    if not isinstance(mounts, list):
        fail("live container mounts are malformed")
    result: dict[str, tuple[str, str, bool]] = {}
    for mount_value in mounts:
        mount = mapping(mount_value, "live container mount is malformed")
        mount_type = mount.get("Type")
        target = mount.get("Destination")
        read_write = mount.get("RW")
        if mount_type == "volume":
            source = mount.get("Name")
        else:
            source = mount.get("Source")
        if (
            mount_type not in {"bind", "volume"}
            or not isinstance(source, str)
            or not isinstance(target, str)
            or not isinstance(read_write, bool)
            or target in result
        ):
            fail("live container mount identity is invalid")
        result[target] = (mount_type, source, not read_write)
    return result


def expected_networks(
    rendered: dict[str, Any], service: dict[str, Any]
) -> tuple[set[str], dict[str, set[str]]]:
    service_networks = mapping(
        service.get("networks", {}), "rendered service networks are malformed"
    )
    top_networks = mapping(
        rendered.get("networks", {}), "rendered networks are malformed"
    )
    names: set[str] = set()
    aliases: dict[str, set[str]] = {}
    for logical_name, attachment_value in service_networks.items():
        network = mapping(
            top_networks.get(logical_name), f"rendered network {logical_name} is missing"
        )
        concrete_name = network.get("name", logical_name)
        if not isinstance(concrete_name, str) or not concrete_name:
            fail("rendered network has no concrete name")
        attachment = mapping(
            attachment_value or {}, "rendered network attachment is malformed"
        )
        raw_aliases = attachment.get("aliases", [])
        if not isinstance(raw_aliases, list) or any(
            not isinstance(alias, str) for alias in raw_aliases
        ):
            fail("rendered network aliases are malformed")
        names.add(concrete_name)
        aliases[concrete_name] = set(raw_aliases)
    return names, aliases


def expected_port_bindings(service: dict[str, Any]) -> set[tuple[str, str, str]]:
    ports = service.get("ports", [])
    if not isinstance(ports, list):
        fail("rendered service ports are malformed")
    result: set[tuple[str, str, str]] = set()
    for port_value in ports:
        port = mapping(port_value, "rendered service port is malformed")
        target = port.get("target")
        published = port.get("published")
        protocol = port.get("protocol", "tcp")
        host_ip = port.get("host_ip", "")
        if (
            not isinstance(target, (str, int))
            or not isinstance(published, (str, int))
            or not isinstance(protocol, str)
            or not isinstance(host_ip, str)
        ):
            fail("rendered service port identity is invalid")
        result.add((f"{target}/{protocol}", host_ip, str(published)))
    return result


def live_port_bindings(host: dict[str, Any]) -> set[tuple[str, str, str]]:
    bindings = host.get("PortBindings")
    if bindings is None:
        return set()
    bindings = mapping(bindings, "live port bindings are malformed")
    result: set[tuple[str, str, str]] = set()
    for target, values in bindings.items():
        if values is None:
            continue
        if not isinstance(target, str) or not isinstance(values, list):
            fail("live port binding is malformed")
        for value in values:
            binding = mapping(value, "live port binding is malformed")
            host_ip = binding.get("HostIp", "")
            host_port = binding.get("HostPort")
            if not isinstance(host_ip, str) or not isinstance(host_port, str):
                fail("live port binding identity is malformed")
            result.add((target, host_ip, host_port))
    return result


def main() -> None:
    if len(sys.argv) != 4:
        fail("usage: audit <service> <expected-image-id> <expected-volume>")
    service_name, expected_image_id, expected_volume = sys.argv[1:]
    rendered, inspection = read_documents()
    services = mapping(rendered.get("services"), "rendered services are missing")
    service = mapping(
        services.get(service_name), f"rendered service {service_name} is missing"
    )
    config = mapping(inspection.get("Config"), "live Config is missing")
    host = mapping(inspection.get("HostConfig"), "live HostConfig is missing")
    network_settings = mapping(
        inspection.get("NetworkSettings"), "live NetworkSettings is missing"
    )

    expected_image_ref = service.get("image")
    if (
        not isinstance(expected_image_ref, str)
        or inspection.get("Image") != expected_image_id
        or config.get("Image") != expected_image_ref
        or config.get("Cmd") != service.get("command")
        or config.get("Entrypoint") != service.get("entrypoint")
        or config.get("Hostname") != service.get("hostname")
        or config.get("User", "") != service.get("user", "")
    ):
        fail("live command, image, hostname or user differs from rendered identity")

    if (
        host.get("ReadonlyRootfs", False) is not service.get("read_only", False)
        or host.get("Privileged") is not False
        or host.get("PublishAllPorts") is not False
        or live_port_bindings(host) != expected_port_bindings(service)
    ):
        fail("live host privileges or published ports differ from rendered identity")
    validate_healthcheck(service, config)
    validate_restart_and_resources(service, config, host)
    if live_tmpfs(host) != expected_tmpfs(service):
        fail("live tmpfs differs from rendered identity")
    if normalise_capabilities(host.get("CapAdd")) != normalise_capabilities(
        service.get("cap_add")
    ) or normalise_capabilities(host.get("CapDrop")) != normalise_capabilities(
        service.get("cap_drop")
    ):
        fail("live capabilities differ from rendered identity")
    expected_security = set(service.get("security_opt", []))
    actual_security = set(host.get("SecurityOpt") or [])
    def normalise_security(value: str) -> str:
        return value.removesuffix(":true")

    if {normalise_security(value) for value in actual_security} != {
        normalise_security(value) for value in expected_security
    }:
        fail("live security options differ from rendered identity")

    expected_mount_identity = expected_mounts(rendered, service)
    if live_mounts(inspection) != expected_mount_identity:
        fail("live mounts differ from rendered identity")
    volume_mounts = [
        source
        for mount_type, source, _ in expected_mount_identity.values()
        if mount_type == "volume"
    ]
    if any(source != expected_volume for source in volume_mounts):
        fail("rendered data volume differs from protected topology state")

    expected_network_names, expected_aliases = expected_networks(rendered, service)
    live_networks = mapping(
        network_settings.get("Networks"), "live network attachments are malformed"
    )
    if set(live_networks) != expected_network_names:
        fail("live networks differ from rendered identity")
    for network_name, required_aliases in expected_aliases.items():
        attachment = mapping(
            live_networks[network_name], "live network attachment is malformed"
        )
        live_aliases = attachment.get("Aliases") or []
        if not isinstance(live_aliases, list) or not required_aliases <= set(live_aliases):
            fail("live network aliases differ from rendered identity")

    labels = mapping(config.get("Labels"), "live Compose labels are missing")
    if labels.get("com.docker.compose.service") != service_name:
        fail("live container has the wrong Compose service label")
    expected_project = rendered.get("name")
    if not isinstance(expected_project, str) or not expected_project:
        fail("rendered Compose project name is missing")
    if labels.get("com.docker.compose.project") != expected_project:
        fail("live container has the wrong Compose project label")
    expected_environment = service.get("environment", {})
    if expected_environment is None:
        expected_environment = {}
    expected_environment = mapping(
        expected_environment, "rendered environment is malformed"
    )
    protected_environment = {
        key: value
        for key, value in expected_environment.items()
        if key.startswith("SEAWEEDFS_")
    }
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in protected_environment.items()
    ):
        fail("rendered protected environment is malformed")
    live_environment: dict[str, str] = {}
    for item in config.get("Env") or []:
        if not isinstance(item, str) or "=" not in item:
            fail("live environment is malformed")
        key, value = item.split("=", 1)
        if not key or key in live_environment:
            fail("live environment contains an invalid or duplicate key")
        live_environment[key] = value
    live_protected_environment = {
        key: value
        for key, value in live_environment.items()
        if key.startswith("SEAWEEDFS_")
    }
    if live_protected_environment != protected_environment:
        fail("live protected environment differs from rendered identity")


if __name__ == "__main__":
    main()
