#!/usr/bin/env python3
"""Fail closed unless a live SeaweedFS control network is isolated."""

from __future__ import annotations

import json
import sys
from typing import NoReturn


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def main() -> None:
    expected_name = sys.argv[1]
    allowed_members = set(sys.argv[2:])
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        fail(f"invalid Docker network inspection: {error.msg}")
    if not isinstance(payload, list) or len(payload) != 1:
        fail("Docker network inspection must contain exactly one network")
    network = payload[0]
    if not isinstance(network, dict):
        fail("Docker network inspection is malformed")
    if (
        network.get("Name") != expected_name
        or network.get("Driver") != "bridge"
        or network.get("Scope") != "local"
        or network.get("Internal") is not True
        or network.get("Attachable") is not False
    ):
        fail("SeaweedFS control network is not an internal local bridge")
    labels = network.get("Labels")
    if not isinstance(labels, dict) or labels.get("com.docker.compose.network") != "seaweedfs-control":
        fail("SeaweedFS control network does not have the reviewed Compose identity")
    containers = network.get("Containers")
    if containers is None:
        containers = {}
    if not isinstance(containers, dict):
        fail("SeaweedFS control network membership is malformed")
    members: list[str] = []
    for endpoint_id, endpoint in containers.items():
        if not isinstance(endpoint_id, str) or not isinstance(endpoint, dict):
            fail("SeaweedFS control network endpoint is malformed")
        name = endpoint.get("Name")
        if not isinstance(name, str) or not name:
            fail("SeaweedFS control network member has no stable name")
        members.append(name)
    if len(members) != len(set(members)) or not set(members) <= allowed_members:
        fail("SeaweedFS control network contains an unreviewed member")


if __name__ == "__main__":
    main()
