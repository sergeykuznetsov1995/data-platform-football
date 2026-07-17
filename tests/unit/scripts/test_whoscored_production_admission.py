from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

import pytest

from scripts import whoscored_production_admission as admission


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
PAYLOADS = {
    "airflow-scheduler": f"sha256:{SHA_A}",
    "flaresolverr": f"sha256:{SHA_B}",
    "flaresolverr_whoscored_paid": f"sha256:{SHA_B}",
    "whoscored_paid_gateway": f"sha256:{SHA_C}",
    "whoscored_proxy_filter": f"sha256:{SHA_C}",
}
BINDINGS = {
    "airflow-scheduler": f"registry.example/scheduler@sha256:{SHA_A}",
    "flaresolverr": f"registry.example/flaresolverr@sha256:{SHA_B}",
    "flaresolverr_whoscored_paid": (f"registry.example/flaresolverr@sha256:{SHA_B}"),
    "whoscored_paid_gateway": f"registry.example/proxy@sha256:{SHA_C}",
    "whoscored_proxy_filter": f"registry.example/proxy@sha256:{SHA_C}",
}
CONFIG_FILES = (
    Path("/release/compose.yaml"),
    Path("/release/compose.seaweedfs-supervised.yaml"),
    Path("/evidence/digest-only.yaml"),
)
ENV_FILES = (
    Path("/evidence/platform.env"),
    Path("/evidence/whoscored.env"),
    Path("/evidence/proxy.env"),
)
CONFIG_HASHES = {service: SHA_D for service in admission.PROTECTED_SERVICES}
PROXY_COMMAND = tuple(
    {
        "${WHOSCORED_PROXY_FILTER_DAILY_BUDGET_MB:-850}": "850",
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_MB:-2}": "2",
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_TTL_SECONDS:-3600}": "3600",
        "${WHOSCORED_PROXY_FILTER_DAGRUN_BUDGET_BYTES:-1000000000}": "1000000000",
        "${WHOSCORED_PROXY_FILTER_URL_BUDGET_BYTES:-2000000}": "2000000",
        "${WHOSCORED_PROXY_FILTER_MAX_ACTIVE_LEASES:-2}": "2",
    }.get(item, item)
    for item in admission.provenance.WHOSCORED_PROXY_COMMAND
)
EFFECTIVE_COMMANDS = {
    "airflow-scheduler": ("scheduler",),
    "flaresolverr": ("/usr/local/bin/whoscored-flaresolverr-entrypoint",),
    "flaresolverr_whoscored_paid": (
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
    ),
    "whoscored_paid_gateway": admission._EXPECTED_COMMANDS["whoscored_paid_gateway"],
    "whoscored_proxy_filter": PROXY_COMMAND,
}


def _rendered_environment(service: str) -> dict[str, str]:
    environment = {name: "" for name in admission._EXPECTED_ENVIRONMENT_NAMES[service]}
    environment.update(admission._FIXED_ENVIRONMENT[service])
    if service == "airflow-scheduler":
        environment.update(
            {
                "FBREF_PROXY_CONTROL_TOKEN": "b" * 64,
                "TM_NATIVE_V2_ENABLED": "false",
                "TM_STANDING_POLICY_ENABLED": "false",
                "TM_REQUIRE_METERED_PROXY": "false",
            }
        )
        environment["WHOSCORED_SOURCE_POOL_SLOTS"] = "2"
        environment["WHOSCORED_PAID_GATEWAY_TOKEN"] = "g" * 32
    elif service == "flaresolverr_whoscored_paid":
        environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"] = "f" * 32
    elif service == "whoscored_paid_gateway":
        environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"] = "f" * 32
        environment["WHOSCORED_PAID_ALERT_HMAC_SECRET"] = "h" * 32
        environment["WHOSCORED_PAID_ALERT_SECRET_PATH"] = (
            "/opt/airflow/secure/whoscored-alert-authority/paid-alert-secret.json"
        )
        environment["WHOSCORED_PAID_ALERT_BINDING_PATH"] = (
            "/opt/airflow/secure/whoscored-alert-authority/paid-alert-binding.json"
        )
        environment["WHOSCORED_PAID_GATEWAY_TOKEN"] = "g" * 32
        environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"] = "a" * 32
        environment["WHOSCORED_PROXY_CONTROL_TOKEN"] = "c" * 32
    elif service == "whoscored_proxy_filter":
        environment["PROXY_FILTER_CONTROL_TOKEN"] = "c" * 32
        environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"] = "a" * 32
        environment["WHOSCORED_PROXY_LEDGER_HMAC_SECRET"] = "l" * 32
    return environment


def _enable_metered_transfermarkt(environment: dict[str, str]) -> None:
    environment.update(
        {
            "TM_NATIVE_V2_ENABLED": "true",
            "TM_STANDING_POLICY_ENABLED": "true",
            "TM_PROXY_CONTROL_TOKEN": "t" * 64,
            "TM_PROXY_CONTROL_URL": "http://proxy_filter:8899",
            "TM_REQUIRE_METERED_PROXY": "true",
        }
    )


def _canonical(path: Path, value: object) -> None:
    path.write_bytes(admission._canonical_bytes(value))


def _provider_quota_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Path, Path, dict[str, object]]:
    screenshot = tmp_path / "provider-quota.png"
    screenshot.write_bytes(b"bounded external quota evidence")
    screenshot.chmod(0o600)
    observed = datetime.fromtimestamp(
        screenshot.stat().st_mtime, tz=timezone.utc
    ).replace(microsecond=0)
    document: dict[str, object] = {
        "schema_version": 1,
        "status": "active",
        "provider": "PROXYS.IO",
        "order_id": "38950",
        "plan": "Bronze",
        "quota_decimal_gb": "1.00",
        "remaining_decimal_gb": "1.00",
        "observed_at": observed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "screenshot_path": str(screenshot),
        "screenshot_sha256": hashlib.sha256(screenshot.read_bytes()).hexdigest(),
    }
    receipt = tmp_path / "provider-quota-receipt.json"
    _canonical(receipt, document)
    receipt.chmod(0o600)
    monkeypatch.setattr(
        admission, "_provider_receipt_now", lambda: observed + timedelta(hours=1)
    )
    return receipt, screenshot, document


def test_provider_quota_receipt_binds_fresh_protected_screenshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    receipt, screenshot, _document = _provider_quota_receipt(monkeypatch, tmp_path)

    projection = admission.validate_provider_quota_receipt(receipt)

    assert projection["provider"] == "PROXYS.IO"
    assert projection["order_id"] == "38950"
    assert projection["remaining_decimal_gb"] == "1.00"
    assert projection["receipt_path"] == str(receipt)
    assert projection["screenshot_path"] == str(screenshot)


@pytest.mark.parametrize("mutation", ["digest", "stale", "writable"])
def test_provider_quota_receipt_rejects_unbound_or_stale_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutation: str
) -> None:
    receipt, screenshot, document = _provider_quota_receipt(monkeypatch, tmp_path)
    if mutation == "digest":
        document["screenshot_sha256"] = "0" * 64
        _canonical(receipt, document)
    elif mutation == "stale":
        observed = datetime.fromisoformat(
            str(document["observed_at"]).replace("Z", "+00:00")
        )
        monkeypatch.setattr(
            admission,
            "_provider_receipt_now",
            lambda: (
                observed
                + admission.MAX_PROVIDER_QUOTA_RECEIPT_AGE
                + timedelta(seconds=1)
            ),
        )
    else:
        screenshot.chmod(0o620)

    with pytest.raises(admission.AdmissionError, match="digest|stale|protected"):
        admission.validate_provider_quota_receipt(receipt)


def _deployment(
    *,
    payloads: Mapping[str, str] = PAYLOADS,
    bindings: Mapping[str, str] = BINDINGS,
) -> dict[str, object]:
    return {
        "images": [
            {
                "final_image": bindings[service],
                "payload_image_id": payloads[service],
                "service": service,
            }
            for service in sorted(payloads)
        ],
        "provenance_manifest_sha256": SHA_D,
        "schema_version": 1,
        "status": "ready-v1",
    }


def _discovery(
    payloads: Mapping[str, str] = PAYLOADS,
    deployment: Mapping[str, object] | None = None,
) -> SimpleNamespace:
    selected = dict(deployment or _deployment(payloads=payloads))
    images = selected["images"]
    assert isinstance(images, list)
    return SimpleNamespace(
        deployment_attestation=selected,
        deployment_attestation_raw=admission._canonical_bytes(selected),
        deployment_final_images={
            str(record["service"]): str(record["final_image"]) for record in images
        },
        records={
            "local_images": [
                {"payload_image_id": payloads[service], "service": service}
                for service in sorted(payloads)
            ]
        },
    )


def _validated_bindings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    deployment: object | None = None,
    *,
    payloads: Mapping[str, str] = PAYLOADS,
) -> tuple[Path, dict[str, str]]:
    deployment_path = tmp_path / "deployment.json"
    _canonical(deployment_path, deployment or _deployment(payloads=payloads))
    monkeypatch.setattr(
        admission.provenance,
        "validate",
        lambda *_args, **_kwargs: _discovery(
            payloads, deployment if isinstance(deployment, dict) else None
        ),
    )
    bindings = admission.validate_bindings(
        root=tmp_path,
        attestation_path=tmp_path / "attestation.json",
        manifest_path=tmp_path / "manifest.json",
        deployment_attestation_path=deployment_path,
    )
    return deployment_path, bindings


def test_generate_override_is_digest_only_atomic_and_never_overwrites(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _, bindings = _validated_bindings(monkeypatch, tmp_path)
    output = tmp_path / "production-digests.yaml"

    admission.write_new_regular_file(output, admission.compose_override_bytes(bindings))

    assert output.read_bytes() == (
        b"services:\n"
        b"  airflow-scheduler:\n"
        b"    build: !reset null\n"
        b'    image: "registry.example/scheduler@sha256:' + SHA_A.encode() + b'"\n'
        b"  flaresolverr:\n"
        b"    build: !reset null\n"
        b'    image: "registry.example/flaresolverr@sha256:' + SHA_B.encode() + b'"\n'
        b"  flaresolverr_whoscored_paid:\n"
        b"    build: !reset null\n"
        b'    image: "registry.example/flaresolverr@sha256:' + SHA_B.encode() + b'"\n'
        b"  whoscored_paid_gateway:\n"
        b"    build: !reset null\n"
        b'    image: "registry.example/proxy@sha256:' + SHA_C.encode() + b'"\n'
        b"  whoscored_proxy_filter:\n"
        b"    build: !reset null\n"
        b'    image: "registry.example/proxy@sha256:' + SHA_C.encode() + b'"\n'
    )
    assert output.stat().st_mode & 0o777 == 0o600
    with pytest.raises(admission.AdmissionError, match="will not be overwritten"):
        admission.write_new_regular_file(output, b"replacement")
    assert output.read_bytes().startswith(b"services:\n")


@pytest.mark.parametrize("existing_kind", ["symlink", "directory"])
def test_generate_override_rejects_nonregular_existing_output(
    tmp_path: Path, existing_kind: str
) -> None:
    output = tmp_path / "production-digests.yaml"
    if existing_kind == "symlink":
        target = tmp_path / "target"
        target.write_text("untouched", encoding="utf-8")
        output.symlink_to(target)
    else:
        output.mkdir()

    with pytest.raises(admission.AdmissionError, match="will not be overwritten"):
        admission.write_new_regular_file(output, b"new")


@pytest.mark.parametrize("kind", ["symlink-ancestor", "writable-parent"])
def test_generate_override_rejects_unsafe_output_ancestors(
    tmp_path: Path, kind: str
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    if kind == "symlink-ancestor":
        parent = tmp_path / "redirect"
        parent.symlink_to(real, target_is_directory=True)
    else:
        parent = real
        parent.chmod(0o777)
    with pytest.raises(admission.AdmissionError, match="unsafe|symlinked"):
        admission.write_new_regular_file(parent / "override.yaml", b"protected")


def test_deployment_attestation_rejects_duplicate_json_keys(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    deployment = tmp_path / "deployment.json"
    deployment.write_text(
        '{"images":[],"images":[],"provenance_manifest_sha256":"'
        + SHA_A
        + '","schema_version":1,"status":"ready-v1"}\n',
        encoding="utf-8",
    )
    called = False

    def validate(*_args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        admission.provenance._load_canonical_object(
            kwargs["deployment_attestation_path"],
            label="deployment attestation",
            protected=True,
        )
        return _discovery()

    monkeypatch.setattr(admission.provenance, "validate", validate)
    with pytest.raises(admission.AdmissionError, match="canonical JSON"):
        admission.validate_bindings(
            root=tmp_path,
            attestation_path=tmp_path / "attestation.json",
            manifest_path=tmp_path / "manifest.json",
            deployment_attestation_path=deployment,
        )
    assert called


@pytest.mark.parametrize("kind", ["symlink", "directory"])
def test_deployment_attestation_must_be_a_nonsymlink_regular_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str
) -> None:
    deployment = tmp_path / "deployment.json"
    if kind == "symlink":
        target = tmp_path / "real.json"
        _canonical(target, _deployment())
        deployment.symlink_to(target)
    else:
        deployment.mkdir()

    def validate(*_args: object, **kwargs: object) -> object:
        admission.provenance.read_protected_regular_file(
            kwargs["deployment_attestation_path"], label="deployment attestation"
        )
        return _discovery()

    monkeypatch.setattr(admission.provenance, "validate", validate)
    with pytest.raises(admission.AdmissionError, match="missing|protected regular"):
        admission.validate_bindings(
            root=tmp_path,
            attestation_path=tmp_path / "attestation.json",
            manifest_path=tmp_path / "manifest.json",
            deployment_attestation_path=deployment,
        )


@pytest.mark.parametrize("mutation", ["missing", "extra", "mutable"])
def test_deployment_attestation_rejects_service_or_digest_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutation: str
) -> None:
    deployment = _deployment()
    records = deployment["images"]
    assert isinstance(records, list)
    if mutation == "missing":
        records.pop()
    elif mutation == "extra":
        records.insert(
            2,
            {
                "final_image": f"registry.example/extra@sha256:{SHA_D}",
                "payload_image_id": f"sha256:{SHA_D}",
                "service": "other",
            },
        )
    else:
        records[0]["final_image"] = "registry.example/scheduler:latest"
    path = tmp_path / "deployment.json"
    _canonical(path, deployment)
    monkeypatch.setattr(
        admission.provenance,
        "validate",
        lambda *_args, **_kwargs: _discovery(deployment=deployment),
    )
    with pytest.raises(
        admission.AdmissionError, match="extra|missing|mutable|duplicated"
    ):
        admission.validate_bindings(
            root=tmp_path,
            attestation_path=tmp_path / "attestation.json",
            manifest_path=tmp_path / "manifest.json",
            deployment_attestation_path=path,
        )


def _rendered(bindings: Mapping[str, str] = BINDINGS) -> dict[str, object]:
    commands = {
        "airflow-scheduler": ["scheduler"],
        "flaresolverr": None,
        "flaresolverr_whoscored_paid": None,
        "whoscored_paid_gateway": list(
            admission._EXPECTED_COMMANDS["whoscored_paid_gateway"]
        ),
        "whoscored_proxy_filter": list(PROXY_COMMAND),
    }
    duration = {
        3_000_000_000: "3s",
        5_000_000_000: "5s",
        10_000_000_000: "10s",
        30_000_000_000: "30s",
        60_000_000_000: "1m0s",
    }
    rendered = {
        "networks": json.loads(json.dumps(admission._EXPECTED_NETWORK_DEFINITIONS)),
        "volumes": {"soccerdata_cache": {"name": "soccerdata_cache"}},
        "services": {
            service: {
                "cap_add": sorted(admission._SECURITY_POLICY[service]["cap_add"]),
                "cap_drop": ["ALL"],
                "command": commands[service],
                "container_name": service,
                "deploy": json.loads(json.dumps(admission._EXPECTED_DEPLOY[service])),
                "entrypoint": None,
                "environment": _rendered_environment(service),
                "image": bindings[service],
                "networks": admission._EXPECTED_NETWORKS[service],
                "read_only": admission._SECURITY_POLICY[service]["read_only"],
                "restart": "unless-stopped",
                "security_opt": list(admission._EXPECTED_SECURITY_OPT),
                "healthcheck": {
                    "interval": duration[
                        admission._EXPECTED_HEALTHCHECKS[service]["Interval"]
                    ],
                    "retries": admission._EXPECTED_HEALTHCHECKS[service]["Retries"],
                    "start_period": duration[
                        admission._EXPECTED_HEALTHCHECKS[service]["StartPeriod"]
                    ],
                    "test": list(admission._EXPECTED_HEALTHCHECKS[service]["Test"]),
                    "timeout": duration[
                        admission._EXPECTED_HEALTHCHECKS[service]["Timeout"]
                    ],
                },
                "tmpfs": [
                    target + ":" + ",".join(sorted(options))
                    for target, options in admission._ALLOWED_TMPFS[service].items()
                ]
                or None,
                "volumes": [
                    {
                        "source": (
                            "soccerdata_cache"
                            if kind == "volume"
                            else "/release/source" + target
                        ),
                        "target": target,
                        "type": kind,
                        kind: {"create_host_path": False} if kind == "bind" else {},
                        **({"read_only": True} if read_only else {}),
                    }
                    for target, (kind, read_only) in admission._ALLOWED_VOLUME_TARGETS[
                        service
                    ].items()
                ]
                or None,
            }
            for service in admission.PROTECTED_SERVICES
        },
    }
    services = rendered["services"]
    scheduler = services["airflow-scheduler"]
    scheduler.pop("cap_add")
    scheduler.pop("tmpfs")
    scheduler.pop("read_only")
    scheduler.update(
        {
            "depends_on": json.loads(
                json.dumps(admission._EXPECTED_DEPENDS_ON["airflow-scheduler"])
            ),
            "memswap_limit": "17179869184",
            "shm_size": "536870912",
        }
    )
    flaresolverr = services["flaresolverr"]
    flaresolverr.pop("cap_add")
    flaresolverr.pop("volumes")
    flaresolverr.update(
        {
            "ports": [
                {
                    "host_ip": "127.0.0.1",
                    "mode": "ingress",
                    "protocol": "tcp",
                    "published": "8191",
                    "target": 8191,
                }
            ],
            "shm_size": "1073741824",
        }
    )
    paid_flaresolverr = services["flaresolverr_whoscored_paid"]
    paid_flaresolverr.pop("cap_add")
    paid_flaresolverr.pop("volumes")
    paid_flaresolverr.update(
        {
            "profiles": ["whoscored-paid"],
            "shm_size": "1073741824",
        }
    )
    gateway = services["whoscored_paid_gateway"]
    gateway.pop("cap_add")
    gateway.update(
        {
            "depends_on": json.loads(
                json.dumps(admission._EXPECTED_DEPENDS_ON["whoscored_paid_gateway"])
            ),
            "profiles": ["whoscored-paid"],
        }
    )
    proxy = services["whoscored_proxy_filter"]
    proxy.pop("cap_add")
    proxy.update(
        {
            "depends_on": json.loads(
                json.dumps(admission._EXPECTED_DEPENDS_ON["whoscored_proxy_filter"])
            ),
            "profiles": ["whoscored-paid"],
        }
    )
    return rendered


def _bind_volume(
    rendered: Mapping[str, object], *, service: str, target: str
) -> dict[str, object]:
    services = rendered["services"]
    assert isinstance(services, dict)
    model = services[service]
    assert isinstance(model, dict)
    volumes = model["volumes"]
    assert isinstance(volumes, list)
    matches = [item for item in volumes if item.get("target") == target]
    assert len(matches) == 1
    return matches[0]


def _materialize_bind_sources(rendered: Mapping[str, object], tmp_path: Path) -> Path:
    root = tmp_path / "release"
    root.mkdir()
    for targets in admission._RELEASE_BIND_TARGETS.values():
        for relative in targets.values():
            path = root / relative
            if relative.endswith(".py"):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# protected\n", encoding="utf-8")
                path.chmod(0o644)
            else:
                path.mkdir(parents=True, exist_ok=True)
                path.chmod(0o755)
    for service, targets in admission._RELEASE_BIND_TARGETS.items():
        for target, relative in targets.items():
            _bind_volume(rendered, service=service, target=target)["source"] = str(
                root / relative
            )
    host = tmp_path / "host"
    host.mkdir()
    writable: dict[str, Path] = {}
    for name in ("logs", "gateway-state", "filter-state"):
        path = host / name
        path.mkdir()
        path.chmod(0o770)
        writable[name] = path
    approvals = host / "approvals"
    approvals.mkdir()
    approvals.chmod(0o700)
    alert_authority = host / "alert-authority"
    alert_authority.mkdir()
    alert_authority.chmod(0o700)
    proxy_file = host / "proxys.txt"
    proxy_file.write_text("127.0.0.1:8080\n", encoding="utf-8")
    proxy_file.chmod(0o600)
    assignments = {
        ("airflow-scheduler", "/opt/airflow/logs"): writable["logs"],
        ("airflow-scheduler", "/opt/airflow/proxys.txt"): proxy_file,
        (
            "airflow-scheduler",
            "/opt/airflow/secure/whoscored-approvals",
        ): approvals,
        (
            "airflow-scheduler",
            "/opt/airflow/state/whoscored-proxy-filter",
        ): writable["filter-state"],
        (
            "whoscored_paid_gateway",
            "/opt/airflow/state/whoscored-paid-gateway",
        ): writable["gateway-state"],
        (
            "whoscored_paid_gateway",
            "/opt/airflow/secure/whoscored-alert-authority",
        ): alert_authority,
        (
            "whoscored_proxy_filter",
            "/opt/airflow/state/whoscored-proxy-filter",
        ): writable["filter-state"],
    }
    for (service, target), source in assignments.items():
        _bind_volume(rendered, service=service, target=target)["source"] = str(source)
    return root


def test_bind_source_policy_requires_preexisting_separate_protected_paths(
    tmp_path: Path,
) -> None:
    rendered = _rendered()
    root = _materialize_bind_sources(rendered, tmp_path)
    projections = admission.verify_rendered_compose(rendered, BINDINGS)

    admission._validate_bind_source_policy(projections, root=root)

    filter_state = _bind_volume(
        rendered,
        service="whoscored_proxy_filter",
        target="/opt/airflow/state/whoscored-proxy-filter",
    )["source"]
    _bind_volume(
        rendered,
        service="whoscored_paid_gateway",
        target="/opt/airflow/state/whoscored-paid-gateway",
    )["source"] = filter_state
    projections = admission.verify_rendered_compose(rendered, BINDINGS)
    with pytest.raises(admission.AdmissionError, match="alias or nest"):
        admission._validate_bind_source_policy(projections, root=root)


def test_bind_source_policy_rejects_auto_create_and_writable_release_code(
    tmp_path: Path,
) -> None:
    rendered = _rendered()
    root = _materialize_bind_sources(rendered, tmp_path)
    dags = _bind_volume(
        rendered, service="airflow-scheduler", target="/opt/airflow/dags"
    )
    dags["bind"] = {"create_host_path": True}
    with pytest.raises(admission.AdmissionError, match="volume options"):
        admission.verify_rendered_compose(rendered, BINDINGS)

    dags["bind"] = {"create_host_path": False}
    (root / "dags").chmod(0o775)
    projections = admission.verify_rendered_compose(rendered, BINDINGS)
    with pytest.raises(admission.AdmissionError, match="protected directory"):
        admission._validate_bind_source_policy(projections, root=root)


def test_rendered_compose_binds_all_protected_services(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rendered.json"
    path.write_text(json.dumps(_rendered()), encoding="utf-8")
    rendered = json.loads(path.read_text(encoding="utf-8"))
    projections = admission.verify_rendered_compose(rendered, BINDINGS)
    assert set(projections) == set(admission.PROTECTED_SERVICES)


def test_helper_renders_fixed_file_set_and_captures_each_compose_config_hash(
    tmp_path: Path,
) -> None:
    root = tmp_path / "release"
    root.mkdir()
    base = root / "compose.yaml"
    supervised = root / "compose.seaweedfs-supervised.yaml"
    override = tmp_path / "digest-only.yaml"
    env_files = tuple(
        tmp_path / name for name in ("platform.env", "ws.env", "proxy.env")
    )
    for path in (base, supervised, *env_files):
        path.write_text("# admission input\n", encoding="utf-8")
    override.write_bytes(admission.compose_override_bytes(BINDINGS))
    calls: list[tuple[str, ...]] = []

    def runner(arguments: Sequence[str]) -> bytes:
        call = tuple(arguments)
        calls.append(call)
        if call[-3:] == ("config", "--format", "json"):
            return json.dumps(_rendered()).encode()
        if call[-3:-1] == ("config", "--hash"):
            return f"{call[-1]} {SHA_D}\n".encode("ascii")
        raise AssertionError(call)

    projections, hashes, files, rendered = admission.render_attested_compose(
        BINDINGS,
        root=root,
        override_path=override,
        env_files=env_files,
        project="data-platform",
        runner=runner,
    )

    assert set(projections) == set(BINDINGS)
    assert hashes == CONFIG_HASHES
    assert files == (base, supervised, override)
    assert rendered == _rendered()
    expected_prefix = (
        "compose",
        "--project-name",
        "data-platform",
        "--env-file",
        str(env_files[0]),
        "--env-file",
        str(env_files[1]),
        "--env-file",
        str(env_files[2]),
        "--profile",
        "whoscored-paid",
        "--file",
        str(base),
        "--file",
        str(supervised),
        "--file",
        str(override),
    )
    assert len(calls) == 1 + len(admission.PROTECTED_SERVICES)
    assert all(call[: len(expected_prefix)] == expected_prefix for call in calls)


def test_helper_rejects_compose_input_changed_during_render(tmp_path: Path) -> None:
    root = tmp_path / "release"
    root.mkdir()
    for name in ("compose.yaml", "compose.seaweedfs-supervised.yaml"):
        (root / name).write_text("# fixed\n", encoding="utf-8")
    override = tmp_path / "digest-only.yaml"
    override.write_bytes(admission.compose_override_bytes(BINDINGS))
    env_file = tmp_path / "platform.env"
    env_file.write_text("FIXED=one\n", encoding="utf-8")

    def runner(arguments: Sequence[str]) -> bytes:
        assert tuple(arguments)[-3:] == ("config", "--format", "json")
        env_file.write_text("FIXED=attacker\n", encoding="utf-8")
        return json.dumps(_rendered()).encode()

    with pytest.raises(admission.AdmissionError, match="input changed"):
        admission.render_attested_compose(
            BINDINGS,
            root=root,
            override_path=override,
            env_files=(env_file,),
            project="data-platform",
            runner=runner,
        )


@pytest.mark.parametrize("mutation", ["wrong-image", "build", "entrypoint", "missing"])
def test_rendered_compose_rejects_late_override_bypasses(
    tmp_path: Path, mutation: str
) -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    scheduler = services["airflow-scheduler"]
    assert isinstance(scheduler, dict)
    if mutation == "wrong-image":
        scheduler["image"] = "registry.example/scheduler:latest"
    elif mutation == "build":
        scheduler["build"] = {"context": "."}
    elif mutation == "entrypoint":
        scheduler["entrypoint"] = ["/bin/sh"]
    else:
        del services["flaresolverr"]
    path = tmp_path / "rendered.json"
    path.write_text(json.dumps(rendered), encoding="utf-8")
    with pytest.raises(
        admission.AdmissionError, match="differs|build|entrypoint|omits"
    ):
        admission.verify_rendered_compose(rendered, BINDINGS)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("lifecycle", "lifecycle hooks"),
        ("host-pid", "unmodeled fields"),
        ("capability", "unmodeled fields"),
        ("loader-env", "loader controls"),
        ("dependency", "dependency policy"),
        ("resource", "resource policy"),
        ("network", "network definitions"),
        ("child-shadow", "shadows image trust path"),
        ("executor", "security environment"),
        ("gateway-url", "security environment"),
        ("paid-url", "environment names"),
        ("control-url", "environment names"),
        ("scraper-python", "security environment"),
        ("proxy-fallback", "security environment"),
        ("http-proxy", "loader controls"),
        ("path", "environment names"),
    ],
)
def test_rendered_compose_rejects_unmodeled_execution_controls(
    mutation: str, message: str
) -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    scheduler = services["airflow-scheduler"]
    assert isinstance(scheduler, dict)
    if mutation == "lifecycle":
        scheduler["post_start"] = ["/bin/sh", "-c", "touch /tmp/bypass"]
    elif mutation == "host-pid":
        scheduler["pid"] = "host"
    elif mutation == "capability":
        scheduler["cap_add"] = ["SYS_PTRACE"]
    elif mutation == "loader-env":
        environment = scheduler["environment"]
        assert isinstance(environment, dict)
        environment["LD_AUDIT"] = "/tmp/attacker.so"
    elif mutation == "dependency":
        scheduler["depends_on"] = {}
    elif mutation == "resource":
        scheduler["deploy"] = {"replicas": 2}
    elif mutation == "network":
        networks = rendered["networks"]
        assert isinstance(networks, dict)
        networks["backend"] = {"driver": "host", "name": "host"}
    elif mutation == "child-shadow":
        volumes = scheduler["volumes"]
        assert isinstance(volumes, list)
        volume = volumes[0]
        assert isinstance(volume, dict)
        volume["target"] = (
            "/usr/local/share/whoscored/build-provenance-attestation.json"
        )
    elif mutation in {
        "executor",
        "gateway-url",
        "paid-url",
        "control-url",
        "scraper-python",
        "http-proxy",
        "path",
    }:
        environment = scheduler["environment"]
        assert isinstance(environment, dict)
        name, value = {
            "executor": ("AIRFLOW__CORE__EXECUTOR", "CeleryExecutor"),
            "gateway-url": (
                "WHOSCORED_PAID_GATEWAY_URL",
                "http://attacker:8898",
            ),
            "paid-url": ("WHOSCORED_PAID_PROXY_URL", "http://attacker:9999"),
            "control-url": ("WHOSCORED_PROXY_CONTROL_URL", "http://attacker"),
            "scraper-python": (
                "WHOSCORED_SCRAPER_PYTHON",
                "/opt/airflow/scripts/python",
            ),
            "http-proxy": ("HTTPS_PROXY", "http://unmetered.invalid:8080"),
            "path": ("PATH", "/opt/airflow/scripts:/usr/bin"),
        }[mutation]
        environment[name] = value
    else:
        proxy = services["whoscored_proxy_filter"]
        assert isinstance(proxy, dict)
        environment = proxy["environment"]
        assert isinstance(environment, dict)
        environment["PROXY_FILTER_ALLOW_FILE_FALLBACK"] = "true"
    with pytest.raises(admission.AdmissionError, match=message):
        admission.verify_rendered_compose(rendered, BINDINGS)


def test_scheduler_admission_requires_gateway_token_and_forbids_raw_origins():
    environment = _rendered_environment("airflow-scheduler")

    assert environment["WHOSCORED_PAID_GATEWAY_URL"] == (
        "http://whoscored_paid_gateway:8898"
    )
    assert "WHOSCORED_PAID_PROXY_URL" not in environment
    assert "WHOSCORED_PROXY_CONTROL_URL" not in environment
    assert "WHOSCORED_PROXY_CONTROL_TOKEN" not in environment
    assert "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET" not in environment
    assert "WHOSCORED_PAID_ALERT_HMAC_SECRET" not in environment
    assert "WHOSCORED_PAID_ALERT_SECRET_PATH" not in environment
    assert "WHOSCORED_PAID_ALERT_BINDING_PATH" not in environment
    assert "WHOSCORED_PAID_ALERT_RECEIPT_ROOT" not in environment
    for token in ("", "g" * 31):
        candidate = {**environment, "WHOSCORED_PAID_GATEWAY_TOKEN": token}
        with pytest.raises(admission.AdmissionError, match="paid-gateway token"):
            admission._validate_rendered_environment(
                candidate,
                service="airflow-scheduler",
            )
    for forbidden in (
        "WHOSCORED_PROXY_CONTROL_TOKEN",
        "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
        "WHOSCORED_PAID_ALERT_HMAC_SECRET",
        "WHOSCORED_PAID_ALERT_SECRET_PATH",
        "WHOSCORED_PAID_ALERT_BINDING_PATH",
        "WHOSCORED_PAID_ALERT_RECEIPT_ROOT",
    ):
        with pytest.raises(admission.AdmissionError, match="environment names"):
            admission._validate_rendered_environment(
                {**environment, forbidden: "x" * 32},
                service="airflow-scheduler",
            )


def test_scheduler_admission_preserves_metered_transfermarkt_controls():
    environment = _rendered_environment("airflow-scheduler")
    _enable_metered_transfermarkt(environment)

    admission._validate_rendered_environment(
        environment,
        service="airflow-scheduler",
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("TM_STANDING_POLICY_ENABLED", "false"),
        ("TM_REQUIRE_METERED_PROXY", "false"),
        ("TM_PROXY_CONTROL_URL", "http://attacker:8899"),
        ("TM_PROXY_CONTROL_TOKEN", "short"),
    ],
)
def test_scheduler_admission_rejects_unsafe_enabled_transfermarkt_controls(
    name: str,
    value: str,
):
    environment = _rendered_environment("airflow-scheduler")
    _enable_metered_transfermarkt(environment)
    environment[name] = value

    with pytest.raises(admission.AdmissionError, match="paid controls"):
        admission._validate_rendered_environment(
            environment,
            service="airflow-scheduler",
        )


def test_paid_boundary_is_five_service_isolated_and_credential_bound():
    rendered = _rendered()
    projections = admission.verify_rendered_compose(rendered, BINDINGS)

    assert set(admission.PROTECTED_SERVICES) == {
        "airflow-scheduler",
        "flaresolverr",
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    }
    assert set(projections["airflow-scheduler"]["network_names"]) >= {
        "dp-whoscored-paid-api"
    }
    assert projections["flaresolverr_whoscored_paid"]["network_names"] == (
        "dp-whoscored-paid-browser",
    )
    assert set(projections["whoscored_paid_gateway"]["network_names"]) == {
        "dp-whoscored-paid-api",
        "dp-whoscored-paid-browser",
        "dp-whoscored-paid-direct-egress",
    }
    assert set(projections["whoscored_proxy_filter"]["network_names"]) == {
        "dp-whoscored-paid-browser",
        "dp-whoscored-paid-provider-egress",
    }
    assert projections["flaresolverr_whoscored_paid"]["port_bindings"] == {}
    assert projections["whoscored_paid_gateway"]["port_bindings"] == {}
    gateway_volumes = {
        target: (source, read_only)
        for _kind, source, target, read_only in projections["whoscored_paid_gateway"][
            "volumes"
        ]
    }
    assert gateway_volumes["/opt/airflow/state/whoscored-paid-gateway"][1] is False
    assert gateway_volumes["/opt/airflow/secure/whoscored-alert-authority"][1] is True
    services = rendered["services"]
    assert isinstance(services, dict)
    gateway = services["whoscored_paid_gateway"]
    assert isinstance(gateway, dict)
    gateway_environment = gateway["environment"]
    assert isinstance(gateway_environment, dict)
    gateway_environment["WHOSCORED_PAID_GATEWAY_TOKEN"] = "x" * 32
    with pytest.raises(admission.AdmissionError, match="credentials differ"):
        admission.verify_rendered_compose(rendered, BINDINGS)

    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    gateway = services["whoscored_paid_gateway"]
    assert isinstance(gateway, dict)
    gateway_environment = gateway["environment"]
    assert isinstance(gateway_environment, dict)
    gateway_environment["WHOSCORED_PAID_ALERT_HMAC_SECRET"] = "f" * 32
    with pytest.raises(admission.AdmissionError, match="not distinct"):
        admission.verify_rendered_compose(rendered, BINDINGS)


def test_paid_gateway_command_endpoints_are_exact():
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    gateway = services["whoscored_paid_gateway"]
    assert isinstance(gateway, dict)
    command = gateway["command"]
    assert isinstance(command, list)
    command[command.index("--proxy-control-url") + 1] = "http://attacker:8899"

    with pytest.raises(admission.AdmissionError, match="paid-gateway command"):
        admission.verify_rendered_compose(rendered, BINDINGS)


def test_rendered_proxy_accepts_850_mib_daily_safety_cap() -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    proxy = services["whoscored_proxy_filter"]
    assert isinstance(proxy, dict)
    command = proxy["command"]
    assert isinstance(command, list)
    command[command.index("--daily-budget-mb") + 1] = "850"

    projection = admission.verify_rendered_compose(rendered, BINDINGS)

    assert projection["whoscored_proxy_filter"]["command"] == tuple(command)


@pytest.mark.parametrize("value", ["851", "1000"])
def test_rendered_proxy_rejects_daily_budget_above_850_mib(value: str) -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    proxy = services["whoscored_proxy_filter"]
    assert isinstance(proxy, dict)
    command = proxy["command"]
    assert isinstance(command, list)
    command[command.index("--daily-budget-mb") + 1] = value

    with pytest.raises(admission.AdmissionError, match="daily-budget-mb"):
        admission.verify_rendered_compose(rendered, BINDINGS)


@pytest.mark.parametrize("value", ["1", "999999999", "1000000000"])
def test_rendered_proxy_accepts_positive_capped_dagrun_budget(value: str) -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    proxy = services["whoscored_proxy_filter"]
    assert isinstance(proxy, dict)
    command = proxy["command"]
    assert isinstance(command, list)
    command[command.index("--dagrun-budget-bytes") + 1] = value
    projection = admission.verify_rendered_compose(rendered, BINDINGS)
    assert projection["whoscored_proxy_filter"]["command"] == tuple(command)


@pytest.mark.parametrize("value", ["0", "1000000001", "${ATTACKER}"])
def test_rendered_proxy_rejects_invalid_dagrun_budget(value: str) -> None:
    rendered = _rendered()
    services = rendered["services"]
    assert isinstance(services, dict)
    proxy = services["whoscored_proxy_filter"]
    assert isinstance(proxy, dict)
    command = proxy["command"]
    assert isinstance(command, list)
    command[command.index("--dagrun-budget-bytes") + 1] = value
    with pytest.raises(admission.AdmissionError, match="dagrun-budget-bytes"):
        admission.verify_rendered_compose(rendered, BINDINGS)


def test_checked_in_compose_model_matches_admission_policy(tmp_path: Path) -> None:
    root = Path(__file__).absolute().parents[3]
    override = tmp_path / "digest-only.yaml"
    override.write_bytes(admission.compose_override_bytes(BINDINGS))
    environment = {
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "ALERT_ENV": "prod",
        "JUPYTER_PUBLIC_HOST": "jupyter.ci.invalid",
        "KC_PUBLIC_URL": "https://auth.ci.invalid",
        "LANG": "C.UTF-8",
        "LAKEKEEPER_DB_PASSWORD": "ci-not-a-secret",
        "LAKEKEEPER_PG_ENCRYPTION_KEY": "0" * 32,
        "LC_ALL": "C.UTF-8",
        "OIDC_ISSUER": "https://ci.invalid",
        "PATH": "/usr/bin:/bin",
        "PUBLIC_IP": "127.0.0.1",
        "SEAWEEDFS_DATA_VOLUME_NAME": "seaweedfs_data",
        "SEAWEEDFS_VOLUME_SIZE_LIMIT_MB": "1024",
        "SOFASCORE_PROXY_CONTROL_TOKEN": "b" * 64,
        "TRINO_PUBLIC_HOST": "trino.ci.invalid",
        "WHOSCORED_PROXY_APPROVAL_HOST_DIR": (
            "/var/lib/data-platform-football/whoscored-approvals"
        ),
        "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET": "f" * 32,
        "WHOSCORED_PAID_GATEWAY_TOKEN": "g" * 32,
        "WHOSCORED_PAID_ALERT_HMAC_SECRET": "h" * 32,
        "WHOSCORED_PAID_ALERT_BINDING_PATH": (
            "/opt/airflow/secure/whoscored-alert-authority/paid-alert-binding.json"
        ),
        "WHOSCORED_PAID_ALERT_SECRET_PATH": (
            "/opt/airflow/secure/whoscored-alert-authority/paid-alert-secret.json"
        ),
        "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET": "a" * 32,
        "WHOSCORED_PROXY_LEDGER_HMAC_SECRET": "l" * 32,
        "WHOSCORED_PROXY_FILTER_CONTROL_TOKEN": "c" * 32,
    }
    calls: list[tuple[str, ...]] = []

    def runner(arguments: Sequence[str]) -> bytes:
        call = tuple(arguments)
        calls.append(call)
        assert call[-3:] == ("config", "--format", "json") or call[-3:-1] == (
            "config",
            "--hash",
        )
        result = subprocess.run(
            ("/usr/bin/docker", *call),
            env=environment,
            capture_output=True,
            check=False,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
        return result.stdout

    projections, hashes, files, rendered = admission.render_attested_compose(
        BINDINGS,
        root=root,
        override_path=override,
        env_files=(root / ".env.example",),
        project="data-platform",
        runner=runner,
    )

    assert set(projections) == set(admission.PROTECTED_SERVICES)
    assert set(hashes) == set(admission.PROTECTED_SERVICES)
    assert files[-1] == override
    rendered_services = rendered["services"]
    assert isinstance(rendered_services, dict)
    assert all("build" not in rendered_services[service] for service in BINDINGS)
    assert len(calls) == 1 + len(admission.PROTECTED_SERVICES)


def _docker_runner(
    mutation: Callable[[str, list[str], dict[str, Any], dict[str, Any]], None]
    | None = None,
) -> admission.DockerRunner:
    container_ids = {
        service: format(index + 1, "064x")
        for index, service in enumerate(admission.PROTECTED_SERVICES)
    }
    binding_image_ids = {
        binding: f"sha256:{format(index + 4, '064x')}"
        for index, binding in enumerate(sorted(set(BINDINGS.values())))
    }
    image_ids = {
        service: binding_image_ids[BINDINGS[service]]
        for service in admission.PROTECTED_SERVICES
    }
    network_ids = {
        definition["name"]: format(index + 10, "064x")
        for index, definition in enumerate(
            admission._EXPECTED_NETWORK_DEFINITIONS.values()
        )
    }
    network_subnets = {
        "dp-backend": ("172.18.0.0/16", "172.18.0.1"),
        "dp-frontend": ("172.19.0.0/16", "172.19.0.1"),
        "dp-storage": ("172.20.0.0/16", "172.20.0.1"),
        "dp-whoscored-paid-api": ("172.21.0.0/16", "172.21.0.1"),
        "dp-whoscored-paid-browser": ("172.22.0.0/16", "172.22.0.1"),
        "dp-whoscored-paid-direct-egress": ("172.23.0.0/16", "172.23.0.1"),
        "dp-whoscored-paid-provider-egress": ("172.24.0.0/16", "172.24.0.1"),
    }

    def runner(arguments: Sequence[str]) -> bytes:
        args = list(arguments)
        if args == ["info", "--format", "{{json .SecurityOptions}}"]:
            return json.dumps(
                [
                    "name=apparmor",
                    "name=cgroupns",
                    "name=seccomp,profile=builtin",
                ]
            ).encode()
        if args == list(
            admission._apparmor_probe_arguments(BINDINGS["airflow-scheduler"])
        ):
            return b"docker-default (enforce)\n"
        if args[:2] == ["network", "inspect"]:
            name = args[2]
            logical_name = next(
                logical
                for logical, definition in admission._EXPECTED_NETWORK_DEFINITIONS.items()
                if definition["name"] == name
            )
            subnet, gateway = network_subnets[name]
            network = {
                "Attachable": False,
                "Driver": "bridge",
                "EnableIPv4": True,
                "EnableIPv6": False,
                "IPAM": {
                    "Config": [{"Gateway": gateway, "IPRange": "", "Subnet": subnet}],
                    "Driver": "default",
                    "Options": None,
                },
                "Id": network_ids[name],
                "Ingress": False,
                "Internal": bool(
                    admission._EXPECTED_NETWORK_DEFINITIONS[logical_name].get(
                        "internal", False
                    )
                ),
                "Labels": {
                    "com.docker.compose.config-hash": SHA_D,
                    "com.docker.compose.network": logical_name,
                    "com.docker.compose.project": "data-platform",
                    "com.docker.compose.version": "2.40.3",
                },
                "Name": name,
                "Options": {},
                "Scope": "local",
            }
            return json.dumps([network]).encode()
        if args[:2] == ["volume", "inspect"]:
            assert args[2] == "soccerdata_cache"
            volume = {
                "Driver": "local",
                "Labels": {
                    "com.docker.compose.config-hash": SHA_D,
                    "com.docker.compose.project": "data-platform",
                    "com.docker.compose.version": "2.40.3",
                    "com.docker.compose.volume": "soccerdata_cache",
                },
                "Mountpoint": "/var/lib/docker/volumes/soccerdata_cache/_data",
                "Name": "soccerdata_cache",
                "Options": None,
                "Scope": "local",
            }
            return json.dumps([volume]).encode()
        if args[:3] == ["container", "ls", "--all"]:
            service_filter = next(
                item
                for item in args
                if item.startswith("label=com.docker.compose.service=")
            )
            service = service_filter.rsplit("=", 1)[1]
            ids = [container_ids[service]]
            container: dict[str, Any] = {}
            image: dict[str, Any] = {}
            if mutation is not None:
                mutation(service, ids, container, image)
            return ("\n".join(ids) + ("\n" if ids else "")).encode("ascii")
        if args[:2] == ["container", "inspect"]:
            service = next(
                name for name, value in container_ids.items() if value == args[2]
            )
            container = {
                "AppArmorProfile": "docker-default",
                "Config": {
                    "Cmd": list(EFFECTIVE_COMMANDS[service]),
                    "Entrypoint": list(admission._EXPECTED_ENTRYPOINTS[service]),
                    "Env": [
                        f"{name}={value}"
                        for name, value in {
                            **admission._EXPECTED_IMAGE_ENVIRONMENT[service],
                            **_rendered_environment(service),
                        }.items()
                    ],
                    "Healthcheck": {
                        key: (
                            [item.replace("$$", "$") for item in value]
                            if key == "Test"
                            else value
                        )
                        for key, value in admission._EXPECTED_HEALTHCHECKS[
                            service
                        ].items()
                    },
                    "Image": BINDINGS[service],
                    "Labels": {
                        "com.docker.compose.config-hash": CONFIG_HASHES[service],
                        "com.docker.compose.container-number": "1",
                        "com.docker.compose.depends_on": "",
                        "com.docker.compose.image": image_ids[service],
                        "com.docker.compose.oneoff": "False",
                        "com.docker.compose.project": "data-platform",
                        "com.docker.compose.project.config_files": ",".join(
                            str(path) for path in CONFIG_FILES
                        ),
                        "com.docker.compose.project.environment_file": ",".join(
                            str(path) for path in ENV_FILES
                        ),
                        "com.docker.compose.project.working_dir": "/release",
                        "com.docker.compose.service": service,
                        "com.docker.compose.version": "2.40.3",
                    },
                    "OpenStdin": False,
                    "Tty": False,
                    "User": admission._EXPECTED_IMAGE_USER[service],
                    "WorkingDir": admission._EXPECTED_WORKING_DIR[service],
                },
                "HostConfig": {
                    "AutoRemove": False,
                    "CapAdd": sorted(admission._SECURITY_POLICY[service]["cap_add"]),
                    "CapDrop": ["ALL"],
                    "DeviceRequests": None,
                    "Devices": [],
                    "CgroupnsMode": "private",
                    "IpcMode": "private",
                    "NetworkMode": admission._EXPECTED_NETWORK_MODE[service],
                    "OomKillDisable": False,
                    "OomScoreAdj": 0,
                    "PublishAllPorts": False,
                    "PortBindings": {
                        port: [dict(binding) for binding in bindings]
                        for port, bindings in admission._EXPECTED_PORT_BINDINGS[
                            service
                        ].items()
                    },
                    "Privileged": False,
                    "ReadonlyRootfs": admission._SECURITY_POLICY[service]["read_only"],
                    "RestartPolicy": {
                        "MaximumRetryCount": 0,
                        "Name": "unless-stopped",
                    },
                    "SecurityOpt": list(admission._EXPECTED_SECURITY_OPT),
                    "ShmSize": admission._EXPECTED_SHM_SIZE[service],
                    "Tmpfs": (
                        {
                            target: ",".join(sorted(options))
                            for target, options in admission._ALLOWED_TMPFS[
                                service
                            ].items()
                        }
                        or None
                    ),
                    "VolumesFrom": None,
                    "Runtime": "runc",
                    "Dns": None,
                    "DnsOptions": None,
                    "DnsSearch": None,
                    "ExtraHosts": [],
                    "Init": None,
                    "Links": None,
                    "LogConfig": {"Config": {}, "Type": "json-file"},
                    "MaskedPaths": list(admission._EXPECTED_MASKED_PATHS),
                    "ReadonlyPaths": list(admission._EXPECTED_READONLY_PATHS),
                    **admission._EXPECTED_CONTAINER_RESOURCES[service],
                },
                "Image": image_ids[service],
                "Mounts": [
                    {
                        "Destination": target,
                        "Name": "soccerdata_cache" if kind == "volume" else None,
                        "Driver": "local" if kind == "volume" else None,
                        "Mode": "ro" if read_only else "rw",
                        "Propagation": "" if kind == "volume" else "rprivate",
                        "RW": not read_only,
                        "Source": (
                            "/var/lib/docker/volumes/soccerdata_cache/_data"
                            if kind == "volume"
                            else "/release/source" + target
                        ),
                        "Type": kind,
                    }
                    for target, (kind, read_only) in admission._ALLOWED_VOLUME_TARGETS[
                        service
                    ].items()
                ],
                "NetworkSettings": {
                    "Networks": {
                        name: {
                            "Aliases": [service, service],
                            "DriverOpts": None,
                            "GlobalIPv6Address": "",
                            "GwPriority": 0,
                            "IPAddress": network_subnets[name][1][:-1] + str(index + 2),
                            "IPAMConfig": None,
                            "Links": None,
                            "MacAddress": f"02:42:ac:12:00:0{index + 2}",
                            "NetworkID": network_ids[name],
                        }
                        for index, name in enumerate(
                            admission._EXPECTED_NETWORK_DEFINITIONS[logical]["name"]
                            for logical in admission._EXPECTED_NETWORKS[service]
                        )
                    }
                },
                "State": {"Running": False, "Status": "created"},
            }
            if mutation is not None:
                mutation(service, [], container, {})
            return json.dumps([container]).encode()
        if args[:2] == ["image", "inspect"]:
            service = next(name for name, value in BINDINGS.items() if value == args[2])
            image = {
                "Config": {
                    "Entrypoint": list(admission._EXPECTED_ENTRYPOINTS[service]),
                    "Env": [
                        f"{name}={value}"
                        for name, value in admission._EXPECTED_IMAGE_ENVIRONMENT[
                            service
                        ].items()
                    ],
                    "Labels": {},
                    "User": admission._EXPECTED_IMAGE_USER[service],
                    "WorkingDir": admission._EXPECTED_WORKING_DIR[service],
                },
                "Id": image_ids[service],
                "RepoDigests": [BINDINGS[service]],
            }
            if mutation is not None:
                mutation(service, [], {}, image)
            return json.dumps([image]).encode()
        raise AssertionError(args)

    return runner


def test_post_create_verifies_container_and_digest_selected_image_identity() -> None:
    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    report = admission.verify_created_containers(
        BINDINGS,
        project="data-platform",
        selected_services=admission.PROTECTED_SERVICES,
        projections=projections,
        config_hashes=CONFIG_HASHES,
        config_files=CONFIG_FILES,
        env_files=ENV_FILES,
        runner=_docker_runner(),
    )
    assert report["status"] == "admitted-v1"
    assert report["apparmor_profile"] == "docker-default (enforce)"
    assert report["docker_security_options"] == [
        "name=apparmor",
        "name=cgroupns",
        "name=seccomp,profile=builtin",
    ]
    assert [record["service"] for record in report["images"]] == list(
        admission.PROTECTED_SERVICES
    )


def test_post_create_accepts_engine_29_empty_bind_mode_with_exact_request() -> None:
    def use_engine_29_mount_metadata(
        _service: str,
        _ids: list[str],
        container: dict[str, Any],
        _image: dict[str, Any],
    ) -> None:
        if not container:
            return
        requested = []
        for mount in container["Mounts"]:
            if mount["Type"] != "bind":
                continue
            mount["Mode"] = ""
            record = {
                "Type": "bind",
                "Source": mount["Source"],
                "Target": mount["Destination"],
                "BindOptions": {},
            }
            if mount["RW"] is False:
                record["ReadOnly"] = True
            requested.append(record)
        container["HostConfig"]["Mounts"] = requested

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    report = admission.verify_created_containers(
        BINDINGS,
        project="data-platform",
        selected_services=admission.PROTECTED_SERVICES,
        projections=projections,
        config_hashes=CONFIG_HASHES,
        config_files=CONFIG_FILES,
        env_files=ENV_FILES,
        runner=_docker_runner(use_engine_29_mount_metadata),
    )
    assert report["status"] == "admitted-v1"


@pytest.mark.parametrize(
    "drift",
    (
        "missing",
        "writable",
        "missing-options",
        "nonempty-options",
        "wrong-source",
        "wrong-target",
        "wrong-type",
        "noncanonical-mode",
    ),
)
def test_post_create_rejects_unproven_engine_29_empty_bind_mode(
    drift: str,
) -> None:
    def use_unproven_empty_mode(
        service: str,
        _ids: list[str],
        container: dict[str, Any],
        _image: dict[str, Any],
    ) -> None:
        if service != "airflow-scheduler" or not container:
            return
        mount = next(
            item
            for item in container["Mounts"]
            if item["Type"] == "bind" and item["RW"] is False
        )
        mount["Mode"] = ""
        if drift == "missing":
            return
        requested: dict[str, Any] = {
            "Type": "bind",
            "Source": mount["Source"],
            "Target": mount["Destination"],
            "ReadOnly": drift != "writable",
        }
        if drift != "missing-options":
            requested["BindOptions"] = {}
        if drift == "nonempty-options":
            requested["BindOptions"] = {"ReadOnlyNonRecursive": True}
        elif drift == "wrong-source":
            requested["Source"] += "-drift"
        elif drift == "wrong-target":
            requested["Target"] += "-drift"
        elif drift == "wrong-type":
            requested["Type"] = "volume"
        elif drift == "noncanonical-mode":
            mount["Mode"] = "readonly"
        container["HostConfig"]["Mounts"] = [requested]

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="mount mode differs"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=_docker_runner(use_unproven_empty_mode),
        )


def test_post_create_rejects_duplicate_requested_mount_target() -> None:
    def duplicate_requested_target(
        service: str,
        _ids: list[str],
        container: dict[str, Any],
        _image: dict[str, Any],
    ) -> None:
        if service != "airflow-scheduler" or not container:
            return
        mount = next(item for item in container["Mounts"] if item["Type"] == "bind")
        requested = {
            "Type": "bind",
            "Source": mount["Source"],
            "Target": mount["Destination"],
            "BindOptions": {},
        }
        container["HostConfig"]["Mounts"] = [requested, requested.copy()]

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="requested-mount identity"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=_docker_runner(duplicate_requested_target),
        )


def test_post_create_rejects_empty_volume_mode() -> None:
    def empty_volume_mode(
        service: str,
        _ids: list[str],
        container: dict[str, Any],
        _image: dict[str, Any],
    ) -> None:
        if service != "airflow-scheduler" or not container:
            return
        mount = next(item for item in container["Mounts"] if item["Type"] == "volume")
        mount["Mode"] = ""
        container["HostConfig"]["Mounts"] = [
            {
                "Type": "volume",
                "Source": mount["Name"],
                "Target": mount["Destination"],
                "VolumeOptions": {},
            }
        ]

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="mount mode differs"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=_docker_runner(empty_volume_mode),
        )


@pytest.mark.parametrize(
    ("name", "mutation", "message"),
    [
        (
            "duplicate-container",
            lambda service, ids, _container, _image: (
                ids.append(ids[0]) if service == "airflow-scheduler" and ids else None
            ),
            "exactly one",
        ),
        (
            "config-image",
            lambda service, _ids, container, _image: (
                container["Config"].update(
                    {"Image": "registry.example/scheduler:latest"}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "Config.Image",
        ),
        (
            "container-image-id",
            lambda service, _ids, container, _image: (
                container.update({"Image": f"sha256:{SHA_D}"})
                if service == "airflow-scheduler" and container
                else None
            ),
            "differs",
        ),
        (
            "repo-digest",
            lambda service, _ids, _container, image: (
                image.update({"RepoDigests": []})
                if service == "airflow-scheduler" and image
                else None
            ),
            "RepoDigest",
        ),
        (
            "already-running",
            lambda service, _ids, container, _image: (
                container["State"].update({"Running": True, "Status": "running"})
                if service == "airflow-scheduler" and container
                else None
            ),
            "started before",
        ),
        (
            "config-hash",
            lambda service, _ids, container, _image: (
                container["Config"]["Labels"].update(
                    {"com.docker.compose.config-hash": SHA_A}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "config hash",
        ),
        (
            "privileged",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"Privileged": True})
                if service == "airflow-scheduler" and container
                else None
            ),
            "privileged",
        ),
        (
            "capability",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"CapAdd": ["SYS_PTRACE"]})
                if service == "airflow-scheduler" and container
                else None
            ),
            "capability policy",
        ),
        (
            "apparmor-profile",
            lambda service, _ids, container, _image: (
                container.update({"AppArmorProfile": "unconfined"})
                if service == "airflow-scheduler" and container
                else None
            ),
            "AppArmor profile",
        ),
        (
            "seccomp-unconfined",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {
                        "SecurityOpt": [
                            "no-new-privileges:true",
                            "seccomp=unconfined",
                        ]
                    }
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "security options",
        ),
        (
            "entrypoint",
            lambda service, _ids, container, _image: (
                container["Config"].update({"Entrypoint": ["/bin/sh"]})
                if service == "airflow-scheduler" and container
                else None
            ),
            "Entrypoint",
        ),
        (
            "command",
            lambda service, _ids, container, _image: (
                container["Config"].update({"Cmd": ["bash"]})
                if service == "airflow-scheduler" and container
                else None
            ),
            "Cmd",
        ),
        (
            "shadow-mount",
            lambda service, _ids, container, _image: (
                container["Mounts"].append(
                    {
                        "Destination": "/usr/local/bin",
                        "Mode": "rw",
                        "RW": True,
                        "Source": "/tmp/forged",
                        "Type": "bind",
                    }
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "shadows",
        ),
        (
            "child-shadow-mount",
            lambda service, _ids, container, _image: (
                container["Mounts"].append(
                    {
                        "Destination": (
                            "/usr/local/share/whoscored/"
                            "build-provenance-attestation.json"
                        ),
                        "Mode": "ro",
                        "RW": False,
                        "Source": "/tmp/forged",
                        "Type": "bind",
                    }
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "shadows",
        ),
        (
            "image-loader-env",
            lambda service, _ids, _container, image: (
                image["Config"]["Env"].append("LD_AUDIT=/tmp/attacker.so")
                if service == "airflow-scheduler" and image
                else None
            ),
            "loader controls",
        ),
        (
            "network-alias",
            lambda service, _ids, container, _image: (
                next(iter(container["NetworkSettings"]["Networks"].values())).update(
                    {"Aliases": ["attacker"]}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "network aliases",
        ),
        (
            "device",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {"Devices": [{"PathOnHost": "/dev/kvm"}]}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "forbidden Devices",
        ),
        (
            "resource",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"Memory": 0})
                if service == "airflow-scheduler" and container
                else None
            ),
            "resource policy",
        ),
        (
            "restart-policy",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {"RestartPolicy": {"MaximumRetryCount": 0, "Name": "always"}}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "restart policy",
        ),
        (
            "extra-port",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {
                        "PortBindings": {
                            "80/tcp": [{"HostIp": "0.0.0.0", "HostPort": "80"}]
                        }
                    }
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "port bindings",
        ),
        (
            "publish-all-ports",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"PublishAllPorts": True})
                if service == "airflow-scheduler" and container
                else None
            ),
            "unmodeled ports",
        ),
        (
            "custom-dns",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"Dns": ["203.0.113.53"]})
                if service == "airflow-scheduler" and container
                else None
            ),
            "DNS/host-link",
        ),
        (
            "extra-host",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {"ExtraHosts": ["whoscored.com:203.0.113.10"]}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "DNS/host-link",
        ),
        (
            "remote-logging",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update(
                    {"LogConfig": {"Config": {}, "Type": "syslog"}}
                )
                if service == "airflow-scheduler" and container
                else None
            ),
            "logging policy",
        ),
        (
            "unmasked-proc",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"MaskedPaths": []})
                if service == "airflow-scheduler" and container
                else None
            ),
            "masked-path",
        ),
        (
            "docker-init",
            lambda service, _ids, container, _image: (
                container["HostConfig"].update({"Init": True})
                if service == "airflow-scheduler" and container
                else None
            ),
            "init policy",
        ),
        (
            "integration-label",
            lambda service, _ids, container, _image: (
                container["Config"]["Labels"].update({"traefik.enable": "true"})
                if service == "airflow-scheduler" and container
                else None
            ),
            "unexpected integration labels",
        ),
        (
            "root-with-group",
            lambda service, _ids, _container, image: (
                image["Config"].update({"User": "0:50000"})
                if service == "airflow-scheduler" and image
                else None
            ),
            "non-root image policy",
        ),
        (
            "image-path",
            lambda service, _ids, _container, image: (
                image["Config"].update(
                    {
                        "Env": [
                            "PATH=/opt/airflow/scripts:/usr/bin"
                            if item.startswith("PATH=")
                            else item
                            for item in image["Config"]["Env"]
                        ]
                    }
                )
                if service == "airflow-scheduler" and image
                else None
            ),
            "hardening environment",
        ),
        (
            "bind-propagation",
            lambda service, _ids, container, _image: (
                next(
                    mount for mount in container["Mounts"] if mount["Type"] == "bind"
                ).update({"Propagation": "rshared"})
                if service == "airflow-scheduler" and container
                else None
            ),
            "bind propagation",
        ),
    ],
)
def test_post_create_rejects_runtime_identity_bypasses(
    name: str,
    mutation: Callable[[str, list[str], dict[str, Any], dict[str, Any]], None],
    message: str,
) -> None:
    del name
    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match=message):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=admission.PROTECTED_SERVICES,
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=_docker_runner(mutation),
        )


def test_post_create_requires_explicit_unique_protected_service_selection() -> None:
    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    for selected in ((), ("airflow-scheduler", "airflow-scheduler"), ("database",)):
        with pytest.raises(admission.AdmissionError, match="non-empty unique"):
            admission.verify_created_containers(
                BINDINGS,
                project="data-platform",
                selected_services=selected,
                projections=projections,
                config_hashes=CONFIG_HASHES,
                config_files=CONFIG_FILES,
                env_files=ENV_FILES,
                runner=_docker_runner(),
            )

    report = admission.verify_created_containers(
        BINDINGS,
        project="data-platform",
        selected_services=("airflow-scheduler",),
        projections=projections,
        config_hashes=CONFIG_HASHES,
        config_files=CONFIG_FILES,
        env_files=ENV_FILES,
        runner=_docker_runner(),
    )
    assert [item["service"] for item in report["images"]] == ["airflow-scheduler"]


@pytest.mark.parametrize(
    "options",
    [
        ["name=seccomp,profile=builtin", "name=cgroupns"],
        ["name=apparmor", "name=cgroupns"],
        ["name=apparmor", "name=seccomp,profile=unconfined"],
        [
            "name=apparmor",
            "name=seccomp,profile=builtin",
            "name=seccomp,profile=unconfined",
        ],
    ],
)
def test_post_create_rejects_unsafe_docker_daemon_security_options(
    options: list[str],
) -> None:
    base_runner = _docker_runner()

    def runner(arguments: Sequence[str]) -> bytes:
        if tuple(arguments) == (
            "info",
            "--format",
            "{{json .SecurityOptions}}",
        ):
            return json.dumps(options).encode()
        return base_runner(arguments)

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="AppArmor.*seccomp"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=runner,
        )


@pytest.mark.parametrize(
    "profiles",
    [
        b"",
        b"docker-default (complain)\n",
        b"docker-default (kill)\n",
        b"docker-default (enforce)\ndocker-default (complain)\n",
    ],
)
def test_post_create_requires_docker_apparmor_profile_in_enforce_mode(
    profiles: bytes,
) -> None:
    base_runner = _docker_runner()

    def runner(arguments: Sequence[str]) -> bytes:
        if tuple(arguments) == admission._apparmor_probe_arguments(
            BINDINGS["airflow-scheduler"]
        ):
            return profiles
        return base_runner(arguments)

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="enforce mode"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=runner,
        )


def test_apparmor_probe_is_exact_constrained_and_digest_attested() -> None:
    observed: list[tuple[str, ...]] = []

    def runner(arguments: Sequence[str]) -> bytes:
        observed.append(tuple(arguments))
        return b"docker-default (enforce)\n"

    assert (
        admission._verify_apparmor_enforcement(
            runner=runner, image=BINDINGS["airflow-scheduler"]
        )
        == "docker-default (enforce)"
    )
    assert observed == [
        admission._apparmor_probe_arguments(BINDINGS["airflow-scheduler"])
    ]
    arguments = observed[0]
    assert "--pull=never" in arguments
    assert "--network=none" in arguments
    assert "--read-only" in arguments
    assert "--cap-drop=ALL" in arguments
    assert "--user=50000:0" in arguments


def test_failed_apparmor_container_probe_fails_closed() -> None:
    def fail(_arguments: Sequence[str]) -> bytes:
        raise admission.AdmissionError("probe denied")

    with pytest.raises(admission.AdmissionError, match="probe denied"):
        admission._verify_apparmor_enforcement(
            runner=fail, image=BINDINGS["airflow-scheduler"]
        )


def test_post_create_accepts_unstarted_network_endpoint_without_dynamic_identity() -> (
    None
):
    def clear_dynamic_endpoint(
        service: str,
        _ids: list[str],
        container: dict[str, Any],
        _image: dict[str, Any],
    ) -> None:
        if service != "airflow-scheduler" or not container:
            return
        for endpoint in container["NetworkSettings"]["Networks"].values():
            endpoint.update({"IPAddress": "", "MacAddress": "", "NetworkID": ""})

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    report = admission.verify_created_containers(
        BINDINGS,
        project="data-platform",
        selected_services=("airflow-scheduler",),
        projections=projections,
        config_hashes=CONFIG_HASHES,
        config_files=CONFIG_FILES,
        env_files=ENV_FILES,
        runner=_docker_runner(clear_dynamic_endpoint),
    )
    assert report["images"][0]["service"] == "airflow-scheduler"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("driver", "network policy"),
        ("attachable", "network policy"),
        ("label", "network labels"),
        ("ipam", "network subnet"),
    ],
)
def test_post_create_rejects_untrusted_docker_network(
    mutation: str, message: str
) -> None:
    base_runner = _docker_runner()

    def runner(arguments: Sequence[str]) -> bytes:
        raw = base_runner(arguments)
        if tuple(arguments)[:2] != ("network", "inspect"):
            return raw
        value = json.loads(raw)
        network = value[0]
        if mutation == "driver":
            network["Driver"] = "overlay"
        elif mutation == "attachable":
            network["Attachable"] = True
        elif mutation == "label":
            network["Labels"]["traefik.enable"] = "true"
        else:
            network["IPAM"]["Config"][0]["Subnet"] = "203.0.113.0/24"
        return json.dumps(value).encode()

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match=message):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=runner,
        )


@pytest.mark.parametrize("mutation", ["driver", "options", "label"])
def test_post_create_rejects_untrusted_soccerdata_volume(mutation: str) -> None:
    base_runner = _docker_runner()

    def runner(arguments: Sequence[str]) -> bytes:
        raw = base_runner(arguments)
        if tuple(arguments)[:2] != ("volume", "inspect"):
            return raw
        value = json.loads(raw)
        volume = value[0]
        if mutation == "driver":
            volume["Driver"] = "nfs"
        elif mutation == "options":
            volume["Options"] = {"device": ":/attacker", "type": "nfs"}
        else:
            volume["Labels"]["traefik.enable"] = "true"
        return json.dumps(value).encode()

    projections = admission.verify_rendered_compose(_rendered(), BINDINGS)
    with pytest.raises(admission.AdmissionError, match="soccerdata volume"):
        admission.verify_created_containers(
            BINDINGS,
            project="data-platform",
            selected_services=("airflow-scheduler",),
            projections=projections,
            config_hashes=CONFIG_HASHES,
            config_files=CONFIG_FILES,
            env_files=ENV_FILES,
            runner=runner,
        )


def test_helper_source_contains_no_container_lifecycle_subprocess() -> None:
    source = Path(admission.__file__).read_text(encoding="utf-8")
    for lifecycle in ("create", "start", "restart", "up", "run", "rm"):
        assert f'runner(("{lifecycle}"' not in source
        assert f'("docker", "{lifecycle}"' not in source


@pytest.mark.parametrize(
    "name",
    [
        "COMPOSE_FILE",
        "COMPOSE_PROFILES",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "DYLD_INSERT_LIBRARIES",
        "LD_PRELOAD",
    ],
)
def test_host_control_environment_is_rejected(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    for variable in admission._FORBIDDEN_CONTROL_ENV:
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv(name, "attacker-controlled")
    with pytest.raises(admission.AdmissionError, match=name):
        admission._assert_clean_control_environment()


def test_canonical_release_requires_root_system_python_and_exact_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = Path(admission.__file__).absolute().parents[1]
    monkeypatch.setattr(admission.os, "geteuid", lambda: 1000)
    with pytest.raises(admission.AdmissionError, match="effective UID 0"):
        admission._assert_canonical_release(root)

    monkeypatch.setattr(admission.os, "geteuid", lambda: 0)
    monkeypatch.setattr(admission.sys, "executable", "/tmp/python3")
    with pytest.raises(admission.AdmissionError, match="exact /usr/bin/python3"):
        admission._assert_canonical_release(root)

    monkeypatch.setattr(admission.sys, "executable", "/usr/bin/python3")
    with pytest.raises(admission.AdmissionError, match="canonical protected release"):
        admission._assert_canonical_release(tmp_path)


def test_canonical_release_binds_loaded_validator_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(admission.__file__).absolute().parents[1]
    monkeypatch.setattr(admission.os, "geteuid", lambda: 0)
    monkeypatch.setattr(admission.sys, "executable", "/usr/bin/python3")
    monkeypatch.setattr(
        admission.provenance, "_whoscored_loaded_source_sha256", "0" * 64
    )
    with pytest.raises(admission.AdmissionError, match="changed after loading"):
        admission._assert_canonical_release(root)


def test_docker_invocation_ignores_hostile_path_and_uses_sanitized_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attacker = tmp_path / "docker"
    attacker.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
    attacker.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(
        admission,
        "_trusted_docker_environment",
        lambda: {
            "DOCKER_HOST": "unix:///run/docker.sock",
            "HOME": "/nonexistent",
            "PATH": "/usr/bin:/bin",
        },
    )
    captured: dict[str, object] = {}

    def run(command: Sequence[str], **kwargs: object) -> SimpleNamespace:
        captured["command"] = tuple(command)
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(admission.subprocess, "run", run)
    assert admission._run_docker(("version",)) == b"ok\n"
    assert captured["command"] == ("/usr/bin/docker", "version")
    assert captured["env"] == {
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "PATH": "/usr/bin:/bin",
    }


def test_host_cli_rejects_nonisolated_or_hostile_python_import_environment(
    tmp_path: Path,
) -> None:
    helper = Path(admission.__file__).absolute()
    clean_environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    nonisolated = subprocess.run(
        ("/usr/bin/python3", "-S", str(helper), "--help"),
        env=clean_environment,
        capture_output=True,
        check=False,
    )
    assert nonisolated.returncode == admission.EXIT_CONFIG
    assert nonisolated.stderr == b""

    attacker = tmp_path / "attacker"
    forged_scripts = attacker / "scripts"
    forged_scripts.mkdir(parents=True)
    (forged_scripts / "__init__.py").write_text("", encoding="utf-8")
    sentinel = tmp_path / "forged-validator-executed"
    (forged_scripts / "validate_whoscored_build_provenance.py").write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('owned')\n",
        encoding="utf-8",
    )
    hostile_environment = {
        **clean_environment,
        "PYTHONPATH": f"{attacker}:{helper.parents[1]}",
    }
    hostile = subprocess.run(
        ("/usr/bin/python3", "-I", "-S", str(helper), "--help"),
        env=hostile_environment,
        capture_output=True,
        check=False,
    )
    assert hostile.returncode == admission.EXIT_CONFIG
    assert b"PYTHONPATH" in hostile.stderr
    assert not sentinel.exists()

    isolated = subprocess.run(
        ("/usr/bin/python3", "-I", "-S", str(helper), "--help"),
        env=clean_environment,
        capture_output=True,
        check=False,
    )
    assert isolated.returncode == 0, isolated.stderr.decode()
