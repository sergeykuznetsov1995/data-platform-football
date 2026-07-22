from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path

import pytest

from scripts import whoscored_go_decision as decision


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
NOW = datetime(2026, 7, 23, 7, 50, tzinfo=timezone.utc)
FINAL_COMPLETED = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)
SECRET = "owner-secret-" + "s" * 52
PROVIDER_SITE_EVIDENCE = b"provider-account-region-site-contract\n"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _protected(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _admission() -> dict[str, object]:
    return {
        "schema_version": 2,
        "status": "admitted-running-v1",
        "images": [
            {"service": service} for service in sorted(decision.PROTECTED_SERVICES)
        ],
        "provider_policy": {"document_sha256": SHA_A},
        "rollout_acceptance": {
            "status": "accepted",
            "authority_binding": "current-signed-rollout",
            "accepted_waves": ["wave-20", "wave-70", "wave-all"],
            "missing_waves": [],
            "rollout_id": "rollout-954",
            "final_wave_receipt_sha256": SHA_B,
            "latest_scheduled_run": {
                "state": "success",
                "completed_at": FINAL_COMPLETED.isoformat().replace("+00:00", "Z"),
            },
            "backup_recovery": {
                "status": "passed",
                "off_host_receipt_key": (
                    "restore-drill-receipts/v2/20260722T120000Z-" + SHA_C + ".json"
                ),
                "off_host_receipt_sha256": SHA_C,
                "source_uris": [
                    "s3://football/ops/whoscored",
                    "s3://football/raw/whoscored",
                ],
                "live_backup": {
                    "status": "passed",
                    "capability": {"bucket": "whoscored-off-host"},
                },
            },
            "rollout_authority": {
                "authority_binding": "current-signed-rollout",
                "rollout_manifest_sha256": SHA_D,
                "charter_sha256": SHA_E,
            },
        },
    }


def _site_attestation(**updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "source": "whoscored",
        "attestation_type": "off-host-backup-site",
        "operational_owner": "sergeykuznetsov1995",
        "production_bucket": "football",
        "backup_bucket": "whoscored-off-host",
        "production_endpoint_sha256": SHA_D,
        "backup_endpoint_sha256": SHA_E,
        "production_failure_domain": "production-host-berlin",
        "backup_failure_domain": "provider-eu-central-1",
        "provider_evidence_sha256": hashlib.sha256(PROVIDER_SITE_EVIDENCE).hexdigest(),
        "valid_from": "2026-07-22T00:00:00Z",
        "valid_until": "2026-08-22T00:00:00Z",
    }
    value.update(updates)
    return value


def _unsigned(
    admission_raw: bytes, site_raw: bytes, **updates: object
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "source": "whoscored",
        "decision": "GO",
        "operational_owner": "sergeykuznetsov1995",
        "channel": "telegram",
        "message_id": "ws954-final-alert-42",
        "delivered_at": "2026-07-23T07:01:00Z",
        "acked_at": "2026-07-23T07:45:00Z",
        "acked_by": "sergeykuznetsov1995",
        "decision_at": "2026-07-23T07:50:00Z",
        "admission_report_sha256": hashlib.sha256(admission_raw).hexdigest(),
        "rollout_id": "rollout-954",
        "final_wave_receipt_sha256": SHA_B,
        "rollout_manifest_sha256": SHA_D,
        "charter_sha256": SHA_E,
        "provider_policy_sha256": SHA_A,
        "backup_restore_receipt_sha256": SHA_C,
        "off_host_site_attestation_sha256": hashlib.sha256(site_raw).hexdigest(),
        "signature_algorithm": "hmac-sha256",
    }
    value.update(updates)
    return value


def _inputs(
    tmp_path: Path,
    *,
    admission: dict[str, object] | None = None,
    decision_updates: dict[str, object] | None = None,
    site_updates: dict[str, object] | None = None,
    now: datetime = NOW,
) -> tuple[Path, Path, Path, Path, Path, Path]:
    admission_path = _protected(
        tmp_path / "admission.json", _canonical(admission or _admission())
    )
    admission_time = now - timedelta(minutes=50)
    os.utime(admission_path, (admission_time.timestamp(), admission_time.timestamp()))
    admission_raw = admission_path.read_bytes()
    site_path = _protected(
        tmp_path / "off-host-site.json",
        _canonical(_site_attestation(**(site_updates or {}))),
    )
    site_raw = site_path.read_bytes()
    provider_evidence_path = _protected(
        tmp_path / "provider-site-evidence.txt", PROVIDER_SITE_EVIDENCE
    )
    decision_path = _protected(
        tmp_path / "decision.json",
        _canonical(_unsigned(admission_raw, site_raw, **(decision_updates or {}))),
    )
    secret_path = _protected(tmp_path / "owner-secret", SECRET.encode("utf-8"))
    return (
        admission_path,
        decision_path,
        site_path,
        provider_evidence_path,
        secret_path,
        tmp_path / "go.json",
    )


def test_final_go_decision_binds_admission_owner_ack_and_cutover(
    tmp_path: Path,
) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)

    artifact = decision.finalize_go_decision(
        admission_path=admission_path,
        decision_path=input_path,
        off_host_site_attestation_path=site_path,
        provider_site_evidence_path=provider_evidence_path,
        owner_secret_path=secret_path,
        output_path=output_path,
        now=NOW,
    )

    assert output_path.stat().st_mode & 0o777 == 0o600
    assert json.loads(output_path.read_text(encoding="utf-8")) == artifact
    unsigned = {
        key: value
        for key, value in artifact.items()
        if key not in {"document_sha256", "signature"}
    }
    assert (
        artifact["document_sha256"]
        == hashlib.sha256(
            decision._canonical_bytes(unsigned, newline=False)
        ).hexdigest()
    )
    assert (
        artifact["signature"]
        == hmac.new(
            SECRET.encode("utf-8"),
            decision._canonical_bytes(
                {**unsigned, "document_sha256": artifact["document_sha256"]},
                newline=False,
            ),
            hashlib.sha256,
        ).hexdigest()
    )


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {"acked_at": "2026-07-23T08:01:01Z"},
            "one-hour SLA",
        ),
        (
            {"acked_by": "somebody-else"},
            "owner acknowledgement identity",
        ),
        (
            {"final_wave_receipt_sha256": "f" * 64},
            "differs from admission",
        ),
        (
            {
                "delivered_at": "2026-07-23T08:11:00Z",
                "acked_at": "2026-07-23T08:45:00Z",
                "decision_at": "2026-07-23T09:00:00Z",
            },
            "outside 06:00..09:00",
        ),
    ],
)
def test_final_go_decision_rejects_invalid_human_gate(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    selected_now = (
        datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc)
        if updates.get("decision_at") == "2026-07-23T09:00:00Z"
        else NOW
    )
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path, decision_updates=updates, now=selected_now)

    with pytest.raises(decision.GoDecisionError, match=message):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=selected_now,
        )

    assert not output_path.exists()


def test_final_go_decision_requires_all_five_running_services(tmp_path: Path) -> None:
    admission = _admission()
    admission["images"] = admission["images"][:-1]  # type: ignore[index]
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path, admission=admission)

    with pytest.raises(decision.GoDecisionError, match="all five running services"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )


@pytest.mark.parametrize(
    ("site_updates", "message"),
    [
        (
            {"backup_failure_domain": "production-host-berlin"},
            "distinct failure domain",
        ),
        (
            {"backup_endpoint_sha256": SHA_D},
            "endpoint is not independent",
        ),
        (
            {"backup_bucket": "football"},
            "site attestation identity",
        ),
    ],
)
def test_final_go_decision_requires_independent_off_host_site_evidence(
    tmp_path: Path,
    site_updates: dict[str, object],
    message: str,
) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path, site_updates=site_updates)

    with pytest.raises(decision.GoDecisionError, match=message):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )


def test_final_go_decision_reads_exact_protected_provider_site_evidence(
    tmp_path: Path,
) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)
    provider_evidence_path.write_bytes(b"different-provider-site\n")

    with pytest.raises(decision.GoDecisionError, match="differs from"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )


def test_final_go_decision_rejects_stale_admission(tmp_path: Path) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)
    stale = NOW - timedelta(minutes=76)
    os.utime(admission_path, (stale.timestamp(), stale.timestamp()))

    with pytest.raises(decision.GoDecisionError, match="running admission is stale"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )


def test_final_go_decision_requires_admission_before_telegram_ack(
    tmp_path: Path,
) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)
    after_delivery = datetime(2026, 7, 23, 7, 2, tzinfo=timezone.utc)
    os.utime(
        admission_path,
        (after_delivery.timestamp(), after_delivery.timestamp()),
    )

    with pytest.raises(decision.GoDecisionError, match="operational order"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )


def test_final_go_decision_rejects_naive_current_time(tmp_path: Path) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)

    with pytest.raises(decision.GoDecisionError, match="timezone-aware"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW.replace(tzinfo=None),
        )


def test_final_go_decision_never_overwrites_an_artifact(tmp_path: Path) -> None:
    (
        admission_path,
        input_path,
        site_path,
        provider_evidence_path,
        secret_path,
        output_path,
    ) = _inputs(tmp_path)
    output_path.write_text("sentinel", encoding="utf-8")
    output_path.chmod(0o600)

    with pytest.raises(decision.GoDecisionError, match="already exists"):
        decision.finalize_go_decision(
            admission_path=admission_path,
            decision_path=input_path,
            off_host_site_attestation_path=site_path,
            provider_site_evidence_path=provider_evidence_path,
            owner_secret_path=secret_path,
            output_path=output_path,
            now=NOW,
        )

    assert output_path.read_text(encoding="utf-8") == "sentinel"
