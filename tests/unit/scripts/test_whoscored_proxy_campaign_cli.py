from __future__ import annotations

import json
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.whoscored.proxy_campaign import (
    ProxyCampaignApproval,
    ProxyCampaignLedger,
    ProxyCampaignRevoked,
)
from scripts import whoscored_proxy_campaign as cli


def _times() -> tuple[str, str]:
    issued = datetime.now(timezone.utc) - timedelta(minutes=1)
    return issued.isoformat(), (issued + timedelta(days=1)).isoformat()


def _template_args(path: Path) -> list[str]:
    issued, expires = _times()
    return [
        "template",
        "--campaign-id",
        "campaign-canary-test",
        "--approval-id",
        "approval-canary-test",
        "--runtime-sha256",
        "a" * 64,
        "--classifier-sha256",
        "b" * 64,
        "--issued-at",
        issued,
        "--expires-at",
        expires,
        "--output",
        str(path),
    ]


@pytest.mark.unit
def test_template_is_unsigned_private_and_exact_decimal_gigabyte(tmp_path):
    path = tmp_path / "template.json"

    assert cli.main(_template_args(path)) == 0

    value = json.loads(path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "signature" not in value
    assert "approval_sha256" not in value
    assert value["caps"] == {
        "total_provider_bytes": 1_000_000_000,
        "discovery_provider_bytes": cli.CANARY_DISCOVERY_CAP_BYTES,
        "capture_provider_bytes": cli.CANARY_CAPTURE_CAP_BYTES,
        "daily_provider_bytes": 1_000_000_000,
    }
    assert value["allowed_dag_ids"] == ["dag_canary_whoscored_proxy"]
    assert value["allocations"] == [
        {
            "allocation_id": cli.CANARY_DISCOVERY_ALLOCATION_ID,
            "phase": "discovery",
            "workload_class": "catalog_discovery",
            "work_item_id": cli.CANARY_DISCOVERY_WORK_ITEM_ID,
            "task_id": cli.CANARY_TASK_ID,
            "budget_bytes": cli.CANARY_DISCOVERY_CAP_BYTES,
            "request_limit": cli.WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT,
            "lease_limit": cli.WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT,
            "allowed_path_families": list(
                cli.CANARY_DISCOVERY_PATH_FAMILIES
            ),
        },
        {
            "allocation_id": cli.CANARY_ALLOCATION_ID,
            "phase": "capture",
            "workload_class": "representative_cohort",
            "work_item_id": cli.CANARY_WORK_ITEM_ID,
            "task_id": cli.CANARY_TASK_ID,
            "budget_bytes": cli.CANARY_CAPTURE_CAP_BYTES,
            "request_limit": cli.WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT,
            "lease_limit": cli.WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT,
            "allowed_path_families": list(cli.CANARY_ALLOWED_PATH_FAMILIES),
        }
    ]


def test_cli_rejects_duplicate_keys_before_human_review_or_signing(tmp_path):
    path = tmp_path / "duplicate.json"
    path.write_text(
        '{"caps":{"total_provider_bytes":1,"total_provider_bytes":2}}',
        encoding="utf-8",
    )

    with pytest.raises(cli.CampaignCliError, match="duplicate JSON key"):
        cli._read_json(path)


@pytest.mark.unit
def test_sign_and_verify_never_print_hmac_secret(tmp_path, monkeypatch, capsys):
    template = tmp_path / "template.json"
    approval_path = tmp_path / "approval-canary-test.json"
    secret = "test-control-secret-which-is-at-least-thirty-two-bytes"
    monkeypatch.setenv("TEST_CAMPAIGN_SECRET", secret)
    cli.main(_template_args(template))
    capsys.readouterr()

    assert (
        cli.main(
            [
                "sign",
                "--input",
                str(template),
                "--output",
                str(approval_path),
                "--secret-env",
                "TEST_CAMPAIGN_SECRET",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert secret not in output
    assert stat.S_IMODE(approval_path.stat().st_mode) == 0o600
    approval = ProxyCampaignApproval.from_dict(
        json.loads(approval_path.read_text(encoding="utf-8"))
    )
    approval.verify(secret)
    assert approval.is_exact_canary

    assert (
        cli.main(
            [
                "verify",
                "--approval",
                str(approval_path),
                "--require-exact-canary",
                "--secret-env",
                "TEST_CAMPAIGN_SECRET",
            ]
        )
        == 0
    )
    assert secret not in capsys.readouterr().out


@pytest.mark.unit
def test_revoke_uses_pinned_approval_and_writes_private_receipt(tmp_path, monkeypatch):
    template = tmp_path / "template.json"
    approval_path = tmp_path / "approval-canary-test.json"
    ledger_path = tmp_path / "campaign-ledger.json"
    receipt_path = tmp_path / "revoked.json"
    approval_secret = "test-approval-secret-which-is-at-least-thirty-two-bytes"
    ledger_secret = "test-ledger-secret-which-is-at-least-thirty-two-bytes"
    monkeypatch.setenv("TEST_CAMPAIGN_SECRET", approval_secret)
    monkeypatch.setenv("TEST_LEDGER_SECRET", ledger_secret)
    cli.main(_template_args(template))
    cli.main(
        [
            "sign",
            "--input",
            str(template),
            "--output",
            str(approval_path),
            "--secret-env",
            "TEST_CAMPAIGN_SECRET",
        ]
    )
    approval = ProxyCampaignApproval.from_dict(
        json.loads(approval_path.read_text(encoding="utf-8"))
    )
    ledger = ProxyCampaignLedger(
        ledger_path,
        secret=ledger_secret,
        approval_secret=approval_secret,
    )
    ledger.snapshot(approval)

    assert (
        cli.main(
            [
                "revoke",
                "--approval",
                str(approval_path),
                "--approval-id",
                approval.approval_id,
                "--approval-sha256",
                approval.approval_sha256,
                "--ledger",
                str(ledger_path),
                "--reason",
                "operator kill switch test",
                "--output",
                str(receipt_path),
                "--secret-env",
                "TEST_CAMPAIGN_SECRET",
                "--ledger-secret-env",
                "TEST_LEDGER_SECRET",
            ]
        )
        == 0
    )
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    with pytest.raises(ProxyCampaignRevoked):
        ledger.snapshot(approval)


@pytest.mark.unit
def test_approval_and_ledger_defaults_never_use_control_token(monkeypatch):
    parser = cli.build_parser()
    sign = parser.parse_args(
        ["sign", "--input", "in.json", "--output", "out.json"]
    )
    revoke = parser.parse_args(
        [
            "revoke",
            "--approval",
            "approval.json",
            "--reason",
            "stop",
            "--output",
            "receipt.json",
        ]
    )

    assert sign.secret_env == "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"
    assert revoke.secret_env == "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"
    assert revoke.ledger_secret_env == "WHOSCORED_PROXY_LEDGER_HMAC_SECRET"


@pytest.mark.unit
def test_atomic_outputs_refuse_overwrite_without_force(tmp_path):
    path = tmp_path / "template.json"
    cli.main(_template_args(path))

    with pytest.raises(SystemExit):
        cli.main(_template_args(path))


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    (
        "https://www.whoscored.com/Matches/123/Live",
        "https://www.whoscored.com/Matches/123/Preview",
        "https://www.whoscored.com/Players/42/Show",
        "https://www.whoscored.com/tournaments/23752/data/?d=2026-07-15",
        "https://www.whoscored.com/Regions/247/Tournaments/36",
        "https://www.whoscored.com/stagestatfeed/23752/stageteams/",
        "https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
        "https://www.whoscored.com/statisticsfeed/1/getplayerstatistics",
        "https://www.whoscored.com/",
    ),
)
def test_canary_path_families_allow_every_production_target(tmp_path, url):
    template = tmp_path / "template.json"
    secret = "test-control-secret-which-is-at-least-thirty-two-bytes"
    unsigned = cli._unsigned_canary(
        cli.build_parser().parse_args(_template_args(template))
    )
    from scrapers.whoscored.proxy_campaign import sign_proxy_campaign_approval

    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, secret)
    )

    assert approval.allows_url(url, allocation_id=cli.CANARY_ALLOCATION_ID)


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    (
        "https://www.whoscored.com/MatchesEvil/123/Live",
        "https://www.whoscored.com/PlayersEvil/42/Show",
        "https://www.whoscored.com/tournamentsEvil/23752/data/",
        "https://www.whoscored.com/not-an-approved-family",
        "https://evil.example/Matches/123/Live",
        "http://www.whoscored.com/Matches/123/Live",
    ),
)
def test_canary_path_families_deny_boundary_and_origin_escapes(tmp_path, url):
    template = tmp_path / "template.json"
    secret = "test-control-secret-which-is-at-least-thirty-two-bytes"
    unsigned = cli._unsigned_canary(
        cli.build_parser().parse_args(_template_args(template))
    )
    from scrapers.whoscored.proxy_campaign import sign_proxy_campaign_approval

    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, secret)
    )

    assert not approval.allows_url(url, allocation_id=cli.CANARY_ALLOCATION_ID)


@pytest.mark.unit
def test_discovery_allocation_cannot_expand_into_capture_paths(tmp_path):
    template = tmp_path / "template.json"
    secret = "test-control-secret-which-is-at-least-thirty-two-bytes"
    unsigned = cli._unsigned_canary(
        cli.build_parser().parse_args(_template_args(template))
    )
    from scrapers.whoscored.proxy_campaign import sign_proxy_campaign_approval

    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, secret)
    )

    assert approval.allows_url(
        "https://www.whoscored.com/Regions/247/Tournaments/36",
        allocation_id=cli.CANARY_DISCOVERY_ALLOCATION_ID,
    )
    assert approval.allows_url(
        "https://www.whoscored.com/tournaments/23752/data/?d=2026-07-15",
        allocation_id=cli.CANARY_DISCOVERY_ALLOCATION_ID,
    )
    assert not approval.allows_url(
        "https://www.whoscored.com/Matches/123/Live",
        allocation_id=cli.CANARY_DISCOVERY_ALLOCATION_ID,
    )
