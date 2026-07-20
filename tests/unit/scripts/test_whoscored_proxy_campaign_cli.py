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


# --- Daily-ingest standing approval issuance (#954 automation) ----------------

import hashlib as _hashlib  # noqa: E402

from scrapers.whoscored.proxy_campaign import (  # noqa: E402
    WHOSCORED_INGEST_DAG_ID,
)


def _secret_file(tmp_path):
    path = tmp_path / "secret"
    path.write_text("k" * 40)
    path.chmod(0o600)
    return path


def _charter(tmp_path, *, daily_mb=300, days=25, order_id="proxysio-38950"):
    path = tmp_path / f"charter_{daily_mb}_{days}.json"
    valid_until = (datetime.now(timezone.utc) + timedelta(days=days)).replace(
        microsecond=0
    ).isoformat()
    path.write_text(json.dumps({
        "schema_version": 1, "order_id": order_id,
        "valid_until": valid_until, "daily_mb": daily_mb, "monthly_mb": daily_mb * 30,
    }))
    path.chmod(0o600)
    return path


def _scopes_file(tmp_path, scopes=("ENG-Premier League=2526", "INT-World Cup=2026")):
    path = tmp_path / "scopes.json"
    path.write_text(json.dumps(list(scopes)))
    return path


_RUN_ID = "scheduled__2026-07-19T10:00:00+00:00"


def _issue_args(tmp_path, **overrides):
    args = {
        "run_id": _RUN_ID,
        "scopes_file": str(_scopes_file(tmp_path)),
        "charter": str(_charter(tmp_path)),
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "total_mb": "150",
        "approval_root": str(tmp_path / "appr"),
        "pointer_root": str(tmp_path / "ptr"),
        "secret_file": str(_secret_file(tmp_path)),
    }
    args.update(overrides)
    argv = [
        "issue-daily-ingest",
        "--run-id", args["run_id"],
        "--scopes-file", args["scopes_file"],
        "--charter", args["charter"],
        "--runtime-sha256", args["runtime_sha256"],
        "--classifier-sha256", args["classifier_sha256"],
        "--total-mb", str(args["total_mb"]),
        "--approval-root", args["approval_root"],
        "--pointer-root", args["pointer_root"],
        "--secret-file", args["secret_file"],
    ]
    return argv


@pytest.mark.unit
def test_issue_daily_ingest_writes_valid_approval_and_pointer(tmp_path):
    assert cli.main(_issue_args(tmp_path)) == 0

    digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{digest}.json").read_text())
    assert stat.S_IMODE((tmp_path / "ptr" / f"{digest}.json").stat().st_mode) == 0o600
    assert pointer["dag_id"] == WHOSCORED_INGEST_DAG_ID
    assert pointer["run_id"] == _RUN_ID
    assert pointer["schema_version"] == 1

    approval_path = tmp_path / "appr" / f"{pointer['approval_id']}.json"
    assert stat.S_IMODE(approval_path.stat().st_mode) == 0o600
    approval = ProxyCampaignApproval.from_dict(json.loads(approval_path.read_text()))
    approval.verify("k" * 40)
    assert approval.run_id == _RUN_ID
    assert approval.allowed_dag_ids == (WHOSCORED_INGEST_DAG_ID,)
    assert not approval.is_exact_canary
    assert approval.approval_sha256 == pointer["approval_sha256"]
    # discovery + profiles + 2 scopes = 4 allocations; budgets sum exactly.
    assert len(approval.allocations) == 4
    assert sum(a.budget_bytes for a in approval.allocations) == 150_000_000


@pytest.mark.unit
def test_issue_daily_ingest_rejects_expired_charter(tmp_path):
    argv = _issue_args(tmp_path, charter=str(_charter(tmp_path, days=-1)))
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_budget_over_charter(tmp_path):
    argv = _issue_args(tmp_path, charter=str(_charter(tmp_path, daily_mb=50)),
                       total_mb="150")
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_non_scheduled_run_id(tmp_path):
    argv = _issue_args(tmp_path, run_id="manual__nope")
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_duplicate_scopes(tmp_path):
    dup = tmp_path / "dups.json"
    dup.write_text(json.dumps(["A=2526", "A=2526"]))
    argv = _issue_args(tmp_path, scopes_file=str(dup))
    with pytest.raises(SystemExit):
        cli.main(argv)
