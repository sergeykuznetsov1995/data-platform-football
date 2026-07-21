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


_ISSUER_NOW = datetime(2026, 7, 20, 9, 15, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _freeze_daily_issuer_clock(monkeypatch):
    monkeypatch.setattr(cli, "_issuance_now", lambda: _ISSUER_NOW)


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


def _private_json(path, value):
    if path.exists():
        path.chmod(0o600)
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return path


def _owner_secret_file(tmp_path):
    path = tmp_path / "owner-secret"
    path.write_text("o" * 40)
    path.chmod(0o600)
    return path


def _ledger_secret_file(tmp_path):
    path = tmp_path / "ledger-secret"
    path.write_text("l" * 40)
    path.chmod(0o600)
    return path


def _cohort():
    return {
        "schema_version": 1,
        "cohort_id": "smoke-prefix",
        "scopes": ["ENG-Premier League=2526", "INT-World Cup=2026"],
    }


def _cohort_file(tmp_path, *, value=None):
    return _private_json(tmp_path / "cohort-authority.json", value or _cohort())


@pytest.mark.unit
def test_daily_planner_counts_schedule_months_and_player_pagination(
    monkeypatch, tmp_path
):
    from datetime import date
    from types import SimpleNamespace
    from dags.scripts import run_whoscored_scraper as runner

    scope = SimpleNamespace(
        spec="ENG-Premier League=2526",
        competition_id="ENG-Premier League",
        season_id="2526",
    )
    runtime_scope = SimpleNamespace(
        scope=object(),
        stage_ids=(700,),
        start=date(2025, 8, 1),
        end=date(2025, 10, 31),
    )
    rows = {
        "competitions": [{}, {}],
        "seasons": [{}],
        "stages": [{}],
    }
    catalog = SimpleNamespace(to_rows=lambda: rows)
    matches = [
        SimpleNamespace(game_id=11, exact_candidate_count=2),
        SimpleNamespace(game_id=12, exact_candidate_count=None),
    ]
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-parent",
                "catalog_payload_sha256": "c" * 64,
            },
            catalog,
        ),
        list_match_candidates=lambda *_args, **_kwargs: matches,
        list_preview_candidates=lambda *_args, **_kwargs: [{"game_id": 21}],
        profile_candidate_snapshot=lambda **_kwargs: SimpleNamespace(
            count=0, payload_sha256="e" * 64
        ),
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda *_args, **_kwargs: [(scope, runtime_scope)],
    )
    cohort = _private_json(
        tmp_path / "cohort.json",
        {
            "schema_version": 1,
            "cohort_id": "smoke-prefix",
            "scopes": [scope.spec],
        },
    )
    output = tmp_path / "daily-plan.json"

    cli.command_plan_daily_ingest(
        SimpleNamespace(
            cohort_file=str(cohort),
            max_scopes=1,
            output=str(output),
            force=False,
        )
    )

    plan = cli._read_daily_plan(output)
    workload = plan["scope_workloads"][0]
    assert plan["schema_version"] == 2
    assert plan["max_scopes"] == 1
    # season page + calendar + 3 month pages + all 68 first-page feeds
    assert workload["schedule_target_limit"] == 1 + 1 + 3 + 68
    assert workload["player_pagination_target_limit"] == 30 * 99
    assert workload["paid_target_count"] == 1 + 1 + 3 + 68 + 30 * 99 + 2 + 1
    # root + two competitions + season + three stage probes + headroom
    assert plan["discovery_parent_target_count"] == 7
    assert plan["discovery_target_limit"] == (
        7 + cli.SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
    )
    assert plan["profile_target_count"] == 0


@pytest.mark.unit
def test_daily_planner_rejects_match_backlog_above_runtime_cap(
    monkeypatch, tmp_path
):
    from datetime import date
    from types import SimpleNamespace
    from dags.scripts import run_whoscored_scraper as runner

    scope = SimpleNamespace(
        spec="ENG-Premier League=2526",
        competition_id="ENG-Premier League",
        season_id="2526",
    )
    runtime_scope = SimpleNamespace(
        scope=object(), stage_ids=(700,), start=date(2025, 8, 1), end=date(2025, 8, 31)
    )
    candidates = [
        SimpleNamespace(
            game_id=value,
            exact_candidate_count=101 if value == 1 else None,
        )
        for value in range(1, 102)
    ]
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-parent",
                "catalog_payload_sha256": "c" * 64,
            },
            SimpleNamespace(
                to_rows=lambda: {
                    "competitions": [{}], "seasons": [{}], "stages": [{}]
                }
            ),
        ),
        list_match_candidates=lambda *_args, **_kwargs: candidates,
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda *_args, **_kwargs: [(scope, runtime_scope)],
    )
    cohort = _private_json(
        tmp_path / "cohort.json",
        {
            "schema_version": 1,
            "cohort_id": "smoke-prefix",
            "scopes": [scope.spec],
        },
    )

    with pytest.raises(cli.CampaignCliError, match="backlog exceeds"):
        cli.command_plan_daily_ingest(
            SimpleNamespace(
                cohort_file=str(cohort),
                max_scopes=1,
                output=str(tmp_path / "plan.json"),
                force=False,
            )
        )


def _plan(
    tmp_path,
    *,
    workloads=None,
    profile_target_count=12,
    max_scopes=2,
    cohort=None,
):
    values = workloads or [
        {
            "scope": scope,
            "work_item_id": cli._scope_work_item_id(scope),
            "schedule_target_limit": demand - 12,
            "schedule_targets_sha256": _hashlib.sha256(
                cli.canonical_json_bytes([f"season:{scope}"])
            ).hexdigest(),
            "player_pagination_target_limit": 0,
            "match_target_count": 5,
            "match_targets_sha256": _hashlib.sha256(
                cli.canonical_json_bytes([str(value) for value in range(5)])
            ).hexdigest(),
            "preview_target_count": 7,
            "preview_targets_sha256": _hashlib.sha256(
                cli.canonical_json_bytes([str(value) for value in range(7)])
            ).hexdigest(),
            "paid_target_count": demand,
        }
        for scope, demand in (
            ("ENG-Premier League=2526", 32),
            ("INT-World Cup=2026", 40),
        )
    ]
    cohort_sha = _hashlib.sha256(
        cli.canonical_json_bytes(cohort or _cohort())
    ).hexdigest()
    path = _private_json(
        tmp_path / "plan.json",
        {
            "schema_version": 2,
            "cohort_id": "smoke-prefix",
            "cohort_sha256": cohort_sha,
            "max_scopes": max_scopes,
            "catalog_batch_id": "wsc2-parent",
            "catalog_payload_sha256": "c" * 64,
            "workload_sha256": _hashlib.sha256(
                cli.canonical_json_bytes(values)
            ).hexdigest(),
            "scope_workloads": values,
            "discovery_parent_target_count": 100,
            "discovery_expansion_headroom": (
                cli.SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
            ),
            "discovery_target_limit": (
                100 + cli.SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
            ),
            "profile_target_count": profile_target_count,
            "profile_targets_sha256": "e" * 64,
        },
    )
    path.chmod(0o440)
    return path


def _policy(
    tmp_path,
    *,
    daily_bytes=300_000_000,
    days=25,
    valid_from=None,
    valid_until=None,
    name="default",
):
    now = _ISSUER_NOW
    unsigned = {
        "schema_version": 1,
        "source": "whoscored",
        "provider_id": "proxysio",
        "order_id": "proxysio-38950",
        "plan_id": "bronze-1gb",
        "valid_from": (valid_from or now - timedelta(minutes=5)).isoformat(),
        "valid_until": (
            valid_until or now + timedelta(days=max(days, 1))
        ).isoformat(),
        "receipt_sha256": "d" * 64,
        "provider_quota_bytes": 1_000_000_000,
        "safety_cap_bytes": 300_000_000,
        "daily_cap_bytes": daily_bytes,
        "monthly_cap_bytes": 300_000_000,
        "order_cap_bytes": 300_000_000,
        "signature_algorithm": "hmac-sha256",
    }
    signed = cli._sign_authority_document(
        unsigned,
        expected_fields=cli._PROVIDER_POLICY_UNSIGNED_FIELDS,
        secret="o" * 40,
    )
    return _private_json(tmp_path / f"policy-{name}-{daily_bytes}.json", signed)


def _charter(
    tmp_path,
    *,
    daily_bytes=300_000_000,
    days=25,
    policy_path=None,
    valid_from=None,
    valid_until=None,
    name="default",
    cohort=None,
):
    now = _ISSUER_NOW
    policy_path = policy_path or _policy(
        tmp_path, daily_bytes=daily_bytes, days=days, name=name
    )
    policy = json.loads(policy_path.read_text())
    cohort_value = cohort or _cohort()
    cohort_sha = _hashlib.sha256(
        cli.canonical_json_bytes(cohort_value)
    ).hexdigest()
    unsigned = {
        "schema_version": 2,
        "source": "whoscored",
        "provider_policy_sha256": policy["document_sha256"],
        "order_id": policy["order_id"],
        "billing_month": now.strftime("%Y-%m"),
        "cohort_id": cohort_value["cohort_id"],
        "cohort_sha256": cohort_sha,
        "valid_from": (valid_from or now - timedelta(minutes=5)).isoformat(),
        "valid_until": (valid_until or now + timedelta(days=days)).isoformat(),
        "daily_cap_bytes": daily_bytes,
        "monthly_cap_bytes": 300_000_000,
        "order_cap_bytes": 300_000_000,
        "max_issuances": 2,
        "signature_algorithm": "hmac-sha256",
    }
    signed = cli._sign_authority_document(
        unsigned,
        expected_fields=cli._CHARTER_UNSIGNED_FIELDS,
        secret="o" * 40,
    )
    return _private_json(
        tmp_path / f"charter-{name}-{daily_bytes}-{days}.json", signed
    )


_RUN_ID = "scheduled__" + cli._expected_daily_logical_date(_ISSUER_NOW).isoformat()


def _issue_args(tmp_path, **overrides):
    args = {
        "run_id": _RUN_ID,
        "plan_file": str(_plan(tmp_path)),
        "cohort_file": str(_cohort_file(tmp_path)),
        "max_scopes": 2,
        "provider_policy": str(_policy(tmp_path)),
        "charter": str(_charter(tmp_path)),
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "total_bytes": "150000000",
        "approval_root": str(tmp_path / "appr"),
        "pointer_root": str(tmp_path / "ptr"),
        "secret_file": str(_secret_file(tmp_path)),
        "owner_secret_file": str(_owner_secret_file(tmp_path)),
        "issuance_ledger_secret_file": str(_ledger_secret_file(tmp_path)),
        "issuance_ledger": str(tmp_path / "issuance.json"),
    }
    args.update(overrides)
    argv = [
        "issue-daily-ingest",
        "--run-id", args["run_id"],
        "--plan-file", args["plan_file"],
        "--cohort-file", args["cohort_file"],
        "--max-scopes", str(args["max_scopes"]),
        "--provider-policy", args["provider_policy"],
        "--charter", args["charter"],
        "--runtime-sha256", args["runtime_sha256"],
        "--classifier-sha256", args["classifier_sha256"],
        "--total-bytes", str(args["total_bytes"]),
        "--approval-root", args["approval_root"],
        "--pointer-root", args["pointer_root"],
        "--secret-file", args["secret_file"],
        "--owner-secret-file", args["owner_secret_file"],
        "--issuance-ledger-secret-file", args["issuance_ledger_secret_file"],
        "--issuance-ledger", args["issuance_ledger"],
    ]
    if args.get("issued_at"):
        argv.extend(("--issued-at", str(args["issued_at"])))
    if args.get("expires_at"):
        argv.extend(("--expires-at", str(args["expires_at"])))
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
    approval.verify("k" * 40, now=_ISSUER_NOW)
    assert approval.run_id == _RUN_ID
    assert approval.allowed_dag_ids == (WHOSCORED_INGEST_DAG_ID,)
    assert not approval.is_exact_canary
    assert approval.approval_sha256 == pointer["approval_sha256"]
    # discovery + profiles + 2 scopes = 4 allocations; budgets sum exactly.
    assert len(approval.allocations) == 4
    assert sum(a.budget_bytes for a in approval.allocations) == 150_000_000
    assert approval.schema_version == 3
    assert approval.scheduled_authority is not None


@pytest.mark.unit
def test_offline_signer_accepts_wrapper_private_read_only_inputs(tmp_path):
    argv = _issue_args(tmp_path)
    for flag in (
        "--cohort-file",
        "--provider-policy",
        "--charter",
        "--secret-file",
        "--owner-secret-file",
        "--issuance-ledger-secret-file",
    ):
        Path(argv[argv.index(flag) + 1]).chmod(0o400)

    assert cli.main(argv) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("policy_minutes", "charter_minutes"),
    ((20, 20), (30, 15)),
    ids=("policy-near-expiry", "charter-near-expiry"),
)
def test_issue_daily_ingest_rejects_authority_expiring_before_runtime_window(
    tmp_path, policy_minutes, charter_minutes
):
    policy_until = _ISSUER_NOW + timedelta(minutes=policy_minutes)
    charter_until = _ISSUER_NOW + timedelta(minutes=charter_minutes)
    policy = _policy(
        tmp_path,
        valid_until=policy_until,
        name=f"near-{policy_minutes}-{charter_minutes}",
    )
    charter = _charter(
        tmp_path,
        policy_path=policy,
        valid_until=charter_until,
        name=f"near-{policy_minutes}-{charter_minutes}",
    )

    with pytest.raises(SystemExit):
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
            )
        )

    assert not (tmp_path / "issuance.json").exists()
    assert not (tmp_path / "ptr").exists()
    assert not (tmp_path / "appr").exists()


@pytest.mark.unit
def test_issue_daily_ingest_clamps_default_expiry_to_viable_authority_window(tmp_path):
    policy_until = _ISSUER_NOW + timedelta(hours=14)
    charter_until = _ISSUER_NOW + timedelta(hours=13)
    policy = _policy(tmp_path, valid_until=policy_until, name="viable-clamp")
    charter = _charter(
        tmp_path,
        policy_path=policy,
        valid_until=charter_until,
        name="viable-clamp",
    )

    assert cli.main(
        _issue_args(
            tmp_path,
            provider_policy=str(policy),
            charter=str(charter),
        )
    ) == 0

    digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
    )
    assert approval.expires_at == charter_until.isoformat()


@pytest.mark.unit
def test_issue_daily_ingest_rejects_explicit_expiry_past_authority_window(tmp_path):
    valid_until = _ISSUER_NOW + timedelta(minutes=15)
    policy = _policy(tmp_path, valid_until=valid_until, name="explicit-expiry")
    charter = _charter(
        tmp_path,
        policy_path=policy,
        valid_until=valid_until,
        name="explicit-expiry",
    )

    with pytest.raises(SystemExit):
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
                expires_at=(valid_until + timedelta(seconds=1)).isoformat(),
            )
        )


@pytest.mark.unit
def test_issue_daily_ingest_rejects_issued_at_before_authority_window(tmp_path):
    valid_from = _ISSUER_NOW - timedelta(minutes=1)
    policy = _policy(tmp_path, valid_from=valid_from, name="future-not-before")
    charter = _charter(
        tmp_path,
        policy_path=policy,
        valid_from=valid_from,
        name="future-not-before",
    )

    with pytest.raises(SystemExit):
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
                issued_at=(_ISSUER_NOW - timedelta(minutes=2)).isoformat(),
            )
        )


@pytest.mark.unit
def test_issue_daily_ingest_rejects_charter_outside_provider_policy(tmp_path):
    policy_until = _ISSUER_NOW + timedelta(minutes=15)
    policy = _policy(tmp_path, valid_until=policy_until, name="narrow-policy")
    charter = _charter(
        tmp_path,
        policy_path=policy,
        valid_until=policy_until + timedelta(seconds=1),
        name="wide-charter",
    )

    with pytest.raises(SystemExit):
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
            )
        )


@pytest.mark.unit
def test_issue_daily_ingest_zero_profile_backlog_keeps_one_safe_binding(tmp_path):
    argv = _issue_args(tmp_path)
    _plan(tmp_path, profile_target_count=0)

    assert cli.main(argv) == 0
    digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads(
            (tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text()
        )
    )
    approval.verify("k" * 40, now=_ISSUER_NOW)
    assert approval.scheduled_authority is not None
    assert approval.scheduled_authority.profile_target_count == 0
    allocation = approval.allocation("profiles-daily")
    assert allocation.request_limit == 2
    assert allocation.lease_limit == 1


@pytest.mark.unit
def test_issue_daily_ingest_is_idempotent_for_the_same_run(tmp_path):
    argv = _issue_args(tmp_path)
    assert cli.main(argv) == 0
    assert cli.main(argv) == 0
    ledger = json.loads((tmp_path / "issuance.json").read_text())
    assert len(ledger["entries"]) == 1


@pytest.mark.unit
def test_issue_daily_ingest_rejects_wrong_logical_timestamp(tmp_path):
    wrong = "scheduled__" + _ISSUER_NOW.replace(hour=10, minute=0).isoformat()
    with pytest.raises(SystemExit):
        cli.main(_issue_args(tmp_path, run_id=wrong))


@pytest.mark.unit
def test_issue_daily_ingest_rejects_outside_issuance_window(tmp_path, monkeypatch):
    monkeypatch.setattr(
        cli,
        "_issuance_now",
        lambda: _ISSUER_NOW.replace(hour=8, minute=59),
    )
    with pytest.raises(SystemExit):
        cli.main(_issue_args(tmp_path))


@pytest.mark.unit
def test_issue_daily_ingest_rejects_expired_charter(tmp_path):
    argv = _issue_args(tmp_path, charter=str(_charter(tmp_path, days=-1)))
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_budget_over_charter(tmp_path):
    argv = _issue_args(
        tmp_path,
        provider_policy=str(_policy(tmp_path, daily_bytes=50_000_000)),
        charter=str(_charter(tmp_path, daily_bytes=50_000_000)),
    )
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_non_scheduled_run_id(tmp_path):
    argv = _issue_args(tmp_path, run_id="manual__nope")
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_duplicate_scopes(tmp_path):
    scope = "A=2526"
    workloads = [
        {
            "scope": scope,
            "work_item_id": cli._scope_work_item_id(scope),
            "schedule_target_limit": 1,
            "schedule_targets_sha256": "d" * 64,
            "player_pagination_target_limit": 0,
            "match_target_count": 2,
            "match_targets_sha256": "e" * 64,
            "preview_target_count": 2,
            "preview_targets_sha256": "f" * 64,
            "paid_target_count": 5,
        },
        {
            "scope": scope,
            "work_item_id": cli._scope_work_item_id(scope),
            "schedule_target_limit": 1,
            "schedule_targets_sha256": "d" * 64,
            "player_pagination_target_limit": 0,
            "match_target_count": 2,
            "match_targets_sha256": "e" * 64,
            "preview_target_count": 2,
            "preview_targets_sha256": "f" * 64,
            "paid_target_count": 5,
        },
    ]
    argv = _issue_args(tmp_path)
    _plan(tmp_path, workloads=workloads)
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["foreign", "reordered"])
def test_offline_signer_rejects_planner_scope_outside_exact_cohort_prefix(
    tmp_path, mutation
):
    argv = _issue_args(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan = json.loads(plan_path.read_text())
    workloads = plan["scope_workloads"]
    if mutation == "foreign":
        foreign = "ESP-La Liga=2526"
        workloads[0]["scope"] = foreign
        workloads[0]["work_item_id"] = cli._scope_work_item_id(foreign)
    else:
        workloads.reverse()
    plan["workload_sha256"] = _hashlib.sha256(
        cli.canonical_json_bytes(workloads)
    ).hexdigest()
    _private_json(plan_path, plan).chmod(0o440)

    with pytest.raises(SystemExit):
        cli.main(argv)
    assert not (tmp_path / "issuance.json").exists()


@pytest.mark.unit
def test_offline_signer_preserves_nonlexical_owner_cohort_priority(tmp_path):
    argv = _issue_args(tmp_path)
    cohort = {
        **_cohort(),
        "scopes": list(reversed(_cohort()["scopes"])),
    }
    cohort_path = _cohort_file(tmp_path, value=cohort)
    default_plan = json.loads((tmp_path / "plan.json").read_text())
    workloads = list(reversed(default_plan["scope_workloads"]))
    plan_path = _plan(tmp_path, workloads=workloads, cohort=cohort)
    policy_path = Path(argv[argv.index("--provider-policy") + 1])
    charter_path = _charter(
        tmp_path,
        policy_path=policy_path,
        cohort=cohort,
        name="nonlexical-priority",
    )
    for flag, value in (
        ("--cohort-file", cohort_path),
        ("--plan-file", plan_path),
        ("--charter", charter_path),
    ):
        argv[argv.index(flag) + 1] = str(value)

    assert cli.main(argv) == 0

    run_digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{run_digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
    )
    assert approval.scheduled_authority is not None
    assert [
        item.scope for item in approval.scheduled_authority.scope_workloads
    ] == cohort["scopes"]
