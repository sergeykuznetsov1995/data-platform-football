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
    ProxyCampaignValidationError,
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
            "allowed_path_families": list(cli.CANARY_DISCOVERY_PATH_FAMILIES),
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
        },
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
    sign = parser.parse_args(["sign", "--input", "in.json", "--output", "out.json"])
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
def test_bootstrap_parser_rejects_conflicting_secret_sources():
    parser = cli.build_parser()
    required = [
        "issue-bootstrap-ingest",
        "--run-id",
        "scheduled__2026-07-01T10:00:00+00:00",
        "--plan-file",
        "plan.json",
        "--rollout-file",
        "rollout.json",
        "--provider-policy",
        "policy.json",
        "--charter",
        "charter.json",
        "--runtime-sha256",
        "a" * 64,
        "--classifier-sha256",
        "b" * 64,
    ]
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                *required,
                "--secret-file",
                "approval.secret",
                "--secret-env",
                "APPROVAL_SECRET",
            ]
        )


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


def _scope_workload_value(scope, demand):
    return {
        "scope": scope,
        "work_item_id": cli._scope_work_item_id(scope),
        "schedule_target_limit": demand,
        "schedule_targets_sha256": "a" * 64,
        "player_pagination_target_limit": 0,
        "match_target_count": 0,
        "match_targets_sha256": "b" * 64,
        "preview_target_count": 0,
        "preview_targets_sha256": "c" * 64,
        "paid_target_count": demand,
    }


def _rollout(
    *,
    wave_id="wave-20",
    max_scopes=20,
    require_full_active=False,
    scopes=None,
    ranked_scope_ids=None,
    ranking_basis=None,
):
    ranked_ids = (
        ranked_scope_ids
        or scopes
        or [
            "INT-World Cup=2026",
            "ENG-Premier League=2526",
        ]
    )
    basis = ranking_basis or [
        _scope_workload_value(scope, 100 - index)
        for index, scope in enumerate(ranked_ids)
    ]
    bootstrap_slots = cli._bootstrap_slots_from_start("2026-07-16T10:00:00Z")
    return {
        "schema_version": 4,
        "cohort_id": "smoke-prefix",
        "rollout_id": "production-rollout-2026-07",
        "wave_id": wave_id,
        "max_scopes": max_scopes,
        "require_full_active": require_full_active,
        "ranked_scope_ids": ranked_ids,
        "ranked_scope_ids_sha256": cli._scope_specs_sha256(ranked_ids),
        "ranking_basis_workload_sha256": _hashlib.sha256(
            cli.canonical_json_bytes(basis)
        ).hexdigest(),
        "ranking_basis_scope_workloads": basis,
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "promotion_acceptance_sha256": (
            cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
            if wave_id == "wave-20"
            else "d" * 64
        ),
        "promotion_terminal_receipt_sha256": (
            cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
            if wave_id == "wave-20"
            else "e" * 64
        ),
        "acceptance_mode": cli.WHOSCORED_ACCELERATED_BOOTSTRAP_ACCEPTANCE_MODE,
        "bootstrap_slots": bootstrap_slots,
        "capacity_receipt_sha256": "6" * 64,
        "provider_order_cap_bytes": 1_000_000_000,
    }


def _rollout_file(tmp_path, *, value=None):
    return _private_json(tmp_path / "rollout-authority.json", value or _rollout())


def _promotion_receipt_files(
    tmp_path,
    source_rollout,
    *,
    prior_receipt_files=(),
    receipt_classifier_sha256=None,
    source_wave_attempts=2,
):
    from dags.scripts import whoscored_rollout_acceptance as acceptance

    def scope_dq_value(scope):
        return {
            "scope": scope,
            "paid_proxy_bytes": 0,
            "expected_scope_batches": 1,
            "exact_scope_manifests": 1,
            "exact_scope_dataset_mismatches": 0,
            "expected_match_batches": 1,
            "exact_match_manifests": 1,
            "exact_match_dataset_mismatches": 0,
            "expected_match_not_available_batches": 0,
            "exact_match_not_available_manifests": 0,
            "exact_match_not_available_physical_rows": 0,
            "exact_match_outcome_count": 1,
            "exact_match_outcome_duplicates": 0,
            "exact_match_outcome_payload_sha256": _hashlib.sha256(
                f"match:{scope}".encode()
            ).hexdigest(),
            "expected_preview_batches": 1,
            "exact_preview_manifests": 1,
            "exact_preview_dataset_mismatches": 0,
            "expected_preview_not_available_batches": 0,
            "exact_preview_not_available_manifests": 0,
            "exact_preview_not_available_physical_rows": 0,
            "exact_preview_outcome_count": 1,
            "exact_preview_outcome_duplicates": 0,
            "exact_preview_outcome_payload_sha256": _hashlib.sha256(
                f"preview:{scope}".encode()
            ).hexdigest(),
            "schedule_rows": 2,
            "schedule_games": 2,
            "manifest_event_rows": 3,
            "current_event_rows": 3,
            "manifest_lineup_rows": 4,
            "current_lineup_rows": 4,
            "manifest_match_rows": 1,
            "current_match_rows": 1,
            "manifest_substitution_rows": 2,
            "current_substitution_rows": 2,
            "manifest_formation_rows": 2,
            "current_formation_rows": 2,
            "manifest_team_stat_rows": 2,
            "current_team_stat_rows": 2,
            "manifest_player_stat_rows": 4,
            "current_player_stat_rows": 4,
            "duplicate_source_event_ids": 0,
            "duplicate_team_event_ids": 0,
            "scope_manifest_mismatches": 0,
            "manifest_missing_player_rows": 1,
            "current_missing_player_rows": 1,
            "manifest_preview_lineup_rows": 2,
            "current_preview_lineup_rows": 2,
            "manifest_preview_section_rows": 3,
            "current_preview_section_rows": 3,
        }

    profile_dq = {
        "status": "success",
        "expected_profile_batches": 2,
        "exact_profile_manifests": 2,
        "exact_profile_row_mismatches": 0,
        "exact_profile_participation_mismatches": 0,
        "expected_profile_not_available_batches": 0,
        "exact_profile_not_available_manifests": 0,
        "exact_profile_not_available_physical_rows": 0,
        "exact_profile_outcome_count": 2,
        "exact_profile_outcome_duplicates": 0,
        "exact_profile_outcome_payload_sha256": _hashlib.sha256(
            b"profile-outcomes"
        ).hexdigest(),
        "roster_players": 2,
        "current_profile_manifests": 2,
        "current_profile_rows": 2,
        "manifest_participation_rows": 4,
        "current_participation_rows": 4,
        "uncovered_profiles": 0,
        "stale_profiles": 0,
    }

    ranked = list(source_rollout["ranked_scope_ids"])
    source_wave = source_rollout["wave_id"]
    source_index = acceptance.WAVE_ORDER.index(source_wave)
    source_cohort_sha256 = _hashlib.sha256(
        cli.canonical_json_bytes(source_rollout)
    ).hexdigest()
    runtime = {
        "status": "success",
        "transport_policy": "direct_then_paid",
        "direct_only": False,
        "campaign_id": "campaign-954",
        "approval_id": "approval-954",
        "approval_sha256": _hashlib.sha256(b"approval-954").hexdigest(),
        "runtime_contract": {
            "parser_version": acceptance.REQUIRED_PARSER_VERSION,
            "manifest_sha256": "f" * 64,
            "code_tree_sha256": source_rollout["runtime_sha256"],
        },
    }
    records = [
        (
            f"{acceptance.receipts_prefix(source_rollout['rollout_id'])}/{Path(path).name}",
            json.loads(Path(path).read_text()),
        )
        for path in prior_receipt_files
    ]
    previous = None
    if records:
        last_receipt = sorted(records, key=lambda item: item[1]["logical_date"])[-1][1]
        last_logical = datetime.fromisoformat(
            last_receipt["logical_date"].replace("Z", "+00:00")
        )
        previous = {
            "run_id": last_receipt["run_id"],
            "state": "success",
            "logical_date": last_logical,
        }
    first_wave_index = len(records) // 2
    for wave_id in acceptance.WAVE_ORDER[first_wave_index : source_index + 1]:
        maximum = acceptance.WAVE_LIMITS[wave_id]
        selected = ranked[: min(maximum, len(ranked))]
        deferred = sorted(set(ranked) - set(selected))
        scope_plan = {
            "rollout_id": source_rollout["rollout_id"],
            "wave_id": wave_id,
            "max_scopes": maximum,
            "require_full_active": wave_id == "wave-all",
            "active_scopes": selected,
            "active_scope_count": len(selected),
            "active_scopes_sha256": cli._scope_specs_sha256(selected),
            "catalog_active_scope_count": len(ranked),
            "catalog_active_scopes_sha256": cli._scope_specs_sha256(sorted(ranked)),
            "deferred_scope_count": len(deferred),
            "deferred_scopes_sha256": cli._scope_specs_sha256(deferred),
            "cohort_sha256": (
                source_cohort_sha256
                if wave_id == source_wave
                else _hashlib.sha256(f"prior:{wave_id}".encode()).hexdigest()
            ),
            "workload_sha256": _hashlib.sha256(
                f"workload:{wave_id}".encode()
            ).hexdigest(),
            "ranked_scope_ids_sha256": source_rollout["ranked_scope_ids_sha256"],
            "ranked_workload_sha256": _hashlib.sha256(
                f"ranked:{wave_id}".encode()
            ).hexdigest(),
            "runtime_sha256": source_rollout["runtime_sha256"],
            "classifier_sha256": (
                receipt_classifier_sha256 or source_rollout["classifier_sha256"]
            ),
            "promotion_acceptance_sha256": (
                cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
                if wave_id == "wave-20"
                else source_rollout["promotion_acceptance_sha256"]
            ),
            "promotion_terminal_receipt_sha256": (
                cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
                if wave_id == "wave-20"
                else source_rollout["promotion_terminal_receipt_sha256"]
            ),
            "acceptance_mode": source_rollout["acceptance_mode"],
            "bootstrap_slots": source_rollout["bootstrap_slots"],
            "capacity_receipt_sha256": source_rollout["capacity_receipt_sha256"],
            "provider_order_cap_bytes": source_rollout["provider_order_cap_bytes"],
        }
        attempt_count = source_wave_attempts if wave_id == source_wave else 2
        for _attempt in range(attempt_count):
            slot_index = len(records)
            if slot_index >= len(source_rollout["bootstrap_slots"]):
                raise ValueError("test fixture exhausted exact bootstrap slots")
            slot = source_rollout["bootstrap_slots"][slot_index]
            logical_date = datetime.fromisoformat(
                slot["logical_date"].replace("Z", "+00:00")
            )
            run_id = slot["run_id"]
            receipt = acceptance.build_success_receipt(
                run_id=run_id,
                logical_date=logical_date,
                scope_plan=scope_plan,
                runtime_preflight=runtime,
                catalog_dq={
                    "status": "success",
                    "active_scopes": len(ranked),
                    "quarantined": 0,
                },
                profile_dq=profile_dq,
                traffic_dq={
                    "schema_version": 1,
                    "paid_proxy_bytes": 123,
                    "reported_paid_proxy_bytes": 123,
                    "request_ledger_paid_proxy_bytes": 123,
                    "durable_paid_proxy_bytes": 123,
                    "campaign_paid_proxy_bytes": 123,
                    "artifact_sha256": "c" * 64,
                    "artifact_bytes": 42,
                },
                daily_slo={
                    "schema_version": 1,
                    "contract": "bootstrap-slo-v1",
                    "capacity_contract": "cache-capacity-v1",
                    "status": "success",
                    "acceptance_mode": source_rollout["acceptance_mode"],
                    "slot_index": slot_index,
                    "run_id": run_id,
                    "logical_date": slot["logical_date"],
                    "wave_id": slot["wave_id"],
                    "elapsed_seconds": 7_200,
                    "limit_seconds": acceptance.BOOTSTRAP_LIMIT_SECONDS,
                    "capacity_receipt_sha256": source_rollout[
                        "capacity_receipt_sha256"
                    ],
                    "runtime_sha256": source_rollout["runtime_sha256"],
                },
                alert_preflight={
                    "status": "delivered",
                    "campaign_id": "campaign-954",
                    "approval_id": "approval-954",
                    "approval_sha256": runtime["approval_sha256"],
                    "transport_policy": "direct_then_paid",
                },
                scope_dq=[scope_dq_value(scope) for scope in selected],
                terminal_task_states=[
                    {
                        "task_id": "validate_active_scope",
                        "map_index": index,
                        "state": "success",
                    }
                    for index in range(len(selected))
                ]
                + [
                    {
                        "task_id": "final_success_gate",
                        "map_index": -1,
                        "state": "success",
                    }
                ],
                previous_terminal_run=previous,
                existing_records=records,
            )
            receipt_sha256 = _hashlib.sha256(
                acceptance._canonical_json_bytes(receipt)
            ).hexdigest()
            key = f"{acceptance.receipts_prefix(source_rollout['rollout_id'])}/{receipt_sha256}.json"
            records.append((key, receipt))
            previous = {
                "run_id": run_id,
                "state": "success",
                "logical_date": logical_date,
            }
    paths = []
    for key, receipt in records:
        path = _private_json(tmp_path / Path(key).name, receipt)
        paths.append(str(path))
    return paths


@pytest.mark.unit
@pytest.mark.parametrize(
    ("wave_id", "max_scopes", "require_full_active"),
    (
        ("wave-20", 20, False),
        ("wave-70", 70, False),
        ("wave-all", 2_000, True),
    ),
)
def test_rollout_manifest_accepts_only_the_three_reviewed_wave_contracts(
    tmp_path, wave_id, max_scopes, require_full_active
):
    path = _rollout_file(
        tmp_path,
        value=_rollout(
            wave_id=wave_id,
            max_scopes=max_scopes,
            require_full_active=require_full_active,
        ),
    )

    assert cli._read_rollout(path)["wave_id"] == wave_id

    invalid = _rollout(
        wave_id=wave_id,
        max_scopes=max_scopes + 1,
        require_full_active=require_full_active,
    )
    with pytest.raises(cli.CampaignCliError, match="exact wave"):
        cli._read_rollout(_rollout_file(tmp_path, value=invalid))


@pytest.mark.unit
def test_rollout_promotion_preserves_frozen_ranking_across_adjacent_waves(
    tmp_path, capsys
):
    from types import SimpleNamespace

    from dags.scripts import whoscored_rollout_acceptance as acceptance

    def expected_evidence(paths, *, source_wave_id):
        records = [
            (
                f"{acceptance.receipts_prefix(source['rollout_id'])}/{Path(path).name}",
                json.loads(Path(path).read_text()),
            )
            for path in paths
        ]
        terminal = next(
            (key, receipt)
            for key, receipt in records
            if receipt["scope"]["wave_id"] == source_wave_id
            and receipt["wave_accepted"] is True
        )
        return acceptance.promotion_acceptance_evidence(
            records,
            rollout_id=source["rollout_id"],
            source_wave_id=source_wave_id,
            expected_terminal_receipt_sha256=Path(terminal[0]).stem,
        )

    source_path = _rollout_file(tmp_path)
    source = cli._read_rollout(source_path)
    wave_20_receipts = _promotion_receipt_files(tmp_path, source)
    wave_70_path = tmp_path / "rollout-wave-70.json"

    cli.command_promote_rollout(
        SimpleNamespace(
            input=str(source_path),
            cohort_id="wave-70-cohort",
            wave_id="wave-70",
            acceptance_receipt=wave_20_receipts,
            output=str(wave_70_path),
            force=False,
        )
    )

    wave_70 = cli._read_rollout(wave_70_path)
    mutable_fields = {
        "cohort_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    }
    assert {
        field: value for field, value in wave_70.items() if field not in mutable_fields
    } == {
        field: value for field, value in source.items() if field not in mutable_fields
    }
    assert (
        wave_70["wave_id"],
        wave_70["max_scopes"],
        wave_70["require_full_active"],
    ) == (
        "wave-70",
        70,
        False,
    )
    summary = json.loads(capsys.readouterr().out)
    assert (
        summary["cohort_sha256"]
        == _hashlib.sha256(cli.canonical_json_bytes(wave_70)).hexdigest()
    )
    expected_wave_70_proof = expected_evidence(
        wave_20_receipts, source_wave_id="wave-20"
    )
    assert (
        wave_70["promotion_acceptance_sha256"]
        == expected_wave_70_proof["promotion_acceptance_sha256"]
    )
    assert (
        wave_70["promotion_terminal_receipt_sha256"]
        == expected_wave_70_proof["terminal_receipt_sha256"]
    )

    wave_all_path = tmp_path / "rollout-wave-all.json"
    wave_70_receipts = _promotion_receipt_files(
        tmp_path,
        wave_70,
        prior_receipt_files=wave_20_receipts,
    )
    cli.command_promote_rollout(
        SimpleNamespace(
            input=str(wave_70_path),
            cohort_id="wave-all-cohort",
            wave_id="wave-all",
            acceptance_receipt=wave_70_receipts,
            output=str(wave_all_path),
            force=False,
        )
    )
    wave_all = cli._read_rollout(wave_all_path)
    assert (
        wave_all["wave_id"],
        wave_all["max_scopes"],
        wave_all["require_full_active"],
    ) == (
        "wave-all",
        2_000,
        True,
    )
    assert wave_all["rollout_id"] == source["rollout_id"]
    assert wave_all["ranked_scope_ids"] == source["ranked_scope_ids"]
    assert (
        wave_all["ranking_basis_scope_workloads"]
        == source["ranking_basis_scope_workloads"]
    )
    assert (
        wave_all["promotion_acceptance_sha256"]
        != cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
    )
    expected_wave_all_proof = expected_evidence(
        wave_70_receipts, source_wave_id="wave-70"
    )
    assert (
        wave_all["promotion_acceptance_sha256"]
        == expected_wave_all_proof["promotion_acceptance_sha256"]
    )
    assert (
        wave_all["promotion_terminal_receipt_sha256"]
        == expected_wave_all_proof["terminal_receipt_sha256"]
    )


@pytest.mark.unit
def test_rollout_promotion_uses_exact_two_signed_wave_slots(tmp_path):
    from types import SimpleNamespace

    source_path = _rollout_file(tmp_path)
    source = cli._read_rollout(source_path)
    exact_pair = _promotion_receipt_files(tmp_path, source)
    first = json.loads(Path(exact_pair[0]).read_text())
    second = json.loads(Path(exact_pair[1]).read_text())
    assert first["wave_accepted"] is False
    assert second["wave_accepted"] is True
    assert second["previous_run_receipt_sha256"] == Path(exact_pair[0]).stem

    promoted_path = tmp_path / "rollout-wave-70-from-latest-pair.json"
    cli.command_promote_rollout(
        SimpleNamespace(
            input=str(source_path),
            cohort_id="wave-70-latest-pair",
            wave_id="wave-70",
            acceptance_receipt=exact_pair,
            output=str(promoted_path),
            force=False,
        )
    )

    promoted = cli._read_rollout(promoted_path)
    assert promoted["promotion_terminal_receipt_sha256"] == Path(exact_pair[1]).stem


@pytest.mark.unit
def test_rollout_promotion_rejects_missing_or_content_tampered_acceptance_chain(
    tmp_path,
):
    from types import SimpleNamespace

    source_path = _rollout_file(tmp_path)
    source = cli._read_rollout(source_path)
    receipts = _promotion_receipt_files(tmp_path, source)
    common = {
        "input": str(source_path),
        "cohort_id": "wave-70-cohort",
        "wave_id": "wave-70",
        "output": str(tmp_path / "promoted.json"),
        "force": False,
    }
    with pytest.raises(cli.CampaignCliError, match="exact two-receipt"):
        cli.command_promote_rollout(
            SimpleNamespace(**common, acceptance_receipt=receipts[:1])
        )

    tampered = Path(receipts[0])
    payload = json.loads(tampered.read_text())
    payload["run_id"] = payload["run_id"] + "-tampered"
    _private_json(tampered, payload)
    with pytest.raises(cli.CampaignCliError, match="acceptance evidence is invalid"):
        cli.command_promote_rollout(
            SimpleNamespace(**common, acceptance_receipt=receipts)
        )


@pytest.mark.unit
def test_rollout_promotion_rejects_rehashed_receipt_authority_preimage_drift(
    tmp_path,
):
    from types import SimpleNamespace

    source_path = _rollout_file(tmp_path)
    source = cli._read_rollout(source_path)
    receipts = _promotion_receipt_files(
        tmp_path,
        source,
        receipt_classifier_sha256="f" * 64,
    )

    with pytest.raises(cli.CampaignCliError, match="frozen rollout/release"):
        cli.command_promote_rollout(
            SimpleNamespace(
                input=str(source_path),
                cohort_id="wave-70-cohort",
                wave_id="wave-70",
                acceptance_receipt=receipts,
                output=str(tmp_path / "promoted.json"),
                force=False,
            )
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("source_wave", "source_maximum", "source_full", "target_wave"),
    (
        ("wave-20", 20, False, "wave-all"),
        ("wave-70", 70, False, "wave-20"),
        ("wave-all", 2_000, True, "wave-70"),
    ),
)
def test_rollout_promotion_rejects_skip_backward_and_terminal_transitions(
    tmp_path, source_wave, source_maximum, source_full, target_wave
):
    from types import SimpleNamespace

    source = _rollout(
        wave_id=source_wave,
        max_scopes=source_maximum,
        require_full_active=source_full,
    )
    source_path = _rollout_file(tmp_path, value=source)

    with pytest.raises(cli.CampaignCliError, match="promotion must be exactly"):
        cli.command_promote_rollout(
            SimpleNamespace(
                input=str(source_path),
                cohort_id="next-wave-cohort",
                wave_id=target_wave,
                output=str(tmp_path / "invalid-promotion.json"),
                force=False,
            )
        )


@pytest.mark.unit
@pytest.mark.parametrize("tamper", ("ranked_identity", "ranking_basis"))
def test_rollout_promotion_rejects_tampered_frozen_identity(tmp_path, tamper):
    from types import SimpleNamespace

    source = _rollout()
    if tamper == "ranked_identity":
        source["ranked_scope_ids_sha256"] = "0" * 64
    else:
        source["ranking_basis_scope_workloads"][0]["paid_target_count"] += 1
    source_path = _rollout_file(tmp_path, value=source)

    with pytest.raises(cli.CampaignCliError, match="rollout"):
        cli.command_promote_rollout(
            SimpleNamespace(
                input=str(source_path),
                cohort_id="wave-70-cohort",
                wave_id="wave-70",
                output=str(tmp_path / "tampered-promotion.json"),
                force=False,
            )
        )


@pytest.mark.unit
def test_rollout_creation_freezes_heavy_order_while_daily_demand_refreshes(
    monkeypatch, tmp_path
):
    from types import SimpleNamespace
    from dags.scripts import run_whoscored_scraper as runner

    scope_specs = ["A=2526", "B=2526", "C=2526"]
    scopes = [
        SimpleNamespace(spec=spec, competition_id=spec[0], season_id="2526")
        for spec in scope_specs
    ]
    runtimes = [SimpleNamespace(scope=object()) for _scope in scopes]
    catalog = SimpleNamespace(
        to_rows=lambda: {
            "competitions": [{}],
            "seasons": [{}],
            "stages": [{}],
        }
    )
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-parent",
                "catalog_payload_sha256": "c" * 64,
            },
            catalog,
        ),
        profile_candidate_snapshot=lambda **_kwargs: SimpleNamespace(
            count=0, payload_sha256="e" * 64
        ),
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda *_args, **_kwargs: list(reversed(list(zip(scopes, runtimes)))),
    )
    demand = {"A=2526": 10, "B=2526": 20, "C=2526": 20}

    def planned(_repository, *, scope, runtime):
        del _repository, runtime
        count = demand[scope.spec]
        return {
            "scope": scope.spec,
            "work_item_id": cli._scope_work_item_id(scope.spec),
            "schedule_target_limit": count,
            "schedule_targets_sha256": "a" * 64,
            "player_pagination_target_limit": 0,
            "match_target_count": 0,
            "match_targets_sha256": "b" * 64,
            "preview_target_count": 0,
            "preview_targets_sha256": "c" * 64,
            "paid_target_count": count,
        }

    monkeypatch.setattr(cli, "_planned_scope_workload", planned)
    rollout = tmp_path / "created-rollout.json"
    cli.command_create_rollout(
        SimpleNamespace(
            rollout_id="production-rollout-2026-07",
            cohort_id="wave-20-cohort",
            wave_id="wave-20",
            runtime_sha256="a" * 64,
            classifier_sha256="b" * 64,
            capacity_receipt_sha256="6" * 64,
            provider_order_cap_bytes=1_000_000_000,
            bootstrap_start_logical_date="2026-07-14T10:00:00Z",
            output=str(rollout),
            force=False,
        )
    )
    frozen = cli._read_rollout(rollout)
    expected = ["B=2526", "C=2526", "A=2526"]
    assert frozen["ranked_scope_ids"] == expected

    # Exact due workloads are refreshed daily, but cannot reorder the rollout
    # and reset the two-scheduled-run acceptance identity.
    demand.update({"A=2526": 30, "B=2526": 5, "C=2526": 5})
    output = tmp_path / "heavy-first-plan.json"

    cli.command_plan_daily_ingest(
        SimpleNamespace(
            rollout_file=str(rollout),
            output=str(output),
            force=False,
        )
    )

    plan = cli._read_daily_plan(output)
    assert [item["scope"] for item in plan["ranked_scope_workloads"]] == expected
    assert [item["scope"] for item in plan["scope_workloads"]] == expected
    assert [item["paid_target_count"] for item in plan["scope_workloads"]] == [
        5,
        5,
        30,
    ]
    assert plan["ranked_scope_ids_sha256"] == frozen["ranked_scope_ids_sha256"]
    assert plan["catalog_active_scope_count"] == 3
    assert plan["catalog_active_scopes_sha256"] == cli._scope_specs_sha256(scope_specs)


@pytest.mark.unit
@pytest.mark.parametrize("wave_id", ("wave-70", "wave-all"))
def test_rollout_creation_rejects_non_initial_waves(wave_id):
    from types import SimpleNamespace

    with pytest.raises(cli.CampaignCliError, match="can only create wave-20"):
        cli.command_create_rollout(
            SimpleNamespace(
                rollout_id="production-rollout-2026-07",
                cohort_id=f"{wave_id}-cohort",
                wave_id=wave_id,
                output="unused.json",
                force=False,
            )
        )


@pytest.mark.unit
def test_rollout_creation_rejects_bootstrap_slots_not_all_backdated():
    from types import SimpleNamespace

    with pytest.raises(cli.CampaignCliError, match="all six.*backdated"):
        cli.command_create_rollout(
            SimpleNamespace(
                rollout_id="production-rollout-2026-07",
                cohort_id="wave-20-cohort",
                wave_id="wave-20",
                runtime_sha256="a" * 64,
                classifier_sha256="b" * 64,
                capacity_receipt_sha256="6" * 64,
                provider_order_cap_bytes=1_000_000_000,
                bootstrap_start_logical_date="2999-01-01T10:00:00Z",
                output="unused.json",
                force=False,
            )
        )


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
    rollout = _private_json(
        tmp_path / "rollout.json",
        _rollout(scopes=[scope.spec]),
    )
    output = tmp_path / "daily-plan.json"

    cli.command_plan_daily_ingest(
        SimpleNamespace(
            rollout_file=str(rollout),
            output=str(output),
            force=False,
        )
    )

    plan = cli._read_daily_plan(output)
    workload = plan["scope_workloads"][0]
    assert plan["schema_version"] == 4
    assert plan["max_scopes"] == 20
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
def test_daily_planner_rejects_match_backlog_above_runtime_cap(monkeypatch, tmp_path):
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
                to_rows=lambda: {"competitions": [{}], "seasons": [{}], "stages": [{}]}
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
    rollout = _private_json(
        tmp_path / "rollout.json",
        _rollout(scopes=[scope.spec]),
    )

    with pytest.raises(cli.CampaignCliError, match="backlog exceeds"):
        cli.command_plan_daily_ingest(
            SimpleNamespace(
                rollout_file=str(rollout),
                output=str(tmp_path / "plan.json"),
                force=False,
            )
        )


def _plan(
    tmp_path,
    *,
    workloads=None,
    profile_target_count=12,
    rollout=None,
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
    if rollout is None:
        rollout_value = _rollout()
        by_scope = {str(item["scope"]): item for item in values}
        if set(by_scope) == set(rollout_value["ranked_scope_ids"]):
            ranked_values = [
                by_scope[str(scope)] for scope in rollout_value["ranked_scope_ids"]
            ]
        else:
            ranked_values = sorted(
                values,
                key=lambda workload: (
                    -int(workload["paid_target_count"]),
                    str(workload["scope"]),
                ),
            )
            rollout_value = _rollout(
                ranked_scope_ids=[str(item["scope"]) for item in ranked_values],
                ranking_basis=ranked_values,
            )
    else:
        rollout_value = rollout
        by_scope = {str(item["scope"]): item for item in values}
        ranked_values = [
            by_scope[str(scope)] for scope in rollout_value["ranked_scope_ids"]
        ]
    cohort_sha = _hashlib.sha256(cli.canonical_json_bytes(rollout_value)).hexdigest()
    path = _private_json(
        tmp_path / "plan.json",
        {
            "schema_version": 4,
            "cohort_id": "smoke-prefix",
            "cohort_sha256": cohort_sha,
            "rollout_id": rollout_value["rollout_id"],
            "wave_id": rollout_value["wave_id"],
            "max_scopes": rollout_value["max_scopes"],
            "require_full_active": rollout_value["require_full_active"],
            "catalog_batch_id": "wsc2-parent",
            "catalog_payload_sha256": "c" * 64,
            "catalog_active_scope_count": len(ranked_values),
            "catalog_active_scopes_sha256": cli._scope_specs_sha256(
                sorted(str(item["scope"]) for item in ranked_values)
            ),
            "ranked_scope_ids_sha256": rollout_value["ranked_scope_ids_sha256"],
            "ranked_workload_sha256": _hashlib.sha256(
                cli.canonical_json_bytes(ranked_values)
            ).hexdigest(),
            "ranked_scope_workloads": ranked_values,
            "workload_sha256": _hashlib.sha256(
                cli.canonical_json_bytes(
                    ranked_values[
                        : min(rollout_value["max_scopes"], len(ranked_values))
                    ]
                )
            ).hexdigest(),
            "scope_workloads": ranked_values[
                : min(rollout_value["max_scopes"], len(ranked_values))
            ],
            "discovery_parent_target_count": 100,
            "discovery_expansion_headroom": (
                cli.SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
            ),
            "discovery_target_limit": (
                100 + cli.SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
            ),
            "profile_target_count": profile_target_count,
            "profile_targets_sha256": "e" * 64,
            "acceptance_mode": rollout_value["acceptance_mode"],
            "bootstrap_slots": rollout_value["bootstrap_slots"],
            "capacity_receipt_sha256": rollout_value["capacity_receipt_sha256"],
            "provider_order_cap_bytes": rollout_value["provider_order_cap_bytes"],
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
        "valid_until": (valid_until or now + timedelta(days=max(days, 1))).isoformat(),
        "receipt_sha256": "d" * 64,
        "provider_quota_bytes": 1_000_000_000,
        "safety_cap_bytes": 1_000_000_000,
        "daily_cap_bytes": daily_bytes,
        "monthly_cap_bytes": 950_000_000,
        "order_cap_bytes": 1_000_000_000,
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
    daily_bytes=150_000_000,
    days=25,
    policy_path=None,
    valid_from=None,
    valid_until=None,
    name="default",
    rollout=None,
):
    now = _ISSUER_NOW
    policy_path = policy_path or _policy(tmp_path, days=days, name=name)
    policy = json.loads(policy_path.read_text())
    rollout_value = rollout or _rollout()
    cohort_sha = _hashlib.sha256(cli.canonical_json_bytes(rollout_value)).hexdigest()
    unsigned = {
        "schema_version": 5,
        "source": "whoscored",
        "provider_policy_sha256": policy["document_sha256"],
        "order_id": policy["order_id"],
        "billing_month": now.strftime("%Y-%m"),
        "cohort_id": rollout_value["cohort_id"],
        "cohort_sha256": cohort_sha,
        "rollout_id": rollout_value["rollout_id"],
        "wave_id": rollout_value["wave_id"],
        "max_scopes": rollout_value["max_scopes"],
        "require_full_active": rollout_value["require_full_active"],
        "ranked_scope_ids_sha256": rollout_value["ranked_scope_ids_sha256"],
        "runtime_sha256": rollout_value["runtime_sha256"],
        "classifier_sha256": rollout_value["classifier_sha256"],
        "promotion_acceptance_sha256": rollout_value["promotion_acceptance_sha256"],
        "promotion_terminal_receipt_sha256": rollout_value[
            "promotion_terminal_receipt_sha256"
        ],
        "acceptance_mode": rollout_value["acceptance_mode"],
        "bootstrap_slots": rollout_value["bootstrap_slots"],
        "capacity_receipt_sha256": rollout_value["capacity_receipt_sha256"],
        "provider_order_cap_bytes": rollout_value["provider_order_cap_bytes"],
        "valid_from": (valid_from or now - timedelta(minutes=5)).isoformat(),
        "valid_until": (valid_until or now + timedelta(days=days)).isoformat(),
        "daily_cap_bytes": daily_bytes,
        "monthly_cap_bytes": 950_000_000,
        "order_cap_bytes": 950_000_000,
        "max_issuances": 10,
        "signature_algorithm": "hmac-sha256",
    }
    signed = cli._sign_authority_document(
        unsigned,
        expected_fields=cli._CHARTER_UNSIGNED_FIELDS,
        secret="o" * 40,
    )
    return _private_json(tmp_path / f"charter-{name}-{daily_bytes}-{days}.json", signed)


_RUN_ID = "scheduled__" + cli._expected_daily_logical_date(_ISSUER_NOW).isoformat()


def _issue_args(tmp_path, **overrides):
    args = {
        "run_id": _RUN_ID,
        "plan_file": str(overrides.get("plan_file") or _plan(tmp_path)),
        "rollout_file": str(overrides.get("rollout_file") or _rollout_file(tmp_path)),
        "provider_policy": str(overrides.get("provider_policy") or _policy(tmp_path)),
        "charter": str(overrides.get("charter") or _charter(tmp_path)),
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
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
        "--run-id",
        args["run_id"],
        "--plan-file",
        args["plan_file"],
        "--rollout-file",
        args["rollout_file"],
        "--provider-policy",
        args["provider_policy"],
        "--charter",
        args["charter"],
        "--runtime-sha256",
        args["runtime_sha256"],
        "--classifier-sha256",
        args["classifier_sha256"],
        "--approval-root",
        args["approval_root"],
        "--pointer-root",
        args["pointer_root"],
        "--secret-file",
        args["secret_file"],
        "--owner-secret-file",
        args["owner_secret_file"],
        "--issuance-ledger-secret-file",
        args["issuance_ledger_secret_file"],
        "--issuance-ledger",
        args["issuance_ledger"],
    ]
    if args.get("issued_at"):
        argv.extend(("--issued-at", str(args["issued_at"])))
    if args.get("expires_at"):
        argv.extend(("--expires-at", str(args["expires_at"])))
    return argv


def _bootstrap_issue_args(tmp_path, *, slot_index=0, **overrides):
    argv = _issue_args(
        tmp_path,
        run_id=_rollout()["bootstrap_slots"][slot_index]["run_id"],
        **overrides,
    )
    argv[0] = "issue-bootstrap-ingest"
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
    assert approval.scheduled_authority.rollout_id == "production-rollout-2026-07"
    assert approval.scheduled_authority.wave_id == "wave-20"
    assert approval.scheduled_authority.max_scopes == 20
    assert approval.scheduled_authority.require_full_active is False
    assert (
        approval.scheduled_authority.ranked_scope_ids_sha256
        == _rollout()["ranked_scope_ids_sha256"]
    )

    legacy = json.loads(approval_path.read_text())
    del legacy["scheduled_authority"]["acceptance_mode"]
    with pytest.raises(ProxyCampaignValidationError, match="fields are invalid"):
        ProxyCampaignApproval.from_dict(legacy)


@pytest.mark.unit
def test_issue_daily_ingest_carries_fail_closed_wave_all_authority(tmp_path):
    rollout = _rollout(
        wave_id="wave-all",
        max_scopes=2_000,
        require_full_active=True,
    )
    rollout_path = _rollout_file(tmp_path, value=rollout)
    plan_path = _plan(tmp_path, rollout=rollout)
    policy_path = _policy(tmp_path)
    charter_path = _charter(
        tmp_path,
        policy_path=policy_path,
        rollout=rollout,
        name="wave-all",
    )
    prior_states = [
        _ledger_rollout_state(
            wave,
            cohort=wave,
            ranked_scope_ids_sha256=rollout["ranked_scope_ids_sha256"],
        )
        for wave in ("wave-20", "wave-70")
    ]
    _write_rollout_ledger(tmp_path / "issuance.json", prior_states)

    assert (
        cli.main(
            _issue_args(
                tmp_path,
                rollout_file=str(rollout_path),
                plan_file=str(plan_path),
                provider_policy=str(policy_path),
                charter=str(charter_path),
            )
        )
        == 0
    )

    digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
    )
    authority = approval.scheduled_authority
    assert authority is not None
    assert authority.wave_id == "wave-all"
    assert authority.max_scopes == 2_000
    assert authority.require_full_active is True
    assert len(authority.scope_workloads) == authority.catalog_active_scope_count == 2
    assert authority.catalog_active_scopes_sha256 == cli._scope_specs_sha256(
        sorted(item.scope for item in authority.scope_workloads)
    )


@pytest.mark.unit
def test_issue_daily_ingest_rejects_first_spend_on_promoted_wave(tmp_path):
    rollout = _rollout(
        wave_id="wave-70",
        max_scopes=70,
        require_full_active=False,
    )
    with pytest.raises(SystemExit):
        cli.main(
            _issue_args(
                tmp_path,
                rollout_file=str(_rollout_file(tmp_path, value=rollout)),
                plan_file=str(_plan(tmp_path, rollout=rollout)),
                charter=str(_charter(tmp_path, rollout=rollout, name="wave-70")),
            )
        )


@pytest.mark.unit
def test_provider_policy_cannot_raise_code_owned_one_gb_safety_cap(tmp_path):
    now = _ISSUER_NOW
    unsigned = {
        "schema_version": 1,
        "source": "whoscored",
        "provider_id": "proxysio",
        "order_id": "proxysio-38950",
        "plan_id": "bronze-1gb",
        "valid_from": (now - timedelta(minutes=5)).isoformat(),
        "valid_until": (now + timedelta(days=1)).isoformat(),
        "receipt_sha256": "d" * 64,
        "provider_quota_bytes": 1_000_000_001,
        "safety_cap_bytes": 1_000_000_001,
        "daily_cap_bytes": 300_000_000,
        "monthly_cap_bytes": 950_000_000,
        "order_cap_bytes": 1_000_000_001,
        "signature_algorithm": "hmac-sha256",
    }
    signed = cli._sign_authority_document(
        unsigned,
        expected_fields=cli._PROVIDER_POLICY_UNSIGNED_FIELDS,
        secret="o" * 40,
    )
    path = _private_json(tmp_path / "over-cap-policy.json", signed)

    with pytest.raises(cli.CampaignCliError, match="inconsistent"):
        cli._signed_provider_policy(path, owner_secret="o" * 40, now=now)


@pytest.mark.unit
def test_offline_signer_accepts_wrapper_private_read_only_inputs(tmp_path):
    argv = _issue_args(tmp_path)
    for flag in (
        "--rollout-file",
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

    assert (
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
            )
        )
        == 0
    )

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
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
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
def test_issuance_ledger_rejects_changed_ranking_for_same_rollout_id(tmp_path):
    argv = _issue_args(tmp_path)
    assert cli.main(argv) == 0
    ledger_path = tmp_path / "issuance.json"
    ledger = json.loads(ledger_path.read_text())
    ledger["entries"][0]["ranked_scope_ids_sha256"] = "0" * 64
    body = {
        "schema_version": cli.ISSUANCE_LEDGER_SCHEMA_VERSION,
        "entries": ledger["entries"],
    }
    _private_json(
        ledger_path,
        cli._seal_issuance_ledger(body, secret="l" * 40),
    )

    with pytest.raises(SystemExit):
        cli.main(argv)


def _ledger_rollout_state(
    wave_id,
    *,
    cohort="c",
    runtime="a",
    classifier="b",
    ranked_scope_ids_sha256="9" * 64,
):
    maximum, require_full_active = cli._ROLLOUT_WAVE_CONTRACTS[wave_id]
    genesis = cli.WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
    proof_token = {"wave-70": "7", "wave-all": "5"}.get(wave_id, "")
    terminal_token = {"wave-70": "8", "wave-all": "6"}.get(wave_id, "")
    return {
        "rollout_id": "production-rollout-2026-07",
        "wave_id": wave_id,
        "max_scopes": maximum,
        "require_full_active": require_full_active,
        "cohort_sha256": _hashlib.sha256(cohort.encode()).hexdigest(),
        "ranked_scope_ids_sha256": ranked_scope_ids_sha256,
        "runtime_sha256": runtime * 64,
        "classifier_sha256": classifier * 64,
        "promotion_acceptance_sha256": (
            genesis if wave_id == "wave-20" else proof_token * 64
        ),
        "promotion_terminal_receipt_sha256": (
            genesis if wave_id == "wave-20" else terminal_token * 64
        ),
        "acceptance_mode": cli.WHOSCORED_ACCELERATED_BOOTSTRAP_ACCEPTANCE_MODE,
        "bootstrap_slots": cli._bootstrap_slots_from_start("2026-07-16T10:00:00Z"),
        "capacity_receipt_sha256": "6" * 64,
        "provider_order_cap_bytes": 1_000_000_000,
    }


def _write_rollout_ledger(path, states):
    entries = []
    for index, state in enumerate(states):
        entries.append(
            {
                "run_id": f"scheduled__2026-07-{index + 1:02d}T10:00:00+00:00",
                "request_sha256": _hashlib.sha256(
                    f"request:{index}".encode()
                ).hexdigest(),
                "order_id": "prior-order",
                "billing_month": "2026-07",
                "day": f"2026-07-{index + 1:02d}",
                "charter_sha256": _hashlib.sha256(
                    f"charter:{index}".encode()
                ).hexdigest(),
                "provider_policy_sha256": "d" * 64,
                "receipt_sha256": "e" * 64,
                "provider_order_cap_bytes": 1_000_000_000,
                "safety_reserve_bytes": 50_000_000,
                "issuance_mode": "daily",
                "bootstrap_slot_index": None,
                **state,
                "total_provider_bytes": 1,
                "approval": {},
                "pointer": {},
            }
        )
    body = {
        "schema_version": cli.ISSUANCE_LEDGER_SCHEMA_VERSION,
        "entries": entries,
    }
    _private_json(path, cli._seal_issuance_ledger(body, secret="l" * 40))


@pytest.mark.unit
def test_issuance_rollout_sequence_is_adjacent_monotonic_and_release_pinned():
    wave_20 = _ledger_rollout_state("wave-20", cohort="20")
    wave_70 = _ledger_rollout_state("wave-70", cohort="70")
    wave_all = _ledger_rollout_state("wave-all", cohort="all")
    cli._validate_issuance_rollout_sequence(
        [wave_20, wave_20, wave_70, wave_70, wave_all]
    )

    invalid_sequences = (
        [wave_70],
        [wave_20, wave_all],
        [wave_20, wave_70, wave_20],
        [wave_20, {**wave_20, "runtime_sha256": "f" * 64}],
        [wave_20, {**wave_20, "classifier_sha256": "f" * 64}],
        [wave_20, {**wave_20, "cohort_sha256": "f" * 64}],
    )
    for values in invalid_sequences:
        with pytest.raises(cli.CampaignCliError):
            cli._validate_issuance_rollout_sequence(values)


@pytest.mark.unit
def test_issue_daily_ingest_rejects_release_pin_drift_before_spend(tmp_path):
    with pytest.raises(SystemExit):
        cli.main(_issue_args(tmp_path, runtime_sha256="f" * 64))


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
def test_issue_daily_ingest_derives_budget_from_signed_charter(tmp_path):
    policy = _policy(tmp_path)
    charter = _charter(
        tmp_path,
        daily_bytes=50_000_000,
        policy_path=policy,
        name="signed-budget",
    )
    assert (
        cli.main(
            _issue_args(
                tmp_path,
                provider_policy=str(policy),
                charter=str(charter),
            )
        )
        == 0
    )

    digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
    )
    assert approval.caps.total_provider_bytes == 50_000_000


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
def test_offline_signer_rejects_planner_scope_outside_exact_rollout_wave(
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
def test_offline_signer_preserves_exact_heavy_first_scope_priority(tmp_path):
    argv = _issue_args(tmp_path)
    assert cli.main(argv) == 0

    run_digest = _hashlib.sha256(_RUN_ID.encode()).hexdigest()
    pointer = json.loads((tmp_path / "ptr" / f"{run_digest}.json").read_text())
    approval = ProxyCampaignApproval.from_dict(
        json.loads((tmp_path / "appr" / f"{pointer['approval_id']}.json").read_text())
    )
    assert approval.scheduled_authority is not None
    assert [item.scope for item in approval.scheduled_authority.scope_workloads] == [
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    ]
    assert approval.scheduled_authority.rollout_id == "production-rollout-2026-07"
    assert approval.scheduled_authority.wave_id == "wave-20"


@pytest.mark.unit
def test_bootstrap_projection_is_content_addressed_and_idempotent(tmp_path):
    rollout_path = _rollout_file(tmp_path)
    policy_path = _policy(tmp_path)
    charter_path = _charter(tmp_path, policy_path=policy_path)
    argv = [
        "publish-bootstrap-authority",
        "--rollout-file",
        str(rollout_path),
        "--provider-policy",
        str(policy_path),
        "--charter",
        str(charter_path),
        "--pointer-root",
        str(tmp_path / "ptr"),
        "--owner-secret-file",
        str(_owner_secret_file(tmp_path)),
        "--issuance-ledger-secret-file",
        str(_ledger_secret_file(tmp_path)),
    ]
    assert cli.main(argv) == 0
    assert cli.main(argv) == 0
    path = tmp_path / "ptr" / "bootstrap.json"
    assert cli.main(argv) == 0
    value = json.loads(path.read_text())
    body = {
        key: value[key] for key in value if key not in {"authority_sha256", "signature"}
    }
    assert (
        value["authority_sha256"]
        == _hashlib.sha256(cli.canonical_json_bytes(body)).hexdigest()
    )
    assert "charter_sha256" not in value
    assert stat.S_IMODE(path.stat().st_mode) == 0o440

    value["provider_order_cap_bytes"] -= 1
    _private_json(path, value)
    with pytest.raises(SystemExit):
        cli.main(argv)


@pytest.mark.unit
def test_bootstrap_issuance_is_exact_ordered_and_idempotent(tmp_path):
    argv = _bootstrap_issue_args(tmp_path, slot_index=0)
    assert cli.main(argv) == 0
    assert cli.main(argv) == 0
    ledger = json.loads((tmp_path / "issuance.json").read_text())
    assert len(ledger["entries"]) == 1
    assert ledger["entries"][0]["bootstrap_slot_index"] == 0
    assert ledger["entries"][0]["safety_reserve_bytes"] == 50_000_000
    assert cli.main(_bootstrap_issue_args(tmp_path, slot_index=1)) == 0
    ledger = json.loads((tmp_path / "issuance.json").read_text())
    assert len(ledger["entries"]) == 2
    assert {entry["day"] for entry in ledger["entries"]} == {
        _ISSUER_NOW.date().isoformat()
    }
    assert (
        sum(entry["total_provider_bytes"] for entry in ledger["entries"]) == 300_000_000
    )

    # Slot 1 is authorized by the same wave but cannot be replayed before slot 0.
    other = tmp_path / "out-of-order"
    other.mkdir()
    with pytest.raises(SystemExit):
        cli.main(_bootstrap_issue_args(other, slot_index=1))


@pytest.mark.unit
def test_bootstrap_issuance_aggregates_actual_day_before_artifact_publish(tmp_path):
    policy = _policy(tmp_path, daily_bytes=300_000_000, name="bootstrap-daily")
    charter = _charter(
        tmp_path,
        daily_bytes=160_000_000,
        policy_path=policy,
        name="bootstrap-daily",
    )
    overrides = {
        "provider_policy": str(policy),
        "charter": str(charter),
    }
    assert cli.main(_bootstrap_issue_args(tmp_path, slot_index=0, **overrides)) == 0

    with pytest.raises(SystemExit):
        cli.main(_bootstrap_issue_args(tmp_path, slot_index=1, **overrides))

    ledger = json.loads((tmp_path / "issuance.json").read_text())
    assert len(ledger["entries"]) == 1
    assert ledger["entries"][0]["day"] == _ISSUER_NOW.date().isoformat()
    second_run = _rollout()["bootstrap_slots"][1]["run_id"]
    second_digest = _hashlib.sha256(second_run.encode()).hexdigest()
    assert not (tmp_path / "ptr" / f"{second_digest}.json").exists()


@pytest.mark.unit
def test_bootstrap_issuance_serializes_concurrent_duplicate_calls(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    argv = _bootstrap_issue_args(tmp_path, slot_index=0)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: cli.main(argv), range(2)))
    assert results == [0, 0]
    ledger = json.loads((tmp_path / "issuance.json").read_text())
    assert len(ledger["entries"]) == 1


@pytest.mark.unit
def test_bootstrap_issuance_stops_before_provider_order_overspend(tmp_path):
    argv = _bootstrap_issue_args(tmp_path, slot_index=0)
    policy = json.loads(Path(argv[argv.index("--provider-policy") + 1]).read_text())
    charter = json.loads(Path(argv[argv.index("--charter") + 1]).read_text())
    prior = _ledger_rollout_state(
        "wave-20",
        cohort="smoke",
        ranked_scope_ids_sha256=_rollout()["ranked_scope_ids_sha256"],
    )
    ledger_path = tmp_path / "issuance.json"
    _write_rollout_ledger(ledger_path, [prior])
    ledger = json.loads(ledger_path.read_text())
    entry = ledger["entries"][0]
    entry["order_id"] = "proxysio-38950"
    entry["provider_policy_sha256"] = policy["document_sha256"]
    entry["receipt_sha256"] = policy["receipt_sha256"]
    entry["cohort_sha256"] = charter["cohort_sha256"]
    entry["total_provider_bytes"] = 900_000_000
    body = {
        "schema_version": cli.ISSUANCE_LEDGER_SCHEMA_VERSION,
        "entries": ledger["entries"],
    }
    _private_json(ledger_path, cli._seal_issuance_ledger(body, secret="l" * 40))

    with pytest.raises(SystemExit):
        cli.main(argv)
    assert not (tmp_path / "ptr").exists()
    assert not (tmp_path / "appr").exists()
