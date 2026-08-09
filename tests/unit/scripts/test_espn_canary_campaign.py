from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import threading

import pytest
import yaml

import scripts.espn_canary_campaign as campaign_operator
from scripts.espn_canary_campaign import (
    claim_campaign_attempt,
    finish_campaign_attempt,
)
from scrapers.espn.canary_campaign import CampaignError


UTC = timezone.utc
NOW = datetime(2026, 8, 8, 12, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.usefixtures("espn_canary_test_owner")
_TEST_OWNER_PRELUDE = """
import os
import sys
import scripts.espn_canary_campaign as campaign_operator

assert campaign_operator._required_shared_owner() == (0, 0)
campaign_operator._required_shared_owner = lambda: (os.geteuid(), os.getegid())
"""


class InjectedCrash(RuntimeError):
    pass


def _scopes(count: int = 181) -> tuple[str, ...]:
    return tuple(f"{index}:2026" for index in range(1, count + 1))


def _claim(path, **changes):
    values = {
        "ledger_path": path,
        "release_commit": "a" * 40,
        "release_tree_sha256": "b" * 64,
        "registry_signature": "c" * 64,
        "target_scope_ids": _scopes(),
        "now": NOW,
    }
    values.update(changes)
    return claim_campaign_attempt(**values)


@pytest.mark.unit
def test_operator_guard_only_does_not_create_or_consume_ledger(tmp_path):
    path = tmp_path / "campaigns.json"
    result = _claim(path, guard_only=True)

    assert result["attempt"]["ordinal"] is None
    assert result["ledger_ref"] is None
    assert not path.exists()
    assert list(tmp_path.iterdir()) == []

    claimed = _claim(path)
    assert claimed["attempt"]["ordinal"] == 1
    assert path.stat().st_mode & 0o777 == 0o660


@pytest.mark.unit
@pytest.mark.parametrize("count", [180, 182])
def test_operator_rejects_non_exact_canary_target(tmp_path, count):
    with pytest.raises(CampaignError, match="exact 181"):
        _claim(tmp_path / "campaigns.json", target_scope_ids=_scopes(count))


@pytest.mark.unit
def test_operator_persists_failure_and_next_same_campaign_ordinal(tmp_path):
    path = tmp_path / "campaigns.json"
    first = _claim(path)
    failure_ref = {"uri": "s3://evidence/failure.json", "sha256": "d" * 64}
    finished = finish_campaign_attempt(
        ledger_path=path,
        attempt_id=first["attempt"]["attempt_id"],
        terminal_ref=failure_ref,
        successful=False,
        now=NOW,
    )
    second = _claim(path)

    assert finished["status"] == "failed"
    assert second["attempt"]["ordinal"] == 2
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert [item["status"] for item in persisted["attempts"]] == ["failed", "active"]


@pytest.mark.unit
def test_operator_preserves_exact_same_campaign_ordinal001_through_003(tmp_path):
    ledger_path = tmp_path / "campaigns.json"

    for ordinal in range(1, 4):
        claimed = _claim(ledger_path)
        assert claimed["attempt"]["ordinal"] == ordinal
        finish_campaign_attempt(
            ledger_path=ledger_path,
            attempt_id=claimed["attempt"]["attempt_id"],
            terminal_ref={
                "uri": f"s3://evidence/failure-{ordinal}.json",
                "sha256": str(ordinal) * 64,
            },
            successful=False,
            now=NOW,
        )

    with pytest.raises(CampaignError, match="three attempts"):
        _claim(ledger_path)


@pytest.mark.unit
def test_claim_and_finish_evidence_are_hashed_and_immutable(tmp_path):
    path = tmp_path / "campaigns.json"
    claimed = _claim(path)
    claim_ref = claimed["claim_ref"]
    claim_path = Path(claim_ref["uri"].removeprefix("file://"))
    original_claim = claim_path.read_bytes()

    finished = finish_campaign_attempt(
        ledger_path=path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "d" * 64},
        successful=False,
        now=NOW,
    )

    assert hashlib.sha256(original_claim).hexdigest() == claim_ref["sha256"]
    assert claim_path.read_bytes() == original_claim
    assert finished["finish_ref"]["uri"] != claim_ref["uri"]
    finish_path = Path(finished["finish_ref"]["uri"].removeprefix("file://"))
    assert hashlib.sha256(finish_path.read_bytes()).hexdigest() == finished["finish_ref"]["sha256"]
    assert claim_path.parent.stat().st_mode & 0o7777 == 0o1770
    assert claim_path.stat().st_mode & 0o777 == 0o440
    assert finish_path.stat().st_mode & 0o777 == 0o440


@pytest.mark.unit
def test_group_zero_scheduler_does_not_chmod_root_owned_shared_directory(
    tmp_path, monkeypatch
):
    evidence_dir = tmp_path / "campaigns.json.evidence"
    evidence_dir.mkdir()
    evidence_dir.chmod(0o1770)
    original_chmod = campaign_operator.os.chmod

    def guarded_chmod(path, mode):
        if Path(path) == evidence_dir:
            raise AssertionError("non-owner scheduler tried to chmod shared root")
        return original_chmod(path, mode)

    monkeypatch.setattr(campaign_operator.os, "geteuid", lambda: 50000)
    monkeypatch.setattr(campaign_operator.os, "chmod", guarded_chmod)
    persisted = campaign_operator._persist_immutable_at(
        evidence_dir / "scheduler-marker.json",
        {"kind": "scheduler-test"},
    )

    assert persisted["uri"].endswith("/scheduler-marker.json")
    assert (evidence_dir / "scheduler-marker.json").stat().st_mode & 0o777 == 0o440


@pytest.mark.unit
def test_claim_consumption_is_single_use_and_same_run_retry_is_idempotent(tmp_path):
    claimed = _claim(tmp_path / "campaigns.json")
    binding = {
        "claim_ref": claimed["claim_ref"],
        "dag_id": "dag_backfill_espn",
        "run_id": "manual__canary-a",
        "admission_identity": "d" * 64,
        "now": NOW,
    }

    first = campaign_operator.consume_campaign_claim(**binding)
    retry = campaign_operator.consume_campaign_claim(**binding)

    assert retry == first
    with pytest.raises(CampaignError, match="already consumed"):
        campaign_operator.consume_campaign_claim(
            **{**binding, "run_id": "manual__canary-b"}
        )
    campaign_operator.validate_campaign_consumption(
        claim_ref=claimed["claim_ref"],
        consumption_ref=first,
        dag_id=binding["dag_id"],
        run_id=binding["run_id"],
        admission_identity=binding["admission_identity"],
    )


@pytest.mark.unit
def test_existing_consumption_marker_uses_guarded_regular_file_reader(
    tmp_path, monkeypatch
):
    claimed = _claim(tmp_path / "campaigns.json")
    binding = {
        "claim_ref": claimed["claim_ref"],
        "dag_id": "dag_backfill_espn",
        "run_id": "manual__canary",
        "admission_identity": "d" * 64,
        "now": NOW,
    }
    consumption_ref = campaign_operator.consume_campaign_claim(**binding)
    marker_path = Path(consumption_ref["uri"].removeprefix("file://"))
    original_read_bytes = Path.read_bytes

    def reject_unguarded_read(path):
        if path == marker_path:
            raise AssertionError("unguarded consumption marker read")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_unguarded_read)

    assert campaign_operator.consume_campaign_claim(**binding) == consumption_ref


@pytest.mark.unit
def test_concurrent_claim_reuse_allows_exactly_one_run(tmp_path):
    claimed = _claim(tmp_path / "campaigns.json")
    barrier = threading.Barrier(2)

    def consume(run_id):
        barrier.wait()
        try:
            return campaign_operator.consume_campaign_claim(
                claim_ref=claimed["claim_ref"],
                dag_id="dag_backfill_espn",
                run_id=run_id,
                admission_identity=hashlib.sha256(run_id.encode()).hexdigest(),
                now=NOW,
            )
        except CampaignError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(consume, ("manual__a", "manual__b")))

    assert sum(isinstance(item, dict) for item in results) == 1
    errors = [item for item in results if isinstance(item, CampaignError)]
    assert len(errors) == 1
    assert "already consumed" in str(errors[0])


@pytest.mark.unit
def test_finished_claim_stays_revoked_after_active_ledger_restore(tmp_path):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    active_ledger = ledger_path.read_bytes()
    consumption_ref = campaign_operator.consume_campaign_claim(
        claim_ref=claimed["claim_ref"],
        dag_id="dag_backfill_espn",
        run_id="manual__canary",
        admission_identity="d" * 64,
        now=NOW,
    )
    finish_campaign_attempt(
        ledger_path=ledger_path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "e" * 64},
        successful=False,
        now=NOW,
    )
    ledger_path.write_bytes(active_ledger)

    with pytest.raises(CampaignError, match="finished|revoked"):
        campaign_operator.validate_campaign_consumption(
            claim_ref=claimed["claim_ref"],
            consumption_ref=consumption_ref,
            dag_id="dag_backfill_espn",
            run_id="manual__canary",
            admission_identity="d" * 64,
        )


@pytest.mark.unit
def test_finished_validation_uses_guarded_regular_file_reader(tmp_path, monkeypatch):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    consumption_ref = campaign_operator.consume_campaign_claim(
        claim_ref=claimed["claim_ref"],
        dag_id="dag_backfill_espn",
        run_id="manual__canary",
        admission_identity="d" * 64,
        now=NOW,
    )
    finished = finish_campaign_attempt(
        ledger_path=ledger_path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "e" * 64},
        successful=False,
        now=NOW,
    )
    finish_path = Path(finished["finish_ref"]["uri"].removeprefix("file://"))
    original_read_bytes = Path.read_bytes

    def reject_unguarded_read(path):
        if path == finish_path:
            raise AssertionError("unguarded finish marker read")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_unguarded_read)

    with pytest.raises(CampaignError, match="finished|revoked"):
        campaign_operator.validate_campaign_consumption(
            claim_ref=claimed["claim_ref"],
            consumption_ref=consumption_ref,
            dag_id="dag_backfill_espn",
            run_id="manual__canary",
            admission_identity="d" * 64,
        )


@pytest.mark.unit
def test_missing_immutable_finish_history_blocks_next_ordinal(tmp_path):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    finished = finish_campaign_attempt(
        ledger_path=ledger_path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "e" * 64},
        successful=False,
        now=NOW,
    )
    finish_path = Path(finished["finish_ref"]["uri"].removeprefix("file://"))
    finish_path.unlink()

    with pytest.raises(CampaignError, match="missing immutable finish"):
        _claim(ledger_path)


@pytest.mark.unit
def test_missing_immutable_claim_history_blocks_next_ordinal(tmp_path):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    claim_path = Path(claimed["claim_ref"]["uri"].removeprefix("file://"))
    finish_campaign_attempt(
        ledger_path=ledger_path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "e" * 64},
        successful=False,
        now=NOW,
    )
    claim_path.unlink()

    with pytest.raises(CampaignError, match="missing immutable claim"):
        _claim(ledger_path)


@pytest.mark.unit
def test_noncanonical_ledger_bytes_fail_closed_instead_of_being_laundered(tmp_path):
    ledger_path = tmp_path / "campaigns.json"
    _claim(ledger_path)
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(CampaignError, match="canonical"):
        campaign_operator.recover_campaign_state(ledger_path=ledger_path)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dag_id", "dag_ingest_espn"),
        ("run_id", "manual__other"),
        ("admission_identity", "f" * 64),
    ],
)
def test_consumption_validation_requires_exact_admission_binding(
    tmp_path, field, value
):
    claimed = _claim(tmp_path / "campaigns.json")
    binding = {
        "dag_id": "dag_backfill_espn",
        "run_id": "manual__canary",
        "admission_identity": "d" * 64,
    }
    consumption_ref = campaign_operator.consume_campaign_claim(
        claim_ref=claimed["claim_ref"],
        now=NOW,
        **binding,
    )

    with pytest.raises(CampaignError, match="binding"):
        campaign_operator.validate_campaign_consumption(
            claim_ref=claimed["claim_ref"],
            consumption_ref=consumption_ref,
            **{**binding, field: value},
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "boundary", ["evidence_link", "claim_evidence", "claim_ledger"]
)
def test_claim_is_recoverable_after_every_persistence_boundary(
    tmp_path, monkeypatch, boundary
):
    ledger_path = tmp_path / "campaigns.json"
    crashed = False

    def inject(observed):
        nonlocal crashed
        if observed == boundary and not crashed:
            crashed = True
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash, match=boundary):
        _claim(ledger_path)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    recovered = campaign_operator.recover_campaign_state(ledger_path=ledger_path)

    assert recovered["operation"] == "claim"
    assert recovered["attempt"]["ordinal"] == 1
    assert recovered["attempt"]["status"] == "active"
    assert recovered["claim_ref"]["uri"].startswith("file://")
    persisted = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert [item["ordinal"] for item in persisted["attempts"]] == [1]


@pytest.mark.unit
def test_guard_only_fails_closed_while_claim_recovery_is_pending(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "campaigns.json"

    def inject(boundary):
        if boundary == "claim_evidence":
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash, match="claim_evidence"):
        _claim(ledger_path)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    with pytest.raises(CampaignError, match="recover"):
        _claim(ledger_path, guard_only=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "boundary", ["evidence_link", "finish_evidence", "finish_ledger"]
)
def test_finish_is_recoverable_after_every_persistence_boundary(
    tmp_path, monkeypatch, boundary
):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    terminal_ref = {"uri": "s3://evidence/failure.json", "sha256": "d" * 64}
    crashed = False

    def inject(observed):
        nonlocal crashed
        if observed == boundary and not crashed:
            crashed = True
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash, match=boundary):
        finish_campaign_attempt(
            ledger_path=ledger_path,
            attempt_id=claimed["attempt"]["attempt_id"],
            terminal_ref=terminal_ref,
            successful=False,
            now=NOW,
        )

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    recovered = campaign_operator.recover_campaign_state(ledger_path=ledger_path)

    assert recovered["operation"] == "finish"
    assert recovered["status"] == "failed"
    assert recovered["terminal_ref"] == terminal_ref
    assert _claim(ledger_path)["attempt"]["ordinal"] == 2


@pytest.mark.unit
def test_pending_finish_with_missing_claim_evidence_fails_without_advancing_ledger(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    claim_path = Path(claimed["claim_ref"]["uri"].removeprefix("file://"))

    def inject(boundary):
        if boundary == "finish_evidence":
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash, match="finish_evidence"):
        finish_campaign_attempt(
            ledger_path=ledger_path,
            attempt_id=claimed["attempt"]["attempt_id"],
            terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "d" * 64},
            successful=False,
            now=NOW,
        )
    claim_path.unlink()

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    with pytest.raises(CampaignError, match="claim evidence is missing"):
        campaign_operator.recover_campaign_state(ledger_path=ledger_path)
    with pytest.raises(CampaignError, match="claim evidence is missing"):
        finish_campaign_attempt(
            ledger_path=ledger_path,
            attempt_id=claimed["attempt"]["attempt_id"],
            terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "d" * 64},
            successful=False,
            now=NOW,
        )

    persisted = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert [attempt["status"] for attempt in persisted["attempts"]] == ["active"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "boundary", ["evidence_link", "consumption_evidence"]
)
def test_consumption_retry_recovers_after_its_persistence_boundary(
    tmp_path, monkeypatch, boundary
):
    claimed = _claim(tmp_path / "campaigns.json")
    binding = {
        "claim_ref": claimed["claim_ref"],
        "dag_id": "dag_backfill_espn",
        "run_id": "manual__canary",
        "admission_identity": "d" * 64,
        "now": NOW,
    }

    def inject(observed):
        if observed == boundary:
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash, match=boundary):
        campaign_operator.consume_campaign_claim(**binding)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    recovered = campaign_operator.consume_campaign_claim(**binding)
    assert recovered["uri"].startswith("file://")


@pytest.mark.unit
def test_recover_resyncs_linked_marker_before_promoting_ledger(tmp_path, monkeypatch):
    ledger_path = tmp_path / "campaigns.json"
    evidence_dir = tmp_path / "campaigns.json.evidence"
    real_fsync = campaign_operator.os.fsync
    failed = False

    def fail_first_marker_directory_sync(descriptor):
        nonlocal failed
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        marker_linked = any(
            evidence_dir.glob("espn-canary-claim-evidence-v1-*.json")
        )
        if target == evidence_dir and marker_linked and not failed:
            failed = True
            raise OSError("injected evidence directory fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(
        campaign_operator.os, "fsync", fail_first_marker_directory_sync
    )
    with pytest.raises(OSError, match="evidence directory fsync"):
        _claim(ledger_path)

    assert not ledger_path.exists()
    assert any(evidence_dir.glob("espn-canary-claim-evidence-v1-*.json"))
    evidence_syncs_before_ledger = []

    def record_recovery_sync(descriptor):
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target == evidence_dir:
            evidence_syncs_before_ledger.append(not ledger_path.exists())
        return real_fsync(descriptor)

    monkeypatch.setattr(campaign_operator.os, "fsync", record_recovery_sync)
    recovered = campaign_operator.recover_campaign_state(ledger_path=ledger_path)

    assert recovered["operation"] == "claim"
    assert evidence_syncs_before_ledger == [True]


@pytest.mark.unit
def test_new_evidence_directory_is_synced_into_state_root_before_marker(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "campaigns.json"
    evidence_dir = tmp_path / "campaigns.json.evidence"
    real_fsync = campaign_operator.os.fsync
    state_root_syncs = []

    def record_sync(descriptor):
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target == tmp_path:
            markers = list(
                evidence_dir.glob("espn-canary-claim-evidence-v1-*.json")
            )
            state_root_syncs.append((evidence_dir.exists(), bool(markers)))
        return real_fsync(descriptor)

    monkeypatch.setattr(campaign_operator.os, "fsync", record_sync)
    _claim(ledger_path)

    assert (True, False) in state_root_syncs


@pytest.mark.unit
def test_recover_materializes_evidence_for_legacy_ledger_first_active_state(
    tmp_path,
):
    ledger_path = tmp_path / "campaigns.json"

    # Recreate the old ordering explicitly: derive a valid active ledger and
    # retain it without its claim artifact.
    identity = campaign_operator.CampaignIdentity.create(
        release_commit="a" * 40,
        release_tree_sha256="b" * 64,
        registry_signature="c" * 64,
        target_scope_ids=_scopes(),
    )
    ledger = campaign_operator.CampaignLedger.empty()
    ledger.claim(identity, now=NOW)
    campaign_operator._persist(ledger_path, ledger)

    recovered = campaign_operator.recover_campaign_state(ledger_path=ledger_path)

    assert recovered["operation"] == "claim"
    assert recovered["attempt"]["ordinal"] == 1
    claim_path = Path(recovered["claim_ref"]["uri"].removeprefix("file://"))
    assert claim_path.is_file()


@pytest.mark.unit
def test_claim_failure_before_immutable_publish_leaves_ordinal_unconsumed(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "campaigns.json"
    original_link = campaign_operator.os.link

    def fail_publish(*_args, **_kwargs):
        raise OSError("injected atomic publish failure")

    monkeypatch.setattr(campaign_operator.os, "link", fail_publish)
    with pytest.raises(OSError, match="atomic publish"):
        _claim(ledger_path)

    assert not ledger_path.exists()
    monkeypatch.setattr(campaign_operator.os, "link", original_link)
    claimed = _claim(ledger_path)
    assert claimed["attempt"]["ordinal"] == 1


@pytest.mark.unit
def test_recover_rejects_ambiguous_forked_claim_evidence(tmp_path, monkeypatch):
    ledger_path = tmp_path / "campaigns.json"

    def inject(boundary):
        if boundary == "claim_evidence":
            raise InjectedCrash(boundary)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", inject)
    with pytest.raises(InjectedCrash):
        _claim(ledger_path)
    evidence_dir = ledger_path.with_name(ledger_path.name + ".evidence")
    marker = next(evidence_dir.glob("espn-canary-claim-evidence-v1-*.json"))
    fork = evidence_dir / f"espn-canary-claim-evidence-v1-{'f' * 64}.json"
    fork.write_bytes(marker.read_bytes())
    fork.chmod(0o440)

    monkeypatch.setattr(campaign_operator, "_persistence_boundary", lambda _name: None)
    with pytest.raises(CampaignError, match="ambiguous"):
        campaign_operator.recover_campaign_state(ledger_path=ledger_path)


@pytest.mark.unit
def test_next_claim_rejects_copied_finish_evidence_fork(tmp_path):
    ledger_path = tmp_path / "campaigns.json"
    claimed = _claim(ledger_path)
    finished = finish_campaign_attempt(
        ledger_path=ledger_path,
        attempt_id=claimed["attempt"]["attempt_id"],
        terminal_ref={"uri": "s3://evidence/failure.json", "sha256": "d" * 64},
        successful=False,
        now=NOW,
    )
    finish_path = Path(finished["finish_ref"]["uri"].removeprefix("file://"))
    fork = finish_path.with_name("espn-canary-finish-evidence-v1-fork.json")
    fork.write_bytes(finish_path.read_bytes())
    fork.chmod(0o440)

    with pytest.raises(CampaignError, match="finish evidence.*ambiguous"):
        _claim(ledger_path)


@pytest.mark.unit
def test_isolated_compose_uses_one_same_path_durable_canary_namespace():
    compose = yaml.safe_load(
        (ROOT / "deploy/espn/airflow.compose.yaml").read_text(encoding="utf-8")
    )
    common = compose["x-espn-airflow-common"]
    expected = str(campaign_operator.DEFAULT_CANARY_STATE_ROOT)

    assert common["environment"]["ESPN_CANARY_STATE_ROOT"] == expected
    matches = [
        item
        for item in common["volumes"]
        if isinstance(item, dict) and item.get("target") == expected
    ]
    assert matches == [
        {
            "type": "bind",
            "source": expected,
            "target": expected,
            "read_only": False,
            "bind": {"create_host_path": False},
        }
    ]


@pytest.mark.unit
def test_runtime_rejects_claim_file_uri_outside_configured_namespace(
    tmp_path, monkeypatch
):
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    claimed = _claim(outside / "campaigns.json")
    configured = tmp_path / "configured"
    configured.mkdir()
    configured.chmod(0o770)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(configured))

    with pytest.raises(CampaignError, match="ESPN_CANARY_STATE_ROOT"):
        campaign_operator.consume_campaign_claim(
            claim_ref=claimed["claim_ref"],
            dag_id="dag_backfill_espn",
            run_id="manual__canary",
            admission_identity="d" * 64,
        )


@pytest.mark.unit
def test_runtime_rejects_unprotected_configured_state_root(tmp_path, monkeypatch):
    state_root = tmp_path / "configured"
    state_root.mkdir(mode=0o700)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))

    with pytest.raises(CampaignError, match="root:0.*0770"):
        _claim(state_root / "campaigns.json")


@pytest.mark.unit
def test_runtime_rejects_non_root_owner_even_with_exact_state_root_mode(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "configured"
    state_root.mkdir()
    state_root.chmod(0o770)
    assert stat.S_IMODE(state_root.stat().st_mode) == 0o770
    real_lstat = Path.lstat

    def synthetic_non_root_lstat(path):
        details = real_lstat(path)
        if path != state_root:
            return details
        values = list(details)
        values[4] = 1001
        values[5] = 1001
        return os.stat_result(values)

    monkeypatch.setattr(campaign_operator, "_required_shared_owner", lambda: (0, 0))
    monkeypatch.setattr(Path, "lstat", synthetic_non_root_lstat)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))

    with pytest.raises(CampaignError, match="root:0.*0770"):
        _claim(state_root / "campaigns.json")


@pytest.mark.unit
def test_runtime_rejects_symlinked_claim_evidence_inside_state_root(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "configured"
    state_root.mkdir()
    state_root.chmod(0o770)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))
    claimed = _claim(state_root / "campaigns.json")
    claim_path = Path(claimed["claim_ref"]["uri"].removeprefix("file://"))
    alias = claim_path.with_name("claim-alias.json")
    alias.symlink_to(claim_path)

    with pytest.raises(CampaignError, match="symlink"):
        campaign_operator.consume_campaign_claim(
            claim_ref={**claimed["claim_ref"], "uri": alias.as_uri()},
            dag_id="dag_backfill_espn",
            run_id="manual__canary",
            admission_identity="d" * 64,
        )


@pytest.mark.unit
def test_runtime_rejects_noncanonical_copy_of_exact_claim_bytes(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "configured"
    state_root.mkdir()
    state_root.chmod(0o770)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))
    claimed = _claim(state_root / "campaigns.json")
    claim_path = Path(claimed["claim_ref"]["uri"].removeprefix("file://"))
    copied = state_root / "copied-claim.json"
    copied.write_bytes(claim_path.read_bytes())
    copied.chmod(0o440)

    with pytest.raises(CampaignError, match="canonical claim marker"):
        campaign_operator.consume_campaign_claim(
            claim_ref={**claimed["claim_ref"], "uri": copied.as_uri()},
            dag_id="dag_backfill_espn",
            run_id="manual__canary",
            admission_identity="d" * 64,
        )


@pytest.mark.unit
def test_runtime_rejects_symlinked_ledger_parent_inside_state_root(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "configured"
    state_root.mkdir()
    state_root.chmod(0o770)
    real_parent = state_root / "real"
    real_parent.mkdir()
    alias_parent = state_root / "alias"
    alias_parent.symlink_to(real_parent, target_is_directory=True)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))

    with pytest.raises(CampaignError, match="symlink"):
        _claim(alias_parent / "campaigns.json")


@pytest.mark.unit
def test_cli_rejects_symlinked_state_root(tmp_path, monkeypatch):
    real = tmp_path / "real"
    real.mkdir()
    real.chmod(0o770)
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(linked))

    with pytest.raises(CampaignError, match="symlink"):
        campaign_operator._cli_ledger_path(str(linked / "campaigns.json"))


@pytest.mark.unit
@pytest.mark.parametrize(
    "relative_ledger", ["alternate.json", "unexpected/nested/campaigns.json"]
)
def test_cli_guard_only_rejects_noncanonical_ledger_without_creating_state(
    tmp_path, monkeypatch, capsys, relative_ledger
):
    state_root = tmp_path / "shared-canary-state"
    state_root.mkdir()
    state_root.chmod(0o770)
    targets = tmp_path / "targets.json"
    targets.write_text(json.dumps(list(_scopes())), encoding="utf-8")
    noncanonical = state_root / relative_ledger
    monkeypatch.setenv("ESPN_CANARY_STATE_ROOT", str(state_root))

    result = campaign_operator.main(
        [
            "claim",
            "--ledger-path",
            str(noncanonical),
            "--release-commit",
            "a" * 40,
            "--release-tree-sha256",
            "b" * 64,
            "--registry-signature",
            "c" * 64,
            "--target-scopes",
            str(targets),
            "--guard-only",
        ]
    )

    assert result == 2
    assert "canonical canary ledger" in capsys.readouterr().err
    assert list(state_root.iterdir()) == []


@pytest.mark.unit
def test_real_module_entrypoint_fails_closed_without_protected_state(tmp_path):
    state_root = tmp_path / "missing-canary-state"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.espn_canary_campaign",
            "recover",
            "--ledger-path",
            str(state_root / "campaigns.json"),
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "ESPN_CANARY_STATE_ROOT": str(state_root),
        },
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert completed.stderr == (
        "ESPN canary campaign refused: "
        "canary state root is missing or unreadable\n"
    )
    assert not state_root.exists()


@pytest.mark.unit
def test_cli_rejects_fifo_ledger_without_blocking(tmp_path):
    state_root = tmp_path / "shared-canary-state"
    state_root.mkdir()
    state_root.chmod(0o770)
    ledger_path = state_root / "campaigns.json"
    os.mkfifo(ledger_path, mode=0o660)
    environment = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "ESPN_CANARY_STATE_ROOT": str(state_root),
    }

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _TEST_OWNER_PRELUDE
            + "\nraise SystemExit(campaign_operator.main(sys.argv[1:]))",
            "recover",
            "--ledger-path",
            str(ledger_path),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 2
    assert "regular file" in completed.stderr


@pytest.mark.unit
def test_cli_process_recreate_reads_consumes_finishes_and_recovers(tmp_path):
    state_root = tmp_path / "shared-canary-state"
    state_root.mkdir()
    state_root.chmod(0o770)
    ledger_path = state_root / "campaigns.json"
    targets = tmp_path / "targets.json"
    targets.write_text(json.dumps({"target_scope_ids": list(_scopes())}))
    environment = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "ESPN_CANARY_STATE_ROOT": str(state_root),
    }
    claim_command = [
        sys.executable,
        "-c",
        _TEST_OWNER_PRELUDE
        + "\nraise SystemExit(campaign_operator.main(sys.argv[1:]))",
        "claim",
        "--ledger-path",
        str(ledger_path),
        "--release-commit",
        "a" * 40,
        "--release-tree-sha256",
        "b" * 64,
        "--registry-signature",
        "c" * 64,
        "--target-scopes",
        str(targets),
    ]
    claimed_process = subprocess.run(
        claim_command,
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    claimed = json.loads(claimed_process.stdout)
    assert Path(claimed["claim_ref"]["uri"].removeprefix("file://")).is_relative_to(
        state_root
    )

    # A fresh interpreter stands in for a recreated LocalExecutor scheduler;
    # it must consume the exact host-produced file URI without translation.
    consume_program = (
        _TEST_OWNER_PRELUDE
        + """
import json
import sys
from scripts.espn_canary_campaign import consume_campaign_claim

claim_ref = json.loads(sys.argv[1])
result = consume_campaign_claim(
    claim_ref=claim_ref,
    dag_id="dag_backfill_espn",
    run_id="manual__canary",
    admission_identity="d" * 64,
)
print(json.dumps(result, sort_keys=True))
"""
    )
    consumed_process = subprocess.run(
        [sys.executable, "-c", consume_program, json.dumps(claimed["claim_ref"])],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    consumed = json.loads(consumed_process.stdout)
    assert Path(consumed["uri"].removeprefix("file://")).is_relative_to(state_root)

    finish_command = [
        sys.executable,
        "-c",
        _TEST_OWNER_PRELUDE
        + "\nraise SystemExit(campaign_operator.main(sys.argv[1:]))",
        "finish",
        "--ledger-path",
        str(ledger_path),
        "--attempt-id",
        claimed["attempt"]["attempt_id"],
        "--terminal-uri",
        "s3://evidence/success.json",
        "--terminal-sha256",
        "e" * 64,
        "--successful",
    ]
    finished_process = subprocess.run(
        finish_command,
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(finished_process.stdout)["status"] == "successful"

    recovered_process = subprocess.run(
        [
            sys.executable,
            "-c",
            _TEST_OWNER_PRELUDE
            + "\nraise SystemExit(campaign_operator.main(sys.argv[1:]))",
            "recover",
            "--ledger-path",
            str(ledger_path),
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    recovered = json.loads(recovered_process.stdout)
    assert recovered["operation"] == "finish"
    assert recovered["status"] == "successful"
