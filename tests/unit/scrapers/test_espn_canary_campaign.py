from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from scrapers.espn.canary_campaign import (
    CampaignError,
    CampaignIdentity,
    CampaignLedger,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 8, 12, tzinfo=UTC)


def _identity(release: str = "a" * 40) -> CampaignIdentity:
    return CampaignIdentity.create(
        release_commit=release,
        release_tree_sha256="b" * 64,
        registry_signature="c" * 64,
        target_scope_ids=("1:2026", "2:2026"),
    )


@pytest.mark.unit
def test_new_release_campaign_starts_at_001_and_old_attempts_are_isolated():
    ledger = CampaignLedger.empty()
    old = ledger.claim(_identity("a" * 40), now=NOW)
    ledger = ledger.fail(
        old,
        failure_ref={"uri": "s3://evidence/old-failure.json", "sha256": "d" * 64},
        now=NOW,
    )

    new = ledger.claim(
        _identity("e" * 40),
        predecessor_failure_ref={
            "uri": "s3://evidence/old-failure.json",
            "sha256": "d" * 64,
        },
        remediation="release e repairs the failed parser qualification",
        now=NOW,
    )

    assert old.ordinal == 1
    assert new.ordinal == 1
    assert new.campaign_id != old.campaign_id
    assert new.predecessor_campaign_id == old.campaign_id


@pytest.mark.unit
def test_guard_only_does_not_consume_attempt_and_same_campaign_is_capped_at_003():
    ledger = CampaignLedger.empty()
    identity = _identity()
    guard = ledger.claim(identity, now=NOW, guard_only=True)
    assert guard.ordinal is None
    assert ledger.attempts == ()

    for ordinal in range(1, 4):
        attempt = ledger.claim(identity, now=NOW)
        assert attempt.ordinal == ordinal
        ledger = ledger.fail(
            attempt,
            failure_ref={
                "uri": f"s3://evidence/failure-{ordinal}.json",
                "sha256": f"{ordinal}" * 64,
            },
            now=NOW,
        )
    with pytest.raises(CampaignError, match="three attempts"):
        ledger.claim(identity, now=NOW)


@pytest.mark.unit
def test_campaign_fails_closed_on_active_success_malformed_or_registry_drift():
    identity = _identity()
    ledger = CampaignLedger.empty()
    active = ledger.claim(identity, now=NOW)
    with pytest.raises(CampaignError, match="active"):
        ledger.claim(identity, now=NOW)

    successful = ledger.succeed(
        active,
        success_receipt_ref={
            "uri": "s3://evidence/success.json",
            "sha256": "f" * 64,
        },
        now=NOW,
    )
    with pytest.raises(CampaignError, match="successful"):
        successful.claim(identity, now=NOW)
    with pytest.raises(CampaignError, match="registry drift"):
        successful.claim(
            CampaignIdentity.create(
                release_commit=identity.release_commit,
                release_tree_sha256=identity.release_tree_sha256,
                registry_signature="0" * 64,
                target_scope_ids=identity.target_scope_ids,
            ),
            now=NOW,
        )
    with pytest.raises(CampaignError, match="malformed"):
        CampaignLedger.from_dict({"kind": "espn-canary-campaign-ledger-v1"})


@pytest.mark.unit
def test_campaign_claim_rejects_directly_forged_identity():
    identity = _identity()

    with pytest.raises(CampaignError, match="malformed or drifted"):
        CampaignLedger.empty().claim(
            replace(identity, campaign_id="0" * 64),
            now=NOW,
        )
