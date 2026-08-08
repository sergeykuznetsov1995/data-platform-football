from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scripts.espn_canary_campaign import (
    claim_campaign_attempt,
    finish_campaign_attempt,
)
from scrapers.espn.canary_campaign import CampaignError


UTC = timezone.utc
NOW = datetime(2026, 8, 8, 12, tzinfo=UTC)


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

    claimed = _claim(path)
    assert claimed["attempt"]["ordinal"] == 1
    assert path.stat().st_mode & 0o777 == 0o600


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
def test_claim_and_finish_evidence_are_content_addressed_and_immutable(tmp_path):
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
