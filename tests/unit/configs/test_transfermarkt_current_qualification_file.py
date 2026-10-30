"""Integrity of the delivered #1393 qualification (owner parity decision 2026-10-30)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scrapers.transfermarkt.signal_qualification import (
    OWNER_PARITY_DECISION,
    load_qualification_wrapper,
    validate_qualification,
)

CONFIG = Path(__file__).resolve().parents[3] / "dags/configs/transfermarkt"
WRAPPER = CONFIG / "current_signal_qualification.json"
REPORTS = CONFIG / "qualification/2026-10-28-29"


def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_delivered_wrapper_passes_the_runtime_validator():
    result = validate_qualification(load_qualification_wrapper(WRAPPER))
    assert result["players_compared"] == 1000
    assert result["same_day_changes"] is False and result["changed_observations"] == []
    assert result["owner_acceptance"]["decision"] == OWNER_PARITY_DECISION


def test_owner_decision_is_bound_to_the_delivered_reports():
    wrapper = load_qualification_wrapper(WRAPPER)
    evidence = Path(wrapper["evidence_path"])
    assert wrapper["evidence_sha256"] == _sha256(evidence)
    artifact = json.loads(evidence.read_text())
    assert artifact["owner_acceptance"]["reports_sha256"] == [
        _sha256(REPORTS / "day1.json"), _sha256(REPORTS / "day2.json")]
