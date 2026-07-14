"""The committed Transfermarkt registry standing policy stays contour-exact."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

from dags.scripts.run_transfermarkt_discovery import (
    HARD_PROVIDER_BYTE_BUDGET,
    STANDING_POLICY_DAG_ID,
    TARGET_TABLES,
)
from dags.utils.transfermarkt_approval import (
    StandingPolicy,
    load_standing_policy,
)
from dags.utils.transfermarkt_registry_publish import (
    COMPETITIONS_TABLE,
    EDITIONS_TABLE,
    REGISTRY_STATE_TABLE,
)


ROOT = Path(__file__).resolve().parents[3]
POLICY_PATH = (
    ROOT / "dags" / "configs" / "transfermarkt" / "standing_registry_policy.json"
)
PREPARE_SCRIPT = ROOT / "scripts" / "prepare_transfermarkt_registry_approval.py"


def _prepare_module():
    """Load the manual ritual's CLI: its pinned caps are the caps of record."""

    spec = importlib.util.spec_from_file_location(
        "tm_registry_prepare_for_policy_test", PREPARE_SCRIPT,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_committed_policy_parses_into_a_standing_policy():
    policy = load_standing_policy(POLICY_PATH)

    assert isinstance(policy, StandingPolicy)
    assert policy.dag_id == STANDING_POLICY_DAG_ID
    assert policy.dag_id == "dag_discover_transfermarkt_registry"
    assert len(policy.policy_hash) == 64


def test_committed_policy_caps_equal_the_manual_ritual_constants():
    prepare = _prepare_module()
    policy = load_standing_policy(POLICY_PATH)

    assert policy.paid_proxy.byte_cap_bytes == prepare.PROVIDER_HARD_CAP_BYTES
    assert policy.paid_proxy.byte_cap_bytes == HARD_PROVIDER_BYTE_BUDGET
    assert policy.paid_proxy.request_limit == prepare.PROXY_REQUEST_LIMIT
    assert policy.paid_proxy.retry_limit == prepare.PROXY_RETRY_LIMIT
    assert policy.paid_proxy.concurrency == prepare.PROXY_CONCURRENCY
    assert policy.production_write.byte_cap_bytes == 0
    assert policy.production_write.request_limit == 0
    assert policy.production_write.retry_limit == 0
    assert policy.production_write.concurrency == 1


def test_committed_policy_covers_bronze_and_publication_tables():
    prepare = _prepare_module()
    policy = load_standing_policy(POLICY_PATH)
    allowed = set(policy.allowed_write_tables)

    assert set(TARGET_TABLES) <= allowed
    assert set(prepare.BRONZE_TABLES) <= allowed
    assert {COMPETITIONS_TABLE, EDITIONS_TABLE, REGISTRY_STATE_TABLE} <= allowed


def test_committed_policy_validity_window_is_well_formed():
    # The format is pinned; whether expires_at is still in the future is an
    # operational question, not a CI one — a fixed date must not time-bomb CI.
    raw = json.loads(POLICY_PATH.read_text("utf-8"))
    for field in ("approved_at", "expires_at"):
        value = datetime.fromisoformat(str(raw[field]).replace("Z", "+00:00"))
        assert value.tzinfo is not None
        assert value.utcoffset() == timezone.utc.utcoffset(None)

    policy = load_standing_policy(POLICY_PATH)
    assert policy.expires_at > policy.approved_at
