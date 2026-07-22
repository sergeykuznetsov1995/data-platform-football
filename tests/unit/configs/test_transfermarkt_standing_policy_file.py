"""The committed Transfermarkt standing approval policy stays child-exact."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from dags.scripts.run_transfermarkt_scope_cycle import (
    DEFAULT_ENTITY_LIMITS,
    HARD_BYTE_CAP,
    SCOPE_RETRY_LIMIT,
    STANDING_POLICY_DAG_ID,
    required_write_tables,
)
from dags.utils.transfermarkt_approval import StandingPolicy, load_standing_policy


POLICY_PATH = (
    Path(__file__).resolve().parents[3]
    / 'dags' / 'configs' / 'transfermarkt' / 'standing_approval_policy.json'
)
BACKFILL_POLICY_PATH = (
    Path(__file__).resolve().parents[3]
    / 'dags' / 'configs' / 'transfermarkt' / 'standing_backfill_policy.json'
)


def test_committed_policy_parses_into_a_standing_policy():
    policy = load_standing_policy(POLICY_PATH)

    assert isinstance(policy, StandingPolicy)
    assert policy.dag_id == STANDING_POLICY_DAG_ID
    assert policy.dag_id == 'dag_ingest_transfermarkt'
    assert len(policy.policy_hash) == 64
    # v2 = the daily-throughput caps (24 MiB / 1610 / 800 per scope); the
    # standing-authorization records key on standing-policy-v{version}.
    assert policy.policy_version == 2


def test_committed_policy_caps_equal_the_child_wrapper_constants():
    policy = load_standing_policy(POLICY_PATH)

    # The policy pins the per-SCOPE caps — the numbers enforced on the child;
    # the parent (daily) caps are pinned by the wrapper's argv equality.
    assert policy.paid_proxy.byte_cap_bytes == HARD_BYTE_CAP
    assert policy.paid_proxy.byte_cap_bytes == 25_165_824
    assert policy.paid_proxy.request_limit == sum(
        item['requests'] for item in DEFAULT_ENTITY_LIMITS.values()
    )
    assert policy.paid_proxy.request_limit == 1_610
    assert policy.paid_proxy.retry_limit == SCOPE_RETRY_LIMIT
    assert policy.paid_proxy.retry_limit == 800
    assert policy.paid_proxy.concurrency == 1
    assert policy.production_write.byte_cap_bytes == 0
    assert policy.production_write.request_limit == 0
    assert policy.production_write.retry_limit == 0
    assert policy.production_write.concurrency == 1


def test_committed_policy_covers_both_write_modes():
    policy = load_standing_policy(POLICY_PATH)
    allowed = set(policy.allowed_write_tables)

    for write_mode in ('dual', 'native-only'):
        assert required_write_tables(write_mode) <= allowed


def test_committed_policy_validity_window_is_well_formed():
    # The format is pinned; whether expires_at is still in the future is an
    # operational question, not a CI one — a fixed date must not time-bomb CI.
    raw = json.loads(POLICY_PATH.read_text('utf-8'))
    for field in ('approved_at', 'expires_at'):
        value = datetime.fromisoformat(str(raw[field]).replace('Z', '+00:00'))
        assert value.tzinfo is not None
        assert value.utcoffset() == timezone.utc.utcoffset(None)

    policy = load_standing_policy(POLICY_PATH)
    assert policy.expires_at > policy.approved_at


def test_backfill_policy_is_native_only_and_matches_the_same_scope_caps():
    policy = load_standing_policy(BACKFILL_POLICY_PATH)

    assert policy.dag_id == 'dag_backfill_transfermarkt'
    assert policy.paid_proxy.byte_cap_bytes == HARD_BYTE_CAP
    assert policy.paid_proxy.request_limit == 1_610
    assert policy.paid_proxy.retry_limit == 800
    assert required_write_tables('native-only') <= set(
        policy.allowed_write_tables
    )
    assert not (
        required_write_tables('dual') - required_write_tables('native-only')
    ) & set(policy.allowed_write_tables)
