"""The committed Transfermarkt standing approval policy stays child-exact."""

from __future__ import annotations

import json

import pytest
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
from dags.utils.transfermarkt_staff import validate_staff_write_policy
from scrapers.transfermarkt.staff import STAFF_TABLE, STAFF_RECEIPTS_TABLE


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
    # Policy v8 preserves existing caps and includes referee and rumour targets.
    assert policy.policy_version == 8
    assert 'iceberg.ops.transfermarkt_season_close_v1' in policy.allowed_write_tables
    assert 'iceberg.ops.transfermarkt_referee_receipts_v1' in policy.allowed_write_tables
    assert 'iceberg.bronze.transfermarkt_referee_profiles' in policy.allowed_write_tables
    assert 'iceberg.bronze.transfermarkt_referee_season_aggregates' in policy.allowed_write_tables


def test_committed_policy_caps_equal_the_child_wrapper_constants():
    policy = load_standing_policy(POLICY_PATH)

    # The policy pins the per-SCOPE caps — the numbers enforced on the child;
    # the parent (daily) caps are pinned by the wrapper's argv equality.
    assert policy.paid_proxy.byte_cap_bytes == HARD_BYTE_CAP
    assert policy.paid_proxy.byte_cap_bytes == 25_165_824
    assert policy.paid_proxy.request_limit == sum(
        item['requests'] for item in DEFAULT_ENTITY_LIMITS.values()
    )
    assert policy.paid_proxy.request_limit == 1_900
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
    assert {
        'iceberg.bronze.transfermarkt_rumour_observations',
        'iceberg.ops.transfermarkt_rumour_receipts_v1',
    } <= allowed


def test_committed_policy_authorizes_staff_writes():
    policy = load_standing_policy(POLICY_PATH)

    assert STAFF_TABLE in policy.allowed_write_tables
    assert STAFF_RECEIPTS_TABLE in policy.allowed_write_tables
    validate_staff_write_policy(policy)


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
    assert policy.policy_version == 4
    assert 'iceberg.ops.transfermarkt_season_close_v1' in policy.allowed_write_tables
    assert policy.paid_proxy.byte_cap_bytes == HARD_BYTE_CAP
    assert policy.paid_proxy.request_limit == 1_900
    assert policy.paid_proxy.retry_limit == 800
    assert required_write_tables('native-only') <= set(
        policy.allowed_write_tables
    )
    assert not (
        required_write_tables('dual') - required_write_tables('native-only')
    ) & set(policy.allowed_write_tables)


@pytest.mark.parametrize('path,version,approved', [
    (POLICY_PATH, 8, datetime(2026, 11, 20, 17, 18, 44, tzinfo=timezone.utc)),
    (BACKFILL_POLICY_PATH, 4, datetime(2026, 10, 28, tzinfo=timezone.utc)),
])
def test_committed_policy_renewal_has_twelve_calendar_months(path, version, approved):
    policy = load_standing_policy(path)
    assert policy.policy_version == version
    assert policy.approved_at == approved
    assert policy.expires_at >= policy.approved_at.replace(year=policy.approved_at.year + 1)
