"""Contract of configs/espn/transport_policy.json (#1500)."""

from __future__ import annotations

import copy
import json

import pytest

from scrapers.espn.gate import (
    DEFAULT_POLICY_PATH,
    load_transport_policy,
    parse_transport_policy,
)


def _raw():
    return json.loads(DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))


@pytest.mark.unit
def test_policy_file_parses_with_s0_ladder_and_half_for_live():
    policy = load_transport_policy()
    assert policy.steps == (60, 120, 240, 360)
    assert policy.live_share == 0.5
    assert set(policy.lanes) == {"live", "history"}
    assert all(set(spec) == {"daily_requests"} for spec in policy.lanes.values())
    assert policy.reset["cooldown_seconds"] == 900
    assert policy.uncompressed_warn_bytes == 102400


def _mutated(path, value):
    raw = copy.deepcopy(_raw())
    node = raw
    for key in path[:-1]:
        node = node[key]
    if value is KeyError:
        del node[path[-1]]
    else:
        node[path[-1]] = value
    return raw


@pytest.mark.unit
@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("unexpected",), 1),
        (("steps",), [60, 60, 240]),
        (("steps",), [120, 60]),
        (("steps",), []),
        (("live_share",), 0),
        (("live_share",), 1),
        (("live_share",), 1.5),
        (("lanes",), {}),
        (("lanes", "history"), KeyError),
        (("lanes", "live", "daily_requests"), 0),
        (("lanes", "live", "daily_bytes"), 10**9),
        (("reset", "error_share"), 2),
        (("reset", "cooldown_seconds"), KeyError),
        (("clusters", "site", "primary"), "http://site.web.api.espn.com"),
        (("clusters", "core", "reserve"), "https://site.api.espn.com"),
        (("origin_block_seconds",), -1),
    ],
)
def test_broken_policy_fails_closed(path, value):
    with pytest.raises(ValueError):
        parse_transport_policy(_mutated(path, value))
