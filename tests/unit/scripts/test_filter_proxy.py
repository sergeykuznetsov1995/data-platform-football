"""Unit tests for scripts/proxy_filter/filter_proxy.py (#652).

``filter_proxy`` is a standalone script (not a package). Its only container-only
import (``scrapers.utils.proxy_manager``) is lazy — inside ``_residential`` — so the
module loads on the host with no stubbing and no network.

What we cover (the safety-critical, pure logic):
  - ``_is_blocked``: dot-boundary suffix matching, case-insensitivity, and the
    invariant that the Cloudflare challenge + target sites are NEVER blocked.
  - ``_load_blocklist``: comment/blank stripping, lowercasing, None -> empty.
  - ``_dump``: report shape (total_mb / allowed_hosts / blocked_hosts).
  - the SHIPPED ``configs/proxy_filter/blocklist.txt`` does not footgun CF/the sites.
"""
from __future__ import annotations

import importlib.util
import json
from collections import defaultdict
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "proxy_filter" / "filter_proxy.py"
_BLOCKLIST_PATH = REPO_ROOT / "configs" / "proxy_filter" / "blocklist.txt"


def _load_module():
    spec = importlib.util.spec_from_file_location("filter_proxy", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f"cannot load {_SCRIPT_PATH}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load_module()


# --- _is_blocked --------------------------------------------------------------

def test_blocks_exact_domain(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("doubleclick.net") is True


def test_blocks_subdomain_via_dot_suffix(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert — a real ad subdomain must be caught by the suffix rule
    assert mod._is_blocked("securepubads.g.doubleclick.net") is True


def test_does_not_block_lookalike_without_dot_boundary(mod):
    # Arrange — "notdoubleclick.net" merely ends with the string, not ".doubleclick.net"
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("notdoubleclick.net") is False


def test_matching_is_case_insensitive(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("SecurePubAds.G.DoubleClick.NET") is True


@pytest.mark.parametrize(
    "host",
    [
        "challenges.cloudflare.com",  # CF Turnstile — blocking it breaks the bypass
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
    ],
)
def test_never_blocks_cf_or_target_sites(mod, host):
    # Arrange — even with a broad ad blocklist, these must always pass
    mod.BLOCKLIST = {"doubleclick.net", "googletagmanager.com", "adnxs.com"}
    # Act / Assert
    assert mod._is_blocked(host) is False


def test_empty_blocklist_blocks_nothing(mod):
    # Arrange — observe mode: no blocklist
    mod.BLOCKLIST = set()
    # Act / Assert
    assert mod._is_blocked("securepubads.g.doubleclick.net") is False


# --- _load_blocklist ----------------------------------------------------------

def test_load_blocklist_none_returns_empty(mod):
    assert mod._load_blocklist(None) == set()


def test_load_blocklist_strips_comments_blanks_and_lowercases(mod, tmp_path):
    # Arrange
    f = tmp_path / "bl.txt"
    f.write_text("# header comment\n\nDoubleClick.net\n  adnxs.com  \n# trailing\n")
    # Act
    result = mod._load_blocklist(str(f))
    # Assert
    assert result == {"doubleclick.net", "adnxs.com"}


# --- _dump --------------------------------------------------------------------

def test_dump_writes_expected_report_shape(mod, tmp_path):
    # Arrange — populate the module-level byte counters
    mod.up_bytes = defaultdict(int, {"sofifa.com": 1000})
    mod.down_bytes = defaultdict(int, {"sofifa.com": 1_048_576})  # 1 MiB down
    mod.conn_count = defaultdict(int, {"sofifa.com": 2})
    mod.blocked_count = defaultdict(int, {"doubleclick.net": 5})
    out = tmp_path / "report.json"
    # Act
    mod._dump(str(out), quiet=True)
    # Assert
    report = json.loads(out.read_text())
    assert set(report) == {"total_mb", "allowed_hosts", "blocked_hosts"}
    assert report["allowed_hosts"][0]["host"] == "sofifa.com"
    assert report["allowed_hosts"][0]["down_mb"] == pytest.approx(1.0, abs=0.01)
    assert report["blocked_hosts"] == [{"host": "doubleclick.net", "attempts": 5}]


# --- shipped blocklist safety -------------------------------------------------

def test_shipped_blocklist_blocks_adtech_but_not_cf_or_sites(mod):
    # Arrange — load the real config the filter ships with
    mod.BLOCKLIST = mod._load_blocklist(str(_BLOCKLIST_PATH))
    assert mod.BLOCKLIST, "shipped blocklist must not be empty"
    # Assert — must keep CF + the scraped sites alive
    for keep in (
        "challenges.cloudflare.com",
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
        "fonts.gstatic.com",
    ):
        assert mod._is_blocked(keep) is False, f"{keep} must NOT be blocked"
    # Assert — must drop the heavy ad-tech seen in the observe run
    for drop in (
        "securepubads.g.doubleclick.net",
        "www.googletagmanager.com",
        "ib.adnxs.com",
        "connect.facebook.net",
        "cdn.intergient.com",
    ):
        assert mod._is_blocked(drop) is True, f"{drop} must be blocked"
