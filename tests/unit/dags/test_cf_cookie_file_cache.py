"""
Unit tests for the inter-process CF cookie file cache (issue #118).

The FBref prewarm task writes ``/tmp/fbref_cf_cookies.json`` via
``_write_cf_cookies_file``; each scraper subprocess reads it via
``NodriverBypass._load_cf_cookies_file`` and injects the cookies to skip the
Cloudflare challenge. These tests exercise that write -> load contract and the
freshness / robustness guarantees — no browser or network required.
"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from dags.utils.fbref_callbacks import _write_cf_cookies_file, prewarm_cf_cookies
from scrapers.base.browser.nodriver_bypass import NodriverBypass

_load = NodriverBypass._load_cf_cookies_file


# ---------------------------------------------------------------------------
# Writer (prewarm side)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_write_produces_inject_ready_shape(tmp_path):
    """Writer converts {name: value} into the list-of-dicts inject_cookies expects."""
    path = str(tmp_path / "cf.json")

    ok = _write_cf_cookies_file(
        path, {"cf_clearance": "AAA", "__cf_bm": "BBB"}, proxy_idx=3
    )

    assert ok is True
    raw = json.loads((tmp_path / "cf.json").read_text())
    assert [c["name"] for c in raw["cookies"]] == ["cf_clearance", "__cf_bm"]
    assert raw["cookies"][0] == {
        "name": "cf_clearance",
        "value": "AAA",
        "domain": ".fbref.com",
        "path": "/",
        "secure": True,
        "httpOnly": True,
    }
    assert raw["proxy_idx"] == 3
    # extracted_at must be ISO 8601 parseable
    datetime.fromisoformat(raw["extracted_at"])


@pytest.mark.unit
def test_write_empty_cookies_writes_empty_list(tmp_path):
    """Edge: no cookies still produces a valid (empty) payload."""
    path = str(tmp_path / "cf.json")
    assert _write_cf_cookies_file(path, {}) is True
    raw = json.loads((tmp_path / "cf.json").read_text())
    assert raw["cookies"] == []


# ---------------------------------------------------------------------------
# Loader (scraper side) — round-trip + freshness + robustness
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_round_trip_write_then_load(tmp_path):
    """A freshly-written file loads back as the cookie list."""
    path = str(tmp_path / "cf.json")
    _write_cf_cookies_file(path, {"cf_clearance": "AAA", "__cf_bm": "BBB"})

    loaded = _load(path)

    assert [c["name"] for c in loaded] == ["cf_clearance", "__cf_bm"]
    assert loaded[0]["value"] == "AAA"


@pytest.mark.unit
def test_load_accepts_fresh_file(tmp_path):
    """A file within the max age is returned."""
    path = tmp_path / "cf.json"
    path.write_text(json.dumps({
        "cookies": [{"name": "cf_clearance", "value": "X"}],
        "extracted_at": (datetime.now() - timedelta(minutes=5)).isoformat(),
        "proxy_idx": 0,
    }))

    assert len(_load(str(path))) == 1


@pytest.mark.unit
def test_load_rejects_stale_file(tmp_path):
    """A file older than the 25 min default is ignored (returns [])."""
    path = tmp_path / "cf.json"
    path.write_text(json.dumps({
        "cookies": [{"name": "cf_clearance", "value": "X"}],
        "extracted_at": (datetime.now() - timedelta(minutes=40)).isoformat(),
        "proxy_idx": 0,
    }))

    assert _load(str(path)) == []


@pytest.mark.unit
def test_load_respects_custom_max_age(tmp_path):
    """A 5-minute-old file is rejected when max_age_min is 1."""
    path = tmp_path / "cf.json"
    path.write_text(json.dumps({
        "cookies": [{"name": "cf_clearance", "value": "X"}],
        "extracted_at": (datetime.now() - timedelta(minutes=5)).isoformat(),
        "proxy_idx": 0,
    }))

    assert _load(str(path), max_age_min=1) == []


@pytest.mark.unit
def test_load_missing_extracted_at_returns_cookies(tmp_path):
    """No extracted_at => age check skipped, cookies returned (best-effort)."""
    path = tmp_path / "cf.json"
    path.write_text(json.dumps({"cookies": [{"name": "cf_clearance", "value": "X"}]}))

    assert len(_load(str(path))) == 1


@pytest.mark.unit
def test_load_none_path_returns_empty():
    assert _load(None) == []


@pytest.mark.unit
def test_load_missing_file_returns_empty(tmp_path):
    assert _load(str(tmp_path / "does_not_exist.json")) == []


@pytest.mark.unit
def test_load_malformed_json_returns_empty(tmp_path):
    path = tmp_path / "cf.json"
    path.write_text("{not valid json")

    assert _load(str(path)) == []


# ---------------------------------------------------------------------------
# prewarm_cf_cookies integration — persists the file (CFCookieManager mocked)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_prewarm_writes_cookie_file(tmp_path):
    """prewarm_cf_cookies persists obtained cookies AND still pushes XCom."""
    path = str(tmp_path / "cf.json")

    fake_pm = MagicMock()
    fake_pm.total_count = 2
    fake_pm.get_current_proxy.return_value = None  # -> proxy_idx == -1

    fake_mgr = MagicMock()
    fake_mgr.get_cookies_with_retry_sync.return_value = {
        "cf_clearance": "AAA",
        "__cf_bm": "BBB",
    }

    ti = MagicMock()

    with patch("scrapers.utils.proxy_manager.ProxyManager", return_value=fake_pm), \
         patch(
             "scrapers.base.browser.cf_cookie_manager.CFCookieManager",
             return_value=fake_mgr,
         ):
        result = prewarm_cf_cookies(
            proxy_file="/tmp/proxys.txt",
            cf_cookies_file=path,
            cache_ttl_minutes=25,
            use_cf_verify=True,
            cf_verify_max_retries=3,
            cf_verify_interval=1.5,
            use_xvfb=True,
            max_attempts=5,
            ti=ti,
        )

    assert result["success"] is True
    raw = json.loads((tmp_path / "cf.json").read_text())
    assert [c["name"] for c in raw["cookies"]] == ["cf_clearance", "__cf_bm"]
    assert raw["proxy_idx"] == -1
    # The file must round-trip back through the scraper-side loader.
    assert len(_load(path)) == 2
    # XCom push preserved (back-compat).
    ti.xcom_push.assert_called_once()
