"""
Unit tests for `_apply_nodriver_parser_safety_patch` — the monkey-patch that
wraps `nodriver.cdp.util.parse_json_event` so broken CDP parsers in
nodriver 0.48.1 cannot crash the `Connection._listener` event loop.

We don't depend on the real `nodriver` package: a fake module is injected
into `sys.modules['nodriver.cdp.util']` so the patch can be applied in
isolation and the test stays in the `unit` marker.
"""

from __future__ import annotations

import sys
import types

import pytest


@pytest.fixture
def fake_nodriver_util(monkeypatch):
    """Inject a fake `nodriver.cdp.util` module with a broken parser.

    The parser returns the input for valid events and raises `KeyError`
    on the sentinel `{"method": "Network.responseReceivedExtraInfo"}` —
    the exact regression from nodriver 0.48.1.
    """
    # Clear any sentinel from a previous test run.
    fake = types.ModuleType("nodriver.cdp.util")

    def _broken_parser(json):
        if isinstance(json, dict) and json.get("method") == "Network.responseReceivedExtraInfo":
            raise KeyError("charset")
        return ("parsed", json)

    fake.parse_json_event = _broken_parser

    # Provide enough of the package tree for `import nodriver.cdp.util` to work.
    pkg_nodriver = types.ModuleType("nodriver")
    pkg_cdp = types.ModuleType("nodriver.cdp")
    monkeypatch.setitem(sys.modules, "nodriver", pkg_nodriver)
    monkeypatch.setitem(sys.modules, "nodriver.cdp", pkg_cdp)
    monkeypatch.setitem(sys.modules, "nodriver.cdp.util", fake)

    yield fake


@pytest.mark.unit
def test_patch_swallows_broken_parser_exception(fake_nodriver_util):
    from scrapers.base.browser.nodriver_bypass import (
        _apply_nodriver_parser_safety_patch,
    )

    _apply_nodriver_parser_safety_patch()

    # The original parser raised KeyError on this event; after the patch it
    # must return None instead of propagating.
    result = fake_nodriver_util.parse_json_event(
        {"method": "Network.responseReceivedExtraInfo", "params": {}}
    )
    assert result is None


@pytest.mark.unit
def test_patch_passes_through_successful_events(fake_nodriver_util):
    from scrapers.base.browser.nodriver_bypass import (
        _apply_nodriver_parser_safety_patch,
    )

    _apply_nodriver_parser_safety_patch()

    payload = {"method": "Network.requestWillBeSent", "params": {"requestId": "1"}}
    assert fake_nodriver_util.parse_json_event(payload) == ("parsed", payload)


@pytest.mark.unit
def test_patch_is_idempotent(fake_nodriver_util):
    from scrapers.base.browser.nodriver_bypass import (
        _NODRIVER_PATCH_SENTINEL,
        _apply_nodriver_parser_safety_patch,
    )

    _apply_nodriver_parser_safety_patch()
    wrapped_once = fake_nodriver_util.parse_json_event

    _apply_nodriver_parser_safety_patch()
    assert fake_nodriver_util.parse_json_event is wrapped_once
    assert getattr(fake_nodriver_util, _NODRIVER_PATCH_SENTINEL) is True


@pytest.mark.unit
def test_patch_handles_non_dict_events(fake_nodriver_util):
    """Defensive: `parse_json_event` may receive None or junk during teardown."""
    from scrapers.base.browser.nodriver_bypass import (
        _apply_nodriver_parser_safety_patch,
    )

    # Make the original parser raise on non-dict input.
    def _picky(json):
        if not isinstance(json, dict):
            raise TypeError("string indices must be integers, not 'str'")
        return json

    fake_nodriver_util.parse_json_event = _picky
    # Clear sentinel so the patch re-applies on top of the new original.
    if hasattr(fake_nodriver_util, "__data_platform_parse_json_event_patched__"):
        delattr(fake_nodriver_util, "__data_platform_parse_json_event_patched__")

    from scrapers.base.browser.nodriver_bypass import (
        _apply_nodriver_parser_safety_patch,
    )

    _apply_nodriver_parser_safety_patch()
    assert fake_nodriver_util.parse_json_event(None) is None
    assert fake_nodriver_util.parse_json_event("not a dict") is None
