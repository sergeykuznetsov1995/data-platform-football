"""Safety and response-contract tests for the local FlareSolverr extension."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from types import ModuleType, SimpleNamespace

import pytest

from scrapers.base.flaresolverr_client import MAX_XHR_BATCH_URLS
from scrapers.whoscored.service import STRUCTURED_REQUEST_BURST_SIZE


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "flaresolverr_extended.py"
SPEC = importlib.util.spec_from_file_location("flaresolverr_extended", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
ext = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ext
SPEC.loader.exec_module(ext)


BASE_URL = "https://www.whoscored.com/statisticsfeed/1/getteamstatistics"


def _payload(**overrides):
    value = {
        "url": BASE_URL,
        "session": "ws-direct_flaresolverr-0123456789",
        "maxTimeout": 30_000,
    }
    value.update(overrides)
    return value


def _batch_payload(urls=None, **overrides):
    value = {
        "urls": list([BASE_URL] if urls is None else urls),
        "session": "ws-direct_flaresolverr-0123456789",
        "maxTimeout": 30_000,
    }
    value.update(overrides)
    return value


def _batch_success(url=BASE_URL, body=b'{"teamTableStats":[]}'):
    return {
        "ok": True,
        "requestedUrl": url,
        "finalUrl": url,
        "status": 200,
        "headers": {"content-type": "application/json; charset=utf-8"},
        "bodyBase64": base64.b64encode(body).decode("ascii"),
        "responseBytes": len(body),
    }


class FakeDriver:
    def __init__(self, result=None, *, delay=0.0):
        body = b'{"teamTableStats":[]}'
        self.result = result or {
            "ok": True,
            "finalUrl": BASE_URL,
            "status": 200,
            "headers": {"content-type": "application/json; charset=utf-8"},
            "bodyBase64": base64.b64encode(body).decode("ascii"),
            "responseBytes": len(body),
        }
        self.delay = delay
        self.timeouts = SimpleNamespace(script=17.0)
        self.timeout_calls = []
        self.execute_calls = []
        self._active_guard = threading.Lock()
        self.active = 0
        self.max_active = 0

    def set_script_timeout(self, timeout):
        self.timeout_calls.append(timeout)
        self.timeouts.script = timeout

    def execute_async_script(self, *args):
        self.execute_calls.append(args)
        with self._active_guard:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(self.delay)
            return self.result
        finally:
            with self._active_guard:
                self.active -= 1


class FakeStorage:
    def __init__(self, sessions=None):
        self.sessions = sessions or {}
        self.get_called = False

    def exists(self, session_id):
        return session_id in self.sessions

    def get(self, *_args, **_kwargs):
        self.get_called = True
        raise AssertionError("the XHR endpoint must not create a missing session")


class FakeCdpDriver:
    def __init__(self, *, fail_set_blocked_urls=False):
        self.fail_set_blocked_urls = fail_set_blocked_urls
        self.calls = []

    def execute_cdp_cmd(self, command, params):
        self.calls.append(("cdp", command, params))
        if command == "Network.setBlockedURLs" and self.fail_set_blocked_urls:
            raise RuntimeError("CDP unavailable")
        return {}

    def get(self, url):
        self.calls.append(("get", url))


def _stock_like_evil_logic(req, driver, method):
    disable_media = req.config_disable_media
    if req.disableMedia is not None:
        disable_media = req.disableMedia
    if disable_media:
        stock_urls = ["*.png", "*.css", "*.woff"]
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": stock_urls})
        except Exception:
            pass
    driver.get(req.url)
    return method


def _stock_without_cdp(req, driver, method):
    driver.get(req.url)
    return method


def _stock_wrong_signature(req, driver):
    driver.get(req.url)
    return None


def _fake_upstream_service(function, *, configured=False):
    return SimpleNamespace(
        _evil_logic=function,
        utils=SimpleNamespace(get_config_disable_media=lambda: configured),
    )


def _pin_function_source(monkeypatch, function):
    digest = hashlib.sha256(inspect.getsource(function).encode("utf-8")).hexdigest()
    monkeypatch.setattr(ext, "_UPSTREAM_EVIL_LOGIC_SHA256", digest)


def _media_request(*, disable_media=True, configured=False):
    return SimpleNamespace(
        url="https://www.whoscored.com/Regions/252/Tournaments/2",
        disableMedia=disable_media,
        config_disable_media=configured,
    )


def test_media_extension_catalog_is_fixed_audio_video_suffixes_only():
    expected = {
        "mp4",
        "webm",
        "m3u8",
        "mov",
        "m4v",
        "avi",
        "mpeg",
        "mpg",
        "ogv",
        "mp3",
        "wav",
        "ogg",
        "aac",
        "m4a",
        "flac",
    }

    assert set(ext._AUDIO_VIDEO_EXTENSIONS) == expected
    assert set(ext.AUDIO_VIDEO_BLOCK_PATTERNS) == {
        pattern
        for suffix in expected
        for pattern in (f"*.{suffix}", f"*.{suffix.upper()}")
    }
    assert not any(
        token in pattern
        for pattern in ext.AUDIO_VIDEO_BLOCK_PATTERNS
        for token in ("analytics", "advert", "doubleclick", "google")
    )


def test_disable_media_patch_appends_without_replacing_stock_patterns(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic)
    _pin_function_source(monkeypatch, service._evil_logic)
    ext._install_disable_media_extension(service, version="3.4.6")
    driver = FakeCdpDriver()

    result = service._evil_logic(_media_request(), driver, "GET")

    assert result == "GET"
    assert [call[:2] for call in driver.calls] == [
        ("cdp", "Network.enable"),
        ("cdp", "Network.setBlockedURLs"),
        ("get", "https://www.whoscored.com/Regions/252/Tournaments/2"),
    ]
    blocked = driver.calls[1][2]["urls"]
    assert blocked[:3] == ["*.png", "*.css", "*.woff"]
    assert blocked[3:] == list(ext.AUDIO_VIDEO_BLOCK_PATTERNS)
    assert len(blocked) == len(set(blocked))


def test_disable_media_patch_refuses_navigation_when_cdp_fails(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic)
    _pin_function_source(monkeypatch, service._evil_logic)
    ext._install_disable_media_extension(service, version="3.4.6")
    driver = FakeCdpDriver(fail_set_blocked_urls=True)

    with pytest.raises(ext.DisableMediaPatchError, match="before.*blocklist"):
        service._evil_logic(_media_request(), driver, "GET")

    assert all(call[0] != "get" for call in driver.calls)


def test_disable_media_patch_refuses_unpatched_navigation_path(monkeypatch):
    service = _fake_upstream_service(_stock_without_cdp)
    _pin_function_source(monkeypatch, service._evil_logic)
    ext._install_disable_media_extension(service, version="3.4.6")
    driver = FakeCdpDriver()

    with pytest.raises(ext.DisableMediaPatchError, match="before.*blocklist"):
        service._evil_logic(_media_request(), driver, "GET")

    assert driver.calls == []


def test_disable_media_false_keeps_stock_navigation_semantics(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic, configured=False)
    _pin_function_source(monkeypatch, service._evil_logic)
    ext._install_disable_media_extension(service, version="3.4.6")
    driver = FakeCdpDriver()

    service._evil_logic(
        _media_request(disable_media=False, configured=False), driver, "GET"
    )

    assert driver.calls == [
        ("get", "https://www.whoscored.com/Regions/252/Tournaments/2")
    ]


def test_disable_media_patch_is_version_source_and_signature_pinned(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic)
    with pytest.raises(ext.DisableMediaPatchError, match="Unsupported"):
        ext._install_disable_media_extension(service, version="3.5.0")
    with pytest.raises(ext.DisableMediaPatchError, match="source does not match"):
        ext._install_disable_media_extension(service, version="3.4.6")

    wrong_signature = _fake_upstream_service(_stock_wrong_signature)
    _pin_function_source(monkeypatch, wrong_signature._evil_logic)
    with pytest.raises(ext.DisableMediaPatchError, match="signature"):
        ext._install_disable_media_extension(wrong_signature, version="3.4.6")

    _pin_function_source(monkeypatch, service._evil_logic)
    ext._install_disable_media_extension(service, version="3.4.6")
    patched = service._evil_logic
    ext._install_disable_media_extension(service, version="3.4.6")
    assert service._evil_logic is patched


def test_create_app_installs_media_patch_before_registering_route(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic)
    service.SESSIONS_STORAGE = FakeStorage()
    _pin_function_source(monkeypatch, service._evil_logic)

    class _App:
        def __init__(self):
            self.routes = []

        def post(self, path):
            def register(function):
                self.routes.append((path, function))
                return function

            return register

    app = _App()
    bottle = ModuleType("bottle")
    bottle.request = SimpleNamespace()
    bottle.response = SimpleNamespace()
    upstream = ModuleType("flaresolverr")
    upstream.app = app
    utils = ModuleType("utils")
    utils.get_flaresolverr_version = lambda: "3.4.6"
    monkeypatch.setitem(sys.modules, "bottle", bottle)
    monkeypatch.setitem(sys.modules, "flaresolverr", upstream)
    monkeypatch.setitem(sys.modules, "flaresolverr_service", service)
    monkeypatch.setitem(sys.modules, "utils", utils)

    assert ext.create_app() is app
    assert app.routes[0][0] == "/v1/xhr"
    assert app.routes[1][0] == "/v1/xhr/batch"
    assert getattr(service._evil_logic, ext._MEDIA_PATCH_MARKER) == (
        "3.4.6",
        ext._UPSTREAM_EVIL_LOGIC_SHA256,
    )


def test_main_installs_media_patch_before_upstream_browser_self_test():
    source = inspect.getsource(ext.main)

    assert source.index("_install_disable_media_extension(") < source.index(
        "test_browser_installation()"
    )


@pytest.mark.parametrize(
    "url",
    [
        BASE_URL,
        "https://www.whoscored.com/stagestatfeed/23752/stageteams/?type=2",
        "https://www.whoscored.com/stageplayerstatfeed/23752/playerstats/?page=1",
    ],
)
def test_url_allowlist_accepts_only_three_structured_feed_prefixes(url):
    assert ext._validate_whoscored_feed_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "http://www.whoscored.com/statisticsfeed/1/x",
        "https://whoscored.com/statisticsfeed/1/x",
        "https://api.whoscored.com/statisticsfeed/1/x",
        "https://www.whoscored.com.evil.test/statisticsfeed/1/x",
        "https://www.whoscored.com:443/statisticsfeed/1/x",
        "https://user:password@www.whoscored.com/statisticsfeed/1/x",
        "https://www.whoscored.com/statisticsfeed/1/x#fragment",
        "https://www.whoscored.com/statisticsfeedx/1/x",
        "https://www.whoscored.com/Regions/252/Tournaments/2",
        "https://www.whoscored.com/statisticsfeed/../../Regions/252",
        "https://www.whoscored.com/statisticsfeed/%2e%2e/Regions/252",
        "https://www.whoscored.com/statisticsfeed//1/x",
        "https://www.whoscored.com/statisticsfeed/1/arbitrary",
        "https://www.whoscored.com/stagestatfeed/0/stageteams/?type=2",
        "https://www.whoscored.com/stagestatfeed/23752/other/?type=2",
        "https://www.whoscored.com/stageplayerstatfeed/x/playerstats/?page=1",
        "https://www.whoscored.com\\@evil.test/statisticsfeed/1/x",
        "https://www.whoscored.com/statisticsfeed/1/x\nHost:evil.test",
    ],
)
def test_url_allowlist_rejects_other_origins_ports_credentials_and_paths(url):
    with pytest.raises(ext.XhrEndpointError):
        ext._validate_whoscored_feed_url(url)


@pytest.mark.parametrize(
    "field,value",
    [
        ("method", "POST"),
        ("headers", {"Authorization": "secret"}),
        ("javascript", "return document.cookie"),
        ("script", "fetch('https://evil.test')"),
        ("proxy", {"url": "http://paid-proxy"}),
        ("cookies", [{"name": "x", "value": "y"}]),
        ("credentials", "omit"),
        ("maxResponseBytes", 999_999_999),
    ],
)
def test_payload_rejects_every_request_controlled_browser_option(field, value):
    with pytest.raises(ext.XhrEndpointError, match="Unsupported"):
        ext._validate_payload(_payload(**{field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("concurrency", 100),
        ("method", "POST"),
        ("headers", {"Authorization": "secret"}),
        ("javascript", "return document.cookie"),
        ("proxy", {"url": "http://paid-proxy"}),
        ("cookies", [{"name": "x", "value": "y"}]),
        ("maxResponseBytes", 999_999_999),
    ],
)
def test_batch_payload_rejects_every_request_controlled_browser_option(field, value):
    with pytest.raises(ext.XhrEndpointError, match="Unsupported"):
        ext._validate_batch_payload(_batch_payload(**{field: value}))


@pytest.mark.parametrize(
    "urls",
    [
        [],
        [BASE_URL] * 2,
        [f"{BASE_URL}?item={index}" for index in range(9)],
        [BASE_URL, "https://evil.test/statisticsfeed/1/x"],
    ],
)
def test_batch_payload_has_hard_count_uniqueness_and_url_allowlist(urls):
    with pytest.raises(ext.XhrEndpointError):
        ext._validate_batch_payload(_batch_payload(urls))


@pytest.mark.parametrize(
    "session",
    ["fs-123", "ws-", "ws-has spaces", "ws-has.dot", "WS-direct-1", 123],
)
def test_payload_requires_bounded_ws_session_id(session):
    with pytest.raises(ext.XhrEndpointError, match="session"):
        ext._validate_payload(_payload(session=session))


@pytest.mark.parametrize("timeout", [True, 1.5, "30000", 999, 120_001])
def test_payload_rejects_unsafe_timeout_values(timeout):
    with pytest.raises(ext.XhrEndpointError, match="maxTimeout"):
        ext._validate_payload(_payload(maxTimeout=timeout))


def test_fixed_script_enforces_method_headers_credentials_origin_and_size():
    script = ext.XHR_SCRIPT
    assert 'method: "GET"' in script
    assert 'credentials: "same-origin"' in script
    assert 'mode: "cors"' in script
    assert 'redirect: "error"' in script
    assert 'redirect: "follow"' not in script
    assert "finalUrl.origin" in script
    assert '"Accept": "application/json, text/javascript, */*; q=0.01"' in script
    assert '"X-Requested-With": "XMLHttpRequest"' in script
    assert 'siteConfig.gSiteHeaderName !== "Model-last-Mode"' in script
    assert '"Model-last-Mode": siteConfig.gSiteHeaderValue' in script
    assert "/^[A-Za-z0-9+/]{43}=$/" in script
    assert "headers[siteConfig" not in script
    assert 'cache: "no-store"' not in script
    assert "get(?:team|player)statistics" in script
    assert "stagestatfeed\\/[1-9][0-9]*\\/stageteams" in script
    assert "stageplayerstatfeed\\/[1-9][0-9]*\\/playerstats" in script
    assert "response_too_large" in script
    assert "eval(" not in script
    assert "new Function" not in script
    assert BASE_URL not in script


def test_fixed_batch_script_has_server_side_security_and_resource_limits():
    script = ext.BATCH_XHR_SCRIPT
    assert ext.MAX_BATCH_URLS == MAX_XHR_BATCH_URLS == 8
    assert ext.BATCH_CONCURRENCY == STRUCTURED_REQUEST_BURST_SIZE == 4
    assert ext.MAX_RESPONSE_BYTES == 4 * 1024 * 1024
    assert ext.MAX_BATCH_RESPONSE_BYTES == 8 * 1024 * 1024
    assert 'method: "GET"' in script
    assert 'credentials: "same-origin"' in script
    assert 'mode: "cors"' in script
    assert 'redirect: "error"' in script
    assert 'redirect: "follow"' not in script
    assert '"Model-last-Mode": siteConfig.gSiteHeaderValue' in script
    assert "Math.min(concurrency, targetUrls.length)" in script
    assert "maxBytesPerResponse" in script
    assert "maxAggregateBytes" in script
    assert "let consumedBytes = 0" in script
    assert "let successBytes = 0" in script
    assert "consumedBytes += item.value.byteLength" in script
    assert "consumedBytes -=" not in script
    assert 'kind: "aggregate_too_large"' in script
    assert (
        "for (const activeController of controllers) activeController.abort()" in script
    )
    assert "eval(" not in script
    assert "new Function" not in script
    assert BASE_URL not in script


def test_success_uses_existing_session_and_returns_exact_client_contract():
    driver = FakeDriver()
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 200
    assert body["status"] == "ok"
    assert body["version"] == "3.4.6"
    assert set(body["solution"]) == {
        "responseBase64",
        "responseBytes",
        "headers",
        "finalUrl",
        "status",
    }
    assert base64.b64decode(body["solution"]["responseBase64"]) == (
        b'{"teamTableStats":[]}'
    )
    assert body["solution"]["responseBytes"] == len(b'{"teamTableStats":[]}')
    assert body["solution"]["finalUrl"] == BASE_URL
    assert body["solution"]["status"] == 200
    assert storage.get_called is False
    assert len(driver.execute_calls) == 1
    script, url, response_limit, timeout_ms = driver.execute_calls[0]
    assert script == ext.XHR_SCRIPT
    assert url == BASE_URL
    assert response_limit == 4 * 1024 * 1024
    assert 0 < timeout_ms <= 30_000
    assert driver.timeout_calls[-1] == 17.0


def test_batch_returns_successes_and_sanitised_runtime_item_errors_in_order():
    second_url = BASE_URL + "?category=passing"
    success = _batch_success()
    failure = {
        "ok": False,
        "requestedUrl": second_url,
        "kind": "fetch_failed",
        "error": "fetch_failed",
    }
    driver = FakeDriver(
        {"ok": True, "responses": [success, failure], "responseBytes": 21}
    )
    session_id = _batch_payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_batch_request(
        _batch_payload([BASE_URL, second_url]),
        storage=storage,
        version_getter=lambda: "3.4.6",
    )

    assert status == 200
    assert body["status"] == "ok"
    assert body["solution"]["responses"][0]["ok"] is True
    assert (
        base64.b64decode(body["solution"]["responses"][0]["responseBase64"])
        == b'{"teamTableStats":[]}'
    )
    assert body["solution"]["responses"][1] == {
        "ok": False,
        "requestedUrl": second_url,
        "kind": "fetch_failed",
    }
    script, urls, per_item_limit, aggregate_limit, timeout_ms, concurrency = (
        driver.execute_calls[0]
    )
    assert script == ext.BATCH_XHR_SCRIPT
    assert urls == [BASE_URL, second_url]
    assert per_item_limit == 4 * 1024 * 1024
    assert aggregate_limit == 8 * 1024 * 1024
    assert 0 < timeout_ms <= 30_000
    assert concurrency == 4


@pytest.mark.parametrize(
    "bad_item",
    [
        {"ok": False, "requestedUrl": BASE_URL, "kind": "unknown", "error": "x"},
        {
            "ok": False,
            "requestedUrl": BASE_URL,
            "kind": "fetch_failed",
            "error": "fetch_failed",
            "bodyBase64": "e30=",
        },
        {"ok": True, "requestedUrl": "https://evil.test/statisticsfeed/1/x"},
    ],
)
def test_malformed_runtime_batch_item_fails_whole_endpoint_contract(bad_item):
    driver = FakeDriver({"ok": True, "responses": [bad_item], "responseBytes": 0})
    session_id = _batch_payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_batch_request(
        _batch_payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 502
    assert body["status"] == "error"
    assert "solution" not in body


def test_batch_aggregate_limit_is_global_monotonic_and_exposes_no_bodies():
    """Eight failed items cannot each consume a fresh per-item allowance."""

    script = ext.BATCH_XHR_SCRIPT
    assert "consumedBytes += item.value.byteLength" in script
    assert "consumedBytes = Math.max" not in script
    assert "aggregateBytes" not in script
    assert "if (consumedBytes > maxAggregateBytes)" in script

    # Even if a compromised driver includes previously successful bodies, the
    # endpoint maps the global terminal result before inspecting responses.
    driver = FakeDriver(
        {
            "ok": False,
            "kind": "aggregate_too_large",
            "error": "aggregate_too_large",
            "responses": [
                {
                    "ok": True,
                    "requestedUrl": BASE_URL,
                    "bodyBase64": base64.b64encode(b"must-not-escape").decode(),
                }
            ],
        }
    )
    session_id = _batch_payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_batch_request(
        _batch_payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 413
    assert body["status"] == "error"
    assert "solution" not in body


def test_batch_aggregate_limit_aborts_eight_adversarial_streams_in_node():
    """Execute the shipped JS: failed items cannot reset the global byte cap."""

    node = shutil.which("node")
    assert node is not None, "Node.js is required for the browser-script safety test"
    batch_script = json.dumps(ext.BATCH_XHR_SCRIPT)
    harness = f"""
const batchScript = {batch_script};
const MiB = 1024 * 1024;
const urls = Array.from({{length: 8}}, (_, index) =>
  "https://www.whoscored.com/statisticsfeed/1/getteamstatistics?item=" + index
);
global.window = {{require: {{config: {{params: {{site: {{
  gSiteHeaderName: "Model-last-Mode",
  gSiteHeaderValue: "A".repeat(43) + "="
}}}}}}}}}};
let observedBytes = 0;
let fetchStarts = 0;
let activeStreams = 0;
let maxActiveStreams = 0;
const chunkBytes = MiB;
global.fetch = async (url, options) => {{
  fetchStarts += 1;
  activeStreams += 1;
  maxActiveStreams = Math.max(maxActiveStreams, activeStreams);
  let chunkIndex = 0;
  let finished = false;
  const finish = () => {{
    if (!finished) {{
      finished = true;
      activeStreams -= 1;
    }}
  }};
  const reader = {{
    async read() {{
      await new Promise((resolve) => setImmediate(resolve));
      if (options.signal.aborted) {{
        finish();
        const error = new Error("aborted");
        error.name = "AbortError";
        throw error;
      }}
      if (chunkIndex >= 5) {{
        finish();
        return {{done: true}};
      }}
      chunkIndex += 1;
      observedBytes += chunkBytes;
      return {{done: false, value: new Uint8Array(chunkBytes)}};
    }},
    async cancel() {{ finish(); }},
    releaseLock() {{ finish(); }}
  }};
  return {{
    url,
    status: 200,
    headers: {{entries: () => []}},
    body: {{getReader: () => reader}}
  }};
}};
const execute = new Function(batchScript);
const terminal = await new Promise((resolve, reject) => {{
  const timer = setTimeout(
    () => reject(new Error("batch callback timeout")),
    5000
  );
  execute(urls, 4 * MiB, 8 * MiB, 3000, 4, (result) => {{
    clearTimeout(timer);
    resolve(result);
  }});
}});
process.stdout.write(JSON.stringify({{
  terminal,
  observedBytes,
  fetchStarts,
  maxActiveStreams,
  activeStreams,
  chunkBytes
}}));
"""

    completed = subprocess.run(
        [node, "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=True,
        timeout=10,
    )
    observed = json.loads(completed.stdout)

    assert observed["terminal"] == {
        "ok": False,
        "kind": "aggregate_too_large",
        "error": "aggregate_too_large",
    }
    assert "responses" not in observed["terminal"]
    assert ext.MAX_BATCH_RESPONSE_BYTES < observed["observedBytes"]
    assert observed["observedBytes"] <= (
        ext.MAX_BATCH_RESPONSE_BYTES + observed["chunkBytes"]
    )
    assert observed["fetchStarts"] <= ext.BATCH_CONCURRENCY
    assert observed["maxActiveStreams"] <= ext.BATCH_CONCURRENCY
    assert observed["activeStreams"] == 0


def test_missing_session_is_404_and_never_created():
    storage = FakeStorage()

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 404
    assert body["status"] == "error"
    assert "does not exist" in body["message"]
    assert storage.get_called is False


@pytest.mark.parametrize(
    "result,expected_status,expected_message",
    [
        (
            {"ok": False, "kind": "response_too_large"},
            413,
            "exceeds",
        ),
        ({"ok": False, "kind": "timeout"}, 504, "timed out"),
        (
            {"ok": False, "kind": "source_header_unavailable"},
            502,
            "request header is unavailable",
        ),
        (
            {"ok": False, "kind": "source_redirect_rejected"},
            502,
            "redirected outside the allow-list",
        ),
        ({"ok": False, "kind": "fetch_failed"}, 502, "fetch failed"),
    ],
)
def test_browser_failures_have_typed_explicit_errors(
    result, expected_status, expected_message
):
    driver = FakeDriver(result)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == expected_status
    assert body["status"] == "error"
    assert expected_message in body["message"]
    assert "solution" not in body


def test_external_or_non_feed_final_url_is_rejected_after_browser_execution():
    result = FakeDriver().result | {"finalUrl": "https://evil.test/statisticsfeed/1/x"}
    driver = FakeDriver(result)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 502
    assert body["status"] == "error"
    assert "forbidden final" in body["message"]


def test_allowlisted_but_different_final_url_is_rejected_without_body():
    different = BASE_URL + "?stageId=999"
    result = FakeDriver().result | {"finalUrl": different}
    driver = FakeDriver(result)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 502
    assert body["status"] == "error"
    assert "unexpected final" in body["message"]
    assert "solution" not in body


@pytest.mark.parametrize(
    "overrides",
    [
        {"bodyBase64": "not-base64"},
        {"responseBytes": 999},
        {"status": True},
        {"headers": [("content-type", "application/json")]},
        {"headers": {"bad header": "value"}},
        {"headers": {"x-test": "bad\r\nvalue"}},
    ],
)
def test_malformed_browser_results_fail_closed(overrides):
    result = FakeDriver().result | overrides
    driver = FakeDriver(result)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    body, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 502
    assert body["status"] == "error"


def test_python_side_limit_rejects_oversized_body_even_if_browser_claims_success():
    body = b"x" * (ext.MAX_RESPONSE_BYTES + 1)
    result = FakeDriver().result | {
        "bodyBase64": base64.b64encode(body).decode("ascii"),
        "responseBytes": len(body),
    }
    driver = FakeDriver(result)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    response, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == 413
    assert response["status"] == "error"


@pytest.mark.parametrize(
    "error,expected_status",
    [(RuntimeError("tab crashed"), 502), (TimeoutError("slow"), 504)],
)
def test_webdriver_execution_errors_are_sanitised(error, expected_status):
    driver = FakeDriver()

    def fail(*_args):
        raise error

    driver.execute_async_script = fail
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})

    response, status = ext.handle_xhr_request(
        _payload(), storage=storage, version_getter=lambda: "3.4.6"
    )

    assert status == expected_status
    assert response["status"] == "error"
    assert "tab crashed" not in response["message"]


def test_same_session_browser_calls_are_serialised():
    driver = FakeDriver(delay=0.03)
    session_id = _payload()["session"]
    storage = FakeStorage({session_id: SimpleNamespace(driver=driver)})
    locks = ext._SessionLocks()
    results = []

    def run_request():
        results.append(
            ext.handle_xhr_request(
                _payload(maxTimeout=5_000),
                storage=storage,
                version_getter=lambda: "3.4.6",
                locks=locks,
            )
        )

    threads = [threading.Thread(target=run_request) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert all(status == 200 for _body, status in results)
    assert len(results) == 5
    assert driver.max_active == 1
