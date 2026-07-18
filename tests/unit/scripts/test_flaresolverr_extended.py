"""Safety and response-contract tests for the local FlareSolverr extension."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import inspect
import io
import json
import logging
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


@pytest.fixture(autouse=True)
def _fresh_global_xhr_pacer(monkeypatch):
    """Keep fast fake-driver tests independent while production stays process-wide."""

    monkeypatch.setattr(ext, "_XHR_START_PACER", ext._XhrStartPacer())
    monkeypatch.delenv(ext.PAID_EXCLUSIVE_MODE_ENV, raising=False)
    monkeypatch.delenv(ext.PAID_GATEWAY_SECRET_ENV, raising=False)


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
    def __init__(self, result=None, *, delay=0.0, launch_delay=0.0):
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
        self.launch_delay = launch_delay
        self.timeouts = SimpleNamespace(script=17.0)
        self.timeout_calls = []
        self.execute_calls = []
        self.launch_calls = []
        self.abort_calls = []
        self.actual_starts = []
        self.collect_started = threading.Event()
        self.collect_release = threading.Event()
        self.collect_release.set()
        self.quit_started = threading.Event()
        self.quit_calls = 0
        self._active_guard = threading.Lock()
        self.active = 0
        self.max_active = 0

    def set_script_timeout(self, timeout):
        self.timeout_calls.append(timeout)
        self.timeouts.script = timeout

    def execute_script(self, *args):
        if args[0] == ext.XHR_ABORT_SCRIPT:
            self.abort_calls.append(args)
            return True
        assert args[0] == ext.XHR_SCRIPT
        self.launch_calls.append(args)
        time.sleep(self.launch_delay)
        self.actual_starts.append(time.monotonic())
        return {"ok": True, "started": True, "itemIndex": args[-1]}

    def execute_async_script(self, *args):
        assert args[0] == ext.XHR_COLLECT_SCRIPT
        self.execute_calls.append(args)
        self.collect_started.set()
        assert self.collect_release.wait(timeout=3)
        with self._active_guard:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(self.delay)
            if "responses" in self.result or self.result.get("kind") == (
                "aggregate_too_large"
            ):
                return self.result
            requested_url = self.launch_calls[-1][1]
            item = {"requestedUrl": requested_url, **self.result}
            return {
                "ok": True,
                "responses": [item],
                "responseBytes": (
                    self.result.get("responseBytes", 0)
                    if self.result.get("ok") is True
                    else 0
                ),
            }
        finally:
            with self._active_guard:
                self.active -= 1

    def close(self):
        return None

    def quit(self):
        self.quit_calls += 1
        self.quit_started.set()


class FakeStorage:
    def __init__(self, sessions=None):
        self.sessions = sessions or {}
        self.get_called = False

    def exists(self, session_id):
        return session_id in self.sessions

    def get(self, *_args, **_kwargs):
        self.get_called = True
        raise AssertionError("the XHR endpoint must not create a missing session")

    def create(self, session_id=None, proxy=None, force_new=False):
        del proxy
        if force_new:
            self.destroy(session_id)
        if session_id in self.sessions:
            return self.sessions[session_id], False
        session = SimpleNamespace(session_id=session_id, driver=SimpleNamespace())
        self.sessions[session_id] = session
        return session, True

    def destroy(self, session_id):
        return self.sessions.pop(session_id, None) is not None

    def session_ids(self):
        return list(self.sessions)


class FakeV1Response:
    def __init__(self, _value):
        self.__error_500__ = False


def _stock_controller_v1(req):
    return req


def _stock_controller_handler(req):
    return req


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
    utils = SimpleNamespace(
        get_config_disable_media=lambda: configured,
        get_flaresolverr_version=lambda: "3.4.6",
        PLATFORM_VERSION="linux",
    )
    return SimpleNamespace(
        _evil_logic=function,
        utils=utils,
        controller_v1_endpoint=_stock_controller_v1,
        _controller_v1_handler=_stock_controller_handler,
        V1ResponseBase=FakeV1Response,
        STATUS_ERROR="error",
    )


def _pin_function_source(monkeypatch, function):
    digest = hashlib.sha256(inspect.getsource(function).encode("utf-8")).hexdigest()
    monkeypatch.setattr(ext, "_UPSTREAM_EVIL_LOGIC_SHA256", digest)


def _pin_lifecycle_sources(monkeypatch, service):
    storage_digest = hashlib.sha256(
        inspect.getsource(type(service.SESSIONS_STORAGE)).encode("utf-8")
    ).hexdigest()
    controller_digest = hashlib.sha256(
        inspect.getsource(service.controller_v1_endpoint).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(ext, "_UPSTREAM_SESSIONS_STORAGE_SHA256", storage_digest)
    monkeypatch.setattr(ext, "_UPSTREAM_CONTROLLER_V1_SHA256", controller_digest)


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


def test_paid_exclusive_mode_is_explicit_and_requires_a_strong_secret():
    secret = "gateway-secret-0123456789abcdef-strong"

    assert ext._paid_gateway_authorizer_from_environment({}) is None
    assert (
        ext._paid_gateway_authorizer_from_environment(
            {
                ext.PAID_EXCLUSIVE_MODE_ENV: "0",
                ext.PAID_GATEWAY_SECRET_ENV: secret,
            }
        )
        is None
    )
    with pytest.raises(ext.PaidGatewayConfigurationError, match="must be 0 or 1"):
        ext._paid_gateway_authorizer_from_environment(
            {ext.PAID_EXCLUSIVE_MODE_ENV: "true"}
        )
    for invalid_secret in ("", "short", " " * 32, "a" * 31):
        with pytest.raises(ext.PaidGatewayConfigurationError, match="32..4096"):
            ext._paid_gateway_authorizer_from_environment(
                {
                    ext.PAID_EXCLUSIVE_MODE_ENV: "1",
                    ext.PAID_GATEWAY_SECRET_ENV: invalid_secret,
                }
            )

    authorizer = ext._paid_gateway_authorizer_from_environment(
        {
            ext.PAID_EXCLUSIVE_MODE_ENV: "1",
            ext.PAID_GATEWAY_SECRET_ENV: secret,
        }
    )
    assert isinstance(authorizer, ext._PaidGatewayAuthorizer)
    assert secret not in repr(authorizer)


def test_paid_secret_and_capability_never_enter_errors_or_logs(caplog):
    invalid_secret = "a" * 32 + "\ud800"
    with pytest.raises(ext.PaidGatewayConfigurationError) as invalid:
        ext._PaidGatewayAuthorizer(invalid_secret)
    assert invalid_secret not in str(invalid.value)
    assert invalid.value.__context__ is None

    secret = "gateway-secret-0123456789abcdef-strong"
    instance_id = "9" * 32
    body = b'{"cmd":"sessions.list"}'
    headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=1_750_000_000,
        nonce="8" * 32,
    )
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id=instance_id,
        clock=lambda: 1_750_000_000,
    )
    with caplog.at_level(logging.DEBUG):
        assert authorizer.authorize(
            method="POST",
            path="/v1",
            query_string="",
            body=body,
            headers=headers,
        )
        assert not authorizer.authorize(
            method="POST",
            path="/v1",
            query_string="",
            body=body,
            headers=headers,
        )
    assert secret not in caplog.text
    assert headers[ext.PAID_GATEWAY_CAPABILITY_HEADER] not in caplog.text


def test_paid_capability_is_body_session_path_instance_and_replay_bound():
    now = 1_750_000_000
    secret = "gateway-secret-0123456789abcdef-strong"
    instance_id = "a" * 32
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id=instance_id,
        clock=lambda: now,
    )
    body = json.dumps(
        {
            "cmd": "sessions.create",
            "session": "ws-paid-session-a",
        },
        separators=(",", ":"),
    ).encode()
    headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now,
        nonce="b" * 32,
    )

    assert authorizer.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=headers
    )
    assert not authorizer.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=headers
    )

    # A fresh verifier proves the HMAC itself cannot cross a session/body,
    # route, method, query or process-instance boundary.
    mutations = (
        {
            "body": body.replace(b"session-a", b"session-b"),
        },
        {"path": "/v1/xhr"},
        {"method": "DELETE"},
        {"query_string": "debug=1"},
    )
    for mutation in mutations:
        verifier = ext._PaidGatewayAuthorizer(
            secret,
            instance_id=instance_id,
            clock=lambda: now,
        )
        request = {
            "method": "POST",
            "path": "/v1",
            "query_string": "",
            "body": body,
            "headers": headers,
            **mutation,
        }
        assert not verifier.authorize(**request)
    restarted = ext._PaidGatewayAuthorizer(
        secret,
        instance_id="c" * 32,
        clock=lambda: now,
    )
    assert not restarted.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=headers
    )


@pytest.mark.parametrize("offset", [-31, 31])
def test_paid_capability_rejects_stale_or_future_timestamp(offset):
    now = 1_750_000_000
    secret = "gateway-secret-0123456789abcdef-strong"
    instance_id = "d" * 32
    body = b'{"cmd":"sessions.list"}'
    headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now + offset,
        nonce="e" * 32,
    )
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id=instance_id,
        clock=lambda: now,
    )

    assert not authorizer.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=headers
    )


def test_paid_capability_rejects_huge_numeric_timestamp_before_int(monkeypatch):
    authorizer = ext._PaidGatewayAuthorizer(
        "gateway-secret-0123456789abcdef-strong",
        instance_id="d" * 32,
        clock=lambda: 1_750_000_000,
    )
    original_int = int

    def bounded_int(value):
        if isinstance(value, str):
            raise AssertionError("oversized timestamp reached int()")
        return original_int(value)

    monkeypatch.setattr(ext, "int", bounded_int, raising=False)
    assert not authorizer.authorize(
        method="POST",
        path="/v1",
        query_string="",
        body=b'{"cmd":"sessions.list"}',
        headers={
            ext.PAID_GATEWAY_INSTANCE_HEADER: authorizer.instance_id,
            ext.PAID_GATEWAY_TIMESTAMP_HEADER: "9" * 100_000,
            ext.PAID_GATEWAY_NONCE_HEADER: "e" * 32,
            ext.PAID_GATEWAY_CAPABILITY_HEADER: "f" * 64,
        },
    )


def test_paid_capability_cache_ceiling_fails_closed_and_expiry_reclaims_space():
    now = [1_750_000_000]
    secret = "gateway-secret-0123456789abcdef-strong"
    instance_id = "e" * 32
    body = b'{"cmd":"sessions.list"}'
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id=instance_id,
        clock=lambda: now[0],
    )
    authorizer._used_nonces = {
        f"{index:032x}": now[0] + ext.PAID_GATEWAY_MAX_CLOCK_SKEW_SECONDS
        for index in range(ext.PAID_GATEWAY_MAX_REPLAY_ENTRIES)
    }
    full_nonce = "f" * 32
    full_headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now[0],
        nonce=full_nonce,
    )

    assert not authorizer.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=full_headers
    )
    assert len(authorizer._used_nonces) == ext.PAID_GATEWAY_MAX_REPLAY_ENTRIES
    assert full_nonce not in authorizer._used_nonces

    now[0] += ext.PAID_GATEWAY_MAX_CLOCK_SKEW_SECONDS + 1
    fresh_nonce = "a" * 32
    fresh_headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now[0],
        nonce=fresh_nonce,
    )
    assert authorizer.authorize(
        method="POST", path="/v1", query_string="", body=body, headers=fresh_headers
    )
    assert authorizer._used_nonces == {
        fresh_nonce: now[0] + ext.PAID_GATEWAY_MAX_CLOCK_SKEW_SECONDS
    }


def test_paid_capability_replay_consumption_is_atomic():
    now = 1_750_000_000
    secret = "gateway-secret-0123456789abcdef-strong"
    instance_id = "f" * 32
    body = b'{"cmd":"sessions.destroy","session":"ws-paid-a"}'
    headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now,
        nonce="1" * 32,
    )
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id=instance_id,
        clock=lambda: now,
    )
    barrier = threading.Barrier(8)
    outcomes = []
    outcome_lock = threading.Lock()

    def authorize_once():
        barrier.wait(timeout=2)
        result = authorizer.authorize(
            method="POST",
            path="/v1",
            query_string="",
            body=body,
            headers=headers,
        )
        with outcome_lock:
            outcomes.append(result)

    threads = [threading.Thread(target=authorize_once) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 7


def test_paid_capability_malformed_auth_still_uses_constant_time_comparisons(
    monkeypatch,
):
    authorizer = ext._PaidGatewayAuthorizer(
        "gateway-secret-0123456789abcdef-strong",
        instance_id="2" * 32,
        clock=lambda: 1_750_000_000,
    )
    original = ext.secrets.compare_digest
    compared = []

    def record_compare(left, right):
        compared.append((left, right))
        return original(left, right)

    monkeypatch.setattr(ext.secrets, "compare_digest", record_compare)

    assert not authorizer.authorize(
        method="POST",
        path="/v1",
        query_string="",
        body=b"{}",
        headers={},
    )
    assert len(compared) == 2
    assert all(len(left) == len(right) for left, right in compared)


def test_paid_create_app_protects_all_v1_routes_and_leaves_identity_read_only(
    monkeypatch,
):
    service = _fake_upstream_service(_stock_like_evil_logic)
    service.SESSIONS_STORAGE = FakeStorage()
    _pin_function_source(monkeypatch, service._evil_logic)
    _pin_lifecycle_sources(monkeypatch, service)
    monkeypatch.setattr(ext, "_install_safe_logging", lambda: None)

    class PaidAbort(Exception):
        def __init__(self, status, message):
            super().__init__(message)
            self.status = status

    class _App:
        def __init__(self):
            self.routes = []
            self.hooks = []

        def get(self, path):
            def register(function):
                self.routes.append((path, function))
                return function

            return register

        def post(self, path):
            def register(function):
                self.routes.append((path, function))
                return function

            return register

        def hook(self, name):
            assert name == "before_request"

            def register(function):
                self.hooks.append(function)
                return function

            return register

    request = SimpleNamespace(
        method="GET",
        path="/health",
        query_string="",
        body=io.BytesIO(b""),
        headers={},
        content_type="application/json",
    )
    app = _App()
    bottle = ModuleType("bottle")
    bottle.request = request
    bottle.response = SimpleNamespace()

    def abort(status, message):
        raise PaidAbort(status, message)

    bottle.abort = abort
    upstream = ModuleType("flaresolverr")
    upstream.app = app
    utils = ModuleType("utils")
    utils.get_flaresolverr_version = lambda: "3.4.6"
    monkeypatch.setitem(sys.modules, "bottle", bottle)
    monkeypatch.setitem(sys.modules, "flaresolverr", upstream)
    monkeypatch.setitem(sys.modules, "flaresolverr_service", service)
    monkeypatch.setitem(sys.modules, "utils", utils)
    secret = "gateway-secret-0123456789abcdef-strong"
    now = 1_750_000_000
    authorizer = ext._PaidGatewayAuthorizer(
        secret,
        instance_id="3" * 32,
        clock=lambda: now,
    )

    assert ext.create_app(paid_gateway_authorizer=authorizer) is app
    assert len(app.hooks) == 1
    hook = app.hooks[0]
    assert hook() is None  # GET /health
    request.path = "/v1/whoscored/runtime-identity"
    assert hook() is None
    identity_route = next(
        function
        for path, function in app.routes
        if path == "/v1/whoscored/runtime-identity"
    )
    assert identity_route() == {
        "status": "ok",
        "version": "3.4.6",
        "extension_sha256": ext.EXTENSION_SHA256,
        "paid_exclusive": True,
        "capability_schema": ext.PAID_GATEWAY_CAPABILITY_SCHEMA,
        "capability_instance_id": "3" * 32,
    }

    protected_requests = (
        ("/v1", b'{"cmd":"sessions.list"}'),
        (
            "/v1",
            b'{"cmd":"sessions.create","session":"ws-paid-create"}',
        ),
        (
            "/v1",
            b'{"cmd":"request.get","session":"ws-paid-use",'
            b'"url":"https://www.whoscored.com/"}',
        ),
        (
            "/v1",
            b'{"cmd":"sessions.destroy","session":"ws-paid-destroy"}',
        ),
        ("/v1/xhr", b"{}"),
        ("/v1/xhr/batch", b"{}"),
        ("/v1/whoscored/capacity-sessions/cleanup", b"{}"),
    )
    for path, body in protected_requests:
        request.method = "POST"
        request.path = path
        request.body = io.BytesIO(body)
        request.headers = {}
        with pytest.raises(PaidAbort) as denied:
            hook()
        assert denied.value.status == 401

    body = b'{"cmd":"sessions.create","session":"ws-paid-create"}'
    request.method = "POST"
    request.path = "/v1"
    request.body = io.BytesIO(body)
    request.content_type = "text/plain"
    request.headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=authorizer.instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now,
        nonce="5" * 32,
    )
    with pytest.raises(PaidAbort) as wrong_content_type:
        hook()
    assert wrong_content_type.value.status == 401

    request.body = io.BytesIO(body)
    request.content_type = "application/json; charset=utf-8"
    request.headers = ext.build_paid_gateway_capability_headers(
        secret,
        instance_id=authorizer.instance_id,
        method="POST",
        path="/v1",
        body=body,
        timestamp=now,
        nonce="4" * 32,
    )
    assert hook() is None
    assert request.body.tell() == 0
    with pytest.raises(PaidAbort) as replayed:
        hook()
    assert replayed.value.status == 401


def test_create_app_installs_media_patch_before_registering_route(monkeypatch):
    service = _fake_upstream_service(_stock_like_evil_logic)
    service.SESSIONS_STORAGE = FakeStorage()
    _pin_function_source(monkeypatch, service._evil_logic)
    _pin_lifecycle_sources(monkeypatch, service)
    monkeypatch.setattr(ext, "_install_safe_logging", lambda: None)

    class _App:
        def __init__(self):
            self.routes = []

        def get(self, path):
            def register(function):
                self.routes.append((path, function))
                return function

            return register

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
    assert app.routes[0][0] == "/v1/whoscored/runtime-identity"
    assert app.routes[0][1]() == {
        "status": "ok",
        "version": "3.4.6",
        "extension_sha256": ext.EXTENSION_SHA256,
    }
    assert app.routes[1][0] == "/v1/xhr"
    assert app.routes[2][0] == "/v1/xhr/batch"
    assert app.routes[3][0] == "/v1/whoscored/capacity-sessions/cleanup"
    assert isinstance(service.SESSIONS_STORAGE, ext._TrackingSessionsStorage)
    assert getattr(service.controller_v1_endpoint, ext._SAFE_CONTROLLER_MARKER) == (
        "3.4.6",
        ext._UPSTREAM_CONTROLLER_V1_SHA256,
    )
    assert getattr(service._evil_logic, ext._MEDIA_PATCH_MARKER) == (
        "3.4.6",
        ext._UPSTREAM_EVIL_LOGIC_SHA256,
    )


def test_main_installs_media_patch_before_upstream_browser_self_test():
    source = inspect.getsource(ext.main)

    assert ext.WAITRESS_THREADS == 8
    assert "threads=WAITRESS_THREADS" in source
    assert "threads=int(os.environ" not in source
    assert source.index("_paid_gateway_authorizer_from_environment()") < source.index(
        "test_browser_installation()"
    )
    assert source.index("_install_disable_media_extension(") < source.index(
        "test_browser_installation()"
    )
    assert source.index("_install_capacity_session_tracking(") < source.index(
        "test_browser_installation()"
    )
    assert source.index("_install_safe_v1_controller(") < source.index(
        "test_browser_installation()"
    )


def test_extension_identity_matches_current_helper_bytes():
    expected = hashlib.sha256(SCRIPT_PATH.read_bytes()).hexdigest()

    assert ext.EXTENSION_SHA256 == expected
    assert len(ext.EXTENSION_SHA256) == 64
    assert ext.EXTENSION_SHA256 == ext.EXTENSION_SHA256.lower()


def test_extension_identity_is_frozen_at_module_startup(tmp_path):
    copied_script = tmp_path / "flaresolverr_extended_frozen.py"
    original_bytes = SCRIPT_PATH.read_bytes()
    copied_script.write_bytes(original_bytes)
    module_name = "flaresolverr_extended_frozen_identity_test"
    spec = importlib.util.spec_from_file_location(module_name, copied_script)
    assert spec is not None and spec.loader is not None
    frozen = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = frozen
    try:
        spec.loader.exec_module(frozen)
    finally:
        sys.modules.pop(module_name, None)

    startup_hash = hashlib.sha256(original_bytes).hexdigest()
    assert frozen.EXTENSION_SHA256 == startup_hash
    copied_script.write_bytes(original_bytes + b"\n# changed after import\n")
    assert frozen.EXTENSION_SHA256 == startup_hash
    assert (
        frozen.EXTENSION_SHA256
        != hashlib.sha256(copied_script.read_bytes()).hexdigest()
    )


class LifecycleDriver:
    def __init__(self, quit_effects=None):
        self.quit_effects = list(quit_effects or [None])
        self.quit_started = threading.Event()
        self.quit_release = threading.Event()
        self.quit_release.set()
        self.quit_calls = 0

    def close(self):
        return None

    def quit(self):
        self.quit_calls += 1
        self.quit_started.set()
        assert self.quit_release.wait(timeout=2)
        effect = self.quit_effects.pop(0) if self.quit_effects else None
        if isinstance(effect, BaseException):
            raise effect


class LifecycleSession:
    def __init__(self, session_id, driver):
        self.session_id = session_id
        self.driver = driver

    def lifetime(self):
        return 0


class LifecycleStorage:
    def __init__(self, driver_factory=None):
        self.sessions = {}
        self.driver_factory = driver_factory or (lambda _proxy: LifecycleDriver())

    def create(self, session_id=None, proxy=None, force_new=False):
        if force_new:
            self.destroy(session_id)
        if session_id in self.sessions:
            return self.sessions[session_id], False
        driver = self.driver_factory(proxy)
        session = LifecycleSession(session_id, driver)
        self.sessions[session_id] = session
        return session, True

    def exists(self, session_id):
        return session_id in self.sessions

    def destroy(self, session_id):
        if session_id not in self.sessions:
            return False
        session = self.sessions.pop(session_id)
        session.driver.quit()
        return True

    def get(self, session_id, ttl=None):
        session, fresh = self.create(session_id)
        if ttl is not None and not fresh and session.lifetime() > ttl:
            return self.create(session_id, force_new=True)
        return session, fresh

    def session_ids(self):
        return list(self.sessions)


def _tracking_storage(delegate=None, *, execution_locks=None):
    return ext._TrackingSessionsStorage(
        delegate or LifecycleStorage(),
        platform_version_getter=lambda: "linux",
        execution_locks=execution_locks,
    )


def _wait_until(predicate, *, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true")


def test_capacity_blocked_create_is_pending_then_active_and_cleaned_async():
    owner = "a1b2c3d4e5f60718"
    session_id = f"ws-cap-{owner}-direct-1"
    create_started = threading.Event()
    create_release = threading.Event()
    driver = LifecycleDriver()

    def driver_factory(_proxy):
        create_started.set()
        assert create_release.wait(timeout=2)
        return driver

    storage = _tracking_storage(LifecycleStorage(driver_factory))
    create_thread = threading.Thread(target=storage.create, args=(session_id,))
    create_thread.start()
    assert create_started.wait(timeout=1)
    assert storage.owner_snapshot(owner) == {
        "active": 0,
        "pending_create": 1,
        "pending_destroy": 0,
        "failed_create": 0,
        "failed_destroy": 0,
        "failure_generation": 0,
        "cleanup_scheduled": False,
    }

    body, status = ext.handle_capacity_session_cleanup(
        {"owner": owner}, storage=storage
    )
    assert status == 200
    assert body["pending_create"] == 1
    create_release.set()
    create_thread.join(timeout=1)
    assert not create_thread.is_alive()
    assert storage.owner_snapshot(owner)["active"] == 1

    ext.handle_capacity_session_cleanup({"owner": owner}, storage=storage)
    _wait_until(lambda: storage.owner_snapshot(owner)["active"] == 0)
    assert driver.quit_calls == 1


def test_capacity_blocked_quit_stays_pending_without_blocking_handler():
    owner = "b1b2c3d4e5f60718"
    session_id = f"ws-cap-{owner}-direct-1"
    driver = LifecycleDriver()
    driver.quit_release.clear()
    storage = _tracking_storage(LifecycleStorage(lambda _proxy: driver))
    storage.create(session_id)

    started = time.monotonic()
    body, status = ext.handle_capacity_session_cleanup(
        {"owner": owner}, storage=storage
    )
    elapsed = time.monotonic() - started
    assert status == 200
    assert elapsed < 0.2
    assert body["cleanup_scheduled"] is True
    assert driver.quit_started.wait(timeout=1)
    assert storage.owner_snapshot(owner)["pending_destroy"] == 1
    assert session_id not in storage.sessions

    driver.quit_release.set()
    _wait_until(lambda: storage.owner_snapshot(owner)["pending_destroy"] == 0)


def test_capacity_cleanup_waits_for_active_paced_xhr_before_pop_and_quit():
    owner = "b9b8c7d6e5f40321"
    session_id = f"ws-cap-{owner}-direct-1"
    locks = ext._SessionLocks()
    driver = FakeDriver()
    driver.collect_release.clear()
    storage = _tracking_storage(
        LifecycleStorage(lambda _proxy: driver), execution_locks=locks
    )
    storage.create(session_id)
    request_result = []

    request_thread = threading.Thread(
        target=lambda: request_result.append(
            ext.handle_xhr_request(
                _payload(session=session_id, maxTimeout=5_000),
                storage=storage,
                version_getter=lambda: "3.4.6",
            )
        )
    )
    request_thread.start()
    assert driver.collect_started.wait(timeout=1)

    body, status = ext.handle_capacity_session_cleanup(
        {"owner": owner}, storage=storage
    )
    assert status == 200
    assert body["cleanup_scheduled"] is True
    time.sleep(0.05)
    assert session_id in storage.sessions
    assert driver.quit_calls == 0

    driver.collect_release.set()
    request_thread.join(timeout=2)
    assert not request_thread.is_alive()
    assert request_result[0][1] == 200
    assert driver.quit_started.wait(timeout=1)
    _wait_until(lambda: session_id not in storage.sessions)


def test_ordinary_session_destroy_waits_for_active_paced_xhr_lease():
    session_id = "ws-direct_flaresolverr-destroy-race"
    locks = ext._SessionLocks()
    driver = FakeDriver()
    driver.collect_release.clear()
    storage = _tracking_storage(
        LifecycleStorage(lambda _proxy: driver), execution_locks=locks
    )
    storage.create(session_id)
    request_thread = threading.Thread(
        target=ext.handle_xhr_request,
        kwargs={
            "payload": _payload(session=session_id, maxTimeout=5_000),
            "storage": storage,
            "version_getter": lambda: "3.4.6",
        },
    )
    request_thread.start()
    assert driver.collect_started.wait(timeout=1)

    destroyed = []
    destroy_thread = threading.Thread(
        target=lambda: destroyed.append(storage.destroy(session_id))
    )
    destroy_thread.start()
    time.sleep(0.05)
    assert destroy_thread.is_alive()
    assert session_id in storage.sessions
    assert driver.quit_calls == 0

    driver.collect_release.set()
    request_thread.join(timeout=2)
    destroy_thread.join(timeout=2)
    assert not request_thread.is_alive()
    assert not destroy_thread.is_alive()
    assert destroyed == [True]
    assert driver.quit_calls == 1


def test_ordinary_force_new_reuses_lifecycle_lock_without_deadlock():
    session_id = "ws-direct_flaresolverr-force-new"
    locks = ext._SessionLocks()
    old_driver = FakeDriver()
    new_driver = FakeDriver()
    drivers = iter([old_driver, new_driver])
    storage = _tracking_storage(
        LifecycleStorage(lambda _proxy: next(drivers)), execution_locks=locks
    )
    old_session, fresh = storage.create(session_id)
    assert fresh is True
    result = []

    thread = threading.Thread(
        target=lambda: result.append(storage.create(session_id, force_new=True))
    )
    thread.start()
    thread.join(timeout=1)

    assert not thread.is_alive()
    new_session, fresh = result[0]
    assert fresh is True
    assert new_session is not old_session
    assert old_driver.quit_calls == 1
    assert storage.sessions[session_id] is new_session


def test_capacity_failed_destroy_increments_generation_and_retries_later():
    owner = "c1b2c3d4e5f60718"
    session_id = f"ws-cap-{owner}-direct-1"
    driver = LifecycleDriver([RuntimeError("secret failure"), None])
    storage = _tracking_storage(LifecycleStorage(lambda _proxy: driver))
    storage.create(session_id)

    ext.handle_capacity_session_cleanup({"owner": owner}, storage=storage)
    _wait_until(
        lambda: (
            storage.owner_snapshot(owner)["failed_destroy"] == 1
            and storage.owner_snapshot(owner)["cleanup_scheduled"] is False
        )
    )
    failed = storage.owner_snapshot(owner)
    assert failed["failure_generation"] == 1
    assert failed["active"] == 0
    assert failed["pending_destroy"] == 0
    assert session_id not in storage.sessions

    ext.handle_capacity_session_cleanup({"owner": owner}, storage=storage)
    _wait_until(lambda: storage.owner_snapshot(owner)["failed_destroy"] == 0)
    assert driver.quit_calls == 2
    assert storage.owner_snapshot(owner)["failure_generation"] == 1


def test_cleanup_response_freezes_generation_before_instant_failed_retry(
    monkeypatch,
):
    owner = "c9b8a7d6e5f40321"
    session_id = f"ws-cap-{owner}-direct-1"
    driver = LifecycleDriver(
        [RuntimeError("stale failure"), RuntimeError("instant retry failure"), None]
    )
    storage = _tracking_storage(LifecycleStorage(lambda _proxy: driver))
    storage.create(session_id)
    with pytest.raises(RuntimeError, match="stale failure"):
        storage.destroy(session_id)
    assert storage.owner_snapshot(owner)["failure_generation"] == 1

    class InlineThread:
        def __init__(self, *, target, args, name, daemon):
            del name, daemon
            self.target = target
            self.args = args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(ext.threading, "Thread", InlineThread)

    baseline, status = ext.handle_capacity_session_cleanup(
        {"owner": owner}, storage=storage
    )
    assert status == 200
    assert baseline["failure_generation"] == 1
    assert baseline["failed_destroy"] == 1
    assert baseline["cleanup_scheduled"] is True
    # The inline worker already failed, but that new failure was not folded
    # into the response that triggered it.
    assert storage.owner_snapshot(owner)["failure_generation"] == 2

    observed, status = ext.handle_capacity_session_cleanup(
        {"owner": owner}, storage=storage
    )
    assert status == 200
    assert observed["failure_generation"] == 2
    assert observed["failed_destroy"] == 1
    assert observed["cleanup_scheduled"] is True
    assert storage.owner_snapshot(owner)["failed_destroy"] == 0


def test_capacity_failed_create_is_retained_as_fail_closed_evidence():
    owner = "d1b2c3d4e5f60718"
    session_id = f"ws-cap-{owner}-direct-1"

    def fail_create(_proxy):
        raise RuntimeError("driver failed")

    storage = _tracking_storage(LifecycleStorage(fail_create))
    with pytest.raises(RuntimeError, match="driver failed"):
        storage.create(session_id)
    assert storage.owner_snapshot(owner) == {
        "active": 0,
        "pending_create": 0,
        "pending_destroy": 0,
        "failed_create": 1,
        "failed_destroy": 0,
        "failure_generation": 1,
        "cleanup_scheduled": False,
    }


def test_capacity_cleanup_is_exact_owner_isolated():
    owner = "e1b2c3d4e5f60718"
    other_owner = "f1b2c3d4e5f60718"
    own_driver = LifecycleDriver()
    other_driver = LifecycleDriver()
    drivers = iter([own_driver, other_driver])
    storage = _tracking_storage(LifecycleStorage(lambda _proxy: next(drivers)))
    storage.create(f"ws-cap-{owner}-direct-1")
    storage.create(f"ws-cap-{other_owner}-direct-1")

    ext.handle_capacity_session_cleanup({"owner": owner}, storage=storage)
    _wait_until(lambda: storage.owner_snapshot(owner)["active"] == 0)

    assert own_driver.quit_calls == 1
    assert other_driver.quit_calls == 0
    assert storage.owner_snapshot(other_owner)["active"] == 1


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"owner": "a" * 15},
        {"owner": "A" * 16},
        {"owner": "a" * 33},
        {"owner": "a" * 16, "extra": True},
        {"owner": 123},
    ],
)
def test_capacity_cleanup_validation_and_response_are_exact_and_secret_free(payload):
    body, status = ext.handle_capacity_session_cleanup(
        payload, storage=_tracking_storage()
    )

    assert status == 400
    assert set(body) == ext._CAPACITY_CLEANUP_RESPONSE_FIELDS
    assert body == {
        "status": "error",
        "version": "3.4.6",
        "extension_sha256": ext.EXTENSION_SHA256,
        "active": 0,
        "pending_create": 0,
        "pending_destroy": 0,
        "failed_create": 0,
        "failed_destroy": 0,
        "failure_generation": 0,
        "cleanup_scheduled": False,
    }
    encoded = json.dumps(body)
    assert "owner" not in encoded
    assert "ws-cap" not in encoded


def test_capacity_cleanup_quiet_zero_is_accepted_and_scheduled():
    body, status = ext.handle_capacity_session_cleanup(
        {"owner": "a1b2c3d4e5f60718"}, storage=_tracking_storage()
    )

    assert status == 200
    assert set(body) == ext._CAPACITY_CLEANUP_RESPONSE_FIELDS
    assert body == {
        "status": "ok",
        "version": "3.4.6",
        "extension_sha256": ext.EXTENSION_SHA256,
        "active": 0,
        "pending_create": 0,
        "pending_destroy": 0,
        "failed_create": 0,
        "failed_destroy": 0,
        "failure_generation": 0,
        "cleanup_scheduled": True,
    }


def test_capacity_tracking_install_is_source_pinned_and_idempotent(monkeypatch):
    storage = LifecycleStorage()
    service = SimpleNamespace(
        SESSIONS_STORAGE=storage,
        utils=SimpleNamespace(PLATFORM_VERSION="linux"),
    )
    digest = hashlib.sha256(
        inspect.getsource(LifecycleStorage).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(ext, "_UPSTREAM_SESSIONS_STORAGE_SHA256", digest)

    installed = ext._install_capacity_session_tracking(service, version="3.4.6")
    assert installed is service.SESSIONS_STORAGE
    assert ext._install_capacity_session_tracking(service, version="3.4.6") is installed

    wrong = SimpleNamespace(
        SESSIONS_STORAGE=LifecycleStorage(),
        utils=SimpleNamespace(PLATFORM_VERSION="linux"),
    )
    monkeypatch.setattr(ext, "_UPSTREAM_SESSIONS_STORAGE_SHA256", "0" * 64)
    with pytest.raises(ext.CapacitySessionLifecycleError, match="does not match"):
        ext._install_capacity_session_tracking(wrong, version="3.4.6")
    with pytest.raises(ext.CapacitySessionLifecycleError, match="Unsupported"):
        ext._install_capacity_session_tracking(wrong, version="3.5.0")


def test_safe_controller_logs_no_session_owner_url_or_proxy_credentials(
    monkeypatch, caplog
):
    owner = "a9b8c7d6e5f40321"
    session_id = f"ws-cap-{owner}-direct-secret"
    source_url = 'https://www.whoscored.com/secret-source?token=quoted")\\source-tail'
    proxy_url = 'http://proxy.internal:8080/path?token=quoted")\\proxy-tail'
    proxy_user = 'unique"proxy\\user'
    proxy_password = "p'ass\"\\word"

    def handler(req):
        values = (
            req.session,
            req.url,
            req.proxy["url"],
            req.proxy["username"],
            req.proxy["password"],
        )
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR):
            logging.log(level, "unsafe %s %s %s %s %s", *values)
        logging.error(
            "unsafe json %s",
            json.dumps(
                {
                    "session": req.session,
                    "url": req.url,
                    "proxy": req.proxy,
                }
            ),
        )
        logging.error("unsafe repr %r", {"values": values})
        json_document = json.dumps(
            {"password": req.proxy["password"]}, ensure_ascii=True
        )
        logging.error("unsafe nested repr %r", json_document)
        logging.error("unsafe nested bytes %r", json_document.encode("utf-8"))
        try:
            raise RuntimeError(" ".join(values))
        except RuntimeError:
            logging.exception("unsafe exception %s", req.url)
        return FakeV1Response({})

    service = SimpleNamespace(
        controller_v1_endpoint=_stock_controller_v1,
        _controller_v1_handler=handler,
        V1ResponseBase=FakeV1Response,
        STATUS_ERROR="error",
        utils=SimpleNamespace(get_flaresolverr_version=lambda: "3.4.6"),
    )
    digest = hashlib.sha256(
        inspect.getsource(service.controller_v1_endpoint).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(ext, "_UPSTREAM_CONTROLLER_V1_SHA256", digest)
    old_factory = logging.getLogRecordFactory()
    try:
        ext._install_safe_logging()
        ext._install_safe_v1_controller(service, version="3.4.6")
        with caplog.at_level(logging.DEBUG):
            response = service.controller_v1_endpoint(
                SimpleNamespace(
                    session=session_id,
                    url=source_url,
                    proxy={
                        "url": proxy_url,
                        "username": proxy_user,
                        "password": proxy_password,
                    },
                )
            )
        assert response.version == "3.4.6"
        assert response.extension_sha256 == ext.EXTENSION_SHA256
        logs = "\n".join(record.getMessage() for record in caplog.records)
        for secret in (
            session_id,
            owner,
            source_url,
            proxy_url,
            proxy_user,
            proxy_password,
        ):
            for variant in ext._sensitive_log_variants(secret):
                assert variant not in logs
        assert "POST /v1 body" not in logs
        assert ext._SENSITIVE_LOG_VALUES.snapshot() == ()
    finally:
        logging.setLogRecordFactory(old_factory)


def test_fixed_log_redaction_does_not_need_per_request_url_or_ws_registry():
    session_id = "ws-cap-1234567890abcdef-direct-never-registered"
    source_url = (
        'https://www.whoscored.com/path?secret=value")\\still-secret-after-quote'
    )

    redacted = ext._redact_log_text(f"session={session_id} url={source_url}")

    assert session_id not in redacted
    assert source_url not in redacted
    assert "never-registered" not in redacted
    assert "secret=value" not in redacted
    assert "still-secret-after-quote" not in redacted


def test_sensitive_variants_cover_nested_json_repr_and_bytes_with_fixed_bound():
    secret = "p'ass\"\\word"
    json_inner = json.dumps(secret, ensure_ascii=True)[1:-1]
    variants = ext._sensitive_log_variants(secret)

    assert secret in variants
    assert json_inner in variants
    assert repr(json_inner) in variants
    assert repr(json_inner)[1:-1] in variants
    assert repr(json_inner.encode("utf-8")) in variants
    assert repr(json_inner.encode("utf-8"))[2:-1] in variants
    assert len(variants) <= ext._MAX_SENSITIVE_VARIANTS_PER_VALUE == 64

    document = json.dumps({"password": secret}, ensure_ascii=True)
    registry = ext._SensitiveLogValues()
    with registry.scope(secret):
        assert secret not in registry.redact(repr(document))
        assert secret not in registry.redact(repr(document.encode("utf-8")))
        assert "p\\'ass" not in registry.redact(repr(document))
        assert "p\\'ass" not in registry.redact(repr(document.encode("utf-8")))
    assert registry.snapshot() == ()


def test_sensitive_log_scope_refcounts_concurrent_requests_and_removes_values():
    registry = ext._SensitiveLogValues()
    secret = 'same"secret\\for-both'
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]

    def hold_scope(index):
        with registry.scope(secret):
            entered[index].set()
            assert release[index].wait(timeout=2)

    threads = [threading.Thread(target=hold_scope, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    assert all(event.wait(timeout=1) for event in entered)
    assert ext._sensitive_log_variants(secret) <= set(registry.snapshot())

    release[0].set()
    threads[0].join(timeout=1)
    assert not threads[0].is_alive()
    assert ext._sensitive_log_variants(secret) <= set(registry.snapshot())

    release[1].set()
    threads[1].join(timeout=1)
    assert not threads[1].is_alive()
    assert registry.snapshot() == ()

    with pytest.raises(RuntimeError):
        with registry.scope(secret):
            raise RuntimeError("stop")
    assert registry.snapshot() == ()


@pytest.mark.parametrize(
    "url",
    [
        BASE_URL,
        "https://www.whoscored.com/stagestatfeed/23752/stageteams/?type=2",
    ],
)
def test_url_allowlist_accepts_only_active_structured_feed_prefixes(url):
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
        "https://www.whoscored.com/stageplayerstatfeed/23752/playerstats/?page=1",
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
        ("minimumStartIntervalMs", 0),
        ("startNotBeforeEpochMs", 0),
        ("executionMarginMs", 0),
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
        ("minimumStartIntervalMs", 0),
        ("startNotBeforeEpochMs", 0),
        ("executionMarginMs", 0),
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
    assert "stageplayerstatfeed" not in script
    assert "response_too_large" in script
    assert "deadlineEpochMs - Date.now() < minimumExecutionMarginMs" in script
    assert "responsePromise = fetch(requested.href" in script
    assert "startNotBeforeEpochMs" not in script
    assert "minimumStartIntervalMs" not in script
    assert "eval(" not in script
    assert "new Function" not in script
    assert BASE_URL not in script


def test_fixed_batch_script_has_server_side_security_and_resource_limits():
    script = ext.BATCH_XHR_SCRIPT
    assert script == ext.XHR_SCRIPT
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
    assert "maxBytesPerResponse" in script
    assert "maxAggregateBytes" in script
    assert "consumedBytes: 0" in script
    assert "operation.consumedBytes += item.value.byteLength" in script
    assert "operation.consumedBytes -=" not in script
    assert 'failure("aggregate_too_large")' not in script
    assert 'kind: "aggregate_too_large"' in ext.XHR_COLLECT_SCRIPT
    assert "for (const activeController of operation.controllers)" in script
    assert "range(0, len(request_data.urls), BATCH_CONCURRENCY)" in inspect.getsource(
        ext._execute_browser_batch_fetch
    )
    assert "eval(" not in script
    assert "new Function" not in script
    assert BASE_URL not in script


class FakeMonotonicClock:
    def __init__(self, now):
        self.now = now

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_process_wide_xhr_pacer_has_one_immediate_start_and_no_burst_credit():
    clock = FakeMonotonicClock(10.0)
    pacer = ext._XhrStartPacer(monotonic=clock.monotonic, sleep=clock.sleep)
    starts = []

    def start():
        starts.append(clock.monotonic())
        return len(starts)

    assert pacer.launch(deadline=20.0, starter=start) == 1
    assert pacer.launch(deadline=20.0, starter=start) == 2
    clock.now = 100.0
    assert pacer.launch(deadline=110.0, starter=start) == 3
    assert pacer.launch(deadline=110.0, starter=start) == 4

    assert starts == [10.0, 10.546, 100.0, 100.546]


def test_process_wide_xhr_pacer_arbitrates_delayed_independent_commands():
    interval_ms = 25
    pacer = ext._XhrStartPacer(
        interval_ms=interval_ms,
        execution_margin_ms=10,
    )
    actual_starts = []
    guard = threading.Lock()

    def delayed_command(delay):
        def start():
            time.sleep(delay)
            with guard:
                actual_starts.append(time.monotonic())
            # Keep the command in flight after its real fetch invocation.
            time.sleep(delay)

        pacer.launch(deadline=time.monotonic() + 2, starter=start)

    threads = [
        threading.Thread(target=delayed_command, args=(0.06,)),
        threading.Thread(target=delayed_command, args=(0.01,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    ordered = sorted(actual_starts)
    assert len(ordered) == 2
    assert ordered[1] - ordered[0] >= (interval_ms / 1_000.0)


def test_process_wide_xhr_pacer_rejects_deadline_without_starting_or_consuming():
    clock = FakeMonotonicClock(30.0)
    pacer = ext._XhrStartPacer(monotonic=clock.monotonic, sleep=clock.sleep)
    starts = []

    def starter():
        starts.append(clock.monotonic())

    pacer.launch(deadline=40.0, starter=starter)

    with pytest.raises(ext.XhrEndpointError, match="global WhoScored source pace"):
        pacer.launch(deadline=30.9, starter=starter)

    assert starts == [30.0]
    pacer.launch(deadline=32.0, starter=starter)
    assert starts == [30.0, 30.546]


@pytest.mark.parametrize("interval", [True, 0, -1, 1.5])
def test_process_wide_xhr_pacer_rejects_invalid_fixed_policy(interval):
    with pytest.raises(ValueError):
        ext._XhrStartPacer(interval_ms=interval)


@pytest.mark.parametrize("margin", [True, 0, -1, 1.5])
def test_process_wide_xhr_pacer_rejects_invalid_execution_margin(margin):
    with pytest.raises(ValueError):
        ext._XhrStartPacer(execution_margin_ms=margin)


def test_delayed_independent_single_and_batch_commands_cannot_burst():
    interval_ms = 35
    pacer = ext._XhrStartPacer(
        interval_ms=interval_ms,
        execution_margin_ms=10,
    )
    locks = ext._SessionLocks()
    single_session = "ws-direct_flaresolverr-independent-single"
    batch_session = "ws-direct_flaresolverr-independent-batch"
    single_driver = FakeDriver(launch_delay=0.08)
    batch_driver = FakeDriver(launch_delay=0.03)
    storage = FakeStorage(
        {
            single_session: SimpleNamespace(driver=single_driver),
            batch_session: SimpleNamespace(driver=batch_driver),
        }
    )
    responses = []

    threads = [
        threading.Thread(
            target=lambda: responses.append(
                ext.handle_xhr_request(
                    _payload(session=single_session, maxTimeout=5_000),
                    storage=storage,
                    version_getter=lambda: "3.4.6",
                    locks=locks,
                    pacer=pacer,
                )
            )
        ),
        threading.Thread(
            target=lambda: responses.append(
                ext.handle_xhr_batch_request(
                    _batch_payload(session=batch_session, maxTimeout=5_000),
                    storage=storage,
                    version_getter=lambda: "3.4.6",
                    locks=locks,
                    pacer=pacer,
                )
            )
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert len(responses) == 2
    assert all(status == 200 for _body, status in responses)
    actual_starts = sorted(single_driver.actual_starts + batch_driver.actual_starts)
    assert len(actual_starts) == 2
    assert actual_starts[1] - actual_starts[0] >= interval_ms / 1_000.0


def test_deadline_without_safe_execution_margin_is_504_before_browser_start():
    pacer = ext._XhrStartPacer()
    locks = ext._SessionLocks()
    first_session = "ws-direct_flaresolverr-deadline-first"
    late_session = "ws-direct_flaresolverr-deadline-late"
    first_driver = FakeDriver()
    late_driver = FakeDriver()
    storage = FakeStorage(
        {
            first_session: SimpleNamespace(driver=first_driver),
            late_session: SimpleNamespace(driver=late_driver),
        }
    )

    first_body, first_status = ext.handle_xhr_request(
        _payload(session=first_session, maxTimeout=5_000),
        storage=storage,
        version_getter=lambda: "3.4.6",
        locks=locks,
        pacer=pacer,
    )
    late_body, late_status = ext.handle_xhr_request(
        _payload(session=late_session, maxTimeout=1_000),
        storage=storage,
        version_getter=lambda: "3.4.6",
        locks=locks,
        pacer=pacer,
    )

    assert first_status == 200
    assert first_body["status"] == "ok"
    assert late_status == 504
    assert late_body["status"] == "error"
    assert "global WhoScored source pace" in late_body["message"]
    assert late_driver.launch_calls == []
    assert late_driver.actual_starts == []


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
    assert body["extension_sha256"] == ext.EXTENSION_SHA256
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
    (
        script,
        url,
        response_limit,
        aggregate_limit,
        deadline_epoch_ms,
        execution_margin_ms,
        operation_key,
        item_index,
    ) = driver.launch_calls[0]
    assert script == ext.XHR_SCRIPT
    assert url == BASE_URL
    assert response_limit == 4 * 1024 * 1024
    assert aggregate_limit == response_limit
    assert deadline_epoch_ms > int(time.time() * 1_000)
    assert execution_margin_ms == ext.XHR_MIN_EXECUTION_MARGIN_MS == 500
    assert operation_key.startswith("xhr-")
    assert item_index == 0
    assert driver.execute_calls[0][0] == ext.XHR_COLLECT_SCRIPT
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
    assert len(driver.launch_calls) == 2
    assert [call[1] for call in driver.launch_calls] == [BASE_URL, second_url]
    assert all(call[0] == ext.BATCH_XHR_SCRIPT for call in driver.launch_calls)
    assert all(call[2] == 4 * 1024 * 1024 for call in driver.launch_calls)
    assert all(call[3] == 8 * 1024 * 1024 for call in driver.launch_calls)
    assert [call[-1] for call in driver.launch_calls] == [0, 1]
    assert driver.actual_starts[1] - driver.actual_starts[0] >= 0.54
    assert driver.execute_calls[0][0] == ext.XHR_COLLECT_SCRIPT
    assert driver.execute_calls[0][2] == [0, 1]


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
    assert body["version"] == "3.4.6"
    assert body["extension_sha256"] == ext.EXTENSION_SHA256
    assert "solution" not in body


def test_batch_aggregate_limit_is_global_monotonic_and_exposes_no_bodies():
    """Eight failed items cannot each consume a fresh per-item allowance."""

    script = ext.BATCH_XHR_SCRIPT
    assert "operation.consumedBytes += item.value.byteLength" in script
    assert "consumedBytes = Math.max" not in script
    assert "aggregateBytes" not in script
    assert "if (operation.consumedBytes > maxAggregateBytes)" in script

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


def test_batch_aggregate_limit_aborts_adversarial_wave_in_node():
    """Execute the shipped JS: failed items cannot reset the global byte cap."""

    node = shutil.which("node")
    assert node is not None, "Node.js is required for the browser-script safety test"
    launch_script = json.dumps(ext.BATCH_XHR_SCRIPT)
    collect_script = json.dumps(ext.XHR_COLLECT_SCRIPT)
    harness = f"""
const launch = new Function({launch_script});
const collect = new Function({collect_script});
const MiB = 1024 * 1024;
const urls = Array.from({{length: 4}}, (_, index) =>
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
const operationKey = "xhr-" + "a".repeat(32);
const deadline = Date.now() + 3000;
const acknowledgements = urls.map((url, index) => launch(
  url,
  4 * MiB,
  8 * MiB,
  deadline,
  {ext.XHR_MIN_EXECUTION_MARGIN_MS},
  operationKey,
  index
));
const terminal = await new Promise((resolve, reject) => {{
  const timer = setTimeout(
    () => reject(new Error("batch callback timeout")),
    5000
  );
  collect(
    operationKey,
    [0, 1, 2, 3],
    true,
    (result) => {{
      clearTimeout(timer);
      resolve(result);
    }}
  );
}});
process.stdout.write(JSON.stringify({{
  acknowledgements,
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
    assert all(item["ok"] is True for item in observed["acknowledgements"])
    assert "responses" not in observed["terminal"]
    assert ext.MAX_BATCH_RESPONSE_BYTES < observed["observedBytes"]
    assert observed["observedBytes"] <= (
        ext.MAX_BATCH_RESPONSE_BYTES + observed["chunkBytes"]
    )
    assert observed["fetchStarts"] <= ext.BATCH_CONCURRENCY
    assert observed["maxActiveStreams"] <= ext.BATCH_CONCURRENCY
    assert observed["activeStreams"] == 0


def test_shipped_launch_script_starts_synchronously_and_rejects_stale_deadline():
    """The fixed JS has no stale timer between arbitration and ``fetch``."""

    node = shutil.which("node")
    assert node is not None, "Node.js is required for the browser pacing test"
    launch_script = json.dumps(ext.XHR_SCRIPT)
    abort_script = json.dumps(ext.XHR_ABORT_SCRIPT)
    harness = f"""
const launch = new Function({launch_script});
const abort = new Function({abort_script});
global.window = {{require: {{config: {{params: {{site: {{
  gSiteHeaderName: "Model-last-Mode",
  gSiteHeaderValue: "A".repeat(43) + "="
}}}}}}}}}};
const starts = [];
global.fetch = async (url) => {{
  starts.push(Date.now());
  return {{
    url,
    status: 200,
    headers: {{entries: () => []}},
    body: {{getReader: () => ({{
      read: async () => ({{done: true}}),
      releaseLock: () => {{}}
    }})}}
  }};
}};
const operationKey = "xhr-" + "b".repeat(32);
const ack = launch(
  "https://www.whoscored.com/statisticsfeed/1/getteamstatistics?item=0",
  1024,
  4096,
  Date.now() + 2000,
  {ext.XHR_MIN_EXECUTION_MARGIN_MS},
  operationKey,
  0
);
const startsAtAck = starts.length;
const stale = launch(
  "https://www.whoscored.com/statisticsfeed/1/getteamstatistics?item=1",
  1024,
  4096,
  Date.now() + {ext.XHR_MIN_EXECUTION_MARGIN_MS - 1},
  {ext.XHR_MIN_EXECUTION_MARGIN_MS},
  "xhr-" + "c".repeat(32),
  0
);
abort(operationKey);
process.stdout.write(JSON.stringify({{ack, stale, starts, startsAtAck}}));
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

    assert observed["ack"] == {"ok": True, "started": True, "itemIndex": 0}
    assert observed["startsAtAck"] == 1
    assert len(observed["starts"]) == 1
    assert observed["stale"] == {
        "ok": False,
        "started": False,
        "kind": "timeout",
        "error": "fetch_timeout",
    }


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
    pacer = ext._XhrStartPacer(interval_ms=5, execution_margin_ms=10)
    results = []

    def run_request():
        results.append(
            ext.handle_xhr_request(
                _payload(maxTimeout=5_000),
                storage=storage,
                version_getter=lambda: "3.4.6",
                locks=locks,
                pacer=pacer,
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
