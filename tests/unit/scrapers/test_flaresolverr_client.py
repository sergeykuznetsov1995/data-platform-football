"""Unit tests for FlareSolverrClient (scrapers/base/flaresolverr_client.py)."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from scrapers.base.flaresolverr_client import (
    _MAX_SENSITIVE_VARIANTS_PER_VALUE,
    _escaped_sensitive_variants,
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrResponseTooLarge,
    FlareSolverrRuntimeIdentityError,
    FlareSolverrTimeout,
    describe_proxy_mode,
    is_chromium_error_page,
)


# -----------------------------------------------------------------------------
# describe_proxy_mode (issue #616 — proxy mode visibility)
# -----------------------------------------------------------------------------


class TestDescribeProxyMode:
    def test_none_is_proxy_less(self):
        assert describe_proxy_mode(None).startswith("PROXY-LESS")

    def test_empty_string_is_proxy_less(self):
        assert describe_proxy_mode("").startswith("PROXY-LESS")

    def test_filter_url_named_as_filter(self):
        assert "filter" in describe_proxy_mode("http://proxy_filter:8899")

    def test_residential_url_named_without_leaking_creds(self):
        url = "http://user:secret@residential.example.com:8080"
        desc = describe_proxy_mode(url)
        assert desc == "via residential proxy"
        assert "secret" not in desc


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _ok_response(json_payload: dict, status_code: int = 200) -> MagicMock:
    """Build a MagicMock that mimics a successful requests.Response."""
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.json.return_value = json_payload
    resp.text = ""
    resp.raise_for_status = MagicMock()
    return resp


def _identity_response(
    *, version: str = "3.4.6", extension_sha256: str = "a" * 64
) -> MagicMock:
    return _ok_response(
        {
            "status": "ok",
            "version": version,
            "extension_sha256": extension_sha256,
        }
    )


def _ok_solution_payload() -> dict:
    return {
        "status": "ok",
        "message": "",
        "solution": {
            "response": "<html>hello</html>",
            "cookies": [{"name": "cf_clearance", "value": "abc"}],
            "userAgent": "Mozilla/5.0",
            "status": 200,
        },
    }


# -----------------------------------------------------------------------------
# request.get tests
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrGet:
    @pytest.mark.parametrize(
        ("version", "extension_sha256"),
        [
            (None, None),
            ("3.4.5", "a" * 64),
            ("3.4.6", None),
            ("3.4.6", "b" * 64),
        ],
    )
    def test_expected_runtime_identity_rejects_stale_or_missing_v1_receipt(
        self, version, extension_sha256
    ):
        client = FlareSolverrClient(
            expected_version="3.4.6",
            expected_extension_sha256="a" * 64,
        )
        payload = {
            **_ok_solution_payload(),
            "version": version,
            "extension_sha256": extension_sha256,
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.return_value = _identity_response()
            sess.post.return_value = _ok_response(payload)
            with pytest.raises(FlareSolverrRuntimeIdentityError):
                client.get("https://example.com", "ws-direct-1")

        stats = client.get_traffic_stats()
        assert stats["fs_response_bytes"] == 0
        assert stats["requests"] == 0

    def test_sensitive_variants_cover_nested_json_repr_and_bytes_with_fixed_bound(
        self,
    ):
        secret = "p'ass\"\\word"
        json_inner = json.dumps(secret, ensure_ascii=True)[1:-1]
        variants = _escaped_sensitive_variants(secret)

        assert secret in variants
        assert json_inner in variants
        assert repr(json_inner) in variants
        assert repr(json_inner)[1:-1] in variants
        assert repr(json_inner.encode("utf-8")) in variants
        assert repr(json_inner.encode("utf-8"))[2:-1] in variants
        assert len(variants) <= _MAX_SENSITIVE_VARIANTS_PER_VALUE == 64

    def test_get_happy_path(self):
        client = FlareSolverrClient(url="http://fs:8191")
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())

            out = client.get("https://example.com", session_id="s1")

        assert out["html"] == "<html>hello</html>"
        assert out["cookies"] == [{"name": "cf_clearance", "value": "abc"}]
        assert out["userAgent"] == "Mozilla/5.0"
        assert out["status"] == 200

        sess.post.assert_called_once()
        call_args = sess.post.call_args
        assert call_args.args[0] == "http://fs:8191/v1"
        payload = call_args.kwargs["json"]
        assert payload["cmd"] == "request.get"
        assert payload["url"] == "https://example.com"
        assert payload["session"] == "s1"
        assert payload["maxTimeout"] == 60_000
        assert "returnOnlyCookies" not in payload

    def test_get_returns_only_cookies(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get("https://x.com", "s1", return_only_cookies=True)

        payload = sess.post.call_args.kwargs["json"]
        assert payload["returnOnlyCookies"] is True

    def test_get_can_disable_media_per_navigation(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get("https://x.com", "s1", disable_media=True)

        payload = sess.post.call_args.kwargs["json"]
        assert payload["disableMedia"] is True

    def test_post_commands_are_not_retried_by_http_adapter(self):
        client = FlareSolverrClient()

        retries = client.session.get_adapter("http://").max_retries

        assert retries.total == 2
        assert retries.allowed_methods == frozenset({"GET"})
        assert client.session.trust_env is False

    def test_debug_log_identifies_operation_without_endpoint_url(self, caplog):
        endpoint_base = "http://endpoint-user:endpoint-secret@fs.internal:8191"
        client = FlareSolverrClient(url=endpoint_base)
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok", "sessions": []})
            with caplog.at_level("DEBUG"):
                assert client.list_sessions() == []

        assert endpoint_base not in caplog.text
        assert "endpoint-secret" not in caplog.text
        assert "endpoint_type=api" in caplog.text
        assert "cmd=sessions.list" in caplog.text

    def test_transport_error_redacts_endpoint_source_and_session_without_context(
        self, caplog
    ):
        endpoint_base = "http://endpoint-user:endpoint-secret@fs.internal:8191"
        source_url = 'https://source.invalid/a"quoted\\path?token=source-secret'
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        endpoint = f"{endpoint_base}/v1"
        failure = requests.exceptions.ConnectionError(
            f"failed endpoint={endpoint!r} source={source_url!r} session={session_id}"
        )
        client = FlareSolverrClient(url=endpoint_base)
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = failure
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrTimeout) as raised:
                    client.get(source_url, session_id)

        diagnostic = caplog.text + str(raised.value)
        for sensitive in (
            endpoint_base,
            endpoint,
            "endpoint-secret",
            source_url,
            repr(source_url)[1:-1],
            "source-secret",
            session_id,
        ):
            assert sensitive not in diagnostic
        assert "error_type=ConnectionError" in caplog.text
        assert "cmd=request.get" in caplog.text
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert raised.value.__suppress_context__ is True

    def test_batch_transport_error_redacts_every_source_url(self, caplog):
        urls = [
            "https://source.invalid/one?token=batch-secret-one",
            'https://source.invalid/two"quoted\\path?token=batch-secret-two',
        ]
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        client = FlareSolverrClient(url="http://private-fs.invalid:8191")
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.RequestException(
                f"batch failed: {urls!r}"
            )
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrError) as raised:
                    client.xhr_get_many(urls, session_id)

        diagnostic = caplog.text + str(raised.value)
        for url in urls:
            assert url not in diagnostic
            assert repr(url)[1:-1] not in diagnostic
        assert "batch-secret-one" not in diagnostic
        assert "batch-secret-two" not in diagnostic
        assert "endpoint_type=xhr_batch" in caplog.text
        assert "error_type=RequestException" in caplog.text
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    @pytest.mark.parametrize("as_bytes", [False, True])
    def test_transport_error_redacts_repr_of_json_document_and_bytes(
        self, caplog, as_bytes
    ):
        secret = "p'ass\"\\word"
        payload = {
            "cmd": "sessions.create",
            "session": "ws-cap-a1b2c3d4e5f60718-direct-nested",
            "proxy": {
                "url": "http://proxy_filter:8900",
                "username": "lease",
                "password": secret,
            },
        }
        document = json.dumps(payload, ensure_ascii=True)
        rendered = repr(document.encode("utf-8") if as_bytes else document)
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError(
                f"nested diagnostic={rendered}"
            )
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrTimeout) as raised:
                    client._post(payload)

        diagnostic = caplog.text + str(raised.value)
        assert secret not in diagnostic
        assert "p\\'ass" not in diagnostic
        for variant in _escaped_sensitive_variants(secret):
            assert variant not in diagnostic
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    def test_non_json_error_redacts_values_and_drops_decoder_context(self):
        endpoint_base = "http://private-fs.invalid:8191"
        source_url = "https://source.invalid/page?token=non-json-secret"
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        response = _ok_response({"status": "ok"})
        response.json.side_effect = ValueError(
            f"invalid response for {endpoint_base}/v1 {source_url} {session_id}"
        )
        client = FlareSolverrClient(url=endpoint_base)
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response
            with pytest.raises(FlareSolverrError) as raised:
                client.get(source_url, session_id)

        diagnostic = str(raised.value)
        assert endpoint_base not in diagnostic
        assert source_url not in diagnostic
        assert "non-json-secret" not in diagnostic
        assert session_id not in diagnostic
        assert "ValueError" in diagnostic
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    def test_xhr_get_uses_restricted_endpoint_and_decodes_exact_body(self):
        client = FlareSolverrClient(url="http://fs:8191")
        body = b'{"teamTableStats":[]}'
        payload = {
            "status": "ok",
            "solution": {
                "responseBase64": base64.b64encode(body).decode(),
                "responseBytes": len(body),
                "headers": {"content-type": "application/json"},
                "finalUrl": "https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
                "status": 200,
            },
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.return_value = _identity_response()
            sess.post.return_value = _ok_response(payload)
            out = client.xhr_get(
                "https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
                "ws-direct-1",
                max_timeout_ms=30_000,
            )

        assert out["content"] == body
        assert out["responseBytes"] == len(body)
        assert out["status"] == 200
        call = sess.post.call_args
        assert call.args[0] == "http://fs:8191/v1/xhr"
        assert call.kwargs["json"] == {
            "url": "https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
            "session": "ws-direct-1",
            "maxTimeout": 30_000,
        }

    @pytest.mark.parametrize(
        "mutation",
        [
            {"version": "3.4.5"},
            {"extension_sha256": "b" * 64},
            {"extension_sha256": None},
            {"unexpected": True},
        ],
    )
    def test_xhr_identity_is_exact_and_checked_before_bytes_or_body(self, mutation):
        expected_hash = "a" * 64
        client = FlareSolverrClient(
            expected_version="3.4.6",
            expected_extension_sha256=expected_hash,
        )
        source_url = "https://www.whoscored.com/statisticsfeed/1/x"
        body = b'{"secret":"must-not-be-exposed"}'
        payload = {
            "status": "ok",
            "message": "completed",
            "solution": {
                "responseBase64": base64.b64encode(body).decode(),
                "responseBytes": len(body),
                "headers": {},
                "finalUrl": source_url,
                "status": 200,
            },
            "startTimestamp": 100,
            "endTimestamp": 101,
            "version": "3.4.6",
            "extension_sha256": expected_hash,
            **mutation,
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(payload)
            with pytest.raises(FlareSolverrRuntimeIdentityError):
                client.xhr_get(source_url, "ws-direct-1")

        stats = client.get_traffic_stats()
        assert stats["fs_response_bytes"] == 0
        assert stats["requests"] == 0

    def test_exact_xhr_runtime_identity_allows_body_and_accounting(self):
        expected_hash = "a" * 64
        client = FlareSolverrClient(
            expected_version="3.4.6",
            expected_extension_sha256=expected_hash,
        )
        source_url = "https://www.whoscored.com/statisticsfeed/1/x"
        body = b'{"rows":[]}'
        payload = {
            "status": "ok",
            "message": "completed",
            "solution": {
                "responseBase64": base64.b64encode(body).decode(),
                "responseBytes": len(body),
                "headers": {"content-type": "application/json"},
                "finalUrl": source_url,
                "status": 200,
            },
            "startTimestamp": 100,
            "endTimestamp": 101,
            "version": "3.4.6",
            "extension_sha256": expected_hash,
        }
        response = _ok_response(payload)
        response.content = b"identity-envelope"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.return_value = _identity_response()
            sess.post.return_value = response
            result = client.xhr_get(source_url, "ws-direct-1")

        assert result["content"] == body
        stats = client.get_traffic_stats()
        assert stats["fs_response_bytes"] == len(response.content)
        assert stats["requests"] == 1

    def test_xhr_http_error_requires_identity_before_status_handling(self):
        client = FlareSolverrClient(
            expected_version="3.4.6",
            expected_extension_sha256="a" * 64,
        )
        payload = {
            "status": "error",
            "message": "rejected",
            "startTimestamp": 100,
            "endTimestamp": 101,
            "version": "3.4.6",
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.return_value = _identity_response()
            sess.post.return_value = _ok_response(payload, status_code=400)
            with pytest.raises(FlareSolverrRuntimeIdentityError):
                client.xhr_get(
                    "https://www.whoscored.com/statisticsfeed/1/x", "ws-direct-1"
                )

        assert client.get_traffic_stats()["fs_response_bytes"] == 0

    @pytest.mark.parametrize(
        "identity_response",
        [
            _identity_response(version="3.4.5"),
            _identity_response(extension_sha256="b" * 64),
            _ok_response({"status": "ok", "version": "3.4.6"}),
            _ok_response({}, status_code=404),
        ],
    )
    def test_preflight_rejects_stale_runtime_before_post(self, identity_response):
        client = FlareSolverrClient(
            expected_version="3.4.6",
            expected_extension_sha256="a" * 64,
        )
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.return_value = identity_response
            with pytest.raises(FlareSolverrRuntimeIdentityError):
                client.create_session("ws-direct-1")

        sess.post.assert_not_called()
        assert client.get_traffic_stats()["sessions_created"] == 0

    def test_xhr_get_rejects_mismatched_body_size(self):
        client = FlareSolverrClient()
        payload = {
            "status": "ok",
            "solution": {
                "responseBase64": base64.b64encode(b"{}").decode(),
                "responseBytes": 3,
                "headers": {},
                "finalUrl": "https://www.whoscored.com/statisticsfeed/1/x",
                "status": 200,
            },
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(payload)
            with pytest.raises(FlareSolverrError, match="byte count"):
                client.xhr_get(
                    "https://www.whoscored.com/statisticsfeed/1/x", "ws-direct-1"
                )

    def test_xhr_get_rejects_allowlisted_but_different_final_url(self):
        client = FlareSolverrClient()
        requested = "https://www.whoscored.com/statisticsfeed/1/getteamstatistics?a=1"
        payload = {
            "status": "ok",
            "solution": {
                "responseBase64": base64.b64encode(b"{}").decode(),
                "responseBytes": 2,
                "headers": {},
                "finalUrl": requested + "&different=1",
                "status": 200,
            },
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(payload)

            with pytest.raises(FlareSolverrError, match="final URL"):
                client.xhr_get(requested, "ws-direct-1")

    def test_xhr_get_many_preserves_success_and_runtime_failure_per_url(self):
        client = FlareSolverrClient(url="http://fs:8191")
        first = "https://www.whoscored.com/statisticsfeed/1/a"
        second = "https://www.whoscored.com/statisticsfeed/1/b"
        body = b'{"rows":[]}'
        payload = {
            "status": "ok",
            "solution": {
                "responses": [
                    {
                        "ok": True,
                        "requestedUrl": first,
                        "responseBase64": base64.b64encode(body).decode(),
                        "responseBytes": len(body),
                        "headers": {"content-type": "application/json"},
                        "finalUrl": first,
                        "status": 200,
                    },
                    {
                        "ok": False,
                        "requestedUrl": second,
                        "kind": "fetch_failed",
                    },
                ],
                "responseBytes": len(body),
            },
        }
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(payload)
            out = client.xhr_get_many(
                [first, second], "ws-direct-1", max_timeout_ms=30_000
            )

        assert out[0]["ok"] is True
        assert out[0]["content"] == body
        assert out[1] == {
            "ok": False,
            "kind": "fetch_failed",
            "responseBytes": 0,
        }
        call = sess.post.call_args
        assert call.args[0] == "http://fs:8191/v1/xhr/batch"
        assert call.kwargs["json"] == {
            "urls": [first, second],
            "session": "ws-direct-1",
            "maxTimeout": 30_000,
        }
        assert client.get_traffic_stats()["requests"] == 2

    @pytest.mark.parametrize(
        "urls",
        [
            [],
            ["https://www.whoscored.com/statisticsfeed/1/x"] * 2,
            [str(i) for i in range(9)],
        ],
    )
    def test_xhr_get_many_enforces_count_and_uniqueness_before_http(self, urls):
        client = FlareSolverrClient()
        with pytest.raises(ValueError):
            client.xhr_get_many(urls, "ws-direct-1")

    @pytest.mark.parametrize(
        "message",
        [
            "Cloudflare challenge could not be solved",
            "Turnstile timed out",
            "Failed to bypass challenge",
            "CLOUDFLARE error",
            "cloudflare detected",
        ],
    )
    def test_get_raises_cf_challenge(self, message):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {"status": "error", "message": message}
            )
            with pytest.raises(FlareSolverrCFChallengeFailed):
                client.get("https://x.com", "s1")

    @pytest.mark.parametrize("status_code", [200, 500])
    def test_generic_error_solving_challenge_is_not_cf(self, caplog, status_code):
        client = FlareSolverrClient()
        source_url = "https://source.invalid/cloudflare-status?token=internal-secret"
        session_id = "ws-cap-a1b2c3d4e5f60718-turnstile-internal"
        payload = {
            "status": "error",
            "message": (
                "Error: Error solving the challenge. WebDriver failed for "
                f"{source_url} in {session_id}"
            ),
        }
        response = _ok_response(payload, status_code=status_code)
        response.text = json.dumps(payload)

        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrError) as raised:
                    client.get(source_url, session_id)

        assert not isinstance(raised.value, FlareSolverrCFChallengeFailed)
        assert client.get_traffic_stats()["cf_challenge_failures"] == 0
        diagnostic = caplog.text + str(raised.value)
        assert source_url not in diagnostic
        assert "internal-secret" not in diagnostic
        assert session_id not in diagnostic

    @pytest.mark.parametrize(
        ("status_code", "message"),
        [
            (500, "Cloudflare has blocked this request"),
            (503, "Turnstile verification failed"),
        ],
    )
    def test_http_error_keeps_explicit_cf_evidence(self, status_code, message):
        client = FlareSolverrClient()
        response = _ok_response({}, status_code=status_code)
        response.text = json.dumps({"status": "error", "message": message})

        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response
            with pytest.raises(FlareSolverrCFChallengeFailed):
                client.get("https://x.com", "s1")

        assert client.get_traffic_stats()["cf_challenge_failures"] == 1

    def test_get_raises_generic_error(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {"status": "error", "message": "Session not found"}
            )
            with pytest.raises(FlareSolverrError) as exc_info:
                client.get("https://x.com", "s1")
            # Must be the base error, NOT the CF subclass.
            assert not isinstance(exc_info.value, FlareSolverrCFChallengeFailed)

    def test_get_raises_timeout_on_requests_timeout(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.Timeout("slow")
            with pytest.raises(FlareSolverrTimeout):
                client.get("https://x.com", "s1")

    def test_get_raises_timeout_on_connection_error(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError("refused")
            with pytest.raises(FlareSolverrTimeout):
                client.get("https://x.com", "s1")

    def test_get_raises_on_non_2xx(self):
        client = FlareSolverrClient()
        bad_resp = MagicMock()
        bad_resp.ok = False
        bad_resp.status_code = 500
        bad_resp.text = "Internal Server Error"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = bad_resp
            with pytest.raises(FlareSolverrError) as exc_info:
                client.get("https://x.com", "s1")
            assert not isinstance(exc_info.value, FlareSolverrCFChallengeFailed)
            assert not isinstance(exc_info.value, FlareSolverrTimeout)

    def test_xhr_http_413_is_typed_as_response_budget_failure(self):
        client = FlareSolverrClient(url="http://fs:8191")
        response = _ok_response({}, status_code=413)
        response.text = "WhoScored batch exceeds the byte limit"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response

            with pytest.raises(FlareSolverrResponseTooLarge):
                client.xhr_get_many(
                    ["https://www.whoscored.com/statisticsfeed/1/getteamstatistics"],
                    "ws-direct-1",
                )

    @pytest.mark.parametrize("payload", [None, [], "ok", 1, True])
    def test_get_rejects_non_object_json_with_typed_protocol_error(self, payload):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(payload)

            with pytest.raises(FlareSolverrError, match="non-object JSON"):
                client.get("https://x.com", "s1")

    def test_get_max_timeout_default(self):
        client = FlareSolverrClient(default_max_timeout_ms=60_000)
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get("https://x.com", "s1")
        payload = sess.post.call_args.kwargs["json"]
        assert payload["maxTimeout"] == 60_000

    def test_get_max_timeout_custom(self):
        client = FlareSolverrClient(default_max_timeout_ms=60_000)
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(_ok_solution_payload())
            client.get("https://x.com", "s1", max_timeout_ms=30_000)
        payload = sess.post.call_args.kwargs["json"]
        assert payload["maxTimeout"] == 30_000


# -----------------------------------------------------------------------------
# session lifecycle tests
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrSessions:
    def test_create_session_payload(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            client.create_session("my-id")

        payload = sess.post.call_args.kwargs["json"]
        assert payload == {"cmd": "sessions.create", "session": "my-id"}

    def test_create_session_with_proxy_splits_credentials(self):
        # Chromium rejects creds embedded in the proxy URL
        # (ERR_NO_SUPPORTED_PROXIES, #647) — auth must go in separate
        # username/password fields, with a credential-free url.
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            client.create_session("id", proxy_url="http://u:p@h:1")

        payload = sess.post.call_args.kwargs["json"]
        assert payload["cmd"] == "sessions.create"
        assert payload["session"] == "id"
        assert payload["proxy"] == {
            "url": "http://h:1",
            "username": "u",
            "password": "p",
        }

    def test_create_session_proxy_without_credentials_unchanged(self):
        # A credential-free proxy URL is passed through verbatim.
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            client.create_session("id", proxy_url="http://h:1")

        payload = sess.post.call_args.kwargs["json"]
        assert payload["proxy"] == {"url": "http://h:1"}

    def test_create_session_decodes_percent_encoded_lease_credentials(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            client.create_session(
                "id", proxy_url="http://lease:s%2Fecret%2Btoken@proxy_filter:8900"
            )

        payload = sess.post.call_args.kwargs["json"]
        assert payload["proxy"] == {
            "url": "http://proxy_filter:8900",
            "username": "lease",
            "password": "s/ecret+token",
        }

    def test_create_session_log_never_contains_proxy_credentials(self, caplog):
        client = FlareSolverrClient()
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            with caplog.at_level("INFO"):
                client.create_session(
                    session_id,
                    proxy_url="http://lease:top-secret@proxy_filter:8900",
                )

        assert session_id not in caplog.text
        assert "top-secret" not in caplog.text
        assert "ad-tech filter" in caplog.text

    def test_create_error_redacts_echoed_session_and_proxy_values(self, caplog):
        endpoint_base = "http://endpoint-user:endpoint-secret@fs.internal:8191"
        endpoint = f"{endpoint_base}/v1"
        client = FlareSolverrClient(url=endpoint_base)
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        secret = "top-secret"
        proxy_url = "http://proxy_filter:8900"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {
                    "status": "error",
                    "message": (
                        f"failed {session_id} at {endpoint} through {proxy_url} "
                        f"with user lease and password {secret}"
                    ),
                }
            )
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrError) as raised:
                    client.create_session(
                        session_id,
                        proxy_url=f"http://lease:{secret}@proxy_filter:8900",
                    )

        assert session_id not in caplog.text
        assert endpoint_base not in caplog.text
        assert "endpoint-secret" not in caplog.text
        assert proxy_url not in caplog.text
        assert "lease" not in caplog.text
        assert secret not in caplog.text
        assert session_id not in str(raised.value)
        assert endpoint_base not in str(raised.value)
        assert proxy_url not in str(raised.value)
        assert "lease" not in str(raised.value)
        assert secret not in str(raised.value)

    @pytest.mark.parametrize("rendering", ["json", "repr"])
    def test_http_error_redacts_escaped_credential_before_truncation(
        self, caplog, rendering
    ):
        client = FlareSolverrClient()
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        secret = 'BOUNDARY-"quoted\\credential'
        proxy_url = "http://proxy_filter:8900"
        encoded_proxy_url = (
            "http://lease:BOUNDARY-%22quoted%5Ccredential@proxy_filter:8900"
        )
        rendered_secret = (
            json.dumps(secret)[1:-1] if rendering == "json" else repr(secret)[1:-1]
        )
        response = _ok_response({"status": "error"}, status_code=500)
        # The old implementation truncated here first and leaked "BOUND" from
        # a sensitive value spanning its 300-character boundary.
        response.text = "x" * 294 + rendered_secret + f" {session_id} {proxy_url} lease"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrError) as raised:
                    client.create_session(session_id, proxy_url=encoded_proxy_url)

        diagnostic = caplog.text + str(raised.value)
        assert "BOUND" not in diagnostic
        assert secret not in diagnostic
        assert json.dumps(secret)[1:-1] not in diagnostic
        assert repr(secret)[1:-1] not in diagnostic
        assert session_id not in diagnostic
        assert proxy_url not in diagnostic
        assert "HTTP 500" in diagnostic
        assert "cmd=sessions.create" in caplog.text

    @pytest.mark.parametrize("as_bytes", [False, True])
    def test_http_body_redacts_nested_repr_before_truncation(self, caplog, as_bytes):
        secret = "p'ass\"\\word"
        payload = {
            "cmd": "sessions.create",
            "session": "ws-cap-a1b2c3d4e5f60718-direct-nested",
            "proxy": {
                "url": "http://proxy_filter:8900",
                "username": "lease",
                "password": secret,
            },
        }
        json_secret = json.dumps(secret, ensure_ascii=True)
        nested = repr(json_secret.encode("utf-8") if as_bytes else json_secret)
        response = _ok_response({"status": "error"}, status_code=500)
        # The nested secret straddles the 300-character diagnostic boundary.
        # Redaction must happen on the complete body before slicing it.
        response.text = "x" * 280 + nested + "tail" * 100
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = response
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrError) as raised:
                    client._post(payload)

        diagnostic = caplog.text + str(raised.value)
        assert secret not in diagnostic
        assert "p\\'ass" not in diagnostic
        for variant in _escaped_sensitive_variants(secret):
            assert variant not in diagnostic
        assert "HTTP 500" in diagnostic

    def test_create_transport_error_suppresses_sensitive_cause(self, caplog):
        client = FlareSolverrClient()
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError(session_id)
            with caplog.at_level("WARNING"):
                with pytest.raises(FlareSolverrTimeout) as raised:
                    client.create_session(session_id)

        assert session_id not in caplog.text
        assert session_id not in str(raised.value)
        assert raised.value.__suppress_context__ is True
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    def test_destroy_session_log_never_contains_session_id(self, caplog):
        client = FlareSolverrClient()
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            with caplog.at_level("INFO"):
                client.destroy_session(session_id)

        assert session_id not in caplog.text

    def test_destroy_session_idempotent(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {"status": "error", "message": "Session not found"}
            )
            # Must NOT raise.
            client.destroy_session("ghost")

    def test_destroy_session_swallows_timeout(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = requests.exceptions.ConnectionError("x")
            # FlareSolverrTimeout is a subclass of FlareSolverrError → swallowed.
            client.destroy_session("whatever")

    def test_list_sessions(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response(
                {"status": "ok", "sessions": ["a", "b"]}
            )
            assert client.list_sessions() == ["a", "b"]


# -----------------------------------------------------------------------------
# /health
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrHealth:
    def test_health_returns_true(self):
        client = FlareSolverrClient(url="http://fs:8191")
        with patch.object(client, "session", new=MagicMock()) as sess:
            resp = MagicMock()
            resp.status_code = 200
            sess.get.return_value = resp
            assert client.health() is True
        sess.get.assert_called_once()
        assert sess.get.call_args.args[0] == "http://fs:8191/health"

    def test_health_returns_false_on_error(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.get.side_effect = requests.exceptions.ConnectionError("nope")
            # Must NOT raise.
            assert client.health() is False

    def test_health_returns_false_on_non_200(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            resp = MagicMock()
            resp.status_code = 503
            sess.get.return_value = resp
            assert client.health() is False


# -----------------------------------------------------------------------------
# Context manager
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestFlareSolverrContextManager:
    @pytest.mark.parametrize("method", ["close", "exit"])
    def test_context_cleanup_log_never_contains_session_id(self, caplog, method):
        client = FlareSolverrClient()
        session_id = "ws-cap-a1b2c3d4e5f60718-direct-0123456789"
        client._auto_session_id = session_id
        error = RuntimeError(f"could not destroy {session_id}")

        with patch.object(client, "destroy_session", side_effect=error):
            with caplog.at_level("DEBUG"):
                if method == "close":
                    client.close()
                else:
                    client.__exit__(None, None, None)

        assert session_id not in caplog.text

    def test_context_manager_failed_create_destroys_orphan_and_closes_pool(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.side_effect = [
                requests.exceptions.ConnectionError("create response lost"),
                _ok_response({"status": "ok"}),
            ]

            with pytest.raises(FlareSolverrTimeout):
                client.__enter__()

            assert sess.post.call_count == 2
            destroy = sess.post.call_args_list[1].kwargs["json"]
            assert destroy["cmd"] == "sessions.destroy"
            assert destroy["session"].startswith("fs-")
            sess.close.assert_called_once_with()
            assert client._session is None

    def test_context_manager_creates_and_destroys(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})

            with client as (c, session_id):
                assert c is client
                assert session_id.startswith("fs-")
                # 'fs-' + 8 hex chars = 11 chars total.
                assert len(session_id) == 11

        # Two posts: sessions.create then sessions.destroy.
        assert sess.post.call_count == 2
        cmds = [call.kwargs["json"]["cmd"] for call in sess.post.call_args_list]
        sessions = [call.kwargs["json"]["session"] for call in sess.post.call_args_list]
        assert cmds == ["sessions.create", "sessions.destroy"]
        # Both calls used the same auto-generated session id.
        assert sessions[0] == sessions[1]
        assert sessions[0].startswith("fs-")

    def test_context_manager_destroys_on_exception(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})

            with pytest.raises(RuntimeError):
                with client as (_c, _sid):
                    raise RuntimeError("boom")

        # destroy still happened.
        cmds = [call.kwargs["json"]["cmd"] for call in sess.post.call_args_list]
        assert "sessions.destroy" in cmds

    def test_context_manager_resets_auto_session_after_exit(self):
        client = FlareSolverrClient()
        with patch.object(client, "session", new=MagicMock()) as sess:
            sess.post.return_value = _ok_response({"status": "ok"})
            with client as (_c, _sid):
                assert client._auto_session_id is not None
            # Cleared after __exit__.
            assert client._auto_session_id is None
            sess.close.assert_called_once_with()


# -----------------------------------------------------------------------------
# is_chromium_error_page tests (#655)
# -----------------------------------------------------------------------------
@pytest.mark.unit
class TestIsChromiumErrorPage:
    """FlareSolverr returns Chromium net-error pages as HTTP 200 HTML; these
    must be detected so callers refuse to cache them (#655)."""

    @pytest.mark.parametrize(
        "html",
        [
            "<html><body>chrome-error://dino/</body></html>",
            '<html id="neterror"><body>...</body></html>',
            "<html><body>ERR_NO_SUPPORTED_PROXIES</body></html>",
            # Realistic shape: title is the host, body is the chrome error page.
            "<html><head><title>sofifa.com</title></head>"
            '<body class="neterror"><div id="main-frame-error"></div></body></html>',
        ],
    )
    def test_detects_error_pages(self, html):
        assert is_chromium_error_page(html) is True

    @pytest.mark.parametrize(
        "html",
        [
            "",
            "<html><body><p>real content</p></body></html>",
            '<select id="select-version"><option>FC 26</option></select>',
        ],
    )
    def test_passes_real_html(self, html):
        assert is_chromium_error_page(html) is False
