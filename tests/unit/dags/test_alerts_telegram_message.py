"""
Unit tests for ``utils.alerts.send_telegram_message``.

This helper is invoked from:
  * ``dag_superset_alerts.check_alerts`` — Superset alert bridge
  * ``data_quality.run_checks`` indirectly via ``telegram_dq_summary``

Behaviour we exercise here:
  * Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID → False, no exception, no
    HTTP call attempted (otherwise local dev would hammer Telegram).
  * Successful HTTP → returns True, posts to the documented endpoint with
    chat_id, text, parse_mode=HTML, disable_web_page_preview=true.
  * HTTPError (e.g. 401 invalid token) → returns False, NO exception.
  * URLError (e.g. network down) → returns False, NO exception.
  * Special HTML characters (<, >, &) — the helper passes parse_mode=HTML so
    the caller is responsible for escaping. We assert the body is forwarded
    unmodified (this is the implementation contract — the DAGs that call
    send_telegram_message already pre-escape via ``str.replace``).
  * Long messages (>4096 chars) — Telegram hard limit ~4096; the helper
    truncates to 4000 to leave headroom for the level prefix.
"""

from __future__ import annotations

import io
import hashlib
import json
import time
import urllib.error
import urllib.parse
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def alerts_module():
    """Reload utils.alerts so previous monkeypatches don't leak."""
    import importlib

    import utils.alerts as m  # type: ignore  # resolved via sys.path tweak in conftest

    importlib.reload(m)
    return m


def _ok_urlopen():
    """Build a fake urlopen returning HTTP 200."""
    cm = MagicMock()
    cm.status = 200
    cm.read.return_value = b"{\"ok\": true}"
    cm.__enter__ = MagicMock(return_value=cm)
    cm.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=cm)


_PAID_CONTROL_SECRET = "paid-alert-test-control-secret-at-least-32-bytes"
_PAID_NONCE = "0123456789abcdef0123456789abcdef"


def _canonical_bytes(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _paid_rendered_text(
    *,
    campaign_id="campaign-1",
    approval_id="approval-1",
    approval_sha256="a" * 64,
    run_id="manual__campaign-1",
    delivery_nonce=_PAID_NONCE,
):
    return (
        "WhoScored paid-proxy preflight ✅\n"
        f"Campaign: {campaign_id}\n"
        f"Approval: {approval_id}\n"
        f"Approval SHA-256: {approval_sha256}\n"
        f"Run: {run_id}\n"
        f"Delivery nonce: {delivery_nonce}"
    )


def _paid_response(
    *,
    bot_id=123456789,
    chat_id=-100123,
    ok=True,
    message_id=77,
    date=None,
    text=None,
):
    payload = {
        "ok": ok,
        "result": {
            "chat": {"id": chat_id},
            "date": int(time.time()) if date is None else date,
            "from": {"id": bot_id, "is_bot": True},
            "message_id": message_id,
            "text": _paid_rendered_text() if text is None else text,
        },
    }
    cm = MagicMock()
    cm.status = 200
    cm.read.return_value = _canonical_bytes(payload)
    cm.__enter__ = MagicMock(return_value=cm)
    cm.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=cm)


def _paid_alert_setup(
    alerts_module,
    monkeypatch,
    tmp_path: Path,
    *,
    campaign_id="campaign-1",
    approval_id="approval-1",
    approval_sha256="a" * 64,
    token="123456789:paid-alert-secret-token-value",
    chat_id="-100123",
):
    authority = tmp_path / "authority"
    authority.mkdir()
    secret_path = authority / "telegram-secret.json"
    secret_path.write_bytes(
        _canonical_bytes(
            {
                "schema_version": 1,
                "bot_token": token,
                "chat_id": chat_id,
            }
        )
    )
    secret_path.chmod(0o444)
    target_sha256 = alerts_module.paid_alert_target_sha256(
        bot_id=int(token.split(":", 1)[0]),
        chat_id=int(chat_id),
    )
    binding = alerts_module.sign_paid_alert_binding(
        {
            "schema_version": 1,
            "source": "whoscored",
            "campaign_id": campaign_id,
            "approval_id": approval_id,
            "approval_sha256": approval_sha256,
            "target_sha256": target_sha256,
            "signature_algorithm": "hmac-sha256",
        },
        _PAID_CONTROL_SECRET,
    )
    binding_path = authority / "telegram-binding.json"
    binding_path.write_bytes(_canonical_bytes(binding))
    binding_path.chmod(0o444)
    monkeypatch.setenv("ALERT_ENV", "prod")
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_AUTHORITY_ROOT", str(authority))
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_SECRET_PATH", str(secret_path))
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_BINDING_PATH", str(binding_path))
    monkeypatch.setenv(
        "WHOSCORED_PAID_ALERT_RECEIPT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_HMAC_SECRET", _PAID_CONTROL_SECRET)
    monkeypatch.setattr(alerts_module.secrets, "token_hex", lambda _size: _PAID_NONCE)
    return {
        "campaign_id": campaign_id,
        "approval_id": approval_id,
        "approval_sha256": approval_sha256,
        "dag_id": "dag_canary_whoscored_proxy",
        "run_id": "manual__campaign-1",
        "alert_task_id": "deliver_whoscored_proxy_canary_alert",
        "target_sha256": target_sha256,
        "secret_path": secret_path,
        "binding_path": binding_path,
    }


def _deliver_paid(alerts_module, monkeypatch, authority):
    monkeypatch.setattr(
        alerts_module,
        "_open_paid_telegram_request",
        _paid_response(),
    )
    return alerts_module.validate_alert_delivery(
        **{
            key: authority[key]
            for key in (
                "campaign_id",
                "approval_id",
                "approval_sha256",
                "dag_id",
                "run_id",
                "alert_task_id",
            )
        }
    )


@pytest.mark.unit
class TestProductionEnvironmentReadiness:
    def test_prod_alert_environment_is_ready(
        self, alerts_module, monkeypatch
    ):
        monkeypatch.setenv("ALERT_ENV", "prod")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")

        assert alerts_module.validate_alert_environment() == {
            "alert_env": "prod",
            "alert_delivery": "telegram",
            "status": "ready",
        }

    @pytest.mark.parametrize("value", [None, "dev", "test", "production"])
    def test_non_prod_alert_environment_fails_closed(
        self, alerts_module, monkeypatch, value
    ):
        if value is None:
            monkeypatch.delenv("ALERT_ENV", raising=False)
        else:
            monkeypatch.setenv("ALERT_ENV", value)

        with pytest.raises(RuntimeError, match="ALERT_ENV"):
            alerts_module.validate_alert_environment()

    @pytest.mark.parametrize(
        "missing_name", ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
    )
    def test_missing_telegram_credential_fails_closed(
        self, alerts_module, monkeypatch, missing_name
    ):
        monkeypatch.setenv("ALERT_ENV", "prod")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
        monkeypatch.setenv(missing_name, "   ")

        with pytest.raises(RuntimeError, match=missing_name):
            alerts_module.validate_alert_environment()

    def test_paid_preflight_requires_read_only_secret_not_variable_fallback(
        self, alerts_module, monkeypatch
    ):
        monkeypatch.setenv("ALERT_ENV", "prod")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "generic-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
        monkeypatch.setattr(
            alerts_module,
            "_get_var",
            lambda name, default=None: {
                "TELEGRAM_BOT_TOKEN": "variable-token",
                "TELEGRAM_CHAT_ID": "456",
            }.get(name, default),
        )

        with pytest.raises(
            alerts_module.PaidAlertError,
            match="WHOSCORED_PAID_ALERT_SECRET_PATH",
        ):
            alerts_module.validate_alert_delivery(
                campaign_id="campaign-1",
                approval_id="approval-1",
                approval_sha256="a" * 64,
                dag_id="dag_canary_whoscored_proxy",
                run_id="manual__campaign-1",
                alert_task_id="deliver_whoscored_proxy_canary_alert",
            )


@pytest.mark.unit
class TestPaidAlertAuthorityAndReceipt:
    def test_exact_delivery_persists_and_revalidates_content_addressed_receipt(
        self, alerts_module, monkeypatch, tmp_path
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)

        assert result["status"] == "delivered"
        assert result["target_sha256"] == authority["target_sha256"]
        receipt = Path(result["receipt_path"])
        assert receipt.name == f"{result['receipt_sha256']}.json"
        assert receipt.stat().st_mode & 0o777 == 0o400
        verified = alerts_module.verify_paid_alert_receipt(
            result,
            **{
                key: authority[key]
                for key in (
                    "campaign_id",
                    "approval_id",
                    "approval_sha256",
                    "dag_id",
                    "run_id",
                    "alert_task_id",
                )
            },
        )
        assert verified["telegram_message_id"] == 77
        assert abs(verified["telegram_message_date"] - int(time.time())) <= 2

    @pytest.mark.parametrize(
        ("raw", "message"),
        (
            (_canonical_bytes({"ok": False}), "does not prove exact delivery"),
            (b"not-json", "not canonical JSON"),
            (
                _canonical_bytes(
                    {
                        "ok": True,
                        "result": {
                            "chat": {"id": -999},
                            "date": int(time.time()),
                            "from": {"id": 123456789, "is_bot": True},
                            "message_id": 77,
                            "text": _paid_rendered_text(),
                        },
                    }
                ),
                "does not prove exact delivery",
            ),
            (
                _canonical_bytes(
                    {
                        "ok": True,
                        "result": {
                            "chat": {"id": -100123},
                            "date": int(time.time()),
                            "from": {"id": 123456789, "is_bot": True},
                            "message_id": 0,
                            "text": _paid_rendered_text(),
                        },
                    }
                ),
                "does not prove exact delivery",
            ),
            (
                _canonical_bytes(
                    {
                        "ok": True,
                        "result": {
                            "chat": {"id": -100123},
                            "date": int(time.time()),
                            "from": {"id": 999999, "is_bot": True},
                            "message_id": 77,
                            "text": _paid_rendered_text(),
                        },
                    }
                ),
                "does not prove exact delivery",
            ),
            (
                _canonical_bytes(
                    {
                        "ok": True,
                        "result": {
                            "chat": {"id": -100123},
                            "date": int(time.time()),
                            "from": {"id": 123456789, "is_bot": True},
                            "message_id": 77,
                            "text": "replayed unrelated message",
                        },
                    }
                ),
                "does not prove exact delivery",
            ),
            (
                _canonical_bytes(
                    {
                        "ok": True,
                        "result": {
                            "chat": {"id": -100123},
                            "date": int(time.time()) - 600,
                            "from": {"id": 123456789, "is_bot": True},
                            "message_id": 77,
                            "text": _paid_rendered_text(),
                        },
                    }
                ),
                "does not prove exact delivery",
            ),
        ),
    )
    def test_http_200_requires_ok_exact_chat_and_integer_message_proof(
        self,
        alerts_module,
        monkeypatch,
        tmp_path,
        raw,
        message,
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        response = MagicMock()
        response.status = 200
        response.read.return_value = raw
        response.__enter__ = MagicMock(return_value=response)
        response.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(
            alerts_module,
            "_open_paid_telegram_request",
            MagicMock(return_value=response),
        )

        with pytest.raises(alerts_module.PaidAlertError, match=message):
            alerts_module.validate_alert_delivery(
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                }
            )

    def test_receipt_tamper_and_cross_campaign_reuse_are_rejected(
        self, alerts_module, monkeypatch, tmp_path
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)

        with pytest.raises(alerts_module.PaidAlertError):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    **{
                        key: authority[key]
                        for key in (
                            "approval_id",
                            "approval_sha256",
                            "dag_id",
                            "run_id",
                            "alert_task_id",
                        )
                    },
                    "campaign_id": "campaign-2",
                },
            )

        receipt = Path(result["receipt_path"])
        raw = receipt.read_bytes()
        receipt.chmod(0o600)
        receipt.write_bytes(raw.replace(b'"message_sha256":"', b'"message_sha256":"0'))
        receipt.chmod(0o400)
        with pytest.raises(alerts_module.PaidAlertError, match="address/content"):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

    def test_secret_rotation_and_paid_secret_path_mutation_after_preflight_reject(
        self, alerts_module, monkeypatch, tmp_path
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)
        secret_path = authority["secret_path"]
        original = secret_path.read_bytes()

        secret_path.chmod(0o600)
        secret_path.write_bytes(
            _canonical_bytes(
                {
                    "schema_version": 1,
                    "bot_token": "123456789:rotated-paid-alert-token-value",
                    "chat_id": "-100123",
                }
            )
        )
        secret_path.chmod(0o444)
        with pytest.raises(
            alerts_module.PaidAlertError,
            match="campaign/target authority",
        ):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

        secret_path.chmod(0o600)
        secret_path.write_bytes(original)
        secret_path.chmod(0o444)
        alternate = secret_path.with_name("alternate-secret.json")
        alternate.write_bytes(original)
        alternate.chmod(0o444)
        monkeypatch.setenv("WHOSCORED_PAID_ALERT_SECRET_PATH", str(alternate))
        with pytest.raises(
            alerts_module.PaidAlertError,
            match="campaign/target authority",
        ):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

    @pytest.mark.parametrize(
        "field",
        ("message_sha256", "telegram_response_sha256"),
    )
    def test_readdressed_receipt_cannot_tamper_message_or_response_proof(
        self, alerts_module, monkeypatch, tmp_path, field
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)
        original = Path(result["receipt_path"])
        payload = json.loads(original.read_text(encoding="utf-8"))
        payload[field] = "d" * 64
        raw = _canonical_bytes(payload)
        digest = hashlib.sha256(raw).hexdigest()
        tampered_path = original.with_name(f"{digest}.json")
        tampered_path.write_bytes(raw)
        tampered_path.chmod(0o400)
        tampered_metadata = {
            **result,
            "receipt_path": str(tampered_path),
            "receipt_sha256": digest,
        }

        with pytest.raises(
            alerts_module.PaidAlertError,
            match="campaign/target authority",
        ):
            alerts_module.verify_paid_alert_receipt(
                tampered_metadata,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

    def test_old_receipt_is_rejected_after_message_contract_change(
        self, alerts_module, monkeypatch, tmp_path
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)
        original = alerts_module._paid_preflight_message

        def changed_message(**kwargs):
            html, rendered = original(**kwargs)
            return html + " v2", rendered + " v2"

        monkeypatch.setattr(alerts_module, "_paid_preflight_message", changed_message)
        with pytest.raises(
            alerts_module.PaidAlertError,
            match="campaign/target authority",
        ):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

    def test_alert_environment_mutation_after_preflight_is_rejected(
        self, alerts_module, monkeypatch, tmp_path
    ):
        authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
        result = _deliver_paid(alerts_module, monkeypatch, authority)
        monkeypatch.setenv("ALERT_ENV", "dev")

        with pytest.raises(alerts_module.PaidAlertError, match="environment"):
            alerts_module.verify_paid_alert_receipt(
                result,
                **{
                    key: authority[key]
                    for key in (
                        "campaign_id",
                        "approval_id",
                        "approval_sha256",
                        "dag_id",
                        "run_id",
                        "alert_task_id",
                    )
                },
            )

    def test_paid_request_ignores_ambient_proxy_environment(
        self, alerts_module, monkeypatch
    ):
        monkeypatch.setenv("HTTPS_PROXY", "http://attacker.invalid:8080")
        captured = []
        opener = MagicMock()
        monkeypatch.setattr(
            alerts_module.urllib.request,
            "build_opener",
            lambda *handlers: captured.extend(handlers) or opener,
        )
        request = alerts_module.urllib.request.Request("https://example.invalid")

        assert (
            alerts_module._open_paid_telegram_request(request, timeout=3)
            is opener.open.return_value
        )
        assert len(captured) == 1
        assert isinstance(captured[0], alerts_module.urllib.request.ProxyHandler)
        assert captured[0].proxies == {}


@pytest.mark.unit
def test_gateway_owned_alert_state_is_restart_idempotent(
    alerts_module, monkeypatch, tmp_path
):
    authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
    opener = _paid_response()
    monkeypatch.setattr(alerts_module, "_open_paid_telegram_request", opener)
    identity = {
        name: authority[name]
        for name in (
            "campaign_id",
            "approval_id",
            "approval_sha256",
            "dag_id",
            "run_id",
            "alert_task_id",
        )
    }

    first = alerts_module.ensure_paid_alert_delivery(**identity)
    second = alerts_module.ensure_paid_alert_delivery(**identity)
    required = alerts_module.require_paid_alert_delivery(**identity)

    assert opener.call_count == 1
    assert first == second == required


@pytest.mark.unit
def test_ambiguous_alert_crash_fails_closed_without_resend(
    alerts_module, monkeypatch, tmp_path
):
    authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
    identity = {
        name: authority[name]
        for name in (
            "campaign_id",
            "approval_id",
            "approval_sha256",
            "dag_id",
            "run_id",
            "alert_task_id",
        )
    }
    attempts = []

    def ambiguous(**_kwargs):
        attempts.append("send")
        raise alerts_module.PaidAlertError("ambiguous Telegram outcome")

    monkeypatch.setattr(alerts_module, "validate_alert_delivery", ambiguous)

    with pytest.raises(alerts_module.PaidAlertError, match="ambiguous"):
        alerts_module.ensure_paid_alert_delivery(**identity)
    with pytest.raises(alerts_module.PaidAlertError, match="not durably delivered"):
        alerts_module.ensure_paid_alert_delivery(**identity)

    assert attempts == ["send"]


@pytest.mark.unit
def test_alert_hmac_never_falls_back_to_proxy_control_token(
    alerts_module, monkeypatch, tmp_path
):
    authority = _paid_alert_setup(alerts_module, monkeypatch, tmp_path)
    monkeypatch.delenv("WHOSCORED_PAID_ALERT_HMAC_SECRET")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", _PAID_CONTROL_SECRET)

    with pytest.raises(
        alerts_module.PaidAlertError,
        match="WHOSCORED_PAID_ALERT_HMAC_SECRET",
    ):
        _deliver_paid(alerts_module, monkeypatch, authority)


# ===========================================================================
# 1. Missing credentials → no-op
# ===========================================================================


@pytest.mark.unit
class TestMissingCredentials:
    def test_missing_token_returns_false(
        self, alerts_module, no_telegram_env, monkeypatch, caplog
    ):
        urlopen = MagicMock()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        with caplog.at_level("WARNING"):
            ok = alerts_module.send_telegram_message("hello")

        assert ok is False
        urlopen.assert_not_called()
        # Warning should mention what's missing without leaking a token value.
        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert "TELEGRAM" in msgs.upper()

    def test_missing_chat_id_returns_false(
        self, alerts_module, monkeypatch
    ):
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        urlopen = MagicMock()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        ok = alerts_module.send_telegram_message("hello")
        assert ok is False
        urlopen.assert_not_called()


# ===========================================================================
# 2. Successful send
# ===========================================================================


@pytest.mark.unit
class TestSuccessfulSend:
    def test_returns_true_and_posts_to_telegram(
        self, alerts_module, telegram_env, monkeypatch
    ):
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        ok = alerts_module.send_telegram_message("hello world", level="info")
        assert ok is True

        # Inspect the Request that was sent.
        request_obj = urlopen.call_args[0][0]
        assert request_obj.full_url.startswith("https://api.telegram.org/bot")
        assert "/sendMessage" in request_obj.full_url

        # Body is application/x-www-form-urlencoded
        body = urllib.parse.parse_qs(request_obj.data.decode("utf-8"))
        assert body["chat_id"] == ["999"]
        assert body["parse_mode"] == ["HTML"]
        assert body["disable_web_page_preview"] == ["true"]
        text = body["text"][0]
        assert "hello world" in text
        # The level + env prefix should also be in the message
        assert "INFO" in text
        assert "test" in text  # ALERT_ENV value

    def test_warning_level_emits_warning_emoji(
        self, alerts_module, telegram_env, monkeypatch
    ):
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        alerts_module.send_telegram_message("threshold breached", level="warning")
        body = urllib.parse.parse_qs(
            urlopen.call_args[0][0].data.decode("utf-8")
        )
        text = body["text"][0]
        # Warning emoji from the level→emoji map
        assert "WARNING" in text
        assert "threshold breached" in text


# ===========================================================================
# 3. HTTP failures → graceful False
# ===========================================================================


@pytest.mark.unit
class TestHttpFailure:
    def test_http_error_401_returns_false(
        self, alerts_module, telegram_env, monkeypatch, caplog
    ):
        def _raise(*a, **kw):
            raise urllib.error.HTTPError(
                "url", 401, "Unauthorized", hdrs=None, fp=io.BytesIO(b"")
            )

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)
        with caplog.at_level("WARNING"):
            ok = alerts_module.send_telegram_message("hi")
        assert ok is False
        # Must NOT raise — telemetry failure must not mask the original error
        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert "Telegram send failed" in msgs

    def test_url_error_returns_false(
        self, alerts_module, telegram_env, monkeypatch
    ):
        def _raise(*a, **kw):
            raise urllib.error.URLError("Network is unreachable")

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)
        ok = alerts_module.send_telegram_message("hi")
        assert ok is False

    def test_non_200_status_returns_false(
        self, alerts_module, telegram_env, monkeypatch
    ):
        cm = MagicMock()
        cm.status = 503
        cm.read.return_value = b"upstream busy"
        cm.__enter__ = MagicMock(return_value=cm)
        cm.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(
            alerts_module.urllib.request,
            "urlopen",
            MagicMock(return_value=cm),
        )

        ok = alerts_module.send_telegram_message("hi")
        assert ok is False


# ===========================================================================
# 4. Special characters / encoding
# ===========================================================================


@pytest.mark.unit
class TestEncoding:
    def test_html_special_chars_passed_through(
        self, alerts_module, telegram_env, monkeypatch
    ):
        """parse_mode=HTML means callers escape <,>,&. We just URL-encode."""
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        msg = "<b>raw</b> & <code>tag</code>"
        alerts_module.send_telegram_message(msg)

        body = urlopen.call_args[0][0].data.decode("utf-8")
        # urlencoded form: '<' -> '%3C', '&' -> '%26'
        decoded = urllib.parse.parse_qs(body)["text"][0]
        # The raw HTML must round-trip unchanged
        assert "<b>raw</b>" in decoded
        assert "&" in decoded
        assert "<code>tag</code>" in decoded

    def test_unicode_message_round_trips(
        self, alerts_module, telegram_env, monkeypatch
    ):
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        msg = "Тест ⚠️ alert: значение упало ниже 100"
        alerts_module.send_telegram_message(msg, level="warning")

        body = urlopen.call_args[0][0].data.decode("utf-8")
        decoded = urllib.parse.parse_qs(body)["text"][0]
        assert "Тест" in decoded
        assert "⚠️" in decoded
        assert "значение упало ниже 100" in decoded


# ===========================================================================
# 5. Long messages
# ===========================================================================


@pytest.mark.unit
class TestLongMessage:
    def test_long_message_truncated_under_4096(
        self, alerts_module, telegram_env, monkeypatch
    ):
        """
        Telegram hard-limits sendMessage text to ~4096 chars. The helper
        ``_send_telegram`` truncates ``message[:4000]`` to leave headroom
        for the level prefix added by ``send_telegram_message``.
        """
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        big_payload = "x" * 10_000
        ok = alerts_module.send_telegram_message(big_payload, level="info")
        assert ok is True

        body = urllib.parse.parse_qs(
            urlopen.call_args[0][0].data.decode("utf-8")
        )
        text = body["text"][0]
        assert len(text) <= 4096, (
            f"Telegram text must respect 4096-char limit, got {len(text)}"
        )

    def test_short_message_not_truncated(
        self, alerts_module, telegram_env, monkeypatch
    ):
        urlopen = _ok_urlopen()
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        alerts_module.send_telegram_message("short", level="info")
        text = urllib.parse.parse_qs(
            urlopen.call_args[0][0].data.decode("utf-8")
        )["text"][0]
        assert "short" in text


# ===========================================================================
# 6. Internal exceptions never escape
# ===========================================================================


@pytest.mark.unit
class TestNeverRaises:
    def test_unexpected_internal_error_returns_false(
        self, alerts_module, telegram_env, monkeypatch, caplog
    ):
        """If `_send_telegram` blows up for a non-HTTP reason we still return False."""
        def _broken(*a, **kw):
            raise RuntimeError("synthetic crash")

        monkeypatch.setattr(alerts_module, "_send_telegram", _broken)

        with caplog.at_level("WARNING"):
            ok = alerts_module.send_telegram_message("anything")

        assert ok is False
        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert "swallowed" in msgs or "synthetic crash" in msgs
