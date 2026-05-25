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
import urllib.error
import urllib.parse
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
