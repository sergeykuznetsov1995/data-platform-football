"""
Alert Callbacks
===============

on_failure / on_retry callbacks that push notifications to Telegram (and
optionally SMTP). Configure via Airflow Variables or environment variables:

    TELEGRAM_BOT_TOKEN   — bot token from @BotFather
    TELEGRAM_CHAT_ID     — target chat/group id
    ALERT_ENV            — environment tag (dev/prod), shown in message

Missing tokens cause the callback to log a warning and return quietly —
never raise, so failing to notify does not mask the original error.
"""

from __future__ import annotations

import logging
import os
import urllib.request
import urllib.parse
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_TG_API = "https://api.telegram.org"


def validate_alert_environment(expected: str = "prod") -> Dict[str, str]:
    """Fail closed unless production Telegram alerting is configured.

    Alert callbacks deliberately remain best-effort, but a production source
    DAG must not start paid work while its notifications are labelled as a
    development/test environment or have no delivery credentials. Keeping this
    as an explicit task callable makes the deployment prerequisite visible in
    the Airflow graph.
    """

    normalized_expected = str(expected).strip().casefold()
    actual = str(_get_var("ALERT_ENV", "") or "").strip().casefold()
    if not normalized_expected:
        raise ValueError("expected alert environment must not be empty")
    if actual != normalized_expected:
        raise RuntimeError(
            "Production alert environment is not ready: "
            f"ALERT_ENV={actual or '<unset>'!r}, expected "
            f"{normalized_expected!r}"
        )
    missing = [
        name
        for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
        if not str(_get_var(name, "") or "").strip()
    ]
    if missing:
        raise RuntimeError(
            "Production alert delivery is not ready: missing "
            + ", ".join(missing)
        )
    return {
        "alert_env": actual,
        "alert_delivery": "telegram",
        "status": "ready",
    }


def _get_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """Prefer Airflow Variable if available, fall back to env var."""
    try:
        from airflow.models import Variable
        val = Variable.get(name, default_var=None)
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, default)


def _send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    token = _get_var('TELEGRAM_BOT_TOKEN')
    chat_id = _get_var('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        logger.warning("Telegram alert skipped: TELEGRAM_BOT_TOKEN/CHAT_ID not set")
        return False

    url = f"{_TG_API}/bot{token}/sendMessage"
    body = urllib.parse.urlencode({
        'chat_id': chat_id,
        'text': message[:4000],  # Telegram hard limit ~4096 chars
        'parse_mode': parse_mode,
        'disable_web_page_preview': 'true',
    }).encode()
    try:
        req = urllib.request.Request(url, data=body, method='POST')
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = resp.status == 200
            if not ok:
                logger.warning(f"Telegram returned {resp.status}: {resp.read()[:200]!r}")
            return ok
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False


def _format_failure(context: Dict[str, Any]) -> str:
    ti = context.get('task_instance') or context.get('ti')
    dag_id = context.get('dag').dag_id if context.get('dag') else getattr(ti, 'dag_id', '?')
    task_id = getattr(ti, 'task_id', '?')
    run_id = context.get('run_id') or getattr(ti, 'run_id', '?')
    try_number = getattr(ti, 'try_number', '?')
    exception = context.get('exception') or context.get('reason') or ''
    log_url = getattr(ti, 'log_url', '') or ''
    env = _get_var('ALERT_ENV', 'dev')

    lines = [
        f"<b>[{env}] DAG FAILED</b> ❌",
        f"<b>DAG:</b> <code>{dag_id}</code>",
        f"<b>Task:</b> <code>{task_id}</code>",
        f"<b>Run:</b> <code>{run_id}</code>",
        f"<b>Try:</b> {try_number}",
    ]
    if exception:
        msg = str(exception).replace('<', '&lt;').replace('>', '&gt;')
        lines.append(f"<b>Error:</b> <code>{msg[:800]}</code>")
    if log_url:
        lines.append(f"<a href=\"{log_url}\">Airflow log</a>")
    return "\n".join(lines)


def telegram_on_failure(context: Dict[str, Any]) -> None:
    """Airflow on_failure_callback — sends a Telegram alert.

    Never raises: telemetry failure must not mask the task failure.
    """
    try:
        message = _format_failure(context)
        _send_telegram(message)
    except Exception as e:
        logger.warning(f"telegram_on_failure swallowed: {e}")


def send_telegram_message(message: str, level: str = "info") -> bool:
    """Send an arbitrary message to Telegram (no Airflow context required).

    Use this for ad-hoc notifications from inside task callables (e.g.
    Superset alert bridge in dag_superset_alerts). For DAG-level
    on_failure callbacks, prefer ``telegram_on_failure``.

    Args:
        message: Message body. May contain HTML (<b>, <code>, <a href>);
            anything <4000 chars will be passed through, longer messages
            are truncated by ``_send_telegram``.
        level: Severity tag prepended to the message — one of
            ``info``, ``warning``, ``error``, ``critical``.

    Returns:
        True if Telegram accepted the message, False otherwise (missing
        creds, HTTP error, network error). Never raises.
    """
    try:
        env = _get_var('ALERT_ENV', 'dev')
        emoji = {
            'info': 'ℹ️',
            'warning': '⚠️',
            'error': '❌',
            'critical': '🔥',
        }.get(level.lower(), 'ℹ️')
        prefix = f"<b>[{env}] {emoji} {level.upper()}</b>\n"
        return _send_telegram(prefix + message)
    except Exception as e:
        logger.warning(f"send_telegram_message swallowed: {e}")
        return False


def telegram_dq_summary(report, header: str = "DQ report") -> None:
    """Post a DQ run_checks() report to Telegram.

    Args:
        report: RunReport from utils.data_quality.run_checks
        header: short label for the message
    """
    try:
        env = _get_var('ALERT_ENV', 'dev')
        lines = [f"<b>[{env}] {header}</b>", f"<i>{report.summary()}</i>"]
        for r in report.errors[:10]:
            lines.append(f"❌ <code>{r.name}</code>: {(r.details or r.error)[:200]}")
        for r in report.warnings[:5]:
            lines.append(f"⚠️ <code>{r.name}</code>: {(r.details or r.error)[:200]}")
        _send_telegram("\n".join(lines))
    except Exception as e:
        logger.warning(f"telegram_dq_summary swallowed: {e}")
