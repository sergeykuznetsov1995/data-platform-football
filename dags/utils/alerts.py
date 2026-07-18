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

Paid WhoScored traffic uses a separate, fail-closed path.  Its Telegram
credential is read only from a protected file, its destination is bound to the
signed campaign by a second HMAC document, and a source task must revalidate a
content-addressed delivery receipt before it may run.  Generic Airflow
Variables are deliberately never consulted by that path.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import hmac
import html as html_lib
import json
import logging
import os
import re
import secrets
import stat
import time
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

_TG_API = "https://api.telegram.org"
_PAID_ALERT_SECRET_PATH_ENV = "WHOSCORED_PAID_ALERT_SECRET_PATH"
_PAID_ALERT_BINDING_PATH_ENV = "WHOSCORED_PAID_ALERT_BINDING_PATH"
_PAID_ALERT_RECEIPT_ROOT_ENV = "WHOSCORED_PAID_ALERT_RECEIPT_ROOT"
_PAID_ALERT_AUTHORITY_ROOT_ENV = "WHOSCORED_PAID_ALERT_AUTHORITY_ROOT"
PAID_ALERT_HMAC_SECRET_ENV = "WHOSCORED_PAID_ALERT_HMAC_SECRET"
_DEFAULT_PAID_ALERT_RECEIPT_ROOT = (
    "/opt/airflow/logs/whoscored_paid_alert_receipts"
)
_MAX_PAID_ALERT_FILE_BYTES = 64 * 1024
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_TOKEN_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z", re.ASCII)
_BOT_TOKEN_RE = re.compile(r"\A([1-9][0-9]{0,18}):([^\s\x00-\x1f\x7f]{16,240})\Z")
_CHAT_ID_RE = re.compile(r"\A-?[1-9][0-9]{0,19}\Z")
_PAID_SECRET_FIELDS = frozenset({"schema_version", "bot_token", "chat_id"})
_PAID_BINDING_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "target_sha256",
        "signature_algorithm",
        "signature",
    }
)
_PAID_BINDING_UNSIGNED_FIELDS = _PAID_BINDING_FIELDS - {"signature"}
_PAID_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "dag_id",
        "run_id",
        "alert_task_id",
        "alert_env",
        "delivery_nonce",
        "target_sha256",
        "bot_id",
        "chat_id",
        "alert_secret_sha256",
        "alert_secret_path_sha256",
        "alert_binding_sha256",
        "alert_binding_path_sha256",
        "message_sha256",
        "telegram_response_sha256",
        "telegram_message_id",
        "telegram_message_date",
        "request_started_at",
        "response_received_at",
    }
)
_PAID_ALERT_CLOCK_SKEW_SECONDS = 30
PAID_ALERT_RECEIPT_ENV = {
    "campaign_id": "WHOSCORED_PAID_ALERT_CAMPAIGN_ID",
    "approval_id": "WHOSCORED_PAID_ALERT_APPROVAL_ID",
    "approval_sha256": "WHOSCORED_PAID_ALERT_APPROVAL_SHA256",
    "target_sha256": "WHOSCORED_PAID_ALERT_TARGET_SHA256",
    "telegram_message_id": "WHOSCORED_PAID_ALERT_MESSAGE_ID",
    "telegram_message_date": "WHOSCORED_PAID_ALERT_MESSAGE_DATE",
    "receipt_path": "WHOSCORED_PAID_ALERT_RECEIPT_PATH",
    "receipt_sha256": "WHOSCORED_PAID_ALERT_RECEIPT_SHA256",
}
_PAID_ALERT_METADATA_FIELDS = frozenset(
    {
        "status",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "target_sha256",
        "telegram_message_id",
        "telegram_message_date",
        "receipt_path",
        "receipt_sha256",
    }
)
_PAID_ALERT_TASK_BY_DAG = {
    "dag_ingest_whoscored": "validate_whoscored_paid_alert_delivery",
    "dag_backfill_whoscored": "validate_whoscored_paid_alert_delivery",
    "dag_canary_whoscored_proxy": "deliver_whoscored_proxy_canary_alert",
}
_PAID_ALERT_STATE_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "dag_id",
        "run_id",
        "alert_task_id",
        "metadata",
    }
)


class PaidAlertError(RuntimeError):
    """Paid alert authority, delivery, or receipt is not exact."""


class _DuplicateJsonKey(ValueError):
    """A security-sensitive JSON object contains an ambiguous duplicate key."""


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _unique_json_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _strict_json_object(
    raw: bytes,
    *,
    expected_fields: frozenset[str] | None = None,
    label: str,
    require_canonical: bool = True,
) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise PaidAlertError(f"{label} is not canonical JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise PaidAlertError(f"{label} must be a JSON object")
    if expected_fields is not None and frozenset(value) != expected_fields:
        raise PaidAlertError(f"{label} fields are not exact")
    if require_canonical and _canonical_json_bytes(value) != raw:
        raise PaidAlertError(f"{label} must use canonical JSON encoding")
    return value


def _bounded_identity(value: object, field: str) -> str:
    result = str(value or "")
    if (
        not result
        or result != result.strip()
        or len(result) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in result)
    ):
        raise PaidAlertError(f"{field} is not a bounded identity")
    return result


def _campaign_token(value: object, field: str) -> str:
    result = str(value or "")
    if _TOKEN_RE.fullmatch(result) is None:
        raise PaidAlertError(f"{field} is not a canonical token")
    return result


def _digest(value: object, field: str) -> str:
    result = str(value or "")
    if _SHA256_RE.fullmatch(result) is None:
        raise PaidAlertError(f"{field} must be a lowercase SHA-256")
    return result


def paid_alert_target_sha256(*, bot_id: int, chat_id: int) -> str:
    """Hash the non-secret Telegram identity used by campaign authority."""

    if (
        type(bot_id) is not int
        or bot_id <= 0
        or type(chat_id) is not int
        or chat_id == 0
    ):
        raise PaidAlertError("paid Telegram bot/chat identity is invalid")
    return hashlib.sha256(
        _canonical_json_bytes({"bot_id": bot_id, "chat_id": chat_id})
    ).hexdigest()


def paid_alert_task_id_for_dag(dag_id: str) -> str:
    """Return the only alert task allowed to authorize one paid source DAG."""

    dag = _campaign_token(dag_id, "dag_id")
    try:
        return _PAID_ALERT_TASK_BY_DAG[dag]
    except KeyError as exc:
        raise PaidAlertError("paid alert is not authorized for this DAG") from exc


def sign_paid_alert_binding(
    unsigned: Mapping[str, Any], secret: str | bytes
) -> dict[str, Any]:
    """Create the small HMAC authority document in an offline workspace.

    The helper is intentionally deterministic so the operator can independently
    reproduce and review the artifact.  It never writes a file.
    """

    if frozenset(unsigned) != _PAID_BINDING_UNSIGNED_FIELDS:
        raise PaidAlertError("paid alert binding fields are not exact")
    body = dict(unsigned)
    if body.get("schema_version") != 1 or body.get("source") != "whoscored":
        raise PaidAlertError("paid alert binding schema/source is invalid")
    _campaign_token(body.get("campaign_id"), "campaign_id")
    _campaign_token(body.get("approval_id"), "approval_id")
    _digest(body.get("approval_sha256"), "approval_sha256")
    _digest(body.get("target_sha256"), "target_sha256")
    if body.get("signature_algorithm") != "hmac-sha256":
        raise PaidAlertError("paid alert binding signature algorithm is invalid")
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not isinstance(key, bytes) or len(key) < 32:
        raise PaidAlertError("paid alert HMAC secret must contain at least 32 bytes")
    signature = hmac.new(key, _canonical_json_bytes(body), hashlib.sha256).hexdigest()
    return {**body, "signature": signature}


def _protected_paid_alert_file(path_env: str) -> tuple[Path, bytes]:
    raw_path = str(os.environ.get(path_env) or "").strip()
    raw_root = str(os.environ.get(_PAID_ALERT_AUTHORITY_ROOT_ENV) or "").strip()
    if not raw_path or not raw_root:
        raise PaidAlertError(
            f"paid alert requires {path_env} and {_PAID_ALERT_AUTHORITY_ROOT_ENV}"
        )
    path = Path(raw_path)
    root_path = Path(raw_root)
    try:
        if not path.is_absolute() or not root_path.is_absolute():
            raise PaidAlertError("paid alert authority paths must be absolute")
        if path.is_symlink() or root_path.is_symlink():
            raise PaidAlertError("paid alert authority paths must not be symlinks")
        lexical = path.relative_to(root_path)
        if not lexical.parts or any(part in {"", ".", ".."} for part in lexical.parts):
            raise PaidAlertError("paid alert authority path is non-canonical")
        cursor = root_path
        for component in lexical.parts:
            cursor = cursor / component
            if stat.S_ISLNK(cursor.lstat().st_mode):
                raise PaidAlertError(
                    "paid alert authority path contains a symlink component"
                )
        root = root_path.resolve(strict=True)
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        metadata = resolved.stat(follow_symlinks=False)
    except PaidAlertError:
        raise
    except (OSError, ValueError) as exc:
        raise PaidAlertError("paid alert authority file is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise PaidAlertError("paid alert authority must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o222:
        raise PaidAlertError("paid alert authority file must be read-only")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(resolved, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                opened.st_dev != metadata.st_dev
                or opened.st_ino != metadata.st_ino
                or not stat.S_ISREG(opened.st_mode)
                or stat.S_IMODE(opened.st_mode) & 0o222
            ):
                raise PaidAlertError("paid alert authority changed while opening")
            chunks: list[bytes] = []
            remaining = _MAX_PAID_ALERT_FILE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
        finally:
            os.close(descriptor)
    except PaidAlertError:
        raise
    except OSError as exc:
        raise PaidAlertError("paid alert authority file cannot be read") from exc
    if not raw or len(raw) > _MAX_PAID_ALERT_FILE_BYTES:
        raise PaidAlertError("paid alert authority file has an invalid size")
    return resolved, raw


def _load_paid_telegram_target() -> dict[str, Any]:
    path, raw = _protected_paid_alert_file(_PAID_ALERT_SECRET_PATH_ENV)
    value = _strict_json_object(
        raw,
        expected_fields=_PAID_SECRET_FIELDS,
        label="paid Telegram secret",
    )
    token = value.get("bot_token")
    chat_text = value.get("chat_id")
    if value.get("schema_version") != 1 or not isinstance(token, str):
        raise PaidAlertError("paid Telegram secret schema is invalid")
    token_match = _BOT_TOKEN_RE.fullmatch(token)
    if token_match is None:
        raise PaidAlertError("paid Telegram bot token is malformed")
    if not isinstance(chat_text, str) or _CHAT_ID_RE.fullmatch(chat_text) is None:
        raise PaidAlertError("paid Telegram chat id must be a canonical integer")
    bot_id = int(token_match.group(1))
    chat_id = int(chat_text)
    if abs(chat_id) > 2**63 - 1:
        raise PaidAlertError("paid Telegram chat id is outside signed 64-bit range")
    target_sha256 = paid_alert_target_sha256(bot_id=bot_id, chat_id=chat_id)
    return {
        "bot_token": token,
        "bot_id": bot_id,
        "chat_id": chat_id,
        "target_sha256": target_sha256,
        "secret_sha256": hashlib.sha256(raw).hexdigest(),
        "secret_path_sha256": hashlib.sha256(
            str(path).encode("utf-8")
        ).hexdigest(),
    }


def _load_paid_alert_binding(
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    target_sha256: str,
) -> dict[str, str]:
    path, raw = _protected_paid_alert_file(_PAID_ALERT_BINDING_PATH_ENV)
    value = _strict_json_object(
        raw,
        expected_fields=_PAID_BINDING_FIELDS,
        label="paid alert binding",
    )
    expected = {
        "schema_version": 1,
        "source": "whoscored",
        "campaign_id": _campaign_token(campaign_id, "campaign_id"),
        "approval_id": _campaign_token(approval_id, "approval_id"),
        "approval_sha256": _digest(approval_sha256, "approval_sha256"),
        "target_sha256": _digest(target_sha256, "target_sha256"),
        "signature_algorithm": "hmac-sha256",
    }
    body = {key: value.get(key) for key in _PAID_BINDING_UNSIGNED_FIELDS}
    if body != expected:
        raise PaidAlertError("paid alert binding is outside campaign authority")
    signature = _digest(value.get("signature"), "binding.signature")
    raw_secret = str(os.environ.get(PAID_ALERT_HMAC_SECRET_ENV) or "").strip()
    if len(raw_secret.encode("utf-8")) < 32:
        raise PaidAlertError(
            f"{PAID_ALERT_HMAC_SECRET_ENV} is unavailable"
        )
    calculated = hmac.new(
        raw_secret.encode("utf-8"),
        _canonical_json_bytes(body),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, calculated):
        raise PaidAlertError("paid alert binding HMAC is invalid")
    return {
        "binding_sha256": hashlib.sha256(raw).hexdigest(),
        "binding_path_sha256": hashlib.sha256(
            str(path).encode("utf-8")
        ).hexdigest(),
    }


def _paid_alert_environment(expected: str) -> dict[str, str]:
    normalized_expected = str(expected).strip().casefold()
    actual = str(os.environ.get("ALERT_ENV") or "").strip().casefold()
    if not normalized_expected or actual != normalized_expected:
        raise PaidAlertError(
            "paid alert environment is not exact: "
            f"ALERT_ENV={actual or '<unset>'!r}, expected={normalized_expected!r}"
        )
    return {
        "alert_env": actual,
        "alert_delivery": "telegram",
    }


def _paid_preflight_message(
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    run_id: str,
    delivery_nonce: str,
) -> tuple[str, str]:
    """Return exact HTML request text and Telegram's rendered plain text."""

    campaign = _campaign_token(campaign_id, "campaign_id")
    approval = _campaign_token(approval_id, "approval_id")
    approval_digest = _digest(approval_sha256, "approval_sha256")
    run = _bounded_identity(run_id, "run_id")
    if re.fullmatch(r"[0-9a-f]{32}", delivery_nonce) is None:
        raise PaidAlertError("paid alert delivery nonce is invalid")
    rendered = (
        "WhoScored paid-proxy preflight ✅\n"
        f"Campaign: {campaign}\n"
        f"Approval: {approval}\n"
        f"Approval SHA-256: {approval_digest}\n"
        f"Run: {run}\n"
        f"Delivery nonce: {delivery_nonce}"
    )
    html = (
        "<b>WhoScored paid-proxy preflight</b> ✅\n"
        f"<b>Campaign:</b> <code>{campaign}</code>\n"
        f"<b>Approval:</b> <code>{approval}</code>\n"
        f"<b>Approval SHA-256:</b> <code>{approval_digest}</code>\n"
        f"<b>Run:</b> <code>{html_lib.escape(run, quote=False)}</code>\n"
        f"<b>Delivery nonce:</b> <code>{delivery_nonce}</code>"
    )
    return html, rendered


def _telegram_response_evidence_sha256(
    *,
    bot_id: int,
    chat_id: int,
    message_id: int,
    message_date: int,
    rendered_text: str,
) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(
            {
                "ok": True,
                "bot_id": bot_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "date": message_date,
                "text": rendered_text,
            }
        )
    ).hexdigest()


def _open_paid_telegram_request(
    request: urllib.request.Request, *, timeout: int
) -> Any:
    """Open Telegram directly, ignoring ambient HTTP(S)_PROXY variables."""

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout)


def _send_paid_telegram(
    message: str,
    *,
    rendered_text: str,
    target: Mapping[str, Any],
) -> dict[str, int | str]:
    token = str(target["bot_token"])
    expected_bot_id = int(target["bot_id"])
    expected_chat_id = int(target["chat_id"])
    url = f"{_TG_API}/bot{token}/sendMessage"
    body = urllib.parse.urlencode(
        {
            "chat_id": str(expected_chat_id),
            "text": message[:4000],
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request_started_at = int(time.time())
    try:
        request = urllib.request.Request(url, data=body, method="POST")
        with _open_paid_telegram_request(request, timeout=10) as response:
            status = response.status
            raw = response.read(_MAX_PAID_ALERT_FILE_BYTES + 1)
    except Exception:
        # HTTPError/URLError may retain the token-bearing request URL.  Do not
        # chain that object into Airflow logs.
        raise PaidAlertError("paid Telegram preflight request failed") from None
    response_received_at = int(time.time())
    if status != 200 or not raw or len(raw) > _MAX_PAID_ALERT_FILE_BYTES:
        raise PaidAlertError("paid Telegram preflight response is not HTTP 200 JSON")
    value = _strict_json_object(
        raw,
        label="paid Telegram response",
        require_canonical=False,
    )
    result = value.get("result")
    chat = result.get("chat") if isinstance(result, Mapping) else None
    sender = result.get("from") if isinstance(result, Mapping) else None
    chat_id = chat.get("id") if isinstance(chat, Mapping) else None
    bot_id = sender.get("id") if isinstance(sender, Mapping) else None
    is_bot = sender.get("is_bot") if isinstance(sender, Mapping) else None
    returned_text = result.get("text") if isinstance(result, Mapping) else None
    message_id = result.get("message_id") if isinstance(result, Mapping) else None
    message_date = result.get("date") if isinstance(result, Mapping) else None
    if (
        value.get("ok") is not True
        or type(bot_id) is not int
        or bot_id != expected_bot_id
        or is_bot is not True
        or type(chat_id) is not int
        or chat_id != expected_chat_id
        or returned_text != rendered_text
        or type(message_id) is not int
        or message_id <= 0
        or type(message_date) is not int
        or message_date <= 0
        or message_date < request_started_at - _PAID_ALERT_CLOCK_SKEW_SECONDS
        or message_date > response_received_at + _PAID_ALERT_CLOCK_SKEW_SECONDS
    ):
        raise PaidAlertError("paid Telegram response does not prove exact delivery")
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "message_date": message_date,
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "response_sha256": _telegram_response_evidence_sha256(
            bot_id=bot_id,
            chat_id=chat_id,
            message_id=message_id,
            message_date=message_date,
            rendered_text=rendered_text,
        ),
    }


def _paid_receipt_root(*, create: bool) -> Path:
    raw = str(
        os.environ.get(_PAID_ALERT_RECEIPT_ROOT_ENV)
        or _DEFAULT_PAID_ALERT_RECEIPT_ROOT
    ).strip()
    root = Path(raw)
    if not root.is_absolute():
        raise PaidAlertError("paid alert receipt root must be absolute")
    try:
        if create:
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(root, 0o700)
        resolved = root.resolve(strict=True)
        metadata = resolved.stat(follow_symlinks=False)
    except OSError as exc:
        raise PaidAlertError("paid alert receipt root is unavailable") from exc
    if root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise PaidAlertError("paid alert receipt root must be a real directory")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise PaidAlertError("paid alert receipt root must be owned mode 0700")
    return resolved


def _persist_paid_alert_receipt(receipt: Mapping[str, Any]) -> tuple[Path, str]:
    if frozenset(receipt) != _PAID_RECEIPT_FIELDS:
        raise PaidAlertError("paid alert receipt fields are not exact")
    payload = _canonical_json_bytes(dict(receipt))
    digest = hashlib.sha256(payload).hexdigest()
    root = _paid_receipt_root(create=True)
    campaign = _campaign_token(receipt.get("campaign_id"), "campaign_id")
    approval = _campaign_token(receipt.get("approval_id"), "approval_id")
    parent = root / campaign / approval
    try:
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root / campaign, 0o700)
        os.chmod(parent, 0o700)
        resolved_parent = parent.resolve(strict=True)
        resolved_parent.relative_to(root)
    except (OSError, ValueError) as exc:
        raise PaidAlertError("paid alert receipt directory is unsafe") from exc
    path = resolved_parent / f"{digest}.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o400)
    except FileExistsError:
        try:
            existing = path.read_bytes()
            metadata = path.stat(follow_symlinks=False)
        except OSError as exc:
            raise PaidAlertError("paid alert receipt cannot be reopened") from exc
        if (
            existing != payload
            or path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o400
        ):
            raise PaidAlertError("paid alert receipt address is not immutable")
    except OSError as exc:
        raise PaidAlertError("paid alert receipt cannot be created") from exc
    else:
        try:
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fchmod(descriptor, 0o400)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory = os.open(resolved_parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return path, digest


def _read_paid_alert_receipt(path_value: object, digest_value: object) -> dict[str, Any]:
    path_text = _bounded_identity(path_value, "receipt_path")
    expected_digest = _digest(digest_value, "receipt_sha256")
    path = Path(path_text)
    root = _paid_receipt_root(create=False)
    try:
        if path.is_symlink():
            raise PaidAlertError("paid alert receipt must not be a symlink")
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        metadata = resolved.stat(follow_symlinks=False)
        raw = resolved.read_bytes()
    except PaidAlertError:
        raise
    except (OSError, ValueError) as exc:
        raise PaidAlertError("paid alert receipt is unavailable") from exc
    actual_digest = hashlib.sha256(raw).hexdigest()
    if (
        resolved.name != f"{expected_digest}.json"
        or actual_digest != expected_digest
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o400
    ):
        raise PaidAlertError("paid alert receipt address/content is not exact")
    return _strict_json_object(
        raw,
        expected_fields=_PAID_RECEIPT_FIELDS,
        label="paid alert receipt",
    )


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


def validate_alert_delivery(
    *,
    expected: str = "prod",
    campaign_id: str = "",
    approval_id: str = "",
    approval_sha256: str = "",
    dag_id: str = "",
    run_id: str = "",
    alert_task_id: str = "",
) -> Dict[str, Any]:
    """Send and persist one exact Telegram proof for a paid campaign."""

    environment = _paid_alert_environment(expected)
    campaign = _campaign_token(campaign_id, "campaign_id")
    approval = _campaign_token(approval_id, "approval_id")
    approval_digest = _digest(approval_sha256, "approval_sha256")
    dag = _campaign_token(dag_id, "dag_id")
    run = _bounded_identity(run_id, "run_id")
    task = _campaign_token(alert_task_id, "alert_task_id")
    target = _load_paid_telegram_target()
    binding = _load_paid_alert_binding(
        campaign_id=campaign,
        approval_id=approval,
        approval_sha256=approval_digest,
        target_sha256=str(target["target_sha256"]),
    )
    delivery_nonce = secrets.token_hex(16)
    message, rendered_text = _paid_preflight_message(
        campaign_id=campaign,
        approval_id=approval,
        approval_sha256=approval_digest,
        run_id=run,
        delivery_nonce=delivery_nonce,
    )
    delivery = _send_paid_telegram(
        message,
        rendered_text=rendered_text,
        target=target,
    )
    receipt = {
        "schema_version": 1,
        "kind": "whoscored_paid_alert_delivery",
        "campaign_id": campaign,
        "approval_id": approval,
        "approval_sha256": approval_digest,
        "dag_id": dag,
        "run_id": run,
        "alert_task_id": task,
        "alert_env": environment["alert_env"],
        "delivery_nonce": delivery_nonce,
        "target_sha256": target["target_sha256"],
        "bot_id": target["bot_id"],
        "chat_id": target["chat_id"],
        "alert_secret_sha256": target["secret_sha256"],
        "alert_secret_path_sha256": target["secret_path_sha256"],
        "alert_binding_sha256": binding["binding_sha256"],
        "alert_binding_path_sha256": binding["binding_path_sha256"],
        "message_sha256": hashlib.sha256(message.encode("utf-8")).hexdigest(),
        "telegram_response_sha256": delivery["response_sha256"],
        "telegram_message_id": delivery["message_id"],
        "telegram_message_date": delivery["message_date"],
        "request_started_at": delivery["request_started_at"],
        "response_received_at": delivery["response_received_at"],
    }
    receipt_path, receipt_sha256 = _persist_paid_alert_receipt(receipt)
    return {
        **environment,
        "status": "delivered",
        "campaign_id": campaign,
        "approval_id": approval,
        "approval_sha256": approval_digest,
        "target_sha256": target["target_sha256"],
        "telegram_message_id": delivery["message_id"],
        "telegram_message_date": delivery["message_date"],
        "receipt_path": str(receipt_path),
        "receipt_sha256": receipt_sha256,
    }


def verify_paid_alert_receipt(
    metadata: Mapping[str, Any],
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    dag_id: str,
    run_id: str,
    alert_task_id: str,
) -> Dict[str, Any]:
    """Revalidate immutable delivery evidence and the current read-only secret."""

    if not isinstance(metadata, Mapping) or metadata.get("status") != "delivered":
        raise PaidAlertError("paid alert metadata is not a delivered receipt")
    expected_campaign = _campaign_token(campaign_id, "campaign_id")
    expected_approval = _campaign_token(approval_id, "approval_id")
    expected_approval_sha256 = _digest(approval_sha256, "approval_sha256")
    expected_dag = _campaign_token(dag_id, "dag_id")
    expected_run = _bounded_identity(run_id, "run_id")
    expected_task = _campaign_token(alert_task_id, "alert_task_id")
    current_environment = _paid_alert_environment("prod")
    receipt = _read_paid_alert_receipt(
        metadata.get("receipt_path"), metadata.get("receipt_sha256")
    )
    current_target = _load_paid_telegram_target()
    current_binding = _load_paid_alert_binding(
        campaign_id=expected_campaign,
        approval_id=expected_approval,
        approval_sha256=expected_approval_sha256,
        target_sha256=str(current_target["target_sha256"]),
    )
    delivery_nonce = str(receipt.get("delivery_nonce") or "")
    _message_html, rendered_text = _paid_preflight_message(
        campaign_id=expected_campaign,
        approval_id=expected_approval,
        approval_sha256=expected_approval_sha256,
        run_id=expected_run,
        delivery_nonce=delivery_nonce,
    )
    expected_message_sha256 = hashlib.sha256(
        _message_html.encode("utf-8")
    ).hexdigest()
    message_id = receipt.get("telegram_message_id")
    message_date = receipt.get("telegram_message_date")
    request_started_at = receipt.get("request_started_at")
    response_received_at = receipt.get("response_received_at")
    scalar_proof_is_valid = (
        type(message_id) is int
        and message_id > 0
        and type(message_date) is int
        and message_date > 0
        and type(request_started_at) is int
        and request_started_at > 0
        and type(response_received_at) is int
        and response_received_at >= request_started_at
        and message_date
        >= request_started_at - _PAID_ALERT_CLOCK_SKEW_SECONDS
        and message_date
        <= response_received_at + _PAID_ALERT_CLOCK_SKEW_SECONDS
    )
    expected_response_sha256 = (
        _telegram_response_evidence_sha256(
            bot_id=int(current_target["bot_id"]),
            chat_id=int(current_target["chat_id"]),
            message_id=message_id,
            message_date=message_date,
            rendered_text=rendered_text,
        )
        if scalar_proof_is_valid
        else ""
    )
    expected_receipt = {
        "campaign_id": expected_campaign,
        "approval_id": expected_approval,
        "approval_sha256": expected_approval_sha256,
        "dag_id": expected_dag,
        "run_id": expected_run,
        "alert_task_id": expected_task,
        "alert_env": current_environment["alert_env"],
        "delivery_nonce": delivery_nonce,
        "target_sha256": current_target["target_sha256"],
        "bot_id": current_target["bot_id"],
        "chat_id": current_target["chat_id"],
        "alert_secret_sha256": current_target["secret_sha256"],
        "alert_secret_path_sha256": current_target["secret_path_sha256"],
        "alert_binding_sha256": current_binding["binding_sha256"],
        "alert_binding_path_sha256": current_binding["binding_path_sha256"],
    }
    if (
        receipt.get("schema_version") != 1
        or receipt.get("kind") != "whoscored_paid_alert_delivery"
        or any(receipt.get(key) != value for key, value in expected_receipt.items())
        or metadata.get("campaign_id") != expected_campaign
        or metadata.get("approval_id") != expected_approval
        or metadata.get("approval_sha256") != expected_approval_sha256
        or metadata.get("target_sha256") != current_target["target_sha256"]
        or metadata.get("telegram_message_id")
        != receipt.get("telegram_message_id")
        or metadata.get("telegram_message_date")
        != receipt.get("telegram_message_date")
        or not scalar_proof_is_valid
        or receipt.get("message_sha256") != expected_message_sha256
        or receipt.get("telegram_response_sha256")
        != expected_response_sha256
        or _SHA256_RE.fullmatch(str(receipt.get("message_sha256") or "")) is None
        or _SHA256_RE.fullmatch(
            str(receipt.get("telegram_response_sha256") or "")
        )
        is None
    ):
        raise PaidAlertError(
            "paid alert receipt is outside the exact campaign/target authority"
        )
    return dict(receipt)


def _paid_alert_state_identity(
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    dag_id: str,
    run_id: str,
    alert_task_id: str,
) -> dict[str, str]:
    return {
        "campaign_id": _campaign_token(campaign_id, "campaign_id"),
        "approval_id": _campaign_token(approval_id, "approval_id"),
        "approval_sha256": _digest(approval_sha256, "approval_sha256"),
        "dag_id": _campaign_token(dag_id, "dag_id"),
        "run_id": _bounded_identity(run_id, "run_id"),
        "alert_task_id": _campaign_token(alert_task_id, "alert_task_id"),
    }


def _paid_alert_state_paths(identity: Mapping[str, str]) -> tuple[Path, Path]:
    root = _paid_receipt_root(create=True)
    parent = root / identity["campaign_id"] / identity["approval_id"]
    try:
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root / identity["campaign_id"], 0o700)
        os.chmod(parent, 0o700)
        resolved = parent.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise PaidAlertError("paid alert state directory is unsafe") from exc
    return resolved / "preflight-state.json", resolved / ".preflight.lock"


def _write_paid_alert_state(path: Path, value: Mapping[str, Any]) -> None:
    if frozenset(value) != _PAID_ALERT_STATE_FIELDS:
        raise PaidAlertError("paid alert state fields are not exact")
    payload = _canonical_json_bytes(dict(value))
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".preflight-state.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise PaidAlertError("paid alert state cannot be persisted") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_paid_alert_state(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        if path.is_symlink():
            raise PaidAlertError("paid alert state must not be a symlink")
        metadata = path.stat(follow_symlinks=False)
        raw = path.read_bytes()
    except PaidAlertError:
        raise
    except OSError as exc:
        raise PaidAlertError("paid alert state is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not raw
        or len(raw) > _MAX_PAID_ALERT_FILE_BYTES
    ):
        raise PaidAlertError("paid alert state file is unsafe")
    return _strict_json_object(
        raw,
        expected_fields=_PAID_ALERT_STATE_FIELDS,
        label="paid alert state",
    )


def _paid_alert_state_lock(path: Path):
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(descriptor, 0o600)
    handle = os.fdopen(descriptor, "a+b")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return handle


def _verified_paid_alert_state(
    state: Mapping[str, Any], identity: Mapping[str, str]
) -> dict[str, Any]:
    if (
        state.get("schema_version") != 1
        or state.get("status") != "delivered"
        or any(state.get(key) != value for key, value in identity.items())
        or not isinstance(state.get("metadata"), Mapping)
    ):
        raise PaidAlertError("paid alert preflight is not durably delivered")
    metadata = dict(state["metadata"])
    verify_paid_alert_receipt(metadata, **identity)
    return metadata


def ensure_paid_alert_delivery(
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    dag_id: str,
    run_id: str,
    alert_task_id: str,
) -> dict[str, Any]:
    """Deliver one gateway-owned preflight alert at most once.

    A durable ``sending`` marker is written before Telegram I/O.  If the process
    dies in the ambiguous send/response window, all retries fail closed instead
    of risking a duplicate alert or authorising paid traffic.
    """

    identity = _paid_alert_state_identity(
        campaign_id=campaign_id,
        approval_id=approval_id,
        approval_sha256=approval_sha256,
        dag_id=dag_id,
        run_id=run_id,
        alert_task_id=alert_task_id,
    )
    state_path, lock_path = _paid_alert_state_paths(identity)
    lock = _paid_alert_state_lock(lock_path)
    try:
        state = _read_paid_alert_state(state_path)
        if state is not None:
            return _verified_paid_alert_state(state, identity)
        _write_paid_alert_state(
            state_path,
            {
                "schema_version": 1,
                "status": "sending",
                **identity,
                "metadata": None,
            },
        )
        metadata = validate_alert_delivery(expected="prod", **identity)
        _write_paid_alert_state(
            state_path,
            {
                "schema_version": 1,
                "status": "delivered",
                **identity,
                "metadata": metadata,
            },
        )
        return _verified_paid_alert_state(
            _read_paid_alert_state(state_path) or {}, identity
        )
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def require_paid_alert_delivery(
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
    dag_id: str,
    run_id: str,
    alert_task_id: str,
) -> dict[str, Any]:
    """Require a previously delivered gateway-only preflight receipt."""

    identity = _paid_alert_state_identity(
        campaign_id=campaign_id,
        approval_id=approval_id,
        approval_sha256=approval_sha256,
        dag_id=dag_id,
        run_id=run_id,
        alert_task_id=alert_task_id,
    )
    state_path, lock_path = _paid_alert_state_paths(identity)
    lock = _paid_alert_state_lock(lock_path)
    try:
        state = _read_paid_alert_state(state_path)
        if state is None:
            raise PaidAlertError("paid alert preflight has not been delivered")
        return _verified_paid_alert_state(state, identity)
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def paid_alert_receipt_environment(metadata: Mapping[str, Any]) -> dict[str, str]:
    """Project only non-secret, exact receipt identity into one source process."""

    if not isinstance(metadata, Mapping) or metadata.get("status") != "delivered":
        raise PaidAlertError("paid alert metadata is not delivered")
    normalized: dict[str, str] = {
        "campaign_id": _campaign_token(metadata.get("campaign_id"), "campaign_id"),
        "approval_id": _campaign_token(metadata.get("approval_id"), "approval_id"),
        "approval_sha256": _digest(
            metadata.get("approval_sha256"), "approval_sha256"
        ),
        "target_sha256": _digest(metadata.get("target_sha256"), "target_sha256"),
        "receipt_path": _bounded_identity(metadata.get("receipt_path"), "receipt_path"),
        "receipt_sha256": _digest(metadata.get("receipt_sha256"), "receipt_sha256"),
    }
    for field in ("telegram_message_id", "telegram_message_date"):
        value = metadata.get(field)
        if type(value) is not int or value <= 0:
            raise PaidAlertError(f"{field} must be a positive integer")
        normalized[field] = str(value)
    return {
        environment_name: normalized[field]
        for field, environment_name in PAID_ALERT_RECEIPT_ENV.items()
    }


def paid_alert_metadata_from_environment(
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Read a complete paid receipt identity; partial/manual projection fails."""

    source = os.environ if environ is None else environ
    values: dict[str, str] = {}
    for field, environment_name in PAID_ALERT_RECEIPT_ENV.items():
        value = str(source.get(environment_name) or "")
        if not value or value != value.strip():
            raise PaidAlertError(
                f"paid source requires exact environment {environment_name}"
            )
        values[field] = value
    result: dict[str, Any] = {
        "status": "delivered",
        "campaign_id": _campaign_token(values["campaign_id"], "campaign_id"),
        "approval_id": _campaign_token(values["approval_id"], "approval_id"),
        "approval_sha256": _digest(
            values["approval_sha256"], "approval_sha256"
        ),
        "target_sha256": _digest(values["target_sha256"], "target_sha256"),
        "receipt_path": _bounded_identity(values["receipt_path"], "receipt_path"),
        "receipt_sha256": _digest(values["receipt_sha256"], "receipt_sha256"),
    }
    for field in ("telegram_message_id", "telegram_message_date"):
        raw = values[field]
        try:
            value = int(raw)
        except ValueError as exc:
            raise PaidAlertError(f"{field} is not an integer") from exc
        if value <= 0 or str(value) != raw:
            raise PaidAlertError(f"{field} is not a canonical positive integer")
        result[field] = value
    return result


def validate_paid_alert_metadata(
    metadata: Mapping[str, Any],
    *,
    campaign_id: str,
    approval_id: str,
    approval_sha256: str,
) -> dict[str, Any]:
    """Validate only the non-secret receipt address carried by a source runner.

    This check intentionally opens no authority files and reads no HMAC secret.
    The isolated paid gateway must call :func:`verify_paid_alert_receipt` before
    creating a lease.
    """

    if (
        not isinstance(metadata, Mapping)
        or frozenset(metadata) != _PAID_ALERT_METADATA_FIELDS
        or metadata.get("status") != "delivered"
    ):
        raise PaidAlertError("paid alert metadata fields are not exact")
    normalized = {
        "status": "delivered",
        "campaign_id": _campaign_token(metadata.get("campaign_id"), "campaign_id"),
        "approval_id": _campaign_token(metadata.get("approval_id"), "approval_id"),
        "approval_sha256": _digest(
            metadata.get("approval_sha256"), "approval_sha256"
        ),
        "target_sha256": _digest(metadata.get("target_sha256"), "target_sha256"),
        "receipt_path": _bounded_identity(metadata.get("receipt_path"), "receipt_path"),
        "receipt_sha256": _digest(metadata.get("receipt_sha256"), "receipt_sha256"),
    }
    for field in ("telegram_message_id", "telegram_message_date"):
        value = metadata.get(field)
        if type(value) is not int or value <= 0:
            raise PaidAlertError(f"{field} must be a positive integer")
        normalized[field] = value
    if (
        normalized["campaign_id"] != _campaign_token(campaign_id, "campaign_id")
        or normalized["approval_id"] != _campaign_token(approval_id, "approval_id")
        or normalized["approval_sha256"]
        != _digest(approval_sha256, "approval_sha256")
    ):
        raise PaidAlertError("paid alert metadata is outside campaign authority")
    return normalized


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


def _paid_receipt_cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Revalidate a WhoScored paid-alert receipt without network I/O"
    )
    parser.add_argument("--receipt-path", required=True)
    parser.add_argument("--receipt-sha256", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--approval-sha256", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--alert-task-id", required=True)
    parser.add_argument("--target-sha256", required=True)
    parser.add_argument("--telegram-message-id", required=True, type=int)
    parser.add_argument("--telegram-message-date", required=True, type=int)
    args = parser.parse_args(argv)
    metadata = {
        "status": "delivered",
        "campaign_id": args.campaign_id,
        "approval_id": args.approval_id,
        "approval_sha256": args.approval_sha256,
        "target_sha256": args.target_sha256,
        "telegram_message_id": args.telegram_message_id,
        "telegram_message_date": args.telegram_message_date,
        "receipt_path": args.receipt_path,
        "receipt_sha256": args.receipt_sha256,
    }
    verify_paid_alert_receipt(
        metadata,
        campaign_id=args.campaign_id,
        approval_id=args.approval_id,
        approval_sha256=args.approval_sha256,
        dag_id=args.dag_id,
        run_id=args.run_id,
        alert_task_id=args.alert_task_id,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through source shell
    raise SystemExit(_paid_receipt_cli())
