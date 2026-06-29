"""
Superset → Telegram Alert Bridge
================================

Superset's native alerting only ships email and Slack channels. We poll
Superset's REST API on a schedule, evaluate threshold rules against chart
metric values, and forward firing alerts to Telegram via the existing
``send_telegram_message`` helper in ``utils.alerts``.

Configuration
-------------

Default alert list lives in ``SUPERSET_ALERTS`` (this file). Override at
runtime without redeploy by setting the Airflow Variable
``superset_alerts_config`` to a JSON list with the same shape:

    [
        {
            "name": "ingestion_row_count_drop",
            "chart_id": 1,
            "metric_path": "data.0.value",
            "threshold": 100,
            "comparison": "lt",
            "message": "Bronze ingestion row count dropped..."
        }
    ]

Required environment variables (already set by compose):
    SUPERSET_URL              — default http://superset:8088 (compose service)
    SUPERSET_ADMIN_USERNAME   — default 'admin'
    SUPERSET_ADMIN_PASSWORD   — required, NEVER logged
    TELEGRAM_BOT_TOKEN        — required for alert delivery
    TELEGRAM_CHAT_ID          — required for alert delivery

Failure semantics
-----------------

The DAG MUST NOT fail when Superset is down — that would generate a
flood of "DAG failed" Telegram alerts via the on_failure callback while
Superset is being deployed/restarted. All transport-level errors are
caught and logged at WARNING; we only raise on programming errors
(bad config shape, etc).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.models import Variable

from utils.alerts import send_telegram_message
from utils.default_args import DEFAULT_ARGS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults — override via Airflow Variable `superset_alerts_config` (JSON)
# ---------------------------------------------------------------------------

SUPERSET_ALERTS: List[Dict[str, Any]] = [
    {
        "name": "ingestion_row_count_drop",
        "chart_id": 1,  # placeholder — replace once Superset chart is created
        "metric_path": "data.0.value",
        "threshold": 100,
        "comparison": "lt",
        "message": (
            "⚠️ Bronze ingestion row count dropped below 100 — check FBref scraper"
        ),
    },
    {
        "name": "silver_freshness_stale",
        "chart_id": 2,  # placeholder
        "metric_path": "data.0.value",
        "threshold": 24,  # hours since last load
        "comparison": "gt",
        "message": (
            "⚠️ Silver layer hasn't been refreshed in >24h — check Silver DAG"
        ),
    },
]

# Per-request timeout for Superset HTTP calls (seconds).
HTTP_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Superset REST helpers
# ---------------------------------------------------------------------------


def _superset_base_url() -> str:
    return os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")


def _superset_login() -> Optional[str]:
    """Login to Superset and return a JWT access token, or None on failure.

    Never logs the password. Returns None (not raises) on network errors,
    so callers can decide to skip alerts gracefully.
    """
    base = _superset_base_url()
    username = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
    password = os.environ.get("SUPERSET_ADMIN_PASSWORD")
    if not password:
        logger.warning(
            "SUPERSET_ADMIN_PASSWORD not set; cannot authenticate to Superset"
        )
        return None

    body = json.dumps(
        {
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": False,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/api/v1/security/login",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            token = payload.get("access_token")
            if not token:
                logger.warning("Superset login: response had no access_token")
                return None
            return token
    except urllib.error.URLError as e:
        # Connection refused, DNS failure, timeout — Superset down.
        logger.warning(f"Superset login transport error: {e}")
        return None
    except Exception as e:  # pragma: no cover — defensive
        logger.warning(f"Superset login unexpected error: {e}")
        return None


def _fetch_chart_data(chart_id: int, token: str) -> Optional[Dict[str, Any]]:
    """Fetch fresh data for a Superset chart. Returns None on failure.

    Uses ``force=true`` so Superset re-runs the underlying query rather
    than serving from cache — alert evaluation must reflect current data.
    """
    base = _superset_base_url()
    url = f"{base}/api/v1/chart/{chart_id}/data/?force=true"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"Chart {chart_id} not found (404), skipping")
        else:
            logger.warning(f"Chart {chart_id} HTTP {e.code}: {e.reason}")
        return None
    except urllib.error.URLError as e:
        logger.warning(f"Chart {chart_id} transport error: {e}")
        return None
    except Exception as e:  # pragma: no cover — defensive
        logger.warning(f"Chart {chart_id} unexpected error: {e}")
        return None


# ---------------------------------------------------------------------------
# Threshold evaluation
# ---------------------------------------------------------------------------


def _resolve_metric_path(payload: Dict[str, Any], path: str) -> Any:
    """Walk a dot-path like ``result.0.data.0.value`` through dicts/lists.

    Path segments that are integers index into lists; otherwise they are
    treated as dict keys. Returns None if any segment is missing.
    Superset's chart data response is wrapped in ``{"result": [...]}``,
    so paths starting with ``data.`` are auto-prefixed with ``result.0.``.
    """
    if path.startswith("data.") and "result" not in path:
        path = f"result.0.{path}"
    cursor: Any = payload
    for segment in path.split("."):
        if cursor is None:
            return None
        if segment.isdigit():
            idx = int(segment)
            if isinstance(cursor, list) and 0 <= idx < len(cursor):
                cursor = cursor[idx]
            else:
                return None
        else:
            if isinstance(cursor, dict):
                cursor = cursor.get(segment)
            else:
                return None
    return cursor


def _compare(value: float, threshold: float, comparison: str) -> bool:
    if comparison == "gt":
        return value > threshold
    if comparison == "lt":
        return value < threshold
    if comparison == "eq":
        return value == threshold
    if comparison == "gte":
        return value >= threshold
    if comparison == "lte":
        return value <= threshold
    raise ValueError(f"Unknown comparison operator: {comparison!r}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="superset_alerts",
    description="Poll Superset REST API and forward firing alerts to Telegram.",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 25),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "owner": "airflow", "retries": 0},
    tags=["superset", "alerts", "monitoring"],
)
def superset_alerts():
    @task
    def check_alerts() -> Dict[str, int]:
        """Evaluate every configured alert; emit Telegram on threshold breach.

        Returns a small summary dict (checked / fired / skipped) so it
        shows up in XCom for ad-hoc inspection.
        """
        # Allow runtime override without code change.
        try:
            alerts: List[Dict[str, Any]] = Variable.get(
                "superset_alerts_config",
                deserialize_json=True,
                default_var=SUPERSET_ALERTS,
            )
        except Exception as e:
            logger.warning(
                f"Failed to read Variable superset_alerts_config ({e}); "
                "falling back to in-DAG defaults"
            )
            alerts = SUPERSET_ALERTS

        if not alerts:
            logger.info("No alerts configured — nothing to do")
            return {"checked": 0, "fired": 0, "skipped": 0}

        token = _superset_login()
        if not token:
            # Superset down or misconfigured — degrade gracefully so that
            # we don't fire spurious failure callbacks every 15 minutes.
            logger.warning(
                "Cannot reach Superset; skipping all alert checks for this run"
            )
            return {"checked": 0, "fired": 0, "skipped": len(alerts)}

        checked = 0
        fired = 0
        skipped = 0

        for alert in alerts:
            name = alert.get("name", "<unnamed>")
            chart_id = alert.get("chart_id")
            metric_path = alert.get("metric_path", "data.0.value")
            threshold = alert.get("threshold")
            comparison = alert.get("comparison", "lt")
            message = alert.get(
                "message", f"Superset alert '{name}' fired"
            )

            if chart_id is None or threshold is None:
                logger.warning(
                    f"Alert '{name}' missing chart_id/threshold — skipping"
                )
                skipped += 1
                continue

            payload = _fetch_chart_data(chart_id, token)
            if payload is None:
                # Already logged inside _fetch_chart_data.
                skipped += 1
                continue

            raw_value = _resolve_metric_path(payload, metric_path)
            if raw_value is None:
                logger.warning(
                    f"Alert '{name}': metric_path '{metric_path}' did not "
                    f"resolve to a value in chart {chart_id} payload"
                )
                skipped += 1
                continue

            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                logger.warning(
                    f"Alert '{name}': non-numeric value {raw_value!r} at "
                    f"path '{metric_path}'"
                )
                skipped += 1
                continue

            checked += 1
            try:
                triggered = _compare(value, float(threshold), comparison)
            except ValueError as e:
                logger.warning(f"Alert '{name}': {e}")
                skipped += 1
                continue

            if triggered:
                logger.info(
                    f"Alert '{name}' FIRED: value={value} "
                    f"{comparison} threshold={threshold}"
                )
                body = (
                    f"<b>Alert:</b> <code>{name}</code>\n"
                    f"<b>Chart:</b> {chart_id}\n"
                    f"<b>Value:</b> {value} ({comparison} {threshold})\n"
                    f"{message}"
                )
                ok = send_telegram_message(body, level="warning")
                if not ok:
                    logger.error(
                        f"Telegram delivery failed for alert '{name}'"
                    )
                fired += 1
            else:
                logger.info(
                    f"Alert '{name}' OK: value={value} not "
                    f"{comparison} threshold={threshold}"
                )

        logger.info(
            f"Superset alert sweep complete: checked={checked} "
            f"fired={fired} skipped={skipped} total={len(alerts)}"
        )
        return {"checked": checked, "fired": fired, "skipped": skipped}

    check_alerts()


dag = superset_alerts()
