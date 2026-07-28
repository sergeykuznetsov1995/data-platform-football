"""Shared task helpers for the Understat current and history DAGs.

Keep these helpers outside either DAG module. Importing one DAG file from
another makes Airflow discover the imported DAG object under both file paths
and reject it as a duplicate DAG id.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)

RUNNER = "dags/scripts/run_understat_scraper.py"
RESULT_ROOT = "/tmp"
TERMINAL_CURRENT_STATUSES = frozenset(
    {"complete", "upstream_pending", "not_published"}
)

_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
}


def _scope_value(scope: Any, name: str) -> Any:
    """Read a catalog scope from either its dataclass or mapping form."""

    if isinstance(scope, Mapping):
        return scope[name]
    return getattr(scope, name)


def _safe_token(value: Any) -> str:
    """Make a deterministic filename token without accepting path syntax."""

    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return token or "unknown"


def scope_environment(
    scope: Any,
    *,
    mode: str,
    run_id: str,
) -> dict[str, str]:
    """Convert one discovered source scope to a mapped runner environment."""

    league = str(_scope_value(scope, "league"))
    season = str(_scope_value(scope, "season"))
    source_season_id = str(_scope_value(scope, "source_season_id"))
    source_discovered = _scope_value(scope, "discovered")
    if not league or not season or not source_season_id:
        raise AirflowException(f"Understat catalog returned an invalid scope: {scope!r}")
    if not isinstance(source_discovered, bool):
        raise AirflowException(
            "Understat catalog scope must carry a boolean discovered flag: "
            f"{scope!r}"
        )
    if not re.fullmatch(r"\d{4}", season):
        raise AirflowException(
            f"Understat season must be a canonical four-digit slug, got {season!r}"
        )
    try:
        source_year = int(source_season_id)
    except ValueError as exc:
        raise AirflowException(
            f"Understat source season id must be an integer year, got {source_season_id!r}"
        ) from exc
    expected_slug = f"{source_year % 100:02d}{(source_year + 1) % 100:02d}"
    if season != expected_slug:
        raise AirflowException(
            "Understat season must be the canonical four-digit slug for its "
            f"source id: expected {expected_slug!r}, got {season!r}"
        )

    result_name = "understat_{}_{}_{}_{}.json".format(
        _safe_token(mode),
        _safe_token(run_id),
        _safe_token(league),
        season,
    )
    return {
        **_TASK_ENV,
        "UNDERSTAT_MODE": str(mode),
        "UNDERSTAT_LEAGUE": league,
        "UNDERSTAT_SEASON_SLUG": season,
        "UNDERSTAT_SOURCE_SEASON_ID": source_season_id,
        "UNDERSTAT_SOURCE_DISCOVERED": (
            "true" if source_discovered else "false"
        ),
        "UNDERSTAT_RESULT_PATH": str(Path(RESULT_ROOT) / result_name),
    }


def _deduplicate_scopes(scopes: Iterable[Any]) -> list[Any]:
    """Fail closed on conflicting discovery while removing exact repeats."""

    selected: dict[tuple[str, str], Any] = {}
    source_ids: dict[tuple[str, str], str] = {}
    for scope in scopes:
        key = (
            str(_scope_value(scope, "league")),
            str(_scope_value(scope, "season")),
        )
        source_id = str(_scope_value(scope, "source_season_id"))
        if key in source_ids and source_ids[key] != source_id:
            raise AirflowException(
                "Understat discovery returned conflicting source season ids "
                f"for {key!r}: {source_ids[key]!r} and {source_id!r}"
            )
        source_ids[key] = source_id
        selected[key] = scope
    return [selected[key] for key in sorted(selected)]


def _close_understat_client(client: Any) -> None:
    """Close an explicit client/session without requiring a context protocol."""

    close = getattr(client, "close", None)
    if not callable(close):
        close = getattr(getattr(client, "session", None), "close", None)
    if callable(close):
        close()


def _load_result(path: str) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except FileNotFoundError as exc:
        raise AirflowException(f"Understat result file is missing: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"Understat result file is invalid: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AirflowException(f"Understat result must be an object: {path}")
    return value


def validate_scope_result(**context: Any) -> dict[str, Any]:
    """Validate runner identity and terminal state for one exact mapped scope.

    Row/key/cross-entity DQ and the publication manifest are enforced inside
    the runner before it reports ``complete``. This hook prevents a stale or
    colliding result artifact from satisfying a different Airflow scope and
    keeps expected next-season source absence observable without failing the
    current-data DAG.
    """

    path = str(context["UNDERSTAT_RESULT_PATH"])
    report = _load_result(path)
    expected = {
        "league": str(context["UNDERSTAT_LEAGUE"]),
        "season": str(context["UNDERSTAT_SEASON_SLUG"]),
        "source_season_id": str(context["UNDERSTAT_SOURCE_SEASON_ID"]),
    }
    actual = {
        "league": str(report.get("league") or ""),
        "season": str(report.get("season") or ""),
        "source_season_id": str(report.get("source_season_id") or ""),
    }
    if actual != expected:
        raise AirflowException(
            f"Understat result scope mismatch: expected={expected!r}, actual={actual!r}"
        )

    status = str(report.get("status") or "").strip().casefold()
    mode = str(context.get("UNDERSTAT_MODE") or "current").strip().casefold()
    accepted_statuses = (
        TERMINAL_CURRENT_STATUSES
        if mode == "current"
        else frozenset({"complete"})
    )
    if status not in accepted_statuses:
        raise AirflowException(
            f"Understat scope {expected!r} did not reach a valid terminal state: "
            f"status={status!r}, errors={report.get('errors')!r}"
        )

    from scrapers.understat.manifest import (
        CONTRACT_VERSION,
        ScopeKey,
        validate_scope_attempt_result,
    )

    expected_scope = ScopeKey(
        league=expected["league"],
        season=expected["season"],
        source_season_id=expected["source_season_id"],
    )
    try:
        attempt = validate_scope_attempt_result(
            report,
            expected_scope=expected_scope,
            accepted_statuses=accepted_statuses,
            contract_version=CONTRACT_VERSION,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowException(
            f"Understat scope publication evidence is invalid for {expected!r}: {exc}"
        ) from exc
    if attempt.status.value != status:
        raise AirflowException(
            "Understat top-level status disagrees with scope attempt: "
            f"summary={status!r}, attempt={attempt.status.value!r}"
        )
    if str(attempt.scope.source_season_id) != expected["source_season_id"]:
        raise AirflowException(
            "Understat scope result source season mismatch: "
            f"expected={expected['source_season_id']!r}, "
            f"actual={attempt.scope.source_season_id!r}"
        )
    if str(report.get("batch_id") or "") != attempt.batch_id:
        raise AirflowException(
            "Understat top-level batch_id disagrees with scope attempt: "
            f"summary={report.get('batch_id')!r}, attempt={attempt.batch_id!r}"
        )
    if report.get("errors"):
        raise AirflowException(
            f"Understat scope {expected!r} reported errors: {report['errors']!r}"
        )

    logger.info(
        "Understat exact scope validated: league=%s season=%s status=%s rows=%s",
        expected["league"],
        expected["season"],
        status,
        report.get("row_counts", {}),
    )
    return report


__all__ = ["RUNNER", "scope_environment", "validate_scope_result"]
