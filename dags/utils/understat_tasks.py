"""Shared task helpers for the Understat current and history DAGs.

Keep these helpers outside either DAG module. Importing one DAG file from
another makes Airflow discover the imported DAG object under both file paths
and reject it as a duplicate DAG id.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Iterable, Mapping

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.bash import BashOperator


logger = logging.getLogger(__name__)

RUNNER = "dags/scripts/run_understat_scraper.py"
TERMINAL_CURRENT_STATUSES = frozenset(
    {"complete", "upstream_pending", "not_published"}
)

_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
}


def validate_understat_leagues(configured: Iterable[str]) -> None:
    """The source catalog owns league membership; config is only checked."""
    from scrapers.understat.catalog import PRODUCTION_LEAGUES

    expected = frozenset(PRODUCTION_LEAGUES)
    actual = frozenset(configured)
    if actual != expected:
        raise AirflowFailException(
            "Understat UNDERSTAT_LEAGUES disagrees with the source catalog; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}; "
            f"expected={sorted(expected)}, configured={sorted(actual)}"
        )


def _scope_value(scope: Any, name: str) -> Any:
    """Read a catalog scope from either its dataclass or mapping form."""

    if isinstance(scope, Mapping):
        return scope[name]
    return getattr(scope, name)


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
        raise AirflowFailException(f"Understat catalog returned an invalid scope: {scope!r}")
    if not isinstance(source_discovered, bool):
        raise AirflowFailException(
            "Understat catalog scope must carry a boolean discovered flag: "
            f"{scope!r}"
        )
    if not re.fullmatch(r"\d{4}", season):
        raise AirflowFailException(
            f"Understat season must be a canonical four-digit slug, got {season!r}"
        )
    try:
        source_year = int(source_season_id)
    except ValueError as exc:
        raise AirflowFailException(
            f"Understat source season id must be an integer year, got {source_season_id!r}"
        ) from exc
    expected_slug = f"{source_year % 100:02d}{(source_year + 1) % 100:02d}"
    if season != expected_slug:
        raise AirflowFailException(
            "Understat season must be the canonical four-digit slug for its "
            f"source id: expected {expected_slug!r}, got {season!r}"
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
        "UNDERSTAT_RUN_ID": str(run_id),
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
            raise AirflowFailException(
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


def _validate_result_identity(
    report: Any, context: Mapping[str, Any],
) -> dict[str, str]:
    """Even a retryable failure must belong to this exact mapped scope."""
    if not isinstance(report, dict):
        raise AirflowFailException("Understat result must be an object")
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
        raise AirflowFailException(
            f"Understat result scope mismatch: expected={expected!r}, actual={actual!r}"
        )

    return expected


def validate_scope_result(report: dict[str, Any], **context: Any) -> dict[str, Any]:
    """Validate runner identity and terminal state for one exact mapped scope.

    Row/key/cross-entity DQ and the publication manifest are enforced inside
    the runner before it reports ``complete``. This hook prevents a stale or
    colliding result artifact from satisfying a different Airflow scope and
    keeps expected next-season source absence observable without failing the
    current-data DAG.
    """

    expected = _validate_result_identity(report, context)

    status = str(report.get("status") or "").strip().casefold()
    mode = str(context.get("UNDERSTAT_MODE") or "current").strip().casefold()
    accepted_statuses = (
        TERMINAL_CURRENT_STATUSES
        if mode == "current"
        else frozenset({"complete"})
    )
    if status not in accepted_statuses:
        raise AirflowFailException(
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
        raise AirflowFailException(
            f"Understat scope publication evidence is invalid for {expected!r}: {exc}"
        ) from exc
    if attempt.status.value != status:
        raise AirflowFailException(
            "Understat top-level status disagrees with scope attempt: "
            f"summary={status!r}, attempt={attempt.status.value!r}"
        )
    if str(attempt.scope.source_season_id) != expected["source_season_id"]:
        raise AirflowFailException(
            "Understat scope result source season mismatch: "
            f"expected={expected['source_season_id']!r}, "
            f"actual={attempt.scope.source_season_id!r}"
        )
    if str(report.get("batch_id") or "") != attempt.batch_id:
        raise AirflowFailException(
            "Understat top-level batch_id disagrees with scope attempt: "
            f"summary={report.get('batch_id')!r}, attempt={attempt.batch_id!r}"
        )
    if report.get("errors"):
        raise AirflowFailException(
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


class UnderstatScopeOperator(BashOperator):
    """Run, validate and return one scope as XCom without a worker-local file."""

    def execute(self, context: Any) -> dict[str, Any]:
        environment = self.get_env(context)
        # An explicit cwd avoids SubprocessHook's default temporary directory.
        # The inherited on_kill terminates this hook's process group.
        result = self.subprocess_hook.run_command(
            command=["bash", "-c", self.bash_command],
            env=environment,
            output_encoding="utf-8",
            cwd=self.cwd,
        )
        if result.exit_code not in (0, 1):
            raise AirflowFailException(
                f"Understat non-retryable failure (exit {result.exit_code}): {result.output}"
            )
        try:
            report = json.loads(result.output)
        except (TypeError, ValueError) as exc:
            raise AirflowFailException("Understat runner returned invalid JSON") from exc
        if result.exit_code == 1:
            _validate_result_identity(report, environment)
            if report.get("status") != "retryable_failure":
                raise AirflowFailException("Understat exit 1 has no confirmed retryable failure")
            raise AirflowException(f"Understat temporary failure: {result.output}")
        return validate_scope_result(report, **environment)


__all__ = ["RUNNER", "UnderstatScopeOperator", "scope_environment", "validate_scope_result"]
