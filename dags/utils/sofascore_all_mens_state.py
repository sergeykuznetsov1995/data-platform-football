"""Durable newest-first planning state for the SofaScore all-men campaign."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from scrapers.sofascore.workload_plan import (
    WorkloadPlanError,
    production_season_shape,
    season_workload_class,
    team_count_band,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - production is Linux
    fcntl = None


logger = logging.getLogger(__name__)

STATE_SCHEMA_VERSION = 1
FAILURES_SCHEMA_VERSION = 1
DEFAULT_FIRST_START_YEAR = 2025
DEFAULT_MAX_SCOPE_ATTEMPTS = 3


class CampaignPlanningError(ValueError):
    """The frozen campaign cannot produce a safe next capture batch."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id", None)
    return hashlib.sha256(_canonical_json(unsigned)).hexdigest()


def campaign_scope_key(
    campaign_id: str, tournament_id: int, source_season_id: int
) -> str:
    return f"{campaign_id}:{int(tournament_id)}:{int(source_season_id)}"


def plan_historical_batch(
    snapshot: Mapping[str, Any],
    *,
    completed: Iterable[str],
    batch_size: int = 1,
    first_start_year: int = DEFAULT_FIRST_START_YEAR,
    snapshot_path: str = "/opt/airflow/runtime/sofascore/all-men/snapshot.json",
    policy_path: str = "/opt/airflow/configs/sofascore/all_mens_campaign.json",
    result_dir: str = "/opt/airflow/logs/sofascore-all-men/results",
    workload_artifact: str = (
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json"
    ),
    metadata_budget_bytes: int = 64 * 1024 * 1024,
    dag_run_id: str = "manual",
    authorized_season_classes: Mapping[str, Iterable[int | str]] | None = None,
    task_env: Mapping[str, str] | None = None,
    failures: Mapping[str, Mapping[str, Any]] | None = None,
    max_scope_attempts: int = DEFAULT_MAX_SCOPE_ATTEMPTS,
) -> list[dict[str, str]]:
    """Select a bounded batch: every tournament's newest season, then deeper.

    Candidates inside the allowed waves (``start_year <= first_start_year``)
    are ranked by ``(depth, -start_year, tournament_id)`` where ``depth`` is
    the season's position in the tournament's newest-first season list. A
    pending season yields one serialized metadata task for its wave; a
    deferred (unmeasured) season is skipped and holds the deeper seasons of
    its tournament until a later canary wave unlocks the shape.

    ``failures`` is the campaign's failure memory (see ``read_failures``): a
    scope with ``count >= max_scope_attempts`` is parked like a deferred one
    instead of retrying at the head of the queue forever.

    ``task_env`` is the lane's own environment (gateway URL, rate limit) and
    is forwarded verbatim to every planned task; campaign keys win over it.
    """

    lane_env = {str(key): str(value) for key, value in (task_env or {}).items()}
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise CampaignPlanningError("batch_size must be a positive integer")
    if (
        isinstance(max_scope_attempts, bool)
        or not isinstance(max_scope_attempts, int)
        or max_scope_attempts < 1
    ):
        raise CampaignPlanningError("max_scope_attempts must be a positive integer")
    if (
        isinstance(metadata_budget_bytes, bool)
        or not isinstance(metadata_budget_bytes, int)
        or metadata_budget_bytes < 1
    ):
        raise CampaignPlanningError("metadata_budget_bytes must be positive")
    snapshot_id = str(snapshot.get("snapshot_id") or "")
    if not snapshot_id or snapshot_id != _snapshot_digest(snapshot):
        raise CampaignPlanningError("campaign snapshot digest mismatch")
    campaign_id = str(snapshot.get("campaign_id") or "")
    if not campaign_id:
        raise CampaignPlanningError("campaign_id is missing")
    tournaments = snapshot.get("tournaments")
    if not isinstance(tournaments, list):
        raise CampaignPlanningError("campaign tournaments must be a list")
    completed_keys = {str(value) for value in completed}
    authorized: dict[str, frozenset[str]] | None = None
    if authorized_season_classes is not None:
        authorized = {
            str(name): frozenset(str(value) for value in tournaments)
            for name, tournaments in authorized_season_classes.items()
        }
    # (rank, kind, tournament, season); kind in ready/completed/pending/deferred
    ranked: list[tuple[tuple[int, int, int], str, Mapping[str, Any], Mapping[str, Any]]] = []
    for tournament in tournaments:
        if not isinstance(tournament, Mapping):
            raise CampaignPlanningError("campaign tournament must be an object")
        tournament_status = str(tournament.get("metadata_status") or "pending")
        if tournament_status == "excluded":
            continue
        seasons = tournament.get("seasons")
        if not isinstance(seasons, list):
            raise CampaignPlanningError("campaign seasons must be a list")
        chain: list[tuple[int, Mapping[str, Any]]] = []
        for season in seasons:
            if not isinstance(season, Mapping):
                raise CampaignPlanningError("campaign season must be an object")
            try:
                wave = int(season.get("start_year"))
            except (TypeError, ValueError) as exc:
                raise CampaignPlanningError("season start_year must be an integer") from exc
            if wave < 0 or wave > int(first_start_year):
                continue
            if str(season.get("metadata_status") or "pending") == "excluded":
                continue
            chain.append((wave, season))
        chain.sort(key=lambda item: -item[0])
        for depth, (wave, season) in enumerate(chain):
            status = str(season.get("metadata_status") or "pending")
            kind = "ready"
            if tournament_status != "ready" or status != "ready":
                kind = "pending"
            else:
                key = campaign_scope_key(
                    campaign_id,
                    int(tournament["unique_tournament_id"]),
                    int(season["source_season_id"]),
                )
                attempts = (failures or {}).get(key) or {}
                if key in completed_keys:
                    kind = "completed"
                elif int(attempts.get("count", 0)) >= max_scope_attempts:
                    kind = "parked"
                    logger.warning(
                        "campaign scope %s parked after %s failed attempts "
                        "(last run %s)",
                        key, attempts.get("count"), attempts.get("last_run_id"),
                    )
                elif authorized is not None:
                    source_format = str(season.get("season_format") or "")
                    capture_format = {
                        "split_year": "split_year",
                        "single_year": "calendar_year",
                    }.get(source_format, source_format)
                    try:
                        shape = production_season_shape(
                            season_format=capture_format,
                            team_count_band=team_count_band(
                                season.get("team_count")
                            ),
                            max_pages_per_direction=50,
                        )
                        class_name = season_workload_class(shape)
                        measured = authorized.get(class_name)
                    except WorkloadPlanError:
                        measured = None
                    tournament_id = str(tournament.get("unique_tournament_id"))
                    if measured is None or (
                        tournament_id not in measured and len(measured) < 2
                    ):
                        kind = "deferred"
            rank = (depth, -wave, int(tournament["unique_tournament_id"]))
            ranked.append((rank, kind, tournament, season))
            if kind not in ("ready", "completed"):
                # Deeper seasons of this tournament wait behind the blocker.
                break
    ranked.sort(key=lambda item: item[0])
    planned: list[dict[str, str]] = []
    for _rank, kind, tournament, season in ranked:
        if kind in ("completed", "deferred", "parked"):
            continue
        if kind == "pending":
            if planned:
                break
            wave = int(season["start_year"])
            safe_run = hashlib.sha256(
                f"{dag_run_id}:metadata:{campaign_id}:{wave}".encode("utf-8")
            ).hexdigest()[:20]
            return [{
                **lane_env,
                "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
                "SOFASCORE_CAMPAIGN_ACTION": "metadata",
                "SOFASCORE_CAMPAIGN_SNAPSHOT": snapshot_path,
                "SOFASCORE_ALL_MENS_POLICY": policy_path,
                "SOFASCORE_EXPECTED_SNAPSHOT_ID": snapshot_id,
                "SOFASCORE_EXPECTED_CAMPAIGN_ID": campaign_id,
                "SOFASCORE_METADATA_WAVE": str(wave),
                "SOFASCORE_METADATA_BUDGET_BYTES": str(metadata_budget_bytes),
                "SOFASCORE_SCOPE_RESULT_PATH": str(
                    Path(result_dir) / f"{safe_run}.json"
                ),
                "SOFASCORE_SCOPE_OUTPUT_DIR": str(
                    Path(result_dir) / safe_run
                ),
            }]
        tournament_id = int(tournament["unique_tournament_id"])
        season_id = int(season["source_season_id"])
        scope_key = campaign_scope_key(campaign_id, tournament_id, season_id)
        safe_run = hashlib.sha256(
            f"{dag_run_id}:{scope_key}".encode("utf-8")
        ).hexdigest()[:20]
        planned.append({
            **lane_env,
            "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
            "SOFASCORE_CAMPAIGN_ACTION": "capture",
            "SOFASCORE_CAMPAIGN_SNAPSHOT": snapshot_path,
            "SOFASCORE_ALL_MENS_POLICY": policy_path,
            "SOFASCORE_EXPECTED_SNAPSHOT_ID": snapshot_id,
            "SOFASCORE_EXPECTED_CAMPAIGN_ID": campaign_id,
            "SOFASCORE_TOURNAMENT_ID": str(tournament_id),
            "SOFASCORE_SOURCE_SEASON_ID": str(season_id),
            "SOFASCORE_CANONICAL_SEASON": str(
                season["canonical_season"]
            ),
            "SOFASCORE_SCOPE_KEY": scope_key,
            "SOFASCORE_SCOPE_RESULT_PATH": str(
                Path(result_dir) / f"{safe_run}.json"
            ),
            "SOFASCORE_SCOPE_OUTPUT_DIR": str(
                Path(result_dir) / safe_run
            ),
            "SOFASCORE_WORKLOAD_ARTIFACT": workload_artifact,
            # The gateway ledger holds one immutable plan per run_id,
            # so scopes of one DagRun must not share it (batch > 1).
            # An Airflow retry keeps the same id and reuses the plan.
            "SOFASCORE_SCOPE_RUN_ID": (
                f"{dag_run_id}--{tournament_id}-{season_id}"
            ),
        })
        if len(planned) == batch_size:
            break
    return planned


def read_snapshot(
    path: str | Path, *, policy_path: str | Path | None = None
) -> Mapping[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignPlanningError(f"cannot read campaign snapshot: {exc}") from exc
    if not isinstance(value, Mapping):
        raise CampaignPlanningError("campaign snapshot root must be an object")
    if policy_path is not None:
        try:
            policy = json.loads(Path(policy_path).read_text(encoding="utf-8"))
            from scrapers.sofascore.all_mens_campaign import (
                validate_campaign_snapshot,
            )
            validate_campaign_snapshot(value, policy)
        except Exception as exc:
            raise CampaignPlanningError(
                f"campaign snapshot does not match policy: {exc}"
            ) from exc
    return value


def read_completed(path: str | Path, *, campaign_id: str) -> set[str]:
    source = Path(path)
    if not source.exists():
        return set()
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignPlanningError(f"cannot read campaign state: {exc}") from exc
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != STATE_SCHEMA_VERSION
        or value.get("campaign_id") != campaign_id
        or not isinstance(value.get("completed"), list)
    ):
        raise CampaignPlanningError("campaign state does not match the snapshot")
    return {str(item) for item in value["completed"]}


@contextmanager
def _state_lock(path: Path):
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_document_atomically(destination: Path, document: Mapping[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=str(destination.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(json.dumps(
                document, ensure_ascii=False, indent=2
            ).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def mark_completed(path: str | Path, *, campaign_id: str, scope_key: str) -> None:
    destination = Path(path)
    with _state_lock(destination):
        completed = read_completed(destination, campaign_id=campaign_id)
        completed.add(str(scope_key))
        _write_document_atomically(destination, {
            "schema_version": STATE_SCHEMA_VERSION,
            "campaign_id": campaign_id,
            "completed": sorted(completed),
        })


def read_failures(path: str | Path, *, campaign_id: str) -> dict[str, dict[str, Any]]:
    """Failure memory: ``{scope_key: {"count", "last_run_id", "last_at"}}``.

    Lives in ``failures.json`` next to ``state.json`` (which stays at schema
    v1 untouched); a missing file means no failures.
    """

    source = Path(path)
    if not source.exists():
        return {}
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignPlanningError(f"cannot read campaign failures: {exc}") from exc
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != FAILURES_SCHEMA_VERSION
        or value.get("campaign_id") != campaign_id
        or not isinstance(value.get("attempts"), Mapping)
    ):
        raise CampaignPlanningError("campaign failures do not match the snapshot")
    return {str(key): dict(item) for key, item in value["attempts"].items()}


def _write_failures(
    destination: Path, *, campaign_id: str, attempts: Mapping[str, Mapping[str, Any]]
) -> None:
    _write_document_atomically(destination, {
        "schema_version": FAILURES_SCHEMA_VERSION,
        "campaign_id": campaign_id,
        "attempts": {key: attempts[key] for key in sorted(attempts)},
    })


def mark_failed(
    path: str | Path, *, campaign_id: str, scope_key: str, run_id: str
) -> None:
    destination = Path(path)
    with _state_lock(destination):
        attempts = read_failures(destination, campaign_id=campaign_id)
        previous = attempts.get(str(scope_key)) or {}
        attempts[str(scope_key)] = {
            "count": int(previous.get("count", 0)) + 1,
            "last_run_id": str(run_id),
            "last_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_failures(destination, campaign_id=campaign_id, attempts=attempts)


def clear_failed(path: str | Path, *, campaign_id: str, scope_key: str) -> None:
    destination = Path(path)
    with _state_lock(destination):
        attempts = read_failures(destination, campaign_id=campaign_id)
        if attempts.pop(str(scope_key), None) is None:
            return
        _write_failures(destination, campaign_id=campaign_id, attempts=attempts)


__all__ = [
    "CampaignPlanningError",
    "campaign_scope_key",
    "clear_failed",
    "mark_completed",
    "mark_failed",
    "plan_historical_batch",
    "read_completed",
    "read_failures",
    "read_snapshot",
]
