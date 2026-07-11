"""Durable, bounded queue for source-discovered FBref competitions.

The queue plan is immutable.  Each competition gets its own commit manifest,
so a crash can only make that one item run again.  Raw pages are still reused
by :class:`FBrefDiscoveryService` on the retry.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict, List, Optional, Sequence

from scrapers.fbref.discovery import (
    DISCOVERY_PARSER_VERSION,
    CompetitionRef,
)
from scrapers.fbref.discovery_service import (
    DiscoveryRunResult,
    FBrefDiscoveryService,
)
from scrapers.fbref.raw_store import (
    RawPageCorrupt,
    RawPageStore,
    competition_index_target,
    utc_now_iso,
)


DISCOVERY_QUEUE_VERSION = "fbref-discovery-queue-v1"
MAX_QUEUE_COMPETITIONS_PER_RUN = 25
MAX_QUEUE_SEASONS_PER_COMPETITION = 5
MAX_QUEUE_NETWORK_PAGES = 100
MAX_QUEUE_ATTEMPTS = 10
_ITEM_STATUSES = {"complete", "error"}


class DiscoveryQueueError(RuntimeError):
    """Base error for an invalid or incompatible discovery queue."""


class DiscoveryQueueScopeMismatch(DiscoveryQueueError):
    """An existing queue was opened with a different immutable scope."""


@dataclass
class DiscoveryQueueRun:
    """One bounded queue slice and its serializable progress metadata."""

    result: DiscoveryRunResult
    queue: Dict[str, object]


def _jsonable(value):
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _competition_payload(competition: CompetitionRef) -> dict:
    return _jsonable(asdict(competition))


def _competition_sort_key(competition: CompetitionRef) -> tuple:
    competition_id = competition.competition_id
    if competition_id.isdecimal():
        return (0, int(competition_id), competition_id)
    return (1, competition_id.casefold(), competition_id)


def _payload_fingerprint(payload: Sequence[dict]) -> str:
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _snapshot_fingerprint(competitions: Sequence[CompetitionRef]) -> str:
    return _payload_fingerprint([
        _competition_payload(item) for item in competitions
    ])


def _dedupe(values: Sequence, key) -> list:
    output = []
    seen = set()
    for value in values:
        identity = key(value)
        if identity in seen:
            continue
        seen.add(identity)
        output.append(value)
    return output


class FBrefDiscoveryQueue:
    """Process all index competitions over repeatable, bounded invocations.

    The manual CLI holds a process lock. Direct callers must also serialize
    runs that use the same raw store and queue id.
    """

    def __init__(
        self,
        raw_store: RawPageStore,
        service: FBrefDiscoveryService,
    ) -> None:
        self.raw_store = raw_store
        self.service = service

    @staticmethod
    def _scope(
        *,
        max_seasons_per_competition: int,
    ) -> dict:
        return {
            "max_seasons_per_competition": max_seasons_per_competition,
        }

    def _create_plan(
        self,
        queue_id: str,
        competitions: Sequence[CompetitionRef],
        *,
        index_content_hash: str,
        scope: dict,
    ) -> dict:
        ordered = sorted(competitions, key=_competition_sort_key)
        competition_ids = [item.competition_id for item in ordered]
        if not competition_ids:
            raise DiscoveryQueueError("Cannot create a queue from an empty index")
        if len(competition_ids) != len(set(competition_ids)):
            raise DiscoveryQueueError("Competition ids in the queue are not unique")
        plan = {
            "manifest_version": DISCOVERY_QUEUE_VERSION,
            "queue_id": queue_id,
            "created_at": utc_now_iso(),
            "index_target_id": competition_index_target().target_id,
            "index_content_hash": index_content_hash,
            "parser_version": DISCOVERY_PARSER_VERSION,
            "snapshot_fingerprint": _snapshot_fingerprint(ordered),
            "scope": scope,
            "competition_ids": competition_ids,
            "competitions": [_competition_payload(item) for item in ordered],
        }
        self.raw_store.write_discovery_queue_plan(queue_id, plan)
        return plan

    def _validate_plan(
        self,
        plan: dict,
        queue_id: str,
        *,
        index_content_hash: str,
        competitions: Sequence[CompetitionRef],
        scope: dict,
    ) -> None:
        if plan.get("manifest_version") != DISCOVERY_QUEUE_VERSION:
            raise RawPageCorrupt("Invalid FBref discovery queue plan version")
        if plan.get("queue_id") != queue_id:
            raise RawPageCorrupt("FBref discovery queue id does not match its key")
        if plan.get("scope") != scope:
            raise DiscoveryQueueScopeMismatch(
                "Queue scope differs from the existing plan; use a new --queue-id"
            )
        if plan.get("parser_version") != DISCOVERY_PARSER_VERSION:
            raise DiscoveryQueueScopeMismatch(
                "The discovery parser changed; use a new --queue-id"
            )
        if plan.get("index_content_hash") != index_content_hash:
            raise DiscoveryQueueScopeMismatch(
                "The stored /en/comps/ snapshot changed; use a new --queue-id"
            )
        competition_ids = plan.get("competition_ids")
        stored_competitions = plan.get("competitions")
        if (
            not isinstance(competition_ids, list)
            or not competition_ids
            or any(not isinstance(item, str) or not item for item in competition_ids)
            or len(competition_ids) != len(set(competition_ids))
        ):
            raise RawPageCorrupt("Invalid competition ids in discovery queue plan")
        if (
            not isinstance(stored_competitions, list)
            or len(stored_competitions) != len(competition_ids)
            or any(not isinstance(item, dict) for item in stored_competitions)
            or [item.get("comp_id") for item in stored_competitions]
            != competition_ids
            or plan.get("snapshot_fingerprint")
            != _payload_fingerprint(stored_competitions)
        ):
            raise RawPageCorrupt("Invalid competition snapshot in discovery queue plan")
        current = sorted(competitions, key=_competition_sort_key)
        if plan.get("snapshot_fingerprint") != _snapshot_fingerprint(current):
            raise DiscoveryQueueScopeMismatch(
                "The parsed competition snapshot changed; use a new --queue-id"
            )

    def _load_item(
        self,
        plan: dict,
        queue_id: str,
        competition_id: str,
    ) -> Optional[dict]:
        if not self.raw_store.has_discovery_queue_item(queue_id, competition_id):
            return None
        item = self.raw_store.read_discovery_queue_item(queue_id, competition_id)
        result = item.get("result")
        if (
            item.get("manifest_version") != DISCOVERY_QUEUE_VERSION
            or item.get("queue_id") != queue_id
            or item.get("competition_id") != competition_id
            or item.get("snapshot_fingerprint")
            != plan.get("snapshot_fingerprint")
            or item.get("parser_version") != plan.get("parser_version")
            or item.get("status") not in _ITEM_STATUSES
            or not isinstance(item.get("attempts"), int)
            or item["attempts"] <= 0
            or not isinstance(result, dict)
            or not isinstance(result.get("errors"), list)
            or not isinstance(result.get("competitions"), list)
            or not isinstance(result.get("seasons"), list)
            or not isinstance(result.get("schedules"), list)
            or not isinstance(result.get("matches"), list)
            or (item.get("status") == "complete" and result["errors"])
            or (item.get("status") == "error" and not result["errors"])
            or any(
                not isinstance(record, dict)
                for record in result["competitions"]
            )
            or [
                record.get("comp_id")
                for record in result["competitions"]
                if isinstance(record, dict)
            ]
            != [competition_id]
            or any(
                not isinstance(record, dict)
                or record.get("comp_id") != competition_id
                for name in ("seasons", "schedules", "matches")
                for record in result[name]
            )
        ):
            raise RawPageCorrupt(
                f"Invalid discovery queue item for competition {competition_id}"
            )
        return item

    def _states(self, plan: dict, queue_id: str) -> Dict[str, Optional[dict]]:
        return {
            competition_id: self._load_item(
                plan,
                queue_id,
                competition_id,
            )
            for competition_id in plan["competition_ids"]
        }

    @staticmethod
    def _select(
        plan: dict,
        states: Dict[str, Optional[dict]],
        *,
        max_competitions: int,
        max_attempts: int,
    ) -> List[str]:
        pending = [
            competition_id
            for competition_id in plan["competition_ids"]
            if states[competition_id] is None
        ]
        retryable = [
            competition_id
            for competition_id in plan["competition_ids"]
            if states[competition_id] is not None
            and states[competition_id]["status"] == "error"
            and states[competition_id]["attempts"] < max_attempts
        ]
        return (pending + retryable)[:max_competitions]

    @staticmethod
    def _merge_result(
        output: DiscoveryRunResult,
        source: DiscoveryRunResult,
        *,
        errors: Optional[Sequence[dict]] = None,
    ) -> None:
        output.competitions.extend(source.competitions)
        output.seasons.extend(source.seasons)
        output.schedules.extend(source.schedules)
        output.matches.extend(source.matches)
        output.errors.extend(source.errors if errors is None else errors)

    def _write_item(
        self,
        queue_id: str,
        competition_id: str,
        result: DiscoveryRunResult,
        previous: Optional[dict],
        plan: dict,
    ) -> str:
        attempts = 1 if previous is None else previous["attempts"] + 1
        status = "complete" if result.ok else "error"
        payload = {
            "manifest_version": DISCOVERY_QUEUE_VERSION,
            "queue_id": queue_id,
            "competition_id": competition_id,
            "snapshot_fingerprint": plan["snapshot_fingerprint"],
            "parser_version": plan["parser_version"],
            "status": status,
            "attempts": attempts,
            "processed_at": utc_now_iso(),
            "result": result.to_dict(),
        }
        return self.raw_store.write_discovery_queue_item(
            queue_id,
            competition_id,
            payload,
        )

    @staticmethod
    def _summary(
        plan: dict,
        states: Dict[str, Optional[dict]],
        *,
        max_attempts: int,
    ) -> dict:
        completed = []
        retryable = []
        failed = []
        pending = []
        for competition_id in plan["competition_ids"]:
            item = states[competition_id]
            if item is None:
                pending.append(competition_id)
            elif item["status"] == "complete":
                completed.append(competition_id)
            elif item["attempts"] < max_attempts:
                retryable.append(competition_id)
            else:
                failed.append(competition_id)
        completed_states = [
            states[competition_id]
            for competition_id in completed
        ]
        return {
            "total": len(plan["competition_ids"]),
            "completed": len(completed),
            "pending": len(pending),
            "retryable": len(retryable),
            "failed": len(failed),
            "complete": len(completed) == len(plan["competition_ids"]),
            "drained": not pending and not retryable,
            "retryable_competition_ids": retryable,
            "failed_competition_ids": failed,
            "discovered_seasons": sum(
                len(item["result"]["seasons"])
                for item in completed_states
            ),
            "discovered_schedules": sum(
                len(item["result"]["schedules"])
                for item in completed_states
            ),
            "discovered_matches": sum(
                len(item["result"]["matches"])
                for item in completed_states
            ),
        }

    def run(
        self,
        queue_id: str,
        *,
        max_competitions: int = 1,
        max_seasons_per_competition: int = 1,
        max_attempts: int = 3,
    ) -> DiscoveryQueueRun:
        if not str(queue_id).strip():
            raise ValueError("queue_id must not be empty")
        if max_competitions <= 0:
            raise ValueError("max_competitions must be positive")
        if max_competitions > MAX_QUEUE_COMPETITIONS_PER_RUN:
            raise ValueError(
                "max_competitions must be at most "
                f"{MAX_QUEUE_COMPETITIONS_PER_RUN}"
            )
        if max_seasons_per_competition <= 0:
            raise ValueError("max_seasons_per_competition must be positive")
        if (
            max_seasons_per_competition
            > MAX_QUEUE_SEASONS_PER_COMPETITION
        ):
            raise ValueError(
                "max_seasons_per_competition must be at most "
                f"{MAX_QUEUE_SEASONS_PER_COMPETITION}"
            )
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if max_attempts > MAX_QUEUE_ATTEMPTS:
            raise ValueError(
                f"max_attempts must be at most {MAX_QUEUE_ATTEMPTS}"
            )
        if self.service.max_network_pages > MAX_QUEUE_NETWORK_PAGES:
            raise ValueError(
                "max_network_pages must be at most "
                f"{MAX_QUEUE_NETWORK_PAGES}"
            )

        queue_id = str(queue_id).strip()
        output = DiscoveryRunResult(
            mode="discover-batch",
            offline=self.service.offline,
        )
        index = self.service.discover_index()
        if not index.ok:
            self._merge_result(output, index)
            output.page_manifests = list(self.service.page_manifests)
            output.raw_hits = self.service.raw_hits
            output.raw_writes = self.service.raw_writes
            output.network_pages = self.service.network_pages
            return DiscoveryQueueRun(
                result=output,
                queue={
                    "queue_id": queue_id,
                    "status": "failed",
                    "stop_reason": "index_failed",
                    "attempted_competition_ids": [],
                },
            )

        _, index_record = self.raw_store.load_html(competition_index_target())
        scope = self._scope(
            max_seasons_per_competition=max_seasons_per_competition,
        )
        if self.raw_store.has_discovery_queue_plan(queue_id):
            plan = self.raw_store.read_discovery_queue_plan(queue_id)
            self._validate_plan(
                plan,
                queue_id,
                index_content_hash=index_record.content_hash,
                competitions=index.competitions,
                scope=scope,
            )
        else:
            plan = self._create_plan(
                queue_id,
                index.competitions,
                index_content_hash=index_record.content_hash,
                scope=scope,
            )

        states = self._states(plan, queue_id)
        selected = self._select(
            plan,
            states,
            max_competitions=max_competitions,
            max_attempts=max_attempts,
        )
        attempted: List[str] = []
        item_manifests: List[str] = []
        stop_reason = "queue_drained" if not selected else "batch_limit_reached"

        for competition_id in selected:
            attempted.append(competition_id)
            before_raw_hits = self.service.raw_hits
            before_raw_writes = self.service.raw_writes
            before_network_pages = self.service.network_pages
            before_manifests = len(self.service.page_manifests)
            item_result = self.service.discover_graph(
                [competition_id],
                max_competitions=1,
                max_seasons_per_competition=max_seasons_per_competition,
            )
            item_result.raw_hits = self.service.raw_hits - before_raw_hits
            item_result.raw_writes = self.service.raw_writes - before_raw_writes
            item_result.network_pages = (
                self.service.network_pages - before_network_pages
            )
            item_result.page_manifests = list(
                self.service.page_manifests[before_manifests:]
            )
            budget_errors = [
                error
                for error in item_result.errors
                if error.get("error_type") == "NetworkPageBudgetExceeded"
            ]
            offline_missing_errors = [
                error
                for error in item_result.errors
                if self.service.offline
                and error.get("error_type") == "RawPageNotFound"
            ]
            non_budget_errors = [
                error
                for error in item_result.errors
                if error.get("error_type") != "NetworkPageBudgetExceeded"
                and error not in offline_missing_errors
            ]
            self._merge_result(
                output,
                item_result,
                errors=non_budget_errors,
            )
            if budget_errors:
                stop_reason = "network_budget_exhausted"
                break
            if offline_missing_errors:
                stop_reason = "offline_raw_missing"
                break

            item_manifests.append(self._write_item(
                queue_id,
                competition_id,
                item_result,
                states[competition_id],
                plan,
            ))
            states[competition_id] = self._load_item(
                plan,
                queue_id,
                competition_id,
            )

        output.competitions = _dedupe(
            output.competitions,
            key=lambda item: item.competition_id,
        )
        output.seasons = _dedupe(
            output.seasons,
            key=lambda item: (item.competition_id, item.season_id),
        )
        output.schedules = _dedupe(
            output.schedules,
            key=lambda item: (
                item.get("comp_id"),
                item.get("season_id"),
                item.get("schedule_url"),
            ),
        )
        output.matches = _dedupe(
            output.matches,
            key=lambda item: item.match_id,
        )
        output.page_manifests = list(dict.fromkeys(self.service.page_manifests))
        output.raw_hits = self.service.raw_hits
        output.raw_writes = self.service.raw_writes
        output.network_pages = self.service.network_pages

        states = self._states(plan, queue_id)
        summary = self._summary(plan, states, max_attempts=max_attempts)
        if (
            stop_reason not in {"network_budget_exhausted", "offline_raw_missing"}
            and summary["drained"]
        ):
            stop_reason = (
                "queue_drained_with_failures"
                if summary["failed"]
                else "queue_drained"
            )
        if summary["failed"] and not output.errors:
            output.errors.append({
                "target_id": f"fbref:discovery-queue:{queue_id}",
                "page_kind": "discovery_queue",
                "dataset": "competitions",
                "reason": "queue_drained_with_failures",
                "error_type": "DiscoveryQueueIncompleteError",
                "message": (
                    f"{summary['failed']} competition(s) exhausted their attempts"
                ),
            })

        if stop_reason in {"network_budget_exhausted", "offline_raw_missing"}:
            queue_status = "failed" if output.errors else "paused"
        elif summary["complete"]:
            queue_status = "complete"
        elif summary["drained"]:
            queue_status = "failed"
        else:
            queue_status = "progress"

        queue = {
            "manifest_version": DISCOVERY_QUEUE_VERSION,
            "queue_id": queue_id,
            "plan_manifest": self.raw_store.discovery_queue_plan_key(queue_id),
            "snapshot_fingerprint": plan["snapshot_fingerprint"],
            "scope": dict(plan["scope"]),
            "status": queue_status,
            "stop_reason": stop_reason,
            "attempted_competition_ids": attempted,
            "item_manifests": item_manifests,
            "all_item_manifests": [
                self.raw_store.discovery_queue_item_key(
                    queue_id,
                    competition_id,
                )
                for competition_id in plan["competition_ids"]
                if states[competition_id] is not None
            ],
            **summary,
        }
        return DiscoveryQueueRun(result=output, queue=queue)
