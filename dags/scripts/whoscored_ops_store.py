"""Immutable operational artifacts for WhoScored workflows.

The source raw store and the operational store deliberately share the same
filesystem and credentials.  Operational objects live below ``ops/`` and are
append-only: plans, work receipts and traffic ledgers are never updated in
place.  This makes Airflow task retries safe and removes host-local checkpoint
files from the correctness path.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Optional, TypeVar

from pyarrow import fs

from scrapers.whoscored.raw_store import WhoScoredRawStore
from scrapers.whoscored.runtime_contract import require_production_runtime_class


OPS_SCHEMA_VERSION = 1
BACKFILL_PLAN_VERSION = 5
BACKFILL_RECEIPT_VERSION = 5
BACKFILL_POLICY_VERSION = 6
LEGACY_BACKFILL_CHECKPOINT_VERSION = 2
BACKFILL_CHECKPOINT_VERSION = 3
BACKFILL_CHECKPOINT_DATA_VERSION = 1
# A controller generation contains at most DEFAULT_WORK_LIMIT new receipts.
# Keep the read chain bounded and periodically fold its compact frontier into
# a new snapshot.  Full match/profile receipts remain in their original
# immutable objects and are rebuilt only by explicit recovery/terminal DQ.
CHECKPOINT_DELTAS_PER_SNAPSHOT = 64
CHECKPOINT_INDEX_MAX_BYTES = 64 * 1024
CHECKPOINT_GENERATION_DIGITS = 12
CHECKPOINT_MAX_GENERATION = (10**CHECKPOINT_GENERATION_DIGITS) - 1
BACKFILL_BATCH_VERSION = 2
DEFAULT_WORK_LIMIT = 100
MATCH_CHUNK_SIZE = 25
PROFILE_CHUNK_SIZE = 200
SCHEDULE_REQUEST_UNITS_PER_STAGE = 70
SCHEDULE_MINIMUM_STAGE_COUNT = 1
MATCH_REQUEST_UNITS_PER_GAME = 1
PREVIEW_REQUEST_UNITS_PER_GAME = 1
PROFILE_REQUESTS_PER_PLAYER = 1
ROSTER_REQUEST_UNITS = 0
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_T = TypeVar("_T")


class WhoScoredOpsStoreError(RuntimeError):
    """Operational object storage is missing or violates its contract."""


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_id(value: object, *, field: str) -> str:
    token = str(value or "")
    if not _SAFE_ID.fullmatch(token):
        raise WhoScoredOpsStoreError(f"invalid {field}: {value!r}")
    return token


def _scope_digest(scope: str) -> str:
    return hashlib.sha256(scope.encode("utf-8")).hexdigest()[:20]


def schedule_request_units(stage_count: int) -> int:
    """Conservative schedule capacity for one frozen source scope.

    A schedule sync executes roughly 68 structured feeds *per stage*, in
    addition to calendar/schedule requests. The previous flat 70-unit charge
    silently treated a 13-stage World Cup scope as a one-stage scope. Keep the
    reviewed 70-unit stage envelope, but multiply it by the exact cardinality
    frozen in the catalog-backed plan (and reserve one stage when a synthetic
    or source-unavailable fixture has no stage identity).
    """

    if type(stage_count) is not int or stage_count < 0:
        raise WhoScoredOpsStoreError(f"invalid schedule stage count: {stage_count!r}")
    return (
        max(SCHEDULE_MINIMUM_STAGE_COUNT, stage_count)
        * SCHEDULE_REQUEST_UNITS_PER_STAGE
    )


def _policy_identity() -> dict[str, Any]:
    # Import lazily: lightweight DAG discovery hosts do not necessarily ship
    # the complete scraper runtime, while workers always do.
    from scrapers.whoscored.parsers import (
        MATCH_AVAILABILITY_VERSION,
        PARSER_VERSION,
    )

    return {
        "policy_version": BACKFILL_POLICY_VERSION,
        "match_candidate_policy": "all_completed_schedule_matches",
        "profile_candidate_policy": "all_post_match_frozen_roster_players",
        "parser_version": PARSER_VERSION,
        "availability_version": MATCH_AVAILABILITY_VERSION,
        "schedule_capacity_policy": "pinned-catalog-stage-count-v1",
        "schedule_request_units_per_stage": SCHEDULE_REQUEST_UNITS_PER_STAGE,
        "schedule_minimum_stage_count": SCHEDULE_MINIMUM_STAGE_COUNT,
        "match_capacity_policy": "exact-match-plus-preview-cardinality-v1",
        "match_request_units_per_game": MATCH_REQUEST_UNITS_PER_GAME,
        "preview_request_units_per_game": PREVIEW_REQUEST_UNITS_PER_GAME,
        "profile_requests_per_player": PROFILE_REQUESTS_PER_PLAYER,
        "roster_request_units": ROSTER_REQUEST_UNITS,
    }


def _policy_is_compatible(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    # Parser and availability semantics are part of the frozen work identity.
    # Mixing releases in one queue would let old receipts suppress work while
    # final DQ evaluates a newer contract. A deployment therefore starts a new
    # plan id and reparses immutable raw evidence under that exact policy.
    return value == _policy_identity()


class WhoScoredOpsStore:
    """Append-only JSON objects co-located with the WhoScored raw store."""

    def __init__(self, raw_store: WhoScoredRawStore, prefix: str = "ops") -> None:
        require_production_runtime_class(
            operation="WhoScored operational persistence"
        )
        self.raw_store = raw_store
        self.filesystem = raw_store.filesystem
        self.root = str(PurePosixPath(raw_store.root) / prefix.strip("/"))

    @classmethod
    def from_env(cls, *, optional: bool = False) -> Optional["WhoScoredOpsStore"]:
        explicit_uri = os.environ.get("WHOSCORED_OPS_STORE_URI", "").strip()
        if explicit_uri:
            # An explicit URI is already the dedicated operational root.
            return cls(WhoScoredRawStore.from_uri(explicit_uri), prefix="")
        raw_store = WhoScoredRawStore.from_env(optional=optional)
        if raw_store is None:
            return None
        return cls(raw_store, prefix=os.environ.get("WHOSCORED_OPS_PREFIX", "ops"))

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative.strip("/"))

    def _retry_io(self, relative: str, operation: Callable[[], _T]) -> _T:
        try:
            attempts = int(os.environ.get("WHOSCORED_OPS_IO_ATTEMPTS", "4"))
            base_delay = float(
                os.environ.get("WHOSCORED_OPS_RETRY_BASE_SECONDS", "0.2")
            )
        except ValueError as exc:
            raise WhoScoredOpsStoreError("invalid ops-store retry settings") from exc
        if (
            not 1 <= attempts <= 10
            or not math.isfinite(base_delay)
            or not 0 <= base_delay <= 10
        ):
            raise WhoScoredOpsStoreError("invalid ops-store retry bounds")
        for attempt in range(1, attempts + 1):
            try:
                return operation()
            except (OSError, TimeoutError) as exc:
                if attempt == attempts:
                    raise WhoScoredOpsStoreError(
                        f"ops-store I/O failed after {attempts} attempts: {relative}"
                    ) from exc
                jitter = (
                    int(hashlib.sha256(relative.encode("utf-8")).hexdigest()[:4], 16)
                    / 0xFFFF
                )
                time.sleep(base_delay * (2 ** (attempt - 1)) * (0.75 + 0.5 * jitter))
        raise AssertionError("ops-store retry loop exhausted")

    def object_uri(self, relative: str) -> str:
        path = self._path(relative)
        if isinstance(self.filesystem, fs.S3FileSystem):
            return f"s3://{path.lstrip('/')}"
        return path

    def _read_bytes(self, relative: str) -> bytes:
        def operation() -> bytes:
            path = self._path(relative)
            info = self.filesystem.get_file_info(path)
            if info.type != fs.FileType.File:
                raise WhoScoredOpsStoreError(
                    f"operational object not found: {relative}"
                )
            with self.filesystem.open_input_file(path) as stream:
                return stream.read()

        return self._retry_io(relative, operation)

    def exists(self, relative: str) -> bool:
        return self._retry_io(
            relative,
            lambda: (
                self.filesystem.get_file_info(self._path(relative)).type
                == fs.FileType.File
            ),
        )

    def read_json(self, relative: str) -> dict[str, Any]:
        value, _payload = self.read_json_with_bytes(relative)
        return value

    def read_json_with_bytes(self, relative: str) -> tuple[dict[str, Any], bytes]:
        payload = self._read_bytes(relative)
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise WhoScoredOpsStoreError(
                f"invalid operational JSON: {relative}"
            ) from exc
        if not isinstance(value, dict):
            raise WhoScoredOpsStoreError(
                f"operational JSON must be an object: {relative}"
            )
        return value, payload

    def put_json_immutable(
        self,
        relative: str,
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Create an immutable object and verify its persisted bytes.

        A repeated write is accepted only when the exact canonical payload is
        already present.  Keys containing content hashes are safe when two
        retries race: both writers publish identical bytes and the read-back
        check is the commit boundary.
        """

        payload = _canonical_json(value)
        digest = hashlib.sha256(payload).hexdigest()
        path = self._path(relative)
        info = self._retry_io(relative, lambda: self.filesystem.get_file_info(path))
        if info.type == fs.FileType.File:
            if self._read_bytes(relative) != payload:
                raise WhoScoredOpsStoreError(
                    f"immutable operational object conflict: {relative}"
                )
        elif info.type == fs.FileType.NotFound:

            def operation() -> None:
                self.filesystem.create_dir(
                    str(PurePosixPath(path).parent), recursive=True
                )
                with self.filesystem.open_output_stream(
                    path, compression=None
                ) as stream:
                    stream.write(payload)
                if self._read_bytes(relative) != payload:
                    raise WhoScoredOpsStoreError(
                        f"operational object read-back failed: {relative}"
                    )

            self._retry_io(relative, operation)
        else:
            raise WhoScoredOpsStoreError(
                f"operational object path is not a file: {relative}"
            )
        return {
            "uri": self.object_uri(relative),
            "key": relative,
            "sha256": digest,
            "bytes": len(payload),
        }

    def put_content_addressed_json(
        self,
        prefix: str,
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        payload = _canonical_json(value)
        digest = hashlib.sha256(payload).hexdigest()
        relative = f"{prefix.strip('/')}/{digest}.json"
        return self.put_json_immutable(relative, value)

    def read_content_addressed_json(
        self,
        relative: str,
        *,
        expected_sha256: Optional[str] = None,
        expected_bytes: Optional[int] = None,
    ) -> dict[str, Any]:
        """Read and prove a canonical ``<sha256>.json`` operational object."""
        value, payload = self.read_json_with_bytes(relative)
        return self._validate_content_addressed_json(
            relative,
            value,
            payload,
            expected_sha256=expected_sha256,
            expected_bytes=expected_bytes,
        )

    @staticmethod
    def _validate_content_addressed_json(
        relative: str,
        value: Mapping[str, Any],
        payload: bytes,
        *,
        expected_sha256: Optional[str] = None,
        expected_bytes: Optional[int] = None,
    ) -> dict[str, Any]:
        match = re.fullmatch(r"([0-9a-f]{64})\.json", PurePosixPath(relative).name)
        if match is None:
            raise WhoScoredOpsStoreError(
                f"content-addressed operational key is invalid: {relative}"
            )
        digest = hashlib.sha256(payload).hexdigest()
        if (
            payload != _canonical_json(value)
            or digest != match.group(1)
            or (expected_sha256 is not None and digest != expected_sha256)
            or (expected_bytes is not None and len(payload) != expected_bytes)
        ):
            raise WhoScoredOpsStoreError(
                f"content-addressed operational object failed integrity: {relative}"
            )
        return value

    def iter_content_addressed_json(
        self,
        prefix: str,
    ) -> Iterable[tuple[str, dict[str, Any]]]:
        for relative, value, payload in self.iter_json_with_bytes(prefix):
            yield (
                relative,
                self._validate_content_addressed_json(
                    relative,
                    value,
                    payload,
                ),
            )

    def iter_json(self, prefix: str) -> Iterable[tuple[str, dict[str, Any]]]:
        for relative, value, _payload in self.iter_json_with_bytes(prefix):
            yield relative, value

    def iter_json_with_bytes(
        self,
        prefix: str,
    ) -> Iterable[tuple[str, dict[str, Any], bytes]]:
        selector = fs.FileSelector(
            self._path(prefix), recursive=True, allow_not_found=True
        )
        infos = self._retry_io(prefix, lambda: self.filesystem.get_file_info(selector))
        root = self.root.rstrip("/") + "/"
        for info in sorted(infos, key=lambda item: item.path):
            if info.type != fs.FileType.File or not info.path.endswith(".json"):
                continue
            relative = (
                info.path[len(root) :] if info.path.startswith(root) else info.path
            )
            value, payload = self.read_json_with_bytes(relative)
            yield relative, value, payload

    def list_json_keys(self, prefix: str) -> list[str]:
        """List object metadata without issuing one GET per JSON object."""

        selector = fs.FileSelector(
            self._path(prefix), recursive=True, allow_not_found=True
        )
        infos = self._retry_io(prefix, lambda: self.filesystem.get_file_info(selector))
        root = self.root.rstrip("/") + "/"
        return sorted(
            info.path[len(root) :] if info.path.startswith(root) else info.path
            for info in infos
            if info.type == fs.FileType.File and info.path.endswith(".json")
        )

    def list_children(self, prefix: str) -> list[tuple[str, str]]:
        """List immediate child names and kinds for a bounded radix lookup."""

        selector = fs.FileSelector(
            self._path(prefix), recursive=False, allow_not_found=True
        )
        infos = self._retry_io(prefix, lambda: self.filesystem.get_file_info(selector))
        parent = self._path(prefix).rstrip("/") + "/"
        children: list[tuple[str, str]] = []
        for info in sorted(infos, key=lambda item: item.path):
            if not info.path.startswith(parent):
                raise WhoScoredOpsStoreError(
                    f"operational child escaped prefix: {prefix}"
                )
            name = info.path[len(parent) :]
            if not name or "/" in name:
                raise WhoScoredOpsStoreError(
                    f"operational child is not immediate: {prefix}"
                )
            if info.type == fs.FileType.Directory:
                kind = "directory"
            elif info.type == fs.FileType.File:
                kind = "file"
            else:
                raise WhoScoredOpsStoreError(
                    f"invalid operational child type: {prefix}/{name}"
                )
            children.append((name, kind))
        return children


class WhoScoredBackfillState:
    """Immutable plan and receipt protocol for bounded Airflow backfills."""

    def __init__(self, store: WhoScoredOpsStore) -> None:
        self.store = store

    @classmethod
    def from_env(cls) -> "WhoScoredBackfillState":
        store = WhoScoredOpsStore.from_env(optional=False)
        if store is None:  # pragma: no cover - guarded by optional=False
            raise WhoScoredOpsStoreError("WhoScored ops store is required")
        return cls(store)

    @staticmethod
    def _normalise_schedule_stage_ids(
        scopes: Iterable[str],
        value: Optional[Mapping[str, Iterable[int]]],
    ) -> dict[str, list[int]]:
        normalized_scopes = sorted(set(str(scope) for scope in scopes))
        if value is None:
            return {scope: [] for scope in normalized_scopes}
        source = dict(value)
        if set(source) != set(normalized_scopes):
            raise WhoScoredOpsStoreError(
                "schedule stage identities must cover every plan scope exactly"
            )
        result: dict[str, list[int]] = {}
        for scope in normalized_scopes:
            raw_ids = source.get(scope, ())
            if isinstance(raw_ids, (str, bytes)):
                raise WhoScoredOpsStoreError(
                    f"invalid schedule stage identities for {scope}"
                )
            try:
                stage_ids = list(raw_ids)
            except TypeError as exc:
                raise WhoScoredOpsStoreError(
                    f"invalid schedule stage identities for {scope}"
                ) from exc
            if (
                not stage_ids
                or any(type(stage_id) is not int or stage_id <= 0 for stage_id in stage_ids)
                or stage_ids != sorted(set(stage_ids))
            ):
                raise WhoScoredOpsStoreError(
                    f"schedule stage identities for {scope} must be non-empty "
                    "sorted unique IDs"
                )
            result[scope] = stage_ids
        return result

    @staticmethod
    def plan_id(
        selector: Mapping[str, Any],
        scopes: Iterable[str],
        provenance: Optional[Mapping[str, Any]] = None,
        policy: Optional[Mapping[str, Any]] = None,
        schedule_stage_ids: Optional[Mapping[str, Iterable[int]]] = None,
    ) -> str:
        normalized_scopes = sorted(set(scopes))
        frozen = {
            "selector": dict(selector),
            "scopes": normalized_scopes,
            "provenance": dict(provenance or {}),
            "policy": dict(policy or _policy_identity()),
            "schedule_stage_ids": WhoScoredBackfillState._normalise_schedule_stage_ids(
                normalized_scopes,
                schedule_stage_ids,
            ),
            "match_chunk_size": MATCH_CHUNK_SIZE,
            "profile_chunk_size": PROFILE_CHUNK_SIZE,
        }
        return hashlib.sha256(_canonical_json(frozen)).hexdigest()

    @staticmethod
    def _plan_key(queue_id: str, plan_id: str) -> str:
        return f"backfill/{queue_id}/plans/{plan_id}.json"

    @staticmethod
    def _receipt_prefix(queue_id: str, plan_id: str) -> str:
        return f"backfill/{queue_id}/receipts/{plan_id}"

    @staticmethod
    def _checkpoint_prefix(queue_id: str, plan_id: str) -> str:
        return f"backfill/{queue_id}/checkpoints/{plan_id}"

    @staticmethod
    def _checkpoint_data_prefix(queue_id: str, plan_id: str) -> str:
        return f"backfill/{queue_id}/checkpoint-data/{plan_id}"

    @classmethod
    def _checkpoint_manifest_prefix(
        cls,
        queue_id: str,
        plan_id: str,
        generation: int,
    ) -> str:
        if type(generation) is not int or not 0 <= generation <= CHECKPOINT_MAX_GENERATION:
            raise WhoScoredOpsStoreError(
                f"invalid checkpoint generation: {generation!r}"
            )
        digits = f"{generation:0{CHECKPOINT_GENERATION_DIGITS}d}"
        return (
            f"{cls._checkpoint_prefix(queue_id, plan_id)}/v3/"
            + "/".join(digits)
        )

    @staticmethod
    def _batch_prefix(queue_id: str, plan_id: str, batch_id: str) -> str:
        digest = hashlib.sha256(str(batch_id).encode("utf-8")).hexdigest()[:32]
        return f"backfill/{queue_id}/batches/{plan_id}/{digest}"

    def create_plan(
        self,
        *,
        queue_id: str,
        selector: Mapping[str, Any],
        scopes: Iterable[str],
        provenance: Optional[Mapping[str, Any]] = None,
        schedule_stage_ids: Optional[Mapping[str, Iterable[int]]] = None,
    ) -> dict[str, Any]:
        queue_id = _safe_id(queue_id, field="queue_id")
        normalized_scopes = sorted(set(scopes))
        if not normalized_scopes:
            raise WhoScoredOpsStoreError("backfill plan has no scopes")
        normalized_provenance = dict(provenance or {})
        normalized_schedule_stage_ids = self._normalise_schedule_stage_ids(
            normalized_scopes,
            schedule_stage_ids,
        )
        if normalized_provenance.get("catalog_batch_id") and any(
            not normalized_schedule_stage_ids[scope] for scope in normalized_scopes
        ):
            raise WhoScoredOpsStoreError(
                "catalog-backed plans require frozen stage identities for every scope"
            )
        policy = _policy_identity()
        plan_id = self.plan_id(
            selector,
            normalized_scopes,
            normalized_provenance,
            policy,
            schedule_stage_ids=normalized_schedule_stage_ids,
        )
        key = self._plan_key(queue_id, plan_id)
        if self.store.exists(key):
            plan = self.store.read_json(key)
            expected_id = self.plan_id(
                plan.get("selector", {}),
                plan.get("scopes", []),
                plan.get("provenance", {}),
                plan.get("policy", {}),
                plan.get("schedule_stage_ids"),
            )
            if (
                plan.get("plan_version") != BACKFILL_PLAN_VERSION
                or plan.get("queue_id") != queue_id
                or plan.get("plan_id") != plan_id
                or expected_id != plan_id
            ):
                raise WhoScoredOpsStoreError(
                    f"invalid existing backfill plan {queue_id}/{plan_id}"
                )
            artifact = {
                "uri": self.store.object_uri(key),
                "key": key,
                "sha256": hashlib.sha256(_canonical_json(plan)).hexdigest(),
                "bytes": len(_canonical_json(plan)),
            }
        else:
            plan = {
                "schema_version": OPS_SCHEMA_VERSION,
                "plan_version": BACKFILL_PLAN_VERSION,
                "queue_id": queue_id,
                "plan_id": plan_id,
                "selector": dict(selector),
                "scopes": normalized_scopes,
                "provenance": normalized_provenance,
                "policy": policy,
                "schedule_stage_ids": normalized_schedule_stage_ids,
                "match_chunk_size": MATCH_CHUNK_SIZE,
                "profile_chunk_size": PROFILE_CHUNK_SIZE,
            }
            artifact = self.store.put_json_immutable(key, plan)
        return {**plan, "artifact": artifact}

    def load_plan(self, queue_id: str, plan_id: str) -> dict[str, Any]:
        queue_id = _safe_id(queue_id, field="queue_id")
        if re.fullmatch(r"[0-9a-f]{64}", plan_id or "") is None:
            raise WhoScoredOpsStoreError(f"invalid plan_id: {plan_id!r}")
        plan = self.store.read_json(self._plan_key(queue_id, plan_id))
        try:
            normalized_schedule_stage_ids = self._normalise_schedule_stage_ids(
                plan.get("scopes", []),
                plan.get("schedule_stage_ids"),
            )
        except WhoScoredOpsStoreError as exc:
            raise WhoScoredOpsStoreError(
                f"backfill plan integrity failed: {plan_id}"
            ) from exc
        if (
            plan.get("plan_version") != BACKFILL_PLAN_VERSION
            or plan.get("queue_id") != queue_id
            or plan.get("plan_id") != plan_id
            or self.plan_id(
                plan.get("selector", {}),
                plan.get("scopes", []),
                plan.get("provenance", {}),
                plan.get("policy", {}),
                plan.get("schedule_stage_ids"),
            )
            != plan_id
            or not _policy_is_compatible(plan.get("policy"))
            or plan.get("schedule_stage_ids") != normalized_schedule_stage_ids
        ):
            raise WhoScoredOpsStoreError(f"backfill plan integrity failed: {plan_id}")
        return plan

    def plan_reference(self, queue_id: str, plan_id: str) -> dict[str, Any]:
        plan = self.load_plan(queue_id, plan_id)
        key = self._plan_key(queue_id, plan_id)
        payload = self.store._read_bytes(key)
        if payload != _canonical_json(plan):
            raise WhoScoredOpsStoreError(f"backfill plan is not canonical: {plan_id}")
        return {
            "uri": self.store.object_uri(key),
            "key": key,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }

    @staticmethod
    def _int_ids(
        value: Any,
        *,
        field: str,
        maximum: Optional[int] = None,
    ) -> list[int]:
        if not isinstance(value, list) or any(
            type(item) is not int or item <= 0 for item in value
        ):
            raise WhoScoredOpsStoreError(f"invalid {field}")
        if value != sorted(set(value)):
            raise WhoScoredOpsStoreError(f"{field} must be sorted and unique")
        if maximum is not None and len(value) > maximum:
            raise WhoScoredOpsStoreError(f"{field} exceeds {maximum} items")
        return value

    @classmethod
    def _validate_outcome(
        cls,
        work_item: Mapping[str, Any],
        outcome: Any,
    ) -> None:
        if not isinstance(outcome, dict):
            raise WhoScoredOpsStoreError("backfill receipt outcome must be an object")
        kind = work_item["kind"]
        if kind == "schedule":
            if set(outcome) != {
                "candidate_game_ids",
                "preview_game_ids",
                "source_stage_ids",
                "source_request_attempts",
                "estimated_request_units",
                "actual_request_units",
            }:
                raise WhoScoredOpsStoreError("invalid schedule receipt outcome schema")
            candidate_ids = cls._int_ids(
                outcome["candidate_game_ids"], field="candidate_game_ids"
            )
            preview_ids = cls._int_ids(
                outcome["preview_game_ids"], field="preview_game_ids"
            )
            if not set(preview_ids) <= set(candidate_ids):
                raise WhoScoredOpsStoreError(
                    "preview_game_ids must be completed candidate_game_ids"
                )
            source_stage_ids = cls._int_ids(
                outcome["source_stage_ids"], field="source_stage_ids"
            )
            estimated_units = outcome["estimated_request_units"]
            source_attempts = outcome["source_request_attempts"]
            actual_units = outcome["actual_request_units"]
            frozen_estimate = cls.request_units(work_item)
            accounted_actual = max(
                frozen_estimate,
                schedule_request_units(len(source_stage_ids)),
                source_attempts if type(source_attempts) is int else -1,
            )
            if (
                type(estimated_units) is not int
                or estimated_units != frozen_estimate
                or type(source_attempts) is not int
                or source_attempts < 0
                or type(actual_units) is not int
                or actual_units != accounted_actual
            ):
                raise WhoScoredOpsStoreError(
                    "schedule receipt request-unit accounting is invalid"
                )
        elif kind == "roster":
            if set(outcome) != {"profile_player_ids"}:
                raise WhoScoredOpsStoreError("invalid roster receipt outcome schema")
            cls._int_ids(outcome["profile_player_ids"], field="profile_player_ids")
        elif kind == "matches":
            if set(outcome) != {"game_ids"}:
                raise WhoScoredOpsStoreError("invalid match receipt outcome schema")
            game_ids = cls._int_ids(
                outcome["game_ids"],
                field="game_ids",
                maximum=MATCH_CHUNK_SIZE,
            )
            if game_ids != work_item["game_ids"]:
                raise WhoScoredOpsStoreError("match receipt outcome identity mismatch")
        elif kind == "profiles":
            if set(outcome) != {"player_ids", "attempted", "succeeded"}:
                raise WhoScoredOpsStoreError("invalid profile receipt outcome schema")
            player_ids = cls._int_ids(
                outcome["player_ids"],
                field="player_ids",
                maximum=PROFILE_CHUNK_SIZE,
            )
            if player_ids != work_item["player_ids"]:
                raise WhoScoredOpsStoreError(
                    "profile receipt outcome identity mismatch"
                )
            if (
                type(outcome["attempted"]) is not int
                or type(outcome["succeeded"]) is not int
                or outcome["attempted"] != len(player_ids)
                or outcome["succeeded"] != len(player_ids)
            ):
                raise WhoScoredOpsStoreError(
                    "profile receipt must prove every frozen player succeeded"
                )
        else:  # pragma: no cover - work item validation rejects this first
            raise WhoScoredOpsStoreError(f"unknown receipt kind: {kind!r}")

    @classmethod
    def receipt_request_units(cls, receipt: Mapping[str, Any]) -> int:
        """Return actual-safe units for completed work.

        Schedule receipts can discover more source stages or attempts than the
        pinned estimate. Other work kinds have an exact immutable cardinality
        in their work item, so their estimate is also their accounted actual.
        """

        work_item = receipt.get("work_item")
        outcome = receipt.get("outcome")
        if not isinstance(work_item, Mapping) or not isinstance(outcome, Mapping):
            raise WhoScoredOpsStoreError("invalid receipt request-unit accounting")
        if work_item.get("kind") == "schedule":
            value = outcome.get("actual_request_units")
            if type(value) is not int or value < cls.request_units(work_item):
                raise WhoScoredOpsStoreError(
                    "invalid actual schedule request-unit accounting"
                )
            return value
        return cls.request_units(work_item)

    def _validate_receipt_value(
        self,
        *,
        receipt: Mapping[str, Any],
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        required = {
            "schema_version",
            "receipt_version",
            "queue_id",
            "plan_id",
            "work_id",
            "kind",
            "scope",
            "finished_at",
            "status",
            "airflow",
            "work_item",
            "outcome",
        }
        if set(receipt) != required:
            raise WhoScoredOpsStoreError("invalid backfill receipt schema")
        queue_id = str(plan["queue_id"])
        plan_id = str(plan["plan_id"])
        work_item = receipt.get("work_item")
        if not isinstance(work_item, dict):
            raise WhoScoredOpsStoreError("invalid backfill receipt work item")
        self.validate_work_item(plan, work_item)
        work_id = str(work_item["work_id"])
        if (
            receipt.get("schema_version") != OPS_SCHEMA_VERSION
            or receipt.get("receipt_version") != BACKFILL_RECEIPT_VERSION
            or receipt.get("queue_id") != queue_id
            or receipt.get("plan_id") != plan_id
            or receipt.get("work_id") != work_id
            or receipt.get("kind") != work_item["kind"]
            or receipt.get("scope") != work_item["scope"]
            or receipt.get("status") != "success"
            or not isinstance(receipt.get("finished_at"), str)
            or not isinstance(receipt.get("airflow"), dict)
        ):
            raise WhoScoredOpsStoreError("invalid backfill receipt identity")
        self._validate_outcome(work_item, receipt.get("outcome"))
        return dict(receipt)

    def _validate_receipt(
        self,
        *,
        key: str,
        receipt: Mapping[str, Any],
        plan: Mapping[str, Any],
        raw: bytes,
    ) -> dict[str, Any]:
        validated = self._validate_receipt_value(receipt=receipt, plan=plan)
        queue_id = str(plan["queue_id"])
        plan_id = str(plan["plan_id"])
        work_id = str(validated["work_id"])
        expected_prefix = (
            f"{self._receipt_prefix(queue_id, plan_id)}/"
            f"{_safe_id(work_id, field='work_id')}/"
        )
        basename = key.removeprefix(expected_prefix)
        match = re.fullmatch(r"([0-9a-f]{64})\.json", basename)
        canonical = _canonical_json(validated)
        if (
            not key.startswith(expected_prefix)
            or match is None
            or raw != canonical
            or hashlib.sha256(raw).hexdigest() != match.group(1)
        ):
            raise WhoScoredOpsStoreError(
                f"backfill receipt content-address integrity failed: {key}"
            )
        return validated

    def receipts(
        self,
        queue_id: str,
        plan_id: str,
        *,
        plan: Optional[Mapping[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        loaded_plan = dict(plan or self.load_plan(queue_id, plan_id))
        values: list[dict[str, Any]] = []
        for key, receipt, raw in self.store.iter_json_with_bytes(
            self._receipt_prefix(queue_id, plan_id)
        ):
            values.append(
                self._validate_receipt(
                    key=key,
                    receipt=receipt,
                    plan=loaded_plan,
                    raw=raw,
                )
            )
        identities: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        receipt_by_work: dict[str, dict[str, Any]] = {}
        for receipt in values:
            identity = (receipt["work_item"], receipt["outcome"])
            work_id = str(receipt["work_id"])
            previous = identities.setdefault(work_id, identity)
            if previous != identity:
                raise WhoScoredOpsStoreError(
                    f"conflicting backfill receipts for {receipt['work_id']}"
                )
            current = receipt_by_work.get(work_id)
            if current is None or _canonical_json(receipt) < _canonical_json(current):
                receipt_by_work[work_id] = receipt
        return sorted(
            receipt_by_work.values(),
            key=lambda item: str(item.get("finished_at") or ""),
        )

    def append_receipt(
        self,
        *,
        queue_id: str,
        plan_id: str,
        work_item: Mapping[str, Any],
        outcome: Mapping[str, Any],
        airflow: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        plan = self.load_plan(queue_id, plan_id)
        self.validate_work_item(plan, work_item)
        self._validate_outcome(work_item, dict(outcome))
        work_id = str(work_item["work_id"])
        existing_prefix = (
            f"{self._receipt_prefix(queue_id, plan_id)}/"
            f"{_safe_id(work_id, field='work_id')}/"
        )
        for key, existing, raw in self.store.iter_json_with_bytes(existing_prefix):
            validated = self._validate_receipt(
                key=key,
                receipt=existing,
                plan=plan,
                raw=raw,
            )
            if validated["work_item"] != dict(work_item) or validated[
                "outcome"
            ] != dict(outcome):
                raise WhoScoredOpsStoreError(
                    f"conflicting backfill receipt for {work_id}"
                )
            return {
                **validated,
                "artifact": {
                    "uri": self.store.object_uri(key),
                    "key": key,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                },
            }
        finished_at = _utc_now_iso()
        receipt = {
            "schema_version": OPS_SCHEMA_VERSION,
            "receipt_version": BACKFILL_RECEIPT_VERSION,
            "queue_id": queue_id,
            "plan_id": plan_id,
            "work_id": str(work_item["work_id"]),
            "kind": str(work_item["kind"]),
            "scope": str(work_item["scope"]),
            "finished_at": finished_at,
            "status": "success",
            "airflow": dict(airflow or {}),
            "work_item": dict(work_item),
            "outcome": dict(outcome),
        }
        digest = hashlib.sha256(_canonical_json(receipt)).hexdigest()
        key = (
            f"{self._receipt_prefix(queue_id, plan_id)}/"
            f"{_safe_id(work_item['work_id'], field='work_id')}/"
            f"{digest}.json"
        )
        return {**receipt, "artifact": self.store.put_json_immutable(key, receipt)}

    def work_completed(
        self,
        queue_id: str,
        plan_id: str,
        work_id: str,
        *,
        plan: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        loaded_plan = dict(plan or self.load_plan(queue_id, plan_id))
        return (
            self._receipt_for_work(
                queue_id,
                plan_id,
                work_id,
                plan=loaded_plan,
            )
            is not None
        )

    def _receipt_for_work(
        self,
        queue_id: str,
        plan_id: str,
        work_id: str,
        *,
        plan: Mapping[str, Any],
    ) -> Optional[dict[str, Any]]:
        safe_work_id = _safe_id(work_id, field="work_id")
        prefix = f"{self._receipt_prefix(queue_id, plan_id)}/{safe_work_id}/"
        found: list[dict[str, Any]] = []
        for key, receipt, raw in self.store.iter_json_with_bytes(prefix):
            validated = self._validate_receipt(
                key=key,
                receipt=receipt,
                plan=plan,
                raw=raw,
            )
            if validated["work_id"] != work_id:
                raise WhoScoredOpsStoreError(f"invalid backfill receipt path: {key}")
            found.append(validated)
        if found and any(
            (item["work_item"], item["outcome"])
            != (found[0]["work_item"], found[0]["outcome"])
            for item in found[1:]
        ):
            raise WhoScoredOpsStoreError(f"conflicting backfill receipts for {work_id}")
        return found[0] if found else None

    @staticmethod
    def _schedule_work(plan: Mapping[str, Any], scope: str) -> dict[str, Any]:
        raw_stage_ids = plan.get("schedule_stage_ids", {}).get(scope)
        if not isinstance(raw_stage_ids, list):
            raise WhoScoredOpsStoreError(
                f"backfill plan has no frozen schedule stages for {scope}"
            )
        stage_ids = list(raw_stage_ids)
        return {
            "work_id": f"schedule-{_scope_digest(scope)}",
            "kind": "schedule",
            "scope": scope,
            "catalog_stage_ids": stage_ids,
            "estimated_request_units": schedule_request_units(len(stage_ids)),
        }

    @staticmethod
    def request_units(item: Mapping[str, Any]) -> int:
        kind = item.get("kind")
        if kind == "schedule":
            stage_ids = item.get("catalog_stage_ids")
            if not isinstance(stage_ids, list):
                raise WhoScoredOpsStoreError("schedule work has no catalog stage IDs")
            expected = schedule_request_units(len(stage_ids))
            if item.get("estimated_request_units") != expected:
                raise WhoScoredOpsStoreError(
                    "schedule work request-unit estimate is inconsistent"
                )
            return expected
        if kind == "roster":
            return ROSTER_REQUEST_UNITS
        if kind == "matches":
            game_ids = item.get("game_ids", [])
            preview_game_ids = item.get("preview_game_ids", [])
            return (
                len(game_ids) * MATCH_REQUEST_UNITS_PER_GAME
                + len(preview_game_ids) * PREVIEW_REQUEST_UNITS_PER_GAME
            )
        if kind == "profiles":
            return len(item.get("player_ids", [])) * PROFILE_REQUESTS_PER_PLAYER
        raise WhoScoredOpsStoreError(f"unknown work item kind: {kind!r}")

    @staticmethod
    def _match_work(
        scope: str,
        game_ids: list[int],
        preview_game_ids: list[int],
        ordinal: int,
    ) -> dict[str, Any]:
        encoded = (
            ",".join(str(item) for item in game_ids)
            + "|"
            + ",".join(str(item) for item in preview_game_ids)
        )
        digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()[:12]
        return {
            "work_id": f"matches-{_scope_digest(scope)}-{ordinal:06d}-{digest}",
            "kind": "matches",
            "scope": scope,
            "game_ids": game_ids,
            "preview_game_ids": preview_game_ids,
            "chunk_ordinal": ordinal,
        }

    @staticmethod
    def _profile_work(
        scope: str,
        player_ids: list[int],
        ordinal: int,
    ) -> dict[str, Any]:
        encoded = ",".join(str(item) for item in player_ids)
        digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()[:12]
        return {
            "work_id": f"profiles-{ordinal:06d}-{digest}",
            "kind": "profiles",
            "scope": scope,
            "player_ids": player_ids,
            "chunk_ordinal": ordinal,
        }

    @staticmethod
    def _roster_work(scope: str) -> dict[str, Any]:
        return {
            "work_id": f"roster-{_scope_digest(scope)}",
            "kind": "roster",
            "scope": scope,
        }

    @staticmethod
    def validate_work_item(plan: Mapping[str, Any], item: Mapping[str, Any]) -> None:
        scope = str(item.get("scope") or "")
        if scope not in plan.get("scopes", []):
            raise WhoScoredOpsStoreError(f"work item scope is not in plan: {scope}")
        kind = item.get("kind")
        if kind == "schedule":
            expected = WhoScoredBackfillState._schedule_work(plan, scope)
        elif kind == "roster":
            expected = WhoScoredBackfillState._roster_work(scope)
        elif kind == "matches":
            game_ids = item.get("game_ids")
            preview_game_ids = item.get("preview_game_ids")
            ordinal = item.get("chunk_ordinal")
            if (
                not isinstance(game_ids, list)
                or not game_ids
                or len(game_ids) > MATCH_CHUNK_SIZE
                or any(type(value) is not int or value <= 0 for value in game_ids)
                or game_ids != sorted(set(game_ids))
                or not isinstance(preview_game_ids, list)
                or any(
                    type(value) is not int or value <= 0 for value in preview_game_ids
                )
                or preview_game_ids != sorted(set(preview_game_ids))
                or not set(preview_game_ids) <= set(game_ids)
                or type(ordinal) is not int
                or ordinal < 0
            ):
                raise WhoScoredOpsStoreError("invalid match work item")
            expected = WhoScoredBackfillState._match_work(
                scope,
                game_ids,
                preview_game_ids,
                ordinal,
            )
        elif kind == "profiles":
            player_ids = item.get("player_ids")
            ordinal = item.get("chunk_ordinal")
            if (
                not isinstance(player_ids, list)
                or not player_ids
                or len(player_ids) > PROFILE_CHUNK_SIZE
                or any(type(value) is not int or value <= 0 for value in player_ids)
                or player_ids != sorted(set(player_ids))
                or type(ordinal) is not int
                or ordinal < 0
            ):
                raise WhoScoredOpsStoreError("invalid profile work item")
            expected = WhoScoredBackfillState._profile_work(
                scope,
                player_ids,
                ordinal,
            )
        else:
            raise WhoScoredOpsStoreError(f"unknown work item kind: {kind!r}")
        if dict(item) != expected:
            raise WhoScoredOpsStoreError(
                f"work item identity mismatch: {item.get('work_id')!r}"
            )

    def _pending_work_from_completion(
        self,
        plan: Mapping[str, Any],
        completed: set[str],
        earliest: Mapping[str, Mapping[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        schedules = [self._schedule_work(plan, scope) for scope in plan["scopes"]]
        pending_schedules = [
            item for item in schedules if item["work_id"] not in completed
        ]
        result = pending_schedules[:limit]
        if len(result) >= limit:
            return result

        match_work: list[dict[str, Any]] = []
        for schedule in schedules:
            receipt = earliest.get(schedule["work_id"])
            if receipt is None:
                continue
            game_ids = receipt["outcome"]["candidate_game_ids"]
            preview_ids = set(receipt["outcome"]["preview_game_ids"])
            chunks = [
                game_ids[index : index + MATCH_CHUNK_SIZE]
                for index in range(0, len(game_ids), MATCH_CHUNK_SIZE)
            ]
            match_work.extend(
                self._match_work(
                    schedule["scope"],
                    chunk,
                    sorted(preview_ids & set(chunk)),
                    ordinal,
                )
                for ordinal, chunk in enumerate(chunks)
            )
        pending_matches = [
            item for item in match_work if item["work_id"] not in completed
        ]
        result.extend(pending_matches[: limit - len(result)])
        if len(result) >= limit:
            return result

        # Freeze each final roster only after all match and preview chunks have
        # committed. A cold backfill can discover players solely through
        # lineups, substitutions, or preview injury data, so the pre-match
        # roster is not a complete profile population.
        if any(schedule["work_id"] not in completed for schedule in schedules):
            return result
        if any(item["work_id"] not in completed for item in match_work):
            return result
        roster_work = [self._roster_work(scope) for scope in plan["scopes"]]
        pending_rosters = [
            item for item in roster_work if item["work_id"] not in completed
        ]
        result.extend(pending_rosters[: limit - len(result)])
        if len(result) >= limit or pending_rosters:
            return result

        # Profile IDs are now a single, globally deduplicated immutable
        # population. Later mutable roster changes cannot alter issued chunks.
        player_ids = sorted(
            {
                player_id
                for item in roster_work
                for player_id in earliest[item["work_id"]]["outcome"][
                    "profile_player_ids"
                ]
            }
        )
        owner_scope = str(plan["scopes"][0])
        profile_chunks = [
            player_ids[index : index + PROFILE_CHUNK_SIZE]
            for index in range(0, len(player_ids), PROFILE_CHUNK_SIZE)
        ]
        profile_work = [
            self._profile_work(owner_scope, chunk, ordinal)
            for ordinal, chunk in enumerate(profile_chunks)
        ]
        pending_profiles = [
            item for item in profile_work if item["work_id"] not in completed
        ]
        result.extend(pending_profiles[: limit - len(result)])
        return result

    def _pending_work_from_receipts(
        self,
        plan: Mapping[str, Any],
        receipts: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        completed = {str(item["work_id"]) for item in receipts}
        earliest: dict[str, dict[str, Any]] = {}
        for receipt in receipts:
            earliest.setdefault(str(receipt["work_id"]), receipt)
        return self._pending_work_from_completion(
            plan, completed, earliest, limit=limit
        )

    def _pending_work_from_frontier(
        self,
        plan: Mapping[str, Any],
        frontier: Mapping[str, Any],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        completed = set(frontier["completed_work_ids"])
        earliest = {
            str(value["work_id"]): value
            for value in (
                *frontier["schedule_receipts"],
                *frontier["roster_receipts"],
            )
        }
        return self._pending_work_from_completion(
            plan, completed, earliest, limit=limit
        )

    def pending_work(
        self,
        queue_id: str,
        plan_id: str,
        *,
        limit: int = DEFAULT_WORK_LIMIT,
        request_unit_limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if not 1 <= int(limit) <= DEFAULT_WORK_LIMIT:
            raise WhoScoredOpsStoreError(
                f"work limit must be in 1..{DEFAULT_WORK_LIMIT}"
            )
        plan = self.load_plan(queue_id, plan_id)
        if (
            request_unit_limit is not None
            and not 200 <= int(request_unit_limit) <= 10000
        ):
            raise WhoScoredOpsStoreError("request-unit limit must be in 200..10000")
        receipts = self.receipts(queue_id, plan_id, plan=plan)
        pending = self._pending_work_from_receipts(plan, receipts, limit=limit)
        if request_unit_limit is None:
            return pending
        bounded: list[dict[str, Any]] = []
        consumed = 0
        for item in pending:
            units = self.request_units(item)
            if not bounded and units > int(request_unit_limit):
                raise WhoScoredOpsStoreError(
                    "next backfill work item exceeds the request-unit limit: "
                    f"work_id={item['work_id']}, required={units}, "
                    f"limit={int(request_unit_limit)}"
                )
            if bounded and consumed + units > int(request_unit_limit):
                break
            bounded.append(item)
            consumed += units
        return bounded

    def _progress_from_receipts(
        self,
        plan: Mapping[str, Any],
        receipts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        queue_id = str(plan["queue_id"])
        plan_id = str(plan["plan_id"])
        completed = {str(item["work_id"]) for item in receipts}
        schedule_receipts = [item for item in receipts if item["kind"] == "schedule"]
        match_receipts = [item for item in receipts if item["kind"] == "matches"]
        profile_receipts = [item for item in receipts if item["kind"] == "profiles"]
        roster_receipts = [item for item in receipts if item["kind"] == "roster"]
        earliest = {str(item["work_id"]): item for item in receipts}
        schedule_work = [
            self._schedule_work(plan, scope) for scope in plan["scopes"]
        ]
        freeze_complete = all(item["work_id"] in completed for item in schedule_work)
        expected_match_work: list[dict[str, Any]] = []
        for schedule in schedule_work:
            receipt = earliest.get(schedule["work_id"])
            if receipt is None:
                continue
            game_ids = receipt["outcome"]["candidate_game_ids"]
            preview_ids = set(receipt["outcome"]["preview_game_ids"])
            expected_match_work.extend(
                self._match_work(
                    schedule["scope"],
                    game_ids[index : index + MATCH_CHUNK_SIZE],
                    sorted(
                        preview_ids & set(game_ids[index : index + MATCH_CHUNK_SIZE])
                    ),
                    index // MATCH_CHUNK_SIZE,
                )
                for index in range(0, len(game_ids), MATCH_CHUNK_SIZE)
            )
        expected_profile_work: list[dict[str, Any]] = []
        expected_roster_work = (
            [self._roster_work(scope) for scope in plan["scopes"]]
            if freeze_complete
            else []
        )
        roster_freeze_complete = bool(freeze_complete) and all(
            item["work_id"] in completed for item in expected_roster_work
        )
        if roster_freeze_complete:
            player_ids = sorted(
                {
                    player_id
                    for item in expected_roster_work
                    for player_id in earliest[item["work_id"]]["outcome"][
                        "profile_player_ids"
                    ]
                }
            )
            owner_scope = str(plan["scopes"][0])
            expected_profile_work = [
                self._profile_work(
                    owner_scope,
                    player_ids[index : index + PROFILE_CHUNK_SIZE],
                    index // PROFILE_CHUNK_SIZE,
                )
                for index in range(0, len(player_ids), PROFILE_CHUNK_SIZE)
            ]
        known_work_ids = {
            item["work_id"]
            for item in (
                *schedule_work,
                *expected_match_work,
                *expected_roster_work,
                *expected_profile_work,
            )
        }
        remaining_lower_bound = len(known_work_ids - completed)
        known_work = {
            str(item["work_id"]): item
            for item in (
                *schedule_work,
                *expected_match_work,
                *expected_roster_work,
                *expected_profile_work,
            )
        }

        remaining_request_units_lower_bound = sum(
            self.request_units(item)
            for work_id, item in known_work.items()
            if work_id not in completed
        )
        estimated_completed_request_units = sum(
            self.request_units(receipt["work_item"]) for receipt in receipts
        )
        actual_completed_request_units = sum(
            self.receipt_request_units(receipt) for receipt in receipts
        )
        schedule_stage_cardinality_drifts = sum(
            receipt["kind"] == "schedule"
            and len(receipt["outcome"]["source_stage_ids"])
            != len(receipt["work_item"]["catalog_stage_ids"])
            for receipt in receipts
        )
        total_work_items = len(known_work_ids) if roster_freeze_complete else None
        remaining_work_items = remaining_lower_bound if roster_freeze_complete else None
        estimated_total_request_units = (
            sum(self.request_units(item) for item in known_work.values())
            if roster_freeze_complete
            else None
        )
        remaining_request_units = (
            remaining_request_units_lower_bound if roster_freeze_complete else None
        )
        total_request_units = (
            actual_completed_request_units + remaining_request_units
            if remaining_request_units is not None
            else None
        )
        pending = self._pending_work_from_receipts(
            plan,
            receipts,
            limit=DEFAULT_WORK_LIMIT,
        )
        schedule_count = len(plan["scopes"])
        status = (
            "complete"
            if len(schedule_receipts) >= schedule_count and not pending
            else "running"
        )
        return {
            "queue_id": queue_id,
            "plan_id": plan_id,
            "plan_uri": self.store.object_uri(self._plan_key(queue_id, plan_id)),
            "status": status,
            "scopes": schedule_count,
            "completed_schedules": len(
                {str(item["scope"]) for item in schedule_receipts}
            ),
            "completed_match_chunks": len(
                {str(item["work_id"]) for item in match_receipts}
            ),
            "completed_roster_freezes": len(
                {str(item["scope"]) for item in roster_receipts}
            ),
            "completed_profile_chunks": len(
                {str(item["work_id"]) for item in profile_receipts}
            ),
            "completed_profile_players": len(
                {
                    int(player_id)
                    for item in profile_receipts
                    for player_id in item["outcome"]["player_ids"]
                }
            ),
            "successful_receipts": len(completed),
            "next_work_items": len(pending),
            "work_limit": DEFAULT_WORK_LIMIT,
            "freeze_complete": freeze_complete,
            "roster_freeze_complete": roster_freeze_complete,
            "total_work_items": total_work_items,
            "remaining_work_items": remaining_work_items,
            "remaining_work_items_lower_bound": remaining_lower_bound,
            "estimated_completed_request_units": (
                estimated_completed_request_units
            ),
            "actual_completed_request_units": actual_completed_request_units,
            "estimated_total_request_units": estimated_total_request_units,
            "total_request_units": total_request_units,
            "remaining_request_units": remaining_request_units,
            "remaining_request_units_lower_bound": (
                remaining_request_units_lower_bound
            ),
            "projected_request_units_lower_bound": (
                actual_completed_request_units
                + remaining_request_units_lower_bound
            ),
            "schedule_stage_cardinality_drifts": (
                schedule_stage_cardinality_drifts
            ),
        }

    def _progress_from_frontier(
        self,
        plan: Mapping[str, Any],
        frontier: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Compute controller progress without loading cumulative receipts."""

        queue_id = str(plan["queue_id"])
        plan_id = str(plan["plan_id"])
        completed = set(frontier["completed_work_ids"])
        by_kind = frontier["completed_by_kind"]
        schedule_receipts = frontier["schedule_receipts"]
        roster_receipts = frontier["roster_receipts"]
        earliest = {
            str(value["work_id"]): value
            for value in (*schedule_receipts, *roster_receipts)
        }
        schedule_work = [
            self._schedule_work(plan, scope) for scope in plan["scopes"]
        ]
        freeze_complete = all(item["work_id"] in completed for item in schedule_work)
        expected_match_work: list[dict[str, Any]] = []
        for schedule in schedule_work:
            receipt = earliest.get(schedule["work_id"])
            if receipt is None:
                continue
            game_ids = receipt["outcome"]["candidate_game_ids"]
            preview_ids = set(receipt["outcome"]["preview_game_ids"])
            expected_match_work.extend(
                self._match_work(
                    schedule["scope"],
                    game_ids[index : index + MATCH_CHUNK_SIZE],
                    sorted(
                        preview_ids & set(game_ids[index : index + MATCH_CHUNK_SIZE])
                    ),
                    index // MATCH_CHUNK_SIZE,
                )
                for index in range(0, len(game_ids), MATCH_CHUNK_SIZE)
            )
        expected_roster_work = (
            [self._roster_work(scope) for scope in plan["scopes"]]
            if freeze_complete
            else []
        )
        roster_freeze_complete = bool(freeze_complete) and all(
            item["work_id"] in completed for item in expected_roster_work
        )
        expected_profile_work: list[dict[str, Any]] = []
        if roster_freeze_complete:
            player_ids = sorted(
                {
                    player_id
                    for item in expected_roster_work
                    for player_id in earliest[item["work_id"]]["outcome"][
                        "profile_player_ids"
                    ]
                }
            )
            owner_scope = str(plan["scopes"][0])
            expected_profile_work = [
                self._profile_work(
                    owner_scope,
                    player_ids[index : index + PROFILE_CHUNK_SIZE],
                    index // PROFILE_CHUNK_SIZE,
                )
                for index in range(0, len(player_ids), PROFILE_CHUNK_SIZE)
            ]
        known_work = {
            str(item["work_id"]): item
            for item in (
                *schedule_work,
                *expected_match_work,
                *expected_roster_work,
                *expected_profile_work,
            )
        }
        remaining_lower_bound = len(set(known_work) - completed)
        remaining_request_units_lower_bound = sum(
            self.request_units(item)
            for work_id, item in known_work.items()
            if work_id not in completed
        )
        total_work_items = len(known_work) if roster_freeze_complete else None
        remaining_work_items = (
            remaining_lower_bound if roster_freeze_complete else None
        )
        estimated_total_request_units = (
            sum(self.request_units(item) for item in known_work.values())
            if roster_freeze_complete
            else None
        )
        remaining_request_units = (
            remaining_request_units_lower_bound if roster_freeze_complete else None
        )
        actual_completed_request_units = int(
            frontier["actual_completed_request_units"]
        )
        total_request_units = (
            actual_completed_request_units + remaining_request_units
            if remaining_request_units is not None
            else None
        )
        pending = self._pending_work_from_frontier(
            plan, frontier, limit=DEFAULT_WORK_LIMIT
        )
        schedule_count = len(plan["scopes"])
        status = (
            "complete"
            if int(by_kind["schedule"]) >= schedule_count and not pending
            else "running"
        )
        return {
            "queue_id": queue_id,
            "plan_id": plan_id,
            "plan_uri": self.store.object_uri(self._plan_key(queue_id, plan_id)),
            "status": status,
            "scopes": schedule_count,
            "completed_schedules": int(by_kind["schedule"]),
            "completed_match_chunks": int(by_kind["matches"]),
            "completed_roster_freezes": int(by_kind["roster"]),
            "completed_profile_chunks": int(by_kind["profiles"]),
            "completed_profile_players": int(
                frontier["completed_profile_players"]
            ),
            "successful_receipts": len(completed),
            "next_work_items": len(pending),
            "work_limit": DEFAULT_WORK_LIMIT,
            "freeze_complete": freeze_complete,
            "roster_freeze_complete": roster_freeze_complete,
            "total_work_items": total_work_items,
            "remaining_work_items": remaining_work_items,
            "remaining_work_items_lower_bound": remaining_lower_bound,
            "estimated_completed_request_units": int(
                frontier["estimated_completed_request_units"]
            ),
            "actual_completed_request_units": actual_completed_request_units,
            "estimated_total_request_units": estimated_total_request_units,
            "total_request_units": total_request_units,
            "remaining_request_units": remaining_request_units,
            "remaining_request_units_lower_bound": (
                remaining_request_units_lower_bound
            ),
            "projected_request_units_lower_bound": (
                actual_completed_request_units
                + remaining_request_units_lower_bound
            ),
            "schedule_stage_cardinality_drifts": int(
                frontier["schedule_stage_cardinality_drifts"]
            ),
        }

    def progress(self, queue_id: str, plan_id: str) -> dict[str, Any]:
        """Full receipt rebuild path retained for explicit recovery tooling."""

        plan = self.load_plan(queue_id, plan_id)
        receipts = self.receipts(queue_id, plan_id, plan=plan)
        return self._progress_from_receipts(plan, receipts)

    @staticmethod
    def _artifact_reference(
        store: WhoScoredOpsStore,
        key: str,
        payload: bytes,
    ) -> dict[str, Any]:
        return {
            "uri": store.object_uri(key),
            "key": key,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }

    @staticmethod
    def _frontier_sha256(frontier: Mapping[str, Any]) -> str:
        return hashlib.sha256(_canonical_json(frontier)).hexdigest()

    @staticmethod
    def _delta_frontier_sha256(
        parent_sha256: str,
        delta_reference: Mapping[str, Any],
        receipt_count: int,
    ) -> str:
        """Extend the frontier integrity chain without serializing its corpus."""

        return hashlib.sha256(
            _canonical_json(
                {
                    "parent_frontier_sha256": parent_sha256,
                    "delta_sha256": str(delta_reference["sha256"]),
                    "receipt_count": receipt_count,
                }
            )
        ).hexdigest()

    @staticmethod
    def _empty_frontier() -> dict[str, Any]:
        return {
            "completed_work_ids": [],
            "completed_by_kind": {
                "schedule": 0,
                "matches": 0,
                "roster": 0,
                "profiles": 0,
            },
            "schedule_receipts": [],
            "roster_receipts": [],
            "completed_profile_players": 0,
            "estimated_completed_request_units": 0,
            "actual_completed_request_units": 0,
            "schedule_stage_cardinality_drifts": 0,
        }

    def _validate_frontier_receipt(
        self,
        value: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        if set(value) != {"work_id", "kind", "scope", "work_item", "outcome"}:
            raise WhoScoredOpsStoreError("invalid compact frontier receipt schema")
        work_item = value.get("work_item")
        outcome = value.get("outcome")
        if not isinstance(work_item, dict) or not isinstance(outcome, dict):
            raise WhoScoredOpsStoreError("invalid compact frontier receipt value")
        self.validate_work_item(plan, work_item)
        self._validate_outcome(work_item, outcome)
        if (
            value.get("work_id") != work_item["work_id"]
            or value.get("kind") != kind
            or work_item.get("kind") != kind
            or value.get("scope") != work_item["scope"]
        ):
            raise WhoScoredOpsStoreError("compact frontier receipt identity mismatch")
        return {
            "work_id": str(value["work_id"]),
            "kind": kind,
            "scope": str(value["scope"]),
            "work_item": dict(work_item),
            "outcome": dict(outcome),
        }

    def _validate_frontier(
        self,
        frontier: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        if set(frontier) != set(self._empty_frontier()):
            raise WhoScoredOpsStoreError("invalid compact frontier schema")
        completed = frontier.get("completed_work_ids")
        by_kind = frontier.get("completed_by_kind")
        schedule_values = frontier.get("schedule_receipts")
        roster_values = frontier.get("roster_receipts")
        if (
            not isinstance(completed, list)
            or any(not isinstance(value, str) for value in completed)
            or completed != sorted(set(completed))
            or not isinstance(by_kind, dict)
            or set(by_kind) != {"schedule", "matches", "roster", "profiles"}
            or any(type(value) is not int or value < 0 for value in by_kind.values())
            or sum(by_kind.values()) != len(completed)
            or not isinstance(schedule_values, list)
            or not isinstance(roster_values, list)
        ):
            raise WhoScoredOpsStoreError("invalid compact frontier identity")
        for work_id in completed:
            _safe_id(work_id, field="work_id")
        schedules = [
            self._validate_frontier_receipt(value, plan=plan, kind="schedule")
            for value in schedule_values
        ]
        rosters = [
            self._validate_frontier_receipt(value, plan=plan, kind="roster")
            for value in roster_values
        ]
        for name, values in (("schedule", schedules), ("roster", rosters)):
            ids = [str(value["work_id"]) for value in values]
            scopes = [str(value["scope"]) for value in values]
            if (
                ids != sorted(set(ids))
                or len(scopes) != len(set(scopes))
                or not set(ids) <= set(completed)
                or int(by_kind[name]) != len(ids)
            ):
                raise WhoScoredOpsStoreError(
                    f"invalid compact frontier {name} population"
                )
        scalar_names = {
            "completed_profile_players",
            "estimated_completed_request_units",
            "actual_completed_request_units",
            "schedule_stage_cardinality_drifts",
        }
        if any(
            type(frontier.get(name)) is not int or int(frontier[name]) < 0
            for name in scalar_names
        ):
            raise WhoScoredOpsStoreError("invalid compact frontier counters")
        if (
            int(frontier["completed_profile_players"])
            > int(by_kind["profiles"]) * PROFILE_CHUNK_SIZE
            or int(frontier["actual_completed_request_units"])
            < int(frontier["estimated_completed_request_units"])
            or int(frontier["schedule_stage_cardinality_drifts"])
            > int(by_kind["schedule"])
        ):
            raise WhoScoredOpsStoreError("inconsistent compact frontier counters")
        return {
            "completed_work_ids": list(completed),
            "completed_by_kind": {name: int(by_kind[name]) for name in sorted(by_kind)},
            "schedule_receipts": schedules,
            "roster_receipts": rosters,
            "completed_profile_players": int(
                frontier["completed_profile_players"]
            ),
            "estimated_completed_request_units": int(
                frontier["estimated_completed_request_units"]
            ),
            "actual_completed_request_units": int(
                frontier["actual_completed_request_units"]
            ),
            "schedule_stage_cardinality_drifts": int(
                frontier["schedule_stage_cardinality_drifts"]
            ),
        }

    @staticmethod
    def _compact_frontier_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "work_id": str(receipt["work_id"]),
            "kind": str(receipt["kind"]),
            "scope": str(receipt["scope"]),
            "work_item": dict(receipt["work_item"]),
            "outcome": dict(receipt["outcome"]),
        }

    def _apply_frontier_receipts(
        self,
        frontier: Mapping[str, Any],
        receipts: Iterable[Mapping[str, Any]],
        *,
        plan: Mapping[str, Any],
        frontier_is_validated: bool = False,
    ) -> dict[str, Any]:
        selected = (
            dict(frontier)
            if frontier_is_validated
            else self._validate_frontier(frontier, plan=plan)
        )
        completed = set(selected["completed_work_ids"])
        by_kind = dict(selected["completed_by_kind"])
        schedules = {
            str(value["work_id"]): value for value in selected["schedule_receipts"]
        }
        rosters = {
            str(value["work_id"]): value for value in selected["roster_receipts"]
        }
        profile_players = int(selected["completed_profile_players"])
        estimated_units = int(selected["estimated_completed_request_units"])
        actual_units = int(selected["actual_completed_request_units"])
        stage_drifts = int(selected["schedule_stage_cardinality_drifts"])
        for raw_receipt in receipts:
            receipt = self._validate_receipt_value(receipt=raw_receipt, plan=plan)
            work_id = str(receipt["work_id"])
            if work_id in completed:
                raise WhoScoredOpsStoreError(
                    f"duplicate receipt in compact checkpoint lineage: {work_id}"
                )
            kind = str(receipt["kind"])
            completed.add(work_id)
            by_kind[kind] += 1
            estimated_units += self.request_units(receipt["work_item"])
            actual_units += self.receipt_request_units(receipt)
            if kind == "schedule":
                schedules[work_id] = self._compact_frontier_receipt(receipt)
                stage_drifts += int(
                    len(receipt["outcome"]["source_stage_ids"])
                    != len(receipt["work_item"]["catalog_stage_ids"])
                )
            elif kind == "roster":
                rosters[work_id] = self._compact_frontier_receipt(receipt)
            elif kind == "profiles":
                # Issued profile chunks are a globally deduplicated frozen
                # population and their validated outcome exactly equals the
                # work item.  Summing is therefore the exact distinct count.
                profile_players += len(receipt["outcome"]["player_ids"])
        updated = {
            "completed_work_ids": sorted(completed),
            "completed_by_kind": by_kind,
            "schedule_receipts": sorted(
                schedules.values(), key=lambda value: str(value["work_id"])
            ),
            "roster_receipts": sorted(
                rosters.values(), key=lambda value: str(value["work_id"])
            ),
            "completed_profile_players": profile_players,
            "estimated_completed_request_units": estimated_units,
            "actual_completed_request_units": actual_units,
            "schedule_stage_cardinality_drifts": stage_drifts,
        }
        return (
            updated
            if frontier_is_validated
            else self._validate_frontier(updated, plan=plan)
        )

    def _frontier_from_receipts(
        self,
        plan: Mapping[str, Any],
        receipts: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        values = list(receipts)
        frontier = self._apply_frontier_receipts(
            self._empty_frontier(), values, plan=plan
        )
        # Preserve the historical public progress semantics on explicit full
        # rebuilds and make a forged overlapping profile population diverge
        # from the incrementally issued compact frontier at terminal DQ.
        frontier["completed_profile_players"] = len(
            {
                int(player_id)
                for receipt in values
                if receipt.get("kind") == "profiles"
                for player_id in receipt.get("outcome", {}).get("player_ids", [])
            }
        )
        return self._validate_frontier(frontier, plan=plan)

    @staticmethod
    def _checkpoint_reference(artifact: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "key": str(artifact["key"]),
            "sha256": str(artifact["sha256"]),
            "bytes": int(artifact["bytes"]),
        }

    def _validate_checkpoint_reference(
        self,
        reference: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        if set(reference) != {"key", "sha256", "bytes"}:
            raise WhoScoredOpsStoreError("invalid checkpoint data reference schema")
        key = reference.get("key")
        digest = reference.get("sha256")
        byte_count = reference.get("bytes")
        prefix = self._checkpoint_data_prefix(
            str(plan["queue_id"]), str(plan["plan_id"])
        )
        if (
            not isinstance(key, str)
            or not key.startswith(prefix + "/")
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or PurePosixPath(key).name != f"{digest}.json"
            or type(byte_count) is not int
            or byte_count <= 0
        ):
            raise WhoScoredOpsStoreError("invalid checkpoint data reference")
        return {"key": key, "sha256": digest, "bytes": byte_count}

    def _validate_legacy_checkpoint(
        self,
        checkpoint: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        if set(checkpoint) != {
            "schema_version",
            "checkpoint_version",
            "queue_id",
            "plan_id",
            "generation",
            "parent_sha256",
            "created_at",
            "receipts",
        }:
            raise WhoScoredOpsStoreError("invalid legacy backfill checkpoint schema")
        generation = checkpoint.get("generation")
        parent_sha = checkpoint.get("parent_sha256")
        receipts = checkpoint.get("receipts")
        if (
            checkpoint.get("schema_version") != OPS_SCHEMA_VERSION
            or checkpoint.get("checkpoint_version")
            != LEGACY_BACKFILL_CHECKPOINT_VERSION
            or checkpoint.get("queue_id") != plan["queue_id"]
            or checkpoint.get("plan_id") != plan["plan_id"]
            or type(generation) is not int
            or generation < 0
            or generation > CHECKPOINT_MAX_GENERATION
            or (
                parent_sha is not None
                and (
                    not isinstance(parent_sha, str)
                    or re.fullmatch(r"[0-9a-f]{64}", parent_sha) is None
                )
            )
            or not isinstance(checkpoint.get("created_at"), str)
            or not isinstance(receipts, list)
        ):
            raise WhoScoredOpsStoreError("invalid legacy checkpoint identity")
        receipt_by_work: dict[str, dict[str, Any]] = {}
        identities: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for value in receipts:
            receipt = self._validate_receipt_value(receipt=value, plan=plan)
            identity = (receipt["work_item"], receipt["outcome"])
            work_id = str(receipt["work_id"])
            previous = identities.setdefault(work_id, identity)
            if previous != identity:
                raise WhoScoredOpsStoreError(
                    f"conflicting checkpoint receipts for {work_id}"
                )
            current = receipt_by_work.get(work_id)
            if current is None or _canonical_json(receipt) < _canonical_json(current):
                receipt_by_work[work_id] = receipt
        normalized = dict(checkpoint)
        normalized["receipts"] = sorted(
            receipt_by_work.values(), key=lambda value: str(value["work_id"])
        )
        return normalized

    def _validate_checkpoint_manifest(
        self,
        checkpoint: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        if set(checkpoint) != {
            "schema_version",
            "checkpoint_version",
            "queue_id",
            "plan_id",
            "generation",
            "parent_sha256",
            "created_at",
            "receipt_count",
            "frontier_sha256",
            "snapshot",
            "deltas",
        }:
            raise WhoScoredOpsStoreError("invalid segmented checkpoint schema")
        generation = checkpoint.get("generation")
        parent_sha = checkpoint.get("parent_sha256")
        deltas = checkpoint.get("deltas")
        if (
            checkpoint.get("schema_version") != OPS_SCHEMA_VERSION
            or checkpoint.get("checkpoint_version") != BACKFILL_CHECKPOINT_VERSION
            or checkpoint.get("queue_id") != plan["queue_id"]
            or checkpoint.get("plan_id") != plan["plan_id"]
            or type(generation) is not int
            or generation < 0
            or generation > CHECKPOINT_MAX_GENERATION
            or (
                parent_sha is not None
                and (
                    not isinstance(parent_sha, str)
                    or re.fullmatch(r"[0-9a-f]{64}", parent_sha) is None
                )
            )
            or not isinstance(checkpoint.get("created_at"), str)
            or type(checkpoint.get("receipt_count")) is not int
            or int(checkpoint["receipt_count"]) < 0
            or not isinstance(checkpoint.get("frontier_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", checkpoint["frontier_sha256"])
            is None
            or not isinstance(checkpoint.get("snapshot"), dict)
            or not isinstance(deltas, list)
            or len(deltas) >= CHECKPOINT_DELTAS_PER_SNAPSHOT
        ):
            raise WhoScoredOpsStoreError("invalid segmented checkpoint identity")
        return {
            **dict(checkpoint),
            "snapshot": self._validate_checkpoint_reference(
                checkpoint["snapshot"], plan=plan
            ),
            "deltas": [
                self._validate_checkpoint_reference(value, plan=plan)
                for value in deltas
            ],
        }

    def _read_checkpoint_data(
        self,
        reference: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.store.read_content_addressed_json(
            str(reference["key"]),
            expected_sha256=str(reference["sha256"]),
            expected_bytes=int(reference["bytes"]),
        )

    def _load_segmented_checkpoint(
        self,
        checkpoint: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
        key: str,
        payload: bytes,
    ) -> dict[str, Any]:
        manifest = self._validate_checkpoint_manifest(checkpoint, plan=plan)
        if len(payload) > CHECKPOINT_INDEX_MAX_BYTES:
            raise WhoScoredOpsStoreError("segmented checkpoint index exceeds 64 KiB")
        snapshot = self._read_checkpoint_data(manifest["snapshot"])
        if set(snapshot) != {
            "schema_version",
            "checkpoint_data_version",
            "kind",
            "queue_id",
            "plan_id",
            "generation",
            "created_at",
            "frontier_sha256",
            "frontier",
        }:
            raise WhoScoredOpsStoreError("invalid frontier snapshot schema")
        if (
            snapshot.get("schema_version") != OPS_SCHEMA_VERSION
            or snapshot.get("checkpoint_data_version")
            != BACKFILL_CHECKPOINT_DATA_VERSION
            or snapshot.get("kind") != "frontier_snapshot"
            or snapshot.get("queue_id") != plan["queue_id"]
            or snapshot.get("plan_id") != plan["plan_id"]
            or type(snapshot.get("generation")) is not int
            or int(snapshot["generation"]) < 0
            or not isinstance(snapshot.get("created_at"), str)
            or not isinstance(snapshot.get("frontier_sha256"), str)
            or not isinstance(snapshot.get("frontier"), dict)
        ):
            raise WhoScoredOpsStoreError("invalid frontier snapshot identity")
        frontier = self._validate_frontier(snapshot["frontier"], plan=plan)
        frontier_sha = self._frontier_sha256(frontier)
        if frontier_sha != snapshot["frontier_sha256"]:
            raise WhoScoredOpsStoreError("frontier snapshot hash mismatch")
        expected_generation = int(snapshot["generation"])
        expected_receipt_count = len(frontier["completed_work_ids"])
        created_at = str(snapshot["created_at"])
        delta_receipts: list[Mapping[str, Any]] = []
        for reference in manifest["deltas"]:
            delta = self._read_checkpoint_data(reference)
            if set(delta) != {
                "schema_version",
                "checkpoint_data_version",
                "kind",
                "queue_id",
                "plan_id",
                "generation",
                "created_at",
                "parent_frontier_sha256",
                "receipts",
            }:
                raise WhoScoredOpsStoreError("invalid checkpoint receipt delta schema")
            receipts = delta.get("receipts")
            if (
                delta.get("schema_version") != OPS_SCHEMA_VERSION
                or delta.get("checkpoint_data_version")
                != BACKFILL_CHECKPOINT_DATA_VERSION
                or delta.get("kind") != "receipt_delta"
                or delta.get("queue_id") != plan["queue_id"]
                or delta.get("plan_id") != plan["plan_id"]
                or delta.get("generation") != expected_generation + 1
                or not isinstance(delta.get("created_at"), str)
                or delta.get("parent_frontier_sha256") != frontier_sha
                or not isinstance(receipts, list)
                or not 1 <= len(receipts) <= DEFAULT_WORK_LIMIT
            ):
                raise WhoScoredOpsStoreError("invalid checkpoint receipt delta")
            expected_receipt_count += len(receipts)
            frontier_sha = self._delta_frontier_sha256(
                frontier_sha,
                reference,
                expected_receipt_count,
            )
            delta_receipts.extend(receipts)
            expected_generation += 1
            created_at = max(created_at, str(delta["created_at"]))
        if delta_receipts:
            # Apply the bounded chain in one pass.  Rebuilding the cumulative
            # completed-id set once avoids O(total_receipts * delta_count)
            # controller CPU as a snapshot approaches compaction.
            frontier = self._apply_frontier_receipts(
                frontier,
                delta_receipts,
                plan=plan,
                frontier_is_validated=True,
            )
        frontier = self._validate_frontier(frontier, plan=plan)
        if (
            expected_generation != manifest["generation"]
            or frontier_sha != manifest["frontier_sha256"]
            or len(frontier["completed_work_ids"]) != manifest["receipt_count"]
            or created_at != manifest["created_at"]
        ):
            raise WhoScoredOpsStoreError("segmented checkpoint materialization mismatch")
        return {
            **manifest,
            "frontier": frontier,
            "artifact": self._artifact_reference(self.store, key, payload),
        }

    def _write_checkpoint(
        self,
        *,
        plan: Mapping[str, Any],
        receipts: list[dict[str, Any]],
        generation: int,
        parent_sha256: Optional[str],
        parent_checkpoint: Optional[Mapping[str, Any]] = None,
        delta_receipts: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        queue_id = str(plan["queue_id"])
        plan_id = str(plan["plan_id"])
        default_created_at = str(
            plan.get("provenance", {}).get("backfill_started_at")
            or "1970-01-01T00:00:00+00:00"
        )
        parent_is_segmented = bool(
            parent_checkpoint
            and parent_checkpoint.get("checkpoint_version")
            == BACKFILL_CHECKPOINT_VERSION
            and isinstance(parent_checkpoint.get("frontier"), dict)
        )
        normalized_delta: list[dict[str, Any]] = []
        if delta_receipts is not None:
            if not 1 <= len(delta_receipts) <= DEFAULT_WORK_LIMIT:
                raise WhoScoredOpsStoreError("checkpoint delta must contain 1..100 receipts")
            normalized_delta = sorted(
                (
                    self._validate_receipt_value(receipt=value, plan=plan)
                    for value in delta_receipts
                ),
                key=lambda value: str(value["work_id"]),
            )
            if len({value["work_id"] for value in normalized_delta}) != len(
                normalized_delta
            ):
                raise WhoScoredOpsStoreError("duplicate checkpoint delta receipt")

        if parent_is_segmented and normalized_delta:
            frontier = self._apply_frontier_receipts(
                parent_checkpoint["frontier"],
                normalized_delta,
                plan=plan,
                frontier_is_validated=True,
            )
            created_at = max(
                str(parent_checkpoint["created_at"]),
                *(str(value["finished_at"]) for value in normalized_delta),
            )
        else:
            # Bootstrap and one-time v2 migration are the only paths that
            # inspect the complete receipt population.
            frontier = self._frontier_from_receipts(plan, receipts)
            created_at = max(
                (str(value.get("finished_at") or "") for value in receipts),
                default=default_created_at,
            )

        data_prefix = self._checkpoint_data_prefix(queue_id, plan_id)
        parent_deltas = (
            list(parent_checkpoint.get("deltas", []))
            if parent_is_segmented
            else []
        )
        write_delta = bool(
            parent_is_segmented
            and normalized_delta
            and len(parent_deltas) < CHECKPOINT_DELTAS_PER_SNAPSHOT - 1
        )
        if write_delta:
            delta = {
                "schema_version": OPS_SCHEMA_VERSION,
                "checkpoint_data_version": BACKFILL_CHECKPOINT_DATA_VERSION,
                "kind": "receipt_delta",
                "queue_id": queue_id,
                "plan_id": plan_id,
                "generation": generation,
                "created_at": created_at,
                "parent_frontier_sha256": str(
                    parent_checkpoint["frontier_sha256"]
                ),
                "receipts": normalized_delta,
            }
            delta_artifact = self.store.put_content_addressed_json(
                f"{data_prefix}/deltas/{generation:012d}", delta
            )
            snapshot_reference = dict(parent_checkpoint["snapshot"])
            delta_reference = self._checkpoint_reference(delta_artifact)
            delta_references = [
                *parent_deltas,
                delta_reference,
            ]
            frontier_sha = self._delta_frontier_sha256(
                str(parent_checkpoint["frontier_sha256"]),
                delta_reference,
                len(frontier["completed_work_ids"]),
            )
        else:
            frontier_sha = self._frontier_sha256(frontier)
            snapshot = {
                "schema_version": OPS_SCHEMA_VERSION,
                "checkpoint_data_version": BACKFILL_CHECKPOINT_DATA_VERSION,
                "kind": "frontier_snapshot",
                "queue_id": queue_id,
                "plan_id": plan_id,
                "generation": generation,
                "created_at": created_at,
                "frontier_sha256": frontier_sha,
                "frontier": frontier,
            }
            snapshot_artifact = self.store.put_content_addressed_json(
                f"{data_prefix}/snapshots/{generation:012d}", snapshot
            )
            snapshot_reference = self._checkpoint_reference(snapshot_artifact)
            delta_references = []

        manifest = self._validate_checkpoint_manifest(
            {
                "schema_version": OPS_SCHEMA_VERSION,
                "checkpoint_version": BACKFILL_CHECKPOINT_VERSION,
                "queue_id": queue_id,
                "plan_id": plan_id,
                "generation": generation,
                "parent_sha256": parent_sha256,
                "created_at": created_at,
                "receipt_count": len(frontier["completed_work_ids"]),
                "frontier_sha256": frontier_sha,
                "snapshot": snapshot_reference,
                "deltas": delta_references,
            },
            plan=plan,
        )
        payload = _canonical_json(manifest)
        if len(payload) > CHECKPOINT_INDEX_MAX_BYTES:
            raise WhoScoredOpsStoreError("segmented checkpoint index exceeds 64 KiB")
        digest = hashlib.sha256(payload).hexdigest()
        key = (
            f"{self._checkpoint_manifest_prefix(queue_id, plan_id, generation)}/"
            f"{digest}.json"
        )
        artifact = self.store.put_json_immutable(key, manifest)
        return {**manifest, "frontier": frontier, "artifact": artifact}

    def _latest_segmented_manifest_key(
        self,
        queue_id: str,
        plan_id: str,
    ) -> Optional[tuple[int, str]]:
        """Find the newest v3 manifest with bounded non-recursive LISTs.

        Fixed-width decimal generations are stored as a 12-level radix tree.
        Lexicographically selecting the greatest digit at every level finds
        the greatest generation while every LIST returns at most ten entries.
        """

        prefix = f"{self._checkpoint_prefix(queue_id, plan_id)}/v3"
        digits: list[str] = []
        children = self.store.list_children(prefix)
        if not children:
            return None
        for depth in range(CHECKPOINT_GENERATION_DIGITS):
            if (
                not children
                or len(children) > 10
                or any(kind != "directory" for _name, kind in children)
                or any(len(name) != 1 or not name.isdigit() for name, _kind in children)
                or len({name for name, _kind in children}) != len(children)
            ):
                raise WhoScoredOpsStoreError(
                    f"invalid checkpoint radix level {depth}"
                )
            digit = max(name for name, _kind in children)
            digits.append(digit)
            prefix = f"{prefix}/{digit}"
            children = self.store.list_children(prefix)
        if (
            len(children) != 1
            or children[0][1] != "file"
            or re.fullmatch(r"[0-9a-f]{64}\.json", children[0][0]) is None
        ):
            raise WhoScoredOpsStoreError(
                "latest checkpoint radix leaf must contain one manifest"
            )
        generation = int("".join(digits))
        return generation, f"{prefix}/{children[0][0]}"

    def _latest_frontier_checkpoint(
        self,
        queue_id: str,
        plan_id: str,
        *,
        rebuild_if_missing: bool = True,
    ) -> dict[str, Any]:
        plan = self.load_plan(queue_id, plan_id)
        segmented = self._latest_segmented_manifest_key(queue_id, plan_id)
        if segmented is not None:
            generation, key = segmented
        else:
            # v2 used a flat generation directory.  Its unbounded scan is a
            # one-time compatibility path only; the first successful advance
            # writes v3 and every subsequent lookup uses the bounded radix.
            prefix = self._checkpoint_prefix(queue_id, plan_id)
            candidates: dict[int, list[str]] = {}
            expression = re.compile(
                rf"^{re.escape(prefix)}/([0-9]{{12}})/([0-9a-f]{{64}})\.json$"
            )
            for legacy_key in self.store.list_json_keys(prefix):
                match = expression.fullmatch(legacy_key)
                if match is None:
                    raise WhoScoredOpsStoreError(
                        f"invalid legacy backfill checkpoint key: {legacy_key}"
                    )
                candidates.setdefault(int(match.group(1)), []).append(legacy_key)
            generation = max(candidates) if candidates else -1
            keys = candidates.get(generation, [])
            if len(keys) > 1:
                raise WhoScoredOpsStoreError(
                    f"conflicting backfill checkpoints at generation {generation}"
                )
            key = keys[0] if keys else ""
        if generation < 0:
            if not rebuild_if_missing:
                raise WhoScoredOpsStoreError("backfill checkpoint is missing")
            rebuilt = self.receipts(queue_id, plan_id, plan=plan)
            return self._write_checkpoint(
                plan=plan,
                receipts=rebuilt,
                generation=0,
                parent_sha256=None,
            )
        value, payload = self.store.read_json_with_bytes(key)
        value = self.store._validate_content_addressed_json(key, value, payload)
        version = value.get("checkpoint_version")
        if version == LEGACY_BACKFILL_CHECKPOINT_VERSION:
            legacy = self._validate_legacy_checkpoint(value, plan=plan)
            checkpoint = {
                **legacy,
                "frontier": self._frontier_from_receipts(
                    plan, legacy["receipts"]
                ),
                "artifact": self._artifact_reference(self.store, key, payload),
            }
        elif version == BACKFILL_CHECKPOINT_VERSION:
            checkpoint = self._load_segmented_checkpoint(
                value, plan=plan, key=key, payload=payload
            )
        else:
            raise WhoScoredOpsStoreError("unsupported backfill checkpoint version")
        if checkpoint["generation"] != generation:
            raise WhoScoredOpsStoreError("backfill checkpoint generation mismatch")
        return checkpoint

    def latest_checkpoint(
        self,
        queue_id: str,
        plan_id: str,
        *,
        rebuild_if_missing: bool = True,
    ) -> dict[str, Any]:
        """Return the legacy materialized-receipt view for DQ/recovery.

        Normal controller continuation uses the compact frontier.  This public
        recovery API deliberately reloads every immutable receipt and proves
        that its projection equals the segmented checkpoint before returning
        the historical ``receipts`` shape.
        """

        plan = self.load_plan(queue_id, plan_id)
        checkpoint = self._latest_frontier_checkpoint(
            queue_id, plan_id, rebuild_if_missing=rebuild_if_missing
        )
        if checkpoint["checkpoint_version"] == LEGACY_BACKFILL_CHECKPOINT_VERSION:
            return {
                key: value for key, value in checkpoint.items() if key != "frontier"
            }
        receipts = sorted(
            self.receipts(queue_id, plan_id, plan=plan),
            key=lambda value: str(value["work_id"]),
        )
        rebuilt = self._frontier_from_receipts(plan, receipts)
        if rebuilt != checkpoint["frontier"]:
            raise WhoScoredOpsStoreError(
                "immutable receipts do not match the compact checkpoint frontier"
            )
        return {
            "schema_version": OPS_SCHEMA_VERSION,
            "checkpoint_version": BACKFILL_CHECKPOINT_VERSION,
            "queue_id": queue_id,
            "plan_id": plan_id,
            "generation": checkpoint["generation"],
            "parent_sha256": checkpoint["parent_sha256"],
            "created_at": checkpoint["created_at"],
            "receipts": receipts,
            "artifact": checkpoint["artifact"],
        }

    def checkpoint_progress(
        self,
        queue_id: str,
        plan_id: str,
    ) -> dict[str, Any]:
        checkpoint = self._latest_frontier_checkpoint(queue_id, plan_id)
        return {
            **self._progress_from_frontier(
                self.load_plan(queue_id, plan_id),
                checkpoint["frontier"],
            ),
            "checkpoint": checkpoint["artifact"],
            "checkpoint_generation": checkpoint["generation"],
        }

    def _bounded_pending_from_frontier(
        self,
        plan: Mapping[str, Any],
        frontier: Mapping[str, Any],
        *,
        limit: int,
        request_unit_limit: int,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= DEFAULT_WORK_LIMIT:
            raise WhoScoredOpsStoreError(
                f"work limit must be in 1..{DEFAULT_WORK_LIMIT}"
            )
        if not 200 <= request_unit_limit <= 10000:
            raise WhoScoredOpsStoreError("request-unit limit must be in 200..10000")
        return self._bound_pending_items(
            self._pending_work_from_frontier(plan, frontier, limit=limit),
            request_unit_limit=request_unit_limit,
        )

    def _bound_pending_items(
        self,
        pending: list[dict[str, Any]],
        *,
        request_unit_limit: int,
    ) -> list[dict[str, Any]]:
        bounded: list[dict[str, Any]] = []
        consumed = 0
        for item in pending:
            units = self.request_units(item)
            if not bounded and units > request_unit_limit:
                raise WhoScoredOpsStoreError(
                    "next backfill work item exceeds the request-unit limit: "
                    f"work_id={item['work_id']}, required={units}, "
                    f"limit={request_unit_limit}"
                )
            if bounded and consumed + units > request_unit_limit:
                break
            bounded.append(item)
            consumed += units
        return bounded

    def _validate_batch(
        self,
        batch: Mapping[str, Any],
        *,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        if set(batch) != {
            "schema_version",
            "batch_version",
            "queue_id",
            "plan_id",
            "batch_id",
            "created_at",
            "checkpoint",
            "work_items",
            "request_units",
        }:
            raise WhoScoredOpsStoreError("invalid backfill batch schema")
        checkpoint = batch.get("checkpoint")
        work_items = batch.get("work_items")
        if (
            batch.get("schema_version") != OPS_SCHEMA_VERSION
            or batch.get("batch_version") != BACKFILL_BATCH_VERSION
            or batch.get("queue_id") != plan["queue_id"]
            or batch.get("plan_id") != plan["plan_id"]
            or not isinstance(batch.get("batch_id"), str)
            or not batch.get("batch_id")
            or not isinstance(batch.get("created_at"), str)
            or not isinstance(checkpoint, dict)
            or set(checkpoint) != {"uri", "key", "sha256", "bytes"}
            or not isinstance(work_items, list)
            or len(work_items) > DEFAULT_WORK_LIMIT
            or type(batch.get("request_units")) is not int
        ):
            raise WhoScoredOpsStoreError("invalid backfill batch identity")
        for item in work_items:
            if not isinstance(item, dict):
                raise WhoScoredOpsStoreError("invalid work item in backfill batch")
            self.validate_work_item(plan, item)
        if len({str(item["work_id"]) for item in work_items}) != len(work_items):
            raise WhoScoredOpsStoreError("duplicate work item in backfill batch")
        if batch["request_units"] != sum(
            self.request_units(item) for item in work_items
        ):
            raise WhoScoredOpsStoreError("backfill batch request units mismatch")
        return dict(batch)

    def _load_batch(
        self,
        queue_id: str,
        plan_id: str,
        batch_id: str,
    ) -> dict[str, Any]:
        plan = self.load_plan(queue_id, plan_id)
        prefix = self._batch_prefix(queue_id, plan_id, batch_id)
        keys = self.store.list_json_keys(prefix)
        if len(keys) != 1:
            raise WhoScoredOpsStoreError(
                f"backfill batch must have one immutable object: {batch_id}"
            )
        key = keys[0]
        value, payload = self.store.read_json_with_bytes(key)
        value = self.store._validate_content_addressed_json(key, value, payload)
        batch = self._validate_batch(value, plan=plan)
        if batch["batch_id"] != batch_id:
            raise WhoScoredOpsStoreError("backfill batch id mismatch")
        return {
            **batch,
            "artifact": self._artifact_reference(self.store, key, payload),
        }

    def create_batch(
        self,
        queue_id: str,
        plan_id: str,
        *,
        batch_id: str,
        limit: int = DEFAULT_WORK_LIMIT,
        request_unit_limit: int = 1000,
    ) -> dict[str, Any]:
        """Freeze one controller batch from the latest validated checkpoint."""

        plan = self.load_plan(queue_id, plan_id)
        prefix = self._batch_prefix(queue_id, plan_id, batch_id)
        if self.store.list_json_keys(prefix):
            return self._load_batch(queue_id, plan_id, batch_id)
        checkpoint = self._latest_frontier_checkpoint(queue_id, plan_id)
        work_items = self._bounded_pending_from_frontier(
            plan,
            checkpoint["frontier"],
            limit=limit,
            request_unit_limit=request_unit_limit,
        )
        batch = self._validate_batch(
            {
                "schema_version": OPS_SCHEMA_VERSION,
                "batch_version": BACKFILL_BATCH_VERSION,
                "queue_id": queue_id,
                "plan_id": plan_id,
                "batch_id": batch_id,
                # Batch identity is deterministic for one DagRun and source
                # checkpoint, including concurrent controller retries.
                "created_at": str(checkpoint["created_at"]),
                "checkpoint": checkpoint["artifact"],
                "work_items": work_items,
                "request_units": sum(self.request_units(item) for item in work_items),
            },
            plan=plan,
        )
        artifact = self.store.put_content_addressed_json(prefix, batch)
        return {**batch, "artifact": artifact}

    def advance_batch(
        self,
        queue_id: str,
        plan_id: str,
        *,
        batch_id: str,
    ) -> dict[str, Any]:
        """Merge at most one batch (<=100 receipts) into a new checkpoint."""

        plan = self.load_plan(queue_id, plan_id)
        batch = self._load_batch(queue_id, plan_id, batch_id)
        checkpoint = self._latest_frontier_checkpoint(queue_id, plan_id)
        completed = set(checkpoint["frontier"]["completed_work_ids"])
        batch_ids = {str(item["work_id"]) for item in batch["work_items"]}
        if batch_ids <= completed:
            selected = checkpoint
        else:
            expected_parent = batch["checkpoint"]
            if checkpoint["artifact"]["sha256"] != expected_parent.get(
                "sha256"
            ) and checkpoint.get("parent_sha256") != expected_parent.get("sha256"):
                raise WhoScoredOpsStoreError(
                    "backfill checkpoint advanced before this batch was merged"
                )
            delta_receipts: list[dict[str, Any]] = []
            for item in batch["work_items"]:
                work_id = str(item["work_id"])
                if work_id in completed:
                    continue
                receipt = self._receipt_for_work(
                    queue_id,
                    plan_id,
                    work_id,
                    plan=plan,
                )
                if receipt is not None:
                    delta_receipts.append(receipt)
            selected = (
                self._write_checkpoint(
                    plan=plan,
                    receipts=[
                        *list(checkpoint.get("receipts", [])),
                        *delta_receipts,
                    ],
                    generation=int(checkpoint["generation"]) + 1,
                    parent_sha256=str(checkpoint["artifact"]["sha256"]),
                    parent_checkpoint=checkpoint,
                    delta_receipts=delta_receipts,
                )
                if delta_receipts
                else checkpoint
            )
        return {
            **self._progress_from_frontier(plan, selected["frontier"]),
            "checkpoint": selected["artifact"],
            "checkpoint_generation": selected["generation"],
            "batch_id": batch_id,
            "batch_work_items": len(batch["work_items"]),
        }
