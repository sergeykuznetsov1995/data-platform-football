"""Run-bound evidence from disposable acceptance replay control effects."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from typing import Any


REPLAY_CONTROL_EFFECTS_SCHEMA_VERSION = "fbref-replay-control-effects-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_TARGET_FIELDS = {
    "ordinal",
    "target_id",
    "replay_target_id",
    "source_logical_refresh_id",
    "logical_refresh_id",
    "status",
    "target_status",
    "page_kind",
    "source_ids",
    "frontier_state",
    "last_content_hash",
    "content_hash",
    "parser_version",
    "typed_parser_version",
    "stateful_parser_version",
    "observation_status",
    "generic_status",
    "typed_status",
    "stateful_status",
    "validation_status",
    "evidence_class",
    "latest_guarded",
}
_DATASET_FIELDS = {
    "ordinal",
    "target_id",
    "replay_target_id",
    "content_hash",
    "parser_version",
    "dataset",
    "availability",
    "parse_status",
    "persistence_status",
    "validation_status",
    "row_count",
    "empty_reason",
}
_CORE_FIELDS = {
    "schema_version",
    "control_run_id",
    "source_run_id",
    "mode",
    "targets",
    "datasets",
}


def _uuid(value: object, name: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a UUID") from exc


def _text(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _sha256(value: object, name: str) -> str:
    normalized = _text(value, name).casefold()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be a SHA-256")
    return normalized


def _json_copy(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    try:
        copied = json.loads(
            json.dumps(
                dict(value),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be JSON serializable") from exc
    if not isinstance(copied, dict):  # pragma: no cover - mapping guarantees it
        raise ValueError(f"{name} must be a mapping")
    return copied


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def make_replay_control_target_id(
    replay_run_id: object, source_target_id: object
) -> str:
    replay = _uuid(replay_run_id, "replay_run_id")
    source = _text(source_target_id, "source_target_id")
    return f"fbref:acceptance-replay:{replay}:{source}"


def make_replay_control_refresh_id(
    replay_run_id: object, source_refresh_id: object
) -> str:
    replay = uuid.UUID(_uuid(replay_run_id, "replay_run_id"))
    source = _uuid(source_refresh_id, "source_refresh_id")
    return str(
        uuid.uuid5(replay, f"fbref-acceptance-control-refresh:{source}")
    )


def _normalized_target(
    value: Mapping[str, Any], *, replay_run_id: str
) -> dict[str, Any]:
    if set(value) != _TARGET_FIELDS:
        raise ValueError("replay control target fields are invalid")
    ordinal = value.get("ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        raise ValueError("replay control target ordinal is invalid")
    target_id = _text(value.get("target_id"), "target_id")
    source_ids = _json_copy(value.get("source_ids"), "source_ids")
    match_id = _text(source_ids.get("match_id"), "source match_id")
    if target_id != f"fbref:match:{match_id}":
        raise ValueError("replay control target is not a canonical match")
    replay_target_id = _text(
        value.get("replay_target_id"), "replay_target_id"
    )
    if replay_target_id != make_replay_control_target_id(
        replay_run_id, target_id
    ):
        raise ValueError("replay control target identity is invalid")
    source_refresh_id = _uuid(
        value.get("source_logical_refresh_id"),
        "source_logical_refresh_id",
    )
    logical_refresh_id = _uuid(
        value.get("logical_refresh_id"), "logical_refresh_id"
    )
    if logical_refresh_id != make_replay_control_refresh_id(
        replay_run_id, source_refresh_id
    ):
        raise ValueError("replay control logical refresh identity is invalid")
    latest_guarded = value.get("latest_guarded")
    if not isinstance(latest_guarded, bool):
        raise ValueError("latest_guarded must be boolean")
    evidence_class = value.get("evidence_class")
    if evidence_class is not None:
        evidence_class = _text(evidence_class, "evidence_class")
    return {
        "ordinal": ordinal,
        "target_id": target_id,
        "replay_target_id": replay_target_id,
        "source_logical_refresh_id": source_refresh_id,
        "logical_refresh_id": logical_refresh_id,
        "status": _text(value.get("status"), "status"),
        "target_status": _text(
            value.get("target_status"), "target_status"
        ),
        "page_kind": _text(value.get("page_kind"), "page_kind"),
        "source_ids": source_ids,
        "frontier_state": _text(
            value.get("frontier_state"), "frontier_state"
        ),
        "last_content_hash": _sha256(
            value.get("last_content_hash"), "last_content_hash"
        ),
        "content_hash": _sha256(value.get("content_hash"), "content_hash"),
        "parser_version": _text(
            value.get("parser_version"), "parser_version"
        ),
        "typed_parser_version": _text(
            value.get("typed_parser_version"), "typed_parser_version"
        ),
        "stateful_parser_version": _text(
            value.get("stateful_parser_version"),
            "stateful_parser_version",
        ),
        "observation_status": _text(
            value.get("observation_status"), "observation_status"
        ),
        "generic_status": _text(
            value.get("generic_status"), "generic_status"
        ),
        "typed_status": _text(value.get("typed_status"), "typed_status"),
        "stateful_status": _text(
            value.get("stateful_status"), "stateful_status"
        ),
        "validation_status": _text(
            value.get("validation_status"), "validation_status"
        ),
        "evidence_class": evidence_class,
        "latest_guarded": latest_guarded,
    }


def _normalized_dataset(
    value: Mapping[str, Any],
    *,
    replay_run_id: str,
    target_ordinals: Mapping[str, int],
) -> dict[str, Any]:
    if set(value) != _DATASET_FIELDS:
        raise ValueError("replay control dataset fields are invalid")
    target_id = _text(value.get("target_id"), "dataset target_id")
    if target_id not in target_ordinals:
        raise ValueError("replay control dataset target is unknown")
    ordinal = value.get("ordinal")
    if isinstance(ordinal, bool) or ordinal != target_ordinals[target_id]:
        raise ValueError("replay control dataset ordinal is invalid")
    replay_target_id = _text(
        value.get("replay_target_id"), "dataset replay_target_id"
    )
    if replay_target_id != make_replay_control_target_id(
        replay_run_id, target_id
    ):
        raise ValueError("replay control dataset target identity is invalid")
    row_count = value.get("row_count")
    if (
        isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count < 0
    ):
        raise ValueError("replay control dataset row_count is invalid")
    empty_reason = value.get("empty_reason")
    if empty_reason is not None:
        empty_reason = _text(empty_reason, "empty_reason")
    return {
        "ordinal": ordinal,
        "target_id": target_id,
        "replay_target_id": replay_target_id,
        "content_hash": _sha256(
            value.get("content_hash"), "dataset content_hash"
        ),
        "parser_version": _text(
            value.get("parser_version"), "dataset parser_version"
        ),
        "dataset": _text(value.get("dataset"), "dataset"),
        "availability": _text(
            value.get("availability"), "dataset availability"
        ),
        "parse_status": _text(
            value.get("parse_status"), "dataset parse_status"
        ),
        "persistence_status": _text(
            value.get("persistence_status"), "dataset persistence_status"
        ),
        "validation_status": _text(
            value.get("validation_status"), "dataset validation_status"
        ),
        "row_count": row_count,
        "empty_reason": empty_reason,
    }


def _normalized_core(
    *,
    control_run_id: object,
    source_run_id: object,
    mode: object,
    targets: object,
    datasets: object,
) -> dict[str, Any]:
    replay = _uuid(control_run_id, "control_run_id")
    source = _uuid(source_run_id, "source_run_id")
    normalized_mode = _text(mode, "mode").casefold()
    if normalized_mode not in {"sequential", "batch"}:
        raise ValueError("replay control mode is invalid")
    if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
        raise ValueError("replay control targets are invalid")
    if not 1 <= len(targets) <= 25:
        raise ValueError("replay control target count is invalid")
    normalized_targets = []
    for item in targets:
        if not isinstance(item, Mapping):
            raise ValueError("replay control target is invalid")
        normalized_targets.append(
            _normalized_target(item, replay_run_id=replay)
        )
    normalized_targets.sort(key=lambda item: item["ordinal"])
    if [item["ordinal"] for item in normalized_targets] != list(
        range(len(normalized_targets))
    ):
        raise ValueError("replay control target ordinals are not contiguous")
    target_ids = [item["target_id"] for item in normalized_targets]
    replay_target_ids = [
        item["replay_target_id"] for item in normalized_targets
    ]
    source_refresh_ids = [
        item["source_logical_refresh_id"] for item in normalized_targets
    ]
    replay_refresh_ids = [
        item["logical_refresh_id"] for item in normalized_targets
    ]
    if any(
        len(values) != len(set(values))
        for values in (
            target_ids,
            replay_target_ids,
            source_refresh_ids,
            replay_refresh_ids,
        )
    ):
        raise ValueError("replay control target or logical refresh is duplicate")
    if not isinstance(datasets, Sequence) or isinstance(
        datasets, (str, bytes)
    ):
        raise ValueError("replay control datasets are invalid")
    if len(datasets) > 10_000:
        raise ValueError("replay control dataset count is invalid")
    target_ordinals = {
        item["target_id"]: item["ordinal"] for item in normalized_targets
    }
    normalized_datasets = []
    for item in datasets:
        if not isinstance(item, Mapping):
            raise ValueError("replay control dataset is invalid")
        normalized_datasets.append(
            _normalized_dataset(
                item,
                replay_run_id=replay,
                target_ordinals=target_ordinals,
            )
        )
    normalized_datasets.sort(
        key=lambda item: (
            item["ordinal"],
            item["parser_version"],
            item["dataset"],
        )
    )
    dataset_keys = [
        (
            item["replay_target_id"],
            item["content_hash"],
            item["parser_version"],
            item["dataset"],
        )
        for item in normalized_datasets
    ]
    if len(dataset_keys) != len(set(dataset_keys)):
        raise ValueError("replay control dataset identity is duplicate")
    return {
        "schema_version": REPLAY_CONTROL_EFFECTS_SCHEMA_VERSION,
        "control_run_id": replay,
        "source_run_id": source,
        "mode": normalized_mode,
        "targets": normalized_targets,
        "datasets": normalized_datasets,
    }


def build_replay_control_effects(
    *,
    control_run_id: object,
    source_run_id: object,
    mode: object,
    targets: Sequence[Mapping[str, Any]],
    datasets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    core = _normalized_core(
        control_run_id=control_run_id,
        source_run_id=source_run_id,
        mode=mode,
        targets=targets,
        datasets=datasets,
    )
    return {**core, "artifact_sha256": _digest(core)}


def normalize_replay_control_effects(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        *_CORE_FIELDS,
        "artifact_sha256",
    }:
        raise ValueError("replay control effects fields are invalid")
    if value.get("schema_version") != REPLAY_CONTROL_EFFECTS_SCHEMA_VERSION:
        raise ValueError("replay control effects schema is invalid")
    core = _normalized_core(
        control_run_id=value.get("control_run_id"),
        source_run_id=value.get("source_run_id"),
        mode=value.get("mode"),
        targets=value.get("targets"),
        datasets=value.get("datasets"),
    )
    supplied = _sha256(value.get("artifact_sha256"), "artifact digest")
    if supplied != _digest(core):
        raise ValueError("replay control effects digest does not match")
    return {**core, "artifact_sha256": supplied}
