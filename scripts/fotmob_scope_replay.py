#!/usr/bin/env python3
"""Replay stored FotMob competition profiles without network or database access."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import tempfile
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from scrapers.fotmob.catalog import (  # noqa: E402
    CLASSIFIER_VERSION,
    CatalogShapeError,
    classify_competition,
    competition_from_league_payload,
    discover_competitions,
    parse_seasons,
)
from scrapers.fotmob.catalog_contract import build_catalog_contract  # noqa: E402
from scrapers.fotmob.domain import (  # noqa: E402
    CompetitionRef,
    ScopeClassification,
    ScopeDecision,
    SeasonRef,
)
from scrapers.fotmob.planner import RunMode, ScopeLane, plan_seasons  # noqa: E402
from scrapers.fotmob.repository import PARSER_VERSION  # noqa: E402


REPORT_SCHEMA = "fotmob-scope-replay-v1"
OBSERVATIONS_FILE = "profile-observations.json"
CATALOG_URL = "https://www.fotmob.com/api/data/allLeagues"
PROFILE_ENDPOINT = "/api/data/leagues"
AUTOMATIC_ENTITIES = (
    "season",
    "leaderboards",
    "matches",
    "teams",
    "players",
    "transfers",
)
AUTOMATIC_ENTITY_POLICY = {
    "match_policy": "finished_only",
    "leaderboard_policy": "all_advertised",
    "team_policy": "global_observed_snapshot",
    "player_policy": "global_observed_snapshot",
    "transfer_policy": {
        "window": "1year",
        "pagination": "unique_hits",
        "completion_scope": "included_ids",
        "completion_signature": "catalog_contract",
    },
}


class ReplayError(RuntimeError):
    """Stored evidence is incomplete, corrupt, or contradicts itself."""


@dataclass(frozen=True, slots=True)
class _Profile:
    competition_id: int
    payload: Mapping[str, Any] | None
    content_hash: str
    fetched_at: str
    validated_at: str
    target_key: str


@dataclass(frozen=True, slots=True)
class _Catalog:
    payload: Mapping[str, Any]
    content_hash: str
    fetched_at: str
    validated_at: str
    target_key: str


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _positive_id(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ReplayError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ReplayError(f"{field} must be a positive integer") from exc
    if parsed <= 0 or str(parsed) != str(value).strip():
        raise ReplayError(f"{field} must be a positive integer")
    return parsed


def _timestamp(value: Any, *, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ReplayError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _profile_id(canonical_url: Any) -> int | None:
    if not isinstance(canonical_url, str):
        return None
    try:
        parsed = urlparse(canonical_url)
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or parsed.netloc != "www.fotmob.com"
        or parsed.path != PROFILE_ENDPOINT
        or parsed.params
        or parsed.fragment
    ):
        return None
    try:
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None
    if set(query) != {"id"} or len(query["id"]) != 1:
        return None
    try:
        competition_id = _positive_id(query["id"][0], field="profile URL id")
    except ReplayError:
        return None
    expected = f"https://www.fotmob.com/api/data/leagues?id={competition_id}"
    return competition_id if canonical_url == expected else None


def _raw_uri_prefix(raw_uri: Any, *, blob_key: str, path: Path) -> str:
    if not isinstance(raw_uri, str) or not raw_uri:
        raise ReplayError(f"raw target manifest has no raw_uri: {path}")
    try:
        parsed = urlparse(raw_uri)
    except ValueError as exc:
        raise ReplayError(f"raw target manifest has invalid raw_uri: {path}") from exc
    if (
        parsed.scheme not in {"file", "s3"}
        or parsed.params
        or parsed.query
        or parsed.fragment
        or any(part in {".", ".."} for part in parsed.path.split("/"))
        or (parsed.scheme == "file" and parsed.netloc not in {"", "localhost"})
        or (parsed.scheme == "s3" and not parsed.netloc)
    ):
        raise ReplayError(f"raw target manifest has invalid raw_uri: {path}")
    suffix = f"/{blob_key}"
    if not raw_uri.endswith(suffix):
        raise ReplayError(f"raw target manifest raw_uri does not match blob_key: {path}")
    prefix = raw_uri[: -len(suffix)]
    if not prefix or prefix.endswith("/"):
        raise ReplayError(f"raw target manifest has invalid raw_uri: {path}")
    return prefix


def _load_manifest_payload(
    raw_root: Path, path: Path
) -> tuple[
    str,
    int | None,
    Mapping[str, Any] | None,
    str,
    str,
    str,
    str,
    str,
] | None:
    try:
        raw_manifest = path.read_bytes()
        manifest = json.loads(raw_manifest)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ReplayError(f"invalid raw target manifest {path}: {exc}") from exc
    if not isinstance(manifest, Mapping):
        raise ReplayError(f"raw target manifest is not an object: {path}")
    canonical_url = manifest.get("canonical_url")
    competition_id = _profile_id(canonical_url)
    kind = "profile" if competition_id is not None else "catalog"
    if competition_id is None and canonical_url != CATALOG_URL:
        return None
    target_key = str(manifest.get("target_key") or "")
    expected_target_key = _sha256(str(canonical_url).encode("utf-8"))
    expected_manifest_path = (
        raw_root
        / "targets"
        / "sha256"
        / expected_target_key[:2]
        / f"{expected_target_key}.json"
    )
    if (
        target_key != expected_target_key
        or path != expected_manifest_path
        or path.is_symlink()
        or not path.resolve().is_relative_to(raw_root)
    ):
        raise ReplayError(f"profile target key does not match its URL: {path}")
    content_hash = str(manifest.get("content_hash") or "")
    expected_blob_key = (
        f"blobs/sha256/{content_hash[:2]}/{content_hash}.json.gz"
        if len(content_hash) == 64
        else ""
    )
    if (
        manifest.get("manifest_version") != "fotmob-raw-v1"
        or manifest.get("source") != "fotmob"
        or manifest.get("hash_algorithm") != "sha256"
        or len(content_hash) != 64
        or any(character not in "0123456789abcdef" for character in content_hash)
        or manifest.get("blob_key") != expected_blob_key
        or manifest.get("compression") != "gzip"
    ):
        raise ReplayError(f"profile manifest contract is invalid: {path}")
    raw_uri_prefix = _raw_uri_prefix(
        manifest.get("raw_uri"), blob_key=expected_blob_key, path=path
    )
    blob_path = raw_root / expected_blob_key
    if blob_path.is_symlink() or not blob_path.resolve().is_relative_to(raw_root):
        raise ReplayError(f"raw blob must not be a symlink: {blob_path}")
    try:
        compressed = blob_path.read_bytes()
        body = gzip.decompress(compressed)
    except (OSError, gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise ReplayError(f"profile blob is missing or corrupt: {blob_path}") from exc
    if (
        _sha256(body) != content_hash
        or manifest.get("decoded_bytes") != len(body)
        or manifest.get("stored_bytes") != len(compressed)
    ):
        raise ReplayError(f"profile blob hash/length differs: {blob_path}")
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ReplayError(f"profile payload is not valid JSON: {blob_path}") from exc
    if kind == "catalog" and not isinstance(payload, Mapping):
        raise ReplayError(f"allLeagues payload is not an object: {blob_path}")
    if kind == "profile" and payload is not None and not isinstance(payload, Mapping):
        raise ReplayError(f"profile payload is neither an object nor null: {blob_path}")
    fetched_at = _timestamp(manifest.get("fetched_at"), field="fetched_at")
    validated_at = _timestamp(manifest.get("validated_at"), field="validated_at")
    if datetime.fromisoformat(validated_at) < datetime.fromisoformat(fetched_at):
        raise ReplayError(f"raw validation predates its fetch: {path}")
    return (
        kind,
        competition_id,
        payload,
        content_hash,
        fetched_at,
        validated_at,
        target_key,
        raw_uri_prefix,
    )


def _stored_evidence(raw_root: Path) -> tuple[_Catalog, dict[int, _Profile]]:
    targets = raw_root / "targets" / "sha256"
    if not targets.is_dir():
        raise ReplayError(f"raw root has no targets/sha256 directory: {raw_root}")
    catalog: _Catalog | None = None
    profiles: dict[int, _Profile] = {}
    store_prefix: str | None = None
    for path in sorted(targets.rglob("*.json")):
        loaded = _load_manifest_payload(raw_root, path)
        if loaded is None:
            continue
        (
            kind,
            competition_id,
            payload,
            content_hash,
            fetched_at,
            validated_at,
            target_key,
            raw_uri_prefix,
        ) = loaded
        if store_prefix is None:
            store_prefix = raw_uri_prefix
        elif raw_uri_prefix != store_prefix:
            raise ReplayError("raw manifests do not belong to one raw-store prefix")
        if kind == "catalog":
            assert isinstance(payload, Mapping)
            if catalog is not None:
                raise ReplayError("raw root contains duplicate allLeagues manifests")
            catalog = _Catalog(
                payload,
                content_hash,
                fetched_at,
                validated_at,
                target_key,
            )
            continue
        assert competition_id is not None
        details = payload.get("details") if isinstance(payload, Mapping) else None
        if isinstance(details, Mapping) and details.get("id") is not None:
            payload_id = _positive_id(details.get("id"), field="details.id")
            if payload_id != competition_id:
                raise ReplayError(
                    "profile URL/payload identity differs for competition "
                    f"{competition_id}"
                )
        profile = _Profile(
            competition_id=competition_id,
            payload=payload,
            content_hash=content_hash,
            fetched_at=fetched_at,
            validated_at=validated_at,
            target_key=target_key,
        )
        previous = profiles.get(competition_id)
        if previous is not None:
            raise ReplayError(
                f"duplicate profile identity for competition {competition_id}"
            )
        profiles[competition_id] = profile
    if catalog is None:
        raise ReplayError(f"raw root contains no stored allLeagues payload: {raw_root}")
    return catalog, profiles


def _load_observations(
    raw_root: Path, *, catalog: _Catalog
) -> list[dict[str, Any]]:
    path = raw_root / OBSERVATIONS_FILE
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ReplayError(f"invalid {OBSERVATIONS_FILE}: {exc}") from exc
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != "fotmob-profile-observations-v1"
        or payload.get("catalog_target_key") != catalog.target_key
        or payload.get("catalog_content_hash") != catalog.content_hash
    ):
        raise ReplayError(
            f"{OBSERVATIONS_FILE} is not bound to the replayed catalog"
        )
    raw_items = payload.get("observations")
    if not isinstance(raw_items, list) or any(
        not isinstance(item, Mapping) for item in raw_items
    ):
        raise ReplayError(f"{OBSERVATIONS_FILE} must contain an observations list")
    observations: list[dict[str, Any]] = []
    seen_attempts: dict[tuple[int, str], dict[str, Any]] = {}
    seen_times: dict[tuple[int, str], str] = {}
    for index, raw in enumerate(raw_items):
        competition_id = _positive_id(
            raw.get("competition_id"), field=f"observations[{index}].competition_id"
        )
        outcome = str(raw.get("outcome") or "").strip().casefold()
        if outcome not in {
            "timeout",
            "retryable_failure",
            "not_found",
            "not_available",
        }:
            raise ReplayError(f"observations[{index}].outcome is unsupported")
        profile_url = (
            "https://www.fotmob.com/api/data/leagues?id=" f"{competition_id}"
        )
        if raw.get("profile_target_key") != _sha256(profile_url.encode("utf-8")):
            raise ReplayError(
                f"observations[{index}] is not bound to the profile target"
            )
        attempt_id = str(raw.get("attempt_id") or "").strip()
        attempts = raw.get("attempts")
        stale = raw.get("stale")
        source_validated = raw.get("source_validated")
        if (
            not attempt_id
            or attempt_id.startswith("raw-null:")
            or type(attempts) is not int
            or attempts < 1
            or type(stale) is not bool
            or type(source_validated) is not bool
        ):
            raise ReplayError(
                f"observations[{index}] has incomplete attempt provenance"
            )
        http_status = raw.get("http_status")
        if http_status is not None and type(http_status) is not int:
            raise ReplayError(f"observations[{index}].http_status must be an integer")
        json_null = raw.get("json_null") is True
        if (
            outcome in {"not_found", "not_available"}
            and source_validated
            and not stale
            and http_status in {200, 304}
            and json_null
        ):
            raise ReplayError(
                f"observations[{index}] authoritative JSON null requires a raw null manifest"
            )
        authoritative = (
            outcome in {"not_found", "not_available"}
            and source_validated
            and not stale
            and http_status in {204, 404}
        )
        stale_null_replay = (
            outcome == "not_available"
            and stale
            and not source_validated
            and json_null
        )
        if (
            outcome in {"not_found", "not_available"}
            and not authoritative
            and not stale_null_replay
        ):
            raise ReplayError(
                f"observations[{index}] absence is not source-authoritative"
            )
        if outcome in {"timeout", "retryable_failure"} and source_validated:
            raise ReplayError(
                f"observations[{index}] transient failure cannot be source-validated"
            )
        observed_at = _timestamp(
            raw.get("observed_at"), field=f"observations[{index}].observed_at"
        )
        if datetime.fromisoformat(observed_at) < datetime.fromisoformat(
            catalog.fetched_at
        ):
            raise ReplayError(
                f"observations[{index}] predates the replayed catalog fetch"
            )
        normalized = {
            "competition_id": competition_id,
            "catalog_name": str(
                raw.get("catalog_name") or f"competition-{competition_id}"
            ).strip(),
            "outcome": outcome,
            "http_status": http_status,
            "json_null": json_null,
            "authoritative": authoritative,
            "attempt_id": attempt_id,
            "attempts": attempts,
            "source_validated": source_validated,
            "stale": stale,
            "profile_target_key": raw["profile_target_key"],
            "observed_at": observed_at,
        }
        attempt_key = (competition_id, attempt_id)
        previous = seen_attempts.get(attempt_key)
        if previous is not None and previous != normalized:
            raise ReplayError(
                f"observations[{index}] reuses an attempt identity inconsistently"
            )
        if previous is not None:
            continue
        time_key = (competition_id, observed_at)
        event_effect = "authoritative_absence" if authoritative else "transient"
        previous_at_time = seen_times.get(time_key)
        if previous_at_time is not None and previous_at_time != event_effect:
            raise ReplayError(
                f"observations[{index}] has ambiguous events at the same timestamp"
            )
        seen_attempts[attempt_key] = normalized
        seen_times[time_key] = event_effect
        observations.append(normalized)
    return observations


def _successful_decision(
    profile: _Profile,
    catalog_ref: CompetitionRef,
    *,
    conflict_fields: Sequence[str] = (),
) -> tuple[dict[str, Any], tuple[SeasonRef, ...], ScopeClassification | None]:
    assert isinstance(profile.payload, Mapping)
    try:
        source = competition_from_league_payload(profile.payload)
        classification = classify_competition(catalog_ref, source)
    except (CatalogShapeError, TypeError, ValueError) as exc:
        classification = ScopeClassification(
            catalog_ref,
            ScopeDecision.REVIEW_REQUIRED,
            f"invalid identity-matching profile: {type(exc).__name__}: {exc}",
            "review_invalid_profile",
        )
        return (
            {
                "competition_id": profile.competition_id,
                "catalog_name": catalog_ref.name,
                "profile_name": None,
                "source_gender": None,
                "source_age_group": None,
                "source_type": None,
                "probe_status": "invalid",
                "decision": classification.decision.value,
                "reason": classification.reason,
                "policy_rule": classification.policy_rule,
                "classifier_version": CLASSIFIER_VERSION,
                "profile_content_hash": profile.content_hash,
                "profile_target_key": profile.target_key,
                "profile_fetched_at": profile.fetched_at,
                "observed_at": profile.validated_at,
                "source_seasons": [],
            },
            (),
            classification,
        )
    if conflict_fields:
        classification = ScopeClassification(
            source,
            ScopeDecision.REVIEW_REQUIRED,
            "conflicting allLeagues metadata: " + ",".join(conflict_fields),
            "review_catalog_conflict",
        )
    try:
        seasons = parse_seasons(profile.payload, source)
    except (CatalogShapeError, TypeError, ValueError) as exc:
        if classification.decision is ScopeDecision.INCLUDED:
            classification = type(classification)(
                source,
                ScopeDecision.REVIEW_REQUIRED,
                f"invalid source season inventory: {type(exc).__name__}: {exc}",
                "review_invalid_seasons",
            )
        seasons = ()
    decision = {
        "competition_id": source.competition_id,
        "catalog_name": catalog_ref.name,
        "profile_name": source.name,
        "source_gender": source.gender,
        "source_age_group": source.age_group,
        "source_type": source.competition_type,
        "probe_status": "success",
        "decision": classification.decision.value,
        "reason": classification.reason,
        "policy_rule": classification.policy_rule,
        "classifier_version": CLASSIFIER_VERSION,
        "profile_content_hash": profile.content_hash,
        "profile_target_key": profile.target_key,
        "profile_fetched_at": profile.fetched_at,
        "observed_at": profile.validated_at,
        "source_seasons": [season.source_season_key for season in seasons],
    }
    return decision, seasons, classification


def _unavailable_decisions(
    catalog: Sequence[CompetitionRef],
    observations: Iterable[Mapping[str, Any]],
    *,
    catalog_observed_at: str,
    carried_decisions: Mapping[int, Mapping[str, Any]],
    successful_ids: set[int],
) -> tuple[list[dict[str, Any]], list[ScopeClassification]]:
    grouped: dict[int, list[Mapping[str, Any]]] = {}
    for observation in observations:
        competition_id = int(observation["competition_id"])
        if competition_id in successful_ids:
            raise ReplayError(
                f"competition {competition_id} has both a stored profile and failure evidence"
            )
        grouped.setdefault(competition_id, []).append(observation)
    decisions: list[dict[str, Any]] = []
    classifications: list[ScopeClassification] = []
    for competition in catalog:
        competition_id = competition.competition_id
        if competition_id in successful_ids:
            continue
        items = grouped.get(competition_id, [])
        if not items:
            classification = classify_competition(competition)
            classifications.append(classification)
            decisions.append(
                {
                    "competition_id": competition_id,
                    "catalog_name": competition.name,
                    "profile_name": None,
                    "source_gender": None,
                    "source_age_group": None,
                    "source_type": None,
                    "probe_status": "pending",
                    "decision": classification.decision.value,
                    "reason": classification.reason,
                    "policy_rule": classification.policy_rule,
                    "classifier_version": CLASSIFIER_VERSION,
                    "profile_content_hash": None,
                    "profile_target_key": _sha256(
                        (
                            "https://www.fotmob.com/api/data/leagues?id="
                            f"{competition_id}"
                        ).encode("utf-8")
                    ),
                    "profile_fetched_at": None,
                    "observed_at": catalog_observed_at,
                    "source_seasons": [],
                }
            )
            continue
        ordered = sorted(items, key=lambda item: str(item["observed_at"]))
        authoritative = [item for item in ordered if bool(item["authoritative"])]
        misses = len({str(item["attempt_id"]) for item in authoritative})
        latest = ordered[-1]
        if not bool(latest["authoritative"]):
            probe_status = "pending"
            decision = ScopeDecision.PENDING_PROBE.value
            reason = (
                "identity-matching /leagues profile probe is pending after a "
                "transient failure"
            )
            policy_rule = "probe_retryable"
        elif misses >= 2:
            probe_status = "dead"
            decision = ScopeDecision.EXCLUDED.value
            reason = "identity-matching /leagues profile was authoritatively absent twice"
            policy_rule = "exclude_dead_profile"
        elif misses == 1:
            probe_status = "not_found"
            decision = ScopeDecision.PENDING_PROBE.value
            reason = "identity-matching /leagues profile was authoritatively absent once"
            policy_rule = "probe_not_found"
        else:
            probe_status = "pending"
            decision = ScopeDecision.PENDING_PROBE.value
            reason = (
                "identity-matching /leagues profile probe is pending after a "
                "transient failure"
            )
            policy_rule = "probe_retryable"
        classification = ScopeClassification(
            competition,
            ScopeDecision(decision),
            reason,
            policy_rule,
        )
        classifications.append(classification)
        carried = carried_decisions.get(competition_id, {})
        decisions.append(
            {
                "competition_id": competition_id,
                "catalog_name": competition.name,
                "profile_name": carried.get("profile_name"),
                "source_gender": carried.get("source_gender"),
                "source_age_group": carried.get("source_age_group"),
                "source_type": carried.get("source_type"),
                "probe_status": probe_status,
                "decision": decision,
                "reason": reason,
                "policy_rule": policy_rule,
                "classifier_version": CLASSIFIER_VERSION,
                "profile_content_hash": carried.get("profile_content_hash"),
                "profile_target_key": (
                    carried.get("profile_target_key")
                    or latest.get("profile_target_key")
                ),
                "profile_fetched_at": carried.get("profile_fetched_at"),
                "observed_at": str(latest["observed_at"]),
                "source_seasons": [],
            }
        )
    return decisions, classifications


def build_replay_report(raw_root: Path) -> dict[str, Any]:
    """Re-run the production classifier over immutable local raw evidence."""

    root = Path(raw_root).resolve()
    if not root.is_dir():
        raise ReplayError(f"raw root is not a directory: {root}")
    stored_catalog, stored_profiles = _stored_evidence(root)
    try:
        discovery = discover_competitions(stored_catalog.payload)
    except (CatalogShapeError, TypeError, ValueError) as exc:
        raise ReplayError(f"stored allLeagues payload is invalid: {exc}") from exc
    if discovery.issues:
        raise ReplayError(
            "stored allLeagues payload has invalid entries: "
            + "; ".join(item.message for item in discovery.issues)
        )
    catalog = tuple(discovery.competitions)
    catalog_ids = {item.competition_id for item in catalog}
    catalog_by_id = {item.competition_id: item for item in catalog}
    profiles = {
        competition_id: profile
        for competition_id, profile in stored_profiles.items()
        if competition_id in catalog_ids
    }
    successful_profiles = {
        competition_id: profile
        for competition_id, profile in profiles.items()
        if isinstance(profile.payload, Mapping)
    }
    null_profiles = {
        competition_id: profile
        for competition_id, profile in profiles.items()
        if profile.payload is None
    }
    orphan_profile_ids = sorted(set(stored_profiles) - catalog_ids)
    all_observations = _load_observations(root, catalog=stored_catalog)
    observations = [
        item
        for item in all_observations
        if int(item["competition_id"]) in catalog_ids
    ]
    for competition_id, profile in sorted(null_profiles.items()):
        raw_validations = [(profile.fetched_at, 200, "fetched")]
        if profile.validated_at != profile.fetched_at:
            raw_validations.append((profile.validated_at, 304, "validated"))
        observations.extend(
            {
                "competition_id": competition_id,
                "catalog_name": catalog_by_id[competition_id].name,
                "outcome": "not_available",
                "http_status": status,
                "json_null": True,
                "authoritative": True,
                "attempt_id": f"raw-null:{stage}:{profile.target_key}:{observed_at}",
                "attempts": 1,
                "source_validated": True,
                "stale": False,
                "profile_target_key": profile.target_key,
                "observed_at": observed_at,
            }
            for observed_at, status, stage in raw_validations
        )

    effects_by_time: dict[tuple[int, str], str] = {}
    for observation in observations:
        time_key = (
            int(observation["competition_id"]),
            str(observation["observed_at"]),
        )
        effect = (
            "authoritative_absence"
            if bool(observation["authoritative"])
            else "transient"
        )
        previous_effect = effects_by_time.get(time_key)
        if previous_effect is not None and previous_effect != effect:
            raise ReplayError(
                "competition "
                f"{time_key[0]} has ambiguous events at the same timestamp"
            )
        effects_by_time[time_key] = effect
    orphan_observation_ids = sorted(
        {int(item["competition_id"]) for item in all_observations} - catalog_ids
    )
    conflicts = {
        item.competition_id: item.fields for item in discovery.conflicts
    }
    decisions: list[dict[str, Any]] = []
    classifications: list[ScopeClassification] = []
    seasons: list[SeasonRef] = []
    profile_results: dict[
        int,
        tuple[
            dict[str, Any],
            tuple[SeasonRef, ...],
            ScopeClassification | None,
        ],
    ] = {}
    for competition_id, profile in sorted(successful_profiles.items()):
        profile_results[competition_id] = _successful_decision(
            profile,
            catalog_by_id[competition_id],
            conflict_fields=conflicts.get(competition_id, ()),
        )

    observations_by_id: dict[int, list[dict[str, Any]]] = {}
    for observation in observations:
        observations_by_id.setdefault(int(observation["competition_id"]), []).append(
            observation
        )
    accepted_success_ids: set[int] = set()
    effective_observations: list[dict[str, Any]] = []
    for competition_id in sorted(catalog_ids):
        items = observations_by_id.get(competition_id, [])
        profile = successful_profiles.get(competition_id)
        if profile is None:
            effective_observations.extend(items)
            continue
        ties = [
            item
            for item in items
            if datetime.fromisoformat(str(item["observed_at"]))
            == datetime.fromisoformat(profile.validated_at)
        ]
        if ties:
            raise ReplayError(
                "competition "
                f"{competition_id} has ambiguous success/failure at the same timestamp"
            )
        later = [
            item
            for item in items
            if datetime.fromisoformat(str(item["observed_at"]))
            > datetime.fromisoformat(profile.validated_at)
        ]
        if later:
            # A successful profile resets previous miss history. Only events
            # after its latest source validation can replace that decision.
            effective_observations.extend(later)
            continue
        accepted_success_ids.add(competition_id)
        decision, profile_seasons, classification = profile_results[competition_id]
        decisions.append(decision)
        seasons.extend(profile_seasons)
        if classification is not None:
            classifications.append(classification)
    unavailable, unavailable_classifications = _unavailable_decisions(
        catalog,
        effective_observations,
        catalog_observed_at=stored_catalog.validated_at,
        carried_decisions={
            competition_id: result[0]
            for competition_id, result in profile_results.items()
        },
        successful_ids=accepted_success_ids,
    )
    decisions.extend(unavailable)
    classifications.extend(unavailable_classifications)
    decisions.sort(key=lambda item: int(item["competition_id"]))
    ids = [int(item["competition_id"]) for item in decisions]
    if len(ids) != len(set(ids)) or set(ids) != catalog_ids:
        raise ReplayError("replay did not produce exactly one decision per catalog ID")
    included_ids = [
        int(item["competition_id"])
        for item in decisions
        if item["decision"] == ScopeDecision.INCLUDED.value
    ]
    scopes = [
        item.identity
        for item in plan_seasons(
            classifications,
            seasons,
            mode=RunMode.DAILY,
            lane=ScopeLane.CURRENT,
        )
    ]
    content_hash = stored_catalog.content_hash
    contract = build_catalog_contract(
        catalog_batch_id=f"offline-replay-{stored_catalog.target_key}",
        catalog_content_hash=content_hash,
        classifier_version=CLASSIFIER_VERSION,
        parser_version=PARSER_VERSION,
        entities=AUTOMATIC_ENTITIES,
        entity_policy=AUTOMATIC_ENTITY_POLICY,
        included_ids=included_ids,
        scopes=scopes,
    )
    decision_counts = {
        decision.value: sum(item["decision"] == decision.value for item in decisions)
        for decision in (
            ScopeDecision.EXCLUDED,
            ScopeDecision.INCLUDED,
            ScopeDecision.PENDING_PROBE,
            ScopeDecision.REVIEW_REQUIRED,
        )
    }
    gender_counts = {"blank": 0, "female": 0, "male": 0}
    for item in decisions:
        if item["probe_status"] != "success":
            continue
        gender = str(item.get("source_gender") or "").strip().casefold()
        if gender in {"male", "men", "man", "m"}:
            gender_counts["male"] += 1
        elif gender in {"female", "women", "woman", "f"}:
            gender_counts["female"] += 1
        else:
            gender_counts["blank"] += 1
    review_inventory = [
        item
        for item in decisions
        if item["decision"] == ScopeDecision.REVIEW_REQUIRED.value
    ]
    pending_inventory = [
        item
        for item in decisions
        if item["decision"] == ScopeDecision.PENDING_PROBE.value
    ]
    return {
        "schema_version": REPORT_SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_root": str(root),
        "classifier_version": CLASSIFIER_VERSION,
        "parser_version": PARSER_VERSION,
        "catalog_content_hash": content_hash,
        "catalog_target_key": stored_catalog.target_key,
        "catalog_fetched_at": stored_catalog.fetched_at,
        "catalog_validated_at": stored_catalog.validated_at,
        "included_ids_sha256": contract.included_ids_sha256,
        "summary": {
            "profile_payload_count": len(profiles),
            "stored_profile_payload_count": len(stored_profiles),
            "orphan_profile_count": len(orphan_profile_ids),
            "orphan_observation_count": len(orphan_observation_ids),
            "catalog_id_count": len(catalog),
            "decisions": decision_counts,
            "structural_gender": gender_counts,
            "included_id_count": len(included_ids),
            "scope_count": len(scopes),
            "review_count": len(review_inventory),
            "pending_count": len(pending_inventory),
        },
        "decisions": decisions,
        "review_inventory": review_inventory,
        "pending_inventory": pending_inventory,
        "orphan_profile_ids": orphan_profile_ids,
        "orphan_observation_ids": orphan_observation_ids,
        "catalog_contract": contract.as_dict(),
    }


def _assert_expected(actual: Any, expected: Any, *, path: str) -> None:
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            raise ReplayError(f"expected summary mismatch at {path}")
        missing = sorted(set(expected) - set(actual))
        if missing:
            raise ReplayError(f"actual summary misses expected fields at {path}: {missing}")
        for key, value in expected.items():
            _assert_expected(actual[key], value, path=f"{path}.{key}")
        return
    if actual != expected:
        raise ReplayError(
            f"expected summary mismatch at {path}: expected={expected!r}, actual={actual!r}"
        )


def validate_expected_summary(
    report: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    if not isinstance(expected, Mapping):
        raise ReplayError("expected summary must be an object")
    summary = report.get("summary")
    if not isinstance(summary, Mapping):
        raise ReplayError("replay report has no summary")
    _assert_expected(summary, expected, path="summary")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--expected-summary",
        type=Path,
        help="optional JSON subset that must match the observed summary",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = build_replay_report(args.raw_root)
        if args.expected_summary is not None:
            try:
                expected = json.loads(args.expected_summary.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ReplayError(f"invalid expected summary: {exc}") from exc
            validate_expected_summary(report, expected)
        _atomic_json(args.output, report)
    except ReplayError as exc:
        print(
            json.dumps(
                {
                    "schema_version": REPORT_SCHEMA,
                    "passed": False,
                    "error": f"ReplayError: {exc}",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "REPORT_SCHEMA",
    "ReplayError",
    "build_replay_report",
    "main",
    "validate_expected_summary",
]
