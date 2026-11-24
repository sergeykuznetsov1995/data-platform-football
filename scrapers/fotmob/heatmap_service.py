"""Publish heatmaps after the parent match commit, without undoing that commit."""

import json
from dataclasses import asdict, replace
from hashlib import sha256
from typing import Any

from scrapers.fotmob.heatmaps import (
    AssetMetadata,
    FetchOrigin,
    HEATMAP_IDENTITY_RESOLVER_VERSION,
    HeatmapObservation,
    HeatmapOutcome,
    classify_heatmap_fetch,
    extract_heatmap_reference,
    observation_from_parse,
    parse_heatmap,
    resolve_heatmap_players,
)
from scrapers.fotmob.match_refresh import match_time
from scrapers.fotmob.planner import BudgetExceeded, RunMode
from scrapers.fotmob.repository import (
    PARSER_VERSION,
    ManifestStatus,
    TableRows,
    deterministic_target_batch_id,
    utc_now,
)
from scrapers.fotmob.transport import FetchResult


def _fetch_heatmap(
    service: Any, reference: Any, *, late: bool, single_attempt: bool, cutoff_at: Any
) -> FetchResult:
    """Reserve retry capacity and account for transport work exactly once."""
    service._raise_if_cancelled()
    if service.ledger.direct_bytes >= service.ledger.budget.max_direct_bytes:
        raise BudgetExceeded("heatmap byte budget exhausted")
    max_attempts = (
        1 if single_attempt else max(1, int(getattr(service.transport, "max_attempts", 1)))
    )
    if service.ledger.remaining_requests < max_attempts:
        raise BudgetExceeded("remaining request budget cannot cover heatmap retries")
    late_requests = service._late_match_requests + service._late_heatmap_requests
    if late and 9 * (late_requests + max_attempts) > (
        service.ledger.requests - late_requests
    ):
        raise BudgetExceeded("late heatmap quota deferred target")
    before = service.transport.snapshot_stats()

    def before_attempt() -> None:
        service._raise_if_cancelled()
        if cutoff_at is not None and utc_now() >= cutoff_at:
            raise RuntimeError("FotMob match refresh cutoff reached")

    try:
        kwargs = {"max_attempts": 1} if single_attempt else {}
        if cutoff_at is not None:
            kwargs["before_attempt"] = before_attempt
        fetch = service.transport.fetch_json(reference.target.canonical_url, **kwargs)
    except Exception:
        after = service.transport.snapshot_stats()
        attempts = max(0, after.attempts - before.attempts)
        service._late_heatmap_requests += attempts if late else 0
        service.ledger.account_fetch(
            attempts=attempts,
            direct_bytes=max(0, after.direct_bytes - before.direct_bytes),
            proxy_bytes=max(0, after.proxy_bytes - before.proxy_bytes),
        )
        raise
    service._late_heatmap_requests += fetch.attempts if late else 0
    try:
        service._account(fetch)
    except BudgetExceeded as exc:
        # The raw response already exists: publish it before stopping further work.
        service._heatmap_budget_error = str(exc)
    return fetch


def _defer(operation: Any, deferred: list[str], match_id: int, reason: str) -> None:
    diagnostic = f"match {match_id}: heatmap {reason[:300]}"
    deferred.append(diagnostic)
    operation.metadata.setdefault("heatmap_diagnostics", []).append(diagnostic)


def _fetch_observation(
    service: Any,
    match_id: int,
    reference: Any,
    fetch: FetchResult,
    outcome: HeatmapOutcome,
) -> HeatmapObservation:
    """Attach body provenance only when there are bytes whose hash can be checked."""
    provenance = {}
    if isinstance(fetch.body, bytes) and fetch.content_hash:
        verified = sha256(fetch.body).hexdigest() == fetch.content_hash
        provenance = {
            "content_hash": fetch.content_hash,
            "hash_verified": verified,
        }
        if verified:
            provenance["fetch_origin"] = (
                FetchOrigin.REPLAY
                if service.mode == RunMode.REPLAY or fetch.stale or fetch.attempts == 0
                else FetchOrigin.NETWORK
            )
    return HeatmapObservation(
        service.run_id,
        match_id,
        outcome,
        True,
        target_key=reference.target.target_key,
        attempted=True,
        **provenance,
    )


def _commit_outcome(
    service: Any,
    match: Any,
    match_id: int,
    fetch: FetchResult,
    operation: Any,
    paths: list[str],
    outcome: HeatmapOutcome,
    detail: str,
    *,
    decoded: Any = None,
) -> None:
    """Publish a row-free manifest and expose failures without changing parent counts."""
    detail = detail[:300]
    message = f"match {match_id}: heatmap {outcome.value}: {detail}"
    if outcome == HeatmapOutcome.RESOURCE_UNAVAILABLE:
        status = ManifestStatus.NOT_AVAILABLE
    elif outcome in (HeatmapOutcome.UNEXPECTED_FORMAT, HeatmapOutcome.PARSE_FAILED):
        status = ManifestStatus.SCHEMA_DRIFT
        operation.errors.append(message)
    else:
        status = ManifestStatus.RETRYABLE_FAILURE
        operation.retryable.append(message)
    capabilities = {"heatmap_outcome": outcome.value, "error_detail": detail}
    if decoded is not None:
        capabilities["decoded"] = decoded
    paths.extend(
        service._commit_for_fetch(
            fetch,
            target_type="heatmap",
            status=status,
            competition_id=match["competition_id"],
            source_season_key=match["source_season_key"],
            entity_id=str(match_id),
            capabilities=capabilities,
            error_code=outcome.value,
            error=detail,
        )
    )


def sync_match_heatmap(
    service: Any,
    match: Any,
    parent_fetch: FetchResult,
    operation: Any,
    *,
    late: bool,
    single_attempt: bool,
    cutoff_at: Any,
    observations: list,
    deferred: list[str],
) -> list[str]:
    """Commit the child asset independently of an already successful parent match."""
    paths: list[str] = []
    match_id = None
    reference = None
    has_url = False
    try:
        match_id = int(match["match_id"])
        result = extract_heatmap_reference(parent_fetch.data, match_id)
        if result.outcome == HeatmapOutcome.URL_MISSING:
            observations.append(
                HeatmapObservation(service.run_id, match_id, HeatmapOutcome.URL_MISSING, False)
            )
            if service.repository.latest_entity_success("heatmap", match_id) is not None:
                paths.extend(
                    service._commit_for_fetch(
                        parent_fetch,
                        target_type="heatmap",
                        status=ManifestStatus.NOT_AVAILABLE,
                        competition_id=match["competition_id"],
                        source_season_key=match["source_season_key"],
                        entity_id=str(match_id),
                        error_code=HeatmapOutcome.URL_MISSING.value,
                        capabilities={
                            "heatmap_outcome": HeatmapOutcome.URL_MISSING.value,
                            "parent_target_key": parent_fetch.target_key,
                        },
                    )
                )
            return paths

        if result.outcome == HeatmapOutcome.UNEXPECTED_FORMAT:
            payload = parent_fetch.data
            content = payload.get("content") if isinstance(payload, dict) else None
            url = content.get("heatmapUrl") if isinstance(content, dict) else None
            has_url = isinstance(url, str) and bool(url)
            observations.append(
                HeatmapObservation(
                    service.run_id, match_id, HeatmapOutcome.UNEXPECTED_FORMAT, has_url
                )
            )
            _commit_outcome(
                service, match, match_id, parent_fetch, operation, paths,
                HeatmapOutcome.UNEXPECTED_FORMAT, "invalid parent heatmap reference",
            )
            return paths

        reference = result.reference
        has_url = True
        try:
            if service.mode == RunMode.REPLAY:
                fetch = service._fetch(reference.target.canonical_url, replay=True)
            else:
                fetch = _fetch_heatmap(
                    service, reference, late=late, single_attempt=single_attempt,
                    cutoff_at=cutoff_at,
                )
        except Exception as exc:
            observations.append(
                HeatmapObservation(
                    service.run_id, match_id, HeatmapOutcome.UNKNOWN, True,
                    target_key=reference.target.target_key, attempted=False,
                )
            )
            _defer(operation, deferred, match_id, f"{type(exc).__name__}: {exc}")
            return paths

        budget_error = getattr(service, "_heatmap_budget_error", None)
        if budget_error:
            service._heatmap_budget_error = None
            _defer(operation, deferred, match_id, budget_error)

        classification = classify_heatmap_fetch(fetch)
        if not classification.parse_body:
            outcome = classification.outcome
            if outcome not in (
                HeatmapOutcome.RESOURCE_UNAVAILABLE,
                HeatmapOutcome.UNEXPECTED_FORMAT,
                HeatmapOutcome.UNKNOWN,
            ):
                outcome = HeatmapOutcome.UNKNOWN
            observations.append(_fetch_observation(service, match_id, reference, fetch, outcome))
            _commit_outcome(
                service, match, match_id, fetch, operation, paths, outcome,
                str(fetch.error or f"heatmap fetch classified as {outcome.value}"),
            )
            return paths

        observed_at = fetch.source_observed_at or fetch.validated_at or fetch.fetched_at
        if (
            not isinstance(fetch.body, bytes)
            or not fetch.content_hash
            or not fetch.raw_uri
            or not observed_at
        ):
            observations.append(
                _fetch_observation(service, match_id, reference, fetch, HeatmapOutcome.UNKNOWN)
            )
            _commit_outcome(
                service, match, match_id, fetch, operation, paths, HeatmapOutcome.UNKNOWN,
                "missing heatmap body or raw provenance",
            )
            return paths

        metadata = AssetMetadata(
            reference=reference,
            observed_at=observed_at,
            content_hash=fetch.content_hash,
            raw_uri=fetch.raw_uri,
            competition_id=match["competition_id"],
            source_season_key=match["source_season_key"],
            http_last_modified=fetch.last_modified,
        )
        parsed = parse_heatmap(fetch.body, metadata)
        origin = (
            FetchOrigin.REPLAY
            if service.mode == RunMode.REPLAY or fetch.attempts == 0
            else FetchOrigin.NETWORK
        )
        observations.append(observation_from_parse(service.run_id, metadata, parsed, origin))
        if parsed.outcome in (HeatmapOutcome.SUCCESS, HeatmapOutcome.NOT_APPLICABLE):
            asset_row = asdict(parsed.asset)
            point_rows = [asdict(point) for point in parsed.points]
            asset_row["outcome"] = parsed.outcome.value
            timestamp = match_time(observed_at)
            asset_row["_observed_at"] = timestamp
            for row in point_rows:
                row["_observed_at"] = timestamp
            paths.extend(
                service._commit_for_fetch(
                    fetch,
                    target_type="heatmap",
                    competition_id=match["competition_id"],
                    source_season_key=match["source_season_key"],
                    entity_id=str(match_id),
                    datasets=(
                        TableRows("fotmob_heatmap_assets", [asset_row], "heatmap_assets"),
                        TableRows("fotmob_heatmap_points", point_rows, "heatmap_points"),
                    ),
                    expected_counts={"heatmap_assets": 1, "heatmap_points": len(point_rows)},
                    capabilities={
                        "heatmap_outcome": parsed.outcome.value,
                        "view_box": asset_row.get("view_box"),
                        "source_last_modified": asset_row.get(
                            "source_last_modified", fetch.last_modified
                        ),
                        "player_count": asset_row.get("player_count"),
                        "circle_count": asset_row.get("circle_count"),
                        "heatmap_url": reference.heatmap_url,
                    },
                )
            )
            if not parent_fetch.content_hash or not parent_fetch.raw_uri:
                _defer(operation, deferred, match_id, "identity parent.raw_provenance.missing")
                return paths
            try:
                heatmap_batch_id = deterministic_target_batch_id(
                    reference.target.target_key, fetch.content_hash, PARSER_VERSION
                )
                identity_rows = resolve_heatmap_players(
                    parent_fetch.data,
                    match_id,
                    parsed.player_keys,
                    metadata,
                    heatmap_batch_id=heatmap_batch_id,
                    parent_content_hash=parent_fetch.content_hash,
                    parent_raw_uri=parent_fetch.raw_uri,
                )
                identity_dicts = []
                for identity in identity_rows:
                    row = asdict(identity)
                    row["match_status"] = identity.match_status.value
                    row["identity_status"] = identity.identity_status.value
                    row["_observed_at"] = timestamp
                    identity_dicts.append(row)
                identity_observation = sha256(json.dumps(
                    [
                        HEATMAP_IDENTITY_RESOLVER_VERSION,
                        parent_fetch.content_hash,
                        heatmap_batch_id,
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")).hexdigest()
                identity_target_key = sha256(
                    f"heatmap-identity:{reference.target.target_key}".encode("utf-8")
                ).hexdigest()
                identity_fetch = replace(fetch, target_key=identity_target_key)
                paths.extend(
                    service._commit_for_fetch(
                        identity_fetch,
                        target_type="heatmap_identity",
                        competition_id=match["competition_id"],
                        source_season_key=match["source_season_key"],
                        entity_id=str(match_id),
                        datasets=(
                            TableRows(
                                "fotmob_heatmap_player_identities",
                                identity_dicts,
                                "heatmap_player_identities",
                            ),
                        ),
                        expected_counts={
                            "heatmap_player_identities": len(identity_dicts),
                        },
                        observation_id=identity_observation,
                        capabilities={
                            "heatmap_target_key": reference.target.target_key,
                            "heatmap_batch_id": heatmap_batch_id,
                            "parent_content_hash": parent_fetch.content_hash,
                            "resolver_version": HEATMAP_IDENTITY_RESOLVER_VERSION,
                        },
                    )
                )
            except Exception as exc:
                operation.record_exception(
                    f"match {match_id}: heatmap identity processing failed: "
                    f"{type(exc).__name__}: {exc}",
                    exc,
                )
        else:
            _commit_outcome(
                service, match, match_id, fetch, operation, paths, parsed.outcome,
                f"heatmap parser returned {parsed.outcome.value}", decoded=parsed.decoded,
            )
    except Exception as exc:
        operation.record_exception(
            f"match {match_id}: heatmap processing failed: {type(exc).__name__}: {exc}",
            exc,
        )
        for index, observation in enumerate(observations):
            if (
                match_id is not None
                and getattr(observation, "match_id", None) == match_id
                and observation.outcome in (HeatmapOutcome.SUCCESS, HeatmapOutcome.NOT_APPLICABLE)
            ):
                observations[index] = replace(
                    observation, outcome=HeatmapOutcome.UNKNOWN, decoded=False, typed_rows=0,
                )
        if match_id is not None and not any(
            getattr(observation, "match_id", None) == match_id for observation in observations
        ):
            observations.append(
                HeatmapObservation(
                    service.run_id, match_id, HeatmapOutcome.UNKNOWN, has_url,
                    target_key=reference.target.target_key if reference is not None else None,
                )
            )
    return paths
