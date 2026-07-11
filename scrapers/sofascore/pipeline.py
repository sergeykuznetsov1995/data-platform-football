"""Production wiring for SofaScore endpoint specs, raw replay and lineage."""

from __future__ import annotations

import os
from dataclasses import replace
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence

from scrapers.sofascore.adapters import (
    PrefetchedCaptureTransport,
    TrinoManifestStore,
)
from scrapers.sofascore.capture_engine import (
    CaptureSink,
    CaptureResult,
    EndpointSpec,
    HttpPayload,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    JsonFileManifestStore,
    ManifestKey,
    ManifestStatus,
    ManifestStore,
    utc_now_iso,
)
from scrapers.sofascore.raw_store import RawPayloadStore
from scripts.proxy_filter.budget import (
    ProductionBudgetUnavailable,
    SharedBudgetLedger,
    load_verified_policy,
)


EVENT_PATHS = {
    'event': '/api/v1/event/{target_id}',
    'lineups': '/api/v1/event/{target_id}/lineups',
    'statistics': '/api/v1/event/{target_id}/statistics',
    'shotmap': '/api/v1/event/{target_id}/shotmap',
    'incidents': '/api/v1/event/{target_id}/incidents',
}

PLAYER_PATHS = {
    'player_profile': '/api/v1/player/{target_id}',
    'player_season_statistics': (
        '/api/v1/player/{target_id}/unique-tournament/'
        '{source_tournament_id}/season/{source_season_id}/statistics/overall'
    ),
}

PLAYER_TARGET_TYPES = {
    'player_profile': 'player',
    'player_season_statistics': 'season_player',
}


@dataclass(frozen=True)
class CaptureRuntime:
    engine: SofaScoreCaptureEngine
    manifest_store: ManifestStore
    raw_store: RawPayloadStore
    budget_error: Optional[str] = None


class DeferredMaterialization(RuntimeError):
    """Raw/parser succeeded; Bronze commit must finalize the endpoint."""


class DeferredCaptureSink(CaptureSink):
    def write(self, key, datasets, raw) -> None:
        raise DeferredMaterialization(
            f'endpoint {key.endpoint} awaits atomic Bronze MERGE'
        )


def build_capture_runtime(
    *,
    run_id: str,
    task_id: str,
    raw_store_uri: Optional[str] = None,
    manifest_backend: Optional[str] = None,
) -> CaptureRuntime:
    """Build the shared CLI/DAG/backfill raw+manifest runtime.

    Airflow sets ``SOFASCORE_MANIFEST_BACKEND=trino``.  A standalone CLI uses
    the same state machine with an atomic JSON manifest, which keeps offline
    fixture/smoke runs independent of a live lakehouse.
    """
    raw_uri = (
        raw_store_uri
        or os.environ.get('SOFASCORE_RAW_STORE_URI')
        or 'file:///tmp/sofascore-raw'
    )
    raw_store = RawPayloadStore.from_uri(raw_uri)
    backend = (
        manifest_backend
        or os.environ.get('SOFASCORE_MANIFEST_BACKEND')
        or 'json'
    ).strip().lower()
    if backend == 'trino':
        from scrapers.base.trino_manager import TrinoTableManager

        manifest_store: ManifestStore = TrinoManifestStore(TrinoTableManager())
    elif backend == 'json':
        path = os.environ.get(
            'SOFASCORE_MANIFEST_PATH', '/tmp/sofascore-endpoint-manifest.json'
        )
        manifest_store = JsonFileManifestStore(path)
    else:
        raise ValueError(f'unsupported SofaScore manifest backend: {backend!r}')
    budget = None
    budget_error = None
    artifact_path = os.environ.get('SOFASCORE_PROXY_BUDGET_ARTIFACT', '').strip()
    ledger_path = os.environ.get('SOFASCORE_PROXY_BUDGET_LEDGER', '').strip()
    if bool(artifact_path) != bool(ledger_path):
        budget_error = (
            'SOFASCORE_PROXY_BUDGET_ARTIFACT and '
            'SOFASCORE_PROXY_BUDGET_LEDGER must be configured together'
        )
    elif artifact_path:
        try:
            budget = SharedBudgetLedger(
                ledger_path,
                load_verified_policy(artifact_path),
            )
        except ProductionBudgetUnavailable as exc:
            # A bad/unreviewed canary must not disable raw replay or exact
            # no-op runs. Paid EndpointSpecs still fail closed because the
            # engine receives no budget ledger.
            budget_error = str(exc)

    engine = SofaScoreCaptureEngine(
        raw_store=raw_store,
        manifest_store=manifest_store,
        transport=PrefetchedCaptureTransport({}),
        sink=DeferredCaptureSink(),
        run_id=run_id,
        task_id=task_id,
        budget=budget,
        max_workers=max(1, int(os.environ.get('SOFASCORE_MAX_CONCURRENCY', '4'))),
    )
    return CaptureRuntime(engine, manifest_store, raw_store, budget_error)


def _schema_validator(endpoint: str):
    def validate(payload):
        if not isinstance(payload, dict):
            return False
        if endpoint == 'event':
            value = payload.get('event', payload)
            structurally_valid = (
                isinstance(value, dict)
                and value.get('id') is not None
                and isinstance(value.get('season'), dict)
                and isinstance(value.get('homeTeam'), dict)
                and isinstance(value.get('awayTeam'), dict)
            )
        elif endpoint == 'lineups':
            structurally_valid = all(
                side in payload
                and isinstance(payload.get(side), dict)
                and 'players' in payload[side]
                and isinstance(payload[side].get('players'), list)
                for side in ('home', 'away')
            )
        else:
            required_key = {
                'statistics': 'statistics',
                'shotmap': 'shotmap',
                'incidents': 'incidents',
            }[endpoint]
            structurally_valid = (
                required_key in payload
                and isinstance(payload.get(required_key), list)
            )
        if not structurally_valid:
            return False

        # The versioned coverage contract owns required JSON paths and arrays
        # that must survive exact raw capture. Cardinality is intentionally not
        # checked here: parser row counts are only available after this gate.
        from dags.utils.sofascore_dq import validate_raw_payload

        return validate_raw_payload(endpoint, payload).passed

    return validate


def _empty_predicate(endpoint: str):
    def empty(payload):
        if endpoint == 'event':
            return not bool(payload.get('event', payload))
        if endpoint == 'lineups':
            return not any(
                (payload.get(side) or {}).get('players')
                for side in ('home', 'away')
            )
        return not bool(payload.get({
            'statistics': 'statistics',
            'shotmap': 'shotmap',
            'incidents': 'incidents',
        }[endpoint]))

    return empty


def _parsers(endpoint: str, target_id: str):
    # Lazy import avoids importing pandas/soccerdata for manifest-only checks.
    from scrapers.sofascore.scraper import SofaScoreScraper

    if endpoint == 'event':
        return {
            'events': lambda payload: list(filter(None, [
                SofaScoreScraper._flatten_full_event(target_id, payload)
            ])),
            'event_participants': lambda payload: (
                SofaScoreScraper._flatten_event_participants(target_id, payload)
            ),
            'venue': lambda payload: list(filter(None, [
                SofaScoreScraper._flatten_event_venue(target_id, payload)
            ])),
        }
    if endpoint == 'lineups':
        def ratings(payload):
            return [
                row
                for side in ('home', 'away')
                for row in SofaScoreScraper._flatten_lineup_side(
                    target_id, side, payload.get(side) or {}
                )
            ]

        return {
            'player_ratings': ratings,
            'lineups': lambda payload: [
                {key: value for key, value in row.items() if key != 'rating'}
                for row in ratings(payload)
            ],
            'event_player_stats': lambda payload: (
                SofaScoreScraper._flatten_event_player_stats_from_lineups(
                    target_id, payload
                )
            ),
        }
    if endpoint == 'statistics':
        return {
            'match_stats': lambda payload: (
                SofaScoreScraper._flatten_match_stats(target_id, payload)
            )
        }
    if endpoint == 'shotmap':
        return {
            'event_shotmap': lambda payload: (
                SofaScoreScraper._flatten_shotmap(target_id, payload)
            )
        }
    return {
        'incidents': lambda payload: (
            SofaScoreScraper._flatten_incidents(target_id, payload)
        )
    }


def _player_schema_validator(endpoint: str, target_id: str):
    def validate(payload):
        if not isinstance(payload, dict):
            return False
        if endpoint == 'player_profile':
            if 'player' not in payload:
                return False
            player = payload.get('player')
            if player is None:
                return True
            return (
                isinstance(player, dict)
                and player.get('id') is not None
                and str(player.get('id')) == target_id
            )
        statistics = payload.get('statistics')
        team = payload.get('team')
        return (
            'statistics' in payload
            and isinstance(statistics, dict)
            and (team is None or isinstance(team, dict))
        )

    return validate


def _player_empty_predicate(endpoint: str):
    def empty(payload):
        if endpoint == 'player_profile':
            return payload.get('player') is None
        return not bool(payload.get('statistics'))

    return empty


def _player_parsers(
    endpoint: str,
    target_id: str,
    source_tournament_id: int,
    source_season_id: int,
):
    # Reuse the compatibility flatteners so replay and legacy Bronze keep the
    # same column contract while exact raw bytes remain the source of truth.
    from scrapers.sofascore.scraper import SofaScoreScraper

    if endpoint == 'player_profile':
        return {
            'player_profile': lambda payload: list(filter(None, [
                SofaScoreScraper._flatten_player_profile(payload)
            ]))
        }
    return {
        'player_season_stats': lambda payload: list(filter(None, [
            SofaScoreScraper._flatten_player_season_stats(
                target_id,
                source_tournament_id,
                source_season_id,
                payload,
            )
        ]))
    }


def build_event_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    target_id: str | int,
    endpoint: str,
    freshness_key: str,
    paid_proxy: bool,
) -> EndpointSpec:
    endpoint = str(endpoint)
    if endpoint not in EVENT_PATHS:
        raise ValueError(f'unsupported event endpoint: {endpoint!r}')
    target = str(target_id)
    path = EVENT_PATHS[endpoint].format(target_id=target)
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=str(source_tournament_id),
            source_season_id=str(source_season_id),
            target_type='event',
            target_id=target,
            endpoint=endpoint,
            freshness_key=str(freshness_key),
        ),
        url=f'https://www.sofascore.com{path}',
        schema_validator=_schema_validator(endpoint),
        empty_predicate=_empty_predicate(endpoint),
        parsers=_parsers(endpoint, target),
        paid_proxy=paid_proxy,
    )


def build_player_spec(
    *,
    source_tournament_id: str | int,
    source_season_id: str | int,
    target_id: str | int,
    endpoint: str,
    freshness_key: str,
    paid_proxy: bool,
) -> EndpointSpec:
    """Build a raw/replay spec for one exact player payload.

    Profile snapshots are keyed to the requested source competition season even
    though SofaScore's profile URL itself is season-independent. This prevents
    a current-club snapshot from being materialized without capture-season
    lineage. Season statistics always use the explicit tournament and season
    IDs in the source URL; no latest/default-season fallback exists here.
    """
    endpoint = str(endpoint)
    if endpoint not in PLAYER_PATHS:
        raise ValueError(f'unsupported player endpoint: {endpoint!r}')
    target = str(target_id).strip()
    source_tournament = str(source_tournament_id).strip()
    source_season = str(source_season_id).strip()
    if not target or not source_tournament or not source_season:
        raise ValueError('player and source tournament/season IDs must not be empty')
    try:
        numeric_tournament = int(source_tournament)
        numeric_season = int(source_season)
    except ValueError as exc:
        raise ValueError('SofaScore tournament and season IDs must be integers') from exc
    if numeric_tournament <= 0 or numeric_season <= 0:
        raise ValueError('SofaScore tournament and season IDs must be positive')
    path = PLAYER_PATHS[endpoint].format(
        target_id=target,
        source_tournament_id=source_tournament,
        source_season_id=source_season,
    )
    return EndpointSpec(
        key=ManifestKey(
            source_tournament_id=source_tournament,
            source_season_id=source_season,
            target_type=PLAYER_TARGET_TYPES[endpoint],
            target_id=target,
            endpoint=endpoint,
            freshness_key=str(freshness_key),
        ),
        url=f'https://www.sofascore.com{path}',
        schema_validator=_player_schema_validator(endpoint, target),
        empty_predicate=_player_empty_predicate(endpoint),
        parsers=_player_parsers(
            endpoint,
            target,
            numeric_tournament,
            numeric_season,
        ),
        paid_proxy=paid_proxy,
    )


def endpoint_resume_plan(
    manifest_store: ManifestStore,
    specs: Iterable[EndpointSpec],
) -> dict[str, tuple[str, ...]]:
    """Return only non-terminal endpoint names, grouped by target id."""
    plan: dict[str, list[str]] = {}
    for spec in specs:
        record = manifest_store.get(spec.key)
        if record is not None and record.is_terminal:
            continue
        plan.setdefault(spec.key.target_id, []).append(spec.key.endpoint)
    return {target: tuple(names) for target, names in plan.items()}


def ingest_prefetched_records(
    runtime: CaptureRuntime,
    *,
    specs: Mapping[tuple[str, str], EndpointSpec],
    records: Mapping[str, Mapping[str, object]],
) -> list[CaptureResult]:
    """Persist exact already-captured records through the canonical engine.

    This is intended for zero-paid direct captures and tests. A paid browser
    path must call ``authorize_request`` before moving bytes and then call
    ``ingest_prefetched`` with provider-path accounting; after-the-fact budget
    authorization is intentionally impossible.
    """
    results: list[CaptureResult] = []
    for value in records.values():
        target_id = str(
            value.get('match_id')
            or value.get('player_id')
            or value.get('target_id')
            or ''
        )
        endpoint = str(value.get('endpoint') or '')
        spec = specs.get((target_id, endpoint))
        if spec is None:
            continue
        existing = runtime.manifest_store.get(spec.key)
        if existing is not None and existing.is_terminal:
            results.append(CaptureResult(manifest=existing, cache_hit=True))
            continue
        body = value.get('body')
        if isinstance(body, str):
            body = body.encode('utf-8')
        if not isinstance(body, bytes):
            continue
        response = HttpPayload(
            status_code=int(value.get('status') or 0),
            body=body,
            headers=dict(value.get('headers') or {}),
            provider_bytes=0,
        )
        if spec.paid_proxy:
            raise ValueError(
                'paid prefetched record lacks preauthorization/provider meter'
            )
        results.append(
            runtime.engine.ingest_prefetched(
                spec, response, authorization=None
            )
        )
    return results


def replay_event_specs(
    runtime: CaptureRuntime,
    specs: Sequence[EndpointSpec],
) -> list[CaptureResult]:
    return runtime.engine.capture_many(specs, offline=True, force_replay=True)


def replay_player_specs(
    runtime: CaptureRuntime,
    specs: Sequence[EndpointSpec],
) -> list[CaptureResult]:
    return runtime.engine.capture_many(specs, offline=True, force_replay=True)


def materialize_player_datasets(
    scraper,
    results: Iterable[CaptureResult],
    *,
    league: str,
    season: str,
):
    """Build player Bronze frames with canonical source/raw lineage.

    The helper is intentionally write-free. A CLI/DAG/backfill consumer may
    atomically MERGE both returned frames and only then call
    :func:`finalize_materialized_results`. This preserves raw-first replay when
    either Iceberg write fails.
    """
    import pandas as pd

    entity_types = {
        'player_profile': 'player_profile',
        'player_season_stats': 'player_season_stats',
    }
    rows_by_dataset = {name: [] for name in entity_types}
    for result in results:
        unknown = set(result.datasets) - set(entity_types)
        if unknown:
            raise ValueError(
                f'unexpected player capture datasets: {sorted(unknown)!r}'
            )
        key = result.manifest.key
        for name, dataset in result.datasets.items():
            raw_hash = (
                result.raw.content_hash
                if result.raw is not None
                else result.manifest.raw_content_hash
            )
            raw_blob = (
                result.raw.blob_key
                if result.raw is not None
                else result.manifest.raw_blob_key
            )
            if not raw_hash or not raw_blob:
                raise ValueError(
                    f'normalized player dataset {name!r} lacks raw lineage'
                )
            for row in dataset.rows:
                player_id = row.get('player_id')
                if player_id is None or str(player_id) != key.target_id:
                    raise ValueError(
                        f'player row target mismatch: {player_id!r} != '
                        f'{key.target_id!r}'
                    )
                enriched = dict(row)
                enriched.update({
                    'source_tournament_id': key.source_tournament_id,
                    'source_season_id': key.source_season_id,
                    'raw_content_hash': raw_hash,
                    'raw_blob_key': raw_blob,
                })
                rows_by_dataset[name].append(enriched)

    empty_columns = {
        'player_profile': [
            'player_id', 'source_tournament_id', 'source_season_id',
            'raw_content_hash', 'raw_blob_key', 'league', 'season',
        ],
        'player_season_stats': [
            'player_id', 'unique_tournament_id', 'sofascore_season_id',
            'source_tournament_id', 'source_season_id',
            'raw_content_hash', 'raw_blob_key', 'league', 'season',
        ],
    }
    frames = {}
    for name, entity_type in entity_types.items():
        rows = rows_by_dataset[name]
        frame = (
            pd.DataFrame(rows)
            if rows
            else pd.DataFrame(columns=empty_columns[name])
        )
        if not frame.empty:
            frame['league'] = str(league)
            frame['season'] = str(season)
            frame = scraper._add_metadata(frame, entity_type)
        frames[name] = frame
    return frames


def finalize_materialized_results(
    runtime: CaptureRuntime,
    results: Iterable[CaptureResult],
) -> None:
    """Commit success only after every related Bronze MERGE has succeeded."""
    for result in results:
        record = result.manifest
        if not (
            record.status == ManifestStatus.RETRYABLE_FAILURE
            and record.error_type == 'DeferredMaterialization'
            and record.raw_content_hash
            and record.raw_blob_key
            and record.row_count > 0
        ):
            continue
        runtime.manifest_store.upsert(
            replace(
                record,
                status=ManifestStatus.SUCCESS,
                error_type=None,
                error_message=None,
                parsed_at=record.parsed_at or utc_now_iso(),
                updated_at=utc_now_iso(),
            )
        )


__all__ = [
    'CaptureRuntime',
    'EVENT_PATHS',
    'PLAYER_PATHS',
    'PLAYER_TARGET_TYPES',
    'build_capture_runtime',
    'build_event_spec',
    'build_player_spec',
    'endpoint_resume_plan',
    'finalize_materialized_results',
    'ingest_prefetched_records',
    'materialize_player_datasets',
    'replay_event_specs',
    'replay_player_specs',
]
