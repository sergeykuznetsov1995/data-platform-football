#!/usr/bin/env python3
"""Bounded offline remediation for FBref match player statistics.

The command replays immutable HTML from one required source control run.  It
does not construct URLs, instantiate a scraper/fetcher, or perform network
requests.  Successful pages receive a tool-specific ``__page__`` manifest so
the next invocation advances to the next bounded batch.

Usage (inside the Airflow image):

    python /opt/airflow/scripts/rescrape_match_player_stats.py \
        --source-control-run-id 12345678-1234-5678-1234-567812345678 \
        --max-pages 25
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


LOGGER = logging.getLogger("fbref_offline_remediation")
MAX_REPLAY_PAGES = 25
PLAYER_STATS_DATASET = "match_player_stats"
PLAYER_STATS_PARSER_VERSION = "fbref-remediation-match-player-stats-v1"
REMEDIATION_STATEFUL_VERSION = "fbref-remediation-stateful-skip-v1"


class OfflineRemediationError(RuntimeError):
    """A bounded raw replay could not be completed safely."""


@dataclass(frozen=True)
class OfflineReplayResult:
    source_control_run_id: str
    parser_version: str
    target_dataset: str
    selected: int
    processed: int
    rows_written: int
    remaining: bool
    dry_run: bool
    network_requests: int = 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _uuid_argument(value: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError(
            "source control run id must be a UUID"
        ) from exc


def _bounded_pages(value: str) -> int:
    try:
        pages = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("max pages must be an integer") from exc
    if not 1 <= pages <= MAX_REPLAY_PAGES:
        raise argparse.ArgumentTypeError(
            f"max pages must be between 1 and {MAX_REPLAY_PAGES}"
        )
    return pages


def _status_value(dataset: object) -> str:
    status = getattr(dataset, "status", "error")
    return str(getattr(status, "value", status))


def _remediation_run_id(source_run_id: str, parser_version: str) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"fbref-offline-remediation:{source_run_id}:{parser_version}",
        )
    )


def _source_context(record: object) -> object:
    from scrapers.fbref.typed_bronze import TypedSourceContext

    source_ids = dict(getattr(record, "source_ids", {}) or {})
    competition_id = str(source_ids.get("competition_id") or "").strip()
    season_id = str(source_ids.get("season_id") or "").strip()
    if not competition_id or not season_id:
        raise OfflineRemediationError(
            f"Raw match {getattr(record, 'target_id', '?')} lacks source-native "
            "competition_id/season_id"
        )
    return TypedSourceContext(
        source_competition_id=competition_id,
        source_season_id=season_id,
        competition_name=(
            str(source_ids["competition_name"])
            if source_ids.get("competition_name")
            else None
        ),
        season_label=(
            str(source_ids["season_label"])
            if source_ids.get("season_label")
            else season_id
        ),
    )


def _record_manifest(
    control: Any,
    record: object,
    *,
    parser_version: str,
    dataset: str,
    availability: str,
    persistence_status: str,
    validation_status: str,
    row_count: int,
    error: Optional[Exception] = None,
) -> None:
    control.record_dataset_manifest(
        target_id=str(getattr(record, "target_id")),
        content_hash=str(getattr(record, "content_hash")),
        parser_version=parser_version,
        dataset=dataset,
        availability=availability,
        parse_status="failed" if error is not None else "succeeded",
        persistence_status=persistence_status,
        validation_status=validation_status,
        row_count=int(row_count),
        error_class=None if error is None else type(error).__name__,
        error_message=None if error is None else str(error),
    )


def _validate_record(item: dict[str, object], record: object) -> None:
    expected_refresh = str(item.get("logical_refresh_id") or "")
    expected_target = str(item.get("target_id") or "")
    if str(getattr(record, "logical_refresh_id", "")) != expected_refresh:
        raise OfflineRemediationError(
            f"Raw/control refresh mismatch for {expected_refresh}"
        )
    if str(getattr(record, "target_id", "")) != expected_target:
        raise OfflineRemediationError(
            f"Raw/control target mismatch for {expected_refresh}"
        )
    if str(getattr(record, "page_kind", "")) != "match":
        raise OfflineRemediationError(
            f"Replay candidate {expected_target} is not a match page"
        )
    if str(getattr(record, "source", "fbref")) != "fbref":
        raise OfflineRemediationError(
            f"Replay candidate {expected_target} is not FBref evidence"
        )
    expected_hash = str(item.get("content_hash") or "")
    if expected_hash and str(getattr(record, "content_hash", "")) != expected_hash:
        raise OfflineRemediationError(
            f"Raw/control content hash mismatch for {expected_refresh}"
        )


def _validate_source_run(control: Any, source_run_id: str) -> None:
    """Accept immutable evidence only from a terminal live/backfill run."""

    try:
        source_run = control.get_run(source_run_id)
    except (TypeError, ValueError) as exc:
        raise OfflineRemediationError(
            f"Invalid source control run {source_run_id}"
        ) from exc
    if source_run is None:
        raise OfflineRemediationError(
            f"Unknown source control run {source_run_id}"
        )
    run_type = str(source_run.get("run_type") or "unknown").lower()
    if run_type not in {"current", "backfill"}:
        raise OfflineRemediationError(
            f"Source control run type is not replayable: {run_type}"
        )
    status = str(source_run.get("status") or "unknown").lower()
    if status not in {"succeeded", "failed", "cancelled"}:
        raise OfflineRemediationError(
            f"Source control run is not terminal: {status}"
        )


def run_offline_match_replay(
    *,
    source_control_run_id: str,
    target_dataset: str,
    parser_version: str,
    max_pages: int = MAX_REPLAY_PAGES,
    dry_run: bool = False,
    control: Optional[Any] = None,
    raw_store: Optional[Any] = None,
    adapter: Optional[Any] = None,
) -> OfflineReplayResult:
    """Replay one bounded batch of stored match pages with network fixed at 0."""

    source_run_id = _uuid_argument(source_control_run_id)
    if not str(target_dataset).strip() or target_dataset == "__page__":
        raise OfflineRemediationError("target dataset is invalid")
    if not str(parser_version).strip():
        raise OfflineRemediationError("parser version is required")
    if not 1 <= int(max_pages) <= MAX_REPLAY_PAGES:
        raise OfflineRemediationError(
            f"max pages must be between 1 and {MAX_REPLAY_PAGES}"
        )

    if control is None:
        from scrapers.fbref.control import ControlStore

        control = ControlStore.from_env()
    control_store = control
    _validate_source_run(control_store, source_run_id)

    candidates = control_store.list_replay_fetches(
        source_run_id,
        parser_version=parser_version,
        typed_parser_version=parser_version,
        stateful_parser_version=REMEDIATION_STATEFUL_VERSION,
        page_kinds=["match"],
        limit=int(max_pages),
    )
    if not candidates:
        source_matches = control_store.list_run_fetches(
            source_run_id,
            page_kinds=["match"],
            only_unparsed=False,
            parser_version=None,
            limit=1,
        )
        if not source_matches:
            raise OfflineRemediationError(
                f"Source control run {source_run_id} has no committed match pages"
            )
        return OfflineReplayResult(
            source_control_run_id=source_run_id,
            parser_version=parser_version,
            target_dataset=target_dataset,
            selected=0,
            processed=0,
            rows_written=0,
            remaining=False,
            dry_run=bool(dry_run),
        )

    if dry_run:
        return OfflineReplayResult(
            source_control_run_id=source_run_id,
            parser_version=parser_version,
            target_dataset=target_dataset,
            selected=len(candidates),
            processed=0,
            rows_written=0,
            remaining=len(candidates) == int(max_pages),
            dry_run=True,
        )

    if raw_store is None:
        from scrapers.fbref.raw_store import RawPageStore

        raw_store = RawPageStore.from_env(optional=False)
    store = raw_store
    if store is None:  # Defensive for injected factories and type narrowing.
        raise OfflineRemediationError("FBREF_RAW_STORE_URI is required")
    if adapter is None:
        from scrapers.fbref.typed_bronze import FBrefTypedBronzeAdapter

        adapter = FBrefTypedBronzeAdapter()
    typed_adapter = adapter
    run_id = _remediation_run_id(source_run_id, parser_version)
    enabled_datasets = {target_dataset}
    # Manager remediation also refreshes the legacy player table from the same
    # immutable HTML. Independent dataset-availability evidence is committed
    # last by the writer, without fetching anything.
    enabled_datasets.add(PLAYER_STATS_DATASET)

    processed = 0
    rows_written = 0
    failures: list[str] = []
    for item in candidates:
        record = None
        observation_lease = None
        try:
            observation_lease = control_store.claim_observation_processing(
                logical_refresh_id=item["logical_refresh_id"],
                target_id=item["target_id"],
                content_hash=item["content_hash"],
                parser_version=parser_version,
                typed_parser_version=parser_version,
                stateful_parser_version=REMEDIATION_STATEFUL_VERSION,
            )
            if observation_lease is None:
                raise OfflineRemediationError(
                    f"Observation already active or complete: "
                    f"{item['logical_refresh_id']}"
                )
            html, record = store.load_fetch_html(item["logical_refresh_id"])
            _validate_record(item, record)
            source_ids = dict(getattr(record, "source_ids", {}) or {})
            match_id = str(source_ids.get("match_id") or "").strip()
            if not match_id:
                raise OfflineRemediationError(
                    f"Raw match {getattr(record, 'target_id', '?')} lacks match_id"
                )
            with control_store.guard_latest_content(
                str(getattr(record, "target_id")),
                str(getattr(record, "content_hash")),
                str(getattr(record, "logical_refresh_id")),
            ) as is_latest:
                if is_latest is None:
                    raise OfflineRemediationError(
                        f"Latest-content fence unavailable for {match_id}"
                    )
                if not is_latest:
                    control_store.complete_observation_processing(
                        observation_lease,
                        typed_status="skipped",
                        stateful_status="skipped",
                    )
                    processed += 1
                    continue
                parsed, counts = typed_adapter.ingest_match_html(
                    html,
                    match_id=match_id,
                    context=_source_context(record),
                    run_id=run_id,
                    target_identity=str(getattr(record, "logical_refresh_id")),
                    enabled_datasets=enabled_datasets,
                    require_player_contract=True,
                )
                target_result = parsed.datasets.get(target_dataset)
                if target_result is None:
                    raise OfflineRemediationError(
                        f"Typed parser omitted requested dataset {target_dataset}"
                    )
                availability = _status_value(target_result)
                if availability == "error":
                    raise OfflineRemediationError(
                        f"Typed parser failed requested dataset {target_dataset}"
                    )
                target_rows = int(counts.get(target_dataset, 0) or 0)
                if availability == "available" and target_dataset not in counts:
                    raise OfflineRemediationError(
                        f"Requested dataset {target_dataset} was not persisted"
                    )
                _record_manifest(
                    control_store,
                    record,
                    parser_version=parser_version,
                    dataset=f"remediation:{target_dataset}",
                    availability=availability,
                    persistence_status=(
                        "succeeded" if target_dataset in counts else "skipped"
                    ),
                    validation_status="succeeded",
                    row_count=target_rows,
                )
                # Completion marker is deliberately last and inside the same
                # latest-content fence as the typed replacement.
                _record_manifest(
                    control_store,
                    record,
                    parser_version=parser_version,
                    dataset="__page__",
                    availability=availability,
                    persistence_status="succeeded",
                    validation_status="succeeded",
                    row_count=target_rows,
                )
                control_store.complete_observation_processing(
                    observation_lease,
                    typed_status="succeeded",
                    stateful_status="skipped",
                )
            processed += 1
            rows_written += target_rows
        except Exception as exc:  # Continue the bounded batch, fail overall.
            if observation_lease is not None:
                try:
                    control_store.fail_observation_processing(
                        observation_lease,
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                    )
                except Exception as observation_exc:  # pragma: no cover
                    failures.append(
                        f"{item.get('target_id')}:observation:"
                        f"{type(observation_exc).__name__}:{observation_exc}"
                    )
            if record is not None:
                try:
                    _record_manifest(
                        control_store,
                        record,
                        parser_version=parser_version,
                        dataset="__page__",
                        availability="error",
                        persistence_status="failed",
                        validation_status="failed",
                        row_count=0,
                        error=exc,
                    )
                except Exception as manifest_exc:  # pragma: no cover - DB outage
                    failures.append(
                        f"{item.get('target_id')}:manifest:"
                        f"{type(manifest_exc).__name__}:{manifest_exc}"
                    )
            failures.append(
                f"{item.get('target_id')}: {type(exc).__name__}: {exc}"
            )

    if failures:
        raise OfflineRemediationError("; ".join(failures))

    remaining = bool(
        control_store.list_replay_fetches(
            source_run_id,
            parser_version=parser_version,
            typed_parser_version=parser_version,
            stateful_parser_version=REMEDIATION_STATEFUL_VERSION,
            page_kinds=["match"],
            limit=1,
        )
    )
    return OfflineReplayResult(
        source_control_run_id=source_run_id,
        parser_version=parser_version,
        target_dataset=target_dataset,
        selected=len(candidates),
        processed=processed,
        rows_written=rows_written,
        remaining=remaining,
        dry_run=False,
    )


def build_cli_parser(*, description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--source-control-run-id",
        required=True,
        type=_uuid_argument,
        help="Required UUID of a raw-first source run containing match HTML",
    )
    parser.add_argument(
        "--max-pages",
        type=_bounded_pages,
        default=MAX_REPLAY_PAGES,
        help=f"Maximum stored match pages per invocation (1-{MAX_REPLAY_PAGES})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select the bounded raw cohort without parsing or persistence",
    )
    return parser


def remediation_cli(
    argv: Optional[Sequence[str]],
    *,
    description: str,
    target_dataset: str,
    parser_version: str,
    control: Optional[Any] = None,
    raw_store: Optional[Any] = None,
    adapter: Optional[Any] = None,
) -> int:
    args = build_cli_parser(description=description).parse_args(argv)
    try:
        result = run_offline_match_replay(
            source_control_run_id=args.source_control_run_id,
            target_dataset=target_dataset,
            parser_version=parser_version,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            control=control,
            raw_store=raw_store,
            adapter=adapter,
        )
    except OfflineRemediationError as exc:
        LOGGER.error("Offline remediation failed: %s", exc)
        return 1
    LOGGER.info("%s", json.dumps(result.as_dict(), sort_keys=True))
    return 0


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    control: Optional[Any] = None,
    raw_store: Optional[Any] = None,
    adapter: Optional[Any] = None,
) -> int:
    return remediation_cli(
        argv,
        description=__doc__ or "Offline FBref match player stats remediation",
        target_dataset=PLAYER_STATS_DATASET,
        parser_version=PLAYER_STATS_PARSER_VERSION,
        control=control,
        raw_store=raw_store,
        adapter=adapter,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(main())
