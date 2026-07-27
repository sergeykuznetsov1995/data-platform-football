#!/usr/bin/env python3
"""Run one production Understat league-season scope.

The Airflow DAG maps this process once per exact scope.  A successful
data-bearing attempt replaces all seven Bronze partitions with one shared
``_batch_id``.  A write-started manifest marker closes the legacy first-publish
crash window; the batch becomes visible only after the physical fence passes
and a terminal manifest row is appended.  Failures before that marker stay in
the task result/log and cannot supersede a previously published generation.
Expected source absence for a future season is represented by
``not_published``/``upstream_pending`` and is never confused with a published
empty partition.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Optional

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


MIN_REPLACE_RATIO = 0.9
PARSER_VERSION = "understat-native-v1"
REPLACE_GUARD_MARKER = "UNDERSTAT_REPLACE_GUARD"
PHYSICAL_FENCE_MARKER = "UNDERSTAT_PHYSICAL_BATCH_FENCE"


class PhysicalBatchFenceError(RuntimeError):
    """Seven written partitions do not match the proposed manifest batch."""


class LegacyCutoverBlocked(ValueError):
    """An empty v2 probe would hide a data-bearing legacy Bronze scope."""


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest one exact Understat league-season scope"
    )
    parser.add_argument(
        "--mode",
        choices=("current", "backfill"),
        required=True,
        help="Current rolling ingestion or manifest-resumable closed history",
    )
    parser.add_argument("--league", required=True, help="Canonical platform league")
    parser.add_argument(
        "--season-slug",
        required=True,
        help="Canonical four-digit season slug, for example 2526",
    )
    parser.add_argument(
        "--source-season-id",
        required=True,
        type=int,
        help="Understat season start year, for example 2025",
    )
    parser.add_argument(
        "--source-discovered",
        choices=("true", "false"),
        required=True,
        help=(
            "Whether getStatData advertised this exact scope. An empty "
            "advertised scope is a failure; only an undiscovered calendar "
            "probe may terminate as not_published."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Per-scope JSON result consumed by the mapped Airflow validator",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Logical scheduler run identifier (defaults to Airflow context)",
    )
    parser.add_argument(
        "--reparse",
        action="store_true",
        help="Bypass persistent source payload cache; DQ guards stay enabled",
    )
    parser.add_argument(
        "--force-replace",
        action="store_true",
        help=(
            "Explicitly bypass only the 90%% partition-size guard. Contract, "
            "natural-key and physical batch checks remain mandatory."
        ),
    )
    return parser


def _atomic_write_json(path: str | Path, payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, default=str)
            handle.write("\n")
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _result_payload(
    attempt: Any,
    *,
    tables: Optional[Mapping[str, str]] = None,
    errors: Optional[list[str]] = None,
) -> dict[str, Any]:
    serialized = attempt.to_dict()
    scope = serialized["scope"]
    return {
        "status": serialized["status"],
        "league": scope["league"],
        "season": scope["season"],
        "source_league": scope["source_league"],
        "source_season_id": scope["source_season_id"],
        "batch_id": serialized["batch_id"],
        "entity_statuses": serialized["entity_statuses"],
        "row_counts": serialized["row_counts"],
        "natural_key_counts": serialized["natural_key_counts"],
        "payload_hashes": serialized["payload_hashes"],
        "tables": dict(tables or {}),
        "errors": list(errors or []),
        "scope_attempt": serialized,
    }


def _decorate_frames(
    frames: Mapping[str, Optional[pd.DataFrame]],
    *,
    batch_id: str,
) -> dict[str, Optional[pd.DataFrame]]:
    """Attach stable runner-owned metadata before DQ and writer metadata."""

    from scrapers.understat.contracts import TABLE_CONTRACT_BY_NAME

    decorated: dict[str, Optional[pd.DataFrame]] = {}
    for entity, value in frames.items():
        if value is None:
            decorated[entity] = None
            continue
        if not isinstance(value, pd.DataFrame):
            raise TypeError(f"{entity}: expected a pandas DataFrame, got {type(value)!r}")
        frame = value.copy()
        contract = TABLE_CONTRACT_BY_NAME.get(entity)
        frame["_entity_type"] = contract.result_key if contract else entity
        frame["_batch_id"] = batch_id
        decorated[entity] = frame
    return decorated


def _classify_exception(exc: BaseException) -> tuple[Any, int, str]:
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.understat import (
        UnderstatHTTPError,
        UnderstatPayloadError,
        UnderstatSchemaDrift,
    )
    from scrapers.understat.manifest import ManifestStatus

    if isinstance(exc, ReplaceGuardError):
        return (
            ManifestStatus.CONTRACT_FAILURE,
            3,
            f"{REPLACE_GUARD_MARKER}: {exc}",
        )
    if isinstance(exc, PhysicalBatchFenceError):
        return (
            ManifestStatus.CONTRACT_FAILURE,
            1,
            f"{PHYSICAL_FENCE_MARKER}: {exc}",
        )
    if isinstance(exc, (UnderstatSchemaDrift, UnderstatPayloadError)):
        return ManifestStatus.SCHEMA_DRIFT, 1, str(exc)
    if isinstance(exc, (UnderstatHTTPError, TimeoutError, ConnectionError, OSError)):
        return ManifestStatus.RETRYABLE_FAILURE, 1, str(exc)
    if isinstance(exc, (TypeError, ValueError)):
        return ManifestStatus.CONTRACT_FAILURE, 1, str(exc)
    return ManifestStatus.RETRYABLE_FAILURE, 1, str(exc)


def _append_failure_best_effort(repository: Any, attempt: Any) -> Optional[str]:
    if repository is None:
        return "manifest repository was not initialized"
    try:
        repository.append_attempt(attempt)
    except Exception as exc:  # the original failure remains the primary verdict
        logger.exception("Unable to append terminal Understat failure manifest")
        return f"manifest append failed: {exc}"
    return None


def run_scope(
    args: argparse.Namespace,
    *,
    scraper_factory: Any = None,
    repository: Any = None,
) -> tuple[dict[str, Any], int]:
    """Execute one exact scope; injectable dependencies keep tests hermetic."""

    from scrapers.understat import UnderstatScraper
    from scrapers.understat.catalog import (
        LEAGUE_BY_CANONICAL,
        current_source_season_id,
        season_slug,
    )
    from scrapers.understat.manifest import (
        CONTRACT_VERSION,
        UNDERSTAT_ENTITIES,
        ManifestStatus,
        ScopeAttempt,
        ScopeKey,
        UnderstatManifestRepository,
        new_attempt_id,
        utc_now_iso,
    )
    from scrapers.understat.coverage import coverage_exceptions_for_scope
    from scrapers.understat.quality import (
        build_failure_attempt,
        build_scope_attempt,
        validate_understat_scope,
    )

    definition = LEAGUE_BY_CANONICAL.get(args.league)
    if definition is None:
        raise ValueError(f"Unsupported Understat league: {args.league!r}")
    expected_slug = season_slug(args.source_season_id)
    if args.season_slug != expected_slug:
        raise ValueError(
            "season/source mismatch: "
            f"{args.season_slug!r} != {expected_slug!r} for "
            f"{args.source_season_id}"
        )
    if args.mode == "backfill" and args.source_discovered != "true":
        raise ValueError(
            "backfill accepts only scopes advertised by getStatData"
        )

    batch_id = str(uuid.uuid4())
    run_id = str(
        args.run_id
        or os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
        or f"manual__{batch_id}"
    )
    scope = ScopeKey(
        league=args.league,
        season=args.season_slug,
        source_league=definition.source_league,
        source_season_id=str(args.source_season_id),
    )
    started_at = utc_now_iso()
    attempt_no = 1
    report = None
    proposed_attempt = None
    publication_started = False
    written_tables: dict[str, str] = {}

    if repository is None:
        repository = UnderstatManifestRepository.from_env()
    if scraper_factory is None:
        scraper_factory = UnderstatScraper

    try:
        repository.ensure_table()
        latest = repository.latest_attempt(scope, contract_version=CONTRACT_VERSION)
        attempt_no = (latest.attempt_no + 1) if latest else 1
        if (
            args.mode == "backfill"
            and latest is not None
            and latest.status is ManifestStatus.COMPLETE
        ):
            # Planning and execution release/reacquire the shared pool. A
            # higher-priority current task may publish this exact closed scope
            # in between; never invalidate that commit with a redundant retry.
            # A transient verification outage must likewise leave the good
            # manifest untouched instead of appending a newer failure marker.
            try:
                already_complete = repository.verify_physical_batch(latest)
            except Exception as exc:
                logger.exception(
                    "Unable to verify already-complete Understat history scope"
                )
                return _result_payload(
                    latest,
                    errors=[
                        "physical verification of the already-complete scope "
                        f"failed: {type(exc).__name__}: {exc}"
                    ],
                ), 1
            if already_complete:
                logger.info(
                    "Understat history scope already complete; skipping: %s/%s",
                    scope.league,
                    scope.season,
                )
                return _result_payload(latest), 0
        previous = repository.latest_data_attempt(
            scope,
            contract_version=CONTRACT_VERSION,
        )
        previous_counts = previous.row_counts if previous else {}

        service_mode = "history" if args.mode == "backfill" else "current"
        # History is intentionally source-refreshed on every non-complete run.
        # Pre-write failures are audit-only and do not supersede the publication
        # manifest, so manifest state cannot safely be used as a retry marker.
        # Refreshing each selected history scope also prevents a cached partial
        # HTTP-200 payload from trapping the self-draining DAG forever.
        retry_refresh = args.mode == "backfill"
        with scraper_factory(
            leagues=[args.league],
            seasons=[args.season_slug],
        ) as scraper:
            raw_frames = scraper.scrape_scope(
                args.league,
                args.season_slug,
                args.source_season_id,
                mode=service_mode,
                force_refresh=bool(args.reparse or retry_refresh),
            )
            frames = _decorate_frames(raw_frames, batch_id=batch_id)
            if (
                args.mode == "current"
                and frames["understat_schedule"].empty
                and previous is None
            ):
                # This check deliberately runs before either empty-response
                # branch below and until a data-bearing v2 attempt exists.
                # Appending even a failure marker here would make every
                # manifest-fenced reader treat the legacy scope as cut over
                # and hide its rows.  LegacyCutoverBlocked is therefore
                # returned to the caller without appending a v2 attempt.
                try:
                    legacy_schedule_rows = repository.physical_scope_row_count(
                        UNDERSTAT_ENTITIES[0], scope
                    )
                except Exception as exc:
                    raise LegacyCutoverBlocked(
                        "unable to establish that the exact legacy physical "
                        "scope is empty; refusing the v2 cutover"
                    ) from exc
                if legacy_schedule_rows:
                    raise LegacyCutoverBlocked(
                        "empty current probe conflicts with the exact legacy "
                        f"physical scope ({legacy_schedule_rows} schedule row(s)); "
                        "refusing to create a v2 manifest attempt"
                    )
            if (
                args.mode == "current"
                and args.source_discovered == "true"
                and frames["understat_schedule"].empty
            ):
                raise ValueError(
                    "source-discovered current scope returned an empty schedule; "
                    "treating it as a transient/contract failure, not not_published"
                )
            report = validate_understat_scope(
                frames,
                scope=scope,
                active=(
                    args.mode == "current"
                    and args.source_season_id >= current_source_season_id()
                ),
                previous_row_counts=previous_counts,
                batch_id=batch_id,
                coverage_exceptions=coverage_exceptions_for_scope(scope),
            )
            proposed_attempt = build_scope_attempt(
                report,
                batch_id=batch_id,
                run_id=run_id,
                mode=args.mode,
                parser_version=PARSER_VERSION,
                attempt_no=attempt_no,
                started_at=started_at,
            )

            if not report.passed:
                # No physical partition was touched. Persisting this DQ
                # failure in the publication manifest would hide either
                # pre-v2 legacy rows or the last COMPLETE batch. The task
                # result/log remains the audit record.
                message = proposed_attempt.error_message or report.status.value
                return _result_payload(proposed_attempt, errors=[message]), 1

            if report.status is ManifestStatus.COMPLETE:
                entities_to_write = UNDERSTAT_ENTITIES
            elif report.status is ManifestStatus.UPSTREAM_PENDING:
                entities_to_write = (UNDERSTAT_ENTITIES[0],)
            else:
                entities_to_write = ()

            if entities_to_write:
                # Fence the exact scope before the first non-transactional
                # table replacement. If the worker is killed mid-loop, this
                # marker remains the latest attempt and every native consumer
                # fails closed instead of treating a partial first migration
                # as legacy pre-manifest data.
                write_started = ScopeAttempt(
                    scope=scope,
                    status=ManifestStatus.IN_PROGRESS,
                    batch_id=batch_id,
                    run_id=run_id,
                    attempt_id=new_attempt_id(),
                    attempt_no=attempt_no,
                    mode=args.mode,
                    parser_version=PARSER_VERSION,
                    contract_version=CONTRACT_VERSION,
                    entity_statuses={
                        entity: ManifestStatus.IN_PROGRESS
                        for entity in UNDERSTAT_ENTITIES
                    },
                    row_counts={entity: 0 for entity in UNDERSTAT_ENTITIES},
                    natural_key_counts={
                        entity: 0 for entity in UNDERSTAT_ENTITIES
                    },
                    payload_hashes={entity: "" for entity in UNDERSTAT_ENTITIES},
                    quality={
                        "phase": "bronze_write_started",
                        "planned_row_counts": dict(report.row_counts),
                    },
                    started_at=started_at,
                    # Keep the marker ordered before the terminal row, whose
                    # completion time is captured after extraction/DQ.
                    completed_at=started_at,
                )
                repository.append_attempt(write_started)
                publication_started = True

            for entity in entities_to_write:
                frame = frames[entity]
                if frame is None or frame.empty:
                    raise ValueError(
                        f"{entity}: terminal {report.status.value} cannot write empty data"
                    )
                written_tables[entity] = scraper.save_to_iceberg(
                    df=frame,
                    table_name=entity,
                    partition_cols=["league", "season"],
                    replace_partitions=["league", "season"],
                    min_replace_ratio=(
                        None if args.force_replace else MIN_REPLACE_RATIO
                    ),
                    batch_id=batch_id,
                )

        if report.status is ManifestStatus.COMPLETE and not repository.verify_physical_batch(
            proposed_attempt
        ):
            raise PhysicalBatchFenceError(
                "one or more Bronze partitions have the wrong row count or batch_id"
            )

        # The report is built before the non-transactional writes, but the
        # terminal manifest timestamp must describe the actual publication
        # completion (including the physical fence), not DQ completion.
        proposed_attempt = replace(proposed_attempt, completed_at=utc_now_iso())
        repository.append_attempt(proposed_attempt)
        if args.mode == "backfill" and report.status is not ManifestStatus.COMPLETE:
            message = (
                "closed history scope did not publish completely: "
                f"{report.status.value}"
            )
            return _result_payload(
                proposed_attempt,
                tables=written_tables,
                errors=[message],
            ), 1
        return _result_payload(proposed_attempt, tables=written_tables), 0

    except Exception as exc:
        status, exit_code, message = _classify_exception(exc)
        logger.exception(
            "Understat scope failed: league=%s season=%s status=%s",
            scope.league,
            scope.season,
            status.value,
        )
        failure_kwargs: dict[str, Any] = {}
        if report is not None:
            failure_kwargs.update(
                row_counts=report.row_counts,
                natural_key_counts=report.natural_key_counts,
                payload_hashes=report.payload_hashes,
            )
        failure = build_failure_attempt(
            scope=scope,
            status=status,
            batch_id=batch_id,
            run_id=run_id,
            mode=args.mode,
            parser_version=PARSER_VERSION,
            error_type=type(exc).__name__,
            error_message=message,
            attempt_no=attempt_no,
            started_at=started_at,
            **failure_kwargs,
        )
        # Only a failure after the write-started fence may supersede an older
        # COMPLETE generation: at that point one or more physical partitions
        # may have changed. Pre-write extraction/DQ/HTTP failures remain in
        # the task result/log and leave both legacy fallback and the last
        # COMPLETE publication visible.
        append_error = None
        if publication_started:
            append_error = _append_failure_best_effort(repository, failure)
        errors = [message]
        if append_error:
            errors.append(append_error)
        return _result_payload(
            failure,
            tables=written_tables,
            errors=errors,
        ), exit_code


def main(argv: Optional[list[str]] = None) -> int:
    args = _argument_parser().parse_args(argv)
    try:
        result, exit_code = run_scope(args)
    except Exception as exc:
        # Argument/scope validation can fail before a ScopeAttempt exists.
        result = {
            "status": "contract_failure",
            "league": str(getattr(args, "league", "")),
            "season": str(getattr(args, "season_slug", "")),
            "source_season_id": str(getattr(args, "source_season_id", "")),
            "errors": [str(exc)],
        }
        exit_code = 1
        logger.exception("Understat runner could not initialize the requested scope")
    _atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
