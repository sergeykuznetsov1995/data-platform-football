"""Run one bounded FBref competition-index canary without Airflow or Silver.

The online scope is exactly one ``competition_index`` target.  Parsing reads
the immutable raw manifest written by the fetch step; no downstream frontier
target is fetched and no Airflow DAG is imported or triggered.

The 25-MiB control budget is settled after network responses complete; it is
not a preventive transport cap.  Every live invocation therefore requires a
separate preventive network-namespace quota capped at no more than 25 MiB.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.control import (  # noqa: E402
    CohortTarget,
    make_control_run_id,
    make_logical_refresh_id,
)
from scrapers.fbref.fetcher import MAX_TARGET_HTTP_ATTEMPTS  # noqa: E402
from scrapers.fbref.pipeline import (  # noqa: E402
    FBrefPipeline,
    PipelineSettings,
    SENTINEL_COMPETITIONS,
    frontier_target,
)
from scrapers.fbref.raw_store import competition_index_target  # noqa: E402
from scrapers.fbref.settings import (  # noqa: E402
    DEFAULT_BOOTSTRAP_REQUEST_RESERVATION,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    MIB,
)


CANARY_DAG_ID = "fbref_direct_live_canary"
CANARY_PAGE_KINDS = ("competition_index",)
CANARY_SHARD_SIZE = 1
MAX_REQUEST_LIMIT = 25
MAX_BYTE_LIMIT_MB = 25
MIN_REQUEST_LIMIT = (
    DEFAULT_BOOTSTRAP_REQUEST_RESERVATION + MAX_TARGET_HTTP_ATTEMPTS
)
MIN_BYTE_LIMIT_MB = max(1, DEFAULT_REQUEST_RESERVATION_BYTES // MIB)
_RUN_LABEL_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z")
_WAVE_INTEGER_FIELDS = (
    "cohort_size",
    "claimed",
    "fetched",
    "recovered_from_raw",
    "parsed",
    "seeded",
    "skipped_ineligible",
    "requests",
    "wire_bytes",
    "decoded_html_bytes",
    "browser_document_bytes",
    "browser_asset_bytes",
    "browser_bootstraps",
)
_TRAFFIC_COUNT_FIELDS = (
    "network_attempts",
    "warm_http_successes",
    "failed_network_attempts",
    "unclassified_failures",
    "classified_retries",
    "duplicate_fetch_violations",
)
_TRAFFIC_PERCENTILE_FIELDS = (
    "p50_latency_ms",
    "p95_latency_ms",
    "p50_http_wire_bytes",
    "p95_http_wire_bytes",
    "p50_provider_billed_bytes",
    "p95_provider_billed_bytes",
)
_TRAFFIC_AGGREGATE_BYTE_FIELDS = (
    "http_wire_bytes",
    "decoded_html_bytes",
    "compressed_raw_bytes",
    "provider_billed_bytes",
)
_TRAFFIC_TOTAL_COUNT_FIELDS = (
    "network_attempts",
    "warm_http_successes",
    "failed_network_attempts",
    "unclassified_failures",
    "classified_retries",
    "duplicate_fetch_violations",
)
_SESSION_INTEGER_FIELDS = (
    "sessions",
    "max_bootstraps_per_session",
    "browser_bootstrap_requests",
    "browser_document_bytes",
    "browser_asset_bytes",
    "http_requests",
    "http_wire_bytes",
    "decoded_html_bytes",
    "compressed_raw_bytes",
    "provider_billed_bytes",
)


class CanaryConfigurationError(ValueError):
    """The requested canary cannot satisfy its fixed safety contract."""


class CanaryInvariantError(RuntimeError):
    """The pipeline did not process exactly the requested canary scope."""


class CanaryExecutionError(RuntimeError):
    """Safe error wrapper that never retains an exception message."""

    def __init__(
        self,
        *,
        stage: str,
        error_class: str,
        logical_run_label: str,
        control_run_id: str,
        failure_finish_attempted: bool,
    ) -> None:
        super().__init__(f"FBref canary failed during {stage} ({error_class})")
        self.stage = stage
        self.error_class = error_class
        self.logical_run_label = logical_run_label
        self.control_run_id = control_run_id
        self.failure_finish_attempted = failure_finish_attempted

    def as_dict(self) -> dict[str, object]:
        return {
            "status": "failed",
            "logical_run_label": self.logical_run_label,
            "control_run_id": self.control_run_id,
            "stage": self.stage,
            "error_class": self.error_class,
            "failure_finish_attempted": self.failure_finish_attempted,
            "silver_triggered": False,
        }


@dataclass(frozen=True)
class CanaryConfig:
    """Validated bounds for one direct live canary."""

    logical_run_label: str
    proxy_file: Path
    request_limit: int = MAX_REQUEST_LIMIT
    byte_limit_mb: int = MAX_BYTE_LIMIT_MB

    def __post_init__(self) -> None:
        label = str(self.logical_run_label).strip()
        if not label:
            raise CanaryConfigurationError("logical run label is required")
        if len(label) > 160:
            raise CanaryConfigurationError("logical run label is too long")
        object.__setattr__(self, "logical_run_label", label)

        if type(self.request_limit) is not int or not (
            MIN_REQUEST_LIMIT <= self.request_limit <= MAX_REQUEST_LIMIT
        ):
            raise CanaryConfigurationError(
                f"request_limit must be {MIN_REQUEST_LIMIT}-{MAX_REQUEST_LIMIT}"
            )
        if type(self.byte_limit_mb) is not int or not (
            MIN_BYTE_LIMIT_MB <= self.byte_limit_mb <= MAX_BYTE_LIMIT_MB
        ):
            raise CanaryConfigurationError(
                f"byte_limit_mb must be {MIN_BYTE_LIMIT_MB}-{MAX_BYTE_LIMIT_MB}"
            )

        try:
            proxy_file = Path(self.proxy_file)
            if not proxy_file.is_absolute():
                raise CanaryConfigurationError(
                    "proxy_file must be an absolute path"
                )
            if not proxy_file.is_file() or not os.access(proxy_file, os.R_OK):
                raise CanaryConfigurationError(
                    "proxy_file must be a readable file"
                )
            if proxy_file.stat().st_size <= 0:
                raise CanaryConfigurationError("proxy_file must not be empty")
        except (OSError, TypeError) as exc:
            raise CanaryConfigurationError(
                "proxy_file must be a readable non-empty file"
            ) from exc
        object.__setattr__(self, "proxy_file", proxy_file)


def generate_logical_run_label(
    prefix: Optional[str] = None,
    *,
    now: Optional[datetime] = None,
    nonce: Optional[uuid.UUID] = None,
) -> str:
    """Return a traceable collision-resistant label for every invocation."""

    normalized_prefix = str(prefix or "fbref-canary").strip()
    if not _RUN_LABEL_PREFIX.fullmatch(normalized_prefix):
        raise CanaryConfigurationError(
            "run label must use only letters, digits, dot, underscore, or dash"
        )
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    suffix = (nonce or uuid.uuid4()).hex
    return (
        f"{normalized_prefix}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"
    )


def _settings(config: CanaryConfig) -> PipelineSettings:
    return PipelineSettings(
        run_type="current",
        request_limit=config.request_limit,
        byte_limit=config.byte_limit_mb * MIB,
        shard_size=CANARY_SHARD_SIZE,
        request_reservation_bytes=DEFAULT_REQUEST_RESERVATION_BYTES,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        bootstrap_request_reservation=DEFAULT_BOOTSTRAP_REQUEST_RESERVATION,
        proxy_file=str(config.proxy_file),
    )


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _safe_float(value: object) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        rendered = float(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None
    if rendered is None or not math.isfinite(rendered):
        return None
    return rendered


def _safe_token(value: object, *, max_length: int = 64) -> Optional[str]:
    if not isinstance(value, str) or len(value) > max_length:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        return None
    return value


def _safe_counts(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, count in value.items():
        normalized = str(key)
        if not re.fullmatch(r"[a-z_]{1,32}", normalized):
            continue
        safe_count = _safe_int(count)
        if safe_count is not None:
            counts[normalized] = safe_count
    return counts


def _safe_wave_summary(result: object) -> dict[str, object]:
    summary: dict[str, object] = {}
    for field in _WAVE_INTEGER_FIELDS:
        summary[field] = _safe_int(getattr(result, field, None))
    failures = getattr(result, "failures", ())
    summary["failure_count"] = len(failures) if isinstance(failures, list) else 0
    return summary


def _safe_traffic_row(value: object) -> dict[str, object]:
    source = value if isinstance(value, Mapping) else {}
    output: dict[str, object] = {
        field: _safe_int(source.get(field)) for field in _TRAFFIC_COUNT_FIELDS
    }
    output["warm_http_success_rate"] = _safe_float(
        source.get("warm_http_success_rate")
    )
    output.update({
        field: _safe_float(source.get(field))
        for field in _TRAFFIC_PERCENTILE_FIELDS
    })
    output.update({
        field: _safe_int(source.get(field))
        for field in _TRAFFIC_AGGREGATE_BYTE_FIELDS
    })
    return output


def _safe_traffic_by_page_kind(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, object] = {}
    for raw_page_kind, row in value.items():
        page_kind = _safe_token(raw_page_kind, max_length=32)
        if page_kind is not None and isinstance(row, Mapping):
            output[page_kind] = _safe_traffic_row(row)
    return output


def _safe_traffic_totals(value: object) -> dict[str, object]:
    source = value if isinstance(value, Mapping) else {}
    output: dict[str, object] = {
        field: _safe_int(source.get(field))
        for field in _TRAFFIC_TOTAL_COUNT_FIELDS
    }
    output["warm_http_success_rate"] = _safe_float(
        source.get("warm_http_success_rate")
    )
    output["unclassified_failure_rate"] = _safe_float(
        source.get("unclassified_failure_rate")
    )
    return output


def _safe_session_metrics(value: object) -> dict[str, Optional[int]]:
    source = value if isinstance(value, Mapping) else {}
    return {
        field: _safe_int(source.get(field)) for field in _SESSION_INTEGER_FIELDS
    }


def _safe_competition_coverage(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    output = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        crawl_state = _safe_token(item.get("crawl_state"))
        lifecycle_state = _safe_token(item.get("lifecycle_state"))
        count = _safe_int(item.get("count"))
        if crawl_state is None or lifecycle_state is None or count is None:
            continue
        output.append({
            "crawl_state": crawl_state,
            "lifecycle_state": lifecycle_state,
            "count": count,
        })
    return output


def _safe_sentinel_coverage(value: object) -> dict[str, object]:
    source = value if isinstance(value, Mapping) else {}
    output: dict[str, object] = {}
    for sentinel in SENTINEL_COMPETITIONS:
        item = source.get(sentinel)
        if not isinstance(item, Mapping):
            continue
        published = item.get("published")
        output[sentinel] = {
            "published": published if isinstance(published, bool) else None,
            "competition_id": _safe_token(item.get("competition_id")),
            "gender": _safe_token(item.get("gender")),
            "eligibility": _safe_token(item.get("eligibility")),
        }
    return output


def _safe_validation_summary(summary: object) -> dict[str, object]:
    source = summary if isinstance(summary, Mapping) else {}
    return {
        "requests_used": _safe_int(source.get("requests_used")),
        "bytes_used": _safe_int(source.get("bytes_used")),
        "budget_exceeded": bool(source.get("budget_exceeded", False)),
        "target_counts": _safe_counts(source.get("target_counts")),
        "dataset_validation_counts": _safe_counts(
            source.get("dataset_validation_counts")
        ),
        "traffic_by_page_kind": _safe_traffic_by_page_kind(
            source.get("traffic_by_page_kind")
        ),
        "traffic_totals": _safe_traffic_totals(source.get("traffic_totals")),
        "session_metrics": _safe_session_metrics(
            source.get("session_metrics")
        ),
        "competition_coverage": _safe_competition_coverage(
            source.get("competition_coverage")
        ),
        "table_availability": _safe_counts(
            source.get("table_availability")
        ),
        "sentinel_coverage": _safe_sentinel_coverage(
            source.get("sentinel_coverage")
        ),
        "female_downstream_targets": _safe_int(
            source.get("female_downstream_targets")
        ),
        "unknown_gender_downstream_targets": _safe_int(
            source.get("unknown_gender_downstream_targets")
        ),
    }


def _assert_fetch_scope(result: object, config: CanaryConfig) -> None:
    expected = {
        "cohort_size": 1,
        "claimed": 1,
        "fetched": 1,
        "recovered_from_raw": 0,
    }
    for field, expected_value in expected.items():
        if _safe_int(getattr(result, field, None)) != expected_value:
            raise CanaryInvariantError(
                f"fetch result must have {field}={expected_value}"
            )
    requests = _safe_int(getattr(result, "requests", None))
    if requests is None or not 1 <= requests <= config.request_limit:
        raise CanaryInvariantError("fetch request count is outside the canary limit")
    if getattr(result, "failures", None):
        raise CanaryInvariantError("fetch result contains failures")


def _assert_parse_scope(result: object) -> None:
    if _safe_int(getattr(result, "parsed", None)) != 1:
        raise CanaryInvariantError("offline parse must process exactly one page")
    if getattr(result, "failures", None):
        raise CanaryInvariantError("offline parse result contains failures")


def _best_effort_fail_run(pipeline: object, run_id: str) -> None:
    try:
        pipeline.control.finish_run(run_id, succeeded=False)
    except Exception:  # noqa: BLE001 - preserve the original safe failure class
        pass


def run_canary(
    config: CanaryConfig,
    *,
    pipeline: Optional[Any] = None,
    clock: Optional[Callable[[], datetime]] = None,
) -> dict[str, object]:
    """Execute one initialize/seed/fetch/offline-parse/validate lifecycle."""

    expected_run_id = make_control_run_id(
        config.logical_run_label,
        dag_id=CANARY_DAG_ID,
    )
    stage = "construct_pipeline"
    active_pipeline = pipeline
    try:
        if active_pipeline is None:
            active_pipeline = FBrefPipeline.from_env()
        settings = _settings(config)

        stage = "initialize"
        initialized_run_id = active_pipeline.initialize_run(
            airflow_run_id=config.logical_run_label,
            dag_id=CANARY_DAG_ID,
            settings=settings,
        )
        if initialized_run_id != expected_run_id:
            raise CanaryInvariantError("pipeline returned an unexpected run id")

        stage = "seed"
        target_id = active_pipeline.seed_competition_index()
        source_target = competition_index_target()
        if target_id != source_target.target_id:
            raise CanaryInvariantError("pipeline seeded an unexpected target")

        stage = "force_due_competition_index"
        due_at = (clock or (lambda: datetime.now(timezone.utc)))()
        if due_at.tzinfo is None:
            raise CanaryInvariantError("canary clock must return an aware datetime")
        active_pipeline.control.upsert_frontier_target(
            replace(
                frontier_target(source_target),
                next_fetch_at=due_at.astimezone(timezone.utc),
            )
        )

        stage = "seed_cohort"
        inserted = active_pipeline.control.create_run_cohort(
            expected_run_id,
            [
                CohortTarget(
                    target_id=target_id,
                    logical_refresh_id=make_logical_refresh_id(
                        expected_run_id, target_id
                    ),
                    ordinal=0,
                )
            ],
        )
        if inserted != 1:
            raise CanaryInvariantError("canary cohort must contain one new target")

        stage = "fetch_competition_index"
        fetch_result = active_pipeline.fetch_wave(
            expected_run_id,
            worker_id=f"direct-canary:{expected_run_id}",
            page_kinds=CANARY_PAGE_KINDS,
            settings=settings,
        )
        _assert_fetch_scope(fetch_result, config)

        stage = "offline_parse_competition_index"
        parse_result = active_pipeline.parse_wave(
            expected_run_id,
            page_kinds=CANARY_PAGE_KINDS,
            settings=settings,
        )
        _assert_parse_scope(parse_result)

        stage = "validate"
        validation = active_pipeline.validate_and_finish(expected_run_id)
    except Exception as exc:  # noqa: BLE001 - return a redacted JSON failure
        finish_attempted = False
        if active_pipeline is not None:
            finish_attempted = True
            _best_effort_fail_run(active_pipeline, expected_run_id)
        raise CanaryExecutionError(
            stage=stage,
            error_class=type(exc).__name__,
            logical_run_label=config.logical_run_label,
            control_run_id=expected_run_id,
            failure_finish_attempted=finish_attempted,
        ) from None

    return {
        "status": "succeeded",
        "logical_run_label": config.logical_run_label,
        "control_run_id": expected_run_id,
        "scope": {
            "page_kinds": list(CANARY_PAGE_KINDS),
            "shard_size": CANARY_SHARD_SIZE,
            "parse_mode": "offline_raw_manifest",
        },
        "limits": {
            "request_limit": config.request_limit,
            "byte_limit_mb": config.byte_limit_mb,
        },
        "target_id": target_id,
        "fetch": _safe_wave_summary(fetch_result),
        "parse": _safe_wave_summary(parse_result),
        "validation": _safe_validation_summary(validation),
        "silver_triggered": False,
    }


def _bounded_int(value: str, *, name: str, lower: int, upper: int) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{name} must be an integer") from exc
    if not lower <= parsed <= upper:
        raise argparse.ArgumentTypeError(
            f"{name} must be between {lower} and {upper}"
        )
    return parsed


def _request_limit(value: str) -> int:
    return _bounded_int(
        value,
        name="request limit",
        lower=MIN_REQUEST_LIMIT,
        upper=MAX_REQUEST_LIMIT,
    )


def _byte_limit_mb(value: str) -> int:
    return _bounded_int(
        value,
        name="byte limit MiB",
        lower=MIN_BYTE_LIMIT_MB,
        upper=MAX_BYTE_LIMIT_MB,
    )


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--proxy-file",
        type=Path,
        required=True,
        help="Explicit absolute path to the non-empty proxy list",
    )
    parser.add_argument(
        "--run-label",
        help=(
            "Optional logical label prefix; a UTC timestamp and UUID are always "
            "appended"
        ),
    )
    parser.add_argument(
        "--request-limit",
        type=_request_limit,
        default=MAX_REQUEST_LIMIT,
        help=f"Hard control-plane request limit ({MIN_REQUEST_LIMIT}-25)",
    )
    parser.add_argument(
        "--byte-limit-mb",
        type=_byte_limit_mb,
        default=MAX_BYTE_LIMIT_MB,
        help=f"Hard control-plane byte limit in MiB ({MIN_BYTE_LIMIT_MB}-25)",
    )
    return parser


def _configuration_failure(error_class: str) -> dict[str, object]:
    return {
        "status": "failed",
        "stage": "configuration",
        "error_class": error_class,
        "failure_finish_attempted": False,
        "silver_triggered": False,
    }


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    pipeline: Optional[Any] = None,
) -> int:
    args = build_cli_parser().parse_args(argv)
    try:
        config = CanaryConfig(
            logical_run_label=generate_logical_run_label(args.run_label),
            proxy_file=args.proxy_file,
            request_limit=args.request_limit,
            byte_limit_mb=args.byte_limit_mb,
        )
    except CanaryConfigurationError as exc:
        print(json.dumps(_configuration_failure(type(exc).__name__), sort_keys=True))
        return 2

    try:
        result = run_canary(config, pipeline=pipeline)
    except CanaryExecutionError as exc:
        print(json.dumps(exc.as_dict(), sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
