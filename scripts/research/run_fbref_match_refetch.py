"""Refetch an explicit list of FBref match pages through the raw-first pipeline.

Bounded remediation for match pages whose Bronze scores exist without raw
event rows (issues #901/#934): the legacy scraper kept no raw HTML, so the
listed pages are fetched again through the production control plane
(Camoufox clearance + warm curl_cffi) and committed as raw-v2, generic
Bronze, and typed Bronze match datasets.  No Airflow module is imported and
no Silver transform is triggered.

Budget semantics: one fetch wave constructs one fetcher and therefore
reserves one 20-request clearance bootstrap; every live target reserves two
further HTTP attempts.  Within the fixed 25-request ceiling only one wave is
sane, so each invocation runs exactly one bounded wave and live-fetches at
most a couple of targets.  Targets whose raw already exists are recovered
through the raw-first bridge with zero network.  Unfinished targets stay
pending and are resumed by re-running the script: every rerun creates a new
control run and cohort, the control store requeues completed frontier rows,
and only targets held by an ACTIVE run are refused.

Control-plane preconditions (never bypassed): claiming requires a present,
active, male competition-registry row for every target competition and a
present season-registry row for every (competition, season) pair; the
``historical_once`` refresh policy waives the current-season requirement.
When nothing is claimable the failure JSON carries ``hint="registry_missing"``.
Run validation additionally applies the sentinel-competition gate to every
non-replay run, so the control database must already hold published,
eligible sentinel competitions (true after production discovery).

The 25-MiB control budget is settled after network responses complete; it is
not a preventive transport cap.  Every live invocation therefore requires a
separate preventive network-namespace quota capped at no more than 25 MiB.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import signal
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
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
    frontier_target,
)
from scrapers.fbref.raw_store import (  # noqa: E402
    PageTarget,
    match_page_target,
)
from scrapers.fbref.settings import (  # noqa: E402
    DEFAULT_BOOTSTRAP_REQUEST_RESERVATION,
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    MIB,
)
_CANARY_MODULE_NAME = "_fbref_canary_shared"
_CANARY_PATH = Path(__file__).resolve().parent / "run_fbref_canary.py"


def _load_canary_module() -> ModuleType:
    """Load the sibling canary script by file path.

    A plain ``scripts.research`` package import is unreliable: the test
    harness prepends ``dags/`` to ``sys.path`` and its regular ``scripts``
    package shadows the repository-root namespace package.
    """

    existing = sys.modules.get(_CANARY_MODULE_NAME)
    if isinstance(existing, ModuleType):
        return existing
    spec = importlib.util.spec_from_file_location(
        _CANARY_MODULE_NAME, _CANARY_PATH
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load canary module from {_CANARY_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_CANARY_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


_canary = _load_canary_module()
CanaryConfigurationError = _canary.CanaryConfigurationError
_best_effort_fail_run = _canary._best_effort_fail_run
_bounded_int = _canary._bounded_int
_configuration_failure = _canary._configuration_failure
_safe_int = _canary._safe_int
_safe_validation_summary = _canary._safe_validation_summary
_safe_wave_summary = _canary._safe_wave_summary
generate_logical_run_label = _canary.generate_logical_run_label


REFETCH_DAG_ID = "fbref_match_refetch_remediation"
REFETCH_PAGE_KINDS = ("match",)
DEFAULT_RUN_LABEL_PREFIX = "fbref-match-refetch"
MAX_REQUEST_LIMIT = 25
MAX_BYTE_LIMIT_MB = 25
MIN_REQUEST_LIMIT = (
    DEFAULT_BOOTSTRAP_REQUEST_RESERVATION + MAX_TARGET_HTTP_ATTEMPTS
)
MIN_BYTE_LIMIT_MB = max(
    1,
    (
        DEFAULT_BROWSER_BYTE_LIMIT_BYTES
        + DEFAULT_REQUEST_RESERVATION_BYTES
        + MIB
        - 1
    )
    // MIB,
)
MAX_TARGETS_PER_INVOCATION = 25
REGISTRY_MISSING_HINT = "registry_missing"
_FETCH_STAGE = "fetch_matches"
_TARGET_FIELDS = ("match_id", "url", "competition_id", "season_id")


class RefetchConfigurationError(ValueError):
    """The requested refetch cannot satisfy its fixed safety contract."""


class RefetchInvariantError(RuntimeError):
    """The pipeline did not process exactly the requested refetch scope."""


class RefetchExecutionError(RuntimeError):
    """Safe error wrapper that never retains an exception message."""

    def __init__(
        self,
        *,
        stage: str,
        error_class: str,
        logical_run_label: str,
        control_run_id: str,
        failure_finish_attempted: bool,
        hint: Optional[str] = None,
    ) -> None:
        super().__init__(
            f"FBref match refetch failed during {stage} ({error_class})"
        )
        self.stage = stage
        self.error_class = error_class
        self.logical_run_label = logical_run_label
        self.control_run_id = control_run_id
        self.failure_finish_attempted = failure_finish_attempted
        self.hint = hint

    def as_dict(self) -> dict[str, object]:
        return {
            "status": "failed",
            "logical_run_label": self.logical_run_label,
            "control_run_id": self.control_run_id,
            "stage": self.stage,
            "error_class": self.error_class,
            "failure_finish_attempted": self.failure_finish_attempted,
            "hint": self.hint,
            "silver_triggered": False,
        }


@dataclass(frozen=True)
class MatchRefetchTarget:
    """One validated match page plus its mandatory typed-Bronze identity."""

    match_id: str
    competition_id: str
    season_id: str
    page_target: PageTarget


def _validated_input_file(value: object, name: str) -> Path:
    try:
        path = Path(value)
        if not path.is_absolute():
            raise RefetchConfigurationError(
                f"{name} must be an absolute path"
            )
        if not path.is_file() or not os.access(path, os.R_OK):
            raise RefetchConfigurationError(f"{name} must be a readable file")
        if path.stat().st_size <= 0:
            raise RefetchConfigurationError(f"{name} must not be empty")
    except (OSError, TypeError) as exc:
        raise RefetchConfigurationError(
            f"{name} must be a readable non-empty file"
        ) from exc
    return path


@dataclass(frozen=True)
class RefetchConfig:
    """Validated bounds for one bounded match refetch invocation."""

    logical_run_label: str
    proxy_file: Path
    targets_file: Path
    request_limit: int = MAX_REQUEST_LIMIT
    byte_limit_mb: int = MAX_BYTE_LIMIT_MB

    def __post_init__(self) -> None:
        label = str(self.logical_run_label).strip()
        if not label:
            raise RefetchConfigurationError("logical run label is required")
        if len(label) > 160:
            raise RefetchConfigurationError("logical run label is too long")
        object.__setattr__(self, "logical_run_label", label)

        if type(self.request_limit) is not int or not (
            MIN_REQUEST_LIMIT <= self.request_limit <= MAX_REQUEST_LIMIT
        ):
            raise RefetchConfigurationError(
                f"request_limit must be {MIN_REQUEST_LIMIT}-{MAX_REQUEST_LIMIT}"
            )
        if type(self.byte_limit_mb) is not int or not (
            MIN_BYTE_LIMIT_MB <= self.byte_limit_mb <= MAX_BYTE_LIMIT_MB
        ):
            raise RefetchConfigurationError(
                f"byte_limit_mb must be {MIN_BYTE_LIMIT_MB}-{MAX_BYTE_LIMIT_MB}"
            )

        object.__setattr__(
            self,
            "proxy_file",
            _validated_input_file(self.proxy_file, "proxy_file"),
        )
        object.__setattr__(
            self,
            "targets_file",
            _validated_input_file(self.targets_file, "targets_file"),
        )


def build_match_refetch_target(item: object) -> MatchRefetchTarget:
    """Validate one targets-file entry into a canonical match PageTarget."""

    if not isinstance(item, Mapping):
        raise RefetchConfigurationError("every target must be a JSON object")
    values: dict[str, str] = {}
    for field in _TARGET_FIELDS:
        value = item.get(field)
        if not isinstance(value, str) or not value.strip():
            raise RefetchConfigurationError(
                f"target field {field} must be a non-empty string"
            )
        values[field] = value.strip()
    try:
        base = match_page_target(values["url"])
    except ValueError as exc:
        raise RefetchConfigurationError(
            "target url must be a canonical FBref match URL"
        ) from exc
    if base.page_kind != "match" or not base.source_ids.get("match_id"):
        raise RefetchConfigurationError(
            "target url did not resolve to a match page"
        )
    match_id = base.source_ids["match_id"]
    if values["match_id"].lower() != match_id:
        raise RefetchConfigurationError(
            "target match_id does not match its url"
        )
    # competition_id and season_id are mandatory: without them the typed
    # persistence context cannot be built and typed Bronze fails closed.
    return MatchRefetchTarget(
        match_id=match_id,
        competition_id=values["competition_id"],
        season_id=values["season_id"],
        page_target=PageTarget(
            source=base.source,
            page_kind=base.page_kind,
            target_id=base.target_id,
            canonical_url=base.canonical_url,
            source_ids={
                "match_id": match_id,
                "competition_id": values["competition_id"],
                "season_id": values["season_id"],
            },
        ),
    )


def load_refetch_targets(targets_file: Path) -> tuple[MatchRefetchTarget, ...]:
    """Load and validate the explicit bounded refetch cohort."""

    try:
        payload = json.loads(targets_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise RefetchConfigurationError(
            "targets_file must contain valid JSON"
        ) from exc
    if not isinstance(payload, list) or not payload:
        raise RefetchConfigurationError(
            "targets_file must be a non-empty JSON list"
        )
    if len(payload) > MAX_TARGETS_PER_INVOCATION:
        raise RefetchConfigurationError(
            "targets_file must list at most "
            f"{MAX_TARGETS_PER_INVOCATION} targets per invocation"
        )
    targets = tuple(build_match_refetch_target(item) for item in payload)
    target_ids = [target.page_target.target_id for target in targets]
    if len(set(target_ids)) != len(target_ids):
        raise RefetchConfigurationError(
            "targets_file must not repeat a match"
        )
    return targets


def _settings(config: RefetchConfig, *, shard_size: int) -> PipelineSettings:
    return PipelineSettings(
        run_type="backfill",
        request_limit=config.request_limit,
        byte_limit=config.byte_limit_mb * MIB,
        shard_size=shard_size,
        request_reservation_bytes=DEFAULT_REQUEST_RESERVATION_BYTES,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        bootstrap_request_reservation=DEFAULT_BOOTSTRAP_REQUEST_RESERVATION,
        proxy_file=str(config.proxy_file),
    )


def _assert_fetch_scope(
    result: object, config: RefetchConfig, target_count: int
) -> None:
    fetched = _safe_int(getattr(result, "fetched", None))
    recovered = _safe_int(getattr(result, "recovered_from_raw", None))
    claimed = _safe_int(getattr(result, "claimed", None))
    if (
        fetched is None
        or recovered is None
        or claimed is None
        or fetched < 0
        or recovered < 0
    ):
        raise RefetchInvariantError("fetch result is missing bounded counters")
    if fetched + recovered > target_count or claimed > target_count:
        raise RefetchInvariantError(
            "fetch result exceeds the requested refetch targets"
        )
    if fetched + recovered != claimed:
        raise RefetchInvariantError(
            "every claimed target must be recovered from raw or fetched"
        )
    requests = _safe_int(getattr(result, "requests", None))
    if requests is None or not 0 <= requests <= config.request_limit:
        raise RefetchInvariantError(
            "fetch request count is outside the refetch limit"
        )
    if getattr(result, "failures", None):
        raise RefetchInvariantError("fetch result contains failures")


def _assert_parse_scope(result: object, target_count: int) -> None:
    parsed = _safe_int(getattr(result, "parsed", None))
    if parsed is None or not 0 <= parsed <= target_count:
        raise RefetchInvariantError(
            "offline parse count is outside the refetch cohort"
        )
    if getattr(result, "failures", None):
        raise RefetchInvariantError("offline parse result contains failures")


def _counts(target_count: int, fetch_result: object) -> dict[str, int]:
    recovered = _safe_int(
        getattr(fetch_result, "recovered_from_raw", None)
    ) or 0
    fetched = _safe_int(getattr(fetch_result, "fetched", None)) or 0
    failures = getattr(fetch_result, "failures", ())
    failed = len(failures) if isinstance(failures, list) else 0
    pending = max(0, target_count - recovered - fetched - failed)
    return {
        "targets": target_count,
        "recovered_from_raw": recovered,
        "fetched": fetched,
        "failed": failed,
        "pending": pending,
    }


def _best_effort_registry_hint(pipeline: Any, run_id: str) -> Optional[str]:
    """Return a fixed-vocabulary claim-gate hint; never surface store values.

    An untouched cohort (every run target still pending after a fetch wave)
    means claiming returned no leases, which is exactly what missing
    competition/season registry rows look like from the control plane.
    """

    try:
        summary = pipeline.control.get_run_summary(run_id) or {}
        target_counts = summary.get("target_counts") or {}
        pending = 0
        attempted = 0
        for status, count in dict(target_counts).items():
            safe_count = _safe_int(count) or 0
            if str(status) == "pending":
                pending += safe_count
            else:
                attempted += safe_count
        if pending > 0 and attempted == 0:
            return REGISTRY_MISSING_HINT
    except Exception:  # noqa: BLE001 - hints must never mask the real failure
        return None
    return None


def run_refetch(
    config: RefetchConfig,
    targets: Sequence[MatchRefetchTarget],
    *,
    pipeline: Optional[Any] = None,
    clock: Optional[Callable[[], datetime]] = None,
) -> dict[str, object]:
    """Execute one initialize/seed/recover/fetch/parse/validate lifecycle."""

    if not targets or len(targets) > MAX_TARGETS_PER_INVOCATION:
        raise RefetchConfigurationError(
            "refetch requires between 1 and "
            f"{MAX_TARGETS_PER_INVOCATION} validated targets"
        )
    expected_run_id = make_control_run_id(
        config.logical_run_label,
        dag_id=REFETCH_DAG_ID,
    )
    shard_size = min(len(targets), MAX_TARGETS_PER_INVOCATION)
    stage = "construct_pipeline"
    active_pipeline = pipeline
    try:
        if active_pipeline is None:
            active_pipeline = FBrefPipeline.from_env()
        settings = _settings(config, shard_size=shard_size)

        stage = "initialize"
        initialized_run_id = active_pipeline.initialize_run(
            airflow_run_id=config.logical_run_label,
            dag_id=REFETCH_DAG_ID,
            settings=settings,
        )
        if initialized_run_id != expected_run_id:
            raise RefetchInvariantError("pipeline returned an unexpected run id")

        stage = "acquire_publication_lock"
        active_pipeline.control.acquire_publication_lock(
            expected_run_id,
            dag_id=REFETCH_DAG_ID,
            ttl_seconds=2 * 60 * 60,
        )

        stage = "force_due_targets"
        due_at = (clock or (lambda: datetime.now(timezone.utc)))()
        if due_at.tzinfo is None:
            raise RefetchInvariantError(
                "refetch clock must return an aware datetime"
            )
        due_at = due_at.astimezone(timezone.utc)
        for target in targets:
            # historical_once makes the backfill claim filter and the
            # non-current season gate pass; the explicit due-now timestamp is
            # load-bearing because a match row left behind by a recurring
            # policy would otherwise keep its future next_fetch_at.
            active_pipeline.control.upsert_frontier_target(
                replace(
                    frontier_target(target.page_target, historical=True),
                    next_fetch_at=due_at,
                )
            )

        stage = "seed_cohort"
        cohort = [
            CohortTarget(
                target_id=target.page_target.target_id,
                logical_refresh_id=make_logical_refresh_id(
                    expected_run_id, target.page_target.target_id
                ),
                ordinal=ordinal,
            )
            for ordinal, target in enumerate(targets)
        ]
        inserted = active_pipeline.control.create_run_cohort(
            expected_run_id, cohort
        )
        if inserted != len(cohort):
            raise RefetchInvariantError(
                "refetch cohort must insert every requested target"
            )

        stage = "recover_from_raw"
        raw_store = getattr(active_pipeline, "raw_store", None)
        bridge = getattr(raw_store, "import_fetch_from_available_raw", None)
        if callable(bridge):
            for target, member in zip(targets, cohort):
                # Pre-existing raw becomes a v2 commit under this cohort's
                # logical refresh id, so the fetch wave recovers the target
                # with zero network.  Corrupt raw fails closed here.
                bridge(
                    target.page_target,
                    logical_refresh_id=member.logical_refresh_id,
                )

        stage = _FETCH_STAGE
        fetch_result = active_pipeline.fetch_wave(
            expected_run_id,
            worker_id=f"match-refetch:{expected_run_id}",
            page_kinds=REFETCH_PAGE_KINDS,
            settings=settings,
        )
        _assert_fetch_scope(fetch_result, config, len(targets))

        stage = "offline_parse_matches"
        parse_result = active_pipeline.parse_wave(
            expected_run_id,
            page_kinds=REFETCH_PAGE_KINDS,
            settings=settings,
        )
        _assert_parse_scope(parse_result, len(targets))

        stage = "validate"
        validation = active_pipeline.validate_and_finish(expected_run_id)

        stage = "release_publication_lock"
        active_pipeline.control.release_publication_lock(expected_run_id)
    except Exception as exc:  # noqa: BLE001 - return a redacted JSON failure
        finish_attempted = False
        hint = None
        if active_pipeline is not None:
            if stage == _FETCH_STAGE:
                hint = _best_effort_registry_hint(
                    active_pipeline, expected_run_id
                )
            finish_attempted = True
            _best_effort_fail_run(active_pipeline, expected_run_id)
            try:
                active_pipeline.control.release_publication_lock(
                    expected_run_id
                )
            except Exception:  # noqa: BLE001 - preserve original failure
                pass
        raise RefetchExecutionError(
            stage=stage,
            error_class=type(exc).__name__,
            logical_run_label=config.logical_run_label,
            control_run_id=expected_run_id,
            failure_finish_attempted=finish_attempted,
            hint=hint,
        ) from None

    return {
        "status": "succeeded",
        "logical_run_label": config.logical_run_label,
        "control_run_id": expected_run_id,
        "scope": {
            "page_kinds": list(REFETCH_PAGE_KINDS),
            "shard_size": shard_size,
            "run_type": "backfill",
            "parse_mode": "offline_raw_manifest",
        },
        "limits": {
            "request_limit": config.request_limit,
            "byte_limit_mb": config.byte_limit_mb,
        },
        "counts": _counts(len(targets), fetch_result),
        "fetch": _safe_wave_summary(fetch_result),
        "parse": _safe_wave_summary(parse_result),
        "validation": _safe_validation_summary(validation),
        "silver_triggered": False,
    }


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
        "--targets-file",
        type=Path,
        required=True,
        help=(
            "Absolute path to a JSON list of objects with match_id, url, "
            "competition_id, and season_id"
        ),
    )
    parser.add_argument(
        "--run-label",
        help=(
            "Optional logical label prefix; a UTC timestamp and UUID are "
            "always appended"
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


def _raise_on_terminate(signum: int, _frame: object) -> None:
    raise RuntimeError(f"refetch terminated by signal {signum}")


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    pipeline: Optional[Any] = None,
) -> int:
    args = build_cli_parser().parse_args(argv)
    # A Camoufox launch can hang on a dead proxy with no timeout of its own, so
    # this script is normally run under an external one.  Turn that SIGTERM into
    # an exception: the failure path below then fails the control run and
    # releases its fenced leases, instead of leaving them attached to a run
    # that is still marked 'running'.
    signal.signal(signal.SIGTERM, _raise_on_terminate)
    try:
        config = RefetchConfig(
            logical_run_label=generate_logical_run_label(
                args.run_label or DEFAULT_RUN_LABEL_PREFIX
            ),
            proxy_file=args.proxy_file,
            targets_file=args.targets_file,
            request_limit=args.request_limit,
            byte_limit_mb=args.byte_limit_mb,
        )
        targets = load_refetch_targets(config.targets_file)
    except (RefetchConfigurationError, CanaryConfigurationError) as exc:
        print(json.dumps(_configuration_failure(type(exc).__name__), sort_keys=True))
        return 2

    try:
        result = run_refetch(config, targets, pipeline=pipeline)
    except RefetchExecutionError as exc:
        print(json.dumps(exc.as_dict(), sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
