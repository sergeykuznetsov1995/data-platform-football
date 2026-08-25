#!/usr/bin/env python3
"""Reconcile explicit FBref competitions from committed source evidence.

The default is a read-only dry-run. ``--apply`` additionally requires the
control run that owns each latest immutable competition-history observation;
the script verifies that lineage before invoking the normal registry/frontier
installation path. It never fetches a URL.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence


def _repo_root_for_execution(script_file: object) -> Optional[Path]:
    """Return a checkout root only for an on-disk script invocation."""

    if str(script_file) == "<stdin>":
        return None
    return Path(str(script_file)).resolve().parents[2]


REPO_ROOT = _repo_root_for_execution(__file__)
if REPO_ROOT is not None:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.control import (  # noqa: E402
    ControlStore,
    CurrentSeasonRemediationEvidence,
)
from scrapers.fbref.discovery import (  # noqa: E402
    advertised_current_season,
    parse_competition_html,
)
from scrapers.fbref.pipeline import (  # noqa: E402
    CURRENT_SEASON_INSTALL_CONTRACT_VERSION,
    CurrentSeasonRemediationItem,
    FBrefPipeline,
    _competition_from_registry,
    _resolve_current_season_install,
)
from scrapers.fbref.raw_store import (  # noqa: E402
    RawFetchRecord,
    RawPageStore,
    competition_page_target,
)


MAX_COMPETITIONS = 25


class RemediationConfigurationError(ValueError):
    """The requested remediation is not explicit and bounded."""


class RemediationEvidenceError(RuntimeError):
    """Committed raw or control lineage cannot prove the requested change."""


@dataclass(frozen=True)
class CurrentSeasonRemediationPlan:
    competition_id: str
    installed_current_season_id: Optional[str]
    advertised_current_season_id: str
    advertised_current_label: str
    advertised_current_url: str
    action: str
    history_attempt_id: str
    history_run_id: str
    history_content_hash: str
    history_logical_refresh_id: str
    history_raw_manifest_key: str
    index_snapshot_id: str
    index_run_id: str
    index_attempt_id: str
    index_target_id: str
    index_logical_refresh_id: str
    index_content_hash: str
    index_raw_manifest_key: str
    install_contract_version: str
    _history_html: str = field(repr=False)
    _history_record: RawFetchRecord = field(repr=False)

    def remediation_item(self) -> CurrentSeasonRemediationItem:
        return CurrentSeasonRemediationItem(
            evidence=CurrentSeasonRemediationEvidence(
                competition_id=self.competition_id,
                advertised_label=self.advertised_current_label,
                advertised_href=self.advertised_current_url,
                advertised_season_id=self.advertised_current_season_id,
                index_snapshot_id=self.index_snapshot_id,
                index_run_id=self.index_run_id,
                index_attempt_id=self.index_attempt_id,
                index_target_id=self.index_target_id,
                index_logical_refresh_id=self.index_logical_refresh_id,
                index_content_hash=self.index_content_hash,
                index_raw_manifest_key=self.index_raw_manifest_key,
                history_run_id=self.history_run_id,
                history_attempt_id=self.history_attempt_id,
                history_target_id=self._history_record.target_id,
                history_logical_refresh_id=self.history_logical_refresh_id,
                history_content_hash=self.history_content_hash,
                history_raw_manifest_key=self.history_raw_manifest_key,
            ),
            history_html=self._history_html,
            history_record=self._history_record,
        )

    def evidence(self) -> dict:
        return {
            "competition_id": self.competition_id,
            "installed_current_season_id": self.installed_current_season_id,
            "advertised_current_season_id": (self.advertised_current_season_id),
            "advertised_current_label": self.advertised_current_label,
            "advertised_current_url": self.advertised_current_url,
            "action": self.action,
            "history_attempt_id": self.history_attempt_id,
            "history_run_id": self.history_run_id,
            "history_content_hash": self.history_content_hash,
            "history_logical_refresh_id": self.history_logical_refresh_id,
            "history_raw_manifest_key": self.history_raw_manifest_key,
            "index_snapshot_id": self.index_snapshot_id,
            "index_run_id": self.index_run_id,
            "index_attempt_id": self.index_attempt_id,
            "index_target_id": self.index_target_id,
            "index_logical_refresh_id": self.index_logical_refresh_id,
            "index_content_hash": self.index_content_hash,
            "index_raw_manifest_key": self.index_raw_manifest_key,
            "install_contract_version": self.install_contract_version,
        }


def _normalized_scope(competition_ids: Sequence[object]) -> list[str]:
    normalized = [str(value).strip() for value in competition_ids]
    if not normalized or any(not value for value in normalized):
        raise RemediationConfigurationError(
            "at least one explicit competition_id is required"
        )
    if len(normalized) > MAX_COMPETITIONS:
        raise RemediationConfigurationError(
            f"at most {MAX_COMPETITIONS} competition IDs are allowed"
        )
    if len(normalized) != len(set(normalized)):
        raise RemediationConfigurationError("competition IDs must be unique")
    return normalized


def _current_seasons(control) -> dict[str, dict]:
    rows: list[dict] = []
    after = None
    while True:
        page = control.list_seasons(current=True, limit=25, after=after)
        rows.extend(page)
        if len(page) < 25:
            break
        after = (
            str(page[-1]["competition_id"]),
            str(page[-1]["season_id"]),
        )
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row["competition_id"]), []).append(row)
    duplicates = sorted(key for key, value in grouped.items() if len(value) != 1)
    if duplicates:
        raise RemediationEvidenceError(
            "current-season cardinality is not singular: " + ",".join(duplicates)
        )
    return {key: value[0] for key, value in grouped.items()}


def build_remediation_plans(
    control,
    raw_store: RawPageStore,
    *,
    competition_ids: Sequence[object],
) -> list[CurrentSeasonRemediationPlan]:
    scope = _normalized_scope(competition_ids)
    competitions = {
        str(row["competition_id"]): row for row in control.eligible_competitions()
    }
    missing = sorted(set(scope) - set(competitions))
    if missing:
        raise RemediationEvidenceError(
            "competition is not active eligible male scope: " + ",".join(missing)
        )
    currents = _current_seasons(control)
    missing_current = sorted(set(scope) - set(currents))
    if missing_current:
        raise RemediationEvidenceError(
            "requested competition has no installed current season: "
            + ",".join(missing_current)
        )
    plans = []
    for competition_id in scope:
        row = competitions[competition_id]
        try:
            index = control.get_current_season_index_evidence(competition_id)
        except Exception as exc:
            raise RemediationEvidenceError(
                f"competition {competition_id} index evidence is invalid"
            ) from exc
        competition = _competition_from_registry(row)
        advertised = advertised_current_season(competition)
        if advertised is None:
            raise RemediationEvidenceError(
                f"competition {competition_id} has no saved maxseason label+href"
            )
        target = competition_page_target(
            competition_id,
            str(row["canonical_url"]),
        )
        raw, record = raw_store.load_latest_response(target)
        history_manifest_key = raw_store.fetch_manifest_key(record.logical_refresh_id)
        try:
            history_attempt = control.get_succeeded_fetch_evidence(
                record.logical_refresh_id,
                target_id=record.target_id,
                content_hash=record.content_hash,
                raw_manifest_key=history_manifest_key,
            )
        except Exception as exc:
            raise RemediationEvidenceError(
                f"competition {competition_id} history evidence is invalid"
            ) from exc
        expected_history = {
            "logical_refresh_id": record.logical_refresh_id,
            "target_id": record.target_id,
            "content_hash": record.content_hash,
            "raw_manifest_key": history_manifest_key,
        }
        if (
            any(
                str(history_attempt.get(key) or "") != expected
                for key, expected in expected_history.items()
            )
            or not history_attempt.get("attempt_id")
            or not history_attempt.get("run_id")
        ):
            raise RemediationEvidenceError(
                f"competition {competition_id} history evidence is not exact"
            )
        try:
            history_html = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RemediationEvidenceError(
                f"competition {competition_id} history raw is not UTF-8"
            ) from exc
        parsed = parse_competition_html(history_html, competition)
        if parsed.has_errors:
            raise RemediationEvidenceError(
                f"competition {competition_id} history raw fails discovery"
            )
        seasons, current_season_id = _resolve_current_season_install(
            competition,
            parsed.datasets["seasons"].records,
            parsed.datasets["matches"].records,
        )
        if current_season_id != advertised.season_id:
            raise RemediationEvidenceError(
                f"competition {competition_id} resolved current season differs "
                "from maxseason evidence"
            )
        installed = currents.get(competition_id)
        installed_id = None if installed is None else str(installed["season_id"])
        if not any(
            season.season_id == current_season_id for season in seasons
        ) and not any(
            match.season_id == current_season_id
            for match in parsed.datasets["matches"].records
        ):
            raise RemediationEvidenceError(
                f"competition {competition_id} current season was not installed"
            )
        index_raw = dict(index["raw"])
        plan = CurrentSeasonRemediationPlan(
            competition_id=competition_id,
            installed_current_season_id=installed_id,
            advertised_current_season_id=current_season_id,
            advertised_current_label=advertised.label,
            advertised_current_url=advertised.season_url,
            action=("no_change" if installed_id == current_season_id else "reconcile"),
            history_attempt_id=str(history_attempt["attempt_id"]),
            history_run_id=str(history_attempt["run_id"]),
            history_content_hash=record.content_hash,
            history_logical_refresh_id=record.logical_refresh_id,
            history_raw_manifest_key=history_manifest_key,
            index_snapshot_id=str(index["snapshot_id"]),
            index_run_id=str(index["run_id"]),
            index_attempt_id=str(index_raw["attempt_id"]),
            index_target_id=str(index_raw["target_id"]),
            index_logical_refresh_id=str(index_raw["logical_refresh_id"]),
            index_content_hash=str(index["content_hash"]),
            index_raw_manifest_key=str(index_raw["manifest_key"]),
            install_contract_version=CURRENT_SEASON_INSTALL_CONTRACT_VERSION,
            _history_html=history_html,
            _history_record=record,
        )
        try:
            FBrefPipeline(control, raw_store).validate_current_season_remediation_item(
                plan.remediation_item()
            )
        except Exception as exc:
            raise RemediationEvidenceError(
                f"competition {competition_id} immutable raw evidence is invalid"
            ) from exc
        plans.append(plan)
    return plans


def run_remediation(
    control,
    raw_store: RawPageStore,
    *,
    competition_ids: Sequence[object],
    apply: bool,
    source_run_id: Optional[str] = None,
    pipeline_factory=FBrefPipeline,
) -> dict:
    scope = _normalized_scope(competition_ids)
    if apply and not str(source_run_id or "").strip():
        raise RemediationConfigurationError("source_run_id is required with --apply")
    plans = build_remediation_plans(
        control,
        raw_store,
        competition_ids=scope,
    )
    if apply:
        reconcile_plans = [plan for plan in plans if plan.action == "reconcile"]
        mismatched_runs = [
            plan.competition_id
            for plan in reconcile_plans
            if plan.history_run_id != str(source_run_id)
        ]
        if mismatched_runs:
            raise RemediationEvidenceError(
                "source_run_id does not own exact history evidence: "
                + ",".join(mismatched_runs)
            )
        if reconcile_plans:
            pipeline_factory(control, raw_store).remediate_current_seasons(
                [plan.remediation_item() for plan in reconcile_plans]
            )
    return {
        "schema_version": "fbref-current-season-remediation-v1",
        "mode": "apply" if apply else "dry_run",
        "competition_count": len(plans),
        "reconcile_count": sum(plan.action == "reconcile" for plan in plans),
        "plans": [plan.evidence() for plan in plans],
    }


def _arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--competition-id",
        action="append",
        required=True,
        help="explicit competition ID; repeat for each bounded target",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply the proposed registry/frontier reconcile",
    )
    parser.add_argument(
        "--source-run-id",
        help="control run owning the latest immutable history raw",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _arguments(argv)
    result = run_remediation(
        ControlStore.from_env(),
        RawPageStore.from_env(optional=False),
        competition_ids=args.competition_id,
        apply=args.apply,
        source_run_id=args.source_run_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
