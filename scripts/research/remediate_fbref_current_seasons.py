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
from typing import Callable, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.control import ControlStore  # noqa: E402
from scrapers.fbref.discovery import (  # noqa: E402
    advertised_current_season,
    parse_competition_html,
)
from scrapers.fbref.pipeline import (  # noqa: E402
    CURRENT_SEASON_INSTALL_CONTRACT_VERSION,
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
    history_content_hash: str
    history_logical_refresh_id: str
    competition_snapshot_id: Optional[str]
    install_contract_version: str
    _history_html: str = field(repr=False)
    _history_record: RawFetchRecord = field(repr=False)

    def evidence(self) -> dict:
        return {
            "competition_id": self.competition_id,
            "installed_current_season_id": self.installed_current_season_id,
            "advertised_current_season_id": (self.advertised_current_season_id),
            "advertised_current_label": self.advertised_current_label,
            "advertised_current_url": self.advertised_current_url,
            "action": self.action,
            "history_content_hash": self.history_content_hash,
            "history_logical_refresh_id": self.history_logical_refresh_id,
            "competition_snapshot_id": self.competition_snapshot_id,
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
        plans.append(
            CurrentSeasonRemediationPlan(
                competition_id=competition_id,
                installed_current_season_id=installed_id,
                advertised_current_season_id=current_season_id,
                advertised_current_label=advertised.label,
                advertised_current_url=advertised.season_url,
                action=(
                    "no_change" if installed_id == current_season_id else "reconcile"
                ),
                history_content_hash=record.content_hash,
                history_logical_refresh_id=record.logical_refresh_id,
                competition_snapshot_id=(
                    None
                    if row.get("last_snapshot_id") is None
                    else str(row["last_snapshot_id"])
                ),
                install_contract_version=CURRENT_SEASON_INSTALL_CONTRACT_VERSION,
                _history_html=history_html,
                _history_record=record,
            )
        )
    return plans


def _verify_source_run_lineage(
    control,
    plan: CurrentSeasonRemediationPlan,
    source_run_id: str,
) -> None:
    attempts = control.list_fetch_attempts_for_refresh(
        source_run_id,
        plan.history_logical_refresh_id,
    )
    matching = [
        row
        for row in attempts
        if str(row.get("status")) == "succeeded"
        and str(row.get("target_id")) == plan._history_record.target_id
        and str(row.get("content_hash")) == plan.history_content_hash
        and row.get("raw_manifest_key")
    ]
    if len(matching) != 1:
        raise RemediationEvidenceError(
            f"competition {plan.competition_id} source-run raw lineage is not singular"
        )


def _install_one(
    control,
    raw_store: RawPageStore,
    plan: CurrentSeasonRemediationPlan,
    source_run_id: str,
) -> None:
    FBrefPipeline(control, raw_store)._parse_competition(
        source_run_id,
        plan._history_html,
        plan._history_record,
        run_type="current",
    )
    installed = _current_seasons(control).get(plan.competition_id)
    if installed is None or str(installed["season_id"]) != (
        plan.advertised_current_season_id
    ):
        raise RemediationEvidenceError(
            f"competition {plan.competition_id} post-apply current mismatch"
        )


def _apply_one(
    control,
    raw_store: RawPageStore,
    plan: CurrentSeasonRemediationPlan,
    source_run_id: str,
) -> None:
    _verify_source_run_lineage(control, plan, source_run_id)
    _install_one(control, raw_store, plan, source_run_id)


def run_remediation(
    control,
    raw_store: RawPageStore,
    *,
    competition_ids: Sequence[object],
    apply: bool,
    source_run_id: Optional[str] = None,
    apply_one: Optional[
        Callable[[object, RawPageStore, CurrentSeasonRemediationPlan, str], None]
    ] = None,
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
        if apply_one is None:
            # Prove the complete requested batch before the first registry
            # write, so a later source-run mismatch cannot leave a partial
            # remediation behind.
            for plan in reconcile_plans:
                _verify_source_run_lineage(control, plan, str(source_run_id))
            for plan in reconcile_plans:
                _install_one(control, raw_store, plan, str(source_run_id))
        else:
            for plan in reconcile_plans:
                apply_one(control, raw_store, plan, str(source_run_id))
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
