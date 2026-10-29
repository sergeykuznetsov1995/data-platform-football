"""Bounded repair without source HTTP of schedule discovery from immutable raw.

This is an explicit maintenance mode, independent of historical root seeding.
It neither marks raw as parsed nor claims that queued schedules reached Bronze.
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict

from scrapers.fbref.discovery import (
    DiscoveredPageLink,
    SeasonRef,
    CalendarType,
    parse_season_html,
)
from scrapers.fbref.raw_store import RawStoreError, season_page_target

logger = logging.getLogger(__name__)

# A permanent redirect cannot be followed without a reviewed registry (#1933);
# requeueing the same URL would only loop.
_REDIRECT_STATUSES = frozenset({301, 308})


def recover_all_schedules(pipeline) -> dict:
    """Offline backfill preflight: drain the denominator and account for quarantine."""
    from scrapers.fbref.control.models import FrontierTarget

    inventory = []
    after_target_id = None
    while True:
        rows = pipeline.control.list_schedule_recovery_quarantine(
            after_target_id=after_target_id, limit=25,
        )
        inventory.extend(rows)
        if len(rows) < 25:
            break
        after_target_id = rows[-1]["target_id"]
    for row in inventory:
        # The reconciler alone can clear a resolved scope verdict.
        if (
            row["last_error_class"] == "ScopeQuarantined"
            and row["historical_eligible"]
            and row.get("last_http_status") not in _REDIRECT_STATUSES
        ):
            pipeline.control.upsert_frontier_target(FrontierTarget(
                target_id=row["target_id"], page_kind=row["page_kind"],
                canonical_url=row["canonical_url"], source_ids=row["source_ids"],
                refresh_policy="historical_once", priority=row["priority"],
            ))
    pipeline._reconcile_frontier_scope()
    quarantines = []
    for row in inventory:
        installed = pipeline.control.get_frontier_target(row["target_id"])
        quarantine = {
            "target_id": row["target_id"], "state": installed["state"],
            "reason": installed.get("last_error_message"),
            "error_class": installed.get("last_error_class"),
        }
        if row.get("last_http_status") in _REDIRECT_STATUSES:
            quarantine["detail"] = quarantine["reason"]
            quarantine["reason"] = "redirect_unresolved"
            logger.warning(
                "FBref schedule recovery: quarantined %s answered HTTP %s; "
                "not requeued until a reviewed redirect exists",
                row["target_id"], row["last_http_status"],
            )
        quarantines.append(quarantine)
    results = []
    qualifications = None
    after = None
    while True:
        page = recover_schedules(pipeline, limit=25, after=after)
        results.extend(page["results"])
        if after is None:
            qualifications = page["qualifications"]
        after = page["next_after"]
        if after is None:
            break
    return {"results": results, "quarantines": quarantines,
            "qualifications": qualifications}


def recover_qualification_index(pipeline) -> dict:
    """Register advertised adult qualifiers and seed history with raw provenance."""
    from scrapers.fbref.daily_report import adult_competition
    from scrapers.fbref.discovery import parse_competition_index_html
    from scrapers.fbref.pipeline import _registry_entry
    from scrapers.fbref.raw_store import competition_index_target

    loader = getattr(pipeline.control, "latest_schedule_recovery_index", None)
    evidence = None if loader is None else loader()
    if not evidence:
        return {"reason": "index_raw_missing"}
    refresh = evidence["logical_refresh_id"]
    try:
        html, record = pipeline.raw_store.load_fetch_html(refresh)
    except RawStoreError as exc:
        return {"reason": type(exc).__name__}
    expected = competition_index_target()
    if (
        record.source != "fbref"
        or record.page_kind != expected.page_kind
        or record.target_id != expected.target_id
        or record.canonical_url != expected.canonical_url
        or dict(record.source_ids) != dict(expected.source_ids)
        or record.content_hash != evidence["content_hash"]
        or str(record.logical_refresh_id) != str(refresh)
    ):
        return {"reason": "index_raw_identity_mismatch"}
    fetch = pipeline.control.get_succeeded_fetch_evidence(
        refresh, target_id=record.target_id, content_hash=record.content_hash,
        raw_manifest_key=pipeline.raw_store.fetch_manifest_key(refresh),
    )
    parsed = parse_competition_index_html(html)
    if parsed.has_errors:
        return {"reason": "index_parse_failed"}
    qualifiers = set()
    for item in parsed.datasets["competitions"].records:
        entry = _registry_entry(item, current_season_index={})
        if (
            adult_competition(asdict(entry))
            and re.search(r"champions|europa|conference", item.name, re.I)
            and re.search(r"qualif", item.name, re.I)
        ):
            qualifiers.add(item.competition_id)
    if not qualifiers:
        return {"reason": "qualification_links_missing"}
    with pipeline.control.guard_latest_content(
        record.target_id, record.content_hash, record.logical_refresh_id,
    ) as latest:
        if not latest:
            return {"reason": "index_raw_not_latest_or_leased"}
        # Reconcile the complete authoritative index, just as ordinary replay
        # does; a partial index must never mark unrelated competitions missing.
        seeded, skipped = pipeline._parse_competition_index(
            fetch["run_id"], html, record, seed_competition_ids=qualifiers,
        )
    return {"reason": "qualification_histories_seeded", "seeded": seeded,
            "skipped": skipped, "competition_ids": sorted(qualifiers)}


def recover_schedules(pipeline, *, limit: int = 25, after=None) -> dict:
    """Inspect one newest-first denominator page and repair proven links only.

    Continue with ``next_after`` even if every row lacks raw or stays quarantined:
    a broken recent edition must not starve the rest of the denominator.
    """
    if not 1 <= int(limit) <= 25:
        raise ValueError("limit must be between 1 and 25")
    qualifications = recover_qualification_index(pipeline) if after is None else None
    rows = pipeline.control.list_schedule_recovery_seasons(
        limit=int(limit), after=after
    )
    results = []
    for row in rows:
        result = {
            "competition_id": str(row["competition_id"]),
            "season_id": str(row["season_id"]),
        }
        results.append(result)
        if (row.get("metadata") or {}).get("direct_match_only"):
            result["reason"] = "direct_match_only"
            continue
        if row.get("root_state") == "quarantined":
            result["quarantine_reason"] = row.get("root_error_message")
            if row.get("root_error_class") != "ScopeQuarantined":
                result["reason"] = "season_quarantined"
                continue
        if row.get("schedule_target_id"):
            result["reason"] = (
                "schedule_processed"
                if row.get("schedule_processed")
                else "schedule_" + str(row["schedule_state"])
            )
            result["detail"] = row.get("schedule_error_message")
            result["quarantine_reason"] = row.get("schedule_error_message")
            if (
                not row.get("schedule_processed")
                and row.get("schedule_http_status") in _REDIRECT_STATUSES
            ):
                _redirect_unresolved(result, row["schedule_http_status"])
                continue
            if row["schedule_state"] != "quarantined":
                continue
            if row.get("schedule_error_class") != "ScopeQuarantined":
                continue
        refresh = row.get("logical_refresh_id")
        if not refresh:
            result["reason"] = "raw_missing"
            if row.get("root_http_status") in _REDIRECT_STATUSES:
                _redirect_unresolved(result, row["root_http_status"])
                continue
            # A never-fetched edition still needs a real season fetch. Queue
            # only its registry URL; no schedule URL can be invented from it.
            if row.get("root_state") in {None, "queued", "retry"}:
                from scrapers.fbref.pipeline import frontier_target, page_target_from_link

                target = page_target_from_link(DiscoveredPageLink(
                    page_kind="season", canonical_url=row["canonical_url"],
                    source_ids={"competition_id": result["competition_id"],
                                "season_id": result["season_id"]},
                ))
                pipeline.control.upsert_frontier_target(
                    frontier_target(target, historical=not row["is_current"])
                )
                pipeline._reconcile_frontier_scope()
                installed = pipeline.control.get_frontier_target(target.target_id)
                result["reason"] = "season_" + installed["state"]
                result["detail"] = installed.get("last_error_message")
            continue
        try:
            html, record = pipeline.raw_store.load_fetch_html(refresh)
        except RawStoreError as exc:
            result["reason"] = type(exc).__name__
            continue
        expected = season_page_target(
            result["competition_id"], result["season_id"], row["canonical_url"]
        )
        if (
            record.page_kind != "season"
            or record.target_id != expected.target_id
            or dict(record.source_ids) != dict(expected.source_ids)
            or record.content_hash != row["content_hash"]
            or str(record.logical_refresh_id) != str(refresh)
        ):
            result["reason"] = "raw_identity_mismatch"
            continue
        pipeline.control.get_succeeded_fetch_evidence(
            refresh,
            target_id=record.target_id,
            content_hash=record.content_hash,
            raw_manifest_key=pipeline.raw_store.fetch_manifest_key(refresh),
        )
        parsed = parse_season_html(
            html,
            SeasonRef(
                comp_id=result["competition_id"],
                season_id=result["season_id"],
                label=str(row.get("label") or result["season_id"]),
                calendar_type=CalendarType.OPAQUE,
                season_url=record.canonical_url,
            ),
            historical=not row["is_current"],
        )
        dataset = parsed.datasets["schedules"]
        if parsed.has_errors or not dataset.records:
            result["reason"] = dataset.reason
            continue
        links = [
            DiscoveredPageLink(
                page_kind="schedule",
                canonical_url=schedule.schedule_url,
                source_ids=dict(expected.source_ids),
            )
            for schedule in dataset.records
        ]
        # Reuse the atomic target/provenance writer and the existing fail-closed
        # scope reconciliation. Upsert alone must never reopen parser quarantine.
        from scrapers.fbref.pipeline import (
            _FrontierSeedCandidate,
            page_target_from_link,
        )

        with pipeline.control.guard_latest_content(
            record.target_id,
            record.content_hash,
            record.logical_refresh_id,
        ) as latest:
            if not latest:
                result["reason"] = "raw_not_latest_or_leased"
                continue
            pipeline._seed_link_candidates(
                [
                    _FrontierSeedCandidate(link=link, historical=not row["is_current"])
                    for link in links
                ],
                parent_record=record,
                eligible_competitions=(
                    {
                        str(item["competition_id"]): item
                        for item in pipeline.control.eligible_historical_competitions()
                    }
                    if not row["is_current"]
                    and hasattr(pipeline.control, "eligible_historical_competitions")
                    else None
                ),
                provenance_metadata={"schedule_recovery": "v1"},
                provenance_install_id="schedule-recovery-v1",
                reconcile_after=False,
            )
        pipeline._reconcile_frontier_scope()
        installed = pipeline.control.get_frontier_target(
            page_target_from_link(links[0]).target_id
        )
        result["reason"] = "schedule_" + str(installed["state"])
        result["detail"] = installed.get("last_error_message")
    return {
        "results": results,
        "qualifications": qualifications,
        "next_after": (
            rows[-1]["sort_year"],
            str(rows[-1]["competition_id"]),
            str(rows[-1]["season_id"]),
        )
        if len(rows) == int(limit)
        else None,
    }


def _redirect_unresolved(result: dict, http_status) -> None:
    result["reason"] = "redirect_unresolved"
    result["detail"] = f"HTTP {http_status}"
    logger.warning(
        "FBref schedule recovery: %s %s answered HTTP %s; not requeued "
        "until a reviewed redirect exists",
        result["competition_id"], result["season_id"], http_status,
    )
