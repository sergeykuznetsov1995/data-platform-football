"""Mark four registry-disabled replay scopes as not_supported from raw evidence."""
from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime, timezone

from dags.scripts.replay_sofascore_raw import _replay_manager
from scrapers.sofascore.manifest import ManifestStatus, utc_now_iso
from scrapers.sofascore.pipeline import build_capture_runtime
from scrapers.sofascore.raw_replay import MODE_DRAIN, load_replay_scope_records
from scrapers.sofascore.raw_store import PayloadTarget
from scrapers.sofascore.scraper import SofaScoreCatalog

DISABLED_SCOPES = (
    ("10783", "16000"),
    ("7", "11773"),
    ("270", "1138"),
    ("7", "36886"),
)


class DisabledScopeError(RuntimeError):
    pass


def build_disabled_disposition(
    records_by_scope, raw_loader, capture_allowed_by_scope, *, run_id,
    scopes=DISABLED_SCOPES,
):
    plan = []
    for scope in scopes:
        if capture_allowed_by_scope[scope]:
            raise DisabledScopeError(f"registry allows capture for {scope}")
        records = tuple(records_by_scope[scope])
        if not records:
            plan.append((scope, ()))
            continue
        event_records = [
            record for record in records if record.key.endpoint == "event"
        ]
        if not event_records:
            raise DisabledScopeError(f"no event evidence for {scope}")
        verified_event_ids = set()
        for record in records:
            raw = raw_loader(record.key)
            if record.key.endpoint != "event":
                continue
            payload = json.loads(raw)
            event = payload.get("event", payload)
            season = event.get("season") or {}
            tournament = event.get("tournament") or {}
            unique_tournament = tournament.get("uniqueTournament") or {}
            if str(season.get("id")) != scope[1]:
                raise DisabledScopeError(
                    f"unexpected payload season for {record.key.stable_id()}"
                )
            if str(unique_tournament.get("id")) != scope[0]:
                raise DisabledScopeError(
                    f"unexpected payload tournament for {record.key.stable_id()}"
                )
            if str(event.get("id")) != record.key.target_id:
                raise DisabledScopeError(
                    f"payload event id mismatch for {record.key.stable_id()}"
                )
            verified_event_ids.add(record.key.target_id)
        if any(
            record.key.target_id not in verified_event_ids for record in records
        ):
            raise DisabledScopeError(f"unlinked record for {scope}")
        moved = []
        for record in records:
            moved.append(
                replace(
                    record,
                    status=ManifestStatus.NOT_SUPPORTED,
                    run_id=run_id,
                    task_id="disabled_scope_disposition",
                    attempts=record.attempts + 1,
                    error_type="ReplayScopeNotSupported",
                    error_message=(
                        "registry capture_allowed=false; raw scope identity verified"
                    ),
                    parsed_at=utc_now_iso(),
                    updated_at=utc_now_iso(),
                )
            )
        plan.append((scope, tuple(moved)))
    return tuple(plan)


def apply_disabled_disposition(store, plan):
    for _scope, records in plan:
        for record in records:
            store.upsert(record)
    flush = getattr(store, "flush", None)
    if callable(flush):
        flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    run_id = "disabled-scopes-" + datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    runtime = build_capture_runtime(run_id=run_id, task_id="disposition")
    manager = _replay_manager(getattr(runtime, "manifest_store", None))
    catalog = getattr(runtime, "catalog", None) or SofaScoreCatalog.load()
    capture_allowed_by_scope = {
        scope: bool(catalog.tournament(int(scope[0])).capture_allowed)
        for scope in DISABLED_SCOPES
    }
    records_by_scope = {
        scope: load_replay_scope_records(
            manager, (*scope, "final"), mode=MODE_DRAIN
        )
        for scope in DISABLED_SCOPES
    }
    plan = build_disabled_disposition(
        records_by_scope,
        lambda key: runtime.raw_store.load_bytes(PayloadTarget(*key.as_tuple()))[0],
        capture_allowed_by_scope,
        run_id=run_id,
    )
    if args.apply:
        apply_disabled_disposition(runtime.manifest_store, plan)
    report = []
    for scope, records in plan:
        remaining = load_replay_scope_records(
            manager, (*scope, "final"), mode=MODE_DRAIN
        )
        report.append(
            {
                "scope": scope,
                "not_supported_records": len(records),
                "remaining_retryable_failure": len(remaining),
                "status": "not_supported" if args.apply else "validated",
            }
        )
    print(json.dumps({"run_id": run_id, "applied": args.apply, "scopes": report}))


if __name__ == "__main__":
    main()
