"""Close five proven moved-source-season manifest scopes without new capture."""
from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime, timezone

from dags.scripts.replay_sofascore_raw import _replay_manager
from scrapers.sofascore.manifest import ManifestStatus, utc_now_iso
from scrapers.sofascore.pipeline import build_capture_runtime
from scrapers.sofascore.raw_replay import (
    MODE_DRAIN,
    MODE_FIELDS,
    load_replay_scope_records,
)
from scrapers.sofascore.raw_store import PayloadTarget

MOVED_SCOPES = {
    ("11058", "74464"): ("11058", "84686"),
    ("1020", "69473"): ("1020", "71095"),
    ("1682", "71030"): ("1682", "73770"),
    ("3080", "66969"): ("3080", "67604"),
    ("13614", "95576"): ("13614", "98037"),
}


class DispositionError(RuntimeError):
    pass


def build_disposition(records_by_scope, raw_loader, *, run_id, scopes=MOVED_SCOPES):
    plan = []
    for old_scope, new_scope in scopes.items():
        old_records = tuple(records_by_scope[old_scope])
        correct_records = tuple(records_by_scope[new_scope])
        if not old_records:
            plan.append((old_scope, new_scope, ()))
            continue
        if not any(
            record.status == ManifestStatus.SUCCESS for record in correct_records
        ):
            raise DispositionError(f"no success evidence for correct scope {new_scope}")
        if any(
            record.status == ManifestStatus.RETRYABLE_FAILURE
            for record in correct_records
        ):
            raise DispositionError(f"correct scope {new_scope} is not settled")
        event_records = [
            record for record in old_records if record.key.endpoint == "event"
        ]
        if not event_records:
            raise DispositionError(f"no event evidence for {old_scope}")
        verified_event_ids = set()
        for record in old_records:
            raw = raw_loader(record.key)
            if record.key.endpoint != "event":
                continue
            payload = json.loads(raw)
            event = payload.get("event", payload)
            season = event.get("season") or {}
            tournament = event.get("tournament") or {}
            unique_tournament = tournament.get("uniqueTournament") or {}
            if str(season.get("id")) != new_scope[1]:
                raise DispositionError(
                    f"unexpected payload season for {record.key.stable_id()}: {season}"
                )
            if str(unique_tournament.get("id")) != new_scope[0]:
                raise DispositionError(
                    f"unexpected payload tournament for {record.key.stable_id()}"
                )
            if str(event.get("id")) != record.key.target_id:
                raise DispositionError(
                    f"payload event id mismatch for {record.key.stable_id()}"
                )
            verified_event_ids.add(record.key.target_id)
        unlinked = [
            record.key.stable_id()
            for record in old_records
            if record.key.target_id not in verified_event_ids
        ]
        if unlinked:
            raise DispositionError(
                f"records without a verified event: {unlinked[:3]}"
            )
        moved_records = []
        for record in old_records:
            message = (
                f"payload moved to {new_scope[0]}/{new_scope[1]}; "
                "correct scope has success evidence; exact raw hash verified"
            )
            moved_records.append(
                replace(
                    record,
                    status=ManifestStatus.SCHEMA_ERROR,
                    run_id=run_id,
                    task_id="moved_season_disposition",
                    attempts=record.attempts + 1,
                    error_type="MovedSourceSeason",
                    error_message=message,
                    parsed_at=utc_now_iso(),
                    updated_at=utc_now_iso(),
                )
            )
        plan.append((old_scope, new_scope, tuple(moved_records)))
    return tuple(plan)


def apply_disposition(store, plan):
    for _old_scope, _new_scope, records in plan:
        for record in records:
            store.upsert(record)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    run_id = "moved-season-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    runtime = build_capture_runtime(run_id=run_id, task_id="disposition")
    manager = _replay_manager(getattr(runtime, "manifest_store", None))
    records_by_scope = {}
    for old_scope, new_scope in MOVED_SCOPES.items():
        records_by_scope[old_scope] = load_replay_scope_records(
            manager, (*old_scope, "final"), mode=MODE_DRAIN
        )
        records_by_scope[new_scope] = load_replay_scope_records(
            manager, (*new_scope, "final"), mode=MODE_FIELDS
        )
    plan = build_disposition(
        records_by_scope,
        lambda key: runtime.raw_store.load_bytes(PayloadTarget(*key.as_tuple()))[0],
        run_id=run_id,
    )
    if args.apply:
        apply_disposition(runtime.manifest_store, plan)
    report = []
    for old_scope, new_scope, records in plan:
        remaining = load_replay_scope_records(
            manager, (*old_scope, "final"), mode=MODE_DRAIN
        )
        report.append(
            {
                "old_scope": old_scope,
                "correct_scope": new_scope,
                "moved_records": len(records),
                "remaining_retryable_failure": len(remaining),
                "status": "schema_error" if args.apply else "validated",
            }
        )
    print(json.dumps({"run_id": run_id, "applied": args.apply, "scopes": report}))


if __name__ == "__main__":
    main()
