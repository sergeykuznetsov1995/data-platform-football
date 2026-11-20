"""Bounded raw-first production capture for Transfermarkt achievements."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any

from dags.utils.transfermarkt_achievement_dq import run_achievement_dq
from dags.utils.transfermarkt_achievements import (
    DEFAULT_POLICY_PATH,
    validate_achievement_write_policy,
    write_achievement_observations,
)
from dags.utils.transfermarkt_approval import load_standing_policy
from scrapers.transfermarkt.achievement_observations import (
    AchievementRecipient,
    AchievementUniverse,
    achievement_url,
    extract_achievement_capture,
)
from scrapers.transfermarkt.achievement_state import AchievementState
from scrapers.transfermarkt.models import FetchStatus, SharedTrafficLedger

DEFAULT_STATE = "/opt/airflow/logs/transfermarkt-achievements/state.sqlite3"
DEFAULT_REPORT = "/opt/airflow/logs/transfermarkt-achievements/last-report.json"
MONTHLY_SECONDS = 30 * 86400


def load_achievement_universe(connection, *, universe_version: str) -> AchievementUniverse:
    query = """
SELECT 'player', player_id, NULL FROM iceberg.bronze.transfermarkt_player_attribute_observations GROUP BY player_id
UNION ALL
SELECT 'club', club_id, NULL FROM iceberg.bronze.transfermarkt_squad_memberships GROUP BY club_id
UNION ALL
SELECT 'coach', coach_id, coach_slug FROM iceberg.bronze.transfermarkt_coach_profiles GROUP BY coach_id, coach_slug
"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = list(cursor.fetchall())
    finally:
        cursor.close()
    recipients = tuple(AchievementRecipient(kind, identifier, slug) for kind, identifier, slug in rows)
    return AchievementUniverse(universe_version, recipients)


def post_season_proof(connection, *, today: datetime) -> str | None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT scope_id, registry_snapshot_id, completed_at FROM iceberg.ops.transfermarkt_season_close_v1 "
            "WHERE status='complete' ORDER BY completed_at DESC LIMIT 1"
        )
        close = cursor.fetchone()
        if close:
            return json.dumps({
                "kind": "season_close", "scope_id": close[0],
                "registry_snapshot_id": close[1], "completed_at": str(close[2]),
            }, sort_keys=True, separators=(",", ":"))
        cursor.execute(
            "SELECT competition_id, edition_id, registry_snapshot_id, end_date "
            "FROM iceberg.bronze.transfermarkt_competition_editions e "
            "WHERE current=false AND end_date IS NOT NULL AND end_date <= ? "
            "AND EXISTS (SELECT 1 FROM iceberg.bronze.transfermarkt_competition_editions s "
            "WHERE s.competition_id=e.competition_id AND s.current=true) "
            "ORDER BY end_date DESC, competition_id, edition_id LIMIT 1",
            [today.date().isoformat()],
        )
        edition = cursor.fetchone()
    finally:
        cursor.close()
    if not edition:
        return None
    return json.dumps({
        "kind": "completed_edition", "competition_id": edition[0], "edition_id": edition[1],
        "registry_snapshot_id": edition[2], "end_date": str(edition[3]),
    }, sort_keys=True, separators=(",", ":"))


def _atomic_json(path: str | Path, value: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    with temporary.open("w") as output:
        output.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, target)


def _active_universe(state: AchievementState) -> AchievementUniverse | None:
    active = state.active_assessment()
    return state.universe(active["manifest_id"]) if active else None


def _dq(scraper, universe: AchievementUniverse, state: AchievementState, now: datetime):
    connection = scraper._bronze_connection()
    try:
        return run_achievement_dq(connection, universe, state, evaluated_at=now)
    finally:
        connection.close()


def run_achievement_portion(
    scraper,
    state: AchievementState,
    universe: AchievementUniverse,
    *,
    run_id: str,
    now: datetime,
    max_recipients: int,
    post_season_proof: str | None = None,
    deadline_monotonic: float | None = None,
    writer=write_achievement_observations,
    preflight: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("portion clock must be timezone-aware")
    now = now.astimezone(timezone.utc)
    active = _active_universe(state)
    if active is not None:
        universe = active
        assessment = state.assessment(active.assessment_manifest_id)
        post_season_proof = assessment["post_season_proof"]
    else:
        latest = state.connection.execute(
            "SELECT manifest_id, created_at, post_season_proof FROM assessments ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        monthly_due = latest is None or (
            now - datetime.fromisoformat(latest[1]).astimezone(timezone.utc)
        ).total_seconds() >= MONTHLY_SECONDS
        post_season_due = (
            latest is not None and post_season_proof is not None
            and post_season_proof != latest[2]
        )
        if not monthly_due and not post_season_due:
            return {
                "status": "no_op", "assessment_manifest_id": latest[0],
                "processed": 0, "dq": _dq(scraper, state.universe(latest[0]), state, now),
            }
        state.start_assessment(universe, created_at=now, post_season_proof=post_season_proof)
    manifest = universe.assessment_manifest_id
    recipients = state.due_recipients(manifest, limit=max_recipients)
    processed = 0
    errors = 0
    for recipient in recipients:
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            break
        try:
            url = achievement_url(**recipient.inventory_entry())
            outcome = scraper._http_client.fetch(
                url, as_json=False, label=f"{recipient.recipient_type}_achievement",
                context={"scope_id": f"achievement:{recipient.recipient_type}:{recipient.recipient_id}"},
            )
            if outcome.status is not FetchStatus.OK or outcome.value is None or not outcome.raw_capture_id:
                raise ValueError("achievement source has no conclusive raw response")
            body = outcome.value.encode("utf-8") if isinstance(outcome.value, str) else outcome.value
            if not isinstance(body, bytes):
                raise ValueError("achievement source body is not bytes or text")
            raw_store = scraper._http_client._raw_store
            if raw_store is None:
                raise ValueError("achievement raw store is required")
            verified_body, raw = raw_store.load_capture(outcome.raw_capture_id)
            if verified_body != body:
                raise ValueError("achievement raw body differs from parser input")
            extraction = extract_achievement_capture(
                body, raw, recipient=recipient, universe=universe,
                run_id=run_id, extracted_at=now,
            )
        except Exception as exc:
            reason = getattr(exc, "reason", type(exc).__name__)
            state.mark_error(manifest, recipient, str(reason), now)
            errors += 1
            continue
        receipt = writer(
            scraper, extraction, preflight or {},
            committed_at=datetime.now(timezone.utc),
        )
        state.mark_success(manifest, recipient, extraction, receipt, now)
        processed += 1
    dq = _dq(scraper, universe, state, now)
    return {
        "status": "complete" if state.assessment(manifest)["completed_at"] else "in_progress",
        "assessment_manifest_id": manifest, "universe_version": universe.universe_version,
        "post_season_proof": post_season_proof,
        "selected": len(recipients), "processed": processed, "errors": errors,
        "remaining": len(state.due_recipients(manifest, limit=max_recipients + 1)),
        "dq": dq,
    }


def _reader_preflight() -> dict[str, Any]:
    from dags.utils import transfermarkt_native_v2 as native

    connection = native.connect()
    cursor = connection.cursor()
    try:
        state = native.read_reader_state(cursor, allow_missing=True)
        if not state.exists:
            raise ValueError("Transfermarkt reader state is required")
        return {
            "write_mode": "native-only" if state.legacy_writers_disabled_at is not None else "dual",
            "revision": state.revision,
        }
    finally:
        cursor.close()
        connection.close()


def _require_metered_proxy_environment(environment: Any) -> None:
    if str(environment.get("TM_REQUIRE_METERED_PROXY", "")).strip().lower() not in {
        "1", "true", "yes", "on",
    }:
        raise ValueError("achievements require TM_REQUIRE_METERED_PROXY=true")
    if not str(environment.get("TM_PROXY_CONTROL_URL", "")).strip():
        raise ValueError("achievements require a metered proxy control URL")


def _require_raw_store(scraper) -> None:
    if scraper._http_client._raw_store is None:
        raise ValueError("achievements require configured immutable raw storage")


def main() -> None:
    from dags.utils import transfermarkt_native_v2 as native
    from scrapers.transfermarkt.scraper import TransfermarktScraper

    _require_metered_proxy_environment(os.environ)
    now = datetime.now(timezone.utc)
    run_id = os.environ.get("TM_RUN_ID") or f"tm-achievements-{now.isoformat()}"
    policy = load_standing_policy(os.environ.get("TM_ACHIEVEMENTS_POLICY_PATH", DEFAULT_POLICY_PATH))
    validate_achievement_write_policy(policy, now=now)
    ledger = SharedTrafficLedger(
        hard_provider_bytes=policy.paid_proxy.byte_cap_bytes,
        soft_provider_bytes=policy.paid_proxy.byte_cap_bytes * 3 // 4,
        retry_limit=policy.paid_proxy.retry_limit,
    )
    started = time.monotonic()
    deadline = started + int(os.environ.get("TM_ACHIEVEMENT_PORTION_SECONDS", "2700"))
    connection = native.connect()
    try:
        version = os.environ.get("TM_ACHIEVEMENT_UNIVERSE_VERSION") or "daily:" + now.date().isoformat()
        universe = load_achievement_universe(connection, universe_version=version)
        proof = post_season_proof(connection, today=now)
    finally:
        connection.close()
    scraper = TransfermarktScraper(
        proxy_control_url=os.environ.get("TM_PROXY_CONTROL_URL"),
        traffic_ledger=ledger,
        lease_metadata={
            "dag_id": "dag_ingest_transfermarkt_achievements", "run_id": run_id,
            "task_id": "run_achievement_portion", "scope": "achievements",
        },
        request_deadline_monotonic=deadline,
    )
    try:
        _require_raw_store(scraper)
        with AchievementState(os.environ.get("TM_ACHIEVEMENT_STATE", DEFAULT_STATE)) as state:
            report = run_achievement_portion(
                scraper, state, universe, run_id=run_id, now=now,
                max_recipients=min(
                    int(os.environ.get("TM_ACHIEVEMENT_MAX_RECIPIENTS", "180")),
                    policy.paid_proxy.request_limit,
                ),
                post_season_proof=proof, deadline_monotonic=deadline,
                preflight=_reader_preflight(),
            )
    finally:
        scraper.close()
    _atomic_json(os.environ.get("TM_ACHIEVEMENT_REPORT", DEFAULT_REPORT), report)
    print(json.dumps(report, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
