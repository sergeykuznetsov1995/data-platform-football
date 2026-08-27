#!/usr/bin/env python3
"""Resume-safe paid browser validation for the frozen all-men snapshot."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from scrapers.sofascore.discovery import (
    DISCOVERY_LEASE_MAX_BYTES,
    DISCOVERY_LEASE_TTL_SECONDS,
    DiscoverySchemaError,
    LeaseBrowserSofaScoreClient,
    SEASON_TEAMS_PATH,
    TOURNAMENT_PATH,
    parse_catalog_payload,
    parse_team_count_payload,
)
from scrapers.sofascore.all_mens_campaign import validate_campaign_snapshot


class SnapshotEnrichmentError(ValueError):
    """The paid metadata pass cannot safely advance its immutable snapshot."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id", None)
    return hashlib.sha256(_canonical_json(unsigned)).hexdigest()


def _validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    if str(snapshot.get("snapshot_id") or "") != _snapshot_digest(snapshot):
        raise SnapshotEnrichmentError("snapshot digest mismatch")
    tournaments = snapshot.get("tournaments")
    if not isinstance(tournaments, list):
        raise SnapshotEnrichmentError("snapshot tournaments must be a list")
    if snapshot.get("candidate_count") != len(tournaments):
        raise SnapshotEnrichmentError("snapshot candidate_count mismatch")


def enrich_snapshot(
    snapshot: Mapping[str, Any],
    client: Any,
    *,
    wave_start_year: int,
    max_tournaments: Optional[int] = None,
    checkpoint: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> tuple[dict[str, Any], dict[str, int]]:
    """Confirm source gender and team counts for one breadth-first wave."""

    _validate_snapshot(snapshot)
    if max_tournaments is not None and (
        isinstance(max_tournaments, bool)
        or not isinstance(max_tournaments, int)
        or max_tournaments < 1
    ):
        raise SnapshotEnrichmentError("max_tournaments must be positive")
    document = deepcopy(dict(snapshot))
    processed = 0
    source_requests = 0
    for tournament in document["tournaments"]:
        changed = False
        if max_tournaments is not None and processed >= max_tournaments:
            break
        if not isinstance(tournament, dict):
            raise SnapshotEnrichmentError("snapshot tournament must be an object")
        status = str(tournament.get("metadata_status") or "pending")
        wave_seasons = [
            season for season in tournament.get("seasons") or []
            if isinstance(season, dict)
            and int(season.get("start_year", -1)) == int(wave_start_year)
        ]
        needs_identity = status == "pending"
        needs_teams = any(
            season.get("metadata_status") == "pending" for season in wave_seasons
        )
        if not needs_identity and not needs_teams:
            continue
        processed += 1
        source_id = int(tournament["unique_tournament_id"])
        if needs_identity:
            endpoint = TOURNAMENT_PATH.format(unique_tournament_id=source_id)
            payload = client.get_json(endpoint)
            source_requests += 1
            parsed = parse_catalog_payload(payload, endpoint=endpoint)
            if len(parsed) != 1 or parsed[0]["unique_tournament_id"] != source_id:
                raise SnapshotEnrichmentError(
                    f"detail response for tournament {source_id} is incomplete"
                )
            classification = parsed[0]["classification"]
            tournament["classification"] = classification
            excluded = (
                classification.get("gender") != "male"
                or classification.get("sport") != "football"
                or classification.get("status") == "excluded"
                or bool(classification.get("exclusion_reasons"))
            )
            if excluded:
                tournament["metadata_status"] = "excluded"
                for season in tournament.get("seasons") or []:
                    if isinstance(season, dict):
                        season["metadata_status"] = "excluded"
                changed = True
                document["snapshot_id"] = _snapshot_digest(document)
                if checkpoint is not None:
                    checkpoint(document)
                continue
            tournament["metadata_status"] = "ready"
            changed = True

        for season in wave_seasons:
            if season.get("metadata_status") != "pending":
                continue
            season_id = int(season["source_season_id"])
            endpoint = SEASON_TEAMS_PATH.format(
                unique_tournament_id=source_id,
                season_id=season_id,
            )
            payload = client.get_json(endpoint)
            source_requests += 1
            try:
                team_count, evidence = parse_team_count_payload(
                    payload,
                    unique_tournament_id=source_id,
                    season_id=season_id,
                )
            except DiscoverySchemaError:
                # Some source-listed cup seasons legitimately expose no team
                # index. Keep the exact season fail-closed without aborting
                # metadata validation for every other tournament.
                season["team_count"] = None
                season["team_count_evidence"] = {
                    "type": "source_team_ids_unavailable",
                    "endpoint": endpoint,
                    "reason": "schema_error",
                }
                season["metadata_status"] = "excluded"
                changed = True
                continue
            season["team_count"] = team_count
            season["team_count_evidence"] = evidence
            season["metadata_status"] = "ready"
            changed = True
        if changed:
            document["snapshot_id"] = _snapshot_digest(document)
            if checkpoint is not None:
                checkpoint(document)

    document["snapshot_id"] = _snapshot_digest(document)
    ready_tournaments = sum(
        item.get("metadata_status") == "ready"
        for item in document["tournaments"]
    )
    excluded_tournaments = sum(
        item.get("metadata_status") == "excluded"
        for item in document["tournaments"]
    )
    ready_wave_scopes = sum(
        season.get("metadata_status") == "ready"
        and int(season.get("start_year", -1)) == int(wave_start_year)
        for item in document["tournaments"]
        for season in item.get("seasons") or []
        if isinstance(season, Mapping)
    )
    excluded_wave_scopes = sum(
        season.get("metadata_status") == "excluded"
        and int(season.get("start_year", -1)) == int(wave_start_year)
        for item in document["tournaments"]
        for season in item.get("seasons") or []
        if isinstance(season, Mapping)
    )
    return document, {
        "processed_tournaments": processed,
        "ready_tournaments": ready_tournaments,
        "excluded_tournaments": excluded_tournaments,
        "ready_wave_scopes": ready_wave_scopes,
        "excluded_wave_scopes": excluded_wave_scopes,
        "source_requests": source_requests,
    }


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotEnrichmentError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise SnapshotEnrichmentError(f"{path} root must be an object")
    return value


@contextlib.contextmanager
def _snapshot_lock(path: Path):
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class _SnapshotCheckpoint:
    """Atomically advance exactly the snapshot revision that was planned."""

    def __init__(
        self,
        output_path: Path,
        *,
        source_path: Path,
        expected_snapshot_id: str,
    ) -> None:
        self.output_path = output_path
        self.source_path = source_path
        self.expected_snapshot_id = str(expected_snapshot_id).strip()
        if not self.expected_snapshot_id:
            raise SnapshotEnrichmentError("expected snapshot ID is required")

    def write(self, value: Mapping[str, Any]) -> None:
        _validate_snapshot(value)
        with _snapshot_lock(self.output_path):
            current_path = (
                self.output_path if self.output_path.exists() else self.source_path
            )
            current = _read_json(current_path)
            _validate_snapshot(current)
            if current.get("snapshot_id") != self.expected_snapshot_id:
                raise SnapshotEnrichmentError(
                    "snapshot changed after planning; refusing to overwrite it"
                )
            _atomic_json(self.output_path, value)
            self.expected_snapshot_id = str(value["snapshot_id"])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--expected-snapshot-id", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--wave-start-year", type=int, default=2025)
    parser.add_argument("--max-tournaments", type=int)
    parser.add_argument("--budget-cap-bytes", type=int, required=True)
    parser.add_argument(
        "--per-lease-max-bytes", type=int, default=DISCOVERY_LEASE_MAX_BYTES
    )
    parser.add_argument(
        "--lease-ttl-seconds", type=int, default=DISCOVERY_LEASE_TTL_SECONDS
    )
    parser.add_argument("--max-attempts", type=int, default=12)
    parser.add_argument(
        "--control-url", default=os.environ.get("SOFASCORE_PROXY_CONTROL_URL", "")
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    source_path = Path(args.output) if Path(args.output).exists() else Path(args.snapshot)
    output_path = Path(args.output)
    report_path = Path(args.report)
    report: dict[str, Any] = {
        "status": "running",
        "wave_start_year": args.wave_start_year,
        "errors": [],
    }
    client: Optional[LeaseBrowserSofaScoreClient] = None
    try:
        snapshot = _read_json(source_path)
        if snapshot.get("snapshot_id") != args.expected_snapshot_id:
            raise SnapshotEnrichmentError(
                "snapshot changed after planning; refusing to start metadata"
            )
        policy = _read_json(Path(args.policy))
        validate_campaign_snapshot(snapshot, policy)
        if not str(args.control_url).strip():
            raise SnapshotEnrichmentError("--control-url is required")
        client = LeaseBrowserSofaScoreClient(
            control_url=str(args.control_url).strip(),
            budget_cap_bytes=args.budget_cap_bytes,
            per_lease_max_bytes=args.per_lease_max_bytes,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_attempts=args.max_attempts,
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
        )
        checkpoint = _SnapshotCheckpoint(
            output_path,
            source_path=source_path,
            expected_snapshot_id=args.expected_snapshot_id,
        )
        enriched, counts = enrich_snapshot(
            snapshot,
            client,
            wave_start_year=args.wave_start_year,
            max_tournaments=args.max_tournaments,
            checkpoint=checkpoint.write,
        )
        checkpoint.write(enriched)
        report.update(counts)
        report["snapshot_id"] = enriched["snapshot_id"]
        report["campaign_id"] = enriched["campaign_id"]
        report["status"] = "success"
        exit_code = 0
    except Exception as exc:
        from scrapers.sofascore.lease_client import redact_sensitive

        report["status"] = "failed"
        report["errors"] = [redact_sensitive(exc)]
        exit_code = 1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                from scrapers.sofascore.lease_client import redact_sensitive

                report["status"] = "failed"
                report.setdefault("errors", []).append(redact_sensitive(exc))
                exit_code = 1
            report["traffic"] = client.stats
        _atomic_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "SnapshotEnrichmentError",
    "_SnapshotCheckpoint",
    "enrich_snapshot",
    "main",
]
