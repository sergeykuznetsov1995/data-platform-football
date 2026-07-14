#!/usr/bin/env python3
"""Discover and publish one complete Transfermarkt competition registry.

The command is deliberately a separate production boundary from entity
crawling.  It has one proxy lease client, one shared byte ledger and no direct
network fallback.  All source pages are reconciled and flattened before either
Bronze table is touched.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from dags.utils.transfermarkt_approval import (
    ApprovalJournal,
    ApprovalPacket,
    ApprovalStateError,
    ApprovalValidationError,
    StandingPolicy,
    load_standing_policy,
)
from scrapers.transfermarkt.client import (
    ProxyFilterLeaseProvider,
    TransfermarktHttpClient,
    redact_sensitive,
)
from scrapers.transfermarkt.discovery import discover_competition_registry
from scrapers.utils.rate_limiter import RateLimiter
from scrapers.transfermarkt.models import (
    FetchOutcome,
    FetchStatus,
    HARD_PROVIDER_BYTE_BUDGET,
    SOFT_PROVIDER_BYTE_STOP,
    SharedTrafficLedger,
    stable_payload_hash,
)
from scrapers.transfermarkt.registry import (
    ClassificationStatus,
    RegistryPage,
    RegistrySnapshot,
    deterministic_scope_id,
    reconcile_registry_pages,
)


ENTITY = "competition_registry"
EXPECTED_ENTITIES = ("competitions", "competition_editions")
MAX_ATTEMPTS = 6
# A lease is sticky, so an unthrottled crawl hits the source from one exit as
# fast as it can parse; the catalogue then starts answering 502/504. Pace the
# crawl like the other sources do (scrapers/utils/rate_limiter.py).
REQUESTS_PER_MINUTE = 10
CONCURRENCY = 1
COMPETITIONS_TABLE = "iceberg.bronze.transfermarkt_competitions"
EDITIONS_TABLE = "iceberg.bronze.transfermarkt_competition_editions"
TARGET_TABLES = (COMPETITIONS_TABLE, EDITIONS_TABLE)
PAID_PRESENTED_HASH_ENV = "TM_PAID_APPROVAL_PRESENTED_HASH"
WRITE_PRESENTED_HASH_ENV = "TM_WRITE_APPROVAL_PRESENTED_HASH"
STANDING_POLICY_DAG_ID = "dag_discover_transfermarkt_registry"
# Deliberately the same activation key as the ingest contour: both paid
# Transfermarkt DAGs stand or fall together on one operator decision, and
# per-DAG isolation comes from the dag_id pinned inside each policy file.
STANDING_POLICY_ENV_GATE = "TM_STANDING_POLICY_ENABLED"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$")

COMPETITION_COLUMNS = (
    "competition_id",
    "slug",
    "name",
    "country",
    "confederation",
    "competition_type",
    "gender",
    "team_type",
    "age_category",
    "season_format",
    "active",
    "source_url",
    "discovered_at",
    "canonical_competition_id",
    "classification_status",
    "classification_evidence",
    "registry_snapshot_id",
    "source_body_hash",
    "parser_revision",
    "schema_revision",
    "fetched_at",
    "cycle_id",
    "scope_id",
    "_source",
    "_entity_type",
    "_ingested_at",
    "_batch_id",
)

EDITION_COLUMNS = (
    "competition_id",
    "edition_id",
    "edition_label",
    "canonical_season",
    "season_format",
    "start_date",
    "end_date",
    "active",
    "current",
    "participant_count",
    "participant_hash",
    "source_url",
    "discovered_at",
    "registry_snapshot_id",
    "source_body_hash",
    "parser_revision",
    "schema_revision",
    "fetched_at",
    "cycle_id",
    "scope_id",
    "_source",
    "_entity_type",
    "_ingested_at",
    "_batch_id",
)


class DiscoveryRunnerError(RuntimeError):
    """A fail-closed runner invariant was violated."""


class AtomicJsonMapping(MutableMapping[str, Any]):
    """Small atomic JSON mapping used for durable resume and HTTP cache.

    Discovery is intentionally single-concurrency.  The file lock still keeps
    a second accidental process from observing a partially replaced document.
    A corrupt state file fails closed instead of silently starting over and
    spending the proxy budget again.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("checkpoint/cache path must be absolute")
        self._thread_lock = threading.RLock()
        self._data = self._read()

    @property
    def lock_path(self) -> Path:
        return self.path.with_name(f".{self.path.name}.lock")

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DiscoveryRunnerError(
                f"persistent JSON state is unreadable: {self.path}"
            ) from exc
        if not isinstance(value, dict):
            raise DiscoveryRunnerError(
                f"persistent JSON state must be an object: {self.path}"
            )
        return value

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            descriptor, temporary = tempfile.mkstemp(
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
            )
            try:
                os.fchmod(descriptor, 0o600)
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    json.dump(
                        self._data,
                        handle,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
            except BaseException:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
                raise

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        with self._thread_lock:
            self._data[key] = value
            self._persist()

    def __delitem__(self, key: str) -> None:
        with self._thread_lock:
            del self._data[key]
            self._persist()

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


@dataclass(frozen=True)
class DiscoveryRunResult:
    manifest_path: str
    manifest_hash: str
    manifest: Mapping[str, Any]


def _absolute_path(value: str, *, field: str) -> str:
    path = Path(value)
    if not path.is_absolute():
        raise DiscoveryRunnerError(f"{field} must be an absolute path")
    return str(path.resolve())


def _safe_component(value: str, *, field: str) -> str:
    if not _SAFE_COMPONENT.fullmatch(value):
        raise DiscoveryRunnerError(f"{field} is not a safe path component")
    return value


def _atomic_json_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _load_packet(path: str) -> ApprovalPacket:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoveryRunnerError("approval packet is unreadable") from exc
    if not isinstance(payload, dict):
        raise DiscoveryRunnerError("approval packet must be a JSON object")
    try:
        return ApprovalPacket(**payload)
    except (TypeError, ApprovalValidationError) as exc:
        raise DiscoveryRunnerError(f"approval packet is invalid: {exc}") from exc


class _OneShotAuthorization:
    """Validate one exact packet and consume it immediately before first effect."""

    def __init__(
        self,
        *,
        packet_path: str | None,
        journal_path: str | None,
        execution_argv: Sequence[str],
        expected_action: str,
        required: bool,
        request_limit: int,
        retry_limit: int,
        affected_files: Sequence[str],
        presented_hash: str | None,
    ) -> None:
        supplied = (packet_path is not None, journal_path is not None)
        if packet_path is not None and journal_path is None:
            raise DiscoveryRunnerError(
                "approval packet and journal must be supplied together"
            )
        if required and not all(supplied):
            raise DiscoveryRunnerError(
                f"discovery requires an approved {expected_action} packet"
            )
        self.packet = _load_packet(packet_path) if packet_path else None
        self.journal = ApprovalJournal(journal_path) if journal_path else None
        self.presented_hash = presented_hash
        self.consumed = False

        if self.packet is None:
            return
        if self.packet.action != expected_action:
            raise DiscoveryRunnerError(
                f"approval action must be {expected_action!r}"
            )
        if tuple(self.packet.argv) != tuple(execution_argv):
            raise DiscoveryRunnerError(
                "execution argv differs from the approved command"
            )
        if self.packet.byte_cap_bytes != HARD_PROVIDER_BYTE_BUDGET:
            raise DiscoveryRunnerError("approval byte cap differs from 15 MiB")
        if self.packet.request_limit != request_limit:
            raise DiscoveryRunnerError("approval request limit drift")
        if self.packet.retry_limit != retry_limit:
            raise DiscoveryRunnerError("approval retry limit drift")
        if self.packet.concurrency != CONCURRENCY:
            raise DiscoveryRunnerError("approval concurrency must be 1")
        expected_tables = TARGET_TABLES if expected_action == "production_write" else ()
        if tuple(sorted(self.packet.affected_tables)) != tuple(sorted(expected_tables)):
            raise DiscoveryRunnerError("approval table assets are not exact")
        if tuple(sorted(self.packet.affected_files)) != tuple(
            sorted(affected_files)
        ):
            raise DiscoveryRunnerError("approval file assets are not exact")
        if self.presented_hash != self.packet.packet_hash:
            raise DiscoveryRunnerError("presented approval hash is missing or stale")

    def require(self) -> None:
        if self.consumed:
            return
        if self.packet is None or self.journal is None:
            raise DiscoveryRunnerError(
                "paid proxy I/O requires an approved one-shot packet"
            )
        self.journal.consume(
            self.packet,
            presented_hash=str(self.presented_hash),
            execution_argv=self.packet.argv,
        )
        self.consumed = True

    def fail(self, reason: str) -> None:
        # A terminal run failure invalidates the whole approval set.  In
        # particular, the production-write companion must not remain approved
        # after the paid-proxy packet was consumed by a failed source attempt.
        if self.packet is None or self.journal is None:
            return
        try:
            self.journal.fail(
                self.packet,
                presented_hash=str(self.presented_hash),
                reason=redact_sensitive(reason)[:1000] or "discovery failed",
            )
        except ApprovalStateError:
            pass


def _approval_mode(args: argparse.Namespace) -> str:
    standing = bool(args.standing_policy or args.standing_policy_sha256)
    one_shot = bool(
        args.paid_proxy_approval_packet
        or args.production_write_approval_packet
        or args.approval_journal
    )
    if standing and one_shot:
        raise DiscoveryRunnerError(
            "standing-policy and one-shot approval flags are mutually exclusive"
        )
    # No approval flags at all stays on the one-shot path: a dry run needs
    # neither packet and a production run fails closed inside
    # _OneShotAuthorization exactly as before.
    return "standing_policy" if standing else "one_shot"


def validate_standing_policy_for_discovery(
    policy: StandingPolicy,
    *,
    request_limit: int,
    retry_limit: int,
) -> None:
    """Check one standing policy against the discovery contour's exact caps."""

    if policy.dag_id != STANDING_POLICY_DAG_ID:
        raise DiscoveryRunnerError(
            f"standing policy dag_id mismatch: {policy.dag_id!r}"
        )
    policy.assert_not_expired(datetime.now(timezone.utc))
    paid = policy.paid_proxy
    if (
        paid.byte_cap_bytes != HARD_PROVIDER_BYTE_BUDGET
        or paid.request_limit != int(request_limit)
        or paid.retry_limit != int(retry_limit)
        or paid.concurrency != CONCURRENCY
    ):
        raise DiscoveryRunnerError(
            "standing policy paid_proxy caps differ from discovery limits"
        )
    write = policy.production_write
    if (
        write.byte_cap_bytes != 0
        or write.request_limit != 0
        or write.retry_limit != 0
        or write.concurrency != 1
    ):
        raise DiscoveryRunnerError(
            "standing policy production_write caps are unsafe"
        )
    missing = sorted(set(TARGET_TABLES) - set(policy.allowed_write_tables))
    if missing:
        raise DiscoveryRunnerError(
            f"standing policy omits write tables: {missing}"
        )


def _enforce_standing_policy(args: argparse.Namespace) -> StandingPolicy:
    """Prove the committed policy before the checkpoint, client or any I/O."""

    if os.environ.get(STANDING_POLICY_ENV_GATE, "").strip().lower() not in {
        "1", "true", "yes", "on",
    }:
        raise DiscoveryRunnerError(
            f"{STANDING_POLICY_ENV_GATE} must be true before a "
            "standing-policy discovery"
        )
    if not args.standing_policy:
        raise DiscoveryRunnerError("standing_policy is required")
    if not args.standing_policy_sha256:
        raise DiscoveryRunnerError("standing_policy_sha256 is required")
    policy = load_standing_policy(
        _absolute_path(args.standing_policy, field="standing_policy")
    )
    if policy.policy_hash != str(args.standing_policy_sha256):
        raise DiscoveryRunnerError(
            "standing policy content differs from the pinned sha256"
        )
    validate_standing_policy_for_discovery(
        policy,
        request_limit=args.request_limit,
        retry_limit=args.retry_limit,
    )
    return policy


class _StandingAuthorization:
    """Standing-policy stand-in for both one-shot discovery authorizations.

    The policy is fully proven (env gate, pinned sha, caps, tables, expiry)
    by _enforce_standing_policy before this object exists, so require() has
    nothing left to consume and fail() has no journal to invalidate: the
    grant lives in git and failure evidence is the hash-named manifest.
    """

    packet = None

    def __init__(self, policy: StandingPolicy) -> None:
        self.policy = policy
        self.consumed = False

    def require(self) -> None:
        self.consumed = True

    def fail(self, reason: str) -> None:
        return None


def _record_standing_authorization(
    *,
    output_root: str,
    cycle_id: str,
    run_id: str,
    manifest_hash: str,
    policy: StandingPolicy,
) -> None:
    """Persist which standing policy authorized this committed discovery.

    Deliberately its own stable-named file: terminal manifests are hash-named,
    so a later failed rerun of the same cycle can never overwrite this record.
    """

    _atomic_json_write(
        Path(output_root) / cycle_id / "standing-authorization.json",
        {
            "status": "complete",
            "cycle_id": cycle_id,
            "run_id": run_id,
            "manifest_hash": manifest_hash,
            "approval_mode": "standing_policy",
            "standing_policy": {
                "policy_hash": policy.policy_hash,
                "policy_version": int(policy.policy_version),
            },
        },
    )


def _validate_html(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return "empty HTML body"
    soup = BeautifulSoup(value, "html.parser")
    if soup.html is None or soup.body is None:
        return "incomplete HTML document"
    return None


def _mib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(int(value) / 1024 / 1024, 6)


def _traffic_manifest(ledger: SharedTrafficLedger) -> dict[str, Any]:
    raw = ledger.snapshot()
    by_entity: dict[str, Any] = {}
    entity_defaults = {
        "decoded_bytes": 0,
        "wire_bytes": 0,
        "provider_up_bytes": 0,
        "provider_down_bytes": 0,
        "provider_bytes": 0,
        "requests": 0,
        "retries": 0,
        "cache_hits": 0,
        "cache_hit_rate": 0.0,
        "duration_seconds": 0.0,
    }
    entity_items = {ENTITY: entity_defaults, **raw["by_entity"]}
    for entity, item in sorted(entity_items.items()):
        enriched = dict(item)
        for field in (
            "decoded_bytes",
            "wire_bytes",
            "provider_up_bytes",
            "provider_down_bytes",
            "provider_bytes",
        ):
            enriched[field.replace("_bytes", "_mib")] = _mib(item[field])
        by_entity[entity] = enriched
    return {
        "hard_provider_byte_budget": raw["hard_provider_byte_budget"],
        "soft_provider_byte_stop": raw["soft_provider_byte_stop"],
        "decoded_bytes": raw["decoded_bytes"],
        "decoded_mib": _mib(raw["decoded_bytes"]),
        "wire_bytes": raw["wire_bytes"],
        "wire_mib": _mib(raw["wire_bytes"]),
        "provider_up_bytes": raw["provider_up_bytes"],
        "provider_down_bytes": raw["provider_down_bytes"],
        "provider_metered_bytes": raw["provider_metered_bytes"],
        "provider_metered_mib": _mib(raw["provider_metered_bytes"]),
        "requests": raw["requests"],
        "retries": raw["retries"],
        "cache_hits": raw["cache_hits"],
        "cache_hit_rate": raw["cache_hit_rate"],
        "duration_seconds": raw["duration_seconds"],
        "soft_stop_reached": raw["soft_stop_reached"],
        "by_entity": by_entity,
    }


def _metadata_row(
    row: Mapping[str, Any],
    *,
    fetched_at: datetime,
    cycle_id: str,
    scope_id: str,
    batch_id: str,
    entity_type: str,
) -> dict[str, Any]:
    return {
        **row,
        "fetched_at": fetched_at.isoformat(),
        "cycle_id": cycle_id,
        "scope_id": scope_id,
        "_source": "transfermarkt",
        "_entity_type": entity_type,
        "_ingested_at": fetched_at.isoformat(),
        "_batch_id": batch_id,
    }


def _flatten_snapshot(
    snapshot: RegistrySnapshot,
    *,
    cycle_id: str,
    fetched_at: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], tuple[dict[str, Any], ...]]:
    competition_rows = [
        _metadata_row(
            competition.as_dict(),
            fetched_at=fetched_at,
            cycle_id=cycle_id,
            scope_id=snapshot.snapshot_id,
            batch_id=snapshot.snapshot_id,
            entity_type="competitions",
        )
        for competition in snapshot.competitions
    ]
    edition_rows = [
        _metadata_row(
            edition.as_dict(),
            fetched_at=fetched_at,
            cycle_id=cycle_id,
            scope_id=deterministic_scope_id(
                edition.competition_id, edition.edition_id
            ),
            batch_id=snapshot.snapshot_id,
            entity_type="competition_editions",
        )
        for edition in snapshot.editions
    ]
    for row in competition_rows:
        if tuple(row) != COMPETITION_COLUMNS:
            raise DiscoveryRunnerError("competition Bronze schema drift")
    for row in edition_rows:
        if tuple(row) != EDITION_COLUMNS:
            raise DiscoveryRunnerError("edition Bronze schema drift")

    competition_keys = [row["competition_id"] for row in competition_rows]
    edition_keys = [
        (row["competition_id"], row["edition_id"]) for row in edition_rows
    ]
    if not competition_keys or not edition_keys:
        raise DiscoveryRunnerError("registry snapshot cannot be empty")
    if len(competition_keys) != len(set(competition_keys)):
        raise DiscoveryRunnerError("duplicate competition natural key")
    if len(edition_keys) != len(set(edition_keys)):
        raise DiscoveryRunnerError("duplicate edition natural key")
    edition_parent_ids = {key[0] for key in edition_keys}
    if edition_parent_ids != set(competition_keys):
        raise DiscoveryRunnerError(
            "every discovered competition must have at least one edition"
        )
    current_counts = Counter(
        row["competition_id"] for row in edition_rows if row["current"]
    )
    if any(current_counts[competition_id] != 1 for competition_id in competition_keys):
        raise DiscoveryRunnerError(
            "every competition must have exactly one current edition"
        )
    for row in (*competition_rows, *edition_rows):
        if not re.fullmatch(r"[a-f0-9]{64}", row["source_body_hash"]):
            raise DiscoveryRunnerError("source body hash is not SHA-256")

    scopes = tuple(item.as_dict() for item in snapshot.crawl_scopes(strict=False))
    eligible_ids = {
        item.competition_id
        for item in snapshot.competitions
        if item.classification_status is ClassificationStatus.ELIGIBLE
    }
    if any(scope["competition_id"] not in eligible_ids for scope in scopes):
        raise DiscoveryRunnerError("unknown classification escaped into crawl scopes")
    return competition_rows, edition_rows, scopes


def _dataframe(rows: list[dict[str, Any]], columns: Sequence[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=list(columns))
    for field in ("discovered_at", "fetched_at", "_ingested_at"):
        frame[field] = pd.to_datetime(frame[field], utc=True)
    if "start_date" in frame:
        frame["start_date"] = pd.to_datetime(frame["start_date"]).dt.date
        frame["end_date"] = pd.to_datetime(frame["end_date"]).dt.date
        frame["participant_count"] = pd.array(
            frame["participant_count"], dtype="Int64"
        )
    return frame


def _default_writer_factory():
    from scrapers.base.iceberg_writer import IcebergWriter

    return IcebergWriter()


def _write_snapshot(
    writer: Any,
    competition_rows: list[dict[str, Any]],
    edition_rows: list[dict[str, Any]],
    snapshot_id: str,
) -> tuple[dict[str, Any], ...]:
    if not re.fullmatch(r"tm-discovery-[a-f0-9]{24}", snapshot_id):
        raise DiscoveryRunnerError("unsafe registry snapshot id")
    delete_filter = f"registry_snapshot_id = '{snapshot_id}'"
    outputs = []
    for table, rows, columns in (
        ("transfermarkt_competitions", competition_rows, COMPETITION_COLUMNS),
        (
            "transfermarkt_competition_editions",
            edition_rows,
            EDITION_COLUMNS,
        ),
    ):
        frame = _dataframe(rows, columns)
        path = writer.write_dataframe(
            frame,
            database="bronze",
            table=table,
            partition_spec=None,
            mode="append",
            add_metadata=False,
            delete_filter=delete_filter,
        )
        outputs.append({"table": path, "rows": len(frame)})
    return tuple(outputs)


def _manifest_output(
    output_root: str,
    cycle_id: str,
    manifest: Mapping[str, Any],
) -> tuple[str, str]:
    manifest_hash = stable_payload_hash(manifest)
    path = (
        Path(output_root)
        / cycle_id
        / f"transfermarkt-discovery-{manifest_hash}.json"
    )
    _atomic_json_write(
        path,
        {"manifest_hash": manifest_hash, "manifest": dict(manifest)},
    )
    return str(path), manifest_hash


def _duration_seconds(
    monotonic: Callable[[], float],
    started: float,
) -> float | None:
    """Measure wall time without allowing a broken test clock to hide failure."""

    try:
        return round(max(0.0, float(monotonic()) - float(started)), 6)
    except BaseException:
        return None


def _resume_manifest(
    *,
    checkpoint: AtomicJsonMapping | None,
    cache: AtomicJsonMapping | None,
    checkpoint_entries_before: int | None,
    cache_entries_before: int | None,
) -> dict[str, Any]:
    checkpoint_after = len(checkpoint) if checkpoint is not None else None
    cache_after = len(cache) if cache is not None else None
    return {
        "resumed": (
            checkpoint_entries_before > 0
            if checkpoint_entries_before is not None
            else None
        ),
        "entries_before": checkpoint_entries_before,
        "entries_after": checkpoint_after,
        "new_entries": (
            checkpoint_after - checkpoint_entries_before
            if checkpoint_after is not None
            and checkpoint_entries_before is not None
            else None
        ),
        "transport_cache_entries_before": cache_entries_before,
        "transport_cache_entries_after": cache_after,
    }


def _scope_manifest(args: argparse.Namespace, cycle_id: str) -> dict[str, str]:
    return {
        "cycle_id": cycle_id,
        "dag_id": str(args.dag_id),
        "run_id": str(args.run_id),
        "task_id": str(args.task_id),
        "scope_id": cycle_id,
        "entity": ENTITY,
    }


def _failure_manifest(
    *,
    args: argparse.Namespace,
    cycle_id: str,
    error: BaseException,
    ledger: SharedTrafficLedger,
    duration_seconds: float | None,
    checkpoint: AtomicJsonMapping | None,
    cache: AtomicJsonMapping | None,
    checkpoint_entries_before: int | None,
    cache_entries_before: int | None,
    competition_rows: list[dict[str, Any]] | None,
    edition_rows: list[dict[str, Any]] | None,
    source_started: bool,
    source_validated: bool,
    write_authorized: bool,
    write_started: bool,
    write_completed: bool,
    writes: Sequence[Mapping[str, Any]],
    paid_authorization: _OneShotAuthorization | _StandingAuthorization | None,
    write_authorization: _OneShotAuthorization | _StandingAuthorization | None,
) -> dict[str, Any]:
    """Build non-ambiguous terminal evidence without claiming empty source data."""

    standing_policy_hash = (
        paid_authorization.policy.policy_hash
        if isinstance(paid_authorization, _StandingAuthorization)
        else None
    )
    try:
        # The mode the run *attempted*: an enforcement failure (gate off, sha
        # drift) happens before any authorization object exists, yet the
        # evidence must still attribute the failure to the standing path.
        approval_mode: str | None = _approval_mode(args)
    except DiscoveryRunnerError:
        approval_mode = None

    if not source_started:
        source_status = "not_started"
        dq_status = "not_run"
    elif not source_validated:
        source_status = "failed"
        dq_status = "failed"
    else:
        source_status = "success"
        dq_status = "passed"

    if bool(args.dry_run):
        write_status = "not_applicable"
    elif write_completed:
        write_status = "complete"
    elif write_started:
        write_status = "failed_or_partial"
    elif source_validated and not write_authorized:
        write_status = "blocked_approval"
    elif source_started and not source_validated:
        write_status = "blocked_source_failure"
    else:
        write_status = "not_started"

    message = redact_sensitive(error)[:2000] or "discovery failed"
    return {
        "status": "failed",
        "error": {
            "type": type(error).__name__,
            "message": message,
        },
        "dry_run": bool(args.dry_run),
        "cycle_id": cycle_id,
        "run_id": str(args.run_id),
        "scope": _scope_manifest(args, cycle_id),
        "expected_entities": list(EXPECTED_ENTITIES),
        # None means unknown because source completeness was not proven.  Zero
        # would incorrectly look like authoritative_empty evidence.
        "rows": {
            "competitions": (
                len(competition_rows) if competition_rows is not None else None
            ),
            "competition_editions": (
                len(edition_rows) if edition_rows is not None else None
            ),
        },
        "hashes": {
            "competitions": (
                stable_payload_hash(competition_rows)
                if competition_rows is not None
                else None
            ),
            "competition_editions": (
                stable_payload_hash(edition_rows)
                if edition_rows is not None
                else None
            ),
        },
        # A failed or incomplete source is never evidence of an empty entity.
        "authoritative_empty": False,
        "not_applicable": False,
        "source_status": source_status,
        "dq": {
            "status": dq_status,
            "source_complete": source_validated,
            "schema_validation": "passed" if source_validated else dq_status,
            "classification_validation": (
                "passed" if source_validated else dq_status
            ),
        },
        "write_status": {
            "status": write_status,
            "authorized": write_authorized,
            "attempted": write_started,
            "completed": write_completed,
            "affected_tables": list(TARGET_TABLES),
        },
        "writes": [dict(item) for item in writes],
        "traffic": _traffic_manifest(ledger),
        "duration_seconds": duration_seconds,
        "checkpoint_resume": _resume_manifest(
            checkpoint=checkpoint,
            cache=cache,
            checkpoint_entries_before=checkpoint_entries_before,
            cache_entries_before=cache_entries_before,
        ),
        "paid_proxy_approval_packet_hash": (
            paid_authorization.packet.packet_hash
            if paid_authorization is not None
            and paid_authorization.packet is not None
            else None
        ),
        "production_write_approval_packet_hash": (
            write_authorization.packet.packet_hash
            if write_authorization is not None
            and write_authorization.packet is not None
            else None
        ),
        "approval_mode": approval_mode,
        "standing_policy_hash": standing_policy_hash,
    }


def _execute_once(
    args: argparse.Namespace,
    *,
    execution_argv: Sequence[str],
    discovery_fn: Callable[..., tuple[RegistryPage, ...]] = (
        discover_competition_registry
    ),
    lease_provider_factory: Callable[..., Any] = ProxyFilterLeaseProvider,
    http_client_factory: Callable[..., Any] = TransfermarktHttpClient,
    writer_factory: Callable[[], Any] = _default_writer_factory,
    utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
) -> DiscoveryRunResult:
    """Execute one fully bounded registry snapshot (dependency-injectable)."""

    if args.request_limit <= 0:
        raise DiscoveryRunnerError("request limit must be positive")
    if args.retry_limit < 0 or args.retry_limit > args.request_limit:
        raise DiscoveryRunnerError("retry limit must be between 0 and request limit")
    if args.cache_ttl_seconds <= 0:
        raise DiscoveryRunnerError("cache TTL must be positive")
    if args.lease_ttl_seconds <= 0:
        raise DiscoveryRunnerError("lease TTL must be positive")
    cycle_id = _safe_component(args.cycle_id, field="cycle_id")
    checkpoint_path = _absolute_path(args.checkpoint, field="checkpoint")
    cache_path = _absolute_path(args.cache, field="cache")
    output_root = _absolute_path(args.output_root, field="output_root")
    paid_packet_path = (
        _absolute_path(
            args.paid_proxy_approval_packet,
            field="paid_proxy_approval_packet",
        )
        if args.paid_proxy_approval_packet
        else None
    )
    write_packet_path = (
        _absolute_path(
            args.production_write_approval_packet,
            field="production_write_approval_packet",
        )
        if args.production_write_approval_packet
        else None
    )
    journal_path = (
        _absolute_path(args.approval_journal, field="approval_journal")
        if args.approval_journal
        else None
    )
    affected_files = (
        checkpoint_path,
        cache_path,
        output_root,
        journal_path or "not-used-dry-run-approval-journal",
    )
    approval_mode = _approval_mode(args)
    standing_policy: StandingPolicy | None = None
    paid_authorization: _OneShotAuthorization | _StandingAuthorization
    write_authorization: _OneShotAuthorization | _StandingAuthorization
    if approval_mode == "standing_policy":
        # The env gate, pinned sha, caps and table checks all fail here,
        # before the checkpoint, cache, lease client or any paid I/O exist.
        standing_policy = _enforce_standing_policy(args)
        paid_authorization = _StandingAuthorization(standing_policy)
        write_authorization = paid_authorization
    else:
        paid_authorization = _OneShotAuthorization(
            packet_path=paid_packet_path,
            journal_path=journal_path,
            execution_argv=execution_argv,
            expected_action="paid_proxy",
            required=not bool(args.dry_run),
            request_limit=args.request_limit,
            retry_limit=args.retry_limit,
            affected_files=affected_files,
            presented_hash=os.environ.get(PAID_PRESENTED_HASH_ENV),
        )
        write_authorization = _OneShotAuthorization(
            packet_path=write_packet_path,
            journal_path=journal_path,
            execution_argv=execution_argv,
            expected_action="production_write",
            required=not bool(args.dry_run),
            request_limit=args.request_limit,
            retry_limit=args.retry_limit,
            affected_files=affected_files,
            presented_hash=os.environ.get(WRITE_PRESENTED_HASH_ENV),
        )

    checkpoint = AtomicJsonMapping(checkpoint_path)
    cache = AtomicJsonMapping(cache_path)
    checkpoint_entries_before = len(checkpoint)
    cache_entries_before = len(cache)
    ledger = SharedTrafficLedger(
        hard_provider_bytes=HARD_PROVIDER_BYTE_BUDGET,
        soft_provider_bytes=SOFT_PROVIDER_BYTE_STOP,
        retry_limit=args.retry_limit,
    )
    lease_provider = lease_provider_factory(args.proxy_control_url)
    client = http_client_factory(
        lease_provider=lease_provider,
        traffic_ledger=ledger,
        retry_budget=args.retry_limit,
        lease_metadata={
            "dag_id": args.dag_id,
            "run_id": args.run_id,
            "task_id": args.task_id,
            "entity": ENTITY,
            "scope": cycle_id,
        },
        lease_ttl_seconds=args.lease_ttl_seconds,
        cache=cache,
        rate_limiter=RateLimiter(
            max_requests=REQUESTS_PER_MINUTE, window_seconds=60,
        ),
    )
    client.begin_request_scope(request_attempt_budget=args.request_limit)
    started = monotonic()
    source_started = False
    source_validated = False
    write_authorized = False
    write_started = False
    write_completed = False
    competition_rows: list[dict[str, Any]] | None = None
    edition_rows: list[dict[str, Any]] | None = None
    writes: tuple[dict[str, Any], ...] = ()

    def fetch(url: str) -> FetchOutcome[str]:
        paid_authorization.require()
        retries_used = int(client.get_traffic_stats().get("retries", 0))
        remaining_retries = max(0, args.retry_limit - retries_used)
        outcome = client.fetch(
            url,
            as_json=False,
            max_attempts=min(MAX_ATTEMPTS, remaining_retries + 1),
            label=ENTITY,
            context={"scope": cycle_id},
            validator=_validate_html,
            cache_key=hashlib.sha256(url.encode("utf-8")).hexdigest(),
            cache_ttl_seconds=args.cache_ttl_seconds,
        )
        if outcome.status is not FetchStatus.OK or outcome.status_code != 200:
            return outcome
        if not outcome.payload_hash:
            raise DiscoveryRunnerError("successful response has no payload hash")
        if outcome.payload_hash != stable_payload_hash(outcome.value):
            raise DiscoveryRunnerError("successful response payload hash mismatch")
        return outcome

    try:
        source_started = True
        pages = discovery_fn(
            fetch=fetch,
            checkpoint=checkpoint,
            traffic_ledger=ledger,
            clock=utcnow,
        )
        snapshot = reconcile_registry_pages(pages)
        fetched_at = utcnow()
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise DiscoveryRunnerError("runner clock must be timezone-aware")
        competition_rows, edition_rows, scopes = _flatten_snapshot(
            snapshot,
            cycle_id=cycle_id,
            fetched_at=fetched_at,
        )
        # Final provider counters and hard-cap enforcement are known before
        # either production table is touched.
        client.close()
        traffic = _traffic_manifest(ledger)
        if traffic["provider_metered_bytes"] > HARD_PROVIDER_BYTE_BUDGET:
            raise DiscoveryRunnerError("provider hard byte budget exceeded")
        if traffic["requests"] > args.request_limit:
            raise DiscoveryRunnerError("request limit exceeded")
        if traffic["retries"] > args.retry_limit:
            raise DiscoveryRunnerError("retry limit exceeded")
        source_validated = True

        if not args.dry_run:
            write_authorization.require()
            write_authorized = True
            write_started = True
            writes = _write_snapshot(
                writer_factory(),
                competition_rows,
                edition_rows,
                snapshot.snapshot_id,
            )
            write_completed = True

        classifications = Counter(
            row["classification_status"] for row in competition_rows
        )
        manifest = {
            "status": "success",
            "error": None,
            "dry_run": bool(args.dry_run),
            "cycle_id": cycle_id,
            "run_id": str(args.run_id),
            "scope": _scope_manifest(args, cycle_id),
            "expected_entities": list(EXPECTED_ENTITIES),
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_hash": snapshot.snapshot_hash,
            # The snapshot's own timezone-aware capture time: standing
            # publication compares it with the canonical state's promoted_at
            # so a cleared publish task of an old run cannot replay a stale
            # snapshot over a newer promotion.
            "fetched_at": fetched_at.isoformat(),
            "page_count": snapshot.page_count,
            "source_body_hashes": list(snapshot.source_body_hashes),
            "rows": {
                "competitions": len(competition_rows),
                "competition_editions": len(edition_rows),
            },
            "hashes": {
                "competitions": stable_payload_hash(competition_rows),
                "competition_editions": stable_payload_hash(edition_rows),
                "crawl_scopes": stable_payload_hash(scopes),
            },
            "classification_counts": dict(sorted(classifications.items())),
            "blocked_competition_ids": list(snapshot.blocked_competition_ids),
            "promotable": snapshot.promotable,
            "crawl_scope_count": len(scopes),
            "crawl_scopes": list(scopes),
            "traffic": traffic,
            "duration_seconds": _duration_seconds(monotonic, started),
            "source_status": "success",
            "dq": {
                "status": "passed",
                "source_complete": True,
                "schema_validation": "passed",
                "classification_validation": "passed",
            },
            "write_status": {
                "status": "not_applicable" if args.dry_run else "complete",
                "authorized": write_authorized,
                "attempted": write_started,
                "completed": write_completed,
                "affected_tables": list(TARGET_TABLES),
            },
            "checkpoint_resume": _resume_manifest(
                checkpoint=checkpoint,
                cache=cache,
                checkpoint_entries_before=checkpoint_entries_before,
                cache_entries_before=cache_entries_before,
            ),
            "writes": list(writes),
            "paid_proxy_approval_packet_hash": (
                paid_authorization.packet.packet_hash
                if paid_authorization.packet is not None else None
            ),
            "production_write_approval_packet_hash": (
                write_authorization.packet.packet_hash
                if write_authorization.packet is not None else None
            ),
            "approval_mode": approval_mode,
            "standing_policy_hash": (
                standing_policy.policy_hash
                if standing_policy is not None else None
            ),
        }
        manifest_path, manifest_hash = _manifest_output(
            output_root, cycle_id, manifest
        )
        # The record attests an authorized production write under the policy;
        # a dry run writes nothing, so leaving no record is the honest state —
        # a "complete" record from a dry run could pass for write evidence.
        if standing_policy is not None and not bool(args.dry_run):
            _record_standing_authorization(
                output_root=output_root,
                cycle_id=cycle_id,
                run_id=str(args.run_id),
                manifest_hash=manifest_hash,
                policy=standing_policy,
            )
        return DiscoveryRunResult(
            manifest_path=manifest_path,
            manifest_hash=manifest_hash,
            manifest=manifest,
        )
    except BaseException as exc:
        try:
            client.close()
        except BaseException:
            pass
        for authorization in (paid_authorization, write_authorization):
            try:
                authorization.fail(str(exc))
            except BaseException:
                # Failure evidence must still be persisted when the approval
                # journal itself is temporarily unavailable or corrupt.
                pass
        failure = _failure_manifest(
            args=args,
            cycle_id=cycle_id,
            error=exc,
            ledger=ledger,
            duration_seconds=_duration_seconds(monotonic, started),
            checkpoint=checkpoint,
            cache=cache,
            checkpoint_entries_before=checkpoint_entries_before,
            cache_entries_before=cache_entries_before,
            competition_rows=competition_rows,
            edition_rows=edition_rows,
            source_started=source_started,
            source_validated=source_validated,
            write_authorized=write_authorized,
            write_started=write_started,
            write_completed=write_completed,
            writes=writes,
            paid_authorization=paid_authorization,
            write_authorization=write_authorization,
        )
        manifest_path, manifest_hash = _manifest_output(
            output_root, cycle_id, failure
        )
        try:
            setattr(exc, "manifest_path", manifest_path)
            setattr(exc, "manifest_hash", manifest_hash)
            setattr(exc, "manifest", failure)
        except BaseException:
            pass
        raise


def execute(
    args: argparse.Namespace,
    *,
    execution_argv: Sequence[str],
    discovery_fn: Callable[..., tuple[RegistryPage, ...]] = (
        discover_competition_registry
    ),
    lease_provider_factory: Callable[..., Any] = ProxyFilterLeaseProvider,
    http_client_factory: Callable[..., Any] = TransfermarktHttpClient,
    writer_factory: Callable[[], Any] = _default_writer_factory,
    utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
) -> DiscoveryRunResult:
    """Execute discovery and persist terminal evidence for setup failures too."""

    try:
        return _execute_once(
            args,
            execution_argv=execution_argv,
            discovery_fn=discovery_fn,
            lease_provider_factory=lease_provider_factory,
            http_client_factory=http_client_factory,
            writer_factory=writer_factory,
            utcnow=utcnow,
            monotonic=monotonic,
        )
    except BaseException as exc:
        if getattr(exc, "manifest_path", None):
            raise
        # Invalid cycle/output arguments cannot name a safe manifest location;
        # every failure after those two boundaries still receives terminal
        # evidence, including approval, checkpoint and client setup failures.
        try:
            cycle_id = _safe_component(args.cycle_id, field="cycle_id")
            output_root = _absolute_path(args.output_root, field="output_root")
            retry_limit = max(0, int(args.retry_limit))
            ledger = SharedTrafficLedger(
                hard_provider_bytes=HARD_PROVIDER_BYTE_BUDGET,
                soft_provider_bytes=SOFT_PROVIDER_BYTE_STOP,
                retry_limit=retry_limit,
            )
            failure = _failure_manifest(
                args=args,
                cycle_id=cycle_id,
                error=exc,
                ledger=ledger,
                duration_seconds=0.0,
                checkpoint=None,
                cache=None,
                checkpoint_entries_before=None,
                cache_entries_before=None,
                competition_rows=None,
                edition_rows=None,
                source_started=False,
                source_validated=False,
                write_authorized=False,
                write_started=False,
                write_completed=False,
                writes=(),
                paid_authorization=None,
                write_authorization=None,
            )
            manifest_path, manifest_hash = _manifest_output(
                output_root, cycle_id, failure
            )
            setattr(exc, "manifest_path", manifest_path)
            setattr(exc, "manifest_hash", manifest_hash)
            setattr(exc, "manifest", failure)
        except BaseException:
            pass
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover the complete Transfermarkt competition registry"
    )
    parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument(
        "--proxy-control-url",
        default=os.environ.get("TM_PROXY_CONTROL_URL"),
        required=os.environ.get("TM_PROXY_CONTROL_URL") is None,
    )
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--request-limit", type=int, default=1024)
    parser.add_argument("--retry-limit", type=int, default=12)
    parser.add_argument("--cache-ttl-seconds", type=int, default=86400)
    parser.add_argument("--lease-ttl-seconds", type=int, default=3600)
    parser.add_argument("--paid-proxy-approval-packet")
    parser.add_argument("--production-write-approval-packet")
    parser.add_argument("--approval-journal")
    parser.add_argument("--standing-policy")
    parser.add_argument("--standing-policy-sha256")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    execution_argv = (str(Path(__file__).resolve()), *raw_argv)
    try:
        args = _parser().parse_args(raw_argv)
        result = execute(args, execution_argv=execution_argv)
        print(
            json.dumps(
                {
                    "status": "success",
                    "manifest_path": result.manifest_path,
                    "manifest_hash": result.manifest_hash,
                    "rows": result.manifest["rows"],
                    "traffic": result.manifest["traffic"],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except Exception as exc:
        failure = {
            "status": "failed",
            "error": redact_sensitive(exc),
        }
        manifest_path = getattr(exc, "manifest_path", None)
        manifest_hash = getattr(exc, "manifest_hash", None)
        if manifest_path and manifest_hash:
            failure.update(
                manifest_path=manifest_path,
                manifest_hash=manifest_hash,
            )
        print(
            json.dumps(
                failure,
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
