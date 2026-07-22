"""Runtime integration for the durable Transfermarkt historical campaign.

The state contract in :mod:`utils.transfermarkt_backfill_state` is intentionally
pure.  This module is the narrow I/O boundary used by the Airflow DAG: strict
cutover/proxy/raw readiness, exact Trino readback after every mutation, frozen
registry campaign construction, and bounded scope planning.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

from scrapers.transfermarkt.models import MAX_SCOPE_BATCH
from scrapers.transfermarkt.raw_store import RawResponseStore
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_backfill_artifacts import BackfillArtifactStore
from utils.transfermarkt_backfill_attempts import verify_envelope_set
from utils.transfermarkt_scope_planner import (
    RegistryScopeTarget,
    build_promoted_registry_query,
    eligible_registry_scopes,
    plan_transfermarkt_scopes,
)


BACKFILL_DAG_ID = "dag_backfill_transfermarkt"
BACKFILL_RESULT_ROOT = "/opt/airflow/logs/transfermarkt-backfill"
DEFAULT_DISCOVERY_MAX_AGE = timedelta(hours=24)
INITIAL_SCOPE_CHUNK_SIZE = 128
MAX_STATE_MUTATION_SQL_BYTES = 900_000
_OPEN_CAMPAIGN_STATUSES = (
    state.CampaignStatus.WAITING_PREREQUISITE,
    state.CampaignStatus.ACTIVE,
    state.CampaignStatus.BLOCKED_PLATFORM,
)


class BackfillRuntimeError(RuntimeError):
    """A live prerequisite or durable state mutation failed closed."""


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise BackfillRuntimeError(f"{name} is required")
    return value


def _column_name(value: Any) -> str:
    name = getattr(value, "name", None)
    if name is not None:
        return str(name)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and value:
        return str(value[0])
    raise BackfillRuntimeError("Trino returned invalid column metadata")


def _rows(cur: Any) -> list[dict[str, Any]]:
    values = list(cur.fetchall())
    columns = [_column_name(item) for item in (cur.description or ())]
    if values and (not columns or any(len(item) != len(columns) for item in values)):
        raise BackfillRuntimeError("Trino returned an inconsistent row shape")
    return [dict(zip(columns, item, strict=True)) for item in values]


def _sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def read_promoted_registry(
    *,
    registry_snapshot_id: str | None = None,
    connection_factory: Callable[[], Any] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Read an exact logical registry snapshot without source discovery."""

    from utils import transfermarkt_native_v2 as tm_v2

    conn = (connection_factory or tm_v2.connect)()
    cur = conn.cursor()
    try:
        cur.execute(
            build_promoted_registry_query(
                registry_snapshot_id=registry_snapshot_id,
            )
        )
        result = tuple(_rows(cur))
    finally:
        cur.close()
        conn.close()
    if not result:
        raise BackfillRuntimeError("no exact promoted registry snapshot is available")
    return result


class BackfillStateRepository:
    """Execute state SQL with mandatory hash/revision read-after-write."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.cursor = connection.cursor()
        self._prefetched_scopes: dict[
            str, tuple[state.BackfillScopeState, ...]
        ] = {}

    @classmethod
    def connect(cls) -> "BackfillStateRepository":
        from utils import transfermarkt_native_v2 as tm_v2

        return cls(tm_v2.connect())

    def close(self) -> None:
        try:
            self.cursor.close()
        finally:
            self.connection.close()

    def __enter__(self) -> "BackfillStateRepository":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def ensure_schema(self) -> None:
        for statement in state.ddl_statements():
            self.cursor.execute(statement)

    def execute(self, statement: str) -> None:
        self.cursor.execute(statement)

    def query(self, statement: str) -> list[dict[str, Any]]:
        self.cursor.execute(statement)
        return _rows(self.cursor)

    def _parse_campaign_with_scopes(
        self,
        row: Mapping[str, Any],
        scopes: tuple[state.BackfillScopeState, ...],
    ) -> state.BackfillCampaign:
        try:
            return state.parse_campaign_row(
                row, targets=tuple(item.target for item in scopes)
            )
        except state.BackfillStateError as original:
            pointer = state.parse_campaign_pointer_row(row)
            if (
                pointer.get("status")
                != state.CampaignStatus.WAITING_PREREQUISITE.value
                or int(pointer.get("revision", -1)) != 0
            ):
                raise original
            campaign_id = str(pointer["campaign_id"])
            registry_rows = read_promoted_registry(
                registry_snapshot_id=str(pointer["registry_snapshot_id"])
            )
            targets = historical_targets_from_registry(registry_rows)
            prior_rows = self.query(
                f"SELECT s.* FROM {state.SCOPE_TABLE} s "
                f"JOIN {state.CAMPAIGN_TABLE} c "
                "ON c.campaign_id = s.campaign_id "
                f"WHERE s.campaign_id <> {_sql_text(campaign_id)}"
            )
            prior_identities = {
                _semantic_target_identity(state.parse_scope_row(item).target)
                for item in prior_rows
            }
            targets = tuple(
                item for item in targets
                if _semantic_target_identity(item) not in prior_identities
            )
            candidate = state.BackfillCampaign.build(
                registry_snapshot_id=str(pointer["registry_snapshot_id"]),
                policy_sha256=str(pointer["policy_sha256"]),
                parser_revision=str(pointer["parser_revision"]),
                schema_revision=str(pointer["schema_revision"]),
                targets=targets,
                now=_utc_timestamp(pointer["created_at"], field="created_at"),
                status=state.CampaignStatus.WAITING_PREREQUISITE,
            )
            try:
                return state.parse_campaign_row(row, targets=candidate.targets)
            except state.BackfillStateError as exc:
                raise BackfillRuntimeError(
                    "waiting campaign pointer cannot recover its frozen denominator"
                ) from exc

    def persist(self, record: Any, statement: str) -> None:
        """Execute one idempotent mutation and prove the exact row won."""

        self.cursor.execute(statement)
        rows = self.query(state.record_readback_sql(record))
        try:
            state.verify_record_readback(record, rows)
        except state.BackfillStateError as exc:
            raise BackfillRuntimeError(
                f"state readback failed for {type(record).__name__}"
            ) from exc

    def open_campaign(self) -> state.BackfillCampaign | None:
        statuses = ", ".join(_sql_text(item.value) for item in _OPEN_CAMPAIGN_STATUSES)
        rows = self.query(
            f"SELECT * FROM {state.CAMPAIGN_TABLE} "
            f"WHERE status IN ({statuses}) ORDER BY created_at DESC"
        )
        if len(rows) > 1:
            raise BackfillRuntimeError("more than one open backfill campaign exists")
        if not rows:
            return None
        campaign_id = str(rows[0].get("campaign_id") or "")
        scopes = self._query_scopes(campaign_id)
        self._prefetched_scopes[campaign_id] = scopes
        return self._parse_campaign_with_scopes(rows[0], scopes)

    def latest_campaign(self) -> state.BackfillCampaign | None:
        rows = self.query(
            f"SELECT * FROM {state.CAMPAIGN_TABLE} ORDER BY created_at DESC LIMIT 1"
        )
        if not rows:
            return None
        campaign_id = str(rows[0].get("campaign_id") or "")
        scopes = self._query_scopes(campaign_id)
        self._prefetched_scopes[campaign_id] = scopes
        return self._parse_campaign_with_scopes(rows[0], scopes)

    def load_campaigns(self) -> tuple[state.BackfillCampaign, ...]:
        rows = self.query(
            f"SELECT * FROM {state.CAMPAIGN_TABLE} ORDER BY created_at, campaign_id"
        )
        all_scope_rows = self.query(
            f"SELECT * FROM {state.SCOPE_TABLE} ORDER BY campaign_id, scope_id"
        ) if rows else []
        scopes_by_campaign: dict[str, list[state.HistoricalScopeTarget]] = {}
        for row in all_scope_rows:
            parsed = state.parse_scope_row(row)
            scopes_by_campaign.setdefault(parsed.campaign_id, []).append(
                parsed.target
            )
        return tuple(
            state.parse_campaign_row(
                item,
                targets=tuple(
                    scopes_by_campaign.get(str(item.get("campaign_id") or ""), ())
                ),
            )
            for item in rows
        )

    def load_campaign(self, campaign_id: str) -> state.BackfillCampaign:
        rows = self.query(state.campaign_select_sql(campaign_id))
        if len(rows) != 1:
            raise BackfillRuntimeError("campaign read cardinality mismatch")
        scopes = self._query_scopes(campaign_id)
        self._prefetched_scopes[campaign_id] = scopes
        return self._parse_campaign_with_scopes(rows[0], scopes)

    def _query_scopes(
        self, campaign_id: str
    ) -> tuple[state.BackfillScopeState, ...]:
        rows = self.query(state.scope_select_sql(campaign_id))
        return tuple(state.parse_scope_row(item) for item in rows)

    def load_scopes(self, campaign_id: str) -> tuple[state.BackfillScopeState, ...]:
        cached = self._prefetched_scopes.pop(campaign_id, None)
        if cached is not None:
            return cached
        return self._query_scopes(campaign_id)

    def persist_initial_scopes(
        self, scopes: Iterable[state.BackfillScopeState]
    ) -> None:
        """Install the frozen denominator in bounded, exactly read-back chunks."""

        items = tuple(scopes)
        for offset in range(0, len(items), INITIAL_SCOPE_CHUNK_SIZE):
            chunk = items[offset : offset + INITIAL_SCOPE_CHUNK_SIZE]
            statement = state.initial_scope_chunk_merge_sql(chunk)
            if len(statement.encode("utf-8")) > MAX_STATE_MUTATION_SQL_BYTES:
                raise BackfillRuntimeError(
                    "initial scope chunk exceeds the Trino statement bound"
                )
            self.cursor.execute(statement)
            rows = self.query(state.initial_scope_chunk_readback_sql(chunk))
            try:
                state.verify_initial_scope_chunk_readback(chunk, rows)
            except state.BackfillStateError as exc:
                raise BackfillRuntimeError(
                    "initial scope chunk readback failed"
                ) from exc

    def load_batch(self, batch_id: str) -> state.BackfillBatch:
        rows = self.query(
            f"SELECT * FROM {state.BATCH_TABLE} WHERE batch_id = {_sql_text(batch_id)}"
        )
        if len(rows) != 1:
            raise BackfillRuntimeError("batch read cardinality mismatch")
        return state.parse_batch_row(rows[0])

    def load_attempts(self, campaign_id: str) -> tuple[state.BackfillAttempt, ...]:
        rows = self.query(
            f"SELECT * FROM {state.ATTEMPT_TABLE} "
            f"WHERE campaign_id = {_sql_text(campaign_id)} ORDER BY attempt_id"
        )
        return tuple(state.parse_attempt_row(item) for item in rows)

    def load_batches(self, campaign_id: str) -> tuple[state.BackfillBatch, ...]:
        rows = self.query(
            f"SELECT * FROM {state.BATCH_TABLE} "
            f"WHERE campaign_id = {_sql_text(campaign_id)} ORDER BY claimed_at, batch_id"
        )
        return tuple(state.parse_batch_row(item) for item in rows)

    def initialise_campaign(
        self,
        campaign: state.BackfillCampaign,
    ) -> tuple[state.BackfillCampaign, tuple[state.BackfillScopeState, ...]]:
        """Idempotently install denominator rows, then make campaign runnable."""

        if campaign.status is not state.CampaignStatus.WAITING_PREREQUISITE:
            raise BackfillRuntimeError("new campaign must start waiting_prerequisite")
        orphan_scopes = self._query_scopes(campaign.campaign_id)
        if orphan_scopes:
            bootstrap_times = {item.updated_at for item in orphan_scopes}
            if (
                len(bootstrap_times) != 1
                or any(
                    item.revision != 0
                    or item.status is not state.ScopeStatus.PENDING
                    for item in orphan_scopes
                )
            ):
                raise BackfillRuntimeError(
                    "orphan campaign denominator is not an initial bootstrap"
                )
            # campaign_id/target_sha256 deliberately exclude wall-clock fields.
            # Reuse the first durable chunk timestamp after a crash so every
            # remaining immutable revision-zero scope has the exact same hash.
            bootstrap_at = next(iter(bootstrap_times))
            campaign = replace(
                campaign,
                created_at=bootstrap_at,
                updated_at=bootstrap_at,
            )
        scopes = tuple(
            state.BackfillScopeState.initial(campaign, target, now=campaign.created_at)
            for target in campaign.targets
        )
        expected_by_id = {item.target.scope_id: item for item in scopes}
        for orphan in orphan_scopes:
            if expected_by_id.get(orphan.target.scope_id) != orphan:
                raise BackfillRuntimeError(
                    "orphan campaign denominator differs from frozen registry"
                )
        # Publish the small frozen bootstrap header first.  If a later chunk
        # fails, the next run reads this exact snapshot from registry history
        # and resumes missing denominator rows even after canonical promotion.
        self.persist(campaign, state.campaign_merge_sql(campaign, expected_revision=-1))
        self.persist_initial_scopes(scopes)
        active = campaign.transition(
            state.CampaignStatus.ACTIVE, now=campaign.updated_at
        )
        self.persist(active, state.campaign_transition_merge_sql(campaign, active))
        return active, scopes

    def resume_waiting_campaign(
        self,
        campaign: state.BackfillCampaign,
    ) -> tuple[state.BackfillCampaign, tuple[state.BackfillScopeState, ...]]:
        """Recover a crash between campaign insert and denominator completion."""

        if campaign.status is not state.CampaignStatus.WAITING_PREREQUISITE:
            raise BackfillRuntimeError("campaign is not waiting for initialization")
        expected = tuple(
            state.BackfillScopeState.initial(campaign, target, now=campaign.created_at)
            for target in campaign.targets
        )
        existing_items = self.load_scopes(campaign.campaign_id)
        existing: dict[str, state.BackfillScopeState] = {}
        expected_ids = {item.target.scope_id for item in expected}
        for item in existing_items:
            scope_id = item.target.scope_id
            if scope_id in existing:
                raise BackfillRuntimeError(
                    "partially initialized denominator contains duplicate scopes"
                )
            if scope_id not in expected_ids:
                raise BackfillRuntimeError(
                    "partially initialized denominator contains an extra scope"
                )
            existing[scope_id] = item
        missing: list[state.BackfillScopeState] = []
        for item in expected:
            persisted = existing.get(item.target.scope_id)
            if persisted is None:
                missing.append(item)
            elif persisted != item:
                raise BackfillRuntimeError("partially initialized scope row drifted")
        self.persist_initial_scopes(missing)
        active = campaign.transition(
            state.CampaignStatus.ACTIVE, now=datetime.now(timezone.utc)
        )
        self.persist(active, state.campaign_transition_merge_sql(campaign, active))
        return active, expected

    def persist_claim(
        self,
        before: Iterable[state.BackfillScopeState],
        claim: state.ClaimResult,
    ) -> None:
        before_items = tuple(before)
        previous = {item.target.scope_id: item for item in before_items}
        intermediate = dict(previous)
        operations: list[tuple[Any, str]] = []
        for reclaimed in sorted(
            claim.reclaimed_scopes,
            key=lambda item: item.target.scope_id,
        ):
            old = previous[reclaimed.target.scope_id]
            operations.append(
                (
                    reclaimed,
                    state.scope_transition_merge_sql(old, reclaimed),
                )
            )
            intermediate[reclaimed.target.scope_id] = reclaimed
        if claim.batch is not None:
            operations.append(
                (
                    claim.batch,
                    state.batch_merge_sql(claim.batch, expected_revision=-1),
                )
            )
        for current in claim.scopes:
            old = intermediate[current.target.scope_id]
            if state.record_sha256(old) == state.record_sha256(current):
                continue
            operations.append(
                (
                    current,
                    state.scope_transition_merge_sql(old, current),
                )
            )
        expected = state.claim_merge_statements(before_items, claim)
        if tuple(statement for _, statement in operations) != expected:
            raise BackfillRuntimeError(
                "claim persistence plan differs from state contract"
            )
        for record, statement in operations:
            self.persist(record, statement)

    def recover_batch_claim(
        self,
        batch: state.BackfillBatch,
        scopes: Iterable[state.BackfillScopeState],
        *,
        lease_owner: str,
        now: datetime,
    ) -> tuple[state.BackfillScopeState, ...]:
        """Finish a partially persisted claim and refresh its running leases."""

        supplied = tuple(scopes)
        by_id = {item.target.scope_id: item for item in supplied}
        if len(by_id) != len(supplied):
            raise BackfillRuntimeError("campaign contains duplicate scope_id")
        try:
            before = tuple(by_id[scope_id] for scope_id in batch.scope_ids)
        except KeyError as exc:
            raise BackfillRuntimeError(
                "batch scope is absent from campaign state"
            ) from exc
        recovered = state.recover_batch_scopes(
            batch,
            before,
            lease_owner=lease_owner,
            now=now,
        )
        for previous, current in zip(before, recovered, strict=True):
            if state.record_sha256(previous) != state.record_sha256(current):
                self.persist_scope_transition(previous, current)
                by_id[current.target.scope_id] = current
        return tuple(sorted(by_id.values(), key=lambda item: item.target.scope_id))

    def persist_attempt(
        self,
        campaign: state.BackfillCampaign,
        previous: state.BackfillScopeState,
        attempt: state.BackfillAttempt,
    ) -> state.AttemptTransitionPlan:
        plan = state.plan_attempt_transition(campaign, previous, attempt)
        records: list[Any] = [attempt, plan.scope]
        if plan.campaign != campaign:
            records.append(plan.campaign)
        if len(records) != len(plan.statements):
            raise BackfillRuntimeError("attempt persistence plan shape is invalid")
        for record, statement in zip(records, plan.statements, strict=True):
            self.persist(record, statement)
        return plan

    def resume_persisted_attempt(
        self,
        campaign: state.BackfillCampaign,
        previous: state.BackfillScopeState,
        attempt: state.BackfillAttempt,
    ) -> state.AttemptTransitionPlan:
        """Apply an immutable attempt journal row whose later CAS was interrupted."""

        plan = state.plan_attempt_transition(campaign, previous, attempt)
        records: list[Any] = [plan.scope]
        if plan.campaign != campaign:
            records.append(plan.campaign)
        statements = plan.statements[1:]
        if len(records) != len(statements):
            raise BackfillRuntimeError("persisted attempt recovery shape is invalid")
        for record, statement in zip(records, statements, strict=True):
            self.persist(record, statement)
        return plan

    def persist_scope_transition(
        self,
        previous: state.BackfillScopeState,
        current: state.BackfillScopeState,
    ) -> None:
        self.persist(current, state.scope_transition_merge_sql(previous, current))

    def persist_batch_transition(
        self,
        previous: state.BackfillBatch,
        current: state.BackfillBatch,
    ) -> None:
        self.persist(current, state.batch_transition_merge_sql(previous, current))

    def persist_campaign_transition(
        self,
        previous: state.BackfillCampaign,
        current: state.BackfillCampaign,
    ) -> None:
        self.persist(current, state.campaign_transition_merge_sql(previous, current))

    def reconcile_platform_block(
        self,
        campaign: state.BackfillCampaign,
        *,
        now: datetime,
    ) -> tuple[state.BackfillBatch, ...]:
        """Finish the batch half of an interrupted campaign platform block."""

        if campaign.status is not state.CampaignStatus.BLOCKED_PLATFORM:
            raise BackfillRuntimeError("campaign is not platform-blocked")
        reconciled: list[state.BackfillBatch] = []
        for batch in self.load_batches(campaign.campaign_id):
            # A durable batch incident owns its exact pre-block status.  In
            # particular, COMPLETE must remain COMPLETE and DQ_PENDING must
            # retain its snapshot pins.  Only resolve_platform_incident may
            # restore that recorded status after its artifact is verified.
            if batch.open_platform_incident_id is not None:
                reconciled.append(batch)
                continue
            if batch.status in {
                state.BatchStatus.CLAIMED,
                state.BatchStatus.RUNNING,
                state.BatchStatus.DQ_PENDING,
            }:
                blocked = batch.transition(
                    state.BatchStatus.BLOCKED_PLATFORM,
                    now=now,
                )
                self.persist_batch_transition(batch, blocked)
                batch = blocked
            reconciled.append(batch)
        return tuple(reconciled)

    @staticmethod
    def _open_batch_incident(
        batches: Iterable[state.BackfillBatch],
    ) -> tuple[state.BackfillBatch, state.BackfillPlatformIncident] | None:
        matches: list[tuple[state.BackfillBatch, state.BackfillPlatformIncident]] = []
        for batch in batches:
            incident_id = batch.open_platform_incident_id
            if incident_id is None:
                continue
            incidents = tuple(
                item
                for item in batch.platform_incidents
                if item.incident_id == incident_id
            )
            if len(incidents) != 1:
                raise BackfillRuntimeError(
                    "open platform incident is absent from batch history"
                )
            matches.append((batch, incidents[0]))
        if len(matches) > 1:
            raise BackfillRuntimeError(
                "more than one open batch platform incident exists"
            )
        return matches[0] if matches else None

    def reconcile_open_platform_incident(
        self,
        campaign: state.BackfillCampaign,
        *,
        now: datetime,
    ) -> tuple[state.BackfillCampaign, state.BackfillBatch | None]:
        """Converge ``ACTIVE + open incident`` back to a blocked campaign.

        The campaign CAS deliberately happens independently of the batch row.
        This closes the crash window in explicit resume: if activation wins but
        incident resolution does not, the next planner observes the open
        incident and blocks the campaign again before planning any source work.
        """

        opened = self._open_batch_incident(self.load_batches(campaign.campaign_id))
        if opened is None:
            return campaign, None
        batch, _incident = opened
        if campaign.status is state.CampaignStatus.ACTIVE:
            blocked = campaign.transition(
                state.CampaignStatus.BLOCKED_PLATFORM,
                now=now,
            )
            self.persist_campaign_transition(campaign, blocked)
            campaign = blocked
        elif campaign.status is not state.CampaignStatus.BLOCKED_PLATFORM:
            raise BackfillRuntimeError(
                "open batch platform incident requires an active or blocked campaign"
            )
        return campaign, batch

    @staticmethod
    def _verify_platform_incident_artifact(
        *,
        campaign: state.BackfillCampaign,
        batch: state.BackfillBatch,
        incident: state.BackfillPlatformIncident,
        artifact_store: BackfillArtifactStore,
        raw_store: RawResponseStore,
    ) -> None:
        try:
            payload = artifact_store.load_json(
                incident.report_uri,
                expected_sha256=incident.report_sha256,
            )
        except Exception as exc:  # immutable-store adapter boundary
            raise BackfillRuntimeError(
                "platform incident artifact cannot be verified"
            ) from exc
        expected = {
            "contract_version": state.CONTRACT_VERSION,
            "campaign_id": campaign.campaign_id,
            "batch_id": batch.batch_id,
            "incident_id": incident.incident_id,
            "batch_revision": batch.revision - 1,
            "phase": incident.phase,
            "error_class": incident.error_class,
            "blocked_from_status": incident.blocked_from_status.value,
            "snapshot_pins": (
                dict(batch.snapshot_pins) if batch.snapshot_pins is not None else None
            ),
            "raw_evidence_ids": list(incident.raw_evidence_ids),
        }
        if not set(expected).issubset(payload) or any(
            state.canonical_json(payload[key]) != state.canonical_json(value)
            for key, value in expected.items()
        ):
            raise BackfillRuntimeError(
                "platform incident artifact differs from durable state"
            )
        cause_artifact = payload.get("cause_artifact")
        if cause_artifact is not None:
            if (
                not isinstance(cause_artifact, Mapping)
                or set(cause_artifact) != {"uri", "sha256"}
                or not isinstance(cause_artifact.get("uri"), str)
                or not isinstance(cause_artifact.get("sha256"), str)
            ):
                raise BackfillRuntimeError(
                    "platform incident cause artifact reference is invalid"
                )
            try:
                artifact_store.load_bytes(
                    cause_artifact["uri"],
                    expected_sha256=cause_artifact["sha256"],
                )
            except Exception as exc:  # immutable-store adapter boundary
                raise BackfillRuntimeError(
                    "platform incident cause artifact cannot be verified"
                ) from exc
        try:
            records = verify_envelope_set(raw_store, incident.raw_evidence_ids)
        except Exception as exc:  # immutable-store adapter boundary
            raise BackfillRuntimeError(
                "platform incident raw evidence cannot be verified"
            ) from exc
        if any(item.scope_id not in batch.scope_ids for item in records):
            raise BackfillRuntimeError(
                "platform incident raw evidence belongs to another batch"
            )

    def _resume_batch_platform_incident(
        self,
        campaign: state.BackfillCampaign,
        batch: state.BackfillBatch,
        incident: state.BackfillPlatformIncident,
        *,
        lease_owner: str,
        now: datetime,
        raw_store: RawResponseStore | None = None,
        artifact_store: BackfillArtifactStore | None = None,
    ) -> tuple[
        state.BackfillCampaign,
        tuple[state.BackfillScopeState, ...],
        state.BackfillBatch,
    ]:
        if (raw_store is None) != (artifact_store is None):
            raise BackfillRuntimeError(
                "raw and artifact stores must be supplied together"
            )
        if raw_store is None:
            try:
                resolved_raw_store = RawResponseStore.from_env()
            except Exception as exc:  # environment/filesystem adapter boundary
                raise BackfillRuntimeError(
                    "raw store is unavailable for platform incident resume"
                ) from exc
            if resolved_raw_store is None:
                raise BackfillRuntimeError(
                    "raw store is unavailable for platform incident resume"
                )
            raw_store = resolved_raw_store
            artifact_store = BackfillArtifactStore(raw_store)
        assert artifact_store is not None
        self._verify_platform_incident_artifact(
            campaign=campaign,
            batch=batch,
            incident=incident,
            artifact_store=artifact_store,
            raw_store=raw_store,
        )

        all_scopes = self.load_scopes(campaign.campaign_id)
        scope_by_id = {item.target.scope_id: item for item in all_scopes}
        if len(scope_by_id) != len(all_scopes):
            raise BackfillRuntimeError("campaign contains duplicate scope_id")
        attempts = tuple(
            item for item in self.load_attempts(campaign.campaign_id)
            if item.batch_id == batch.batch_id
            and item.outcome is state.AttemptOutcome.PLATFORM_ERROR
        )
        latest: dict[str, state.BackfillAttempt] = {}
        for attempt in attempts:
            previous_attempt = latest.get(attempt.scope_id)
            if previous_attempt is None or attempt.sequence > previous_attempt.sequence:
                latest[attempt.scope_id] = attempt

        # Close a crash between immutable attempt insertion and its scope CAS
        # while the campaign is still protected by the open incident.
        for scope_id, attempt in latest.items():
            scope = scope_by_id.get(scope_id)
            if scope is None:
                raise BackfillRuntimeError("platform attempt scope is missing")
            if (
                scope.status is state.ScopeStatus.RUNNING
                and scope.attempt_count + 1 == attempt.sequence
                and scope.claim_generation == attempt.claim_generation
            ):
                transition = self.resume_persisted_attempt(
                    campaign,
                    scope,
                    attempt,
                )
                scope_by_id[scope_id] = transition.scope

        platform_scope_ids = tuple(sorted(
            scope_id
            for scope_id, attempt in latest.items()
            if (
                scope_id in scope_by_id
                and scope_by_id[scope_id].attempt_count == attempt.sequence
                and scope_by_id[scope_id].claim_generation
                == attempt.claim_generation
                and scope_by_id[scope_id].status
                is state.ScopeStatus.RETRYABLE_ERROR
            )
        ))
        if platform_scope_ids:
            try:
                batch_scopes = tuple(scope_by_id[item] for item in batch.scope_ids)
            except KeyError as exc:
                raise BackfillRuntimeError(
                    "platform incident batch scope is missing"
                ) from exc
            resumed_scopes = state.resume_platform_scopes(
                batch,
                batch_scopes,
                platform_scope_ids=platform_scope_ids,
                lease_owner=lease_owner,
                now=now,
            )
            for previous_scope, resumed_scope in zip(
                batch_scopes, resumed_scopes, strict=True
            ):
                if state.record_sha256(previous_scope) != state.record_sha256(
                    resumed_scope
                ):
                    self.persist_scope_transition(previous_scope, resumed_scope)
                scope_by_id[resumed_scope.target.scope_id] = resumed_scope

        resolved = batch.resolve_platform_incident(now=now)
        if (
            incident.phase == "post_claim_planning"
            and not platform_scope_ids
            and resolved.status in {
                state.BatchStatus.CLAIMED,
                state.BatchStatus.RUNNING,
            }
        ):
            try:
                batch_scopes = tuple(scope_by_id[item] for item in batch.scope_ids)
                recovered_scopes = state.recover_batch_scopes(
                    resolved,
                    batch_scopes,
                    lease_owner=lease_owner,
                    now=now,
                )
            except (KeyError, state.BackfillStateError) as exc:
                raise BackfillRuntimeError(
                    "planner incident batch claim cannot be recovered"
                ) from exc
            for previous_scope, recovered_scope in zip(
                batch_scopes, recovered_scopes, strict=True
            ):
                if state.record_sha256(previous_scope) != state.record_sha256(
                    recovered_scope
                ):
                    self.persist_scope_transition(previous_scope, recovered_scope)
                scope_by_id[recovered_scope.target.scope_id] = recovered_scope

        # This ordering is the recovery fence.  A crash after campaign CAS and
        # before batch CAS leaves ACTIVE + open incident, which the next
        # planner immediately converges back to BLOCKED_PLATFORM.
        active = campaign.transition(state.CampaignStatus.ACTIVE, now=now)
        self.persist_campaign_transition(campaign, active)
        self.persist_batch_transition(batch, resolved)
        # Batch incidents are finalizer incidents, not scope-attempt failures:
        # scope leases/statuses must remain byte-for-byte unchanged.
        return (
            active,
            tuple(sorted(scope_by_id.values(), key=lambda item: item.target.scope_id)),
            resolved,
        )

    def resume_platform_campaign(
        self,
        campaign: state.BackfillCampaign,
        *,
        lease_owner: str,
        now: datetime,
        raw_store: RawResponseStore | None = None,
        artifact_store: BackfillArtifactStore | None = None,
    ) -> tuple[
        state.BackfillCampaign,
        tuple[state.BackfillScopeState, ...],
        state.BackfillBatch,
    ]:
        """Perform the explicit, evidence-bound resume of one blocked batch."""

        if campaign.status is not state.CampaignStatus.BLOCKED_PLATFORM:
            raise BackfillRuntimeError("campaign is not platform-blocked")
        opened = self._open_batch_incident(self.load_batches(campaign.campaign_id))
        if opened is not None:
            batch, incident = opened
            return self._resume_batch_platform_incident(
                campaign,
                batch,
                incident,
                lease_owner=lease_owner,
                now=now,
                raw_store=raw_store,
                artifact_store=artifact_store,
            )
        raise BackfillRuntimeError(
            "blocked campaign lacks an evidence-bound platform incident"
        )


def strict_cutover_preflight(
    *,
    connection_factory: Callable[[], Any] | None = None,
    raw_store_factory: Callable[[], RawResponseStore | None] | None = None,
    proxy_health_get: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Prove v2-only writer readiness before a campaign may claim paid work."""

    if not _truthy_env("TM_NATIVE_V2_ENABLED"):
        raise BackfillRuntimeError("TM_NATIVE_V2_ENABLED must be true")
    if not _truthy_env("TM_STANDING_POLICY_ENABLED"):
        raise BackfillRuntimeError("TM_STANDING_POLICY_ENABLED must be true")
    proxy_url = _required_env("TM_BACKFILL_PROXY_CONTROL_URL").rstrip("/")
    proxy_token = _required_env("TM_BACKFILL_PROXY_CONTROL_TOKEN")
    if len(proxy_token) < 32:
        raise BackfillRuntimeError(
            "TM_BACKFILL_PROXY_CONTROL_TOKEN must contain at least 32 characters"
        )
    production_token = os.environ.get("TM_PROXY_CONTROL_TOKEN", "").strip()
    if production_token and production_token == proxy_token:
        raise BackfillRuntimeError("production and backfill proxy tokens must differ")
    shared_token = os.environ.get("PROXY_FILTER_CONTROL_TOKEN", "").strip()
    if shared_token and shared_token == proxy_token:
        raise BackfillRuntimeError("shared and backfill proxy tokens must differ")

    from utils import transfermarkt_native_v2 as tm_v2

    factory = connection_factory or tm_v2.connect
    conn = factory()
    cur = conn.cursor()
    try:
        reader = tm_v2.read_reader_state(cur, allow_missing=True)
        if (
            not reader.exists
            or reader.active_version != "v2"
            or reader.active_slot not in {"a", "b"}
            or reader.legacy_writers_disabled_at is None
        ):
            raise BackfillRuntimeError(
                "Transfermarkt v2 cutover and legacy-writer shutdown are required"
            )
        cleanup = reader.cleanup_completed_at is not None
        views = tm_v2.verify_reader_views(
            cur,
            expected_version="v2",
            expected_revision=reader.revision,
            expected_slot=reader.active_slot,
            allow_static_slot=cleanup,
            require_no_legacy=cleanup,
        )
        if not views.get("passed"):
            raise BackfillRuntimeError("Transfermarkt v2 reader route preflight failed")
    finally:
        cur.close()
        conn.close()

    raw_factory = raw_store_factory or (lambda: RawResponseStore.from_env())
    raw_store = raw_factory()
    if raw_store is None:
        raise BackfillRuntimeError("Transfermarkt raw store is unavailable")

    if proxy_health_get is None:
        import requests

        proxy_health_get = requests.get
    try:
        response = proxy_health_get(
            f"{proxy_url}/v1/auth-check",
            headers={"X-Proxy-Control-Token": proxy_token},
            timeout=5,
        )
        status_code = int(getattr(response, "status_code", 0) or 0)
        health = response.json()
    except Exception as exc:  # noqa: BLE001 - network adapter boundary
        raise BackfillRuntimeError(
            "backfill proxy control health check failed"
        ) from exc
    if (
        status_code != 200
        or not isinstance(health, Mapping)
        or health.get("transfermarkt_backfill_paid_enabled") is not True
        or int(health.get("transfermarkt_requests_per_minute", 0)) != 12
        or health.get("transfermarkt_backfill_uses_production_daily_budget")
        is not False
        or health.get("transfermarkt_request_permit_consume_required") is not True
        or health.get("transfermarkt_request_permit_state_durable") is not True
        or int(health.get("transfermarkt_backfill_max_queue_seconds", 0)) <= 0
        or int(health.get("transfermarkt_backfill_max_queue_seconds", 0))
        >= int(health.get("transfermarkt_request_permit_pending_ttl_seconds", 0))
    ):
        raise BackfillRuntimeError("backfill proxy/pool/quota is not ready")

    return {
        "paid_io_allowed": True,
        "write_mode": "native-only",
        "active_version": reader.active_version,
        "active_slot": reader.active_slot,
        "candidate_slot": tm_v2.inactive_slot(reader),
        "revision": int(reader.revision),
        "reader_views": views,
        "proxy_control_url": proxy_url,
        "raw_store_uri": raw_store.uri_prefix,
    }


def _utc_timestamp(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise BackfillRuntimeError(
                f"registry {field} is not an ISO timestamp"
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BackfillRuntimeError(f"registry {field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def validate_fresh_registry_snapshot(
    rows: Iterable[Mapping[str, Any]],
    *,
    now: datetime | None = None,
    max_age: timedelta = DEFAULT_DISCOVERY_MAX_AGE,
) -> tuple[dict[str, Any], ...]:
    """Require a complete, single, freshly discovered promoted snapshot."""

    items = tuple(dict(item) for item in rows)
    if not items:
        raise BackfillRuntimeError("promoted registry snapshot is empty")
    snapshot_ids = {str(item.get("registry_snapshot_id") or "") for item in items}
    if len(snapshot_ids) != 1 or "" in snapshot_ids:
        raise BackfillRuntimeError("registry rows do not form one exact snapshot")
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise BackfillRuntimeError("registry freshness clock must include a timezone")
    cutoff = observed.astimezone(timezone.utc) - max_age
    for item in items:
        for field in ("competition_discovered_at", "edition_discovered_at"):
            if _utc_timestamp(item.get(field), field=field) < cutoff:
                raise BackfillRuntimeError(
                    "a fresh full discovery snapshot is required before campaign creation"
                )
    return items


def historical_targets_from_registry(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[state.HistoricalScopeTarget, ...]:
    items = tuple(dict(item) for item in rows)
    targets = eligible_registry_scopes(items)
    historical = tuple(item for item in targets if not item.current)
    if not historical:
        raise BackfillRuntimeError(
            "promoted registry has no historical senior-men scopes"
        )
    snapshot_ids = {str(item.get("registry_snapshot_id") or "") for item in items}
    if len(snapshot_ids) != 1 or "" in snapshot_ids:
        raise BackfillRuntimeError("registry rows do not form one exact snapshot")
    snapshot_id = next(iter(snapshot_ids))
    return tuple(
        state.HistoricalScopeTarget(
            scope_id=item.scope_id,
            competition_id=item.competition_id,
            edition_id=item.edition_id,
            canonical_competition_id=item.canonical_competition_id,
            canonical_season=item.canonical_season,
            registry_snapshot_id=snapshot_id,
            gender=item.gender,
            age_category=item.age_category,
        )
        for item in historical
    )


def _semantic_target_identity(target: state.HistoricalScopeTarget) -> tuple[str, ...]:
    return (
        target.scope_id,
        target.competition_id,
        target.edition_id,
        target.canonical_competition_id,
        target.canonical_season,
        target.gender,
        target.age_category,
    )


def build_campaign_from_registry(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy_sha256: str,
    now: datetime,
    previous_campaign: state.BackfillCampaign | None = None,
    previous_campaigns: Iterable[state.BackfillCampaign] = (),
) -> state.BackfillCampaign | None:
    """Build the initial full campaign or a later frozen snapshot delta."""

    items = validate_fresh_registry_snapshot(rows, now=now)
    targets = historical_targets_from_registry(items)
    history = tuple(previous_campaigns)
    if previous_campaign is not None:
        history = (*history, previous_campaign)
    if history:
        previous = {
            _semantic_target_identity(target)
            for campaign_record in history
            for target in campaign_record.targets
        }
        targets = tuple(
            item for item in targets if _semantic_target_identity(item) not in previous
        )
        if not targets:
            return None
    snapshot_id = targets[0].registry_snapshot_id
    parser_revisions = sorted(
        {
            str(item.get(field) or "")
            for item in items
            for field in ("competition_parser_revision", "edition_parser_revision")
        }
    )
    schema_revisions = sorted(
        {
            str(item.get(field) or "")
            for item in items
            for field in ("competition_schema_revision", "edition_schema_revision")
        }
    )
    if "" in parser_revisions or "" in schema_revisions:
        raise BackfillRuntimeError("registry parser/schema revision is missing")
    return state.BackfillCampaign.build(
        registry_snapshot_id=snapshot_id,
        policy_sha256=policy_sha256,
        parser_revision=json.dumps(parser_revisions, separators=(",", ":")),
        schema_revision=json.dumps(schema_revisions, separators=(",", ":")),
        targets=targets,
        now=now,
        status=state.CampaignStatus.WAITING_PREREQUISITE,
    )


def claim_and_plan(
    campaign: state.BackfillCampaign,
    scopes: Iterable[state.BackfillScopeState],
    *,
    registry_rows: Iterable[Mapping[str, Any]],
    run_id: str,
    lease_owner: str,
    now: datetime,
    limit: int = MAX_SCOPE_BATCH,
) -> tuple[state.ClaimResult, tuple[dict[str, Any], ...]]:
    """Claim <=8 durable scopes and produce matching campaign-stable payloads."""

    before = tuple(scopes)
    claim = state.claim_scopes(
        campaign,
        before,
        lease_owner=lease_owner,
        now=now,
        limit=limit,
    )
    if claim.batch is None:
        return claim, ()
    claimed = {
        item.target.scope_id: item
        for item in claim.scopes
        if item.batch_id == claim.batch.batch_id
        and item.status is state.ScopeStatus.RUNNING
    }
    selectors = [
        {
            "competition_id": claimed[scope_id].target.competition_id,
            "edition_id": claimed[scope_id].target.edition_id,
        }
        for scope_id in claim.batch.scope_ids
    ]
    plan = plan_transfermarkt_scopes(
        {"scopes": selectors},
        # The parent ledger is the durable batch budget.  Tying it to the
        # Airflow run would silently reset that budget after a crash/retry.
        parent_cycle_id=claim.batch.batch_id,
        resume_cycle_id=campaign.campaign_id,
        registry_rows=tuple(registry_rows),
        max_batch_size=limit,
        result_root=BACKFILL_RESULT_ROOT,
        selection_mode="historical_only",
        now=now,
    )
    payloads = tuple(plan.mapped_payloads)
    if tuple(item["scope_id"] for item in payloads) != claim.batch.scope_ids:
        raise BackfillRuntimeError("planned payloads differ from the durable claim")
    return claim, payloads


def select_recoverable_batch(
    campaign: state.BackfillCampaign,
    scopes: Iterable[state.BackfillScopeState],
    batches: Iterable[state.BackfillBatch],
) -> state.BackfillBatch | None:
    """Return the one batch that must finish before any new claim is allowed."""

    if campaign.status is not state.CampaignStatus.ACTIVE:
        raise BackfillRuntimeError("batch recovery requires an active campaign")
    scope_items = tuple(scopes)
    scope_by_id = {item.target.scope_id: item for item in scope_items}
    if len(scope_by_id) != len(scope_items):
        raise BackfillRuntimeError("campaign contains duplicate scope_id")
    batch_items = tuple(batches)
    batch_by_id = {item.batch_id: item for item in batch_items}
    if len(batch_by_id) != len(batch_items):
        raise BackfillRuntimeError("campaign contains duplicate batch_id")
    if any(item.campaign_id != campaign.campaign_id for item in batch_items):
        raise BackfillRuntimeError("batch belongs to another campaign")
    unfinished = tuple(
        item
        for item in batch_items
        if item.status
        in {
            state.BatchStatus.CLAIMED,
            state.BatchStatus.RUNNING,
            state.BatchStatus.DQ_PENDING,
            state.BatchStatus.BLOCKED_PLATFORM,
        }
    )
    incident_batch_ids = {
        item.batch_id
        for item in batch_items
        if item.open_platform_incident_id is not None
    }
    captured_batch_ids = {
        str(item.batch_id)
        for item in scope_items
        if item.status is state.ScopeStatus.CAPTURED_PENDING_DQ
        and item.batch_id is not None
    }
    candidates = (
        {item.batch_id for item in unfinished} | captured_batch_ids | incident_batch_ids
    )
    if len(candidates) > 1:
        raise BackfillRuntimeError("more than one batch requires recovery")
    if not candidates:
        return None
    batch_id = next(iter(candidates))
    batch = batch_by_id.get(batch_id)
    if batch is None:
        raise BackfillRuntimeError("recoverable scope references a missing batch")
    if (
        batch.status is state.BatchStatus.COMPLETE
        and not captured_batch_ids
        and batch.open_platform_incident_id is None
    ):
        return None
    expected = dict(
        zip(
            batch.scope_ids,
            batch.scope_claim_generations,
            strict=True,
        )
    )
    for scope_id, generation in expected.items():
        scope = scope_by_id.get(scope_id)
        if scope is None:
            raise BackfillRuntimeError("recoverable batch scope is missing")
        accounted = (
            scope.batch_id == batch.batch_id and scope.claim_generation == generation
        )
        partial_claim = (
            batch.status in {state.BatchStatus.CLAIMED, state.BatchStatus.RUNNING}
            and scope.status
            in {
                state.ScopeStatus.PENDING,
                state.ScopeStatus.RETRYABLE_ERROR,
            }
            and scope.claim_generation + 1 == generation
        )
        if not accounted and not partial_claim:
            raise BackfillRuntimeError("recoverable batch membership drifted")
    return batch


def plan_existing_batch(
    campaign: state.BackfillCampaign,
    batch: state.BackfillBatch,
    *,
    registry_rows: Iterable[Mapping[str, Any]],
    run_id: str,
    now: datetime,
) -> tuple[dict[str, Any], ...]:
    """Rebuild stable result paths for an interrupted batch without claiming."""

    if batch.campaign_id != campaign.campaign_id:
        raise BackfillRuntimeError("batch belongs to another campaign")
    targets = {item.scope_id: item for item in campaign.targets}
    try:
        selectors = [
            {
                "competition_id": targets[scope_id].competition_id,
                "edition_id": targets[scope_id].edition_id,
            }
            for scope_id in batch.scope_ids
        ]
    except KeyError as exc:
        raise BackfillRuntimeError("batch scope is outside frozen campaign") from exc
    plan = plan_transfermarkt_scopes(
        {"scopes": selectors},
        # Recovery must reuse the same ledger and therefore the same batch
        # budget even though it happens in a later Airflow DagRun.
        parent_cycle_id=batch.batch_id,
        resume_cycle_id=campaign.campaign_id,
        registry_rows=tuple(registry_rows),
        max_batch_size=MAX_SCOPE_BATCH,
        result_root=BACKFILL_RESULT_ROOT,
        selection_mode="historical_only",
        now=now,
    )
    payloads = tuple(plan.mapped_payloads)
    if tuple(item["scope_id"] for item in payloads) != batch.scope_ids:
        raise BackfillRuntimeError("recovery plan differs from durable batch")
    for payload in payloads:
        target = targets[str(payload["scope_id"])]
        if (
            str(payload.get("registry_snapshot_id")) != campaign.registry_snapshot_id
            or str(payload.get("canonical_competition_id"))
            != target.canonical_competition_id
            or str(payload.get("canonical_season")) != target.canonical_season
        ):
            raise BackfillRuntimeError("recovery payload differs from frozen target")
    return payloads


def registry_target_for_scope(
    targets: Iterable[RegistryScopeTarget],
    scope_id: str,
) -> RegistryScopeTarget:
    matches = [item for item in targets if item.scope_id == scope_id]
    if len(matches) != 1:
        raise BackfillRuntimeError("scope is not unique in the frozen registry")
    return matches[0]


__all__ = [
    "BACKFILL_DAG_ID",
    "BACKFILL_RESULT_ROOT",
    "DEFAULT_DISCOVERY_MAX_AGE",
    "BackfillRuntimeError",
    "BackfillStateRepository",
    "build_campaign_from_registry",
    "claim_and_plan",
    "historical_targets_from_registry",
    "plan_existing_batch",
    "registry_target_for_scope",
    "read_promoted_registry",
    "select_recoverable_batch",
    "strict_cutover_preflight",
    "validate_fresh_registry_snapshot",
]
