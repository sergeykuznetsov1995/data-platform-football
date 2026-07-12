"""Versioned PostgreSQL migrations for the FBref control plane.

The control schema deliberately lives beside Airflow metadata in PostgreSQL,
but never uses Airflow's ORM or metadata tables.  Migrations are plain SQL so
they can be run from ``airflow-init`` as well as from focused administration
commands without importing Airflow.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Tuple


CONTROL_SCHEMA = "fbref_control"
MIGRATION_LOCK_KEY = 7_733_210_923


@dataclass(frozen=True)
class Migration:
    """One append-only control-schema migration."""

    version: int
    name: str
    statements: Tuple[str, ...]

    @property
    def checksum(self) -> str:
        rendered = "\n-- statement --\n".join(self.statements)
        return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


MIGRATIONS = (
    Migration(
        version=1,
        name="runs_budgets_frontier",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS fbref_control.crawl_run (
                run_id uuid PRIMARY KEY,
                run_type text NOT NULL,
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'running', 'succeeded', 'failed',
                        'cancelled'
                    )),
                request_limit bigint NOT NULL CHECK (request_limit >= 0),
                byte_limit bigint NOT NULL CHECK (byte_limit >= 0),
                requests_reserved bigint NOT NULL DEFAULT 0
                    CHECK (requests_reserved >= 0),
                bytes_reserved bigint NOT NULL DEFAULT 0
                    CHECK (bytes_reserved >= 0),
                requests_used bigint NOT NULL DEFAULT 0
                    CHECK (requests_used >= 0),
                bytes_used bigint NOT NULL DEFAULT 0
                    CHECK (bytes_used >= 0),
                budget_exceeded boolean NOT NULL DEFAULT false,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                started_at timestamptz,
                finished_at timestamptz,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.page_frontier (
                target_id text PRIMARY KEY,
                source text NOT NULL DEFAULT 'fbref',
                page_kind text NOT NULL,
                canonical_url text NOT NULL UNIQUE,
                source_ids jsonb NOT NULL DEFAULT '{}'::jsonb,
                refresh_policy text NOT NULL,
                state text NOT NULL DEFAULT 'queued'
                    CHECK (state IN (
                        'queued', 'leased', 'fetched', 'retry', 'dead',
                        'quarantined', 'skipped'
                    )),
                priority integer NOT NULL DEFAULT 0,
                next_fetch_at timestamptz,
                retry_after timestamptz,
                claim_token uuid,
                lease_epoch bigint NOT NULL DEFAULT 0,
                lease_run_id uuid REFERENCES fbref_control.crawl_run(run_id),
                lease_refresh_id uuid,
                leased_by text,
                lease_expires_at timestamptz,
                last_fetched_at timestamptz,
                last_http_status integer,
                last_content_hash text,
                last_etag text,
                last_modified text,
                last_error_class text,
                last_error_message text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CHECK ((state = 'leased') = (claim_token IS NOT NULL)),
                CHECK ((state = 'leased') = (lease_expires_at IS NOT NULL)),
                CHECK ((state = 'leased') = (lease_run_id IS NOT NULL)),
                CHECK ((state = 'leased') = (lease_refresh_id IS NOT NULL))
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS page_frontier_due_idx
            ON fbref_control.page_frontier (
                state, retry_after, next_fetch_at, priority DESC
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.run_target (
                run_id uuid NOT NULL
                    REFERENCES fbref_control.crawl_run(run_id),
                target_id text NOT NULL
                    REFERENCES fbref_control.page_frontier(target_id),
                logical_refresh_id uuid NOT NULL UNIQUE,
                ordinal bigint NOT NULL,
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'leased', 'retry', 'succeeded', 'failed',
                        'skipped'
                    )),
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (run_id, target_id),
                UNIQUE (run_id, ordinal)
            )
            """,
            """
            CREATE OR REPLACE FUNCTION fbref_control.guard_run_target_identity()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $body$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'FBref run cohorts are immutable';
                END IF;
                IF NEW.run_id IS DISTINCT FROM OLD.run_id
                   OR NEW.target_id IS DISTINCT FROM OLD.target_id
                   OR NEW.logical_refresh_id IS DISTINCT FROM OLD.logical_refresh_id
                   OR NEW.ordinal IS DISTINCT FROM OLD.ordinal THEN
                    RAISE EXCEPTION 'FBref run cohort identity is immutable';
                END IF;
                RETURN NEW;
            END
            $body$
            """,
            """
            DROP TRIGGER IF EXISTS guard_run_target_identity
            ON fbref_control.run_target
            """,
            """
            CREATE TRIGGER guard_run_target_identity
            BEFORE UPDATE OR DELETE ON fbref_control.run_target
            FOR EACH ROW EXECUTE FUNCTION
                fbref_control.guard_run_target_identity()
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.budget_reservation (
                reservation_id uuid PRIMARY KEY,
                run_id uuid NOT NULL
                    REFERENCES fbref_control.crawl_run(run_id),
                logical_refresh_id uuid NOT NULL,
                requests_reserved bigint NOT NULL
                    CHECK (requests_reserved >= 0),
                bytes_reserved bigint NOT NULL
                    CHECK (bytes_reserved >= 0),
                requests_used bigint CHECK (requests_used >= 0),
                bytes_used bigint CHECK (bytes_used >= 0),
                status text NOT NULL DEFAULT 'reserved'
                    CHECK (status IN ('reserved', 'settled')),
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                settled_at timestamptz,
                CHECK (
                    (status = 'reserved'
                        AND requests_used IS NULL AND bytes_used IS NULL)
                    OR
                    (status = 'settled'
                        AND requests_used IS NOT NULL AND bytes_used IS NOT NULL)
                )
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS budget_reservation_refresh_idx
            ON fbref_control.budget_reservation (
                run_id, logical_refresh_id, created_at
            )
            """,
        ),
    ),
    Migration(
        version=2,
        name="source_registry",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS fbref_control.registry_snapshot (
                snapshot_id uuid PRIMARY KEY,
                run_id uuid REFERENCES fbref_control.crawl_run(run_id),
                source text NOT NULL DEFAULT 'fbref',
                content_hash text,
                successful boolean NOT NULL,
                fetched_at timestamptz NOT NULL,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.competition_registry (
                source text NOT NULL DEFAULT 'fbref',
                competition_id text NOT NULL,
                canonical_url text NOT NULL,
                name text NOT NULL,
                gender text NOT NULL
                    CHECK (gender IN ('male', 'female', 'unknown')),
                classification text NOT NULL,
                calendar_type text,
                lifecycle_state text NOT NULL DEFAULT 'present'
                    CHECK (lifecycle_state IN (
                        'present', 'missing_once', 'disappeared'
                    )),
                crawl_state text NOT NULL
                    CHECK (crawl_state IN (
                        'active', 'skipped', 'quarantined'
                    )),
                present boolean NOT NULL DEFAULT true,
                consecutive_misses integer NOT NULL DEFAULT 0
                    CHECK (consecutive_misses >= 0),
                first_seen_at timestamptz NOT NULL,
                last_seen_at timestamptz NOT NULL,
                first_snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                last_snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (source, competition_id),
                UNIQUE (source, canonical_url)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.snapshot_competition (
                snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                source text NOT NULL,
                competition_id text NOT NULL,
                PRIMARY KEY (snapshot_id, source, competition_id),
                FOREIGN KEY (source, competition_id) REFERENCES
                    fbref_control.competition_registry(source, competition_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.season_registry (
                source text NOT NULL DEFAULT 'fbref',
                competition_id text NOT NULL,
                season_id text NOT NULL,
                canonical_url text NOT NULL,
                label text,
                is_current boolean NOT NULL DEFAULT false,
                lifecycle_state text NOT NULL DEFAULT 'present'
                    CHECK (lifecycle_state IN (
                        'present', 'missing_once', 'disappeared'
                    )),
                present boolean NOT NULL DEFAULT true,
                consecutive_misses integer NOT NULL DEFAULT 0
                    CHECK (consecutive_misses >= 0),
                first_seen_at timestamptz NOT NULL,
                last_seen_at timestamptz NOT NULL,
                first_snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                last_snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (source, competition_id, season_id),
                UNIQUE (source, canonical_url),
                FOREIGN KEY (source, competition_id) REFERENCES
                    fbref_control.competition_registry(source, competition_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.snapshot_season (
                snapshot_id uuid NOT NULL
                    REFERENCES fbref_control.registry_snapshot(snapshot_id),
                source text NOT NULL,
                competition_id text NOT NULL,
                season_id text NOT NULL,
                PRIMARY KEY (
                    snapshot_id, source, competition_id, season_id
                ),
                FOREIGN KEY (source, competition_id, season_id) REFERENCES
                    fbref_control.season_registry(
                        source, competition_id, season_id
                    )
            )
            """,
        ),
    ),
    Migration(
        version=3,
        name="attempts_datasets_sessions_throttle",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS fbref_control.fetch_attempt (
                attempt_id uuid PRIMARY KEY,
                run_id uuid NOT NULL
                    REFERENCES fbref_control.crawl_run(run_id),
                target_id text NOT NULL
                    REFERENCES fbref_control.page_frontier(target_id),
                logical_refresh_id uuid NOT NULL,
                attempt_number integer NOT NULL CHECK (attempt_number > 0),
                claim_token uuid NOT NULL,
                lease_epoch bigint NOT NULL,
                reservation_id uuid UNIQUE REFERENCES
                    fbref_control.budget_reservation(reservation_id),
                status text NOT NULL
                    CHECK (status IN (
                        'claimed', 'succeeded', 'failed', 'expired'
                    )),
                http_status integer,
                content_hash text,
                raw_manifest_key text,
                decoded_bytes bigint CHECK (decoded_bytes >= 0),
                compressed_bytes bigint CHECK (compressed_bytes >= 0),
                wire_bytes bigint CHECK (wire_bytes >= 0),
                provider_billed_bytes bigint
                    CHECK (provider_billed_bytes >= 0),
                etag text,
                last_modified text,
                transport_version text,
                session_version text,
                latency_ms bigint CHECK (latency_ms >= 0),
                error_class text,
                error_message text,
                started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                finished_at timestamptz,
                UNIQUE (logical_refresh_id, attempt_number),
                UNIQUE (claim_token, lease_epoch)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS fetch_attempt_target_idx
            ON fbref_control.fetch_attempt (target_id, started_at DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.dataset_manifest (
                target_id text NOT NULL
                    REFERENCES fbref_control.page_frontier(target_id),
                content_hash text NOT NULL,
                parser_version text NOT NULL,
                dataset text NOT NULL,
                availability text NOT NULL
                    CHECK (availability IN (
                        'available', 'empty', 'restricted', 'not_applicable',
                        'duplicate', 'layout_only', 'unknown', 'error'
                    )),
                parse_status text NOT NULL
                    CHECK (parse_status IN (
                        'pending', 'succeeded', 'failed', 'skipped'
                    )),
                persistence_status text NOT NULL
                    CHECK (persistence_status IN (
                        'pending', 'succeeded', 'failed', 'skipped'
                    )),
                validation_status text NOT NULL
                    CHECK (validation_status IN (
                        'pending', 'succeeded', 'failed', 'skipped'
                    )),
                row_count bigint NOT NULL DEFAULT 0 CHECK (row_count >= 0),
                manifest_key text,
                error_class text,
                error_message text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                completed_at timestamptz,
                PRIMARY KEY (
                    target_id, content_hash, parser_version, dataset
                )
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.clearance_session (
                session_id uuid PRIMARY KEY,
                run_id uuid REFERENCES fbref_control.crawl_run(run_id),
                domain text NOT NULL,
                session_version text NOT NULL,
                status text NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'expired', 'closed', 'failed')),
                browser_bootstrap_requests bigint NOT NULL DEFAULT 0,
                browser_document_bytes bigint NOT NULL DEFAULT 0,
                browser_asset_bytes bigint NOT NULL DEFAULT 0,
                http_requests bigint NOT NULL DEFAULT 0,
                http_wire_bytes bigint NOT NULL DEFAULT 0,
                decoded_html_bytes bigint NOT NULL DEFAULT 0,
                compressed_raw_bytes bigint NOT NULL DEFAULT 0,
                provider_billed_bytes bigint,
                opened_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                expires_at timestamptz NOT NULL,
                closed_at timestamptz,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                CHECK (provider_billed_bytes IS NULL
                    OR provider_billed_bytes >= 0)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS clearance_session_active_idx
            ON fbref_control.clearance_session (domain, expires_at)
            WHERE status = 'active'
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.domain_throttle (
                domain text PRIMARY KEY,
                next_request_at timestamptz NOT NULL,
                lease_epoch bigint NOT NULL DEFAULT 0,
                last_slot_token uuid,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """,
        ),
    ),
    Migration(
        version=4,
        name="observation_processing_and_http_evidence",
        statements=(
            """
            ALTER TABLE fbref_control.fetch_attempt
            ADD COLUMN IF NOT EXISTS http_request_count bigint NOT NULL
                DEFAULT 0 CHECK (http_request_count >= 0)
            """,
            """
            ALTER TABLE fbref_control.fetch_attempt
            ADD COLUMN IF NOT EXISTS http_status_history integer[] NOT NULL
                DEFAULT '{}'::integer[]
            """,
            """
            CREATE TABLE IF NOT EXISTS fbref_control.observation_processing (
                logical_refresh_id uuid NOT NULL
                    REFERENCES fbref_control.run_target(logical_refresh_id),
                parser_version text NOT NULL,
                typed_parser_version text NOT NULL,
                target_id text NOT NULL
                    REFERENCES fbref_control.page_frontier(target_id),
                content_hash text NOT NULL,
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'processing', 'succeeded', 'failed'
                    )),
                generic_status text NOT NULL DEFAULT 'pending'
                    CHECK (generic_status IN (
                        'pending', 'succeeded', 'failed'
                    )),
                typed_status text NOT NULL DEFAULT 'pending'
                    CHECK (typed_status IN (
                        'pending', 'succeeded', 'skipped', 'failed'
                    )),
                stateful_status text NOT NULL DEFAULT 'pending'
                    CHECK (stateful_status IN (
                        'pending', 'succeeded', 'skipped', 'failed'
                    )),
                validation_status text NOT NULL DEFAULT 'pending'
                    CHECK (validation_status IN (
                        'pending', 'succeeded', 'failed'
                    )),
                claim_token uuid,
                lease_expires_at timestamptz,
                error_class text,
                error_message text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                started_at timestamptz,
                completed_at timestamptz,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (
                    logical_refresh_id, parser_version, typed_parser_version
                ),
                CHECK (
                    (status = 'processing') = (claim_token IS NOT NULL)
                ),
                CHECK (
                    (status = 'processing') = (lease_expires_at IS NOT NULL)
                )
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS observation_processing_work_idx
            ON fbref_control.observation_processing (
                status, lease_expires_at, parser_version,
                typed_parser_version
            )
            """,
        ),
    ),
    Migration(
        version=5,
        name="clearance_bootstrap_attempt_evidence",
        statements=(
            """
            ALTER TABLE fbref_control.clearance_session
            ADD COLUMN IF NOT EXISTS browser_bootstrap_attempts bigint
                NOT NULL DEFAULT 0
                CHECK (browser_bootstrap_attempts >= 0)
            """,
        ),
    ),
    Migration(
        version=6,
        name="clearance_unobserved_browser_bytes",
        statements=(
            """
            ALTER TABLE fbref_control.clearance_session
            ADD COLUMN IF NOT EXISTS browser_unobserved_bytes bigint
                NOT NULL DEFAULT 0
                CHECK (browser_unobserved_bytes >= 0)
            """,
        ),
    ),
    Migration(
        version=7,
        name="observation_stateful_parser_version",
        statements=(
            """
            ALTER TABLE fbref_control.observation_processing
            ADD COLUMN IF NOT EXISTS stateful_parser_version text NOT NULL
                DEFAULT 'fbref-discovery-parser-v1'
            """,
            """
            ALTER TABLE fbref_control.observation_processing
            DROP CONSTRAINT observation_processing_pkey
            """,
            """
            ALTER TABLE fbref_control.observation_processing
            ADD PRIMARY KEY (
                logical_refresh_id, parser_version, typed_parser_version,
                stateful_parser_version
            )
            """,
        ),
    ),
)


def bootstrap_statements() -> Tuple[str, ...]:
    """Return the statements required before versioned migrations run."""
    return (
        f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.schema_migration (
            version integer PRIMARY KEY,
            name text NOT NULL,
            checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """,
    )
